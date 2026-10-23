// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Experimental singleton filesystem effects for exact physical generations.
//!
//! This is deliberately not a `StoreDriver`. Its caller owns generation and
//! operation allocation, committed protection, retirement, unresolved effects,
//! and replay. A physical receipt does not authorize publication or collection.
//! Methods perform synchronous bounded I/O; an async caller must supply its own
//! bounded blocking executor. The namespace must be on one local filesystem,
//! exclusively managed by this adapter, with file and directory sync support.
//! Hostile external namespace mutation and power-loss certification are outside
//! this experiment. No automatic namespace repair or legacy-format adoption occurs.

use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use sha2::{Digest, Sha256};

const MANIFEST_MAGIC: &[u8; 8] = b"PHYSMAN1";
const OBJECT_MAGIC: &[u8; 8] = b"PHYSOBJ1";
const MANIFEST_BODY: usize = 56;
const MANIFEST_SIZE: usize = MANIFEST_BODY + 32;
const HEADER_SIZE: usize = 73;
const BUFFER_SIZE: usize = 64 * 1024;
const MAX_OBJECT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FILES: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureKind {
    Missing,
    Unavailable,
    Corrupt,
    Conflict,
    Capacity,
    Invalid,
}

/// Bounded diagnostics: neither object data nor caller-supplied paths are retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Failure {
    pub kind: FailureKind,
    pub operation: &'static str,
    pub io_kind: Option<std::io::ErrorKind>,
}

impl Failure {
    const fn new(kind: FailureKind, operation: &'static str) -> Self {
        Self {
            kind,
            operation,
            io_kind: None,
        }
    }

    fn io(operation: &'static str, error: &std::io::Error) -> Self {
        let kind = match error.kind() {
            std::io::ErrorKind::AlreadyExists => FailureKind::Conflict,
            std::io::ErrorKind::UnexpectedEof => FailureKind::Corrupt,
            _ => FailureKind::Unavailable,
        };
        Self {
            kind,
            operation,
            io_kind: Some(error.kind()),
        }
    }

    fn lookup(operation: &'static str, error: &std::io::Error) -> Self {
        let mut failure = Self::io(operation, error);
        if error.kind() == std::io::ErrorKind::NotFound {
            failure.kind = FailureKind::Missing;
        }
        failure
    }
}

impl Display for Failure {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            FailureKind::Missing => "missing",
            FailureKind::Unavailable => "unavailable",
            FailureKind::Corrupt => "corrupt",
            FailureKind::Conflict => "conflict",
            FailureKind::Capacity => "capacity",
            FailureKind::Invalid => "invalid",
        };
        write!(f, "{}: {kind}", self.operation)?;
        if let Some(io) = self.io_kind {
            write!(f, " ({io})")?;
        }
        Ok(())
    }
}

impl std::error::Error for Failure {}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Incarnation([u8; 16]);

impl Incarnation {
    pub fn new(bytes: [u8; 16]) -> Result<Self, Failure> {
        if bytes == [0; 16] {
            return Err(Failure::new(FailureKind::Invalid, "zero incarnation"));
        }
        Ok(Self(bytes))
    }

    pub fn directory_name(self) -> String {
        format!("incarnation-{}", hex::encode(self.0))
    }

    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PhysicalKey {
    pub incarnation: Incarnation,
    generation: u64,
}

impl PhysicalKey {
    pub const fn new(incarnation: Incarnation, generation: u64) -> Result<Self, Failure> {
        if generation == 0 {
            return Err(Failure::new(FailureKind::Invalid, "zero generation"));
        }
        Ok(Self {
            incarnation,
            generation,
        })
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Algorithm {
    Sha256,
    Blake3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObjectId {
    pub algorithm: Algorithm,
    pub hash: [u8; 32],
    pub size: u64,
}

impl ObjectId {
    pub fn for_bytes(algorithm: Algorithm, bytes: &[u8]) -> Self {
        let mut hash = Hasher::new(algorithm);
        hash.update(bytes);
        Self {
            algorithm,
            hash: hash.finish(),
            size: bytes.len() as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub object_bytes: u64,
    pub objects: usize,
    pub staged_writes: usize,
    pub concurrent_reads: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            object_bytes: 16 * 1024 * 1024,
            objects: 1024,
            staged_writes: 16,
            concurrent_reads: 8,
        }
    }
}

impl Limits {
    const fn validate(self) -> Result<(), Failure> {
        if self.object_bytes == 0
            || self.object_bytes > MAX_OBJECT_BYTES
            || self.objects == 0
            || self.objects > MAX_FILES
            || self.staged_writes == 0
            || self.staged_writes > MAX_FILES
            || self.concurrent_reads == 0
            || self.concurrent_reads > 32
        {
            return Err(Failure::new(FailureKind::Invalid, "physical limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteReceipt {
    pub key: PhysicalKey,
    pub write: u64,
    pub object: ObjectId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteReceipt {
    pub key: PhysicalKey,
    pub delete: u64,
    pub already_absent: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct VerifiedRead {
    pub key: PhysicalKey,
    pub object: ObjectId,
    pub offset: u64,
    /// Exposed only after the complete physical object's identity is verified.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
enum Hasher {
    Sha256(Box<Sha256>),
    Blake3(Box<blake3::Hasher>),
}

impl Hasher {
    fn new(algorithm: Algorithm) -> Self {
        match algorithm {
            Algorithm::Sha256 => Self::Sha256(Box::new(Sha256::new())),
            Algorithm::Blake3 => Self::Blake3(Box::default()),
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha256(hash) => hash.update(bytes),
            Self::Blake3(hash) => {
                hash.update(bytes);
            }
        }
    }
    fn finish(self) -> [u8; 32] {
        match self {
            Self::Sha256(hash) => (*hash).finalize().into(),
            Self::Blake3(hash) => *hash.finalize().as_bytes(),
        }
    }
}

#[derive(Debug)]
struct Inner {
    incarnation: Incarnation,
    limits: Limits,
    namespace: PathBuf,
    objects: PathBuf,
    staging: PathBuf,
    // Every staged handle holds Inner, so dropping its original adapter cannot
    // release ownership while an old local write can still publish.
    _manifest_lock: File,
    active: Mutex<BTreeSet<(u64, u64)>>,
    reads: AtomicUsize,
}

#[derive(Clone, Debug)]
pub struct PhysicalFilesystem(Arc<Inner>);

fn sync_directory(path: &Path) -> Result<(), Failure> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            let mut failure = Failure::io("sync directory", &error);
            failure.kind = FailureKind::Unavailable;
            failure
        })
}

fn namespace(base: &Path, incarnation: Incarnation) -> Result<PathBuf, Failure> {
    if base.as_os_str().as_encoded_bytes().len() > 3500 {
        return Err(Failure::new(FailureKind::Invalid, "namespace path bound"));
    }
    Ok(base.join(incarnation.directory_name()))
}

fn header(key: PhysicalKey, object: ObjectId) -> [u8; HEADER_SIZE] {
    let mut result = [0; HEADER_SIZE];
    result[..8].copy_from_slice(OBJECT_MAGIC);
    result[8..24].copy_from_slice(&key.incarnation.0);
    result[24..32].copy_from_slice(&key.generation.to_be_bytes());
    result[32] = match object.algorithm {
        Algorithm::Sha256 => 1,
        Algorithm::Blake3 => 2,
    };
    result[33..41].copy_from_slice(&object.size.to_be_bytes());
    result[41..].copy_from_slice(&object.hash);
    result
}

impl PhysicalFilesystem {
    /// Creates a never-used incarnation directory. An interrupted bootstrap is
    /// not repaired: opening fails and reusing this incarnation conflicts.
    /// The caller must allocate a fresh incarnation after authority loss; an
    /// empty directory is never evidence that an old identity is reusable.
    pub fn bootstrap(
        base: &Path,
        incarnation: Incarnation,
        limits: Limits,
    ) -> Result<Self, Failure> {
        limits.validate()?;
        let namespace = namespace(base, incarnation)?;
        // The caller provisions the local mount/root durably. Creating an
        // arbitrary parent chain here would require syncing every ancestor.
        if !base.is_dir() {
            return Err(Failure::new(FailureKind::Unavailable, "base directory"));
        }
        fs::create_dir(&namespace).map_err(|error| Failure::io("create incarnation", &error))?;
        sync_directory(base)?;
        let manifest_path = namespace.join("manifest");
        let mut manifest = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&manifest_path)
            .map_err(|error| Failure::io("create manifest", &error))?;
        manifest
            .try_lock()
            .map_err(|_| Failure::new(FailureKind::Conflict, "namespace ownership"))?;
        let mut bytes = [0; MANIFEST_SIZE];
        bytes[..8].copy_from_slice(MANIFEST_MAGIC);
        bytes[8..24].copy_from_slice(&incarnation.0);
        for (index, value) in [
            limits.object_bytes,
            limits.objects as u64,
            limits.staged_writes as u64,
            limits.concurrent_reads as u64,
        ]
        .into_iter()
        .enumerate()
        {
            bytes[24 + index * 8..32 + index * 8].copy_from_slice(&value.to_be_bytes());
        }
        let checksum: [u8; 32] = Sha256::digest(&bytes[..MANIFEST_BODY]).into();
        bytes[MANIFEST_BODY..].copy_from_slice(&checksum);
        let objects = namespace.join("objects-v1");
        let staging = namespace.join("staging-v1");
        fs::create_dir(&objects).map_err(|error| Failure::io("create objects", &error))?;
        fs::create_dir(&staging).map_err(|error| Failure::io("create staging", &error))?;
        sync_directory(&objects)?;
        sync_directory(&staging)?;
        manifest
            .write_all(&bytes)
            .and_then(|()| manifest.sync_all())
            .map_err(|error| Failure::io("publish manifest", &error))?;
        sync_directory(&namespace)?;
        Ok(Self(Arc::new(Inner {
            incarnation,
            limits,
            namespace,
            objects,
            staging,
            _manifest_lock: manifest,
            active: Mutex::new(BTreeSet::new()),
            reads: AtomicUsize::new(0),
        })))
    }

    /// Opens exactly the supplied incarnation; never creates, repairs, scans
    /// for a replacement manifest, or infers an allocation counter from blobs.
    pub fn open(base: &Path, incarnation: Incarnation) -> Result<Self, Failure> {
        let namespace = namespace(base, incarnation)?;
        let manifest_path = namespace.join("manifest");
        let metadata = fs::symlink_metadata(&manifest_path)
            .map_err(|error| Failure::lookup("manifest metadata", &error))?;
        if !metadata.is_file() || metadata.len() != MANIFEST_SIZE as u64 {
            return Err(Failure::new(FailureKind::Corrupt, "manifest shape"));
        }
        let mut manifest = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&manifest_path)
            .map_err(|error| Failure::lookup("open manifest", &error))?;
        manifest
            .try_lock()
            .map_err(|_| Failure::new(FailureKind::Conflict, "namespace ownership"))?;
        let mut bytes = [0; MANIFEST_SIZE];
        manifest
            .read_exact(&mut bytes)
            .map_err(|error| Failure::io("read manifest", &error))?;
        let checksum: [u8; 32] = Sha256::digest(&bytes[..MANIFEST_BODY]).into();
        if bytes[..8] != *MANIFEST_MAGIC
            || bytes[8..24] != incarnation.0
            || bytes[MANIFEST_BODY..] != checksum
        {
            return Err(Failure::new(
                FailureKind::Corrupt,
                "manifest identity or checksum",
            ));
        }
        let mut values = [0; 4];
        for (index, value) in values.iter_mut().enumerate() {
            *value = u64::from_be_bytes(
                bytes[24 + index * 8..32 + index * 8]
                    .try_into()
                    .expect("fixed manifest field"),
            );
        }
        let to_usize = |value| {
            usize::try_from(value).map_err(|_| Failure::new(FailureKind::Corrupt, "manifest limit"))
        };
        let limits = Limits {
            object_bytes: values[0],
            objects: to_usize(values[1])?,
            staged_writes: to_usize(values[2])?,
            concurrent_reads: to_usize(values[3])?,
        };
        limits
            .validate()
            .map_err(|_| Failure::new(FailureKind::Corrupt, "manifest limits"))?;
        let store = Self(Arc::new(Inner {
            incarnation,
            limits,
            objects: namespace.join("objects-v1"),
            staging: namespace.join("staging-v1"),
            namespace,
            _manifest_lock: manifest,
            active: Mutex::new(BTreeSet::new()),
            reads: AtomicUsize::new(0),
        }));
        store.count_files(false)?;
        store.count_files(true)?;
        Ok(store)
    }

    pub fn namespace_path(&self) -> &Path {
        &self.0.namespace
    }

    fn guard(&self) -> Result<MutexGuard<'_, BTreeSet<(u64, u64)>>, Failure> {
        self.0
            .active
            .try_lock()
            .map_err(|_| Failure::new(FailureKind::Unavailable, "physical operation busy"))
    }

    fn validate(&self, key: PhysicalKey, object: Option<ObjectId>) -> Result<(), Failure> {
        if key.incarnation != self.0.incarnation {
            return Err(Failure::new(FailureKind::Conflict, "foreign incarnation"));
        }
        if object.is_some_and(|object| object.size > self.0.limits.object_bytes) {
            return Err(Failure::new(FailureKind::Capacity, "object byte bound"));
        }
        Ok(())
    }

    fn object_path(&self, key: PhysicalKey) -> PathBuf {
        self.0.objects.join(format!("{:016x}", key.generation))
    }
    fn stage_path(&self, key: PhysicalKey, write: u64) -> PathBuf {
        self.0
            .staging
            .join(format!("{:016x}-{write:016x}", key.generation))
    }

    fn count_files(&self, staging: bool) -> Result<usize, Failure> {
        let (path, cap, name_len) = if staging {
            (&self.0.staging, self.0.limits.staged_writes, 33)
        } else {
            (&self.0.objects, self.0.limits.objects, 16)
        };
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| Failure::io("physical directory", &error))?;
        if !metadata.is_dir() {
            return Err(Failure::new(
                FailureKind::Corrupt,
                "physical directory type",
            ));
        }
        let mut count = 0;
        for entry in
            fs::read_dir(path).map_err(|error| Failure::io("scan physical directory", &error))?
        {
            let entry = entry.map_err(|error| Failure::io("scan physical entry", &error))?;
            count += 1;
            if count > cap {
                return Err(Failure::new(FailureKind::Capacity, "physical file count"));
            }
            let name = entry.file_name();
            let name = name.as_encoded_bytes();
            if name.len() != name_len
                || !name.iter().enumerate().all(|(index, byte)| {
                    if staging && index == 16 {
                        *byte == b'-'
                    } else {
                        byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)
                    }
                })
            {
                return Err(Failure::new(FailureKind::Corrupt, "physical filename"));
            }
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| Failure::io("physical entry metadata", &error))?;
            if !metadata.is_file()
                || metadata.len() > self.0.limits.object_bytes + HEADER_SIZE as u64
            {
                return Err(Failure::new(FailureKind::Corrupt, "physical entry shape"));
            }
        }
        Ok(count)
    }

    pub fn begin_write(
        &self,
        key: PhysicalKey,
        write: u64,
        object: ObjectId,
    ) -> Result<StagedWrite, Failure> {
        self.validate(key, Some(object))?;
        if write == 0 {
            return Err(Failure::new(FailureKind::Invalid, "zero write identity"));
        }
        let mut active = self.guard()?;
        if active.contains(&(key.generation, write)) {
            return Err(Failure::new(FailureKind::Conflict, "active write identity"));
        }
        if self.count_files(true)? >= self.0.limits.staged_writes {
            return Err(Failure::new(FailureKind::Capacity, "staged write bound"));
        }
        let path = self.stage_path(key, write);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| Failure::io("create staged write", &error))?;
        active.insert((key.generation, write));
        drop(active);
        let mut staged = StagedWrite {
            store: self.clone(),
            key,
            write,
            object,
            path,
            file: Some(file),
            hash: Hasher::new(object.algorithm),
            bytes: 0,
            failed: false,
        };
        staged
            .file
            .as_mut()
            .expect("new stage")
            .write_all(&header(key, object))
            .map_err(|error| Failure::io("write object header", &error))?;
        Ok(staged)
    }

    /// Deletes one exact locator. Absence is an idempotent physical outcome,
    /// never evidence that unresolved writes are quiescent or retirement ended.
    pub fn delete(&self, key: PhysicalKey, delete: u64) -> Result<DeleteReceipt, Failure> {
        self.validate(key, None)?;
        if delete == 0 {
            return Err(Failure::new(FailureKind::Invalid, "zero delete identity"));
        }
        let _guard = self.guard()?;
        let already_absent = match fs::remove_file(self.object_path(key)) {
            Ok(()) => false,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(Failure::io("delete physical object", &error)),
        };
        // Missing directory is unavailable, not a successful missing object.
        sync_directory(&self.0.objects)?;
        Ok(DeleteReceipt {
            key,
            delete,
            already_absent,
        })
    }

    /// Removes an abandoned unpublished stage only. Active handles are fenced;
    /// reopening the namespace establishes that the previous local owner exited.
    pub fn discard_stage(&self, key: PhysicalKey, write: u64) -> Result<(), Failure> {
        self.validate(key, None)?;
        if write == 0 {
            return Err(Failure::new(FailureKind::Invalid, "zero write identity"));
        }
        let active = self.guard()?;
        if active.contains(&(key.generation, write)) {
            return Err(Failure::new(FailureKind::Conflict, "active staged write"));
        }
        match fs::remove_file(self.stage_path(key, write)) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(Failure::io("discard staged write", &error)),
        }
        sync_directory(&self.0.staging)
    }

    /// Reads and hashes the entire object, retaining only the requested range.
    /// No range bytes escape before whole-object verification succeeds.
    pub fn read(
        &self,
        key: PhysicalKey,
        object: ObjectId,
        offset: u64,
        length: Option<u64>,
    ) -> Result<VerifiedRead, Failure> {
        self.validate(key, Some(object))?;
        let length = length.unwrap_or_else(|| object.size.saturating_sub(offset));
        let end = offset
            .checked_add(length)
            .filter(|end| offset <= object.size && *end <= object.size)
            .ok_or_else(|| Failure::new(FailureKind::Invalid, "read range"))?;
        self.0
            .reads
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |reads| {
                (reads < self.0.limits.concurrent_reads).then_some(reads + 1)
            })
            .map_err(|_| Failure::new(FailureKind::Capacity, "concurrent read bound"))?;
        let _read = ReadGuard(&self.0.reads);
        if !self.0.objects.is_dir() {
            return Err(Failure::new(
                FailureKind::Unavailable,
                "object directory unavailable",
            ));
        }
        let path = self.object_path(key);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| Failure::lookup("object metadata", &error))?;
        if !metadata.is_file() {
            return Err(Failure::new(FailureKind::Corrupt, "object type"));
        }
        let mut file =
            File::open(path).map_err(|error| Failure::lookup("open physical object", &error))?;
        let mut actual_header = [0; HEADER_SIZE];
        file.read_exact(&mut actual_header)
            .map_err(|error| Failure::io("read object header", &error))?;
        if actual_header != header(key, object) {
            return Err(Failure::new(FailureKind::Corrupt, "object descriptor"));
        }
        if metadata.len() != HEADER_SIZE as u64 + object.size {
            return Err(Failure::new(FailureKind::Corrupt, "object size"));
        }
        let mut result =
            Vec::with_capacity(usize::try_from(length).expect("bounded object length"));
        let mut buffer = vec![0; BUFFER_SIZE];
        let mut hash = Hasher::new(object.algorithm);
        let mut received = 0_u64;
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(|error| Failure::io("read physical object", &error))?;
            if count == 0 {
                break;
            }
            let next = received + count as u64;
            if next > object.size {
                return Err(Failure::new(FailureKind::Corrupt, "object grew"));
            }
            hash.update(&buffer[..count]);
            let from = received.max(offset);
            let until = next.min(end);
            if from < until {
                result.extend_from_slice(
                    &buffer[usize::try_from(from - received).expect("bounded chunk offset")
                        ..usize::try_from(until - received).expect("bounded chunk offset")],
                );
            }
            received = next;
        }
        if received != object.size || hash.finish() != object.hash {
            return Err(Failure::new(FailureKind::Corrupt, "object content"));
        }
        Ok(VerifiedRead {
            key,
            object,
            offset,
            bytes: result,
        })
    }
}

struct ReadGuard<'a>(&'a AtomicUsize);
impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub struct StagedWrite {
    store: PhysicalFilesystem,
    key: PhysicalKey,
    write: u64,
    object: ObjectId,
    path: PathBuf,
    file: Option<File>,
    hash: Hasher,
    bytes: u64,
    failed: bool,
}

impl StagedWrite {
    pub fn append(&mut self, bytes: &[u8]) -> Result<(), Failure> {
        if self.failed {
            return Err(Failure::new(FailureKind::Conflict, "failed staged write"));
        }
        let Some(next) = self
            .bytes
            .checked_add(bytes.len() as u64)
            .filter(|size| *size <= self.object.size)
        else {
            self.failed = true;
            return Err(Failure::new(
                FailureKind::Corrupt,
                "upload size exceeds descriptor",
            ));
        };
        if let Err(error) = self
            .file
            .as_mut()
            .expect("unfinished stage")
            .write_all(bytes)
        {
            self.failed = true;
            return Err(Failure::io("append staged write", &error));
        }
        self.hash.update(bytes);
        self.bytes = next;
        Ok(())
    }

    pub fn finish(mut self) -> Result<WriteReceipt, Failure> {
        if self.failed
            || self.bytes != self.object.size
            || self.hash.clone().finish() != self.object.hash
        {
            return Err(Failure::new(FailureKind::Corrupt, "upload identity"));
        }
        let file = self.file.take().expect("unfinished stage");
        let mut permissions = file
            .metadata()
            .map_err(|error| Failure::io("staged permissions", &error))?
            .permissions();
        permissions.set_readonly(true);
        file.set_permissions(permissions)
            .and_then(|()| file.sync_all())
            .map_err(|error| Failure::io("sync staged object", &error))?;
        drop(file);
        let _guard = self.store.guard()?;
        let destination = self.store.object_path(self.key);
        if !destination.exists() && self.store.count_files(false)? >= self.store.0.limits.objects {
            return Err(Failure::new(FailureKind::Capacity, "object count bound"));
        }
        match fs::hard_link(&self.path, &destination) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                // A retry may observe an acknowledged or ambiguous earlier
                // publication. It must never overwrite a different descriptor.
                self.store
                    .read(self.key, self.object, 0, Some(0))
                    .map_err(|mut failure| {
                        if failure.kind == FailureKind::Corrupt
                            && failure.operation == "object descriptor"
                        {
                            failure.kind = FailureKind::Conflict;
                        }
                        failure
                    })?;
            }
            Err(error) => return Err(Failure::io("publish physical object", &error)),
        }
        sync_directory(&self.store.0.objects)?;
        Ok(WriteReceipt {
            key: self.key,
            write: self.write,
            object: self.object,
        })
    }
}

impl Drop for StagedWrite {
    fn drop(&mut self) {
        drop(self.file.take());
        // This removes only this handle's unpublished name. A successful
        // hardlink publication has independent directory ownership.
        let _cleanup = fs::remove_file(&self.path);
        if let Ok(mut active) = self.store.0.active.lock() {
            active.remove(&(self.key.generation, self.write));
        }
    }
}
