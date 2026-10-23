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

//! Descriptor-relative first-digest-layout operations, without a path escape.
use std::fmt::{self, Write as _};
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::OwnedFd;
use std::sync::Arc;

use nativelink_canonical_config::Error;
use rustix::fs::{
    AtFlags, Dir, FileType, Mode, OFlags, RenameFlags, Stat, fstat, fsync, mkdirat, openat,
    renameat, renameat_with, statat, unlinkat,
};
use rustix::io::Errno;

use super::{DIRECTORY_FLAGS, OWNERSHIP_RECORD, Ownership, Role, failure};

const CHILDREN: [&str; 3] = ["content", "temp", "content.exec"];

/// Algorithms admitted by the experimental digest layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DigestAlgorithm {
    Sha256,
    Blake3,
}

/// A single canonical filename, constructed without accepting a path.
/// This identifies a digest slot, not a non-reused physical generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DigestLeaf(String);

impl DigestLeaf {
    pub fn new(algorithm: DigestAlgorithm, hash: [u8; 32], size: u64) -> Self {
        let prefix = match algorithm {
            DigestAlgorithm::Sha256 => "sha256",
            DigestAlgorithm::Blake3 => "blake3",
        };
        let mut name = format!("{prefix}-");
        for byte in hash {
            write!(&mut name, "{byte:02x}").expect("writing into a String cannot fail");
        }
        write!(&mut name, "-{size}").expect("writing into a String cannot fail");
        Self(name)
    }

    /// Accept only the exact encoding emitted by `new`, including lowercase
    /// hex, canonical decimal size, and one known algorithm. No path is accepted.
    pub fn parse(name: &str) -> Result<Self, Error> {
        if !(73..=92).contains(&name.len()) {
            return Err(failure("invalid_digest_leaf"));
        }
        let mut fields = name.split('-');
        let algorithm = match fields.next() {
            Some("sha256") => DigestAlgorithm::Sha256,
            Some("blake3") => DigestAlgorithm::Blake3,
            _ => return Err(failure("invalid_digest_leaf")),
        };
        let hash = fields
            .next()
            .ok_or_else(|| failure("invalid_digest_leaf"))?;
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(failure("invalid_digest_leaf"));
        }
        let size = fields
            .next()
            .ok_or_else(|| failure("invalid_digest_leaf"))?;
        let size = size
            .parse::<u64>()
            .map_err(|_| failure("invalid_digest_leaf"))?;
        if fields.next().is_some() {
            return Err(failure("invalid_digest_leaf"));
        }
        let mut bytes = [0; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16)
                .map_err(|_| failure("invalid_digest_leaf"))?;
        }
        let canonical = Self::new(algorithm, bytes, size);
        if canonical.0 != name {
            return Err(failure("invalid_digest_leaf"));
        }
        Ok(canonical)
    }

    /// The leaf encoding only; no ambient root path is available from this API.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Caller-chosen temporary identity. Collision is an exclusive-create error;
/// this library neither invents persistent generations nor adopts old files.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporaryId(pub u128);

impl TemporaryId {
    fn leaf(self) -> String {
        format!("pending-{:032x}", self.0)
    }
}

struct Directories {
    content: OwnedFd,
    temporary: OwnedFd,
    executable: OwnedFd,
    // Every descendant handle retains all four role locks, including when moved
    // into a blocking closure after the layout and original caller have dropped.
    // Declared last so the child descriptors close before the final lock release.
    ownership: Arc<Ownership>,
}

/// Fixed CAS and mutable sink capabilities.
///
/// Child descriptors stay pinned when
/// ancestors, role roots or children are renamed. Their original directories
/// may then be outside the configured ambient path: no path-containment claim.
pub struct FixedLayout {
    cas: CasDirectory,
    sink: SinkDirectory,
}

impl FixedLayout {
    /// Create six fixed child directories after checking that BOTH data roots
    /// contain only their existing ownership record. No rollback on partial I/O.
    /// This does not sync directories or establish durable initialization.
    pub fn create(ownership: &Arc<Ownership>) -> Result<Self, Error> {
        for role in [Role::Cas, Role::TemporaryActionCache] {
            let root = role_root(ownership, role);
            let mut entries = Dir::read_from(root).map_err(|_| failure("read_layout_root"))?;
            for entry in &mut entries {
                let entry = entry.map_err(|_| failure("read_layout_root"))?;
                if !matches!(entry.file_name().to_bytes(), b"." | b"..")
                    && entry.file_name().to_bytes() != OWNERSHIP_RECORD.as_bytes()
                {
                    return Err(failure("layout_root_not_empty"));
                }
            }
        }
        let cas = open_directories(ownership, Role::Cas, true)?;
        let sink = open_directories(ownership, Role::TemporaryActionCache, true)?;
        Ok(Self {
            cas: CasDirectory(cas),
            sink: SinkDirectory(sink),
        })
    }

    /// Open every fixed child with NOFOLLOW and DIRECTORY; missing, symbolic or
    /// non-directory children fail. No creation, scanning, pruning or migration.
    pub fn reopen(ownership: &Arc<Ownership>) -> Result<Self, Error> {
        let cas = open_directories(ownership, Role::Cas, false)?;
        let sink = open_directories(ownership, Role::TemporaryActionCache, false)?;
        Ok(Self {
            cas: CasDirectory(cas),
            sink: SinkDirectory(sink),
        })
    }

    pub fn cas(&self) -> CasDirectory {
        self.cas.clone()
    }

    pub fn sink(&self) -> SinkDirectory {
        self.sink.clone()
    }

    /// Sync all fixed children, then both pinned data-role parents. This exposes
    /// parent-entry synchronization after creation without claiming historical
    /// initialization or power-loss recovery. Failure preserves all artifacts.
    pub fn sync_layout(&self) -> Result<(), Error> {
        self.cas.sync_directories()?;
        self.sink.sync_directories()?;
        for role in [Role::Cas, Role::TemporaryActionCache] {
            fsync(role_root(&self.cas.0.ownership, role))
                .map_err(|_| failure("sync_layout_parent"))?;
        }
        Ok(())
    }
}

fn role_root(ownership: &Ownership, role: Role) -> &OwnedFd {
    &ownership
        .roots
        .iter()
        .find(|root| root.role == role)
        .expect("Ownership always retains all four roles")
        .descriptor
}

fn open_directories(
    ownership: &Arc<Ownership>,
    role: Role,
    create: bool,
) -> Result<Arc<Directories>, Error> {
    let root = role_root(ownership, role);
    let mut opened = Vec::new();
    for name in CHILDREN {
        if create {
            mkdirat(root, name, Mode::RUSR | Mode::WUSR | Mode::XUSR)
                .map_err(|_| failure("create_layout_child"))?;
        }
        opened.push(
            openat(root, name, DIRECTORY_FLAGS, Mode::empty())
                .map_err(|_| failure("unsafe_or_unavailable_layout_child"))?,
        );
    }
    let [content, temporary, executable]: [OwnedFd; 3] = opened
        .try_into()
        .expect("exactly three fixed children were opened");
    Ok(Arc::new(Directories {
        ownership: Arc::clone(ownership),
        content,
        temporary,
        executable,
    }))
}

/// Immutable digest-slot publication: an existing destination is always conflict.
#[derive(Clone)]
pub struct CasDirectory(Arc<Directories>);

/// Mutable temporary AC sink publication, with atomic replacement of a regular
/// destination. This is a filesystem slot, not authority to accept an AC result.
#[derive(Clone)]
pub struct SinkDirectory(Arc<Directories>);

macro_rules! common_directory_operations {
    ($kind:ident) => {
        impl $kind {
            pub fn read(&self, leaf: &DigestLeaf) -> Result<ReadHandle, Error> {
                self.0.read(leaf)
            }

            pub fn create_exclusive(&self, id: TemporaryId) -> Result<PendingWrite, Error> {
                self.0.create_exclusive(id)
            }

            /// Remove a currently observed regular slot; absence returns false.
            /// Callers must serialize slot mutations. This is not compare-and-
            /// unlink and supplies no retention or collector authorization.
            pub fn remove(&self, leaf: &DigestLeaf) -> Result<bool, Error> {
                self.0.remove(leaf)
            }

            /// Explicit directory sync only. The caller must order file sync,
            /// publication and recovery; success is not a power-loss proof.
            pub fn sync_directories(&self) -> Result<(), Error> {
                self.0.sync_directories()
            }
        }
    };
}
common_directory_operations!(CasDirectory);
common_directory_operations!(SinkDirectory);

impl CasDirectory {
    pub fn publish(&self, pending: PendingWrite, leaf: &DigestLeaf) -> Result<ReadHandle, Error> {
        self.0.publish(pending, leaf, false)
    }
}

impl SinkDirectory {
    pub fn replace(&self, pending: PendingWrite, leaf: &DigestLeaf) -> Result<ReadHandle, Error> {
        self.0.publish(pending, leaf, true)
    }
}

impl Directories {
    fn read(self: &Arc<Self>, leaf: &DigestLeaf) -> Result<ReadHandle, Error> {
        let observed =
            entry(&self.content, leaf.as_str())?.ok_or_else(|| failure("missing_slot"))?;
        regular(&observed)?;
        let descriptor = openat(
            &self.content,
            leaf.as_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| failure("open_slot"))?;
        let opened = fstat(&descriptor).map_err(|_| failure("stat_slot"))?;
        regular(&opened)?;
        same_file(&observed, &opened)?;
        Ok(ReadHandle {
            file: File::from(descriptor),
            _directories: Arc::clone(self),
        })
    }

    fn create_exclusive(self: &Arc<Self>, id: TemporaryId) -> Result<PendingWrite, Error> {
        let name = id.leaf();
        let descriptor = openat(
            &self.temporary,
            name.as_str(),
            OFlags::RDWR
                | OFlags::CREATE
                | OFlags::EXCL
                | OFlags::NOFOLLOW
                | OFlags::NONBLOCK
                | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )
        .map_err(|_| failure("create_temporary"))?;
        regular(&fstat(&descriptor).map_err(|_| failure("stat_temporary"))?)?;
        Ok(PendingWrite {
            file: File::from(descriptor),
            name,
            directories: Arc::clone(self),
        })
    }

    fn publish(
        self: &Arc<Self>,
        mut pending: PendingWrite,
        leaf: &DigestLeaf,
        replace: bool,
    ) -> Result<ReadHandle, Error> {
        if !Arc::ptr_eq(self, &pending.directories) {
            return Err(failure("different_layout_capability"));
        }
        // Seek before rename so no fallible post-publication wrapper operation
        // can obscure whether the rename happened. No automatic cleanup exists.
        pending
            .file
            .rewind()
            .map_err(|_| failure("seek_temporary"))?;
        let opened = fstat(&pending.file).map_err(|_| failure("stat_temporary"))?;
        regular(&opened)?;
        let named =
            entry(&self.temporary, &pending.name)?.ok_or_else(|| failure("missing_temporary"))?;
        regular(&named)?;
        same_file(&opened, &named)?;
        if let Some(destination) = entry(&self.content, leaf.as_str())? {
            if !replace {
                return Err(failure("slot_exists"));
            }
            // This is a precheck under cooperative ownership. A hostile writer
            // can replace the entry between stat and rename; rename never follows
            // a destination symlink, but this is not atomic type enforcement.
            regular(&destination)?;
        }
        if replace {
            renameat(
                &self.temporary,
                pending.name.as_str(),
                &self.content,
                leaf.as_str(),
            )
            .map_err(|_| failure("publish_slot"))?;
        } else {
            renameat_with(
                &self.temporary,
                pending.name.as_str(),
                &self.content,
                leaf.as_str(),
                RenameFlags::NOREPLACE,
            )
            .map_err(|error| {
                failure(if error == Errno::EXIST {
                    "slot_exists"
                } else {
                    "publish_slot"
                })
            })?;
        }
        // PendingWrite is neither cloneable nor convertible to a File/FD. Once
        // consumed, the only returned interface can read/seek, never write.
        Ok(ReadHandle {
            file: pending.file,
            _directories: pending.directories,
        })
    }

    fn remove(&self, leaf: &DigestLeaf) -> Result<bool, Error> {
        let Some(observed) = entry(&self.content, leaf.as_str())? else {
            return Ok(false);
        };
        regular(&observed)?;
        unlinkat(&self.content, leaf.as_str(), AtFlags::empty())
            .map_err(|_| failure("remove_slot"))?;
        Ok(true)
    }

    fn sync_directories(&self) -> Result<(), Error> {
        // Keep the owning Arc borrowed across every sync. This is intentionally
        // separate from publication: visibility and durable commit are distinct.
        for descriptor in [&self.temporary, &self.content, &self.executable] {
            fsync(descriptor).map_err(|_| failure("sync_layout_directory"))?;
        }
        Ok(())
    }
}

fn entry(directory: &OwnedFd, name: &str) -> Result<Option<Stat>, Error> {
    match statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) => Ok(Some(stat)),
        Err(Errno::NOENT) => Ok(None),
        Err(_) => Err(failure("stat_slot")),
    }
}

fn regular(stat: &Stat) -> Result<(), Error> {
    if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile || stat.st_nlink != 1 {
        return Err(failure("unsafe_slot_type_or_links"));
    }
    Ok(())
}

fn same_file(left: &Stat, right: &Stat) -> Result<(), Error> {
    if (left.st_dev, left.st_ino) != (right.st_dev, right.st_ino) {
        return Err(failure("slot_replaced"));
    }
    Ok(())
}

/// Unique writable staging handle. Drop closes it and preserves the temporary
/// entry for review/recovery; it never unlinks a possibly replaced name.
pub struct PendingWrite {
    file: File,
    name: String,
    directories: Arc<Directories>,
}

impl PendingWrite {
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

impl Write for PendingWrite {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for PendingWrite {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}

/// Read-only interface retaining every role lock. It deliberately offers no
/// `File`, raw descriptor, clone, or path conversion to a caller.
pub struct ReadHandle {
    file: File,
    _directories: Arc<Directories>,
}

impl Read for ReadHandle {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.file.read(bytes)
    }
}

impl Seek for ReadHandle {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}

macro_rules! opaque_debug {
    ($($kind:ident),+) => { $(
        impl fmt::Debug for $kind {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.debug_struct(stringify!($kind)).finish_non_exhaustive()
            }
        }
    )+ };
}
opaque_debug!(
    FixedLayout,
    CasDirectory,
    SinkDirectory,
    PendingWrite,
    ReadHandle
);
