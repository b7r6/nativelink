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

//! Experimental Linux namespace ownership. This never constructs a store,
//! grants admission, fences an old authority, or authorizes worker incarnation
//! reuse. The existing path-based store constructor cannot consume this safely.

#[cfg(not(target_os = "linux"))]
compile_error!("The experimental canonical filesystem placement is Linux only.");

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::OwnedFd;
use std::path::{Component, Path};

use nativelink_canonical_config::{Error, Manifest};
use rustix::fs::{Dir, FlockOperation, Mode, OFlags, flock, fstat, fsync, open, openat};
use serde::{Deserialize, Serialize};

mod capability;
#[cfg(feature = "redb")]
mod journal;
pub use capability::{
    CasDirectory, DigestAlgorithm, DigestLeaf, FixedLayout, PendingWrite, ReadHandle,
    SinkDirectory, TemporaryId,
};
#[cfg(feature = "redb")]
pub use journal::{JOURNAL_FILE, JournalBinding, JournalDirectory, JournalParent, OpenedJournal};

/// Outside the fixed content/temp layout that a store may scan or prune.
pub const OWNERSHIP_RECORD: &str = ".nativelink-namespace-v1.json";
const MAX_RECORD_BYTES: u64 = 64 * 1024;
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum Role {
    Journal,
    Cas,
    TemporaryActionCache,
    Worker,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    device: u64,
    inode: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RootBinding {
    role: Role,
    identity: Identity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    instance: String,
    node: u64,
    epoch: u64,
    authority_namespace: u64,
    principal: String,
    cas_namespace: String,
    sink_namespace: String,
    worker: u64,
    execution_configuration: String,
    roots: Vec<RootBinding>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Record {
    role: Role,
    binding: Binding,
}

#[derive(Debug)]
struct Root {
    role: Role,
    path: String,
    descriptor: OwnedFd,
    ancestors: Vec<Identity>,
}

/// Holds exclusive advisory locks and directory descriptors for four roles.
///
/// All participants must honor these locks. Legacy constructors do not. This
/// guard does not protect against other users of the filesystem, mount changes,
/// or a path-based store reopening a different directory after a rename.
/// Persisted device/inode observations cannot distinguish a replacement after
/// inode reuse or prove physical-directory continuity across restoration.
#[derive(Debug)]
pub struct Ownership {
    roots: Vec<Root>,
    binding: Binding,
}

impl Ownership {
    /// Claim four already existing, empty roots. Preflight checks every root
    /// before creating any record. Record and parent are synced after each write.
    ///
    /// On any write or sync failure, leave all evidence in place. Partial
    /// initialization requires explicit review; neither operation adopts it.
    pub fn claim_cold(manifest: &Manifest) -> Result<Self, Error> {
        let owned = Self::preflight(manifest)?;
        for root in &owned.roots {
            let mut entries =
                Dir::read_from(&root.descriptor).map_err(|_| failure("read_directory"))?;
            for entry in &mut entries {
                let entry = entry.map_err(|_| failure("read_directory"))?;
                if !matches!(entry.file_name().to_bytes(), b"." | b"..") {
                    return Err(failure("cold_root_not_empty"));
                }
            }
        }
        owned.verify_attached()?;
        // Bound all serialized records before the first filesystem mutation.
        let records = owned
            .roots
            .iter()
            .map(|root| {
                let bytes = serde_json::to_vec(&owned.record(root.role))
                    .map_err(|_| failure("encode_record"))?;
                if bytes.len() as u64 > MAX_RECORD_BYTES {
                    return Err(failure("record_too_large"));
                }
                Ok(bytes)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        for (root, bytes) in owned.roots.iter().zip(records) {
            let descriptor = openat(
                &root.descriptor,
                OWNERSHIP_RECORD,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|_| failure("create_record"))?;
            let mut file = File::from(descriptor);
            file.write_all(&bytes)
                .map_err(|_| failure("write_record"))?;
            file.sync_all().map_err(|_| failure("sync_record"))?;
            fsync(&root.descriptor).map_err(|_| failure("sync_directory"))?;
        }
        owned.verify_attached()?;
        Ok(owned)
    }

    /// Reopen matching namespace records, never execution authority.
    ///
    /// All four currently readable records must agree with the requested
    /// domain/roles and opened device/inode observations. This operation does
    /// not sync records: matching records can remain after a failed cold-claim
    /// sync and do not prove historical durability or directory continuity.
    /// This does not validate data, prepare roots for a legacy store, restore
    /// authority, or admit a worker incarnation.
    pub fn reopen(manifest: &Manifest) -> Result<Self, Error> {
        let owned = Self::preflight(manifest)?;
        for root in &owned.roots {
            let descriptor = openat(
                &root.descriptor,
                OWNERSHIP_RECORD,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| failure("open_record"))?;
            let file = File::from(descriptor);
            let metadata = file.metadata().map_err(|_| failure("stat_record"))?;
            if !metadata.is_file() || metadata.len() > MAX_RECORD_BYTES {
                return Err(failure("invalid_record"));
            }
            let mut bytes = Vec::new();
            file.take(MAX_RECORD_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| failure("read_record"))?;
            if bytes.len() as u64 > MAX_RECORD_BYTES {
                return Err(failure("invalid_record"));
            }
            let record: Record =
                serde_json::from_slice(&bytes).map_err(|_| failure("invalid_record"))?;
            if record != owned.record(root.role) {
                return Err(failure("ownership_mismatch"));
            }
        }
        owned.verify_attached()?;
        Ok(owned)
    }

    /// Detect a path replacement at this instant. This cannot make a later
    /// path-based constructor race-free. Descriptors remain pinned after rename.
    pub fn verify_attached(&self) -> Result<(), Error> {
        for root in &self.roots {
            let current = open_root(root.role, &root.path)?;
            if identity(&current.descriptor)? != identity(&root.descriptor)? {
                return Err(failure("root_replaced"));
            }
        }
        Ok(())
    }

    fn preflight(manifest: &Manifest) -> Result<Self, Error> {
        // The schema gate precedes all I/O. All retained configuration is owned.
        manifest.validate()?;
        let roots = [
            (Role::Journal, &manifest.authority.journal_root),
            (Role::Cas, &manifest.cas.root),
            (
                Role::TemporaryActionCache,
                &manifest.temporary_action_cache.root,
            ),
            (Role::Worker, &manifest.worker.root),
        ]
        .into_iter()
        .map(|(role, path)| open_root(role, path))
        .collect::<Result<Vec<_>, _>>()?;
        for (index, root) in roots.iter().enumerate() {
            let here = identity(&root.descriptor)?;
            for other in &roots[index + 1..] {
                let there = identity(&other.descriptor)?;
                if root.ancestors.contains(&there) || other.ancestors.contains(&here) {
                    return Err(failure("physical_overlap"));
                }
            }
        }
        for root in &roots {
            flock(&root.descriptor, FlockOperation::NonBlockingLockExclusive)
                .map_err(|_| failure("exclusive_ownership_unavailable"))?;
        }
        let binding = Binding {
            version: 1,
            instance: manifest.instance_name.clone(),
            node: manifest.authority.node_id,
            epoch: manifest.authority.cluster_epoch,
            authority_namespace: manifest.authority.namespace,
            principal: manifest.authority.principal.clone(),
            cas_namespace: manifest.cas.namespace_id.clone(),
            sink_namespace: manifest.temporary_action_cache.namespace_id.clone(),
            worker: manifest.worker.worker_id,
            execution_configuration: manifest.worker.execution_configuration_id.clone(),
            roots: roots
                .iter()
                .map(|root| {
                    Ok(RootBinding {
                        role: root.role,
                        identity: identity(&root.descriptor)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
        };
        let owned = Self { roots, binding };
        owned.verify_attached()?;
        Ok(owned)
    }

    fn record(&self, role: Role) -> Record {
        Record {
            role,
            binding: self.binding.clone(),
        }
    }
}

fn open_root(role: Role, path: &str) -> Result<Root, Error> {
    let mut descriptor =
        open("/", DIRECTORY_FLAGS, Mode::empty()).map_err(|_| failure("open_root"))?;
    let mut ancestors = vec![identity(&descriptor)?];
    for component in Path::new(path).components() {
        if let Component::Normal(name) = component {
            descriptor = openat(&descriptor, name, DIRECTORY_FLAGS, Mode::empty())
                .map_err(|_| failure("unsafe_or_unavailable_path"))?;
            ancestors.push(identity(&descriptor)?);
        }
    }
    Ok(Root {
        role,
        path: path.to_owned(),
        descriptor,
        ancestors,
    })
}

fn identity(descriptor: &OwnedFd) -> Result<Identity, Error> {
    let stat = fstat(descriptor).map_err(|_| failure("stat_directory"))?;
    if stat.st_nlink == 0 {
        return Err(failure("unlinked_directory"));
    }
    Ok(Identity {
        device: stat.st_dev,
        inode: stat.st_ino,
    })
}

fn failure(code: &'static str) -> Error {
    Error::new(
        code,
        "",
        "Experimental namespace ownership refused; no store was constructed.",
    )
}

#[cfg(test)]
mod tests;
