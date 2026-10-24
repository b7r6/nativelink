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

//! A fixed journal file whose redb backend retains namespace ownership.
use std::fs::File;
use std::io;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use redb::backends::FileBackend;
use redb::{BackendError, Database, StorageBackend};
use rustix::fs::{Dir, FileType, Mode, OFlags, fstat, fsync, openat};

use super::{OWNERSHIP_RECORD, Ownership, Role};

/// Experimental layout name, not a production format or migration promise.
pub const JOURNAL_FILE: &str = "authority.redb";

/// Authority facts recorded by all four owned roots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalBinding {
    pub node_id: u64,
    pub epoch: u64,
    pub namespace: u64,
}

/// Opening authority for one fixed leaf. No ambient path or descriptor escapes.
#[derive(Debug)]
pub struct JournalDirectory {
    ownership: Arc<Ownership>,
}

impl JournalDirectory {
    /// Check expected authority facts before any journal file can be opened.
    pub fn new(ownership: &Arc<Ownership>, expected: JournalBinding) -> io::Result<Self> {
        let actual = JournalBinding {
            node_id: ownership.binding.node,
            epoch: ownership.binding.epoch,
            namespace: ownership.binding.authority_namespace,
        };
        if expected != actual {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal ownership binding mismatch",
            ));
        }
        Ok(Self {
            ownership: Arc::clone(ownership),
        })
    }

    pub fn binding(&self) -> JournalBinding {
        JournalBinding {
            node_id: self.ownership.binding.node,
            epoch: self.ownership.binding.epoch,
            namespace: self.ownership.binding.authority_namespace,
        }
    }

    /// Exclusively create a new redb file in a root containing only its record.
    /// The caller must initialize authority metadata durably, then sync the
    /// returned parent. Failures preserve artifacts; no rollback or adoption.
    pub fn create(&self) -> io::Result<OpenedJournal> {
        let root = journal_root(&self.ownership);
        for entry in Dir::read_from(root)? {
            let entry = entry?;
            if !matches!(entry.file_name().to_bytes(), b"." | b"..")
                && entry.file_name().to_bytes() != OWNERSHIP_RECORD.as_bytes()
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "journal root contains prior artifacts",
                ));
            }
        }
        self.open(true)
    }

    /// Open an existing regular, nonempty redb file; never create or initialize
    /// a missing or empty file. Redb may repair an existing database on opening.
    pub fn reopen(&self) -> io::Result<OpenedJournal> {
        self.open(false)
    }

    fn open(&self, created: bool) -> io::Result<OpenedJournal> {
        let backend = self.open_backend(created)?;
        let repair_observed = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&repair_observed);
        let database = Database::builder()
            .set_repair_callback(move |_| observed.store(true, Ordering::Relaxed))
            .create_with_backend(backend)
            .map_err(io::Error::other)?;
        Ok(OpenedJournal {
            database,
            parent: JournalParent(Arc::clone(&self.ownership)),
            created,
            repair_observed: repair_observed.load(Ordering::Relaxed),
        })
    }

    fn open_backend(&self, created: bool) -> io::Result<OwnedBackend> {
        let mut flags = OFlags::RDWR | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        if created {
            flags |= OFlags::CREATE | OFlags::EXCL;
        }
        let descriptor = openat(
            journal_root(&self.ownership),
            JOURNAL_FILE,
            flags,
            Mode::RUSR | Mode::WUSR,
        )?;
        let metadata = fstat(&descriptor)?;
        if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile
            || metadata.st_nlink != 1
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal must be a regular file with one link",
            ));
        }
        Ok(OwnedBackend {
            backend: FileBackend::new(File::from(descriptor)).map_err(io::Error::other)?,
            created,
            _ownership: Arc::clone(&self.ownership),
        })
    }
}

fn journal_root(ownership: &Ownership) -> &std::os::fd::OwnedFd {
    &ownership
        .roots
        .iter()
        .find(|root| root.role == Role::Journal)
        .expect("Ownership always retains the journal role")
        .descriptor
}

/// A database with ownership held inside its backend and pinned parent custody.
/// Creation provenance is separate; splitting cannot detach database ownership.
#[derive(Debug)]
pub struct OpenedJournal {
    database: Database,
    parent: JournalParent,
    created: bool,
    repair_observed: bool,
}

impl OpenedJournal {
    /// Whether redb invoked its fixed internal repair observer during this open.
    /// Observation does not certify authority metadata or a current sync barrier.
    pub const fn repair_observed(&self) -> bool {
        self.repair_observed
    }

    /// Pinned parent custody for a current sync, regardless of creation status.
    /// This is never evidence that the database was exclusively created.
    pub fn current_parent(&self) -> JournalParent {
        JournalParent(Arc::clone(&self.parent.0))
    }

    pub fn into_parts(self) -> (Database, Option<JournalParent>) {
        (self.database, self.created.then_some(self.parent))
    }
}

/// Synchronization of the pinned journal root, independent of ambient renames.
#[derive(Debug)]
pub struct JournalParent(Arc<Ownership>);

impl JournalParent {
    /// Recheck current records and configured attachment against pinned custody.
    pub fn verify_current(&self) -> io::Result<()> {
        self.0.verify_current_records()
    }

    /// Sync the pinned parent between current namespace checks. No ambient
    /// replacement is followed, and failure never grants a successful barrier.
    pub fn sync_current(self) -> io::Result<()> {
        self.verify_current()?;
        fsync(journal_root(&self.0))?;
        self.verify_current()
    }

    pub fn sync_all(self) -> io::Result<()> {
        fsync(journal_root(&self.0))?;
        Ok(())
    }
}

// Do not expose or clone the FileBackend: every redb clone/transaction must keep
// the owner with its storage. Declaration order closes the file before releasing
// the final namespace locks. All redb locking behavior remains delegated.
#[derive(Debug)]
struct OwnedBackend {
    backend: FileBackend,
    created: bool,
    _ownership: Arc<Ownership>,
}

impl StorageBackend for OwnedBackend {
    fn len(&self) -> io::Result<u64> {
        let length = self.backend.len()?;
        // Redb calls len after taking its opening locks. Checking only metadata
        // before handing over the backend would leave an empty-file race.
        if length == 0 && !self.created {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "existing journal is empty",
            ));
        }
        Ok(length)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.backend.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.backend.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.backend.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.backend.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.backend.close()
    }

    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.backend.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.backend.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.backend.query_lock_range(start, end)
    }
}

#[cfg(test)]
#[path = "journal_tests.rs"]
mod tests;
