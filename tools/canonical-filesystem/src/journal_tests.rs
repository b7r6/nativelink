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

#![allow(
    clippy::significant_drop_tightening,
    reason = "The tests deliberately control database, transaction and ownership lifetimes."
)]

use std::fs;
use std::os::unix::fs::symlink;
use std::path::Path;

use redb::{Durability, ReadableDatabase, TableDefinition};
use rustix::fs::{FlockOperation, flock, mkfifoat};

use super::*;
use crate::tests::Fixture;

const TABLE: TableDefinition<&str, u64> = TableDefinition::new("test");

fn binding(fixture: &Fixture) -> JournalBinding {
    JournalBinding {
        node_id: fixture.manifest.authority.node_id,
        epoch: fixture.manifest.authority.cluster_epoch,
        namespace: fixture.manifest.authority.namespace,
    }
}

fn directory(fixture: &Fixture, owner: &Arc<Ownership>) -> JournalDirectory {
    JournalDirectory::new(owner, binding(fixture)).unwrap()
}

fn write_value(database: &Database) {
    let mut transaction = database.begin_write().unwrap();
    transaction.set_durability(Durability::Immediate).unwrap();
    transaction
        .open_table(TABLE)
        .unwrap()
        .insert("value", 42)
        .unwrap();
    transaction.commit().unwrap();
}

fn assert_value(database: &Database) {
    let transaction = database.begin_read().unwrap();
    let table = transaction.open_table(TABLE).unwrap();
    assert_eq!(table.get("value").unwrap().unwrap().value(), 42);
}

fn assert_all_roots_locked(fixture: &Fixture) {
    for root in [
        &fixture.manifest.authority.journal_root,
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
        &fixture.manifest.worker.root,
    ] {
        let file = File::open(root).unwrap();
        assert_eq!(
            flock(&file, FlockOperation::NonBlockingLockExclusive),
            Err(rustix::io::Errno::WOULDBLOCK),
            "role root was released: {root}",
        );
    }
}

#[test]
fn binding_mismatch_precedes_file_creation() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let expected = binding(&fixture);
    for mismatch in [
        JournalBinding {
            node_id: expected.node_id + 1,
            ..expected
        },
        JournalBinding {
            epoch: expected.epoch + 1,
            ..expected
        },
        JournalBinding {
            namespace: expected.namespace + 1,
            ..expected
        },
    ] {
        assert_eq!(
            JournalDirectory::new(&owner, mismatch).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
    assert!(
        !Path::new(&fixture.manifest.authority.journal_root)
            .join(JOURNAL_FILE)
            .exists()
    );
    assert_eq!(directory(&fixture, &owner).binding(), expected);
}

#[test]
fn database_alone_retains_every_role_and_redb_exclusion() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.create().unwrap().into_parts();
    write_value(&database);
    parent.unwrap().sync_all().unwrap();
    let path = Path::new(&fixture.manifest.authority.journal_root).join(JOURNAL_FILE);
    assert!(Database::open(&path).is_err());
    assert!(journal.reopen().is_err());
    drop(journal);
    drop(owner);
    assert_all_roots_locked(&fixture);
    assert_value(&database);
    drop(database);
    let owner = Arc::new(Ownership::reopen(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.reopen().unwrap().into_parts();
    assert!(parent.is_none());
    assert_value(&database);
}

#[test]
fn live_transactions_retain_every_role_after_database_drop() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.create().unwrap().into_parts();
    write_value(&database);
    parent.unwrap().sync_all().unwrap();
    let read = database.begin_read().unwrap();
    let write = database.begin_write().unwrap();
    drop(database);
    drop(journal);
    drop(owner);
    assert_all_roots_locked(&fixture);
    write.commit().unwrap();
    assert_all_roots_locked(&fixture);
    assert_eq!(
        read.open_table(TABLE)
            .unwrap()
            .get("value")
            .unwrap()
            .unwrap()
            .value(),
        42
    );
    drop(read);
    Ownership::reopen(&fixture.manifest).unwrap();
}

#[test]
fn parent_token_retains_every_role_until_sync_completes() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.create().unwrap().into_parts();
    write_value(&database);
    drop(database);
    drop(journal);
    drop(owner);
    assert_all_roots_locked(&fixture);
    parent.unwrap().sync_all().unwrap();
    Ownership::reopen(&fixture.manifest).unwrap();
}

#[test]
fn parent_sync_result_preserves_initialized_file() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.create().unwrap().into_parts();
    write_value(&database);
    let result = parent.unwrap().sync_all();
    // The same compiled test is also run under a recorded strace fsync EIO
    // injection. Ordinary Cargo/Bazel execution exercises the successful path.
    if std::env::var_os("NL_JOURNAL_TEST_EXPECT_PARENT_SYNC_EIO").is_some() {
        assert_eq!(result.unwrap_err().raw_os_error(), Some(5));
    } else {
        result.unwrap();
    }
    assert!(
        Path::new(&fixture.manifest.authority.journal_root)
            .join(JOURNAL_FILE)
            .is_file()
    );
    assert_value(&database);
    drop(database);
    let (database, parent) = journal.reopen().unwrap().into_parts();
    assert!(parent.is_none());
    assert_value(&database);
}

#[test]
fn missing_empty_and_corrupt_reopen_never_initialize_or_remove() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let path = Path::new(&fixture.manifest.authority.journal_root).join(JOURNAL_FILE);
    assert_eq!(
        journal.reopen().unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(!path.exists());
    for bytes in [b"".as_slice(), b"retained invalid journal".as_slice()] {
        fs::write(&path, bytes).unwrap();
        assert!(journal.reopen().is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert!(journal.create().is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    drop(journal);
    drop(owner);
    Ownership::reopen(&fixture.manifest).unwrap();
}

#[test]
fn empty_file_is_rechecked_by_redb_after_backend_open() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.create().unwrap().into_parts();
    write_value(&database);
    parent.unwrap().sync_all().unwrap();
    drop(database);
    let backend = journal.open_backend(false).unwrap();
    assert!(backend.len().unwrap() > 0);
    let path = Path::new(&fixture.manifest.authority.journal_root).join(JOURNAL_FILE);
    fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(0)
        .unwrap();
    assert!(Database::builder().create_with_backend(backend).is_err());
    assert_eq!(fs::metadata(&path).unwrap().len(), 0);
}

#[test]
fn symbolic_nonregular_and_multiply_linked_files_refuse() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let path = Path::new(&fixture.manifest.authority.journal_root).join(JOURNAL_FILE);
    let outside = fixture.directory.join("outside");
    fs::write(&outside, b"outside retained").unwrap();
    symlink(&outside, &path).unwrap();
    assert!(journal.reopen().is_err());
    fs::remove_file(&path).unwrap();
    fs::hard_link(&outside, &path).unwrap();
    assert!(journal.reopen().is_err());
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(journal.reopen().is_err());
    fs::remove_dir(&path).unwrap();
    mkfifoat(journal_root(&owner), JOURNAL_FILE, Mode::RUSR | Mode::WUSR).unwrap();
    assert!(journal.reopen().is_err());
    assert_eq!(fs::read(&outside).unwrap(), b"outside retained");
}

#[test]
fn creation_and_parent_sync_remain_on_renamed_root() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let old = fixture.directory.join("detached-journal");
    fs::rename(&fixture.manifest.authority.journal_root, &old).unwrap();
    fs::create_dir(&fixture.manifest.authority.journal_root).unwrap();
    let replacement = Path::new(&fixture.manifest.authority.journal_root).join(JOURNAL_FILE);
    fs::write(&replacement, b"replacement untouched").unwrap();
    let (database, parent) = journal.create().unwrap().into_parts();
    write_value(&database);
    parent.unwrap().sync_all().unwrap();
    assert!(old.join(JOURNAL_FILE).is_file());
    assert_eq!(fs::read(&replacement).unwrap(), b"replacement untouched");
    assert_value(&database);
    drop(database);
    let (database, parent) = journal.reopen().unwrap().into_parts();
    assert!(parent.is_none());
    assert_value(&database);
}

#[test]
fn every_backend_lock_operation_and_close_is_delegated() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let journal = directory(&fixture, &owner);
    let (database, parent) = journal.create().unwrap().into_parts();
    parent.unwrap().sync_all().unwrap();
    drop(database);
    let left = journal.open_backend(false).unwrap();
    let right = journal.open_backend(false).unwrap();
    let range = (Bound::Included(4096), Bound::Excluded(8192));
    assert!(left.try_lock_range(range.0, range.1).unwrap());
    assert!(right.query_lock_range(range.0, range.1).unwrap());
    assert!(!right.try_lock_range(range.0, range.1).unwrap());
    assert!(!right.try_lock_shared_range(range.0, range.1).unwrap());
    left.unlock_range(range.0, range.1).unwrap();
    left.lock_shared_range(range.0, range.1).unwrap();
    assert!(right.try_lock_shared_range(range.0, range.1).unwrap());
    right.unlock_range(range.0, range.1).unwrap();
    left.unlock_range(range.0, range.1).unwrap();
    left.lock_range(range.0, range.1).unwrap();
    assert!(right.query_lock_range(range.0, range.1).unwrap());
    left.close().unwrap();
    assert!(right.try_lock_range(range.0, range.1).unwrap());
    right.close().unwrap();
    assert!(!left.query_lock_range(range.0, range.1).unwrap());
}
