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

//! Tests-first contract for a PRIVATE worker filesystem slice. The worker
//! module does not exist in this checkpoint. Missing imports are not semantic
//! failure evidence. No digest validation or production worker is qualified.

use std::io::{self, Cursor, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use rustix::fs::{FileType, Timespec, mknodat};

use super::*;
use crate::worker::{
    ActionId, EntryName, InputId, OwnedWorkerFs, QUALIFICATION_MAX_INPUT_BYTES, TestIoFault,
    WorkerFileMetadata,
};

const WAIT: Duration = Duration::from_secs(5);
const COPY_CHUNK: usize = 8192;

fn setup(fixture: &Fixture) -> (Arc<Ownership>, OwnedWorkerFs) {
    let owned = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let worker = OwnedWorkerFs::create(&owned).unwrap();
    (owned, worker)
}

fn root(fixture: &Fixture) -> PathBuf {
    PathBuf::from(&fixture.manifest.worker.root)
}

fn input_name(id: u128) -> String {
    format!("input-{id:032x}")
}

fn pending_name(id: u128) -> String {
    format!("pending-{id:032x}")
}

fn action_name(id: u128) -> String {
    format!("action-{id:032x}")
}

fn leaf(name: &str) -> EntryName {
    EntryName::new(name).unwrap()
}

fn input_path(fixture: &Fixture, id: u128) -> PathBuf {
    root(fixture).join("inputs").join(input_name(id))
}

fn pending_path(fixture: &Fixture, id: u128) -> PathBuf {
    root(fixture).join("staging").join(pending_name(id))
}

fn action_path(fixture: &Fixture, id: u128) -> PathBuf {
    root(fixture).join("actions").join(action_name(id))
}

fn assert_error<T>(result: Result<T, Error>, expected: &str) {
    match result {
        Ok(_) => panic!("accepted operation that must refuse with {expected}"),
        Err(error) => assert_eq!(error.code, expected),
    }
}

fn held(fixture: &Fixture) {
    for path in fixture.roots() {
        let descriptor = open(path, DIRECTORY_FLAGS, Mode::empty()).unwrap();
        assert_eq!(
            flock(&descriptor, FlockOperation::NonBlockingLockExclusive),
            Err(rustix::io::Errno::WOULDBLOCK),
            "worker descendant released role lock: {path}"
        );
    }
}

fn released(fixture: &Fixture) {
    drop(Ownership::reopen(&fixture.manifest).unwrap());
}

fn metadata(path: &Path) -> (u64, u64, u64, u32, i64, i64) {
    let stat = fs::metadata(path).unwrap();
    (
        stat.dev(),
        stat.ino(),
        stat.nlink(),
        stat.mode() & 0o7777,
        stat.mtime(),
        stat.mtime_nsec(),
    )
}

fn records(fixture: &Fixture) -> Vec<Vec<u8>> {
    (0..4)
        .map(|i| fs::read(fixture.record(i)).unwrap())
        .collect()
}

#[test]
fn worker_cold_layout_preserves_records_and_rejects_second_creation() {
    let fixture = Fixture::new();
    let (owned, worker) = setup(&fixture);
    let before = records(&fixture);
    for child in ["inputs", "staging", "actions"] {
        assert!(root(&fixture).join(child).is_dir());
        assert_eq!(fs::read_dir(root(&fixture).join(child)).unwrap().count(), 0);
    }
    assert_error(OwnedWorkerFs::create(&owned), "worker_root_not_empty");
    assert_eq!(records(&fixture), before);
    drop((worker, owned));
    released(&fixture);
}

#[test]
fn worker_cold_layout_refuses_unknown_partial_and_special_state_without_mutation() {
    for kind in 0..5 {
        let fixture = Fixture::new();
        let owned = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
        let before = records(&fixture);
        let target = root(&fixture).join(if kind == 0 { "unknown" } else { "inputs" });
        let outside = fixture.directory.join("outside");
        fs::write(&outside, b"sentinel").unwrap();
        match kind {
            0 | 1 => fs::write(&target, b"existing").unwrap(),
            2 => fs::create_dir(&target).unwrap(),
            3 => symlink(&outside, &target).unwrap(),
            4 => {
                let directory = open(root(&fixture), DIRECTORY_FLAGS, Mode::empty()).unwrap();
                mknodat(&directory, "inputs", FileType::Fifo, Mode::RUSR, 0).unwrap();
            }
            _ => unreachable!(),
        }
        let old_type = fs::symlink_metadata(&target).unwrap().file_type();
        assert_error(OwnedWorkerFs::create(&owned), "worker_root_not_empty");
        assert_eq!(fs::symlink_metadata(&target).unwrap().file_type(), old_type);
        assert_eq!(fs::read(&outside).unwrap(), b"sentinel");
        assert_eq!(records(&fixture), before);
        assert!(!root(&fixture).join("staging").exists());
        assert!(!root(&fixture).join("actions").exists());
        if kind < 2 {
            assert_eq!(fs::read(&target).unwrap(), b"existing");
        }
    }
}

#[test]
fn worker_entry_names_are_single_components_before_any_filesystem_work() {
    for name in ["", ".", "..", "a/b", "/outside", "../outside", "x\0y"] {
        assert_error(EntryName::new(name), "invalid_worker_entry_name");
    }
    for name in ["input", "a.b", "under_score", "with spaces", "é"] {
        EntryName::new(name).unwrap();
    }
}

#[test]
fn worker_action_and_pending_id_collisions_never_adopt_or_overwrite() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let action = worker.create_action(ActionId(1)).unwrap();
    fs::write(action_path(&fixture, 1).join("sentinel"), b"keep").unwrap();
    assert_error(worker.create_action(ActionId(1)), "create_worker_action");
    assert_eq!(
        fs::read(action_path(&fixture, 1).join("sentinel")).unwrap(),
        b"keep"
    );
    let pending = worker.begin_input(InputId(1), 3).unwrap();
    assert_error(worker.begin_input(InputId(1), 3), "create_worker_pending");
    drop(pending);
    assert!(pending_path(&fixture, 1).is_file());
    assert_error(worker.begin_input(InputId(1), 3), "create_worker_pending");
    drop(action);
}

#[test]
fn worker_staged_input_never_replaces_an_existing_input() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    fs::write(input_path(&fixture, 1), b"old").unwrap();
    let pending = worker.begin_input(InputId(1), 3).unwrap();
    assert_error(
        pending.copy_from(&mut Cursor::new(b"new")),
        "worker_input_exists",
    );
    assert_eq!(fs::read(input_path(&fixture, 1)).unwrap(), b"old");
    assert_eq!(fs::read(pending_path(&fixture, 1)).unwrap(), b"new");
}

#[test]
fn worker_each_descendant_alone_retains_all_four_role_locks() {
    for kind in 0..4 {
        let fixture = Fixture::new();
        let (owned, worker) = setup(&fixture);
        let retained: Box<dyn std::any::Any> = match kind {
            0 => {
                drop(owned);
                Box::new(worker)
            }
            1 => {
                let handle = worker.create_action(ActionId(1)).unwrap();
                drop((worker, owned));
                Box::new(handle)
            }
            2 => {
                let handle = worker.begin_input(InputId(1), 3).unwrap();
                drop((worker, owned));
                Box::new(handle)
            }
            3 => {
                let handle = worker
                    .begin_input(InputId(1), 3)
                    .unwrap()
                    .copy_from(&mut Cursor::new(b"one"))
                    .unwrap();
                drop((worker, owned));
                Box::new(handle)
            }
            _ => unreachable!(),
        };
        held(&fixture);
        drop(retained);
        released(&fixture);
    }
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "Standalone descriptor test uses a real blocking thread without runtime dependencies."
)]
fn worker_detached_copy_closure_retains_locks_until_physical_completion() {
    let fixture = Fixture::new();
    let (owned, worker) = setup(&fixture);
    let pending = worker.begin_input(InputId(1), 3).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = std::thread::Builder::new()
        .name("owned-worker-copy".into())
        .spawn(move || {
            ready_tx.send(()).unwrap();
            go_rx
                .recv_timeout(WAIT)
                .expect("copy closure gate was not released");
            let input = pending.copy_from(&mut Cursor::new(b"one")).unwrap();
            drop(input);
            done_tx.send(()).unwrap();
        })
        .unwrap();
    ready_rx
        .recv_timeout(WAIT)
        .expect("copy closure never reached physical-work gate");
    drop((worker, owned, waiter));
    assert!(input_path(&fixture, 1).symlink_metadata().is_err());
    held(&fixture);
    go_tx.send(()).unwrap();
    done_rx
        .recv_timeout(WAIT)
        .expect("detached copy closure did not complete");
    assert_eq!(fs::read(input_path(&fixture, 1)).unwrap(), b"one");
    released(&fixture);
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "Standalone descriptor test uses a real blocking thread without runtime dependencies."
)]
fn worker_detached_link_closure_retains_locks_until_physical_completion() {
    let fixture = Fixture::new();
    let (owned, worker) = setup(&fixture);
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = std::thread::Builder::new()
        .name("owned-worker-link".into())
        .spawn(move || {
            ready_tx.send(()).unwrap();
            go_rx
                .recv_timeout(WAIT)
                .expect("link closure gate was not released");
            input.link_into(&action, &leaf("file")).unwrap();
            drop((input, action));
            done_tx.send(()).unwrap();
        })
        .unwrap();
    ready_rx
        .recv_timeout(WAIT)
        .expect("link closure never reached physical-work gate");
    drop((worker, owned, waiter));
    assert!(!action_path(&fixture, 1).join("file").exists());
    held(&fixture);
    go_tx.send(()).unwrap();
    done_rx
        .recv_timeout(WAIT)
        .expect("detached link closure did not complete");
    assert_eq!(
        fs::read(action_path(&fixture, 1).join("file")).unwrap(),
        b"one"
    );
    assert_eq!(metadata(&input_path(&fixture, 1)).2, 2);
    released(&fixture);
}

#[test]
fn worker_copy_from_real_cas_keeps_authoritative_nlink_one() {
    let fixture = Fixture::new();
    let owned = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let layout = FixedLayout::create(&owned).unwrap();
    let worker = OwnedWorkerFs::create(&owned).unwrap();
    let cas = layout.cas();
    // Arbitrary leaf bytes deliberately prove this is not hash verification.
    let digest = DigestLeaf::new(DigestAlgorithm::Sha256, [0x81; 32], 3);
    let mut write = cas.create_exclusive(TemporaryId(1)).unwrap();
    write.write_all(b"one").unwrap();
    let mut source = cas.publish(write, &digest).unwrap();
    let cas_path = Path::new(&fixture.manifest.cas.root)
        .join("content")
        .join(digest.as_str());
    let before = metadata(&cas_path);
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut source)
        .unwrap();
    let first = worker.create_action(ActionId(1)).unwrap();
    let second = worker.create_action(ActionId(2)).unwrap();
    input.link_into(&first, &leaf("file")).unwrap();
    input.link_into(&second, &leaf("file")).unwrap();
    assert_eq!(
        metadata(&cas_path),
        before,
        "worker changed authoritative CAS metadata"
    );
    assert_eq!(before.2, 1);
    let local = metadata(&input_path(&fixture, 1));
    assert_ne!(
        (local.0, local.1),
        (before.0, before.1),
        "worker reused CAS inode"
    );
    assert_eq!(local.2, 3);
    assert_eq!(local.3, 0o444);
    for id in [1, 2] {
        let action = action_path(&fixture, id).join("file");
        assert_eq!(metadata(&action), local);
        assert_eq!(fs::read(action).unwrap(), b"one");
    }
    let mut read = cas.read(&digest).unwrap();
    let mut bytes = Vec::new();
    read.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"one");
    assert!(!pending_path(&fixture, 1).exists());
}

#[test]
fn worker_private_metadata_copy_does_not_change_other_action_or_input() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    let shared = worker.create_action(ActionId(1)).unwrap();
    input.link_into(&shared, &leaf("file")).unwrap();
    let before = metadata(&input_path(&fixture, 1));
    for (id, executable, unix_mode, expected_mode) in [
        (2, true, None, 0o555),
        (3, false, Some(0o640), 0o640),
        (4, true, Some(0o600), 0o711),
    ] {
        let action = worker.create_action(ActionId(id)).unwrap();
        input
            .copy_into(
                &action,
                &leaf("file"),
                WorkerFileMetadata {
                    executable,
                    unix_mode,
                    mtime: Some(Timespec {
                        tv_sec: 123_456_789,
                        tv_nsec: 345_678_901,
                    }),
                },
            )
            .unwrap();
        let path = action_path(&fixture, id).join("file");
        let private = metadata(&path);
        assert_ne!(
            (private.0, private.1),
            (before.0, before.1),
            "metadata copy reused shared inode"
        );
        assert_eq!(private.2, 1);
        assert_eq!(private.3, expected_mode);
        assert_eq!((private.4, private.5), (123_456_789, 345_678_901));
        assert_eq!(fs::read(path).unwrap(), b"one");
        assert_eq!(
            metadata(&input_path(&fixture, 1)),
            before,
            "private metadata changed local input"
        );
        assert_eq!(
            metadata(&action_path(&fixture, 1).join("file")),
            before,
            "private metadata changed another action"
        );
    }
}

#[test]
fn worker_empty_files_are_private_and_need_no_cas_or_local_input_slot() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    for id in [1, 2] {
        let action = worker.create_action(ActionId(id)).unwrap();
        action
            .create_empty(&leaf("empty"), WorkerFileMetadata::default())
            .unwrap();
        assert_eq!(
            fs::read(action_path(&fixture, id).join("empty")).unwrap(),
            b""
        );
        assert_eq!(metadata(&action_path(&fixture, id).join("empty")).2, 1);
    }
    assert_ne!(
        metadata(&action_path(&fixture, 1).join("empty")).1,
        metadata(&action_path(&fixture, 2).join("empty")).1
    );
    assert_eq!(
        fs::read_dir(root(&fixture).join("inputs")).unwrap().count(),
        0
    );
    assert_eq!(fs::read_dir(&fixture.manifest.cas.root).unwrap().count(), 1);
}

#[test]
fn worker_ancestor_rename_with_symlink_replacement_preserves_original_operations() {
    let fixture = Fixture::new();
    let (owned, worker) = setup(&fixture);
    let moved = fixture.directory.with_extension("worker-moved");
    let decoy = fixture.directory.with_extension("worker-decoy");
    fs::create_dir(&decoy).unwrap();
    fs::write(decoy.join("sentinel"), b"outside").unwrap();
    fs::rename(&fixture.directory, &moved).unwrap();
    symlink(&decoy, &fixture.directory).unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    input.link_into(&action, &leaf("file")).unwrap();
    assert!(owned.verify_attached().is_err());
    assert_eq!(
        fs::read(
            moved
                .join("worker/actions")
                .join(action_name(1))
                .join("file")
        )
        .unwrap(),
        b"one"
    );
    assert_eq!(fs::read(decoy.join("sentinel")).unwrap(), b"outside");
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 1);
    fs::remove_file(&fixture.directory).unwrap();
    fs::rename(&moved, &fixture.directory).unwrap();
    fs::remove_dir_all(&decoy).unwrap();
}

#[test]
fn worker_root_rename_with_symlink_replacement_preserves_original_operations() {
    let fixture = Fixture::new();
    let (owned, worker) = setup(&fixture);
    let old_root = root(&fixture);
    let moved = fixture.directory.join("worker-moved");
    let decoy = fixture.directory.join("decoy");
    fs::create_dir(&decoy).unwrap();
    fs::write(decoy.join("sentinel"), b"outside").unwrap();
    fs::rename(&old_root, &moved).unwrap();
    symlink(&decoy, &old_root).unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    input.link_into(&action, &leaf("file")).unwrap();
    assert!(owned.verify_attached().is_err());
    assert_eq!(
        fs::read(moved.join("actions").join(action_name(1)).join("file")).unwrap(),
        b"one"
    );
    assert_eq!(fs::read(decoy.join("sentinel")).unwrap(), b"outside");
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 1);
}

#[test]
fn worker_each_fixed_child_rename_keeps_pinned_directory_operations() {
    for child in ["inputs", "staging", "actions"] {
        let fixture = Fixture::new();
        let (_owned, worker) = setup(&fixture);
        let original = root(&fixture).join(child);
        let moved = root(&fixture).join(format!("{child}-moved"));
        let decoy = fixture.directory.join("decoy");
        fs::create_dir(&decoy).unwrap();
        fs::write(decoy.join("sentinel"), b"outside").unwrap();
        fs::rename(&original, &moved).unwrap();
        symlink(&decoy, &original).unwrap();
        let pending = worker.begin_input(InputId(1), 3).unwrap();
        let observed_staging = if child == "staging" {
            moved.clone()
        } else {
            root(&fixture).join("staging")
        };
        assert!(observed_staging.join(pending_name(1)).is_file());
        let input = pending.copy_from(&mut Cursor::new(b"one")).unwrap();
        let action = worker.create_action(ActionId(1)).unwrap();
        input.link_into(&action, &leaf("file")).unwrap();
        let observed_inputs = if child == "inputs" {
            moved.clone()
        } else {
            root(&fixture).join("inputs")
        };
        let observed_actions = if child == "actions" {
            moved
        } else {
            root(&fixture).join("actions")
        };
        assert_eq!(
            fs::read(observed_inputs.join(input_name(1))).unwrap(),
            b"one"
        );
        assert_eq!(
            fs::read(observed_actions.join(action_name(1)).join("file")).unwrap(),
            b"one"
        );
        assert_eq!(fs::read(decoy.join("sentinel")).unwrap(), b"outside");
        assert_eq!(fs::read_dir(&decoy).unwrap().count(), 1);
    }
}

#[test]
fn worker_open_action_rename_preserves_link_and_private_copy_targets() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let original = action_path(&fixture, 1);
    let moved = root(&fixture).join("actions/moved");
    let decoy = fixture.directory.join("decoy");
    fs::create_dir(&decoy).unwrap();
    fs::write(decoy.join("sentinel"), b"outside").unwrap();
    fs::rename(&original, &moved).unwrap();
    symlink(&decoy, &original).unwrap();
    input.link_into(&action, &leaf("linked")).unwrap();
    input
        .copy_into(
            &action,
            &leaf("private"),
            WorkerFileMetadata {
                executable: true,
                ..WorkerFileMetadata::default()
            },
        )
        .unwrap();
    action
        .create_empty(&leaf("empty"), WorkerFileMetadata::default())
        .unwrap();
    assert_eq!(fs::read(moved.join("linked")).unwrap(), b"one");
    assert_eq!(fs::read(moved.join("private")).unwrap(), b"one");
    assert_eq!(metadata(&moved.join("private")).3, 0o555);
    assert_eq!(fs::read(moved.join("empty")).unwrap(), b"");
    assert_eq!(fs::read(decoy.join("sentinel")).unwrap(), b"outside");
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 1);
}

#[test]
fn worker_replaced_input_name_cannot_redirect_hardlink_to_another_inode() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let original = input_path(&fixture, 1);
    let moved = original.with_extension("saved");
    fs::rename(&original, &moved).unwrap();
    fs::write(&original, b"two").unwrap();
    assert_error(
        input.link_into(&action, &leaf("file")),
        "worker_input_replaced",
    );
    assert!(!action_path(&fixture, 1).join("file").exists());
    assert_eq!(fs::read(&original).unwrap(), b"two");
    assert_eq!(fs::read(&moved).unwrap(), b"one");
}

#[test]
fn worker_replaced_pending_name_is_not_published_or_unlinked() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let pending = worker.begin_input(InputId(1), 3).unwrap();
    let original = pending_path(&fixture, 1);
    let moved = original.with_extension("saved");
    fs::rename(&original, &moved).unwrap();
    fs::write(&original, b"decoy").unwrap();
    assert_error(
        pending.copy_from(&mut Cursor::new(b"one")),
        "worker_pending_replaced",
    );
    assert!(!input_path(&fixture, 1).exists());
    assert_eq!(fs::read(&original).unwrap(), b"decoy");
    // Whether a pre-copy check rejected or the pinned original was filled is
    // deliberately unspecified; neither artifact may be removed or adopted.
    assert!(moved.is_file());
}

#[test]
fn worker_existing_regular_symlink_directory_fifo_destinations_are_preserved() {
    for operation in 0..3 {
        for kind in 0..4 {
            let fixture = Fixture::new();
            let (_owned, worker) = setup(&fixture);
            let input = worker
                .begin_input(InputId(1), 3)
                .unwrap()
                .copy_from(&mut Cursor::new(b"one"))
                .unwrap();
            let action = worker.create_action(ActionId(1)).unwrap();
            let path = action_path(&fixture, 1).join("file");
            let outside = fixture.directory.join("outside");
            fs::write(&outside, b"sentinel").unwrap();
            match kind {
                0 => fs::write(&path, b"existing").unwrap(),
                1 => symlink(&outside, &path).unwrap(),
                2 => fs::create_dir(&path).unwrap(),
                3 => {
                    let directory =
                        open(action_path(&fixture, 1), DIRECTORY_FLAGS, Mode::empty()).unwrap();
                    mknodat(&directory, "file", FileType::Fifo, Mode::RUSR, 0).unwrap();
                }
                _ => unreachable!(),
            }
            let before = fs::symlink_metadata(&path).unwrap();
            let result = match operation {
                0 => input.link_into(&action, &leaf("file")),
                1 => input.copy_into(&action, &leaf("file"), WorkerFileMetadata::default()),
                2 => action.create_empty(&leaf("file"), WorkerFileMetadata::default()),
                _ => unreachable!(),
            };
            assert_error(result, "worker_destination_exists");
            let after = fs::symlink_metadata(&path).unwrap();
            assert_eq!(
                (after.dev(), after.ino(), after.mode()),
                (before.dev(), before.ino(), before.mode())
            );
            assert_eq!(fs::read(&outside).unwrap(), b"sentinel");
            if kind == 0 {
                assert_eq!(fs::read(&path).unwrap(), b"existing");
            }
        }
    }
}

struct Counted<'a> {
    inner: Cursor<Vec<u8>>,
    calls: &'a AtomicUsize,
    largest: &'a AtomicUsize,
}

impl Read for Counted<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.largest.fetch_max(bytes.len(), Ordering::Relaxed);
        self.inner.read(bytes)
    }
}

#[test]
fn worker_exact_qualification_limit_and_empty_input_require_real_eof() {
    assert_eq!(QUALIFICATION_MAX_INPUT_BYTES, 65_536);
    for size in [0, 1, 8192, QUALIFICATION_MAX_INPUT_BYTES] {
        let fixture = Fixture::new();
        let (_owned, worker) = setup(&fixture);
        let bytes = vec![0x71; usize::try_from(size).unwrap()];
        let calls = AtomicUsize::new(0);
        let largest = AtomicUsize::new(0);
        let mut reader = Counted {
            inner: Cursor::new(bytes.clone()),
            calls: &calls,
            largest: &largest,
        };
        let input = worker
            .begin_input(InputId(1), size)
            .unwrap()
            .copy_from(&mut reader)
            .unwrap();
        assert_eq!(fs::read(input_path(&fixture, 1)).unwrap(), bytes);
        assert!(
            calls.load(Ordering::Relaxed) >= 1,
            "empty input still requires EOF check"
        );
        assert!(
            largest.load(Ordering::Relaxed) <= COPY_CHUNK,
            "copy requested an unbounded buffer"
        );
        assert_eq!(metadata(&input_path(&fixture, 1)).3, 0o444);
        drop(input);
    }
}

#[test]
fn worker_declared_size_above_qualification_limit_refuses_before_creation() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    for size in [QUALIFICATION_MAX_INPUT_BYTES + 1, u64::MAX] {
        assert_error(
            worker.begin_input(InputId(1), size),
            "worker_input_too_large",
        );
        assert!(!pending_path(&fixture, 1).exists());
        assert!(!input_path(&fixture, 1).exists());
    }
}

#[test]
fn worker_short_and_overlong_streams_never_publish_input_capability() {
    for (declared, bytes) in [(3, &b"on"[..]), (3, &b"ones"[..]), (0, &b"x"[..])] {
        let fixture = Fixture::new();
        let (_owned, worker) = setup(&fixture);
        let pending = worker.begin_input(InputId(1), declared).unwrap();
        assert_error(
            pending.copy_from(&mut Cursor::new(bytes)),
            "worker_input_size_mismatch",
        );
        assert!(!input_path(&fixture, 1).exists());
        assert!(pending_path(&fixture, 1).is_file());
        assert!(fs::metadata(pending_path(&fixture, 1)).unwrap().len() <= declared);
    }
}

struct FailAfterPrefix {
    first: bool,
}

impl Read for FailAfterPrefix {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.first {
            self.first = false;
            bytes[0] = b'x';
            Ok(1)
        } else {
            Err(io::Error::other(
                "planned read failure after physical prefix",
            ))
        }
    }
}

#[test]
fn worker_real_read_error_preserves_written_prefix_without_publication() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let pending = worker.begin_input(InputId(1), 3).unwrap();
    assert_error(
        pending.copy_from(&mut FailAfterPrefix { first: true }),
        "read_worker_input",
    );
    assert_eq!(fs::read(pending_path(&fixture, 1)).unwrap(), b"x");
    assert!(!input_path(&fixture, 1).exists());
}

#[test]
fn worker_actual_write_syscall_failure_cannot_be_reported_as_ready() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let reached = AtomicBool::new(false);
    let bytes = vec![0x62; COPY_CHUNK * 2];
    let pending = worker
        .begin_input(InputId(1), u64::try_from(bytes.len()).unwrap())
        .unwrap();
    let result = pending.copy_from_with_fault(
        &mut Cursor::new(bytes),
        TestIoFault::ReadOnlyWriterAfterFirstChunk,
        &reached,
    );
    assert!(
        reached.load(Ordering::Acquire),
        "test did not replace the writer before its real failing write"
    );
    assert_error(result, "write_worker_input");
    assert_eq!(
        fs::read(pending_path(&fixture, 1)).unwrap(),
        vec![0x62; COPY_CHUNK]
    );
    assert!(!input_path(&fixture, 1).exists());
}

#[test]
fn worker_actual_fchmod_failure_cannot_be_reported_as_ready() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let reached = AtomicBool::new(false);
    let pending = worker.begin_input(InputId(1), 3).unwrap();
    let result = pending.copy_from_with_fault(
        &mut Cursor::new(b"one"),
        TestIoFault::PathOnlyModeDescriptor,
        &reached,
    );
    assert!(
        reached.load(Ordering::Acquire),
        "test did not reach fchmod on the O_PATH descriptor"
    );
    assert_error(result, "mode_worker_input");
    assert_eq!(fs::read(pending_path(&fixture, 1)).unwrap(), b"one");
    assert!(!input_path(&fixture, 1).exists());
}

#[test]
fn worker_preexisting_action_and_staging_special_files_are_never_opened_or_adopted() {
    for role in ["actions", "staging"] {
        for kind in 0..3 {
            let fixture = Fixture::new();
            let (_owned, worker) = setup(&fixture);
            let name = if role == "actions" {
                action_name(1)
            } else {
                pending_name(1)
            };
            let path = root(&fixture).join(role).join(&name);
            let outside = fixture.directory.join("outside");
            fs::write(&outside, b"sentinel").unwrap();
            match kind {
                0 => symlink(&outside, &path).unwrap(),
                1 => fs::create_dir(&path).unwrap(),
                2 => {
                    let directory =
                        open(root(&fixture).join(role), DIRECTORY_FLAGS, Mode::empty()).unwrap();
                    mknodat(&directory, name.as_str(), FileType::Fifo, Mode::RUSR, 0).unwrap();
                }
                _ => unreachable!(),
            }
            let before = fs::symlink_metadata(&path).unwrap();
            if role == "actions" {
                assert_error(worker.create_action(ActionId(1)), "create_worker_action");
            } else {
                assert_error(worker.begin_input(InputId(1), 3), "create_worker_pending");
            }
            let after = fs::symlink_metadata(&path).unwrap();
            assert_eq!(
                (after.dev(), after.ino(), after.mode()),
                (before.dev(), before.ino(), before.mode())
            );
            assert_eq!(fs::read(&outside).unwrap(), b"sentinel");
        }
    }
}

#[test]
fn worker_publication_refuses_special_destination_and_preserves_staging() {
    for kind in 0..3 {
        let fixture = Fixture::new();
        let (_owned, worker) = setup(&fixture);
        let destination = input_path(&fixture, 1);
        let outside = fixture.directory.join("outside");
        fs::write(&outside, b"sentinel").unwrap();
        match kind {
            0 => symlink(&outside, &destination).unwrap(),
            1 => fs::create_dir(&destination).unwrap(),
            2 => {
                let directory = open(
                    root(&fixture).join("inputs"),
                    DIRECTORY_FLAGS,
                    Mode::empty(),
                )
                .unwrap();
                mknodat(
                    &directory,
                    input_name(1).as_str(),
                    FileType::Fifo,
                    Mode::RUSR,
                    0,
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = fs::symlink_metadata(&destination).unwrap();
        let pending = worker.begin_input(InputId(1), 3).unwrap();
        assert_error(
            pending.copy_from(&mut Cursor::new(b"one")),
            "worker_input_exists",
        );
        let after = fs::symlink_metadata(&destination).unwrap();
        assert_eq!(
            (after.dev(), after.ino(), after.mode()),
            (before.dev(), before.ino(), before.mode())
        );
        assert_eq!(fs::read(pending_path(&fixture, 1)).unwrap(), b"one");
        assert_eq!(fs::read(&outside).unwrap(), b"sentinel");
    }
}

#[test]
fn worker_overlong_stream_at_limit_reads_only_one_extra_byte() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let limit = usize::try_from(QUALIFICATION_MAX_INPUT_BYTES).unwrap();
    let calls = AtomicUsize::new(0);
    let largest = AtomicUsize::new(0);
    let mut source = Counted {
        inner: Cursor::new(vec![0x64; limit + 100]),
        calls: &calls,
        largest: &largest,
    };
    let pending = worker
        .begin_input(InputId(1), QUALIFICATION_MAX_INPUT_BYTES)
        .unwrap();
    assert_error(pending.copy_from(&mut source), "worker_input_size_mismatch");
    assert_eq!(source.inner.position(), QUALIFICATION_MAX_INPUT_BYTES + 1);
    assert!(largest.load(Ordering::Relaxed) <= COPY_CHUNK);
    assert_eq!(
        fs::metadata(pending_path(&fixture, 1)).unwrap().len(),
        QUALIFICATION_MAX_INPUT_BYTES
    );
    assert!(!input_path(&fixture, 1).exists());
}

#[test]
fn worker_read_error_at_expected_eof_is_not_success() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let pending = worker.begin_input(InputId(1), 1).unwrap();
    assert_error(
        pending.copy_from(&mut FailAfterPrefix { first: true }),
        "read_worker_input",
    );
    assert_eq!(fs::read(pending_path(&fixture, 1)).unwrap(), b"x");
    assert!(!input_path(&fixture, 1).exists());
}

#[test]
fn worker_completed_input_retains_a_read_only_descriptor() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let mut input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    // Test-only physical write through the retained descriptor, with no raw FD
    // escape. A mode of 0444 alone cannot revoke an already open RDWR handle.
    let error = input
        .test_write_attempt()
        .expect_err("completed input retained writable access");
    assert_eq!(
        error.raw_os_error(),
        Some(rustix::io::Errno::BADF.raw_os_error())
    );
    assert_eq!(fs::read(input_path(&fixture, 1)).unwrap(), b"one");
}

#[test]
fn worker_private_metadata_failure_preserves_partial_file_and_shared_input() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let before = metadata(&input_path(&fixture, 1));
    let result = input.copy_into(
        &action,
        &leaf("file"),
        WorkerFileMetadata {
            executable: false,
            unix_mode: Some(0o600),
            // Invalid nanoseconds force the real futimens syscall to refuse.
            mtime: Some(Timespec {
                tv_sec: 123,
                tv_nsec: 1_000_000_000,
            }),
        },
    );
    assert_error(result, "mtime_worker_file");
    assert_eq!(
        fs::read(action_path(&fixture, 1).join("file")).unwrap(),
        b"one"
    );
    assert_eq!(metadata(&input_path(&fixture, 1)), before);
}

fn cross_namespace_materialization_refuses(copy: bool) {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    // Use one fixture lock, but four additional physical role roots and a
    // separate Ownership claim. Logical IDs deliberately remain identical.
    let mut other_manifest = fixture.manifest.clone();
    let other_roots = ["other-journal", "other-cas", "other-sink", "other-worker"].map(|name| {
        let path = fixture.directory.join(name);
        fs::create_dir(&path).unwrap();
        path.to_str().unwrap().to_owned()
    });
    other_manifest
        .authority
        .journal_root
        .clone_from(&other_roots[0]);
    other_manifest.cas.root.clone_from(&other_roots[1]);
    other_manifest
        .temporary_action_cache
        .root
        .clone_from(&other_roots[2]);
    other_manifest.worker.root.clone_from(&other_roots[3]);
    let other_owned = Arc::new(Ownership::claim_cold(&other_manifest).unwrap());
    let other_worker = OwnedWorkerFs::create(&other_owned).unwrap();
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"one"))
        .unwrap();
    let other_input = other_worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut Cursor::new(b"two"))
        .unwrap();
    let action = worker.create_action(ActionId(1)).unwrap();
    let other_action = other_worker.create_action(ActionId(1)).unwrap();
    let input_path = input_path(&fixture, 1);
    let other_input_path = Path::new(&other_manifest.worker.root)
        .join("inputs")
        .join(input_name(1));
    let action_path = action_path(&fixture, 1);
    let other_action_path = Path::new(&other_manifest.worker.root)
        .join("actions")
        .join(action_name(1));
    assert_eq!(
        fixture.manifest.worker.worker_id,
        other_manifest.worker.worker_id
    );
    assert_eq!(
        fixture.manifest.cas.namespace_id,
        other_manifest.cas.namespace_id
    );
    assert_ne!(metadata(&input_path).1, metadata(&other_input_path).1);
    for (source, destination, path) in [
        (&input, &action, &action_path),
        (&other_input, &other_action, &other_action_path),
    ] {
        // Same-family operation succeeds before the cross-family refusal.
        if copy {
            source
                .copy_into(destination, &leaf("local"), WorkerFileMetadata::default())
                .unwrap();
        } else {
            source.link_into(destination, &leaf("local")).unwrap();
        }
        fs::write(path.join("sentinel"), b"keep").unwrap();
    }
    let before = [metadata(&input_path), metadata(&other_input_path)];
    for (source, destination, path) in [
        (&input, &other_action, &other_action_path),
        (&other_input, &action, &action_path),
    ] {
        let result = if copy {
            source.copy_into(destination, &leaf("foreign"), WorkerFileMetadata::default())
        } else {
            source.link_into(destination, &leaf("foreign"))
        };
        assert_error(result, "different_worker_capability");
        assert!(
            !path.join("foreign").exists(),
            "cross-family refusal created a destination"
        );
    }
    for (path, source, expected, original_metadata) in [
        (&action_path, &input_path, &b"one"[..], before[0]),
        (
            &other_action_path,
            &other_input_path,
            &b"two"[..],
            before[1],
        ),
    ] {
        assert_eq!(fs::read_dir(path).unwrap().count(), 2);
        assert_eq!(fs::read(path.join("sentinel")).unwrap(), b"keep");
        assert_eq!(fs::read(path.join("local")).unwrap(), expected);
        assert_eq!(fs::read(source).unwrap(), expected);
        assert_eq!(
            metadata(source),
            original_metadata,
            "cross-family operation changed its source"
        );
    }
}

#[test]
fn worker_cross_namespace_link_refuses_equal_ids_without_mutation() {
    cross_namespace_materialization_refuses(false);
}

#[test]
fn worker_cross_namespace_copy_refuses_equal_ids_without_mutation() {
    cross_namespace_materialization_refuses(true);
}

struct FiniteInterrupted {
    step: usize,
    delivered: usize,
    interruptions: usize,
    eof_reads: usize,
}

impl Read for FiniteInterrupted {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        assert!(
            !bytes.is_empty(),
            "qualification reader received an empty request"
        );
        let step = self.step;
        self.step += 1;
        match step {
            0 | 2 | 4 | 5 => {
                self.interruptions += 1;
                Err(io::Error::from(io::ErrorKind::Interrupted))
            }
            1 => {
                bytes[0] = b'o';
                self.delivered += 1;
                Ok(1)
            }
            3 => {
                assert!(
                    bytes.len() >= 2,
                    "copy did not request the two remaining bytes"
                );
                bytes[..2].copy_from_slice(b"ne");
                self.delivered += 2;
                Ok(2)
            }
            6 => {
                // Both immediately preceding errors occurred during the
                // mandatory EOF probe, after all three bytes were copied.
                assert_eq!(self.delivered, 3);
                assert_eq!(bytes.len(), 1);
                self.eof_reads += 1;
                Ok(0)
            }
            _ => panic!("finite Interrupted fixture was read past its one EOF"),
        }
    }
}

#[test]
fn worker_finite_interrupted_reads_retry_mid_copy_and_mandatory_eof() {
    let fixture = Fixture::new();
    let (_owned, worker) = setup(&fixture);
    let mut reader = FiniteInterrupted {
        step: 0,
        delivered: 0,
        interruptions: 0,
        eof_reads: 0,
    };
    let input = worker
        .begin_input(InputId(1), 3)
        .unwrap()
        .copy_from(&mut reader)
        .unwrap();
    assert_eq!(reader.step, 7);
    assert_eq!(reader.interruptions, 4);
    assert_eq!(reader.delivered, 3);
    assert_eq!(reader.eof_reads, 1);
    assert_eq!(fs::read(input_path(&fixture, 1)).unwrap(), b"one");
    assert_eq!(metadata(&input_path(&fixture, 1)).3, 0o444);
    assert!(!pending_path(&fixture, 1).exists());
    drop(input);
}
