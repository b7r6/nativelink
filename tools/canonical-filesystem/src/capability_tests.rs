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

use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use rustix::fs::{FileType, mknodat};

use super::*;

fn layout(fixture: &Fixture) -> (Arc<Ownership>, FixedLayout) {
    let owned = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let layout = FixedLayout::create(&owned).unwrap();
    (owned, layout)
}

fn digest(value: u8) -> DigestLeaf {
    DigestLeaf::new(DigestAlgorithm::Sha256, [value; 32], 3)
}

fn text(mut handle: ReadHandle) -> String {
    let mut text = String::new();
    handle.read_to_string(&mut text).unwrap();
    text
}

fn all_locks_held(fixture: &Fixture) {
    for path in fixture.roots() {
        let descriptor = open(path, DIRECTORY_FLAGS, Mode::empty()).unwrap();
        assert_eq!(
            flock(&descriptor, FlockOperation::NonBlockingLockExclusive),
            Err(rustix::io::Errno::WOULDBLOCK),
            "role lock released: {path}"
        );
    }
}

#[test]
fn digest_leaf_parser_refuses_every_path_and_noncanonical_encoding() {
    for algorithm in [DigestAlgorithm::Sha256, DigestAlgorithm::Blake3] {
        for size in [0, u64::MAX] {
            let leaf = DigestLeaf::new(algorithm, [0xaf; 32], size);
            assert_eq!(DigestLeaf::parse(leaf.as_str()).unwrap(), leaf);
        }
    }
    let valid = digest(4);
    let hash = "a".repeat(64);
    for bad in [
        String::new(),
        ".".into(),
        "..".into(),
        "a/b".into(),
        "/etc/passwd".into(),
        "../content".into(),
        OWNERSHIP_RECORD.into(),
        format!("../{}", valid.as_str()),
        format!("{}/child", valid.as_str()),
        format!("{}\0", valid.as_str()),
        format!("{}\n", valid.as_str()),
        format!("sha256-{hash}-00"),
        format!("sha256-{hash}-+1"),
        format!("sha256-{hash}-18446744073709551616"),
        format!("sha256-{hash}--1"),
        format!("sha512-{hash}-1"),
        format!("sha256-{}-1", "A".repeat(64)),
        format!("sha256-{}-1", "a".repeat(63)),
    ] {
        assert!(DigestLeaf::parse(&bad).is_err(), "accepted {bad:?}");
    }
}

#[test]
fn cas_never_overwrites_and_sink_replacement_preserves_open_reader() {
    let fixture = Fixture::new();
    let (owned, layout) = layout(&fixture);
    let cas = layout.cas();
    let sink = layout.sink();
    let leaf = digest(1);
    let mut first = cas.create_exclusive(TemporaryId(1)).unwrap();
    first.write_all(b"one").unwrap();
    first.sync_all().unwrap();
    assert_eq!(text(cas.publish(first, &leaf).unwrap()), "one");
    let mut second = cas.create_exclusive(TemporaryId(2)).unwrap();
    second.write_all(b"two").unwrap();
    assert_eq!(cas.publish(second, &leaf).unwrap_err().code, "slot_exists");
    assert_eq!(text(cas.read(&leaf).unwrap()), "one");
    assert_eq!(
        fs::read(
            Path::new(&fixture.manifest.cas.root)
                .join("temp/pending-00000000000000000000000000000002")
        )
        .unwrap(),
        b"two"
    );
    let mut first = sink.create_exclusive(TemporaryId(1)).unwrap();
    first.write_all(b"one").unwrap();
    let old_reader = sink.replace(first, &leaf).unwrap();
    let mut second = sink.create_exclusive(TemporaryId(2)).unwrap();
    second.write_all(b"two").unwrap();
    assert_eq!(text(sink.replace(second, &leaf).unwrap()), "two");
    assert_eq!(text(old_reader), "one");
    assert_eq!(text(sink.read(&leaf).unwrap()), "two");
    cas.sync_directories().unwrap();
    sink.sync_directories().unwrap();
    assert!(cas.remove(&leaf).unwrap());
    assert!(!cas.remove(&leaf).unwrap());
    assert!(sink.remove(&leaf).unwrap());
    drop((cas, sink, layout, owned));
    let reopened = Arc::new(Ownership::reopen(&fixture.manifest).unwrap());
    FixedLayout::reopen(&reopened).unwrap();
}

#[test]
fn creation_checks_both_roots_before_writing_and_reopen_refuses_symbolic_children() {
    let fixture = Fixture::new();
    let owned = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let outside = fixture.directory.join("outside");
    fs::create_dir(&outside).unwrap();
    let child = Path::new(&fixture.manifest.temporary_action_cache.root).join("content");
    symlink(&outside, &child).unwrap();
    assert_eq!(
        FixedLayout::create(&owned).unwrap_err().code,
        "layout_root_not_empty"
    );
    assert!(
        !Path::new(&fixture.manifest.cas.root)
            .join("content")
            .exists()
    );
    fs::remove_file(&child).unwrap();
    drop(FixedLayout::create(&owned).unwrap());
    let records: Vec<_> = (0..4)
        .map(|i| fs::read(fixture.record(i)).unwrap())
        .collect();
    for role in [
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
    ] {
        for name in ["content", "temp", "content.exec"] {
            let child = Path::new(role).join(name);
            let saved = Path::new(role).join(format!("{name}.saved"));
            fs::rename(&child, &saved).unwrap();
            symlink(&outside, &child).unwrap();
            assert_eq!(
                FixedLayout::reopen(&owned).unwrap_err().code,
                "unsafe_or_unavailable_layout_child"
            );
            fs::remove_file(&child).unwrap();
            fs::rename(&saved, &child).unwrap();
        }
    }
    for (i, bytes) in records.iter().enumerate() {
        assert_eq!(&fs::read(fixture.record(i)).unwrap(), bytes);
    }
    assert_eq!(fs::read_dir(&outside).unwrap().count(), 0);
}

#[test]
fn ancestor_rename_and_symlink_substitution_do_not_redirect_operations() {
    let fixture = Fixture::new();
    let (owned, layout) = layout(&fixture);
    let moved = fixture.directory.with_extension("moved");
    let decoy = fixture.directory.with_extension("decoy");
    fs::create_dir(&decoy).unwrap();
    fs::rename(&fixture.directory, &moved).unwrap();
    symlink(&decoy, &fixture.directory).unwrap();
    assert!(owned.verify_attached().is_err());
    let leaf = digest(2);
    let cas = layout.cas();
    let mut pending = cas.create_exclusive(TemporaryId(3)).unwrap();
    pending.write_all(b"cas").unwrap();
    assert_eq!(text(cas.publish(pending, &leaf).unwrap()), "cas");
    assert_eq!(text(cas.read(&leaf).unwrap()), "cas");
    assert_eq!(
        fs::read(moved.join("cas/content").join(leaf.as_str())).unwrap(),
        b"cas"
    );
    let sink = layout.sink();
    let mut pending = sink.create_exclusive(TemporaryId(3)).unwrap();
    pending.write_all(b"acy").unwrap();
    assert_eq!(text(sink.replace(pending, &leaf).unwrap()), "acy");
    assert!(cas.remove(&leaf).unwrap());
    assert!(sink.remove(&leaf).unwrap());
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 0);
    fs::remove_file(&fixture.directory).unwrap();
    fs::rename(&moved, &fixture.directory).unwrap();
    fs::remove_dir(&decoy).unwrap();
}

#[test]
fn root_rename_and_replacement_keep_original_descriptors() {
    let fixture = Fixture::new();
    let (owned, layout) = layout(&fixture);
    let decoy = fixture.directory.join("decoy");
    fs::create_dir(&decoy).unwrap();
    for role in [
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
    ] {
        fs::rename(role, format!("{role}-moved")).unwrap();
        symlink(&decoy, role).unwrap();
    }
    let leaf = digest(3);
    let cas = layout.cas();
    let mut pending = cas.create_exclusive(TemporaryId(4)).unwrap();
    pending.write_all(b"cas").unwrap();
    assert_eq!(text(cas.publish(pending, &leaf).unwrap()), "cas");
    let sink = layout.sink();
    let mut pending = sink.create_exclusive(TemporaryId(4)).unwrap();
    pending.write_all(b"acy").unwrap();
    assert_eq!(text(sink.replace(pending, &leaf).unwrap()), "acy");
    assert!(owned.verify_attached().is_err());
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 0);
    assert_eq!(text(cas.read(&leaf).unwrap()), "cas");
    assert_eq!(text(sink.read(&leaf).unwrap()), "acy");
    assert!(cas.remove(&leaf).unwrap());
    assert!(sink.remove(&leaf).unwrap());
}

#[test]
fn all_child_renames_and_symlink_replacements_keep_original_operations() {
    let fixture = Fixture::new();
    let (_owned, layout) = layout(&fixture);
    let decoy = fixture.directory.join("decoy");
    fs::create_dir(&decoy).unwrap();
    for role in [
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
    ] {
        for child in ["content", "temp", "content.exec"] {
            fs::rename(
                Path::new(role).join(child),
                Path::new(role).join(format!("{child}-moved")),
            )
            .unwrap();
            symlink(&decoy, Path::new(role).join(child)).unwrap();
        }
    }
    let leaf = digest(4);
    let cas = layout.cas();
    let mut pending = cas.create_exclusive(TemporaryId(5)).unwrap();
    pending.write_all(b"cas").unwrap();
    assert_eq!(text(cas.publish(pending, &leaf).unwrap()), "cas");
    assert_eq!(text(cas.read(&leaf).unwrap()), "cas");
    assert!(
        Path::new(&fixture.manifest.cas.root)
            .join("content-moved")
            .join(leaf.as_str())
            .is_file()
    );
    let sink = layout.sink();
    let mut pending = sink.create_exclusive(TemporaryId(5)).unwrap();
    pending.write_all(b"acy").unwrap();
    assert_eq!(text(sink.replace(pending, &leaf).unwrap()), "acy");
    assert_eq!(text(sink.read(&leaf).unwrap()), "acy");
    cas.sync_directories().unwrap();
    sink.sync_directories().unwrap();
    assert!(cas.remove(&leaf).unwrap());
    assert!(sink.remove(&leaf).unwrap());
    assert_eq!(fs::read_dir(&decoy).unwrap().count(), 0);
}

#[test]
fn regular_type_prechecks_refuse_special_files_and_symlinks_without_opening_them() {
    let fixture = Fixture::new();
    let (_owned, layout) = layout(&fixture);
    let sink = layout.sink();
    let leaf = digest(5);
    let content = Path::new(&fixture.manifest.temporary_action_cache.root).join("content");
    let slot = content.join(leaf.as_str());
    let directory = open(&content, DIRECTORY_FLAGS, Mode::empty()).unwrap();
    let outside = fixture.directory.join("outside-file");
    fs::write(&outside, b"safe").unwrap();
    for kind in 0..5 {
        let listener = match kind {
            0 => {
                symlink(&outside, &slot).unwrap();
                None
            }
            1 => {
                fs::create_dir(&slot).unwrap();
                None
            }
            2 => {
                mknodat(
                    &directory,
                    leaf.as_str(),
                    FileType::Fifo,
                    Mode::RUSR | Mode::WUSR,
                    0,
                )
                .unwrap();
                None
            }
            3 => {
                // sockaddr_un has a shorter path bound than a digest slot.
                let short = fixture.directory.join("socket");
                let listener = UnixListener::bind(&short).unwrap();
                fs::rename(&short, &slot).unwrap();
                Some(listener)
            }
            4 => {
                fs::hard_link(&outside, &slot).unwrap();
                None
            }
            _ => unreachable!(),
        };
        assert_eq!(
            sink.read(&leaf).unwrap_err().code,
            "unsafe_slot_type_or_links"
        );
        assert_eq!(
            sink.remove(&leaf).unwrap_err().code,
            "unsafe_slot_type_or_links"
        );
        let pending = sink.create_exclusive(TemporaryId(kind)).unwrap();
        assert_eq!(
            sink.replace(pending, &leaf).unwrap_err().code,
            "unsafe_slot_type_or_links"
        );
        drop(listener);
        if kind == 1 {
            fs::remove_dir(&slot).unwrap();
        } else {
            fs::remove_file(&slot).unwrap();
        }
    }
    assert_eq!(fs::read(&outside).unwrap(), b"safe");
}

#[test]
fn exclusive_temporary_names_refuse_existing_symlinks_and_special_files() {
    let fixture = Fixture::new();
    let (_owned, layout) = layout(&fixture);
    let cas = layout.cas();
    let first = cas.create_exclusive(TemporaryId(6)).unwrap();
    assert_eq!(
        cas.create_exclusive(TemporaryId(6)).unwrap_err().code,
        "create_temporary"
    );
    drop(first);
    let temp = Path::new(&fixture.manifest.cas.root).join("temp");
    let slot = temp.join("pending-00000000000000000000000000000007");
    symlink("/dev/zero", &slot).unwrap();
    assert_eq!(
        cas.create_exclusive(TemporaryId(7)).unwrap_err().code,
        "create_temporary"
    );
    fs::remove_file(&slot).unwrap();
    let directory = open(&temp, DIRECTORY_FLAGS, Mode::empty()).unwrap();
    mknodat(
        &directory,
        "pending-00000000000000000000000000000007",
        FileType::Fifo,
        Mode::RUSR,
        0,
    )
    .unwrap();
    assert_eq!(
        cas.create_exclusive(TemporaryId(7)).unwrap_err().code,
        "create_temporary"
    );
}

#[test]
fn wrong_role_and_replaced_temporary_preserve_artifacts_on_failure() {
    let fixture = Fixture::new();
    let (_owned, layout) = layout(&fixture);
    let cas = layout.cas();
    let sink = layout.sink();
    let leaf = digest(6);
    let mut pending = cas.create_exclusive(TemporaryId(8)).unwrap();
    pending.write_all(b"old").unwrap();
    assert_eq!(
        sink.replace(pending, &leaf).unwrap_err().code,
        "different_layout_capability"
    );
    let mut pending = cas.create_exclusive(TemporaryId(9)).unwrap();
    pending.write_all(b"one").unwrap();
    let slot =
        Path::new(&fixture.manifest.cas.root).join("temp/pending-00000000000000000000000000000009");
    let saved = slot.with_extension("saved");
    fs::rename(&slot, &saved).unwrap();
    fs::write(&slot, b"two").unwrap();
    assert_eq!(
        cas.publish(pending, &leaf).unwrap_err().code,
        "slot_replaced"
    );
    assert_eq!(fs::read(&slot).unwrap(), b"two");
    assert_eq!(fs::read(&saved).unwrap(), b"one");
    assert_eq!(cas.read(&leaf).unwrap_err().code, "missing_slot");
}

#[test]
fn every_returned_handle_retains_every_role_lock() {
    for handle_kind in 0..5 {
        let fixture = Fixture::new();
        let (owned, layout) = layout(&fixture);
        let cas = layout.cas();
        let pending = cas.create_exclusive(TemporaryId(10)).unwrap();
        let handle: Box<dyn std::any::Any> = match handle_kind {
            0 => {
                drop(pending);
                Box::new(cas.clone())
            }
            1 => {
                drop(pending);
                Box::new(layout.sink())
            }
            2 => Box::new(pending),
            3 => Box::new(cas.publish(pending, &digest(7)).unwrap()),
            4 => {
                drop(cas.publish(pending, &digest(7)).unwrap());
                Box::new(cas.read(&digest(7)).unwrap())
            }
            _ => unreachable!(),
        };
        drop((cas, layout, owned));
        all_locks_held(&fixture);
        drop(handle);
        drop(Ownership::reopen(&fixture.manifest).unwrap());
    }
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "This standalone ownership test has no NativeLink runtime/task dependency."
)]
fn delayed_blocking_closure_owns_locks_after_original_callers_drop() {
    let fixture = Fixture::new();
    let (owned, layout) = layout(&fixture);
    let cas = layout.cas();
    let mut pending = cas.create_exclusive(TemporaryId(11)).unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("delayed-capability-io".into())
        .spawn(move || {
            ready_tx.send(()).unwrap();
            go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            pending.write_all(b"new").unwrap();
            text(cas.publish(pending, &digest(8)).unwrap())
        })
        .unwrap();
    drop((layout, owned));
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    all_locks_held(&fixture);
    assert!(Ownership::reopen(&fixture.manifest).is_err());
    go_tx.send(()).unwrap();
    assert_eq!(thread.join().unwrap(), "new");
    drop(Ownership::reopen(&fixture.manifest).unwrap());
}

#[test]
fn explicit_layout_sync_preserves_children_records_and_locks() {
    let fixture = Fixture::new();
    let (owned, layout) = layout(&fixture);
    let records: Vec<_> = (0..4)
        .map(|i| fs::read(fixture.record(i)).unwrap())
        .collect();
    let result = layout.sync_layout();
    // These assertions precede the result check so a syscall-injected failure
    // still checks the preserved evidence and lock lifetime at the error return.
    for role in [
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
    ] {
        for child in ["content", "temp", "content.exec"] {
            assert!(Path::new(role).join(child).is_dir());
        }
    }
    for (i, record) in records.iter().enumerate() {
        assert_eq!(&fs::read(fixture.record(i)).unwrap(), record);
    }
    all_locks_held(&fixture);
    drop((layout, owned));
    let reopened = Arc::new(Ownership::reopen(&fixture.manifest).unwrap());
    drop(FixedLayout::reopen(&reopened).unwrap());
    result.unwrap();
}
