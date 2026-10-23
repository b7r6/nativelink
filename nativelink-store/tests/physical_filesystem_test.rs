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

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::process::Command;

use nativelink_store::physical_filesystem::{
    Algorithm, Failure, FailureKind, Incarnation, Limits, ObjectId, PhysicalFilesystem, PhysicalKey,
};
use tempfile::TempDir;

fn incarnation(value: u8) -> Incarnation {
    Incarnation::new([value; 16]).unwrap()
}

fn key(incarnation: Incarnation, generation: u64) -> PhysicalKey {
    PhysicalKey::new(incarnation, generation).unwrap()
}

fn publish(store: &PhysicalFilesystem, key: PhysicalKey, write: u64, bytes: &[u8]) -> ObjectId {
    let object = ObjectId::for_bytes(Algorithm::Sha256, bytes);
    let mut stage = store.begin_write(key, write, object).unwrap();
    stage.append(bytes).unwrap();
    let receipt = stage.finish().unwrap();
    assert_eq!(receipt.key, key);
    assert_eq!(receipt.write, write);
    assert_eq!(receipt.object, object);
    object
}

fn failure<T>(result: Result<T, Failure>, kind: FailureKind) {
    assert_eq!(result.err().expect("operation must fail").kind, kind);
}

fn object_path(store: &PhysicalFilesystem, key: PhysicalKey) -> std::path::PathBuf {
    store
        .namespace_path()
        .join("objects-v1")
        .join(format!("{:016x}", key.generation()))
}

#[test]
fn both_algorithms_verify_full_object_before_exposing_exact_ranges() {
    let directory = TempDir::new().unwrap();
    let store =
        PhysicalFilesystem::bootstrap(directory.path(), incarnation(1), Limits::default()).unwrap();
    let data: Vec<u8> = (0..200_000)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    for (index, algorithm) in [Algorithm::Sha256, Algorithm::Blake3]
        .into_iter()
        .enumerate()
    {
        let key = key(incarnation(1), index as u64 + 1);
        let object = ObjectId::for_bytes(algorithm, &data);
        let mut stage = store.begin_write(key, 1, object).unwrap();
        for chunk in data.chunks(8191) {
            stage.append(chunk).unwrap();
        }
        stage.finish().unwrap();
        assert_eq!(store.read(key, object, 0, None).unwrap().bytes, data);
        assert_eq!(
            store.read(key, object, 65_000, Some(9000)).unwrap().bytes,
            data[65_000..74_000]
        );
        assert_eq!(
            store.read(key, object, object.size, None).unwrap().bytes,
            b""
        );
        failure(
            store.read(key, object, object.size + 1, None),
            FailureKind::Invalid,
        );
        failure(
            store.read(key, object, u64::MAX, Some(2)),
            FailureKind::Invalid,
        );
    }
}

#[test]
fn immutable_retry_cannot_overwrite_another_descriptor_or_accept_bad_input() {
    let directory = TempDir::new().unwrap();
    let store =
        PhysicalFilesystem::bootstrap(directory.path(), incarnation(1), Limits::default()).unwrap();
    let key = key(incarnation(1), 1);
    let original = publish(&store, key, 1, b"original");
    assert_eq!(publish(&store, key, 1, b"original"), original);
    let replacement = ObjectId::for_bytes(Algorithm::Sha256, b"replace!");
    let mut stage = store.begin_write(key, 2, replacement).unwrap();
    stage.append(b"replace!").unwrap();
    failure(stage.finish(), FailureKind::Conflict);
    assert_eq!(
        store.read(key, original, 0, None).unwrap().bytes,
        b"original"
    );
    let mut wrong_hash = store.begin_write(key, 3, original).unwrap();
    wrong_hash.append(b"incorrect".get(..8).unwrap()).unwrap();
    failure(wrong_hash.finish(), FailureKind::Corrupt);
    let mut too_long = store.begin_write(key, 4, original).unwrap();
    failure(
        too_long.append(b"too long for the object"),
        FailureKind::Corrupt,
    );
    failure(too_long.finish(), FailureKind::Corrupt);
    let empty = PhysicalKey::new(incarnation(1), 2).unwrap();
    let id = publish(&store, empty, 5, b"");
    assert!(store.read(empty, id, 0, None).unwrap().bytes.is_empty());
}

#[test]
fn empty_catalogue_and_explicit_new_incarnation_never_alias_old_deletion() {
    let directory = TempDir::new().unwrap();
    let old_incarnation = incarnation(1);
    let old_key = key(old_incarnation, 1);
    let old = PhysicalFilesystem::bootstrap(directory.path(), old_incarnation, Limits::default())
        .unwrap();
    let object = publish(&old, old_key, 1, b"same logical bytes");
    assert!(!old.delete(old_key, 1).unwrap().already_absent);
    assert!(old.delete(old_key, 1).unwrap().already_absent);
    drop(old);
    // The physical catalogue is now empty. Its incarnation is still occupied.
    failure(
        PhysicalFilesystem::bootstrap(directory.path(), old_incarnation, Limits::default()),
        FailureKind::Conflict,
    );
    let old = PhysicalFilesystem::open(directory.path(), old_incarnation).unwrap();
    let new_incarnation = incarnation(2);
    let new_key = key(new_incarnation, 1);
    let replacement =
        PhysicalFilesystem::bootstrap(directory.path(), new_incarnation, Limits::default())
            .unwrap();
    publish(&replacement, new_key, 1, b"same logical bytes");
    old.delete(old_key, 1).unwrap();
    failure(replacement.delete(old_key, 1), FailureKind::Conflict);
    assert_eq!(
        replacement.read(new_key, object, 0, None).unwrap().bytes,
        b"same logical bytes"
    );
    let later = key(new_incarnation, 2);
    publish(&replacement, later, 2, b"same logical bytes");
    replacement.delete(new_key, 2).unwrap();
    replacement.delete(new_key, 2).unwrap();
    assert_eq!(
        replacement.read(later, object, 0, None).unwrap().bytes,
        b"same logical bytes"
    );
}

#[test]
fn missing_corrupt_manifest_and_interrupted_bootstrap_never_reinitialize() {
    let directory = TempDir::new().unwrap();
    let inc = incarnation(1);
    failure(
        PhysicalFilesystem::open(directory.path(), inc),
        FailureKind::Missing,
    );
    assert!(!directory.path().join(inc.directory_name()).exists());
    let store = PhysicalFilesystem::bootstrap(directory.path(), inc, Limits::default()).unwrap();
    let manifest = store.namespace_path().join("manifest");
    drop(store);
    let original = fs::read(&manifest).unwrap();
    let mut corrupt = original.clone();
    corrupt[30] ^= 1;
    fs::write(&manifest, corrupt).unwrap();
    failure(
        PhysicalFilesystem::open(directory.path(), inc),
        FailureKind::Corrupt,
    );
    failure(
        PhysicalFilesystem::bootstrap(directory.path(), inc, Limits::default()),
        FailureKind::Conflict,
    );
    assert_ne!(fs::read(&manifest).unwrap(), original);
    fs::remove_file(&manifest).unwrap();
    failure(
        PhysicalFilesystem::open(directory.path(), inc),
        FailureKind::Missing,
    );
    failure(
        PhysicalFilesystem::bootstrap(directory.path(), inc, Limits::default()),
        FailureKind::Conflict,
    );
}

#[test]
fn staged_handle_keeps_exclusive_incarnation_ownership_and_blocks_discard() {
    let directory = TempDir::new().unwrap();
    let inc = incarnation(1);
    let key = key(inc, 1);
    let store = PhysicalFilesystem::bootstrap(directory.path(), inc, Limits::default()).unwrap();
    let object = ObjectId::for_bytes(Algorithm::Sha256, b"pending");
    let mut stage = store.begin_write(key, 1, object).unwrap();
    stage.append(b"pending").unwrap();
    failure(store.discard_stage(key, 1), FailureKind::Conflict);
    failure(
        PhysicalFilesystem::open(directory.path(), inc),
        FailureKind::Conflict,
    );
    drop(store);
    failure(
        PhysicalFilesystem::open(directory.path(), inc),
        FailureKind::Conflict,
    );
    stage.finish().unwrap();
    let reopened = PhysicalFilesystem::open(directory.path(), inc).unwrap();
    assert_eq!(
        reopened.read(key, object, 0, None).unwrap().bytes,
        b"pending"
    );
}

#[test]
fn late_write_after_successful_delete_is_an_orphan_requiring_another_delete() {
    let directory = TempDir::new().unwrap();
    let inc = incarnation(1);
    let key = key(inc, 1);
    let object = ObjectId::for_bytes(Algorithm::Sha256, b"late bytes");
    let store = PhysicalFilesystem::bootstrap(directory.path(), inc, Limits::default()).unwrap();
    let mut late = store.begin_write(key, 11, object).unwrap();
    late.append(b"late bytes").unwrap();
    // The authoritative fixture records retirement before issuing this effect.
    // A successful physical absence does not settle the still-pending writer.
    assert!(store.delete(key, 21).unwrap().already_absent);
    let lost_receipt = late.finish().unwrap();
    assert_eq!(lost_receipt.write, 11);
    drop(store);
    let reopened = PhysicalFilesystem::open(directory.path(), inc).unwrap();
    assert_eq!(
        reopened.read(key, object, 0, None).unwrap().bytes,
        b"late bytes"
    );
    assert!(!reopened.delete(key, 22).unwrap().already_absent);
    failure(reopened.read(key, object, 0, None), FailureKind::Missing);
    assert!(reopened.delete(key, 22).unwrap().already_absent);
}

#[test]
fn corrupt_bytes_outside_requested_range_are_not_exposed_as_verified() {
    let directory = TempDir::new().unwrap();
    let store =
        PhysicalFilesystem::bootstrap(directory.path(), incarnation(1), Limits::default()).unwrap();
    let key = key(incarnation(1), 1);
    let data = vec![7; 100_000];
    let object = publish(&store, key, 1, &data);
    let path = object_path(&store, key);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[8]).unwrap();
    failure(store.read(key, object, 0, Some(4)), FailureKind::Corrupt);
    failure(store.read(key, object, 0, Some(0)), FailureKind::Corrupt);
    let mut retry = store.begin_write(key, 2, object).unwrap();
    retry.append(&data).unwrap();
    failure(retry.finish(), FailureKind::Corrupt);
    file.set_len(20).unwrap();
    failure(store.read(key, object, 0, None), FailureKind::Corrupt);
    drop(file);
    fs::remove_file(&path).unwrap();
    failure(store.read(key, object, 0, None), FailureKind::Missing);
    fs::remove_dir(store.namespace_path().join("objects-v1")).unwrap();
    failure(store.read(key, object, 0, None), FailureKind::Unavailable);
    failure(store.delete(key, 1), FailureKind::Unavailable);
    fs::remove_dir(store.namespace_path().join("staging-v1")).unwrap();
    failure(store.begin_write(key, 3, object), FailureKind::Unavailable);
}

#[test]
fn admission_and_staging_bounds_do_not_overwrite_or_leak_completed_stages() {
    let directory = TempDir::new().unwrap();
    let store = PhysicalFilesystem::bootstrap(
        directory.path(),
        incarnation(1),
        Limits {
            object_bytes: 8,
            objects: 1,
            staged_writes: 1,
            concurrent_reads: 1,
        },
    )
    .unwrap();
    let first = key(incarnation(1), 1);
    let second = key(incarnation(1), 2);
    let object = ObjectId::for_bytes(Algorithm::Sha256, b"small");
    let mut stage = store.begin_write(first, 1, object).unwrap();
    failure(store.begin_write(second, 2, object), FailureKind::Capacity);
    stage.append(b"small").unwrap();
    stage.finish().unwrap();
    assert_eq!(
        fs::read_dir(store.namespace_path().join("staging-v1"))
            .unwrap()
            .count(),
        0
    );
    let mut second_stage = store.begin_write(second, 2, object).unwrap();
    second_stage.append(b"small").unwrap();
    failure(second_stage.finish(), FailureKind::Capacity);
    assert_eq!(store.read(first, object, 0, None).unwrap().bytes, b"small");
    let oversized = ObjectId::for_bytes(Algorithm::Sha256, b"too large");
    failure(
        store.begin_write(second, 3, oversized),
        FailureKind::Capacity,
    );
}

const PROCESS_ENV: &str = "NATIVELINK_PHYSICAL_PROCESS_FIXTURE";

#[test]
fn physical_process_cutpoint() {
    let Some(base) = std::env::var_os(PROCESS_ENV) else {
        return;
    };
    let mode = std::env::var("NATIVELINK_PHYSICAL_PROCESS_MODE").unwrap();
    let store = PhysicalFilesystem::open(Path::new(&base), incarnation(1)).unwrap();
    let key = key(incarnation(1), 1);
    let object = ObjectId::for_bytes(Algorithm::Sha256, b"durable bytes");
    let mut stage = store.begin_write(key, 1, object).unwrap();
    if mode == "partial" {
        stage.append(b"durable").unwrap();
    } else {
        stage.append(b"durable bytes").unwrap();
        stage.finish().unwrap();
    }
    // No Rust destructors run: lock release and staging cleanup differ from a
    // drop/reopen test, while the process exit is observed by the parent.
    std::process::exit(17);
}

#[test]
fn acknowledged_data_survives_process_exit_and_partial_upload_stays_unpublished() {
    for mode in ["acknowledged", "partial"] {
        let directory = TempDir::new().unwrap();
        let store =
            PhysicalFilesystem::bootstrap(directory.path(), incarnation(1), Limits::default())
                .unwrap();
        drop(store);
        let status = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "physical_process_cutpoint", "--nocapture"])
            .env(PROCESS_ENV, directory.path())
            .env("NATIVELINK_PHYSICAL_PROCESS_MODE", mode)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(17));
        let store = PhysicalFilesystem::open(directory.path(), incarnation(1)).unwrap();
        let key = key(incarnation(1), 1);
        let object = ObjectId::for_bytes(Algorithm::Sha256, b"durable bytes");
        if mode == "acknowledged" {
            assert_eq!(
                store.read(key, object, 0, None).unwrap().bytes,
                b"durable bytes"
            );
        } else {
            failure(store.read(key, object, 0, None), FailureKind::Missing);
            failure(store.begin_write(key, 1, object), FailureKind::Conflict);
            store.discard_stage(key, 1).unwrap();
        }
        // Lost success and known abandoned staging preserve the exact locator.
        publish(&store, key, 1, b"durable bytes");
    }
}
