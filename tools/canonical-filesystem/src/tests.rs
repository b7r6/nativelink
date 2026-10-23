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
    reason = "Fixture ownership and the fork exclusion lock must span the entire test."
)]

use std::fs;
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use nativelink_canonical_config::parse_canonical;

use super::*;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
// A concurrently spawned child can briefly inherit another test's flock FDs
// before exec closes them. Keep fixture lifetimes separate from that window.
static FIXTURE_LOCK: Mutex<()> = Mutex::new(());

#[path = "capability_tests.rs"]
mod capabilities;

#[expect(
    clippy::redundant_pub_crate,
    reason = "Shared fixture is visible only to sibling crate tests."
)]
pub(super) struct Fixture {
    pub(super) directory: PathBuf,
    pub(super) manifest: Manifest,
    _guard: MutexGuard<'static, ()>,
}

impl Fixture {
    pub(super) fn new() -> Self {
        let guard = FIXTURE_LOCK.lock().unwrap();
        let directory = std::env::temp_dir().join(format!(
            "nativelink-namespace-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&directory).unwrap();
        let directory = directory.canonicalize().unwrap();
        let mut manifest = parse_canonical(include_bytes!(
            "../../canonical-config/fixtures/canonical.json"
        ))
        .unwrap();
        let roots = ["journal", "cas", "sink", "worker"].map(|name| {
            let path = directory.join(name);
            fs::create_dir(&path).unwrap();
            path.to_str().unwrap().to_owned()
        });
        manifest.authority.journal_root.clone_from(&roots[0]);
        manifest.cas.root.clone_from(&roots[1]);
        manifest.temporary_action_cache.root.clone_from(&roots[2]);
        manifest.worker.root.clone_from(&roots[3]);
        Self {
            directory,
            manifest,
            _guard: guard,
        }
    }

    fn roots(&self) -> [&str; 4] {
        [
            &self.manifest.authority.journal_root,
            &self.manifest.cas.root,
            &self.manifest.temporary_action_cache.root,
            &self.manifest.worker.root,
        ]
    }

    fn record(&self, index: usize) -> PathBuf {
        Path::new(self.roots()[index]).join(OWNERSHIP_RECORD)
    }

    fn assert_no_records(&self) {
        for index in 0..4 {
            assert!(!self.record(index).exists());
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn rejected(result: Result<Ownership, Error>, code: &str) {
    assert_eq!(result.unwrap_err().code, code);
}

#[test]
fn cold_claim_reopen_and_same_process_conflict() {
    let fixture = Fixture::new();
    let owned = Ownership::claim_cold(&fixture.manifest).unwrap();
    for index in 0..4 {
        assert!(fixture.record(index).is_file());
    }
    rejected(
        Ownership::reopen(&fixture.manifest),
        "exclusive_ownership_unavailable",
    );
    drop(owned);
    let reopened = Ownership::reopen(&fixture.manifest).unwrap();
    reopened.verify_attached().unwrap();
    drop(reopened);
    rejected(
        Ownership::claim_cold(&fixture.manifest),
        "cold_root_not_empty",
    );
}

#[test]
fn exclusive_lock_cross_process() {
    const CHILD_MANIFEST: &str = "NL_NAMESPACE_TEST_CHILD_MANIFEST";
    const CHILD_EXPECT: &str = "NL_NAMESPACE_TEST_CHILD_EXPECT";
    if let Some(path) = std::env::var_os(CHILD_MANIFEST) {
        let manifest = parse_canonical(&fs::read(path).unwrap()).unwrap();
        let result = Ownership::reopen(&manifest);
        if std::env::var(CHILD_EXPECT).unwrap() == "locked" {
            rejected(result, "exclusive_ownership_unavailable");
        } else {
            result.unwrap();
        }
        return;
    }
    let fixture = Fixture::new();
    let owned = Ownership::claim_cold(&fixture.manifest).unwrap();
    let manifest_file = fixture.directory.join("manifest.json");
    fs::write(
        &manifest_file,
        serde_json::to_vec(&fixture.manifest).unwrap(),
    )
    .unwrap();
    let child = |expect| {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::exclusive_lock_cross_process",
                "--test-threads=1",
            ])
            .env(CHILD_MANIFEST, &manifest_file)
            .env(CHILD_EXPECT, expect)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    };
    child("locked");
    drop(owned);
    child("free");
}

#[test]
fn cold_checks_last_root_before_mutating_first() {
    let fixture = Fixture::new();
    fs::write(
        Path::new(&fixture.manifest.worker.root).join("old-data"),
        b"retained",
    )
    .unwrap();
    rejected(
        Ownership::claim_cold(&fixture.manifest),
        "cold_root_not_empty",
    );
    fixture.assert_no_records();
    assert_eq!(
        fs::read(Path::new(&fixture.manifest.worker.root).join("old-data")).unwrap(),
        b"retained"
    );
}

#[test]
fn missing_root_does_not_create_anything() {
    let fixture = Fixture::new();
    fs::remove_dir(&fixture.manifest.worker.root).unwrap();
    rejected(
        Ownership::claim_cold(&fixture.manifest),
        "unsafe_or_unavailable_path",
    );
    fixture.assert_no_records();
    assert!(!Path::new(&fixture.manifest.worker.root).exists());
}

#[test]
fn symbolic_root_alias_and_symbolic_ancestor_refused() {
    let fixture = Fixture::new();
    fs::remove_dir(&fixture.manifest.temporary_action_cache.root).unwrap();
    symlink(
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
    )
    .unwrap();
    rejected(
        Ownership::claim_cold(&fixture.manifest),
        "unsafe_or_unavailable_path",
    );
    fixture.assert_no_records();
    fs::remove_file(&fixture.manifest.temporary_action_cache.root).unwrap();
    fs::create_dir(&fixture.manifest.temporary_action_cache.root).unwrap();
    let alias = fixture.directory.join("alias");
    symlink(&fixture.directory, &alias).unwrap();
    let mut manifest = fixture.manifest.clone();
    manifest.cas.root = alias.join("cas").to_str().unwrap().to_owned();
    rejected(
        Ownership::claim_cold(&manifest),
        "unsafe_or_unavailable_path",
    );
    fixture.assert_no_records();
}

#[test]
fn overlapping_and_lexical_alias_roots_refused_before_io() {
    let fixture = Fixture::new();
    for path in [
        fixture.manifest.cas.root.clone(),
        format!("{}/child", fixture.manifest.cas.root),
    ] {
        let mut manifest = fixture.manifest.clone();
        manifest.authority.journal_root = path;
        rejected(Ownership::claim_cold(&manifest), "overlapping_roots");
    }
    let mut manifest = fixture.manifest.clone();
    manifest.cas.root.push_str("/../cas");
    rejected(Ownership::claim_cold(&manifest), "literal_absolute_path");
    fixture.assert_no_records();
}

#[test]
fn wrong_domain_namespace_and_role_do_not_rewrite_records() {
    let fixture = Fixture::new();
    drop(Ownership::claim_cold(&fixture.manifest).unwrap());
    let original: Vec<_> = (0..4)
        .map(|i| fs::read(fixture.record(i)).unwrap())
        .collect();
    let mut wrong = fixture.manifest.clone();
    wrong.authority.cluster_epoch += 1;
    rejected(Ownership::reopen(&wrong), "ownership_mismatch");
    let mut wrong = fixture.manifest.clone();
    wrong.cas.namespace_id = "4".repeat(64);
    rejected(Ownership::reopen(&wrong), "ownership_mismatch");
    let mut wrong = fixture.manifest.clone();
    std::mem::swap(&mut wrong.cas.root, &mut wrong.temporary_action_cache.root);
    rejected(Ownership::reopen(&wrong), "ownership_mismatch");
    for (i, before) in original.iter().enumerate() {
        assert_eq!(&fs::read(fixture.record(i)).unwrap(), before);
    }
    let mut record: Record = serde_json::from_slice(&original[1]).unwrap();
    record.role = Role::Worker;
    fs::write(fixture.record(1), serde_json::to_vec(&record).unwrap()).unwrap();
    rejected(Ownership::reopen(&fixture.manifest), "ownership_mismatch");
}

#[test]
fn partial_initialization_and_torn_record_fail_closed() {
    for retained in 1..4 {
        let fixture = Fixture::new();
        drop(Ownership::claim_cold(&fixture.manifest).unwrap());
        for index in retained..4 {
            fs::remove_file(fixture.record(index)).unwrap();
        }
        let before = fs::read(fixture.record(0)).unwrap();
        rejected(Ownership::reopen(&fixture.manifest), "open_record");
        rejected(
            Ownership::claim_cold(&fixture.manifest),
            "cold_root_not_empty",
        );
        assert_eq!(fs::read(fixture.record(0)).unwrap(), before);
        for index in retained..4 {
            assert!(!fixture.record(index).exists());
        }
    }
    let fixture = Fixture::new();
    drop(Ownership::claim_cold(&fixture.manifest).unwrap());
    fs::write(fixture.record(2), b"{\"version\":").unwrap();
    rejected(Ownership::reopen(&fixture.manifest), "invalid_record");
    rejected(
        Ownership::claim_cold(&fixture.manifest),
        "cold_root_not_empty",
    );
    assert_eq!(fs::read(fixture.record(2)).unwrap(), b"{\"version\":");
}

#[test]
fn replaced_directory_and_copied_record_refused() {
    let fixture = Fixture::new();
    let owned = Ownership::claim_cold(&fixture.manifest).unwrap();
    let old = fixture.directory.join("old-cas");
    fs::rename(&fixture.manifest.cas.root, &old).unwrap();
    fs::create_dir(&fixture.manifest.cas.root).unwrap();
    rejected(owned.verify_attached().map(|()| owned), "root_replaced");
    fs::copy(old.join(OWNERSHIP_RECORD), fixture.record(1)).unwrap();
    rejected(Ownership::reopen(&fixture.manifest), "ownership_mismatch");
}

#[test]
fn symbolic_record_and_nonregular_record_refused() {
    let fixture = Fixture::new();
    drop(Ownership::claim_cold(&fixture.manifest).unwrap());
    let original = fixture.directory.join("saved-record");
    fs::rename(fixture.record(1), &original).unwrap();
    symlink(&original, fixture.record(1)).unwrap();
    rejected(Ownership::reopen(&fixture.manifest), "open_record");
    fs::remove_file(fixture.record(1)).unwrap();
    fs::create_dir(fixture.record(1)).unwrap();
    rejected(Ownership::reopen(&fixture.manifest), "invalid_record");
}

#[test]
fn caller_mutation_cannot_rebind_owned_guard() {
    let mut fixture = Fixture::new();
    let owned = Ownership::claim_cold(&fixture.manifest).unwrap();
    fixture.manifest.authority.cluster_epoch += 1;
    fixture.manifest.worker.incarnation_id += 1;
    owned.verify_attached().unwrap();
    assert_ne!(
        owned.binding.epoch,
        fixture.manifest.authority.cluster_epoch
    );
    drop(owned);
    rejected(Ownership::reopen(&fixture.manifest), "ownership_mismatch");
    fixture.manifest.authority.cluster_epoch -= 1;
    // A different incarnation changes no namespace identity. This API neither
    // registers it nor authorizes reuse of the original incarnation.
    Ownership::reopen(&fixture.manifest).unwrap();
}

#[test]
fn duplicate_unknown_and_oversized_records_refused_without_writes() {
    let fixture = Fixture::new();
    drop(Ownership::claim_cold(&fixture.manifest).unwrap());
    let original = fs::read(fixture.record(1)).unwrap();
    let mut duplicate = br#"{"role":"Cas","#.to_vec();
    duplicate.extend_from_slice(&original[1..]);
    let mut unknown: serde_json::Value = serde_json::from_slice(&original).unwrap();
    unknown["extra"] = true.into();
    let mut oversized = original.clone();
    oversized.resize(usize::try_from(MAX_RECORD_BYTES).unwrap() + 1, b' ');
    for invalid in [duplicate, serde_json::to_vec(&unknown).unwrap(), oversized] {
        fs::write(fixture.record(1), &invalid).unwrap();
        rejected(Ownership::reopen(&fixture.manifest), "invalid_record");
        assert_eq!(fs::read(fixture.record(1)).unwrap(), invalid);
    }
}

#[test]
fn oversized_binding_refused_before_first_record() {
    let fixture = Fixture::new();
    let mut manifest = fixture.manifest.clone();
    manifest.authority.principal = "p".repeat(usize::try_from(MAX_RECORD_BYTES).unwrap());
    rejected(Ownership::claim_cold(&manifest), "record_too_large");
    fixture.assert_no_records();
}
