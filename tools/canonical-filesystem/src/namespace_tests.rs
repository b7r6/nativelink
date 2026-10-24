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

use std::sync::{Arc, mpsc};
use std::time::Duration;

use rustix::fs::{FileType, mknodat};
use sha2::{Digest, Sha256};

use super::*;

fn binding() -> Binding {
    Binding {
        version: 1,
        instance: "build-μ".into(),
        node: 0x0102_0304_0506_0708,
        epoch: 9,
        authority_namespace: 10,
        principal: "team".into(),
        cas_namespace: "cas-A".into(),
        sink_namespace: "ac-B".into(),
        worker: 11,
        execution_configuration: "exec-v1".into(),
        roots: [
            Role::Journal,
            Role::Cas,
            Role::TemporaryActionCache,
            Role::Worker,
        ]
        .into_iter()
        .zip(100..104)
        .map(|(role, device)| RootBinding {
            role,
            identity: Identity {
                device,
                inode: device + 100,
            },
        })
        .collect(),
    }
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
fn commitment_matches_independent_preimage_and_string_framing() {
    // Generated independently with Python struct.pack('<I'/'<Q'/'<BQQ') and
    // hashlib.sha256, not with the implementation's streaming encoder.
    let encoded = include_str!("../fixtures/namespace-v1.hex").trim();
    assert_eq!(encoded.len() % 2, 0);
    let preimage: Vec<_> = (0..encoded.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&encoded[index..index + 2], 16).unwrap())
        .collect();
    let expected = [
        89, 139, 179, 192, 157, 55, 0, 70, 179, 224, 117, 131, 71, 108, 101, 155, 25, 29, 39, 43,
        2, 177, 146, 35, 140, 242, 76, 16, 149, 226, 190, 52,
    ];
    assert_eq!(preimage.len(), 186);
    assert_eq!(<[u8; 32]>::from(Sha256::digest(&preimage)), expected);
    assert_eq!(binding().namespace_commitment().unwrap(), expected);

    let mut left = binding();
    left.principal = "ab".into();
    left.cas_namespace = "c".into();
    let mut right = left.clone();
    right.principal = "a".into();
    right.cas_namespace = "bc".into();
    assert_ne!(
        left.namespace_commitment().unwrap(),
        right.namespace_commitment().unwrap()
    );
}

#[test]
fn every_bound_field_changes_the_commitment_or_refuses_invalid_roots() {
    let original = binding();
    let expected = original.namespace_commitment().unwrap();
    for field in 0..18 {
        let mut changed = original.clone();
        match field {
            0 => changed.version += 1,
            1 => changed.instance.push('x'),
            2 => changed.node += 1,
            3 => changed.epoch += 1,
            4 => changed.authority_namespace += 1,
            5 => changed.principal.push('x'),
            6 => changed.cas_namespace.push('x'),
            7 => changed.sink_namespace.push('x'),
            8 => changed.worker += 1,
            9 => changed.execution_configuration.push('x'),
            10..=13 => changed.roots[field - 10].identity.device += 1,
            14..=17 => changed.roots[field - 14].identity.inode += 1,
            _ => unreachable!(),
        }
        assert_ne!(
            changed.namespace_commitment().unwrap(),
            expected,
            "field {field}"
        );
    }
    for index in 0..4 {
        let mut changed = original.clone();
        changed.roots[index].role = if index == 0 { Role::Cas } else { Role::Journal };
        assert_eq!(
            changed.namespace_commitment().unwrap_err().code,
            "invalid_namespace_roots"
        );
    }
    for count in [0, 3, 5] {
        let mut changed = original.clone();
        changed.roots.resize(count, original.roots[0].clone());
        assert_eq!(
            changed.namespace_commitment().unwrap_err().code,
            "invalid_namespace_roots"
        );
    }
    assert_eq!(
        crate::namespace::nonzero_commitment([0; 32])
            .unwrap_err()
            .code,
        "unbound_namespace_commitment"
    );
}

#[test]
fn cold_reopen_and_excluded_facts_preserve_the_commitment() {
    let mut fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let expected = owner.resynchronize().unwrap().commitment();
    for field in 0..7 {
        let mut manifest = fixture.manifest.clone();
        match field {
            0 => manifest.worker.incarnation_id += 1,
            1 => manifest.worker.capacity.cpu_millis += 1,
            2 => manifest.worker.capacity.memory_bytes += 1,
            3 => manifest.worker.capacity.slots += 1,
            4 => manifest.listeners.client.bind_address.set_port(43210),
            5 => manifest.listeners.client.certificate_file = "/different/certificate.pem".into(),
            6 => manifest.worker.transfer.server_name = "changed.example".into(),
            _ => unreachable!(),
        }
        let reopened = Arc::new(Ownership::reopen(&manifest).unwrap());
        assert_eq!(reopened.resynchronize().unwrap().commitment(), expected);
    }
    for invalid in 0..4 {
        let mut manifest = fixture.manifest.clone();
        match invalid {
            0 => manifest.worker.incarnation_id = 0,
            1 => manifest.worker.capacity.cpu_millis = 0,
            2 => manifest.worker.capacity.memory_bytes = 0,
            3 => manifest.worker.capacity.slots = 0,
            _ => unreachable!(),
        }
        assert_eq!(
            Ownership::reopen(&manifest).unwrap_err().code,
            "zero_identity_or_capacity"
        );
    }
    // Only spelling changes: the retained directory identity moves with it.
    let moved = fixture.directory.join("relocated-cas");
    fs::rename(&fixture.manifest.cas.root, &moved).unwrap();
    fixture.manifest.cas.root = moved.to_str().unwrap().into();
    let reopened = Arc::new(Ownership::reopen(&fixture.manifest).unwrap());
    assert_eq!(reopened.resynchronize().unwrap().commitment(), expected);
    // Matching bytes do not grant TLS, capacity, incarnation or move approval.
}

#[test]
fn existing_record_hard_link_acceptance_is_preserved() {
    let fixture = Fixture::new();
    drop(Ownership::claim_cold(&fixture.manifest).unwrap());
    fs::hard_link(fixture.record(3), fixture.directory.join("record-alias")).unwrap();
    let reopened = Arc::new(Ownership::reopen(&fixture.manifest).unwrap());
    reopened.resynchronize().unwrap();
}

#[test]
fn invalid_last_record_refuses_before_any_sync() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let original = fs::read(fixture.record(3)).unwrap();
    for shape in 0..10 {
        fs::remove_file(fixture.record(3)).unwrap();
        match shape {
            0 => {}
            1 => symlink(fixture.record(0), fixture.record(3)).unwrap(),
            2 => fs::create_dir(fixture.record(3)).unwrap(),
            3 => {
                let root = open(fixture.roots()[3], DIRECTORY_FLAGS, Mode::empty()).unwrap();
                mknodat(
                    &root,
                    OWNERSHIP_RECORD,
                    FileType::Fifo,
                    Mode::RUSR | Mode::WUSR,
                    0,
                )
                .unwrap();
            }
            4 => fs::write(
                fixture.record(3),
                vec![b' '; usize::try_from(MAX_RECORD_BYTES).unwrap() + 1],
            )
            .unwrap(),
            5 => fs::write(fixture.record(3), b"{").unwrap(),
            6 => {
                let mut record: serde_json::Value = serde_json::from_slice(&original).unwrap();
                record["unexpected"] = true.into();
                fs::write(fixture.record(3), serde_json::to_vec(&record).unwrap()).unwrap();
            }
            7 => fs::write(fixture.record(3), fs::read(fixture.record(0)).unwrap()).unwrap(),
            8 => {
                let mut record: serde_json::Value = serde_json::from_slice(&original).unwrap();
                record["binding"]["worker"] = 9001.into();
                fs::write(fixture.record(3), serde_json::to_vec(&record).unwrap()).unwrap();
            }
            9 => {
                let mut bytes = b"{\"role\":\"Worker\",".to_vec();
                bytes.extend_from_slice(&original[1..]);
                fs::write(fixture.record(3), bytes).unwrap();
            }
            _ => unreachable!(),
        }
        let mut syncs = 0;
        let result = owner.clone().resynchronize_inner(|| syncs += 1);
        assert!(result.is_err(), "invalid record shape {shape}");
        assert_eq!(
            syncs, 0,
            "validated a late record after syncing: shape {shape}"
        );
        all_locks_held(&fixture);
        if shape == 2 {
            fs::remove_dir(fixture.record(3)).unwrap();
        } else if shape != 0 {
            fs::remove_file(fixture.record(3)).unwrap();
        }
        fs::write(fixture.record(3), &original).unwrap();
    }
}

#[test]
fn resynchronization_result_preserves_records_and_allows_explicit_retry() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let expected = owner.binding.namespace_commitment().unwrap();
    let records: Vec<_> = (0..4)
        .map(|i| fs::read(fixture.record(i)).unwrap())
        .collect();
    let result = owner.clone().resynchronize();
    // A separately recorded strace run targets each of the eight real fsyncs
    // after the eight cold-claim fsyncs. This code also checks error evidence.
    if let Ok(code) = std::env::var("NL_NAMESPACE_TEST_EXPECT_SYNC_ERROR") {
        assert_eq!(result.unwrap_err().code, code);
    } else {
        assert_eq!(result.unwrap().commitment(), expected);
    }
    for (index, bytes) in records.iter().enumerate() {
        assert_eq!(&fs::read(fixture.record(index)).unwrap(), bytes);
    }
    all_locks_held(&fixture);
    drop(owner);
    let reopened = Arc::new(Ownership::reopen(&fixture.manifest).unwrap());
    assert_eq!(reopened.resynchronize().unwrap().commitment(), expected);
    // A current successful retry cannot establish a past acknowledgement.
}

#[test]
fn namespace_token_alone_retains_every_role_lock() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let token = owner.clone().resynchronize().unwrap();
    drop(owner);
    all_locks_held(&fixture);
    assert_ne!(token.commitment(), [0; 32]);
    drop(token);
    Ownership::reopen(&fixture.manifest).unwrap();
}

#[test]
fn every_validated_record_descriptor_spans_every_sync() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let opened_records = || {
        fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|entry| fs::read_link(entry.unwrap().path()).ok())
            .filter(|path| (0..4).any(|index| *path == fixture.record(index)))
            .count()
    };
    assert_eq!(opened_records(), 0);
    let mut syncs = 0;
    let token = owner
        .resynchronize_inner(|| {
            assert_eq!(opened_records(), 4, "record descriptors closed early");
            syncs += 1;
        })
        .unwrap();
    assert_eq!(syncs, 8);
    assert_eq!(opened_records(), 0);
    all_locks_held(&fixture);
    drop(token);
}

#[test]
#[allow(
    clippy::disallowed_methods,
    reason = "Standalone synchronous ownership test has no async runtime dependency."
)]
fn dropped_waiter_keeps_blocking_operation_custody_until_completion() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let (ready_tx, ready_rx) = mpsc::channel();
    let (go_tx, go_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let worker = std::thread::Builder::new()
        .name("namespace-sync".into())
        .spawn(move || {
            let mut first = true;
            let result = owner.resynchronize_inner(|| {
                if first {
                    first = false;
                    ready_tx.send(()).unwrap();
                    go_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                }
            });
            let _ignored = result_tx.send(result);
        })
        .unwrap();
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(result_rx);
    all_locks_held(&fixture);
    go_tx.send(()).unwrap();
    worker.join().unwrap();
    Ownership::reopen(&fixture.manifest).unwrap();
}

#[test]
fn attachment_checks_refuse_before_and_after_sync() {
    let fixture = Fixture::new();
    let owner = Arc::new(Ownership::claim_cold(&fixture.manifest).unwrap());
    let path = Path::new(&fixture.manifest.worker.root);
    let moved = fixture.directory.join("moved-worker");
    for replace_during_sync in [false, true] {
        let replace = || {
            fs::rename(path, &moved).unwrap();
            fs::create_dir(path).unwrap();
        };
        if !replace_during_sync {
            replace();
        }
        let mut syncs = 0;
        let result = owner.clone().resynchronize_inner(|| {
            if syncs == 0 && replace_during_sync {
                replace();
            }
            syncs += 1;
        });
        assert_eq!(result.unwrap_err().code, "root_replaced");
        assert_eq!(syncs, if replace_during_sync { 8 } else { 0 });
        assert!(moved.join(OWNERSHIP_RECORD).is_file());
        assert!(!path.join(OWNERSHIP_RECORD).exists());
        fs::remove_dir(path).unwrap();
        fs::rename(&moved, path).unwrap();
    }
}
