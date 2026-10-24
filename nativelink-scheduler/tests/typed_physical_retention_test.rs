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

//! Test-only typed preparation → exact catalogue/fence checks → durable cohort
//! protection. This does not install production publication or authority state.

#[path = "retention/coarse.rs"]
mod coarse;
#[path = "retention/protocol.rs"]
mod protocol;
#[path = "retention/reference.rs"]
mod reference;
#[path = "retention/typed_bridge.rs"]
mod typed_bridge;

use std::collections::BTreeSet;
use std::fmt::Write;
use std::fs;
use std::path::PathBuf;

use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, FileNode, OutputDirectory, Tree,
};
use nativelink_store::physical_closure::Failure as ClosureFailure;
use nativelink_store::physical_filesystem::{Algorithm, FailureKind, ObjectId};
use prost::Message;
use protocol::*;
use typed_bridge::{Bridge, Cut, Error, Pending, Row};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "nativelink-typed-retention-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&path).unwrap();
        fs::create_dir(path.join("authority")).unwrap();
        fs::create_dir(path.join("physical")).unwrap();
        Self(path)
    }
    fn checkpoint(&self) -> PathBuf {
        self.0.join("authority/checkpoint")
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[derive(Clone, Copy)]
struct Output {
    file: Row,
    tree: Row,
    result: Row,
}
impl Output {
    const fn rows(self) -> [Row; 3] {
        [self.file, self.tree, self.result]
    }
    fn keys(self) -> Vec<Key> {
        self.rows().into_iter().map(|row| row.key).collect()
    }
}

fn digest(row: Row) -> Digest {
    let object = row.binding().object;
    let mut hash = String::with_capacity(64);
    for byte in object.hash {
        write!(hash, "{byte:02x}").unwrap();
    }
    Digest {
        hash,
        size_bytes: i64::try_from(object.size).unwrap(),
    }
}

fn output(bridge: &mut Bridge, payload: &[u8], cohort: CohortId, replace: bool) -> Output {
    let file = bridge.upload(payload, cohort, replace).unwrap();
    let tree = bridge
        .upload(
            &Tree {
                root: Some(Directory {
                    files: vec![FileNode {
                        name: "f".into(),
                        digest: Some(digest(file)),
                        ..FileNode::default()
                    }],
                    ..Directory::default()
                }),
                children: vec![],
            }
            .encode_to_vec(),
            cohort,
            replace,
        )
        .unwrap();
    let result = bridge
        .upload(
            &ActionResult {
                output_directories: vec![OutputDirectory {
                    path: "out".into(),
                    tree_digest: Some(digest(tree)),
                    is_topologically_sorted: true,
                }],
                stdout_digest: Some(digest(file)),
                ..ActionResult::default()
            }
            .encode_to_vec(),
            cohort,
            replace,
        )
        .unwrap();
    Output { file, tree, result }
}

fn complete(bridge: &Bridge, result: Row) -> Pending {
    let mut pending = bridge.begin(result).unwrap();
    while !bridge.advance(&mut pending, 1).unwrap() {}
    pending
}

#[test]
fn incomplete_preparation_has_no_head_or_protection_and_is_count_and_time_bounded() {
    let fixture = Fixture::new();
    let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
    let output = output(&mut bridge, b"output", CohortId(1), false);
    assert_eq!(Bridge::reopen(&fixture.0).err(), Some(Error::Storage));
    let before = bridge.view();
    let checkpoint = fs::read(fixture.checkpoint()).unwrap();
    let mut pending = bridge.begin(output.result).unwrap();
    assert!(!bridge.advance(&mut pending, 1).unwrap());
    assert_eq!(bridge.head().unwrap(), None);
    assert!(bridge.view().protected.is_empty());
    assert_eq!(
        bridge.publish(pending, RootId(10), 1, 10),
        Err(Error::Closure(ClosureFailure::Incomplete))
    );
    assert_eq!(bridge.view(), before);
    assert_eq!(fs::read(fixture.checkpoint()).unwrap(), checkpoint);
    let mut first = bridge.begin(output.result).unwrap();
    let second = bridge.begin(output.result).unwrap();
    assert_eq!(bridge.begin(output.result).err(), Some(Error::Capacity));
    bridge.event(Event::AdvanceTime { now: 5 }).unwrap();
    assert_eq!(bridge.advance(&mut first, 1), Err(Error::Expired));
    assert_eq!(
        bridge.publish(first, RootId(10), 1, 10),
        Err(Error::Expired)
    );
    drop(second);
    let pending = complete(&bridge, output.result);
    bridge.publish(pending, RootId(10), 1, 10).unwrap();
    assert_eq!(bridge.view().protected, output.keys().into_iter().collect());
}

#[test]
fn retirement_and_delete_between_verification_and_acquisition_cannot_gain_protection() {
    for delete in [false, true] {
        let fixture = Fixture::new();
        let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
        let output = output(&mut bridge, b"output", CohortId(1), false);
        let prepared_before_retirement = complete(&bridge, output.result);
        let fence = bridge.close(CohortId(1)).unwrap();
        bridge
            .event(Event::Retire {
                cohort: CohortId(1),
                fence,
                keys: vec![output.file.key],
            })
            .unwrap();
        if delete {
            bridge.delete_current(output.file).unwrap();
        }
        bridge
            .event(Event::Reopen {
                cohort: CohortId(1),
                fence,
            })
            .unwrap();
        let before = bridge.view();
        assert_eq!(
            bridge.publish(prepared_before_retirement, RootId(10), 1, 10),
            Err(Error::Rejected)
        );
        assert_eq!(bridge.view(), before);
        assert_eq!(bridge.head().unwrap(), None);
        assert!(bridge.view().protected.is_empty());
        let mut fresh = bridge.begin(output.result).unwrap();
        if delete {
            let failure = loop {
                match bridge.advance(&mut fresh, 1) {
                    Ok(false) => {}
                    Ok(true) => panic!("missing required file accepted"),
                    Err(error) => break error,
                }
            };
            assert!(
                matches!(failure, Error::Closure(ClosureFailure::Physical(error)) if error.kind == FailureKind::Missing)
            );
        } else {
            // Physical bytes can still verify after retirement. Current fences
            // alone cannot make the candidate grant a retired generation.
            while !bridge.advance(&mut fresh, 1).unwrap() {}
            assert_eq!(
                bridge.publish(fresh, RootId(10), 1, 10),
                Err(Error::Rejected)
            );
        }
    }
}

#[test]
fn stale_catalogue_revision_rebinding_and_preparation_fences_are_rejected() {
    for rebind in [false, true] {
        let fixture = Fixture::new();
        let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
        let output = output(&mut bridge, b"output", CohortId(1), false);
        let pending = complete(&bridge, output.result);
        let new_row = bridge
            .upload(
                if rebind { b"output" } else { b"unrelated" },
                CohortId(2),
                rebind,
            )
            .unwrap();
        assert_eq!(
            bridge.publish(pending, RootId(10), 1, 10),
            Err(Error::StaleCatalogue)
        );
        assert!(bridge.view().protected.is_empty());
        let pending = complete(&bridge, output.result);
        bridge.publish(pending, RootId(10), 1, 10).unwrap();
        let expected = if rebind {
            BTreeSet::from([new_row.key, output.tree.key, output.result.key])
        } else {
            output.keys().into_iter().collect()
        };
        assert_eq!(bridge.view().protected, expected);
        if rebind {
            assert!(!bridge.view().protected.contains(&output.file.key));
        }
    }
    let fixture = Fixture::new();
    let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
    let output = output(&mut bridge, b"output", CohortId(1), false);
    let pending = complete(&bridge, output.result);
    let fence = bridge.close(CohortId(1)).unwrap();
    bridge
        .event(Event::Reopen {
            cohort: CohortId(1),
            fence,
        })
        .unwrap();
    assert_eq!(
        bridge.publish(pending, RootId(10), 1, 10),
        Err(Error::Rejected)
    );
    let fresh = complete(&bridge, output.result);
    bridge.publish(fresh, RootId(10), 1, 10).unwrap();
}

#[test]
fn prepared_ticket_from_another_physical_incarnation_cannot_bind_equal_numeric_handles() {
    let first_fixture = Fixture::new();
    let second_fixture = Fixture::new();
    let mut first = Bridge::bootstrap(&first_fixture.0).unwrap();
    let mut second = Bridge::bootstrap(&second_fixture.0).unwrap();
    let first_output = output(&mut first, b"same bytes", CohortId(1), false);
    let second_output = output(&mut second, b"same bytes", CohortId(1), false);
    assert_eq!(first_output.file.key, second_output.file.key);
    assert_eq!(
        first_output.file.binding().object,
        second_output.file.binding().object
    );
    assert_ne!(
        first_output.file.binding().key,
        second_output.file.binding().key
    );
    let pending = complete(&first, first_output.result);
    assert_eq!(
        second.publish(pending, RootId(10), 1, 10),
        Err(Error::StaleCatalogue)
    );
    assert!(second.view().protected.is_empty());
}

#[test]
fn serving_grant_cannot_cross_namespaces_with_equal_root_lease_and_descriptor() {
    let first_fixture = Fixture::new();
    let second_fixture = Fixture::new();
    let mut first = Bridge::bootstrap(&first_fixture.0).unwrap();
    let mut second = Bridge::bootstrap(&second_fixture.0).unwrap();
    let first_output = output(&mut first, b"same bytes", CohortId(1), false);
    let second_output = output(&mut second, b"same bytes", CohortId(1), false);
    let first_pending = complete(&first, first_output.result);
    let second_pending = complete(&second, second_output.result);
    first.publish(first_pending, RootId(10), 1, 10).unwrap();
    second.publish(second_pending, RootId(10), 1, 10).unwrap();
    let first_grant = first.serve(RootId(20), 20).unwrap();
    let second_grant = second.serve(RootId(20), 20).unwrap();
    let object = first_output.file.binding().object;
    assert_eq!(object, second_output.file.binding().object);
    assert_eq!(first.read(first_grant, object).unwrap(), b"same bytes");
    assert_eq!(second.read(second_grant, object).unwrap(), b"same bytes");
    assert_eq!(first.read(second_grant, object), Err(Error::Rejected));
    assert_eq!(second.read(first_grant, object), Err(Error::Rejected));
    drop(first);
    let first = Bridge::reopen(&first_fixture.0).unwrap();
    assert_eq!(first.read(first_grant, object).unwrap(), b"same bytes");
    assert_eq!(first.read(second_grant, object), Err(Error::Rejected));
}

#[test]
fn durable_serving_coverage_survives_head_removal_and_blocks_real_delete_intents() {
    let fixture = Fixture::new();
    let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
    let output = output(&mut bridge, b"output", CohortId(1), false);
    let pending = complete(&bridge, output.result);
    bridge.publish(pending, RootId(10), 1, 10).unwrap();
    drop(bridge);
    let mut bridge = Bridge::reopen(&fixture.0).unwrap();
    assert_eq!(bridge.head().unwrap(), Some(RootId(10)));
    assert_eq!(bridge.view().protected, output.keys().into_iter().collect());
    let grant = bridge.serve(RootId(20), 20).unwrap();
    assert_eq!(bridge.view().protected, output.keys().into_iter().collect());
    drop(bridge);
    let mut bridge = Bridge::reopen(&fixture.0).unwrap();
    assert_eq!(
        bridge.read(grant, output.file.binding().object).unwrap(),
        b"output"
    );
    bridge.remove_head().unwrap();
    assert_eq!(bridge.head().unwrap(), None);
    assert_eq!(bridge.view().protected, output.keys().into_iter().collect());
    let fence = bridge.close(CohortId(1)).unwrap();
    for key in output.keys() {
        assert_eq!(
            bridge.event(Event::Retire {
                cohort: CohortId(1),
                fence,
                keys: vec![key]
            }),
            Err(Error::Rejected)
        );
    }
    assert!(bridge.view().deletes.is_empty());
    drop(bridge);
    let bridge = Bridge::reopen(&fixture.0).unwrap();
    for row in output.rows() {
        let object = row.binding().object;
        assert_eq!(
            bridge.read(grant, object).unwrap().len() as u64,
            object.size
        );
    }
    assert_eq!(
        bridge.read(
            grant,
            ObjectId::for_bytes(Algorithm::Sha256, b"not in closure")
        ),
        Err(Error::Rejected)
    );
}

#[test]
fn head_replacement_preserves_old_read_coverage_and_uncovered_new_outputs_can_collect() {
    let fixture = Fixture::new();
    let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
    let old = output(&mut bridge, b"old", CohortId(1), false);
    let pending = complete(&bridge, old.result);
    bridge.publish(pending, RootId(10), 1, 10).unwrap();
    let grant = bridge.serve(RootId(20), 20).unwrap();
    let replacement = output(&mut bridge, b"replacement", CohortId(2), false);
    let pending = complete(&bridge, replacement.result);
    bridge.publish(pending, RootId(11), 1, 10).unwrap();
    assert_eq!(bridge.head().unwrap(), Some(RootId(11)));
    assert_eq!(
        bridge.read(grant, old.file.binding().object).unwrap(),
        b"old"
    );
    bridge.remove_head().unwrap();
    assert_eq!(bridge.view().protected, old.keys().into_iter().collect());
    let old_fence = bridge.close(CohortId(1)).unwrap();
    assert_eq!(
        bridge.event(Event::Retire {
            cohort: CohortId(1),
            fence: old_fence,
            keys: old.keys()
        }),
        Err(Error::Rejected)
    );
    let new_fence = bridge.close(CohortId(2)).unwrap();
    bridge
        .event(Event::Retire {
            cohort: CohortId(2),
            fence: new_fence,
            keys: replacement.keys(),
        })
        .unwrap();
    for row in replacement.rows() {
        bridge.delete_current(row).unwrap();
    }
    drop(bridge);
    let bridge = Bridge::reopen(&fixture.0).unwrap();
    assert_eq!(
        bridge.read(grant, old.file.binding().object).unwrap(),
        b"old"
    );
    assert_eq!(
        bridge
            .store
            .read(
                replacement.file.binding().key,
                replacement.file.binding().object,
                0,
                None
            )
            .unwrap_err()
            .kind,
        FailureKind::Missing
    );
}

#[test]
fn expired_root_reacquisition_cannot_revive_retired_generations_even_when_bytes_remain() {
    let fixture = Fixture::new();
    let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
    let old = output(&mut bridge, b"output", CohortId(1), false);
    let pending = complete(&bridge, old.result);
    let grant = bridge.publish(pending, RootId(10), 1, 4).unwrap();
    bridge.event(Event::AdvanceTime { now: 4 }).unwrap();
    assert_eq!(
        bridge.read(grant, old.file.binding().object),
        Err(Error::Expired)
    );
    assert_eq!(bridge.serve(RootId(20), 10), Err(Error::Expired));
    assert_eq!(
        bridge.event(Event::Renew {
            root: RootId(10),
            lease: 1,
            until: 10
        }),
        Err(Error::Rejected)
    );
    let fence = bridge.close(CohortId(1)).unwrap();
    bridge
        .event(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: old.keys(),
        })
        .unwrap();
    bridge
        .event(Event::Reopen {
            cohort: CohortId(1),
            fence,
        })
        .unwrap();
    let pending = complete(&bridge, old.result);
    assert_eq!(
        bridge.publish(pending, RootId(10), 2, 10),
        Err(Error::Rejected)
    );
    assert!(bridge.view().protected.is_empty());
    for row in old.rows() {
        bridge.delete_current(row).unwrap();
    }
    let mut absent = bridge.begin(old.result).unwrap();
    assert!(
        matches!(bridge.advance(&mut absent, 1), Err(Error::Closure(ClosureFailure::Physical(error))) if error.kind == FailureKind::Missing)
    );
    drop(absent);
    let replacement = output(&mut bridge, b"output", CohortId(2), true);
    assert_ne!(replacement.file.binding().key, old.file.binding().key);
    let pending = complete(&bridge, replacement.result);
    bridge.publish(pending, RootId(11), 1, 10).unwrap();
    assert_eq!(
        bridge.view().protected,
        replacement.keys().into_iter().collect()
    );
    assert!(
        old.keys()
            .iter()
            .all(|key| bridge.view().generations[key].retired)
    );
}

#[test]
fn persistence_cuts_expose_no_success_and_post_rename_ambiguity_can_recover_a_grant() {
    for cut in [Cut::BeforeRename, Cut::AfterRename, Cut::AfterDurability] {
        let fixture = Fixture::new();
        let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
        let output = output(&mut bridge, b"output", CohortId(1), false);
        let pending = complete(&bridge, output.result);
        let before = bridge.view();
        bridge.inject(cut);
        assert_eq!(
            bridge.publish(pending, RootId(10), 1, 10),
            Err(if cut == Cut::BeforeRename {
                Error::Storage
            } else {
                Error::Ambiguous
            })
        );
        assert_eq!(bridge.head(), Err(Error::Poisoned));
        assert_eq!(bridge.view(), before);
        drop(bridge);
        let mut bridge = Bridge::reopen(&fixture.0).unwrap();
        if cut == Cut::BeforeRename {
            assert_eq!(bridge.head().unwrap(), None);
            assert!(bridge.view().protected.is_empty());
        } else {
            assert_eq!(bridge.head().unwrap(), Some(RootId(10)));
            assert_eq!(bridge.view().protected, output.keys().into_iter().collect());
            let grant = bridge.serve(RootId(20), 20).unwrap();
            drop(bridge);
            let bridge = Bridge::reopen(&fixture.0).unwrap();
            assert_eq!(
                bridge.read(grant, output.file.binding().object).unwrap(),
                b"output"
            );
        }
    }
}

fn repack(payload: &[u8]) -> Vec<u8> {
    let mut bytes = b"TYPEDR01".to_vec();
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&ObjectId::for_bytes(Algorithm::Sha256, payload).hash);
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn malformed_checkpoint_is_rejected_without_normalizing_or_truncating_physical_identity() {
    let fixture = Fixture::new();
    let mut bridge = Bridge::bootstrap(&fixture.0).unwrap();
    let output = output(&mut bridge, b"output", CohortId(1), false);
    let pending = complete(&bridge, output.result);
    bridge.publish(pending, RootId(10), 1, 10).unwrap();
    let original = fs::read(fixture.checkpoint()).unwrap();
    drop(bridge);
    let json: serde_json::Value = serde_json::from_slice(&original[48..]).unwrap();
    // Preserve canonical field order while changing high incarnation bits, so
    // encoding-order rejection cannot mask an identity-validation regression.
    let namespace = &json["metadata"]["namespace"];
    let mut changed = namespace.clone();
    changed[0] = (namespace[0].as_u64().unwrap() ^ 1).into();
    let payload = String::from_utf8(original[48..].to_vec()).unwrap();
    for algorithm in [0, 3, 255] {
        // Keep field order and every descriptor consistent, so only the
        // unsupported digest-function value can reject this restored state.
        let changed = payload.replace("\"algorithm\":1", &format!("\"algorithm\":{algorithm}"));
        assert_ne!(changed, payload);
        fs::write(fixture.checkpoint(), repack(changed.as_bytes())).unwrap();
        assert_eq!(
            Bridge::reopen(&fixture.0).err(),
            Some(Error::CorruptCheckpoint),
            "unsupported descriptor algorithm must fail closed"
        );
    }
    for field in ["namespace", "physical_incarnation"] {
        let needle = format!("\"{field}\":{namespace}");
        let changed = payload.replacen(&needle, &format!("\"{field}\":{changed}"), 1);
        assert_ne!(changed, payload);
        fs::write(fixture.checkpoint(), repack(changed.as_bytes())).unwrap();
        assert_eq!(
            Bridge::reopen(&fixture.0).err(),
            Some(Error::CorruptCheckpoint)
        );
    }
    for case in 0..7 {
        let mut value = json.clone();
        match case {
            0 => value["metadata"]["namespace"][0] = changed[0].clone(),
            1 => value["metadata"]["catalogue"][0]["physical_incarnation"][0] = changed[0].clone(),
            2 => value["metadata"]["catalogue"][0]["physical_generation"] = 999.into(),
            3 => {
                let copy = value["metadata"]["catalogue"][0].clone();
                value["metadata"]["catalogue"]
                    .as_array_mut()
                    .unwrap()
                    .push(copy);
            }
            4 => {
                value["metadata"]["manifests"][0][1]["objects"]
                    .as_array_mut()
                    .unwrap()
                    .remove(0);
            }
            5 => value["metadata"]["unknown"] = true.into(),
            6 => value["version"] = 2.into(),
            _ => unreachable!(),
        }
        assert_ne!(
            value, json,
            "corruption case {case} must change the checkpoint"
        );
        fs::write(
            fixture.checkpoint(),
            repack(&serde_json::to_vec(&value).unwrap()),
        )
        .unwrap();
        assert_eq!(
            Bridge::reopen(&fixture.0).err(),
            Some(Error::CorruptCheckpoint),
            "corruption case {case} must fail closed"
        );
    }
    for bytes in [original[..40].to_vec(), vec![0; 1024 * 1024 + 1]] {
        fs::write(fixture.checkpoint(), bytes).unwrap();
        assert_eq!(
            Bridge::reopen(&fixture.0).err(),
            Some(Error::CorruptCheckpoint)
        );
    }
    let mut corrupt = original.clone();
    corrupt[48] ^= 1;
    fs::write(fixture.checkpoint(), corrupt).unwrap();
    assert_eq!(
        Bridge::reopen(&fixture.0).err(),
        Some(Error::CorruptCheckpoint)
    );
    fs::write(fixture.checkpoint(), original).unwrap();
    assert_eq!(
        Bridge::reopen(&fixture.0).unwrap().head().unwrap(),
        Some(RootId(10))
    );
}
