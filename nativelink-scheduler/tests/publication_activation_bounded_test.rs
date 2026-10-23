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

#[path = "publication_activation_v2/bounded.rs"]
mod bounded;
#[path = "publication_activation_v2/reference.rs"]
mod reference;
#[path = "publication_activation_v2/types.rs"]
mod types;

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest as _, Sha256};
use types::*;

fn fixture(owners: u64, members: u64, large_payload: bool) -> (Legacy, Facts, Activation) {
    let old = Namespace([1; 16]);
    let new = Namespace([2; 16]);
    let cas = Namespace([3; 16]);
    let bytes = if large_payload {
        vec![1; MAX_PAYLOAD_BYTES]
    } else {
        b"result".to_vec()
    };
    let response = if large_payload {
        vec![2; MAX_PAYLOAD_BYTES]
    } else {
        b"response".to_vec()
    };
    let value = TerminalValue::Completed(Payload {
        digest: "original".into(),
        exit_code: 0,
        error: None,
        response: Some(CapturedResponse {
            version: 1,
            sha256: Sha256::digest(&response).into(),
            encoded: response,
        }),
        result: Some(ResultGraph {
            object: descriptor(&bytes),
            bytes,
        }),
    });
    let result = value.result().unwrap().object.clone();
    let closure: BTreeSet<_> = (0..members)
        .map(|index| Member {
            object: if index == 0 {
                result.clone()
            } else {
                descriptor(format!("blob {index}").as_bytes())
            },
            generation: Generation {
                namespace: cas,
                incarnation: [9; 16],
                number: index + 1,
            },
            role: if index == 0 { Role::Result } else { Role::Blob },
        })
        .collect();
    let roots: BTreeMap<_, _> = (0..owners)
        .map(|id| {
            (
                id + 1,
                Root {
                    id: id + 1,
                    subject: if id == 0 {
                        Subject::ExecutionResult(1)
                    } else {
                        Subject::OperationResult(id)
                    },
                    object: result.clone(),
                    required: closure.clone(),
                },
            )
        })
        .collect();
    let proofs: BTreeMap<_, _> = roots
        .iter()
        .map(|(&id, root)| {
            (
                id,
                Protection {
                    id,
                    root: id,
                    catalogue_revision: 1,
                    complete: true,
                    verified: true,
                    typed: root.required.clone(),
                    protected: root
                        .required
                        .iter()
                        .map(|member| member.generation)
                        .collect(),
                    fences: BTreeSet::from([Fence {
                        namespace: cas,
                        revision: 1,
                    }]),
                    until: 100,
                },
            )
        })
        .collect();
    let legacy = Legacy {
        old_sink: old,
        domain_exposure: Exposure::Opaque,
        terminals: BTreeMap::from([(
            1,
            ExecutionTerminal {
                id: 1,
                action: "qualified".into(),
                provenance: Provenance::Unrecorded,
                value: value.clone(),
                terminal_at: Some(1),
                ac_eligible: true,
            },
        )]),
        operations: (1..owners)
            .map(|id| {
                (
                    id,
                    OperationTerminal {
                        id,
                        execution: 1,
                        value: value.clone(),
                        provenance: OperationProvenance::Unrecorded,
                        version: 1,
                        interested: true,
                        interest_deadline: 100,
                    },
                )
            })
            .collect(),
        effects: BTreeMap::new(),
        histories: BTreeMap::new(),
        roots,
        late_outcomes: BTreeSet::new(),
        request_quiescence: BTreeSet::new(),
    };
    let facts = Facts {
        now: 10,
        catalogue_revision: 1,
        catalogue: closure
            .iter()
            .map(|member| CatalogueRow {
                key: member.generation,
                object: member.object.clone(),
                state: Readiness::Ready,
            })
            .collect(),
        protections: proofs,
        fences: BTreeSet::from([Fence {
            namespace: cas,
            revision: 1,
        }]),
        bindings: [old, new, cas]
            .into_iter()
            .map(|namespace| Binding {
                namespace,
                physical_domain: [namespace.0[0]; 32],
                old_requests_confined: namespace == old,
            })
            .collect(),
        cold: BTreeSet::from([new]),
        ..Facts::default()
    };
    let activation = Activation {
        commit: 20,
        new_namespace: new,
        dispositions: BTreeMap::new(),
        imports: vec![],
        protections: (1..=owners).map(|id| (id, id)).collect(),
    };
    (legacy, facts, activation)
}

fn descriptor(bytes: &[u8]) -> Object {
    Object {
        algorithm: Algorithm::Sha256,
        hash: Sha256::digest(bytes).into(),
        bytes: bytes.len() as u64,
    }
}

#[test]
fn frozen_reference_counterexample_is_preserved_and_guard_refuses_atomically() {
    let (legacy, facts, activation) = fixture(32, 32, false);
    let mut original = reference::Model::new(legacy.clone()).unwrap();
    assert!(original.snapshot().is_ok());
    original
        .apply(&Event::Activate(activation.clone()), &facts)
        .unwrap();
    assert_eq!(original.snapshot(), Err(Reject::Bounds));
    let mut guarded = bounded::Model::new(legacy).unwrap();
    let before = guarded.snapshot().unwrap();
    assert_eq!(
        guarded.apply(&Event::Activate(activation), &facts),
        Err(Reject::Bounds)
    );
    assert_eq!(guarded.snapshot().unwrap(), before);
    assert!(guarded.view().activation.is_none());
}

#[test]
fn constructor_refuses_structurally_valid_oversized_inventory() {
    let (legacy, _, _) = fixture(32, 32, true);
    let original = reference::Model::new(legacy.clone()).unwrap();
    assert_eq!(original.snapshot(), Err(Reject::Bounds));
    assert_eq!(bounded::Model::new(legacy), Err(Reject::Bounds));
}

#[test]
fn ordinary_activation_restore_and_collection_remain_identical() {
    let (legacy, facts, activation) = fixture(2, 2, false);
    let mut original = reference::Model::new(legacy.clone()).unwrap();
    let mut guarded = bounded::Model::new(legacy.clone()).unwrap();
    for event in [
        Event::Activate(activation),
        Event::EnableCollection {
            namespace: Namespace([2; 16]),
            through: 90,
        },
    ] {
        assert_eq!(
            original.apply(&event, &facts),
            guarded.apply(&event, &facts)
        );
        assert_eq!(&original, guarded.view());
        guarded = bounded::Model::restore(&guarded.snapshot().unwrap(), &legacy).unwrap();
    }
    assert!(guarded.view().may_collect(Namespace([2; 16]), 50));
    assert!(guarded.view().unresolved_requests().is_empty());
    assert!(guarded.view().unresolved_effects().is_empty());
    assert!(guarded.view().unresolved_legacy_domain());
    assert!(guarded.view().pending_history().is_empty());
}

#[test]
fn every_transition_checks_bytes_including_later_protection_refresh() {
    let (mut legacy, mut facts, activation, mut gap) = (2..=32)
        .rev()
        .find_map(|owners| {
            let (mut legacy, facts, activation) = fixture(owners, 32, false);
            for op in legacy.operations.values_mut() {
                if let TerminalValue::Completed(payload) = &mut op.value {
                    payload.error = Some(String::new());
                }
            }
            let mut original = reference::Model::new(legacy.clone()).unwrap();
            original
                .apply(&Event::Activate(activation.clone()), &facts)
                .unwrap();
            let bytes = serde_json::to_vec(&original).unwrap().len();
            (bytes <= MAX_SNAPSHOT_BYTES
                && MAX_SNAPSHOT_BYTES - bytes <= legacy.operations.len() * MAX_PAYLOAD_BYTES)
                .then(|| (legacy, facts, activation, MAX_SNAPSHOT_BYTES - bytes))
        })
        .unwrap();
    for op in legacy.operations.values_mut() {
        let add = gap.min(MAX_PAYLOAD_BYTES);
        if let TerminalValue::Completed(payload) = &mut op.value {
            payload.error = Some("a".repeat(add));
        }
        gap -= add;
    }
    assert_eq!(gap, 0);
    let mut guarded = bounded::Model::new(legacy.clone()).unwrap();
    guarded.apply(&Event::Activate(activation), &facts).unwrap();
    let before = guarded.snapshot().unwrap();
    assert_eq!(before.len(), MAX_SNAPSHOT_BYTES);
    facts.protections.get_mut(&1).unwrap().until = u64::MAX;
    assert_eq!(
        guarded.apply(
            &Event::Protect {
                root: 1,
                evidence: 1
            },
            &facts
        ),
        Err(Reject::Bounds)
    );
    assert_eq!(guarded.snapshot().unwrap(), before);
    assert_eq!(bounded::Model::restore(&before, &legacy).unwrap(), guarded);
}
