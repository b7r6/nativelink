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

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
#[path = "publication_head/reference.rs"]
mod reference;
#[path = "publication_head/types.rs"]
mod types;
use reference::Model;
use types::*;

const ACTION: &str =
    "principal=tenant;instance=prod;algorithm=sha256;action=abc/20;platform=linux;exec=toolchain1";
const OTHER: &str =
    "principal=tenant;instance=prod;algorithm=blake3;action=abc/20;platform=linux;exec=toolchain1";
const UNCACHED: &str = "principal=tenant;instance=prod;algorithm=sha256;action=uncached/30;platform=linux;exec=toolchain1";

fn descriptor(bytes: &[u8], algorithm: Algorithm) -> Object {
    let hash = match algorithm {
        Algorithm::Sha256 => Sha256::digest(bytes).into(),
        Algorithm::Blake3 => *blake3::hash(bytes).as_bytes(),
    };
    Object {
        algorithm,
        hash,
        bytes: bytes.len() as u64,
    }
}
const fn key(generation: u64) -> Key {
    Key {
        namespace: [7; 16],
        incarnation: [9; 16],
        generation,
    }
}
const fn attempt(worker: u64, id: u64) -> Attempt {
    Attempt {
        id,
        worker,
        incarnation: worker * 10,
    }
}

fn fixture(algorithm: Algorithm) -> (Scenario, Facts) {
    let actions = BTreeMap::from([
        (ACTION.into(), false),
        (OTHER.into(), false),
        (UNCACHED.into(), true),
    ]);
    let blob = Member {
        object: descriptor(b"shared file stdout bytes", algorithm),
        key: key(100),
        role: Role::Blob,
    };
    let tree = Member {
        object: descriptor(b"Tree containing embedded Directory", algorithm),
        key: key(101),
        role: Role::Tree,
    };
    let mut candidates = BTreeMap::new();
    for id in 1..=8 {
        let (action, source) = match id {
            1 => (
                ACTION,
                Source::Worker {
                    execution: 101,
                    attempt: attempt(7, 1),
                },
            ),
            2 => (
                ACTION,
                Source::Worker {
                    execution: 102,
                    attempt: attempt(8, 1),
                },
            ),
            3 => (ACTION, Source::Client { principal: 77 }),
            4 => (
                UNCACHED,
                Source::Worker {
                    execution: 104,
                    attempt: attempt(7, 1),
                },
            ),
            5 => (
                UNCACHED,
                Source::Worker {
                    execution: 105,
                    attempt: attempt(8, 1),
                },
            ),
            6 => (
                ACTION,
                Source::Worker {
                    execution: 106,
                    attempt: attempt(7, 1),
                },
            ),
            7 => (
                ACTION,
                Source::Worker {
                    execution: 101,
                    attempt: attempt(8, 2),
                },
            ),
            8 => (
                OTHER,
                Source::Worker {
                    execution: 108,
                    attempt: attempt(7, 1),
                },
            ),
            _ => unreachable!(),
        };
        let bytes = format!("immutable result {id}").into_bytes();
        let result = descriptor(&bytes, algorithm);
        let required = BTreeSet::from([
            Member {
                object: result.clone(),
                key: key(id),
                role: Role::Result,
            },
            blob.clone(),
            tree.clone(),
        ]);
        candidates.insert(
            id,
            Candidate {
                id,
                action: action.into(),
                source,
                result,
                bytes,
                response: format!("stable operation response {id}").into_bytes(),
                successful: true,
                error_free: true,
                required,
            },
        );
    }
    let mut physical = BTreeMap::new();
    for candidate in candidates.values() {
        for member in &candidate.required {
            physical.insert(
                member.key,
                Physical {
                    key: member.key,
                    object: member.object.clone(),
                    ready: true,
                    retired: false,
                },
            );
        }
    }
    let facts = Facts {
        catalogue_revision: 7,
        physical: physical.into_values().collect(),
        fences: BTreeSet::from([Fence {
            namespace: [7; 16],
            revision: 9,
        }]),
        receipts: BTreeSet::new(),
    };
    (
        Scenario {
            actions,
            candidates,
        },
        facts,
    )
}

fn step(model: &mut Model, event: &Event, facts: &Facts) -> Reply {
    model.apply(model.applied + 1, event, facts).unwrap()
}
fn rejected(model: &mut Model, event: &Event, facts: &Facts) -> Reject {
    let before = model.snapshot().unwrap();
    let error = model.apply(model.applied + 1, event, facts).unwrap_err();
    assert_eq!(
        model.snapshot().unwrap(),
        before,
        "rejection must be atomic: {event:?}"
    );
    error
}
fn initialized(scenario: &Scenario, facts: &Facts) -> Model {
    let mut model = Model::new(scenario.clone()).unwrap();
    step(
        &mut model,
        &Event::Register {
            worker: 7,
            incarnation: 70,
        },
        facts,
    );
    step(
        &mut model,
        &Event::Register {
            worker: 8,
            incarnation: 80,
        },
        facts,
    );
    step(&mut model, &Event::AuthorizeClient(77), facts);
    model
}
fn request(operation: u64, execution: u64, action: &str, skip_lookup: bool) -> Request {
    Request {
        operation,
        action: action.into(),
        skip_lookup,
        execution_if_needed: execution,
        result_until: 80,
    }
}
fn start(model: &mut Model, candidate: u64, operation: u64, facts: &Facts) {
    let row = model.scenario.candidates[&candidate].clone();
    if let Source::Worker { execution, attempt } = row.source {
        assert_eq!(
            step(
                model,
                &Event::Admit(request(operation, execution, &row.action, true)),
                facts
            ),
            Reply::Started(execution)
        );
        step(model, &Event::Assign { execution, attempt }, facts);
    }
}
fn stage_event(candidate: u64) -> Event {
    Event::Stage {
        id: 10 + candidate,
        candidate,
        until: 10,
        writes: BTreeMap::from([
            (1000 + candidate * 10, key(candidate)),
            (1001 + candidate * 10, key(candidate)),
        ]),
    }
}
fn preparation(scenario: &Scenario, candidate: u64) -> Preparation {
    let row = &scenario.candidates[&candidate];
    Preparation {
        id: 100 + candidate,
        stage: 10 + candidate,
        result: row.result.clone(),
        typed: row.required.clone(),
        protected: row.required.iter().map(|member| member.key).collect(),
        catalogue_revision: 7,
        fences: BTreeSet::from([Fence {
            namespace: [7; 16],
            revision: 9,
        }]),
        complete: true,
        verified: true,
        until: 90,
    }
}
fn prepare(model: &mut Model, candidate: u64, facts: &Facts) {
    step(model, &stage_event(candidate), facts);
    let evidence = preparation(&model.scenario, candidate);
    step(
        model,
        &Event::Prepare {
            stage: 10 + candidate,
            evidence,
        },
        facts,
    );
}
fn publish(model: &mut Model, candidate: u64, facts: &Facts) {
    step(
        model,
        &Event::Publish {
            stage: 10 + candidate,
            publication: 200 + candidate,
            until: 80,
        },
        facts,
    );
}
fn receipt(scenario: &Scenario, candidate: u64, write_offset: u64) -> Receipt {
    Receipt {
        stage: 10 + candidate,
        write: 1000 + candidate * 10 + write_offset,
        key: key(candidate),
        object: scenario.candidates[&candidate].result.clone(),
        success: true,
    }
}
fn terminal(model: &Model, operation: u64) -> &Terminal {
    let OperationState::Terminal(terminal) = &model.operations[&operation].state else {
        panic!("expected terminal")
    };
    terminal
}

#[test]
fn staging_is_invisible_and_publish_atomically_installs_terminal_head_and_protection() {
    for algorithm in [Algorithm::Sha256, Algorithm::Blake3] {
        let (scenario, facts) = fixture(algorithm);
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        prepare(&mut model, 1, &facts);
        assert!(model.accepted.is_empty());
        assert!(model.heads.is_empty());
        assert!(model.index.is_empty());
        assert_eq!(model.operations[&1].state, OperationState::Waiting(101));
        let required: BTreeSet<_> = scenario.candidates[&1]
            .required
            .iter()
            .map(|member| member.key)
            .collect();
        assert!(required.is_subset(&model.protected()));
        publish(&mut model, 1, &facts);
        assert_eq!(
            terminal(&model, 1).response,
            scenario.candidates[&1].response
        );
        assert_eq!(model.heads[ACTION], 201);
        assert!(
            model.index.is_empty(),
            "acceptance precedes disposable view materialization"
        );
        assert!(required.is_subset(&model.protected()));
        assert!(
            model
                .coverage
                .iter()
                .any(|row| row.root == Root::Operation(1)
                    && row.keys == required
                    && row.until == 80)
        );
        assert!(
            model
                .coverage
                .iter()
                .any(|row| row.root == Root::Result(201)
                    && row.keys == required
                    && row.until == 80)
        );
        assert!(
            model
                .coverage
                .iter()
                .any(|row| row.root == Root::Stage(11) && row.until == model.now)
        );
        assert_eq!(model.accepted[&201].source, scenario.candidates[&1].source);
        assert_eq!(
            Model::restore(&model.snapshot().unwrap(), &scenario).unwrap(),
            model
        );
    }
}

#[test]
fn late_a_write_and_view_completion_cannot_replace_b_or_revoke_a_grant() {
    let (scenario, mut facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    let original_terminal = terminal(&model, 1).clone();
    let barrier = model.applied;
    step(
        &mut model,
        &Event::Serve {
            action: ACTION.into(),
            expected: 201,
            barrier,
            grant: 1,
            until: 90,
        },
        &facts,
    );
    step(
        &mut model,
        &Event::Revoke {
            worker: 7,
            incarnation: 70,
        },
        &facts,
    );
    start(&mut model, 2, 2, &facts);
    prepare(&mut model, 2, &facts);
    publish(&mut model, 2, &facts);
    step(&mut model, &Event::Materialize(202), &facts);
    let late = receipt(&scenario, 1, 0);
    facts.receipts.insert(late.clone());
    step(&mut model, &Event::Observe(late), &facts);
    assert_eq!(
        step(&mut model, &Event::Materialize(201), &facts),
        Reply::Ignored
    );
    assert_eq!(model.heads[ACTION], 202);
    assert_eq!(model.index[ACTION], 202);
    assert_eq!(terminal(&model, 1), &original_terminal);
    assert_eq!(terminal(&model, 2).publication, 202);
    assert!(model.unresolved_writes().contains(&(11, 1011)));
    assert!(
        model
            .coverage
            .iter()
            .any(|row| row.root == Root::Result(201) && row.until == 0)
    );
    for member in &scenario.candidates[&1].required {
        assert!(model.can_read(1, member));
    }
    let mut wrong = scenario.candidates[&1].required.first().unwrap().clone();
    wrong.key.incarnation[15] ^= 1;
    assert!(!model.can_read(1, &wrong));
}

#[test]
fn preparation_does_not_authorize_stale_attempt_incarnation_or_expired_stage() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    for change in 0..4 {
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        prepare(&mut model, 1, &facts);
        match change {
            0 => {
                step(
                    &mut model,
                    &Event::Assign {
                        execution: 101,
                        attempt: attempt(8, 2),
                    },
                    &facts,
                );
            }
            1 => {
                step(
                    &mut model,
                    &Event::Register {
                        worker: 7,
                        incarnation: 71,
                    },
                    &facts,
                );
            }
            2 => {
                step(
                    &mut model,
                    &Event::Revoke {
                        worker: 7,
                        incarnation: 70,
                    },
                    &facts,
                );
            }
            3 => {
                step(&mut model, &Event::Advance(10), &facts);
            }
            _ => unreachable!(),
        }
        rejected(
            &mut model,
            &Event::Publish {
                stage: 11,
                publication: 201,
                until: 80,
            },
            &facts,
        );
        assert!(model.accepted.is_empty());
        assert!(model.heads.is_empty());
        assert_eq!(model.operations[&1].state, OperationState::Waiting(101));
        if change == 0 {
            prepare(&mut model, 7, &facts);
            publish(&mut model, 7, &facts);
            assert_eq!(model.heads[ACTION], 207);
            assert_eq!(
                terminal(&model, 1).response,
                scenario.candidates[&7].response
            );
        }
        if change == 3 {
            step(&mut model, &Event::ExpireStage(11), &facts);
            assert_eq!(model.stages[&11].state, StageState::Abandoned);
            assert!(model.unresolved_writes().contains(&(11, 1010)));
        }
    }
}

#[test]
fn direct_client_and_worker_use_commit_order_without_fabricated_client_attempt() {
    let (scenario, facts) = fixture(Algorithm::Blake3);
    for direct_first in [false, true] {
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        prepare(&mut model, 1, &facts);
        prepare(&mut model, 3, &facts);
        let before = model.executions.clone();
        if direct_first {
            publish(&mut model, 3, &facts);
            publish(&mut model, 1, &facts);
        } else {
            publish(&mut model, 1, &facts);
            publish(&mut model, 3, &facts);
        }
        assert_eq!(model.heads[ACTION], if direct_first { 201 } else { 203 });
        assert_eq!(terminal(&model, 1).publication, 201);
        assert_eq!(
            model.accepted[&203].source,
            Source::Client { principal: 77 }
        );
        assert_eq!(model.executions.len(), before.len());
    }
    let mut model = Model::new(scenario.clone()).unwrap();
    prepare(&mut model, 3, &facts);
    assert_eq!(
        rejected(
            &mut model,
            &Event::Publish {
                stage: 13,
                publication: 203,
                until: 80
            },
            &facts
        ),
        Reject::Unauthorized
    );
    step(&mut model, &Event::AuthorizeClient(77), &facts);
    publish(&mut model, 3, &facts);
    assert!(model.executions.is_empty());
    assert!(model.operations.is_empty());
    let mut outside = scenario;
    outside.candidates.get_mut(&3).unwrap().successful = false;
    assert_eq!(
        Model::new(outside),
        Err(Reject::Invalid),
        "direct failure policy is outside this scenario alphabet"
    );
}

#[test]
fn fixed_request_matrix_separates_lookup_coalescing_and_cacheability() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    assert_eq!(
        step(
            &mut model,
            &Event::Admit(request(2, 102, ACTION, true)),
            &facts
        ),
        Reply::Joined(101)
    );
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    assert_eq!(terminal(&model, 1).publication, 201);
    assert_eq!(terminal(&model, 2).publication, 201);
    assert_eq!(
        step(
            &mut model,
            &Event::Admit(request(3, 103, ACTION, false)),
            &facts
        ),
        Reply::Hit(201)
    );
    assert!(terminal(&model, 3).cached);
    assert_eq!(
        step(
            &mut model,
            &Event::Admit(request(4, 102, ACTION, true)),
            &facts
        ),
        Reply::Started(102)
    );
    assert_eq!(
        step(
            &mut model,
            &Event::Admit(request(5, 109, ACTION, true)),
            &facts
        ),
        Reply::Joined(102)
    );
    step(
        &mut model,
        &Event::Assign {
            execution: 102,
            attempt: attempt(8, 1),
        },
        &facts,
    );
    prepare(&mut model, 2, &facts);
    publish(&mut model, 2, &facts);
    assert_eq!(model.heads[ACTION], 202, "skip lookup remains publishable");
    start(&mut model, 4, 6, &facts);
    start(&mut model, 5, 7, &facts);
    assert_eq!(model.operations[&6].state, OperationState::Waiting(104));
    assert_eq!(model.operations[&7].state, OperationState::Waiting(105));
    prepare(&mut model, 4, &facts);
    publish(&mut model, 4, &facts);
    prepare(&mut model, 5, &facts);
    publish(&mut model, 5, &facts);
    assert!(!model.heads.contains_key(UNCACHED));
    assert_ne!(
        terminal(&model, 6).publication,
        terminal(&model, 7).publication
    );
    assert!(
        model
            .coverage
            .iter()
            .any(|row| row.root == Root::Operation(6) && row.until > model.now)
    );
    assert!(
        model
            .coverage
            .iter()
            .any(|row| row.root == Root::Operation(7) && row.until > model.now)
    );
    start(&mut model, 8, 8, &facts);
    prepare(&mut model, 8, &facts);
    publish(&mut model, 8, &facts);
    assert_eq!(model.heads[OTHER], 208);
    assert_eq!(
        model.heads[ACTION], 202,
        "qualified keys cannot collapse by action digest text"
    );
}

#[test]
fn unsuccessful_or_error_terminal_keeps_protection_without_replacing_head() {
    for error in [false, true] {
        let (mut scenario, facts) = fixture(Algorithm::Sha256);
        if error {
            scenario.candidates.get_mut(&6).unwrap().error_free = false;
        } else {
            scenario.candidates.get_mut(&6).unwrap().successful = false;
        }
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        prepare(&mut model, 1, &facts);
        publish(&mut model, 1, &facts);
        start(&mut model, 6, 6, &facts);
        prepare(&mut model, 6, &facts);
        publish(&mut model, 6, &facts);
        assert_eq!(model.heads[ACTION], 201);
        assert!(!model.accepted[&206].eligible);
        assert_eq!(
            terminal(&model, 6).response,
            scenario.candidates[&6].response
        );
        assert!(
            model
                .coverage
                .iter()
                .any(|row| row.root == Root::Operation(6) && row.until == 80)
        );
    }
}

#[test]
fn canceled_joined_operation_never_receives_worker_terminal_association() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    step(
        &mut model,
        &Event::Admit(request(2, 102, ACTION, true)),
        &facts,
    );
    step(&mut model, &Event::CancelJoined(2), &facts);
    let canceled = model.operations[&2].clone();
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    assert_eq!(model.operations[&2], canceled);
    assert!(matches!(canceled.state, OperationState::Canceled(_)));
    assert!(
        !model
            .coverage
            .iter()
            .any(|row| row.root == Root::Operation(2))
    );
    assert_eq!(terminal(&model, 1).publication, 201);
    rejected(&mut model, &Event::CancelJoined(1), &facts);
}

#[test]
fn exact_identity_typed_closure_and_current_preparation_fences_are_required() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    for variant in 0..14 {
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        step(&mut model, &stage_event(1), &facts);
        let mut changed = facts.clone();
        let mut evidence = preparation(&scenario, 1);
        match variant {
            0 => {
                evidence.typed.retain(|member| member.role != Role::Blob);
            }
            1 => {
                let mut tree = evidence
                    .typed
                    .iter()
                    .find(|member| member.role == Role::Tree)
                    .unwrap()
                    .clone();
                evidence.typed.remove(&tree);
                tree.role = Role::Directory;
                evidence.typed.insert(tree);
            }
            2 => {
                evidence.protected.remove(&key(100));
            }
            3 => evidence.complete = false,
            4 => evidence.verified = false,
            5 => evidence.catalogue_revision -= 1,
            6 => {
                evidence.fences = BTreeSet::from([Fence {
                    namespace: [7; 16],
                    revision: 8,
                }]);
            }
            7 => evidence.stage = 12,
            8 => evidence.result.hash[0] ^= 1,
            9 => {
                changed
                    .physical
                    .iter_mut()
                    .find(|row| row.key == key(100))
                    .unwrap()
                    .retired = true;
            }
            10 => {
                changed
                    .physical
                    .iter_mut()
                    .find(|row| row.key == key(100))
                    .unwrap()
                    .ready = false;
            }
            11 => {
                changed
                    .physical
                    .iter_mut()
                    .find(|row| row.key == key(100))
                    .unwrap()
                    .key
                    .incarnation[15] ^= 1;
            }
            12 => {
                changed
                    .physical
                    .iter_mut()
                    .find(|row| row.key == key(100))
                    .unwrap()
                    .object
                    .hash[0] ^= 1;
            }
            13 => evidence.until = 0,
            _ => unreachable!(),
        }
        rejected(
            &mut model,
            &Event::Prepare {
                stage: 11,
                evidence,
            },
            &changed,
        );
        assert!(model.heads.is_empty());
        assert!(model.accepted.is_empty());
    }
    for variant in 0..3 {
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        prepare(&mut model, 1, &facts);
        let mut changed = facts.clone();
        match variant {
            0 => changed.catalogue_revision += 1,
            1 => {
                changed.fences = BTreeSet::from([Fence {
                    namespace: [7; 16],
                    revision: 10,
                }]);
            }
            2 => {
                changed
                    .physical
                    .iter_mut()
                    .find(|row| row.key == key(100))
                    .unwrap()
                    .retired = true;
            }
            _ => unreachable!(),
        }
        rejected(
            &mut model,
            &Event::Publish {
                stage: 11,
                publication: 201,
                until: 80,
            },
            &changed,
        );
    }
}

#[test]
fn abandoned_or_expired_stage_retains_late_write_identity_without_publishing() {
    let (scenario, mut facts) = fixture(Algorithm::Sha256);
    for expired in [false, true] {
        let mut model = initialized(&scenario, &facts);
        start(&mut model, 1, 1, &facts);
        prepare(&mut model, 1, &facts);
        if expired {
            step(&mut model, &Event::Advance(10), &facts);
            step(&mut model, &Event::ExpireStage(11), &facts);
        } else {
            step(&mut model, &Event::Abandon(11), &facts);
        }
        let late = receipt(&scenario, 1, 0);
        facts.receipts.insert(late.clone());
        step(&mut model, &Event::Observe(late), &facts);
        rejected(
            &mut model,
            &Event::Publish {
                stage: 11,
                publication: 201,
                until: 80,
            },
            &facts,
        );
        assert!(model.accepted.is_empty());
        assert!(model.heads.is_empty());
        assert_eq!(model.operations[&1].state, OperationState::Waiting(101));
        assert!(model.unresolved_writes().contains(&(11, 1011)));
        assert!(model.stages[&11].outcomes[&1010]);
    }
}

#[test]
fn serving_requires_current_selection_and_head_replacement_preserves_grants() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    step(&mut model, &Event::Materialize(201), &facts);
    let old_barrier = model.applied;
    let captured = Event::Serve {
        action: ACTION.into(),
        expected: 201,
        barrier: old_barrier,
        grant: 1,
        until: 100,
    };
    prepare(&mut model, 3, &facts);
    publish(&mut model, 3, &facts);
    assert_eq!(
        model.index[ACTION], 201,
        "a stale index is possible and cannot authorize a hit"
    );
    assert_eq!(rejected(&mut model, &captured, &facts), Reject::Stale);
    let barrier = model.applied;
    assert_eq!(
        rejected(
            &mut model,
            &Event::Serve {
                action: ACTION.into(),
                expected: 201,
                barrier,
                grant: 1,
                until: 100
            },
            &facts
        ),
        Reject::Stale
    );
    let barrier = model.applied;
    step(
        &mut model,
        &Event::Serve {
            action: ACTION.into(),
            expected: 203,
            barrier,
            grant: 1,
            until: 100,
        },
        &facts,
    );
    step(&mut model, &Event::Advance(80), &facts);
    for member in &scenario.candidates[&3].required {
        assert!(model.can_read(1, member));
    }
    step(&mut model, &Event::Advance(100), &facts);
    for member in &scenario.candidates[&3].required {
        assert!(!model.can_read(1, member));
    }
}

#[test]
fn expired_latest_head_misses_without_old_fallback_and_needs_fresh_valid_protection() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    prepare(&mut model, 3, &facts);
    step(
        &mut model,
        &Event::Publish {
            stage: 13,
            publication: 203,
            until: 40,
        },
        &facts,
    );
    step(&mut model, &Event::Materialize(203), &facts);
    step(&mut model, &Event::Advance(40), &facts);
    step(&mut model, &Event::RebuildIndex, &facts);
    assert!(model.index.is_empty());
    assert_eq!(model.heads[ACTION], 203);
    assert!(
        model
            .coverage
            .iter()
            .any(|row| row.root == Root::Operation(1) && row.until == 80)
    );
    let barrier = model.applied;
    assert_eq!(
        rejected(
            &mut model,
            &Event::Serve {
                action: ACTION.into(),
                expected: 203,
                barrier,
                grant: 1,
                until: 90
            },
            &facts
        ),
        Reject::Miss
    );
    let mut retired = facts.clone();
    retired
        .physical
        .iter_mut()
        .find(|row| row.key == key(3))
        .unwrap()
        .retired = true;
    let mut fresh = preparation(&scenario, 3);
    fresh.id = 999;
    fresh.until = 120;
    rejected(
        &mut model,
        &Event::Reprotect {
            publication: 203,
            evidence: fresh.clone(),
            until: 100,
        },
        &retired,
    );
    let mut stale = fresh.clone();
    stale.catalogue_revision -= 1;
    rejected(
        &mut model,
        &Event::Reprotect {
            publication: 203,
            evidence: stale,
            until: 100,
        },
        &facts,
    );
    step(
        &mut model,
        &Event::Reprotect {
            publication: 203,
            evidence: fresh,
            until: 100,
        },
        &facts,
    );
    step(&mut model, &Event::RebuildIndex, &facts);
    assert_eq!(model.index[ACTION], 203);
    assert_eq!(terminal(&model, 1).publication, 201);
}

#[test]
fn old_observations_cannot_invent_write_identity_or_change_a_final_outcome() {
    let (scenario, mut facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    let good = receipt(&scenario, 1, 0);
    for variant in 0..4 {
        let mut bad = good.clone();
        match variant {
            0 => bad.stage = 99,
            1 => bad.write = 999,
            2 => bad.key.incarnation[0] ^= 1,
            3 => bad.object.hash[0] ^= 1,
            _ => unreachable!(),
        }
        let mut changed = facts.clone();
        changed.receipts.insert(bad.clone());
        rejected(&mut model, &Event::Observe(bad), &changed);
    }
    rejected(&mut model, &Event::Observe(good.clone()), &facts);
    facts.receipts.insert(good.clone());
    step(&mut model, &Event::Observe(good.clone()), &facts);
    assert_eq!(
        step(&mut model, &Event::Observe(good.clone()), &facts),
        Reply::Ignored
    );
    let mut conflicting = good;
    conflicting.success = false;
    facts.receipts.insert(conflicting.clone());
    assert_eq!(
        rejected(&mut model, &Event::Observe(conflicting), &facts),
        Reject::Conflict
    );
    assert_eq!(model.heads[ACTION], 201);
}

#[test]
fn stage_capacity_refusal_preserves_reserved_acceptance_and_expiry_records() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    for id in 1..=MAX_STAGES as u64 {
        step(
            &mut model,
            &Event::Stage {
                id,
                candidate: 3,
                until: 10,
                writes: BTreeMap::new(),
            },
            &facts,
        );
    }
    assert_eq!(
        rejected(
            &mut model,
            &Event::Stage {
                id: 99,
                candidate: 3,
                until: 10,
                writes: BTreeMap::new()
            },
            &facts
        ),
        Reject::Bounds
    );
    let mut evidence = preparation(&scenario, 3);
    evidence.stage = 1;
    step(&mut model, &Event::Prepare { stage: 1, evidence }, &facts);
    step(
        &mut model,
        &Event::Publish {
            stage: 1,
            publication: 203,
            until: 80,
        },
        &facts,
    );
    step(&mut model, &Event::Advance(10), &facts);
    for id in 2..=MAX_STAGES as u64 {
        step(&mut model, &Event::ExpireStage(id), &facts);
    }
    assert_eq!(
        model.stages.len(),
        MAX_STAGES,
        "identity tombstones are bounded and never silently reused"
    );
    assert_eq!(
        rejected(
            &mut model,
            &Event::Stage {
                id: 99,
                candidate: 3,
                until: 20,
                writes: BTreeMap::new()
            },
            &facts
        ),
        Reject::Bounds
    );
    let mut operations = initialized(&scenario, &facts);
    for id in 1..=MAX_EXECUTIONS as u64 {
        assert_eq!(
            step(
                &mut operations,
                &Event::Admit(request(id, id, UNCACHED, false)),
                &facts
            ),
            Reply::Started(id)
        );
    }
    assert_eq!(
        rejected(
            &mut operations,
            &Event::Admit(request(99, 99, UNCACHED, false)),
            &facts
        ),
        Reject::Bounds
    );
}

#[test]
fn accepted_retry_after_revocation_and_expiry_preserves_exact_original_decision() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    let accepted = model.accepted.clone();
    let operation = model.operations[&1].clone();
    step(
        &mut model,
        &Event::Revoke {
            worker: 7,
            incarnation: 70,
        },
        &facts,
    );
    step(&mut model, &Event::Advance(100), &facts);
    assert_eq!(
        step(
            &mut model,
            &Event::Publish {
                stage: 11,
                publication: 201,
                until: 80
            },
            &facts
        ),
        Reply::Ignored
    );
    assert_eq!(model.accepted, accepted);
    assert_eq!(model.operations[&1], operation);
    assert_eq!(
        rejected(
            &mut model,
            &Event::Publish {
                stage: 11,
                publication: 201,
                until: 90
            },
            &facts
        ),
        Reject::Conflict
    );
}

#[test]
fn closed_worker_incarnation_cannot_be_registered_again_and_operation_deadlines_hold() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    assert_eq!(
        rejected(
            &mut model,
            &Event::Publish {
                stage: 11,
                publication: 201,
                until: 40
            },
            &facts
        ),
        Reject::Unprotected
    );
    step(
        &mut model,
        &Event::Register {
            worker: 7,
            incarnation: 71,
        },
        &facts,
    );
    assert_eq!(
        rejected(
            &mut model,
            &Event::Register {
                worker: 7,
                incarnation: 70
            },
            &facts
        ),
        Reject::Stale
    );
    step(
        &mut model,
        &Event::Revoke {
            worker: 7,
            incarnation: 71,
        },
        &facts,
    );
    assert_eq!(
        rejected(
            &mut model,
            &Event::Register {
                worker: 7,
                incarnation: 71
            },
            &facts
        ),
        Reject::Stale
    );
    assert_eq!(model.worker_epochs[&7], 71);
    rejected(
        &mut model,
        &Event::Publish {
            stage: 11,
            publication: 201,
            until: 80,
        },
        &facts,
    );
    assert_eq!(
        Model::restore(&model.snapshot().unwrap(), &scenario).unwrap(),
        model
    );
}

#[test]
fn snapshot_cuts_and_all_late_outcome_orders_preserve_authoritative_associations() {
    let (scenario, mut facts) = fixture(Algorithm::Blake3);
    let late = receipt(&scenario, 1, 0);
    facts.receipts.insert(late.clone());
    let mut base = initialized(&scenario, &facts);
    start(&mut base, 1, 1, &facts);
    prepare(&mut base, 1, &facts);
    publish(&mut base, 1, &facts);
    let barrier = base.applied;
    step(
        &mut base,
        &Event::Serve {
            action: ACTION.into(),
            expected: 201,
            barrier,
            grant: 1,
            until: 90,
        },
        &facts,
    );
    start(&mut base, 2, 2, &facts);
    prepare(&mut base, 2, &facts);
    publish(&mut base, 2, &facts);
    let permutations = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let late_events = [
        Event::Observe(late),
        Event::Materialize(201),
        Event::Revoke {
            worker: 7,
            incarnation: 70,
        },
    ];
    for order in permutations {
        let mut events = vec![Event::Materialize(202), Event::LoseIndex];
        events.extend(order.into_iter().map(|index| late_events[index].clone()));
        events.push(Event::RebuildIndex);
        let mut full = base.clone();
        for event in &events {
            step(&mut full, event, &facts);
        }
        assert_eq!(full.index[ACTION], 202);
        for cut in 0..=events.len() {
            let mut prefix = base.clone();
            for event in &events[..cut] {
                step(&mut prefix, event, &facts);
            }
            let mut restored = Model::restore(&prefix.snapshot().unwrap(), &scenario).unwrap();
            for event in &events[cut..] {
                step(&mut restored, event, &facts);
            }
            assert_eq!(
                restored.snapshot().unwrap(),
                full.snapshot().unwrap(),
                "order={order:?} cut={cut}"
            );
            for member in &scenario.candidates[&1].required {
                assert!(restored.can_read(1, member));
            }
        }
    }
}

#[test]
fn snapshots_refuse_forged_heads_dropped_roots_changed_terminals_and_scenario() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let mut model = initialized(&scenario, &facts);
    start(&mut model, 1, 1, &facts);
    prepare(&mut model, 1, &facts);
    publish(&mut model, 1, &facts);
    for variant in 0..9 {
        let mut bad = model.clone();
        match variant {
            0 => {
                bad.heads.insert(ACTION.into(), 999);
            }
            1 => {
                bad.coverage.retain(|root| root.root != Root::Operation(1));
            }
            2 => {
                if let OperationState::Terminal(terminal) =
                    &mut bad.operations.get_mut(&1).unwrap().state
                {
                    terminal.response.push(7);
                }
            }
            3 => {
                bad.accepted.get_mut(&201).unwrap().source = Source::Client { principal: 77 };
            }
            4 => {
                bad.stages.get_mut(&11).unwrap().candidate = 2;
            }
            5 => {
                bad.coverage
                    .iter_mut()
                    .find(|root| root.root == Root::Result(201))
                    .unwrap()
                    .keys
                    .remove(&key(100));
            }
            6 => {
                bad.scenario
                    .candidates
                    .get_mut(&1)
                    .unwrap()
                    .response
                    .push(7);
            }
            7 => {
                bad.stages.get_mut(&11).unwrap().outcomes.insert(999, true);
            }
            8 => {
                bad.coverage
                    .iter_mut()
                    .find(|row| row.root == Root::Operation(1))
                    .unwrap()
                    .until = 1;
            }
            _ => unreachable!(),
        }
        assert!(Model::restore(&serde_json::to_vec(&bad).unwrap(), &scenario).is_err());
    }
    let mut trailing = model.snapshot().unwrap();
    trailing.push(b' ');
    assert!(Model::restore(&trailing, &scenario).is_err());
    assert_eq!(
        Model::restore(&vec![0; MAX_SNAPSHOT + 1], &scenario),
        Err(Reject::Bounds)
    );
}

#[test]
fn every_cut_through_staging_acceptance_and_head_replacement_replays_identically() {
    let (scenario, facts) = fixture(Algorithm::Sha256);
    let events = vec![
        Event::Register {
            worker: 7,
            incarnation: 70,
        },
        Event::AuthorizeClient(77),
        Event::Admit(request(1, 101, ACTION, true)),
        Event::Assign {
            execution: 101,
            attempt: attempt(7, 1),
        },
        Event::Admit(request(2, 102, ACTION, true)),
        Event::CancelJoined(2),
        stage_event(1),
        Event::Prepare {
            stage: 11,
            evidence: preparation(&scenario, 1),
        },
        Event::Publish {
            stage: 11,
            publication: 201,
            until: 80,
        },
        Event::Revoke {
            worker: 7,
            incarnation: 70,
        },
        stage_event(3),
        Event::Prepare {
            stage: 13,
            evidence: preparation(&scenario, 3),
        },
        Event::Publish {
            stage: 13,
            publication: 203,
            until: 80,
        },
        Event::Materialize(201),
        Event::Materialize(203),
        Event::LoseIndex,
        Event::RebuildIndex,
    ];
    let mut full = Model::new(scenario.clone()).unwrap();
    for event in &events {
        step(&mut full, event, &facts);
    }
    for cut in 0..=events.len() {
        let mut prefix = Model::new(scenario.clone()).unwrap();
        for event in &events[..cut] {
            step(&mut prefix, event, &facts);
        }
        let mut restored = Model::restore(&prefix.snapshot().unwrap(), &scenario).unwrap();
        for event in &events[cut..] {
            step(&mut restored, event, &facts);
        }
        assert_eq!(
            restored.snapshot().unwrap(),
            full.snapshot().unwrap(),
            "acceptance cut={cut}"
        );
        assert_eq!(restored.heads[ACTION], 203);
        assert_eq!(restored.index[ACTION], 203);
        assert_eq!(terminal(&restored, 1).publication, 201);
        assert!(matches!(
            restored.operations[&2].state,
            OperationState::Canceled(_)
        ));
        assert_eq!(
            restored.unresolved_writes(),
            BTreeSet::from([(11, 1010), (11, 1011), (13, 1030), (13, 1031)])
        );
    }
}
