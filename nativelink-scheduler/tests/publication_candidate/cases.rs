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

use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority as a;
use prost::Message;

use super::engine::PublicationAuthority;
use super::protocol as r;
use super::types::*;

fn authority(actions: &[(&str, bool)]) -> PublicationAuthority {
    PublicationAuthority::new(
        actions
            .iter()
            .map(|(key, value)| ((*key).into(), *value))
            .collect(),
    )
    .unwrap()
}
fn register(state: &mut PublicationAuthority, incarnation: u64) {
    let capacity = a::Resources {
        cpu_millis: 100,
        memory_bytes: 100,
        slots: 8,
    };
    state
        .scheduler_event(a::Event::Register {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(incarnation),
            session: a::SessionId(incarnation),
            properties: BTreeMap::new(),
            capacity,
            available: capacity,
        })
        .unwrap();
}
fn assign(state: &mut PublicationAuthority) -> a::AttemptView {
    let a::Reply::Matched { attempt, .. } = state.scheduler_event(a::Event::Match).unwrap() else {
        panic!("expected placement")
    };
    let row = state
        .scheduler_view()
        .attempts
        .into_iter()
        .find(|row| row.id == attempt)
        .unwrap();
    state
        .scheduler_event(a::Event::WorkerAccepted {
            attempt,
            worker: row.worker,
            incarnation: row.incarnation,
        })
        .unwrap();
    row
}
const fn key(generation: u64) -> Key {
    Key {
        namespace: [1; 16],
        incarnation: [2; 16],
        generation,
    }
}
fn candidate(
    state: &mut PublicationAuthority,
    action: &str,
    publisher: Publisher,
    generation: u64,
    exit: i32,
    status: i32,
) -> Candidate {
    let result = ActionResult {
        exit_code: exit,
        stdout_raw: format!("output-{generation}").into_bytes().into(),
        stderr_raw: b"diagnostic".to_vec().into(),
        ..ActionResult::default()
    };
    let bytes = result.encode_to_vec();
    let object = Object::for_bytes(1, &bytes).unwrap();
    let owner = match publisher {
        Publisher::Worker { execution, .. } => Some(execution),
        Publisher::Client { .. } => None,
    };
    state
        .install_physical_for(owner, key(generation), object, true)
        .unwrap();
    Candidate {
        action: action.into(),
        publisher,
        result: object,
        result_bytes: bytes,
        response: a::ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(result),
                status: Some(Status {
                    code: status,
                    message: if status == 0 {
                        String::new()
                    } else {
                        "execution diagnostic".into()
                    },
                    ..Status::default()
                }),
                ..ExecuteResponse::default()
            },
            MAX_PAYLOAD,
        )
        .unwrap(),
        closure: BTreeSet::from([Member {
            object,
            key: key(generation),
            role: Role::Result,
        }]),
    }
}
const fn worker(row: a::AttemptView) -> Publisher {
    Publisher::Worker {
        execution: row.execution,
        attempt: row.id,
        worker: row.worker,
        incarnation: row.incarnation,
    }
}
fn prepare(state: &mut PublicationAuthority, id: u64) {
    let closure = state.stages()[&id].candidate.closure.clone();
    let evidence = Evidence {
        stage: id,
        catalogue_revision: state.catalogue_revision(),
        fences: state.fences(&closure).unwrap(),
        closure,
        until: 100,
    };
    state.prepare(id, evidence).unwrap();
}
fn stage(state: &mut PublicationAuthority, id: u64, value: Candidate) {
    state.stage(id, value, 10, BTreeMap::new()).unwrap();
    prepare(state, id);
}
fn terminal(state: &PublicationAuthority, operation: u64) -> a::TerminalResult {
    state
        .scheduler_view()
        .operations
        .into_iter()
        .find(|row| row.id.0 == operation)
        .unwrap()
        .terminal
        .unwrap()
}

#[test]
fn worker_acceptance_uses_real_engines_and_one_protected_outer_commit() {
    let mut state = authority(&[("principal/instance/sha256/action/platform/config", false)]);
    register(&mut state, 1);
    state
        .admit(
            1,
            "principal/instance/sha256/action/platform/config".into(),
            true,
            100,
        )
        .unwrap();
    let attempt = assign(&mut state);
    let value = candidate(
        &mut state,
        "principal/instance/sha256/action/platform/config",
        worker(attempt),
        1,
        0,
        0,
    );
    let exact = value
        .response
        .require_response()
        .unwrap()
        .encoded()
        .to_vec();
    state.stage(1, value, 10, BTreeMap::new()).unwrap();
    let before = state.snapshot().unwrap();
    assert_eq!(state.publish(1, 1, 100), Err(Error::Incomplete));
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(state.heads().is_empty());
    assert!(state.scheduler_view().operations[0].terminal.is_none());
    prepare(&mut state, 1);
    state.publish(1, 1, 100).unwrap();
    assert_eq!(state.heads().values().copied().collect::<Vec<_>>(), vec![1]);
    assert_eq!(state.accepted()[&1].commit, state.commit());
    assert!(
        state
            .scheduler_view()
            .reservations
            .iter()
            .all(|row| row.released)
    );
    assert!(
        !state
            .scheduler_view()
            .effects
            .iter()
            .any(|row| matches!(row.kind, a::EffectKind::PublishResult { .. }))
    );
    let a::TerminalResult::Completed(result) = terminal(&state, 1) else {
        panic!("missing result")
    };
    assert_eq!(result.require_response().unwrap().encoded(), exact);
    let retention = state.retention_view();
    assert!(
        retention
            .roots
            .values()
            .any(|root| root.kind == r::RootKind::Operation && root.until == 100)
    );
    assert!(
        retention
            .roots
            .values()
            .any(|root| root.kind == r::RootKind::Publication && root.until == 100)
    );
    assert_eq!(retention.protected.len(), 1);
    let restored = PublicationAuthority::restore(&state.snapshot().unwrap()).unwrap();
    assert_eq!(restored, state);
}

#[test]
fn stale_attempt_cannot_partially_accept_or_refund_real_reservation() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    stage(&mut state, 1, value);
    register(&mut state, 2);
    let before = state.snapshot().unwrap();
    assert!(matches!(
        state.publish(1, 1, 100),
        Err(Error::Scheduler(a::Rejection::Stale))
    ));
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(state.accepted().is_empty());
    assert!(
        state
            .scheduler_view()
            .reservations
            .iter()
            .any(|row| row.attempt == attempt.id && !row.released)
    );
    assert!(state.scheduler_view().operations[0].terminal.is_none());
}

#[test]
fn direct_head_replacement_late_write_and_index_rebuild_preserve_operation_and_serving() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.authorize_client(9).unwrap();
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    let object = value.result;
    state
        .stage(1, value, 10, BTreeMap::from([(70, key(1))]))
        .unwrap();
    prepare(&mut state, 1);
    state.publish(1, 1, 100).unwrap();
    let terminal_before = terminal(&state, 1);
    state.serve("a", 1, state.commit(), 1, 90).unwrap();
    let member = state.stages()[&1]
        .candidate
        .closure
        .first()
        .unwrap()
        .clone();
    assert!(state.can_read(1, &member));
    assert_eq!(state.grants()[&1].publication, 1);
    assert!(state.protected_keys().contains(&member.key));
    let mut wrong = member;
    wrong.object.bytes += 1;
    assert!(!state.can_read(1, &wrong));
    let stale_barrier = state.commit();
    let value = candidate(&mut state, "a", Publisher::Client { principal: 9 }, 2, 0, 0);
    stage(&mut state, 2, value);
    state.publish(2, 2, 100).unwrap();
    assert_eq!(state.heads()["a"], 2);
    let before = state.snapshot().unwrap();
    assert_eq!(state.serve("a", 1, stale_barrier, 2, 90), Err(Error::Miss));
    assert_eq!(state.snapshot().unwrap(), before);
    state.observe(1, 70, key(1), object, true).unwrap();
    assert_eq!(state.heads()["a"], 2);
    assert_eq!(terminal(&state, 1), terminal_before);
    assert_eq!(state.retention_view().protected.len(), 2);
    assert_eq!(
        state.scheduler_view().attempts.len(),
        1,
        "direct update fabricated an attempt"
    );
    state.lose_index();
    assert!(state.index().is_empty());
    let restored = PublicationAuthority::restore(&state.snapshot().unwrap()).unwrap();
    assert_eq!(restored.index().get("a"), Some(&2));
    state.rebuild_index();
    assert_eq!(state, restored);
    state.admit(2, "a".into(), false, 80).unwrap();
    assert_eq!(state.operations()[&2].publication, Some(2));
    assert!(state.operations()[&2].cached);
    let a::TerminalResult::Completed(cached) = terminal(&state, 2) else {
        panic!("cache miss")
    };
    assert!(
        cached
            .require_response()
            .unwrap()
            .decode()
            .unwrap()
            .cached_result
    );
    let a::TerminalResult::Completed(original) = &terminal_before else {
        panic!("missing worker terminal")
    };
    assert!(
        !original
            .require_response()
            .unwrap()
            .decode()
            .unwrap()
            .cached_result
    );
    assert_eq!(terminal(&state, 1), terminal_before);
}

#[test]
fn noncacheable_and_error_terminals_get_operation_roots_without_heads() {
    for (do_not_cache, exit, status) in [(true, 0, 0), (false, 7, 0), (false, 0, 13)] {
        let mut state = authority(&[("a", do_not_cache)]);
        register(&mut state, 1);
        state.admit(1, "a".into(), false, 100).unwrap();
        let attempt = assign(&mut state);
        let value = candidate(&mut state, "a", worker(attempt), 1, exit, status);
        stage(&mut state, 1, value);
        state.publish(1, 1, 100).unwrap();
        assert!(state.heads().is_empty());
        assert!(state.index().is_empty());
        assert!(!state.accepted()[&1].eligible);
        let view = state.retention_view();
        assert_eq!(view.protected.len(), 1);
        assert!(
            view.roots
                .values()
                .any(|root| root.kind == r::RootKind::Operation && root.until == 100)
        );
        assert!(
            view.roots
                .values()
                .filter(|root| root.kind == r::RootKind::Publication)
                .all(|root| root.until == 0)
        );
        assert_eq!(
            PublicationAuthority::restore(&state.snapshot().unwrap()).unwrap(),
            state
        );
    }
}

#[test]
fn request_matrix_and_canceled_join_are_owned_by_real_scheduler() {
    let mut state = authority(&[
        ("a", false),
        ("uncacheable", true),
        ("other-principal/a", false),
    ]);
    register(&mut state, 1);
    let first = state.admit(1, "a".into(), false, 100).unwrap();
    assert_eq!(state.admit(2, "a".into(), true, 100).unwrap(), first);
    state.cancel_joined(1).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
    assert_eq!(terminal(&state, 1), a::TerminalResult::Cancelled);
    assert!(state.operations()[&1].root.is_none());
    assert!(state.operations()[&2].root.is_some());
    let later = state.admit(3, "a".into(), true, 100).unwrap();
    assert_ne!(first, later);
    let left = state.admit(4, "uncacheable".into(), false, 100).unwrap();
    let right = state.admit(5, "uncacheable".into(), false, 100).unwrap();
    assert_ne!(left, right);
    assert_ne!(
        state
            .admit(6, "other-principal/a".into(), false, 100)
            .unwrap(),
        later
    );
}

#[test]
fn execution_admission_prepays_stage_members_writes_and_result_roots() {
    let mut state = authority(&[("a", false), ("other", false)]);
    register(&mut state, 1);
    state.authorize_client(9).unwrap();
    let execution = state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let result = ActionResult::default();
    let bytes = result.encode_to_vec();
    let object = Object::for_bytes(1, &bytes).unwrap();
    for generation in 1..=56 {
        state
            .install_physical(key(generation), object, true)
            .unwrap();
    }
    let before = state.snapshot().unwrap();
    assert_eq!(
        state.install_physical(key(57), object, true),
        Err(Error::Bounds)
    );
    assert_eq!(
        state.admit(2, "other".into(), true, 100),
        Err(Error::Bounds)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    for generation in 57..=64 {
        state
            .install_physical_for(Some(execution), key(generation), object, true)
            .unwrap();
    }
    let response = a::ResultPayload::from_response(
        &ExecuteResponse {
            result: Some(result),
            ..ExecuteResponse::default()
        },
        MAX_PAYLOAD,
    )
    .unwrap();
    let direct = Candidate {
        action: "other".into(),
        publisher: Publisher::Client { principal: 9 },
        result: object,
        result_bytes: bytes.clone(),
        response: response.clone(),
        closure: BTreeSet::from([Member {
            object,
            key: key(1),
            role: Role::Result,
        }]),
    };
    for id in 1..=7 {
        state
            .stage(id, direct.clone(), 10, BTreeMap::new())
            .unwrap();
    }
    assert_eq!(
        state.stage(8, direct, 10, BTreeMap::new()),
        Err(Error::Bounds)
    );
    let closure = (57..=64)
        .map(|generation| Member {
            object,
            key: key(generation),
            role: if generation == 57 {
                Role::Result
            } else {
                Role::Blob
            },
        })
        .collect();
    let candidate = Candidate {
        action: "a".into(),
        publisher: worker(attempt),
        result: object,
        result_bytes: bytes,
        response,
        closure,
    };
    let writes = (57..=64)
        .map(|generation| (generation, key(generation)))
        .collect();
    state.stage(99, candidate, 10, writes).unwrap();
    prepare(&mut state, 99);
    state.publish(99, 1, 100).unwrap();
    assert_eq!(state.heads()["a"], 1);
    assert!(state.operations()[&1].root.is_some());
    assert!(
        state
            .scheduler_view()
            .reservations
            .iter()
            .all(|row| row.released)
    );
}

#[test]
fn physical_identity_fences_and_expiry_reject_without_partial_state() {
    let mut state = authority(&[("a", false)]);
    state.authorize_client(9).unwrap();
    let value = candidate(&mut state, "a", Publisher::Client { principal: 9 }, 1, 0, 0);
    let mut alias = key(1);
    alias.incarnation[15] = 3;
    state.install_physical(alias, value.result, true).unwrap();
    assert_eq!(state.retention_view().generations.len(), 2);
    assert_eq!(
        state
            .retention_view()
            .generations
            .keys()
            .map(|key| key.incarnation)
            .collect::<BTreeSet<_>>()
            .len(),
        2
    );
    state
        .stage(1, value.clone(), 10, BTreeMap::from([(1, key(1))]))
        .unwrap();
    prepare(&mut state, 1);
    state.fence_namespace(1).unwrap();
    let before = state.snapshot().unwrap();
    assert_eq!(state.publish(1, 1, 100), Err(Error::Stale));
    assert_eq!(state.snapshot().unwrap(), before);
    state.abandon(1).unwrap();
    state.observe(1, 1, key(1), value.result, true).unwrap();
    assert!(state.heads().is_empty());
    assert_eq!(state.stages()[&1].outcomes.get(&1), Some(&true));
    let mut expiry = authority(&[("a", false)]);
    expiry.authorize_client(9).unwrap();
    let value = candidate(
        &mut expiry,
        "a",
        Publisher::Client { principal: 9 },
        1,
        0,
        0,
    );
    stage(&mut expiry, 1, value);
    expiry.advance(11).unwrap();
    let before = expiry.snapshot().unwrap();
    assert_eq!(expiry.publish(1, 1, 100), Err(Error::Stale));
    assert_eq!(expiry.snapshot().unwrap(), before);
}

#[test]
fn snapshot_rejects_cross_component_corruption_and_unknown_fields() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
    let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    for field in ["next_root", "next_write"] {
        let old = format!("\"{field}\":{}", parsed["metadata"][field]);
        let new = format!("\"{field}\":{}", u64::MAX);
        assert!(encoded.contains(&old));
        assert_eq!(
            PublicationAuthority::restore(encoded.replacen(&old, &new, 1).as_bytes()),
            Err(Error::Corrupt)
        );
    }
    for (old, new) in [
        ("\"eligible\":true", "\"eligible\":false"),
        ("\"heads\":{\"a\":1}", "\"heads\":{\"a\":2}"),
        ("\"cached\":false", "\"cached\":true"),
        ("\"version\":1,", "\"version\":1,\"unknown\":0,"),
        ("\"physical_remaining\":0", "\"physical_remaining\":8"),
        ("\"generation\":1", "\"generation\":2"),
    ] {
        assert!(encoded.contains(old), "missing corruption target {old}");
        let corrupt = encoded.replacen(old, new, 1);
        assert_eq!(
            PublicationAuthority::restore(corrupt.as_bytes()),
            Err(Error::Corrupt),
            "accepted corruption {old}"
        );
    }
}

#[test]
fn replacement_attempt_reserves_a_new_stage_before_becoming_executable() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let old = assign(&mut state);
    let value = candidate(&mut state, "a", worker(old), 1, 0, 0);
    stage(&mut state, 1, value);
    register(&mut state, 2);
    assert_eq!(
        state.publish(1, 1, 100),
        Err(Error::Scheduler(a::Rejection::Stale))
    );
    state
        .scheduler_event(a::Event::Cleanup {
            attempt: old.id,
            worker: old.worker,
            incarnation: old.incarnation,
        })
        .unwrap();
    let replacement = assign(&mut state);
    assert_eq!(replacement.execution, old.execution);
    assert_ne!(replacement.id, old.id);
    let value = candidate(&mut state, "a", worker(replacement), 2, 0, 0);
    stage(&mut state, 2, value);
    state.publish(2, 2, 100).unwrap();
    assert_eq!(state.heads()["a"], 2);
    assert_eq!(state.stages()[&1].state, StageState::Open);
    assert_eq!(state.stages()[&2].state, StageState::Accepted(2));
    assert!(
        state
            .scheduler_view()
            .reservations
            .iter()
            .all(|row| row.released)
    );
    assert_eq!(
        PublicationAuthority::restore(&state.snapshot().unwrap()).unwrap(),
        state
    );
    let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let original = serde_json::to_string(&state.stages()[&2]).unwrap();
    let mut forged = state.stages()[&2].clone();
    forged.candidate.publisher = worker(old);
    assert_eq!(encoded.matches(&original).count(), 1);
    let corrupt = encoded.replacen(&original, &serde_json::to_string(&forged).unwrap(), 1);
    assert_eq!(
        PublicationAuthority::restore(corrupt.as_bytes()),
        Err(Error::Corrupt),
        "accepted publication claimed the revoked predecessor attempt"
    );
}

#[test]
fn operation_root_failure_rolls_back_completion_and_staging_transfer() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 5).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    stage(&mut state, 1, value);
    state.advance(6).unwrap();
    // The staging guard and prepared closure remain live. The expired operation
    // promise makes Acquire(Operation) fail after real Complete and Transfer.
    let before = state.snapshot().unwrap();
    assert_eq!(state.publish(1, 1, 100), Err(Error::Unprotected));
    assert_eq!(state.snapshot().unwrap(), before);
    assert!(state.scheduler_view().operations[0].terminal.is_none());
    assert!(
        state
            .scheduler_view()
            .reservations
            .iter()
            .all(|row| !row.released)
    );
    assert!(state.accepted().is_empty());
    assert_eq!(state.stages()[&1].state, StageState::Open);
    assert_eq!(state.protected_keys(), BTreeSet::from([key(1)]));
}

#[test]
fn catalogue_revision_change_requires_new_preparation_without_fence_change() {
    let mut state = authority(&[("a", false)]);
    state.authorize_client(9).unwrap();
    let value = candidate(&mut state, "a", Publisher::Client { principal: 9 }, 1, 0, 0);
    stage(&mut state, 1, value.clone());
    let fences = state.fences(&value.closure).unwrap();
    state.install_physical(key(2), value.result, true).unwrap();
    assert_eq!(state.fences(&value.closure).unwrap(), fences);
    let before = state.snapshot().unwrap();
    assert_eq!(state.publish(1, 1, 100), Err(Error::Stale));
    assert_eq!(state.snapshot().unwrap(), before);
    prepare(&mut state, 1);
    state.publish(1, 1, 100).unwrap();
}

#[test]
fn serving_requires_fresh_barrier_even_when_the_head_did_not_change() {
    let mut state = authority(&[("a", false)]);
    state.authorize_client(9).unwrap();
    let value = candidate(&mut state, "a", Publisher::Client { principal: 9 }, 1, 0, 0);
    stage(&mut state, 1, value);
    state.publish(1, 1, 100).unwrap();
    let old_barrier = state.commit();
    state.authorize_client(10).unwrap();
    assert_eq!(state.heads()["a"], 1);
    let before = state.snapshot().unwrap();
    assert_eq!(state.serve("a", 1, old_barrier, 1, 90), Err(Error::Miss));
    assert_eq!(state.snapshot().unwrap(), before);
    state.serve("a", 1, state.commit(), 1, 90).unwrap();
}

#[test]
fn snapshot_rejects_preparation_without_bounded_fences_and_sufficient_interval() {
    let mut state = authority(&[("a", false)]);
    register(&mut state, 1);
    state.admit(1, "a".into(), true, 100).unwrap();
    let attempt = assign(&mut state);
    let value = candidate(&mut state, "a", worker(attempt), 1, 0, 0);
    stage(&mut state, 1, value);
    state.publish(1, 1, 50).unwrap();
    let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
    let preparation = state.stages()[&1].preparation.as_ref().unwrap();
    let original = serde_json::to_string(preparation).unwrap();
    assert_eq!(encoded.matches(&original).count(), 1);
    let mut bad = Vec::new();
    let mut changed = preparation.clone();
    changed.until = 40;
    bad.push(("insufficient result interval", changed));
    let mut changed = preparation.clone();
    changed.until = 80;
    bad.push(("insufficient operation interval", changed));
    let mut changed = preparation.clone();
    changed.until = 0;
    bad.push(("expired interval", changed));
    let mut changed = preparation.clone();
    changed.until = u64::MAX;
    bad.push(("unbounded interval", changed));
    let mut changed = preparation.clone();
    changed.fences.clear();
    bad.push(("missing fence", changed));
    let mut changed = preparation.clone();
    changed.fences.insert(2, 0);
    bad.push(("unrelated fence", changed));
    let mut changed = preparation.clone();
    changed.fences.insert(1, u64::MAX);
    bad.push(("future fence", changed));
    let accepted: Vec<_> = bad
        .into_iter()
        .filter_map(|(name, changed)| {
            let corrupt = encoded.replacen(&original, &serde_json::to_string(&changed).unwrap(), 1);
            PublicationAuthority::restore(corrupt.as_bytes())
                .is_ok()
                .then_some(name)
        })
        .collect();
    assert!(
        accepted.is_empty(),
        "accepted forged preparations: {accepted:?}"
    );
}

#[path = "capacity.rs"]
mod capacity;

#[path = "real_request_tests.rs"]
mod real_request_tests;
