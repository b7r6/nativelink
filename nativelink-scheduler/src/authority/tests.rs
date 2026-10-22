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

use std::collections::BTreeMap;

use super::*;

fn apply(state: &mut State, event: Event) -> Reply {
    let reply = state.apply(RecordedEvent {
        epoch: ClusterEpoch(1),
        event,
    });
    state.check_invariants().unwrap();
    reply
}

fn charged_state() -> State {
    let mut state = State::new(ClusterEpoch(1), Limits::default());
    let resources = Resources {
        cpu_millis: 1000,
        memory_bytes: 1024,
        slots: 1,
    };
    apply(
        &mut state,
        Event::Register {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            properties: BTreeMap::new(),
            capacity: resources,
            available: resources,
        },
    );
    apply(
        &mut state,
        Event::Submit {
            operation: OperationId(1),
            request: Request {
                action: ActionKey {
                    instance: String::new(),
                    digest_function: "sha256".into(),
                    digest: "action".into(),
                    digest_size_bytes: 1,
                },
                execution_scope: None,
                do_not_cache: false,
                skip_cache_lookup: true,
                priority: 0,
                properties: BTreeMap::new(),
                resources,
                execution_timeout: 60,
                payload: None,
            },
        },
    );
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched { .. }
    ));
    state
}

#[test]
fn snapshots_reject_inconsistent_authority_relations() {
    let state = charged_state();
    let original = state.snapshot().unwrap();
    let base: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let corruptions: &[(&str, serde_json::Value)] = &[
        ("/state/next_attempt", 0.into()),
        ("/state/next_effect", 1.into()),
        ("/state/operation_watermark", 0.into()),
        ("/state/reservations/1/released", true.into()),
        ("/state/reservations/1/charged/slots", 0.into()),
        ("/state/attempts/1/incarnation", 2.into()),
        ("/state/operations/1/view/version", u64::MAX.into()),
        ("/state/limits/max_effects", 0.into()),
        ("/state/executions/1/attempt", serde_json::Value::Null),
        ("/state/effects", serde_json::json!({})),
    ];
    for (pointer, value) in corruptions {
        let mut corrupt = base.clone();
        *corrupt.pointer_mut(pointer).unwrap() = value.clone();
        let bytes = serde_json::to_vec(&corrupt).unwrap();
        assert!(
            State::restore(&bytes).is_err(),
            "accepted corrupt {pointer}"
        );
    }
    assert_eq!(
        State::restore(&original).unwrap().snapshot().unwrap(),
        original
    );
}

#[test]
fn snapshots_fail_closed_on_unknown_version_and_fields() {
    let state = charged_state();
    let original = state.snapshot().unwrap();
    for pointer in ["", "/state", "/state/executions/1"] {
        let mut corrupt: serde_json::Value = serde_json::from_slice(&original).unwrap();
        corrupt
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("future_authority_section".into(), true.into());
        assert!(State::restore(&serde_json::to_vec(&corrupt).unwrap()).is_err());
    }
    let mut corrupt: serde_json::Value = serde_json::from_slice(&original).unwrap();
    corrupt["version"] = (SNAPSHOT_VERSION + 1).into();
    assert!(State::restore(&serde_json::to_vec(&corrupt).unwrap()).is_err());
}

#[test]
fn rejected_event_cannot_change_state_or_allocate_identity() {
    let mut state = charged_state();
    let original = state.snapshot().unwrap();
    let events = [
        Event::Complete {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(2),
            result: ResultPayload {
                response: None,
                digest: "stale".into(),
                exit_code: 0,
                error: None,
            },
        },
        Event::Cleanup {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
        },
        Event::Progress {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            phase: Phase::Running,
            progress: 1,
        },
        Event::ExpireAttempt {
            attempt: AttemptId(1),
            generation: 1,
        },
    ];
    for event in events {
        assert!(matches!(apply(&mut state, event), Reply::Rejected(_)));
        assert_eq!(state.snapshot().unwrap(), original);
    }
}

#[test]
fn restored_counter_exhaustion_reserves_admitted_finalization() {
    let state = charged_state();
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    // A fabricated snapshot cannot consume identity space already owed to
    // admitted retries/cancellation. Reject before any reducer can overflow.
    snapshot["state"]["next_effect"] = (u64::MAX - 1).into();
    assert!(State::restore(&serde_json::to_vec(&snapshot).unwrap()).is_err());
}

#[test]
fn unknown_authority_variant_fields_are_not_silently_discarded() {
    let mut event = serde_json::to_value(Event::Complete {
        attempt: AttemptId(1),
        worker: WorkerId(1),
        incarnation: IncarnationId(1),
        result: ResultPayload {
            response: None,
            digest: "result".into(),
            exit_code: 0,
            error: None,
        },
    })
    .unwrap();
    event["Complete"]["future_acceptance_semantics"] = true.into();
    assert!(serde_json::from_value::<Event>(event).is_err());
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&charged_state().snapshot().unwrap()).unwrap();
    snapshot["state"]["effects"]["1"]["kind"]["Dispatch"]["future_ownership_semantics"] =
        true.into();
    assert!(State::restore(&serde_json::to_vec(&snapshot).unwrap()).is_err());
}

fn hold_request(id: u64, memory_bytes: u64, priority: i32) -> Request {
    Request {
        action: ActionKey {
            instance: String::new(),
            digest_function: "sha256".into(),
            digest: format!("hold-action-{id}"),
            digest_size_bytes: 1,
        },
        execution_scope: None,
        do_not_cache: false,
        skip_cache_lookup: true,
        priority,
        properties: BTreeMap::new(),
        resources: Resources {
            cpu_millis: 10,
            memory_bytes,
            slots: 1,
        },
        execution_timeout: 60,
        payload: None,
    }
}

fn hold_submit(state: &mut State, id: u64, memory_bytes: u64, priority: i32) {
    assert!(matches!(
        apply(
            state,
            Event::Submit {
                operation: OperationId(id),
                request: hold_request(id, memory_bytes, priority),
            }
        ),
        Reply::Submitted { .. }
    ));
}

fn hold_capacity(memory_bytes: u64) -> Resources {
    Resources {
        cpu_millis: 100,
        memory_bytes,
        slots: 10,
    }
}

fn hold_register(state: &mut State, worker: u64, memory_bytes: u64) {
    assert_eq!(
        apply(
            state,
            Event::Register {
                worker: WorkerId(worker),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: BTreeMap::new(),
                capacity: hold_capacity(memory_bytes),
                available: hold_capacity(memory_bytes),
            }
        ),
        Reply::Applied
    );
}

fn hold_workers(count: u64) -> State {
    let mut state = State::new(ClusterEpoch(1), Limits::default());
    for worker in 1..=count {
        // Fill each worker before registering the next, then expose the larger
        // true ceiling. Every worker starts with two current small attempts.
        hold_register(&mut state, worker, 20);
        for id in [worker * 2 - 1, worker * 2] {
            hold_submit(&mut state, id, 10, 0);
            assert!(matches!(apply(&mut state, Event::Match),
                Reply::Matched { worker: placed, .. } if placed == WorkerId(worker)));
        }
    }
    for worker in 1..=count {
        apply(
            &mut state,
            Event::Capacity {
                worker: WorkerId(worker),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                capacity: hold_capacity(100),
                available: hold_capacity(100),
                draining: false,
            },
        );
    }
    state
}

fn hold_complete(state: &mut State, attempt: u64) {
    let entry = state.attempts[&AttemptId(attempt)];
    assert_eq!(
        apply(
            state,
            Event::Complete {
                attempt: entry.id,
                worker: entry.worker,
                incarnation: entry.incarnation,
                result: ResultPayload {
                    response: None,
                    digest: "done".into(),
                    exit_code: 0,
                    error: None
                },
            }
        ),
        Reply::Applied
    );
}

fn held_state() -> State {
    let mut state = hold_workers(2);
    hold_submit(&mut state, 5, 95, 0);
    apply(&mut state, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut state, Event::Match), Reply::Applied);
    assert_eq!(state.view().holds.len(), 1);
    assert_eq!(state.view().holds[0].worker, WorkerId(1));
    state
}

#[test]
fn holds_stop_lower_ranked_refill_and_then_place_the_large_request() {
    let mut state = held_state();
    let snapshot = state.snapshot().unwrap();
    state = State::restore(&snapshot).unwrap();
    assert_eq!(state.snapshot().unwrap(), snapshot);
    hold_submit(&mut state, 6, 10, 0);
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched {
            execution: ExecutionId(6),
            worker: WorkerId(2),
            ..
        }
    ));
    hold_complete(&mut state, 1);
    assert_eq!(apply(&mut state, Event::Match), Reply::Ignored);
    assert_eq!(state.view().holds.len(), 1);
    hold_complete(&mut state, 2);
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched {
            execution: ExecutionId(5),
            worker: WorkerId(1),
            ..
        }
    ));
    assert!(state.view().holds.is_empty());
    assert_eq!(state.view().workers[0].charged.memory_bytes, 95);
}

#[test]
fn aged_one_completion_waits_are_held_and_uncertain_cleanup_cannot_steal_a_hold() {
    let mut one = hold_workers(1);
    hold_submit(&mut one, 3, 90, 0);
    apply(
        &mut one,
        Event::AdvanceTime {
            now: HOLD_AFTER - 1,
        },
    );
    assert_eq!(apply(&mut one, Event::Match), Reply::Ignored);
    assert!(one.view().holds.is_empty());
    apply(&mut one, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut one, Event::Match), Reply::Applied);
    assert_eq!(one.view().holds.len(), 1);

    let mut uncertain = hold_workers(2);
    hold_complete(&mut uncertain, 2);
    apply(
        &mut uncertain,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    hold_submit(&mut uncertain, 5, 95, 0);
    apply(&mut uncertain, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut uncertain, Event::Match), Reply::Applied);
    assert_eq!(uncertain.view().holds[0].worker, WorkerId(2));
    assert_eq!(uncertain.view().workers[0].charged.memory_bytes, 10);
}

#[test]
fn revocation_can_invalidate_a_hold_without_refunding_the_original_charge() {
    let mut state = held_state();
    apply(
        &mut state,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    assert!(state.view().holds.is_empty());
    assert_eq!(state.view().workers[0].charged.memory_bytes, 20);
    assert_eq!(apply(&mut state, Event::Match), Reply::Applied);
    assert_eq!(state.view().holds[0].worker, WorkerId(2));
}

#[test]
fn holds_expire_without_matching_and_cooldown_survives_restart() {
    let mut state = held_state();
    let until = HOLD_AFTER + HOLD_DURATION;
    apply(&mut state, Event::AdvanceTime { now: until });
    assert!(state.view().holds.is_empty());
    assert_eq!(
        state.executions[&ExecutionId(5)].hold_cooldown_until,
        until + HOLD_COOLDOWN
    );
    assert_eq!(apply(&mut state, Event::Match), Reply::Ignored);
    state = State::restore(&state.snapshot().unwrap()).unwrap();
    apply(
        &mut state,
        Event::AdvanceTime {
            now: until + HOLD_COOLDOWN - 1,
        },
    );
    assert_eq!(apply(&mut state, Event::Match), Reply::Ignored);
    apply(
        &mut state,
        Event::AdvanceTime {
            now: until + HOLD_COOLDOWN,
        },
    );
    assert_eq!(apply(&mut state, Event::Match), Reply::Applied);
    assert_eq!(
        state.view().holds[0].until,
        until + HOLD_COOLDOWN + HOLD_DURATION
    );
}

#[test]
fn holds_respect_priority_and_do_not_extend_when_more_urgent_work_arrives() {
    let mut state = held_state();
    let hold = state.view().holds[0];
    hold_submit(&mut state, 6, 10, -1);
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched {
            execution: ExecutionId(6),
            worker: WorkerId(1),
            ..
        }
    ));
    assert_eq!(state.view().holds, vec![hold]);
    hold_submit(&mut state, 7, 10, 0);
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched {
            execution: ExecutionId(7),
            worker: WorkerId(2),
            ..
        }
    ));
    assert_eq!(state.view().holds, vec![hold]);
}

#[test]
fn hold_cap_tracks_usable_fleet_and_keeps_the_best_ranked_owner() {
    let mut state = hold_workers(20);
    for id in 41..=43 {
        hold_submit(&mut state, id, 95, 0);
    }
    apply(&mut state, Event::AdvanceTime { now: HOLD_AFTER });
    assert_eq!(apply(&mut state, Event::Match), Reply::Applied);
    assert_eq!(state.view().holds.len(), 2);
    apply(
        &mut state,
        Event::Capacity {
            worker: WorkerId(20),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: hold_capacity(100),
            available: hold_capacity(100),
            draining: true,
        },
    );
    assert_eq!(state.view().holds.len(), 1);
    assert_eq!(state.view().holds[0].execution, ExecutionId(41));
}

#[test]
fn worker_loss_is_fenced_and_clears_holds_without_creating_capacity() {
    let mut state = held_state();
    let before = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::WorkerUnavailable {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(2),
            }
        ),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(state.snapshot().unwrap(), before);
    assert_eq!(
        apply(
            &mut state,
            Event::WorkerUnavailable {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
            }
        ),
        Reply::Applied
    );
    assert!(state.view().holds.is_empty());
    assert!(state.view().workers[0].draining);
    assert_eq!(state.view().workers[0].charged.memory_bytes, 20);
    assert!(
        state.view().attempts[..2]
            .iter()
            .all(|attempt| !attempt.authorized)
    );
    let lost = state.snapshot().unwrap();
    assert_eq!(
        apply(
            &mut state,
            Event::WorkerUnavailable {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
            }
        ),
        Reply::Ignored
    );
    assert_eq!(state.snapshot().unwrap(), lost);
    apply(
        &mut state,
        Event::Heartbeat {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
        },
    );
    assert!(state.view().workers[0].draining);
}

#[test]
fn placement_elsewhere_and_loss_of_interest_release_the_held_worker() {
    let mut elsewhere = held_state();
    hold_register(&mut elsewhere, 3, 100);
    assert!(matches!(
        apply(&mut elsewhere, Event::Match),
        Reply::Matched {
            execution: ExecutionId(5),
            worker: WorkerId(3),
            ..
        }
    ));
    assert!(elsewhere.view().holds.is_empty());

    let mut cancelled = held_state();
    apply(
        &mut cancelled,
        Event::Cancel {
            operation: OperationId(5),
        },
    );
    assert!(cancelled.view().holds.is_empty());
    assert_eq!(cancelled.view().workers[0].charged.memory_bytes, 20);
}

#[test]
fn malformed_hold_snapshots_fail_closed_and_old_format_is_not_migrated() {
    let state = held_state();
    let base: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    for (pointer, value) in [
        ("/version", serde_json::json!(1)),
        ("/state/holds/1/until", serde_json::json!(u64::MAX)),
        ("/state/holds/1/incarnation", serde_json::json!(2)),
        ("/state/holds/1/execution", serde_json::json!(999)),
        (
            "/state/executions/5/enqueue_at",
            serde_json::json!(HOLD_AFTER + 1),
        ),
        (
            "/state/executions/5/hold_cooldown_until",
            serde_json::json!(HOLD_AFTER + 1),
        ),
    ] {
        let mut corrupt = base.clone();
        *corrupt.pointer_mut(pointer).unwrap() = value;
        assert!(
            State::restore(&serde_json::to_vec(&corrupt).unwrap()).is_err(),
            "accepted {pointer}"
        );
    }
}

#[test]
fn exhausted_logical_time_cannot_create_an_overflowing_hold() {
    let mut state = hold_workers(1);
    hold_submit(&mut state, 3, 95, 0);
    apply(
        &mut state,
        Event::AdvanceTime {
            now: u64::MAX - HOLD_DURATION + 1,
        },
    );
    let before = state.snapshot().unwrap();
    assert_eq!(apply(&mut state, Event::Match), Reply::Ignored);
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn decline_closes_admission_and_moves_the_hold_until_fresh_capacity() {
    let mut state = held_state();
    hold_complete(&mut state, 1);
    hold_complete(&mut state, 2);
    let Reply::Matched {
        attempt, worker, ..
    } = apply(&mut state, Event::Match)
    else {
        panic!("held action did not dispatch");
    };
    assert_eq!(worker, WorkerId(1));
    assert!(state.view().holds.is_empty());
    assert_eq!(
        apply(
            &mut state,
            Event::Decline {
                attempt,
                worker,
                incarnation: IncarnationId(1),
            }
        ),
        Reply::Applied
    );
    assert_eq!(state.view().workers[0].available, Resources::default());
    assert_eq!(state.view().workers[0].charged, Resources::default());
    assert_eq!(apply(&mut state, Event::Match), Reply::Applied);
    assert_eq!(state.view().holds[0].worker, WorkerId(2));
    apply(
        &mut state,
        Event::Heartbeat {
            worker,
            incarnation: IncarnationId(1),
            session: SessionId(1),
        },
    );
    assert_eq!(apply(&mut state, Event::Match), Reply::Ignored);
    apply(
        &mut state,
        Event::Capacity {
            worker,
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: hold_capacity(100),
            available: hold_capacity(100),
            draining: false,
        },
    );
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched {
            execution: ExecutionId(5),
            worker: WorkerId(1),
            ..
        }
    ));
    assert!(state.view().holds.is_empty());
}

#[test]
fn reconnect_does_not_refresh_a_hold_and_replacement_does_not_forget_charges() {
    let mut state = held_state();
    let held = state.view().holds;
    apply(
        &mut state,
        Event::Register {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(2),
            properties: BTreeMap::new(),
            capacity: hold_capacity(100),
            available: hold_capacity(100),
        },
    );
    assert_eq!(state.view().holds, held);
    apply(
        &mut state,
        Event::Register {
            worker: WorkerId(1),
            incarnation: IncarnationId(2),
            session: SessionId(1),
            properties: BTreeMap::new(),
            capacity: hold_capacity(100),
            available: hold_capacity(100),
        },
    );
    assert!(state.view().holds.is_empty());
    assert_eq!(state.view().workers[0].charged.memory_bytes, 20);
    assert!(state.view().workers[0].quarantined);
}

#[test]
fn encoded_payload_limit_counts_escaping_before_allocating_identity() {
    let mut state = charged_state();
    let before = state.snapshot().unwrap();
    let mut request = hold_request(2, 10, 0);
    request.properties.insert(
        "escaped".into(),
        "\"".repeat(state.limits.max_payload_bytes / 2),
    );
    // Raw value length fits; JSON escaping alone consumes the entire bound,
    // before counting the accepted request's other fields.
    assert_eq!(
        apply(
            &mut state,
            Event::Submit {
                operation: OperationId(2),
                request,
            }
        ),
        Reply::Rejected(Rejection::Invalid)
    );
    assert_eq!(state.snapshot().unwrap(), before);
}

fn memory_state(memory_bytes: u64, capacity: u64) -> State {
    let mut state = State::new(ClusterEpoch(1), Limits::default());
    hold_register(&mut state, 1, capacity);
    hold_submit(&mut state, 1, memory_bytes, 0);
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched { .. }
    ));
    state
}

fn memory_failure(state: &State, attempt: u64, peak_memory_bytes: Option<u64>) -> Event {
    let entry = state.attempts[&AttemptId(attempt)];
    Event::MemoryFailure {
        attempt: entry.id,
        worker: entry.worker,
        incarnation: entry.incarnation,
        charged: state.reservations[&entry.reservation].charged,
        peak_memory_bytes,
        quiescent: true,
        result: ResultPayload {
            digest: "memory-failure-response".into(),
            exit_code: 137,
            error: Some("explicit worker memory failure".into()),
            response: None,
        },
    }
}

#[test]
fn memory_failure_validation_is_atomic_and_stale_evidence_cannot_regrow() {
    let mut state = memory_state(10, 100);
    let event = memory_failure(&state, 1, None);
    for defect in 0..7 {
        let mut invalid = event.clone();
        let Event::MemoryFailure {
            worker,
            incarnation,
            charged,
            quiescent,
            result,
            ..
        } = &mut invalid
        else {
            unreachable!()
        };
        match defect {
            0 => *quiescent = false,
            1 => charged.memory_bytes = 9,
            2 => charged.cpu_millis = 11,
            3 => charged.slots = 2,
            4 => {
                result.exit_code = 0;
                result.error = None;
            }
            5 => *worker = WorkerId(2),
            6 => *incarnation = IncarnationId(2),
            _ => unreachable!(),
        }
        let before = state.snapshot().unwrap();
        assert!(matches!(apply(&mut state, invalid), Reply::Rejected(_)));
        assert_eq!(state.snapshot().unwrap(), before);
    }
    assert_eq!(apply(&mut state, event.clone()), Reply::Applied);
    assert_eq!(
        state.executions[&ExecutionId(1)]
            .reservation_target
            .memory_bytes,
        20
    );
    let before = state.snapshot().unwrap();
    assert_eq!(apply(&mut state, event), Reply::Rejected(Rejection::Stale));
    assert_eq!(state.snapshot().unwrap(), before);
}

#[test]
fn memory_growth_preserves_request_handles_queue_age_and_original_grants() {
    let mut state = memory_state(10, 100);
    apply(&mut state, Event::AdvanceTime { now: 20 });
    let original = state.executions[&ExecutionId(1)].request.clone();
    let event = memory_failure(&state, 1, None);
    assert_eq!(apply(&mut state, event), Reply::Applied);
    let mut joined = original.clone();
    joined.priority = -5;
    assert_eq!(
        apply(
            &mut state,
            Event::Submit {
                operation: OperationId(2),
                request: joined
            }
        ),
        Reply::Submitted {
            operation: OperationId(2),
            execution: ExecutionId(1)
        }
    );
    let execution = &state.executions[&ExecutionId(1)];
    assert_eq!(execution.request, original);
    assert_eq!(execution.enqueue_at, 0);
    assert_eq!(execution.effective_priority, Some(-5));
    assert_eq!(execution.attempts_started, 1);
    assert_eq!(
        state.reservations[&ReservationId(1)].charged.memory_bytes,
        10
    );
    assert!(state.reservations[&ReservationId(1)].released);
    let mut restored = State::restore(&state.snapshot().unwrap()).unwrap();
    assert_eq!(
        apply(&mut state, Event::Match),
        apply(&mut restored, Event::Match)
    );
    assert_eq!(state.snapshot().unwrap(), restored.snapshot().unwrap());
    let dispatch = state.effects.values().next().unwrap();
    let EffectKind::Dispatch {
        charged, request, ..
    } = &dispatch.kind
    else {
        unreachable!()
    };
    assert_eq!(charged.memory_bytes, 20);
    assert_eq!(request, &original);
    assert_eq!(state.reservations[&ReservationId(2)].charged, *charged);
    hold_complete(&mut state, 2);
    assert!(
        state
            .operations
            .values()
            .all(|op| matches!(op.view.terminal, Some(TerminalResult::Completed(_))))
    );
    assert_eq!(
        state
            .effects
            .values()
            .filter(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
            .count(),
        1
    );
}

#[test]
fn memory_growth_uses_one_compatible_declared_shape_despite_transient_zero_available() {
    let mut state = State::new(ClusterEpoch(1), Limits::default());
    let properties = BTreeMap::from([("arch".into(), "x86".into())]);
    for (worker, arch, capacity) in [(1, "x86", 15), (2, "arm", 100)] {
        apply(
            &mut state,
            Event::Register {
                worker: WorkerId(worker),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: BTreeMap::from([("arch".into(), arch.into())]),
                capacity: hold_capacity(capacity),
                available: hold_capacity(capacity),
            },
        );
    }
    let mut request = hold_request(1, 10, 0);
    request.properties = properties;
    apply(
        &mut state,
        Event::Submit {
            operation: OperationId(1),
            request,
        },
    );
    apply(&mut state, Event::Match);
    apply(
        &mut state,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: hold_capacity(15),
            available: Resources::default(),
            draining: false,
        },
    );
    let event = memory_failure(&state, 1, None);
    assert_eq!(apply(&mut state, event), Reply::Applied);
    assert_eq!(
        state.executions[&ExecutionId(1)]
            .reservation_target
            .memory_bytes,
        15
    );
    assert_eq!(apply(&mut state, Event::Match), Reply::Ignored);
    apply(
        &mut state,
        Event::Capacity {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
            capacity: hold_capacity(15),
            available: hold_capacity(15),
            draining: false,
        },
    );
    assert!(matches!(
        apply(&mut state, Event::Match),
        Reply::Matched {
            worker: WorkerId(1),
            ..
        }
    ));
}

#[test]
fn memory_growth_checks_immovable_cpu_slots_and_all_remaining_original_charges() {
    for dimension in 0..3 {
        let mut state = memory_state(30, 100);
        hold_submit(&mut state, 2, 10, 0);
        assert!(matches!(
            apply(&mut state, Event::Match),
            Reply::Matched {
                attempt: AttemptId(2),
                ..
            }
        ));
        apply(
            &mut state,
            Event::Cancel {
                operation: OperationId(1),
            },
        );
        let mut capacity = hold_capacity(100);
        match dimension {
            0 => capacity.cpu_millis = 10,
            1 => capacity.slots = 1,
            2 => capacity.memory_bytes = 20,
            _ => unreachable!(),
        }
        apply(
            &mut state,
            Event::Capacity {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                capacity,
                available: capacity,
                draining: false,
            },
        );
        let event = memory_failure(&state, 2, None);
        assert_eq!(apply(&mut state, event), Reply::Applied);
        assert!(matches!(
            state.executions[&ExecutionId(2)].terminal,
            Some(TerminalResult::ResourceFailed {
                reason: ResourceFailureReason::NoLargerEligibleWorker,
                ..
            })
        ));
        assert!(!state.reservations[&ReservationId(1)].released);
        assert!(state.reservations[&ReservationId(2)].released);
    }
}

#[test]
fn memory_zero_requires_positive_measurement_and_doubling_never_overflows() {
    for (initial, peak, capacity, expected) in [
        (0, None, 100, None),
        (0, Some(0), 100, None),
        (0, Some(7), 100, Some(14)),
        (0, Some(u64::MAX), 100, Some(100)),
        (u64::MAX / 2 + 1, None, u64::MAX, Some(u64::MAX)),
    ] {
        let mut state = memory_state(initial, capacity);
        let event = memory_failure(&state, 1, peak);
        assert_eq!(apply(&mut state, event), Reply::Applied);
        let execution = &state.executions[&ExecutionId(1)];
        if let Some(expected) = expected {
            assert_eq!(execution.reservation_target.memory_bytes, expected);
            assert_eq!(execution.phase, Phase::Queued);
        } else {
            assert!(matches!(
                execution.terminal,
                Some(TerminalResult::ResourceFailed {
                    reason: ResourceFailureReason::MissingGrowthEvidence,
                    ..
                })
            ));
        }
    }
}

#[test]
fn resource_exhaustion_preserves_full_original_response_and_has_no_publication() {
    use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
    use nativelink_proto::google::rpc::Status;
    let response = ExecuteResponse {
        result: Some(ActionResult {
            exit_code: 137,
            stdout_raw: b"partial stdout".to_vec().into(),
            stderr_raw: b"oom detail".to_vec().into(),
            ..Default::default()
        }),
        status: Some(Status {
            code: 8,
            message: "worker memory exhausted".into(),
            details: Vec::new(),
        }),
        message: "preserve this response verbatim".into(),
        ..Default::default()
    };
    let result = ResultPayload::from_response(&response, 64 * 1024).unwrap();
    let mut state = memory_state(10, 100);
    for attempt in 1..=3 {
        let mut event = memory_failure(&state, attempt, None);
        let Event::MemoryFailure {
            result: candidate, ..
        } = &mut event
        else {
            unreachable!()
        };
        *candidate = result.clone();
        assert_eq!(apply(&mut state, event), Reply::Applied);
        if attempt < 3 {
            assert!(matches!(
                apply(&mut state, Event::Match),
                Reply::Matched { .. }
            ));
        }
    }
    let expected = TerminalResult::ResourceFailed {
        result: result.clone(),
        reason: ResourceFailureReason::AttemptBudgetExhausted,
    };
    assert_eq!(
        state.operations[&OperationId(1)].view.terminal,
        Some(expected.clone())
    );
    assert_eq!(state.executions[&ExecutionId(1)].terminal, Some(expected));
    assert_eq!(
        state.executions[&ExecutionId(1)].attempts_started,
        MAX_ATTEMPTS
    );
    assert!(state.effects.is_empty());
    assert!(
        state
            .reservations
            .values()
            .all(|reservation| reservation.released)
    );
    assert_eq!(
        State::restore(&state.snapshot().unwrap()).unwrap().view(),
        state.view()
    );
    assert_eq!(
        result.require_response().unwrap().decode().unwrap(),
        response
    );
}

#[test]
fn cleanup_after_memory_growth_releases_only_the_original_attempt_vector() {
    let mut state = memory_state(10, 100);
    let event = memory_failure(&state, 1, None);
    apply(&mut state, event);
    apply(&mut state, Event::Match);
    apply(
        &mut state,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    assert_eq!(state.view().workers[0].charged.memory_bytes, 20);
    let cleanup = |attempt| Event::Cleanup {
        attempt: AttemptId(attempt),
        worker: WorkerId(1),
        incarnation: IncarnationId(1),
    };
    assert_eq!(apply(&mut state, cleanup(1)), Reply::Ignored);
    assert_eq!(state.view().workers[0].charged.memory_bytes, 20);
    assert_eq!(apply(&mut state, cleanup(2)), Reply::Applied);
    assert_eq!(state.view().workers[0].charged, Resources::default());
    assert_eq!(
        state.reservations[&ReservationId(1)].charged.memory_bytes,
        10
    );
    assert_eq!(
        state.reservations[&ReservationId(2)].charged.memory_bytes,
        20
    );
}

#[test]
fn snapshot_rejects_mutated_grants_targets_and_legacy_resource_semantics() {
    let mut state = memory_state(10, 100);
    let event = memory_failure(&state, 1, None);
    apply(&mut state, event);
    apply(&mut state, Event::Match);
    let snapshot: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    for (pointer, value) in [
        ("/version", serde_json::json!(2)),
        (
            "/state/executions/1/reservation_target/cpu_millis",
            serde_json::json!(11),
        ),
        (
            "/state/executions/1/reservation_target/memory_bytes",
            serde_json::json!(9),
        ),
        (
            "/state/reservations/1/charged/memory_bytes",
            serde_json::json!(21),
        ),
        (
            "/state/reservations/2/charged/memory_bytes",
            serde_json::json!(10),
        ),
        (
            "/state/effects/2/kind/Dispatch/charged/memory_bytes",
            serde_json::json!(10),
        ),
    ] {
        let mut corrupt = snapshot.clone();
        *corrupt.pointer_mut(pointer).unwrap() = value;
        assert!(
            State::restore(&serde_json::to_vec(&corrupt).unwrap()).is_err(),
            "accepted {pointer}"
        );
    }
}

#[test]
fn restored_snapshot_rejects_duplicate_accepted_result_publications() {
    let mut state = charged_state();
    hold_complete(&mut state, 1);
    let original: serde_json::Value = serde_json::from_slice(&state.snapshot().unwrap()).unwrap();
    for extra in [1_u64, 8] {
        let mut corrupt = original.clone();
        let publication = corrupt["state"]["effects"]["2"].clone();
        for offset in 1..=extra {
            let id = 2 + offset;
            let mut duplicate = publication.clone();
            duplicate["id"] = id.into();
            corrupt["state"]["effects"]
                .as_object_mut()
                .unwrap()
                .insert(id.to_string(), duplicate);
        }
        corrupt["state"]["next_effect"] = (3 + extra).into();
        assert!(State::restore(&serde_json::to_vec(&corrupt).unwrap()).is_err());
    }
    assert_eq!(State::restore(&state.snapshot().unwrap()).unwrap(), state);
}
