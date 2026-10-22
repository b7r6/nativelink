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

//! Contract fixtures for the deterministic authority. These tests exercise the
//! replacement contract; legacy tests are incident inputs, not the oracle.

use std::collections::BTreeMap;

use nativelink_scheduler::authority::*;

#[path = "authority/adversarial.rs"]
mod adversarial;
#[path = "authority/campaign.rs"]
mod campaign;
#[path = "authority/harness.rs"]
mod harness;
#[path = "authority/recipient.rs"]
mod recipient;
#[path = "authority/reference.rs"]
mod reference;

use harness::{Harness, Input, Scenario, Schedule, shrink_inputs, shrink_scenario};

fn harness() -> Harness {
    Harness::new(ClusterEpoch(1), Limits::default())
}

fn request(key: u64) -> Request {
    Request {
        action: ActionKey {
            instance: "test".into(),
            digest_function: "sha256".into(),
            digest: format!("{key:064x}"),
            digest_size_bytes: 1,
        },
        execution_scope: None,
        do_not_cache: false,
        skip_cache_lookup: true,
        priority: 0,
        properties: BTreeMap::from([("os".into(), "linux".into())]),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 10,
            slots: 1,
        },
        execution_timeout: 100,
        payload: None,
    }
}

fn register(worker: u64, incarnation: u64, session: u64, slots: u64) -> Event {
    let capacity = Resources {
        cpu_millis: 10_000,
        memory_bytes: 10_000,
        slots,
    };
    Event::Register {
        worker: WorkerId(worker),
        incarnation: IncarnationId(incarnation),
        session: SessionId(session),
        properties: BTreeMap::from([("os".into(), "linux".into())]),
        capacity,
        available: capacity,
    }
}

fn apply(h: &mut Harness, event: Event) -> Reply {
    h.event(event).unwrap_or_else(|error| panic!("{error}"))
}

fn submit(h: &mut Harness, operation: u64, request: Request) -> ExecutionId {
    match apply(
        h,
        Event::Submit {
            operation: OperationId(operation),
            request,
        },
    ) {
        Reply::Submitted { execution, .. } => execution,
        other => panic!(
            "submission failed: {}",
            serde_json::to_string(&other).unwrap()
        ),
    }
}

fn assign(h: &mut Harness) -> AttemptView {
    let Reply::Matched { attempt, .. } = apply(h, Event::Match) else {
        panic!("expected an eligible placement");
    };
    h.view()
        .attempts
        .into_iter()
        .find(|a| a.id == attempt)
        .unwrap()
}

fn result(key: u64) -> ResultPayload {
    ResultPayload {
        response: None,
        digest: format!("{key:064x}"),
        exit_code: 0,
        error: None,
    }
}

fn finish(h: &mut Harness, attempt: &AttemptView, value: u64) {
    apply(
        h,
        Event::WorkerAccepted {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        },
    );
    for (progress, phase) in [
        (1, Phase::Preparing),
        (2, Phase::Running),
        (3, Phase::Uploading),
    ] {
        apply(
            h,
            Event::Progress {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
                phase,
                progress,
            },
        );
    }
    assert_eq!(
        apply(
            h,
            Event::Complete {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
                result: result(value),
            }
        ),
        Reply::Applied
    );
}

#[test]
fn proposals_and_commits_cannot_dispatch_before_apply() {
    let mut h = harness();
    let mut req = request(1);
    req.skip_cache_lookup = false;
    h.propose(
        10,
        RecordedEvent {
            epoch: ClusterEpoch(1),
            event: Event::Submit {
                operation: OperationId(1),
                request: req,
            },
        },
    )
    .unwrap();
    assert!(h.view().operations.is_empty());
    assert!(h.deliver(EffectId(1)).is_err());
    h.commit(10).unwrap();
    assert!(h.view().operations.is_empty());
    assert!(h.deliver(EffectId(1)).is_err());
    h.apply_next().unwrap();
    let intent = h.deliver(EffectId(1)).unwrap();
    assert!(matches!(intent.kind, EffectKind::Lookup { .. }));
    h.checkpoint().unwrap();
    h.propose(
        20,
        RecordedEvent {
            epoch: ClusterEpoch(1),
            event: Event::Cancel {
                operation: OperationId(1),
            },
        },
    )
    .unwrap();
    h.crash_restore().unwrap();
    assert!(h.commit(20).is_err());
    assert_eq!(h.deliver(EffectId(1)).unwrap(), intent);
}

#[test]
fn uncertain_dispatch_survives_snapshot_and_sender_loss() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    let effect = h.view().effects[0].clone();
    let before = h.view();
    h.checkpoint().unwrap();
    h.deliver(effect.id).unwrap();
    h.crash_restore().unwrap();
    assert_eq!(h.view(), before);
    assert_eq!(h.deliver(effect.id).unwrap(), effect);
    assert!(!h.view().reservations[0].released);
    apply(
        &mut h,
        Event::WorkerAccepted {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        },
    );
    assert!(h.view().effects.iter().all(|e| e.id != effect.id));
    assert_eq!(h.view().workers[0].charged.slots, 1);
}

#[test]
fn stale_attempt_cannot_complete_successor_or_refund_unknown_work() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    apply(&mut h, register(2, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let old = assign(&mut h);
    apply(
        &mut h,
        Event::AdvanceTime {
            now: old.deadline.at,
        },
    );
    apply(
        &mut h,
        Event::ExpireAttempt {
            attempt: old.id,
            generation: old.deadline.generation,
        },
    );
    assert!(!h.view().reservations[0].released);
    assert_eq!(h.view().workers[0].charged.slots, 1);
    let next = assign(&mut h);
    assert_ne!(old.worker, next.worker);
    let unchanged = h.view();
    assert!(matches!(
        apply(
            &mut h,
            Event::Complete {
                attempt: old.id,
                worker: old.worker,
                incarnation: old.incarnation,
                result: result(9),
            }
        ),
        Reply::Rejected(Rejection::Stale)
    ));
    assert_eq!(h.view(), unchanged);
    finish(&mut h, &next, 2);
    apply(
        &mut h,
        Event::Cleanup {
            attempt: old.id,
            worker: old.worker,
            incarnation: old.incarnation,
        },
    );
    assert!(h.view().reservations.iter().all(|r| r.released));
    let stable = h.view();
    apply(
        &mut h,
        Event::Cleanup {
            attempt: old.id,
            worker: old.worker,
            incarnation: old.incarnation,
        },
    );
    apply(
        &mut h,
        Event::Complete {
            attempt: next.id,
            worker: next.worker,
            incarnation: next.incarnation,
            result: result(8),
        },
    );
    assert_eq!(h.view(), stable);
}

#[test]
fn worker_replacement_quarantines_uncertain_capacity() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let old = assign(&mut h);
    apply(&mut h, register(1, 2, 1, 1));
    assert!(h.view().workers[0].quarantined);
    assert!(!h.view().reservations[0].released);
    assert!(!matches!(
        apply(&mut h, Event::Match),
        Reply::Matched { .. }
    ));
    apply(
        &mut h,
        Event::Cleanup {
            attempt: old.id,
            worker: old.worker,
            incarnation: old.incarnation,
        },
    );
    assert!(!h.view().workers[0].quarantined);
    let next = assign(&mut h);
    assert_eq!(next.incarnation, IncarnationId(2));
    let before = h.view();
    apply(&mut h, register(1, 1, 2, 1));
    assert_eq!(h.view(), before);
}

#[test]
fn reconnect_and_changed_advertisements_do_not_recreate_capacity() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let original = assign(&mut h);
    submit(&mut h, 2, request(2));
    apply(&mut h, register(1, 1, 2, 1));
    assert_eq!(h.view().workers[0].charged.slots, 1);
    assert!(!matches!(
        apply(&mut h, Event::Match),
        Reply::Matched { .. }
    ));
    finish(&mut h, &original, 1);
    assert_eq!(h.view().workers[0].charged, Resources::default());
    let next = assign(&mut h);
    assert_ne!(next.id, original.id);
}

#[test]
fn coalesced_handles_cancel_independently_and_rekey_priority() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    let execution = submit(&mut h, 1, request(1));
    let mut urgent = request(1);
    urgent.priority = -10;
    assert_eq!(submit(&mut h, 2, urgent), execution);
    assert_eq!(h.view().executions[0].effective_priority, Some(-10));
    let ordinal = h.view().executions[0].enqueue_ordinal;
    apply(
        &mut h,
        Event::Cancel {
            operation: OperationId(2),
        },
    );
    assert_eq!(h.view().executions[0].effective_priority, Some(0));
    assert_eq!(h.view().executions[0].enqueue_ordinal, ordinal);
    let cancelled = h.view().operations[1].clone();
    let before = h.view();
    apply(
        &mut h,
        Event::Observe {
            operation: OperationId(1),
        },
    );
    apply(
        &mut h,
        Event::Observe {
            operation: OperationId(1),
        },
    );
    assert_eq!(h.view(), before);
    let attempt = assign(&mut h);
    finish(&mut h, &attempt, 1);
    assert_eq!(h.view().operations[1], cancelled);
    assert_eq!(cancelled.terminal, Some(TerminalResult::Cancelled));
    assert!(matches!(
        h.view().operations[0].terminal,
        Some(TerminalResult::Completed(_))
    ));
}

#[test]
fn numeric_priority_and_fifo_age_survive_joins() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 3));
    let oldest = submit(&mut h, 1, request(1));
    let mut positive = request(2);
    positive.priority = 10;
    let last = submit(&mut h, 2, positive);
    let mut negative = request(3);
    negative.priority = -10;
    let first = submit(&mut h, 3, negative);
    let mut promotion = request(1);
    promotion.priority = -10;
    submit(&mut h, 4, promotion);
    assert_eq!(assign(&mut h).execution, oldest);
    assert_eq!(assign(&mut h).execution, first);
    assert_eq!(assign(&mut h).execution, last);
}

#[test]
fn skip_lookup_never_joins_lookup_but_can_join_actual_work_and_publish() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    let mut ordinary = request(1);
    ordinary.skip_cache_lookup = false;
    let lookup_execution = submit(&mut h, 1, ordinary);
    let lookup = h.view().effects[0].id;
    let actual_execution = submit(&mut h, 2, request(1));
    assert_ne!(lookup_execution, actual_execution);
    apply(
        &mut h,
        Event::LookupResult {
            effect: lookup,
            result: Some(result(99)),
        },
    );
    assert_eq!(submit(&mut h, 3, request(1)), actual_execution);
    let attempt = assign(&mut h);
    finish(&mut h, &attempt, 1);
    assert!(h.view().effects.iter().any(|e| matches!(&e.kind,
        EffectKind::PublishResult { execution, .. } if *execution == actual_execution)));
    assert_ne!(submit(&mut h, 4, request(1)), actual_execution);
    assert_eq!(
        h.view().operations[0].terminal,
        Some(TerminalResult::Completed(result(99)))
    );
}

#[test]
fn noncacheable_work_is_independent_and_has_no_publication_intent() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 2));
    let mut req = request(1);
    req.do_not_cache = true;
    req.skip_cache_lookup = false;
    let first = submit(&mut h, 1, req.clone());
    let second = submit(&mut h, 2, req);
    assert_ne!(first, second);
    assert!(h.view().effects.is_empty());
    let a = assign(&mut h);
    let b = assign(&mut h);
    finish(&mut h, &a, 1);
    finish(&mut h, &b, 2);
    assert!(h.view().effects.is_empty());
}

#[test]
fn buck2_scope_and_digest_size_are_distinct_inflight_identities() {
    let mut h = harness();
    let mut a = request(1);
    a.execution_scope = Some("invocation-a".into());
    let mut b = a.clone();
    b.execution_scope = Some("invocation-b".into());
    assert_eq!(a.action, b.action);
    let first = submit(&mut h, 1, a.clone());
    assert_ne!(submit(&mut h, 2, b), first);
    a.action.digest_size_bytes = 2;
    assert_ne!(submit(&mut h, 3, a), first);
}

#[test]
fn progress_generations_fence_timers_and_heartbeats_do_not_mask_stalls() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    apply(
        &mut h,
        Event::WorkerAccepted {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        },
    );
    apply(&mut h, Event::AdvanceTime { now: 5 });
    apply(
        &mut h,
        Event::Progress {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
            phase: Phase::Preparing,
            progress: 1,
        },
    );
    apply(
        &mut h,
        Event::AdvanceTime {
            now: attempt.deadline.at,
        },
    );
    apply(
        &mut h,
        Event::ExpireAttempt {
            attempt: attempt.id,
            generation: attempt.deadline.generation,
        },
    );
    assert!(h.view().attempts[0].authorized);
    let deadline = h.view().attempts[0].deadline;
    apply(
        &mut h,
        Event::Heartbeat {
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            session: SessionId(1),
        },
    );
    assert_eq!(h.view().attempts[0].deadline, deadline);
    apply(&mut h, Event::AdvanceTime { now: deadline.at });
    apply(
        &mut h,
        Event::ExpireAttempt {
            attempt: attempt.id,
            generation: deadline.generation,
        },
    );
    assert!(!h.view().attempts[0].authorized);
    assert!(!h.view().reservations[0].released);
    apply(&mut h, Event::AdvanceTime { now: 0 });
    assert_eq!(h.view().now, deadline.at);
}

#[test]
fn last_interest_cancellation_requires_cleanup_even_without_observers() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    let old_dispatch = h.view().effects[0].clone();
    apply(
        &mut h,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    assert_eq!(
        h.view().operations[0].terminal,
        Some(TerminalResult::Cancelled)
    );
    assert!(!h.view().reservations[0].released);
    assert!(
        h.view()
            .effects
            .iter()
            .all(|effect| effect.id != old_dispatch.id)
    );
    assert!(
        h.view()
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
    );
    // This event represents confirmed cleanup. Recipient-side delayed dispatch
    // and termination uncertainty are exercised separately in adversarial.rs.
    apply(
        &mut h,
        Event::Cleanup {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        },
    );
    assert!(h.view().reservations[0].released);
}

#[test]
fn retained_snapshot_plus_suffix_matches_uninterrupted_execution() {
    let mut h = harness();
    apply(&mut h, register(1, 1, 1, 1));
    submit(&mut h, 1, request(1));
    let attempt = assign(&mut h);
    h.checkpoint().unwrap();
    finish(&mut h, &attempt, 1);
    let before = h.core.snapshot().unwrap();
    h.crash_restore().unwrap();
    assert_eq!(h.core.snapshot().unwrap(), before);
    let accepted = h.view().operations[0].clone();
    apply(
        &mut h,
        Event::Cancel {
            operation: OperationId(1),
        },
    );
    assert_eq!(h.view().operations[0], accepted);
}

#[test]
fn wrong_epoch_and_overload_do_not_install_state() {
    let limits = Limits {
        max_executions: 1,
        max_operations: 1,
        ..Limits::default()
    };
    let mut h = Harness::new(ClusterEpoch(1), limits);
    submit(&mut h, 1, request(1));
    let before = h.view();
    assert_eq!(
        apply(
            &mut h,
            Event::Submit {
                operation: OperationId(2),
                request: request(2)
            }
        ),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!(h.view(), before);
    let wrong = RecordedEvent {
        epoch: ClusterEpoch(2),
        event: Event::Cancel {
            operation: OperationId(1),
        },
    };
    h.propose(99, wrong).unwrap();
    h.commit(99).unwrap();
    assert_eq!(
        h.apply_next().unwrap(),
        Reply::Rejected(Rejection::WrongEpoch)
    );
    assert_eq!(h.view(), before);
}

#[test]
fn versioned_scenarios_replay_separate_input_and_delivery_schedules() {
    let scenario = Scenario {
        version: 1,
        name: "commit-crash-delivery".into(),
        seed: 0,
        epoch: ClusterEpoch(1),
        limits: Limits::default(),
        inputs: vec![Input {
            id: 1,
            parents: vec![],
            event: RecordedEvent {
                epoch: ClusterEpoch(1),
                event: Event::Submit {
                    operation: OperationId(1),
                    request: Request {
                        skip_cache_lookup: false,
                        ..request(1)
                    },
                },
            },
        }],
        schedule: vec![
            Schedule::Propose(1),
            Schedule::Commit(1),
            Schedule::Crash,
            Schedule::Checkpoint,
            Schedule::Deliver(EffectId(1)),
            Schedule::Crash,
            Schedule::Deliver(EffectId(1)),
        ],
    };
    let bytes = serde_json::to_vec(&scenario).unwrap();
    let decoded: Scenario = serde_json::from_slice(&bytes).unwrap();
    let replayed = decoded.replay().unwrap();
    assert_eq!(replayed.delivered.len(), 2);
    assert_eq!(replayed.delivered[0], replayed.delivered[1]);
    assert_eq!(replayed.view(), scenario.replay().unwrap().view());

    let fixture: Scenario =
        serde_json::from_str(include_str!("authority/fixtures/committed_lookup.json")).unwrap();
    assert_eq!(fixture.replay().unwrap().view(), replayed.view());
}

#[test]
fn causal_shrinking_preserves_required_producers() {
    let input = |id, parents| Input {
        id,
        parents,
        event: RecordedEvent {
            epoch: ClusterEpoch(1),
            event: Event::AdvanceTime { now: id },
        },
    };
    let history = vec![
        input(1, vec![]),
        input(2, vec![]),
        input(3, vec![2]),
        input(4, vec![3]),
    ];
    let reduced = shrink_inputs(&history, |candidate| {
        candidate.iter().any(|input| input.id == 4)
    });
    assert_eq!(
        reduced.iter().map(|input| input.id).collect::<Vec<_>>(),
        vec![2, 3, 4]
    );
}

#[test]
fn schedule_shrinking_preserves_snapshot_crash_and_duplicate_delivery_race() {
    let mut scenario: Scenario =
        serde_json::from_str(include_str!("authority/fixtures/committed_lookup.json")).unwrap();
    scenario.inputs.insert(
        0,
        Input {
            id: 99,
            parents: vec![],
            event: RecordedEvent {
                epoch: scenario.epoch,
                event: Event::AdvanceTime { now: 0 },
            },
        },
    );
    scenario.schedule.splice(
        0..0,
        [Schedule::Propose(99), Schedule::Commit(99), Schedule::Apply],
    );
    scenario.schedule.push(Schedule::Checkpoint);
    let has_race = |candidate: &Scenario| {
        let Ok(replayed) = candidate.replay() else {
            return false;
        };
        let mut checkpoint = false;
        let mut deliveries = 0;
        let mut crashed_after_first = false;
        for step in &candidate.schedule {
            match step {
                Schedule::Checkpoint => checkpoint = true,
                Schedule::Deliver(_) if checkpoint => {
                    deliveries += 1;
                    if deliveries == 2 && crashed_after_first {
                        return replayed.delivered[0] == replayed.delivered[1];
                    }
                }
                Schedule::Crash if deliveries == 1 => crashed_after_first = true,
                _ => {}
            }
        }
        false
    };
    assert!(has_race(&scenario));
    let reduced = shrink_scenario(&scenario, has_race);
    assert_eq!(reduced.inputs.len(), 1);
    assert_eq!(reduced.inputs[0].id, 1);
    assert_eq!(reduced.schedule.len(), 7);
    assert!(has_race(&reduced));
    assert!(reduced.replay().is_ok());
}

#[test]
fn schedule_shrinking_rejects_malformed_transport_as_failure_evidence() {
    let mut scenario: Scenario =
        serde_json::from_str(include_str!("authority/fixtures/committed_lookup.json")).unwrap();
    scenario.schedule.remove(0); // Commit has no preceding proposal.
    let mut consulted = false;
    let unchanged = shrink_scenario(&scenario, |_| {
        consulted = true;
        true
    });
    assert!(!consulted);
    assert_eq!(unchanged.schedule.len(), scenario.schedule.len());
}
