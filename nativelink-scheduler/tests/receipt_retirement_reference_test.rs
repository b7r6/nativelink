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

#[allow(
    dead_code,
    reason = "real-State adapter is supplied by a separate candidate target"
)]
#[path = "receipt_retirement/differential.rs"]
mod differential;
#[path = "receipt_retirement/reference.rs"]
mod reference;

use std::collections::{BTreeSet, VecDeque};

use nativelink_scheduler::authority::{
    AttemptId, ClusterEpoch, EffectId, Event, ExecutionId, IncarnationId, MAX_RECEIPT_BINDINGS,
    ReceiptBinding, ReceiptCertificate, RecordedEvent, Rejection, Reply, ReservationId, WorkerId,
};
use reference::{AttemptFact, ExecutionFact, Facts, Reference, protobuf_certificate_len};

const EPOCH: ClusterEpoch = ClusterEpoch(37);
const WORKER: WorkerId = WorkerId(1);
const INCARNATION: IncarnationId = IncarnationId(1);

fn initial(count: u64) -> (Reference, Facts) {
    let mut facts = Facts::default();
    facts.workers.insert(WORKER, INCARNATION);
    for _ in 0..count {
        add_attempt(&mut facts);
    }
    (Reference::new(EPOCH), facts)
}

fn add_attempt(facts: &mut Facts) -> AttemptId {
    let id = AttemptId(facts.next_attempt);
    let execution = ExecutionId(id.0);
    facts.executions.insert(
        execution,
        ExecutionFact {
            terminal: false,
            attempts_started: 1,
            current_attempt: true,
            do_not_cache: false,
        },
    );
    facts.attempts.insert(
        id,
        AttemptFact {
            worker: WORKER,
            incarnation: facts.workers[&WORKER],
            binding: ReceiptBinding {
                attempt: id,
                execution,
                dispatch_effect: EffectId(facts.next_effect),
                reservation: ReservationId(facts.next_reservation),
                cancel_effect: None,
            },
            released: false,
        },
    );
    facts.next_attempt += 1;
    facts.next_effect += 1;
    facts.next_reservation += 1;
    id
}

fn release(model: &mut Reference, facts: &mut Facts, attempt: AttemptId) {
    let row = facts.attempts.get_mut(&attempt).expect("scenario attempt");
    if !row.released {
        row.released = true;
        model.released(row.worker, row.incarnation);
        let execution = facts
            .executions
            .get_mut(&row.binding.execution)
            .expect("scenario execution");
        execution.terminal = true;
        execution.current_attempt = false;
    }
}

fn cancel(facts: &mut Facts, attempt: AttemptId) {
    let execution = facts.attempts[&attempt].binding.execution;
    facts
        .attempts
        .get_mut(&attempt)
        .expect("scenario attempt")
        .binding
        .cancel_effect = Some(EffectId(facts.next_effect));
    facts.next_effect += 1;
    let execution = facts.executions.get_mut(&execution).unwrap();
    execution.terminal = true;
    execution.current_attempt = false;
}

fn issue(model: &mut Reference, facts: &mut Facts, after_ack: Option<EffectId>) -> Reply {
    model.apply(
        RecordedEvent {
            epoch: EPOCH,
            event: Event::IssueReceipts {
                worker: WORKER,
                incarnation: facts.workers[&WORKER],
                after_ack,
            },
        },
        facts,
    )
}

fn pending(model: &Reference) -> ReceiptCertificate {
    model.projection().expect("activated").streams[&WORKER]
        .pending
        .clone()
        .expect("pending")
}

fn acknowledge(model: &mut Reference, facts: &mut Facts, certificate: ReceiptCertificate) -> Reply {
    model.apply(
        RecordedEvent {
            epoch: EPOCH,
            event: Event::AcknowledgeReceipts {
                worker: WORKER,
                incarnation: facts.workers[&WORKER],
                certificate,
            },
        },
        facts,
    )
}

#[test]
fn disabled_scope_precedence_empty_frontiers_and_duplicate_ack() {
    let (mut model, mut facts) = initial(0);
    let original = model.clone();
    assert_eq!(
        issue(&mut model, &mut facts, None),
        Reply::Rejected(Rejection::Invalid)
    );
    assert_eq!(model, original);
    let wrong_epoch = RecordedEvent {
        epoch: ClusterEpoch(999),
        event: Event::IssueReceipts {
            worker: WorkerId(999),
            incarnation: IncarnationId(999),
            after_ack: None,
        },
    };
    assert_eq!(
        model.apply(wrong_epoch, &mut facts),
        Reply::Rejected(Rejection::WrongEpoch)
    );
    model.activate(&facts);
    model.activate(&facts);
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Applied);
    let certificate = pending(&model);
    assert_eq!(
        (
            certificate.effect.0,
            certificate.attempts_through.0,
            certificate.effects_through.0,
            certificate.reservations_through.0
        ),
        (1, 0, 1, 0)
    );
    assert_eq!(certificate.retain, Vec::<ReceiptBinding>::new());
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Ignored);
    let before_ack = facts.clone();
    assert_eq!(
        acknowledge(&mut model, &mut facts, certificate.clone()),
        Reply::Applied
    );
    assert_eq!(facts, before_ack);
    assert_eq!(
        acknowledge(&mut model, &mut facts, certificate.clone()),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(
        issue(&mut model, &mut facts, Some(certificate.effect)),
        Reply::Ignored
    );
    model.validate(&facts).unwrap();
}

#[test]
fn pending_body_survives_later_cancel_cleanup_replay_and_full_body_spoofs() {
    let (mut model, mut facts) = initial(2);
    model.activate(&facts);
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Applied);
    let certificate = pending(&model);
    cancel(&mut facts, AttemptId(1));
    release(&mut model, &mut facts, AttemptId(1));
    assert_eq!(pending(&model), certificate);
    assert!(model.blocks_retirement(ExecutionId(1), &facts));
    assert!(model.projection().unwrap().streams[&WORKER].dirty);
    model.validate(&facts).unwrap();
    let mut restored = Reference::restore(&model.snapshot(), &facts).unwrap();
    assert_eq!(restored, model);
    for field in 0..8 {
        let mut forged = certificate.clone();
        match field {
            0 => forged.effect.0 += 1,
            1 => forged.attempts_through.0 += 1,
            2 => forged.effects_through.0 += 1,
            3 => forged.reservations_through.0 += 1,
            4 => forged.retain[0].execution.0 += 1,
            5 => forged.retain[0].dispatch_effect.0 += 1,
            6 => forged.retain[0].reservation.0 += 1,
            _ => forged.retain.reverse(),
        }
        let before = (model.clone(), facts.clone());
        assert_eq!(
            acknowledge(&mut model, &mut facts, forged),
            Reply::Rejected(Rejection::Stale)
        );
        assert_eq!((model.clone(), facts.clone()), before);
    }
    let original_facts = facts.clone();
    assert_eq!(
        acknowledge(&mut model, &mut facts, certificate.clone()),
        Reply::Applied
    );
    let mut restored_facts = original_facts.clone();
    assert_eq!(
        acknowledge(&mut restored, &mut restored_facts, certificate.clone()),
        Reply::Applied
    );
    assert_eq!(model, restored);
    assert_eq!(facts, original_facts);
    assert!(!model.blocks_retirement(ExecutionId(1), &facts));
    assert_eq!(
        issue(&mut model, &mut facts, None),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(
        issue(&mut model, &mut facts, Some(certificate.effect)),
        Reply::Applied
    );
    let successor = pending(&model);
    assert_eq!(successor.retain.len(), 1);
    assert_eq!(successor.retain[0].attempt, AttemptId(2));
    assert!(successor.effect > certificate.effects_through);
    model.validate(&facts).unwrap();
}

#[test]
fn old_active_hole_does_not_pin_newer_completed_attempts() {
    let (mut model, mut facts) = initial(1);
    model.activate(&facts);
    let mut after_ack = None;
    for _ in 0..48 {
        let newer = add_attempt(&mut facts);
        release(&mut model, &mut facts, newer);
        assert_eq!(issue(&mut model, &mut facts, after_ack), Reply::Applied);
        let certificate = pending(&model);
        assert_eq!(certificate.retain.len(), 1);
        assert_eq!(certificate.retain[0].attempt, AttemptId(1));
        assert!(!model.blocks_retirement(ExecutionId(newer.0), &facts));
        facts.attempts.remove(&newer);
        facts.executions.remove(&ExecutionId(newer.0));
        model.validate(&facts).unwrap();
        after_ack = Some(certificate.effect);
        let charged_before = facts
            .attempts
            .values()
            .filter(|attempt| !attempt.released)
            .count();
        assert_eq!(
            acknowledge(&mut model, &mut facts, certificate),
            Reply::Applied
        );
        assert_eq!(
            facts
                .attempts
                .values()
                .filter(|attempt| !attempt.released)
                .count(),
            charged_before
        );
    }
    assert_eq!(facts.attempts.len(), 1);
    assert_eq!(facts.next_attempt, 50);
}

#[test]
fn replacement_resets_only_recipient_stream_and_same_incarnation_preserves_it() {
    let (mut model, mut facts) = initial(1);
    model.activate(&facts);
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Applied);
    let original = pending(&model);
    let before_reconnect = model.clone();
    model.registered(WORKER, INCARNATION);
    assert_eq!(model, before_reconnect);
    facts.workers.insert(WORKER, IncarnationId(2));
    model.registered(WORKER, IncarnationId(2));
    assert!(model.pins().is_empty());
    assert!(!facts.attempts[&AttemptId(1)].released);
    let old_ack = RecordedEvent {
        epoch: EPOCH,
        event: Event::AcknowledgeReceipts {
            worker: WORKER,
            incarnation: INCARNATION,
            certificate: original,
        },
    };
    assert_eq!(
        model.apply(old_ack, &mut facts),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Applied);
    assert_eq!(pending(&model).retain, Vec::<ReceiptBinding>::new());
    let new_certificate = pending(&model);
    assert_eq!(
        acknowledge(&mut model, &mut facts, new_certificate),
        Reply::Applied
    );
    release(&mut model, &mut facts, AttemptId(1));
    assert!(!model.projection().unwrap().streams[&WORKER].dirty);
    model.validate(&facts).unwrap();
}

#[test]
fn count_and_identity_headroom_refusals_are_atomic() {
    let count = u64::try_from(MAX_RECEIPT_BINDINGS).unwrap();
    let (mut model, mut facts) = initial(count + 1);
    model.activate(&facts);
    let before = (model.clone(), facts.clone());
    assert_eq!(
        issue(&mut model, &mut facts, None),
        Reply::Rejected(Rejection::Overloaded)
    );
    assert_eq!((model.clone(), facts.clone()), before);
    release(&mut model, &mut facts, AttemptId(count + 1));
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Applied);
    assert_eq!(pending(&model).retain.len(), MAX_RECEIPT_BINDINGS);
    assert!(protobuf_certificate_len(&pending(&model)) <= 64 * 1024);
    for (cursor, expected) in [
        (u64::MAX - 7, Reply::Applied),
        (u64::MAX - 6, Reply::Rejected(Rejection::Overloaded)),
        (u64::MAX, Reply::Rejected(Rejection::Overloaded)),
    ] {
        let (mut model, mut facts) = initial(1);
        facts.next_effect = cursor;
        model.activate(&facts);
        let before = (model.clone(), facts.clone());
        assert_eq!(issue(&mut model, &mut facts, None), expected);
        if expected != Reply::Applied {
            assert_eq!((model.clone(), facts.clone()), before);
        }
    }
}

#[test]
fn restore_rejects_missing_pins_changed_bindings_and_regressed_floors() {
    let (mut model, mut facts) = initial(2);
    cancel(&mut facts, AttemptId(1));
    model.activate(&facts);
    assert_eq!(issue(&mut model, &mut facts, None), Reply::Applied);
    let bytes = model.snapshot();
    let mut removed = facts.clone();
    removed.attempts.remove(&AttemptId(1));
    assert!(Reference::restore(&bytes, &removed).is_err());
    for field in [
        "execution",
        "dispatch_effect",
        "reservation",
        "cancel_effect",
    ] {
        let mut corrupted: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        corrupted["retirement"]["streams"]["1"]["pending"]["retain"][0][field] =
            serde_json::json!(99);
        assert!(Reference::restore(&serde_json::to_vec(&corrupted).unwrap(), &facts).is_err());
    }
    let certificate = pending(&model);
    assert_eq!(
        acknowledge(&mut model, &mut facts, certificate),
        Reply::Applied
    );
    release(&mut model, &mut facts, AttemptId(2));
    let after = model.projection().unwrap().streams[&WORKER]
        .acknowledged
        .unwrap()
        .effect;
    assert_eq!(issue(&mut model, &mut facts, Some(after)), Reply::Applied);
    let mut regressed: serde_json::Value = serde_json::from_slice(&model.snapshot()).unwrap();
    regressed["retirement"]["streams"]["1"]["pending"]["attempts_through"] = serde_json::json!(1);
    assert!(Reference::restore(&serde_json::to_vec(&regressed).unwrap(), &facts).is_err());
}

#[test]
#[expect(clippy::print_stdout, reason = "record bounded exploration counts")]
fn bounded_metadata_interleavings_preserve_pending_identity_and_physical_facts() {
    let (model, facts) = initial(2);
    let mut queue = VecDeque::from([(model, facts, 0_u8)]);
    let mut seen = BTreeSet::new();
    let mut transitions = 0;
    while let Some((model, facts, depth)) = queue.pop_front() {
        let key = (model.snapshot(), serde_json::to_vec(&facts).unwrap());
        if !seen.insert(key) {
            continue;
        }
        model.validate(&facts).unwrap();
        if depth == 9 {
            continue;
        }
        for action in 0..10 {
            let mut next = model.clone();
            let mut next_facts = facts.clone();
            match action {
                0 => next.activate(&next_facts),
                1 => {
                    let after = next.projection().and_then(|value| {
                        value.streams[&WORKER].acknowledged.map(|ack| ack.effect)
                    });
                    issue(&mut next, &mut next_facts, after);
                }
                2 => {
                    if let Some(certificate) = next
                        .projection()
                        .and_then(|value| value.streams[&WORKER].pending.clone())
                    {
                        assert_eq!(
                            acknowledge(&mut next, &mut next_facts, certificate),
                            Reply::Applied
                        );
                        assert_eq!(next_facts, facts, "ack cannot mutate lifecycle facts");
                    }
                }
                3 => {
                    if let Some(mut certificate) = next
                        .projection()
                        .and_then(|value| value.streams[&WORKER].pending.clone())
                    {
                        certificate.effects_through.0 += 1;
                        assert_eq!(
                            acknowledge(&mut next, &mut next_facts, certificate),
                            Reply::Rejected(Rejection::Stale)
                        );
                        assert_eq!(next, model);
                    }
                }
                4 | 5 => release(
                    &mut next,
                    &mut next_facts,
                    AttemptId(u64::from(action - 3_u8)),
                ),
                6 => {
                    next_facts.workers.insert(WORKER, IncarnationId(2));
                    next.registered(WORKER, IncarnationId(2));
                }
                7 => next.registered(WORKER, next_facts.workers[&WORKER]),
                _ => {
                    let attempt = AttemptId(u64::from(action - 7_u8));
                    let row = next_facts.attempts[&attempt];
                    if !row.released && row.binding.cancel_effect.is_none() {
                        cancel(&mut next_facts, attempt);
                    }
                }
            }
            next.validate(&next_facts).unwrap();
            if let Some(old) = model
                .projection()
                .and_then(|value| value.streams[&WORKER].pending.as_ref())
                && let Some(current) = next
                    .projection()
                    .and_then(|value| value.streams[&WORKER].pending.as_ref())
                && old.effect == current.effect
            {
                assert_eq!(old, current);
            }
            assert!(next_facts.next_effect >= facts.next_effect);
            transitions += 1;
            queue.push_back((next, next_facts, depth + 1));
        }
    }
    println!(
        "receipt reference exploration: states={} transitions={transitions} depth=9 actions=10",
        seen.len()
    );
    assert!(seen.len() >= 100);
}
