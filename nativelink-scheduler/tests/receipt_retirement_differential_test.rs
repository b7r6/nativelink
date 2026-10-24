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

//! The adapter uses public State methods only. All expected identities, grants,
//! release facts, and allocation cursors come from the explicit scenario below.

#[path = "receipt_retirement/differential.rs"]
mod differential;
#[path = "receipt_retirement/reference.rs"]
mod reference;

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use differential::{Candidate, Differential};
use nativelink_scheduler::authority::{
    ActionKey, AttemptId, ClusterEpoch, EffectId, Event, ExecutionId, IncarnationId, Limits,
    ObservableState, OperationId, ReceiptBinding, ReceiptCertificate, ReceiptRetirement,
    RecordedEvent, Rejection, Reply, Request, ReservationId, Resources, ResultPayload, SessionId,
    State, TERMINAL_RETENTION, WorkerId,
};
use reference::{AttemptFact, ExecutionFact, Facts};

const EPOCH: ClusterEpoch = ClusterEpoch(37);
const WORKER: WorkerId = WorkerId(1);
const GRANT: Resources = Resources {
    cpu_millis: 100,
    memory_bytes: 1024,
    slots: 1,
};
const CAPACITY: Resources = Resources {
    cpu_millis: 6400,
    memory_bytes: 65536,
    slots: 64,
};

struct Adapter(Rc<RefCell<State>>);

impl Candidate for Adapter {
    fn enable_receipts(&mut self) -> Result<(), String> {
        self.0.borrow_mut().enable_receipt_retirement()
    }

    fn apply(&mut self, event: RecordedEvent) -> Reply {
        let reply = self.0.borrow_mut().apply(event.clone());
        if std::env::var_os("NL_RECEIPT_TRACE").is_some() {
            let event_json = serde_json::to_string(&event).unwrap();
            let reply_json = serde_json::to_string(&reply).unwrap();
            eprintln!("receipt differential: {event_json} => {reply_json}");
        }
        reply
    }

    fn receipt_projection(&self) -> Option<ReceiptRetirement> {
        self.0.borrow().receipt_retirement().cloned()
    }

    fn lifecycle_projection(&self) -> ObservableState {
        self.0.borrow().view()
    }

    fn checkpoint_roundtrip(&mut self) -> Result<(), String> {
        let bytes = self.0.borrow().snapshot()?;
        let restored = State::restore(&bytes)?;
        *self.0.borrow_mut() = restored;
        Ok(())
    }
}

struct Fixture {
    actual: Rc<RefCell<State>>,
    driver: Differential<Adapter>,
    next_operation: u64,
    session: u64,
    now: u64,
}

impl Fixture {
    fn new() -> Self {
        let actual = Rc::new(RefCell::new(State::new(EPOCH, Limits::default()).unwrap()));
        Self {
            driver: Differential::new(Adapter(Rc::clone(&actual)), EPOCH, Facts::default()),
            actual,
            next_operation: 1,
            session: 0,
            now: 0,
        }
    }

    const fn event(event: Event) -> RecordedEvent {
        RecordedEvent {
            epoch: EPOCH,
            event,
        }
    }

    fn register(&mut self, incarnation: IncarnationId) {
        self.session += 1;
        self.driver.baseline(
            Self::event(Event::Register {
                worker: WORKER,
                incarnation,
                session: SessionId(self.session),
                properties: BTreeMap::new(),
                capacity: CAPACITY,
                available: CAPACITY,
            }),
            &Reply::Applied,
            |facts, model| {
                if let Some(previous) = facts.workers.insert(WORKER, incarnation)
                    && previous != incarnation
                {
                    for attempt in facts.attempts.values_mut().filter(|attempt| {
                        attempt.worker == WORKER
                            && attempt.incarnation == previous
                            && !attempt.released
                            && attempt.binding.cancel_effect.is_none()
                    }) {
                        attempt.binding.cancel_effect = Some(EffectId(facts.next_effect));
                        facts.next_effect += 1;
                        facts
                            .executions
                            .get_mut(&attempt.binding.execution)
                            .unwrap()
                            .current_attempt = false;
                    }
                }
                model.registered(WORKER, incarnation);
            },
        );
        self.check_physical_facts();
    }

    fn submit_and_match(&mut self) -> (ExecutionId, AttemptId) {
        let operation = OperationId(self.next_operation);
        let execution = ExecutionId(self.next_operation);
        self.next_operation += 1;
        let request = Request {
            action: ActionKey {
                instance: "receipt-reference".into(),
                digest_function: "sha256".into(),
                digest: format!("{:064x}", operation.0),
                digest_size_bytes: 1,
            },
            execution_scope: None,
            do_not_cache: true,
            skip_cache_lookup: true,
            priority: 0,
            properties: BTreeMap::new(),
            resources: GRANT,
            execution_timeout: 100,
            payload: None,
        };
        self.driver.baseline(
            Self::event(Event::Submit { operation, request }),
            &Reply::Submitted {
                operation,
                execution,
            },
            |facts, _| {
                facts.executions.insert(
                    execution,
                    ExecutionFact {
                        terminal: false,
                        attempts_started: 0,
                        current_attempt: false,
                        do_not_cache: true,
                    },
                );
            },
        );
        let attempt = AttemptId(self.driver.facts().next_attempt);
        self.driver.baseline(
            Self::event(Event::Match),
            &Reply::Matched {
                execution,
                attempt,
                worker: WORKER,
            },
            |facts, _| {
                facts.attempts.insert(
                    attempt,
                    AttemptFact {
                        worker: WORKER,
                        incarnation: facts.workers[&WORKER],
                        released: false,
                        binding: ReceiptBinding {
                            attempt,
                            execution,
                            dispatch_effect: EffectId(facts.next_effect),
                            reservation: ReservationId(facts.next_reservation),
                            cancel_effect: None,
                        },
                    },
                );
                facts.next_attempt += 1;
                facts.next_effect += 1;
                facts.next_reservation += 1;
                let execution = facts.executions.get_mut(&execution).unwrap();
                execution.attempts_started += 1;
                execution.current_attempt = true;
            },
        );
        let incarnation = self.driver.facts().workers[&WORKER];
        self.driver.baseline(
            Self::event(Event::WorkerAccepted {
                attempt,
                worker: WORKER,
                incarnation,
            }),
            &Reply::Applied,
            |_, _| {},
        );
        self.check_physical_facts();
        (execution, attempt)
    }

    fn complete(&mut self, attempt: AttemptId) {
        let incarnation = self.driver.facts().attempts[&attempt].incarnation;
        self.driver.baseline(
            Self::event(Event::Complete {
                attempt,
                worker: WORKER,
                incarnation,
                result: ResultPayload {
                    digest: "accepted-synthetic-result".into(),
                    exit_code: 0,
                    error: None,
                    response: None,
                },
            }),
            &Reply::Applied,
            |facts, model| {
                let attempt = facts.attempts.get_mut(&attempt).unwrap();
                attempt.released = true;
                let execution = facts
                    .executions
                    .get_mut(&attempt.binding.execution)
                    .unwrap();
                execution.terminal = true;
                execution.current_attempt = false;
                model.released(attempt.worker, attempt.incarnation);
            },
        );
        self.check_physical_facts();
    }

    fn cancel(&mut self, execution: ExecutionId, attempt: AttemptId) {
        self.driver.baseline(
            Self::event(Event::Cancel {
                operation: OperationId(execution.0),
            }),
            &Reply::Applied,
            |facts, _| {
                facts
                    .attempts
                    .get_mut(&attempt)
                    .unwrap()
                    .binding
                    .cancel_effect = Some(EffectId(facts.next_effect));
                facts.next_effect += 1;
                let execution = facts.executions.get_mut(&execution).unwrap();
                execution.terminal = true;
                execution.current_attempt = false;
            },
        );
        self.check_physical_facts();
    }

    fn cleanup(&mut self, attempt: AttemptId) {
        let incarnation = self.driver.facts().attempts[&attempt].incarnation;
        self.driver.baseline(
            Self::event(Event::Cleanup {
                attempt,
                worker: WORKER,
                incarnation,
            }),
            &Reply::Applied,
            |facts, model| {
                let attempt = facts.attempts.get_mut(&attempt).unwrap();
                attempt.released = true;
                model.released(attempt.worker, attempt.incarnation);
            },
        );
        self.check_physical_facts();
    }

    fn retire(&mut self, execution: ExecutionId, expected: &Reply) {
        self.now += TERMINAL_RETENTION + 1;
        self.driver.baseline(
            Self::event(Event::AdvanceTime { now: self.now }),
            &Reply::Applied,
            |_, _| {},
        );
        let incarnation = self.driver.facts().workers[&WORKER];
        self.driver.baseline(
            Self::event(Event::Heartbeat {
                worker: WORKER,
                incarnation,
                session: SessionId(self.session),
            }),
            &Reply::Applied,
            |_, _| {},
        );
        self.driver.baseline(
            Self::event(Event::RetireExecution { execution }),
            expected,
            |facts, _| {
                if *expected == Reply::Applied {
                    facts
                        .attempts
                        .retain(|_, attempt| attempt.binding.execution != execution);
                    facts.executions.remove(&execution);
                }
            },
        );
        self.check_physical_facts();
    }

    fn issue(&mut self) -> Reply {
        let incarnation = self.driver.facts().workers[&WORKER];
        let after_ack = self.driver.expected().projection().and_then(|retirement| {
            retirement.streams[&WORKER]
                .acknowledged
                .map(|ack| ack.effect)
        });
        self.driver.receipt(Self::event(Event::IssueReceipts {
            worker: WORKER,
            incarnation,
            after_ack,
        }))
    }

    fn certificate(&self) -> ReceiptCertificate {
        self.driver.expected().projection().unwrap().streams[&WORKER]
            .pending
            .clone()
            .unwrap()
    }

    fn acknowledge(&mut self, certificate: ReceiptCertificate) -> Reply {
        let incarnation = self.driver.facts().workers[&WORKER];
        self.driver.receipt(Self::event(Event::AcknowledgeReceipts {
            worker: WORKER,
            incarnation,
            certificate,
        }))
    }

    fn check_physical_facts(&self) {
        let view = self.actual.borrow().view();
        let actual: BTreeMap<_, _> = view
            .reservations
            .iter()
            .map(|row| {
                (
                    row.attempt,
                    (
                        row.id,
                        row.worker,
                        row.incarnation,
                        row.released,
                        row.charged,
                    ),
                )
            })
            .collect();
        let expected: BTreeMap<_, _> = self
            .driver
            .facts()
            .attempts
            .iter()
            .map(|(id, attempt)| {
                (
                    *id,
                    (
                        attempt.binding.reservation,
                        attempt.worker,
                        attempt.incarnation,
                        attempt.released,
                        GRANT,
                    ),
                )
            })
            .collect();
        assert_eq!(
            actual, expected,
            "original physical obligations differ from scenario"
        );
        let actual: BTreeMap<_, _> = view
            .attempts
            .iter()
            .map(|row| {
                (
                    row.id,
                    (
                        row.execution,
                        row.worker,
                        row.incarnation,
                        row.dispatch_effect,
                        row.reservation,
                    ),
                )
            })
            .collect();
        let expected: BTreeMap<_, _> = self
            .driver
            .facts()
            .attempts
            .iter()
            .map(|(id, attempt)| {
                (
                    *id,
                    (
                        attempt.binding.execution,
                        attempt.worker,
                        attempt.incarnation,
                        attempt.binding.dispatch_effect,
                        attempt.binding.reservation,
                    ),
                )
            })
            .collect();
        assert_eq!(
            actual, expected,
            "attempt identities differ from independent scenario"
        );
    }
}

#[test]
fn real_state_disabled_empty_frontier_and_ack_scope() {
    let mut fixture = Fixture::new();
    fixture.register(IncarnationId(1));
    assert_eq!(fixture.issue(), Reply::Rejected(Rejection::Invalid));
    fixture.driver.activate();
    assert_eq!(fixture.issue(), Reply::Applied);
    let certificate = fixture.certificate();
    assert_eq!(
        (
            certificate.attempts_through.0,
            certificate.reservations_through.0
        ),
        (0, 0)
    );
    let mut corrupted = certificate.clone();
    corrupted.effects_through.0 += 1;
    assert_eq!(
        fixture.acknowledge(corrupted),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(fixture.acknowledge(certificate.clone()), Reply::Applied);
    assert_eq!(
        fixture.acknowledge(certificate),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(fixture.issue(), Reply::Ignored);
    fixture.driver.checkpoint();
}

#[test]
fn real_state_pending_cut_survives_cancel_cleanup_pins_and_corrupt_ack() {
    let mut fixture = Fixture::new();
    fixture.register(IncarnationId(1));
    fixture.submit_and_match();
    let (newer_execution, newer_attempt) = fixture.submit_and_match();
    fixture.driver.activate();
    assert_eq!(fixture.issue(), Reply::Applied);
    let certificate = fixture.certificate();
    fixture.cancel(newer_execution, newer_attempt);
    fixture.cleanup(newer_attempt);
    fixture.driver.checkpoint();
    assert_eq!(fixture.certificate(), certificate);
    assert!(
        fixture
            .driver
            .expected()
            .blocks_retirement(newer_execution, fixture.driver.facts())
    );
    fixture.retire(newer_execution, &Reply::Rejected(Rejection::Busy));
    for field in 0..7 {
        let mut corrupted = certificate.clone();
        match field {
            0 => corrupted.effect.0 += 1,
            1 => corrupted.attempts_through.0 += 1,
            2 => corrupted.effects_through.0 += 1,
            3 => corrupted.reservations_through.0 += 1,
            4 => corrupted.retain[0].execution.0 += 1,
            5 => corrupted.retain[0].cancel_effect = Some(EffectId(999)),
            _ => corrupted.retain.reverse(),
        }
        assert_eq!(
            fixture.acknowledge(corrupted),
            Reply::Rejected(Rejection::Stale)
        );
    }
    assert_eq!(fixture.acknowledge(certificate), Reply::Applied);
    fixture.driver.checkpoint();
    fixture.retire(newer_execution, &Reply::Applied);
    assert_eq!(fixture.issue(), Reply::Applied);
    assert_eq!(fixture.certificate().retain.len(), 1);
    assert_eq!(fixture.certificate().retain[0].attempt, AttemptId(1));
}

#[test]
fn real_state_forty_eight_completed_attempts_cross_one_old_active_hole() {
    let mut fixture = Fixture::new();
    fixture.register(IncarnationId(1));
    fixture.submit_and_match();
    fixture.driver.activate();
    assert_eq!(fixture.issue(), Reply::Applied);
    assert_eq!(fixture.acknowledge(fixture.certificate()), Reply::Applied);
    for iteration in 0..48 {
        let (execution, attempt) = fixture.submit_and_match();
        fixture.complete(attempt);
        assert_eq!(fixture.issue(), Reply::Applied);
        let certificate = fixture.certificate();
        assert_eq!(certificate.retain.len(), 1);
        assert_eq!(certificate.retain[0].attempt, AttemptId(1));
        fixture.retire(execution, &Reply::Applied);
        if iteration % 8 == 0 {
            fixture.driver.checkpoint();
        }
        assert_eq!(fixture.acknowledge(certificate), Reply::Applied);
    }
    fixture.check_physical_facts();
    assert_eq!(fixture.driver.facts().attempts.len(), 1);
    assert!(!fixture.driver.facts().attempts[&AttemptId(1)].released);
}

#[test]
fn real_state_replacement_discards_old_control_without_refund_or_new_stream_dirt() {
    let mut fixture = Fixture::new();
    fixture.register(IncarnationId(1));
    let (_, attempt) = fixture.submit_and_match();
    fixture.driver.activate();
    assert_eq!(fixture.issue(), Reply::Applied);
    let old = fixture.certificate();
    fixture.driver.checkpoint();
    fixture.register(IncarnationId(2));
    assert!(fixture.driver.expected().pins().is_empty());
    assert!(!fixture.driver.facts().attempts[&attempt].released);
    assert_eq!(
        fixture
            .driver
            .receipt(Fixture::event(Event::AcknowledgeReceipts {
                worker: WORKER,
                incarnation: IncarnationId(1),
                certificate: old.clone(),
            })),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(fixture.acknowledge(old), Reply::Rejected(Rejection::Stale));
    assert_eq!(fixture.issue(), Reply::Applied);
    let current = fixture.certificate();
    assert_eq!(current.retain, Vec::<ReceiptBinding>::new());
    fixture.register(IncarnationId(2));
    assert_eq!(fixture.certificate(), current);
    assert_eq!(fixture.acknowledge(current), Reply::Applied);
    fixture.cleanup(attempt);
    assert_eq!(fixture.issue(), Reply::Ignored);
    fixture.driver.checkpoint();
}

#[test]
fn real_state_snapshot_and_suffix_matches_uninterrupted_history() {
    let mut uninterrupted = Fixture::new();
    let mut restored = Fixture::new();
    for fixture in [&mut uninterrupted, &mut restored] {
        fixture.register(IncarnationId(1));
        fixture.submit_and_match();
        fixture.driver.activate();
        assert_eq!(fixture.issue(), Reply::Applied);
    }
    restored.driver.checkpoint();
    for fixture in [&mut uninterrupted, &mut restored] {
        fixture.cancel(ExecutionId(1), AttemptId(1));
        fixture.cleanup(AttemptId(1));
        let certificate = fixture.certificate();
        assert_eq!(fixture.acknowledge(certificate), Reply::Applied);
        assert_eq!(fixture.issue(), Reply::Applied);
        assert_eq!(fixture.certificate().retain, Vec::<ReceiptBinding>::new());
    }
    assert_eq!(uninterrupted.driver.expected(), restored.driver.expected());
    assert_eq!(
        uninterrupted.actual.borrow().snapshot().unwrap(),
        restored.actual.borrow().snapshot().unwrap()
    );
}

#[test]
fn real_state_replayed_proposal_cannot_create_successor_after_ack() {
    let mut fixture = Fixture::new();
    fixture.register(IncarnationId(1));
    fixture.submit_and_match();
    fixture.driver.activate();
    let original_proposal = Fixture::event(Event::IssueReceipts {
        worker: WORKER,
        incarnation: IncarnationId(1),
        after_ack: None,
    });
    assert_eq!(
        fixture.driver.receipt(original_proposal.clone()),
        Reply::Applied
    );
    let first = fixture.certificate();
    fixture.cancel(ExecutionId(1), AttemptId(1));
    fixture.cleanup(AttemptId(1));
    assert_eq!(
        fixture.driver.receipt(original_proposal.clone()),
        Reply::Ignored
    );
    assert_eq!(fixture.acknowledge(first.clone()), Reply::Applied);
    assert_eq!(
        fixture.driver.receipt(original_proposal),
        Reply::Rejected(Rejection::Stale)
    );
    assert_eq!(fixture.issue(), Reply::Applied);
    assert_eq!(
        fixture.acknowledge(first),
        Reply::Rejected(Rejection::Stale)
    );
    fixture.driver.checkpoint();
}
