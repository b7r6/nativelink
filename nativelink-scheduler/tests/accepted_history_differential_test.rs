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

//! Actual-State adapter for the separately frozen accepted-history contract.
//! Every expected fact and allocator advance belongs to the scenario below.

#[path = "accepted_history/types.rs"]
mod types;
use types as history;
#[path = "accepted_history/reference.rs"]
mod reference;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority::accepted_history::{
    HistoryAlgorithm, HistoryAssociation, HistoryObject, HistoryOutcome, HistoryStatus,
};
use nativelink_scheduler::authority::{
    ActionKey, AttemptId, ClusterEpoch, Effect, EffectId, EffectKind, Event, ExecutionId,
    IncarnationId, Limits, MAX_ATTEMPTS, MAX_STATE_BYTES, OperationId, ReceiptBinding,
    ReceiptCertificate, RecordedEvent, Rejection, Reply, Request, ReservationId,
    ResourceFailureReason, Resources, ResultPayload, SessionId, State, TERMINAL_RETENTION,
    TerminalResult, WorkerId,
};
use prost::Message as _;
use reference::Model;

const NAMESPACE: history::Namespace = history::Namespace([29; 32]);
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

#[derive(Clone)]
struct Execution {
    request: Request,
    current: Option<u64>,
    attempts_started: u32,
    target: Resources,
    terminal: Option<TerminalResult>,
}

#[test]
fn real_state_fixed_policy_algorithms_and_zero_caller_identities() {
    for algorithm in [history::Algorithm::Sha256, history::Algorithm::Blake3] {
        for (do_not_cache, exit_code, status) in
            [(false, 0, 0), (true, 0, 0), (false, 1, 0), (true, 137, 8)]
        {
            let mut fixture = Fixture::new(0, 0, 0);
            fixture.activate();
            let (execution, operation) = fixture.submit(7, algorithm, do_not_cache, true, None);
            let attempt = fixture.matched(execution);
            let (payload, raw) = result(
                "fixed policy preserves exact worker response",
                exit_code,
                status,
            );
            let success = exit_code == 0 && status == 0;
            fixture.completion(attempt, payload.clone(), raw, success, None);
            fixture.duplicate_completion(attempt, payload);
            let expected_effect = if success && !do_not_cache { 3 } else { 2 };
            assert_eq!(
                fixture.expected.associations[&execution].effect,
                expected_effect
            );
            assert!(
                fixture
                    .actual
                    .history_for_operation(OperationId(operation))
                    .is_some()
            );
            fixture = fixture.recovered();
            fixture.stored(execution);
            let before = fixture.actual.snapshot().unwrap();
            fixture.actual.enable_accepted_history(NAMESPACE.0).unwrap();
            assert_eq!(fixture.actual.snapshot().unwrap(), before);
            assert!(fixture.actual.enable_accepted_history([30; 32]).is_err());
            assert_eq!(fixture.actual.snapshot().unwrap(), before);
        }
    }
}

#[test]
fn real_state_activation_does_not_backfill_old_terminals_or_cache_hits() {
    let mut fixture = Fixture::new(11, 3, 1);
    let (old, old_operation) = fixture.submit(1, history::Algorithm::Sha256, true, true, None);
    let attempt = fixture.matched(old);
    let (payload, raw) = result("terminal before activation", 0, 0);
    fixture.completion(attempt, payload, raw, true, None);
    let bogus = reference::recipe(NAMESPACE, &fixture.facts.executions[&old].action, b"")
        .unwrap()
        .object;
    let before = fixture.actual.snapshot().unwrap();
    fixture.send(
        Event::HistoryPublication {
            effect: EffectId(99),
            object: object(&bogus),
            outcome: HistoryOutcome::Stored,
        },
        &Reply::Rejected(Rejection::Invalid),
    );
    assert_eq!(fixture.actual.snapshot().unwrap(), before);
    let (unfinished, _) = fixture.submit(2, history::Algorithm::Blake3, true, true, None);
    let attempt = fixture.matched(unfinished);
    fixture.activate();
    assert_eq!(fixture.preactivation, BTreeSet::from([old]));
    fixture = fixture.recovered();
    let (payload, raw) = result("unfinished at activation", 1, 0);
    fixture.completion(attempt, payload, raw, false, None);
    let (cached, cached_operation) =
        fixture.submit(3, history::Algorithm::Sha256, false, false, None);
    let (payload, _) = result("cache hit is not worker acceptance", 0, 0);
    fixture.cache_hit(cached, payload);
    assert!(
        fixture
            .actual
            .history_for_operation(OperationId(old_operation))
            .is_none()
    );
    assert!(
        fixture
            .actual
            .history_for_operation(OperationId(cached_operation))
            .is_none()
    );
    assert_eq!(fixture.expected.associations.len(), 1);
    fixture.retirement_time();
    fixture.retire(old, true);
    fixture.retire(cached, true);
    fixture.retire(unfinished, false);
    fixture.stored(unfinished);
    fixture.retire(unfinished, true);
    fixture.recovered();
}

#[test]
fn real_state_memory_retry_reports_produce_only_final_resource_failed_history() {
    let mut fixture = Fixture::new(12, 4, 2);
    fixture.activate();
    let (execution, operation) = fixture.submit(4, history::Algorithm::Sha256, false, true, None);
    for index in 0..MAX_ATTEMPTS {
        let attempt = fixture.matched(execution);
        let (payload, raw) = result(&format!("memory report {index}"), 137, 8);
        if index + 1 == MAX_ATTEMPTS {
            fixture.completion(
                attempt,
                payload,
                raw,
                false,
                Some(ResourceFailureReason::AttemptBudgetExhausted),
            );
        } else {
            fixture.memory_retry(attempt, payload);
            assert!(fixture.expected.associations.is_empty());
            assert!(
                fixture
                    .actual
                    .history_for_operation(OperationId(operation))
                    .is_none()
            );
            fixture = fixture.recovered();
        }
    }
    assert_eq!(fixture.expected.associations[&execution].effect, 4);
    assert_eq!(
        fixture.expected.associations[&execution].original.attempt,
        3
    );
    assert!(matches!(
        fixture.operations[&operation].terminal,
        Some(TerminalResult::ResourceFailed {
            reason: ResourceFailureReason::AttemptBudgetExhausted,
            ..
        })
    ));
    fixture.stored(execution);
    fixture.recovered();
}

#[test]
fn real_state_shared_operation_cancellation_and_later_replacement_preserve_acceptance() {
    let mut fixture = Fixture::new(13, 5, 1);
    fixture.activate();
    let (execution, canceled) = fixture.submit(5, history::Algorithm::Sha256, false, true, None);
    let (_, interested) =
        fixture.submit(5, history::Algorithm::Sha256, false, true, Some(execution));
    let attempt = fixture.matched(execution);
    fixture.cancel_shared_operation(canceled);
    let (payload, raw) = result("accepted for remaining operation", 0, 0);
    fixture.completion(attempt, payload, raw, true, None);
    assert!(
        fixture
            .actual
            .history_for_operation(OperationId(canceled))
            .is_none()
    );
    assert!(
        fixture
            .actual
            .history_for_operation(OperationId(interested))
            .is_some()
    );
    let accepted = fixture.expected.associations[&execution].clone();
    fixture.register(2);
    fixture = fixture.recovered();
    assert_eq!(fixture.expected.associations[&execution], accepted);
    fixture.stored(execution);
    assert_eq!(
        fixture.expected.associations[&execution]
            .original
            .incarnation,
        1
    );
}

#[test]
fn real_state_replacement_before_acceptance_keeps_stale_bytes_out_of_history() {
    let mut fixture = Fixture::new(14, 6, 1);
    fixture.activate();
    let (execution, _) = fixture.submit(6, history::Algorithm::Blake3, true, true, None);
    let old_attempt = fixture.matched(execution);
    fixture.register(2);
    let (payload, raw) = result("revoked worker response", 0, 0);
    fixture.stale_completion(old_attempt, payload, raw);
    assert!(fixture.expected.associations.is_empty());
    fixture = fixture.recovered();
    fixture.cleanup(old_attempt);
    let replacement_attempt = fixture.matched(execution);
    let (payload, raw) = result("accepted replacement response", 0, 0);
    fixture.completion(replacement_attempt, payload, raw, true, None);
    assert_eq!(fixture.expected.associations[&execution].effect, 4);
    assert_eq!(
        fixture.expected.associations[&execution]
            .original
            .incarnation,
        2
    );
    fixture.stored(execution);
}

#[test]
fn real_state_wrong_unknown_duplicate_and_retired_publication_observations() {
    let mut fixture = Fixture::new(15, 7, 1);
    fixture.activate();
    let (execution, operation) = fixture.submit(7, history::Algorithm::Sha256, true, true, None);
    let attempt = fixture.matched(execution);
    let (payload, raw) = result("stable terminal despite publication failures", 1, 0);
    fixture.completion(attempt, payload, raw, false, None);
    let row = fixture.expected.associations[&execution].clone();
    for outcome in [
        history::PublicationOutcome::Failed,
        history::PublicationOutcome::Unknown,
    ] {
        fixture.publication(
            row.effect,
            row.recipe.object.clone(),
            outcome,
            &Reply::Ignored,
        );
    }
    for corruption in 0..4 {
        let mut bad = row.recipe.object.clone();
        match corruption {
            0 => bad.namespace.0[0] ^= 1,
            1 => bad.algorithm = history::Algorithm::Blake3,
            2 => bad.hash[0] ^= 1,
            3 => bad.size_bytes += 1,
            _ => unreachable!(),
        }
        fixture.publication(
            row.effect,
            bad,
            history::PublicationOutcome::Stored,
            &Reply::Rejected(Rejection::Stale),
        );
    }
    fixture.publication(
        999,
        row.recipe.object.clone(),
        history::PublicationOutcome::Stored,
        &Reply::Ignored,
    );
    let before = fixture.actual.snapshot().unwrap();
    assert_eq!(
        fixture.actual.apply(RecordedEvent {
            epoch: ClusterEpoch(16),
            event: Event::HistoryPublication {
                effect: EffectId(row.effect),
                object: object(&row.recipe.object),
                outcome: HistoryOutcome::Stored
            }
        }),
        Reply::Rejected(Rejection::WrongEpoch)
    );
    assert_eq!(fixture.actual.snapshot().unwrap(), before);
    fixture.retirement_time();
    fixture.retire(execution, false);
    fixture = fixture.recovered();
    fixture.stored(execution);
    fixture.publication(
        row.effect,
        row.recipe.object.clone(),
        history::PublicationOutcome::Stored,
        &Reply::Ignored,
    );
    fixture.retire(execution, true);
    fixture.publication(
        row.effect,
        row.recipe.object,
        history::PublicationOutcome::Stored,
        &Reply::Ignored,
    );
    assert!(
        fixture
            .actual
            .history_for_operation(OperationId(operation))
            .is_none()
    );
    assert!(
        fixture
            .actual
            .history_object_bytes(EffectId(row.effect))
            .is_err()
    );
}

#[test]
fn real_state_receipt_history_and_ac_share_allocator_and_independent_pins() {
    let mut fixture = Fixture::new(16, 8, 1);
    fixture.enable_receipts();
    fixture.activate();
    let (execution, _) = fixture.submit(8, history::Algorithm::Sha256, false, true, None);
    let attempt = fixture.matched(execution);
    fixture.issue_receipts();
    assert_eq!(
        fixture.pending_receipt.as_ref().unwrap().effect,
        EffectId(2)
    );
    let certificate = fixture.pending_receipt.clone();
    let (payload, raw) = result("AC before history after receipt cut", 0, 0);
    fixture.completion(attempt, payload, raw, true, None);
    assert!(matches!(
        fixture.ordinary_effects.get(&3),
        Some(EffectKind::PublishResult { .. })
    ));
    assert_eq!(fixture.expected.associations[&execution].effect, 4);
    assert_eq!(fixture.pending_receipt, certificate);
    fixture = fixture.recovered();
    fixture.retirement_time();
    fixture.retire(execution, false);
    fixture.stored(execution);
    fixture.settle_ac(execution);
    fixture.retire(execution, false);
    fixture.acknowledge_receipts();
    fixture.retire(execution, true);
    fixture.recovered();
}

#[test]
fn real_state_snapshot_and_suffix_keep_all_history_and_lifecycle_bytes_equal() {
    let mut uninterrupted = Fixture::new(17, 9, 1);
    uninterrupted.enable_receipts();
    uninterrupted.activate();
    let (first, _) = uninterrupted.submit(9, history::Algorithm::Sha256, true, true, None);
    let attempt = uninterrupted.matched(first);
    let (payload, raw) = result("first committed terminal", 0, 0);
    uninterrupted.completion(attempt, payload, raw, true, None);
    let (second, _) = uninterrupted.submit(10, history::Algorithm::Blake3, true, true, None);
    let attempt = uninterrupted.matched(second);
    uninterrupted.issue_receipts();
    let mut recovered = uninterrupted.recovered();
    for fixture in [&mut uninterrupted, &mut recovered] {
        let (payload, raw) = result("second committed terminal", 1, 0);
        fixture.completion(attempt, payload, raw, false, None);
        fixture.stored(first);
        fixture.stored(second);
        fixture.acknowledge_receipts();
        fixture.retirement_time();
        fixture.retire(first, true);
        fixture.retire(second, true);
        let (third, _) = fixture.submit(9, history::Algorithm::Sha256, true, true, None);
        let attempt = fixture.matched(third);
        let (payload, raw) = result("new acceptance after retirement", 0, 0);
        fixture.completion(attempt, payload, raw, true, None);
    }
    assert_eq!(uninterrupted.expected, recovered.expected);
    assert_eq!(uninterrupted.facts, recovered.facts);
    assert_eq!(uninterrupted.actual.view(), recovered.actual.view());
    assert_eq!(
        uninterrupted.actual.snapshot().unwrap(),
        recovered.actual.snapshot().unwrap()
    );
}

#[derive(Clone)]
struct Attempt {
    execution: u64,
    binding: history::Binding,
    reservation: u64,
    dispatch: u64,
    cancel: Option<u64>,
    charged: Resources,
    released: bool,
    authorized: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Operation {
    execution: u64,
    interested: bool,
    terminal: Option<TerminalResult>,
}

struct Fixture {
    actual: State,
    expected: Model,
    facts: history::Facts,
    preactivation: BTreeSet<u64>,
    executions: BTreeMap<u64, Execution>,
    operations: BTreeMap<u64, Operation>,
    attempts: BTreeMap<u64, Attempt>,
    ordinary_effects: BTreeMap<u64, EffectKind>,
    next_execution: u64,
    next_operation: u64,
    next_attempt: u64,
    next_reservation: u64,
    worker: u64,
    incarnation: u64,
    session: u64,
    now: u64,
    receipts: bool,
    pending_receipt: Option<ReceiptCertificate>,
    acknowledged_receipt: Option<EffectId>,
}

fn project(row: &HistoryAssociation) -> history::Association {
    history::Association {
        epoch: row.epoch.0,
        effect: row.effect.0,
        execution: row.execution.0,
        original: history::Binding {
            attempt: row.attempt.0,
            worker: row.worker.0,
            incarnation: row.incarnation.0,
        },
        recipe: history::Recipe {
            prefix: row.recipe.prefix.clone(),
            response_sha256: row.recipe.response_sha256,
            object: history::ObjectKey {
                namespace: history::Namespace(row.recipe.object.namespace),
                algorithm: match row.recipe.object.algorithm {
                    HistoryAlgorithm::Sha256 => history::Algorithm::Sha256,
                    HistoryAlgorithm::Blake3 => history::Algorithm::Blake3,
                },
                hash: row.recipe.object.hash,
                size_bytes: row.recipe.object.size_bytes,
            },
        },
        state: match row.status {
            HistoryStatus::Pending => history::PublicationState::Pending,
            HistoryStatus::Materialized => history::PublicationState::Materialized,
        },
    }
}

const fn object(value: &history::ObjectKey) -> HistoryObject {
    HistoryObject {
        namespace: value.namespace.0,
        algorithm: match value.algorithm {
            history::Algorithm::Sha256 => HistoryAlgorithm::Sha256,
            history::Algorithm::Blake3 => HistoryAlgorithm::Blake3,
        },
        hash: value.hash,
        size_bytes: value.size_bytes,
    }
}

fn result(message: &str, exit_code: i32, status: i32) -> (ResultPayload, Arc<[u8]>) {
    let response = ExecuteResponse {
        result: Some(ActionResult {
            exit_code,
            stdout_raw: format!("output:{message}").into_bytes().into(),
            ..ActionResult::default()
        }),
        status: Some(Status {
            code: status,
            message: if status == 0 {
                String::new()
            } else {
                message.into()
            },
            details: Vec::new(),
        }),
        message: message.into(),
        ..ExecuteResponse::default()
    };
    // Input facts are frozen before invoking the candidate's capture API.
    let raw = Arc::from(response.encode_to_vec());
    let candidate = ResultPayload::from_response(&response, 64 * 1024).unwrap();
    (candidate, raw)
}

impl Fixture {
    fn new(epoch: u64, worker: u64, incarnation: u64) -> Self {
        let mut fixture = Self {
            actual: State::new(ClusterEpoch(epoch), Limits::default()).unwrap(),
            expected: Model::default(),
            facts: history::Facts {
                epoch,
                qualified_namespace: Some(NAMESPACE),
                executions: BTreeMap::new(),
                next_effect: 1,
                other_future_effect_ids: 0,
                effect_capacity: 512,
                other_effect_slots: 0,
                metadata_capacity: MAX_STATE_BYTES,
                // Conservative fixture allowance for the ordinary lifecycle.
                // This adapter does not qualify the candidate byte proof.
                other_metadata_bytes: 8 * 1024 * 1024,
            },
            preactivation: BTreeSet::new(),
            executions: BTreeMap::new(),
            operations: BTreeMap::new(),
            attempts: BTreeMap::new(),
            ordinary_effects: BTreeMap::new(),
            next_execution: 1,
            next_operation: 1,
            next_attempt: 1,
            next_reservation: 1,
            worker,
            incarnation,
            session: 0,
            now: 0,
            receipts: false,
            pending_receipt: None,
            acknowledged_receipt: None,
        };
        fixture.register(incarnation);
        fixture
    }

    fn send(&mut self, event: Event, expected: &Reply) {
        let recorded = RecordedEvent {
            epoch: ClusterEpoch(self.facts.epoch),
            event,
        };
        let actual = self.actual.apply(recorded.clone());
        if std::env::var_os("NL_HISTORY_TRACE").is_some() {
            eprintln!(
                "history differential: {} => {}",
                serde_json::to_string(&recorded).unwrap(),
                serde_json::to_string(&actual).unwrap()
            );
        }
        assert_eq!(&actual, expected, "event: {recorded:?}");
    }

    fn allocate_effect(&mut self, kind: EffectKind) -> u64 {
        let effect = self.facts.next_effect;
        self.facts.next_effect += 1;
        assert!(self.ordinary_effects.insert(effect, kind).is_none());
        effect
    }

    fn refresh_credit(&mut self) {
        self.facts.other_future_effect_ids = self
            .executions
            .values()
            .filter(|row| row.terminal.is_none())
            .map(|row| {
                u64::from(MAX_ATTEMPTS - row.attempts_started) * 2
                    + u64::from(row.current.is_some())
                    + u64::from(!row.request.do_not_cache)
            })
            .sum();
        self.facts.other_effect_slots = self.ordinary_effects.len()
            + usize::try_from(self.facts.other_future_effect_ids).unwrap()
            + usize::from(self.pending_receipt.is_some());
    }

    fn activate(&mut self) {
        self.refresh_credit();
        self.preactivation = self
            .executions
            .iter()
            .filter_map(|(id, row)| row.terminal.as_ref().map(|_| *id))
            .collect();
        assert_eq!(
            self.expected.apply(
                &history::Event::Activate {
                    namespace: NAMESPACE
                },
                &self.facts
            ),
            Ok(history::Outcome::Applied)
        );
        self.actual.enable_accepted_history(NAMESPACE.0).unwrap();
        self.check();
    }

    fn register(&mut self, incarnation: u64) {
        self.session += 1;
        if incarnation != self.incarnation {
            let revoked: Vec<_> = self
                .attempts
                .iter()
                .filter(|(_, row)| row.authorized)
                .map(|(id, _)| *id)
                .collect();
            for attempt in revoked {
                let row = self.attempts[&attempt].clone();
                let cancel = self.allocate_effect(EffectKind::Cancel {
                    execution: ExecutionId(row.execution),
                    attempt: AttemptId(attempt),
                    worker: WorkerId(self.worker),
                    incarnation: IncarnationId(row.binding.incarnation),
                });
                let row = self.attempts.get_mut(&attempt).unwrap();
                row.authorized = false;
                row.cancel = Some(cancel);
                self.executions.get_mut(&row.execution).unwrap().current = None;
                self.facts
                    .executions
                    .get_mut(&row.execution)
                    .unwrap()
                    .authorized = None;
            }
            self.pending_receipt = None;
            self.acknowledged_receipt = None;
        }
        self.incarnation = incarnation;
        self.send(
            Event::Register {
                worker: WorkerId(self.worker),
                incarnation: IncarnationId(incarnation),
                session: SessionId(self.session),
                properties: BTreeMap::new(),
                capacity: CAPACITY,
                available: CAPACITY,
            },
            &Reply::Applied,
        );
        self.refresh_credit();
        self.check();
    }

    fn submit(
        &mut self,
        key: u8,
        algorithm: history::Algorithm,
        do_not_cache: bool,
        skip_lookup: bool,
        join: Option<u64>,
    ) -> (u64, u64) {
        let operation = self.next_operation;
        self.next_operation += 1;
        let execution = join.unwrap_or(self.next_execution);
        let request = Request {
            action: ActionKey {
                instance: "history-independent".into(),
                digest_function: match algorithm {
                    history::Algorithm::Sha256 => "sha256",
                    history::Algorithm::Blake3 => "blake3",
                }
                .into(),
                digest: format!("{key:02x}").repeat(32),
                digest_size_bytes: u64::from(key),
            },
            execution_scope: None,
            do_not_cache,
            skip_cache_lookup: skip_lookup,
            priority: 0,
            properties: BTreeMap::new(),
            resources: GRANT,
            execution_timeout: 100,
            payload: None,
        };
        if join.is_none() {
            self.next_execution += 1;
            self.executions.insert(
                execution,
                Execution {
                    request: request.clone(),
                    current: None,
                    attempts_started: 0,
                    target: GRANT,
                    terminal: None,
                },
            );
            self.facts.executions.insert(
                execution,
                history::ExecutionFacts {
                    action: history::ActionDigest {
                        algorithm,
                        hash: [key; 32],
                        size_bytes: u64::from(key),
                    },
                    phase: history::Phase::Open,
                    authorized: None,
                    accepted_operations: BTreeSet::new(),
                    retirement_permitted: false,
                },
            );
            if !skip_lookup {
                self.allocate_effect(EffectKind::Lookup {
                    execution: ExecutionId(execution),
                    action: request.action.clone(),
                });
            }
            self.refresh_credit();
            if self.expected.namespace.is_some() {
                assert_eq!(
                    self.expected
                        .apply(&history::Event::Admit { execution }, &self.facts),
                    Ok(history::Outcome::Applied)
                );
            }
        }
        self.operations.insert(
            operation,
            Operation {
                execution,
                interested: true,
                terminal: None,
            },
        );
        self.send(
            Event::Submit {
                operation: OperationId(operation),
                request,
            },
            &Reply::Submitted {
                operation: OperationId(operation),
                execution: ExecutionId(execution),
            },
        );
        self.check();
        (execution, operation)
    }

    fn matched(&mut self, execution: u64) -> u64 {
        let attempt = self.next_attempt;
        self.next_attempt += 1;
        let reservation = self.next_reservation;
        self.next_reservation += 1;
        let row = &self.executions[&execution];
        let charged = row.target;
        let dispatch = self.allocate_effect(EffectKind::Dispatch {
            execution: ExecutionId(execution),
            attempt: AttemptId(attempt),
            worker: WorkerId(self.worker),
            incarnation: IncarnationId(self.incarnation),
            reservation: ReservationId(reservation),
            charged,
            request: row.request.clone(),
        });
        let binding = history::Binding {
            attempt,
            worker: self.worker,
            incarnation: self.incarnation,
        };
        self.attempts.insert(
            attempt,
            Attempt {
                execution,
                binding,
                reservation,
                dispatch,
                cancel: None,
                charged,
                released: false,
                authorized: true,
            },
        );
        let row = self.executions.get_mut(&execution).unwrap();
        row.current = Some(attempt);
        row.attempts_started += 1;
        self.facts
            .executions
            .get_mut(&execution)
            .unwrap()
            .authorized = Some(binding);
        self.send(
            Event::Match,
            &Reply::Matched {
                execution: ExecutionId(execution),
                attempt: AttemptId(attempt),
                worker: WorkerId(self.worker),
            },
        );
        self.refresh_credit();
        self.check();
        self.ordinary_effects.remove(&dispatch);
        self.send(
            Event::WorkerAccepted {
                attempt: AttemptId(attempt),
                worker: WorkerId(self.worker),
                incarnation: IncarnationId(self.incarnation),
            },
            &Reply::Applied,
        );
        self.refresh_credit();
        self.check();
        attempt
    }

    fn terminal(&mut self, execution: u64, result: TerminalResult) {
        self.executions.get_mut(&execution).unwrap().current = None;
        let mut accepted = BTreeSet::new();
        for (id, operation) in self
            .operations
            .iter_mut()
            .filter(|(_, op)| op.execution == execution && op.interested)
        {
            accepted.insert(*id);
            operation.interested = false;
            operation.terminal = Some(result.clone());
        }
        self.executions.get_mut(&execution).unwrap().terminal = Some(result);
        self.facts
            .executions
            .get_mut(&execution)
            .unwrap()
            .accepted_operations = accepted;
    }

    fn completion(
        &mut self,
        attempt: u64,
        payload: ResultPayload,
        raw: Arc<[u8]>,
        cacheable_success: bool,
        failure: Option<ResourceFailureReason>,
    ) {
        let previous = self.attempts[&attempt].clone();
        let execution = previous.execution;
        let terminal = failure.map_or_else(
            || TerminalResult::Completed(payload.clone()),
            |reason| TerminalResult::ResourceFailed {
                result: payload.clone(),
                reason,
            },
        );
        self.terminal(execution, terminal);
        let fact = self.facts.executions.get_mut(&execution).unwrap();
        fact.phase = history::Phase::WorkerTerminal {
            original: previous.binding,
            response: raw,
            cacheable: !self.executions[&execution].request.do_not_cache,
            success: cacheable_success,
        };
        if cacheable_success && !self.executions[&execution].request.do_not_cache {
            self.allocate_effect(EffectKind::PublishResult {
                execution: ExecutionId(execution),
                action: self.executions[&execution].request.action.clone(),
                result: payload.clone(),
            });
        }
        let row = self.attempts.get_mut(&attempt).unwrap();
        row.released = true;
        row.authorized = false;
        self.refresh_credit();
        if self.expected.namespace.is_some() {
            let expected_effect = self.facts.next_effect;
            assert_eq!(
                self.expected.apply(
                    &history::Event::AcceptTerminal {
                        execution,
                        proposal: None
                    },
                    &self.facts
                ),
                Ok(history::Outcome::Allocated {
                    effect: expected_effect,
                    next_effect: expected_effect + 1
                })
            );
            self.facts.next_effect += 1;
        }
        let event = if failure.is_some() {
            Event::MemoryFailure {
                attempt: AttemptId(attempt),
                worker: WorkerId(previous.binding.worker),
                incarnation: IncarnationId(previous.binding.incarnation),
                charged: previous.charged,
                peak_memory_bytes: Some(previous.charged.memory_bytes),
                quiescent: true,
                result: payload,
            }
        } else {
            Event::Complete {
                attempt: AttemptId(attempt),
                worker: WorkerId(previous.binding.worker),
                incarnation: IncarnationId(previous.binding.incarnation),
                result: payload,
            }
        };
        self.send(event, &Reply::Applied);
        self.facts
            .executions
            .get_mut(&execution)
            .unwrap()
            .authorized = None;
        self.refresh_credit();
        self.check();
    }

    fn memory_retry(&mut self, attempt: u64, payload: ResultPayload) {
        let previous = self.attempts[&attempt].clone();
        let row = self.attempts.get_mut(&attempt).unwrap();
        row.released = true;
        row.authorized = false;
        let row = self.executions.get_mut(&previous.execution).unwrap();
        row.current = None;
        row.target.memory_bytes *= 2;
        self.facts
            .executions
            .get_mut(&previous.execution)
            .unwrap()
            .authorized = None;
        self.refresh_credit();
        assert_eq!(
            self.expected.apply(
                &history::Event::AcceptTerminal {
                    execution: previous.execution,
                    proposal: None
                },
                &self.facts
            ),
            Ok(history::Outcome::Ignored)
        );
        self.send(
            Event::MemoryFailure {
                attempt: AttemptId(attempt),
                worker: WorkerId(previous.binding.worker),
                incarnation: IncarnationId(previous.binding.incarnation),
                charged: previous.charged,
                peak_memory_bytes: Some(previous.charged.memory_bytes),
                quiescent: true,
                result: payload,
            },
            &Reply::Applied,
        );
        self.check();
    }

    fn duplicate_completion(&mut self, attempt: u64, payload: ResultPayload) {
        let original = self.attempts[&attempt].clone();
        let before = self.actual.snapshot().unwrap();
        assert_eq!(
            self.expected.apply(
                &history::Event::AcceptTerminal {
                    execution: original.execution,
                    proposal: None
                },
                &self.facts,
            ),
            Ok(history::Outcome::Ignored)
        );
        self.send(
            Event::Complete {
                attempt: AttemptId(attempt),
                worker: WorkerId(original.binding.worker),
                incarnation: IncarnationId(original.binding.incarnation),
                result: payload,
            },
            &Reply::Rejected(Rejection::Stale),
        );
        assert_eq!(self.actual.snapshot().unwrap(), before);
        self.check();
    }

    fn stale_completion(&mut self, attempt: u64, payload: ResultPayload, raw: Arc<[u8]>) {
        let previous = self.attempts[&attempt].clone();
        let execution = previous.execution;
        let before = self.actual.view();
        let old_phase = self.facts.executions[&execution].phase.clone();
        self.facts.executions.get_mut(&execution).unwrap().phase = history::Phase::WorkerTerminal {
            original: previous.binding,
            response: raw,
            cacheable: false,
            success: true,
        };
        assert_eq!(
            self.expected.apply(
                &history::Event::AcceptTerminal {
                    execution,
                    proposal: None
                },
                &self.facts
            ),
            Ok(history::Outcome::Ignored)
        );
        self.facts.executions.get_mut(&execution).unwrap().phase = old_phase;
        self.send(
            Event::Complete {
                attempt: AttemptId(attempt),
                worker: WorkerId(previous.binding.worker),
                incarnation: IncarnationId(previous.binding.incarnation),
                result: payload,
            },
            &Reply::Rejected(Rejection::Stale),
        );
        assert_eq!(self.actual.view(), before);
        self.check();
    }

    fn cancel_shared_operation(&mut self, operation: u64) {
        let execution = self.operations[&operation].execution;
        assert!(
            self.operations
                .values()
                .filter(|row| row.execution == execution && row.interested)
                .count()
                > 1
        );
        let row = self.operations.get_mut(&operation).unwrap();
        row.interested = false;
        row.terminal = Some(TerminalResult::Cancelled);
        self.send(
            Event::Cancel {
                operation: OperationId(operation),
            },
            &Reply::Applied,
        );
        self.check();
    }

    fn cleanup(&mut self, attempt: u64) {
        let previous = self.attempts[&attempt].clone();
        if let Some(cancel) = previous.cancel {
            self.ordinary_effects.remove(&cancel);
        }
        self.attempts.get_mut(&attempt).unwrap().released = true;
        self.send(
            Event::Cleanup {
                attempt: AttemptId(attempt),
                worker: WorkerId(previous.binding.worker),
                incarnation: IncarnationId(previous.binding.incarnation),
            },
            &Reply::Applied,
        );
        self.refresh_credit();
        self.check();
    }

    fn cache_hit(&mut self, execution: u64, payload: ResultPayload) {
        let effect = *self
            .ordinary_effects
            .iter()
            .find_map(|(id, kind)| {
                matches!(kind, EffectKind::Lookup { execution: id, .. } if id.0 == execution)
                    .then_some(id)
            })
            .unwrap();
        self.ordinary_effects.remove(&effect);
        self.terminal(execution, TerminalResult::Completed(payload.clone()));
        self.facts.executions.get_mut(&execution).unwrap().phase = history::Phase::OtherTerminal;
        self.refresh_credit();
        assert_eq!(
            self.expected
                .apply(&history::Event::ReleaseUnused { execution }, &self.facts),
            Ok(history::Outcome::Applied)
        );
        self.send(
            Event::LookupResult {
                effect: EffectId(effect),
                result: Some(payload),
            },
            &Reply::Applied,
        );
        self.check();
    }

    fn publication(
        &mut self,
        effect: u64,
        key: history::ObjectKey,
        outcome: history::PublicationOutcome,
        expected_reply: &Reply,
    ) {
        let before = self.actual.view();
        let candidate_object = object(&key);
        let expected_outcome = if *expected_reply == Reply::Applied {
            history::Outcome::Applied
        } else {
            history::Outcome::Ignored
        };
        assert_eq!(
            self.expected.apply(
                &history::Event::ObservePublication {
                    epoch: self.facts.epoch,
                    effect,
                    object: key,
                    outcome
                },
                &self.facts
            ),
            Ok(expected_outcome)
        );
        self.send(
            Event::HistoryPublication {
                effect: EffectId(effect),
                object: candidate_object,
                outcome: match outcome {
                    history::PublicationOutcome::Stored => HistoryOutcome::Stored,
                    history::PublicationOutcome::Failed => HistoryOutcome::Failed,
                    history::PublicationOutcome::Unknown => HistoryOutcome::Unknown,
                },
            },
            expected_reply,
        );
        assert_eq!(
            self.actual.view(),
            before,
            "publication changed ordinary lifecycle facts"
        );
        self.refresh_credit();
        self.check();
    }

    fn stored(&mut self, execution: u64) {
        let row = self.expected.associations[&execution].clone();
        self.publication(
            row.effect,
            row.recipe.object,
            history::PublicationOutcome::Stored,
            &Reply::Applied,
        );
    }

    fn settle_ac(&mut self, execution: u64) {
        let ids: Vec<_> = self.ordinary_effects.iter().filter_map(|(id, kind)| matches!(kind, EffectKind::PublishResult { execution: value, .. } if value.0 == execution).then_some(*id)).collect();
        for effect in ids {
            self.ordinary_effects.remove(&effect);
            self.send(
                Event::EffectOutcome {
                    effect: EffectId(effect),
                    success: true,
                },
                &Reply::Applied,
            );
        }
        self.refresh_credit();
        self.check();
    }

    fn retirement_time(&mut self) {
        self.now += TERMINAL_RETENTION + 1;
        self.send(Event::AdvanceTime { now: self.now }, &Reply::Applied);
        self.send(
            Event::Heartbeat {
                worker: WorkerId(self.worker),
                incarnation: IncarnationId(self.incarnation),
                session: SessionId(self.session),
            },
            &Reply::Applied,
        );
        self.check();
    }

    fn retire(&mut self, execution: u64, allowed: bool) {
        let external_pins = self.pending_receipt.as_ref().is_some_and(|certificate| {
            certificate
                .retain
                .iter()
                .any(|binding| binding.execution.0 == execution)
        });
        let unsettled = self.ordinary_effects.values().any(|kind| match kind {
            EffectKind::Lookup { execution: id, .. }
            | EffectKind::Dispatch { execution: id, .. }
            | EffectKind::Cancel { execution: id, .. }
            | EffectKind::PublishResult { execution: id, .. } => id.0 == execution,
        });
        let eligible = !external_pins
            && !unsettled
            && self
                .attempts
                .values()
                .filter(|row| row.execution == execution)
                .all(|row| row.released)
            && self.executions[&execution].terminal.is_some();
        self.facts
            .executions
            .get_mut(&execution)
            .unwrap()
            .retirement_permitted = eligible;
        let expected = if allowed {
            Ok(history::Outcome::Applied)
        } else {
            Err(history::Reject::Busy)
        };
        // Preactivation and cache-hit executions have no component row to remove.
        let expected = if allowed && !self.expected.associations.contains_key(&execution) {
            Ok(history::Outcome::Ignored)
        } else {
            expected
        };
        assert_eq!(
            self.expected
                .apply(&history::Event::Retire { execution }, &self.facts),
            expected
        );
        self.send(
            Event::RetireExecution {
                execution: ExecutionId(execution),
            },
            &if allowed {
                Reply::Applied
            } else {
                Reply::Rejected(Rejection::Busy)
            },
        );
        if allowed {
            self.preactivation.remove(&execution);
            self.executions.remove(&execution);
            self.facts.executions.remove(&execution);
            self.operations.retain(|_, row| row.execution != execution);
            self.attempts.retain(|_, row| row.execution != execution);
        }
        self.refresh_credit();
        self.check();
    }

    fn enable_receipts(&mut self) {
        self.actual.enable_receipt_retirement().unwrap();
        self.receipts = true;
        self.check();
    }

    fn issue_receipts(&mut self) {
        assert!(self.receipts && self.pending_receipt.is_none());
        let effect = self.facts.next_effect;
        self.facts.next_effect += 1;
        let retain = self
            .attempts
            .iter()
            .filter(|(_, row)| row.binding.incarnation == self.incarnation && !row.released)
            .map(|(id, row)| ReceiptBinding {
                attempt: AttemptId(*id),
                execution: ExecutionId(row.execution),
                dispatch_effect: EffectId(row.dispatch),
                reservation: ReservationId(row.reservation),
                cancel_effect: row.cancel.map(EffectId),
            })
            .collect();
        self.pending_receipt = Some(ReceiptCertificate {
            effect: EffectId(effect),
            attempts_through: AttemptId(self.next_attempt - 1),
            effects_through: EffectId(effect),
            reservations_through: ReservationId(self.next_reservation - 1),
            retain,
        });
        self.send(
            Event::IssueReceipts {
                worker: WorkerId(self.worker),
                incarnation: IncarnationId(self.incarnation),
                after_ack: self.acknowledged_receipt,
            },
            &Reply::Applied,
        );
        self.refresh_credit();
        self.check();
    }

    fn acknowledge_receipts(&mut self) {
        let certificate = self.pending_receipt.take().unwrap();
        self.acknowledged_receipt = Some(certificate.effect);
        let before = self.actual.view();
        self.send(
            Event::AcknowledgeReceipts {
                worker: WorkerId(self.worker),
                incarnation: IncarnationId(self.incarnation),
                certificate,
            },
            &Reply::Applied,
        );
        assert_eq!(self.actual.view(), before);
        self.refresh_credit();
        self.check();
    }

    fn recovered(&self) -> Self {
        let bytes = self.actual.snapshot().unwrap();
        let result = Self {
            actual: State::restore(&bytes).unwrap(),
            expected: Model::restore(&self.expected.snapshot(), &self.facts).unwrap(),
            facts: self.facts.clone(),
            preactivation: self.preactivation.clone(),
            executions: self.executions.clone(),
            operations: self.operations.clone(),
            attempts: self.attempts.clone(),
            ordinary_effects: self.ordinary_effects.clone(),
            next_execution: self.next_execution,
            next_operation: self.next_operation,
            next_attempt: self.next_attempt,
            next_reservation: self.next_reservation,
            worker: self.worker,
            incarnation: self.incarnation,
            session: self.session,
            now: self.now,
            receipts: self.receipts,
            pending_receipt: self.pending_receipt.clone(),
            acknowledged_receipt: self.acknowledged_receipt,
        };
        assert_eq!(result.actual.snapshot().unwrap(), bytes);
        result.check();
        result
    }

    fn check(&self) {
        match self.actual.accepted_history() {
            None => assert!(self.expected.namespace.is_none()),
            Some(actual) => {
                assert_eq!(
                    self.expected.namespace,
                    Some(history::Namespace(actual.namespace))
                );
                assert_eq!(
                    actual
                        .preactivation_terminals
                        .iter()
                        .map(|id| id.0)
                        .collect::<BTreeSet<_>>(),
                    self.preactivation
                );
                let projected: BTreeMap<_, _> = actual
                    .associations
                    .iter()
                    .map(|(id, row)| (id.0, project(row)))
                    .collect();
                assert_eq!(projected, self.expected.associations);
                let pins = actual
                    .associations
                    .values()
                    .filter(|row| row.status == HistoryStatus::Pending)
                    .map(|row| (row.execution.0, row.attempt.0))
                    .collect();
                assert_eq!(self.expected.pins(), pins);
            }
        }
        for (execution, row) in &self.expected.associations {
            let history::Phase::WorkerTerminal { response, .. } =
                &self.facts.executions[execution].phase
            else {
                unreachable!()
            };
            let bytes = [row.recipe.prefix.as_slice(), response].concat();
            assert_eq!(
                self.actual
                    .history_object_bytes(EffectId(row.effect))
                    .unwrap(),
                bytes
            );
        }
        for operation in self.operations.keys() {
            assert_eq!(
                self.actual
                    .history_for_operation(OperationId(*operation))
                    .map(project),
                self.expected.operation(*operation, &self.facts).cloned()
            );
        }
        let view = self.actual.view();
        let effects: Vec<_> = self
            .ordinary_effects
            .iter()
            .map(|(id, kind)| Effect {
                id: EffectId(*id),
                epoch: ClusterEpoch(self.facts.epoch),
                kind: kind.clone(),
            })
            .collect();
        assert_eq!(view.effects, effects);
        let operations: BTreeMap<_, _> = view
            .operations
            .iter()
            .map(|row| {
                (
                    row.id.0,
                    Operation {
                        execution: row.execution.0,
                        interested: row.interested,
                        terminal: row.terminal.clone(),
                    },
                )
            })
            .collect();
        assert_eq!(operations, self.operations);
        assert_eq!(view.executions.len(), self.executions.len());
        for row in &view.executions {
            let expected = &self.executions[&row.id.0];
            assert_eq!(row.request, expected.request);
            assert_eq!(row.terminal, expected.terminal);
            assert_eq!(row.attempt.map(|id| id.0), expected.current);
            assert_eq!(row.attempts_started, expected.attempts_started);
            assert_eq!(row.reservation_target, expected.target);
        }
        assert_eq!(view.attempts.len(), self.attempts.len());
        assert_eq!(view.reservations.len(), self.attempts.len());
        for row in &view.attempts {
            let expected = &self.attempts[&row.id.0];
            assert_eq!(
                (
                    row.execution.0,
                    row.worker.0,
                    row.incarnation.0,
                    row.dispatch_effect.0,
                    row.reservation.0,
                    row.authorized
                ),
                (
                    expected.execution,
                    expected.binding.worker,
                    expected.binding.incarnation,
                    expected.dispatch,
                    expected.reservation,
                    expected.authorized
                )
            );
        }
        for row in &view.reservations {
            let expected = &self.attempts[&row.attempt.0];
            assert_eq!(
                (
                    row.id.0,
                    row.worker.0,
                    row.incarnation.0,
                    row.charged,
                    row.released
                ),
                (
                    expected.reservation,
                    expected.binding.worker,
                    expected.binding.incarnation,
                    expected.charged,
                    expected.released
                )
            );
        }
        let charged = self.attempts.values().filter(|row| !row.released).fold(
            Resources {
                cpu_millis: 0,
                memory_bytes: 0,
                slots: 0,
            },
            |sum, row| Resources {
                cpu_millis: sum.cpu_millis + row.charged.cpu_millis,
                memory_bytes: sum.memory_bytes + row.charged.memory_bytes,
                slots: sum.slots + row.charged.slots,
            },
        );
        assert_eq!(view.workers.len(), 1);
        let worker = &view.workers[0];
        assert_eq!(
            (
                worker.id.0,
                worker.incarnation.0,
                worker.session.0,
                worker.charged
            ),
            (self.worker, self.incarnation, self.session, charged)
        );
        if self.receipts {
            let receipts = self.actual.receipt_retirement().unwrap();
            let stream = &receipts.streams[&WorkerId(self.worker)];
            assert_eq!(stream.incarnation, IncarnationId(self.incarnation));
            assert_eq!(stream.pending, self.pending_receipt);
            assert_eq!(
                stream.acknowledged.map(|ack| ack.effect),
                self.acknowledged_receipt
            );
        }
    }
}
