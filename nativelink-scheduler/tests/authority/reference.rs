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

//! Slow relational oracle for the bounded authority contract.
//!
//! Only the input and observation vocabulary is shared with the implementation.
//! This model uses linear scans over relations, recomputes matching and charged
//! resources, and owns an unrelated snapshot encoding. It must never import the
//! production State, reducer, accounting, matcher, or snapshot implementation.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Digest, ExecuteRequest, ExecuteResponse, RequestMetadata,
};
use nativelink_scheduler::authority::{
    ACK_TIMEOUT, AttemptId, AttemptView, ClusterEpoch, Deadline, Effect, EffectId, EffectKind,
    Event, ExecutionId, ExecutionView, HOLD_AFTER, HOLD_COOLDOWN, HOLD_DURATION, HoldView,
    INTEREST_TIMEOUT, IncarnationId, Limits, MAX_ATTEMPTS, ObservableState, OperationId,
    OperationView, PROGRESS_TIMEOUT, Phase, RecordedEvent, Rejection, Reply, Request,
    ReservationId, ReservationView, ResourceFailureReason, Resources, ResultPayload, SessionId,
    TERMINAL_RETENTION, TerminalResult, WorkerId, WorkerView,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;
use serde::{Deserialize, Serialize};

pub(crate) const CREDIT_LIMIT: usize = 14 * 1024 * 1024;
const COMPLETE_PAYLOAD_LIMIT: usize = 512 * 1024;

/// Independent decoding of the agreed capture envelope, not its producer or
/// projection implementation. Raw protobuf values remain the source of facts.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapturedRequest {
    version: u32,
    execute: Vec<u8>,
    metadata: Option<Vec<u8>>,
    queued: Vec<u8>,
    principal: String,
    action: Vec<u8>,
    command: Vec<u8>,
    sha256: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Interest {
    operation: OperationId,
    execution: ExecutionId,
    urgency: i32,
    active: bool,
    result: Option<TerminalResult>,
    revision: u64,
    expires: Deadline,
    original: Request,
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    fn request(digest: &str) -> Request {
        Request {
            action: serde_json::from_value(serde_json::json!({
                "instance": "test", "digest_function": "sha256", "digest": digest,
                "digest_size_bytes": 1,
            }))
            .unwrap(),
            execution_scope: None,
            do_not_cache: false,
            skip_cache_lookup: true,
            priority: 0,
            properties: BTreeMap::new(),
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 4,
                slots: 1,
            },
            execution_timeout: 100,
            payload: None,
        }
    }

    fn apply(model: &mut Reference, event: Event) -> Reply {
        model.apply(RecordedEvent {
            epoch: ClusterEpoch(1),
            event,
        })
    }

    fn register(model: &mut Reference, incarnation: u64, session: u64) {
        let capacity = Resources {
            cpu_millis: 1,
            memory_bytes: 4,
            slots: 1,
        };
        assert_eq!(
            apply(
                model,
                Event::Register {
                    worker: WorkerId(1),
                    incarnation: IncarnationId(incarnation),
                    session: SessionId(session),
                    properties: BTreeMap::new(),
                    capacity,
                    available: capacity,
                }
            ),
            Reply::Applied
        );
    }

    fn matched(model: &mut Reference) -> AttemptId {
        match apply(model, Event::Match) {
            Reply::Matched { attempt, .. } => attempt,
            reply => panic!("expected placement, got {reply:?}"),
        }
    }

    #[test]
    fn replacement_requires_cleanup_and_retains_original_charge() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        register(&mut model, 1, 1);
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: request("a"),
            },
        );
        let old = matched(&mut model);
        register(&mut model, 2, 1);
        let view = model.view();
        assert!(view.workers[0].quarantined);
        assert_eq!(view.workers[0].charged.memory_bytes, 4);
        assert_eq!(apply(&mut model, Event::Match), Reply::Ignored);
        assert_eq!(
            apply(
                &mut model,
                Event::Cleanup {
                    attempt: old,
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1)
                }
            ),
            Reply::Applied
        );
        assert!(!model.view().workers[0].quarantined);
        let new = matched(&mut model);
        assert_ne!(new, old);
        let before = model.snapshot().unwrap();
        assert_eq!(
            apply(
                &mut model,
                Event::Complete {
                    attempt: old,
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                    result: ResultPayload {
                        response: None,
                        digest: "stale".to_owned(),
                        exit_code: 0,
                        error: None,
                    }
                }
            ),
            Reply::Rejected(Rejection::Stale)
        );
        assert_eq!(model.snapshot().unwrap(), before);
    }

    #[test]
    fn transport_reconnect_and_late_decline_do_not_create_a_second_slot() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        register(&mut model, 1, 1);
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: request("a"),
            },
        );
        let attempt = matched(&mut model);
        apply(
            &mut model,
            Event::WorkerAccepted {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
        );
        register(&mut model, 1, 2);
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(2),
                request: request("b"),
            },
        );
        let before = model.snapshot().unwrap();
        assert_eq!(
            apply(
                &mut model,
                Event::Decline {
                    attempt,
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1)
                }
            ),
            Reply::Rejected(Rejection::Stale)
        );
        assert_eq!(model.snapshot().unwrap(), before);
        assert_eq!(apply(&mut model, Event::Match), Reply::Ignored);
        assert_eq!(model.view().workers[0].charged.slots, 1);
    }

    #[test]
    fn priority_interest_and_invocation_identity_are_distinct_relations() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        let base = request("a");
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: base.clone(),
            },
        );
        let mut urgent = base.clone();
        urgent.priority = -20;
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(2),
                request: urgent,
            },
        );
        assert_eq!(model.view().executions.len(), 1);
        assert_eq!(model.view().executions[0].effective_priority, Some(-20));
        apply(
            &mut model,
            Event::Cancel {
                operation: OperationId(2),
            },
        );
        let view = model.view();
        assert_eq!(view.executions[0].effective_priority, Some(0));
        assert_eq!(view.executions[0].enqueue_ordinal, 1);
        assert_eq!(view.operations[0].version, 1);
        let mut scoped = base;
        scoped.execution_scope = Some("buck2-other-invocation".to_owned());
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(3),
                request: scoped,
            },
        );
        let view = model.view();
        assert_eq!(view.executions.len(), 2);
        assert_eq!(
            view.executions[0].request.action,
            view.executions[1].request.action
        );
    }

    #[test]
    fn snapshot_keeps_uncertain_effects_and_stale_timer_cannot_expire_progress() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        register(&mut model, 1, 1);
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: request("a"),
            },
        );
        let attempt = matched(&mut model);
        let bytes = model.snapshot().unwrap();
        let restored = Reference::restore(&bytes).unwrap();
        assert_eq!(model.view(), restored.view());
        assert_eq!(restored.view().effects.len(), 1);
        assert!(!restored.view().reservations[0].released);
        apply(
            &mut model,
            Event::WorkerAccepted {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
        );
        apply(&mut model, Event::AdvanceTime { now: 10 });
        apply(
            &mut model,
            Event::Progress {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                phase: Phase::Preparing,
                progress: 1,
            },
        );
        let before = model.snapshot().unwrap();
        assert_eq!(
            apply(
                &mut model,
                Event::ExpireAttempt {
                    attempt,
                    generation: 1
                }
            ),
            Reply::Ignored
        );
        assert_eq!(model.snapshot().unwrap(), before);
    }

    #[test]
    fn retired_handle_identity_survives_snapshot_and_invalid_ids_do_not_advance_it() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        register(&mut model, 1, 1);
        let mut original = request("first");
        original.do_not_cache = true;
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: original,
            },
        );
        let attempt = matched(&mut model);
        apply(
            &mut model,
            Event::Complete {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                result: ResultPayload {
                    response: None,
                    digest: "result".to_owned(),
                    exit_code: 0,
                    error: None,
                },
            },
        );
        apply(
            &mut model,
            Event::AdvanceTime {
                now: TERMINAL_RETENTION,
            },
        );
        assert_eq!(
            apply(
                &mut model,
                Event::RetireExecution {
                    execution: ExecutionId(1)
                }
            ),
            Reply::Applied
        );
        let bytes = model.snapshot().unwrap();
        let mut restored = Reference::restore(&bytes).unwrap();
        assert_eq!(
            apply(
                &mut restored,
                Event::Submit {
                    operation: OperationId(1),
                    request: request("second")
                }
            ),
            Reply::Rejected(Rejection::Stale)
        );
        let mut invalid = request("invalid");
        invalid.execution_timeout = 0;
        assert_eq!(
            apply(
                &mut restored,
                Event::Submit {
                    operation: OperationId(100),
                    request: invalid
                }
            ),
            Reply::Rejected(Rejection::Invalid)
        );
        assert_eq!(
            apply(
                &mut restored,
                Event::Submit {
                    operation: OperationId(2),
                    request: request("second")
                }
            ),
            Reply::Submitted {
                operation: OperationId(2),
                execution: ExecutionId(2)
            }
        );
    }

    #[test]
    fn one_live_capacity_observation_cannot_fund_two_dispatches() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        let first = request("one");
        assert_eq!(
            apply(
                &mut model,
                Event::Register {
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                    session: SessionId(1),
                    properties: BTreeMap::new(),
                    capacity: Resources {
                        cpu_millis: 4,
                        memory_bytes: 16,
                        slots: 4
                    },
                    available: first.resources,
                }
            ),
            Reply::Applied
        );
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: first,
            },
        );
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(2),
                request: request("two"),
            },
        );
        matched(&mut model);
        assert_eq!(apply(&mut model, Event::Match), Reply::Ignored);
        assert_eq!(model.view().attempts.len(), 1);
        assert_eq!(model.view().workers[0].charged.slots, 1);
    }

    #[test]
    fn absolute_execution_expiry_is_independent_of_renewed_progress() {
        let mut model = Reference::new(ClusterEpoch(1), Limits::default()).unwrap();
        register(&mut model, 1, 1);
        let mut req = request("absolute-deadline");
        req.execution_timeout = 5;
        apply(
            &mut model,
            Event::Submit {
                operation: OperationId(1),
                request: req,
            },
        );
        let attempt = matched(&mut model);
        assert_eq!(
            apply(&mut model, Event::ExpireExecution { attempt }),
            Reply::Ignored
        );
        apply(
            &mut model,
            Event::WorkerAccepted {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
        );
        apply(
            &mut model,
            Event::Progress {
                attempt,
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                phase: Phase::Running,
                progress: 1,
            },
        );
        assert_eq!(
            apply(&mut model, Event::ExpireExecution { attempt }),
            Reply::Rejected(Rejection::TooEarly)
        );
        for now in 1..=5 {
            let old_generation = model.view().attempts[0].deadline.generation;
            apply(&mut model, Event::AdvanceTime { now });
            apply(
                &mut model,
                Event::Progress {
                    attempt,
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                    phase: Phase::Running,
                    progress: now + 1,
                },
            );
            assert_eq!(
                apply(
                    &mut model,
                    Event::ExpireAttempt {
                        attempt,
                        generation: old_generation,
                    }
                ),
                Reply::Ignored
            );
        }
        model = Reference::restore(&model.snapshot().unwrap()).unwrap();
        assert_eq!(model.view().attempts[0].running_deadline, Some(5));
        assert!(model.view().attempts[0].deadline.at > 5);
        assert_eq!(
            apply(&mut model, Event::ExpireExecution { attempt }),
            Reply::Applied
        );
        assert!(!model.view().attempts[0].authorized);
        assert!(!model.view().reservations[0].released);
        assert_eq!(model.view().workers[0].charged.slots, 1);
        assert!(model.view().effects.iter().any(|effect| matches!(
            effect.kind,
            EffectKind::Cancel { attempt: target, .. } if target == attempt
        )));
        let unchanged = model.snapshot().unwrap();
        assert_eq!(
            apply(&mut model, Event::ExpireExecution { attempt }),
            Reply::Ignored
        );
        assert_eq!(model.snapshot().unwrap(), unchanged);
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Job {
    execution: ExecutionId,
    request: Request,
    target: Resources,
    phase: Phase,
    ordinal: u64,
    enqueue_at: u64,
    hold_cooldown_until: u64,
    current: Option<AttemptId>,
    attempts: u32,
    result: Option<TerminalResult>,
    finished_at: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Node {
    worker: WorkerId,
    incarnation: IncarnationId,
    session: SessionId,
    properties: BTreeMap<String, String>,
    capacity: Resources,
    reported_free: Resources,
    draining: bool,
    heartbeat_at: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Trial {
    attempt: AttemptId,
    dispatch_effect: EffectId,
    execution: ExecutionId,
    worker: WorkerId,
    incarnation: IncarnationId,
    reservation: ReservationId,
    valid: bool,
    phase: Phase,
    progress: u64,
    expires: Deadline,
    run_until: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Charge {
    reservation: ReservationId,
    attempt: AttemptId,
    worker: WorkerId,
    incarnation: IncarnationId,
    original: Resources,
    released: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    epoch: ClusterEpoch,
    clock: u64,
    limits: Limits,
    interests: Vec<Interest>,
    jobs: Vec<Job>,
    nodes: Vec<Node>,
    trials: Vec<Trial>,
    charges: Vec<Charge>,
    outbox: Vec<Effect>,
    holds: Vec<HoldView>,
    next_execution: u64,
    next_attempt: u64,
    next_reservation: u64,
    next_effect: u64,
    highest_admitted_operation: u64,
}

impl Reference {
    pub(crate) fn new(epoch: ClusterEpoch, limits: Limits) -> Result<Self, String> {
        if limits.max_payload_bytes > COMPLETE_PAYLOAD_LIMIT {
            return Err("independent reference payload bound exceeds commit envelope".into());
        }
        Ok(Self {
            epoch,
            clock: 0,
            limits,
            interests: Vec::new(),
            jobs: Vec::new(),
            nodes: Vec::new(),
            trials: Vec::new(),
            charges: Vec::new(),
            outbox: Vec::new(),
            holds: Vec::new(),
            next_execution: 1,
            next_attempt: 1,
            next_reservation: 1,
            next_effect: 1,
            highest_admitted_operation: 0,
        })
    }

    pub(crate) fn apply(&mut self, recorded: RecordedEvent) -> Reply {
        let reply = self.transition(recorded);
        if matches!(
            reply,
            Reply::Applied | Reply::Submitted { .. } | Reply::Matched { .. }
        ) {
            self.reconcile_holds();
        }
        reply
    }

    fn transition(&mut self, recorded: RecordedEvent) -> Reply {
        if recorded.epoch != self.epoch {
            return Reply::Rejected(Rejection::WrongEpoch);
        }
        match recorded.event {
            Event::Register {
                worker,
                incarnation,
                session,
                properties,
                capacity,
                available,
            } => self.register(
                worker,
                incarnation,
                session,
                properties,
                capacity,
                available,
            ),
            Event::Capacity {
                worker,
                incarnation,
                session,
                capacity,
                available,
                draining,
            } => {
                let node = match self.session_node(worker, incarnation, session) {
                    Ok(node) => node,
                    Err(reason) => return Reply::Rejected(reason),
                };
                self.nodes[node].capacity = capacity;
                self.nodes[node].reported_free = available;
                self.nodes[node].draining = draining;
                Reply::Applied
            }
            Event::Heartbeat {
                worker,
                incarnation,
                session,
            } => {
                let node = match self.session_node(worker, incarnation, session) {
                    Ok(node) => node,
                    Err(reason) => return Reply::Rejected(reason),
                };
                self.nodes[node].heartbeat_at = self.clock;
                Reply::Applied
            }
            Event::WorkerUnavailable {
                worker,
                incarnation,
                session,
            } => {
                let node = match self.session_node(worker, incarnation, session) {
                    Ok(node) => node,
                    Err(reason) => return Reply::Rejected(reason),
                };
                let current: Vec<_> = self
                    .trials
                    .iter()
                    .enumerate()
                    .filter(|(_, trial)| {
                        trial.worker == worker && trial.incarnation == incarnation && trial.valid
                    })
                    .map(|(index, _)| index)
                    .collect();
                if self.nodes[node].draining && current.is_empty() {
                    return Reply::Ignored;
                }
                self.nodes[node].draining = true;
                for trial in current {
                    self.revoke(trial, false);
                }
                Reply::Applied
            }
            Event::Submit { operation, request } => self.submit(operation, request),
            Event::LookupResult { effect, result } => self.lookup(effect, result),
            Event::Match => self.assign(),
            Event::WorkerAccepted {
                attempt,
                worker,
                incarnation,
            } => {
                let trial = match self.current_trial(attempt, worker, incarnation) {
                    Ok(trial) => trial,
                    Err(reason) => return Reply::Rejected(reason),
                };
                if self.trials[trial].phase != Phase::Dispatching {
                    return Reply::Ignored;
                }
                self.trials[trial].phase = Phase::Accepted;
                self.trials[trial].expires = Deadline {
                    at: self.clock.saturating_add(PROGRESS_TIMEOUT),
                    generation: self.trials[trial].expires.generation + 1,
                };
                self.remove_dispatch(attempt);
                self.change_phase(self.trials[trial].execution, Phase::Accepted);
                Reply::Applied
            }
            Event::Progress {
                attempt,
                worker,
                incarnation,
                phase,
                progress,
            } => self.progress(attempt, worker, incarnation, phase, progress),
            Event::Complete {
                attempt,
                worker,
                incarnation,
                result,
            } => self.complete(attempt, worker, incarnation, result),
            Event::MemoryFailure {
                attempt,
                worker,
                incarnation,
                charged,
                peak_memory_bytes,
                quiescent,
                result,
            } => self.memory_failure(
                (attempt, worker, incarnation),
                charged,
                peak_memory_bytes,
                quiescent,
                result,
            ),
            Event::Decline {
                attempt,
                worker,
                incarnation,
            } => {
                let trial = match self.current_trial(attempt, worker, incarnation) {
                    Ok(trial) => trial,
                    Err(reason) => return Reply::Rejected(reason),
                };
                if self.trials[trial].phase != Phase::Dispatching {
                    return Reply::Rejected(Rejection::Stale);
                }
                self.nodes
                    .iter_mut()
                    .find(|node| node.worker == worker)
                    .unwrap()
                    .reported_free = Resources::default();
                self.revoke(trial, true);
                Reply::Applied
            }
            Event::Cancel { operation } => self.cancel(operation),
            Event::Observe { operation } => {
                match self.interests.iter().find(|row| row.operation == operation) {
                    Some(row) => Reply::Observed(Self::operation_view(row)),
                    None => Reply::Rejected(Rejection::Unknown),
                }
            }
            Event::RenewInterest { operation } => {
                let Some(row) = self
                    .interests
                    .iter_mut()
                    .find(|row| row.operation == operation)
                else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if row.result.is_some() || !row.active {
                    return Reply::Ignored;
                }
                row.expires.at = self.clock.saturating_add(INTEREST_TIMEOUT);
                row.expires.generation += 1;
                Reply::Applied
            }
            Event::AdvanceTime { now } => match now.cmp(&self.clock) {
                std::cmp::Ordering::Less => Reply::Rejected(Rejection::Stale),
                std::cmp::Ordering::Equal => Reply::Ignored,
                std::cmp::Ordering::Greater => {
                    self.clock = now;
                    Reply::Applied
                }
            },
            Event::ExpireAttempt {
                attempt,
                generation,
            } => {
                let Some(trial) = self.trials.iter().position(|row| row.attempt == attempt) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                let row = &self.trials[trial];
                if !row.valid || row.expires.generation != generation {
                    return Reply::Ignored;
                }
                let deadline = if row.phase == Phase::Running {
                    row.run_until
                        .map_or(row.expires.at, |running| running.min(row.expires.at))
                } else {
                    row.expires.at
                };
                if self.clock < deadline {
                    return Reply::Rejected(Rejection::TooEarly);
                }
                self.revoke(trial, false);
                Reply::Applied
            }
            Event::ExpireExecution { attempt } => {
                let Some(trial) = self.trials.iter().position(|row| row.attempt == attempt) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                let row = &self.trials[trial];
                if !row.valid || row.phase != Phase::Running {
                    return Reply::Ignored;
                }
                let Some(deadline) = row.run_until else {
                    return Reply::Ignored;
                };
                if self.clock < deadline {
                    return Reply::Rejected(Rejection::TooEarly);
                }
                self.revoke(trial, false);
                Reply::Applied
            }
            Event::ExpireInterest {
                operation,
                generation,
            } => {
                let Some(row) = self.interests.iter().find(|row| row.operation == operation) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if !row.active || row.result.is_some() || row.expires.generation != generation {
                    return Reply::Ignored;
                }
                if self.clock < row.expires.at {
                    return Reply::Rejected(Rejection::TooEarly);
                }
                self.cancel(operation)
            }
            Event::Cleanup {
                attempt,
                worker,
                incarnation,
            } => self.cleanup(attempt, worker, incarnation),
            Event::EffectOutcome { effect, success } => {
                let Some(index) = self.outbox.iter().position(|row| row.id == effect) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if success && matches!(self.outbox[index].kind, EffectKind::PublishResult { .. }) {
                    self.outbox.remove(index);
                    Reply::Applied
                } else {
                    Reply::Ignored
                }
            }
            Event::RetireExecution { execution } => self.retire(execution),
            // This original oracle qualifies the unactivated core-5 contract.
            Event::IssueReceipts { .. }
            | Event::AcknowledgeReceipts { .. }
            | Event::HistoryPublication { .. } => Reply::Rejected(Rejection::Invalid),
        }
    }

    fn session_node(
        &self,
        worker: WorkerId,
        incarnation: IncarnationId,
        session: SessionId,
    ) -> Result<usize, Rejection> {
        let Some(node) = self.nodes.iter().position(|row| row.worker == worker) else {
            return Err(Rejection::Unknown);
        };
        if self.nodes[node].incarnation != incarnation || self.nodes[node].session != session {
            return Err(Rejection::Stale);
        }
        Ok(node)
    }

    fn current_trial(
        &self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    ) -> Result<usize, Rejection> {
        let Some(index) = self.trials.iter().position(|row| row.attempt == attempt) else {
            return Err(Rejection::Unknown);
        };
        let row = &self.trials[index];
        let current = self.jobs.iter().any(|job| {
            job.execution == row.execution && job.current == Some(attempt) && job.result.is_none()
        });
        let live_incarnation = self
            .nodes
            .iter()
            .any(|node| node.worker == worker && node.incarnation == incarnation);
        if !row.valid
            || row.worker != worker
            || row.incarnation != incarnation
            || !current
            || !live_incarnation
        {
            return Err(Rejection::Stale);
        }
        Ok(index)
    }

    fn effect(&mut self, kind: EffectKind) {
        let id = EffectId(self.next_effect);
        self.next_effect += 1;
        self.outbox.push(Effect {
            id,
            epoch: self.epoch,
            kind,
        });
    }

    fn remove_dispatch(&mut self, attempt: AttemptId) {
        self.outbox.retain(|row| {
            !matches!(row.kind,
            EffectKind::Dispatch { attempt: target, .. } if target == attempt)
        });
    }

    fn change_phase(&mut self, execution: ExecutionId, phase: Phase) {
        let job = self
            .jobs
            .iter_mut()
            .find(|row| row.execution == execution)
            .unwrap();
        if job.phase == phase {
            return;
        }
        job.phase = phase;
        for interest in self
            .interests
            .iter_mut()
            .filter(|row| row.execution == execution && row.active && row.result.is_none())
        {
            interest.revision += 1;
        }
    }

    fn terminalize(&mut self, execution: ExecutionId, result: &TerminalResult) {
        let job = self
            .jobs
            .iter_mut()
            .find(|row| row.execution == execution)
            .unwrap();
        job.phase = Phase::Terminal;
        job.current = None;
        job.result = Some(result.clone());
        job.finished_at = Some(self.clock);
        for interest in self
            .interests
            .iter_mut()
            .filter(|row| row.execution == execution && row.active && row.result.is_none())
        {
            interest.result = Some(result.clone());
            interest.active = false;
            interest.revision += 1;
        }
    }

    fn release(&mut self, attempt: AttemptId) {
        self.charges
            .iter_mut()
            .find(|row| row.attempt == attempt)
            .unwrap()
            .released = true;
    }

    fn revoke(&mut self, trial: usize, quiescent: bool) {
        let row = self.trials[trial].clone();
        self.trials[trial].valid = false;
        self.remove_dispatch(row.attempt);
        if quiescent {
            self.release(row.attempt);
        } else {
            self.effect(EffectKind::Cancel {
                execution: row.execution,
                attempt: row.attempt,
                worker: row.worker,
                incarnation: row.incarnation,
            });
        }
        let job = self
            .jobs
            .iter_mut()
            .find(|job| job.execution == row.execution)
            .unwrap();
        job.current = None;
        if job.attempts >= MAX_ATTEMPTS {
            self.terminalize(row.execution, &TerminalResult::Exhausted);
        } else {
            self.change_phase(row.execution, Phase::Queued);
        }
    }

    fn register(
        &mut self,
        worker: WorkerId,
        incarnation: IncarnationId,
        session: SessionId,
        properties: BTreeMap<String, String>,
        capacity: Resources,
        available: Resources,
    ) -> Reply {
        if serde_json::to_vec(&properties).unwrap().len() > self.limits.max_payload_bytes {
            return Reply::Rejected(Rejection::Invalid);
        }
        if let Some(index) = self.nodes.iter().position(|row| row.worker == worker) {
            let old = &self.nodes[index];
            if incarnation < old.incarnation
                || (incarnation == old.incarnation && session <= old.session)
            {
                return Reply::Rejected(Rejection::Stale);
            }
            let prospective = self
                .reserved_bytes()
                .and_then(|credit| credit.checked_sub(Self::worker_credit(&old.properties)?))
                .and_then(|credit| credit.checked_add(Self::worker_credit(&properties)?));
            if prospective.is_none_or(|credit| credit > CREDIT_LIMIT) {
                return Reply::Rejected(Rejection::Overloaded);
            }
            if incarnation != old.incarnation {
                let mut revoke: Vec<_> = self
                    .trials
                    .iter()
                    .enumerate()
                    .filter(|(_, trial)| trial.worker == worker && trial.valid)
                    .map(|(index, trial)| (trial.attempt, index))
                    .collect();
                revoke.sort_by_key(|(attempt, _)| *attempt);
                for (_, trial) in revoke {
                    self.revoke(trial, false);
                }
            }
            self.nodes[index] = Node {
                worker,
                incarnation,
                session,
                properties,
                capacity,
                reported_free: available,
                draining: false,
                heartbeat_at: self.clock,
            };
        } else {
            if self.nodes.len() >= self.limits.max_workers {
                return Reply::Rejected(Rejection::Overloaded);
            }
            if self
                .reserved_bytes()
                .and_then(|credit| credit.checked_add(Self::worker_credit(&properties)?))
                .is_none_or(|credit| credit > CREDIT_LIMIT)
            {
                return Reply::Rejected(Rejection::Overloaded);
            }
            self.nodes.push(Node {
                worker,
                incarnation,
                session,
                properties,
                capacity,
                reported_free: available,
                draining: false,
                heartbeat_at: self.clock,
            });
        }
        Reply::Applied
    }

    fn compatible(left: &Request, right: &Request) -> bool {
        !left.do_not_cache
            && !right.do_not_cache
            && left.action == right.action
            && left.execution_scope == right.execution_scope
            && left.properties == right.properties
            && left.resources == right.resources
            && left.execution_timeout == right.execution_timeout
    }

    fn submit(&mut self, operation: OperationId, request: Request) -> Reply {
        if let Some(row) = self.interests.iter().find(|row| row.operation == operation) {
            return if row.original == request {
                Reply::Submitted {
                    operation,
                    execution: row.execution,
                }
            } else {
                Reply::Rejected(Rejection::Invalid)
            };
        }
        if operation.0 <= self.highest_admitted_operation {
            return Reply::Rejected(Rejection::Stale);
        }
        if !self.valid_request(&request) {
            return Reply::Rejected(Rejection::Invalid);
        }
        if self.interests.len() >= self.limits.max_operations {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let join = self
            .jobs
            .iter()
            .filter(|row| {
                row.phase != Phase::Terminal
                    && Self::compatible(&row.request, &request)
                    && !(request.skip_cache_lookup && row.phase == Phase::Lookup)
            })
            .min_by_key(|row| row.execution)
            .map(|row| row.execution);
        let prospective = self
            .reserved_bytes()
            .and_then(|credit| credit.checked_add(self.operation_credit(&request)?))
            .and_then(|credit| {
                if join.is_some() {
                    Some(credit)
                } else {
                    credit.checked_add(self.execution_credit(&request)?)
                }
            });
        if prospective.is_none_or(|credit| credit > CREDIT_LIMIT) {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let execution = if let Some(execution) = join {
            execution
        } else {
            if self.jobs.len() >= self.limits.max_executions
                || self.jobs.len().saturating_add(1).saturating_mul(8) > self.limits.max_effects
            {
                return Reply::Rejected(Rejection::Overloaded);
            }
            let execution = ExecutionId(self.next_execution);
            self.next_execution += 1;
            let needs_lookup = !request.do_not_cache && !request.skip_cache_lookup;
            self.jobs.push(Job {
                execution,
                target: request.resources,
                request: request.clone(),
                phase: if needs_lookup {
                    Phase::Lookup
                } else {
                    Phase::Queued
                },
                ordinal: execution.0,
                enqueue_at: self.clock,
                hold_cooldown_until: 0,
                current: None,
                attempts: 0,
                result: None,
                finished_at: None,
            });
            if needs_lookup {
                self.effect(EffectKind::Lookup {
                    execution,
                    action: request.action.clone(),
                });
            }
            execution
        };
        self.interests.push(Interest {
            operation,
            execution,
            urgency: request.priority,
            active: true,
            result: None,
            revision: 1,
            expires: Deadline {
                at: self.clock.saturating_add(INTEREST_TIMEOUT),
                generation: 1,
            },
            original: request,
        });
        self.highest_admitted_operation = operation.0;
        Reply::Submitted {
            operation,
            execution,
        }
    }

    fn lookup(&mut self, effect: EffectId, result: Option<ResultPayload>) -> Reply {
        let Some(index) = self.outbox.iter().position(|row| row.id == effect) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        let EffectKind::Lookup { execution, .. } = self.outbox[index].kind else {
            return Reply::Rejected(Rejection::Invalid);
        };
        if let Some(result) = &result
            && !self.valid_result(result)
        {
            return Reply::Rejected(Rejection::Invalid);
        }
        self.outbox.remove(index);
        if let Some(result) = result {
            self.terminalize(execution, &TerminalResult::Completed(result));
        } else {
            self.change_phase(execution, Phase::Queued);
        }
        Reply::Applied
    }

    fn fits(&self, job: &Job, node: &Node) -> bool {
        if node.draining
            || self.quarantined(node.worker, node.incarnation)
            || job
                .request
                .properties
                .iter()
                .any(|(key, value)| node.properties.get(key) != Some(value))
        {
            return false;
        }
        let charged = self.charged(node.worker);
        let required = job.target;
        [
            (
                required.cpu_millis,
                charged.cpu_millis,
                node.capacity.cpu_millis,
                node.reported_free.cpu_millis,
            ),
            (
                required.memory_bytes,
                charged.memory_bytes,
                node.capacity.memory_bytes,
                node.reported_free.memory_bytes,
            ),
            (
                required.slots,
                charged.slots,
                node.capacity.slots,
                node.reported_free.slots,
            ),
        ]
        .iter()
        .all(|(need, held, total, free)| {
            held <= total && held <= free && need <= &(total - held) && need <= &(free - held)
        })
    }

    fn assign(&mut self) -> Reply {
        // Enumerate and sort the complete queue relation. Each failed placement
        // may establish an aged hold before considering lower-ranked work.
        let mut queue: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|job| self.queue_rank(job.execution))
            .collect();
        queue.sort_unstable();
        if queue.is_empty() {
            return Reply::Ignored;
        }
        if self.next_attempt == u64::MAX || self.next_reservation == u64::MAX {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let mut changed = false;
        let mut selected = None;
        for (_, _, execution) in queue {
            let job = self
                .jobs
                .iter()
                .find(|job| job.execution == execution)
                .unwrap()
                .clone();
            let worker = self
                .nodes
                .iter()
                .filter(|node| {
                    self.fits(&job, node) && !self.blocked_by_hold(execution, node.worker)
                })
                .map(|node| node.worker)
                .min();
            if let Some(worker) = worker {
                selected = Some((execution, worker));
                break;
            }
            changed |= self.consider_hold(&job);
        }
        let Some((execution, worker)) = selected else {
            return if changed {
                Reply::Applied
            } else {
                Reply::Ignored
            };
        };
        let node = self.nodes.iter().find(|row| row.worker == worker).unwrap();
        let incarnation = node.incarnation;
        let request = self
            .jobs
            .iter()
            .find(|row| row.execution == execution)
            .unwrap()
            .request
            .clone();
        let grant = self
            .jobs
            .iter()
            .find(|job| job.execution == execution)
            .unwrap()
            .target;
        let attempt = AttemptId(self.next_attempt);
        self.next_attempt += 1;
        let reservation = ReservationId(self.next_reservation);
        self.next_reservation += 1;
        self.charges.push(Charge {
            reservation,
            attempt,
            worker,
            incarnation,
            original: grant,
            released: false,
        });
        self.trials.push(Trial {
            attempt,
            dispatch_effect: EffectId(self.next_effect),
            execution,
            worker,
            incarnation,
            reservation,
            valid: true,
            phase: Phase::Dispatching,
            progress: 0,
            expires: Deadline {
                at: self.clock.saturating_add(ACK_TIMEOUT),
                generation: 1,
            },
            run_until: None,
        });
        let job = self
            .jobs
            .iter_mut()
            .find(|row| row.execution == execution)
            .unwrap();
        job.current = Some(attempt);
        job.attempts += 1;
        job.hold_cooldown_until = 0;
        self.holds.retain(|hold| hold.execution != execution);
        self.change_phase(execution, Phase::Dispatching);
        self.effect(EffectKind::Dispatch {
            execution,
            attempt,
            worker,
            incarnation,
            reservation,
            charged: grant,
            request,
        });
        Reply::Matched {
            execution,
            attempt,
            worker,
        }
    }

    fn queue_rank(&self, execution: ExecutionId) -> Option<(i32, u64, ExecutionId)> {
        let job = self.jobs.iter().find(|job| job.execution == execution)?;
        (job.phase == Phase::Queued && job.attempts < MAX_ATTEMPTS).then_some(())?;
        Some((self.priority(execution)?, job.ordinal, execution))
    }

    fn blocked_by_hold(&self, execution: ExecutionId, worker: WorkerId) -> bool {
        self.holds
            .iter()
            .filter(|hold| hold.worker == worker)
            .any(|hold| {
                self.queue_rank(hold.execution)
                    .zip(self.queue_rank(execution))
                    .is_some_and(|(owner, contender)| contender > owner)
            })
    }

    fn room_for(need: Resources, charged: Resources, node: &Node) -> bool {
        [
            (
                need.cpu_millis,
                charged.cpu_millis,
                node.capacity.cpu_millis,
                node.reported_free.cpu_millis,
            ),
            (
                need.memory_bytes,
                charged.memory_bytes,
                node.capacity.memory_bytes,
                node.reported_free.memory_bytes,
            ),
            (
                need.slots,
                charged.slots,
                node.capacity.slots,
                node.reported_free.slots,
            ),
        ]
        .into_iter()
        .all(|(need, charged, capacity, ceiling)| {
            charged <= capacity
                && charged <= ceiling
                && need <= capacity - charged
                && need <= ceiling - charged
        })
    }

    fn usable_for_holds(&self, node: &Node) -> bool {
        !node.draining
            && !self.quarantined(node.worker, node.incarnation)
            && node.capacity.slots > 0
            && node.reported_free.slots > 0
            && Self::room_for(Resources::default(), self.charged(node.worker), node)
    }

    fn immovable(&self, worker: WorkerId) -> Resources {
        let mut floor = Resources::default();
        for charge in self
            .charges
            .iter()
            .filter(|charge| charge.worker == worker && !charge.released)
        {
            if self
                .trials
                .iter()
                .any(|trial| trial.attempt == charge.attempt && trial.valid)
            {
                continue;
            }
            floor.cpu_millis = floor
                .cpu_millis
                .checked_add(charge.original.cpu_millis)
                .expect("oracle cpu floor overflow");
            floor.memory_bytes = floor
                .memory_bytes
                .checked_add(charge.original.memory_bytes)
                .expect("oracle memory floor overflow");
            floor.slots = floor
                .slots
                .checked_add(charge.original.slots)
                .expect("oracle slot floor overflow");
        }
        floor
    }

    fn hold_compatible(&self, job: &Job, node: &Node) -> bool {
        self.usable_for_holds(node)
            && job
                .request
                .properties
                .iter()
                .all(|(key, value)| node.properties.get(key) == Some(value))
            && Self::room_for(job.target, self.immovable(node.worker), node)
    }

    fn hold_cap(&self) -> usize {
        let usable = self
            .nodes
            .iter()
            .filter(|node| self.usable_for_holds(node))
            .count();
        if usable == 0 { 0 } else { (usable / 10).max(1) }
    }

    fn consider_hold(&mut self, job: &Job) -> bool {
        if self.clock.saturating_sub(job.enqueue_at) < HOLD_AFTER
            || self.clock < job.hold_cooldown_until
            || self
                .holds
                .iter()
                .any(|hold| hold.execution == job.execution)
            || self.holds.len() >= self.hold_cap()
        {
            return false;
        }
        let Some(until) = self.clock.checked_add(HOLD_DURATION) else {
            return false;
        };
        let choice = self
            .nodes
            .iter()
            .filter(|node| {
                self.hold_compatible(job, node)
                    && !self.holds.iter().any(|hold| hold.worker == node.worker)
            })
            .filter_map(|node| {
                let count = self
                    .charges
                    .iter()
                    .filter(|charge| charge.worker == node.worker && !charge.released)
                    .count();
                if count == 0 {
                    return None;
                }
                let free_memory = node
                    .capacity
                    .memory_bytes
                    .min(node.reported_free.memory_bytes)
                    - self.charged(node.worker).memory_bytes;
                Some((
                    count,
                    std::cmp::Reverse(free_memory),
                    node.worker,
                    node.incarnation,
                ))
            })
            .min();
        let Some((_, _, worker, incarnation)) = choice else {
            return false;
        };
        self.holds.push(HoldView {
            execution: job.execution,
            worker,
            incarnation,
            started_at: self.clock,
            until,
        });
        true
    }

    fn reconcile_holds(&mut self) {
        let mut retained = Vec::new();
        for hold in self.holds.clone() {
            let Some(job) = self.jobs.iter().find(|job| job.execution == hold.execution) else {
                continue;
            };
            let Some(node) = self
                .nodes
                .iter()
                .find(|node| node.worker == hold.worker && node.incarnation == hold.incarnation)
            else {
                continue;
            };
            if self.queue_rank(hold.execution).is_none() || !self.hold_compatible(job, node) {
                continue;
            }
            if hold.until <= self.clock {
                self.jobs
                    .iter_mut()
                    .find(|job| job.execution == hold.execution)
                    .unwrap()
                    .hold_cooldown_until = self.clock.saturating_add(HOLD_COOLDOWN);
            } else {
                retained.push(hold);
            }
        }
        retained.sort_by_key(|hold| (self.queue_rank(hold.execution).unwrap(), hold.worker));
        retained.truncate(self.hold_cap());
        self.holds = retained;
    }

    const fn phase_order(phase: Phase) -> Option<u8> {
        match phase {
            Phase::Dispatching => Some(0),
            Phase::Accepted => Some(1),
            Phase::Preparing => Some(2),
            Phase::Running => Some(3),
            Phase::Uploading => Some(4),
            _ => None,
        }
    }

    fn progress(
        &mut self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        phase: Phase,
        progress: u64,
    ) -> Reply {
        let trial = match self.current_trial(attempt, worker, incarnation) {
            Ok(trial) => trial,
            Err(reason) => return Reply::Rejected(reason),
        };
        if !matches!(phase, Phase::Preparing | Phase::Running | Phase::Uploading) {
            return Reply::Rejected(Rejection::Invalid);
        }
        let old_phase = self.trials[trial].phase;
        if old_phase == Phase::Dispatching {
            return Reply::Rejected(Rejection::Invalid);
        }
        if Self::phase_order(phase) < Self::phase_order(old_phase) {
            return Reply::Rejected(Rejection::Invalid);
        }
        if old_phase == phase && progress <= self.trials[trial].progress {
            return Reply::Ignored;
        }
        let execution = self.trials[trial].execution;
        let timeout = self
            .jobs
            .iter()
            .find(|row| row.execution == execution)
            .unwrap()
            .request
            .execution_timeout;
        let row = &mut self.trials[trial];
        row.phase = phase;
        row.progress = progress;
        row.expires.at = self.clock.saturating_add(PROGRESS_TIMEOUT);
        row.expires.generation += 1;
        if phase == Phase::Running && row.run_until.is_none() {
            row.run_until = Some(self.clock.saturating_add(timeout));
        }
        self.remove_dispatch(attempt);
        self.change_phase(execution, phase);
        Reply::Applied
    }

    fn complete(
        &mut self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        result: ResultPayload,
    ) -> Reply {
        let trial = match self.current_trial(attempt, worker, incarnation) {
            Ok(trial) => trial,
            Err(reason) => return Reply::Rejected(reason),
        };
        if !self.valid_result(&result) {
            return Reply::Rejected(Rejection::Invalid);
        }
        let execution = self.trials[trial].execution;
        self.trials[trial].valid = false;
        self.release(attempt);
        self.remove_dispatch(attempt);
        self.terminalize(execution, &TerminalResult::Completed(result.clone()));
        let request = &self
            .jobs
            .iter()
            .find(|row| row.execution == execution)
            .unwrap()
            .request;
        if !request.do_not_cache && result.exit_code == 0 && result.error.is_none() {
            self.effect(EffectKind::PublishResult {
                execution,
                action: request.action.clone(),
                result,
            });
        }
        Reply::Applied
    }

    fn memory_failure(
        &mut self,
        identity: (AttemptId, WorkerId, IncarnationId),
        charged: Resources,
        peak: Option<u64>,
        quiescent: bool,
        result: ResultPayload,
    ) -> Reply {
        let (attempt, worker, incarnation) = identity;
        let trial = match self.current_trial(attempt, worker, incarnation) {
            Ok(trial) => trial,
            Err(reason) => return Reply::Rejected(reason),
        };
        if !quiescent
            || (result.exit_code == 0 && result.error.is_none())
            || !self.valid_result(&result)
            || self
                .charges
                .iter()
                .find(|row| row.attempt == attempt)
                .unwrap()
                .original
                != charged
        {
            return Reply::Rejected(Rejection::Invalid);
        }
        let execution = self.trials[trial].execution;
        let job = self
            .jobs
            .iter()
            .find(|job| job.execution == execution)
            .unwrap()
            .clone();
        self.trials[trial].valid = false;
        self.release(attempt);
        self.remove_dispatch(attempt);
        self.jobs
            .iter_mut()
            .find(|job| job.execution == execution)
            .unwrap()
            .current = None;

        let base = if charged.memory_bytes == 0 {
            peak.unwrap_or(0)
        } else {
            charged.memory_bytes
        };
        let reason = if job.attempts >= MAX_ATTEMPTS {
            Some(ResourceFailureReason::AttemptBudgetExhausted)
        } else if base == 0 {
            Some(ResourceFailureReason::MissingGrowthEvidence)
        } else {
            let ceiling = self
                .nodes
                .iter()
                .filter_map(|node| {
                    if node.draining
                        || self.quarantined(node.worker, node.incarnation)
                        || job
                            .request
                            .properties
                            .iter()
                            .any(|(key, value)| node.properties.get(key) != Some(value))
                    {
                        return None;
                    }
                    let total = self.charged(node.worker);
                    if total.cpu_millis > node.capacity.cpu_millis
                        || total.memory_bytes > node.capacity.memory_bytes
                        || total.slots > node.capacity.slots
                    {
                        return None;
                    }
                    let floor = self.immovable(node.worker);
                    if job.target.cpu_millis
                        > node.capacity.cpu_millis.checked_sub(floor.cpu_millis)?
                        || job.target.slots > node.capacity.slots.checked_sub(floor.slots)?
                    {
                        return None;
                    }
                    node.capacity.memory_bytes.checked_sub(floor.memory_bytes)
                })
                .max()
                .unwrap_or(0);
            let next = base.saturating_mul(2).min(ceiling);
            if next <= charged.memory_bytes || next < job.request.resources.memory_bytes {
                Some(ResourceFailureReason::NoLargerEligibleWorker)
            } else {
                self.jobs
                    .iter_mut()
                    .find(|job| job.execution == execution)
                    .unwrap()
                    .target
                    .memory_bytes = next;
                None
            }
        };
        if let Some(reason) = reason {
            self.terminalize(
                execution,
                &TerminalResult::ResourceFailed { result, reason },
            );
        } else {
            self.change_phase(execution, Phase::Queued);
        }
        Reply::Applied
    }

    fn cancel(&mut self, operation: OperationId) -> Reply {
        let Some(index) = self
            .interests
            .iter()
            .position(|row| row.operation == operation)
        else {
            return Reply::Rejected(Rejection::Unknown);
        };
        if self.interests[index].result.is_some() {
            return Reply::Ignored;
        }
        let execution = self.interests[index].execution;
        self.interests[index].active = false;
        self.interests[index].result = Some(TerminalResult::Cancelled);
        self.interests[index].revision += 1;
        if self.priority(execution).is_some() {
            return Reply::Applied;
        }
        if let Some(trial) = self
            .trials
            .iter()
            .position(|row| row.execution == execution && row.valid)
        {
            let row = self.trials[trial].clone();
            self.trials[trial].valid = false;
            self.remove_dispatch(row.attempt);
            self.effect(EffectKind::Cancel {
                execution,
                attempt: row.attempt,
                worker: row.worker,
                incarnation: row.incarnation,
            });
        }
        self.outbox.retain(|row| {
            !matches!(row.kind,
            EffectKind::Lookup { execution: target, .. } if target == execution)
        });
        self.terminalize(execution, &TerminalResult::Cancelled);
        Reply::Applied
    }

    fn cleanup(
        &mut self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    ) -> Reply {
        let Some(trial) = self.trials.iter().find(|row| row.attempt == attempt) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        if trial.worker != worker || trial.incarnation != incarnation {
            return Reply::Rejected(Rejection::Stale);
        }
        if trial.valid {
            return Reply::Rejected(Rejection::Busy);
        }
        if self
            .charges
            .iter()
            .find(|row| row.attempt == attempt)
            .unwrap()
            .released
        {
            return Reply::Ignored;
        }
        self.release(attempt);
        self.outbox.retain(|row| !matches!(row.kind,
            EffectKind::Dispatch { attempt: target, .. } | EffectKind::Cancel { attempt: target, .. }
                if target == attempt));
        Reply::Applied
    }

    fn retire(&mut self, execution: ExecutionId) -> Reply {
        let Some(job) = self.jobs.iter().find(|row| row.execution == execution) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        let Some(finished) = job.finished_at else {
            return Reply::Rejected(Rejection::Busy);
        };
        if self.clock < finished.saturating_add(TERMINAL_RETENTION) {
            return Reply::Rejected(Rejection::TooEarly);
        }
        let attempts: Vec<_> = self
            .trials
            .iter()
            .filter(|row| row.execution == execution)
            .map(|row| row.attempt)
            .collect();
        if self
            .charges
            .iter()
            .any(|row| attempts.contains(&row.attempt) && !row.released)
            || self.outbox.iter().any(|row| match row.kind {
                EffectKind::Lookup {
                    execution: target, ..
                }
                | EffectKind::Dispatch {
                    execution: target, ..
                }
                | EffectKind::Cancel {
                    execution: target, ..
                }
                | EffectKind::PublishResult {
                    execution: target, ..
                } => target == execution,
            })
        {
            return Reply::Rejected(Rejection::Busy);
        }
        self.jobs.retain(|row| row.execution != execution);
        self.interests.retain(|row| row.execution != execution);
        self.trials.retain(|row| row.execution != execution);
        self.charges.retain(|row| !attempts.contains(&row.attempt));
        Reply::Applied
    }

    pub(crate) fn view(&self) -> ObservableState {
        let mut operations: Vec<_> = self.interests.iter().map(Self::operation_view).collect();
        operations.sort_by_key(|row| row.id);
        let mut executions: Vec<_> = self
            .jobs
            .iter()
            .map(|row| ExecutionView {
                id: row.execution,
                request: row.request.clone(),
                reservation_target: row.target,
                phase: row.phase,
                effective_priority: self.priority(row.execution),
                enqueue_ordinal: row.ordinal,
                enqueue_at: row.enqueue_at,
                hold_cooldown_until: row.hold_cooldown_until,
                attempt: row.current,
                attempts_started: row.attempts,
                terminal: row.result.clone(),
                terminal_at: row.finished_at,
            })
            .collect();
        executions.sort_by_key(|row| row.id);
        let mut workers: Vec<_> = self
            .nodes
            .iter()
            .map(|row| WorkerView {
                id: row.worker,
                incarnation: row.incarnation,
                session: row.session,
                properties: row.properties.clone(),
                capacity: row.capacity,
                available: row.reported_free,
                draining: row.draining,
                last_heartbeat: row.heartbeat_at,
                charged: self.charged(row.worker),
                quarantined: self.quarantined(row.worker, row.incarnation),
            })
            .collect();
        workers.sort_by_key(|row| row.id);
        let mut attempts: Vec<_> = self
            .trials
            .iter()
            .map(|row| AttemptView {
                id: row.attempt,
                dispatch_effect: row.dispatch_effect,
                execution: row.execution,
                worker: row.worker,
                incarnation: row.incarnation,
                reservation: row.reservation,
                authorized: row.valid,
                phase: row.phase,
                progress: row.progress,
                deadline: row.expires,
                running_deadline: row.run_until,
            })
            .collect();
        attempts.sort_by_key(|row| row.id);
        let mut reservations: Vec<_> = self
            .charges
            .iter()
            .map(|row| ReservationView {
                id: row.reservation,
                attempt: row.attempt,
                worker: row.worker,
                incarnation: row.incarnation,
                charged: row.original,
                released: row.released,
            })
            .collect();
        reservations.sort_by_key(|row| row.id);
        let mut effects = self.outbox.clone();
        effects.sort_by_key(|row| row.id);
        let mut holds = self.holds.clone();
        holds.sort_by_key(|hold| hold.worker);
        ObservableState {
            epoch: self.epoch,
            now: self.clock,
            operations,
            executions,
            workers,
            attempts,
            reservations,
            effects,
            holds,
        }
    }

    fn operation_view(row: &Interest) -> OperationView {
        OperationView {
            id: row.operation,
            execution: row.execution,
            priority: row.urgency,
            interested: row.active,
            terminal: row.result.clone(),
            version: row.revision,
            interest_deadline: row.expires,
        }
    }

    fn priority(&self, execution: ExecutionId) -> Option<i32> {
        self.interests
            .iter()
            .filter(|row| row.execution == execution && row.active)
            .map(|row| row.urgency)
            .min()
    }

    fn charged(&self, worker: WorkerId) -> Resources {
        let mut sum = Resources::default();
        for charge in self
            .charges
            .iter()
            .filter(|row| row.worker == worker && !row.released)
        {
            sum.cpu_millis = sum
                .cpu_millis
                .checked_add(charge.original.cpu_millis)
                .expect("oracle admitted overflowing cpu obligations");
            sum.memory_bytes = sum
                .memory_bytes
                .checked_add(charge.original.memory_bytes)
                .expect("oracle admitted overflowing memory obligations");
            sum.slots = sum
                .slots
                .checked_add(charge.original.slots)
                .expect("oracle admitted overflowing slot obligations");
        }
        sum
    }

    fn quarantined(&self, worker: WorkerId, incarnation: IncarnationId) -> bool {
        self.charges
            .iter()
            .any(|row| row.worker == worker && row.incarnation != incarnation && !row.released)
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, String> {
        // Tuple framing is independent from the implementation's envelope.
        let bytes = serde_json::to_vec(&("independent-authority-relations-v5", self))
            .map_err(|error| error.to_string())?;
        if bytes.len() > CREDIT_LIMIT {
            return Err("independent reference snapshot exceeds byte limit".into());
        }
        Ok(bytes)
    }

    pub(crate) fn restore(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > CREDIT_LIMIT {
            return Err("independent reference raw snapshot exceeds byte limit".into());
        }
        let (format, state): (String, Self) =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if format != "independent-authority-relations-v5" {
            return Err("unknown independent reference format".to_owned());
        }
        if state.limits.max_payload_bytes > COMPLETE_PAYLOAD_LIMIT
            || state
                .reserved_bytes()
                .is_none_or(|credit| credit > CREDIT_LIMIT)
        {
            return Err("independent reference has invalid future byte credits".into());
        }
        if state.interests.len() > state.limits.max_operations
            || state.jobs.len() > state.limits.max_executions
            || state.nodes.len() > state.limits.max_workers
            || state.outbox.len() > state.limits.max_effects
            || state.trials.len() != state.charges.len()
        {
            return Err("independent reference exceeds prepaid relation counts".into());
        }
        let unique =
            |ids: Vec<u64>| ids.iter().copied().collect::<BTreeSet<_>>().len() == ids.len();
        if !unique(state.interests.iter().map(|row| row.operation.0).collect())
            || !unique(state.jobs.iter().map(|row| row.execution.0).collect())
            || !unique(state.nodes.iter().map(|row| row.worker.0).collect())
            || !unique(state.trials.iter().map(|row| row.attempt.0).collect())
            || !unique(
                state
                    .trials
                    .iter()
                    .map(|row| row.dispatch_effect.0)
                    .collect(),
            )
            || !unique(state.charges.iter().map(|row| row.reservation.0).collect())
            || !unique(state.charges.iter().map(|row| row.attempt.0).collect())
            || !unique(state.outbox.iter().map(|row| row.id.0).collect())
        {
            return Err("independent reference duplicates prepaid relation identity".into());
        }
        if state.interests.iter().any(|interest| {
            !state
                .jobs
                .iter()
                .any(|job| job.execution == interest.execution)
        }) || state.trials.iter().any(|trial| {
            !state
                .jobs
                .iter()
                .any(|job| job.execution == trial.execution)
        }) {
            return Err("independent reference has an unowned prepaid row".into());
        }
        if state.trials.iter().any(|trial| {
            trial.dispatch_effect.0 == 0 || trial.dispatch_effect.0 >= state.next_effect
        }) {
            return Err("independent reference has an unallocated dispatch identity".into());
        }
        for request in state
            .interests
            .iter()
            .map(|row| &row.original)
            .chain(state.jobs.iter().map(|row| &row.request))
            .chain(state.outbox.iter().filter_map(|effect| match &effect.kind {
                EffectKind::Dispatch { request, .. } => Some(request),
                _ => None,
            }))
        {
            if !state.valid_request(request) {
                return Err("independent reference has invalid captured request facts".into());
            }
        }
        for terminal in state
            .interests
            .iter()
            .filter_map(|row| row.result.as_ref())
            .chain(state.jobs.iter().filter_map(|row| row.result.as_ref()))
        {
            let invalid = match terminal {
                TerminalResult::Completed(result) => !state.valid_result(result),
                TerminalResult::ResourceFailed { result, .. } => {
                    !state.valid_result(result) || (result.exit_code == 0 && result.error.is_none())
                }
                _ => false,
            };
            if invalid {
                return Err("independent reference has invalid terminal response facts".to_owned());
            }
        }
        let mut owned_effects = BTreeMap::<ExecutionId, (usize, usize)>::new();
        for effect in &state.outbox {
            let (execution, publication) = match &effect.kind {
                EffectKind::Lookup { execution, .. }
                | EffectKind::Dispatch { execution, .. }
                | EffectKind::Cancel { execution, .. } => (*execution, false),
                EffectKind::PublishResult { execution, .. } => (*execution, true),
            };
            let counts = owned_effects.entry(execution).or_default();
            counts.0 += 1;
            counts.1 += usize::from(publication);
            if counts.0 > 8
                || counts.1 > 1
                || !state.jobs.iter().any(|job| job.execution == execution)
            {
                return Err("independent reference effect ownership bounds are invalid".into());
            }
            let owner = state
                .jobs
                .iter()
                .find(|job| job.execution == execution)
                .unwrap();
            match &effect.kind {
                EffectKind::Lookup { action, .. } | EffectKind::PublishResult { action, .. }
                    if action != &owner.request.action =>
                {
                    return Err("independent reference has an unpaid action-key copy".into());
                }
                EffectKind::Dispatch { request, .. } if request != &owner.request => {
                    return Err("independent reference has an unpaid dispatch request copy".into());
                }
                _ => {}
            }
            if let EffectKind::PublishResult { result, .. } = &effect.kind
                && !state.valid_result(result)
            {
                return Err(
                    "independent reference has invalid publication response facts".to_owned(),
                );
            }
            if let EffectKind::Dispatch { attempt, .. } = &effect.kind
                && !state
                    .trials
                    .iter()
                    .any(|trial| trial.attempt == *attempt && trial.dispatch_effect == effect.id)
            {
                return Err("independent reference dispatch identity mismatch".into());
            }
            if let EffectKind::Dispatch {
                attempt, charged, ..
            } = &effect.kind
                && !state.charges.iter().any(|reservation| {
                    reservation.attempt == *attempt && reservation.original == *charged
                })
            {
                return Err("independent reference dispatch charge mismatch".into());
            }
        }
        for job in &state.jobs {
            let trials: Vec<_> = state
                .trials
                .iter()
                .filter(|trial| trial.execution == job.execution)
                .collect();
            let current: Vec<_> = trials.iter().filter(|trial| trial.valid).collect();
            let lookup_count = state
                .outbox
                .iter()
                .filter(|effect| {
                    matches!(effect.kind,
                EffectKind::Lookup { execution, .. } if execution == job.execution)
                })
                .count();
            let dispatch_count = state
                .outbox
                .iter()
                .filter(|effect| {
                    matches!(effect.kind,
                EffectKind::Dispatch { execution, .. } if execution == job.execution)
                })
                .count();
            if trials.len() != job.attempts as usize
                || job.attempts > MAX_ATTEMPTS
                || current.len() > 1
                || job.current != current.first().map(|trial| trial.attempt)
                || lookup_count != usize::from(job.phase == Phase::Lookup)
                || dispatch_count != usize::from(job.phase == Phase::Dispatching)
            {
                return Err(
                    "independent reference exceeds prepaid execution rows or payload copies".into(),
                );
            }
            if job.target.cpu_millis != job.request.resources.cpu_millis
                || job.target.slots != job.request.resources.slots
                || job.target.memory_bytes < job.request.resources.memory_bytes
            {
                return Err("independent reference target violates original minima".into());
            }
            for trial in state
                .trials
                .iter()
                .filter(|trial| trial.execution == job.execution)
            {
                let charge = state
                    .charges
                    .iter()
                    .find(|charge| charge.attempt == trial.attempt)
                    .ok_or("independent reference attempt has no original charge")?
                    .original;
                if charge.cpu_millis != job.request.resources.cpu_millis
                    || charge.slots != job.request.resources.slots
                    || charge.memory_bytes < job.request.resources.memory_bytes
                    || charge.memory_bytes > job.target.memory_bytes
                    || (trial.valid && charge != job.target)
                {
                    return Err(
                        "independent reference original charge or current target mismatch".into(),
                    );
                }
            }
        }
        if state.jobs.iter().any(|job| job.enqueue_at > state.clock)
            || state.holds.len() > state.hold_cap()
        {
            return Err("independent reference hold age or cap is invalid".into());
        }
        let mut held_workers = BTreeSet::new();
        let mut held_jobs = BTreeSet::new();
        for hold in &state.holds {
            let job = state
                .jobs
                .iter()
                .find(|job| job.execution == hold.execution)
                .ok_or("hold owner absent")?;
            let node = state
                .nodes
                .iter()
                .find(|node| node.worker == hold.worker && node.incarnation == hold.incarnation)
                .ok_or("hold worker absent")?;
            if !held_workers.insert(hold.worker)
                || !held_jobs.insert(hold.execution)
                || state.queue_rank(hold.execution).is_none()
                || !state.hold_compatible(job, node)
                || hold.started_at > state.clock
                || hold.until <= state.clock
                || hold.started_at.checked_add(HOLD_DURATION) != Some(hold.until)
                || hold
                    .started_at
                    .checked_sub(job.enqueue_at)
                    .is_none_or(|age| age < HOLD_AFTER)
                || hold.started_at < job.hold_cooldown_until
            {
                return Err("independent reference contains invalid hold relation".into());
            }
        }
        Ok(state)
    }

    /// Charge each relation for its lifetime independently. This row-by-row
    /// sum is algebraically RO+3RE+P(O+2E)+WP+1024(O+15E+2W+1).
    pub(crate) fn reserved_bytes(&self) -> Option<usize> {
        let mut credit = 1024_usize;
        for interest in &self.interests {
            credit = credit.checked_add(self.operation_credit(&interest.original)?)?;
        }
        for job in &self.jobs {
            credit = credit.checked_add(self.execution_credit(&job.request)?)?;
        }
        for node in &self.nodes {
            credit = credit.checked_add(Self::worker_credit(&node.properties)?)?;
        }
        Some(credit)
    }

    fn operation_credit(&self, request: &Request) -> Option<usize> {
        serde_json::to_vec(request)
            .ok()?
            .len()
            .checked_add(self.limits.max_payload_bytes)?
            .checked_add(1024)
    }

    fn execution_credit(&self, request: &Request) -> Option<usize> {
        serde_json::to_vec(request)
            .ok()?
            .len()
            .checked_mul(3)?
            .checked_add(self.limits.max_payload_bytes.checked_mul(2)?)?
            .checked_add(15 * 1024)
    }

    fn worker_credit(properties: &BTreeMap<String, String>) -> Option<usize> {
        serde_json::to_vec(properties)
            .ok()?
            .len()
            .checked_add(2 * 1024)
    }

    fn valid_request(&self, request: &Request) -> bool {
        if request.resources.slots == 0
            || request.execution_timeout == 0
            || request.action.digest_function.is_empty()
            || request.action.digest.is_empty()
            || !serde_json::to_vec(request)
                .is_ok_and(|bytes| bytes.len() <= self.limits.max_payload_bytes)
        {
            return false;
        }
        let Some(captured) = &request.payload else {
            return true;
        };
        let Some(projected) = serde_json::to_value(captured)
            .ok()
            .and_then(Self::captured_request_facts)
        else {
            return false;
        };
        let mut actual = request.clone();
        actual.payload = None;
        actual == projected
    }

    pub(crate) fn captured_request_facts(wire: serde_json::Value) -> Option<Request> {
        Self::request_facts(&serde_json::from_value::<CapturedRequest>(wire).ok()?)
    }

    fn request_facts(capture: &CapturedRequest) -> Option<Request> {
        let parts = [
            capture.execute.as_slice(),
            capture.metadata.as_deref().unwrap_or_default(),
            capture.queued.as_slice(),
            capture.principal.as_bytes(),
            capture.action.as_slice(),
            capture.command.as_slice(),
        ];
        if capture.version != 1
            || parts
                .iter()
                .try_fold(0_usize, |size, part| size.checked_add(part.len()))?
                > 256 * 1024
        {
            return None;
        }
        let mut checksum = DigestHasherFunc::Sha256.hasher();
        checksum.update(b"nativelink-request-v1");
        checksum.update(&capture.version.to_be_bytes());
        checksum.update(&[u8::from(capture.metadata.is_some())]);
        for part in parts {
            checksum.update(&u64::try_from(part.len()).ok()?.to_be_bytes());
            checksum.update(part);
        }
        if **checksum.finalize_digest().packed_hash() != capture.sha256 {
            return None;
        }
        let execute = ExecuteRequest::decode(capture.execute.as_slice()).ok()?;
        let metadata = capture
            .metadata
            .as_deref()
            .map(RequestMetadata::decode)
            .transpose()
            .ok()?;
        let queued = prost_types::Timestamp::decode(capture.queued.as_slice()).ok()?;
        if !(-62_135_596_800..=253_402_300_799).contains(&queued.seconds)
            || !(0..1_000_000_000).contains(&queued.nanos)
        {
            return None;
        }
        let action = Action::decode(capture.action.as_slice()).ok()?;
        let command = Command::decode(capture.command.as_slice()).ok()?;
        let (algorithm, name) = match execute.digest_function {
            0 | 1 => (DigestHasherFunc::Sha256, "sha256"),
            9 => (DigestHasherFunc::Blake3, "blake3"),
            _ => return None,
        };
        let action_digest = execute.action_digest.as_ref()?;
        for (digest, bytes) in [
            (action_digest, capture.action.as_slice()),
            (action.command_digest.as_ref()?, capture.command.as_slice()),
        ] {
            if !Self::digest_syntax(digest)
                || u64::try_from(digest.size_bytes).ok()? != u64::try_from(bytes.len()).ok()?
            {
                return None;
            }
            let mut hash = algorithm.hasher();
            hash.update(bytes);
            if hash.finalize_digest().packed_hash().to_string() != digest.hash {
                return None;
            }
        }
        if !Self::digest_syntax(action.input_root_digest.as_ref()?) {
            return None;
        }
        let platform = action
            .platform
            .as_ref()
            .filter(|platform| !platform.properties.is_empty())
            .or(command.platform.as_ref());
        let mut properties = BTreeMap::new();
        if let Some(platform) = platform {
            for property in &platform.properties {
                if properties
                    .insert(property.name.clone(), property.value.clone())
                    .is_some()
                {
                    return None;
                }
            }
        }
        for reserved in [
            "cpu_millis",
            "cpu_millicores",
            "memory_bytes",
            "memory_mib",
            "memory_mb",
            "slots",
            "disk_kb",
            "network_kbps",
            "disk_read_iops",
            "disk_write_iops",
            "disk_read_bps",
            "disk_write_bps",
            "shm_size",
            "gpu_count",
        ] {
            if properties.contains_key(reserved) {
                return None;
            }
        }
        let cpu_millis = Self::quantity(properties.remove("cpu_count"))?.checked_mul(1000)?;
        let memory_bytes = Self::quantity(properties.remove("memory_kb"))?.checked_mul(1024)?;
        let execution_timeout = match action.timeout {
            None => u64::MAX,
            Some(timeout)
                if (0..=315_576_000_000).contains(&timeout.seconds)
                    && (0..1_000_000_000).contains(&timeout.nanos) =>
            {
                (u64::try_from(timeout.seconds).ok()? + u64::from(timeout.nanos != 0)).max(1)
            }
            Some(_) => return None,
        };
        let execution_scope = metadata.as_ref().and_then(|metadata| {
            if metadata.tool_invocation_id.is_empty()
                || !metadata
                    .tool_details
                    .as_ref()
                    .is_some_and(|details| details.tool_name.eq_ignore_ascii_case("buck2"))
            {
                return None;
            }
            let mut hash = DigestHasherFunc::Sha256.hasher();
            hash.update(&(capture.principal.len() as u64).to_be_bytes());
            hash.update(capture.principal.as_bytes());
            hash.update(metadata.tool_invocation_id.as_bytes());
            Some(hash.finalize_digest().packed_hash().to_string())
        });
        Some(Request {
            action: nativelink_scheduler::authority::ActionKey {
                instance: execute.instance_name,
                digest_function: name.into(),
                digest: action_digest.hash.clone(),
                digest_size_bytes: u64::try_from(action_digest.size_bytes).ok()?,
            },
            execution_scope,
            do_not_cache: action.do_not_cache,
            skip_cache_lookup: execute.skip_cache_lookup,
            priority: execute.execution_policy.map_or(0, |policy| policy.priority),
            properties,
            resources: Resources {
                cpu_millis,
                memory_bytes,
                slots: 1,
            },
            execution_timeout,
            payload: None,
        })
    }

    fn digest_syntax(digest: &Digest) -> bool {
        digest.size_bytes >= 0
            && digest.hash.len() == 64
            && digest
                .hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }

    fn quantity(value: Option<String>) -> Option<u64> {
        let Some(value) = value else {
            return Some(0);
        };
        if value.is_empty()
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }
        value.parse().ok()
    }

    /// Recompute facts from the captured protobuf bytes. This intentionally
    /// does not call the candidate's capture, facts, validation or REAPI codec.
    fn valid_result(&self, result: &ResultPayload) -> bool {
        if !serde_json::to_vec(result)
            .is_ok_and(|bytes| bytes.len() <= self.limits.max_payload_bytes)
        {
            return false;
        }
        let Some(captured) = &result.response else {
            return true;
        };
        let bytes = captured.encoded();
        if bytes.len() > 256 * 1024 {
            return false;
        }
        let Ok(wire) = serde_json::to_value(captured) else {
            return false;
        };
        if wire.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
            return false;
        }
        let mut hash = DigestHasherFunc::Sha256.hasher();
        hash.update(bytes);
        let digest = hash.finalize_digest();
        let raw_hash: [u8; 32] = **digest.packed_hash();
        if wire.get("sha256") != serde_json::to_value(raw_hash).ok().as_ref()
            || result.digest != digest.packed_hash().to_string()
        {
            return false;
        }
        let Ok(decoded) = ExecuteResponse::decode(bytes) else {
            return false;
        };
        let code = decoded.status.as_ref().map_or(0, |status| status.code);
        if code == 0 && decoded.result.is_none() {
            return false;
        }
        let expected_exit = decoded.result.as_ref().map_or(0, |result| result.exit_code);
        let expected_error = (code != 0).then(|| {
            format!(
                "REAPI status {code}: {}",
                decoded
                    .status
                    .as_ref()
                    .map_or("", |status| status.message.as_str())
            )
        });
        result.exit_code == expected_exit && result.error == expected_error
    }
}
