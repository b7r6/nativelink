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

//! Experimental committed-event authority for the M1 scheduler contract.
//!
//! This module is not connected to production RPC, worker dispatch, or Raft.
//! `apply` requires a committed input. The harness separately checks proposal,
//! commit, application, and effect delivery. External work is represented only
//! by the recoverable outbox; this module has no I/O or clock dependencies.
//!
//! Snapshot version 4 is an experimental test format, not a production durable
//! format. M0 workload qualification and M3 format activation remain required.

#[cfg(test)]
mod tests;

pub mod journal;
pub mod reapi;
pub mod request;
pub mod types;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

use serde::{Deserialize, Serialize};
pub use types::*;

/// Each retained execution reserves every possible attempt's dispatch/cancel
/// slots, one cache lookup, and one accepted-result handoff. This conservative
/// bound prevents admission from consuming the space needed to finish work.
const EFFECT_HEADROOM: usize = MAX_ATTEMPTS as usize * 2 + 2;
const SNAPSHOT_VERSION: u32 = 4;
/// Audited fixed fields, map key, and punctuation for every current row kind.
/// Variable request/result/property subtrees are charged separately.
const FIXED_ROW_BYTES: usize = 1024;
const EXECUTION_ROWS: usize = 1 + MAX_ATTEMPTS as usize * 2 + EFFECT_HEADROOM;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    view: OperationView,
    original_request: Request,
}

/// All durable facts, including uncertain effects and charged reservations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    epoch: ClusterEpoch,
    limits: Limits,
    now: u64,
    next_execution: u64,
    operation_watermark: u64,
    next_attempt: u64,
    next_reservation: u64,
    next_effect: u64,
    operations: BTreeMap<OperationId, Operation>,
    executions: BTreeMap<ExecutionId, ExecutionView>,
    workers: BTreeMap<WorkerId, WorkerView>,
    holds: BTreeMap<WorkerId, HoldView>,
    attempts: BTreeMap<AttemptId, AttemptView>,
    reservations: BTreeMap<ReservationId, ReservationView>,
    effects: BTreeMap<EffectId, Effect>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot<T = State> {
    version: u32,
    state: T,
}

/// Defense against an omitted future field in the credit proof. No encoded
/// snapshot bytes are exposed if serialization exceeds the fixed limit.
#[derive(Default)]
struct SnapshotBytes {
    bytes: Vec<u8>,
}

impl Write for SnapshotBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let size = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_STATE_BYTES)
            .ok_or_else(|| io::Error::other("authority snapshot exceeds byte bound"))?;
        if size > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(size)
                .clamp(1024, MAX_STATE_BYTES);
            self.bytes
                .try_reserve_exact(capacity - self.bytes.len())
                .map_err(io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Count the exact encoded payload without allocating a serialized copy before
/// discovering that it exceeds admission's byte bound.
struct PayloadSizeLimit {
    used: usize,
    limit: usize,
}

impl Write for PayloadSizeLimit {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let Some(used) = self
            .used
            .checked_add(bytes.len())
            .filter(|used| *used <= self.limit)
        else {
            return Err(io::Error::other("authority payload exceeds byte bound"));
        };
        self.used = used;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

const fn fits(required: Resources, capacity: Resources) -> bool {
    required.cpu_millis <= capacity.cpu_millis
        && required.memory_bytes <= capacity.memory_bytes
        && required.slots <= capacity.slots
}

const fn remaining(capacity: Resources, charged: Resources) -> Resources {
    Resources {
        cpu_millis: capacity.cpu_millis.saturating_sub(charged.cpu_millis),
        memory_bytes: capacity.memory_bytes.saturating_sub(charged.memory_bytes),
        slots: capacity.slots.saturating_sub(charged.slots),
    }
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

const fn phase_rank(phase: Phase) -> Option<u8> {
    match phase {
        Phase::Dispatching => Some(0),
        Phase::Accepted => Some(1),
        Phase::Preparing => Some(2),
        Phase::Running => Some(3),
        Phase::Uploading => Some(4),
        _ => None,
    }
}

impl State {
    /// Initialize only limits whose largest accepted payload can be committed.
    /// Zero ceilings are valid and may deliberately prevent admission.
    pub fn new(epoch: ClusterEpoch, limits: Limits) -> Result<Self, String> {
        if limits.max_payload_bytes > MAX_PAYLOAD_BYTES {
            return Err("authority payload limit exceeds fixed command byte bound".to_owned());
        }
        Ok(Self {
            epoch,
            limits,
            now: 0,
            next_execution: 1,
            operation_watermark: 0,
            next_attempt: 1,
            next_reservation: 1,
            next_effect: 1,
            operations: BTreeMap::new(),
            executions: BTreeMap::new(),
            workers: BTreeMap::new(),
            holds: BTreeMap::new(),
            attempts: BTreeMap::new(),
            reservations: BTreeMap::new(),
            effects: BTreeMap::new(),
        })
    }

    /// Apply exactly one committed event. Rejections and ignored events leave
    /// state unchanged. No external response may precede this boundary.
    pub fn apply(&mut self, recorded: RecordedEvent) -> Reply {
        if recorded.epoch != self.epoch {
            return Reply::Rejected(Rejection::WrongEpoch);
        }
        let reply = self.apply_event(recorded.event);
        if matches!(
            reply,
            Reply::Applied | Reply::Submitted { .. } | Reply::Matched { .. }
        ) {
            self.reconcile_holds();
        }
        reply
    }

    fn apply_event(&mut self, event: Event) -> Reply {
        match event {
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
                let Some(entry) = self.workers.get_mut(&worker) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if entry.incarnation != incarnation || entry.session != session {
                    return Reply::Rejected(Rejection::Stale);
                }
                entry.capacity = capacity;
                entry.available = available;
                entry.draining = draining;
                Reply::Applied
            }
            Event::Heartbeat {
                worker,
                incarnation,
                session,
            } => {
                let Some(entry) = self.workers.get_mut(&worker) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if entry.incarnation != incarnation || entry.session != session {
                    return Reply::Rejected(Rejection::Stale);
                }
                entry.last_heartbeat = self.now;
                Reply::Applied
            }
            Event::WorkerUnavailable {
                worker,
                incarnation,
                session,
            } => {
                let Some(entry) = self.workers.get(&worker) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if entry.incarnation != incarnation || entry.session != session {
                    return Reply::Rejected(Rejection::Stale);
                }
                let attempts: Vec<_> = self
                    .attempts
                    .values()
                    .filter(|attempt| attempt.worker == worker && attempt.authorized)
                    .map(|attempt| attempt.id)
                    .collect();
                if entry.draining && attempts.is_empty() {
                    return Reply::Ignored;
                }
                self.workers
                    .get_mut(&worker)
                    .expect("known worker")
                    .draining = true;
                for attempt in attempts {
                    self.revoke(attempt, false);
                }
                Reply::Applied
            }
            Event::Submit { operation, request } => self.submit(operation, request),
            Event::LookupResult { effect, result } => self.lookup_result(effect, result),
            Event::Match => self.match_one(),
            Event::WorkerAccepted {
                attempt,
                worker,
                incarnation,
            } => self.worker_accepted(attempt, worker, incarnation),
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
                if let Err(error) = self.authorized_attempt(attempt, worker, incarnation) {
                    return Reply::Rejected(error);
                }
                if self.attempts[&attempt].phase != Phase::Dispatching {
                    return Reply::Rejected(Rejection::Stale);
                }
                // A confirmed refusal is fresh evidence that the previous
                // capacity observation is unusable. Require an explicit fresh
                // observation before offering more work to this worker.
                self.workers
                    .get_mut(&worker)
                    .expect("attempt worker")
                    .available = Resources::default();
                self.revoke(attempt, true);
                Reply::Applied
            }
            Event::Cancel { operation } => self.cancel(operation),
            Event::Observe { operation } => self
                .operations
                .get(&operation)
                .map_or(Reply::Rejected(Rejection::Unknown), |operation| {
                    Reply::Observed(operation.view.clone())
                }),
            Event::RenewInterest { operation } => {
                let Some(entry) = self.operations.get_mut(&operation) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if entry.view.terminal.is_some() {
                    return Reply::Ignored;
                }
                let Some(generation) = entry.view.interest_deadline.generation.checked_add(1)
                else {
                    return Reply::Rejected(Rejection::Invalid);
                };
                entry.view.interest_deadline = Deadline {
                    at: self.now.saturating_add(INTEREST_TIMEOUT),
                    generation,
                };
                Reply::Applied
            }
            Event::AdvanceTime { now } => {
                if now < self.now {
                    return Reply::Rejected(Rejection::Stale);
                }
                if now == self.now {
                    return Reply::Ignored;
                }
                self.now = now;
                Reply::Applied
            }
            Event::ExpireAttempt {
                attempt,
                generation,
            } => {
                let Some(entry) = self.attempts.get(&attempt) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if !entry.authorized || entry.deadline.generation != generation {
                    return Reply::Ignored;
                }
                let deadline = if entry.phase == Phase::Running {
                    entry
                        .running_deadline
                        .map_or(entry.deadline.at, |at| at.min(entry.deadline.at))
                } else {
                    entry.deadline.at
                };
                if self.now < deadline {
                    return Reply::Rejected(Rejection::TooEarly);
                }
                self.revoke(attempt, false);
                Reply::Applied
            }
            Event::ExpireExecution { attempt } => {
                let Some(entry) = self.attempts.get(&attempt) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if !entry.authorized || entry.phase != Phase::Running {
                    return Reply::Ignored;
                }
                let Some(deadline) = entry.running_deadline else {
                    return Reply::Ignored;
                };
                if self.now < deadline {
                    return Reply::Rejected(Rejection::TooEarly);
                }
                self.revoke(attempt, false);
                Reply::Applied
            }
            Event::ExpireInterest {
                operation,
                generation,
            } => {
                let Some(entry) = self.operations.get(&operation) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if entry.view.terminal.is_some()
                    || entry.view.interest_deadline.generation != generation
                {
                    return Reply::Ignored;
                }
                if self.now < entry.view.interest_deadline.at {
                    return Reply::Rejected(Rejection::TooEarly);
                }
                self.cancel(operation)
            }
            Event::Cleanup {
                attempt,
                worker,
                incarnation,
            } => {
                let Some(entry) = self.attempts.get(&attempt) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if entry.worker != worker || entry.incarnation != incarnation {
                    return Reply::Rejected(Rejection::Stale);
                }
                if entry.authorized {
                    return Reply::Rejected(Rejection::Busy);
                }
                let reservation = self
                    .reservations
                    .get_mut(&entry.reservation)
                    .expect("attempt reservation");
                if reservation.released {
                    return Reply::Ignored;
                }
                reservation.released = true;
                self.drop_attempt_effects(attempt, true);
                Reply::Applied
            }
            Event::EffectOutcome { effect, success } => {
                let Some(entry) = self.effects.get(&effect) else {
                    return Reply::Rejected(Rejection::Unknown);
                };
                if !success || !matches!(entry.kind, EffectKind::PublishResult { .. }) {
                    return Reply::Ignored;
                }
                self.effects.remove(&effect);
                Reply::Applied
            }
            Event::RetireExecution { execution } => self.retire(execution),
        }
    }

    fn payload_fits<T: Serialize>(&self, value: &T) -> bool {
        self.payload_bytes(value).is_some()
    }

    fn payload_bytes<T: Serialize>(&self, value: &T) -> Option<usize> {
        let mut count = PayloadSizeLimit {
            used: 0,
            limit: self.limits.max_payload_bytes,
        };
        serde_json::to_writer(&mut count, value).ok()?;
        Some(count.used)
    }

    /// Every admitted lifecycle prepays its maximum terminal copies and rows.
    /// Count from immutable durable facts rather than maintaining a second
    /// mutable accounting ledger. See design/categorical/byte-headroom.md.
    fn byte_credit(&self) -> Option<usize> {
        let operations = self.operations.len();
        let executions = self.executions.len();
        let rows = operations
            .checked_add(executions.checked_mul(EXECUTION_ROWS)?)?
            .checked_add(self.workers.len().checked_mul(2)?)?
            .checked_add(1)?;
        let results = operations.checked_add(executions.checked_mul(2)?)?;
        let mut credit = rows
            .checked_mul(FIXED_ROW_BYTES)?
            .checked_add(results.checked_mul(self.limits.max_payload_bytes)?)?;
        for operation in self.operations.values() {
            credit = credit.checked_add(self.payload_bytes(&operation.original_request)?)?;
        }
        for execution in self.executions.values() {
            credit = credit.checked_add(self.payload_bytes(&execution.request)?.checked_mul(3)?)?;
        }
        for worker in self.workers.values() {
            credit = credit.checked_add(self.payload_bytes(&worker.properties)?)?;
        }
        Some(credit)
    }

    fn submit_byte_credit(&self, request: &Request, new_execution: bool) -> Option<usize> {
        let request_bytes = self.payload_bytes(request)?;
        let mut credit = self
            .byte_credit()?
            .checked_add(request_bytes)?
            .checked_add(self.limits.max_payload_bytes)?
            .checked_add(FIXED_ROW_BYTES)?;
        if new_execution {
            credit = credit
                .checked_add(request_bytes.checked_mul(3)?)?
                .checked_add(self.limits.max_payload_bytes.checked_mul(2)?)?
                .checked_add(EXECUTION_ROWS.checked_mul(FIXED_ROW_BYTES)?)?;
        }
        Some(credit)
    }

    fn register_byte_credit(
        &self,
        worker: WorkerId,
        properties: &BTreeMap<String, String>,
    ) -> Option<usize> {
        let mut credit = self.byte_credit()?;
        if let Some(old) = self.workers.get(&worker) {
            credit = credit.checked_sub(self.payload_bytes(&old.properties)?)?;
        } else {
            credit = credit.checked_add(FIXED_ROW_BYTES.checked_mul(2)?)?;
        }
        credit.checked_add(self.payload_bytes(properties)?)
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
        if !self.payload_fits(&properties) {
            return Reply::Rejected(Rejection::Invalid);
        }
        if let Some(old) = self.workers.get(&worker) {
            if incarnation < old.incarnation
                || (incarnation == old.incarnation && session <= old.session)
            {
                return Reply::Rejected(Rejection::Stale);
            }
        } else if self.workers.len() >= self.limits.max_workers {
            return Reply::Rejected(Rejection::Overloaded);
        }
        if self
            .register_byte_credit(worker, &properties)
            .is_none_or(|credit| credit > MAX_STATE_BYTES)
        {
            return Reply::Rejected(Rejection::Overloaded);
        }
        if self
            .workers
            .get(&worker)
            .is_some_and(|old| incarnation != old.incarnation)
        {
            let revoked: Vec<_> = self
                .attempts
                .values()
                .filter(|attempt| attempt.worker == worker && attempt.authorized)
                .map(|attempt| attempt.id)
                .collect();
            for attempt in revoked {
                self.revoke(attempt, false);
            }
        }
        self.workers.insert(
            worker,
            WorkerView {
                id: worker,
                incarnation,
                session,
                properties,
                capacity,
                available,
                draining: false,
                last_heartbeat: self.now,
                charged: Resources::default(),
                quarantined: false,
            },
        );
        Reply::Applied
    }

    fn submit(&mut self, operation: OperationId, request: Request) -> Reply {
        if let Some(existing) = self.operations.get(&operation) {
            return if existing.original_request == request {
                Reply::Submitted {
                    operation,
                    execution: existing.view.execution,
                }
            } else {
                Reply::Rejected(Rejection::Invalid)
            };
        }
        if operation.0 <= self.operation_watermark {
            return Reply::Rejected(Rejection::Stale);
        }
        if request.resources.slots == 0
            || request.execution_timeout == 0
            || request.action.digest.is_empty()
            || request.action.digest_function.is_empty()
            || !self.payload_fits(&request)
            || request.validate_payload().is_err()
        {
            return Reply::Rejected(Rejection::Invalid);
        }
        if self.operations.len() >= self.limits.max_operations {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let joining = self
            .executions
            .values()
            .find(|execution| {
                execution.terminal.is_none()
                    && compatible(&request, &execution.request)
                    && !(request.skip_cache_lookup && execution.phase == Phase::Lookup)
            })
            .map(|execution| execution.id);
        if self
            .submit_byte_credit(&request, joining.is_none())
            .is_none_or(|credit| credit > MAX_STATE_BYTES)
        {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let execution = if let Some(execution) = joining {
            execution
        } else {
            if self.executions.len() >= self.limits.max_executions
                || self
                    .executions
                    .len()
                    .saturating_add(1)
                    .saturating_mul(EFFECT_HEADROOM)
                    > self.limits.max_effects
                || self.next_execution == u64::MAX
                || !self.identities_have_headroom(true)
            {
                return Reply::Rejected(Rejection::Overloaded);
            }
            let execution = ExecutionId(self.next_execution);
            self.next_execution += 1;
            let phase = if !request.do_not_cache && !request.skip_cache_lookup {
                Phase::Lookup
            } else {
                Phase::Queued
            };
            self.executions.insert(
                execution,
                ExecutionView {
                    id: execution,
                    request: request.clone(),
                    reservation_target: request.resources,
                    phase,
                    effective_priority: Some(request.priority),
                    enqueue_ordinal: execution.0,
                    enqueue_at: self.now,
                    hold_cooldown_until: 0,
                    attempt: None,
                    attempts_started: 0,
                    terminal: None,
                    terminal_at: None,
                },
            );
            if phase == Phase::Lookup {
                self.emit(EffectKind::Lookup {
                    execution,
                    action: request.action.clone(),
                });
            }
            execution
        };
        self.operation_watermark = operation.0;
        self.operations.insert(
            operation,
            Operation {
                view: OperationView {
                    id: operation,
                    execution,
                    priority: request.priority,
                    interested: true,
                    terminal: None,
                    version: 1,
                    interest_deadline: Deadline {
                        at: self.now.saturating_add(INTEREST_TIMEOUT),
                        generation: 1,
                    },
                },
                original_request: request,
            },
        );
        self.recompute_priority(execution);
        Reply::Submitted {
            operation,
            execution,
        }
    }

    fn lookup_result(&mut self, effect: EffectId, result: Option<ResultPayload>) -> Reply {
        let Some(intent) = self.effects.get(&effect) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        let EffectKind::Lookup { execution, .. } = intent.kind else {
            return Reply::Rejected(Rejection::Invalid);
        };
        if result
            .as_ref()
            .is_some_and(|result| !self.payload_fits(result) || result.validate_response().is_err())
        {
            return Reply::Rejected(Rejection::Invalid);
        }
        if self.executions[&execution].phase != Phase::Lookup {
            return Reply::Rejected(Rejection::Stale);
        }
        self.effects.remove(&effect);
        if let Some(result) = result {
            self.terminal(execution, TerminalResult::Completed(result));
        } else {
            self.set_phase(execution, Phase::Queued);
        }
        Reply::Applied
    }

    fn queue_key(&self, execution: ExecutionId) -> (i32, u64, ExecutionId) {
        let entry = &self.executions[&execution];
        (
            entry.effective_priority.expect("queued interest"),
            entry.enqueue_ordinal,
            execution,
        )
    }

    fn worker_usable(&self, worker: &WorkerView) -> bool {
        let (charged, quarantined) = self.worker_accounting(worker.id);
        !worker.draining
            && !quarantined
            && worker.capacity.slots > 0
            && worker.available.slots > 0
            && fits(charged, worker.capacity)
            && fits(charged, worker.available)
    }

    fn hold_cap(&self) -> usize {
        let usable = self
            .workers
            .values()
            .filter(|worker| self.worker_usable(worker))
            .count();
        if usable == 0 { 0 } else { (usable / 10).max(1) }
    }

    /// Unknown physical termination is not a future completion. These original
    /// charges are an immovable floor when deciding whether a hold can help.
    fn immovable_charge(&self, worker: WorkerId) -> Resources {
        self.reservations
            .values()
            .filter(|reservation| {
                reservation.worker == worker
                    && !reservation.released
                    && !self.attempts[&reservation.attempt].authorized
            })
            .fold(Resources::default(), |total, reservation| Resources {
                cpu_millis: total
                    .cpu_millis
                    .checked_add(reservation.charged.cpu_millis)
                    .expect("bounded CPU charge"),
                memory_bytes: total
                    .memory_bytes
                    .checked_add(reservation.charged.memory_bytes)
                    .expect("bounded memory charge"),
                slots: total
                    .slots
                    .checked_add(reservation.charged.slots)
                    .expect("bounded slot charge"),
            })
    }

    fn hold_hard_fit(&self, execution: ExecutionId, worker: &WorkerView) -> bool {
        let request = &self.executions[&execution].request;
        let target = self.executions[&execution].reservation_target;
        let floor = self.immovable_charge(worker.id);
        self.worker_usable(worker)
            && request
                .properties
                .iter()
                .all(|(key, value)| worker.properties.get(key) == Some(value))
            && fits(target, remaining(worker.capacity, floor))
            && fits(target, remaining(worker.available, floor))
    }

    fn hold_permits(&self, execution: ExecutionId, worker: WorkerId) -> bool {
        self.holds
            .get(&worker)
            .is_none_or(|hold| self.queue_key(execution) <= self.queue_key(hold.execution))
    }

    fn consider_hold(&mut self, execution: ExecutionId) -> bool {
        let entry = &self.executions[&execution];
        if self.now.saturating_sub(entry.enqueue_at) < HOLD_AFTER
            || self.now < entry.hold_cooldown_until
            || self.holds.values().any(|hold| hold.execution == execution)
            || self.holds.len() >= self.hold_cap()
        {
            return false;
        }
        let Some(until) = self.now.checked_add(HOLD_DURATION) else {
            return false;
        };
        let mut candidates = Vec::new();
        for worker in self.workers.values() {
            if !self.hold_hard_fit(execution, worker) || !self.hold_permits(execution, worker.id) {
                continue;
            }
            let (charged, _) = self.worker_accounting(worker.id);
            let count = self
                .reservations
                .values()
                .filter(|reservation| reservation.worker == worker.id && !reservation.released)
                .count();
            // Even one expected completion cannot justify backfilling the
            // partial free capacity that the aged request needs to accumulate.
            if count > 0 && !self.holds.contains_key(&worker.id) {
                let free_memory = remaining(worker.capacity, charged)
                    .memory_bytes
                    .min(remaining(worker.available, charged).memory_bytes);
                candidates.push((count, Reverse(free_memory), worker.id));
            }
        }
        let Some((_, _, worker)) = candidates.into_iter().min() else {
            return false;
        };
        self.holds.insert(
            worker,
            HoldView {
                execution,
                worker,
                incarnation: self.workers[&worker].incarnation,
                started_at: self.now,
                until,
            },
        );
        true
    }

    /// Run only after an accepted state change. Rejected/stale events cannot
    /// expire a hold, rekey its owner, or change a cooldown as a side effect.
    fn reconcile_holds(&mut self) {
        let workers: Vec<_> = self.holds.keys().copied().collect();
        for worker in workers {
            let hold = self.holds[&worker];
            let valid = self
                .executions
                .get(&hold.execution)
                .is_some_and(|execution| {
                    execution.phase == Phase::Queued && execution.effective_priority.is_some()
                })
                && self.workers.get(&worker).is_some_and(|entry| {
                    entry.incarnation == hold.incarnation
                        && self.hold_hard_fit(hold.execution, entry)
                });
            if !valid {
                self.holds.remove(&worker);
            } else if hold.until <= self.now {
                self.holds.remove(&worker);
                self.executions
                    .get_mut(&hold.execution)
                    .expect("hold execution")
                    .hold_cooldown_until = self.now.saturating_add(HOLD_COOLDOWN);
            }
        }
        let mut ranked: Vec<_> = self
            .holds
            .values()
            .map(|hold| (self.queue_key(hold.execution), hold.worker))
            .collect();
        ranked.sort_unstable();
        for (_, worker) in ranked.into_iter().skip(self.hold_cap()) {
            self.holds.remove(&worker);
        }
    }

    fn match_one(&mut self) -> Reply {
        let mut queue: Vec<_> = self
            .executions
            .values()
            .filter(|execution| execution.phase == Phase::Queued)
            .map(|execution| {
                (
                    execution.effective_priority.expect("queued interest"),
                    execution.enqueue_ordinal,
                    execution.id,
                )
            })
            .collect();
        queue.sort_unstable();
        if queue.is_empty() {
            return Reply::Ignored;
        }
        // Preflight before creating any holds, preserving rejection atomicity.
        if self.next_attempt == u64::MAX || self.next_reservation == u64::MAX {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let mut held = false;
        for (_, _, execution) in queue {
            let request = &self.executions[&execution].request;
            let target = self.executions[&execution].reservation_target;
            let worker = self
                .workers
                .values()
                .find(|worker| {
                    let (charged, quarantined) = self.worker_accounting(worker.id);
                    !worker.draining
                        && !quarantined
                        && self.hold_permits(execution, worker.id)
                        && fits(charged, worker.capacity)
                        && fits(charged, worker.available)
                        && request
                            .properties
                            .iter()
                            .all(|(key, value)| worker.properties.get(key) == Some(value))
                        && fits(target, remaining(worker.capacity, charged))
                        && fits(target, remaining(worker.available, charged))
                })
                .map(|worker| worker.id);
            let Some(worker) = worker else {
                held |= self.consider_hold(execution);
                continue;
            };
            let request = request.clone();
            let incarnation = self.workers[&worker].incarnation;
            let attempt = AttemptId(self.next_attempt);
            self.next_attempt += 1;
            let reservation = ReservationId(self.next_reservation);
            self.next_reservation += 1;
            self.attempts.insert(
                attempt,
                AttemptView {
                    id: attempt,
                    execution,
                    worker,
                    incarnation,
                    reservation,
                    authorized: true,
                    phase: Phase::Dispatching,
                    progress: 0,
                    deadline: Deadline {
                        at: self.now.saturating_add(ACK_TIMEOUT),
                        generation: 1,
                    },
                    running_deadline: None,
                },
            );
            self.reservations.insert(
                reservation,
                ReservationView {
                    id: reservation,
                    attempt,
                    worker,
                    incarnation,
                    charged: target,
                    released: false,
                },
            );
            let entry = self
                .executions
                .get_mut(&execution)
                .expect("queued execution");
            entry.attempt = Some(attempt);
            entry.attempts_started += 1;
            entry.hold_cooldown_until = 0;
            self.holds.retain(|_, hold| hold.execution != execution);
            self.set_phase(execution, Phase::Dispatching);
            self.emit(EffectKind::Dispatch {
                execution,
                attempt,
                worker,
                incarnation,
                reservation,
                charged: target,
                request,
            });
            return Reply::Matched {
                execution,
                attempt,
                worker,
            };
        }
        if held { Reply::Applied } else { Reply::Ignored }
    }

    fn authorized_attempt(
        &self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    ) -> Result<(), Rejection> {
        let entry = self.attempts.get(&attempt).ok_or(Rejection::Unknown)?;
        if !entry.authorized
            || entry.worker != worker
            || entry.incarnation != incarnation
            || self
                .workers
                .get(&worker)
                .is_none_or(|worker| worker.incarnation != incarnation)
            || self
                .executions
                .get(&entry.execution)
                .is_none_or(|execution| {
                    execution.attempt != Some(attempt) || execution.terminal.is_some()
                })
        {
            return Err(Rejection::Stale);
        }
        Ok(())
    }

    fn worker_accepted(
        &mut self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    ) -> Reply {
        if let Err(error) = self.authorized_attempt(attempt, worker, incarnation) {
            return Reply::Rejected(error);
        }
        let entry = self.attempts.get_mut(&attempt).expect("authorized attempt");
        if entry.phase != Phase::Dispatching {
            return Reply::Ignored;
        }
        entry.phase = Phase::Accepted;
        entry.deadline = Deadline {
            at: self.now.saturating_add(PROGRESS_TIMEOUT),
            generation: entry.deadline.generation + 1,
        };
        let execution = entry.execution;
        self.drop_attempt_effects(attempt, false);
        self.set_phase(execution, Phase::Accepted);
        Reply::Applied
    }

    fn progress(
        &mut self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        phase: Phase,
        progress: u64,
    ) -> Reply {
        if let Err(error) = self.authorized_attempt(attempt, worker, incarnation) {
            return Reply::Rejected(error);
        }
        let entry = &self.attempts[&attempt];
        if !matches!(phase, Phase::Preparing | Phase::Running | Phase::Uploading)
            || entry.phase == Phase::Dispatching
            || phase_rank(phase) < phase_rank(entry.phase)
        {
            return Reply::Rejected(Rejection::Invalid);
        }
        if entry.phase == phase && progress <= entry.progress {
            return Reply::Ignored;
        }
        let Some(generation) = entry.deadline.generation.checked_add(1) else {
            return Reply::Rejected(Rejection::Invalid);
        };
        let execution = entry.execution;
        let execution_timeout = self.executions[&execution].request.execution_timeout;
        let entry = self.attempts.get_mut(&attempt).expect("authorized attempt");
        if phase == Phase::Running && entry.running_deadline.is_none() {
            entry.running_deadline = Some(self.now.saturating_add(execution_timeout));
        }
        entry.phase = phase;
        entry.progress = progress;
        entry.deadline = Deadline {
            at: self.now.saturating_add(PROGRESS_TIMEOUT),
            generation,
        };
        self.set_phase(execution, phase);
        Reply::Applied
    }

    fn complete(
        &mut self,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        result: ResultPayload,
    ) -> Reply {
        if let Err(error) = self.authorized_attempt(attempt, worker, incarnation) {
            return Reply::Rejected(error);
        }
        if !self.payload_fits(&result) || result.validate_response().is_err() {
            return Reply::Rejected(Rejection::Invalid);
        }
        let entry = self.attempts.get_mut(&attempt).expect("authorized attempt");
        entry.authorized = false;
        let execution = entry.execution;
        self.reservations
            .get_mut(&entry.reservation)
            .expect("attempt reservation")
            .released = true;
        self.drop_attempt_effects(attempt, false);
        let request = self.executions[&execution].request.clone();
        self.terminal(execution, TerminalResult::Completed(result.clone()));
        if !request.do_not_cache && result.exit_code == 0 && result.error.is_none() {
            self.emit(EffectKind::PublishResult {
                execution,
                action: request.action,
                result,
            });
        }
        Reply::Applied
    }

    /// A declared ceiling is a possible future allocation on one actual worker,
    /// not a free-capacity sample or a per-dimension maximum across the fleet.
    /// Called only after releasing the confirmed-quiescent failed attempt.
    fn memory_growth_ceiling(&self, execution: ExecutionId) -> u64 {
        let entry = &self.executions[&execution];
        self.workers
            .values()
            .filter_map(|worker| {
                let (charged, quarantined) = self.worker_accounting(worker.id);
                if worker.draining
                    || quarantined
                    || !fits(charged, worker.capacity)
                    || !entry
                        .request
                        .properties
                        .iter()
                        .all(|(key, value)| worker.properties.get(key) == Some(value))
                {
                    return None;
                }
                let floor = self.immovable_charge(worker.id);
                let ceiling = Resources {
                    cpu_millis: worker.capacity.cpu_millis.checked_sub(floor.cpu_millis)?,
                    memory_bytes: worker
                        .capacity
                        .memory_bytes
                        .checked_sub(floor.memory_bytes)?,
                    slots: worker.capacity.slots.checked_sub(floor.slots)?,
                };
                (entry.reservation_target.cpu_millis <= ceiling.cpu_millis
                    && entry.reservation_target.slots <= ceiling.slots)
                    .then_some(ceiling.memory_bytes)
            })
            .max()
            .unwrap_or(0)
    }

    fn memory_failure(
        &mut self,
        identity: (AttemptId, WorkerId, IncarnationId),
        charged: Resources,
        peak_memory_bytes: Option<u64>,
        quiescent: bool,
        result: ResultPayload,
    ) -> Reply {
        let (attempt, worker, incarnation) = identity;
        if let Err(error) = self.authorized_attempt(attempt, worker, incarnation) {
            return Reply::Rejected(error);
        }
        let entry = self.attempts[&attempt];
        if !quiescent
            || self.reservations[&entry.reservation].charged != charged
            || (result.exit_code == 0 && result.error.is_none())
            || !self.payload_fits(&result)
            || result.validate_response().is_err()
        {
            return Reply::Rejected(Rejection::Invalid);
        }

        // Validation ends before the first mutation. This accepted report closes
        // exactly its original physical obligation, never a successor's target.
        self.attempts
            .get_mut(&attempt)
            .expect("authorized attempt")
            .authorized = false;
        self.reservations
            .get_mut(&entry.reservation)
            .expect("attempt reservation")
            .released = true;
        self.drop_attempt_effects(attempt, false);
        self.executions
            .get_mut(&entry.execution)
            .expect("attempt execution")
            .attempt = None;

        let original = charged.memory_bytes;
        let base = if original == 0 {
            peak_memory_bytes.unwrap_or(0)
        } else {
            original
        };
        let ceiling = self.memory_growth_ceiling(entry.execution);
        // This comparison avoids overflowing even for u64::MAX bases/ceilings.
        let grown = if base > ceiling / 2 {
            ceiling
        } else {
            base * 2
        };
        let reason = if self.executions[&entry.execution].attempts_started >= MAX_ATTEMPTS {
            Some(ResourceFailureReason::AttemptBudgetExhausted)
        } else if base == 0 {
            Some(ResourceFailureReason::MissingGrowthEvidence)
        } else if grown <= original {
            Some(ResourceFailureReason::NoLargerEligibleWorker)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.terminal(
                entry.execution,
                TerminalResult::ResourceFailed { result, reason },
            );
        } else {
            self.executions
                .get_mut(&entry.execution)
                .expect("attempt execution")
                .reservation_target
                .memory_bytes = grown;
            self.set_phase(entry.execution, Phase::Queued);
        }
        Reply::Applied
    }

    fn cancel(&mut self, operation: OperationId) -> Reply {
        let Some(entry) = self.operations.get_mut(&operation) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        if entry.view.terminal.is_some() {
            return Reply::Ignored;
        }
        entry.view.terminal = Some(TerminalResult::Cancelled);
        entry.view.interested = false;
        entry.view.version += 1;
        let execution = entry.view.execution;
        self.recompute_priority(execution);
        if self.executions[&execution].effective_priority.is_none() {
            if let Some(attempt) = self.executions[&execution].attempt {
                let entry = self.attempts.get_mut(&attempt).expect("execution attempt");
                entry.authorized = false;
                let (worker, incarnation) = (entry.worker, entry.incarnation);
                self.drop_attempt_effects(attempt, false);
                self.emit(EffectKind::Cancel {
                    execution,
                    attempt,
                    worker,
                    incarnation,
                });
            }
            self.effects.retain(|_, effect| !matches!(effect.kind, EffectKind::Lookup { execution: target, .. } if target == execution));
            self.terminal(execution, TerminalResult::Cancelled);
        }
        Reply::Applied
    }

    fn revoke(&mut self, attempt: AttemptId, quiescent: bool) {
        let entry = self.attempts.get_mut(&attempt).expect("revoked attempt");
        entry.authorized = false;
        let (execution, worker, incarnation) = (entry.execution, entry.worker, entry.incarnation);
        if quiescent {
            self.reservations
                .get_mut(&entry.reservation)
                .expect("attempt reservation")
                .released = true;
        }
        self.drop_attempt_effects(attempt, false);
        if !quiescent {
            self.emit(EffectKind::Cancel {
                execution,
                attempt,
                worker,
                incarnation,
            });
        }
        let entry = self
            .executions
            .get_mut(&execution)
            .expect("attempt execution");
        entry.attempt = None;
        if entry.attempts_started >= MAX_ATTEMPTS {
            self.terminal(execution, TerminalResult::Exhausted);
        } else {
            self.set_phase(execution, Phase::Queued);
        }
    }

    fn recompute_priority(&mut self, execution: ExecutionId) {
        self.executions
            .get_mut(&execution)
            .expect("operation execution")
            .effective_priority = self
            .operations
            .values()
            .filter(|operation| operation.view.execution == execution && operation.view.interested)
            .map(|operation| operation.view.priority)
            .min();
    }

    fn set_phase(&mut self, execution: ExecutionId, phase: Phase) {
        let entry = self.executions.get_mut(&execution).expect("execution");
        if entry.phase == phase {
            return;
        }
        entry.phase = phase;
        for operation in self
            .operations
            .values_mut()
            .filter(|operation| operation.view.execution == execution && operation.view.interested)
        {
            operation.view.version += 1;
        }
    }

    fn terminal(&mut self, execution: ExecutionId, result: TerminalResult) {
        let entry = self
            .executions
            .get_mut(&execution)
            .expect("terminal execution");
        entry.phase = Phase::Terminal;
        entry.attempt = None;
        entry.terminal_at = Some(self.now);
        entry.effective_priority = None;
        for operation in self
            .operations
            .values_mut()
            .filter(|operation| operation.view.execution == execution && operation.view.interested)
        {
            operation.view.interested = false;
            operation.view.terminal = Some(result.clone());
            operation.view.version += 1;
        }
        entry.terminal = Some(result);
    }

    fn emit(&mut self, kind: EffectKind) {
        let id = EffectId(self.next_effect);
        // Admission reserves all possible effects for this bounded lifecycle.
        self.next_effect = self
            .next_effect
            .checked_add(1)
            .expect("effect identity exhausted");
        self.effects.insert(
            id,
            Effect {
                id,
                epoch: self.epoch,
                kind,
            },
        );
    }

    fn drop_attempt_effects(&mut self, target: AttemptId, include_cancel: bool) {
        self.effects.retain(|_, effect| match effect.kind {
            EffectKind::Dispatch { attempt, .. } => attempt != target,
            EffectKind::Cancel { attempt, .. } if include_cancel => attempt != target,
            _ => true,
        });
    }

    // Counter exhaustion must reject admission while there is still enough
    // identity space for every admitted execution's remaining finalization.
    fn identities_have_headroom(&self, admitting: bool) -> bool {
        let mut attempts_left = u64::from(admitting) * u64::from(MAX_ATTEMPTS);
        let mut effects_left = u64::from(admitting) * EFFECT_HEADROOM as u64;
        for execution in self
            .executions
            .values()
            .filter(|entry| entry.terminal.is_none())
        {
            let remaining = u64::from(MAX_ATTEMPTS.saturating_sub(execution.attempts_started));
            let Some(attempts) = attempts_left.checked_add(remaining) else {
                return false;
            };
            attempts_left = attempts;
            let effects = remaining * 2
                + u64::from(execution.attempt.is_some())
                + u64::from(!execution.request.do_not_cache);
            let Some(total) = effects_left.checked_add(effects) else {
                return false;
            };
            effects_left = total;
        }
        self.next_attempt.checked_add(attempts_left).is_some()
            && self.next_reservation.checked_add(attempts_left).is_some()
            && self.next_effect.checked_add(effects_left).is_some()
    }

    fn worker_accounting(&self, worker: WorkerId) -> (Resources, bool) {
        let incarnation = self.workers[&worker].incarnation;
        self.reservations
            .values()
            .filter(|reservation| reservation.worker == worker && !reservation.released)
            .fold(
                (Resources::default(), false),
                |(charged, quarantined), reservation| {
                    (
                        Resources {
                            cpu_millis: charged
                                .cpu_millis
                                .checked_add(reservation.charged.cpu_millis)
                                .expect("bounded CPU charge"),
                            memory_bytes: charged
                                .memory_bytes
                                .checked_add(reservation.charged.memory_bytes)
                                .expect("bounded memory charge"),
                            slots: charged
                                .slots
                                .checked_add(reservation.charged.slots)
                                .expect("bounded slot charge"),
                        },
                        quarantined || reservation.incarnation != incarnation,
                    )
                },
            )
    }

    fn retire(&mut self, execution: ExecutionId) -> Reply {
        let Some(entry) = self.executions.get(&execution) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        let Some(at) = entry.terminal_at else {
            return Reply::Rejected(Rejection::Busy);
        };
        if self.now < at.saturating_add(TERMINAL_RETENTION) {
            return Reply::Rejected(Rejection::TooEarly);
        }
        let attempts: BTreeSet<_> = self
            .attempts
            .values()
            .filter(|attempt| attempt.execution == execution)
            .map(|attempt| attempt.id)
            .collect();
        if self
            .reservations
            .values()
            .any(|reservation| attempts.contains(&reservation.attempt) && !reservation.released)
            || self
                .effects
                .values()
                .any(|effect| effect_execution(effect) == execution)
        {
            return Reply::Rejected(Rejection::Busy);
        }
        self.executions.remove(&execution);
        self.operations
            .retain(|_, operation| operation.view.execution != execution);
        self.attempts.retain(|id, _| !attempts.contains(id));
        self.reservations
            .retain(|_, reservation| !attempts.contains(&reservation.attempt));
        Reply::Applied
    }

    /// Canonical public state for refinement comparisons. Derived accounting is
    /// recomputed from original reservations, never from worker advertisements.
    #[must_use]
    pub fn view(&self) -> ObservableState {
        ObservableState {
            epoch: self.epoch,
            now: self.now,
            operations: self
                .operations
                .values()
                .map(|operation| operation.view.clone())
                .collect(),
            executions: self.executions.values().cloned().collect(),
            workers: self
                .workers
                .values()
                .map(|worker| {
                    let mut worker = worker.clone();
                    (worker.charged, worker.quarantined) = self.worker_accounting(worker.id);
                    worker
                })
                .collect(),
            holds: self.holds.values().copied().collect(),
            attempts: self.attempts.values().copied().collect(),
            reservations: self.reservations.values().copied().collect(),
            effects: self.effects.values().cloned().collect(),
        }
    }

    /// Serialize canonical state including IDs and the complete pending outbox.
    pub fn snapshot(&self) -> Result<Vec<u8>, String> {
        self.check_invariants()?;
        let mut snapshot = SnapshotBytes::default();
        serde_json::to_writer(
            &mut snapshot,
            &Snapshot {
                version: SNAPSHOT_VERSION,
                state: self,
            },
        )
        .map_err(|error| error.to_string())?;
        Ok(snapshot.bytes)
    }

    /// Restore only the declared experimental format and validate its relations.
    pub fn restore(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_STATE_BYTES {
            return Err("authority snapshot exceeds fixed byte bound".to_owned());
        }
        let snapshot: Snapshot =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if snapshot.version != SNAPSHOT_VERSION {
            return Err(format!(
                "unsupported authority snapshot version {}",
                snapshot.version
            ));
        }
        snapshot.state.check_invariants()?;
        Ok(snapshot.state)
    }

    /// Check named safety predicates independently of external observations.
    pub fn check_invariants(&self) -> Result<(), String> {
        let fail = |name: &str| Err(name.to_owned());
        if self.limits.max_payload_bytes > MAX_PAYLOAD_BYTES
            || self
                .byte_credit()
                .is_none_or(|credit| credit > MAX_STATE_BYTES)
        {
            return fail("BOUND2: aggregate bytes and prepaid finalization fit fixed limits");
        }
        if self.executions.len() > self.limits.max_executions
            || self.operations.len() > self.limits.max_operations
            || self.workers.len() > self.limits.max_workers
            || self.effects.len() > self.limits.max_effects
            || self.executions.len().saturating_mul(EFFECT_HEADROOM) > self.limits.max_effects
            || self.attempts.len() > self.executions.len().saturating_mul(MAX_ATTEMPTS as usize)
            || self.reservations.len() != self.attempts.len()
        {
            return fail("BOUND1: bounded authority and finalization headroom");
        }
        if self.next_execution == 0
            || self.next_attempt == 0
            || self.next_reservation == 0
            || self.next_effect == 0
            || self.executions.keys().any(|id| id.0 >= self.next_execution)
            || self.attempts.keys().any(|id| id.0 >= self.next_attempt)
            || self
                .reservations
                .keys()
                .any(|id| id.0 >= self.next_reservation)
            || self.effects.keys().any(|id| id.0 >= self.next_effect)
            || self.next_attempt != self.next_reservation
            || !self.identities_have_headroom(false)
        {
            return fail("ID1: identities are monotonic and never reused");
        }
        for (id, execution) in &self.executions {
            if execution.id != *id
                || !self.payload_fits(&execution.request)
                || execution.request.validate_payload().is_err()
                || execution.request.resources.slots == 0
                || execution.request.execution_timeout == 0
                || execution.request.action.digest.is_empty()
                || execution.request.action.digest_function.is_empty()
                || execution.terminal_at.is_some_and(|at| at > self.now)
                || matches!(&execution.terminal, Some(TerminalResult::Completed(result) | TerminalResult::ResourceFailed { result, .. }) if !self.payload_fits(result) || result.validate_response().is_err())
                || matches!(&execution.terminal, Some(TerminalResult::ResourceFailed { result, .. }) if result.exit_code == 0 && result.error.is_none())
            {
                return fail("ID2: execution key and bounded request");
            }
            if execution.reservation_target.cpu_millis != execution.request.resources.cpu_millis
                || execution.reservation_target.slots != execution.request.resources.slots
                || !fits(execution.request.resources, execution.reservation_target)
                || (execution.attempts_started == 0
                    && execution.reservation_target != execution.request.resources)
            {
                return fail("RES4: target preserves immutable minima and fixed CPU/slots");
            }
            let priority = self
                .operations
                .values()
                .filter(|operation| operation.view.execution == *id && operation.view.interested)
                .map(|operation| operation.view.priority)
                .min();
            if execution.effective_priority != priority
                || execution.enqueue_ordinal != id.0
                || execution.enqueue_at > self.now
                || execution.hold_cooldown_until > self.now.saturating_add(HOLD_COOLDOWN)
            {
                return fail("QUEUE1: live minimum priority and immutable FIFO ordinal");
            }
            if execution.terminal.is_some() != (execution.phase == Phase::Terminal)
                || execution.terminal.is_some() != execution.terminal_at.is_some()
                || (execution.terminal.is_some()
                    && (priority.is_some() || execution.attempt.is_some()))
                || (execution.terminal.is_none() && priority.is_none())
            {
                return fail("TERM1: terminal execution and live interest");
            }
            let handles: Vec<_> = self
                .operations
                .values()
                .filter(|operation| operation.view.execution == *id)
                .collect();
            if handles.is_empty()
                || (execution.request.do_not_cache && handles.len() != 1)
                || !handles
                    .iter()
                    .any(|operation| operation.original_request == execution.request)
                || (execution.phase == Phase::Lookup
                    && (execution.request.do_not_cache
                        || execution.request.skip_cache_lookup
                        || execution.attempts_started != 0))
                || (execution.phase == Phase::Queued && execution.attempts_started >= MAX_ATTEMPTS)
            {
                return fail("REQUEST1: immutable request identity and cacheability");
            }
            let attempts: Vec<_> = self
                .attempts
                .values()
                .filter(|attempt| attempt.execution == *id)
                .collect();
            if attempts.len() != execution.attempts_started as usize
                || execution.attempts_started > MAX_ATTEMPTS
                || attempts.iter().filter(|attempt| attempt.authorized).count() > 1
            {
                return fail("AUTH1: bounded attempts and at most one authorized attempt");
            }
            let mut previous_memory = execution.request.resources.memory_bytes;
            for attempt in &attempts {
                let Some(reservation) = self.reservations.get(&attempt.reservation) else {
                    return fail("RES1: attempt reservation exists");
                };
                if reservation.charged.memory_bytes < previous_memory {
                    return fail("RES4: later reservations never decrease accepted memory");
                }
                previous_memory = reservation.charged.memory_bytes;
            }
            if matches!(
                execution.terminal,
                Some(TerminalResult::ResourceFailed { .. })
            ) && attempts
                .last()
                .is_none_or(|attempt| !self.reservations[&attempt.reservation].released)
            {
                return fail("RES4: accepted resource failure confirms final attempt quiescence");
            }
            if let Some(attempt) = execution.attempt {
                if self.attempts.get(&attempt).is_none_or(|attempt| {
                    !attempt.authorized
                        || attempt.execution != *id
                        || attempt.phase != execution.phase
                }) {
                    return fail("AUTH2: current execution owns its attempt");
                }
            } else if !matches!(
                execution.phase,
                Phase::Lookup | Phase::Queued | Phase::Terminal
            ) {
                return fail("AUTH2: active phase requires current attempt");
            }
            let lookups = self.effects.values().filter(|effect| matches!(effect.kind, EffectKind::Lookup { execution: target, .. } if target == *id)).count();
            if lookups != usize::from(execution.phase == Phase::Lookup) {
                return fail("EFFECT1: lookup has exactly one persistent intent");
            }
            let publications = self.effects.values().filter(|effect| matches!(effect.kind, EffectKind::PublishResult { execution: target, .. } if target == *id)).count();
            let owned_effects = self
                .effects
                .values()
                .filter(|effect| effect_execution(effect) == *id)
                .count();
            if publications > 1 || owned_effects > EFFECT_HEADROOM {
                return fail("EFFECT1: one publication and bounded effects per execution");
            }
        }
        for (id, operation) in &self.operations {
            if operation.view.id != *id
                || id.0 > self.operation_watermark
                || !self.executions.contains_key(&operation.view.execution)
                || operation.view.interested == operation.view.terminal.is_some()
                || operation.view.version == 0
                || operation.view.version > u64::from(MAX_ATTEMPTS) * 6 + 3
                || operation.view.interest_deadline.generation == 0
                || !self.payload_fits(&operation.original_request)
                || operation.original_request.validate_payload().is_err()
            {
                return fail("OP1: operation ownership and terminal interest");
            }
            let execution = &self.executions[&operation.view.execution];
            if operation.view.priority != operation.original_request.priority
                || (operation.original_request != execution.request
                    && !compatible(&operation.original_request, &execution.request))
                || (operation.original_request.skip_cache_lookup
                    && execution.phase == Phase::Lookup)
                || matches!(
                    &operation.view.terminal,
                    Some(
                        TerminalResult::Completed(_)
                            | TerminalResult::ResourceFailed { .. }
                            | TerminalResult::Exhausted
                    )
                ) && operation.view.terminal != execution.terminal
            {
                return fail("OP2: handle request and terminal association");
            }
        }
        for (id, attempt) in &self.attempts {
            let Some(execution) = self.executions.get(&attempt.execution) else {
                return fail("AUTH3: attempt execution exists");
            };
            let Some(worker) = self.workers.get(&attempt.worker) else {
                return fail("AUTH3: attempt worker exists");
            };
            let Some(reservation) = self.reservations.get(&attempt.reservation) else {
                return fail("RES1: attempt reservation exists");
            };
            if attempt.id != *id
                || attempt.deadline.generation == 0
                || phase_rank(attempt.phase).is_none()
                || (attempt.phase == Phase::Dispatching && attempt.deadline.generation != 1)
                || (attempt.phase == Phase::Running && attempt.running_deadline.is_none())
                || (matches!(
                    attempt.phase,
                    Phase::Dispatching | Phase::Accepted | Phase::Preparing
                ) && attempt.running_deadline.is_some())
                || (attempt.authorized
                    && (execution.attempt != Some(*id)
                        || worker.incarnation != attempt.incarnation
                        || reservation.released))
            {
                return fail("AUTH3: current attempt and incarnation own live charge");
            }
            if reservation.attempt != *id
                || reservation.worker != attempt.worker
                || reservation.incarnation != attempt.incarnation
                || reservation.charged.cpu_millis != execution.request.resources.cpu_millis
                || reservation.charged.slots != execution.request.resources.slots
                || !fits(execution.request.resources, reservation.charged)
                || !fits(reservation.charged, execution.reservation_target)
                || (attempt.authorized && reservation.charged != execution.reservation_target)
            {
                return fail("RES1: immutable original reservation vector");
            }
            let dispatches = self.effects.values().filter(|effect| matches!(effect.kind, EffectKind::Dispatch { attempt: target, .. } if target == *id)).count();
            if dispatches != usize::from(attempt.authorized && attempt.phase == Phase::Dispatching)
            {
                return fail("EFFECT2: dispatching attempt has exactly one persistent intent");
            }
            let cancellations = self.effects.values().filter(|effect| matches!(effect.kind, EffectKind::Cancel { attempt: target, .. } if target == *id)).count();
            if cancellations != usize::from(!attempt.authorized && !reservation.released) {
                return fail("EFFECT3: uncertain revoked attempt has exactly one cleanup intent");
            }
        }
        for (id, worker) in &self.workers {
            if worker.id != *id
                || !self.payload_fits(&worker.properties)
                || worker.last_heartbeat > self.now
                || worker.charged != Resources::default()
                || worker.quarantined
            {
                return fail("WORKER1: worker identity and bounded properties");
            }
            let mut cpu = 0_u64;
            let mut memory = 0_u64;
            let mut slots = 0_u64;
            for reservation in self
                .reservations
                .values()
                .filter(|reservation| reservation.worker == *id && !reservation.released)
            {
                cpu = cpu
                    .checked_add(reservation.charged.cpu_millis)
                    .ok_or("RES2: CPU sum overflow")?;
                memory = memory
                    .checked_add(reservation.charged.memory_bytes)
                    .ok_or("RES2: memory sum overflow")?;
                slots = slots
                    .checked_add(reservation.charged.slots)
                    .ok_or("RES2: slot sum overflow")?;
            }
        }
        for (id, reservation) in &self.reservations {
            if reservation.id != *id
                || self
                    .attempts
                    .get(&reservation.attempt)
                    .is_none_or(|attempt| attempt.reservation != *id)
            {
                return fail("RES3: reservation identity and owner");
            }
        }
        for (id, effect) in &self.effects {
            if effect.id != *id
                || effect.epoch != self.epoch
                || !self.executions.contains_key(&effect_execution(effect))
            {
                return fail("EFFECT4: effect identity and execution ownership");
            }
            match &effect.kind {
                EffectKind::Dispatch {
                    execution,
                    attempt,
                    worker,
                    incarnation,
                    reservation,
                    charged,
                    request,
                } => {
                    if self.attempts.get(attempt).is_none_or(|entry| {
                        entry.execution != *execution
                            || entry.worker != *worker
                            || entry.incarnation != *incarnation
                            || entry.reservation != *reservation
                    }) || self
                        .reservations
                        .get(reservation)
                        .is_none_or(|entry| entry.charged != *charged)
                        || &self.executions[execution].request != request
                    {
                        return fail("EFFECT5: immutable dispatch payload");
                    }
                }
                EffectKind::Cancel {
                    execution,
                    attempt,
                    worker,
                    incarnation,
                } => {
                    if self.attempts.get(attempt).is_none_or(|entry| {
                        entry.execution != *execution
                            || entry.worker != *worker
                            || entry.incarnation != *incarnation
                    }) {
                        return fail("EFFECT5: cancellation identity");
                    }
                }
                EffectKind::Lookup { execution, action } => {
                    if &self.executions[execution].request.action != action {
                        return fail("EFFECT5: lookup action identity");
                    }
                }
                EffectKind::PublishResult {
                    execution,
                    action,
                    result,
                } => {
                    let entry = &self.executions[execution];
                    if entry.request.do_not_cache
                        || entry.request.action != *action
                        || result.exit_code != 0
                        || result.error.is_some()
                        || entry.terminal.as_ref()
                            != Some(&TerminalResult::Completed(result.clone()))
                        || !self.payload_fits(result)
                        || result.validate_response().is_err()
                    {
                        return fail(
                            "PUB1: only accepted eligible results produce publication intents",
                        );
                    }
                }
            }
        }
        if self.holds.len() > self.hold_cap() {
            return fail("HOLD1: held workers fit the fixed usable-fleet cap");
        }
        let mut holders = BTreeSet::new();
        for (worker, hold) in &self.holds {
            let Some(execution) = self.executions.get(&hold.execution) else {
                return fail("HOLD2: hold has a queued owner");
            };
            let Some(entry) = self.workers.get(worker) else {
                return fail("HOLD2: hold has a current worker");
            };
            if hold.worker != *worker
                || hold.incarnation != entry.incarnation
                || execution.phase != Phase::Queued
                || execution.effective_priority.is_none()
                || !holders.insert(hold.execution)
                || hold.started_at > self.now
                || hold
                    .started_at
                    .checked_sub(execution.enqueue_at)
                    .is_none_or(|age| age < HOLD_AFTER)
                || execution.hold_cooldown_until > hold.started_at
                || hold.started_at.checked_add(HOLD_DURATION) != Some(hold.until)
                || hold.until <= self.now
                || !self.hold_hard_fit(hold.execution, entry)
            {
                return fail("HOLD2: fixed lifetime, eligible worker, and unique live owner");
            }
        }
        Ok(())
    }
}

const fn effect_execution(effect: &Effect) -> ExecutionId {
    match effect.kind {
        EffectKind::Lookup { execution, .. }
        | EffectKind::Dispatch { execution, .. }
        | EffectKind::Cancel { execution, .. }
        | EffectKind::PublishResult { execution, .. } => execution,
    }
}
