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

//! Independent atomic relation between the frozen scheduler and retention
//! oracles. Neither production State nor Coarse nor a publication reducer is
//! imported. Scheduling policy is delegated to the pre-existing M1 oracle.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command as ReapiCommand, ExecuteResponse,
};
use nativelink_scheduler::authority as a;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::types::*;
use super::{m1, protocol as r, retention};

// Version 1 did not retain classified-retry ordering witnesses. Refuse its
// private snapshots; do not invent positions or clocks while restoring them.
const FORMAT: &str = "independent-no-graph-terminals-v2";
const EPOCH: a::ClusterEpoch = a::ClusterEpoch(1);
const COHORT: r::CohortId = r::CohortId(1);

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Model {
    context: Context,
    position: u64,
    scheduler: m1::Reference,
    storage: retention::Reference,
    actions: BTreeMap<a::ExecutionId, QualifiedAction>,
    reports: BTreeMap<u64, ReportRecord>,
    accepted: BTreeMap<a::ExecutionId, Acceptance>,
    operations: BTreeMap<a::OperationId, OperationLink>,
    credits: BTreeMap<a::ExecutionId, Credit>,
    stages: BTreeMap<u64, Stage>,
    physical: Vec<(Physical, r::Key)>,
    next_root: u64,
    next_write: u64,
}

const fn scheduler_limits() -> a::Limits {
    a::Limits {
        max_executions: MAX_EXECUTIONS,
        max_operations: MAX_OPERATIONS,
        max_workers: MAX_EXECUTIONS,
        max_effects: 128,
        max_payload_bytes: 512 * 1024,
    }
}

const fn retention_limits() -> r::Limits {
    r::Limits {
        cohorts: 1,
        generations: MAX_GENERATIONS,
        roots: 32,
        closure: MAX_MEMBERS,
        mark_batch: 8,
        retire_batch: 8,
        writes: 128,
        delete_intents: 192,
        lease_ticks: 100,
    }
}

impl Model {
    pub(crate) fn new(context: Context, seeds: &[Seed]) -> Result<Self, Refusal> {
        if context.principal.is_empty()
            || context.execution_config.is_empty()
            || context.principal.len() > 4096
            || context.execution_config.len() > 4096
            || seeds.len() > 8
        {
            return Err(Refusal::Invalid);
        }
        let mut model = Self {
            context,
            position: 0,
            scheduler: m1::Reference::new(EPOCH, scheduler_limits())
                .map_err(|_| Refusal::Invalid)?,
            storage: retention::Reference::new(retention_limits()).map_err(|_| Refusal::Invalid)?,
            actions: BTreeMap::new(),
            reports: BTreeMap::new(),
            accepted: BTreeMap::new(),
            operations: BTreeMap::new(),
            credits: BTreeMap::new(),
            stages: BTreeMap::new(),
            physical: Vec::new(),
            next_root: 1,
            next_write: 1,
        };
        model.retain(r::Event::CreateCohort { cohort: COHORT })?;
        // These are explicitly trusted initial protections, not fabricated
        // publications or a graph traversal. Their exact retention relations
        // are preserved across the new transition alphabet.
        for seed in seeds {
            if !matches!(
                seed.kind,
                r::RootKind::Publication | r::RootKind::Serving | r::RootKind::Operation
            ) {
                return Err(Refusal::Invalid);
            }
            let keys = model.allocate(&seed.members, true)?;
            let staging = model.root(r::RootKind::Staging, keys.clone(), seed.until)?;
            for key in &keys {
                let write = model.write(*key, staging)?;
                model.retain(r::Event::WriteDone {
                    write,
                    success: true,
                })?;
            }
            model.root(seed.kind, keys, seed.until)?;
            model.retain(r::Event::Release {
                root: staging,
                lease: 1,
            })?;
        }
        model.check()?;
        model.snapshot()?;
        Ok(model)
    }

    /// All three authorities (scheduler, storage, terminal metadata) commit
    /// together, including the byte gate. Ignored/duplicate observations commit
    /// neither a new position nor any incidental derived state.
    pub(crate) fn apply(&mut self, command: &Command) -> Result<Reply, Refusal> {
        let mut next = self.clone();
        let (reply, changed) = next.transition(command)?;
        if !changed {
            return Ok(reply);
        }
        next.position = self.position.checked_add(1).ok_or(Refusal::Capacity)?;
        next.reconcile()?;
        next.check()?;
        next.snapshot()?;
        *self = next;
        Ok(reply)
    }

    fn transition(&mut self, command: &Command) -> Result<(Reply, bool), Refusal> {
        match command {
            Command::Admit { operation, request } => self.admit(*operation, request),
            Command::Scheduler(event) => self.scheduler_event(event),
            Command::Report(report) => self.report(report),
            Command::Stage {
                id,
                attempt,
                members,
                until,
            } => {
                self.stage(*id, *attempt, members, *until)?;
                Ok((Reply::Applied, true))
            }
            Command::WriteDone { write, success } => {
                let changed = self.retain(r::Event::WriteDone {
                    write: *write,
                    success: *success,
                })?;
                Ok((
                    if changed {
                        Reply::Applied
                    } else {
                        Reply::Ignored
                    },
                    changed,
                ))
            }
            Command::Abandon(id) => {
                let stage = self.stages.get(id).ok_or(Refusal::Invalid)?;
                if stage.abandoned {
                    return Ok((Reply::Ignored, false));
                }
                self.retain(r::Event::Release {
                    root: stage.root,
                    lease: 1,
                })?;
                self.stages.get_mut(id).ok_or(Refusal::Invalid)?.abandoned = true;
                Ok((Reply::Applied, true))
            }
        }
    }

    fn schedule(&mut self, event: a::Event) -> Result<a::Reply, Refusal> {
        let reply = self.scheduler.apply(a::RecordedEvent {
            epoch: EPOCH,
            event,
        });
        if let a::Reply::Rejected(reason) = reply {
            return Err(Refusal::Scheduler(reason));
        }
        Ok(reply)
    }

    fn admit(
        &mut self,
        operation: a::OperationId,
        request: &a::Request,
    ) -> Result<(Reply, bool), Refusal> {
        let action = self.binding(request)?;
        let already_admitted = self.operations.contains_key(&operation);
        let reply = self.schedule(a::Event::Submit {
            operation,
            request: request.clone(),
        })?;
        let a::Reply::Submitted { execution, .. } = reply else {
            return Ok((Reply::Scheduler(reply), false));
        };
        if already_admitted {
            return Ok((Reply::Scheduler(reply), false));
        }
        if let Some(original) = self.actions.get(&execution) {
            if original != &action {
                return Err(Refusal::Unauthorized);
            }
        } else {
            self.actions.insert(execution, action);
            self.credits.insert(
                execution,
                Credit {
                    pending_stage: true,
                    unused_members: MAX_MEMBERS,
                    attempt: None,
                    stages: Vec::new(),
                },
            );
        }
        self.operations
            .entry(operation)
            .or_insert(OperationLink::Waiting);
        Ok((Reply::Scheduler(reply), true))
    }

    fn scheduler_event(&mut self, event: &a::Event) -> Result<(Reply, bool), Refusal> {
        if !matches!(
            event,
            a::Event::Register { .. }
                | a::Event::Capacity { .. }
                | a::Event::Heartbeat { .. }
                | a::Event::WorkerUnavailable { .. }
                | a::Event::Match
                | a::Event::WorkerAccepted { .. }
                | a::Event::Progress { .. }
                | a::Event::Cancel { .. }
                | a::Event::Observe { .. }
                | a::Event::RenewInterest { .. }
                | a::Event::AdvanceTime { .. }
                | a::Event::ExpireAttempt { .. }
                | a::Event::ExpireExecution { .. }
                | a::Event::ExpireInterest { .. }
                | a::Event::Cleanup { .. }
                | a::Event::EffectOutcome { .. }
                | a::Event::LookupResult { result: None, .. }
        ) {
            return Err(Refusal::Unsupported);
        }
        let reply = self.schedule(event.clone())?;
        let changed = matches!(reply, a::Reply::Applied | a::Reply::Matched { .. });
        if !changed {
            return Ok((Reply::Scheduler(reply), false));
        }
        if let a::Event::AdvanceTime { now } = event {
            self.retain(r::Event::AdvanceTime { now: *now })?;
        }
        if let a::Reply::Matched {
            execution, attempt, ..
        } = reply
        {
            let credit = self.credits.get_mut(&execution).ok_or(Refusal::Invalid)?;
            // A retry match must reserve fresh stage/member capacity before its
            // dispatch can escape the outer transaction. Existing stages stay.
            credit.pending_stage = true;
            credit.unused_members = MAX_MEMBERS;
            credit.attempt = Some(attempt);
        }
        Ok((Reply::Scheduler(reply), true))
    }

    fn report(&mut self, report: &Report) -> Result<(Reply, bool), Refusal> {
        if let Some(previous) = self.reports.get(&report.id) {
            return if previous.report == *report {
                Ok((Reply::Duplicate, false))
            } else {
                Err(Refusal::Conflict)
            };
        }
        if report.id == 0 {
            return Err(Refusal::Invalid);
        }
        let position = self.position.checked_add(1).ok_or(Refusal::Capacity)?;
        Self::no_graph(&report.payload)?;
        let before = self.scheduler.view();
        let trial = before
            .attempts
            .iter()
            .find(|trial| trial.id == report.identity.attempt)
            .ok_or(Refusal::Unauthorized)?;
        if !trial.authorized
            || trial.worker != report.identity.worker
            || trial.incarnation != report.identity.incarnation
        {
            return Err(Refusal::Unauthorized);
        }
        let execution = trial.execution;
        let identity = report.identity;
        let event = match report.classification {
            Classification::Complete => a::Event::Complete {
                attempt: identity.attempt,
                worker: identity.worker,
                incarnation: identity.incarnation,
                result: report.payload.clone(),
            },
            Classification::MemoryFailure {
                original_charged,
                peak_memory_bytes,
                quiescent,
            } => a::Event::MemoryFailure {
                attempt: identity.attempt,
                worker: identity.worker,
                incarnation: identity.incarnation,
                charged: original_charged,
                peak_memory_bytes,
                quiescent,
                result: report.payload.clone(),
            },
        };
        if self.schedule(event)? != a::Reply::Applied {
            return Err(Refusal::Unauthorized);
        }
        let after = self.scheduler.view();
        let job = after
            .executions
            .iter()
            .find(|job| job.id == execution)
            .ok_or(Refusal::Invalid)?;
        let disposition = match (&report.classification, &job.terminal) {
            (Classification::Complete, Some(a::TerminalResult::Completed(payload)))
                if payload == &report.payload =>
            {
                Disposition::TerminalAccepted(Acceptance {
                    report: report.id,
                    origin: Origin {
                        epoch: EPOCH,
                        position,
                        now: after.now,
                        execution,
                        identity,
                    },
                    terminal: job.terminal.clone().ok_or(Refusal::Invalid)?,
                })
            }
            (
                Classification::MemoryFailure { .. },
                Some(a::TerminalResult::ResourceFailed { result, .. }),
            ) if result == &report.payload => Disposition::TerminalAccepted(Acceptance {
                report: report.id,
                origin: Origin {
                    epoch: EPOCH,
                    position,
                    now: after.now,
                    execution,
                    identity,
                },
                terminal: job.terminal.clone().ok_or(Refusal::Invalid)?,
            }),
            (Classification::MemoryFailure { .. }, None) if job.phase == a::Phase::Queued => {
                Disposition::RetryScheduled
            }
            _ => return Err(Refusal::Invalid),
        };
        if let Disposition::TerminalAccepted(accepted) = &disposition
            && self.accepted.insert(execution, accepted.clone()).is_some()
        {
            return Err(Refusal::Conflict);
        }
        self.reports.insert(
            report.id,
            ReportRecord {
                report: report.clone(),
                execution,
                position,
                now: after.now,
                disposition: disposition.clone(),
            },
        );
        Ok((Reply::Report(disposition), true))
    }

    fn reconcile(&mut self) -> Result<(), Refusal> {
        let view = self.scheduler.view();
        for execution in &view.executions {
            let credit = self
                .credits
                .get_mut(&execution.id)
                .ok_or(Refusal::Invalid)?;
            if execution.terminal.is_some() {
                // Logical future admission credit is not a physical refund.
                credit.pending_stage = false;
                credit.unused_members = 0;
            }
        }
        for operation in &view.operations {
            let link = match &operation.terminal {
                None => OperationLink::Waiting,
                Some(terminal @ (a::TerminalResult::Cancelled | a::TerminalResult::Exhausted)) => {
                    OperationLink::Independent(terminal.clone())
                }
                Some(terminal) => {
                    let accepted = self
                        .accepted
                        .get(&operation.execution)
                        .ok_or(Refusal::Invalid)?;
                    if &accepted.terminal != terminal {
                        return Err(Refusal::Invalid);
                    }
                    OperationLink::NoGraph(accepted.report)
                }
            };
            self.operations.insert(operation.id, link);
        }
        Ok(())
    }

    fn binding(&self, request: &a::Request) -> Result<QualifiedAction, Refusal> {
        let payload = request.payload.as_ref().ok_or(Refusal::Unsupported)?;
        let wire = serde_json::to_value(payload).map_err(|_| Refusal::Invalid)?;
        if wire.get("principal").and_then(serde_json::Value::as_str)
            != Some(&self.context.principal)
        {
            return Err(Refusal::Unauthorized);
        }
        let projected =
            m1::Reference::captured_request_facts(wire.clone()).ok_or(Refusal::Invalid)?;
        let mut facts = request.clone();
        facts.payload = None;
        if facts != projected {
            return Err(Refusal::Invalid);
        }
        let action_bytes: Vec<u8> =
            serde_json::from_value(wire["action"].clone()).map_err(|_| Refusal::Invalid)?;
        let command_bytes: Vec<u8> =
            serde_json::from_value(wire["command"].clone()).map_err(|_| Refusal::Invalid)?;
        let action = Action::decode(action_bytes.as_slice()).map_err(|_| Refusal::Invalid)?;
        let command =
            ReapiCommand::decode(command_bytes.as_slice()).map_err(|_| Refusal::Invalid)?;
        let platform = action
            .platform
            .as_ref()
            .filter(|platform| !platform.properties.is_empty())
            .or(command.platform.as_ref());
        let semantics = platform.map_or_else(BTreeMap::new, |platform| {
            platform
                .properties
                .iter()
                .map(|property| (property.name.clone(), property.value.clone()))
                .collect()
        });
        Ok(QualifiedAction {
            key: request.action.clone(),
            context: self.context.clone(),
            semantics,
        })
    }

    fn no_graph(payload: &a::ResultPayload) -> Result<(), Refusal> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Captured {
            version: u32,
            encoded: Vec<u8>,
            sha256: [u8; 32],
        }
        let raw = payload.response.as_ref().ok_or(Refusal::Unsupported)?;
        let captured: Captured =
            serde_json::from_value(serde_json::to_value(raw).map_err(|_| Refusal::Invalid)?)
                .map_err(|_| Refusal::Invalid)?;
        let hash = Sha256::digest(&captured.encoded);
        if captured.version != 1
            || captured.encoded.len() > 256 * 1024
            || captured.sha256 != <[u8; 32]>::from(hash)
            || payload.digest != format!("{hash:x}")
        {
            return Err(Refusal::Invalid);
        }
        let response = ExecuteResponse::decode(captured.encoded.as_slice())
            .map_err(|_| Refusal::Unsupported)?;
        if response.result.is_some() {
            return Err(Refusal::GraphBearing);
        }
        let status = response.status.as_ref().ok_or(Refusal::Invalid)?;
        if status.code == 0
            || payload.exit_code != 0
            || payload.error != Some(format!("REAPI status {}: {}", status.code, status.message))
        {
            return Err(Refusal::Invalid);
        }
        Ok(())
    }

    /// Read each retained original directly from the independent scheduler's
    /// stable relation. The representative execution request is insufficient.
    fn originals(&self) -> Result<BTreeMap<a::OperationId, a::Request>, Refusal> {
        #[derive(Deserialize)]
        struct Interest {
            operation: a::OperationId,
            original: a::Request,
        }
        let wire = serde_json::to_value(&self.scheduler).map_err(|_| Refusal::Snapshot)?;
        let interests: Vec<Interest> =
            serde_json::from_value(wire["interests"].clone()).map_err(|_| Refusal::Snapshot)?;
        let len = interests.len();
        let originals: BTreeMap<_, _> = interests
            .into_iter()
            .map(|interest| (interest.operation, interest.original))
            .collect();
        if originals.len() != len {
            return Err(Refusal::Snapshot);
        }
        Ok(originals)
    }

    fn retain(&mut self, event: r::Event) -> Result<bool, Refusal> {
        match self.storage.apply(event) {
            r::Reply::Applied => Ok(true),
            r::Reply::Ignored => Ok(false),
            r::Reply::Rejected => Err(Refusal::Invalid),
        }
    }

    fn allocate(&mut self, members: &[Physical], reuse: bool) -> Result<Vec<r::Key>, Refusal> {
        if members.is_empty()
            || members.len() > MAX_MEMBERS
            || members.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Refusal::Invalid);
        }
        let mut keys = Vec::new();
        for physical in members {
            if let Some((_, key)) = self
                .physical
                .iter()
                .find(|(existing, _)| existing == physical)
            {
                if !reuse {
                    return Err(Refusal::Conflict);
                }
                keys.push(*key);
                continue;
            }
            let key = r::Key {
                incarnation: 1,
                generation: u64::try_from(self.physical.len() + 1)
                    .map_err(|_| Refusal::Capacity)?,
            };
            self.retain(r::Event::Allocate {
                cohort: COHORT,
                key,
                logical_object: key.generation,
            })?;
            self.physical.push((*physical, key));
            keys.push(key);
        }
        keys.sort();
        Ok(keys)
    }

    fn root(
        &mut self,
        kind: r::RootKind,
        closure: Vec<r::Key>,
        until: u64,
    ) -> Result<r::RootId, Refusal> {
        let root = r::RootId(self.next_root);
        self.next_root = self.next_root.checked_add(1).ok_or(Refusal::Capacity)?;
        self.retain(r::Event::Acquire {
            root,
            lease: 1,
            kind,
            closure,
            until,
            fences: BTreeMap::from([(COHORT, 0)]),
        })?;
        Ok(root)
    }

    fn write(&mut self, key: r::Key, staging: r::RootId) -> Result<r::WriteId, Refusal> {
        let write = r::WriteId(self.next_write);
        self.next_write = self.next_write.checked_add(1).ok_or(Refusal::Capacity)?;
        self.retain(r::Event::StartWrite {
            write,
            key,
            staging,
            lease: 1,
        })?;
        Ok(write)
    }

    fn stage(
        &mut self,
        id: u64,
        attempt: a::AttemptId,
        members: &[Physical],
        until: u64,
    ) -> Result<(), Refusal> {
        if id == 0 || self.stages.contains_key(&id) {
            return Err(Refusal::Conflict);
        }
        let view = self.scheduler.view();
        let trial = view
            .attempts
            .iter()
            .find(|trial| trial.id == attempt && trial.authorized)
            .ok_or(Refusal::Unauthorized)?;
        let execution = trial.execution;
        let credit = self.credits.get(&execution).ok_or(Refusal::Invalid)?;
        if credit.attempt != Some(attempt)
            || !credit.pending_stage
            || credit.unused_members < members.len()
        {
            return Err(Refusal::Capacity);
        }
        let keys = self.allocate(members, false)?;
        let root = self.root(r::RootKind::Staging, keys.clone(), until)?;
        let mut writes = Vec::new();
        // One explicit stable provider intent per scenario-declared member;
        // absence of WriteDone is deliberately an unknown physical outcome.
        for key in keys {
            writes.push(self.write(key, root)?);
        }
        self.stages.insert(
            id,
            Stage {
                id,
                execution,
                attempt,
                root,
                members: members.to_vec(),
                writes,
                abandoned: false,
            },
        );
        let credit = self.credits.get_mut(&execution).ok_or(Refusal::Invalid)?;
        credit.pending_stage = false;
        credit.unused_members = 0;
        credit.stages.push(id);
        Ok(())
    }

    pub(crate) fn view(&self) -> Result<View, Refusal> {
        Ok(View {
            position: self.position,
            scheduler: self.scheduler.view(),
            retention: self.storage.view(),
            original_requests: self.originals()?,
            actions: self.actions.clone(),
            reports: self.reports.clone(),
            accepted: self.accepted.clone(),
            operations: self.operations.clone(),
            credits: self.credits.clone(),
            stages: self.stages.clone(),
            physical: self.physical.clone(),
        })
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, Refusal> {
        let bytes = serde_json::to_vec(&(FORMAT, self)).map_err(|_| Refusal::Snapshot)?;
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Refusal::Snapshot);
        }
        Ok(bytes)
    }

    pub(crate) fn restore(bytes: &[u8]) -> Result<Self, Refusal> {
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Refusal::Snapshot);
        }
        let (format, model): (String, Self) =
            serde_json::from_slice(bytes).map_err(|_| Refusal::Snapshot)?;
        if format != FORMAT {
            return Err(Refusal::Snapshot);
        }
        model.check()?;
        if model.snapshot()? != bytes {
            return Err(Refusal::Snapshot);
        }
        Ok(model)
    }

    fn check(&self) -> Result<(), Refusal> {
        // Validate through each frozen oracle's own independent reader, never
        // through the candidate's snapshot or validation implementation.
        m1::Reference::restore(&self.scheduler.snapshot().map_err(|_| Refusal::Snapshot)?)
            .map_err(|_| Refusal::Snapshot)?;
        retention::Reference::restore(&self.storage.snapshot().map_err(|_| Refusal::Snapshot)?)
            .map_err(|_| Refusal::Snapshot)?;
        let view = self.view()?;
        let scheduler_wire =
            serde_json::to_value(&self.scheduler).map_err(|_| Refusal::Snapshot)?;
        if self.context.principal.is_empty()
            || self.context.execution_config.is_empty()
            || self.context.principal.len() > 4096
            || self.context.execution_config.len() > 4096
            || scheduler_wire["limits"]
                != serde_json::to_value(scheduler_limits()).map_err(|_| Refusal::Snapshot)?
            || view.retention.limits != retention_limits()
            || view.scheduler.epoch != EPOCH
            || view.scheduler.now != view.retention.now
            || view.scheduler.executions.len() != self.actions.len()
            || view.scheduler.executions.len() != self.credits.len()
            || view.scheduler.operations.len() != self.operations.len()
            || view.scheduler.operations.len() != view.original_requests.len()
            || self.accepted.len() > MAX_EXECUTIONS
            || self.reports.len() > MAX_EXECUTIONS * MAX_REPORTS
            || self
                .reports
                .values()
                .map(|record| record.report.identity.attempt)
                .collect::<BTreeSet<_>>()
                .len()
                != self.reports.len()
            || self
                .stages
                .values()
                .map(|stage| stage.attempt)
                .collect::<BTreeSet<_>>()
                .len()
                != self.stages.len()
            || self.stages.len()
                + self
                    .credits
                    .values()
                    .filter(|credit| credit.pending_stage)
                    .count()
                > MAX_STAGES
            || self.physical.len()
                + self
                    .credits
                    .values()
                    .map(|credit| credit.unused_members)
                    .sum::<usize>()
                > MAX_GENERATIONS
        {
            return Err(Refusal::Capacity);
        }
        if self
            .physical
            .iter()
            .map(|(physical, _)| physical)
            .collect::<BTreeSet<_>>()
            .len()
            != self.physical.len()
            || self
                .physical
                .iter()
                .map(|(_, key)| key)
                .collect::<BTreeSet<_>>()
                .len()
                != self.physical.len()
            || self
                .physical
                .iter()
                .map(|(_, key)| *key)
                .collect::<BTreeSet<_>>()
                != view.retention.generations.keys().copied().collect()
            || view
                .retention
                .roots
                .keys()
                .any(|root| root.0 >= self.next_root)
            || view
                .retention
                .writes
                .keys()
                .any(|write| write.0 >= self.next_write)
        {
            return Err(Refusal::Invalid);
        }
        for execution in &view.scheduler.executions {
            if self.actions.get(&execution.id) != Some(&self.binding(&execution.request)?) {
                return Err(Refusal::Unauthorized);
            }
            let credit = self.credits.get(&execution.id).ok_or(Refusal::Invalid)?;
            if credit.attempt.is_some_and(|id| {
                !view
                    .scheduler
                    .attempts
                    .iter()
                    .any(|trial| trial.id == id && trial.execution == execution.id)
            }) || (execution.terminal.is_none()
                && execution.attempt.is_some()
                && credit.attempt != execution.attempt)
                || (credit.pending_stage
                    && credit.stages.iter().any(|id| {
                        self.stages
                            .get(id)
                            .is_some_and(|stage| Some(stage.attempt) == credit.attempt)
                    }))
            {
                return Err(Refusal::Invalid);
            }
            if credit.unused_members != if credit.pending_stage { MAX_MEMBERS } else { 0 }
                || (execution.terminal.is_some() && credit.pending_stage)
                || credit.stages.iter().collect::<BTreeSet<_>>().len() != credit.stages.len()
                || credit.stages.iter().any(|id| {
                    self.stages
                        .get(id)
                        .is_none_or(|stage| stage.execution != execution.id)
                })
                || self
                    .reports
                    .values()
                    .filter(|record| record.execution == execution.id)
                    .count()
                    > MAX_REPORTS
            {
                return Err(Refusal::Invalid);
            }
            match &execution.terminal {
                Some(
                    terminal @ (a::TerminalResult::Completed(_)
                    | a::TerminalResult::ResourceFailed { .. }),
                ) => {
                    if self
                        .accepted
                        .get(&execution.id)
                        .is_none_or(|accepted| &accepted.terminal != terminal)
                    {
                        return Err(Refusal::Invalid);
                    }
                }
                _ => {
                    if self.accepted.contains_key(&execution.id) {
                        return Err(Refusal::Invalid);
                    }
                }
            }
        }
        for operation in &view.scheduler.operations {
            let original = view
                .original_requests
                .get(&operation.id)
                .ok_or(Refusal::Invalid)?;
            if self.actions.get(&operation.execution) != Some(&self.binding(original)?) {
                return Err(Refusal::Unauthorized);
            }
            match (&operation.terminal, self.operations.get(&operation.id)) {
                (None, Some(OperationLink::Waiting)) => {}
                (
                    Some(terminal @ (a::TerminalResult::Cancelled | a::TerminalResult::Exhausted)),
                    Some(OperationLink::Independent(link)),
                ) if terminal == link => {}
                (Some(terminal), Some(OperationLink::NoGraph(report)))
                    if self
                        .accepted
                        .get(&operation.execution)
                        .is_some_and(|accepted| {
                            accepted.report == *report && &accepted.terminal == terminal
                        }) => {}
                _ => return Err(Refusal::Invalid),
            }
        }
        for (execution, accepted) in &self.accepted {
            let origin = accepted.origin;
            let record = self.reports.get(&accepted.report).ok_or(Refusal::Invalid)?;
            if origin.execution != *execution
                || origin.epoch != EPOCH
                || origin.position == 0
                || origin.position > self.position
                || origin.now > view.scheduler.now
                || record.disposition != Disposition::TerminalAccepted(accepted.clone())
                || record.report.identity != origin.identity
                || record.execution != *execution
                || !view.scheduler.attempts.iter().any(|attempt| {
                    attempt.id == origin.identity.attempt
                        && attempt.execution == *execution
                        && attempt.worker == origin.identity.worker
                        && attempt.incarnation == origin.identity.incarnation
                })
            {
                return Err(Refusal::Invalid);
            }
        }
        for (id, record) in &self.reports {
            Self::no_graph(&record.report.payload)?;
            if *id == 0 || *id != record.report.id {
                return Err(Refusal::Invalid);
            }
            let trial = view
                .scheduler
                .attempts
                .iter()
                .find(|trial| trial.id == record.report.identity.attempt)
                .ok_or(Refusal::Invalid)?;
            let reservation = view
                .scheduler
                .reservations
                .iter()
                .find(|row| row.id == trial.reservation)
                .ok_or(Refusal::Invalid)?;
            if trial.execution != record.execution
                || trial.worker != record.report.identity.worker
                || trial.incarnation != record.report.identity.incarnation
                || trial.authorized
                || !reservation.released
            {
                return Err(Refusal::Invalid);
            }
            match (&record.report.classification, &record.disposition) {
                (
                    Classification::Complete,
                    Disposition::TerminalAccepted(Acceptance {
                        terminal: a::TerminalResult::Completed(payload),
                        ..
                    }),
                ) if payload == &record.report.payload => {}
                (
                    Classification::MemoryFailure {
                        original_charged,
                        quiescent: true,
                        ..
                    },
                    disposition,
                ) if *original_charged == reservation.charged => {
                    if let Disposition::TerminalAccepted(accepted) = disposition
                        && !matches!(&accepted.terminal, a::TerminalResult::ResourceFailed { result, .. } if result == &record.report.payload)
                    {
                        return Err(Refusal::Invalid);
                    }
                }
                _ => return Err(Refusal::Invalid),
            }
            if let Disposition::TerminalAccepted(accepted) = &record.disposition
                && self.accepted.get(&record.execution) != Some(accepted)
            {
                return Err(Refusal::Invalid);
            }
        }
        self.check_report_history(&view)?;
        for (id, stage) in &self.stages {
            let root = view
                .retention
                .roots
                .get(&stage.root)
                .ok_or(Refusal::Invalid)?;
            let expected: BTreeSet<_> = stage
                .members
                .iter()
                .map(|member| {
                    self.physical
                        .iter()
                        .find(|(physical, _)| physical == member)
                        .map(|(_, key)| *key)
                        .ok_or(Refusal::Invalid)
                })
                .collect::<Result<_, _>>()?;
            if *id != stage.id
                || expected.len() != stage.members.len()
                || expected.is_empty()
                || root.kind != r::RootKind::Staging
                || root.lease != 1
                || root.closure.iter().copied().collect::<BTreeSet<_>>() != expected
                || stage.writes.len() != expected.len()
                || stage
                    .writes
                    .iter()
                    .map(|write| {
                        view.retention
                            .writes
                            .get(write)
                            .map(|row| row.key)
                            .ok_or(Refusal::Invalid)
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?
                    != expected
                || self
                    .credits
                    .get(&stage.execution)
                    .is_none_or(|credit| !credit.stages.contains(id))
                || !view
                    .scheduler
                    .attempts
                    .iter()
                    .any(|trial| (trial.id, trial.execution) == (stage.attempt, stage.execution))
            {
                return Err(Refusal::Invalid);
            }
        }
        Ok(())
    }

    /// Check contradictions expressible in retained immutable facts. This is
    /// not a reconstruction of historical worker eligibility: current capacity,
    /// availability and worker sessions may have changed after a valid report.
    fn check_report_history(&self, view: &View) -> Result<(), Refusal> {
        let mut reports: Vec<_> = self.reports.values().collect();
        reports.sort_by_key(|record| record.position);
        if reports.iter().any(|record| {
            record.position == 0
                || record.position > self.position
                || record.now > view.scheduler.now
        }) || reports
            .windows(2)
            .any(|pair| pair[0].position >= pair[1].position || pair[0].now > pair[1].now)
        {
            return Err(Refusal::Invalid);
        }

        for execution in &view.scheduler.executions {
            let mut trials: Vec<_> = view
                .scheduler
                .attempts
                .iter()
                .filter(|trial| trial.execution == execution.id)
                .collect();
            trials.sort_by_key(|trial| trial.id);
            if usize::try_from(execution.attempts_started).ok() != Some(trials.len())
                || trials.len() > MAX_REPORTS
            {
                return Err(Refusal::Invalid);
            }
            let mut target = execution.request.resources;
            let mut preceding_report: Option<&ReportRecord> = None;
            for (index, trial) in trials.iter().enumerate() {
                let reservation = view
                    .scheduler
                    .reservations
                    .iter()
                    .find(|row| row.id == trial.reservation)
                    .ok_or(Refusal::Invalid)?;
                if reservation.charged != target {
                    // A successor grant cannot manufacture memory growth after
                    // the retained retry record has disappeared.
                    return Err(Refusal::Invalid);
                }
                let Some(record) = self
                    .reports
                    .values()
                    .find(|record| record.report.identity.attempt == trial.id)
                else {
                    continue;
                };
                if record.now < execution.enqueue_at
                    || preceding_report.is_some_and(|previous| {
                        previous.position >= record.position || previous.now > record.now
                    })
                {
                    return Err(Refusal::Invalid);
                }
                preceding_report = Some(record);
                if let Disposition::TerminalAccepted(accepted) = &record.disposition
                    && (index + 1 != trials.len()
                        || accepted.origin.position != record.position
                        || accepted.origin.now != record.now
                        || execution.terminal_at != Some(record.now))
                {
                    return Err(Refusal::Invalid);
                }

                let Classification::MemoryFailure {
                    original_charged,
                    peak_memory_bytes,
                    ..
                } = record.report.classification
                else {
                    continue;
                };
                let ordinal = index + 1;
                let base = if original_charged.memory_bytes == 0 {
                    peak_memory_bytes.unwrap_or(0)
                } else {
                    original_charged.memory_bytes
                };
                match &record.disposition {
                    Disposition::RetryScheduled => {
                        if ordinal >= MAX_REPORTS || base == 0 {
                            return Err(Refusal::Invalid);
                        }
                        // The next actual immutable grant (or the retained
                        // target before it exists) records the growth result.
                        let successor = if let Some(next) = trials.get(index + 1) {
                            view.scheduler
                                .reservations
                                .iter()
                                .find(|row| row.id == next.reservation)
                                .ok_or(Refusal::Invalid)?
                                .charged
                        } else {
                            execution.reservation_target
                        };
                        if successor.cpu_millis != target.cpu_millis
                            || successor.slots != target.slots
                            || successor.memory_bytes <= original_charged.memory_bytes
                            || successor.memory_bytes < execution.request.resources.memory_bytes
                            || successor.memory_bytes > base.saturating_mul(2)
                        {
                            return Err(Refusal::Invalid);
                        }
                        target = successor;
                    }
                    Disposition::TerminalAccepted(accepted) => {
                        let a::TerminalResult::ResourceFailed { reason, .. } = accepted.terminal
                        else {
                            return Err(Refusal::Invalid);
                        };
                        let locally_possible = match reason {
                            a::ResourceFailureReason::AttemptBudgetExhausted => {
                                ordinal == MAX_REPORTS
                            }
                            a::ResourceFailureReason::MissingGrowthEvidence => {
                                ordinal < MAX_REPORTS && base == 0
                            }
                            a::ResourceFailureReason::NoLargerEligibleWorker => {
                                ordinal < MAX_REPORTS && base > 0
                            }
                        };
                        if !locally_possible {
                            return Err(Refusal::Invalid);
                        }
                    }
                }
            }
            if target != execution.reservation_target {
                return Err(Refusal::Invalid);
            }
        }
        Ok(())
    }
}
