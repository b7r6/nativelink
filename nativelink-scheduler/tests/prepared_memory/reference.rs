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

//! Independent finite composition. Scheduling decisions come only from the
//! unchanged relational M1 oracle. Graph completeness is a scenario fact.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command as ReapiCommand, ExecuteResponse,
};
use nativelink_scheduler::authority as a;
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::head::{reference as graph, types as h};
use super::m1;
use super::types::*;

const FORMAT: &str = "independent-prepared-memory-history-v2";
const EPOCH: a::ClusterEpoch = a::ClusterEpoch(1);

#[derive(Clone, Debug)]
pub(crate) struct Model {
    context: Context,
    scenario: h::Scenario,
    seeds: Vec<h::Coverage>,
    scheduler: m1::Reference,
    position: u64,
    actions: BTreeMap<a::ExecutionId, QualifiedAction>,
    originals: BTreeMap<a::OperationId, a::Request>,
    stages: BTreeMap<u64, Stage>,
    classified: BTreeMap<u64, Classified>,
    accepted: BTreeMap<a::ExecutionId, Accepted>,
    operations: BTreeMap<a::OperationId, u64>,
    roots: Vec<h::Coverage>,
    heads: BTreeMap<String, u64>,
    credits: BTreeMap<a::ExecutionId, Credit>,
    history: Vec<Step>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    format: String,
    context: Context,
    scenario: h::Scenario,
    seeds: Vec<h::Coverage>,
    history: Vec<Step>,
}

impl Model {
    pub(crate) fn new(
        context: Context,
        scenario: h::Scenario,
        seeds: Vec<h::Coverage>,
    ) -> Result<Self, Refusal> {
        graph::Model::new(scenario.clone()).map_err(|_| Refusal::Invalid)?;
        if context.principal.is_empty()
            || context.execution_config.is_empty()
            || context.principal.len() > 4096
            || context.execution_config.len() > 4096
            || seeds.len() > 8
            || seeds.iter().any(|seed| {
                !matches!(seed.root, h::Root::Serving(_))
                    || seed.keys.is_empty()
                    || seed.keys.len() > h::MAX_MEMBERS
                    || seed.until == 0
                    || seed.until > h::LEASE_TICKS
            })
            || seeds
                .iter()
                .map(|seed| seed.root)
                .collect::<BTreeSet<_>>()
                .len()
                != seeds.len()
        {
            return Err(Refusal::Invalid);
        }
        let scheduler = m1::Reference::new(
            EPOCH,
            a::Limits {
                max_executions: h::MAX_EXECUTIONS,
                max_operations: h::MAX_OPERATIONS,
                max_workers: h::MAX_EXECUTIONS,
                max_effects: 128,
                max_payload_bytes: 512 * 1024,
            },
        )
        .map_err(|_| Refusal::Invalid)?;
        let model = Self {
            context,
            scenario,
            roots: seeds.clone(),
            seeds,
            scheduler,
            position: 0,
            actions: BTreeMap::new(),
            originals: BTreeMap::new(),
            stages: BTreeMap::new(),
            classified: BTreeMap::new(),
            accepted: BTreeMap::new(),
            operations: BTreeMap::new(),
            heads: BTreeMap::new(),
            credits: BTreeMap::new(),
            history: Vec::new(),
        };
        model.snapshot()?;
        Ok(model)
    }

    pub(crate) fn apply(&mut self, command: &Command, facts: &h::Facts) -> Result<Reply, Refusal> {
        let mut next = self.clone();
        let (reply, changed) = next.transition(command, facts)?;
        if !changed {
            return Ok(reply);
        }
        next.position = self.position.checked_add(1).ok_or(Refusal::Capacity)?;
        for job in next.scheduler.view().executions {
            if job.terminal.is_some() {
                next.credits
                    .get_mut(&job.id)
                    .ok_or(Refusal::Invalid)?
                    .pending = false;
            }
        }
        next.history.push(Step {
            command: command.clone(),
            facts: facts.clone(),
        });
        next.bounds()?;
        next.snapshot()?;
        *self = next;
        Ok(reply)
    }

    fn transition(
        &mut self,
        command: &Command,
        facts: &h::Facts,
    ) -> Result<(Reply, bool), Refusal> {
        match command {
            Command::Admit { operation, request } => {
                self.deadline(OPERATION_UNTIL, h::LEASE_TICKS)?;
                let qualified = binding(&self.context, request)?;
                if self.originals.contains_key(operation) {
                    return Err(Refusal::Conflict);
                }
                let reply = self.schedule(a::Event::Submit {
                    operation: *operation,
                    request: request.clone(),
                })?;
                let a::Reply::Submitted { execution, .. } = reply else {
                    return Ok((Reply::Scheduler(reply), false));
                };
                // This alphabet has no selected AC heads. Resolve the miss
                // inside the same outer admission, through independent M1.
                let lookup = self.scheduler.view().effects.iter().find_map(|effect| {
                    matches!(effect.kind, a::EffectKind::Lookup { execution: id, .. } if id == execution).then_some(effect.id)
                });
                if let Some(effect) = lookup {
                    self.schedule(a::Event::LookupResult {
                        effect,
                        result: None,
                    })?;
                }
                if let Some(old) = self.actions.get(&execution) {
                    if old != &qualified {
                        return Err(Refusal::Unauthorized);
                    }
                } else {
                    self.actions.insert(execution, qualified);
                    self.credits.insert(
                        execution,
                        Credit {
                            pending: true,
                            attempt: None,
                        },
                    );
                }
                self.originals.insert(*operation, request.clone());
                Ok((Reply::Scheduler(reply), true))
            }
            Command::Scheduler(event) => {
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
                if let a::Reply::Matched {
                    execution, attempt, ..
                } = reply
                {
                    let credit = self.credits.get_mut(&execution).ok_or(Refusal::Invalid)?;
                    credit.pending = true;
                    credit.attempt = Some(attempt);
                }
                Ok((Reply::Scheduler(reply), changed))
            }
            Command::Stage {
                id,
                candidate,
                report,
                epoch,
                until,
                writes,
            } => {
                if self.stages.contains_key(id) {
                    return Err(Refusal::Conflict);
                }
                if *id == 0 || *epoch != EPOCH {
                    return Err(Refusal::Invalid);
                }
                self.deadline(*until, h::STAGE_TICKS)?;
                let execution = self.authorized(report)?;
                self.available_report(report, execution)?;
                let graph = self
                    .scenario
                    .candidates
                    .get(candidate)
                    .ok_or(Refusal::Invalid)?;
                let identity = report.identity;
                if !matches!(report.classification, Classification::MemoryFailure { .. })
                    || graph.source
                        != (h::Source::Worker {
                            execution: execution.0,
                            attempt: h::Attempt {
                                id: identity.attempt.0,
                                worker: identity.worker.0,
                                incarnation: identity.incarnation.0,
                            },
                        })
                    || graph.action
                        != serde_json::to_string(&self.actions[&execution])
                            .map_err(|_| Refusal::Invalid)?
                {
                    return Err(Refusal::Unauthorized);
                }
                let decoded = response(&report.payload)?;
                let captured = response_bytes(&report.payload)?;
                if captured != graph.response
                    || decoded.result.as_ref().map(Message::encode_to_vec)
                        != Some(graph.bytes.clone())
                    || graph::object(&graph.bytes, graph.result.algorithm) != graph.result
                    || graph.successful != (report.payload.exit_code == 0)
                    || graph.error_free != report.payload.error.is_none()
                {
                    return Err(Refusal::Invalid);
                }
                let expected_algorithm = match self.actions[&execution].key.digest_function.as_str()
                {
                    "sha256" => h::Algorithm::Sha256,
                    "blake3" => h::Algorithm::Blake3,
                    _ => return Err(Refusal::Unsupported),
                };
                if graph.result.algorithm != expected_algorithm {
                    return Err(Refusal::Invalid);
                }
                let members = keys(graph);
                if writes.len() > h::MAX_MEMBERS
                    || writes.contains_key(&0)
                    || writes.values().any(|key| !members.contains(key))
                {
                    return Err(Refusal::Conflict);
                }
                physical(graph, facts, false)?;
                let credit = self.credits.get_mut(&execution).ok_or(Refusal::Invalid)?;
                if !credit.pending || credit.attempt != Some(identity.attempt) {
                    return Err(Refusal::Capacity);
                }
                // Preview checks validity only. Never cache retry/terminal policy.
                let mut preview = self.scheduler.clone();
                if preview.apply(a::RecordedEvent {
                    epoch: EPOCH,
                    event: report_event(report),
                }) != a::Reply::Applied
                {
                    return Err(Refusal::Invalid);
                }
                credit.pending = false;
                self.roots.push(h::Coverage {
                    root: h::Root::Stage(*id),
                    keys: members,
                    until: *until,
                });
                self.stages.insert(
                    *id,
                    Stage {
                        candidate: *candidate,
                        report: report.clone(),
                        epoch: *epoch,
                        until: *until,
                        writes: writes.clone(),
                        outcomes: BTreeMap::new(),
                        preparation: None,
                        abandoned: false,
                        classified: false,
                    },
                );
                Ok((Reply::Applied, true))
            }
            Command::Observe(receipt) => {
                let stage = self
                    .stages
                    .get_mut(&receipt.stage)
                    .ok_or(Refusal::Invalid)?;
                let graph = &self.scenario.candidates[&stage.candidate];
                if stage.writes.get(&receipt.write) != Some(&receipt.key)
                    || !graph
                        .required
                        .iter()
                        .any(|m| m.key == receipt.key && m.object == receipt.object)
                    || !facts.receipts.contains(receipt)
                {
                    return Err(Refusal::Unauthorized);
                }
                if let Some(old) = stage.outcomes.get(&receipt.write) {
                    return if *old == receipt.success {
                        Ok((Reply::Duplicate, false))
                    } else {
                        Err(Refusal::Conflict)
                    };
                }
                stage.outcomes.insert(receipt.write, receipt.success);
                Ok((Reply::Applied, true))
            }
            Command::Prepare { stage, evidence } => {
                let row = self.stages.get(stage).ok_or(Refusal::Invalid)?;
                self.proof(*stage, evidence, facts, evidence.until)?;
                if row.preparation.as_ref() == Some(evidence) {
                    return Ok((Reply::Duplicate, false));
                }
                self.stages
                    .get_mut(stage)
                    .ok_or(Refusal::Invalid)?
                    .preparation = Some(evidence.clone());
                Ok((Reply::Applied, true))
            }
            Command::Accept(arguments) => self.accept(arguments, facts),
            Command::NoGraph(report) => self.no_graph(report),
            Command::Abandon(id) => {
                let row = self.stages.get_mut(id).ok_or(Refusal::Invalid)?;
                if row.abandoned {
                    return Err(Refusal::Stale);
                }
                if self
                    .accepted
                    .values()
                    .any(|accepted| accepted.report == row.report.id)
                {
                    return Err(Refusal::Conflict);
                }
                row.abandoned = true;
                self.roots.retain(|root| root.root != h::Root::Stage(*id));
                Ok((Reply::Applied, true))
            }
        }
    }

    fn accept(
        &mut self,
        arguments: &AcceptanceArguments,
        facts: &h::Facts,
    ) -> Result<(Reply, bool), Refusal> {
        let stage = self
            .stages
            .get(&arguments.stage)
            .ok_or(Refusal::Invalid)?
            .clone();
        if let Some(old) = self.classified.get(&stage.report.id) {
            return if old.arguments.as_ref() == Some(arguments) {
                Ok((Reply::Duplicate, false))
            } else {
                Err(Refusal::Conflict)
            };
        }
        if arguments.publication == 0
            || self
                .accepted
                .values()
                .any(|old| old.publication == Some(arguments.publication))
        {
            return Err(Refusal::Conflict);
        }
        self.deadline(arguments.until, h::LEASE_TICKS)?;
        self.proof(
            arguments.stage,
            stage.preparation.as_ref().ok_or(Refusal::Incomplete)?,
            facts,
            arguments.until,
        )?;
        let execution = self.authorized(&stage.report)?;
        let before = self.scheduler.view();
        let waiting: Vec<_> = before
            .operations
            .iter()
            .filter(|op| op.execution == execution && op.interested && op.terminal.is_none())
            .map(|op| op.id)
            .collect();
        let disposition = self.classify(&stage.report, execution, Some(arguments.clone()))?;
        if matches!(disposition, Disposition::TerminalAccepted { .. }) {
            if !waiting.is_empty() {
                self.deadline(OPERATION_UNTIL, h::LEASE_TICKS)?;
                if stage.preparation.as_ref().ok_or(Refusal::Incomplete)?.until < OPERATION_UNTIL {
                    return Err(Refusal::Unprotected);
                }
            }
            let members = keys(&self.scenario.candidates[&stage.candidate]);
            let after = self.scheduler.view();
            for operation in waiting {
                let actual = after
                    .operations
                    .iter()
                    .find(|op| op.id == operation)
                    .ok_or(Refusal::Invalid)?;
                if actual.terminal.as_ref() != Some(&self.accepted[&execution].terminal) {
                    return Err(Refusal::Invalid);
                }
                self.operations.insert(operation, stage.report.id);
                self.roots.push(h::Coverage {
                    root: h::Root::Operation(operation.0),
                    keys: members.clone(),
                    until: OPERATION_UNTIL,
                });
            }
            // The transient result root transfers directly to operation roots.
            // Failure has no retained publication root and never selects AC.
            self.roots
                .retain(|root| root.root != h::Root::Stage(arguments.stage));
        }
        self.stages
            .get_mut(&arguments.stage)
            .ok_or(Refusal::Invalid)?
            .classified = true;
        Ok((Reply::Classified(disposition), true))
    }

    fn no_graph(&mut self, report: &Report) -> Result<(Reply, bool), Refusal> {
        if let Some(old) = self.classified.get(&report.id) {
            return if old.arguments.is_none() && old.report == *report {
                Ok((Reply::Duplicate, false))
            } else {
                Err(Refusal::Conflict)
            };
        }
        let execution = self.authorized(report)?;
        self.available_report(report, execution)?;
        if response(&report.payload)?.result.is_some() {
            return Err(Refusal::Unsupported);
        }
        let disposition = self.classify(report, execution, None)?;
        if matches!(disposition, Disposition::TerminalAccepted { .. }) {
            for operation in self.scheduler.view().operations {
                if operation.execution == execution
                    && operation.terminal.as_ref() == Some(&self.accepted[&execution].terminal)
                {
                    self.operations.insert(operation.id, report.id);
                }
            }
        }
        Ok((Reply::Classified(disposition), true))
    }

    fn classify(
        &mut self,
        report: &Report,
        execution: a::ExecutionId,
        arguments: Option<AcceptanceArguments>,
    ) -> Result<Disposition, Refusal> {
        // This is the only policy decision: the scheduler state at acceptance.
        if self.schedule(report_event(report))? != a::Reply::Applied {
            return Err(Refusal::Unauthorized);
        }
        let after = self.scheduler.view();
        let job = after
            .executions
            .iter()
            .find(|job| job.id == execution)
            .ok_or(Refusal::Invalid)?;
        let disposition = match (&report.classification, &job.terminal) {
            (
                Classification::MemoryFailure { .. },
                Some(terminal @ a::TerminalResult::ResourceFailed { result, .. }),
            ) if result == &report.payload => Disposition::TerminalAccepted {
                terminal: terminal.clone(),
            },
            (Classification::Complete, Some(terminal @ a::TerminalResult::Completed(result)))
                if result == &report.payload =>
            {
                Disposition::TerminalAccepted {
                    terminal: terminal.clone(),
                }
            }
            (Classification::MemoryFailure { .. }, None) if job.phase == a::Phase::Queued => {
                Disposition::RetryScheduled {
                    target: job.reservation_target,
                }
            }
            _ => return Err(Refusal::Invalid),
        };
        let position = self.position.checked_add(1).ok_or(Refusal::Capacity)?;
        if let Disposition::TerminalAccepted { terminal } = &disposition
            && self
                .accepted
                .insert(
                    execution,
                    Accepted {
                        report: report.id,
                        execution,
                        position,
                        now: after.now,
                        terminal: terminal.clone(),
                        publication: arguments.as_ref().map(|args| args.publication),
                    },
                )
                .is_some()
        {
            return Err(Refusal::Conflict);
        }
        self.classified.insert(
            report.id,
            Classified {
                report: report.clone(),
                execution,
                position,
                now: after.now,
                arguments,
                disposition: disposition.clone(),
            },
        );
        Ok(disposition)
    }

    fn authorized(&self, report: &Report) -> Result<a::ExecutionId, Refusal> {
        if report.id == 0 {
            return Err(Refusal::Invalid);
        }
        self.scheduler
            .view()
            .attempts
            .into_iter()
            .find(|trial| {
                trial.id == report.identity.attempt
                    && trial.authorized
                    && trial.worker == report.identity.worker
                    && trial.incarnation == report.identity.incarnation
            })
            .map(|trial| trial.execution)
            .ok_or(Refusal::Unauthorized)
    }

    fn available_report(&self, report: &Report, execution: a::ExecutionId) -> Result<(), Refusal> {
        if self.classified.values().any(|old| {
            old.report.id == report.id || old.report.identity.attempt == report.identity.attempt
        }) || self.stages.values().any(|old| {
            old.report.id == report.id || old.report.identity.attempt == report.identity.attempt
        }) {
            return Err(Refusal::Conflict);
        }
        let attempts: BTreeSet<_> = self
            .scheduler
            .view()
            .attempts
            .into_iter()
            .filter(|trial| trial.execution == execution)
            .map(|trial| trial.id)
            .collect();
        let reserved: BTreeSet<_> = self
            .classified
            .values()
            .map(|row| &row.report)
            .chain(self.stages.values().map(|row| &row.report))
            .filter(|report| attempts.contains(&report.identity.attempt))
            .map(|report| report.id)
            .collect();
        if reserved.len() >= 3 {
            return Err(Refusal::Capacity);
        }
        Ok(())
    }

    fn proof(
        &self,
        stage: u64,
        proof: &h::Preparation,
        facts: &h::Facts,
        until: u64,
    ) -> Result<(), Refusal> {
        let row = self.stages.get(&stage).ok_or(Refusal::Invalid)?;
        let graph = &self.scenario.candidates[&row.candidate];
        let now = self.scheduler.view().now;
        if row.abandoned || row.classified || row.until <= now {
            return Err(Refusal::Stale);
        }
        if proof.id != stage
            || proof.stage != stage
            || proof.result != graph.result
            || !proof.complete
            || !proof.verified
            || proof.typed != graph.required
            || proof.protected != keys(graph)
        {
            return Err(Refusal::Incomplete);
        }
        let namespaces: BTreeSet<_> = proof.protected.iter().map(|key| key.namespace).collect();
        if proof
            .fences
            .iter()
            .map(|fence| fence.namespace)
            .collect::<BTreeSet<_>>()
            != namespaces
            || proof.fences.len() != namespaces.len()
        {
            return Err(Refusal::Incomplete);
        }
        if proof.catalogue_revision != facts.catalogue_revision
            || !proof.fences.is_subset(&facts.fences)
            || proof.until < until
            || proof.until <= now
            || now
                .checked_add(h::LEASE_TICKS)
                .is_none_or(|limit| proof.until > limit)
        {
            return Err(Refusal::Stale);
        }
        physical(graph, facts, true)?;
        if !self.roots.iter().any(|root| {
            root.root == h::Root::Stage(stage) && root.keys == keys(graph) && root.until > now
        }) {
            return Err(Refusal::Unprotected);
        }
        Ok(())
    }

    fn deadline(&self, until: u64, maximum: u64) -> Result<(), Refusal> {
        let now = self.scheduler.view().now;
        if until <= now || now.checked_add(maximum).is_none_or(|limit| until > limit) {
            return Err(Refusal::Invalid);
        }
        Ok(())
    }

    fn schedule(&mut self, event: a::Event) -> Result<a::Reply, Refusal> {
        match self.scheduler.apply(a::RecordedEvent {
            epoch: EPOCH,
            event,
        }) {
            a::Reply::Rejected(reason) => Err(Refusal::Scheduler(reason)),
            reply => Ok(reply),
        }
    }

    fn bounds(&self) -> Result<(), Refusal> {
        let pending = self
            .credits
            .values()
            .filter(|credit| credit.pending)
            .count();
        let members: usize = self
            .stages
            .values()
            .map(|stage| self.scenario.candidates[&stage.candidate].required.len())
            .sum();
        let reports: BTreeSet<_> = self
            .classified
            .keys()
            .copied()
            .chain(self.stages.values().map(|stage| stage.report.id))
            .collect();
        if self.history.len() > MAX_EVENTS
            || self.stages.len() + pending > h::MAX_STAGES
            || members + pending * h::MAX_MEMBERS > 64
            || self.roots.len() > 32
            || reports.len() > 24
            || self.originals.len() > h::MAX_OPERATIONS
            || !self.heads.is_empty()
        {
            return Err(Refusal::Capacity);
        }
        Ok(())
    }

    pub(crate) fn view(&self) -> View {
        View {
            position: self.position,
            scheduler: self.scheduler.view(),
            actions: self.actions.clone(),
            originals: self.originals.clone(),
            stages: self.stages.clone(),
            classified: self.classified.clone(),
            accepted: self.accepted.clone(),
            operations: self.operations.clone(),
            roots: self.roots.clone(),
            heads: self.heads.clone(),
            credits: self.credits.clone(),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, Refusal> {
        let bytes = serde_json::to_vec(&Snapshot {
            format: FORMAT.into(),
            context: self.context.clone(),
            scenario: self.scenario.clone(),
            seeds: self.seeds.clone(),
            history: self.history.clone(),
        })
        .map_err(|_| Refusal::Snapshot)?;
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Refusal::Capacity);
        }
        Ok(bytes)
    }

    pub(crate) fn restore(bytes: &[u8]) -> Result<Self, Refusal> {
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Refusal::Snapshot);
        }
        let snapshot: Snapshot = serde_json::from_slice(bytes).map_err(|_| Refusal::Snapshot)?;
        if snapshot.format != FORMAT || snapshot.history.len() > MAX_EVENTS {
            return Err(Refusal::Snapshot);
        }
        let mut restored = Self::new(snapshot.context, snapshot.scenario, snapshot.seeds)?;
        for step in snapshot.history {
            restored.apply(&step.command, &step.facts)?;
        }
        if restored.snapshot()? != bytes {
            return Err(Refusal::Snapshot);
        }
        Ok(restored)
    }
}

fn keys(graph: &h::Candidate) -> BTreeSet<h::Key> {
    graph.required.iter().map(|member| member.key).collect()
}

fn physical(graph: &h::Candidate, facts: &h::Facts, require_ready: bool) -> Result<(), Refusal> {
    if facts.catalogue_revision == 0
        || facts.physical.len() > 64
        || facts.fences.len() > 8
        || facts.receipts.len() > 128
        || facts
            .physical
            .iter()
            .map(|row| row.key)
            .collect::<BTreeSet<_>>()
            .len()
            != facts.physical.len()
        || facts
            .fences
            .iter()
            .map(|f| f.namespace)
            .collect::<BTreeSet<_>>()
            .len()
            != facts.fences.len()
    {
        return Err(Refusal::Invalid);
    }
    for member in &graph.required {
        let row = facts
            .physical
            .iter()
            .find(|row| row.key == member.key)
            .ok_or(Refusal::Unprotected)?;
        if row.object != member.object || row.retired || (require_ready && !row.ready) {
            return Err(Refusal::Unprotected);
        }
    }
    Ok(())
}

fn report_event(report: &Report) -> a::Event {
    let WorkerIdentity {
        attempt,
        worker,
        incarnation,
    } = report.identity;
    match report.classification {
        Classification::Complete => a::Event::Complete {
            attempt,
            worker,
            incarnation,
            result: report.payload.clone(),
        },
        Classification::MemoryFailure {
            original_charged,
            peak_memory_bytes,
            quiescent,
        } => a::Event::MemoryFailure {
            attempt,
            worker,
            incarnation,
            charged: original_charged,
            peak_memory_bytes,
            quiescent,
            result: report.payload.clone(),
        },
    }
}

pub(crate) fn binding(context: &Context, request: &a::Request) -> Result<QualifiedAction, Refusal> {
    let payload = request.payload.as_ref().ok_or(Refusal::Unsupported)?;
    let wire = serde_json::to_value(payload).map_err(|_| Refusal::Invalid)?;
    if wire["principal"].as_str() != Some(&context.principal) {
        return Err(Refusal::Unauthorized);
    }
    let projected = m1::Reference::captured_request_facts(wire.clone()).ok_or(Refusal::Invalid)?;
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
    let command = ReapiCommand::decode(command_bytes.as_slice()).map_err(|_| Refusal::Invalid)?;
    let platform = action
        .platform
        .as_ref()
        .filter(|p| !p.properties.is_empty())
        .or(command.platform.as_ref());
    Ok(QualifiedAction {
        key: request.action.clone(),
        context: context.clone(),
        semantics: platform.map_or_else(BTreeMap::new, |p| {
            p.properties
                .iter()
                .map(|p| (p.name.clone(), p.value.clone()))
                .collect()
        }),
    })
}

pub(crate) fn response_bytes(payload: &a::ResultPayload) -> Result<Vec<u8>, Refusal> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Captured {
        version: u32,
        encoded: Vec<u8>,
        sha256: [u8; 32],
    }
    let captured: Captured = serde_json::from_value(
        serde_json::to_value(payload.response.as_ref().ok_or(Refusal::Unsupported)?)
            .map_err(|_| Refusal::Invalid)?,
    )
    .map_err(|_| Refusal::Invalid)?;
    let hash = Sha256::digest(&captured.encoded);
    if captured.version != 1
        || captured.encoded.len() > 256 * 1024
        || captured.sha256 != <[u8; 32]>::from(hash)
        || payload.digest != format!("{hash:x}")
    {
        return Err(Refusal::Invalid);
    }
    Ok(captured.encoded)
}

fn response(payload: &a::ResultPayload) -> Result<ExecuteResponse, Refusal> {
    let response = ExecuteResponse::decode(response_bytes(payload)?.as_slice())
        .map_err(|_| Refusal::Invalid)?;
    let code = response.status.as_ref().map_or(0, |s| s.code);
    let expected_error = (code != 0).then(|| {
        format!(
            "REAPI status {code}: {}",
            response.status.as_ref().map_or("", |s| s.message.as_str())
        )
    });
    if (code == 0 && response.result.is_none())
        || payload.exit_code != response.result.as_ref().map_or(0, |r| r.exit_code)
        || payload.error != expected_error
    {
        return Err(Refusal::Invalid);
    }
    Ok(response)
}
