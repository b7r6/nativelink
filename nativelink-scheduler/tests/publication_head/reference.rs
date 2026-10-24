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

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::types::*;

type Result<T> = std::result::Result<T, Reject>;

pub(crate) fn object(bytes: &[u8], algorithm: Algorithm) -> Object {
    Object {
        algorithm,
        hash: match algorithm {
            Algorithm::Sha256 => Sha256::digest(bytes).into(),
            Algorithm::Blake3 => *blake3::hash(bytes).as_bytes(),
        },
        bytes: bytes.len() as u64,
    }
}

fn keys(candidate: &Candidate) -> BTreeSet<Key> {
    candidate.required.iter().map(|member| member.key).collect()
}

fn scenario_valid(scenario: &Scenario) -> Result<()> {
    if scenario.actions.len() > MAX_EXECUTIONS || scenario.candidates.len() > MAX_STAGES * 2 {
        return Err(Reject::Bounds);
    }
    if scenario
        .actions
        .keys()
        .any(|key| key.is_empty() || key.len() > 512)
    {
        return Err(Reject::Invalid);
    }
    for (id, candidate) in &scenario.candidates {
        if *id == 0
            || *id != candidate.id
            || !scenario.actions.contains_key(&candidate.action)
            || candidate.bytes.len() > MAX_PAYLOAD
            || candidate.response.is_empty()
            || candidate.response.len() > MAX_PAYLOAD
            || candidate.required.is_empty()
            || candidate.required.len() > MAX_MEMBERS
            || object(&candidate.bytes, candidate.result.algorithm) != candidate.result
        {
            return Err(Reject::Invalid);
        }
        if !candidate
            .required
            .iter()
            .any(|member| member.object == candidate.result && member.role == Role::Result)
        {
            return Err(Reject::Incomplete);
        }
        let mut physical = BTreeMap::new();
        let mut logical = BTreeMap::new();
        for member in &candidate.required {
            if member.key.incarnation == [0; 16]
                || member.key.generation == 0
                || physical
                    .insert(member.key, &member.object)
                    .is_some_and(|old| old != &member.object)
                || logical
                    .insert(&member.object, member.key)
                    .is_some_and(|old| old != member.key)
            {
                return Err(Reject::Invalid);
            }
        }
        if let Source::Worker { execution, attempt } = candidate.source {
            if execution == 0 || attempt.id == 0 {
                return Err(Reject::Invalid);
            }
        } else if scenario.actions[&candidate.action]
            || !candidate.successful
            || !candidate.error_free
        {
            // Direct-update policy beyond the canonical successful cacheable case
            // is deliberately outside this oracle's scenario alphabet.
            return Err(Reject::Invalid);
        }
    }
    Ok(())
}

fn facts_valid(facts: &Facts) -> Result<()> {
    if facts.physical.len() > MAX_STAGES * MAX_MEMBERS
        || facts.fences.len() > MAX_MEMBERS
        || facts.receipts.len() > MAX_STAGES * MAX_MEMBERS * 2
    {
        return Err(Reject::Bounds);
    }
    let mut seen = BTreeSet::new();
    if facts.catalogue_revision == 0 || facts.physical.iter().any(|row| !seen.insert(row.key)) {
        return Err(Reject::Invalid);
    }
    let mut namespaces = BTreeSet::new();
    if facts
        .fences
        .iter()
        .any(|fence| !namespaces.insert(fence.namespace))
    {
        return Err(Reject::Invalid);
    }
    Ok(())
}

fn ready(candidate: &Candidate, facts: &Facts) -> Result<()> {
    for member in &candidate.required {
        let key = member.key;
        let physical = facts
            .physical
            .iter()
            .find(|row| row.key == key)
            .ok_or(Reject::Unprotected)?;
        if physical.object != member.object || !physical.ready || physical.retired {
            return Err(Reject::Unprotected);
        }
    }
    Ok(())
}

fn proof_shape(candidate: &Candidate, stage: u64, proof: &Preparation) -> Result<()> {
    if proof.id == 0
        || proof.stage != stage
        || proof.result != candidate.result
        || !proof.complete
        || !proof.verified
    {
        return Err(Reject::Incomplete);
    }
    if proof.typed != candidate.required || proof.protected != keys(candidate) {
        return Err(Reject::Incomplete);
    }
    let namespaces: BTreeSet<_> = proof.protected.iter().map(|key| key.namespace).collect();
    let proof_namespaces: BTreeSet<_> = proof.fences.iter().map(|fence| fence.namespace).collect();
    if proof_namespaces != namespaces || proof_namespaces.len() != proof.fences.len() {
        return Err(Reject::Incomplete);
    }
    Ok(())
}

fn proof_current(
    candidate: &Candidate,
    stage: u64,
    proof: &Preparation,
    facts: &Facts,
    now: u64,
    until: u64,
) -> Result<()> {
    proof_shape(candidate, stage, proof)?;
    if proof.catalogue_revision != facts.catalogue_revision
        || !proof.fences.is_subset(&facts.fences)
        || proof.until < until
        || proof.until <= now
    {
        return Err(Reject::Stale);
    }
    ready(candidate, facts)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Model {
    pub(crate) scenario: Scenario,
    pub(crate) now: u64,
    pub(crate) applied: u64,
    pub(crate) workers: BTreeMap<u64, u64>,
    pub(crate) worker_epochs: BTreeMap<u64, u64>,
    pub(crate) clients: BTreeSet<u64>,
    pub(crate) operations: BTreeMap<u64, Operation>,
    pub(crate) executions: BTreeMap<u64, Execution>,
    pub(crate) stages: BTreeMap<u64, Stage>,
    pub(crate) accepted: BTreeMap<u64, Accepted>,
    pub(crate) heads: BTreeMap<String, u64>,
    pub(crate) index: BTreeMap<String, u64>,
    pub(crate) coverage: Vec<Coverage>,
    pub(crate) grants: BTreeMap<u64, Grant>,
}

impl Model {
    pub(crate) fn new(scenario: Scenario) -> Result<Self> {
        scenario_valid(&scenario)?;
        Ok(Self {
            scenario,
            now: 0,
            applied: 0,
            workers: BTreeMap::new(),
            worker_epochs: BTreeMap::new(),
            clients: BTreeSet::new(),
            operations: BTreeMap::new(),
            executions: BTreeMap::new(),
            stages: BTreeMap::new(),
            accepted: BTreeMap::new(),
            heads: BTreeMap::new(),
            index: BTreeMap::new(),
            coverage: vec![],
            grants: BTreeMap::new(),
        })
    }

    pub(crate) fn apply(&mut self, at: u64, event: &Event, facts: &Facts) -> Result<Reply> {
        if at <= self.applied {
            return Err(Reject::Stale);
        }
        facts_valid(facts)?;
        let mut next = self.clone();
        let reply = next.reduce(at, event, facts)?;
        if reply != Reply::Ignored {
            next.applied = at;
        }
        next.validate()?;
        *self = next;
        Ok(reply)
    }

    fn deadline(&self, until: u64, ticks: u64) -> Result<()> {
        if until <= self.now || self.now.checked_add(ticks).is_none_or(|end| until > end) {
            return Err(Reject::Bounds);
        }
        Ok(())
    }

    fn root(&self, root: Root) -> Option<&Coverage> {
        self.coverage.iter().find(|row| row.root == root)
    }

    fn protect(&mut self, root: Root, members: BTreeSet<Key>, until: u64) {
        if let Some(row) = self.coverage.iter_mut().find(|row| row.root == root) {
            row.keys = members;
            row.until = until;
        } else {
            self.coverage.push(Coverage {
                root,
                keys: members,
                until,
            });
            self.coverage.sort_by_key(|row| row.root);
        }
    }

    fn release(&mut self, root: Root) {
        if let Some(row) = self.coverage.iter_mut().find(|row| row.root == root) {
            row.until = row.until.min(self.now);
        }
    }

    fn candidate(&self, publication: u64) -> Result<&Candidate> {
        let accepted = self.accepted.get(&publication).ok_or(Reject::Invalid)?;
        self.scenario
            .candidates
            .get(&accepted.candidate)
            .ok_or(Reject::Invalid)
    }

    fn live_head(&self, action: &str) -> Option<u64> {
        self.heads.get(action).copied().filter(|id| {
            self.root(Root::Result(*id))
                .is_some_and(|root| root.until > self.now)
        })
    }

    fn projection(&self) -> BTreeMap<String, u64> {
        self.heads
            .keys()
            .filter_map(|action| self.live_head(action).map(|id| (action.clone(), id)))
            .collect()
    }

    fn reduce(&mut self, at: u64, event: &Event, facts: &Facts) -> Result<Reply> {
        match event {
            Event::Advance(now) => {
                if *now < self.now {
                    return Err(Reject::Stale);
                }
                if *now == self.now {
                    return Ok(Reply::Ignored);
                }
                self.now = *now;
            }
            Event::Register {
                worker,
                incarnation,
            } => {
                if self.workers.get(worker) == Some(incarnation) {
                    return Ok(Reply::Ignored);
                }
                if self
                    .worker_epochs
                    .get(worker)
                    .is_some_and(|old| incarnation <= old)
                {
                    return Err(Reject::Stale);
                }
                if self.worker_epochs.len() >= MAX_EXECUTIONS
                    && !self.worker_epochs.contains_key(worker)
                {
                    return Err(Reject::Bounds);
                }
                self.worker_epochs.insert(*worker, *incarnation);
                self.workers.insert(*worker, *incarnation);
            }
            Event::Revoke {
                worker,
                incarnation,
            } => {
                if self.workers.get(worker) != Some(incarnation) {
                    return Err(Reject::Stale);
                }
                self.workers.remove(worker);
            }
            Event::AuthorizeClient(principal) => {
                if self.clients.contains(principal) {
                    return Ok(Reply::Ignored);
                }
                if self.clients.len() == MAX_EXECUTIONS {
                    return Err(Reject::Bounds);
                }
                self.clients.insert(*principal);
            }
            Event::Admit(request) => {
                if let Some(old) = self.operations.get(&request.operation) {
                    return if old.request == *request {
                        Ok(Reply::Ignored)
                    } else {
                        Err(Reject::Conflict)
                    };
                }
                if request.operation == 0 || request.execution_if_needed == 0 {
                    return Err(Reject::Invalid);
                }
                if self.operations.len() == MAX_OPERATIONS {
                    return Err(Reject::Bounds);
                }
                self.deadline(request.result_until, LEASE_TICKS)?;
                let noncacheable = *self
                    .scenario
                    .actions
                    .get(&request.action)
                    .ok_or(Reject::Invalid)?;
                if !noncacheable
                    && !request.skip_lookup
                    && let Some(publication) = self.live_head(&request.action)
                {
                    let candidate = self.candidate(publication)?.clone();
                    ready(&candidate, facts)?;
                    self.protect(
                        Root::Operation(request.operation),
                        keys(&candidate),
                        request.result_until,
                    );
                    self.operations.insert(
                        request.operation,
                        Operation {
                            request: request.clone(),
                            state: OperationState::Terminal(Terminal {
                                publication,
                                response: candidate.response,
                                commit: at,
                                cached: true,
                            }),
                        },
                    );
                    return Ok(Reply::Hit(publication));
                }
                if !noncacheable
                    && let Some((&execution, _)) = self
                        .executions
                        .iter()
                        .find(|(_, row)| row.action == request.action && row.accepted.is_none())
                {
                    self.operations.insert(
                        request.operation,
                        Operation {
                            request: request.clone(),
                            state: OperationState::Waiting(execution),
                        },
                    );
                    return Ok(Reply::Joined(execution));
                }
                if self.executions.contains_key(&request.execution_if_needed) {
                    return Err(Reject::Conflict);
                }
                if self.executions.len() == MAX_EXECUTIONS {
                    return Err(Reject::Bounds);
                }
                self.executions.insert(
                    request.execution_if_needed,
                    Execution {
                        action: request.action.clone(),
                        attempt: None,
                        accepted: None,
                    },
                );
                self.operations.insert(
                    request.operation,
                    Operation {
                        request: request.clone(),
                        state: OperationState::Waiting(request.execution_if_needed),
                    },
                );
                return Ok(Reply::Started(request.execution_if_needed));
            }
            Event::CancelJoined(operation) => {
                let row = self.operations.get(operation).ok_or(Reject::Invalid)?;
                if matches!(row.state, OperationState::Canceled(_)) {
                    return Ok(Reply::Ignored);
                }
                let OperationState::Waiting(execution) = row.state else {
                    return Err(Reject::WrongPhase);
                };
                if self
                    .operations
                    .values()
                    .filter(|op| op.state == OperationState::Waiting(execution))
                    .count()
                    < 2
                {
                    return Err(Reject::WrongPhase);
                }
                self.operations
                    .get_mut(operation)
                    .ok_or(Reject::Invalid)?
                    .state = OperationState::Canceled(at);
            }
            Event::Assign { execution, attempt } => {
                if attempt.id == 0
                    || self.workers.get(&attempt.worker) != Some(&attempt.incarnation)
                {
                    return Err(Reject::Unauthorized);
                }
                let row = self.executions.get_mut(execution).ok_or(Reject::Invalid)?;
                if row.accepted.is_some() {
                    return Err(Reject::WrongPhase);
                }
                if row.attempt == Some(*attempt) {
                    return Ok(Reply::Ignored);
                }
                if row.attempt.is_some_and(|old| attempt.id <= old.id) {
                    return Err(Reject::Stale);
                }
                row.attempt = Some(*attempt);
            }
            Event::Stage {
                id,
                candidate,
                until,
                writes,
            } => {
                if let Some(old) = self.stages.get(id) {
                    return if old.candidate == *candidate
                        && old.until == *until
                        && old.writes == *writes
                    {
                        Ok(Reply::Ignored)
                    } else {
                        Err(Reject::Conflict)
                    };
                }
                if *id == 0 || writes.len() > MAX_MEMBERS || writes.contains_key(&0) {
                    return Err(Reject::Invalid);
                }
                if self.stages.len() == MAX_STAGES {
                    return Err(Reject::Bounds);
                }
                self.deadline(*until, STAGE_TICKS)?;
                let candidate_row = self
                    .scenario
                    .candidates
                    .get(candidate)
                    .ok_or(Reject::Invalid)?;
                if let Source::Worker { execution, .. } = candidate_row.source {
                    let execution = self.executions.get(&execution).ok_or(Reject::Invalid)?;
                    if execution.action != candidate_row.action || execution.accepted.is_some() {
                        return Err(Reject::WrongPhase);
                    }
                }
                let members = keys(candidate_row);
                if writes.values().any(|key| !members.contains(key))
                    || self
                        .stages
                        .values()
                        .any(|stage| writes.keys().any(|id| stage.writes.contains_key(id)))
                {
                    return Err(Reject::Conflict);
                }
                for member in &candidate_row.required {
                    let row = facts
                        .physical
                        .iter()
                        .find(|row| row.key == member.key)
                        .ok_or(Reject::Invalid)?;
                    if row.object != member.object || row.retired {
                        return Err(Reject::Unprotected);
                    }
                }
                self.protect(Root::Stage(*id), members, *until);
                self.stages.insert(
                    *id,
                    Stage {
                        candidate: *candidate,
                        until: *until,
                        writes: writes.clone(),
                        outcomes: BTreeMap::new(),
                        preparation: None,
                        state: StageState::Open,
                    },
                );
            }
            Event::Observe(receipt) => {
                let stage = self.stages.get(&receipt.stage).ok_or(Reject::Invalid)?;
                if stage.writes.get(&receipt.write) != Some(&receipt.key) {
                    return Err(Reject::Invalid);
                }
                let candidate = &self.scenario.candidates[&stage.candidate];
                if !candidate
                    .required
                    .iter()
                    .any(|member| member.key == receipt.key && member.object == receipt.object)
                    || !facts.receipts.contains(receipt)
                {
                    return Err(Reject::Unauthorized);
                }
                if let Some(old) = stage.outcomes.get(&receipt.write) {
                    return if *old == receipt.success {
                        Ok(Reply::Ignored)
                    } else {
                        Err(Reject::Conflict)
                    };
                }
                self.stages
                    .get_mut(&receipt.stage)
                    .ok_or(Reject::Invalid)?
                    .outcomes
                    .insert(receipt.write, receipt.success);
            }
            Event::Prepare { stage, evidence } => {
                let row = self.stages.get(stage).ok_or(Reject::Invalid)?;
                if row.state != StageState::Open || row.until <= self.now {
                    return Err(Reject::WrongPhase);
                }
                proof_current(
                    &self.scenario.candidates[&row.candidate],
                    *stage,
                    evidence,
                    facts,
                    self.now,
                    self.now.saturating_add(1),
                )?;
                self.stages
                    .get_mut(stage)
                    .ok_or(Reject::Invalid)?
                    .preparation = Some(evidence.clone());
            }
            Event::Abandon(stage) | Event::ExpireStage(stage) => {
                let row = self.stages.get(stage).ok_or(Reject::Invalid)?;
                if row.state == StageState::Abandoned {
                    return Ok(Reply::Ignored);
                }
                if row.state != StageState::Open {
                    return Err(Reject::WrongPhase);
                }
                if matches!(event, Event::ExpireStage(_)) && row.until > self.now {
                    return Err(Reject::WrongPhase);
                }
                self.stages.get_mut(stage).ok_or(Reject::Invalid)?.state = StageState::Abandoned;
                self.release(Root::Stage(*stage));
            }
            Event::Publish {
                stage,
                publication,
                until,
            } => {
                let row = self.stages.get(stage).ok_or(Reject::Invalid)?.clone();
                if row.state == StageState::Accepted(*publication) {
                    return if self.accepted[publication].until == *until {
                        Ok(Reply::Ignored)
                    } else {
                        Err(Reject::Conflict)
                    };
                }
                if row.state != StageState::Open || row.until <= self.now {
                    return Err(Reject::WrongPhase);
                }
                if *publication == 0 || self.accepted.contains_key(publication) {
                    return Err(Reject::Conflict);
                }
                self.deadline(*until, LEASE_TICKS)?;
                let candidate = self.scenario.candidates[&row.candidate].clone();
                proof_current(
                    &candidate,
                    *stage,
                    row.preparation.as_ref().ok_or(Reject::Incomplete)?,
                    facts,
                    self.now,
                    *until,
                )?;
                match candidate.source {
                    Source::Worker { execution, attempt } => {
                        let current = self.executions.get(&execution).ok_or(Reject::Invalid)?;
                        if current.accepted.is_some()
                            || current.attempt != Some(attempt)
                            || self.workers.get(&attempt.worker) != Some(&attempt.incarnation)
                        {
                            return Err(Reject::Unauthorized);
                        }
                        if self.operations.values().any(|operation| {
                            operation.state == OperationState::Waiting(execution)
                                && operation.request.result_until > *until
                        }) {
                            return Err(Reject::Unprotected);
                        }
                    }
                    Source::Client { principal } => {
                        if !self.clients.contains(&principal) {
                            return Err(Reject::Unauthorized);
                        }
                    }
                }
                let eligible = !self.scenario.actions[&candidate.action]
                    && candidate.successful
                    && candidate.error_free;
                let members = keys(&candidate);
                self.accepted.insert(
                    *publication,
                    Accepted {
                        stage: *stage,
                        candidate: row.candidate,
                        commit: at,
                        source: candidate.source,
                        eligible,
                        until: *until,
                    },
                );
                self.protect(Root::Result(*publication), members.clone(), *until);
                if let Source::Worker { execution, .. } = candidate.source {
                    self.executions
                        .get_mut(&execution)
                        .ok_or(Reject::Invalid)?
                        .accepted = Some(*publication);
                    let operations: Vec<_> = self
                        .operations
                        .iter()
                        .filter_map(|(id, op)| {
                            (op.state == OperationState::Waiting(execution)).then_some(*id)
                        })
                        .collect();
                    for id in operations {
                        self.operations.get_mut(&id).ok_or(Reject::Invalid)?.state =
                            OperationState::Terminal(Terminal {
                                publication: *publication,
                                response: candidate.response.clone(),
                                commit: at,
                                cached: false,
                            });
                        self.protect(Root::Operation(id), members.clone(), *until);
                    }
                }
                if eligible && let Some(old) = self.heads.insert(candidate.action, *publication) {
                    self.release(Root::Result(old));
                }
                self.stages.get_mut(stage).ok_or(Reject::Invalid)?.state =
                    StageState::Accepted(*publication);
                self.release(Root::Stage(*stage));
            }
            Event::Materialize(publication) => {
                let candidate = self.candidate(*publication)?;
                if self.live_head(&candidate.action) != Some(*publication) {
                    return Ok(Reply::Ignored);
                }
                if self.index.get(&candidate.action) == Some(publication) {
                    return Ok(Reply::Ignored);
                }
                self.index.insert(candidate.action.clone(), *publication);
            }
            Event::LoseIndex => {
                if self.index.is_empty() {
                    return Ok(Reply::Ignored);
                }
                self.index.clear();
            }
            Event::RebuildIndex => {
                let rebuilt = self.projection();
                if self.index == rebuilt {
                    return Ok(Reply::Ignored);
                }
                self.index = rebuilt;
            }
            Event::Serve {
                action,
                expected,
                barrier,
                grant,
                until,
            } => {
                if *barrier != self.applied || self.heads.get(action) != Some(expected) {
                    return Err(Reject::Stale);
                }
                if self.live_head(action) != Some(*expected) {
                    return Err(Reject::Miss);
                }
                if *grant == 0 || self.grants.contains_key(grant) {
                    return Err(Reject::Conflict);
                }
                if self.grants.len() == MAX_GRANTS {
                    return Err(Reject::Bounds);
                }
                self.deadline(*until, LEASE_TICKS)?;
                let candidate = self.candidate(*expected)?.clone();
                ready(&candidate, facts)?;
                self.protect(Root::Serving(*grant), keys(&candidate), *until);
                self.grants.insert(
                    *grant,
                    Grant {
                        publication: *expected,
                        selected_at: at,
                        until: *until,
                    },
                );
                return Ok(Reply::Granted(*expected));
            }
            Event::Reprotect {
                publication,
                evidence,
                until,
            } => {
                let accepted = self.accepted.get(publication).ok_or(Reject::Invalid)?;
                let candidate = self.candidate(*publication)?.clone();
                if self.heads.get(&candidate.action) != Some(publication) {
                    return Err(Reject::Stale);
                }
                self.deadline(*until, LEASE_TICKS)?;
                proof_current(
                    &candidate,
                    accepted.stage,
                    evidence,
                    facts,
                    self.now,
                    *until,
                )?;
                self.protect(Root::Result(*publication), keys(&candidate), *until);
            }
        }
        Ok(Reply::Applied)
    }

    pub(crate) fn protected(&self) -> BTreeSet<Key> {
        self.coverage
            .iter()
            .filter(|row| row.until > self.now)
            .flat_map(|row| row.keys.iter().copied())
            .collect()
    }

    pub(crate) fn can_read(&self, grant: u64, member: &Member) -> bool {
        self.grants.get(&grant).is_some_and(|row| {
            row.until > self.now
                && self
                    .candidate(row.publication)
                    .is_ok_and(|candidate| candidate.required.contains(member))
                && self
                    .root(Root::Serving(grant))
                    .is_some_and(|root| root.until >= row.until && root.keys.contains(&member.key))
        })
    }

    pub(crate) fn unresolved_writes(&self) -> BTreeSet<(u64, u64)> {
        self.stages
            .iter()
            .flat_map(|(id, stage)| {
                stage
                    .writes
                    .keys()
                    .filter(|write| !stage.outcomes.contains_key(write))
                    .map(move |write| (*id, *write))
            })
            .collect()
    }

    fn validate(&self) -> Result<()> {
        scenario_valid(&self.scenario)?;
        if self.operations.len() > MAX_OPERATIONS
            || self.executions.len() > MAX_EXECUTIONS
            || self.stages.len() > MAX_STAGES
            || self.accepted.len() > MAX_STAGES
            || self.grants.len() > MAX_GRANTS
            || self.workers.len() > MAX_EXECUTIONS
            || self.worker_epochs.len() > MAX_EXECUTIONS
            || self.clients.len() > MAX_EXECUTIONS
            || self.coverage.len() > MAX_STAGES * 2 + MAX_OPERATIONS + MAX_GRANTS
        {
            return Err(Reject::Bounds);
        }
        if self
            .workers
            .iter()
            .any(|(worker, incarnation)| self.worker_epochs.get(worker) != Some(incarnation))
        {
            return Err(Reject::Invalid);
        }
        let mut expected_heads = BTreeMap::<String, (u64, u64)>::new();
        let mut commits = BTreeSet::new();
        for (id, accepted) in &self.accepted {
            let stage = self.stages.get(&accepted.stage).ok_or(Reject::Invalid)?;
            let candidate = self
                .scenario
                .candidates
                .get(&accepted.candidate)
                .ok_or(Reject::Invalid)?;
            let eligible = !self.scenario.actions[&candidate.action]
                && candidate.successful
                && candidate.error_free;
            if *id == 0
                || accepted.commit == 0
                || accepted.commit > self.applied
                || !commits.insert(accepted.commit)
                || stage.state != StageState::Accepted(*id)
                || stage.candidate != candidate.id
                || accepted.source != candidate.source
                || accepted.eligible != eligible
                || self.root(Root::Result(*id)).is_none()
            {
                return Err(Reject::Invalid);
            }
            proof_shape(
                candidate,
                accepted.stage,
                stage.preparation.as_ref().ok_or(Reject::Incomplete)?,
            )?;
            if eligible {
                let head = expected_heads.entry(candidate.action.clone()).or_default();
                if accepted.commit > head.0 {
                    *head = (accepted.commit, *id);
                }
            }
        }
        if self.heads
            != expected_heads
                .into_iter()
                .map(|(key, (_, id))| (key, id))
                .collect()
        {
            return Err(Reject::Invalid);
        }
        for (action, id) in &self.index {
            if self.candidate(*id)?.action != *action || !self.accepted[id].eligible {
                return Err(Reject::Invalid);
            }
        }
        for (id, op) in &self.operations {
            if *id == 0
                || *id != op.request.operation
                || !self.scenario.actions.contains_key(&op.request.action)
            {
                return Err(Reject::Invalid);
            }
            match &op.state {
                OperationState::Waiting(execution) => {
                    let row = self.executions.get(execution).ok_or(Reject::Invalid)?;
                    if row.action != op.request.action || row.accepted.is_some() {
                        return Err(Reject::Invalid);
                    }
                }
                OperationState::Terminal(terminal) => {
                    let candidate = self.candidate(terminal.publication)?;
                    let promised_until = if terminal.cached {
                        op.request.result_until
                    } else {
                        self.accepted[&terminal.publication].until
                    };
                    if candidate.action != op.request.action
                        || candidate.response != terminal.response
                        || terminal.commit < self.accepted[&terminal.publication].commit
                        || terminal.commit > self.applied
                        || self
                            .root(Root::Operation(*id))
                            .is_none_or(|root| root.until != promised_until)
                    {
                        return Err(Reject::Invalid);
                    }
                    if terminal.cached
                        && (op.request.skip_lookup || self.scenario.actions[&op.request.action])
                    {
                        return Err(Reject::Invalid);
                    }
                    if !terminal.cached
                        && terminal.commit != self.accepted[&terminal.publication].commit
                    {
                        return Err(Reject::Invalid);
                    }
                }
                OperationState::Canceled(commit) => {
                    if *commit == 0
                        || *commit > self.applied
                        || self.root(Root::Operation(*id)).is_some()
                    {
                        return Err(Reject::Invalid);
                    }
                }
            }
        }
        let mut write_ids = BTreeSet::new();
        for (id, stage) in &self.stages {
            let candidate = self
                .scenario
                .candidates
                .get(&stage.candidate)
                .ok_or(Reject::Invalid)?;
            let members = keys(candidate);
            if *id == 0
                || stage.writes.len() > MAX_MEMBERS
                || stage
                    .writes
                    .iter()
                    .any(|(id, key)| *id == 0 || !write_ids.insert(*id) || !members.contains(key))
                || stage
                    .outcomes
                    .keys()
                    .any(|id| !stage.writes.contains_key(id))
                || self.root(Root::Stage(*id)).is_none()
            {
                return Err(Reject::Invalid);
            }
            if let Some(proof) = &stage.preparation {
                proof_shape(candidate, *id, proof)?;
            }
            if let StageState::Accepted(publication) = stage.state
                && self
                    .accepted
                    .get(&publication)
                    .is_none_or(|row| row.stage != *id)
            {
                return Err(Reject::Invalid);
            }
        }
        let mut root_ids = BTreeSet::new();
        for row in &self.coverage {
            if !root_ids.insert(row.root) {
                return Err(Reject::Invalid);
            }
            let candidate = match row.root {
                Root::Stage(id) => {
                    let stage = self.stages.get(&id).ok_or(Reject::Invalid)?;
                    let now = self.now;
                    if row.until > stage.until
                        || (stage.state == StageState::Open && row.until != stage.until)
                        || (stage.state != StageState::Open && row.until > now)
                    {
                        return Err(Reject::Invalid);
                    }
                    &self.scenario.candidates[&stage.candidate]
                }
                Root::Result(id) => self.candidate(id)?,
                Root::Operation(id) => {
                    let OperationState::Terminal(terminal) =
                        &self.operations.get(&id).ok_or(Reject::Invalid)?.state
                    else {
                        return Err(Reject::Invalid);
                    };
                    self.candidate(terminal.publication)?
                }
                Root::Serving(id) => {
                    let grant = self.grants.get(&id).ok_or(Reject::Invalid)?;
                    if row.until < grant.until {
                        return Err(Reject::Unprotected);
                    }
                    self.candidate(grant.publication)?
                }
            };
            if row.keys != keys(candidate) {
                return Err(Reject::Incomplete);
            }
        }
        for (id, grant) in &self.grants {
            if *id == 0
                || !self.accepted.contains_key(&grant.publication)
                || grant.selected_at == 0
                || grant.selected_at > self.applied
                || self.root(Root::Serving(*id)).is_none()
            {
                return Err(Reject::Invalid);
            }
        }
        for (id, execution) in &self.executions {
            if *id == 0 || !self.scenario.actions.contains_key(&execution.action) {
                return Err(Reject::Invalid);
            }
            if let Some(publication) = execution.accepted
                && !matches!(self.accepted.get(&publication).map(|row| row.source),Some(Source::Worker {execution,..}) if execution == *id)
            {
                return Err(Reject::Invalid);
            }
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| Reject::Invalid)?;
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Reject::Bounds);
        }
        Ok(bytes)
    }

    pub(crate) fn restore(bytes: &[u8], scenario: &Scenario) -> Result<Self> {
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Reject::Bounds);
        }
        let restored: Self = serde_json::from_slice(bytes).map_err(|_| Reject::Invalid)?;
        restored.validate()?;
        if restored.scenario != *scenario || restored.snapshot()? != bytes {
            return Err(Reject::Invalid);
        }
        Ok(restored)
    }
}
