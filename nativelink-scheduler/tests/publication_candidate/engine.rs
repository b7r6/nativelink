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

//! One outer atomic boundary over the real scheduler and coarse retention engine.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
use nativelink_scheduler::authority as a;
use prost::Message;
use serde::{Deserialize, Serialize};

use super::coarse::Coarse;
use super::protocol as r;
use super::types::*;

#[path = "binding.rs"]
mod binding;
#[path = "direct_client.rs"]
mod direct_client;
use direct_client::UpdateAuthorization;

#[path = "terminal.rs"]
mod terminal;

#[path = "validation.rs"]
mod validation;

const EPOCH: a::ClusterEpoch = a::ClusterEpoch(1);
const ROOT_CAP: usize = MAX_PHYSICAL + MAX_STAGES * 2 + MAX_OPERATIONS + MAX_GRANTS;
const WRITE_CAP: usize = MAX_PHYSICAL + MAX_STAGES * MAX_MEMBERS;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NamespaceBinding {
    namespace: [u8; 16],
    incarnation: [u8; 16],
    handle: u64,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Physical {
    key: Key,
    object: Object,
    logical: u64,
    bootstrap: Option<(u64, u64)>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Credit {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    closed: bool,
    physical_remaining: usize,
    stage: Option<u64>,
    prior_stages: Vec<u64>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    commit: u64,
    revision: u64,
    next_root: u64,
    next_write: u64,
    actions: BTreeMap<String, bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    real: Option<RealBindings>,
    credits: BTreeMap<u64, Credit>,
    clients: BTreeSet<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    update_authorizations: BTreeMap<u64, UpdateAuthorization>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal: Option<TerminalState>,
    namespaces: Vec<NamespaceBinding>,
    physical: Vec<Physical>,
    stages: BTreeMap<u64, Stage>,
    accepted: BTreeMap<u64, Accepted>,
    heads: BTreeMap<String, u64>,
    operations: BTreeMap<u64, Operation>,
    grants: BTreeMap<u64, Grant>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u32,
    scheduler: Vec<u8>,
    retention: Vec<u8>,
    metadata: Metadata,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicationAuthority {
    scheduler: a::State,
    retention: Coarse,
    metadata: Metadata,
    index: BTreeMap<String, u64>,
}

impl PublicationAuthority {
    pub(crate) fn new(actions: BTreeMap<String, bool>) -> Result<Self, Error> {
        if actions.len() > MAX_STAGES || actions.keys().any(|key| key.is_empty() || key.len() > 256)
        {
            return Err(Error::Bounds);
        }
        let state = Self {
            scheduler: a::State::new(
                EPOCH,
                a::Limits {
                    max_executions: MAX_STAGES,
                    max_operations: MAX_OPERATIONS,
                    max_workers: MAX_STAGES,
                    max_effects: MAX_STAGES * 8,
                    max_payload_bytes: MAX_PAYLOAD,
                },
            )
            .map_err(|_| Error::Invalid)?,
            retention: Coarse::new(r::Limits {
                cohorts: MAX_PHYSICAL,
                generations: MAX_PHYSICAL,
                roots: ROOT_CAP,
                closure: MAX_MEMBERS,
                mark_batch: MAX_MEMBERS,
                retire_batch: MAX_MEMBERS,
                writes: WRITE_CAP,
                delete_intents: MAX_PHYSICAL + WRITE_CAP,
                lease_ticks: LEASE_TICKS,
            })
            .map_err(|_| Error::Invalid)?,
            metadata: Metadata {
                commit: 0,
                revision: 0,
                next_root: 1,
                next_write: 1,
                actions,
                real: None,
                credits: BTreeMap::new(),
                clients: BTreeSet::new(),
                update_authorizations: BTreeMap::new(),
                terminal: None,
                namespaces: Vec::new(),
                physical: Vec::new(),
                stages: BTreeMap::new(),
                accepted: BTreeMap::new(),
                heads: BTreeMap::new(),
                operations: BTreeMap::new(),
                grants: BTreeMap::new(),
            },
            index: BTreeMap::new(),
        };
        state.validate()?;
        Ok(state)
    }

    fn transaction<T>(
        &mut self,
        apply: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut next = self.clone();
        next.metadata.commit = next.metadata.commit.checked_add(1).ok_or(Error::Bounds)?;
        let reply = apply(&mut next)?;
        next.reconcile_terminal_credits();
        if self.metadata.terminal.is_some() {
            next.metadata.commit = self.metadata.commit;
            if next.scheduler == self.scheduler
                && next.retention == self.retention
                && next.metadata == self.metadata
            {
                return Ok(reply);
            }
            next.metadata.commit = self.metadata.commit.checked_add(1).ok_or(Error::Bounds)?;
        }
        // This is a current-state byte admission gate, not a reservation for
        // every future outer-metadata transition of an admitted execution.
        next.snapshot()?;
        *self = next;
        Ok(reply)
    }

    fn scheduler_apply(&mut self, event: a::Event) -> Result<a::Reply, Error> {
        match self.scheduler.apply(a::RecordedEvent {
            epoch: EPOCH,
            event,
        }) {
            a::Reply::Rejected(reason) => Err(Error::Scheduler(reason)),
            reply => Ok(reply),
        }
    }
    fn retain(&mut self, event: r::Event) -> Result<(), Error> {
        match self.retention.apply(event) {
            r::Reply::Rejected => Err(Error::Unprotected),
            r::Reply::Applied | r::Reply::Ignored => Ok(()),
        }
    }
    fn root_id(&mut self) -> Result<u64, Error> {
        let id = self.metadata.next_root;
        if usize::try_from(id).map_err(|_| Error::Bounds)? > ROOT_CAP {
            return Err(Error::Bounds);
        }
        self.metadata.next_root = id.checked_add(1).ok_or(Error::Bounds)?;
        Ok(id)
    }
    fn write_id(&mut self) -> Result<u64, Error> {
        let id = self.metadata.next_write;
        if usize::try_from(id).map_err(|_| Error::Bounds)? > WRITE_CAP {
            return Err(Error::Bounds);
        }
        self.metadata.next_write = id.checked_add(1).ok_or(Error::Bounds)?;
        Ok(id)
    }
    fn now(&self) -> u64 {
        self.scheduler.view().now
    }
    fn valid_until(&self, until: u64, maximum: u64) -> bool {
        self.now()
            .checked_add(maximum)
            .is_some_and(|limit| until > self.now() && until <= limit)
    }
    fn key(&self, key: Key) -> Result<r::Key, Error> {
        let handle = self
            .metadata
            .namespaces
            .iter()
            .find(|row| row.namespace == key.namespace && row.incarnation == key.incarnation)
            .ok_or(Error::Invalid)?
            .handle;
        Ok(r::Key {
            incarnation: handle,
            generation: key.generation,
        })
    }
    fn keys(&self, closure: &BTreeSet<Member>) -> Result<Vec<r::Key>, Error> {
        let keys = closure
            .iter()
            .map(|member| self.key(member.key))
            .collect::<Result<BTreeSet<_>, _>>()?;
        Ok(keys.into_iter().collect())
    }
    pub(crate) fn fences(&self, closure: &BTreeSet<Member>) -> Result<BTreeMap<u64, u64>, Error> {
        let view = self.retention.view();
        self.keys(closure)?
            .iter()
            .map(|key| {
                let cohort = view.generations.get(key).ok_or(Error::Invalid)?.cohort;
                Ok((cohort.0, view.cohorts[&cohort].fence))
            })
            .collect()
    }
    pub(crate) const fn catalogue_revision(&self) -> u64 {
        self.metadata.revision
    }
    pub(crate) fn scheduler_view(&self) -> a::ObservableState {
        self.scheduler.view()
    }
    pub(crate) fn retention_view(&self) -> r::View {
        self.retention.view()
    }
    pub(crate) const fn stages(&self) -> &BTreeMap<u64, Stage> {
        &self.metadata.stages
    }
    pub(crate) const fn accepted(&self) -> &BTreeMap<u64, Accepted> {
        &self.metadata.accepted
    }
    pub(crate) const fn heads(&self) -> &BTreeMap<String, u64> {
        &self.metadata.heads
    }
    pub(crate) const fn operations(&self) -> &BTreeMap<u64, Operation> {
        &self.metadata.operations
    }
    pub(crate) const fn grants(&self) -> &BTreeMap<u64, Grant> {
        &self.metadata.grants
    }
    pub(crate) fn protected_keys(&self) -> BTreeSet<Key> {
        let protected = self.retention.view().protected;
        self.metadata
            .physical
            .iter()
            .filter(|row| self.key(row.key).is_ok_and(|key| protected.contains(&key)))
            .map(|row| row.key)
            .collect()
    }
    pub(crate) fn can_read(&self, grant: u64, member: &Member) -> bool {
        self.metadata.grants.get(&grant).is_some_and(|grant| {
            grant.until > self.now()
                && self
                    .candidate(grant.publication)
                    .is_ok_and(|candidate| candidate.closure.contains(member))
                && self
                    .retention
                    .view()
                    .roots
                    .get(&r::RootId(grant.root))
                    .is_some_and(|root| {
                        root.until > self.now()
                            && self
                                .key(member.key)
                                .is_ok_and(|key| root.closure.contains(&key))
                    })
        })
    }
    pub(crate) const fn index(&self) -> &BTreeMap<String, u64> {
        &self.index
    }
    pub(crate) const fn commit(&self) -> u64 {
        self.metadata.commit
    }

    /// Lifecycle events enter here; worker results require a publication or
    /// the separately qualified no-graph report boundary.
    pub(crate) fn scheduler_event(&mut self, event: a::Event) -> Result<a::Reply, Error> {
        if !(matches!(
            &event,
            a::Event::Register { .. }
                | a::Event::Capacity { .. }
                | a::Event::Heartbeat { .. }
                | a::Event::WorkerUnavailable { .. }
                | a::Event::Match
                | a::Event::WorkerAccepted { .. }
                | a::Event::Progress { .. }
                | a::Event::Cleanup { .. }
                | a::Event::EffectOutcome { .. }
        ) || (self.metadata.terminal.is_some()
            && matches!(
                &event,
                a::Event::Cancel { .. }
                    | a::Event::RenewInterest { .. }
                    | a::Event::ExpireInterest { .. }
                    | a::Event::ExpireAttempt { .. }
                    | a::Event::ExpireExecution { .. }
            )))
        {
            return Err(Error::Invalid);
        }
        self.transaction(|next| {
            let reply = next.scheduler_apply(event)?;
            if let a::Reply::Matched { execution, .. } = &reply {
                let credit = next
                    .metadata
                    .credits
                    .get(&execution.0)
                    .ok_or(Error::Corrupt)?;
                if let Some(old) = credit.stage {
                    let pending = next
                        .metadata
                        .credits
                        .values()
                        .filter(|row| row.stage.is_none() && !row.closed)
                        .count();
                    let physical = next.metadata.physical.len()
                        + next
                            .metadata
                            .credits
                            .values()
                            .map(|row| row.physical_remaining)
                            .sum::<usize>()
                        - credit.physical_remaining;
                    if next.metadata.stages.len() + pending >= MAX_STAGES
                        || physical + MAX_MEMBERS > MAX_PHYSICAL
                    {
                        return Err(Error::Bounds);
                    }
                    // A new actual attempt cannot become visible until its next
                    // candidate has prepaid capacity. Prior write obligations stay.
                    let credit = next.metadata.credits.get_mut(&execution.0).unwrap();
                    credit.prior_stages.push(old);
                    credit.stage = None;
                    credit.physical_remaining = MAX_MEMBERS;
                }
            }
            Ok(reply)
        })
    }

    pub(crate) fn advance(&mut self, now: u64) -> Result<(), Error> {
        self.transaction(|next| {
            next.scheduler_apply(a::Event::AdvanceTime { now })?;
            next.retain(r::Event::AdvanceTime { now })?;
            next.rebuild_index_inner();
            Ok(())
        })
    }
    pub(crate) fn authorize_client(&mut self, principal: u64) -> Result<(), Error> {
        if self.metadata.real.is_some() {
            return Err(Error::Unauthorized);
        }
        self.transaction(|next| {
            if principal == 0 || next.metadata.clients.len() >= MAX_STAGES {
                return Err(Error::Bounds);
            }
            next.metadata.clients.insert(principal);
            Ok(())
        })
    }

    /// Trusted physical catalogue observation; readiness is not inferred from RPC.
    pub(crate) fn install_physical(
        &mut self,
        key: Key,
        object: Object,
        ready: bool,
    ) -> Result<(), Error> {
        self.install_physical_for(None, key, object, ready)
    }

    /// New output generations spend the owning execution's prepaid member credit.
    pub(crate) fn install_physical_for(
        &mut self,
        execution: Option<a::ExecutionId>,
        key: Key,
        object: Object,
        ready: bool,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            if key.namespace == [0; 16]
                || key.incarnation == [0; 16]
                || key.generation == 0
                || !matches!(object.algorithm, 1 | 9)
                || object.bytes > MAX_PAYLOAD as u64
            {
                return Err(Error::Invalid);
            }
            if next.metadata.physical.iter().any(|row| row.key == key) {
                return Err(Error::Conflict);
            }
            if next.metadata.physical.len() >= MAX_PHYSICAL {
                return Err(Error::Bounds);
            }
            if let Some(execution) = execution {
                let credit = next
                    .metadata
                    .credits
                    .get_mut(&execution.0)
                    .ok_or(Error::Invalid)?;
                credit.physical_remaining = credit
                    .physical_remaining
                    .checked_sub(1)
                    .ok_or(Error::Bounds)?;
            } else if next.metadata.physical.len()
                + next
                    .metadata
                    .credits
                    .values()
                    .map(|credit| credit.physical_remaining)
                    .sum::<usize>()
                >= MAX_PHYSICAL
            {
                return Err(Error::Bounds);
            }
            if !next
                .metadata
                .namespaces
                .iter()
                .any(|row| row.namespace == key.namespace && row.incarnation == key.incarnation)
            {
                if next.metadata.namespaces.len() >= MAX_PHYSICAL {
                    return Err(Error::Bounds);
                }
                let handle =
                    u64::try_from(next.metadata.namespaces.len()).map_err(|_| Error::Bounds)? + 1;
                next.retain(r::Event::CreateCohort {
                    cohort: r::CohortId(handle),
                })?;
                next.metadata.namespaces.push(NamespaceBinding {
                    namespace: key.namespace,
                    incarnation: key.incarnation,
                    handle,
                });
            }
            let internal = next.key(key)?;
            let logical =
                u64::try_from(next.metadata.physical.len()).map_err(|_| Error::Bounds)? + 1;
            next.retain(r::Event::Allocate {
                cohort: r::CohortId(internal.incarnation),
                key: internal,
                logical_object: logical,
            })?;
            let bootstrap = if ready {
                let root = next.root_id()?;
                let write = next.write_id()?;
                let fence = next.retention.view().cohorts[&r::CohortId(internal.incarnation)].fence;
                next.retain(r::Event::Acquire {
                    root: r::RootId(root),
                    lease: 1,
                    kind: r::RootKind::Staging,
                    closure: vec![internal],
                    until: next.now().checked_add(1).ok_or(Error::Bounds)?,
                    fences: BTreeMap::from([(r::CohortId(internal.incarnation), fence)]),
                })?;
                next.retain(r::Event::StartWrite {
                    write: r::WriteId(write),
                    key: internal,
                    staging: r::RootId(root),
                    lease: 1,
                })?;
                next.retain(r::Event::WriteDone {
                    write: r::WriteId(write),
                    success: true,
                })?;
                next.retain(r::Event::Release {
                    root: r::RootId(root),
                    lease: 1,
                })?;
                Some((root, write))
            } else {
                None
            };
            next.metadata.physical.push(Physical {
                key,
                object,
                logical,
                bootstrap,
            });
            next.metadata.revision = next.metadata.revision.checked_add(1).ok_or(Error::Bounds)?;
            Ok(())
        })
    }

    pub(crate) fn admit(
        &mut self,
        operation: u64,
        action: String,
        skip_lookup: bool,
        until: u64,
    ) -> Result<a::ExecutionId, Error> {
        if self.metadata.real.is_some() {
            return Err(Error::Unauthorized);
        }
        let do_not_cache = *self.metadata.actions.get(&action).ok_or(Error::Invalid)?;
        let request = a::Request {
            action: a::ActionKey {
                instance: action.clone(),
                digest_function: "sha256".into(),
                digest: "00".repeat(32),
                digest_size_bytes: 0,
            },
            execution_scope: None,
            do_not_cache,
            skip_cache_lookup: skip_lookup,
            priority: 0,
            properties: BTreeMap::new(),
            resources: a::Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                slots: 1,
            },
            execution_timeout: 100,
            payload: None,
        };
        self.admit_bound(operation, action, request, until)
    }

    fn admit_bound(
        &mut self,
        operation: u64,
        action: String,
        request: a::Request,
        until: u64,
    ) -> Result<a::ExecutionId, Error> {
        self.transaction(|next| {
            if !next.request_matches_action(&action, &request) { return Err(Error::Unauthorized); }
            if next.metadata.operations.len() >= MAX_OPERATIONS || !next.valid_until(until, LEASE_TICKS) { return Err(Error::Bounds); }
            if next.metadata.operations.contains_key(&operation) { return Err(Error::Conflict); }
            let a::Reply::Submitted { execution, .. } = next.scheduler_apply(a::Event::Submit { operation: a::OperationId(operation), request })? else { return Err(Error::Conflict); };
            next.metadata.operations.insert(operation, Operation { execution, action, until, publication: None, root: None, cached: false, payload: None });
            let lookup = next.scheduler.view().effects.iter().find_map(|effect| matches!(effect.kind, a::EffectKind::Lookup { execution: id, .. } if id == execution).then_some(effect.id));
            if let Some(effect) = lookup {
                let publication = next.metadata.heads.get(&next.metadata.operations[&operation].action).copied().filter(|id| next.protected_head(*id, until));
                let payload = if let Some(id) = publication {
                    next.install_operation_root(operation, id)?;
                    let captured = Self::cached_payload(next.candidate(id)?)?;
                    let record = next.metadata.operations.get_mut(&operation).unwrap();
                    record.cached = true;
                    record.payload = Some(captured.clone());
                    Some(captured)
                } else { None };
                next.scheduler_apply(a::Event::LookupResult { effect, result: payload })?;
            }
            let view = next.scheduler.view();
            if view.executions.iter().any(|row| row.id == execution && row.terminal.is_none()) && !next.metadata.credits.contains_key(&execution.0) {
                let pending = next.metadata.credits.values().filter(|credit| credit.stage.is_none() && !credit.closed).count();
                let physical = next.metadata.physical.len() + next.metadata.credits.values().map(|credit| credit.physical_remaining).sum::<usize>();
                if next.metadata.stages.len() + pending >= MAX_STAGES || physical.checked_add(MAX_MEMBERS).is_none_or(|value| value > MAX_PHYSICAL) { return Err(Error::Bounds); }
                // Each reserved stage also prepays MAX_MEMBERS stable writes and
                // one accepted/result root; operation roots have separate caps.
                next.metadata.credits.insert(execution.0, Credit { closed: false, physical_remaining: MAX_MEMBERS, stage: None, prior_stages: Vec::new() });
            }
            Ok(execution)
        })
    }

    pub(crate) fn cancel_joined(&mut self, operation: u64) -> Result<(), Error> {
        self.transaction(|next| {
            let current = next
                .metadata
                .operations
                .get(&operation)
                .ok_or(Error::Invalid)?;
            if next
                .scheduler
                .view()
                .operations
                .iter()
                .filter(|row| {
                    row.execution == current.execution && row.terminal.is_none() && row.interested
                })
                .count()
                < 2
            {
                return Err(Error::Invalid);
            }
            next.scheduler_apply(a::Event::Cancel {
                operation: a::OperationId(operation),
            })?;
            Ok(())
        })
    }

    fn cached_payload(candidate: &Candidate) -> Result<a::ResultPayload, Error> {
        let result =
            ActionResult::decode(candidate.result_bytes.as_slice()).map_err(|_| Error::Invalid)?;
        a::ResultPayload::from_response(
            &ExecuteResponse {
                result: Some(result),
                cached_result: true,
                ..ExecuteResponse::default()
            },
            MAX_PAYLOAD,
        )
        .map_err(|_| Error::Invalid)
    }

    fn validate_candidate(&self, candidate: &Candidate) -> Result<(), Error> {
        if !self.metadata.actions.contains_key(&candidate.action)
            || candidate.result_bytes.len() > MAX_PAYLOAD
            || candidate.closure.is_empty()
            || candidate.closure.len() > MAX_MEMBERS
        {
            return Err(Error::Bounds);
        }
        if let Some(real) = &self.metadata.real {
            let algorithm = real.actions[&candidate.action].result_algorithm();
            if candidate.result.algorithm != algorithm
                || candidate
                    .closure
                    .iter()
                    .any(|member| member.object.algorithm != algorithm)
                || matches!(candidate.publisher, Publisher::Client { .. })
            {
                return Err(Error::Unauthorized);
            }
        }
        let payload = candidate
            .response
            .require_response()
            .map_err(|_| Error::Invalid)?;
        if payload.encoded().len() > MAX_PAYLOAD
            || serde_json::to_vec(&candidate.response)
                .map_err(|_| Error::Invalid)?
                .len()
                > self.scheduler.payload_limit_bytes()
        {
            return Err(Error::Bounds);
        }
        let response = payload.decode().map_err(|_| Error::Invalid)?;
        let result =
            ActionResult::decode(candidate.result_bytes.as_slice()).map_err(|_| Error::Invalid)?;
        if result.encode_to_vec() != candidate.result_bytes
            || response.result.as_ref() != Some(&result)
            || Object::for_bytes(candidate.result.algorithm, &candidate.result_bytes)?
                != candidate.result
        {
            return Err(Error::Invalid);
        }
        if !candidate
            .closure
            .iter()
            .any(|member| member.role == Role::Result && member.object == candidate.result)
        {
            return Err(Error::Incomplete);
        }
        for member in &candidate.closure {
            if !self
                .metadata
                .physical
                .iter()
                .any(|row| row.key == member.key && row.object == member.object)
            {
                return Err(Error::Incomplete);
            }
        }
        if let Publisher::Worker {
            execution,
            attempt,
            worker,
            incarnation,
        } = candidate.publisher
        {
            let scheduler = self.scheduler.view();
            let receipt = scheduler
                .attempts
                .iter()
                .find(|row| row.id == attempt)
                .ok_or(Error::Unauthorized)?;
            if receipt.execution != execution
                || receipt.worker != worker
                || receipt.incarnation != incarnation
            {
                return Err(Error::Unauthorized);
            }
            let request = &scheduler
                .executions
                .iter()
                .find(|row| row.id == execution)
                .ok_or(Error::Unauthorized)?
                .request;
            if !self.request_matches_action(&candidate.action, request) {
                return Err(Error::Unauthorized);
            }
        }
        if !self.metadata.actions[&candidate.action]
            && candidate.response.exit_code == 0
            && candidate.response.error.is_none()
            && serde_json::to_vec(&Self::cached_payload(candidate)?)
                .map_err(|_| Error::Invalid)?
                .len()
                > self.scheduler.payload_limit_bytes()
        {
            return Err(Error::Bounds);
        }
        self.validate_direct_candidate(candidate, &result, &response)?;
        if let Publisher::Client { principal } = candidate.publisher {
            if !self.metadata.clients.contains(&principal) {
                return Err(Error::Unauthorized);
            }
            if self.metadata.actions[&candidate.action]
                || candidate.response.exit_code != 0
                || candidate.response.error.is_some()
                || response.cached_result
            {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }

    fn validate_worker_stage(&self, candidate: &Candidate) -> Result<(), Error> {
        if let Publisher::Worker {
            attempt,
            worker,
            incarnation,
            ..
        } = candidate.publisher
        {
            // Admission must not let a stale publisher spend the current
            // execution's single stage credit. Reuse the actual reducer's
            // authority check; this preview and every effect it emits are
            // discarded. Publish repeats Complete on its outer commit clone.
            let mut preview = self.scheduler.clone();
            if let a::Reply::Rejected(reason) = preview.apply(a::RecordedEvent {
                epoch: EPOCH,
                event: a::Event::Complete {
                    attempt,
                    worker,
                    incarnation,
                    result: candidate.response.clone(),
                },
            }) {
                return Err(Error::Scheduler(reason));
            }
        }
        Ok(())
    }

    pub(crate) fn stage(
        &mut self,
        id: u64,
        candidate: Candidate,
        until: u64,
        writes: BTreeMap<u64, Key>,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            if id == 0 || next.metadata.stages.contains_key(&id) {
                return Err(Error::Conflict);
            }
            if next.metadata.stages.len() >= MAX_STAGES
                || writes.len() > MAX_MEMBERS
                || !next.valid_until(until, STAGE_TICKS)
            {
                return Err(Error::Bounds);
            }
            next.validate_candidate(&candidate)?;
            next.validate_worker_stage(&candidate)?;
            next.validate_current_update(&candidate)?;
            match candidate.publisher {
                Publisher::Worker { execution, .. } => {
                    let credit = next
                        .metadata
                        .credits
                        .get_mut(&execution.0)
                        .ok_or(Error::Unauthorized)?;
                    if credit.stage.is_some() {
                        return Err(Error::Bounds);
                    }
                    credit.stage = Some(id);
                }
                Publisher::Client { .. } | Publisher::AuthorizedClient { .. } => {
                    let pending = next
                        .metadata
                        .credits
                        .values()
                        .filter(|credit| credit.stage.is_none() && !credit.closed)
                        .count();
                    if next.metadata.stages.len() + pending >= MAX_STAGES {
                        return Err(Error::Bounds);
                    }
                }
            }
            let root = next.root_id()?;
            let closure = next.keys(&candidate.closure)?;
            let fences = next
                .fences(&candidate.closure)?
                .into_iter()
                .map(|(id, fence)| (r::CohortId(id), fence))
                .collect();
            next.retain(r::Event::Acquire {
                root: r::RootId(root),
                lease: 1,
                kind: r::RootKind::Staging,
                closure,
                until,
                fences,
            })?;
            let mut internal_writes = BTreeMap::new();
            for (write, key) in &writes {
                if *write == 0 || !candidate.closure.iter().any(|member| member.key == *key) {
                    return Err(Error::Invalid);
                }
                let internal = next.write_id()?;
                next.retain(r::Event::StartWrite {
                    write: r::WriteId(internal),
                    key: next.key(*key)?,
                    staging: r::RootId(root),
                    lease: 1,
                })?;
                internal_writes.insert(*write, internal);
            }
            let authorized_at = matches!(candidate.publisher, Publisher::AuthorizedClient { .. })
                .then_some(next.metadata.commit);
            next.metadata.stages.insert(
                id,
                Stage {
                    candidate,
                    authorized_at,
                    until,
                    root,
                    writes,
                    internal_writes,
                    outcomes: BTreeMap::new(),
                    preparation: None,
                    state: StageState::Open,
                },
            );
            Ok(())
        })
    }

    pub(crate) fn observe(
        &mut self,
        stage: u64,
        write: u64,
        key: Key,
        object: Object,
        success: bool,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            let row = next.metadata.stages.get(&stage).ok_or(Error::Invalid)?;
            if row.writes.get(&write) != Some(&key)
                || !row
                    .candidate
                    .closure
                    .iter()
                    .any(|member| member.key == key && member.object == object)
            {
                return Err(Error::Invalid);
            }
            // In private v4 an identical settled observation is a true no-op.
            // Identity checks still precede this shortcut, and conflicting
            // outcomes must pass the retention engine's rejection boundary.
            if next.metadata.terminal.is_some() && row.outcomes.get(&write) == Some(&success) {
                return Ok(());
            }
            let internal = *row.internal_writes.get(&write).ok_or(Error::Corrupt)?;
            next.retain(r::Event::WriteDone {
                write: r::WriteId(internal),
                success,
            })?;
            next.metadata
                .stages
                .get_mut(&stage)
                .unwrap()
                .outcomes
                .insert(write, success);
            next.metadata.revision = next.metadata.revision.checked_add(1).ok_or(Error::Bounds)?;
            Ok(())
        })
    }

    fn validate_evidence(
        &self,
        stage: &Stage,
        evidence: &Evidence,
        until: u64,
    ) -> Result<(), Error> {
        if stage.state != StageState::Open
            || stage.until <= self.now()
            || evidence.catalogue_revision != self.metadata.revision
            || evidence.closure != stage.candidate.closure
            || evidence.fences != self.fences(&evidence.closure)?
            || evidence.until < until
            || !self.valid_until(evidence.until, LEASE_TICKS)
        {
            return Err(Error::Stale);
        }
        let view = self.retention.view();
        for key in self.keys(&evidence.closure)? {
            let row = view.generations.get(&key).ok_or(Error::Incomplete)?;
            if !row.ready
                || row.retired
                || !matches!(
                    view.cohorts[&row.cohort].phase,
                    r::Phase::Allocating | r::Phase::Sealed
                )
            {
                return Err(Error::Unprotected);
            }
        }
        Ok(())
    }
    pub(crate) fn prepare(&mut self, id: u64, evidence: Evidence) -> Result<(), Error> {
        self.transaction(|next| {
            if evidence.stage != id {
                return Err(Error::Invalid);
            }
            let stage = next.metadata.stages.get(&id).ok_or(Error::Invalid)?;
            next.validate_evidence(
                stage,
                &evidence,
                next.now().checked_add(1).ok_or(Error::Bounds)?,
            )?;
            next.metadata.stages.get_mut(&id).unwrap().preparation = Some(evidence);
            Ok(())
        })
    }
    pub(crate) fn abandon(&mut self, id: u64) -> Result<(), Error> {
        self.transaction(|next| {
            let stage = next.metadata.stages.get(&id).ok_or(Error::Invalid)?;
            if stage.state != StageState::Open {
                return Err(Error::Stale);
            }
            let root = stage.root;
            next.retain(r::Event::Release {
                root: r::RootId(root),
                lease: 1,
            })?;
            next.metadata.stages.get_mut(&id).unwrap().state = StageState::Abandoned;
            Ok(())
        })
    }
    pub(crate) fn fence_namespace(&mut self, handle: u64) -> Result<(), Error> {
        self.transaction(|next| {
            next.retain(r::Event::Seal {
                cohort: r::CohortId(handle),
            })?;
            next.retain(r::Event::BeginClose {
                cohort: r::CohortId(handle),
            })
        })
    }

    fn candidate(&self, publication: u64) -> Result<&Candidate, Error> {
        let accepted = self
            .metadata
            .accepted
            .get(&publication)
            .ok_or(Error::Invalid)?;
        Ok(&self
            .metadata
            .stages
            .get(&accepted.stage)
            .ok_or(Error::Corrupt)?
            .candidate)
    }
    fn acquire(
        &mut self,
        kind: r::RootKind,
        closure: &BTreeSet<Member>,
        until: u64,
    ) -> Result<u64, Error> {
        let root = self.root_id()?;
        self.retain(r::Event::Acquire {
            root: r::RootId(root),
            lease: 1,
            kind,
            closure: self.keys(closure)?,
            until,
            fences: self
                .fences(closure)?
                .into_iter()
                .map(|(id, fence)| (r::CohortId(id), fence))
                .collect(),
        })?;
        Ok(root)
    }
    fn install_operation_root(&mut self, operation: u64, publication: u64) -> Result<(), Error> {
        let candidate = self.candidate(publication)?.clone();
        let until = self
            .metadata
            .operations
            .get(&operation)
            .ok_or(Error::Corrupt)?
            .until;
        let root = self.acquire(r::RootKind::Operation, &candidate.closure, until)?;
        let operation = self.metadata.operations.get_mut(&operation).unwrap();
        operation.publication = Some(publication);
        operation.root = Some(root);
        operation.payload = Some(candidate.response);
        Ok(())
    }

    pub(crate) fn publish(
        &mut self,
        stage_id: u64,
        publication: u64,
        until: u64,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            if publication == 0 || next.metadata.accepted.contains_key(&publication) { return Err(Error::Conflict); }
            if !next.valid_until(until, LEASE_TICKS) { return Err(Error::Bounds); }
            let stage = next.metadata.stages.get(&stage_id).ok_or(Error::Invalid)?.clone();
            let preparation = stage.preparation.as_ref().ok_or(Error::Incomplete)?;
            next.validate_evidence(&stage, preparation, until)?;
            next.validate_candidate(&stage.candidate)?;
            next.validate_current_update(&stage.candidate)?;
            let eligible = match stage.candidate.publisher {
                Publisher::Worker { execution, attempt, worker, incarnation } => {
                    let before = next.scheduler.view();
                    let actual_attempt = before.attempts.iter().find(|row| row.id == attempt).ok_or(Error::Unauthorized)?;
                    if actual_attempt.execution != execution { return Err(Error::Unauthorized); }
                    let request = &before.executions.iter().find(|row| row.id == execution).ok_or(Error::Unauthorized)?.request;
                    if !next.request_matches_action(&stage.candidate.action, request) { return Err(Error::Unauthorized); }
                    let eligible = !request.do_not_cache && stage.candidate.response.exit_code == 0 && stage.candidate.response.error.is_none();
                    next.scheduler_apply(a::Event::Complete { attempt, worker, incarnation, result: stage.candidate.response.clone() })?;
                    next.metadata.credits.get_mut(&execution.0).ok_or(Error::Corrupt)?.physical_remaining = 0;
                    // The real reducer's temporary M4 intent never leaves this outer clone.
                    let effects = next.scheduler.view().effects;
                    for effect in effects {
                        if matches!(effect.kind, a::EffectKind::PublishResult { execution: id, .. } if id == execution) {
                            next.scheduler_apply(a::Event::EffectOutcome { effect: effect.id, success: true })?;
                        }
                    }
                    eligible
                }
                Publisher::Client { .. } | Publisher::AuthorizedClient { .. } => true,
            };
            let root = next.root_id()?;
            next.retain(r::Event::Transfer { staging: r::RootId(stage.root), lease: 1, publication: r::RootId(root), until })?;
            next.metadata.accepted.insert(publication, Accepted { stage: stage_id, commit: next.metadata.commit, at: next.now(), eligible, root, until });
            if let Publisher::Worker { execution, .. } = stage.candidate.publisher {
                let waiting: Vec<_> = next.scheduler.view().operations.iter().filter(|operation| operation.execution == execution && operation.terminal.as_ref() == Some(&a::TerminalResult::Completed(stage.candidate.response.clone()))).map(|row| row.id.0).collect();
                for operation in waiting {
                    if next.metadata.operations[&operation].until > preparation.until { return Err(Error::Unprotected); }
                    next.install_operation_root(operation, publication)?;
                }
            }
            if eligible {
                if let Some(old) = next.metadata.heads.insert(stage.candidate.action.clone(), publication) {
                    next.retain(r::Event::Release { root: r::RootId(next.metadata.accepted[&old].root), lease: 1 })?;
                }
            } else {
                next.retain(r::Event::Release { root: r::RootId(root), lease: 1 })?;
            }
            next.metadata.stages.get_mut(&stage_id).unwrap().state = StageState::Accepted(publication);
            next.rebuild_index_inner();
            Ok(())
        })
    }

    fn protected_head(&self, publication: u64, until: u64) -> bool {
        self.metadata.accepted.get(&publication).is_some_and(|row| {
            row.eligible
                && self
                    .metadata
                    .heads
                    .get(&self.metadata.stages[&row.stage].candidate.action)
                    == Some(&publication)
                && self
                    .retention
                    .view()
                    .roots
                    .get(&r::RootId(row.root))
                    .is_some_and(|root| root.until >= until && root.until > self.now())
        })
    }
    fn rebuild_index_inner(&mut self) {
        self.index = self
            .metadata
            .heads
            .iter()
            .filter(|(_, id)| self.protected_head(**id, self.now().saturating_add(1)))
            .map(|(key, id)| (key.clone(), *id))
            .collect();
    }
    pub(crate) fn lose_index(&mut self) {
        self.index.clear();
    }
    pub(crate) fn rebuild_index(&mut self) {
        self.rebuild_index_inner();
    }
    pub(crate) fn serve(
        &mut self,
        action: &str,
        expected: u64,
        barrier: u64,
        grant: u64,
        until: u64,
    ) -> Result<(), Error> {
        self.transaction(|next| {
            if barrier != next.metadata.commit - 1
                || next.metadata.heads.get(action) != Some(&expected)
                || !next.protected_head(expected, until)
            {
                return Err(Error::Miss);
            }
            if next.metadata.grants.len() >= MAX_GRANTS
                || next.metadata.grants.contains_key(&grant)
                || !next.valid_until(until, LEASE_TICKS)
            {
                return Err(Error::Bounds);
            }
            let closure = next.candidate(expected)?.closure.clone();
            let root = next.acquire(r::RootKind::Serving, &closure, until)?;
            next.metadata.grants.insert(
                grant,
                Grant {
                    publication: expected,
                    root,
                    until,
                },
            );
            Ok(())
        })
    }
    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let snapshot = Snapshot {
            version: self.metadata.snapshot_version(),
            scheduler: self.scheduler.snapshot().map_err(|_| Error::Corrupt)?,
            retention: self.retention.snapshot().map_err(|_| Error::Corrupt)?,
            metadata: self.metadata.clone(),
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(|_| Error::Corrupt)?;
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Error::Bounds);
        }
        Ok(bytes)
    }
    pub(crate) fn restore(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_SNAPSHOT {
            return Err(Error::Bounds);
        }
        let snapshot: Snapshot = serde_json::from_slice(bytes).map_err(|_| Error::Corrupt)?;
        if snapshot.version != snapshot.metadata.snapshot_version()
            || serde_json::to_vec(&snapshot).map_err(|_| Error::Corrupt)? != bytes
        {
            return Err(Error::Corrupt);
        }
        let mut state = Self {
            scheduler: a::State::restore(&snapshot.scheduler).map_err(|_| Error::Corrupt)?,
            retention: Coarse::restore(&snapshot.retention).map_err(|_| Error::Corrupt)?,
            metadata: snapshot.metadata,
            index: BTreeMap::new(),
        };
        state.validate()?;
        state.rebuild_index_inner();
        Ok(state)
    }
}
