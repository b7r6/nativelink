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

//! Cross-component validation before any outer state becomes visible or restores.

use super::*;

impl PublicationAuthority {
    pub(super) fn validate(&self) -> Result<(), Error> {
        self.scheduler
            .check_invariants()
            .map_err(|_| Error::Corrupt)?;
        self.retention.snapshot().map_err(|_| Error::Corrupt)?;
        let scheduler = self.scheduler.view();
        let retention = self.retention.view();
        let meta = &self.metadata;
        if self.scheduler.accepted_history_enabled()
            || self.scheduler.receipt_retirement_enabled()
            || self.scheduler.payload_limit_bytes() != MAX_PAYLOAD
            || scheduler.epoch != EPOCH
            || scheduler.now != retention.now
            || scheduler.executions.len() > MAX_STAGES
            || scheduler.operations.len() > MAX_OPERATIONS
            || scheduler.workers.len() > MAX_STAGES
            || scheduler.effects.len() > MAX_STAGES * 8
            || scheduler
                .effects
                .iter()
                .any(|effect| matches!(effect.kind, a::EffectKind::PublishResult { .. }))
            || meta.actions.len() > MAX_STAGES
            || meta
                .actions
                .keys()
                .any(|key| key.is_empty() || key.len() > 256)
            || meta.clients.len() > MAX_STAGES
            || meta.clients.contains(&0)
            || meta.namespaces.len() > MAX_PHYSICAL
            || meta.physical.len() > MAX_PHYSICAL
            || meta.stages.len() > MAX_STAGES
            || meta.accepted.len() > MAX_STAGES
            || meta.operations.len() != scheduler.operations.len()
            || meta.grants.len() > MAX_GRANTS
            || meta.credits.len() > MAX_STAGES
            || meta.next_root == 0
            || meta.next_root > u64::try_from(ROOT_CAP).map_err(|_| Error::Corrupt)? + 1
            || meta.next_write == 0
            || meta.next_write > u64::try_from(WRITE_CAP).map_err(|_| Error::Corrupt)? + 1
            || meta
                .credits
                .values()
                .any(|credit| credit.physical_remaining > MAX_MEMBERS)
            || !retention.deletes.is_empty()
            || retention.generations.values().any(|row| row.retired)
            || retention.limits.roots != ROOT_CAP
            || retention.limits.writes != WRITE_CAP
            || retention.limits.generations != MAX_PHYSICAL
            || retention.limits.cohorts != MAX_PHYSICAL
            || retention.limits.closure != MAX_MEMBERS
            || retention.limits.lease_ticks != LEASE_TICKS
            || retention.limits.mark_batch != MAX_MEMBERS
            || retention.limits.retire_batch != MAX_MEMBERS
            || retention.limits.delete_intents != MAX_PHYSICAL + WRITE_CAP
        {
            return Err(Error::Corrupt);
        }
        let pending = meta
            .credits
            .values()
            .filter(|credit| credit.stage.is_none())
            .count();
        if meta.stages.len() + pending > MAX_STAGES
            || meta.physical.len()
                + meta
                    .credits
                    .values()
                    .map(|credit| credit.physical_remaining)
                    .sum::<usize>()
                > MAX_PHYSICAL
        {
            return Err(Error::Corrupt);
        }
        for (execution, credit) in &meta.credits {
            let actual = scheduler
                .executions
                .iter()
                .find(|row| row.id.0 == *execution)
                .ok_or(Error::Corrupt)?;
            if credit.physical_remaining > MAX_MEMBERS
                || (actual.terminal.is_some() && credit.physical_remaining != 0)
            {
                return Err(Error::Corrupt);
            }
            if credit.prior_stages.len() >= a::MAX_ATTEMPTS as usize
                || credit.prior_stages.iter().collect::<BTreeSet<_>>().len()
                    != credit.prior_stages.len()
                || credit
                    .stage
                    .is_some_and(|stage| credit.prior_stages.contains(&stage))
            {
                return Err(Error::Corrupt);
            }
            for prior in &credit.prior_stages {
                let stage = meta.stages.get(prior).ok_or(Error::Corrupt)?;
                if !matches!(stage.candidate.publisher, Publisher::Worker { execution: id, .. } if id.0 == *execution)
                    || matches!(stage.state, StageState::Accepted(_))
                {
                    return Err(Error::Corrupt);
                }
            }
            if let Some(stage) = credit.stage
                && !matches!(meta.stages.get(&stage).map(|row| row.candidate.publisher), Some(Publisher::Worker { execution: id, .. }) if id.0 == *execution)
            {
                return Err(Error::Corrupt);
            }
        }
        for execution in &scheduler.executions {
            if !meta
                .actions
                .contains_key(&execution.request.action.instance)
                || execution.request.do_not_cache
                    != meta.actions[&execution.request.action.instance]
                || (execution.terminal.is_none() && !meta.credits.contains_key(&execution.id.0))
            {
                return Err(Error::Corrupt);
            }
        }
        let mut pairs = BTreeSet::new();
        for (index, namespace) in meta.namespaces.iter().enumerate() {
            if namespace.namespace == [0; 16]
                || namespace.incarnation == [0; 16]
                || namespace.handle != u64::try_from(index).map_err(|_| Error::Corrupt)? + 1
                || !pairs.insert((namespace.namespace, namespace.incarnation))
                || !retention
                    .cohorts
                    .contains_key(&r::CohortId(namespace.handle))
            {
                return Err(Error::Corrupt);
            }
        }
        if retention.cohorts.len() != meta.namespaces.len()
            || retention.generations.len() != meta.physical.len()
        {
            return Err(Error::Corrupt);
        }
        let mut keys = BTreeSet::new();
        let mut roots = BTreeSet::new();
        let mut writes = BTreeSet::new();
        for (index, row) in meta.physical.iter().enumerate() {
            if !keys.insert(row.key)
                || row.key.generation == 0
                || row.logical != u64::try_from(index).map_err(|_| Error::Corrupt)? + 1
                || !matches!(row.object.algorithm, 1 | 9)
                || row.object.bytes > MAX_PAYLOAD as u64
            {
                return Err(Error::Corrupt);
            }
            let key = self.key(row.key)?;
            let actual = retention.generations.get(&key).ok_or(Error::Corrupt)?;
            if actual.logical_object != row.logical || actual.cohort.0 != key.incarnation {
                return Err(Error::Corrupt);
            }
            if let Some((root, write)) = row.bootstrap {
                if !roots.insert(root) || !writes.insert(write) {
                    return Err(Error::Corrupt);
                }
                let guard = retention
                    .roots
                    .get(&r::RootId(root))
                    .ok_or(Error::Corrupt)?;
                let receipt = retention
                    .writes
                    .get(&r::WriteId(write))
                    .ok_or(Error::Corrupt)?;
                if guard.kind != r::RootKind::Staging
                    || guard.closure != vec![key]
                    || guard.lease != 1
                    || guard.until > scheduler.now
                    || receipt.key != key
                    || receipt.outcome != Some(true)
                {
                    return Err(Error::Corrupt);
                }
            }
        }
        for (id, stage) in &meta.stages {
            if *id == 0
                || stage.until.saturating_sub(scheduler.now) > STAGE_TICKS
                || !roots.insert(stage.root)
                || stage.writes.len() > MAX_MEMBERS
                || stage.writes.len() != stage.internal_writes.len()
            {
                return Err(Error::Corrupt);
            }
            self.validate_candidate(&stage.candidate)
                .map_err(|_| Error::Corrupt)?;
            let actual = retention
                .roots
                .get(&r::RootId(stage.root))
                .ok_or(Error::Corrupt)?;
            if actual.kind != r::RootKind::Staging
                || actual.lease != 1
                || actual.closure != self.keys(&stage.candidate.closure)?
            {
                return Err(Error::Corrupt);
            }
            match stage.state {
                StageState::Open if actual.until != stage.until => return Err(Error::Corrupt),
                StageState::Abandoned if actual.until > scheduler.now => {
                    return Err(Error::Corrupt);
                }
                StageState::Accepted(publication) => {
                    let accepted = meta.accepted.get(&publication).ok_or(Error::Corrupt)?;
                    if accepted.stage != *id || actual.until != accepted.at {
                        return Err(Error::Corrupt);
                    }
                }
                _ => {}
            }
            if let Some(preparation) = &stage.preparation {
                let current_fences = self.fences(&stage.candidate.closure)?;
                if preparation.stage != *id
                    || preparation.closure != stage.candidate.closure
                    || preparation.catalogue_revision == 0
                    || preparation.catalogue_revision > meta.revision
                    || preparation.until == 0
                    || preparation.until > stage.until.saturating_add(LEASE_TICKS)
                    || preparation.fences.keys().ne(current_fences.keys())
                    || preparation.fences.iter().any(|(cohort, fence)| {
                        current_fences
                            .get(cohort)
                            .is_none_or(|current| fence > current)
                    })
                {
                    return Err(Error::Corrupt);
                }
            }
            for (write, key) in &stage.writes {
                let internal = *stage.internal_writes.get(write).ok_or(Error::Corrupt)?;
                if *write == 0
                    || !writes.insert(internal)
                    || !stage
                        .candidate
                        .closure
                        .iter()
                        .any(|member| member.key == *key)
                {
                    return Err(Error::Corrupt);
                }
                let actual = retention
                    .writes
                    .get(&r::WriteId(internal))
                    .ok_or(Error::Corrupt)?;
                if actual.key != self.key(*key)?
                    || actual.outcome != stage.outcomes.get(write).copied()
                {
                    return Err(Error::Corrupt);
                }
            }
            if stage
                .outcomes
                .keys()
                .any(|id| !stage.writes.contains_key(id))
            {
                return Err(Error::Corrupt);
            }
            if let Publisher::Worker { execution, .. } = stage.candidate.publisher
                && !meta.credits.get(&execution.0).is_some_and(|credit| {
                    credit.stage == Some(*id) || credit.prior_stages.contains(id)
                })
            {
                return Err(Error::Corrupt);
            }
        }
        let mut latest: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        let mut commits = BTreeSet::new();
        for (id, accepted) in &meta.accepted {
            if *id == 0
                || accepted.commit == 0
                || accepted.commit > meta.commit
                || !commits.insert(accepted.commit)
                || accepted.at > scheduler.now
                || accepted.until <= accepted.at
                || accepted.until - accepted.at > LEASE_TICKS
                || !roots.insert(accepted.root)
            {
                return Err(Error::Corrupt);
            }
            let stage = meta.stages.get(&accepted.stage).ok_or(Error::Corrupt)?;
            if stage.state != StageState::Accepted(*id)
                || stage.until <= accepted.at
                || stage.until - accepted.at > STAGE_TICKS
                || stage
                    .preparation
                    .as_ref()
                    .is_none_or(|preparation| preparation.until < accepted.until)
            {
                return Err(Error::Corrupt);
            }
            let candidate = &stage.candidate;
            let eligible = match candidate.publisher {
                Publisher::Worker {
                    execution,
                    attempt,
                    worker,
                    incarnation,
                } => {
                    let entry = scheduler
                        .executions
                        .iter()
                        .find(|row| row.id == execution)
                        .ok_or(Error::Corrupt)?;
                    let receipt = scheduler
                        .attempts
                        .iter()
                        .find(|row| row.id == attempt)
                        .ok_or(Error::Corrupt)?;
                    if entry.request.action.instance != candidate.action
                        || entry.terminal.as_ref()
                            != Some(&a::TerminalResult::Completed(candidate.response.clone()))
                        || receipt.execution != execution
                        || receipt.worker != worker
                        || receipt.incarnation != incarnation
                        || receipt.authorized
                        || entry.terminal_at != Some(accepted.at)
                        || scheduler
                            .attempts
                            .iter()
                            .any(|later| later.execution == execution && later.id > attempt)
                    {
                        return Err(Error::Corrupt);
                    }
                    !entry.request.do_not_cache
                        && candidate.response.exit_code == 0
                        && candidate.response.error.is_none()
                }
                Publisher::Client { principal } => {
                    if !meta.clients.contains(&principal) {
                        return Err(Error::Corrupt);
                    }
                    true
                }
            };
            if accepted.eligible != eligible {
                return Err(Error::Corrupt);
            }
            if eligible
                && latest
                    .get(&candidate.action)
                    .is_none_or(|(commit, _)| *commit < accepted.commit)
            {
                latest.insert(candidate.action.clone(), (accepted.commit, *id));
            }
            let root = retention
                .roots
                .get(&r::RootId(accepted.root))
                .ok_or(Error::Corrupt)?;
            if root.kind != r::RootKind::Publication
                || root.lease != 1
                || root.closure != self.keys(&candidate.closure)?
                || root.until > accepted.until
            {
                return Err(Error::Corrupt);
            }
        }
        if meta.heads
            != latest
                .iter()
                .map(|(action, (_, id))| (action.clone(), *id))
                .collect()
        {
            return Err(Error::Corrupt);
        }
        for (id, accepted) in &meta.accepted {
            let candidate = self.candidate(*id)?;
            let root = &retention.roots[&r::RootId(accepted.root)];
            let expected_until = if !accepted.eligible {
                accepted.at
            } else if meta.heads.get(&candidate.action) == Some(id) {
                accepted.until
            } else {
                meta.accepted
                    .values()
                    .filter(|later| {
                        later.eligible
                            && later.commit > accepted.commit
                            && meta.stages[&later.stage].candidate.action == candidate.action
                    })
                    .min_by_key(|later| later.commit)
                    .ok_or(Error::Corrupt)?
                    .at
                    .min(accepted.until)
            };
            if root.until != expected_until {
                return Err(Error::Corrupt);
            }
        }
        for (id, operation) in &meta.operations {
            let actual = scheduler
                .operations
                .iter()
                .find(|row| row.id.0 == *id)
                .ok_or(Error::Corrupt)?;
            let execution = scheduler
                .executions
                .iter()
                .find(|row| row.id == operation.execution)
                .ok_or(Error::Corrupt)?;
            if actual.execution != operation.execution
                || operation.until.saturating_sub(scheduler.now) > LEASE_TICKS
                || execution.request.action.instance != operation.action
            {
                return Err(Error::Corrupt);
            }
            match (operation.publication, operation.root) {
                (Some(publication), Some(root)) => {
                    if !roots.insert(root) {
                        return Err(Error::Corrupt);
                    }
                    let accepted = meta.accepted.get(&publication).ok_or(Error::Corrupt)?;
                    if meta.stages[&accepted.stage]
                        .preparation
                        .as_ref()
                        .is_none_or(|preparation| preparation.until < operation.until)
                    {
                        return Err(Error::Corrupt);
                    }
                    let candidate = self.candidate(publication)?;
                    let protection = retention
                        .roots
                        .get(&r::RootId(root))
                        .ok_or(Error::Corrupt)?;
                    if operation.payload.as_ref().is_none_or(|payload| {
                        actual.terminal.as_ref()
                            != Some(&a::TerminalResult::Completed(payload.clone()))
                            || payload.require_response().is_err()
                    }) || candidate.action != operation.action
                        || protection.kind != r::RootKind::Operation
                        || protection.lease != 1
                        || protection.until != operation.until
                        || protection.closure != self.keys(&candidate.closure)?
                    {
                        return Err(Error::Corrupt);
                    }
                    if operation.cached {
                        let result = ActionResult::decode(candidate.result_bytes.as_slice())
                            .map_err(|_| Error::Corrupt)?;
                        let expected = ExecuteResponse {
                            result: Some(result),
                            cached_result: true,
                            ..ExecuteResponse::default()
                        };
                        if execution.attempts_started != 0
                            || operation
                                .payload
                                .as_ref()
                                .ok_or(Error::Corrupt)?
                                .require_response()
                                .map_err(|_| Error::Corrupt)?
                                .decode()
                                .map_err(|_| Error::Corrupt)?
                                != expected
                        {
                            return Err(Error::Corrupt);
                        }
                    } else if operation.payload.as_ref() != Some(&candidate.response)
                        || !matches!(candidate.publisher, Publisher::Worker { execution: id, .. } if id == operation.execution)
                    {
                        return Err(Error::Corrupt);
                    }
                }
                (None, None)
                    if operation.payload.is_none()
                        && !operation.cached
                        && !matches!(actual.terminal, Some(a::TerminalResult::Completed(_))) => {}
                _ => return Err(Error::Corrupt),
            }
        }
        for grant in meta.grants.values() {
            if !roots.insert(grant.root) || grant.until.saturating_sub(scheduler.now) > LEASE_TICKS
            {
                return Err(Error::Corrupt);
            }
            let candidate = self.candidate(grant.publication)?;
            let actual = retention
                .roots
                .get(&r::RootId(grant.root))
                .ok_or(Error::Corrupt)?;
            if actual.kind != r::RootKind::Serving
                || actual.lease != 1
                || actual.until != grant.until
                || actual.closure != self.keys(&candidate.closure)?
            {
                return Err(Error::Corrupt);
            }
        }
        if roots != (1..meta.next_root).collect()
            || writes != (1..meta.next_write).collect()
            || roots.len() != retention.roots.len()
            || writes.len() != retention.writes.len()
        {
            return Err(Error::Corrupt);
        }
        for (action, id) in &self.index {
            if meta.heads.get(action) != Some(id)
                || !self.protected_head(*id, scheduler.now.saturating_add(1))
            {
                return Err(Error::Corrupt);
            }
        }
        Ok(())
    }
}
