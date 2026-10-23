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

//! Private-v5 prepared graph memory reports. State decides retry or failure;
//! the outer transaction gives an accepted graph its operation protection.

use super::*;

impl PublicationAuthority {
    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate private v5."
    )]
    pub(crate) fn enable_prepared_memory(&mut self) -> Result<(), Error> {
        if self.metadata.prepared_memory_at.is_some() {
            return Ok(());
        }
        let mut activated = self.clone();
        activated.enable_terminal_outcomes()?;
        activated.transaction(|next| {
            next.metadata.prepared_memory_at = Some(next.metadata.commit);
            Ok(())
        })?;
        *self = activated;
        Ok(())
    }

    fn memory_origin(
        candidate: &Candidate,
        report: &PreparedMemoryReport,
    ) -> Result<WorkerOrigin, Error> {
        let Publisher::Worker {
            execution,
            attempt,
            worker,
            incarnation,
        } = candidate.publisher
        else {
            return Err(Error::Invalid);
        };
        Ok(WorkerOrigin {
            epoch: report.epoch,
            action: candidate.action.clone(),
            execution,
            attempt,
            worker,
            incarnation,
        })
    }

    pub(super) fn validate_memory_identity(
        &self,
        candidate: &Candidate,
        report: &PreparedMemoryReport,
    ) -> Result<(), Error> {
        if self.metadata.prepared_memory_at.is_none() || report.id == 0 {
            return Err(Error::Invalid);
        }
        let origin = Self::memory_origin(candidate, report)?;
        self.report_origin(
            &origin,
            TerminalReportKind::MemoryFailure {
                charged: report.charged,
                peak_memory_bytes: report.peak_memory_bytes,
                quiescent: report.quiescent,
            },
        )?;
        if candidate.response.exit_code == 0 && candidate.response.error.is_none() {
            return Err(Error::Invalid);
        }
        Ok(())
    }

    /// Stage admission reserves a report identity even before classification.
    pub(super) fn reserve_memory_report(
        &self,
        candidate: &Candidate,
        report: &PreparedMemoryReport,
    ) -> Result<(), Error> {
        let origin = Self::memory_origin(candidate, report)?;
        self.check_report_slot(report.id, &origin)
    }

    pub(super) fn check_report_slot(&self, id: u64, origin: &WorkerOrigin) -> Result<(), Error> {
        self.check_report_slot_except(id, origin, None)
    }

    fn check_report_slot_except(
        &self,
        id: u64,
        origin: &WorkerOrigin,
        own_stage: Option<u64>,
    ) -> Result<(), Error> {
        let terminal = self.metadata.terminal.as_ref().ok_or(Error::Invalid)?;
        let mut count = terminal.reports.len();
        let mut execution_count = terminal
            .reports
            .values()
            .filter(|row| row.report.origin.execution == origin.execution)
            .count();
        if terminal.reports.contains_key(&id)
            || terminal
                .reports
                .values()
                .any(|row| row.report.origin.attempt == origin.attempt)
        {
            return Err(Error::Conflict);
        }
        for (stage_id, stage) in &self.metadata.stages {
            if Some(*stage_id) == own_stage {
                continue;
            }
            if let Some(memory) = &stage.memory {
                let other = Self::memory_origin(&stage.candidate, &memory.report)?;
                if memory.report.id == id || other.attempt == origin.attempt {
                    return Err(Error::Conflict);
                }
                count += 1;
                execution_count += usize::from(other.execution == origin.execution);
            }
        }
        if count >= MAX_STAGES * a::MAX_ATTEMPTS as usize
            || execution_count >= a::MAX_ATTEMPTS as usize
        {
            return Err(Error::Bounds);
        }
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate private v5."
    )]
    pub(crate) fn stage_memory(
        &mut self,
        id: u64,
        candidate: Candidate,
        report: PreparedMemoryReport,
        until: u64,
        writes: BTreeMap<u64, Key>,
    ) -> Result<(), Error> {
        self.stage_inner(id, candidate, Some(report), until, writes)
    }

    pub(super) fn stage_terminal(stage: &Stage) -> Option<a::TerminalResult> {
        match &stage.memory {
            None => Some(a::TerminalResult::Completed(
                stage.candidate.response.clone(),
            )),
            Some(memory) => match memory.decision?.disposition {
                PreparedMemoryDisposition::Terminal { reason } => {
                    Some(a::TerminalResult::ResourceFailed {
                        result: stage.candidate.response.clone(),
                        reason,
                    })
                }
                PreparedMemoryDisposition::Retry { .. } => None,
            },
        }
    }

    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate private v5."
    )]
    pub(crate) fn accept_prepared_memory(
        &mut self,
        stage_id: u64,
        publication: u64,
        until: u64,
    ) -> Result<NoGraphOutcome, Error> {
        let stage = self.metadata.stages.get(&stage_id).ok_or(Error::Invalid)?;
        let memory = stage.memory.as_ref().ok_or(Error::Invalid)?;
        if let Some(decision) = memory.decision {
            return if decision.publication == publication && decision.until == until {
                Ok(NoGraphOutcome::Duplicate)
            } else {
                Err(Error::Conflict)
            };
        }
        self.transaction(|next| {
            if publication == 0 || next.metadata.accepted.contains_key(&publication) {
                return Err(Error::Conflict);
            }
            if !next.valid_until(until, LEASE_TICKS) {
                return Err(Error::Bounds);
            }
            let stage = next
                .metadata
                .stages
                .get(&stage_id)
                .ok_or(Error::Invalid)?
                .clone();
            let memory = stage.memory.as_ref().ok_or(Error::Invalid)?;
            let report = &memory.report;
            let preparation = stage.preparation.as_ref().ok_or(Error::Incomplete)?;
            next.validate_evidence(&stage, preparation, until)?;
            next.validate_candidate(&stage.candidate)?;
            next.validate_memory_identity(&stage.candidate, report)?;
            let origin = Self::memory_origin(&stage.candidate, report)?;
            next.check_report_slot_except(report.id, &origin, Some(stage_id))?;
            // No preview decision is retained: current capacity controls this event.
            if next.scheduler_apply(a::Event::MemoryFailure {
                attempt: origin.attempt,
                worker: origin.worker,
                incarnation: origin.incarnation,
                charged: report.charged,
                peak_memory_bytes: report.peak_memory_bytes,
                quiescent: report.quiescent,
                result: stage.candidate.response.clone(),
            })? != a::Reply::Applied
            {
                return Err(Error::Conflict);
            }
            let view = next.scheduler.view();
            let execution = view
                .executions
                .iter()
                .find(|row| row.id == origin.execution)
                .ok_or(Error::Corrupt)?;
            let (disposition, outcome) = match &execution.terminal {
                Some(value @ a::TerminalResult::ResourceFailed { result, reason })
                    if result == &stage.candidate.response =>
                {
                    let root = next.root_id()?;
                    next.retain(r::Event::Transfer {
                        staging: r::RootId(stage.root),
                        lease: 1,
                        publication: r::RootId(root),
                        until,
                    })?;
                    next.metadata.accepted.insert(
                        publication,
                        Accepted {
                            stage: stage_id,
                            commit: next.metadata.commit,
                            at: view.now,
                            eligible: false,
                            root,
                            until,
                        },
                    );
                    for operation in &view.operations {
                        if operation.execution == origin.execution
                            && operation.terminal.as_ref() == Some(value)
                        {
                            if next.metadata.operations[&operation.id.0].until > preparation.until {
                                return Err(Error::Unprotected);
                            }
                            next.install_operation_root(operation.id.0, publication)?;
                        }
                    }
                    next.retain(r::Event::Release {
                        root: r::RootId(root),
                        lease: 1,
                    })?;
                    next.metadata
                        .stages
                        .get_mut(&stage_id)
                        .ok_or(Error::Corrupt)?
                        .state = StageState::Accepted(publication);
                    (
                        PreparedMemoryDisposition::Terminal { reason: *reason },
                        NoGraphOutcome::TerminalAccepted(value.clone()),
                    )
                }
                None if execution.phase == a::Phase::Queued && execution.attempt.is_none() => (
                    PreparedMemoryDisposition::Retry {
                        target: execution.reservation_target,
                    },
                    NoGraphOutcome::RetryScheduled,
                ),
                _ => return Err(Error::Corrupt),
            };
            next.metadata
                .stages
                .get_mut(&stage_id)
                .ok_or(Error::Corrupt)?
                .memory
                .as_mut()
                .ok_or(Error::Corrupt)?
                .decision = Some(PreparedMemoryDecision {
                publication,
                until,
                position: next.metadata.commit,
                at: view.now,
                disposition,
            });
            Ok(outcome)
        })
    }

    /// A temporary validation projection, never a second retained payload copy.
    pub(super) fn all_classified_reports(
        &self,
    ) -> Result<Vec<(u64, ClassifiedReport, Option<u64>)>, Error> {
        let mut records = self
            .metadata
            .terminal
            .as_ref()
            .map_or_else(Vec::new, |terminal| {
                terminal
                    .reports
                    .iter()
                    .map(|(id, report)| (*id, report.clone(), None))
                    .collect()
            });
        for (stage_id, stage) in &self.metadata.stages {
            if let Some(memory) = &stage.memory
                && let Some(decision) = memory.decision
            {
                let origin = Self::memory_origin(&stage.candidate, &memory.report)?;
                let disposition = match decision.disposition {
                    PreparedMemoryDisposition::Retry { target } => {
                        ReportDisposition::Retry { target }
                    }
                    PreparedMemoryDisposition::Terminal { reason } => {
                        ReportDisposition::Terminal(a::TerminalResult::ResourceFailed {
                            result: stage.candidate.response.clone(),
                            reason,
                        })
                    }
                };
                records.push((
                    memory.report.id,
                    ClassifiedReport {
                        report: NoGraphReport {
                            origin,
                            kind: TerminalReportKind::MemoryFailure {
                                charged: memory.report.charged,
                                peak_memory_bytes: memory.report.peak_memory_bytes,
                                quiescent: memory.report.quiescent,
                            },
                            result: stage.candidate.response.clone(),
                        },
                        position: decision.position,
                        at: decision.at,
                        disposition,
                    },
                    Some(*stage_id),
                ));
            }
        }
        Ok(records)
    }

    pub(super) fn validate_prepared_memory(&self) -> Result<(), Error> {
        let enabled = self.metadata.prepared_memory_at;
        if let Some(position) = enabled
            && (self
                .metadata
                .terminal
                .as_ref()
                .is_none_or(|terminal| position <= terminal.enabled_at)
                || position > self.metadata.commit
                || self.retained_publication_positions().contains(&position))
        {
            return Err(Error::Corrupt);
        }
        let mut ids = BTreeSet::new();
        let mut attempts = BTreeSet::new();
        let mut executions = BTreeMap::<a::ExecutionId, usize>::new();
        if let Some(terminal) = &self.metadata.terminal {
            for (id, record) in &terminal.reports {
                ids.insert(*id);
                attempts.insert(record.report.origin.attempt);
                *executions
                    .entry(record.report.origin.execution)
                    .or_default() += 1;
                if enabled == Some(record.position) {
                    return Err(Error::Corrupt);
                }
            }
        }
        for (id, stage) in &self.metadata.stages {
            let Some(memory) = &stage.memory else {
                continue;
            };
            self.validate_memory_identity(&stage.candidate, &memory.report)
                .map_err(|_| Error::Corrupt)?;
            let origin = Self::memory_origin(&stage.candidate, &memory.report)?;
            if !ids.insert(memory.report.id) || !attempts.insert(origin.attempt) {
                return Err(Error::Corrupt);
            }
            *executions.entry(origin.execution).or_default() += 1;
            if let Some(decision) = memory.decision {
                if decision.publication == 0
                    || decision.position <= enabled.ok_or(Error::Corrupt)?
                    || decision.position > self.metadata.commit
                    || decision.at > self.now()
                    || decision.until <= decision.at
                    || decision.until - decision.at > LEASE_TICKS
                    || stage.until <= decision.at
                    || stage.until - decision.at > STAGE_TICKS
                    || stage
                        .preparation
                        .as_ref()
                        .is_none_or(|proof| proof.until < decision.until)
                {
                    return Err(Error::Corrupt);
                }
                match decision.disposition {
                    PreparedMemoryDisposition::Retry { .. } => {
                        if matches!(stage.state, StageState::Accepted(_))
                            || self
                                .metadata
                                .accepted
                                .values()
                                .any(|accepted| accepted.stage == *id)
                        {
                            return Err(Error::Corrupt);
                        }
                    }
                    PreparedMemoryDisposition::Terminal { .. } => {
                        let accepted = self
                            .metadata
                            .accepted
                            .get(&decision.publication)
                            .ok_or(Error::Corrupt)?;
                        if accepted.stage != *id
                            || accepted.commit != decision.position
                            || accepted.at != decision.at
                            || accepted.until != decision.until
                            || accepted.eligible
                            || stage.state != StageState::Accepted(decision.publication)
                        {
                            return Err(Error::Corrupt);
                        }
                    }
                }
            } else if matches!(stage.state, StageState::Accepted(_)) {
                return Err(Error::Corrupt);
            }
        }
        if ids.len() > MAX_STAGES * a::MAX_ATTEMPTS as usize
            || executions
                .values()
                .any(|count| *count > a::MAX_ATTEMPTS as usize)
        {
            return Err(Error::Corrupt);
        }
        Ok(())
    }
}
