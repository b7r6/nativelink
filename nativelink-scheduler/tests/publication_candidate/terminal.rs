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

//! No-graph terminal outcomes compose actual scheduler decisions with retained
//! storage obligations. This private successor does not enable accepted history.

use super::*;

impl PublicationAuthority {
    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate the terminal successor."
    )]
    pub(crate) fn enable_terminal_outcomes(&mut self) -> Result<(), Error> {
        if self.metadata.terminal.is_some() {
            return Ok(());
        }
        self.transaction(|next| {
            next.metadata.terminal = Some(TerminalState {
                enabled_at: next.metadata.commit,
                reports: BTreeMap::new(),
                accepted: BTreeMap::new(),
                operations: BTreeMap::new(),
            });
            Ok(())
        })
    }

    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate the terminal successor."
    )]
    pub(crate) fn terminal_reports(&self) -> Option<&BTreeMap<u64, ClassifiedReport>> {
        self.metadata.terminal.as_ref().map(|state| &state.reports)
    }
    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate the terminal successor."
    )]
    pub(crate) fn terminal_acceptances(
        &self,
    ) -> Option<&BTreeMap<a::ExecutionId, NoGraphAcceptance>> {
        self.metadata.terminal.as_ref().map(|state| &state.accepted)
    }
    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate the terminal successor."
    )]
    pub(crate) fn terminal_operation_links(&self) -> Option<&BTreeMap<u64, a::ExecutionId>> {
        self.metadata
            .terminal
            .as_ref()
            .map(|state| &state.operations)
    }

    fn no_graph_payload(result: &a::ResultPayload) -> Result<(), Error> {
        if serde_json::to_vec(result)
            .map_err(|_| Error::Invalid)?
            .len()
            > MAX_PAYLOAD
        {
            return Err(Error::Bounds);
        }
        let response = result.require_response().map_err(|_| Error::Invalid)?;
        if response
            .decode()
            .map_err(|_| Error::Invalid)?
            .result
            .is_some()
        {
            return Err(Error::Invalid);
        }
        Ok(())
    }

    fn report_identity(&self, report: &NoGraphReport) -> Result<(), Error> {
        Self::no_graph_payload(&report.result)?;
        let view = self.scheduler.view();
        let origin = &report.origin;
        if origin.epoch != view.epoch {
            return Err(Error::Unauthorized);
        }
        let execution = view
            .executions
            .iter()
            .find(|row| row.id == origin.execution)
            .ok_or(Error::Unauthorized)?;
        if !self.request_matches_action(&origin.action, &execution.request) {
            return Err(Error::Unauthorized);
        }
        let attempt = view
            .attempts
            .iter()
            .find(|row| row.id == origin.attempt)
            .ok_or(Error::Unauthorized)?;
        if attempt.execution != origin.execution
            || attempt.worker != origin.worker
            || attempt.incarnation != origin.incarnation
        {
            return Err(Error::Unauthorized);
        }
        if let TerminalReportKind::MemoryFailure {
            charged, quiescent, ..
        } = report.kind
        {
            let reservation = view
                .reservations
                .iter()
                .find(|row| row.id == attempt.reservation)
                .ok_or(Error::Corrupt)?;
            if !quiescent || charged != reservation.charged {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "Older shared fixtures do not activate the terminal successor."
    )]
    pub(crate) fn report_no_graph(
        &mut self,
        id: u64,
        report: NoGraphReport,
    ) -> Result<NoGraphOutcome, Error> {
        let terminal = self.metadata.terminal.as_ref().ok_or(Error::Invalid)?;
        if let Some(old) = terminal.reports.get(&id) {
            return if old.report == report {
                Ok(NoGraphOutcome::Duplicate)
            } else {
                Err(Error::Conflict)
            };
        }
        self.report_identity(&report)?;
        if terminal.reports.len() >= MAX_STAGES * a::MAX_ATTEMPTS as usize
            || terminal
                .reports
                .values()
                .filter(|row| row.report.origin.execution == report.origin.execution)
                .count()
                >= a::MAX_ATTEMPTS as usize
            || terminal
                .reports
                .values()
                .any(|row| row.report.origin.attempt == report.origin.attempt)
        {
            return Err(Error::Bounds);
        }
        self.transaction(|next| {
            let origin = &report.origin;
            let event = match report.kind {
                TerminalReportKind::Complete => a::Event::Complete {
                    attempt: origin.attempt,
                    worker: origin.worker,
                    incarnation: origin.incarnation,
                    result: report.result.clone(),
                },
                TerminalReportKind::MemoryFailure {
                    charged,
                    peak_memory_bytes,
                    quiescent,
                } => a::Event::MemoryFailure {
                    attempt: origin.attempt,
                    worker: origin.worker,
                    incarnation: origin.incarnation,
                    charged,
                    peak_memory_bytes,
                    quiescent,
                    result: report.result.clone(),
                },
            };
            if next.scheduler_apply(event)? != a::Reply::Applied {
                return Err(Error::Conflict);
            }
            let view = next.scheduler.view();
            let execution = view
                .executions
                .iter()
                .find(|row| row.id == origin.execution)
                .ok_or(Error::Corrupt)?;
            let (disposition, outcome) = match (&report.kind, &execution.terminal) {
                (
                    TerminalReportKind::Complete,
                    Some(value @ a::TerminalResult::Completed(result)),
                ) if result == &report.result => (
                    ReportDisposition::Terminal(value.clone()),
                    NoGraphOutcome::TerminalAccepted(value.clone()),
                ),
                (
                    TerminalReportKind::MemoryFailure { .. },
                    Some(value @ a::TerminalResult::ResourceFailed { result, .. }),
                ) if result == &report.result => (
                    ReportDisposition::Terminal(value.clone()),
                    NoGraphOutcome::TerminalAccepted(value.clone()),
                ),
                (TerminalReportKind::MemoryFailure { .. }, None)
                    if execution.phase == a::Phase::Queued && execution.attempt.is_none() =>
                {
                    (
                        ReportDisposition::Retry {
                            target: execution.reservation_target,
                        },
                        NoGraphOutcome::RetryScheduled,
                    )
                }
                _ => return Err(Error::Corrupt),
            };
            let terminal = next.metadata.terminal.as_mut().ok_or(Error::Corrupt)?;
            if let ReportDisposition::Terminal(value) = &disposition {
                if terminal.accepted.contains_key(&origin.execution)
                    || terminal.accepted.len() >= MAX_STAGES
                {
                    return Err(Error::Bounds);
                }
                terminal.accepted.insert(
                    origin.execution,
                    NoGraphAcceptance {
                        report_id: id,
                        origin: origin.clone(),
                        terminal: value.clone(),
                        position: next.metadata.commit,
                        at: view.now,
                    },
                );
                for operation in &view.operations {
                    if operation.execution == origin.execution
                        && operation.terminal.as_ref() == Some(value)
                        && terminal
                            .operations
                            .insert(operation.id.0, origin.execution)
                            .is_some()
                    {
                        return Err(Error::Corrupt);
                    }
                }
            }
            terminal.reports.insert(
                id,
                ClassifiedReport {
                    report,
                    position: next.metadata.commit,
                    at: view.now,
                    disposition,
                },
            );
            Ok(outcome)
        })
    }

    pub(super) fn reconcile_terminal_credits(&mut self) {
        if self.metadata.terminal.is_none() {
            return;
        }
        let view = self.scheduler.view();
        for execution in &view.executions {
            if execution.terminal.is_some()
                && let Some(credit) = self.metadata.credits.get_mut(&execution.id.0)
            {
                credit.closed = true;
                credit.physical_remaining = 0;
            }
        }
    }

    pub(super) fn operation_has_no_graph(
        &self,
        operation: u64,
        value: Option<&a::TerminalResult>,
    ) -> bool {
        self.metadata.terminal.as_ref().is_some_and(|state| {
            state
                .operations
                .get(&operation)
                .and_then(|execution| state.accepted.get(execution))
                .is_some_and(|accepted| Some(&accepted.terminal) == value)
        })
    }

    pub(super) fn validate_terminal_state(&self) -> Result<(), Error> {
        let Some(terminal_state) = &self.metadata.terminal else {
            return Ok(());
        };
        let view = self.scheduler.view();
        if terminal_state.enabled_at == 0
            || terminal_state.enabled_at > self.metadata.commit
            || terminal_state.reports.len() > MAX_STAGES * a::MAX_ATTEMPTS as usize
            || terminal_state.accepted.len() > MAX_STAGES
            || terminal_state.operations.len() > MAX_OPERATIONS
        {
            return Err(Error::Corrupt);
        }
        let mut positions = self.retained_publication_positions();
        if !positions.insert(terminal_state.enabled_at) {
            return Err(Error::Corrupt);
        }
        let mut dated_positions = self
            .metadata
            .accepted
            .values()
            .map(|accepted| (accepted.commit, accepted.at))
            .chain(
                terminal_state
                    .reports
                    .values()
                    .map(|report| (report.position, report.at)),
            )
            .collect::<Vec<_>>();
        dated_positions.sort_unstable_by_key(|(position, _)| *position);
        if dated_positions.windows(2).any(|pair| pair[0].1 > pair[1].1) {
            return Err(Error::Corrupt);
        }
        let mut attempts = BTreeSet::new();
        let mut counts = BTreeMap::<a::ExecutionId, usize>::new();
        let mut expected_accepted = BTreeMap::new();
        for (id, record) in &terminal_state.reports {
            self.report_identity(&record.report)
                .map_err(|_| Error::Corrupt)?;
            let report = &record.report;
            let origin = &report.origin;
            let execution = view
                .executions
                .iter()
                .find(|row| row.id == origin.execution)
                .ok_or(Error::Corrupt)?;
            let attempt = view
                .attempts
                .iter()
                .find(|row| row.id == origin.attempt)
                .ok_or(Error::Corrupt)?;
            let reservation = view
                .reservations
                .iter()
                .find(|row| row.id == attempt.reservation)
                .ok_or(Error::Corrupt)?;
            let count = counts.entry(origin.execution).or_default();
            *count += 1;
            if *count > a::MAX_ATTEMPTS as usize
                || record.position <= terminal_state.enabled_at
                || record.position > self.metadata.commit
                || record.at > view.now
                || record.at < execution.enqueue_at
                || execution.terminal_at.is_some_and(|at| record.at > at)
                || !positions.insert(record.position)
                || !attempts.insert(origin.attempt)
                || attempt.authorized
                || !reservation.released
            {
                return Err(Error::Corrupt);
            }
            let ordinal = view
                .attempts
                .iter()
                .filter(|row| row.execution == origin.execution)
                .filter(|row| row.id <= origin.attempt)
                .count();
            if terminal_state.reports.values().any(|earlier| {
                earlier.report.origin.execution == origin.execution
                    && earlier.report.origin.attempt < origin.attempt
                    && (earlier.position >= record.position || earlier.at > record.at)
            }) {
                return Err(Error::Corrupt);
            }
            let memory_base = match report.kind {
                TerminalReportKind::MemoryFailure {
                    charged,
                    peak_memory_bytes,
                    ..
                } => Some(if charged.memory_bytes == 0 {
                    peak_memory_bytes.unwrap_or(0)
                } else {
                    charged.memory_bytes
                }),
                TerminalReportKind::Complete => None,
            };
            match &record.disposition {
                ReportDisposition::Retry { target } => {
                    let TerminalReportKind::MemoryFailure { charged, .. } = report.kind else {
                        return Err(Error::Corrupt);
                    };
                    let base = memory_base.ok_or(Error::Corrupt)?;
                    let successor = view
                        .attempts
                        .iter()
                        .filter(|row| row.execution == origin.execution)
                        .filter(|row| row.id > origin.attempt)
                        .min_by_key(|row| row.id);
                    let expected = if let Some(successor) = successor {
                        view.reservations
                            .iter()
                            .find(|row| row.id == successor.reservation)
                            .ok_or(Error::Corrupt)?
                            .charged
                    } else {
                        execution.reservation_target
                    };
                    if ordinal >= a::MAX_ATTEMPTS as usize
                        || base == 0
                        || target.memory_bytes > base.saturating_mul(2)
                        || *target != expected
                        || target.cpu_millis != charged.cpu_millis
                        || target.slots != charged.slots
                        || target.memory_bytes <= charged.memory_bytes
                        || target.memory_bytes > execution.reservation_target.memory_bytes
                    {
                        return Err(Error::Corrupt);
                    }
                    for accepted in self.metadata.accepted.values() {
                        let stage = self
                            .metadata
                            .stages
                            .get(&accepted.stage)
                            .ok_or(Error::Corrupt)?;
                        if matches!(stage.candidate.publisher, Publisher::Worker { execution, .. } if execution == origin.execution)
                            && accepted.commit <= record.position
                        {
                            return Err(Error::Corrupt);
                        }
                    }
                    if matches!(
                        execution.terminal,
                        Some(
                            a::TerminalResult::Completed(_)
                                | a::TerminalResult::ResourceFailed { .. }
                        )
                    ) && view
                        .attempts
                        .iter()
                        .filter(|row| row.execution == origin.execution)
                        .map(|row| row.id)
                        .max()
                        .is_none_or(|last| last <= origin.attempt)
                    {
                        return Err(Error::Corrupt);
                    }
                }
                ReportDisposition::Terminal(value) => {
                    // Validate immutable evidence that is still available.
                    // Current workers cannot reconstruct a historical ceiling.
                    if let a::TerminalResult::ResourceFailed { reason, .. } = value {
                        let base = memory_base.ok_or(Error::Corrupt)?;
                        let consistent = match reason {
                            a::ResourceFailureReason::AttemptBudgetExhausted => {
                                ordinal == a::MAX_ATTEMPTS as usize
                            }
                            a::ResourceFailureReason::MissingGrowthEvidence => {
                                ordinal < a::MAX_ATTEMPTS as usize && base == 0
                            }
                            a::ResourceFailureReason::NoLargerEligibleWorker => {
                                ordinal < a::MAX_ATTEMPTS as usize && base > 0
                            }
                        };
                        if !consistent {
                            return Err(Error::Corrupt);
                        }
                    }
                    let correct = match (&report.kind, value) {
                        (TerminalReportKind::Complete, a::TerminalResult::Completed(result))
                        | (
                            TerminalReportKind::MemoryFailure { .. },
                            a::TerminalResult::ResourceFailed { result, .. },
                        ) => result == &report.result,
                        _ => false,
                    };
                    if !correct
                        || execution.terminal.as_ref() != Some(value)
                        || execution.terminal_at != Some(record.at)
                        || view
                            .attempts
                            .iter()
                            .filter(|row| row.execution == origin.execution)
                            .map(|row| row.id)
                            .max()
                            != Some(origin.attempt)
                    {
                        return Err(Error::Corrupt);
                    }
                    for accepted in self.metadata.accepted.values() {
                        let stage = self
                            .metadata
                            .stages
                            .get(&accepted.stage)
                            .ok_or(Error::Corrupt)?;
                        if matches!(stage.candidate.publisher, Publisher::Worker { execution, .. } if execution == origin.execution)
                        {
                            return Err(Error::Corrupt);
                        }
                    }
                    if expected_accepted
                        .insert(
                            origin.execution,
                            NoGraphAcceptance {
                                report_id: *id,
                                origin: origin.clone(),
                                terminal: value.clone(),
                                position: record.position,
                                at: record.at,
                            },
                        )
                        .is_some()
                    {
                        return Err(Error::Corrupt);
                    }
                }
            }
        }
        if terminal_state.accepted != expected_accepted {
            return Err(Error::Corrupt);
        }
        let mut expected_operations = BTreeMap::new();
        for operation in &view.operations {
            if let Some(accepted) = terminal_state.accepted.get(&operation.execution)
                && operation.terminal.as_ref() == Some(&accepted.terminal)
            {
                let original = self
                    .scheduler
                    .operation_request(operation.id)
                    .ok_or(Error::Corrupt)?;
                if !self.request_matches_action(&accepted.origin.action, original) {
                    return Err(Error::Corrupt);
                }
                let outer = self
                    .metadata
                    .operations
                    .get(&operation.id.0)
                    .ok_or(Error::Corrupt)?;
                if outer.publication.is_some()
                    || outer.root.is_some()
                    || outer.payload.is_some()
                    || outer.cached
                {
                    return Err(Error::Corrupt);
                }
                expected_operations.insert(operation.id.0, operation.execution);
            }
        }
        if terminal_state.operations != expected_operations {
            return Err(Error::Corrupt);
        }
        for execution in &view.executions {
            // A changed target requires its retained memory report. Checking the
            // whole reservation chain also detects removal of a retry record.
            let mut expected_target = execution.request.resources;
            let mut execution_attempts = view
                .attempts
                .iter()
                .filter(|attempt| attempt.execution == execution.id)
                .collect::<Vec<_>>();
            execution_attempts.sort_unstable_by_key(|attempt| attempt.id);
            for attempt in execution_attempts {
                let reservation = view
                    .reservations
                    .iter()
                    .find(|reservation| reservation.id == attempt.reservation)
                    .ok_or(Error::Corrupt)?;
                if reservation.charged != expected_target {
                    return Err(Error::Corrupt);
                }
                if let Some(record) = terminal_state
                    .reports
                    .values()
                    .find(|record| record.report.origin.attempt == attempt.id)
                    && let ReportDisposition::Retry { target } = record.disposition
                {
                    expected_target = target;
                }
            }
            if expected_target != execution.reservation_target {
                return Err(Error::Corrupt);
            }
            if let Some(terminal) = &execution.terminal {
                match terminal {
                    a::TerminalResult::ResourceFailed { .. } => {
                        if !terminal_state.accepted.contains_key(&execution.id) {
                            return Err(Error::Corrupt);
                        }
                    }
                    a::TerminalResult::Completed(result) => {
                        if result
                            .require_response()
                            .map_err(|_| Error::Corrupt)?
                            .decode()
                            .map_err(|_| Error::Corrupt)?
                            .result
                            .is_none()
                            && !terminal_state.accepted.contains_key(&execution.id)
                        {
                            return Err(Error::Corrupt);
                        }
                    }
                    a::TerminalResult::Cancelled | a::TerminalResult::Exhausted => {}
                }
            }
        }
        Ok(())
    }
}
