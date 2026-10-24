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

//! Explicit command-boundary adapter. Neither frozen reducer is modified.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
use nativelink_scheduler::authority as a;
use prost::Message as _;
use serde_json::Value;

use super::fixtures::{self as f, LABEL, SEED};
use super::publication_candidate::engine::PublicationAuthority;
use super::publication_candidate::types as c;
use super::reference::Model;
use super::{protocol as p, types as r};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Applied,
    Ignored,
    Rejected,
    Duplicate,
    Retry,
    Terminal(a::TerminalResult),
}

pub(super) struct Pair {
    pub actual: PublicationAuthority,
    pub model: Model,
    pub algorithm: u8,
    pub memory_kb: u64,
    originals: BTreeMap<a::OperationId, a::Request>,
    native_candidate_positions: BTreeMap<u64, usize>,
    native_reference_positions: BTreeMap<u64, usize>,
    graph_baseline: Value,
    pub history: Vec<r::Command>,
}

impl Pair {
    pub(super) fn new(algorithm: u8, memory_kb: u64, protected: bool) -> Self {
        let actions = [LABEL, SEED]
            .into_iter()
            .map(|label| {
                let request = f::request(algorithm, memory_kb, 0, label);
                (
                    label.to_owned(),
                    c::ActionBinding::capture(&f::context(), &request).unwrap(),
                )
            })
            .collect();
        let mut actual = PublicationAuthority::new_real(f::context(), actions).unwrap();
        let seeds = if protected {
            Self::seed_publication(&mut actual, algorithm, memory_kb);
            vec![r::Seed {
                kind: p::RootKind::Publication,
                members: vec![f::physical(1)],
                until: 80,
            }]
        } else {
            vec![]
        };
        actual.enable_terminal_outcomes().unwrap();
        let model = Model::new(
            r::Context {
                principal: f::context().principal,
                execution_config: f::context().execution_config,
            },
            &seeds,
        )
        .unwrap();
        let graph_baseline = Self::graph_metadata(&actual);
        let mut pair = Self {
            native_candidate_positions: BTreeMap::from([(actual.commit(), 0)]),
            native_reference_positions: BTreeMap::from([(model.view().unwrap().position, 0)]),
            actual,
            model,
            algorithm,
            memory_kb,
            originals: BTreeMap::new(),
            graph_baseline,
            history: vec![],
        };
        pair.check_and_recover();
        pair
    }

    fn seed_publication(actual: &mut PublicationAuthority, algorithm: u8, memory_kb: u64) {
        let context = f::context();
        let binding =
            c::ActionBinding::capture(&context, &f::request(algorithm, memory_kb, 0, SEED))
                .unwrap();
        let result = ActionResult::default();
        let bytes = result.encode_to_vec();
        let key = f::key(f::physical(1));
        let object = c::Object::for_bytes(algorithm, &bytes).unwrap();
        actual.install_physical(key, object, false).unwrap();
        actual
            .authorize_update(1, &context, SEED, &binding)
            .unwrap();
        let closure = BTreeSet::from([c::Member {
            key,
            object,
            role: c::Role::Result,
        }]);
        let candidate = actual
            .direct_update_candidate(1, &context, &binding, result, closure.clone())
            .unwrap();
        actual
            .stage(1, candidate, 10, BTreeMap::from([(1, key)]))
            .unwrap();
        actual.observe(1, 1, key, object, true).unwrap();
        actual
            .prepare(
                1,
                c::Evidence {
                    stage: 1,
                    closure: closure.clone(),
                    catalogue_revision: actual.catalogue_revision(),
                    fences: actual.fences(&closure).unwrap(),
                    until: 80,
                },
            )
            .unwrap();
        actual.publish(1, 1, 80).unwrap();
    }

    fn graph_metadata(actual: &PublicationAuthority) -> Value {
        serde_json::json!({ "heads": actual.heads(), "accepted": actual.accepted(), "grants": actual.grants() })
    }

    pub(super) fn view(&self) -> a::ObservableState {
        self.actual.scheduler_view()
    }

    pub(super) fn admit(&mut self, id: u64, caller: i32) {
        let request = f::request(self.algorithm, self.memory_kb, caller, LABEL);
        assert_eq!(
            self.step(r::Command::Admit {
                operation: a::OperationId(id),
                request
            }),
            Outcome::Applied
        );
    }

    pub(super) fn event(&mut self, event: a::Event) -> Outcome {
        self.step(r::Command::Scheduler(event))
    }

    pub(super) fn identity(&self, attempt: u64) -> r::WorkerIdentity {
        let row = self
            .view()
            .attempts
            .into_iter()
            .find(|row| row.id.0 == attempt)
            .unwrap();
        r::WorkerIdentity {
            attempt: row.id,
            worker: row.worker,
            incarnation: row.incarnation,
        }
    }

    pub(super) fn report(
        &self,
        id: u64,
        attempt: u64,
        memory: bool,
        peak: Option<u64>,
    ) -> r::Report {
        let view = self.view();
        let trial = view
            .attempts
            .iter()
            .find(|trial| trial.id.0 == attempt)
            .unwrap();
        let charged = view
            .reservations
            .iter()
            .find(|row| row.id == trial.reservation)
            .unwrap()
            .charged;
        r::Report {
            id,
            identity: self.identity(attempt),
            payload: f::response("independent failure", false),
            classification: if memory {
                r::Classification::MemoryFailure {
                    original_charged: charged,
                    peak_memory_bytes: peak,
                    quiescent: true,
                }
            } else {
                r::Classification::Complete
            },
        }
    }

    pub(super) fn stage(&mut self, id: u64, attempt: u64) -> Vec<p::WriteId> {
        let start = u64::try_from(self.model.view().unwrap().physical.len()).unwrap() + 1;
        let members = (start..start + 8).map(f::physical).collect();
        assert_eq!(
            self.step(r::Command::Stage {
                id,
                attempt: a::AttemptId(attempt),
                members,
                until: self.view().now + 10,
            }),
            Outcome::Applied
        );
        self.model.view().unwrap().stages[&id].writes.clone()
    }

    fn apply_candidate(&mut self, command: &r::Command) -> Outcome {
        let result = match command {
            r::Command::Admit { operation, request } => self
                .actual
                .admit_request(
                    operation.0,
                    LABEL.into(),
                    &f::context(),
                    request.clone(),
                    self.view().now + 100,
                )
                .map(|_| Outcome::Applied),
            r::Command::Scheduler(a::Event::AdvanceTime { now }) => {
                let same = *now == self.view().now;
                self.actual.advance(*now).map(|()| {
                    if same {
                        Outcome::Ignored
                    } else {
                        Outcome::Applied
                    }
                })
            }
            r::Command::Scheduler(event) => {
                self.actual.scheduler_event(event.clone()).map(|reply| {
                    if reply == a::Reply::Ignored {
                        Outcome::Ignored
                    } else {
                        Outcome::Applied
                    }
                })
            }
            r::Command::Report(report) => {
                let view = self.view();
                let execution = view
                    .attempts
                    .iter()
                    .find(|row| row.id == report.identity.attempt)
                    .map_or(a::ExecutionId(1), |row| row.execution);
                self.actual
                    .report_no_graph(
                        report.id,
                        c::NoGraphReport {
                            origin: c::WorkerOrigin {
                                epoch: view.epoch,
                                action: LABEL.into(),
                                execution,
                                attempt: report.identity.attempt,
                                worker: report.identity.worker,
                                incarnation: report.identity.incarnation,
                            },
                            kind: match report.classification {
                                r::Classification::Complete => c::TerminalReportKind::Complete,
                                r::Classification::MemoryFailure {
                                    original_charged,
                                    peak_memory_bytes,
                                    quiescent,
                                } => c::TerminalReportKind::MemoryFailure {
                                    charged: original_charged,
                                    peak_memory_bytes,
                                    quiescent,
                                },
                            },
                            result: report.payload.clone(),
                        },
                    )
                    .map(|reply| match reply {
                        c::NoGraphOutcome::Duplicate => Outcome::Duplicate,
                        c::NoGraphOutcome::RetryScheduled => Outcome::Retry,
                        c::NoGraphOutcome::TerminalAccepted(value) => Outcome::Terminal(value),
                    })
            }
            r::Command::Stage {
                id,
                attempt,
                members,
                until,
            } => self.candidate_stage(*id, *attempt, members, *until),
            r::Command::WriteDone { write, success } => {
                let found = self.actual.stages().iter().find_map(|(id, stage)| {
                    stage
                        .internal_writes
                        .iter()
                        .find(|(_, internal)| **internal == write.0)
                        .map(|(external, _)| {
                            (
                                *id,
                                *external,
                                stage.writes[external],
                                stage.outcomes.get(external) == Some(success),
                            )
                        })
                });
                if let Some((stage, external, key, duplicate)) = found {
                    let object = self.actual.stages()[&stage]
                        .candidate
                        .closure
                        .iter()
                        .find(|row| row.key == key)
                        .unwrap()
                        .object;
                    self.actual
                        .observe(stage, external, key, object, *success)
                        .map(|()| {
                            if duplicate {
                                Outcome::Ignored
                            } else {
                                Outcome::Applied
                            }
                        })
                } else {
                    Err(c::Error::Invalid)
                }
            }
            r::Command::Abandon(stage) => self.actual.abandon(*stage).map(|()| Outcome::Applied),
        };
        result.unwrap_or(Outcome::Rejected)
    }

    fn candidate_stage(
        &mut self,
        id: u64,
        attempt: a::AttemptId,
        members: &[r::Physical],
        until: u64,
    ) -> Result<Outcome, c::Error> {
        // The reference's atomic Stage includes fresh allocation. The candidate
        // exposes these as separate commands; compare only this explicit clone boundary.
        let mut next = self.actual.clone();
        let trial = next
            .scheduler_view()
            .attempts
            .into_iter()
            .find(|row| row.id == attempt)
            .ok_or(c::Error::Invalid)?;
        let result_bytes = ActionResult::default().encode_to_vec();
        let mut closure = BTreeSet::new();
        let mut writes = BTreeMap::new();
        for (index, physical) in members.iter().enumerate() {
            let bytes = if index == 0 {
                result_bytes.clone()
            } else {
                format!("stage-{id}-member-{index}").into_bytes()
            };
            let object = c::Object::for_bytes(self.algorithm, &bytes)?;
            let key = f::key(*physical);
            next.install_physical_for(Some(trial.execution), key, object, false)?;
            closure.insert(c::Member {
                key,
                object,
                role: if index == 0 {
                    c::Role::Result
                } else {
                    c::Role::Blob
                },
            });
            writes.insert(physical.generation, key);
        }
        next.stage(
            id,
            c::Candidate {
                action: LABEL.into(),
                publisher: c::Publisher::Worker {
                    execution: trial.execution,
                    attempt,
                    worker: trial.worker,
                    incarnation: trial.incarnation,
                },
                result: c::Object::for_bytes(self.algorithm, &result_bytes)?,
                result_bytes,
                response: a::ResultPayload::from_response(
                    &ExecuteResponse {
                        result: Some(ActionResult::default()),
                        ..ExecuteResponse::default()
                    },
                    c::MAX_PAYLOAD,
                )
                .unwrap(),
                closure,
            },
            until,
            writes,
        )?;
        self.actual = next;
        Ok(Outcome::Applied)
    }

    pub(super) fn step(&mut self, command: r::Command) -> Outcome {
        let before_candidate = self.actual.snapshot().unwrap();
        let before_reference = self.model.snapshot().unwrap();
        self.history.push(command);
        let command = self.history.last().unwrap().clone();
        let actual = self.apply_candidate(&command);
        let expected = match self.model.apply(&command) {
            Err(_) => Outcome::Rejected,
            Ok(r::Reply::Duplicate) => Outcome::Duplicate,
            Ok(r::Reply::Ignored | r::Reply::Scheduler(a::Reply::Ignored)) => Outcome::Ignored,
            Ok(r::Reply::Report(r::Disposition::RetryScheduled)) => Outcome::Retry,
            Ok(r::Reply::Report(r::Disposition::TerminalAccepted(value))) => {
                Outcome::Terminal(value.terminal)
            }
            Ok(_) => Outcome::Applied,
        };
        assert_eq!(actual, expected, "command history: {:#?}", self.history);
        if matches!(
            actual,
            Outcome::Rejected | Outcome::Ignored | Outcome::Duplicate
        ) {
            assert_eq!(
                self.actual.snapshot().unwrap(),
                before_candidate,
                "candidate no-op: {:?}",
                self.history
            );
            assert_eq!(
                self.model.snapshot().unwrap(),
                before_reference,
                "reference no-op: {:?}",
                self.history
            );
        }
        if let r::Command::Admit { operation, request } = &command
            && actual == Outcome::Applied
        {
            self.originals.insert(*operation, request.clone());
        }
        if let r::Command::Report(report) = &command
            && matches!(actual, Outcome::Retry | Outcome::Terminal(_))
        {
            // Check native acceptance positions before converting them to a
            // shared command ordinal; mapping must not conceal wrong provenance.
            assert_eq!(
                self.actual.terminal_reports().unwrap()[&report.id].position,
                self.actual.commit()
            );
            let view = self.model.view().unwrap();
            assert_eq!(view.reports[&report.id].position, view.position);
        }
        self.native_candidate_positions
            .entry(self.actual.commit())
            .or_insert(self.history.len());
        self.native_reference_positions
            .entry(self.model.view().unwrap().position)
            .or_insert(self.history.len());
        self.check_and_recover();
        actual
    }

    fn check_and_recover(&mut self) {
        self.compare();
        let actual = self.actual.snapshot().unwrap();
        let reference = self.model.snapshot().unwrap();
        self.actual = PublicationAuthority::restore(&actual).unwrap();
        self.model = Model::restore(&reference).unwrap();
        assert_eq!(self.actual.snapshot().unwrap(), actual);
        assert_eq!(self.model.snapshot().unwrap(), reference);
        self.actual.lose_index();
        assert_eq!(self.actual.snapshot().unwrap(), actual);
        self.actual.rebuild_index();
        self.compare();
    }

    fn compare(&self) {
        let expected = self.model.view().unwrap();
        let actual = self.view();
        assert_eq!(
            actual, expected.scheduler,
            "full scheduler mismatch: {:?}",
            self.history
        );
        assert_eq!(Self::graph_metadata(&self.actual), self.graph_baseline);
        assert_eq!(expected.original_requests, self.originals);
        let snapshot: Value = serde_json::from_slice(&self.actual.snapshot().unwrap()).unwrap();
        let scheduler_bytes: Vec<u8> =
            serde_json::from_value(snapshot["scheduler"].clone()).unwrap();
        let scheduler = a::State::restore(&scheduler_bytes).unwrap();
        assert_eq!(
            snapshot["metadata"]["credits"].as_object().unwrap().len(),
            expected.credits.len()
        );
        for (id, original) in &self.originals {
            assert_eq!(scheduler.operation_request(*id), Some(original));
        }
        for execution in &actual.executions {
            let action = &expected.actions[&execution.id];
            let binding = c::ActionBinding::capture(&f::context(), &execution.request).unwrap();
            assert_eq!(action.key, binding.action);
            assert_eq!(action.semantics, binding.platform);
            assert_eq!(action.context.principal, binding.context.principal);
            assert_eq!(
                action.context.execution_config,
                binding.context.execution_config
            );
            let credit = &snapshot["metadata"]["credits"][execution.id.0.to_string()];
            let reference_credit = &expected.credits[&execution.id];
            let closed = credit["closed"].as_bool().unwrap_or(false);
            assert_eq!(closed, execution.terminal.is_some());
            assert_eq!(
                !closed && credit["stage"].is_null(),
                reference_credit.pending_stage
            );
            assert_eq!(
                credit["physical_remaining"].as_u64().unwrap(),
                u64::try_from(reference_credit.unused_members).unwrap()
            );
            let mut stages: Vec<u64> =
                serde_json::from_value(credit["prior_stages"].clone()).unwrap();
            if let Some(stage) = credit["stage"].as_u64() {
                stages.push(stage);
            }
            assert_eq!(stages, reference_credit.stages);
            assert_eq!(
                reference_credit.attempt,
                actual
                    .attempts
                    .iter()
                    .filter(|row| row.execution == execution.id)
                    .map(|row| row.id)
                    .max()
            );
        }
        self.compare_reports(&expected);
        for operation in &actual.operations {
            let link = match &operation.terminal {
                None => r::OperationLink::Waiting,
                Some(value @ (a::TerminalResult::Cancelled | a::TerminalResult::Exhausted)) => {
                    r::OperationLink::Independent(value.clone())
                }
                Some(_) => {
                    r::OperationLink::NoGraph(expected.accepted[&operation.execution].report)
                }
            };
            assert_eq!(expected.operations[&operation.id], link);
            let outer = &self.actual.operations()[&operation.id.0];
            assert!(
                outer.root.is_none()
                    && outer.publication.is_none()
                    && outer.payload.is_none()
                    && !outer.cached
            );
            assert_eq!(
                self.actual
                    .terminal_operation_links()
                    .unwrap()
                    .get(&operation.id.0)
                    .copied(),
                matches!(link, r::OperationLink::NoGraph(_)).then_some(operation.execution)
            );
        }
        // Structured physical keys cannot be JSON object keys. Preserve every
        // ordered map entry as an array, without dropping retention fields.
        macro_rules! retention_wire {
            ($view:expr) => {{
                let view = $view;
                serde_json::json!({
                    "now": view.now, "next_delete": view.next_delete,
                    "cohorts": view.cohorts.iter().collect::<Vec<_>>(),
                    "generations": view.generations.iter().collect::<Vec<_>>(),
                    "roots": view.roots.iter().collect::<Vec<_>>(),
                    "writes": view.writes.iter().collect::<Vec<_>>(),
                    "deletes": view.deletes.iter().collect::<Vec<_>>(),
                    "protected": view.protected,
                })
            }};
        }
        let actual_retention = retention_wire!(self.actual.retention_view());
        let reference_retention = retention_wire!(&expected.retention);
        for name in [
            "now",
            "next_delete",
            "generations",
            "roots",
            "writes",
            "deletes",
            "protected",
        ] {
            assert_eq!(
                actual_retention[name], reference_retention[name],
                "retention {name}: {:?}",
                self.history
            );
        }
        if expected.physical.is_empty() {
            assert!(self.actual.retention_view().cohorts.is_empty());
            assert_eq!(expected.retention.cohorts.len(), 1);
            let empty = &expected.retention.cohorts[&p::CohortId(1)];
            assert_eq!(empty.phase, p::Phase::Allocating);
            assert_eq!(empty.fence, 0);
            assert!(empty.captured.is_empty() && empty.marked.is_empty());
            assert_eq!(empty.cursor, 0);
        } else {
            assert_eq!(actual_retention["cohorts"], reference_retention["cohorts"]);
        }
        let physical = snapshot["metadata"]["physical"].as_array().unwrap();
        let namespaces = snapshot["metadata"]["namespaces"].as_array().unwrap();
        assert_eq!(physical.len(), expected.physical.len());
        for (row, (identity, internal)) in physical.iter().zip(&expected.physical) {
            let key: c::Key = serde_json::from_value(row["key"].clone()).unwrap();
            assert_eq!(key, f::key(*identity));
            let namespace = namespaces
                .iter()
                .find(|namespace| {
                    namespace["namespace"] == row["key"]["namespace"]
                        && namespace["incarnation"] == row["key"]["incarnation"]
                })
                .unwrap();
            assert_eq!(namespace["handle"].as_u64().unwrap(), internal.incarnation);
            assert_eq!(key.generation, internal.generation);
            assert_eq!(
                row["logical"].as_u64().unwrap(),
                expected.retention.generations[internal].logical_object
            );
        }
        assert_eq!(
            self.actual.stages().len(),
            expected.stages.len() + self.actual.accepted().len()
        );
        for (id, stage) in &expected.stages {
            let actual = &self.actual.stages()[id];
            assert_eq!(actual.root, stage.root.0);
            assert_eq!(
                actual.state,
                if stage.abandoned {
                    c::StageState::Abandoned
                } else {
                    c::StageState::Open
                }
            );
            let physical: BTreeSet<_> = actual
                .candidate
                .closure
                .iter()
                .map(|member| r::Physical {
                    namespace: member.key.namespace,
                    incarnation: member.key.incarnation,
                    generation: member.key.generation,
                })
                .collect();
            assert_eq!(physical, stage.members.iter().copied().collect());
            let trial = expected
                .scheduler
                .attempts
                .iter()
                .find(|trial| trial.id == stage.attempt)
                .unwrap();
            assert_eq!(
                actual.candidate.publisher,
                c::Publisher::Worker {
                    execution: stage.execution,
                    attempt: stage.attempt,
                    worker: trial.worker,
                    incarnation: trial.incarnation,
                }
            );
            assert_eq!(
                actual.internal_writes.values().copied().collect::<Vec<_>>(),
                stage.writes.iter().map(|id| id.0).collect::<Vec<_>>()
            );
        }
    }

    fn compare_reports(&self, expected: &r::View) {
        let reports = self.actual.terminal_reports().unwrap();
        assert_eq!(reports.len(), expected.reports.len());
        for (id, report) in &expected.reports {
            let actual = &reports[id];
            assert_eq!(actual.report.origin.epoch, expected.scheduler.epoch);
            assert_eq!(actual.report.origin.action, LABEL);
            assert_eq!(actual.report.origin.execution, report.execution);
            assert_eq!(actual.report.origin.attempt, report.report.identity.attempt);
            assert_eq!(actual.report.origin.worker, report.report.identity.worker);
            assert_eq!(
                actual.report.origin.incarnation,
                report.report.identity.incarnation
            );
            assert_eq!(actual.report.result, report.report.payload);
            assert_eq!(actual.at, report.now);
            assert_eq!(
                self.native_candidate_positions[&actual.position],
                self.native_reference_positions[&report.position]
            );
            match (&actual.report.kind, &report.report.classification) {
                (c::TerminalReportKind::Complete, r::Classification::Complete) => {}
                (
                    c::TerminalReportKind::MemoryFailure {
                        charged,
                        peak_memory_bytes,
                        quiescent,
                    },
                    r::Classification::MemoryFailure {
                        original_charged,
                        peak_memory_bytes: peak,
                        quiescent: quiet,
                    },
                ) => {
                    assert_eq!(charged, original_charged);
                    assert_eq!(peak_memory_bytes, peak);
                    assert_eq!(quiescent, quiet);
                }
                _ => panic!("classification mismatch"),
            }
            match (&actual.disposition, &report.disposition) {
                (c::ReportDisposition::Retry { target }, r::Disposition::RetryScheduled) => {
                    let successor = expected
                        .scheduler
                        .attempts
                        .iter()
                        .filter(|trial| {
                            trial.execution == report.execution
                                && trial.id > report.report.identity.attempt
                        })
                        .min_by_key(|trial| trial.id);
                    let expected_target = successor.map_or_else(
                        || {
                            expected
                                .scheduler
                                .executions
                                .iter()
                                .find(|row| row.id == report.execution)
                                .unwrap()
                                .reservation_target
                        },
                        |trial| {
                            expected
                                .scheduler
                                .reservations
                                .iter()
                                .find(|row| row.id == trial.reservation)
                                .unwrap()
                                .charged
                        },
                    );
                    assert_eq!(*target, expected_target);
                }
                (
                    c::ReportDisposition::Terminal(value),
                    r::Disposition::TerminalAccepted(acceptance),
                ) => assert_eq!(value, &acceptance.terminal),
                _ => panic!("disposition mismatch"),
            }
        }
        let accepted = self.actual.terminal_acceptances().unwrap();
        assert_eq!(accepted.len(), expected.accepted.len());
        for (execution, reference) in &expected.accepted {
            let actual = &accepted[execution];
            assert_eq!(actual.report_id, reference.report);
            assert_eq!(actual.terminal, reference.terminal);
            assert_eq!(actual.at, reference.origin.now);
            assert_eq!(actual.origin.epoch, reference.origin.epoch);
            assert_eq!(actual.origin.execution, reference.origin.execution);
            assert_eq!(actual.origin.attempt, reference.origin.identity.attempt);
            assert_eq!(actual.origin.worker, reference.origin.identity.worker);
            assert_eq!(
                actual.origin.incarnation,
                reference.origin.identity.incarnation
            );
            assert_eq!(
                self.native_candidate_positions[&actual.position],
                self.native_reference_positions[&reference.origin.position]
            );
        }
    }
}
