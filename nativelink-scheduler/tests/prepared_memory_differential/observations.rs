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

//! Read each candidate ledger into the frozen neutral vocabulary.
use super::*;
use crate::protocol as p;

const fn key(value: c::Key) -> h::Key {
    h::Key {
        namespace: value.namespace,
        incarnation: value.incarnation,
        generation: value.generation,
    }
}
fn object(value: c::Object) -> h::Object {
    h::Object {
        algorithm: match value.algorithm {
            1 => h::Algorithm::Sha256,
            9 => h::Algorithm::Blake3,
            _ => panic!("unexpected digest family"),
        },
        hash: value.hash,
        bytes: value.bytes,
    }
}
fn member(value: &c::Member) -> h::Member {
    h::Member {
        key: key(value.key),
        object: object(value.object),
        role: match value.role {
            c::Role::Result => h::Role::Result,
            c::Role::Blob => h::Role::Blob,
            c::Role::Directory => h::Role::Directory,
            c::Role::Tree => h::Role::Tree,
        },
    }
}
const fn kind(value: c::TerminalReportKind) -> r::Classification {
    match value {
        c::TerminalReportKind::Complete => r::Classification::Complete,
        c::TerminalReportKind::MemoryFailure {
            charged,
            peak_memory_bytes,
            quiescent,
        } => r::Classification::MemoryFailure {
            original_charged: charged,
            peak_memory_bytes,
            quiescent,
        },
    }
}
impl Pair {
    fn candidate_position(&self, value: u64) -> u64 {
        self.actual_positions[&value].try_into().unwrap()
    }
    fn reference_position(&self, value: u64) -> u64 {
        self.model_positions[&value].try_into().unwrap()
    }
    fn memory_report(stage: &c::Stage) -> r::Report {
        let memory = stage.memory.as_ref().unwrap();
        let c::Publisher::Worker {
            attempt,
            worker,
            incarnation,
            ..
        } = stage.candidate.publisher
        else {
            panic!("worker required")
        };
        r::Report {
            id: memory.report.id,
            identity: r::WorkerIdentity {
                attempt,
                worker,
                incarnation,
            },
            payload: stage.candidate.response.clone(),
            classification: r::Classification::MemoryFailure {
                original_charged: memory.report.charged,
                peak_memory_bytes: memory.report.peak_memory_bytes,
                quiescent: memory.report.quiescent,
            },
        }
    }
    fn decision(stage: &c::Stage) -> r::Disposition {
        match stage.memory.as_ref().unwrap().decision.unwrap().disposition {
            c::PreparedMemoryDisposition::Retry { target } => {
                r::Disposition::RetryScheduled { target }
            }
            c::PreparedMemoryDisposition::Terminal { reason } => r::Disposition::TerminalAccepted {
                terminal: a::TerminalResult::ResourceFailed {
                    result: stage.candidate.response.clone(),
                    reason,
                },
            },
        }
    }
    pub(super) fn compare(&self) {
        let expected = self.model.view();
        let scheduler = self.view();
        assert_eq!(
            scheduler, expected.scheduler,
            "full scheduler: {:?}",
            self.history
        );
        assert_eq!(self.originals, expected.originals);
        assert!(self.actual.heads().is_empty() && expected.heads.is_empty());
        assert!(self.actual.grants().is_empty());
        assert_eq!(
            self.actual.catalogue_revision(),
            self.facts.catalogue_revision
        );
        let wire: Value = serde_json::from_slice(&self.actual.snapshot().unwrap()).unwrap();
        let bytes: Vec<u8> = serde_json::from_value(wire["scheduler"].clone()).unwrap();
        let child = a::State::restore(&bytes).unwrap();
        for (operation, request) in &self.originals {
            assert_eq!(child.operation_request(*operation), Some(request));
        }
        assert_eq!(expected.actions.len(), scheduler.executions.len());
        assert!(scheduler.executions.len() <= 1);
        let mut credits = BTreeMap::new();
        for execution in &scheduler.executions {
            assert_eq!(execution.id, a::ExecutionId(1));
            let actual = c::ActionBinding::capture(&f::context(), &execution.request).unwrap();
            let qualified = &expected.actions[&execution.id];
            assert_eq!(qualified.key, actual.action);
            assert_eq!(qualified.semantics, actual.platform);
            assert_eq!(qualified.context.principal, actual.context.principal);
            assert_eq!(
                qualified.context.execution_config,
                actual.context.execution_config
            );
            let credit = &wire["metadata"]["credits"][execution.id.0.to_string()];
            let closed = credit["closed"].as_bool().unwrap_or(false);
            assert_eq!(closed, execution.terminal.is_some());
            assert_eq!(
                credit["physical_remaining"].as_u64(),
                Some(if closed { 0 } else { 8 })
            );
            let latest_attempt = scheduler
                .attempts
                .iter()
                .filter(|row| row.execution == execution.id)
                .map(|row| row.id)
                .max();
            let current = expected
                .stages
                .iter()
                .find(|(_, row)| Some(row.report.identity.attempt) == latest_attempt)
                .map(|(id, _)| *id);
            assert_eq!(credit["stage"].as_u64(), current);
            let prior: Vec<_> = expected
                .stages
                .iter()
                .filter(|(_, row)| Some(row.report.identity.attempt) < latest_attempt)
                .map(|(id, _)| *id)
                .collect();
            assert_eq!(
                serde_json::from_value::<Vec<u64>>(credit["prior_stages"].clone()).unwrap(),
                prior
            );
            credits.insert(
                execution.id,
                r::Credit {
                    pending: !closed && credit["stage"].is_null(),
                    attempt: scheduler
                        .attempts
                        .iter()
                        .filter(|row| row.execution == execution.id)
                        .map(|row| row.id)
                        .max(),
                },
            );
        }
        assert_eq!(
            wire["metadata"]["credits"].as_object().unwrap().len(),
            credits.len()
        );
        assert_eq!(credits, expected.credits);
        let mut stages = BTreeMap::new();
        let mut classified = BTreeMap::new();
        for (id, stage) in self.actual.stages() {
            let scenario = &self.scenario.candidates[&self.staged[id]];
            assert_eq!(stage.candidate.action, f::LABEL);
            assert_eq!(object(stage.candidate.result), scenario.result);
            assert_eq!(stage.candidate.result_bytes, scenario.bytes);
            assert_eq!(
                stage
                    .candidate
                    .response
                    .require_response()
                    .unwrap()
                    .encoded(),
                scenario.response
            );
            assert_eq!(
                stage
                    .candidate
                    .closure
                    .iter()
                    .map(member)
                    .collect::<BTreeSet<_>>(),
                scenario.required
            );
            let h::Source::Worker { execution, attempt } = scenario.source else {
                panic!("scenario worker")
            };
            assert_eq!(
                stage.candidate.publisher,
                c::Publisher::Worker {
                    execution: a::ExecutionId(execution),
                    attempt: a::AttemptId(attempt.id),
                    worker: a::WorkerId(attempt.worker),
                    incarnation: a::IncarnationId(attempt.incarnation)
                }
            );
            let report = Self::memory_report(stage);
            let memory = stage.memory.as_ref().unwrap();
            let preparation = stage.preparation.as_ref().map(|e| h::Preparation {
                id: e.stage,
                stage: e.stage,
                result: object(stage.candidate.result),
                typed: e.closure.iter().map(member).collect(),
                protected: e.closure.iter().map(|m| key(m.key)).collect(),
                catalogue_revision: e.catalogue_revision,
                fences: e
                    .fences
                    .iter()
                    .map(|(handle, revision)| {
                        assert_eq!(*handle, 1);
                        h::Fence {
                            namespace: f::NAMESPACE,
                            revision: *revision,
                        }
                    })
                    .collect(),
                complete: true,
                verified: true,
                until: e.until,
            });
            stages.insert(
                *id,
                r::Stage {
                    candidate: self.staged[id],
                    report: report.clone(),
                    epoch: memory.report.epoch,
                    until: stage.until,
                    writes: stage.writes.iter().map(|(id, k)| (*id, key(*k))).collect(),
                    outcomes: stage.outcomes.clone(),
                    preparation,
                    abandoned: stage.state == c::StageState::Abandoned,
                    classified: memory.decision.is_some(),
                },
            );
            if let Some(decision) = memory.decision {
                assert!(
                    classified
                        .insert(
                            report.id,
                            r::Classified {
                                report,
                                execution: a::ExecutionId(execution),
                                position: self.candidate_position(decision.position),
                                now: decision.at,
                                arguments: Some(r::AcceptanceArguments {
                                    stage: *id,
                                    publication: decision.publication,
                                    until: decision.until
                                }),
                                disposition: Self::decision(stage)
                            }
                        )
                        .is_none()
                );
            }
        }
        assert_eq!(
            stages, expected.stages,
            "all retained stage fields: {:?}",
            self.history
        );
        for (id, row) in self.actual.terminal_reports().unwrap() {
            assert_eq!(row.report.origin.epoch, a::ClusterEpoch(1));
            assert_eq!(row.report.origin.action, f::LABEL);
            let origin = &row.report.origin;
            let disposition = match &row.disposition {
                c::ReportDisposition::Retry { target } => {
                    r::Disposition::RetryScheduled { target: *target }
                }
                c::ReportDisposition::Terminal(terminal) => r::Disposition::TerminalAccepted {
                    terminal: terminal.clone(),
                },
            };
            assert!(
                classified
                    .insert(
                        *id,
                        r::Classified {
                            report: r::Report {
                                id: *id,
                                identity: r::WorkerIdentity {
                                    attempt: origin.attempt,
                                    worker: origin.worker,
                                    incarnation: origin.incarnation
                                },
                                classification: kind(row.report.kind),
                                payload: row.report.result.clone()
                            },
                            execution: origin.execution,
                            position: self.candidate_position(row.position),
                            now: row.at,
                            arguments: None,
                            disposition
                        }
                    )
                    .is_none()
            );
        }
        let normalized: BTreeMap<_, _> = expected
            .classified
            .iter()
            .map(|(id, row)| {
                let mut row = row.clone();
                row.position = self.reference_position(row.position);
                (*id, row)
            })
            .collect();
        assert_eq!(
            classified, normalized,
            "all reports, exact payloads, arguments, targets, provenance: {:?}",
            self.history
        );
        let mut accepted = BTreeMap::new();
        for (publication, row) in self.actual.accepted() {
            let stage = &self.actual.stages()[&row.stage];
            let report = Self::memory_report(stage);
            let decision = stage.memory.as_ref().unwrap().decision.unwrap();
            assert_eq!(stage.state, c::StageState::Accepted(*publication));
            assert!(!row.eligible);
            assert_eq!(row.commit, decision.position);
            assert_eq!(row.at, decision.at);
            assert_eq!(row.until, decision.until);
            let r::Disposition::TerminalAccepted { terminal } = Self::decision(stage) else {
                panic!("retry acquired publication")
            };
            let execution = classified[&report.id].execution;
            assert!(
                accepted
                    .insert(
                        execution,
                        r::Accepted {
                            report: report.id,
                            execution,
                            position: self.candidate_position(row.commit),
                            now: row.at,
                            terminal,
                            publication: Some(*publication)
                        }
                    )
                    .is_none()
            );
        }
        for (execution, row) in self.actual.terminal_acceptances().unwrap() {
            assert!(
                accepted
                    .insert(
                        *execution,
                        r::Accepted {
                            report: row.report_id,
                            execution: *execution,
                            position: self.candidate_position(row.position),
                            now: row.at,
                            terminal: row.terminal.clone(),
                            publication: None
                        }
                    )
                    .is_none()
            );
        }
        let normalized: BTreeMap<_, _> = expected
            .accepted
            .iter()
            .map(|(id, row)| {
                let mut row = row.clone();
                row.position = self.reference_position(row.position);
                (*id, row)
            })
            .collect();
        assert_eq!(accepted, normalized);
        let mut operations = BTreeMap::new();
        for (id, operation) in self.actual.operations() {
            assert_eq!(operation.until, r::OPERATION_UNTIL);
            assert!(!operation.cached);
            assert_eq!(operation.action, f::LABEL);
            if let Some(publication) = operation.publication {
                let row = &self.actual.accepted()[&publication];
                let stage = &self.actual.stages()[&row.stage];
                let report = Self::memory_report(stage);
                assert!(operation.root.is_some());
                assert_eq!(operation.payload.as_ref(), Some(&report.payload));
                operations.insert(a::OperationId(*id), report.id);
            } else {
                assert!(operation.root.is_none() && operation.payload.is_none());
                if let Some(execution) = self.actual.terminal_operation_links().unwrap().get(id) {
                    operations.insert(
                        a::OperationId(*id),
                        self.actual.terminal_acceptances().unwrap()[execution].report_id,
                    );
                }
            }
        }
        assert_eq!(operations, expected.operations);
        self.compare_retention(&expected);
    }
    fn compare_retention(&self, expected: &r::View) {
        let retention = self.actual.retention_view();
        let now = self.view().now;
        assert_eq!(retention.now, now);
        assert!(retention.deletes.is_empty());
        assert_eq!(retention.cohorts.len(), 1);
        let cohort = &retention.cohorts[&p::CohortId(1)];
        assert_eq!(cohort.phase, p::Phase::Allocating);
        assert_eq!(cohort.fence, 0);
        assert!(cohort.captured.is_empty() && cohort.marked.is_empty());
        let translate = |k: p::Key| {
            assert_eq!(k.incarnation, 1);
            h::Key {
                namespace: f::NAMESPACE,
                incarnation: f::INCARNATION,
                generation: k.generation,
            }
        };
        let mut roots = BTreeMap::new();
        let mut writes = BTreeSet::new();
        let wire: Value = serde_json::from_slice(&self.actual.snapshot().unwrap()).unwrap();
        let physical = wire["metadata"]["physical"].as_array().unwrap();
        assert_eq!(physical.len(), self.facts.physical.len());
        assert_eq!(retention.generations.len(), physical.len());
        for (row, fact) in physical.iter().zip(&self.facts.physical) {
            let actual_key: c::Key = serde_json::from_value(row["key"].clone()).unwrap();
            let actual_object: c::Object = serde_json::from_value(row["object"].clone()).unwrap();
            assert_eq!(key(actual_key), fact.key);
            assert_eq!(object(actual_object), fact.object);
            let bootstrap: (u64, u64) = serde_json::from_value(row["bootstrap"].clone()).unwrap();
            let root = &retention.roots[&p::RootId(bootstrap.0)];
            assert_eq!(root.until, 0);
            assert_eq!(root.lease, 1);
            assert_eq!(root.kind, p::RootKind::Staging);
            assert_eq!(
                root.closure
                    .iter()
                    .copied()
                    .map(translate)
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([fact.key])
            );
            assert!(roots.insert(bootstrap.0, None).is_none());
            assert!(writes.insert(bootstrap.1));
            let receipt = &retention.writes[&p::WriteId(bootstrap.1)];
            assert_eq!(translate(receipt.key), fact.key);
            assert_eq!(receipt.outcome, Some(true));
            let generation = &retention.generations[&receipt.key];
            assert_eq!(generation.ready, fact.ready);
            assert_eq!(generation.retired, fact.retired);
            assert_eq!(
                generation.write_revision,
                1 + u64::try_from(
                    expected
                        .stages
                        .values()
                        .flat_map(|stage| {
                            stage.outcomes.iter().filter(|(write, success)| {
                                **success && stage.writes[write] == fact.key
                            })
                        })
                        .count()
                )
                .unwrap()
            );
            assert!(
                generation.current_delete.is_none()
                    && generation.delete_confirmed_revision.is_none()
            );
        }
        for (id, stage) in self.actual.stages() {
            assert!(
                roots
                    .insert(stage.root, Some(h::Root::Stage(*id)))
                    .is_none()
            );
            let root = &retention.roots[&p::RootId(stage.root)];
            assert_eq!(root.kind, p::RootKind::Staging);
            assert_eq!(root.lease, 1);
            assert_eq!(
                root.closure
                    .iter()
                    .copied()
                    .map(translate)
                    .collect::<BTreeSet<_>>(),
                self.scenario.candidates[&expected.stages[id].candidate]
                    .required
                    .iter()
                    .map(|m| m.key)
                    .collect()
            );
            for (external, internal) in &stage.internal_writes {
                assert!(writes.insert(*internal));
                let receipt = &retention.writes[&p::WriteId(*internal)];
                assert_eq!(translate(receipt.key), expected.stages[id].writes[external]);
                assert_eq!(
                    receipt.outcome,
                    expected.stages[id].outcomes.get(external).copied()
                );
            }
            assert_eq!(stage.internal_writes.len(), stage.writes.len());
        }
        for (id, accepted) in self.actual.accepted() {
            assert!(
                roots
                    .insert(accepted.root, Some(h::Root::Result(*id)))
                    .is_none()
            );
            assert_eq!(
                retention.roots[&p::RootId(accepted.root)].kind,
                p::RootKind::Publication
            );
        }
        for (id, operation) in self.actual.operations() {
            if let Some(root) = operation.root {
                assert!(roots.insert(root, Some(h::Root::Operation(*id))).is_none());
                assert_eq!(
                    retention.roots[&p::RootId(root)].kind,
                    p::RootKind::Operation
                );
            }
        }
        assert_eq!(roots.len(), retention.roots.len());
        assert_eq!(writes.len(), retention.writes.len());
        let actual: BTreeMap<_, _> = retention
            .roots
            .iter()
            .filter(|(_, root)| root.until > now)
            .map(|(id, root)| {
                let owner = roots[&id.0].expect("unexpected live bootstrap root");
                (
                    owner,
                    (
                        root.closure
                            .iter()
                            .copied()
                            .map(translate)
                            .collect::<BTreeSet<_>>(),
                        root.until,
                    ),
                )
            })
            .collect();
        let reference: BTreeMap<_, _> = expected
            .roots
            .iter()
            .filter(|root| root.until > now)
            .map(|root| (root.root, (root.keys.clone(), root.until)))
            .collect();
        assert_eq!(
            actual.len(),
            retention
                .roots
                .values()
                .filter(|root| root.until > now)
                .count()
        );
        assert_eq!(
            reference.len(),
            expected
                .roots
                .iter()
                .filter(|root| root.until > now)
                .count()
        );
        assert_eq!(
            actual, reference,
            "all live root coverage: {:?}",
            self.history
        );
        let protected: BTreeSet<_> = actual
            .values()
            .flat_map(|(keys, _)| keys.iter().copied())
            .collect();
        assert_eq!(
            self.actual
                .protected_keys()
                .into_iter()
                .map(key)
                .collect::<BTreeSet<_>>(),
            protected
        );
    }
}
