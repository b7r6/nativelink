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

//! Bounded command adapter; fixtures own graphs, facts and command identities.
use std::collections::{BTreeMap, BTreeSet};

use nativelink_scheduler::authority as a;
use serde_json::Value;

use super::head::types as h;
use super::publication_candidate::engine::PublicationAuthority;
use super::publication_candidate::types as c;
use super::reference::Model;
use super::{fixtures as f, types as r};
#[path = "observations.rs"]
mod observations;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Applied,
    Scheduler(a::Reply),
    Duplicate,
    Rejected,
    Classified(r::Disposition),
}
pub(super) struct Pair {
    pub actual: PublicationAuthority,
    pub model: Model,
    pub algorithm: u8,
    pub memory_kb: u64,
    pub scenario: h::Scenario,
    pub payloads: BTreeMap<u64, a::ResultPayload>,
    pub facts: h::Facts,
    pub history: Vec<r::Step>,
    originals: BTreeMap<a::OperationId, a::Request>,
    staged: BTreeMap<u64, u64>,
    actual_positions: BTreeMap<u64, usize>,
    model_positions: BTreeMap<u64, usize>,
}
impl Pair {
    pub(super) fn new(algorithm: u8, memory_kb: u64) -> Self {
        let (scenario, payloads, facts) = f::scenario(algorithm, memory_kb);
        let request = f::request(algorithm, memory_kb, 0, f::LABEL);
        let mut actual = PublicationAuthority::new_real(
            f::context(),
            BTreeMap::from([(
                f::LABEL.into(),
                c::ActionBinding::capture(&f::context(), &request).unwrap(),
            )]),
        )
        .unwrap();
        for physical in &facts.physical {
            actual
                .install_physical(
                    f::key(physical.key),
                    f::object(&physical.object),
                    physical.ready,
                )
                .unwrap();
        }
        actual.enable_prepared_memory().unwrap();
        assert_eq!(actual.catalogue_revision(), facts.catalogue_revision);
        let model = Model::new(f::oracle_context(), scenario.clone(), vec![]).unwrap();
        let mut pair = Self {
            actual_positions: BTreeMap::from([(actual.commit(), 0)]),
            model_positions: BTreeMap::from([(0, 0)]),
            actual,
            model,
            algorithm,
            memory_kb,
            scenario,
            payloads,
            facts,
            history: vec![],
            originals: BTreeMap::new(),
            staged: BTreeMap::new(),
        };
        pair.check_and_restore();
        pair
    }
    pub(super) fn view(&self) -> a::ObservableState {
        self.actual.scheduler_view()
    }
    pub(super) fn event(&mut self, event: a::Event) -> Outcome {
        self.step(r::Command::Scheduler(event))
    }
    pub(super) fn admit(&mut self, id: u64, caller: i32) {
        assert_eq!(
            self.step(r::Command::Admit {
                operation: a::OperationId(id),
                request: f::request(self.algorithm, self.memory_kb, caller, f::LABEL)
            }),
            Outcome::Applied
        );
    }
    pub(super) fn report(&self, attempt: u64, graph: bool, peak: Option<u64>) -> r::Report {
        let view = self.model.view().scheduler;
        let trial = view
            .attempts
            .iter()
            .find(|row| row.id.0 == attempt)
            .unwrap();
        let charged = view
            .reservations
            .iter()
            .find(|row| row.id == trial.reservation)
            .unwrap()
            .charged;
        r::Report {
            id: 100 + attempt,
            identity: r::WorkerIdentity {
                attempt: trial.id,
                worker: trial.worker,
                incarnation: trial.incarnation,
            },
            classification: r::Classification::MemoryFailure {
                original_charged: charged,
                peak_memory_bytes: peak,
                quiescent: true,
            },
            payload: if graph {
                self.payloads[&attempt].clone()
            } else {
                f::response("no-graph original failure", false)
            },
        }
    }
    pub(super) fn stage_command(&self, attempt: u64, peak: Option<u64>) -> r::Command {
        r::Command::Stage {
            id: 300 + attempt,
            candidate: 200 + attempt,
            report: self.report(attempt, true, peak),
            epoch: a::ClusterEpoch(1),
            until: self.model.view().scheduler.now + 10,
            writes: self.scenario.candidates[&(200 + attempt)]
                .required
                .iter()
                .enumerate()
                .map(|(i, m)| (u64::try_from(i).unwrap() + 1, m.key))
                .collect(),
        }
    }
    pub(super) fn proof(&self, stage: u64, until: u64) -> h::Preparation {
        let graph = &self.scenario.candidates[&self.staged[&stage]];
        h::Preparation {
            id: stage,
            stage,
            result: graph.result.clone(),
            typed: graph.required.clone(),
            protected: graph.required.iter().map(|m| m.key).collect(),
            catalogue_revision: self.facts.catalogue_revision,
            fences: self.facts.fences.clone(),
            complete: true,
            verified: true,
            until,
        }
    }
    pub(super) fn prepare(&mut self, stage: u64, until: u64) -> Outcome {
        self.step(r::Command::Prepare {
            stage,
            evidence: self.proof(stage, until),
        })
    }
    pub(super) fn accept(&mut self, stage: u64, publication: u64, until: u64) -> Outcome {
        self.step(r::Command::Accept(r::AcceptanceArguments {
            stage,
            publication,
            until,
        }))
    }
    pub(super) fn receipt(&self, stage: u64, write: u64, success: bool) -> h::Receipt {
        let row = &self.model.view().stages[&stage];
        let key = row.writes[&write];
        let object = self.scenario.candidates[&row.candidate]
            .required
            .iter()
            .find(|m| m.key == key)
            .unwrap()
            .object
            .clone();
        h::Receipt {
            stage,
            write,
            key,
            object,
            success,
        }
    }
    fn candidate_evidence(&self, e: &h::Preparation) -> c::Evidence {
        // This common alphabet fixes neutral proof-only fields; it never emulates
        // a candidate capability that does not exist.
        assert_eq!(e.id, e.stage);
        assert_eq!(
            e.result,
            self.scenario.candidates[&self.staged[&e.stage]].result
        );
        assert!(e.complete && e.verified);
        assert_eq!(e.protected, e.typed.iter().map(|m| m.key).collect());
        c::Evidence {
            stage: e.stage,
            catalogue_revision: e.catalogue_revision,
            closure: e.typed.iter().map(f::member).collect(),
            fences: e
                .fences
                .iter()
                .map(|fence| {
                    (
                        if fence.namespace == f::NAMESPACE {
                            1
                        } else {
                            u64::MAX
                        },
                        fence.revision,
                    )
                })
                .collect(),
            until: e.until,
        }
    }
    fn translate_outcome(&self, reply: c::NoGraphOutcome, report: &r::Report) -> Outcome {
        match reply {
            c::NoGraphOutcome::Duplicate => Outcome::Duplicate,
            c::NoGraphOutcome::TerminalAccepted(terminal) => {
                Outcome::Classified(r::Disposition::TerminalAccepted { terminal })
            }
            c::NoGraphOutcome::RetryScheduled => {
                let view = self.view();
                let execution = view
                    .attempts
                    .iter()
                    .find(|row| row.id == report.identity.attempt)
                    .unwrap()
                    .execution;
                let target = view
                    .executions
                    .iter()
                    .find(|row| row.id == execution)
                    .unwrap()
                    .reservation_target;
                Outcome::Classified(r::Disposition::RetryScheduled { target })
            }
        }
    }
    fn apply_candidate(&mut self, command: &r::Command) -> Result<Outcome, c::Error> {
        match command {
            r::Command::Admit { operation, request } => self
                .actual
                .admit_request(
                    operation.0,
                    f::LABEL.into(),
                    &f::context(),
                    request.clone(),
                    r::OPERATION_UNTIL,
                )
                .map(|_| Outcome::Applied),
            r::Command::Scheduler(a::Event::AdvanceTime { now }) => {
                let changed = *now != self.view().now;
                self.actual.advance(*now).map(|()| {
                    Outcome::Scheduler(if changed {
                        a::Reply::Applied
                    } else {
                        a::Reply::Ignored
                    })
                })
            }
            r::Command::Scheduler(event) => self
                .actual
                .scheduler_event(event.clone())
                .map(Outcome::Scheduler),
            r::Command::Stage {
                id,
                candidate,
                report,
                epoch,
                until,
                writes,
            } => {
                let graph = &self.scenario.candidates[candidate];
                let h::Source::Worker { execution, attempt } = graph.source else {
                    unreachable!()
                };
                assert_eq!(execution, 1);
                assert_eq!(report.identity.attempt.0, attempt.id);
                assert_eq!(report.identity.worker.0, attempt.worker);
                assert_eq!(
                    report.payload.require_response().unwrap().encoded(),
                    graph.response
                );
                let r::Classification::MemoryFailure {
                    original_charged,
                    peak_memory_bytes,
                    quiescent,
                } = report.classification
                else {
                    panic!("classification outside common Stage alphabet")
                };
                self.actual
                    .stage_memory(
                        *id,
                        c::Candidate {
                            action: f::LABEL.into(),
                            publisher: c::Publisher::Worker {
                                execution: a::ExecutionId(1),
                                attempt: report.identity.attempt,
                                worker: report.identity.worker,
                                incarnation: report.identity.incarnation,
                            },
                            result: f::object(&graph.result),
                            result_bytes: graph.bytes.clone(),
                            response: report.payload.clone(),
                            closure: graph.required.iter().map(f::member).collect(),
                        },
                        c::PreparedMemoryReport {
                            id: report.id,
                            epoch: *epoch,
                            charged: original_charged,
                            peak_memory_bytes,
                            quiescent,
                        },
                        *until,
                        writes.iter().map(|(id, key)| (*id, f::key(*key))).collect(),
                    )
                    .map(|()| Outcome::Applied)
            }
            r::Command::Observe(receipt) => {
                let duplicate = self
                    .actual
                    .stages()
                    .get(&receipt.stage)
                    .is_some_and(|stage| {
                        stage.outcomes.get(&receipt.write) == Some(&receipt.success)
                    });
                self.actual
                    .observe(
                        receipt.stage,
                        receipt.write,
                        f::key(receipt.key),
                        f::object(&receipt.object),
                        receipt.success,
                    )
                    .map(|()| {
                        if duplicate {
                            Outcome::Duplicate
                        } else {
                            Outcome::Applied
                        }
                    })
            }
            r::Command::Prepare { stage, evidence } => {
                assert_eq!(evidence.stage, *stage);
                let translated = self.candidate_evidence(evidence);
                let duplicate = self
                    .actual
                    .stages()
                    .get(stage)
                    .is_some_and(|row| row.preparation.as_ref() == Some(&translated));
                self.actual.prepare(*stage, translated).map(|()| {
                    if duplicate {
                        Outcome::Duplicate
                    } else {
                        Outcome::Applied
                    }
                })
            }
            r::Command::Accept(arguments) => {
                let report = self
                    .actual
                    .stages()
                    .get(&arguments.stage)
                    .and_then(|stage| {
                        stage.memory.as_ref().map(|memory| r::Report {
                            id: memory.report.id,
                            identity: match stage.candidate.publisher {
                                c::Publisher::Worker {
                                    attempt,
                                    worker,
                                    incarnation,
                                    ..
                                } => r::WorkerIdentity {
                                    attempt,
                                    worker,
                                    incarnation,
                                },
                                _ => unreachable!(),
                            },
                            classification: r::Classification::MemoryFailure {
                                original_charged: memory.report.charged,
                                peak_memory_bytes: memory.report.peak_memory_bytes,
                                quiescent: memory.report.quiescent,
                            },
                            payload: stage.candidate.response.clone(),
                        })
                    });
                let reply = self.actual.accept_prepared_memory(
                    arguments.stage,
                    arguments.publication,
                    arguments.until,
                )?;
                Ok(self.translate_outcome(reply, &report.unwrap()))
            }
            r::Command::NoGraph(report) => {
                let reply = self.actual.report_no_graph(
                    report.id,
                    c::NoGraphReport {
                        origin: c::WorkerOrigin {
                            epoch: a::ClusterEpoch(1),
                            action: f::LABEL.into(),
                            execution: a::ExecutionId(1),
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
                )?;
                Ok(self.translate_outcome(reply, report))
            }
            r::Command::Abandon(stage) => self.actual.abandon(*stage).map(|()| Outcome::Applied),
        }
    }
    #[allow(
        clippy::needless_pass_by_value,
        reason = "The test command interface owns each replay input."
    )]
    pub(super) fn step(&mut self, command: r::Command) -> Outcome {
        let before_actual = self.actual.snapshot().unwrap();
        let before_model = self.model.snapshot().unwrap();
        let mut observed = self.facts.clone();
        if let r::Command::Observe(receipt) = &command {
            observed.receipts.insert(receipt.clone());
        }
        self.history.push(r::Step {
            command: command.clone(),
            facts: observed.clone(),
        });
        let actual_native = self.apply_candidate(&command);
        let reference_native = self.model.apply(&command, &observed);
        let actual = actual_native.clone().unwrap_or(Outcome::Rejected);
        let expected = match &reference_native {
            Err(_) => Outcome::Rejected,
            Ok(r::Reply::Applied) => Outcome::Applied,
            Ok(r::Reply::Duplicate) => Outcome::Duplicate,
            Ok(r::Reply::Scheduler(reply)) => {
                if matches!(command, r::Command::Admit { .. }) {
                    Outcome::Applied
                } else {
                    Outcome::Scheduler(reply.clone())
                }
            }
            Ok(r::Reply::Classified(value)) => Outcome::Classified(value.clone()),
        };
        assert_eq!(
            actual, expected,
            "native candidate={actual_native:?}, reference={reference_native:?}; history={:#?}",
            self.history
        );
        if matches!(
            actual,
            Outcome::Rejected
                | Outcome::Duplicate
                | Outcome::Scheduler(a::Reply::Ignored | a::Reply::Observed(_))
        ) {
            assert_eq!(
                self.actual.snapshot().unwrap(),
                before_actual,
                "candidate atomic refusal/no-op: {:?}",
                self.history
            );
            assert_eq!(
                self.model.snapshot().unwrap(),
                before_model,
                "reference atomic refusal/no-op: {:?}",
                self.history
            );
        }
        if actual == Outcome::Applied {
            match &command {
                r::Command::Admit { operation, request } => {
                    self.originals.insert(*operation, request.clone());
                }
                r::Command::Stage { id, candidate, .. } => {
                    self.staged.insert(*id, *candidate);
                }
                r::Command::Observe(receipt) => {
                    self.facts = observed;
                    self.facts.catalogue_revision += 1;
                    if receipt.success {
                        self.facts
                            .physical
                            .iter_mut()
                            .find(|p| p.key == receipt.key)
                            .unwrap()
                            .ready = true;
                    }
                }
                _ => {}
            }
        }
        if let Outcome::Classified(_) = &actual {
            let id = match &command {
                r::Command::Accept(args) => self.model.view().stages[&args.stage].report.id,
                r::Command::NoGraph(report) => report.id,
                _ => unreachable!(),
            };
            let expected = self.model.view();
            assert_eq!(expected.classified[&id].position, expected.position);
            let position = if let r::Command::Accept(args) = &command {
                self.actual.stages()[&args.stage]
                    .memory
                    .as_ref()
                    .unwrap()
                    .decision
                    .unwrap()
                    .position
            } else {
                self.actual.terminal_reports().unwrap()[&id].position
            };
            assert_eq!(position, self.actual.commit());
        }
        self.actual_positions
            .entry(self.actual.commit())
            .or_insert(self.history.len());
        self.model_positions
            .entry(self.model.view().position)
            .or_insert(self.history.len());
        self.check_and_restore();
        actual
    }
    fn check_and_restore(&mut self) {
        self.compare();
        let actual = self.actual.snapshot().unwrap();
        let model = self.model.snapshot().unwrap();
        self.actual = PublicationAuthority::restore(&actual).unwrap();
        self.model = Model::restore(&model).unwrap();
        assert_eq!(self.actual.snapshot().unwrap(), actual);
        assert_eq!(self.model.snapshot().unwrap(), model);
        self.actual.lose_index();
        self.actual.rebuild_index();
        self.compare();
    }
}
