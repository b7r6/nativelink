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

use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command as ReapiCommand, Digest as ReapiDigest, ExecuteRequest,
    ExecuteResponse, ExecutionPolicy, Platform, platform,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority as a;
use prost::Message;
use prost_types::{Duration, Timestamp};
use sha2::{Digest, Sha256};

use super::head::{reference as graph, types as h};
use super::m1;
use super::reference::{Model, binding, response_bytes};
use super::types::*;

fn context() -> Context {
    Context {
        principal: "prepared-principal".into(),
        execution_config: "prepared-config-v1".into(),
    }
}

fn digest(bytes: &[u8], blake3: bool) -> ReapiDigest {
    ReapiDigest {
        hash: if blake3 {
            blake3::hash(bytes).to_hex().to_string()
        } else {
            format!("{:x}", Sha256::digest(bytes))
        },
        size_bytes: bytes.len().try_into().unwrap(),
    }
}

/// Independently assemble the agreed request capture, including its framing
/// hash and protobuf facts. No candidate request capture/project method runs.
fn request(
    label: &str,
    blake3: bool,
    memory_kb: u64,
    priority: i32,
    queued: i64,
    principal: &str,
) -> a::Request {
    let command = ReapiCommand {
        arguments: vec!["/bin/false".into(), label.into()],
        ..ReapiCommand::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(&command, blake3)),
        input_root_digest: Some(digest(&[], blake3)),
        timeout: Some(Duration {
            seconds: 20,
            nanos: 0,
        }),
        platform: Some(Platform {
            properties: vec![
                platform::Property {
                    name: "os".into(),
                    value: "linux".into(),
                },
                platform::Property {
                    name: "cpu_count".into(),
                    value: "1".into(),
                },
                platform::Property {
                    name: "memory_kb".into(),
                    value: memory_kb.to_string(),
                },
            ],
        }),
        ..Action::default()
    }
    .encode_to_vec();
    let execute = ExecuteRequest {
        instance_name: "prepared-memory-fixture".into(),
        action_digest: Some(digest(&action, blake3)),
        digest_function: if blake3 { 9 } else { 1 },
        skip_cache_lookup: true,
        execution_policy: Some(ExecutionPolicy { priority }),
        ..ExecuteRequest::default()
    }
    .encode_to_vec();
    let queued = Timestamp {
        seconds: queued,
        nanos: 0,
    }
    .encode_to_vec();
    let version = 1_u32;
    let mut hash = Sha256::new();
    hash.update(b"nativelink-request-v1");
    hash.update(version.to_be_bytes());
    hash.update([0]);
    for part in [
        execute.as_slice(),
        &[],
        queued.as_slice(),
        principal.as_bytes(),
        action.as_slice(),
        command.as_slice(),
    ] {
        hash.update((part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    let wire = serde_json::json!({ "version": version, "execute": execute, "metadata": null,
        "queued": queued, "principal": principal, "action": action, "command": command,
        "sha256": <[u8; 32]>::from(hash.finalize()) });
    let mut result = m1::Reference::captured_request_facts(wire.clone()).unwrap();
    result.payload = Some(Box::new(serde_json::from_value(wire).unwrap()));
    result
}

fn original(label: &str, memory_kb: u64) -> a::Request {
    request(label, false, memory_kb, 0, 0, &context().principal)
}

/// A captured response is an exact opaque payload plus independently expected
/// pinned-protobuf facts; its diagnostic digest is not an `ActionResult` object.
fn payload(message: &str, result: Option<ActionResult>) -> a::ResultPayload {
    let response = ExecuteResponse {
        result,
        status: Some(Status {
            code: 13,
            message: "worker error retained verbatim".into(),
            details: vec![],
        }),
        message: message.into(),
        ..ExecuteResponse::default()
    };
    let exit_code = response
        .result
        .as_ref()
        .map_or(0, |result| result.exit_code);
    let bytes = response.encode_to_vec();
    let hash = Sha256::digest(&bytes);
    serde_json::from_value(serde_json::json!({
        "digest": format!("{hash:x}"), "exit_code": exit_code,
        "error": "REAPI status 13: worker error retained verbatim",
        "response": { "version": 1, "encoded": bytes, "sha256": <[u8; 32]>::from(hash) },
    }))
    .unwrap()
}

struct Fixture {
    model: Model,
    facts: h::Facts,
    request: a::Request,
    candidates: BTreeMap<u64, h::Candidate>,
    payloads: BTreeMap<u64, a::ResultPayload>,
}

impl Fixture {
    fn new(blake3: bool, memory_kb: u64, capacity: u64) -> Self {
        let request = request(
            "graph-worker",
            blake3,
            memory_kb,
            0,
            0,
            &context().principal,
        );
        let qualified = serde_json::to_string(&binding(&context(), &request).unwrap()).unwrap();
        let algorithm = if blake3 {
            h::Algorithm::Blake3
        } else {
            h::Algorithm::Sha256
        };
        let mut candidates = BTreeMap::new();
        let mut payloads = BTreeMap::new();
        let mut physical = Vec::new();
        for attempt in 1..=3 {
            let id = 200 + attempt;
            let stdout = format!("immutable stdout of attempt {attempt}").into_bytes();
            let result = ActionResult {
                exit_code: 17,
                stdout_digest: Some(digest(&stdout, blake3)),
                ..ActionResult::default()
            };
            let bytes = result.encode_to_vec();
            let captured = payload(&format!("exact memory failure {attempt}"), Some(result));
            let result_object = graph::object(&bytes, algorithm);
            let members = BTreeSet::from([
                h::Member {
                    object: result_object.clone(),
                    key: h::Key {
                        namespace: [1; 16],
                        incarnation: [2; 16],
                        generation: id * 2,
                    },
                    role: h::Role::Result,
                },
                h::Member {
                    object: graph::object(&stdout, algorithm),
                    key: h::Key {
                        namespace: [1; 16],
                        incarnation: [2; 16],
                        generation: id * 2 + 1,
                    },
                    role: h::Role::Blob,
                },
            ]);
            physical.extend(members.iter().map(|member| h::Physical {
                key: member.key,
                object: member.object.clone(),
                ready: true,
                retired: false,
            }));
            candidates.insert(
                id,
                h::Candidate {
                    id,
                    action: qualified.clone(),
                    source: h::Source::Worker {
                        execution: 1,
                        attempt: h::Attempt {
                            id: attempt,
                            worker: 1,
                            incarnation: 1,
                        },
                    },
                    result: result_object,
                    bytes,
                    response: response_bytes(&captured).unwrap(),
                    successful: false,
                    error_free: false,
                    required: members,
                },
            );
            payloads.insert(attempt, captured);
        }
        let scenario = h::Scenario {
            actions: BTreeMap::from([(qualified, false)]),
            candidates: candidates.clone(),
        };
        let seed = h::Coverage {
            root: h::Root::Serving(900),
            keys: BTreeSet::from([h::Key {
                namespace: [9; 16],
                incarnation: [8; 16],
                generation: 7,
            }]),
            until: 100,
        };
        let model = Model::new(context(), scenario, vec![seed]).unwrap();
        let facts = h::Facts {
            catalogue_revision: 1,
            physical,
            fences: BTreeSet::from([h::Fence {
                namespace: [1; 16],
                revision: 1,
            }]),
            receipts: BTreeSet::new(),
        };
        let mut fixture = Self {
            model,
            facts,
            request,
            candidates,
            payloads,
        };
        let resources = resources(capacity);
        fixture.scheduler(a::Event::Register {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            properties: BTreeMap::from([("os".into(), "linux".into())]),
            capacity: resources,
            available: resources,
        });
        fixture.apply(Command::Admit {
            operation: a::OperationId(1),
            request: fixture.request.clone(),
        });
        fixture
    }

    // Fixture commands are written inline and discarded after one transition.
    #[allow(clippy::needless_pass_by_value)]
    fn apply(&mut self, command: Command) -> Reply {
        self.model.apply(&command, &self.facts).unwrap()
    }

    fn scheduler(&mut self, event: a::Event) -> a::Reply {
        let Reply::Scheduler(reply) = self.apply(Command::Scheduler(event)) else {
            panic!("scheduler reply expected")
        };
        reply
    }

    fn assign(&mut self) -> a::AttemptView {
        let a::Reply::Matched { attempt, .. } = self.scheduler(a::Event::Match) else {
            panic!("match expected")
        };
        let trial = self
            .model
            .view()
            .scheduler
            .attempts
            .into_iter()
            .find(|trial| trial.id == attempt)
            .unwrap();
        self.scheduler(a::Event::WorkerAccepted {
            attempt,
            worker: trial.worker,
            incarnation: trial.incarnation,
        });
        trial
    }

    fn report(&self, trial: a::AttemptView, peak: Option<u64>, graph: bool) -> Report {
        let charged = self
            .model
            .view()
            .scheduler
            .reservations
            .into_iter()
            .find(|row| row.id == trial.reservation)
            .unwrap()
            .charged;
        Report {
            id: 100 + trial.id.0,
            identity: WorkerIdentity {
                attempt: trial.id,
                worker: trial.worker,
                incarnation: trial.incarnation,
            },
            classification: Classification::MemoryFailure {
                original_charged: charged,
                peak_memory_bytes: peak,
                quiescent: true,
            },
            payload: if graph {
                self.payloads[&trial.id.0].clone()
            } else {
                payload("no graph memory failure", None)
            },
        }
    }

    fn stage_command(&self, trial: a::AttemptView, peak: Option<u64>) -> Command {
        let id = 300 + trial.id.0;
        let candidate = 200 + trial.id.0;
        let writes = self.candidates[&candidate]
            .required
            .iter()
            .enumerate()
            .map(|(index, member)| (id * 10 + u64::try_from(index).unwrap(), member.key))
            .collect();
        Command::Stage {
            id,
            candidate,
            report: self.report(trial, peak, true),
            epoch: a::ClusterEpoch(1),
            until: self.model.view().scheduler.now + 10,
            writes,
        }
    }

    fn prepare(&mut self, trial: a::AttemptView, peak: Option<u64>) -> u64 {
        let id = 300 + trial.id.0;
        self.apply(self.stage_command(trial, peak));
        self.apply(Command::Prepare {
            stage: id,
            evidence: self.proof(id),
        });
        id
    }

    fn proof(&self, stage: u64) -> h::Preparation {
        let graph = &self.candidates[&self.model.view().stages[&stage].candidate];
        h::Preparation {
            id: stage,
            stage,
            result: graph.result.clone(),
            typed: graph.required.clone(),
            protected: graph.required.iter().map(|member| member.key).collect(),
            catalogue_revision: self.facts.catalogue_revision,
            fences: self.facts.fences.clone(),
            complete: true,
            verified: true,
            until: OPERATION_UNTIL,
        }
    }

    fn accept(&mut self, stage: u64) -> Disposition {
        let Reply::Classified(disposition) = self.apply(Command::Accept(arguments(stage))) else {
            panic!("classification expected")
        };
        disposition
    }

    fn capacity(&mut self, memory: u64) {
        self.scheduler(a::Event::Capacity {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            capacity: resources(memory),
            available: resources(memory),
            draining: false,
        });
    }

    fn reject(&mut self, command: &Command) -> Refusal {
        let before = self.model.snapshot().unwrap();
        let view = self.model.view();
        let rejection = self.model.apply(command, &self.facts).unwrap_err();
        assert_eq!(self.model.snapshot().unwrap(), before);
        assert_eq!(self.model.view(), view);
        rejection
    }
}

const fn resources(memory_bytes: u64) -> a::Resources {
    a::Resources {
        cpu_millis: 8000,
        memory_bytes,
        slots: 8,
    }
}
const fn arguments(stage: u64) -> AcceptanceArguments {
    AcceptanceArguments {
        stage,
        publication: stage + 100,
        until: 80,
    }
}

fn reason(
    disposition: &Disposition,
    expected: a::ResourceFailureReason,
    payload: &a::ResultPayload,
) {
    assert_eq!(
        disposition,
        &Disposition::TerminalAccepted {
            terminal: a::TerminalResult::ResourceFailed {
                result: payload.clone(),
                reason: expected
            }
        }
    );
}

#[test]
fn graph_final_uses_exact_response_live_operations_and_no_ac_for_both_algorithms() {
    for blake3 in [false, true] {
        let mut f = Fixture::new(blake3, 1, 1024);
        f.apply(Command::Admit {
            operation: a::OperationId(2),
            request: f.request.clone(),
        });
        f.scheduler(a::Event::Cancel {
            operation: a::OperationId(1),
        });
        let trial = f.assign();
        let stage = f.prepare(trial, None);
        let seed = f.model.view().roots[0].clone();
        reason(
            &f.accept(stage),
            a::ResourceFailureReason::NoLargerEligibleWorker,
            &f.payloads[&1],
        );
        let view = f.model.view();
        assert!(view.heads.is_empty());
        assert!(
            view.scheduler
                .effects
                .iter()
                .all(|effect| !matches!(effect.kind, a::EffectKind::PublishResult { .. }))
        );
        assert_eq!(view.operations, BTreeMap::from([(a::OperationId(2), 101)]));
        assert_eq!(view.roots.len(), 2);
        assert_eq!(view.roots[0], seed);
        assert_eq!(view.roots[1].root, h::Root::Operation(2));
        assert_eq!(view.roots[1].until, OPERATION_UNTIL);
        assert_eq!(
            view.roots[1].keys,
            f.candidates[&201].required.iter().map(|m| m.key).collect()
        );
        assert_eq!(
            view.scheduler.operations[0].terminal,
            Some(a::TerminalResult::Cancelled)
        );
        assert!(view.scheduler.reservations.iter().all(|row| row.released));
        assert!(!view.credits[&a::ExecutionId(1)].pending);
        assert_eq!(
            Model::restore(&f.model.snapshot().unwrap()).unwrap().view(),
            view
        );
    }
}

#[test]
fn operation_lease_must_be_prepared_before_terminal_charge_is_released() {
    for capacity in [1024, 8192] {
        let mut f = Fixture::new(false, 1, capacity);
        let trial = f.assign();
        let stage = f.prepare(trial, None);
        let mut proof = f.proof(stage);
        proof.until = arguments(stage).until;
        f.apply(Command::Prepare {
            stage,
            evidence: proof,
        });
        if capacity == 1024 {
            f.reject(&Command::Accept(arguments(stage)));
            assert!(
                f.model
                    .view()
                    .scheduler
                    .reservations
                    .iter()
                    .all(|r| !r.released)
            );
            assert!(f.model.view().classified.is_empty());
            f.apply(Command::Prepare {
                stage,
                evidence: f.proof(stage),
            });
            reason(
                &f.accept(stage),
                a::ResourceFailureReason::NoLargerEligibleWorker,
                &f.payloads[&1],
            );
        } else {
            assert!(matches!(
                f.accept(stage),
                Disposition::RetryScheduled { .. }
            ));
            assert!(f.model.view().accepted.is_empty());
        }
    }
}

#[test]
fn preparation_never_caches_capacity_decision_in_either_direction() {
    for (before, after, retry) in [(1024, 8192, true), (8192, 1024, false)] {
        let mut f = Fixture::new(false, 1, before);
        let trial = f.assign();
        let stage = f.prepare(trial, None);
        assert!(f.model.view().accepted.is_empty());
        assert!(!f.model.view().scheduler.reservations[0].released);
        f.capacity(after);
        let disposition = f.accept(stage);
        if retry {
            assert!(
                matches!(disposition, Disposition::RetryScheduled { target } if target.memory_bytes == 2048)
            );
            let view = f.model.view();
            assert!(view.accepted.is_empty() && view.operations.is_empty());
            assert!(
                view.roots
                    .iter()
                    .any(|root| root.root == h::Root::Stage(stage))
            );
            assert_eq!(view.stages[&stage].outcomes.len(), 0);
        } else {
            reason(
                &disposition,
                a::ResourceFailureReason::NoLargerEligibleWorker,
                &f.payloads[&1],
            );
        }
    }
}

#[test]
fn all_memory_terminal_reasons_preserve_original_graph_response() {
    let mut missing = Fixture::new(false, 0, 8192);
    let trial = missing.assign();
    let stage = missing.prepare(trial, None);
    reason(
        &missing.accept(stage),
        a::ResourceFailureReason::MissingGrowthEvidence,
        &missing.payloads[&1],
    );
    let mut budget = Fixture::new(true, 1, 65536);
    for attempt in 1..=3 {
        let trial = budget.assign();
        let stage = budget.prepare(trial, None);
        let disposition = budget.accept(stage);
        if attempt < 3 {
            assert!(matches!(disposition, Disposition::RetryScheduled { .. }));
        } else {
            reason(
                &disposition,
                a::ResourceFailureReason::AttemptBudgetExhausted,
                &budget.payloads[&3],
            );
        }
    }
    assert_eq!(budget.model.view().stages.len(), 3);
    assert_eq!(budget.model.view().classified.len(), 3);
    assert!(budget.model.view().heads.is_empty());
}

#[test]
fn explicit_peak_grows_zero_original_grant_without_rewriting_request() {
    let mut f = Fixture::new(false, 0, 8192);
    let trial = f.assign();
    let stage = f.prepare(trial, Some(1200));
    assert!(
        matches!(f.accept(stage), Disposition::RetryScheduled { target } if target.memory_bytes == 2400)
    );
    assert_eq!(
        f.model.view().originals[&a::OperationId(1)]
            .resources
            .memory_bytes,
        0
    );
    assert_eq!(
        f.model.view().scheduler.reservations[0]
            .charged
            .memory_bytes,
        0
    );
}

#[test]
fn retry_publication_argument_is_not_reserved_and_exact_replay_survives_reuse() {
    let mut f = Fixture::new(false, 1, 8192);
    let first = f.assign();
    let stage = f.prepare(first, None);
    assert!(matches!(
        f.accept(stage),
        Disposition::RetryScheduled { .. }
    ));
    let old = Command::Accept(arguments(stage));
    let second = f.assign();
    let next_stage = f.prepare(second, None);
    f.capacity(2048);
    let reused = AcceptanceArguments {
        stage: next_stage,
        publication: arguments(stage).publication,
        until: 80,
    };
    assert!(matches!(
        f.apply(Command::Accept(reused)),
        Reply::Classified(Disposition::TerminalAccepted { .. })
    ));
    f.scheduler(a::Event::WorkerUnavailable {
        worker: a::WorkerId(1),
        incarnation: a::IncarnationId(1),
        session: a::SessionId(1),
    });
    let before = f.model.snapshot().unwrap();
    assert_eq!(f.apply(old), Reply::Duplicate);
    assert_eq!(f.model.snapshot().unwrap(), before);
    for changed in [
        AcceptanceArguments {
            publication: 999,
            ..arguments(stage)
        },
        AcceptanceArguments {
            until: 79,
            ..arguments(stage)
        },
    ] {
        assert_eq!(f.reject(&Command::Accept(changed)), Refusal::Conflict);
    }
}

#[test]
fn invalid_report_authority_grant_epoch_quiescence_and_exact_response_are_atomic() {
    let mut f = Fixture::new(false, 1, 8192);
    let trial = f.assign();
    let baseline = f.stage_command(trial, None);
    let mut mutations = Vec::new();
    for selector in 0..7 {
        let mut command = baseline.clone();
        let Command::Stage { report, epoch, .. } = &mut command else {
            unreachable!()
        };
        match selector {
            0 => report.id = 0,
            1 => report.identity.incarnation = a::IncarnationId(2),
            2 => *epoch = a::ClusterEpoch(2),
            3 => report.classification = Classification::Complete,
            4 => {
                let Classification::MemoryFailure {
                    original_charged, ..
                } = &mut report.classification
                else {
                    unreachable!()
                };
                original_charged.cpu_millis += 1;
            }
            5 => {
                let Classification::MemoryFailure { quiescent, .. } = &mut report.classification
                else {
                    unreachable!()
                };
                *quiescent = false;
            }
            6 => report.payload = f.payloads[&2].clone(),
            _ => unreachable!(),
        }
        mutations.push(command);
    }
    for command in mutations {
        f.reject(&command);
    }
    assert_eq!(f.apply(baseline), Reply::Applied);
}

#[test]
fn shared_ids_and_attempts_conflict_across_graph_and_no_graph_paths() {
    let mut f = Fixture::new(false, 1, 8192);
    let trial = f.assign();
    let staged = f.stage_command(trial, None);
    f.apply(staged.clone());
    f.reject(&staged);
    let mut no_graph = f.report(trial, None, false);
    assert_eq!(
        f.reject(&Command::NoGraph(no_graph.clone())),
        Refusal::Conflict
    );
    no_graph.id = 999;
    assert_eq!(f.reject(&Command::NoGraph(no_graph)), Refusal::Conflict);
    let mut other_stage = f.stage_command(trial, None);
    let Command::Stage { id, report, .. } = &mut other_stage else {
        unreachable!()
    };
    *id = 999;
    report.id = 888;
    assert_eq!(f.reject(&other_stage), Refusal::Conflict);
}

#[test]
fn mixed_no_graph_retry_then_prepared_final_preserves_growth_and_deduplication() {
    let mut f = Fixture::new(true, 1, 8192);
    let first = f.assign();
    let first_report = f.report(first, None, false);
    assert!(matches!(
        f.apply(Command::NoGraph(first_report.clone())),
        Reply::Classified(Disposition::RetryScheduled { .. })
    ));
    let second = f.assign();
    let mut bad_stage = f.stage_command(second, None);
    let Command::Stage { report, .. } = &mut bad_stage else {
        unreachable!()
    };
    report.id = first_report.id;
    assert_eq!(f.reject(&bad_stage), Refusal::Conflict);
    let stage = f.prepare(second, None);
    f.capacity(2048);
    reason(
        &f.accept(stage),
        a::ResourceFailureReason::NoLargerEligibleWorker,
        &f.payloads[&2],
    );
    let before = f.model.snapshot().unwrap();
    assert_eq!(f.apply(Command::NoGraph(first_report)), Reply::Duplicate);
    assert_eq!(f.model.snapshot().unwrap(), before);
    let restored = Model::restore(&before).unwrap();
    assert_eq!(restored.view(), f.model.view());
    assert_eq!(restored.view().classified.len(), 2);
}

#[test]
fn prepared_retry_then_no_graph_final_retains_stage_without_invented_graph_root() {
    let mut f = Fixture::new(false, 1, 8192);
    let first = f.assign();
    let stage = f.prepare(first, None);
    f.accept(stage);
    let second = f.assign();
    f.capacity(2048);
    let report = f.report(second, None, false);
    let Reply::Classified(disposition) = f.apply(Command::NoGraph(report.clone())) else {
        panic!("classified report expected")
    };
    reason(
        &disposition,
        a::ResourceFailureReason::NoLargerEligibleWorker,
        &report.payload,
    );
    let view = f.model.view();
    assert_eq!(view.accepted[&a::ExecutionId(1)].publication, None);
    assert!(view.heads.is_empty());
    assert!(
        view.roots
            .iter()
            .all(|root| !matches!(root.root, h::Root::Operation(_) | h::Root::Result(_)))
    );
    assert!(
        view.roots
            .iter()
            .any(|root| root.root == h::Root::Stage(stage))
    );
    assert_eq!(view.stages[&stage].writes.len(), 2);
    assert_eq!(
        Model::restore(&f.model.snapshot().unwrap()).unwrap().view(),
        view
    );
}

#[test]
fn retry_matching_cannot_borrow_another_admitted_executions_stage_credit() {
    let mut f = Fixture::new(false, 1, 8192);
    let trial = f.assign();
    let stage = f.prepare(trial, None);
    f.accept(stage);
    for operation in 2..=8 {
        f.apply(Command::Admit {
            operation: a::OperationId(operation),
            request: original(&format!("other-{operation}"), 1),
        });
    }
    assert_eq!(f.model.view().stages.len(), 1);
    assert_eq!(
        f.model
            .view()
            .credits
            .values()
            .filter(|credit| credit.pending)
            .count(),
        7
    );
    assert_eq!(
        f.reject(&Command::Scheduler(a::Event::Match)),
        Refusal::Capacity
    );
    assert_eq!(f.model.view().scheduler.attempts.len(), 1);
}

#[test]
fn finite_history_capacity_refuses_next_event_atomically() {
    let mut f = Fixture::new(false, 1, 8192);
    while f.model.view().position < u64::try_from(MAX_EVENTS).unwrap() {
        let next = 8192 + f.model.view().position;
        f.capacity(next);
    }
    assert_eq!(
        f.reject(&Command::Scheduler(a::Event::Capacity {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            capacity: resources(99999),
            available: resources(99999),
            draining: false
        })),
        Refusal::Capacity
    );
}

#[test]
fn incomplete_stale_retired_and_unready_preparation_cannot_refund_or_finalize() {
    let mut f = Fixture::new(false, 1, 1024);
    let trial = f.assign();
    f.apply(f.stage_command(trial, None));
    let stage = 301;
    assert_eq!(
        f.reject(&Command::Accept(arguments(stage))),
        Refusal::Incomplete
    );
    let proof = f.proof(stage);
    for selector in 0..6 {
        let mut broken = proof.clone();
        match selector {
            0 => {
                broken.typed.pop_first();
            }
            1 => {
                broken.protected.pop_first();
            }
            2 => broken.catalogue_revision += 1,
            3 => broken.fences.clear(),
            4 => broken.complete = false,
            5 => broken.result.hash[0] ^= 1,
            _ => unreachable!(),
        }
        f.reject(&Command::Prepare {
            stage,
            evidence: broken,
        });
    }
    f.apply(Command::Prepare {
        stage,
        evidence: proof,
    });
    f.facts.catalogue_revision += 1;
    assert_eq!(f.reject(&Command::Accept(arguments(stage))), Refusal::Stale);
    f.facts.catalogue_revision -= 1;
    f.facts.physical[0].retired = true;
    assert_eq!(
        f.reject(&Command::Accept(arguments(stage))),
        Refusal::Unprotected
    );
    f.facts.physical[0].retired = false;
    f.facts.physical[0].ready = false;
    assert_eq!(
        f.reject(&Command::Accept(arguments(stage))),
        Refusal::Unprotected
    );
    assert!(!f.model.view().scheduler.reservations[0].released);
    assert!(f.model.view().accepted.is_empty());
    f.facts.physical[0].ready = true;
    f.accept(stage);
}

#[test]
fn revoked_or_expired_stage_cannot_use_earlier_preparation() {
    for revoke in [false, true] {
        let mut f = Fixture::new(false, 1, 8192);
        let trial = f.assign();
        let stage = f.prepare(trial, None);
        if revoke {
            f.scheduler(a::Event::WorkerUnavailable {
                worker: trial.worker,
                incarnation: trial.incarnation,
                session: a::SessionId(1),
            });
        } else {
            f.scheduler(a::Event::AdvanceTime { now: 10 });
        }
        f.reject(&Command::Accept(arguments(stage)));
        assert!(f.model.view().accepted.is_empty());
        assert!(!f.model.view().scheduler.reservations[0].released);
        assert_eq!(f.model.view().stages.len(), 1);
    }
}

#[test]
fn retry_abandonment_and_finalization_retain_old_unknown_writes() {
    let mut f = Fixture::new(false, 1, 8192);
    let first = f.assign();
    let old_stage = f.prepare(first, None);
    f.accept(old_stage);
    let second = f.assign();
    let final_stage = f.prepare(second, None);
    f.capacity(2048);
    f.accept(final_stage);
    f.apply(Command::Abandon(old_stage));
    let before = f.model.view();
    let (write, key) = before.stages[&old_stage].writes.first_key_value().unwrap();
    let object = f.candidates[&201]
        .required
        .iter()
        .find(|member| member.key == *key)
        .unwrap()
        .object
        .clone();
    let receipt = h::Receipt {
        stage: old_stage,
        write: *write,
        key: *key,
        object,
        success: true,
    };
    f.facts.receipts.insert(receipt.clone());
    f.apply(Command::Observe(receipt.clone()));
    let after = f.model.view();
    assert_eq!(before.accepted, after.accepted);
    assert_eq!(before.operations, after.operations);
    assert_eq!(before.roots, after.roots);
    assert_eq!(before.scheduler.reservations, after.scheduler.reservations);
    assert_eq!(after.stages[&old_stage].writes.len(), 2);
    assert_eq!(after.stages[&old_stage].outcomes.len(), 1);
    let snapshot = f.model.snapshot().unwrap();
    assert_eq!(f.apply(Command::Observe(receipt.clone())), Reply::Duplicate);
    assert_eq!(f.model.snapshot().unwrap(), snapshot);
    let mut conflict = receipt;
    conflict.success = false;
    f.facts.receipts.insert(conflict.clone());
    assert_eq!(f.reject(&Command::Observe(conflict)), Refusal::Conflict);
}

#[test]
fn restore_every_cut_and_suffix_replays_original_capacity_decisions() {
    let mut f = Fixture::new(true, 1, 8192);
    let first = f.assign();
    let stage = f.prepare(first, None);
    f.capacity(1024);
    f.accept(stage);
    f.capacity(65536);
    let complete = f.model.snapshot().unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&complete).unwrap();
    let history: Vec<Step> = serde_json::from_value(decoded["history"].clone()).unwrap();
    for cut in 0..=history.len() {
        let mut prefix = decoded.clone();
        prefix["history"] = serde_json::to_value(&history[..cut]).unwrap();
        // Re-encode the typed frame so struct field order stays canonical.
        let bytes = canonical_frame(&prefix);
        let mut restored = Model::restore(&bytes).unwrap();
        for step in &history[cut..] {
            restored.apply(&step.command, &step.facts).unwrap();
        }
        assert_eq!(restored.snapshot().unwrap(), complete, "cut {cut}");
        assert_eq!(restored.view(), f.model.view(), "cut {cut}");
    }
}

fn canonical_frame(value: &serde_json::Value) -> Vec<u8> {
    #[derive(serde::Deserialize, serde::Serialize)]
    struct Frame {
        format: String,
        context: Context,
        scenario: h::Scenario,
        seeds: Vec<h::Coverage>,
        history: Vec<Step>,
    }
    serde_json::to_vec(&serde_json::from_value::<Frame>(value.clone()).unwrap()).unwrap()
}

#[test]
fn restore_refuses_torn_history_conflicting_identity_downgrade_and_oversize() {
    let mut f = Fixture::new(false, 1, 1024);
    let trial = f.assign();
    let stage = f.prepare(trial, None);
    f.accept(stage);
    let snapshot = f.model.snapshot().unwrap();
    let original: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
    let mut no_prepare = original.clone();
    no_prepare["history"]
        .as_array_mut()
        .unwrap()
        .retain(|step| step["command"].get("Prepare").is_none());
    assert!(Model::restore(&canonical_frame(&no_prepare)).is_err());
    let mut grant = original.clone();
    for step in grant["history"].as_array_mut().unwrap() {
        if let Some(stage) = step["command"].get_mut("Stage") {
            stage["report"]["classification"]["MemoryFailure"]["original_charged"]["memory_bytes"] =
                999.into();
        }
    }
    assert!(Model::restore(&canonical_frame(&grant)).is_err());
    let mut old = original;
    old["format"] = "independent-no-graph-terminals-v2".into();
    assert!(Model::restore(&canonical_frame(&old)).is_err());
    assert!(Model::restore(&snapshot[..snapshot.len() - 1]).is_err());
    let mut trailing = snapshot;
    trailing.push(b' ');
    assert!(Model::restore(&trailing).is_err());
    assert!(Model::restore(&vec![0; MAX_SNAPSHOT + 1]).is_err());
}

#[path = "audit_tests.rs"]
mod audit_tests;
