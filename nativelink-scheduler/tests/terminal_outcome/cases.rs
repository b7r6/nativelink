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

use std::collections::BTreeMap;

use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command as ReapiCommand, Digest as ReapiDigest, ExecuteRequest,
    ExecuteResponse, ExecutionPolicy, Platform, platform,
};
use nativelink_proto::google::rpc::Status;
use nativelink_scheduler::authority as a;
use prost::Message;
use prost_types::{Duration, Timestamp};
use sha2::{Digest, Sha256};

use super::reference::Model;
use super::types::*;
use super::{m1, protocol as r};

fn context() -> Context {
    Context {
        principal: "terminal-principal".into(),
        execution_config: "terminal-config-v1".into(),
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
        instance_name: "no-graph-terminal-fixture".into(),
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

fn model() -> Model {
    Model::new(context(), &[]).unwrap()
}

fn scheduler(model: &mut Model, event: a::Event) -> a::Reply {
    let Reply::Scheduler(reply) = model.apply(&Command::Scheduler(event)).unwrap() else {
        panic!("scheduler reply expected")
    };
    reply
}

fn register(model: &mut Model, memory: u64) {
    let capacity = a::Resources {
        cpu_millis: 8000,
        memory_bytes: memory,
        slots: 8,
    };
    assert_eq!(
        scheduler(
            model,
            a::Event::Register {
                worker: a::WorkerId(1),
                incarnation: a::IncarnationId(1),
                session: a::SessionId(1),
                properties: BTreeMap::from([("os".into(), "linux".into())]),
                capacity,
                available: capacity,
            }
        ),
        a::Reply::Applied
    );
}

fn admit(model: &mut Model, operation: u64, request: a::Request) -> a::ExecutionId {
    let Reply::Scheduler(a::Reply::Submitted { execution, .. }) = model
        .apply(&Command::Admit {
            operation: a::OperationId(operation),
            request,
        })
        .unwrap()
    else {
        panic!("admission expected")
    };
    execution
}

fn assign(model: &mut Model) -> a::AttemptView {
    let a::Reply::Matched { attempt, .. } = scheduler(model, a::Event::Match) else {
        panic!("match expected")
    };
    let trial = model
        .view()
        .unwrap()
        .scheduler
        .attempts
        .into_iter()
        .find(|trial| trial.id == attempt)
        .unwrap();
    assert_eq!(
        scheduler(
            model,
            a::Event::WorkerAccepted {
                attempt,
                worker: trial.worker,
                incarnation: trial.incarnation
            }
        ),
        a::Reply::Applied
    );
    trial
}

const fn report(
    id: u64,
    trial: a::AttemptView,
    classification: Classification,
    payload: a::ResultPayload,
) -> Report {
    Report {
        id,
        identity: WorkerIdentity {
            attempt: trial.id,
            worker: trial.worker,
            incarnation: trial.incarnation,
        },
        classification,
        payload,
    }
}

fn complete(id: u64, trial: a::AttemptView) -> Report {
    report(
        id,
        trial,
        Classification::Complete,
        payload("original worker diagnostic", None),
    )
}

fn memory(model: &Model, id: u64, trial: a::AttemptView, peak: Option<u64>) -> Report {
    let charged = model
        .view()
        .unwrap()
        .scheduler
        .reservations
        .into_iter()
        .find(|row| row.id == trial.reservation)
        .unwrap()
        .charged;
    report(
        id,
        trial,
        Classification::MemoryFailure {
            original_charged: charged,
            peak_memory_bytes: peak,
            quiescent: true,
        },
        payload("explicit memory classifier evidence", None),
    )
}

fn accepted(model: &mut Model, report: Report) -> Acceptance {
    let Reply::Report(Disposition::TerminalAccepted(accepted)) =
        model.apply(&Command::Report(report)).unwrap()
    else {
        panic!("terminal acceptance expected")
    };
    accepted
}

fn rejected(model: &mut Model, command: &Command) -> Refusal {
    let before = model.snapshot().unwrap();
    let reason = model.apply(command).unwrap_err();
    assert_eq!(
        model.snapshot().unwrap(),
        before,
        "refusal changed an authority"
    );
    reason
}

fn recover(model: &mut Model) {
    let bytes = model.snapshot().unwrap();
    let view = model.view().unwrap();
    *model = Model::restore(&bytes).unwrap();
    assert_eq!(model.snapshot().unwrap(), bytes);
    assert_eq!(model.view().unwrap(), view);
}

const fn physical(generation: u64) -> Physical {
    Physical {
        namespace: [7; 16],
        incarnation: [9; 16],
        generation,
    }
}

fn stage(model: &mut Model, id: u64, trial: a::AttemptView, generation: u64) -> Stage {
    let until = model.view().unwrap().scheduler.now + 100;
    assert_eq!(
        model.apply(&Command::Stage {
            id,
            attempt: trial.id,
            members: vec![physical(generation)],
            until
        }),
        Ok(Reply::Applied)
    );
    model.view().unwrap().stages[&id].clone()
}

fn cleanup(model: &mut Model, trial: a::AttemptView) -> a::Reply {
    scheduler(
        model,
        a::Event::Cleanup {
            attempt: trial.id,
            worker: trial.worker,
            incarnation: trial.incarnation,
        },
    )
}

#[test]
fn no_result_complete_preserves_exact_bytes_and_origin_for_both_algorithms() {
    for blake3 in [false, true] {
        let mut model = model();
        register(&mut model, 8192);
        let original = request("complete", blake3, 1, 0, 37, &context().principal);
        let execution = admit(&mut model, 1, original.clone());
        let trial = assign(&mut model);
        let report = complete(1, trial);
        let before = model.view().unwrap();
        let accepted = accepted(&mut model, report.clone());
        let after = model.view().unwrap();
        assert_eq!(
            accepted.terminal,
            a::TerminalResult::Completed(report.payload)
        );
        assert_eq!(
            accepted.origin,
            Origin {
                epoch: a::ClusterEpoch(1),
                position: before.position + 1,
                now: 0,
                execution,
                identity: report.identity
            }
        );
        assert_eq!(after.original_requests[&a::OperationId(1)], original);
        assert_eq!(
            after.retention, before.retention,
            "no graph must create no root"
        );
        assert!(
            after.scheduler.effects.is_empty(),
            "error completion must not hand off an AC write"
        );
        assert!(after.scheduler.reservations.iter().all(|row| row.released));
        assert_eq!(
            after.operations[&a::OperationId(1)],
            OperationLink::NoGraph(1)
        );
        assert!(!after.credits[&execution].pending_stage);
        recover(&mut model);
    }
}

#[test]
fn memory_classification_preserves_all_three_actual_final_reasons() {
    for (memory_kb, capacity, reason) in [
        (0, 8192, a::ResourceFailureReason::MissingGrowthEvidence),
        (1, 1024, a::ResourceFailureReason::NoLargerEligibleWorker),
        (1, 8192, a::ResourceFailureReason::AttemptBudgetExhausted),
    ] {
        let mut model = model();
        register(&mut model, capacity);
        admit(&mut model, 1, original("memory", memory_kb));
        for attempt in 1..=3 {
            let trial = assign(&mut model);
            let report = memory(&model, attempt, trial, None);
            let terminal =
                attempt == 3 || reason != a::ResourceFailureReason::AttemptBudgetExhausted;
            let reply = model.apply(&Command::Report(report.clone())).unwrap();
            if terminal {
                let Reply::Report(Disposition::TerminalAccepted(accepted)) = reply else {
                    panic!("final expected")
                };
                assert_eq!(
                    accepted.terminal,
                    a::TerminalResult::ResourceFailed {
                        result: report.payload.clone(),
                        reason
                    }
                );
                assert_eq!(accepted.origin.identity.attempt, trial.id);
                assert_eq!(model.view().unwrap().accepted.len(), 1);
                let before = model.snapshot().unwrap();
                assert_eq!(model.apply(&Command::Report(report)), Ok(Reply::Duplicate));
                assert_eq!(model.snapshot().unwrap(), before);
                assert_eq!(cleanup(&mut model, trial), a::Reply::Ignored);
                recover(&mut model);
                break;
            }
            assert_eq!(reply, Reply::Report(Disposition::RetryScheduled));
            assert!(model.view().unwrap().accepted.is_empty());
        }
        assert_eq!(
            model.view().unwrap().scheduler.workers[0].charged,
            a::Resources::default()
        );
    }
}

#[test]
fn retry_growth_is_decided_by_m1_and_preserves_original_requests_and_stages() {
    let mut model = model();
    register(&mut model, 8192);
    let request = original("retry", 1);
    let execution = admit(&mut model, 1, request.clone());
    let first = assign(&mut model);
    let old_stage = stage(&mut model, 1, first, 1);
    let before_storage = model.view().unwrap().retention;
    let report = memory(&model, 1, first, Some(100_000));
    assert_eq!(
        model.apply(&Command::Report(report)),
        Ok(Reply::Report(Disposition::RetryScheduled))
    );
    let view = model.view().unwrap();
    assert_eq!(
        view.scheduler.executions[0].reservation_target.memory_bytes, 2048,
        "nonzero original grant, not peak, is the growth base"
    );
    assert_eq!(view.scheduler.executions[0].request, request);
    assert_eq!(view.retention, before_storage);
    assert_eq!(view.operations[&a::OperationId(1)], OperationLink::Waiting);
    assert!(view.accepted.is_empty());
    let second = assign(&mut model);
    assert_ne!(second.id, first.id);
    assert!(model.view().unwrap().credits[&execution].pending_stage);
    stage(&mut model, 2, second, 2);
    let reservation = model.view().unwrap().scheduler.reservations[1];
    assert_eq!(reservation.charged.memory_bytes, 2048);
    assert_eq!(
        model.apply(&Command::WriteDone {
            write: old_stage.writes[0],
            success: true
        }),
        Ok(Reply::Applied)
    );
    assert!(model.view().unwrap().accepted.is_empty());
    assert!(!model.view().unwrap().scheduler.reservations[1].released);
    recover(&mut model);
}

#[test]
fn graph_bearing_complete_and_memory_reports_are_refused_without_empty_graph_fallback() {
    for memory in [false, true] {
        for exit_code in [0, 17] {
            let mut model = model();
            register(&mut model, 1024);
            admit(&mut model, 1, original("graph", 1));
            let trial = assign(&mut model);
            let mut report = if memory {
                self::memory(&model, 1, trial, None)
            } else {
                complete(1, trial)
            };
            report.payload = payload(
                "an empty ActionResult is still a graph",
                Some(ActionResult {
                    exit_code,
                    ..ActionResult::default()
                }),
            );
            assert_eq!(
                rejected(&mut model, &Command::Report(report)),
                Refusal::GraphBearing
            );
        }
    }
}

#[test]
fn stale_identity_wrong_grant_and_nonquiescent_reports_are_atomic_refusals() {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("invalid", 1));
    let trial = assign(&mut model);
    for mutation in 0..5 {
        let mut report = memory(&model, 1, trial, None);
        match mutation {
            0 => report.identity.attempt = a::AttemptId(99),
            1 => report.identity.incarnation = a::IncarnationId(99),
            2 => report.identity.worker = a::WorkerId(99),
            3 => {
                if let Classification::MemoryFailure {
                    original_charged, ..
                } = &mut report.classification
                {
                    original_charged.memory_bytes += 1;
                }
            }
            4 => {
                if let Classification::MemoryFailure { quiescent, .. } = &mut report.classification
                {
                    *quiescent = false;
                }
            }
            _ => unreachable!(),
        }
        rejected(&mut model, &Command::Report(report));
    }
}

#[test]
fn each_joined_handle_keeps_its_terminal_in_both_cancel_report_orders() {
    for cancel_first in [false, true] {
        let mut model = model();
        register(&mut model, 8192);
        let execution = admit(&mut model, 1, original("joined", 1));
        let joined = request("joined", false, 1, -9, 123, &context().principal);
        assert_eq!(admit(&mut model, 2, joined.clone()), execution);
        let trial = assign(&mut model);
        if cancel_first {
            assert_eq!(
                scheduler(
                    &mut model,
                    a::Event::Cancel {
                        operation: a::OperationId(1)
                    }
                ),
                a::Reply::Applied
            );
        }
        let accepted = accepted(&mut model, complete(1, trial));
        let expected = if cancel_first {
            a::TerminalResult::Cancelled
        } else {
            accepted.terminal.clone()
        };
        assert_eq!(
            scheduler(
                &mut model,
                a::Event::Cancel {
                    operation: a::OperationId(1)
                }
            ),
            a::Reply::Ignored
        );
        let view = model.view().unwrap();
        assert_eq!(view.scheduler.operations[0].terminal, Some(expected));
        assert_eq!(
            view.scheduler.operations[1].terminal,
            Some(accepted.terminal)
        );
        assert_eq!(view.original_requests[&a::OperationId(2)], joined);
        assert_eq!(
            view.operations[&a::OperationId(1)],
            if cancel_first {
                OperationLink::Independent(a::TerminalResult::Cancelled)
            } else {
                OperationLink::NoGraph(1)
            }
        );
        recover(&mut model);
    }
}

#[test]
fn last_cancel_closes_logical_credit_but_retains_physical_charge_until_cleanup() {
    for active in [false, true] {
        let mut model = model();
        register(&mut model, 8192);
        let execution = admit(&mut model, 1, original("last-cancel", 1));
        let trial = active.then(|| assign(&mut model));
        let before = model.view().unwrap();
        assert_eq!(
            scheduler(
                &mut model,
                a::Event::Cancel {
                    operation: a::OperationId(1)
                }
            ),
            a::Reply::Applied
        );
        let view = model.view().unwrap();
        assert_eq!(
            view.scheduler.executions[0].terminal,
            Some(a::TerminalResult::Cancelled)
        );
        assert_eq!(
            view.scheduler.workers[0].charged,
            before.scheduler.workers[0].charged
        );
        assert!(!view.credits[&execution].pending_stage);
        assert_eq!(view.credits[&execution].unused_members, 0);
        assert!(view.accepted.is_empty());
        if let Some(trial) = trial {
            assert!(!view.scheduler.reservations[0].released);
            assert!(view.scheduler.effects.iter().any(|effect| matches!(effect.kind, a::EffectKind::Cancel { attempt, .. } if attempt == trial.id)));
            rejected(&mut model, &Command::Report(complete(1, trial)));
            assert_eq!(cleanup(&mut model, trial), a::Reply::Applied);
            assert_eq!(cleanup(&mut model, trial), a::Reply::Ignored);
            assert_eq!(
                model.view().unwrap().scheduler.workers[0].charged,
                a::Resources::default()
            );
        }
        recover(&mut model);
    }
}

#[test]
fn interest_expiry_requires_current_generation_and_advance_only_moves_clocks() {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("interest", 1));
    let trial = assign(&mut model);
    let deadline = model.view().unwrap().scheduler.operations[0].interest_deadline;
    assert_eq!(
        rejected(
            &mut model,
            &Command::Scheduler(a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: deadline.generation
            })
        ),
        Refusal::Scheduler(a::Rejection::TooEarly)
    );
    scheduler(&mut model, a::Event::AdvanceTime { now: 100 });
    scheduler(
        &mut model,
        a::Event::RenewInterest {
            operation: a::OperationId(1),
        },
    );
    let renewed = model.view().unwrap().scheduler.operations[0].interest_deadline;
    scheduler(&mut model, a::Event::AdvanceTime { now: 400 });
    assert!(
        model.view().unwrap().scheduler.operations[0]
            .terminal
            .is_none()
    );
    assert_eq!(
        scheduler(
            &mut model,
            a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: deadline.generation
            }
        ),
        a::Reply::Ignored
    );
    assert_eq!(
        scheduler(
            &mut model,
            a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: renewed.generation
            }
        ),
        a::Reply::Applied
    );
    assert!(!model.view().unwrap().scheduler.reservations[0].released);
    assert_eq!(cleanup(&mut model, trial), a::Reply::Applied);
    recover(&mut model);
}

#[test]
fn attempt_progress_and_running_timeouts_fence_successors_and_end_in_exhausted() {
    for running in [false, true] {
        let mut model = model();
        register(&mut model, 8192);
        let execution = admit(&mut model, 1, original("timeout", 1));
        let mut old = None;
        for index in 0..3 {
            let trial = assign(&mut model);
            if let Some(old) = old {
                assert_eq!(
                    scheduler(&mut model, a::Event::ExpireExecution { attempt: old }),
                    a::Reply::Ignored
                );
                assert_eq!(
                    rejected(
                        &mut model,
                        &Command::Scheduler(a::Event::Cleanup {
                            attempt: trial.id,
                            worker: trial.worker,
                            incarnation: trial.incarnation
                        })
                    ),
                    Refusal::Scheduler(a::Rejection::Busy)
                );
            }
            scheduler(
                &mut model,
                a::Event::Progress {
                    attempt: trial.id,
                    worker: trial.worker,
                    incarnation: trial.incarnation,
                    phase: if running {
                        a::Phase::Running
                    } else {
                        a::Phase::Preparing
                    },
                    progress: 1,
                },
            );
            let current = model
                .view()
                .unwrap()
                .scheduler
                .attempts
                .iter()
                .find(|row| row.id == trial.id)
                .copied()
                .unwrap();
            assert_eq!(
                scheduler(
                    &mut model,
                    a::Event::ExpireAttempt {
                        attempt: trial.id,
                        generation: trial.deadline.generation
                    }
                ),
                a::Reply::Ignored
            );
            let deadline = if running {
                current.running_deadline.unwrap()
            } else {
                current.deadline.at
            };
            let event = if running {
                a::Event::ExpireExecution { attempt: trial.id }
            } else {
                a::Event::ExpireAttempt {
                    attempt: trial.id,
                    generation: current.deadline.generation,
                }
            };
            assert_eq!(
                rejected(&mut model, &Command::Scheduler(event.clone())),
                Refusal::Scheduler(a::Rejection::TooEarly)
            );
            scheduler(&mut model, a::Event::AdvanceTime { now: deadline });
            assert!(
                model.view().unwrap().scheduler.executions[0]
                    .terminal
                    .is_none()
            );
            assert_eq!(scheduler(&mut model, event), a::Reply::Applied);
            let view = model.view().unwrap();
            assert!(!view.scheduler.reservations[index].released);
            assert_eq!(
                view.scheduler.executions[0].terminal,
                (index == 2).then_some(a::TerminalResult::Exhausted)
            );
            assert_eq!(cleanup(&mut model, trial), a::Reply::Applied);
            old = Some(trial.id);
            recover(&mut model);
        }
        assert!(model.view().unwrap().accepted.is_empty());
        assert!(!model.view().unwrap().credits[&execution].pending_stage);
        assert_eq!(
            model.view().unwrap().operations[&a::OperationId(1)],
            OperationLink::Independent(a::TerminalResult::Exhausted)
        );
    }
}

#[test]
fn unknown_stage_writes_survive_cancel_retry_final_and_expiry_then_late_success() {
    for scenario in 0..4 {
        let mut model = model();
        register(&mut model, 8192);
        admit(
            &mut model,
            1,
            original("unknown-write", u64::from(scenario != 2)),
        );
        let trial = assign(&mut model);
        let stage = stage(&mut model, 1, trial, 1);
        let before = model.view().unwrap();
        match scenario {
            0 => {
                scheduler(
                    &mut model,
                    a::Event::Cancel {
                        operation: a::OperationId(1),
                    },
                );
            }
            1 | 2 => {
                let report = memory(&model, 1, trial, None);
                model.apply(&Command::Report(report)).unwrap();
            }
            3 => {
                accepted(&mut model, complete(1, trial));
            }
            _ => unreachable!(),
        }
        assert_eq!(model.view().unwrap().retention, before.retention);
        let terminal = model.view().unwrap().scheduler.executions[0]
            .terminal
            .clone();
        scheduler(&mut model, a::Event::AdvanceTime { now: 100 });
        assert!(model.view().unwrap().retention.protected.is_empty());
        assert_eq!(
            model.view().unwrap().retention.writes[&stage.writes[0]].outcome,
            None
        );
        model.apply(&Command::Abandon(1)).unwrap();
        let accepted_before = model.view().unwrap().accepted;
        assert_eq!(
            model.apply(&Command::WriteDone {
                write: stage.writes[0],
                success: true
            }),
            Ok(Reply::Applied)
        );
        assert_eq!(
            model.view().unwrap().scheduler.executions[0].terminal,
            terminal
        );
        assert_eq!(model.view().unwrap().accepted, accepted_before);
        assert_eq!(model.view().unwrap().stages.len(), 1);
        assert_eq!(model.view().unwrap().retention.writes.len(), 1);
        recover(&mut model);
    }
}

#[test]
fn unrelated_publication_operation_and_serving_roots_survive_no_graph_acceptance() {
    let seeds: Vec<_> = [
        r::RootKind::Publication,
        r::RootKind::Operation,
        r::RootKind::Serving,
    ]
    .into_iter()
    .map(|kind| Seed {
        kind,
        members: vec![physical(100)],
        until: 100,
    })
    .collect();
    let mut model = Model::new(context(), &seeds).unwrap();
    register(&mut model, 8192);
    admit(&mut model, 1, original("protected", 1));
    let trial = assign(&mut model);
    let before = model.view().unwrap().retention;
    accepted(&mut model, complete(1, trial));
    assert_eq!(model.view().unwrap().retention, before);
    assert_eq!(before.protected.len(), 1);
    assert_eq!(
        before.roots.values().filter(|root| root.until > 0).count(),
        3
    );
    recover(&mut model);
}

#[test]
fn recorded_origin_survives_later_worker_replacement_and_duplicate_report_is_stable() {
    let mut model = model();
    register(&mut model, 8192);
    let execution = admit(&mut model, 1, original("origin", 1));
    let trial = assign(&mut model);
    let report = complete(91, trial);
    let accepted = accepted(&mut model, report.clone());
    scheduler(
        &mut model,
        a::Event::WorkerUnavailable {
            worker: trial.worker,
            incarnation: trial.incarnation,
            session: a::SessionId(1),
        },
    );
    let capacity = a::Resources {
        cpu_millis: 8000,
        memory_bytes: 8192,
        slots: 8,
    };
    scheduler(
        &mut model,
        a::Event::Register {
            worker: trial.worker,
            incarnation: a::IncarnationId(2),
            session: a::SessionId(2),
            properties: BTreeMap::new(),
            capacity,
            available: capacity,
        },
    );
    assert_eq!(model.view().unwrap().accepted[&execution], accepted);
    let before = model.snapshot().unwrap();
    assert_eq!(
        model.apply(&Command::Report(report.clone())),
        Ok(Reply::Duplicate)
    );
    assert_eq!(model.snapshot().unwrap(), before);
    let mut conflict = report;
    conflict.payload = payload("changed report under the same identity", None);
    assert_eq!(
        rejected(&mut model, &Command::Report(conflict)),
        Refusal::Conflict
    );
    recover(&mut model);
}

#[test]
fn per_operation_principal_binding_is_checked_on_admission_and_snapshot_restore() {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("binding", 1));
    let different_original = request("binding", false, 1, -9, 123, &context().principal);
    admit(&mut model, 2, different_original);
    let foreign = request("binding", false, 1, -9, 123, "foreign-principal");
    assert_eq!(
        rejected(
            &mut model,
            &Command::Admit {
                operation: a::OperationId(3),
                request: foreign.clone()
            }
        ),
        Refusal::Unauthorized
    );
    let mut wire: serde_json::Value = serde_json::from_slice(&model.snapshot().unwrap()).unwrap();
    wire[1]["scheduler"]["interests"][1]["original"] = serde_json::to_value(foreign).unwrap();
    let inner: m1::Reference = serde_json::from_value(wire[1]["scheduler"].clone()).unwrap();
    assert!(
        m1::Reference::restore(&inner.snapshot().unwrap()).is_ok(),
        "a valid non-Buck2 joined capture reaches the outer principal boundary"
    );
    let changed: Model = serde_json::from_value(wire[1].clone()).unwrap();
    assert!(
        Model::restore(&changed.snapshot().unwrap()).is_err(),
        "representative request must not hide a foreign joined caller"
    );
    recover(&mut model);
}

#[test]
fn snapshot_suffix_cuts_preserve_terminals_charges_writes_and_originals() {
    let mut start = model();
    register(&mut start, 8192);
    admit(&mut start, 1, original("cuts", 1));
    admit(
        &mut start,
        2,
        request("cuts", true, 1, 0, 12, &context().principal),
    );
    // Distinct algorithms deliberately create independent qualified executions.
    let first = assign(&mut start);
    let stage = stage(&mut start, 1, first, 1);
    let commands = [
        Command::Scheduler(a::Event::Cancel {
            operation: a::OperationId(1),
        }),
        Command::Scheduler(a::Event::AdvanceTime { now: 100 }),
        Command::WriteDone {
            write: stage.writes[0],
            success: true,
        },
        Command::Scheduler(a::Event::Cleanup {
            attempt: first.id,
            worker: first.worker,
            incarnation: first.incarnation,
        }),
        Command::Abandon(1),
    ];
    let mut expected = start.clone();
    for command in &commands {
        expected.apply(command).unwrap();
    }
    for cut in 0..=commands.len() {
        let mut model = start.clone();
        for command in &commands[..cut] {
            model.apply(command).unwrap();
        }
        recover(&mut model);
        for command in &commands[cut..] {
            model.apply(command).unwrap();
        }
        assert_eq!(model.snapshot().unwrap(), expected.snapshot().unwrap());
        assert_eq!(model.view().unwrap(), expected.view().unwrap());
    }
}

#[test]
fn finite_stage_and_execution_credits_refuse_without_dispatch_or_borrowing() {
    let mut model = model();
    register(&mut model, 8192 * 16);
    for operation in 1..=8 {
        admit(
            &mut model,
            operation,
            original(&format!("job-{operation}"), 1),
        );
    }
    assert!(matches!(
        rejected(
            &mut model,
            &Command::Admit {
                operation: a::OperationId(9),
                request: original("overflow", 1)
            }
        ),
        Refusal::Scheduler(a::Rejection::Overloaded)
    ));
    let trial = assign(&mut model);
    stage(&mut model, 1, trial, 1);
    let report = memory(&model, 1, trial, None);
    assert_eq!(
        model.apply(&Command::Report(report)),
        Ok(Reply::Report(Disposition::RetryScheduled))
    );
    // Original queue ordering selects this retry. Its extra retained stage
    // cannot borrow one of the seven other executions' prepaid stage slots.
    assert_eq!(
        rejected(&mut model, &Command::Scheduler(a::Event::Match)),
        Refusal::Capacity
    );
    assert!(model.view().unwrap().stages.contains_key(&1));
    assert_eq!(model.view().unwrap().credits.len(), 8);
    recover(&mut model);
}

#[test]
fn complete_outer_snapshot_byte_gate_refuses_a_valid_terminal_atomically() {
    let mut model = model();
    register(&mut model, 8192 * 16);
    for operation in 1..=8 {
        admit(
            &mut model,
            operation,
            original(&format!("large-{operation}"), 1),
        );
    }
    let large = payload(&"x".repeat(70_000), None);
    let mut accepted_count = 0;
    let mut byte_refusal = false;
    for id in 1..=8 {
        let trial = assign(&mut model);
        let before = model.snapshot().unwrap();
        let report = report(id, trial, Classification::Complete, large.clone());
        match model.apply(&Command::Report(report)) {
            Ok(Reply::Report(Disposition::TerminalAccepted(_))) => {
                accepted_count += 1;
                assert!(model.snapshot().unwrap().len() <= MAX_SNAPSHOT);
                recover(&mut model);
            }
            Err(Refusal::Snapshot) => {
                byte_refusal = true;
                assert_eq!(model.snapshot().unwrap(), before);
                assert!(
                    !model
                        .view()
                        .unwrap()
                        .scheduler
                        .reservations
                        .last()
                        .unwrap()
                        .released
                );
                break;
            }
            reply => panic!("unexpected result before the outer byte boundary: {reply:?}"),
        }
    }
    assert!(accepted_count > 0);
    assert!(
        byte_refusal,
        "the measured large payload must exercise the aggregate outer gate"
    );
}

#[test]
fn every_retry_then_final_snapshot_cut_keeps_classification_and_exact_payload() {
    let mut start = model();
    register(&mut start, 8192);
    admit(&mut start, 1, original("retry-cuts", 1));
    let first = assign(&mut start);
    let first_report = memory(&start, 1, first, None);
    let mut recording = start.clone();
    let mut commands = vec![Command::Report(first_report)];
    recording.apply(&commands[0]).unwrap();
    commands.push(Command::Scheduler(a::Event::Match));
    recording.apply(commands.last().unwrap()).unwrap();
    let second = *recording.view().unwrap().scheduler.attempts.last().unwrap();
    commands.push(Command::Scheduler(a::Event::WorkerAccepted {
        attempt: second.id,
        worker: second.worker,
        incarnation: second.incarnation,
    }));
    recording.apply(commands.last().unwrap()).unwrap();
    commands.push(Command::Report(complete(2, second)));
    recording.apply(commands.last().unwrap()).unwrap();
    commands.push(Command::Scheduler(a::Event::Cancel {
        operation: a::OperationId(1),
    }));
    recording.apply(commands.last().unwrap()).unwrap();
    for cut in 0..=commands.len() {
        let mut model = start.clone();
        for command in &commands[..cut] {
            model.apply(command).unwrap();
        }
        recover(&mut model);
        for command in &commands[cut..] {
            model.apply(command).unwrap();
        }
        assert_eq!(model.snapshot().unwrap(), recording.snapshot().unwrap());
        assert_eq!(model.view().unwrap(), recording.view().unwrap());
    }
}

#[test]
fn worker_unavailability_final_exhaustion_releases_only_unused_logical_credit() {
    let mut model = model();
    register(&mut model, 8192);
    let execution = admit(&mut model, 1, original("unavailable", 1));
    let capacity = a::Resources {
        cpu_millis: 8000,
        memory_bytes: 8192,
        slots: 8,
    };
    for index in 0..3 {
        let trial = assign(&mut model);
        scheduler(
            &mut model,
            a::Event::WorkerUnavailable {
                worker: trial.worker,
                incarnation: trial.incarnation,
                session: a::SessionId(1),
            },
        );
        let view = model.view().unwrap();
        assert!(!view.scheduler.reservations[index].released);
        if index == 2 {
            assert_eq!(
                view.scheduler.executions[0].terminal,
                Some(a::TerminalResult::Exhausted)
            );
            assert!(!view.credits[&execution].pending_stage);
            assert_eq!(view.credits[&execution].unused_members, 0);
        }
        cleanup(&mut model, trial);
        scheduler(
            &mut model,
            a::Event::Capacity {
                worker: trial.worker,
                incarnation: trial.incarnation,
                session: a::SessionId(1),
                capacity,
                available: capacity,
                draining: false,
            },
        );
    }
    assert!(model.view().unwrap().accepted.is_empty());
    recover(&mut model);
}

#[test]
fn full_physical_identity_is_bijective_across_namespaces_and_incarnations() {
    let mut second = physical(1);
    second.namespace[15] = 99;
    let mut third = physical(1);
    third.incarnation[15] = 99;
    let mut members = vec![physical(1), second, third];
    members.sort();
    let mut model = Model::new(
        context(),
        &[Seed {
            kind: r::RootKind::Publication,
            members: members.clone(),
            until: 100,
        }],
    )
    .unwrap();
    let view = model.view().unwrap();
    assert_eq!(view.physical.len(), 3);
    assert_eq!(view.retention.protected.len(), 3);
    assert_eq!(
        view.physical
            .iter()
            .map(|(physical, _)| *physical)
            .collect::<Vec<_>>(),
        members
    );
    recover(&mut model);
}

#[test]
fn malformed_or_synthetic_result_facts_and_unsupported_events_refuse_unchanged() {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("malformed", 1));
    let trial = assign(&mut model);
    for mutation in 0..4 {
        let mut report = complete(1, trial);
        match mutation {
            0 => report.payload.digest = "00".repeat(32),
            1 => report.payload.exit_code = 17,
            2 => report.payload.error = None,
            3 => report.payload.response = None,
            _ => unreachable!(),
        }
        rejected(&mut model, &Command::Report(report));
    }
    assert_eq!(
        rejected(
            &mut model,
            &Command::Scheduler(a::Event::Complete {
                attempt: trial.id,
                worker: trial.worker,
                incarnation: trial.incarnation,
                result: payload("bypass", None)
            })
        ),
        Refusal::Unsupported
    );
    assert_eq!(
        rejected(
            &mut model,
            &Command::Scheduler(a::Event::RetireExecution {
                execution: trial.execution
            })
        ),
        Refusal::Unsupported
    );
}

#[test]
fn original_zero_memory_grant_uses_explicit_peak_only_for_retry_target() {
    let mut model = model();
    register(&mut model, 8192);
    let original = original("zero-grant", 0);
    admit(&mut model, 1, original.clone());
    let trial = assign(&mut model);
    let report = memory(&model, 1, trial, Some(1024));
    assert_eq!(
        model.apply(&Command::Report(report)),
        Ok(Reply::Report(Disposition::RetryScheduled))
    );
    let view = model.view().unwrap();
    assert_eq!(view.scheduler.executions[0].request, original);
    assert_eq!(
        view.scheduler.executions[0].reservation_target.memory_bytes,
        2048
    );
    assert_eq!(view.scheduler.reservations[0].charged.memory_bytes, 0);
    assert_eq!(view.scheduler.workers[0].charged, a::Resources::default());
    recover(&mut model);
}

#[test]
fn duplicate_admission_is_stable_and_operation_link_rows_are_prepaid() {
    let mut model = model();
    register(&mut model, 8192);
    let request = original("operation-bound", 1);
    let execution = admit(&mut model, 1, request.clone());
    let before = model.snapshot().unwrap();
    assert_eq!(admit(&mut model, 1, request.clone()), execution);
    assert_eq!(model.snapshot().unwrap(), before);
    for operation in 2..=16 {
        assert_eq!(admit(&mut model, operation, request.clone()), execution);
    }
    assert_eq!(
        rejected(
            &mut model,
            &Command::Admit {
                operation: a::OperationId(17),
                request
            }
        ),
        Refusal::Scheduler(a::Rejection::Overloaded)
    );
    let trial = assign(&mut model);
    accepted(&mut model, complete(1, trial));
    assert_eq!(model.view().unwrap().operations.len(), 16);
    assert!(
        model
            .view()
            .unwrap()
            .operations
            .values()
            .all(|link| link == &OperationLink::NoGraph(1))
    );
    recover(&mut model);
}
