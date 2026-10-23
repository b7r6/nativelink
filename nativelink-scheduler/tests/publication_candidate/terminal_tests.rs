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

//! Candidate scenarios written from the frozen terminal contract, without reading
//! the terminal reference reducer or tests. Real State decides every outcome.

use super::*;

fn diagnostic(label: &str) -> a::ResultPayload {
    a::ResultPayload::from_response(
        &ExecuteResponse {
            result: None,
            status: Some(Status {
                code: 13,
                message: format!("failure: {label}"),
                ..Status::default()
            }),
            message: format!("exact diagnostic: \"{label}\" λ"),
            ..ExecuteResponse::default()
        },
        MAX_PAYLOAD,
    )
    .unwrap()
}

fn origin(attempt: a::AttemptView) -> WorkerOrigin {
    WorkerOrigin {
        epoch: a::ClusterEpoch(1),
        action: "action".into(),
        execution: attempt.execution,
        attempt: attempt.id,
        worker: attempt.worker,
        incarnation: attempt.incarnation,
    }
}

fn report(
    attempt: a::AttemptView,
    kind: TerminalReportKind,
    result: &a::ResultPayload,
) -> NoGraphReport {
    NoGraphReport {
        origin: origin(attempt),
        kind,
        result: result.clone(),
    }
}

fn current_attempt(state: &PublicationAuthority, id: a::AttemptId) -> a::AttemptView {
    *state
        .scheduler_view()
        .attempts
        .iter()
        .find(|row| row.id == id)
        .unwrap()
}

fn reservation(state: &PublicationAuthority, attempt: a::AttemptView) -> a::ReservationView {
    *state
        .scheduler_view()
        .reservations
        .iter()
        .find(|row| row.id == attempt.reservation)
        .unwrap()
}

fn operation(state: &PublicationAuthority, id: u64) -> a::OperationView {
    state
        .scheduler_view()
        .operations
        .into_iter()
        .find(|row| row.id.0 == id)
        .unwrap()
}

fn recover(state: &mut PublicationAuthority) {
    let bytes = state.snapshot().unwrap();
    let restored = PublicationAuthority::restore(&bytes).unwrap();
    assert_eq!(restored.snapshot().unwrap(), bytes);
    assert_eq!(restored.scheduler_view(), state.scheduler_view());
    assert_eq!(restored.retention_view(), state.retention_view());
    assert_eq!(restored.terminal_reports(), state.terminal_reports());
    assert_eq!(
        restored.terminal_acceptances(),
        state.terminal_acceptances()
    );
    assert_eq!(
        restored.terminal_operation_links(),
        state.terminal_operation_links()
    );
    *state = restored;
}

fn assert_report_record(
    state: &PublicationAuthority,
    id: u64,
    evidence: &NoGraphReport,
    outcome: &NoGraphOutcome,
) {
    let row = &state.terminal_reports().unwrap()[&id];
    assert_eq!(&row.report, evidence);
    assert_eq!(row.position, state.commit());
    assert_eq!(row.at, state.scheduler_view().now);
    match outcome {
        NoGraphOutcome::TerminalAccepted(terminal) => {
            assert_eq!(
                row.disposition,
                ReportDisposition::Terminal(terminal.clone())
            );
            let accepted = &state.terminal_acceptances().unwrap()[&evidence.origin.execution];
            assert_eq!(accepted.report_id, id);
            assert_eq!(accepted.origin, evidence.origin);
            assert_eq!(&accepted.terminal, terminal);
            assert_eq!(accepted.position, row.position);
            assert_eq!(accepted.at, row.at);
        }
        NoGraphOutcome::RetryScheduled => {
            let target = state
                .scheduler_view()
                .executions
                .into_iter()
                .find(|execution| execution.id == evidence.origin.execution)
                .unwrap()
                .reservation_target;
            assert_eq!(row.disposition, ReportDisposition::Retry { target });
            assert!(
                !state
                    .terminal_acceptances()
                    .unwrap()
                    .contains_key(&evidence.origin.execution)
            );
        }
        NoGraphOutcome::Duplicate => panic!("record assertion requires a new report"),
    }
}

fn no_sink(state: &PublicationAuthority) {
    assert!(
        !state
            .scheduler_view()
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, a::EffectKind::PublishResult { .. }))
    );
}

fn register_memory(state: &mut PublicationAuthority, memory_bytes: u64) {
    let capacity = a::Resources {
        cpu_millis: 20_000,
        memory_bytes,
        slots: 8,
    };
    state
        .scheduler_event(a::Event::Register {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            properties: BTreeMap::from([("OSFamily".into(), "linux".into())]),
            capacity,
            available: capacity,
        })
        .unwrap();
}

fn memory_request(algorithm: u8, memory_kb: u64, invocation: &str) -> a::Request {
    let mut selected = platform();
    selected
        .properties
        .iter_mut()
        .find(|property| property.name == "memory_kb")
        .unwrap()
        .value = memory_kb.to_string();
    let command = Command {
        arguments: vec!["/bin/true".into()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(algorithm, &command)),
        input_root_digest: Some(digest(algorithm, &[])),
        timeout: Some(Duration {
            seconds: 17,
            nanos: 1,
        }),
        platform: Some(selected),
        ..Action::default()
    }
    .encode_to_vec();
    a::Request::from_payload(
        RequestPayload::capture(
            &ExecuteRequest {
                instance_name: "tenant/instance".into(),
                action_digest: Some(digest(algorithm, &action)),
                digest_function: i32::from(algorithm),
                skip_cache_lookup: true,
                ..ExecuteRequest::default()
            },
            Some(&RequestMetadata {
                tool_details: Some(ToolDetails {
                    tool_name: "buck2".into(),
                    tool_version: "fixture".into(),
                }),
                tool_invocation_id: invocation.into(),
                ..RequestMetadata::default()
            }),
            &Timestamp {
                seconds: 123,
                nanos: 456,
            },
            &context().principal,
            &action,
            &command,
            MAX_PAYLOAD,
        )
        .unwrap(),
    )
    .unwrap()
}

fn active(
    algorithm: u8,
    memory_kb: u64,
    capacity: u64,
) -> (PublicationAuthority, a::Request, a::AttemptView) {
    let original = memory_request(algorithm, memory_kb, "terminal-invocation");
    let mut state = real(&original);
    state.enable_terminal_outcomes().unwrap();
    register_memory(&mut state, capacity);
    state
        .admit_request(1, "action".into(), &context(), original.clone(), 100)
        .unwrap();
    let attempt = assign(&mut state);
    (state, original, attempt)
}

fn memory_kind(state: &PublicationAuthority, attempt: a::AttemptView) -> TerminalReportKind {
    TerminalReportKind::MemoryFailure {
        charged: reservation(state, attempt).charged,
        peak_memory_bytes: None,
        quiescent: true,
    }
}

fn cleanup(state: &mut PublicationAuthority, attempt: a::AttemptView) {
    state
        .scheduler_event(a::Event::Cleanup {
            attempt: attempt.id,
            worker: attempt.worker,
            incarnation: attempt.incarnation,
        })
        .unwrap();
}

fn unknown_graph_stage(
    state: &mut PublicationAuthority,
    algorithm: u8,
    attempt: a::AttemptView,
    id: u64,
) -> Member {
    let result = ActionResult {
        stdout_raw: format!("unconfirmed-{id}").into_bytes().into(),
        ..ActionResult::default()
    };
    let bytes = result.encode_to_vec();
    let object = Object::for_bytes(algorithm, &bytes).unwrap();
    let member = Member {
        object,
        key: key(id),
        role: Role::Result,
    };
    state
        .install_physical_for(Some(attempt.execution), member.key, object, false)
        .unwrap();
    state
        .stage(
            id,
            Candidate {
                action: "action".into(),
                publisher: worker(attempt),
                result: object,
                result_bytes: bytes,
                response: a::ResultPayload::from_response(
                    &ExecuteResponse {
                        result: Some(result),
                        ..ExecuteResponse::default()
                    },
                    MAX_PAYLOAD,
                )
                .unwrap(),
                closure: BTreeSet::from([member.clone()]),
            },
            state.scheduler_view().now + 10,
            BTreeMap::from([(id, member.key)]),
        )
        .unwrap();
    member
}

#[test]
fn no_graph_complete_preserves_exact_real_request_matrix_without_cas_roots() {
    for algorithm in [1, 9] {
        for skip in [false, true] {
            for no_cache in [false, true] {
                let original = request(
                    algorithm,
                    skip,
                    no_cache,
                    "terminal-invocation",
                    &context().principal,
                    false,
                );
                let mut state = real(&original);
                assert!(state.terminal_reports().is_none());
                let old = state.snapshot().unwrap();
                assert!(
                    String::from_utf8(old.clone())
                        .unwrap()
                        .starts_with("{\"version\":2,")
                );
                state.enable_terminal_outcomes().unwrap();
                assert!(
                    String::from_utf8(state.snapshot().unwrap())
                        .unwrap()
                        .starts_with("{\"version\":4,")
                );
                register_real(&mut state);
                let execution = state
                    .admit_request(1, "action".into(), &context(), original.clone(), 100)
                    .unwrap();
                let second_execution = state
                    .admit_request(2, "action".into(), &context(), original.clone(), 100)
                    .unwrap();
                assert_eq!(execution == second_execution, !no_cache);
                let attempt = assign(&mut state);
                let payload = diagnostic("complete is not memory classification");
                let evidence = report(attempt, TerminalReportKind::Complete, &payload);
                let retention = state.retention_view();
                recover(&mut state);
                assert_eq!(
                    state.report_no_graph(1, evidence.clone()).unwrap(),
                    NoGraphOutcome::TerminalAccepted(a::TerminalResult::Completed(payload.clone()))
                );
                assert_report_record(
                    &state,
                    1,
                    &evidence,
                    &NoGraphOutcome::TerminalAccepted(a::TerminalResult::Completed(
                        payload.clone(),
                    )),
                );
                assert_eq!(
                    terminal(&state, 1),
                    a::TerminalResult::Completed(payload.clone())
                );
                if no_cache {
                    assert!(operation(&state, 2).terminal.is_none());
                    assert_eq!(
                        state.terminal_operation_links().unwrap(),
                        &BTreeMap::from([(1, execution)])
                    );
                    let separate = assign(&mut state);
                    assert_eq!(separate.execution, second_execution);
                    assert_eq!(
                        state
                            .report_no_graph(
                                2,
                                report(separate, TerminalReportKind::Complete, &payload)
                            )
                            .unwrap(),
                        NoGraphOutcome::TerminalAccepted(a::TerminalResult::Completed(
                            payload.clone()
                        ))
                    );
                    assert!(reservation(&state, separate).released);
                }
                assert_eq!(
                    terminal(&state, 2),
                    a::TerminalResult::Completed(payload.clone())
                );
                assert_eq!(state.scheduler_view().executions[0].request, original);
                assert_eq!(state.retention_view(), retention);
                assert!(state.heads().is_empty());
                assert!(state.accepted().is_empty());
                assert_eq!(
                    state.terminal_acceptances().unwrap().len(),
                    if no_cache { 2 } else { 1 }
                );
                assert!(
                    state
                        .terminal_acceptances()
                        .unwrap()
                        .contains_key(&execution)
                );
                assert_eq!(
                    state.terminal_operation_links().unwrap(),
                    &BTreeMap::from([(1, execution), (2, second_execution)])
                );
                assert_eq!(
                    state.terminal_reports().unwrap().len(),
                    if no_cache { 2 } else { 1 }
                );
                assert!(reservation(&state, attempt).released);
                no_sink(&state);
                recover(&mut state);
                let before = state.snapshot().unwrap();
                assert_eq!(
                    state.report_no_graph(1, evidence.clone()).unwrap(),
                    NoGraphOutcome::Duplicate
                );
                assert_eq!(state.snapshot().unwrap(), before);
                let mut conflict = evidence;
                conflict.result = diagnostic("conflicting reuse");
                assert!(state.report_no_graph(1, conflict).is_err());
                assert_eq!(state.snapshot().unwrap(), before);
                state
                    .scheduler_event(a::Event::WorkerUnavailable {
                        worker: attempt.worker,
                        incarnation: attempt.incarnation,
                        session: a::SessionId(1),
                    })
                    .unwrap();
                assert_eq!(
                    terminal(&state, 1),
                    a::TerminalResult::Completed(payload.clone())
                );
                assert_eq!(terminal(&state, 2), a::TerminalResult::Completed(payload));
                assert_eq!(
                    state.terminal_acceptances().unwrap().len(),
                    if no_cache { 2 } else { 1 }
                );
                recover(&mut state);
                assert!(
                    PublicationAuthority::restore(&old)
                        .unwrap()
                        .terminal_reports()
                        .is_none()
                );
            }
        }
    }
}

#[test]
fn explicit_memory_reports_choose_all_actual_final_reasons_and_preserve_refunds() {
    for algorithm in [1, 9] {
        for (memory_kb, capacity, reason) in [
            (0, 32_768, a::ResourceFailureReason::MissingGrowthEvidence),
            (3, 3_072, a::ResourceFailureReason::NoLargerEligibleWorker),
        ] {
            let (mut state, original, attempt) = active(algorithm, memory_kb, capacity);
            let payload = diagnostic("classified memory exhaustion");
            let evidence = report(attempt, memory_kind(&state, attempt), &payload);
            let retention = state.retention_view();
            let expected = a::TerminalResult::ResourceFailed {
                result: payload,
                reason,
            };
            assert_eq!(
                state.report_no_graph(1, evidence.clone()).unwrap(),
                NoGraphOutcome::TerminalAccepted(expected.clone())
            );
            assert_report_record(
                &state,
                1,
                &evidence,
                &NoGraphOutcome::TerminalAccepted(expected.clone()),
            );
            assert_eq!(terminal(&state, 1), expected);
            assert_eq!(state.scheduler_view().executions[0].request, original);
            assert!(reservation(&state, attempt).released);
            assert_eq!(
                state.scheduler_view().workers[0].charged,
                a::Resources::default()
            );
            assert_eq!(state.retention_view(), retention);
            assert!(state.heads().is_empty());
            assert!(state.accepted().is_empty());
            recover(&mut state);
            let before = state.snapshot().unwrap();
            assert_eq!(
                state.report_no_graph(1, evidence).unwrap(),
                NoGraphOutcome::Duplicate
            );
            assert_eq!(state.snapshot().unwrap(), before);
            cleanup(&mut state, attempt);
            assert_eq!(state.snapshot().unwrap(), before);
            no_sink(&state);
        }
        let (mut state, original, mut attempt) = active(algorithm, 3, 32_768);
        let payload = diagnostic("retry then exact attempt-budget failure");
        let mut first_report = None;
        for id in 1..=3 {
            let old_charge = reservation(&state, attempt).charged;
            let evidence = report(attempt, memory_kind(&state, attempt), &payload);
            first_report.get_or_insert_with(|| evidence.clone());
            let outcome = state.report_no_graph(id, evidence.clone()).unwrap();
            assert_report_record(&state, id, &evidence, &outcome);
            assert!(reservation(&state, attempt).released);
            assert_eq!(
                state.scheduler_view().workers[0].charged,
                a::Resources::default()
            );
            assert_eq!(state.scheduler_view().executions[0].request, original);
            assert_eq!(
                state.terminal_reports().unwrap().len(),
                usize::try_from(id).unwrap()
            );
            if id < 3 {
                assert_eq!(outcome, NoGraphOutcome::RetryScheduled);
                assert!(operation(&state, 1).terminal.is_none());
                assert!(state.terminal_acceptances().unwrap().is_empty());
                assert!(state.terminal_operation_links().unwrap().is_empty());
                let target = state.scheduler_view().executions[0].reservation_target;
                assert_eq!(target.memory_bytes, old_charge.memory_bytes * 2);
                assert_eq!(target.cpu_millis, old_charge.cpu_millis);
                assert_eq!(target.slots, old_charge.slots);
                recover(&mut state);
                let next = assign(&mut state);
                assert_ne!(next.id, attempt.id);
                assert_eq!(reservation(&state, next).charged, target);
                let before = state.snapshot().unwrap();
                assert_eq!(
                    state
                        .report_no_graph(1, first_report.clone().unwrap())
                        .unwrap(),
                    NoGraphOutcome::Duplicate
                );
                cleanup(&mut state, attempt);
                assert_eq!(state.snapshot().unwrap(), before);
                assert_eq!(state.scheduler_view().workers[0].charged, target);
                attempt = next;
            } else {
                let expected = a::TerminalResult::ResourceFailed {
                    result: payload.clone(),
                    reason: a::ResourceFailureReason::AttemptBudgetExhausted,
                };
                assert_eq!(outcome, NoGraphOutcome::TerminalAccepted(expected.clone()));
                assert_eq!(terminal(&state, 1), expected);
                assert_eq!(state.terminal_acceptances().unwrap().len(), 1);
            }
            assert!(state.heads().is_empty());
            assert!(state.retention_view().roots.is_empty());
            no_sink(&state);
            recover(&mut state);
        }
    }
}

#[test]
fn no_graph_report_refuses_graph_identity_and_false_quiescence_atomically() {
    for algorithm in [1, 9] {
        let (mut state, _, attempt) = active(algorithm, 3, 32_768);
        let payload = diagnostic("refusal");
        let valid = report(attempt, TerminalReportKind::Complete, &payload);
        let mut variants = Vec::new();
        let mut foreign = valid.clone();
        foreign.origin.epoch = a::ClusterEpoch(2);
        variants.push(foreign);
        let mut foreign = valid.clone();
        foreign.origin.action = "foreign".into();
        variants.push(foreign);
        let mut foreign = valid.clone();
        foreign.origin.execution = a::ExecutionId(99);
        variants.push(foreign);
        let mut foreign = valid.clone();
        foreign.origin.attempt = a::AttemptId(99);
        variants.push(foreign);
        let mut foreign = valid.clone();
        foreign.origin.worker = a::WorkerId(99);
        variants.push(foreign);
        let mut foreign = valid.clone();
        foreign.origin.incarnation = a::IncarnationId(99);
        variants.push(foreign);
        let charged = reservation(&state, attempt).charged;
        for wrong_charge in [
            a::Resources {
                cpu_millis: charged.cpu_millis + 1,
                ..charged
            },
            a::Resources {
                memory_bytes: charged.memory_bytes + 1,
                ..charged
            },
            a::Resources {
                slots: charged.slots + 1,
                ..charged
            },
        ] {
            variants.push(report(
                attempt,
                TerminalReportKind::MemoryFailure {
                    charged: wrong_charge,
                    peak_memory_bytes: None,
                    quiescent: true,
                },
                &payload,
            ));
        }
        variants.push(report(
            attempt,
            TerminalReportKind::MemoryFailure {
                charged,
                peak_memory_bytes: None,
                quiescent: false,
            },
            &payload,
        ));
        for exit in [0, 137] {
            for code in [0, 13] {
                let graph = a::ResultPayload::from_response(
                    &ExecuteResponse {
                        result: Some(ActionResult {
                            exit_code: exit,
                            ..ActionResult::default()
                        }),
                        status: Some(Status {
                            code,
                            ..Status::default()
                        }),
                        ..ExecuteResponse::default()
                    },
                    MAX_PAYLOAD,
                )
                .unwrap();
                variants.push(report(attempt, TerminalReportKind::Complete, &graph));
                variants.push(report(attempt, memory_kind(&state, attempt), &graph));
            }
        }
        let mut corrupt = valid.clone();
        corrupt.result.exit_code = 1;
        variants.push(corrupt);
        let before = state.snapshot().unwrap();
        for rejected in variants {
            assert!(state.report_no_graph(1, rejected).is_err());
            assert_eq!(state.snapshot().unwrap(), before);
        }
        assert_eq!(
            state.report_no_graph(1, valid).unwrap(),
            NoGraphOutcome::TerminalAccepted(a::TerminalResult::Completed(payload))
        );
        recover(&mut state);
    }
}

#[test]
fn joined_cancel_and_late_cancel_preserve_each_operations_original_terminal() {
    for algorithm in [1, 9] {
        for cancel_first in [false, true] {
            let (mut state, original, attempt) = active(algorithm, 3, 32_768);
            assert_eq!(
                state
                    .admit_request(2, "action".into(), &context(), original, 100)
                    .unwrap(),
                attempt.execution
            );
            if cancel_first {
                state
                    .scheduler_event(a::Event::Cancel {
                        operation: a::OperationId(1),
                    })
                    .unwrap();
                assert_eq!(terminal(&state, 1), a::TerminalResult::Cancelled);
            }
            let canceled = operation(&state, 1);
            let payload = diagnostic("joined terminal");
            state
                .report_no_graph(1, report(attempt, TerminalReportKind::Complete, &payload))
                .unwrap();
            let expected = a::TerminalResult::Completed(payload);
            assert_eq!(terminal(&state, 2), expected);
            if cancel_first {
                assert_eq!(operation(&state, 1), canceled);
                assert!(!state.terminal_operation_links().unwrap().contains_key(&1));
            } else {
                assert_eq!(terminal(&state, 1), expected);
                assert_eq!(
                    state.terminal_operation_links().unwrap().get(&1),
                    Some(&attempt.execution)
                );
            }
            assert_eq!(
                state.terminal_operation_links().unwrap().get(&2),
                Some(&attempt.execution)
            );
            let before = state.snapshot().unwrap();
            assert_eq!(
                state
                    .scheduler_event(a::Event::Cancel {
                        operation: a::OperationId(2)
                    })
                    .unwrap(),
                a::Reply::Ignored
            );
            assert_eq!(state.snapshot().unwrap(), before);
            recover(&mut state);
        }
    }
}

#[test]
fn last_cancel_keeps_live_charge_and_unknown_writes_until_actual_cleanup() {
    for algorithm in [1, 9] {
        let original = memory_request(algorithm, 3, "queued-cancel");
        let mut queued = real(&original);
        queued.enable_terminal_outcomes().unwrap();
        queued
            .admit_request(1, "action".into(), &context(), original, 100)
            .unwrap();
        queued
            .scheduler_event(a::Event::Cancel {
                operation: a::OperationId(1),
            })
            .unwrap();
        assert_eq!(terminal(&queued, 1), a::TerminalResult::Cancelled);
        assert!(queued.scheduler_view().attempts.is_empty());
        assert!(queued.terminal_acceptances().unwrap().is_empty());
        recover(&mut queued);

        let (mut state, _, attempt) = active(algorithm, 3, 32_768);
        let member = unknown_graph_stage(&mut state, algorithm, attempt, 1);
        let stage_before = state.stages()[&1].clone();
        let retention = state.retention_view();
        let charged = reservation(&state, attempt).charged;
        state
            .scheduler_event(a::Event::Cancel {
                operation: a::OperationId(1),
            })
            .unwrap();
        assert_eq!(terminal(&state, 1), a::TerminalResult::Cancelled);
        assert!(!reservation(&state, attempt).released);
        assert_eq!(state.scheduler_view().workers[0].charged, charged);
        assert_eq!(state.retention_view(), retention);
        assert_eq!(state.stages()[&1], stage_before);
        assert!(state.scheduler_view().effects.iter().any(|effect| matches!(effect.kind, a::EffectKind::Cancel { attempt: id, .. } if id == attempt.id)));
        assert!(state.terminal_acceptances().unwrap().is_empty());
        recover(&mut state);
        let before = state.snapshot().unwrap();
        assert!(
            state
                .report_no_graph(
                    1,
                    report(
                        attempt,
                        TerminalReportKind::Complete,
                        &diagnostic("late canceled worker")
                    )
                )
                .is_err()
        );
        assert_eq!(state.snapshot().unwrap(), before);
        cleanup(&mut state, attempt);
        assert!(reservation(&state, attempt).released);
        assert_eq!(
            state.scheduler_view().workers[0].charged,
            a::Resources::default()
        );
        assert!(!state.scheduler_view().effects.iter().any(|effect| matches!(effect.kind, a::EffectKind::Cancel { attempt: id, .. } if id == attempt.id)));
        let before = state.snapshot().unwrap();
        cleanup(&mut state, attempt);
        assert_eq!(state.snapshot().unwrap(), before);
        state.advance(11).unwrap();
        assert_eq!(state.stages()[&1].writes, stage_before.writes);
        state
            .observe(1, 1, member.key, member.object, true)
            .unwrap();
        assert_eq!(terminal(&state, 1), a::TerminalResult::Cancelled);
        assert!(state.heads().is_empty());
        assert!(state.terminal_acceptances().unwrap().is_empty());
        assert_eq!(state.stages()[&1].outcomes.get(&1), Some(&true));
        recover(&mut state);
    }
}

#[test]
fn interest_expiry_is_generation_fenced_and_advance_does_not_cancel() {
    for algorithm in [1, 9] {
        let (mut state, _, attempt) = active(algorithm, 3, 32_768);
        let old = operation(&state, 1).interest_deadline;
        state.advance(1).unwrap();
        state
            .scheduler_event(a::Event::RenewInterest {
                operation: a::OperationId(1),
            })
            .unwrap();
        let renewed = operation(&state, 1).interest_deadline;
        assert!(renewed.generation > old.generation);
        assert!(renewed.at > old.at);
        let before = state.snapshot().unwrap();
        assert!(
            state
                .scheduler_event(a::Event::ExpireInterest {
                    operation: a::OperationId(1),
                    generation: renewed.generation
                })
                .is_err()
        );
        assert_eq!(state.snapshot().unwrap(), before);
        state.advance(old.at).unwrap();
        assert!(operation(&state, 1).terminal.is_none());
        let before = state.snapshot().unwrap();
        assert_eq!(
            state
                .scheduler_event(a::Event::ExpireInterest {
                    operation: a::OperationId(1),
                    generation: old.generation
                })
                .unwrap(),
            a::Reply::Ignored
        );
        assert_eq!(state.snapshot().unwrap(), before);
        state.advance(renewed.at).unwrap();
        assert!(operation(&state, 1).terminal.is_none());
        state
            .scheduler_event(a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: renewed.generation,
            })
            .unwrap();
        assert_eq!(terminal(&state, 1), a::TerminalResult::Cancelled);
        assert!(!reservation(&state, attempt).released);
        assert!(state.terminal_acceptances().unwrap().is_empty());
        recover(&mut state);
        cleanup(&mut state, attempt);
        assert_eq!(
            state.scheduler_view().workers[0].charged,
            a::Resources::default()
        );
        recover(&mut state);
    }
}

#[test]
fn progress_and_execution_timers_cannot_revoke_successors_or_refund_live_work() {
    for algorithm in [1, 9] {
        let (mut state, _, mut attempt) = active(algorithm, 3, 32_768);
        let old = current_attempt(&state, attempt.id).deadline;
        state
            .scheduler_event(a::Event::Progress {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
                phase: a::Phase::Running,
                progress: 1,
            })
            .unwrap();
        let running = current_attempt(&state, attempt.id);
        assert!(running.deadline.generation > old.generation);
        let before = state.snapshot().unwrap();
        assert_eq!(
            state
                .scheduler_event(a::Event::ExpireAttempt {
                    attempt: attempt.id,
                    generation: old.generation
                })
                .unwrap(),
            a::Reply::Ignored
        );
        assert_eq!(state.snapshot().unwrap(), before);
        assert!(
            state
                .scheduler_event(a::Event::ExpireAttempt {
                    attempt: attempt.id,
                    generation: running.deadline.generation
                })
                .is_err()
        );
        assert_eq!(state.snapshot().unwrap(), before);
        for number in 1..=3 {
            if number > 1 {
                state
                    .scheduler_event(a::Event::Progress {
                        attempt: attempt.id,
                        worker: attempt.worker,
                        incarnation: attempt.incarnation,
                        phase: a::Phase::Running,
                        progress: 1,
                    })
                    .unwrap();
            }
            let deadline = current_attempt(&state, attempt.id)
                .running_deadline
                .unwrap();
            state.advance(deadline).unwrap();
            assert!(operation(&state, 1).terminal.is_none());
            state
                .scheduler_event(a::Event::ExpireExecution {
                    attempt: attempt.id,
                })
                .unwrap();
            assert!(!reservation(&state, attempt).released);
            assert!(!current_attempt(&state, attempt.id).authorized);
            assert!(state.terminal_acceptances().unwrap().is_empty());
            if number == 3 {
                assert_eq!(terminal(&state, 1), a::TerminalResult::Exhausted);
            } else {
                assert!(operation(&state, 1).terminal.is_none());
            }
            recover(&mut state);
            cleanup(&mut state, attempt);
            if number < 3 {
                let successor = assign(&mut state);
                let before = state.snapshot().unwrap();
                assert_eq!(
                    state
                        .scheduler_event(a::Event::ExpireExecution {
                            attempt: attempt.id
                        })
                        .unwrap(),
                    a::Reply::Ignored
                );
                assert_eq!(state.snapshot().unwrap(), before);
                assert!(current_attempt(&state, successor.id).authorized);
                assert!(!reservation(&state, successor).released);
                attempt = successor;
            }
        }
        assert!(state.heads().is_empty());
        assert!(state.retention_view().roots.is_empty());
        recover(&mut state);
    }
}

#[test]
fn protected_graph_and_unknown_write_survive_a_distinct_no_graph_acceptance() {
    for algorithm in [1, 9] {
        let original = memory_request(algorithm, 3, "protected-graph");
        let mut state = real(&original);
        let binding = ActionBinding::capture(&context(), &original).unwrap();
        state
            .authorize_update(1, &context(), "action", &binding)
            .unwrap();
        register_memory(&mut state, 32_768);
        state
            .admit_request(1, "action".into(), &context(), original, 100)
            .unwrap();
        let first = assign(&mut state);
        let graph = real_candidate(&mut state, algorithm, first);
        let member = graph.closure.first().unwrap().clone();
        let original_terminal = a::TerminalResult::Completed(graph.response.clone());
        stage(&mut state, 1, graph);
        state.publish(1, 1, 100).unwrap();
        state.serve("action", 1, state.commit(), 1, 90).unwrap();
        let version3 = state.snapshot().unwrap();
        assert!(
            String::from_utf8(version3.clone())
                .unwrap()
                .starts_with("{\"version\":3,")
        );
        let old_retention = state.retention_view();
        state.enable_terminal_outcomes().unwrap();
        assert_eq!(state.retention_view(), old_retention);
        assert!(
            String::from_utf8(state.snapshot().unwrap())
                .unwrap()
                .starts_with("{\"version\":4,")
        );
        let old_reader = PublicationAuthority::restore(&version3).unwrap();
        assert!(old_reader.terminal_reports().is_none());
        assert!(old_reader.can_read(1, &member));
        let next_request = memory_request(algorithm, 3, "another-invocation");
        state
            .admit_request(2, "action".into(), &context(), next_request, 100)
            .unwrap();
        let second = assign(&mut state);
        assert_ne!(second.execution, first.execution);
        let unknown = unknown_graph_stage(&mut state, algorithm, second, 2);
        let retention = state.retention_view();
        let prior_stage = state.stages()[&2].clone();
        let payload = diagnostic("no graph after protected graph");
        state
            .report_no_graph(1, report(second, TerminalReportKind::Complete, &payload))
            .unwrap();
        assert_eq!(state.retention_view(), retention);
        assert_eq!(state.stages()[&2], prior_stage);
        assert_eq!(terminal(&state, 1), original_terminal);
        assert_eq!(terminal(&state, 2), a::TerminalResult::Completed(payload));
        assert_eq!(state.heads()["action"], 1);
        assert!(state.can_read(1, &member));
        assert_eq!(
            state.terminal_operation_links().unwrap(),
            &BTreeMap::from([(2, second.execution)])
        );
        recover(&mut state);
        let graph_record = state.accepted()[&1].clone();
        let exact_graph = serde_json::to_string(&graph_record).unwrap();
        let mut wrong_graph = graph_record;
        wrong_graph.stage = 99;
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        assert!(encoded.contains(&exact_graph));
        let corrupt = encoded.replacen(
            &exact_graph,
            &serde_json::to_string(&wrong_graph).unwrap(),
            1,
        );
        assert_eq!(
            PublicationAuthority::restore(corrupt.as_bytes()),
            Err(Error::Corrupt)
        );
        state.lose_index();
        let before = state.snapshot().unwrap();
        state.advance(state.scheduler_view().now).unwrap();
        assert_eq!(state.snapshot().unwrap(), before);
        state.advance(11).unwrap();
        state
            .observe(2, 2, unknown.key, unknown.object, true)
            .unwrap();
        assert_eq!(state.heads()["action"], 1);
        assert_eq!(terminal(&state, 1), original_terminal);
        assert!(state.can_read(1, &member));
        let before = state.snapshot().unwrap();
        assert!(state.publish(2, 2, 100).is_err());
        assert_eq!(state.snapshot().unwrap(), before);
        recover(&mut state);
    }
}

#[test]
fn finite_terminal_capacity_retains_twenty_four_reports_and_sixteen_exact_handles() {
    for algorithm in [1, 9] {
        let first = memory_request(algorithm, 3, "capacity-1");
        let mut state = real(&first);
        state.enable_terminal_outcomes().unwrap();
        register_memory(&mut state, 32_768);
        for execution in 1..=8 {
            let original = memory_request(algorithm, 3, &format!("capacity-{execution}"));
            let operation_id = execution * 2 - 1;
            let id = state
                .admit_request(
                    operation_id,
                    "action".into(),
                    &context(),
                    original.clone(),
                    100,
                )
                .unwrap();
            assert_eq!(
                state
                    .admit_request(operation_id + 1, "action".into(), &context(), original, 100)
                    .unwrap(),
                id
            );
            let payload = diagnostic(&format!("capacity-{execution}"));
            for number in 1..=3 {
                let attempt = assign(&mut state);
                let evidence = report(attempt, memory_kind(&state, attempt), &payload);
                let outcome = state
                    .report_no_graph((execution - 1) * 3 + number, evidence)
                    .unwrap();
                if number < 3 {
                    assert_eq!(outcome, NoGraphOutcome::RetryScheduled);
                } else {
                    let expected = a::TerminalResult::ResourceFailed {
                        result: payload.clone(),
                        reason: a::ResourceFailureReason::AttemptBudgetExhausted,
                    };
                    assert_eq!(outcome, NoGraphOutcome::TerminalAccepted(expected.clone()));
                    assert_eq!(terminal(&state, operation_id), expected);
                    assert_eq!(terminal(&state, operation_id + 1), expected);
                }
                recover(&mut state);
            }
        }
        assert_eq!(state.terminal_reports().unwrap().len(), 24);
        assert_eq!(state.terminal_acceptances().unwrap().len(), 8);
        assert_eq!(state.terminal_operation_links().unwrap().len(), 16);
        assert!(state.heads().is_empty());
        assert!(state.retention_view().roots.is_empty());
        assert_eq!(
            state.scheduler_view().workers[0].charged,
            a::Resources::default()
        );
        let before = state.snapshot().unwrap();
        assert!(
            state
                .admit_request(
                    17,
                    "action".into(),
                    &context(),
                    memory_request(algorithm, 3, "capacity-overflow"),
                    100
                )
                .is_err()
        );
        assert_eq!(state.snapshot().unwrap(), before);
        no_sink(&state);
        recover(&mut state);
    }
}

#[test]
fn staged_retry_expiry_and_final_memory_failure_keep_each_original_write() {
    for algorithm in [1, 9] {
        let (mut state, _, first) = active(algorithm, 3, 32_768);
        let old_member = unknown_graph_stage(&mut state, algorithm, first, 1);
        let old_stage = state.stages()[&1].clone();
        let retention = state.retention_view();
        let payload = diagnostic("staged memory failure");
        let first_report = report(first, memory_kind(&state, first), &payload);
        assert_eq!(
            state.report_no_graph(1, first_report.clone()).unwrap(),
            NoGraphOutcome::RetryScheduled
        );
        assert_eq!(state.stages()[&1], old_stage);
        assert_eq!(state.retention_view(), retention);
        assert!(reservation(&state, first).released);
        recover(&mut state);
        let second = assign(&mut state);
        let next_member = unknown_graph_stage(&mut state, algorithm, second, 2);
        let next_stage = state.stages()[&2].clone();
        let current_charge = reservation(&state, second).charged;
        let before = state.snapshot().unwrap();
        assert_eq!(
            state.report_no_graph(1, first_report).unwrap(),
            NoGraphOutcome::Duplicate
        );
        cleanup(&mut state, first);
        assert_eq!(state.snapshot().unwrap(), before);
        assert_eq!(state.scheduler_view().workers[0].charged, current_charge);
        state
            .observe(1, 1, old_member.key, old_member.object, true)
            .unwrap();
        assert_eq!(state.stages()[&1].writes, old_stage.writes);
        assert_eq!(state.stages()[&1].outcomes.get(&1), Some(&true));
        assert_eq!(state.scheduler_view().workers[0].charged, current_charge);
        assert!(operation(&state, 1).terminal.is_none());
        let deadline = current_attempt(&state, second.id).deadline;
        state.advance(deadline.at).unwrap();
        state
            .scheduler_event(a::Event::ExpireAttempt {
                attempt: second.id,
                generation: deadline.generation,
            })
            .unwrap();
        assert!(!reservation(&state, second).released);
        assert_eq!(state.scheduler_view().workers[0].charged, current_charge);
        assert_eq!(state.stages()[&2], next_stage);
        recover(&mut state);
        cleanup(&mut state, second);
        let third = assign(&mut state);
        let final_member = unknown_graph_stage(&mut state, algorithm, third, 3);
        let retention = state.retention_view();
        let evidence = report(third, memory_kind(&state, third), &payload);
        let expected = a::TerminalResult::ResourceFailed {
            result: payload,
            reason: a::ResourceFailureReason::AttemptBudgetExhausted,
        };
        assert_eq!(
            state.report_no_graph(2, evidence).unwrap(),
            NoGraphOutcome::TerminalAccepted(expected.clone())
        );
        assert_eq!(terminal(&state, 1), expected);
        assert_eq!(state.retention_view(), retention);
        assert_eq!(state.stages().len(), 3);
        assert_eq!(state.stages()[&1].outcomes.get(&1), Some(&true));
        assert!(state.stages()[&2].outcomes.is_empty());
        assert!(state.stages()[&3].outcomes.is_empty());
        assert!(reservation(&state, third).released);
        recover(&mut state);
        state
            .observe(2, 2, next_member.key, next_member.object, true)
            .unwrap();
        state
            .observe(3, 3, final_member.key, final_member.object, true)
            .unwrap();
        assert_eq!(terminal(&state, 1), expected);
        assert!(state.heads().is_empty());
        assert!(state.accepted().is_empty());
        assert_eq!(state.terminal_acceptances().unwrap().len(), 1);
        assert_eq!(
            state.scheduler_view().workers[0].charged,
            a::Resources::default()
        );
        no_sink(&state);
        recover(&mut state);
    }
}

#[test]
fn terminal_snapshot_refuses_forged_report_origin_links_and_classification() {
    for algorithm in [1, 9] {
        let (mut state, _, attempt) = active(algorithm, 3, 32_768);
        let evidence = report(
            attempt,
            TerminalReportKind::Complete,
            &diagnostic("restore relations"),
        );
        state.report_no_graph(1, evidence).unwrap();
        let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
        let accepted = state.terminal_acceptances().unwrap()[&attempt.execution].clone();
        let exact = serde_json::to_string(&accepted).unwrap();
        assert!(encoded.contains(&exact));
        let mut variants = Vec::new();
        let mut wrong = accepted.clone();
        wrong.origin.epoch = a::ClusterEpoch(2);
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.origin.action = "foreign".into();
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.origin.execution = a::ExecutionId(99);
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.origin.attempt = a::AttemptId(99);
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.origin.worker = a::WorkerId(99);
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.origin.incarnation = a::IncarnationId(99);
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.report_id = 99;
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.position = 0;
        variants.push(wrong);
        let mut wrong = accepted.clone();
        wrong.at = state.scheduler_view().now + 1;
        variants.push(wrong);
        let mut wrong = accepted;
        wrong.terminal = a::TerminalResult::Completed(diagnostic("replacement diagnostic"));
        variants.push(wrong);
        for wrong in variants {
            let changed = encoded.replacen(&exact, &serde_json::to_string(&wrong).unwrap(), 1);
            assert_eq!(
                PublicationAuthority::restore(changed.as_bytes()),
                Err(Error::Corrupt)
            );
        }
        let classified = state.terminal_reports().unwrap()[&1].clone();
        let exact = serde_json::to_string(&classified).unwrap();
        assert!(encoded.contains(&exact));
        let mut variants = Vec::new();
        let mut wrong = classified.clone();
        wrong.report.origin.incarnation = a::IncarnationId(99);
        variants.push(wrong);
        let mut wrong = classified.clone();
        wrong.report.result = diagnostic("replacement report");
        variants.push(wrong);
        let mut wrong = classified.clone();
        wrong.position = 0;
        variants.push(wrong);
        let mut wrong = classified.clone();
        wrong.at = state.scheduler_view().now + 1;
        variants.push(wrong);
        let mut wrong = classified.clone();
        wrong.disposition = ReportDisposition::Retry {
            target: reservation(&state, attempt).charged,
        };
        variants.push(wrong);
        let mut wrong = classified;
        wrong.report.kind = memory_kind(&state, attempt);
        variants.push(wrong);
        for wrong in variants {
            let changed = encoded.replacen(&exact, &serde_json::to_string(&wrong).unwrap(), 1);
            assert_eq!(
                PublicationAuthority::restore(changed.as_bytes()),
                Err(Error::Corrupt)
            );
        }
        let links = format!(
            "\"operations\":{}",
            serde_json::to_string(state.terminal_operation_links().unwrap()).unwrap()
        );
        assert!(encoded.contains(&links));
        let changed = encoded.replacen(&links, "\"operations\":{}", 1);
        assert_eq!(
            PublicationAuthority::restore(changed.as_bytes()),
            Err(Error::Corrupt)
        );
        recover(&mut state);
    }
}

#[test]
fn resource_failed_snapshot_cannot_discard_terminal_authority_by_downgrading() {
    let mut accepted_downgrades = Vec::new();
    for algorithm in [1, 9] {
        for with_grant in [false, true] {
            let (mut state, original, attempt) = active(algorithm, 3, 3_072);
            if with_grant {
                let binding = ActionBinding::capture(&context(), &original).unwrap();
                state
                    .authorize_update(1, &context(), "action", &binding)
                    .unwrap();
            }
            let payload = diagnostic("retained resource failure");
            let evidence = report(attempt, memory_kind(&state, attempt), &payload);
            assert_eq!(
                state.report_no_graph(1, evidence).unwrap(),
                NoGraphOutcome::TerminalAccepted(a::TerminalResult::ResourceFailed {
                    result: payload,
                    reason: a::ResourceFailureReason::NoLargerEligibleWorker,
                })
            );
            let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
            let start = encoded.find("\"terminal\":").unwrap();
            let end = start + encoded[start..].find(",\"namespaces\":").unwrap();
            let mut downgraded = format!("{}{}", &encoded[..start], &encoded[end + 1..]);
            assert!(downgraded.contains("\"closed\":true,"));
            downgraded = downgraded.replace("\"closed\":true,", "").replacen(
                "\"version\":4",
                if with_grant {
                    "\"version\":3"
                } else {
                    "\"version\":2"
                },
                1,
            );
            serde_json::from_str::<serde_json::Value>(&downgraded).unwrap();
            if PublicationAuthority::restore(downgraded.as_bytes()) != Err(Error::Corrupt) {
                accepted_downgrades.push(format!("algorithm={algorithm}, with_grant={with_grant}"));
            }
            recover(&mut state);
        }
    }
    assert!(
        accepted_downgrades.is_empty(),
        "ResourceFailed lost its authority ledger: {accepted_downgrades:?}"
    );
}

#[test]
fn memory_report_evidence_and_attempt_sequence_remain_locally_consistent_on_restore() {
    let mut accepted_corruptions = Vec::new();
    for algorithm in [1, 9] {
        let (mut retry, _, first) = active(algorithm, 0, 32_768);
        let payload = diagnostic("positive peak from an uncharged memory request");
        let evidence = report(
            first,
            TerminalReportKind::MemoryFailure {
                charged: reservation(&retry, first).charged,
                peak_memory_bytes: Some(1_024),
                quiescent: true,
            },
            &payload,
        );
        assert_eq!(
            retry.report_no_graph(1, evidence).unwrap(),
            NoGraphOutcome::RetryScheduled
        );
        assert_eq!(
            retry.scheduler_view().executions[0]
                .reservation_target
                .memory_bytes,
            2_048
        );
        recover(&mut retry);
        let encoded = String::from_utf8(retry.snapshot().unwrap()).unwrap();
        let recorded = retry.terminal_reports().unwrap()[&1].clone();
        let exact = serde_json::to_string(&recorded).unwrap();
        let mut without_peak = recorded;
        let TerminalReportKind::MemoryFailure {
            peak_memory_bytes, ..
        } = &mut without_peak.report.kind
        else {
            panic!("memory report")
        };
        *peak_memory_bytes = None;
        assert!(encoded.contains(&exact));
        let corrupt = encoded.replacen(&exact, &serde_json::to_string(&without_peak).unwrap(), 1);
        if PublicationAuthority::restore(corrupt.as_bytes()) != Err(Error::Corrupt) {
            accepted_corruptions.push(format!(
                "algorithm={algorithm}: positive retry peak removed"
            ));
        }

        let (mut missing, _, attempt) = active(algorithm, 0, 32_768);
        let payload = diagnostic("missing memory growth evidence");
        missing
            .report_no_graph(1, report(attempt, memory_kind(&missing, attempt), &payload))
            .unwrap();
        assert_eq!(
            terminal(&missing, 1),
            a::TerminalResult::ResourceFailed {
                result: payload,
                reason: a::ResourceFailureReason::MissingGrowthEvidence
            }
        );
        recover(&mut missing);
        let encoded = String::from_utf8(missing.snapshot().unwrap()).unwrap();
        let recorded = missing.terminal_reports().unwrap()[&1].clone();
        let exact = serde_json::to_string(&recorded).unwrap();
        let mut with_peak = recorded;
        let TerminalReportKind::MemoryFailure {
            peak_memory_bytes, ..
        } = &mut with_peak.report.kind
        else {
            panic!("memory report")
        };
        *peak_memory_bytes = Some(1);
        assert!(encoded.contains(&exact));
        let corrupt = encoded.replacen(&exact, &serde_json::to_string(&with_peak).unwrap(), 1);
        if PublicationAuthority::restore(corrupt.as_bytes()) != Err(Error::Corrupt) {
            accepted_corruptions.push(format!(
                "algorithm={algorithm}: missing-evidence terminal gained a positive peak"
            ));
        }

        let outer: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        let scheduler_bytes: Vec<u8> = serde_json::from_value(outer["scheduler"].clone()).unwrap();
        let scheduler_text = String::from_utf8(scheduler_bytes.clone()).unwrap();
        assert!(scheduler_text.matches("\"MissingGrowthEvidence\"").count() >= 2);
        let changed_scheduler = scheduler_text
            .replace("\"MissingGrowthEvidence\"", "\"AttemptBudgetExhausted\"")
            .into_bytes();
        let scheduler_field = format!(
            "\"scheduler\":{}",
            serde_json::to_string(&scheduler_bytes).unwrap()
        );
        assert!(encoded.contains(&scheduler_field));
        let corrupt = encoded
            .replacen(
                &scheduler_field,
                &format!(
                    "\"scheduler\":{}",
                    serde_json::to_string(&changed_scheduler).unwrap()
                ),
                1,
            )
            .replace("\"MissingGrowthEvidence\"", "\"AttemptBudgetExhausted\"");
        if PublicationAuthority::restore(corrupt.as_bytes()) != Err(Error::Corrupt) {
            accepted_corruptions.push(format!(
                "algorithm={algorithm}: budget exhausted after one attempt"
            ));
        }

        let second = assign(&mut retry);
        assert_eq!(reservation(&retry, second).charged.memory_bytes, 2_048);
        let second_report = report(
            second,
            memory_kind(&retry, second),
            &diagnostic("second retry"),
        );
        assert_eq!(
            retry.report_no_graph(2, second_report).unwrap(),
            NoGraphOutcome::RetryScheduled
        );
        recover(&mut retry);
        let encoded = String::from_utf8(retry.snapshot().unwrap()).unwrap();
        let first = retry.terminal_reports().unwrap()[&1].clone();
        let second = retry.terminal_reports().unwrap()[&2].clone();
        let exact_first = serde_json::to_string(&first).unwrap();
        let exact_second = serde_json::to_string(&second).unwrap();
        assert!(encoded.contains(&exact_first) && encoded.contains(&exact_second));
        let mut wrong_target = first.clone();
        let ReportDisposition::Retry { target } = &mut wrong_target.disposition else {
            panic!("retry target")
        };
        target.memory_bytes = 1_024;
        let corrupt = encoded.replacen(
            &exact_first,
            &serde_json::to_string(&wrong_target).unwrap(),
            1,
        );
        if PublicationAuthority::restore(corrupt.as_bytes()) != Err(Error::Corrupt) {
            accepted_corruptions.push(format!(
                "algorithm={algorithm}: retry target differs from successor grant"
            ));
        }
        let mut reversed_first = first.clone();
        reversed_first.position = second.position;
        let mut reversed_second = second;
        reversed_second.position = first.position;
        let corrupt = encoded
            .replacen(
                &exact_first,
                &serde_json::to_string(&reversed_first).unwrap(),
                1,
            )
            .replacen(
                &exact_second,
                &serde_json::to_string(&reversed_second).unwrap(),
                1,
            );
        if PublicationAuthority::restore(corrupt.as_bytes()) != Err(Error::Corrupt) {
            accepted_corruptions.push(format!(
                "algorithm={algorithm}: report order reverses actual attempt order"
            ));
        }
    }
    assert!(
        accepted_corruptions.is_empty(),
        "locally contradictory memory reports restored: {accepted_corruptions:?}"
    );
}

#[test]
fn retry_records_and_terminal_mode_cannot_be_erased_from_retained_lifecycle() {
    let mut accepted_corruptions = Vec::new();
    for algorithm in [1, 9] {
        let (mut retry, _, attempt) = active(algorithm, 3, 32_768);
        let evidence = report(
            attempt,
            memory_kind(&retry, attempt),
            &diagnostic("retained retry authority"),
        );
        assert_eq!(
            retry.report_no_graph(1, evidence).unwrap(),
            NoGraphOutcome::RetryScheduled
        );
        let encoded = String::from_utf8(retry.snapshot().unwrap()).unwrap();
        let reports = format!(
            "\"reports\":{}",
            serde_json::to_string(retry.terminal_reports().unwrap()).unwrap()
        );
        assert!(encoded.contains(&reports));
        let no_report = encoded.replacen(&reports, "\"reports\":{}", 1);
        if PublicationAuthority::restore(no_report.as_bytes()) != Err(Error::Corrupt) {
            accepted_corruptions.push(format!(
                "algorithm={algorithm}: grown retry lost its report"
            ));
        }
        let (mut canceled, _, attempt) = active(algorithm, 3, 32_768);
        canceled
            .scheduler_event(a::Event::Cancel {
                operation: a::OperationId(1),
            })
            .unwrap();
        assert_eq!(terminal(&canceled, 1), a::TerminalResult::Cancelled);
        assert!(!reservation(&canceled, attempt).released);
        for (label, state) in [("retry", &mut retry), ("last cancellation", &mut canceled)] {
            let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
            let start = encoded.find("\"terminal\":").unwrap();
            let end = start + encoded[start..].find(",\"namespaces\":").unwrap();
            let downgraded = format!("{}{}", &encoded[..start], &encoded[end + 1..])
                .replace("\"closed\":true,", "")
                .replacen("\"version\":4", "\"version\":2", 1);
            serde_json::from_str::<serde_json::Value>(&downgraded).unwrap();
            if PublicationAuthority::restore(downgraded.as_bytes()) != Err(Error::Corrupt) {
                accepted_corruptions
                    .push(format!("algorithm={algorithm}: {label} lost terminal mode"));
            }
            recover(state);
        }
    }
    assert!(
        accepted_corruptions.is_empty(),
        "retained lifecycle lost its authority: {accepted_corruptions:?}"
    );
}

#[test]
fn repeated_write_observation_keeps_v4_preparation_and_preserves_legacy_behavior() {
    let mut regressions = Vec::new();
    for algorithm in [1, 9] {
        for version in [2, 3, 4] {
            let original = memory_request(algorithm, 3, "write-observation");
            let mut state = real(&original);
            if version >= 3 {
                let binding = ActionBinding::capture(&context(), &original).unwrap();
                state
                    .authorize_update(1, &context(), "action", &binding)
                    .unwrap();
            }
            if version == 4 {
                state.enable_terminal_outcomes().unwrap();
            }
            register_memory(&mut state, 32_768);
            state
                .admit_request(1, "action".into(), &context(), original, 100)
                .unwrap();
            let attempt = assign(&mut state);
            let member = unknown_graph_stage(&mut state, algorithm, attempt, 1);
            state
                .observe(1, 1, member.key, member.object, true)
                .unwrap();
            prepare(&mut state, 1);
            let before = state.snapshot().unwrap();
            assert!(
                String::from_utf8(before.clone())
                    .unwrap()
                    .starts_with(&format!("{{\"version\":{version},"))
            );
            assert!(state.observe(1, 1, key(99), member.object, true).is_err());
            let mut wrong_object = member.object;
            wrong_object.hash[0] ^= 1;
            assert!(state.observe(1, 1, member.key, wrong_object, true).is_err());
            assert!(
                state
                    .observe(1, 1, member.key, member.object, false)
                    .is_err()
            );
            assert_eq!(state.snapshot().unwrap(), before);
            state
                .observe(1, 1, member.key, member.object, true)
                .unwrap();
            if version == 4 {
                if state.snapshot().unwrap() != before {
                    regressions.push(format!(
                        "algorithm={algorithm}: duplicate changed authoritative snapshot"
                    ));
                }
                if state.publish(1, 1, 100).is_err() {
                    regressions.push(format!(
                        "algorithm={algorithm}: duplicate invalidated prepared publication"
                    ));
                }
            } else {
                assert_ne!(state.snapshot().unwrap(), before);
                assert!(state.publish(1, 1, 100).is_err());
                prepare(&mut state, 1);
                state.publish(1, 1, 100).unwrap();
            }
            recover(&mut state);
        }
    }
    assert!(
        regressions.is_empty(),
        "duplicate observation regressed: {regressions:?}"
    );
}

#[test]
fn retained_report_time_cannot_follow_a_later_commit_on_another_execution() {
    let mut accepted_corruptions = Vec::new();
    for algorithm in [1, 9] {
        for later_graph in [false, true] {
            let (mut state, _, first) = active(algorithm, 3, 32_768);
            let evidence = report(
                first,
                memory_kind(&state, first),
                &diagnostic("earlier retry"),
            );
            assert_eq!(
                state.report_no_graph(1, evidence).unwrap(),
                NoGraphOutcome::RetryScheduled
            );
            let successor = assign(&mut state);
            assert_eq!(successor.execution, first.execution);
            let other_request = memory_request(algorithm, 3, "other-execution");
            let other_execution = state
                .admit_request(2, "action".into(), &context(), other_request, 100)
                .unwrap();
            assert_ne!(other_execution, first.execution);
            let other = assign(&mut state);
            assert_eq!(other.execution, other_execution);
            state.advance(1).unwrap();
            if later_graph {
                let value = real_candidate(&mut state, algorithm, other);
                stage(&mut state, 1, value);
                state.publish(1, 1, 100).unwrap();
                assert_eq!(state.accepted()[&1].at, 1);
            } else {
                state
                    .report_no_graph(
                        2,
                        report(
                            other,
                            TerminalReportKind::Complete,
                            &diagnostic("later terminal"),
                        ),
                    )
                    .unwrap();
                assert_eq!(state.terminal_reports().unwrap()[&2].at, 1);
            }
            state.advance(2).unwrap();
            recover(&mut state);
            let encoded = String::from_utf8(state.snapshot().unwrap()).unwrap();
            let recorded = state.terminal_reports().unwrap()[&1].clone();
            assert_eq!(recorded.at, 0);
            let exact = serde_json::to_string(&recorded).unwrap();
            let mut later_time = recorded;
            later_time.at = 2;
            assert!(encoded.contains(&exact));
            let corrupt = encoded.replacen(&exact, &serde_json::to_string(&later_time).unwrap(), 1);
            if PublicationAuthority::restore(corrupt.as_bytes()) != Err(Error::Corrupt) {
                accepted_corruptions
                    .push(format!("algorithm={algorithm}, later_graph={later_graph}"));
            }
        }
    }
    assert!(
        accepted_corruptions.is_empty(),
        "commit/time order reversed across executions: {accepted_corruptions:?}"
    );
}
