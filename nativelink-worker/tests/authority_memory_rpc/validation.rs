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

use super::*;

fn assert_unchanged(before: &ObservableState, after: &ObservableState) {
    assert_eq!(after.attempts, before.attempts);
    assert_eq!(after.reservations, before.reservations);
    assert_eq!(after.executions, before.executions);
    assert_eq!(after.effects, before.effects);
    assert_eq!(after.workers[0].charged, before.workers[0].charged);
}

#[nativelink_test]
async fn memory_rpc_malformed_evidence_cannot_acknowledge_release_decline_or_grow() {
    let (context, contents) = context().await;
    let request = request(&context, DigestHasherFunc::Sha256, 128 * MIB, 1, false).await;
    let mut runtime = Runtime::start_history(
        &context,
        Arc::new(AtomicU64::new(0)),
        roundtrip::faults(),
        None,
    )
    .await;
    let mut connection = Connection::open(&runtime, &context, 1, CAPACITY.memory_bytes).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let report = launch(&context, &connection, command, &request, &contents).await;
    let before = connection.barrier(&runtime).await;
    assert_memory_report(&report, &before, 128 * MIB);
    assert_eq!(before.attempts[0].phase, Phase::Dispatching);
    for case in 0..17 {
        let mut invalid = report.clone();
        match case {
            0 => invalid.memory_breach.as_mut().unwrap().reservation_id += 1,
            1 => invalid.memory_breach.as_mut().unwrap().charged = None,
            2 => {
                invalid
                    .memory_breach
                    .as_mut()
                    .unwrap()
                    .charged
                    .as_mut()
                    .unwrap()
                    .memory_bytes += 1;
            }
            3 => {
                invalid
                    .memory_breach
                    .as_mut()
                    .unwrap()
                    .charged
                    .as_mut()
                    .unwrap()
                    .cpu_millis += 1;
            }
            4 => {
                invalid
                    .memory_breach
                    .as_mut()
                    .unwrap()
                    .charged
                    .as_mut()
                    .unwrap()
                    .slots += 1;
            }
            5 => {
                invalid
                    .memory_breach
                    .as_mut()
                    .unwrap()
                    .charged
                    .as_mut()
                    .unwrap()
                    .memory_bytes = 0;
            }
            6 => invalid.memory_breach.as_mut().unwrap().limit_kb = 0,
            7 => invalid.memory_breach.as_mut().unwrap().limit_kb = (128 * MIB / 1024) - 1,
            8 => {
                let evidence = invalid.memory_breach.as_mut().unwrap();
                evidence.observed_kb = evidence.limit_kb;
            }
            9 => invalid.phase = AuthorityAttemptPhase::Running as i32,
            10 => invalid.phase = AuthorityAttemptPhase::TerminationUncertain as i32,
            11 => invalid.result = None,
            12 => {
                invalid.result = Some(authority_attempt_report::Result::InternalError(
                    nativelink_proto::google::rpc::Status {
                        code: tonic::Code::Internal as i32,
                        message: "upload failed".into(),
                        details: Vec::new(),
                    },
                ));
            }
            13 => {
                invalid.result = Some(authority_attempt_report::Result::Decline(
                    AuthorityDecline::default(),
                ));
            }
            14 => {
                invalid.result = Some(authority_attempt_report::Result::ExecuteResponse(
                    ExecuteResponse {
                        result: Some(ActionResult::default()),
                        ..ExecuteResponse::default()
                    },
                ));
            }
            15 => {
                invalid.result = Some(authority_attempt_report::Result::ExecuteResponse(
                    ExecuteResponse {
                        status: captured(&report).status.clone(),
                        ..ExecuteResponse::default()
                    },
                ));
            }
            16 => {
                let Some(authority_attempt_report::Result::ExecuteResponse(response)) =
                    invalid.result.as_mut()
                else {
                    unreachable!()
                };
                response.message = "candidate".repeat(4000);
                assert!(invalid.encoded_len() < 64 * 1024);
            }
            _ => unreachable!(),
        }
        connection.report(invalid).await;
        let after = connection.barrier(&runtime).await;
        assert_unchanged(&before, &after);
        roundtrip::assert_retry_has_no_history(&runtime, &after);
    }
    connection.report(report).await;
    let retry = runtime.wait(|state| state.attempts.len() == 2).await;
    assert_eq!(
        retry.executions[0].reservation_target.memory_bytes,
        256 * MIB
    );
    roundtrip::assert_retry_has_no_history(&runtime, &retry);
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}

async fn rejected_registration(runtime: &Runtime, registration: AuthorityRegistration) {
    let incoming = stream::iter([AuthorityWorkerUpdate {
        update: Some(authority_worker_update::Update::Registration(registration)),
    }]);
    let mut client = AuthorityWorkerApiClient::new(runtime.worker_channel.clone());
    let error = client.connect(incoming).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[nativelink_test]
async fn memory_rpc_v2_and_capability_are_required_before_register_or_inventory() {
    let (context, contents) = context().await;
    let request = request(&context, DigestHasherFunc::Sha256, 128 * MIB, 1, false).await;
    let mut runtime = Runtime::start_history(
        &context,
        Arc::new(AtomicU64::new(0)),
        roundtrip::faults(),
        None,
    )
    .await;
    let mut connection = Connection::open(&runtime, &context, 1, CAPACITY.memory_bytes).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let report = launch(&context, &connection, command, &request, &contents).await;
    let before = connection.barrier(&runtime).await;
    assert_memory_report(&report, &before, 128 * MIB);
    for case in 0..3 {
        let mut registration = context.worker.make_registration(2, Vec::new()).unwrap();
        assert_eq!(
            registration.reconciliation.as_ref().unwrap().attempts,
            vec![report.clone()]
        );
        match case {
            0 => registration.protocol_version = 1,
            1 => registration.protocol_version = PROTOCOL_VERSION + 1,
            2 => registration.capabilities.retain(|value| *value != 7),
            _ => unreachable!(),
        }
        rejected_registration(&runtime, registration).await;
        let after = connection.barrier(&runtime).await;
        assert_unchanged(&before, &after);
        assert_eq!(after.workers[0].session, before.workers[0].session);
        roundtrip::assert_retry_has_no_history(&runtime, &after);
    }
    drop(connection);
    context.worker.disconnect(1);
    let connection = Connection::open(&runtime, &context, 2, CAPACITY.memory_bytes).await;
    let retry = runtime.wait(|state| state.attempts.len() == 2).await;
    assert_eq!(
        retry.executions[0].reservation_target.memory_bytes,
        256 * MIB
    );
    roundtrip::assert_retry_has_no_history(&runtime, &retry);
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn memory_rpc_upload_failure_has_no_marker_or_manufactured_graph() {
    let (context, contents) = context().await;
    let request = request(&context, DigestHasherFunc::Sha256, 128 * MIB, 1, true).await;
    let mut runtime = Runtime::start_history(
        &context,
        Arc::new(AtomicU64::new(0)),
        roundtrip::faults(),
        None,
    )
    .await;
    let mut connection = Connection::open(&runtime, &context, 1, CAPACITY.memory_bytes).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let attempt = start(&context, &connection, command, &request, &contents).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !context.directory.path().join("memory-ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let temp = context.directory.path().join("temp");
    let hidden = context.directory.path().join("temp-unavailable");
    std::fs::rename(&temp, &hidden).unwrap();
    std::fs::write(context.directory.path().join("memory-release"), b"go").unwrap();
    let report = candidate(&context, attempt).await;
    std::fs::rename(&hidden, &temp).unwrap();
    // Upload failed before ordinary resource_usage was finalized. The actual
    // sampler/kill log witnesses the policy stop independently of the error.
    assert!(logs_contain(
        "Action exceeded its memory reservation, killed"
    ));
    assert!(logs_contain("outcome=\"KILLED_MEMORY\""));
    assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
    assert!(report.memory_breach.is_none());
    assert!(matches!(
        report.result,
        Some(authority_attempt_report::Result::InternalError(_))
    ));
    connection.report(report).await;
    terminal(&mut operations).await;
    let state = runtime
        .wait(|state| state.executions[0].terminal.is_some())
        .await;
    assert!(matches!(
        state.executions[0].terminal,
        Some(TerminalResult::Completed(_))
    ));
    assert_eq!(state.executions[0].attempts_started, 1);
    assert_eq!(
        state.executions[0].reservation_target.memory_bytes,
        128 * MIB
    );
    assert_eq!(context.ac.len_for_test(), 0);
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn memory_rpc_oversize_recipient_fallback_clears_explicit_memory_evidence() {
    // The bounded recipient accepts the small ExecuteRequest while the result
    // grows beyond its 8 KiB limit. The Command still fits coordinator admission.
    let (context, contents) = context_with_payload_bound(8 * 1024).await;
    let request = request(&context, DigestHasherFunc::Sha256, 128 * MIB, 50, false).await;
    let mut runtime = Runtime::start_history(
        &context,
        Arc::new(AtomicU64::new(0)),
        roundtrip::faults(),
        None,
    )
    .await;
    let mut connection = Connection::open(&runtime, &context, 1, CAPACITY.memory_bytes).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let report = launch(&context, &connection, command, &request, &contents).await;
    assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
    assert!(report.memory_breach.is_none());
    let Some(authority_attempt_report::Result::InternalError(status)) = &report.result else {
        panic!("Expected bounded overflow fallback");
    };
    assert_eq!(status.code, tonic::Code::ResourceExhausted as i32);
    assert!(status.message.contains("bound"));
    assert!(logs_contain(
        "Action exceeded its memory reservation, killed"
    ));
    assert!(logs_contain("outcome=\"KILLED_MEMORY\""));
    connection.report(report).await;
    terminal(&mut operations).await;
    let state = runtime
        .wait(|state| state.executions[0].terminal.is_some())
        .await;
    assert!(matches!(
        state.executions[0].terminal,
        Some(TerminalResult::Completed(_))
    ));
    assert_eq!(state.executions[0].attempts_started, 1);
    assert_eq!(
        state.executions[0].reservation_target.memory_bytes,
        128 * MIB
    );
    assert_eq!(context.ac.len_for_test(), 0);
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn memory_rpc_usage_telemetry_alone_cannot_trigger_retry() {
    let (context, contents) = context().await;
    let request = request(&context, DigestHasherFunc::Sha256, 128 * MIB, 1, false).await;
    let mut runtime = Runtime::start_history(
        &context,
        Arc::new(AtomicU64::new(0)),
        roundtrip::faults(),
        None,
    )
    .await;
    let mut connection = Connection::open(&runtime, &context, 1, CAPACITY.memory_bytes).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let mut report = launch(&context, &connection, command, &request, &contents).await;
    assert!(report.memory_breach.take().is_some());
    let usage = report.resource_usage.as_ref().unwrap();
    assert_eq!(usage.outcome, nativelink_proto::com::github::trace_machina::nativelink::remote_execution::ResourceOutcome::KilledMemory as i32);
    assert!(usage.enforced);
    let expected = captured(&report).encode_to_vec();
    connection.report(report).await;
    let accepted = terminal(&mut operations).await;
    let Some(operation::Result::Response(response)) = accepted.result else {
        unreachable!()
    };
    assert_eq!(response.value, expected);
    let state = runtime
        .wait(|state| state.executions[0].terminal.is_some())
        .await;
    assert!(matches!(
        state.executions[0].terminal,
        Some(TerminalResult::Completed(_))
    ));
    assert_eq!(state.executions[0].attempts_started, 1);
    assert_eq!(
        state.executions[0].reservation_target.memory_bytes,
        128 * MIB
    );
    assert_eq!(context.ac.len_for_test(), 0);
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}
