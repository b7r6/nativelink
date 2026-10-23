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

//! Local policy observations stay separate from RPC acceptance.
use bytes::Bytes;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use nativelink_worker::running_actions_manager::{RunningAction as _, RunningActionsManager as _};
use opentelemetry::trace::FutureExt;

use super::*;

async fn fixture(algorithm: DigestHasherFunc, external_signal: bool) -> (Context, StartExecute) {
    fixture_with_namespace(algorithm, external_signal, true).await
}

async fn fixture_with_namespace(
    algorithm: DigestHasherFunc,
    external_signal: bool,
    namespaced: bool,
) -> (Context, StartExecute) {
    if namespaced {
        assert!(namespace_utils::namespaces_supported(false, false));
    }
    let context = setup_configured(
        false,
        4,
        namespaced,
        AuthorityResources {
            cpu_millis: 8000,
            memory_bytes: 512 * MIB,
            slots: 2,
        },
        ExecutionConfiguration {
            persistent_workers: PersistentWorkersSettings {
                enabled: false,
                ..PersistentWorkersSettings::default()
            },
            resource_enforcement: Some(ResourceEnforcement {
                memory: Some(MemoryLimit {
                    property_name: "memory_kb".into(),
                    headroom_percent: if external_signal { 0 } else { 20 },
                }),
                disk_property_name: None,
                disk_soft: false,
                disk_headroom_percent: 0,
            }),
            additional_environment: Some(HashMap::from([(
                "ORIGINAL_MEMORY".into(),
                EnvironmentSource::Property("memory_kb".into()),
            )])),
            ..ExecutionConfiguration::default()
        },
    )
    .await;
    let command = Command {
        arguments: vec![
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "--exact".into(),
            "grant::allocator_child".into(),
            "--nocapture".into(),
        ],
        environment_variables: vec![
            EnvironmentVariable {
                name: CHILD_ENV.into(),
                value: if external_signal {
                    "external-signal"
                } else {
                    "1"
                }
                .into(),
            },
            EnvironmentVariable {
                name: "NATIVELINK_BREACH_OUTPUT".into(),
                value: "1".into(),
            },
        ],
        output_paths: vec!["artifact".into()],
        ..Command::default()
    };
    let command_digest =
        serialize_and_upload_message(&command, context.cas.as_pin(), &mut algorithm.hasher())
            .await
            .unwrap();
    let root = serialize_and_upload_message(
        &Directory::default(),
        context.cas.as_pin(),
        &mut algorithm.hasher(),
    )
    .await
    .unwrap();
    let platform = Platform {
        properties: vec![Property {
            name: "memory_kb".into(),
            value: (ORIGINAL / 1024).to_string(),
        }],
    };
    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(root.into()),
        platform: Some(platform.clone()),
        ..Action::default()
    };
    let digest =
        serialize_and_upload_message(&action, context.cas.as_pin(), &mut algorithm.hasher())
            .await
            .unwrap();
    let start = StartExecute {
        execute_request: Some(ExecuteRequest {
            action_digest: Some(digest.into()),
            digest_function: algorithm.proto_digest_func().into(),
            ..ExecuteRequest::default()
        }),
        operation_id: "breach-observation".into(),
        platform: Some(platform),
        queued_timestamp: Some(prost_types::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        worker_id: "test".into(),
        request_metadata: None,
    };
    (context, start)
}

async fn execute(
    context: &Context,
    start: StartExecute,
    memory_bytes: u64,
    external_signal: bool,
) -> Arc<nativelink_worker::running_actions_manager::RunningActionImpl> {
    let ready = context
        .directory
        .path()
        .join("actions")
        .join(&start.operation_id)
        .join("work/ready-to-signal");
    let action = context
        .manager
        .create_and_add_authority_action(
            "test".into(),
            start,
            AuthorityResources {
                cpu_millis: 1234,
                memory_bytes,
                slots: 1,
            },
        )
        .await
        .unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    action.observe_launch(sender).unwrap();
    assert!(action.authority_memory_breach().is_none());
    action.clone().prepare_action().await.unwrap();
    if external_signal {
        let signal = async {
            let pid = i32::try_from(receiver.await.unwrap()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !ready.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            // Allow several 250 ms samples of the retained allocation. The
            // outcome assertion below requires that the heuristic really ran.
            tokio::time::sleep(Duration::from_secs(1)).await;
            // SAFETY: pid is the live outer process reported by this action's
            // launch observer; its child is waiting for this external signal.
            assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
        };
        let (execution, ()) = tokio::join!(action.clone().execute(), signal);
        execution.unwrap();
    } else {
        action.clone().execute().await.unwrap();
    }
    assert!(action.process_domain_quiescent());
    action
}

#[nativelink_test]
async fn authority_memory_receipt_binds_both_digest_grants_and_preserves_outputs() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        async {
            let (context, start) = fixture(algorithm, false).await;
            let original_request = start.execute_request.as_ref().unwrap().encode_to_vec();
            for attempt in 1..=2 {
                let mut start = start.clone();
                start.operation_id = format!("breach-{attempt}");
                let action = execute(&context, start, ORIGINAL * attempt, false).await;
                let receipt = action.authority_memory_breach();
                if attempt == 1 {
                    let receipt = receipt.unwrap();
                    assert_eq!(
                        receipt.charged(),
                        action.authority_resource_limits().unwrap().charged
                    );
                    assert_eq!(
                        Some(receipt.limit_kb()),
                        action.authority_resource_limits().unwrap().memory_limit_kb
                    );
                    assert!(receipt.observed_kb() > receipt.limit_kb());
                } else {
                    assert!(receipt.is_none());
                }
                action.clone().upload_results().await.unwrap();
                let result = action.clone().get_finished_result().await.unwrap();
                assert_eq!(action.authority_memory_breach(), receipt);
                if attempt == 1 {
                    assert_ne!(result.exit_code, 0);
                    assert_eq!(
                        result.error.as_ref().unwrap().code,
                        Code::FailedPrecondition
                    );
                } else {
                    assert_eq!(result.exit_code, 0);
                    assert!(result.error.is_none());
                }
                assert_eq!(result.output_files.len(), 1);
                let stdout = context
                    .cas
                    .get_part_unchunked(result.stdout_digest, 0, None)
                    .await
                    .unwrap();
                let stderr = context
                    .cas
                    .get_part_unchunked(result.stderr_digest, 0, None)
                    .await
                    .unwrap();
                assert!(String::from_utf8_lossy(&stdout).contains("BEFORE_ALLOCATION"));
                assert!(String::from_utf8_lossy(&stderr).contains("STDERR_BEFORE_ALLOCATION"));
                assert_eq!(
                    context
                        .cas
                        .get_part_unchunked(result.output_files[0].digest, 0, None)
                        .await
                        .unwrap(),
                    Bytes::from_static(b"output-before-breach")
                );
                action.cleanup().await.unwrap();
            }
            assert_eq!(
                start.execute_request.as_ref().unwrap().encode_to_vec(),
                original_request
            );
        }
        .with_context(make_ctx_for_hash_func(algorithm).unwrap())
        .await;
    }
}

#[nativelink_test]
async fn authority_memory_receipt_survives_upload_failure_without_a_finished_result() {
    let (context, start) = fixture(DigestHasherFunc::Sha256, false).await;
    let action = execute(&context, start, ORIGINAL, false).await;
    let receipt = action.authority_memory_breach().unwrap();
    let temp = context.directory.path().join("temp");
    let unavailable = context.directory.path().join("unavailable-temp");
    std::fs::rename(&temp, &unavailable).unwrap();
    let upload = action.clone().upload_results().await;
    std::fs::rename(&unavailable, &temp).unwrap();
    assert!(upload.is_err());
    assert_eq!(action.authority_memory_breach(), Some(receipt));
    assert!(action.clone().get_finished_result().await.is_err());
    action.cleanup().await.unwrap();
}

#[nativelink_test]
async fn authority_memory_heuristic_sigkill_does_not_mint_a_receipt() {
    let (context, start) = fixture(DigestHasherFunc::Sha256, true).await;
    let action = execute(&context, start, 208 * MIB, true).await;
    assert!(action.authority_memory_breach().is_none());
    action.clone().upload_results().await.unwrap();
    let usage = action.resource_usage().unwrap();
    assert_eq!(
        usage.outcome,
        ResourceOutcome::KilledMemory as i32,
        "must exercise the existing near-limit heuristic"
    );
    assert!(!usage.enforced);
    assert!(action.authority_memory_breach().is_none());
    assert_ne!(
        action
            .clone()
            .get_finished_result()
            .await
            .unwrap()
            .exit_code,
        0
    );
    action.cleanup().await.unwrap();
}

#[nativelink_test]
async fn authority_memory_wire_requires_independent_domain_quiescence() {
    let (context, start) = fixture_with_namespace(DigestHasherFunc::Sha256, false, false).await;
    let command = AuthorityCommand {
        attempt: Some(target(1)),
        effect_id: 2,
        command: Some(authority_command::Command::Dispatch(AuthorityDispatch {
            reservation_id: 1,
            execute_request: start.execute_request,
            platform: start.platform,
            queued_timestamp: start.queued_timestamp,
            reserved_resources: Some(AuthorityResources {
                cpu_millis: 1000,
                memory_bytes: ORIGINAL,
                slots: 1,
            }),
            ..AuthorityDispatch::default()
        })),
    };
    context.worker.negotiate(&negotiation(1)).unwrap();
    assert!(context.worker.command(1, command).unwrap().result.is_none());
    let report = wait_result(&context.worker, 1).await;
    assert_eq!(
        report.phase,
        AuthorityAttemptPhase::TerminationUncertain as i32
    );
    let usage = report.resource_usage.as_ref().unwrap();
    assert_eq!(usage.outcome, ResourceOutcome::KilledMemory as i32);
    assert!(usage.enforced);
    assert!(report.memory_breach.is_none());
    let Some(authority_attempt_report::Result::ExecuteResponse(response)) = report.result else {
        panic!("Actual uploaded result remains captured independently of domain closure");
    };
    assert_ne!(response.result.as_ref().unwrap().exit_code, 0);
    assert_eq!(response.result.as_ref().unwrap().output_files.len(), 1);
    assert_eq!(context.worker.reconcile().attempts.len(), 1);
}
