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

//! Real-process grant binding: no RPC memory classification is enabled here.
use std::collections::HashMap;

use nativelink_config::cas_server::EnvironmentSource;
use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::ResourceOutcome;
use nativelink_worker::running_actions_manager::{MemoryLimit, ResourceEnforcement};

use super::*;

const MIB: u64 = 1024 * 1024;
const ORIGINAL: u64 = 128 * MIB;
const CHILD_ENV: &str = "NATIVELINK_AUTHORITY_GRANT_ALLOCATOR_CHILD";

#[nativelink_test]
async fn typed_memory_and_cpu_preserve_original_disk_diagnostic() {
    assert!(namespace_utils::namespaces_supported(false, false));
    let context = setup_configured(
        false,
        4,
        true,
        RESOURCES,
        ExecutionConfiguration {
            persistent_workers: PersistentWorkersSettings {
                enabled: false,
                ..PersistentWorkersSettings::default()
            },
            resource_enforcement: Some(ResourceEnforcement {
                memory: None,
                disk_property_name: Some("custom_disk_kb".into()),
                disk_soft: false,
                disk_headroom_percent: 0,
            }),
            ..ExecutionConfiguration::default()
        },
    )
    .await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    let mut command = context.dispatch(1, vec!["/bin/true".into()]).await;
    let Some(authority_command::Command::Dispatch(dispatch)) = &mut command.command else {
        unreachable!()
    };
    dispatch.platform = Some(Platform {
        properties: vec![Property {
            name: "custom_disk_kb".into(),
            value: "37".into(),
        }],
    });
    context.worker.command(1, command).unwrap();
    let report = wait_result(&context.worker, 1).await;
    assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
    let usage = report.resource_usage.unwrap();
    assert_eq!(usage.outcome, ResourceOutcome::Completed as i32);
    let reserved = usage.reserved.unwrap();
    assert_eq!(reserved.cpu_count, RESOURCES.cpu_millis.div_ceil(1000));
    assert_eq!(reserved.memory_kb, RESOURCES.memory_bytes.div_ceil(1024));
    assert_eq!(reserved.disk_kb, 37);
}

#[test]
#[allow(
    clippy::print_stdout,
    reason = "Child stdout proves that the action still observes its original environment"
)]
fn allocator_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    println!("BEFORE_ALLOCATION");
    eprintln!("STDERR_BEFORE_ALLOCATION");
    if std::env::var_os("NATIVELINK_BREACH_OUTPUT").is_some() {
        std::fs::write("artifact", b"output-before-breach").unwrap();
    }
    let mut bytes = vec![0_u8; 192 * 1024 * 1024];
    for byte in bytes.iter_mut().step_by(4096) {
        *byte = 17;
    }
    std::hint::black_box(&bytes);
    std::thread::sleep(Duration::from_secs(2));
    if std::env::var(CHILD_ENV).as_deref() == Ok("external-signal") {
        std::fs::write("ready-to-signal", b"allocated").unwrap();
        // Namespace PID 1 cannot kill itself with SIGKILL. The test supervisor
        // signals the observed outer process after the sampler sees this RSS.
        loop {
            std::thread::park();
            std::hint::black_box(&bytes);
        }
    }
    println!(
        "GRANT_CHILD_COMPLETED;platform={}",
        std::env::var("ORIGINAL_MEMORY").unwrap()
    );
    std::hint::black_box(bytes);
}

#[nativelink_test]
async fn same_request_larger_typed_grant_changes_actual_memory_enforcement() {
    assert!(
        namespace_utils::namespaces_supported(false, false),
        "This qualification requires actual namespace quiescence"
    );
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let configuration = ExecutionConfiguration {
            persistent_workers: PersistentWorkersSettings {
                enabled: false,
                ..PersistentWorkersSettings::default()
            },
            resource_enforcement: Some(ResourceEnforcement {
                memory: Some(MemoryLimit {
                    property_name: "memory_kb".into(),
                    headroom_percent: 20,
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
        };
        let context = setup_configured(
            false,
            4,
            true,
            AuthorityResources {
                cpu_millis: 8000,
                memory_bytes: 512 * MIB,
                slots: 2,
            },
            configuration,
        )
        .await;
        context.worker.negotiate(&negotiation(1)).unwrap();
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
            environment_variables: vec![EnvironmentVariable {
                name: CHILD_ENV.into(),
                value: "1".into(),
            }],
            ..Command::default()
        };
        let command_bytes = command.encode_to_vec();
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
        let action_bytes = action.encode_to_vec();
        let action_digest =
            serialize_and_upload_message(&action, context.cas.as_pin(), &mut algorithm.hasher())
                .await
                .unwrap();
        let request = ExecuteRequest {
            action_digest: Some(action_digest.into()),
            digest_function: algorithm.proto_digest_func().into(),
            ..ExecuteRequest::default()
        };
        let request_bytes = request.encode_to_vec();
        for attempt in 1..=2 {
            let grant = AuthorityResources {
                cpu_millis: 1000,
                memory_bytes: ORIGINAL * attempt,
                slots: 1,
            };
            let command = AuthorityCommand {
                attempt: Some(target(attempt)),
                effect_id: attempt * 2,
                command: Some(authority_command::Command::Dispatch(AuthorityDispatch {
                    reservation_id: attempt,
                    execute_request: Some(request.clone()),
                    queued_timestamp: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 0,
                    }),
                    platform: Some(platform.clone()),
                    reserved_resources: Some(grant),
                    ..AuthorityDispatch::default()
                })),
            };
            context.worker.command(attempt, command.clone()).unwrap();
            let mut conflict = command.clone();
            let Some(authority_command::Command::Dispatch(dispatch)) = &mut conflict.command else {
                unreachable!()
            };
            dispatch.reserved_resources.as_mut().unwrap().memory_bytes += 1;
            assert!(context.worker.command(attempt, conflict).is_err());
            let report = wait_result(&context.worker, attempt).await;
            assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
            assert_eq!(
                context.worker.command(attempt, command.clone()).unwrap(),
                report
            );
            if attempt == 1 {
                context.worker.disconnect(1);
                assert_eq!(
                    context.worker.negotiate(&negotiation(2)).unwrap().attempts,
                    vec![report.clone()]
                );
                assert_eq!(context.worker.command(2, command.clone()).unwrap(), report);
                let mut changed = command;
                let Some(authority_command::Command::Dispatch(dispatch)) = &mut changed.command
                else {
                    unreachable!()
                };
                dispatch.reserved_resources.as_mut().unwrap().memory_bytes *= 2;
                assert!(context.worker.command(2, changed).is_err());
            }
            let usage = report
                .resource_usage
                .as_ref()
                .expect("completed upload retains usage");
            assert_eq!(
                usage.reserved.unwrap().memory_kb,
                grant.memory_bytes.div_ceil(1024)
            );
            assert_eq!(
                usage.reserved.unwrap().cpu_count,
                grant.cpu_millis.div_ceil(1000)
            );
            let Some(authority_attempt_report::Result::ExecuteResponse(response)) = &report.result
            else {
                panic!("expected exact response: {report:?}")
            };
            if attempt == 1 {
                assert_eq!(usage.outcome, ResourceOutcome::KilledMemory as i32);
                assert!(usage.enforced);
                assert_ne!(response.status.as_ref().map_or(0, |s| s.code), 0);
            } else {
                assert_eq!(
                    usage.outcome,
                    ResourceOutcome::Completed as i32,
                    "A doubled immutable grant must not enforce the original Action string; {usage:?}"
                );
                assert!(!usage.enforced);
                assert_eq!(response.result.as_ref().unwrap().exit_code, 0);
                let digest: nativelink_util::common::DigestInfo = response
                    .result
                    .as_ref()
                    .unwrap()
                    .stdout_digest
                    .clone()
                    .unwrap()
                    .try_into()
                    .unwrap();
                let stdout = context
                    .cas
                    .get_part_unchunked(digest, 0, None)
                    .await
                    .unwrap();
                assert!(
                    String::from_utf8(stdout.to_vec())
                        .unwrap()
                        .contains(&format!(
                            "GRANT_CHILD_COMPLETED;platform={}",
                            ORIGINAL / 1024
                        ))
                );
            }
            assert_eq!(
                context
                    .cas
                    .get_part_unchunked(action_digest, 0, None)
                    .await
                    .unwrap()
                    .as_ref(),
                action_bytes
            );
            assert_eq!(
                context
                    .cas
                    .get_part_unchunked(command_digest, 0, None)
                    .await
                    .unwrap()
                    .as_ref(),
                command_bytes
            );
            assert_eq!(request.encode_to_vec(), request_bytes);
        }
        eprintln!(
            "AUTHORITY_TYPED_GRANT_REAL_PROCESS: {algorithm}; same request R, explicit grants R/2R; first killed, second complete; namespace quiescent; original platform environment and CAS bytes retained"
        );
    }
}

#[nativelink_test]
async fn exact_grant_and_conservative_kib_ceiling_validate_before_launch() {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
    use nativelink_worker::running_actions_manager::{
        RunningAction as _, RunningActionsManager as _,
    };
    for (headroom, memory, slots, expected) in [
        (Some(0), 0, 1, Ok(None)),
        (Some(0), 1, 1, Ok(Some(1))),
        (Some(0), 1023, 1, Ok(Some(1))),
        (Some(0), 1024, 1, Ok(Some(1))),
        (Some(0), 1025, 1, Ok(Some(2))),
        (Some(20), 853, 1, Ok(Some(1))),
        (Some(20), 854, 1, Ok(Some(2))),
        (Some(20), 1024, 1, Ok(Some(2))),
        (Some(20), ORIGINAL, 1, Ok(Some(157_287))),
        (Some(0), u64::MAX, 1, Ok(Some(u64::MAX.div_ceil(1024)))),
        (None, u64::MAX, 1, Ok(None)),
        (Some(20), u64::MAX, 1, Err(())),
        (Some(u64::MAX), u64::MAX, 1, Err(())),
        (Some(0), 1024, 0, Err(())),
    ] {
        let context = setup_configured(
            false,
            4,
            false,
            RESOURCES,
            ExecutionConfiguration {
                resource_enforcement: Some(ResourceEnforcement {
                    memory: headroom.map(|headroom_percent| MemoryLimit {
                        property_name: "custom_legacy_memory".into(),
                        headroom_percent,
                    }),
                    disk_property_name: None,
                    disk_soft: false,
                    disk_headroom_percent: 0,
                }),
                ..ExecutionConfiguration::default()
            },
        )
        .await;
        let command = context.dispatch(1, vec!["/bin/true".into()]).await;
        let Some(authority_command::Command::Dispatch(dispatch)) = command.command else {
            unreachable!()
        };
        let grant = AuthorityResources {
            cpu_millis: 1234,
            memory_bytes: memory,
            slots,
        };
        let start = StartExecute {
            execute_request: dispatch.execute_request,
            operation_id: "grant-boundary".into(),
            queued_timestamp: dispatch.queued_timestamp,
            platform: Some(Platform {
                properties: vec![Property {
                    name: "custom_legacy_memory".into(),
                    value: "1".into(),
                }],
            }),
            worker_id: "test".into(),
            request_metadata: None,
        };
        let result = context
            .manager
            .create_and_add_authority_action("test".into(), start, grant)
            .await;
        if let Ok(expected_limit) = expected {
            let action = result.unwrap();
            let observed = action.authority_resource_limits().unwrap();
            assert_eq!(observed.charged, grant);
            assert_eq!(observed.memory_limit_kb, expected_limit);
            assert!(action.resource_usage().is_none());
            action.cleanup().await.unwrap();
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code, Code::InvalidArgument);
            assert_eq!(
                std::fs::read_dir(context.directory.path().join("actions"))
                    .unwrap()
                    .count(),
                0,
                "validation must precede action directory and launch"
            );
        }
    }
}

#[path = "breach.rs"]
mod breach;
