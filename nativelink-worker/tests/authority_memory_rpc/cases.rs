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

//! Explicit policy receipts over actual HTTP/2, with real namespace processes.
use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;

use bytes::Bytes;
use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptReport, AuthorityCapacityObservation, AuthorityCommand,
    AuthorityCoordinatorUpdate, AuthorityDecline, AuthorityRegistration,
    AuthoritySession, AuthorityWorkerUpdate, authority_attempt_report, authority_command,
    authority_coordinator_update, authority_worker_update,
};
use nativelink_scheduler::authority::accepted_history::{HistoryAssociation, HistoryStatus};
use nativelink_scheduler::authority::{ResourceFailureReason, TerminalResult};
use nativelink_worker::authority_worker::{PROTOCOL_VERSION, REQUIRED_CAPABILITIES};
use nativelink_config::cas_server::EnvironmentSource;
use nativelink_worker::running_actions_manager::{MemoryLimit, ResourceEnforcement};
use tokio::sync::mpsc;

use super::*;

const MIB: u64 = 1024 * 1024;
const CHILD_ENV: &str = "NATIVELINK_MEMORY_RPC_CHILD";

fn output_paths(count: usize) -> Vec<String> {
    if count == 1 {
        vec!["artifact".into()]
    } else {
        (0..count)
            .map(|index| format!("artifact-{index:04}-{}", "x".repeat(190)))
            .collect()
    }
}

#[test]
#[allow(
    clippy::print_stdout,
    reason = "Actual child output is part of the captured response"
)]
fn allocator_child() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }
    println!(
        "MEMORY_BEFORE;platform={}",
        std::env::var("ORIGINAL_MEMORY").unwrap()
    );
    eprintln!("MEMORY_STDERR_BEFORE");
    let count = std::env::var("OUTPUT_COUNT").unwrap().parse().unwrap();
    for path in output_paths(count) {
        std::fs::write(path, b"memory-artifact").unwrap();
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::env::var("LAUNCH_PATH").unwrap())
        .unwrap()
        .write_all(b"x")
        .unwrap();
    if let Ok(ready) = std::env::var("READY_PATH") {
        std::fs::write(ready, b"ready").unwrap();
        let release = std::env::var("RELEASE_PATH").unwrap();
        while !Path::new(&release).exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    let mut allocation = vec![0_u8; 192 * 1024 * 1024];
    for byte in allocation.iter_mut().step_by(4096) {
        *byte = 19;
    }
    std::hint::black_box(&allocation);
    std::thread::sleep(Duration::from_secs(2));
    println!("MEMORY_AFTER");
    std::hint::black_box(allocation);
}

async fn context() -> (Context, Arc<MemoryStore>) {
    context_with_payload_bound(64 * 1024).await
}

async fn context_with_payload_bound(payload_bytes: usize) -> (Context, Arc<MemoryStore>) {
    assert!(namespace_utils::namespaces_supported(false, false));
    let contents = MemoryStore::new(&MemorySpec::default());
    let context = Context::with_manager_configuration(
        false,
        Store::new(contents.clone()),
        Duration::from_secs(10),
        8,
        Duration::from_secs(30),
        ManagerConstruction::AuthorityOnlyBounded { payload_bytes },
        ExecutionConfiguration {
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
        },
    )
    .await;
    (context, contents)
}

struct MemoryRequest {
    request: ExecuteRequest,
    action: DigestInfo,
    action_bytes: Bytes,
    command: DigestInfo,
    command_bytes: Bytes,
    original_memory: u64,
}

async fn request(
    context: &Context,
    algorithm: DigestHasherFunc,
    memory: u64,
    outputs: usize,
    gate: bool,
) -> MemoryRequest {
    async {
        let mut environment_variables = vec![
            EnvironmentVariable {
                name: CHILD_ENV.into(),
                value: "1".into(),
            },
            EnvironmentVariable {
                name: "OUTPUT_COUNT".into(),
                value: outputs.to_string(),
            },
            EnvironmentVariable {
                name: "LAUNCH_PATH".into(),
                value: context
                    .directory
                    .path()
                    .join("memory-launches")
                    .to_string_lossy()
                    .into_owned(),
            },
        ];
        if gate {
            for (name, path) in [
                ("READY_PATH", "memory-ready"),
                ("RELEASE_PATH", "memory-release"),
            ] {
                environment_variables.push(EnvironmentVariable {
                    name: name.into(),
                    value: context
                        .directory
                        .path()
                        .join(path)
                        .to_string_lossy()
                        .into_owned(),
                });
            }
        }
        let command = Command {
            arguments: vec![
                std::env::current_exe()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                "--exact".into(),
                "memory::allocator_child".into(),
                "--nocapture".into(),
            ],
            environment_variables,
            output_paths: output_paths(outputs),
            ..Command::default()
        };
        let command_bytes: Bytes = command.encode_to_vec().into();
        let command =
            serialize_and_upload_message(&command, context.cas.as_pin(), &mut algorithm.hasher())
                .await
                .unwrap();
        let input = serialize_and_upload_message(
            &Directory::default(),
            context.cas.as_pin(),
            &mut algorithm.hasher(),
        )
        .await
        .unwrap();
        let action = Action {
            command_digest: Some(command.into()),
            input_root_digest: Some(input.into()),
            timeout: Some(prost_types::Duration {
                seconds: 30,
                nanos: 0,
            }),
            platform: Some(Platform {
                properties: vec![
                    Property {
                        name: "cpu_count".into(),
                        value: "1".into(),
                    },
                    Property {
                        name: "memory_kb".into(),
                        value: memory.div_ceil(1024).to_string(),
                    },
                ],
            }),
            ..Action::default()
        };
        let mut action_bytes = action.encode_to_vec();
        action_bytes.extend_from_slice(&[0xf8, 0xf0, 0x04, 1]);
        let mut hasher = algorithm.hasher();
        hasher.update(&action_bytes);
        let action = hasher.finalize_digest();
        context
            .cas
            .update_oneshot(action, action_bytes.clone().into())
            .await
            .unwrap();
        MemoryRequest {
            request: ExecuteRequest {
                instance_name: "rpc-test".into(),
                action_digest: Some(action.into()),
                digest_function: algorithm.proto_digest_func().into(),
                skip_cache_lookup: true,
                ..ExecuteRequest::default()
            },
            action,
            action_bytes: action_bytes.into(),
            command,
            command_bytes,
            original_memory: memory,
        }
    }
    .with_context(make_ctx_for_hash_func(algorithm).unwrap())
    .await
}

struct Connection {
    sender: mpsc::Sender<AuthorityWorkerUpdate>,
    responses: Streaming<AuthorityCoordinatorUpdate>,
    session: AuthoritySession,
    capacity: AuthorityResources,
    capacity_sequence: u64,
}

impl Connection {
    async fn open(runtime: &Runtime, context: &Context, session: u64, memory: u64) -> Self {
        let mut registration = context
            .worker
            .make_registration(session, Vec::new())
            .unwrap();
        let capacity = AuthorityResources {
            memory_bytes: memory,
            ..resources(CAPACITY)
        };
        registration.capacity = Some(capacity);
        registration.admission_ceiling = Some(capacity);
        let session = registration.session.unwrap();
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Registration(registration)),
            })
            .await
            .unwrap();
        let incoming = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|value| (value, receiver))
        });
        let mut client = AuthorityWorkerApiClient::new(runtime.worker_channel.clone());
        let mut responses = client.connect(incoming).await.unwrap().into_inner();
        let update = responses.message().await.unwrap().unwrap();
        let Some(authority_coordinator_update::Update::Registration(registration)) = update.update
        else {
            panic!("Registration must precede commands");
        };
        assert_eq!(registration.protocol_version, 2);
        assert_eq!(registration.required_capabilities, REQUIRED_CAPABILITIES);
        context.worker.negotiate(&registration).unwrap();
        Self {
            sender,
            responses,
            session,
            capacity,
            capacity_sequence: 0,
        }
    }

    async fn dispatch(&mut self) -> AuthorityCommand {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let update = self.responses.message().await.unwrap().unwrap();
                if let Some(authority_coordinator_update::Update::Command(command)) = update.update
                {
                    assert!(matches!(
                        command.command,
                        Some(authority_command::Command::Dispatch(_))
                    ));
                    return command;
                }
            }
        })
        .await
        .unwrap()
    }

    async fn report(&self, report: AuthorityAttemptReport) {
        self.sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(report)),
            })
            .await
            .unwrap();
    }

    async fn capacity(
        &mut self,
        runtime: &Runtime,
        memory: u64,
        draining: bool,
    ) -> Arc<ObservableState> {
        self.capacity_sequence += 1;
        self.capacity.memory_bytes = memory;
        self.sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Capacity(
                    AuthorityCapacityObservation {
                        session: Some(self.session),
                        observation_sequence: self.capacity_sequence,
                        capacity: Some(self.capacity),
                        admission_ceiling: Some(self.capacity),
                        draining,
                    },
                )),
            })
            .await
            .unwrap();
        runtime
            .wait(|state| {
                state.workers.first().is_some_and(|worker| {
                    worker.capacity.memory_bytes == memory && worker.draining == draining
                })
            })
            .await
    }

    async fn barrier(&mut self, runtime: &Runtime) -> Arc<ObservableState> {
        self.capacity(runtime, self.capacity.memory_bytes, true)
            .await;
        self.capacity(runtime, self.capacity.memory_bytes, false)
            .await
    }
}

async fn candidate(context: &Context, attempt: u64) -> AuthorityAttemptReport {
    let mut changes = context.worker.subscribe();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(report) = context
                .worker
                .reconcile()
                .attempts
                .into_iter()
                .find(|report| {
                    report.attempt.unwrap().attempt_id == attempt && report.result.is_some()
                })
            {
                return report;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

async fn start(
    context: &Context,
    connection: &Connection,
    command: AuthorityCommand,
    request: &MemoryRequest,
    contents: &MemoryStore,
) -> u64 {
    let attempt = command.attempt.unwrap().attempt_id;
    let Some(authority_command::Command::Dispatch(dispatch)) = &command.command else {
        unreachable!()
    };
    assert_eq!(
        dispatch.execute_request.as_ref().unwrap().encode_to_vec(),
        request.request.encode_to_vec()
    );
    for (digest, expected) in [
        (request.action, &request.action_bytes),
        (request.command, &request.command_bytes),
    ] {
        assert_eq!(
            &contents.get_part_unchunked(digest, 0, None).await.unwrap(),
            expected
        );
    }
    assert!(
        context
            .worker
            .command(connection.session.session_id, command)
            .unwrap()
            .result
            .is_none()
    );
    attempt
}

async fn launch(
    context: &Context,
    connection: &Connection,
    command: AuthorityCommand,
    request: &MemoryRequest,
    contents: &MemoryStore,
) -> AuthorityAttemptReport {
    let attempt = start(context, connection, command, request, contents).await;
    candidate(context, attempt).await
}

fn assert_memory_report(
    report: &AuthorityAttemptReport,
    state: &ObservableState,
    expected_memory: u64,
) {
    assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
    let marker = report.memory_breach.unwrap();
    let attempt = state
        .attempts
        .iter()
        .find(|attempt| attempt.id.0 == report.attempt.unwrap().attempt_id)
        .unwrap();
    let reservation = state
        .reservations
        .iter()
        .find(|reservation| reservation.id == attempt.reservation)
        .unwrap();
    assert_eq!(marker.reservation_id, reservation.id.0);
    assert_eq!(marker.charged, Some(resources(reservation.charged)));
    assert_eq!(marker.charged.unwrap().memory_bytes, expected_memory);
    assert_eq!(
        marker.limit_kb,
        (expected_memory * 120).div_ceil(100 * 1024)
    );
    assert!(marker.observed_kb > marker.limit_kb);
    assert_ne!(captured(report).result.as_ref().unwrap().exit_code, 0);
    assert_eq!(
        captured(report).status.as_ref().unwrap().code,
        tonic::Code::FailedPrecondition as i32
    );
}

fn captured(report: &AuthorityAttemptReport) -> &ExecuteResponse {
    let Some(authority_attempt_report::Result::ExecuteResponse(response)) = &report.result else {
        panic!("Expected uploaded response: {report:?}");
    };
    response
}

async fn assert_graph(
    contents: &MemoryStore,
    response: &ExecuteResponse,
    algorithm: DigestHasherFunc,
    original_memory: u64,
) {
    let result = response.result.as_ref().unwrap();
    assert_eq!(result.output_files.len(), 1);
    let file = result.output_files[0].digest.as_ref().unwrap();
    let bytes = contents
        .get_part_unchunked(DigestInfo::try_from(file).unwrap(), 0, None)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), b"memory-artifact");
    let mut hasher = algorithm.hasher();
    hasher.update(&bytes);
    assert_eq!(
        hasher.finalize_digest(),
        DigestInfo::try_from(file).unwrap()
    );
    let stdout = contents
        .get_part_unchunked(
            DigestInfo::try_from(result.stdout_digest.as_ref().unwrap()).unwrap(),
            0,
            None,
        )
        .await
        .unwrap();
    let stderr = contents
        .get_part_unchunked(
            DigestInfo::try_from(result.stderr_digest.as_ref().unwrap()).unwrap(),
            0,
            None,
        )
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(&stdout).contains(&format!(
        "MEMORY_BEFORE;platform={}",
        original_memory.div_ceil(1024)
    )));
    assert!(String::from_utf8_lossy(&stderr).contains("MEMORY_STDERR_BEFORE"));
}

#[path = "roundtrip.rs"]
mod roundtrip;
#[path = "validation.rs"]
mod validation;
