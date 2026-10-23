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

//! Canonical namespaces expose ambient-hash mistakes hidden by MemoryStore.
//! The manual wire worker supplies a candidate, not a process-quiescence proof.

use std::sync::atomic::Ordering;

use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, ExecuteRequest, ExecuteResponse, digest_function,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapability, AuthorityCommand,
    AuthorityCoordinatorUpdate, AuthorityReconciliation, AuthorityRegistration,
    AuthorityResources, AuthoritySession, AuthorityWorkerIdentity, AuthorityWorkerUpdate,
    authority_attempt_report, authority_command, authority_coordinator_update,
    authority_worker_update,
};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::{EffectKind, ObservableState};
use tokio::sync::mpsc;
use tonic::Streaming;

use super::*;

fn other(algorithm: DigestHasherFunc) -> DigestHasherFunc {
    match algorithm {
        DigestHasherFunc::Sha256 => DigestHasherFunc::Blake3,
        DigestHasherFunc::Blake3 => DigestHasherFunc::Sha256,
    }
}

async fn action(rpc: &FixedRpc, algorithm: DigestHasherFunc) -> DigestInfo {
    let command = Command {
        arguments: vec!["canonical namespace witness".to_owned()],
        ..Command::default()
    };
    let command_digest = upload(rpc.client.clone(), algorithm, &command.encode_to_vec()).await;
    upload(
        rpc.client.clone(),
        algorithm,
        &Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(digest(algorithm, b"").into()),
            ..Action::default()
        }
        .encode_to_vec(),
    )
    .await
}

fn execute(key: DigestInfo, algorithm: DigestHasherFunc, skip_lookup: bool) -> ExecuteRequest {
    ExecuteRequest {
        instance_name: INSTANCE.to_owned(),
        action_digest: Some(key.into()),
        digest_function: algorithm.proto_digest_func().into(),
        skip_cache_lookup: skip_lookup,
        ..ExecuteRequest::default()
    }
}

fn result(bytes: &'static [u8]) -> ActionResult {
    ActionResult {
        stdout_raw: Bytes::from_static(bytes),
        ..ActionResult::default()
    }
}

async fn cache_put(
    rpc: &FixedRpc,
    key: DigestInfo,
    algorithm: DigestHasherFunc,
    value: &ActionResult,
) {
    assert_eq!(
        ActionCacheClient::new(rpc.client.clone())
            .update_action_result(UpdateActionResultRequest {
                instance_name: INSTANCE.to_owned(),
                action_digest: Some(key.into()),
                digest_function: algorithm.proto_digest_func().into(),
                action_result: Some(value.clone()),
                ..UpdateActionResultRequest::default()
            })
            .await
            .unwrap()
            .into_inner(),
        *value,
    );
}

async fn cache_get(
    rpc: &FixedRpc,
    key: DigestInfo,
    algorithm: DigestHasherFunc,
) -> Result<ActionResult, tonic::Status> {
    ActionCacheClient::new(rpc.client.clone())
        .get_action_result(GetActionResultRequest {
            instance_name: INSTANCE.to_owned(),
            action_digest: Some(key.into()),
            digest_function: algorithm.proto_digest_func().into(),
            ..GetActionResultRequest::default()
        })
        .await
        .map(tonic::Response::into_inner)
}

async fn state(rpc: &FixedRpc, predicate: impl Fn(&ObservableState) -> bool) {
    let mut states = rpc.service.subscribe();
    timeout(Duration::from_secs(10), async {
        loop {
            if predicate(&states.borrow()) {
                break;
            }
            states.changed().await.unwrap();
        }
    })
    .await
    .expect("canonical authority did not reach the required committed state");
}

async fn terminal(operations: &mut Streaming<Operation>) -> ExecuteResponse {
    timeout(Duration::from_secs(10), async {
        loop {
            let operation = operations.message().await.unwrap().unwrap();
            if operation.done {
                let Some(operation::Result::Response(response)) = operation.result else {
                    panic!("expected a terminal ExecuteResponse");
                };
                break ExecuteResponse::decode(response.value).unwrap();
            }
        }
    })
    .await
    .expect("canonical execution did not complete")
}

#[nativelink_macro::nativelink_test]
async fn capture_uses_request_algorithm_for_canonical_action_and_command() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let (fixture, stores) = Fixture::new().await;
        let rpc = FixedRpc::new(&fixture, &stores).await;
        let key = action(&rpc, algorithm).await;
        let mut operations = ExecutionClient::new(rpc.client.clone())
            .execute(execute(key, algorithm, true))
            .await
            .expect("capture must read the uploaded canonical hash namespace")
            .into_inner();
        assert!(!operations.message().await.unwrap().unwrap().done);
        state(&rpc, |state| state.executions.len() == 1).await;
        drop(operations);
        rpc.shutdown().await;
    }
}

#[nativelink_macro::nativelink_test]
async fn lookup_uses_action_algorithm_and_preserves_same_key_namespace_isolation() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let (fixture, stores) = Fixture::new().await;
        let rpc = FixedRpc::new(&fixture, &stores).await;
        let key = action(&rpc, algorithm).await;
        let selected = result(b"selected algorithm cache entry");
        let unrelated = result(b"same opaque key, other algorithm");
        cache_put(&rpc, key, algorithm, &selected).await;
        cache_put(&rpc, key, other(algorithm), &unrelated).await;
        let mut request = execute(key, algorithm, false);
        // The fixed request contract treats Unknown as SHA256. This control
        // also verifies that contextual storage does not reinterpret that key.
        if algorithm == DigestHasherFunc::Sha256 {
            request.digest_function = digest_function::Value::Unknown.into();
        }
        let mut operations = ExecutionClient::new(rpc.client.clone())
            .execute(request)
            .await
            .unwrap()
            .into_inner();
        let response = terminal(&mut operations).await;
        assert!(response.cached_result);
        assert_eq!(response.result, Some(selected.clone()));
        assert_eq!(cache_get(&rpc, key, algorithm).await.unwrap(), selected);
        assert_eq!(
            cache_get(&rpc, key, other(algorithm)).await.unwrap(),
            unrelated
        );
        drop(operations);
        rpc.shutdown().await;
    }
}

async fn worker(
    rpc: &FixedRpc,
) -> (
    mpsc::Sender<AuthorityWorkerUpdate>,
    Streaming<AuthorityCoordinatorUpdate>,
) {
    let session = AuthoritySession {
        worker: Some(AuthorityWorkerIdentity {
            cluster_epoch: 1,
            worker_id: 2,
            incarnation_id: 3,
        }),
        session_id: 1,
    };
    let resources = AuthorityResources {
        cpu_millis: 1000,
        memory_bytes: 1024 * 1024,
        slots: 1,
    };
    let (updates, incoming) = mpsc::channel(4);
    updates
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Registration(
                AuthorityRegistration {
                    session: Some(session),
                    protocol_version: 2,
                    capabilities: [
                        AuthorityCapability::AttemptEffectIdentities,
                        AuthorityCapability::ReconnectReconciliation,
                        AuthorityCapability::CleanupTombstones,
                        AuthorityCapability::CandidateResults,
                        AuthorityCapability::MeaningfulProgress,
                        AuthorityCapability::ReceiptRetirement,
                        AuthorityCapability::ObservedMemoryBreach,
                    ]
                    .map(|capability| capability as i32)
                    .to_vec(),
                    capacity: Some(resources),
                    admission_ceiling: Some(resources),
                    reconciliation: Some(AuthorityReconciliation {
                        session: Some(session),
                        ..AuthorityReconciliation::default()
                    }),
                    ..AuthorityRegistration::default()
                },
            )),
        })
        .await
        .unwrap();
    let mut commands = AuthorityWorkerApiClient::new(rpc.worker.clone())
        .connect(stream::unfold(incoming, |mut incoming| async {
            incoming.recv().await.map(|update| (update, incoming))
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        commands.message().await.unwrap().unwrap().update,
        Some(authority_coordinator_update::Update::Registration(_))
    ));
    (updates, commands)
}

async fn dispatch(commands: &mut Streaming<AuthorityCoordinatorUpdate>) -> AuthorityCommand {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(authority_coordinator_update::Update::Command(command)) =
                commands.message().await.unwrap().unwrap().update
                && matches!(
                    command.command,
                    Some(authority_command::Command::Dispatch(_))
                )
            {
                break command;
            }
        }
    })
    .await
    .expect("worker did not receive dispatch")
}

#[nativelink_macro::nativelink_test]
async fn accepted_publication_replay_uses_persisted_algorithm_after_restart() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let (fixture, stores) = Fixture::new().await;
        let faults = Arc::new(QualificationFaults::default());
        faults.hold_publications.store(true, Ordering::Relaxed);
        let rpc = FixedRpc::with_faults(&fixture, &stores, faults).await;
        let key = action(&rpc, algorithm).await;
        let unrelated = result(b"other algorithm must survive publication");
        cache_put(&rpc, key, other(algorithm), &unrelated).await;
        assert_eq!(
            cache_get(&rpc, key, algorithm).await.unwrap_err().code(),
            RpcCode::NotFound
        );
        let (updates, mut commands) = worker(&rpc).await;
        let mut operations = ExecutionClient::new(rpc.client.clone())
            .execute(execute(key, algorithm, true))
            .await
            .unwrap()
            .into_inner();
        let command = dispatch(&mut commands).await;
        let accepted = result(b"accepted canonical worker result");
        updates
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(
                    AuthorityAttemptReport {
                        attempt: command.attempt,
                        dispatch_effect_id: command.effect_id,
                        observation_sequence: 1,
                        phase: AuthorityAttemptPhase::Quiescent as i32,
                        result: Some(authority_attempt_report::Result::ExecuteResponse(
                            ExecuteResponse {
                                result: Some(accepted.clone()),
                                ..ExecuteResponse::default()
                            },
                        )),
                        ..AuthorityAttemptReport::default()
                    },
                )),
            })
            .await
            .unwrap();
        state(&rpc, |state| {
            state
                .executions
                .iter()
                .any(|execution| execution.terminal.is_some())
                && state
                    .effects
                    .iter()
                    .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        })
        .await;
        let response = terminal(&mut operations).await;
        assert!(!response.cached_result);
        assert_eq!(response.result, Some(accepted.clone()));
        assert_eq!(
            cache_get(&rpc, key, algorithm).await.unwrap_err().code(),
            RpcCode::NotFound
        );
        drop(operations);
        drop(commands);
        drop(updates);
        rpc.shutdown().await;

        // Reopen the real singleton journal while retaining the canonical
        // filesystem role capabilities. No RPC request context survives this.
        let replay = FixedRpc::new(&fixture, &stores).await;
        state(&replay, |state| {
            !state
                .effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        })
        .await;
        assert_eq!(cache_get(&replay, key, algorithm).await.unwrap(), accepted);
        assert_eq!(
            cache_get(&replay, key, other(algorithm)).await.unwrap(),
            unrelated
        );
        replay.shutdown().await;
    }
}
