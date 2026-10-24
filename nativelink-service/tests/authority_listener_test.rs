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

//! Actual local authority/CAS/`ByteStream`/AC routes. The manual wire worker is not
//! a process-quiescence witness. Plaintext sockets deliberately demonstrate that
//! fixed routes and separate stores do not certify a credential boundary.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::{StreamExt, stream};
use nativelink_config::stores::MemorySpec;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest, Command, Digest, Directory,
    ExecuteRequest, ExecuteResponse, GetActionResultRequest, GetCapabilitiesRequest,
    UpdateActionResultRequest, batch_update_blobs_request, digest_function,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapability, AuthorityReconciliation,
    AuthorityRegistration, AuthorityResources, AuthoritySession, AuthorityWorkerIdentity,
    AuthorityWorkerUpdate, authority_attempt_report, authority_command, authority_coordinator_update,
    authority_worker_update,
};
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{ReadRequest, WriteRequest};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::{ClusterEpoch, EffectKind, Limits, ObservableState};
use nativelink_service::authority_listener::AuthorityListeners;
use nativelink_service::authority_server::{
    AuthorityService, CoordinatorTask, QualificationConfig, QualificationFaults,
};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::background_spawn;
use nativelink_util::common::make_temp_path;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use nativelink_util::store_trait::Store;
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Server};
use tonic::{Code, Streaming};

const INSTANCE: &str = "authority-listener-test";

#[path = "authority_listener/tls.rs"]
mod tls;

#[path = "authority_listener/limits.rs"]
mod limits;

#[path = "authority_listener/cas_admission.rs"]
mod cas_admission;

#[path = "authority_listener/bytestream_identity.rs"]
mod bytestream_identity;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = PathBuf::from(make_temp_path("authority-listeners"));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _cleanup = std::fs::remove_dir_all(&self.0);
    }
}

fn config(cas: Store, publication_sink: Store) -> QualificationConfig {
    QualificationConfig {
        instance: INSTANCE.to_owned(),
        principal: "local-listener-fixture".to_owned(),
        cas,
        cas_namespace: [1; 32],
        publication_sink,
        now: Arc::new(|| Ok(1)),
        faults: Arc::new(QualificationFaults::default()),
    }
}

struct Runtime {
    _directory: TestDirectory,
    service: AuthorityService,
    coordinator: CoordinatorTask,
    worker: Channel,
    client: Channel,
    client_address: SocketAddr,
    stops: Vec<oneshot::Sender<()>>,
    servers: Vec<JoinHandle<Result<(), tonic::transport::Error>>>,
    faults: Arc<QualificationFaults>,
}

impl Runtime {
    async fn start() -> Self {
        Self::start_with_transports(Server::builder(), Server::builder(), None, None).await
    }

    async fn start_with_transports(
        worker_transport: Server,
        client_transport: Server,
        worker_tls: Option<ClientTlsConfig>,
        client_tls: Option<ClientTlsConfig>,
    ) -> Self {
        Self::start_with_store_and_transports(
            Store::new(MemoryStore::new(&MemorySpec::default())),
            worker_transport,
            client_transport,
            worker_tls,
            client_tls,
        )
        .await
    }

    async fn start_with_store_and_transports(
        cas: Store,
        worker_transport: Server,
        client_transport: Server,
        worker_tls: Option<ClientTlsConfig>,
        client_tls: Option<ClientTlsConfig>,
    ) -> Self {
        let directory = TestDirectory::new();
        let authority = Singleton::open(
            directory.0.join("authority.redb"),
            1,
            ClusterEpoch(1),
            Limits::default(),
        )
        .await
        .unwrap();
        let config = config(cas, Store::new(MemoryStore::new(&MemorySpec::default())));
        let faults = config.faults.clone();
        let listeners = AuthorityListeners::start(authority, config).await.unwrap();
        let worker_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker_address = worker_socket.local_addr().unwrap();
        let client_socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_address = client_socket.local_addr().unwrap();
        let (stop_worker, stopped_worker) = oneshot::channel();
        let (stop_client, stopped_client) = oneshot::channel();
        let worker_server = background_spawn!("fixed_worker_listener", async move {
            listeners
                .worker
                .serve(worker_transport, worker_socket, async {
                    let _stopped = stopped_worker.await;
                })
                .await
        });
        let client_server = background_spawn!("fixed_client_listener", async move {
            listeners
                .client
                .serve(client_transport, client_socket, async {
                    let _stopped = stopped_client.await;
                })
                .await
        });
        Self {
            _directory: directory,
            service: listeners.service,
            coordinator: listeners.coordinator,
            worker: endpoint(worker_address, worker_tls)
                .connect()
                .await
                .unwrap(),
            client: endpoint(client_address, client_tls)
                .connect()
                .await
                .unwrap(),
            client_address,
            stops: vec![stop_worker, stop_client],
            servers: vec![worker_server, client_server],
            faults,
        }
    }

    async fn shutdown(self) {
        self.coordinator.shutdown().await.unwrap();
        for stop in self.stops {
            let _stopped = stop.send(());
        }
        drop(self.worker);
        drop(self.client);
        for server in self.servers {
            tokio::time::timeout(Duration::from_secs(5), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    async fn wait_state(&self, predicate: impl Fn(&ObservableState) -> bool + Send + Sync) {
        let mut state = self.service.subscribe();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if predicate(&state.borrow()) {
                    return;
                }
                state.changed().await.unwrap();
            }
        })
        .await
        .expect("authority failed to reach expected committed state");
    }
}

fn endpoint(address: SocketAddr, tls: Option<ClientTlsConfig>) -> Endpoint {
    match tls {
        Some(tls) => Endpoint::from_shared(format!("https://{address}"))
            .unwrap()
            .tls_config(tls)
            .unwrap(),
        None => Endpoint::from_shared(format!("http://{address}")).unwrap(),
    }
}

fn digest(algorithm: DigestHasherFunc, bytes: &[u8]) -> Digest {
    let mut hasher = algorithm.hasher();
    hasher.update(bytes);
    hasher.finalize_digest().into()
}

async fn upload(channel: Channel, algorithm: DigestHasherFunc, bytes: Vec<u8>) -> Digest {
    let digest = digest(algorithm, &bytes);
    let response = ContentAddressableStorageClient::new(channel)
        .batch_update_blobs(BatchUpdateBlobsRequest {
            instance_name: INSTANCE.to_owned(),
            digest_function: algorithm.proto_digest_func().into(),
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(digest.clone()),
                data: bytes.into(),
                ..batch_update_blobs_request::Request::default()
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].status.as_ref().unwrap().code, 0);
    digest
}

fn cache_update(
    digest: &Digest,
    algorithm: DigestHasherFunc,
    exit_code: i32,
) -> UpdateActionResultRequest {
    UpdateActionResultRequest {
        instance_name: INSTANCE.to_owned(),
        action_digest: Some(digest.clone()),
        action_result: Some(ActionResult {
            exit_code,
            ..ActionResult::default()
        }),
        digest_function: algorithm.proto_digest_func().into(),
        ..UpdateActionResultRequest::default()
    }
}

fn cache_get(digest: &Digest, algorithm: DigestHasherFunc) -> GetActionResultRequest {
    GetActionResultRequest {
        instance_name: INSTANCE.to_owned(),
        action_digest: Some(digest.clone()),
        digest_function: algorithm.proto_digest_func().into(),
        ..GetActionResultRequest::default()
    }
}

#[nativelink_test]
async fn same_driver_rejected_and_consumed_authority_is_closed() {
    let directory = TestDirectory::new();
    let path = directory.0.join("authority.redb");
    let authority = Singleton::open(&path, 1, ClusterEpoch(1), Limits::default())
        .await
        .unwrap();
    let driver = MemoryStore::new(&MemorySpec::default());
    let result = AuthorityListeners::start(
        authority,
        config(Store::new(driver.clone()), Store::new(driver)),
    )
    .await;
    assert!(matches!(result, Err(error) if error.contains("same store driver")));
    // OpenRaft joins its core and timer at shutdown, but the state-machine
    // worker closes asynchronously after its command channel is dropped.
    let reopened = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match Singleton::open(&path, 1, ClusterEpoch(1), Limits::default()).await {
                Ok(authority) => break authority,
                Err(error) => {
                    assert!(error.contains("Database already open"), "{error}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    })
    .await
    .expect("rejected construction retained the journal");
    reopened.shutdown().await.unwrap();
}

async fn fixed_route_boundary(runtime: &Runtime, algorithm: DigestHasherFunc) {
    let capabilities = CapabilitiesClient::new(runtime.client.clone())
        .get_capabilities(GetCapabilitiesRequest {
            instance_name: INSTANCE.to_owned(),
        })
        .await
        .unwrap()
        .into_inner();
    let cache = capabilities.cache_capabilities.unwrap();
    let execution = capabilities.execution_capabilities.unwrap();
    let algorithms = vec![
        digest_function::Value::Sha256 as i32,
        digest_function::Value::Blake3 as i32,
    ];
    assert_eq!(cache.digest_functions, algorithms);
    assert!(
        cache
            .action_cache_update_capabilities
            .unwrap()
            .update_enabled
    );
    assert_eq!(cache.supported_compressors, Vec::<i32>::new());
    assert_eq!(cache.supported_batch_update_compressors, Vec::<i32>::new());
    assert!(!cache.split_blob_support);
    assert!(!cache.splice_blob_support);
    assert!(execution.exec_enabled);
    assert_eq!(execution.digest_functions, algorithms);
    assert_eq!(execution.supported_node_properties, Vec::<String>::new());
    let mut client_ac = ActionCacheClient::new(runtime.client.clone());
    let mut worker_ac = ActionCacheClient::new(runtime.worker.clone());
    let malicious = ActionResult {
        exit_code: 17,
        ..ActionResult::default()
    }
    .encode_to_vec();
    let key = digest(algorithm, &malicious);

    // The valid request succeeds on the client route and reaches no AC service
    // on the worker route. Record tonic's actual unserved-route status.
    client_ac
        .update_action_result(cache_update(&key, algorithm, 7))
        .await
        .unwrap();
    assert_eq!(
        worker_ac
            .update_action_result(cache_update(&key, algorithm, 17))
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        upload(runtime.worker.clone(), algorithm, malicious.clone()).await,
        key
    );
    assert_eq!(
        client_ac
            .get_action_result(cache_get(&key, algorithm))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );
    let read = ContentAddressableStorageClient::new(runtime.worker.clone())
        .batch_read_blobs(BatchReadBlobsRequest {
            instance_name: INSTANCE.to_owned(),
            digests: vec![key.clone()],
            digest_function: algorithm.proto_digest_func().into(),
            ..BatchReadBlobsRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(read.responses.len(), 1);
    assert_eq!(read.responses[0].status.as_ref().unwrap().code, 0);
    assert_eq!(read.responses[0].data.as_ref(), malicious.as_slice());

    let algorithm_segment = match algorithm {
        DigestHasherFunc::Sha256 => "",
        DigestHasherFunc::Blake3 => "blake3/",
    };
    let blob = format!("blobs/{algorithm_segment}{}/{}", key.hash, key.size_bytes);
    let mut bytestream = ByteStreamClient::new(runtime.worker.clone());
    let written = bytestream
        .write(stream::iter([WriteRequest {
            resource_name: format!("{INSTANCE}/uploads/listener-test/{blob}"),
            write_offset: 0,
            finish_write: true,
            data: malicious.clone().into(),
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.committed_size, key.size_bytes);
    let mut client_bytestream = ByteStreamClient::new(runtime.client.clone());
    let mut read = client_bytestream
        .read(ReadRequest {
            resource_name: format!("{INSTANCE}/{blob}"),
            read_offset: 0,
            read_limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = read.next().await {
        bytes.extend_from_slice(&chunk.unwrap().data);
    }
    assert_eq!(bytes, malicious);
    assert_eq!(
        client_ac
            .get_action_result(cache_get(&key, algorithm))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        7
    );

    // Negative control: the same unauthenticated peer can open the client
    // channel and write. Port separation is not worker-credential exclusion.
    let mut untrusted_client_route = ActionCacheClient::new(runtime.client.clone());
    untrusted_client_route
        .update_action_result(cache_update(&key, algorithm, 19))
        .await
        .unwrap();
    assert_eq!(
        client_ac
            .get_action_result(cache_get(&key, algorithm))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        19
    );
}

async fn accepted_publication(runtime: &Runtime, algorithm: DigestHasherFunc) {
    let mut client_ac = ActionCacheClient::new(runtime.client.clone());
    let input_root_digest = upload(
        runtime.client.clone(),
        algorithm,
        Directory::default().encode_to_vec(),
    )
    .await;
    let command_digest = upload(
        runtime.client.clone(),
        algorithm,
        Command {
            arguments: vec!["local-wire-candidate".to_owned()],
            ..Command::default()
        }
        .encode_to_vec(),
    )
    .await;
    let action_digest = upload(
        runtime.client.clone(),
        algorithm,
        Action {
            command_digest: Some(command_digest),
            input_root_digest: Some(input_root_digest),
            ..Action::default()
        }
        .encode_to_vec(),
    )
    .await;
    assert_eq!(
        client_ac
            .get_action_result(cache_get(&action_digest, algorithm))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );

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
    let mut worker = AuthorityWorkerApiClient::new(runtime.worker.clone());
    let mut commands = worker
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

    runtime
        .faults
        .hold_publications
        .store(true, Ordering::Relaxed);
    let mut execution = ExecutionClient::new(runtime.client.clone());
    let execute = ExecuteRequest {
        instance_name: INSTANCE.to_owned(),
        action_digest: Some(action_digest.clone()),
        digest_function: algorithm.proto_digest_func().into(),
        ..ExecuteRequest::default()
    };
    let mut operations = execution
        .execute(execute.clone())
        .await
        .unwrap()
        .into_inner();
    let command = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(authority_coordinator_update::Update::Command(command)) =
                commands.message().await.unwrap().unwrap().update
            {
                break command;
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        command.command,
        Some(authority_command::Command::Dispatch(_))
    ));
    let accepted = ActionResult {
        stdout_raw: b"accepted wire candidate".to_vec().into(),
        ..ActionResult::default()
    };
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
    runtime
        .wait_state(|state| {
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
    assert_eq!(
        client_ac
            .get_action_result(cache_get(&action_digest, algorithm))
            .await
            .unwrap_err()
            .code(),
        Code::NotFound
    );
    let completed = terminal_response(&mut operations).await;
    assert_eq!(completed.result, Some(accepted.clone()));
    assert!(!completed.cached_result);

    runtime
        .faults
        .hold_publications
        .store(false, Ordering::Relaxed);
    runtime
        .wait_state(|state| {
            !state
                .effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        })
        .await;
    assert_eq!(
        client_ac
            .get_action_result(cache_get(&action_digest, algorithm))
            .await
            .unwrap()
            .into_inner(),
        accepted
    );
    let mut cached = execution.execute(execute).await.unwrap().into_inner();
    let completed = terminal_response(&mut cached).await;
    assert!(completed.cached_result);
    assert_eq!(completed.result, Some(accepted));
}

async fn terminal_response(operations: &mut Streaming<Operation>) -> ExecuteResponse {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let operation = operations.message().await.unwrap().unwrap();
            if operation.done {
                let Some(operation::Result::Response(response)) = operation.result else {
                    panic!("expected result response")
                };
                break ExecuteResponse::decode(response.value.as_slice()).unwrap();
            }
        }
    })
    .await
    .unwrap()
}

async fn fixed_routes_and_accepted_publication(algorithm: DigestHasherFunc) {
    let runtime = Runtime::start().await;
    fixed_route_boundary(&runtime, algorithm).await;
    accepted_publication(&runtime, algorithm).await;
    runtime.shutdown().await;
}

#[nativelink_test]
async fn sha256_fixed_routes_and_accepted_publication() {
    fixed_routes_and_accepted_publication(DigestHasherFunc::Sha256).await;
}

#[nativelink_test]
async fn blake3_fixed_routes_and_accepted_publication() {
    fixed_routes_and_accepted_publication(DigestHasherFunc::Blake3).await;
}

#[path = "authority_listener/operations.rs"]
mod operations;
