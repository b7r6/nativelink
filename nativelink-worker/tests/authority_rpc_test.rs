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

//! Actual RPC ingress, automatic committed coordinator, retained recipient and
//! real process fixture. This does not mount a production construction path.
#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::{StreamExt, stream};
use nativelink_util::background_spawn;
use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::{Execution, ExecutionServer};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{Action, ActionResult, Command, Directory, ExecuteOperationMetadata, ExecuteRequest, ExecuteResponse, Platform, RequestMetadata, UpdateActionResultRequest, WaitExecutionRequest};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::AuthorityWorkerApiServer;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityResources, AuthorityWorkerIdentity};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::journal::{Journal, Singleton};
use nativelink_scheduler::authority::format::{AdmissionRegistry, AuthorityDomain, JournalProfile, ReaderCapabilities, RECEIPT_RETIREMENT};
use nativelink_scheduler::authority::{ClusterEpoch, EffectKind, Event, Limits, ObservableState, Phase, RecordedEvent, Reply, Resources, ReceiptRetirement, WorkerId};
use nativelink_service::authority_server::{AuthorityService, CoordinatorTask, QualificationConfig, QualificationFaults};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::authority_worker::AuthorityWorker;
use nativelink_worker::authority_worker_transport::run_reconnecting;
use nativelink_worker::namespace_utils;
use nativelink_worker::running_actions_manager::{AuthorityRunningActionsManagerArgs, ExecutionConfiguration, PersistentWorkersSettings, RunningActionsManagerArgs, RunningActionsManagerImpl, UseNamespaces};
use opentelemetry::context::FutureExt;
use prost::Message;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Streaming};

#[path = "authority_operations_rpc/cases.rs"]
mod operations;

#[path = "authority_deadline_rpc/cases.rs"]
mod deadlines;

const EPOCH: ClusterEpoch = ClusterEpoch(1);
const IDENTITY: AuthorityWorkerIdentity = AuthorityWorkerIdentity {
    cluster_epoch: 1,
    worker_id: 2,
    incarnation_id: 3,
};
const CAPACITY: Resources = Resources {
    cpu_millis: 2000,
    memory_bytes: 1024 * 1024 * 1024,
    slots: 2,
};
const fn resources(value: Resources) -> AuthorityResources {
    AuthorityResources {
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        slots: value.slots,
    }
}

struct Context {
    directory: TempDir,
    cas: Arc<FastSlowStore>,
    ac: Arc<MemoryStore>,
    history: Arc<MemoryStore>,
    worker: Arc<AuthorityWorker<RunningActionsManagerImpl>>,
    authority_incarnation: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
enum ManagerConstruction {
    LegacyPublication,
    AuthorityOnly,
    AuthorityOnlyBounded { payload_bytes: usize },
}

impl Context {
    async fn new(single_use: bool) -> Self {
        Self::with_slow_store(
            single_use,
            Store::new(MemoryStore::new(&MemorySpec::default())),
            Duration::from_secs(10),
        )
        .await
    }

    async fn with_slow_store(single_use: bool, slow: Store, download_timeout: Duration) -> Self {
        Self::with_worker_bounds(
            single_use,
            slow,
            download_timeout,
            8,
            Duration::from_secs(30),
        )
        .await
    }

    async fn with_worker_bounds(
        single_use: bool,
        slow: Store,
        download_timeout: Duration,
        receipts: usize,
        action_timeout: Duration,
    ) -> Self {
        Self::with_manager(
            single_use,
            slow,
            download_timeout,
            receipts,
            action_timeout,
            ManagerConstruction::LegacyPublication,
        )
        .await
    }

    async fn with_manager(
        single_use: bool,
        slow: Store,
        download_timeout: Duration,
        receipts: usize,
        action_timeout: Duration,
        construction: ManagerConstruction,
    ) -> Self {
        Self::with_manager_configuration(
            single_use,
            slow,
            download_timeout,
            receipts,
            action_timeout,
            construction,
            ExecutionConfiguration {
                persistent_workers: PersistentWorkersSettings {
                    enabled: false,
                    ..PersistentWorkersSettings::default()
                },
                ..ExecutionConfiguration::default()
            },
        )
        .await
    }

    async fn with_manager_configuration(
        single_use: bool,
        slow: Store,
        download_timeout: Duration,
        receipts: usize,
        action_timeout: Duration,
        construction: ManagerConstruction,
        execution_configuration: ExecutionConfiguration,
    ) -> Self {
        let directory = TempDir::new().unwrap();
        let filesystem = FilesystemSpec {
            content_path: directory
                .path()
                .join("content")
                .to_string_lossy()
                .into_owned(),
            temp_path: directory.path().join("temp").to_string_lossy().into_owned(),
            ..FilesystemSpec::default()
        };
        let fast: Arc<FilesystemStore> = FilesystemStore::new(&filesystem).await.unwrap();
        let cas = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(filesystem),
                slow: StoreSpec::Memory(MemorySpec::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                bypass_dedup_threshold_bytes: 0,
            },
            Store::new(fast),
            slow,
        );
        let ac = MemoryStore::new(&MemorySpec::default());
        let history = MemoryStore::new(&MemorySpec::default());
        let actions = directory.path().join("actions");
        std::fs::create_dir(&actions).unwrap();
        let execution = AuthorityRunningActionsManagerArgs {
            root_action_directory: actions.to_string_lossy().into_owned(),
            execution_configuration,
            cas_store: cas.clone(),
            max_action_timeout: action_timeout,
            max_upload_timeout: Duration::from_secs(10),
            max_download_timeout: download_timeout,
            max_cleanup_wait: Duration::from_secs(10),
            max_cleanup_backoff: Duration::from_millis(10),
            timeout_handled_externally: false,
            directory_cache: None,
            active_input_leases: true,
            readonly_input_mounts: Vec::new(),
            use_namespaces: UseNamespaces::Yes,
        };
        let manager = Arc::new(
            match construction {
                ManagerConstruction::AuthorityOnly
                | ManagerConstruction::AuthorityOnlyBounded { .. } => {
                    RunningActionsManagerImpl::new_authority(execution)
                }
                // Preserve the original bypass regressions with deliberately enabled
                // legacy publishers. The additive authority path never receives them.
                ManagerConstruction::LegacyPublication => {
                    RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                        root_action_directory: execution.root_action_directory,
                        execution_configuration: execution.execution_configuration,
                        cas_store: execution.cas_store,
                        ac_store: Some(Store::new(ac.clone())),
                        historical_store: Store::new(history.clone()),
                        upload_action_result_config: &UploadActionResultConfig {
                            upload_ac_results_strategy: UploadCacheResultsStrategy::Everything,
                            upload_historical_results_strategy: Some(
                                UploadCacheResultsStrategy::Everything,
                            ),
                            success_message_template: "legacy history link must not appear"
                                .to_owned(),
                            ..UploadActionResultConfig::default()
                        },
                        max_action_timeout: execution.max_action_timeout,
                        max_upload_timeout: execution.max_upload_timeout,
                        max_download_timeout: execution.max_download_timeout,
                        max_cleanup_wait: execution.max_cleanup_wait,
                        max_cleanup_backoff: execution.max_cleanup_backoff,
                        timeout_handled_externally: execution.timeout_handled_externally,
                        directory_cache: execution.directory_cache,
                        active_input_leases: execution.active_input_leases,
                        readonly_input_mounts: execution.readonly_input_mounts,
                        use_namespaces: execution.use_namespaces,
                    })
                }
            }
            .unwrap(),
        );
        let worker = AuthorityWorker::new(
            IDENTITY,
            single_use,
            resources(CAPACITY),
            receipts,
            match construction {
                ManagerConstruction::AuthorityOnlyBounded { payload_bytes } => payload_bytes,
                _ => 64 * 1024,
            },
            manager,
        )
        .unwrap();
        Self {
            directory,
            cas,
            ac,
            history,
            worker,
            authority_incarnation: AtomicU64::new(0),
        }
    }

    async fn request(&self, timeout: i64) -> (ExecuteRequest, DigestInfo, Vec<u8>) {
        let marker = self.directory.path().join("executed");
        let release = self.directory.path().join("release");
        let command = Command {
            arguments: vec![
                "/bin/sh".to_owned(),
                "-c".to_owned(),
                format!(
                    "printf x >> '{}'; while [ ! -f '{}' ]; do /bin/sleep 0.02; done; printf artifact > output; printf stdout; printf stderr >&2",
                    marker.display(),
                    release.display()
                ),
            ],
            output_files: vec!["output".to_owned()],
            ..Command::default()
        };
        self.command_request(timeout, command).await
    }

    async fn fast_request(&self, number: usize) -> ExecuteRequest {
        let marker = self.directory.path().join(format!("completed-{number}"));
        self.command_request(
            10,
            Command {
                arguments: vec![
                    "/bin/sh".to_owned(),
                    "-c".to_owned(),
                    format!("printf x >> '{}'; printf completed", marker.display()),
                ],
                ..Command::default()
            },
        )
        .await
        .0
    }

    async fn command_request(
        &self,
        timeout: i64,
        command: Command,
    ) -> (ExecuteRequest, DigestInfo, Vec<u8>) {
        self.command_request_with_policy(timeout, command, false)
            .await
    }

    async fn command_request_with_policy(
        &self,
        timeout: i64,
        command: Command,
        do_not_cache: bool,
    ) -> (ExecuteRequest, DigestInfo, Vec<u8>) {
        self.command_request_with_policy_and_algorithm(
            timeout,
            command,
            do_not_cache,
            DigestHasherFunc::Sha256,
        )
        .await
    }

    async fn command_request_with_policy_and_algorithm(
        &self,
        timeout: i64,
        command: Command,
        do_not_cache: bool,
        algorithm: DigestHasherFunc,
    ) -> (ExecuteRequest, DigestInfo, Vec<u8>) {
        let context = make_ctx_for_hash_func(algorithm).unwrap();
        async {
            let command_digest =
                serialize_and_upload_message(&command, self.cas.as_pin(), &mut algorithm.hasher())
                    .await
                    .unwrap();
            let input_root_digest = serialize_and_upload_message(
                &Directory::default(),
                self.cas.as_pin(),
                &mut algorithm.hasher(),
            )
            .await
            .unwrap();
            let action = Action {
                do_not_cache,
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input_root_digest.into()),
                timeout: Some(prost_types::Duration {
                    seconds: timeout,
                    nanos: 0,
                }),
                platform: Some(Platform {
                    properties: vec![
                        Property {
                            name: "cpu_count".to_owned(),
                            value: "1".to_owned(),
                        },
                        Property {
                            name: "memory_kb".to_owned(),
                            value: "1024".to_owned(),
                        },
                    ],
                }),
                ..Action::default()
            };
            let mut bytes = action.encode_to_vec();
            // A valid unknown protobuf field is retained in original CAS bytes.
            bytes.extend_from_slice(&[0xf8, 0xf0, 0x04, 1]);
            let mut hasher = algorithm.hasher();
            hasher.update(&bytes);
            let digest = hasher.finalize_digest();
            self.cas
                .update_oneshot(digest, bytes.clone().into())
                .await
                .unwrap();
            (
                ExecuteRequest {
                    instance_name: "rpc-test".to_owned(),
                    action_digest: Some(digest.into()),
                    digest_function: algorithm.proto_digest_func().into(),
                    skip_cache_lookup: true,
                    ..ExecuteRequest::default()
                },
                digest,
                bytes,
            )
        }
        .with_context(context)
        .await
    }

    fn release(&self) {
        std::fs::write(self.directory.path().join("release"), b"go").unwrap();
    }
    fn assert_once(&self) {
        assert_eq!(
            std::fs::read(self.directory.path().join("executed")).unwrap(),
            b"x"
        );
    }
}

struct Runtime {
    service: AuthorityService,
    task: CoordinatorTask,
    client: ExecutionClient<Channel>,
    worker_channel: Channel,
    worker_address: SocketAddr,
    client_address: SocketAddr,
    servers: Vec<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl Runtime {
    async fn start(
        context: &Context,
        clock: Arc<AtomicU64>,
        faults: Arc<QualificationFaults>,
        worker_address: Option<SocketAddr>,
    ) -> Self {
        Self::start_with_receipts(context, clock, faults, worker_address, false).await
    }

    async fn start_with_receipts(
        context: &Context,
        clock: Arc<AtomicU64>,
        faults: Arc<QualificationFaults>,
        worker_address: Option<SocketAddr>,
        activated: bool,
    ) -> Self {
        let authority = if activated {
            let domain = AuthorityDomain {
                epoch: EPOCH,
                namespace: 73,
            };
            let path = context.directory.path().join("authority.redb");
            let admission = if path.exists() {
                // Runtime::shutdown has stopped the previous local member and
                // closed its journal. Reconstruct the fixture admission registry
                // from that artifact before admitting the later incarnation.
                Journal::recover_admission(
                    domain,
                    ReaderCapabilities::ReceiptRetirement,
                    vec![(1, path.clone())],
                )
                .await
                .unwrap()
            } else {
                AdmissionRegistry::new(domain)
            };
            let authority = Singleton::open_with_profile(
                path,
                1,
                Limits::default(),
                JournalProfile {
                    domain,
                    incarnation: context
                        .authority_incarnation
                        .fetch_add(1, Ordering::Relaxed)
                        + 1,
                    reader: ReaderCapabilities::ReceiptRetirement,
                    admission,
                },
            )
            .await
            .unwrap();
            if !authority.read().await.unwrap().receipt_retirement_enabled() {
                authority
                    .journal
                    .activate_features(&authority.raft, BTreeSet::from([RECEIPT_RETIREMENT]))
                    .await
                    .unwrap();
            }
            let format = authority.journal.active_format().await.unwrap();
            assert!(format.required.contains(&RECEIPT_RETIREMENT));
            assert!(format.feature_epoch > 0);
            assert!(authority.read().await.unwrap().receipt_retirement_enabled());
            authority
        } else {
            Singleton::open(
                context.directory.path().join("authority.redb"),
                1,
                EPOCH,
                Limits::default(),
            )
            .await
            .unwrap()
        };
        Self::start_authority(context, clock, faults, worker_address, authority, [0; 32]).await
    }

    async fn start_authority(
        context: &Context,
        clock: Arc<AtomicU64>,
        faults: Arc<QualificationFaults>,
        worker_address: Option<SocketAddr>,
        authority: Singleton,
        cas_namespace: [u8; 32],
    ) -> Self {
        let (service, task) = AuthorityService::start(
            authority,
            QualificationConfig {
                instance: "rpc-test".to_owned(),
                principal: "trusted-local-fixture".to_owned(),
                cas: Store::new(context.cas.clone()),
                cas_namespace,
                publication_sink: Store::new(context.ac.clone()),
                now: Arc::new(move || Ok(clock.load(Ordering::Relaxed))),
                faults,
            },
        )
        .await
        .unwrap();
        let listener =
            TcpListener::bind(worker_address.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap()))
                .await
                .unwrap();
        let worker_address = listener.local_addr().unwrap();
        let incoming = stream::unfold(listener, |listener| async {
            Some((
                listener.accept().await.map(|(connection, _)| connection),
                listener,
            ))
        });
        let worker_server = background_spawn!(
            "authority_rpc_fixture",
            Server::builder()
                .add_service(AuthorityWorkerApiServer::new(service.clone()))
                .serve_with_incoming(incoming),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client_address = listener.local_addr().unwrap();
        let incoming = stream::unfold(listener, |listener| async {
            Some((
                listener.accept().await.map(|(connection, _)| connection),
                listener,
            ))
        });
        let client_server = background_spawn!(
            "authority_rpc_fixture",
            Server::builder()
                .add_service(
                    ExecutionServer::new(service.clone()).max_encoding_message_size(128 * 1024)
                )
                .add_service(
                    nativelink_proto::google::longrunning::operations_server::OperationsServer::new(
                        service.clone()
                    )
                )
                .serve_with_incoming(incoming),
        );
        let client = ExecutionClient::new(
            Endpoint::from_shared(format!("http://{client_address}"))
                .unwrap()
                .connect()
                .await
                .unwrap(),
        );
        let worker_channel = Endpoint::from_shared(format!("http://{worker_address}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(1))
            .connect()
            .await
            .unwrap();
        Self {
            service,
            task,
            client,
            worker_channel,
            worker_address,
            client_address,
            servers: vec![worker_server, client_server],
        }
    }

    async fn wait(
        &self,
        predicate: impl Fn(&ObservableState) -> bool + Send + Sync,
    ) -> Arc<ObservableState> {
        let mut state = self.service.subscribe();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let value = state.borrow_and_update().clone();
                if predicate(&value) {
                    return value;
                }
                state.changed().await.unwrap();
            }
        })
        .await
        .unwrap()
    }

    async fn shutdown(self) {
        for server in &self.servers {
            server.abort();
        }
        for server in self.servers {
            let _ignored = server.await;
        }
        self.task.shutdown().await.unwrap();
    }

    async fn wait_receipts(
        &self,
        predicate: impl Fn(&ReceiptRetirement) -> bool + Send + Sync,
    ) -> Arc<Option<ReceiptRetirement>> {
        let mut receipts = self.service.subscribe_receipts();
        let outcome = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let value = receipts.borrow_and_update().clone();
                if value.as_ref().as_ref().is_some_and(&predicate) {
                    return value;
                }
                receipts.changed().await.unwrap();
            }
        })
        .await;
        outcome.unwrap_or_else(|error| {
            panic!(
                "Receipt wait failed: {error}; committed metadata: {:?}",
                receipts.borrow()
            )
        })
    }
}

async fn next_operation(stream: &mut Streaming<Operation>) -> Operation {
    tokio::time::timeout(Duration::from_secs(20), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn terminal(stream: &mut Streaming<Operation>) -> Operation {
    loop {
        let operation = next_operation(stream).await;
        if operation.done {
            return operation;
        }
    }
}

async fn receipt_terminal(
    runtime: &Runtime,
    context: &Context,
    stream: &mut Streaming<Operation>,
) -> Operation {
    loop {
        let operation = tokio::time::timeout(Duration::from_secs(20), stream.message()).await.unwrap_or_else(|error| {
            let view = runtime.service.subscribe().borrow().clone();
            let receipts = runtime.service.subscribe_receipts().borrow().clone();
            let executions: Vec<_> = view.executions.iter().map(|execution| (execution.id, execution.phase, execution.attempt, execution.terminal.is_some())).collect();
            panic!("Receipt RPC wait failed: {error}; workers={:?}; executions={executions:?}; last_attempt={:?}; receipt_metadata={receipts:?}; recipient={:?}", view.workers, view.attempts.last(), context.worker.reconcile());
        }).unwrap().unwrap();
        if operation.done {
            return operation;
        }
    }
}

fn response(operation: &Operation) -> ExecuteResponse {
    let Some(operation::Result::Response(response)) = &operation.result else {
        panic!("Expected full ExecuteResponse: {operation:?}");
    };
    ExecuteResponse::decode(response.value.as_slice()).unwrap()
}

fn namespaces() -> bool {
    if namespace_utils::namespaces_supported(false, false) {
        true
    } else {
        eprintln!(
            "SKIP: namespaces unavailable; actual RPC/quiescent acceptance remains unqualified"
        );
        false
    }
}

#[nativelink_test]
async fn activated_receipt_outbox_survives_rpc_restart_cutpoints_and_old_active_hole() {
    if !namespaces() {
        return;
    }
    let context = Context::with_worker_bounds(
        false,
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Duration::from_secs(10),
        4,
        Duration::from_mins(15),
    )
    .await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_receipt_issues.store(true, Ordering::Relaxed);
    let mut runtime =
        Runtime::start_with_receipts(&context, clock.clone(), faults, None, true).await;
    let address = runtime.worker_address;
    let (stop, stopped) = watch::channel(false);
    let supervisor = background_spawn!(
        "authority_rpc_fixture",
        run_reconnecting(
            context.worker.clone(),
            Endpoint::from_shared(format!("http://{address}"))
                .unwrap()
                .connect_timeout(Duration::from_secs(1)),
            1,
            Vec::new(),
            stopped,
        )
    );
    runtime.wait(|view| view.workers.len() == 1).await;
    let (request, _, _) = context.request(900).await;
    let mut old_stream = runtime.client.execute(request).await.unwrap().into_inner();
    let old_name = next_operation(&mut old_stream).await.name;
    let running = runtime
        .wait(|view| {
            view.attempts
                .first()
                .is_some_and(|attempt| attempt.phase == Phase::Running)
        })
        .await;
    let old = running.attempts[0];
    context.assert_once();
    let mut completed = runtime
        .client
        .execute(context.fast_request(0).await)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response(&receipt_terminal(&runtime, &context, &mut completed).await)
            .result
            .unwrap()
            .exit_code,
        0
    );
    let released = runtime
        .wait_receipts(|receipts| {
            receipts
                .streams
                .get(&WorkerId(2))
                .is_some_and(|stream| stream.dirty && stream.pending.is_none())
        })
        .await;
    assert!(
        released.as_ref().as_ref().unwrap().streams[&WorkerId(2)]
            .acknowledged
            .is_none()
    );
    assert!(context.worker.reconcile().receipt_retirement.is_none());
    assert_eq!(context.worker.ledger_counts_for_test(), (2, 2, 2, 0));

    eprintln!("AUTOMATIC_RECEIPT_CUT: released before issuance");
    // An accepted result alone cannot tell the recipient to discard its receipt.
    // Stop before IssueReceipts is committed, then recover the actual journal.
    runtime.shutdown().await;
    drop(old_stream);
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_receipt_delivery.store(true, Ordering::Relaxed);
    faults
        .hold_receipt_acknowledgments
        .store(true, Ordering::Relaxed);
    runtime =
        Runtime::start_with_receipts(&context, clock.clone(), faults, Some(address), true).await;
    let issued = runtime
        .wait_receipts(|receipts| {
            receipts
                .streams
                .get(&WorkerId(2))
                .is_some_and(|stream| stream.pending.is_some())
        })
        .await;
    let certificate = issued.as_ref().as_ref().unwrap().streams[&WorkerId(2)]
        .pending
        .clone()
        .unwrap();
    assert_eq!(certificate.retain.len(), 1);
    assert_eq!(certificate.retain[0].attempt, old.id);
    assert_eq!(certificate.retain[0].dispatch_effect, old.dispatch_effect);
    assert!(context.worker.reconcile().receipt_retirement.is_none());
    assert_eq!(context.worker.ledger_counts_for_test(), (2, 2, 2, 0));

    eprintln!("AUTOMATIC_RECEIPT_CUT: issued before wire");
    // The certificate now exists only as committed intent, before wire delivery.
    runtime.shutdown().await;
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_receipt_delivery.store(true, Ordering::Relaxed);
    runtime =
        Runtime::start_with_receipts(&context, clock.clone(), faults.clone(), Some(address), true)
            .await;
    runtime
        .wait(|_| faults.receipt_inventories.load(Ordering::Relaxed) >= 2)
        .await;
    let restored = runtime
        .wait_receipts(|receipts| {
            receipts
                .streams
                .get(&WorkerId(2))
                .is_some_and(|stream| stream.pending.as_ref() == Some(&certificate))
        })
        .await;
    assert_eq!(
        restored.as_ref().as_ref().unwrap().streams[&WorkerId(2)].pending,
        Some(certificate.clone())
    );
    // Registration and the first healthy inventory both omitted the certificate.
    // Neither observation acknowledges the still-undelivered pending body.
    faults
        .hold_receipt_acknowledgments
        .store(true, Ordering::Relaxed);
    faults.hold_receipt_delivery.store(false, Ordering::Relaxed);
    let mut changed = context.worker.subscribe();
    let wire_certificate = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(applied) = context.worker.reconcile().receipt_retirement
                && applied.effect_id == certificate.effect.0
            {
                break applied;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        wire_certificate.attempts_through,
        certificate.attempts_through.0
    );
    assert_eq!(
        wire_certificate.effects_through,
        certificate.effects_through.0
    );
    assert_eq!(
        wire_certificate.reservations_through,
        certificate.reservations_through.0
    );
    assert_eq!(wire_certificate.retain.len(), 1);
    let binding = &wire_certificate.retain[0];
    assert_eq!(binding.attempt_id, old.id.0);
    assert_eq!(binding.execution_id, old.execution.0);
    assert_eq!(binding.dispatch_effect_id, old.dispatch_effect.0);
    assert_eq!(binding.reservation_id, certificate.retain[0].reservation.0);
    assert_eq!(binding.cancel_effect_id, 0);
    assert_eq!(context.worker.ledger_counts_for_test(), (1, 1, 1, 1));
    let unacknowledged = runtime.service.subscribe_receipts().borrow().clone();
    assert_eq!(
        unacknowledged.as_ref().as_ref().unwrap().streams[&WorkerId(2)].pending,
        Some(certificate.clone())
    );

    eprintln!("AUTOMATIC_RECEIPT_CUT: applied before acknowledgment");
    // Worker application is not an authority acknowledgement until that exact
    // body's authenticated inventory commits. Lose the coordinator at this cut.
    let applied_session = context.worker.reconcile().session.unwrap().session_id;
    runtime.shutdown().await;
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_receipt_delivery.store(true, Ordering::Relaxed);
    faults
        .hold_receipt_acknowledgments
        .store(true, Ordering::Relaxed);
    runtime =
        Runtime::start_with_receipts(&context, clock, faults.clone(), Some(address), true).await;
    runtime
        .wait(|view| {
            view.workers
                .first()
                .is_some_and(|worker| worker.session.0 > applied_session)
        })
        .await;
    let restored = runtime.service.subscribe_receipts().borrow().clone();
    assert_eq!(
        restored.as_ref().as_ref().unwrap().streams[&WorkerId(2)].pending,
        Some(certificate.clone())
    );
    assert_eq!(
        context.worker.reconcile().receipt_retirement,
        Some(wire_certificate)
    );
    assert_eq!(context.worker.ledger_counts_for_test(), (1, 1, 1, 1));
    faults
        .hold_receipt_acknowledgments
        .store(false, Ordering::Relaxed);
    faults.hold_receipt_delivery.store(false, Ordering::Relaxed);
    runtime
        .wait_receipts(|receipts| {
            receipts.streams.get(&WorkerId(2)).is_some_and(|stream| {
                stream.pending.is_none()
                    && stream
                        .acknowledged
                        .is_some_and(|floor| floor.effect == certificate.effect)
            })
        })
        .await;

    eprintln!("AUTOMATIC_RECEIPT_CUT: explicit acknowledgment");
    let mut previous = certificate.effect.0;
    for number in 1..16 {
        let mut stream = runtime
            .client
            .execute(context.fast_request(number).await)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response(&receipt_terminal(&runtime, &context, &mut stream).await)
                .result
                .unwrap()
                .exit_code,
            0
        );
        eprintln!("AUTOMATIC_RECEIPT_PROGRESS: completed={number} previous={previous}");
        let acknowledged = runtime
            .wait_receipts(|receipts| {
                receipts.streams.get(&WorkerId(2)).is_some_and(|stream| {
                    !stream.dirty
                        && stream.pending.is_none()
                        && stream
                            .acknowledged
                            .is_some_and(|floor| floor.effect.0 > previous)
                })
            })
            .await;
        previous = acknowledged.as_ref().as_ref().unwrap().streams[&WorkerId(2)]
            .acknowledged
            .unwrap()
            .effect
            .0;
        let inventory = context.worker.reconcile();
        assert_eq!(inventory.receipt_retirement.unwrap().effect_id, previous);
        assert_eq!(context.worker.ledger_counts_for_test(), (1, 1, 1, 1));
        assert_eq!(inventory.attempts[0].attempt.unwrap().attempt_id, old.id.0);
        assert_ne!(
            inventory.attempts[0].phase,
            AuthorityAttemptPhase::Quiescent as i32
        );
        assert!(!context.worker.admission_closed());
        context.assert_once();
    }
    for number in 0..16 {
        assert_eq!(
            std::fs::read(context.directory.path().join(format!("completed-{number}"))).unwrap(),
            b"x"
        );
    }
    context.release();
    let mut old_stream = runtime
        .client
        .wait_execution(WaitExecutionRequest { name: old_name })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response(&receipt_terminal(&runtime, &context, &mut old_stream).await)
            .result
            .unwrap()
            .exit_code,
        0
    );
    runtime
        .wait_receipts(|receipts| {
            receipts.streams.get(&WorkerId(2)).is_some_and(|stream| {
                !stream.dirty
                    && stream.pending.is_none()
                    && stream
                        .acknowledged
                        .is_some_and(|floor| floor.effect.0 > previous)
            })
        })
        .await;
    assert_eq!(context.worker.ledger_counts_for_test(), (0, 0, 0, 0));
    assert!(!context.worker.admission_closed());
    let completed = runtime
        .wait(|view| view.workers[0].charged == Resources::default())
        .await;
    assert!(
        completed
            .reservations
            .iter()
            .all(|reservation| reservation.released)
    );
    stop.send_replace(true);
    supervisor.await.unwrap().unwrap();
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_AUTOMATIC_RECEIPTS: feature2 activated through actual Raft prepare/activate; beforeissue/beforewire/lostack/reconnect durableoutbox cutpoints; 16 actual quiescent completions beside originalactivehole with ledgerlimit4; exactpendingbody replay and explicit acknowledgment; allreceipts finally retired"
    );
}

#[nativelink_test]
async fn actual_rpc_restart_after_acceptance_retains_candidate_and_publishes_accepted_result() {
    if !namespaces() {
        return;
    }
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock.clone(), faults.clone(), None).await;
    let address = runtime.worker_address;
    let (stop, stopped) = watch::channel(false);
    let supervisor = background_spawn!(
        "authority_rpc_fixture",
        run_reconnecting(
            context.worker.clone(),
            Endpoint::from_shared(format!("http://{}", runtime.worker_address))
                .unwrap()
                .connect_timeout(Duration::from_secs(1)),
            1,
            Vec::new(),
            stopped,
        )
    );
    runtime.wait(|view| view.workers.len() == 1).await;
    let (execute, action_digest, original_bytes) = context.request(30).await;
    let mut request = Request::new(execute.clone());
    request.metadata_mut().insert_bin(
        "build.bazel.remote.execution.v2.requestmetadata-bin",
        tonic::metadata::MetadataValue::from_bytes(
            &RequestMetadata {
                tool_invocation_id: "rpc-invocation".to_owned(),
                ..RequestMetadata::default()
            }
            .encode_to_vec(),
        ),
    );
    let mut stream = runtime.client.execute(request).await.unwrap().into_inner();
    let name = next_operation(&mut stream).await.name;
    let accepted = runtime
        .wait(|view| {
            view.attempts
                .first()
                .is_some_and(|attempt| attempt.phase == Phase::Running)
                && !view
                    .effects
                    .iter()
                    .any(|effect| matches!(effect.kind, EffectKind::Dispatch { .. }))
        })
        .await;
    assert_eq!(
        accepted.executions[0]
            .request
            .require_payload()
            .unwrap()
            .action_bytes(),
        original_bytes
    );
    let dispatch = accepted.attempts[0].dispatch_effect;
    assert_ne!(dispatch.0, 0);
    assert!(!accepted.reservations[0].released);
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    context.assert_once();
    // The outbox no longer contains Dispatch. Recovery must use the persisted
    // attempt receipt fence, not an Effect retained by the test coordinator.
    runtime.shutdown().await;
    drop(stream);
    context.release();
    let mut changed = context.worker.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if context.worker.reconcile().attempts.iter().any(|report| {
                report.phase == AuthorityAttemptPhase::Quiescent as i32 && report.result.is_some()
            }) {
                break;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_publications.store(true, Ordering::Relaxed);
    runtime = Runtime::start(&context, clock.clone(), faults, Some(address)).await;
    let completed = runtime
        .wait(|view| {
            view.operations
                .first()
                .is_some_and(|operation| operation.terminal.is_some())
        })
        .await;
    assert_eq!(completed.attempts[0].dispatch_effect, dispatch);
    assert!(completed.reservations[0].released);
    assert_eq!(completed.workers[0].charged, Resources::default());
    assert!(completed.workers[0].session.0 > 1);
    let mut stream = runtime
        .client
        .wait_execution(WaitExecutionRequest { name: name.clone() })
        .await
        .unwrap()
        .into_inner();
    let final_operation = terminal(&mut stream).await;
    let full = response(&final_operation);
    assert_eq!(full.result.as_ref().unwrap().exit_code, 0);
    assert_eq!(full.result.as_ref().unwrap().output_files.len(), 1);
    assert_eq!(context.history.len_for_test(), 0);
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert!(
        completed
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    runtime.shutdown().await;
    runtime = Runtime::start(
        &context,
        clock,
        Arc::new(QualificationFaults::default()),
        Some(address),
    )
    .await;
    runtime.wait(|view| view.effects.is_empty()).await;
    let mut stream = runtime
        .client
        .wait_execution(WaitExecutionRequest { name })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(terminal(&mut stream).await, final_operation);
    assert_eq!(
        ActionResult::decode(
            context
                .ac
                .get_part_unchunked(action_digest, 0, None)
                .await
                .unwrap()
        )
        .unwrap(),
        full.result.unwrap()
    );
    let mut cache_request = execute;
    cache_request.skip_cache_lookup = false;
    let mut cached = runtime
        .client
        .execute(cache_request)
        .await
        .unwrap()
        .into_inner();
    let cached = response(&terminal(&mut cached).await);
    assert!(cached.cached_result);
    assert_eq!(cached.result.as_ref().unwrap().exit_code, 0);
    context.assert_once();
    // Real worker listener does not mount a mutable AC route.
    let mut ac_client = ActionCacheClient::new(runtime.worker_channel.clone());
    let rejected = ac_client
        .update_action_result(UpdateActionResultRequest {
            instance_name: "rpc-test".to_owned(),
            action_digest: Some(action_digest.into()),
            action_result: Some(ActionResult::default()),
            ..UpdateActionResultRequest::default()
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.code(), tonic::Code::Unimplemented);
    stop.send(true).unwrap();
    supervisor.await.unwrap().unwrap();
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_RPC: automatic match, accepted-dispatch restart, retained candidate, stable WaitExecution and publication restart exercised"
    );
}

#[nativelink_test]
async fn automatic_deadlines_cleanup_retirement_and_ingress_ids_survive_restart() {
    if !namespaces() {
        return;
    }
    let context = Context::new(true).await;
    let clock = Arc::new(AtomicU64::new(0));
    let mut runtime = Runtime::start(
        &context,
        clock.clone(),
        Arc::new(QualificationFaults::default()),
        None,
    )
    .await;
    let address = runtime.worker_address;
    let (stop, stopped) = watch::channel(false);
    let supervisor = background_spawn!(
        "authority_rpc_fixture",
        run_reconnecting(
            context.worker.clone(),
            Endpoint::from_shared(format!("http://{}", runtime.worker_address))
                .unwrap()
                .connect_timeout(Duration::from_secs(1)),
            1,
            Vec::new(),
            stopped,
        )
    );
    runtime.wait(|view| view.workers.len() == 1).await;
    let (execute, _, _) = context.request(5).await;
    let mut stream = runtime
        .client
        .execute(execute.clone())
        .await
        .unwrap()
        .into_inner();
    let original_name = next_operation(&mut stream).await.name;
    let running = runtime
        .wait(|view| {
            view.attempts
                .first()
                .is_some_and(|attempt| attempt.phase == Phase::Running)
        })
        .await;
    assert_eq!(running.attempts[0].running_deadline, Some(5));
    clock.store(6, Ordering::Relaxed);
    runtime
        .wait(|view| {
            view.attempts
                .first()
                .is_some_and(|attempt| !attempt.authorized)
                && view
                    .reservations
                    .first()
                    .is_some_and(|reservation| reservation.released)
        })
        .await;
    context.assert_once();
    assert_eq!(context.history.len_for_test(), 0);
    clock.store(301, Ordering::Relaxed);
    runtime
        .wait(|view| {
            view.operations
                .first()
                .is_some_and(|operation| operation.terminal.is_some())
        })
        .await;
    assert!(terminal(&mut stream).await.done);
    clock.store(602, Ordering::Relaxed);
    runtime.wait(|view| view.operations.is_empty()).await;
    runtime.shutdown().await;
    runtime = Runtime::start(
        &context,
        clock,
        Arc::new(QualificationFaults::default()),
        Some(address),
    )
    .await;
    let mut stream = runtime.client.execute(execute).await.unwrap().into_inner();
    let next_name = next_operation(&mut stream).await.name;
    assert_ne!(next_name, original_name);
    let old_id: u64 = original_name.rsplit('/').next().unwrap().parse().unwrap();
    let next_id: u64 = next_name.rsplit('/').next().unwrap().parse().unwrap();
    assert!(next_id > old_id);
    assert_eq!(
        runtime
            .client
            .wait_execution(WaitExecutionRequest {
                name: original_name
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    stop.send(true).unwrap();
    supervisor.await.unwrap().unwrap();
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_RPC: automatic execution timer, cancellation cleanup, interest expiry, retirement and persisted ingress watermark exercised"
    );
}

#[nativelink_test]
async fn actual_rpc_cancel_before_first_dispatch_releases_only_after_recipient_tombstone() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_dispatches.store(true, Ordering::Relaxed);
    let mut runtime = Runtime::start(&context, clock.clone(), faults, None).await;
    let (stop, stopped) = watch::channel(false);
    let supervisor = background_spawn!(
        "authority_rpc_fixture",
        run_reconnecting(
            context.worker.clone(),
            Endpoint::from_shared(format!("http://{}", runtime.worker_address)).unwrap(),
            1,
            Vec::new(),
            stopped,
        )
    );
    runtime.wait(|view| view.workers.len() == 1).await;
    let (execute, _, _) = context.request(30).await;
    let mut stream = runtime.client.execute(execute).await.unwrap().into_inner();
    let _operation = next_operation(&mut stream).await;
    let dispatching = runtime
        .wait(|view| {
            view.attempts
                .first()
                .is_some_and(|attempt| attempt.phase == Phase::Dispatching)
        })
        .await;
    assert!(!dispatching.reservations[0].released);
    assert_eq!(
        context.worker.reconcile().attempts,
        Vec::<AuthorityAttemptReport>::new()
    );
    clock.store(301, Ordering::Relaxed);
    let cleaned = runtime
        .wait(|view| {
            view.reservations
                .first()
                .is_some_and(|reservation| reservation.released)
        })
        .await;
    assert_eq!(cleaned.workers[0].charged, Resources::default());
    let inventory = context.worker.reconcile();
    assert_eq!(inventory.attempts.len(), 1);
    assert_eq!(inventory.attempts[0].dispatch_effect_id, 0);
    assert_ne!(inventory.attempts[0].cancel_effect_id, 0);
    assert_eq!(
        inventory.attempts[0].phase,
        AuthorityAttemptPhase::Quiescent as i32
    );
    assert!(!context.directory.path().join("executed").exists());
    stop.send(true).unwrap();
    supervisor.await.unwrap().unwrap();
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_RPC: cancel arrived before any dispatch, exact committed cancel receipt released the original grant"
    );
}

#[nativelink_test]
async fn wire_invalid_reports_and_cancel_fences_cannot_accept_or_refund_work() {
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
    use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
        AuthorityAttemptIdentity, AuthorityAttemptReport, AuthorityCapacityObservation,
        AuthorityDecline, AuthoritySession, AuthorityWorkerUpdate, authority_attempt_report,
        authority_worker_update,
    };
    use tokio::sync::mpsc;

    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_dispatches.store(true, Ordering::Relaxed);
    let mut runtime = Runtime::start(&context, clock.clone(), faults, None).await;
    let (sender, receiver) = mpsc::channel(8);
    let session = AuthoritySession {
        worker: Some(IDENTITY),
        session_id: 1,
    };
    sender
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Registration(
                context.worker.make_registration(1, Vec::new()).unwrap(),
            )),
        })
        .await
        .unwrap();
    let updates = stream::unfold(receiver, |mut receiver| async {
        receiver.recv().await.map(|value| (value, receiver))
    });
    let mut client = AuthorityWorkerApiClient::new(runtime.worker_channel.clone());
    let mut responses = client.connect(updates).await.unwrap().into_inner();
    assert!(responses.message().await.unwrap().is_some());
    let (execute, _, _) = context.request(30).await;
    let mut operation = runtime
        .client
        .execute(execute.clone())
        .await
        .unwrap()
        .into_inner();
    let name = next_operation(&mut operation).await.name;
    let dispatching = runtime.wait(|view| !view.attempts.is_empty()).await;
    let attempt = dispatching.attempts[0];
    let target = AuthorityAttemptIdentity {
        worker: Some(IDENTITY),
        execution_id: attempt.execution.0,
        attempt_id: attempt.id.0,
    };
    let report = AuthorityAttemptReport {
        attempt: Some(target),
        dispatch_effect_id: attempt.dispatch_effect.0,
        observation_sequence: 1,
        phase: 999,
        ..AuthorityAttemptReport::default()
    };
    sender
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Report(report.clone())),
        })
        .await
        .unwrap();
    sender
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Report(
                AuthorityAttemptReport {
                    phase: AuthorityAttemptPhase::Running as i32,
                    observation_sequence: 0,
                    ..report.clone()
                },
            )),
        })
        .await
        .unwrap();
    sender
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Report(
                AuthorityAttemptReport {
                    phase: AuthorityAttemptPhase::Running as i32,
                    dispatch_effect_id: report.dispatch_effect_id + 1,
                    ..report.clone()
                },
            )),
        })
        .await
        .unwrap();
    let capacity = |sequence, draining| AuthorityWorkerUpdate {
        update: Some(authority_worker_update::Update::Capacity(
            AuthorityCapacityObservation {
                session: Some(session),
                observation_sequence: sequence,
                capacity: Some(resources(CAPACITY)),
                admission_ceiling: Some(resources(CAPACITY)),
                draining,
            },
        )),
    };
    // Capacity is an ordering barrier on the same incoming HTTP/2 stream.
    sender.send(capacity(1, true)).await.unwrap();
    let unchanged = runtime.wait(|view| view.workers[0].draining).await;
    assert_eq!(unchanged.attempts[0], attempt);
    assert!(!unchanged.reservations[0].released);
    assert!(
        unchanged
            .effects
            .iter()
            .any(|effect| effect.id == attempt.dispatch_effect)
    );

    // The global bound applies before capture, and held observation streams
    // retain their permits. One worker + one Execute + 62 observers fill it.
    let mut observers = Vec::new();
    for _ in 0..62 {
        observers.push(
            runtime
                .client
                .wait_execution(WaitExecutionRequest { name: name.clone() })
                .await
                .unwrap()
                .into_inner(),
        );
    }
    assert_eq!(
        runtime
            .client
            .execute(ExecuteRequest::default())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    drop(observers);

    clock.store(301, Ordering::Relaxed);
    let revoked = runtime.wait(|view| !view.attempts[0].authorized).await;
    let cancel = revoked
        .effects
        .iter()
        .find(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
        .unwrap()
        .id;
    let closed = AuthorityAttemptReport {
        dispatch_effect_id: 0,
        cancel_effect_id: cancel.0 + 1,
        phase: AuthorityAttemptPhase::Quiescent as i32,
        observation_sequence: 2,
        ..report
    };
    sender
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Report(closed.clone())),
        })
        .await
        .unwrap();
    sender.send(capacity(2, false)).await.unwrap();
    let still_charged = runtime.wait(|view| !view.workers[0].draining).await;
    assert!(!still_charged.reservations[0].released);
    sender
        .send(AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Report(
                AuthorityAttemptReport {
                    cancel_effect_id: cancel.0,
                    observation_sequence: 3,
                    result: Some(authority_attempt_report::Result::Decline(
                        AuthorityDecline {
                            reason: 2,
                            detail: "recipient tombstone".to_owned(),
                        },
                    )),
                    ..closed
                },
            )),
        })
        .await
        .unwrap();
    let cleaned = runtime.wait(|view| view.reservations[0].released).await;
    assert_eq!(cleaned.workers[0].charged, Resources::default());
    for oversized in [
        authority_attempt_report::Result::ExecuteResponse(ExecuteResponse {
            result: Some(ActionResult {
                stdout_raw: vec![255; 64 * 1024 - 128].into(),
                ..ActionResult::default()
            }),
            ..ExecuteResponse::default()
        }),
        authority_attempt_report::Result::InternalError(nativelink_proto::google::rpc::Status {
            code: tonic::Code::Internal as i32,
            message: "\u{0001}".repeat(9000),
            details: Vec::new(),
        }),
    ] {
        let mut operation = runtime
            .client
            .execute(execute.clone())
            .await
            .unwrap()
            .into_inner();
        let _initial = next_operation(&mut operation).await;
        let pending = runtime
            .wait(|view| view.attempts.iter().any(|attempt| attempt.authorized))
            .await;
        let attempt = pending
            .attempts
            .iter()
            .find(|attempt| attempt.authorized)
            .unwrap();
        sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(
                    AuthorityAttemptReport {
                        attempt: Some(AuthorityAttemptIdentity {
                            worker: Some(IDENTITY),
                            execution_id: attempt.execution.0,
                            attempt_id: attempt.id.0,
                        }),
                        dispatch_effect_id: attempt.dispatch_effect.0,
                        observation_sequence: 1,
                        phase: AuthorityAttemptPhase::Quiescent as i32,
                        result: Some(oversized),
                        ..AuthorityAttemptReport::default()
                    },
                )),
            })
            .await
            .unwrap();
        let completed = response(&terminal(&mut operation).await);
        assert_eq!(
            completed.status.unwrap().code,
            tonic::Code::ResourceExhausted as i32
        );
        assert!(completed.result.is_none());
        let clean = runtime
            .wait(|view| view.workers[0].charged == Resources::default())
            .await;
        assert!(
            !clean
                .effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        );
    }
    assert!(!context.directory.path().join("executed").exists());
    drop(responses);
    drop(sender);
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_RPC: invalid phase/zero sequence/wrong dispatch rejected, stream concurrency bounded, wrong cancel cannot refund, revoked decline closes only exact cancel"
    );
}

#[nativelink_test]
async fn undersized_core_payload_refuses_rpc_constructor_and_closes_journal() {
    let context = Context::new(false).await;
    let path = context.directory.path().join("small-authority.redb");
    let limits = Limits {
        max_payload_bytes: 64 * 1024 - 1,
        ..Limits::default()
    };
    let authority = Singleton::open(&path, 1, EPOCH, limits).await.unwrap();
    let rejected = AuthorityService::start(
        authority,
        QualificationConfig {
            instance: "rpc-test".to_owned(),
            principal: "trusted-local-fixture".to_owned(),
            cas: Store::new(context.cas.clone()),
            cas_namespace: [0; 32],
            publication_sink: Store::new(context.ac.clone()),
            now: Arc::new(|| Ok(0)),
            faults: Arc::new(QualificationFaults::default()),
        },
    )
    .await
    .unwrap_err();
    assert!(rejected.contains("64 KiB"));
    Singleton::open(&path, 1, EPOCH, limits)
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

#[nativelink_test]
async fn quiet_rpc_reader_renews_one_interest_across_reconnect_and_restart() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock.clone(), faults.clone(), None).await;
    let (request, _, _) = context.request(30).await;
    let mut stream = runtime.client.execute(request).await.unwrap().into_inner();
    let mut queued = next_operation(&mut stream).await;
    let name = queued.name.clone();
    runtime
        .wait(|view| view.executions[0].phase == Phase::Queued)
        .await;
    // Drain any queued-stage notification before measuring identical heartbeats.
    while ExecuteOperationMetadata::decode(queued.metadata.as_ref().unwrap().value.as_slice())
        .unwrap()
        .stage
        != 2
    {
        queued = next_operation(&mut stream).await;
    }
    for now in (100..=1000).step_by(100) {
        clock.store(now, Ordering::Relaxed);
        assert_eq!(next_operation(&mut stream).await, queued);
        let live = runtime
            .wait(|view| view.operations[0].interest_deadline.at == now + 300)
            .await;
        assert_eq!(live.operations.len(), 1);
        assert!(live.operations[0].interested);
        assert!(live.operations[0].terminal.is_none());
    }
    let before = runtime.service.subscribe().borrow().operations[0].clone();
    let mut duplicate = runtime
        .client
        .wait_execution(WaitExecutionRequest { name: name.clone() })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next_operation(&mut duplicate).await, queued);
    assert_eq!(runtime.service.subscribe().borrow().operations[0], before);
    clock.store(1100, Ordering::Relaxed);
    let (original, reattached) =
        tokio::join!(next_operation(&mut stream), next_operation(&mut duplicate));
    assert_eq!(original, queued);
    assert_eq!(reattached, queued);
    let renewed = runtime
        .wait(|view| view.operations[0].interest_deadline.at == 1400)
        .await;
    assert_eq!(renewed.operations.len(), 1);
    assert_eq!(
        renewed.operations[0].interest_deadline.generation,
        before.interest_deadline.generation + 1
    );
    let retained = renewed.operations[0].clone();
    drop(stream);
    drop(duplicate);
    runtime.shutdown().await;

    runtime = Runtime::start(&context, clock.clone(), faults, None).await;
    let mut stream = runtime
        .client
        .wait_execution(WaitExecutionRequest { name })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next_operation(&mut stream).await, queued);
    assert_eq!(runtime.service.subscribe().borrow().operations[0], retained);
    clock.store(1200, Ordering::Relaxed);
    assert_eq!(next_operation(&mut stream).await, queued);
    runtime
        .wait(|view| view.operations[0].interest_deadline.at == 1500)
        .await;
    drop(stream);
    // Disconnect itself leaves the persisted window intact. A jump past the
    // whole window must expire before any queued renewal can be considered.
    assert!(runtime.service.subscribe().borrow().operations[0].interested);
    clock.store(1501, Ordering::Relaxed);
    runtime
        .wait(|view| view.operations[0].terminal.is_some())
        .await;
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_INTEREST: identical queued-operation heartbeats preserve one interest for four windows; duplicate streams, reconnect and restart retain identity and persisted cadence"
    );
}

#[nativelink_test]
async fn paused_rpc_reader_exhausts_fixed_credit_and_interest_expires() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let runtime = Runtime::start(
        &context,
        clock.clone(),
        Arc::new(QualificationFaults::default()),
        None,
    )
    .await;
    let channel = Endpoint::from_shared(format!("http://{}", runtime.client_address))
        .unwrap()
        .initial_stream_window_size(512)
        .initial_connection_window_size(4096)
        .http2_adaptive_window(false)
        .connect()
        .await
        .unwrap();
    let mut client = ExecutionClient::new(channel);
    let (request, _, _) = context.request(30).await;
    let mut stream = client.execute(request).await.unwrap().into_inner();
    let first = next_operation(&mut stream).await;
    assert!(!first.done);
    let heartbeat_bytes = first.encoded_len() + 5;
    let mut terminal = None;
    for now in (100..=2000).step_by(100) {
        clock.store(now, Ordering::Relaxed);
        runtime.wait(|view| view.now >= now).await;
        // Let actual TCP/h2 delivery settle without polling the response body.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let view = runtime.service.subscribe().borrow().clone();
        assert_eq!(view.operations.len(), 1);
        if view.operations[0].terminal.is_some() {
            terminal = Some(view.operations[0].clone());
            break;
        }
    }
    let terminal = terminal.expect("Paused bounded-credit response kept renewing indefinitely");
    assert!(!terminal.interested);
    // Quiet heartbeats flush a single message at each cadence. Permit the
    // initial decoder batch, finite stream credit and one server body chunk.
    let allowance = 512 / heartbeat_bytes + 4;
    assert!(terminal.interest_deadline.generation <= u64::try_from(allowance).unwrap());
    eprintln!(
        "AUTHORITY_INTEREST: paused reader expired; peer_stream_credit=512 peer_connection_credit=4096 adaptive=false heartbeat_bytes={heartbeat_bytes} last_deadline={} generation={} renewal_allowance={allowance}",
        terminal.interest_deadline.at, terminal.interest_deadline.generation
    );
    drop(stream);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn stream_polling_cadence_shutdown_and_stale_expiry_are_fenced() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock.clone(), faults.clone(), None).await;
    let (request, _, _) = context.request(30).await;
    // Direct trait calls give exact control over body polling, independently of
    // the actual h2 finite-credit fixture above.
    let mut stream = Execution::execute(&runtime.service, Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let name = stream.next().await.unwrap().unwrap().name;
    runtime
        .wait(|view| view.executions[0].phase == Phase::Queued)
        .await;
    clock.store(250, Ordering::Relaxed);
    runtime.wait(|view| view.now == 250).await;
    let initial = runtime.service.subscribe().borrow().operations[0].clone();
    assert_eq!(initial.interest_deadline.at, 300);
    let mut attached = Execution::wait_execution(
        &runtime.service,
        Request::new(WaitExecutionRequest { name: name.clone() }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(attached.next().await.unwrap().unwrap().name, name);
    assert_eq!(runtime.service.subscribe().borrow().operations[0], initial);
    assert!(!attached.next().await.unwrap().unwrap().done);
    let renewed = runtime
        .wait(|view| view.operations[0].interest_deadline.at == 550)
        .await;
    let retained = renewed.operations[0].clone();
    assert_eq!(
        retained.interest_deadline.generation,
        initial.interest_deadline.generation + 1
    );
    drop(attached);
    runtime.shutdown().await;
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().code(),
        tonic::Code::Unavailable
    );
    drop(stream);

    // Inject a delayed timer from the old generation after a restart boundary.
    // It targets its original deadline, now due, while the renewed one is live.
    let authority = Singleton::open(
        context.directory.path().join("authority.redb"),
        1,
        EPOCH,
        Limits::default(),
    )
    .await
    .unwrap();
    authority
        .write(RecordedEvent {
            epoch: EPOCH,
            event: Event::AdvanceTime { now: 300 },
        })
        .await
        .unwrap();
    assert_eq!(
        authority
            .write(RecordedEvent {
                epoch: EPOCH,
                event: Event::ExpireInterest {
                    operation: initial.id,
                    generation: initial.interest_deadline.generation
                }
            })
            .await
            .unwrap()
            .data,
        Reply::Ignored
    );
    let restored = authority.read().await.unwrap().view();
    assert_eq!(restored.operations[0], retained);
    authority.shutdown().await.unwrap();
    clock.store(300, Ordering::Relaxed);
    runtime = Runtime::start(&context, clock.clone(), faults, None).await;
    let mut stream = Execution::wait_execution(
        &runtime.service,
        Request::new(WaitExecutionRequest { name }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(!stream.next().await.unwrap().unwrap().done);
    assert_eq!(runtime.service.subscribe().borrow().operations[0], retained);
    clock.store(600, Ordering::Relaxed);
    let terminal = tokio::time::timeout(Duration::from_secs(20), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(terminal.done);
    let expired = runtime.service.subscribe().borrow().operations[0].clone();
    assert!(!expired.interested);
    assert_eq!(expired.interest_deadline, retained.interest_deadline);
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_INTEREST: Observe and first poll do not renew; later polling renews; shutdown stops polling; persisted generation rejects delayed expiry; clock jump expires before renewal"
    );
}

#[nativelink_test]
async fn restart_replays_due_interest_after_committed_clock_before_expiry() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let runtime = Runtime::start(&context, clock.clone(), faults.clone(), None).await;
    let (request, _, _) = context.request(30).await;
    let mut stream = Execution::execute(&runtime.service, Request::new(request))
        .await
        .unwrap()
        .into_inner();
    let name = stream.next().await.unwrap().unwrap().name;
    runtime
        .wait(|view| view.executions[0].phase == Phase::Queued)
        .await;
    let initial = runtime.service.subscribe().borrow().operations[0].clone();
    drop(stream);
    runtime.shutdown().await;
    let authority = Singleton::open(
        context.directory.path().join("authority.redb"),
        1,
        EPOCH,
        Limits::default(),
    )
    .await
    .unwrap();
    // Durable crash cutpoint: the clock committed, but the driver's separate
    // expiry proposals did not. Recovery receives the exact same clock value.
    authority
        .write(RecordedEvent {
            epoch: EPOCH,
            event: Event::AdvanceTime { now: 300 },
        })
        .await
        .unwrap();
    assert_eq!(
        authority.read().await.unwrap().view().operations[0],
        initial
    );
    authority.shutdown().await.unwrap();
    clock.store(300, Ordering::Relaxed);
    let runtime = Runtime::start(&context, clock, faults, None).await;
    let expired = runtime
        .wait(|view| view.operations[0].terminal.is_some())
        .await;
    assert_eq!(expired.now, 300);
    assert!(!expired.operations[0].interested);
    assert_eq!(
        expired.operations[0].interest_deadline,
        initial.interest_deadline
    );
    let mut stream = Execution::wait_execution(
        &runtime.service,
        Request::new(WaitExecutionRequest { name }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(stream.next().await.unwrap().unwrap().done);
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_INTEREST: equal-clock restart replays due expiry after durable AdvanceTime cutpoint without renewing or postponing the retained deadline"
    );
}

#[path = "authority_history_rpc/cases.rs"]
mod history;

#[path = "authority_memory_rpc/cases.rs"]
mod memory;
