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

//! M4 permits accepted A to overwrite accepted B in a temporary mutable sink.
//! Real RPC and durable restart qualify acceptance and stable operation results.
//! The manual worker supplies synthetic candidates; it does not execute a process.
//! An owned gated sink task models an external write surviving request cancellation.
//! This fixture does not qualify provider durability, deployment trust, or M6 heads.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use nativelink_config::cas_server::{AcStoreConfig, WithInstanceName};
use nativelink_config::stores::MemorySpec;
use nativelink_error::{Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::ExecutionServer;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Digest, Directory, ExecuteRequest, ExecuteResponse,
    GetActionResultRequest, UpdateActionResultRequest, WaitExecutionRequest,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::AuthorityWorkerApiServer;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapacityObservation, AuthorityCommand,
    AuthorityCoordinatorUpdate, AuthorityReconciliation, AuthorityRegistration, AuthorityResources,
    AuthoritySession, AuthorityWorkerIdentity, AuthorityWorkerUpdate, authority_attempt_report,
    authority_command, authority_coordinator_update, authority_worker_update,
};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::{
    ACK_TIMEOUT, ClusterEpoch, Effect, EffectKind, Limits, ObservableState,
};
use nativelink_service::ac_server::AcServer;
use nativelink_service::authority_server::{
    AuthorityService, CoordinatorTask, QualificationConfig, QualificationFaults,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::background_spawn;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::default_health_status_indicator;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::health_utils::HealthStatusIndicator;
use nativelink_util::store_trait::{
    RemoveCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use prost::Message;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Streaming, async_trait};

const EPOCH: ClusterEpoch = ClusterEpoch(41);
const INSTANCE: &str = "publication-ordering";
const CAPACITY: AuthorityResources = AuthorityResources {
    cpu_millis: 1000,
    memory_bytes: 1024 * 1024 * 1024,
    slots: 1,
};
const WAIT: Duration = Duration::from_secs(20);

#[derive(Debug)]
struct PendingWrite {
    key: StoreKey<'static>,
    bytes: Bytes,
    release: oneshot::Sender<()>,
    completed: oneshot::Receiver<Result<(), String>>,
}

impl PendingWrite {
    fn assert_provenance(&self, accepted: &Effect, expected: &ActionResult, key: DigestInfo) {
        let EffectKind::PublishResult { action, result, .. } = &accepted.kind else {
            panic!("Expected a committed publication intent");
        };
        assert_eq!(accepted.epoch, EPOCH);
        assert_eq!(self.key, StoreKey::Digest(key));
        let digest: Digest = key.into();
        assert_eq!(action.digest, digest.hash);
        assert_eq!(action.digest_size_bytes, key.size_bytes());
        assert_eq!(
            result.require_response().unwrap().decode().unwrap().result,
            Some(expected.clone())
        );
        assert_eq!(self.bytes.as_ref(), expected.encode_to_vec());
    }

    async fn complete(self) {
        self.release.send(()).unwrap();
        tokio::time::timeout(WAIT, self.completed)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[derive(Debug, MetricsComponent)]
struct GatedSink {
    inner: Store,
    pending: mpsc::UnboundedSender<PendingWrite>,
}

default_health_status_indicator!(GatedSink);

#[async_trait]
impl StoreDriver for GatedSink {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner.has_with_results(keys, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _upload_size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        let bytes = reader.consume(None).await?;
        let size = u64::try_from(bytes.len()).unwrap();
        let key = key.into_owned();
        let inner = self.inner.clone();
        let (release, released) = oneshot::channel();
        let (completed, observed_completion) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        self.pending
            .send(PendingWrite {
                key: key.clone(),
                bytes: bytes.clone(),
                release,
                completed: observed_completion,
            })
            .map_err(|_| make_err!(tonic::Code::Unavailable, "Sink controller stopped"))?;
        // Dropping this handle deliberately detaches the owned request. The
        // coordinator can cancel its wait without cancelling external execution.
        let task = background_spawn!("ambiguous_external_publication", async move {
            if released.await.is_err() {
                return;
            }
            let outcome = inner.update_oneshot(key, bytes).await;
            let _observed = completed.send(outcome.as_ref().copied().map_err(ToString::to_string));
            let _reply = reply.send(outcome);
        });
        drop(task);
        response
            .await
            .map_err(|_| make_err!(tonic::Code::Unavailable, "External write stopped"))??;
        Ok(size)
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.inner.get_part(key, writer, offset, length).await
    }
    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }
    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }
    fn register_remove_callback(self: Arc<Self>, callback: RemoveCallback) -> Result<(), Error> {
        self.inner.register_remove_callback(callback)
    }
}

struct Context {
    directory: TempDir,
    cas: Store,
    contents: Store,
    sink: Store,
    pending: mpsc::UnboundedReceiver<PendingWrite>,
}

impl Context {
    fn new() -> Self {
        let contents = Store::new(MemoryStore::new(&MemorySpec::default()));
        let (pending, receiver) = mpsc::unbounded_channel();
        Self {
            directory: TempDir::new().unwrap(),
            cas: Store::new(MemoryStore::new(&MemorySpec::default())),
            sink: Store::new(Arc::new(GatedSink {
                inner: contents.clone(),
                pending,
            })),
            contents,
            pending: receiver,
        }
    }

    async fn request(&self) -> (ExecuteRequest, DigestInfo) {
        let command = serialize_and_upload_message(
            &Command {
                arguments: vec!["/bin/true".into()],
                ..Command::default()
            },
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let root = serialize_and_upload_message(
            &Directory::default(),
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let action = Action {
            command_digest: Some(command.into()),
            input_root_digest: Some(root.into()),
            timeout: Some(prost_types::Duration {
                seconds: 30,
                nanos: 0,
            }),
            ..Action::default()
        };
        let key = serialize_and_upload_message(
            &action,
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        (
            ExecuteRequest {
                instance_name: INSTANCE.into(),
                action_digest: Some(key.into()),
                digest_function: 1,
                skip_cache_lookup: true,
                ..ExecuteRequest::default()
            },
            key,
        )
    }

    async fn next_write(&mut self) -> PendingWrite {
        tokio::time::timeout(WAIT, self.pending.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn assert_contents(&self, key: DigestInfo, expected: &ActionResult) {
        let bytes = self
            .contents
            .get_part_unchunked(key, 0, None)
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), expected.encode_to_vec());
    }
}

struct Runtime {
    service: AuthorityService,
    task: CoordinatorTask,
    client: ExecutionClient<Channel>,
    channel: Channel,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Runtime {
    async fn start(
        context: &Context,
        clock: Arc<AtomicU64>,
        faults: Arc<QualificationFaults>,
    ) -> Self {
        let authority = Singleton::open(
            context.directory.path().join("authority.redb"),
            1,
            EPOCH,
            Limits::default(),
        )
        .await
        .unwrap();
        let (service, task) = AuthorityService::start(
            authority,
            QualificationConfig {
                instance: INSTANCE.into(),
                principal: "trusted-synthetic-worker-fixture".into(),
                cas: context.cas.clone(),
                cas_namespace: [0; 32],
                publication_sink: context.sink.clone(),
                now: Arc::new(move || Ok(clock.load(Ordering::Relaxed))),
                faults,
            },
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = stream::unfold(listener, |listener| async {
            Some((
                listener.accept().await.map(|(connection, _)| connection),
                listener,
            ))
        });
        let stores = StoreManager::new();
        stores
            .add_store("temporary-ac", context.sink.clone())
            .unwrap();
        let ac = AcServer::new(
            &[WithInstanceName {
                instance_name: INSTANCE.into(),
                config: AcStoreConfig {
                    ac_store: "temporary-ac".into(),
                    read_only: false,
                },
            }],
            &stores,
        )
        .unwrap();
        // This local fixture mounts all APIs together; it makes no deployment
        // trust or listener-exclusion claim.
        let server = background_spawn!(
            "publication_ordering_rpc",
            Server::builder()
                .add_service(ExecutionServer::new(service.clone()))
                .add_service(AuthorityWorkerApiServer::new(service.clone()))
                .add_service(ac.into_service())
                .serve_with_incoming(incoming)
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            service,
            task,
            client: ExecutionClient::new(channel.clone()),
            channel,
            server,
        }
    }

    async fn wait(
        &self,
        predicate: impl Fn(&ObservableState) -> bool + Send + Sync,
    ) -> Arc<ObservableState> {
        let mut state = self.service.subscribe();
        tokio::time::timeout(WAIT, async {
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

    async fn accepted(&self, expected: &ActionResult) -> Effect {
        let state = self
            .wait(|state| {
                state
                    .effects
                    .iter()
                    .any(|effect| publication_is(effect, expected))
            })
            .await;
        state
            .effects
            .iter()
            .find(|effect| publication_is(effect, expected))
            .unwrap()
            .clone()
    }

    async fn assert_terminal(&mut self, expected: &Operation) {
        let mut stream = self
            .client
            .wait_execution(WaitExecutionRequest {
                name: expected.name.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(terminal(&mut stream).await, *expected);
    }

    async fn shutdown(self) {
        self.server.abort();
        let _server = self.server.await;
        self.task.shutdown().await.unwrap();
    }
}

fn publication_is(effect: &Effect, expected: &ActionResult) -> bool {
    matches!(&effect.kind, EffectKind::PublishResult { result, .. } if result.require_response().unwrap().decode().unwrap().result.as_ref() == Some(expected))
}

struct Worker {
    session: AuthoritySession,
    sender: mpsc::Sender<AuthorityWorkerUpdate>,
    responses: Streaming<AuthorityCoordinatorUpdate>,
}

impl Worker {
    async fn connect(runtime: &Runtime, incarnation: u64) -> Self {
        let session = AuthoritySession {
            worker: Some(AuthorityWorkerIdentity {
                cluster_epoch: EPOCH.0,
                worker_id: 1,
                incarnation_id: incarnation,
            }),
            session_id: incarnation,
        };
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Registration(
                    AuthorityRegistration {
                        session: Some(session),
                        protocol_version: 2,
                        capabilities: vec![1, 2, 3, 4, 5, 6, 7],
                        capacity: Some(CAPACITY),
                        admission_ceiling: Some(CAPACITY),
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
        let updates = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|value| (value, receiver))
        });
        let mut client = AuthorityWorkerApiClient::new(runtime.channel.clone());
        let mut responses = client.connect(updates).await.unwrap().into_inner();
        let registered = tokio::time::timeout(WAIT, responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            registered.update,
            Some(authority_coordinator_update::Update::Registration(_))
        ));
        Self {
            session,
            sender,
            responses,
        }
    }

    async fn dispatch(&mut self) -> AuthorityCommand {
        tokio::time::timeout(WAIT, async {
            loop {
                let update = self.responses.message().await.unwrap().unwrap();
                if let Some(authority_coordinator_update::Update::Command(command)) = update.update
                    && matches!(
                        command.command,
                        Some(authority_command::Command::Dispatch(_))
                    )
                {
                    return command;
                }
            }
        })
        .await
        .unwrap()
    }

    async fn candidate(&self, dispatch: &AuthorityCommand, result: ActionResult) {
        self.sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(
                    AuthorityAttemptReport {
                        attempt: dispatch.attempt,
                        dispatch_effect_id: dispatch.effect_id,
                        observation_sequence: 1,
                        phase: AuthorityAttemptPhase::Quiescent as i32,
                        result: Some(authority_attempt_report::Result::ExecuteResponse(
                            ExecuteResponse {
                                result: Some(result),
                                ..ExecuteResponse::default()
                            },
                        )),
                        ..AuthorityAttemptReport::default()
                    },
                )),
            })
            .await
            .unwrap();
    }

    async fn capacity_barrier(
        &self,
        runtime: &Runtime,
        sequence: u64,
        draining: bool,
    ) -> Arc<ObservableState> {
        self.sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Capacity(
                    AuthorityCapacityObservation {
                        session: Some(self.session),
                        observation_sequence: sequence,
                        capacity: Some(CAPACITY),
                        admission_ceiling: Some(CAPACITY),
                        draining,
                    },
                )),
            })
            .await
            .unwrap();
        runtime
            .wait(|state| state.workers[0].draining == draining)
            .await
    }
}

async fn next_operation(stream: &mut Streaming<Operation>) -> Operation {
    tokio::time::timeout(WAIT, stream.message())
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

fn result(marker: &str) -> ActionResult {
    ActionResult {
        stdout_raw: Bytes::copy_from_slice(marker.as_bytes()),
        ..ActionResult::default()
    }
}

fn assert_result(terminal: &Operation, expected: &ActionResult) {
    assert!(terminal.done);
    let Some(operation::Result::Response(encoded)) = &terminal.result else {
        panic!("Expected a successful terminal ExecuteResponse");
    };
    let response = ExecuteResponse::decode(encoded.value.as_slice()).unwrap();
    assert_eq!(response.result.as_ref(), Some(expected));
    assert!(response.status.is_none_or(|status| status.code == 0));
    assert!(!response.cached_result);
}

#[nativelink_test]
async fn accepted_old_write_can_overwrite_new_after_restart_without_changing_terminals() {
    let mut context = Context::new();
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_publications.store(true, Ordering::Relaxed);
    let (request, key) = context.request().await;
    let mut runtime = Runtime::start(&context, clock.clone(), faults.clone()).await;
    let mut worker = Worker::connect(&runtime, 1).await;
    let a = result("accepted A");
    let b = result("accepted B");
    let mut operation_a = runtime
        .client
        .execute(request.clone())
        .await
        .unwrap()
        .into_inner();
    let dispatch_a = worker.dispatch().await;
    worker.candidate(&dispatch_a, a.clone()).await;
    let terminal_a = terminal(&mut operation_a).await;
    assert_result(&terminal_a, &a);
    let accepted_a = runtime.accepted(&a).await;
    faults.hold_publications.store(false, Ordering::Relaxed);
    let old_a = context.next_write().await;
    old_a.assert_provenance(&accepted_a, &a, key);
    assert_eq!(context.contents.has(key).await.unwrap(), None);
    drop(worker);
    runtime.shutdown().await;

    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock, faults.clone()).await;
    assert_eq!(
        runtime.accepted(&a).await,
        accepted_a,
        "Restart changed the accepted effect"
    );
    let retry_a = context.next_write().await;
    retry_a.assert_provenance(&accepted_a, &a, key);
    retry_a.complete().await;
    runtime
        .wait(|state| {
            !state
                .effects
                .iter()
                .any(|effect| effect.id == accepted_a.id)
        })
        .await;
    context.assert_contents(key, &a).await;
    runtime.assert_terminal(&terminal_a).await;

    // A remains authorized after replacement because acceptance already committed.
    let mut worker = Worker::connect(&runtime, 2).await;
    faults.hold_publications.store(true, Ordering::Relaxed);
    let mut operation_b = runtime.client.execute(request).await.unwrap().into_inner();
    let dispatch_b = worker.dispatch().await;
    assert_ne!(
        dispatch_a.attempt.unwrap().execution_id,
        dispatch_b.attempt.unwrap().execution_id
    );
    worker.candidate(&dispatch_b, b.clone()).await;
    let terminal_b = terminal(&mut operation_b).await;
    assert_result(&terminal_b, &b);
    assert_ne!(terminal_a.name, terminal_b.name);
    let accepted_b = runtime.accepted(&b).await;
    assert!(accepted_b.id > accepted_a.id);
    faults.hold_publications.store(false, Ordering::Relaxed);
    let write_b = context.next_write().await;
    write_b.assert_provenance(&accepted_b, &b, key);
    write_b.complete().await;
    runtime
        .wait(|state| {
            !state
                .effects
                .iter()
                .any(|effect| effect.id == accepted_b.id)
        })
        .await;
    context.assert_contents(key, &b).await;
    runtime.assert_terminal(&terminal_a).await;
    runtime.assert_terminal(&terminal_b).await;

    // The first A request still exists outside the restarted coordinator. Its
    // late success overwrites B; neither accepted operation changes its result.
    old_a.complete().await;
    context.assert_contents(key, &a).await;
    runtime.assert_terminal(&terminal_a).await;
    runtime.assert_terminal(&terminal_b).await;
    assert!(context.pending.try_recv().is_err());
    eprintln!(
        "M4_PUBLICATION: accepted effects A={} B={}; sink completion order retry-A,B,old-A; both terminal Operations unchanged",
        accepted_a.id.0, accepted_b.id.0
    );
    drop(worker);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn revoked_candidate_creates_no_accepted_publication_or_terminal_result() {
    let mut context = Context::new();
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let (request, key) = context.request().await;
    let mut runtime = Runtime::start(&context, clock.clone(), faults).await;
    let mut worker = Worker::connect(&runtime, 1).await;
    let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
    let initial = next_operation(&mut operation).await;
    let dispatch = worker.dispatch().await;
    worker.capacity_barrier(&runtime, 1, true).await;
    clock.store(ACK_TIMEOUT + 1, Ordering::Relaxed);
    let revoked = runtime
        .wait(|state| {
            state.attempts.iter().any(|attempt| {
                attempt.id.0 == dispatch.attempt.unwrap().attempt_id && !attempt.authorized
            })
        })
        .await;
    let terminal_before = revoked.operations[0].terminal.clone();
    assert!(terminal_before.is_none());
    assert!(!revoked.reservations[0].released);
    worker
        .candidate(&dispatch, result("stale candidate must never publish"))
        .await;
    // Capacity travels on the same incoming stream, so observing this changed
    // flag proves the earlier stale report was processed without a timed sleep.
    let after = worker.capacity_barrier(&runtime, 2, false).await;
    assert_eq!(after.operations[0].terminal, terminal_before);
    assert!(
        after.reservations[0].released,
        "Quiescence may settle the old charge"
    );
    assert!(
        !after
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    assert_eq!(context.contents.has(key).await.unwrap(), None);
    assert!(context.pending.try_recv().is_err());
    let mut observed = runtime
        .client
        .wait_execution(WaitExecutionRequest { name: initial.name })
        .await
        .unwrap()
        .into_inner();
    assert!(!next_operation(&mut observed).await.done);
    eprintln!(
        "M4_PUBLICATION: revocation-before-candidate leaves terminal absent and emits no sink write; quiescence only settles original charge"
    );
    drop(worker);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn direct_client_update_and_accepted_worker_write_share_mutable_ordering() {
    let mut context = Context::new();
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_publications.store(true, Ordering::Relaxed);
    let (request, key) = context.request().await;
    let mut runtime = Runtime::start(&context, clock, faults.clone()).await;
    let mut worker = Worker::connect(&runtime, 1).await;
    let accepted_result = result("accepted worker result");
    let direct_result = result("direct client result");
    let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
    let dispatch = worker.dispatch().await;
    worker.candidate(&dispatch, accepted_result.clone()).await;
    let original_terminal = terminal(&mut operation).await;
    assert_result(&original_terminal, &accepted_result);
    let accepted = runtime.accepted(&accepted_result).await;
    faults.hold_publications.store(false, Ordering::Relaxed);
    let worker_write = context.next_write().await;
    worker_write.assert_provenance(&accepted, &accepted_result, key);
    let before_direct = runtime.service.subscribe().borrow().clone();

    // Direct client provenance is the successful RPC itself. It must not create
    // an execution, attempt, reservation, or accepted worker publication intent.
    let mut direct = ActionCacheClient::new(runtime.channel.clone());
    let direct_request = UpdateActionResultRequest {
        instance_name: INSTANCE.into(),
        action_digest: Some(key.into()),
        action_result: Some(direct_result.clone()),
        digest_function: 1,
        ..UpdateActionResultRequest::default()
    };
    let update = background_spawn!("direct_client_ac_update", async move {
        direct
            .update_action_result(direct_request)
            .await
            .unwrap()
            .into_inner()
    });
    let direct_write = context.next_write().await;
    assert_eq!(direct_write.key, StoreKey::Digest(key));
    assert_eq!(direct_write.bytes.as_ref(), direct_result.encode_to_vec());
    direct_write.complete().await;
    assert_eq!(
        tokio::time::timeout(WAIT, update).await.unwrap().unwrap(),
        direct_result
    );
    assert_eq!(*runtime.service.subscribe().borrow(), before_direct);
    let mut ac = ActionCacheClient::new(runtime.channel.clone());
    let lookup = GetActionResultRequest {
        instance_name: INSTANCE.into(),
        action_digest: Some(key.into()),
        digest_function: 1,
        ..GetActionResultRequest::default()
    };
    assert_eq!(
        ac.get_action_result(lookup.clone())
            .await
            .unwrap()
            .into_inner(),
        direct_result
    );
    runtime.assert_terminal(&original_terminal).await;

    worker_write.complete().await;
    runtime
        .wait(|state| !state.effects.iter().any(|effect| effect.id == accepted.id))
        .await;
    assert_eq!(
        ac.get_action_result(lookup).await.unwrap().into_inner(),
        accepted_result
    );
    runtime.assert_terminal(&original_terminal).await;
    assert!(context.pending.try_recv().is_err());
    eprintln!(
        "M4_PUBLICATION: direct client RPC changes only sink; prior accepted worker effect={} later overwrites it while worker terminal stays fixed",
        accepted.id.0
    );
    drop(worker);
    runtime.shutdown().await;
}

#[path = "publication_history/cases.rs"]
mod history;
