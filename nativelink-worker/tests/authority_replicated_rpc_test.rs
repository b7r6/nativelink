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

//! The automatic RPC coordinator over three real replicated journals, using the
//! bounded serialized transport pattern from the scheduler cluster qualification.
//! Peer identity and worker endpoint routing are explicit local fixture facts.
#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::stream;
use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
use nativelink_config::stores::{FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::ExecutionServer;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{Action, Command, Directory, ExecuteRequest, ExecuteResponse, Platform, WaitExecutionRequest};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::AuthorityWorkerApiServer;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{AuthorityAttemptPhase, AuthorityResources, AuthorityWorkerIdentity, authority_attempt_report};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::journal::{Journal, Singleton, TypeConfig};
use nativelink_scheduler::authority::{ClusterEpoch, EffectKind, Limits, ObservableState, Phase};
use nativelink_service::authority_server::{AuthorityService, CoordinatorTask, QualificationConfig, QualificationFaults};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::background_spawn;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::authority_worker::AuthorityWorker;
use nativelink_worker::authority_worker_transport::run_reconnecting;
use nativelink_worker::namespace_utils;
use nativelink_worker::running_actions_manager::{ExecutionConfiguration, PersistentWorkersSettings, RunningActionsManagerArgs, RunningActionsManagerImpl, UseNamespaces};
use openraft::error::{CheckIsLeaderError, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse};
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory, SnapshotPolicy};
use parking_lot::Mutex;
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Streaming};

const EPOCH: ClusterEpoch = ClusterEpoch(97);
const WAIT: Duration = Duration::from_secs(20);
const IDENTITY: AuthorityWorkerIdentity = AuthorityWorkerIdentity {
    cluster_epoch: 97,
    worker_id: 7,
    incarnation_id: 11,
};
const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;

fn unreachable<E: std::error::Error>(message: &str) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&io::Error::new(
        io::ErrorKind::NotConnected,
        message,
    )))
}

#[derive(Default)]
struct Links {
    peers: BTreeMap<u64, Raft<TypeConfig>>,
    blocked: BTreeSet<(u64, u64)>,
    max_rpc_bytes: usize,
}

#[derive(Clone, Default)]
struct Fabric(Arc<Mutex<Links>>);

impl Fabric {
    fn peer(&self, from: u64, to: u64) -> Result<Raft<TypeConfig>, &'static str> {
        let links = self.0.lock();
        if !links.peers.contains_key(&from) || links.blocked.contains(&(from, to)) {
            return Err("stopped sender or scheduled partition");
        }
        links.peers.get(&to).cloned().ok_or("stopped peer")
    }

    fn encode<T: Serialize + DeserializeOwned>(&self, value: &T) -> T {
        let bytes = serde_json::to_vec(value).unwrap();
        assert!(bytes.len() <= MAX_RPC_BYTES);
        let mut links = self.0.lock();
        links.max_rpc_bytes = links.max_rpc_bytes.max(bytes.len());
        drop(links);
        serde_json::from_slice(&bytes).unwrap()
    }

    fn isolate(&self, id: u64) {
        let mut links = self.0.lock();
        for other in 1..=3 {
            if other != id {
                links.blocked.insert((id, other));
                links.blocked.insert((other, id));
            }
        }
    }
}

#[derive(Clone)]
struct Network {
    from: u64,
    to: u64,
    fabric: Fabric,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Self;
    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self {
        Self {
            to: target,
            ..self.clone()
        }
    }
}

impl RaftNetwork<TypeConfig> for Network {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        assert!(rpc.entries.len() <= 4);
        let peer = self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = peer.append_entries(self.fabric.encode(&rpc)).await;
        self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = response.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        Ok(self.fabric.encode(&response))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        assert!(rpc.data.len() <= 512);
        let peer = self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = peer.install_snapshot(self.fabric.encode(&rpc)).await;
        self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = response.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        Ok(self.fabric.encode(&response))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let peer = self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = peer.vote(self.fabric.encode(&rpc)).await;
        self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = response.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        Ok(self.fabric.encode(&response))
    }
}

struct Cluster {
    _directory: TempDir,
    fabric: Fabric,
    nodes: BTreeMap<u64, (Raft<TypeConfig>, Journal)>,
}

impl Cluster {
    async fn start() -> Self {
        let directory = TempDir::new().unwrap();
        let fabric = Fabric::default();
        let mut nodes = BTreeMap::new();
        for id in 1..=3 {
            let journal = Journal::open(
                directory.path().join(format!("node-{id}.redb")),
                id,
                EPOCH,
                Limits::default(),
            )
            .await
            .unwrap();
            let config = openraft::Config {
                cluster_name: "automatic-replicated-rpc-fixture".to_owned(),
                heartbeat_interval: 100,
                election_timeout_min: 400,
                election_timeout_max: 800,
                enable_elect: false,
                max_payload_entries: 4,
                snapshot_max_chunk_size: 512,
                snapshot_policy: SnapshotPolicy::Never,
                ..Default::default()
            }
            .validate()
            .unwrap();
            let raft = Raft::new(
                id,
                Arc::new(config),
                Network {
                    from: id,
                    to: id,
                    fabric: fabric.clone(),
                },
                journal.clone(),
                journal.clone(),
            )
            .await
            .unwrap();
            fabric.0.lock().peers.insert(id, raft.clone());
            nodes.insert(id, (raft, journal));
        }
        let result = Self {
            _directory: directory,
            fabric,
            nodes,
        };
        result.nodes[&1]
            .0
            .initialize(BTreeSet::from([1]))
            .await
            .unwrap();
        result.leader(1).await;
        for id in 2..=3 {
            tokio::time::timeout(
                WAIT,
                result.nodes[&1]
                    .0
                    .add_learner(id, BasicNode::default(), true),
            )
            .await
            .unwrap()
            .unwrap();
        }
        tokio::time::timeout(
            WAIT,
            result.nodes[&1]
                .0
                .change_membership(BTreeSet::from([1, 2, 3]), false),
        )
        .await
        .unwrap()
        .unwrap();
        result.barrier(1).await;
        result
    }

    async fn barrier(&self, id: u64) {
        tokio::time::timeout(WAIT, async {
            loop {
                match self.nodes[&id].0.ensure_linearizable().await {
                    Ok(_) => return,
                    Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("Healthy quorum barrier failed: {error}"),
                }
            }
        })
        .await
        .unwrap();
    }

    async fn leader(&self, id: u64) {
        self.nodes[&id]
            .0
            .wait(Some(WAIT))
            .current_leader(id, "fixture leader elected")
            .await
            .unwrap();
        self.barrier(id).await;
    }

    async fn elect(&self, id: u64) {
        self.nodes[&id].0.runtime_config().elect(true);
        self.nodes[&id].0.trigger().elect().await.unwrap();
        self.leader(id).await;
        self.nodes[&id].0.runtime_config().elect(false);
    }

    fn handle(&self, id: u64) -> Singleton {
        Singleton {
            raft: self.nodes[&id].0.clone(),
            journal: self.nodes[&id].1.clone(),
        }
    }

    async fn shutdown(self) {
        for (_, (raft, _)) in self.nodes {
            raft.shutdown().await.unwrap();
        }
        self.fabric.0.lock().peers.clear();
        assert!(self.fabric.0.lock().max_rpc_bytes <= MAX_RPC_BYTES);
    }
}

struct Context {
    directory: TempDir,
    cas: Arc<FastSlowStore>,
    ac: Arc<MemoryStore>,
    worker: Arc<AuthorityWorker<RunningActionsManagerImpl>>,
}

impl Context {
    async fn start() -> Self {
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
            Store::new(MemoryStore::new(&MemorySpec::default())),
        );
        let ac = MemoryStore::new(&MemorySpec::default());
        let actions = directory.path().join("actions");
        std::fs::create_dir(&actions).unwrap();
        let manager = Arc::new(
            RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: actions.to_string_lossy().into_owned(),
                execution_configuration: ExecutionConfiguration {
                    persistent_workers: PersistentWorkersSettings {
                        enabled: false,
                        ..PersistentWorkersSettings::default()
                    },
                    ..ExecutionConfiguration::default()
                },
                cas_store: cas.clone(),
                ac_store: Some(Store::new(ac.clone())),
                historical_store: Store::new(MemoryStore::new(&MemorySpec::default())),
                upload_action_result_config: &UploadActionResultConfig {
                    upload_ac_results_strategy: UploadCacheResultsStrategy::Everything,
                    ..UploadActionResultConfig::default()
                },
                max_action_timeout: Duration::from_mins(2),
                max_upload_timeout: Duration::from_secs(10),
                max_download_timeout: Duration::from_secs(10),
                max_cleanup_wait: Duration::from_secs(10),
                max_cleanup_backoff: Duration::from_millis(10),
                timeout_handled_externally: false,
                directory_cache: None,
                active_input_leases: true,
                readonly_input_mounts: Vec::new(),
                use_namespaces: UseNamespaces::Yes,
            })
            .unwrap(),
        );
        let worker = AuthorityWorker::new(
            IDENTITY,
            false,
            AuthorityResources {
                cpu_millis: 2000,
                memory_bytes: 1024 * 1024 * 1024,
                slots: 2,
            },
            4,
            64 * 1024,
            manager,
        )
        .unwrap();
        Self {
            directory,
            cas,
            ac,
            worker,
        }
    }

    async fn request(&self, name: &str, held: bool) -> (ExecuteRequest, DigestInfo) {
        let marker = self.directory.path().join(format!("executed-{name}"));
        let release = self.directory.path().join(format!("release-{name}"));
        let release_wait = if held {
            format!(
                "while [ ! -f '{}' ]; do /bin/sleep 0.02; done; ",
                release.display()
            )
        } else {
            String::new()
        };
        let command = Command {
            arguments: vec![
                "/bin/sh".to_owned(),
                "-c".to_owned(),
                format!(
                    "printf x >> '{}'; {release_wait}printf {name}",
                    marker.display()
                ),
            ],
            ..Command::default()
        };
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        let command_digest = serialize_and_upload_message(&command, self.cas.as_pin(), &mut hasher)
            .await
            .unwrap();
        let input_root_digest = serialize_and_upload_message(
            &Directory::default(),
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(input_root_digest.into()),
            timeout: Some(prost_types::Duration {
                seconds: 120,
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
        let digest = serialize_and_upload_message(
            &action,
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        (
            ExecuteRequest {
                instance_name: "replicated-rpc".to_owned(),
                action_digest: Some(digest.into()),
                digest_function: 1,
                skip_cache_lookup: true,
                ..ExecuteRequest::default()
            },
            digest,
        )
    }
}

struct Runtime {
    service: AuthorityService,
    coordinator: CoordinatorTask,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    address: SocketAddr,
    client: ExecutionClient<Channel>,
}

impl Runtime {
    async fn start(
        cluster: &Cluster,
        id: u64,
        context: &Context,
        faults: Arc<QualificationFaults>,
    ) -> Self {
        cluster.barrier(id).await;
        let (service, coordinator) = AuthorityService::start(
            cluster.handle(id),
            QualificationConfig {
                instance: "replicated-rpc".to_owned(),
                principal: "trusted-local-replicated-fixture".to_owned(),
                cas: Store::new(context.cas.clone()),
                publication_sink: Store::new(context.ac.clone()),
                now: Arc::new(|| 0),
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
        let server = background_spawn!(
            "replicated_rpc_listener",
            Server::builder()
                .add_service(
                    ExecutionServer::new(service.clone()).max_encoding_message_size(128 * 1024)
                )
                .add_service(AuthorityWorkerApiServer::new(service.clone()))
                .serve_with_incoming(incoming)
        );
        let client = ExecutionClient::new(
            Endpoint::from_shared(format!("http://{address}"))
                .unwrap()
                .connect()
                .await
                .unwrap(),
        );
        Self {
            service,
            coordinator,
            server,
            address,
            client,
        }
    }

    async fn wait(
        &self,
        predicate: impl Fn(&ObservableState) -> bool + Send + Sync,
    ) -> Arc<ObservableState> {
        let mut changed = self.service.subscribe();
        tokio::time::timeout(WAIT, async {
            loop {
                let view = changed.borrow_and_update().clone();
                if predicate(&view) {
                    return view;
                }
                changed
                    .changed()
                    .await
                    .expect("coordinator observation ended");
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "Coordinator wait failed: {error}; last={:?}",
                changed.borrow()
            )
        })
    }

    async fn shutdown(self) -> Result<(), String> {
        self.server.abort();
        let _server = self.server.await;
        self.coordinator.shutdown().await
    }
}

/// Explicit fixture routing. Replacing the backend closes existing TCP streams;
/// the real recipient supervisor reconnects without replacing its process state.
struct WorkerRoute {
    address: SocketAddr,
    route: watch::Sender<Option<SocketAddr>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl WorkerRoute {
    async fn start(address: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        let (route, routed) = watch::channel(Some(address));
        let (stop, mut stopped) = watch::channel(false);
        let task = background_spawn!("replicated_worker_fixture_route", async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    _ = stopped.changed() => break,
                    done = connections.join_next(), if !connections.is_empty() => { done.unwrap().unwrap(); }
                    incoming = listener.accept() => {
                        let (mut worker, _) = incoming.unwrap();
                        let mut changed = routed.clone();
                        let target = *changed.borrow_and_update();
                        if let Some(target) = target {
                            connections.spawn(async move {
                                let Ok(mut backend) = TcpStream::connect(target).await else { return; };
                                tokio::select! {
                                    _ = tokio::io::copy_bidirectional(&mut worker, &mut backend) => {}
                                    _ = changed.changed() => {}
                                }
                            });
                        }
                    }
                }
            }
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        });
        Self {
            address: local,
            route,
            stop,
            task,
        }
    }

    async fn shutdown(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap();
    }
}

async fn terminal(stream: &mut Streaming<Operation>) -> Operation {
    loop {
        let operation = tokio::time::timeout(WAIT, stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if operation.done {
            return operation;
        }
    }
}

fn response(operation: &Operation) -> ExecuteResponse {
    let Some(operation::Result::Response(value)) = &operation.result else {
        panic!("Expected response");
    };
    ExecuteResponse::decode(value.value.as_slice()).unwrap()
}

#[nativelink_test]
async fn replicated_rpc_failover_preserves_publication_and_live_worker_candidate() {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!("SKIP replicated RPC physical qualification: PID namespaces unavailable");
        return;
    }
    let context = Context::start().await;
    let cluster = Cluster::start().await;
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_publications.store(true, Ordering::Relaxed);
    let mut first = Runtime::start(&cluster, 1, &context, faults.clone()).await;
    let route = WorkerRoute::start(first.address).await;
    let (stop_worker, stopped) = watch::channel(false);
    let worker = context.worker.clone();
    let endpoint = Endpoint::from_shared(format!("http://{}", route.address))
        .unwrap()
        .connect_timeout(Duration::from_secs(1));
    let supervisor = background_spawn!(
        "replicated_worker_supervisor",
        run_reconnecting(worker, endpoint, 1, vec![], stopped)
    );
    first.wait(|view| !view.workers.is_empty()).await;

    let (request_a, digest_a) = context.request("a", false).await;
    let mut stream_a = first
        .client
        .execute(Request::new(request_a))
        .await
        .unwrap()
        .into_inner();
    let terminal_a = terminal(&mut stream_a).await;
    assert_eq!(response(&terminal_a).result.as_ref().unwrap().exit_code, 0);
    let accepted = first
        .wait(|view| {
            view.effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        })
        .await;
    let publication = accepted
        .effects
        .iter()
        .find(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        .unwrap()
        .clone();
    assert!(context.ac.has(digest_a).await.unwrap().is_none());

    let (request_b, digest_b) = context.request("b", true).await;
    let mut stream_b = first
        .client
        .execute(Request::new(request_b))
        .await
        .unwrap()
        .into_inner();
    let handle_b = tokio::time::timeout(WAIT, stream_b.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .name;
    let running = first
        .wait(|view| view.executions.len() == 2 && view.executions[1].phase == Phase::Running)
        .await;
    let attempt_b = running
        .attempts
        .iter()
        .find(|attempt| attempt.execution == running.executions[1].id)
        .copied()
        .unwrap();
    let session = running.workers[0].session;
    let committed = cluster.nodes[&1].1.read_applied().await.unwrap().0.unwrap();
    for id in [2, 3] {
        cluster.nodes[&id]
            .0
            .wait(Some(WAIT))
            .applied_index_at_least(Some(committed.index), "both survivors have running grant")
            .await
            .unwrap();
    }

    route.route.send_replace(None);
    cluster.fabric.isolate(1);
    assert!(cluster.nodes[&1].0.ensure_linearizable().await.is_err());
    let (request_x, _) = context.request("x", false).await;
    let action_x = request_x.action_digest.as_ref().unwrap().hash.clone();
    let mut minority_client = first.client.clone();
    let mut minority = Some(background_spawn!("minority_rpc_submission", async move {
        minority_client.execute(Request::new(request_x)).await
    }));
    cluster.elect(2).await;
    let minority_view = cluster.nodes[&1].1.read_applied().await.unwrap().1.view();
    assert!(
        minority_view
            .executions
            .iter()
            .all(|execution| execution.request.action.digest != action_x)
    );
    assert_eq!(minority_view.operations.len(), 2);
    assert!(
        minority_view
            .effects
            .iter()
            .any(|effect| effect == &publication)
    );
    assert!(!context.directory.path().join("executed-x").exists());
    // Cancellation of a client wait would not erase an old proposal. Keep the
    // actual RPC alive and record its disposition after the higher term wins.
    let early_rejection = if minority.as_ref().unwrap().is_finished() {
        Some(
            minority
                .take()
                .unwrap()
                .await
                .unwrap()
                .expect_err("minority produced a committed handle"),
        )
    } else {
        None
    };

    std::fs::write(context.directory.path().join("release-b"), b"go").unwrap();
    let mut observations = context.worker.subscribe();
    let candidate_b = tokio::time::timeout(WAIT, async {
        loop {
            if let Some(report) = context
                .worker
                .reconcile()
                .attempts
                .into_iter()
                .find(|report| {
                    report
                        .attempt
                        .is_some_and(|id| id.attempt_id == attempt_b.id.0)
                        && report.phase == AuthorityAttemptPhase::Quiescent as i32
                        && report.result.is_some()
                })
            {
                break report;
            }
            observations.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(candidate_b.dispatch_effect_id, attempt_b.dispatch_effect.0);
    let Some(authority_attempt_report::Result::ExecuteResponse(candidate_response)) =
        candidate_b.result
    else {
        panic!("Expected successful full candidate during coordinator outage");
    };
    assert_eq!(
        std::fs::read(context.directory.path().join("executed-b")).unwrap(),
        b"x"
    );

    let mut successor = Runtime::start(&cluster, 2, &context, faults.clone()).await;
    let restored = successor.service.subscribe().borrow().clone();
    assert_eq!(
        restored.operations[0].terminal,
        running.operations[0].terminal
    );
    assert!(restored.effects.iter().any(|effect| effect == &publication));
    assert!(
        restored
            .attempts
            .iter()
            .any(|attempt| attempt.id == attempt_b.id
                && attempt.dispatch_effect == attempt_b.dispatch_effect
                && attempt.authorized)
    );
    route.route.send_replace(Some(successor.address));
    let terminal_view = successor
        .wait(|view| {
            view.operations.len() == 2
                && view.operations[1].terminal.is_some()
                && view.workers[0].session > session
        })
        .await;
    assert_eq!(terminal_view.executions[1].attempts_started, 1);
    assert_eq!(
        terminal_view.workers[0].incarnation.0,
        IDENTITY.incarnation_id
    );
    assert!(
        terminal_view
            .effects
            .iter()
            .any(|effect| effect == &publication)
    );
    let mut recovered_a = successor
        .client
        .wait_execution(Request::new(WaitExecutionRequest {
            name: terminal_a.name.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(terminal(&mut recovered_a).await, terminal_a);
    let mut recovered_b = successor
        .client
        .wait_execution(Request::new(WaitExecutionRequest { name: handle_b }))
        .await
        .unwrap()
        .into_inner();
    let terminal_b = terminal(&mut recovered_b).await;
    assert_eq!(response(&terminal_b), candidate_response);
    assert_eq!(response(&terminal_b).result.as_ref().unwrap().exit_code, 0);

    let successor_applied = cluster.nodes[&2].1.read_applied().await.unwrap().0.unwrap();
    assert!(successor_applied.leader_id.term > committed.leader_id.term);
    assert!(successor_applied.index > committed.index);
    let settled_after_heal = early_rejection.is_none();
    let minority_error = if let Some(error) = early_rejection {
        // A coordinator whose quorum read already failed may close early. Do
        // not restore transport to that stopped member or reuse its identity.
        cluster.fabric.0.lock().peers.remove(&1);
        error
    } else {
        // Heal the still-running old member, allowing the real higher-term log
        // to settle the unresolved proposal before its driver shuts the member.
        cluster.fabric.0.lock().blocked.clear();
        tokio::time::timeout(WAIT, minority.unwrap())
            .await
            .unwrap()
            .unwrap()
            .expect_err("old minority proposal unexpectedly produced a committed handle")
    };
    let stopped_first = tokio::time::timeout(WAIT, first.shutdown()).await.unwrap();
    assert!(
        stopped_first.is_err(),
        "lost leadership must stop the old automatic driver"
    );
    cluster.fabric.0.lock().peers.remove(&1);
    eprintln!(
        "AUTHORITY_REPLICATED_RPC_FENCE: old_term={} successor_term={} old_committed_index={} successor_committed_index={} old_session={} successor_session={} publication_effect={} candidate_attempt={} settled_after_heal={settled_after_heal} minority_status={}",
        committed.leader_id.term,
        successor_applied.leader_id.term,
        committed.index,
        successor_applied.index,
        session.0,
        terminal_view.workers[0].session.0,
        publication.id.0,
        attempt_b.id.0,
        minority_error.code()
    );

    faults.hold_publications.store(false, Ordering::Relaxed);
    let published = successor
        .wait(|view| {
            !view
                .effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        })
        .await;
    assert_eq!(
        published.operations[0].terminal,
        running.operations[0].terminal
    );
    assert!(
        published
            .executions
            .iter()
            .all(|execution| execution.request.action.digest != action_x)
    );
    assert!(context.ac.has(digest_a).await.unwrap().is_some());
    assert!(context.ac.has(digest_b).await.unwrap().is_some());
    assert_eq!(
        std::fs::read(context.directory.path().join("executed-a")).unwrap(),
        b"x"
    );
    assert_eq!(
        std::fs::read(context.directory.path().join("executed-b")).unwrap(),
        b"x"
    );
    assert!(!context.directory.path().join("executed-x").exists());
    stop_worker.send_replace(true);
    supervisor.await.unwrap().unwrap();
    route.shutdown().await;
    successor.shutdown().await.unwrap();
    cluster.shutdown().await;
    eprintln!(
        "AUTHORITY_REPLICATED_RPC: three durable journals; minority RPC produced no committed handle; higher-term successor retained exact accepted terminal and pending publication identity; same live worker reconnected and delivered full outage candidate once; original proposal disposition recorded without client cancellation; both publications drained"
    );
}
