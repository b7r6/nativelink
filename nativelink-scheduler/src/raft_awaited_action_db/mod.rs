// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! A raft-replicated `AwaitedActionDb`. See `DESIGN.md` in this directory.

pub mod state;
pub mod store;
pub mod types;
mod wal;

pub use types::{Command, CommandResponse, NodeId, RejectReason, TypeConfig};

use core::future::Future;
use core::ops::Bound;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{Stream, stream};
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::action_messages::{ActionInfo, OperationId, WorkerId};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Notify;
use tracing::{debug, warn};

use crate::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, CLIENT_KEEPALIVE_DURATION,
    SortedAwaitedAction, SortedAwaitedActionState, capture_origin_metadata,
};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft, SnapshotPolicy};
use tokio::sync::watch;

use self::state::AppliedState;
use self::store::{LogStore, OnApply, StateMachineStore};

/// A network that goes nowhere: a single voter never dials a peer. Every method
/// is unreachable for a one-node cluster, so they error rather than lie.
#[derive(Debug, Clone, Copy)]
pub struct SingleNodeNetwork;

impl RaftNetworkFactory<TypeConfig> for SingleNodeNetwork {
    type Network = SingleNodeConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        SingleNodeConnection { target }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SingleNodeConnection {
    target: NodeId,
}

impl RaftNetwork<TypeConfig> for SingleNodeConnection {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // A single voter never dials a peer; if it somehow tries, surface it as
        // an unreachable node rather than lie with a success response.
        Err(unreachable_peer(self.target))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        Err(unreachable_peer(self.target))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(unreachable_peer(self.target))
    }
}

/// A single voter has no peers; any RPC attempt is an unreachable network node.
fn unreachable_peer<E: std::error::Error>(target: NodeId) -> RPCError<NodeId, BasicNode, E> {
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "single-node cluster has no peer {target}"
    ))))
}


fn unix_nanos(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
}

/// How a node of the replicated scheduler state is set up.
#[derive(Debug, Clone)]
pub struct RaftDbConfig {
    /// This voter's id.
    pub node_id: NodeId,
    /// Where the log and snapshots live. `None` keeps everything in memory:
    /// a restart loses all state, as with the memory backend.
    pub data_dir: Option<PathBuf>,
    /// How long a client operation id lives after its last use, as the
    /// memory backend's `retain_completed_for_s` eviction.
    pub retain_client: Duration,
    /// Requeues past this many attempts are refused in apply.
    pub retry_limit: usize,
    /// Build a snapshot (and allow the log before it to be purged) after this
    /// many applied entries.
    pub snapshot_every: u64,
}

impl Default for RaftDbConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            data_dir: None,
            retain_client: Duration::from_secs(60),
            retry_limit: state::DEFAULT_RETRY_LIMIT,
            snapshot_every: 50_000,
        }
    }
}

/// Facts about clients that only this replica can observe: when each client
/// operation id was last used, and the latest keepalive per operation. They
/// never enter the log; the one decision derived from them, dropping stale
/// clients, does (`Command::DropClients`).
#[derive(Debug, Default)]
struct ClientTracker {
    /// client operation id -> last time this replica saw it used.
    last_touch: HashMap<OperationId, SystemTime>,
    /// The same, ordered by time, so expiry costs O(expired).
    by_time: BTreeSet<(SystemTime, OperationId)>,
    /// operation id -> newest client keepalive seen here.
    keepalive: HashMap<OperationId, SystemTime>,
}

impl ClientTracker {
    fn touch(&mut self, client_operation_id: &OperationId, now: SystemTime) {
        if let Some(previous) = self.last_touch.insert(client_operation_id.clone(), now) {
            self.by_time.remove(&(previous, client_operation_id.clone()));
        }
        self.by_time.insert((now, client_operation_id.clone()));
    }

    fn keep_alive(&mut self, client_operation_id: &OperationId, operation_id: &OperationId, now: SystemTime) {
        self.touch(client_operation_id, now);
        let entry = self.keepalive.entry(operation_id.clone()).or_insert(now);
        *entry = (*entry).max(now);
    }

    /// Whether `client_operation_id` was last used longer than `retain` ago.
    fn is_stale(&self, client_operation_id: &OperationId, now: SystemTime, retain: Duration) -> bool {
        self.last_touch
            .get(client_operation_id)
            .is_some_and(|touched| now > *touched + retain)
    }

    /// Every client last used longer than `retain` ago.
    fn stale(&self, now: SystemTime, retain: Duration) -> Vec<OperationId> {
        self.by_time
            .iter()
            .take_while(|(touched, _)| now > *touched + retain)
            .map(|(_, client)| client.clone())
            .collect()
    }

    /// Apply the locally known keepalive to a record read from the log.
    fn overlay(&self, action: &mut AwaitedAction) {
        if let Some(ts) = self.keepalive.get(action.operation_id()) {
            if *ts > action.last_client_keepalive_timestamp() {
                action.update_client_keep_alive(*ts);
            }
        }
    }

    fn forget(&mut self, client_operation_ids: &[OperationId], removed_ops: &[OperationId]) {
        for client in client_operation_ids {
            if let Some(touched) = self.last_touch.remove(client) {
                self.by_time.remove(&(touched, client.clone()));
            }
        }
        for op in removed_ops {
            self.keepalive.remove(op);
        }
    }
}

/// The per-client part of a subscriber, mirroring the memory backend: while a
/// client waits on `changed()`, it reports a keepalive every
/// `CLIENT_KEEPALIVE_DURATION`.
#[derive(Debug)]
struct ClientInfo<I: InstantWrapper, NowFn: Fn() -> I> {
    client_operation_id: OperationId,
    last_keep_alive: I,
    now_fn: NowFn,
}

/// Subscriber backed by a `watch` channel, like the memory backend.
#[derive(Debug)]
pub struct RaftAwaitedActionSubscriber<I: InstantWrapper, NowFn: Fn() -> I> {
    rx: watch::Receiver<AwaitedAction>,
    client: Option<ClientInfo<I, NowFn>>,
    clients: Arc<SyncMutex<ClientTracker>>,
}

impl<I: InstantWrapper, NowFn: Fn() -> I> RaftAwaitedActionSubscriber<I, NowFn> {
    fn current(&self) -> AwaitedAction {
        let mut action = self.rx.borrow().clone();
        self.clients.lock().overlay(&mut action);
        if let Some(client) = &self.client {
            action.set_client_operation_id(client.client_operation_id.clone());
        }
        action
    }
}

impl<I, NowFn> AwaitedActionSubscriber for RaftAwaitedActionSubscriber<I, NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Send + Sync + 'static,
{
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        {
            let operation_id = self.rx.borrow().operation_id().clone();
            let changed_fut = self.rx.changed();
            tokio::pin!(changed_fut);
            match self.client.as_mut() {
                None => changed_fut
                    .await
                    .map_err(|e| Error::from_std_err(Code::Internal, &e))
                    .err_tip(|| "Failed to wait for awaited action to change")?,
                Some(client) => loop {
                    if client.last_keep_alive.elapsed() > CLIENT_KEEPALIVE_DURATION {
                        client.last_keep_alive = (client.now_fn)();
                        let now = client.last_keep_alive.now();
                        self.clients
                            .lock()
                            .keep_alive(&client.client_operation_id, &operation_id, now);
                    }
                    let sleep_fut = (client.now_fn)().sleep(CLIENT_KEEPALIVE_DURATION);
                    tokio::select! {
                        result = &mut changed_fut => {
                            result
                                .map_err(|e| Error::from_std_err(Code::Internal, &e))
                                .err_tip(|| "Failed to wait for awaited action to change")?;
                            break;
                        }
                        () = sleep_fut => {}
                    }
                },
            }
        }
        Ok(self.current())
    }

    fn borrow(&self) -> impl Future<Output = Result<AwaitedAction, Error>> + Send {
        core::future::ready(Ok(self.current()))
    }
}

/// The watch channels the db layer keeps to notify subscribers, mirroring
/// `operation_id_to_awaited_action` in the memory backend. Fed from the state
/// machine's applied output — never a source of truth, just a notification bus.
///
/// The published value carries its own revision (`AwaitedAction::version_public`).
/// Publication is guarded so it never regresses: a stale value captured before a
/// newer one committed cannot overwrite the channel (Astra premortem S2/S3).
#[derive(Debug, Default)]
struct Watchers {
    txs: BTreeMap<OperationId, watch::Sender<AwaitedAction>>,
}

impl Watchers {
    /// Publish `action` for `op_id`, but only if its revision is newer than
    /// what the channel already holds. Creating a fresh channel always seeds it.
    /// Returns true if the channel value advanced.
    fn publish(&mut self, op_id: &OperationId, action: &AwaitedAction) -> bool {
        match self.txs.get(op_id) {
            Some(tx) => {
                let current_rev = tx.borrow().version_public();
                if action.version_public() <= current_rev {
                    // Older than what was delivered: drop it, so a stale value
                    // never overwrites a newer one. The same revision: nothing
                    // changed (every accepted mutation bumps it), so do not wake
                    // subscribers for it. Revision order, not stage order — an
                    // Executing->Queued retry has a HIGHER revision and is kept.
                    return false;
                }
                tx.send_replace(action.clone());
                true
            }
            None => {
                let (tx, _rx) = watch::channel(action.clone());
                self.txs.insert(op_id.clone(), tx);
                true
            }
        }
    }

    /// Drop the channel of an operation that left the state: its subscribers
    /// see the channel close, as with the memory backend.
    fn remove(&mut self, op_id: &OperationId) {
        self.txs.remove(op_id);
    }

    /// Drop channels nobody listens to for finished operations, so the bus
    /// does not grow with every operation ever seen.
    fn prune_unwatched_finished(&mut self) {
        self.txs
            .retain(|_, tx| tx.receiver_count() > 0 || !tx.borrow().state().stage.is_finished());
    }
}

/// The parts of the db its subscribers and apply hook share.
struct Shared {
    sm: StateMachineStore,
    watchers: Arc<SyncMutex<Watchers>>,
    clients: Arc<SyncMutex<ClientTracker>>,
}

/// The raft-backed database.
pub struct RaftAwaitedActionDb<I: InstantWrapper, NowFn: Fn() -> I> {
    raft_rx: watch::Receiver<Option<Result<Raft<TypeConfig>, String>>>,
    shared: Arc<Shared>,
    config: RaftDbConfig,
    now_fn: NowFn,
    _init: JoinHandleDropGuard<()>,
}

impl<I: InstantWrapper, NowFn: Fn() -> I> MetricsComponent for RaftAwaitedActionDb<I, NowFn> {
    fn publish(
        &self,
        _kind: nativelink_metric::MetricKind,
        _field_metadata: nativelink_metric::MetricFieldData,
    ) -> Result<nativelink_metric::MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(nativelink_metric::MetricPublishKnownKindData::Component)
    }
}

impl<I: InstantWrapper, NowFn: Fn() -> I> core::fmt::Debug for RaftAwaitedActionDb<I, NowFn> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RaftAwaitedActionDb")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The db on the system clock, as production and the spike tests use it.
pub type SystemRaftAwaitedActionDb = RaftAwaitedActionDb<SystemTime, fn() -> SystemTime>;

/// The subscriber of [`SystemRaftAwaitedActionDb`].
pub type SystemRaftAwaitedActionSubscriber =
    RaftAwaitedActionSubscriber<SystemTime, fn() -> SystemTime>;

impl SystemRaftAwaitedActionDb {
    /// Boot an in-memory single-voter cluster on the system clock.
    pub async fn new_single_node() -> Result<Self, Error> {
        let now_fn: fn() -> SystemTime = SystemTime::now;
        Self::open(RaftDbConfig::default(), Arc::new(Notify::new()), now_fn).await
    }
}

impl<I, NowFn> RaftAwaitedActionDb<I, NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Clone + Send + Sync + Unpin + 'static,
{
    /// Build the db and start raft in the background. Reads work at once
    /// (from the snapshot, if any); writes wait until raft is ready. Use
    /// [`Self::open`] to also wait for readiness.
    pub fn new(
        config: RaftDbConfig,
        tasks_change_notify: Arc<Notify>,
        now_fn: NowFn,
    ) -> Result<Self, Error> {
        let watchers = Arc::new(SyncMutex::new(Watchers::default()));
        let on_apply = Self::on_apply(Arc::clone(&watchers), tasks_change_notify);
        let (log_store, sm) = match &config.data_dir {
            Some(dir) => (
                LogStore::open(&dir.join("log"))
                    .map_err(|e| Error::from_std_err(Code::Internal, &e))
                    .err_tip(|| format!("opening raft log in {}", dir.display()))?,
                StateMachineStore::open(&dir.join("snapshot"), on_apply)
                    .map_err(|e| Error::from_std_err(Code::Internal, &e))
                    .err_tip(|| format!("opening raft snapshot in {}", dir.display()))?,
            ),
            None => (LogStore::default(), StateMachineStore::new(on_apply)),
        };
        sm.set_retry_limit(config.retry_limit);

        let raft_config = Config {
            cluster_name: "nativelink-scheduler".to_string(),
            heartbeat_interval: 100,
            election_timeout_min: 200,
            election_timeout_max: 400,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_every),
            max_in_snapshot_log_to_keep: 1000,
            ..Default::default()
        }
        .validate()
        .map_err(|e| make_err!(Code::InvalidArgument, "raft config invalid: {e}"))?;

        let (raft_tx, raft_rx) = watch::channel(None);
        let node_id = config.node_id;
        let init_sm = sm.clone();
        let init = spawn!("raft_awaited_action_db_init", async move {
            let result = Self::start_raft(node_id, Arc::new(raft_config), log_store, init_sm).await;
            if let Err(e) = &result {
                warn!(?e, "raft scheduler state failed to start");
            }
            raft_tx.send_replace(Some(result));
        });

        let shared = Arc::new(Shared {
            sm,
            watchers,
            clients: Arc::new(SyncMutex::new(ClientTracker::default())),
        });
        Ok(Self {
            raft_rx,
            shared,
            config,
            now_fn,
            _init: init,
        })
    }

    /// Build the db and wait until raft has elected this node and applied
    /// everything already in its log.
    pub async fn open(
        config: RaftDbConfig,
        tasks_change_notify: Arc<Notify>,
        now_fn: NowFn,
    ) -> Result<Self, Error> {
        let db = Self::new(config, tasks_change_notify, now_fn)?;
        db.raft().await?;
        // Clients restored from a snapshot get a full retention period from
        // now, not from whenever they were last seen before the restart.
        let now = (db.now_fn)().now();
        let restored: Vec<OperationId> = db
            .shared
            .sm
            .with_state(|s| s.client_to_operation.keys().cloned().collect())
            .await;
        {
            let mut clients = db.shared.clients.lock();
            for client in &restored {
                clients.touch(client, now);
            }
        }
        Ok(db)
    }

    async fn start_raft(
        node_id: NodeId,
        config: Arc<Config>,
        log_store: LogStore,
        sm: StateMachineStore,
    ) -> Result<Raft<TypeConfig>, String> {
        let raft = Raft::new(node_id, config, SingleNodeNetwork, log_store, sm)
            .await
            .map_err(|e| format!("raft init failed: {e}"))?;
        if !raft
            .is_initialized()
            .await
            .map_err(|e| format!("raft is_initialized failed: {e}"))?
        {
            let mut members = BTreeMap::new();
            members.insert(node_id, BasicNode::default());
            raft.initialize(members)
                .await
                .map_err(|e| format!("raft initialize failed: {e}"))?;
        }
        // Wait to become leader and to have applied everything committed,
        // including entries replayed from a durable log after a restart.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match raft.ensure_linearizable().await {
                Ok(_) => return Ok(raft),
                Err(e) if tokio::time::Instant::now() < deadline => {
                    debug!(%e, "raft not ready yet");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => return Err(format!("raft did not become ready: {e}")),
            }
        }
    }

    async fn raft(&self) -> Result<Raft<TypeConfig>, Error> {
        let mut rx = self.raft_rx.clone();
        let guard = rx
            .wait_for(Option::is_some)
            .await
            .map_err(|e| make_err!(Code::Internal, "raft init task ended: {e}"))?;
        match guard.as_ref() {
            Some(Ok(raft)) => Ok(raft.clone()),
            Some(Err(e)) => Err(make_err!(Code::Unavailable, "{e}")),
            None => Err(make_err!(Code::Internal, "raft init state missing")),
        }
    }

    fn now(&self) -> SystemTime {
        (self.now_fn)().now()
    }

    fn now_unix_nanos(&self) -> u128 {
        unix_nanos(self.now())
    }

    /// The hook apply calls with each change: publish it to subscribers and
    /// wake the matching engine. Runs under the state machine's write lock,
    /// so it must not block; both steps are synchronous and cheap.
    fn on_apply(watchers: Arc<SyncMutex<Watchers>>, tasks_change_notify: Arc<Notify>) -> OnApply {
        Arc::new(move |changes| {
            {
                let mut watchers = watchers.lock();
                for (op_id, action) in changes {
                    match action {
                        Some(action) => {
                            watchers.publish(&op_id, &action);
                        }
                        None => watchers.remove(&op_id),
                    }
                }
                watchers.prune_unwatched_finished();
            }
            tasks_change_notify.notify_one();
        })
    }

    /// Propose a command through the log and wait for it to be applied.
    async fn propose(&self, cmd: Command) -> Result<CommandResponse, Error> {
        let resp = self
            .raft()
            .await?
            .client_write(cmd)
            .await
            .map_err(|e| make_err!(Code::Unavailable, "raft client_write failed: {e}"))?;
        Ok(resp.data)
    }

    /// Ensure a watch channel exists for `op_id`, seeded with the current state,
    /// and return a receiver. Called on the read path so a subscriber can be
    /// handed out before the next change.
    async fn subscribe_op(
        &self,
        op_id: &OperationId,
        client_operation_id: Option<OperationId>,
    ) -> Result<Option<RaftAwaitedActionSubscriber<I, NowFn>>, Error> {
        let Some(action) = self
            .shared
            .sm
            .with_state(|state| state.operations.get(op_id).cloned())
            .await
        else {
            return Ok(None);
        };
        let mut w = self.shared.watchers.lock();
        // Guarded publish: if a newer revision was already delivered by the pump
        // while we were between the snapshot read and this lock, do NOT regress
        // the channel to our captured (older) value (Astra premortem S3).
        w.publish(op_id, &action);
        let Some(tx) = w.txs.get(op_id) else {
            return Err(make_err!(Code::Internal, "publish did not create a channel"));
        };
        let mut rx = tx.subscribe();
        rx.mark_changed();
        let client = client_operation_id.map(|client_operation_id| ClientInfo {
            client_operation_id,
            last_keep_alive: I::from_secs(0),
            now_fn: self.now_fn.clone(),
        });
        Ok(Some(RaftAwaitedActionSubscriber {
            rx,
            client,
            clients: Arc::clone(&self.shared.clients),
        }))
    }

    /// Drop client operation ids through the log, and forget what this
    /// replica tracked about them and about the operations that went with
    /// them.
    async fn drop_clients(&self, clients: Vec<OperationId>, now: SystemTime) -> Result<(), Error> {
        if clients.is_empty() {
            return Ok(());
        }
        let resp = self
            .propose(Command::DropClients {
                client_operation_ids: clients.clone(),
                now_unix_nanos: unix_nanos(now),
            })
            .await?;
        let removed = match resp {
            CommandResponse::Dropped { removed } => removed,
            _ => Vec::new(),
        };
        self.shared.clients.lock().forget(&clients, &removed);
        Ok(())
    }

    /// Drop every client operation id this replica has not seen used for
    /// `retain_client`. Runs on insert, as the memory backend's `EvictingMap`
    /// does, so it follows an injected clock exactly.
    async fn evict_stale_clients(&self) -> Result<(), Error> {
        let now = self.now();
        let stale = self.shared.clients.lock().stale(now, self.config.retain_client);
        self.drop_clients(stale, now).await
    }

    /// Test/driver helper: run one deterministic `Expire` sweep through the log.
    pub async fn expire(&self, lease_timeout: Duration) -> Result<Vec<OperationId>, Error> {
        let resp = self
            .propose(Command::Expire {
                now_unix_nanos: self.now_unix_nanos(),
                lease_timeout_nanos: lease_timeout.as_nanos(),
            })
            .await?;
        match resp {
            CommandResponse::Expired { requeued } => Ok(requeued),
            _ => Ok(Vec::new()),
        }
    }

    /// Assign a currently-eligible (Queued) operation to a worker session,
    /// through the log with validation in apply. Returns the applied response so
    /// callers can see acceptance vs. a `RevisionConflict` — "client_write
    /// succeeded" does NOT mean "assignment accepted" (Astra premortem S0).
    pub async fn assign_to_worker(
        &self,
        operation_id: OperationId,
        expected_revision: i64,
        worker_id: WorkerId,
        session_epoch: u64,
    ) -> Result<CommandResponse, Error> {
        self.propose(Command::AssignToWorker {
            operation_id,
            expected_revision,
            worker_id,
            session_epoch,
            now_unix_nanos: self.now_unix_nanos(),
        })
        .await
    }

    /// Record a worker heartbeat that names its owning attempt/session, through
    /// the log. A heartbeat that no longer owns the op (superseded attempt) is
    /// rejected in apply (S6d).
    pub async fn heartbeat(
        &self,
        operation_id: OperationId,
        worker_id: WorkerId,
        session_epoch: u64,
        attempt: u64,
    ) -> Result<CommandResponse, Error> {
        self.heartbeat_at(
            operation_id,
            worker_id,
            session_epoch,
            attempt,
            self.now_unix_nanos(),
        )
        .await
    }

    /// Like [`Self::heartbeat`] but with an explicit timestamp, for tests that
    /// need to construct heartbeat reordering deterministically.
    pub async fn heartbeat_at(
        &self,
        operation_id: OperationId,
        worker_id: WorkerId,
        session_epoch: u64,
        attempt: u64,
        now_unix_nanos: u128,
    ) -> Result<CommandResponse, Error> {
        self.propose(Command::Heartbeat {
            operation_id,
            worker_id,
            session_epoch,
            attempt,
            now_unix_nanos,
        })
        .await
    }

    /// The current lease timestamp recorded for an operation, for tests.
    pub async fn lease_of(&self, operation_id: &OperationId) -> Option<u128> {
        self.shared
            .sm
            .with_state(|s| s.leases.get(operation_id).copied())
            .await
    }

    /// Register (or re-register) a worker session under `worker_id` at
    /// `session_epoch`, through the log. Returns `(applied, requeued)`: whether
    /// the join took effect, and the operations a supersession requeued.
    pub async fn worker_join(
        &self,
        worker_id: WorkerId,
        session_epoch: u64,
    ) -> Result<(bool, Vec<OperationId>), Error> {
        let resp = self
            .propose(Command::WorkerJoin {
                worker_id,
                session_epoch,
                now_unix_nanos: self.now_unix_nanos(),
            })
            .await?;
        match resp {
            CommandResponse::WorkerMembership { applied, requeued } => Ok((applied, requeued)),
            _ => Ok((false, Vec::new())),
        }
    }

    /// A worker session leaves, through the log. Returns `(applied, requeued)`.
    pub async fn worker_leave(
        &self,
        worker_id: WorkerId,
        session_epoch: u64,
    ) -> Result<(bool, Vec<OperationId>), Error> {
        let resp = self
            .propose(Command::WorkerLeave {
                worker_id,
                session_epoch,
                now_unix_nanos: self.now_unix_nanos(),
            })
            .await?;
        match resp {
            CommandResponse::WorkerMembership { applied, requeued } => Ok((applied, requeued)),
            _ => Ok((false, Vec::new())),
        }
    }

    /// A consistent snapshot of the worker-set as the applied log defines it:
    /// worker_id -> current session epoch. Matching reads this; there is no
    /// "changed under me" epoch to detect because the snapshot is a point in
    /// the totally-ordered log (CROSSED-OFF #5).
    pub async fn worker_sessions_snapshot(&self) -> HashMap<WorkerId, u64> {
        self.shared.sm.with_state(|s| s.sessions.clone()).await
    }

    /// The owning `(worker_id, session_epoch, attempt)` of an in-flight
    /// operation, from the same applied snapshot as the worker-set. Never
    /// disagrees with `worker_sessions_snapshot`: both are one function of the
    /// log.
    pub async fn operation_owner(
        &self,
        operation_id: &OperationId,
    ) -> Option<(WorkerId, u64, u64)> {
        self.shared
            .sm
            .with_state(|s| {
                s.operation_owner
                    .get(operation_id)
                    .map(|o| (o.worker_id.clone(), o.session_epoch, o.attempt))
            })
            .await
    }

    /// The current revision recorded for an operation, for callers that need an
    /// `expected_revision` without holding a subscriber.
    pub async fn revision_of(&self, operation_id: &OperationId) -> Option<i64> {
        self.shared
            .sm
            .with_state(|s| s.operation_revision.get(operation_id).copied())
            .await
    }

    /// A clone of the current applied state, for tests that exercise the
    /// snapshot serialization path directly.
    pub async fn applied_state_for_test(&self) -> AppliedState {
        self.shared.sm.snapshot_state().await
    }

    /// Stop raft cleanly, for restart tests: the log and snapshot on disk are
    /// what the next `open` of the same `data_dir` starts from.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.raft()
            .await?
            .shutdown()
            .await
            .map_err(|e| make_err!(Code::Internal, "raft shutdown failed: {e}"))
    }

    /// Force a snapshot now, for tests of the snapshot-plus-replay path.
    pub async fn trigger_snapshot(&self) -> Result<(), Error> {
        let raft = self.raft().await?;
        raft.trigger()
            .snapshot()
            .await
            .map_err(|e| make_err!(Code::Internal, "raft snapshot trigger failed: {e}"))?;
        // Copy out of the metrics channel before awaiting: holding its read
        // guard across the wait would block raft from publishing metrics.
        let last_applied = raft
            .metrics()
            .borrow()
            .last_applied
            .ok_or_else(|| make_err!(Code::Internal, "nothing applied to snapshot"))?;
        raft.wait(Some(Duration::from_secs(10)))
            .snapshot(last_applied, "trigger_snapshot")
            .await
            .map_err(|e| make_err!(Code::Internal, "waiting for snapshot failed: {e}"))?;
        Ok(())
    }
}

impl<I, NowFn> AwaitedActionDb for RaftAwaitedActionDb<I, NowFn>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Clone + Send + Sync + Unpin + 'static,
{
    type Subscriber = RaftAwaitedActionSubscriber<I, NowFn>;

    async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        // A lookup evicts only its own key, and only if it expired; otherwise
        // it counts as a use (`EvictingMap::get`).
        let now = self.now();
        let stale = self
            .shared
            .clients
            .lock()
            .is_stale(client_operation_id, now, self.config.retain_client);
        if stale {
            self.drop_clients(vec![client_operation_id.clone()], now)
                .await?;
            return Ok(None);
        }
        let Some(op_id) = self
            .shared
            .sm
            .with_state(|s| s.client_to_operation.get(client_operation_id).cloned())
            .await
        else {
            return Ok(None);
        };
        self.shared.clients.lock().touch(client_operation_id, now);
        self.subscribe_op(&op_id, Some(client_operation_id.clone()))
            .await
    }

    fn get_all_awaited_actions(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error>,
    > + Send {
        async move {
            let op_ids: Vec<OperationId> = self
                .shared
                .sm
                .with_state(|s| s.operations.keys().cloned().collect())
                .await;
            let mut subs = Vec::with_capacity(op_ids.len());
            for op_id in op_ids {
                if let Some(sub) = self.subscribe_op(&op_id, None).await? {
                    subs.push(Ok(sub));
                }
            }
            Ok(stream::iter(subs))
        }
    }

    async fn get_by_operation_id(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        self.subscribe_op(operation_id, None).await
    }

    fn get_range_of_actions(
        &self,
        state: SortedAwaitedActionState,
        start: Bound<SortedAwaitedAction>,
        end: Bound<SortedAwaitedAction>,
        desc: bool,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error>,
    > + Send {
        async move {
            let mut sorted: Vec<SortedAwaitedAction> = self
                .shared
                .sm
                .with_state(|applied| {
                    applied
                        .sorted
                        .btree_for_sorted_state(state)
                        .range((start, end))
                        .cloned()
                        .collect()
                })
                .await;
            if desc {
                sorted.reverse();
            }
            let mut subs = Vec::with_capacity(sorted.len());
            for sa in sorted {
                if let Some(sub) = self.subscribe_op(&sa.operation_id, None).await? {
                    subs.push(Ok(sub));
                }
            }
            Ok(stream::iter(subs))
        }
    }

    async fn update_awaited_action(&self, new_awaited_action: AwaitedAction) -> Result<(), Error> {
        let operation_id = new_awaited_action.operation_id().clone();
        // The caller's record carries the revision it last observed; apply
        // validates it (the CAS, moved into apply) and keeps every counter the
        // caller changed (S0).
        let expected_revision = new_awaited_action.version_public();
        let resp = self
            .propose(Command::UpdateAwaitedAction {
                operation_id,
                expected_revision,
                awaited_action: Box::new(new_awaited_action),
                now_unix_nanos: self.now_unix_nanos(),
            })
            .await?;
        // A rejected update is committed but not applied; surface it as an error
        // so a caller cannot mistake "client_write succeeded" for "accepted".
        if let CommandResponse::Rejected { reason } = resp {
            return Err(make_err!(
                Code::Aborted,
                "update_awaited_action rejected: {reason:?}"
            ));
        }
        Ok(())
    }

    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        _no_event_action_timeout: Duration,
    ) -> Result<Self::Subscriber, Error> {
        self.evict_stale_clients().await?;
        let operation_id = OperationId::default();
        // Capture the request's ambient origin metadata HERE (on the request
        // path, where the client's tracing baggage is in scope) and pass it in
        // the command, so apply never reads Context::current() and stays a pure
        // function of the command bytes (Astra premortem S6d).
        let origin_metadata = capture_origin_metadata();
        let now = self.now();
        self.propose(Command::AddAction {
            client_operation_id: client_operation_id.clone(),
            operation_id: operation_id.clone(),
            action_info,
            now_unix_nanos: unix_nanos(now),
            origin_metadata,
        })
        .await?;
        // The applied command may have joined an existing operation, so read the
        // real operation id back from the client map.
        let real_op = self
            .shared
            .sm
            .with_state(|s| s.client_to_operation.get(&client_operation_id).cloned())
            .await
            .unwrap_or(operation_id);
        self.shared
            .clients
            .lock()
            .keep_alive(&client_operation_id, &real_op, now);
        self.subscribe_op(&real_op, Some(client_operation_id))
            .await?
            .ok_or_else(|| make_err!(Code::Internal, "add_action: operation vanished after apply"))
    }
}
