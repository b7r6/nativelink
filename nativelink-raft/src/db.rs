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

//! The raft-backed `AwaitedActionDb` (single node).

use core::future::Future;
use core::ops::Bound;
use core::time::Duration;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{Stream, stream};
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_scheduler::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedAction,
    SortedAwaitedActionState, capture_origin_metadata,
};
use nativelink_util::action_messages::{ActionInfo, ActionState, OperationId, WorkerId};
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft};
use tokio::sync::{Mutex, mpsc, watch};

use crate::state::AppliedState;
use crate::store::{LogStore, StateMachineStore};
use crate::types::{Command, CommandResponse, NodeId, TypeConfig};

/// A network that goes nowhere: a single voter never dials a peer. Every method
/// is unreachable for a one-node cluster, so they error rather than lie.
#[derive(Clone)]
pub struct SingleNodeNetwork;

impl RaftNetworkFactory<TypeConfig> for SingleNodeNetwork {
    type Network = SingleNodeConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        SingleNodeConnection { target }
    }
}

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

fn now_unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

/// Subscriber backed by a `watch` channel, exactly like the memory backend.
#[derive(Debug, Clone)]
pub struct RaftAwaitedActionSubscriber {
    rx: watch::Receiver<AwaitedAction>,
    client_operation_id: Option<OperationId>,
}

impl AwaitedActionSubscriber for RaftAwaitedActionSubscriber {
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        self.rx
            .changed()
            .await
            .map_err(|e| Error::from_std_err(Code::Internal, &e))?;
        let mut awaited = self.rx.borrow().clone();
        if let Some(id) = &self.client_operation_id {
            awaited.set_client_operation_id_public(id.clone());
        }
        Ok(awaited)
    }

    fn borrow(&self) -> impl Future<Output = Result<AwaitedAction, Error>> + Send {
        let mut awaited = self.rx.borrow().clone();
        if let Some(id) = &self.client_operation_id {
            awaited.set_client_operation_id_public(id.clone());
        }
        std::future::ready(Ok(awaited))
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
    /// Publish `action` for `op_id`, but only if its revision is not older than
    /// what the channel already holds. Creating a fresh channel always seeds it.
    /// Returns true if the channel value advanced.
    fn publish(&mut self, op_id: &OperationId, action: &AwaitedAction) -> bool {
        match self.txs.get(op_id) {
            Some(tx) => {
                let current_rev = tx.borrow().version_public();
                if action.version_public() < current_rev {
                    // A publication for an older revision than already delivered:
                    // drop it. Nondecreasing revision, not stage order — an
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
}

/// The raft-backed database.
pub struct RaftAwaitedActionDb {
    raft: Raft<TypeConfig>,
    sm: StateMachineStore,
    watchers: Arc<Mutex<Watchers>>,
}

impl MetricsComponent for RaftAwaitedActionDb {
    fn publish(
        &self,
        _kind: nativelink_metric::MetricKind,
        _field_metadata: nativelink_metric::MetricFieldData,
    ) -> Result<nativelink_metric::MetricPublishKnownKindData, nativelink_metric::Error> {
        Ok(nativelink_metric::MetricPublishKnownKindData::Component)
    }
}

impl core::fmt::Debug for RaftAwaitedActionDb {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RaftAwaitedActionDb")
            .finish_non_exhaustive()
    }
}

impl RaftAwaitedActionDb {
    /// Boot a single-node cluster (one voter) and return the db.
    pub async fn new_single_node() -> Result<Self, Error> {
        let config = Arc::new(
            Config {
                cluster_name: "nativelink-scheduler".to_string(),
                // Fast timers so a single-node election completes promptly in tests.
                heartbeat_interval: 100,
                election_timeout_min: 200,
                election_timeout_max: 400,
                ..Default::default()
            }
            .validate()
            .map_err(|e| make_err!(Code::Internal, "raft config invalid: {e}"))?,
        );

        let (changed_tx, changed_rx) = mpsc::unbounded_channel();
        let log_store = LogStore::default();
        let sm = StateMachineStore::new(changed_tx);

        let raft = Raft::new(1, config, SingleNodeNetwork, log_store, sm.clone())
            .await
            .map_err(|e| make_err!(Code::Internal, "raft init failed: {e}"))?;

        // Elect ourselves the single voter.
        let mut members = std::collections::BTreeMap::new();
        members.insert(1u64, BasicNode::default());
        raft.initialize(members)
            .await
            .map_err(|e| make_err!(Code::Internal, "raft initialize failed: {e}"))?;

        let watchers = Arc::new(Mutex::new(Watchers::default()));

        // Pump changed-operation-ids from the state machine into the watch
        // channels. The log is the writer; this only wakes readers.
        Self::spawn_watcher_pump(sm.clone(), Arc::clone(&watchers), changed_rx);

        Ok(Self { raft, sm, watchers })
    }

    fn spawn_watcher_pump(
        sm: StateMachineStore,
        watchers: Arc<Mutex<Watchers>>,
        mut changed_rx: mpsc::UnboundedReceiver<Vec<OperationId>>,
    ) {
        tokio::spawn(async move {
            while let Some(op_ids) = changed_rx.recv().await {
                let state = sm.snapshot_state().await;
                let mut w = watchers.lock().await;
                for op_id in op_ids {
                    if let Some(action) = state.operations.get(&op_id) {
                        w.publish(&op_id, action);
                    }
                }
            }
        });
    }

    /// Propose a command through the log and wait for it to be applied.
    async fn propose(&self, cmd: Command) -> Result<CommandResponse, Error> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| make_err!(Code::Internal, "raft client_write failed: {e}"))?;
        Ok(resp.data)
    }

    /// Ensure a watch channel exists for `op_id`, seeded with the current state,
    /// and return a receiver. Called on the read path so a subscriber can be
    /// handed out before the next change.
    async fn subscribe_op(
        &self,
        op_id: &OperationId,
        client_operation_id: Option<OperationId>,
    ) -> Result<Option<RaftAwaitedActionSubscriber>, Error> {
        let state = self.sm.snapshot_state().await;
        let Some(action) = state.operations.get(op_id) else {
            return Ok(None);
        };
        let mut w = self.watchers.lock().await;
        // Guarded publish: if a newer revision was already delivered by the pump
        // while we were between the snapshot read and this lock, do NOT regress
        // the channel to our captured (older) value (Astra premortem S3).
        w.publish(op_id, action);
        let tx = w
            .txs
            .get(op_id)
            .expect("publish inserts the channel if absent");
        let mut rx = tx.subscribe();
        rx.mark_changed();
        Ok(Some(RaftAwaitedActionSubscriber {
            rx,
            client_operation_id,
        }))
    }

    /// Test/driver helper: run one deterministic `Expire` sweep through the log.
    pub async fn expire(&self, lease_timeout: Duration) -> Result<Vec<OperationId>, Error> {
        let resp = self
            .propose(Command::Expire {
                now_unix_nanos: now_unix_nanos(),
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
            now_unix_nanos: now_unix_nanos(),
        })
        .await
    }

    /// Record a worker heartbeat that names its owning attempt/session, through
    /// the log. The identity is read from current ownership. A heartbeat that no
    /// longer owns the op (superseded attempt) is rejected in apply (S6d).
    pub async fn heartbeat(
        &self,
        operation_id: OperationId,
        worker_id: WorkerId,
        session_epoch: u64,
        attempt: u64,
    ) -> Result<CommandResponse, Error> {
        self.propose(Command::Heartbeat {
            operation_id,
            worker_id,
            session_epoch,
            attempt,
            now_unix_nanos: now_unix_nanos(),
        })
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
        self.sm
            .snapshot_state()
            .await
            .leases
            .get(operation_id)
            .copied()
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
                now_unix_nanos: now_unix_nanos(),
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
                now_unix_nanos: now_unix_nanos(),
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
        self.sm.snapshot_state().await.sessions
    }

    /// The owning `(worker_id, session_epoch, attempt)` of an in-flight
    /// operation, from the same applied snapshot as the worker-set. Never
    /// disagrees with `worker_sessions_snapshot`: both are one function of the
    /// log.
    pub async fn operation_owner(
        &self,
        operation_id: &OperationId,
    ) -> Option<(WorkerId, u64, u64)> {
        self.sm
            .snapshot_state()
            .await
            .operation_owner
            .get(operation_id)
            .map(|o| (o.worker_id.clone(), o.session_epoch, o.attempt))
    }

    /// The current revision recorded for an operation, for callers that need an
    /// `expected_revision` without holding a subscriber.
    pub async fn revision_of(&self, operation_id: &OperationId) -> Option<i64> {
        self.sm
            .snapshot_state()
            .await
            .operation_revision
            .get(operation_id)
            .copied()
    }

    /// A clone of the current applied state, for tests that exercise the
    /// snapshot serialization path directly.
    pub async fn applied_state_for_test(&self) -> AppliedState {
        self.sm.snapshot_state().await
    }
}

impl AwaitedActionDb for RaftAwaitedActionDb {
    type Subscriber = RaftAwaitedActionSubscriber;

    async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        let state = self.sm.snapshot_state().await;
        let Some(op_id) = state.client_to_operation.get(client_operation_id).cloned() else {
            return Ok(None);
        };
        self.subscribe_op(&op_id, Some(client_operation_id.clone()))
            .await
    }

    fn get_all_awaited_actions(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error>,
    > + Send {
        async move {
            let state = self.sm.snapshot_state().await;
            let op_ids: Vec<OperationId> = state.operations.keys().cloned().collect();
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
            let applied: AppliedState = self.sm.snapshot_state().await;
            let btree = applied.sorted.btree_for_sorted_state(state);
            let mut sorted: Vec<SortedAwaitedAction> = btree.range((start, end)).cloned().collect();
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
        let new_state: Arc<ActionState> = new_awaited_action.state().clone();
        let worker_id: Option<WorkerId> = new_awaited_action.worker_id_public().cloned();
        // The caller's record carries the revision it last observed and the
        // attempt counter it just incremented; both travel with the command so
        // apply can validate the CAS and preserve the counter (S0).
        let expected_revision = new_awaited_action.version_public();
        let attempts = new_awaited_action.attempts;
        let resp = self
            .propose(Command::UpdateAwaitedAction {
                operation_id,
                expected_revision,
                new_state,
                worker_id,
                attempts,
                now_unix_nanos: now_unix_nanos(),
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
        let operation_id = OperationId::default();
        // Capture the request's ambient origin metadata HERE (on the request
        // path, where the client's tracing baggage is in scope) and pass it in
        // the command, so apply never reads Context::current() and stays a pure
        // function of the command bytes (Astra premortem S6d).
        let origin_metadata = capture_origin_metadata();
        let resp = self
            .propose(Command::AddAction {
                client_operation_id: client_operation_id.clone(),
                operation_id: operation_id.clone(),
                action_info,
                now_unix_nanos: now_unix_nanos(),
                origin_metadata,
            })
            .await?;
        // The applied command may have joined an existing operation, so read the
        // real operation id back from the client map.
        let state = self.sm.snapshot_state().await;
        let real_op = state
            .client_to_operation
            .get(&client_operation_id)
            .cloned()
            .unwrap_or(operation_id);
        let _ = resp;
        self.subscribe_op(&real_op, Some(client_operation_id))
            .await?
            .ok_or_else(|| make_err!(Code::Internal, "add_action: operation vanished after apply"))
    }
}
