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

//! Real replicated `OpenRaft` with a bounded serialized in-process fault
//! transport. Durable and volatile histories use the same journal implementation.
//! The harness supplies identity/fresh-learner procedures explicitly; it does not
//! qualify authenticated peer networking or a production membership reconciler.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::journal::{Batch, Journal, TypeConfig};
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, Event, IncarnationId, Limits, OperationId, RecordedEvent, Reply,
    Request, Resources, SessionId, State, WorkerId,
};
use openraft::error::{
    CheckIsLeaderError, Fatal, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, EntryPayload, Raft, RaftNetwork, RaftNetworkFactory, SnapshotMeta, SnapshotPolicy,
    StoredMembership, Vote,
};
use parking_lot::Mutex;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Notify;

const EPOCH: ClusterEpoch = ClusterEpoch(83);
const WAIT: Duration = Duration::from_secs(15);
const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;
const MAX_APPEND_ENTRIES: usize = 4;

fn unreachable<E: std::error::Error>(message: &str) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&io::Error::new(
        io::ErrorKind::NotConnected,
        message,
    )))
}

#[derive(Default)]
struct Links {
    peers: BTreeMap<u64, Peer>,
    blocked: BTreeSet<(u64, u64)>,
    lose_append_reply: Option<(u64, u64, OperationId)>,
    lost_append_entry: Option<(u64, u64, u64)>,
    acknowledged_after_loss: Option<u64>,
    delivered_entries: BTreeMap<(u64, u64, u64), usize>,
    interrupt_snapshot: Option<(u64, u64)>,
    lost_append_replies: usize,
    interrupted_snapshots: usize,
    append_entries: usize,
    snapshot_chunks: usize,
    max_rpc_bytes: usize,
    hold_reply: Option<(u64, u64, RpcKind)>,
    withhold_uniform_from: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RpcKind {
    Append,
    Snapshot,
    Vote,
}

#[derive(Clone)]
struct Peer {
    epoch: ClusterEpoch,
    raft: Raft<TypeConfig>,
}

#[derive(Clone, Default)]
struct Fabric {
    links: Arc<Mutex<Links>>,
    snapshot_interrupted: Arc<Notify>,
    reply_held: Arc<Notify>,
    release_reply: Arc<Notify>,
    uniform_withheld: Arc<Notify>,
}

impl Fabric {
    fn peer(
        &self,
        from: u64,
        to: u64,
        epoch: ClusterEpoch,
    ) -> Result<Raft<TypeConfig>, &'static str> {
        let links = self.links.lock();
        // This represents trusted transport identity in the fixture. OpenRaft's
        // cluster_name is only a log label, and its votes/empty heartbeats do not
        // carry application epoch. A deployed transport must authenticate this
        // identity before calling any Raft API, including snapshot fragments.
        let source = links.peers.get(&from).ok_or("sender stopped")?;
        let target = links.peers.get(&to).ok_or("peer stopped")?;
        if source.epoch != epoch || target.epoch != epoch {
            return Err("foreign authority domain");
        }
        if links.blocked.contains(&(from, to)) {
            return Err("scheduled directed partition");
        }
        Ok(target.raft.clone())
    }

    fn encode<T: Serialize + DeserializeOwned>(&self, value: &T) -> T {
        // Both sides cross the serialization boundary. The test producer also
        // bounds entries/chunks; this is not an untrusted production decoder.
        let bytes = serde_json::to_vec(value).unwrap();
        assert!(bytes.len() <= MAX_RPC_BYTES, "aggregate RPC byte bound");
        let mut links = self.links.lock();
        links.max_rpc_bytes = links.max_rpc_bytes.max(bytes.len());
        drop(links);
        serde_json::from_slice(&bytes).unwrap()
    }

    fn isolate(&self, id: u64) {
        let mut links = self.links.lock();
        let others: Vec<_> = links.peers.keys().copied().filter(|p| *p != id).collect();
        for other in others {
            links.blocked.insert((id, other));
            links.blocked.insert((other, id));
        }
    }

    fn heal(&self) {
        self.links.lock().blocked.clear();
    }
}

#[derive(Clone)]
struct Network {
    from: u64,
    to: u64,
    epoch: ClusterEpoch,
    fabric: Fabric,
}

impl Network {
    fn peer(&self) -> Result<Raft<TypeConfig>, &'static str> {
        self.fabric.peer(self.from, self.to, self.epoch)
    }

    async fn response_boundary(&self, kind: RpcKind) -> Result<(), &'static str> {
        let held = {
            let mut links = self.fabric.links.lock();
            if links.hold_reply == Some((self.from, self.to, kind)) {
                links.hold_reply = None;
                true
            } else {
                false
            }
        };
        if held {
            self.fabric.reply_held.notify_one();
            self.fabric.release_reply.notified().await;
        }
        // The endpoint can change domains while either a successful response
        // or a remote error is in flight. Check before interpreting either.
        self.peer().map(|_| ())
    }
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
        let peer = self.peer().map_err(unreachable)?;
        assert!(rpc.entries.len() <= MAX_APPEND_ENTRIES);
        let withhold = self.fabric.links.lock().withhold_uniform_from == Some(self.from);
        if withhold
            && let Some(index) = rpc.entries.iter().position(|entry| {
                matches!(&entry.payload, EntryPayload::Membership(membership)
                    if membership.get_joint_config().len() == 1
                        && membership.voter_ids().eq([1, 2, 4]))
            })
        {
            // Deliver only the valid preceding prefix, including the commit
            // marker for joint membership. The final uniform entry never
            // reaches any recipient. This is an explicit sender cutpoint,
            // not a claim that partial wire bytes form a valid RPC.
            let mut prefix = rpc;
            prefix.entries.truncate(index);
            let _response = peer.append_entries(self.fabric.encode(&prefix)).await;
            self.fabric.uniform_withheld.notify_one();
            return Err(unreachable("uniform membership held before delivery"));
        }
        self.fabric.links.lock().append_entries += rpc.entries.len();
        let response = peer.append_entries(self.fabric.encode(&rpc)).await;
        self.response_boundary(RpcKind::Append)
            .await
            .map_err(unreachable)?;
        let response = response.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        let mut links = self.fabric.links.lock();
        if matches!(response, AppendEntriesResponse::Success) {
            // Count only recipient-confirmed delivery, not an RPC merely
            // attempted or rejected for a conflicting previous log entry.
            for entry in &rpc.entries {
                *links
                    .delivered_entries
                    .entry((self.from, self.to, entry.log_id.index))
                    .or_default() += 1;
            }
            if let Some((from, to, operation)) = links.lose_append_reply
                && (from, to) == (self.from, self.to)
                && let Some(entry) = rpc.entries.iter().find(|entry| {
                    matches!(&entry.payload, EntryPayload::Normal(batch) if batch.events().iter().any(|recorded| {
                        matches!(&recorded.event, Event::Submit { operation: target, .. } if *target == operation)
                    }))
                })
            {
                links.lose_append_reply = None;
                links.lost_append_replies += 1;
                links.lost_append_entry = Some((self.from, self.to, entry.log_id.index));
                return Err(unreachable("target normal entry durable on recipient; reply lost"));
            }
            if let Some((from, to, index)) = links.lost_append_entry
                && (from, to) == (self.from, self.to)
                && let Some(confirmed) = rpc
                    .entries
                    .last()
                    .map(|entry| entry.log_id)
                    .or(rpc.prev_log_id)
                && confirmed.index >= index
            {
                // A retry may resend data or prove an already matched prefix
                // with a heartbeat. Both yield a real response after the loss.
                links.acknowledged_after_loss = Some(confirmed.index);
            }
        }
        drop(links);
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
        let peer = self.peer().map_err(unreachable)?;
        assert!(rpc.data.len() <= 512);
        let partial = !rpc.done;
        self.fabric.links.lock().snapshot_chunks += 1;
        let response = peer.install_snapshot(self.fabric.encode(&rpc)).await;
        self.response_boundary(RpcKind::Snapshot)
            .await
            .map_err(unreachable)?;
        let response = response.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        let mut links = self.fabric.links.lock();
        if partial && links.interrupt_snapshot == Some((self.from, self.to)) {
            links.interrupt_snapshot = None;
            links.interrupted_snapshots += 1;
            links.blocked.insert((self.from, self.to));
            links.blocked.insert((self.to, self.from));
            self.fabric.snapshot_interrupted.notify_one();
            return Err(unreachable("snapshot prefix received; link interrupted"));
        }
        drop(links);
        Ok(self.fabric.encode(&response))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let peer = self.peer().map_err(unreachable)?;
        let response = peer.vote(self.fabric.encode(&rpc)).await;
        self.response_boundary(RpcKind::Vote)
            .await
            .map_err(unreachable)?;
        let response = response.map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        Ok(self.fabric.encode(&response))
    }
}

struct Node {
    raft: Raft<TypeConfig>,
    journal: Journal,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Backing {
    Durable,
    Volatile,
}

struct Cluster {
    directory: PathBuf,
    fabric: Fabric,
    nodes: BTreeMap<u64, Node>,
    backing: Backing,
    used_volatile_ids: BTreeSet<u64>,
}

impl Cluster {
    async fn new() -> Self {
        Self::with_backing(Backing::Durable).await
    }

    async fn with_backing(backing: Backing) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "nativelink-authority-cluster-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut cluster = Self {
            directory,
            fabric: Fabric::default(),
            nodes: BTreeMap::new(),
            backing,
            used_volatile_ids: BTreeSet::new(),
        };
        for id in 1..=3 {
            cluster.start(id).await;
        }
        cluster.nodes[&1]
            .raft
            .initialize(BTreeSet::from([1]))
            .await
            .unwrap();
        cluster.leader(1).await;
        for id in 2..=3 {
            tokio::time::timeout(
                WAIT,
                cluster.nodes[&1]
                    .raft
                    .add_learner(id, BasicNode::default(), true),
            )
            .await
            .unwrap()
            .unwrap();
        }
        tokio::time::timeout(
            WAIT,
            cluster.nodes[&1]
                .raft
                .change_membership(BTreeSet::from([1, 2, 3]), false),
        )
        .await
        .unwrap()
        .unwrap();
        cluster
    }

    async fn start(&mut self, id: u64) {
        self.try_start(id).await.unwrap();
    }

    async fn try_start(&mut self, id: u64) -> Result<(), &'static str> {
        if self.nodes.contains_key(&id) {
            return Err("member is already running");
        }
        // The test's allocator records identities outside the volatile member.
        // This is an explicit procedure, not proof that Journal/OpenRaft can
        // infer identity loss or authorize a full-cluster disaster reset.
        if self.backing == Backing::Volatile && self.used_volatile_ids.contains(&id) {
            return Err("volatile voting history was lost; a fresh member identity is required");
        }
        let journal = match self.backing {
            Backing::Durable => {
                Journal::open(
                    self.directory.join(format!("node-{id}.redb")),
                    id,
                    EPOCH,
                    Limits::default(),
                )
                .await
            }
            Backing::Volatile => Journal::fresh_volatile(id, EPOCH, Limits::default()).await,
        }
        .unwrap();
        let config = openraft::Config {
            cluster_name: "categorical-replicated-test".to_owned(),
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            enable_elect: false,
            max_payload_entries: MAX_APPEND_ENTRIES as u64,
            snapshot_max_chunk_size: 512,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
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
                epoch: EPOCH,
                fabric: self.fabric.clone(),
            },
            journal.clone(),
            journal.clone(),
        )
        .await
        .unwrap();
        self.fabric.links.lock().peers.insert(
            id,
            Peer {
                epoch: EPOCH,
                raft: raft.clone(),
            },
        );
        self.nodes.insert(id, Node { raft, journal });
        if self.backing == Backing::Volatile {
            self.used_volatile_ids.insert(id);
        }
        Ok(())
    }

    async fn leader(&self, id: u64) {
        self.nodes[&id]
            .raft
            .wait(Some(WAIT))
            .current_leader(id, "requested leader is established")
            .await
            .unwrap();
        self.healthy_barrier(id).await;
    }

    async fn healthy_barrier(&self, id: u64) {
        // A healthy cluster need not answer within the first 50 ms heartbeat
        // window on a loaded test host. Retry only that availability failure;
        // no state is read before a successful real quorum barrier. The
        // partition fixture calls ensure_linearizable directly and still fails.
        tokio::time::timeout(WAIT, async {
            loop {
                match self.nodes[&id].raft.ensure_linearizable().await {
                    Ok(_) => break,
                    Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("healthy quorum barrier failed: {error}"),
                }
            }
        })
        .await
        .expect("healthy cluster must regain a quorum read within the recovery bound");
    }

    async fn elect(&self, id: u64) {
        // Allow the candidate to retry through the real follower leader lease;
        // one forced vote immediately after partition is correctly rejected.
        self.nodes[&id].raft.runtime_config().elect(true);
        self.nodes[&id].raft.trigger().elect().await.unwrap();
        self.leader(id).await;
        self.nodes[&id].raft.runtime_config().elect(false);
    }

    async fn write(&self, leader: u64, events: impl IntoIterator<Item = RecordedEvent>) -> u64 {
        let reply = tokio::time::timeout(
            WAIT,
            self.nodes[&leader]
                .raft
                .client_write(Batch::new(events).unwrap()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            reply
                .data
                .replies
                .iter()
                .all(|r| !matches!(r, Reply::Rejected(_)))
        );
        reply.log_id.index
    }

    async fn read(&self, id: u64) -> State {
        self.healthy_barrier(id).await;
        self.nodes[&id].journal.read_applied().await.unwrap().1
    }

    async fn converge(&self, leader: u64, index: u64, members: &[u64]) {
        let expected = self.read(leader).await.snapshot().unwrap();
        for id in members {
            self.nodes[id]
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(Some(index), "replicated application")
                .await
                .unwrap();
            assert_eq!(
                self.nodes[id]
                    .journal
                    .read_applied()
                    .await
                    .unwrap()
                    .1
                    .snapshot()
                    .unwrap(),
                expected
            );
        }
    }

    async fn compact(&self, leader: u64, index: u64) {
        let raft = &self.nodes[&leader].raft;
        raft.trigger().snapshot().await.unwrap();
        raft.wait(Some(WAIT))
            .metrics(
                |m| m.snapshot.is_some_and(|id| id.index >= index),
                "snapshot published",
            )
            .await
            .unwrap();
        raft.trigger().purge_log(index).await.unwrap();
        raft.wait(Some(WAIT))
            .metrics(
                |m| m.purged.is_some_and(|id| id.index >= index),
                "compacted source",
            )
            .await
            .unwrap();
    }

    async fn assert_empty_learner(&self, id: u64) {
        let node = &self.nodes[&id];
        assert!(
            node.raft
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .voter_ids()
                .next()
                .is_none()
        );
        assert!(node.journal.clone().read_vote().await.unwrap().is_none());
        let state = node.journal.read_applied().await.unwrap();
        assert!(state.0.is_none());
        assert!(state.1.view().operations.is_empty());
        assert!(state.1.view().effects.is_empty());
        assert!(state.1.view().reservations.is_empty());
        assert!(node.raft.ensure_linearizable().await.is_err());
        assert!(
            tokio::time::timeout(
                WAIT,
                node.raft.client_write(Batch::new([submit(99)]).unwrap())
            )
            .await
            .unwrap()
            .is_err()
        );
        assert_eq!(node.journal.read_applied().await.unwrap().0, state.0);
    }

    async fn stop(&mut self, id: u64) {
        self.fabric.links.lock().peers.remove(&id);
        self.nodes
            .remove(&id)
            .unwrap()
            .raft
            .shutdown()
            .await
            .unwrap();
    }

    async fn shutdown(mut self) {
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
        assert!(self.fabric.links.lock().max_rpc_bytes <= MAX_RPC_BYTES);
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn submit(operation: u64) -> RecordedEvent {
    RecordedEvent {
        epoch: EPOCH,
        event: Event::Submit {
            operation: OperationId(operation),
            request: Request {
                action: ActionKey {
                    instance: "replicated-test".to_owned(),
                    digest_function: "sha256".to_owned(),
                    digest: format!("{operation:064x}"),
                    digest_size_bytes: 1,
                },
                execution_scope: None,
                do_not_cache: false,
                skip_cache_lookup: false,
                priority: 0,
                properties: BTreeMap::new(),
                resources: Resources {
                    cpu_millis: 100,
                    memory_bytes: 1024,
                    slots: 1,
                },
                execution_timeout: 100,
                payload: None,
            },
        },
    }
}

#[nativelink_test]
async fn minority_cannot_apply_or_serve_authority_and_rejoins_new_leader() {
    let cluster = Cluster::new().await;
    let mut first = submit(1);
    let Event::Submit { request, .. } = &mut first.event else {
        unreachable!()
    };
    request.skip_cache_lookup = true;
    let capacity = Resources {
        cpu_millis: 1000,
        memory_bytes: 4096,
        slots: 1,
    };
    let initial = cluster
        .write(
            1,
            [
                RecordedEvent {
                    epoch: EPOCH,
                    event: Event::Register {
                        worker: WorkerId(1),
                        incarnation: IncarnationId(1),
                        session: SessionId(1),
                        properties: BTreeMap::new(),
                        capacity,
                        available: capacity,
                    },
                },
                first,
                RecordedEvent {
                    epoch: EPOCH,
                    event: Event::Match,
                },
            ],
        )
        .await;
    cluster.converge(1, initial, &[1, 2, 3]).await;
    let before = cluster.read(1).await.snapshot().unwrap();
    cluster.fabric.isolate(1);

    // Observe the minority proposal in its log before electing the majority.
    // It must create neither an accepted reply nor an applied lookup effect.
    let mut pending = cluster.nodes[&1]
        .raft
        .client_write_ff(Batch::new([submit(99)]).unwrap())
        .await
        .unwrap();
    cluster.nodes[&1]
        .raft
        .wait(Some(WAIT))
        .log_index_at_least(Some(initial + 1), "minority has proposed log")
        .await
        .unwrap();
    assert_eq!(
        cluster.nodes[&1]
            .journal
            .read_applied()
            .await
            .unwrap()
            .1
            .snapshot()
            .unwrap(),
        before
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert!(!matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            cluster.nodes[&1].raft.ensure_linearizable()
        )
        .await,
        Ok(Ok(_))
    ));

    cluster.elect(2).await;
    let accepted = cluster.write(2, [submit(2), submit(3)]).await;
    cluster.fabric.heal();
    cluster.converge(2, accepted, &[1, 2, 3]).await;
    let state = cluster.read(2).await.view();
    assert_eq!(
        state.operations.iter().map(|o| o.id.0).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        state.effects.len(),
        3,
        "all accepted pending effects survive election"
    );
    assert_eq!(state.reservations.len(), 1);
    assert!(!state.reservations[0].released);
    assert_eq!(state.workers[0].charged.slots, 1);
    cluster.shutdown().await;
}

#[nativelink_test]
async fn durable_append_with_lost_reply_is_retried_without_duplicate_effects() {
    let cluster = Cluster::new().await;
    cluster.fabric.isolate(3);
    cluster.fabric.links.lock().lose_append_reply = Some((1, 2, OperationId(1)));
    let accepted = cluster.write(1, [submit(1), submit(2), submit(3)]).await;
    {
        let links = cluster.fabric.links.lock();
        assert_eq!(links.lost_append_replies, 1);
        assert_eq!(links.lost_append_entry, Some((1, 2, accepted)));
        let deliveries = links.delivered_entries[&(1, 2, accepted)];
        assert!(
            deliveries >= 1,
            "the actual submitted batch reached durable recipient storage"
        );
        assert!(
            links
                .acknowledged_after_loss
                .is_some_and(|index| index >= accepted),
            "the isolated third member cannot provide quorum; member 2 confirmed the target entry after losing its first reply"
        );
        let confirmed = links.acknowledged_after_loss.unwrap();
        eprintln!(
            "lost-reply target log index={accepted}, successful data deliveries={deliveries}, later confirmed index={confirmed}"
        );
    }
    cluster.fabric.heal();
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    let state = cluster.read(1).await.view();
    assert_eq!(state.operations.len(), 3);
    assert_eq!(state.effects.len(), 3);
    assert_eq!(
        state
            .effects
            .iter()
            .map(|e| e.id.0)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    cluster.shutdown().await;
}

#[nativelink_test]
async fn interrupted_snapshot_cannot_publish_partial_state_and_new_learner_replaces_voter() {
    let mut cluster = Cluster::new().await;
    let accepted = cluster.write(1, (1..=16).map(submit)).await;
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    cluster.nodes[&1].raft.trigger().snapshot().await.unwrap();
    cluster.nodes[&1]
        .raft
        .wait(Some(WAIT))
        .metrics(
            |m| m.snapshot.is_some_and(|id| id.index >= accepted),
            "snapshot published",
        )
        .await
        .unwrap();
    cluster.nodes[&1]
        .raft
        .trigger()
        .purge_log(accepted)
        .await
        .unwrap();
    cluster.nodes[&1]
        .raft
        .wait(Some(WAIT))
        .metrics(
            |m| m.purged.is_some_and(|id| id.index >= accepted),
            "compacted source",
        )
        .await
        .unwrap();

    cluster.start(4).await;
    cluster.fabric.links.lock().interrupt_snapshot = Some((1, 4));
    cluster.nodes[&1]
        .raft
        .add_learner(4, BasicNode::default(), false)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, cluster.fabric.snapshot_interrupted.notified())
        .await
        .unwrap();
    assert_eq!(cluster.fabric.links.lock().interrupted_snapshots, 1);
    assert!(
        cluster.nodes[&4]
            .journal
            .read_applied()
            .await
            .unwrap()
            .1
            .view()
            .operations
            .is_empty()
    );

    // Restart the recipient with only a received prefix. No bootstrap occurs:
    // it remains a learner and must receive a complete valid snapshot.
    cluster.stop(4).await;
    cluster.start(4).await;
    assert!(
        !cluster.nodes[&4]
            .raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == 4)
    );
    cluster.fabric.heal();
    tokio::time::timeout(
        WAIT,
        cluster.nodes[&1]
            .raft
            .add_learner(4, BasicNode::default(), true),
    )
    .await
    .unwrap()
    .unwrap();
    let membership = tokio::time::timeout(
        WAIT,
        cluster.nodes[&1]
            .raft
            .change_membership(BTreeSet::from([1, 2, 4]), false),
    )
    .await
    .unwrap()
    .unwrap();
    cluster
        .converge(1, membership.log_id.index, &[1, 2, 4])
        .await;
    assert!(cluster.fabric.links.lock().snapshot_chunks > 1);
    cluster.stop(3).await;
    cluster.fabric.isolate(1);
    cluster.elect(4).await;
    let accepted = cluster.write(4, [submit(17)]).await;
    cluster.fabric.heal();
    cluster.converge(4, accepted, &[1, 2, 4]).await;
    assert_eq!(cluster.read(4).await.view().effects.len(), 17);
    cluster.shutdown().await;
}

#[nativelink_test]
async fn leader_loss_after_joint_commit_preserves_effects_until_successor_finishes_membership() {
    let mut cluster = Cluster::new().await;
    let accepted = cluster.write(1, [submit(1), submit(2)]).await;
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    let expected = cluster.read(1).await.snapshot().unwrap();
    cluster.start(4).await;
    cluster.nodes[&1]
        .raft
        .add_learner(4, BasicNode::default(), true)
        .await
        .unwrap();
    cluster.fabric.links.lock().withhold_uniform_from = Some(1);
    let leader = cluster.nodes[&1].raft.clone();
    let changing = tokio::spawn(async move {
        leader
            .change_membership(BTreeSet::from([1, 2, 4]), false)
            .await
    });
    tokio::time::timeout(WAIT, cluster.fabric.uniform_withheld.notified())
        .await
        .unwrap();
    let (joint_position, joint) = cluster.nodes[&1]
        .journal
        .clone()
        .applied_state()
        .await
        .unwrap();
    assert_eq!(joint.membership().get_joint_config().len(), 2);
    let joint_position = joint_position.unwrap();
    for id in [2, 3, 4] {
        cluster.nodes[&id]
            .raft
            .wait(Some(WAIT))
            .applied_index_at_least(
                Some(joint_position.index),
                "joint membership committed at recipient",
            )
            .await
            .unwrap();
        let (_, membership) = cluster.nodes[&id]
            .journal
            .clone()
            .applied_state()
            .await
            .unwrap();
        assert_eq!(membership, joint);
    }
    cluster.stop(1).await;
    assert!(
        tokio::time::timeout(WAIT, changing)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    // Survivors 2,3,4 contain a majority of BOTH configurations. The fresh
    // leader still has joint membership; the withheld final entry cannot be
    // mistaken for a committed decision from the stopped leader.
    cluster.elect(4).await;
    assert_eq!(cluster.read(4).await.snapshot().unwrap(), expected);
    let (_, membership) = cluster.nodes[&4]
        .journal
        .clone()
        .applied_state()
        .await
        .unwrap();
    assert_eq!(membership.membership().get_joint_config().len(), 2);
    let finished = cluster.nodes[&4]
        .raft
        .change_membership(BTreeSet::from([1, 2, 4]), false)
        .await
        .unwrap();
    cluster.converge(4, finished.log_id.index, &[2, 4]).await;
    let (_, membership) = cluster.nodes[&4]
        .journal
        .clone()
        .applied_state()
        .await
        .unwrap();
    assert_eq!(
        membership.membership().get_joint_config(),
        &vec![BTreeSet::from([1, 2, 4])]
    );
    let next = cluster.write(4, [submit(3)]).await;
    cluster.converge(4, next, &[2, 4]).await;
    assert_eq!(cluster.read(4).await.view().effects.len(), 3);
    cluster.fabric.links.lock().withhold_uniform_from = None;
    cluster.start(1).await;
    cluster.converge(4, next, &[1, 2, 4]).await;
    let (_, recovered_membership) = cluster.nodes[&1]
        .journal
        .clone()
        .applied_state()
        .await
        .unwrap();
    assert_eq!(recovered_membership, membership);
    cluster.shutdown().await;
}

#[nativelink_test]
async fn all_durable_members_restart_without_reinitializing_their_authority() {
    let mut cluster = Cluster::new().await;
    let accepted = cluster.write(1, [submit(1), submit(2)]).await;
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    let expected = cluster.read(1).await.snapshot().unwrap();
    for id in 1..=3 {
        cluster.stop(id).await;
    }
    for id in 1..=3 {
        cluster.start(id).await;
    }
    // A recovered durable leader may resume its committed vote. Fence it from
    // the majority before asking another member to win a fresh election.
    cluster.fabric.isolate(1);
    cluster.elect(2).await;
    assert_eq!(cluster.read(2).await.snapshot().unwrap(), expected);
    let duplicate = cluster.write(2, [submit(1), submit(2)]).await;
    cluster.fabric.heal();
    cluster.converge(2, duplicate, &[1, 2, 3]).await;
    assert_eq!(cluster.read(2).await.snapshot().unwrap(), expected);
    cluster.shutdown().await;
}

#[nativelink_test]
async fn lost_volatile_voter_rejoins_as_fresh_learner_before_promotion() {
    recover_lost_volatile_voter(false).await;
}

#[nativelink_test]
async fn interrupted_volatile_snapshot_then_recipient_loss_requires_another_fresh_learner() {
    recover_lost_volatile_voter(true).await;
}

async fn recover_lost_volatile_voter(lose_snapshot_recipient: bool) {
    let mut cluster = Cluster::with_backing(Backing::Volatile).await;
    let capacity = Resources {
        cpu_millis: 1000,
        memory_bytes: 4096,
        slots: 1,
    };
    let mut running = submit(1);
    let Event::Submit { request, .. } = &mut running.event else {
        unreachable!()
    };
    request.skip_cache_lookup = true;
    let accepted = cluster
        .write(
            1,
            [
                RecordedEvent {
                    epoch: EPOCH,
                    event: Event::Register {
                        worker: WorkerId(1),
                        incarnation: IncarnationId(1),
                        session: SessionId(1),
                        properties: BTreeMap::new(),
                        capacity,
                        available: capacity,
                    },
                },
                running,
                RecordedEvent {
                    epoch: EPOCH,
                    event: Event::Match,
                },
                submit(2),
            ],
        )
        .await;
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    let before = cluster.read(1).await.snapshot().unwrap();
    cluster.stop(3).await;
    assert_eq!(
        cluster.try_start(3).await,
        Err("volatile voting history was lost; a fresh member identity is required")
    );
    cluster.compact(1, accepted).await;
    let suffix = cluster.write(1, [submit(3)]).await;

    cluster.start(4).await;
    cluster.assert_empty_learner(4).await;
    cluster.nodes[&4].raft.trigger().elect().await.unwrap();
    assert!(cluster.nodes[&4].raft.ensure_linearizable().await.is_err());
    let replacement = if lose_snapshot_recipient {
        cluster.fabric.links.lock().interrupt_snapshot = Some((1, 4));
        cluster.nodes[&1]
            .raft
            .add_learner(4, BasicNode::default(), false)
            .await
            .unwrap();
        tokio::time::timeout(WAIT, cluster.fabric.snapshot_interrupted.notified())
            .await
            .unwrap();
        assert_eq!(cluster.fabric.links.lock().interrupted_snapshots, 1);
        let (applied, state) = cluster.nodes[&4].journal.read_applied().await.unwrap();
        assert!(applied.is_none());
        assert!(state.view().operations.is_empty());
        assert!(state.view().effects.is_empty());
        assert!(state.view().reservations.is_empty());
        // A received prefix can already have voting history. Losing even this
        // learner's RAM requires another identity, never restart under ID 4.
        cluster.stop(4).await;
        assert!(cluster.try_start(4).await.is_err());
        cluster.start(5).await;
        cluster.assert_empty_learner(5).await;
        cluster.fabric.heal();
        5
    } else {
        4
    };
    // The surviving quorum, not the empty member, admits a new learner. The
    // compacted dispatch and suffix lookup must both arrive before promotion.
    tokio::time::timeout(
        WAIT,
        cluster.nodes[&1]
            .raft
            .add_learner(replacement, BasicNode::default(), true),
    )
    .await
    .unwrap()
    .unwrap();
    cluster.converge(1, suffix, &[1, 2, replacement]).await;
    {
        let metrics = cluster.nodes[&replacement].raft.metrics();
        let metrics = metrics.borrow();
        assert!(
            !metrics
                .membership_config
                .membership()
                .voter_ids()
                .any(|id| id == replacement)
        );
    }
    assert!(
        cluster.nodes[&replacement]
            .raft
            .ensure_linearizable()
            .await
            .is_err()
    );
    let restored = cluster.nodes[&replacement]
        .journal
        .read_applied()
        .await
        .unwrap()
        .1
        .view();
    assert_eq!(restored.operations.len(), 3);
    assert_eq!(restored.effects.len(), 3);
    assert_eq!(restored.reservations.len(), 1);
    assert!(!restored.reservations[0].released);
    assert_eq!(restored.workers[0].charged.slots, 1);
    assert_ne!(cluster.read(1).await.snapshot().unwrap(), before);
    assert!(cluster.fabric.links.lock().snapshot_chunks > 1);

    let promotion = cluster.nodes[&1]
        .raft
        .change_membership(BTreeSet::from([1, 2, replacement]), false)
        .await
        .unwrap();
    cluster
        .converge(1, promotion.log_id.index, &[1, 2, replacement])
        .await;
    cluster.fabric.isolate(1);
    cluster.elect(replacement).await;
    let committed = cluster.write(replacement, [submit(4)]).await;
    cluster.fabric.heal();
    cluster
        .converge(replacement, committed, &[1, 2, replacement])
        .await;
    let state = cluster.read(replacement).await.view();
    assert_eq!(state.operations.len(), 4);
    assert_eq!(state.effects.len(), 4);
    assert_eq!(state.reservations.len(), 1);
    assert!(!state.reservations[0].released);
    assert_eq!(state.workers[0].charged.slots, 1);
    assert!(
        std::fs::read_dir(&cluster.directory)
            .unwrap()
            .next()
            .is_none()
    );
    cluster.shutdown().await;
}

#[nativelink_test]
async fn lost_volatile_quorum_and_total_loss_do_not_bootstrap_new_authority() {
    let mut cluster = Cluster::with_backing(Backing::Volatile).await;
    let accepted = cluster.write(1, [submit(1)]).await;
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    let before = cluster.nodes[&1]
        .journal
        .read_applied()
        .await
        .unwrap()
        .1
        .snapshot()
        .unwrap();
    cluster.stop(2).await;
    cluster.stop(3).await;
    cluster.start(4).await;
    cluster.assert_empty_learner(4).await;
    assert!(cluster.nodes[&1].raft.ensure_linearizable().await.is_err());
    let mut pending = cluster.nodes[&1]
        .raft
        .client_write_ff(Batch::new([submit(2)]).unwrap())
        .await
        .unwrap();
    cluster.nodes[&1]
        .raft
        .wait(Some(WAIT))
        .log_index_at_least(Some(accepted + 1), "minority proposed entry")
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(
        cluster.nodes[&1]
            .journal
            .read_applied()
            .await
            .unwrap()
            .1
            .snapshot()
            .unwrap(),
        before
    );
    cluster.stop(1).await;
    // All voting histories are now gone. Fresh IDs have no right to initialize
    // another authority for these workers or storage. No initialize() is called.
    cluster.start(5).await;
    cluster.start(6).await;
    for id in 1..=3 {
        assert!(cluster.try_start(id).await.is_err());
    }
    for id in 4..=6 {
        cluster.nodes[&id].raft.trigger().elect().await.unwrap();
        cluster.assert_empty_learner(id).await;
    }
    assert!(
        std::fs::read_dir(&cluster.directory)
            .unwrap()
            .next()
            .is_none()
    );
    cluster.shutdown().await;
}

async fn consensus_facts(node: &Node) -> Vec<u8> {
    let vote = node.journal.clone().read_vote().await.unwrap();
    let log = node.journal.clone().get_log_state().await.unwrap();
    let applied = node.journal.read_applied().await.unwrap();
    let metrics = node.raft.metrics().borrow().clone();
    serde_json::to_vec(&(
        vote,
        log.last_purged_log_id,
        log.last_log_id,
        applied.0,
        applied.1.snapshot().unwrap(),
        metrics.current_term,
        metrics.current_leader,
        metrics.membership_config,
        metrics.snapshot,
        metrics.purged,
    ))
    .unwrap()
}

#[nativelink_test]
async fn foreign_domain_votes_heartbeats_and_snapshot_fragments_cannot_change_consensus() {
    let cluster = Cluster::new().await;
    let accepted = cluster.write(1, [submit(1)]).await;
    cluster.converge(1, accepted, &[1, 2, 3]).await;
    // Represent endpoint replacement by an isolated foreign execution domain.
    // This does not authorize resetting a real domain whose old workers exist.
    cluster.fabric.links.lock().peers.get_mut(&3).unwrap().epoch = ClusterEpoch(EPOCH.0 + 1);
    let original = consensus_facts(&cluster.nodes[&3]).await;
    let mut network = Network {
        from: 1,
        to: 3,
        epoch: EPOCH,
        fabric: cluster.fabric.clone(),
    };
    assert!(matches!(
        network
            .vote(
                VoteRequest {
                    vote: Vote::new(100, 1),
                    last_log_id: None
                },
                RPCOption::new(WAIT)
            )
            .await,
        Err(RPCError::Unreachable(_))
    ));
    assert!(matches!(
        network
            .append_entries(
                AppendEntriesRequest {
                    vote: Vote::new_committed(100, 1),
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                },
                RPCOption::new(WAIT),
            )
            .await,
        Err(RPCError::Unreachable(_))
    ));
    for (offset, done) in [(0, false), (1, false), (2, true)] {
        assert!(matches!(
            network
                .install_snapshot(
                    InstallSnapshotRequest {
                        vote: Vote::new_committed(100, 1),
                        meta: SnapshotMeta {
                            last_log_id: None,
                            last_membership: StoredMembership::default(),
                            snapshot_id: "foreign-authority".to_owned(),
                        },
                        offset,
                        data: vec![1],
                        done,
                    },
                    RPCOption::new(WAIT),
                )
                .await,
            Err(RPCError::Unreachable(_))
        ));
    }
    assert_eq!(consensus_facts(&cluster.nodes[&3]).await, original);
    assert_eq!(cluster.fabric.links.lock().snapshot_chunks, 0);
    // The same receiver must remain usable after every rejected foreign chunk.
    cluster.fabric.links.lock().peers.get_mut(&3).unwrap().epoch = EPOCH;
    cluster.fabric.isolate(3);
    let next = cluster.write(1, (2..=8).map(submit)).await;
    cluster.compact(1, next).await;
    cluster.fabric.heal();
    cluster.converge(1, next, &[1, 2, 3]).await;
    assert!(cluster.fabric.links.lock().snapshot_chunks > 1);
    assert_eq!(cluster.read(1).await.view().effects.len(), 8);
    cluster.shutdown().await;
}

#[derive(Debug, Eq, PartialEq)]
enum ProbeFailure {
    Unreachable,
    RemoteStopped,
}

fn classify_reply<T, E: std::error::Error>(
    reply: Result<T, RPCError<u64, BasicNode, RaftError<u64, E>>>,
) -> Result<(), ProbeFailure> {
    match reply {
        Ok(_) => Ok(()),
        Err(RPCError::Unreachable(_)) => Err(ProbeFailure::Unreachable),
        Err(RPCError::RemoteError(error))
            if matches!(error.source, RaftError::Fatal(Fatal::Stopped)) =>
        {
            Err(ProbeFailure::RemoteStopped)
        }
        Err(error) => panic!("unexpected probe response: {error}"),
    }
}

async fn probe_reply(network: &mut Network, kind: RpcKind) -> Result<(), ProbeFailure> {
    match kind {
        RpcKind::Append => classify_reply(
            network
                .append_entries(
                    AppendEntriesRequest {
                        vote: Vote::new_committed(0, 2),
                        prev_log_id: None,
                        entries: Vec::new(),
                        leader_commit: None,
                    },
                    RPCOption::new(WAIT),
                )
                .await,
        ),
        RpcKind::Snapshot => classify_reply(
            network
                .install_snapshot(
                    InstallSnapshotRequest {
                        vote: Vote::new_committed(0, 2),
                        meta: SnapshotMeta {
                            last_log_id: None,
                            last_membership: StoredMembership::default(),
                            snapshot_id: "delayed-response".to_owned(),
                        },
                        offset: 0,
                        data: Vec::new(),
                        done: false,
                    },
                    RPCOption::new(WAIT),
                )
                .await,
        ),
        RpcKind::Vote => classify_reply(
            network
                .vote(
                    VoteRequest {
                        vote: Vote::new(0, 2),
                        last_log_id: None,
                    },
                    RPCOption::new(WAIT),
                )
                .await,
        ),
    }
}

#[nativelink_test]
async fn delayed_success_and_remote_error_responses_require_both_endpoints_in_the_same_domain() {
    let cluster = Cluster::new().await;
    let network = Network {
        from: 2,
        to: 3,
        epoch: EPOCH,
        fabric: cluster.fabric.clone(),
    };
    for stopped in [false, true] {
        if stopped {
            // Preserve the transport endpoint so all three real Raft calls
            // return Fatal::Stopped. This is a remote error, not a failed lookup.
            cluster.nodes[&3].raft.shutdown().await.unwrap();
        }
        for kind in [RpcKind::Append, RpcKind::Snapshot, RpcKind::Vote] {
            // Controls prove that valid same-domain responses still return,
            // including remote errors; fencing cannot mask them unconditionally.
            assert_eq!(
                probe_reply(&mut network.clone(), kind).await,
                if stopped {
                    Err(ProbeFailure::RemoteStopped)
                } else {
                    Ok(())
                },
                "same-domain response for {kind:?}, stopped={stopped}"
            );
            for replaced in [2, 3] {
                cluster.fabric.links.lock().hold_reply = Some((2, 3, kind));
                let mut request = network.clone();
                let response = tokio::spawn(async move { probe_reply(&mut request, kind).await });
                tokio::time::timeout(WAIT, cluster.fabric.reply_held.notified())
                    .await
                    .unwrap();
                cluster
                    .fabric
                    .links
                    .lock()
                    .peers
                    .get_mut(&replaced)
                    .unwrap()
                    .epoch = ClusterEpoch(EPOCH.0 + 1);
                cluster.fabric.release_reply.notify_one();
                assert_eq!(
                    tokio::time::timeout(WAIT, response).await.unwrap().unwrap(),
                    Err(ProbeFailure::Unreachable),
                    "stale response for {kind:?}, stopped={stopped}, changed endpoint={replaced}"
                );
                cluster
                    .fabric
                    .links
                    .lock()
                    .peers
                    .get_mut(&replaced)
                    .unwrap()
                    .epoch = EPOCH;
            }
        }
    }
    cluster.shutdown().await;
}
