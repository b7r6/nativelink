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

//! Real replicated `OpenRaft` and durable journals with a bounded serialized
//! in-process fault transport. This does not qualify deployed peer networking,
//! volatile storage, or a production membership reconciler.

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
    CheckIsLeaderError, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, EntryPayload, Raft, RaftNetwork, RaftNetworkFactory, SnapshotPolicy};
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
    peers: BTreeMap<u64, Raft<TypeConfig>>,
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
}

#[derive(Clone, Default)]
struct Fabric {
    links: Arc<Mutex<Links>>,
    snapshot_interrupted: Arc<Notify>,
}

impl Fabric {
    fn peer(&self, from: u64, to: u64) -> Result<Raft<TypeConfig>, &'static str> {
        let links = self.links.lock();
        if links.blocked.contains(&(from, to)) {
            return Err("scheduled directed partition");
        }
        links.peers.get(&to).cloned().ok_or("peer stopped")
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
        let peer = self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        assert!(rpc.entries.len() <= MAX_APPEND_ENTRIES);
        self.fabric.links.lock().append_entries += rpc.entries.len();
        let response = peer
            .append_entries(self.fabric.encode(&rpc))
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
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
        let peer = self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        assert!(rpc.data.len() <= 512);
        let partial = !rpc.done;
        self.fabric.links.lock().snapshot_chunks += 1;
        let response = peer
            .install_snapshot(self.fabric.encode(&rpc))
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
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
        let peer = self.fabric.peer(self.from, self.to).map_err(unreachable)?;
        let response = peer
            .vote(self.fabric.encode(&rpc))
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.to, e)))?;
        Ok(self.fabric.encode(&response))
    }
}

struct Node {
    raft: Raft<TypeConfig>,
    journal: Journal,
}

struct Cluster {
    directory: PathBuf,
    fabric: Fabric,
    nodes: BTreeMap<u64, Node>,
}

impl Cluster {
    async fn new() -> Self {
        let directory = std::env::temp_dir().join(format!(
            "nativelink-authority-cluster-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let mut cluster = Self {
            directory,
            fabric: Fabric::default(),
            nodes: BTreeMap::new(),
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
        assert!(!self.nodes.contains_key(&id));
        let journal = Journal::open(
            self.directory.join(format!("node-{id}.redb")),
            id,
            EPOCH,
            Limits::default(),
        )
        .await
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
                fabric: self.fabric.clone(),
            },
            journal.clone(),
            journal.clone(),
        )
        .await
        .unwrap();
        self.fabric.links.lock().peers.insert(id, raft.clone());
        self.nodes.insert(id, Node { raft, journal });
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
