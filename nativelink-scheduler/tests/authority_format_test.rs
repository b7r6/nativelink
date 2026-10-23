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

//! Experimental format profiles exercise real journal and Raft boundaries.
//! The admission registry is an explicit trusted in-process fixture.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::format::{
    AdmissionRegistry, AuthorityDomain, DIRECT_SCHEDULER, FormatControl, JournalProfile,
    MAX_FEATURES, MAX_FORMAT_MEMBERS, MemberEvidence, Preparation, ReaderCapabilities,
};
use nativelink_scheduler::authority::journal::{
    Batch, Journal, JournalFault, Singleton, TypeConfig,
};
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, EffectKind, Event, IncarnationId, Limits, OperationId, RecordedEvent,
    Reply, Request, Resources, ResultPayload, SessionId, WorkerId,
};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, Raft, RaftLogReader, RaftNetwork, RaftNetworkFactory,
    RaftSnapshotBuilder, SnapshotPolicy,
};
use parking_lot::Mutex;
use redb::{ReadableDatabase, ReadableTable};

const DOMAIN: AuthorityDomain = AuthorityDomain {
    epoch: ClusterEpoch(97),
    namespace: 19,
};
const NODE: u64 = 1;
const WAIT: Duration = Duration::from_secs(15);

#[derive(Default)]
struct FormatLinks {
    peers: BTreeMap<u64, (u64, Raft<TypeConfig>)>,
    blocked: BTreeSet<(u64, u64)>,
}

/// Transport identities and profile capabilities are trusted fixture facts.
/// This is real Raft replication, without a deployed authentication claim.
#[derive(Clone)]
struct FormatNetwork {
    from: u64,
    to: u64,
    links: Arc<Mutex<FormatLinks>>,
}

fn unavailable<E: std::error::Error>(message: &str) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        message,
    )))
}

impl FormatNetwork {
    fn peer(&self) -> Result<(u64, u64, Raft<TypeConfig>), &'static str> {
        let links = self.links.lock();
        if links.blocked.contains(&(self.from, self.to)) {
            return Err("format fixture partition");
        }
        let from = links.peers.get(&self.from).ok_or("sender stopped")?;
        let to = links.peers.get(&self.to).ok_or("receiver stopped")?;
        Ok((from.0, to.0, to.1.clone()))
    }

    fn response(&self, from: u64, to: u64) -> Result<(), &'static str> {
        let (current_from, current_to, _) = self.peer()?;
        if (from, to) != (current_from, current_to) {
            return Err("peer incarnation changed in flight");
        }
        Ok(())
    }
}

impl RaftNetworkFactory<TypeConfig> for FormatNetwork {
    type Network = Self;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self {
        Self {
            to: target,
            ..self.clone()
        }
    }
}

impl RaftNetwork<TypeConfig> for FormatNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let (from, to, peer) = self.peer().map_err(unavailable)?;
        let response = peer.append_entries(rpc).await;
        self.response(from, to).map_err(unavailable)?;
        response.map_err(|error| RPCError::RemoteError(RemoteError::new(self.to, error)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let (from, to, peer) = self.peer().map_err(unavailable)?;
        let response = peer.install_snapshot(rpc).await;
        self.response(from, to).map_err(unavailable)?;
        response.map_err(|error| RPCError::RemoteError(RemoteError::new(self.to, error)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let (from, to, peer) = self.peer().map_err(unavailable)?;
        let response = peer.vote(rpc).await;
        self.response(from, to).map_err(unavailable)?;
        response.map_err(|error| RPCError::RemoteError(RemoteError::new(self.to, error)))
    }
}

struct FormatNode {
    raft: Raft<TypeConfig>,
    journal: Journal,
}

struct FormatCluster {
    directory: Directory,
    registry: Arc<AdmissionRegistry>,
    links: Arc<Mutex<FormatLinks>>,
    nodes: BTreeMap<u64, FormatNode>,
}

impl FormatCluster {
    async fn new(last_reader: ReaderCapabilities) -> Self {
        Self::with_voters(last_reader, BTreeSet::from([1, 2, 3])).await
    }

    async fn with_voters(last_reader: ReaderCapabilities, voters: BTreeSet<u64>) -> Self {
        let mut cluster = Self {
            directory: Directory::new(),
            registry: AdmissionRegistry::new(DOMAIN),
            links: Arc::default(),
            nodes: BTreeMap::new(),
        };
        for id in 1..=3 {
            cluster
                .start(
                    id,
                    if id == 3 {
                        last_reader
                    } else {
                        ReaderCapabilities::DirectScheduler
                    },
                    1,
                )
                .await;
        }
        cluster.nodes[&1]
            .raft
            .initialize(BTreeSet::from([1]))
            .await
            .unwrap();
        cluster.leader(1).await;
        for id in 2..=3 {
            cluster.nodes[&1]
                .raft
                .add_learner(id, BasicNode::default(), true)
                .await
                .unwrap();
        }
        cluster.nodes[&1]
            .raft
            .change_membership(voters, true)
            .await
            .unwrap();
        cluster
    }

    async fn start(&mut self, id: u64, reader: ReaderCapabilities, incarnation: u64) {
        let journal = Journal::open_with_profile(
            self.directory.file(&format!("member-{id}")),
            id,
            Limits::default(),
            profile(&self.registry, reader, incarnation),
        )
        .await
        .unwrap();
        self.connect_journal(id, incarnation, journal).await;
    }

    async fn connect_journal(&mut self, id: u64, incarnation: u64, journal: Journal) {
        let config = openraft::Config {
            cluster_name: "categorical-format-test".into(),
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            enable_elect: false,
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()
        .unwrap();
        let raft = Raft::new(
            id,
            Arc::new(config),
            FormatNetwork {
                from: id,
                to: id,
                links: self.links.clone(),
            },
            journal.clone(),
            journal.clone(),
        )
        .await
        .unwrap();
        self.links
            .lock()
            .peers
            .insert(id, (incarnation, raft.clone()));
        assert!(
            self.nodes
                .insert(id, FormatNode { raft, journal })
                .is_none()
        );
    }

    async fn leader(&self, id: u64) {
        self.nodes[&id]
            .raft
            .wait(Some(WAIT))
            .current_leader(id, "format leader")
            .await
            .unwrap();
        tokio::time::timeout(WAIT, async {
            loop {
                if self.nodes[&id].raft.ensure_linearizable().await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn isolate(&self, id: u64) {
        let mut links = self.links.lock();
        for other in 1..=3 {
            if id != other {
                links.blocked.insert((id, other));
                links.blocked.insert((other, id));
            }
        }
    }

    async fn stop(&mut self, id: u64) {
        self.links.lock().peers.remove(&id);
        let node = self.nodes.remove(&id).unwrap();
        node.raft.shutdown().await.unwrap();
        drop(node);
    }

    async fn shutdown(mut self) {
        let ids: Vec<_> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("authority-format-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _removed = std::fs::remove_dir_all(&self.0);
    }
}

fn profile(
    registry: &Arc<AdmissionRegistry>,
    reader: ReaderCapabilities,
    incarnation: u64,
) -> JournalProfile {
    JournalProfile {
        domain: DOMAIN,
        incarnation,
        reader,
        admission: registry.clone(),
    }
}

fn submit(operation: u64) -> RecordedEvent {
    RecordedEvent {
        epoch: DOMAIN.epoch,
        event: Event::Submit {
            operation: OperationId(operation),
            request: Request {
                action: ActionKey {
                    instance: "format-test".into(),
                    digest_function: "sha256".into(),
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

const fn entry(position: LogId<u64>, batch: Batch) -> Entry<TypeConfig> {
    Entry {
        log_id: position,
        payload: EntryPayload::Normal(batch),
    }
}

fn next(position: LogId<u64>) -> LogId<u64> {
    LogId::new(position.leader_id, position.index + 1)
}

fn metadata(path: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
    let db = redb::Database::open(path).unwrap();
    let tx = db.begin_read().unwrap();
    tx.open_table(redb::TableDefinition::<&str, &[u8]>::new("authority_meta"))
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_owned(), value.value().to_vec())
        })
        .collect()
}

#[nativelink_test]
async fn upgraded_reader_writes_identical_base_artifacts_before_activation() {
    let directory = Directory::new();
    let mut outputs = Vec::new();
    for (name, reader) in [
        ("base", ReaderCapabilities::Base),
        ("upgraded", ReaderCapabilities::DirectScheduler),
    ] {
        let registry = AdmissionRegistry::new(DOMAIN);
        let mut journal = Journal::open_with_profile(
            directory.file(name),
            NODE,
            Limits::default(),
            profile(&registry, reader, 1),
        )
        .await
        .unwrap();
        let position = LogId::new(openraft::CommittedLeaderId::new(1, NODE), 0);
        let command = entry(position, Batch::new([submit(1)]).unwrap());
        journal.blocking_append([command.clone()]).await.unwrap();
        journal.save_committed(Some(position)).await.unwrap();
        journal.apply([command]).await.unwrap();
        let first = journal.build_snapshot().await.unwrap();
        journal.purge(position).await.unwrap();
        let second = journal.build_snapshot().await.unwrap();
        assert_ne!(first.meta.snapshot_id, second.meta.snapshot_id);
        let expected = journal.read_applied().await.unwrap();
        drop(journal);
        let before = metadata(&directory.file(name));
        let mut journal = Journal::open_with_profile(
            directory.file(name),
            NODE,
            Limits::default(),
            profile(&registry, ReaderCapabilities::Base, 2),
        )
        .await
        .unwrap();
        assert_eq!(journal.read_applied().await.unwrap(), expected);
        assert_eq!(
            *journal
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .snapshot,
            *second.snapshot
        );
        journal.build_snapshot().await.unwrap();
        drop(journal);
        outputs.push((before, metadata(&directory.file(name))));
    }
    assert_eq!(
        outputs[0], outputs[1],
        "capable executable alone must not upgrade any persisted bytes"
    );
}

#[nativelink_test]
async fn real_singleton_activation_preserves_pending_authority_and_refuses_old_reader() {
    let directory = Directory::new();
    let registry = AdmissionRegistry::new(DOMAIN);
    let node = Singleton::open_with_profile(
        directory.file("journal"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 1),
    )
    .await
    .unwrap();
    assert!(matches!(
        node.write(submit(1)).await.unwrap().data,
        Reply::Submitted { .. }
    ));
    let before = node.read().await.unwrap();
    assert_eq!(before.view().effects.len(), 1);
    node.journal
        .activate_features(&node.raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await
        .unwrap();
    assert_eq!(node.journal.active_format().await.unwrap().feature_epoch, 1);
    assert_eq!(node.read().await.unwrap(), before);
    let snapshot = node.journal.clone().build_snapshot().await.unwrap();
    let wire: serde_json::Value = serde_json::from_slice(snapshot.snapshot.get_ref()).unwrap();
    let payload: serde_json::Value =
        serde_json::from_str(wire["payload"].as_str().unwrap()).unwrap();
    assert!(payload["applied"]["scheduler"].is_object());
    assert_eq!(
        payload["applied"]["format"]["required"],
        serde_json::json!([DIRECT_SCHEDULER])
    );
    node.write(submit(2)).await.unwrap();
    let expected = node.read().await.unwrap();
    node.shutdown().await.unwrap();
    assert!(
        Journal::open_with_profile(
            directory.file("journal"),
            NODE,
            Limits::default(),
            profile(&registry, ReaderCapabilities::Base, 2)
        )
        .await
        .is_err()
    );
    let node = Singleton::open_with_profile(
        directory.file("journal"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 2),
    )
    .await
    .unwrap();
    assert_eq!(node.read().await.unwrap(), expected);
    assert_eq!(expected.view().effects.len(), 2);
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn committed_activation_suffix_blocks_old_startup_before_apply_and_replays_from_old_snapshot()
{
    let directory = Directory::new();
    let registry = AdmissionRegistry::new(DOMAIN);
    let node = Singleton::open_with_profile(
        directory.file("source"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 1),
    )
    .await
    .unwrap();
    node.write(submit(1)).await.unwrap();
    let old_snapshot = node.journal.clone().build_snapshot().await.unwrap();
    let prepared = node
        .journal
        .prepare_features(&node.raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await
        .unwrap();
    let response = node
        .raft
        .client_write(Batch::format(FormatControl::Prepare(prepared.clone())).unwrap())
        .await
        .unwrap();
    let position = next(response.log_id);
    let mut journal = node.journal.clone();
    node.shutdown().await.unwrap();
    let activation = entry(
        position,
        Batch::format(FormatControl::Activate(prepared)).unwrap(),
    );
    journal.blocking_append([activation.clone()]).await.unwrap();
    // The upgraded writer has appended an activation but has not committed it.
    // A copied artifact is decodable by the base profile. That does not
    // authorize joining the unresolved activation as an unsupported voter.
    drop(journal);
    drop(registry);
    std::fs::copy(directory.file("source"), directory.file("uncommitted-copy")).unwrap();
    let offline = Journal::recover_admission(
        DOMAIN,
        ReaderCapabilities::Base,
        vec![(NODE, directory.file("uncommitted-copy"))],
    )
    .await
    .unwrap();
    let old = Journal::open_with_profile(
        directory.file("uncommitted-copy"),
        NODE,
        Limits::default(),
        profile(&offline, ReaderCapabilities::Base, 2),
    )
    .await;
    assert!(
        old.is_err(),
        "artifact decoding does not permit an unsupported voter to rejoin unresolved activation"
    );
    // Every prior process has stopped. Reconstruct the trusted fixture from
    // closed artifacts, then admit a later capable incarnation.
    let recovered_registry = Journal::recover_admission(
        DOMAIN,
        ReaderCapabilities::DirectScheduler,
        vec![(NODE, directory.file("source"))],
    )
    .await
    .unwrap();
    let mut journal = Journal::open_with_profile(
        directory.file("source"),
        NODE,
        Limits::default(),
        profile(&recovered_registry, ReaderCapabilities::DirectScheduler, 2),
    )
    .await
    .unwrap();
    journal.save_committed(Some(position)).await.unwrap();
    assert_eq!(
        journal.active_format().await.unwrap().feature_epoch,
        0,
        "commit precedes apply"
    );
    drop(journal);
    assert!(
        Journal::open_with_profile(
            directory.file("source"),
            NODE,
            Limits::default(),
            profile(&recovered_registry, ReaderCapabilities::Base, 3)
        )
        .await
        .is_err()
    );
    let mut journal = Journal::open_with_profile(
        directory.file("source"),
        NODE,
        Limits::default(),
        profile(&recovered_registry, ReaderCapabilities::DirectScheduler, 3),
    )
    .await
    .unwrap();
    journal.apply([activation]).await.unwrap();
    let expected = journal.read_applied().await.unwrap();
    let suffix = journal
        .try_get_log_entries((old_snapshot.meta.last_log_id.unwrap().index + 1)..=position.index)
        .await
        .unwrap();
    let target_registry = AdmissionRegistry::new(DOMAIN);
    let mut target = Journal::open_with_profile(
        directory.file("target"),
        NODE + 1,
        Limits::default(),
        profile(&target_registry, ReaderCapabilities::DirectScheduler, 1),
    )
    .await
    .unwrap();
    target
        .install_snapshot(&old_snapshot.meta, old_snapshot.snapshot)
        .await
        .unwrap();
    target.apply(suffix).await.unwrap();
    assert_eq!(target.read_applied().await.unwrap(), expected);
    assert_eq!(
        target.active_format().await.unwrap(),
        journal.active_format().await.unwrap()
    );
}

#[nativelink_test]
async fn activation_and_snapshot_replacement_are_atomic_at_commit_cutpoints() {
    let directory = Directory::new();
    let registry = AdmissionRegistry::new(DOMAIN);
    let node = Singleton::open_with_profile(
        directory.file("journal"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 1),
    )
    .await
    .unwrap();
    node.write(submit(1)).await.unwrap();
    let prepared = node
        .journal
        .prepare_features(&node.raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await
        .unwrap();
    let response = node
        .raft
        .client_write(Batch::format(FormatControl::Prepare(prepared.clone())).unwrap())
        .await
        .unwrap();
    let activation = entry(
        next(response.log_id),
        Batch::format(FormatControl::Activate(prepared)).unwrap(),
    );
    let mut journal = node.journal.clone();
    node.shutdown().await.unwrap();
    let old = journal.read_applied().await.unwrap();
    let old_snapshot = journal.build_snapshot().await.unwrap();
    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.apply([activation.clone()]).await.is_err());
    assert_eq!(journal.active_format().await.unwrap().feature_epoch, 0);
    assert_eq!(journal.read_applied().await.unwrap(), old);
    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.apply([activation]).await.is_err());
    assert_eq!(journal.active_format().await.unwrap().feature_epoch, 1);
    assert_eq!(journal.read_applied().await.unwrap().1, old.1);
    journal.inject_failure_once(JournalFault::BeforeCommit);
    assert!(journal.build_snapshot().await.is_err());
    assert_eq!(
        journal.get_current_snapshot().await.unwrap().unwrap().meta,
        old_snapshot.meta
    );
    journal.inject_failure_once(JournalFault::AfterCommit);
    assert!(journal.build_snapshot().await.is_err());
    let installed = journal.get_current_snapshot().await.unwrap().unwrap();
    assert_ne!(installed.meta.snapshot_id, old_snapshot.meta.snapshot_id);
    drop(journal);
    let mut journal = Journal::open_with_profile(
        directory.file("journal"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 2),
    )
    .await
    .unwrap();
    assert_eq!(journal.active_format().await.unwrap().feature_epoch, 1);
    assert_eq!(
        journal.get_current_snapshot().await.unwrap().unwrap().meta,
        installed.meta
    );
    assert_eq!(journal.read_applied().await.unwrap().1, old.1);
}

#[nativelink_test]
async fn actual_membership_blocks_old_reader_then_activates_and_catches_up_lagging_member() {
    for voters in [BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2])] {
        let mut cluster = FormatCluster::with_voters(ReaderCapabilities::Base, voters).await;
        let first = cluster.nodes[&1]
            .raft
            .client_write(Batch::new([submit(1)]).unwrap())
            .await
            .unwrap();
        assert!(
            cluster.nodes[&1]
                .journal
                .activate_features(&cluster.nodes[&1].raft, BTreeSet::from([DIRECT_SCHEDULER]),)
                .await
                .unwrap_err()
                .contains("required member cannot read")
        );
        assert_eq!(
            cluster.nodes[&1]
                .journal
                .active_format()
                .await
                .unwrap()
                .feature_epoch,
            0
        );
        cluster.nodes[&3]
            .raft
            .wait(Some(WAIT))
            .applied_index_at_least(Some(first.log_id.index), "base member replicated")
            .await
            .unwrap();
        cluster.stop(3).await;
        cluster
            .start(3, ReaderCapabilities::DirectScheduler, 2)
            .await;
        // The replacement is admitted from the actual executable profile; its
        // absence from the activation quorum does not omit it from the evidence.
        cluster.isolate(3);
        cluster.nodes[&1]
            .journal
            .activate_features(&cluster.nodes[&1].raft, BTreeSet::from([DIRECT_SCHEDULER]))
            .await
            .unwrap();
        let activated = cluster.nodes[&1].journal.active_format().await.unwrap();
        assert_eq!(activated.feature_epoch, 1);
        assert_eq!(
            cluster.nodes[&3]
                .journal
                .active_format()
                .await
                .unwrap()
                .feature_epoch,
            0
        );
        let write = cluster.nodes[&1]
            .raft
            .client_write(Batch::new([submit(2)]).unwrap().at_feature_epoch(1))
            .await
            .unwrap();
        assert!(
            Journal::fresh_volatile_with_profile(
                4,
                Limits::default(),
                profile(&cluster.registry, ReaderCapabilities::Base, 1)
            )
            .await
            .is_err()
        );
        cluster.links.lock().blocked.clear();
        for id in 1..=3 {
            cluster.nodes[&id]
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(Some(write.log_id.index), "lagging format replay")
                .await
                .unwrap();
            assert_eq!(
                cluster.nodes[&id].journal.active_format().await.unwrap(),
                activated
            );
            assert_eq!(
                cluster.nodes[&id].journal.read_applied().await.unwrap().1,
                cluster.nodes[&1].journal.read_applied().await.unwrap().1
            );
        }
        cluster.shutdown().await;
    }
}

#[nativelink_test]
async fn paused_leader_cannot_release_admission_or_reuse_an_aborted_activation() {
    let mut cluster = FormatCluster::new(ReaderCapabilities::DirectScheduler).await;
    let preparation = cluster.nodes[&1]
        .journal
        .prepare_features(&cluster.nodes[&1].raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await
        .unwrap();
    let prepared = cluster.nodes[&1]
        .raft
        .client_write(Batch::format(FormatControl::Prepare(preparation.clone())).unwrap())
        .await
        .unwrap();
    for id in 2..=3 {
        cluster.nodes[&id]
            .raft
            .wait(Some(WAIT))
            .applied_index_at_least(Some(prepared.log_id.index), "prepared on quorum peers")
            .await
            .unwrap();
    }
    cluster.isolate(1);
    // Enqueue a real old-leader activation, then abandon the client response.
    // Admission remains pinned even though no caller still waits for it.
    let _pending_reply = cluster.nodes[&1]
        .raft
        .client_write_ff(Batch::format(FormatControl::Activate(preparation.clone())).unwrap())
        .await
        .unwrap();
    assert!(
        Journal::fresh_volatile_with_profile(
            4,
            Limits::default(),
            profile(&cluster.registry, ReaderCapabilities::DirectScheduler, 1)
        )
        .await
        .is_err()
    );
    cluster.nodes[&2].raft.runtime_config().elect(true);
    cluster.nodes[&2].raft.trigger().elect().await.unwrap();
    cluster.leader(2).await;
    cluster.nodes[&2].raft.runtime_config().elect(false);
    cluster.nodes[&2]
        .journal
        .abort_preparation(&cluster.nodes[&2].raft, preparation.permit)
        .await
        .unwrap();
    let admitted = Journal::fresh_volatile_with_profile(
        4,
        Limits::default(),
        profile(&cluster.registry, ReaderCapabilities::Base, 1),
    )
    .await
    .unwrap();
    let position = next(
        cluster.nodes[&2]
            .journal
            .read_applied()
            .await
            .unwrap()
            .0
            .unwrap(),
    );
    // A stale control shifted to a fresh index is refused at the common
    // persistence boundary, before a journal transaction changes the log.
    let mut journal = cluster.nodes[&2].journal.clone();
    assert!(
        journal
            .blocking_append([entry(
                position,
                Batch::format(FormatControl::Activate(preparation.clone())).unwrap()
            )])
            .await
            .is_err()
    );
    assert!(
        journal
            .blocking_append([entry(
                position,
                Batch::format(FormatControl::Prepare(preparation)).unwrap()
            )])
            .await
            .is_err()
    );
    drop(admitted);
    // Only the consensus-committed abort permits a Base replacement to reopen
    // the old leader's preserved, uncommitted activation tail.
    cluster.stop(1).await;
    cluster.start(1, ReaderCapabilities::Base, 2).await;
    cluster.links.lock().blocked.clear();
    cluster.nodes[&1]
        .raft
        .wait(Some(WAIT))
        .applied_index_at_least(Some(position.index - 1), "old leader sees committed abort")
        .await
        .unwrap();
    for id in 1..=3 {
        assert_eq!(
            cluster.nodes[&id]
                .journal
                .active_format()
                .await
                .unwrap()
                .feature_epoch,
            0
        );
    }
    cluster.shutdown().await;
}

#[nativelink_test]
async fn fresh_registry_recovers_capable_quorum_and_consensus_resolves_the_uncommitted_tail() {
    for recover_leader in [1, 2] {
        let mut cluster = FormatCluster::new(ReaderCapabilities::DirectScheduler).await;
        cluster.nodes[&1]
            .raft
            .client_write(Batch::new([submit(1)]).unwrap())
            .await
            .unwrap();
        let expected = cluster.nodes[&1].journal.read_applied().await.unwrap().1;
        let preparation = cluster.nodes[&1]
            .journal
            .prepare_features(&cluster.nodes[&1].raft, BTreeSet::from([DIRECT_SCHEDULER]))
            .await
            .unwrap();
        let prepared = cluster.nodes[&1]
            .raft
            .client_write(Batch::format(FormatControl::Prepare(preparation.clone())).unwrap())
            .await
            .unwrap();
        for id in 2..=3 {
            cluster.nodes[&id]
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(
                    Some(prepared.log_id.index),
                    "recovery preparation replicated",
                )
                .await
                .unwrap();
        }
        cluster.isolate(1);
        let _unobserved_reply = cluster.nodes[&1]
            .raft
            .client_write_ff(Batch::format(FormatControl::Activate(preparation.clone())).unwrap())
            .await
            .unwrap();
        tokio::time::timeout(WAIT, async {
            loop {
                if cluster.nodes[&1]
                    .journal
                    .clone()
                    .get_log_state()
                    .await
                    .unwrap()
                    .last_log_id
                    .is_some_and(|position| position.index > prepared.log_id.index)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for id in 1..=3 {
            cluster.stop(id).await;
        }
        cluster.registry = AdmissionRegistry::new(DOMAIN);
        // All old Raft processes and the original admission registry are gone.
        // This fixture supplies the fencing fact; closed artifacts supply the
        // control history, exact positions, and required member set.
        let artifacts: Vec<_> = (1..=3)
            .map(|id| (id, cluster.directory.file(&format!("member-{id}"))))
            .collect();
        let fresh =
            Journal::recover_admission(DOMAIN, ReaderCapabilities::DirectScheduler, artifacts)
                .await
                .unwrap();
        cluster.registry = fresh;
        if recover_leader == 1 {
            cluster.links.lock().blocked.clear();
        }
        assert!(
            Journal::open_with_profile(
                cluster.directory.file("member-1"),
                1,
                Limits::default(),
                profile(&cluster.registry, ReaderCapabilities::Base, 2)
            )
            .await
            .is_err()
        );
        let mut journals = Vec::new();
        for id in 1..=3 {
            journals.push((
                id,
                Journal::open_with_profile(
                    cluster.directory.file(&format!("member-{id}")),
                    id,
                    Limits::default(),
                    profile(&cluster.registry, ReaderCapabilities::DirectScheduler, 2),
                )
                .await
                .unwrap(),
            ));
        }
        // Admission of the entire recovering set precedes any restarted Raft
        // process, including a prior leader that may resume replication.
        for (id, journal) in journals {
            cluster.connect_journal(id, 2, journal).await;
        }
        cluster.nodes[&recover_leader]
            .raft
            .runtime_config()
            .elect(true);
        cluster.nodes[&recover_leader]
            .raft
            .trigger()
            .elect()
            .await
            .unwrap();
        cluster.leader(recover_leader).await;
        cluster.nodes[&recover_leader]
            .raft
            .runtime_config()
            .elect(false);
        if recover_leader == 1 {
            // A new-term blank can commit the preserved old activation. A
            // local pre-crash commit marker never promised base rollback.
            cluster.nodes[&1]
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(
                    Some(prepared.log_id.index + 1),
                    "recovered activation committed",
                )
                .await
                .unwrap();
            assert_eq!(
                cluster.nodes[&1]
                    .journal
                    .active_format()
                    .await
                    .unwrap()
                    .feature_epoch,
                1
            );
        } else {
            assert_eq!(
                cluster.nodes[&2]
                    .journal
                    .active_format()
                    .await
                    .unwrap()
                    .feature_epoch,
                0
            );
            cluster.nodes[&2]
                .journal
                .abort_preparation(&cluster.nodes[&2].raft, preparation.permit)
                .await
                .unwrap();
            cluster.links.lock().blocked.clear();
            cluster.nodes[&2]
                .journal
                .activate_features(&cluster.nodes[&2].raft, BTreeSet::from([DIRECT_SCHEDULER]))
                .await
                .unwrap();
        }
        assert_eq!(
            cluster.nodes[&recover_leader]
                .journal
                .read_applied()
                .await
                .unwrap()
                .1,
            expected
        );
        let position = cluster.nodes[&recover_leader]
            .journal
            .read_applied()
            .await
            .unwrap()
            .0
            .unwrap();
        for id in 1..=3 {
            cluster.nodes[&id]
                .raft
                .wait(Some(WAIT))
                .applied_index_at_least(Some(position.index), "recovered format convergence")
                .await
                .unwrap();
            assert_eq!(
                cluster.nodes[&id]
                    .journal
                    .active_format()
                    .await
                    .unwrap()
                    .feature_epoch,
                1
            );
            assert_eq!(
                cluster.nodes[&id].journal.read_applied().await.unwrap().1,
                expected
            );
        }
        cluster.shutdown().await;
    }
}

#[nativelink_test]
async fn activated_snapshot_install_is_atomic_and_rejects_incompatible_or_foreign_targets() {
    let directory = Directory::new();
    let registry = AdmissionRegistry::new(DOMAIN);
    let node = Singleton::open_with_profile(
        directory.file("source"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 1),
    )
    .await
    .unwrap();
    node.write(submit(1)).await.unwrap();
    node.journal
        .activate_features(&node.raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await
        .unwrap();
    let snapshot = node.journal.clone().build_snapshot().await.unwrap();
    let expected = node.read().await.unwrap();
    let mut target = Journal::fresh_volatile_with_profile(
        2,
        Limits::default(),
        profile(
            &AdmissionRegistry::new(DOMAIN),
            ReaderCapabilities::DirectScheduler,
            1,
        ),
    )
    .await
    .unwrap();
    let old = target.read_applied().await.unwrap();
    target.inject_failure_once(JournalFault::BeforeCommit);
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .is_err()
    );
    assert_eq!(target.read_applied().await.unwrap(), old);
    assert!(target.get_current_snapshot().await.unwrap().is_none());
    target.inject_failure_once(JournalFault::AfterCommit);
    assert!(
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .is_err()
    );
    assert_eq!(target.read_applied().await.unwrap().1, expected);
    assert_eq!(target.active_format().await.unwrap().feature_epoch, 1);
    assert_eq!(
        *target
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .snapshot,
        *snapshot.snapshot
    );
    let mut base = Journal::fresh_volatile_with_profile(
        3,
        Limits::default(),
        profile(&AdmissionRegistry::new(DOMAIN), ReaderCapabilities::Base, 1),
    )
    .await
    .unwrap();
    let before = base.read_applied().await.unwrap();
    assert!(
        base.install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .is_err()
    );
    assert_eq!(base.read_applied().await.unwrap(), before);
    let foreign = AuthorityDomain {
        namespace: DOMAIN.namespace + 1,
        ..DOMAIN
    };
    let mut foreign_target = Journal::fresh_volatile_with_profile(
        4,
        Limits::default(),
        JournalProfile {
            domain: foreign,
            incarnation: 1,
            reader: ReaderCapabilities::DirectScheduler,
            admission: AdmissionRegistry::new(foreign),
        },
    )
    .await
    .unwrap();
    assert!(
        foreign_target
            .install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .is_err()
    );
    assert_eq!(
        foreign_target.active_format().await.unwrap().feature_epoch,
        0
    );
    node.shutdown().await.unwrap();
}

#[nativelink_test]
async fn activation_preserves_dispatch_uncertain_charge_and_accepted_publication_together() {
    let directory = Directory::new();
    let registry = AdmissionRegistry::new(DOMAIN);
    let node = Singleton::open_with_profile(
        directory.file("journal"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 1),
    )
    .await
    .unwrap();
    let record = |event| RecordedEvent {
        epoch: DOMAIN.epoch,
        event,
    };
    let capacity = Resources {
        cpu_millis: 300,
        memory_bytes: 3072,
        slots: 3,
    };
    node.write(record(Event::Register {
        worker: WorkerId(1),
        incarnation: IncarnationId(1),
        session: SessionId(1),
        properties: BTreeMap::new(),
        capacity,
        available: capacity,
    }))
    .await
    .unwrap();
    for operation in 1..=3 {
        let mut command = submit(operation);
        if let Event::Submit { request, .. } = &mut command.event {
            request.skip_cache_lookup = true;
        }
        node.write(command).await.unwrap();
        assert!(matches!(
            node.write(record(Event::Match)).await.unwrap().data,
            Reply::Matched { .. }
        ));
    }
    let first = node.read().await.unwrap().view().attempts[0];
    node.write(record(Event::WorkerAccepted {
        attempt: first.id,
        worker: first.worker,
        incarnation: first.incarnation,
    }))
    .await
    .unwrap();
    node.write(record(Event::Complete {
        attempt: first.id,
        worker: first.worker,
        incarnation: first.incarnation,
        result: ResultPayload {
            digest: "accepted-result".into(),
            exit_code: 0,
            error: None,
            response: None,
        },
    }))
    .await
    .unwrap();
    node.write(record(Event::Cancel {
        operation: OperationId(2),
    }))
    .await
    .unwrap();
    node.write(submit(4)).await.unwrap();
    let expected = node.read().await.unwrap();
    let view = expected.view();
    assert!(
        view.effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::Dispatch { .. }))
    );
    assert!(
        view.effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
    );
    assert!(
        view.effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    assert_eq!(
        view.reservations
            .iter()
            .filter(|reservation| !reservation.released)
            .count(),
        2
    );
    node.journal
        .activate_features(&node.raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await
        .unwrap();
    assert_eq!(node.read().await.unwrap(), expected);
    let snapshot = node.journal.clone().build_snapshot().await.unwrap();
    let before = node.journal.read_applied().await.unwrap();
    let wrong_epoch = entry(next(before.0.unwrap()), Batch::new([submit(5)]).unwrap());
    assert!(node.journal.clone().apply([wrong_epoch]).await.is_err());
    assert_eq!(node.journal.read_applied().await.unwrap(), before);
    node.shutdown().await.unwrap();
    let reopened = Singleton::open_with_profile(
        directory.file("journal"),
        NODE,
        Limits::default(),
        profile(&registry, ReaderCapabilities::DirectScheduler, 2),
    )
    .await
    .unwrap();
    assert_eq!(reopened.read().await.unwrap(), expected);
    assert_eq!(
        *reopened
            .journal
            .clone()
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap()
            .snapshot,
        *snapshot.snapshot
    );
    reopened.shutdown().await.unwrap();
}

#[test]
fn future_feature_ids_remain_bounded_known_controls_without_becoming_active_sections() {
    let preparation = Preparation {
        domain: DOMAIN,
        permit: 1,
        from_epoch: 0,
        target_epoch: 1,
        required: BTreeSet::from([500]),
        membership_revision: LogId::new(openraft::CommittedLeaderId::new(1, NODE), 0),
        members: BTreeMap::from([(
            NODE,
            MemberEvidence {
                incarnation: 1,
                reader: ReaderCapabilities::DirectScheduler,
            },
        )]),
    };
    assert!(!ReaderCapabilities::Base.supports(&preparation.required));
    assert!(!ReaderCapabilities::DirectScheduler.supports(&preparation.required));
    let control = Batch::format(FormatControl::Activate(preparation.clone())).unwrap();
    let encoded = serde_json::to_vec(&control).unwrap();
    let restored: Batch = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(serde_json::to_vec(&restored).unwrap(), encoded);
    let mut unknown: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    unknown["future_authority_section"] = true.into();
    assert!(serde_json::from_value::<Batch>(unknown).is_err());
    let mut too_many = preparation.clone();
    too_many.required = (1..=u16::try_from(MAX_FEATURES + 1).unwrap()).collect();
    assert!(Batch::format(FormatControl::Prepare(too_many)).is_err());
    let mut too_many = preparation;
    too_many.members = (1..=u64::try_from(MAX_FORMAT_MEMBERS + 1).unwrap())
        .map(|id| {
            (
                id,
                MemberEvidence {
                    incarnation: 1,
                    reader: ReaderCapabilities::DirectScheduler,
                },
            )
        })
        .collect();
    assert!(Batch::format(FormatControl::Prepare(too_many)).is_err());
}
