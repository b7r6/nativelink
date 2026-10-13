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

//! openraft storage (v2): `RaftLogStorage` + `RaftStateMachine`.
//!
//! The log is mirrored in a `BTreeMap<u64, Entry>` for reads and, when a data
//! directory is configured, made durable through the [`Wal`]: every append,
//! truncate, purge, vote and committed id is on disk before openraft is told it
//! happened. The state machine is [`super::state::AppliedState`]; it is durable
//! through snapshots (written atomically to the data directory) plus replay of
//! the log entries after the snapshot, which openraft performs on startup.

use core::fmt::Debug;
use core::ops::RangeBounds;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nativelink_util::action_messages::OperationId;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogId, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use tokio::sync::RwLock;

use nativelink_util::action_messages::ActionStage;
use nativelink_util::spawn_blocking;

use crate::awaited_action_db::AwaitedAction;
use nativelink_util::metrics::{
    EXECUTION_METRICS, ExecutionResult, ExecutionStage, make_execution_attributes,
};

use super::state::{AppliedState, PortableState, StageTransition};
use super::types::{CommandResponse, Entry, NodeId, TypeConfig};
use super::wal::{Wal, WalRecord, read_optional, write_atomic};

type StorageResult<T> = Result<T, StorageError<NodeId>>;

/// See [`StateMachineStore::on_apply`].
pub type OnApply = Arc<dyn Fn(Vec<(OperationId, Option<AwaitedAction>)>) + Send + Sync>;

const SNAPSHOT_FILE: &str = "snapshot.json";

/// The execution metrics the memory backend records for the same stage
/// changes, so dashboards read the same with either backend.
fn emit_stage_metrics(transitions: &[StageTransition]) {
    let metrics = &*EXECUTION_METRICS;
    let stage_attrs = |stage: &ActionStage| {
        vec![opentelemetry::KeyValue::new(
            nativelink_util::metrics::EXECUTION_STAGE,
            ExecutionStage::from(stage),
        )]
    };
    for t in transitions {
        if let Some(from) = &t.from {
            metrics.execution_active_count.add(-1, &stage_attrs(from));
        }
        if let Some(to) = &t.to {
            metrics.execution_active_count.add(1, &stage_attrs(to));
        }
        let (Some(_), Some(to)) = (&t.from, &t.to) else {
            continue;
        };
        metrics.execution_stage_transitions.add(
            1,
            &make_execution_attributes("unknown", None, Some(t.priority)),
        );
        let result = match to {
            ActionStage::Completed(action_result) if action_result.exit_code == 0 => {
                ExecutionResult::Success
            }
            ActionStage::Completed(_) => ExecutionResult::Failure,
            ActionStage::CompletedFromCache(_) => ExecutionResult::CacheHit,
            _ => continue,
        };
        metrics.execution_completed_count.add(
            1,
            &[
                opentelemetry::KeyValue::new(nativelink_util::metrics::EXECUTION_RESULT, result),
                opentelemetry::KeyValue::new(
                    nativelink_util::metrics::EXECUTION_ACTION_DIGEST,
                    t.action_digest.clone(),
                ),
            ],
        );
    }
}
/// Rewrite the log file once this many purged entries are still in it.
const COMPACT_AFTER_PURGED: u64 = 10_000;

fn io_err(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    e: std::io::Error,
) -> StorageError<NodeId> {
    StorageError::from_io_error(subject, verb, e)
}

/// Shared log state, mirrored in memory.
#[derive(Debug, Default)]
struct LogInner {
    log: BTreeMap<u64, Entry>,
    last_purged: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    /// Entries purged from the mirror but still present in the log file.
    purged_since_rewrite: u64,
}

impl LogInner {
    fn apply_record(&mut self, record: WalRecord) {
        match record {
            WalRecord::Entry(entry) => {
                self.log.insert(entry.get_log_id().index, *entry);
            }
            WalRecord::Vote(vote) => self.vote = Some(vote),
            WalRecord::Truncate(index) => {
                self.log.split_off(&index);
            }
            WalRecord::Purge(log_id) => {
                self.last_purged = Some(log_id);
                self.log = self.log.split_off(&(log_id.index + 1));
            }
            WalRecord::Committed(committed) => self.committed = committed,
        }
    }

    /// The minimal record set that reproduces this state.
    fn live_records(&self) -> Vec<WalRecord> {
        let mut records = Vec::with_capacity(self.log.len() + 3);
        if let Some(vote) = self.vote {
            records.push(WalRecord::Vote(vote));
        }
        if let Some(last_purged) = self.last_purged {
            records.push(WalRecord::Purge(last_purged));
        }
        records.push(WalRecord::Committed(self.committed));
        records.extend(
            self.log
                .values()
                .cloned()
                .map(|e| WalRecord::Entry(Box::new(e))),
        );
        records
    }
}

/// The log store half. Cloneable: the `LogReader` shares the same inner.
#[derive(Debug, Clone)]
pub struct LogStore {
    inner: Arc<RwLock<LogInner>>,
    /// `None` keeps the log in memory only (tests, and the spike's behavior).
    wal: Option<Arc<std::sync::Mutex<Wal>>>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(LogInner::default())),
            wal: None,
        }
    }
}

impl LogStore {
    /// Open a durable log in `dir`, replaying whatever is already there.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        let (wal, records) = Wal::open(dir)?;
        let mut inner = LogInner::default();
        for record in records {
            inner.apply_record(record);
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            wal: Some(Arc::new(std::sync::Mutex::new(wal))),
        })
    }

    /// Make `records` durable. Runs the blocking write and `fdatasync` off the
    /// async executor.
    async fn persist(&self, records: Vec<WalRecord>) -> std::io::Result<()> {
        let Some(wal) = self.wal.clone() else {
            return Ok(());
        };
        spawn_blocking!("raft_log_append", move || {
            wal.lock()
                .map_err(|_| std::io::Error::other("raft log mutex poisoned"))?
                .append(&records)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn rewrite(&self, records: Vec<WalRecord>) -> std::io::Result<()> {
        let Some(wal) = self.wal.clone() else {
            return Ok(());
        };
        spawn_blocking!("raft_log_rewrite", move || {
            wal.lock()
                .map_err(|_| std::io::Error::other("raft log mutex poisoned"))?
                .rewrite(&records)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    /// Size of the log file on disk, for tests and metrics.
    pub fn file_len(&self) -> Option<u64> {
        self.wal
            .as_ref()
            .and_then(|wal| wal.lock().ok().and_then(|wal| wal.len().ok()))
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: R,
    ) -> StorageResult<Vec<Entry>> {
        let inner = self.inner.read().await;
        Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let inner = self.inner.read().await;
        let last = inner.log.iter().next_back().map(|(_, e)| *e.get_log_id());
        let last_purged = inner.last_purged;
        let last = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        self.persist(vec![WalRecord::Vote(*vote)])
            .await
            .map_err(|e| io_err(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        self.inner.write().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self.inner.read().await.vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> StorageResult<()> {
        self.persist(vec![WalRecord::Committed(committed)])
            .await
            .map_err(|e| io_err(ErrorSubject::Store, ErrorVerb::Write, e))?;
        self.inner.write().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(self.inner.read().await.committed)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        let records: Vec<WalRecord> = entries
            .iter()
            .cloned()
            .map(|e| WalRecord::Entry(Box::new(e)))
            .collect();
        // Durable first, then visible, then acknowledged: openraft may count
        // this node toward a quorum as soon as the callback fires.
        let result = self.persist(records).await;
        if let Err(e) = result {
            let err = io_err(ErrorSubject::Logs, ErrorVerb::Write, e);
            callback.log_io_completed(Err(std::io::Error::other(err.to_string())));
            return Err(err);
        }
        {
            let mut inner = self.inner.write().await;
            for entry in entries {
                inner.log.insert(entry.get_log_id().index, entry);
            }
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        self.persist(vec![WalRecord::Truncate(log_id.index)])
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        let mut inner = self.inner.write().await;
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        self.persist(vec![WalRecord::Purge(log_id)])
            .await
            .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Delete, e))?;
        let compact = {
            let mut inner = self.inner.write().await;
            let before = inner.log.len() as u64;
            inner.apply_record(WalRecord::Purge(log_id));
            inner.purged_since_rewrite += before - inner.log.len() as u64;
            if inner.purged_since_rewrite >= COMPACT_AFTER_PURGED {
                inner.purged_since_rewrite = 0;
                Some(inner.live_records())
            } else {
                None
            }
        };
        if let Some(records) = compact {
            self.rewrite(records)
                .await
                .map_err(|e| io_err(ErrorSubject::Logs, ErrorVerb::Write, e))?;
        }
        Ok(())
    }
}

/// The state machine half, with a broadcast of changed operation ids.
///
/// Cloneable: clones share the same inner `Arc<RwLock<..>>`, so the db layer
/// can hold a read handle while `Raft` owns the write handle.
#[derive(Clone)]
pub struct StateMachineStore {
    inner: Arc<RwLock<StateMachineInner>>,
    /// Called inside each `apply`, before it returns, with every operation
    /// the entry changed and its new value (`None`: removed). Subscribers
    /// therefore see a change before the write that made it is acknowledged,
    /// as with the memory backend.
    on_apply: OnApply,
    /// Where snapshots are written. `None` keeps them in memory only.
    snapshot_dir: Option<PathBuf>,
}

#[derive(Debug, Default)]
struct StateMachineInner {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    state: AppliedState,
    snapshot_idx: u64,
    current_snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotPayload {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    // Snapshot-safe pair-sequence encoding: the applied state has maps keyed by
    // ActionUniqueKey / OperationId that cannot be JSON object keys (S6b).
    state: PortableState,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

impl Debug for StateMachineStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StateMachineStore")
            .field("snapshot_dir", &self.snapshot_dir)
            .finish_non_exhaustive()
    }
}

impl StateMachineStore {
    pub fn new(on_apply: OnApply) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StateMachineInner::default())),
            on_apply,
            snapshot_dir: None,
        }
    }

    /// Open a state machine whose snapshots live in `dir`. If a snapshot is
    /// already there, the applied state starts from it; openraft then replays
    /// the log entries after `last_applied`.
    pub fn open(dir: &Path, on_apply: OnApply) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut inner = StateMachineInner::default();
        if let Some(data) = read_optional(dir, SNAPSHOT_FILE)? {
            let stored: StoredSnapshotFile =
                serde_json::from_slice(&data).map_err(std::io::Error::other)?;
            let payload: SnapshotPayload =
                serde_json::from_slice(&stored.data).map_err(std::io::Error::other)?;
            inner.last_applied = payload.last_applied;
            inner.last_membership = payload.last_membership;
            inner.state = AppliedState::from_portable(payload.state);
            inner.current_snapshot = Some(StoredSnapshot {
                meta: stored.meta,
                data: stored.data,
            });
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
            on_apply,
            snapshot_dir: Some(dir.to_path_buf()),
        })
    }

    /// Set the retry cap before raft starts applying entries.
    pub fn set_retry_limit(&self, retry_limit: usize) {
        if let Ok(mut inner) = self.inner.try_write() {
            inner.state.retry_limit = retry_limit;
        }
    }

    /// Run `f` against the applied state under the read lock, without
    /// cloning the whole state.
    pub async fn with_state<R>(&self, f: impl FnOnce(&AppliedState) -> R) -> R {
        f(&self.inner.read().await.state)
    }

    /// A full copy of the applied state. Costs O(state); for tests and
    /// snapshot checks only, never the request path.
    pub async fn snapshot_state(&self) -> AppliedState {
        self.inner.read().await.state.clone()
    }

    async fn persist_snapshot(&self, snapshot: &StoredSnapshot) -> std::io::Result<()> {
        let Some(dir) = self.snapshot_dir.clone() else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(&StoredSnapshotFile {
            meta: snapshot.meta.clone(),
            data: snapshot.data.clone(),
        })
        .map_err(std::io::Error::other)?;
        spawn_blocking!("raft_snapshot_write", move || write_atomic(
            &dir,
            SNAPSHOT_FILE,
            &bytes
        ))
        .await
        .map_err(std::io::Error::other)?
    }
}

/// A snapshot as it is stored on disk: its metadata and the payload bytes
/// exactly as they are shipped to followers.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredSnapshotFile {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
    async fn build_snapshot(&mut self) -> StorageResult<Snapshot<TypeConfig>> {
        let (payload, snapshot_id) = {
            let mut inner = self.inner.write().await;
            inner.snapshot_idx += 1;
            let payload = SnapshotPayload {
                last_applied: inner.last_applied,
                last_membership: inner.last_membership.clone(),
                state: inner.state.to_portable(),
            };
            let snapshot_id = format!(
                "{}-{}",
                inner.last_applied.map(|l| l.index).unwrap_or_default(),
                inner.snapshot_idx
            );
            (payload, snapshot_id)
        };
        let data = serde_json::to_vec(&payload).map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other(e),
            )
        })?;
        let meta = SnapshotMeta {
            last_log_id: payload.last_applied,
            last_membership: payload.last_membership,
            snapshot_id,
        };
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        // On disk before openraft may purge the log entries it covers.
        self.persist_snapshot(&stored)
            .await
            .map_err(|e| io_err(ErrorSubject::Snapshot(None), ErrorVerb::Write, e))?;
        self.inner.write().await.current_snapshot = Some(stored);
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> StorageResult<(
        Option<LogId<NodeId>>,
        StoredMembership<NodeId, openraft::BasicNode>,
    )> {
        let inner = self.inner.read().await;
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<CommandResponse>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();
        let mut inner = self.inner.write().await;
        for entry in entries {
            let log_id = *entry.get_log_id();
            inner.last_applied = Some(log_id);
            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(CommandResponse::Applied {
                        awaited_action: None,
                        revision: 0,
                    });
                }
                EntryPayload::Normal(cmd) => {
                    let (resp, changed) = inner.state.apply(cmd);
                    emit_stage_metrics(&inner.state.transitions);
                    if !changed.is_empty() {
                        let published: Vec<(OperationId, Option<AwaitedAction>)> = changed
                            .into_iter()
                            .map(|op| {
                                let action = inner.state.operations.get(&op).cloned();
                                (op, action)
                            })
                            .collect();
                        (self.on_apply)(published);
                    }
                    responses.push(resp);
                }
                EntryPayload::Membership(mem) => {
                    inner.last_membership = StoredMembership::new(Some(log_id), mem);
                    responses.push(CommandResponse::Applied {
                        awaited_action: None,
                        revision: 0,
                    });
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StorageResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StorageResult<()> {
        let data = snapshot.into_inner();
        let payload: SnapshotPayload = serde_json::from_slice(&data).map_err(|e| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                std::io::Error::other(e),
            )
        })?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        self.persist_snapshot(&stored).await.map_err(|e| {
            io_err(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Write,
                e,
            )
        })?;
        let mut inner = self.inner.write().await;
        inner.last_applied = payload.last_applied;
        inner.last_membership = payload.last_membership;
        inner.state = AppliedState::from_portable(payload.state);
        inner.current_snapshot = Some(stored);
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
        let inner = self.inner.read().await;
        Ok(inner.current_snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
