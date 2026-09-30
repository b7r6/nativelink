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

//! In-memory openraft storage (v2): `RaftLogStorage` + `RaftStateMachine`.
//!
//! The log lives in a `BTreeMap<u64, Entry>`; the state machine is
//! [`crate::state::AppliedState`]. On each `apply` we run the command through
//! the applied state and push the changed operation ids to a broadcast so the
//! db layer can wake watchers.

use core::fmt::Debug;
use core::ops::RangeBounds;
use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use nativelink_util::action_messages::OperationId;
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogId, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use tokio::sync::{RwLock, mpsc};

use crate::state::{AppliedState, PortableState};
use crate::types::{CommandResponse, Entry, NodeId, TypeConfig};

type StorageResult<T> = Result<T, StorageError<NodeId>>;

/// Shared, cloneable in-memory log.
#[derive(Debug, Default)]
struct LogInner {
    log: BTreeMap<u64, Entry>,
    last_purged: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

/// The log store half. Cloneable: the `LogReader` shares the same inner.
#[derive(Debug, Clone)]
pub struct LogStore {
    inner: Arc<RwLock<LogInner>>,
}

impl Default for LogStore {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(LogInner::default())),
        }
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
        self.inner.write().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        Ok(self.inner.read().await.vote)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.write().await;
            for entry in entries {
                inner.log.insert(entry.get_log_id().index, entry);
            }
        }
        // In-memory: the write above is durable enough for the spike.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let mut inner = self.inner.write().await;
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let mut inner = self.inner.write().await;
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }
}

/// The state machine half, with a broadcast of changed operation ids.
///
/// Cloneable: clones share the same inner `Arc<RwLock<..>>`, so the db layer
/// can hold a read handle while `Raft` owns the write handle.
#[derive(Debug, Clone)]
pub struct StateMachineStore {
    inner: Arc<RwLock<StateMachineInner>>,
    /// Notified after each `apply` with the operation ids that changed.
    changed_tx: mpsc::UnboundedSender<Vec<OperationId>>,
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

impl StateMachineStore {
    pub fn new(changed_tx: mpsc::UnboundedSender<Vec<OperationId>>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StateMachineInner::default())),
            changed_tx,
        }
    }

    /// Read-only snapshot of the applied state, for the db query path.
    pub async fn snapshot_state(&self) -> AppliedState {
        self.inner.read().await.state.clone()
    }
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
        let mut inner = self.inner.write().await;
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
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
                    if !changed.is_empty() {
                        let _ = self.changed_tx.send(changed);
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
        Self {
            inner: self.inner.clone(),
            changed_tx: self.changed_tx.clone(),
        }
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
        let mut inner = self.inner.write().await;
        inner.last_applied = payload.last_applied;
        inner.last_membership = payload.last_membership;
        inner.state = AppliedState::from_portable(payload.state);
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
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
