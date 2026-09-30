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

//! The applied state of the replicated `AwaitedActionDb`.
//!
//! This mirrors the inner state of `MemoryAwaitedActionDb`'s `AwaitedActionDbImpl`:
//! the operation-id map, the client-operation-id map, the unique-key dedup map,
//! and the sorted-state indices. The difference is that here it is a pure,
//! deterministic function of the raft log — no `Mutex`, no `now_fn`, no version
//! compare-and-swap.

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_scheduler::awaited_action_db::{
    AwaitedAction, SortedAwaitedAction, SortedAwaitedActionState,
};
use nativelink_util::action_messages::{
    ActionStage, ActionUniqueKey, ActionUniqueQualifier, OperationId, WorkerId,
};
use serde::{Deserialize, Serialize};

use crate::types::{Command, CommandResponse};

/// The five sorted indices, keyed by lifecycle stage, exactly as the memory
/// backend keeps them.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SortedIndices {
    pub unknown: BTreeSet<SortedAwaitedAction>,
    pub cache_check: BTreeSet<SortedAwaitedAction>,
    pub queued: BTreeSet<SortedAwaitedAction>,
    pub executing: BTreeSet<SortedAwaitedAction>,
    pub completed: BTreeSet<SortedAwaitedAction>,
}

impl SortedIndices {
    fn btree_for_state(&mut self, stage: &ActionStage) -> &mut BTreeSet<SortedAwaitedAction> {
        match stage {
            ActionStage::Unknown => &mut self.unknown,
            ActionStage::CacheCheck => &mut self.cache_check,
            ActionStage::Queued => &mut self.queued,
            ActionStage::Executing => &mut self.executing,
            ActionStage::Completed(_) | ActionStage::CompletedFromCache(_) => &mut self.completed,
        }
    }

    pub const fn btree_for_sorted_state(
        &self,
        state: SortedAwaitedActionState,
    ) -> &BTreeSet<SortedAwaitedAction> {
        match state {
            SortedAwaitedActionState::CacheCheck => &self.cache_check,
            SortedAwaitedActionState::Queued => &self.queued,
            SortedAwaitedActionState::Executing => &self.executing,
            SortedAwaitedActionState::Completed => &self.completed,
        }
    }
}

fn nanos_to_system_time(nanos: u128) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

/// The applied state machine. Deterministic in the log: identical bytes on
/// every replica.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AppliedState {
    /// operation_id -> the last-applied AwaitedAction.
    pub operations: BTreeMap<OperationId, AwaitedAction>,
    /// client_operation_id -> operation_id.
    pub client_to_operation: HashMap<OperationId, OperationId>,
    /// unique action key -> operation_id (the dedup map).
    pub unique_key_to_operation: HashMap<ActionUniqueKey, OperationId>,
    /// The sorted lifecycle indices.
    pub sorted: SortedIndices,
    /// worker-session lease: operation_id -> last heartbeat (unix nanos).
    pub leases: HashMap<OperationId, u128>,
    /// Worker-session membership as a log fact: worker_id -> the epoch of the
    /// session currently registered under that id. A `WorkerJoin` with a
    /// higher epoch supersedes the recorded one.
    pub sessions: HashMap<WorkerId, u64>,
    /// Which worker session owns each in-flight operation:
    /// operation_id -> (worker_id, session_epoch). This and `sessions` are the
    /// SAME deterministic function of the log, so they cannot disagree: there
    /// is no window in which ownership points at a session membership has
    /// already replaced. Set when an update assigns a worker; cleared on
    /// requeue / finish / supersession — all inside one apply.
    pub operation_owner: HashMap<OperationId, (WorkerId, u64)>,
}

impl AppliedState {
    /// Requeue a single in-flight operation: move it back to `Queued`, clear its
    /// worker assignment and its session ownership. Returns true if the
    /// operation existed and was requeued. Deterministic: touches only
    /// log-derived state, reads no clock beyond the caller-supplied `now`.
    fn requeue_operation(&mut self, op: &OperationId, now_unix_nanos: u128) -> bool {
        let Some(existing) = self.operations.get(op) else {
            return false;
        };
        // Already queued (or finished): nothing to requeue.
        if !matches!(existing.state().stage, ActionStage::Executing) {
            self.operation_owner.remove(op);
            return false;
        }
        let old_sorted = SortedAwaitedAction::from(existing);
        let mut updated = existing.clone();
        let now = nanos_to_system_time(now_unix_nanos);
        let mut new_state = existing.state().as_ref().clone();
        new_state.stage = ActionStage::Queued;
        updated.set_worker_id(None, now);
        updated.worker_set_state(std::sync::Arc::new(new_state), now);
        self.sorted
            .btree_for_state(&ActionStage::Executing)
            .remove(&old_sorted);
        self.sorted
            .btree_for_state(&ActionStage::Queued)
            .insert(SortedAwaitedAction::from(&updated));
        self.leases.insert(op.clone(), now_unix_nanos);
        self.operation_owner.remove(op);
        self.operations.insert(op.clone(), updated);
        true
    }

    /// Requeue every operation owned by `(worker_id, epoch)`. Returns the ids
    /// requeued. This is the reassignment half of supersession/leave; it runs
    /// in the same apply as the membership change that calls it.
    fn requeue_sessions_operations(
        &mut self,
        worker_id: &WorkerId,
        epoch: u64,
        now_unix_nanos: u128,
    ) -> Vec<OperationId> {
        let owned: Vec<OperationId> = self
            .operation_owner
            .iter()
            .filter(|(_, (w, e))| w == worker_id && *e == epoch)
            .map(|(op, _)| op.clone())
            .collect();
        let mut requeued = Vec::new();
        for op in owned {
            if self.requeue_operation(&op, now_unix_nanos) {
                requeued.push(op);
            } else {
                // Non-executing owned op: still drop stale ownership.
                self.operation_owner.remove(&op);
            }
        }
        requeued
    }

    /// Apply one log command. Returns the response and the set of operation ids
    /// whose `AwaitedAction` changed (so the db layer can wake their watchers).
    pub fn apply(&mut self, cmd: Command) -> (CommandResponse, Vec<OperationId>) {
        match cmd {
            Command::AddAction {
                client_operation_id,
                operation_id,
                action_info,
                now_unix_nanos,
            } => {
                // Dedup: if a cacheable action with the same unique key is already
                // live, join it instead of adding. This is the log-native form of
                // `try_subscribe` — but with no two-search + 20ms sleep race,
                // because the log is the single writer (see CROSSED-OFF N7).
                if let ActionUniqueQualifier::Cacheable(unique_key) = &action_info.unique_qualifier
                {
                    if let Some(existing) = self.unique_key_to_operation.get(unique_key) {
                        if let Some(existing_action) = self.operations.get(existing) {
                            if !existing_action.state().stage.is_finished() {
                                self.client_to_operation
                                    .insert(client_operation_id, existing.clone());
                                let awaited = existing_action.clone();
                                return (
                                    CommandResponse::Applied {
                                        awaited_action: Some(awaited),
                                    },
                                    vec![existing.clone()],
                                );
                            }
                        }
                    }
                }

                let awaited = AwaitedAction::new(
                    operation_id.clone(),
                    action_info.clone(),
                    nanos_to_system_time(now_unix_nanos),
                );
                let sorted = SortedAwaitedAction::from(&awaited);
                self.sorted
                    .btree_for_state(&ActionStage::Queued)
                    .insert(sorted);
                if let ActionUniqueQualifier::Cacheable(unique_key) = &action_info.unique_qualifier
                {
                    self.unique_key_to_operation
                        .insert(unique_key.clone(), operation_id.clone());
                }
                self.client_to_operation
                    .insert(client_operation_id, operation_id.clone());
                self.leases.insert(operation_id.clone(), now_unix_nanos);
                self.operations
                    .insert(operation_id.clone(), awaited.clone());
                (
                    CommandResponse::Applied {
                        awaited_action: Some(awaited),
                    },
                    vec![operation_id],
                )
            }
            Command::UpdateAwaitedAction {
                operation_id,
                new_state,
                worker_id,
                now_unix_nanos,
            } => {
                let Some(existing) = self.operations.get(&operation_id) else {
                    return (
                        CommandResponse::Applied {
                            awaited_action: None,
                        },
                        Vec::new(),
                    );
                };
                let old_stage = existing.state().stage.clone();
                let old_sorted = SortedAwaitedAction::from(existing);

                let mut updated = existing.clone();
                let now = nanos_to_system_time(now_unix_nanos);
                updated.set_worker_id(worker_id.clone(), now);
                updated.worker_set_state(new_state.clone(), now);

                // Stamp (or clear) session ownership from the SAME log entry.
                // If a worker is assigned and the action is executing, the
                // operation is owned by that worker's currently-recorded
                // session epoch. Otherwise ownership is cleared. There is no
                // separate write that could leave ownership dangling.
                match (&worker_id, new_state.stage.is_finished()) {
                    (Some(w), false) if matches!(new_state.stage, ActionStage::Executing) => {
                        let epoch = self.sessions.get(w).copied().unwrap_or(0);
                        self.operation_owner
                            .insert(operation_id.clone(), (w.clone(), epoch));
                    }
                    _ => {
                        self.operation_owner.remove(&operation_id);
                    }
                }

                // Re-index if the stage changed.
                if !old_stage.is_same_stage(&new_state.stage) {
                    self.sorted.btree_for_state(&old_stage).remove(&old_sorted);
                    let new_sorted = SortedAwaitedAction::from(&updated);
                    self.sorted
                        .btree_for_state(&new_state.stage)
                        .insert(new_sorted);
                    // Drop the dedup entry once finished, matching the memory
                    // backend's `process_state_changes_for_hash_key_map`.
                    if new_state.stage.is_finished() {
                        if let ActionUniqueQualifier::Cacheable(unique_key) =
                            &updated.action_info().unique_qualifier
                        {
                            if self
                                .unique_key_to_operation
                                .get(unique_key)
                                .is_some_and(|id| id == &operation_id)
                            {
                                self.unique_key_to_operation.remove(unique_key);
                            }
                        }
                    }
                }
                self.leases.insert(operation_id.clone(), now_unix_nanos);
                self.operations
                    .insert(operation_id.clone(), updated.clone());
                (
                    CommandResponse::Applied {
                        awaited_action: Some(updated),
                    },
                    vec![operation_id],
                )
            }
            Command::Heartbeat {
                operation_id,
                now_unix_nanos,
            } => {
                let changed = if let Some(existing) = self.operations.get_mut(&operation_id) {
                    // `worker_set_state` bumps the worker-updated timestamp; re-set
                    // the current state to record the heartbeat via the public API.
                    let state = existing.state().clone();
                    existing.worker_set_state(state, nanos_to_system_time(now_unix_nanos));
                    self.leases.insert(operation_id.clone(), now_unix_nanos);
                    vec![operation_id.clone()]
                } else {
                    Vec::new()
                };
                let awaited = self.operations.get(&operation_id).cloned();
                (
                    CommandResponse::Applied {
                        awaited_action: awaited,
                    },
                    changed,
                )
            }
            Command::Expire {
                now_unix_nanos,
                lease_timeout_nanos,
            } => {
                // Deterministic requeue. Every executing action whose lease is
                // older than the timeout, as measured by log-recorded
                // timestamps, is requeued. No node reads its own clock.
                let mut requeued = Vec::new();
                let mut changed = Vec::new();
                let expired: Vec<OperationId> = self
                    .operations
                    .iter()
                    .filter(|(op, action)| {
                        matches!(action.state().stage, ActionStage::Executing)
                            && self.leases.get(*op).is_some_and(|last| {
                                now_unix_nanos.saturating_sub(*last) > lease_timeout_nanos
                            })
                    })
                    .map(|(op, _)| op.clone())
                    .collect();
                for op in expired {
                    if self.requeue_operation(&op, now_unix_nanos) {
                        requeued.push(op.clone());
                        changed.push(op);
                    }
                }
                (CommandResponse::Expired { requeued }, changed)
            }
            Command::WorkerJoin {
                worker_id,
                session_epoch,
                now_unix_nanos,
            } => {
                let current = self.sessions.get(&worker_id).copied();
                // A join only takes effect if it is newer than the recorded
                // session. A stale/duplicate join is ignored.
                if current.is_some_and(|c| session_epoch <= c) {
                    return (
                        CommandResponse::WorkerMembership {
                            applied: false,
                            requeued: Vec::new(),
                        },
                        Vec::new(),
                    );
                }
                // Supersede: requeue everything the OLD session still owned,
                // then record the new session. Both happen here, atomically.
                let requeued = match current {
                    Some(old_epoch) => {
                        self.requeue_sessions_operations(&worker_id, old_epoch, now_unix_nanos)
                    }
                    None => Vec::new(),
                };
                self.sessions.insert(worker_id, session_epoch);
                let changed = requeued.clone();
                (
                    CommandResponse::WorkerMembership {
                        applied: true,
                        requeued,
                    },
                    changed,
                )
            }
            Command::WorkerLeave {
                worker_id,
                session_epoch,
                now_unix_nanos,
            } => {
                let current = self.sessions.get(&worker_id).copied();
                // Ignore a leave from a session that is not the current one
                // (e.g. a late leave from an already-superseded session).
                if current != Some(session_epoch) {
                    return (
                        CommandResponse::WorkerMembership {
                            applied: false,
                            requeued: Vec::new(),
                        },
                        Vec::new(),
                    );
                }
                let requeued =
                    self.requeue_sessions_operations(&worker_id, session_epoch, now_unix_nanos);
                self.sessions.remove(&worker_id);
                let changed = requeued.clone();
                (
                    CommandResponse::WorkerMembership {
                        applied: true,
                        requeued,
                    },
                    changed,
                )
            }
        }
    }
}
