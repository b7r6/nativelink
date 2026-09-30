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

use crate::types::{Command, CommandResponse, RejectReason};

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

/// Default retry cap when none is configured.
pub const DEFAULT_RETRY_LIMIT: usize = 5;

/// The applied state machine. Deterministic in the log: identical bytes on
/// every replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// operation_id -> (worker_id, session_epoch, attempt). This and `sessions`
    /// are the SAME deterministic function of the log, so they cannot disagree:
    /// there is no window in which ownership points at a session membership has
    /// already replaced. Set on assignment; cleared on requeue / finish /
    /// supersession — all inside one apply.
    pub operation_owner: HashMap<OperationId, WorkerOwnership>,
    /// operation_id -> current revision. Bumped on every accepted mutation. A
    /// validated command carries the revision the caller last saw; apply
    /// rejects it if this has moved on. This is the version-CAS, moved inside
    /// apply as a semantic guard (Astra premortem S0).
    pub operation_revision: HashMap<OperationId, i64>,
    /// operation_id -> current attempt number. Bumped on each assignment. Names
    /// the attempt for heartbeat/finish ownership so a delayed message from a
    /// superseded attempt cannot affect its replacement (S6d).
    pub operation_attempt: HashMap<OperationId, u64>,
    /// The retry cap. A finish reporting failure past this many attempts fails
    /// the op instead of requeueing it.
    pub retry_limit: usize,
}

/// Who owns an in-flight operation: a worker session and the attempt number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerOwnership {
    pub worker_id: WorkerId,
    pub session_epoch: u64,
    pub attempt: u64,
}

impl Default for AppliedState {
    fn default() -> Self {
        Self {
            operations: BTreeMap::new(),
            client_to_operation: HashMap::new(),
            unique_key_to_operation: HashMap::new(),
            sorted: SortedIndices::default(),
            leases: HashMap::new(),
            sessions: HashMap::new(),
            operation_owner: HashMap::new(),
            operation_revision: HashMap::new(),
            operation_attempt: HashMap::new(),
            retry_limit: DEFAULT_RETRY_LIMIT,
        }
    }
}

/// A snapshot-safe encoding of [`AppliedState`].
///
/// `AppliedState` holds maps keyed by `ActionUniqueKey` and `OperationId`
/// (a struct and an enum). JSON — and any self-describing format that encodes a
/// map as an object — cannot use those as object keys; `serde_json::to_vec`
/// errors at runtime the moment such a map is nonempty. Astra premortem S6b
/// caught this: empty-state snapshot tests miss it. The fix is a reversible
/// encoding where every map becomes a sequence of key/value pairs, which serde
/// encodes as a JSON array and round-trips losslessly for ANY key type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortableState {
    operations: Vec<(OperationId, AwaitedAction)>,
    client_to_operation: Vec<(OperationId, OperationId)>,
    unique_key_to_operation: Vec<(ActionUniqueKey, OperationId)>,
    sorted: SortedIndices,
    leases: Vec<(OperationId, u128)>,
    sessions: Vec<(WorkerId, u64)>,
    operation_owner: Vec<(OperationId, WorkerOwnership)>,
    operation_revision: Vec<(OperationId, i64)>,
    operation_attempt: Vec<(OperationId, u64)>,
    retry_limit: usize,
}

impl AppliedState {
    /// Convert to the snapshot-safe pair-sequence encoding.
    pub fn to_portable(&self) -> PortableState {
        PortableState {
            operations: self
                .operations
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            client_to_operation: self
                .client_to_operation
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            unique_key_to_operation: self
                .unique_key_to_operation
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            sorted: self.sorted.clone(),
            leases: self.leases.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            sessions: self.sessions.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            operation_owner: self
                .operation_owner
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            operation_revision: self
                .operation_revision
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            operation_attempt: self
                .operation_attempt
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            retry_limit: self.retry_limit,
        }
    }

    /// Rebuild from the snapshot-safe encoding.
    pub fn from_portable(p: PortableState) -> Self {
        Self {
            operations: p.operations.into_iter().collect(),
            client_to_operation: p.client_to_operation.into_iter().collect(),
            unique_key_to_operation: p.unique_key_to_operation.into_iter().collect(),
            sorted: p.sorted,
            leases: p.leases.into_iter().collect(),
            sessions: p.sessions.into_iter().collect(),
            operation_owner: p.operation_owner.into_iter().collect(),
            operation_revision: p.operation_revision.into_iter().collect(),
            operation_attempt: p.operation_attempt.into_iter().collect(),
            retry_limit: p.retry_limit,
        }
    }
}

impl AppliedState {
    /// Bump and return the new revision for an operation, and stamp it onto the
    /// record so a published `AwaitedAction` carries its own revision.
    fn bump_revision(&mut self, op: &OperationId, action: &mut AwaitedAction) -> i64 {
        let next = self.operation_revision.get(op).copied().unwrap_or(0) + 1;
        self.operation_revision.insert(op.clone(), next);
        action.set_version_public(next);
        next
    }

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
        self.bump_revision(op, &mut updated);
        self.sorted
            .btree_for_state(&ActionStage::Executing)
            .remove(&old_sorted);
        self.sorted
            .btree_for_state(&ActionStage::Queued)
            .insert(SortedAwaitedAction::from(&updated));
        self.leases.insert(op.clone(), now_unix_nanos);
        self.operation_owner.remove(op);
        // The attempt that was running is over; the next assignment starts a new
        // one. Keep operation_attempt as the high-water mark so attempts never
        // reuse a number.
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
            .filter(|(_, o)| &o.worker_id == worker_id && o.session_epoch == epoch)
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

    /// Assign a Queued op to a worker, validated in apply (S0). Rejects a
    /// revision conflict (a second assignment off the same Queued revision), a
    /// non-Queued predecessor, a terminal op, or a stale worker session.
    fn apply_assign(
        &mut self,
        operation_id: OperationId,
        expected_revision: i64,
        worker_id: WorkerId,
        session_epoch: u64,
        now_unix_nanos: u128,
    ) -> (CommandResponse, Vec<OperationId>) {
        let Some(existing) = self.operations.get(&operation_id) else {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::NoSuchOperation,
                },
                Vec::new(),
            );
        };
        let actual = self
            .operation_revision
            .get(&operation_id)
            .copied()
            .unwrap_or(0);
        if actual != expected_revision {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::RevisionConflict {
                        expected: expected_revision,
                        actual,
                    },
                },
                Vec::new(),
            );
        }
        if existing.state().stage.is_finished() {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::AlreadyTerminal,
                },
                Vec::new(),
            );
        }
        // Only a Queued op is eligible for assignment.
        if !matches!(existing.state().stage, ActionStage::Queued) {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::RevisionConflict {
                        expected: expected_revision,
                        actual,
                    },
                },
                Vec::new(),
            );
        }
        // The worker session must be the current one for that worker id.
        if self.sessions.get(&worker_id).copied() != Some(session_epoch) {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::StaleWorkerSession {
                        worker_id,
                        epoch: session_epoch,
                    },
                },
                Vec::new(),
            );
        }

        let old_sorted = SortedAwaitedAction::from(existing);
        let mut updated = existing.clone();
        let now = nanos_to_system_time(now_unix_nanos);
        let mut new_state = existing.state().as_ref().clone();
        new_state.stage = ActionStage::Executing;
        updated.set_worker_id(Some(worker_id.clone()), now);
        updated.worker_set_state(std::sync::Arc::new(new_state), now);

        let attempt = self
            .operation_attempt
            .get(&operation_id)
            .copied()
            .unwrap_or(0)
            + 1;
        self.operation_attempt.insert(operation_id.clone(), attempt);
        self.operation_owner.insert(
            operation_id.clone(),
            WorkerOwnership {
                worker_id,
                session_epoch,
                attempt,
            },
        );

        let revision = self.bump_revision(&operation_id, &mut updated);
        self.sorted
            .btree_for_state(&ActionStage::Queued)
            .remove(&old_sorted);
        self.sorted
            .btree_for_state(&ActionStage::Executing)
            .insert(SortedAwaitedAction::from(&updated));
        self.leases.insert(operation_id.clone(), now_unix_nanos);
        self.operations
            .insert(operation_id.clone(), updated.clone());
        (
            CommandResponse::Applied {
                awaited_action: Some(updated),
                revision,
            },
            vec![operation_id],
        )
    }

    /// Apply a validated worker-progress / finish update (S0). Rejects a
    /// revision conflict or a nonterminal update to a terminal op.
    fn apply_update(
        &mut self,
        operation_id: OperationId,
        expected_revision: i64,
        new_state: std::sync::Arc<nativelink_util::action_messages::ActionState>,
        worker_id: Option<WorkerId>,
        attempts: usize,
        now_unix_nanos: u128,
    ) -> (CommandResponse, Vec<OperationId>) {
        let Some(existing) = self.operations.get(&operation_id) else {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::NoSuchOperation,
                },
                Vec::new(),
            );
        };
        let actual = self
            .operation_revision
            .get(&operation_id)
            .copied()
            .unwrap_or(0);
        if actual != expected_revision {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::RevisionConflict {
                        expected: expected_revision,
                        actual,
                    },
                },
                Vec::new(),
            );
        }
        // A terminal op absorbs no further nonterminal update: it cannot be
        // resurrected (protects N7's "one live op per qualifier" — a stale
        // Queued/Executing write can neither un-finish X nor delete Y's claim).
        if existing.state().stage.is_finished() && !new_state.stage.is_finished() {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::AlreadyTerminal,
                },
                Vec::new(),
            );
        }
        // Retry cap: a requeue (back to Queued for another attempt) past the cap
        // is refused, so a persistently failing action does not loop forever.
        // The caller carries the running attempt count; the decision lives here.
        if matches!(new_state.stage, ActionStage::Queued) && attempts > self.retry_limit {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::RetryCapExceeded {
                        attempts,
                        limit: self.retry_limit,
                    },
                },
                Vec::new(),
            );
        }

        let old_stage = existing.state().stage.clone();
        let old_sorted = SortedAwaitedAction::from(existing);
        let mut updated = existing.clone();
        let now = nanos_to_system_time(now_unix_nanos);
        updated.set_worker_id(worker_id.clone(), now);
        updated.worker_set_state(new_state.clone(), now);
        // Preserve the caller's retry counter rather than the stored one, so a
        // per-attempt increment is not silently dropped (adapter defect, S0).
        updated.attempts = attempts;

        // Ownership: a finish or a move off Executing clears ownership.
        if new_state.stage.is_finished() || !matches!(new_state.stage, ActionStage::Executing) {
            self.operation_owner.remove(&operation_id);
        }

        if !old_stage.is_same_stage(&new_state.stage) {
            self.sorted.btree_for_state(&old_stage).remove(&old_sorted);
            let new_sorted = SortedAwaitedAction::from(&updated);
            self.sorted
                .btree_for_state(&new_state.stage)
                .insert(new_sorted);
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
        let revision = self.bump_revision(&operation_id, &mut updated);
        self.leases.insert(operation_id.clone(), now_unix_nanos);
        self.operations
            .insert(operation_id.clone(), updated.clone());
        (
            CommandResponse::Applied {
                awaited_action: Some(updated),
                revision,
            },
            vec![operation_id],
        )
    }

    /// Apply a heartbeat that names its owning attempt/session (S6d). Rejects a
    /// heartbeat from a session/attempt that no longer owns the op, and never
    /// moves the lease time backward within an identity.
    fn apply_heartbeat(
        &mut self,
        operation_id: OperationId,
        worker_id: WorkerId,
        session_epoch: u64,
        attempt: u64,
        now_unix_nanos: u128,
    ) -> (CommandResponse, Vec<OperationId>) {
        let Some(owner) = self.operation_owner.get(&operation_id) else {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::StaleAttempt,
                },
                Vec::new(),
            );
        };
        if owner.worker_id != worker_id
            || owner.session_epoch != session_epoch
            || owner.attempt != attempt
        {
            return (
                CommandResponse::Rejected {
                    reason: RejectReason::StaleAttempt,
                },
                Vec::new(),
            );
        }
        // Never regress the lease: an older heartbeat that commits after a newer
        // one must not lower the recorded time.
        let current_lease = self.leases.get(&operation_id).copied().unwrap_or(0);
        if now_unix_nanos <= current_lease {
            // No-op refresh: the newer lease stands. Not a rejection (the sender
            // is legitimate), just no state change.
            let awaited = self.operations.get(&operation_id).cloned();
            let revision = self
                .operation_revision
                .get(&operation_id)
                .copied()
                .unwrap_or(0);
            return (
                CommandResponse::Applied {
                    awaited_action: awaited,
                    revision,
                },
                Vec::new(),
            );
        }
        self.leases.insert(operation_id.clone(), now_unix_nanos);
        if let Some(existing) = self.operations.get_mut(&operation_id) {
            let state = existing.state().clone();
            existing.worker_set_state(state, nanos_to_system_time(now_unix_nanos));
        }
        let awaited = self.operations.get(&operation_id).cloned();
        let revision = self
            .operation_revision
            .get(&operation_id)
            .copied()
            .unwrap_or(0);
        (
            CommandResponse::Applied {
                awaited_action: awaited,
                revision,
            },
            vec![operation_id],
        )
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
                origin_metadata,
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
                                let revision = awaited.version_public();
                                return (
                                    CommandResponse::Applied {
                                        awaited_action: Some(awaited),
                                        revision,
                                    },
                                    vec![existing.clone()],
                                );
                            }
                        }
                    }
                }

                // Pure constructor: origin metadata was captured on the request
                // path and passed in the command; apply reads no ambient context.
                let mut awaited = AwaitedAction::new_with_metadata(
                    operation_id.clone(),
                    action_info.clone(),
                    nanos_to_system_time(now_unix_nanos),
                    origin_metadata,
                );
                let revision = self.bump_revision(&operation_id, &mut awaited);
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
                self.operation_attempt.insert(operation_id.clone(), 0);
                self.operations
                    .insert(operation_id.clone(), awaited.clone());
                (
                    CommandResponse::Applied {
                        awaited_action: Some(awaited),
                        revision,
                    },
                    vec![operation_id],
                )
            }
            Command::AssignToWorker {
                operation_id,
                expected_revision,
                worker_id,
                session_epoch,
                now_unix_nanos,
            } => self.apply_assign(
                operation_id,
                expected_revision,
                worker_id,
                session_epoch,
                now_unix_nanos,
            ),
            Command::UpdateAwaitedAction {
                operation_id,
                expected_revision,
                new_state,
                worker_id,
                attempts,
                now_unix_nanos,
            } => self.apply_update(
                operation_id,
                expected_revision,
                new_state,
                worker_id,
                attempts,
                now_unix_nanos,
            ),
            Command::Heartbeat {
                operation_id,
                worker_id,
                session_epoch,
                attempt,
                now_unix_nanos,
            } => self.apply_heartbeat(
                operation_id,
                worker_id,
                session_epoch,
                attempt,
                now_unix_nanos,
            ),
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
