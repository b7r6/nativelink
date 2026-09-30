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

use std::io::Cursor;
use std::sync::Arc;

use nativelink_scheduler::awaited_action_db::AwaitedAction;
use nativelink_util::action_messages::{ActionInfo, ActionState, OperationId, WorkerId};
use nativelink_util::origin_event::OriginMetadata;
use openraft::{RaftTypeConfig, declare_raft_types};
use serde::{Deserialize, Serialize};

/// The node id in the cluster. A `u64` is all a single-voter spike needs.
pub type NodeId = u64;

/// Why a command was rejected during apply. A rejected command IS committed to
/// the log (every replica must agree it was rejected) — "client_write
/// succeeded" therefore does NOT mean "the transition was accepted". Callers
/// must inspect the [`CommandResponse`]. See Astra premortem S0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// The operation id is not in the applied state.
    NoSuchOperation,
    /// The caller's `expected_revision` did not match the current record: some
    /// other command mutated it first. This is the CAS, moved into apply.
    RevisionConflict { expected: i64, actual: i64 },
    /// The operation is already terminal; a nonterminal update is refused so a
    /// completed op cannot be resurrected (this also protects the N7 dedup
    /// invariant — a stale update cannot un-finish an op whose qualifier claim
    /// has already passed to a replacement).
    AlreadyTerminal,
    /// An assignment named a worker session that is not the current one for
    /// that worker id (superseded/left), so the assignment is not eligible.
    StaleWorkerSession { worker_id: WorkerId, epoch: u64 },
    /// A heartbeat named an attempt/session that no longer owns the operation.
    StaleAttempt,
    /// The retry cap was reached; the operation is failed rather than requeued.
    RetryCapExceeded { attempts: usize, limit: usize },
}

/// The transition alphabet of the replicated `AwaitedAction` state machine.
///
/// Every mutation of the scheduler state is one of these, and the only way to
/// mutate the state is to append one to the log. Because the log is totally
/// ordered and each replica applies it in the same order, the applied state is
/// identical on every node without any per-node wall-clock or CAS.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Add (or join) an action. The leader mints the `operation_id` before
    /// proposing, so replication is deterministic. `now_unix_nanos` is the
    /// leader's clock frozen into the log entry — replicas do NOT read their
    /// own clock, they replay this value.
    AddAction {
        client_operation_id: OperationId,
        operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        now_unix_nanos: u128,
        /// Origin metadata captured from the request's ambient context BEFORE
        /// proposing, so apply never reads `Context::current()` and is a pure
        /// function of the command bytes. See Astra premortem S6d.
        origin_metadata: Option<OriginMetadata>,
    },
    /// Assign a currently-eligible operation to a worker session.
    ///
    /// Validated in apply: the operation must exist, be at `expected_revision`,
    /// be Queued (a legal predecessor for assignment), and the worker session
    /// must be the current one. Exactly one of two assignments computed off the
    /// same Queued revision can win; the other gets `RevisionConflict`. This is
    /// the CAS the memory/store backends did with a version counter, moved
    /// inside apply. See Astra premortem S0.
    AssignToWorker {
        operation_id: OperationId,
        expected_revision: i64,
        worker_id: WorkerId,
        session_epoch: u64,
        now_unix_nanos: u128,
    },
    /// Transition an existing action to a new worker-reported state (progress or
    /// finish), validated against the revision the caller last observed.
    ///
    /// Unlike the old unconditional form, apply checks `expected_revision` and
    /// refuses a nonterminal update to an already-terminal op. `attempts` is
    /// carried through from the caller's record so the retry counter is not
    /// silently reset to the stored value (Astra premortem S0 adapter defect).
    UpdateAwaitedAction {
        operation_id: OperationId,
        expected_revision: i64,
        new_state: Arc<ActionState>,
        worker_id: Option<WorkerId>,
        attempts: usize,
        now_unix_nanos: u128,
    },
    /// A worker session heartbeat that NAMES the attempt and session it renews.
    ///
    /// Rejected if it does not name the session/attempt that currently owns the
    /// operation (a delayed heartbeat from a superseded attempt must not renew
    /// its replacement's lease), and the lease time never moves backward within
    /// an identity (a reordered older heartbeat cannot regress it). See Astra
    /// premortem S6d.
    Heartbeat {
        operation_id: OperationId,
        worker_id: WorkerId,
        session_epoch: u64,
        attempt: u64,
        now_unix_nanos: u128,
    },
    /// Deterministic liveness. Applied on the leader from the log (NOT decided
    /// by each node's wall clock): any executing action whose worker lease is
    /// older than `lease_timeout_nanos` as of `now_unix_nanos` is requeued.
    /// Because the decision is a function of log-recorded timestamps, every
    /// replica requeues exactly the same set.
    Expire {
        now_unix_nanos: u128,
        lease_timeout_nanos: u128,
    },
    /// A worker session registers (or re-registers) under `worker_id`.
    ///
    /// `session_epoch` is the leader-assigned monotonic tag for this session.
    /// A `WorkerJoin` with a higher epoch than the currently-recorded session
    /// for the same `worker_id` *supersedes* it: in the SAME apply, every
    /// in-flight operation owned by the old session is requeued and its
    /// ownership cleared. Because supersession and reassignment are one
    /// deterministic function of one log entry, the ownership map can never be
    /// left pointing at a session the membership map has already replaced —
    /// the same-id `add_worker` orphan (CROSSED-OFF #7) is unrepresentable.
    WorkerJoin {
        worker_id: WorkerId,
        session_epoch: u64,
        now_unix_nanos: u128,
    },
    /// A worker session leaves. Its in-flight operations are requeued in the
    /// same apply. A `WorkerLeave` carrying a stale epoch (older than the
    /// recorded session) is ignored, so a late leave from a superseded session
    /// cannot disturb the session that replaced it.
    WorkerLeave {
        worker_id: WorkerId,
        session_epoch: u64,
        now_unix_nanos: u128,
    },
}

/// What `apply` returns to the caller that proposed the command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandResponse {
    /// The action after applying the command, if it exists. `revision` is the
    /// applied record revision after the mutation, so a caller can use it as its
    /// next `expected_revision`.
    Applied {
        awaited_action: Option<AwaitedAction>,
        revision: i64,
    },
    /// The command was committed but NOT applied: the transition was illegal
    /// against current state. The caller MUST treat this as a failed
    /// assignment/update, not a success. See Astra premortem S0.
    Rejected { reason: RejectReason },
    /// The set of operation ids that an `Expire` requeued.
    Expired { requeued: Vec<OperationId> },
    /// The result of a worker membership change: the operations that were
    /// requeued because their owning session was superseded or left, and
    /// whether the command took effect (an ignored stale command is `false`).
    WorkerMembership {
        applied: bool,
        requeued: Vec<OperationId>,
    },
}

declare_raft_types!(
    /// Single-node type config for the spike.
    pub TypeConfig:
        D = Command,
        R = CommandResponse,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Sanity: openraft can build its entry type over our config.
pub type Entry = <TypeConfig as RaftTypeConfig>::Entry;
