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
use openraft::{RaftTypeConfig, declare_raft_types};
use serde::{Deserialize, Serialize};

/// The node id in the cluster. A `u64` is all a single-voter spike needs.
pub type NodeId = u64;

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
    },
    /// Transition an existing action to a new worker-reported state.
    ///
    /// Note there is no `version` field: the log index is the version. Two
    /// concurrent updates cannot both be applied to the same prior state
    /// because raft linearizes them, so the lost-update the version-CAS was
    /// guarding against cannot occur.
    UpdateAwaitedAction {
        operation_id: OperationId,
        new_state: Arc<ActionState>,
        worker_id: Option<WorkerId>,
        now_unix_nanos: u128,
    },
    /// A worker session heartbeat. Refreshes the lease recorded in the log.
    Heartbeat {
        operation_id: OperationId,
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
}

/// What `apply` returns to the caller that proposed the command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandResponse {
    /// The action after applying the command, if it exists.
    Applied {
        awaited_action: Option<AwaitedAction>,
    },
    /// The set of operation ids that an `Expire` requeued.
    Expired { requeued: Vec<OperationId> },
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
