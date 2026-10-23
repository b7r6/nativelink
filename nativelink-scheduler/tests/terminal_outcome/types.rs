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

//! Neutral successor vocabulary; no production authority implementation.

use std::collections::BTreeMap;

use nativelink_scheduler::authority as a;
use serde::{Deserialize, Serialize};

use super::protocol as r;

pub(crate) const MAX_SNAPSHOT: usize = 2 * 1024 * 1024;
pub(crate) const MAX_EXECUTIONS: usize = 8;
pub(crate) const MAX_OPERATIONS: usize = 16;
pub(crate) const MAX_STAGES: usize = 8;
pub(crate) const MAX_GENERATIONS: usize = 64;
pub(crate) const MAX_MEMBERS: usize = 8;
pub(crate) const MAX_REPORTS: usize = 3;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Context {
    pub principal: String,
    pub execution_config: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QualifiedAction {
    pub key: a::ActionKey,
    pub context: Context,
    /// Selected immutable platform, including resource properties before their
    /// scheduling projection. Dynamic retry grants are not action identity.
    pub semantics: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Physical {
    pub namespace: [u8; 16],
    pub incarnation: [u8; 16],
    pub generation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Seed {
    pub kind: r::RootKind,
    pub members: Vec<Physical>,
    pub until: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerIdentity {
    pub attempt: a::AttemptId,
    pub worker: a::WorkerId,
    pub incarnation: a::IncarnationId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Classification {
    Complete,
    MemoryFailure {
        original_charged: a::Resources,
        peak_memory_bytes: Option<u64>,
        quiescent: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Report {
    pub id: u64,
    pub identity: WorkerIdentity,
    pub classification: Classification,
    pub payload: a::ResultPayload,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Origin {
    pub epoch: a::ClusterEpoch,
    pub position: u64,
    pub now: u64,
    pub execution: a::ExecutionId,
    pub identity: WorkerIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Acceptance {
    pub report: u64,
    pub origin: Origin,
    pub terminal: a::TerminalResult,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Disposition {
    RetryScheduled,
    TerminalAccepted(Acceptance),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReportRecord {
    pub report: Report,
    pub execution: a::ExecutionId,
    pub disposition: Disposition,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum OperationLink {
    Waiting,
    NoGraph(u64),
    /// Cancellation/exhaustion is not a worker-accepted response.
    Independent(a::TerminalResult),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Credit {
    pub pending_stage: bool,
    pub unused_members: usize,
    pub attempt: Option<a::AttemptId>,
    pub stages: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stage {
    pub id: u64,
    pub execution: a::ExecutionId,
    pub attempt: a::AttemptId,
    pub root: r::RootId,
    pub members: Vec<Physical>,
    pub writes: Vec<r::WriteId>,
    pub abandoned: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Command {
    Admit {
        operation: a::OperationId,
        request: a::Request,
    },
    Scheduler(a::Event),
    Report(Report),
    Stage {
        id: u64,
        attempt: a::AttemptId,
        members: Vec<Physical>,
        until: u64,
    },
    WriteDone {
        write: r::WriteId,
        success: bool,
    },
    Abandon(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Reply {
    Scheduler(a::Reply),
    Applied,
    Ignored,
    Duplicate,
    Report(Disposition),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    Scheduler(a::Rejection),
    Invalid,
    Unauthorized,
    GraphBearing,
    Capacity,
    Conflict,
    Unsupported,
    Snapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct View {
    pub position: u64,
    pub scheduler: a::ObservableState,
    pub retention: r::View,
    pub original_requests: BTreeMap<a::OperationId, a::Request>,
    pub actions: BTreeMap<a::ExecutionId, QualifiedAction>,
    pub reports: BTreeMap<u64, ReportRecord>,
    pub accepted: BTreeMap<a::ExecutionId, Acceptance>,
    pub operations: BTreeMap<a::OperationId, OperationLink>,
    pub credits: BTreeMap<a::ExecutionId, Credit>,
    pub stages: BTreeMap<u64, Stage>,
    pub physical: Vec<(Physical, r::Key)>,
}
