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

use std::collections::BTreeMap;

use nativelink_scheduler::authority as a;
use serde::{Deserialize, Serialize};

use super::head::types as h;
pub(crate) use super::terminal_types::{
    Classification, Context, QualifiedAction, Report, WorkerIdentity,
};

pub(crate) const MAX_EVENTS: usize = 192;
pub(crate) const MAX_SNAPSHOT: usize = 2 * 1024 * 1024;
// Common-alphabet admissions all retain their operation through this tick.
pub(crate) const OPERATION_UNTIL: u64 = 100;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stage {
    pub candidate: u64,
    pub report: Report,
    pub epoch: a::ClusterEpoch,
    pub until: u64,
    pub writes: BTreeMap<u64, h::Key>,
    pub outcomes: BTreeMap<u64, bool>,
    pub preparation: Option<h::Preparation>,
    pub abandoned: bool,
    pub classified: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptanceArguments {
    pub stage: u64,
    pub publication: u64,
    pub until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Disposition {
    RetryScheduled { target: a::Resources },
    TerminalAccepted { terminal: a::TerminalResult },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Classified {
    pub report: Report,
    pub execution: a::ExecutionId,
    pub position: u64,
    pub now: u64,
    pub arguments: Option<AcceptanceArguments>,
    pub disposition: Disposition,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Accepted {
    pub report: u64,
    pub execution: a::ExecutionId,
    pub position: u64,
    pub now: u64,
    pub terminal: a::TerminalResult,
    pub publication: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Credit {
    pub pending: bool,
    pub attempt: Option<a::AttemptId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Command {
    Admit {
        operation: a::OperationId,
        request: a::Request,
    },
    Scheduler(a::Event),
    Stage {
        id: u64,
        candidate: u64,
        report: Report,
        epoch: a::ClusterEpoch,
        until: u64,
        writes: BTreeMap<u64, h::Key>,
    },
    Observe(h::Receipt),
    Prepare {
        stage: u64,
        evidence: h::Preparation,
    },
    Accept(AcceptanceArguments),
    NoGraph(Report),
    Abandon(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Reply {
    Scheduler(a::Reply),
    Applied,
    Duplicate,
    Classified(Disposition),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Refusal {
    Invalid,
    Unsupported,
    Unauthorized,
    Conflict,
    Capacity,
    Stale,
    Unprotected,
    Incomplete,
    Scheduler(a::Rejection),
    Snapshot,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Step {
    pub command: Command,
    pub facts: h::Facts,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct View {
    pub position: u64,
    pub scheduler: a::ObservableState,
    pub actions: BTreeMap<a::ExecutionId, QualifiedAction>,
    pub originals: BTreeMap<a::OperationId, a::Request>,
    pub stages: BTreeMap<u64, Stage>,
    pub classified: BTreeMap<u64, Classified>,
    pub accepted: BTreeMap<a::ExecutionId, Accepted>,
    pub operations: BTreeMap<a::OperationId, u64>,
    pub roots: Vec<h::Coverage>,
    pub heads: BTreeMap<String, u64>,
    pub credits: BTreeMap<a::ExecutionId, Credit>,
}
