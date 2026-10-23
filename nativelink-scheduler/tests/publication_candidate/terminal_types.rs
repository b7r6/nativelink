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

//! Explicit no-graph reports and immutable acceptance records.

use std::collections::BTreeMap;

use nativelink_scheduler::authority as a;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WorkerOrigin {
    pub(crate) epoch: a::ClusterEpoch,
    pub(crate) action: String,
    pub(crate) execution: a::ExecutionId,
    pub(crate) attempt: a::AttemptId,
    pub(crate) worker: a::WorkerId,
    pub(crate) incarnation: a::IncarnationId,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum TerminalReportKind {
    Complete,
    MemoryFailure {
        charged: a::Resources,
        peak_memory_bytes: Option<u64>,
        quiescent: bool,
    },
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoGraphReport {
    pub(crate) origin: WorkerOrigin,
    pub(crate) kind: TerminalReportKind,
    pub(crate) result: a::ResultPayload,
}
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    dead_code,
    reason = "Older shared fixtures do not activate the terminal successor."
)]
pub(crate) enum NoGraphOutcome {
    Duplicate,
    RetryScheduled,
    TerminalAccepted(a::TerminalResult),
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum ReportDisposition {
    Retry { target: a::Resources },
    Terminal(a::TerminalResult),
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClassifiedReport {
    pub(crate) report: NoGraphReport,
    pub(crate) position: u64,
    pub(crate) at: u64,
    pub(crate) disposition: ReportDisposition,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NoGraphAcceptance {
    pub(crate) report_id: u64,
    pub(crate) origin: WorkerOrigin,
    pub(crate) terminal: a::TerminalResult,
    pub(crate) position: u64,
    pub(crate) at: u64,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TerminalState {
    pub(crate) enabled_at: u64,
    pub(crate) reports: BTreeMap<u64, ClassifiedReport>,
    pub(crate) accepted: BTreeMap<a::ExecutionId, NoGraphAcceptance>,
    pub(crate) operations: BTreeMap<u64, a::ExecutionId>,
}
