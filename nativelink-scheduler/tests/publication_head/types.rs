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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub(crate) const MAX_OPERATIONS: usize = 16;
pub(crate) const MAX_EXECUTIONS: usize = 8;
pub(crate) const MAX_STAGES: usize = 8;
pub(crate) const MAX_GRANTS: usize = 16;
pub(crate) const MAX_MEMBERS: usize = 8;
pub(crate) const MAX_PAYLOAD: usize = 4096;
pub(crate) const MAX_SNAPSHOT: usize = 2 * 1024 * 1024;
pub(crate) const STAGE_TICKS: u64 = 10;
pub(crate) const LEASE_TICKS: u64 = 100;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum Algorithm {
    Sha256,
    Blake3,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Object {
    pub(crate) algorithm: Algorithm,
    pub(crate) hash: [u8; 32],
    pub(crate) bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Key {
    pub(crate) namespace: [u8; 16],
    pub(crate) incarnation: [u8; 16],
    pub(crate) generation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum Role {
    Result,
    Blob,
    Directory,
    Tree,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Member {
    pub(crate) object: Object,
    pub(crate) key: Key,
    pub(crate) role: Role,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Attempt {
    pub(crate) id: u64,
    pub(crate) worker: u64,
    pub(crate) incarnation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(
    variant_size_differences,
    reason = "Worker provenance includes an attempt; client provenance must not fabricate one."
)]
pub(crate) enum Source {
    Worker { execution: u64, attempt: Attempt },
    Client { principal: u64 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Candidate {
    pub(crate) id: u64,
    /// Opaque fully qualified action key: all identity fields are significant.
    pub(crate) action: String,
    pub(crate) source: Source,
    pub(crate) result: Object,
    pub(crate) bytes: Vec<u8>,
    /// Expected operation encoding owned by the scenario, not a direct-update RPC field.
    pub(crate) response: Vec<u8>,
    pub(crate) successful: bool,
    pub(crate) error_free: bool,
    /// Independently known complete typed closure, never supplied by a candidate engine.
    pub(crate) required: BTreeSet<Member>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Scenario {
    /// Immutable action cacheability; true means `Action.do_not_cache`.
    pub(crate) actions: BTreeMap<String, bool>,
    pub(crate) candidates: BTreeMap<u64, Candidate>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fence {
    pub(crate) namespace: [u8; 16],
    pub(crate) revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Preparation {
    pub(crate) id: u64,
    pub(crate) stage: u64,
    pub(crate) result: Object,
    pub(crate) typed: BTreeSet<Member>,
    pub(crate) protected: BTreeSet<Key>,
    pub(crate) catalogue_revision: u64,
    pub(crate) fences: BTreeSet<Fence>,
    pub(crate) complete: bool,
    pub(crate) verified: bool,
    pub(crate) until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Physical {
    pub(crate) key: Key,
    pub(crate) object: Object,
    pub(crate) ready: bool,
    pub(crate) retired: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Receipt {
    pub(crate) stage: u64,
    pub(crate) write: u64,
    pub(crate) key: Key,
    pub(crate) object: Object,
    pub(crate) success: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Facts {
    pub(crate) catalogue_revision: u64,
    pub(crate) physical: Vec<Physical>,
    pub(crate) fences: BTreeSet<Fence>,
    pub(crate) receipts: BTreeSet<Receipt>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    pub(crate) operation: u64,
    pub(crate) action: String,
    pub(crate) skip_lookup: bool,
    pub(crate) execution_if_needed: u64,
    pub(crate) result_until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Terminal {
    pub(crate) publication: u64,
    pub(crate) response: Vec<u8>,
    pub(crate) commit: u64,
    pub(crate) cached: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum OperationState {
    Waiting(u64),
    Terminal(Terminal),
    Canceled(u64),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Operation {
    pub(crate) request: Request,
    pub(crate) state: OperationState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Execution {
    pub(crate) action: String,
    pub(crate) attempt: Option<Attempt>,
    pub(crate) accepted: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum StageState {
    Open,
    Abandoned,
    Accepted(u64),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stage {
    pub(crate) candidate: u64,
    pub(crate) until: u64,
    pub(crate) writes: BTreeMap<u64, Key>,
    pub(crate) outcomes: BTreeMap<u64, bool>,
    pub(crate) preparation: Option<Preparation>,
    pub(crate) state: StageState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Accepted {
    pub(crate) stage: u64,
    pub(crate) candidate: u64,
    pub(crate) commit: u64,
    pub(crate) source: Source,
    pub(crate) eligible: bool,
    pub(crate) until: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum Root {
    Stage(u64),
    Result(u64),
    Operation(u64),
    Serving(u64),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Coverage {
    pub(crate) root: Root,
    pub(crate) keys: BTreeSet<Key>,
    pub(crate) until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grant {
    pub(crate) publication: u64,
    pub(crate) selected_at: u64,
    pub(crate) until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Event {
    Advance(u64),
    Register {
        worker: u64,
        incarnation: u64,
    },
    Revoke {
        worker: u64,
        incarnation: u64,
    },
    AuthorizeClient(u64),
    Admit(Request),
    CancelJoined(u64),
    Assign {
        execution: u64,
        attempt: Attempt,
    },
    Stage {
        id: u64,
        candidate: u64,
        until: u64,
        writes: BTreeMap<u64, Key>,
    },
    Observe(Receipt),
    Prepare {
        stage: u64,
        evidence: Preparation,
    },
    Abandon(u64),
    ExpireStage(u64),
    Publish {
        stage: u64,
        publication: u64,
        until: u64,
    },
    Materialize(u64),
    LoseIndex,
    RebuildIndex,
    Serve {
        action: String,
        expected: u64,
        barrier: u64,
        grant: u64,
        until: u64,
    },
    Reprotect {
        publication: u64,
        evidence: Preparation,
        until: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reply {
    Applied,
    Ignored,
    Started(u64),
    Joined(u64),
    Hit(u64),
    Granted(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reject {
    Invalid,
    Bounds,
    Conflict,
    Stale,
    Unauthorized,
    Unprotected,
    Incomplete,
    WrongPhase,
    Miss,
}
