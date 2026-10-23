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

//! Test-only inputs and audit records; no provider or RPC proof is inferred.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_scheduler::authority as a;
use nativelink_store::physical_filesystem::{Algorithm, ObjectId};
use serde::{Deserialize, Serialize};

#[path = "real_request.rs"]
mod real_request;
pub(crate) use real_request::{ActionBinding, RealBindings, TrustedContext};

pub(crate) const MAX_STAGES: usize = 8;
pub(crate) const MAX_OPERATIONS: usize = 16;
pub(crate) const MAX_PHYSICAL: usize = 64;
pub(crate) const MAX_MEMBERS: usize = 8;
pub(crate) const MAX_GRANTS: usize = 16;
pub(crate) const MAX_PAYLOAD: usize = 4096;
pub(crate) const MAX_SNAPSHOT: usize = 2 * 1024 * 1024;
pub(crate) const LEASE_TICKS: u64 = 100;
pub(crate) const STAGE_TICKS: u64 = 10;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Object {
    pub(crate) algorithm: u8,
    pub(crate) hash: [u8; 32],
    pub(crate) bytes: u64,
}
impl Object {
    pub(crate) fn for_bytes(algorithm: u8, bytes: &[u8]) -> Result<Self, Error> {
        let algorithm_value = match algorithm {
            1 => Algorithm::Sha256,
            9 => Algorithm::Blake3,
            _ => return Err(Error::Invalid),
        };
        let object = ObjectId::for_bytes(algorithm_value, bytes);
        Ok(Self {
            algorithm,
            hash: object.hash,
            bytes: object.size,
        })
    }
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(
    variant_size_differences,
    reason = "Client provenance has no worker attempt identity."
)]
pub(crate) enum Publisher {
    Worker {
        execution: a::ExecutionId,
        attempt: a::AttemptId,
        worker: a::WorkerId,
        incarnation: a::IncarnationId,
    },
    Client {
        principal: u64,
    },
    /// Immutable trusted grant; real-mode principal and binding live in its ledger.
    AuthorizedClient {
        authorization: u64,
    },
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Candidate {
    /// Registered action label; real mode resolves its explicit qualified binding.
    pub(crate) action: String,
    pub(crate) publisher: Publisher,
    pub(crate) result: Object,
    pub(crate) result_bytes: Vec<u8>,
    pub(crate) response: a::ResultPayload,
    /// Supplied by an explicitly trusted typed closure preparer, never an RPC claim.
    pub(crate) closure: BTreeSet<Member>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Evidence {
    pub(crate) stage: u64,
    pub(crate) catalogue_revision: u64,
    pub(crate) closure: BTreeSet<Member>,
    pub(crate) fences: BTreeMap<u64, u64>,
    pub(crate) until: u64,
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
    pub(crate) candidate: Candidate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) authorized_at: Option<u64>,
    pub(crate) until: u64,
    pub(crate) root: u64,
    pub(crate) writes: BTreeMap<u64, Key>,
    pub(crate) internal_writes: BTreeMap<u64, u64>,
    pub(crate) outcomes: BTreeMap<u64, bool>,
    pub(crate) preparation: Option<Evidence>,
    pub(crate) state: StageState,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Accepted {
    pub(crate) stage: u64,
    pub(crate) commit: u64,
    pub(crate) at: u64,
    pub(crate) eligible: bool,
    pub(crate) root: u64,
    pub(crate) until: u64,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Operation {
    pub(crate) execution: a::ExecutionId,
    pub(crate) action: String,
    pub(crate) until: u64,
    pub(crate) publication: Option<u64>,
    pub(crate) root: Option<u64>,
    pub(crate) cached: bool,
    pub(crate) payload: Option<a::ResultPayload>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Grant {
    pub(crate) publication: u64,
    pub(crate) root: u64,
    pub(crate) until: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Error {
    Invalid,
    Bounds,
    Stale,
    Incomplete,
    Unauthorized,
    Unprotected,
    Conflict,
    Miss,
    Corrupt,
    Scheduler(a::Rejection),
}
