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

//! Independent contract types. No candidate scheduler types or helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub(crate) const MAX_RESPONSE_BYTES: usize = 256 * 1024;
pub(crate) const MAX_PREFIX_BYTES: usize = 82;
/// Conservative metadata credit, checked against a maximal serialized row.
pub(crate) const ROW_CREDIT: usize = 2048;
pub(crate) const HEADER_CREDIT: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub(crate) struct Namespace(pub(crate) [u8; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum Algorithm {
    Sha256,
    Blake3,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ActionDigest {
    pub(crate) algorithm: Algorithm,
    pub(crate) hash: [u8; 32],
    /// Nonnegative protobuf int64; values above `i64::MAX` are invalid.
    pub(crate) size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ObjectKey {
    pub(crate) namespace: Namespace,
    pub(crate) algorithm: Algorithm,
    pub(crate) hash: [u8; 32],
    pub(crate) size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Recipe {
    pub(crate) prefix: Vec<u8>,
    pub(crate) response_sha256: [u8; 32],
    pub(crate) object: ObjectKey,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Binding {
    pub(crate) attempt: u64,
    pub(crate) worker: u64,
    pub(crate) incarnation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum PublicationState {
    Pending,
    Materialized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Association {
    pub(crate) epoch: u64,
    pub(crate) effect: u64,
    pub(crate) execution: u64,
    pub(crate) original: Binding,
    pub(crate) recipe: Recipe,
    pub(crate) state: PublicationState,
}

/// These facts belong to an independent lifecycle oracle or fixture. They
/// must never be copied out of the candidate's `ObservableState`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    Open,
    /// Includes successful, failing, noncacheable, and terminal infra results.
    WorkerTerminal {
        original: Binding,
        response: Arc<[u8]>,
        cacheable: bool,
        success: bool,
    },
    /// Cache hit, direct-client publication, cancellation, or other terminal.
    OtherTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutionFacts {
    pub(crate) action: ActionDigest,
    pub(crate) phase: Phase,
    /// Authorization immediately before the event; absent after revocation.
    pub(crate) authorized: Option<Binding>,
    /// Operations interested at terminal acceptance; cancelled handles are absent.
    /// This survives the lifecycle clearing its live-interest bits.
    pub(crate) accepted_operations: BTreeSet<u64>,
    pub(crate) retirement_permitted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Facts {
    pub(crate) epoch: u64,
    /// Namespace is independently qualified once, not chosen by a worker.
    pub(crate) qualified_namespace: Option<Namespace>,
    pub(crate) executions: BTreeMap<u64, ExecutionFacts>,
    /// Next globally available ID *after* any preceding AC effect allocation.
    pub(crate) next_effect: u64,
    /// All non-history future obligations, including receipt retirement.
    pub(crate) other_future_effect_ids: u64,
    pub(crate) effect_capacity: usize,
    /// Live effects plus prepaid effect slots outside this component.
    pub(crate) other_effect_slots: usize,
    pub(crate) metadata_capacity: usize,
    pub(crate) other_metadata_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Event {
    Activate {
        namespace: Namespace,
    },
    Admit {
        execution: u64,
    },
    ReleaseUnused {
        execution: u64,
    },
    AcceptTerminal {
        execution: u64,
        /// Optional externally prepared proposal is checked, never trusted.
        proposal: Option<Recipe>,
    },
    ObservePublication {
        epoch: u64,
        effect: u64,
        object: ObjectKey,
        outcome: PublicationOutcome,
    },
    Retire {
        execution: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationOutcome {
    Stored,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    Applied,
    Ignored,
    Allocated { effect: u64, next_effect: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reject {
    Disabled,
    Namespace,
    Facts,
    Capacity,
    Headroom,
    InvalidRecipe,
    Busy,
    Snapshot,
}
