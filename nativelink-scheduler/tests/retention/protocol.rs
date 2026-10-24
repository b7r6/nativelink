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

//! Neutral, bounded retention experiment. These are neither production types
//! nor proof that the supplied flat closures contain every typed strong edge.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
        pub(crate) struct $name(pub u64);
    };
}

identifier!(CohortId);
identifier!(RootId);
identifier!(WriteId);
identifier!(DeleteId);

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Key {
    pub incarnation: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Limits {
    pub cohorts: usize,
    pub generations: usize,
    pub roots: usize,
    pub closure: usize,
    pub mark_batch: usize,
    pub retire_batch: usize,
    pub writes: usize,
    pub delete_intents: usize,
    pub lease_ticks: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            cohorts: 4,
            generations: 16,
            roots: 16,
            closure: 8,
            mark_batch: 4,
            retire_batch: 4,
            writes: 32,
            delete_intents: 48,
            lease_ticks: 100,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum RootKind {
    Staging,
    Publication,
    Serving,
    Operation,
    Input,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Phase {
    Allocating,
    Sealed,
    Closing,
    Closed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapturedRoot {
    pub root: RootId,
    pub lease: u64,
    pub closure: Vec<Key>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    pub lease: u64,
    pub kind: RootKind,
    pub closure: Vec<Key>,
    pub until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Cohort {
    pub phase: Phase,
    pub fence: u64,
    pub captured: Vec<CapturedRoot>,
    pub cursor: usize,
    pub marked: BTreeSet<Key>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Generation {
    pub logical_object: u64,
    pub cohort: CohortId,
    pub ready: bool,
    pub retired: bool,
    pub write_revision: u64,
    pub current_delete: Option<DeleteId>,
    pub delete_confirmed_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Write {
    pub key: Key,
    /// None includes ambiguous provider outcomes. Some is known quiescent.
    pub outcome: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Delete {
    pub key: Key,
    pub write_revision: u64,
    pub confirmed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct View {
    pub limits: Limits,
    pub now: u64,
    pub next_delete: u64,
    pub cohorts: BTreeMap<CohortId, Cohort>,
    pub generations: BTreeMap<Key, Generation>,
    pub roots: BTreeMap<RootId, Root>,
    pub writes: BTreeMap<WriteId, Write>,
    pub deletes: BTreeMap<DeleteId, Delete>,
    pub protected: BTreeSet<Key>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Event {
    AdvanceTime {
        now: u64,
    },
    CreateCohort {
        cohort: CohortId,
    },
    Allocate {
        cohort: CohortId,
        key: Key,
        logical_object: u64,
    },
    Seal {
        cohort: CohortId,
    },
    BeginClose {
        cohort: CohortId,
    },
    FinishClose {
        cohort: CohortId,
        fence: u64,
    },
    Mark {
        cohort: CohortId,
        fence: u64,
        at: usize,
        roots: Vec<CapturedRoot>,
    },
    Retire {
        cohort: CohortId,
        fence: u64,
        keys: Vec<Key>,
    },
    Reopen {
        cohort: CohortId,
        fence: u64,
    },
    Acquire {
        root: RootId,
        lease: u64,
        kind: RootKind,
        closure: Vec<Key>,
        until: u64,
        fences: BTreeMap<CohortId, u64>,
    },
    Renew {
        root: RootId,
        lease: u64,
        until: u64,
    },
    Release {
        root: RootId,
        lease: u64,
    },
    Transfer {
        staging: RootId,
        lease: u64,
        publication: RootId,
        until: u64,
    },
    StartWrite {
        write: WriteId,
        key: Key,
        staging: RootId,
        lease: u64,
    },
    WriteDone {
        write: WriteId,
        success: bool,
    },
    DeleteDone {
        delete: DeleteId,
        success: bool,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Reply {
    Applied,
    Ignored,
    Rejected,
}
