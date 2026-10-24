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

pub(crate) const MAX_TERMINALS: usize = 8;
pub(crate) const MAX_EFFECTS: usize = 16;
pub(crate) const MAX_ROOTS: usize = 16;
pub(crate) const MAX_MEMBERS: usize = 32;
pub(crate) const MAX_REQUESTS: usize = 32;
pub(crate) const MAX_PAYLOAD_BYTES: usize = 4096;
pub(crate) const MAX_SNAPSHOT_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct Namespace(pub(crate) [u8; 16]);

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
pub(crate) struct Generation {
    pub(crate) namespace: Namespace,
    pub(crate) incarnation: [u8; 16],
    pub(crate) number: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum Role {
    Result,
    History,
    Blob,
    Directory,
    Tree,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Member {
    pub(crate) object: Object,
    pub(crate) generation: Generation,
    pub(crate) role: Role,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Origin {
    pub(crate) epoch: u64,
    pub(crate) accepted_commit: u64,
    pub(crate) execution: u64,
    pub(crate) attempt: u64,
    pub(crate) worker: u64,
    pub(crate) incarnation: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Terminal {
    pub(crate) id: u64,
    pub(crate) action: String,
    pub(crate) origin: Origin,
    pub(crate) operations: BTreeSet<u64>,
    pub(crate) response: Vec<u8>,
    pub(crate) result: Object,
    pub(crate) result_bytes: Vec<u8>,
    pub(crate) ac_eligible: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum Delivery {
    Issued,
    Unknown,
    Stored,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Exposure {
    Opaque,
    ProvenQuiescent,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SinkEffect {
    pub(crate) id: u64,
    pub(crate) terminal: u64,
    pub(crate) destination: Namespace,
    pub(crate) exposure: Exposure,
    pub(crate) payload: Vec<u8>,
    pub(crate) requests: BTreeMap<u64, Delivery>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryEffect {
    pub(crate) id: u64,
    pub(crate) terminal: u64,
    pub(crate) origin: Origin,
    pub(crate) destination: Namespace,
    pub(crate) exposure: Exposure,
    pub(crate) object: Object,
    pub(crate) payload: Vec<u8>,
    pub(crate) requests: BTreeMap<u64, Delivery>,
    pub(crate) materialized: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum Subject {
    Result(u64),
    History(u64),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    pub(crate) id: u64,
    pub(crate) subject: Subject,
    pub(crate) object: Object,
    /// Complete typed closure supplied independently by the scenario.
    pub(crate) required: BTreeSet<Member>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Legacy {
    pub(crate) old_sink: Namespace,
    pub(crate) domain_exposure: Exposure,
    pub(crate) terminals: BTreeMap<u64, Terminal>,
    pub(crate) effects: BTreeMap<u64, SinkEffect>,
    pub(crate) histories: BTreeMap<u64, HistoryEffect>,
    pub(crate) roots: BTreeMap<u64, Root>,
    pub(crate) late_outcomes: BTreeSet<Observation>,
    pub(crate) request_quiescence: BTreeSet<RequestRef>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum EffectKind {
    Sink,
    History,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EffectRef {
    pub(crate) kind: EffectKind,
    pub(crate) effect: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RequestRef {
    pub(crate) kind: EffectKind,
    pub(crate) effect: u64,
    pub(crate) request: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Observation {
    pub(crate) request: RequestRef,
    pub(crate) outcome: Delivery,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Readiness {
    Ready,
    Writing,
    Retired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogueRow {
    pub(crate) key: Generation,
    pub(crate) object: Object,
    pub(crate) state: Readiness,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fence {
    pub(crate) namespace: Namespace,
    pub(crate) revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Protection {
    pub(crate) id: u64,
    pub(crate) root: u64,
    pub(crate) catalogue_revision: u64,
    pub(crate) complete: bool,
    pub(crate) verified: bool,
    pub(crate) typed: BTreeSet<Member>,
    pub(crate) protected: BTreeSet<Generation>,
    pub(crate) fences: BTreeSet<Fence>,
    pub(crate) until: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub(crate) namespace: Namespace,
    /// Independently qualified destination identity; aliases share this value.
    pub(crate) physical_domain: [u8; 32],
    pub(crate) old_requests_confined: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Facts {
    pub(crate) now: u64,
    pub(crate) catalogue_revision: u64,
    pub(crate) catalogue: Vec<CatalogueRow>,
    pub(crate) fences: BTreeSet<Fence>,
    pub(crate) protections: BTreeMap<u64, Protection>,
    pub(crate) bindings: Vec<Binding>,
    pub(crate) cold: BTreeSet<Namespace>,
    pub(crate) migration_publishers: BTreeSet<u64>,
    pub(crate) quiescent: BTreeSet<RequestRef>,
    /// Authorized evidence covering every possibly issued request, even unenumerated ones.
    pub(crate) quiescent_effects: BTreeSet<EffectRef>,
    pub(crate) quiescent_domains: BTreeSet<Namespace>,
    /// Deliberately irrelevant to authority already recorded at acceptance.
    pub(crate) current_workers: BTreeSet<(u64, u64)>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Disposition {
    Settled,
    Converted { publication: u64 },
    Retired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Import {
    pub(crate) publication: u64,
    pub(crate) terminal: u64,
    pub(crate) publisher: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Activation {
    pub(crate) commit: u64,
    pub(crate) new_namespace: Namespace,
    pub(crate) dispositions: BTreeMap<u64, Disposition>,
    pub(crate) imports: Vec<Import>,
    pub(crate) protections: BTreeMap<u64, u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Head {
    pub(crate) action: String,
    pub(crate) publication: u64,
    pub(crate) terminal: u64,
    pub(crate) publisher: u64,
    pub(crate) origin: Origin,
    pub(crate) commit: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Event {
    Activate(Activation),
    Observe {
        request: RequestRef,
        destination: Namespace,
        object: Object,
        outcome: Delivery,
    },
    Quiesce(RequestRef),
    QuiesceEffect(EffectRef),
    QuiesceDomain(Namespace),
    Protect {
        root: u64,
        evidence: u64,
    },
    EnableCollection {
        namespace: Namespace,
        through: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Outcome {
    Applied,
    Ignored,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Reject {
    Invalid,
    Bounds,
    Incomplete,
    Namespace,
    Unprotected,
    Stale,
    Unauthorized,
    WrongPhase,
    Uncertain,
    Conflict,
}
