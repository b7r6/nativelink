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

//! Shared input/output vocabulary for the experimental deterministic authority.
//!
//! These types are deliberately independent of the reducer and may be shared by
//! an independently implemented oracle. None of these types performs matching,
//! accounting, state transitions, or snapshot reconstruction.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

macro_rules! identity {
    ($($name:ident),+ $(,)?) => {$(
        #[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        pub struct $name(pub u64);
    )+};
}
identity!(
    ClusterEpoch,
    ExecutionId,
    OperationId,
    AttemptId,
    WorkerId,
    IncarnationId,
    SessionId,
    ReservationId,
    EffectId
);

pub const ACK_TIMEOUT: u64 = 10;
pub const PROGRESS_TIMEOUT: u64 = 30;
pub const INTEREST_TIMEOUT: u64 = 300;
pub const TERMINAL_RETENTION: u64 = 300;
pub const MAX_ATTEMPTS: u32 = 3;
/// Fixed experimental canonical state bound, including prepaid finalization.
pub const MAX_STATE_BYTES: usize = 14 * 1024 * 1024;
/// Complete encoded request/result/property bound. Together with the bounded
/// command frame this fits a singleton command in the journal's 1 MiB batch.
pub const MAX_PAYLOAD_BYTES: usize = 512 * 1024;
/// Fixed experimental scheduling intervals, in the same virtual ticks as time.
pub const HOLD_AFTER: u64 = 30;
pub const HOLD_DURATION: u64 = 300;
pub const HOLD_COOLDOWN: u64 = 300;

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActionKey {
    pub instance: String,
    pub digest_function: String,
    pub digest: String,
    pub digest_size_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub slots: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub action: ActionKey,
    /// Buck2 invocation identity; intentionally absent from the AC key.
    pub execution_scope: Option<String>,
    pub do_not_cache: bool,
    pub skip_cache_lookup: bool,
    pub priority: i32,
    pub properties: BTreeMap<String, String>,
    pub resources: Resources,
    /// Applies only to Running, not queueing, dispatch, or input preparation.
    pub execution_timeout: u64,
    /// Captured ingress facts; None is reserved for synthetic model fixtures.
    /// Real ingress must call `require_payload()` and reuse its initial capture
    /// on retries, including the original queued timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Box<super::request::RequestPayload>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResultPayload {
    pub digest: String,
    pub exit_code: i32,
    pub error: Option<String>,
    /// Full captured response for real adapters. None is reserved for synthetic
    /// model fixtures; canonical adapters must call `require_response()`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<super::reapi::ResponsePayload>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum TerminalResult {
    Completed(ResultPayload),
    /// Original failure is preserved. The policy reason is internal authority
    /// state; a canonical adapter must define its diagnostic projection before
    /// claiming clients receive it.
    ResourceFailed {
        result: ResultPayload,
        reason: ResourceFailureReason,
    },
    Cancelled,
    Exhausted,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum ResourceFailureReason {
    AttemptBudgetExhausted,
    MissingGrowthEvidence,
    NoLargerEligibleWorker,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum Phase {
    Lookup,
    Queued,
    Dispatching,
    Accepted,
    Preparing,
    Running,
    Uploading,
    Terminal,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Deadline {
    pub at: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_executions: usize,
    pub max_operations: usize,
    pub max_workers: usize,
    /// Includes reserved finalization headroom, not merely current outbox size.
    pub max_effects: usize,
    /// Bounds every encoded request, result, and property map. Initialization
    /// rejects values above `MAX_PAYLOAD_BYTES`; count limits and this limit are
    /// upper bounds, subject to the aggregate prepaid state byte limit.
    pub max_payload_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_executions: 64,
            max_operations: 256,
            max_workers: 32,
            max_effects: 512,
            max_payload_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedEvent {
    pub epoch: ClusterEpoch,
    pub event: Event,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum Event {
    Register {
        worker: WorkerId,
        incarnation: IncarnationId,
        session: SessionId,
        properties: BTreeMap<String, String>,
        capacity: Resources,
        available: Resources,
    },
    Capacity {
        worker: WorkerId,
        incarnation: IncarnationId,
        session: SessionId,
        capacity: Resources,
        available: Resources,
        draining: bool,
    },
    Heartbeat {
        worker: WorkerId,
        incarnation: IncarnationId,
        session: SessionId,
    },
    /// Fenced loss closes admission and revokes authority without proving that
    /// processes stopped. Original reservations remain charged until cleanup.
    WorkerUnavailable {
        worker: WorkerId,
        incarnation: IncarnationId,
        session: SessionId,
    },
    /// IDs model a monotonic authority ingress sequence, not a client-chosen
    /// arbitrary number. Accepted IDs are never reused after record expiry.
    Submit {
        operation: OperationId,
        request: Request,
    },
    LookupResult {
        effect: EffectId,
        result: Option<ResultPayload>,
    },
    /// One deterministic placement opportunity, independent of event delivery.
    Match,
    WorkerAccepted {
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    },
    Progress {
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        phase: Phase,
        progress: u64,
    },
    Complete {
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        result: ResultPayload,
    },
    /// Explicit memory exhaustion, authenticated and classified by the adapter.
    /// Exit codes and message text do not establish this outcome. Confirmed
    /// quiescence and the exact original grant are mandatory for acceptance.
    MemoryFailure {
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        charged: Resources,
        peak_memory_bytes: Option<u64>,
        quiescent: bool,
        result: ResultPayload,
    },
    Decline {
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    },
    Cancel {
        operation: OperationId,
    },
    /// Observation reconnect neither creates nor removes interested handles.
    Observe {
        operation: OperationId,
    },
    RenewInterest {
        operation: OperationId,
    },
    AdvanceTime {
        now: u64,
    },
    ExpireAttempt {
        attempt: AttemptId,
        generation: u64,
    },
    /// Scheduled once on entry into Running. Unlike inactivity expiry, this
    /// deadline cannot be renewed by progress. Attempt identity fences stale
    /// timers across retries and incarnation replacement.
    ExpireExecution {
        attempt: AttemptId,
    },
    ExpireInterest {
        operation: OperationId,
        generation: u64,
    },
    /// Confirms physical quiescence and recipient closure against delayed dispatch.
    /// The worker retains an attempt tombstone before producing this evidence;
    /// revocation alone never releases resources.
    Cleanup {
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    },
    /// Failed/ambiguous delivery leaves the original intent intact. Delivered
    /// dispatch/cancel intents remain until acceptance/cleanup respectively.
    EffectOutcome {
        effect: EffectId,
        success: bool,
    },
    /// Explicit expiry of retained terminal records; refuses pending effects or cleanup.
    RetireExecution {
        execution: ExecutionId,
    },
    /// Commit one immutable certificate only when the preceding acknowledgement
    /// matches. Replayed proposals cannot create a successor after an intervening
    /// acknowledgement. Current pending certificates are retried without change.
    IssueReceipts {
        worker: WorkerId,
        incarnation: IncarnationId,
        after_ack: Option<EffectId>,
    },
    /// The adapter authenticates the current worker session. Exact certificate
    /// equality confirms compaction delivery only, never physical quiescence.
    AcknowledgeReceipts {
        worker: WorkerId,
        incarnation: IncarnationId,
        certificate: ReceiptCertificate,
    },
    /// Exact committed diagnostic-object observation. This acknowledges only
    /// materialization, never completion, cleanup, or a retention guarantee.
    HistoryPublication {
        effect: EffectId,
        object: super::accepted_history::HistoryObject,
        outcome: super::accepted_history::HistoryOutcome,
    },
}

/// A receipt exception binds every identity domain that recipient compaction
/// would otherwise forget. This is evidence for retirement, never a work grant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptBinding {
    pub attempt: AttemptId,
    pub execution: ExecutionId,
    pub dispatch_effect: EffectId,
    pub reservation: ReservationId,
    pub cancel_effect: Option<EffectId>,
}

/// One immutable committed recipient-retirement intent. Its stable effect ID
/// shares the allocator used by dispatch, cancellation, and publication.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptCertificate {
    pub effect: EffectId,
    pub attempts_through: AttemptId,
    pub effects_through: EffectId,
    pub reservations_through: ReservationId,
    pub retain: Vec<ReceiptBinding>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptFloors {
    pub effect: EffectId,
    pub attempts_through: AttemptId,
    pub effects_through: EffectId,
    pub reservations_through: ReservationId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptStream {
    pub incarnation: IncarnationId,
    pub acknowledged: Option<ReceiptFloors>,
    pub pending: Option<ReceiptCertificate>,
    /// New committed releases since the last issued certificate. Acknowledging
    /// that certificate cannot clear releases that happened while it was pending.
    pub dirty: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptRetirement {
    pub streams: BTreeMap<WorkerId, ReceiptStream>,
}

pub const MAX_RECEIPT_BINDINGS: usize = 1024;
/// JSON's named fields cost more than the separately bounded 64 KiB protobuf.
pub const MAX_RECEIPT_CERTIFICATE_BYTES: usize = 256 * 1024;

impl Event {
    #[must_use]
    pub const fn requires_accepted_history(&self) -> bool {
        matches!(self, Self::HistoryPublication { .. })
    }
    #[must_use]
    pub const fn requires_receipt_retirement(&self) -> bool {
        matches!(
            self,
            Self::IssueReceipts { .. } | Self::AcknowledgeReceipts { .. }
        )
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum Rejection {
    WrongEpoch,
    Unknown,
    Stale,
    Invalid,
    Overloaded,
    TooEarly,
    Busy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum Reply {
    Applied,
    Ignored,
    Rejected(Rejection),
    Submitted {
        operation: OperationId,
        execution: ExecutionId,
    },
    Matched {
        execution: ExecutionId,
        attempt: AttemptId,
        worker: WorkerId,
    },
    Observed(OperationView),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum EffectKind {
    Lookup {
        execution: ExecutionId,
        action: ActionKey,
    },
    Dispatch {
        execution: ExecutionId,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
        reservation: ReservationId,
        /// This attempt's original grant; it may exceed immutable client minima.
        charged: Resources,
        request: Request,
    },
    Cancel {
        execution: ExecutionId,
        attempt: AttemptId,
        worker: WorkerId,
        incarnation: IncarnationId,
    },
    /// M1 handoff only. No production sink is connected to this intent.
    PublishResult {
        execution: ExecutionId,
        action: ActionKey,
        result: ResultPayload,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Effect {
    pub id: EffectId,
    pub epoch: ClusterEpoch,
    pub kind: EffectKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationView {
    pub id: OperationId,
    pub execution: ExecutionId,
    pub priority: i32,
    pub interested: bool,
    pub terminal: Option<TerminalResult>,
    pub version: u64,
    pub interest_deadline: Deadline,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionView {
    pub id: ExecutionId,
    pub request: Request,
    /// Current grant target. Only accepted memory-failure evidence can increase
    /// memory; CPU, slots, and the original request remain unchanged.
    pub reservation_target: Resources,
    pub phase: Phase,
    pub effective_priority: Option<i32>,
    pub enqueue_ordinal: u64,
    /// Original admission time; lookup, joins, retries, and priority do not reset it.
    pub enqueue_at: u64,
    pub hold_cooldown_until: u64,
    pub attempt: Option<AttemptId>,
    pub attempts_started: u32,
    pub terminal: Option<TerminalResult>,
    pub terminal_at: Option<u64>,
}

/// A bounded opportunity to drain a worker for one queued execution. It is not
/// a charged reservation and never releases an existing physical obligation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HoldView {
    pub execution: ExecutionId,
    pub worker: WorkerId,
    pub incarnation: IncarnationId,
    pub started_at: u64,
    pub until: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerView {
    pub id: WorkerId,
    pub incarnation: IncarnationId,
    pub session: SessionId,
    pub properties: BTreeMap<String, String>,
    pub capacity: Resources,
    /// Total admissible resource ceiling, INCLUDING existing reservations.
    /// A raw free-memory observation requires explicit adapter translation.
    pub available: Resources,
    pub draining: bool,
    pub last_heartbeat: u64,
    pub charged: Resources,
    pub quarantined: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptView {
    pub id: AttemptId,
    /// Original delivery identity, retained after acknowledgment consumes the
    /// dispatch intent. Recovered coordinators fence reports against this fact.
    pub dispatch_effect: EffectId,
    pub execution: ExecutionId,
    pub worker: WorkerId,
    pub incarnation: IncarnationId,
    pub reservation: ReservationId,
    pub authorized: bool,
    pub phase: Phase,
    pub progress: u64,
    pub deadline: Deadline,
    pub running_deadline: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReservationView {
    pub id: ReservationId,
    pub attempt: AttemptId,
    pub worker: WorkerId,
    pub incarnation: IncarnationId,
    pub charged: Resources,
    pub released: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObservableState {
    pub epoch: ClusterEpoch,
    pub now: u64,
    pub operations: Vec<OperationView>,
    pub executions: Vec<ExecutionView>,
    pub workers: Vec<WorkerView>,
    pub holds: Vec<HoldView>,
    pub attempts: Vec<AttemptView>,
    pub reservations: Vec<ReservationView>,
    pub effects: Vec<Effect>,
}
