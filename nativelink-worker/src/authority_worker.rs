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

//! Experimental process-incarnation recipient, reachable through test wiring.
//!
//! Receipts survive connection loss, not process loss. A new process must use a
//! new incarnation, and the authority must retain old charges until trusted
//! cleanup/fencing evidence exists. This module cannot supply that evidence for
//! another process. Its finite ledger removes only quiescent receipts covered by
//! an explicit trusted retirement certificate; uncertain attempts always veto.
//! A candidate result alone cannot become the current core's `Complete` event:
//! that event releases its reservation, so the adapter must also require trusted
//! `Quiescent` evidence. Byte and owned-child observations are implemented, but
//! supported-load and optimized cache/mount liveness remain unqualified. See
//! `nativelink-scheduler/design/categorical/worker-progress.md`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use nativelink_error::{Code, Error, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptIdentity, AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapability,
    AuthorityCapacityObservation, AuthorityCommand, AuthorityDecline, AuthorityDispatch,
    AuthorityReceiptCertificate, AuthorityReceiptRetirement, AuthorityReconciliation,
    AuthorityRegistration, AuthorityRegistrationResult, AuthorityResources, AuthoritySession,
    AuthorityWorkerIdentity, StartExecute, authority_attempt_report, authority_command,
    authority_decline,
};
use nativelink_util::action_messages::{ActionResult, ActionStage, OperationId};
use nativelink_util::background_spawn;
use nativelink_util::digest_hasher::{DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::work_progress::{PhaseProgress, ProgressObserver, ProgressPhase};
use opentelemetry::context::FutureExt;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::{oneshot, watch};

use crate::running_actions_manager::{RunningAction, RunningActionsManager};

pub const PROTOCOL_VERSION: u32 = 1;
/// Includes inventory envelope headroom, below tonic's default 4 MiB limit.
pub const MAX_RECONCILIATION_BYTES: usize = 3 * 1024 * 1024;
pub const MAX_RECEIPT_CERTIFICATE_BYTES: usize = 64 * 1024;
pub const MAX_RECEIPT_EXCEPTIONS: usize = 1024;
pub const REQUIRED_CAPABILITIES: [i32; 6] = [
    AuthorityCapability::AttemptEffectIdentities as i32,
    AuthorityCapability::ReconnectReconciliation as i32,
    AuthorityCapability::CleanupTombstones as i32,
    AuthorityCapability::CandidateResults as i32,
    AuthorityCapability::MeaningfulProgress as i32,
    AuthorityCapability::ReceiptRetirement as i32,
];

struct Receipt {
    report: AuthorityAttemptReport,
    dispatch: Option<Vec<u8>>,
    cancelled: watch::Sender<bool>,
    reserved: Option<AuthorityResources>,
    reservation_id: u64,
}

#[derive(Default)]
struct Ledger {
    session: u64,
    connected: bool,
    spent: bool,
    attempts: BTreeMap<u64, Receipt>,
    effects: BTreeMap<u64, u64>,
    reservations: BTreeMap<u64, u64>,
    retirement: Option<AuthorityReceiptCertificate>,
}

/// Bounded, volatile receipt state for exactly one process incarnation.
pub struct AuthorityWorker<M: RunningActionsManager> {
    identity: AuthorityWorkerIdentity,
    single_use: bool,
    capacity: AuthorityResources,
    max_attempts: usize,
    max_payload_bytes: usize,
    manager: Arc<M>,
    ledger: Mutex<Ledger>,
    changed: watch::Sender<()>,
    drop_launch_receipt: AtomicBool,
}

impl<M: RunningActionsManager> fmt::Debug for AuthorityWorker<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthorityWorker")
            .field("identity", &self.identity)
            .field("single_use", &self.single_use)
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

fn invalid(message: &str) -> Error {
    make_err!(Code::FailedPrecondition, "{message}")
}

fn operation_id(identity: &AuthorityAttemptIdentity) -> String {
    let worker = identity
        .worker
        .as_ref()
        .expect("validated attempt identity");
    format!(
        "authority-{}-{}-{}-{}-{}",
        worker.cluster_epoch,
        worker.worker_id,
        worker.incarnation_id,
        identity.execution_id,
        identity.attempt_id
    )
}

impl<M: RunningActionsManager> AuthorityWorker<M> {
    pub fn new(
        identity: AuthorityWorkerIdentity,
        single_use: bool,
        capacity: AuthorityResources,
        max_attempts: usize,
        max_payload_bytes: usize,
        manager: Arc<M>,
    ) -> Result<Arc<Self>, Error> {
        if identity.cluster_epoch == 0
            || identity.worker_id == 0
            || identity.incarnation_id == 0
            || max_attempts == 0
            || max_payload_bytes < 1024
            || capacity.slots == 0
            || max_payload_bytes
                .checked_add(5)
                .and_then(|bytes| max_attempts.checked_mul(bytes))
                .and_then(|bytes| bytes.checked_add(MAX_RECEIPT_CERTIFICATE_BYTES + 8))
                .and_then(|bytes| bytes.checked_add(256))
                .is_none_or(|bytes| bytes > MAX_RECONCILIATION_BYTES)
        {
            return Err(invalid("Worker identity and ledger bounds must be nonzero"));
        }
        Ok(Arc::new(Self {
            identity,
            single_use,
            capacity,
            max_attempts,
            max_payload_bytes,
            manager,
            ledger: Mutex::new(Ledger::default()),
            changed: watch::channel(()).0,
            drop_launch_receipt: AtomicBool::new(false),
        }))
    }

    /// Complete mandatory negotiation before accepting any command. A newer
    /// transport session reuses the same process receipts and single-use fact.
    pub fn negotiate(
        &self,
        result: &AuthorityRegistrationResult,
    ) -> Result<AuthorityReconciliation, Error> {
        let session = result.session.ok_or_else(|| invalid("Missing session"))?;
        if session.worker.as_ref() != Some(&self.identity)
            || session.session_id == 0
            || result.protocol_version != PROTOCOL_VERSION
            || REQUIRED_CAPABILITIES
                .iter()
                .any(|cap| !result.required_capabilities.contains(cap))
            || result
                .required_capabilities
                .iter()
                .any(|cap| !REQUIRED_CAPABILITIES.contains(cap))
        {
            return Err(invalid(
                "Incompatible authority protocol or worker identity",
            ));
        }
        {
            let mut ledger = self.ledger.lock();
            if session.session_id <= ledger.session {
                return Err(invalid("Transport session must advance monotonically"));
            }
            ledger.session = session.session_id;
            ledger.connected = true;
        }
        Ok(self.reconcile())
    }

    /// Build registration from this recipient's actual process-lifetime facts.
    /// The requested session is not active until negotiation succeeds.
    pub fn make_registration(
        &self,
        session: u64,
        properties: Vec<Property>,
    ) -> Result<AuthorityRegistration, Error> {
        let ledger = self.ledger.lock();
        if session <= ledger.session {
            return Err(invalid("Registration session must advance monotonically"));
        }
        let identity = AuthoritySession {
            worker: Some(self.identity),
            session_id: session,
        };
        let closed =
            ledger.attempts.len() >= self.max_attempts || (self.single_use && ledger.spent);
        let registration = AuthorityRegistration {
            session: Some(identity),
            protocol_version: PROTOCOL_VERSION,
            capabilities: REQUIRED_CAPABILITIES.to_vec(),
            properties,
            capacity: Some(self.capacity),
            admission_ceiling: Some(if closed {
                AuthorityResources::default()
            } else {
                self.capacity
            }),
            single_use: self.single_use,
            reconciliation: Some(AuthorityReconciliation {
                session: Some(identity),
                attempts: ledger
                    .attempts
                    .values()
                    .map(|entry| entry.report.clone())
                    .collect(),
                single_use_spent: self.single_use && ledger.spent,
                receipt_retirement: ledger.retirement.clone(),
            }),
        };
        if registration.encoded_len() > MAX_RECONCILIATION_BYTES {
            return Err(make_err!(
                Code::ResourceExhausted,
                "Registration exceeds reconciliation byte bound"
            ));
        }
        Ok(registration)
    }

    /// Report the configured total admission ceiling, including held charges.
    /// This is not a new measurement of physical free memory or CPU capacity.
    pub fn make_capacity_observation(
        &self,
        session: u64,
        observation_sequence: u64,
    ) -> Result<AuthorityCapacityObservation, Error> {
        let ledger = self.ledger.lock();
        if !ledger.connected || session != ledger.session || observation_sequence == 0 {
            return Err(invalid(
                "Capacity observation requires the current negotiated session and nonzero sequence",
            ));
        }
        let closed =
            ledger.attempts.len() >= self.max_attempts || (self.single_use && ledger.spent);
        Ok(AuthorityCapacityObservation {
            session: Some(AuthoritySession {
                worker: Some(self.identity),
                session_id: session,
            }),
            observation_sequence,
            capacity: Some(self.capacity),
            admission_ceiling: Some(if closed {
                AuthorityResources::default()
            } else {
                self.capacity
            }),
            draining: closed,
        })
    }

    /// Disconnect is a connectivity fact. It does not kill work or reset state.
    pub fn disconnect(&self, session: u64) {
        let mut ledger = self.ledger.lock();
        if ledger.session == session {
            ledger.connected = false;
        }
    }

    pub fn reconcile(&self) -> AuthorityReconciliation {
        let ledger = self.ledger.lock();
        AuthorityReconciliation {
            session: Some(AuthoritySession {
                worker: Some(self.identity),
                session_id: ledger.session,
            }),
            attempts: ledger
                .attempts
                .values()
                .map(|entry| entry.report.clone())
                .collect(),
            single_use_spent: self.single_use && ledger.spent,
            receipt_retirement: ledger.retirement.clone(),
        }
    }

    /// Notification is only a hint; callers reread the retained inventory.
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub fn admission_closed(&self) -> bool {
        let ledger = self.ledger.lock();
        ledger.attempts.len() >= self.max_attempts || (self.single_use && ledger.spent)
    }

    /// Apply one trusted, committed compaction directive. This never establishes
    /// cleanup, accepts output or releases an authority reservation. `OutOfRange`
    /// distinguishes an obsolete certificate from a conflicting current one.
    pub fn retire_receipts(&self, control: AuthorityReceiptRetirement) -> Result<bool, Error> {
        let session = control
            .session
            .ok_or_else(|| invalid("Missing retirement session"))?;
        let certificate = control
            .certificate
            .ok_or_else(|| invalid("Missing retirement certificate"))?;
        if session.worker != Some(self.identity)
            || certificate.effect_id == 0
            || certificate.effects_through < certificate.effect_id
            || certificate.retain.len() > MAX_RECEIPT_EXCEPTIONS
            || certificate.encoded_len() > MAX_RECEIPT_CERTIFICATE_BYTES
        {
            return Err(invalid("Invalid receipt retirement identity or bound"));
        }
        let mut retained = BTreeMap::new();
        let mut effects = BTreeSet::new();
        let mut reservations = BTreeSet::new();
        let mut previous_attempt = 0;
        for entry in &certificate.retain {
            if entry.attempt_id <= previous_attempt
                || entry.attempt_id > certificate.attempts_through
                || entry.execution_id == 0
                || entry.dispatch_effect_id == 0
                || entry.dispatch_effect_id > certificate.effects_through
                || entry.reservation_id == 0
                || entry.reservation_id > certificate.reservations_through
                || entry.cancel_effect_id > certificate.effects_through
                || !effects.insert(entry.dispatch_effect_id)
                || (entry.cancel_effect_id != 0 && !effects.insert(entry.cancel_effect_id))
                || !reservations.insert(entry.reservation_id)
            {
                return Err(invalid("Invalid or aliased retained receipt binding"));
            }
            previous_attempt = entry.attempt_id;
            retained.insert(entry.attempt_id, entry);
        }
        if effects.contains(&certificate.effect_id) {
            return Err(invalid("Retirement effect aliases a retained command"));
        }
        let mut ledger = self.ledger.lock();
        if !ledger.connected || session.session_id != ledger.session {
            return Err(invalid(
                "Retirement is not on the negotiated current session",
            ));
        }
        if let Some(previous) = &ledger.retirement {
            if certificate.effect_id == previous.effect_id {
                return if certificate == *previous {
                    Ok(false)
                } else {
                    Err(invalid("Retirement identity changed contents"))
                };
            }
            if certificate.effect_id < previous.effect_id {
                return Err(make_err!(
                    Code::OutOfRange,
                    "Receipt retirement certificate is obsolete"
                ));
            }
            if certificate.effect_id <= previous.effects_through {
                return Err(invalid(
                    "Receipt retirement reused an allocated effect identity",
                ));
            }
            if certificate.attempts_through < previous.attempts_through
                || certificate.effects_through < previous.effects_through
                || certificate.reservations_through < previous.reservations_through
            {
                return Err(invalid(
                    "Receipt retirement watermarks regressed or reused identity",
                ));
            }
            let old: BTreeMap<_, _> = previous
                .retain
                .iter()
                .map(|entry| (entry.attempt_id, entry))
                .collect();
            for entry in &certificate.retain {
                if entry.attempt_id <= previous.attempts_through {
                    let prior = old
                        .get(&entry.attempt_id)
                        .ok_or_else(|| invalid("Retirement revived an omitted attempt"))?;
                    if entry.execution_id != prior.execution_id
                        || entry.dispatch_effect_id != prior.dispatch_effect_id
                        || entry.reservation_id != prior.reservation_id
                        || (entry.cancel_effect_id != prior.cancel_effect_id
                            && (prior.cancel_effect_id != 0
                                || entry.cancel_effect_id <= previous.effects_through))
                    {
                        return Err(invalid("Retained receipt identity changed"));
                    }
                } else if entry.dispatch_effect_id <= previous.effects_through
                    || entry.reservation_id <= previous.reservations_through
                    || (entry.cancel_effect_id != 0
                        && entry.cancel_effect_id <= previous.effects_through)
                {
                    return Err(invalid("New retained attempt reused retired identities"));
                }
            }
        }
        if ledger.effects.contains_key(&certificate.effect_id) {
            return Err(invalid("Retirement effect aliases a local command"));
        }
        for entry in &certificate.retain {
            if [entry.dispatch_effect_id, entry.cancel_effect_id]
                .into_iter()
                .filter(|id| *id != 0)
                .any(|id| {
                    ledger
                        .effects
                        .get(&id)
                        .is_some_and(|attempt| *attempt != entry.attempt_id)
                })
                || ledger
                    .reservations
                    .get(&entry.reservation_id)
                    .is_some_and(|attempt| *attempt != entry.attempt_id)
            {
                return Err(invalid("Retained binding aliases another local attempt"));
            }
        }
        for (attempt, receipt) in &ledger.attempts {
            let report = &receipt.report;
            let known_effects = [report.dispatch_effect_id, report.cancel_effect_id];
            if *attempt > certificate.attempts_through {
                if known_effects
                    .into_iter()
                    .any(|id| id != 0 && id <= certificate.effects_through)
                    || (receipt.reservation_id != 0
                        && receipt.reservation_id <= certificate.reservations_through)
                {
                    return Err(invalid(
                        "Certificate cuts a newer local attempt's identities",
                    ));
                }
            } else if let Some(binding) = retained.get(attempt) {
                if report
                    .attempt
                    .as_ref()
                    .is_none_or(|target| target.execution_id != binding.execution_id)
                    || (report.dispatch_effect_id != 0
                        && report.dispatch_effect_id != binding.dispatch_effect_id)
                    || (receipt.reservation_id != 0
                        && receipt.reservation_id != binding.reservation_id)
                    || (report.cancel_effect_id != 0
                        && report.cancel_effect_id != binding.cancel_effect_id
                        && (binding.cancel_effect_id != 0
                            || report.cancel_effect_id <= certificate.effects_through))
                {
                    return Err(invalid(
                        "Certificate conflicts with a retained local receipt",
                    ));
                }
            } else if report.phase != AuthorityAttemptPhase::Quiescent as i32
                || known_effects
                    .into_iter()
                    .any(|id| id > certificate.effects_through)
                || receipt.reservation_id > certificate.reservations_through
            {
                return Err(invalid(
                    "Receipt retirement requires covered local quiescence",
                ));
            }
        }
        ledger.attempts.retain(|attempt, _| {
            *attempt > certificate.attempts_through || retained.contains_key(attempt)
        });
        ledger.effects.retain(|_, attempt| {
            *attempt > certificate.attempts_through || retained.contains_key(attempt)
        });
        ledger.reservations.retain(|_, attempt| {
            *attempt > certificate.attempts_through || retained.contains_key(attempt)
        });
        ledger.retirement = Some(certificate);
        drop(ledger);
        self.changed.send_replace(());
        Ok(true)
    }

    #[doc(hidden)]
    pub fn ledger_counts_for_test(&self) -> (usize, usize, usize, usize) {
        let ledger = self.ledger.lock();
        (
            ledger.attempts.len(),
            ledger.effects.len(),
            ledger.reservations.len(),
            ledger
                .retirement
                .as_ref()
                .map_or(0, |certificate| certificate.retain.len()),
        )
    }

    /// Test a real successful spawn whose receipt is lost before being retained.
    #[doc(hidden)]
    pub fn lose_next_launch_receipt(&self) {
        self.drop_launch_receipt.store(true, Ordering::SeqCst);
    }

    pub fn command(
        self: &Arc<Self>,
        session: u64,
        command: AuthorityCommand,
    ) -> Result<AuthorityAttemptReport, Error> {
        if command.encoded_len() > self.max_payload_bytes {
            return Err(make_err!(
                Code::ResourceExhausted,
                "Authority command exceeds payload bound"
            ));
        }
        let target = command
            .attempt
            .ok_or_else(|| invalid("Missing attempt identity"))?;
        if target.worker.as_ref() != Some(&self.identity)
            || target.execution_id == 0
            || target.attempt_id == 0
            || command.effect_id == 0
        {
            return Err(invalid(
                "Wrong epoch, worker, incarnation or attempt identity",
            ));
        }
        let kind = command.command.ok_or_else(|| invalid("Missing command"))?;
        if let authority_command::Command::Dispatch(dispatch) = &kind
            && (dispatch.reservation_id == 0
                || dispatch
                    .execute_request
                    .as_ref()
                    .and_then(|r| r.action_digest.as_ref())
                    .is_none()
                || dispatch
                    .reserved_resources
                    .as_ref()
                    .is_none_or(|resources| resources.slots == 0))
        {
            return Err(invalid("Missing reservation or action digest"));
        }
        let mut ledger = self.ledger.lock();
        if !ledger.connected || ledger.session != session {
            return Err(invalid("Command is not on the negotiated current session"));
        }
        if let Some(certificate) = &ledger.retirement {
            let retained = certificate
                .retain
                .iter()
                .find(|entry| entry.attempt_id == target.attempt_id);
            if target.attempt_id <= certificate.attempts_through && retained.is_none() {
                return Err(make_err!(
                    Code::OutOfRange,
                    "Attempt was permanently compacted in this incarnation"
                ));
            }
            if retained.is_some_and(|entry| entry.execution_id != target.execution_id) {
                return Err(invalid("Command changed a retained execution identity"));
            }
            let expected_effect = retained.map(|entry| match &kind {
                authority_command::Command::Dispatch(_) => entry.dispatch_effect_id,
                authority_command::Command::Cancel(()) => entry.cancel_effect_id,
            });
            if command.effect_id <= certificate.effects_through
                && expected_effect != Some(command.effect_id)
            {
                return Err(invalid("Command reused a retired effect identity"));
            }
            match &kind {
                authority_command::Command::Dispatch(dispatch) => {
                    let changed_effect =
                        retained.is_some_and(|entry| entry.dispatch_effect_id != command.effect_id);
                    let changed_reservation = retained
                        .is_some_and(|entry| entry.reservation_id != dispatch.reservation_id);
                    if changed_effect
                        || changed_reservation
                        || (dispatch.reservation_id <= certificate.reservations_through
                            && retained.is_none_or(|entry| {
                                entry.reservation_id != dispatch.reservation_id
                            }))
                    {
                        return Err(invalid("Dispatch changed or reused a retired identity"));
                    }
                }
                authority_command::Command::Cancel(()) => {
                    if retained.is_some_and(|entry| {
                        entry.cancel_effect_id != 0 && entry.cancel_effect_id != command.effect_id
                    }) {
                        return Err(invalid("Cancel changed a retained effect identity"));
                    }
                }
            }
        }
        if ledger
            .effects
            .get(&command.effect_id)
            .is_some_and(|id| *id != target.attempt_id)
        {
            return Err(invalid("Effect identity was reused for another attempt"));
        }
        if let authority_command::Command::Dispatch(dispatch) = &kind
            && ledger
                .reservations
                .get(&dispatch.reservation_id)
                .is_some_and(|id| *id != target.attempt_id)
        {
            return Err(invalid(
                "Reservation identity was reused for another attempt",
            ));
        }
        if !ledger.attempts.contains_key(&target.attempt_id) {
            if ledger.attempts.len() >= self.max_attempts {
                // No room for a tombstone means no confirmed refusal either.
                // The coordinator must retain the original uncertain charge.
                return Err(make_err!(
                    Code::ResourceExhausted,
                    "Worker lifetime receipt ledger is full"
                ));
            }
            ledger.attempts.insert(
                target.attempt_id,
                Receipt {
                    report: AuthorityAttemptReport {
                        attempt: Some(target),
                        observation_sequence: 1,
                        phase: AuthorityAttemptPhase::LaunchUncertain as i32,
                        ..AuthorityAttemptReport::default()
                    },
                    dispatch: None,
                    cancelled: watch::channel(false).0,
                    reserved: None,
                    reservation_id: 0,
                },
            );
        }
        let spent = ledger.spent;
        let mut available = self.capacity;
        for receipt in ledger.attempts.values() {
            if receipt.report.phase != AuthorityAttemptPhase::Quiescent as i32
                && let Some(resources) = receipt.reserved
            {
                available.cpu_millis = available.cpu_millis.saturating_sub(resources.cpu_millis);
                available.memory_bytes = available
                    .memory_bytes
                    .saturating_sub(resources.memory_bytes);
                available.slots = available.slots.saturating_sub(resources.slots);
            }
        }
        let entry = ledger
            .attempts
            .get_mut(&target.attempt_id)
            .expect("entry installed");
        if entry.report.attempt.as_ref() != Some(&target) {
            return Err(invalid("Attempt identity was reused for another execution"));
        }
        let mut launch = None;
        let mut reservation = None;
        match kind {
            authority_command::Command::Dispatch(dispatch) => {
                let fingerprint = dispatch.encode_to_vec();
                if let Some(previous) = &entry.dispatch {
                    if previous != &fingerprint
                        || entry.report.dispatch_effect_id != command.effect_id
                    {
                        return Err(invalid(
                            "Dispatch identity was reused with different contents",
                        ));
                    }
                    return Ok(entry.report.clone());
                }
                if entry.report.cancel_effect_id == command.effect_id {
                    return Err(invalid(
                        "Effect identity was reused for a different command",
                    ));
                }
                entry.dispatch = Some(fingerprint);
                entry.report.dispatch_effect_id = command.effect_id;
                entry.report.observation_sequence =
                    entry.report.observation_sequence.saturating_add(1);
                let resources = dispatch.reserved_resources.expect("validated resources");
                let fits = resources.cpu_millis <= available.cpu_millis
                    && resources.memory_bytes <= available.memory_bytes
                    && resources.slots <= available.slots;
                reservation = Some(dispatch.reservation_id);
                entry.reservation_id = dispatch.reservation_id;
                if *entry.cancelled.borrow() {
                    // The earlier cancel tombstone closes this recipient even
                    // when the first dispatch arrives after cleanup evidence.
                } else if (self.single_use && spent) || !fits {
                    entry.cancelled.send_replace(true);
                    entry.report.phase = AuthorityAttemptPhase::Quiescent as i32;
                    entry.report.result = Some(authority_attempt_report::Result::Decline(
                        AuthorityDecline {
                            reason: if self.single_use && spent {
                                authority_decline::Reason::SingleUseSpent
                            } else {
                                authority_decline::Reason::Admission
                            } as i32,
                            detail: "Process lifetime or reserved resource admission limit"
                                .to_owned(),
                        },
                    ));
                } else {
                    entry.reserved = Some(resources);
                    launch = Some((dispatch, entry.cancelled.subscribe()));
                }
            }
            authority_command::Command::Cancel(()) => {
                if entry.report.dispatch_effect_id == command.effect_id
                    || (entry.report.cancel_effect_id != 0
                        && entry.report.cancel_effect_id != command.effect_id)
                {
                    return Err(invalid("Cancel effect identity mismatch"));
                }
                if entry.report.cancel_effect_id == 0 {
                    entry.report.observation_sequence =
                        entry.report.observation_sequence.saturating_add(1);
                }
                entry.report.cancel_effect_id = command.effect_id;
                if !entry.cancelled.send_replace(true) {
                    if entry.dispatch.is_none() {
                        entry.report.phase = AuthorityAttemptPhase::Quiescent as i32;
                    } else if entry.report.phase != AuthorityAttemptPhase::Quiescent as i32 {
                        entry.report.phase = AuthorityAttemptPhase::TerminationUncertain as i32;
                    }
                }
            }
        }
        let report = entry.report.clone();
        ledger.effects.insert(command.effect_id, target.attempt_id);
        if let Some(reservation) = reservation {
            ledger.reservations.insert(reservation, target.attempt_id);
        }
        if launch.is_some() {
            ledger.spent = true;
        }
        drop(ledger);
        self.changed.send_replace(());
        if let Some((dispatch, cancelled)) = launch {
            let worker = self.clone();
            background_spawn!("authority_worker_attempt", async move {
                worker.drive(target, dispatch, cancelled).await;
            });
        }
        Ok(report)
    }

    fn phase(&self, attempt: u64, phase: AuthorityAttemptPhase) {
        let mut ledger = self.ledger.lock();
        let entry = ledger.attempts.get_mut(&attempt).expect("retained receipt");
        if *entry.cancelled.borrow()
            || (phase == AuthorityAttemptPhase::Running
                && entry.report.phase != AuthorityAttemptPhase::LaunchUncertain as i32)
        {
            return;
        }
        entry.report.phase = phase as i32;
        entry.report.phase_progress = 0;
        let Some(sequence) = entry.report.observation_sequence.checked_add(1) else {
            return;
        };
        entry.report.observation_sequence = sequence;
        drop(ledger);
        self.changed.send_replace(());
    }

    fn progress(&self, attempt: u64, progress: PhaseProgress) {
        if progress.exhausted {
            return;
        }
        let phase = match progress.phase {
            ProgressPhase::Preparing => AuthorityAttemptPhase::Preparing,
            ProgressPhase::Running => AuthorityAttemptPhase::Running,
            ProgressPhase::Uploading => AuthorityAttemptPhase::Uploading,
        };
        let mut ledger = self.ledger.lock();
        let entry = ledger.attempts.get_mut(&attempt).expect("retained receipt");
        // A lost launch receipt must remain uncertain even when the process
        // observer is alive. Old phases and cancellation can never renew.
        if *entry.cancelled.borrow()
            || entry.report.phase != phase as i32
            || progress.progress <= entry.report.phase_progress
        {
            return;
        }
        let Some(sequence) = entry.report.observation_sequence.checked_add(1) else {
            tracing::warn!(
                attempt,
                "Attempt report sequence exhausted; renewal disabled"
            );
            return;
        };
        entry.report.observation_sequence = sequence;
        entry.report.phase_progress = progress.progress;
        drop(ledger);
        self.changed.send_replace(());
    }

    async fn run_action(
        &self, target: AuthorityAttemptIdentity, dispatch: AuthorityDispatch,
        cancelled: watch::Receiver<bool>, observer: oneshot::Sender<u32>, progress: ProgressObserver,
    ) -> (Result<ActionResult, Error>, bool, Option<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::ActionResourceUsage>){
        // Authority v1 fixes an omitted digest function to SHA256. Scope the
        // whole action future so uploads and inherited task contexts use its
        // algorithm, independently of the legacy process default.
        let algorithm = match dispatch
            .execute_request
            .as_ref()
            .map_or(0, |request| request.digest_function)
        {
            0 => Ok(DigestHasherFunc::Sha256),
            value => DigestHasherFunc::try_from(value),
        };
        let context = match algorithm.and_then(make_ctx_for_hash_func) {
            Ok(context) => context,
            Err(error) => return (Err(error), true, None),
        };
        async {
            let reserved_resources = dispatch
                .reserved_resources
                .expect("validated immutable grant");
            let start = StartExecute {
                execute_request: dispatch.execute_request,
                operation_id: operation_id(&target),
                queued_timestamp: dispatch.queued_timestamp,
                platform: dispatch.platform,
                worker_id: self.identity.worker_id.to_string(),
                request_metadata: dispatch.request_metadata,
            };
            // Creation does not launch the action process. The launch seam below
            // is the only observation that permits a Running report.
            self.phase(target.attempt_id, AuthorityAttemptPhase::Preparing);
            let action = match self
                .manager
                .create_and_add_authority_action_observed(
                    self.identity.worker_id.to_string(),
                    start,
                    reserved_resources,
                    progress,
                )
                .await
            {
                Ok(action) => action,
                Err(error) => return (Err(error), true, None),
            };
            let result = async {
                action.observe_launch(observer)?;
                if *cancelled.borrow() {
                    return Err(make_err!(
                        Code::Cancelled,
                        "Attempt cancelled before preparation"
                    ));
                }
                action.clone().prepare_action().await?;
                if *cancelled.borrow() {
                    return Err(make_err!(
                        Code::Cancelled,
                        "Attempt cancelled before launch"
                    ));
                }
                self.phase(target.attempt_id, AuthorityAttemptPhase::LaunchUncertain);
                action.clone().execute().await?;
                self.phase(target.attempt_id, AuthorityAttemptPhase::Uploading);
                action.clone().upload_results().await?;
                action.clone().get_finished_result().await
            }
            .await;
            let usage = action.resource_usage();
            let quiescent = action.process_domain_quiescent();
            match action.cleanup().await {
                Ok(_) => (result, quiescent, usage),
                Err(error) => (Err(error), false, usage),
            }
        }
        .with_context(context)
        .await
    }

    async fn drive(
        self: Arc<Self>,
        target: AuthorityAttemptIdentity,
        dispatch: AuthorityDispatch,
        mut cancelled: watch::Receiver<bool>,
    ) {
        let (observer, mut launched) = oneshot::channel();
        let lose_receipt = self.drop_launch_receipt.swap(false, Ordering::SeqCst);
        if lose_receipt {
            launched.close();
        }
        let progress = ProgressObserver::new();
        let mut progress_updates = progress.subscribe();
        let report_progress = async {
            loop {
                if progress_updates.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
                // Only an actual change arms this delay. Timer ticks by
                // themselves never publish or increment progress.
                tokio::time::sleep(Duration::from_secs(5)).await;
                let progress = *progress_updates.borrow_and_update();
                self.progress(target.attempt_id, progress);
            }
        };
        tokio::pin!(report_progress);
        let run = self.run_action(target, dispatch, cancelled.clone(), observer, progress);
        tokio::pin!(run);
        let mut receipt_done = lose_receipt;
        let mut signalled = false;
        let (result, quiescent, usage) = loop {
            tokio::select! {
                result = &mut run => break result,
                () = &mut report_progress => unreachable!(),
                receipt = &mut launched, if !receipt_done => {
                    receipt_done = true;
                    if receipt.is_ok() { self.phase(target.attempt_id, AuthorityAttemptPhase::Running); }
                }
                () = async { let _cancelled = cancelled.wait_for(|cancelled| *cancelled).await; }, if !signalled => {
                    signalled = true;
                    // This only requests termination. We keep polling the
                    // process and await cleanup before claiming quiescence.
                    let _signal = self.manager.kill_operation(&OperationId::from(operation_id(&target))).await;
                }
            }
        };
        let mut ledger = self.ledger.lock();
        let entry = ledger
            .attempts
            .get_mut(&target.attempt_id)
            .expect("retained receipt");
        entry.cancelled.send_replace(true);
        entry.report.phase = if quiescent {
            AuthorityAttemptPhase::Quiescent
        } else {
            AuthorityAttemptPhase::TerminationUncertain
        } as i32;
        entry.report.result = Some(match result {
            Ok(result) => authority_attempt_report::Result::ExecuteResponse(
                ActionStage::Completed(result).into(),
            ),
            Err(error) => authority_attempt_report::Result::InternalError(error.into()),
        });
        entry.report.resource_usage = usage;
        entry.report.observation_sequence = entry.report.observation_sequence.saturating_add(1);
        if entry.report.encoded_len() > self.max_payload_bytes {
            entry.report.result = Some(authority_attempt_report::Result::InternalError(
                make_err!(
                    Code::ResourceExhausted,
                    "Candidate result exceeds recipient payload bound"
                )
                .into(),
            ));
            entry.report.resource_usage = None;
        }
        drop(ledger);
        self.changed.send_replace(());
    }
}
