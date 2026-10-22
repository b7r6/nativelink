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
//! another process. Its finite lifetime ledger never evicts uncertain attempts
//! or tombstones; saturation stops admission pending a future retirement design.
//! A candidate result alone cannot become the current core's `Complete` event:
//! that event releases its reservation, so the adapter must also require trusted
//! `Quiescent` evidence. Long-phase meaningful-progress observations and timeout
//! qualification are unfinished; short subprocess tests do not qualify them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nativelink_error::{Code, Error, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptIdentity, AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapability,
    AuthorityCapacityObservation, AuthorityCommand, AuthorityDecline, AuthorityDispatch,
    AuthorityReconciliation, AuthorityRegistration, AuthorityRegistrationResult,
    AuthorityResources, AuthoritySession, AuthorityWorkerIdentity, StartExecute,
    authority_attempt_report, authority_command, authority_decline,
};
use nativelink_util::action_messages::{ActionResult, ActionStage, OperationId};
use nativelink_util::background_spawn;
use parking_lot::Mutex;
use prost::Message;
use tokio::sync::{oneshot, watch};

use crate::running_actions_manager::{RunningAction, RunningActionsManager};

pub const PROTOCOL_VERSION: u32 = 1;
/// Includes inventory envelope headroom, below tonic's default 4 MiB limit.
pub const MAX_RECONCILIATION_BYTES: usize = 3 * 1024 * 1024;
pub const REQUIRED_CAPABILITIES: [i32; 4] = [
    AuthorityCapability::AttemptEffectIdentities as i32,
    AuthorityCapability::ReconnectReconciliation as i32,
    AuthorityCapability::CleanupTombstones as i32,
    AuthorityCapability::CandidateResults as i32,
];

struct Receipt {
    report: AuthorityAttemptReport,
    dispatch: Option<Vec<u8>>,
    cancelled: watch::Sender<bool>,
    reserved: Option<AuthorityResources>,
}

#[derive(Default)]
struct Ledger {
    session: u64,
    connected: bool,
    spent: bool,
    attempts: BTreeMap<u64, Receipt>,
    effects: BTreeMap<u64, u64>,
    reservations: BTreeMap<u64, u64>,
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
                entry.report.observation_sequence += 1;
                let resources = dispatch.reserved_resources.expect("validated resources");
                let fits = resources.cpu_millis <= available.cpu_millis
                    && resources.memory_bytes <= available.memory_bytes
                    && resources.slots <= available.slots;
                reservation = Some(dispatch.reservation_id);
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
                    entry.report.observation_sequence += 1;
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
        entry.report.observation_sequence += 1;
        drop(ledger);
        self.changed.send_replace(());
    }

    async fn run_action(
        &self, target: AuthorityAttemptIdentity, dispatch: AuthorityDispatch,
        cancelled: watch::Receiver<bool>, observer: oneshot::Sender<u32>,
    ) -> (Result<ActionResult, Error>, bool, Option<nativelink_proto::com::github::trace_machina::nativelink::remote_execution::ActionResourceUsage>){
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
        let action = match self
            .manager
            .create_and_add_authority_action(self.identity.worker_id.to_string(), start)
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
            self.phase(target.attempt_id, AuthorityAttemptPhase::Preparing);
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
        let run = self.run_action(target, dispatch, cancelled.clone(), observer);
        tokio::pin!(run);
        let mut receipt_done = lose_receipt;
        let mut signalled = false;
        let (result, quiescent, usage) = loop {
            tokio::select! {
                result = &mut run => break result,
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
        entry.report.observation_sequence += 1;
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
