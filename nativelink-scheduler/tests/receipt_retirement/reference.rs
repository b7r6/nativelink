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

//! Independent receipt-retirement specification model.
//!
//! Only inert public vocabulary is shared with the candidate. Baseline facts
//! and allocation cursors must come from a specification scenario or another
//! independent model, never from the candidate's observed state. This module
//! does not import the production State, reducer, allocator, wire encoder,
//! accounting, or snapshot helpers.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_scheduler::authority::{
    AttemptId, ClusterEpoch, EffectId, Event, ExecutionId, IncarnationId, MAX_ATTEMPTS,
    MAX_RECEIPT_BINDINGS, MAX_RECEIPT_CERTIFICATE_BYTES, ReceiptBinding, ReceiptCertificate,
    ReceiptFloors, ReceiptRetirement, ReceiptStream, RecordedEvent, Rejection, Reply,
    ReservationId, WorkerId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AttemptFact {
    pub worker: WorkerId,
    pub incarnation: IncarnationId,
    pub binding: ReceiptBinding,
    pub released: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionFact {
    pub terminal: bool,
    pub attempts_started: u32,
    pub current_attempt: bool,
    pub do_not_cache: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Facts {
    pub workers: BTreeMap<WorkerId, IncarnationId>,
    pub attempts: BTreeMap<AttemptId, AttemptFact>,
    pub executions: BTreeMap<ExecutionId, ExecutionFact>,
    pub next_attempt: u64,
    pub next_effect: u64,
    pub next_reservation: u64,
}

impl Default for Facts {
    fn default() -> Self {
        Self {
            workers: BTreeMap::new(),
            attempts: BTreeMap::new(),
            executions: BTreeMap::new(),
            next_attempt: 1,
            next_effect: 1,
            next_reservation: 1,
        }
    }
}

impl Facts {
    fn finalization_ids(&self) -> Option<u64> {
        let mut remaining = 0_u64;
        for execution in self
            .executions
            .values()
            .filter(|execution| !execution.terminal)
        {
            let retries = u64::from(MAX_ATTEMPTS.checked_sub(execution.attempts_started)?);
            remaining = remaining.checked_add(retries.checked_mul(2)?)?;
            remaining = remaining.checked_add(u64::from(execution.current_attempt))?;
            remaining = remaining.checked_add(u64::from(!execution.do_not_cache))?;
        }
        Some(remaining)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.next_attempt == 0 || self.next_effect == 0 || self.next_reservation == 0 {
            return Err("zero allocation cursor".into());
        }
        let mut effects = BTreeSet::new();
        let mut reservations = BTreeSet::new();
        for (id, attempt) in &self.attempts {
            let binding = attempt.binding;
            if *id != binding.attempt
                || id.0 == 0
                || id.0 >= self.next_attempt
                || binding.execution.0 == 0
                || !self.executions.contains_key(&binding.execution)
                || binding.dispatch_effect.0 == 0
                || binding.dispatch_effect.0 >= self.next_effect
                || binding.reservation.0 == 0
                || binding.reservation.0 >= self.next_reservation
                || !effects.insert(binding.dispatch_effect)
                || !reservations.insert(binding.reservation)
                || !self
                    .workers
                    .get(&attempt.worker)
                    .is_some_and(|incarnation| {
                        attempt.incarnation.0 > 0 && attempt.incarnation <= *incarnation
                    })
            {
                return Err("invalid independent attempt facts".into());
            }
            if let Some(cancel) = binding.cancel_effect
                && (cancel.0 == 0 || cancel.0 >= self.next_effect || !effects.insert(cancel))
            {
                return Err("invalid independent cancellation identity".into());
            }
        }
        self.finalization_ids()
            .ok_or("invalid finalization obligation")?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    epoch: ClusterEpoch,
    retirement: Option<ReceiptRetirement>,
}

impl Reference {
    pub(crate) const fn new(epoch: ClusterEpoch) -> Self {
        Self {
            epoch,
            retirement: None,
        }
    }

    pub(crate) fn activate(&mut self, facts: &Facts) {
        if self.retirement.is_none() {
            self.retirement = Some(ReceiptRetirement {
                streams: facts
                    .workers
                    .iter()
                    .map(|(worker, incarnation)| (*worker, fresh_stream(*incarnation)))
                    .collect(),
            });
        }
    }

    pub(crate) const fn projection(&self) -> Option<&ReceiptRetirement> {
        self.retirement.as_ref()
    }

    /// Records an independently accepted registration. Reconnection changes no
    /// receipt state. Replacement removes only the obsolete stream and its pins.
    pub(crate) fn registered(&mut self, worker: WorkerId, incarnation: IncarnationId) {
        let Some(retirement) = &mut self.retirement else {
            return;
        };
        if retirement
            .streams
            .get(&worker)
            .is_none_or(|stream| stream.incarnation != incarnation)
        {
            retirement.streams.insert(worker, fresh_stream(incarnation));
        }
    }

    /// The independent lifecycle driver supplies the original reservation's
    /// owner only after accepting a transition from charged to released.
    pub(crate) fn released(&mut self, worker: WorkerId, incarnation: IncarnationId) {
        if let Some(stream) = self
            .retirement
            .as_mut()
            .and_then(|value| value.streams.get_mut(&worker))
            && stream.incarnation == incarnation
        {
            stream.dirty = true;
        }
    }

    pub(crate) fn pins(&self) -> BTreeSet<AttemptId> {
        self.retirement
            .iter()
            .flat_map(|retirement| retirement.streams.values())
            .filter_map(|stream| stream.pending.as_ref())
            .flat_map(|certificate| certificate.retain.iter().map(|binding| binding.attempt))
            .collect()
    }

    pub(crate) fn blocks_retirement(&self, execution: ExecutionId, facts: &Facts) -> bool {
        self.pins().iter().any(|attempt| {
            facts
                .attempts
                .get(attempt)
                .is_some_and(|value| value.binding.execution == execution)
        })
    }

    pub(crate) fn apply(&mut self, event: RecordedEvent, facts: &mut Facts) -> Reply {
        if event.epoch != self.epoch {
            return Reply::Rejected(Rejection::WrongEpoch);
        }
        match event.event {
            Event::IssueReceipts {
                worker,
                incarnation,
                after_ack,
            } => self.issue(worker, incarnation, after_ack, facts),
            Event::AcknowledgeReceipts {
                worker,
                incarnation,
                certificate,
            } => self.acknowledge(worker, incarnation, &certificate, facts),
            _ => Reply::Rejected(Rejection::Invalid),
        }
    }

    fn check_scope(
        &self,
        worker: WorkerId,
        incarnation: IncarnationId,
        facts: &Facts,
    ) -> Result<(), Rejection> {
        if self.retirement.is_none() {
            return Err(Rejection::Invalid);
        }
        match facts.workers.get(&worker) {
            None => Err(Rejection::Unknown),
            Some(current) if *current != incarnation => Err(Rejection::Stale),
            Some(_) => Ok(()),
        }
    }

    fn issue(
        &mut self,
        worker: WorkerId,
        incarnation: IncarnationId,
        after_ack: Option<EffectId>,
        facts: &mut Facts,
    ) -> Reply {
        if let Err(error) = self.check_scope(worker, incarnation, facts) {
            return Reply::Rejected(error);
        }
        let stream = &self.retirement.as_ref().expect("scope checked").streams[&worker];
        if stream.acknowledged.map(|ack| ack.effect) != after_ack {
            return Reply::Rejected(Rejection::Stale);
        }
        if stream.pending.is_some() || !stream.dirty {
            return Reply::Ignored;
        }
        let Some(next_effect) = facts.next_effect.checked_add(1) else {
            return Reply::Rejected(Rejection::Overloaded);
        };
        if facts
            .finalization_ids()
            .and_then(|reserved| next_effect.checked_add(reserved))
            .is_none()
        {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let certificate = ReceiptCertificate {
            effect: EffectId(facts.next_effect),
            attempts_through: AttemptId(facts.next_attempt - 1),
            effects_through: EffectId(facts.next_effect),
            reservations_through: ReservationId(facts.next_reservation - 1),
            retain: facts
                .attempts
                .values()
                .filter(|attempt| {
                    attempt.worker == worker
                        && attempt.incarnation == incarnation
                        && !attempt.released
                })
                .map(|attempt| attempt.binding)
                .collect(),
        };
        if !within_certificate_bounds(&certificate) {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let stream = self
            .retirement
            .as_mut()
            .expect("scope checked")
            .streams
            .get_mut(&worker)
            .expect("registered stream");
        stream.pending = Some(certificate);
        stream.dirty = false;
        facts.next_effect = next_effect;
        Reply::Applied
    }

    fn acknowledge(
        &mut self,
        worker: WorkerId,
        incarnation: IncarnationId,
        certificate: &ReceiptCertificate,
        facts: &Facts,
    ) -> Reply {
        if let Err(error) = self.check_scope(worker, incarnation, facts) {
            return Reply::Rejected(error);
        }
        let stream = self
            .retirement
            .as_mut()
            .expect("scope checked")
            .streams
            .get_mut(&worker)
            .expect("registered stream");
        if stream.pending.as_ref() != Some(certificate) {
            return Reply::Rejected(Rejection::Stale);
        }
        stream.acknowledged = Some(ReceiptFloors {
            effect: certificate.effect,
            attempts_through: certificate.attempts_through,
            effects_through: certificate.effects_through,
            reservations_through: certificate.reservations_through,
        });
        stream.pending = None;
        Reply::Applied
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("independent model encoding")
    }

    pub(crate) fn restore(bytes: &[u8], facts: &Facts) -> Result<Self, String> {
        let restored: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        restored.validate(facts)?;
        Ok(restored)
    }

    pub(crate) fn validate(&self, facts: &Facts) -> Result<(), String> {
        facts.validate()?;
        let Some(retirement) = &self.retirement else {
            return Ok(());
        };
        if retirement.streams.len() != facts.workers.len() {
            return Err("stream ownership is incomplete".into());
        }
        let mut pinned = BTreeSet::new();
        let mut control_ids = BTreeSet::new();
        for (worker, stream) in &retirement.streams {
            if facts.workers.get(worker) != Some(&stream.incarnation) {
                return Err("stream belongs to an obsolete incarnation".into());
            }
            if let Some(ack) = stream.acknowledged
                && (ack.effect.0 == 0
                    || ack.effect != ack.effects_through
                    || ack.effect.0 >= facts.next_effect
                    || ack.attempts_through.0 >= facts.next_attempt
                    || ack.reservations_through.0 >= facts.next_reservation
                    || !control_ids.insert(ack.effect))
            {
                return Err("invalid acknowledged floors".into());
            }
            let Some(certificate) = &stream.pending else {
                continue;
            };
            if !within_certificate_bounds(certificate)
                || certificate.effect.0 == 0
                || certificate.effect != certificate.effects_through
                || certificate.effect.0 >= facts.next_effect
                || certificate.attempts_through.0 >= facts.next_attempt
                || certificate.reservations_through.0 >= facts.next_reservation
                || !control_ids.insert(certificate.effect)
            {
                return Err("invalid pending certificate frontier".into());
            }
            if let Some(ack) = stream.acknowledged
                && (certificate.effects_through <= ack.effects_through
                    || certificate.attempts_through < ack.attempts_through
                    || certificate.reservations_through < ack.reservations_through)
            {
                return Err("pending frontier regressed".into());
            }
            let mut previous = AttemptId(0);
            let mut retained = BTreeSet::new();
            for binding in &certificate.retain {
                if binding.attempt <= previous
                    || !pinned.insert(binding.attempt)
                    || binding.attempt > certificate.attempts_through
                    || binding.dispatch_effect > certificate.effects_through
                    || binding.reservation > certificate.reservations_through
                    || binding
                        .cancel_effect
                        .is_some_and(|cancel| cancel > certificate.effects_through)
                {
                    return Err("invalid retained ordering or cut".into());
                }
                previous = binding.attempt;
                retained.insert(binding.attempt);
                let current = facts
                    .attempts
                    .get(&binding.attempt)
                    .ok_or("pinned attempt was removed")?;
                if current.worker != *worker
                    || current.incarnation != stream.incarnation
                    || current.binding.execution != binding.execution
                    || current.binding.dispatch_effect != binding.dispatch_effect
                    || current.binding.reservation != binding.reservation
                    || match (binding.cancel_effect, current.binding.cancel_effect) {
                        (Some(old), Some(now)) => old != now,
                        (None, Some(now)) => now <= certificate.effects_through,
                        (None, None) => false,
                        (Some(_), None) => true,
                    }
                {
                    return Err("retained identity changed".into());
                }
            }
            if facts.attempts.values().any(|attempt| {
                attempt.worker == *worker
                    && attempt.incarnation == stream.incarnation
                    && !attempt.released
                    && attempt.binding.attempt <= certificate.attempts_through
                    && !retained.contains(&attempt.binding.attempt)
            }) {
                return Err("unreleased obligation omitted".into());
            }
        }
        if facts.attempts.values().any(|attempt| {
            control_ids.contains(&attempt.binding.dispatch_effect)
                || attempt
                    .binding
                    .cancel_effect
                    .is_some_and(|cancel| control_ids.contains(&cancel))
        }) {
            return Err("control identity aliases a worker command".into());
        }
        Ok(())
    }
}

const fn fresh_stream(incarnation: IncarnationId) -> ReceiptStream {
    ReceiptStream {
        incarnation,
        acknowledged: None,
        pending: None,
        dirty: true,
    }
}

fn within_certificate_bounds(certificate: &ReceiptCertificate) -> bool {
    certificate.retain.len() <= MAX_RECEIPT_BINDINGS
        && serde_json::to_vec(certificate)
            .is_ok_and(|bytes| bytes.len() <= MAX_RECEIPT_CERTIFICATE_BYTES)
        && protobuf_certificate_len(certificate) <= 64 * 1024
}

const fn varint_len(mut value: u64) -> usize {
    let mut count = 1;
    while value >= 128 {
        count += 1;
        value >>= 7;
    }
    count
}

const fn scalar_len(value: u64) -> usize {
    if value == 0 { 0 } else { 1 + varint_len(value) }
}

/// Independent protobuf-size arithmetic for the agreed public schema. No
/// production encoder or prost-generated message implementation is used.
pub(crate) fn protobuf_certificate_len(certificate: &ReceiptCertificate) -> usize {
    let header = [
        certificate.effect.0,
        certificate.attempts_through.0,
        certificate.effects_through.0,
        certificate.reservations_through.0,
    ]
    .into_iter()
    .map(scalar_len)
    .sum::<usize>();
    header
        + certificate
            .retain
            .iter()
            .map(|binding| {
                let body = [
                    binding.attempt.0,
                    binding.execution.0,
                    binding.dispatch_effect.0,
                    binding.reservation.0,
                    binding.cancel_effect.map_or(0, |effect| effect.0),
                ]
                .into_iter()
                .map(scalar_len)
                .sum::<usize>();
                1 + varint_len(u64::try_from(body).expect("five bounded scalar fields")) + body
            })
            .sum::<usize>()
}
