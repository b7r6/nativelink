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

//! Activated, bounded retirement intents for worker receipt ledgers.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use super::{
    AttemptId, EffectId, EffectKind, FIXED_ROW_BYTES, IncarnationId, MAX_RECEIPT_BINDINGS,
    MAX_RECEIPT_CERTIFICATE_BYTES, MAX_STATE_BYTES, PayloadSizeLimit, ReceiptBinding,
    ReceiptCertificate, ReceiptFloors, ReceiptRetirement, ReceiptStream, Rejection, Reply,
    ReservationId, SNAPSHOT_VERSION, State, WorkerId,
};

impl State {
    /// Called by the committed format activation transaction. Installing an
    /// executable or reading this accessor does not activate a new authority.
    pub fn enable_receipt_retirement(&mut self) -> Result<(), String> {
        if self.receipt_retirement.is_some() {
            return Ok(());
        }
        self.check_invariants()?;
        let mut candidate = self.clone();
        candidate.receipt_retirement = Some(ReceiptRetirement {
            streams: self
                .workers
                .iter()
                .map(|(worker, entry)| (*worker, ReceiptStream::fresh(entry.incarnation)))
                .collect(),
        });
        candidate.check_receipt_invariants()?;
        *self = candidate;
        Ok(())
    }

    #[must_use]
    pub const fn receipt_retirement_enabled(&self) -> bool {
        self.receipt_retirement.is_some()
    }

    /// Durable outbox and acknowledgement state. External delivery requires a
    /// committed read and the current authenticated recipient session.
    #[must_use]
    pub const fn receipt_retirement(&self) -> Option<&ReceiptRetirement> {
        self.receipt_retirement.as_ref()
    }

    pub(super) const fn snapshot_version(&self) -> u32 {
        if self.receipt_retirement_enabled() {
            6
        } else {
            SNAPSHOT_VERSION
        }
    }

    pub(super) fn register_receipt_stream(&mut self, worker: WorkerId, incarnation: IncarnationId) {
        if let Some(retirement) = &mut self.receipt_retirement {
            let stream = retirement
                .streams
                .entry(worker)
                .or_insert_with(|| ReceiptStream::fresh(incarnation));
            if stream.incarnation != incarnation {
                // The accepted replacement fences the old recipient. Its
                // physical reservations and cleanup intents remain untouched.
                *stream = ReceiptStream::fresh(incarnation);
            }
        }
    }

    pub(super) fn release_reservation(&mut self, reservation: ReservationId) {
        let entry = self
            .reservations
            .get_mut(&reservation)
            .expect("attempt reservation");
        if !entry.released {
            entry.released = true;
            if let Some(stream) = self
                .receipt_retirement
                .as_mut()
                .and_then(|state| state.streams.get_mut(&entry.worker))
                .filter(|stream| stream.incarnation == entry.incarnation)
            {
                stream.dirty = true;
            }
        }
    }

    pub(super) fn receipt_attempts_pinned(&self, attempts: &BTreeSet<AttemptId>) -> bool {
        self.receipt_retirement.as_ref().is_some_and(|state| {
            state
                .streams
                .values()
                .filter_map(|stream| stream.pending.as_ref())
                .any(|certificate| {
                    certificate
                        .retain
                        .iter()
                        .any(|binding| attempts.contains(&binding.attempt))
                })
        })
    }

    pub(super) fn issue_receipts(
        &mut self,
        worker: WorkerId,
        incarnation: IncarnationId,
        after_ack: Option<EffectId>,
    ) -> Reply {
        let Some(retirement) = &self.receipt_retirement else {
            return Reply::Rejected(Rejection::Invalid);
        };
        let Some(stream) = retirement.streams.get(&worker) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        if stream.incarnation != incarnation
            || stream.acknowledged.map(|floors| floors.effect) != after_ack
        {
            return Reply::Rejected(Rejection::Stale);
        }
        if stream.pending.is_some() || !stream.dirty {
            return Reply::Ignored;
        }
        let retained: Vec<_> = self.attempts.values()
            .filter(|attempt| attempt.worker == worker && attempt.incarnation == incarnation
                && !self.reservations[&attempt.reservation].released)
            .take(MAX_RECEIPT_BINDINGS + 1)
            .map(|attempt| ReceiptBinding {
                attempt: attempt.id, execution: attempt.execution,
                dispatch_effect: attempt.dispatch_effect, reservation: attempt.reservation,
                cancel_effect: self.effects.values().find_map(|effect| {
                    matches!(effect.kind, EffectKind::Cancel { attempt: id, .. } if id == attempt.id)
                        .then_some(effect.id)
                }),
            }).collect();
        if retained.len() > MAX_RECEIPT_BINDINGS {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let certificate = ReceiptCertificate {
            effect: EffectId(self.next_effect),
            attempts_through: AttemptId(self.next_attempt - 1),
            effects_through: EffectId(self.next_effect),
            reservations_through: ReservationId(self.next_reservation - 1),
            retain: retained,
        };
        if !encoded_within(&certificate, MAX_RECEIPT_CERTIFICATE_BYTES) {
            return Reply::Rejected(Rejection::Overloaded);
        }
        let Some(next_effect) = self.next_effect.checked_add(1) else {
            return Reply::Rejected(Rejection::Overloaded);
        };
        // Check the already promised finalization IDs before allocating this
        // optional intent. A refused issue leaves every counter unchanged.
        let old_next_effect = self.next_effect;
        self.next_effect = next_effect;
        if !self.identities_have_headroom(false) {
            self.next_effect = old_next_effect;
            return Reply::Rejected(Rejection::Overloaded);
        }
        let stream = self
            .receipt_retirement
            .as_mut()
            .expect("activated")
            .streams
            .get_mut(&worker)
            .expect("current recipient");
        stream.pending = Some(certificate);
        stream.dirty = false;
        Reply::Applied
    }

    pub(super) fn acknowledge_receipts(
        &mut self,
        worker: WorkerId,
        incarnation: IncarnationId,
        certificate: &ReceiptCertificate,
    ) -> Reply {
        let Some(retirement) = &mut self.receipt_retirement else {
            return Reply::Rejected(Rejection::Invalid);
        };
        let Some(stream) = retirement.streams.get_mut(&worker) else {
            return Reply::Rejected(Rejection::Unknown);
        };
        if stream.incarnation != incarnation || stream.pending.as_ref() != Some(certificate) {
            return Reply::Rejected(Rejection::Stale);
        }
        stream.acknowledged = Some(certificate.floors());
        stream.pending = None;
        Reply::Applied
    }

    pub(super) fn check_receipt_invariants(&self) -> Result<(), String> {
        let Some(retirement) = &self.receipt_retirement else {
            return Ok(());
        };
        let invalid = || "RECEIPT1: bounded exact recipient retirement intent".to_owned();
        if retirement.streams.len() != self.workers.len() {
            return Err(invalid());
        }
        let mut certificate_ids = BTreeSet::new();
        let mut retained_cancellations = BTreeSet::new();
        for (worker, stream) in &retirement.streams {
            let Some(owner) = self.workers.get(worker) else {
                return Err(invalid());
            };
            if owner.incarnation != stream.incarnation {
                return Err(invalid());
            }
            for floors in stream
                .acknowledged
                .iter()
                .copied()
                .chain(stream.pending.iter().map(ReceiptCertificate::floors))
            {
                if floors.effect.0 == 0
                    || floors.effect != floors.effects_through
                    || floors.effect.0 >= self.next_effect
                    || floors.attempts_through.0 >= self.next_attempt
                    || floors.reservations_through.0 >= self.next_reservation
                    || floors.attempts_through.0 != floors.reservations_through.0
                    || self.effects.contains_key(&floors.effect)
                    || !certificate_ids.insert(floors.effect)
                    || self.attempts.values().any(|attempt| {
                        // All three allocators are captured at the same
                        // committed cut, including work on other recipients.
                        let captured = attempt.id <= floors.attempts_through;
                        attempt.dispatch_effect == floors.effect
                            || captured != (attempt.dispatch_effect < floors.effect)
                            || captured != (attempt.reservation <= floors.reservations_through)
                    })
                {
                    return Err(invalid());
                }
            }
            let mut header = stream.clone();
            if let Some(certificate) = &mut header.pending {
                certificate.retain.clear();
            }
            // A worker and optional hold already reserve two fixed rows. Charge
            // the certificate header in that group; bindings are charged with
            // the attempt rows they pin, even after physical release.
            let worker_bytes = encoded_length(&BTreeMap::from([(*worker, owner)]))?
                .checked_sub(encoded_length(&owner.properties)?)
                .ok_or_else(invalid)?;
            let hold_bytes = self.holds.get(worker).map_or(Ok(0), |hold| {
                encoded_length(&BTreeMap::from([(*worker, hold)]))
            })?;
            let header_bytes = encoded_length(&BTreeMap::from([(*worker, header)]))?;
            if worker_bytes + hold_bytes + header_bytes > 2 * FIXED_ROW_BYTES {
                return Err(invalid());
            }
            let Some(certificate) = &stream.pending else {
                continue;
            };
            if certificate.retain.len() > MAX_RECEIPT_BINDINGS
                || !encoded_within(certificate, MAX_RECEIPT_CERTIFICATE_BYTES)
                || stream.acknowledged.is_some_and(|old| {
                    let previously_allocated_effect = old.effects_through;
                    certificate.effect <= previously_allocated_effect
                        || certificate.attempts_through < old.attempts_through
                        || certificate.reservations_through < old.reservations_through
                })
            {
                return Err(invalid());
            }
            let mut previous = None;
            let mut command_ids = BTreeSet::new();
            let mut reservation_ids = BTreeSet::new();
            for binding in &certificate.retain {
                let Some(attempt) = self.attempts.get(&binding.attempt) else {
                    return Err(invalid());
                };
                if previous.is_some_and(|id| id >= binding.attempt)
                    || attempt.worker != *worker
                    || attempt.incarnation != stream.incarnation
                    || attempt.execution != binding.execution
                    || attempt.dispatch_effect != binding.dispatch_effect
                    || attempt.reservation != binding.reservation
                    || binding.attempt > certificate.attempts_through
                    || binding.reservation > certificate.reservations_through
                    || binding.dispatch_effect >= certificate.effect
                    || !command_ids.insert(binding.dispatch_effect)
                    || !reservation_ids.insert(binding.reservation)
                    || binding.cancel_effect.is_some_and(|id| {
                        attempt.authorized
                            || id <= binding.dispatch_effect
                            || id >= certificate.effect
                            || !command_ids.insert(id)
                            || !retained_cancellations.insert(id)
                            || self.attempts.values().any(|a| a.dispatch_effect == id)
                            || self.effects.get(&id).is_some_and(|effect| {
                                !matches!(effect.kind,
                            EffectKind::Cancel { attempt: owner, .. } if owner == binding.attempt)
                            })
                    })
                {
                    return Err(invalid());
                }
                if encoded_length(&BTreeMap::from([(binding.attempt, attempt)]))?
                    + encoded_length(binding)?
                    + 1
                    > FIXED_ROW_BYTES
                {
                    return Err(invalid());
                }
                let pending_cancel = self.effects.values().find(|effect| {
                    matches!(effect.kind,
                    EffectKind::Cancel { attempt: owner, .. } if owner == binding.attempt)
                });
                if pending_cancel.is_some_and(|effect| {
                    if effect.id < certificate.effect {
                        binding.cancel_effect != Some(effect.id)
                    } else {
                        binding.cancel_effect.is_some()
                    }
                }) {
                    return Err(invalid());
                }
                previous = Some(binding.attempt);
            }
            // Every still-unreleased attempt inside the captured prefix needs
            // an exception. New attempts necessarily have higher IDs.
            if self.attempts.values().any(|attempt| {
                attempt.worker == *worker
                    && attempt.incarnation == stream.incarnation
                    && attempt.id <= certificate.attempts_through
                    && !self.reservations[&attempt.reservation].released
                    && certificate
                        .retain
                        .binary_search_by_key(&attempt.id, |binding| binding.attempt)
                        .is_err()
            }) {
                return Err(invalid());
            }
        }
        if !certificate_ids.is_disjoint(&retained_cancellations) {
            return Err(invalid());
        }
        Ok(())
    }
}

impl ReceiptStream {
    const fn fresh(incarnation: IncarnationId) -> Self {
        Self {
            incarnation,
            acknowledged: None,
            pending: None,
            dirty: true,
        }
    }
}

impl ReceiptCertificate {
    #[must_use]
    pub const fn floors(&self) -> ReceiptFloors {
        ReceiptFloors {
            effect: self.effect,
            attempts_through: self.attempts_through,
            effects_through: self.effects_through,
            reservations_through: self.reservations_through,
        }
    }
}

fn encoded_within(value: &impl Serialize, limit: usize) -> bool {
    serde_json::to_writer(&mut PayloadSizeLimit { used: 0, limit }, value).is_ok()
}

fn encoded_length(value: &impl Serialize) -> Result<usize, String> {
    let mut count = PayloadSizeLimit {
        used: 0,
        limit: MAX_STATE_BYTES,
    };
    serde_json::to_writer(&mut count, value).map_err(|error| error.to_string())?;
    Ok(count.used)
}
