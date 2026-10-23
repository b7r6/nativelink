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

//! Set-based specification of the accepted-history component only.
//! Hash primitives are external; framing and transitions are independent.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use super::types::{
    ActionDigest, Algorithm, Association, Binding, Event, Facts, HEADER_CREDIT, MAX_PREFIX_BYTES,
    MAX_RESPONSE_BYTES, Namespace, ObjectKey, Outcome, Phase, PublicationOutcome, PublicationState,
    ROW_CREDIT, Recipe, Reject,
};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Model {
    pub(crate) namespace: Option<Namespace>,
    pub(crate) reserved: BTreeSet<u64>,
    pub(crate) associations: BTreeMap<u64, Association>,
}

const fn valid_binding(binding: Binding) -> bool {
    binding.attempt != 0
}

fn varint(mut value: u64, output: &mut Vec<u8>) {
    loop {
        let low = u8::try_from(value & 127).expect("seven bits");
        value >>= 7;
        output.push(if value == 0 { low } else { low | 128 });
        if value == 0 {
            break;
        }
    }
}

/// Canonical protobuf fields: `HistoricalExecuteResponse.action_digest=1` and
/// `execute_response=3`. The embedded response is never decoded or re-encoded.
pub(crate) fn recipe(
    namespace: Namespace,
    action: &ActionDigest,
    response: &[u8],
) -> Result<Recipe, Reject> {
    if action.size_bytes > i64::MAX as u64 || response.len() > MAX_RESPONSE_BYTES {
        return Err(Reject::InvalidRecipe);
    }
    let alphabet = b"0123456789abcdef";
    let mut digest = vec![10, 64];
    for byte in action.hash {
        digest.push(alphabet[usize::from(byte >> 4)]);
        digest.push(alphabet[usize::from(byte & 15)]);
    }
    // Proto3 omits a zero scalar, but retains the present message field.
    if action.size_bytes != 0 {
        digest.push(16);
        varint(action.size_bytes, &mut digest);
    }
    let mut prefix = vec![10];
    varint(digest.len() as u64, &mut prefix);
    prefix.extend_from_slice(&digest);
    prefix.push(26);
    varint(response.len() as u64, &mut prefix);
    if prefix.len() > MAX_PREFIX_BYTES {
        return Err(Reject::InvalidRecipe);
    }
    let hash = match action.algorithm {
        Algorithm::Sha256 => {
            let mut hash = Sha256::new();
            hash.update(&prefix);
            hash.update(response);
            hash.finalize().into()
        }
        Algorithm::Blake3 => {
            let mut hash = blake3::Hasher::new();
            hash.update(&prefix);
            hash.update(response);
            *hash.finalize().as_bytes()
        }
    };
    Ok(Recipe {
        object: ObjectKey {
            namespace,
            algorithm: action.algorithm,
            hash,
            size_bytes: (prefix.len() + response.len()) as u64,
        },
        prefix,
        response_sha256: Sha256::digest(response).into(),
    })
}

impl Model {
    pub(crate) fn metadata_credit(&self) -> usize {
        usize::from(self.namespace.is_some()) * HEADER_CREDIT
            + (self.reserved.len() + self.associations.len()) * ROW_CREDIT
    }

    pub(crate) fn effect_slots(&self) -> usize {
        self.reserved.len()
            + self
                .associations
                .values()
                .filter(|row| row.state == PublicationState::Pending)
                .count()
    }

    fn capacity(&self, facts: &Facts, next_effect: u64) -> Result<(), Reject> {
        if next_effect == 0 {
            return Err(Reject::Facts);
        }
        if self
            .metadata_credit()
            .checked_add(facts.other_metadata_bytes)
            .is_none_or(|used| used > facts.metadata_capacity)
            || self
                .effect_slots()
                .checked_add(facts.other_effect_slots)
                .is_none_or(|used| used > facts.effect_capacity)
        {
            return Err(Reject::Capacity);
        }
        next_effect
            .checked_add(facts.other_future_effect_ids)
            .and_then(|end| end.checked_add(self.reserved.len() as u64))
            .ok_or(Reject::Headroom)?;
        Ok(())
    }

    /// Failed transitions leave every byte of metadata unchanged. Facts and
    /// lifecycle state are immutable inputs; callers advance their own oracle.
    pub(crate) fn apply(&mut self, event: &Event, facts: &Facts) -> Result<Outcome, Reject> {
        let mut after = self.clone();
        let outcome = after.transition(event, facts)?;
        let next_effect = match outcome {
            Outcome::Allocated { next_effect, .. } => next_effect,
            _ => facts.next_effect,
        };
        after.capacity(facts, next_effect)?;
        *self = after;
        Ok(outcome)
    }

    fn transition(&mut self, event: &Event, facts: &Facts) -> Result<Outcome, Reject> {
        if let Event::Activate { namespace } = event {
            if facts.qualified_namespace != Some(*namespace) {
                return Err(Reject::Namespace);
            }
            if let Some(active) = self.namespace {
                return if active == *namespace {
                    Ok(Outcome::Ignored)
                } else {
                    Err(Reject::Namespace)
                };
            }
            self.namespace = Some(*namespace);
            for (execution, fact) in &facts.executions {
                if fact.phase == Phase::Open {
                    if *execution == 0 || fact.action.size_bytes > i64::MAX as u64 {
                        return Err(Reject::Facts);
                    }
                    self.reserved.insert(*execution);
                }
            }
            return Ok(Outcome::Applied);
        }
        let namespace = self.namespace.ok_or(Reject::Disabled)?;
        if facts.qualified_namespace != Some(namespace) {
            return Err(Reject::Namespace);
        }
        match event {
            Event::Activate { .. } => unreachable!("handled above"),
            Event::Admit { execution } => {
                let fact = facts.executions.get(execution).ok_or(Reject::Facts)?;
                if *execution == 0 || fact.action.size_bytes > i64::MAX as u64 {
                    return Err(Reject::Facts);
                }
                if fact.phase != Phase::Open || self.associations.contains_key(execution) {
                    return Ok(Outcome::Ignored);
                }
                Ok(if self.reserved.insert(*execution) {
                    Outcome::Applied
                } else {
                    Outcome::Ignored
                })
            }
            Event::ReleaseUnused { execution } => {
                let fact = facts.executions.get(execution).ok_or(Reject::Facts)?;
                if fact.phase != Phase::OtherTerminal {
                    return Err(Reject::Facts);
                }
                Ok(if self.reserved.remove(execution) {
                    Outcome::Applied
                } else {
                    Outcome::Ignored
                })
            }
            Event::AcceptTerminal {
                execution,
                proposal,
            } => {
                let fact = facts.executions.get(execution).ok_or(Reject::Facts)?;
                let Phase::WorkerTerminal {
                    original, response, ..
                } = &fact.phase
                else {
                    return Ok(Outcome::Ignored);
                };
                let derived = recipe(namespace, &fact.action, response)?;
                if proposal.as_ref().is_some_and(|given| *given != derived) {
                    return Err(Reject::InvalidRecipe);
                }
                if let Some(existing) = self.associations.get(execution) {
                    return if existing.original == *original && existing.recipe == derived {
                        Ok(Outcome::Ignored)
                    } else {
                        Err(Reject::Facts)
                    };
                }
                if !self.reserved.contains(execution)
                    || !valid_binding(*original)
                    || fact.authorized != Some(*original)
                {
                    return Ok(Outcome::Ignored);
                }
                let next_effect = facts.next_effect.checked_add(1).ok_or(Reject::Headroom)?;
                if self
                    .associations
                    .values()
                    .any(|row| (row.epoch, row.effect) == (facts.epoch, facts.next_effect))
                {
                    return Err(Reject::Facts);
                }
                self.reserved.remove(execution);
                self.associations.insert(
                    *execution,
                    Association {
                        epoch: facts.epoch,
                        effect: facts.next_effect,
                        execution: *execution,
                        original: *original,
                        recipe: derived,
                        state: PublicationState::Pending,
                    },
                );
                Ok(Outcome::Allocated {
                    effect: facts.next_effect,
                    next_effect,
                })
            }
            Event::ObservePublication {
                epoch,
                effect,
                object,
                outcome,
            } => {
                let Some(row) = self
                    .associations
                    .values_mut()
                    .find(|row| row.epoch == *epoch && row.effect == *effect)
                else {
                    return Ok(Outcome::Ignored);
                };
                if row.recipe.object != *object
                    || row.state == PublicationState::Materialized
                    || *outcome != PublicationOutcome::Stored
                {
                    return Ok(Outcome::Ignored);
                }
                row.state = PublicationState::Materialized;
                Ok(Outcome::Applied)
            }
            Event::Retire { execution } => {
                if self.reserved.contains(execution)
                    || self
                        .associations
                        .get(execution)
                        .is_some_and(|row| row.state == PublicationState::Pending)
                {
                    return Err(Reject::Busy);
                }
                if !facts
                    .executions
                    .get(execution)
                    .is_some_and(|fact| fact.retirement_permitted)
                {
                    return Err(Reject::Busy);
                }
                Ok(if self.associations.remove(execution).is_some() {
                    Outcome::Applied
                } else {
                    Outcome::Ignored
                })
            }
        }
    }

    pub(crate) fn operation(&self, operation: u64, facts: &Facts) -> Option<&Association> {
        self.associations.values().find(|row| {
            facts
                .executions
                .get(&row.execution)
                .is_some_and(|fact| fact.accepted_operations.contains(&operation))
        })
    }

    /// Pending metadata pins the existing immutable response and provenance.
    /// This is not a transitive root for the action's input/output digests.
    pub(crate) fn pins(&self) -> BTreeSet<(u64, u64)> {
        self.associations
            .values()
            .filter(|row| row.state == PublicationState::Pending)
            .map(|row| (row.execution, row.original.attempt))
            .collect()
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("finite metadata serializes")
    }

    /// The reference snapshot is an oracle artifact, not a production format.
    /// No activation, backfill, namespace selection, or repair happens here.
    pub(crate) fn restore(bytes: &[u8], facts: &Facts) -> Result<Self, Reject> {
        let state: Self = serde_json::from_slice(bytes).map_err(|_| Reject::Snapshot)?;
        if state.namespace.is_none() {
            if !state.reserved.is_empty() || !state.associations.is_empty() {
                return Err(Reject::Snapshot);
            }
            return Ok(state);
        }
        if state.namespace != facts.qualified_namespace {
            return Err(Reject::Namespace);
        }
        state.capacity(facts, facts.next_effect)?;
        let namespace = state.namespace.ok_or(Reject::Snapshot)?;
        let mut effects = BTreeSet::new();
        for execution in &state.reserved {
            let fact = facts.executions.get(execution).ok_or(Reject::Snapshot)?;
            if *execution == 0
                || fact.phase != Phase::Open
                || fact.action.size_bytes > i64::MAX as u64
                || state.associations.contains_key(execution)
            {
                return Err(Reject::Snapshot);
            }
        }
        for (execution, row) in &state.associations {
            let fact = facts.executions.get(execution).ok_or(Reject::Snapshot)?;
            let Phase::WorkerTerminal {
                original, response, ..
            } = &fact.phase
            else {
                return Err(Reject::Snapshot);
            };
            if *execution == 0
                || row.execution != *execution
                || row.epoch != facts.epoch
                || row.effect == 0
                || row.effect >= facts.next_effect
                || row.original != *original
                || !valid_binding(row.original)
                || !effects.insert((row.epoch, row.effect))
                || row.recipe != recipe(namespace, &fact.action, response)?
            {
                return Err(Reject::Snapshot);
            }
        }
        Ok(state)
    }
}
