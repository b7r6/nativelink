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
use sha2::{Digest, Sha256};

use super::types::*;

type Result<T> = std::result::Result<T, Reject>;

pub(crate) fn object(bytes: &[u8], algorithm: Algorithm) -> Object {
    let hash = match algorithm {
        Algorithm::Sha256 => Sha256::digest(bytes).into(),
        Algorithm::Blake3 => *blake3::hash(bytes).as_bytes(),
    };
    Object {
        algorithm,
        hash,
        bytes: bytes.len() as u64,
    }
}

fn check_payload(bytes: &[u8], expected: &Object) -> Result<()> {
    if bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(Reject::Bounds);
    }
    if object(bytes, expected.algorithm) != *expected {
        return Err(Reject::Invalid);
    }
    Ok(())
}

fn valid_generation(key: Generation) -> bool {
    key.number != 0 && key.incarnation != [0; 16]
}

fn request_count(legacy: &Legacy) -> usize {
    legacy
        .effects
        .values()
        .map(|row| row.requests.len())
        .sum::<usize>()
        + legacy
            .histories
            .values()
            .map(|row| row.requests.len())
            .sum::<usize>()
}

fn requests(legacy: &Legacy) -> BTreeMap<RequestRef, Delivery> {
    legacy
        .effects
        .values()
        .flat_map(|effect| {
            effect.requests.iter().map(move |(id, value)| {
                (
                    RequestRef {
                        kind: EffectKind::Sink,
                        effect: effect.id,
                        request: *id,
                    },
                    *value,
                )
            })
        })
        .chain(legacy.histories.values().flat_map(|effect| {
            effect.requests.iter().map(move |(id, value)| {
                (
                    RequestRef {
                        kind: EffectKind::History,
                        effect: effect.id,
                        request: *id,
                    },
                    *value,
                )
            })
        }))
        .collect()
}

fn effects(legacy: &Legacy) -> BTreeMap<EffectRef, Exposure> {
    legacy
        .effects
        .values()
        .map(|row| {
            (
                EffectRef {
                    kind: EffectKind::Sink,
                    effect: row.id,
                },
                row.exposure.clone(),
            )
        })
        .chain(legacy.histories.values().map(|row| {
            (
                EffectRef {
                    kind: EffectKind::History,
                    effect: row.id,
                },
                row.exposure.clone(),
            )
        }))
        .collect()
}

const fn effect_of(request: RequestRef) -> EffectRef {
    EffectRef {
        kind: request.kind,
        effect: request.effect,
    }
}

fn validate_observations(
    legacy: &Legacy,
    observations: &BTreeSet<Observation>,
    quiesced: &BTreeSet<RequestRef>,
) -> Result<()> {
    let known = requests(legacy);
    if observations.len() > MAX_REQUESTS * 3
        || quiesced.iter().any(|request| !known.contains_key(request))
    {
        return Err(Reject::Invalid);
    }
    for seen in observations {
        let initial = known.get(&seen.request).ok_or(Reject::Invalid)?;
        if seen.outcome == Delivery::Issued
            || (matches!(initial, Delivery::Stored | Delivery::Failed)
                && matches!(seen.outcome, Delivery::Stored | Delivery::Failed)
                && *initial != seen.outcome)
        {
            return Err(Reject::Conflict);
        }
        if matches!(seen.outcome, Delivery::Stored | Delivery::Failed)
            && observations.iter().any(|other| {
                other.request == seen.request
                    && matches!(other.outcome, Delivery::Stored | Delivery::Failed)
                    && other.outcome != seen.outcome
            })
        {
            return Err(Reject::Conflict);
        }
    }
    Ok(())
}

fn validate_legacy(legacy: &Legacy) -> Result<()> {
    if legacy.terminals.len() > MAX_TERMINALS
        || legacy.effects.len() + legacy.histories.len() > MAX_EFFECTS
        || legacy.roots.len() > MAX_ROOTS
        || request_count(legacy) > MAX_REQUESTS
    {
        return Err(Reject::Bounds);
    }
    let mut operations = BTreeSet::new();
    for (id, terminal) in &legacy.terminals {
        if *id == 0
            || *id != terminal.id
            || terminal.origin.accepted_commit == 0
            || terminal.origin.execution == 0
            || terminal.origin.attempt == 0
            || terminal.action.is_empty()
            || terminal.action.len() > 256
            || terminal.operations.is_empty()
            || terminal.operations.len() > MAX_REQUESTS
            || terminal
                .operations
                .iter()
                .any(|id| *id == 0 || !operations.insert(*id))
            || terminal.response.is_empty()
            || terminal.response.len() > MAX_PAYLOAD_BYTES
        {
            return Err(Reject::Invalid);
        }
        check_payload(&terminal.result_bytes, &terminal.result)?;
    }
    for (id, effect) in &legacy.effects {
        let terminal = legacy
            .terminals
            .get(&effect.terminal)
            .ok_or(Reject::Invalid)?;
        if *id == 0
            || *id != effect.id
            || legacy.histories.contains_key(id)
            || effect.destination != legacy.old_sink
            || !terminal.ac_eligible
            || effect.payload != terminal.result_bytes
            || effect.requests.contains_key(&0)
        {
            return Err(Reject::Invalid);
        }
    }
    for (id, effect) in &legacy.histories {
        let terminal = legacy
            .terminals
            .get(&effect.terminal)
            .ok_or(Reject::Invalid)?;
        if *id == 0
            || *id != effect.id
            || effect.origin != terminal.origin
            || effect.requests.contains_key(&0)
        {
            return Err(Reject::Invalid);
        }
        check_payload(&effect.payload, &effect.object)?;
    }
    let expected: BTreeSet<_> = legacy
        .terminals
        .keys()
        .map(|id| Subject::Result(*id))
        .chain(legacy.histories.keys().map(|id| Subject::History(*id)))
        .collect();
    let mut subjects = BTreeSet::new();
    for (id, root) in &legacy.roots {
        if *id == 0
            || *id != root.id
            || root.required.is_empty()
            || root.required.len() > MAX_MEMBERS
            || !subjects.insert(root.subject)
        {
            return Err(Reject::Invalid);
        }
        let (expected_object, role) = match root.subject {
            Subject::Result(id) => (
                &legacy.terminals.get(&id).ok_or(Reject::Invalid)?.result,
                Role::Result,
            ),
            Subject::History(id) => (
                &legacy.histories.get(&id).ok_or(Reject::Invalid)?.object,
                Role::History,
            ),
        };
        if &root.object != expected_object
            || !root
                .required
                .iter()
                .any(|member| member.object == root.object && member.role == role)
        {
            return Err(Reject::Incomplete);
        }
        let mut logical = BTreeMap::new();
        let mut physical = BTreeMap::new();
        for member in &root.required {
            if !valid_generation(member.generation)
                || member.object.bytes > i64::MAX as u64
                || logical
                    .insert(member.object.clone(), member.generation)
                    .is_some_and(|prior| prior != member.generation)
                || physical
                    .insert(member.generation, member.object.clone())
                    .is_some_and(|prior| prior != member.object)
            {
                return Err(Reject::Invalid);
            }
        }
    }
    if subjects != expected {
        return Err(Reject::Incomplete);
    }
    validate_observations(legacy, &legacy.late_outcomes, &legacy.request_quiescence)
}

fn validate_facts(facts: &Facts) -> Result<()> {
    if facts.catalogue.len() > MAX_MEMBERS * MAX_ROOTS
        || facts.protections.len() > MAX_ROOTS * 2
        || facts.bindings.len() > MAX_ROOTS
        || facts.fences.len() > MAX_ROOTS
        || facts.quiescent.len() > MAX_REQUESTS
        || facts.quiescent_effects.len() > MAX_EFFECTS
        || facts.quiescent_domains.len() > MAX_ROOTS
        || facts.current_workers.len() > MAX_TERMINALS
        || facts.migration_publishers.len() > MAX_TERMINALS
        || facts.cold.len() > MAX_ROOTS
    {
        return Err(Reject::Bounds);
    }
    let mut keys = BTreeSet::new();
    if facts.catalogue_revision == 0
        || facts
            .catalogue
            .iter()
            .any(|row| !valid_generation(row.key) || !keys.insert(row.key))
    {
        return Err(Reject::Invalid);
    }
    let mut namespaces = BTreeSet::new();
    if facts
        .bindings
        .iter()
        .any(|binding| binding.physical_domain == [0; 32] || !namespaces.insert(binding.namespace))
    {
        return Err(Reject::Namespace);
    }
    namespaces.clear();
    if facts
        .fences
        .iter()
        .any(|fence| fence.revision == 0 || !namespaces.insert(fence.namespace))
    {
        return Err(Reject::Invalid);
    }
    Ok(())
}

fn binding(facts: &Facts, namespace: Namespace) -> Result<&Binding> {
    facts
        .bindings
        .iter()
        .find(|binding| binding.namespace == namespace)
        .ok_or(Reject::Namespace)
}

fn protection_shape(root: &Root, proof: &Protection) -> Result<()> {
    if proof.id == 0
        || proof.root != root.id
        || proof.catalogue_revision == 0
        || !proof.complete
        || !proof.verified
        || proof.until == 0
    {
        return Err(Reject::Unprotected);
    }
    if proof.typed != root.required {
        return Err(Reject::Incomplete);
    }
    if proof.protected.len() > MAX_MEMBERS || proof.fences.len() > MAX_ROOTS {
        return Err(Reject::Bounds);
    }
    if root
        .required
        .iter()
        .any(|member| !proof.protected.contains(&member.generation))
    {
        return Err(Reject::Unprotected);
    }
    let namespaces: BTreeSet<_> = proof.protected.iter().map(|key| key.namespace).collect();
    let mut fence_namespaces = BTreeSet::new();
    if proof.protected.iter().any(|key| !valid_generation(*key))
        || proof
            .fences
            .iter()
            .any(|fence| fence.revision == 0 || !fence_namespaces.insert(fence.namespace))
        || fence_namespaces != namespaces
    {
        return Err(Reject::Unprotected);
    }
    Ok(())
}

fn acquire(legacy: &Legacy, root: &Root, evidence: u64, facts: &Facts) -> Result<Protection> {
    let proof = facts
        .protections
        .get(&evidence)
        .ok_or(Reject::Unprotected)?;
    protection_shape(root, proof)?;
    if proof.id != evidence
        || proof.catalogue_revision != facts.catalogue_revision
        || proof.until <= facts.now
    {
        return Err(Reject::Stale);
    }
    if !proof.fences.is_subset(&facts.fences) {
        return Err(Reject::Stale);
    }
    let old = binding(facts, legacy.old_sink)?;
    for key in &proof.protected {
        let row = facts
            .catalogue
            .iter()
            .find(|row| row.key == *key)
            .ok_or(Reject::Unprotected)?;
        if row.state != Readiness::Ready {
            return Err(Reject::Unprotected);
        }
        if binding(facts, key.namespace)?.physical_domain == old.physical_domain {
            return Err(Reject::Namespace);
        }
    }
    for member in &root.required {
        let generation = member.generation;
        if !facts
            .catalogue
            .iter()
            .any(|row| row.key == generation && row.object == member.object)
        {
            return Err(Reject::Stale);
        }
    }
    Ok(proof.clone())
}

fn heads(legacy: &Legacy, activation: &Activation) -> Result<Vec<Head>> {
    if activation.imports.len() > MAX_TERMINALS {
        return Err(Reject::Bounds);
    }
    let mut actions = BTreeSet::new();
    let mut publications = BTreeSet::new();
    let mut result = Vec::new();
    for import in &activation.imports {
        let terminal = legacy
            .terminals
            .get(&import.terminal)
            .ok_or(Reject::Invalid)?;
        if import.publication == 0
            || import.publisher == 0
            || !terminal.ac_eligible
            || !publications.insert(import.publication)
            || !actions.insert(terminal.action.clone())
        {
            return Err(Reject::Invalid);
        }
        result.push(Head {
            action: terminal.action.clone(),
            publication: import.publication,
            terminal: terminal.id,
            publisher: import.publisher,
            origin: terminal.origin,
            commit: activation.commit,
        });
    }
    result.sort_by(|left, right| left.action.cmp(&right.action));
    Ok(result)
}

fn activation_shape(legacy: &Legacy, activation: &Activation) -> Result<()> {
    if activation.commit == 0
        || legacy
            .terminals
            .values()
            .any(|terminal| terminal.origin.accepted_commit >= activation.commit)
    {
        return Err(Reject::Stale);
    }
    if activation.new_namespace == legacy.old_sink {
        return Err(Reject::Namespace);
    }
    if activation.dispositions.keys().ne(legacy.effects.keys())
        || activation.protections.keys().ne(legacy.roots.keys())
    {
        return Err(Reject::Incomplete);
    }
    heads(legacy, activation)?;
    for (id, disposition) in &activation.dispositions {
        if let Disposition::Converted { publication } = disposition {
            let effect = &legacy.effects[id];
            if !activation.imports.iter().any(|import| {
                import.publication == *publication && import.terminal == effect.terminal
            }) {
                return Err(Reject::Invalid);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Model {
    pub(crate) legacy: Legacy,
    pub(crate) activation: Option<Activation>,
    pub(crate) coverage: BTreeMap<u64, Protection>,
    pub(crate) heads: Vec<Head>,
    pub(crate) observations: BTreeSet<Observation>,
    pub(crate) quiesced: BTreeSet<RequestRef>,
    pub(crate) collection_until: Option<u64>,
    pub(crate) namespaces: Option<[Binding; 2]>,
    pub(crate) quiesced_effects: BTreeSet<EffectRef>,
    pub(crate) domain_quiesced: bool,
}

impl Model {
    pub(crate) fn new(legacy: Legacy) -> Result<Self> {
        validate_legacy(&legacy)?;
        Ok(Self {
            observations: legacy.late_outcomes.clone(),
            quiesced: legacy.request_quiescence.clone(),
            quiesced_effects: effects(&legacy)
                .into_iter()
                .filter_map(|(id, exposure)| (exposure == Exposure::ProvenQuiescent).then_some(id))
                .collect(),
            domain_quiesced: legacy.domain_exposure == Exposure::ProvenQuiescent,
            legacy,
            activation: None,
            coverage: BTreeMap::new(),
            heads: Vec::new(),
            collection_until: None,
            namespaces: None,
        })
    }

    pub(crate) fn apply(&mut self, event: &Event, facts: &Facts) -> Result<Outcome> {
        validate_facts(facts)?;
        let mut next = self.clone();
        let outcome = next.reduce(event, facts)?;
        next.validate()?;
        *self = next;
        Ok(outcome)
    }

    fn reduce(&mut self, event: &Event, facts: &Facts) -> Result<Outcome> {
        match event {
            Event::Activate(activation) => {
                if self.activation.is_some() {
                    return Err(Reject::WrongPhase);
                }
                activation_shape(&self.legacy, activation)?;
                let old = binding(facts, self.legacy.old_sink)?;
                let new = binding(facts, activation.new_namespace)?;
                if !old.old_requests_confined
                    || old.physical_domain == new.physical_domain
                    || !facts.cold.contains(&activation.new_namespace)
                {
                    return Err(Reject::Namespace);
                }
                for import in &activation.imports {
                    if !facts.migration_publishers.contains(&import.publisher) {
                        return Err(Reject::Unauthorized);
                    }
                }
                for (id, disposition) in &activation.dispositions {
                    if *disposition == Disposition::Settled {
                        let effect = EffectRef {
                            kind: EffectKind::Sink,
                            effect: *id,
                        };
                        if !self.quiesced_effects.contains(&effect)
                            && !facts.quiescent_effects.contains(&effect)
                        {
                            return Err(Reject::Uncertain);
                        }
                        self.quiesced_effects.insert(effect);
                    }
                }
                self.namespaces = Some([*old, *new]);
                for (id, evidence) in &activation.protections {
                    let root = &self.legacy.roots[id];
                    self.coverage
                        .insert(*id, acquire(&self.legacy, root, *evidence, facts)?);
                }
                self.heads = heads(&self.legacy, activation)?;
                self.activation = Some(activation.clone());
            }
            Event::Observe {
                request,
                destination,
                object,
                outcome,
            } => {
                self.active()?;
                let known = requests(&self.legacy);
                let initial = *known.get(request).ok_or(Reject::Invalid)?;
                let (expected_destination, expected_object) = match request.kind {
                    EffectKind::Sink => {
                        let effect = &self.legacy.effects[&request.effect];
                        (
                            effect.destination,
                            &self.legacy.terminals[&effect.terminal].result,
                        )
                    }
                    EffectKind::History => {
                        let effect = &self.legacy.histories[&request.effect];
                        (effect.destination, &effect.object)
                    }
                };
                if *destination != expected_destination || object != expected_object {
                    return Err(Reject::Stale);
                }
                if *outcome == Delivery::Issued {
                    return Err(Reject::Invalid);
                }
                let finals: BTreeSet<_> = self
                    .observations
                    .iter()
                    .filter(|seen| seen.request == *request)
                    .map(|seen| seen.outcome)
                    .chain([initial])
                    .filter(|status| matches!(status, Delivery::Stored | Delivery::Failed))
                    .collect();
                if matches!(outcome, Delivery::Stored | Delivery::Failed)
                    && finals.iter().any(|prior| prior != outcome)
                {
                    return Err(Reject::Conflict);
                }
                if !self.observations.insert(Observation {
                    request: *request,
                    outcome: *outcome,
                }) {
                    return Ok(Outcome::Ignored);
                }
            }
            Event::Quiesce(request) => {
                self.active()?;
                if !requests(&self.legacy).contains_key(request) {
                    return Err(Reject::Invalid);
                }
                if !facts.quiescent.contains(request) {
                    return Err(Reject::Uncertain);
                }
                if !self.quiesced.insert(*request) {
                    return Ok(Outcome::Ignored);
                }
            }
            Event::QuiesceEffect(effect) => {
                self.active()?;
                if !effects(&self.legacy).contains_key(effect) {
                    return Err(Reject::Invalid);
                }
                if !facts.quiescent_effects.contains(effect) {
                    return Err(Reject::Uncertain);
                }
                if !self.quiesced_effects.insert(*effect) {
                    return Ok(Outcome::Ignored);
                }
            }
            Event::QuiesceDomain(namespace) => {
                self.active()?;
                if *namespace != self.legacy.old_sink {
                    return Err(Reject::Namespace);
                }
                if !facts.quiescent_domains.contains(namespace) {
                    return Err(Reject::Uncertain);
                }
                if self.domain_quiesced {
                    return Ok(Outcome::Ignored);
                }
                self.domain_quiesced = true;
            }
            Event::Protect { root, evidence } => {
                self.active()?;
                let subject = self.legacy.roots.get(root).ok_or(Reject::Invalid)?;
                let protection = acquire(&self.legacy, subject, *evidence, facts)?;
                if self
                    .collection_until
                    .is_some_and(|through| protection.until < through)
                {
                    return Err(Reject::Unprotected);
                }
                self.coverage.insert(*root, protection);
            }
            Event::EnableCollection { namespace, through } => {
                let activation = self.active()?;
                if *namespace != activation.new_namespace || *namespace == self.legacy.old_sink {
                    return Err(Reject::Namespace);
                }
                if *through <= facts.now
                    || self.coverage.keys().ne(self.legacy.roots.keys())
                    || self.coverage.values().any(|proof| proof.until < *through)
                {
                    return Err(Reject::Unprotected);
                }
                if self
                    .collection_until
                    .is_some_and(|current| current >= *through)
                {
                    return Ok(Outcome::Ignored);
                }
                self.collection_until = Some(*through);
            }
        }
        Ok(Outcome::Applied)
    }

    fn active(&self) -> Result<&Activation> {
        self.activation.as_ref().ok_or(Reject::WrongPhase)
    }

    pub(crate) fn unresolved_requests(&self) -> BTreeSet<RequestRef> {
        requests(&self.legacy)
            .into_iter()
            .filter_map(|(request, status)| {
                (matches!(status, Delivery::Issued | Delivery::Unknown)
                    && !self.quiesced.contains(&request)
                    && !self.quiesced_effects.contains(&effect_of(request)))
                .then_some(request)
            })
            .collect()
    }

    pub(crate) fn unresolved_effects(&self) -> BTreeSet<EffectRef> {
        effects(&self.legacy)
            .into_keys()
            .filter(|effect| !self.quiesced_effects.contains(effect))
            .collect()
    }

    pub(crate) const fn unresolved_legacy_domain(&self) -> bool {
        !self.domain_quiesced
    }

    pub(crate) fn pending_history(&self) -> BTreeSet<u64> {
        self.legacy
            .histories
            .values()
            .filter(|effect| {
                !effect.materialized
                    && !self.observations.iter().any(|seen| {
                        seen.request.kind == EffectKind::History
                            && seen.request.effect == effect.id
                            && seen.outcome == Delivery::Stored
                    })
            })
            .map(|effect| effect.id)
            .collect()
    }

    pub(crate) fn may_collect(&self, namespace: Namespace, now: u64) -> bool {
        self.activation
            .as_ref()
            .is_some_and(|activation| activation.new_namespace == namespace)
            && namespace != self.legacy.old_sink
            && self.collection_until.is_some_and(|through| now < through)
    }

    fn validate(&self) -> Result<()> {
        validate_legacy(&self.legacy)?;
        if let Some(activation) = &self.activation {
            activation_shape(&self.legacy, activation)?;
            if self.heads != heads(&self.legacy, activation)?
                || self.coverage.keys().ne(self.legacy.roots.keys())
            {
                return Err(Reject::Incomplete);
            }
            for (id, proof) in &self.coverage {
                protection_shape(&self.legacy.roots[id], proof)?;
            }
            if self.collection_until.is_some_and(|through| {
                through == 0 || self.coverage.values().any(|proof| proof.until < through)
            }) {
                return Err(Reject::Unprotected);
            }
            let [old, new] = self.namespaces.ok_or(Reject::Namespace)?;
            if old.namespace != self.legacy.old_sink
                || new.namespace != activation.new_namespace
                || !old.old_requests_confined
                || old.physical_domain == [0; 32]
                || new.physical_domain == [0; 32]
                || old.physical_domain == new.physical_domain
            {
                return Err(Reject::Namespace);
            }
            for (id, disposition) in &activation.dispositions {
                if *disposition == Disposition::Settled
                    && !self.quiesced_effects.contains(&EffectRef {
                        kind: EffectKind::Sink,
                        effect: *id,
                    })
                {
                    return Err(Reject::Uncertain);
                }
            }
        } else if !self.coverage.is_empty()
            || !self.heads.is_empty()
            || self.observations != self.legacy.late_outcomes
            || self.quiesced != self.legacy.request_quiescence
            || self.namespaces.is_some()
            || self.domain_quiesced != (self.legacy.domain_exposure == Exposure::ProvenQuiescent)
            || self.quiesced_effects
                != effects(&self.legacy)
                    .into_iter()
                    .filter_map(|(id, exposure)| {
                        (exposure == Exposure::ProvenQuiescent).then_some(id)
                    })
                    .collect()
            || self.collection_until.is_some()
        {
            return Err(Reject::WrongPhase);
        }
        if !self.legacy.late_outcomes.is_subset(&self.observations)
            || !self.legacy.request_quiescence.is_subset(&self.quiesced)
        {
            return Err(Reject::Incomplete);
        }
        if self.legacy.domain_exposure == Exposure::ProvenQuiescent && !self.domain_quiesced {
            return Err(Reject::Invalid);
        }
        let known = effects(&self.legacy);
        if self
            .quiesced_effects
            .iter()
            .any(|effect| !known.contains_key(effect))
            || known.iter().any(|(effect, exposure)| {
                *exposure == Exposure::ProvenQuiescent && !self.quiesced_effects.contains(effect)
            })
        {
            return Err(Reject::Invalid);
        }
        validate_observations(&self.legacy, &self.observations, &self.quiesced)?;
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| Reject::Invalid)?;
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(Reject::Bounds);
        }
        Ok(bytes)
    }

    pub(crate) fn restore(bytes: &[u8], original: &Legacy) -> Result<Self> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(Reject::Bounds);
        }
        let restored: Self = serde_json::from_slice(bytes).map_err(|_| Reject::Invalid)?;
        restored.validate()?;
        if restored.legacy != *original
            || serde_json::to_vec(&restored).map_err(|_| Reject::Invalid)? != bytes
        {
            return Err(Reject::Invalid);
        }
        Ok(restored)
    }
}
