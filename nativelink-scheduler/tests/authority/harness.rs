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

//! Deterministic proposal/commit/apply simulator. This does not simulate Raft.
//! The durable log and snapshots are explicit test artifacts; process loss may
//! discard proposals but cannot turn them into committed authority.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_scheduler::authority::{
    ClusterEpoch, Effect, EffectId, Event, Limits, ObservableState, RecordedEvent, Reply, State,
};
use serde::{Deserialize, Serialize};

use super::reference::Reference;

pub(crate) struct Harness {
    pub(crate) core: State,
    pub(crate) oracle: Reference,
    epoch: ClusterEpoch,
    limits: Limits,
    proposals: BTreeMap<u64, RecordedEvent>,
    committed: Vec<RecordedEvent>,
    applied: usize,
    checkpoint: Option<(usize, Vec<u8>, Vec<u8>)>,
    pub(crate) delivered: Vec<Effect>,
}

impl Harness {
    pub(crate) const fn new(epoch: ClusterEpoch, limits: Limits) -> Self {
        Self {
            core: State::new(epoch, limits),
            oracle: Reference::new(epoch, limits),
            epoch,
            limits,
            proposals: BTreeMap::new(),
            committed: Vec::new(),
            applied: 0,
            checkpoint: None,
            delivered: Vec::new(),
        }
    }

    pub(crate) fn propose(&mut self, id: u64, event: RecordedEvent) -> Result<(), String> {
        if self.proposals.contains_key(&id) {
            return Err("proposal identity was reused".into());
        }
        self.proposals.insert(id, event);
        Ok(())
    }

    pub(crate) fn commit(&mut self, id: u64) -> Result<(), String> {
        let event = self.proposals.remove(&id).ok_or("unknown proposal")?;
        self.committed.push(event);
        Ok(())
    }

    pub(crate) fn apply_next(&mut self) -> Result<Reply, String> {
        let event = self
            .committed
            .get(self.applied)
            .ok_or("no committed event ready to apply")?
            .clone();
        let actual = self.core.apply(event.clone());
        let expected = self.oracle.apply(event);
        self.applied += 1;
        if actual != expected {
            return Err(format!(
                "REFINEMENT_REPLY at {}: actual {actual:?}, expected {expected:?}",
                self.applied
            ));
        }
        self.compare()?;
        Ok(actual)
    }

    pub(crate) fn event(&mut self, event: Event) -> Result<Reply, String> {
        let proposal = u64::try_from(self.committed.len()).map_err(|e| e.to_string())?;
        self.propose(
            proposal,
            RecordedEvent {
                epoch: self.epoch,
                event,
            },
        )?;
        self.commit(proposal)?;
        self.apply_next()
    }

    pub(crate) fn compare(&self) -> Result<(), String> {
        self.core
            .check_invariants()
            .map_err(|e| format!("CORE_INVARIANT: {e}"))?;
        let actual = self.core.view();
        let expected = self.oracle.view();
        if actual != expected {
            return Err(format!(
                "REFINEMENT_STATE:\nactual {actual:#?}\nexpected {expected:#?}"
            ));
        }
        Ok(())
    }

    pub(crate) fn deliver(&mut self, effect: EffectId) -> Result<Effect, String> {
        let intent = self
            .core
            .view()
            .effects
            .into_iter()
            .find(|e| e.id == effect)
            .ok_or("EFFECT_1: no committed applied intent")?;
        self.delivered.push(intent.clone());
        Ok(intent)
    }

    pub(crate) fn checkpoint(&mut self) -> Result<(), String> {
        self.checkpoint = Some((self.applied, self.core.snapshot()?, self.oracle.snapshot()?));
        Ok(())
    }

    pub(crate) fn crash_restore(&mut self) -> Result<(), String> {
        self.proposals.clear();
        if let Some((applied, core, oracle)) = &self.checkpoint {
            self.core = State::restore(core)?;
            self.oracle = Reference::restore(oracle)?;
            self.applied = *applied;
        } else {
            self.core = State::new(self.epoch, self.limits);
            self.oracle = Reference::new(self.epoch, self.limits);
            self.applied = 0;
        }
        self.compare()?;
        while self.applied < self.committed.len() {
            self.apply_next()?;
        }
        Ok(())
    }

    pub(crate) fn view(&self) -> ObservableState {
        self.core.view()
    }
}

/// Parents are explicit causal prerequisites. In generated histories every
/// allocation also depends on earlier allocations, so shrinking cannot renumber
/// an attempt/effect that a surviving event names. This conservative dependency
/// rule sacrifices some shrinking power to preserve the actual race.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Input {
    pub(crate) id: u64,
    pub(crate) parents: Vec<u64>,
    pub(crate) event: RecordedEvent,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub(crate) enum Schedule {
    Propose(u64),
    Commit(u64),
    Apply,
    Deliver(EffectId),
    Checkpoint,
    Crash,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Scenario {
    pub(crate) version: u32,
    pub(crate) name: String,
    pub(crate) seed: u64,
    pub(crate) epoch: ClusterEpoch,
    pub(crate) limits: Limits,
    pub(crate) inputs: Vec<Input>,
    pub(crate) schedule: Vec<Schedule>,
}

impl Scenario {
    /// Validate transport prerequisites using only the independent oracle. A
    /// malformed candidate must never count as preserving a reducer failure.
    /// Return the input consumed by each explicit Apply; Crash applies its whole
    /// durable suffix and therefore has no single input owner.
    fn schedule_inputs(&self) -> Result<Vec<Option<u64>>, String> {
        let mut reference = Reference::new(self.epoch, self.limits);
        let mut proposed = BTreeSet::new();
        let mut committed = Vec::new();
        let mut applied = 0;
        let mut checkpoint = None;
        let inputs: BTreeMap<_, _> = self.inputs.iter().map(|input| (input.id, input)).collect();
        let mut known = BTreeSet::new();
        for input in &self.inputs {
            if input.parents.iter().any(|id| !known.contains(id)) || !known.insert(input.id) {
                return Err("invalid causal input graph".into());
            }
        }
        let mut owners = Vec::new();
        for step in &self.schedule {
            let mut owner = None;
            match *step {
                Schedule::Propose(id) => {
                    if !inputs.contains_key(&id) || !proposed.insert(id) {
                        return Err("invalid proposal".into());
                    }
                }
                Schedule::Commit(id) => {
                    if !proposed.remove(&id) {
                        return Err("commit without proposal".into());
                    }
                    if inputs[&id]
                        .parents
                        .iter()
                        .any(|parent| !committed.contains(parent))
                    {
                        return Err("committed input lost a causal prerequisite".into());
                    }
                    committed.push(id);
                }
                Schedule::Apply => {
                    let id = *committed.get(applied).ok_or("apply without commit")?;
                    reference.apply(inputs[&id].event.clone());
                    applied += 1;
                    owner = Some(id);
                }
                Schedule::Deliver(effect) => {
                    if !reference.view().effects.iter().any(|row| row.id == effect) {
                        return Err("delivery without applied intent".into());
                    }
                }
                Schedule::Checkpoint => checkpoint = Some((applied, reference.snapshot()?)),
                Schedule::Crash => {
                    proposed.clear();
                    if let Some((position, bytes)) = &checkpoint {
                        reference = Reference::restore(bytes)?;
                        applied = *position;
                    } else {
                        reference = Reference::new(self.epoch, self.limits);
                        applied = 0;
                    }
                    while applied < committed.len() {
                        reference.apply(inputs[&committed[applied]].event.clone());
                        applied += 1;
                    }
                }
            }
            owners.push(owner);
        }
        Ok(owners)
    }

    pub(crate) fn replay(&self) -> Result<Harness, String> {
        if self.version != 1 {
            return Err("unsupported scenario version".into());
        }
        let mut known = BTreeSet::new();
        for input in &self.inputs {
            if input.parents.iter().any(|id| !known.contains(id)) || !known.insert(input.id) {
                return Err("invalid causal input graph".into());
            }
        }
        let inputs: BTreeMap<_, _> = self.inputs.iter().map(|input| (input.id, input)).collect();
        let mut harness = Harness::new(self.epoch, self.limits);
        for (index, step) in self.schedule.iter().enumerate() {
            let result = match *step {
                Schedule::Propose(id) => inputs
                    .get(&id)
                    .ok_or_else(|| "unknown input".into())
                    .and_then(|input| harness.propose(id, input.event.clone())),
                Schedule::Commit(id) => harness.commit(id),
                Schedule::Apply => harness.apply_next().map(|_| ()),
                Schedule::Deliver(effect) => harness.deliver(effect).map(|_| ()),
                Schedule::Checkpoint => harness.checkpoint(),
                Schedule::Crash => harness.crash_restore(),
            };
            result.map_err(|error| format!("schedule step {index}: {error}"))?;
        }
        Ok(harness)
    }
}

/// Greedily remove inputs with their causal descendants and matching transport
/// steps, then individual fault/delivery steps. Every candidate must preserve
/// independent transport validity and the caller's original failure predicate.
/// This is deletion minimization, not a claim of a globally shortest trace.
pub(crate) fn shrink_scenario(
    original: &Scenario,
    mut still_fails: impl FnMut(&Scenario) -> bool,
) -> Scenario {
    let mut retained = original.clone();
    if retained.schedule_inputs().is_err() || !still_fails(&retained) {
        return retained;
    }
    loop {
        let before = (retained.inputs.len(), retained.schedule.len());
        let mut index = 0;
        while index < retained.inputs.len() {
            let mut removed = BTreeSet::from([retained.inputs[index].id]);
            let mut candidate = retained.clone();
            candidate.inputs.retain(|input| {
                if removed.contains(&input.id)
                    || input.parents.iter().any(|id| removed.contains(id))
                {
                    removed.insert(input.id);
                    false
                } else {
                    true
                }
            });
            let owners = retained
                .schedule_inputs()
                .expect("retained schedule is valid");
            candidate.schedule = retained
                .schedule
                .iter()
                .zip(owners)
                .filter_map(|(step, owner)| {
                    let belongs = match *step {
                        Schedule::Propose(id) | Schedule::Commit(id) => removed.contains(&id),
                        Schedule::Apply => owner.is_some_and(|id| removed.contains(&id)),
                        _ => false,
                    };
                    (!belongs).then_some(*step)
                })
                .collect();
            if candidate.schedule_inputs().is_ok() && still_fails(&candidate) {
                retained = candidate;
                index = 0;
            } else {
                index += 1;
            }
        }
        let mut index = 0;
        while index < retained.schedule.len() {
            if matches!(
                retained.schedule[index],
                Schedule::Propose(_) | Schedule::Commit(_) | Schedule::Apply
            ) {
                index += 1;
                continue;
            }
            let mut candidate = retained.clone();
            candidate.schedule.remove(index);
            if candidate.schedule_inputs().is_ok() && still_fails(&candidate) {
                retained = candidate;
                index = 0;
            } else {
                index += 1;
            }
        }
        if before == (retained.inputs.len(), retained.schedule.len()) {
            return retained;
        }
    }
}

/// Shrink an already failing *committed input history*. An adversarial transport
/// schedule is kept separately and must be minimized with its commit/apply
/// prerequisites; this function makes no claim about shrinking that schedule.
pub(crate) fn shrink_inputs(
    original: &[Input],
    mut still_fails: impl FnMut(&[Input]) -> bool,
) -> Vec<Input> {
    let mut retained = original.to_vec();
    let mut index = 0;
    while index < retained.len() {
        let mut removed = BTreeSet::from([retained[index].id]);
        let candidate: Vec<_> = retained
            .iter()
            .filter_map(|input| {
                if removed.contains(&input.id)
                    || input.parents.iter().any(|id| removed.contains(id))
                {
                    removed.insert(input.id);
                    None
                } else {
                    Some(input.clone())
                }
            })
            .collect();
        if still_fails(&candidate) {
            retained = candidate;
            index = 0;
        } else {
            index += 1;
        }
    }
    retained
}
