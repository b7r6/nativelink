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

//! Test-only root/cohort retention candidate. Lease authority is stored once
//! per immutable root; per-generation protection is an observable projection.
//! Flat closures stand in for independently verified typed manifests. This is
//! not a production collector, provider implementation, or persistent format.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::protocol::{
    CapturedRoot, Cohort, CohortId, Delete, DeleteId, Event, Generation, Key, Limits, Phase, Reply,
    Root, RootId, RootKind, View, Write, WriteId,
};

/// Bounded immutable-root leases and cohort-local collection state. There is no
/// stored per-generation lease index or oracle-owned transition helper here.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Coarse {
    limits: Limits,
    now: u64,
    next_delete: u64,
    #[serde(with = "ordered_map")]
    cohorts: BTreeMap<CohortId, Cohort>,
    #[serde(with = "ordered_map")]
    generations: BTreeMap<Key, Generation>,
    #[serde(with = "ordered_map")]
    roots: BTreeMap<RootId, Root>,
    #[serde(with = "ordered_map")]
    writes: BTreeMap<WriteId, Write>,
    #[serde(with = "ordered_map")]
    deletes: BTreeMap<DeleteId, Delete>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    format: u32,
    state: Coarse,
}

// Canonical vectors support compound physical-generation keys and reject
// duplicate or unordered keys instead of silently overwriting authority data.
mod ordered_map {
    use super::*;

    pub(super) fn serialize<K: Serialize + Ord, V: Serialize, S: Serializer>(
        map: &BTreeMap<K, V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }

    pub(super) fn deserialize<
        'de,
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    >(
        deserializer: D,
    ) -> Result<BTreeMap<K, V>, D::Error> {
        let entries = Vec::<(K, V)>::deserialize(deserializer)?;
        if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(serde::de::Error::custom(
                "unordered or duplicate authority map key",
            ));
        }
        Ok(entries.into_iter().collect())
    }
}

fn valid_limits(limits: Limits) -> bool {
    [
        limits.cohorts,
        limits.generations,
        limits.roots,
        limits.closure,
        limits.mark_batch,
        limits.retire_batch,
        limits.writes,
        limits.delete_intents,
    ]
    .into_iter()
    .all(|limit| limit > 0)
        && limits.lease_ticks > 0
        && limits
            .generations
            .checked_add(limits.writes)
            .is_some_and(|needed| {
                needed <= limits.delete_intents
                    && u64::try_from(needed).is_ok_and(|needed| needed < u64::MAX)
            })
}

fn bounded_sorted<T: Ord>(values: &[T], maximum: usize) -> bool {
    !values.is_empty() && values.len() <= maximum && values.windows(2).all(|pair| pair[0] < pair[1])
}

impl Coarse {
    pub(crate) fn new(limits: Limits) -> Result<Self, String> {
        if !valid_limits(limits) {
            return Err("invalid limits or missing delete-finalization headroom".to_owned());
        }
        Ok(Self {
            limits,
            now: 0,
            next_delete: 1,
            cohorts: BTreeMap::new(),
            generations: BTreeMap::new(),
            roots: BTreeMap::new(),
            writes: BTreeMap::new(),
            deletes: BTreeMap::new(),
        })
    }

    fn protected(&self) -> BTreeSet<Key> {
        self.roots
            .values()
            .filter(|root| root.until > self.now)
            .flat_map(|root| root.closure.iter().copied())
            .collect()
    }

    pub(crate) fn view(&self) -> View {
        View {
            limits: self.limits,
            now: self.now,
            next_delete: self.next_delete,
            cohorts: self.cohorts.clone(),
            generations: self.generations.clone(),
            roots: self.roots.clone(),
            writes: self.writes.clone(),
            deletes: self.deletes.clone(),
            protected: self.protected(),
        }
    }

    fn valid_until(&self, until: u64) -> bool {
        self.now
            .checked_add(self.limits.lease_ticks)
            .is_some_and(|maximum| until > self.now && until <= maximum)
    }

    fn touched_cohorts(&self, closure: &[Key]) -> Option<BTreeSet<CohortId>> {
        closure
            .iter()
            .map(|key| {
                self.generations
                    .get(key)
                    .map(|generation| generation.cohort)
            })
            .collect()
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, String> {
        self.check_invariants()?;
        serde_json::to_vec(&Snapshot {
            format: 1,
            state: self.clone(),
        })
        .map_err(|error| error.to_string())
    }

    pub(crate) fn restore(bytes: &[u8]) -> Result<Self, String> {
        let snapshot: Snapshot =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if snapshot.format != 1 {
            return Err("unsupported experimental retention snapshot".to_owned());
        }
        snapshot.state.check_invariants()?;
        Ok(snapshot.state)
    }

    pub(crate) fn apply(&mut self, event: Event) -> Reply {
        // The finite experimental implementation favors an explicit atomic
        // boundary over in-place rollback. Clone/apply cost is not a production
        // sizing result; every retained relation is bounded by Limits.
        let mut next = self.clone();
        match next.transition(event) {
            Ok(reply) => {
                debug_assert!(
                    next.check_invariants().is_ok(),
                    "coarse retention invariant violated"
                );
                if reply == Reply::Ignored {
                    debug_assert_eq!(&next, self);
                }
                *self = next;
                reply
            }
            Err(()) => Reply::Rejected,
        }
    }

    fn make_delete(&mut self, key: Key) -> Result<(), ()> {
        if self.deletes.len() >= self.limits.delete_intents {
            return Err(());
        }
        let next = self.next_delete.checked_add(1).ok_or(())?;
        let generation = self.generations.get_mut(&key).ok_or(())?;
        let id = DeleteId(self.next_delete);
        self.deletes.insert(
            id,
            Delete {
                key,
                write_revision: generation.write_revision,
                confirmed: false,
            },
        );
        generation.current_delete = Some(id);
        generation.delete_confirmed_revision = None;
        self.next_delete = next;
        Ok(())
    }

    fn transition(&mut self, event: Event) -> Result<Reply, ()> {
        match event {
            Event::AdvanceTime { now } => {
                if now < self.now {
                    return Err(());
                }
                if now == self.now {
                    return Ok(Reply::Ignored);
                }
                self.now = now;
            }
            Event::CreateCohort { cohort } => {
                if self.cohorts.contains_key(&cohort) {
                    return Ok(Reply::Ignored);
                }
                if self.cohorts.len() >= self.limits.cohorts {
                    return Err(());
                }
                self.cohorts.insert(
                    cohort,
                    Cohort {
                        phase: Phase::Allocating,
                        fence: 0,
                        captured: Vec::new(),
                        cursor: 0,
                        marked: BTreeSet::new(),
                    },
                );
            }
            Event::Allocate {
                cohort,
                key,
                logical_object,
            } => {
                if let Some(existing) = self.generations.get(&key) {
                    return if existing.cohort == cohort && existing.logical_object == logical_object
                    {
                        Ok(Reply::Ignored)
                    } else {
                        Err(())
                    };
                }
                if self.generations.len() >= self.limits.generations
                    || self.cohorts.get(&cohort).ok_or(())?.phase != Phase::Allocating
                {
                    return Err(());
                }
                self.generations.insert(
                    key,
                    Generation {
                        logical_object,
                        cohort,
                        ready: false,
                        retired: false,
                        write_revision: 0,
                        current_delete: None,
                        delete_confirmed_revision: None,
                    },
                );
            }
            Event::Seal { cohort } => {
                let record = self.cohorts.get_mut(&cohort).ok_or(())?;
                if record.phase == Phase::Sealed {
                    return Ok(Reply::Ignored);
                }
                if record.phase != Phase::Allocating {
                    return Err(());
                }
                record.fence = record.fence.checked_add(1).ok_or(())?;
                record.phase = Phase::Sealed;
            }
            Event::BeginClose { cohort } => {
                let record = self.cohorts.get_mut(&cohort).ok_or(())?;
                if record.phase == Phase::Closing {
                    return Ok(Reply::Ignored);
                }
                if record.phase != Phase::Sealed {
                    return Err(());
                }
                record.fence = record.fence.checked_add(1).ok_or(())?;
                record.phase = Phase::Closing;
            }
            Event::FinishClose { cohort, fence } => {
                let record = self.cohorts.get(&cohort).ok_or(())?;
                if record.fence != fence {
                    return Err(());
                }
                if record.phase == Phase::Closed {
                    return Ok(Reply::Ignored);
                }
                if record.phase != Phase::Closing {
                    return Err(());
                }
                let mut captured = Vec::new();
                for (id, root) in &self.roots {
                    if root.until <= self.now
                        || !root.closure.iter().any(|key| {
                            self.generations
                                .get(key)
                                .is_some_and(|generation| generation.cohort == cohort)
                        })
                    {
                        continue;
                    }
                    if root.kind == RootKind::Staging {
                        return Err(());
                    }
                    captured.push(CapturedRoot {
                        root: *id,
                        lease: root.lease,
                        closure: root.closure.clone(),
                    });
                }
                let record = self.cohorts.get_mut(&cohort).ok_or(())?;
                record.phase = Phase::Closed;
                record.captured = captured;
                record.cursor = 0;
                record.marked.clear();
            }
            Event::Mark {
                cohort,
                fence,
                at,
                roots,
            } => {
                let record = self.cohorts.get(&cohort).ok_or(())?;
                if record.phase != Phase::Closed
                    || record.fence != fence
                    || roots.is_empty()
                    || roots.len() > self.limits.mark_batch
                {
                    return Err(());
                }
                let end = at.checked_add(roots.len()).ok_or(())?;
                if record.captured.get(at..end) != Some(roots.as_slice()) {
                    return Err(());
                }
                if end <= record.cursor {
                    return Ok(Reply::Ignored);
                }
                if at != record.cursor {
                    return Err(());
                }
                let keys: Vec<_> = roots
                    .iter()
                    .flat_map(|root| root.closure.iter().copied())
                    .filter(|key| {
                        self.generations
                            .get(key)
                            .is_some_and(|generation| generation.cohort == cohort)
                    })
                    .collect();
                let record = self.cohorts.get_mut(&cohort).ok_or(())?;
                record.marked.extend(keys);
                record.cursor = end;
            }
            Event::Retire {
                cohort,
                fence,
                keys,
            } => {
                let record = self.cohorts.get(&cohort).ok_or(())?;
                if record.phase != Phase::Closed
                    || record.fence != fence
                    || record.cursor != record.captured.len()
                    || !bounded_sorted(&keys, self.limits.retire_batch)
                {
                    return Err(());
                }
                for key in &keys {
                    if self.generations.get(key).ok_or(())?.cohort != cohort
                        || record.marked.contains(key)
                    {
                        return Err(());
                    }
                }
                let mut changed = false;
                for key in keys {
                    let generation = self.generations.get_mut(&key).ok_or(())?;
                    if generation.retired {
                        continue;
                    }
                    generation.retired = true;
                    generation.ready = false;
                    self.make_delete(key)?;
                    changed = true;
                }
                if !changed {
                    return Ok(Reply::Ignored);
                }
            }
            Event::Reopen { cohort, fence } => {
                let record = self.cohorts.get_mut(&cohort).ok_or(())?;
                if record.phase != Phase::Closed || record.fence != fence {
                    return Err(());
                }
                record.fence = record.fence.checked_add(1).ok_or(())?;
                record.phase = Phase::Sealed;
                record.captured.clear();
                record.cursor = 0;
                record.marked.clear();
            }
            Event::Acquire {
                root,
                lease,
                kind,
                closure,
                until,
                fences,
            } => {
                if !bounded_sorted(&closure, self.limits.closure) {
                    return Err(());
                }
                if let Some(existing) = self.roots.get(&root) {
                    let immutable = existing.kind == kind && existing.closure == closure;
                    if existing.until > self.now {
                        return if immutable && existing.lease == lease && until <= existing.until {
                            Ok(Reply::Ignored)
                        } else {
                            Err(())
                        };
                    }
                    if !immutable || existing.lease.checked_add(1) != Some(lease) {
                        return Err(());
                    }
                } else if lease != 1 || self.roots.len() >= self.limits.roots {
                    return Err(());
                }
                if !self.valid_until(until) {
                    return Err(());
                }
                let touched = self.touched_cohorts(&closure).ok_or(())?;
                if touched.len() != fences.len() {
                    return Err(());
                }
                for cohort in touched {
                    let record = self.cohorts.get(&cohort).ok_or(())?;
                    if !matches!(record.phase, Phase::Allocating | Phase::Sealed)
                        || fences.get(&cohort) != Some(&record.fence)
                    {
                        return Err(());
                    }
                }
                for key in &closure {
                    let generation = self.generations.get(key).ok_or(())?;
                    if generation.retired || (kind != RootKind::Staging && !generation.ready) {
                        return Err(());
                    }
                }
                self.roots.insert(
                    root,
                    Root {
                        lease,
                        kind,
                        closure,
                        until,
                    },
                );
            }
            Event::Renew { root, lease, until } => {
                let existing = self.roots.get(&root).ok_or(())?;
                if existing.lease != lease || existing.until <= self.now {
                    return Err(());
                }
                if until <= existing.until {
                    return Ok(Reply::Ignored);
                }
                if !self.valid_until(until) {
                    return Err(());
                }
                if existing.kind == RootKind::Staging {
                    for cohort in self.touched_cohorts(&existing.closure).ok_or(())? {
                        if !matches!(
                            self.cohorts.get(&cohort).ok_or(())?.phase,
                            Phase::Allocating | Phase::Sealed
                        ) {
                            return Err(());
                        }
                    }
                }
                self.roots.get_mut(&root).ok_or(())?.until = until;
            }
            Event::Release { root, lease } => {
                let record = self.roots.get_mut(&root).ok_or(())?;
                if record.lease != lease {
                    return Err(());
                }
                if record.until <= self.now {
                    return Ok(Reply::Ignored);
                }
                record.until = self.now;
            }
            Event::Transfer {
                staging,
                lease,
                publication,
                until,
            } => {
                let stage = self.roots.get(&staging).ok_or(())?;
                if stage.kind != RootKind::Staging
                    || stage.lease != lease
                    || stage.until <= self.now
                    || self.roots.contains_key(&publication)
                    || self.roots.len() >= self.limits.roots
                    || !self.valid_until(until)
                {
                    return Err(());
                }
                let closure = stage.closure.clone();
                for key in &closure {
                    let generation = self.generations.get(key).ok_or(())?;
                    if !generation.ready
                        || generation.retired
                        || self.cohorts.get(&generation.cohort).ok_or(())?.phase == Phase::Closed
                    {
                        return Err(());
                    }
                }
                self.roots.get_mut(&staging).ok_or(())?.until = self.now;
                self.roots.insert(
                    publication,
                    Root {
                        lease: 1,
                        kind: RootKind::Publication,
                        closure,
                        until,
                    },
                );
            }
            Event::StartWrite {
                write,
                key,
                staging,
                lease,
            } => {
                if let Some(existing) = self.writes.get(&write) {
                    return if existing.key == key {
                        Ok(Reply::Ignored)
                    } else {
                        Err(())
                    };
                }
                if self.writes.len() >= self.limits.writes
                    || self.generations.get(&key).ok_or(())?.retired
                {
                    return Err(());
                }
                let stage = self.roots.get(&staging).ok_or(())?;
                if stage.kind != RootKind::Staging
                    || stage.lease != lease
                    || stage.until <= self.now
                    || stage.closure.binary_search(&key).is_err()
                {
                    return Err(());
                }
                self.writes.insert(write, Write { key, outcome: None });
            }
            Event::WriteDone { write, success } => {
                let record = self.writes.get(&write).ok_or(())?;
                if let Some(outcome) = record.outcome {
                    return if outcome == success {
                        Ok(Reply::Ignored)
                    } else {
                        Err(())
                    };
                }
                let key = record.key;
                self.writes.get_mut(&write).ok_or(())?.outcome = Some(success);
                if success {
                    let generation = self.generations.get_mut(&key).ok_or(())?;
                    generation.write_revision =
                        generation.write_revision.checked_add(1).ok_or(())?;
                    if generation.retired {
                        self.make_delete(key)?;
                    } else {
                        generation.ready = true;
                    }
                }
            }
            Event::DeleteDone { delete, success } => {
                let record = self.deletes.get_mut(&delete).ok_or(())?;
                if !success || record.confirmed {
                    return Ok(Reply::Ignored);
                }
                record.confirmed = true;
                let generation = self.generations.get_mut(&record.key).ok_or(())?;
                if generation.current_delete == Some(delete)
                    && generation.write_revision == record.write_revision
                {
                    generation.current_delete = None;
                    generation.delete_confirmed_revision = Some(record.write_revision);
                }
            }
        }
        Ok(Reply::Applied)
    }

    fn check_invariants(&self) -> Result<(), String> {
        if !valid_limits(self.limits)
            || self.cohorts.len() > self.limits.cohorts
            || self.generations.len() > self.limits.generations
            || self.roots.len() > self.limits.roots
            || self.writes.len() > self.limits.writes
            || self.deletes.len() > self.limits.delete_intents
        {
            return Err("retained state exceeds bounds or finalization headroom".to_owned());
        }
        if u64::try_from(self.deletes.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            != Some(self.next_delete)
            || self.deletes.keys().enumerate().any(|(index, id)| {
                u64::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_add(1))
                    != Some(id.0)
            })
        {
            return Err("delete identities are not a contiguous nonreused history".to_owned());
        }
        for root in self.roots.values() {
            if root.lease == 0
                || !bounded_sorted(&root.closure, self.limits.closure)
                || root
                    .until
                    .checked_sub(self.now)
                    .is_some_and(|remaining| remaining > self.limits.lease_ticks)
            {
                return Err("invalid immutable root or lease".to_owned());
            }
            for key in &root.closure {
                let generation = self
                    .generations
                    .get(key)
                    .ok_or("root references missing generation")?;
                if root.until > self.now
                    && (generation.retired || (root.kind != RootKind::Staging && !generation.ready))
                {
                    return Err("live root lost required ready generation".to_owned());
                }
            }
        }
        for (id, cohort) in &self.cohorts {
            let valid_phase_fence = match cohort.phase {
                Phase::Allocating => cohort.fence == 0,
                Phase::Sealed => cohort.fence % 2 == 1,
                Phase::Closing | Phase::Closed => cohort.fence >= 2 && cohort.fence % 2 == 0,
            };
            if !valid_phase_fence
                || cohort.captured.len() > self.limits.roots
                || cohort.cursor > cohort.captured.len()
            {
                return Err("invalid cohort fence or mark cursor".to_owned());
            }
            if cohort.phase != Phase::Closed
                && (!cohort.captured.is_empty() || cohort.cursor != 0 || !cohort.marked.is_empty())
            {
                return Err("open cohort contains closed-pass evidence".to_owned());
            }
            if cohort
                .captured
                .windows(2)
                .any(|pair| pair[0].root >= pair[1].root)
            {
                return Err("captured root identities are not canonical".to_owned());
            }
            let mut expected_marks = BTreeSet::new();
            for (index, captured) in cohort.captured.iter().enumerate() {
                let root = self
                    .roots
                    .get(&captured.root)
                    .ok_or("captured root identity missing")?;
                if root.kind == RootKind::Staging
                    || root.lease != captured.lease
                    || root.closure != captured.closure
                {
                    return Err("captured immutable manifest changed".to_owned());
                }
                let local: Vec<_> = captured
                    .closure
                    .iter()
                    .copied()
                    .filter(|key| {
                        self.generations
                            .get(key)
                            .is_some_and(|generation| generation.cohort == *id)
                    })
                    .collect();
                if local.is_empty() {
                    return Err("captured root does not touch cohort".to_owned());
                }
                if index < cohort.cursor {
                    expected_marks.extend(local);
                }
            }
            if expected_marks != cohort.marked {
                return Err("mark set does not match completed immutable prefix".to_owned());
            }
            if cohort.phase == Phase::Closed {
                for (root_id, root) in &self.roots {
                    if root.until > self.now
                        && root.closure.iter().any(|key| {
                            self.generations
                                .get(key)
                                .is_some_and(|generation| generation.cohort == *id)
                        })
                        && !cohort.captured.iter().any(|captured| {
                            captured.root == *root_id && captured.lease == root.lease
                        })
                    {
                        return Err("closed cohort acquired uncaptured live protection".to_owned());
                    }
                }
            }
        }
        for write in self.writes.values() {
            if !self.generations.contains_key(&write.key) {
                return Err("provider write references unknown generation".to_owned());
            }
        }
        for deletion in self.deletes.values() {
            let generation = self
                .generations
                .get(&deletion.key)
                .ok_or("delete references unknown generation")?;
            if !generation.retired || deletion.write_revision > generation.write_revision {
                return Err("delete lacks a valid retirement or write revision".to_owned());
            }
        }
        let protected = self.protected();
        for (key, generation) in &self.generations {
            if !self.cohorts.contains_key(&generation.cohort) {
                return Err("generation cohort missing".to_owned());
            }
            let successful_writes = self
                .writes
                .values()
                .filter(|write| write.key == *key && write.outcome == Some(true))
                .count();
            if u64::try_from(successful_writes).ok() != Some(generation.write_revision)
                || generation.ready != (generation.write_revision != 0 && !generation.retired)
                || (generation.retired && protected.contains(key))
            {
                return Err("generation readiness/revision/protection is inconsistent".to_owned());
            }
            let history: Vec<_> = self
                .deletes
                .iter()
                .filter(|(_, deletion)| deletion.key == *key)
                .collect();
            if !generation.retired {
                if !history.is_empty()
                    || generation.current_delete.is_some()
                    || generation.delete_confirmed_revision.is_some()
                {
                    return Err("live generation has retirement effects".to_owned());
                }
                continue;
            }
            if history.windows(2).any(|pair| {
                pair[0].1.write_revision.checked_add(1) != Some(pair[1].1.write_revision)
            }) {
                return Err(
                    "retired write observations did not create consecutive orphan rounds"
                        .to_owned(),
                );
            }
            let (latest_id, latest) = history
                .last()
                .ok_or("retired generation has no delete history")?;
            if latest.write_revision != generation.write_revision {
                return Err("current orphan revision lacks a delete".to_owned());
            }
            if latest.confirmed {
                if generation.current_delete.is_some()
                    || generation.delete_confirmed_revision != Some(generation.write_revision)
                {
                    return Err("confirmed current deletion has inconsistent settlement".to_owned());
                }
            } else if generation.current_delete != Some(**latest_id)
                || generation.delete_confirmed_revision.is_some()
            {
                return Err("unconfirmed orphan lost its exact current delete intent".to_owned());
            }
        }
        let possible_deletes = self
            .generations
            .values()
            .filter(|generation| generation.retired)
            .count()
            .checked_add(
                self.writes
                    .values()
                    .filter(|write| write.outcome == Some(true))
                    .count(),
            )
            .ok_or("delete headroom overflow")?;
        if self.deletes.len() > possible_deletes {
            return Err("delete history exceeds retirement/write obligations".to_owned());
        }
        Ok(())
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;

    #[test]
    fn restored_candidate_rejects_forged_relations_and_duplicate_identity() {
        let mut model = Coarse::new(Limits::default()).unwrap();
        let key = Key {
            incarnation: 1,
            generation: 1,
        };
        for event in [
            Event::CreateCohort {
                cohort: CohortId(1),
            },
            Event::Allocate {
                cohort: CohortId(1),
                key,
                logical_object: 1,
            },
            Event::Acquire {
                root: RootId(1),
                lease: 1,
                kind: RootKind::Staging,
                closure: vec![key],
                until: 10,
                fences: BTreeMap::from([(CohortId(1), 0)]),
            },
            Event::StartWrite {
                write: WriteId(1),
                key,
                staging: RootId(1),
                lease: 1,
            },
            Event::WriteDone {
                write: WriteId(1),
                success: true,
            },
        ] {
            assert_eq!(model.apply(event), Reply::Applied);
        }
        let bytes = model.snapshot().unwrap();
        assert_eq!(Coarse::restore(&bytes).unwrap().snapshot().unwrap(), bytes);
        let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let mut mutations = Vec::new();
        let mut bad = original.clone();
        bad["format"] = serde_json::json!(99);
        mutations.push(bad);
        let mut bad = original.clone();
        bad["state"]["cohorts"][0][1]["unknown_authority"] = serde_json::json!(true);
        mutations.push(bad);
        let mut bad = original.clone();
        let duplicate = bad["state"]["cohorts"][0].clone();
        bad["state"]["cohorts"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        mutations.push(bad);
        let mut bad = original.clone();
        bad["state"]["generations"][0][1]["write_revision"] = serde_json::json!(2);
        mutations.push(bad);
        let mut bad = original.clone();
        bad["state"]["roots"][0][1]["closure"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::to_value(key).unwrap());
        mutations.push(bad);
        let mut bad = original;
        bad["state"]["limits"]["delete_intents"] = serde_json::json!(1);
        mutations.push(bad);
        for bad in mutations {
            assert!(Coarse::restore(&serde_json::to_vec(&bad).unwrap()).is_err());
        }
    }
}
