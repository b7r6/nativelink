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

//! Independently authored relational oracle. Current protection is a row for
//! each (physical generation, root, acquisition), not a coarse root summary.
//! No candidate implementation or candidate helper is imported here.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::protocol::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Coverage {
    key: Key,
    root: RootId,
    lease: u64,
    until: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Pass {
    id: CohortId,
    phase: Phase,
    fence: u64,
    captured: Vec<CapturedRoot>,
    cursor: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Reference {
    limits: Limits,
    now: u64,
    next_delete: u64,
    cohorts: Vec<Pass>,
    generations: Vec<(Key, Generation)>,
    roots: Vec<(RootId, Root)>,
    coverage: Vec<Coverage>,
    marks: Vec<(CohortId, u64, Key)>,
    writes: Vec<(WriteId, Write)>,
    deletes: Vec<(DeleteId, Delete)>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    version: u64,
    state: Reference,
}

impl Reference {
    pub(crate) fn new(limits: Limits) -> Result<Self, String> {
        if [
            limits.cohorts,
            limits.generations,
            limits.roots,
            limits.closure,
            limits.mark_batch,
            limits.retire_batch,
            limits.writes,
            limits.delete_intents,
        ]
        .contains(&0)
            || limits.lease_ticks == 0
            || limits
                .generations
                .checked_add(limits.writes)
                .is_none_or(|required| required > limits.delete_intents)
            || limits
                .generations
                .checked_add(limits.writes)
                .and_then(|required| u64::try_from(required).ok())
                .is_none_or(|required| required == u64::MAX)
        {
            return Err("invalid finite retention bounds".into());
        }
        Ok(Self {
            limits,
            now: 0,
            next_delete: 1,
            cohorts: Vec::new(),
            generations: Vec::new(),
            roots: Vec::new(),
            coverage: Vec::new(),
            marks: Vec::new(),
            writes: Vec::new(),
            deletes: Vec::new(),
        })
    }

    fn generation(&self, key: Key) -> Option<&Generation> {
        self.generations
            .iter()
            .find(|(id, _)| *id == key)
            .map(|(_, row)| row)
    }

    fn root(&self, root: RootId) -> Option<&Root> {
        self.roots
            .iter()
            .find(|(id, _)| *id == root)
            .map(|(_, row)| row)
    }

    fn cohort(&self, cohort: CohortId) -> Option<&Pass> {
        self.cohorts.iter().find(|row| row.id == cohort)
    }

    fn deadline(&self, until: u64) -> Option<()> {
        (until > self.now && until <= self.now.checked_add(self.limits.lease_ticks)?).then_some(())
    }

    fn canonical(keys: &[Key], bound: usize) -> Option<()> {
        (!keys.is_empty() && keys.len() <= bound && keys.windows(2).all(|pair| pair[0] < pair[1]))
            .then_some(())
    }

    fn add_coverage(&mut self, root: RootId, row: &Root) {
        self.coverage.extend(row.closure.iter().map(|key| Coverage {
            key: *key,
            root,
            lease: row.lease,
            until: row.until,
        }));
    }

    fn delete_for(&mut self, key: Key) -> Option<()> {
        if self.deletes.len() >= self.limits.delete_intents {
            return None;
        }
        let id = DeleteId(self.next_delete);
        self.next_delete = self.next_delete.checked_add(1)?;
        let (_, generation) = self.generations.iter_mut().find(|(id, _)| *id == key)?;
        generation.current_delete = Some(id);
        generation.delete_confirmed_revision = None;
        self.deletes.push((
            id,
            Delete {
                key,
                write_revision: generation.write_revision,
                confirmed: false,
            },
        ));
        Some(())
    }

    /// Transactional by construction: a failed relation constraint publishes
    /// none of the tentative rows, including overflow or capacity failures.
    pub(crate) fn apply(&mut self, event: Event) -> Reply {
        let mut tentative = self.clone();
        match tentative.transition(event) {
            Some(Reply::Applied) => {
                *self = tentative;
                Reply::Applied
            }
            Some(Reply::Ignored) => Reply::Ignored,
            Some(Reply::Rejected) | None => Reply::Rejected,
        }
    }

    #[allow(clippy::too_many_lines)]
    fn transition(&mut self, event: Event) -> Option<Reply> {
        match event {
            Event::AdvanceTime { now } => {
                if now < self.now {
                    return None;
                }
                if now == self.now {
                    return Some(Reply::Ignored);
                }
                self.now = now;
                self.coverage.retain(|row| row.until > now);
            }
            Event::CreateCohort { cohort } => {
                if self.cohort(cohort).is_some() {
                    return Some(Reply::Ignored);
                }
                if self.cohorts.len() >= self.limits.cohorts {
                    return None;
                }
                self.cohorts.push(Pass {
                    id: cohort,
                    phase: Phase::Allocating,
                    fence: 0,
                    captured: Vec::new(),
                    cursor: 0,
                });
            }
            Event::Allocate {
                cohort,
                key,
                logical_object,
            } => {
                if let Some(row) = self.generation(key) {
                    return (row.cohort == cohort && row.logical_object == logical_object)
                        .then_some(Reply::Ignored);
                }
                if self.cohort(cohort)?.phase != Phase::Allocating
                    || self.generations.len() >= self.limits.generations
                {
                    return None;
                }
                self.generations.push((
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
                ));
            }
            Event::Seal { cohort } => {
                let row = self.cohorts.iter_mut().find(|row| row.id == cohort)?;
                if row.phase == Phase::Sealed {
                    return Some(Reply::Ignored);
                }
                if row.phase != Phase::Allocating {
                    return None;
                }
                row.fence = row.fence.checked_add(1)?;
                row.phase = Phase::Sealed;
            }
            Event::BeginClose { cohort } => {
                let row = self.cohorts.iter_mut().find(|row| row.id == cohort)?;
                if row.phase == Phase::Closing {
                    return Some(Reply::Ignored);
                }
                if row.phase != Phase::Sealed {
                    return None;
                }
                row.fence = row.fence.checked_add(1)?;
                row.phase = Phase::Closing;
            }
            Event::FinishClose { cohort, fence } => {
                let row = self.cohort(cohort)?;
                if row.fence != fence {
                    return None;
                }
                if row.phase == Phase::Closed {
                    return Some(Reply::Ignored);
                }
                if row.phase != Phase::Closing {
                    return None;
                }
                let mut captured = Vec::new();
                for (id, root) in &self.roots {
                    if root.until > self.now
                        && root.closure.iter().any(|key| {
                            self.generation(*key)
                                .is_some_and(|generation| generation.cohort == cohort)
                        })
                    {
                        if root.kind == RootKind::Staging {
                            return None;
                        }
                        captured.push(CapturedRoot {
                            root: *id,
                            lease: root.lease,
                            closure: root.closure.clone(),
                        });
                    }
                }
                captured.sort_by_key(|root| root.root);
                let row = self.cohorts.iter_mut().find(|row| row.id == cohort)?;
                row.phase = Phase::Closed;
                row.captured = captured;
                row.cursor = 0;
            }
            Event::Mark {
                cohort,
                fence,
                at,
                roots,
            } => {
                let row = self.cohort(cohort)?;
                let end = at.checked_add(roots.len())?;
                if row.phase != Phase::Closed
                    || row.fence != fence
                    || roots.is_empty()
                    || roots.len() > self.limits.mark_batch
                    || row.captured.get(at..end)? != roots
                {
                    return None;
                }
                if end <= row.cursor {
                    return Some(Reply::Ignored);
                }
                if at != row.cursor {
                    return None;
                }
                for root in roots {
                    for key in root.closure {
                        if self.generation(key)?.cohort == cohort
                            && !self.marks.contains(&(cohort, fence, key))
                        {
                            self.marks.push((cohort, fence, key));
                        }
                    }
                }
                self.cohorts.iter_mut().find(|row| row.id == cohort)?.cursor = end;
            }
            Event::Retire {
                cohort,
                fence,
                keys,
            } => {
                Self::canonical(&keys, self.limits.retire_batch)?;
                let row = self.cohort(cohort)?;
                if row.phase != Phase::Closed
                    || row.fence != fence
                    || row.cursor != row.captured.len()
                {
                    return None;
                }
                for key in &keys {
                    if self.generation(*key)?.cohort != cohort
                        || self.marks.contains(&(cohort, fence, *key))
                    {
                        return None;
                    }
                    // The oracle's actual safety relation is independent of
                    // captured root traversal and the candidate's summary.
                    if self.coverage.iter().any(|row| row.key == *key) {
                        return None;
                    }
                }
                let mut changed = false;
                for key in keys {
                    let (_, generation) = self.generations.iter_mut().find(|(id, _)| *id == key)?;
                    if !generation.retired {
                        generation.retired = true;
                        generation.ready = false;
                        self.delete_for(key)?;
                        changed = true;
                    }
                }
                if !changed {
                    return Some(Reply::Ignored);
                }
            }
            Event::Reopen { cohort, fence } => {
                let row = self.cohorts.iter_mut().find(|row| row.id == cohort)?;
                if row.phase != Phase::Closed || row.fence != fence {
                    return None;
                }
                row.phase = Phase::Sealed;
                row.fence = row.fence.checked_add(1)?;
                row.captured.clear();
                row.cursor = 0;
                self.marks.retain(|(id, _, _)| *id != cohort);
            }
            Event::Acquire {
                root,
                lease,
                kind,
                closure,
                until,
                fences,
            } => {
                Self::canonical(&closure, self.limits.closure)?;
                if let Some(old) = self.root(root) {
                    if old.until > self.now {
                        return (old.lease == lease
                            && old.kind == kind
                            && old.closure == closure
                            && until <= old.until)
                            .then_some(Reply::Ignored);
                    }
                    if old.kind != kind
                        || old.closure != closure
                        || old.lease.checked_add(1)? != lease
                    {
                        return None;
                    }
                } else if lease != 1 || self.roots.len() >= self.limits.roots {
                    return None;
                }
                self.deadline(until)?;
                let mut required = BTreeMap::new();
                for key in &closure {
                    let generation = self.generation(*key)?;
                    if generation.retired || (kind != RootKind::Staging && !generation.ready) {
                        return None;
                    }
                    let pass = self.cohort(generation.cohort)?;
                    if !matches!(pass.phase, Phase::Allocating | Phase::Sealed) {
                        return None;
                    }
                    required.insert(generation.cohort, pass.fence);
                }
                if fences != required {
                    return None;
                }
                let row = Root {
                    lease,
                    kind,
                    closure,
                    until,
                };
                self.coverage.retain(|entry| entry.root != root);
                self.add_coverage(root, &row);
                if let Some((_, old)) = self.roots.iter_mut().find(|(id, _)| *id == root) {
                    *old = row;
                } else {
                    self.roots.push((root, row));
                }
            }
            Event::Renew { root, lease, until } => {
                let row = self.root(root)?;
                if row.lease != lease || row.until <= self.now {
                    return None;
                }
                if until <= row.until {
                    return Some(Reply::Ignored);
                }
                self.deadline(until)?;
                if row.kind == RootKind::Staging {
                    for key in &row.closure {
                        if !matches!(
                            self.cohort(self.generation(*key)?.cohort)?.phase,
                            Phase::Allocating | Phase::Sealed
                        ) {
                            return None;
                        }
                    }
                }
                self.roots.iter_mut().find(|(id, _)| *id == root)?.1.until = until;
                for coverage in &mut self.coverage {
                    if coverage.root == root && coverage.lease == lease {
                        coverage.until = until;
                    }
                }
            }
            Event::Release { root, lease } => {
                let row = self.roots.iter_mut().find(|(id, _)| *id == root)?;
                if row.1.lease != lease {
                    return None;
                }
                if row.1.until <= self.now {
                    return Some(Reply::Ignored);
                }
                row.1.until = self.now;
                self.coverage.retain(|entry| entry.root != root);
            }
            Event::Transfer {
                staging,
                lease,
                publication,
                until,
            } => {
                let row = self.root(staging)?;
                if row.kind != RootKind::Staging
                    || row.lease != lease
                    || row.until <= self.now
                    || self.root(publication).is_some()
                    || self.roots.len() >= self.limits.roots
                {
                    return None;
                }
                self.deadline(until)?;
                for key in &row.closure {
                    let generation = self.generation(*key)?;
                    if generation.retired
                        || !generation.ready
                        || self.cohort(generation.cohort)?.phase == Phase::Closed
                    {
                        return None;
                    }
                }
                let publication_row = Root {
                    lease: 1,
                    kind: RootKind::Publication,
                    closure: row.closure.clone(),
                    until,
                };
                self.coverage.retain(|entry| entry.root != staging);
                self.roots
                    .iter_mut()
                    .find(|(id, _)| *id == staging)?
                    .1
                    .until = self.now;
                self.add_coverage(publication, &publication_row);
                self.roots.push((publication, publication_row));
            }
            Event::StartWrite {
                write,
                key,
                staging,
                lease,
            } => {
                if let Some((_, old)) = self.writes.iter().find(|(id, _)| *id == write) {
                    return (old.key == key).then_some(Reply::Ignored);
                }
                if self.writes.len() >= self.limits.writes || self.generation(key)?.retired {
                    return None;
                }
                let root = self.root(staging)?;
                if root.kind != RootKind::Staging
                    || root.lease != lease
                    || root.until <= self.now
                    || !root.closure.contains(&key)
                {
                    return None;
                }
                self.writes.push((write, Write { key, outcome: None }));
            }
            Event::WriteDone { write, success } => {
                let (_, row) = self.writes.iter_mut().find(|(id, _)| *id == write)?;
                if let Some(old) = row.outcome {
                    return (old == success).then_some(Reply::Ignored);
                }
                row.outcome = Some(success);
                let key = row.key;
                if success {
                    let (_, generation) = self.generations.iter_mut().find(|(id, _)| *id == key)?;
                    generation.write_revision = generation.write_revision.checked_add(1)?;
                    if generation.retired {
                        self.delete_for(key)?;
                    } else {
                        generation.ready = true;
                    }
                }
            }
            Event::DeleteDone { delete, success } => {
                let (_, row) = self.deletes.iter_mut().find(|(id, _)| *id == delete)?;
                if !success || row.confirmed {
                    return Some(Reply::Ignored);
                }
                row.confirmed = true;
                let (_, generation) = self.generations.iter_mut().find(|(id, _)| *id == row.key)?;
                if generation.current_delete == Some(delete)
                    && generation.write_revision == row.write_revision
                {
                    generation.current_delete = None;
                    generation.delete_confirmed_revision = Some(row.write_revision);
                }
            }
        }
        Some(Reply::Applied)
    }

    pub(crate) fn view(&self) -> View {
        View {
            limits: self.limits,
            now: self.now,
            next_delete: self.next_delete,
            cohorts: self
                .cohorts
                .iter()
                .map(|row| {
                    (
                        row.id,
                        Cohort {
                            phase: row.phase,
                            fence: row.fence,
                            captured: row.captured.clone(),
                            cursor: row.cursor,
                            marked: self
                                .marks
                                .iter()
                                .filter(|(cohort, fence, _)| {
                                    *cohort == row.id && *fence == row.fence
                                })
                                .map(|(_, _, key)| *key)
                                .collect(),
                        },
                    )
                })
                .collect(),
            generations: self.generations.iter().cloned().collect(),
            roots: self.roots.iter().cloned().collect(),
            writes: self.writes.iter().cloned().collect(),
            deletes: self.deletes.iter().cloned().collect(),
            protected: self.coverage.iter().map(|row| row.key).collect(),
        }
    }

    pub(crate) fn snapshot(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(&Checkpoint {
            version: 1,
            state: self.clone(),
        })
        .map_err(|error| error.to_string())
    }

    pub(crate) fn restore(bytes: &[u8]) -> Result<Self, String> {
        let checkpoint: Checkpoint =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        if checkpoint.version != 1 {
            return Err("unknown oracle checkpoint version".into());
        }
        checkpoint.state.check()?;
        Ok(checkpoint.state)
    }

    pub(crate) fn check(&self) -> Result<(), String> {
        let _fresh = Self::new(self.limits)?;
        let view = self.view();
        if view.cohorts.len() != self.cohorts.len()
            || view.generations.len() != self.generations.len()
            || view.roots.len() != self.roots.len()
            || view.writes.len() != self.writes.len()
            || view.deletes.len() != self.deletes.len()
        {
            return Err("duplicate relation identity".into());
        }
        if self.cohorts.len() > self.limits.cohorts
            || self.generations.len() > self.limits.generations
            || self.roots.len() > self.limits.roots
            || self.writes.len() > self.limits.writes
            || self.deletes.len() > self.limits.delete_intents
        {
            return Err("finite ledger cap exceeded".into());
        }
        let expected: BTreeSet<_> = self
            .roots
            .iter()
            .filter(|(_, root)| root.until > self.now)
            .flat_map(|(id, root)| {
                root.closure
                    .iter()
                    .map(move |key| (*key, *id, root.lease, root.until))
            })
            .collect();
        let actual: BTreeSet<_> = self
            .coverage
            .iter()
            .map(|row| (row.key, row.root, row.lease, row.until))
            .collect();
        if expected != actual || actual.len() != self.coverage.len() {
            return Err(
                "per-generation protection relation disagrees with live acquisitions".into(),
            );
        }
        for (_, root) in &self.roots {
            if root.lease == 0
                || Self::canonical(&root.closure, self.limits.closure).is_none()
                || root
                    .closure
                    .iter()
                    .any(|key| self.generation(*key).is_none())
            {
                return Err("invalid immutable root closure".into());
            }
        }
        for (key, generation) in &self.generations {
            if self.cohort(generation.cohort).is_none()
                || (generation.retired && (generation.ready || view.protected.contains(key)))
            {
                return Err("retired generation remains usable or protected".into());
            }
            if let Some(id) = generation.current_delete {
                let Some((_, delete)) = self.deletes.iter().find(|(candidate, _)| *candidate == id)
                else {
                    return Err("missing delete identity".into());
                };
                if !generation.retired
                    || delete.key != *key
                    || delete.write_revision != generation.write_revision
                    || delete.confirmed
                    || generation.delete_confirmed_revision.is_some()
                {
                    return Err("current delete disagrees with exact generation revision".into());
                }
            }
        }
        for (_, write) in &self.writes {
            if self.generation(write.key).is_none() {
                return Err("write references missing generation".into());
            }
        }
        if self.next_delete
            != u64::try_from(self.deletes.len())
                .map_err(|error| error.to_string())?
                .checked_add(1)
                .ok_or("delete counter overflow")?
            || self
                .deletes
                .iter()
                .enumerate()
                .any(|(index, (id, _))| id.0 != index as u64 + 1)
        {
            return Err("delete identity history is not contiguous".into());
        }
        for pass in &self.cohorts {
            if pass.cursor > pass.captured.len()
                || pass.captured.len() > self.limits.roots
                || !pass
                    .captured
                    .windows(2)
                    .all(|pair| pair[0].root < pair[1].root)
            {
                return Err("invalid captured pass".into());
            }
            let expected_marks: BTreeSet<_> = pass.captured[..pass.cursor]
                .iter()
                .flat_map(|root| root.closure.iter().copied())
                .filter(|key| {
                    self.generation(*key)
                        .is_some_and(|generation| generation.cohort == pass.id)
                })
                .collect();
            if view.cohorts[&pass.id].marked != expected_marks {
                return Err("mark relation does not match processed immutable slices".into());
            }
        }
        Ok(())
    }
}
