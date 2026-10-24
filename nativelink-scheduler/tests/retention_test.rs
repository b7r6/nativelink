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

//! Finite sealed-cohort refinement; no provider or typed graph is simulated.

#[path = "retention/coarse.rs"]
mod coarse;
#[path = "retention/protocol.rs"]
mod protocol;
#[path = "retention/reference.rs"]
mod reference;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use coarse::Coarse;
use protocol::*;
use reference::Reference;

struct Pair {
    candidate: Coarse,
    oracle: Reference,
    trace: Vec<Event>,
}

impl Pair {
    fn new(limits: Limits) -> Self {
        Self {
            candidate: Coarse::new(limits).unwrap(),
            oracle: Reference::new(limits).unwrap(),
            trace: Vec::new(),
        }
    }

    fn event(&mut self, event: Event) -> Reply {
        self.trace.push(event.clone());
        let before = self.candidate.view();
        let actual = self.candidate.apply(event.clone());
        let expected = self.oracle.apply(event);
        assert_eq!(actual, expected, "reply mismatch: {:?}", self.trace);
        self.oracle.check().unwrap();
        assert_eq!(
            self.candidate.view(),
            self.oracle.view(),
            "state mismatch: {:?}",
            self.trace
        );
        if actual != Reply::Applied {
            assert_eq!(
                self.candidate.view(),
                before,
                "nonmutating reply changed state"
            );
        }
        actual
    }

    fn applied(&mut self, event: Event) {
        assert_eq!(self.event(event), Reply::Applied);
    }

    fn rejected(&mut self, event: Event) {
        assert_eq!(self.event(event), Reply::Rejected);
    }

    fn view(&self) -> View {
        self.candidate.view()
    }

    fn restore(&mut self) {
        self.candidate = Coarse::restore(&self.candidate.snapshot().unwrap()).unwrap();
        self.oracle = Reference::restore(&self.oracle.snapshot().unwrap()).unwrap();
        assert_eq!(self.candidate.view(), self.oracle.view());
    }
}

const fn key(generation: u64) -> Key {
    Key {
        incarnation: 1,
        generation,
    }
}

fn acquire(
    view: &View,
    root: u64,
    lease: u64,
    kind: RootKind,
    closure: Vec<Key>,
    until: u64,
) -> Event {
    let fences = closure
        .iter()
        .map(|key| {
            let cohort = view.generations[key].cohort;
            (cohort, view.cohorts[&cohort].fence)
        })
        .collect();
    Event::Acquire {
        root: RootId(root),
        lease,
        kind,
        closure,
        until,
        fences,
    }
}

fn initialized() -> Pair {
    let mut pair = Pair::new(Limits::default());
    pair.applied(Event::CreateCohort {
        cohort: CohortId(1),
    });
    pair
}

fn allocate(pair: &mut Pair, generation: u64) {
    pair.applied(Event::Allocate {
        cohort: CohortId(1),
        key: key(generation),
        logical_object: generation,
    });
}

fn ready(pair: &mut Pair, generation: u64) {
    allocate(pair, generation);
    pair.applied(acquire(
        &pair.view(),
        generation,
        1,
        RootKind::Staging,
        vec![key(generation)],
        10,
    ));
    pair.applied(Event::StartWrite {
        write: WriteId(generation),
        key: key(generation),
        staging: RootId(generation),
        lease: 1,
    });
    pair.applied(Event::WriteDone {
        write: WriteId(generation),
        success: true,
    });
}

fn close(pair: &mut Pair) -> u64 {
    pair.applied(Event::Seal {
        cohort: CohortId(1),
    });
    pair.applied(Event::BeginClose {
        cohort: CohortId(1),
    });
    let fence = pair.view().cohorts[&CohortId(1)].fence;
    pair.applied(Event::FinishClose {
        cohort: CohortId(1),
        fence,
    });
    fence
}

fn mark_all(pair: &mut Pair, cohort: CohortId) {
    loop {
        let view = pair.view();
        let pass = &view.cohorts[&cohort];
        if pass.cursor == pass.captured.len() {
            return;
        }
        let roots = pass.captured[pass.cursor..]
            .iter()
            .take(view.limits.mark_batch)
            .cloned()
            .collect();
        pair.applied(Event::Mark {
            cohort,
            fence: pass.fence,
            at: pass.cursor,
            roots,
        });
    }
}

#[test]
fn staging_transfer_is_gapless_even_after_acquisition_closes() {
    let mut pair = initialized();
    ready(&mut pair, 1);
    pair.applied(Event::Seal {
        cohort: CohortId(1),
    });
    pair.applied(Event::BeginClose {
        cohort: CohortId(1),
    });
    pair.rejected(Event::FinishClose {
        cohort: CohortId(1),
        fence: 2,
    });
    pair.rejected(Event::Renew {
        root: RootId(1),
        lease: 1,
        until: 11,
    });
    pair.rejected(acquire(
        &pair.view(),
        2,
        1,
        RootKind::Publication,
        vec![key(1)],
        20,
    ));
    let protected = pair.view().protected;
    pair.applied(Event::Transfer {
        staging: RootId(1),
        lease: 1,
        publication: RootId(2),
        until: 20,
    });
    assert_eq!(pair.view().protected, protected);
    pair.applied(Event::FinishClose {
        cohort: CohortId(1),
        fence: 2,
    });
    mark_all(&mut pair, CohortId(1));
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence: 2,
        keys: vec![key(1)],
    });
    pair.restore();
}

#[test]
fn serving_coverage_survives_publication_removal_and_conservative_mark() {
    let mut pair = initialized();
    ready(&mut pair, 1);
    pair.applied(Event::Transfer {
        staging: RootId(1),
        lease: 1,
        publication: RootId(2),
        until: 20,
    });
    pair.applied(acquire(
        &pair.view(),
        3,
        1,
        RootKind::Serving,
        vec![key(1)],
        30,
    ));
    pair.applied(Event::Release {
        root: RootId(2),
        lease: 1,
    });
    assert!(pair.view().protected.contains(&key(1)));
    let fence = close(&mut pair);
    pair.applied(Event::Release {
        root: RootId(3),
        lease: 1,
    });
    assert!(pair.view().protected.is_empty());
    pair.restore();
    mark_all(&mut pair, CohortId(1));
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1)],
    });
    pair.applied(Event::Reopen {
        cohort: CohortId(1),
        fence,
    });
    pair.applied(Event::BeginClose {
        cohort: CohortId(1),
    });
    pair.applied(Event::FinishClose {
        cohort: CohortId(1),
        fence: fence + 2,
    });
    pair.applied(Event::Retire {
        cohort: CohortId(1),
        fence: fence + 2,
        keys: vec![key(1)],
    });
}

#[test]
fn sustained_live_renewals_preserve_mark_cursor_and_unrelated_collection() {
    let mut pair = initialized();
    for generation in 1..=2 {
        ready(&mut pair, generation);
        pair.applied(Event::Transfer {
            staging: RootId(generation),
            lease: 1,
            publication: RootId(generation + 10),
            until: 40,
        });
    }
    allocate(&mut pair, 3);
    let fence = close(&mut pair);
    let captured = pair.view().cohorts[&CohortId(1)].captured.clone();
    pair.applied(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: captured[..1].to_vec(),
    });
    for now in 1..=64 {
        pair.applied(Event::AdvanceTime { now });
        for root in [RootId(11), RootId(12)] {
            pair.applied(Event::Renew {
                root,
                lease: 1,
                until: now + 40,
            });
        }
        pair.rejected(acquire(
            &pair.view(),
            13,
            1,
            RootKind::Publication,
            vec![key(1)],
            now + 20,
        ));
        let pass = &pair.view().cohorts[&CohortId(1)];
        assert_eq!((pass.fence, pass.cursor), (fence, 1));
        assert_eq!(pass.captured, captured);
        if now % 8 == 0 {
            pair.restore();
        }
    }
    mark_all(&mut pair, CohortId(1));
    pair.applied(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(3)],
    });
    let delete = pair.view().generations[&key(3)].current_delete.unwrap();
    pair.applied(Event::DeleteDone {
        delete,
        success: true,
    });
    assert_eq!(pair.view().protected, BTreeSet::from([key(1), key(2)]));
    assert!(pair.view().generations[&key(3)].retired);
}

#[test]
fn expired_renewal_is_not_reacquisition_and_old_lease_cannot_revoke_new_one() {
    let mut pair = initialized();
    ready(&mut pair, 1);
    pair.applied(Event::Transfer {
        staging: RootId(1),
        lease: 1,
        publication: RootId(2),
        until: 4,
    });
    let fence = close(&mut pair);
    pair.applied(Event::AdvanceTime { now: 4 });
    pair.rejected(Event::Renew {
        root: RootId(2),
        lease: 1,
        until: 10,
    });
    pair.rejected(acquire(
        &pair.view(),
        2,
        2,
        RootKind::Publication,
        vec![key(1)],
        10,
    ));
    mark_all(&mut pair, CohortId(1));
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1)],
    });
    pair.applied(Event::Reopen {
        cohort: CohortId(1),
        fence,
    });
    pair.applied(acquire(
        &pair.view(),
        2,
        2,
        RootKind::Publication,
        vec![key(1)],
        10,
    ));
    pair.rejected(Event::Release {
        root: RootId(2),
        lease: 1,
    });
    pair.rejected(acquire(
        &pair.view(),
        2,
        3,
        RootKind::Serving,
        vec![key(1)],
        10,
    ));
    pair.restore();
}

#[test]
fn complete_captured_manifest_and_current_fence_are_required() {
    let mut pair = initialized();
    ready(&mut pair, 1);
    ready(&mut pair, 2);
    pair.applied(acquire(
        &pair.view(),
        3,
        1,
        RootKind::Publication,
        vec![key(1), key(2)],
        20,
    ));
    pair.applied(Event::Release {
        root: RootId(1),
        lease: 1,
    });
    pair.applied(Event::Release {
        root: RootId(2),
        lease: 1,
    });
    let fence = close(&mut pair);
    let captured = pair.view().cohorts[&CohortId(1)].captured.clone();
    let mut missing_child = captured.clone();
    missing_child[0].closure.pop();
    pair.rejected(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: missing_child,
    });
    pair.rejected(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 1,
        roots: captured.clone(),
    });
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1)],
    });
    pair.applied(Event::Release {
        root: RootId(3),
        lease: 1,
    });
    pair.applied(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: captured.clone(),
    });
    assert_eq!(
        pair.event(Event::Mark {
            cohort: CohortId(1),
            fence,
            at: 0,
            roots: captured.clone()
        }),
        Reply::Ignored
    );
    pair.applied(Event::Reopen {
        cohort: CohortId(1),
        fence,
    });
    pair.rejected(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: captured,
    });
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1)],
    });
    pair.applied(acquire(
        &pair.view(),
        3,
        2,
        RootKind::Publication,
        vec![key(1), key(2)],
        30,
    ));
    pair.applied(Event::BeginClose {
        cohort: CohortId(1),
    });
    pair.applied(Event::FinishClose {
        cohort: CohortId(1),
        fence: fence + 2,
    });
    mark_all(&mut pair, CohortId(1));
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence: fence + 2,
        keys: vec![key(1), key(2)],
    });
}

#[test]
fn sealing_fixes_population_and_closing_guards_expire_under_new_arrivals() {
    let mut pair = initialized();
    allocate(&mut pair, 1);
    pair.applied(acquire(
        &pair.view(),
        1,
        1,
        RootKind::Staging,
        vec![key(1)],
        5,
    ));
    pair.applied(Event::StartWrite {
        write: WriteId(1),
        key: key(1),
        staging: RootId(1),
        lease: 1,
    });
    pair.applied(Event::Seal {
        cohort: CohortId(1),
    });
    pair.rejected(Event::Allocate {
        cohort: CohortId(1),
        key: key(2),
        logical_object: 2,
    });
    pair.applied(Event::BeginClose {
        cohort: CohortId(1),
    });
    for now in 1..=5 {
        pair.rejected(acquire(
            &pair.view(),
            now + 1,
            1,
            RootKind::Staging,
            vec![key(1)],
            now + 5,
        ));
        pair.rejected(Event::Renew {
            root: RootId(1),
            lease: 1,
            until: now + 5,
        });
        pair.applied(Event::AdvanceTime { now });
        if now < 5 {
            pair.rejected(Event::FinishClose {
                cohort: CohortId(1),
                fence: 2,
            });
        }
    }
    pair.applied(Event::FinishClose {
        cohort: CohortId(1),
        fence: 2,
    });
    pair.applied(Event::Retire {
        cohort: CohortId(1),
        fence: 2,
        keys: vec![key(1)],
    });
    pair.applied(Event::WriteDone {
        write: WriteId(1),
        success: true,
    });
    assert!(!pair.view().generations[&key(1)].ready);
    pair.restore();
}

#[test]
fn late_write_requires_fresh_delete_and_old_locator_never_targets_recreation() {
    for acknowledge_first in [false, true] {
        let mut pair = initialized();
        allocate(&mut pair, 1);
        pair.applied(acquire(
            &pair.view(),
            1,
            1,
            RootKind::Staging,
            vec![key(1)],
            10,
        ));
        pair.applied(Event::StartWrite {
            write: WriteId(1),
            key: key(1),
            staging: RootId(1),
            lease: 1,
        });
        pair.applied(Event::Release {
            root: RootId(1),
            lease: 1,
        });
        let fence = close(&mut pair);
        pair.applied(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: vec![key(1)],
        });
        if acknowledge_first {
            pair.applied(Event::DeleteDone {
                delete: DeleteId(1),
                success: true,
            });
        }
        assert_eq!(pair.view().writes[&WriteId(1)].outcome, None);
        pair.restore();

        let replacement = Key {
            incarnation: 2,
            generation: 1,
        };
        pair.applied(Event::CreateCohort {
            cohort: CohortId(2),
        });
        pair.applied(Event::Allocate {
            cohort: CohortId(2),
            key: replacement,
            logical_object: 1,
        });
        pair.applied(acquire(
            &pair.view(),
            2,
            1,
            RootKind::Staging,
            vec![replacement],
            10,
        ));
        pair.applied(Event::StartWrite {
            write: WriteId(2),
            key: replacement,
            staging: RootId(2),
            lease: 1,
        });
        pair.applied(Event::WriteDone {
            write: WriteId(2),
            success: true,
        });
        let replacement_before = pair.view().generations[&replacement].clone();
        pair.applied(Event::WriteDone {
            write: WriteId(1),
            success: true,
        });
        assert_eq!(
            pair.view().generations[&key(1)].current_delete,
            Some(DeleteId(2))
        );
        let expected = if acknowledge_first {
            Reply::Ignored
        } else {
            Reply::Applied
        };
        assert_eq!(
            pair.event(Event::DeleteDone {
                delete: DeleteId(1),
                success: true
            }),
            expected
        );
        assert_eq!(
            pair.view().generations[&key(1)].current_delete,
            Some(DeleteId(2))
        );
        pair.applied(Event::DeleteDone {
            delete: DeleteId(2),
            success: true,
        });
        assert_eq!(pair.view().generations[&replacement], replacement_before);
        assert!(
            pair.view()
                .deletes
                .values()
                .all(|delete| delete.key == key(1))
        );
        pair.rejected(Event::WriteDone {
            write: WriteId(1),
            success: false,
        });
        pair.restore();
    }
}

#[test]
fn finite_admission_reserves_every_possible_late_write_delete() {
    let limits = Limits {
        cohorts: 1,
        generations: 2,
        roots: 2,
        closure: 1,
        mark_batch: 1,
        retire_batch: 1,
        writes: 2,
        delete_intents: 4,
        lease_ticks: 10,
    };
    let mut pair = Pair::new(limits);
    pair.applied(Event::CreateCohort {
        cohort: CohortId(1),
    });
    pair.rejected(Event::CreateCohort {
        cohort: CohortId(2),
    });
    allocate(&mut pair, 1);
    allocate(&mut pair, 2);
    pair.rejected(Event::Allocate {
        cohort: CohortId(1),
        key: key(3),
        logical_object: 3,
    });
    pair.rejected(acquire(
        &pair.view(),
        1,
        1,
        RootKind::Staging,
        vec![key(1), key(2)],
        10,
    ));
    for root in 1..=2 {
        pair.applied(acquire(
            &pair.view(),
            root,
            1,
            RootKind::Staging,
            vec![key(root)],
            10,
        ));
    }
    pair.rejected(acquire(
        &pair.view(),
        3,
        1,
        RootKind::Staging,
        vec![key(1)],
        10,
    ));
    for write in 1..=2 {
        pair.applied(Event::StartWrite {
            write: WriteId(write),
            key: key(1),
            staging: RootId(1),
            lease: 1,
        });
    }
    pair.rejected(Event::StartWrite {
        write: WriteId(3),
        key: key(1),
        staging: RootId(1),
        lease: 1,
    });
    pair.applied(Event::AdvanceTime { now: 10 });
    let fence = close(&mut pair);
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1), key(2)],
    });
    for generation in 1..=2 {
        pair.applied(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: vec![key(generation)],
        });
    }
    pair.applied(Event::DeleteDone {
        delete: DeleteId(1),
        success: true,
    });
    for write in 1..=2 {
        pair.applied(Event::WriteDone {
            write: WriteId(write),
            success: true,
        });
    }
    assert_eq!(pair.view().deletes.len(), limits.delete_intents);
    assert_eq!(
        pair.view().generations[&key(1)].current_delete,
        Some(DeleteId(4))
    );
    pair.restore();
}

#[test]
fn cross_cohort_manifest_is_complete_while_each_pass_marks_only_its_population() {
    let mut pair = initialized();
    ready(&mut pair, 1);
    pair.applied(Event::CreateCohort {
        cohort: CohortId(2),
    });
    pair.applied(Event::Allocate {
        cohort: CohortId(2),
        key: key(2),
        logical_object: 2,
    });
    pair.applied(acquire(
        &pair.view(),
        2,
        1,
        RootKind::Staging,
        vec![key(2)],
        10,
    ));
    pair.applied(Event::StartWrite {
        write: WriteId(2),
        key: key(2),
        staging: RootId(2),
        lease: 1,
    });
    pair.applied(Event::WriteDone {
        write: WriteId(2),
        success: true,
    });
    let event = acquire(
        &pair.view(),
        3,
        1,
        RootKind::Publication,
        vec![key(1), key(2)],
        30,
    );
    let Event::Acquire { mut fences, .. } = event.clone() else {
        unreachable!()
    };
    fences.remove(&CohortId(2));
    pair.rejected(Event::Acquire {
        root: RootId(3),
        lease: 1,
        kind: RootKind::Publication,
        closure: vec![key(1), key(2)],
        until: 30,
        fences,
    });
    pair.applied(event);
    pair.applied(Event::Release {
        root: RootId(1),
        lease: 1,
    });
    let fence = close(&mut pair);
    let captured = pair.view().cohorts[&CohortId(1)].captured.clone();
    assert_eq!(captured[0].closure, vec![key(1), key(2)]);
    let mut forged = captured;
    forged[0].closure.pop();
    pair.rejected(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: forged,
    });
    mark_all(&mut pair, CohortId(1));
    assert_eq!(
        pair.view().cohorts[&CohortId(1)].marked,
        BTreeSet::from([key(1)])
    );
}

#[test]
fn fair_finite_drain_settles_retirements_and_every_late_provider_write() {
    let mut pair = initialized();
    let population = 8u64;
    for generation in 1..=population {
        allocate(&mut pair, generation);
        pair.applied(acquire(
            &pair.view(),
            generation,
            1,
            RootKind::Staging,
            vec![key(generation)],
            10,
        ));
        pair.applied(Event::StartWrite {
            write: WriteId(generation),
            key: key(generation),
            staging: RootId(generation),
            lease: 1,
        });
        if generation <= population / 2 {
            pair.applied(Event::WriteDone {
                write: WriteId(generation),
                success: true,
            });
        }
    }
    let start = pair.trace.len();
    pair.applied(Event::AdvanceTime { now: 10 });
    let fence = close(&mut pair);
    let keys: Vec<_> = (1..=population).map(key).collect();
    for batch in keys.chunks(pair.view().limits.retire_batch) {
        pair.applied(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: batch.to_vec(),
        });
    }
    for delete in pair.view().deletes.keys().copied() {
        pair.applied(Event::DeleteDone {
            delete,
            success: true,
        });
    }
    pair.restore();
    // Fair provider recovery supplies terminal observations for all writes;
    // successful deletion alone deliberately did not discharge them.
    for generation in population / 2 + 1..=population {
        pair.applied(Event::WriteDone {
            write: WriteId(generation),
            success: true,
        });
    }
    for delete in pair
        .view()
        .generations
        .values()
        .filter_map(|generation| generation.current_delete)
    {
        pair.applied(Event::DeleteDone {
            delete,
            success: true,
        });
    }
    let steps = pair.trace.len() - start;
    // Clock+seal+begin+finish, bounded retirement batches, one first delete per
    // generation, and two transitions for each previously unresolved write.
    let bound = 4 + 8usize.div_ceil(pair.view().limits.retire_batch) + 8 + 2 * 4;
    assert_eq!(steps, bound);
    assert!(pair.view().protected.is_empty());
    assert!(
        pair.view()
            .writes
            .values()
            .all(|write| write.outcome.is_some())
    );
    assert!(
        pair.view()
            .generations
            .values()
            .all(|generation| generation.retired
                && !generation.ready
                && generation.current_delete.is_none()
                && generation.delete_confirmed_revision == Some(generation.write_revision))
    );
    evidence(
        "retention-fair-drain.json",
        &serde_json::json!({ "population": population, "initial_unresolved_writes": 4, "steps": steps, "derived_bound": bound, "assumptions": ["arrivals stop", "logical time advances", "collector receives service", "every admitted provider write becomes known quiescent", "every current exact-generation delete succeeds"] }),
    );
}

fn evidence(name: &str, value: &serde_json::Value) {
    let directory = std::env::var_os("NATIVELINK_RETENTION_EVIDENCE_DIR").map_or_else(
        || std::path::PathBuf::from("/tmp/nativelink-categorical-evidence"),
        std::path::PathBuf::from,
    );
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(name),
        serde_json::to_vec_pretty(value).unwrap(),
    )
    .unwrap();
}

const fn event_name(event: &Event) -> &'static str {
    match event {
        Event::AdvanceTime { .. } => "AdvanceTime",
        Event::CreateCohort { .. } => "CreateCohort",
        Event::Allocate { .. } => "Allocate",
        Event::Seal { .. } => "Seal",
        Event::BeginClose { .. } => "BeginClose",
        Event::FinishClose { .. } => "FinishClose",
        Event::Mark { .. } => "Mark",
        Event::Retire { .. } => "Retire",
        Event::Reopen { .. } => "Reopen",
        Event::Acquire { .. } => "Acquire",
        Event::Renew { .. } => "Renew",
        Event::Release { .. } => "Release",
        Event::Transfer { .. } => "Transfer",
        Event::StartWrite { .. } => "StartWrite",
        Event::WriteDone { .. } => "WriteDone",
        Event::DeleteDone { .. } => "DeleteDone",
    }
}

#[test]
fn exhaustive_small_retention_histories_refine_per_generation_oracle() {
    struct Node {
        candidate: Vec<u8>,
        oracle: Vec<u8>,
        suffix: Vec<Event>,
    }
    let mut seed = initialized();
    ready(&mut seed, 1);
    seed.applied(Event::Transfer {
        staging: RootId(1),
        lease: 1,
        publication: RootId(2),
        until: 2,
    });
    allocate(&mut seed, 2);
    seed.applied(acquire(
        &seed.view(),
        3,
        1,
        RootKind::Staging,
        vec![key(2)],
        1,
    ));
    seed.applied(Event::StartWrite {
        write: WriteId(2),
        key: key(2),
        staging: RootId(3),
        lease: 1,
    });
    seed.applied(Event::Release {
        root: RootId(3),
        lease: 1,
    });
    seed.applied(Event::Seal {
        cohort: CohortId(1),
    });
    let bootstrap = seed.trace.clone();
    let alphabet = vec![
        Event::BeginClose {
            cohort: CohortId(1),
        },
        Event::FinishClose {
            cohort: CohortId(1),
            fence: 2,
        },
        Event::Mark {
            cohort: CohortId(1),
            fence: 2,
            at: 0,
            roots: vec![CapturedRoot {
                root: RootId(2),
                lease: 1,
                closure: vec![key(1)],
            }],
        },
        Event::Retire {
            cohort: CohortId(1),
            fence: 2,
            keys: vec![key(1)],
        },
        Event::Retire {
            cohort: CohortId(1),
            fence: 2,
            keys: vec![key(2)],
        },
        Event::Reopen {
            cohort: CohortId(1),
            fence: 2,
        },
        Event::Renew {
            root: RootId(2),
            lease: 1,
            until: 3,
        },
        Event::Release {
            root: RootId(2),
            lease: 1,
        },
        Event::AdvanceTime { now: 2 },
        Event::AdvanceTime { now: 3 },
        Event::Acquire {
            root: RootId(2),
            lease: 2,
            kind: RootKind::Publication,
            closure: vec![key(1)],
            until: 3,
            fences: BTreeMap::from([(CohortId(1), 3)]),
        },
        Event::WriteDone {
            write: WriteId(2),
            success: true,
        },
        Event::DeleteDone {
            delete: DeleteId(1),
            success: true,
        },
        Event::DeleteDone {
            delete: DeleteId(2),
            success: true,
        },
    ];
    let initial = seed.candidate.snapshot().unwrap();
    let mut visited = BTreeSet::from([initial.clone()]);
    let mut queue = VecDeque::from([Node {
        candidate: initial,
        oracle: seed.oracle.snapshot().unwrap(),
        suffix: Vec::new(),
    }]);
    let depth = 7;
    let mut depth_counts = vec![0u64; depth + 1];
    depth_counts[0] = 1;
    let mut transitions = 0u64;
    let mut applied = BTreeMap::<&str, u64>::new();
    while let Some(node) = queue.pop_front() {
        if node.suffix.len() == depth {
            continue;
        }
        for event in &alphabet {
            let mut pair = Pair {
                candidate: Coarse::restore(&node.candidate).unwrap(),
                oracle: Reference::restore(&node.oracle).unwrap(),
                trace: bootstrap.iter().chain(&node.suffix).cloned().collect(),
            };
            if pair.event(event.clone()) == Reply::Applied {
                *applied.entry(event_name(event)).or_default() += 1;
            }
            transitions += 1;
            let candidate = pair.candidate.snapshot().unwrap();
            if visited.insert(candidate.clone()) {
                let mut suffix = node.suffix.clone();
                suffix.push(event.clone());
                depth_counts[suffix.len()] += 1;
                queue.push_back(Node {
                    candidate,
                    oracle: pair.oracle.snapshot().unwrap(),
                    suffix,
                });
            }
        }
    }
    assert!(visited.len() > 100);
    assert!(
        applied.contains_key("Retire")
            && applied.contains_key("DeleteDone")
            && applied.contains_key("Acquire")
    );
    evidence(
        "retention-exploration.json",
        &serde_json::json!({ "depth": depth, "alphabet": alphabet, "bootstrap": bootstrap, "states": visited.len(), "transitions": transitions, "depth_counts": depth_counts, "applied_variants": applied, "identity": "complete canonical candidate checkpoint", "state_cap": null, "partial_order_reduction": false, "snapshot_restore": "both models restored before every explored edge" }),
    );
    eprintln!(
        "retention exhaustive depth={depth} states={} transitions={transitions} levels={}",
        visited.len(),
        serde_json::to_string(&depth_counts).unwrap()
    );
}

#[test]
fn invalid_bounds_and_overflow_cannot_consume_authority() {
    for limits in [
        Limits {
            cohorts: 0,
            ..Limits::default()
        },
        Limits {
            delete_intents: 47,
            ..Limits::default()
        },
        Limits {
            generations: usize::MAX,
            writes: 1,
            delete_intents: usize::MAX,
            ..Limits::default()
        },
    ] {
        assert!(Coarse::new(limits).is_err());
        assert!(Reference::new(limits).is_err());
    }
    let mut pair = initialized();
    allocate(&mut pair, 1);
    pair.applied(Event::AdvanceTime { now: u64::MAX - 1 });
    pair.rejected(acquire(
        &pair.view(),
        1,
        1,
        RootKind::Staging,
        vec![key(1)],
        u64::MAX,
    ));
    pair.rejected(Event::AdvanceTime { now: 0 });
    pair.restore();
}

#[test]
fn input_and_operation_roots_renew_across_closed_cohort_without_membership_change() {
    for kind in [RootKind::Input, RootKind::Operation] {
        let mut pair = initialized();
        ready(&mut pair, 1);
        pair.applied(acquire(&pair.view(), 2, 1, kind, vec![key(1)], 10));
        pair.applied(Event::Release {
            root: RootId(1),
            lease: 1,
        });
        let fence = close(&mut pair);
        pair.applied(Event::Renew {
            root: RootId(2),
            lease: 1,
            until: 20,
        });
        pair.applied(Event::AdvanceTime { now: 10 });
        mark_all(&mut pair, CohortId(1));
        pair.rejected(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: vec![key(1)],
        });
        pair.applied(Event::AdvanceTime { now: 20 });
        pair.applied(Event::Reopen {
            cohort: CohortId(1),
            fence,
        });
        pair.rejected(acquire(
            &pair.view(),
            2,
            2,
            RootKind::Serving,
            vec![key(1)],
            30,
        ));
        pair.restore();
    }
}

#[test]
fn mark_batch_bound_and_mixed_retirement_failure_preserve_entire_state() {
    let mut pair = Pair::new(Limits {
        mark_batch: 1,
        ..Limits::default()
    });
    pair.applied(Event::CreateCohort {
        cohort: CohortId(1),
    });
    for generation in 1..=3 {
        ready(&mut pair, generation);
        pair.applied(Event::Release {
            root: RootId(generation),
            lease: 1,
        });
    }
    for generation in 2..=3 {
        pair.applied(acquire(
            &pair.view(),
            generation + 10,
            1,
            RootKind::Publication,
            vec![key(generation)],
            20,
        ));
    }
    let fence = close(&mut pair);
    let captured = pair.view().cohorts[&CohortId(1)].captured.clone();
    pair.rejected(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: captured.clone(),
    });
    pair.applied(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 0,
        roots: captured[..1].to_vec(),
    });
    let mut stale_lease = captured[1..].to_vec();
    stale_lease[0].lease += 1;
    pair.rejected(Event::Mark {
        cohort: CohortId(1),
        fence,
        at: 1,
        roots: stale_lease,
    });
    mark_all(&mut pair, CohortId(1));
    pair.rejected(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1), key(2)],
    });
    assert!(!pair.view().generations[&key(1)].retired);
    assert!(pair.view().deletes.is_empty());
    pair.applied(Event::Retire {
        cohort: CohortId(1),
        fence,
        keys: vec![key(1)],
    });
}
