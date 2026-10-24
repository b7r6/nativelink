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

//! Tests-first composition of supervised custody and the validated clock.
//! Private fixtures synthesize Sealed history; no production seal writer exists.

use std::time::Instant;

use super::super::super::{RecoveredCanonicalOwner, RecoveryDiagnostic};
use super::super::temporal as fixtures;
use super::*;
use crate::authority::journal::{Batch, Journal, LogId, RecordedEvent, encode};
use crate::authority::{Event, Phase, State};

const BASE: u64 = 17;
const CUTOFF: u64 = 18_446_743_758_133_551_613;
type Recovering = JoinHandle<Result<RecoveredCanonicalOwner, RecoveryError>>;

fn recover(owner: PreparedCanonicalOwner, hooks: Hooks) -> Recovering {
    // This private hook entry must be the same implementation called by the
    // public method; the wrapper may not suspend after receiving its armed owner.
    background_spawn!(
        "qualify_recovered_owner",
        owner.recover_sealed_with_hooks(hooks)
    )
}

async fn recovered(task: Recovering) -> Result<RecoveredCanonicalOwner, RecoveryError> {
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap()
}

async fn error_reaches_drop_before_return(task: &mut Recovering, dropping: oneshot::Receiver<()>) {
    tokio::time::timeout(WAIT, async {
        tokio::select! {
            result = task => panic!("recovery returned before its required failure cleanup: {result:?}"),
            result = dropping => result.unwrap(),
        }
    })
    .await
    .expect("neither actual final destruction nor a constructor result was observed");
}

async fn drop_reached_with_locks_held(fixture: &Fixture, mut dropping: oneshot::Receiver<()>) {
    tokio::time::timeout(WAIT, async {
        loop {
            tokio::select! {
                result = &mut dropping => { result.unwrap(); return; },
                () = tokio::task::yield_now() => fixture.locked(),
            }
        }
    })
    .await
    .expect("actual final destruction gate was never reached");
}

async fn reopen(fixture: &Fixture) -> PreparedCanonicalOwner {
    PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap()
}

fn raft_probe(hooks: &Hooks) -> oneshot::Receiver<Raft<TypeConfig>> {
    let (sender, receiver) = oneshot::channel();
    assert!(hooks.0.raft_probe.lock().unwrap().replace(sender).is_none());
    receiver
}

fn origin(hooks: &Hooks) -> Option<Instant> {
    *hooks.0.clock_origin.lock().unwrap()
}

fn state_bytes(fixture: &Fixture) -> Vec<u8> {
    fixtures::with_database(fixture, |database| {
        applied(
            database,
            ClusterEpoch(fixture.manifest.authority.cluster_epoch),
        )
        .scheduler
        .bytes()
        .unwrap()
    })
}

fn assert_capture(diagnostic: RecoveryDiagnostic, fixture: &Fixture, base: u64) {
    assert_eq!(diagnostic.owner.node_id, fixture.manifest.authority.node_id);
    assert_eq!(
        diagnostic.owner.epoch,
        fixture.manifest.authority.cluster_epoch
    );
    assert_eq!(
        diagnostic.owner.namespace,
        fixture.manifest.authority.namespace
    );
    assert_eq!(diagnostic.owner.phase, OwnerPhase::Sealed);
    assert_eq!(diagnostic.applied_node_id, diagnostic.owner.node_id);
    assert!(diagnostic.applied_term >= 1);
    assert!(diagnostic.applied_index >= 1);
    assert_eq!(diagnostic.base_tick, base);
    assert!((base..=CUTOFF).contains(&diagnostic.first_tick));
}

struct HeldApply {
    fixture: Fixture,
    hooks: Hooks,
    caller: Recovering,
    proposal: JoinHandle<bool>,
    journal: Journal,
    raft: Raft<TypeConfig>,
    release: oneshot::Sender<()>,
    watermark: LogId<u64>,
}

async fn held_apply() -> HeldApply {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let journal = owner.journal.clone();
    let hooks = Hooks::default();
    let probe = raft_probe(&hooks);
    let (before_clock, resume_clock) = hooks.gate(Stage::BeforeClock);
    let caller = recover(owner, hooks.clone());
    reached(before_clock).await;
    let raft = tokio::time::timeout(WAIT, probe).await.unwrap().unwrap();
    // Establish an applied initial leader entry before installing the actual
    // Base apply gate. The next committed entry then has a known stale base.
    tokio::time::timeout(WAIT, raft.ensure_linearizable())
        .await
        .expect("initial applied leader prerequisite stalled before the Base gate")
        .unwrap();
    let (applying, release) = journal.pause_canonical_base_apply_once();
    let writer = journal.clone();
    let proposer = raft.clone();
    let proposal = background_spawn!("composed_real_base_proposal", async move {
        writer
            .propose_batch(
                &proposer,
                Batch::new([RecordedEvent {
                    epoch: writer.epoch,
                    event: Event::AdvanceTime { now: BASE + 9 },
                }])
                .unwrap(),
            )
            .await
            .is_ok()
    });
    reached(applying).await;
    let (watermark, applied) = tokio::time::timeout(WAIT, raft.get_read_log_id())
        .await
        .expect("actual leader read watermark prerequisite stalled")
        .unwrap();
    let watermark = watermark.unwrap();
    assert!(watermark.index > applied.unwrap().index);
    let facts = tokio::time::timeout(
        WAIT,
        super::super::super::temporal::audit(
            &journal,
            journal.domain,
            journal.canonical_contract.unwrap(),
            CanonicalBootstrap::Sealed,
            None,
        ),
    )
    .await
    .expect("actual Base gate must leave the journal read queue available")
    .unwrap();
    assert_eq!(facts.now, BASE);
    assert!(facts.last_applied.unwrap().index < watermark.index);
    resume_clock.send(()).unwrap();
    hooks.wait(Stage::ClockReadStarted).await;
    HeldApply {
        fixture,
        hooks,
        caller,
        proposal,
        journal,
        raft,
        release,
        watermark,
    }
}

#[nativelink_test]
async fn composed_public_preparing_refusal_preserves_storage() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let before = records(&owner.journal.database);
    assert_eq!(
        owner.recover_sealed().await.unwrap_err(),
        RecoveryError::Phase
    );
    fixture.released().await;
    assert_eq!(stored(&fixture), before);
}

#[nativelink_test]
async fn composed_recovery_captures_actually_replayed_committed_and_uncommitted_time() {
    for committed in [false, true] {
        let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
        fixtures::suffix(
            &fixture,
            vec![vec![Event::AdvanceTime { now: BASE + 9 }]],
            committed,
        );
        let owner = reopen(&fixture).await;
        let old = owner.diagnostic();
        assert_eq!(
            applied(&owner.journal.database, ClusterEpoch(old.epoch))
                .last_applied
                .unwrap()
                .index,
            0
        );
        let active = owner.recover_sealed().await.unwrap();
        assert_eq!(active.diagnostic().owner, old);
        assert_capture(active.diagnostic(), &fixture, BASE + 9);
        fixture.locked();
        active.shutdown().await.unwrap();
        fixture.released().await;
    }
}

#[nativelink_test]
async fn composed_owner_waits_for_real_base_apply_with_read_queue_available() {
    let HeldApply {
        fixture,
        hooks,
        mut caller,
        proposal,
        journal,
        raft,
        release,
        watermark,
    } = held_apply().await;
    pending(&mut caller).await;
    assert!(!hooks.seen(Stage::Offered));
    assert!(origin(&hooks).is_none());
    let released_at = Instant::now();
    release.send(()).unwrap();
    assert!(tokio::time::timeout(WAIT, proposal).await.unwrap().unwrap());
    let active = recovered(caller).await.unwrap();
    assert_eq!(active.diagnostic().base_tick, BASE + 9);
    assert!(active.diagnostic().applied_index >= watermark.index);
    assert!(origin(&hooks).unwrap() >= released_at);
    // Both intentionally exported cfg(test) aliases disappear before shutdown.
    drop(raft);
    drop(journal);
    active.shutdown().await.unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_cancel_during_real_base_barrier_retains_cleanup_owner() {
    let HeldApply {
        fixture,
        hooks,
        mut caller,
        proposal,
        journal,
        raft,
        release,
        watermark: _,
    } = held_apply().await;
    pending(&mut caller).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    proposal.abort();
    assert!(proposal.await.unwrap_err().is_cancelled());
    drop(raft);
    drop(journal);
    fixture.locked();
    assert!(!hooks.seen(Stage::Offered));
    assert!(!hooks.seen(Stage::DatabaseDropped));
    release.send(()).unwrap();
    finished(&fixture, &hooks).await;
    assert!(!hooks.seen(Stage::Offered));
}

#[nativelink_test]
async fn composed_origin_follows_completed_validated_read() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let hooks = Hooks::default();
    let (hook, reading, gate, done) = fixtures::temporal_pause();
    *hooks.0.clock_audit.lock().unwrap() = Some(hook);
    let mut caller = recover(owner, hooks.clone());
    reached(reading).await;
    pending(&mut caller).await;
    assert!(
        origin(&hooks).is_none(),
        "origin was captured before the validated read completed"
    );
    assert!(!hooks.seen(Stage::Offered));
    let released_at = Instant::now();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_one();
    assert!(tokio::time::timeout(WAIT, done).await.unwrap().unwrap());
    let active = recovered(caller).await.unwrap();
    assert!(origin(&hooks).unwrap() >= released_at);
    assert_capture(active.diagnostic(), &fixture, BASE);
    active.shutdown().await.unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_cancel_during_validated_read_keeps_storage_until_actual_completion() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let hooks = Hooks::default();
    let (hook, reading, gate, done) = fixtures::temporal_pause();
    *hooks.0.clock_audit.lock().unwrap() = Some(hook);
    let (dropping, release_drop) = hooks.final_drop();
    let caller = recover(owner, hooks.clone());
    reached(reading).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    fixture.locked();
    assert!(!hooks.seen(Stage::DatabaseDropped));
    assert!(!hooks.seen(Stage::Offered));
    *gate.0.lock().unwrap() = true;
    gate.1.notify_one();
    assert!(tokio::time::timeout(WAIT, done).await.unwrap().unwrap());
    // Loss of the guardian can release the real backend without ever entering
    // the required final-drop gate. Check that direct physical violation while
    // waiting; a missing gate or its timeout alone is not a fault detection.
    drop_reached_with_locks_held(&fixture, dropping).await;
    fixture.locked();
    assert!(!hooks.seen(Stage::Offered));
    release_drop.send(()).unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test(flavor = "multi_thread", worker_threads = 2)]
async fn composed_real_first_sample_range_failure_cannot_offer_or_skip_final_drop() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, CUTOFF)).await;
    let owner = reopen(&fixture).await;
    let before = applied(
        &owner.journal.database,
        ClusterEpoch(owner.diagnostic().epoch),
    )
    .scheduler
    .bytes()
    .unwrap();
    let hooks = Hooks::default();
    let (captured, waiting) = oneshot::channel();
    let (release_capture, resume) = std::sync::mpsc::channel();
    *hooks.0.clock_capture.lock().unwrap() = Some(Box::new(move |actual_origin| {
        captured.send(actual_origin).unwrap();
        resume.recv_timeout(WAIT).unwrap();
    }));
    let (dropping, release_drop) = hooks.final_drop();
    let mut caller = recover(owner, hooks.clone());
    let actual_origin = tokio::time::timeout(WAIT, waiting).await.unwrap().unwrap();
    assert_eq!(origin(&hooks), Some(actual_origin));
    pending(&mut caller).await;
    assert!(!hooks.seen(Stage::Offered));
    while actual_origin.elapsed() < Duration::from_secs(1) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Both instants and base facts are real. Only scheduling is controlled.
    release_capture.send(()).unwrap();
    error_reaches_drop_before_return(&mut caller, dropping).await;
    assert!(hooks.seen(Stage::CoreJoined));
    assert!(!hooks.seen(Stage::Offered));
    pending(&mut caller).await;
    fixture.locked();
    release_drop.send(()).unwrap();
    assert_eq!(
        recovered(caller).await.unwrap_err(),
        RecoveryError::ClockRange
    );
    finished(&fixture, &hooks).await;
    assert_eq!(state_bytes(&fixture), before);
}

#[nativelink_test]
async fn composed_post_leader_metadata_refusal_waits_for_actual_drop() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let journal = owner.journal.clone();
    let hooks = Hooks::default();
    let (before_clock, resume_clock) = hooks.gate(Stage::BeforeClock);
    let (dropping, release_drop) = hooks.final_drop();
    let mut caller = recover(owner, hooks.clone());
    reached(before_clock).await;
    edit(&journal.database, |table| {
        table
            .insert(
                "canonical_bootstrap",
                encode(journal.epoch, &CanonicalBootstrap::Preparing)
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
    });
    drop(journal);
    resume_clock.send(()).unwrap();
    error_reaches_drop_before_return(&mut caller, dropping).await;
    assert!(origin(&hooks).is_none());
    assert!(!hooks.seen(Stage::Offered));
    pending(&mut caller).await;
    fixture.locked();
    release_drop.send(()).unwrap();
    assert_eq!(
        recovered(caller).await.unwrap_err(),
        RecoveryError::ClockRead
    );
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_panic_after_actual_origin_still_drains_before_error() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let hooks = Hooks::default();
    *hooks.0.clock_capture.lock().unwrap() = Some(Box::new(|_| {
        panic!("controlled panic after actual composed clock origin")
    }));
    let (dropping, release_drop) = hooks.final_drop();
    let mut caller = recover(owner, hooks.clone());
    error_reaches_drop_before_return(&mut caller, dropping).await;
    assert!(origin(&hooks).is_some());
    assert!(!hooks.seen(Stage::Offered));
    assert!(hooks.seen(Stage::CoreJoined));
    pending(&mut caller).await;
    fixture.locked();
    release_drop.send(()).unwrap();
    assert_eq!(recovered(caller).await.unwrap_err(), RecoveryError::Panic);
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_drop_and_unpolled_shutdown_request_permanent_cleanup() {
    for unpolled_shutdown in [false, true] {
        let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
        let hooks = Hooks::default();
        let (dropping, release_drop) = hooks.final_drop();
        let active = recovered(recover(reopen(&fixture).await, hooks.clone()))
            .await
            .unwrap();
        if unpolled_shutdown {
            drop(active.shutdown());
        } else {
            drop(active);
        }
        reached(dropping).await;
        fixture.locked();
        assert!(!hooks.seen(Stage::DatabaseDropped));
        release_drop.send(()).unwrap();
        finished(&fixture, &hooks).await;
    }
}

#[nativelink_test]
async fn composed_cancelled_shutdown_waiter_cannot_abort_final_destruction() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let hooks = Hooks::default();
    let (dropping, release_drop) = hooks.final_drop();
    let active = recovered(recover(reopen(&fixture).await, hooks.clone()))
        .await
        .unwrap();
    let mut stopping = background_spawn!("composed_cancel_stop", active.shutdown());
    reached(dropping).await;
    pending(&mut stopping).await;
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    fixture.locked();
    release_drop.send(()).unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_shutdown_waits_for_real_base_state_machine_after_core_join() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let journal = owner.journal.clone();
    let hooks = Hooks::default();
    let probe = raft_probe(&hooks);
    let active = recovered(recover(owner, hooks.clone())).await.unwrap();
    let raft = tokio::time::timeout(WAIT, probe).await.unwrap().unwrap();
    let (applying, release) = journal.pause_canonical_base_apply_once();
    let proposal = background_spawn!("composed_held_state_machine", async move {
        journal
            .propose_batch(
                &raft,
                Batch::new([RecordedEvent {
                    epoch: journal.epoch,
                    event: Event::AdvanceTime { now: BASE + 9 },
                }])
                .unwrap(),
            )
            .await
            .is_ok()
    });
    reached(applying).await;
    let mut stopping = background_spawn!("composed_state_machine_stop", active.shutdown());
    hooks.wait(Stage::BeforeDrain).await;
    assert!(hooks.seen(Stage::CoreJoined));
    // Core shutdown completes the proposal waiter with an error. Awaiting it
    // also drops the test-owned Journal and Raft; only the actual SM retains I/O.
    assert!(!tokio::time::timeout(WAIT, proposal).await.unwrap().unwrap());
    pending(&mut stopping).await;
    fixture.locked();
    release.send(()).unwrap();
    stopped(stopping).await.unwrap();
    finished(&fixture, &hooks).await;
    fixtures::with_database(&fixture, |database| {
        assert_eq!(
            State::restore(
                &applied(
                    database,
                    ClusterEpoch(fixture.manifest.authority.cluster_epoch)
                )
                .scheduler
                .bytes()
                .unwrap()
            )
            .unwrap()
            .now,
            BASE + 9
        );
    });
}

#[nativelink_test]
async fn composed_shutdown_waits_for_actual_spawned_snapshot_after_core_join() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let journal = owner.journal.clone();
    let hooks = Hooks::default();
    let probe = raft_probe(&hooks);
    let active = recovered(recover(owner, hooks.clone())).await.unwrap();
    let raft = tokio::time::timeout(WAIT, probe).await.unwrap().unwrap();
    journal.enable_measurements();
    let measurements = journal.measurements.clone();
    let counter = &measurements.operations[MeasuredOperation::BuildSnapshot as usize];
    assert_eq!(counter.calls.load(Ordering::Relaxed), 0);
    let queue = tokio::time::timeout(WAIT, journal.queue.clone().lock_owned())
        .await
        .expect("snapshot queue acquisition prerequisite stalled");
    tokio::time::timeout(WAIT, raft.trigger().snapshot())
        .await
        .expect("actual snapshot trigger prerequisite stalled")
        .unwrap();
    tokio::time::timeout(WAIT, async {
        while counter.calls.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(counter.calls.load(Ordering::Relaxed), 1);
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 0);
    drop(raft);
    drop(journal);
    let mut stopping = background_spawn!("composed_snapshot_stop", active.shutdown());
    hooks.wait(Stage::BeforeDrain).await;
    assert!(hooks.seen(Stage::CoreJoined));
    pending(&mut stopping).await;
    fixture.locked();
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 0);
    drop(queue);
    stopped(stopping).await.unwrap();
    finished(&fixture, &hooks).await;
    assert_eq!(counter.completed_calls.load(Ordering::Relaxed), 1);
    fixtures::with_database(&fixture, |database| {
        assert!(
            database
                .begin_read()
                .unwrap()
                .open_table(META)
                .unwrap()
                .get("snapshot")
                .unwrap()
                .is_some()
        );
    });
}

#[nativelink_test]
async fn composed_late_storage_clone_cannot_be_bypassed_by_shutdown() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let first = owner.journal.clone();
    let hooks = Hooks::default();
    let active = recovered(recover(owner, hooks.clone())).await.unwrap();
    let mut stopping = background_spawn!("composed_late_clone_stop", active.shutdown());
    hooks.wait(Stage::BeforeDrain).await;
    assert!(hooks.seen(Stage::CoreJoined));
    pending(&mut stopping).await;
    let late = first.clone();
    drop(first);
    pending(&mut stopping).await;
    fixture.locked();
    drop(late);
    stopped(stopping).await.unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_nested_cleanup_holder_is_released_before_acknowledgement() {
    let fixture = fixtures::seeded(|epoch| fixtures::empty(epoch, BASE)).await;
    let owner = reopen(&fixture).await;
    let journal = owner.journal.clone();
    let hooks = Hooks::default();
    let active = recovered(recover(owner, hooks.clone())).await.unwrap();
    let (start_nested, begin) = oneshot::channel();
    let (nested_started, nested_waiting) = oneshot::channel();
    let (release_nested, nested_release) = oneshot::channel();
    let outer = background_spawn!("composed_nested_outer_holder", async move {
        tokio::time::timeout(WAIT, begin).await.unwrap().unwrap();
        let nested_journal = journal.clone();
        drop(journal);
        let inner = background_spawn!("composed_nested_inner_holder", async move {
            nested_started.send(()).unwrap();
            tokio::time::timeout(WAIT, nested_release)
                .await
                .unwrap()
                .unwrap();
            drop(nested_journal);
        });
        tokio::time::timeout(WAIT, inner).await.unwrap().unwrap();
    });
    let mut stopping = background_spawn!("composed_nested_holder_stop", active.shutdown());
    hooks.wait(Stage::BeforeDrain).await;
    assert!(hooks.seen(Stage::CoreJoined));
    pending(&mut stopping).await;
    start_nested.send(()).unwrap();
    reached(nested_waiting).await;
    pending(&mut stopping).await;
    fixture.locked();
    release_nested.send(()).unwrap();
    tokio::time::timeout(WAIT, outer).await.unwrap().unwrap();
    stopped(stopping).await.unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn composed_restart_preserves_application_deadlines_and_ignores_downtime() {
    let fixture = fixtures::seeded(|epoch| {
        fixtures::attempt(epoch, BASE, fixtures::finite(5, 0), Phase::Running)
    })
    .await;
    let before = state_bytes(&fixture);
    let hooks = Hooks::default();
    let active = recovered(recover(reopen(&fixture).await, hooks.clone()))
        .await
        .unwrap();
    let capture = active.diagnostic();
    assert_capture(capture, &fixture, BASE);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(
        active.diagnostic(),
        capture,
        "diagnostic is a historical capture"
    );
    active.shutdown().await.unwrap();
    finished(&fixture, &hooks).await;
    assert_eq!(state_bytes(&fixture), before);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let before_restart = Instant::now();
    let fresh_hooks = Hooks::default();
    let active = recovered(recover(reopen(&fixture).await, fresh_hooks.clone()))
        .await
        .unwrap();
    assert_eq!(active.diagnostic().base_tick, BASE);
    assert!(origin(&fresh_hooks).unwrap() >= before_restart);
    active.shutdown().await.unwrap();
    finished(&fixture, &fresh_hooks).await;
    assert_eq!(state_bytes(&fixture), before);
    fixtures::with_database(&fixture, |database| {
        let tx = database.begin_read().unwrap();
        let phase: CanonicalBootstrap = decode(
            ClusterEpoch(fixture.manifest.authority.cluster_epoch),
            tx.open_table(META)
                .unwrap()
                .get("canonical_bootstrap")
                .unwrap()
                .unwrap()
                .value(),
        )
        .unwrap();
        assert_eq!(phase, CanonicalBootstrap::Sealed);
    });
}
