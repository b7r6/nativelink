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

//! Private supervised lifecycle only: no public recovery, read barrier or clock.
//! Sealed input is manufactured by the existing private test fixture.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nativelink_macro::nativelink_test;
use nativelink_util::background_spawn;
use openraft::Raft;
use redb::{Database, ReadableDatabase, ReadableTable};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use super::super::canonical_cleanup::DropGate;
use super::super::canonical_recovery::{LifecycleOwner, RecoveryError, start};
use super::super::{CanonicalBootstrap, ClusterEpoch, OwnerPhase, PreparedCanonicalOwner};
use super::{Fixture, applied, edit, sealed};
use crate::authority::journal::{LOGS, META, MeasuredOperation, TypeConfig, decode};

#[path = "canonical_recovered_owner_tests.rs"]
mod composition;

const WAIT: Duration = Duration::from_secs(10);
type Starting = JoinHandle<Result<LifecycleOwner, RecoveryError>>;
type Stopping = JoinHandle<Result<(), RecoveryError>>;
type Records = (Vec<(String, Vec<u8>)>, Vec<(u64, Vec<u8>)>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::authority::journal::canonical_owner) enum Stage {
    ConsumerWaiting,
    SupervisorStarted,
    BeforePreflight,
    BeforeRaft,
    RaftStored,
    Initialized,
    Leader,
    BeforeClock,
    ClockReadStarted,
    ClockStored,
    BeforeOffer,
    Offered,
    ConsumerOffered,
    ConsumerArmed,
    ConsumerAccepted,
    Accepted,
    StopObserved,
    CoreJoined,
    BeforeDrain,
    DatabaseDropped,
    SupervisorDone,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    Error,
    Panic,
}

struct Gate {
    stage: Stage,
    reached: oneshot::Sender<()>,
    resume: oneshot::Receiver<()>,
}

#[derive(Default)]
struct Control {
    stages: Mutex<Vec<Stage>>,
    gates: Mutex<Vec<Gate>>,
    fault: Mutex<Option<(Stage, Fault)>>,
    drop_gate: Mutex<Option<DropGate>>,
    reject_acceptance: AtomicBool,
    raft_probe: Mutex<Option<oneshot::Sender<Raft<TypeConfig>>>>,
    clock_audit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    clock_capture: Mutex<Option<Box<dyn FnOnce(Instant) + Send>>>,
    clock_origin: Mutex<Option<Instant>>,
}

#[derive(Clone, Default)]
pub(in crate::authority::journal::canonical_owner) struct Hooks(Arc<Control>);

impl Hooks {
    fn gate(&self, stage: Stage) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached, waiting) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        self.0.gates.lock().unwrap().push(Gate {
            stage,
            reached,
            resume,
        });
        (waiting, release)
    }

    fn final_drop(&self) -> (oneshot::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (reached, waiting) = oneshot::channel();
        let (release, resume) = std::sync::mpsc::channel();
        assert!(
            self.0
                .drop_gate
                .lock()
                .unwrap()
                .replace(DropGate { reached, resume })
                .is_none()
        );
        (waiting, release)
    }

    fn fault(&self, stage: Stage, fault: Fault) {
        assert!(
            self.0
                .fault
                .lock()
                .unwrap()
                .replace((stage, fault))
                .is_none()
        );
    }

    fn seen(&self, stage: Stage) -> bool {
        self.0.stages.lock().unwrap().contains(&stage)
    }

    async fn wait(&self, stage: Stage) {
        tokio::time::timeout(WAIT, async {
            while !self.seen(stage) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("lifecycle prerequisite never reached {stage:?}"));
    }

    pub(in crate::authority::journal::canonical_owner) fn observe(&self, stage: Stage) {
        self.0.stages.lock().unwrap().push(stage);
    }

    pub(in crate::authority::journal::canonical_owner) async fn at(
        &self,
        stage: Stage,
    ) -> Result<(), RecoveryError> {
        self.observe(stage);
        let gate = {
            let mut gates = self.0.gates.lock().unwrap();
            gates
                .iter()
                .position(|gate| gate.stage == stage)
                .map(|index| gates.remove(index))
        };
        if let Some(gate) = gate {
            gate.reached.send(()).map_err(|()| RecoveryError::Task)?;
            tokio::time::timeout(WAIT, gate.resume)
                .await
                .map_err(|_| RecoveryError::Task)?
                .map_err(|_| RecoveryError::Task)?;
        }
        let fault = *self.0.fault.lock().unwrap();
        match fault.filter(|(at, _)| *at == stage).map(|(_, fault)| fault) {
            Some(Fault::Error) => Err(RecoveryError::Runtime),
            Some(Fault::Panic) => panic!("controlled lifecycle panic at {stage:?}"),
            None => Ok(()),
        }
    }

    // Acceptance and return must occur in one consumer poll. This hook can
    // observe or panic but cannot add an await after the acceptance send.
    pub(in crate::authority::journal::canonical_owner) fn accepted_in_poll(&self) {
        self.observe(Stage::ConsumerAccepted);
        let fault = *self.0.fault.lock().unwrap();
        assert_ne!(
            fault,
            Some((Stage::ConsumerAccepted, Fault::Panic)),
            "controlled panic after consumer acceptance"
        );
    }

    pub(in crate::authority::journal::canonical_owner) fn take_drop_gate(
        &self,
    ) -> Option<DropGate> {
        self.0.drop_gate.lock().unwrap().take()
    }

    pub(in crate::authority::journal::canonical_owner) fn reject_acceptance(&self) -> bool {
        self.0.reject_acceptance.load(Ordering::Relaxed)
    }

    pub(in crate::authority::journal::canonical_owner) fn take_clock_audit(
        &self,
    ) -> Option<Box<dyn FnOnce() + Send>> {
        self.0.clock_audit.lock().unwrap().take()
    }

    // Called with the actual production origin after the actual read completed.
    // Test callbacks can block or panic; they cannot replace facts or instants.
    pub(in crate::authority::journal::canonical_owner) fn clock_capture(&self, origin: Instant) {
        assert!(
            self.0
                .clock_origin
                .lock()
                .unwrap()
                .replace(origin)
                .is_none()
        );
        let hook = self.0.clock_capture.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(origin);
        }
    }

    // Test-only observation of a borrowed resource. No production callback or
    // raw-handle conversion exists; the one test drops its clone before drain.
    pub(in crate::authority::journal::canonical_owner) fn publish_raft(
        &self,
        raft: &Raft<TypeConfig>,
    ) {
        let sender = self.0.raft_probe.lock().unwrap().take();
        if let Some(sender) = sender {
            let _delivered = sender.send(raft.clone());
        }
    }
}

async fn reached(waiting: oneshot::Receiver<()>) {
    tokio::time::timeout(WAIT, waiting)
        .await
        .expect("lifecycle gate was never reached")
        .unwrap();
}

async fn pending<T>(task: &mut JoinHandle<T>) {
    assert!(
        tokio::time::timeout(Duration::from_millis(25), task)
            .await
            .is_err(),
        "lifecycle completed before the witnessed custody/drop gate was released"
    );
}

async fn stopped(task: Stopping) -> Result<(), RecoveryError> {
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap()
}

async fn started(task: Starting) -> Result<LifecycleOwner, RecoveryError> {
    tokio::time::timeout(WAIT, task).await.unwrap().unwrap()
}

fn launch(owner: PreparedCanonicalOwner, hooks: Hooks) -> Starting {
    background_spawn!("qualify_canonical_lifecycle", start(owner, hooks))
}

async fn prepared(fixture: &Fixture) -> PreparedCanonicalOwner {
    sealed(fixture).await;
    PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap()
}

fn records(database: &Database) -> Records {
    let transaction = database.begin_read().unwrap();
    let metadata = transaction
        .open_table(META)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_owned(), value.value().to_vec())
        })
        .collect();
    let logs = transaction
        .open_table(LOGS)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value(), value.value().to_vec())
        })
        .collect();
    (metadata, logs)
}

fn stored(fixture: &Fixture) -> Records {
    let owned = fixture.owned(false);
    let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
    records(&database)
}

async fn finished(fixture: &Fixture, hooks: &Hooks) {
    hooks.wait(Stage::SupervisorDone).await;
    assert!(hooks.seen(Stage::DatabaseDropped));
    fixture.released().await;
}

#[nativelink_test]
async fn lifecycle_unpolled_future_starts_nothing() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    let future = start(owner, hooks.clone());
    drop(future);
    fixture.released().await;
    assert!(hooks.0.stages.lock().unwrap().is_empty());
}

#[nativelink_test]
async fn lifecycle_preparing_refuses_before_raft_and_preserves_records() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let before = records(&owner.journal.database);
    let hooks = Hooks::default();
    assert_eq!(
        started(launch(owner, hooks.clone())).await.unwrap_err(),
        RecoveryError::Phase
    );
    finished(&fixture, &hooks).await;
    assert!(!hooks.seen(Stage::BeforeRaft));
    assert!(!hooks.seen(Stage::RaftStored));
    assert_eq!(stored(&fixture), before);
}

#[nativelink_test]
async fn lifecycle_created_flag_refuses_even_a_private_sealed_fixture() {
    let fixture = Fixture::new();
    let mut owner = prepared(&fixture).await;
    let before = records(&owner.journal.database);
    // Only this private fixture alters original creation provenance.
    owner.journal.created = true;
    let hooks = Hooks::default();
    assert_eq!(
        started(launch(owner, hooks.clone())).await.unwrap_err(),
        RecoveryError::Phase
    );
    finished(&fixture, &hooks).await;
    assert!(!hooks.seen(Stage::BeforeRaft));
    assert_eq!(stored(&fixture), before);
}

#[nativelink_test]
async fn lifecycle_real_sealed_start_and_shutdown_preserve_application_state() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let diagnostic = owner.diagnostic();
    let epoch = ClusterEpoch(diagnostic.epoch);
    let before = applied(&owner.journal.database, epoch)
        .scheduler
        .bytes()
        .unwrap();
    let hooks = Hooks::default();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    assert_eq!(active.diagnostic(), diagnostic);
    assert_eq!(active.diagnostic().phase, OwnerPhase::Sealed);
    for stage in [
        Stage::RaftStored,
        Stage::Initialized,
        Stage::Leader,
        Stage::Offered,
        Stage::ConsumerArmed,
        Stage::ConsumerAccepted,
    ] {
        assert!(hooks.seen(stage), "missing real lifecycle stage {stage:?}");
    }
    hooks.wait(Stage::Accepted).await;
    fixture.locked();
    stopped(background_spawn!(
        "qualify_lifecycle_stop",
        active.shutdown()
    ))
    .await
    .unwrap();
    finished(&fixture, &hooks).await;
    assert!(hooks.seen(Stage::CoreJoined));
    let owned = fixture.owned(false);
    let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
    assert_eq!(applied(&database, epoch).scheduler.bytes().unwrap(), before);
    let transaction = database.begin_read().unwrap();
    let table = transaction.open_table(META).unwrap();
    let phase: CanonicalBootstrap = decode(
        epoch,
        table.get("canonical_bootstrap").unwrap().unwrap().value(),
    )
    .unwrap();
    assert_eq!(phase, CanonicalBootstrap::Sealed);
}

#[nativelink_test]
async fn lifecycle_cancel_at_each_reached_startup_stage_retains_supervisor() {
    for stage in [
        Stage::BeforePreflight,
        Stage::BeforeRaft,
        Stage::RaftStored,
        Stage::Initialized,
        Stage::Leader,
        Stage::BeforeOffer,
    ] {
        let fixture = Fixture::new();
        let owner = prepared(&fixture).await;
        let hooks = Hooks::default();
        let (waiting, release) = hooks.gate(stage);
        let caller = launch(owner, hooks.clone());
        reached(waiting).await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        fixture.locked();
        assert!(!hooks.seen(Stage::SupervisorDone));
        release.send(()).unwrap();
        finished(&fixture, &hooks).await;
        assert!(!hooks.seen(Stage::Accepted));
        if matches!(stage, Stage::BeforePreflight | Stage::BeforeRaft) {
            assert!(!hooks.seen(Stage::RaftStored));
        } else {
            assert!(hooks.seen(Stage::CoreJoined));
        }
    }
}

#[nativelink_test]
async fn lifecycle_cancel_during_actual_raft_initial_storage_read() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    owner.journal.enable_measurements();
    let measurements = owner.journal.measurements.clone();
    let queue = owner.journal.queue.clone();
    let hooks = Hooks::default();
    let (waiting, release) = hooks.gate(Stage::BeforeRaft);
    let caller = launch(owner, hooks.clone());
    reached(waiting).await;
    let held = queue.lock_owned().await;
    let counter = &measurements.operations[MeasuredOperation::ReadVote as usize];
    let prior = counter.calls.load(Ordering::Relaxed);
    release.send(()).unwrap();
    tokio::time::timeout(WAIT, async {
        while counter.calls.load(Ordering::Relaxed) == prior {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual Raft initial read did not enter Journal.io");
    assert!(!hooks.seen(Stage::RaftStored));
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    fixture.locked();
    assert!(!hooks.seen(Stage::SupervisorDone));
    drop(held);
    finished(&fixture, &hooks).await;
    // Startup was allowed to finish far enough to store and stop its real Raft.
    assert!(hooks.seen(Stage::RaftStored));
    assert!(hooks.seen(Stage::CoreJoined));
    assert!(!hooks.seen(Stage::Accepted));
}

#[nativelink_test]
async fn lifecycle_abandoned_queued_offer_is_not_acceptance() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    let (waiting, release) = hooks.gate(Stage::ConsumerWaiting);
    let caller = launch(owner, hooks.clone());
    reached(waiting).await;
    hooks.wait(Stage::Offered).await;
    assert!(!hooks.seen(Stage::ConsumerOffered));
    assert!(!hooks.seen(Stage::Accepted));
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(release);
    finished(&fixture, &hooks).await;
    assert!(!hooks.seen(Stage::Accepted));
}

#[nativelink_test]
async fn lifecycle_received_offer_cancelled_before_acceptance_is_drained() {
    for stage in [Stage::ConsumerOffered, Stage::ConsumerArmed] {
        let fixture = Fixture::new();
        let owner = prepared(&fixture).await;
        let hooks = Hooks::default();
        let (waiting, release) = hooks.gate(stage);
        let caller = launch(owner, hooks.clone());
        reached(waiting).await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(release);
        finished(&fixture, &hooks).await;
        assert!(!hooks.seen(Stage::Accepted));
    }
}

#[nativelink_test]
async fn lifecycle_failed_acceptance_never_returns_an_owner() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    hooks.0.reject_acceptance.store(true, Ordering::Relaxed);
    assert_eq!(
        started(launch(owner, hooks.clone())).await.unwrap_err(),
        RecoveryError::Handoff
    );
    finished(&fixture, &hooks).await;
    assert!(hooks.seen(Stage::ConsumerArmed));
    assert!(!hooks.seen(Stage::ConsumerAccepted));
    assert!(!hooks.seen(Stage::Accepted));
}

#[nativelink_test]
async fn lifecycle_panic_after_acceptance_drops_the_armed_consumer_owner() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    hooks.fault(Stage::ConsumerAccepted, Fault::Panic);
    let (waiting, release) = hooks.final_drop();
    let caller = launch(owner, hooks.clone());
    let error = tokio::time::timeout(WAIT, caller)
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.is_panic());
    assert!(hooks.seen(Stage::ConsumerArmed));
    assert!(hooks.seen(Stage::ConsumerAccepted));
    reached(waiting).await;
    fixture.locked();
    assert!(!hooks.seen(Stage::SupervisorDone));
    release.send(()).unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_accepted_handle_drop_drains_to_blocking_destruction() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    let (waiting, release) = hooks.final_drop();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    hooks.wait(Stage::Accepted).await;
    drop(active);
    reached(waiting).await;
    assert!(hooks.seen(Stage::CoreJoined));
    fixture.locked();
    assert!(!hooks.seen(Stage::SupervisorDone));
    release.send(()).unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_cancelled_shutdown_waiter_does_not_abort_supervisor() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    let (waiting, release) = hooks.final_drop();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    let mut stopping = background_spawn!("qualify_cancel_stop", active.shutdown());
    reached(waiting).await;
    pending(&mut stopping).await;
    stopping.abort();
    assert!(stopping.await.unwrap_err().is_cancelled());
    fixture.locked();
    assert!(!hooks.seen(Stage::SupervisorDone));
    release.send(()).unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_startup_errors_and_panics_wait_for_real_final_drop() {
    for stage in [
        Stage::SupervisorStarted,
        Stage::BeforeRaft,
        Stage::RaftStored,
        Stage::Initialized,
        Stage::BeforeOffer,
    ] {
        for fault in [Fault::Error, Fault::Panic] {
            let fixture = Fixture::new();
            let owner = prepared(&fixture).await;
            let hooks = Hooks::default();
            hooks.fault(stage, fault);
            let (waiting, release) = hooks.final_drop();
            let mut caller = launch(owner, hooks.clone());
            reached(waiting).await;
            pending(&mut caller).await;
            fixture.locked();
            assert!(!hooks.seen(Stage::ConsumerAccepted));
            release.send(()).unwrap();
            let expected = if fault == Fault::Panic {
                RecoveryError::Panic
            } else {
                RecoveryError::Runtime
            };
            assert_eq!(started(caller).await.unwrap_err(), expected);
            finished(&fixture, &hooks).await;
            if hooks.seen(Stage::RaftStored) {
                assert!(hooks.seen(Stage::CoreJoined));
            }
        }
    }
}

#[nativelink_test]
async fn lifecycle_supervisor_panic_after_acceptance_is_drained_and_reported() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    hooks.fault(Stage::Accepted, Fault::Panic);
    let (accepted, proceed) = hooks.gate(Stage::Accepted);
    let (waiting, release) = hooks.final_drop();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    reached(accepted).await;
    proceed.send(()).unwrap();
    reached(waiting).await;
    fixture.locked();
    let mut stopping = background_spawn!("qualify_panicked_supervisor", active.shutdown());
    pending(&mut stopping).await;
    release.send(()).unwrap();
    assert_eq!(stopped(stopping).await, Err(RecoveryError::Panic));
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_actual_core_panic_cannot_be_reported_as_normal_shutdown() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    let (sender, probe) = oneshot::channel();
    *hooks.0.raft_probe.lock().unwrap() = Some(sender);
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    hooks.wait(Stage::Accepted).await;
    let raft = tokio::time::timeout(WAIT, probe).await.unwrap().unwrap();
    let fatal = tokio::time::timeout(
        WAIT,
        raft.with_raft_state::<_, ()>(|_| {
            panic!("controlled real core panic after lifecycle acceptance");
        }),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(fatal, openraft::error::Fatal::Panicked));
    drop(raft);
    let stopping = background_spawn!("qualify_abnormal_core_stop", active.shutdown());
    assert_eq!(stopped(stopping).await, Err(RecoveryError::Runtime));
    finished(&fixture, &hooks).await;
    assert!(hooks.seen(Stage::CoreJoined));
}

#[nativelink_test]
async fn lifecycle_raft_new_storage_failure_has_no_returned_runtime_handle() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let database = owner.journal.database.clone();
    let hooks = Hooks::default();
    let (waiting, release) = hooks.gate(Stage::BeforeRaft);
    let caller = launch(owner, hooks.clone());
    reached(waiting).await;
    // Private corruption after the full preflight forces a real Raft::new
    // storage error. Its early tick is stop-requested on Drop, not joined here.
    edit(&database, |table| {
        table.insert("vote", &b"invalid-private-vote"[..]).unwrap();
    });
    drop(database);
    release.send(()).unwrap();
    assert_eq!(started(caller).await.unwrap_err(), RecoveryError::Runtime);
    finished(&fixture, &hooks).await;
    assert!(!hooks.seen(Stage::RaftStored));
    assert!(!hooks.seen(Stage::CoreJoined));
    assert!(
        stored(&fixture)
            .0
            .iter()
            .any(|(key, value)| key == "vote" && value == b"invalid-private-vote")
    );
}

#[nativelink_test]
async fn lifecycle_cleanup_waits_for_private_backend_alias_after_core_join() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let alias = owner.journal.database.clone();
    let hooks = Hooks::default();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    let mut stopping = background_spawn!("qualify_backend_alias_stop", active.shutdown());
    hooks.wait(Stage::BeforeDrain).await;
    assert!(hooks.seen(Stage::CoreJoined));
    pending(&mut stopping).await;
    fixture.locked();
    drop(alias);
    stopped(stopping).await.unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_cleanup_waits_for_cancelled_actual_blocking_io() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let journal = owner.journal.clone();
    let hooks = Hooks::default();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    let (observed, waiting) = oneshot::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let writer = background_spawn!("qualify_lifecycle_cancelled_io", async move {
        journal
            .io(MeasuredOperation::ReadApplied, 0, move |_| {
                observed.send(()).unwrap();
                resume.recv_timeout(WAIT).unwrap();
                Ok(())
            })
            .await
    });
    reached(waiting).await;
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    let mut stopping = background_spawn!("qualify_lifecycle_io_stop", active.shutdown());
    hooks.wait(Stage::BeforeDrain).await;
    assert!(hooks.seen(Stage::CoreJoined));
    pending(&mut stopping).await;
    fixture.locked();
    release.send(()).unwrap();
    stopped(stopping).await.unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_phase_refusal_waiter_cancellation_keeps_final_drop_custody() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let hooks = Hooks::default();
    let (waiting, release) = hooks.final_drop();
    let mut caller = launch(owner, hooks.clone());
    reached(waiting).await;
    pending(&mut caller).await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    fixture.locked();
    assert!(!hooks.seen(Stage::RaftStored));
    release.send(()).unwrap();
    finished(&fixture, &hooks).await;
}

#[nativelink_test]
async fn lifecycle_success_releases_only_its_own_namespace_alias() {
    let fixture = Fixture::new();
    sealed(&fixture).await;
    let external = fixture.owned(false);
    let owner = PreparedCanonicalOwner::reopen_sealed(external.clone().resynchronize().unwrap())
        .await
        .unwrap();
    let hooks = Hooks::default();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    stopped(background_spawn!(
        "qualify_independent_namespace_alias",
        active.shutdown()
    ))
    .await
    .unwrap();
    hooks.wait(Stage::SupervisorDone).await;
    assert!(hooks.seen(Stage::DatabaseDropped));
    fixture.locked();
    drop(external);
    fixture.released().await;
}

#[nativelink_test]
async fn lifecycle_unpolled_shutdown_future_drops_its_stop_owner() {
    let fixture = Fixture::new();
    let owner = prepared(&fixture).await;
    let hooks = Hooks::default();
    let (waiting, release) = hooks.final_drop();
    let active = started(launch(owner, hooks.clone())).await.unwrap();
    hooks.wait(Stage::Accepted).await;
    let shutdown = active.shutdown();
    drop(shutdown);
    reached(waiting).await;
    fixture.locked();
    assert!(!hooks.seen(Stage::SupervisorDone));
    release.send(()).unwrap();
    finished(&fixture, &hooks).await;
}
