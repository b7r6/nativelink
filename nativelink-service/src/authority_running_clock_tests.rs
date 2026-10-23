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

//! A real journal with controlled report arrival and a delayed periodic timer.
//! Manual reports qualify coordinator ordering, not child-process launch timing.

use std::path::PathBuf;

use nativelink_config::stores::MemorySpec;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::{AttemptView, ClusterEpoch, Limits};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::fs;
use openraft::storage::RaftLogReader;

use super::*;

const EPOCH: ClusterEpoch = ClusterEpoch(53);

fn session() -> AuthoritySession {
    AuthoritySession {
        worker: Some(AuthorityWorkerIdentity {
            cluster_epoch: EPOCH.0,
            worker_id: 1,
            incarnation_id: 1,
        }),
        session_id: 1,
    }
}

fn report(attempt: AttemptView, phase: AuthorityAttemptPhase) -> AuthorityAttemptReport {
    AuthorityAttemptReport {
        attempt: Some(AuthorityAttemptIdentity {
            worker: session().worker,
            execution_id: attempt.execution.0,
            attempt_id: attempt.id.0,
        }),
        dispatch_effect_id: attempt.dispatch_effect.0,
        observation_sequence: 1,
        phase: phase as i32,
        phase_progress: 1,
        ..AuthorityAttemptReport::default()
    }
}

struct Fixture {
    commands: mpsc::Sender<Input>,
    observations: watch::Receiver<Arc<ObservableState>>,
    clock: Arc<AtomicU64>,
    samples: Arc<AtomicU64>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<Driver>,
    directory: PathBuf,
    _worker: mpsc::Receiver<Result<AuthorityCoordinatorUpdate, Status>>,
}

impl Fixture {
    async fn new(phase: Phase, initial_time: u64) -> Self {
        Self::with_timeout(phase, initial_time, 5).await
    }

    async fn with_timeout(phase: Phase, initial_time: u64, execution_timeout: u64) -> Self {
        let resources = Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            slots: 1,
        };
        let request = AuthorityRequest {
            action: ActionKey {
                instance: "running-clock".into(),
                digest_function: "sha256".into(),
                digest: "a".repeat(64),
                digest_size_bytes: 1,
            },
            execution_scope: None,
            do_not_cache: true,
            skip_cache_lookup: true,
            priority: 0,
            properties: BTreeMap::new(),
            resources,
            execution_timeout,
            payload: None,
        };
        Self::with_request(phase, initial_time, request).await
    }

    async fn with_request(phase: Phase, initial_time: u64, request: AuthorityRequest) -> Self {
        Self::with_request_at(phase, initial_time, request, 0).await
    }

    async fn with_request_at(
        phase: Phase,
        initial_time: u64,
        request: AuthorityRequest,
        admission_time: u64,
    ) -> Self {
        let directory =
            std::env::temp_dir().join(format!("authority-running-clock-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).await.unwrap();
        let authority = Singleton::open(
            directory.join("authority.redb"),
            1,
            EPOCH,
            Limits::default(),
        )
        .await
        .unwrap();
        let resources = request.resources;
        if admission_time != 0 {
            assert_eq!(
                authority
                    .write(RecordedEvent {
                        epoch: EPOCH,
                        event: Event::AdvanceTime {
                            now: admission_time
                        },
                    })
                    .await
                    .unwrap()
                    .data,
                Reply::Applied,
            );
        }
        for event in [
            Event::Register {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: request.properties.clone(),
                capacity: resources,
                available: resources,
            },
            Event::Submit {
                operation: OperationId(1),
                request,
            },
            Event::Match,
            // Prevent a replacement after cleanup from hiding the old charge.
            Event::Capacity {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                capacity: resources,
                available: resources,
                draining: true,
            },
        ] {
            let reply = authority
                .write(RecordedEvent {
                    epoch: EPOCH,
                    event,
                })
                .await
                .unwrap()
                .data;
            assert!(matches!(
                reply,
                Reply::Applied | Reply::Submitted { .. } | Reply::Matched { .. }
            ));
        }
        if phase != Phase::Dispatching {
            for event in [
                Event::WorkerAccepted {
                    attempt: AttemptId(1),
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                },
                Event::AdvanceTime { now: initial_time },
                Event::Progress {
                    attempt: AttemptId(1),
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                    phase,
                    progress: 0,
                },
            ] {
                let reply = authority
                    .write(RecordedEvent {
                        epoch: EPOCH,
                        event,
                    })
                    .await
                    .unwrap()
                    .data;
                assert!(matches!(reply, Reply::Applied | Reply::Ignored));
            }
        }
        let restored = authority.read().await.unwrap();
        let state = Arc::new(restored.view());
        let (committed, observations) = watch::channel(state.clone());
        let receipts = Arc::new(restored.receipt_retirement().cloned());
        let (committed_receipts, _) = watch::channel(receipts.clone());
        let history = Arc::new(restored.accepted_history().cloned());
        let (committed_history, _) = watch::channel(history.clone());
        let clock = Arc::new(AtomicU64::new(initial_time));
        let samples = Arc::new(AtomicU64::new(0));
        let observed_clock = clock.clone();
        let counted_samples = samples.clone();
        let faults = Arc::new(QualificationFaults::default());
        faults.hold_dispatches.store(true, Ordering::Relaxed);
        let (worker, worker_output) = mpsc::channel(32);
        let (worker_stop, _) = oneshot::channel();
        let mut driver = Driver {
            authority,
            state,
            watermark: restored.ingress_watermark(),
            committed,
            config: QualificationConfig {
                instance: "running-clock".into(),
                principal: "local-test".into(),
                cas: Store::new(MemoryStore::new(&MemorySpec::default())),
                cas_namespace: [0; 32],
                publication_sink: Store::new(MemoryStore::new(&MemorySpec::default())),
                now: Arc::new(move || {
                    counted_samples.fetch_add(1, Ordering::SeqCst);
                    Ok(observed_clock.load(Ordering::SeqCst))
                }),
                faults,
            },
            workers: BTreeMap::from([(
                WorkerId(1),
                Connection {
                    session: session(),
                    sender: worker,
                    capacity_sequence: 0,
                    _stop: worker_stop,
                },
            )]),
            sent: BTreeMap::new(),
            effects: JoinSet::new(),
            in_flight: BTreeSet::new(),
            retry_at: BTreeMap::new(),
            storage_cursor: EffectId(0),
            restore_deadlines: true,
            receipts_enabled: restored.receipt_retirement_enabled(),
            receipts,
            committed_receipts,
            receipt_issue_due: true,
            receipt_sent: BTreeMap::new(),
            history,
            committed_history,
        };
        let (commands, inputs) = mpsc::channel(32);
        let (stop, stopped) = oneshot::channel();
        let ticker = tokio::time::interval_at(
            Instant::now() + Duration::from_hours(1),
            Duration::from_hours(1),
        );
        let task = background_spawn!("authority_running_clock_test", async move {
            driver.run_loop(inputs, stopped, ticker).await.unwrap();
            driver
        });
        let fixture = Self {
            commands,
            observations,
            clock,
            samples,
            stop,
            task,
            directory,
            _worker: worker_output,
        };
        // Wait until the initial recovery sweep is complete before moving time.
        assert_eq!(fixture.observe().await.now, initial_time);
        fixture
    }

    async fn observe(&self) -> Arc<ObservableState> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Input::Observe(OperationId(1), reply))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.observations.borrow().clone()
    }

    async fn send(&self, connection: AuthoritySession, report: AuthorityAttemptReport) {
        self.commands
            .send(Input::Worker(
                connection,
                Box::new(AuthorityWorkerUpdate {
                    update: Some(authority_worker_update::Update::Report(report)),
                }),
            ))
            .await
            .unwrap();
    }

    async fn sweep(&self, now: u64) -> Arc<ObservableState> {
        self.clock.store(now, Ordering::SeqCst);
        let (reply, response) = oneshot::channel();
        // This existing input provides an ordered clock sweep without a sleep.
        self.commands
            .send(Input::RenewInterest(OperationId(1), reply))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), response)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        self.observe().await
    }

    async fn stop(self) -> (Driver, PathBuf) {
        self.stop.send(()).unwrap();
        let mut driver = tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap();
        driver.effects.abort_all();
        while driver.effects.join_next().await.is_some() {}
        (driver, self.directory)
    }

    async fn close(self) {
        let (mut driver, directory) = self.stop().await;
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

/// Read the actual committed events, so progress revocation cannot be mistaken
/// for execution-duration expiry when their coarse deadlines happen to agree.
async fn recorded_sweep(driver: &mut Driver, now: u64) -> Vec<Event> {
    let after = driver
        .authority
        .journal
        .read_applied()
        .await
        .unwrap()
        .0
        .unwrap()
        .index
        + 1;
    driver.config.now = Arc::new(move || Ok(now));
    driver.time().await.unwrap();
    driver
        .authority
        .journal
        .clone()
        .try_get_log_entries(after..)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|entry| match entry.payload {
            openraft::EntryPayload::Normal(batch) => Some(batch),
            _ => None,
        })
        .flat_map(|batch| batch.events().to_vec())
        .map(|recorded| recorded.event)
        .collect()
}

#[nativelink_test]
async fn quantized_execution_waits_past_boundary_without_releasing_charge() {
    // For a fresh sample at 10.999s and a requested 5s duration, tick15
    // represents only 4.001s since that sample. Tick16 is conservative.
    for duration in [1, 2, 5] {
        let (mut driver, directory) = Fixture::with_timeout(Phase::Running, 10, duration)
            .await
            .stop()
            .await;
        let boundary = 10 + duration;
        let before = driver.state.attempts[0];
        assert_eq!(before.running_deadline, Some(boundary));
        assert!(before.deadline.at > boundary);
        let at_boundary = recorded_sweep(&mut driver, boundary).await;
        assert_eq!(at_boundary, vec![Event::AdvanceTime { now: boundary }]);
        assert!(driver.state.attempts[0].authorized);
        assert!(!driver.state.reservations[0].released);
        let later = recorded_sweep(&mut driver, boundary + 1).await;
        assert_eq!(
            later,
            vec![
                Event::AdvanceTime { now: boundary + 1 },
                Event::ExpireExecution { attempt: before.id },
            ]
        );
        assert!(!driver.state.attempts[0].authorized);
        assert_eq!(
            driver.state.attempts[0].running_deadline,
            before.running_deadline
        );
        assert!(!driver.state.reservations[0].released);
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

#[nativelink_test]
async fn quantized_recovery_keeps_equality_and_expires_strictly_later() {
    for (now, authorized) in [(15, true), (16, false)] {
        let (mut driver, directory) = Fixture::new(Phase::Running, 10).await.stop().await;
        driver
            .authority
            .write(RecordedEvent {
                epoch: EPOCH,
                event: Event::AdvanceTime { now },
            })
            .await
            .unwrap();
        assert!(driver.authority.read().await.unwrap().view().attempts[0].authorized);
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        let reopened = Singleton::open(
            directory.join("authority.redb"),
            1,
            EPOCH,
            Limits::default(),
        )
        .await
        .unwrap();
        // Model an origin reset: no new elapsed observation is supplied. The
        // initial recovery sweep must still apply the selected comparison.
        driver.config.now = Arc::new(|| Ok(0));
        let (service, coordinator) = AuthorityService::start(reopened, driver.config)
            .await
            .unwrap();
        let observed = service.subscribe().borrow().clone();
        assert_eq!(observed.now, now);
        assert_eq!(observed.attempts[0].authorized, authorized);
        assert_eq!(observed.attempts[0].running_deadline, Some(15));
        assert!(!observed.reservations[0].released);
        coordinator.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

#[nativelink_test]
async fn quantized_progress_expiry_is_independent_of_execution_equality() {
    for renew in [false, true] {
        let (mut driver, directory) = Fixture::with_timeout(Phase::Running, 0, 30)
            .await
            .stop()
            .await;
        let initial = driver.state.attempts[0];
        assert_eq!(initial.running_deadline, Some(30));
        assert_eq!(initial.deadline.at, 30);
        if renew {
            driver.commit(Event::AdvanceTime { now: 29 }).await.unwrap();
            driver
                .commit(Event::Progress {
                    attempt: initial.id,
                    worker: initial.worker,
                    incarnation: initial.incarnation,
                    phase: Phase::Running,
                    progress: 1,
                })
                .await
                .unwrap();
            assert_eq!(driver.state.attempts[0].deadline.at, 59);
            assert_eq!(driver.state.attempts[0].running_deadline, Some(30));
        }
        let events = recorded_sweep(&mut driver, 30).await;
        let expected = if renew {
            vec![Event::AdvanceTime { now: 30 }]
        } else {
            vec![
                Event::AdvanceTime { now: 30 },
                Event::ExpireAttempt {
                    attempt: initial.id,
                    generation: initial.deadline.generation,
                },
            ]
        };
        assert_eq!(events, expected);
        assert_eq!(driver.state.attempts[0].authorized, renew);
        assert!(!driver.state.reservations[0].released);
        if renew {
            assert_eq!(
                recorded_sweep(&mut driver, 31).await,
                vec![
                    Event::AdvanceTime { now: 31 },
                    Event::ExpireExecution {
                        attempt: initial.id
                    },
                ]
            );
        }
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

#[nativelink_test]
async fn first_running_clock_proposals_follow_actual_clock_advance() {
    for now in [0, 10] {
        let fixture = Fixture::new(Phase::Preparing, 0).await;
        fixture.clock.store(now, Ordering::SeqCst);
        let samples = fixture.samples.clone();
        let before_samples = samples.load(Ordering::SeqCst);
        // Stop only the outer loop. Then exercise the same private input path
        // without its independent Match/Observe proposals between log counts.
        let (mut driver, directory) = fixture.stop().await;
        let (before, _) = driver.authority.journal.read_applied().await.unwrap();
        driver
            .input(Input::Worker(
                session(),
                Box::new(AuthorityWorkerUpdate {
                    update: Some(authority_worker_update::Update::Report(report(
                        driver.state.attempts[0],
                        AuthorityAttemptPhase::Running,
                    ))),
                }),
            ))
            .await
            .unwrap();
        let (after, state) = driver.authority.journal.read_applied().await.unwrap();
        let expected_proposals = if now == 0 { 1 } else { 2 };
        assert_eq!(
            after.unwrap().index - before.unwrap().index,
            expected_proposals
        );
        assert_eq!(samples.load(Ordering::SeqCst), before_samples + 1);
        assert_eq!(state.view().now, now);
        assert_eq!(state.view().attempts[0].running_deadline, Some(now + 5));
        eprintln!(
            "RUNNING_CLOCK_PROPOSALS: observed={now} committed_before=0 proposals={expected_proposals} (one Progress, plus AdvanceTime only when advanced)"
        );
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

#[nativelink_test]
async fn clock_failure_refuses_start_before_due_recovery_mutations() {
    let (mut driver, directory) = Fixture::new(Phase::Running, 0).await.stop().await;
    driver
        .authority
        .write(RecordedEvent {
            epoch: EPOCH,
            event: Event::AdvanceTime { now: 50 },
        })
        .await
        .unwrap();
    let before = driver.authority.read().await.unwrap();
    assert!(before.view().attempts[0].authorized);
    assert!(!before.view().reservations[0].released);
    driver.config.now = Arc::new(|| Err("exhausted source".to_owned()));
    let error = AuthorityService::start(driver.authority, driver.config)
        .await
        .unwrap_err();
    assert!(error.contains("Authority clock observation failed: exhausted source"));
    let reopened = Singleton::open(
        directory.join("authority.redb"),
        1,
        EPOCH,
        Limits::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened.read().await.unwrap().snapshot().unwrap(),
        before.snapshot().unwrap()
    );
    reopened.shutdown().await.unwrap();
    fs::remove_dir_all(directory).await.unwrap();
}

#[nativelink_test]
async fn clock_failure_after_start_stops_input_without_releasing_charge() {
    let (mut driver, directory) = Fixture::new(Phase::Running, 0).await.stop().await;
    let before = driver.authority.read().await.unwrap().snapshot().unwrap();
    let samples = Arc::new(AtomicU64::new(0));
    let observed_samples = samples.clone();
    driver.config.now = Arc::new(move || {
        if observed_samples.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(0)
        } else {
            Err("exhausted source".to_owned())
        }
    });
    let (commands, inputs) = mpsc::channel(1);
    let (reply, response) = oneshot::channel();
    commands
        .send(Input::RenewInterest(OperationId(1), reply))
        .await
        .unwrap();
    let (_stop, stopped) = oneshot::channel();
    let error = tokio::time::timeout(Duration::from_secs(10), driver.run(inputs, stopped))
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.contains("Authority clock observation failed: exhausted source"));
    assert_eq!(samples.load(Ordering::SeqCst), 2);
    assert!(response.await.is_err());
    assert!(commands.is_closed());
    let reopened = Singleton::open(
        directory.join("authority.redb"),
        1,
        EPOCH,
        Limits::default(),
    )
    .await
    .unwrap();
    let after = reopened.read().await.unwrap();
    assert_eq!(after.snapshot().unwrap(), before);
    assert!(after.view().attempts[0].authorized);
    assert!(!after.view().reservations[0].released);
    reopened.shutdown().await.unwrap();
    fs::remove_dir_all(directory).await.unwrap();
}

#[nativelink_test]
async fn clock_failure_prevents_first_running_and_acknowledgment() {
    let (mut driver, directory) = Fixture::new(Phase::Dispatching, 0).await.stop().await;
    let before = driver.authority.journal.read_applied().await.unwrap();
    driver.config.now = Arc::new(|| Err("exhausted source".to_owned()));
    let error = driver
        .input(Input::Worker(
            session(),
            Box::new(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(report(
                    driver.state.attempts[0],
                    AuthorityAttemptPhase::Running,
                ))),
            }),
        ))
        .await
        .unwrap_err();
    assert!(error.contains("Authority clock observation failed: exhausted source"));
    let after = driver.authority.journal.read_applied().await.unwrap();
    assert_eq!(after.0, before.0);
    assert_eq!(after.1.snapshot().unwrap(), before.1.snapshot().unwrap());
    driver.workers.clear();
    driver.authority.shutdown().await.unwrap();
    fs::remove_dir_all(directory).await.unwrap();
}

#[nativelink_test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_clock_preparation_retains_custody_until_shutdown() {
    struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            *self.0.0.lock().unwrap() = true;
            self.0.1.notify_all();
        }
    }

    let (mut driver, directory) = Fixture::new(Phase::Running, 0).await.stop().await;
    let before = driver.authority.read().await.unwrap().snapshot().unwrap();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = Release(gate.clone());
    let (entered, received) = oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    driver.config.now = Arc::new(move || {
        let sender = entered.lock().unwrap().take();
        if let Some(sender) = sender {
            let _sent = sender.send(());
        }
        let mut released = gate.0.lock().unwrap();
        while !*released {
            released = gate.1.wait(released).unwrap();
        }
        Ok(0)
    });
    let starting = background_spawn!(
        "cancelled_clock_preparation",
        AuthorityService::start(driver.authority, driver.config)
    );
    tokio::time::timeout(Duration::from_secs(10), received)
        .await
        .unwrap()
        .unwrap();
    starting.abort();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), starting)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    let path = directory.join("authority.redb");
    assert!(
        Singleton::open(&path, 1, EPOCH, Limits::default())
            .await
            .is_err()
    );
    drop(release);
    let reopened = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(authority) = Singleton::open(&path, 1, EPOCH, Limits::default()).await {
                break authority;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.read().await.unwrap().snapshot().unwrap(), before);
    reopened.shutdown().await.unwrap();
    fs::remove_dir_all(directory).await.unwrap();
}

#[nativelink_test]
async fn first_running_samples_after_report_before_derived_deadline() {
    let fixture = Fixture::new(Phase::Preparing, 0).await;
    let before = fixture.observe().await;
    fixture.clock.store(10, Ordering::SeqCst);
    fixture
        .send(
            session(),
            report(before.attempts[0], AuthorityAttemptPhase::Running),
        )
        .await;
    let running = fixture.observe().await;
    let at_eleven = fixture.sweep(11).await;
    eprintln!(
        "RUNNING_CLOCK_WITNESS: report_clock=10 committed_running_now={} derived_deadline={} sweep_now={} authorized={} released={}",
        running.now,
        running.attempts[0]
            .running_deadline
            .map_or_else(|| "none".to_owned(), |at| at.to_string()),
        at_eleven.now,
        at_eleven.attempts[0].authorized,
        at_eleven.reservations[0].released
    );
    fixture.close().await;
    assert_eq!(
        running.now, 10,
        "first Running must sample after the report arrives"
    );
    assert_eq!(running.attempts[0].running_deadline, Some(15));
    assert!(at_eleven.attempts[0].authorized);
    assert!(!at_eleven.reservations[0].released);
}

#[nativelink_test]
async fn first_running_from_dispatch_samples_before_acknowledgment() {
    let fixture = Fixture::new(Phase::Dispatching, 0).await;
    let before = fixture.observe().await;
    fixture.clock.store(9, Ordering::SeqCst);
    fixture
        .send(
            session(),
            report(before.attempts[0], AuthorityAttemptPhase::Running),
        )
        .await;
    let running = fixture.observe().await;
    fixture.close().await;
    assert_eq!(running.now, 9);
    assert_eq!(running.attempts[0].phase, Phase::Running);
    assert_eq!(running.attempts[0].running_deadline, Some(14));
    assert_eq!(running.attempts[0].deadline.at, 39);
    assert_eq!(running.attempts[0].deadline.generation, 3);
    assert!(!running.reservations[0].released);
}

#[nativelink_test]
async fn invalid_stale_and_replayed_reports_do_not_sample_or_reanchor() {
    let fixture = Fixture::new(Phase::Preparing, 0).await;
    let original = fixture.observe().await;
    let valid = report(original.attempts[0], AuthorityAttemptPhase::Running);
    let initial_samples = fixture.samples.load(Ordering::SeqCst);
    fixture.clock.store(10, Ordering::SeqCst);
    let mut stale_session = session();
    stale_session.session_id += 1;
    fixture.send(stale_session, valid.clone()).await;
    for choice in 0..7 {
        let mut invalid = valid.clone();
        match choice {
            0 => invalid.observation_sequence = 0,
            1 => invalid.phase = -1,
            2 => {
                invalid
                    .attempt
                    .as_mut()
                    .unwrap()
                    .worker
                    .as_mut()
                    .unwrap()
                    .incarnation_id += 1;
            }
            3 => invalid.attempt.as_mut().unwrap().attempt_id += 1,
            4 => invalid.dispatch_effect_id += 1,
            5 => invalid.attempt = None,
            6 => invalid.phase = AuthorityAttemptPhase::Unspecified as i32,
            _ => unreachable!(),
        }
        fixture.send(session(), invalid).await;
    }
    let ignored = fixture.observe().await;
    assert_eq!(ignored.now, 0);
    assert_eq!(ignored.attempts, original.attempts);
    assert_eq!(fixture.samples.load(Ordering::SeqCst), initial_samples);
    fixture.send(session(), valid.clone()).await;
    let first = fixture.observe().await;
    assert_eq!(first.attempts[0].running_deadline, Some(15));
    assert_eq!(fixture.samples.load(Ordering::SeqCst), initial_samples + 1);
    fixture.clock.store(12, Ordering::SeqCst);
    fixture.send(session(), valid.clone()).await;
    let duplicate = fixture.observe().await;
    assert_eq!(duplicate.attempts, first.attempts);
    assert_eq!(duplicate.now, first.now);
    assert_eq!(fixture.samples.load(Ordering::SeqCst), initial_samples + 1);
    let mut progress = valid.clone();
    progress.observation_sequence += 1;
    progress.phase_progress += 1;
    fixture.send(session(), progress).await;
    let advanced = fixture.observe().await;
    assert_eq!(advanced.attempts[0].progress, 2);
    assert_eq!(advanced.attempts[0].running_deadline, Some(15));
    assert_eq!(fixture.samples.load(Ordering::SeqCst), initial_samples + 1);
    let boundary = fixture.sweep(15).await;
    assert!(boundary.attempts[0].authorized);
    assert!(!boundary.reservations[0].released);
    let expired = fixture.sweep(16).await;
    assert!(!expired.attempts[0].authorized);
    assert!(!expired.reservations[0].released);
    let sampled = fixture.samples.load(Ordering::SeqCst);
    fixture.clock.store(20, Ordering::SeqCst);
    fixture.send(session(), valid).await;
    let stale = fixture.observe().await;
    assert_eq!(stale.now, expired.now);
    assert_eq!(stale.attempts, expired.attempts);
    assert_eq!(fixture.samples.load(Ordering::SeqCst), sampled);
    fixture
        .send(
            session(),
            report(stale.attempts[0], AuthorityAttemptPhase::Quiescent),
        )
        .await;
    let cleaned = fixture.observe().await;
    assert!(cleaned.reservations[0].released);
    fixture.close().await;
}

#[nativelink_test]
async fn due_dispatch_progress_and_interest_precede_first_running_without_refund() {
    for (phase, start, report_time) in [
        (Phase::Dispatching, 0, 10),
        (Phase::Preparing, 0, 30),
        (Phase::Preparing, 290, 300),
    ] {
        let fixture = Fixture::new(phase, start).await;
        let initial = fixture.observe().await;
        let interest = initial.operations[0].interest_deadline;
        fixture.clock.store(report_time, Ordering::SeqCst);
        fixture
            .send(
                session(),
                report(initial.attempts[0], AuthorityAttemptPhase::Running),
            )
            .await;
        let after = fixture.observe().await;
        fixture.close().await;
        assert_eq!(after.now, report_time);
        assert!(!after.attempts[0].authorized);
        assert_eq!(after.attempts[0].running_deadline, None);
        assert!(!after.reservations[0].released);
        assert_eq!(after.operations[0].interest_deadline, interest);
        if report_time == 300 {
            assert!(!after.operations[0].interested);
        }
    }
}

#[nativelink_test]
async fn backward_phase_running_and_backward_clock_do_not_invent_a_start() {
    let upload = Fixture::new(Phase::Uploading, 0).await;
    let initial = upload.observe().await;
    let sampled = upload.samples.load(Ordering::SeqCst);
    upload.clock.store(10, Ordering::SeqCst);
    upload
        .send(
            session(),
            report(initial.attempts[0], AuthorityAttemptPhase::Running),
        )
        .await;
    let rejected = upload.observe().await;
    assert_eq!(rejected.attempts, initial.attempts);
    assert_eq!(rejected.now, 0);
    assert_eq!(upload.samples.load(Ordering::SeqCst), sampled);
    upload.close().await;

    let backward = Fixture::new(Phase::Preparing, 9).await;
    let initial = backward.observe().await;
    backward.clock.store(3, Ordering::SeqCst);
    backward
        .send(
            session(),
            report(initial.attempts[0], AuthorityAttemptPhase::Running),
        )
        .await;
    let running = backward.observe().await;
    assert_eq!(running.now, 9);
    assert_eq!(running.attempts[0].running_deadline, Some(14));
    backward.close().await;
}

#[path = "authority_timeout_tests.rs"]
mod captured_timeouts;
