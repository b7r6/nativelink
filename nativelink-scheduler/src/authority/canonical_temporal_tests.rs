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

//! Synthetic bound storage admission fixtures. No Raft, seal protocol, clock
//! source, service, historic sample provenance or expiry progress is qualified.

use std::collections::{BTreeMap, BTreeSet};

use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Digest, ExecuteRequest,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use openraft::{CommittedLeaderId, Membership, StoredMembership};
use prost::Message;
use prost_types::{Duration as ProtoDuration, Timestamp};

use super::*;
use crate::authority::journal::{
    Batch, Entry, EntryPayload, LOGS, LogId, SavedSnapshot, SchedulerSection, SnapshotMeta,
    TypeConfig,
};
use crate::authority::request::{MAX_REQUEST_BYTES, RequestPayload};
use crate::authority::{
    AttemptId, ExecutionId, IncarnationId, OperationId, Phase, Reply, Request, SessionId, State,
    WorkerId,
};

// Independent numeric oracle from the protobuf duration boundary and strict
// execution expiry: finite max + one representable tick, not only addition.
const FINITE: u64 = 315_576_000_001;
const CUTOFF: u64 = 18_446_743_758_133_551_613;
const BASE: u64 = 17;

type TemporalPause = (
    Box<dyn FnOnce() + Send>,
    tokio::sync::oneshot::Receiver<()>,
    Arc<(Mutex<bool>, Condvar)>,
    tokio::sync::oneshot::Receiver<bool>,
);

pub(super) fn temporal_pause() -> TemporalPause {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let held = gate.clone();
    let (reached, wait) = tokio::sync::oneshot::channel();
    let (finished, done) = tokio::sync::oneshot::channel();
    let hook = Box::new(move || {
        reached.send(()).unwrap();
        let (guard, timeout) = held
            .1
            .wait_timeout_while(
                held.0.lock().unwrap(),
                Duration::from_secs(10),
                |released| !*released,
            )
            .unwrap();
        finished.send(*guard && !timeout.timed_out()).unwrap();
    });
    (hook, wait, gate, done)
}

#[nativelink_test]
async fn temporal_cancelled_owner_waiter_retains_namespace_until_read_finishes() {
    for create in [true, false] {
        let fixture = if create {
            Fixture::new()
        } else {
            seeded(|epoch| empty(epoch, BASE)).await
        };
        let token = fixture.token(create);
        let (pause, reached, gate, done) = temporal_pause();
        let hooks = Hooks {
            during_temporal: Some(pause),
            ..Hooks::default()
        };
        let task = background_spawn!(
            "canonical_temporal_cancel_owner",
            PreparedCanonicalOwner::open(token, create, hooks)
        );
        tokio::time::timeout(Duration::from_secs(5), reached)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        fixture.locked();
        *gate.0.lock().unwrap() = true;
        gate.1.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), done)
                .await
                .unwrap()
                .unwrap()
        );
        fixture.released().await;
    }
}

#[nativelink_test]
async fn temporal_cancelled_audit_retains_owned_journal_queue() {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    // This test alone creates an extra private Journal alias. The public owner
    // exposes no audit or Journal clone; releasing it here does not release the
    // separately owned audit. This is a custody oracle, not a public release
    // acknowledgement failure.
    let journal = owner.journal.clone();
    let queue = journal.queue.clone();
    let domain = journal.domain;
    let contract = journal.canonical_contract.unwrap();
    let (pause, reached, gate, done) = temporal_pause();
    let task = background_spawn!("canonical_temporal_cancel_queue", async move {
        super::super::temporal::audit(
            &journal,
            domain,
            contract,
            CanonicalBootstrap::Preparing,
            Some(pause),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), reached)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        queue.try_lock().is_err(),
        "cancelled audit released Journal.io serialization"
    );
    owner.release().await.unwrap();
    fixture.locked();
    *gate.0.lock().unwrap() = true;
    gate.1.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), done)
            .await
            .unwrap()
            .unwrap()
    );
    fixture.released().await;
    assert!(queue.try_lock().is_ok());
}

#[path = "canonical_recovery_clock_tests.rs"]
mod recovery_clock;

type Stored = (Vec<(String, Vec<u8>)>, Vec<(u64, Vec<u8>)>);

fn digest(bytes: &[u8]) -> Digest {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(bytes);
    hasher.finalize_digest().into()
}

fn request(timeout: Option<ProtoDuration>) -> Request {
    let command = Command {
        arguments: vec!["/bin/true".to_owned()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(&command)),
        input_root_digest: Some(digest(&[])),
        timeout,
        ..Action::default()
    }
    .encode_to_vec();
    Request::from_payload(
        RequestPayload::capture(
            &ExecuteRequest {
                instance_name: "temporal-fixture".to_owned(),
                action_digest: Some(digest(&action)),
                digest_function: 1,
                skip_cache_lookup: true,
                ..ExecuteRequest::default()
            },
            None,
            &Timestamp::default(),
            "test-principal",
            &action,
            &command,
            MAX_REQUEST_BYTES,
        )
        .unwrap(),
    )
    .unwrap()
}

pub(super) fn finite(seconds: i64, nanos: i32) -> Request {
    request(Some(ProtoDuration { seconds, nanos }))
}

fn step(state: &mut State, event: Event) -> Reply {
    let reply = state.apply(RecordedEvent {
        epoch: state.epoch,
        event,
    });
    state.check_invariants().unwrap();
    reply
}

pub(super) fn empty(epoch: ClusterEpoch, now: u64) -> State {
    let mut state = State::new(epoch, LIMITS).unwrap();
    step(&mut state, Event::AdvanceTime { now });
    state
}

fn queued(epoch: ClusterEpoch, now: u64, request: Request) -> State {
    let mut state = empty(epoch, now);
    assert!(matches!(
        step(
            &mut state,
            Event::Submit {
                operation: OperationId(1),
                request,
            },
        ),
        Reply::Submitted { .. }
    ));
    state
}

pub(super) fn attempt(epoch: ClusterEpoch, now: u64, request: Request, phase: Phase) -> State {
    let capacity = request.resources;
    let mut state = queued(epoch, now, request);
    assert_eq!(
        step(
            &mut state,
            Event::Register {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: BTreeMap::new(),
                capacity,
                available: capacity,
            },
        ),
        Reply::Applied
    );
    assert!(matches!(
        step(&mut state, Event::Match),
        Reply::Matched { .. }
    ));
    if phase != Phase::Dispatching {
        assert_eq!(
            step(
                &mut state,
                Event::WorkerAccepted {
                    attempt: AttemptId(1),
                    worker: WorkerId(1),
                    incarnation: IncarnationId(1),
                },
            ),
            Reply::Applied
        );
        if phase != Phase::Accepted {
            assert_eq!(
                step(
                    &mut state,
                    Event::Progress {
                        attempt: AttemptId(1),
                        worker: WorkerId(1),
                        incarnation: IncarnationId(1),
                        phase,
                        progress: 1,
                    },
                ),
                Reply::Applied
            );
        }
    }
    state
}

pub(super) fn with_database<T>(fixture: &Fixture, operation: impl FnOnce(&Database) -> T) -> T {
    let owned = fixture.owned(false);
    let (database, _) = fixture.directory(&owned).reopen().unwrap().into_parts();
    operation(&database)
}

fn stored(fixture: &Fixture) -> Stored {
    with_database(fixture, |database| {
        let tx = database.begin_read().unwrap();
        let metadata = tx
            .open_table(META)
            .unwrap()
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_owned(), value.value().to_vec())
            })
            .collect();
        let logs = tx
            .open_table(LOGS)
            .unwrap()
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value(), value.value().to_vec())
            })
            .collect();
        (metadata, logs)
    })
}

pub(super) async fn seeded(make: impl FnOnce(ClusterEpoch) -> State) -> Fixture {
    let fixture = Fixture::new();
    let owner = PreparedCanonicalOwner::prepare_cold(fixture.token(true))
        .await
        .unwrap();
    let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
    let node = fixture.manifest.authority.node_id;
    let state = make(epoch);
    // Every new temporal negative must still satisfy the old generic snapshot
    // invariants; rejection by those invariants is not the new admission oracle.
    state.check_invariants().unwrap();
    let mut value = applied(&owner.journal.database, epoch);
    let position = LogId::new(CommittedLeaderId::new(1, node), 0);
    let membership = Membership::new(vec![BTreeSet::from([node])], BTreeSet::<u64>::new());
    value.last_applied = Some(position);
    value.membership = StoredMembership::new(Some(position), membership.clone());
    value.scheduler = SchedulerSection::from_state(&state, &value.format).unwrap();
    let tx = owner.journal.database.begin_write().unwrap();
    {
        let mut metadata = tx.open_table(META).unwrap();
        metadata
            .insert("applied", encode(epoch, &value).unwrap().as_slice())
            .unwrap();
        metadata
            .insert(
                "committed",
                encode(epoch, &Some(position)).unwrap().as_slice(),
            )
            .unwrap();
        // Deliberately synthetic test metadata, never a production seal writer.
        metadata
            .insert(
                "canonical_bootstrap",
                encode(epoch, &CanonicalBootstrap::Sealed)
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
        let entry: Entry<TypeConfig> = Entry {
            log_id: position,
            payload: EntryPayload::Membership(membership),
        };
        tx.open_table(LOGS)
            .unwrap()
            .insert(0, encode(epoch, &entry).unwrap().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    owner.release().await.unwrap();
    fixture.released().await;
    fixture
}

async fn admits(fixture: &Fixture) {
    let before = stored(fixture);
    let owner = PreparedCanonicalOwner::reopen_sealed(fixture.token(false))
        .await
        .unwrap();
    assert_eq!(owner.diagnostic().phase, OwnerPhase::Sealed);
    fixture.locked();
    owner.release().await.unwrap();
    assert_eq!(
        stored(fixture),
        before,
        "inert opening changed authority records"
    );
}

async fn refuses(fixture: &Fixture, reason: &str) {
    let before = stored(fixture);
    let result = PreparedCanonicalOwner::reopen_sealed(fixture.token(false)).await;
    let rejected = result.is_err();
    if let Ok(owner) = result {
        owner.release().await.unwrap();
    }
    fixture.released().await;
    assert_eq!(stored(fixture), before, "refusal changed authority records");
    assert!(rejected, "temporal owner admitted {reason}");
}

pub(super) fn suffix(fixture: &Fixture, batches: Vec<Vec<Event>>, committed: bool) {
    with_database(fixture, |database| {
        let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
        let value = applied(database, epoch);
        let tx = database.begin_write().unwrap();
        let mut last = value.last_applied.unwrap();
        {
            let mut logs = tx.open_table(LOGS).unwrap();
            for events in batches {
                last.index += 1;
                let entry: Entry<TypeConfig> = Entry {
                    log_id: last,
                    payload: EntryPayload::Normal(
                        Batch::new(
                            events
                                .into_iter()
                                .map(|event| RecordedEvent { epoch, event }),
                        )
                        .unwrap(),
                    ),
                };
                logs.insert(last.index, encode(epoch, &entry).unwrap().as_slice())
                    .unwrap();
            }
        }
        if committed {
            tx.open_table(META)
                .unwrap()
                .insert("committed", encode(epoch, &Some(last)).unwrap().as_slice())
                .unwrap();
        }
        tx.commit().unwrap();
    });
}

#[nativelink_test]
async fn temporal_cutoff_accepts_exact_headroom_without_mutating_storage() {
    let fixture = seeded(|epoch| empty(epoch, CUTOFF)).await;
    assert_eq!(CUTOFF.checked_add(FINITE + 1), Some(u64::MAX));
    admits(&fixture).await;
}

#[nativelink_test]
async fn temporal_applied_cutoff_plus_one_refuses() {
    let fixture = seeded(|epoch| empty(epoch, CUTOFF + 1)).await;
    refuses(&fixture, "applied tick with no strict-expiry headroom").await;
}

#[nativelink_test]
async fn temporal_snapshot_cutoff_plus_one_refuses_with_safe_applied() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    with_database(&fixture, |database| {
        let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
        let mut value = applied(database, epoch);
        value.scheduler =
            SchedulerSection::from_state(&empty(epoch, CUTOFF + 1), &value.format).unwrap();
        let snapshot = SavedSnapshot {
            meta: SnapshotMeta {
                last_log_id: value.last_applied,
                last_membership: value.membership.clone(),
                snapshot_id: "synthetic-temporal-snapshot".to_owned(),
            },
            applied: value,
        };
        edit(database, |table| {
            table
                .insert("snapshot", encode(epoch, &snapshot).unwrap().as_slice())
                .unwrap();
        });
    });
    refuses(
        &fixture,
        "unsafe retained snapshot beside safe applied state at the same index",
    )
    .await;
}

#[nativelink_test]
async fn temporal_committed_suffix_cutoff_refuses_before_replay() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    suffix(
        &fixture,
        vec![vec![Event::AdvanceTime { now: CUTOFF + 1 }]],
        true,
    );
    refuses(&fixture, "unsafe committed suffix").await;
}

#[nativelink_test]
async fn temporal_uncommitted_suffix_cutoff_refuses_before_replay() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    suffix(
        &fixture,
        vec![vec![Event::AdvanceTime { now: CUTOFF + 1 }]],
        false,
    );
    refuses(&fixture, "unsafe uncommitted suffix").await;
}

#[nativelink_test]
async fn temporal_safe_committed_and_uncommitted_suffixes_remain_unapplied() {
    for committed in [false, true] {
        let fixture = seeded(|epoch| empty(epoch, BASE)).await;
        suffix(
            &fixture,
            vec![vec![Event::AdvanceTime { now: BASE + 1 }]],
            committed,
        );
        admits(&fixture).await;
        with_database(&fixture, |database| {
            let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
            let value = applied(database, epoch);
            let state = value
                .state(
                    epoch,
                    super::super::super::ReaderCapabilities::Base,
                    value.canonical_contract,
                )
                .unwrap();
            assert_eq!(state.now, BASE);
            assert_eq!(value.last_applied.unwrap().index, 0);
        });
    }
}

#[nativelink_test]
async fn temporal_maximum_finite_deadline_is_not_omitted() {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, finite(315_576_000_000, 1), Phase::Running);
        state
            .attempts
            .get_mut(&AttemptId(1))
            .unwrap()
            .running_deadline = Some(u64::MAX);
        state
    })
    .await;
    refuses(&fixture, "finite running deadline saturated to MAX").await;
}

#[nativelink_test]
async fn temporal_finite_maximum_at_cutoff_keeps_a_strict_expiry_tick() {
    let fixture = seeded(|epoch| {
        let state = attempt(epoch, CUTOFF, finite(315_576_000_000, 1), Phase::Running);
        assert_eq!(
            state.attempts[&AttemptId(1)].running_deadline,
            Some(u64::MAX - 1)
        );
        state
    })
    .await;
    admits(&fixture).await;
}

#[nativelink_test]
async fn temporal_omitted_action_timeout_keeps_maximum_sentinel() {
    let fixture = seeded(|epoch| {
        let state = attempt(epoch, CUTOFF, request(None), Phase::Running);
        assert_eq!(
            state.attempts[&AttemptId(1)].running_deadline,
            Some(u64::MAX)
        );
        state
    })
    .await;
    admits(&fixture).await;
}

#[nativelink_test]
async fn temporal_zero_and_fractional_action_projections_are_preserved() {
    for (seconds, nanos, ticks) in [(0, 0, 1), (0, 1, 1), (1, 1, 2)] {
        let captured = finite(seconds, nanos);
        assert_eq!(captured.execution_timeout, ticks);
        let fixture = seeded(|epoch| attempt(epoch, BASE, captured, Phase::Running)).await;
        admits(&fixture).await;
    }
}

#[nativelink_test]
async fn temporal_synthetic_execution_payload_refuses() {
    let fixture = seeded(|epoch| {
        let mut captured = finite(5, 0);
        captured.payload = None;
        queued(epoch, BASE, captured)
    })
    .await;
    refuses(&fixture, "execution request without captured Action").await;
}

#[nativelink_test]
async fn temporal_synthetic_original_operation_payload_refuses() {
    let fixture = seeded(|epoch| {
        let captured = finite(5, 0);
        let mut state = queued(epoch, BASE, captured.clone());
        assert!(matches!(
            step(
                &mut state,
                Event::Submit {
                    operation: OperationId(2),
                    request: captured,
                },
            ),
            Reply::Submitted { .. }
        ));
        // REQUEST1 still has the first handle exactly matching execution.
        // The second compatible handle independently loses captured provenance.
        state
            .operations
            .get_mut(&OperationId(2))
            .unwrap()
            .original_request
            .payload = None;
        state
    })
    .await;
    refuses(
        &fixture,
        "original operation request without captured Action",
    )
    .await;
}

#[nativelink_test]
async fn temporal_synthetic_pending_submit_refuses() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    let mut captured = request(None);
    captured.payload = None;
    suffix(
        &fixture,
        vec![vec![Event::Submit {
            operation: OperationId(1),
            request: captured,
        }]],
        false,
    );
    refuses(&fixture, "uncommitted synthetic Submit with MAX timeout").await;
}

#[nativelink_test]
async fn temporal_invalid_request_cannot_disappear_inside_a_batch() {
    let fixture = seeded(|epoch| empty(epoch, 0)).await;
    let mut captured = finite(5, 0);
    captured.payload = None;
    let events = vec![
        Event::Submit {
            operation: OperationId(1),
            request: captured,
        },
        Event::Cancel {
            operation: OperationId(1),
        },
        Event::AdvanceTime { now: 300 },
        Event::RetireExecution {
            execution: ExecutionId(1),
        },
    ];
    // A final-state-only scan would see no request. The stateless retained
    // Submit guard and intermediate state validation both protect this case;
    // it does not independently isolate either check from the other.
    let mut projected = empty(ClusterEpoch(fixture.manifest.authority.cluster_epoch), 0);
    for event in &events {
        step(&mut projected, event.clone());
    }
    assert!(projected.operations.is_empty());
    assert!(projected.executions.is_empty());
    suffix(&fixture, vec![events], false);
    refuses(
        &fixture,
        "invalid request hidden by later events in its batch",
    )
    .await;
}

async fn bad_attempt_deadline(phase: Phase, at: u64) {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, finite(5, 0), phase);
        state.attempts.get_mut(&AttemptId(1)).unwrap().deadline.at = at;
        state
    })
    .await;
    refuses(
        &fixture,
        "attempt deadline outside its fixed interval range",
    )
    .await;
}

#[nativelink_test]
async fn temporal_dispatching_uses_ack_not_progress_interval() {
    bad_attempt_deadline(Phase::Dispatching, BASE + 30).await;
}

#[nativelink_test]
async fn temporal_accepted_deadline_requires_progress_interval() {
    bad_attempt_deadline(Phase::Accepted, 29).await;
}

#[nativelink_test]
async fn temporal_preparing_deadline_cannot_start_in_the_future() {
    bad_attempt_deadline(Phase::Preparing, BASE + 31).await;
}

#[nativelink_test]
async fn temporal_uploading_deadline_cannot_start_before_zero() {
    bad_attempt_deadline(Phase::Uploading, 0).await;
}

#[nativelink_test]
async fn temporal_running_deadline_cannot_precede_its_finite_duration() {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, finite(5, 0), Phase::Running);
        state
            .attempts
            .get_mut(&AttemptId(1))
            .unwrap()
            .running_deadline = Some(4);
        state
    })
    .await;
    refuses(&fixture, "finite deadline smaller than its timeout").await;
}

#[nativelink_test]
async fn temporal_running_deadline_cannot_anchor_after_current_time() {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, finite(5, 0), Phase::Running);
        state
            .attempts
            .get_mut(&AttemptId(1))
            .unwrap()
            .running_deadline = Some(BASE + 6);
        state
    })
    .await;
    refuses(&fixture, "finite deadline with future derived anchor").await;
}

#[nativelink_test]
async fn temporal_omitted_timeout_cannot_have_finite_running_deadline() {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, request(None), Phase::Running);
        state
            .attempts
            .get_mut(&AttemptId(1))
            .unwrap()
            .running_deadline = Some(BASE + 5);
        state
    })
    .await;
    refuses(&fixture, "omitted timeout with finite running deadline").await;
}

#[nativelink_test]
async fn temporal_interest_deadline_cannot_start_before_zero() {
    let fixture = seeded(|epoch| {
        let mut state = queued(epoch, BASE, finite(5, 0));
        state
            .operations
            .get_mut(&OperationId(1))
            .unwrap()
            .view
            .interest_deadline
            .at = 299;
        state
    })
    .await;
    refuses(&fixture, "interest deadline below its fixed duration").await;
}

#[nativelink_test]
async fn temporal_interest_deadline_cannot_start_in_the_future() {
    let fixture = seeded(|epoch| {
        let mut state = queued(epoch, BASE, finite(5, 0));
        state
            .operations
            .get_mut(&OperationId(1))
            .unwrap()
            .view
            .interest_deadline
            .at = BASE + 301;
        state
    })
    .await;
    refuses(&fixture, "interest deadline beyond now plus fixed duration").await;
}

#[nativelink_test]
async fn temporal_nonzero_cooldown_cannot_start_before_zero() {
    let fixture = seeded(|epoch| {
        let mut state = queued(epoch, BASE, finite(5, 0));
        state
            .executions
            .get_mut(&ExecutionId(1))
            .unwrap()
            .hold_cooldown_until = 299;
        state
    })
    .await;
    refuses(&fixture, "nonzero cooldown below its fixed duration").await;
}

#[nativelink_test]
async fn temporal_uploading_without_running_anchor_is_valid() {
    let fixture = seeded(|epoch| {
        let state = attempt(epoch, BASE, finite(5, 0), Phase::Uploading);
        assert_eq!(state.attempts[&AttemptId(1)].running_deadline, None);
        state
    })
    .await;
    admits(&fixture).await;
}

#[nativelink_test]
async fn temporal_uploading_preserves_an_earlier_running_anchor() {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, finite(5, 0), Phase::Running);
        step(&mut state, Event::AdvanceTime { now: BASE + 3 });
        step(
            &mut state,
            Event::Progress {
                attempt: AttemptId(1),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                phase: Phase::Uploading,
                progress: 2,
            },
        );
        assert_eq!(
            state.attempts[&AttemptId(1)].running_deadline,
            Some(BASE + 5)
        );
        state
    })
    .await;
    admits(&fixture).await;
}

#[nativelink_test]
async fn temporal_revoked_attempt_keeps_old_deadlines_and_unreleased_charge() {
    let fixture = seeded(|epoch| {
        let mut state = attempt(epoch, BASE, finite(5, 0), Phase::Running);
        step(
            &mut state,
            Event::Cancel {
                operation: OperationId(1),
            },
        );
        step(&mut state, Event::AdvanceTime { now: BASE + 1000 });
        assert!(!state.attempts[&AttemptId(1)].authorized);
        assert!(
            state
                .reservations
                .values()
                .any(|reservation| !reservation.released)
        );
        state
    })
    .await;
    admits(&fixture).await;
}

#[nativelink_test]
async fn temporal_generic_unbound_journal_keeps_synthetic_tick_semantics() {
    let fixture = Fixture::new();
    let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
    let node = fixture.manifest.authority.node_id;
    let journal = Journal::open(fixture.file(), node, epoch, LIMITS)
        .await
        .unwrap();
    let mut state = queued(epoch, u64::MAX, finite(5, 0));
    state
        .executions
        .get_mut(&ExecutionId(1))
        .unwrap()
        .request
        .payload = None;
    state
        .operations
        .get_mut(&OperationId(1))
        .unwrap()
        .original_request
        .payload = None;
    let mut value = applied(&journal.database, epoch);
    value.last_applied = Some(LogId::new(CommittedLeaderId::new(1, node), 0));
    value.scheduler = SchedulerSection::from_state(&state, &value.format).unwrap();
    edit(&journal.database, |table| {
        table
            .insert("applied", encode(epoch, &value).unwrap().as_slice())
            .unwrap();
    });
    drop(journal);
    let reopened = Journal::open(fixture.file(), node, epoch, LIMITS)
        .await
        .unwrap();
    let (_, restored) = reopened.read_applied().await.unwrap();
    assert_eq!(restored.now, u64::MAX);
    assert!(
        restored.executions[&ExecutionId(1)]
            .request
            .payload
            .is_none()
    );
}

#[nativelink_test]
async fn temporal_covered_log_cutoff_refuses_with_safe_applied() {
    let fixture = seeded(|epoch| empty(epoch, BASE)).await;
    suffix(
        &fixture,
        vec![vec![Event::AdvanceTime { now: CUTOFF + 1 }]],
        true,
    );
    with_database(&fixture, |database| {
        let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
        let mut value = applied(database, epoch);
        // Deliberately contradictory stored history accepted by generic storage
        // checks: the covered entry is unsafe even though applied time is safe.
        value.last_applied.as_mut().unwrap().index = 1;
        edit(database, |table| {
            table
                .insert("applied", encode(epoch, &value).unwrap().as_slice())
                .unwrap();
        });
    });
    refuses(&fixture, "unsafe retained log already covered by applied").await;
}

#[nativelink_test]
async fn temporal_snapshot_deadline_refuses_with_safe_applied_deadline() {
    let fixture = seeded(|epoch| attempt(epoch, BASE, finite(5, 0), Phase::Uploading)).await;
    with_database(&fixture, |database| {
        let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
        let mut value = applied(database, epoch);
        let mut state = value
            .state(
                epoch,
                super::super::super::ReaderCapabilities::Base,
                value.canonical_contract,
            )
            .unwrap();
        state.attempts.get_mut(&AttemptId(1)).unwrap().deadline.at = 0;
        value.scheduler = SchedulerSection::from_state(&state, &value.format).unwrap();
        let snapshot = SavedSnapshot {
            meta: SnapshotMeta {
                last_log_id: value.last_applied,
                last_membership: value.membership.clone(),
                snapshot_id: "synthetic-temporal-deadline".to_owned(),
            },
            applied: value,
        };
        edit(database, |table| {
            table
                .insert("snapshot", encode(epoch, &snapshot).unwrap().as_slice())
                .unwrap();
        });
    });
    refuses(
        &fixture,
        "unsafe snapshot progress deadline with safe applied",
    )
    .await;
}

#[nativelink_test]
async fn temporal_cooldown_zero_and_elapsed_numeric_anchor_are_valid() {
    for cooldown in [0, BASE + 300] {
        let fixture = seeded(|epoch| {
            let mut state = queued(epoch, BASE, finite(5, 0));
            state
                .executions
                .get_mut(&ExecutionId(1))
                .unwrap()
                .hold_cooldown_until = cooldown;
            step(&mut state, Event::AdvanceTime { now: BASE + 1000 });
            state
        })
        .await;
        // A numeric anchor in range is consistent, not proof that a hold ever
        // existed. Expired cooldown and interest values need not be rewritten.
        admits(&fixture).await;
    }
}

async fn covered_rejected_submit(captured: bool) {
    let fixture = seeded(|epoch| queued(epoch, BASE, finite(5, 0))).await;
    let mut other = finite(6, 0);
    if !captured {
        other.payload = None;
    }
    let event = Event::Submit {
        operation: OperationId(1),
        request: other,
    };
    let epoch = ClusterEpoch(fixture.manifest.authority.cluster_epoch);
    let mut projected = queued(epoch, BASE, finite(5, 0));
    let before = projected.clone();
    assert!(matches!(
        step(&mut projected, event.clone()),
        Reply::Rejected(_)
    ));
    assert_eq!(projected, before);
    suffix(&fixture, vec![vec![event]], true);
    with_database(&fixture, |database| {
        let mut value = applied(database, epoch);
        value.last_applied.as_mut().unwrap().index = 1;
        edit(database, |table| {
            table
                .insert("applied", encode(epoch, &value).unwrap().as_slice())
                .unwrap();
        });
    });
    if captured {
        admits(&fixture).await;
    } else {
        // Canonical admission deliberately requires captured Submit payloads
        // even for covered records whose reducer outcome was rejection.
        refuses(
            &fixture,
            "synthetic covered Submit even though reducer rejects it",
        )
        .await;
    }
}

#[nativelink_test]
async fn temporal_rejected_covered_submit_requires_captured_payload() {
    covered_rejected_submit(false).await;
}

#[nativelink_test]
async fn temporal_rejected_covered_submit_with_captured_payload_is_valid() {
    covered_rejected_submit(true).await;
}
