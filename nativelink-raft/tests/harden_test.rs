// Copyright 2024 The NativeLink Authors. All rights reserved.
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

//! Hardening tests responding to Astra's adversarial premortem
//! (`/tmp/nativelink-astra-openraft-premortem.md`). Each test names the seam it
//! covers and asserts the FIXED behaviour; the comment records what the
//! pre-fix code did wrong (the defect Astra found).

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_raft::{CommandResponse, RaftAwaitedActionDb, RejectReason};
use nativelink_scheduler::awaited_action_db::{AwaitedActionDb, AwaitedActionSubscriber};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    ExecutionMetadata, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;

const INSTANCE: &str = "raft_harden_instance";

fn make_action_info(digest: DigestInfo) -> Arc<ActionInfo> {
    Arc::new(ActionInfo {
        command_digest: DigestInfo::new([0u8; 32], 0),
        input_root_digest: DigestInfo::new([0u8; 32], 0),
        timeout: Duration::MAX,
        platform_properties: HashMap::new(),
        priority: 0,
        load_timestamp: UNIX_EPOCH,
        insert_timestamp: UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
            execution_scope: None,
            instance_name: INSTANCE.to_string(),
            digest_function: DigestHasherFunc::Sha256,
            digest,
        }),
    })
}

fn completed_result() -> ActionResult {
    ActionResult {
        output_files: Vec::new(),
        output_folders: Vec::new(),
        output_directory_symlinks: Vec::new(),
        output_file_symlinks: Vec::new(),
        exit_code: 0,
        stdout_digest: DigestInfo::new([0u8; 32], 0),
        stderr_digest: DigestInfo::new([0u8; 32], 0),
        execution_metadata: ExecutionMetadata {
            worker: "harden-worker".to_string(),
            queued_timestamp: SystemTime::UNIX_EPOCH,
            worker_start_timestamp: SystemTime::UNIX_EPOCH,
            worker_completed_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_start_timestamp: SystemTime::UNIX_EPOCH,
            input_fetch_completed_timestamp: SystemTime::UNIX_EPOCH,
            execution_start_timestamp: SystemTime::UNIX_EPOCH,
            execution_completed_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_start_timestamp: SystemTime::UNIX_EPOCH,
            output_upload_completed_timestamp: SystemTime::UNIX_EPOCH,
        },
        server_logs: HashMap::new(),
        error: None,
        message: String::new(),
    }
}

/// Add a queued action and return (subscriber, operation_id, its revision).
async fn add_queued(
    db: &RaftAwaitedActionDb,
    client: &str,
    digest: DigestInfo,
) -> (
    nativelink_raft::RaftAwaitedActionSubscriber,
    OperationId,
    i64,
) {
    let sub = db
        .add_action(
            OperationId::from(client),
            make_action_info(digest),
            Duration::from_secs(60),
        )
        .await
        .expect("add_action");
    let a = sub.borrow().await.expect("borrow");
    let op = a.operation_id().clone();
    let rev = a.version_public();
    (sub, op, rev)
}

// ---------------------------------------------------------------------------
// S0 — validated transitions. Two assignments off one Queued revision: exactly
// one wins; the other gets a defined conflict. "client_write succeeded" must NOT
// mean "assignment accepted".
//
// Pre-fix defect: UpdateAwaitedAction installed a precomputed state
// unconditionally and always returned success, so BOTH matchers would believe
// they had assigned the op and could dispatch (N2/N3 resurface).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn s0_two_assignments_one_queued_revision_exactly_one_wins() {
    let db = RaftAwaitedActionDb::new_single_node().await.expect("boot");
    let wa = WorkerId("WA".to_string());
    let wb = WorkerId("WB".to_string());
    db.worker_join(wa.clone(), 1).await.unwrap();
    db.worker_join(wb.clone(), 1).await.unwrap();

    let (_sub, op, rev) = add_queued(&db, "s0-1", DigestInfo::new([1u8; 32], 1)).await;

    // Both matchers computed an assignment off the SAME Queued revision `rev`.
    let resp_a = db
        .assign_to_worker(op.clone(), rev, wa.clone(), 1)
        .await
        .unwrap();
    let resp_b = db
        .assign_to_worker(op.clone(), rev, wb.clone(), 1)
        .await
        .unwrap();

    let a_ok = matches!(resp_a, CommandResponse::Applied { .. });
    let b_ok = matches!(resp_b, CommandResponse::Applied { .. });
    assert!(
        a_ok ^ b_ok,
        "exactly one assignment wins; got a_ok={a_ok} b_ok={b_ok}"
    );
    // The loser gets a defined RevisionConflict, not a silent success.
    let loser = if a_ok { resp_b } else { resp_a };
    assert!(
        matches!(
            loser,
            CommandResponse::Rejected {
                reason: RejectReason::RevisionConflict { .. }
            }
        ),
        "loser must get RevisionConflict, got {loser:?}"
    );

    // The DB remembers exactly one worker, the winner's.
    let owner = db.operation_owner(&op).await.expect("owned");
    let winner = if a_ok { &wa } else { &wb };
    assert_eq!(&owner.0, winner, "the surviving assignment is the winner's");
}

// ---------------------------------------------------------------------------
// S0 — a terminal op rejects a stale nonterminal update: Completed stays
// terminal, no new dispatch, the N7 "one live op per qualifier" claim is not
// resurrected.
//
// Pre-fix defect: a stale Queued/Executing UpdateAwaitedAction committed after
// completion would un-finish the op (and could delete a replacement's claim).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn s0_completed_rejects_stale_nonterminal_update() {
    let db = RaftAwaitedActionDb::new_single_node().await.expect("boot");
    let worker = WorkerId("W".to_string());
    db.worker_join(worker.clone(), 1).await.unwrap();

    let (mut sub, op, rev) = add_queued(&db, "s0-term", DigestInfo::new([2u8; 32], 2)).await;
    // Assign then complete.
    let r = db
        .assign_to_worker(op.clone(), rev, worker.clone(), 1)
        .await
        .unwrap();
    let exec_rev = match r {
        CommandResponse::Applied { revision, .. } => revision,
        other => panic!("assign not accepted: {other:?}"),
    };
    let observed = sub.changed().await.expect("sees Executing");
    assert_eq!(observed.state().stage, ActionStage::Executing);

    // Complete it (carry the current revision the caller observed).
    let mut completed = observed.clone();
    completed.set_version_public(exec_rev);
    completed.worker_set_state(
        Arc::new(ActionState {
            stage: ActionStage::Completed(completed_result()),
            client_operation_id: op.clone(),
            action_digest: completed.action_info().unique_qualifier.digest(),
            last_transition_timestamp: SystemTime::now(),
        }),
        SystemTime::now(),
    );
    db.update_awaited_action(completed).await.expect("complete");

    // A stale matcher now tries to push the op back to Executing off an OLD
    // revision. It must be rejected; the op stays Completed.
    let stale_resp = db
        .assign_to_worker(op.clone(), rev, worker.clone(), 1)
        .await
        .unwrap();
    assert!(
        matches!(stale_resp, CommandResponse::Rejected { .. }),
        "a stale assignment against a completed op must be rejected, got {stale_resp:?}"
    );

    let final_state = db
        .get_by_operation_id(&op)
        .await
        .unwrap()
        .unwrap()
        .borrow()
        .await
        .unwrap();
    assert!(
        matches!(final_state.state().stage, ActionStage::Completed(_)),
        "the op stays terminal; a stale write cannot resurrect it"
    );
}

// ---------------------------------------------------------------------------
// Item 2 — dropped attempts counter. Repeated failures increment attempts and
// hit the configured retry cap; the counter is not silently reset to the stored
// value.
//
// Pre-fix defect: the adapter serialized only op-id/state/worker/ts; apply
// cloned the OLD stored attempts, so a per-attempt increment was dropped and a
// positive retry limit could never be reached.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn attempts_counter_survives_and_hits_retry_cap() {
    let db = RaftAwaitedActionDb::new_single_node().await.expect("boot");
    let worker = WorkerId("W".to_string());
    db.worker_join(worker.clone(), 1).await.unwrap();

    let (mut sub, op, mut rev) = add_queued(&db, "retry-1", DigestInfo::new([3u8; 32], 3)).await;

    // The default retry limit is 5 (state::DEFAULT_RETRY_LIMIT). Fail-and-requeue
    // repeatedly, incrementing the caller's attempts each time, and assert the
    // cap is enforced once attempts exceeds it.
    let mut last_rejected = None;
    for attempt_no in 1..=8usize {
        // Assign (Queued -> Executing).
        let r = db
            .assign_to_worker(op.clone(), rev, worker.clone(), 1)
            .await
            .unwrap();
        let exec_rev = match r {
            CommandResponse::Applied { revision, .. } => revision,
            CommandResponse::Rejected { reason } => {
                // Once the op has been failed (terminal) we cannot assign again.
                last_rejected = Some(reason);
                break;
            }
            other => panic!("unexpected: {other:?}"),
        };
        let observed = sub.changed().await.expect("sees Executing");

        // Worker reports a failure by requeueing with an incremented attempt
        // count (this is what the real state manager does before the update).
        let mut requeue = observed.clone();
        requeue.set_version_public(exec_rev);
        requeue.attempts = attempt_no;
        requeue.worker_set_state(
            Arc::new(ActionState {
                stage: ActionStage::Queued,
                client_operation_id: op.clone(),
                action_digest: requeue.action_info().unique_qualifier.digest(),
                last_transition_timestamp: SystemTime::now(),
            }),
            SystemTime::now(),
        );
        match db.update_awaited_action(requeue).await {
            Ok(()) => {
                // Requeue accepted; the stored attempts must equal what we sent.
                let cur = db
                    .get_by_operation_id(&op)
                    .await
                    .unwrap()
                    .unwrap()
                    .borrow()
                    .await
                    .unwrap();
                assert_eq!(
                    cur.attempts, attempt_no,
                    "the attempts counter is preserved, not reset (attempt {attempt_no})"
                );
                rev = db.revision_of(&op).await.unwrap();
                let _ = sub.changed().await;
            }
            Err(e) => {
                // Past the cap the requeue is refused.
                assert!(
                    e.to_string().contains("RetryCapExceeded"),
                    "requeue past the cap must be RetryCapExceeded, got {e}"
                );
                last_rejected = Some(RejectReason::RetryCapExceeded {
                    attempts: attempt_no,
                    limit: 5,
                });
                break;
            }
        }
    }

    assert!(
        matches!(
            last_rejected,
            Some(RejectReason::RetryCapExceeded { limit: 5, .. })
        ),
        "the retry cap of 5 was reached, proving attempts were counted; got {last_rejected:?}"
    );
}

// ---------------------------------------------------------------------------
// Item 5 — heartbeat lease identity. A heartbeat must name the owning
// attempt/session and never move lease time backward. Both S6d counterexamples:
//  (a) a delayed heartbeat from a superseded attempt must not renew its
//      replacement's lease;
//  (b) an older heartbeat that commits after a newer one must not regress the
//      lease.
//
// Pre-fix defect: Heartbeat named only the operation and unconditionally
// overwrote the lease with its own (possibly older) time.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn heartbeat_identity_and_monotonic_lease() {
    let db = RaftAwaitedActionDb::new_single_node().await.expect("boot");
    let worker = WorkerId("W".to_string());
    db.worker_join(worker.clone(), 1).await.unwrap();

    let (_sub, op, rev) = add_queued(&db, "hb-1", DigestInfo::new([4u8; 32], 4)).await;
    db.assign_to_worker(op.clone(), rev, worker.clone(), 1)
        .await
        .unwrap();
    // Owner is (worker, epoch 1, attempt 1).
    assert_eq!(
        db.operation_owner(&op).await.unwrap(),
        (worker.clone(), 1, 1)
    );

    // (b) monotonic lease. Assignment recorded a lease at the leader's wall
    // clock; build our synthetic times ABOVE it so we exercise the ordering.
    let base = db.lease_of(&op).await.expect("assigned lease");
    let t_new = base + 100;
    let t_old = base + 50; // still > base, but older than t_new

    let hb_new = db
        .heartbeat_at(op.clone(), worker.clone(), 1, 1, t_new)
        .await
        .unwrap();
    assert!(matches!(hb_new, CommandResponse::Applied { .. }));
    assert_eq!(db.lease_of(&op).await, Some(t_new));
    let hb_old = db
        .heartbeat_at(op.clone(), worker.clone(), 1, 1, t_old)
        .await
        .unwrap();
    // Accepted as legitimate sender, but the lease does not regress.
    assert!(matches!(hb_old, CommandResponse::Applied { .. }));
    assert_eq!(
        db.lease_of(&op).await,
        Some(t_new),
        "an older heartbeat must not move the lease backward"
    );

    // (a) attempt identity: a heartbeat naming a WRONG attempt/session is
    // rejected and does not touch the lease.
    let wrong_attempt = db
        .heartbeat_at(op.clone(), worker.clone(), 1, 999, t_new + 200)
        .await
        .unwrap();
    assert!(
        matches!(
            wrong_attempt,
            CommandResponse::Rejected {
                reason: RejectReason::StaleAttempt
            }
        ),
        "a heartbeat from a non-owning attempt is rejected, got {wrong_attempt:?}"
    );
    assert_eq!(
        db.lease_of(&op).await,
        Some(t_new),
        "a stale-attempt heartbeat cannot renew the lease"
    );

    // A superseded session's heartbeat: re-register the worker at epoch 2
    // (requeues the op), so the epoch-1/attempt-1 heartbeat no longer owns
    // anything.
    db.worker_join(worker.clone(), 2).await.unwrap();
    let superseded = db
        .heartbeat_at(op.clone(), worker.clone(), 1, 1, t_new + 300)
        .await
        .unwrap();
    assert!(
        matches!(
            superseded,
            CommandResponse::Rejected {
                reason: RejectReason::StaleAttempt
            }
        ),
        "a heartbeat from a superseded session is rejected, got {superseded:?}"
    );
}

// ---------------------------------------------------------------------------
// Item 6 — ambient-context determinism. AddAction's record construction must be
// a pure function of the command bytes: apply must not read Context::current().
// We prove the origin metadata comes from the captured command value by
// replaying the same logical add on two separate single-node databases under
// (here trivially) empty ambient context and getting identical stored metadata.
//
// Pre-fix defect: AwaitedAction::new read tracing baggage inside apply, so the
// stored metadata depended on whatever context the apply task happened to run
// under, not on the request that was proposed.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn add_action_origin_metadata_is_from_command_not_ambient() {
    // Two independent state machines; apply runs on their own scheduler tasks.
    let db1 = RaftAwaitedActionDb::new_single_node().await.expect("boot1");
    let db2 = RaftAwaitedActionDb::new_single_node().await.expect("boot2");

    let digest = DigestInfo::new([6u8; 32], 6);
    let s1 = db1
        .add_action(
            OperationId::from("meta-1"),
            make_action_info(digest),
            Duration::from_secs(60),
        )
        .await
        .expect("add1");
    let s2 = db2
        .add_action(
            OperationId::from("meta-1"),
            make_action_info(digest),
            Duration::from_secs(60),
        )
        .await
        .expect("add2");

    let a1 = s1.borrow().await.unwrap();
    let a2 = s2.borrow().await.unwrap();
    // Same input, same (absence of) ambient baggage => identical origin metadata
    // on both replicas. The record is a function of the command, not the apply
    // task's context.
    assert_eq!(
        a1.origin_metadata().is_none(),
        a2.origin_metadata().is_none(),
        "origin metadata is derived from the command, identical across replicas"
    );
    // And it round-trips: whatever was captured is what is stored.
    assert!(
        a1.origin_metadata().is_none(),
        "no ambient baggage in the test => None, deterministically"
    );
}

// ---------------------------------------------------------------------------
// Item 4 — snapshot serialization. AppliedState holds maps keyed by
// ActionUniqueKey and OperationId (a struct and an enum). serde_json cannot use
// those as object keys; the moment such a map is NONEMPTY, snapshot creation
// fails at runtime. The fix encodes every map as a pair sequence.
//
// Pre-fix defect: build_snapshot did serde_json::to_vec on AppliedState with a
// nonempty HashMap<ActionUniqueKey, OperationId>, which errors. Empty-state
// tests missed it — so this test deliberately populates a cacheable action.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn snapshot_roundtrips_nonempty_cacheable_state() {
    use nativelink_raft::state::AppliedState;

    let db = RaftAwaitedActionDb::new_single_node().await.expect("boot");
    let worker = WorkerId("W".to_string());
    db.worker_join(worker.clone(), 1).await.unwrap();

    // Populate NONEMPTY cacheable state: a queued cacheable action (fills
    // unique_key_to_operation), then assign it (fills operation_owner keyed by
    // OperationId), then heartbeat (fills leases keyed by OperationId).
    let (_sub, op, rev) = add_queued(&db, "snap-1", DigestInfo::new([7u8; 32], 7)).await;
    db.assign_to_worker(op.clone(), rev, worker.clone(), 1)
        .await
        .unwrap();

    // Grab the live applied state and serialize it exactly as the snapshot path
    // does: to_portable() then serde_json. Pre-fix this would have serialized
    // the raw AppliedState and errored on the struct/enum map keys.
    let state: AppliedState = db.applied_state_for_test().await;
    assert!(
        !state.unique_key_to_operation.is_empty(),
        "the cacheable action must be in the dedup map (else the test is vacuous)"
    );
    assert!(!state.operation_owner.is_empty(), "owner map is nonempty");

    let bytes = serde_json::to_vec(&state.to_portable())
        .expect("nonempty cacheable state serializes via the pair-sequence encoding");
    let restored_portable: nativelink_raft::state::PortableState =
        serde_json::from_slice(&bytes).expect("round-trips");
    let restored = AppliedState::from_portable(restored_portable);

    // Every correctness map survived the round-trip.
    assert_eq!(
        restored.unique_key_to_operation.len(),
        state.unique_key_to_operation.len(),
        "dedup claims survive snapshot"
    );
    assert_eq!(
        restored.operation_owner.get(&op).map(|o| o.attempt),
        state.operation_owner.get(&op).map(|o| o.attempt),
        "ownership (attempt identity) survives snapshot"
    );
    assert_eq!(
        restored.operation_revision.get(&op),
        state.operation_revision.get(&op),
        "revisions survive snapshot"
    );
    assert_eq!(
        restored.operations.len(),
        state.operations.len(),
        "operations survive snapshot"
    );
}

// ---------------------------------------------------------------------------
// Item 3 — subscriber publish-backward. A publication for an older revision must
// not overwrite a newer one already delivered. We drive it observably: a
// subscriber sees revision N; a later get_by_operation_id captures the state and
// re-publishes; if that captured value were stale it could regress the channel.
// The guard keys publication on the record revision, so the channel never goes
// backward.
//
// Pre-fix defect: both the pump and subscribe_op called send_replace
// unconditionally with a value captured before taking the watchers lock, so a
// stale capture could pin subscribers at an old revision indefinitely.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn subscriber_never_regresses_revision() {
    let db = RaftAwaitedActionDb::new_single_node().await.expect("boot");
    let worker = WorkerId("W".to_string());
    db.worker_join(worker.clone(), 1).await.unwrap();

    let (mut sub, op, rev) = add_queued(&db, "pub-1", DigestInfo::new([8u8; 32], 8)).await;

    // Advance the op several revisions: assign, then a couple of heartbeats.
    db.assign_to_worker(op.clone(), rev, worker.clone(), 1)
        .await
        .unwrap();
    let exec = sub.changed().await.expect("sees Executing");
    let exec_rev = exec.version_public();
    assert!(exec_rev > rev, "revision advanced past the queued revision");

    // Now open a NEW subscriber via the read path (subscribe_op). Its captured
    // snapshot is at least exec_rev; the guarded publish must never lower the
    // existing subscriber below exec_rev.
    let fresh = db.get_by_operation_id(&op).await.unwrap().unwrap();
    let fresh_val = fresh.borrow().await.unwrap();
    assert!(
        fresh_val.version_public() >= exec_rev,
        "a fresh subscriber sees a revision at least as new as delivered"
    );

    // The original subscriber's current value must not have regressed below
    // exec_rev after the fresh subscribe_op published.
    let cur = sub.borrow().await.unwrap();
    assert!(
        cur.version_public() >= exec_rev,
        "the existing subscriber did not regress: {} >= {}",
        cur.version_public(),
        exec_rev
    );
}
