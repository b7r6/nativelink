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

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nativelink_raft::{CommandResponse, RaftAwaitedActionDb};
use nativelink_scheduler::awaited_action_db::{
    AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedActionState,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    ExecutionMetadata, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;

const INSTANCE: &str = "raft_spike_instance";

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
            worker: "raft-worker".to_string(),
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

/// Drive a full AwaitedAction lifecycle THROUGH THE RAFT LOG:
///   add_action (Queued) -> Executing -> Completed
/// asserting a subscriber observes each transition and that the replicated
/// state (the sorted indices and the operation map) matches at each step.
#[tokio::test]
async fn full_lifecycle_through_the_log() {
    let db = RaftAwaitedActionDb::new_single_node()
        .await
        .expect("single-node raft boots");

    let client_op = OperationId::from("client-lifecycle-1");
    let action_info = make_action_info(DigestInfo::new([7u8; 32], 42));

    // --- add_action: appended to the log, applied as Queued ---
    let mut sub = db
        .add_action(
            client_op.clone(),
            action_info.clone(),
            Duration::from_secs(60),
        )
        .await
        .expect("add_action goes through the log");

    let queued = sub.borrow().await.expect("borrow initial");
    assert_eq!(queued.state().stage, ActionStage::Queued, "starts Queued");
    let operation_id = queued.operation_id().clone();

    // Replicated state: exactly one queued entry, no executing/completed.
    {
        let queued_stream = db
            .get_range_of_actions(
                SortedAwaitedActionState::Queued,
                core::ops::Bound::Unbounded,
                core::ops::Bound::Unbounded,
                false,
            )
            .await
            .expect("range query");
        let count = futures::StreamExt::count(queued_stream).await;
        assert_eq!(count, 1, "one action in the Queued index after add");
    }

    // --- update to Executing: appended to the log ---
    let mut executing_action = queued.clone();
    let executing_state = Arc::new(ActionState {
        stage: ActionStage::Executing,
        client_operation_id: operation_id.clone(),
        action_digest: action_info.unique_qualifier.digest(),
        last_transition_timestamp: SystemTime::now(),
    });
    executing_action.worker_set_state(executing_state, SystemTime::now());
    db.update_awaited_action(executing_action)
        .await
        .expect("update to Executing through the log");

    let observed_exec = sub.changed().await.expect("subscriber sees Executing");
    assert_eq!(
        observed_exec.state().stage,
        ActionStage::Executing,
        "subscriber observed the Executing transition via the log"
    );

    // Replicated state moved from queued index to executing index.
    {
        let exec_stream = db
            .get_range_of_actions(
                SortedAwaitedActionState::Executing,
                core::ops::Bound::Unbounded,
                core::ops::Bound::Unbounded,
                false,
            )
            .await
            .expect("range query");
        assert_eq!(
            futures::StreamExt::count(exec_stream).await,
            1,
            "one action in the Executing index after update"
        );
        let queued_stream = db
            .get_range_of_actions(
                SortedAwaitedActionState::Queued,
                core::ops::Bound::Unbounded,
                core::ops::Bound::Unbounded,
                false,
            )
            .await
            .expect("range query");
        assert_eq!(
            futures::StreamExt::count(queued_stream).await,
            0,
            "Queued index empty after the action started executing"
        );
    }

    // --- update to Completed: appended to the log ---
    let mut completed_action = observed_exec.clone();
    let completed_state = Arc::new(ActionState {
        stage: ActionStage::Completed(completed_result()),
        client_operation_id: operation_id.clone(),
        action_digest: action_info.unique_qualifier.digest(),
        last_transition_timestamp: SystemTime::now(),
    });
    completed_action.worker_set_state(completed_state, SystemTime::now());
    db.update_awaited_action(completed_action)
        .await
        .expect("update to Completed through the log");

    let observed_done = sub.changed().await.expect("subscriber sees Completed");
    assert!(
        matches!(observed_done.state().stage, ActionStage::Completed(_)),
        "subscriber observed the Completed transition via the log"
    );

    // Replicated state: one completed, executing index now empty.
    {
        let completed_stream = db
            .get_range_of_actions(
                SortedAwaitedActionState::Completed,
                core::ops::Bound::Unbounded,
                core::ops::Bound::Unbounded,
                false,
            )
            .await
            .expect("range query");
        assert_eq!(
            futures::StreamExt::count(completed_stream).await,
            1,
            "one action in the Completed index at the end"
        );
    }

    // The replicated operation map agrees with what the subscriber saw.
    let by_op = db
        .get_by_operation_id(&operation_id)
        .await
        .expect("lookup by operation id")
        .expect("operation still present");
    let final_state = by_op.borrow().await.expect("final borrow");
    assert!(
        matches!(final_state.state().stage, ActionStage::Completed(_)),
        "replicated state matches the completed lifecycle"
    );
}

/// Stage 3: liveness-as-log. A worker starts an action, then dies (stops
/// heartbeating). The requeue is NOT decided by any node's wall clock but by
/// applying an `Expire` command to the log: the leader compares the
/// log-recorded lease timestamp against the log-recorded `now`. Every replica
/// requeues the same set, deterministically.
#[tokio::test]
async fn worker_death_requeues_through_the_log() {
    let db = RaftAwaitedActionDb::new_single_node()
        .await
        .expect("single-node raft boots");

    let client_op = OperationId::from("client-liveness-1");
    let action_info = make_action_info(DigestInfo::new([9u8; 32], 7));

    let mut sub = db
        .add_action(
            client_op.clone(),
            action_info.clone(),
            Duration::from_secs(60),
        )
        .await
        .expect("add_action");
    let queued = sub.borrow().await.expect("borrow");
    let operation_id = queued.operation_id().clone();

    // A worker session registers and claims the action -> Executing (this also
    // records a fresh lease and sets ownership at attempt 1).
    let worker = WorkerId("liveness-worker".to_string());
    db.worker_join(worker.clone(), 1).await.expect("join");
    drive_to_executing(&db, &queued, &worker, 1).await;
    let observed = sub.changed().await.expect("sees Executing");
    assert_eq!(observed.state().stage, ActionStage::Executing);

    // The worker keeps heartbeating (naming its owning attempt): an Expire with
    // a generous lease is a no-op.
    let hb = db
        .heartbeat(operation_id.clone(), worker.clone(), 1, 1)
        .await
        .expect("heartbeat through the log");
    assert!(
        matches!(hb, CommandResponse::Applied { .. }),
        "a heartbeat from the owning attempt is accepted"
    );
    let requeued = db
        .expire(Duration::from_secs(3600))
        .await
        .expect("expire sweep");
    assert!(
        requeued.is_empty(),
        "a live (recently heartbeat) worker is not requeued"
    );
    assert_eq!(
        db.get_by_operation_id(&operation_id)
            .await
            .unwrap()
            .unwrap()
            .borrow()
            .await
            .unwrap()
            .state()
            .stage,
        ActionStage::Executing,
        "still executing while the lease is fresh"
    );

    // The worker dies. With a zero lease timeout, the log-recorded lease is now
    // older than the deadline, so the deterministic Expire requeues it.
    let requeued = db
        .expire(Duration::from_nanos(0))
        .await
        .expect("expire sweep after worker death");
    assert_eq!(
        requeued,
        vec![operation_id.clone()],
        "the dead worker's action is requeued by a log entry"
    );

    // The subscriber observes the requeue transition (skipping any pending
    // heartbeat notification that left the stage unchanged), and the replicated
    // state moved the action back into the Queued index.
    loop {
        let observed = sub.changed().await.expect("sees a change");
        if observed.state().stage == ActionStage::Queued {
            break;
        }
    }
    let queued_stream = db
        .get_range_of_actions(
            SortedAwaitedActionState::Queued,
            core::ops::Bound::Unbounded,
            core::ops::Bound::Unbounded,
            false,
        )
        .await
        .expect("range query");
    assert_eq!(
        futures::StreamExt::count(queued_stream).await,
        1,
        "the requeued action is back in the Queued index"
    );
}

/// Assign an already-added, queued action to `worker` at `session_epoch`
/// through the validated `AssignToWorker` command (which sets ownership).
/// Asserts the assignment was accepted.
async fn drive_to_executing(
    db: &RaftAwaitedActionDb,
    queued: &nativelink_scheduler::awaited_action_db::AwaitedAction,
    worker: &WorkerId,
    session_epoch: u64,
) {
    let operation_id = queued.operation_id().clone();
    let expected_revision = queued.version_public();
    let resp = db
        .assign_to_worker(
            operation_id,
            expected_revision,
            worker.clone(),
            session_epoch,
        )
        .await
        .expect("assign through the log");
    assert!(
        matches!(resp, CommandResponse::Applied { .. }),
        "assignment must be accepted, got {resp:?}"
    );
}

/// CROSSED-OFF #7 — orphan-free re-registration.
///
/// A worker holds an executing op. A same-id `WorkerJoin` with a NEWER epoch
/// supersedes the old session. In the SAME apply the old session's op is
/// requeued and its ownership cleared, so the ownership map can never point at
/// a session membership has already replaced. This is the N1/N2-family bug
/// (Astra's N-wave) at the worker-session membership layer.
#[tokio::test]
async fn same_id_rejoin_supersedes_without_orphan() {
    let db = RaftAwaitedActionDb::new_single_node()
        .await
        .expect("single-node raft boots");

    let worker = WorkerId("worker-A".to_string());

    // Session epoch 1 registers, then claims an executing action.
    let (applied, requeued) = db
        .worker_join(worker.clone(), 1)
        .await
        .expect("join epoch 1");
    assert!(applied, "first join takes effect");
    assert!(requeued.is_empty(), "nothing to requeue on a fresh join");

    let client_op = OperationId::from("client-orphan-1");
    let action_info = make_action_info(DigestInfo::new([11u8; 32], 1));
    let mut sub = db
        .add_action(
            client_op.clone(),
            action_info.clone(),
            Duration::from_secs(60),
        )
        .await
        .expect("add_action");
    let queued = sub.borrow().await.expect("borrow");
    let operation_id = queued.operation_id().clone();

    drive_to_executing(&db, &queued, &worker, 1).await;
    let observed = sub.changed().await.expect("sees Executing");
    assert_eq!(observed.state().stage, ActionStage::Executing);

    // Ownership now points at session epoch 1, attempt 1.
    assert_eq!(
        db.operation_owner(&operation_id).await,
        Some((worker.clone(), 1, 1)),
        "the executing op is owned by session epoch 1, attempt 1"
    );

    // The SAME worker id re-registers with a new epoch (crash + reconnect).
    // This must supersede session 1 and requeue its op atomically.
    let (applied, requeued) = db
        .worker_join(worker.clone(), 2)
        .await
        .expect("rejoin epoch 2");
    assert!(applied, "the newer-epoch join supersedes");
    assert_eq!(
        requeued,
        vec![operation_id.clone()],
        "the superseded session's op is requeued in the same apply"
    );

    // The ownership map does NOT point at the dead session 1 (no orphan).
    assert_eq!(
        db.operation_owner(&operation_id).await,
        None,
        "ownership was cleared in the same apply that superseded the session — no orphan"
    );
    // The worker-set records only the new session.
    assert_eq!(
        db.worker_sessions_snapshot().await.get(&worker).copied(),
        Some(2),
        "membership records exactly the surviving session"
    );
    // The action is back in the Queued index, ready to be re-dispatched.
    let by_op = db
        .get_by_operation_id(&operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        by_op.borrow().await.unwrap().state().stage,
        ActionStage::Queued,
        "the op is requeued, not stranded on a dead session"
    );

    // A late WorkerLeave from the DEAD session 1 must be ignored: it cannot
    // disturb the surviving session 2.
    let (applied, requeued) = db
        .worker_leave(worker.clone(), 1)
        .await
        .expect("stale leave");
    assert!(!applied, "a leave from the superseded epoch is ignored");
    assert!(requeued.is_empty());
    assert_eq!(
        db.worker_sessions_snapshot().await.get(&worker).copied(),
        Some(2),
        "the surviving session is untouched by the stale leave"
    );
}

/// CROSSED-OFF #5 — the fleet-generation epoch counter has nothing to detect.
///
/// Matching reads a consistent worker-set snapshot from applied state. Across a
/// WorkerJoin and a WorkerLeave, each snapshot is a single point in the totally
/// ordered log: a snapshot taken before a membership change never observes half
/// of it, and one taken after observes all of it. There is no "the worker set
/// changed under me" to detect, so no generation counter is needed.
#[tokio::test]
async fn matching_reads_consistent_worker_set_without_generation_counter() {
    let db = RaftAwaitedActionDb::new_single_node()
        .await
        .expect("single-node raft boots");

    let w1 = WorkerId("worker-1".to_string());
    let w2 = WorkerId("worker-2".to_string());

    // Empty fleet.
    assert!(
        db.worker_sessions_snapshot().await.is_empty(),
        "no workers yet"
    );

    // Two workers join.
    assert!(db.worker_join(w1.clone(), 1).await.unwrap().0);
    assert!(db.worker_join(w2.clone(), 1).await.unwrap().0);

    // A matching pass reads the fleet snapshot. It is internally consistent:
    // both joins are visible or a prior snapshot shows neither/one, but never a
    // torn state. Here, after both commits, both are present.
    let snap_a = db.worker_sessions_snapshot().await;
    assert_eq!(snap_a.get(&w1).copied(), Some(1));
    assert_eq!(snap_a.get(&w2).copied(), Some(1));
    assert_eq!(snap_a.len(), 2, "matching sees exactly the committed fleet");

    // The fleet changes: worker-1 leaves, worker-2 re-registers at a new epoch.
    assert!(db.worker_leave(w1.clone(), 1).await.unwrap().0);
    assert!(db.worker_join(w2.clone(), 2).await.unwrap().0);

    // The earlier snapshot the matching pass held is unaffected — it is an
    // owned value, a frozen point in the log. There is nothing to invalidate,
    // no generation to compare.
    assert_eq!(
        snap_a.get(&w1).copied(),
        Some(1),
        "the snapshot matching already read is a stable point in the log"
    );
    assert_eq!(snap_a.len(), 2);

    // A fresh read reflects the new committed state, wholesale.
    let snap_b = db.worker_sessions_snapshot().await;
    assert_eq!(snap_b.get(&w1).copied(), None, "worker-1 left");
    assert_eq!(snap_b.get(&w2).copied(), Some(2), "worker-2 at new epoch");
    assert_eq!(snap_b.len(), 1);

    // The two snapshots differ, but neither is torn and no code had to detect a
    // race between them: the log order is the only synchronization.
    assert_ne!(
        snap_a.get(&w2).copied(),
        snap_b.get(&w2).copied(),
        "distinct log points give distinct consistent snapshots"
    );
}
