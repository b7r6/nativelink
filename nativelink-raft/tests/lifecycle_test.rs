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

use nativelink_raft::RaftAwaitedActionDb;
use nativelink_scheduler::awaited_action_db::{
    AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedActionState,
};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueKey, ActionUniqueQualifier,
    ExecutionMetadata, OperationId,
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

    // Worker claims the action -> Executing (this also records a fresh lease).
    let mut executing = queued.clone();
    executing.worker_set_state(
        Arc::new(ActionState {
            stage: ActionStage::Executing,
            client_operation_id: operation_id.clone(),
            action_digest: action_info.unique_qualifier.digest(),
            last_transition_timestamp: SystemTime::now(),
        }),
        SystemTime::now(),
    );
    db.update_awaited_action(executing)
        .await
        .expect("update to Executing");
    let observed = sub.changed().await.expect("sees Executing");
    assert_eq!(observed.state().stage, ActionStage::Executing);

    // The worker keeps heartbeating: an Expire with a generous lease is a no-op.
    db.heartbeat(operation_id.clone())
        .await
        .expect("heartbeat through the log");
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
