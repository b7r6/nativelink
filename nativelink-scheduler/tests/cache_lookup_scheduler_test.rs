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

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

mod utils {
    pub(crate) mod scheduler_utils;
}

use futures::join;
use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::ActionResult as ProtoActionResult;
use nativelink_scheduler::cache_lookup_scheduler::CacheLookupScheduler;
use nativelink_scheduler::mock_scheduler::MockActionScheduler;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, ActionState, ActionUniqueQualifier, OperationId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, OperationFilter,
};
use nativelink_util::store_trait::{Store, StoreLike};
use pretty_assertions::assert_eq;
use prost::Message;
use tokio::sync::watch;
use tokio::{self};
use tokio_stream::StreamExt;
use utils::scheduler_utils::{TokioWatchActionStateResult, make_base_action_info};

struct TestContext {
    mock_scheduler: Arc<MockActionScheduler>,
    ac_store: Store,
    cache_scheduler: CacheLookupScheduler,
}

fn make_cache_scheduler() -> Result<TestContext, Error> {
    let mock_scheduler = Arc::new(MockActionScheduler::new());
    let ac_store = Store::new(MemoryStore::new(&MemorySpec::default()));
    let cache_scheduler = CacheLookupScheduler::new(ac_store.clone(), mock_scheduler.clone())?;
    Ok(TestContext {
        mock_scheduler,
        ac_store,
        cache_scheduler,
    })
}

#[nativelink_test]
async fn different_invocations_share_completed_cache_results() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::new([42; 32], 123));
    let result = ProtoActionResult::try_from(ActionResult::default())?;
    context
        .ac_store
        .update_oneshot(action_info.digest(), result.encode_to_vec().into())
        .await?;
    for scope in ["invocation-one", "invocation-two"] {
        let mut scoped_action = action_info.clone();
        let ActionUniqueQualifier::Cacheable(key) =
            &mut Arc::make_mut(&mut scoped_action).unique_qualifier
        else {
            panic!("Expected cacheable action");
        };
        key.execution_scope = Some(scope.to_string());
        // No mock scheduler response: a regression that rekeys the cache would
        // delegate to execution and fail this bounded wait.
        let listener = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            context
                .cache_scheduler
                .add_action(OperationId::default(), scoped_action),
        )
        .await
        .expect("Completed cache entry must be reusable across invocations")?;
        let (state, _) = listener.as_state().await?;
        assert_eq!(state.action_digest, action_info.digest());
        assert_eq!(state.stage, ActionStage::CompletedFromCache(result.clone()));
    }
    Ok(())
}

#[nativelink_test]
async fn add_action_handles_skip_cache() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::zero_digest());
    let action_result = ProtoActionResult::try_from(ActionResult::default())?;
    context
        .ac_store
        .update_oneshot(action_info.digest(), action_result.encode_to_vec().into())
        .await?;
    let (_forward_watch_channel_tx, forward_watch_channel_rx) =
        watch::channel(Arc::new(ActionState {
            client_operation_id: OperationId::default(),
            stage: ActionStage::Queued,
            action_digest: action_info.unique_qualifier.digest(),
            last_transition_timestamp: SystemTime::now(),
        }));
    let ActionUniqueQualifier::Cacheable(action_key) = action_info.unique_qualifier.clone() else {
        panic!("This test should be testing when item was cached first");
    };
    let mut skip_cache_action = action_info.as_ref().clone();
    skip_cache_action.unique_qualifier = ActionUniqueQualifier::Uncacheable(action_key);
    let skip_cache_action = Arc::new(skip_cache_action);
    let client_operation_id = OperationId::default();
    let _unused = join!(
        context
            .cache_scheduler
            .add_action(client_operation_id.clone(), skip_cache_action),
        context
            .mock_scheduler
            .expect_add_action(Ok(Box::new(TokioWatchActionStateResult::new(
                client_operation_id,
                action_info,
                forward_watch_channel_rx
            ))))
    );
    Ok(())
}

#[nativelink_test]
async fn find_by_client_operation_id_call_passed() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let client_operation_id = OperationId::default();
    let (actual_result, actual_filter) = join!(
        context.cache_scheduler.filter_operations(OperationFilter {
            client_operation_id: Some(client_operation_id.clone()),
            ..Default::default()
        }),
        context
            .mock_scheduler
            .expect_filter_operations(Ok(Box::pin(futures::stream::empty()))),
    );
    assert_eq!(true, actual_result.unwrap().next().await.is_none());
    assert_eq!(
        OperationFilter {
            client_operation_id: Some(client_operation_id),
            ..Default::default()
        },
        actual_filter
    );
    Ok(())
}

fn queued_result(
    client_operation_id: OperationId,
    action_info: Arc<ActionInfo>,
) -> Box<dyn ActionStateResult> {
    let (_tx, rx) = watch::channel(Arc::new(ActionState {
        client_operation_id: client_operation_id.clone(),
        stage: ActionStage::Queued,
        action_digest: action_info.unique_qualifier.digest(),
        last_transition_timestamp: SystemTime::now(),
    }));
    Box::new(TokioWatchActionStateResult::new(
        client_operation_id,
        action_info,
        rx,
    ))
}

// A leader's cache-check task removes its in-flight entry before forwarding a
// miss to the inner scheduler, and that forward is an await point. A second
// request for the same key arriving in that window inserts a fresh entry. The
// leader's drop guard must not remove that fresh entry when the leader's task
// exits, or the second request's sender is dropped and it fails with
// "tx hung up". The default test runtime is single-threaded, so the
// interleaving below is deterministic.
#[nativelink_test]
async fn leader_drop_guard_does_not_orphan_a_later_same_key_request() -> Result<(), Error> {
    let context = make_cache_scheduler()?;
    let cache_scheduler = Arc::new(context.cache_scheduler);
    let mock_scheduler = context.mock_scheduler;
    // Nothing in the AC, so both requests miss and are forwarded.
    let action_info = make_base_action_info(UNIX_EPOCH, DigestInfo::new([7; 32], 77));

    // Request A becomes the leader, misses, removes its entry and parks in the
    // forward to the mock scheduler, which has not answered yet.
    let op_a = OperationId::default();
    let request_a = tokio::spawn({
        let cache_scheduler = cache_scheduler.clone();
        let (op_a, action_info) = (op_a.clone(), action_info.clone());
        async move {
            cache_scheduler
                .add_action(op_a, action_info)
                .await
                .map(|_| ())
        }
    });
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }

    // Request B for the same key arrives while A is parked: A's entry is gone,
    // so B inserts its own.
    let op_b = OperationId::default();
    let request_b = tokio::spawn({
        let cache_scheduler = cache_scheduler.clone();
        let (op_b, action_info) = (op_b.clone(), action_info.clone());
        async move {
            cache_scheduler
                .add_action(op_b, action_info)
                .await
                .map(|_| ())
        }
    });

    // Answer A's forward. A's task then finishes while B's entry is in the map.
    let (forwarded_a, _) = mock_scheduler
        .expect_add_action(Ok(queued_result(op_a.clone(), action_info.clone())))
        .await;
    assert_eq!(forwarded_a, op_a);

    // B must still be forwarded and answered, not orphaned.
    let serve_b = tokio::spawn({
        let mock_scheduler = mock_scheduler.clone();
        let action_info = action_info.clone();
        async move {
            mock_scheduler
                .expect_add_action(Ok(queued_result(op_b, action_info)))
                .await
        }
    });
    let result_b = tokio::time::timeout(std::time::Duration::from_secs(5), request_b)
        .await
        .expect("request B never completed")
        .expect("request B panicked");
    assert!(
        result_b.is_ok(),
        "a same-key request that arrived while the leader was forwarding was orphaned: {result_b:?}"
    );
    serve_b.await.expect("serving B panicked");
    request_a.await.expect("request A panicked")?;
    Ok(())
}
