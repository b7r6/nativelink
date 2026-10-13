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

//! `retain_completed_for_s` bounds how long a client operation id is kept
//! after its client stops using it. It must not bound how long an action may
//! run: a client that is still waiting on its operation keeps it, however
//! long the action takes and however short the retention.
//!
//! Found by rechaos restart-chaos against the raft backend with
//! `retain_completed_for_s: 10`: clients waiting on actions that ran longer
//! than 10s got `NOT_FOUND` from `WaitExecution`.

use core::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::poll;
use mock_instant::thread_local::MockClock;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::awaited_action_db::{AwaitedActionDb, AwaitedActionSubscriber};
use nativelink_scheduler::either_awaited_action_db::test_awaited_action_db_factory;
use nativelink_util::action_messages::OperationId;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use tokio::sync::Notify;
use utils::scheduler_utils::make_base_action_info;

mod utils {
    pub(crate) mod scheduler_utils;
}

const NOW_TIME: u64 = 10_000;
const RETAIN_S: u32 = 10;

fn make_system_time(add_time: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(NOW_TIME + add_time)
}

#[nativelink_test]
async fn a_waiting_client_keeps_its_operation_past_the_retention() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let notify = std::sync::Arc::new(Notify::new());
    let db = test_awaited_action_db_factory(RETAIN_S, &notify, MockInstantWrapped::default);

    let client = OperationId::default();
    let mut waiting = db
        .add_action(
            client.clone(),
            make_base_action_info(make_system_time(0), DigestInfo::new([1; 32], 1)),
            Duration::from_mins(1),
        )
        .await?;
    // The first `changed` reports the initial state.
    waiting.changed().await?;

    {
        // The client now waits for its action to finish, as WaitExecution
        // does, while the action runs for four times the retention.
        let changed = waiting.changed();
        tokio::pin!(changed);
        for step in 1..=8u8 {
            MockClock::advance(Duration::from_secs(5));
            for _ in 0..16 {
                if let core::task::Poll::Ready(result) = poll!(&mut changed) {
                    panic!(
                        "the waiting client was woken after {}s with nothing finished: {result:?}",
                        u32::from(step) * 5
                    );
                }
                tokio::task::yield_now().await;
            }
            // Other clients keep submitting, which is what evicts stale
            // client entries.
            drop(
                db.add_action(
                    OperationId::default(),
                    make_base_action_info(
                        make_system_time(u64::from(step) * 5),
                        DigestInfo::new([step + 1; 32], 1),
                    ),
                    Duration::from_mins(1),
                )
                .await?,
            );
        }
    }

    assert!(
        db.get_awaited_action_by_id(&client).await?.is_some(),
        "a client that kept waiting lost its operation after {RETAIN_S}s of retention",
    );
    Ok(())
}

#[nativelink_test]
async fn a_client_that_stopped_waiting_is_forgotten_after_the_retention() -> Result<(), Error> {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let notify = std::sync::Arc::new(Notify::new());
    let db = test_awaited_action_db_factory(RETAIN_S, &notify, MockInstantWrapped::default);

    let client = OperationId::default();
    drop(
        db.add_action(
            client.clone(),
            make_base_action_info(make_system_time(0), DigestInfo::new([1; 32], 1)),
            Duration::from_mins(1),
        )
        .await?,
    );
    // Let the backend see the client go before time moves on.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    MockClock::advance(Duration::from_secs(u64::from(RETAIN_S) + 5));
    // An insert evicts what has expired.
    drop(
        db.add_action(
            OperationId::default(),
            make_base_action_info(make_system_time(20), DigestInfo::new([2; 32], 1)),
            Duration::from_mins(1),
        )
        .await?,
    );
    assert!(
        db.get_awaited_action_by_id(&client).await?.is_none(),
        "a client nobody waits for is kept no longer than the retention",
    );
    Ok(())
}
