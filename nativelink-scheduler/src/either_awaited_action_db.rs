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

//! One of two `AwaitedActionDb` backends, chosen at run time. The scheduler
//! is generic over its backend; this lets one test body run against either
//! (see [`test_awaited_action_db_factory`]).

use core::ops::Bound;
use core::time::Duration;
use std::sync::Arc;

use futures::{Stream, StreamExt, TryStreamExt};
use nativelink_error::Error;
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_util::action_messages::{ActionInfo, OperationId};
use nativelink_util::instant_wrapper::InstantWrapper;
use nativelink_util::platform_properties::PlatformProperties;
use tokio::sync::Notify;

use crate::awaited_action_db::{
    AwaitedAction, AwaitedActionDb, AwaitedActionSubscriber, SortedAwaitedAction,
    SortedAwaitedActionState,
};
use crate::default_scheduler_factory::memory_awaited_action_db_factory;
use crate::memory_awaited_action_db::MemoryAwaitedActionDb;
use crate::raft_awaited_action_db::{RaftAwaitedActionDb, RaftDbConfig};
use crate::worker_registry::SharedWorkerRegistry;

/// Environment variable that selects the backend
/// [`test_awaited_action_db_factory`] builds: `memory` (default) or `raft`.
pub const TEST_SCHEDULER_BACKEND_ENV: &str = "NATIVELINK_TEST_SCHEDULER_BACKEND";

#[derive(Debug)]
pub enum EitherAwaitedActionDb<A, B> {
    Left(A),
    Right(B),
}

#[derive(Debug)]
pub enum EitherSubscriber<A, B> {
    Left(A),
    Right(B),
}

impl<A: AwaitedActionSubscriber, B: AwaitedActionSubscriber> AwaitedActionSubscriber
    for EitherSubscriber<A, B>
{
    async fn changed(&mut self) -> Result<AwaitedAction, Error> {
        match self {
            Self::Left(a) => a.changed().await,
            Self::Right(b) => b.changed().await,
        }
    }

    async fn borrow(&self) -> Result<AwaitedAction, Error> {
        match self {
            Self::Left(a) => a.borrow().await,
            Self::Right(b) => b.borrow().await,
        }
    }
}

impl<A: MetricsComponent, B: MetricsComponent> MetricsComponent for EitherAwaitedActionDb<A, B> {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        match self {
            Self::Left(a) => a.publish(kind, field_metadata),
            Self::Right(b) => b.publish(kind, field_metadata),
        }
    }
}

fn left<A, B>(
    stream: impl Stream<Item = Result<A, Error>> + Send,
) -> impl Stream<Item = Result<EitherSubscriber<A, B>, Error>> + Send {
    stream.map_ok(EitherSubscriber::Left)
}

fn right<A, B>(
    stream: impl Stream<Item = Result<B, Error>> + Send,
) -> impl Stream<Item = Result<EitherSubscriber<A, B>, Error>> + Send {
    stream.map_ok(EitherSubscriber::Right)
}

impl<A: AwaitedActionDb, B: AwaitedActionDb> AwaitedActionDb for EitherAwaitedActionDb<A, B> {
    type Subscriber = EitherSubscriber<A::Subscriber, B::Subscriber>;

    async fn get_awaited_action_by_id(
        &self,
        client_operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        Ok(match self {
            Self::Left(a) => a
                .get_awaited_action_by_id(client_operation_id)
                .await?
                .map(EitherSubscriber::Left),
            Self::Right(b) => b
                .get_awaited_action_by_id(client_operation_id)
                .await?
                .map(EitherSubscriber::Right),
        })
    }

    async fn get_all_awaited_actions(
        &self,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        Ok(match self {
            Self::Left(a) => left(a.get_all_awaited_actions().await?).left_stream(),
            Self::Right(b) => right(b.get_all_awaited_actions().await?).right_stream(),
        })
    }

    async fn get_by_operation_id(
        &self,
        operation_id: &OperationId,
    ) -> Result<Option<Self::Subscriber>, Error> {
        Ok(match self {
            Self::Left(a) => a
                .get_by_operation_id(operation_id)
                .await?
                .map(EitherSubscriber::Left),
            Self::Right(b) => b
                .get_by_operation_id(operation_id)
                .await?
                .map(EitherSubscriber::Right),
        })
    }

    async fn get_range_of_actions(
        &self,
        state: SortedAwaitedActionState,
        start: Bound<SortedAwaitedAction>,
        end: Bound<SortedAwaitedAction>,
        desc: bool,
    ) -> Result<impl Stream<Item = Result<Self::Subscriber, Error>> + Send, Error> {
        Ok(match self {
            Self::Left(a) => {
                left(a.get_range_of_actions(state, start, end, desc).await?).left_stream()
            }
            Self::Right(b) => {
                right(b.get_range_of_actions(state, start, end, desc).await?).right_stream()
            }
        })
    }

    async fn update_awaited_action(&self, new_awaited_action: AwaitedAction) -> Result<(), Error> {
        match self {
            Self::Left(a) => a.update_awaited_action(new_awaited_action).await,
            Self::Right(b) => b.update_awaited_action(new_awaited_action).await,
        }
    }

    async fn add_action(
        &self,
        client_operation_id: OperationId,
        action_info: Arc<ActionInfo>,
        no_event_action_timeout: Duration,
    ) -> Result<Self::Subscriber, Error> {
        Ok(match self {
            Self::Left(a) => EitherSubscriber::Left(
                a.add_action(client_operation_id, action_info, no_event_action_timeout)
                    .await?,
            ),
            Self::Right(b) => EitherSubscriber::Right(
                b.add_action(client_operation_id, action_info, no_event_action_timeout)
                    .await?,
            ),
        })
    }

    fn set_worker_registry(&mut self, worker_registry: SharedWorkerRegistry) {
        match self {
            Self::Left(a) => a.set_worker_registry(worker_registry),
            Self::Right(b) => b.set_worker_registry(worker_registry),
        }
    }

    async fn exchange_fleet_capabilities(
        &self,
        scheduler_id: &str,
        local: Vec<PlatformProperties>,
        ttl: Duration,
    ) -> Result<Vec<PlatformProperties>, Error> {
        match self {
            Self::Left(a) => {
                a.exchange_fleet_capabilities(scheduler_id, local, ttl)
                    .await
            }
            Self::Right(b) => {
                b.exchange_fleet_capabilities(scheduler_id, local, ttl)
                    .await
            }
        }
    }

    fn shares_state(&self) -> bool {
        match self {
            Self::Left(a) => a.shares_state(),
            Self::Right(b) => b.shares_state(),
        }
    }
}

/// The backend a scheduler test runs against, selected by
/// [`TEST_SCHEDULER_BACKEND_ENV`]. A drop-in for
/// [`memory_awaited_action_db_factory`]: same arguments, and the raft backend
/// gets the same retention. The raft log is kept in memory.
///
/// # Panics
///
/// If the raft backend cannot be built, which only a bug can cause.
pub fn test_awaited_action_db_factory<I, NowFn>(
    configured_retain_completed_for_s: u32,
    task_change_notify: &Arc<Notify>,
    now_fn: NowFn,
) -> EitherAwaitedActionDb<MemoryAwaitedActionDb<I, NowFn>, RaftAwaitedActionDb<I, NowFn>>
where
    I: InstantWrapper,
    NowFn: Fn() -> I + Clone + Send + Sync + Unpin + 'static,
{
    match std::env::var(TEST_SCHEDULER_BACKEND_ENV).as_deref() {
        Ok("raft") => EitherAwaitedActionDb::Right(
            RaftAwaitedActionDb::new(
                RaftDbConfig {
                    retain_client: Duration::from_secs(u64::from(
                        crate::default_scheduler_factory::retain_completed_for_s(
                            configured_retain_completed_for_s,
                        ),
                    )),
                    ..RaftDbConfig::default()
                },
                task_change_notify.clone(),
                now_fn,
            )
            .expect("an in-memory raft db always builds"),
        ),
        _ => EitherAwaitedActionDb::Left(memory_awaited_action_db_factory(
            configured_retain_completed_for_s,
            task_change_notify,
            now_fn,
        )),
    }
}
