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

//! Operations use the same committed observation, cancellation, and delivery
//! paths as Execute/WaitExecution. Deletion remains authority retention policy.

use std::time::Duration;

use futures::StreamExt;
use nativelink_proto::build::bazel::remote::execution::v2::WaitExecutionRequest;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
use nativelink_proto::google::longrunning::operations_server::Operations;
use nativelink_proto::google::longrunning::{
    CancelOperationRequest, DeleteOperationRequest, GetOperationRequest, ListOperationsRequest,
    ListOperationsResponse, Operation, WaitOperationRequest,
};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tonic::{Request, Response, Status};

use super::{AuthorityService, Input};

#[tonic::async_trait]
impl Operations for AuthorityService {
    async fn list_operations(
        &self,
        _request: Request<ListOperationsRequest>,
    ) -> Result<Response<ListOperationsResponse>, Status> {
        Err(Status::unimplemented("Operation listing is not supported"))
    }

    async fn delete_operation(
        &self,
        _request: Request<DeleteOperationRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "Operation deletion follows committed retention",
        ))
    }

    async fn get_operation(
        &self,
        request: Request<GetOperationRequest>,
    ) -> Result<Response<Operation>, Status> {
        let mut stream = self
            .wait_execution(Request::new(WaitExecutionRequest {
                name: request.into_inner().name,
            }))
            .await?
            .into_inner();
        let operation = stream
            .next()
            .await
            .ok_or_else(|| Status::unavailable("Observation ended without a response"))??;
        Ok(Response::new(operation))
    }

    async fn cancel_operation(
        &self,
        request: Request<CancelOperationRequest>,
    ) -> Result<Response<()>, Status> {
        let _permit = self.permit()?;
        let operation = self.parse_name(&request.into_inner().name)?;
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Input::Cancel(operation, reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        response
            .await
            .map_err(|_| Status::unavailable("Cancellation interrupted"))??;
        Ok(Response::new(()))
    }

    async fn wait_operation(
        &self,
        request: Request<WaitOperationRequest>,
    ) -> Result<Response<Operation>, Status> {
        let request = request.into_inner();
        let deadline = request.timeout.map(wait_deadline).transpose()?;
        let initial = async {
            let mut stream = self
                .wait_execution(Request::new(WaitExecutionRequest { name: request.name }))
                .await?
                .into_inner();
            let last = stream
                .next()
                .await
                .ok_or_else(|| Status::unavailable("Observation ended without a response"))??;
            Ok::<_, Status>((stream, last))
        };
        let (mut stream, mut last) = if let Some(deadline) = deadline {
            tokio::time::timeout_at(deadline, initial)
                .await
                .map_err(|_| Status::deadline_exceeded("Initial observation timed out"))??
        } else {
            initial.await?
        };
        while !last.done {
            let next = if let Some(deadline) = deadline {
                match tokio::time::timeout_at(deadline, stream.next()).await {
                    Ok(next) => next,
                    Err(_) => return Ok(Response::new(last)),
                }
            } else {
                stream.next().await
            };
            last =
                next.ok_or_else(|| Status::unavailable("Observation ended before completion"))??;
        }
        Ok(Response::new(last))
    }
}

fn wait_deadline(timeout: prost_types::Duration) -> Result<Instant, Status> {
    // Reject malformed protobuf durations instead of normalizing negative or
    // overflowing values into an unrelated wait. The protobuf limit is 10,000 years.
    if !(0..=315_576_000_000).contains(&timeout.seconds)
        || !(0..1_000_000_000).contains(&timeout.nanos)
    {
        return Err(Status::invalid_argument("Invalid nonnegative wait timeout"));
    }
    let seconds = u64::try_from(timeout.seconds)
        .map_err(|_| Status::invalid_argument("Invalid wait seconds"))?;
    let nanos = u32::try_from(timeout.nanos)
        .map_err(|_| Status::invalid_argument("Invalid wait nanoseconds"))?;
    Instant::now()
        .checked_add(Duration::new(seconds, nanos))
        .ok_or_else(|| Status::invalid_argument("Wait timeout exceeds the clock range"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nativelink_config::stores::MemorySpec;
    use nativelink_macro::nativelink_test;
    use nativelink_scheduler::authority::{ClusterEpoch, Limits, State};
    use nativelink_store::memory_store::MemoryStore;
    use nativelink_util::store_trait::Store;
    use tokio::sync::{Semaphore, mpsc, watch};

    use super::super::{OperationId, QUEUE};
    use super::*;

    #[nativelink_test]
    async fn wait_timeout_includes_the_initial_commit_and_releases_its_permit() {
        let (commands, mut pending) = mpsc::channel(QUEUE);
        let state = State::new(ClusterEpoch(1), Limits::default()).unwrap();
        let (_committed, committed) = watch::channel(Arc::new(state.view()));
        let (_receipts, receipts) = watch::channel(Arc::new(None));
        let (_history, history) = watch::channel(Arc::new(None));
        let service = AuthorityService {
            commands,
            committed,
            receipts,
            history,
            instance: Arc::from("wait-test"),
            principal: Arc::from("test"),
            cas: Store::new(MemoryStore::new(&MemorySpec::default())),
            permits: Arc::new(Semaphore::new(1)),
        };
        // Retain the receiver without driving a commit. The public wait must
        // enforce its duration even before an initial committed reply exists.
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            service.wait_operation(Request::new(WaitOperationRequest {
                name: "wait-test/authority/1/1".to_owned(),
                timeout: Some(prost_types::Duration {
                    seconds: 0,
                    nanos: 10_000_000,
                }),
            })),
        )
        .await
        .expect("The declared wait timeout must bound the stalled commit");
        assert_eq!(response.unwrap_err().code(), tonic::Code::DeadlineExceeded);
        assert_eq!(service.permits.available_permits(), 1);
        let Input::Observe(OperationId(1), reply) = pending.try_recv().unwrap() else {
            panic!("Expected a committed observation request");
        };
        assert!(reply.is_closed());
    }
}
