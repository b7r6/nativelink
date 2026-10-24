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

use nativelink_proto::google::longrunning::operations_client::OperationsClient;
use nativelink_proto::google::longrunning::{
    CancelOperationRequest, GetOperationRequest, WaitOperationRequest,
};

use super::*;

async fn operations(runtime: &Runtime) -> OperationsClient<Channel> {
    OperationsClient::new(
        Endpoint::from_shared(format!("http://{}", runtime.client_address))
            .unwrap()
            .connect()
            .await
            .unwrap(),
    )
}

async fn get(client: &mut OperationsClient<Channel>, name: &str) -> Operation {
    client
        .get_operation(GetOperationRequest {
            name: name.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
}

async fn cancel(client: &mut OperationsClient<Channel>, name: &str) {
    client
        .cancel_operation(CancelOperationRequest {
            name: name.to_owned(),
        })
        .await
        .unwrap();
}

fn assert_cancelled(operation: &Operation) {
    assert!(operation.done);
    let Some(operation::Result::Error(error)) = &operation.result else {
        panic!("Expected a cancelled operation");
    };
    assert_eq!(error.code, tonic::Code::Cancelled as i32);
}

#[nativelink_test]
async fn queued_operation_cancel_get_wait_and_restart_preserve_each_interest() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock.clone(), faults.clone(), None).await;
    let (request, _, _) = context.request(30).await;
    let mut first = runtime
        .client
        .execute(request.clone())
        .await
        .unwrap()
        .into_inner();
    let a = next_operation(&mut first).await;
    let mut second = runtime.client.execute(request).await.unwrap().into_inner();
    let b = next_operation(&mut second).await;
    assert_ne!(a.name, b.name);
    let state = runtime.wait(|s| s.operations.len() == 2).await;
    assert_eq!(state.executions.len(), 1);
    let mut client = operations(&runtime).await;
    for name in [
        "wrong/authority/1/1",
        "rpc-test/authority/999/1",
        "rpc-test/authority/1/999",
        "rpc-test/authority/1/0",
    ] {
        assert_eq!(
            client
                .get_operation(GetOperationRequest {
                    name: name.to_owned()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            client
                .cancel_operation(CancelOperationRequest {
                    name: name.to_owned()
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::NotFound
        );
    }
    for timeout in [
        prost_types::Duration {
            seconds: -1,
            nanos: 0,
        },
        prost_types::Duration {
            seconds: 0,
            nanos: -1,
        },
        prost_types::Duration {
            seconds: 0,
            nanos: 1_000_000_000,
        },
        prost_types::Duration {
            seconds: 315_576_000_001,
            nanos: 0,
        },
    ] {
        assert_eq!(
            client
                .wait_operation(WaitOperationRequest {
                    name: b.name.clone(),
                    timeout: Some(timeout)
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    // Obtaining the initial observation commits through the real journal.
    // Allow that work in this positive finite-wait case; the short transport
    // deadline below and service unit tests exercise deadline expiry itself.
    let waiting = client
        .wait_operation(WaitOperationRequest {
            name: b.name.clone(),
            timeout: Some(prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!waiting.done);
    let mut short_rpc = Request::new(WaitOperationRequest {
        name: b.name.clone(),
        timeout: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
    });
    short_rpc.set_timeout(Duration::from_millis(20));
    let error = client.wait_operation(short_rpc).await.unwrap_err();
    // Pinned Tonic maps its transport TimeoutExpired to Cancelled. A transport
    // timeout ends this wait; it does not cancel the retained operation.
    assert_eq!(error.code(), tonic::Code::Cancelled);
    assert!(!get(&mut client, &b.name).await.done);
    cancel(&mut client, &a.name).await;
    let cancelled_a = get(&mut client, &a.name).await;
    assert_cancelled(&cancelled_a);
    assert_eq!(terminal(&mut first).await, cancelled_a);
    assert!(!get(&mut client, &b.name).await.done);
    cancel(&mut client, &a.name).await;
    assert_eq!(get(&mut client, &a.name).await, cancelled_a);
    let state = runtime.service.subscribe().borrow().clone();
    assert_eq!(
        state.operations.iter().filter(|op| op.interested).count(),
        1
    );
    assert!(state.executions[0].terminal.is_none());

    // A pending unary wait owns one delivery permit; cancellation wakes it with
    // the exact committed terminal, independently of the original Execute stream.
    let mut waiter = operations(&runtime).await;
    let name = b.name.clone();
    let waiting = background_spawn!("authority_operations_wait", async move {
        waiter
            .wait_operation(WaitOperationRequest {
                name,
                timeout: Some(prost_types::Duration {
                    seconds: 10,
                    nanos: 0,
                }),
            })
            .await
            .unwrap()
            .into_inner()
    });
    cancel(&mut client, &b.name).await;
    let cancelled_b = get(&mut client, &b.name).await;
    assert_eq!(waiting.await.unwrap(), cancelled_b);
    assert_eq!(terminal(&mut second).await, cancelled_b);
    assert_cancelled(&cancelled_b);
    assert_eq!(context.ac.len_for_test(), 0);
    assert_eq!(context.history.len_for_test(), 0);
    runtime.shutdown().await;
    let mut runtime = Runtime::start(&context, clock, faults, None).await;
    let mut client = operations(&runtime).await;
    assert_eq!(get(&mut client, &a.name).await, cancelled_a);
    assert_eq!(get(&mut client, &b.name).await, cancelled_b);
    cancel(&mut client, &a.name).await;
    let mut resumed = runtime
        .client
        .wait_execution(WaitExecutionRequest { name: a.name })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(terminal(&mut resumed).await, cancelled_a);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn cancelling_one_running_caller_preserves_other_completion_and_cache() {
    if !namespaces() {
        return;
    }
    let context = Context::with_manager(
        false,
        Store::new(MemoryStore::new(&MemorySpec::default())),
        Duration::from_secs(10),
        8,
        Duration::from_secs(30),
        ManagerConstruction::AuthorityOnly,
    )
    .await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock.clone(), faults.clone(), None).await;
    let (request, action, _) = context.request(30).await;
    let mut first = runtime
        .client
        .execute(request.clone())
        .await
        .unwrap()
        .into_inner();
    let a = next_operation(&mut first).await;
    let mut second = runtime.client.execute(request).await.unwrap().into_inner();
    let b = next_operation(&mut second).await;
    let (stop, stopped) = watch::channel(false);
    let supervisor = background_spawn!(
        "authority_operations_worker",
        run_reconnecting(
            context.worker.clone(),
            Endpoint::from_shared(format!("http://{}", runtime.worker_address)).unwrap(),
            1,
            Vec::new(),
            stopped,
        )
    );
    let running = runtime
        .wait(|s| {
            s.attempts
                .first()
                .is_some_and(|attempt| attempt.phase == Phase::Running)
        })
        .await;
    assert_eq!(running.executions.len(), 1);
    assert_eq!(running.attempts.len(), 1);
    let charged = running.workers[0].charged;
    let mut client = operations(&runtime).await;
    cancel(&mut client, &a.name).await;
    let cancelled = get(&mut client, &a.name).await;
    assert_cancelled(&cancelled);
    assert_eq!(terminal(&mut first).await, cancelled);
    let still_running = runtime.service.subscribe().borrow().clone();
    assert!(still_running.attempts[0].authorized);
    assert!(!still_running.reservations[0].released);
    assert_eq!(still_running.workers[0].charged, charged);
    assert!(
        !still_running
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
    );
    assert!(!get(&mut client, &b.name).await.done);
    context.release();
    let completed = terminal(&mut second).await;
    let accepted = response(&completed);
    assert_eq!(accepted.result.as_ref().unwrap().exit_code, 0);
    runtime
        .wait(|s| {
            !s.effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        })
        .await;
    let cached = ActionResult::decode(
        context
            .ac
            .get_part_unchunked(action, 0, None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(Some(cached), accepted.result);
    assert_eq!(get(&mut client, &a.name).await, cancelled);
    // A late cancellation is an idempotent success and cannot rewrite acceptance.
    cancel(&mut client, &b.name).await;
    assert_eq!(get(&mut client, &b.name).await, completed);
    context.assert_once();
    assert_eq!(context.history.len_for_test(), 0);
    stop.send_replace(true);
    supervisor.await.unwrap().unwrap();
    runtime.shutdown().await;
    let runtime = Runtime::start(&context, clock, faults, None).await;
    let mut client = operations(&runtime).await;
    assert_eq!(get(&mut client, &a.name).await, cancelled);
    assert_eq!(get(&mut client, &b.name).await, completed);
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_OPERATIONS_PROCESS: one cancelled caller; one accepted caller; one process; preserved reservation until completion; exact terminals recovered"
    );
}
