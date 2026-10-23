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

#[nativelink_test]
async fn committed_operations_are_only_on_the_fixed_client_role() {
    let runtime = Runtime::start().await;
    let algorithm = DigestHasherFunc::Sha256;
    let input_root_digest = upload(
        runtime.client.clone(),
        algorithm,
        Directory::default().encode_to_vec(),
    )
    .await;
    let command_digest = upload(
        runtime.client.clone(),
        algorithm,
        Command {
            arguments: vec!["queued-operation".to_owned()],
            ..Command::default()
        }
        .encode_to_vec(),
    )
    .await;
    let action_digest = upload(
        runtime.client.clone(),
        algorithm,
        Action {
            command_digest: Some(command_digest),
            input_root_digest: Some(input_root_digest),
            ..Action::default()
        }
        .encode_to_vec(),
    )
    .await;
    let mut execution = ExecutionClient::new(runtime.client.clone());
    let mut stream = execution
        .execute(ExecuteRequest {
            instance_name: INSTANCE.to_owned(),
            action_digest: Some(action_digest),
            skip_cache_lookup: true,
            digest_function: algorithm.proto_digest_func().into(),
            ..ExecuteRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    let pending = stream.message().await.unwrap().unwrap();
    assert!(!pending.done);
    let mut worker = OperationsClient::new(runtime.worker.clone());
    assert_eq!(
        worker
            .cancel_operation(CancelOperationRequest {
                name: pending.name.clone()
            })
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    assert_eq!(
        worker
            .get_operation(GetOperationRequest {
                name: pending.name.clone()
            })
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    let mut client = OperationsClient::new(runtime.client.clone());
    assert!(
        !client
            .get_operation(GetOperationRequest {
                name: pending.name.clone()
            })
            .await
            .unwrap()
            .into_inner()
            .done
    );
    client
        .cancel_operation(CancelOperationRequest {
            name: pending.name.clone(),
        })
        .await
        .unwrap();
    let terminal = client
        .wait_operation(WaitOperationRequest {
            name: pending.name.clone(),
            timeout: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(terminal.done);
    let Some(operation::Result::Error(error)) = &terminal.result else {
        panic!("Expected cancellation");
    };
    assert_eq!(error.code, Code::Cancelled as i32);
    assert_eq!(
        client
            .get_operation(GetOperationRequest { name: pending.name })
            .await
            .unwrap()
            .into_inner(),
        terminal
    );
    drop(stream);
    runtime.shutdown().await;
}
