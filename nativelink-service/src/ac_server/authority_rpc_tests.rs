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

use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint};

use super::*;

struct Rpc {
    channel: Channel,
    stop: oneshot::Sender<()>,
    task: JoinHandleDropGuard<()>,
    address: String,
}

impl Rpc {
    async fn serve<T: ActionCache>(handler: T, wire_cap: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = spawn!("fixed_ac_admission_rpc", async move {
            tonic::transport::Server::builder()
                .add_service(
                    Server::new(handler)
                        .max_decoding_message_size(wire_cap)
                        .max_encoding_message_size(wire_cap),
                )
                .serve_with_incoming_shutdown(
                    futures::stream::unfold(listener, |listener| async {
                        Some((listener.accept().await.map(|(stream, _)| stream), listener))
                    }),
                    async {
                        let _stopped = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(address.clone())
            .unwrap()
            .timeout(Duration::from_secs(3))
            .connect()
            .await
            .unwrap();
        Self {
            channel,
            stop,
            task,
            address,
        }
    }
    async fn close(self) {
        drop(self.channel);
        let _stopped = self.stop.send(());
        timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

#[nativelink_test]
async fn actual_http2_get_accepts_exact_boundary_and_rejects_oversized_raw_value() {
    let spy = Arc::new(Spy::default());
    let rpc = Rpc::serve(fixed(Store::new(spy.clone())), CAP).await;
    let algorithm = DigestHasherFunc::Sha256;
    let mut client = ActionCacheClient::new(rpc.channel.clone());
    let exact = filled_result(CAP);
    spy.put(0, algorithm, exact.encode_to_vec().into(), Finish::Eof);
    assert_eq!(
        client
            .get_action_result(get(0, algorithm))
            .await
            .unwrap()
            .into_inner(),
        exact
    );
    // Unknown fields would normalize to an empty response before wire encoding.
    spy.put(0, algorithm, unknown_value(CAP + 1), Finish::Eof);
    let error = client
        .get_action_result(get(0, algorithm))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(error.message().len() <= 256);
    drop(client);
    rpc.close().await;
}

#[derive(Clone, Debug, Default)]
struct Upstream {
    reads: Arc<Mutex<Vec<GetActionResultRequest>>>,
    writes: Arc<Mutex<Vec<UpdateActionResultRequest>>>,
    result: Arc<Mutex<ActionResult>>,
}

#[tonic::async_trait]
impl ActionCache for Upstream {
    async fn get_action_result(
        &self,
        request: Request<GetActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        self.reads.lock().unwrap().push(request.into_inner());
        Ok(Response::new(self.result.lock().unwrap().clone()))
    }
    async fn update_action_result(
        &self,
        request: Request<UpdateActionResultRequest>,
    ) -> Result<Response<ActionResult>, Status> {
        self.writes.lock().unwrap().push(request.into_inner());
        Ok(Response::new(self.result.lock().unwrap().clone()))
    }
}

fn grpc(address: &str) -> Store {
    Store::new(Arc::new(
        GrpcStore::new(&GrpcSpec {
            instance_name: "upstream-ac".to_owned(),
            endpoints: vec![GrpcEndpoint {
                address: address.to_owned(),
                tls_config: None,
                concurrency_limit: None,
                connect_timeout_s: 0,
                tcp_keepalive_s: 0,
                http2_keepalive_interval_s: 0,
                http2_keepalive_timeout_s: 0,
            }],
            store_type: StoreType::Ac,
            retry: Retry::default(),
            max_concurrent_requests: 0,
            connections_per_endpoint: 0,
            rpc_timeout_s: 2,
            use_legacy_resource_names: false,
            headers: HashMap::new(),
            forward_headers: vec![],
            experimental_read_batching: None,
            experimental_remote_cache_compression: Some(false),
        })
        .unwrap(),
    ))
}

#[nativelink_test]
async fn grpc_facade_omits_inline_hints_and_applies_fixed_response_limit() {
    let upstream = Upstream::default();
    *upstream.result.lock().unwrap() = ActionResult {
        stdout_digest: Some(key(123)),
        ..ActionResult::default()
    };
    let rpc = Rpc::serve(upstream.clone(), 4 * CAP).await;
    let store = grpc(&rpc.address);
    let server = fixed(store.clone());
    let request = get(147, DigestHasherFunc::Blake3);
    let response = server
        .get_action_result(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.stdout_digest, Some(key(123)));
    let seen = upstream.reads.lock().unwrap()[0].clone();
    assert!(!seen.inline_stdout && !seen.inline_stderr && seen.inline_output_files.is_empty());
    assert_eq!(seen.digest_function, request.digest_function);
    assert_eq!(seen.action_digest, request.action_digest);
    assert_eq!(seen.instance_name, "upstream-ac");
    // Control: the unchanged legacy shortcut forwards the advisory flags.
    legacy(store)
        .get_action_result(Request::new(request.clone()))
        .await
        .unwrap();
    let seen = upstream.reads.lock().unwrap()[1].clone();
    assert!(seen.inline_stdout && seen.inline_stderr);
    assert_eq!(seen.inline_output_files, request.inline_output_files);
    *upstream.result.lock().unwrap() = filled_result(CAP + 1);
    assert_eq!(
        server
            .get_action_result(Request::new(request))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    drop(server);
    rpc.close().await;
}

#[nativelink_test]
async fn grpc_update_refuses_before_upstream_write_and_returns_original_bounded_value() {
    let upstream = Upstream::default();
    *upstream.result.lock().unwrap() = ActionResult {
        exit_code: 99,
        ..ActionResult::default()
    };
    let rpc = Rpc::serve(upstream.clone(), 4 * CAP).await;
    let server = fixed(grpc(&rpc.address));
    let algorithm = DigestHasherFunc::Blake3;
    assert_eq!(
        server
            .update_action_result(Request::new(update(147, algorithm, filled_result(CAP + 1))))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert!(upstream.writes.lock().unwrap().is_empty());
    let accepted = ActionResult {
        exit_code: 45,
        ..ActionResult::default()
    };
    assert_eq!(
        server
            .update_action_result(Request::new(update(147, algorithm, accepted.clone())))
            .await
            .unwrap()
            .into_inner(),
        accepted
    );
    let seen = upstream.writes.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].action_result.as_ref(), Some(&accepted));
    assert_eq!(
        seen[0].digest_function,
        algorithm.proto_digest_func() as i32
    );
    drop(seen);
    drop(server);
    rpc.close().await;
}
