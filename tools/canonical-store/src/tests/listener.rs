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

//! Actual fixed listener roles with canonical CAS/sink drivers. The journal is
//! a separate test file: this fixture does not qualify canonical journal wiring.

use futures::stream;
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::capabilities_client::CapabilitiesClient;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest, GetActionResultRequest,
    GetCapabilitiesRequest, UpdateActionResultRequest, batch_update_blobs_request, compressor,
};
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{ReadRequest, WriteRequest};
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::{ClusterEpoch, Limits};
use nativelink_service::authority_listener::AuthorityListeners;
use nativelink_service::authority_server::{
    AuthorityService, CoordinatorTask, QualificationConfig, QualificationFaults,
};
use nativelink_util::store_trait::Store;
use nativelink_util::task::JoinHandleDropGuard;
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::Code as RpcCode;
use tonic::transport::{Channel, Endpoint, Server};

use super::*;

#[path = "listener/hash_context.rs"]
mod hash_context;

const INSTANCE: &str = "fixed-canonical";

struct FixedRpc {
    worker: Channel,
    client: Channel,
    service: AuthorityService,
    coordinator: CoordinatorTask,
    stops: [oneshot::Sender<()>; 2],
    tasks: [JoinHandleDropGuard<()>; 2],
}

impl FixedRpc {
    async fn new(fixture: &Fixture, stores: &CanonicalStores) -> Self {
        Self::with_faults(fixture, stores, Arc::new(QualificationFaults::default())).await
    }

    async fn with_faults(
        fixture: &Fixture,
        stores: &CanonicalStores,
        faults: Arc<QualificationFaults>,
    ) -> Self {
        let authority = Singleton::open(
            fixture.directory.join("listener-authority.redb"),
            1,
            ClusterEpoch(1),
            Limits::default(),
        )
        .await
        .unwrap();
        let listeners = AuthorityListeners::start(
            authority,
            QualificationConfig {
                instance: INSTANCE.to_owned(),
                principal: "fixed-canonical-fixture".to_owned(),
                cas: Store::new(stores.cas.clone()),
                cas_namespace: [1; 32],
                publication_sink: Store::new(stores.sink.clone()),
                now: Arc::new(|| Ok(1)),
                faults,
            },
        )
        .await
        .unwrap();
        let worker = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let worker_address = worker.local_addr().unwrap();
        let client_address = client.local_addr().unwrap();
        let (stop_worker, worker_stopped) = oneshot::channel();
        let (stop_client, client_stopped) = oneshot::channel();
        let worker_task = spawn!("canonical_worker_role", async move {
            listeners
                .worker
                .serve(Server::builder(), worker, async {
                    let _stopped = worker_stopped.await;
                })
                .await
                .unwrap();
        });
        let client_task = spawn!("canonical_client_role", async move {
            listeners
                .client
                .serve(Server::builder(), client, async {
                    let _stopped = client_stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            worker: Endpoint::from_shared(format!("http://{worker_address}"))
                .unwrap()
                .connect()
                .await
                .unwrap(),
            client: Endpoint::from_shared(format!("http://{client_address}"))
                .unwrap()
                .connect()
                .await
                .unwrap(),
            service: listeners.service,
            coordinator: listeners.coordinator,
            stops: [stop_worker, stop_client],
            tasks: [worker_task, client_task],
        }
    }

    async fn shutdown(self) {
        self.coordinator.shutdown().await.unwrap();
        for stop in self.stops {
            let _stopped = stop.send(());
        }
        drop(self.worker);
        drop(self.client);
        for task in self.tasks {
            timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

fn resource(
    algorithm: DigestHasherFunc,
    key: DigestInfo,
    upload: bool,
    compressed: bool,
) -> String {
    let algorithm = match algorithm {
        DigestHasherFunc::Sha256 => "sha256",
        DigestHasherFunc::Blake3 => "blake3",
    };
    format!(
        "{INSTANCE}/{}{}/{algorithm}/{}/{}",
        if upload {
            "uploads/fixed-canonical/"
        } else {
            ""
        },
        if compressed {
            "compressed-blobs/zstd"
        } else {
            "blobs"
        },
        key.packed_hash(),
        key.size_bytes(),
    )
}

async fn upload(channel: Channel, algorithm: DigestHasherFunc, bytes: &[u8]) -> DigestInfo {
    let key = digest(algorithm, bytes);
    let messages: Vec<_> = bytes
        .chunks(IO_BUFFER_BYTES)
        .enumerate()
        .map(|(index, chunk)| WriteRequest {
            resource_name: resource(algorithm, key, true, false),
            write_offset: i64::try_from(index * IO_BUFFER_BYTES).unwrap(),
            finish_write: (index + 1) * IO_BUFFER_BYTES >= bytes.len(),
            data: chunk.to_vec().into(),
        })
        .collect();
    let written = ByteStreamClient::new(channel)
        .write(stream::iter(messages))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(written.committed_size, i64::try_from(bytes.len()).unwrap());
    key
}

fn assert_compression_disabled(error: &tonic::Status) {
    assert_eq!(error.code(), RpcCode::InvalidArgument);
    assert!(
        error
            .message()
            .contains("Remote cache compression is not supported by this instance"),
        "Expected the disabled-compression guard, got {error}",
    );
}

#[nativelink_macro::nativelink_test]
async fn fixed_roles_transfer_large_objects_in_bounded_chunks_both_algorithms() {
    let (fixture, stores) = Fixture::new().await;
    let rpc = FixedRpc::new(&fixture, &stores).await;
    let capabilities = CapabilitiesClient::new(rpc.client.clone())
        .get_capabilities(GetCapabilitiesRequest {
            instance_name: INSTANCE.to_owned(),
        })
        .await
        .unwrap()
        .into_inner()
        .cache_capabilities
        .unwrap();
    assert_eq!(capabilities.max_batch_total_size_bytes, 64 * 1024);
    assert!(capabilities.supported_compressors.is_empty());
    assert!(capabilities.supported_batch_update_compressors.is_empty());
    let bytes: Vec<_> = (0..2 * MAX_CHUNK_BYTES + 17)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        for (upload_channel, read_channel) in [
            (rpc.worker.clone(), rpc.client.clone()),
            (rpc.client.clone(), rpc.worker.clone()),
        ] {
            let key = upload(upload_channel, algorithm, &bytes).await;
            let mut read = ByteStreamClient::new(read_channel)
                .read(ReadRequest {
                    resource_name: resource(algorithm, key, false, false),
                    ..ReadRequest::default()
                })
                .await
                .unwrap()
                .into_inner();
            let mut received = Vec::new();
            let mut chunks = 0;
            while let Some(chunk) = read.message().await.unwrap() {
                assert!(!chunk.data.is_empty());
                assert!(chunk.data.len() <= 64 * 1024);
                received.extend_from_slice(&chunk.data);
                chunks += 1;
            }
            assert!(chunks > 32);
            assert_eq!(received, bytes);
        }
    }
    rpc.shutdown().await;
}

#[nativelink_macro::nativelink_test]
async fn fixed_roles_refuse_oversized_malformed_and_compressed_writes_without_files() {
    let (fixture, stores) = Fixture::new().await;
    let rpc = FixedRpc::new(&fixture, &stores).await;
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let bytes = vec![7; 1024 * 1024];
        let key = digest(algorithm, &bytes);
        for channel in [rpc.worker.clone(), rpc.client.clone()] {
            let mut cas = ContentAddressableStorageClient::new(channel.clone());
            let oversized = cas
                .batch_update_blobs(BatchUpdateBlobsRequest {
                    instance_name: INSTANCE.to_owned(),
                    digest_function: algorithm.proto_digest_func().into(),
                    requests: vec![batch_update_blobs_request::Request {
                        digest: Some(key.into()),
                        data: bytes.clone().into(),
                        ..batch_update_blobs_request::Request::default()
                    }],
                })
                .await
                .unwrap_err();
            assert_eq!(oversized.code(), RpcCode::OutOfRange);
            assert!(
                oversized
                    .message()
                    .contains("decoded message length too large")
            );
            let mut bytestream = ByteStreamClient::new(channel);
            assert_eq!(
                bytestream
                    .write(stream::iter([WriteRequest {
                        resource_name: resource(algorithm, key, true, false),
                        data: bytes.clone().into(),
                        finish_write: true,
                        write_offset: 0,
                    }]))
                    .await
                    .unwrap_err()
                    .code(),
                RpcCode::OutOfRange,
            );
            assert_eq!(
                cas.batch_update_blobs(BatchUpdateBlobsRequest {
                    instance_name: INSTANCE.to_owned(),
                    digest_function: algorithm.proto_digest_func().into(),
                    requests: vec![batch_update_blobs_request::Request::default()],
                })
                .await
                .unwrap()
                .into_inner()
                .responses[0]
                    .status
                    .as_ref()
                    .unwrap()
                    .code,
                RpcCode::InvalidArgument as i32,
            );
            assert_eq!(
                bytestream
                    .write(stream::iter([WriteRequest {
                        resource_name: "not-a-resource".to_owned(),
                        finish_write: true,
                        ..WriteRequest::default()
                    }]))
                    .await
                    .unwrap_err()
                    .code(),
                RpcCode::Internal,
            );
            let compressed = cas
                .batch_update_blobs(BatchUpdateBlobsRequest {
                    instance_name: INSTANCE.to_owned(),
                    digest_function: algorithm.proto_digest_func().into(),
                    requests: vec![batch_update_blobs_request::Request {
                        digest: Some(digest(algorithm, b"x").into()),
                        data: Bytes::from_static(b"not a zstd frame"),
                        compressor: compressor::Value::Zstd as i32,
                    }],
                })
                .await
                .unwrap()
                .into_inner();
            let status = compressed.responses[0].status.as_ref().unwrap();
            assert_eq!(status.code, RpcCode::InvalidArgument as i32);
            assert!(
                status
                    .message
                    .contains("Remote cache compression is not supported by this instance")
            );
            assert_compression_disabled(
                &bytestream
                    .write(stream::iter([WriteRequest {
                        resource_name: resource(algorithm, key, true, true),
                        data: Bytes::from_static(b"not a zstd frame"),
                        finish_write: true,
                        write_offset: 0,
                    }]))
                    .await
                    .unwrap_err(),
            );
            assert_compression_disabled(
                &bytestream
                    .read(ReadRequest {
                        resource_name: resource(algorithm, key, false, true),
                        ..ReadRequest::default()
                    })
                    .await
                    .unwrap_err(),
            );
        }
        let mut ac = ActionCacheClient::new(rpc.client.clone());
        assert_eq!(
            ac.update_action_result(UpdateActionResultRequest {
                instance_name: INSTANCE.to_owned(),
                action_digest: Some(key.into()),
                digest_function: algorithm.proto_digest_func().into(),
                action_result: Some(ActionResult {
                    stdout_raw: bytes.into(),
                    ..ActionResult::default()
                }),
                ..UpdateActionResultRequest::default()
            })
            .await
            .unwrap_err()
            .code(),
            RpcCode::OutOfRange,
        );
        assert_eq!(
            ac.update_action_result(UpdateActionResultRequest {
                instance_name: INSTANCE.to_owned(),
                action_digest: Some(key.into()),
                digest_function: algorithm.proto_digest_func().into(),
                ..UpdateActionResultRequest::default()
            })
            .await
            .unwrap_err()
            .code(),
            RpcCode::Internal,
        );
    }
    // Nothing reached either driver's create_exclusive: rejected requests left
    // neither abandoned temporary files nor installed content in either role.
    for sink in [false, true] {
        assert_eq!(fixture.temporary_count(sink), 0);
        assert_eq!(
            fs::read_dir(
                fixture
                    .directory
                    .join(if sink { "sink" } else { "cas" })
                    .join("content")
            )
            .unwrap()
            .count(),
            0
        );
    }
    rpc.shutdown().await;
}

#[nativelink_macro::nativelink_test]
async fn fixed_roles_refuse_large_cas_batches_before_reads_and_ac_after_reads() {
    let (fixture, stores) = Fixture::new().await;
    let rpc = FixedRpc::new(&fixture, &stores).await;
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let bytes = vec![9; 1024 * 1024 + 1];
        let key = upload(rpc.worker.clone(), algorithm, &bytes).await;
        for channel in [rpc.worker.clone(), rpc.client.clone()] {
            let Err(error) = ContentAddressableStorageClient::new(channel)
                .batch_read_blobs(BatchReadBlobsRequest {
                    instance_name: INSTANCE.to_owned(),
                    digest_function: algorithm.proto_digest_func().into(),
                    digests: vec![key.into()],
                    ..BatchReadBlobsRequest::default()
                })
                .await
            else {
                panic!("oversized CAS batch must fail before backend reads");
            };
            assert_eq!(error.code(), RpcCode::InvalidArgument);
            assert!(error.message().contains("Fixed CAS batch content limit"));
        }
        let result = ActionResult {
            stdout_raw: bytes.into(),
            ..ActionResult::default()
        };
        let encoded = result.encode_to_vec();
        let (mut sender, receiver) = make_buf_channel_pair();
        let update = pin(&stores.sink).update(
            key.into(),
            receiver,
            UploadSizeInfo::ExactSize(u64::try_from(encoded.len()).unwrap()),
        );
        let send = async {
            for chunk in encoded.chunks(IO_BUFFER_BYTES) {
                sender.send(Bytes::copy_from_slice(chunk)).await.unwrap();
            }
            sender.send_eof().unwrap();
        };
        async {
            let (updated, ()) = tokio::join!(update, send);
            updated.unwrap();
        }
        .with_context(make_ctx_for_hash_func(algorithm).unwrap())
        .await;
        let Err(error) = ActionCacheClient::new(rpc.client.clone())
            .get_action_result(GetActionResultRequest {
                instance_name: INSTANCE.to_owned(),
                action_digest: Some(key.into()),
                digest_function: algorithm.proto_digest_func().into(),
                ..GetActionResultRequest::default()
            })
            .await
        else {
            panic!("oversized AC response must fail during encoding");
        };
        assert_eq!(error.code(), RpcCode::OutOfRange);
        assert!(error.message().contains("encoded message length too large"));
    }
    rpc.shutdown().await;
}
