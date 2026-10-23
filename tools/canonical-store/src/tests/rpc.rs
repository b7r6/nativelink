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

use std::io;
use std::task::{Context, Poll};

use futures::StreamExt;
use hyper_util::rt::TokioIo;
use nativelink_config::cas_server::{
    AcStoreConfig, ByteStreamConfig, CasStoreConfig, WithInstanceName,
};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, BatchUpdateBlobsRequest, GetActionResultRequest, UpdateActionResultRequest,
    batch_update_blobs_request, compressor, digest_function,
};
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{QueryWriteStatusRequest, ReadRequest, WriteRequest};
use nativelink_service::ac_server::AcServer;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::wire_compression::{RemoteCacheCompressionInstances, decompress};
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::Store;
use nativelink_util::task::JoinHandleDropGuard;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Server};
use tower::service_fn;

use super::*;

struct Connection(DuplexStream);
impl Connected for Connection {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(context, buffer)
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(context, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(context)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(context)
    }
}

struct Rpc {
    cas: ContentAddressableStorageClient<Channel>,
    bs: ByteStreamClient<Channel>,
    ac: ActionCacheClient<Channel>,
    _task: JoinHandleDropGuard<()>,
}
impl Rpc {
    async fn new(stores: &CanonicalStores) -> Self {
        let manager = StoreManager::new();
        manager
            .add_store("cas", Store::new(stores.cas.clone()))
            .unwrap();
        manager
            .add_store("sink", Store::new(stores.sink.clone()))
            .unwrap();
        manager.run_post_init().await.unwrap();
        let compression =
            RemoteCacheCompressionInstances::from_enabled_instance_names(["build".into()]);
        let cas = CasServer::new(
            &[WithInstanceName {
                instance_name: "build".into(),
                config: CasStoreConfig {
                    cas_store: "cas".into(),
                    experimental_chunking: None,
                },
            }],
            &manager,
            &RemoteCacheCompressionInstances::default(),
        )
        .unwrap();
        let bs = ByteStreamServer::new(
            &[WithInstanceName {
                instance_name: "build".into(),
                config: ByteStreamConfig {
                    cas_store: "cas".into(),
                    max_bytes_per_stream: IO_BUFFER_BYTES,
                    persist_stream_on_disconnect_timeout_s: 1,
                },
            }],
            &manager,
            &compression,
        )
        .unwrap();
        let ac = AcServer::new(
            &[WithInstanceName {
                instance_name: "build".into(),
                config: AcStoreConfig {
                    ac_store: "sink".into(),
                    read_only: false,
                },
            }],
            &manager,
        )
        .unwrap();
        let (sender, receiver) = tokio::sync::mpsc::channel::<io::Result<Connection>>(1);
        let task = spawn!("canonical_rpc", async move {
            Server::builder()
                .add_service(
                    cas.into_service()
                        .max_decoding_message_size(MAX_CHUNK_BYTES),
                )
                .add_service(bs.into_service().max_decoding_message_size(MAX_CHUNK_BYTES))
                .add_service(ac.into_service().max_decoding_message_size(MAX_CHUNK_BYTES))
                .serve_with_incoming(ReceiverStream::new(receiver))
                .await
                .unwrap();
        });
        let channel = Endpoint::from_static("http://canonical.invalid")
            .connect_with_connector(service_fn(move |_| {
                let sender = sender.clone();
                async move {
                    let (client, server) = tokio::io::duplex(IO_BUFFER_BYTES);
                    sender
                        .send(Ok(Connection(server)))
                        .await
                        .map_err(|_| io::Error::other("server stopped"))?;
                    Ok::<_, io::Error>(TokioIo::new(client))
                }
            }))
            .await
            .unwrap();
        Self {
            cas: ContentAddressableStorageClient::new(channel.clone()),
            bs: ByteStreamClient::new(channel.clone()),
            ac: ActionCacheClient::new(channel),
            _task: task,
        }
    }
}

fn function(algorithm: DigestHasherFunc) -> i32 {
    match algorithm {
        DigestHasherFunc::Sha256 => digest_function::Value::Sha256 as i32,
        DigestHasherFunc::Blake3 => digest_function::Value::Blake3 as i32,
    }
}
fn algorithm_name(algorithm: DigestHasherFunc) -> &'static str {
    match algorithm {
        DigestHasherFunc::Sha256 => "sha256",
        DigestHasherFunc::Blake3 => "blake3",
    }
}
fn resource(algorithm: DigestHasherFunc, key: DigestInfo, upload: bool) -> String {
    format!(
        "build/{}blobs/{}/{}/{}",
        if upload {
            "uploads/canonical-test/"
        } else {
            ""
        },
        algorithm_name(algorithm),
        key.packed_hash(),
        key.size_bytes()
    )
}
fn batch(algorithm: DigestHasherFunc, key: DigestInfo, data: &[u8]) -> BatchUpdateBlobsRequest {
    BatchUpdateBlobsRequest {
        instance_name: "build".into(),
        digest_function: function(algorithm),
        requests: vec![batch_update_blobs_request::Request {
            digest: Some(key.into()),
            data: data.to_vec().into(),
            compressor: 0,
        }],
    }
}
fn write(algorithm: DigestHasherFunc, key: DigestInfo, data: &[u8]) -> WriteRequest {
    WriteRequest {
        resource_name: resource(algorithm, key, true),
        write_offset: 0,
        finish_write: true,
        data: data.to_vec().into(),
    }
}

#[nativelink_macro::nativelink_test]
async fn cas_batch_status_waits_for_directory_sync_and_propagates_refusal() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        for fail in [false, true] {
            let (_fixture, stores) = Fixture::new().await;
            let mut rpc = Rpc::new(&stores).await;
            let key = digest(algorithm, b"CAS final barrier");
            let gate = stores
                .cas
                .0
                .shared
                .hooks
                .install(HookPoint::DirectorySync, fail);
            let mut client = rpc.cas.clone();
            let mut request = spawn!("batch_barrier", async move {
                client
                    .batch_update_blobs(batch(algorithm, key, b"CAS final barrier"))
                    .await
            });
            gate.wait().await;
            assert!(
                timeout(Duration::from_millis(20), &mut request)
                    .await
                    .is_err(),
                "RPC acknowledgement must wait"
            );
            gate.release();
            let response = request.await.unwrap().unwrap().into_inner();
            assert_eq!(response.responses.len(), 1);
            assert_eq!(
                response.responses[0].status.as_ref().unwrap().code,
                if fail { 14 } else { 0 }
            );
            // A post-publication error is ambiguous: a query must run a fresh
            // file+directory barrier before reporting complete=true.
            if fail {
                let gate = stores
                    .cas
                    .0
                    .shared
                    .hooks
                    .install(HookPoint::DirectorySync, true);
                let mut client = rpc.bs.clone();
                let mut query = spawn!("query_barrier", async move {
                    client
                        .query_write_status(QueryWriteStatusRequest {
                            resource_name: resource(algorithm, key, true),
                        })
                        .await
                });
                gate.wait().await;
                assert!(
                    timeout(Duration::from_millis(20), &mut query)
                        .await
                        .is_err()
                );
                gate.release();
                assert_eq!(
                    query.await.unwrap().unwrap_err().code(),
                    tonic::Code::Unavailable
                );
            }
            let status = rpc
                .bs
                .query_write_status(QueryWriteStatusRequest {
                    resource_name: resource(algorithm, key, true),
                })
                .await
                .unwrap()
                .into_inner();
            assert!(status.complete);
            assert_eq!(status.committed_size, key.size_bytes() as i64);
        }
    }
}

#[nativelink_macro::nativelink_test]
async fn bytestream_write_ack_and_identity_compressed_reads_use_qualified_backend() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        for fail in [false, true] {
            let (_fixture, stores) = Fixture::new().await;
            let mut rpc = Rpc::new(&stores).await;
            let data = vec![0x5b; IO_BUFFER_BYTES * 2 + 17];
            let key = digest(algorithm, &data);
            let gate = stores
                .cas
                .0
                .shared
                .hooks
                .install(HookPoint::DirectorySync, fail);
            let mut client = rpc.bs.clone();
            let upload = write(algorithm, key, &data);
            let mut request = spawn!("write_barrier", async move {
                client.write(tokio_stream::iter([upload])).await
            });
            gate.wait().await;
            assert!(
                timeout(Duration::from_millis(20), &mut request)
                    .await
                    .is_err()
            );
            gate.release();
            let response = request.await.unwrap();
            if fail {
                assert_eq!(response.unwrap_err().code(), tonic::Code::Unavailable);
            } else {
                assert_eq!(
                    response.unwrap().into_inner().committed_size,
                    data.len() as i64
                );
            }
            for compressed in [false, true] {
                let name = if compressed {
                    format!(
                        "build/compressed-blobs/zstd/{}/{}/{}",
                        algorithm_name(algorithm),
                        key.packed_hash(),
                        key.size_bytes()
                    )
                } else {
                    resource(algorithm, key, false)
                };
                let mut stream = rpc
                    .bs
                    .read(ReadRequest {
                        resource_name: name,
                        read_offset: 0,
                        read_limit: 0,
                    })
                    .await
                    .unwrap()
                    .into_inner();
                let mut result = Vec::new();
                while let Some(response) = stream.next().await {
                    result.extend_from_slice(&response.unwrap().data);
                }
                let result = if compressed {
                    decompress(&result, compressor::Value::Zstd, data.len()).unwrap()
                } else {
                    Bytes::from(result)
                };
                assert_eq!(result.as_ref(), data.as_slice());
            }
        }
    }
}

#[nativelink_macro::nativelink_test]
async fn cancelled_rpc_can_finish_commit_without_sending_an_acknowledgement() {
    let (_fixture, stores) = Fixture::new().await;
    let mut rpc = Rpc::new(&stores).await;
    let algorithm = DigestHasherFunc::Blake3;
    let key = digest(algorithm, b"cancel after publish");
    let gate = stores
        .cas
        .0
        .shared
        .hooks
        .install(HookPoint::DirectorySync, false);
    let permits = stores.cas.0.shared.permits.clone();
    let mut client = rpc.bs.clone();
    let request = spawn!("cancel_rpc", async move {
        client
            .write(tokio_stream::iter([write(
                algorithm,
                key,
                b"cancel after publish",
            )]))
            .await
    });
    gate.wait().await;
    drop(request);
    assert_eq!(permits.available_permits(), 3);
    gate.release();
    timeout(Duration::from_secs(5), async {
        while permits.available_permits() != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The existing ByteStream service can retain an interrupted upload for
    // its one-second resume window. That transient status is incomplete even
    // when storage committed; wait for the service to consult the CAS again.
    let result = timeout(Duration::from_secs(5), async {
        loop {
            let result = rpc
                .bs
                .query_write_status(QueryWriteStatusRequest {
                    resource_name: resource(algorithm, key, true),
                })
                .await
                .unwrap()
                .into_inner();
            if result.complete {
                break result;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(result.committed_size, 20);
}

#[nativelink_macro::nativelink_test]
async fn ac_update_get_preserves_mutable_action_result_and_final_sync_status() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let (_fixture, stores) = Fixture::new().await;
        let mut rpc = Rpc::new(&stores).await;
        let key = DigestInfo::new([0xc3; 32], 1_000_000);
        for fail in [false, true] {
            let result = ActionResult {
                exit_code: if fail { 7 } else { 3 },
                stdout_raw: b"opaque result payload".to_vec().into(),
                ..Default::default()
            };
            let gate = stores
                .sink
                .0
                .shared
                .hooks
                .install(HookPoint::DirectorySync, fail);
            let mut client = rpc.ac.clone();
            let update = UpdateActionResultRequest {
                instance_name: "build".into(),
                action_digest: Some(key.into()),
                action_result: Some(result.clone()),
                digest_function: function(algorithm),
                ..Default::default()
            };
            let mut request = spawn!("ac_barrier", async move {
                client.update_action_result(update).await
            });
            gate.wait().await;
            assert!(
                timeout(Duration::from_millis(20), &mut request)
                    .await
                    .is_err()
            );
            gate.release();
            let response = request.await.unwrap();
            if fail {
                assert_eq!(response.unwrap_err().code(), tonic::Code::Unavailable);
            } else {
                assert_eq!(response.unwrap().into_inner(), result);
            }
            let read = rpc
                .ac
                .get_action_result(GetActionResultRequest {
                    instance_name: "build".into(),
                    action_digest: Some(key.into()),
                    digest_function: function(algorithm),
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(read, result);
        }
    }
}

// Run this exact frozen test binary under strace fsync fault injection. A
// successful baseline prints its RPC outcome; injected EIO must reach the
// individual BatchUpdateBlobs status and preserve any temporary/content entry.
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "The syscall witness requires a deterministic single blocking worker."
)]
fn syscall_fsync_rpc_witness() {
    // Exactly one blocking worker gives strace's per-thread injection counter
    // a deterministic prefix: eight layout fsyncs, then file and directories.
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let (fixture, stores) = Fixture::new().await;
            let mut rpc = Rpc::new(&stores).await;
            let key = digest(DigestHasherFunc::Sha256, b"syscall failure");
            let scenario = std::env::var("CANONICAL_STORE_FSYNC_SCENARIO").unwrap_or_else(|_| "upload".into());
            let expected = std::env::var("CANONICAL_STORE_EXPECT_FSYNC_ERROR").ok();
            let first = rpc.cas.batch_update_blobs(batch(DigestHasherFunc::Sha256, key, b"syscall failure"))
                .await.unwrap().into_inner().responses[0].status.as_ref().unwrap().code;
            let status = match scenario.as_str() {
                "upload" => first,
                "has" => {
                    assert_eq!(first, 0);
                    match rpc.bs.query_write_status(QueryWriteStatusRequest {
                        resource_name: resource(DigestHasherFunc::Sha256, key, true),
                    }).await {
                        Ok(response) => { assert!(response.into_inner().complete); 0 }
                        Err(error) => error.code() as i32,
                    }
                }
                "duplicate" => {
                    assert_eq!(first, 0);
                    rpc.cas.batch_update_blobs(batch(DigestHasherFunc::Sha256, key, b"syscall failure"))
                        .await.unwrap().into_inner().responses[0].status.as_ref().unwrap().code
                }
                _ => panic!("unknown syscall scenario"),
            };
            assert_eq!(status, if expected.is_some() { 14 } else { 0 });
            let content = fs::read_dir(fixture.directory.join("cas/content")).unwrap().count();
            let temporary = fixture.temporary_count(false);
            let expected_entries = match (scenario.as_str(), expected.as_deref()) {
                ("upload", Some("file")) => (0, 1),
                ("duplicate", _) => (1, 1),
                _ => (1, 0),
            };
            assert_eq!((content, temporary), expected_entries, "failed IO preserves evidence at the observed boundary");
            println!("syscall witness: scenario={scenario} status={status} content={content} temporary={temporary}");
        });
}
