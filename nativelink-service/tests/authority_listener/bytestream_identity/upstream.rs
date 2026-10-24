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

use nativelink_config::cas_server::{ByteStreamConfig, CasStoreConfig, WithInstanceName};
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_proto::google::bytestream::WriteResponse;
use nativelink_proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use nativelink_service::bytestream_server::ByteStreamServer as LegacyByteStream;
use nativelink_service::cas_server::CasServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use tonic::{Request, Response};

use super::*;

struct Observed {
    inner: LegacyByteStream,
    queries: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl ByteStream for Observed {
    type ReadStream = <LegacyByteStream as ByteStream>::ReadStream;

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        self.inner.read(request).await
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        self.inner.write(request).await
    }

    async fn query_write_status(
        &self,
        request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.inner.query_write_status(request).await
    }
}

struct Upstream {
    address: String,
    queries: Arc<AtomicUsize>,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl Upstream {
    async fn start(spy: Arc<Spy>) -> Self {
        let manager = StoreManager::new();
        manager.add_store("cas", Store::new(spy)).unwrap();
        let compression = RemoteCacheCompressionInstances::default();
        let cas = CasServer::new(
            &[WithInstanceName {
                instance_name: INSTANCE.into(),
                config: CasStoreConfig {
                    cas_store: "cas".into(),
                    experimental_chunking: None,
                },
            }],
            &manager,
            &compression,
        )
        .unwrap();
        let inner = LegacyByteStream::new(
            &[WithInstanceName {
                instance_name: INSTANCE.into(),
                config: ByteStreamConfig {
                    cas_store: "cas".into(),
                    max_bytes_per_stream: 64 * 1024,
                    persist_stream_on_disconnect_timeout_s: 1,
                },
            }],
            &manager,
            &compression,
        )
        .unwrap();
        let queries = Arc::new(AtomicUsize::new(0));
        let service = Observed {
            inner,
            queries: queries.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = background_spawn!("identity_fixture_upstream", async move {
            let incoming = stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            Server::builder()
                .add_service(cas.into_service())
                .add_service(ByteStreamServer::new(service))
                .serve_with_incoming_shutdown(incoming, async {
                    let _stopped = stopped.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            queries,
            stop,
            task,
        }
    }

    fn store(&self) -> Store {
        Store::new(
            GrpcStore::new(&GrpcSpec {
                instance_name: INSTANCE.into(),
                endpoints: vec![GrpcEndpoint {
                    address: self.address.clone(),
                    tls_config: None,
                    concurrency_limit: None,
                    connect_timeout_s: 0,
                    tcp_keepalive_s: 0,
                    http2_keepalive_interval_s: 0,
                    http2_keepalive_timeout_s: 0,
                }],
                store_type: StoreType::Cas,
                retry: Retry::default(),
                max_concurrent_requests: 0,
                connections_per_endpoint: 1,
                rpc_timeout_s: 5,
                use_legacy_resource_names: false,
                headers: HashMap::new(),
                forward_headers: Vec::new(),
                experimental_read_batching: None,
                experimental_remote_cache_compression: Some(false),
            })
            .unwrap(),
        )
    }

    async fn shutdown(self) {
        let _stopped = self.stop.send(());
        tokio::time::timeout(WAIT, self.task)
            .await
            .unwrap()
            .unwrap();
    }
}

async fn remote_fixture() -> (Runtime, Upstream, Arc<Spy>) {
    let spy = Spy::new(false, false);
    let upstream = Upstream::start(spy.clone()).await;
    let runtime = Runtime::start_with_store_and_transports(
        upstream.store(),
        Server::builder(),
        Server::builder(),
        None,
        None,
    )
    .await;
    (runtime, upstream, spy)
}

// Observe entry into the real upstream update, without assuming any prefix
// length has passed its transport. Query remains an explicit tested operation.
async fn upstream_started(
    runtime: &Runtime,
    spy: &Spy,
    name: &str,
) -> (mpsc::Sender<WriteRequest>, UploadTask) {
    let (sender, task) = writer(runtime.worker.clone());
    sender
        .send(frame(name.to_owned(), 0, false, &DATA[..PREFIX]))
        .await
        .unwrap();
    tokio::time::timeout(WAIT, async {
        while spy.updates.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upstream backend update did not start");
    (sender, task)
}

#[nativelink_test]
async fn grpc_backed_query_uses_local_exact_upload_identity() {
    let (runtime, upstream, spy) = remote_fixture().await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = upstream_started(&runtime, &spy, &name).await;
    let exact = status(runtime.client.clone(), name.clone()).await.unwrap();
    assert_eq!(upstream.queries.load(Ordering::Relaxed), 0);
    assert_eq!(
        exact,
        QueryWriteStatusResponse {
            committed_size: i64::try_from(PREFIX).unwrap(),
            complete: false
        }
    );
    assert_eq!(
        status(runtime.client.clone(), different(Difference::Hash, &digest))
            .await
            .unwrap(),
        QueryWriteStatusResponse {
            committed_size: 0,
            complete: false
        }
    );
    for invalid in [
        name.replace("/sha256/", "/sha1/"),
        name.replace("/blobs/", "/compressed-blobs/zstd/"),
    ] {
        assert_eq!(
            status(runtime.client.clone(), invalid)
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(upstream.queries.load(Ordering::Relaxed), 0);
    sender
        .send(frame(String::new(), PREFIX, true, &DATA[PREFIX..]))
        .await
        .unwrap();
    drop(sender);
    result(task).await.unwrap();
    assert_eq!(spy.value(DigestHasherFunc::Sha256, &digest).unwrap(), DATA);
    runtime.shutdown().await;
    upstream.shutdown().await;
}

#[nativelink_test]
async fn grpc_backed_write_preserves_exact_active_and_idle_identity() {
    let (runtime, upstream, spy) = remote_fixture().await;
    let digest = digest(DigestHasherFunc::Blake3, DATA);
    let name = resource(UUID, "blake3/", &digest);
    let (sender, task) = upstream_started(&runtime, &spy, &name).await;
    assert_eq!(
        write(runtime.worker.clone(), frame(name.clone(), 0, true, DATA))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    drop(sender);
    assert!(result(task).await.is_err());
    let mut wrong_digest = digest.clone();
    wrong_digest.hash = "aa".repeat(32);
    assert_eq!(
        write(
            runtime.worker.clone(),
            frame(
                resource(UUID, "blake3/", &wrong_digest),
                PREFIX,
                true,
                &DATA[PREFIX..]
            )
        )
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    write(
        runtime.worker.clone(),
        frame(name, PREFIX, true, &DATA[PREFIX..]),
    )
    .await
    .unwrap();
    assert_eq!(spy.updates.load(Ordering::Relaxed), 1);
    assert_eq!(spy.value(DigestHasherFunc::Blake3, &digest).unwrap(), DATA);
    assert_eq!(upstream.queries.load(Ordering::Relaxed), 0);
    runtime.shutdown().await;
    upstream.shutdown().await;
}
