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

//! Lab reproduction of the production "shard-ring wedge": a `ShardStore`
//! ring over `GrpcStore` backends (peer CAS shards) where one peer is dead
//! (killed or never started). Before the fix, every CAS RPC that touched
//! the dead shard hung forever: the peer's `ConnectionManager` retried the
//! connect in the background while `connection()` requests queued
//! unbounded, so no error ever reached the retry machinery. The
//! operational workaround was `systemctl restart nativelink` on the
//! frontend.
//!
//! These tests must fail (by generous outer timeout) against the
//! pre-fix code and pass after dead peers yield fast `Unavailable`.

use core::pin::Pin;
use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;

use async_lock::Mutex;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use nativelink_config::stores::{
    GrpcEndpoint, GrpcSpec, MemorySpec, Retry, ShardConfig, ShardSpec, StoreSpec, StoreType,
};
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_server::{
    ContentAddressableStorage, ContentAddressableStorageServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest,
    GetTreeResponse, SpliceBlobRequest, SpliceBlobResponse, SplitBlobRequest, SplitBlobResponse,
};
use nativelink_proto::google::bytestream::byte_stream_server::{ByteStream, ByteStreamServer};
use nativelink_proto::google::bytestream::{
    QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest,
    WriteResponse,
};
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::shard_store::ShardStore;
use nativelink_util::background_spawn;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};

/// Generous bound: far longer than any configured timeout/retry budget in
/// this test, far shorter than "forever". If an operation does not settle
/// within this, it is the production hang.
const GENEROUS_TIMEOUT: Duration = Duration::from_secs(20);

/// A minimal in-process CAS shard: blobs live in a shared map,
/// `FindMissingBlobs`, `ByteStream.Read` and `ByteStream.Write` behave like
/// a real peer.
#[derive(Debug, Clone)]
struct FakeShardServer {
    blobs: Arc<Mutex<HashMap<String, Bytes>>>,
}

type GetTreeStream = Pin<Box<dyn Stream<Item = Result<GetTreeResponse, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl ContentAddressableStorage for FakeShardServer {
    type GetTreeStream = GetTreeStream;

    async fn find_missing_blobs(
        &self,
        grpc_request: Request<FindMissingBlobsRequest>,
    ) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let request = grpc_request.into_inner();
        let blobs = self.blobs.lock().await;
        let missing_blob_digests = request
            .blob_digests
            .into_iter()
            .filter(|digest| !blobs.contains_key(&digest.hash))
            .collect();
        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests,
        }))
    }

    #[allow(clippy::unimplemented)]
    async fn batch_update_blobs(
        &self,
        _grpc_request: Request<BatchUpdateBlobsRequest>,
    ) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        unimplemented!();
    }

    #[allow(clippy::unimplemented)]
    async fn batch_read_blobs(
        &self,
        _grpc_request: Request<BatchReadBlobsRequest>,
    ) -> Result<Response<BatchReadBlobsResponse>, Status> {
        unimplemented!();
    }

    #[allow(clippy::unimplemented)]
    async fn get_tree(
        &self,
        _grpc_request: Request<GetTreeRequest>,
    ) -> Result<Response<Self::GetTreeStream>, Status> {
        unimplemented!();
    }

    #[allow(clippy::unimplemented)]
    async fn split_blob(
        &self,
        _grpc_request: Request<SplitBlobRequest>,
    ) -> Result<Response<SplitBlobResponse>, Status> {
        unimplemented!();
    }

    #[allow(clippy::unimplemented)]
    async fn splice_blob(
        &self,
        _grpc_request: Request<SpliceBlobRequest>,
    ) -> Result<Response<SpliceBlobResponse>, Status> {
        unimplemented!();
    }
}

type ReadStream = Pin<Box<dyn Stream<Item = Result<ReadResponse, Status>> + Send + 'static>>;

/// Extracts the blob hash from a `ByteStream` resource name of the form
/// `[{instance}/]uploads/{uuid}/blobs/{fn}/{hash}/{size}` or
/// `[{instance}/]blobs/{fn}/{hash}/{size}`.
fn hash_from_resource_name(resource_name: &str) -> String {
    let mut components = resource_name.rsplit('/');
    let _size = components.next();
    components.next().unwrap_or_default().to_string()
}

#[tonic::async_trait]
impl ByteStream for FakeShardServer {
    type ReadStream = ReadStream;

    async fn read(
        &self,
        grpc_request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let request = grpc_request.into_inner();
        let hash = hash_from_resource_name(&request.resource_name);
        let data = self
            .blobs
            .lock()
            .await
            .get(&hash)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("Blob {hash} not seeded")))?;
        let offset = usize::try_from(request.read_offset)
            .map_err(|_| Status::invalid_argument("Bad read_offset"))?;
        if offset > data.len() {
            return Err(Status::out_of_range("read_offset past end of blob"));
        }
        let stream: Self::ReadStream = Box::pin(futures::stream::iter(vec![Ok(ReadResponse {
            data: data.slice(offset..),
        })]));
        Ok(Response::new(stream))
    }

    async fn write(
        &self,
        grpc_request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        let mut stream = grpc_request.into_inner();
        let mut hash = String::new();
        let mut data = Vec::new();
        while let Some(request) = stream.next().await {
            let request = request?;
            if !request.resource_name.is_empty() {
                hash = hash_from_resource_name(&request.resource_name);
            }
            data.extend_from_slice(&request.data);
            if request.finish_write {
                break;
            }
        }
        let committed_size =
            i64::try_from(data.len()).map_err(|_| Status::invalid_argument("Blob too large"))?;
        self.blobs.lock().await.insert(hash, Bytes::from(data));
        Ok(Response::new(WriteResponse { committed_size }))
    }

    #[allow(clippy::unimplemented)]
    async fn query_write_status(
        &self,
        _grpc_request: Request<QueryWriteStatusRequest>,
    ) -> Result<Response<QueryWriteStatusResponse>, Status> {
        unimplemented!();
    }
}

/// Spawns a live in-process shard peer on `port` (0 for ephemeral) and
/// returns its bound port and blob map.
fn spawn_live_peer_on(port: u16) -> (u16, Arc<Mutex<HashMap<String, Bytes>>>) {
    let server = FakeShardServer {
        blobs: Arc::new(Mutex::new(HashMap::new())),
    };
    let blobs = server.blobs.clone();
    let listener = TcpIncoming::bind(format!("127.0.0.1:{port}").parse().unwrap()).unwrap();
    let port = listener.local_addr().unwrap().port();
    let cas_service = ContentAddressableStorageServer::new(server.clone());
    let bytestream_service = ByteStreamServer::new(server);
    background_spawn!("shard_ring_dead_peer_test_server", async move {
        Server::builder()
            .add_service(cas_service)
            .add_service(bytestream_service)
            .serve_with_incoming(listener)
            .await
            .unwrap();
    });
    (port, blobs)
}

/// Reserves a port with nothing listening on it: the dead peer (killed or
/// never started). Connect attempts fail with `ECONNREFUSED`.
fn dead_peer_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn grpc_spec_for_port(port: u16) -> GrpcSpec {
    GrpcSpec {
        instance_name: String::new(),
        endpoints: vec![GrpcEndpoint {
            address: format!("http://127.0.0.1:{port}"),
            tls_config: None,
            concurrency_limit: None,
            connect_timeout_s: 1,
            tcp_keepalive_s: 0,
            http2_keepalive_interval_s: 0,
            http2_keepalive_timeout_s: 0,
        }],
        store_type: StoreType::Cas,
        retry: Retry {
            max_retries: 1,
            delay: 0.05,
            jitter: 0.0,
            retry_on_errors: None,
        },
        max_concurrent_requests: 0,
        connections_per_endpoint: 0,
        rpc_timeout_s: 5,
        use_legacy_resource_names: false,
        headers: HashMap::new(),
        forward_headers: vec![],
        experimental_read_batching: None,
        experimental_remote_cache_compression: Some(false),
    }
}

/// Mirrors `ShardStore`'s routing for a two-shard equal-weight ring: the
/// digest's xor-folded key lands in shard 0 iff it is `<= u32::MAX / 2`.
fn shard_index_for(digest: &DigestInfo) -> usize {
    let packed_hash = digest.packed_hash();
    let size_bytes = digest.size_bytes().to_le_bytes();
    let mut key: u32 = 0;
    for chunk in 0..8 {
        key ^= u32::from_le_bytes(packed_hash[chunk * 4..chunk * 4 + 4].try_into().unwrap());
    }
    key ^= u32::from_le_bytes(size_bytes[0..4].try_into().unwrap());
    key ^= u32::from_le_bytes(size_bytes[4..8].try_into().unwrap());
    usize::from(key > u32::MAX / 2)
}

/// Finds a digest (with 64-byte content) that routes to `shard`.
fn digest_for_shard(shard: usize) -> (DigestInfo, Bytes) {
    for i in 0..1024usize {
        let hash = format!("{i:064x}");
        let content = Bytes::from(format!("{i:0>64}"));
        let digest = DigestInfo::try_new(&hash, content.len()).unwrap();
        if shard_index_for(&digest) == shard {
            return (digest, content);
        }
    }
    unreachable!("no digest found for shard {shard} in 1024 candidates");
}

struct Ring {
    shard_store: Arc<ShardStore>,
    live_shard: usize,
    live_blobs: Arc<Mutex<HashMap<String, Bytes>>>,
    dead_port: u16,
}

/// Builds a two-shard equal-weight ring: shard 0 is a live in-process
/// peer, shard 1 is dead (nothing listening).
async fn make_ring() -> Result<Ring, Error> {
    let (live_port, live_blobs) = spawn_live_peer_on(0);
    let dead_port = dead_peer_port();

    let live_store = GrpcStore::new(&grpc_spec_for_port(live_port))?;
    let dead_store = GrpcStore::new(&grpc_spec_for_port(dead_port))?;

    // The spec's `store` fields are only used for length/weights by
    // `ShardStore::new`; memory placeholders keep the config minimal.
    let shard_store = ShardStore::new(
        &ShardSpec {
            stores: (0..2)
                .map(|_| ShardConfig {
                    store: StoreSpec::Memory(MemorySpec::default()),
                    weight: Some(1),
                })
                .collect(),
        },
        vec![Store::new(live_store), Store::new(dead_store)],
    )?;
    Ok(Ring {
        shard_store,
        live_shard: 0,
        live_blobs,
        dead_port,
    })
}

// A read routed to the dead shard must settle with an error (which the
// retry machinery upstream can handle) instead of hanging forever.
#[nativelink_test]
async fn read_routed_to_dead_shard_errors_instead_of_hanging() -> Result<(), Error> {
    let ring = make_ring().await?;
    let (dead_digest, _) = digest_for_shard(1 - ring.live_shard);

    let result = timeout(
        GENEROUS_TIMEOUT,
        ring.shard_store.get_part_unchunked(dead_digest, 0, None),
    )
    .await
    .expect("read routed to the dead shard hung past the generous timeout: the shard-ring wedge");
    let err = result.expect_err("read from a dead shard cannot succeed");
    assert!(
        matches!(err.code, Code::Unavailable | Code::DeadlineExceeded),
        "expected Unavailable/DeadlineExceeded from the dead shard, got: {err:?}",
    );
    Ok(())
}

// The production symptom: a multi-key existence check (FindMissingBlobs)
// fans out to every shard, so ONE dead peer wedged EVERY CAS RPC through
// the frontend. With read failover the dead shard's bucket is retried on
// the live shard, so the whole call succeeds instead of erroring.
#[nativelink_test]
async fn has_with_dead_shard_fails_over_instead_of_erroring() -> Result<(), Error> {
    let ring = make_ring().await?;
    let (live_digest, live_content) = digest_for_shard(ring.live_shard);
    let (dead_digest, dead_content) = digest_for_shard(1 - ring.live_shard);
    // Both blobs are present on the live shard (as a replica would be).
    {
        let mut blobs = ring.live_blobs.lock().await;
        blobs.insert(live_digest.packed_hash().to_string(), live_content.clone());
        blobs.insert(dead_digest.packed_hash().to_string(), dead_content.clone());
    }

    let results = timeout(
        GENEROUS_TIMEOUT,
        ring.shard_store
            .has_many(&[live_digest.into(), dead_digest.into()]),
    )
    .await
    .expect("multi-key has() with a dead shard hung past the generous timeout")?;
    assert_eq!(
        results,
        vec![
            Some(live_content.len() as u64),
            Some(dead_content.len() as u64)
        ],
        "both keys must resolve: the dead shard's bucket via failover",
    );
    Ok(())
}

// A single-key has() routed to the dead shard must fail over to the live
// shard that holds the blob instead of surfacing Unavailable.
#[nativelink_test]
async fn has_routed_to_dead_shard_fails_over_to_live_replica() -> Result<(), Error> {
    let ring = make_ring().await?;
    let (dead_digest, dead_content) = digest_for_shard(1 - ring.live_shard);
    ring.live_blobs
        .lock()
        .await
        .insert(dead_digest.packed_hash().to_string(), dead_content.clone());

    let result = timeout(GENEROUS_TIMEOUT, ring.shard_store.has(dead_digest))
        .await
        .expect("single-key has() routed to the dead shard hung")?;
    assert_eq!(
        result,
        Some(dead_content.len() as u64),
        "has() must find the blob on the live shard via failover",
    );
    Ok(())
}

// A read routed to the dead shard must fail over to the live shard that
// holds a replica of the blob and return its content.
#[nativelink_test]
async fn get_part_routed_to_dead_shard_fails_over_to_live_replica() -> Result<(), Error> {
    let ring = make_ring().await?;
    let (dead_digest, dead_content) = digest_for_shard(1 - ring.live_shard);
    ring.live_blobs
        .lock()
        .await
        .insert(dead_digest.packed_hash().to_string(), dead_content.clone());

    let data = timeout(
        GENEROUS_TIMEOUT,
        ring.shard_store.get_part_unchunked(dead_digest, 0, None),
    )
    .await
    .expect("read routed to the dead shard hung")?;
    assert_eq!(
        data, dead_content,
        "failover read must return the live shard's replica",
    );
    Ok(())
}

// Health recovery: after enough consecutive failures the dead shard is
// marked suspect and skipped; once the peer comes back and the cooldown
// elapses it is probed again and serves its own keys directly.
#[nativelink_test]
async fn dead_shard_recovers_after_cooldown() -> Result<(), Error> {
    let ring = make_ring().await?;
    let (dead_digest, dead_content) = digest_for_shard(1 - ring.live_shard);

    // Trip the suspect threshold: each read fails over to the live shard
    // (which lacks the blob), so the call errors, but each attempt counts
    // a connectivity failure against the dead shard.
    for _ in 0..3 {
        let result = timeout(
            GENEROUS_TIMEOUT,
            ring.shard_store.get_part_unchunked(dead_digest, 0, None),
        )
        .await
        .expect("read with dead shard hung");
        assert!(result.is_err(), "blob exists nowhere yet; read must fail");
    }

    // Revive the dead peer on its original port, now holding the blob.
    let (_, revived_blobs) = spawn_live_peer_on(ring.dead_port);
    revived_blobs
        .lock()
        .await
        .insert(dead_digest.packed_hash().to_string(), dead_content.clone());

    // Wait out the suspect cooldown so the shard is probed again.
    tokio::time::sleep(Duration::from_secs(6)).await;

    let data = timeout(
        GENEROUS_TIMEOUT,
        ring.shard_store.get_part_unchunked(dead_digest, 0, None),
    )
    .await
    .expect("read after recovery hung")?;
    assert_eq!(data, dead_content, "revived shard must serve its own key");

    // The success must have reset the failure counter: an immediate
    // follow-up (well inside any cooldown) is also served directly.
    let result = timeout(GENEROUS_TIMEOUT, ring.shard_store.has(dead_digest))
        .await
        .expect("has() after recovery hung")?;
    assert_eq!(result, Some(dead_content.len() as u64));
    Ok(())
}

// The live shard must keep serving while its ring-mate is dead: routing
// around the dead peer stays possible for keys that don't touch it.
#[nativelink_test]
async fn live_shard_still_serves_while_peer_dead() -> Result<(), Error> {
    let ring = make_ring().await?;
    let (live_digest, content) = digest_for_shard(ring.live_shard);

    timeout(
        GENEROUS_TIMEOUT,
        ring.shard_store
            .update_oneshot(live_digest, content.clone()),
    )
    .await
    .expect("write to the live shard hung")?;
    assert!(
        ring.live_blobs
            .lock()
            .await
            .contains_key(&live_digest.packed_hash().to_string()),
        "blob did not land on the live peer",
    );

    let data = timeout(
        GENEROUS_TIMEOUT,
        ring.shard_store.get_part_unchunked(live_digest, 0, None),
    )
    .await
    .expect("read from the live shard hung")?;
    assert_eq!(data, content, "live shard returned wrong content");
    Ok(())
}
