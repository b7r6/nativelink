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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;
use nativelink_error::{Error, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::{
    DirectoryNode, FindMissingBlobsRequest, GetTreeRequest, GetTreeResponse, NodeProperties,
    NodeProperty, compressor,
};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{RemoveCallback, StoreDriver, StoreKey, UploadSizeInfo};

use super::*;

const CONTENT: usize = 64 * 1024;
const ALGORITHMS: [DigestHasherFunc; 2] = [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3];

#[derive(Clone, Debug)]
enum Finish {
    Eof,
    Drop,
    Pending,
    Error(Code),
}

#[derive(Clone, Debug)]
struct Value {
    chunks: Vec<Bytes>,
    finish: Finish,
}

#[derive(Debug, Default, MetricsComponent)]
struct Spy {
    values: Mutex<HashMap<DigestInfo, Value>>,
    gets: AtomicUsize,
    writes: AtomicUsize,
    has: AtomicUsize,
    cancelled: AtomicUsize,
}

impl Spy {
    fn put(&self, algorithm: DigestHasherFunc, data: Bytes) -> Digest {
        let digest = hash(algorithm, &data);
        self.set(&digest, vec![data], Finish::Eof);
        digest
    }

    fn set(&self, digest: &Digest, chunks: Vec<Bytes>, finish: Finish) {
        self.values.lock().unwrap().insert(
            DigestInfo::try_from(digest).unwrap(),
            Value { chunks, finish },
        );
    }

    fn directory(&self, algorithm: DigestHasherFunc, directory: &Directory) -> Digest {
        self.put(algorithm, directory.encode_to_vec().into())
    }

    fn calls(&self) -> (usize, usize, usize) {
        (
            self.gets.load(Ordering::Relaxed),
            self.writes.load(Ordering::Relaxed),
            self.has.load(Ordering::Relaxed),
        )
    }
}

struct Cancelled<'a>(&'a AtomicUsize);
impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tonic::async_trait]
impl StoreDriver for Spy {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.has.fetch_add(1, Ordering::Relaxed);
        let values = self.values.lock().unwrap();
        for (key, result) in keys.iter().zip(results) {
            let StoreKey::Digest(digest) = key else {
                panic!("digest key required")
            };
            *result = values
                .get(digest)
                .map(|value| value.chunks.iter().map(|c| c.len() as u64).sum());
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let StoreKey::Digest(digest) = key else {
            panic!("digest key required")
        };
        let data = reader.consume(None).await?;
        let size = data.len() as u64;
        self.values.lock().unwrap().insert(
            digest,
            Value {
                chunks: vec![data],
                finish: Finish::Eof,
            },
        );
        Ok(size)
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        assert_eq!(offset, 0);
        let StoreKey::Digest(digest) = key else {
            panic!("digest key required")
        };
        let value = self
            .values
            .lock()
            .unwrap()
            .get(&digest)
            .cloned()
            .ok_or_else(|| make_err!(Code::NotFound, "missing fixture blob"))?;
        if let Some(length) = length {
            let data = Bytes::from(value.chunks.concat());
            let end = usize::try_from(length).unwrap().min(data.len());
            if end != 0 {
                writer.send(data.slice(..end)).await?;
            }
            return writer.send_eof();
        }
        for chunk in value.chunks {
            if !chunk.is_empty() {
                writer.send(chunk).await?;
            }
        }
        match value.finish {
            Finish::Eof => writer.send_eof(),
            Finish::Drop => Ok(()),
            Finish::Error(code) => Err(make_err!(code, "fixture read failed")),
            Finish::Pending => {
                let _cancelled = Cancelled(&self.cancelled);
                futures::future::pending().await
            }
        }
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }
    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }
    fn register_remove_callback(self: Arc<Self>, _callback: RemoveCallback) -> Result<(), Error> {
        Ok(())
    }
}

default_health_status_indicator!(Spy);

async fn fixture() -> (Runtime, Arc<Spy>) {
    let spy = Arc::new(Spy::default());
    let runtime = Runtime::start_with_store_and_transports(
        Store::new(spy.clone()),
        Server::builder(),
        Server::builder(),
        None,
        None,
    )
    .await;
    (runtime, spy)
}

fn hash(algorithm: DigestHasherFunc, bytes: &[u8]) -> Digest {
    let mut hasher = algorithm.hasher();
    hasher.update(bytes);
    hasher.finalize_digest().into()
}

fn upload(algorithm: DigestHasherFunc, data: Bytes) -> batch_update_blobs_request::Request {
    batch_update_blobs_request::Request {
        digest: Some(hash(algorithm, &data)),
        data,
        compressor: 0,
    }
}

fn update_request(
    algorithm: DigestHasherFunc,
    requests: Vec<batch_update_blobs_request::Request>,
) -> BatchUpdateBlobsRequest {
    BatchUpdateBlobsRequest {
        instance_name: INSTANCE.into(),
        requests,
        digest_function: algorithm.proto_digest_func() as i32,
    }
}

fn read_request(algorithm: DigestHasherFunc, digests: Vec<Digest>) -> BatchReadBlobsRequest {
    BatchReadBlobsRequest {
        instance_name: INSTANCE.into(),
        digests,
        digest_function: algorithm.proto_digest_func() as i32,
        acceptable_compressors: vec![],
    }
}

fn tree_request(
    algorithm: DigestHasherFunc,
    root: &Digest,
    size: i32,
    token: &str,
) -> GetTreeRequest {
    GetTreeRequest {
        instance_name: INSTANCE.into(),
        root_digest: Some(root.clone()),
        page_size: size,
        page_token: token.into(),
        digest_function: algorithm.proto_digest_func() as i32,
    }
}

async fn tree(
    client: &mut ContentAddressableStorageClient<Channel>,
    request: GetTreeRequest,
) -> Result<GetTreeResponse, tonic::Status> {
    let mut response = client.get_tree(request).await?.into_inner();
    let page = response.message().await?.expect("one bounded page");
    assert!(response.message().await?.is_none());
    Ok(page)
}

#[nativelink_test]
async fn fixed_batch_admission_precedes_all_store_calls_both_roles_and_hashes() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        for channel in [runtime.worker.clone(), runtime.client.clone()] {
            let mut client = ContentAddressableStorageClient::new(channel);
            let empty = upload(algorithm, Bytes::new());
            let too_much = upload(algorithm, vec![1; CONTENT + 1].into());
            let mut false_declared = empty.clone();
            false_declared.digest.as_mut().unwrap().size_bytes =
                i64::try_from(CONTENT).unwrap() + 1;
            let mut false_actual = too_much.clone();
            false_actual.digest.as_mut().unwrap().size_bytes = 0;
            for entries in [
                vec![empty.clone(); 65],
                vec![too_much],
                vec![false_declared],
                vec![false_actual],
            ] {
                let before = spy.calls();
                let request = update_request(algorithm, entries);
                assert!(request.encoded_len() < 1024 * 1024);
                assert_eq!(
                    client.batch_update_blobs(request).await.unwrap_err().code(),
                    Code::InvalidArgument
                );
                assert_eq!(spy.calls(), before);
            }
            for digests in [
                vec![hash(algorithm, b""); 65],
                vec![Digest {
                    hash: "a".repeat(64),
                    size_bytes: i64::try_from(CONTENT).unwrap() + 1,
                }],
                vec![
                    Digest {
                        hash: "b".repeat(64),
                        size_bytes: i64::MAX
                    };
                    3
                ],
            ] {
                let before = spy.calls();
                assert_eq!(
                    client
                        .batch_read_blobs(read_request(algorithm, digests))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::InvalidArgument
                );
                assert_eq!(spy.calls(), before);
            }
            let before = spy.calls();
            assert_eq!(
                client
                    .find_missing_blobs(FindMissingBlobsRequest {
                        instance_name: INSTANCE.into(),
                        blob_digests: vec![hash(algorithm, b""); 65],
                        digest_function: algorithm.proto_digest_func() as i32
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
            assert_eq!(spy.calls(), before);
            let data = Bytes::from(vec![37; CONTENT]);
            let response = client
                .batch_update_blobs(update_request(
                    algorithm,
                    vec![upload(algorithm, data.clone())],
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.responses[0].status.as_ref().unwrap().code, 0);
            let response = client
                .batch_read_blobs(read_request(algorithm, vec![hash(algorithm, &data)]))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.responses[0].data, data);
            assert_eq!(response.responses[0].status.as_ref().unwrap().code, 0);
            let response = client
                .batch_update_blobs(update_request(algorithm, vec![empty; 64]))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.responses.len(), 64);
            assert!(
                response
                    .responses
                    .iter()
                    .all(|entry| entry.status.as_ref().unwrap().code == 0)
            );
        }
    }
    let capabilities = CapabilitiesClient::new(runtime.client.clone())
        .get_capabilities(GetCapabilitiesRequest {
            instance_name: INSTANCE.into(),
        })
        .await
        .unwrap()
        .into_inner()
        .cache_capabilities
        .unwrap();
    assert_eq!(
        capabilities.max_batch_total_size_bytes,
        i64::try_from(CONTENT).unwrap()
    );
    assert_eq!(capabilities.max_cas_blob_size_bytes, 0);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn fixed_batches_preserve_independent_errors_and_disabled_compression() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        let mut client = ContentAddressableStorageClient::new(runtime.client.clone());
        let valid = upload(algorithm, Bytes::from_static(b"good"));
        let mut wrong_hash = valid.clone();
        wrong_hash.data = Bytes::from_static(b"evil");
        let mut compressed = valid.clone();
        compressed.compressor = compressor::Value::Zstd as i32;
        let missing = batch_update_blobs_request::Request {
            digest: None,
            data: Bytes::new(),
            compressor: 0,
        };
        let before = spy.calls();
        let response = client
            .batch_update_blobs(update_request(
                algorithm,
                vec![valid.clone(), wrong_hash, compressed, missing],
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response
                .responses
                .iter()
                .map(|entry| entry.status.as_ref().unwrap().code)
                .collect::<Vec<_>>(),
            vec![0, 3, 3, 3]
        );
        assert!(
            response.responses[2]
                .status
                .as_ref()
                .unwrap()
                .message
                .contains("Remote cache compression is not supported by this instance")
        );
        assert_eq!(spy.calls(), (before.0, before.1 + 1, before.2));
        let absent = hash(algorithm, b"absent");
        let failed = hash(algorithm, b"failed");
        spy.set(&failed, vec![], Finish::Error(Code::Unavailable));
        let malformed = Digest {
            hash: "invalid".into(),
            size_bytes: -1,
        };
        let response = client
            .batch_read_blobs(read_request(
                algorithm,
                vec![valid.digest.unwrap(), absent, failed, malformed],
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            response
                .responses
                .iter()
                .map(|entry| entry.status.as_ref().unwrap().code)
                .collect::<Vec<_>>(),
            vec![0, 5, 14, 3]
        );
        assert_eq!(response.responses[0].data, b"good".as_slice());
        assert!(
            response.responses[1..]
                .iter()
                .all(|entry| entry.data.is_empty())
        );
    }
    runtime.shutdown().await;
}

#[nativelink_test]
async fn fixed_reads_require_whole_verified_objects_and_cancel_excess_producers() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        for channel in [runtime.worker.clone(), runtime.client.clone()] {
            let mut client = ContentAddressableStorageClient::new(channel);
            let digest = hash(algorithm, b"right");
            for (chunks, finish, expected) in [
                (
                    vec![Bytes::from_static(b"wrong")],
                    Finish::Eof,
                    Code::DataLoss,
                ),
                (
                    vec![Bytes::from_static(b"right").slice(..4)],
                    Finish::Eof,
                    Code::DataLoss,
                ),
                (
                    vec![Bytes::from_static(b"right"), Bytes::from_static(b"extra")],
                    Finish::Eof,
                    Code::DataLoss,
                ),
                (
                    vec![Bytes::from_static(b"right")],
                    Finish::Drop,
                    Code::Internal,
                ),
                (
                    vec![Bytes::from(vec![0; CONTENT + 1])],
                    Finish::Pending,
                    Code::DataLoss,
                ),
            ] {
                let pending = matches!(finish, Finish::Pending);
                let before_cancel = spy.cancelled.load(Ordering::Relaxed);
                spy.set(&digest, chunks, finish);
                let result = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.batch_read_blobs(read_request(algorithm, vec![digest.clone()])),
                )
                .await
                .unwrap()
                .unwrap()
                .into_inner();
                assert_eq!(
                    result.responses[0].status.as_ref().unwrap().code,
                    expected as i32
                );
                assert!(result.responses[0].data.is_empty());
                if pending {
                    assert_eq!(spy.cancelled.load(Ordering::Relaxed), before_cancel + 1);
                }
            }
            // A valid protobuf prefix is not the complete digest-bearing object.
            let directory = Directory::default();
            let root = spy.directory(algorithm, &directory);
            spy.set(&root, vec![Bytes::from_static(&[0x28, 1])], Finish::Eof);
            assert_eq!(
                tree(&mut client, tree_request(algorithm, &root, 0, ""))
                    .await
                    .unwrap_err()
                    .code(),
                Code::DataLoss
            );
            // Valid prefix plus missing suffix must fail before Directory decode.
            let complete = Directory {
                node_properties: Some(NodeProperties::default()),
                ..Directory::default()
            }
            .encode_to_vec();
            let root = hash(algorithm, &complete);
            spy.set(&root, vec![], Finish::Eof);
            assert_eq!(
                tree(&mut client, tree_request(algorithm, &root, 0, ""))
                    .await
                    .unwrap_err()
                    .code(),
                Code::DataLoss
            );
        }
    }
    runtime.shutdown().await;
}

#[nativelink_test]
async fn fixed_reads_reject_extra_tail_even_when_range_prefix_is_valid() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        let digest = hash(algorithm, b"right");
        spy.set(
            &digest,
            vec![Bytes::from_static(b"right-tail")],
            Finish::Eof,
        );
        // The store honors ranges. Its first five bytes really match the key,
        // so hashing that prefix alone would incorrectly accept the object.
        for channel in [runtime.worker.clone(), runtime.client.clone()] {
            let response = ContentAddressableStorageClient::new(channel)
                .batch_read_blobs(read_request(algorithm, vec![digest.clone()]))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response.responses[0].status.as_ref().unwrap().code,
                Code::DataLoss as i32
            );
            assert!(response.responses[0].data.is_empty());
        }
    }
    runtime.shutdown().await;
}

fn children(digest: &Digest, count: usize) -> Directory {
    Directory {
        directories: (0..count)
            .map(|index| DirectoryNode {
                name: format!("child-{index:03}"),
                digest: Some(digest.clone()),
            })
            .collect(),
        ..Directory::default()
    }
}

#[nativelink_test]
async fn fixed_tree_pagination_advances_repeated_dag_occurrences_and_binds_tokens() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        for channel in [runtime.worker.clone(), runtime.client.clone()] {
            let mut client = ContentAddressableStorageClient::new(channel);
            let leaf = Directory::default();
            let child = spy.directory(algorithm, &leaf);
            let root_directory = children(&child, 2);
            let root = spy.directory(algorithm, &root_directory);
            let first = tree(&mut client, tree_request(algorithm, &root, 1, ""))
                .await
                .unwrap();
            assert_eq!(first.directories, vec![root_directory.clone()]);
            assert_ne!(first.next_page_token, "");
            let second = tree(
                &mut client,
                tree_request(algorithm, &root, 1, &first.next_page_token),
            )
            .await
            .unwrap();
            assert_eq!(second.directories, vec![leaf.clone()]);
            assert_ne!(second.next_page_token, first.next_page_token);
            let third = tree(
                &mut client,
                tree_request(algorithm, &root, 1, &second.next_page_token),
            )
            .await
            .unwrap();
            assert_eq!(third.directories, vec![leaf.clone()]);
            assert_eq!(third.next_page_token, "");
            let zero = tree(
                &mut client,
                tree_request(algorithm, &root, 0, &first.next_page_token),
            )
            .await
            .unwrap();
            assert_eq!(zero.directories, vec![leaf.clone(), leaf]);
            assert_eq!(zero.next_page_token, "");
            assert_eq!(
                tree(
                    &mut client,
                    tree_request(algorithm, &root, 1, &first.next_page_token)
                )
                .await
                .unwrap(),
                second
            );
            let prefix = first.next_page_token.rsplit_once('.').unwrap().0;
            assert_eq!(
                tree(
                    &mut client,
                    tree_request(algorithm, &root, 1, &format!("{prefix}.3"))
                )
                .await
                .unwrap_err()
                .code(),
                Code::InvalidArgument
            );
            for token in [
                format!("{}.extra", first.next_page_token),
                first.next_page_token.replace("v1.", "v2."),
                "x".repeat(161),
            ] {
                let before = spy.calls();
                assert_eq!(
                    tree(&mut client, tree_request(algorithm, &root, 1, &token))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::InvalidArgument
                );
                assert_eq!(spy.calls(), before);
            }
            let before = spy.calls();
            assert_eq!(
                tree(
                    &mut client,
                    tree_request(algorithm, &child, 1, &first.next_page_token)
                )
                .await
                .unwrap_err()
                .code(),
                Code::InvalidArgument
            );
            let other = if algorithm == DigestHasherFunc::Sha256 {
                DigestHasherFunc::Blake3
            } else {
                DigestHasherFunc::Sha256
            };
            assert_eq!(
                tree(
                    &mut client,
                    tree_request(other, &root, 1, &first.next_page_token)
                )
                .await
                .unwrap_err()
                .code(),
                Code::InvalidArgument
            );
            let mut wrong_instance = tree_request(algorithm, &root, 1, &first.next_page_token);
            wrong_instance.instance_name = "other-instance".into();
            assert_eq!(
                tree(&mut client, wrong_instance).await.unwrap_err().code(),
                Code::InvalidArgument
            );
            assert_eq!(spy.calls(), before);
        }
    }
    runtime.shutdown().await;
}

#[nativelink_test]
async fn fixed_grpc_store_reads_do_not_trust_forwarded_cas_responses() {
    use nativelink_config::cas_server::{ByteStreamConfig, CasStoreConfig, WithInstanceName};
    use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
    use nativelink_service::bytestream_server::ByteStreamServer;
    use nativelink_service::cas_server::CasServer;
    use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
    use nativelink_store::grpc_store::GrpcStore;
    use nativelink_store::store_manager::StoreManager;
    use nativelink_util::store_trait::StoreLike;

    // Legacy upstream deliberately returns values without digest verification.
    // This control must succeed there and fail through the fixed outer route.
    let memory = MemoryStore::new(&MemorySpec::default());
    let storage = Store::new(memory);
    let manager = StoreManager::new();
    manager.add_store("upstream", storage.clone()).unwrap();
    let compression = RemoteCacheCompressionInstances::default();
    let cas = CasServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE.into(),
            config: CasStoreConfig {
                cas_store: "upstream".into(),
                experimental_chunking: None,
            },
        }],
        &manager,
        &compression,
    )
    .unwrap();
    let bytestream = ByteStreamServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE.into(),
            config: ByteStreamConfig {
                cas_store: "upstream".into(),
                ..ByteStreamConfig::default()
            },
        }],
        &manager,
        &compression,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = background_spawn!("unverified_cas_upstream", async move {
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        Server::builder()
            .add_service(cas.into_service())
            .add_service(bytestream.into_service())
            .serve_with_incoming_shutdown(incoming, async {
                let _stopped = stopped.await;
            })
            .await
    });
    let spec = GrpcSpec {
        instance_name: INSTANCE.into(),
        endpoints: vec![GrpcEndpoint {
            address: format!("http://{address}"),
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
        connections_per_endpoint: 0,
        rpc_timeout_s: 5,
        use_legacy_resource_names: false,
        headers: HashMap::new(),
        forward_headers: vec![],
        experimental_read_batching: None,
        experimental_remote_cache_compression: Some(false),
    };
    let runtime = Runtime::start_with_store_and_transports(
        Store::new(GrpcStore::new(&spec).unwrap()),
        Server::builder(),
        Server::builder(),
        None,
        None,
    )
    .await;
    let mut upstream =
        ContentAddressableStorageClient::new(endpoint(address, None).connect().await.unwrap());
    for algorithm in ALGORITHMS {
        let digest = hash(algorithm, b"right");
        storage
            .update_oneshot(
                DigestInfo::try_from(&digest).unwrap(),
                Bytes::from_static(b"wrong"),
            )
            .await
            .unwrap();
        let unverified = upstream
            .batch_read_blobs(read_request(algorithm, vec![digest.clone()]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(unverified.responses[0].status.as_ref().unwrap().code, 0);
        assert_eq!(unverified.responses[0].data, b"wrong".as_slice());
        let directory = Directory {
            node_properties: Some(NodeProperties::default()),
            ..Directory::default()
        };
        let root = hash(algorithm, b"x");
        storage
            .update_oneshot(
                DigestInfo::try_from(&root).unwrap(),
                directory.encode_to_vec().into(),
            )
            .await
            .unwrap();
        assert_eq!(
            tree(&mut upstream, tree_request(algorithm, &root, 0, ""))
                .await
                .unwrap()
                .directories,
            vec![directory]
        );
        for channel in [runtime.worker.clone(), runtime.client.clone()] {
            let mut client = ContentAddressableStorageClient::new(channel);
            let response = client
                .batch_read_blobs(read_request(algorithm, vec![digest.clone()]))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                response.responses[0].status.as_ref().unwrap().code,
                Code::DataLoss as i32
            );
            assert!(response.responses[0].data.is_empty());
            assert_eq!(
                tree(&mut client, tree_request(algorithm, &root, 0, ""))
                    .await
                    .unwrap_err()
                    .code(),
                Code::DataLoss
            );
            let oversized = upload(algorithm, Bytes::from(vec![0; CONTENT + 1]));
            assert_eq!(
                client
                    .batch_update_blobs(update_request(algorithm, vec![oversized]))
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        }
    }
    runtime.shutdown().await;
    drop(upstream);
    let _stopped = stop.send(());
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[nativelink_test]
async fn fixed_tree_budgets_cover_whole_walk_frontier_and_missing_attempts() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        let mut client = ContentAddressableStorageClient::new(runtime.client.clone());
        let leaf = spy.directory(algorithm, &Directory::default());
        // A complete Directory with a large unknown protobuf field: its exact
        // stored bytes, including that field, must pass whole-object hashing.
        let mut exact_bytes = vec![0xfa, 0x07, 0xfb, 0xff, 0x03];
        exact_bytes.resize(CONTENT, 0);
        assert_eq!(
            Directory::decode(exact_bytes.as_slice()).unwrap(),
            Directory::default()
        );
        let exact_bytes = spy.put(algorithm, exact_bytes.into());
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &exact_bytes, 0, ""))
                .await
                .unwrap()
                .directories,
            vec![Directory::default()]
        );
        let exact = spy.directory(algorithm, &children(&leaf, 63));
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &exact, i32::MAX, ""))
                .await
                .unwrap()
                .directories
                .len(),
            64
        );
        let excessive = spy.directory(algorithm, &children(&leaf, 64));
        let before = spy.calls();
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &excessive, 1, ""))
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            spy.calls().0,
            before.0 + 1,
            "frontier refused before child reads"
        );
        let absent = hash(algorithm, b"missing Directory");
        let missing_broad = spy.directory(algorithm, &children(&absent, 64));
        let before = spy.calls();
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &missing_broad, 1, ""))
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert_eq!(spy.calls().0, before.0 + 1);
        let oversized = Digest {
            hash: "c".repeat(64),
            size_bytes: i64::try_from(CONTENT).unwrap() + 1,
        };
        let before = spy.calls();
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &oversized, 0, ""))
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &leaf, -1, ""))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(spy.calls(), before);
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &absent, 0, ""))
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        let valid_directory = Directory {
            node_properties: Some(NodeProperties {
                properties: vec![NodeProperty {
                    name: "padding".into(),
                    value: "x".repeat(CONTENT / 2),
                }],
                ..NodeProperties::default()
            }),
            ..Directory::default()
        };
        let large = spy.directory(algorithm, &valid_directory);
        let repeated_large = spy.directory(algorithm, &children(&large, 2));
        let before = spy.calls();
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &repeated_large, 1, ""))
                .await
                .unwrap_err()
                .code(),
            Code::ResourceExhausted
        );
        assert_eq!(
            spy.calls().0,
            before.0 + 1,
            "aggregate bytes charged before child reads"
        );
    }
    runtime.shutdown().await;
}

#[nativelink_test]
async fn fixed_tree_omits_missing_descendants_and_refuses_changed_continuations() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        let mut client = ContentAddressableStorageClient::new(runtime.client.clone());
        let leaf = spy.directory(algorithm, &Directory::default());
        let later_directory = Directory {
            node_properties: Some(NodeProperties::default()),
            ..Directory::default()
        };
        let later = hash(algorithm, &later_directory.encode_to_vec());
        let root_directory = Directory {
            directories: vec![
                DirectoryNode {
                    name: "a".into(),
                    digest: Some(later.clone()),
                },
                DirectoryNode {
                    name: "b".into(),
                    digest: Some(leaf),
                },
            ],
            ..Directory::default()
        };
        let root = spy.directory(algorithm, &root_directory);
        let first = tree(&mut client, tree_request(algorithm, &root, 1, ""))
            .await
            .unwrap();
        assert_eq!(first.directories, vec![root_directory.clone()]);
        let terminal = tree(
            &mut client,
            tree_request(algorithm, &root, 0, &first.next_page_token),
        )
        .await
        .unwrap();
        assert_eq!(terminal.directories, vec![Directory::default()]);
        assert_eq!(terminal.next_page_token, "");
        spy.directory(algorithm, &later_directory);
        assert_eq!(
            tree(
                &mut client,
                tree_request(algorithm, &root, 0, &first.next_page_token)
            )
            .await
            .unwrap_err()
            .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &root, 0, ""))
                .await
                .unwrap()
                .directories,
            vec![root_directory, later_directory, Directory::default()]
        );
        spy.set(&later, vec![], Finish::Error(Code::Unavailable));
        assert_eq!(
            tree(&mut client, tree_request(algorithm, &root, 0, ""))
                .await
                .unwrap_err()
                .code(),
            Code::Unavailable
        );
    }
    runtime.shutdown().await;
}

#[nativelink_test]
async fn fixed_combined_batch_content_counts_duplicates_and_both_upload_sizes() {
    let (runtime, spy) = fixture().await;
    for algorithm in ALGORITHMS {
        for channel in [runtime.worker.clone(), runtime.client.clone()] {
            let mut client = ContentAddressableStorageClient::new(channel);
            let entry = upload(algorithm, Bytes::from(vec![41; 40 * 1024]));
            let mut actual_only = entry.clone();
            actual_only.digest.as_mut().unwrap().size_bytes = 0;
            let mut declared_only = entry.clone();
            declared_only.data = Bytes::new();
            for duplicate in [entry.clone(), actual_only, declared_only] {
                let before = spy.calls();
                let response = client
                    .batch_update_blobs(update_request(
                        algorithm,
                        vec![duplicate.clone(), duplicate],
                    ))
                    .await;
                assert_eq!(response.unwrap_err().code(), Code::InvalidArgument);
                assert_eq!(spy.calls(), before);
            }
            let digest = spy.put(algorithm, entry.data);
            let before = spy.calls();
            assert_eq!(
                client
                    .batch_read_blobs(read_request(algorithm, vec![digest.clone(), digest],))
                    .await
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
            assert_eq!(spy.calls(), before);

            let exact = upload(algorithm, Bytes::from(vec![32; 32 * 1024]));
            let digest = exact.digest.clone().unwrap();
            let updated = client
                .batch_update_blobs(update_request(
                    algorithm,
                    vec![exact.clone(), exact.clone()],
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(updated.responses.len(), 2);
            assert!(
                updated
                    .responses
                    .iter()
                    .all(|entry| entry.status.as_ref().unwrap().code == 0)
            );
            let read = client
                .batch_read_blobs(read_request(algorithm, vec![digest.clone(), digest]))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(read.responses.len(), 2);
            assert!(
                read.responses
                    .iter()
                    .all(|entry| entry.status.as_ref().unwrap().code == 0
                        && entry.data == exact.data)
            );
        }
    }
    runtime.shutdown().await;
}
