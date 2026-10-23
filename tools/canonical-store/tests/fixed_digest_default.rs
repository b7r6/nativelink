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

//! A separate executable initializes the process default before any runtime or
//! fixture can read its `OnceLock`. All services below use actual loopback HTTP/2.
//! This qualifies digest selection, not TLS or canonical journal construction.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use nativelink_canonical_config::parse_canonical;
use nativelink_canonical_filesystem::{FixedLayout, Ownership};
use nativelink_canonical_store::CanonicalStores;
use nativelink_config::cas_server::{
    AcStoreConfig, ByteStreamConfig, CasStoreConfig, WithInstanceName,
};
use nativelink_config::stores::{GrpcEndpoint, GrpcSpec, Retry, StoreType};
use nativelink_proto::build::bazel::remote::execution::v2::action_cache_client::ActionCacheClient;
use nativelink_proto::build::bazel::remote::execution::v2::content_addressable_storage_client::ContentAddressableStorageClient;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, BatchReadBlobsRequest, BatchUpdateBlobsRequest, Command, Directory,
    ExecuteRequest, ExecuteResponse, FindMissingBlobsRequest, GetActionResultRequest,
    GetTreeRequest, UpdateActionResultRequest, batch_update_blobs_request,
};
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::{QueryWriteStatusRequest, ReadRequest, WriteRequest};
use nativelink_proto::google::longrunning::operation;
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::{ClusterEpoch, Limits};
use nativelink_service::ac_server::AcServer;
use nativelink_service::authority_listener::AuthorityListeners;
use nativelink_service::authority_server::{
    CoordinatorTask, QualificationConfig, QualificationFaults,
};
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_store::grpc_store::GrpcStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::background_spawn;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasher, DigestHasherFunc, default_digest_hasher_func, set_default_digest_hasher_func,
};
use nativelink_util::store_trait::Store;
use prost::Message;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tonic::transport::{Channel, Endpoint, Server};

const INSTANCE: &str = "digest-default";
const SHA: DigestHasherFunc = DigestHasherFunc::Sha256;
const B3: DigestHasherFunc = DigestHasherFunc::Blake3;
const FIXED_CHUNK: usize = 64 * 1024;
const UPSTREAM_CHUNK: usize = 256 * 1024;
const PHASES: [&str; 15] = [
    "cas-update",
    "cas-read",
    "cas-find",
    "cas-tree",
    "ac-get",
    "ac-update",
    "bs-read",
    "bs-write",
    "bs-query",
    "execute",
    "legacy",
    "remote-read",
    "remote-write",
    "remote-query",
    "remote-chunks",
];

struct Fixture {
    directory: PathBuf,
    stores: CanonicalStores,
}

impl Fixture {
    async fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("fixed-digest-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let mut manifest = parse_canonical(include_bytes!(
            "../../canonical-config/fixtures/canonical.json"
        ))
        .unwrap();
        let roots = ["journal", "cas", "sink", "worker"].map(|name| {
            let path = directory.join(name);
            fs::create_dir(&path).unwrap();
            path.to_str().unwrap().to_owned()
        });
        manifest.authority.journal_root.clone_from(&roots[0]);
        manifest.cas.root.clone_from(&roots[1]);
        manifest.temporary_action_cache.root.clone_from(&roots[2]);
        manifest.worker.root.clone_from(&roots[3]);
        let ownership = Arc::new(Ownership::claim_cold(&manifest).unwrap());
        let layout = Arc::new(FixedLayout::create(&ownership).unwrap());
        Self {
            directory,
            stores: CanonicalStores::from_layout(layout).await.unwrap(),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

struct Rpc {
    channel: Channel,
    address: String,
    stops: Vec<oneshot::Sender<()>>,
    tasks: Vec<JoinHandle<()>>,
    coordinator: Option<CoordinatorTask>,
}

impl Rpc {
    async fn fixed(fixture: &Fixture, cas: Store, label: &str) -> Self {
        let authority = Singleton::open(
            fixture.directory.join(format!("{label}.redb")),
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
                principal: "digest-default-test".to_owned(),
                cas,
                cas_namespace: [1; 32],
                publication_sink: Store::new(fixture.stores.sink.clone()),
                now: Arc::new(|| Ok(1)),
                faults: Arc::new(QualificationFaults::default()),
            },
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = background_spawn!("fixed_digest_listener", async move {
            listeners
                .client
                .serve(Server::builder(), listener, async {
                    let _stopped = stopped.await;
                })
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(address.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            channel,
            address,
            stops: vec![stop],
            tasks: vec![task],
            coordinator: Some(listeners.coordinator),
        }
    }

    async fn legacy(cas: Store, sink: Store) -> Self {
        let manager = StoreManager::new();
        manager.add_store("cas", cas).unwrap();
        manager.add_store("sink", sink).unwrap();
        manager.run_post_init().await.unwrap();
        let compression = RemoteCacheCompressionInstances::default();
        let cas = CasServer::new(
            &[WithInstanceName {
                instance_name: INSTANCE.to_owned(),
                config: CasStoreConfig {
                    cas_store: "cas".to_owned(),
                    experimental_chunking: None,
                },
            }],
            &manager,
            &compression,
        )
        .unwrap();
        let bs = ByteStreamServer::new(
            &[WithInstanceName {
                instance_name: INSTANCE.to_owned(),
                config: ByteStreamConfig {
                    cas_store: "cas".to_owned(),
                    max_bytes_per_stream: UPSTREAM_CHUNK,
                    persist_stream_on_disconnect_timeout_s: 1,
                },
            }],
            &manager,
            &compression,
        )
        .unwrap();
        let ac = AcServer::new(
            &[WithInstanceName {
                instance_name: INSTANCE.to_owned(),
                config: AcStoreConfig {
                    ac_store: "sink".to_owned(),
                    read_only: false,
                },
            }],
            &manager,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel();
        let task = background_spawn!("legacy_digest_listener", async move {
            let incoming = stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            Server::builder()
                .add_service(cas.into_service())
                .add_service(bs.into_service())
                .add_service(ac.into_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _stopped = stopped.await;
                })
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(address.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            channel,
            address,
            stops: vec![stop],
            tasks: vec![task],
            coordinator: None,
        }
    }

    async fn shutdown(self) {
        if let Some(coordinator) = self.coordinator {
            coordinator.shutdown().await.unwrap();
        }
        for stop in self.stops {
            let _stopped = stop.send(());
        }
        drop(self.channel);
        for task in self.tasks {
            timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

fn remote_store(address: String) -> Store {
    Store::new(
        GrpcStore::new(&GrpcSpec {
            instance_name: INSTANCE.to_owned(),
            endpoints: vec![GrpcEndpoint {
                address,
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

fn digest(algorithm: DigestHasherFunc, bytes: &[u8]) -> DigestInfo {
    let mut hasher = algorithm.hasher();
    hasher.update(bytes);
    hasher.finalize_digest()
}

fn wire(algorithm: DigestHasherFunc) -> i32 {
    algorithm.proto_digest_func().into()
}

fn resource(key: DigestInfo, algorithm: Option<DigestHasherFunc>, upload: bool) -> String {
    let name = match algorithm {
        None => "",
        Some(SHA) => "sha256/",
        Some(B3) => "blake3/",
    };
    format!(
        "{INSTANCE}/{}blobs/{name}{}/{}",
        if upload {
            "uploads/default-witness/"
        } else {
            ""
        },
        key.packed_hash(),
        key.size_bytes()
    )
}

async fn put(channel: &Channel, algorithm: i32, key: DigestInfo, bytes: &[u8]) {
    let response = ContentAddressableStorageClient::new(channel.clone())
        .batch_update_blobs(BatchUpdateBlobsRequest {
            instance_name: INSTANCE.to_owned(),
            digest_function: algorithm,
            requests: vec![batch_update_blobs_request::Request {
                digest: Some(key.into()),
                data: Bytes::copy_from_slice(bytes),
                compressor: 0,
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0].status.as_ref().unwrap().code,
        0,
        "CAS update {response:?}"
    );
}

async fn get(channel: &Channel, algorithm: i32, key: DigestInfo, expected: &[u8]) {
    let response = ContentAddressableStorageClient::new(channel.clone())
        .batch_read_blobs(BatchReadBlobsRequest {
            instance_name: INSTANCE.to_owned(),
            digest_function: algorithm,
            digests: vec![key.into()],
            acceptable_compressors: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0].status.as_ref().unwrap().code,
        0,
        "CAS read {response:?}"
    );
    assert_eq!(response.responses[0].data.as_ref(), expected);
}

async fn bs_write(
    channel: &Channel,
    algorithm: Option<DigestHasherFunc>,
    key: DigestInfo,
    bytes: &[u8],
) {
    let chunks: Vec<_> = bytes
        .chunks(FIXED_CHUNK)
        .enumerate()
        .map(|(index, chunk)| WriteRequest {
            resource_name: resource(key, algorithm, true),
            write_offset: i64::try_from(index * FIXED_CHUNK).unwrap(),
            finish_write: (index + 1) * FIXED_CHUNK >= bytes.len(),
            data: Bytes::copy_from_slice(chunk),
        })
        .collect();
    let result = ByteStreamClient::new(channel.clone())
        .write(stream::iter(chunks))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.committed_size, i64::try_from(bytes.len()).unwrap());
}

async fn bs_read(
    channel: &Channel,
    algorithm: Option<DigestHasherFunc>,
    key: DigestInfo,
    expected: &[u8],
) -> Vec<usize> {
    let mut responses = ByteStreamClient::new(channel.clone())
        .read(ReadRequest {
            resource_name: resource(key, algorithm, false),
            read_offset: 0,
            read_limit: 0,
        })
        .await
        .unwrap()
        .into_inner();
    let mut bytes = Vec::new();
    let mut lengths = Vec::new();
    while let Some(response) = responses.message().await.unwrap() {
        lengths.push(response.data.len());
        bytes.extend_from_slice(&response.data);
    }
    assert_eq!(bytes, expected);
    lengths
}

async fn bs_query(channel: &Channel, algorithm: Option<DigestHasherFunc>, key: DigestInfo) {
    // Completed status must use storage, not the separate active UUID shortcut.
    let response = ByteStreamClient::new(channel.clone())
        .query_write_status(QueryWriteStatusRequest {
            resource_name: resource(key, algorithm, true),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(
        response.complete,
        "completed query selected another namespace: {response:?}"
    );
    assert_eq!(
        response.committed_size,
        i64::try_from(key.size_bytes()).unwrap()
    );
}

fn result(marker: &'static [u8]) -> ActionResult {
    ActionResult {
        stdout_raw: Bytes::from_static(marker),
        ..ActionResult::default()
    }
}

async fn ac_put(channel: &Channel, algorithm: i32, key: DigestInfo, value: &ActionResult) {
    let response = ActionCacheClient::new(channel.clone())
        .update_action_result(UpdateActionResultRequest {
            instance_name: INSTANCE.to_owned(),
            action_digest: Some(key.into()),
            action_result: Some(value.clone()),
            digest_function: algorithm,
            ..UpdateActionResultRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(&response, value);
}

async fn ac_get(channel: &Channel, algorithm: i32, key: DigestInfo, expected: &ActionResult) {
    let response = ActionCacheClient::new(channel.clone())
        .get_action_result(GetActionResultRequest {
            instance_name: INSTANCE.to_owned(),
            action_digest: Some(key.into()),
            digest_function: algorithm,
            ..GetActionResultRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(&response, expected);
}

async fn execute_cache_hit(channel: &Channel, algorithm: DigestHasherFunc) {
    let command = Command {
        arguments: vec!["cached witness".to_owned()],
        ..Command::default()
    }
    .encode_to_vec();
    let command_key = digest(algorithm, &command);
    put(channel, wire(algorithm), command_key, &command).await;
    let action = Action {
        command_digest: Some(command_key.into()),
        input_root_digest: Some(digest(algorithm, b"").into()),
        ..Action::default()
    }
    .encode_to_vec();
    let key = digest(algorithm, &action);
    put(channel, wire(algorithm), key, &action).await;
    let expected = result(b"correct execution namespace");
    ac_put(channel, wire(algorithm), key, &expected).await;
    let mut operations = ExecutionClient::new(channel.clone())
        .execute(ExecuteRequest {
            instance_name: INSTANCE.to_owned(),
            action_digest: Some(key.into()),
            digest_function: if algorithm == SHA { 0 } else { wire(algorithm) },
            ..ExecuteRequest::default()
        })
        .await
        .unwrap()
        .into_inner();
    let response = timeout(Duration::from_secs(10), async {
        loop {
            let item = operations.message().await.unwrap().unwrap();
            if item.done {
                let Some(operation::Result::Response(value)) = item.result else {
                    panic!("expected ExecuteResponse")
                };
                break ExecuteResponse::decode(value.value.as_slice()).unwrap();
            }
        }
    })
    .await
    .unwrap();
    assert!(response.cached_result);
    assert_eq!(response.result.as_ref(), Some(&expected));
}

fn selected(name: &str) -> bool {
    let selected = std::env::var("NL_FIXED_DIGEST_CASE").ok();
    let run = selected.as_deref().is_none_or(|selected| selected == name);
    if run {
        eprintln!("digest phase: {name}");
    }
    run
}

async fn qualify() {
    let fixture = Fixture::new().await;
    let fixed = Rpc::fixed(&fixture, Store::new(fixture.stores.cas.clone()), "local").await;
    let legacy = Rpc::legacy(
        Store::new(fixture.stores.cas.clone()),
        Store::new(fixture.stores.sink.clone()),
    )
    .await;
    let data = b"independent explicit namespace seed";
    let sha = digest(SHA, data);
    let b3 = digest(B3, data);
    put(&fixed.channel, wire(SHA), sha, data).await;
    put(&fixed.channel, wire(B3), b3, data).await;
    let sha_result = result(b"SHA value under identical opaque AC key");
    let b3_result = result(b"BLAKE3 value under identical opaque AC key");
    ac_put(&fixed.channel, wire(SHA), sha, &sha_result).await;
    ac_put(&fixed.channel, wire(B3), sha, &b3_result).await;

    if selected("cas-update") {
        let bytes = b"Unknown CAS update must verify SHA256";
        let key = digest(SHA, bytes);
        put(&fixed.channel, 0, key, bytes).await;
        get(&fixed.channel, wire(SHA), key, bytes).await;
    }
    if selected("cas-read") {
        get(&fixed.channel, 0, sha, data).await;
        get(&fixed.channel, wire(B3), b3, data).await;
    }
    if selected("cas-find") {
        let response = ContentAddressableStorageClient::new(fixed.channel.clone())
            .find_missing_blobs(FindMissingBlobsRequest {
                instance_name: INSTANCE.to_owned(),
                blob_digests: vec![sha.into()],
                digest_function: 0,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(response.missing_blob_digests.is_empty());
    }
    if selected("cas-tree") {
        let directory = Directory {
            files: vec![
                nativelink_proto::build::bazel::remote::execution::v2::FileNode {
                    name: "witness".to_owned(),
                    digest: Some(sha.into()),
                    ..Default::default()
                },
            ],
            ..Directory::default()
        };
        let bytes = directory.encode_to_vec();
        let key = digest(SHA, &bytes);
        put(&fixed.channel, wire(SHA), key, &bytes).await;
        let mut responses = ContentAddressableStorageClient::new(fixed.channel.clone())
            .get_tree(GetTreeRequest {
                instance_name: INSTANCE.to_owned(),
                root_digest: Some(key.into()),
                digest_function: 0,
                ..GetTreeRequest::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            responses.message().await.unwrap().unwrap().directories,
            vec![directory]
        );
        assert!(responses.message().await.unwrap().is_none());
    }
    if selected("ac-get") {
        ac_get(&fixed.channel, 0, sha, &sha_result).await;
        ac_get(&fixed.channel, wire(B3), sha, &b3_result).await;
    }
    if selected("ac-update") {
        let replacement = result(b"fixed Unknown mutation");
        ac_put(&fixed.channel, 0, sha, &replacement).await;
        ac_get(&fixed.channel, wire(SHA), sha, &replacement).await;
        ac_get(&fixed.channel, wire(B3), sha, &b3_result).await;
    }
    if selected("bs-read") {
        bs_read(&fixed.channel, None, sha, data).await;
        bs_read(&fixed.channel, Some(B3), b3, data).await;
    }
    if selected("bs-write") {
        let bytes = vec![0x59; FIXED_CHUNK + 19];
        let key = digest(SHA, &bytes);
        bs_write(&fixed.channel, None, key, &bytes).await;
        bs_read(&fixed.channel, Some(SHA), key, &bytes).await;
        bs_write(&fixed.channel, Some(B3), digest(B3, &bytes), &bytes).await;
    }
    if selected("bs-query") {
        bs_query(&fixed.channel, None, sha).await;
        bs_query(&fixed.channel, Some(B3), b3).await;
    }
    if selected("execute") {
        execute_cache_hit(&fixed.channel, SHA).await;
        execute_cache_hit(&fixed.channel, B3).await;
    }
    if selected("legacy") {
        let bytes = b"legacy omitted algorithms keep process BLAKE3";
        let key = digest(B3, bytes);
        put(&legacy.channel, 0, key, bytes).await;
        get(&legacy.channel, 0, key, bytes).await;
        ac_get(&legacy.channel, 0, sha, &b3_result).await;
        let value = result(b"legacy Unknown mutation");
        ac_put(&legacy.channel, 0, key, &value).await;
        ac_get(&legacy.channel, wire(B3), key, &value).await;
        bs_write(&legacy.channel, None, key, bytes).await;
        bs_read(&legacy.channel, None, key, bytes).await;
        bs_query(&legacy.channel, None, key).await;
    }

    let remote = remote_store(legacy.address.clone());
    let front = Rpc::fixed(&fixture, remote.clone(), "remote").await;
    if selected("remote-read") {
        bs_read(&front.channel, None, sha, data).await;
        bs_read(&front.channel, Some(B3), b3, data).await;
    }
    if selected("remote-write") {
        let bytes = vec![0x37; FIXED_CHUNK + 23];
        let key = digest(SHA, &bytes);
        bs_write(&front.channel, None, key, &bytes).await;
        get(&legacy.channel, wire(SHA), key, &bytes).await;
        bs_write(&front.channel, Some(B3), digest(B3, &bytes), &bytes).await;
    }
    if selected("remote-query") {
        bs_query(&front.channel, None, sha).await;
        bs_query(&front.channel, Some(B3), b3).await;
    }
    if selected("remote-chunks") {
        let bytes = vec![0x63; UPSTREAM_CHUNK + 41];
        let key = digest(SHA, &bytes);
        put(&legacy.channel, wire(SHA), key, &bytes).await;
        let upstream = bs_read(&legacy.channel, Some(SHA), key, &bytes).await;
        assert!(
            upstream.iter().any(|length| *length > FIXED_CHUNK),
            "upstream must expose the forwarding witness: {upstream:?}"
        );
        let fixed_chunks = bs_read(&front.channel, Some(SHA), key, &bytes).await;
        assert!(
            fixed_chunks.iter().all(|length| *length <= FIXED_CHUNK),
            "fixed chunks: {fixed_chunks:?}"
        );
        let legacy_front = Rpc::legacy(remote, Store::new(fixture.stores.sink.clone())).await;
        let forwarded = bs_read(&legacy_front.channel, Some(SHA), key, &bytes).await;
        assert_eq!(
            forwarded, upstream,
            "legacy direct forwarding remains unchanged"
        );
        legacy_front.shutdown().await;
    }
    front.shutdown().await;
    fixed.shutdown().await;
    legacy.shutdown().await;
    assert_eq!(
        default_digest_hasher_func(),
        B3,
        "fixed roles must not replace the process default"
    );
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "this isolated process must set the digest default before constructing its runtime"
)]
fn fixed_roles_ignore_process_blake3_default_and_preserve_legacy() {
    set_default_digest_hasher_func(B3).unwrap();
    assert_eq!(default_digest_hasher_func(), B3);
    if let Ok(phase) = std::env::var("NL_FIXED_DIGEST_CASE") {
        assert!(
            PHASES.contains(&phase.as_str()),
            "unknown qualification phase"
        );
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            timeout(Duration::from_secs(90), qualify()).await.unwrap();
        });
}
