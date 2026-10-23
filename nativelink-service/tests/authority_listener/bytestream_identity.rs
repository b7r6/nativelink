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

//! Actual HTTP/2 upload identity, including retained and optimized store work.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use bytes::Bytes;
use nativelink_error::{Error, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::google::bytestream::{QueryWriteStatusRequest, QueryWriteStatusResponse};
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::digest_hasher_func_from_context;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    RemoveCallback, StoreDriver, StoreKey, StoreOptimizations, UploadSizeInfo,
};
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use super::*;

const DATA: &[u8] = b"0123456789";
const PREFIX: usize = 4;
const UUID: &str = "0123456789abcdef0123456789abcdef";
const WAIT: Duration = Duration::from_secs(5);

type Key = (DigestHasherFunc, DigestInfo);

#[derive(Debug, MetricsComponent)]
struct Spy {
    values: Mutex<HashMap<Key, Bytes>>,
    updates: AtomicUsize,
    has: AtomicUsize,
    received: AtomicUsize,
    completed_input: AtomicUsize,
    oneshot: bool,
    completion: Option<Semaphore>,
}

impl Spy {
    fn new(oneshot: bool, gated: bool) -> Arc<Self> {
        Arc::new(Self {
            values: Mutex::new(HashMap::new()),
            updates: AtomicUsize::new(0),
            has: AtomicUsize::new(0),
            received: AtomicUsize::new(0),
            completed_input: AtomicUsize::new(0),
            oneshot,
            completion: gated.then(|| Semaphore::new(0)),
        })
    }

    async fn finish(&self, key: DigestInfo, data: Bytes) -> Result<(), Error> {
        self.completed_input.fetch_add(1, Ordering::Relaxed);
        if let Some(gate) = &self.completion {
            let permit = tokio::time::timeout(WAIT, gate.acquire())
                .await
                .map_err(|_| make_err!(Code::DeadlineExceeded, "fixture completion gate"))?
                .unwrap();
            permit.forget();
        }
        self.values
            .lock()
            .unwrap()
            .insert((digest_hasher_func_from_context(), key), data);
        Ok(())
    }

    fn value(&self, algorithm: DigestHasherFunc, digest: &Digest) -> Option<Bytes> {
        self.values
            .lock()
            .unwrap()
            .get(&(algorithm, DigestInfo::try_from(digest).unwrap()))
            .cloned()
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
                panic!("digest fixture")
            };
            *result = values
                .get(&(digest_hasher_func_from_context(), *digest))
                .map(|bytes| bytes.len() as u64);
        }
        Ok(())
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.updates.fetch_add(1, Ordering::Relaxed);
        let StoreKey::Digest(digest) = key else {
            panic!("digest fixture")
        };
        let mut bytes = Vec::new();
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            self.received.fetch_add(chunk.len(), Ordering::Relaxed);
            bytes.extend_from_slice(&chunk);
        }
        let length = bytes.len() as u64;
        self.finish(digest, bytes.into()).await?;
        Ok(length)
    }

    async fn update_oneshot(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        bytes: Bytes,
    ) -> Result<(), Error> {
        self.updates.fetch_add(1, Ordering::Relaxed);
        self.received.fetch_add(bytes.len(), Ordering::Relaxed);
        let StoreKey::Digest(digest) = key else {
            panic!("digest fixture")
        };
        self.finish(digest, bytes).await
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        panic!("upload fixture never downloads")
    }

    fn optimized_for(&self, optimization: StoreOptimizations) -> bool {
        self.oneshot && optimization == StoreOptimizations::SubscribesToUpdateOneshot
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

async fn fixture(oneshot: bool, gated: bool) -> (Runtime, Arc<Spy>) {
    let spy = Spy::new(oneshot, gated);
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

fn resource(uuid: &str, algorithm: &str, digest: &Digest) -> String {
    format!(
        "{INSTANCE}/uploads/{uuid}/blobs/{algorithm}{}/{}",
        digest.hash, digest.size_bytes
    )
}

fn frame(resource_name: String, offset: usize, finish_write: bool, data: &[u8]) -> WriteRequest {
    WriteRequest {
        resource_name,
        write_offset: i64::try_from(offset).unwrap(),
        finish_write,
        data: Bytes::copy_from_slice(data),
    }
}

type UploadTask = JoinHandle<
    Result<tonic::Response<nativelink_proto::google::bytestream::WriteResponse>, Status>,
>;

fn writer(channel: Channel) -> (mpsc::Sender<WriteRequest>, UploadTask) {
    let (sender, receiver) = mpsc::channel(4);
    let task = background_spawn!("identity_fixture_writer", async move {
        ByteStreamClient::new(channel)
            .write(ReceiverStream::new(receiver))
            .await
    });
    (sender, task)
}

async fn result(
    task: UploadTask,
) -> Result<nativelink_proto::google::bytestream::WriteResponse, Status> {
    tokio::time::timeout(WAIT, task)
        .await
        .expect("write did not finish")
        .unwrap()
        .map(tonic::Response::into_inner)
}

async fn status(
    channel: Channel,
    resource_name: String,
) -> Result<QueryWriteStatusResponse, Status> {
    tokio::time::timeout(
        WAIT,
        ByteStreamClient::new(channel)
            .query_write_status(QueryWriteStatusRequest { resource_name }),
    )
    .await
    .expect("query did not finish")
    .map(tonic::Response::into_inner)
}

async fn progress(channel: Channel, name: &str, amount: usize) {
    tokio::time::timeout(WAIT, async {
        loop {
            let value = status(channel.clone(), name.to_owned()).await.unwrap();
            if value.committed_size == i64::try_from(amount).unwrap() && !value.complete {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("upload did not reach observed progress");
}

async fn write(
    channel: Channel,
    request: WriteRequest,
) -> Result<nativelink_proto::google::bytestream::WriteResponse, Status> {
    let (sender, task) = writer(channel);
    sender.send(request).await.unwrap();
    drop(sender);
    result(task).await
}

async fn active(runtime: &Runtime, name: &str) -> (mpsc::Sender<WriteRequest>, UploadTask) {
    let (sender, task) = writer(runtime.worker.clone());
    sender
        .send(frame(name.to_owned(), 0, false, &DATA[..PREFIX]))
        .await
        .unwrap();
    progress(runtime.client.clone(), name, PREFIX).await;
    (sender, task)
}

// Server progress records receipt into its channel. Wait separately for this
// local fixture's backend before measuring update/byte counters or parking it.
async fn settled_prefix(spy: &Spy) {
    tokio::time::timeout(WAIT, async {
        while spy.updates.load(Ordering::Relaxed) != 1
            || spy.received.load(Ordering::Relaxed) != PREFIX
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("local backend did not consume the initial prefix");
}

#[derive(Clone, Copy)]
enum Difference {
    Algorithm,
    Hash,
    Size,
    Uuid,
}

fn different(case: Difference, digest: &Digest) -> String {
    let mut digest = digest.clone();
    match case {
        Difference::Algorithm => resource(UUID, "blake3/", &digest),
        Difference::Hash => {
            digest.hash = "aa".repeat(32);
            resource(UUID, "sha256/", &digest)
        }
        Difference::Size => {
            digest.size_bytes += 1;
            resource(UUID, "sha256/", &digest)
        }
        Difference::Uuid => resource(&format!("{UUID}gg"), "sha256/", &digest),
    }
}

async fn active_identity(case: Difference) {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    assert_eq!(
        status(runtime.client.clone(), different(case, &digest))
            .await
            .unwrap(),
        QueryWriteStatusResponse {
            committed_size: 0,
            complete: false
        }
    );
    // Omitted SHA256 spelling still names the same resolved upload identity.
    progress(runtime.client.clone(), &resource(UUID, "", &digest), PREFIX).await;
    sender
        .send(frame(String::new(), PREFIX, true, &DATA[PREFIX..]))
        .await
        .unwrap();
    drop(sender);
    assert_eq!(
        result(task).await.unwrap().committed_size,
        DATA.len() as i64
    );
    assert_eq!(spy.value(DigestHasherFunc::Sha256, &digest).unwrap(), DATA);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn active_query_distinguishes_algorithm() {
    active_identity(Difference::Algorithm).await;
}
#[nativelink_test]
async fn active_query_distinguishes_hash() {
    active_identity(Difference::Hash).await;
}
#[nativelink_test]
async fn active_query_distinguishes_size() {
    active_identity(Difference::Size).await;
}
#[nativelink_test]
async fn active_query_distinguishes_lossy_uuid_alias() {
    active_identity(Difference::Uuid).await;
}

async fn invalid_query(compressed: bool) {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    let invalid = if compressed {
        name.replace("/blobs/", "/compressed-blobs/zstd/")
    } else {
        name.replace("/sha256/", "/sha1/")
    };
    let before = spy.has.load(Ordering::Relaxed);
    assert_eq!(
        status(runtime.client.clone(), invalid)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(spy.has.load(Ordering::Relaxed), before);
    sender
        .send(frame(String::new(), PREFIX, true, &DATA[PREFIX..]))
        .await
        .unwrap();
    drop(sender);
    result(task).await.unwrap();
    runtime.shutdown().await;
}

#[nativelink_test]
async fn active_query_validates_digest_before_progress() {
    invalid_query(false).await;
}
#[nativelink_test]
async fn active_query_validates_compressor_before_progress() {
    invalid_query(true).await;
}

async fn idle_identity(case: Difference) {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    settled_prefix(&spy).await;
    drop(sender);
    assert!(result(task).await.is_err());
    let before = spy.updates.load(Ordering::Relaxed);
    assert_eq!(
        write(
            runtime.worker.clone(),
            frame(different(case, &digest), PREFIX, true, &DATA[PREFIX..])
        )
        .await
        .unwrap_err()
        .code(),
        Code::InvalidArgument
    );
    assert_eq!(spy.updates.load(Ordering::Relaxed), before);
    progress(runtime.client.clone(), &name, PREFIX).await;
    assert_eq!(
        write(
            runtime.worker.clone(),
            frame(name, PREFIX, true, &DATA[PREFIX..])
        )
        .await
        .unwrap()
        .committed_size,
        DATA.len() as i64
    );
    assert_eq!(spy.value(DigestHasherFunc::Sha256, &digest).unwrap(), DATA);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn idle_resume_distinguishes_algorithm() {
    idle_identity(Difference::Algorithm).await;
}
#[nativelink_test]
async fn idle_resume_distinguishes_hash() {
    idle_identity(Difference::Hash).await;
}
#[nativelink_test]
async fn idle_resume_distinguishes_size() {
    idle_identity(Difference::Size).await;
}
#[nativelink_test]
async fn idle_resume_distinguishes_lossy_uuid_alias() {
    idle_identity(Difference::Uuid).await;
}

async fn exact_active_collision(offset: usize) {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    settled_prefix(&spy).await;
    assert_eq!(
        write(
            runtime.worker.clone(),
            frame(name.clone(), offset, true, &DATA[offset..])
        )
        .await
        .unwrap_err()
        .code(),
        Code::Unavailable
    );
    assert_eq!(spy.updates.load(Ordering::Relaxed), 1);
    progress(runtime.client.clone(), &name, PREFIX).await;
    sender
        .send(frame(String::new(), PREFIX, true, &DATA[PREFIX..]))
        .await
        .unwrap();
    drop(sender);
    result(task).await.unwrap();
    runtime.shutdown().await;
}

#[nativelink_test]
async fn exact_active_collision_at_zero_is_retryable() {
    exact_active_collision(0).await;
}

#[nativelink_test]
async fn exact_active_collision_at_prefix_is_retryable() {
    exact_active_collision(PREFIX).await;
}

async fn independent_upload(alias: bool) {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    settled_prefix(&spy).await;
    drop(sender);
    assert!(result(task).await.is_err());
    let other_data = if alias { DATA } else { b"a different blob" };
    let other_digest = super::digest(DigestHasherFunc::Sha256, other_data);
    let other_uuid = if alias {
        format!("{UUID}gg")
    } else {
        UUID.to_owned()
    };
    let other_name = resource(&other_uuid, "sha256/", &other_digest);
    write(
        runtime.worker.clone(),
        frame(other_name, 0, true, other_data),
    )
    .await
    .unwrap();
    assert_eq!(
        spy.value(DigestHasherFunc::Sha256, &other_digest).unwrap(),
        other_data
    );
    progress(runtime.client.clone(), &name, PREFIX).await;
    write(
        runtime.worker.clone(),
        frame(name, PREFIX, true, &DATA[PREFIX..]),
    )
    .await
    .unwrap();
    runtime.shutdown().await;
}

#[nativelink_test]
async fn same_uuid_different_blob_upload_is_independent() {
    independent_upload(false).await;
}

#[nativelink_test]
async fn lossy_uuid_alias_upload_is_independent() {
    independent_upload(true).await;
}

#[nativelink_test]
async fn single_message_tracks_identity_until_backend_completion() {
    let (runtime, spy) = fixture(true, true).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = writer(runtime.worker.clone());
    sender
        .send(frame(name.clone(), 0, true, DATA))
        .await
        .unwrap();
    drop(sender);
    tokio::time::timeout(WAIT, async {
        while spy.completed_input.load(Ordering::Relaxed) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        status(runtime.client.clone(), name.clone()).await.unwrap(),
        QueryWriteStatusResponse {
            committed_size: DATA.len() as i64,
            complete: false,
        }
    );
    assert_eq!(
        write(runtime.worker.clone(), frame(name.clone(), 0, true, DATA))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(spy.updates.load(Ordering::Relaxed), 1);
    spy.completion.as_ref().unwrap().add_permits(1);
    result(task).await.unwrap();
    assert_eq!(
        status(runtime.client.clone(), name).await.unwrap(),
        QueryWriteStatusResponse {
            committed_size: DATA.len() as i64,
            complete: true,
        }
    );
    runtime.shutdown().await;
}

async fn later_resource_name(case: Option<Difference>) {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    settled_prefix(&spy).await;
    sender
        .send(frame(
            case.map_or_else(
                || resource(UUID, "", &digest),
                |case| different(case, &digest),
            ),
            PREFIX,
            true,
            &DATA[PREFIX..],
        ))
        .await
        .unwrap();
    drop(sender);
    assert_eq!(
        result(task).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    assert!(spy.value(DigestHasherFunc::Sha256, &digest).is_none());
    assert_eq!(spy.received.load(Ordering::Relaxed), PREFIX);
    progress(runtime.client.clone(), &name, PREFIX).await;
    write(
        runtime.worker.clone(),
        frame(name, PREFIX, true, &DATA[PREFIX..]),
    )
    .await
    .unwrap();
    runtime.shutdown().await;
}

#[nativelink_test]
async fn later_frame_cannot_change_algorithm() {
    later_resource_name(Some(Difference::Algorithm)).await;
}
#[nativelink_test]
async fn later_frame_cannot_change_hash() {
    later_resource_name(Some(Difference::Hash)).await;
}
#[nativelink_test]
async fn later_frame_cannot_change_size() {
    later_resource_name(Some(Difference::Size)).await;
}
#[nativelink_test]
async fn later_frame_cannot_change_uuid() {
    later_resource_name(Some(Difference::Uuid)).await;
}

#[nativelink_test]
async fn later_frame_cannot_change_equivalent_digest_spelling() {
    later_resource_name(None).await;
}

#[nativelink_test]
async fn repeated_exact_resource_name_is_accepted() {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Blake3, DATA);
    let name = resource(UUID, "blake3/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    progress(
        runtime.client.clone(),
        &name.replace("/blobs/", "/compressed-blobs/identity/"),
        PREFIX,
    )
    .await;
    progress(runtime.client.clone(), &format!("{name}/trace"), PREFIX).await;
    sender
        .send(frame(name, PREFIX, true, &DATA[PREFIX..]))
        .await
        .unwrap();
    drop(sender);
    result(task).await.unwrap();
    assert_eq!(spy.value(DigestHasherFunc::Blake3, &digest).unwrap(), DATA);
    runtime.shutdown().await;
}

#[path = "bytestream_identity/upstream.rs"]
mod upstream;
