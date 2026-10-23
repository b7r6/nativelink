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

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use nativelink_canonical_config::{Manifest, parse_canonical};
use nativelink_canonical_filesystem::Ownership;
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use nativelink_util::spawn;
use opentelemetry::trace::FutureExt;
use tokio::sync::Notify;
use tokio::time::timeout;

use super::*;

mod listener;
mod rpc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum HookPoint {
    Write,
    DirectorySync,
}

#[derive(Debug)]
struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
    fail: bool,
}

impl Gate {
    fn new(fail: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            released: Mutex::new(false),
            wake: Condvar::new(),
            fail,
        })
    }

    async fn wait(&self) {
        timeout(Duration::from_secs(10), self.entered.notified())
            .await
            .unwrap();
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }

    fn run(&self) -> Result<(), Error> {
        self.entered.notify_one();
        let released = self.released.lock().unwrap();
        let (released, result) = self
            .wake
            .wait_timeout_while(released, Duration::from_secs(15), |value| !*value)
            .unwrap();
        assert!(
            *released && !result.timed_out(),
            "test failed to release IO gate"
        );
        if self.fail {
            Err(make_err!(
                Code::Unavailable,
                "injected directory sync refusal"
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct Hooks(Mutex<Option<(HookPoint, Arc<Gate>)>>);
impl Hooks {
    fn install(&self, point: HookPoint, fail: bool) -> Arc<Gate> {
        let gate = Gate::new(fail);
        assert!(
            self.0
                .lock()
                .unwrap()
                .replace((point, gate.clone()))
                .is_none()
        );
        gate
    }
    pub(super) fn hit(&self, point: HookPoint) -> Result<(), Error> {
        let gate = {
            let mut current = self.0.lock().unwrap();
            if current
                .as_ref()
                .is_some_and(|(expected, _)| *expected == point)
            {
                current.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.run()?;
        }
        Ok(())
    }
}

struct Fixture {
    directory: PathBuf,
    manifest: Manifest,
}
impl Fixture {
    async fn new() -> (Self, CanonicalStores) {
        let directory =
            std::env::temp_dir().join(format!("canonical-store-{}", uuid::Uuid::new_v4()));
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
        let stores = CanonicalStores::from_layout(layout).await.unwrap();
        (
            Self {
                directory,
                manifest,
            },
            stores,
        )
    }
    fn path(&self, sink: bool, leaf: &DigestLeaf) -> PathBuf {
        self.directory
            .join(if sink { "sink" } else { "cas" })
            .join("content")
            .join(leaf.as_str())
    }
    fn temporary_count(&self, sink: bool) -> usize {
        fs::read_dir(
            self.directory
                .join(if sink { "sink" } else { "cas" })
                .join("temp"),
        )
        .unwrap()
        .count()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}
fn digest(algorithm: DigestHasherFunc, bytes: &[u8]) -> DigestInfo {
    let mut hasher = algorithm.hasher();
    hasher.update(bytes);
    hasher.finalize_digest()
}
fn pin<T: StoreDriver>(store: &Arc<T>) -> Pin<&T> {
    Pin::new(store.as_ref())
}

#[nativelink_macro::nativelink_test]
async fn cas_roundtrip_ranges_duplicate_and_empty_both_algorithms() {
    let (fixture, stores) = Fixture::new().await;
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        async {
            let bytes = Bytes::from_static(b"canonical object payload");
            let key = digest(algorithm, &bytes);
            assert_eq!(pin(&stores.cas).has(key.into()).await.unwrap(), None);
            pin(&stores.cas)
                .update_oneshot(key.into(), bytes.clone())
                .await
                .unwrap();
            assert_eq!(
                pin(&stores.cas).has(key.into()).await.unwrap(),
                Some(bytes.len() as u64)
            );
            assert_eq!(
                pin(&stores.cas)
                    .get_part_unchunked(key.into(), 2, Some(8))
                    .await
                    .unwrap(),
                &bytes[2..10]
            );
            assert_eq!(
                pin(&stores.cas)
                    .get_part_unchunked(key.into(), 2, Some(1000))
                    .await
                    .unwrap(),
                &bytes[2..]
            );
            assert_eq!(
                pin(&stores.cas)
                    .get_part_unchunked(key.into(), 500, None)
                    .await
                    .unwrap_err()
                    .code,
                Code::OutOfRange
            );
            let before = fixture.temporary_count(false);
            pin(&stores.cas)
                .update_oneshot(key.into(), bytes.clone())
                .await
                .unwrap();
            assert_eq!(
                fixture.temporary_count(false),
                before + 1,
                "duplicate temporary survives"
            );
            assert_eq!(
                pin(&stores.cas)
                    .get_part_unchunked(key.into(), 0, None)
                    .await
                    .unwrap(),
                bytes
            );
            let empty = digest(algorithm, b"");
            assert_eq!(pin(&stores.cas).has(empty.into()).await.unwrap(), Some(0));
            pin(&stores.cas)
                .update_oneshot(empty.into(), Bytes::new())
                .await
                .unwrap();
            assert!(
                pin(&stores.cas)
                    .get_part_unchunked(empty.into(), 0, None)
                    .await
                    .unwrap()
                    .is_empty()
            );
            let wrong_empty = DigestInfo::new([1; 32], 0);
            assert_eq!(
                pin(&stores.cas)
                    .has(wrong_empty.into())
                    .await
                    .unwrap_err()
                    .code,
                Code::InvalidArgument
            );
        }
        .with_context(make_ctx_for_hash_func(algorithm).unwrap())
        .await;
    }
}

#[nativelink_macro::nativelink_test]
async fn sink_payload_size_is_independent_of_key_and_replacement_is_mutable() {
    let (_fixture, stores) = Fixture::new().await;
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        async {
            let key = DigestInfo::new([0x5a; 32], u64::MAX);
            for value in [
                Bytes::from_static(b"first AC result"),
                Bytes::from_static(b"replacement"),
                Bytes::new(),
            ] {
                pin(&stores.sink)
                    .update_oneshot(key.into(), value.clone())
                    .await
                    .unwrap();
                assert_eq!(
                    pin(&stores.sink).has(key.into()).await.unwrap(),
                    Some(value.len() as u64)
                );
                assert_eq!(
                    pin(&stores.sink)
                        .get_part_unchunked(key.into(), 0, None)
                        .await
                        .unwrap(),
                    value
                );
            }
        }
        .with_context(make_ctx_for_hash_func(algorithm).unwrap())
        .await;
    }
}

#[nativelink_macro::nativelink_test]
async fn invalid_uploads_require_eof_size_hash_and_bounded_chunks() {
    let (fixture, stores) = Fixture::new().await;
    let key = digest(DigestHasherFunc::Sha256, b"good");
    assert_eq!(
        pin(&stores.cas)
            .update_oneshot(key.into(), Bytes::from_static(b"evil"))
            .await
            .unwrap_err()
            .code,
        Code::InvalidArgument
    );
    assert_eq!(
        pin(&stores.cas)
            .update_oneshot(key.into(), Bytes::from_static(b"goo"))
            .await
            .unwrap_err()
            .code,
        Code::InvalidArgument
    );
    for (data, eof, hint, expected) in [
        (
            Bytes::from_static(b"good"),
            false,
            UploadSizeInfo::ExactSize(4),
            Code::Internal,
        ),
        (
            Bytes::from_static(b"good"),
            true,
            UploadSizeInfo::ExactSize(5),
            Code::InvalidArgument,
        ),
        (
            Bytes::from_static(b"good"),
            true,
            UploadSizeInfo::MaxSize(3),
            Code::InvalidArgument,
        ),
        (
            Bytes::from(vec![0; MAX_CHUNK_BYTES + 1]),
            true,
            UploadSizeInfo::MaxSize(MAX_OBJECT_BYTES),
            Code::ResourceExhausted,
        ),
    ] {
        let (mut tx, rx) = make_buf_channel_pair();
        tx.send(data).await.unwrap();
        if eof {
            tx.send_eof().unwrap();
        }
        drop(tx);
        assert_eq!(
            pin(&stores.cas)
                .update(key.into(), rx, hint)
                .await
                .unwrap_err()
                .code,
            expected
        );
        assert_eq!(pin(&stores.cas).has(key.into()).await.unwrap(), None);
    }
    assert_eq!(
        fixture.temporary_count(false),
        6,
        "failed uploads remain reviewable"
    );
}

#[nativelink_macro::nativelink_test]
async fn has_validates_arity_key_kind_and_bounded_cost_before_io() {
    let (_fixture, stores) = Fixture::new().await;
    let key = digest(DigestHasherFunc::Sha256, b"missing");
    let mut sentinel = [Some(99)];
    assert_eq!(
        pin(&stores.cas)
            .has_with_results(&[], &mut sentinel)
            .await
            .unwrap_err()
            .code,
        Code::InvalidArgument
    );
    assert_eq!(sentinel, [Some(99)]);
    let keys = vec![StoreKey::from(key); MAX_HAS_KEYS + 1];
    let mut results = vec![Some(99); keys.len()];
    assert_eq!(
        pin(&stores.cas)
            .has_with_results(&keys, &mut results)
            .await
            .unwrap_err()
            .code,
        Code::InvalidArgument
    );
    assert!(results.iter().all(|value| *value == Some(99)));
    assert_eq!(
        pin(&stores.cas)
            .has(StoreKey::Str("alias".into()))
            .await
            .unwrap_err()
            .code,
        Code::InvalidArgument
    );
    assert_eq!(
        pin(&stores.sink)
            .update_oneshot(StoreKey::Str("alias".into()), Bytes::new())
            .await
            .unwrap_err()
            .code,
        Code::InvalidArgument
    );
    let oversized = DigestInfo::new([0; 32], MAX_OBJECT_BYTES + 1);
    assert_eq!(
        pin(&stores.cas)
            .has(oversized.into())
            .await
            .unwrap_err()
            .code,
        Code::ResourceExhausted
    );
}

#[nativelink_macro::nativelink_test]
async fn reads_and_has_refuse_corruption_before_returning_any_bytes() {
    let (fixture, stores) = Fixture::new().await;
    let key = digest(DigestHasherFunc::Sha256, b"good");
    pin(&stores.cas)
        .update_oneshot(key.into(), Bytes::from_static(b"good"))
        .await
        .unwrap();
    let leaf = stores
        .cas
        .0
        .key(&key.into(), DigestHasherFunc::Sha256)
        .unwrap()
        .leaf;
    fs::write(fixture.path(false, &leaf), b"evil").unwrap();
    let (mut tx, mut rx) = make_buf_channel_pair();
    let result = pin(&stores.cas)
        .get_part(key.into(), &mut tx, 0, Some(1))
        .await;
    assert_eq!(result.unwrap_err().code, Code::DataLoss);
    drop(tx);
    assert!(rx.recv().await.is_err(), "no unverified prefix escapes");
    assert_eq!(
        pin(&stores.cas).has(key.into()).await.unwrap_err().code,
        Code::DataLoss
    );
    assert_eq!(
        pin(&stores.cas)
            .update_oneshot(key.into(), Bytes::from_static(b"good"))
            .await
            .unwrap_err()
            .code,
        Code::DataLoss
    );
    assert_eq!(
        fs::read(fixture.path(false, &leaf)).unwrap(),
        b"evil",
        "corrupt destination is never replaced"
    );
    fs::OpenOptions::new()
        .write(true)
        .open(fixture.path(false, &leaf))
        .unwrap()
        .set_len(MAX_OBJECT_BYTES + 1)
        .unwrap();
    assert_eq!(
        pin(&stores.cas).has(key.into()).await.unwrap_err().code,
        Code::DataLoss
    );
    pin(&stores.sink)
        .update_oneshot(key.into(), Bytes::from_static(b"result"))
        .await
        .unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(fixture.path(true, &leaf))
        .unwrap()
        .set_len(MAX_OBJECT_BYTES + 1)
        .unwrap();
    assert_eq!(
        pin(&stores.sink).has(key.into()).await.unwrap_err().code,
        Code::DataLoss
    );
    assert_eq!(
        pin(&stores.sink)
            .get_part_unchunked(key.into(), 0, Some(1))
            .await
            .unwrap_err()
            .code,
        Code::DataLoss
    );
}

#[nativelink_macro::nativelink_test]
async fn shared_admission_has_no_wait_queue_or_nested_oneshot_permit() {
    let (_fixture, stores) = Fixture::new().await;
    let permits = stores.cas.0.shared.permits.clone();
    let three = permits.clone().try_acquire_many_owned(3).unwrap();
    let key = digest(DigestHasherFunc::Sha256, b"x");
    pin(&stores.cas)
        .update_oneshot(key.into(), Bytes::from_static(b"x"))
        .await
        .unwrap();
    let fourth = permits.clone().try_acquire_owned().unwrap();
    assert_eq!(
        pin(&stores.cas).has(key.into()).await.unwrap_err().code,
        Code::ResourceExhausted
    );
    assert_eq!(
        pin(&stores.sink)
            .update_oneshot(key.into(), Bytes::new())
            .await
            .unwrap_err()
            .code,
        Code::ResourceExhausted
    );
    drop((three, fourth));
    assert_eq!(permits.available_permits(), 4);
}

#[nativelink_macro::nativelink_test]
async fn cancelled_blocking_write_retains_all_locks_and_permit_until_finished() {
    let (fixture, stores) = Fixture::new().await;
    let gate = stores.cas.0.shared.hooks.install(HookPoint::Write, false);
    let permits = stores.cas.0.shared.permits.clone();
    let cas = stores.cas.clone();
    let key = digest(DigestHasherFunc::Sha256, b"in flight");
    let task = spawn!("cancelled_write", async move {
        pin(&cas)
            .update_oneshot(key.into(), Bytes::from_static(b"in flight"))
            .await
    });
    gate.wait().await;
    drop(task);
    drop(stores);
    assert_eq!(permits.available_permits(), 3);
    assert!(
        Ownership::reopen(&fixture.manifest).is_err(),
        "blocking closure retains ownership"
    );
    gate.release();
    timeout(Duration::from_secs(5), async {
        loop {
            if permits.available_permits() == 4 && Ownership::reopen(&fixture.manifest).is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(fixture.temporary_count(false), 1);
    assert_eq!(
        fs::read_dir(Path::new(&fixture.manifest.cas.root).join("content"))
            .unwrap()
            .count(),
        0
    );
}

#[nativelink_macro::nativelink_test]
async fn has_acknowledgement_waits_for_existing_file_and_directory_sync() {
    let (_fixture, stores) = Fixture::new().await;
    let key = digest(DigestHasherFunc::Sha256, b"persisted");
    pin(&stores.cas)
        .update_oneshot(key.into(), Bytes::from_static(b"persisted"))
        .await
        .unwrap();
    let gate = stores
        .cas
        .0
        .shared
        .hooks
        .install(HookPoint::DirectorySync, true);
    let cas = stores.cas.clone();
    let mut task = spawn!(
        "has_barrier",
        async move { pin(&cas).has(key.into()).await }
    );
    gate.wait().await;
    assert!(timeout(Duration::from_millis(20), &mut task).await.is_err());
    gate.release();
    assert_eq!(task.await.unwrap().unwrap_err().code, Code::Unavailable);
}

#[nativelink_macro::nativelink_test]
async fn same_digest_key_is_partitioned_by_algorithm() {
    let (fixture, stores) = Fixture::new().await;
    let key = digest(DigestHasherFunc::Sha256, b"qualified bytes");
    pin(&stores.cas)
        .update_oneshot(key.into(), Bytes::from_static(b"qualified bytes"))
        .await
        .unwrap();
    let sha = stores
        .cas
        .0
        .key(&key.into(), DigestHasherFunc::Sha256)
        .unwrap();
    assert!(fixture.path(false, &sha.leaf).exists());
    assert_eq!(
        pin(&stores.cas)
            .has(key.into())
            .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3).unwrap())
            .await
            .unwrap(),
        None
    );
    for (algorithm, value) in [
        (DigestHasherFunc::Sha256, "sha value"),
        (DigestHasherFunc::Blake3, "blake value"),
    ] {
        pin(&stores.sink)
            .update_oneshot(key.into(), Bytes::from_static(value.as_bytes()))
            .with_context(make_ctx_for_hash_func(algorithm).unwrap())
            .await
            .unwrap();
    }
    for (algorithm, value) in [
        (DigestHasherFunc::Sha256, "sha value"),
        (DigestHasherFunc::Blake3, "blake value"),
    ] {
        assert_eq!(
            pin(&stores.sink)
                .get_part_unchunked(key.into(), 0, None)
                .with_context(make_ctx_for_hash_func(algorithm).unwrap())
                .await
                .unwrap(),
            value
        );
    }
}

#[nativelink_macro::nativelink_test]
async fn actual_uploads_share_four_permits_and_release_after_cancellation() {
    let (fixture, stores) = Fixture::new().await;
    let permits = stores.cas.0.shared.permits.clone();
    let mut writers = Vec::new();
    let mut tasks = Vec::new();
    for index in 0..MAX_ACTIVE_OPERATIONS {
        let (tx, rx) = make_buf_channel_pair();
        writers.push(tx);
        let sink = stores.sink.clone();
        tasks.push(spawn!("waiting_upload", async move {
            pin(&sink)
                .update(
                    DigestInfo::new([u8::try_from(index).unwrap(); 32], 1).into(),
                    rx,
                    UploadSizeInfo::ExactSize(1),
                )
                .await
        }));
    }
    timeout(Duration::from_secs(5), async {
        while permits.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        pin(&stores.cas)
            .has(DigestInfo::new([9; 32], 1).into())
            .await
            .unwrap_err()
            .code,
        Code::ResourceExhausted
    );
    drop(tasks);
    drop(writers);
    timeout(Duration::from_secs(5), async {
        while permits.available_permits() != 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Cancellation can precede or follow staging creation, never publication.
    assert!(fixture.temporary_count(true) <= 4);
    assert_eq!(
        fs::read_dir(fixture.directory.join("sink/content"))
            .unwrap()
            .count(),
        0
    );
}

#[nativelink_macro::nativelink_test]
async fn exact_object_cap_is_accepted_and_one_more_byte_is_refused() {
    let (_fixture, stores) = Fixture::new().await;
    let key = DigestInfo::new([0x77; 32], 999);
    let (mut tx, rx) = make_buf_channel_pair();
    let send = async move {
        for _ in 0..64 {
            tx.send(Bytes::from(vec![7; MAX_CHUNK_BYTES])).await?;
        }
        tx.send_eof()
    };
    let ((), size) = futures::try_join!(
        send,
        pin(&stores.sink).update(key.into(), rx, UploadSizeInfo::ExactSize(MAX_OBJECT_BYTES))
    )
    .unwrap();
    assert_eq!(size, MAX_OBJECT_BYTES);
    assert_eq!(
        pin(&stores.sink).has(key.into()).await.unwrap(),
        Some(MAX_OBJECT_BYTES)
    );
    let (mut tx, rx) = make_buf_channel_pair();
    let send = async move {
        for _ in 0..64 {
            tx.send(Bytes::from(vec![8; MAX_CHUNK_BYTES])).await?;
        }
        tx.send(Bytes::from_static(b"!")).await?;
        tx.send_eof()
    };
    let error = futures::try_join!(
        send,
        pin(&stores.sink).update(
            key.into(),
            rx,
            UploadSizeInfo::ExactSize(MAX_OBJECT_BYTES + 1)
        )
    )
    .unwrap_err();
    assert_eq!(error.code, Code::ResourceExhausted);
    assert_eq!(
        pin(&stores.sink)
            .get_part_unchunked(key.into(), MAX_OBJECT_BYTES - 1, Some(1))
            .await
            .unwrap(),
        &[7][..]
    );
}

// Independent cancellation witness written after reading the frozen adapter.
#[nativelink_macro::nativelink_test]
async fn independent_held_reads_share_admission_and_retain_each_root_until_cancelled() {
    let (fixture, stores) = Fixture::new().await;
    let payload = Bytes::from(vec![0x6d; IO_BUFFER_BYTES * 8]);
    let key = digest(DigestHasherFunc::Sha256, &payload);
    pin(&stores.cas)
        .update_oneshot(key.into(), payload.clone())
        .await
        .unwrap();
    pin(&stores.sink)
        .update_oneshot(key.into(), payload.clone())
        .await
        .unwrap();

    let mut tasks = Vec::new();
    let mut readers = Vec::new();
    for index in 0..MAX_ACTIVE_OPERATIONS {
        let store: Arc<dyn StoreDriver> = if index % 2 == 0 {
            stores.cas.clone()
        } else {
            stores.sink.clone()
        };
        let (mut writer, mut reader) = make_buf_channel_pair();
        let mut task = spawn!("independent_held_read", async move {
            Pin::new(store.as_ref())
                .get_part(key.into(), &mut writer, 0, None)
                .await
        });
        // Receiving a first chunk witnesses completion of whole-object
        // verification. Eight chunks cannot fit the two-element output queue,
        // even after this single receive, so these reads remain in output.
        let first = timeout(Duration::from_secs(5), reader.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.as_ref(), &payload[..IO_BUFFER_BYTES]);
        assert!(timeout(Duration::from_millis(20), &mut task).await.is_err());
        tasks.push(task);
        readers.push(reader);
    }
    assert_eq!(
        pin(&stores.cas).has(key.into()).await.unwrap_err().code,
        Code::ResourceExhausted,
        "held CAS and sink outputs consume the same four-operation admission"
    );

    drop(tasks.pop().unwrap());
    timeout(Duration::from_secs(5), async {
        loop {
            match pin(&stores.cas).has(key.into()).await {
                Ok(size) => {
                    assert_eq!(size, Some(payload.len() as u64));
                    break;
                }
                Err(error) => assert_eq!(error.code, Code::ResourceExhausted),
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let probes = [
        &fixture.manifest.authority.journal_root,
        &fixture.manifest.cas.root,
        &fixture.manifest.temporary_action_cache.root,
        &fixture.manifest.worker.root,
    ]
    .map(|path| fs::File::open(path).unwrap());
    drop(stores);
    for file in &probes {
        assert!(
            matches!(file.try_lock(), Err(fs::TryLockError::WouldBlock)),
            "a pending output retains each namespace lock independently"
        );
    }
    drop(tasks);
    drop(readers);
    timeout(Duration::from_secs(5), async {
        for file in &probes {
            loop {
                match file.try_lock() {
                    Ok(()) => break,
                    Err(fs::TryLockError::WouldBlock) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected namespace lock error: {error:?}"),
                }
            }
        }
    })
    .await
    .unwrap();
    drop(probes);
    assert!(Ownership::reopen(&fixture.manifest).is_ok());
}
