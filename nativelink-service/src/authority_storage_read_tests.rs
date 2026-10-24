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

//! Producer completion, channel EOF and collector cancellation are separate.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{RemoveCallback, Store, StoreDriver, StoreKey, UploadSizeInfo};
use tokio::time::timeout;

use super::{FRAME_BYTES, bounded_blob};

#[derive(Clone, Copy, Debug)]
enum Finish {
    Eof,
    ReturnWithoutEof,
    Error,
    EofThenError,
    Pending,
    EofThenPending,
}

#[derive(Debug, MetricsComponent)]
struct Reader {
    chunks: Vec<Bytes>,
    finish: Finish,
    calls: AtomicUsize,
    cancelled: AtomicUsize,
}

struct Cancelled<'a>(&'a AtomicUsize);
impl Drop for Cancelled<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tonic::async_trait]
impl StoreDriver for Reader {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        _keys: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        panic!("bounded read must not query metadata");
    }

    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size_info: UploadSizeInfo,
    ) -> Result<u64, Error> {
        panic!("bounded read must not write");
    }

    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(key, DigestInfo::new([7; 32], 3).into());
        assert_eq!(offset, 0);
        assert_eq!(length, Some(65_537));
        // Excess cases deliberately disregard the requested range: the
        // collector must inspect actual chunks even from a faulty producer.
        for chunk in &self.chunks {
            writer.send(chunk.clone()).await?;
        }
        match self.finish {
            Finish::Eof => writer.send_eof(),
            Finish::ReturnWithoutEof => Ok(()),
            Finish::Error => Err(make_err!(Code::Unavailable, "read fixture failure")),
            Finish::EofThenError => {
                writer.send_eof()?;
                tokio::task::yield_now().await;
                Err(make_err!(
                    Code::Unavailable,
                    "read fixture failure after EOF"
                ))
            }
            Finish::Pending | Finish::EofThenPending => {
                if matches!(self.finish, Finish::EofThenPending) {
                    writer.send_eof()?;
                }
                let _cancelled = Cancelled(&self.cancelled);
                futures::future::pending().await
            }
        }
    }

    fn inner_store(&self, _key: Option<StoreKey>) -> &dyn StoreDriver {
        self
    }

    fn as_any(&self) -> &(dyn core::any::Any + Send + Sync + 'static) {
        self
    }

    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Send + Sync + 'static> {
        self
    }

    fn register_remove_callback(self: Arc<Self>, _callback: RemoveCallback) -> Result<(), Error> {
        Ok(())
    }
}

default_health_status_indicator!(Reader);

fn fixture(chunks: Vec<Bytes>, finish: Finish) -> (Store, Arc<Reader>) {
    let reader = Arc::new(Reader {
        chunks,
        finish,
        calls: AtomicUsize::new(0),
        cancelled: AtomicUsize::new(0),
    });
    (Store::new(reader.clone()), reader)
}

async fn read(store: &Store) -> Result<Bytes, Error> {
    timeout(
        Duration::from_secs(1),
        bounded_blob(store, DigestInfo::new([7; 32], 3)),
    )
    .await
    .expect("completed producer must not leave the bounded collector waiting")
}

#[nativelink_macro::nativelink_test]
async fn producer_success_without_eof_fails_for_empty_and_partial_data() {
    for chunks in [Vec::new(), vec![Bytes::from_static(b"partial")]] {
        let (store, reader) = fixture(chunks, Finish::ReturnWithoutEof);
        let error = read(&store).await.unwrap_err();
        assert_eq!(error.code, Code::Internal);
        assert!(
            error
                .to_string()
                .contains("Sender dropped before sending EOF")
        );
        assert_eq!(reader.calls.load(Ordering::Relaxed), 1);
    }
}

#[nativelink_macro::nativelink_test]
async fn producer_failure_is_preserved_before_and_after_eof() {
    for finish in [Finish::Error, Finish::EofThenError] {
        let (store, reader) = fixture(vec![Bytes::from_static(b"value")], finish);
        let error = read(&store).await.unwrap_err();
        assert_eq!(error.code, Code::Unavailable);
        assert!(error.to_string().contains("read fixture failure"));
        assert_eq!(reader.calls.load(Ordering::Relaxed), 1);
    }
}

#[nativelink_macro::nativelink_test]
async fn empty_and_exact_bound_reads_require_eof_and_producer_success() {
    for chunks in [
        Vec::new(),
        vec![
            Bytes::from(vec![1; FRAME_BYTES / 2]),
            Bytes::from(vec![2; FRAME_BYTES / 2]),
        ],
    ] {
        let expected = Bytes::from(chunks.concat());
        let (store, reader) = fixture(chunks, Finish::Eof);
        assert_eq!(read(&store).await.unwrap(), expected);
        assert_eq!(reader.calls.load(Ordering::Relaxed), 1);
    }
}

#[nativelink_macro::nativelink_test]
async fn excess_bytes_cancel_pending_producer_without_draining() {
    for size in [FRAME_BYTES + 1, FRAME_BYTES * 2] {
        let (store, reader) = fixture(vec![Bytes::from(vec![3; size])], Finish::Pending);
        assert_eq!(
            read(&store).await.unwrap_err().code,
            Code::ResourceExhausted
        );
        assert_eq!(reader.calls.load(Ordering::Relaxed), 1);
        assert_eq!(reader.cancelled.load(Ordering::Relaxed), 1);
    }
}

#[nativelink_macro::nativelink_test]
async fn eof_does_not_finish_a_still_pending_producer() {
    let (store, reader) = fixture(vec![Bytes::from_static(b"value")], Finish::EofThenPending);
    assert!(
        timeout(
            Duration::from_millis(20),
            bounded_blob(&store, DigestInfo::new([7; 32], 3)),
        )
        .await
        .is_err()
    );
    assert_eq!(reader.cancelled.load(Ordering::Relaxed), 1);
}
