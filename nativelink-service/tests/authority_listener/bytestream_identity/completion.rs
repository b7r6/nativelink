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

//! Completed backend futures cannot supply resumable upload state. These HTTP/2
//! fixtures distinguish terminal backend work from an interrupted client whose
//! backend remains pending. The early-success backend is deliberately invalid;
//! it tests future ownership, not content integrity or acknowledgement semantics.

use super::*;

#[derive(Clone, Copy, Debug)]
enum FirstCompletion {
    ErrorBeforeEof,
    ErrorAfterEof,
    EarlySuccess,
}

#[derive(Debug, MetricsComponent)]
struct CompleteOnce {
    inner: Arc<Spy>,
    calls: AtomicUsize,
    completed: AtomicUsize,
    first: FirstCompletion,
}

#[tonic::async_trait]
impl StoreDriver for CompleteOnce {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }

    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        Pin::new(&*self.inner).has_with_results(keys, results).await
    }

    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        if self.calls.fetch_add(1, Ordering::Relaxed) != 0 {
            return Pin::new(&*self.inner).update(key, reader, size).await;
        }
        let mut received = 0;
        loop {
            let chunk = reader.recv().await?;
            received += chunk.len();
            if chunk.is_empty() || !matches!(self.first, FirstCompletion::ErrorAfterEof) {
                break;
            }
        }
        self.completed.fetch_add(1, Ordering::Release);
        match self.first {
            FirstCompletion::EarlySuccess => Ok(u64::try_from(received).unwrap()),
            FirstCompletion::ErrorBeforeEof | FirstCompletion::ErrorAfterEof => {
                Err(make_err!(Code::Unavailable, "fixture backend failed"))
            }
        }
    }

    async fn get_part(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _writer: &mut DropCloserWriteHalf,
        _offset: u64,
        _length: Option<u64>,
    ) -> Result<(), Error> {
        panic!("completion fixture never downloads")
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

default_health_status_indicator!(CompleteOnce);

async fn fixture_with(first: FirstCompletion) -> (Runtime, Arc<CompleteOnce>, String, Digest) {
    let store = Arc::new(CompleteOnce {
        inner: Spy::new(false, false),
        calls: AtomicUsize::new(0),
        completed: AtomicUsize::new(0),
        first,
    });
    let runtime = Runtime::start_with_store_and_transports(
        Store::new(store.clone()),
        Server::builder(),
        Server::builder(),
        None,
        None,
    )
    .await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    (runtime, store, name, digest)
}

async fn failed(first: FirstCompletion) -> (Runtime, Arc<CompleteOnce>, String, Digest) {
    let (runtime, store, name, digest) = fixture_with(first).await;
    let finish = matches!(first, FirstCompletion::ErrorAfterEof);
    let (sender, task) = writer(runtime.worker.clone());
    sender
        .send(frame(
            name.clone(),
            0,
            finish,
            if finish { DATA } else { &DATA[..PREFIX] },
        ))
        .await
        .unwrap();
    // Keep the client's stream open until the backend status arrives. A client
    // disconnect must not be mistaken for the terminal backend failure.
    assert_eq!(result(task).await.unwrap_err().code(), Code::Unavailable);
    drop(sender);
    assert_eq!(store.calls.load(Ordering::Relaxed), 1);
    assert_eq!(store.completed.load(Ordering::Acquire), 1);
    (runtime, store, name, digest)
}

#[nativelink_test]
async fn backend_error_does_not_retain_reported_progress() {
    let (runtime, _, name, _) = failed(FirstCompletion::ErrorBeforeEof).await;
    assert_eq!(
        status(runtime.client.clone(), name).await.unwrap(),
        QueryWriteStatusResponse {
            committed_size: 0,
            complete: false,
        },
        "failed backend left progress which cannot be resumed"
    );
    runtime.shutdown().await;
}

#[nativelink_test]
async fn backend_error_nonzero_resume_refuses_without_repolling() {
    let (runtime, store, name, _) = failed(FirstCompletion::ErrorBeforeEof).await;
    let (sender, task) = writer(runtime.client.clone());
    sender.send(frame(name, PREFIX, false, &[])).await.unwrap();
    // An empty non-final frame makes the client branch await more input. It
    // cannot hide a repoll of a completed backend behind a channel-send error.
    assert_eq!(
        result(task).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    drop(sender);
    assert_eq!(store.calls.load(Ordering::Relaxed), 1);
    runtime.shutdown().await;
}

async fn fresh_retry(first: FirstCompletion) {
    let (runtime, store, name, digest) = failed(first).await;
    let response = write(runtime.client.clone(), frame(name.clone(), 0, true, DATA))
        .await
        .expect("fresh zero-offset retry retained a terminal backend future");
    assert_eq!(response.committed_size, i64::try_from(DATA.len()).unwrap());
    assert_eq!(store.calls.load(Ordering::Relaxed), 2);
    assert_eq!(
        store
            .inner
            .value(DigestHasherFunc::Sha256, &digest)
            .unwrap(),
        DATA
    );
    assert!(status(runtime.worker.clone(), name).await.unwrap().complete);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn backend_error_before_eof_allows_fresh_retry() {
    fresh_retry(FirstCompletion::ErrorBeforeEof).await;
}

#[nativelink_test]
async fn backend_error_after_eof_allows_fresh_retry() {
    fresh_retry(FirstCompletion::ErrorAfterEof).await;
}

#[nativelink_test]
async fn early_backend_success_then_client_error_does_not_park_terminal_work() {
    let (runtime, store, name, _) = fixture_with(FirstCompletion::EarlySuccess).await;
    let (sender, task) = active(&runtime, &name).await;
    tokio::time::timeout(WAIT, async {
        while store.completed.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("backend did not reach its early terminal result");
    sender
        .send(frame(String::new(), PREFIX + 1, false, b"x"))
        .await
        .unwrap();
    assert_eq!(
        result(task).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    drop(sender);
    assert_eq!(
        status(runtime.client.clone(), name)
            .await
            .unwrap()
            .committed_size,
        0,
        "client error parked an already successful backend future"
    );
    runtime.shutdown().await;
}

#[nativelink_test]
async fn client_error_preserves_pending_backend_for_exact_resume() {
    let (runtime, spy) = fixture(false, false).await;
    let digest = digest(DigestHasherFunc::Sha256, DATA);
    let name = resource(UUID, "sha256/", &digest);
    let (sender, task) = active(&runtime, &name).await;
    settled_prefix(&spy).await;
    sender
        .send(frame(String::new(), PREFIX + 1, false, b"x"))
        .await
        .unwrap();
    assert_eq!(
        result(task).await.unwrap_err().code(),
        Code::InvalidArgument
    );
    drop(sender);
    assert_eq!(
        status(runtime.client.clone(), name.clone())
            .await
            .unwrap()
            .committed_size,
        i64::try_from(PREFIX).unwrap()
    );
    write(
        runtime.worker.clone(),
        frame(name, PREFIX, true, &DATA[PREFIX..]),
    )
    .await
    .unwrap();
    assert_eq!(spy.updates.load(Ordering::Relaxed), 1);
    assert_eq!(spy.value(DigestHasherFunc::Sha256, &digest).unwrap(), DATA);
    runtime.shutdown().await;
}
