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

//! Channel EOF and the producer's successful result are separate conditions.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, stream};
use nativelink_error::{Code, Error, make_err};
use nativelink_metric::MetricsComponent;
use nativelink_proto::google::bytestream::ReadRequest;
use nativelink_proto::google::bytestream::byte_stream_client::ByteStreamClient;
use nativelink_proto::google::bytestream::byte_stream_server::ByteStream;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{RemoveCallback, StoreDriver, StoreKey, UploadSizeInfo};
use tokio::net::TcpListener;
use tokio::sync::{Notify, oneshot};
use tokio::time::timeout;
use tonic::Request;
use tonic::transport::{Endpoint, Server};

use super::{ByteStreamConfig, ByteStreamServer, Store, StoreManager, WithInstanceName};
use crate::wire_compression::RemoteCacheCompressionInstances;

#[derive(Clone, Copy, Debug)]
enum Finish {
    Success,
    NoEof,
    Error,
    EofError,
    EofYieldError,
    EofGatedError,
    EofPending,
}

#[derive(Debug, MetricsComponent)]
struct Reader {
    chunks: Vec<Bytes>,
    finish: Finish,
    entered: AtomicUsize,
    dropped: AtomicUsize,
    gate: Notify,
}

struct Dropped<'a>(&'a AtomicUsize);
impl Drop for Dropped<'_> {
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
        panic!("read must not query metadata")
    }
    async fn update(
        self: Pin<&Self>,
        _key: StoreKey<'_>,
        _reader: DropCloserReadHalf,
        _size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        panic!("read must not write")
    }
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        assert_eq!(key, DigestInfo::new([7; 32], 3).into());
        assert_eq!(offset, 0);
        assert_eq!(length, None);
        self.entered.fetch_add(1, Ordering::Relaxed);
        let _dropped = Dropped(&self.dropped);
        for chunk in &self.chunks {
            writer.send(chunk.clone()).await?;
        }
        match self.finish {
            Finish::Success => writer.send_eof(),
            Finish::NoEof => Ok(()),
            Finish::Error => Err(make_err!(Code::Unavailable, "read completion witness")),
            Finish::EofError | Finish::EofYieldError | Finish::EofGatedError => {
                writer.send_eof()?;
                if matches!(self.finish, Finish::EofYieldError) {
                    tokio::task::yield_now().await;
                }
                if matches!(self.finish, Finish::EofGatedError) {
                    self.gate.notified().await;
                }
                Err(make_err!(Code::Unavailable, "read completion witness"))
            }
            Finish::EofPending => {
                writer.send_eof()?;
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

fn fixture(chunks: Vec<Bytes>, finish: Finish) -> (ByteStreamServer, Arc<Reader>) {
    let reader = Arc::new(Reader {
        chunks,
        finish,
        entered: AtomicUsize::new(0),
        dropped: AtomicUsize::new(0),
        gate: Notify::new(),
    });
    let manager = StoreManager::new();
    manager
        .add_store("witness", Store::new(reader.clone()))
        .unwrap();
    let server = ByteStreamServer::new(
        &[WithInstanceName {
            instance_name: "completion".to_owned(),
            config: ByteStreamConfig {
                cas_store: "witness".to_owned(),
                max_bytes_per_stream: 64 * 1024,
                persist_stream_on_disconnect_timeout_s: 1,
            },
        }],
        &manager,
        &RemoteCacheCompressionInstances::default(),
    )
    .unwrap();
    (server, reader)
}

fn request() -> ReadRequest {
    let digest = DigestInfo::new([7; 32], 3);
    ReadRequest {
        resource_name: format!("completion/blobs/sha256/{}/3", digest.packed_hash()),
        read_offset: 0,
        read_limit: 0,
    }
}

async fn collect(server: &ByteStreamServer) -> Result<Vec<Bytes>, tonic::Status> {
    timeout(Duration::from_secs(2), async {
        let mut response = server.read(Request::new(request())).await?.into_inner();
        let mut chunks = Vec::new();
        while let Some(chunk) = response.next().await {
            chunks.push(chunk?.data);
        }
        Ok(chunks)
    })
    .await
    .expect("producer completion must resolve the response")
}

#[nativelink_macro::nativelink_test]
async fn producer_error_after_eof_remains_a_stream_error() {
    for finish in [Finish::EofError, Finish::EofYieldError] {
        for chunks in [Vec::new(), vec![Bytes::from_static(b"abc")]] {
            let (server, reader) = fixture(chunks, finish);
            let error = collect(&server).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::Unavailable);
            assert!(error.message().contains("read completion witness"));
            assert_eq!(reader.entered.load(Ordering::Relaxed), 1);
            assert_eq!(reader.dropped.load(Ordering::Relaxed), 1);
        }
    }
}

#[nativelink_macro::nativelink_test]
async fn eof_waits_for_producer_and_stream_cancellation_releases_it() {
    let (server, reader) = fixture(vec![Bytes::from_static(b"abc")], Finish::EofPending);
    let mut response = server
        .read(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    let first = timeout(Duration::from_secs(2), response.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.data.as_ref(), b"abc");
    assert!(
        timeout(Duration::from_millis(20), response.next())
            .await
            .is_err(),
        "EOF cannot acknowledge a still-pending producer"
    );
    assert_eq!(reader.dropped.load(Ordering::Relaxed), 0);
    drop(response);
    assert_eq!(reader.dropped.load(Ordering::Relaxed), 1);
}

#[nativelink_macro::nativelink_test]
async fn successful_eof_keeps_empty_and_chunked_reads() {
    for chunks in [
        Vec::new(),
        vec![Bytes::from_static(b"abc")],
        vec![Bytes::from(vec![9; 131_079])],
    ] {
        let expected: Vec<_> = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect();
        let (server, reader) = fixture(chunks, Finish::Success);
        let received = collect(&server).await.unwrap();
        assert!(received.iter().all(|chunk| chunk.len() <= 65_536));
        assert_eq!(received.concat(), expected);
        assert_eq!(reader.dropped.load(Ordering::Relaxed), 1);
    }
}

#[nativelink_macro::nativelink_test]
async fn missing_eof_and_early_error_still_fail() {
    for (finish, expected) in [
        (Finish::NoEof, tonic::Code::Internal),
        (Finish::Error, tonic::Code::Unavailable),
    ] {
        for chunks in [Vec::new(), vec![Bytes::from_static(b"abc")]] {
            let (server, reader) = fixture(chunks, finish);
            assert_eq!(collect(&server).await.unwrap_err().code(), expected);
            assert_eq!(reader.dropped.load(Ordering::Relaxed), 1);
        }
    }
}

#[nativelink_macro::nativelink_test]
async fn http2_reports_late_backend_failure_after_delivered_bytes() {
    let (server, reader) = fixture(vec![Bytes::from_static(b"abc")], Finish::EofGatedError);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = oneshot::channel();
    let task = nativelink_util::background_spawn!("read_completion_http2", async move {
        let incoming = stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(socket, _)| socket), listener))
        });
        Server::builder()
            .add_service(server.into_service())
            .serve_with_incoming_shutdown(incoming, async {
                let _stopped = stopped.await;
            })
            .await
            .unwrap();
    });
    let channel = Endpoint::from_shared(address)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = ByteStreamClient::new(channel);
    let mut response = client.read(request()).await.unwrap().into_inner();
    assert_eq!(
        timeout(Duration::from_secs(2), response.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .data
            .as_ref(),
        b"abc"
    );
    reader.gate.notify_one();
    let error = timeout(Duration::from_secs(2), response.message())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().contains("read completion witness"));
    drop(response);
    drop(client);
    stop.send(()).unwrap();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reader.dropped.load(Ordering::Relaxed), 1);
}
