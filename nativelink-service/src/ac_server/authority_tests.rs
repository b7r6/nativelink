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

//! Fixed AC admission oracles. A ranged producer must deliver the complete
//! requested range; EOF alone cannot certify arbitrary store implementations.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::digest_hasher::{DigestHasherFunc, digest_hasher_func_from_context};
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{RemoveCallback, StoreDriver, StoreKey, UploadSizeInfo};
use tokio::time::timeout;

use super::*;

#[path = "authority_rpc_tests.rs"]
mod rpc;

const CAP: usize = 1024 * 1024;
const INSTANCE: &str = "fixed-ac-admission";

fn legacy(store: Store) -> AcServer {
    let manager = StoreManager::new();
    manager.add_store("sink", store).unwrap();
    AcServer::new(
        &[WithInstanceName {
            instance_name: INSTANCE.to_owned(),
            config: AcStoreConfig {
                ac_store: "sink".to_owned(),
                read_only: false,
            },
        }],
        &manager,
    )
    .unwrap()
}

fn fixed(store: Store) -> AcServer {
    // The first qualification run deliberately exercises the existing path.
    // Only this constructor shim changes when fixed admission is implemented.
    legacy(store)
}

fn key(size: i64) -> Digest {
    Digest {
        hash: "1".repeat(64),
        size_bytes: size,
    }
}

fn get(size: i64, algorithm: DigestHasherFunc) -> GetActionResultRequest {
    GetActionResultRequest {
        instance_name: INSTANCE.to_owned(),
        action_digest: Some(key(size)),
        digest_function: algorithm.proto_digest_func().into(),
        inline_stdout: true,
        inline_stderr: true,
        inline_output_files: vec!["large-output".into()],
    }
}

fn update(
    size: i64,
    algorithm: DigestHasherFunc,
    result: ActionResult,
) -> UpdateActionResultRequest {
    UpdateActionResultRequest {
        instance_name: INSTANCE.to_owned(),
        action_digest: Some(key(size)),
        digest_function: algorithm.proto_digest_func().into(),
        action_result: Some(result),
        ..UpdateActionResultRequest::default()
    }
}

fn filled_result(encoded_size: usize) -> ActionResult {
    for overhead in 1..10 {
        let value = ActionResult {
            stdout_raw: vec![7; encoded_size - overhead].into(),
            ..ActionResult::default()
        };
        if value.encoded_len() == encoded_size {
            return value;
        }
    }
    panic!("no exact result size");
}

fn unknown_value(size: usize) -> Bytes {
    for overhead in 3..10 {
        let padding = size - overhead;
        let mut bytes = vec![0xfa, 0x07]; // Unknown field 127, length-delimited.
        prost::encoding::encode_varint(padding as u64, &mut bytes);
        bytes.resize(bytes.len() + padding, 0);
        if bytes.len() == size {
            assert_eq!(
                ActionResult::decode(bytes.as_slice()).unwrap(),
                ActionResult::default()
            );
            return bytes.into();
        }
    }
    panic!("no exact unknown-field size");
}

#[derive(Clone, Copy, Debug)]
enum Finish {
    Eof,
    Drop,
    Error,
    EofError,
    Pending,
}

#[derive(Clone, Debug)]
struct Value {
    bytes: Bytes,
    finish: Finish,
    chunk_size: usize,
}

#[derive(Debug, Default, MetricsComponent)]
struct Spy {
    values: Mutex<HashMap<(DigestHasherFunc, DigestInfo), Value>>,
    ranges: Mutex<Vec<(u64, Option<u64>)>>,
    writes: AtomicUsize,
    has: AtomicUsize,
    cancelled: AtomicUsize,
}

impl Spy {
    fn put(&self, size: i64, algorithm: DigestHasherFunc, bytes: Bytes, finish: Finish) {
        self.values.lock().unwrap().insert(
            (algorithm, DigestInfo::try_from(key(size)).unwrap()),
            Value {
                bytes,
                finish,
                chunk_size: 64 * 1024,
            },
        );
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
        _keys: &[StoreKey<'_>],
        _results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.has.fetch_add(1, Ordering::Relaxed);
        panic!("AC admission must not rely on racy has metadata")
    }
    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        mut reader: DropCloserReadHalf,
        _size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let StoreKey::Digest(key) = key else {
            panic!("digest key required")
        };
        let bytes = reader.consume(None).await?;
        let size = bytes.len() as u64;
        self.values.lock().unwrap().insert(
            (digest_hasher_func_from_context(), key),
            Value {
                bytes,
                finish: Finish::Eof,
                chunk_size: 64 * 1024,
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
        self.ranges.lock().unwrap().push((offset, length));
        assert_eq!(offset, 0);
        let StoreKey::Digest(key) = key else {
            panic!("digest key required")
        };
        let value = self
            .values
            .lock()
            .unwrap()
            .get(&(digest_hasher_func_from_context(), key))
            .cloned()
            .ok_or_else(|| make_err!(Code::NotFound, "missing fixture entry"))?;
        let _cancelled =
            matches!(value.finish, Finish::Pending).then(|| Cancelled(&self.cancelled));
        // A conforming range read delivers all available bytes up to length.
        let end = length.map_or(value.bytes.len(), |length| {
            usize::try_from(length).unwrap().min(value.bytes.len())
        });
        for chunk in value.bytes[..end].chunks(value.chunk_size) {
            writer.send(Bytes::copy_from_slice(chunk)).await?;
        }
        match value.finish {
            Finish::Eof => writer.send_eof(),
            Finish::Drop => Ok(()),
            Finish::Error => Err(make_err!(
                Code::Unavailable,
                "{}",
                "backend diagnostic ".repeat(1024)
            )),
            Finish::EofError => {
                writer.send_eof()?;
                Err(make_err!(
                    Code::Unavailable,
                    "{}",
                    "backend diagnostic ".repeat(1024)
                ))
            }
            Finish::Pending => futures::future::pending().await,
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

#[nativelink_test]
async fn opaque_keys_and_context_are_preserved_without_value_hash_or_size_checks() {
    let spy = Arc::new(Spy::default());
    let server = fixed(Store::new(spy.clone()));
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        for size in [0, 147, i64::MAX] {
            let value = ActionResult {
                exit_code: 45,
                stdout_raw: Bytes::from_static(b"bounded raw content"),
                ..ActionResult::default()
            };
            let response = server
                .update_action_result(Request::new(update(size, algorithm, value.clone())))
                .await
                .unwrap();
            assert_eq!(response.into_inner(), value);
            assert_eq!(
                server
                    .get_action_result(Request::new(get(size, algorithm)))
                    .await
                    .unwrap()
                    .into_inner(),
                value
            );
        }
    }
    assert_eq!(spy.writes.load(Ordering::Relaxed), 6);
    assert_eq!(
        *spy.ranges.lock().unwrap(),
        vec![(0, Some(CAP as u64 + 1)); 6]
    );
    assert_eq!(spy.has.load(Ordering::Relaxed), 0);
}

#[nativelink_test]
async fn raw_unknown_fields_are_bounded_before_decode() {
    let spy = Arc::new(Spy::default());
    let store = Store::new(spy.clone());
    let server = fixed(store.clone());
    let algorithm = DigestHasherFunc::Sha256;
    spy.put(147, algorithm, unknown_value(CAP), Finish::Eof);
    assert_eq!(
        server
            .get_action_result(Request::new(get(147, algorithm)))
            .await
            .unwrap()
            .into_inner(),
        ActionResult::default()
    );
    spy.put(147, algorithm, unknown_value(CAP + 1), Finish::Eof);
    assert_eq!(
        server
            .get_action_result(Request::new(get(147, algorithm)))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
}

#[nativelink_test]
async fn sentinel_detects_valid_cap_prefix_with_semantic_tail() {
    let spy = Arc::new(Spy::default());
    let mut bytes = unknown_value(CAP).to_vec();
    bytes.extend_from_slice(&[0x20, 7]);
    assert_eq!(ActionResult::decode(bytes.as_slice()).unwrap().exit_code, 7);
    spy.put(147, DigestHasherFunc::Sha256, bytes.into(), Finish::Eof);
    assert_eq!(
        fixed(Store::new(spy))
            .get_action_result(Request::new(get(147, DigestHasherFunc::Sha256)))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
}

#[nativelink_test]
async fn valid_ten_mib_prefix_cannot_hide_a_semantic_tail() {
    let spy = Arc::new(Spy::default());
    let store = Store::new(spy.clone());
    let server = fixed(store.clone());
    let algorithm = DigestHasherFunc::Sha256;
    let mut bytes = unknown_value(10 * CAP).to_vec();
    bytes.extend_from_slice(&[0x20, 7]); // A semantic field beyond the legacy prefix.
    assert_eq!(ActionResult::decode(bytes.as_slice()).unwrap().exit_code, 7);
    spy.put(147, algorithm, bytes.into(), Finish::Eof);
    assert_eq!(
        legacy(store)
            .get_action_result(Request::new(get(147, algorithm)))
            .await
            .unwrap()
            .into_inner()
            .exit_code,
        0
    );
    assert_eq!(
        server
            .get_action_result(Request::new(get(147, algorithm)))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
}

#[nativelink_test]
async fn complete_range_requires_eof_and_successful_producer_and_bounds_diagnostics() {
    let spy = Arc::new(Spy::default());
    let server = fixed(Store::new(spy.clone()));
    for (finish, expected) in [
        (Finish::Drop, Code::Internal),
        (Finish::Error, Code::Unavailable),
        (Finish::EofError, Code::Unavailable),
    ] {
        spy.put(
            147,
            DigestHasherFunc::Sha256,
            Bytes::from_static(&[0x20, 7]),
            finish,
        );
        let error = timeout(
            Duration::from_secs(2),
            server.get_action_result(Request::new(get(147, DigestHasherFunc::Sha256))),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code(), expected);
        assert!(
            error.message().len() <= 256,
            "backend diagnostic escaped the fixed boundary"
        );
    }
}

#[nativelink_test]
async fn excess_cancels_pending_producer_without_waiting_for_eof() {
    let spy = Arc::new(Spy::default());
    spy.put(
        147,
        DigestHasherFunc::Sha256,
        unknown_value(CAP + 1),
        Finish::Pending,
    );
    // One oversized chunk also exercises checking before collector copy.
    spy.values
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .chunk_size = CAP + 1;
    let server = fixed(Store::new(spy.clone()));
    let error = timeout(
        Duration::from_secs(1),
        server.get_action_result(Request::new(get(147, DigestHasherFunc::Sha256))),
    )
    .await
    .expect("collector waited for oversized producer")
    .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(spy.cancelled.load(Ordering::Relaxed), 1);
}

#[nativelink_test(start_paused = true)]
async fn fixed_timeout_cancels_incomplete_range() {
    let spy = Arc::new(Spy::default());
    spy.put(
        147,
        DigestHasherFunc::Sha256,
        Bytes::from_static(&[0x20, 7]),
        Finish::Pending,
    );
    let server = fixed(Store::new(spy.clone()));
    let error = timeout(
        Duration::from_secs(31),
        server.get_action_result(Request::new(get(147, DigestHasherFunc::Sha256))),
    )
    .await
    .expect("fixed thirty-second request deadline did not fire")
    .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert_eq!(spy.cancelled.load(Ordering::Relaxed), 1);
}

#[nativelink_test]
async fn decoded_response_growth_is_checked_before_encoding() {
    let spy = Arc::new(Spy::default());
    let mut bytes = filled_result(CAP - 6).encode_to_vec();
    // A five-byte int32 encoding decodes to -1, which prost reencodes in ten.
    bytes.extend_from_slice(&[0x20, 0xff, 0xff, 0xff, 0xff, 0x0f]);
    assert_eq!(bytes.len(), CAP);
    let decoded = ActionResult::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded.exit_code, -1);
    assert_eq!(decoded.encoded_len(), CAP + 5);
    spy.put(147, DigestHasherFunc::Sha256, bytes.into(), Finish::Eof);
    assert_eq!(
        fixed(Store::new(spy))
            .get_action_result(Request::new(get(147, DigestHasherFunc::Sha256)))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
}

#[nativelink_test]
async fn update_checks_serialized_cap_before_any_write_and_accepts_exact_boundary() {
    let spy = Arc::new(Spy::default());
    let server = fixed(Store::new(spy.clone()));
    let algorithm = DigestHasherFunc::Blake3;
    let over = filled_result(CAP + 1);
    assert_eq!(
        server
            .update_action_result(Request::new(update(0, algorithm, over)))
            .await
            .unwrap_err()
            .code(),
        Code::ResourceExhausted
    );
    assert_eq!(spy.writes.load(Ordering::Relaxed), 0);
    let exact = filled_result(CAP);
    assert_eq!(
        server
            .update_action_result(Request::new(update(0, algorithm, exact.clone())))
            .await
            .unwrap()
            .into_inner(),
        exact
    );
    assert_eq!(spy.writes.load(Ordering::Relaxed), 1);
    assert_eq!(
        server
            .get_action_result(Request::new(get(0, algorithm)))
            .await
            .unwrap()
            .into_inner(),
        exact
    );
}
