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

use bytes::{Buf, BufMut, Bytes};
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::{Request, Status};

use super::*;

// Send arbitrary protobuf bodies through real HTTP/2 framing. A malformed
// one-byte body must reach the decoder; an oversized body must fail its length
// check first, without depending on semantic validation in a handler.
struct Raw;
impl Encoder for Raw {
    type Item = Bytes;
    type Error = Status;

    fn encode(&mut self, item: Bytes, destination: &mut EncodeBuf<'_>) -> Result<(), Status> {
        destination.put_slice(&item);
        Ok(())
    }
}
impl Decoder for Raw {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, source: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(source.copy_to_bytes(source.remaining())))
    }
}
impl Codec for Raw {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = Self;
    type Decoder = Self;

    fn encoder(&mut self) -> Self {
        Self
    }

    fn decoder(&mut self) -> Self {
        Self
    }
}

async fn raw(channel: Channel, path: &'static str, bytes: Bytes) -> Result<Bytes, Status> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.unwrap();
    grpc.unary(
        Request::new(bytes),
        tonic::codegen::http::uri::PathAndQuery::from_static(path),
        Raw,
    )
    .await
    .map(tonic::Response::into_inner)
}

#[nativelink_test]
async fn every_fixed_route_rejects_oversized_and_malformed_protobuf_at_wire_decoder() {
    let runtime = Runtime::start().await;
    let cas = "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs";
    let bytestream = "/google.bytestream.ByteStream/Write";
    let cases = [
        (runtime.worker.clone(), cas, 1024 * 1024),
        (runtime.client.clone(), cas, 1024 * 1024),
        (runtime.worker.clone(), bytestream, 1024 * 1024),
        (runtime.client.clone(), bytestream, 1024 * 1024),
        (
            runtime.worker.clone(),
            "/com.github.trace_machina.nativelink.remote_execution.AuthorityWorkerApi/Connect",
            1024 * 1024,
        ),
        (
            runtime.client.clone(),
            "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult",
            1024 * 1024,
        ),
        (
            runtime.client.clone(),
            "/build.bazel.remote.execution.v2.Execution/Execute",
            64 * 1024,
        ),
        (
            runtime.client.clone(),
            "/google.longrunning.Operations/CancelOperation",
            64 * 1024,
        ),
        (
            runtime.client.clone(),
            "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
            64 * 1024,
        ),
    ];
    for (channel, path, limit) in cases {
        let oversized = raw(channel.clone(), path, vec![0; limit + 1].into())
            .await
            .unwrap_err();
        assert_eq!(oversized.code(), Code::OutOfRange, "{path}: {oversized}");
        assert!(
            oversized
                .message()
                .contains("decoded message length too large")
        );
        let malformed = raw(channel, path, Bytes::from_static(&[0xff]))
            .await
            .unwrap_err();
        assert_eq!(malformed.code(), Code::Internal, "{path}: {malformed}");
        assert!(malformed.message().contains("decode"));
    }
    let view = runtime.service.subscribe();
    assert_eq!(
        view.borrow().workers,
        Vec::<nativelink_scheduler::authority::WorkerView>::new()
    );
    assert_eq!(
        view.borrow().executions,
        Vec::<nativelink_scheduler::authority::ExecutionView>::new()
    );
    assert_eq!(
        view.borrow().operations,
        Vec::<nativelink_scheduler::authority::OperationView>::new()
    );
    runtime.shutdown().await;
}

#[nativelink_test]
async fn exact_control_message_ceiling_is_accepted_with_unknown_fields() {
    let runtime = Runtime::start().await;
    let mut bytes = GetCapabilitiesRequest {
        instance_name: INSTANCE.to_owned(),
    }
    .encode_to_vec();
    // Unknown field 100, wire type bytes. Its three-byte length varint and
    // two-byte tag pad a valid request to exactly the declared wire ceiling.
    let padding = 64 * 1024 - bytes.len() - 5;
    bytes.extend_from_slice(&[0xa2, 0x06]);
    let mut length = padding;
    while length >= 128 {
        bytes.push(u8::try_from(length & 0x7f).unwrap() | 0x80);
        length >>= 7;
    }
    bytes.push(u8::try_from(length).unwrap());
    bytes.resize(64 * 1024, 0);
    let response = raw(
        runtime.client.clone(),
        "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
        bytes.into(),
    )
    .await
    .unwrap();
    let capabilities =
        nativelink_proto::build::bazel::remote::execution::v2::ServerCapabilities::decode(response)
            .unwrap();
    assert!(capabilities.execution_capabilities.unwrap().exec_enabled);
    runtime.shutdown().await;
}
