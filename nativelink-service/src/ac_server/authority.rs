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

//! Private admission for the fixed client Action Cache route. Values remain
//! mutable and are not content-addressed by their Action keys.

use std::future::Future;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use nativelink_error::{Code, Error, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, GetActionResultRequest, UpdateActionResultRequest,
};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use tonic::Response;

use super::AcServer;

const VALUE_BYTES: usize = crate::authority_listener::CACHE_MESSAGE_BYTES;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

fn key(digest: Option<&Digest>) -> Result<DigestInfo, Error> {
    digest
        .ok_or_else(|| make_err!(Code::Internal, "Missing Action digest"))
        .and_then(|digest| {
            DigestInfo::try_from(digest)
                .map_err(|_| make_err!(Code::InvalidArgument, "Invalid Action digest"))
        })
}

fn check_encoded(value: &ActionResult) -> Result<(), Error> {
    if value.encoded_len() > VALUE_BYTES {
        return Err(make_err!(
            Code::ResourceExhausted,
            "Fixed Action Cache value limit"
        ));
    }
    Ok(())
}

async fn bounded<T: Send>(
    future: impl Future<Output = Result<T, Error>> + Send,
) -> Result<T, Error> {
    tokio::time::timeout(REQUEST_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| {
            Err(make_err!(
                Code::DeadlineExceeded,
                "Fixed Action Cache timeout"
            ))
        })
        .map_err(|error| {
            if error.code == Code::NotFound {
                make_err!(Code::NotFound, "")
            } else {
                make_err!(error.code, "Fixed Action Cache request failed")
            }
        })
}

/// Requires a producer that delivers every available byte in the requested
/// range before EOF/success. With that premise, at most `VALUE_BYTES` bytes from a
/// `VALUE_BYTES + 1` range prove that the complete stored value fits. `Store`'s generic
/// interface cannot certify arbitrary producers or a backend's memory/IO use.
/// The sentinel avoids requesting an entire potentially 64 MiB canonical value;
/// that backend still scans the whole file and can allocate spare capacity.
/// Producer cancellation does not prove blocking or remote work has stopped.
async fn read_value(store: &Store, digest: DigestInfo) -> Result<Bytes, Error> {
    let (mut writer, mut reader) = make_buf_channel_pair();
    let receive = async {
        let mut bytes = BytesMut::new();
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            if chunk.len() > VALUE_BYTES - bytes.len() {
                return Err(make_err!(
                    Code::ResourceExhausted,
                    "Fixed Action Cache value limit"
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes.freeze())
    };
    let send = async move {
        let result = store
            .get_part(digest, &mut writer, 0, Some(VALUE_BYTES as u64 + 1))
            .await;
        drop(writer);
        result
    };
    let (bytes, ()) = futures::try_join!(receive, send)?;
    Ok(bytes)
}

pub(super) async fn get(
    server: &AcServer,
    request: GetActionResultRequest,
) -> Result<Response<ActionResult>, Error> {
    bounded(async {
        let store = server
            .stores
            .get(&request.instance_name)
            .ok_or_else(|| make_err!(Code::Internal, "Unknown Action Cache instance"))?;
        let digest = key(request.action_digest.as_ref())?;
        // The store facade supplies no inline hints; there is no CAS fetch here.
        // GrpcStore may decode/normalize an upstream response before this read.
        let bytes = read_value(&store.store, digest).await?;
        let value = ActionResult::decode(bytes)
            .map_err(|_| make_err!(Code::NotFound, "Corrupt Action Cache value"))?;
        check_encoded(&value)?;
        Ok(Response::new(value))
    })
    .await
}

pub(super) async fn update(
    server: &AcServer,
    request: UpdateActionResultRequest,
) -> Result<Response<ActionResult>, Error> {
    bounded(async {
        let store = server
            .stores
            .get(&request.instance_name)
            .ok_or_else(|| make_err!(Code::Internal, "Unknown Action Cache instance"))?;
        let digest = key(request.action_digest.as_ref())?;
        let value = request
            .action_result
            .ok_or_else(|| make_err!(Code::Internal, "Missing ActionResult"))?;
        check_encoded(&value)?;
        let mut bytes = BytesMut::with_capacity(value.encoded_len());
        value
            .encode(&mut bytes)
            .map_err(|_| make_err!(Code::Internal, "ActionResult encoding failed"))?;
        // A timed-out write can still finish later; this remains a mutable sink.
        store.store.update_oneshot(digest, bytes.freeze()).await?;
        Ok(Response::new(value))
    })
    .await
}
