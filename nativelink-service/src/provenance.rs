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

//! The CAS services' provenance events: `cas_write` for every blob written
//! through `ByteStream` `Write` or `BatchUpdateBlobs`, and `find_missing` for
//! every `FindMissingBlobs`. See [`nativelink_util::provenance`].
//!
//! Construct these only when [`provenance_enabled`] is true, so that with
//! provenance off a request pays for that check and nothing else.
//!
//! [`provenance_enabled`]: nativelink_util::provenance::provenance_enabled

use core::mem;
use core::net::SocketAddr;
use std::time::Instant;

use nativelink_error::Code;
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchUpdateBlobsRequest, BatchUpdateBlobsResponse, Digest, FindMissingBlobsRequest,
    FindMissingBlobsResponse,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasherFunc, default_digest_hasher_func};
use nativelink_util::provenance::ProvenanceMetadata;
use nativelink_util::provenance_event;
use nativelink_util::resource_info::ResourceInfo;
use tonic::{Request, Response, Status};

/// The `cas_write` events of one write request: a `ByteStream` `Write`, or a
/// `BatchUpdateBlobs` with one event per blob.
///
/// Every blob gets exactly one event. The handler emits them when the RPC
/// returns, with [`Self::finish`] or [`Self::finish_batch_update`]. If the
/// handler's future is dropped first, which is what happens when the client
/// goes away mid-request, they are emitted on drop as `Cancelled`.
#[derive(Debug)]
pub(crate) struct CasWriteEvents {
    source: &'static str,
    instance_name: String,
    digest_function: &'static str,
    peer: String,
    metadata: ProvenanceMetadata,
    started: Instant,
    /// The blobs still waiting for their event. `None` is a blob whose digest
    /// the request did not give in a valid form.
    pending: Vec<Option<DigestInfo>>,
}

impl CasWriteEvents {
    /// For a `ByteStream` `Write`, once its first message has named the blob.
    pub(crate) fn for_write(resource_info: &ResourceInfo<'_>, peer: Option<SocketAddr>) -> Self {
        let digest_function = match resource_info.digest_function.as_deref() {
            Some(name) => DigestHasherFunc::try_from(name).ok(),
            None => Some(default_digest_hasher_func()),
        };
        let digest = DigestInfo::try_new(&resource_info.hash, resource_info.expected_size).ok();
        Self::new(
            "bytestream",
            resource_info.instance_name.to_string(),
            digest_function,
            peer,
            vec![digest],
        )
    }

    /// For a `BatchUpdateBlobs`, one event per blob in the request.
    pub(crate) fn for_batch_update(request: &Request<BatchUpdateBlobsRequest>) -> Self {
        let peer = request.remote_addr();
        let request = request.get_ref();
        let blobs = request
            .requests
            .iter()
            .map(|blob| blob.digest.as_ref().and_then(digest_info))
            .collect();
        Self::new(
            "batch_update",
            request.instance_name.clone(),
            DigestHasherFunc::try_from(request.digest_function).ok(),
            peer,
            blobs,
        )
    }

    fn new(
        source: &'static str,
        instance_name: String,
        digest_function: Option<DigestHasherFunc>,
        peer: Option<SocketAddr>,
        pending: Vec<Option<DigestInfo>>,
    ) -> Self {
        Self {
            source,
            instance_name,
            digest_function: digest_function_name(digest_function),
            peer: peer.map_or_else(String::new, |peer| peer.to_string()),
            metadata: ProvenanceMetadata::from_current_context(),
            started: Instant::now(),
            pending,
        }
    }

    /// Emits every blob's event with the RPC's result.
    pub(crate) fn finish<T>(mut self, result: &Result<T, Status>) {
        self.emit_pending(&Outcome::of(result));
    }

    /// Emits one event per blob of a `BatchUpdateBlobs`: each with its own
    /// status from the response, or all with the RPC's error if it failed.
    pub(crate) fn finish_batch_update(
        mut self,
        result: &Result<Response<BatchUpdateBlobsResponse>, Status>,
    ) {
        let Ok(response) = result else {
            self.finish(result);
            return;
        };
        // The response has an entry per blob, with its digest and status, in
        // completion order rather than request order.
        self.pending.clear();
        for blob in &response.get_ref().responses {
            let outcome = blob.status.as_ref().map_or_else(Outcome::ok, |status| {
                Outcome::new(Code::from_i32(status.code), &status.message)
            });
            self.emit(blob.digest.as_ref().and_then(digest_info), &outcome);
        }
    }

    fn emit_pending(&mut self, outcome: &Outcome) {
        for digest in mem::take(&mut self.pending) {
            self.emit(digest, outcome);
        }
    }

    fn emit(&self, digest: Option<DigestInfo>, outcome: &Outcome) {
        let (digest, size_bytes) = digest.map_or_else(
            || (String::new(), 0),
            |digest| (digest.to_string(), digest.size_bytes()),
        );
        provenance_event!("cas_write", &self.metadata;
            source = self.source,
            instance_name = %self.instance_name,
            digest_function = self.digest_function,
            digest = %digest,
            size_bytes,
            result = outcome.result,
            error_code = outcome.error_code,
            error = %outcome.error,
            elapsed_ms = elapsed_ms(self.started),
            peer = %self.peer,
        );
    }
}

impl Drop for CasWriteEvents {
    fn drop(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        self.emit_pending(&Outcome::new(
            Code::Cancelled,
            "dropped before finishing, most likely because the client went away",
        ));
    }
}

/// The `find_missing` event of one `FindMissingBlobs`.
#[derive(Debug)]
pub(crate) struct FindMissingEvent {
    instance_name: String,
    digest_function: &'static str,
    requested_count: usize,
    requested_bytes: u64,
    metadata: ProvenanceMetadata,
    started: Instant,
}

impl FindMissingEvent {
    pub(crate) fn start(request: &FindMissingBlobsRequest) -> Self {
        Self {
            instance_name: request.instance_name.clone(),
            digest_function: digest_function_name(
                DigestHasherFunc::try_from(request.digest_function).ok(),
            ),
            requested_count: request.blob_digests.len(),
            requested_bytes: total_bytes(&request.blob_digests),
            metadata: ProvenanceMetadata::from_current_context(),
            started: Instant::now(),
        }
    }

    pub(crate) fn finish(self, result: &Result<Response<FindMissingBlobsResponse>, Status>) {
        let (missing_count, missing_bytes) = result.as_ref().map_or((0, 0), |response| {
            let missing = &response.get_ref().missing_blob_digests;
            (missing.len(), total_bytes(missing))
        });
        let outcome = Outcome::of(result);
        provenance_event!("find_missing", &self.metadata;
            instance_name = %self.instance_name,
            digest_function = self.digest_function,
            requested_count = self.requested_count,
            requested_bytes = self.requested_bytes,
            missing_count,
            missing_bytes,
            result = outcome.result,
            error_code = outcome.error_code,
            error = %outcome.error,
            elapsed_ms = elapsed_ms(self.started),
        );
    }
}

/// The `result`, `error_code` and `error` fields of an event.
#[derive(Debug)]
struct Outcome {
    result: &'static str,
    error_code: &'static str,
    error: String,
}

impl Outcome {
    const fn ok() -> Self {
        Self {
            result: "ok",
            error_code: "",
            error: String::new(),
        }
    }

    fn new(code: Code, error: &str) -> Self {
        if code == Code::Ok {
            return Self::ok();
        }
        Self {
            result: "error",
            error_code: code_name(code),
            error: error.to_string(),
        }
    }

    fn of<T>(result: &Result<T, Status>) -> Self {
        match result {
            Ok(_) => Self::ok(),
            Err(status) => Self::new(status.code(), status.message()),
        }
    }
}

/// The `error_code` field: the code's name as [`Code`] spells it.
const fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "Ok",
        Code::Cancelled => "Cancelled",
        Code::Unknown => "Unknown",
        Code::InvalidArgument => "InvalidArgument",
        Code::DeadlineExceeded => "DeadlineExceeded",
        Code::NotFound => "NotFound",
        Code::AlreadyExists => "AlreadyExists",
        Code::PermissionDenied => "PermissionDenied",
        Code::ResourceExhausted => "ResourceExhausted",
        Code::FailedPrecondition => "FailedPrecondition",
        Code::Aborted => "Aborted",
        Code::OutOfRange => "OutOfRange",
        Code::Unimplemented => "Unimplemented",
        Code::Internal => "Internal",
        Code::Unavailable => "Unavailable",
        Code::DataLoss => "DataLoss",
        Code::Unauthenticated => "Unauthenticated",
    }
}

/// The `digest_function` field: the function's lowercase name, or `""` when
/// the request named one this server does not support.
const fn digest_function_name(digest_function: Option<DigestHasherFunc>) -> &'static str {
    match digest_function {
        Some(DigestHasherFunc::Sha256) => "sha256",
        Some(DigestHasherFunc::Blake3) => "blake3",
        None => "",
    }
}

fn digest_info(digest: &Digest) -> Option<DigestInfo> {
    DigestInfo::try_from(digest).ok()
}

/// The sum of the digests' sizes, counting a negative size as zero.
fn total_bytes(digests: &[Digest]) -> u64 {
    digests
        .iter()
        .map(|digest| u64::try_from(digest.size_bytes).unwrap_or(0))
        .fold(0, u64::saturating_add)
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_names_match_debug() {
        for value in 0..=16 {
            let code = Code::from_i32(value);
            assert_eq!(code_name(code), format!("{code:?}"));
        }
    }

    #[test]
    fn negative_and_overflowing_sizes_do_not_panic() {
        let digest = |size_bytes| Digest {
            hash: String::new(),
            size_bytes,
        };
        assert_eq!(total_bytes(&[digest(3), digest(-1), digest(4)]), 7);
        assert_eq!(
            total_bytes(&[digest(i64::MAX), digest(i64::MAX), digest(i64::MAX)]),
            u64::MAX
        );
    }
}
