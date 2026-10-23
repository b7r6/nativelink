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

//! Fixed experimental per-request admission. These constants qualify a small
//! construction, not a workload, process heap, backend I/O or concurrency bound.
//! The legacy constructor never enters these handlers or changes its behavior.

use std::collections::{HashMap, VecDeque};

use bytes::{Bytes, BytesMut};
use nativelink_config::cas_server::InstanceName;
use nativelink_error::{Code, Error, make_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest,
    BatchUpdateBlobsResponse, Digest, Directory, GetTreeRequest, GetTreeResponse,
    batch_read_blobs_response, batch_update_blobs_response, compressor,
};
use nativelink_proto::google::rpc::Status as GrpcStatus;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasher, DigestHasherFunc, digest_hasher_func_from_context,
};
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;

use super::{BATCH_PER_BLOB_TIMEOUT, CasServer, ChunkingMetrics};
use crate::capabilities_server::MAX_BATCH_TOTAL_SIZE;
use crate::wire_compression::RemoteCacheCompressionInstances;

const CONTENT_BYTES: u64 = MAX_BATCH_TOTAL_SIZE as u64;
const ENTRIES: usize = 64;
const TOKEN_BYTES: usize = 160;
const RESPONSE_BYTES: usize = 1024 * 1024;

pub(super) fn check_entries(count: usize) -> Result<(), Error> {
    if count > ENTRIES {
        return Err(make_err!(
            Code::InvalidArgument,
            "Fixed CAS batch entry limit"
        ));
    }
    Ok(())
}

fn charge_batch(total: &mut u64, amount: u64) -> Result<(), Error> {
    *total = total
        .checked_add(amount)
        .filter(|total| *total <= CONTENT_BYTES)
        .ok_or_else(|| make_err!(Code::InvalidArgument, "Fixed CAS batch content limit"))?;
    Ok(())
}

fn parse_digest(digest: Option<&Digest>) -> Result<DigestInfo, Error> {
    digest
        .ok_or_else(|| make_err!(Code::InvalidArgument, "Missing CAS digest"))
        .and_then(|digest| {
            DigestInfo::try_from(digest)
                .map_err(|_| make_err!(Code::InvalidArgument, "Invalid CAS digest"))
        })
}

// Retain only a bounded first diagnostic, including the compression guard.
fn entry_status(error: Error) -> GrpcStatus {
    GrpcStatus {
        code: error.code as i32,
        message: error.messages.into_iter().next().map_or_else(
            || format!("Fixed CAS entry failed ({:?})", error.code),
            |message| message.chars().take(256).collect(),
        ),
        details: Vec::new(),
    }
}

fn check_response(response: &impl Message) -> Result<(), Error> {
    if response.encoded_len() > RESPONSE_BYTES {
        return Err(make_err!(
            Code::ResourceExhausted,
            "Fixed CAS response limit"
        ));
    }
    Ok(())
}

/// Collect only a complete, exact, verified object. A ranged/prefix read cannot
/// establish EOF. On excess or failure, `try_join` drops the other future instead
/// of continuing to drain an untrusted producer. A backend may already own a
/// large chunk or blocking operation; this is not a bound on arbitrary stores.
async fn verified_read(store: &Store, digest: DigestInfo) -> Result<Bytes, Error> {
    if digest.size_bytes() > CONTENT_BYTES {
        return Err(make_err!(
            Code::ResourceExhausted,
            "Fixed CAS object read limit"
        ));
    }
    let (mut writer, mut reader) = make_buf_channel_pair();
    let mut hasher = digest_hasher_func_from_context().hasher();
    let receive = async {
        let mut data = BytesMut::new();
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            let next = (data.len() as u64)
                .checked_add(chunk.len() as u64)
                .filter(|next| *next <= digest.size_bytes())
                .ok_or_else(|| make_err!(Code::DataLoss, "CAS object exceeds digest size"))?;
            debug_assert!(next <= CONTENT_BYTES);
            hasher.update(&chunk);
            data.extend_from_slice(&chunk);
        }
        if hasher.finalize_digest() != digest {
            return Err(make_err!(
                Code::DataLoss,
                "CAS object length or hash mismatch"
            ));
        }
        Ok(data.freeze())
    };
    let send = async move {
        let result = store.get_part(digest, &mut writer, 0, None).await;
        drop(writer);
        result
    };
    let (data, ()) = futures::try_join!(receive, send)?;
    Ok(data)
}

impl CasServer {
    /// The only fixed construction; no caller-supplied admission policy.
    pub(crate) fn new_authority(instance: InstanceName, store: Store) -> Self {
        Self {
            authority_admission: true,
            stores: HashMap::from([(instance, store)]),
            remote_cache_compression_instances: RemoteCacheCompressionInstances::default(),
            chunking_instances: HashMap::new(),
            chunking_metrics: ChunkingMetrics::default(),
        }
    }

    fn authority_store(&self, instance: &str) -> Result<&Store, Error> {
        self.stores
            .get(instance)
            .ok_or_else(|| make_err!(Code::InvalidArgument, "Unknown CAS instance"))
    }

    pub(super) async fn authority_batch_update(
        &self,
        request: BatchUpdateBlobsRequest,
    ) -> Result<BatchUpdateBlobsResponse, Error> {
        check_entries(request.requests.len())?;
        let mut declared = 0;
        let mut actual = 0;
        for entry in &request.requests {
            charge_batch(&mut actual, entry.data.len() as u64)?;
            if let Some(digest) = &entry.digest {
                charge_batch(&mut declared, u64::try_from(digest.size_bytes).unwrap_or(0))?;
            }
        }
        let store = self.authority_store(&request.instance_name)?;
        let algorithm = digest_hasher_func_from_context();
        let mut responses = Vec::with_capacity(request.requests.len());
        // Sequential bounded attempts avoid saturating the canonical backend's
        // four shared permits with one batch. Other RPCs can still exhaust it.
        for entry in request.requests {
            let result = async {
                let digest = parse_digest(entry.digest.as_ref())?;
                if entry.compressor != compressor::Value::Identity as i32 {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "Remote cache compression is not supported by this instance"
                    ));
                }
                let mut hasher = algorithm.hasher();
                hasher.update(&entry.data);
                if hasher.finalize_digest() != digest {
                    return Err(make_err!(
                        Code::InvalidArgument,
                        "CAS upload digest mismatch"
                    ));
                }
                tokio::time::timeout(
                    BATCH_PER_BLOB_TIMEOUT,
                    store.update_oneshot(digest, entry.data),
                )
                .await
                .map_err(|_| make_err!(Code::DeadlineExceeded, "Fixed CAS upload deadline"))?
            }
            .await;
            responses.push(batch_update_blobs_response::Response {
                digest: entry.digest,
                status: Some(result.map_or_else(entry_status, |()| GrpcStatus::default())),
            });
        }
        let response = BatchUpdateBlobsResponse { responses };
        check_response(&response)?;
        Ok(response)
    }

    pub(super) async fn authority_batch_read(
        &self,
        request: BatchReadBlobsRequest,
    ) -> Result<BatchReadBlobsResponse, Error> {
        check_entries(request.digests.len())?;
        let mut declared = 0;
        for digest in &request.digests {
            charge_batch(&mut declared, u64::try_from(digest.size_bytes).unwrap_or(0))?;
        }
        let store = self.authority_store(&request.instance_name)?;
        let mut responses = Vec::with_capacity(request.digests.len());
        for digest in request.digests {
            let result = async {
                let digest = parse_digest(Some(&digest))?;
                tokio::time::timeout(BATCH_PER_BLOB_TIMEOUT, verified_read(store, digest))
                    .await
                    .map_err(|_| make_err!(Code::DeadlineExceeded, "Fixed CAS read deadline"))?
            }
            .await;
            let (data, status) = match result {
                Ok(data) => (data, GrpcStatus::default()),
                Err(error) => (Bytes::new(), entry_status(error)),
            };
            responses.push(batch_read_blobs_response::Response {
                digest: Some(digest),
                data,
                compressor: compressor::Value::Identity as i32,
                status: Some(status),
            });
        }
        let response = BatchReadBlobsResponse { responses };
        check_response(&response)?;
        Ok(response)
    }

    pub(super) async fn authority_get_tree(
        &self,
        request: GetTreeRequest,
    ) -> Result<GetTreeResponse, Error> {
        let page_size = usize::try_from(request.page_size)
            .map_err(|_| make_err!(Code::InvalidArgument, "Negative GetTree page size"))?;
        let page_size = if page_size == 0 {
            ENTRIES
        } else {
            page_size.min(ENTRIES)
        };
        let root = parse_digest(request.root_digest.as_ref())?;
        let binding = tree_binding(&request.instance_name, root);
        let continuation = parse_token(&request.page_token, &binding)?;
        let store = self.authority_store(&request.instance_name)?;
        let mut walk = TreeWalk::default();
        walk.enqueue(root)?;
        let mut directories = Vec::new();
        let mut fingerprint = DigestHasherFunc::Sha256.hasher();
        fingerprint.update(b"nativelink-fixed-tree-walk-v1\0");
        let mut first = true;
        while let Some(digest) = walk.pending.pop_front() {
            fingerprint.update(digest.packed_hash().as_ref());
            fingerprint.update(&digest.size_bytes().to_le_bytes());
            let read = tokio::time::timeout(BATCH_PER_BLOB_TIMEOUT, verified_read(store, digest))
                .await
                .map_err(|_| make_err!(Code::DeadlineExceeded, "Fixed GetTree read deadline"))?;
            let data = match read {
                Ok(data) => data,
                Err(error) if !first && error.code == Code::NotFound => {
                    fingerprint.update(&[0]);
                    continue;
                }
                Err(error) => return Err(Error::new(error.code, entry_status(error).message)),
            };
            first = false;
            fingerprint.update(&[1]);
            // The byte budget applies before decode. Protobuf struct/string
            // expansion is not equal to raw bytes; no decoded-node heap cap is
            // claimed. Enqueue bounds the traversal frontier independently.
            let directory = Directory::decode(data)
                .map_err(|_| make_err!(Code::DataLoss, "Invalid stored Directory protobuf"))?;
            for child in &directory.directories {
                let child = parse_digest(child.digest.as_ref())
                    .map_err(|_| make_err!(Code::DataLoss, "Invalid stored child digest"))?;
                walk.enqueue(child)?;
            }
            directories.push(directory);
        }
        let fingerprint = fingerprint.finalize_digest().packed_hash().to_string();
        let start = if let Some((expected, start)) = continuation {
            if expected != fingerprint {
                return Err(make_err!(
                    Code::FailedPrecondition,
                    "GetTree availability changed; restart"
                ));
            }
            if start >= directories.len() {
                return Err(make_err!(
                    Code::InvalidArgument,
                    "GetTree token offset out of range"
                ));
            }
            start
        } else {
            0
        };
        let end = (start + page_size).min(directories.len());
        let next_page_token = if end < directories.len() {
            format!("v1.{binding}.{fingerprint}.{end}")
        } else {
            String::new()
        };
        let response = GetTreeResponse {
            directories: directories
                .into_iter()
                .skip(start)
                .take(page_size)
                .collect(),
            next_page_token,
        };
        check_response(&response)?;
        Ok(response)
    }
}

#[derive(Default)]
struct TreeWalk {
    pending: VecDeque<DigestInfo>,
    occurrences: usize,
    bytes: u64,
}

impl TreeWalk {
    fn enqueue(&mut self, digest: DigestInfo) -> Result<(), Error> {
        self.occurrences = self
            .occurrences
            .checked_add(1)
            .filter(|count| *count <= ENTRIES)
            .ok_or_else(|| make_err!(Code::ResourceExhausted, "Fixed GetTree directory limit"))?;
        self.bytes = self
            .bytes
            .checked_add(digest.size_bytes())
            .filter(|bytes| *bytes <= CONTENT_BYTES)
            .ok_or_else(|| make_err!(Code::ResourceExhausted, "Fixed GetTree content limit"))?;
        self.pending.push_back(digest);
        Ok(())
    }
}

fn tree_binding(instance: &str, root: DigestInfo) -> String {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(b"nativelink-fixed-tree-binding-v1\0");
    hasher.update(&(instance.len() as u64).to_le_bytes());
    hasher.update(instance.as_bytes());
    hasher.update(&(digest_hasher_func_from_context().proto_digest_func() as i32).to_le_bytes());
    hasher.update(root.packed_hash().as_ref());
    hasher.update(&root.size_bytes().to_le_bytes());
    hasher.finalize_digest().packed_hash().to_string()
}

fn parse_token<'a>(token: &'a str, binding: &str) -> Result<Option<(&'a str, usize)>, Error> {
    if token.is_empty() {
        return Ok(None);
    }
    let invalid = || make_err!(Code::InvalidArgument, "Invalid GetTree continuation token");
    if token.len() > TOKEN_BYTES {
        return Err(invalid());
    }
    let mut parts = token.split('.');
    if parts.next() != Some("v1") || parts.next() != Some(binding) {
        return Err(invalid());
    }
    let fingerprint = parts.next().ok_or_else(invalid)?;
    if fingerprint.len() != 64
        || !fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid());
    }
    let ordinal = parts.next().ok_or_else(invalid)?;
    let offset = ordinal.parse::<usize>().map_err(|_| invalid())?;
    if parts.next().is_some() || offset == 0 || offset >= ENTRIES || offset.to_string() != ordinal {
        return Err(invalid());
    }
    Ok(Some((fingerprint, offset)))
}
