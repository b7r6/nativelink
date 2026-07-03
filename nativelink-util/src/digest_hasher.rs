// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

use blake3::Hasher as Blake3Hasher;
use bytes::BytesMut;
use futures::Future;
use nativelink_config::stores::ConfigDigestHashFunction;
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::{
    MetricFieldData, MetricKind, MetricPublishKnownKindData, MetricsComponent,
};
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;
use opentelemetry::context::Context;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tracing::warn;

use crate::common::DigestInfo;
use crate::{fs, spawn_blocking};

static DEFAULT_DIGEST_HASHER_FUNC: OnceLock<DigestHasherFunc> = OnceLock::new();
static REQUIRE_EXPLICIT_DIGEST_FUNCTION: OnceLock<bool> = OnceLock::new();

/// Utility function to make a context with a specific hasher function set.
pub fn make_ctx_for_hash_func<H>(hasher: H) -> Result<Context, Error>
where
    H: TryInto<DigestHasherFunc>,
    H::Error: Into<Error>,
{
    let digest_hasher_func = hasher
        .try_into()
        .err_tip(|| "Could not convert into DigestHasherFunc")?;

    let new_ctx = Context::current_with_value(digest_hasher_func);

    Ok(new_ctx)
}

/// Get the default hasher.
pub fn default_digest_hasher_func() -> DigestHasherFunc {
    *DEFAULT_DIGEST_HASHER_FUNC.get_or_init(|| DigestHasherFunc::Sha256)
}

/// Sets the default hasher to use if no hasher was requested by the client.
pub fn set_default_digest_hasher_func(hasher: DigestHasherFunc) -> Result<(), Error> {
    DEFAULT_DIGEST_HASHER_FUNC
        .set(hasher)
        .map_err(|_| make_err!(Code::Internal, "default_digest_hasher_func already set"))
}

/// When set to true, `TryFrom<i32>` for `DigestHasherFunc` will reject
/// value 0 (UNKNOWN) with an error rather than silently defaulting to the
/// server's configured digest function. This prevents BLAKE3/SHA256
/// cross-hashing bugs where a client omits the field and the server
/// defaults to a mismatched algorithm.
pub fn set_require_explicit_digest_function(required: bool) -> Result<(), Error> {
    REQUIRE_EXPLICIT_DIGEST_FUNCTION.set(required).map_err(|_| {
        make_err!(
            Code::Internal,
            "require_explicit_digest_function already set"
        )
    })
}

/// Returns whether the server requires clients to explicitly set the
/// digest function (rejects UNKNOWN/0).
pub fn require_explicit_digest_function() -> bool {
    *REQUIRE_EXPLICIT_DIGEST_FUNCTION.get_or_init(|| false)
}

/// Resolve the digest function from an optional `ByteStream` resource-name
/// segment, honoring strict mode.
///
/// REAPI carries the digest function for `ByteStream` as a path segment
/// (`.../blobs/{digest_function}/{hash}/{size}`), not as a proto int field, so
/// an omitted segment is the `ByteStream` analogue of `digest_function == 0` on
/// the structured RPCs. Resolving it through this function (rather than
/// defaulting directly) ensures `require_explicit_digest_function` covers
/// `ByteStream` too: an omitted segment is rejected in strict mode and defaulted
/// (with the same one-time warning as the i32 path) otherwise. A present
/// segment is parsed normally and an unknown value is rejected regardless of
/// mode.
pub fn digest_hasher_from_resource_name_segment(
    segment: Option<&str>,
) -> Result<DigestHasherFunc, Error> {
    if let Some(name) = segment {
        return DigestHasherFunc::try_from(name);
    }
    if require_explicit_digest_function() {
        return Err(make_input_err!(
            "digest_function is required but the ByteStream resource name \
             omitted it (e.g. '.../blobs/{{hash}}/{{size}}' with no \
             '{{digest_function}}' segment). Clients MUST address blobs \
             under '.../blobs/{{digest_function}}/{{hash}}/{{size}}'. Set \
             global.require_explicit_digest_function to false to restore \
             the legacy defaulting behavior."
        ));
    }
    if !WARNED_UNSET_DIGEST_FUNCTION.swap(true, Ordering::Relaxed) {
        let default_fn = default_digest_hasher_func();
        warn!(
            default_digest_function = %default_fn,
            "Received ByteStream request with no digest_function segment in \
             the resource name; defaulting to {default_fn}. This can cause \
             silent corruption if the client is using a different digest \
             function (e.g. BLAKE3 client with SHA256 server default). Set \
             global.require_explicit_digest_function = true to reject such \
             requests, or ensure all clients address blobs under \
             '.../blobs/{{digest_function}}/{{hash}}/{{size}}'.",
        );
    }
    Ok(default_digest_hasher_func())
}

/// Supported digest hash functions.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DigestHasherFunc {
    Sha256,
    Blake3,
}

impl MetricsComponent for DigestHasherFunc {
    fn publish(
        &self,
        kind: MetricKind,
        field_metadata: MetricFieldData,
    ) -> Result<MetricPublishKnownKindData, nativelink_metric::Error> {
        format!("{self:?}").publish(kind, field_metadata)
    }
}

impl DigestHasherFunc {
    pub fn hasher(&self) -> DigestHasherImpl {
        self.into()
    }

    #[must_use]
    pub const fn proto_digest_func(&self) -> ProtoDigestFunction {
        match self {
            Self::Sha256 => ProtoDigestFunction::Sha256,
            Self::Blake3 => ProtoDigestFunction::Blake3,
        }
    }
}

impl From<ConfigDigestHashFunction> for DigestHasherFunc {
    fn from(value: ConfigDigestHashFunction) -> Self {
        match value {
            ConfigDigestHashFunction::Sha256 => Self::Sha256,
            ConfigDigestHashFunction::Blake3 => Self::Blake3,
        }
    }
}

impl TryFrom<ProtoDigestFunction> for DigestHasherFunc {
    type Error = Error;

    fn try_from(value: ProtoDigestFunction) -> Result<Self, Self::Error> {
        match value {
            ProtoDigestFunction::Sha256 => Ok(Self::Sha256),
            ProtoDigestFunction::Blake3 => Ok(Self::Blake3),
            v => Err(make_input_err!(
                "Unknown or unsupported digest function for proto conversion {v:?}"
            )),
        }
    }
}

impl TryFrom<&str> for DigestHasherFunc {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_uppercase().as_str() {
            "SHA256" => Ok(Self::Sha256),
            "BLAKE3" => Ok(Self::Blake3),
            v => Err(make_input_err!(
                "Unknown or unsupported digest function for string conversion: {v:?}"
            )),
        }
    }
}

impl core::fmt::Display for DigestHasherFunc {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Sha256 => write!(f, "SHA256"),
            Self::Blake3 => write!(f, "BLAKE3"),
        }
    }
}

/// Tracks whether we've already warned about unset `digest_function`
/// so we don't spam logs on every single request.
static WARNED_UNSET_DIGEST_FUNCTION: AtomicBool = AtomicBool::new(false);

impl TryFrom<i32> for DigestHasherFunc {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        // Zero means not-set (protobuf default for unset enum fields).
        if value == 0 {
            if require_explicit_digest_function() {
                return Err(make_input_err!(
                    "digest_function is required but was not set (received UNKNOWN/0). \
                     Clients MUST explicitly declare the digest function. \
                     Set global.require_explicit_digest_function to false to restore \
                     the legacy defaulting behavior."
                ));
            }
            // Legacy mode: default, but warn so operators can detect the issue.
            if !WARNED_UNSET_DIGEST_FUNCTION.swap(true, Ordering::Relaxed) {
                let default_fn = default_digest_hasher_func();
                warn!(
                    default_digest_function = %default_fn,
                    "Received request with digest_function unset (UNKNOWN/0); \
                     defaulting to {default_fn}. This can cause silent corruption \
                     if the client is using a different digest function (e.g. BLAKE3 \
                     client with SHA256 server default). Set \
                     global.require_explicit_digest_function = true to reject such \
                     requests, or ensure all clients explicitly set digest_function.",
                );
            }
            return Ok(default_digest_hasher_func());
        }
        match ProtoDigestFunction::try_from(value) {
            Ok(ProtoDigestFunction::Sha256) => Ok(Self::Sha256),
            Ok(ProtoDigestFunction::Blake3) => Ok(Self::Blake3),
            value => Err(make_input_err!(
                "Unknown or unsupported digest function for int conversion: {:?}",
                value.map(|v| v.as_str_name())
            )),
        }
    }
}

impl From<&DigestHasherFunc> for DigestHasherImpl {
    fn from(value: &DigestHasherFunc) -> Self {
        let hash_func_impl = match value {
            DigestHasherFunc::Sha256 => DigestHasherFuncImpl::Sha256(Sha256::new()),
            DigestHasherFunc::Blake3 => DigestHasherFuncImpl::Blake3(Box::default()),
        };
        Self {
            hashed_size: 0,
            hash_func_impl,
        }
    }
}

/// Wrapper to compute a hash of arbitrary data.
pub trait DigestHasher {
    /// Update the hasher with some additional data.
    fn update(&mut self, input: &[u8]);

    /// Finalize the hash function and collect the results into a digest.
    fn finalize_digest(&mut self) -> DigestInfo;

    /// Specialized version of the hashing function that is optimized for
    /// handling files. These optimizations take into account things like,
    /// the file size and the hasher algorithm to decide how to best process
    /// the file and feed it into the hasher.
    fn digest_for_file(
        self,
        file_path: impl AsRef<std::path::Path>,
        file: fs::FileSlot,
        size_hint: Option<u64>,
    ) -> impl Future<Output = Result<(DigestInfo, fs::FileSlot), Error>>;

    /// Utility function to compute a hash from a generic reader.
    fn compute_from_reader<R: AsyncRead + Unpin + Send>(
        &mut self,
        mut reader: R,
    ) -> impl Future<Output = Result<DigestInfo, Error>> {
        async move {
            let mut chunk = BytesMut::with_capacity(fs::DEFAULT_READ_BUFF_SIZE);
            loop {
                reader
                    .read_buf(&mut chunk)
                    .await
                    .err_tip(|| "Could not read chunk during compute_from_reader")?;
                if chunk.is_empty() {
                    break; // EOF.
                }
                DigestHasher::update(self, &chunk);
                chunk.clear();
            }
            Ok(DigestHasher::finalize_digest(self))
        }
    }
}

#[expect(
    variant_size_differences,
    reason = "some variants are already boxed; this is acceptable"
)]
#[derive(Debug)]
pub enum DigestHasherFuncImpl {
    Sha256(Sha256),
    Blake3(Box<Blake3Hasher>), // Box because Blake3Hasher is 1.3kb in size.
}

/// The individual implementation of the hash function.
#[derive(Debug)]
pub struct DigestHasherImpl {
    hashed_size: u64,
    hash_func_impl: DigestHasherFuncImpl,
}

impl DigestHasherImpl {
    #[inline]
    async fn hash_file(
        &mut self,
        mut file: fs::FileSlot,
    ) -> Result<(DigestInfo, fs::FileSlot), Error> {
        let digest = self
            .compute_from_reader(&mut file)
            .await
            .err_tip(|| "In digest_for_file")?;
        Ok((digest, file))
    }
}

impl DigestHasher for DigestHasherImpl {
    #[inline]
    fn update(&mut self, input: &[u8]) {
        self.hashed_size += input.len() as u64;
        match &mut self.hash_func_impl {
            DigestHasherFuncImpl::Sha256(h) => sha2::digest::Update::update(h, input),
            DigestHasherFuncImpl::Blake3(h) => {
                Blake3Hasher::update(h, input);
            }
        }
    }

    #[inline]
    fn finalize_digest(&mut self) -> DigestInfo {
        let hash = match &mut self.hash_func_impl {
            DigestHasherFuncImpl::Sha256(h) => h.finalize_reset().into(),
            DigestHasherFuncImpl::Blake3(h) => h.finalize().into(),
        };
        DigestInfo::new(hash, self.hashed_size)
    }

    async fn digest_for_file(
        mut self,
        file_path: impl AsRef<std::path::Path>,
        mut file: fs::FileSlot,
        size_hint: Option<u64>,
    ) -> Result<(DigestInfo, fs::FileSlot), Error> {
        let file_position = file
            .stream_position()
            .await
            .err_tip(|| "Couldn't get stream position in digest_for_file")?;
        if file_position != 0 {
            return self.hash_file(file).await;
        }
        // If we are a small file, it's faster to just do it the "slow" way.
        // Great read: https://github.com/david-slatinek/c-read-vs.-mmap
        if let Some(size_hint) = size_hint
            && size_hint <= fs::DEFAULT_READ_BUFF_SIZE as u64
        {
            return self.hash_file(file).await;
        }
        let file_path = file_path.as_ref().to_path_buf();
        match self.hash_func_impl {
            DigestHasherFuncImpl::Sha256(_) => self.hash_file(file).await,
            DigestHasherFuncImpl::Blake3(mut hasher) => {
                spawn_blocking!("digest_for_file", move || {
                    hasher.update_mmap(file_path).map_err(|e| {
                        Error::from_std_err(Code::Internal, &e)
                            .append("Error in blake3's update_mmap")
                    })?;
                    Result::<_, Error>::Ok((
                        DigestInfo::new(hasher.finalize().into(), hasher.count()),
                        file,
                    ))
                })
                .await
                .err_tip(|| "Could not spawn blocking task in digest_for_file")?
            }
        }
    }
}
