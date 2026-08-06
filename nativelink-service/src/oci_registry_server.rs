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

//! The OCI Distribution registry HTTP service.
//!
//! Serves the OCI Distribution Specification (pull AND push) directly from
//! `NativeLink` stores — the `nix_cache` facade move applied to the OCI wire
//! format. Blobs are stored ONCE under the deployment's canonical digest
//! function (BLAKE3 on the fleet); the sha256 wire identity is answered via
//! an immutable alias index; tags are mutable string-keyed records.
//!
//! Store layout (see `nativelink-config/examples/oci_registry.json5`):
//!
//! - `cas_store`: digest-keyed blob store — layers, configs, and manifest
//!   bodies under `DigestInfo(canonical_hash(bytes), size)`.
//! - `index_store`: string-keyed `oci-digest:sha256:<hex>` alias records
//!   mapping wire names to canonical identities.
//! - `ref_store`: string-keyed mutable `oci-tag:<name>:<tag>` records and
//!   `oci-tags:<name>` per-repo tag indexes.
//!
//! Design: `design/oci-registry-over-cas.md` (PROD-3).

use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
use core::time::Duration;
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{
    AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, LOCATION, RANGE, WWW_AUTHENTICATE,
};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::Response;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use nativelink_config::cas_server::{OciRegistryServiceConfig, WithInstanceName};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_oci_registry::records::{
    OciDigestAlias, OciRepoTagIndex, OciTagRecord, alias_key, repo_tag_index_key, tag_key,
};
use nativelink_oci_registry::wire::{
    ManifestReference, OCI_MANIFEST_MEDIA_TYPE, OciErrorCode, error_body, is_valid_repository_name,
    parse_digest, parse_manifest, parse_manifest_reference,
};
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::{DropCloserReadHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{
    DigestHasher, DigestHasherFunc, DigestHasherImpl, make_ctx_for_hash_func,
};
use nativelink_util::metrics_utils::{Counter, CounterWithTime};
use nativelink_util::store_trait::{
    Store, StoreKey, StoreLike, UploadSizeInfo, slow_update_store_with_file,
};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{background_spawn, fs, spawn};
use opentelemetry::context::{Context as OtelContext, FutureExt};
use tokio::io::AsyncWriteExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;
use tracing::{debug, error, warn};
use uuid::Uuid;

/// The spec's fixed API root; the default mount path.
const DEFAULT_MOUNT_PATH: &str = "/v2";
/// Directory name under the system temp dir used when `spool_path` is not
/// configured.
const DEFAULT_SPOOL_DIR_NAME: &str = "nativelink-oci-spool";
/// Filename prefix for every upload spool file; startup pruning removes
/// ONLY files carrying this prefix.
const SPOOL_FILE_PREFIX: &str = "nativelink-oci-spool-";
/// The challenge attached to every `401`. `Basic` is spec-legal: skopeo,
/// docker, and crane answer it from configured credentials natively.
const WWW_AUTHENTICATE_CHALLENGE: &str = "Basic realm=\"oci-registry\"";
/// `Content-Type` of the spec's `errors[]` JSON bodies.
const ERROR_CONTENT_TYPE: &str = "application/json";
/// Fallback `Content-Type` for manifests stored without a media type.
const DEFAULT_MANIFEST_CONTENT_TYPE: &str = OCI_MANIFEST_MEDIA_TYPE;
/// `Content-Type` of blob bodies.
const BLOB_CONTENT_TYPE: &str = "application/octet-stream";

/// Returns `sha256(data)`; token comparisons happen in hashed space.
fn sha256_of(data: &[u8]) -> [u8; 32] {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(data);
    let digest = hasher.finalize_digest();
    let hash: &[u8; 32] = digest.packed_hash();
    *hash
}

/// Constant-time equality over equal-length slices.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0_u8;
    for (x, y) in a.iter().zip(b.iter()) {
        acc |= x ^ y;
    }
    acc == 0
}

/// Whether a presented token hash matches any candidate hash; every
/// candidate is compared, no early exit.
fn token_matches_any(presented: &[u8; 32], candidates: &[[u8; 32]]) -> bool {
    let mut found = false;
    for candidate in candidates {
        found |= constant_time_eq(presented, candidate);
    }
    found
}

/// Extracts the presented token: `Bearer <token>`, or `Basic` where the
/// token is the PASSWORD (how docker/skopeo send configured credentials).
fn extract_token(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = value.trim().split_once(' ')?;
    let rest = rest.trim();
    if scheme.eq_ignore_ascii_case("bearer") {
        return (!rest.is_empty()).then(|| rest.to_string());
    }
    if scheme.eq_ignore_ascii_case("basic") {
        let decoded = BASE64.decode(rest).ok()?;
        let text = String::from_utf8(decoded).ok()?;
        let (_user, password) = text.split_once(':')?;
        return Some(password.to_string());
    }
    None
}

/// Loads token files (one token per file, trimmed), returning sha256 hashes.
fn load_token_hashes(files: &[String], what: &str) -> Result<Vec<[u8; 32]>, Error> {
    let mut hashes = Vec::with_capacity(files.len());
    for file in files {
        let contents = std::fs::read_to_string(file)
            .err_tip(|| format!("Failed to read OCI registry {what} token file '{file}'"))?;
        let token = contents.trim();
        if token.is_empty() {
            return Err(make_input_err!(
                "OCI registry {what} token file '{file}' holds no token"
            ));
        }
        hashes.push(sha256_of(token.as_bytes()));
    }
    Ok(hashes)
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

/// An `errors[]` JSON response with the spec's canonical code.
fn oci_error_response(status: StatusCode, code: OciErrorCode, message: &str) -> Response {
    let mut response = Response::new(Body::from(error_body(code, message)));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(ERROR_CONTENT_TYPE));
    response
}

/// The `401` with the `Basic` challenge.
fn unauthorized_response() -> Response {
    let mut response = oci_error_response(
        StatusCode::UNAUTHORIZED,
        OciErrorCode::Unauthorized,
        "authentication required",
    );
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static(WWW_AUTHENTICATE_CHALLENGE),
    );
    response
}

/// The single backend-error mapping: absence is 404, client errors are 4xx,
/// everything else is an honest 500 — never a fabricated 404.
fn backend_error_response(context: &'static str, err: &Error) -> Response {
    match err.code {
        Code::NotFound => oci_error_response(
            StatusCode::NOT_FOUND,
            OciErrorCode::BlobUnknown,
            "content not found",
        ),
        Code::InvalidArgument => {
            debug!(?err, context, "OCI registry client error");
            oci_error_response(
                StatusCode::BAD_REQUEST,
                OciErrorCode::Unsupported,
                "bad request",
            )
        }
        Code::ResourceExhausted => {
            debug!(?err, context, "OCI registry payload too large");
            oci_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                OciErrorCode::SizeInvalid,
                "payload too large",
            )
        }
        Code::DeadlineExceeded => {
            debug!(?err, context, "OCI registry request timed out");
            oci_error_response(
                StatusCode::REQUEST_TIMEOUT,
                OciErrorCode::BlobUploadInvalid,
                "request timeout",
            )
        }
        _ => {
            error!(?err, context, "OCI registry request failed");
            let mut response = Response::new(Body::from("internal error"));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            response
        }
    }
}

/// Creates the spool directory and prunes files left over from a previous
/// run; only files carrying [`SPOOL_FILE_PREFIX`] are ever removed.
fn prepare_spool_dir(spool_dir: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(spool_dir)
        .err_tip(|| format!("Failed to create spool dir {}", spool_dir.display()))?;
    let entries = std::fs::read_dir(spool_dir)
        .err_tip(|| format!("Failed to read spool dir {}", spool_dir.display()))?;
    for entry in entries {
        let entry =
            entry.err_tip(|| format!("Failed to list spool dir {}", spool_dir.display()))?;
        let file_type = entry
            .file_type()
            .err_tip(|| format!("Failed to stat spool entry {}", entry.path().display()))?;
        let is_spool_file = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(SPOOL_FILE_PREFIX));
        if file_type.is_file()
            && is_spool_file
            && let Err(err) = std::fs::remove_file(entry.path())
        {
            warn!(?err, path = %entry.path().display(), "Failed to prune stale OCI spool file");
        }
    }
    Ok(())
}

/// Reads a request body to completion, buffering at most `limit` bytes;
/// `None` when the body exceeds the limit.
async fn read_body_limited(body: Body, limit: usize) -> Result<Option<Bytes>, Error> {
    let mut stream = body.into_data_stream();
    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| make_input_err!("Failed to read request body: {e}"))?;
        if buffer.len() + chunk.len() > limit {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(Some(buffer.freeze()))
}

/// Adapter from the buf-channel read half to a response body stream, with
/// the producer task and the concurrency permit riding the body's lifetime
/// (the `nix_cache` `NarBodyStream` pattern verbatim).
#[derive(Debug)]
struct BlobBodyStream {
    rx: DropCloserReadHalf,
    _task: JoinHandleDropGuard<()>,
    _permit: OwnedSemaphorePermit,
}

impl Stream for BlobBodyStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

/// One in-flight upload session: the spool file, the two running hashers,
/// and the byte offset. Removed from the session map while a request is
/// operating on it, so concurrent requests against one session see a clean
/// `BLOB_UPLOAD_UNKNOWN` instead of interleaved writes.
struct UploadSession {
    spool_path: PathBuf,
    file: fs::FileSlot,
    sha256: DigestHasherImpl,
    canonical: DigestHasherImpl,
    offset: u64,
    last_touch: Instant,
}

impl core::fmt::Debug for UploadSession {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("UploadSession")
            .field("spool_path", &self.spool_path)
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl UploadSession {
    /// Removes the spool file, logging (not failing) on error.
    async fn discard(self) {
        drop(self.file);
        if let Err(err) = tokio::fs::remove_file(&self.spool_path).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(?err, path = %self.spool_path.display(), "Failed to remove OCI spool file");
        }
    }
}

/// A parsed route under the `/v2` mount.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Root,
    Blob { name: String, digest: String },
    UploadPost { name: String },
    UploadSession { name: String, uuid: String },
    Manifest { name: String, reference: String },
    TagsList { name: String },
    Referrers { name: String },
}

/// Parses the request path (already stripped of the mount prefix) into a
/// [`Route`]. Repository names may span multiple path segments, so routes
/// are recognized from their fixed suffixes.
fn parse_route(path: &str) -> Option<Route> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Some(Route::Root);
    }
    let segments: Vec<&str> = trimmed.trim_end_matches('/').split('/').collect();
    let n = segments.len();
    if n >= 3 && segments[n - 2] == "tags" && segments[n - 1] == "list" {
        return Some(Route::TagsList {
            name: segments[..n - 2].join("/"),
        });
    }
    if n >= 3 && segments[n - 2] == "manifests" {
        return Some(Route::Manifest {
            name: segments[..n - 2].join("/"),
            reference: segments[n - 1].to_string(),
        });
    }
    if n >= 3 && segments[n - 2] == "referrers" {
        return Some(Route::Referrers {
            name: segments[..n - 2].join("/"),
        });
    }
    // `blobs/uploads/` with the trailing slash produces a trailing empty
    // segment that `trim_end_matches('/')` above already removed.
    if n >= 3 && segments[n - 2] == "blobs" && segments[n - 1] == "uploads" {
        return Some(Route::UploadPost {
            name: segments[..n - 2].join("/"),
        });
    }
    if n >= 4 && segments[n - 3] == "blobs" && segments[n - 2] == "uploads" {
        return Some(Route::UploadSession {
            name: segments[..n - 3].join("/"),
            uuid: segments[n - 1].to_string(),
        });
    }
    if n >= 3 && segments[n - 2] == "blobs" {
        return Some(Route::Blob {
            name: segments[..n - 2].join("/"),
            digest: segments[n - 1].to_string(),
        });
    }
    None
}

/// Returns the value of one query parameter, when present.
fn query_param<'a>(query: Option<&'a str>, key: &str) -> Option<Cow<'a, str>> {
    let query = query?;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == key {
            // Percent-decode just enough for digest/mount values (':' and
            // '/' arrive encoded from some clients).
            if v.contains('%') {
                let mut out = Vec::with_capacity(v.len());
                let bytes = v.as_bytes();
                let mut i = 0;
                while i < bytes.len() {
                    if bytes[i] == b'%'
                        && let Some(hex) = bytes.get(i + 1..i + 3)
                        && let Ok(hex_str) = core::str::from_utf8(hex)
                        && let Ok(byte) = u8::from_str_radix(hex_str, 16)
                    {
                        out.push(byte);
                        i += 3;
                        continue;
                    }
                    out.push(bytes[i]);
                    i += 1;
                }
                return Some(Cow::Owned(String::from_utf8(out).ok()?));
            }
            return Some(Cow::Borrowed(v));
        }
    }
    None
}

/// One configured OCI registry instance: resolved stores, identity, spool,
/// sessions, and counters.
#[derive(Debug, MetricsComponent)]
pub struct OciRegistryInstance {
    #[metric(help = "The configured instance name of this OCI registry")]
    instance_name: String,
    mount_path: String,
    cas_store: Store,
    index_store: Store,
    ref_store: Store,
    /// Canonical storage digest function (BLAKE3 on the fleet).
    canonical_fn: DigestHasherFunc,
    spool_dir: PathBuf,
    read_only: bool,
    enable_delete: bool,
    max_blob_size_bytes: u64,
    max_manifest_size_bytes: u64,
    upload_idle_timeout: Duration,
    read_token_hashes: Vec<[u8; 32]>,
    write_token_hashes: Vec<[u8; 32]>,
    blob_stream_semaphore: Arc<Semaphore>,
    max_open_upload_sessions: usize,
    sessions: StdMutex<HashMap<String, UploadSession>>,

    #[metric(help = "Number of blob GET/HEAD requests")]
    blob_gets: CounterWithTime,
    #[metric(help = "Number of completed blob uploads")]
    blob_puts: CounterWithTime,
    #[metric(help = "Number of manifest GET/HEAD requests")]
    manifest_gets: CounterWithTime,
    #[metric(help = "Number of accepted manifest PUT requests")]
    manifest_puts: CounterWithTime,
    #[metric(help = "Number of blob bytes served to clients")]
    blob_bytes_served: Counter,
    #[metric(help = "Number of blob bytes ingested into the CAS")]
    blob_bytes_ingested: Counter,
    #[metric(help = "Number of rejected mutating requests")]
    rejected_writes: CounterWithTime,
    #[metric(help = "Number of requests rejected with 401")]
    unauthorized_requests: CounterWithTime,
}

impl OciRegistryInstance {
    fn new(
        config: &WithInstanceName<OciRegistryServiceConfig>,
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let cas_store = store_manager
            .get_store(&config.cas_store)
            .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", config.cas_store))?;
        let index_store = store_manager
            .get_store(&config.index_store)
            .ok_or_else(|| {
                make_input_err!("'index_store': '{}' does not exist", config.index_store)
            })?;
        let ref_store = store_manager
            .get_store(&config.ref_store)
            .ok_or_else(|| make_input_err!("'ref_store': '{}' does not exist", config.ref_store))?;

        let canonical_fn = match config.digest_function.to_uppercase().as_str() {
            "BLAKE3" => DigestHasherFunc::Blake3,
            "SHA256" => DigestHasherFunc::Sha256,
            other => {
                return Err(make_input_err!(
                    "'digest_function' must be \"BLAKE3\" or \"SHA256\", got '{other}'"
                ));
            }
        };

        let mut mount_path = config
            .path
            .clone()
            .unwrap_or_else(|| DEFAULT_MOUNT_PATH.to_string());
        while mount_path.len() > 1 && mount_path.ends_with('/') {
            mount_path.pop();
        }
        if !mount_path.starts_with('/') || mount_path.len() < 2 {
            return Err(make_input_err!(
                "'path' must start with '/' and must not be the root, got '{mount_path}'"
            ));
        }

        let spool_dir = config.spool_path.as_ref().map_or_else(
            || {
                std::env::temp_dir()
                    .join(DEFAULT_SPOOL_DIR_NAME)
                    .join(&config.instance_name)
            },
            PathBuf::from,
        );
        prepare_spool_dir(&spool_dir).err_tip(|| {
            format!(
                "Preparing spool dir for oci_registry instance '{}'",
                config.instance_name
            )
        })?;

        let read_token_hashes = load_token_hashes(&config.read_token_files, "read")?;
        let write_token_hashes = load_token_hashes(&config.write_token_files, "write")?;

        Ok(Self {
            instance_name: config.instance_name.clone(),
            mount_path,
            cas_store,
            index_store,
            ref_store,
            canonical_fn,
            spool_dir,
            read_only: config.read_only,
            enable_delete: config.enable_delete,
            max_blob_size_bytes: config.max_blob_size_bytes,
            max_manifest_size_bytes: config.max_manifest_size_bytes,
            upload_idle_timeout: Duration::from_secs(config.upload_idle_timeout_s),
            read_token_hashes,
            write_token_hashes,
            blob_stream_semaphore: Arc::new(Semaphore::new(
                config.max_concurrent_blob_streams.max(1),
            )),
            max_open_upload_sessions: config.max_open_upload_sessions.max(1),
            sessions: StdMutex::new(HashMap::new()),
            blob_gets: CounterWithTime::default(),
            blob_puts: CounterWithTime::default(),
            manifest_gets: CounterWithTime::default(),
            manifest_puts: CounterWithTime::default(),
            blob_bytes_served: Counter::default(),
            blob_bytes_ingested: Counter::default(),
            rejected_writes: CounterWithTime::default(),
            unauthorized_requests: CounterWithTime::default(),
        })
    }

    /// The single choke point for digest-function hygiene: every store call
    /// in this service runs under the CANONICAL hasher context, so keys are
    /// always the storage identity regardless of the deployment default.
    fn canonical_ctx(&self) -> Result<OtelContext, Error> {
        make_ctx_for_hash_func(self.canonical_fn)
            .err_tip(|| "Making canonical hasher context in OciRegistryServer")
    }

    /// Runs a store future under the canonical hasher context.
    async fn with_canonical_ctx<F, T>(&self, fut: F) -> Result<T, Error>
    where
        F: Future<Output = Result<T, Error>> + Send,
    {
        fut.with_context(self.canonical_ctx()?).await
    }

    /// Gates reads; `None` means authorized.
    fn authorize_read(&self, headers: &HeaderMap) -> Option<Response> {
        if self.read_token_hashes.is_empty() {
            return None;
        }
        let presented = extract_token(headers).map(|token| sha256_of(token.as_bytes()));
        let authorized = presented.is_some_and(|hash| {
            token_matches_any(&hash, &self.read_token_hashes)
                || token_matches_any(&hash, &self.write_token_hashes)
        });
        if authorized {
            None
        } else {
            self.unauthorized_requests.inc();
            Some(unauthorized_response())
        }
    }

    /// Gates writes: read auth first, then — whenever ANY token auth is
    /// configured — a valid WRITE token (fail closed), then `read_only`
    /// (which wins over a valid token, as a 405).
    fn authorize_write(&self, headers: &HeaderMap) -> Option<Response> {
        if let Some(denied) = self.authorize_read(headers) {
            return Some(denied);
        }
        if !self.read_token_hashes.is_empty() || !self.write_token_hashes.is_empty() {
            let presented = extract_token(headers).map(|token| sha256_of(token.as_bytes()));
            let authorized =
                presented.is_some_and(|hash| token_matches_any(&hash, &self.write_token_hashes));
            if !authorized {
                self.unauthorized_requests.inc();
                return Some(unauthorized_response());
            }
        }
        if self.read_only {
            self.rejected_writes.inc();
            return Some(oci_error_response(
                StatusCode::METHOD_NOT_ALLOWED,
                OciErrorCode::Denied,
                "registry is read-only",
            ));
        }
        None
    }

    /// Looks up the digest-alias record for a sha256 wire hex, returning
    /// `None` on a clean miss.
    async fn lookup_alias(&self, sha256_hex: &str) -> Result<Option<OciDigestAlias>, Error> {
        let lookup = self
            .with_canonical_ctx(self.index_store.get_part_unchunked(
                StoreKey::Str(Cow::Owned(alias_key(sha256_hex))),
                0,
                None,
            ))
            .await;
        let raw = match lookup {
            Ok(raw) => raw,
            Err(err) if err.code == Code::NotFound => return Ok(None),
            Err(err) => {
                return Err(err)
                    .err_tip(|| format!("Looking up OCI digest alias for sha256:{sha256_hex}"));
            }
        };
        // A zero-length record is a delete tombstone (string-keyed stores
        // have no removal op); treat it as absent.
        if raw.is_empty() {
            return Ok(None);
        }
        OciDigestAlias::decode_record(&raw)
            .map(Some)
            .map_err(|e| make_err!(Code::Internal, "Corrupt OCI digest alias record: {e}"))
    }

    /// Writes the digest-alias record for an ingested blob. Written AFTER
    /// the blob lands: a crash between leaves an unreferenced blob (GC-able),
    /// never a dangling alias.
    async fn write_alias(
        &self,
        sha256_hex: &str,
        canonical: DigestInfo,
        media_type: &str,
    ) -> Result<(), Error> {
        let record = OciDigestAlias {
            canonical_hex: canonical.packed_hash().to_string(),
            size: canonical.size_bytes(),
            sha256_hex: sha256_hex.to_string(),
            media_type: media_type.to_string(),
        };
        let bytes = record.encode_record()?;
        self.with_canonical_ctx(self.index_store.update_oneshot(
            StoreKey::Str(Cow::Owned(alias_key(sha256_hex))),
            bytes.into(),
        ))
        .await
        .err_tip(|| "Storing OCI digest alias record")
    }

    /// Reads the tag record for `<name>:<tag>`, `None` on a clean miss.
    async fn lookup_tag(&self, name: &str, tag: &str) -> Result<Option<OciTagRecord>, Error> {
        let lookup = self
            .with_canonical_ctx(self.ref_store.get_part_unchunked(
                StoreKey::Str(Cow::Owned(tag_key(name, tag))),
                0,
                None,
            ))
            .await;
        let raw = match lookup {
            Ok(raw) => raw,
            Err(err) if err.code == Code::NotFound => return Ok(None),
            Err(err) => return Err(err).err_tip(|| format!("Looking up OCI tag '{name}:{tag}'")),
        };
        if raw.is_empty() {
            // Delete tombstone; see `lookup_alias`.
            return Ok(None);
        }
        OciTagRecord::decode_record(&raw)
            .map(Some)
            .map_err(|e| make_err!(Code::Internal, "Corrupt OCI tag record: {e}"))
    }

    /// Reads the per-repo tag index, `None` on a clean miss.
    async fn lookup_repo_tags(&self, name: &str) -> Result<Option<OciRepoTagIndex>, Error> {
        let lookup = self
            .with_canonical_ctx(self.ref_store.get_part_unchunked(
                StoreKey::Str(Cow::Owned(repo_tag_index_key(name))),
                0,
                None,
            ))
            .await;
        let raw = match lookup {
            Ok(raw) => raw,
            Err(err) if err.code == Code::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).err_tip(|| format!("Looking up OCI tag index for '{name}'"));
            }
        };
        if raw.is_empty() {
            return Ok(None);
        }
        OciRepoTagIndex::decode_record(&raw)
            .map(Some)
            .map_err(|e| make_err!(Code::Internal, "Corrupt OCI tag index record: {e}"))
    }

    /// Read-modify-merges the per-repo tag index. Idempotent set merge:
    /// concurrent pushes of different tags can race, the authoritative
    /// per-tag records are untouched, and a later PUT/DELETE repairs the
    /// index (design 2).
    async fn merge_repo_tags(
        &self,
        name: &str,
        insert: Option<&str>,
        remove: Option<&str>,
    ) -> Result<(), Error> {
        let mut index = self.lookup_repo_tags(name).await?.unwrap_or_default();
        if let Some(tag) = insert {
            index.insert(tag);
        }
        if let Some(tag) = remove {
            index.remove(tag);
        }
        let bytes = index.encode_record()?;
        self.with_canonical_ctx(self.ref_store.update_oneshot(
            StoreKey::Str(Cow::Owned(repo_tag_index_key(name))),
            bytes.into(),
        ))
        .await
        .err_tip(|| format!("Storing OCI tag index for '{name}'"))
    }

    /// Prunes sessions idle past the timeout; called opportunistically on
    /// session creation. Discarded spool files are removed asynchronously.
    fn prune_idle_sessions(&self) {
        let now = Instant::now();
        let expired: Vec<UploadSession> = {
            let mut sessions = self
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let expired_keys: Vec<String> = sessions
                .iter()
                .filter(|(_, s)| now.duration_since(s.last_touch) > self.upload_idle_timeout)
                .map(|(k, _)| k.clone())
                .collect();
            expired_keys
                .into_iter()
                .filter_map(|k| sessions.remove(&k))
                .collect()
        };
        for session in expired {
            // Fire-and-forget spool cleanup; `background_spawn!` detaches,
            // so the task survives this scope (a `spawn!` guard would abort
            // it on drop).
            background_spawn!("oci_session_prune", session.discard());
        }
    }

    /// Opens a fresh upload session (spool file + dual hashers), enforcing
    /// the open-session ceiling; `Err(response)` carries the HTTP rejection.
    async fn open_session(&self) -> Result<(String, UploadSession), Response> {
        self.prune_idle_sessions();
        {
            let sessions = self
                .sessions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if sessions.len() >= self.max_open_upload_sessions {
                return Err(oci_error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    OciErrorCode::TooManyRequests,
                    "too many open upload sessions",
                ));
            }
        }
        let uuid = Uuid::new_v4().to_string();
        let spool_path = self
            .spool_dir
            .join(format!("{SPOOL_FILE_PREFIX}{uuid}.blob"));
        let file = match fs::create_file(&spool_path).await {
            Ok(file) => file,
            Err(err) => {
                error!(?err, "Failed to create OCI spool file");
                return Err(backend_error_response("open_session", &err));
            }
        };
        Ok((
            uuid,
            UploadSession {
                spool_path,
                file,
                sha256: DigestHasherFunc::Sha256.hasher(),
                canonical: self.canonical_fn.hasher(),
                offset: 0,
                last_touch: Instant::now(),
            },
        ))
    }

    /// Takes a session out of the map (exclusive while in use).
    fn take_session(&self, uuid: &str) -> Option<UploadSession> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(uuid)
    }

    /// Returns a session to the map, refreshing its idle timestamp.
    fn put_session(&self, uuid: String, mut session: UploadSession) {
        session.last_touch = Instant::now();
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(uuid, session);
    }

    /// Streams a request body into a session's spool file, feeding BOTH
    /// hashers, enforcing the idle timeout and the size cap.
    async fn append_body(&self, session: &mut UploadSession, body: Body) -> Result<(), Error> {
        let mut stream = body.into_data_stream();
        loop {
            let chunk = match timeout(self.upload_idle_timeout, stream.next()).await {
                Ok(Some(chunk)) => {
                    chunk.map_err(|e| make_input_err!("Failed to read upload body: {e}"))?
                }
                Ok(None) => break,
                Err(_) => {
                    return Err(make_err!(
                        Code::DeadlineExceeded,
                        "blob upload stalled for more than {}s",
                        self.upload_idle_timeout.as_secs()
                    ));
                }
            };
            let new_offset = session
                .offset
                .saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if new_offset > self.max_blob_size_bytes {
                return Err(make_err!(
                    Code::ResourceExhausted,
                    "uploaded blob grows past the {}-byte limit",
                    self.max_blob_size_bytes
                ));
            }
            session.sha256.update(&chunk);
            session.canonical.update(&chunk);
            session
                .file
                .write_all(&chunk)
                .await
                .err_tip(|| "Writing blob chunk to spool file")?;
            session.offset = new_offset;
        }
        Ok(())
    }

    /// Finalizes an upload: verifies the recomputed sha256 against the
    /// client's declared digest (never trusting a digest we did not
    /// recompute), lands the bytes in `cas_store` under the CANONICAL
    /// digest, writes the alias record, and removes the spool file.
    ///
    /// Returns the `(sha256_hex, canonical_digest)` pair on success.
    async fn finalize_upload(
        &self,
        mut session: UploadSession,
        declared_sha256_hex: &str,
    ) -> Result<(String, DigestInfo), Response> {
        let sha256_digest = session.sha256.finalize_digest();
        let computed_hex = sha256_digest.packed_hash().to_string();
        if computed_hex != declared_sha256_hex {
            session.discard().await;
            return Err(oci_error_response(
                StatusCode::BAD_REQUEST,
                OciErrorCode::DigestInvalid,
                "declared digest does not match uploaded content",
            ));
        }
        let canonical_digest_info = session.canonical.finalize_digest();
        let size = session.offset;
        debug_assert_eq!(canonical_digest_info.size_bytes(), size);

        if let Err(err) = session.file.flush().await {
            session.discard().await;
            let err = Error::from(err);
            return Err(backend_error_response("finalize_upload flush", &err));
        }
        let upload_result = self
            .with_canonical_ctx(slow_update_store_with_file(
                self.cas_store.as_store_driver_pin(),
                canonical_digest_info,
                &mut session.file,
                UploadSizeInfo::ExactSize(size),
            ))
            .await;
        if let Err(err) = upload_result {
            session.discard().await;
            return Err(backend_error_response("finalize_upload store", &err));
        }
        session.discard().await;
        // Alias AFTER blob: a crash between leaves an orphan blob, never a
        // dangling alias.
        if let Err(err) = self
            .write_alias(&computed_hex, canonical_digest_info, "")
            .await
        {
            return Err(backend_error_response("finalize_upload alias", &err));
        }
        self.blob_bytes_ingested.add(size);
        self.blob_puts.inc();
        Ok((computed_hex, canonical_digest_info))
    }

    /// Spawns a task streaming `digest` from the CAS into a response body.
    fn stream_blob(&self, digest: DigestInfo, permit: OwnedSemaphorePermit) -> Result<Body, Error> {
        let (tx, rx) = make_buf_channel_pair();
        let ctx = self.canonical_ctx()?;
        let cas_store = self.cas_store.clone();
        let task = spawn!(
            "oci_registry_blob_stream",
            async move {
                if let Err(err) = cas_store.get_part(digest, tx, 0, None).await {
                    warn!(?err, ?digest, "Failed streaming OCI blob from CAS");
                }
            }
            .with_context(ctx)
        );
        Ok(Body::from_stream(BlobBodyStream {
            rx,
            _task: task,
            _permit: permit,
        }))
    }

    /// Absolute path (mount-prefixed) of a blob URL.
    fn blob_location(&self, name: &str, sha256_hex: &str) -> String {
        format!("{}/{name}/blobs/sha256:{sha256_hex}", self.mount_path)
    }

    /// Absolute path (mount-prefixed) of an upload-session URL.
    fn upload_location(&self, name: &str, uuid: &str) -> String {
        format!("{}/{name}/blobs/uploads/{uuid}", self.mount_path)
    }
}

/// Attaches the standard headers of a completed-blob response.
fn created_blob_response(location: &str, sha256_hex: &str) -> Response {
    let mut response = empty_response(StatusCode::CREATED);
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(location) {
        headers.insert(LOCATION, value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("sha256:{sha256_hex}")) {
        headers.insert("Docker-Content-Digest", value);
    }
    response
}

/// The `202` of an open/updated upload session.
fn upload_accepted_response(location: &str, uuid: &str, offset: u64) -> Response {
    let mut response = empty_response(StatusCode::ACCEPTED);
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(location) {
        headers.insert(LOCATION, value);
    }
    if let Ok(value) = HeaderValue::from_str(uuid) {
        headers.insert("Docker-Upload-UUID", value);
    }
    let end = offset.saturating_sub(1);
    if let Ok(value) = HeaderValue::from_str(&format!("0-{end}")) {
        headers.insert(RANGE, value);
    }
    response
}

async fn handle_v2_root(instance: &OciRegistryInstance, headers: &HeaderMap) -> Response {
    if let Some(denied) = instance.authorize_read(headers) {
        return denied;
    }
    let mut response = Response::new(Body::from("{}"));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(ERROR_CONTENT_TYPE));
    response
}

/// `HEAD`/`GET /v2/<name>/blobs/<digest>`.
async fn handle_blob_get(
    instance: &OciRegistryInstance,
    headers: &HeaderMap,
    digest: &str,
    include_body: bool,
) -> Response {
    if let Some(denied) = instance.authorize_read(headers) {
        return denied;
    }
    instance.blob_gets.inc();
    let Ok(sha256_hex) = parse_digest(digest) else {
        return oci_error_response(
            StatusCode::BAD_REQUEST,
            OciErrorCode::DigestInvalid,
            "unsupported or malformed digest",
        );
    };
    let alias = match instance.lookup_alias(sha256_hex).await {
        Ok(Some(alias)) => alias,
        Ok(None) => {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::BlobUnknown,
                "blob unknown to registry",
            );
        }
        Err(err) => return backend_error_response("blob_get alias", &err),
    };
    let canonical = match DigestInfo::try_new(&alias.canonical_hex, alias.size) {
        Ok(digest) => digest,
        Err(err) => return backend_error_response("blob_get digest", &err),
    };

    let mut response = if include_body {
        let Some(permit) = Arc::clone(&instance.blob_stream_semaphore)
            .try_acquire_owned()
            .ok()
        else {
            return oci_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                OciErrorCode::TooManyRequests,
                "too many concurrent blob streams",
            );
        };
        match instance.stream_blob(canonical, permit) {
            Ok(body) => {
                instance.blob_bytes_served.add(alias.size);
                let mut response = Response::new(body);
                *response.status_mut() = StatusCode::OK;
                response
            }
            Err(err) => return backend_error_response("blob_get stream", &err),
        }
    } else {
        empty_response(StatusCode::OK)
    };

    let headers = response.headers_mut();
    headers.insert(CONTENT_LENGTH, HeaderValue::from(alias.size));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(BLOB_CONTENT_TYPE));
    if let Ok(value) = HeaderValue::from_str(&format!("sha256:{sha256_hex}")) {
        headers.insert("Docker-Content-Digest", value);
    }
    response
}

/// `POST /v2/<name>/blobs/uploads/` — session open, monolithic push, or
/// cross-repo mount.
async fn handle_upload_post(
    instance: &OciRegistryInstance,
    headers: &HeaderMap,
    name: &str,
    query: Option<&str>,
    body: Body,
) -> Response {
    if let Some(denied) = instance.authorize_write(headers) {
        return denied;
    }

    // Cross-repo mount: blobs are global (repositories scope names, not
    // bytes), so a mount is an index existence check.
    if let Some(mount) = query_param(query, "mount")
        && let Ok(sha256_hex) = parse_digest(&mount)
    {
        // Malformed mount digests fall through to a normal session per spec.
        match instance.lookup_alias(sha256_hex).await {
            Ok(Some(_)) => {
                return created_blob_response(
                    &instance.blob_location(name, sha256_hex),
                    sha256_hex,
                );
            }
            Ok(None) => {} // fall through to a normal session
            Err(err) => return backend_error_response("upload_post mount", &err),
        }
    }

    let (uuid, mut session) = match instance.open_session().await {
        Ok(pair) => pair,
        Err(response) => return response,
    };

    // Monolithic one-shot: `?digest=` with the whole blob in this body.
    if let Some(digest) = query_param(query, "digest") {
        let sha256_hex = if let Ok(hex) = parse_digest(&digest) {
            hex.to_string()
        } else {
            session.discard().await;
            return oci_error_response(
                StatusCode::BAD_REQUEST,
                OciErrorCode::DigestInvalid,
                "unsupported or malformed digest",
            );
        };
        if let Err(err) = instance.append_body(&mut session, body).await {
            session.discard().await;
            return backend_error_response("upload_post body", &err);
        }
        return match instance.finalize_upload(session, &sha256_hex).await {
            Ok((sha256_hex, _)) => {
                created_blob_response(&instance.blob_location(name, &sha256_hex), &sha256_hex)
            }
            Err(response) => response,
        };
    }

    // Plain session open. The spec allows a body on POST only with
    // `?digest=`; drain nothing, register the session.
    let location = instance.upload_location(name, &uuid);
    instance.put_session(uuid.clone(), session);
    upload_accepted_response(&location, &uuid, 0)
}

/// `PATCH`/`PUT`/`GET`/`DELETE /v2/<name>/blobs/uploads/<uuid>`.
#[allow(clippy::too_many_lines)]
async fn handle_upload_session(
    instance: &OciRegistryInstance,
    method: &Method,
    headers: &HeaderMap,
    name: &str,
    uuid: &str,
    query: Option<&str>,
    body: Body,
) -> Response {
    if let Some(denied) = instance.authorize_write(headers) {
        return denied;
    }
    let location = instance.upload_location(name, uuid);

    if *method == Method::GET {
        // Session status.
        let Some(session) = instance.take_session(uuid) else {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::BlobUploadUnknown,
                "upload session unknown",
            );
        };
        let offset = session.offset;
        instance.put_session(uuid.to_string(), session);
        let mut response = upload_accepted_response(&location, uuid, offset);
        *response.status_mut() = StatusCode::NO_CONTENT;
        return response;
    }

    if *method == Method::DELETE {
        let Some(session) = instance.take_session(uuid) else {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::BlobUploadUnknown,
                "upload session unknown",
            );
        };
        session.discard().await;
        return empty_response(StatusCode::NO_CONTENT);
    }

    if *method == Method::PATCH {
        let Some(mut session) = instance.take_session(uuid) else {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::BlobUploadUnknown,
                "upload session unknown",
            );
        };
        // `Content-Range`, when present, must continue exactly at the
        // current offset (the spec's resumable-upload contract).
        if let Some(range) = headers.get("Content-Range").and_then(|v| v.to_str().ok()) {
            let start = range
                .split('-')
                .next()
                .and_then(|s| s.trim().parse::<u64>().ok());
            if start != Some(session.offset) {
                let offset = session.offset;
                instance.put_session(uuid.to_string(), session);
                let mut response = upload_accepted_response(&location, uuid, offset);
                *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                return response;
            }
        }
        if let Err(err) = instance.append_body(&mut session, body).await {
            session.discard().await;
            return backend_error_response("upload_patch body", &err);
        }
        let offset = session.offset;
        instance.put_session(uuid.to_string(), session);
        return upload_accepted_response(&location, uuid, offset);
    }

    if *method == Method::PUT {
        let Some(digest) = query_param(query, "digest") else {
            return oci_error_response(
                StatusCode::BAD_REQUEST,
                OciErrorCode::DigestInvalid,
                "final PUT requires ?digest=",
            );
        };
        let sha256_hex = match parse_digest(&digest) {
            Ok(hex) => hex.to_string(),
            Err(_) => {
                return oci_error_response(
                    StatusCode::BAD_REQUEST,
                    OciErrorCode::DigestInvalid,
                    "unsupported or malformed digest",
                );
            }
        };
        let Some(mut session) = instance.take_session(uuid) else {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::BlobUploadUnknown,
                "upload session unknown",
            );
        };
        // Optional final body chunk.
        if let Err(err) = instance.append_body(&mut session, body).await {
            session.discard().await;
            return backend_error_response("upload_put body", &err);
        }
        return match instance.finalize_upload(session, &sha256_hex).await {
            Ok((sha256_hex, _)) => {
                created_blob_response(&instance.blob_location(name, &sha256_hex), &sha256_hex)
            }
            Err(response) => response,
        };
    }

    empty_response(StatusCode::METHOD_NOT_ALLOWED)
}

/// Resolves a manifest reference (tag or digest) to `(sha256_hex,
/// canonical, media_type)`, or an error response.
async fn resolve_manifest(
    instance: &OciRegistryInstance,
    name: &str,
    reference: &str,
) -> Result<(String, DigestInfo, String), Response> {
    let Ok(parsed) = parse_manifest_reference(reference) else {
        return Err(oci_error_response(
            StatusCode::BAD_REQUEST,
            OciErrorCode::ManifestInvalid,
            "invalid manifest reference",
        ));
    };
    match parsed {
        ManifestReference::Tag(tag) => match instance.lookup_tag(name, &tag).await {
            Ok(Some(record)) => {
                let canonical = DigestInfo::try_new(&record.canonical_hex, record.size)
                    .map_err(|err| backend_error_response("resolve_manifest digest", &err))?;
                let media_type = if record.media_type.is_empty() {
                    DEFAULT_MANIFEST_CONTENT_TYPE.to_string()
                } else {
                    record.media_type
                };
                Ok((record.manifest_sha256_hex, canonical, media_type))
            }
            Ok(None) => Err(oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::ManifestUnknown,
                "manifest unknown to registry",
            )),
            Err(err) => Err(backend_error_response("resolve_manifest tag", &err)),
        },
        ManifestReference::Digest(sha256_hex) => match instance.lookup_alias(&sha256_hex).await {
            Ok(Some(alias)) => {
                let canonical = DigestInfo::try_new(&alias.canonical_hex, alias.size)
                    .map_err(|err| backend_error_response("resolve_manifest digest", &err))?;
                let media_type = if alias.media_type.is_empty() {
                    DEFAULT_MANIFEST_CONTENT_TYPE.to_string()
                } else {
                    alias.media_type
                };
                Ok((sha256_hex, canonical, media_type))
            }
            Ok(None) => Err(oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::ManifestUnknown,
                "manifest unknown to registry",
            )),
            Err(err) => Err(backend_error_response("resolve_manifest alias", &err)),
        },
    }
}

/// `HEAD`/`GET /v2/<name>/manifests/<ref>`.
async fn handle_manifest_get(
    instance: &OciRegistryInstance,
    headers: &HeaderMap,
    name: &str,
    reference: &str,
    include_body: bool,
) -> Response {
    if let Some(denied) = instance.authorize_read(headers) {
        return denied;
    }
    instance.manifest_gets.inc();
    let (sha256_hex, canonical, media_type) =
        match resolve_manifest(instance, name, reference).await {
            Ok(resolved) => resolved,
            Err(response) => return response,
        };

    let mut response = if include_body {
        let body = match instance
            .with_canonical_ctx(instance.cas_store.get_part_unchunked(canonical, 0, None))
            .await
        {
            Ok(body) => body,
            Err(err) if err.code == Code::NotFound => {
                return oci_error_response(
                    StatusCode::NOT_FOUND,
                    OciErrorCode::ManifestUnknown,
                    "manifest blob evicted",
                );
            }
            Err(err) => return backend_error_response("manifest_get body", &err),
        };
        let mut response = Response::new(Body::from(body));
        *response.status_mut() = StatusCode::OK;
        response
    } else {
        empty_response(StatusCode::OK)
    };
    let headers = response.headers_mut();
    headers.insert(CONTENT_LENGTH, HeaderValue::from(canonical.size_bytes()));
    if let Ok(value) = HeaderValue::from_str(&media_type) {
        headers.insert(CONTENT_TYPE, value);
    }
    if let Ok(value) = HeaderValue::from_str(&format!("sha256:{sha256_hex}")) {
        headers.insert("Docker-Content-Digest", value);
    }
    response
}

/// `PUT /v2/<name>/manifests/<ref>`.
#[allow(clippy::too_many_lines)]
async fn handle_manifest_put(
    instance: &OciRegistryInstance,
    headers: &HeaderMap,
    name: &str,
    reference: &str,
    body: Body,
) -> Response {
    if let Some(denied) = instance.authorize_write(headers) {
        return denied;
    }
    let Ok(reference) = parse_manifest_reference(reference) else {
        return oci_error_response(
            StatusCode::BAD_REQUEST,
            OciErrorCode::ManifestInvalid,
            "invalid manifest reference",
        );
    };
    let limit = usize::try_from(instance.max_manifest_size_bytes).unwrap_or(usize::MAX);
    let body_bytes = match read_body_limited(body, limit).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return oci_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                OciErrorCode::SizeInvalid,
                "manifest exceeds size limit",
            );
        }
        Err(err) => return backend_error_response("manifest_put body", &err),
    };

    let parsed = match parse_manifest(&body_bytes) {
        Ok(parsed) => parsed,
        Err(err) => {
            debug!(?err, "Rejecting invalid manifest");
            return oci_error_response(
                StatusCode::BAD_REQUEST,
                OciErrorCode::ManifestInvalid,
                "manifest failed validation",
            );
        }
    };

    // Subject-of-truth completeness: every referenced blob and child
    // manifest must already be indexed.
    for descriptor in &parsed.blob_references {
        match instance.lookup_alias(&descriptor.sha256_hex).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return oci_error_response(
                    StatusCode::BAD_REQUEST,
                    OciErrorCode::ManifestBlobUnknown,
                    "manifest references a blob this registry does not hold",
                );
            }
            Err(err) => return backend_error_response("manifest_put blob check", &err),
        }
    }
    for descriptor in &parsed.manifest_references {
        match instance.lookup_alias(&descriptor.sha256_hex).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return oci_error_response(
                    StatusCode::BAD_REQUEST,
                    OciErrorCode::ManifestBlobUnknown,
                    "index references a manifest this registry does not hold",
                );
            }
            Err(err) => return backend_error_response("manifest_put child check", &err),
        }
    }

    // Hash both ways.
    let mut sha256_hasher = DigestHasherFunc::Sha256.hasher();
    sha256_hasher.update(&body_bytes);
    let sha256_hex = sha256_hasher.finalize_digest().packed_hash().to_string();
    let mut canonical_hasher = instance.canonical_fn.hasher();
    canonical_hasher.update(&body_bytes);
    let canonical = canonical_hasher.finalize_digest();

    // A digest-addressed PUT must match its own content.
    if let ManifestReference::Digest(declared) = &reference
        && *declared != sha256_hex
    {
        return oci_error_response(
            StatusCode::BAD_REQUEST,
            OciErrorCode::DigestInvalid,
            "manifest content does not match its declared digest",
        );
    }

    let media_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .or_else(|| parsed.media_type.clone())
        .unwrap_or_else(|| DEFAULT_MANIFEST_CONTENT_TYPE.to_string());

    // Body → CAS under the canonical identity; alias AFTER blob.
    if let Err(err) = instance
        .with_canonical_ctx(
            instance
                .cas_store
                .update_oneshot(canonical, body_bytes.clone()),
        )
        .await
    {
        return backend_error_response("manifest_put store", &err);
    }
    if let Err(err) = instance
        .write_alias(&sha256_hex, canonical, &media_type)
        .await
    {
        return backend_error_response("manifest_put alias", &err);
    }

    // Tag binding: overwrite-wins, then merge the repo tag index.
    if let ManifestReference::Tag(tag) = &reference {
        let previous = match instance.lookup_tag(name, tag).await {
            Ok(previous) => previous,
            Err(err) => return backend_error_response("manifest_put previous", &err),
        };
        let record = OciTagRecord {
            media_type: media_type.clone(),
            manifest_sha256_hex: sha256_hex.clone(),
            size: canonical.size_bytes(),
            canonical_hex: canonical.packed_hash().to_string(),
            created_at_unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default(),
            previous_sha256_hex: previous.map(|p| p.manifest_sha256_hex).unwrap_or_default(),
        };
        let bytes = match record.encode_record() {
            Ok(bytes) => bytes,
            Err(err) => return backend_error_response("manifest_put record", &err),
        };
        if let Err(err) = instance
            .with_canonical_ctx(
                instance
                    .ref_store
                    .update_oneshot(StoreKey::Str(Cow::Owned(tag_key(name, tag))), bytes.into()),
            )
            .await
        {
            return backend_error_response("manifest_put tag", &err);
        }
        if let Err(err) = instance.merge_repo_tags(name, Some(tag), None).await {
            return backend_error_response("manifest_put tag index", &err);
        }
    }
    instance.manifest_puts.inc();

    let location = format!(
        "{}/{name}/manifests/sha256:{sha256_hex}",
        instance.mount_path
    );
    let mut response = created_blob_response(&location, &sha256_hex);
    // OCI 1.1: acknowledge a subject so clients know referrers processing
    // happened (the reverse index itself is schema-reserved, not built).
    if let Some(subject) = &parsed.subject_sha256_hex
        && let Ok(value) = HeaderValue::from_str(&format!("sha256:{subject}"))
    {
        response.headers_mut().insert("OCI-Subject", value);
    }
    response
}

/// `GET /v2/<name>/tags/list`.
async fn handle_tags_list(
    instance: &OciRegistryInstance,
    headers: &HeaderMap,
    name: &str,
    query: Option<&str>,
) -> Response {
    if let Some(denied) = instance.authorize_read(headers) {
        return denied;
    }
    let index = match instance.lookup_repo_tags(name).await {
        Ok(Some(index)) => index,
        Ok(None) => {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::NameUnknown,
                "repository name not known to registry",
            );
        }
        Err(err) => return backend_error_response("tags_list", &err),
    };
    let mut tags: Vec<&str> = index.tags.iter().map(String::as_str).collect();
    if let Some(last) = query_param(query, "last") {
        let last = last.to_string();
        tags.retain(|t| **t > *last);
    }
    let truncated = if let Some(n) = query_param(query, "n").and_then(|n| n.parse::<usize>().ok())
        && tags.len() > n
    {
        tags.truncate(n);
        true
    } else {
        false
    };
    let body = serde_json::json!({ "name": name, "tags": tags }).to_string();
    let link = if truncated {
        tags.last().map(|last| {
            format!(
                "<{}/{name}/tags/list?last={last}>; rel=\"next\"",
                instance.mount_path
            )
        })
    } else {
        None
    };
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(ERROR_CONTENT_TYPE));
    if let Some(link) = link
        && let Ok(value) = HeaderValue::from_str(&link)
    {
        response.headers_mut().insert("Link", value);
    }
    response
}

/// `DELETE /v2/<name>/manifests/<ref>` and `DELETE /v2/<name>/blobs/<digest>`.
///
/// Registry DELETE removes NAMES only — alias and tag records; content
/// lifetime belongs to CAS eviction (the lens has no GC).
async fn handle_delete(
    instance: &OciRegistryInstance,
    headers: &HeaderMap,
    name: &str,
    reference: &str,
    is_manifest: bool,
) -> Response {
    if let Some(denied) = instance.authorize_write(headers) {
        return denied;
    }
    if !instance.enable_delete {
        return oci_error_response(
            StatusCode::METHOD_NOT_ALLOWED,
            OciErrorCode::Unsupported,
            "delete is disabled on this registry",
        );
    }

    if is_manifest {
        match parse_manifest_reference(reference) {
            Ok(ManifestReference::Tag(tag)) => {
                match instance.lookup_tag(name, &tag).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return oci_error_response(
                            StatusCode::NOT_FOUND,
                            OciErrorCode::ManifestUnknown,
                            "tag unknown to registry",
                        );
                    }
                    Err(err) => return backend_error_response("delete tag lookup", &err),
                }
                // Tombstone via zero-length record removal: string-keyed
                // stores lack a delete op, so overwrite with an EMPTY value
                // and treat empty as absent on read.
                if let Err(err) = instance
                    .with_canonical_ctx(instance.ref_store.update_oneshot(
                        StoreKey::Str(Cow::Owned(tag_key(name, &tag))),
                        Bytes::new(),
                    ))
                    .await
                {
                    return backend_error_response("delete tag", &err);
                }
                if let Err(err) = instance.merge_repo_tags(name, None, Some(&tag)).await {
                    return backend_error_response("delete tag index", &err);
                }
                return empty_response(StatusCode::ACCEPTED);
            }
            Ok(ManifestReference::Digest(sha256_hex)) => {
                match instance.lookup_alias(&sha256_hex).await {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        return oci_error_response(
                            StatusCode::NOT_FOUND,
                            OciErrorCode::ManifestUnknown,
                            "manifest unknown to registry",
                        );
                    }
                    Err(err) => return backend_error_response("delete manifest lookup", &err),
                }
                if let Err(err) = instance
                    .with_canonical_ctx(instance.index_store.update_oneshot(
                        StoreKey::Str(Cow::Owned(alias_key(&sha256_hex))),
                        Bytes::new(),
                    ))
                    .await
                {
                    return backend_error_response("delete manifest", &err);
                }
                return empty_response(StatusCode::ACCEPTED);
            }
            Err(_) => {
                return oci_error_response(
                    StatusCode::BAD_REQUEST,
                    OciErrorCode::ManifestInvalid,
                    "invalid manifest reference",
                );
            }
        }
    }

    // Blob delete: alias-record removal only.
    let Ok(sha256_hex) = parse_digest(reference) else {
        return oci_error_response(
            StatusCode::BAD_REQUEST,
            OciErrorCode::DigestInvalid,
            "unsupported or malformed digest",
        );
    };
    match instance.lookup_alias(sha256_hex).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return oci_error_response(
                StatusCode::NOT_FOUND,
                OciErrorCode::BlobUnknown,
                "blob unknown to registry",
            );
        }
        Err(err) => return backend_error_response("delete blob lookup", &err),
    }
    if let Err(err) = instance
        .with_canonical_ctx(instance.index_store.update_oneshot(
            StoreKey::Str(Cow::Owned(alias_key(sha256_hex))),
            Bytes::new(),
        ))
        .await
    {
        return backend_error_response("delete blob", &err);
    }
    empty_response(StatusCode::ACCEPTED)
}

/// Routes one request under the mount to its handler.
async fn dispatch(State(instance): State<Arc<OciRegistryInstance>>, request: Request) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path();
    let query = uri.query();
    let headers = request.headers().clone();
    let body = request.into_body();

    let Some(route) = parse_route(path) else {
        return empty_response(StatusCode::NOT_FOUND);
    };

    // Validate repository names BEFORE any store key is derived.
    let name_ok = match &route {
        Route::Root => true,
        Route::Blob { name, .. }
        | Route::UploadPost { name }
        | Route::UploadSession { name, .. }
        | Route::Manifest { name, .. }
        | Route::TagsList { name }
        | Route::Referrers { name } => is_valid_repository_name(name),
    };
    if !name_ok {
        return oci_error_response(
            StatusCode::BAD_REQUEST,
            OciErrorCode::NameInvalid,
            "invalid repository name",
        );
    }

    match (&method, route) {
        (&(Method::GET | Method::HEAD), Route::Root) => handle_v2_root(&instance, &headers).await,
        (&Method::GET, Route::Blob { digest, .. }) => {
            handle_blob_get(&instance, &headers, &digest, true).await
        }
        (&Method::HEAD, Route::Blob { digest, .. }) => {
            handle_blob_get(&instance, &headers, &digest, false).await
        }
        (&Method::DELETE, Route::Blob { name, digest }) => {
            handle_delete(&instance, &headers, &name, &digest, false).await
        }
        (&Method::POST, Route::UploadPost { name }) => {
            handle_upload_post(&instance, &headers, &name, query, body).await
        }
        (_, Route::UploadSession { name, uuid }) => {
            handle_upload_session(&instance, &method, &headers, &name, &uuid, query, body).await
        }
        (&Method::GET, Route::Manifest { name, reference }) => {
            handle_manifest_get(&instance, &headers, &name, &reference, true).await
        }
        (&Method::HEAD, Route::Manifest { name, reference }) => {
            handle_manifest_get(&instance, &headers, &name, &reference, false).await
        }
        (&Method::PUT, Route::Manifest { name, reference }) => {
            handle_manifest_put(&instance, &headers, &name, &reference, body).await
        }
        (&Method::DELETE, Route::Manifest { name, reference }) => {
            handle_delete(&instance, &headers, &name, &reference, true).await
        }
        (&Method::GET, Route::TagsList { name }) => {
            handle_tags_list(&instance, &headers, &name, query).await
        }
        (&Method::GET, Route::Referrers { .. }) => {
            // OCI 1.1 referrers: schema-reserved, not implemented in v1;
            // 404 tells clients to fall back to the tag schema.
            empty_response(StatusCode::NOT_FOUND)
        }
        _ => empty_response(StatusCode::METHOD_NOT_ALLOWED),
    }
}

/// The OCI registry service: one axum router per configured instance.
#[derive(Debug, MetricsComponent)]
pub struct OciRegistryServer {
    #[metric(group = "instances")]
    instances: Vec<Arc<OciRegistryInstance>>,
}

impl OciRegistryServer {
    pub fn new(
        configs: &[WithInstanceName<OciRegistryServiceConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut instances = Vec::with_capacity(configs.len());
        for config in configs {
            instances.push(Arc::new(
                OciRegistryInstance::new(config, store_manager).err_tip(|| {
                    format!(
                        "Failed to create oci_registry instance '{}'",
                        config.instance_name
                    )
                })?,
            ));
        }
        Ok(Self { instances })
    }

    /// Returns one `(mount_path, Router)` pair per instance, to be nested
    /// into the server's axum router.
    #[must_use]
    pub fn routers(&self) -> Vec<(String, Router)> {
        self.instances
            .iter()
            .map(|instance| {
                let router = Router::new()
                    .fallback(dispatch)
                    .with_state(instance.clone());
                (instance.mount_path.clone(), router)
            })
            .collect()
    }
}
