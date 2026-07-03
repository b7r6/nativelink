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

//! The Nix binary-cache (substituter) HTTP service.
//!
//! Serves the Nix HTTP binary-cache protocol (`nix-cache-info`,
//! `<hash>.narinfo`, and `nar/<name>` endpoints) directly from `NativeLink`
//! stores, so `nix` clients can list a `NativeLink` deployment in their
//! `substituters` and `nix copy` can push to it.
//!
//! Store layout (see `nativelink-config/examples/nix_cache.json5`):
//!
//! - `cas_store`: digest-keyed uncompressed NAR blobs under
//!   `DigestInfo(sha256(nar), nar_size)`.
//! - `path_info_store`: string-keyed `narinfo` records under the
//!   32-character nix32 store-path hash.
//! - `alias_store`: string-keyed map from client-chosen NAR URL basenames
//!   to the `(digest, size)` of the NAR blob in `cas_store`.

use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_compression::tokio::bufread::{BzDecoder, GzipDecoder, XzDecoder, ZstdDecoder};
use axum::Router;
use axum::body::Body;
use axum::extract::{Path as UrlPath, State};
use axum::http::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::get;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, TryStreamExt};
use nativelink_config::cas_server::{NixCacheConfig, WithInstanceName};
use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_metric::MetricsComponent;
use nativelink_nix::nar_url::{
    GZIP_MAGIC, NarCodec, alias_key, canonical_nar_name, codec_for_name, format_alias, parse_alias,
    parse_canonical_nar_name,
};
use nativelink_nix::narinfo::is_store_path_hash;
use nativelink_nix::path_info::NixPathInfo;
use nativelink_nix::signing::NixSigningKey;
use nativelink_nix::{narinfo, nixbase32};
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::{DropCloserReadHalf, make_buf_channel_pair};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::metrics_utils::{Counter, CounterWithTime};
use nativelink_util::store_trait::{
    Store, StoreKey, StoreLike, UploadSizeInfo, slow_update_store_with_file,
};
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_util::{fs, spawn};
use opentelemetry::context::{Context as OtelContext, FutureExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;
use tracing::{error, warn};
use uuid::Uuid;

/// Content type of `nix-cache-info` responses.
const NIX_CACHE_INFO_CONTENT_TYPE: &str = "text/x-nix-cache-info";
/// Content type of `.narinfo` responses.
const NARINFO_CONTENT_TYPE: &str = "text/x-nix-narinfo";
/// Content type of NAR responses.
const NAR_CONTENT_TYPE: &str = "application/x-nix-nar";
/// File extension of `narinfo` documents.
const NARINFO_EXTENSION: &str = ".narinfo";
/// Upper bound on an uploaded `narinfo` body. Real documents are a few
/// hundred bytes; anything beyond this is rejected with `413`.
const MAX_NARINFO_BODY_BYTES: usize = 1024 * 1024;
/// Directory name under the system temp dir used when `spool_path` is not
/// configured.
const DEFAULT_SPOOL_DIR_NAME: &str = "nativelink-nix-spool";

/// Builds an [`opentelemetry`] context that pins the digest function to
/// SHA256. This is the single choke point for digest-function hygiene:
/// every store call in this module runs under this context. NAR digests
/// are always `sha256(nar)`, and with `require_explicit_digest_function`
/// plus a BLAKE3-leaning global default, a missed context would be a
/// silent-wrong-algorithm bug.
fn sha256_hasher_ctx() -> Result<OtelContext, Error> {
    make_ctx_for_hash_func(DigestHasherFunc::Sha256)
        .err_tip(|| "Making SHA256 hasher context in NixCacheServer")
}

/// Runs a store future under the SHA256 hasher context; see
/// [`sha256_hasher_ctx`].
async fn with_sha256_ctx<F, T>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>> + Send,
{
    fut.with_context(sha256_hasher_ctx()?).await
}

/// The single error-to-HTTP-status mapping for this service.
///
/// Absent content maps to `404` and never to a `5xx`: any `5xx` makes nix
/// retry five times and then disable the substituter for 60 seconds.
/// Backend failures must never map to `404` either — honesty both ways.
fn error_response(context: &'static str, err: &Error) -> Response {
    match err.code {
        Code::NotFound => empty_response(StatusCode::NOT_FOUND),
        Code::InvalidArgument => text_response(StatusCode::BAD_REQUEST, &err.to_string()),
        _ => {
            error!(?err, context, "Nix cache request failed");
            text_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn text_response(status: StatusCode, body: &str) -> Response {
    let mut response = Response::new(Body::from(body.to_owned()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}

/// Validates a `<32-char nix32 hash>.narinfo` request file name, returning
/// the store-path hash. Anything else is not a `narinfo` route.
fn narinfo_name_hash(file: &str) -> Option<&str> {
    let hash = file.strip_suffix(NARINFO_EXTENSION)?;
    is_store_path_hash(hash).then_some(hash)
}

/// A parsed `Range` request header, relative to a body of `total` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeRequest {
    /// No `Range` header, or one this service ignores (multi-range,
    /// suffix-range, or malformed): serve `200` with the full body.
    Full,
    /// Serve `start..=end`, guaranteed within `0..total`.
    Range { start: u64, end: u64 },
    /// `start >= total`: serve `416`.
    Unsatisfiable,
}

/// Parses a `Range` header, accepting only the single-range forms
/// `bytes=start-` and `bytes=start-end` (`end` inclusive, clamped to the
/// body). Anything else falls back to serving the full body.
fn parse_range(header: Option<&str>, total: u64) -> RangeRequest {
    let Some(header) = header else {
        return RangeRequest::Full;
    };
    let Some(spec) = header.strip_prefix("bytes=") else {
        return RangeRequest::Full;
    };
    // Multi-range requests are ignored: serve the full body instead.
    if spec.contains(',') {
        return RangeRequest::Full;
    }
    let Some((start_str, end_str)) = spec.trim().split_once('-') else {
        return RangeRequest::Full;
    };
    // Suffix ranges ("bytes=-N") are ignored.
    if start_str.is_empty() {
        return RangeRequest::Full;
    }
    let Ok(start) = start_str.parse::<u64>() else {
        return RangeRequest::Full;
    };
    let end = if end_str.is_empty() {
        total.saturating_sub(1)
    } else {
        match end_str.parse::<u64>() {
            Ok(end) => end.min(total.saturating_sub(1)),
            Err(_) => return RangeRequest::Full,
        }
    };
    if start >= total {
        return RangeRequest::Unsatisfiable;
    }
    if end < start {
        return RangeRequest::Full;
    }
    RangeRequest::Range { start, end }
}

/// Returns the value of the `Content-Length` header, if present and valid.
fn content_length_of(headers: &HeaderMap) -> Option<u64> {
    headers.get(CONTENT_LENGTH)?.to_str().ok()?.parse().ok()
}

/// Accepts only NAR upload names nix's uploader can produce: a stem of 52
/// nix32 characters (the client-side file hash) or the canonical
/// `{nix32(hash)}-{size}` form, followed by a supported extension.
/// Anything else — including traversal-shaped names — is rejected before
/// any body bytes are read and before anything is written.
fn is_valid_nar_upload_name(name: &str) -> bool {
    let Some(stem) = name
        .strip_suffix(".nar")
        .or_else(|| name.strip_suffix(".nar.xz"))
        .or_else(|| name.strip_suffix(".nar.zst"))
        .or_else(|| name.strip_suffix(".nar.bz2"))
    else {
        return false;
    };
    let hash_part = match stem.split_once('-') {
        Some((hash_part, size_str)) => {
            if size_str.is_empty() || !size_str.bytes().all(|b| b.is_ascii_digit()) {
                return false;
            }
            hash_part
        }
        None => stem,
    };
    hash_part.len() == 52 && nixbase32::decode(hash_part).is_ok()
}

/// Derives the CAS digest of a bare `.nar` upload from its client-chosen
/// name.
///
/// Canonical `{nix32(hash)}-{size}.nar` names carry both halves of the
/// digest. Nix's own uploader names identity-compressed NARs
/// `{nix32(sha256(payload))}.nar` — with identity compression that
/// 52-character file hash IS `sha256(nar)`, so together with the request's
/// `Content-Length` the digest is known before reading the body.
fn bare_nar_digest_from_name(name: &str, content_length: Option<u64>) -> Option<([u8; 32], u64)> {
    if let Some(resolved) = parse_canonical_nar_name(name) {
        return Some(resolved);
    }
    let stem = name.strip_suffix(".nar")?;
    if stem.len() != 52 {
        return None;
    }
    let nar_sha256: [u8; 32] = nixbase32::decode(stem).ok()?.try_into().ok()?;
    Some((nar_sha256, content_length?))
}

/// Reads a request body to completion, buffering at most `limit` bytes.
/// Returns `None` when the body exceeds the limit.
async fn read_body_limited(body: Body, limit: usize) -> Result<Option<Bytes>, Error> {
    let mut stream = body.into_data_stream();
    let mut buffer = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|e| make_err!(Code::Unavailable, "Failed to read request body: {e}"))?;
        if buffer.len() + chunk.len() > limit {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(Some(buffer.freeze()))
}

/// Adapter from the buf-channel read half to an HTTP response body stream.
///
/// The read half's [`Stream`] impl already carries the protocol this
/// service needs: an empty-`Bytes` recv is a clean EOF (stream ends), and
/// a writer dropped without `send_eof` surfaces as an `Err`, which makes
/// hyper abort the response mid-body — so a client retries instead of
/// trusting truncated bytes. Holding the [`JoinHandleDropGuard`] keeps the
/// producer task alive exactly as long as the body, and aborts it if the
/// client disconnects early.
#[derive(Debug)]
struct NarBodyStream {
    rx: DropCloserReadHalf,
    _task: JoinHandleDropGuard<()>,
}

impl Stream for NarBodyStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}

/// Removes a NAR spool file on drop unless disarmed via
/// [`SpoolFileGuard::cleanup`], so no spool files leak on error paths.
#[derive(Debug)]
struct SpoolFileGuard {
    path: Option<PathBuf>,
}

impl SpoolFileGuard {
    const fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    /// Removes the spool file eagerly and disarms the drop cleanup.
    async fn cleanup(mut self) {
        if let Some(path) = self.path.take()
            && let Err(err) = tokio::fs::remove_file(&path).await
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(?err, path = %path.display(), "Failed to remove NAR spool file");
        }
    }
}

impl Drop for SpoolFileGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take()
            && let Err(err) = std::fs::remove_file(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            warn!(?err, path = %path.display(), "Failed to remove NAR spool file");
        }
    }
}

/// Creates the spool directory and prunes files left over from a previous
/// run (for example after a crash mid-upload).
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
        if file_type.is_file()
            && let Err(err) = std::fs::remove_file(entry.path())
        {
            warn!(?err, path = %entry.path().display(), "Failed to prune stale NAR spool file");
        }
    }
    Ok(())
}

/// One configured Nix cache instance: resolved stores, identity, and
/// counters. Shared with every request handler through the router state.
#[derive(Debug, MetricsComponent)]
pub struct NixCacheInstance {
    #[metric(help = "The configured instance name of this Nix cache")]
    instance_name: String,
    mount_path: String,
    cas_store: Store,
    path_info_store: Store,
    alias_store: Store,
    signing_keys: Vec<NixSigningKey>,
    store_dir: String,
    priority: u32,
    want_mass_query: bool,
    read_only: bool,
    spool_dir: PathBuf,

    #[metric(help = "Number of narinfo GET requests")]
    narinfo_gets: CounterWithTime,
    #[metric(help = "Number of accepted narinfo PUT requests")]
    narinfo_puts: CounterWithTime,
    #[metric(help = "Number of NAR GET requests")]
    nar_gets: CounterWithTime,
    #[metric(help = "Number of accepted NAR PUT requests")]
    nar_puts: CounterWithTime,
    #[metric(help = "Number of NAR bytes served to clients")]
    nar_bytes_served: Counter,
    #[metric(help = "Number of uncompressed NAR bytes ingested into the CAS")]
    nar_bytes_ingested: Counter,
    #[metric(help = "Number of rejected PUT requests (narinfo and NAR)")]
    rejected_puts: CounterWithTime,
}

impl NixCacheInstance {
    fn new(
        config: &WithInstanceName<NixCacheConfig>,
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let cas_store = store_manager
            .get_store(&config.cas_store)
            .ok_or_else(|| make_input_err!("'cas_store': '{}' does not exist", config.cas_store))?;
        let path_info_store = store_manager
            .get_store(&config.path_info_store)
            .ok_or_else(|| {
                make_input_err!(
                    "'path_info_store': '{}' does not exist",
                    config.path_info_store
                )
            })?;
        let alias_store = store_manager
            .get_store(&config.alias_store)
            .ok_or_else(|| {
                make_input_err!("'alias_store': '{}' does not exist", config.alias_store)
            })?;

        let mut signing_keys = Vec::with_capacity(config.signing_key_files.len());
        for key_file in &config.signing_key_files {
            let contents = std::fs::read_to_string(key_file)
                .err_tip(|| format!("Failed to read Nix signing key file '{key_file}'"))?;
            let key = NixSigningKey::from_secret_string(contents.trim())
                .err_tip(|| format!("In Nix signing key file '{key_file}'"))?;
            signing_keys.push(key);
        }

        let mut mount_path = config
            .path
            .clone()
            .unwrap_or_else(|| format!("/nix/{}", config.instance_name));
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
                "Preparing NAR spool dir for nix_cache instance '{}'",
                config.instance_name
            )
        })?;

        Ok(Self {
            instance_name: config.instance_name.clone(),
            mount_path,
            cas_store,
            path_info_store,
            alias_store,
            signing_keys,
            store_dir: config.store_dir.clone(),
            priority: config.priority,
            want_mass_query: config.want_mass_query,
            read_only: config.read_only,
            spool_dir,
            narinfo_gets: CounterWithTime::default(),
            narinfo_puts: CounterWithTime::default(),
            nar_gets: CounterWithTime::default(),
            nar_puts: CounterWithTime::default(),
            nar_bytes_served: Counter::default(),
            nar_bytes_ingested: Counter::default(),
            rejected_puts: CounterWithTime::default(),
        })
    }

    /// Looks up an alias record for `url_basename`, returning the
    /// `(sha256, size)` of the NAR it names, or `None` when no alias
    /// exists. Corrupt alias records are internal errors, not 404s.
    async fn lookup_alias(&self, url_basename: &str) -> Result<Option<([u8; 32], u64)>, Error> {
        let key = alias_key(url_basename);
        let lookup = with_sha256_ctx(self.alias_store.get_part_unchunked(
            StoreKey::Str(Cow::Owned(key)),
            0,
            None,
        ))
        .await;
        let raw = match lookup {
            Ok(raw) => raw,
            Err(err) if err.code == Code::NotFound => return Ok(None),
            Err(err) => {
                return Err(err).err_tip(|| format!("Looking up NAR alias for '{url_basename}'"));
            }
        };
        let value = core::str::from_utf8(&raw).map_err(|e| {
            make_err!(
                Code::Internal,
                "Corrupt NAR alias record for '{url_basename}': {e}"
            )
        })?;
        match parse_alias(value) {
            Ok(resolved) => Ok(Some(resolved)),
            Err(err) => Err(make_err!(
                Code::Internal,
                "Corrupt NAR alias record for '{url_basename}': {err}"
            )),
        }
    }

    /// Spawns a task that streams `digest` from the CAS into a response
    /// body without buffering the whole NAR. See [`NarBodyStream`] for the
    /// EOF/abort semantics.
    fn stream_nar(
        &self,
        digest: DigestInfo,
        offset: u64,
        length: Option<u64>,
    ) -> Result<Body, Error> {
        let (tx, rx) = make_buf_channel_pair();
        let ctx = sha256_hasher_ctx()?;
        let cas_store = self.cas_store.clone();
        let task = spawn!(
            "nix_cache_nar_stream",
            async move {
                if let Err(err) = cas_store.get_part(digest, tx, offset, length).await {
                    // Dropping `tx` without an EOF aborts the response body,
                    // which is exactly what a mid-body failure must do.
                    warn!(?err, ?digest, "Failed streaming NAR from CAS");
                }
            }
            .with_context(ctx)
        );
        Ok(Body::from_stream(NarBodyStream { rx, _task: task }))
    }
}

/// The Nix binary-cache HTTP service: one router per configured instance.
#[derive(Debug, MetricsComponent)]
pub struct NixCacheServer {
    #[metric(group = "instances")]
    instances: Vec<Arc<NixCacheInstance>>,
}

impl NixCacheServer {
    /// Creates the service, resolving all store references and signing
    /// keys up front so misconfiguration fails at startup rather than on
    /// first request.
    pub fn new(
        configs: &[WithInstanceName<NixCacheConfig>],
        store_manager: &StoreManager,
    ) -> Result<Self, Error> {
        let mut instances = Vec::with_capacity(configs.len());
        for config in configs {
            instances.push(Arc::new(
                NixCacheInstance::new(config, store_manager).err_tip(|| {
                    format!(
                        "Failed to create nix_cache instance '{}'",
                        config.instance_name
                    )
                })?,
            ));
        }
        Ok(Self { instances })
    }

    /// Returns one `(mount_path, Router)` pair per instance, to be nested
    /// into the server's axum router.
    ///
    /// matchit (axum 0.8's router) rejects dynamic-suffix routes like
    /// `/{hash}.narinfo` at router-build time, so only whole-segment
    /// captures are used and the `.narinfo` suffix is stripped in-handler.
    /// HEAD handlers are registered explicitly: axum's `get()` would
    /// otherwise answer HEAD by running the GET body, which must never
    /// happen for multi-gigabyte NARs.
    #[must_use]
    pub fn routers(&self) -> Vec<(String, Router)> {
        self.instances
            .iter()
            .map(|instance| {
                let router = Router::new()
                    .route("/nix-cache-info", get(get_cache_info).put(put_cache_info))
                    .route("/nar/{name}", get(get_nar).head(head_nar).put(put_nar))
                    .route(
                        "/{file}",
                        get(get_narinfo).head(head_narinfo).put(put_narinfo),
                    )
                    .with_state(instance.clone());
                (instance.mount_path.clone(), router)
            })
            .collect()
    }
}

/// `GET /nix-cache-info`: always `200` — a `404` here bricks the client's
/// store open.
async fn get_cache_info(State(instance): State<Arc<NixCacheInstance>>) -> Response {
    let body = format!(
        "StoreDir: {}\nWantMassQuery: {}\nPriority: {}\n",
        instance.store_dir,
        u8::from(instance.want_mass_query),
        instance.priority,
    );
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static(NIX_CACHE_INFO_CONTENT_TYPE),
    );
    response
}

/// `PUT /nix-cache-info`: `nix copy` uploads this file on first push;
/// accept and discard (the served document always comes from config).
async fn put_cache_info(State(instance): State<Arc<NixCacheInstance>>) -> Response {
    if instance.read_only {
        instance.rejected_puts.inc();
        return text_response(StatusCode::METHOD_NOT_ALLOWED, "cache is read-only");
    }
    empty_response(StatusCode::OK)
}

/// `HEAD /{hash}.narinfo`.
async fn head_narinfo(
    State(instance): State<Arc<NixCacheInstance>>,
    UrlPath(file): UrlPath<String>,
) -> Response {
    head_narinfo_inner(&instance, &file)
        .await
        .unwrap_or_else(|err| error_response("HEAD narinfo", &err))
}

async fn head_narinfo_inner(instance: &NixCacheInstance, file: &str) -> Result<Response, Error> {
    let Some(hash) = narinfo_name_hash(file) else {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    };
    // The completeness-checking wrapper around `path_info_store` makes a
    // narinfo whose NAR was evicted read as absent here — by design.
    let found = with_sha256_ctx(instance.path_info_store.has(StoreKey::new_str(hash)))
        .await
        .err_tip(|| "Checking narinfo existence in path_info_store")?;
    if found.is_some() {
        Ok(empty_response(StatusCode::OK))
    } else {
        Ok(empty_response(StatusCode::NOT_FOUND))
    }
}

/// `GET /{hash}.narinfo`.
async fn get_narinfo(
    State(instance): State<Arc<NixCacheInstance>>,
    UrlPath(file): UrlPath<String>,
) -> Response {
    get_narinfo_inner(&instance, &file)
        .await
        .unwrap_or_else(|err| error_response("GET narinfo", &err))
}

async fn get_narinfo_inner(instance: &NixCacheInstance, file: &str) -> Result<Response, Error> {
    instance.narinfo_gets.inc();
    let Some(hash) = narinfo_name_hash(file) else {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    };
    // `Code::NotFound` propagates to a 404 through `error_response`.
    let record = with_sha256_ctx(instance.path_info_store.get_part_unchunked(
        StoreKey::new_str(hash),
        0,
        None,
    ))
    .await
    .err_tip(|| "Fetching narinfo record from path_info_store")?;
    // A record that exists but does not decode is a server-side problem
    // (500), not a client error: never let it surface as a 400 or 404.
    let path_info = NixPathInfo::decode_record(&record)
        .map_err(|err| make_err!(Code::Internal, "Corrupt narinfo record for '{hash}': {err}"))?;
    let nar_sha256: [u8; 32] = path_info.nar_sha256.as_slice().try_into().map_err(|_| {
        make_err!(
            Code::Internal,
            "Corrupt narinfo record for '{hash}': NAR hash is not 32 bytes"
        )
    })?;
    // NARs are stored (and served) uncompressed under their canonical name.
    let url = canonical_nar_name(&nar_sha256, path_info.nar_size);
    let info = path_info
        .to_nar_info(format!("nar/{url}"), "none".to_string())
        .map_err(|err| make_err!(Code::Internal, "Corrupt narinfo record for '{hash}': {err}"))?;
    let rendered = info.render();
    let mut response = Response::new(Body::from(rendered));
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(NARINFO_CONTENT_TYPE));
    Ok(response)
}

/// `PUT /{hash}.narinfo`.
async fn put_narinfo(
    State(instance): State<Arc<NixCacheInstance>>,
    UrlPath(file): UrlPath<String>,
    body: Body,
) -> Response {
    let response = put_narinfo_inner(&instance, &file, body)
        .await
        .unwrap_or_else(|err| error_response("PUT narinfo", &err));
    if response.status() == StatusCode::CREATED {
        instance.narinfo_puts.inc();
    } else {
        instance.rejected_puts.inc();
    }
    response
}

async fn put_narinfo_inner(
    instance: &NixCacheInstance,
    file: &str,
    body: Body,
) -> Result<Response, Error> {
    if instance.read_only {
        return Ok(text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "cache is read-only",
        ));
    }
    let Some(hash) = narinfo_name_hash(file) else {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    };
    let Some(body_bytes) = read_body_limited(body, MAX_NARINFO_BODY_BYTES).await? else {
        return Ok(text_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "narinfo body exceeds 1 MiB",
        ));
    };
    let text = core::str::from_utf8(&body_bytes)
        .map_err(|e| make_input_err!("narinfo body is not UTF-8: {e}"))?;
    let info = narinfo::parse(text).err_tip(|| "Parsing uploaded narinfo")?;
    let store_hash = info
        .store_path_hash()
        .err_tip(|| "Validating StorePath in uploaded narinfo")?;
    if store_hash != hash {
        return Err(make_input_err!(
            "narinfo file name hash '{hash}' does not match StorePath hash '{store_hash}'"
        ));
    }

    // Resolve the NAR digest from the URL the client wrote into the
    // narinfo: either a canonical name, or an alias recorded when the NAR
    // was uploaded.
    let url_basename = info.url.rsplit('/').next().unwrap_or(info.url.as_str());
    let (nar_sha256, nar_size) = match parse_canonical_nar_name(url_basename) {
        Some(resolved) => resolved,
        None => match instance.lookup_alias(url_basename).await? {
            Some(resolved) => resolved,
            None => {
                return Ok(text_response(
                    StatusCode::CONFLICT,
                    "NAR must be uploaded before its narinfo",
                ));
            }
        },
    };
    if nar_sha256 != info.nar_hash || nar_size != info.nar_size {
        return Err(make_input_err!(
            "narinfo NarHash/NarSize do not match the previously uploaded NAR at '{}'",
            info.url
        ));
    }
    let digest = DigestInfo::new(nar_sha256, nar_size);
    let nar_present = with_sha256_ctx(instance.cas_store.has(digest))
        .await
        .err_tip(|| "Checking NAR existence in cas_store")?;
    if nar_present.is_none() {
        return Ok(text_response(
            StatusCode::CONFLICT,
            "NAR must be uploaded before its narinfo",
        ));
    }

    // Preserve the client's signatures verbatim and add ours for any
    // configured key that has not signed this fingerprint yet.
    let mut path_info = NixPathInfo::from_nar_info(&info);
    let fingerprint = path_info
        .fingerprint()
        .err_tip(|| "Computing narinfo fingerprint for signing")?;
    let new_sigs: Vec<String> = instance
        .signing_keys
        .iter()
        .filter(|key| {
            !path_info
                .signatures
                .iter()
                .any(|sig| sig.split(':').next() == Some(key.name()))
        })
        .map(|key| key.sign(&fingerprint))
        .collect();
    path_info.signatures.extend(new_sigs);

    let record = path_info
        .encode_record()
        .map_err(|err| make_err!(Code::Internal, "Failed to encode narinfo record: {err}"))?;
    with_sha256_ctx(
        instance
            .path_info_store
            .update_oneshot(StoreKey::new_str(hash), record.into()),
    )
    .await
    .err_tip(|| "Storing narinfo record in path_info_store")?;
    Ok(empty_response(StatusCode::CREATED))
}

/// Resolves a NAR request name to the `(sha256, size)` of the stored NAR:
/// canonical names directly, anything else through the alias store.
async fn resolve_nar_name(
    instance: &NixCacheInstance,
    name: &str,
) -> Result<Option<([u8; 32], u64)>, Error> {
    if let Some(resolved) = parse_canonical_nar_name(name) {
        return Ok(Some(resolved));
    }
    instance.lookup_alias(name).await
}

/// `HEAD /nar/{name}`.
async fn head_nar(
    State(instance): State<Arc<NixCacheInstance>>,
    UrlPath(name): UrlPath<String>,
) -> Response {
    head_nar_inner(&instance, &name)
        .await
        .unwrap_or_else(|err| error_response("HEAD nar", &err))
}

async fn head_nar_inner(instance: &NixCacheInstance, name: &str) -> Result<Response, Error> {
    let Some((nar_sha256, nar_size)) = resolve_nar_name(instance, name).await? else {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    };
    // The alias alone must not answer: an alias whose NAR was evicted must
    // 404 so `nix copy` re-uploads the NAR. A 200 here followed by a
    // rejected narinfo PUT would brick the push.
    let digest = DigestInfo::new(nar_sha256, nar_size);
    let nar_present = with_sha256_ctx(instance.cas_store.has(digest))
        .await
        .err_tip(|| "Checking NAR existence in cas_store")?;
    if nar_present.is_none() {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    }
    // Content-Length comes from the name/alias, never from has(): store
    // compositions (compression, fast_slow, ...) report physical sizes.
    let mut response = empty_response(StatusCode::OK);
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(NAR_CONTENT_TYPE));
    headers.insert(CONTENT_LENGTH, HeaderValue::from(nar_size));
    headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    Ok(response)
}

/// `GET /nar/{name}`.
async fn get_nar(
    State(instance): State<Arc<NixCacheInstance>>,
    UrlPath(name): UrlPath<String>,
    headers: HeaderMap,
) -> Response {
    get_nar_inner(&instance, &name, &headers)
        .await
        .unwrap_or_else(|err| error_response("GET nar", &err))
}

async fn get_nar_inner(
    instance: &NixCacheInstance,
    name: &str,
    headers: &HeaderMap,
) -> Result<Response, Error> {
    instance.nar_gets.inc();
    let resolved = if let Some(resolved) = parse_canonical_nar_name(name) {
        Some(resolved)
    } else if codec_for_name(name) == Some(NarCodec::None) {
        instance.lookup_alias(name).await?
    } else {
        // We store uncompressed NAR bytes: serving them under a compressed
        // alias name (e.g. `.nar.xz`) would corrupt a client that trusts
        // the name. Nix substitution only ever uses OUR narinfo URLs,
        // which are canonical, so nothing legitimate hits this path.
        None
    };
    let Some((nar_sha256, nar_size)) = resolved else {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    };
    let digest = DigestInfo::new(nar_sha256, nar_size);
    // Check existence before committing a 200: a missing NAR must be an
    // honest 404, not a committed status followed by an aborted body.
    let nar_present = with_sha256_ctx(instance.cas_store.has(digest))
        .await
        .err_tip(|| "Checking NAR existence in cas_store")?;
    if nar_present.is_none() {
        return Ok(empty_response(StatusCode::NOT_FOUND));
    }

    let range_header = headers.get(RANGE).and_then(|value| value.to_str().ok());
    match parse_range(range_header, nar_size) {
        RangeRequest::Unsatisfiable => {
            let mut response = empty_response(StatusCode::RANGE_NOT_SATISFIABLE);
            response.headers_mut().insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{nar_size}"))
                    .map_err(|e| make_err!(Code::Internal, "Invalid Content-Range header: {e}"))?,
            );
            Ok(response)
        }
        RangeRequest::Full => {
            let body = instance.stream_nar(digest, 0, None)?;
            instance.nar_bytes_served.add(nar_size);
            let mut response = Response::new(body);
            let response_headers = response.headers_mut();
            response_headers.insert(CONTENT_TYPE, HeaderValue::from_static(NAR_CONTENT_TYPE));
            response_headers.insert(CONTENT_LENGTH, HeaderValue::from(nar_size));
            response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            Ok(response)
        }
        RangeRequest::Range { start, end } => {
            let length = end - start + 1;
            let body = instance.stream_nar(digest, start, Some(length))?;
            instance.nar_bytes_served.add(length);
            let mut response = Response::new(body);
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            let response_headers = response.headers_mut();
            response_headers.insert(CONTENT_TYPE, HeaderValue::from_static(NAR_CONTENT_TYPE));
            response_headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
            response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            response_headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{end}/{nar_size}"))
                    .map_err(|e| make_err!(Code::Internal, "Invalid Content-Range header: {e}"))?,
            );
            Ok(response)
        }
    }
}

/// `PUT /nar/{name}`.
async fn put_nar(
    State(instance): State<Arc<NixCacheInstance>>,
    UrlPath(name): UrlPath<String>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let response = put_nar_inner(&instance, &name, &headers, body)
        .await
        .unwrap_or_else(|err| error_response("PUT nar", &err));
    if response.status() == StatusCode::CREATED {
        instance.nar_puts.inc();
    } else {
        instance.rejected_puts.inc();
    }
    response
}

async fn put_nar_inner(
    instance: &NixCacheInstance,
    name: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, Error> {
    if instance.read_only {
        return Ok(text_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "cache is read-only",
        ));
    }
    let Some(name_codec) = codec_for_name(name) else {
        return Err(make_input_err!(
            "unsupported NAR extension in '{name}': expected .nar, .nar.xz, .nar.zst, or .nar.bz2"
        ));
    };
    if !is_valid_nar_upload_name(name) {
        return Err(make_input_err!(
            "invalid NAR name '{name}': expected '{{52 nix32 chars}}' or \
             '{{52 nix32 chars}}-{{size}}' before the extension"
        ));
    }
    let content_length = content_length_of(headers);
    let mut data_stream = body.into_data_stream();

    // Nix's uploader maps BOTH gzip and identity compression to a bare
    // `.nar` name, so sniff the first two body bytes for the gzip magic.
    let mut prefix_chunks: Vec<Bytes> = Vec::new();
    let codec = if name_codec == NarCodec::None {
        let mut prefix_len = 0_usize;
        while prefix_len < GZIP_MAGIC.len() {
            match data_stream.next().await {
                Some(Ok(chunk)) => {
                    if !chunk.is_empty() {
                        prefix_len += chunk.len();
                        prefix_chunks.push(chunk);
                    }
                }
                Some(Err(err)) => {
                    return Err(make_err!(
                        Code::Unavailable,
                        "Failed to read NAR upload body: {err}"
                    ));
                }
                None => break,
            }
        }
        let mut magic_bytes = prefix_chunks.iter().flat_map(|chunk| chunk.iter().copied());
        if (magic_bytes.next(), magic_bytes.next()) == (Some(GZIP_MAGIC[0]), Some(GZIP_MAGIC[1])) {
            NarCodec::Gzip
        } else {
            NarCodec::None
        }
    } else {
        name_codec
    };
    let full_stream = futures::stream::iter(prefix_chunks.into_iter().map(Ok)).chain(data_stream);

    let (nar_sha256, nar_size) = if codec == NarCodec::None
        && let Some((digest_sha256, digest_size)) = bare_nar_digest_from_name(name, content_length)
    {
        // Fast path: the digest is known before the body is read, so the
        // bytes stream straight into the CAS. The recommended `verify{}`
        // wrapper recomputes sha256 in-stream and rejects mismatches at
        // EOF. No spool file involved.
        ingest_nar_direct(instance, digest_sha256, digest_size, full_stream).await?;
        (digest_sha256, digest_size)
    } else {
        ingest_nar_spooled(instance, codec, full_stream).await?
    };

    // Only record the alias after the CAS write succeeded: an alias's
    // existence implies its NAR is (or was) present.
    with_sha256_ctx(instance.alias_store.update_oneshot(
        StoreKey::Str(Cow::Owned(alias_key(name))),
        format_alias(&nar_sha256, nar_size).into(),
    ))
    .await
    .err_tip(|| "Recording NAR alias")?;
    instance.nar_bytes_ingested.add(nar_size);
    Ok(empty_response(StatusCode::CREATED))
}

/// Streams an identity-compressed NAR body directly into the CAS under a
/// digest that is already known from the request.
async fn ingest_nar_direct<S>(
    instance: &NixCacheInstance,
    nar_sha256: [u8; 32],
    nar_size: u64,
    mut stream: S,
) -> Result<(), Error>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Send + Unpin,
{
    let digest = DigestInfo::new(nar_sha256, nar_size);
    let (mut tx, rx) = make_buf_channel_pair();
    let update_fut = with_sha256_ctx(instance.cas_store.update(
        digest,
        rx,
        UploadSizeInfo::ExactSize(nar_size),
    ));
    let pump_fut = async move {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk
                .map_err(|e| make_err!(Code::Unavailable, "Failed to read NAR upload body: {e}"))?;
            if chunk.is_empty() {
                continue;
            }
            tx.send(chunk)
                .await
                .err_tip(|| "Sending NAR bytes to cas_store")?;
        }
        tx.send_eof().err_tip(|| "Sending EOF to cas_store")
    };
    let (update_res, pump_res) = futures::join!(update_fut, pump_fut);
    pump_res
        .merge(update_res)
        .err_tip(|| "Uploading NAR to cas_store")?;
    Ok(())
}

/// Stream-decompresses a NAR body while hashing, spooling the decompressed
/// bytes to a temp file; once the digest is known the spool file is
/// streamed into the CAS and removed (also on every error path, via
/// [`SpoolFileGuard`]).
async fn ingest_nar_spooled<S>(
    instance: &NixCacheInstance,
    codec: NarCodec,
    stream: S,
) -> Result<([u8; 32], u64), Error>
where
    S: Stream<Item = Result<Bytes, axum::Error>> + Send + Unpin,
{
    let reader = StreamReader::new(
        stream.map_err(|e| std::io::Error::other(format!("Failed to read NAR upload body: {e}"))),
    );
    let mut decoder: Box<dyn AsyncRead + Send + Unpin> = match codec {
        NarCodec::None => Box::new(reader),
        NarCodec::Gzip => Box::new(GzipDecoder::new(reader)),
        NarCodec::Xz => Box::new(XzDecoder::new(reader)),
        NarCodec::Zstd => Box::new(ZstdDecoder::new(reader)),
        NarCodec::Bzip2 => Box::new(BzDecoder::new(reader)),
    };

    let spool_path = instance.spool_dir.join(format!("{}.nar", Uuid::new_v4()));
    let spool_guard = SpoolFileGuard::new(spool_path.clone());
    let mut spool_file = fs::create_file(&spool_path)
        .await
        .err_tip(|| format!("Creating NAR spool file {}", spool_path.display()))?;

    let mut hasher = DigestHasherFunc::Sha256.hasher();
    let mut chunk = BytesMut::with_capacity(fs::DEFAULT_READ_BUFF_SIZE);
    loop {
        chunk.clear();
        // Decompression failures are client-data problems (400), not
        // internal errors.
        let read = decoder
            .read_buf(&mut chunk)
            .await
            .map_err(|e| make_input_err!("Failed to decompress NAR upload: {e}"))?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk);
        spool_file
            .write_all(&chunk)
            .await
            .err_tip(|| "Writing decompressed NAR to spool file")?;
    }
    spool_file
        .flush()
        .await
        .err_tip(|| "Flushing NAR spool file")?;

    let digest = hasher.finalize_digest();
    let nar_size = digest.size_bytes();
    with_sha256_ctx(slow_update_store_with_file(
        instance.cas_store.as_store_driver_pin(),
        digest,
        &mut spool_file,
        UploadSizeInfo::ExactSize(nar_size),
    ))
    .await
    .err_tip(|| "Uploading spooled NAR to cas_store")?;
    drop(spool_file);
    spool_guard.cleanup().await;

    let nar_sha256_ref: &[u8; 32] = digest.packed_hash();
    Ok((*nar_sha256_ref, nar_size))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use nativelink_config::stores::MemorySpec;
    use nativelink_error::{Code, Error, make_err, make_input_err};
    use nativelink_macro::nativelink_test;
    use nativelink_store::memory_store::MemoryStore;
    use pretty_assertions::assert_eq;
    use tokio::io::{AsyncRead, AsyncReadExt};
    use uuid::Uuid;

    use super::{
        Counter, CounterWithTime, DEFAULT_SPOOL_DIR_NAME, DigestHasher, DigestHasherFunc,
        DigestInfo, NarCodec, NixCacheInstance, RangeRequest, StatusCode, Store, StoreLike,
        bare_nar_digest_from_name, error_response, ingest_nar_spooled, is_valid_nar_upload_name,
        narinfo_name_hash, parse_range, prepare_spool_dir, with_sha256_ctx,
    };

    fn test_instance() -> Arc<NixCacheInstance> {
        let spool_dir = std::env::temp_dir()
            .join(DEFAULT_SPOOL_DIR_NAME)
            .join(format!("unit-test-{}", Uuid::new_v4()));
        prepare_spool_dir(&spool_dir).expect("prepare spool dir");
        Arc::new(NixCacheInstance {
            instance_name: "unit-test".to_string(),
            mount_path: "/nix/unit-test".to_string(),
            cas_store: Store::new(MemoryStore::new(&MemorySpec::default())),
            path_info_store: Store::new(MemoryStore::new(&MemorySpec::default())),
            alias_store: Store::new(MemoryStore::new(&MemorySpec::default())),
            signing_keys: vec![],
            store_dir: "/nix/store".to_string(),
            priority: 40,
            want_mass_query: true,
            read_only: false,
            spool_dir,
            narinfo_gets: CounterWithTime::default(),
            narinfo_puts: CounterWithTime::default(),
            nar_gets: CounterWithTime::default(),
            nar_puts: CounterWithTime::default(),
            nar_bytes_served: Counter::default(),
            nar_bytes_ingested: Counter::default(),
            rejected_puts: CounterWithTime::default(),
        })
    }

    async fn compress(codec: NarCodec, payload: &[u8]) -> Vec<u8> {
        use async_compression::tokio::bufread::{BzEncoder, GzipEncoder, XzEncoder, ZstdEncoder};
        let mut encoder: Box<dyn AsyncRead + Send + Unpin + '_> = match codec {
            NarCodec::None => Box::new(payload),
            NarCodec::Gzip => Box::new(GzipEncoder::new(payload)),
            NarCodec::Xz => Box::new(XzEncoder::new(payload)),
            NarCodec::Zstd => Box::new(ZstdEncoder::new(payload)),
            NarCodec::Bzip2 => Box::new(BzEncoder::new(payload)),
        };
        let mut out = Vec::new();
        encoder.read_to_end(&mut out).await.expect("compress");
        out
    }

    /// Proves every compression backend links and functions at runtime
    /// (notably xz's liblzma), that the spooled path recovers the exact
    /// digest of the decompressed bytes, and that no spool files leak.
    #[nativelink_test]
    async fn spooled_ingest_decompresses_every_codec() -> Result<(), Error> {
        let instance = test_instance();
        let payload = b"nar payload for spooled codec round trips ".repeat(64);
        let expected_size = u64::try_from(payload.len()).expect("size fits in u64");
        let expected_sha256: [u8; 32] = {
            let mut hasher = DigestHasherFunc::Sha256.hasher();
            hasher.update(&payload);
            let digest = hasher.finalize_digest();
            let hash_ref: &[u8; 32] = digest.packed_hash();
            *hash_ref
        };
        for codec in [
            NarCodec::None,
            NarCodec::Gzip,
            NarCodec::Xz,
            NarCodec::Zstd,
            NarCodec::Bzip2,
        ] {
            let compressed = compress(codec, &payload).await;
            let stream = futures::stream::iter([Ok::<_, axum::Error>(Bytes::from(compressed))]);
            let (nar_sha256, nar_size) = ingest_nar_spooled(&instance, codec, stream)
                .await
                .unwrap_or_else(|e| panic!("ingest for {codec:?}: {e:?}"));
            assert_eq!(nar_size, expected_size, "size for {codec:?}");
            assert_eq!(nar_sha256, expected_sha256, "hash for {codec:?}");
            let stored = with_sha256_ctx(instance.cas_store.get_part_unchunked(
                DigestInfo::new(nar_sha256, nar_size),
                0,
                None,
            ))
            .await
            .expect("stored NAR readable");
            assert_eq!(stored.as_ref(), payload.as_slice(), "content for {codec:?}");
        }
        let leftovers = std::fs::read_dir(&instance.spool_dir)
            .expect("read spool dir")
            .count();
        assert_eq!(leftovers, 0, "spool files leaked");
        Ok(())
    }

    /// Corrupt compressed data must be a client error (400 via the error
    /// mapping) and must not leak spool files.
    #[nativelink_test]
    async fn spooled_ingest_rejects_corrupt_compressed_data() -> Result<(), Error> {
        let instance = test_instance();
        for codec in [
            NarCodec::Gzip,
            NarCodec::Xz,
            NarCodec::Zstd,
            NarCodec::Bzip2,
        ] {
            let stream = futures::stream::iter([Ok::<_, axum::Error>(Bytes::from_static(
                b"definitely not compressed data",
            ))]);
            let err = ingest_nar_spooled(&instance, codec, stream)
                .await
                .expect_err("corrupt data must fail");
            assert_eq!(err.code, Code::InvalidArgument, "for {codec:?}");
        }
        let leftovers = std::fs::read_dir(&instance.spool_dir)
            .expect("read spool dir")
            .count();
        assert_eq!(leftovers, 0, "spool files leaked");
        Ok(())
    }

    #[test]
    fn parse_range_accepts_single_ranges() {
        assert_eq!(
            parse_range(Some("bytes=0-99"), 1000),
            RangeRequest::Range { start: 0, end: 99 }
        );
        assert_eq!(
            parse_range(Some("bytes=500-"), 1000),
            RangeRequest::Range {
                start: 500,
                end: 999
            }
        );
        // End is clamped to the body.
        assert_eq!(
            parse_range(Some("bytes=500-100000"), 1000),
            RangeRequest::Range {
                start: 500,
                end: 999
            }
        );
        assert_eq!(
            parse_range(Some("bytes=999-999"), 1000),
            RangeRequest::Range {
                start: 999,
                end: 999
            }
        );
    }

    #[test]
    fn parse_range_ignores_invalid_and_multi_ranges() {
        for header in [
            "bytes=0-99,200-299", // multi-range
            "bytes=-500",         // suffix range
            "bytes=abc-",         // non-numeric start
            "bytes=0-abc",        // non-numeric end
            "bytes=5-4",          // end < start
            "items=0-99",         // unknown unit
            "bytes=",             // empty spec
        ] {
            assert_eq!(
                parse_range(Some(header), 1000),
                RangeRequest::Full,
                "for '{header}'"
            );
        }
        assert_eq!(parse_range(None, 1000), RangeRequest::Full);
    }

    #[test]
    fn parse_range_unsatisfiable_when_start_at_or_past_end() {
        assert_eq!(
            parse_range(Some("bytes=1000-"), 1000),
            RangeRequest::Unsatisfiable
        );
        assert_eq!(
            parse_range(Some("bytes=5000-6000"), 1000),
            RangeRequest::Unsatisfiable
        );
        // Zero-length body: any start is past the end.
        assert_eq!(
            parse_range(Some("bytes=0-"), 0),
            RangeRequest::Unsatisfiable
        );
    }

    #[test]
    fn error_response_maps_codes_to_statuses() {
        assert_eq!(
            error_response("test", &make_err!(Code::NotFound, "missing")).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            error_response("test", &make_input_err!("bad input")).status(),
            StatusCode::BAD_REQUEST
        );
        // Backend failure must never map to 404 — and absent must never map
        // to a 5xx.
        assert_eq!(
            error_response("test", &make_err!(Code::Internal, "backend broke")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            error_response("test", &make_err!(Code::Unavailable, "backend down")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn narinfo_name_hash_validates_shape() {
        assert_eq!(
            narinfo_name_hash("p4pclmv1gyja5kzc26npqpia1qqxrf0l.narinfo"),
            Some("p4pclmv1gyja5kzc26npqpia1qqxrf0l")
        );
        // Wrong extension, wrong length, and non-nix32 characters.
        assert_eq!(
            narinfo_name_hash("p4pclmv1gyja5kzc26npqpia1qqxrf0l.nar"),
            None
        );
        assert_eq!(
            narinfo_name_hash("p4pclmv1gyja5kzc26npqpia1qqxrf0.narinfo"),
            None
        );
        assert_eq!(
            narinfo_name_hash("e4pclmv1gyja5kzc26npqpia1qqxrf0l.narinfo"),
            None
        );
        assert_eq!(narinfo_name_hash("nix-cache-info"), None);
        assert_eq!(narinfo_name_hash(".narinfo"), None);
    }

    #[test]
    fn nar_upload_names_must_be_hash_shaped() {
        const HELLO_NIX32: &str = "094qif9n4cq4fdg459qzbhg1c6wywawwaaivx0k0x8xhbyx4vwic";
        for good in [
            format!("{HELLO_NIX32}.nar"),
            format!("{HELLO_NIX32}.nar.xz"),
            format!("{HELLO_NIX32}.nar.zst"),
            format!("{HELLO_NIX32}.nar.bz2"),
            format!("{HELLO_NIX32}-42.nar"),
        ] {
            assert!(is_valid_nar_upload_name(&good), "for '{good}'");
        }
        for bad in [
            "../../escape.nar".to_string(),
            "random.nar".to_string(),
            format!("{}.nar", "z".repeat(53)),
            format!("{}.nar", "z".repeat(31)),
            // 'e' is outside the nix32 alphabet.
            format!("e{}.nar", &HELLO_NIX32[1..]),
            format!("{HELLO_NIX32}-.nar"),
            format!("{HELLO_NIX32}-4x2.nar"),
            format!("{HELLO_NIX32}.tar"),
            format!("{HELLO_NIX32}.nar.gz"),
            String::new(),
        ] {
            assert!(!is_valid_nar_upload_name(&bad), "for '{bad}'");
        }
    }

    #[test]
    fn bare_nar_digest_requires_hash_shaped_name() {
        // sha256("hello") in nix32 (52 chars).
        const HELLO_NIX32: &str = "094qif9n4cq4fdg459qzbhg1c6wywawwaaivx0k0x8xhbyx4vwic";
        const HELLO_HEX: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let hello_hash: [u8; 32] = hex::decode(HELLO_HEX)
            .expect("valid hex")
            .try_into()
            .expect("32 bytes");

        // 52-char file-hash name + Content-Length.
        assert_eq!(
            bare_nar_digest_from_name(&format!("{HELLO_NIX32}.nar"), Some(123)),
            Some((hello_hash, 123))
        );
        // Same name without Content-Length: digest unknown, spool path.
        assert_eq!(
            bare_nar_digest_from_name(&format!("{HELLO_NIX32}.nar"), None),
            None
        );
        // Canonical names need no Content-Length.
        assert_eq!(
            bare_nar_digest_from_name(&format!("{HELLO_NIX32}-42.nar"), None),
            Some((hello_hash, 42))
        );
        // Non-hash-shaped names never resolve.
        assert_eq!(bare_nar_digest_from_name("random.nar", Some(123)), None);
        assert_eq!(
            bare_nar_digest_from_name(&format!("{HELLO_NIX32}.nar.xz"), Some(123)),
            None
        );
    }
}
