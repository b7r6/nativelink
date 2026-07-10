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

//! On-the-fly certificate authority for the MITM fetch proxy.
//!
//! To cache HTTPS fetches the proxy must terminate TLS toward the client: it
//! answers `CONNECT host:443` and then presents a certificate for `host` that
//! the client trusts. That trust comes from a single locally-generated CA the
//! client is told to trust (nix reads it via `NIX_SSL_CERT_FILE`). This module
//! owns that CA and mints — and caches — a leaf certificate per host on demand.
//!
//! Security note: the CA private key can impersonate ANY host to a client that
//! trusts it. It never leaves the machine, is written `0600`, and should only
//! be trusted by the build clients that use the proxy. Integrity of fetched
//! content does not rest on the proxy: nix verifies every fixed-output
//! derivation against its declared hash regardless of what the proxy serves.

use core::future::Future;
use core::pin::Pin;
use core::task::{Context as TaskContext, Poll};
use core::time::Duration;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex as StdMutex};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use nativelink_config::cas_server::HttpCacheProxyConfig;
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_nix::nixbase32;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::DropCloserReadHalf;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::store_trait::{
    Store, StoreKey, StoreLike, UploadSizeInfo, slow_update_store_with_file,
};
use nativelink_util::task::JoinHandleDropGuard;
use opentelemetry::context::FutureExt;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::crypto::ring::default_provider as ring_default_provider;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Subject common name of the generated CA. Fixed so a CA loaded from a
/// persisted key rebuilds an issuer whose subject matches the persisted
/// certificate the clients trust.
const CA_COMMON_NAME: &str = "NativeLink Fetch Proxy CA";

/// A locally-trusted certificate authority that mints per-host leaf
/// certificates for TLS interception, caching one `rustls` server config per
/// host.
pub struct CertAuthority {
    /// The CA parameters + signing key, used to sign every leaf.
    issuer: Issuer<'static, KeyPair>,
    /// The CA certificate in PEM, handed to clients (`NIX_SSL_CERT_FILE`).
    ca_cert_pem: String,
    /// Crypto provider used to build every per-host server config.
    provider: Arc<CryptoProvider>,
    /// host -> ready-to-use TLS server config (single leaf cert for that host).
    host_cache: StdMutex<HashMap<String, Arc<RustlsServerConfig>>>,
}

impl core::fmt::Debug for CertAuthority {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CertAuthority")
            .field("ca_cert_pem_len", &self.ca_cert_pem.len())
            .finish_non_exhaustive()
    }
}

/// The reconstructable CA parameters: a fixed subject and CA basic
/// constraints. Kept deterministic so a reload from the persisted key yields
/// an issuer whose subject/key-identifier match the persisted certificate.
fn ca_params() -> Result<CertificateParams, Error> {
    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|e| make_err!(Code::Internal, "Building CA params: {e}"))?;
    params
        .distinguished_name
        .push(DnType::CommonName, CA_COMMON_NAME);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    Ok(params)
}

impl CertAuthority {
    /// Loads the CA from `key_file`/`cert_file` if both exist, otherwise
    /// generates a fresh CA and persists it (key `0600`). The certificate is
    /// what clients must trust; the key must stay private.
    pub fn load_or_generate(cert_file: &str, key_file: &str) -> Result<Self, Error> {
        let provider = Arc::new(ring_default_provider());
        let (ca_key, ca_cert_pem) = if Path::new(key_file).exists() && Path::new(cert_file).exists()
        {
            let key_pem = std::fs::read_to_string(key_file)
                .err_tip(|| format!("Reading fetch-proxy CA key '{key_file}'"))?;
            let ca_key = KeyPair::from_pem(&key_pem)
                .map_err(|e| make_err!(Code::InvalidArgument, "Parsing fetch-proxy CA key: {e}"))?;
            let ca_cert_pem = std::fs::read_to_string(cert_file)
                .err_tip(|| format!("Reading fetch-proxy CA cert '{cert_file}'"))?;
            (ca_key, ca_cert_pem)
        } else {
            let ca_key = KeyPair::generate()
                .map_err(|e| make_err!(Code::Internal, "Generating fetch-proxy CA key: {e}"))?;
            let ca_cert = ca_params()?
                .self_signed(&ca_key)
                .map_err(|e| make_err!(Code::Internal, "Self-signing fetch-proxy CA: {e}"))?;
            let ca_cert_pem = ca_cert.pem();
            persist_ca(cert_file, key_file, &ca_cert_pem, &ca_key.serialize_pem())?;
            (ca_key, ca_cert_pem)
        };

        Ok(Self {
            issuer: Issuer::new(ca_params()?, ca_key),
            ca_cert_pem,
            provider,
            host_cache: StdMutex::new(HashMap::new()),
        })
    }

    /// The CA certificate in PEM form — what a client trusts to accept the
    /// intercepted connections (e.g. via `NIX_SSL_CERT_FILE`).
    #[must_use]
    pub fn ca_cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }

    /// Returns a `rustls` server config presenting a leaf certificate for
    /// `host`, minting and caching it on first use. The leaf chain is just the
    /// leaf itself: the client completes it with its trusted copy of the CA.
    pub fn server_config_for_host(&self, host: &str) -> Result<Arc<RustlsServerConfig>, Error> {
        if let Some(cfg) = self
            .host_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(host)
        {
            return Ok(Arc::clone(cfg));
        }
        let cfg = Arc::new(self.mint_host_config(host)?);
        self.host_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(host.to_string(), Arc::clone(&cfg));
        Ok(cfg)
    }

    fn mint_host_config(&self, host: &str) -> Result<RustlsServerConfig, Error> {
        let leaf_key = KeyPair::generate()
            .map_err(|e| make_err!(Code::Internal, "Generating leaf key for '{host}': {e}"))?;
        let mut params = CertificateParams::new(vec![host.to_string()])
            .map_err(|e| make_err!(Code::Internal, "Building leaf params for '{host}': {e}"))?;
        params.distinguished_name.push(DnType::CommonName, host);
        params.use_authority_key_identifier_extension = true;
        let leaf_cert = params
            .signed_by(&leaf_key, &self.issuer)
            .map_err(|e| make_err!(Code::Internal, "Signing leaf for '{host}': {e}"))?;

        let leaf_der = CertificateDer::from(leaf_cert.der().to_vec());
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der()));

        let mut config = RustlsServerConfig::builder_with_provider(Arc::clone(&self.provider))
            .with_safe_default_protocol_versions()
            .map_err(|e| make_err!(Code::Internal, "rustls protocol versions: {e}"))?
            .with_no_client_auth()
            .with_single_cert(vec![leaf_der], key_der)
            .map_err(|e| {
                make_err!(
                    Code::Internal,
                    "Building leaf server config for '{host}': {e}"
                )
            })?;
        // Serve HTTP/1.1 over the intercepted connection.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(config)
    }
}

/// Writes the CA certificate and key, creating the parent directory and
/// restricting the key to `0600` on Unix.
fn persist_ca(cert_file: &str, key_file: &str, cert_pem: &str, key_pem: &str) -> Result<(), Error> {
    for file in [cert_file, key_file] {
        if let Some(parent) = Path::new(file).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .err_tip(|| format!("Creating parent dir for '{file}'"))?;
        }
    }
    std::fs::write(cert_file, cert_pem)
        .err_tip(|| format!("Writing fetch-proxy CA cert '{cert_file}'"))?;
    std::fs::write(key_file, key_pem)
        .err_tip(|| format!("Writing fetch-proxy CA key '{key_file}'"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(key_file, std::fs::Permissions::from_mode(0o600))
            .err_tip(|| format!("Restricting fetch-proxy CA key perms '{key_file}'"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The caching MITM forward proxy
// ---------------------------------------------------------------------------

/// The response body type used throughout the proxy: a boxed body of `Bytes`
/// with `io::Error` framing errors, so streamed-from-CAS, streamed-from-origin,
/// and fixed bodies share one type.
type ProxyBody = BoxBody<Bytes, std::io::Error>;

/// Runs a store future under the SHA256 hasher context so every CAS key is
/// interpreted as `sha256` — bodies are content-addressed by `sha256(body)`.
async fn with_sha256_ctx<F, T>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>> + Send,
{
    let ctx = make_ctx_for_hash_func(DigestHasherFunc::Sha256)
        .err_tip(|| "Making SHA256 hasher context in FetchProxy")?;
    fut.with_context(ctx).await
}

/// The alias-store key for a URL: `fetch:<nixbase32(sha256(url))>` — slash-free
/// by construction so it is safe in a filesystem-backed store.
fn url_alias_key(url: &str) -> String {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(url.as_bytes());
    let digest = hasher.finalize_digest();
    let hash: &[u8; 32] = digest.packed_hash();
    format!("fetch:{}", nixbase32::encode(hash))
}

/// Serializes the alias record: `<nixbase32(sha256(body))>-<size>\n<content-type>`.
fn format_alias_record(digest: DigestInfo, content_type: &str) -> String {
    let hash: &[u8; 32] = digest.packed_hash();
    format!(
        "{}-{}\n{content_type}",
        nixbase32::encode(hash),
        digest.size_bytes()
    )
}

/// Parses [`format_alias_record`] back into the body digest and content type.
fn parse_alias_record(raw: &str) -> Option<(DigestInfo, String)> {
    let (first, content_type) = raw.split_once('\n').unwrap_or((raw, ""));
    let (hash_b32, size_str) = first.rsplit_once('-')?;
    let hash = nixbase32::decode(hash_b32).ok()?;
    let hash: [u8; 32] = hash.as_slice().try_into().ok()?;
    let size = size_str.parse::<u64>().ok()?;
    Some((DigestInfo::new(hash, size), content_type.to_string()))
}

/// Response headers that must never be copied verbatim when proxying: they
/// describe a specific hop's connection, not the payload.
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn status_body(status: StatusCode, message: &'static str) -> Response<ProxyBody> {
    let mut response = Response::new(full_body(Bytes::from_static(message.as_bytes())));
    *response.status_mut() = status;
    response
}

fn full_body(bytes: Bytes) -> ProxyBody {
    BodyExt::boxed(Full::new(bytes).map_err(|never| match never {}))
}

fn empty_body() -> ProxyBody {
    BodyExt::boxed(Empty::<Bytes>::new().map_err(|never| match never {}))
}

/// A response body that streams a CAS blob, holding the producer task alive for
/// the body's lifetime (dropping it on early client disconnect aborts the read).
struct CasBodyStream {
    rx: DropCloserReadHalf,
    _task: JoinHandleDropGuard<()>,
}

impl Stream for CasBodyStream {
    type Item = Result<Frame<Bytes>, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A caching, TLS-intercepting HTTP forward proxy that stores fetched bodies
/// content-addressed in a `NativeLink` CAS. See [`HttpCacheProxyConfig`].
#[derive(Debug)]
pub struct FetchProxy {
    ca: Arc<CertAuthority>,
    cas_store: Store,
    alias_store: Store,
    http_client: reqwest::Client,
    max_fetch_size_bytes: u64,
    fetch_timeout: Duration,
}

impl FetchProxy {
    /// Resolves the stores, loads or generates the CA, and builds the HTTP
    /// client. Fails fast on misconfiguration so a bad config never binds.
    pub fn new(
        config: &HttpCacheProxyConfig,
        store_manager: &StoreManager,
    ) -> Result<Arc<Self>, Error> {
        let cas_store = store_manager.get_store(&config.cas_store).ok_or_else(|| {
            make_err!(
                Code::InvalidArgument,
                "'cas_store': '{}' does not exist",
                config.cas_store
            )
        })?;
        let alias_store = store_manager
            .get_store(&config.alias_store)
            .ok_or_else(|| {
                make_err!(
                    Code::InvalidArgument,
                    "'alias_store': '{}' does not exist",
                    config.alias_store
                )
            })?;
        let ca = Arc::new(CertAuthority::load_or_generate(
            &config.ca_cert_file,
            &config.ca_key_file,
        )?);
        let http_client = reqwest::Client::builder()
            .user_agent(concat!(
                "nativelink-fetch-proxy/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .map_err(|e| make_err!(Code::Internal, "Building fetch-proxy HTTP client: {e}"))?;
        Ok(Arc::new(Self {
            ca,
            cas_store,
            alias_store,
            http_client,
            max_fetch_size_bytes: config.max_fetch_size_bytes,
            fetch_timeout: Duration::from_secs(config.fetch_timeout_s),
        }))
    }

    /// The CA certificate PEM clients must trust (nix: `NIX_SSL_CERT_FILE`).
    #[must_use]
    pub fn ca_cert_pem(&self) -> &str {
        self.ca.ca_cert_pem()
    }

    /// Accepts proxy connections on `listener` until it errors fatally. Each
    /// connection is served on its own task.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let this = Arc::clone(&self);
                    drop(nativelink_util::background_spawn!(
                        "fetch_proxy_conn",
                        async move {
                            if let Err(err) = this.handle_conn(stream).await {
                                debug!(?err, "fetch proxy connection ended");
                            }
                        }
                    ));
                }
                Err(err) => {
                    error!(?err, "fetch proxy accept failed");
                }
            }
        }
    }

    /// Serves one plaintext client connection with HTTP/1 (with upgrades), so a
    /// `CONNECT` can be hijacked for TLS interception and an absolute-form
    /// request can be proxied directly.
    async fn handle_conn(self: Arc<Self>, stream: TcpStream) -> Result<(), Error> {
        let io = TokioIo::new(stream);
        let this = Arc::clone(&self);
        let service = service_fn(move |req: Request<Incoming>| {
            let this = Arc::clone(&this);
            async move { Ok::<_, core::convert::Infallible>(this.route(req).await) }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(io, service)
            .with_upgrades()
            .await
            .map_err(|e| make_err!(Code::Internal, "fetch proxy connection: {e}"))
    }

    /// Routes one client request: `CONNECT` starts a TLS-intercepted tunnel;
    /// anything else is a plaintext (absolute-form) proxy request.
    async fn route(self: Arc<Self>, req: Request<Incoming>) -> Response<ProxyBody> {
        if req.method() == Method::CONNECT {
            return self.handle_connect(req);
        }
        let url = req.uri().to_string();
        self.serve_url(req, url).await
    }

    /// Answers a `CONNECT`: mints a certificate for the target host, returns
    /// `200`, and (after the upgrade) accepts TLS and serves the intercepted
    /// HTTP requests.
    fn handle_connect(self: Arc<Self>, req: Request<Incoming>) -> Response<ProxyBody> {
        let Some(authority) = req.uri().authority().cloned() else {
            return status_body(StatusCode::BAD_REQUEST, "CONNECT requires an authority");
        };
        let host = authority.host().to_string();
        let tls_config = match self.ca.server_config_for_host(&host) {
            Ok(config) => config,
            Err(err) => {
                error!(?err, %host, "failed to mint interception certificate");
                return status_body(StatusCode::BAD_GATEWAY, "certificate error");
            }
        };
        let this = Arc::clone(&self);
        drop(nativelink_util::background_spawn!(
            "fetch_proxy_tunnel",
            async move {
                match hyper::upgrade::on(req).await {
                    Ok(upgraded) => {
                        if let Err(err) = this.serve_tunnel(&host, tls_config, upgraded).await {
                            debug!(?err, %host, "fetch proxy tunnel ended");
                        }
                    }
                    Err(err) => warn!(?err, %host, "CONNECT upgrade failed"),
                }
            }
        ));
        Response::new(empty_body())
    }

    /// Accepts the intercepted TLS connection and serves its HTTP/1 requests,
    /// reconstructing the absolute `https://host/...` URL for each.
    async fn serve_tunnel(
        self: Arc<Self>,
        host: &str,
        tls_config: Arc<RustlsServerConfig>,
        upgraded: hyper::upgrade::Upgraded,
    ) -> Result<(), Error> {
        let tls_stream = TlsAcceptor::from(tls_config)
            .accept(TokioIo::new(upgraded))
            .await
            .map_err(|e| make_err!(Code::Internal, "TLS accept for '{host}': {e}"))?;
        let io = TokioIo::new(tls_stream);
        let host_owned = host.to_string();
        let this = Arc::clone(&self);
        let service = service_fn(move |req: Request<Incoming>| {
            let this = Arc::clone(&this);
            let host = host_owned.clone();
            async move {
                let path = req
                    .uri()
                    .path_and_query()
                    .map_or_else(|| req.uri().path(), |pq| pq.as_str());
                let url = format!("https://{host}{path}");
                Ok::<_, core::convert::Infallible>(this.serve_url(req, url).await)
            }
        });
        hyper::server::conn::http1::Builder::new()
            .serve_connection(io, service)
            .await
            .map_err(|e| make_err!(Code::Internal, "intercepted connection for '{host}': {e}"))
    }

    /// The cache logic for one absolute `url`. `GET` is served from the CAS on a
    /// hit and fetched-then-cached on a miss; other methods are proxied through
    /// uncached.
    async fn serve_url(&self, req: Request<Incoming>, url: String) -> Response<ProxyBody> {
        if req.method() != Method::GET {
            return self
                .passthrough(req, &url)
                .await
                .unwrap_or_else(|err| proxy_error(&url, &err));
        }
        match self.lookup_cached(&url).await {
            Ok(Some(response)) => return response,
            Ok(None) => {}
            Err(err) => warn!(?err, %url, "fetch-proxy cache lookup failed; refetching"),
        }
        self.fetch_and_cache(&url)
            .await
            .unwrap_or_else(|err| proxy_error(&url, &err))
    }

    /// Returns a response served from the CAS if `url` is cached and its body
    /// blob is still present, else `None`.
    async fn lookup_cached(&self, url: &str) -> Result<Option<Response<ProxyBody>>, Error> {
        let key = url_alias_key(url);
        let raw = match with_sha256_ctx(self.alias_store.get_part_unchunked(
            StoreKey::new_str(&key),
            0,
            None,
        ))
        .await
        {
            Ok(raw) => raw,
            Err(err) if err.code == Code::NotFound => return Ok(None),
            Err(err) => return Err(err),
        };
        let text = core::str::from_utf8(&raw)
            .map_err(|e| make_err!(Code::Internal, "corrupt fetch-proxy alias for '{url}': {e}"))?;
        let Some((digest, content_type)) = parse_alias_record(text) else {
            return Ok(None);
        };
        if with_sha256_ctx(self.cas_store.has(digest))
            .await
            .err_tip(|| "checking cached body existence")?
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some(self.serve_from_cas(digest, &content_type)))
    }

    /// Builds a `200` response streaming `digest` from the CAS.
    fn serve_from_cas(&self, digest: DigestInfo, content_type: &str) -> Response<ProxyBody> {
        let (tx, rx) = nativelink_util::buf_channel::make_buf_channel_pair();
        let cas_store = self.cas_store.clone();
        let ctx = make_ctx_for_hash_func(DigestHasherFunc::Sha256).ok();
        let task = nativelink_util::spawn!("fetch_proxy_cas_read", async move {
            let fut = cas_store.get_part(digest, tx, 0, None);
            let result = match ctx {
                Some(ctx) => fut.with_context(ctx).await,
                None => fut.await,
            };
            if let Err(err) = result {
                warn!(?err, ?digest, "failed streaming cached body from CAS");
            }
        });
        let body = BodyExt::boxed(StreamBody::new(CasBodyStream { rx, _task: task }));
        let mut response = Response::new(body);
        response
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValueOf(digest.size_bytes()).into());
        if !content_type.is_empty()
            && let Ok(value) = content_type.parse()
        {
            response.headers_mut().insert(CONTENT_TYPE, value);
        }
        response
    }

    /// Fetches `url` from the origin. A cacheable response (a `200` with a
    /// known `Content-Length` within the size cap) is spooled into the CAS and
    /// then served from it; anything else is streamed straight through.
    async fn fetch_and_cache(&self, url: &str) -> Result<Response<ProxyBody>, Error> {
        let resp = self
            .http_client
            .get(url)
            .timeout(self.fetch_timeout)
            .send()
            .await
            .map_err(|e| make_err!(Code::Unavailable, "fetching '{url}': {e}"))?;
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let content_length = resp
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());

        let cacheable = status == StatusCode::OK
            && content_length.is_some_and(|len| len <= self.max_fetch_size_bytes);
        if !cacheable {
            return stream_reqwest_response(status, resp);
        }

        let digest = self.spool_into_cas(resp, url).await?;
        let key = url_alias_key(url);
        with_sha256_ctx(self.alias_store.update_oneshot(
            StoreKey::new_str(&key),
            Bytes::from(format_alias_record(digest, &content_type)),
        ))
        .await
        .err_tip(|| "storing fetch-proxy alias record")?;
        debug!(%url, ?digest, "fetch-proxy cached body");
        Ok(self.serve_from_cas(digest, &content_type))
    }

    /// Streams the origin body to a spool file while hashing, then uploads it to
    /// the CAS under `DigestInfo(sha256(body), size)` and returns that digest.
    async fn spool_into_cas(
        &self,
        resp: reqwest::Response,
        url: &str,
    ) -> Result<DigestInfo, Error> {
        let spool_path =
            std::env::temp_dir().join(format!("nl-fetch-proxy-{}.bin", Uuid::new_v4()));
        let guard = SpoolGuard(Some(spool_path.clone()));
        let mut file = nativelink_util::fs::create_file(&spool_path)
            .await
            .err_tip(|| "creating fetch-proxy spool file")?;
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        let mut stream = resp.bytes_stream();
        let mut written: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                make_err!(Code::Unavailable, "reading origin body for '{url}': {e}")
            })?;
            written = written.saturating_add(chunk.len() as u64);
            if written > self.max_fetch_size_bytes {
                return Err(make_err!(
                    Code::ResourceExhausted,
                    "body for '{url}' exceeds the {}-byte cache limit",
                    self.max_fetch_size_bytes
                ));
            }
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .err_tip(|| "writing fetch-proxy spool file")?;
        }
        file.flush().await.err_tip(|| "flushing spool file")?;
        let digest = hasher.finalize_digest();
        with_sha256_ctx(slow_update_store_with_file(
            self.cas_store.as_store_driver_pin(),
            digest,
            &mut file,
            UploadSizeInfo::ExactSize(digest.size_bytes()),
        ))
        .await
        .err_tip(|| "uploading fetched body to CAS")?;
        drop(file);
        guard.cleanup().await;
        Ok(digest)
    }

    /// Proxies a non-GET request straight through to the origin without
    /// caching, forwarding the method, safe headers, and request body.
    async fn passthrough(
        &self,
        req: Request<Incoming>,
        url: &str,
    ) -> Result<Response<ProxyBody>, Error> {
        let method = Method::from_bytes(req.method().as_str().as_bytes())
            .map_err(|e| make_err!(Code::InvalidArgument, "bad method: {e}"))?;
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in req.headers() {
            if !is_hop_by_hop(name.as_str())
                && let Ok(rname) = reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes())
                && let Ok(rvalue) = reqwest::header::HeaderValue::from_bytes(value.as_bytes())
            {
                headers.insert(rname, rvalue);
            }
        }
        let body_stream = req
            .into_body()
            .into_data_stream()
            .map(|result| result.map_err(std::io::Error::other));
        let resp = self
            .http_client
            .request(method, url)
            .timeout(self.fetch_timeout)
            .headers(headers)
            .body(reqwest::Body::wrap_stream(body_stream))
            .send()
            .await
            .map_err(|e| make_err!(Code::Unavailable, "proxying '{url}': {e}"))?;
        stream_reqwest_response(resp.status(), resp)
    }
}

/// Wraps a `reqwest` response as a streamed proxy response, copying the status
/// and all non-hop-by-hop headers.
fn stream_reqwest_response(
    status: StatusCode,
    resp: reqwest::Response,
) -> Result<Response<ProxyBody>, Error> {
    let mut builder = Response::builder().status(status.as_u16());
    for (name, value) in resp.headers() {
        if !is_hop_by_hop(name.as_str()) {
            builder = builder.header(name.as_str(), value.as_bytes());
        }
    }
    let body = BodyExt::boxed(StreamBody::new(
        resp.bytes_stream()
            .map(|result| result.map(Frame::data).map_err(std::io::Error::other)),
    ));
    builder
        .body(body)
        .map_err(|e| make_err!(Code::Internal, "building proxy response: {e}"))
}

/// Maps a proxy-side failure to a `502` (the origin/cache is unreachable),
/// keeping the detail in the log rather than the response body.
fn proxy_error(url: &str, err: &Error) -> Response<ProxyBody> {
    warn!(?err, %url, "fetch proxy request failed");
    status_body(StatusCode::BAD_GATEWAY, "fetch proxy error")
}

/// Small helper to render a `Content-Length` header value from a `u64`.
struct HeaderValueOf(u64);
impl From<HeaderValueOf> for hyper::header::HeaderValue {
    fn from(value: HeaderValueOf) -> Self {
        Self::from(value.0)
    }
}

/// Removes a spool file on drop unless disarmed via [`SpoolGuard::cleanup`].
struct SpoolGuard(Option<std::path::PathBuf>);
impl SpoolGuard {
    async fn cleanup(mut self) {
        if let Some(path) = self.0.take() {
            drop(tokio::fs::remove_file(path).await);
        }
    }
}
impl Drop for SpoolGuard {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            drop(std::fs::remove_file(path));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_paths(tag: &str) -> (String, String) {
        let dir =
            std::env::temp_dir().join(format!("nl-fetch-proxy-ca-{}-{tag}", std::process::id()));
        (
            dir.join("ca.crt").display().to_string(),
            dir.join("ca.key").display().to_string(),
        )
    }

    #[test]
    fn generates_persists_and_reloads() {
        let (cert, key) = temp_paths("reload");
        drop(std::fs::remove_dir_all(Path::new(&cert).parent().unwrap()));

        let ca = CertAuthority::load_or_generate(&cert, &key).expect("generate");
        assert!(ca.ca_cert_pem().contains("BEGIN CERTIFICATE"));
        assert!(Path::new(&cert).exists());
        assert!(Path::new(&key).exists());
        let pem1 = ca.ca_cert_pem().to_string();

        // Reload reuses the persisted cert verbatim.
        let ca2 = CertAuthority::load_or_generate(&cert, &key).expect("reload");
        assert_eq!(ca2.ca_cert_pem(), pem1);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&key).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "CA key must be 0600");
        }
    }

    #[test]
    fn mints_and_caches_per_host_leaf_configs() {
        let (cert, key) = temp_paths("mint");
        drop(std::fs::remove_dir_all(Path::new(&cert).parent().unwrap()));
        let ca = CertAuthority::load_or_generate(&cert, &key).expect("generate");

        let a1 = ca.server_config_for_host("example.com").expect("mint a");
        let a2 = ca.server_config_for_host("example.com").expect("cached a");
        assert!(
            Arc::ptr_eq(&a1, &a2),
            "same host must return the cached config"
        );

        let b = ca
            .server_config_for_host("cache.nixos.org")
            .expect("mint b");
        assert!(
            !Arc::ptr_eq(&a1, &b),
            "different hosts get different configs"
        );
        assert_eq!(a1.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }
}
