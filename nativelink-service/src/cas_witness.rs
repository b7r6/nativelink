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

//! On-the-fly certificate authority for the MITM CAS witness.
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

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{CONTENT_LENGTH, CONTENT_TYPE};
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use nativelink_config::cas_server::CasWitnessConfig;
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
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::crypto::ring::default_provider as ring_default_provider;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::witness::{self, WitnessKey};

/// Subject common name of the generated CA. Fixed so a CA loaded from a
/// persisted key rebuilds an issuer whose subject matches the persisted
/// certificate the clients trust.
const CA_COMMON_NAME: &str = "NativeLink CAS Witness CA";

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
                .err_tip(|| format!("Reading cas-witness CA key '{key_file}'"))?;
            let ca_key = KeyPair::from_pem(&key_pem)
                .map_err(|e| make_err!(Code::InvalidArgument, "Parsing cas-witness CA key: {e}"))?;
            let ca_cert_pem = std::fs::read_to_string(cert_file)
                .err_tip(|| format!("Reading cas-witness CA cert '{cert_file}'"))?;
            (ca_key, ca_cert_pem)
        } else {
            let ca_key = KeyPair::generate()
                .map_err(|e| make_err!(Code::Internal, "Generating cas-witness CA key: {e}"))?;
            let ca_cert = ca_params()?
                .self_signed(&ca_key)
                .map_err(|e| make_err!(Code::Internal, "Self-signing cas-witness CA: {e}"))?;
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

    /// Max hostname length (the DNS limit) accepted for leaf minting.
    const MAX_HOST_LEN: usize = 253;
    /// Cap on distinct cached host TLS configs. Legitimate upstreams are few;
    /// a larger population signals abuse, so the cache is cleared rather than
    /// grown without bound.
    const MAX_HOST_CACHE_ENTRIES: usize = 4096;

    /// Returns a `rustls` server config presenting a leaf certificate for
    /// `host`, minting and caching it on first use. The leaf chain is just the
    /// leaf itself: the client completes it with its trusted copy of the CA.
    pub fn server_config_for_host(&self, host: &str) -> Result<Arc<RustlsServerConfig>, Error> {
        // Validate before minting: a client opening many CONNECT tunnels to
        // distinct or garbage authorities must not drive unbounded leaf-key
        // generation (CPU) or feed absurd strings into the certificate SAN/CN.
        if host.is_empty()
            || host.len() > Self::MAX_HOST_LEN
            || !host.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(make_err!(
                Code::InvalidArgument,
                "refusing to mint a leaf certificate for invalid host '{host}'"
            ));
        }
        {
            let cache = self
                .host_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(cfg) = cache.get(host) {
                return Ok(Arc::clone(cfg));
            }
        }
        let cfg = Arc::new(self.mint_host_config(host)?);
        let mut cache = self
            .host_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Bound memory: a distinct-host population this large is abuse, not a
        // real upstream set — drop the cache rather than grow it unboundedly.
        if cache.len() >= Self::MAX_HOST_CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(host.to_string(), Arc::clone(&cfg));
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
        .err_tip(|| format!("Writing cas-witness CA cert '{cert_file}'"))?;
    std::fs::write(key_file, key_pem)
        .err_tip(|| format!("Writing cas-witness CA key '{key_file}'"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(key_file, std::fs::Permissions::from_mode(0o600))
            .err_tip(|| format!("Restricting cas-witness CA key perms '{key_file}'"))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The caching MITM CAS witness
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
        .err_tip(|| "Making SHA256 hasher context in CasWitness")?;
    fut.with_context(ctx).await
}

/// Runs a store future under the BLAKE3 hasher context so every CAS key is
/// interpreted as `blake3` — attestations are content-addressed by
/// `blake3(dsse)`.
async fn with_blake3_ctx<F, T>(fut: F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>> + Send,
{
    let ctx = make_ctx_for_hash_func(DigestHasherFunc::Blake3)
        .err_tip(|| "Making BLAKE3 hasher context in CasWitness")?;
    fut.with_context(ctx).await
}

/// A reference to a stored attestation in the CAS: the BLAKE3 hash (hex) and
/// size of the DSSE envelope, so a cache hit can return the witness headers
/// without re-fetching the attestation.
struct AttestationRef {
    blake3_hex: String,
    size: u64,
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

/// Serializes the alias record:
/// `<nixbase32(sha256(body))>-<size>\n<content-type>` or, when witnessing is
/// enabled, `<nixbase32(sha256(body))>-<size>\n<content-type>\n<blake3-hex>-<attestation-size>`.
fn format_alias_record(
    digest: DigestInfo,
    content_type: &str,
    attestation: Option<&AttestationRef>,
) -> String {
    let hash: &[u8; 32] = digest.packed_hash();
    let base = format!(
        "{}-{}\n{content_type}",
        nixbase32::encode(hash),
        digest.size_bytes()
    );
    match attestation {
        Some(a) => format!("{base}\n{}-{}", a.blake3_hex, a.size),
        None => base,
    }
}

/// Parses [`format_alias_record`] back into the body digest, content type,
/// and optional attestation reference.
fn parse_alias_record(raw: &str) -> Option<(DigestInfo, String, Option<AttestationRef>)> {
    let mut lines = raw.lines();
    let first = lines.next()?;
    let content_type = lines.next().unwrap_or("");
    let (hash_b32, size_str) = first.rsplit_once('-')?;
    let hash = nixbase32::decode(hash_b32).ok()?;
    let hash: [u8; 32] = hash.as_slice().try_into().ok()?;
    let size = size_str.parse::<u64>().ok()?;
    let attestation = lines.next().and_then(|att_line| {
        let (blake3_hex, att_size_str) = att_line.rsplit_once('-')?;
        let att_size = att_size_str.parse::<u64>().ok()?;
        Some(AttestationRef {
            blake3_hex: blake3_hex.to_string(),
            size: att_size,
        })
    });
    Some((
        DigestInfo::new(hash, size),
        content_type.to_string(),
        attestation,
    ))
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

/// A response body that streams from a spool file, deleting it when the
/// stream is dropped (including on early client disconnect).
struct FileBodyStream {
    rx: DropCloserReadHalf,
    _task: JoinHandleDropGuard<()>,
    _guard: SpoolGuard,
}

impl Stream for FileBodyStream {
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

/// The CAS witness: a caching, TLS-intercepting HTTP forward proxy that stores
/// fetched bodies content-addressed in a `NativeLink` CAS. See [`CasWitnessConfig`].
#[derive(Debug)]
pub struct CasWitness {
    ca: Arc<CertAuthority>,
    cas_store: Store,
    alias_store: Store,
    http_client: reqwest::Client,
    max_fetch_size_bytes: u64,
    fetch_timeout: Duration,
    witness_key: Option<Arc<WitnessKey>>,
}

impl CasWitness {
    /// Resolves the stores, loads or generates the CA, and builds the HTTP
    /// client. Fails fast on misconfiguration so a bad config never binds.
    pub fn new(
        config: &CasWitnessConfig,
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
        let witness_key = match &config.witness_key_file {
            Some(path) => Some(Arc::new(WitnessKey::load_or_generate(path)?)),
            None => None,
        };
        let http_client = reqwest::Client::builder()
            .user_agent(concat!(
                "nativelink-cas-witness/",
                env!("CARGO_PKG_VERSION")
            ))
            .tls_info(true)
            // Do NOT follow redirects. A followed 3xx would fetch a URL the
            // client never named (SSRF to internal hosts) and would bind the
            // cached body and its signed attestation to the ORIGINAL url even
            // though the bytes came from the redirect target. Surface the 3xx
            // to the client and let it decide whether to follow.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| make_err!(Code::Internal, "Building cas-witness HTTP client: {e}"))?;
        Ok(Arc::new(Self {
            ca,
            cas_store,
            alias_store,
            http_client,
            max_fetch_size_bytes: config.max_fetch_size_bytes,
            fetch_timeout: Duration::from_secs(config.fetch_timeout_s),
            witness_key,
        }))
    }

    /// The CA certificate PEM clients must trust (nix: `NIX_SSL_CERT_FILE`).
    #[must_use]
    pub fn ca_cert_pem(&self) -> &str {
        self.ca.ca_cert_pem()
    }

    /// The witness verifying key, if witnessing is enabled. Clients use this
    /// to verify `X-Straylight-Witness-Receipt` signatures and DSSE
    /// attestations fetched from the CAS.
    #[must_use]
    pub fn witness_verifying_key(&self) -> Option<ed25519_dalek::VerifyingKey> {
        self.witness_key.as_ref().map(|k| k.verifying_key())
    }

    /// Accepts proxy connections on `listener` until it errors fatally. Each
    /// connection is served on its own task.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, _peer)) => {
                    let this = Arc::clone(&self);
                    drop(nativelink_util::background_spawn!(
                        "cas_witness_conn",
                        async move {
                            if let Err(err) = this.handle_conn(stream).await {
                                debug!(?err, "CAS witness connection ended");
                            }
                        }
                    ));
                }
                Err(err) => {
                    error!(?err, "CAS witness accept failed");
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
            .map_err(|e| make_err!(Code::Internal, "CAS witness connection: {e}"))
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
            "cas_witness_tunnel",
            async move {
                match hyper::upgrade::on(req).await {
                    Ok(upgraded) => {
                        if let Err(err) = this.serve_tunnel(&host, tls_config, upgraded).await {
                            debug!(?err, %host, "CAS witness tunnel ended");
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
            Err(err) => warn!(?err, %url, "cas-witness cache lookup failed; refetching"),
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
            .map_err(|e| make_err!(Code::Internal, "corrupt cas-witness alias for '{url}': {e}"))?;
        let Some((digest, content_type, attestation)) = parse_alias_record(text) else {
            return Ok(None);
        };
        if with_sha256_ctx(self.cas_store.has(digest))
            .await
            .err_tip(|| "checking cached body existence")?
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some(self.serve_from_cas(
            digest,
            &content_type,
            url,
            attestation.as_ref(),
        )))
    }

    /// Builds a `200` response streaming `digest` from the CAS. When
    /// `attestation` is present and witnessing is enabled, adds
    /// `X-Straylight-Witness` and `X-Straylight-Witness-Receipt` headers.
    fn serve_from_cas(
        &self,
        digest: DigestInfo,
        content_type: &str,
        url: &str,
        attestation: Option<&AttestationRef>,
    ) -> Response<ProxyBody> {
        let (tx, rx) = nativelink_util::buf_channel::make_buf_channel_pair();
        let cas_store = self.cas_store.clone();
        let ctx = make_ctx_for_hash_func(DigestHasherFunc::Sha256).ok();
        let task = nativelink_util::spawn!("cas_witness_cas_read", async move {
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
        if let (Some(key), Some(att)) = (&self.witness_key, attestation) {
            add_witness_headers(&mut response, url, &digest, att, key);
        }
        response
    }

    /// Builds a `200` response streaming from a spool file. The file is deleted
    /// when the response body is dropped. No witness headers are added (the
    /// body exceeded the cache size limit and was not attested).
    fn serve_from_file(
        &self,
        file: nativelink_util::fs::FileSlot,
        digest: DigestInfo,
        content_type: &str,
        guard: SpoolGuard,
    ) -> Response<ProxyBody> {
        let (mut tx, rx) = nativelink_util::buf_channel::make_buf_channel_pair();
        let task = nativelink_util::spawn!("cas_witness_file_read", async move {
            let mut file = file;
            if let Err(e) = file.rewind().await {
                warn!(%e, "failed to rewind spool file for serving");
                return;
            }
            loop {
                let mut buf = BytesMut::with_capacity(nativelink_util::fs::DEFAULT_READ_BUFF_SIZE);
                match file.read_buf(&mut buf).await {
                    Ok(0) => break,
                    Ok(_) => {
                        if tx.send(buf.freeze()).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(%e, "failed to read spool file for serving");
                        break;
                    }
                }
            }
        });
        let body = BodyExt::boxed(StreamBody::new(FileBodyStream {
            rx,
            _task: task,
            _guard: guard,
        }));
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

    /// Fetches `url` from the origin. A `200` response is spooled to a temp
    /// file; if the actual body size is within the cache limit it is uploaded
    /// to the CAS and served from there (with an attestation when witnessing
    /// is enabled), otherwise it is served directly from the spool file. A
    /// response whose `Content-Length` is known to exceed the limit is
    /// streamed through without buffering. Non-200 responses are always
    /// streamed through.
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

        // Non-200 responses are always streamed through.
        if status != StatusCode::OK {
            return stream_reqwest_response(status, resp);
        }
        // If the size is known and exceeds the limit, stream through without
        // buffering — avoids spooling a body we already know we won't cache.
        if let Some(len) = content_length
            && len > self.max_fetch_size_bytes
        {
            return stream_reqwest_response(status, resp);
        }

        // Extract the origin TLS leaf certificate fingerprint (if any)
        // before consuming the response body.
        let server_cert_sha256 = resp
            .extensions()
            .get::<reqwest::tls::TlsInfo>()
            .and_then(|info| info.peer_certificate())
            .map(witness::sha256_hex);

        // Spool the entire body to a temp file (handles both Content-Length
        // and chunked responses). The fetch timeout bounds how long we'll
        // spool; the actual size is checked after the body is fully received.
        let (mut file, digest, guard) = self.spool_to_file(resp, url).await?;

        // If the actual size exceeds the limit, serve from the spool file
        // without caching or witnessing.
        if digest.size_bytes() > self.max_fetch_size_bytes {
            return Ok(self.serve_from_file(file, digest, &content_type, guard));
        }

        // Upload to CAS.
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

        // Create and store the attestation when witnessing is enabled.
        let attestation = match &self.witness_key {
            Some(key) => {
                let att = self
                    .create_and_store_attestation(
                        url,
                        &digest,
                        &content_type,
                        status.as_u16(),
                        server_cert_sha256.as_deref(),
                        key,
                    )
                    .await?;
                Some(att)
            }
            None => None,
        };

        let key = url_alias_key(url);
        with_sha256_ctx(self.alias_store.update_oneshot(
            StoreKey::new_str(&key),
            Bytes::from(format_alias_record(
                digest,
                &content_type,
                attestation.as_ref(),
            )),
        ))
        .await
        .err_tip(|| "storing cas-witness alias record")?;
        debug!(%url, ?digest, "cas-witness cached body");
        Ok(self.serve_from_cas(digest, &content_type, url, attestation.as_ref()))
    }

    /// Builds, signs, and stores a DSSE/in-toto attestation for a fetched
    /// body in the CAS under its BLAKE3 key, returning the reference.
    async fn create_and_store_attestation(
        &self,
        url: &str,
        body_digest: &DigestInfo,
        content_type: &str,
        status: u16,
        server_cert_sha256: Option<&str>,
        key: &WitnessKey,
    ) -> Result<AttestationRef, Error> {
        let body_sha256_hex = {
            let hash: &[u8; 32] = body_digest.packed_hash();
            hex::encode(hash)
        };
        let body_cas_key = format!("{}-{}", body_sha256_hex, body_digest.size_bytes());

        // Parse the host from the URL for the upstream metadata.
        let host = reqwest::Url::parse(url)
            .ok()
            .and_then(|u| u.host_str().map(ToString::to_string))
            .unwrap_or_default();

        // Select a few response headers for the attestation (we only have
        // content-type at this point since the response was consumed by
        // spool_into_cas; in the future we could capture more).
        let response_headers = if content_type.is_empty() {
            Vec::new()
        } else {
            vec![("content-type".to_string(), content_type.to_string())]
        };

        let timestamp = witness::now_rfc3339();
        let monotonic_ns = witness::monotonic_ns();
        let hostname = witness::hostname();

        // The attestation's OWN CAS key is BLAKE3(finalized DSSE envelope),
        // which cannot appear inside the signed predicate without a hash
        // cycle (the key would depend on the payload that contains it). Per
        // the in-toto model the storage location is not self-bound: the
        // attestation is content-addressed externally and its key is conveyed
        // out of band via the `X-Straylight-Witness` header and the signed
        // receipt. So `persistence.attestation_key` is left empty in the
        // signed statement; `body_key` (where the body lives) is the binding
        // that matters.
        let statement = witness::build_statement(
            url,
            "GET",
            &body_sha256_hex,
            body_digest.size_bytes(),
            if content_type.is_empty() {
                None
            } else {
                Some(content_type)
            },
            &host,
            None,
            status,
            server_cert_sha256,
            response_headers,
            "none",
            &body_cas_key,
            "", // attestation_key: left empty (self-referential; see comment above)
            &timestamp,
            monotonic_ns,
            &hostname,
        );

        let envelope = witness::create_attestation(statement, key)?;
        let envelope_bytes = witness::envelope_to_bytes(&envelope)?;
        let blake3_hex = witness::blake3_hex(&envelope_bytes);
        let att_size = envelope_bytes.len() as u64;

        // Store the DSSE envelope in the CAS under its BLAKE3 key.
        let att_hash = blake3::hash(&envelope_bytes);
        let att_digest = DigestInfo::new(*att_hash.as_bytes(), att_size);
        with_blake3_ctx(
            self.cas_store
                .update_oneshot(StoreKey::Digest(att_digest), Bytes::from(envelope_bytes)),
        )
        .await
        .err_tip(|| "storing attestation in CAS")?;

        debug!(%url, %blake3_hex, att_size, "cas-witness stored attestation");
        Ok(AttestationRef {
            blake3_hex,
            size: att_size,
        })
    }

    /// Streams the origin body to a spool file while hashing, returning the
    /// file, its SHA256 digest, and a guard that deletes the file on drop.
    /// Works for both `Content-Length` and chunked responses — the entire
    /// body is buffered to disk before the caller decides whether to cache.
    async fn spool_to_file(
        &self,
        resp: reqwest::Response,
        url: &str,
    ) -> Result<(nativelink_util::fs::FileSlot, DigestInfo, SpoolGuard), Error> {
        let spool_path =
            std::env::temp_dir().join(format!("nl-cas-witness-{}.bin", Uuid::new_v4()));
        let guard = SpoolGuard(Some(spool_path.clone()));
        let mut file = nativelink_util::fs::create_file(&spool_path)
            .await
            .err_tip(|| "creating cas-witness spool file")?;
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        let mut stream = resp.bytes_stream();
        let mut written: u64 = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                make_err!(Code::Unavailable, "reading origin body for '{url}': {e}")
            })?;
            written = written.saturating_add(chunk.len() as u64);
            // Abort as soon as the running size exceeds the cap, BEFORE
            // writing more: a chunked / no-Content-Length origin has no
            // declared size, so without this an unbounded body would spool to
            // the local disk (the size check after the loop is too late). The
            // `SpoolGuard` deletes the partial file on this early return.
            if written > self.max_fetch_size_bytes {
                return Err(make_err!(
                    Code::ResourceExhausted,
                    "origin body for '{url}' exceeds the {}-byte fetch limit",
                    self.max_fetch_size_bytes
                ));
            }
            hasher.update(&chunk);
            file.write_all(&chunk)
                .await
                .err_tip(|| "writing cas-witness spool file")?;
        }
        file.flush().await.err_tip(|| "flushing spool file")?;
        let digest = hasher.finalize_digest();
        Ok((file, digest, guard))
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
    warn!(?err, %url, "CAS witness request failed");
    status_body(StatusCode::BAD_GATEWAY, "CAS witness error")
}

/// Adds `X-Straylight-Witness` and `X-Straylight-Witness-Receipt` headers to
/// `response`, binding the served body to the attestation stored in the CAS.
fn add_witness_headers(
    response: &mut Response<ProxyBody>,
    url: &str,
    body_digest: &DigestInfo,
    attestation: &AttestationRef,
    key: &WitnessKey,
) {
    let body_sha256_hex = {
        let hash: &[u8; 32] = body_digest.packed_hash();
        hex::encode(hash)
    };
    let ts = witness::now_rfc3339();
    let headers = response.headers_mut();
    let witness_value = format!("blake3:{}", attestation.blake3_hex);
    if let Ok(value) = witness_value.parse() {
        headers.insert("X-Straylight-Witness", value);
    }
    // Signing a Receipt over all-string fields cannot realistically fail;
    // if it ever does, omit the receipt header rather than fail the response.
    match witness::Receipt::create_and_sign(
        &attestation.blake3_hex,
        &body_sha256_hex,
        url,
        &ts,
        key,
    ) {
        Ok(receipt) => {
            if let Ok(value) = receipt.parse() {
                headers.insert("X-Straylight-Witness-Receipt", value);
            }
        }
        Err(err) => {
            debug!(
                ?err,
                "cas-witness could not sign receipt; omitting receipt header"
            );
        }
    }
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
            std::env::temp_dir().join(format!("nl-cas-witness-ca-{}-{tag}", std::process::id()));
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

    /// Host validation gates leaf minting: an empty host, an over-253-char
    /// host (past the DNS limit), and a host carrying control or non-graphic
    /// characters are all refused. A CONNECT with such an authority must not
    /// drive certificate generation or feed garbage into the SAN/CN — and a
    /// normal host still succeeds and is cached (a second request returns the
    /// same `Arc` without error).
    #[test]
    fn server_config_for_host_validates_and_caches() {
        let (cert, key) = temp_paths("host-validate");
        drop(std::fs::remove_dir_all(Path::new(&cert).parent().unwrap()));
        let ca = CertAuthority::load_or_generate(&cert, &key).expect("generate");

        // Empty host.
        assert!(
            ca.server_config_for_host("").is_err(),
            "empty host must be refused"
        );
        // Over the DNS length limit (253).
        let too_long = "a".repeat(254);
        assert!(
            ca.server_config_for_host(&too_long).is_err(),
            "an over-253-char host must be refused"
        );
        // Control / non-graphic characters.
        for bad in [
            "exam\u{0}ple.com",
            "example.com\n",
            "has space.com",
            "tab\thost",
            "\u{7f}del.com",
            "unicode-\u{e9}.com",
        ] {
            assert!(
                ca.server_config_for_host(bad).is_err(),
                "host '{bad:?}' with control/non-graphic chars must be refused"
            );
        }

        // Exactly at the limit is accepted (boundary is inclusive).
        let at_limit = "a".repeat(253);
        assert!(
            ca.server_config_for_host(&at_limit).is_ok(),
            "a 253-char host is at the limit and accepted"
        );

        // A normal host succeeds, and a second request is a cache hit (same
        // Arc), proving the validated host was cached without error.
        let first = ca.server_config_for_host("example.org").expect("mint");
        let second = ca.server_config_for_host("example.org").expect("cache hit");
        assert!(
            Arc::ptr_eq(&first, &second),
            "the same valid host must return the cached config on the second call"
        );
    }
}
