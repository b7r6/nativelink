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

//! OCI Distribution API client.
//!
//! Speaks the OCI Distribution Spec v2 to pull manifests and layer blobs from
//! any conforming registry (Docker Hub, GHCR, ECR, GCR, R2-backed, etc.).
//!
//! This module handles:
//! - Image reference parsing (`registry/repo:tag` or `registry/repo@sha256:...`)
//! - Token-based authentication (Bearer token via `/v2/token`)
//! - Manifest retrieval (by tag or digest)
//! - Layer blob streaming download
//! - Annotation extraction (for Standard OCI Toolchain hints)

use core::fmt;

use nativelink_error::{Error, ResultExt, make_input_err};
use reqwest::Client;
use serde::Deserialize;
use tracing::info;

/// A parsed OCI image reference.
#[derive(Debug, Clone)]
pub struct ImageReference {
    /// Registry host (e.g. "ghcr.io", "registry.example.com")
    pub registry: String,
    /// Repository path (e.g. "tracemachina/nativelink")
    pub repository: String,
    /// Either a tag ("latest", "v1.0") or a digest ("sha256:abc123...")
    pub reference: Reference,
}

/// Tag or digest reference.
#[derive(Debug, Clone)]
pub enum Reference {
    Tag(String),
    Digest(String),
}

impl fmt::Display for ImageReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reference {
            Reference::Tag(tag) => write!(f, "{}/{}:{}", self.registry, self.repository, tag),
            Reference::Digest(digest) => {
                write!(f, "{}/{}@{}", self.registry, self.repository, digest)
            }
        }
    }
}

impl ImageReference {
    /// Parse a string like `registry.example.com/repo/name:tag` or
    /// `registry.example.com/repo/name@sha256:abcdef...`
    pub fn parse(s: &str) -> Result<Self, Error> {
        // Strip optional docker:// or oci:// prefix
        let s = s
            .strip_prefix("docker://")
            .or_else(|| s.strip_prefix("oci://"))
            .unwrap_or(s);

        // Split on @ for digest references
        if let Some((repo_part, digest)) = s.split_once('@') {
            let (registry, repository) = split_registry_repo(repo_part)?;
            return Ok(Self {
                registry,
                repository,
                reference: Reference::Digest(digest.to_string()),
            });
        }

        // Split on : for tag references (but not port numbers)
        // Find the last : that isn't part of a port (i.e., after a /)
        let (repo_part, tag) = if let Some(slash_pos) = s.rfind('/') {
            if let Some(colon_pos) = s[slash_pos..].rfind(':') {
                let abs_colon = slash_pos + colon_pos;
                (&s[..abs_colon], &s[abs_colon + 1..])
            } else {
                (s, "latest")
            }
        } else if let Some((left, right)) = s.rsplit_once(':') {
            // No slash — could be host:port or name:tag
            // If right is all digits, it's a port; treat whole thing as repo with "latest"
            if right.chars().all(|c| c.is_ascii_digit()) {
                (s, "latest")
            } else {
                (left, right)
            }
        } else {
            (s, "latest")
        };

        let (registry, repository) = split_registry_repo(repo_part)?;
        Ok(Self {
            registry,
            repository,
            reference: Reference::Tag(tag.to_string()),
        })
    }
}

/// Split "registry.example.com/org/repo" into ("registry.example.com", "org/repo").
/// If no registry is apparent, defaults to "registry-1.docker.io" (Docker Hub).
#[allow(clippy::unnecessary_wraps)] // Result kept for call-site uniformity
fn split_registry_repo(s: &str) -> Result<(String, String), Error> {
    if let Some(slash_pos) = s.find('/') {
        let first_part = &s[..slash_pos];
        // Heuristic: if the first part contains a dot or colon, it's a registry host
        if first_part.contains('.') || first_part.contains(':') {
            return Ok((first_part.to_string(), s[slash_pos + 1..].to_string()));
        }
        // Otherwise it's a Docker Hub path (e.g. "library/ubuntu")
        Ok(("registry-1.docker.io".to_string(), s.to_string()))
    } else {
        // Bare name like "ubuntu" → docker.io/library/ubuntu
        Ok(("registry-1.docker.io".to_string(), format!("library/{s}")))
    }
}

/// OCI manifest (simplified — we only need image manifests, not manifest lists).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OciManifest {
    pub schema_version: u32,
    pub media_type: Option<String>,
    pub config: Descriptor,
    pub layers: Vec<Descriptor>,
    #[serde(default)]
    pub annotations: std::collections::HashMap<String, String>,
}

/// OCI descriptor (digest + size + mediaType).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    pub media_type: String,
    pub digest: String,
    pub size: u64,
    #[serde(default)]
    pub annotations: std::collections::HashMap<String, String>,
}

/// OCI image configuration (we only need `diff_ids` from rootfs).
#[derive(Debug, Deserialize)]
pub struct OciConfig {
    pub rootfs: RootFs,
}

#[derive(Debug, Deserialize)]
pub struct RootFs {
    #[serde(rename = "type")]
    pub fs_type: String,
    pub diff_ids: Vec<String>,
}

/// Toolchain hints extracted from OCI annotations (§5.6, §6.5).
#[derive(Debug, Default)]
pub struct ToolchainHints {
    /// The role label (informational, not identity)
    pub role: Option<String>,
    /// Declared REAPI digest function (e.g. "BLAKE3")
    pub reapi_digest_function: Option<String>,
    /// REAPI root Directory digest hint (e.g. "<blake3-hex>/<size>")
    pub reapi_root: Option<String>,
    /// Layout version
    pub layout_version: Option<String>,
}

impl ToolchainHints {
    /// Extract hints from OCI annotations per the Standard OCI Toolchain Spec §5.6.
    pub fn from_annotations(annotations: &std::collections::HashMap<String, String>) -> Self {
        Self {
            role: annotations.get("dev.straylight.toolchain.role").cloned(),
            reapi_digest_function: annotations
                .get("dev.straylight.toolchain.reapi.digest-function")
                .cloned(),
            reapi_root: annotations
                .get("dev.straylight.toolchain.reapi.root")
                .cloned(),
            layout_version: annotations
                .get("dev.straylight.toolchain.layout-version")
                .cloned(),
        }
    }

    /// Returns true if this image carries Standard OCI Toolchain annotations.
    pub const fn is_conforming_toolchain(&self) -> bool {
        self.layout_version.is_some()
    }
}

/// Token response from a registry's Bearer token realm. Registries return the
/// token under `token`; some (older) implementations use `access_token`.
#[derive(Deserialize)]
struct TokenResponse {
    #[serde(alias = "access_token")]
    token: String,
}

/// Credentials for authenticating to an OCI registry (runtime form of the
/// operator's `OciRegistryConfig`). All-`None` means anonymous.
#[derive(Debug, Clone, Default)]
pub struct RegistryAuth {
    pub username: Option<String>,
    pub password: Option<String>,
    /// A pre-issued Bearer token, sent verbatim (short-circuits the challenge).
    pub bearer_token: Option<String>,
}

/// Fully-resolved connection + credential settings for one registry host — the
/// runtime projection of `nativelink-config`'s `OciRegistryConfig`.
#[derive(Debug, Clone)]
pub struct RegistrySettings {
    /// Registry host, matched against `ImageReference::registry`.
    pub host: String,
    /// URL scheme: "https" (default) or "http".
    pub scheme: String,
    /// Inline PEM, or path to a PEM CA bundle, to trust for this registry.
    pub root_certificates: Option<String>,
    /// Skip TLS verification (self-signed internal registries).
    pub insecure_skip_verify: bool,
    /// Credentials (anonymous if every field is `None`).
    pub auth: RegistryAuth,
}

/// Authentication to apply to a registry request, resolved from the
/// `WWW-Authenticate` challenge and the configured credentials.
#[derive(Debug, Clone)]
enum AuthMethod {
    /// No credentials — send the request bare.
    Anonymous,
    /// `Authorization: Bearer <token>`.
    Bearer(String),
    /// `Authorization: Basic <base64(user:pass)>`.
    Basic { username: String, password: String },
}

/// Build a reqwest client honoring a registry's TLS settings. `None` yields the
/// default client (system roots, verification on) used for anonymous HTTPS.
fn build_client(settings: Option<&RegistrySettings>) -> Result<Client, Error> {
    let mut builder = Client::builder().user_agent("nativelink-oci/0.1");
    if let Some(s) = settings {
        if s.insecure_skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if let Some(ca) = &s.root_certificates {
            let pem = if ca.contains("BEGIN CERTIFICATE") {
                ca.clone().into_bytes()
            } else {
                std::fs::read(ca)
                    .err_tip(|| format!("Reading registry root_certificates from '{ca}'"))?
            };
            let cert = reqwest::Certificate::from_pem(&pem)
                .map_err(|e| make_input_err!("Parsing registry root_certificates: {e}"))?;
            builder = builder.add_root_certificate(cert);
        }
    }
    builder
        .build()
        .map_err(|e| make_input_err!("Failed to build HTTP client: {e}"))
}

/// Parse a `WWW-Authenticate: Bearer realm="…",service="…",scope="…"` header
/// into `(realm, service)`. Returns `None` if it is not a Bearer challenge.
fn parse_bearer_challenge(header: &str) -> Option<(String, Option<String>)> {
    let rest = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))?;
    let (mut realm, mut service) = (None, None);
    for part in rest.split(',') {
        let Some((k, v)) = part.split_once('=') else {
            continue;
        };
        let v = v.trim().trim_matches('"').to_string();
        match k.trim() {
            "realm" => realm = Some(v),
            "service" => service = Some(v),
            _ => {}
        }
    }
    realm.map(|r| (r, service))
}

/// Apply a resolved `AuthMethod` to a request builder.
fn apply_auth(req: reqwest::RequestBuilder, auth: &AuthMethod) -> reqwest::RequestBuilder {
    match auth {
        AuthMethod::Anonymous => req,
        AuthMethod::Bearer(token) => req.bearer_auth(token),
        AuthMethod::Basic { username, password } => req.basic_auth(username, Some(password)),
    }
}

/// OCI registry client.
///
/// Holds a default client (anonymous HTTPS, system roots) plus a per-host
/// client+settings for each configured registry, so scheme, TLS trust, and
/// credentials are applied by matching the image's registry host.
#[derive(Debug)]
pub struct RegistryClient {
    default_client: Client,
    /// `(host, client, settings)` for each configured registry.
    entries: Vec<(String, Client, RegistrySettings)>,
}

impl RegistryClient {
    /// A client with no configured registries — every host is contacted
    /// anonymously over HTTPS with system TLS roots.
    pub fn new() -> Result<Self, Error> {
        Self::with_registries(Vec::new())
    }

    /// A client carrying per-registry connection + credential settings.
    pub fn with_registries(registries: Vec<RegistrySettings>) -> Result<Self, Error> {
        let default_client = build_client(None)?;
        let mut entries = Vec::with_capacity(registries.len());
        for settings in registries {
            let client = build_client(Some(&settings))?;
            entries.push((settings.host.clone(), client, settings));
        }
        Ok(Self {
            default_client,
            entries,
        })
    }

    /// Settings configured for `host`, if any.
    fn settings_for(&self, host: &str) -> Option<&RegistrySettings> {
        self.entries
            .iter()
            .find(|(h, _, _)| h == host)
            .map(|(_, _, s)| s)
    }

    /// The client to use for `host` (its per-registry TLS, or the default).
    fn client_for(&self, host: &str) -> &Client {
        self.entries
            .iter()
            .find(|(h, _, _)| h == host)
            .map_or(&self.default_client, |(_, c, _)| c)
    }

    /// The URL scheme for `host` ("https" unless overridden to "http").
    fn scheme_for(&self, host: &str) -> &str {
        self.settings_for(host)
            .map(|s| s.scheme.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("https")
    }

    /// Resolve the authentication for pulling from `image`, following the OCI
    /// Distribution v2 `WWW-Authenticate` challenge.
    ///
    /// - A configured `bearer_token` is returned verbatim (no challenge).
    /// - Otherwise the registry's `/v2/` endpoint is probed: `200` means
    ///   anonymous access; a `401` `Bearer` challenge triggers a token fetch
    ///   from the realm (HTTP Basic with `username`/`password` if set); a
    ///   `Basic` challenge with credentials is applied directly.
    async fn authenticate(&self, image: &ImageReference) -> Result<AuthMethod, Error> {
        let host = &image.registry;
        let auth = self.settings_for(host).map(|s| &s.auth);

        // A pre-issued token short-circuits the challenge.
        if let Some(tok) = auth.and_then(|a| a.bearer_token.clone()) {
            return Ok(AuthMethod::Bearer(tok));
        }

        let client = self.client_for(host);
        let base = format!("{}://{host}/v2/", self.scheme_for(host));

        let resp = client
            .get(&base)
            .send()
            .await
            .err_tip(|| format!("Probing registry auth at {base}"))?;

        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            // 2xx: anonymous access. Any other non-401: not an auth problem —
            // fall through and let the concrete request surface a precise error.
            return Ok(AuthMethod::Anonymous);
        }

        let challenge = resp
            .headers()
            .get("WWW-Authenticate")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        // Bearer challenge → fetch a token from the realm.
        if let Some((realm, service)) = parse_bearer_challenge(challenge) {
            let scope = format!("repository:{}:pull", image.repository);
            let mut token_req = client.get(&realm).query(&[("scope", scope.as_str())]);
            if let Some(svc) = service.as_deref() {
                token_req = token_req.query(&[("service", svc)]);
            }
            if let (Some(u), Some(p)) = (
                auth.and_then(|a| a.username.as_deref()),
                auth.and_then(|a| a.password.as_deref()),
            ) {
                token_req = token_req.basic_auth(u, Some(p));
            }

            let token_resp = token_req
                .send()
                .await
                .err_tip(|| format!("Fetching registry token from {realm}"))?;
            if !token_resp.status().is_success() {
                return Err(make_input_err!(
                    "Registry token endpoint {realm} returned {}",
                    token_resp.status()
                ));
            }
            let parsed: TokenResponse = token_resp
                .json()
                .await
                .err_tip(|| "Parsing registry token response")?;
            return Ok(AuthMethod::Bearer(parsed.token));
        }

        // Basic challenge → apply configured credentials directly.
        if (challenge.starts_with("Basic") || challenge.starts_with("basic"))
            && let (Some(u), Some(p)) = (
                auth.and_then(|a| a.username.clone()),
                auth.and_then(|a| a.password.clone()),
            )
        {
            return Ok(AuthMethod::Basic {
                username: u,
                password: p,
            });
        }

        // Unknown/absent challenge, or no credentials to satisfy it — proceed
        // anonymously and let the concrete request report the failure.
        Ok(AuthMethod::Anonymous)
    }

    /// Fetch the image manifest.
    pub async fn fetch_manifest(
        &self,
        image: &ImageReference,
    ) -> Result<(OciManifest, String), Error> {
        let auth = self.authenticate(image).await?;

        let ref_str = match &image.reference {
            Reference::Tag(tag) => tag.clone(),
            Reference::Digest(digest) => digest.clone(),
        };

        let url = format!(
            "{}://{}/v2/{}/manifests/{ref_str}",
            self.scheme_for(&image.registry),
            image.registry,
            image.repository,
        );

        info!(%url, "Fetching OCI manifest");

        let req = self.client_for(&image.registry).get(&url).header(
            "Accept",
            "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
        );

        let resp = apply_auth(req, &auth)
            .send()
            .await
            .err_tip(|| format!("Fetching manifest from {url}"))?;

        if !resp.status().is_success() {
            return Err(make_input_err!(
                "Registry returned {} for manifest at {url}",
                resp.status()
            ));
        }

        // Capture the digest from Docker-Content-Digest header (if present)
        let manifest_digest = resp
            .headers()
            .get("Docker-Content-Digest")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let manifest: OciManifest = resp.json().await.err_tip(|| "Parsing manifest JSON")?;

        Ok((manifest, manifest_digest))
    }

    /// Fetch the image config blob (contains `diff_ids`).
    pub async fn fetch_config(
        &self,
        image: &ImageReference,
        config_descriptor: &Descriptor,
    ) -> Result<OciConfig, Error> {
        let blob_bytes = self.fetch_blob(image, &config_descriptor.digest).await?;

        serde_json::from_slice(&blob_bytes).map_err(|e| make_input_err!("Parsing OCI config: {e}"))
    }

    /// Download a blob by digest, returning the raw bytes.
    pub async fn fetch_blob(&self, image: &ImageReference, digest: &str) -> Result<Vec<u8>, Error> {
        let auth = self.authenticate(image).await?;

        let url = format!(
            "{}://{}/v2/{}/blobs/{digest}",
            self.scheme_for(&image.registry),
            image.registry,
            image.repository,
        );

        let req = apply_auth(self.client_for(&image.registry).get(&url), &auth);

        let resp = req
            .send()
            .await
            .err_tip(|| format!("Fetching blob {digest}"))?;

        if !resp.status().is_success() {
            return Err(make_input_err!(
                "Registry returned {} for blob {digest}",
                resp.status()
            ));
        }

        let bytes = resp
            .bytes()
            .await
            .err_tip(|| format!("Reading blob {digest}"))?;

        Ok(bytes.to_vec())
    }
}
