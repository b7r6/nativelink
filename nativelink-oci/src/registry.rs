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

/// OCI image configuration (we only need diff_ids from rootfs).
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
    pub fn is_conforming_toolchain(&self) -> bool {
        self.layout_version.is_some()
    }
}

/// Token response from registry auth endpoint.
#[derive(Deserialize)]
struct TokenResponse {
    token: String,
}

/// OCI registry client.
#[derive(Debug)]
pub struct RegistryClient {
    client: Client,
}

impl RegistryClient {
    pub fn new() -> Result<Self, Error> {
        let client = Client::builder()
            .user_agent("nativelink-oci/0.1")
            .build()
            .map_err(|e| make_input_err!("Failed to build HTTP client: {e}"))?;
        Ok(Self { client })
    }

    /// Fetch a Bearer token for the given scope.
    async fn authenticate(
        &self,
        registry: &str,
        repository: &str,
    ) -> Result<Option<String>, Error> {
        // Try the token endpoint (Docker Hub pattern)
        let token_url = if registry == "registry-1.docker.io" {
            format!(
                "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repository}:pull"
            )
        } else {
            // For other registries, try anonymous first (return None)
            return Ok(None);
        };

        let resp = self
            .client
            .get(&token_url)
            .send()
            .await
            .err_tip(|| format!("Auth request to {token_url}"))?;

        if !resp.status().is_success() {
            return Ok(None);
        }

        let token_resp: TokenResponse = resp.json().await.err_tip(|| "Parsing token response")?;

        Ok(Some(token_resp.token))
    }

    /// Fetch the image manifest.
    pub async fn fetch_manifest(
        &self,
        image: &ImageReference,
    ) -> Result<(OciManifest, String), Error> {
        let token = self
            .authenticate(&image.registry, &image.repository)
            .await?;

        let ref_str = match &image.reference {
            Reference::Tag(tag) => tag.clone(),
            Reference::Digest(digest) => digest.clone(),
        };

        let url = format!(
            "https://{}/v2/{}/manifests/{ref_str}",
            image.registry, image.repository,
        );

        info!(%url, "Fetching OCI manifest");

        let mut req = self.client.get(&url).header(
            "Accept",
            "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
        );

        if let Some(ref tok) = token {
            req = req.bearer_auth(tok);
        }

        let resp = req
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

    /// Fetch the image config blob (contains diff_ids).
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
        let token = self
            .authenticate(&image.registry, &image.repository)
            .await?;

        let url = format!(
            "https://{}/v2/{}/blobs/{digest}",
            image.registry, image.repository,
        );

        let mut req = self.client.get(&url);
        if let Some(ref tok) = token {
            req = req.bearer_auth(tok);
        }

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
