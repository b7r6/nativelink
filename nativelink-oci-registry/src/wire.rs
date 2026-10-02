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

//! OCI Distribution Specification wire vocabulary: repository names, tags,
//! digests, canonical error codes, and just-enough manifest parsing.
//!
//! Grammar sources: distribution-spec v1.1, `spec.md` §"Pulling manifests"
//! (`<name>` / `<reference>` regexes) and the error-codes table. This module
//! validates names BEFORE any store key is derived from them, so a crafted
//! name can never smuggle separators into `oci-tag:`/`oci-tags:` keys.

use nativelink_error::{Error, make_input_err};

/// Media type of OCI image manifests.
pub const OCI_MANIFEST_MEDIA_TYPE: &str = "application/vnd.oci.image.manifest.v1+json";
/// Media type of OCI image indexes.
pub const OCI_INDEX_MEDIA_TYPE: &str = "application/vnd.oci.image.index.v1+json";
/// Media type of Docker v2 schema-2 manifests.
pub const DOCKER_MANIFEST_MEDIA_TYPE: &str = "application/vnd.docker.distribution.manifest.v2+json";
/// Media type of Docker v2 manifest lists.
pub const DOCKER_MANIFEST_LIST_MEDIA_TYPE: &str =
    "application/vnd.docker.distribution.manifest.list.v2+json";

/// Maximum length of a repository name; the spec caps the combined
/// `<name>` at 255 characters for compatibility.
const MAX_NAME_LEN: usize = 255;
/// Maximum length of a tag (`[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`).
const MAX_TAG_LEN: usize = 128;

/// Whether one path component of a repository name matches
/// `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*`.
fn is_valid_name_component(component: &str) -> bool {
    let bytes = component.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    // First and last must be [a-z0-9].
    if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    let mut i = 1;
    while i < bytes.len() {
        let b = bytes[i];
        if is_alnum(b) {
            i += 1;
            continue;
        }
        match b {
            b'.' => {
                // A single dot, followed by [a-z0-9].
                match bytes.get(i + 1) {
                    Some(&next) if is_alnum(next) => i += 1,
                    _ => return false,
                }
            }
            b'_' => {
                // `_` or `__`, followed by [a-z0-9].
                let run = if bytes.get(i + 1) == Some(&b'_') {
                    2
                } else {
                    1
                };
                match bytes.get(i + run) {
                    Some(&next) if is_alnum(next) => i += run,
                    _ => return false,
                }
            }
            b'-' => {
                // One or more dashes, followed by [a-z0-9].
                let mut run = 1;
                while bytes.get(i + run) == Some(&b'-') {
                    run += 1;
                }
                match bytes.get(i + run) {
                    Some(&next) if is_alnum(next) => i += run,
                    _ => return false,
                }
            }
            _ => return false,
        }
    }
    true
}

/// Whether `name` is a valid repository name: slash-separated components,
/// each matching the spec's component grammar, total length capped.
#[must_use]
pub fn is_valid_repository_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_NAME_LEN && name.split('/').all(is_valid_name_component)
}

/// Whether `tag` matches `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`.
#[must_use]
pub fn is_valid_tag(tag: &str) -> bool {
    let bytes = tag.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_TAG_LEN {
        return false;
    }
    let first_ok = bytes[0].is_ascii_alphanumeric() || bytes[0] == b'_';
    first_ok
        && bytes[1..]
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Parses an OCI digest string, returning the lowercase-hex sha256.
///
/// v1 accepts ONLY `sha256:<64 lowercase hex>` — the wire identity skopeo,
/// docker, and crane produce. Any other registered algorithm is rejected
/// with an error the caller maps to `UNSUPPORTED` (a digest we cannot
/// recompute is a digest we must not serve).
pub fn parse_digest(digest: &str) -> Result<&str, Error> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(make_input_err!(
            "unsupported or malformed digest '{digest}': only sha256:<hex> is supported"
        ));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(make_input_err!(
            "invalid sha256 digest '{digest}': expected 64 lowercase hex characters"
        ));
    }
    Ok(hex)
}

/// A `<reference>` in a manifests URL: a tag or a digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestReference {
    /// A mutable tag name.
    Tag(String),
    /// `sha256:` digest; carries the validated lowercase hex.
    Digest(String),
}

/// Parses a manifests `<reference>` path segment.
pub fn parse_manifest_reference(reference: &str) -> Result<ManifestReference, Error> {
    if reference.contains(':') {
        return Ok(ManifestReference::Digest(
            parse_digest(reference)?.to_string(),
        ));
    }
    if is_valid_tag(reference) {
        return Ok(ManifestReference::Tag(reference.to_string()));
    }
    Err(make_input_err!("invalid manifest reference '{reference}'"))
}

/// Canonical error codes from the distribution-spec error table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OciErrorCode {
    BlobUnknown,
    BlobUploadInvalid,
    BlobUploadUnknown,
    DigestInvalid,
    ManifestBlobUnknown,
    ManifestInvalid,
    ManifestUnknown,
    NameInvalid,
    NameUnknown,
    SizeInvalid,
    Unauthorized,
    Denied,
    Unsupported,
    TooManyRequests,
}

impl OciErrorCode {
    /// The spec's `code` string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BlobUnknown => "BLOB_UNKNOWN",
            Self::BlobUploadInvalid => "BLOB_UPLOAD_INVALID",
            Self::BlobUploadUnknown => "BLOB_UPLOAD_UNKNOWN",
            Self::DigestInvalid => "DIGEST_INVALID",
            Self::ManifestBlobUnknown => "MANIFEST_BLOB_UNKNOWN",
            Self::ManifestInvalid => "MANIFEST_INVALID",
            Self::ManifestUnknown => "MANIFEST_UNKNOWN",
            Self::NameInvalid => "NAME_INVALID",
            Self::NameUnknown => "NAME_UNKNOWN",
            Self::SizeInvalid => "SIZE_INVALID",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::Denied => "DENIED",
            Self::Unsupported => "UNSUPPORTED",
            Self::TooManyRequests => "TOOMANYREQUESTS",
        }
    }
}

/// Renders the spec's `errors[]` JSON body for one error.
#[must_use]
pub fn error_body(code: OciErrorCode, message: &str) -> String {
    serde_json::json!({
        "errors": [{
            "code": code.as_str(),
            "message": message,
        }]
    })
    .to_string()
}

/// A descriptor referenced by a manifest: its wire digest hex and, when
/// declared, its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferencedDescriptor {
    /// Lowercase hex of the referenced sha256 digest.
    pub sha256_hex: String,
    /// Declared size in bytes, when present.
    pub size: Option<u64>,
}

/// The result of just-enough manifest parsing on PUT: what it claims to be
/// and what it references. NOT a full schema validation — the conformance
/// suite is the oracle for wire behavior; this exists only to reject a
/// manifest referencing unindexed blobs (`MANIFEST_BLOB_UNKNOWN`) and to
/// record the media type.
#[derive(Debug, Clone)]
pub struct ParsedManifest {
    /// The manifest's own `mediaType`, when declared.
    pub media_type: Option<String>,
    /// Blobs this manifest references (config + layers), which must exist
    /// before the manifest is accepted.
    pub blob_references: Vec<ReferencedDescriptor>,
    /// Child MANIFESTS referenced by an index (`manifests[]`), which must
    /// exist as manifests before the index is accepted.
    pub manifest_references: Vec<ReferencedDescriptor>,
    /// The `subject` descriptor digest, when present (OCI 1.1 referrers).
    pub subject_sha256_hex: Option<String>,
    /// Top-level `artifactType`, when declared.
    pub artifact_type: Option<String>,
    /// `config.mediaType`, when present — the referrers fallback for a
    /// descriptor's `artifactType` per the spec.
    pub config_media_type: Option<String>,
    /// Top-level `annotations`, serialized back to a JSON object string;
    /// `None` when absent. Copied verbatim into referrers descriptors.
    pub annotations_json: Option<String>,
}

fn descriptor_from_value(value: &serde_json::Value) -> Result<ReferencedDescriptor, Error> {
    let digest = value
        .get("digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| make_input_err!("manifest descriptor is missing 'digest'"))?;
    let sha256_hex = parse_digest(digest)?.to_string();
    let size = value.get("size").and_then(serde_json::Value::as_u64);
    Ok(ReferencedDescriptor { sha256_hex, size })
}

/// Parses a pushed manifest body just enough to know its media type, the
/// blobs and child manifests it references, and its `subject`.
pub fn parse_manifest(body: &[u8]) -> Result<ParsedManifest, Error> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| make_input_err!("manifest body is not valid JSON: {e}"))?;
    if !value.is_object() {
        return Err(make_input_err!("manifest body is not a JSON object"));
    }
    let media_type = value
        .get("mediaType")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);

    let mut blob_references = Vec::new();
    let mut manifest_references = Vec::new();

    if let Some(config) = value.get("config") {
        blob_references.push(
            descriptor_from_value(config)
                .map_err(|e| make_input_err!("in manifest 'config': {e}"))?,
        );
    }
    let config_media_type = value
        .get("config")
        .and_then(|c| c.get("mediaType"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    if let Some(layers) = value.get("layers") {
        let layers = layers
            .as_array()
            .ok_or_else(|| make_input_err!("manifest 'layers' is not an array"))?;
        for (idx, layer) in layers.iter().enumerate() {
            blob_references.push(
                descriptor_from_value(layer)
                    .map_err(|e| make_input_err!("in manifest 'layers[{idx}]': {e}"))?,
            );
        }
    }
    if let Some(manifests) = value.get("manifests") {
        let manifests = manifests
            .as_array()
            .ok_or_else(|| make_input_err!("index 'manifests' is not an array"))?;
        for (idx, child) in manifests.iter().enumerate() {
            manifest_references.push(
                descriptor_from_value(child)
                    .map_err(|e| make_input_err!("in index 'manifests[{idx}]': {e}"))?,
            );
        }
    }
    let subject_sha256_hex = match value.get("subject") {
        Some(subject) => Some(
            descriptor_from_value(subject)
                .map_err(|e| make_input_err!("in manifest 'subject': {e}"))?
                .sha256_hex,
        ),
        None => None,
    };

    // A manifest must reference SOMETHING recognizable; a body with neither
    // config/layers nor manifests is not a manifest shape we serve.
    if blob_references.is_empty() && manifest_references.is_empty() {
        return Err(make_input_err!(
            "manifest declares neither 'config'/'layers' nor 'manifests'"
        ));
    }
    let artifact_type = value
        .get("artifactType")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let annotations_json = value
        .get("annotations")
        .filter(|a| a.is_object())
        .map(ToString::to_string);
    Ok(ParsedManifest {
        media_type,
        blob_references,
        manifest_references,
        subject_sha256_hex,
        artifact_type,
        config_media_type,
        annotations_json,
    })
}
