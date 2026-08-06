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

//! Store records for the OCI registry facade.
//!
//! Two record families, both string-keyed:
//!
//! - **Digest-alias records** (`oci-digest:sha256:<hex>`, immutable,
//!   content-derived): map a blob's OCI wire name (sha256) to its canonical
//!   storage identity (`(canonical_hex, size)` under the deployment's digest
//!   function — BLAKE3 on the fleet). A lost record is repaired by
//!   re-hashing the blob; a wrong one is detectable the same way.
//! - **Tag records** (`oci-tag:<name>:<tag>`, mutable, overwrite-wins) and
//!   the per-repo tag index (`oci-tags:<name>`): the small separable
//!   name→content residue. Never behind `existence_cache`.
//!
//! Every record is a prost message wrapped in a REAPI `ActionResult`
//! envelope — the `NixPathInfo` pattern — whose single output file names the
//! record's backing blob by its CANONICAL digest, so a `completeness_checking`
//! wrapper over the record store degrades an evicted blob to a clean 404
//! instead of advertising content that can no longer be served. The repo tag
//! index references no blob and carries an empty `output_files`.

use nativelink_error::{Error, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, Digest, ExecutedActionMetadata, OutputFile,
};
use prost::Message;

/// `type_url` of [`OciDigestAlias`] payloads inside the envelope.
pub const OCI_DIGEST_ALIAS_TYPE_URL: &str =
    "type.googleapis.com/com.github.trace_machina.nativelink.oci.OciDigestAlias";
/// `type_url` of [`OciTagRecord`] payloads inside the envelope.
pub const OCI_TAG_RECORD_TYPE_URL: &str =
    "type.googleapis.com/com.github.trace_machina.nativelink.oci.OciTagRecord";
/// `type_url` of [`OciRepoTagIndex`] payloads inside the envelope.
pub const OCI_REPO_TAG_INDEX_TYPE_URL: &str =
    "type.googleapis.com/com.github.trace_machina.nativelink.oci.OciRepoTagIndex";
/// `type_url` of [`OciReferrerList`] payloads inside the envelope.
pub const OCI_REFERRER_LIST_TYPE_URL: &str =
    "type.googleapis.com/com.github.trace_machina.nativelink.oci.OciReferrerList";
/// Name of the single output file in blob-backed record envelopes.
pub const RECORD_OUTPUT_FILE_NAME: &str = "blob";
/// `worker` name stamped into every record envelope's metadata.
pub const OCI_REGISTRY_WORKER_NAME: &str = "nativelink-oci-registry";

/// Store key of the digest-alias record for an OCI wire digest. `sha256_hex`
/// must already be validated lowercase hex (see [`crate::wire::parse_digest`]).
#[must_use]
pub fn alias_key(sha256_hex: &str) -> String {
    format!("oci-digest:sha256:{sha256_hex}")
}

/// Encodes a repository name for use inside a store key. Repository names
/// legally contain `/`, but string-keyed stores map keys to filesystem
/// paths, where a `/` silently becomes a directory separator (an ENOENT on
/// the first multi-segment repo, not a clean error). `%` is outside both
/// the name and tag grammars, so `/` -> `%2F` is injective.
fn encode_name(name: &str) -> String {
    name.replace('/', "%2F")
}

/// Store key of the tag record for `<name>:<tag>`.
#[must_use]
pub fn tag_key(name: &str, tag: &str) -> String {
    format!("oci-tag:{}:{tag}", encode_name(name))
}

/// Store key of the per-repo tag index for `<name>`.
#[must_use]
pub fn repo_tag_index_key(name: &str) -> String {
    format!("oci-tags:{}", encode_name(name))
}

/// Store key of the referrers list for subject `<sha256 hex>` in `<name>`
/// (OCI 1.1 referrers: the reverse `subject` index maintained on manifest
/// PUT).
#[must_use]
pub fn referrers_key(name: &str, subject_sha256_hex: &str) -> String {
    format!("oci-referrers:{}:{subject_sha256_hex}", encode_name(name))
}

/// An immutable digest-alias record: OCI wire name → canonical storage
/// identity. Field tags are a wire contract; only append.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct OciDigestAlias {
    /// Lowercase hex of the blob's canonical digest (the deployment's
    /// digest function; BLAKE3 on the fleet). Exactly 64 hex characters.
    #[prost(string, tag = "1")]
    pub canonical_hex: String,
    /// Size of the blob in bytes.
    #[prost(uint64, tag = "2")]
    pub size: u64,
    /// Lowercase hex of the blob's sha256 (the OCI wire name). Redundant
    /// with the store key, kept so a record is self-describing and the
    /// index is rebuildable from records alone.
    #[prost(string, tag = "3")]
    pub sha256_hex: String,
    /// Media type as pushed, for manifests (`Content-Type` on the PUT or
    /// the manifest's own `mediaType`); empty for plain blobs.
    #[prost(string, tag = "4")]
    pub media_type: String,
}

/// A mutable tag record: the manifest descriptor a tag currently binds to.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct OciTagRecord {
    /// Media type of the manifest the tag points at.
    #[prost(string, tag = "1")]
    pub media_type: String,
    /// Lowercase hex sha256 (wire name) of the manifest.
    #[prost(string, tag = "2")]
    pub manifest_sha256_hex: String,
    /// Size in bytes of the manifest body.
    #[prost(uint64, tag = "3")]
    pub size: u64,
    /// Lowercase hex canonical digest of the manifest body.
    #[prost(string, tag = "4")]
    pub canonical_hex: String,
    /// Seconds since the Unix epoch at which this binding was written.
    #[prost(uint64, tag = "5")]
    pub created_at_unix: u64,
    /// Wire (sha256) hex of the manifest this binding REPLACED; empty for a
    /// first write. Reserved for tag-history audit (design 2, alternative
    /// (c)); no v1 endpoint walks the chain.
    #[prost(string, tag = "6")]
    pub previous_sha256_hex: String,
}

/// The per-repo tag index: a sorted set of tag names, read-modify-merged on
/// tag PUT/DELETE. Discovery only — the authoritative binding is always the
/// per-tag record; a lost race here is repaired by the next PUT/DELETE.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct OciRepoTagIndex {
    /// Tag names, byte-lexicographically sorted, unique.
    #[prost(string, repeated, tag = "1")]
    pub tags: Vec<String>,
}

/// Whether `hex` is exactly 64 lowercase hex characters.
fn is_lower_hex_64(hex: &str) -> bool {
    hex.len() == 64
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Wraps a prost payload in the `ActionResult` record envelope. When
/// `blob` is `Some((canonical_hex, size))` the envelope carries one output
/// file naming that blob, so `completeness_checking` keys on it.
fn encode_envelope(
    type_url: &str,
    payload: Vec<u8>,
    blob: Option<(&str, u64)>,
) -> Result<Vec<u8>, Error> {
    let output_files = match blob {
        Some((canonical_hex, size)) => {
            if !is_lower_hex_64(canonical_hex) {
                return Err(make_input_err!(
                    "OCI record canonical digest '{canonical_hex}' is not 64 lowercase hex characters"
                ));
            }
            let size_bytes = i64::try_from(size).map_err(|e| {
                make_input_err!("OCI record blob size {size} does not fit in i64: {e}")
            })?;
            vec![OutputFile {
                path: RECORD_OUTPUT_FILE_NAME.to_string(),
                digest: Some(Digest {
                    hash: canonical_hex.to_string(),
                    size_bytes,
                }),
                ..Default::default()
            }]
        }
        None => vec![],
    };
    let record = ProtoActionResult {
        output_files,
        exit_code: 0,
        execution_metadata: Some(ExecutedActionMetadata {
            worker: OCI_REGISTRY_WORKER_NAME.to_string(),
            auxiliary_metadata: vec![prost_types::Any {
                type_url: type_url.to_string(),
                value: payload,
            }],
            ..Default::default()
        }),
        ..Default::default()
    };
    Ok(record.encode_to_vec())
}

/// Unwraps the record envelope, returning the payload bytes for `type_url`.
fn decode_envelope(bytes: &[u8], type_url: &str) -> Result<Vec<u8>, Error> {
    let record = ProtoActionResult::decode(bytes)
        .map_err(|e| make_input_err!("OCI record is not a valid ActionResult envelope: {e}"))?;
    if record.exit_code != 0 {
        return Err(make_input_err!(
            "OCI record envelope has nonzero exit_code {}",
            record.exit_code
        ));
    }
    let metadata = record
        .execution_metadata
        .as_ref()
        .ok_or_else(|| make_input_err!("OCI record envelope is missing execution_metadata"))?;
    let any = metadata
        .auxiliary_metadata
        .iter()
        .find(|any| any.type_url == type_url)
        .ok_or_else(|| {
            make_input_err!(
                "OCI record envelope has no auxiliary_metadata with type_url '{type_url}'"
            )
        })?;
    Ok(any.value.clone())
}

impl OciDigestAlias {
    /// Validates the record's invariants.
    pub fn validate(&self) -> Result<(), Error> {
        if !is_lower_hex_64(&self.canonical_hex) {
            return Err(make_input_err!(
                "OciDigestAlias canonical_hex '{}' is not 64 lowercase hex characters",
                self.canonical_hex
            ));
        }
        if !is_lower_hex_64(&self.sha256_hex) {
            return Err(make_input_err!(
                "OciDigestAlias sha256_hex '{}' is not 64 lowercase hex characters",
                self.sha256_hex
            ));
        }
        Ok(())
    }

    /// Encodes into the `ActionResult` record envelope; the output file
    /// names the blob by its canonical digest.
    pub fn encode_record(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode_envelope(
            OCI_DIGEST_ALIAS_TYPE_URL,
            self.encode_to_vec(),
            Some((&self.canonical_hex, self.size)),
        )
    }

    /// Decodes from the record envelope, validating invariants and the
    /// envelope/payload digest agreement both ways.
    pub fn decode_record(bytes: &[u8]) -> Result<Self, Error> {
        let payload = decode_envelope(bytes, OCI_DIGEST_ALIAS_TYPE_URL)?;
        let alias = Self::decode(payload.as_slice()).map_err(|e| {
            make_input_err!("OCI record payload is not a valid OciDigestAlias: {e}")
        })?;
        alias.validate()?;
        // The envelope's output file must agree with the payload; a mismatch
        // means a corrupt or hand-forged record.
        let reencoded = encode_envelope(
            OCI_DIGEST_ALIAS_TYPE_URL,
            alias.encode_to_vec(),
            Some((&alias.canonical_hex, alias.size)),
        )?;
        let original = ProtoActionResult::decode(bytes)
            .map_err(|e| make_input_err!("OCI record is not a valid ActionResult envelope: {e}"))?;
        let rebuilt = ProtoActionResult::decode(reencoded.as_slice())
            .map_err(|e| make_input_err!("re-encoded OCI record failed to decode: {e}"))?;
        if original.output_files != rebuilt.output_files {
            return Err(make_input_err!(
                "OciDigestAlias envelope output file disagrees with its payload"
            ));
        }
        Ok(alias)
    }
}

impl OciTagRecord {
    /// Validates the record's invariants.
    pub fn validate(&self) -> Result<(), Error> {
        if !is_lower_hex_64(&self.manifest_sha256_hex) {
            return Err(make_input_err!(
                "OciTagRecord manifest_sha256_hex '{}' is not 64 lowercase hex characters",
                self.manifest_sha256_hex
            ));
        }
        if !is_lower_hex_64(&self.canonical_hex) {
            return Err(make_input_err!(
                "OciTagRecord canonical_hex '{}' is not 64 lowercase hex characters",
                self.canonical_hex
            ));
        }
        if !self.previous_sha256_hex.is_empty() && !is_lower_hex_64(&self.previous_sha256_hex) {
            return Err(make_input_err!(
                "OciTagRecord previous_sha256_hex '{}' is neither empty nor 64 lowercase hex characters",
                self.previous_sha256_hex
            ));
        }
        Ok(())
    }

    /// Encodes into the record envelope; the output file names the MANIFEST
    /// blob by its canonical digest, so an evicted manifest 404s the tag.
    pub fn encode_record(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode_envelope(
            OCI_TAG_RECORD_TYPE_URL,
            self.encode_to_vec(),
            Some((&self.canonical_hex, self.size)),
        )
    }

    /// Decodes from the record envelope.
    pub fn decode_record(bytes: &[u8]) -> Result<Self, Error> {
        let payload = decode_envelope(bytes, OCI_TAG_RECORD_TYPE_URL)?;
        let record = Self::decode(payload.as_slice())
            .map_err(|e| make_input_err!("OCI record payload is not a valid OciTagRecord: {e}"))?;
        record.validate()?;
        Ok(record)
    }
}

impl OciRepoTagIndex {
    /// Encodes into the record envelope. No output files: the index
    /// references no blob, so completeness checking passes vacuously.
    pub fn encode_record(&self) -> Result<Vec<u8>, Error> {
        encode_envelope(OCI_REPO_TAG_INDEX_TYPE_URL, self.encode_to_vec(), None)
    }

    /// Decodes from the record envelope.
    pub fn decode_record(bytes: &[u8]) -> Result<Self, Error> {
        let payload = decode_envelope(bytes, OCI_REPO_TAG_INDEX_TYPE_URL)?;
        Self::decode(payload.as_slice())
            .map_err(|e| make_input_err!("OCI record payload is not a valid OciRepoTagIndex: {e}"))
    }

    /// Set-union merge of a tag into the sorted index; idempotent.
    pub fn insert(&mut self, tag: &str) {
        if let Err(pos) = self.tags.binary_search_by(|t| t.as_str().cmp(tag)) {
            self.tags.insert(pos, tag.to_string());
        }
    }

    /// Removes a tag from the sorted index; idempotent.
    pub fn remove(&mut self, tag: &str) {
        if let Ok(pos) = self.tags.binary_search_by(|t| t.as_str().cmp(tag)) {
            self.tags.remove(pos);
        }
    }
}

/// One referrer entry: the descriptor of a manifest whose `subject` names
/// the record's subject digest, as served by the referrers API.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct OciReferrer {
    /// Lowercase hex sha256 (wire name) of the REFERRING manifest.
    #[prost(string, tag = "1")]
    pub manifest_sha256_hex: String,
    /// Size in bytes of the referring manifest body.
    #[prost(uint64, tag = "2")]
    pub size: u64,
    /// Media type of the referring manifest.
    #[prost(string, tag = "3")]
    pub media_type: String,
    /// The descriptor's `artifactType`: the manifest's own `artifactType`,
    /// else its `config.mediaType`; empty when neither is declared.
    #[prost(string, tag = "4")]
    pub artifact_type: String,
    /// The referring manifest's top-level `annotations`, serialized as a
    /// JSON object string; empty when absent.
    #[prost(string, tag = "5")]
    pub annotations_json: String,
}

/// The referrers of one subject digest within one repository. Mutable, the
/// tag-index discipline: idempotent set merge keyed on the referrer digest,
/// discovery-only (the authoritative manifests are in the CAS).
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct OciReferrerList {
    /// Referrer descriptors, insertion-ordered, unique by digest.
    #[prost(message, repeated, tag = "1")]
    pub referrers: Vec<OciReferrer>,
}

impl OciReferrerList {
    /// Encodes into the record envelope (no output files: served
    /// descriptors point at manifests whose own lifetime is CAS-managed).
    pub fn encode_record(&self) -> Result<Vec<u8>, Error> {
        encode_envelope(OCI_REFERRER_LIST_TYPE_URL, self.encode_to_vec(), None)
    }

    /// Decodes from the record envelope.
    pub fn decode_record(bytes: &[u8]) -> Result<Self, Error> {
        let payload = decode_envelope(bytes, OCI_REFERRER_LIST_TYPE_URL)?;
        Self::decode(payload.as_slice())
            .map_err(|e| make_input_err!("OCI record payload is not a valid OciReferrerList: {e}"))
    }

    /// Inserts (or replaces) a referrer, idempotent by manifest digest.
    pub fn upsert(&mut self, referrer: OciReferrer) {
        if let Some(existing) = self
            .referrers
            .iter_mut()
            .find(|r| r.manifest_sha256_hex == referrer.manifest_sha256_hex)
        {
            *existing = referrer;
        } else {
            self.referrers.push(referrer);
        }
    }

    /// Removes a referrer by manifest digest; idempotent.
    pub fn remove(&mut self, manifest_sha256_hex: &str) {
        self.referrers
            .retain(|r| r.manifest_sha256_hex != manifest_sha256_hex);
    }
}
