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

//! The [`NixPathInfo`] message — the durable record the Nix substituter
//! facade keeps per store path — and its `ActionResult` envelope, which
//! lets Nix path metadata live in an ordinary REAPI action cache.
//!
//! The message is defined directly with `#[derive(prost::Message)]`
//! because protobuf regeneration (`bazel run
//! nativelink-proto:update_protos`) is broken in this checkout; a
//! `.proto` mirror of [`NixPathInfo`] should land under
//! `nativelink-proto/com/github/trace_machina/nativelink/nix/` once that
//! works again. Until then, the field tags declared here are the wire
//! contract.
//!
//! Record shape (see [`NixPathInfo::encode_record`]): an `ActionResult`
//! with exactly one output file named [`NAR_OUTPUT_FILE_NAME`] whose
//! digest is the lowercase-hex NAR sha256 and NAR size, `exit_code` 0,
//! and an `ExecutedActionMetadata` carrying the prost-encoded
//! [`NixPathInfo`] as an `Any` under [`NIX_PATH_INFO_TYPE_URL`].

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use nativelink_error::{Error, ResultExt, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult as ProtoActionResult, Digest, ExecutedActionMetadata, OutputFile,
};
use prost::Message;

use crate::narinfo::{NarInfo, is_store_path_hash, is_valid_store_path_name};

/// The `Any.type_url` under which a prost-encoded [`NixPathInfo`] rides
/// in the record's `ExecutedActionMetadata.auxiliary_metadata`.
pub const NIX_PATH_INFO_TYPE_URL: &str =
    "type.googleapis.com/com.github.trace_machina.nativelink.nix.NixPathInfo";

/// The `path` of the record's single output file.
pub const NAR_OUTPUT_FILE_NAME: &str = "out.nar";

/// The `ExecutedActionMetadata.worker` value stamped on every record.
pub const NIX_CACHE_WORKER_NAME: &str = "nativelink-nix-cache";

/// Nix path metadata, independent of any particular NAR URL or
/// compression: the intersection of a `.narinfo` document and Nix's
/// `ValidPathInfo`.
///
/// Unlike [`NarInfo`], optional string fields here use the empty string
/// for "absent" (protobuf semantics), and `references` are store-path
/// basenames kept byte-lexicographically sorted.
///
/// The optional `file_*` trio (tags 9–11) describes the *compressed*
/// artifact at the served URL — `FileHash`/`FileSize` in `.narinfo`
/// terms — and is all-absent or all-present (see the field docs). The
/// optional `file_url` (tag 12) is the exact URL a preserved ORIGINAL
/// upload is served under (empty means "derive the canonical name"); it
/// may be set only when the trio is set.
/// The compressed file is deliberately NOT part of the record's
/// `ActionResult` output files: action-cache completeness must key on
/// the uncompressed NAR only, so eviction of the compressed blob
/// degrades gracefully at the handler level (fall back to serving the
/// uncompressed NAR) instead of 404ing the narinfo at the store level.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct NixPathInfo {
    /// Full store path, e.g. `/nix/store/<hash>-<name>`.
    #[prost(string, tag = "1")]
    pub store_path: String,
    /// `sha256` of the uncompressed NAR; exactly 32 bytes.
    #[prost(bytes = "vec", tag = "2")]
    pub nar_sha256: Vec<u8>,
    /// Size in bytes of the uncompressed NAR.
    #[prost(uint64, tag = "3")]
    pub nar_size: u64,
    /// References as store-path basenames, byte-lexicographically sorted.
    #[prost(string, repeated, tag = "4")]
    pub references: Vec<String>,
    /// Basename of the deriving `.drv`; empty means absent.
    #[prost(string, tag = "5")]
    pub deriver: String,
    /// Platform string, e.g. `x86_64-linux`; empty means absent.
    #[prost(string, tag = "6")]
    pub system: String,
    /// Signatures, verbatim `name:base64`.
    #[prost(string, repeated, tag = "7")]
    pub signatures: Vec<String>,
    /// Content-address for fixed-output paths; empty means absent.
    #[prost(string, tag = "8")]
    pub ca: String,
    /// `sha256` of the COMPRESSED file at the served URL; empty (absent)
    /// or exactly 32 bytes. Absent or present together with `file_size`
    /// and `file_compression`.
    #[prost(bytes = "vec", tag = "9")]
    pub file_sha256: Vec<u8>,
    /// Size in bytes of the compressed file; 0 when absent.
    #[prost(uint64, tag = "10")]
    pub file_size: u64,
    /// Compression codec of the compressed file (`zstd` for now); empty
    /// when absent.
    #[prost(string, tag = "11")]
    pub file_compression: String,
    /// The exact URL a preserved ORIGINAL compressed upload is served
    /// under (`nar/{clientFileHash}.nar.{ext}`, the verbatim `url` from
    /// the client's `.narinfo`). Empty means "derive the canonical name
    /// from the `file_*` trio". May be non-empty only when the `file_*`
    /// trio is present. Old records (no tag 12) decode with this empty.
    #[prost(string, tag = "12")]
    pub file_url: String,
}

/// Validates that `reference` is a plausible store-path basename:
/// a 32-character nix32 hash, `-`, and a non-empty name free of `/` and
/// whitespace.
fn validate_reference(reference: &str) -> Result<(), Error> {
    let (hash, rest) = reference.split_at_checked(32).ok_or_else(|| {
        make_input_err!("reference '{reference}' is too short for a 32-character nix32 hash")
    })?;
    if !is_store_path_hash(hash) {
        return Err(make_input_err!(
            "reference '{reference}' does not start with a 32-character nix32 hash"
        ));
    }
    let name = rest.strip_prefix('-').ok_or_else(|| {
        make_input_err!("reference '{reference}' lacks a '-' after the 32-character hash")
    })?;
    // Nix's `checkName`: rejects empty names, NUL and every other control
    // or out-of-charset byte, `/`, whitespace, and a leading `.`.
    if !is_valid_store_path_name(name) {
        return Err(make_input_err!(
            "reference '{reference}' has an invalid store-path name '{name}'"
        ));
    }
    Ok(())
}

/// Validates that `store_path`'s basename is
/// `<32-char nix32 hash>-<name>` with a valid
/// [`is_valid_store_path_name`] name component. This rejects NUL and
/// other out-of-charset bytes in the name a `.narinfo` `StorePath` line
/// might otherwise smuggle through.
fn validate_store_path(store_path: &str) -> Result<(), Error> {
    let basename = store_path.rsplit('/').next().unwrap_or(store_path);
    let (hash, rest) = basename.split_at_checked(32).ok_or_else(|| {
        make_input_err!(
            "store path '{store_path}' basename is too short for a 32-character nix32 hash"
        )
    })?;
    if !is_store_path_hash(hash) {
        return Err(make_input_err!(
            "store path '{store_path}' does not start with a 32-character nix32 hash"
        ));
    }
    let name = rest.strip_prefix('-').ok_or_else(|| {
        make_input_err!("store path '{store_path}' lacks a '-' after the 32-character hash")
    })?;
    if !is_valid_store_path_name(name) {
        return Err(make_input_err!(
            "store path '{store_path}' has an invalid name '{name}'"
        ));
    }
    Ok(())
}

/// Validates that `sig` has Nix's signature shape: a non-empty key name
/// free of whitespace, `:`, and base64 that decodes to a 64-byte ed25519
/// signature.
fn validate_signature(sig: &str) -> Result<(), Error> {
    let (name, sig_b64) = sig
        .split_once(':')
        .ok_or_else(|| make_input_err!("signature '{sig}' lacks a ':' separator"))?;
    if name.is_empty() || name.chars().any(char::is_whitespace) {
        return Err(make_input_err!(
            "signature '{sig}' has an empty or whitespace key name"
        ));
    }
    let sig_bytes = BASE64
        .decode(sig_b64)
        .map_err(|e| make_input_err!("signature '{sig}' has invalid base64: {e}"))?;
    if sig_bytes.len() != 64 {
        return Err(make_input_err!(
            "signature '{sig}' decodes to {} bytes, expected a 64-byte ed25519 signature",
            sig_bytes.len()
        ));
    }
    Ok(())
}

impl NixPathInfo {
    /// Returns `nar_sha256` as a fixed 32-byte digest.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidArgument` error if `nar_sha256` is not exactly
    /// 32 bytes.
    fn nar_hash(&self) -> Result<[u8; 32], Error> {
        self.nar_sha256.as_slice().try_into().map_err(|_| {
            make_input_err!(
                "NixPathInfo nar_sha256 is {} bytes, expected 32",
                self.nar_sha256.len()
            )
        })
    }

    /// Validates the all-or-none invariant of the `file_*` trio: either
    /// all three are absent (empty hash, zero size, empty compression)
    /// or all three are present with a 32-byte `file_sha256`, a nonzero
    /// `file_size`, and a non-empty `file_compression`. `file_url` is an
    /// optional adjunct that may be set only when the trio is present.
    ///
    /// # Errors
    ///
    /// Returns a distinct `InvalidArgument` error for each violation:
    /// a `file_url` set without the trio, `file_size`/`file_compression`
    /// present without `file_sha256`, a `file_sha256` that is not exactly
    /// 32 bytes, a `file_sha256` without a `file_size`, or a `file_sha256`
    /// without a `file_compression`.
    fn validate_file_fields(&self) -> Result<(), Error> {
        let has_hash = !self.file_sha256.is_empty();
        let has_size = self.file_size != 0;
        let has_compression = !self.file_compression.is_empty();
        if !has_hash && !has_size && !has_compression {
            if !self.file_url.is_empty() {
                return Err(make_input_err!(
                    "NixPathInfo has a file_url but no file_sha256/file_size/file_compression; file_url requires the file_* trio"
                ));
            }
            return Ok(());
        }
        if !has_hash {
            return Err(make_input_err!(
                "NixPathInfo has a file_size or file_compression but an empty file_sha256; the file_* fields are all-or-none"
            ));
        }
        if self.file_sha256.len() != 32 {
            return Err(make_input_err!(
                "NixPathInfo file_sha256 is {} bytes, expected 32",
                self.file_sha256.len()
            ));
        }
        if !has_size {
            return Err(make_input_err!(
                "NixPathInfo has a file_sha256 but a zero file_size; the file_* fields are all-or-none"
            ));
        }
        if !has_compression {
            return Err(make_input_err!(
                "NixPathInfo has a file_sha256 but an empty file_compression; the file_* fields are all-or-none"
            ));
        }
        Ok(())
    }

    /// Validates the store path, every reference, and every signature —
    /// the single choke point shared by [`Self::to_nar_info`] (the render
    /// path) and [`Self::encode_record`] (the persist path), so a record
    /// that cannot be rendered can never be stored in the first place.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidArgument` error if the store-path name, any
    /// reference, or any signature is malformed.
    fn validate_store_path_references_and_signatures(&self) -> Result<(), Error> {
        validate_store_path(&self.store_path)
            .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        for reference in &self.references {
            validate_reference(reference)
                .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        }
        for sig in &self.signatures {
            validate_signature(sig)
                .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        }
        Ok(())
    }

    /// Extracts the URL- and compression-independent metadata of a parsed
    /// `.narinfo` document. References are sorted; `FileHash`/`FileSize`
    /// (properties of the *upstream's* compressed NAR, not ours) are
    /// dropped, so the `file_*` trio starts out absent.
    #[must_use]
    pub fn from_nar_info(info: &NarInfo) -> Self {
        let mut references = info.references.clone();
        references.sort_unstable();
        Self {
            store_path: info.store_path.clone(),
            nar_sha256: info.nar_hash.to_vec(),
            nar_size: info.nar_size,
            references,
            deriver: info.deriver.clone().unwrap_or_default(),
            system: info.system.clone().unwrap_or_default(),
            signatures: info.sigs.clone(),
            ca: info.ca.clone().unwrap_or_default(),
            file_sha256: Vec::new(),
            file_size: 0,
            file_compression: String::new(),
            file_url: String::new(),
        }
    }

    /// Builds a renderable [`NarInfo`] pointing at the NAR served under
    /// `url` with `compression`, after validating this message's shape.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidArgument` error if `nar_sha256` is not 32
    /// bytes, `nar_size` is zero, any reference is not a store-path
    /// basename, any signature is not `name:base64(64 bytes)`, or the
    /// `file_*` trio violates its all-or-none invariant (see
    /// [`Self::validate_file_fields`]).
    pub fn to_nar_info(&self, url: String, compression: String) -> Result<NarInfo, Error> {
        let nar_hash = self
            .nar_hash()
            .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        if self.nar_size == 0 {
            return Err(make_input_err!(
                "NixPathInfo for '{}' has a zero nar_size",
                self.store_path
            ));
        }
        self.validate_file_fields()
            .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        self.validate_store_path_references_and_signatures()?;
        Ok(NarInfo {
            store_path: self.store_path.clone(),
            url,
            compression,
            file_hash: None,
            file_size: None,
            nar_hash,
            nar_size: self.nar_size,
            references: self.references.clone(),
            deriver: (!self.deriver.is_empty()).then(|| self.deriver.clone()),
            system: (!self.system.is_empty()).then(|| self.system.clone()),
            sigs: self.signatures.clone(),
            ca: (!self.ca.is_empty()).then(|| self.ca.clone()),
        })
    }

    /// The signing fingerprint of this path, per
    /// [`crate::narinfo::fingerprint`]; reference basenames get the store
    /// directory derived from `store_path`.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidArgument` error if `nar_sha256` is not exactly
    /// 32 bytes.
    pub fn fingerprint(&self) -> Result<String, Error> {
        let nar_hash = self
            .nar_hash()
            .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        Ok(crate::narinfo::fingerprint(
            &self.store_path,
            &nar_hash,
            self.nar_size,
            &self.references,
        ))
    }

    /// Wraps this message in its `ActionResult` record envelope and
    /// prost-encodes it; [`Self::decode_record`] is the exact inverse.
    ///
    /// The record's single output file is always the uncompressed NAR;
    /// the `file_*` trio rides only inside the embedded message, so a
    /// record's action-cache completeness never depends on the
    /// compressed blob.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidArgument` error if `nar_sha256` is not 32
    /// bytes, `nar_size` exceeds `i64::MAX` (the range of a `Digest`'s
    /// `size_bytes`), or the `file_*` trio violates its all-or-none
    /// invariant (see [`Self::validate_file_fields`]).
    pub fn encode_record(&self) -> Result<Vec<u8>, Error> {
        let nar_hash = self
            .nar_hash()
            .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        self.validate_file_fields()
            .err_tip(|| format!("in NixPathInfo for '{}'", self.store_path))?;
        // Same reference/signature/store-path checks `to_nar_info` runs, so
        // an unrenderable record is rejected at PUT (400) instead of being
        // stored and then 500ing forever on GET.
        self.validate_store_path_references_and_signatures()?;
        let size_bytes = i64::try_from(self.nar_size).map_err(|e| {
            make_input_err!(
                "NixPathInfo nar_size {} does not fit in a Digest's i64 size_bytes: {e}",
                self.nar_size
            )
        })?;
        let record = ProtoActionResult {
            output_files: vec![OutputFile {
                path: NAR_OUTPUT_FILE_NAME.to_string(),
                digest: Some(Digest {
                    hash: hex::encode(nar_hash),
                    size_bytes,
                }),
                ..Default::default()
            }],
            exit_code: 0,
            execution_metadata: Some(ExecutedActionMetadata {
                worker: NIX_CACHE_WORKER_NAME.to_string(),
                auxiliary_metadata: vec![prost_types::Any {
                    type_url: NIX_PATH_INFO_TYPE_URL.to_string(),
                    value: self.encode_to_vec(),
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(record.encode_to_vec())
    }

    /// Decodes and validates an `ActionResult` record produced by
    /// [`Self::encode_record`].
    ///
    /// # Errors
    ///
    /// Returns a distinct `InvalidArgument` error for each shape
    /// violation: undecodable `ActionResult`, nonzero exit code, not
    /// exactly one output file, output file not named
    /// [`NAR_OUTPUT_FILE_NAME`], missing digest, digest hash that is not
    /// 64 lowercase hex characters, missing execution metadata, no
    /// auxiliary metadata `Any` under [`NIX_PATH_INFO_TYPE_URL`],
    /// undecodable [`NixPathInfo`], embedded `nar_sha256` that is not 32
    /// bytes, an embedded `file_*` trio that violates its all-or-none
    /// invariant (see [`Self::validate_file_fields`]), or a digest
    /// hash/size that contradicts the embedded message.
    ///
    /// Older records (encoded before the `file_*` fields or `file_url`
    /// existed) decode fine: prost defaults leave the trio all-absent and
    /// `file_url` empty.
    pub fn decode_record(bytes: &[u8]) -> Result<Self, Error> {
        let record = ProtoActionResult::decode(bytes).map_err(|e| {
            make_input_err!("nix path-info record is not a valid ActionResult: {e}")
        })?;
        if record.exit_code != 0 {
            return Err(make_input_err!(
                "nix path-info record has nonzero exit_code {}",
                record.exit_code
            ));
        }
        let [output_file] = record.output_files.as_slice() else {
            return Err(make_input_err!(
                "nix path-info record has {} output files, expected exactly one",
                record.output_files.len()
            ));
        };
        if output_file.path != NAR_OUTPUT_FILE_NAME {
            return Err(make_input_err!(
                "nix path-info record output file is named '{}', expected '{NAR_OUTPUT_FILE_NAME}'",
                output_file.path
            ));
        }
        let digest = output_file.digest.as_ref().ok_or_else(|| {
            make_input_err!("nix path-info record output file is missing its digest")
        })?;
        if digest.hash.len() != 64
            || !digest
                .hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(make_input_err!(
                "nix path-info record digest hash '{}' is not 64 lowercase hex characters",
                digest.hash
            ));
        }
        let metadata = record
            .execution_metadata
            .as_ref()
            .ok_or_else(|| make_input_err!("nix path-info record is missing execution_metadata"))?;
        let any = metadata
            .auxiliary_metadata
            .iter()
            .find(|any| any.type_url == NIX_PATH_INFO_TYPE_URL)
            .ok_or_else(|| {
                make_input_err!(
                    "nix path-info record has no auxiliary_metadata with type_url '{NIX_PATH_INFO_TYPE_URL}'"
                )
            })?;
        let info = Self::decode(any.value.as_slice()).map_err(|e| {
            make_input_err!(
                "nix path-info record auxiliary_metadata is not a valid NixPathInfo: {e}"
            )
        })?;
        let nar_hash = info
            .nar_hash()
            .err_tip(|| "in the NixPathInfo embedded in a nix path-info record")?;
        info.validate_file_fields()
            .err_tip(|| "in the NixPathInfo embedded in a nix path-info record")?;
        let expected_hash = hex::encode(nar_hash);
        if digest.hash != expected_hash {
            return Err(make_input_err!(
                "nix path-info record digest hash '{}' does not match the embedded nar_sha256 '{expected_hash}'",
                digest.hash
            ));
        }
        if u64::try_from(digest.size_bytes).ok() != Some(info.nar_size) {
            return Err(make_input_err!(
                "nix path-info record digest size {} does not match the embedded nar_size {}",
                digest.size_bytes,
                info.nar_size
            ));
        }
        Ok(info)
    }
}

#[cfg(test)]
mod tests {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        ActionResult as ProtoActionResult, OutputFile,
    };
    use prost::Message;

    use super::{NAR_OUTPUT_FILE_NAME, NIX_CACHE_WORKER_NAME, NIX_PATH_INFO_TYPE_URL, NixPathInfo};
    use crate::narinfo::parse;

    /// The same REAL `nix copy` capture (nix 2.34.7) used as
    /// `NIX_CLI_NARINFO` in the `narinfo` module's tests; see there for
    /// provenance.
    const NIX_CLI_NARINFO: &str = concat!(
        "StorePath: /nix/store/bvkx110ylicifcgl0xiid5f100hx3ar7-nix-2.34.7\n",
        "URL: nar/11by0lzb61psglb8810552qb8v569x7y8q9gcj9p1dadpv9q0jqw.nar.xz\n",
        "Compression: xz\n",
        "FileHash: sha256:11by0lzb61psglb8810552qb8v569x7y8q9gcj9p1dadpv9q0jqw\n",
        "FileSize: 772\n",
        "NarHash: sha256:1ip47yiybcjh26ijapy13qzg8mzvypr2qci2ixx5jkm3b76r0bk9\n",
        "NarSize: 8848\n",
        "References: 97zxp9j00zcjmkn3zv9karhwj86q7x5w-nix-nswrapper-2.34.7",
        " hqwkw2nala59avjximpdmn1yi474n4h7-nix-2.34.7\n",
        "Deriver: q0hpd8s75g1h17yr8zqp1yf8sc9g4gp2-nix-2.34.7.drv\n",
        "Sig: cache.nixos.org-1:RPgjMEzWvBICkTl8Usg+ppxnHbBNqCtp5iIy5FFPZzxn2reRAb3ffPwpP",
        "zrARfK1NWO2g4nIQkfMyzsd2J0pAw==\n",
    );

    /// See `narinfo::tests::NIX_CLI_NARINFO` for the base16 conversion.
    const NIX_CLI_NAR_HASH_HEX: &str =
        "692e90cd59a34e597a8f22322cf2f5fb57f43e1ec15f25a31150b2e5a33fe4c6";

    fn sample_info() -> NixPathInfo {
        let info = parse(NIX_CLI_NARINFO).expect("parse fixture");
        NixPathInfo::from_nar_info(&info)
    }

    /// The fixture with the `file_*` trio present, as if the facade had
    /// recompressed the NAR to zstd. The hash is sha256("hello") — any
    /// 32-byte value works; validation only checks the length.
    fn sample_info_with_file_fields() -> NixPathInfo {
        let mut info = sample_info();
        info.file_sha256 =
            hex::decode("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
                .expect("valid hex");
        info.file_size = 772;
        info.file_compression = "zstd".to_string();
        info
    }

    fn sample_record() -> ProtoActionResult {
        let bytes = sample_info().encode_record().expect("encode record");
        ProtoActionResult::decode(bytes.as_slice()).expect("decode as ActionResult")
    }

    fn decode_err(record: &ProtoActionResult) -> String {
        NixPathInfo::decode_record(record.encode_to_vec().as_slice())
            .expect_err("expected shape violation")
            .to_string()
    }

    #[test]
    fn from_nar_info_maps_fields_and_sorts_references() {
        let parsed = parse(NIX_CLI_NARINFO).expect("parse fixture");
        let info = NixPathInfo::from_nar_info(&parsed);
        assert_eq!(
            info.store_path,
            "/nix/store/bvkx110ylicifcgl0xiid5f100hx3ar7-nix-2.34.7"
        );
        assert_eq!(hex::encode(&info.nar_sha256), NIX_CLI_NAR_HASH_HEX);
        assert_eq!(info.nar_size, 8848);
        assert_eq!(
            info.references,
            vec![
                "97zxp9j00zcjmkn3zv9karhwj86q7x5w-nix-nswrapper-2.34.7".to_string(),
                "hqwkw2nala59avjximpdmn1yi474n4h7-nix-2.34.7".to_string(),
            ]
        );
        assert_eq!(
            info.deriver,
            "q0hpd8s75g1h17yr8zqp1yf8sc9g4gp2-nix-2.34.7.drv"
        );
        assert_eq!(info.system, "");
        assert_eq!(info.signatures.len(), 1);
        assert_eq!(info.ca, "");

        // References arrive sorted even if the source was not.
        let mut reversed = parsed;
        reversed.references.reverse();
        assert_eq!(NixPathInfo::from_nar_info(&reversed), info);
    }

    #[test]
    fn to_nar_info_render_parse_from_nar_info_is_a_fixed_point() {
        let parsed = parse(NIX_CLI_NARINFO).expect("parse fixture");
        let info = NixPathInfo::from_nar_info(&parsed);
        let rebuilt = info
            .to_nar_info(parsed.url.clone(), parsed.compression.clone())
            .expect("to_nar_info");
        // FileHash/FileSize are properties of one compressed NAR and are
        // deliberately dropped; everything else survives.
        assert_eq!(rebuilt.file_hash, None);
        assert_eq!(rebuilt.file_size, None);
        assert_eq!(rebuilt.store_path, parsed.store_path);
        assert_eq!(rebuilt.references, parsed.references);
        assert_eq!(rebuilt.deriver, parsed.deriver);
        assert_eq!(rebuilt.sigs, parsed.sigs);
        let reparsed = parse(&rebuilt.render()).expect("re-parse rendered narinfo");
        assert_eq!(reparsed, rebuilt);
        assert_eq!(NixPathInfo::from_nar_info(&reparsed), info);
        // The fingerprint survives the round trip, so the original
        // cache.nixos.org signature stays valid.
        assert_eq!(
            info.fingerprint().expect("fingerprint"),
            parsed.fingerprint()
        );
        assert_eq!(reparsed.fingerprint(), parsed.fingerprint());
    }

    #[test]
    fn to_nar_info_maps_empty_strings_to_absent_fields() {
        let mut info = sample_info();
        info.deriver = String::new();
        info.system = "x86_64-linux".to_string();
        info.ca = "fixed:sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73".to_string();
        let nar_info = info
            .to_nar_info("nar/x.nar".to_string(), "none".to_string())
            .expect("to_nar_info");
        assert_eq!(nar_info.url, "nar/x.nar");
        assert_eq!(nar_info.compression, "none");
        assert_eq!(nar_info.deriver, None);
        assert_eq!(nar_info.system.as_deref(), Some("x86_64-linux"));
        assert_eq!(
            nar_info.ca.as_deref(),
            Some("fixed:sha256:0mdqa9w1p6cmli6976v4wi0sw9r4p5prkj7lzfd1877wk11c9c73")
        );
    }

    #[test]
    fn to_nar_info_rejects_bad_shapes() {
        let assert_rejects = |mutate: &dyn Fn(&mut NixPathInfo), what: &str| {
            let mut info = sample_info();
            mutate(&mut info);
            assert!(
                info.to_nar_info("nar/x.nar".to_string(), "none".to_string())
                    .is_err(),
                "expected rejection of {what}"
            );
        };
        assert_rejects(&|i| i.nar_sha256.truncate(31), "31-byte nar_sha256");
        assert_rejects(&|i| i.nar_sha256.push(0), "33-byte nar_sha256");
        assert_rejects(&|i| i.nar_size = 0, "zero nar_size");
        // References must be store-path basenames.
        assert_rejects(
            &|i| i.references.push("no-hash-prefix".to_string()),
            "short reference",
        );
        assert_rejects(
            &|i| {
                i.references
                    .push("evkx110ylicifcgl0xiid5f100hx3ar7-x".to_string());
            },
            "reference hash outside the nix32 alphabet",
        );
        assert_rejects(
            &|i| {
                i.references
                    .push("bvkx110ylicifcgl0xiid5f100hx3ar7x".to_string());
            },
            "reference without '-' after the hash",
        );
        assert_rejects(
            &|i| {
                i.references
                    .push("bvkx110ylicifcgl0xiid5f100hx3ar7-".to_string());
            },
            "reference with an empty name",
        );
        assert_rejects(
            &|i| {
                i.references
                    .push("bvkx110ylicifcgl0xiid5f100hx3ar7-a/b".to_string());
            },
            "reference containing '/'",
        );
        assert_rejects(
            &|i| {
                i.references
                    .push("bvkx110ylicifcgl0xiid5f100hx3ar7-a b".to_string());
            },
            "reference containing whitespace",
        );
        // Signatures must be name:base64(64 bytes).
        assert_rejects(
            &|i| i.signatures.push("no-colon".to_string()),
            "signature without ':'",
        );
        assert_rejects(
            &|i| i.signatures.push(":RPgjMEzWvBICkTl8Usg+cQ==".to_string()),
            "signature with an empty key name",
        );
        assert_rejects(
            &|i| {
                i.signatures
                    .push("bad name:RPgjMEzWvBICkTl8Usg+cQ==".to_string());
            },
            "signature with whitespace in the key name",
        );
        assert_rejects(
            &|i| i.signatures.push("key-1:!!!not-base64!!!".to_string()),
            "signature with invalid base64",
        );
        assert_rejects(
            &|i| i.signatures.push("key-1:aGVsbG8=".to_string()),
            "signature that is not 64 bytes",
        );
    }

    #[test]
    fn fingerprint_matches_narinfo_and_rejects_bad_hash() {
        let parsed = parse(NIX_CLI_NARINFO).expect("parse fixture");
        let info = NixPathInfo::from_nar_info(&parsed);
        assert_eq!(
            info.fingerprint().expect("fingerprint"),
            parsed.fingerprint()
        );
        let mut broken = info;
        broken.nar_sha256.truncate(31);
        assert!(broken.fingerprint().is_err());
    }

    #[test]
    fn record_round_trips() {
        let info = sample_info();
        let bytes = info.encode_record().expect("encode record");
        let decoded = NixPathInfo::decode_record(&bytes).expect("decode record");
        assert_eq!(decoded, info);
    }

    #[test]
    fn record_envelope_has_the_documented_shape() {
        let record = sample_record();
        assert_eq!(record.exit_code, 0);
        let [output_file] = record.output_files.as_slice() else {
            panic!("expected exactly one output file");
        };
        assert_eq!(output_file.path, NAR_OUTPUT_FILE_NAME);
        let digest = output_file.digest.as_ref().expect("digest present");
        assert_eq!(digest.hash, NIX_CLI_NAR_HASH_HEX);
        assert_eq!(digest.size_bytes, 8848);
        let metadata = record
            .execution_metadata
            .as_ref()
            .expect("metadata present");
        assert_eq!(metadata.worker, NIX_CACHE_WORKER_NAME);
        assert_eq!(metadata.auxiliary_metadata.len(), 1);
        assert_eq!(
            metadata.auxiliary_metadata[0].type_url,
            NIX_PATH_INFO_TYPE_URL
        );
    }

    #[test]
    fn encode_record_rejects_bad_hash_and_oversized_nar() {
        let mut info = sample_info();
        info.nar_sha256.truncate(31);
        assert!(info.encode_record().is_err());
        let mut oversized = sample_info();
        oversized.nar_size = u64::MAX;
        assert!(oversized.encode_record().is_err());
    }

    #[test]
    fn encode_record_and_to_nar_info_reject_the_same_shapes() {
        // Every mutation that makes `to_nar_info` fail must ALSO make
        // `encode_record` fail, so an unrenderable record can never be
        // persisted (the PUT/GET validation-asymmetry fix).
        let cases: [FileFieldViolation; 6] = [
            (
                |i| i.references.push("no-hash-prefix".to_string()),
                "short reference",
            ),
            (
                |i| {
                    i.references
                        .push("bvkx110ylicifcgl0xiid5f100hx3ar7-a/b".to_string());
                },
                "reference with '/'",
            ),
            (
                // NUL byte in an otherwise valid-length reference name.
                |i| {
                    i.references
                        .push("bvkx110ylicifcgl0xiid5f100hx3ar7-na\0me".to_string());
                },
                "reference with NUL",
            ),
            (
                |i| i.signatures.push("no-colon".to_string()),
                "signature without ':'",
            ),
            (
                |i| i.signatures.push("key-1:aGVsbG8=".to_string()),
                "signature that is not 64 bytes",
            ),
            (
                // NUL byte in the store-path name.
                |i| i.store_path = "/nix/store/bvkx110ylicifcgl0xiid5f100hx3ar7-na\0me".to_string(),
                "store path with NUL in name",
            ),
        ];
        for (mutate, what) in cases {
            let mut info = sample_info();
            mutate(&mut info);
            assert!(
                info.encode_record().is_err(),
                "encode_record must reject {what}"
            );
            assert!(
                info.to_nar_info("nar/x.nar".to_string(), "none".to_string())
                    .is_err(),
                "to_nar_info must reject {what}"
            );
        }
    }

    #[test]
    fn decode_record_rejects_undecodable_bytes() {
        // 0xff is field 31 with wire type 7, which does not exist.
        let err = NixPathInfo::decode_record(&[0xff])
            .expect_err("garbage bytes")
            .to_string();
        assert!(err.contains("not a valid ActionResult"), "got: {err}");
    }

    #[test]
    fn decode_record_rejects_nonzero_exit_code() {
        let mut record = sample_record();
        record.exit_code = 1;
        assert!(decode_err(&record).contains("nonzero exit_code"));
    }

    #[test]
    fn decode_record_rejects_wrong_output_file_count() {
        let mut record = sample_record();
        let extra = record.output_files[0].clone();
        record.output_files.push(extra);
        assert!(decode_err(&record).contains("expected exactly one"));
        record.output_files.clear();
        assert!(decode_err(&record).contains("expected exactly one"));
    }

    #[test]
    fn decode_record_rejects_misnamed_output_file() {
        let mut record = sample_record();
        record.output_files[0].path = "out.tar".to_string();
        assert!(decode_err(&record).contains("expected 'out.nar'"));
    }

    #[test]
    fn decode_record_rejects_missing_digest() {
        let mut record = sample_record();
        record.output_files[0].digest = None;
        assert!(decode_err(&record).contains("missing its digest"));
    }

    #[test]
    fn decode_record_rejects_non_lowercase_hex_digest() {
        let assert_bad_hash = |hash: &str| {
            let mut record = sample_record();
            let digest = record.output_files[0].digest.as_mut().expect("digest");
            digest.hash = hash.to_string();
            assert!(
                decode_err(&record).contains("not 64 lowercase hex"),
                "for hash '{hash}'"
            );
        };
        assert_bad_hash(&NIX_CLI_NAR_HASH_HEX.to_uppercase());
        assert_bad_hash(&NIX_CLI_NAR_HASH_HEX[..63]);
        assert_bad_hash(&format!("{NIX_CLI_NAR_HASH_HEX}aa"));
        assert_bad_hash(&format!("g{}", &NIX_CLI_NAR_HASH_HEX[1..]));
        assert_bad_hash("");
    }

    #[test]
    fn decode_record_rejects_missing_execution_metadata() {
        let mut record = sample_record();
        record.execution_metadata = None;
        assert!(decode_err(&record).contains("missing execution_metadata"));
    }

    #[test]
    fn decode_record_rejects_missing_or_foreign_auxiliary_metadata() {
        let mut record = sample_record();
        record
            .execution_metadata
            .as_mut()
            .expect("metadata")
            .auxiliary_metadata[0]
            .type_url = "type.googleapis.com/something.else.Entirely".to_string();
        assert!(decode_err(&record).contains("no auxiliary_metadata with type_url"));
        record
            .execution_metadata
            .as_mut()
            .expect("metadata")
            .auxiliary_metadata
            .clear();
        assert!(decode_err(&record).contains("no auxiliary_metadata with type_url"));
    }

    #[test]
    fn decode_record_rejects_undecodable_embedded_message() {
        let mut record = sample_record();
        let metadata = record.execution_metadata.as_mut().expect("metadata");
        metadata.auxiliary_metadata[0].value = vec![0xff];
        assert!(decode_err(&record).contains("not a valid NixPathInfo"));
    }

    #[test]
    fn decode_record_rejects_embedded_hash_of_wrong_length() {
        let mut inner = sample_info();
        inner.nar_sha256.truncate(31);
        let mut record = sample_record();
        let metadata = record.execution_metadata.as_mut().expect("metadata");
        metadata.auxiliary_metadata[0].value = inner.encode_to_vec();
        assert!(decode_err(&record).contains("expected 32"));
    }

    #[test]
    fn decode_record_rejects_digest_hash_mismatch() {
        let mut record = sample_record();
        let digest = record.output_files[0].digest.as_mut().expect("digest");
        digest.hash = digest.hash.replace('6', "7");
        assert!(decode_err(&record).contains("does not match the embedded nar_sha256"));
    }

    #[test]
    fn decode_record_rejects_digest_size_mismatch() {
        let mut record = sample_record();
        let digest = record.output_files[0].digest.as_mut().expect("digest");
        digest.size_bytes += 1;
        assert!(decode_err(&record).contains("does not match the embedded nar_size"));
        let negative = record.output_files[0].digest.as_mut().expect("digest");
        negative.size_bytes = -1;
        assert!(decode_err(&record).contains("does not match the embedded nar_size"));
    }

    /// A mirror of the phase-1 `NixPathInfo` (tags 1–8 only), used to
    /// prove that records encoded before the `file_*` fields existed
    /// still decode.
    #[derive(Clone, PartialEq, prost::Message)]
    struct Phase1NixPathInfo {
        #[prost(string, tag = "1")]
        store_path: String,
        #[prost(bytes = "vec", tag = "2")]
        nar_sha256: Vec<u8>,
        #[prost(uint64, tag = "3")]
        nar_size: u64,
        #[prost(string, repeated, tag = "4")]
        references: Vec<String>,
        #[prost(string, tag = "5")]
        deriver: String,
        #[prost(string, tag = "6")]
        system: String,
        #[prost(string, repeated, tag = "7")]
        signatures: Vec<String>,
        #[prost(string, tag = "8")]
        ca: String,
    }

    /// A mutation that breaks the `file_*` trio, paired with a fragment
    /// of the expected error message.
    type FileFieldViolation = (fn(&mut NixPathInfo), &'static str);

    /// Re-embeds `inner` (already prost-encoded) into the fixture's
    /// otherwise-valid record envelope and decodes it.
    fn decode_with_embedded(inner: Vec<u8>) -> Result<NixPathInfo, String> {
        let mut record = sample_record();
        let metadata = record.execution_metadata.as_mut().expect("metadata");
        metadata.auxiliary_metadata[0].value = inner;
        NixPathInfo::decode_record(record.encode_to_vec().as_slice()).map_err(|e| e.to_string())
    }

    #[test]
    fn file_fields_absent_record_round_trips_with_empty_trio() {
        let info = sample_info();
        assert!(info.file_sha256.is_empty());
        assert_eq!(info.file_size, 0);
        assert!(info.file_compression.is_empty());
        let bytes = info.encode_record().expect("encode record");
        let decoded = NixPathInfo::decode_record(&bytes).expect("decode record");
        assert_eq!(decoded, info);
        assert!(decoded.file_sha256.is_empty());
        assert_eq!(decoded.file_size, 0);
        assert!(decoded.file_compression.is_empty());
    }

    #[test]
    fn file_fields_present_record_round_trips() {
        let info = sample_info_with_file_fields();
        let bytes = info.encode_record().expect("encode record");
        let decoded = NixPathInfo::decode_record(&bytes).expect("decode record");
        assert_eq!(decoded, info);
        assert_eq!(decoded.file_size, 772);
        assert_eq!(decoded.file_compression, "zstd");
        // The trio never joins the output files: the envelope still has
        // exactly one output file, keyed on the uncompressed NAR.
        let record = ProtoActionResult::decode(bytes.as_slice()).expect("decode as ActionResult");
        let [output_file] = record.output_files.as_slice() else {
            panic!("expected exactly one output file");
        };
        assert_eq!(output_file.path, NAR_OUTPUT_FILE_NAME);
        let digest = output_file.digest.as_ref().expect("digest present");
        assert_eq!(digest.hash, NIX_CLI_NAR_HASH_HEX);
        assert_eq!(digest.size_bytes, 8848);
        // to_nar_info accepts the valid trio too.
        assert!(
            info.to_nar_info("nar/x.nar.zst".to_string(), "zstd".to_string())
                .is_ok()
        );
    }

    #[test]
    fn file_url_round_trips_with_the_trio_present() {
        let mut info = sample_info_with_file_fields();
        info.file_compression = "xz".to_string();
        info.file_url =
            "nar/11by0lzb61psglb8810552qb8v569x7y8q9gcj9p1dadpv9q0jqw.nar.xz".to_string();
        let bytes = info.encode_record().expect("encode record");
        let decoded = NixPathInfo::decode_record(&bytes).expect("decode record");
        assert_eq!(decoded, info);
        assert_eq!(
            decoded.file_url,
            "nar/11by0lzb61psglb8810552qb8v569x7y8q9gcj9p1dadpv9q0jqw.nar.xz"
        );
        // A preserved-original record renders under its exact client URL.
        let rendered = info
            .to_nar_info(info.file_url.clone(), info.file_compression.clone())
            .expect("to_nar_info");
        assert_eq!(rendered.url, info.file_url);
        assert_eq!(rendered.compression, "xz");
    }

    #[test]
    fn file_url_without_the_trio_is_rejected_everywhere() {
        // A file_url with no file_* trio violates the all-or-none rule on
        // every path that runs `validate_file_fields`.
        let mut info = sample_info();
        info.file_url = "nar/deadbeef.nar.xz".to_string();
        let encode_msg = info
            .encode_record()
            .expect_err("encode_record accepted a stray file_url")
            .to_string();
        assert!(
            encode_msg.contains("file_url requires"),
            "got: {encode_msg}"
        );
        let nar_info_msg = info
            .to_nar_info("nar/x.nar".to_string(), "none".to_string())
            .expect_err("to_nar_info accepted a stray file_url")
            .to_string();
        assert!(
            nar_info_msg.contains("file_url requires"),
            "got: {nar_info_msg}"
        );
        let decode_msg = decode_with_embedded(info.encode_to_vec())
            .expect_err("decode_record accepted a stray file_url");
        assert!(
            decode_msg.contains("file_url requires"),
            "got: {decode_msg}"
        );
    }

    #[test]
    fn record_without_file_url_decodes_with_empty_url() {
        // The trio-present fixture leaves file_url empty; it must survive a
        // round trip empty (the "derive the canonical name" sentinel).
        let info = sample_info_with_file_fields();
        assert!(info.file_url.is_empty());
        let bytes = info.encode_record().expect("encode record");
        let decoded = NixPathInfo::decode_record(&bytes).expect("decode record");
        assert!(decoded.file_url.is_empty());
    }

    #[test]
    fn file_fields_all_or_none_violations_are_rejected_everywhere() {
        let violations: [FileFieldViolation; 6] = [
            (
                |i| i.file_sha256.truncate(31),
                "file_sha256 is 31 bytes, expected 32",
            ),
            (
                |i| i.file_sha256.push(0),
                "file_sha256 is 33 bytes, expected 32",
            ),
            (|i| i.file_size = 0, "a zero file_size"),
            (
                |i| i.file_compression = String::new(),
                "an empty file_compression",
            ),
            (
                |i| {
                    i.file_sha256 = Vec::new();
                    i.file_size = 0;
                },
                "an empty file_sha256",
            ),
            (
                |i| {
                    i.file_sha256 = Vec::new();
                    i.file_compression = String::new();
                },
                "an empty file_sha256",
            ),
        ];
        for (mutate, expected) in violations {
            let mut info = sample_info_with_file_fields();
            mutate(&mut info);
            // encode_record path.
            let encode_msg = info
                .encode_record()
                .expect_err("encode_record accepted a file_* violation")
                .to_string();
            assert!(encode_msg.contains(expected), "got: {encode_msg}");
            // to_nar_info path.
            let nar_info_msg = info
                .to_nar_info("nar/x.nar".to_string(), "none".to_string())
                .expect_err("to_nar_info accepted a file_* violation")
                .to_string();
            assert!(nar_info_msg.contains(expected), "got: {nar_info_msg}");
            // decode_record path: prost happily encodes the bad message,
            // so smuggle it into an otherwise-valid envelope.
            let decode_msg = decode_with_embedded(info.encode_to_vec())
                .expect_err("decode_record accepted a file_* violation");
            assert!(decode_msg.contains(expected), "got: {decode_msg}");
        }
    }

    #[test]
    fn phase1_record_without_file_fields_decodes_with_empty_trio() {
        let full = sample_info();
        let phase1 = Phase1NixPathInfo {
            store_path: full.store_path.clone(),
            nar_sha256: full.nar_sha256.clone(),
            nar_size: full.nar_size,
            references: full.references.clone(),
            deriver: full.deriver.clone(),
            system: full.system.clone(),
            signatures: full.signatures.clone(),
            ca: full.ca.clone(),
        };
        let decoded = decode_with_embedded(phase1.encode_to_vec()).expect("decode phase-1 record");
        assert_eq!(decoded, full);
        assert!(decoded.file_sha256.is_empty());
        assert_eq!(decoded.file_size, 0);
        assert!(decoded.file_compression.is_empty());
        // And byte-for-byte: a message with the trio absent encodes
        // identically to its phase-1 form, so phase-2 writers do not
        // perturb phase-1 readers either.
        assert_eq!(phase1.encode_to_vec(), full.encode_to_vec());
    }

    #[test]
    fn decode_record_ignores_extra_output_file_fields() {
        // is_executable and node_properties are irrelevant to the shape
        // checks; only path/digest matter.
        let mut record = sample_record();
        let OutputFile { is_executable, .. } = &mut record.output_files[0];
        *is_executable = true;
        let decoded =
            NixPathInfo::decode_record(record.encode_to_vec().as_slice()).expect("decode record");
        assert_eq!(decoded, sample_info());
    }
}
