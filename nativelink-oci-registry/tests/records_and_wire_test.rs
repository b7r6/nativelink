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

//! Record-envelope and wire-vocabulary tests, falsified both ways: every
//! accepting test has a rejecting twin, so a validator that accepts
//! everything (or rejects everything) fails the suite.

use nativelink_oci_registry::records::{
    OciDigestAlias, OciRepoTagIndex, OciTagRecord, alias_key, repo_tag_index_key, tag_key,
};
use nativelink_oci_registry::wire::{
    ManifestReference, OciErrorCode, error_body, is_valid_repository_name, is_valid_tag,
    parse_digest, parse_manifest, parse_manifest_reference,
};

const HEX_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HEX_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn sample_alias() -> OciDigestAlias {
    OciDigestAlias {
        canonical_hex: HEX_A.to_string(),
        size: 42,
        sha256_hex: HEX_B.to_string(),
        media_type: String::new(),
    }
}

#[test]
fn alias_record_round_trips() {
    let alias = sample_alias();
    let bytes = alias.encode_record().expect("encode");
    let decoded = OciDigestAlias::decode_record(&bytes).expect("decode");
    assert_eq!(decoded, alias);
}

#[test]
fn alias_record_rejects_garbage() {
    assert!(OciDigestAlias::decode_record(b"not a record").is_err());
    assert!(OciDigestAlias::decode_record(&[]).is_err());
}

#[test]
fn alias_record_rejects_bad_hex() {
    let mut alias = sample_alias();
    alias.canonical_hex = "XYZ".to_string();
    assert!(alias.encode_record().is_err());
    let mut alias = sample_alias();
    alias.sha256_hex.truncate(10);
    assert!(alias.encode_record().is_err());
    // Uppercase hex is NOT canonical and must be rejected.
    let mut alias = sample_alias();
    alias.canonical_hex = alias.canonical_hex.to_uppercase();
    assert!(alias.encode_record().is_err());
}

#[test]
fn alias_record_rejects_wrong_type_url() {
    // A tag record's bytes must NOT decode as an alias record.
    let tag = OciTagRecord {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        manifest_sha256_hex: HEX_A.to_string(),
        size: 7,
        canonical_hex: HEX_B.to_string(),
        created_at_unix: 0,
        previous_sha256_hex: String::new(),
    };
    let bytes = tag.encode_record().expect("encode");
    assert!(OciDigestAlias::decode_record(&bytes).is_err());
}

#[test]
fn tag_record_round_trips_and_validates() {
    let tag = OciTagRecord {
        media_type: "application/vnd.oci.image.manifest.v1+json".to_string(),
        manifest_sha256_hex: HEX_A.to_string(),
        size: 1234,
        canonical_hex: HEX_B.to_string(),
        created_at_unix: 1_700_000_000,
        previous_sha256_hex: HEX_A.to_string(),
    };
    let bytes = tag.encode_record().expect("encode");
    assert_eq!(OciTagRecord::decode_record(&bytes).expect("decode"), tag);

    let mut bad = tag;
    bad.previous_sha256_hex = "short".to_string();
    assert!(bad.encode_record().is_err());
}

#[test]
fn repo_tag_index_merge_is_sorted_idempotent() {
    let mut index = OciRepoTagIndex::default();
    index.insert("v2");
    index.insert("latest");
    index.insert("v2"); // idempotent
    index.insert("v1");
    assert_eq!(index.tags, vec!["latest", "v1", "v2"]);
    index.remove("v1");
    index.remove("v1"); // idempotent
    assert_eq!(index.tags, vec!["latest", "v2"]);
    let bytes = index.encode_record().expect("encode");
    assert_eq!(
        OciRepoTagIndex::decode_record(&bytes).expect("decode"),
        index
    );
}

#[test]
fn store_keys_are_prefixed_and_distinct() {
    assert_eq!(alias_key(HEX_A), format!("oci-digest:sha256:{HEX_A}"));
    assert_eq!(tag_key("a/b", "latest"), "oci-tag:a/b:latest");
    assert_eq!(repo_tag_index_key("a/b"), "oci-tags:a/b");
    assert_ne!(tag_key("a", "b"), repo_tag_index_key("a"));
}

#[test]
fn repository_names_validate_both_ways() {
    for good in [
        "library/alpine",
        "a",
        "a0/b1/c2",
        "foo.bar/baz-qux",
        "with__underscores/and---dashes",
        "a.b.c",
    ] {
        assert!(is_valid_repository_name(good), "expected valid: {good}");
    }
    for bad in [
        "",
        "/leading",
        "trailing/",
        "UPPER/case",
        "double//slash",
        "dot./end",
        ".dot/start",
        "a/_b",
        "sp ace",
        "under_",
        "tri___ple",
        "a/b..c",
        "oci-tag:injected",
    ] {
        assert!(!is_valid_repository_name(bad), "expected invalid: {bad}");
    }
    assert!(!is_valid_repository_name(&"a/".repeat(200)));
}

#[test]
fn tags_validate_both_ways() {
    for good in ["latest", "v1.2.3", "_internal", "A-b_c.d", "0"] {
        assert!(is_valid_tag(good), "expected valid: {good}");
    }
    for bad in ["", ".hidden", "-lead", "has space", "has:colon", "x/y"] {
        assert!(!is_valid_tag(bad), "expected invalid: {bad}");
    }
    assert!(is_valid_tag(&"t".repeat(128)));
    assert!(!is_valid_tag(&"t".repeat(129)));
}

#[test]
fn digests_validate_both_ways() {
    assert_eq!(
        parse_digest(&format!("sha256:{HEX_A}")).expect("valid"),
        HEX_A
    );
    for bad in [
        "sha256:short",
        &format!("sha512:{HEX_A}{HEX_A}"),
        &format!("SHA256:{HEX_A}"),
        &format!("sha256:{}", HEX_A.to_uppercase()),
        HEX_A,
        "sha256:",
        "",
    ] {
        assert!(parse_digest(bad).is_err(), "expected invalid: {bad}");
    }
}

#[test]
fn manifest_references_parse_both_ways() {
    assert_eq!(
        parse_manifest_reference("latest").expect("tag"),
        ManifestReference::Tag("latest".to_string())
    );
    assert_eq!(
        parse_manifest_reference(&format!("sha256:{HEX_A}")).expect("digest"),
        ManifestReference::Digest(HEX_A.to_string())
    );
    assert!(parse_manifest_reference("sha256:oops").is_err());
    assert!(parse_manifest_reference("bad tag").is_err());
}

#[test]
fn manifest_parsing_extracts_references() {
    let body = format!(
        r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:{HEX_A}", "size": 7}},
            "layers": [{{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": "sha256:{HEX_B}", "size": 9}}]
        }}"#
    );
    let parsed = parse_manifest(body.as_bytes()).expect("parse");
    assert_eq!(
        parsed.media_type.as_deref(),
        Some("application/vnd.oci.image.manifest.v1+json")
    );
    let hexes: Vec<&str> = parsed
        .blob_references
        .iter()
        .map(|d| d.sha256_hex.as_str())
        .collect();
    assert_eq!(hexes, vec![HEX_A, HEX_B]);
    assert!(parsed.manifest_references.is_empty());
    assert!(parsed.subject_sha256_hex.is_none());
}

#[test]
fn index_parsing_extracts_children_and_subject() {
    let body = format!(
        r#"{{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [{{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:{HEX_A}", "size": 7}}],
            "subject": {{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:{HEX_B}", "size": 3}}
        }}"#
    );
    let parsed = parse_manifest(body.as_bytes()).expect("parse");
    assert!(parsed.blob_references.is_empty());
    assert_eq!(parsed.manifest_references[0].sha256_hex, HEX_A);
    assert_eq!(parsed.subject_sha256_hex.as_deref(), Some(HEX_B));
}

#[test]
fn manifest_parsing_rejects_junk() {
    assert!(parse_manifest(b"not json").is_err());
    assert!(parse_manifest(b"[]").is_err());
    // A JSON object with no references is not a manifest shape we serve.
    assert!(parse_manifest(b"{\"schemaVersion\": 2}").is_err());
    // A manifest with a malformed digest is rejected, not silently accepted.
    assert!(
        parse_manifest(br#"{"config": {"digest": "sha256:oops", "size": 1}, "layers": []}"#)
            .is_err()
    );
}

#[test]
fn error_body_is_spec_shaped() {
    let body = error_body(OciErrorCode::BlobUnknown, "nope");
    let value: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(value["errors"][0]["code"], "BLOB_UNKNOWN");
    assert_eq!(value["errors"][0]["message"], "nope");
}
