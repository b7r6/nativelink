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

use nativelink_macro::nativelink_test;
use nativelink_util::digest_hasher::{
    DigestHasherFunc, default_digest_hasher_func, digest_hasher_from_resource_name_segment,
};
use pretty_assertions::assert_eq;

// NOTE on global state: `require_explicit_digest_function` is backed by a
// process-global `OnceLock`, so the strict-mode (`None` => reject) branch cannot
// be toggled per-test without affecting other tests in the same binary. That
// branch is covered end-to-end by the execution server's
// `execute_rejects_unset_digest_function`. These tests cover the
// strict-mode-independent behavior of the ByteStream segment resolver: a present
// segment is parsed exactly like the structured-RPC path, an unknown segment is
// rejected, and (in the default, non-strict process) an omitted segment falls
// back to the server default.

#[nativelink_test]
async fn present_blake3_segment_parses() -> Result<(), Box<dyn core::error::Error>> {
    let f = digest_hasher_from_resource_name_segment(Some("blake3"))?;
    assert_eq!(f, DigestHasherFunc::Blake3);
    Ok(())
}

#[nativelink_test]
async fn present_sha256_segment_parses() -> Result<(), Box<dyn core::error::Error>> {
    let f = digest_hasher_from_resource_name_segment(Some("sha256"))?;
    assert_eq!(f, DigestHasherFunc::Sha256);
    Ok(())
}

#[nativelink_test]
async fn present_segment_is_case_insensitive() -> Result<(), Box<dyn core::error::Error>> {
    // ResourceInfo lowercases the function name, but the resolver delegates to
    // DigestHasherFunc::try_from(&str), which uppercases — confirm both spellings.
    assert_eq!(
        digest_hasher_from_resource_name_segment(Some("BLAKE3"))?,
        DigestHasherFunc::Blake3
    );
    assert_eq!(
        digest_hasher_from_resource_name_segment(Some("blake3"))?,
        DigestHasherFunc::Blake3
    );
    Ok(())
}

#[nativelink_test]
async fn unknown_segment_is_rejected() -> Result<(), Box<dyn core::error::Error>> {
    // Unknown digest functions are rejected regardless of strict mode — a present
    // but unsupported segment is never silently defaulted.
    let err = digest_hasher_from_resource_name_segment(Some("md5"))
        .expect_err("md5 is not a supported digest function and must be rejected");
    assert!(
        format!("{err:?}").contains("Unknown or unsupported digest function"),
        "unexpected error: {err:?}"
    );
    Ok(())
}

#[nativelink_test]
async fn omitted_segment_defaults_in_legacy_mode() -> Result<(), Box<dyn core::error::Error>> {
    // In the default (non-strict) process, an omitted segment falls back to the
    // server default rather than erroring. (Strict-mode rejection is covered by
    // the execution server test; see the module note above.)
    let f = digest_hasher_from_resource_name_segment(None)?;
    assert_eq!(f, default_digest_hasher_func());
    Ok(())
}
