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

//! Integration tests for the Nix binary-cache HTTP facade
//! (`NixCacheServer`). These tests are the behavioral specification of
//! the service: they drive the axum router returned by
//! `NixCacheServer::routers()` in-process (no TCP listener) against
//! programmatically constructed `NativeLink` stores, exactly as the
//! production binary would wire them:
//!
//! - the NAR store is a `MemoryStore` wrapped in `VerifyStore` with both
//!   `verify_size` and `verify_hash` enabled, and
//! - the path-info store is a `MemoryStore` wrapped in
//!   `CompletenessCheckingStore` referencing the NAR store, so a
//!   `narinfo` whose NAR is missing is invisible to clients.

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, Method, Request, StatusCode, header};
use bytes::Bytes;
use http_body_util::BodyExt;
use nativelink_config::cas_server::{NixCacheConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_nix::narinfo::{self, NarInfo};
use nativelink_nix::path_info::NixPathInfo;
use nativelink_nix::signing::{NixPublicKey, NixSigningKey};
use nativelink_nix::{nar_url, nixbase32};
use nativelink_service::nix_cache_server::NixCacheServer;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use pretty_assertions::assert_eq;
use sha2::{Digest as _, Sha256};
use tower::ServiceExt;

const NAR_STORE_NAME: &str = "nix_cache_nar_store";
const PATH_INFO_STORE_NAME: &str = "nix_cache_path_info_store";
const ALIAS_STORE_NAME: &str = "nix_cache_alias_store";

/// Throwaway ed25519 signing key for the SERVER side, generated once
/// with the local Nix CLI (nix (Nix) 2.34.7) and hardcoded here:
///
/// ```text
/// $ nix key generate-secret --key-name test-int-1
/// ```
///
/// This key exists only for this test suite; it protects nothing.
const SERVER_SECRET_KEY: &str = "test-int-1:59Az40GGu2M3sM6rQ6T/+c61OMLw1r+qx9xMe/SzkIkjJupCgoxM63utsPNUImg3vc6stSSfAlXYYgmmUSZ5Ig==";

/// Throwaway CLIENT-side signing key, used to attach a genuine `Sig`
/// line to uploaded `narinfo` documents so signature preservation can
/// be asserted. Generated the same way:
///
/// ```text
/// $ nix key generate-secret --key-name test-client-1
/// ```
const CLIENT_SECRET_KEY: &str = "test-client-1:1ZNgkOBfktY+KpNRdPjnmm/P5c4Fqb4PmwMv/FyCqI4BfpImUj8jSSISa1lTOX8E+fPRIJpvjeXLcHh5dblOjQ==";

/// The exact `nix-cache-info` document for the default configuration
/// (`store_dir` "/nix/store", `want_mass_query` true, `priority` 40).
const NIX_CACHE_INFO_BODY: &str = "StoreDir: /nix/store\nWantMassQuery: 1\nPriority: 40\n";

/// Decompressed payload of [`GZIP_NAR_BLOB_HEX`]. NAR bodies are opaque
/// to the service, so this does not need real NAR framing.
const GZIP_NAR_PAYLOAD: &[u8] =
    b"nativelink nix-cache gzip fixture: not a real NAR, deterministic payload 0123456789";

/// A gzip blob whose decompressed bytes are [`GZIP_NAR_PAYLOAD`].
/// Hardcoded because `flate2` is not a dev-dependency of this crate.
/// Generated locally (gzip 1.14) with:
///
/// ```text
/// $ printf '<GZIP_NAR_PAYLOAD>' > payload.bin
/// $ gzip -n -9 -c payload.bin | xxd -p | tr -d '\n'
/// ```
const GZIP_NAR_BLOB_HEX: &str = concat!(
    "1f8b080000000000020305c1c10d83300c05d055fe00ad4429b4c08d0538b081",
    "9518b0080e4a0d0a4cdff7944c4e0ea22b54f2d3915b18f32d3b26c97624eea0",
    "d140484c01433f3ee0d9386da2f23371d8e90a913c8a57f9aeeacfb769ff23af",
    "b60553000000"
);

/// One self-contained store/server fixture. Every test builds its own,
/// so tests can run concurrently and assert exact store contents.
struct CacheFixture {
    store_manager: Arc<StoreManager>,
    /// The `MemoryStore` underneath the `VerifyStore` NAR wrapper.
    nar_memory: Arc<MemoryStore>,
    /// The `MemoryStore` underneath the `CompletenessCheckingStore`.
    path_info_memory: Arc<MemoryStore>,
    alias_memory: Arc<MemoryStore>,
    signing_key_path: PathBuf,
}

impl CacheFixture {
    fn new(tag: &str) -> Self {
        let nar_memory = MemoryStore::new(&MemorySpec::default());
        let nar_store = VerifyStore::new(
            &VerifySpec {
                backend: StoreSpec::Memory(MemorySpec::default()),
                verify_size: true,
                verify_hash: true,
            },
            Store::new(nar_memory.clone()),
        );
        let path_info_memory = MemoryStore::new(&MemorySpec::default());
        let path_info_store = CompletenessCheckingStore::new(
            Store::new(path_info_memory.clone()),
            Store::new(nar_store.clone()),
        );
        let alias_memory = MemoryStore::new(&MemorySpec::default());

        let store_manager = Arc::new(StoreManager::new());
        store_manager.add_store(NAR_STORE_NAME, Store::new(nar_store));
        store_manager.add_store(PATH_INFO_STORE_NAME, Store::new(path_info_store));
        store_manager.add_store(ALIAS_STORE_NAME, Store::new(alias_memory.clone()));

        // The config takes key *files* (as produced by
        // `nix key generate-secret > file`), so write the hardcoded key
        // out with the trailing newline a real key file has.
        let signing_key_path = std::env::temp_dir().join(format!(
            "nix_cache_server_test-{}-{tag}.secret",
            std::process::id()
        ));
        std::fs::write(&signing_key_path, format!("{SERVER_SECRET_KEY}\n"))
            .expect("write signing key file");

        Self {
            store_manager,
            nar_memory,
            path_info_memory,
            alias_memory,
            signing_key_path,
        }
    }

    /// A `NixCacheConfig` with everything serde-defaulted except the
    /// store references and the signing key.
    fn config(&self, read_only: bool) -> NixCacheConfig {
        let mut config: NixCacheConfig = serde_json5::from_str(&format!(
            r#"{{
                cas_store: "{NAR_STORE_NAME}",
                path_info_store: "{PATH_INFO_STORE_NAME}",
                alias_store: "{ALIAS_STORE_NAME}",
                signing_key_files: ["{}"],
            }}"#,
            self.signing_key_path.display()
        ))
        .expect("NixCacheConfig json5 parses");
        config.read_only = read_only;
        config
    }

    /// Builds a `NixCacheServer` for one instance and returns its mount
    /// path plus a root router with the instance router nested at that
    /// mount — exactly how the production binary consumes
    /// `NixCacheServer::routers()`. Tests drive the root router with
    /// full-path URIs (`{mount}/...`).
    fn server(&self, instance_name: &str, read_only: bool) -> (String, Router) {
        let server = NixCacheServer::new(
            &[WithInstanceName {
                instance_name: instance_name.to_string(),
                config: self.config(read_only),
            }],
            &self.store_manager,
        )
        .expect("NixCacheServer::new");
        let mut routers = server.routers();
        assert_eq!(routers.len(), 1, "one router per configured instance");
        let (mount, instance_router) = routers.pop().expect("router present");
        let root_router = Router::new().nest(&mount, instance_router);
        (mount, root_router)
    }
}

async fn call(router: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("axum router is infallible");
    let (parts, body) = response.into_parts();
    let collected = body.collect().await.expect("collect response body");
    (parts.status, parts.headers, collected.to_bytes())
}

fn get_request(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .expect("valid GET request")
}

fn head_request(uri: &str) -> Request<Body> {
    Request::builder()
        .method(Method::HEAD)
        .uri(uri)
        .body(Body::empty())
        .expect("valid HEAD request")
}

fn put_request(uri: &str, payload: &[u8]) -> Request<Body> {
    Request::builder()
        .method(Method::PUT)
        .uri(uri)
        .header(header::CONTENT_LENGTH, payload.len())
        .body(Body::from(Bytes::copy_from_slice(payload)))
        .expect("valid PUT request")
}

fn range_request(uri: &str, range: &str) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::RANGE, range)
        .body(Body::empty())
        .expect("valid ranged GET request")
}

fn header_text(headers: &HeaderMap, name: &HeaderName) -> String {
    headers
        .get(name)
        .unwrap_or_else(|| panic!("missing header {name}"))
        .to_str()
        .expect("header value is ASCII")
        .to_string()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// A deterministic, alphabet-valid `/nix/store/<hash>-<name>` path: the
/// 32-character nix32 hash is derived from `seed` (20 bytes of its
/// sha256, exactly the width of a real store-path hash).
fn test_store_path(seed: &str, name: &str) -> String {
    let digest = sha256(seed.as_bytes());
    let path_hash = nixbase32::encode(&digest[..20]);
    assert!(
        narinfo::is_store_path_hash(&path_hash),
        "generated hash '{path_hash}' must be a valid store path hash"
    );
    format!("/nix/store/{path_hash}-{name}")
}

fn store_path_basename(store_path: &str) -> &str {
    store_path.rsplit('/').next().expect("store path basename")
}

fn store_path_hash(store_path: &str) -> &str {
    &store_path_basename(store_path)[..32]
}

/// The `narinfo` a well-behaved client would upload for an uncompressed
/// (`Compression: none`) NAR: `FileHash`/`FileSize` equal
/// `NarHash`/`NarSize` and the URL is the client-chosen NAR location.
fn narinfo_for_payload(store_path: &str, url: String, payload: &[u8]) -> NarInfo {
    let nar_hash = sha256(payload);
    NarInfo {
        store_path: store_path.to_string(),
        url,
        compression: "none".to_string(),
        file_hash: Some(nar_hash),
        file_size: Some(payload.len() as u64),
        nar_hash,
        nar_size: payload.len() as u64,
        references: vec![],
        deriver: None,
        system: None,
        sigs: vec![],
        ca: None,
    }
}

/// The basename a real `nix copy` client PUTs an uncompressed NAR
/// under: `{nix32(sha256(payload))}.nar`.
fn client_nar_basename(payload: &[u8]) -> String {
    format!("{}.nar", nixbase32::encode(&sha256(payload)))
}

/// Uploads `payload` as a bare `.nar` under the client-chosen basename
/// and asserts the 201. Returns the basename.
async fn put_nar(router: &Router, mount: &str, payload: &[u8]) -> String {
    let basename = client_nar_basename(payload);
    let (status, _, body) = call(
        router,
        put_request(&format!("{mount}/nar/{basename}"), payload),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "PUT nar/{basename}: {}",
        String::from_utf8_lossy(&body)
    );
    basename
}

/// PUTs `info` at `{mount}/{store_path_hash}.narinfo` and returns the
/// response status.
async fn put_narinfo(router: &Router, mount: &str, info: &NarInfo) -> StatusCode {
    let path_hash = store_path_hash(&info.store_path);
    let (status, _, _) = call(
        router,
        put_request(
            &format!("{mount}/{path_hash}.narinfo"),
            info.render().as_bytes(),
        ),
    )
    .await;
    status
}

/// Full client upload flow: NAR first, then its `narinfo`; asserts both
/// return 201.
async fn publish(router: &Router, mount: &str, info: &NarInfo, payload: &[u8]) {
    put_nar(router, mount, payload).await;
    let status = put_narinfo(router, mount, info).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "PUT narinfo for {}",
        info.store_path
    );
}

/// GETs and parses `{mount}/{hash}.narinfo`, asserting the 200.
async fn get_narinfo(router: &Router, mount: &str, path_hash: &str) -> NarInfo {
    let (status, _, body) =
        call(router, get_request(&format!("{mount}/{path_hash}.narinfo"))).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {path_hash}.narinfo: {}",
        String::from_utf8_lossy(&body)
    );
    let text = core::str::from_utf8(&body).expect("narinfo body is UTF-8");
    narinfo::parse(text).expect("served narinfo parses")
}

#[nativelink_test]
async fn nix_cache_info_serves_default_document() -> Result<(), Error> {
    let fixture = CacheFixture::new("cache-info");
    let (mount, router) = fixture.server("main", false);
    // The default mount is `/nix/<instance_name>`.
    assert_eq!(mount, "/nix/main");

    let (status, headers, body) =
        call(&router, get_request(&format!("{mount}/nix-cache-info"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        header_text(&headers, &header::CONTENT_TYPE),
        "text/x-nix-cache-info"
    );
    assert_eq!(
        core::str::from_utf8(&body).expect("body is UTF-8"),
        NIX_CACHE_INFO_BODY
    );
    Ok(())
}

#[nativelink_test]
async fn put_nar_then_head_by_client_and_canonical_name() -> Result<(), Error> {
    const PAYLOAD: &[u8] = b"opaque nar payload for the upload/head round trip 0123456789";
    let fixture = CacheFixture::new("put-head");
    let (mount, router) = fixture.server("main", false);

    let basename = put_nar(&router, &mount, PAYLOAD).await;

    // HEAD of the exact URL the client uploaded to.
    let (status, headers, _) =
        call(&router, head_request(&format!("{mount}/nar/{basename}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(header_text(&headers, &header::ACCEPT_RANGES), "bytes");

    // HEAD of the canonical `{nix32(sha256)}-{size}.nar` name.
    let canonical = nar_url::canonical_nar_name(&sha256(PAYLOAD), PAYLOAD.len() as u64);
    let (status, headers, _) =
        call(&router, head_request(&format!("{mount}/nar/{canonical}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        header_text(&headers, &header::CONTENT_LENGTH),
        PAYLOAD.len().to_string()
    );

    // The client-chosen URL also serves the exact bytes back.
    let (status, _, body) = call(&router, get_request(&format!("{mount}/nar/{basename}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), PAYLOAD);
    Ok(())
}

#[nativelink_test]
async fn put_narinfo_before_nar_is_conflict() -> Result<(), Error> {
    let fixture = CacheFixture::new("narinfo-before-nar");
    let (mount, router) = fixture.server("main", false);

    let store_path = test_store_path("conflict-seed", "ghost-1.0");
    let payload = b"nar bytes that were never uploaded";
    let info = narinfo_for_payload(
        &store_path,
        format!("nar/{}", client_nar_basename(payload)),
        payload,
    );

    assert_eq!(
        put_narinfo(&router, &mount, &info).await,
        StatusCode::CONFLICT,
        "narinfo whose NAR was never uploaded must 409"
    );
    // The rejected narinfo must not be visible or stored.
    let path_hash = store_path_hash(&store_path);
    let (status, _, _) = call(
        &router,
        get_request(&format!("{mount}/{path_hash}.narinfo")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(fixture.path_info_memory.len_for_test(), 0);
    Ok(())
}

#[nativelink_test]
async fn put_narinfo_with_mismatched_nar_hash_is_rejected() -> Result<(), Error> {
    let fixture = CacheFixture::new("narhash-mismatch");
    let (mount, router) = fixture.server("main", false);

    let uploaded_payload = b"the nar bytes that really got uploaded!!";
    let basename = put_nar(&router, &mount, uploaded_payload).await;

    // Same length, different bytes: NarHash disagrees with the upload.
    let other_payload = b"completely different nar bytes, same len";
    assert_eq!(uploaded_payload.len(), other_payload.len());
    let store_path = test_store_path("mismatch-seed", "liar-1.0");
    let info = narinfo_for_payload(&store_path, format!("nar/{basename}"), other_payload);

    assert_eq!(
        put_narinfo(&router, &mount, &info).await,
        StatusCode::BAD_REQUEST,
        "narinfo whose NarHash mismatches the uploaded NAR must 400"
    );
    assert_eq!(fixture.path_info_memory.len_for_test(), 0);
    Ok(())
}

#[nativelink_test]
async fn put_narinfo_with_filename_store_path_mismatch_is_rejected() -> Result<(), Error> {
    let fixture = CacheFixture::new("filename-mismatch");
    let (mount, router) = fixture.server("main", false);

    let payload = b"nar payload for the filename mismatch case";
    let basename = put_nar(&router, &mount, payload).await;

    let store_path = test_store_path("body-seed", "inner-1.0");
    let info = narinfo_for_payload(&store_path, format!("nar/{basename}"), payload);
    // PUT under the hash of a DIFFERENT store path than the body claims.
    let wrong_path = test_store_path("filename-seed", "outer-1.0");
    let wrong_hash = store_path_hash(&wrong_path);
    let (status, _, _) = call(
        &router,
        put_request(
            &format!("{mount}/{wrong_hash}.narinfo"),
            info.render().as_bytes(),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "filename hash != StorePath hash must 400"
    );
    assert_eq!(fixture.path_info_memory.len_for_test(), 0);
    Ok(())
}

#[nativelink_test]
async fn served_narinfo_is_canonical_and_signed() -> Result<(), Error> {
    let fixture = CacheFixture::new("served-narinfo");
    let (mount, router) = fixture.server("main", false);

    // A dependency path, so the signed fingerprint covers a non-trivial
    // References list.
    let dep_payload = b"dependency nar payload";
    let dep_path = test_store_path("dep-seed", "libdep-1.2");
    let dep_info = narinfo_for_payload(
        &dep_path,
        format!("nar/{}", client_nar_basename(dep_payload)),
        dep_payload,
    );
    publish(&router, &mount, &dep_info, dep_payload).await;

    // The main path: references (dep + self), a deriver, and a genuine
    // client signature over the client-computed fingerprint.
    let main_payload = b"main nar payload with enough bytes to be interesting";
    let main_path = test_store_path("main-seed", "app-3.4");
    let mut main_info = narinfo_for_payload(
        &main_path,
        format!("nar/{}", client_nar_basename(main_payload)),
        main_payload,
    );
    main_info.references = vec![
        store_path_basename(&dep_path).to_string(),
        store_path_basename(&main_path).to_string(),
    ];
    main_info.deriver =
        Some(store_path_basename(&test_store_path("drv-seed", "app-3.4.drv")).to_string());
    let client_key = NixSigningKey::from_secret_string(CLIENT_SECRET_KEY).expect("client key");
    let client_sig = client_key.sign(&main_info.fingerprint());
    main_info.sigs = vec![client_sig.clone()];
    publish(&router, &mount, &main_info, main_payload).await;

    let served = get_narinfo(&router, &mount, store_path_hash(&main_path)).await;

    // Identity fields survive the round trip.
    assert_eq!(served.store_path, main_path);
    assert_eq!(served.nar_hash, sha256(main_payload));
    assert_eq!(served.nar_size, main_payload.len() as u64);
    assert_eq!(served.deriver, main_info.deriver);
    let mut expected_references = main_info.references.clone();
    expected_references.sort_unstable();
    assert_eq!(served.references, expected_references);

    // The served URL is the canonical NAR name and the compression is
    // `none`, regardless of what the client uploaded.
    let canonical = nar_url::canonical_nar_name(&sha256(main_payload), main_payload.len() as u64);
    assert_eq!(served.url, format!("nar/{canonical}"));
    assert_eq!(served.compression, "none");

    // The fingerprint is unchanged, the client signature is preserved
    // verbatim, and the server added a signature from the configured
    // key that verifies over the served document's fingerprint.
    assert_eq!(served.fingerprint(), main_info.fingerprint());
    assert!(
        served.sigs.contains(&client_sig),
        "client Sig line must be preserved, got: {}",
        served.sigs.join(" | ")
    );
    let server_key = NixSigningKey::from_secret_string(SERVER_SECRET_KEY).expect("server key");
    let public_key = NixPublicKey::from_string(&server_key.public_key_string()).expect("pub key");
    let server_sig = served
        .sigs
        .iter()
        .find(|sig: &&String| sig.starts_with("test-int-1:"))
        .expect("server Sig line present");
    assert!(
        public_key.verify(&served.fingerprint(), server_sig),
        "server signature must verify over the narinfo fingerprint"
    );

    // The advertised URL is fetchable and returns the exact NAR bytes.
    let (status, _, body) = call(&router, get_request(&format!("{mount}/{}", served.url))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), main_payload);
    Ok(())
}

#[nativelink_test]
async fn narinfo_head_unknown_and_malformed_names() -> Result<(), Error> {
    let fixture = CacheFixture::new("narinfo-head");
    let (mount, router) = fixture.server("main", false);

    let payload = b"nar payload for the HEAD narinfo case";
    let store_path = test_store_path("head-seed", "headful-1.0");
    let info = narinfo_for_payload(
        &store_path,
        format!("nar/{}", client_nar_basename(payload)),
        payload,
    );
    publish(&router, &mount, &info, payload).await;

    let path_hash = store_path_hash(&store_path);
    let (status, _, _) = call(
        &router,
        head_request(&format!("{mount}/{path_hash}.narinfo")),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "HEAD of an uploaded narinfo");

    // A well-formed hash that was never uploaded is a clean 404 on both
    // GET and HEAD.
    let unknown_path = test_store_path("unknown-seed", "missing-1.0");
    let unknown_hash = store_path_hash(&unknown_path);
    let (status, _, _) = call(
        &router,
        get_request(&format!("{mount}/{unknown_hash}.narinfo")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, _) = call(
        &router,
        head_request(&format!("{mount}/{unknown_hash}.narinfo")),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Malformed names must be 400 or 404, never a 5xx: 31 characters,
    // and 32 characters outside the nix32 alphabet ('e' is excluded).
    let malformed = [
        path_hash[..31].to_string(),
        "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string(),
    ];
    for name in &malformed {
        for request in [
            get_request(&format!("{mount}/{name}.narinfo")),
            head_request(&format!("{mount}/{name}.narinfo")),
        ] {
            let (status, _, _) = call(&router, request).await;
            assert!(
                matches!(status, StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST),
                "malformed name '{name}' must be 400/404, got {status}"
            );
        }
    }
    Ok(())
}

#[nativelink_test]
async fn nar_get_full_and_range_requests() -> Result<(), Error> {
    const PAYLOAD: &[u8] = b"0123456789abcdefghij:nar-range-payload!";
    let fixture = CacheFixture::new("nar-range");
    let (mount, router) = fixture.server("main", false);

    let basename = put_nar(&router, &mount, PAYLOAD).await;
    let nar_uri = format!("{mount}/nar/{basename}");

    // Full GET: exact bytes, length, and media type.
    let (status, headers, body) = call(&router, get_request(&nar_uri)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        header_text(&headers, &header::CONTENT_TYPE),
        "application/x-nix-nar"
    );
    assert_eq!(
        header_text(&headers, &header::CONTENT_LENGTH),
        PAYLOAD.len().to_string()
    );
    assert_eq!(body.as_ref(), PAYLOAD);

    // A satisfiable byte range: inclusive on both ends.
    let (status, headers, body) = call(&router, range_request(&nar_uri, "bytes=3-9")).await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        header_text(&headers, &header::CONTENT_RANGE),
        format!("bytes 3-9/{}", PAYLOAD.len())
    );
    assert_eq!(body.as_ref(), &PAYLOAD[3..=9]);

    // Ranges starting at or past the end are unsatisfiable.
    let at_end = format!("bytes={}-{}", PAYLOAD.len(), PAYLOAD.len() + 5);
    for range in [at_end.as_str(), "bytes=100000-"] {
        let (status, _, _) = call(&router, range_request(&nar_uri, range)).await;
        assert_eq!(
            status,
            StatusCode::RANGE_NOT_SATISFIABLE,
            "range '{range}' must 416"
        );
    }
    Ok(())
}

#[nativelink_test]
async fn narinfo_of_evicted_nar_is_not_found() -> Result<(), Error> {
    let fixture = CacheFixture::new("evicted-nar");
    let (mount, router) = fixture.server("main", false);

    let payload = b"nar payload that will be evicted from the store";
    let store_path = test_store_path("evict-seed", "shortlived-1.0");
    let info = narinfo_for_payload(
        &store_path,
        format!("nar/{}", client_nar_basename(payload)),
        payload,
    );
    publish(&router, &mount, &info, payload).await;

    let path_hash = store_path_hash(&store_path);
    let narinfo_uri = format!("{mount}/{path_hash}.narinfo");
    let (status, _, _) = call(&router, get_request(&narinfo_uri)).await;
    assert_eq!(status, StatusCode::OK, "narinfo visible before eviction");

    // Evict the NAR out from under the path-info record. NARs are keyed
    // by `DigestInfo(sha256(nar), nar_size)` in the NAR store.
    let removed = fixture
        .nar_memory
        .remove_entry(DigestInfo::new(sha256(payload), payload.len() as u64).into())
        .await;
    assert!(removed, "NAR entry must exist before eviction");

    // The completeness_checking wrapper now hides the record on both
    // GET and HEAD.
    let (status, _, _) = call(&router, get_request(&narinfo_uri)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "GET after eviction");
    let (status, _, _) = call(&router, head_request(&narinfo_uri)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "HEAD after eviction");
    Ok(())
}

#[nativelink_test]
async fn path_info_record_without_nar_is_not_found() -> Result<(), Error> {
    let fixture = CacheFixture::new("phantom-record");
    let (mount, router) = fixture.server("main", false);

    // Write a well-formed path-info record directly into the backing
    // store, referencing a NAR digest that was never uploaded.
    let phantom_payload = b"phantom nar bytes that never reached the store";
    let store_path = test_store_path("phantom-seed", "phantom-2.0");
    let info = narinfo_for_payload(
        &store_path,
        format!("nar/{}", client_nar_basename(phantom_payload)),
        phantom_payload,
    );
    let record = NixPathInfo::from_nar_info(&info).encode_record()?;
    let path_hash = store_path_hash(&store_path);
    fixture
        .path_info_memory
        .update_oneshot(StoreKey::new_str(path_hash), Bytes::from(record))
        .await?;

    let narinfo_uri = format!("{mount}/{path_hash}.narinfo");
    let (status, _, _) = call(&router, get_request(&narinfo_uri)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "GET of a NAR-less record");
    let (status, _, _) = call(&router, head_request(&narinfo_uri)).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "HEAD of a NAR-less record");
    Ok(())
}

#[nativelink_test]
async fn read_only_instance_rejects_uploads_but_serves_reads() -> Result<(), Error> {
    let fixture = CacheFixture::new("read-only");
    let (writable_mount, writable_router) = fixture.server("main", false);
    let (mirror_mount, mirror_router) = fixture.server("mirror", true);
    assert_eq!(mirror_mount, "/nix/mirror");

    // Populate through the writable instance.
    let payload = b"nar payload shared by the writable and read-only mounts";
    let store_path = test_store_path("ro-seed", "shared-1.0");
    let info = narinfo_for_payload(
        &store_path,
        format!("nar/{}", client_nar_basename(payload)),
        payload,
    );
    publish(&writable_router, &writable_mount, &info, payload).await;

    // All reads work on the read-only instance.
    let (status, _, body) = call(
        &mirror_router,
        get_request(&format!("{mirror_mount}/nix-cache-info")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        core::str::from_utf8(&body).expect("UTF-8"),
        NIX_CACHE_INFO_BODY
    );
    let served = get_narinfo(&mirror_router, &mirror_mount, store_path_hash(&store_path)).await;
    let (status, _, body) = call(
        &mirror_router,
        get_request(&format!("{mirror_mount}/{}", served.url)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), payload);

    // Every PUT is 405 and leaves no trace in any store.
    let nar_len = fixture.nar_memory.len_for_test();
    let path_info_len = fixture.path_info_memory.len_for_test();
    let alias_len = fixture.alias_memory.len_for_test();

    let new_payload = b"bytes the read-only mount must refuse";
    let (status, _, _) = call(
        &mirror_router,
        put_request(
            &format!("{mirror_mount}/nar/{}", client_nar_basename(new_payload)),
            new_payload,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::METHOD_NOT_ALLOWED,
        "PUT nar on read-only"
    );

    let new_path = test_store_path("ro-put-seed", "refused-1.0");
    let new_info = narinfo_for_payload(
        &new_path,
        format!("nar/{}", client_nar_basename(new_payload)),
        new_payload,
    );
    assert_eq!(
        put_narinfo(&mirror_router, &mirror_mount, &new_info).await,
        StatusCode::METHOD_NOT_ALLOWED,
        "PUT narinfo on read-only"
    );

    assert_eq!(fixture.nar_memory.len_for_test(), nar_len);
    assert_eq!(fixture.path_info_memory.len_for_test(), path_info_len);
    assert_eq!(fixture.alias_memory.len_for_test(), alias_len);
    Ok(())
}

#[nativelink_test]
async fn gzip_nar_upload_is_sniffed_and_stored_decompressed() -> Result<(), Error> {
    let fixture = CacheFixture::new("gzip-sniff");
    let (mount, router) = fixture.server("main", false);

    let gzip_blob = hex::decode(GZIP_NAR_BLOB_HEX).expect("fixture hex decodes");
    assert_eq!(gzip_blob[..2], nar_url::GZIP_MAGIC, "fixture is gzip");

    // A client that gzipped its NAR but still named it `.nar`: the name
    // is derived from the *compressed* bytes, as `nix copy` would.
    let (status, _, body) = call(
        &router,
        put_request(
            &format!("{mount}/nar/{}", client_nar_basename(&gzip_blob)),
            &gzip_blob,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "PUT gzipped .nar: {}",
        String::from_utf8_lossy(&body)
    );

    // The stored NAR is the DECOMPRESSED stream: fetching the canonical
    // name derived from the decompressed sha256 returns those bytes.
    let canonical =
        nar_url::canonical_nar_name(&sha256(GZIP_NAR_PAYLOAD), GZIP_NAR_PAYLOAD.len() as u64);
    let (status, _, body) = call(&router, get_request(&format!("{mount}/nar/{canonical}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), GZIP_NAR_PAYLOAD);
    Ok(())
}

#[nativelink_test]
async fn put_nar_with_wrong_name_digest_is_client_error() -> Result<(), Error> {
    let fixture = CacheFixture::new("wrong-digest");
    let (mount, router) = fixture.server("main", false);

    // Name computed from DIFFERENT bytes: the verify wrapper must
    // reject the write, surfaced as a 4xx, and store nothing.
    let payload = b"actual bytes of the corrupt upload";
    let wrong_name = client_nar_basename(b"bytes the name was computed from instead");
    let (status, _, _) = call(
        &router,
        put_request(&format!("{mount}/nar/{wrong_name}"), payload),
    )
    .await;
    assert!(
        status.is_client_error(),
        "hash-mismatched NAR upload must be 4xx, got {status}"
    );
    assert_eq!(fixture.nar_memory.len_for_test(), 0);
    Ok(())
}

#[nativelink_test]
async fn traversal_and_garbage_names_are_client_errors_without_writes() -> Result<(), Error> {
    let fixture = CacheFixture::new("traversal");
    let (mount, router) = fixture.server("main", false);

    let garbage_uris = [
        format!("{mount}/..%2Fetc"),
        format!("{mount}/..%2F..%2Fetc%2Fpasswd.narinfo"),
        format!("{mount}/nar/..%2F..%2Fescape.nar"),
        format!("{mount}/nar/../x"),
        format!("{mount}/{}.narinfo", "z".repeat(53)),
        format!("{mount}/nar/{}.nar", "z".repeat(53)),
        format!("{mount}/nar/{}.nar", "z".repeat(31)),
    ];
    for uri in &garbage_uris {
        let (status, _, _) = call(&router, get_request(uri)).await;
        assert!(
            status.is_client_error(),
            "GET {uri} must be 4xx, got {status}"
        );
        let (status, _, _) = call(&router, put_request(uri, b"garbage payload")).await;
        assert!(
            status.is_client_error(),
            "PUT {uri} must be 4xx, got {status}"
        );
    }

    // None of it may have written anything anywhere.
    assert_eq!(fixture.nar_memory.len_for_test(), 0);
    assert_eq!(fixture.path_info_memory.len_for_test(), 0);
    assert_eq!(fixture.alias_memory.len_for_test(), 0);
    Ok(())
}
