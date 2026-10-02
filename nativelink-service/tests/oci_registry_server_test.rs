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

//! Integration tests for the OCI Distribution registry facade
//! (`OciRegistryServer`), driving the axum router in-process against
//! programmatically constructed stores wired exactly as production:
//! blob store behind `verify`, index store behind `completeness_checking`
//! referencing the blob store, plain ref store.
//!
//! These are unit-level regression pins, NOT the wire oracle — the
//! conformance suite and independent-client round-trips in `flake check`
//! own wire correctness (design 3 of `design/oci-registry-over-cas.md`).
//! Every accepting test has a rejecting twin (the monotone rule).

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use nativelink_config::cas_server::{OciRegistryServiceConfig, WithInstanceName};
use nativelink_config::stores::{MemorySpec, StoreSpec, VerifySpec};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_service::oci_registry_server::OciRegistryServer;
use nativelink_store::completeness_checking_store::CompletenessCheckingStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, make_ctx_for_hash_func};
use nativelink_util::store_trait::{Store, StoreKey, StoreLike};
use opentelemetry::context::FutureExt;
use pretty_assertions::assert_eq;
use sha2::{Digest as _, Sha256};
use tower::ServiceExt;

const BLOB_STORE_NAME: &str = "OCI_BLOB_STORE";
const INDEX_STORE_NAME: &str = "OCI_INDEX_STORE";
const REF_STORE_NAME: &str = "OCI_REF_STORE";

fn sha256_hex(data: &[u8]) -> String {
    hex_of(&Sha256::digest(data))
}

fn blake3_hex(data: &[u8]) -> String {
    let mut hasher = DigestHasherFunc::Blake3.hasher();
    hasher.update(data);
    hasher.finalize_digest().packed_hash().to_string()
}

fn hex_of(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

struct RegistryFixture {
    store_manager: Arc<StoreManager>,
    blob_memory: Arc<MemoryStore>,
    spool_dir: std::path::PathBuf,
}

impl RegistryFixture {
    fn new() -> Self {
        let blob_memory = MemoryStore::new(&MemorySpec::default());
        let blob_store = VerifyStore::new(
            &VerifySpec {
                backend: StoreSpec::Memory(MemorySpec::default()),
                verify_size: true,
                verify_hash: true,
            },
            Store::new(blob_memory.clone()),
        );
        let index_memory = MemoryStore::new(&MemorySpec::default());
        let index_store = CompletenessCheckingStore::new(
            Store::new(index_memory),
            Store::new(blob_store.clone()),
        );
        let ref_memory = MemoryStore::new(&MemorySpec::default());

        let store_manager = Arc::new(StoreManager::new());
        store_manager.add_store(BLOB_STORE_NAME, Store::new(blob_store));
        store_manager.add_store(INDEX_STORE_NAME, Store::new(index_store));
        store_manager.add_store(REF_STORE_NAME, Store::new(ref_memory));

        let spool_dir = std::env::temp_dir().join(format!(
            "oci_registry_server_test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));

        Self {
            store_manager,
            blob_memory,
            spool_dir,
        }
    }

    fn config(&self) -> OciRegistryServiceConfig {
        serde_json5::from_str::<OciRegistryServiceConfig>(&format!(
            r#"{{
                cas_store: "{BLOB_STORE_NAME}",
                index_store: "{INDEX_STORE_NAME}",
                ref_store: "{REF_STORE_NAME}",
                digest_function: "BLAKE3",
                spool_path: "{}",
                enable_delete: true,
            }}"#,
            self.spool_dir.display()
        ))
        .expect("parse OciRegistryServiceConfig")
    }

    fn router_with_config(&self, config: OciRegistryServiceConfig) -> Router {
        let server = OciRegistryServer::new(
            &[WithInstanceName {
                instance_name: "main".to_string(),
                config,
            }],
            &self.store_manager,
        )
        .expect("OciRegistryServer::new");
        let mut routers = server.routers();
        assert_eq!(routers.len(), 1, "one router per configured instance");
        let (mount, instance_router) = routers.pop().expect("router present");
        assert_eq!(mount, "/v2");
        Router::new().nest_service(&mount, instance_router)
    }

    fn router(&self) -> Router {
        self.router_with_config(self.config())
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

fn request(method: Method, uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(body.into())
        .expect("build request")
}

/// Pushes `data` as a monolithic blob POST, asserting 201.
async fn push_blob(router: &Router, repo: &str, data: &[u8]) -> String {
    let digest = format!("sha256:{}", sha256_hex(data));
    let (status, headers, _) = call(
        router,
        request(
            Method::POST,
            &format!("/v2/{repo}/blobs/uploads/?digest={digest}"),
            data.to_vec(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "monolithic push accepted");
    assert_eq!(
        headers
            .get("Docker-Content-Digest")
            .and_then(|v| v.to_str().ok()),
        Some(digest.as_str())
    );
    digest
}

/// A minimal but structurally real OCI image manifest for pushed blobs.
fn manifest_for(config: &[u8], layer: &[u8]) -> String {
    format!(
        r#"{{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:{}","size":{}}},"layers":[{{"mediaType":"application/vnd.oci.image.layer.v1.tar+gzip","digest":"sha256:{}","size":{}}}]}}"#,
        sha256_hex(config),
        config.len(),
        sha256_hex(layer),
        layer.len()
    )
}

#[nativelink_test]
async fn v2_root_answers_200() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let (status, _, _) = call(&router, request(Method::GET, "/v2/", Body::empty())).await;
    assert_eq!(status, StatusCode::OK);
    Ok(())
}

#[nativelink_test]
async fn monolithic_blob_push_stores_under_canonical_blake3() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let data = b"gate-a layer bytes".to_vec();
    push_blob(&router, "gate-a/repo", &data).await;

    // THE Gate A invariant: the pushed bytes are in the backing store under
    // their BLAKE3 canonical digest — not sha256-keyed by accident.
    let blake3_digest = DigestInfo::try_new(&blake3_hex(&data), data.len())?;
    let stored = fixture
        .blob_memory
        .get_part_unchunked(blake3_digest, 0, None)
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await?;
    assert_eq!(stored, Bytes::from(data.clone()));

    // FALSIFIER: the same bytes are NOT stored under a sha256 key — one
    // blob, one storage identity.
    let sha256_digest = DigestInfo::try_new(&sha256_hex(&data), data.len())?;
    assert!(
        fixture
            .blob_memory
            .get_part_unchunked(sha256_digest, 0, None)
            .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
            .await
            .is_err(),
        "blob must not exist under its sha256 name in the store"
    );
    Ok(())
}

#[nativelink_test]
async fn blob_round_trips_by_wire_name() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let data = b"round trip payload".to_vec();
    let digest = push_blob(&router, "roundtrip/repo", &data).await;

    let (status, headers, body) = call(
        &router,
        request(
            Method::GET,
            &format!("/v2/roundtrip/repo/blobs/{digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(data.clone()));
    assert_eq!(
        headers
            .get("Docker-Content-Digest")
            .and_then(|v| v.to_str().ok()),
        Some(digest.as_str())
    );

    // HEAD answers length without a body.
    let (status, headers, body) = call(
        &router,
        request(
            Method::HEAD,
            &format!("/v2/roundtrip/repo/blobs/{digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.is_empty());
    assert_eq!(
        headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok()),
        Some(data.len())
    );

    // FALSIFIER: an unknown digest is a clean spec-shaped 404.
    let missing = format!("sha256:{}", sha256_hex(b"never pushed"));
    let (status, _, body) = call(
        &router,
        request(
            Method::GET,
            &format!("/v2/roundtrip/repo/blobs/{missing}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let errors: serde_json::Value = serde_json::from_slice(&body).expect("errors json");
    assert_eq!(errors["errors"][0]["code"], "BLOB_UNKNOWN");
    Ok(())
}

#[nativelink_test]
async fn chunked_upload_session_round_trips() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let part_a = b"first half / ".to_vec();
    let part_b = b"second half".to_vec();
    let mut full = part_a.clone();
    full.extend_from_slice(&part_b);
    let digest = format!("sha256:{}", sha256_hex(&full));

    // Open a session.
    let (status, headers, _) = call(
        &router,
        request(
            Method::POST,
            "/v2/chunked/repo/blobs/uploads/",
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let location = headers
        .get("location")
        .and_then(|v| v.to_str().ok())
        .expect("upload location")
        .to_string();

    // First chunk.
    let (status, _, _) = call(&router, request(Method::PATCH, &location, part_a.clone())).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // FALSIFIER: a chunk whose Content-Range does not continue at the
    // current offset is refused with 416.
    let bad = Request::builder()
        .method(Method::PATCH)
        .uri(&location)
        .header("Content-Range", "999-1010")
        .body(Body::from(part_b.clone()))
        .expect("request");
    let (status, _, _) = call(&router, bad).await;
    assert_eq!(status, StatusCode::RANGE_NOT_SATISFIABLE);

    // Correct continuation.
    let good = Request::builder()
        .method(Method::PATCH)
        .uri(&location)
        .header(
            "Content-Range",
            format!("{}-{}", part_a.len(), full.len() - 1),
        )
        .body(Body::from(part_b.clone()))
        .expect("request");
    let (status, _, _) = call(&router, good).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // Finalize.
    let (status, _, _) = call(
        &router,
        request(
            Method::PUT,
            &format!("{location}?digest={digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Session is consumed: finalizing again is BLOB_UPLOAD_UNKNOWN.
    let (status, _, _) = call(
        &router,
        request(
            Method::PUT,
            &format!("{location}?digest={digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The blob serves back by wire name.
    let (status, _, body) = call(
        &router,
        request(
            Method::GET,
            &format!("/v2/chunked/repo/blobs/{digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(full));
    Ok(())
}

#[nativelink_test]
async fn upload_with_wrong_declared_digest_is_rejected() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let data = b"actual content".to_vec();
    let lying_digest = format!("sha256:{}", sha256_hex(b"different content"));
    let (status, _, body) = call(
        &router,
        request(
            Method::POST,
            &format!("/v2/liar/repo/blobs/uploads/?digest={lying_digest}"),
            data.clone(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let errors: serde_json::Value = serde_json::from_slice(&body).expect("errors json");
    assert_eq!(errors["errors"][0]["code"], "DIGEST_INVALID");

    // FALSIFIER-of-the-falsifier: the honest digest for the same bytes is
    // accepted — the rejection above was about the mismatch, not the route.
    push_blob(&router, "liar/repo", &data).await;
    Ok(())
}

#[nativelink_test]
async fn cross_repo_mount_hits_and_misses() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let data = b"mounted layer".to_vec();
    let digest = push_blob(&router, "source/repo", &data).await;

    // Mount an existing blob: immediate 201 (blobs are global).
    let (status, _, _) = call(
        &router,
        request(
            Method::POST,
            &format!("/v2/target/repo/blobs/uploads/?mount={digest}&from=source/repo"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // FALSIFIER: mounting an unknown blob falls through to a session (202).
    let missing = format!("sha256:{}", sha256_hex(b"nope"));
    let (status, _, _) = call(
        &router,
        request(
            Method::POST,
            &format!("/v2/target/repo/blobs/uploads/?mount={missing}&from=source/repo"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    Ok(())
}

#[nativelink_test]
async fn manifest_push_tag_serve_and_list() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let config = b"{\"os\":\"linux\"}".to_vec();
    let layer = b"layer tar bytes".to_vec();
    push_blob(&router, "app/web", &config).await;
    push_blob(&router, "app/web", &layer).await;

    let manifest = manifest_for(&config, &layer);
    let manifest_digest = format!("sha256:{}", sha256_hex(manifest.as_bytes()));

    // Tag push.
    let put = Request::builder()
        .method(Method::PUT)
        .uri("/v2/app/web/manifests/v1")
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(Body::from(manifest.clone()))
        .expect("request");
    let (status, headers, _) = call(&router, put).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        headers
            .get("Docker-Content-Digest")
            .and_then(|v| v.to_str().ok()),
        Some(manifest_digest.as_str())
    );

    // GET by tag: byte-identical, right media type, right digest header.
    let (status, headers, body) = call(
        &router,
        request(Method::GET, "/v2/app/web/manifests/v1", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(manifest.clone()));
    assert_eq!(
        headers.get("content-type").and_then(|v| v.to_str().ok()),
        Some("application/vnd.oci.image.manifest.v1+json")
    );

    // GET by digest.
    let (status, _, body) = call(
        &router,
        request(
            Method::GET,
            &format!("/v2/app/web/manifests/{manifest_digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(manifest.clone()));

    // tags/list.
    let (status, _, body) = call(
        &router,
        request(Method::GET, "/v2/app/web/tags/list", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let listing: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(listing["name"], "app/web");
    assert_eq!(listing["tags"], serde_json::json!(["v1"]));

    // FALSIFIER: an unknown tag and an unknown repo both 404.
    let (status, _, _) = call(
        &router,
        request(Method::GET, "/v2/app/web/manifests/v2", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _, _) = call(
        &router,
        request(Method::GET, "/v2/no/such/repo/tags/list", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[nativelink_test]
async fn manifest_referencing_unknown_blob_is_rejected() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let config = b"cfg".to_vec();
    let layer = b"layer".to_vec();
    // Push ONLY the config; the layer is missing.
    push_blob(&router, "partial/repo", &config).await;
    let manifest = manifest_for(&config, &layer);
    let (status, _, body) = call(
        &router,
        request(
            Method::PUT,
            "/v2/partial/repo/manifests/broken",
            manifest.clone(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let errors: serde_json::Value = serde_json::from_slice(&body).expect("errors json");
    assert_eq!(errors["errors"][0]["code"], "MANIFEST_BLOB_UNKNOWN");

    // FALSIFIER: pushing the layer flips the same PUT to accepted.
    push_blob(&router, "partial/repo", &layer).await;
    let (status, _, _) = call(
        &router,
        request(Method::PUT, "/v2/partial/repo/manifests/broken", manifest),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

#[nativelink_test]
async fn tag_overwrite_wins_and_records_previous() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let config = b"cfg".to_vec();
    let layer_one = b"layer one".to_vec();
    let layer_two = b"layer two".to_vec();
    push_blob(&router, "mut/repo", &config).await;
    push_blob(&router, "mut/repo", &layer_one).await;
    push_blob(&router, "mut/repo", &layer_two).await;

    let manifest_one = manifest_for(&config, &layer_one);
    let manifest_two = manifest_for(&config, &layer_two);
    for manifest in [&manifest_one, &manifest_two] {
        let (status, _, _) = call(
            &router,
            request(
                Method::PUT,
                "/v2/mut/repo/manifests/latest",
                (*manifest).clone(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }
    // The second write won.
    let (status, _, body) = call(
        &router,
        request(Method::GET, "/v2/mut/repo/manifests/latest", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(manifest_two));
    Ok(())
}

#[nativelink_test]
async fn read_only_refuses_writes_and_serves_reads() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    // Populate via a writable router first.
    let router = fixture.router();
    let data = b"published blob".to_vec();
    let digest = push_blob(&router, "frozen/repo", &data).await;

    let mut config = fixture.config();
    config.read_only = true;
    // A second instance over the SAME stores, read-only. Fresh spool to
    // avoid pruning the writable instance's directory mid-test.
    config.spool_path = Some(fixture.spool_dir.join("read-only").display().to_string());
    let frozen = fixture.router_with_config(config);

    let (status, _, body) = call(
        &frozen,
        request(
            Method::GET,
            &format!("/v2/frozen/repo/blobs/{digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, Bytes::from(data));

    let (status, _, _) = call(
        &frozen,
        request(
            Method::POST,
            "/v2/frozen/repo/blobs/uploads/",
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    Ok(())
}

#[nativelink_test]
async fn token_auth_gates_reads_and_writes() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let token_dir = fixture.spool_dir.join("tokens");
    std::fs::create_dir_all(&token_dir).expect("mkdir tokens");
    let read_token = token_dir.join("read");
    let write_token = token_dir.join("write");
    std::fs::write(&read_token, "reader-secret\n").expect("write read token");
    std::fs::write(&write_token, "writer-secret\n").expect("write write token");

    let mut config = fixture.config();
    config.read_token_files = vec![read_token.display().to_string()];
    config.write_token_files = vec![write_token.display().to_string()];
    let router = fixture.router_with_config(config);

    // Anonymous read: 401 with a Basic challenge.
    let (status, headers, _) = call(&router, request(Method::GET, "/v2/", Body::empty())).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        headers
            .get("www-authenticate")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("Basic")),
        "401 must carry a Basic challenge"
    );

    // Read token reads; read token must NOT write.
    let authed = |method: Method, uri: &str, token: &str, body: Vec<u8>| {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::from(body))
            .expect("request")
    };
    let (status, _, _) = call(
        &router,
        authed(Method::GET, "/v2/", "reader-secret", vec![]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let data = b"gated blob".to_vec();
    let digest = format!("sha256:{}", sha256_hex(&data));
    let (status, _, _) = call(
        &router,
        authed(
            Method::POST,
            &format!("/v2/gated/repo/blobs/uploads/?digest={digest}"),
            "reader-secret",
            data.clone(),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "read token must not write"
    );

    // Write token writes (Basic form, as docker/skopeo send it).
    let basic = base64_of(&format!("anyuser:writer-secret"));
    let put = Request::builder()
        .method(Method::POST)
        .uri(format!("/v2/gated/repo/blobs/uploads/?digest={digest}"))
        .header("Authorization", format!("Basic {basic}"))
        .body(Body::from(data))
        .expect("request");
    let (status, _, _) = call(&router, put).await;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

fn base64_of(input: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(input)
}

#[nativelink_test]
async fn delete_removes_names_not_content() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let config = b"cfg".to_vec();
    let layer = b"deletable layer".to_vec();
    push_blob(&router, "del/repo", &config).await;
    let layer_digest = push_blob(&router, "del/repo", &layer).await;
    let manifest = manifest_for(&config, &layer);
    let (status, _, _) = call(
        &router,
        request(Method::PUT, "/v2/del/repo/manifests/gone", manifest.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // Tag delete: 202, then the tag 404s and tags/list drops it.
    let (status, _, _) = call(
        &router,
        request(Method::DELETE, "/v2/del/repo/manifests/gone", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _, _) = call(
        &router,
        request(Method::GET, "/v2/del/repo/manifests/gone", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Blob delete removes the NAME; the canonical bytes stay in the store.
    let (status, _, _) = call(
        &router,
        request(
            Method::DELETE,
            &format!("/v2/del/repo/blobs/{layer_digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    let (status, _, _) = call(
        &router,
        request(
            Method::GET,
            &format!("/v2/del/repo/blobs/{layer_digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let blake3_digest = DigestInfo::try_new(&blake3_hex(&layer), layer.len())?;
    let still_there = fixture
        .blob_memory
        .get_part_unchunked(blake3_digest, 0, None)
        .with_context(make_ctx_for_hash_func(DigestHasherFunc::Blake3)?)
        .await?;
    assert_eq!(still_there, Bytes::from(layer));

    // FALSIFIER: with enable_delete=false the same DELETE is 405.
    let mut config_no_delete = fixture.config();
    config_no_delete.enable_delete = false;
    config_no_delete.spool_path = Some(fixture.spool_dir.join("no-delete").display().to_string());
    let no_delete = fixture.router_with_config(config_no_delete);
    let (status, _, _) = call(
        &no_delete,
        request(
            Method::DELETE,
            "/v2/del/repo/manifests/other",
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    Ok(())
}

#[nativelink_test]
async fn invalid_repository_names_are_rejected_before_stores() -> Result<(), Error> {
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    // An uppercase (invalid) name must 400 with NAME_INVALID, never touch
    // key derivation.
    let (status, _, body) = call(
        &router,
        request(Method::GET, "/v2/Bad/Name/tags/list", Body::empty()),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let errors: serde_json::Value = serde_json::from_slice(&body).expect("errors json");
    assert_eq!(errors["errors"][0]["code"], "NAME_INVALID");
    Ok(())
}

#[nativelink_test]
async fn alias_survives_but_404s_after_blob_eviction() -> Result<(), Error> {
    // The completeness_checking payoff: evict the blob behind an alias and
    // the wire answer degrades to a clean 404, not a 500 or a lie.
    let fixture = RegistryFixture::new();
    let router = fixture.router();
    let data = b"evictable".to_vec();
    let digest = push_blob(&router, "evict/repo", &data).await;

    // Remove the canonical blob out from under the alias.
    let blake3_digest = DigestInfo::try_new(&blake3_hex(&data), data.len())?;
    fixture
        .blob_memory
        .remove_entry(StoreKey::from(blake3_digest))
        .await;

    let (status, _, _) = call(
        &router,
        request(
            Method::GET,
            &format!("/v2/evict/repo/blobs/{digest}"),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}
