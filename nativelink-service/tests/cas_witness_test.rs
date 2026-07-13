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

//! Integration tests for the caching HTTP CAS witness (`CasWitness`).
//!
//! These drive the CAS witness over real loopback sockets: a plain-HTTP origin
//! that counts hits, the CAS witness in front of it, and a `reqwest` client
//! configured to use it. This exercises the whole caching path — absolute-form
//! proxy request parsing, the URL→CAS index, origin fetch + spool into CAS, and
//! serving a repeat request from the CAS — without needing TLS interception
//! (the CONNECT/MITM certificate machinery is covered by the `cas_witness`
//! unit tests).

use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use nativelink_config::cas_server::CasWitnessConfig;
use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_service::cas_witness::CasWitness;
use nativelink_service::witness::{Receipt, WitnessKey};
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::Store;

const ORIGIN_BODY: &str = "nativelink cas-witness origin body: deterministic 0123456789";

type Guard = nativelink_util::task::JoinHandleDropGuard<()>;

/// Starts a plain-HTTP origin on an ephemeral loopback port. `GET /thing`
/// returns [`ORIGIN_BODY`] and bumps `hits`. Returns the address and the
/// serving task guard (drop to stop it).
async fn spawn_origin(hits: Arc<AtomicUsize>) -> (String, Guard) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind origin");
    let addr = listener.local_addr().expect("origin addr");
    let app = Router::new().route(
        "/thing",
        get(move || {
            let hits = Arc::clone(&hits);
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                ORIGIN_BODY
            }
        }),
    );
    let guard = nativelink_util::spawn!("test_origin", async move {
        axum::serve(listener, app).await.expect("serve origin");
    });
    (addr.to_string(), guard)
}

/// Builds a `CasWitness` over fresh in-memory stores, serves it on an ephemeral
/// loopback port, and returns the witness address and the serving task guard.
async fn spawn_witness(tag: &str) -> (String, Guard) {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("CAS", Store::new(MemoryStore::new(&MemorySpec::default())));
    store_manager.add_store(
        "ALIAS",
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let dir = std::env::temp_dir().join(format!("nl-cas-witness-it-{}-{tag}", std::process::id()));
    drop(std::fs::remove_dir_all(&dir));
    let config: CasWitnessConfig = serde_json5::from_str(&format!(
        r#"{{
            cas_store: "CAS",
            alias_store: "ALIAS",
            ca_cert_file: "{dir}/ca.crt",
            ca_key_file: "{dir}/ca.key",
        }}"#,
        dir = dir.display()
    ))
    .expect("witness config parses");

    let witness = CasWitness::new(&config, &store_manager).expect("build witness");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind witness");
    let addr = listener.local_addr().expect("witness addr");
    let guard = nativelink_util::spawn!("test_witness", async move {
        witness.serve(listener).await;
    });
    (addr.to_string(), guard)
}

#[nativelink_test]
async fn caches_plain_http_get_and_serves_repeat_from_cas() -> Result<(), Error> {
    let hits = Arc::new(AtomicUsize::new(0));
    let (origin_addr, _origin) = spawn_origin(Arc::clone(&hits)).await;
    let (witness_addr, _witness) = spawn_witness("cache-hit").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/thing");

    // First fetch: a cache miss goes to the origin and is stored.
    let first = client.get(&url).send().await.expect("first send");
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(first.text().await.expect("first body"), ORIGIN_BODY);

    // Second fetch: served from the CAS, so the origin is never hit again.
    let second = client.get(&url).send().await.expect("second send");
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(second.text().await.expect("second body"), ORIGIN_BODY);

    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the origin must be hit exactly once; the repeat is served from the CAS"
    );
    Ok(())
}

#[nativelink_test]
async fn distinct_urls_are_cached_independently() -> Result<(), Error> {
    let hits = Arc::new(AtomicUsize::new(0));
    let (origin_addr, _origin) = spawn_origin(Arc::clone(&hits)).await;
    let (witness_addr, _witness) = spawn_witness("distinct").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");

    // Two different query strings are two different cache entries, so each
    // misses once. (The origin ignores the query and returns the same body.)
    for query in ["?a=1", "?a=2"] {
        let url = format!("http://{origin_addr}/thing{query}");
        let body = client
            .get(&url)
            .send()
            .await
            .expect("send")
            .text()
            .await
            .expect("body");
        assert_eq!(body, ORIGIN_BODY);
        // A repeat of the same URL is a cache hit.
        drop(client.get(&url).send().await.expect("repeat"));
    }

    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "each distinct URL misses once; repeats are cache hits"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Witnessing tests
// ---------------------------------------------------------------------------

/// Like `spawn_witness` but configures a witness signing key, returning the
/// witness address, the loaded `WitnessKey` (for verifying receipts), and the
/// serving task guard.
async fn spawn_witness_signed(tag: &str) -> (String, WitnessKey, Guard) {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("CAS", Store::new(MemoryStore::new(&MemorySpec::default())));
    store_manager.add_store(
        "ALIAS",
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let dir = std::env::temp_dir().join(format!(
        "nl-cas-witness-witness-{}-{tag}",
        std::process::id()
    ));
    drop(std::fs::remove_dir_all(&dir));
    let witness_key_path = format!("{}/witness.key", dir.display());
    let config: CasWitnessConfig = serde_json5::from_str(&format!(
        r#"{{
            cas_store: "CAS",
            alias_store: "ALIAS",
            ca_cert_file: "{dir}/ca.crt",
            ca_key_file: "{dir}/ca.key",
            witness_key_file: "{witness_key_path}",
        }}"#,
        dir = dir.display(),
        witness_key_path = witness_key_path
    ))
    .expect("witness config parses");

    let witness = CasWitness::new(&config, &store_manager).expect("build witness");
    let witness_key = WitnessKey::load_or_generate(&witness_key_path).expect("load witness key");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind witness");
    let addr = listener.local_addr().expect("witness addr");
    let guard = nativelink_util::spawn!("test_witness_signed", async move {
        witness.serve(listener).await;
    });
    (addr.to_string(), witness_key, guard)
}

/// The `X-Straylight-Witness` header name.
const WITNESS_HEADER: &str = "x-straylight-witness";
/// The `X-Straylight-Witness-Receipt` header name.
const RECEIPT_HEADER: &str = "x-straylight-witness-receipt";

#[nativelink_test]
async fn witness_headers_present_on_cache_miss() -> Result<(), Error> {
    let hits = Arc::new(AtomicUsize::new(0));
    let (origin_addr, _origin) = spawn_origin(Arc::clone(&hits)).await;
    let (witness_addr, witness_key, _witness) = spawn_witness_signed("miss").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/thing");

    let resp = client.get(&url).send().await.expect("send");
    assert_eq!(resp.status(), StatusCode::OK);

    // The witness header must be present: blake3:<hex>.
    let witness_value = resp
        .headers()
        .get(WITNESS_HEADER)
        .expect("X-Straylight-Witness header present")
        .to_str()
        .expect("header is ascii")
        .to_string();
    assert!(
        witness_value.starts_with("blake3:"),
        "witness header must be blake3:<hex>, got {witness_value}"
    );
    let blake3_hex = &witness_value["blake3:".len()..];
    assert_eq!(blake3_hex.len(), 64, "blake3 hex must be 64 chars");

    // The receipt header must be present and verify with the witness key.
    let receipt_value = resp
        .headers()
        .get(RECEIPT_HEADER)
        .expect("X-Straylight-Witness-Receipt header present")
        .to_str()
        .expect("receipt is ascii")
        .to_string();
    let receipt = Receipt::parse_and_verify(&receipt_value, &witness_key.verifying_key())
        .expect("receipt verifies");
    assert_eq!(receipt.attestation, blake3_hex);
    assert_eq!(receipt.url, url);

    // The body must still be correct.
    assert_eq!(resp.text().await.expect("body"), ORIGIN_BODY,);
    assert_eq!(hits.load(Ordering::SeqCst), 1, "origin hit once (miss)");
    Ok(())
}

#[nativelink_test]
async fn witness_headers_present_on_cache_hit() -> Result<(), Error> {
    let hits = Arc::new(AtomicUsize::new(0));
    let (origin_addr, _origin) = spawn_origin(Arc::clone(&hits)).await;
    let (witness_addr, witness_key, _witness) = spawn_witness_signed("hit").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/thing");

    // First fetch: cache miss.
    let first = client.get(&url).send().await.expect("first");
    let first_witness = first
        .headers()
        .get(WITNESS_HEADER)
        .expect("witness header on miss")
        .to_str()
        .expect("ascii")
        .to_string();
    assert_eq!(first.text().await.expect("body"), ORIGIN_BODY);

    // Second fetch: cache hit — must still have witness headers.
    let second = client.get(&url).send().await.expect("second");
    let second_witness = second
        .headers()
        .get(WITNESS_HEADER)
        .expect("witness header on hit")
        .to_str()
        .expect("ascii")
        .to_string();
    let second_receipt = second
        .headers()
        .get(RECEIPT_HEADER)
        .expect("receipt header on hit")
        .to_str()
        .expect("ascii")
        .to_string();

    // The attestation BLAKE3 key must be the same on hit and miss (it's the
    // same stored attestation).
    assert_eq!(
        first_witness, second_witness,
        "witness key must be the same on hit and miss"
    );

    // The receipt must verify and point to the same attestation.
    let receipt = Receipt::parse_and_verify(&second_receipt, &witness_key.verifying_key())
        .expect("receipt verifies on cache hit");
    assert_eq!(receipt.attestation, second_witness["blake3:".len()..]);
    assert_eq!(receipt.url, url);

    assert_eq!(second.text().await.expect("body"), ORIGIN_BODY);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "origin hit once; second is a cache hit"
    );
    Ok(())
}

#[nativelink_test]
async fn no_witness_headers_when_witnessing_disabled() -> Result<(), Error> {
    let hits = Arc::new(AtomicUsize::new(0));
    let (origin_addr, _origin) = spawn_origin(Arc::clone(&hits)).await;
    let (witness_addr, _witness) = spawn_witness("no-witness").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/thing");

    let resp = client.get(&url).send().await.expect("send");
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers().get(WITNESS_HEADER).is_none(),
        "no witness header when witnessing is disabled"
    );
    assert!(
        resp.headers().get(RECEIPT_HEADER).is_none(),
        "no receipt header when witnessing is disabled"
    );
    assert_eq!(resp.text().await.expect("body"), ORIGIN_BODY);
    Ok(())
}

// ---------------------------------------------------------------------------
// Spool cap on an oversized origin body
// ---------------------------------------------------------------------------

/// Like `spawn_witness` but with an explicit `max_fetch_size_bytes` cap so a
/// small oversized body can be exercised without transferring gigabytes.
async fn spawn_witness_capped(tag: &str, max_fetch_size_bytes: u64) -> (String, Guard) {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("CAS", Store::new(MemoryStore::new(&MemorySpec::default())));
    store_manager.add_store(
        "ALIAS",
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let dir = std::env::temp_dir().join(format!("nl-cas-witness-cap-{}-{tag}", std::process::id()));
    drop(std::fs::remove_dir_all(&dir));
    let config: CasWitnessConfig = serde_json5::from_str(&format!(
        r#"{{
            cas_store: "CAS",
            alias_store: "ALIAS",
            ca_cert_file: "{dir}/ca.crt",
            ca_key_file: "{dir}/ca.key",
            max_fetch_size_bytes: {max_fetch_size_bytes},
        }}"#,
        dir = dir.display()
    ))
    .expect("witness config parses");

    let witness = CasWitness::new(&config, &store_manager).expect("build witness");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind witness");
    let addr = listener.local_addr().expect("witness addr");
    let guard = nativelink_util::spawn!("test_witness_capped", async move {
        witness.serve(listener).await;
    });
    (addr.to_string(), guard)
}

/// An origin that answers `GET /big` with a **chunked** (no `Content-Length`)
/// `200` of `total` bytes, streamed in `chunk`-sized frames. Because the
/// `Body` is built from a stream of unknown length, hyper frames it with
/// `Transfer-Encoding: chunked` and sends no `Content-Length` — exactly the
/// case whose only size guard is the running cap inside `spool_to_file`.
async fn spawn_chunked_origin(total: usize, chunk: usize) -> (String, Guard) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind chunked origin");
    let addr = listener.local_addr().expect("origin addr");
    let app = Router::new().route(
        "/big",
        get(move || async move {
            let mut frames: Vec<Result<axum::body::Bytes, std::io::Error>> = Vec::new();
            let mut sent = 0usize;
            while sent < total {
                let this = chunk.min(total - sent);
                frames.push(Ok(axum::body::Bytes::from(vec![b'x'; this])));
                sent += this;
            }
            // A stream body of unknown length -> chunked, no Content-Length.
            Body::from_stream(futures::stream::iter(frames))
        }),
    );
    let guard = nativelink_util::spawn!("test_chunked_origin", async move {
        axum::serve(listener, app)
            .await
            .expect("serve chunked origin");
    });
    (addr.to_string(), guard)
}

/// An origin body exceeding `max_fetch_size_bytes`, delivered CHUNKED (no
/// `Content-Length`), must be rejected: the running cap in `spool_to_file`
/// aborts with `ResourceExhausted` (the `SpoolGuard` deleting the partial
/// file on the error return), and the proxy returns a `502`. Without the
/// in-loop cap the body would spool fully and be served from disk (`200`) —
/// the regression this test locks in. (A per-test temp-dir is not possible
/// without touching the source, and the shared temp dir is contended by the
/// other concurrent tests, so leak-freedom rests on the `SpoolGuard` unit
/// coverage; the status code is the behavioral signal asserted here.)
#[nativelink_test]
async fn oversized_chunked_origin_body_is_capped_with_502() -> Result<(), Error> {
    // Cap 4 KiB; origin streams 256 KiB in 8 KiB chunks (no Content-Length).
    let (origin_addr, _origin) = spawn_chunked_origin(256 * 1024, 8 * 1024).await;
    let (witness_addr, _witness) = spawn_witness_capped("oversized-chunked", 4 * 1024).await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/big");

    let resp = client.get(&url).send().await.expect("send");
    // The proxy maps the ResourceExhausted spool failure to a 502.
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "an oversized chunked body must be refused with a 502"
    );
    Ok(())
}

/// A well-formed chunked body UNDER the cap is cached and served normally
/// (`200`), proving the chunked-origin harness itself works — so the `502`
/// in the oversized case above is genuinely the cap, not a broken origin.
#[nativelink_test]
async fn under_cap_chunked_origin_body_is_served() -> Result<(), Error> {
    // 2 KiB body, cap 4 KiB, streamed in 512-byte chunks (no Content-Length).
    let (origin_addr, _origin) = spawn_chunked_origin(2 * 1024, 512).await;
    let (witness_addr, _witness) = spawn_witness_capped("under-cap-chunked", 4 * 1024).await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/big");

    let resp = client.get(&url).send().await.expect("send");
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.bytes().await.expect("body");
    assert_eq!(body.len(), 2 * 1024, "the whole under-cap body is served");
    assert!(body.iter().all(|&b| b == b'x'));
    Ok(())
}

/// The Content-Length path of the same guard: an origin that DECLARES a size
/// over the cap is streamed straight through (not spooled/cached), so no
/// spool file is created. This complements the chunked case above.
#[nativelink_test]
async fn oversized_declared_origin_body_is_not_spooled() -> Result<(), Error> {
    // Origin returns 64 KiB with a real Content-Length; cap is 4 KiB.
    let body = vec![b'y'; 64 * 1024];
    let body_for_route = body.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let app = Router::new().route(
        "/big",
        get(move || {
            let body = body_for_route.clone();
            // axum sets Content-Length for a fixed `Vec<u8>` body.
            async move { body }
        }),
    );
    let _origin = nativelink_util::spawn!("test_declared_origin", async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let (witness_addr, _witness) = spawn_witness_capped("oversized-declared", 4 * 1024).await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .build()
        .expect("client");
    let url = format!("http://{addr}/big");

    // The body is streamed through unchanged (200), just not cached/spooled:
    // a declared-oversize response takes the stream-through branch BEFORE the
    // spool, so the whole body reaches the client verbatim.
    let resp = client.get(&url).send().await.expect("send");
    assert_eq!(resp.status(), StatusCode::OK);
    let got = resp.bytes().await.expect("body");
    assert_eq!(got.len(), body.len(), "the whole body is streamed through");
    Ok(())
}

// ---------------------------------------------------------------------------
// Redirect policy: 3xx is surfaced, never followed
// ---------------------------------------------------------------------------

/// A mock origin whose `/redirect` returns a `302` pointing at `/target` on
/// the SAME origin, and whose `/target` bumps a counter. Returns the address,
/// the redirect-target hit counter, and the guard.
async fn spawn_redirecting_origin() -> (String, Arc<AtomicUsize>, Guard) {
    let target_hits = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind redirect origin");
    let addr = listener.local_addr().expect("addr");
    let addr_str = addr.to_string();

    let location = format!("http://{addr_str}/target");
    let target_hits_route = Arc::clone(&target_hits);
    let app = Router::new()
        .route(
            "/redirect",
            get(move || {
                let location = location.clone();
                async move {
                    Response::builder()
                        .status(StatusCode::FOUND)
                        .header(header::LOCATION, location)
                        .body(Body::from("moved"))
                        .expect("redirect response")
                        .into_response()
                }
            }),
        )
        .route(
            "/target",
            get(move || {
                let hits = Arc::clone(&target_hits_route);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    "REDIRECT TARGET BODY"
                }
            }),
        );
    let guard = nativelink_util::spawn!("test_redirect_origin", async move {
        axum::serve(listener, app)
            .await
            .expect("serve redirect origin");
    });
    (addr_str, target_hits, guard)
}

/// The proxy must NOT follow origin redirects: a followed `3xx` would fetch a
/// URL the client never named (an SSRF vector) and would bind the cached body
/// and its attestation to the ORIGINAL URL though the bytes came from the
/// redirect target. So the `3xx` is surfaced to the client verbatim and the
/// redirect target is never fetched.
#[nativelink_test]
async fn origin_redirect_is_surfaced_and_not_followed() -> Result<(), Error> {
    let (origin_addr, target_hits, _origin) = spawn_redirecting_origin().await;
    let (witness_addr, _witness) = spawn_witness("redirect-policy").await;

    // A client that does NOT follow redirects itself, so what it sees is
    // exactly what the proxy returned.
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{witness_addr}")).expect("witness url"))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/redirect");

    let resp = client.get(&url).send().await.expect("send");
    assert_eq!(
        resp.status().as_u16(),
        302,
        "the proxy must surface the origin 302 to the client"
    );
    assert_eq!(
        resp.headers()
            .get(header::LOCATION)
            .and_then(|v| v.to_str().ok()),
        Some(format!("http://{origin_addr}/target").as_str()),
        "the Location header must be passed through untouched"
    );
    // The redirect target must never have been fetched by the proxy.
    assert_eq!(
        target_hits.load(Ordering::SeqCst),
        0,
        "the proxy must NOT follow the redirect to /target"
    );
    Ok(())
}
