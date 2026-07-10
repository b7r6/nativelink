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

//! Integration tests for the caching HTTP forward proxy (`FetchProxy`).
//!
//! These drive the proxy over real loopback sockets: a plain-HTTP origin that
//! counts hits, the proxy in front of it, and a `reqwest` client configured to
//! use the proxy. This exercises the whole caching path — absolute-form proxy
//! request parsing, the URL→CAS index, origin fetch + spool into CAS, and
//! serving a repeat request from the CAS — without needing TLS interception
//! (the CONNECT/MITM certificate machinery is covered by the `fetch_proxy`
//! unit tests).

use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use nativelink_config::cas_server::HttpCacheProxyConfig;
use nativelink_config::stores::MemorySpec;
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_service::fetch_proxy::FetchProxy;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::Store;

const ORIGIN_BODY: &str = "nativelink fetch-proxy origin body: deterministic 0123456789";

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

/// Builds a `FetchProxy` over fresh in-memory stores, serves it on an ephemeral
/// loopback port, and returns the proxy address and the serving task guard.
async fn spawn_proxy(tag: &str) -> (String, Guard) {
    let store_manager = Arc::new(StoreManager::new());
    store_manager.add_store("CAS", Store::new(MemoryStore::new(&MemorySpec::default())));
    store_manager.add_store(
        "ALIAS",
        Store::new(MemoryStore::new(&MemorySpec::default())),
    );

    let dir = std::env::temp_dir().join(format!("nl-fetch-proxy-it-{}-{tag}", std::process::id()));
    drop(std::fs::remove_dir_all(&dir));
    let config: HttpCacheProxyConfig = serde_json5::from_str(&format!(
        r#"{{
            cas_store: "CAS",
            alias_store: "ALIAS",
            ca_cert_file: "{dir}/ca.crt",
            ca_key_file: "{dir}/ca.key",
        }}"#,
        dir = dir.display()
    ))
    .expect("proxy config parses");

    let proxy = FetchProxy::new(&config, &store_manager).expect("build proxy");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind proxy");
    let addr = listener.local_addr().expect("proxy addr");
    let guard = nativelink_util::spawn!("test_proxy", async move {
        proxy.serve(listener).await;
    });
    (addr.to_string(), guard)
}

#[nativelink_test]
async fn caches_plain_http_get_and_serves_repeat_from_cas() -> Result<(), Error> {
    let hits = Arc::new(AtomicUsize::new(0));
    let (origin_addr, _origin) = spawn_origin(Arc::clone(&hits)).await;
    let (proxy_addr, _proxy) = spawn_proxy("cache-hit").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{proxy_addr}")).expect("proxy url"))
        .build()
        .expect("client");
    let url = format!("http://{origin_addr}/thing");

    // First fetch: a cache miss goes to the origin and is stored.
    let first = client.get(&url).send().await.expect("first send");
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    assert_eq!(first.text().await.expect("first body"), ORIGIN_BODY);

    // Second fetch: served from the CAS, so the origin is never hit again.
    let second = client.get(&url).send().await.expect("second send");
    assert_eq!(second.status(), reqwest::StatusCode::OK);
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
    let (proxy_addr, _proxy) = spawn_proxy("distinct").await;

    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::http(format!("http://{proxy_addr}")).expect("proxy url"))
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
