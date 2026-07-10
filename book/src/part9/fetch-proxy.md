# The Caching Fetch Proxy

The [Nix substituter facade](./nix-substituter.md) makes NativeLink a durable
mirror of *store paths*. But a build also pulls raw bytes off the network —
`fetchurl` tarballs, release archives, anything a fixed-output derivation
downloads — and those fetches only become substitutable *after* someone has
built the derivation and pushed its output. The fetch proxy closes that gap: it
is a caching HTTP forward proxy that stores every fetched body in the CAS, so
the first download of a URL makes the deployment a durable mirror of exactly
what the builds pull, no store path required.

It is the third member of the family. The [OCI → CAS bridge](./oci-cas-bridge.md)
pulls foreign images *into* the CAS; the Nix facade serves CAS content *out* in
a foreign protocol; the fetch proxy sits *between* a client and the open
internet, teeing whatever crosses it into the CAS.

## Why a proxy, and why it intercepts TLS

Nix downloads through libcurl, which honors `HTTP_PROXY`/`HTTPS_PROXY`, and the
NixOS manual documents pointing `NIX_SSL_CERT_FILE` at an intercepting proxy's
CA. That is the whole integration: two environment variables, no URL rewriting,
no nixpkgs overlay.

```bash
export HTTPS_PROXY=http://cache.example.com:50080
export HTTP_PROXY=http://cache.example.com:50080
export NIX_SSL_CERT_FILE=/var/lib/nativelink/fetch-proxy/ca.crt
```

The catch is that almost everything worth caching is HTTPS, and an HTTPS request
through a proxy is a `CONNECT` tunnel — opaque, encrypted end to end. To see
(and cache) the bytes, the proxy must terminate TLS toward the client: it
answers `CONNECT host:443`, presents a certificate for `host` minted on the fly
by a locally-generated CA, and opens its own TLS connection onward to the real
origin. This is deliberate man-in-the-middle, and it is safe here for one
reason: **nix verifies every fixed-output derivation against its declared hash**
regardless of what the proxy serves, so the proxy is never trusted for
integrity — only for availability. A stale or wrong cache entry fails a build's
hash check; it can never corrupt one.

The CA private key can impersonate any host to a client that trusts it, so it
never leaves the machine, is written `0600`, and should be trusted only by the
build clients that use the proxy.

## Architecture

```
   nix (HTTPS_PROXY, NIX_SSL_CERT_FILE)
                │  CONNECT host:443
                ▼
   ┌───────────────────────────────────────────┐
   │                fetch proxy                  │
   │  CONNECT ─► mint leaf cert for host ─► TLS  │
   │  GET https://host/path                      │
   │     hit  ─► stream body from CAS            │
   │     miss ─► fetch origin ─► spool ─► CAS    │
   └──────┬───────────────────────┬──────────────┘
          │                       │
          ▼                       ▼
     alias_store              cas_store
     url → (digest,           sha256(body) blobs,
     size, content-type)      verify{} ◄──────┘
```

Two stores, mirroring the substituter's split: `cas_store` holds each body
under `DigestInfo(sha256(body), size)` (share it with the gRPC CAS if you
like), and the string-keyed `alias_store` maps a URL to the `(digest, size,
content-type)` of its body under the slash-free key
`fetch:{nixbase32(sha256(url))}`.

## The Request Path

`FetchProxy` owns its whole listener because it speaks the proxy protocol, not
the shared gRPC/axum stack. Each connection is served with HTTP/1 (with
upgrades):

- **`CONNECT host:port`** → the proxy replies `200`, mints (and caches) a
  `rustls` server config presenting a leaf certificate for `host` signed by the
  CA, accepts TLS over the upgraded stream, and then serves the decrypted HTTP
  requests, reconstructing each absolute URL as `https://host/path`.
- **An absolute-form request** (plain-HTTP proxying) is handled the same way
  without interception.

For a `GET`, the proxy consults the `alias_store`; on a hit whose body blob is
still present it streams the body straight from the CAS. On a miss it fetches
the origin, and if the response is a `200` with a `Content-Length` within
`max_fetch_size_bytes` it spools the body to disk while hashing, uploads it to
the CAS under `sha256(body)`, records the URL alias, and then serves it from the
CAS. Responses that are not cacheable — a non-`200`, an unknown length, an
over-cap body — and every non-`GET` method are proxied straight through,
uncached. The body is fetched eagerly before the client is answered, which is
exactly nix's own behavior (it downloads a fixed-output derivation in full
before hashing it).

Cache entries are **immutable**: a URL maps to the bytes seen on the first
successful fetch, matching nix's contract that a `fetchurl` URL and hash name
immutable content. A different URL (query string included) is a different entry.

## Configuration

`nativelink-config/examples/http_cache_proxy.json5` is a full runnable example:
a `verify`-wrapped fast/slow CAS for bodies, a plain memory store for the URL
index, and one proxy listener.

```json5
services: {
  http_cache_proxy: {
    cas_store: "FETCH_CAS",
    alias_store: "FETCH_ALIAS",
    ca_cert_file: "/var/lib/nativelink/fetch-proxy/ca.crt",  // trust this on clients
    ca_key_file: "/var/lib/nativelink/fetch-proxy/ca.key",   // keep private (0600)
    max_fetch_size_bytes: 2147483648,                        // 2 GiB; larger is proxied uncached
    fetch_timeout_s: 300,
  },
}
```

The CA certificate and key are generated together on first start if either file
is missing. Point every build client's `NIX_SSL_CERT_FILE` at the certificate.

## Code Map

| File | Purpose |
|---|---|
| `nativelink-service/src/fetch_proxy.rs` | The CA (leaf-cert minting), the proxy connection handling, and the URL→CAS caching |
| `nativelink-config/src/cas_server.rs` | `HttpCacheProxyConfig` schema and defaults |
| `nativelink-service/tests/fetch_proxy_test.rs` | Caching behavior driven over real loopback sockets |
