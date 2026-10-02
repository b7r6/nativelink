# Multi-Protocol Transport — HTTP/1.1 + HTTP/2 + HTTP/3 across all surfaces

- **Status:** Draft / Proposed
- **Date:** 2026-07-17
- **Owner:** b7r6
- **Scope:** the `straylight-nativelink` fork's own network surfaces

## Summary

Serve **HTTP/1.1, HTTP/2, and HTTP/3 on every network surface we own**, with
graceful negotiation (ALPN on TCP, `Alt-Svc` advertising the H3 endpoint) and
correct fallback. The design leverages one property the fork already has: every
served surface is — or trivially becomes — a single `tower::Service<http::Request>`.
So this is not three implementations per surface; it is **one service core behind
three transport frontends**, built once and reused everywhere.

The only surfaces that stay HTTP-version-constrained are ones we do not own:
external egress (Cloudflare R2, public OCI registries, `cache.nixos.org`) gets
*opportunistic* H3 where the remote offers it, and a native REAPI client that
cannot exceed HTTP/2 (Bazel's grpc-java/c++) leaves an H2 island — the single
place H3 is unreachable, and only if that client is retained.

## Motivation

QUIC/H3 is production-grade, and this fork controls an unusually large fraction
of its own client population (a from-scratch `nix` client, a Firecracker-class
worker sandbox, Buck2's tonic-based RE client). That removes the usual blocker —
"the clients only speak HTTP/1.1" — for everything except the genuinely external
edges. Supporting all three versions gives:

- **H3** where both ends are ours: fewer round-trips (0-RTT resumption), no TCP
  head-of-line blocking across multiplexed streams, connection migration.
- **H2** for native gRPC and for REAPI clients that require it (Bazel, Buck2).
- **H1** for the widest interop (plain HTTP tools, `curl`, gRPC-Web/Connect
  clients, anything behind an H1-only middlebox).

No client is left behind, and capable clients transparently upgrade.

### Goals

- Every fork-owned surface negotiates H1, H2, and H3.
- One reusable tri-protocol frontend; no per-surface transport code.
- Transparent `Alt-Svc` upgrade to H3 with correct, silent fallback to H2/H1.
- Opportunistic H3 on outbound fetches where the remote supports it.
- Per-listener config for which versions are enabled; per-connection metrics
  tagged with the negotiated version.

### Non-goals

- Making external services speak QUIC (R2/S3, public registries, upstream Nix
  caches) — out of our control; opportunistic outbound H3 only.
- Forcing "H3-only." H3 is always additive (advertise-and-upgrade); H1/H2 remain.
- Changing REAPI semantics or the Nix binary-cache protocol. This is transport,
  not protocol.

## Background — the current transport map

Grounded in the tree (file:line):

| Surface | Today | Where |
|---|---|---|
| REAPI gRPC (CAS/AC/Exec/ByteStream/Capabilities/Fetch/Push/BEP/WorkerApi/Health) | tonic 0.14.6 → hyper 1.6 → h2 0.4.14, ALPN `h2` | `src/bin/nativelink.rs:371` (`Routes::builder`), `:463` (`into_axum_router`), `:664` (hyper `auto::Builder`) |
| nix_cache (Nix binary cache) | Axum 0.8.3 over hyper, HTTP/1.1 | `nativelink-service/src/nix_cache_server.rs`, wired ~`nativelink.rs:548` |
| cas_witness (fetch proxy) | hyper HTTP/1.1 `CONNECT`, TLS-MITM | `nativelink-service/src/cas_witness.rs:540,550` |
| worker → scheduler | tonic `Channel` (`grpc://…`) | `nativelink-worker/src/local_worker.rs:715-743` |
| worker → CAS | `GrpcStore` (BatchRead/Update + ByteStream) | `nativelink-store/src/grpc_store.rs` |
| Server TLS / mTLS | tokio-rustls, `WebPkiClientVerifier` | `nativelink.rs:560-634`, `nativelink-util/src/tls_utils.rs:55,597` |
| Outbound (upstream caches, origins, registries, cache client) | reqwest 0.12.15 (`rustls-tls`, `stream`) | `nix_cache_server.rs`, `cas_witness.rs`, `nativelink-oci/src/registry.rs`, `nativelink-nix-client/src/client.rs` |
| Object store | aws-sdk-s3 1.82.0 over hyper | `nativelink-store/src/r2_store.rs` |

Two enabling facts:

- **Everything already funnels through `tower`/axum.** The tonic REAPI services
  are converted to an axum router (`into_axum_router()`, `nativelink.rs:463`) and
  nix_cache is an axum `Router`. Whether these are one merged router or a few
  listeners is a topology detail — each is a `tower::Service<http::Request>`, which
  is all the tri-protocol frontend needs.
- **`quinn 0.11.9` is already transitive** in `Cargo.lock`, and **BLAKE3 is
  already a supported digest function** (`nativelink-util/src/digest_hasher.rs`;
  default SHA256, per-request, strict mode available) — relevant if the QUIC path
  ever grows a content-addressed transfer mode.

## Decision

Adopt a **one-service-core, three-transport-frontend** architecture. Build the
tri-protocol frontend once in `nativelink-util`; apply it to every fork-owned
surface. Keep HTTP at external egress with opportunistic outbound H3. Accept a
single H2 island for Bazel only if Bazel remains a REAPI client.

## Architecture

```
        ┌───────────────── one tower::Service<http::Request<Body>> ─────────────────┐
        │  tonic REAPI routes  +  nix_cache routes   (+ gRPC-Web layer, + Alt-Svc)   │
        └───────────────┬──────────────────────┬──────────────────────────┬─────────┘
                        │                      │                          │
                H1 (hyper/TCP)         H2 (hyper/TCP, ALPN h2)     H3 (h3+quinn/UDP, ALPN h3)
                └──────── already: hyper_util auto::Builder ───────┘        │
                        └─── Alt-Svc: h3=":<port>" advertises the UDP endpoint ───┘
```

- **H1 + H2** are already served by `hyper_util::server::conn::auto::Builder`
  (`nativelink.rs:664`) with ALPN selecting `h2` vs `http/1.1`.
- **H3** is a new frontend: a `quinn` endpoint on a UDP socket, driving the `h3`
  crate, reconstructing `http::Request`, calling the *same* service, and streaming
  the `http::Response` back.
- **`Alt-Svc`** is a response-header middleware on the shared service so H1/H2
  clients discover the H3 endpoint and upgrade.

The frontend is a small `nativelink-util` component:

```
serve_multi_protocol(
    svc:      impl tower::Service<http::Request<Body>, Response = http::Response<Body>> + Clone,
    tcp:      TcpListener,          // H1 + H2 (hyper auto)
    udp:      UdpSocket,            // H3 (quinn + h3)
    tls:      Arc<rustls::ServerConfig>,   // one cert; ALPN {h2, http/1.1} on TCP, {h3} on quinn
    versions: EnabledVersions,      // per-listener toggles
    alt_svc:  AltSvcAdvertisement,
)
```

## Per-surface treatment

| Surface | H1 | H2 | H3 |
|---|---|---|---|
| nix_cache (plain HTTP) | native | native | via frontend |
| REAPI gRPC | **gRPC-Web/Connect** (native gRPC needs H2) | native tonic | trailer-bridge over h3 |
| worker↔scheduler / worker↔CAS | n/a (gRPC) | native tonic | trailer-bridge over h3 |
| cas_witness `CONNECT` | native | extended-CONNECT (RFC 8441) | CONNECT-UDP / MASQUE — **retire instead** |
| external egress (R2, registries, upstream) | — | — | opportunistic outbound only |

### nix_cache — trivial

It is HTTP semantics; the same axum `Router` serves over all three transports.
No surface-specific work beyond wiring it to the frontend.

### REAPI gRPC — the nuance that matters

Native gRPC *requires* HTTP/2 framing (length-prefixed messages + `grpc-status`
trailer). Therefore:

- **H2** — native tonic. Already done.
- **H3** — small, because tonic 0.14 speaks `http` + `http-body 1.0` (`Frame` =
  data **or** trailers) and does not care what moved the bytes. The work is a
  **bridge**, not a tonic fork: the h3 frontend maps `http-body` `Frame::trailers`
  ↔ `h3`'s trailer send/recv, so `grpc-status` survives. Streaming RPCs
  (ByteStream, Execution operations, WorkerApi bidi) ride QUIC bidirectional
  streams natively. (Forking tonic is on the table and is small; the trailer
  bridge is smaller and preferred.)
- **H1** — **there is no native gRPC over HTTP/1.1.** "gRPC on H1" means
  **gRPC-Web** (or the Connect protocol) via a `tonic-web`/tower layer. This
  serves browsers and gRPC-Web tooling — **not Bazel** (grpc-java/c++ require H2
  minimum). Naming it plainly: "H1 across the gRPC surface" buys gRPC-Web compat,
  not a Bazel fallback.

### cas_witness — the one genuine exception, and the recommendation to retire it

`CONNECT` is transport-version-specific — H1 `CONNECT`, H2 extended-`CONNECT`
(RFC 8441, `SETTINGS_ENABLE_CONNECT_PROTOCOL`), H3 `CONNECT-UDP`/MASQUE (RFC 9298)
— three mechanisms, not one handler over three transports. cas_witness exists only
to intercept stock `nix`'s `fetchurl` because we could not change `nix`. **We are
rewriting `nix`**, so fixed-output fetches become CAS-aware natively (ask the CAS
for `sha256(body)` directly), and the TLS-MITM proxy has no reason to exist.
**Recommendation: retire cas_witness** rather than port a MITM proxy to MASQUE. If
retained, it is the only surface needing per-version `CONNECT` code.

### Outbound / client side

The client population must *negotiate* all three:

- Our `nix` rewrite and `nl-nix`: dial H1/H2/H3 with `Alt-Svc` discovery +
  happy-eyeballs (race H3, fall back to H2 on QUIC failure).
- reqwest fetches (upstream caches, origins, registries): H1/H2 native; **H3 is
  behind `--cfg reqwest_unstable`** — a build-wide cfg, not a cargo feature (see
  Risks). Enable per-client; auto-upgrades via `Alt-Svc`.
- Worker `Channel` (`local_worker.rs:715`): same tri-version dial for the
  scheduler/CAS legs.
- aws-sdk-s3 (R2): stays hyper H1/H2; a custom H3 connector is possible but not
  worth it (throughput-bound, not RTT-bound).

## Detailed design — the parts that need care

1. **One cert, three ALPNs.** Load the same rustls cert into the tokio-rustls
   acceptor (`ALPN {h2, http/1.1}`) and the quinn `ServerConfig` (`ALPN {h3}`).
   The tailscale-cert rotation timer (`nativelink-tls-cert`) must reload **both**.
2. **UDP + firewall.** H3 is UDP on the same port number as TCP by convention;
   the NixOS nativelink module must open the UDP port. Enable UDP GSO/GRO for
   throughput.
3. **`Alt-Svc` + fallback correctness.** The UX is: H1/H2 response advertises
   `alt-svc: h3=":<port>"; ma=…`; the client caches it and races H3 next time;
   on QUIC failure (blocked UDP, path-MTU black-hole) it falls back silently.
   Getting fallback right is the real reliability work, not the happy path.
4. **Config surface.** Per-listener `{h1, h2, h3}` toggles beside the existing
   ~15 HTTP/2 knobs (`nativelink.rs:667-707`); add quinn knobs (max concurrent
   streams, flow-control windows, congestion controller). Schema in
   `nativelink-config`.
5. **Observability.** Tag every connection/RPC metric with the negotiated
   version (`h1`/`h2`/`h3`) via the existing OTLP pipeline, so H3 adoption and
   fallback rate are visible.
6. **Test matrix.** Each surface × `{curl --http1.1, --http2, --http3; grpcurl
   (H2); a gRPC-Web client (H1); our nix (H3)}`, plus fallback fault injection
   (drop UDP, shrink MTU) to prove silent degradation.

## Risks and open questions

- **`h3` crate is pre-1.0** — API churn; we track a moving target (quinn itself
  is solid). Mitigation: isolate all h3 usage inside the one frontend component.
- **`reqwest_unstable` is build-wide** — enabling reqwest H3 ripples across the
  workspace build (a `RUSTFLAGS` cfg, not a localized feature). Decide whether
  outbound H3 is worth that friction now or later.
- **Bazel H2 island** — grpc-java/c++ will not ride H3, and we will not fork
  Bazel. If Bazel remains a REAPI client, its leg is H2-gRPC forever. Open
  question: **is Bazel a retained client, or is the client set only our nix +
  Buck2 (tonic, forkable)?** This is the one product decision that changes scope.
- **QUIC operational surface** — path-MTU discovery, UDP buffer sizing, GSO/GRO,
  amplification limits, and middlebox UDP blocking are new failure modes to
  instrument and alert on.
- **CONNECT semantics** — if cas_witness is *not* retired, per-version CONNECT
  (esp. MASQUE) is real, isolated work; retiring it is strongly preferred.

## Implementation plan (phased)

1. **Tri-protocol frontend component** (`nativelink-util`): wrap a `tower::Service`
   → serve H1+H2 (existing) + H3 (quinn+h3), inject `Alt-Svc`. **Prove it on
   nix_cache first** (pure HTTP, no gRPC subtlety). Deliverable: `curl
   --http3` fetches a NAR; H1/H2 unaffected.
2. **gRPC-over-H3 trailer bridge**: `http-body` `Frame::trailers` ↔ h3 trailers.
   Validate ByteStream and a bidi stream (WorkerApi) end to end over H3.
3. **gRPC-Web/Connect layer** for H1 on the gRPC routes (`tonic-web`).
4. **Client side**: `Alt-Svc`/H3 in our nix + the worker `Channel`; reqwest
   `http3` (gated on the `reqwest_unstable` decision).
5. **Retire cas_witness** (native CAS-aware FOD fetch in our nix) — or, if kept,
   per-version `CONNECT`.
6. **Plumbing**: config toggles, NixOS UDP firewall, metric version-tagging, the
   full test matrix.

Effort concentrates in phases 1–2; the rest reuses existing patterns. The
frontend is built once and every surface inherits all three versions.

## Alternatives considered

- **Pick a single version.** Rejected: leaves clients behind (H1-only tooling, or
  H2-only Bazel, or no H3 upgrade). The whole point is maximal compatibility.
- **Content-addressed QUIC substrate instead of HTTP-over-QUIC** (raw quinn /
  `iroh-blobs`, protobuf-on-QUIC-streams). This is the leaner shape for a CAS +
  content-addressed `nix` + microVM fleet, and — once we are forking transport
  libs anyway — is roughly the same spend as gRPC-over-H3. It was **not** chosen
  because it drops HTTP semantics we still want for interop (REAPI wire compat,
  the Nix binary-cache protocol, external egress). H1/H2/H3 keeps every existing
  wire while adding QUIC underneath. The substrate remains a possible *additional*
  internal store-tier (see the iroh evaluation), not a replacement for this.
- **Full tonic fork for gRPC-over-H3.** Small, but the trailer bridge is smaller
  and keeps us on upstream tonic; preferred unless the bridge hits a wall.

## References

- Current stack: `src/bin/nativelink.rs:371,463,560-634,664,667-707`;
  `nativelink-service/src/{nix_cache_server.rs,cas_witness.rs}`;
  `nativelink-worker/src/local_worker.rs:715-743`;
  `nativelink-store/src/grpc_store.rs`; `nativelink-util/src/tls_utils.rs`.
- Digest functions / BLAKE3: `nativelink-util/src/digest_hasher.rs`;
  `nativelink-config/src/stores.rs:39-46`.
- Deps: tonic 0.14.6, hyper 1.6.0, h2 0.4.14, axum 0.8.3, reqwest 0.12.15,
  quinn 0.11.9 (transitive), aws-sdk-s3 1.82.0, tokio-rustls 0.26.2.
- RFCs: 9114 (HTTP/3), 9000 (QUIC), 8441 (H2 extended CONNECT), 9298 (CONNECT-UDP /
  MASQUE), 7838 (`Alt-Svc`).
- Companion: the `iroh` networking evaluation (content-addressed QUIC substrate).
