# The CAS Witness

The [Nix substituter facade](./nix-substituter.md) makes NativeLink a durable
mirror of *store paths*. But a build also pulls raw bytes off the network —
`fetchurl` tarballs, release archives, anything a fixed-output derivation
downloads — and those fetches only become substitutable *after* someone has
built the derivation and pushed its output. The CAS witness closes that gap: it
is a caching HTTP forward proxy that stores every fetched body in the CAS, so
the first download of a URL makes the deployment a durable mirror of exactly
what the builds pull, no store path required. And when it is given a signing
key, it does more than mirror: it *witnesses*, emitting a signed attestation
that binds each fetched body to the URL, time, and host that produced it.

It is the third member of the family. The [OCI → CAS bridge](./oci-cas-bridge.md)
pulls foreign images *into* the CAS; the Nix facade serves CAS content *out* in
a foreign protocol; the CAS witness sits *between* a client and the open
internet, teeing whatever crosses it into the CAS and, optionally, notarizing
the crossing.

## Why a proxy, and why it intercepts TLS

Nix downloads through libcurl, which honors `HTTP_PROXY`/`HTTPS_PROXY`, and the
NixOS manual documents pointing `NIX_SSL_CERT_FILE` at an intercepting proxy's
CA. That is the whole integration: two environment variables, no URL rewriting,
no nixpkgs overlay.

```bash
export HTTPS_PROXY=http://cache.example.com:50080
export HTTP_PROXY=http://cache.example.com:50080
export NIX_SSL_CERT_FILE=/var/lib/nativelink/cas-witness/ca.crt
```

The catch is that almost everything worth caching is HTTPS, and an HTTPS request
through a proxy is a `CONNECT` tunnel — opaque, encrypted end to end. To see
(and cache) the bytes, the CAS witness must terminate TLS toward the client: it
answers `CONNECT host:443`, presents a certificate for `host` minted on the fly
by a locally-generated CA, and opens its own TLS connection onward to the real
origin. This is deliberate man-in-the-middle, and it is safe here for one
reason: **nix verifies every fixed-output derivation against its declared hash**
regardless of what the CAS witness serves, so it is never trusted for
integrity — only for availability. A stale or wrong cache entry fails a build's
hash check; it can never corrupt one.

The CA private key can impersonate any host to a client that trusts it, so it
never leaves the machine, is written `0600`, and should be trusted only by the
build clients that use the CAS witness.

## Architecture

```
   nix (HTTPS_PROXY, NIX_SSL_CERT_FILE)
                │  CONNECT host:443
                ▼
   ┌───────────────────────────────────────────┐
   │                CAS witness                  │
   │  CONNECT ─► mint leaf cert for host ─► TLS  │
   │  GET https://host/path                      │
   │     hit  ─► stream body from CAS            │
   │     miss ─► fetch origin ─► spool ─► CAS    │
   │            └─(if witness_key)─► attest ─────┼─► X-Straylight-Witness*
   └──────┬───────────────────────┬──────────────┘
          │                       │
          ▼                       ▼
     alias_store              cas_store
     url → (digest,           sha256(body) blobs,
     size, content-type,      blake3(dsse) attestations,
     [attestation ref])       verify{} ◄──────┘
```

Two stores, mirroring the substituter's split: `cas_store` holds each body
under `DigestInfo(sha256(body), size)` (share it with the gRPC CAS if you
like), and the string-keyed `alias_store` maps a URL to the `(digest, size,
content-type)` of its body under the slash-free key
`fetch:{nixbase32(sha256(url))}`. When witnessing is enabled the same
`cas_store` also holds each DSSE attestation under its own `blake3(envelope)`
key, and the alias record grows one line pointing at it — so a cache hit can
reproduce the witness headers without re-signing the statement.

## The Request Path

`CasWitness` owns its whole listener because it speaks the proxy protocol, not
the shared gRPC/axum stack. Each connection is served with HTTP/1 (with
upgrades):

- **`CONNECT host:port`** → the CAS witness replies `200`, mints (and caches)
  a `rustls` server config presenting a leaf certificate for `host` signed by
  the CA, accepts TLS over the upgraded stream, and then serves the decrypted
  HTTP requests, reconstructing each absolute URL as `https://host/path`.
- **An absolute-form request** (plain-HTTP proxying) is handled the same way
  without interception.

For a `GET`, the CAS witness consults the `alias_store`; on a hit whose body
blob is still present it streams the body straight from the CAS. On a miss it
fetches the origin, spools any `200` body to a temp file while hashing, and — if
the actual size is within `max_fetch_size_bytes` — uploads it to the CAS under
`sha256(body)`, records the URL alias, and then serves it from the CAS. A body
whose declared `Content-Length` already exceeds the cap is streamed straight
through without spooling; a body that overshoots the cap only after spooling
(a chunked response with no declared length) is served from its spool file and
not cached. Non-`200` responses and every non-`GET` method are proxied straight
through, uncached. The body is fetched eagerly before the client is answered,
which is exactly nix's own behavior (it downloads a fixed-output derivation in
full before hashing it).

Cache entries are **immutable**: a URL maps to the bytes seen on the first
successful fetch, matching nix's contract that a `fetchurl` URL and hash name
immutable content. A different URL (query string included) is a different entry.

### The CA is bounded and validated

The interception CA is not a blank cheque. Before minting a leaf for a
`CONNECT` authority the host is validated — non-empty, at most 253 bytes (the
DNS limit), ASCII-graphic only — so a client opening tunnels to garbage
authorities can neither drive unbounded leaf-key generation (each mint
generates a fresh key pair) nor feed control characters into a certificate's
SAN/CN. Minted configs are cached one per host; the cache is capped at 4096
distinct hosts and cleared wholesale if it is exceeded, because a host
population that large is abuse rather than a real upstream set. A rejected host
surfaces as a `502` on the tunnel, never a crash.

### Redirects are surfaced, never followed

The onward HTTP client is built with `redirect::Policy::none()`, so a `3xx`
from the origin is returned to the client verbatim, `Location` header intact —
the proxy does not chase it. This is deliberate and load-bearing on two
fronts. Following a redirect would let an origin steer the proxy to fetch a URL
the client never named — an SSRF vector into internal hosts — and it would bind
the cached body (and, when witnessing, its signed attestation) to the
*original* URL even though the bytes came from the redirect target. Surfacing
the `3xx` keeps the URL→bytes binding honest and leaves the decision to follow
with the client.

### Size is capped in the spool loop

`max_fetch_size_bytes` is enforced in three places, because the danger is a body
with no declared length. A response whose `Content-Length` is known to exceed
the cap is streamed through untouched, never spooled. A response with no
`Content-Length` (a chunked body) is spooled to a temp file, but the running
byte count is checked *inside* the write loop and the fetch is aborted the
moment it crosses the cap — before another chunk is written — so an unbounded
chunked origin cannot fill the local disk. The whole fetch is also bounded by
`fetch_timeout_s`. A body that spools fully but lands over the cap is served
from the spool file and not cached; a body under the cap is uploaded to the CAS
and the spool file deleted. The spool file is removed on every exit path — a
`SpoolGuard` deletes it on drop, including on an early client disconnect or the
over-cap abort.

## Witnessing

Caching makes the deployment a mirror. *Witnessing* makes it a notary. When the
service is configured with a `witness_key_file`, every cached fetch also
produces a signed **DSSE-wrapped in-toto Statement** that records what the proxy
observed: this body (by `sha256`) was fetched from this URL, at this wall-clock
time, by this host, over a connection to this origin certificate, and is durably
persisted under these CAS keys. The attestation is itself stored in the CAS,
content-addressed under its own `blake3(envelope)` key, and a compact signed
**Receipt** rides back inline in two response headers so a client can verify the
binding without a CAS round-trip.

Witnessing is strictly opt-in: the proxy caches whether or not a key is set, and
the attestation machinery activates only when `witness_key_file` is present.
Omit the field and the service is a plain caching proxy that emits no witness
headers.

### The trust model (v1)

Be precise about what a signature here does and does not prove. The witness is a
**trusted attester**: a verifier who trusts the witness's public key is trusting
the statement *"the proxy observed X."* It is **not** third-party,
non-repudiable proof that the origin actually served those bytes. TLS 1.3
provides no post-hoc non-repudiation — nothing in a completed TLS session lets a
third party later prove which plaintext the server sent — so a witness that
terminates TLS can only assert what it saw, on the strength of its own key. The
`notary` field in the predicate is reserved for a future MPC-TLS notarization
path that *would* yield third-party proof; in v1 it is always `null`.

For the CAS witness's actual job this is enough. The binding that matters to a
Nix deployment is *"URL U was served bytes B, mirrored under CAS key K, at time
T, by host H"* — a provenance and availability record, signed by infrastructure
the operator already runs. Integrity of the bytes never rests on the witness:
nix re-hashes every fixed-output derivation regardless.

### The signing key

`WitnessKey` is a raw 32-byte ed25519 seed, stored **base64 on a single line**
in `witness_key_file` (deliberately *not* the Nix secret-key format — no
`name:` prefix). If the file is absent it is generated from the OS CSPRNG and
persisted `0600` on Unix; if present it is loaded and validated to be exactly 32
bytes. The key's identifier is `keyid = blake3(pubkey)` rendered as hex; it
appears in every DSSE signature entry and every receipt so a verifier knows
which public key to check. `WitnessKey`'s `Debug` prints only the `keyid`, never
the seed.

### The DSSE / in-toto statement

The attestation is an in-toto Statement v1 (`_type:
https://in-toto.dev/Statement/v1`) whose subject is the fetched artifact — its
URL as `name`, and `{ "sha256": "<hex>" }` as its digest — and whose predicate
is a custom **fetch-witness** predicate
(`predicate_type: https://straylight.dev/fetch-witness/v0.1`). The predicate
gathers everything the proxy saw, in six groups:

| Predicate field | Contents |
|---|---|
| `resource` | The fetched `uri` and HTTP `method`. |
| `upstream` | Origin `host`, HTTP `status`, the origin TLS leaf's `server_cert_sha256` fingerprint (`null` for plain HTTP), and a few selected `response_headers`. |
| `authentication` | How the client authenticated to the origin — `"none"` in v1. |
| `content` | The body's `sha256`, `size`, and `media_type`. |
| `persistence` | `body_key` — the body's CAS key `"<sha256-hex>-<size>"`. |
| `witness` | RFC 3339 `timestamp`, `hostname`, and a `monotonic_ns` counter for ordering. |
| `notary` | Reserved for MPC-TLS; `null` in v1. |

The Statement is serialized to JSON, and the JSON is wrapped in a DSSE envelope:
the base64 payload, the payload type `application/vnd.in-toto+json`, and one
ed25519 signature over the DSSE **PAE** (pre-authentication encoding —
`"DSSEV1"` followed by the length-prefixed payload type and payload). Signing
the PAE rather than the raw JSON is what lets verification bind the payload
*type*: `verify_envelope` rejects any envelope whose `payload_type` is not
exactly `application/vnd.in-toto+json`, closing a type-confusion attack where a
signature legitimately computed over some other type would otherwise be accepted
and its payload misread as an in-toto Statement.

One deliberate subtlety lives in `persistence`. The attestation's own CAS key is
`blake3` of the finalized envelope — which cannot appear *inside* the signed
payload without a hash cycle, since the key would depend on the bytes that
contain it. So `attestation_key` is left empty in the signed Statement; the real
key is conveyed out of band, in the `X-Straylight-Witness` header and the
signed receipt. `body_key` — where the bytes actually live — is the binding that
matters and is fully signed.

### The receipt

Fetching and verifying a full DSSE envelope from the CAS is a round-trip a
client often does not want to pay just to check that the response it is holding
was witnessed. The **Receipt** is the compact alternative: a small signed record
carried inline in a response header. It names the attestation
(`attestation = blake3(envelope)` hex), the body (`subject = sha256(body)` hex),
the `url`, an RFC 3339 `ts`, and the `keyid`, signed with the same ed25519 key.
On the wire it is `base64(json).base64(sig)` — payload and detached signature,
dot-separated. `Receipt::parse_and_verify` splits on the dot, checks the
signature against the trusted public key, and returns the parsed record; any
tamper (payload or signature) fails the check.

Two headers accompany every witnessed `200`:

- **`X-Straylight-Witness: blake3:<hex>`** — the CAS key of the DSSE attestation,
  so a client can fetch and independently verify the full Statement.
- **`X-Straylight-Witness-Receipt: <base64(json)>.<base64(sig)>`** — the signed
  receipt, verifiable on its own.

A client that trusts the witness's public key can therefore verify provenance at
two depths: cheaply from the receipt alone, or fully by pulling the attestation
named in `X-Straylight-Witness` out of the CAS and checking its DSSE signature.

### Witnessing on a cache hit

The attestation is signed once, on the miss that first fetches and caches the
body, and stored in the CAS; its `blake3` key and size are appended to the alias
record for that URL. On a later cache **hit** the proxy does not re-fetch or re-sign the
Statement — it reads the attestation reference back out of the alias record and
reproduces the `X-Straylight-Witness` header pointing at the same stored
envelope. The *receipt*, however, is re-signed on each response with a fresh
timestamp, so `X-Straylight-Witness-Receipt` is current while
`X-Straylight-Witness` stays stable across hit and miss (both resolve to the one
signed attestation).

## Configuration

`nativelink-config/examples/cas_witness.json5` is a full runnable example:
a `verify`-wrapped fast/slow CAS for bodies (and, when witnessing, attestations),
a plain memory store for the URL index, and one CAS witness listener.

```json5
services: {
  cas_witness: {
    cas_store: "FETCH_CAS",
    alias_store: "FETCH_ALIAS",
    ca_cert_file: "/var/lib/nativelink/cas-witness/ca.crt",   // trust this on clients
    ca_key_file: "/var/lib/nativelink/cas-witness/ca.key",    // keep private (0600)
    witness_key_file: "/var/lib/nativelink/cas-witness/witness.key",  // omit to disable witnessing
    max_fetch_size_bytes: 2147483648,                         // 2 GiB; larger is proxied uncached
    fetch_timeout_s: 300,
  },
}
```

`cas_store` and `alias_store` are required; the rest have the behavior above.
The CA certificate and key are generated together on first start if either file
is missing — point every build client's `NIX_SSL_CERT_FILE` at the certificate.
`witness_key_file` is optional: when set it is generated on first start if
absent (`0600`) and turns on attestations; when omitted, witnessing is off.
`max_fetch_size_bytes` defaults to 2 GiB and `fetch_timeout_s` to 300 seconds.
Because bodies are content-addressed by `sha256(body)`, wrapping `cas_store` in
`verify{}` rejects a corrupt or truncated body at write time; the `alias_store`
must stay a plain string-keyed store (do **not** wrap it in `verify`,
`size_partitioning`, or `completeness_checking`).

## Current Limitations

Two boundaries are worth stating plainly, both flagged WIP in the code.

**There is no request authentication.** Anyone who can reach the listener can
drive fetches through it and — when witnessing is on — obtain signed
attestations for URLs they chose. The witness's key vouches for *"the proxy
observed this,"* not for *who* asked it to; deploy the listener only where the
ability to reach it is already the trust boundary you want (a private build
network), and treat an attestation as evidence about the fetch, not the requester.

**A cache hit re-attests after only an existence check.** On a hit the proxy
confirms the body blob is still present in the CAS (a `has` probe, not a re-read
or re-hash) and then re-emits the witness headers and a freshly-signed receipt.
The binding it re-asserts is the one captured at first fetch; a hit does not
re-verify the bytes against their digest before signing the receipt. The `verify`
wrapper on `cas_store` is what actually guarantees the stored bytes still match
their key.

Neither weakens the integrity story — nix re-hashes every fixed-output
derivation regardless of what the proxy or its attestations say — but both are
reasons the witness is best understood as a *trusted-infrastructure* attester
today, not a hardened public notary.

## Code Map

| File | Purpose |
|---|---|
| `nativelink-service/src/cas_witness.rs` | The CA (leaf-cert minting, host validation, bounded cache), the proxy connection handling, the URL→CAS caching, and attestation storage |
| `nativelink-service/src/witness.rs` | `WitnessKey`, the in-toto Statement + DSSE envelope, the fetch-witness predicate, and the `Receipt` |
| `nativelink-config/src/cas_server.rs` | `CasWitnessConfig` schema and defaults |
| `nativelink-service/tests/cas_witness_test.rs` | Caching, witness-header, size-cap, and redirect behavior over real loopback sockets |
