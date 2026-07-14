# The Nix Substituter Facade

Attic-style Nix binary caches re-implement chunking, deduplication, tiering, and garbage collection — the exact machinery a serious CAS already has. NativeLink takes the opposite approach: the `nix_cache` service fronts the Nix HTTP binary-cache protocol directly over store composition. Each NAR is a CAS blob, path metadata rides in REv2 `ActionResult` envelopes, and eviction, tiering, and verification come from the same store wrappers every other service uses.

This is the mirror image of the [OCI → CAS Bridge](./oci-cas-bridge.md). The OCI bridge pulls foreign content *into* the CAS; the Nix facade serves CAS content *out* in a foreign protocol. It is also NativeLink's first config-declared plain-HTTP service: every other service on a listener speaks gRPC, while `nix_cache` mounts an HTTP router at a URL prefix (default `/nix/<instance_name>`) so stock `nix` clients can talk to it with nothing but a `substituters` entry.

## Architecture

```
        nix client (nix copy, substitution)
                     │  HTTP
                     ▼
   ┌──────────────────────────────────────────┐
   │             nix_cache service              │
   │  GET  /nix-cache-info                      │
   │  GET|HEAD|PUT /<hash>.narinfo, /<hash>.ls  │
   │  GET|HEAD|PUT /nar/<name>, /log/<drv>      │
   └──────┬───────────────┬───────────────┬────┘
          │               │               │
          ▼               ▼               ▼
   path_info_store     cas_store       alias_store
   string-keyed by     digest-keyed    string-keyed
   store-path hash,    NAR blobs,      URL name →
   completeness_       verify{} ◄──────(digest, size)
   checking{} ─────────┘
```

## The Data Model

**Each NAR is an uncompressed CAS blob.** Every NAR is stored in its canonical uncompressed form under `DigestInfo(sha256(nar), nar_size)` — the same identity the `NarHash`/`NarSize` fields advertise. Uploads arrive compressed (default `nix copy` behavior) or raw; the service decompresses to the canonical form before storing (`nar_url::codec_for_name` maps `.nar`, `.nar.xz`, `.nar.zst`, `.nar.bz2`; bare `.nar` bodies get sniffed for the gzip magic bytes). Because the key is content-derived, integrity is enforced *inside the service*, not delegated to the store: the ingest path hashes every byte and, on the fast direct route, `ingest_nar_direct` withholds EOF from the CAS write the moment `sha256(body)` diverges from the named digest — dropping the writer aborts the write, so wrong bytes never commit under the claimed digest even on a plain store. The spooled route keys the blob on the digest it computed while decompressing, and a bare `{hash}.nar` name is additionally cross-checked so its 52-char stem must equal `sha256(content)`. Wrapping `cas_store` in a `verify{}` store (both `verify_size` and `verify_hash`) is therefore defense-in-depth, not the guarantee — the examples still recommend it, and every example config does so.

**Path-info records are `ActionResult` envelopes.** The metadata for one store path — everything a `.narinfo` needs except the URL and compression — is a `NixPathInfo` message (`nativelink-nix/src/path_info.rs`) carried inside a standard REv2 `ActionResult`:

```
ActionResult {
  output_files: [ OutputFile {
    path: "out.nar",
    digest: Digest { hash: hex(sha256(nar)), size_bytes: nar_size } } ],
  exit_code: 0,
  execution_metadata: ExecutedActionMetadata {
    worker: "nativelink-nix-cache",
    auxiliary_metadata: [ Any {
      type_url: "…/nativelink.nix.NixPathInfo",
      value: NixPathInfo { store_path, nar_sha256, nar_size,
                           references, deriver, system, signatures, ca } } ] } }
```

The record lives in the path-info store under a string key: the 32-character nix32 store-path hash (the `<hash>` in `/nix/store/<hash>-<name>`). The envelope isn't ceremony — it's what makes `completeness_checking{}` work. That wrapper decodes `ActionResult` protos and confirms every referenced digest still exists in the CAS store before admitting the record. So when the NAR store evicts a blob, the corresponding `.narinfo` reads as absent and the client sees a 404 — a clean substitution miss, falling back to another cache or a local build. Garbage collection is not a bespoke sweep; it's eviction plus a completeness check, composed from wrappers that already exist. Phase 2 grows the record with an optional `file_sha256`/`file_size`/`file_compression` trio — all present or all absent, enforced at decode — describing the compressed artifact the facade serves; phase-1 records decode unchanged.

**The alias store maps client-chosen URLs to digests.** A client uploading with `nix copy` picks its own NAR URL — `nar/<filehash>.nar.xz`, or `nar/<nix32(narhash)>.nar` with `?compression=none` — and writes that URL into its `.narinfo`. Neither matches the canonical name the facade serves (`{nix32(nar_sha256)}-{nar_size}.nar`), so every upload records an alias: key `nar-alias:{basename}` (slash-free by construction), value `{lowercase hex}-{size}`. The alias store must **not** sit behind the `completeness_checking` wrapper used for path-info records: alias values are not `ActionResult` protos and would fail its decoding.

One consequence worth noticing: the Nix signing fingerprint covers the store path, `NarHash`, `NarSize`, and references — *not* the URL or compression. The facade can therefore serve a re-generated `.narinfo` pointing at its canonical uncompressed NAR while preserving upstream signatures (say, `cache.nixos.org-1`) verbatim, appending its own `Sig` lines from `signing_key_files`.

## Protocol Discipline

The Nix client is unforgiving in specific, documented ways. Each rule below is load-bearing:

| Rule | Why |
|---|---|
| `/nix-cache-info` always answers 200 | It's the existence probe. If it fails, the client writes off the whole cache, not one path. |
| A miss is 404, never 5xx | On any other error status the client disables the cache for 60 seconds — one flaky lookup turns into a build-wide fallback. Absence must look like absence. |
| `Compression` is always rendered | An absent field means `bzip2` to Nix. Serve a raw NAR without `Compression: none` and the client tries to bzip2-decompress it. |
| `Content-Length` only from size-in-key | Store reads are streams with no trustworthy length. The canonical NAR name and the alias value both embed the size, so the header is exact when present and omitted otherwise — never guessed. |
| `Accept-Ranges: bytes` only where `Range` is honored | Nix resumes interrupted downloads. Advertise ranges, then answer a `Range` request with 200, and the client appends a full body to a partial file — a corrupt NAR and a hard failure. |
| No `Content-Encoding` on NAR responses | NAR compression is application data, described by `Compression`/`FileHash`/`FileSize`. Transport-level recoding changes the bytes those fields describe. |
| `Name: value` with exactly colon-space | Nix reads each value starting at `colon + 2`. Any other separator shifts every value by a byte. |
| One `Sig` line per key, lines accumulate | Multiple signatures are additive; signing with old and new keys at once is how rotation works while clients migrate `trusted-public-keys`. |

## Access Tokens

Private caches gate on static tokens: `read_token_files` and `write_token_files` each list files holding exactly one token apiece — several files, several valid tokens, which is how rotation works. A client presents a token as `Authorization: Bearer <token>` or as HTTP Basic auth with the token as the *password* (the username is ignored) — the Basic form is exactly what stock nix sends from a netrc entry (`machine cache.example.com password <token>`), so private substituters need no client-side plugin. With read tokens configured, every request on the instance — including the `/nix-cache-info` probe — needs a valid read or write token; anything else gets a 401 with a `Basic` challenge, which nix treats as a clean miss, so a private cache is indistinguishable from an empty one. With write tokens configured, every PUT additionally needs a write token — a read token alone won't do — and `read_only` still beats a valid write token with a 405. Tokens are hashed at startup and compared as SHA-256 digests in constant time.

## Listings and Build Logs

The alias store doubles as the home for auxiliary documents, always under slash-free string keys. `nix copy --to '…?write-nar-listing=1'` uploads a JSON file listing to `PUT /<hash>.ls`, stored verbatim under `ls:{hash}` — deliberately with no cross-check against path-info or CAS state, because nix pushes the listing *before* the NAR. `nix store copy-log` uploads build logs to `PUT /log/<drv>`, keyed by the full `.drv` name under `log:{drv}`. Logs are the one place transport encoding matters: with `?log-compression=br` nix uploads pre-compressed bytes under a `Content-Encoding: br` header, so the facade stores the body verbatim, records the encoding under `log-enc:{drv}`, and replays it on GET. Every nix client sends `Accept-Encoding: br, zstd, gzip, …`, so `nix log` decodes transparently and the server never re-encodes a log.

## Compression and Round-Trip Fidelity

By default (`preserve_upload_compression: true`) the facade serves back exactly what a client pushed. A `nix copy --to` upload arrives compressed (`xz` by default) or raw; the facade stores the canonical *uncompressed* NAR — for deduplication, completeness, and sharing with the gRPC CAS — *and* the client's original compressed blob under its own digest. The served narinfo then advertises the original `Compression`, `FileHash`, `FileSize`, and URL, so a client that pushed a path can pull it back from its own cached narinfo. This is the round-trip behavior of `attic`, `harmonia`, and `nix-serve`. Setting `preserve_upload_compression: false` stores only the uncompressed form and serves `Compression: none`, trading that fidelity for the storage of the second blob.

Independently, `serve_compression: "zstd"` re-encodes *uncompressed* uploads on ingest: the NAR streams out of the CAS through a zstd encoder (`compression_level`, default 3), and the compressed blob is stored under its own digest with a real `FileHash`/`FileSize` measured at ingest, never guessed.

The served narinfo chooses its advertised form by precedence — a preserved original, else a zstd re-encoding, else the canonical uncompressed NAR — each guarded by the blob still being present. Stored signatures survive every rendering: the fingerprint covers the uncompressed NAR — its hash, size, and references — never the URL or the compression.

The compressed digest, preserved or re-encoded, stays *out* of the record's `output_files` on purpose. Completeness must key on the uncompressed NAR alone: losing a compressed blob to eviction is recoverable, so the narinfo handler checks for it per request and falls back to the uncompressed rendering — a slower download, not a substitution miss. Listing it in `output_files` would let `completeness_checking` 404 the whole narinfo over derived data the facade can serve around (and a later re-upload of the path regenerates the blob, so it self-heals).

## Configuration

The runnable example is `nativelink-config/examples/nix_cache.json5`: a `verify`-wrapped fast/slow filesystem store for NAR blobs, a `completeness_checking`-wrapped memory store for path-info records, a plain memory store for aliases, and one service instance:

```json5
services: {
  nix_cache: [{
    instance_name: "main",              // cache root: /nix/main on this listener
    cas_store: "NIX_NAR_STORE",
    path_info_store: "NIX_PATH_INFO_STORE",
    alias_store: "NIX_ALIAS_STORE",
    store_dir: "/nix/store",            // must match the clients' store dir
    priority: 40,                       // lower sorts earlier (cache.nixos.org is 40)
    want_mass_query: true,
    signing_key_files: [],              // nix key generate-secret output, one Sig per key
    read_only: false,                   // true on public listeners: PUT returns 405
    read_token_files: [],               // each file holds ONE token; Bearer or netrc password
    write_token_files: [],              // PUTs additionally need one of these
    preserve_upload_compression: true,  // serve pushed compression back verbatim (default)
    serve_compression: "zstd",          // omit (or "none", the default) to serve uncompressed NARs
    compression_level: 3,               // zstd level (1..=22), applied at ingest; default 3
    // Hardening limits, all optional; defaults shown. Generous by design.
    max_nar_size_bytes: 34359738368,    // 32 GiB decompressed-NAR cap (decompression-bomb guard)
    max_concurrent_nar_streams: 256,    // NAR GETs streamed at once; over this → retryable 503
    max_concurrent_transcodes: 8,       // concurrent zstd transcodes (must be >= 1)
    nar_upload_idle_timeout_s: 60,      // abort a NAR upload stalled this long with 408
    // spool_path: "/var/lib/nativelink/nix-spool",  // default: <tmp>/nativelink-nix-spool/<instance>
  }],
}
```

The hardening limits keep an untrusted or misbehaving client from turning an upload into a denial of service. `max_nar_size_bytes` caps the *decompressed* size of a single NAR: a declared size over the cap (a canonical name's embedded size, or the `Content-Length` of a direct upload) is refused with `413` before any body is read, and a compressed stream that expands past it — a decompression bomb — is aborted mid-flight with the partial spool file deleted. `max_concurrent_nar_streams` bounds the NAR GET response bodies streamed at once, each holding a producer task and a permit released only when its body is fully drained or the client disconnects; a request over the ceiling gets a retryable `503` rather than a committed `200` and unbounded buffering. `max_concurrent_transcodes` bounds how many concurrent runs of the zstd transcoder re-encode a NAR at once (both ceilings are coerced up to 1 at startup, and `--check` rejects a configured 0). `nar_upload_idle_timeout_s` aborts an upload with `408` once its body stalls past the window — the timer resets on every received chunk, so a slow-but-steady push survives — releasing the spool file and descriptor. `spool_path` is the staging directory where compressed uploads are stream-decompressed before landing in `cas_store`; it is created if missing and, at startup, pruned of *only* the files this instance wrote (a fixed `nativelink-nix-spool-` prefix with a `.nar` extension), so pointing it at a shared directory can never delete unrelated data.

Store composition has sharp edges here because two of the three stores are string-keyed. [Store Composition](../part3/store-composition.md) covers the wrapper stack in general; these are the edges specific to `nix_cache`:

- **Never wrap the path-info store in `existence_cache`.** It drops overwrites — a re-upload of the same store path with new signatures or references is silently ignored — and it rewrites string keys.
- **Never wrap string-keyed stores in `verify` or `size_partitioning`.** Both parse keys as digests and reject string keys outright.
- **Keep string keys slash-free.** Filesystem stores turn keys into file names; a `/` in a key is a path traversal. This is why alias keys are `nar-alias:{basename}` rather than the URL itself.
- **Mind deduplication in the existence path.** `completeness_checking` turns every `.narinfo` GET into existence checks against the NAR store. If a `dedup` store sits in that path, each check fans out into an index read plus per-chunk probes — one metadata request amplified into dozens of store operations. Keep `dedup` out of the completeness-checked path, or accept the amplification knowingly.

## Client Usage

```bash
# Upload a closure. Default compression is xz; the facade serves the
# compressed blob back verbatim and also stores the uncompressed NAR.
nix copy --to 'http://cache.example.com:50071/nix/main' ./result

# Or push uncompressed, which is fastest to ingest.
nix copy --to 'http://cache.example.com:50071/nix/main?compression=none' ./result

# Fetch explicitly, or let substitution find it during builds.
nix copy --from 'http://cache.example.com:50071/nix/main' /nix/store/<hash>-<name>
```

Stock `nix` is not the only client. The fork ships a dedicated one — `nl-nix` (push/pull/info) and the `nl-watch-store` auto-push daemon — that speaks these exact routes while streaming zstd on the wire and emitting OTLP metrics. See [The Nix Cache Client](./nix-cache-client.md).

In `nix.conf`, the cache is an ordinary entry:

```
substituters = http://cache.example.com:50071/nix/main https://cache.nixos.org
trusted-public-keys = nix-cache.example.org-1:<base64> cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
netrc-file = /etc/nix/netrc   # on token-gated caches the token is the password
```

## Upstream Read-Through

A local miss is a `404` by default. Configure `upstream_caches` and it
becomes a *read-through*: the first request for a path the cache doesn't
have fetches it from an upstream substituter, verifies it, ingests it into
the same stores every other path lives in, and serves it — so one fetch
makes the deployment a durable mirror of exactly the closure your builds
pull. This is the substituter's answer to "front `cache.nixos.org` and
`nix-community` but own the bytes."

```json5
nix_cache: [{
  instance_name: "main",
  cas_store: "NIX_NAR_STORE",
  path_info_store: "NIX_PATH_INFO_STORE",
  alias_store: "NIX_ALIAS_STORE",
  signing_key_files: ["/etc/nativelink/my-cache.key"],
  upstream_caches: [
    { url: "https://cache.nixos.org",
      trusted_public_keys: ["cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="] },
    { url: "https://nix-community.cachix.org",
      trusted_public_keys: ["nix-community.cachix.org-1:mB9FSh9qf2dCimDSUo8Zy7bkq5CX+/rkCWyvRCYg3Fs="] },
  ],
  upstream_negative_ttl_s: 60,   // remember misses so closure probes don't re-hammer upstream
  upstream_timeout_s: 30,        // narinfo probe timeout; the NAR download is bounded by max_nar_size_bytes
}]
```

The full example is `nativelink-config/examples/nix_cache_readthrough.json5`.

The miss path in `get_narinfo_inner` runs this sequence per upstream, in
order, taking the first hit:

1. **Probe** `GET <url>/<hash>.narinfo`. A `404`/`401`/`403` is a clean
   miss — try the next upstream. Any other non-success is a transport
   error: logged and treated as a miss so a flaky upstream never becomes a
   client-facing `5xx`.
2. **Bind the metadata to what was asked.** The returned `narinfo`'s
   `StorePath` hash must equal the requested hash, and its store dir must
   equal this instance's `store_dir`; otherwise it isn't ours to cache.
3. **Verify the signature — before storing anything.** The `narinfo`
   fingerprint (which covers `StorePath`, `NarHash`, `NarSize`, and
   `References`) must verify against one of the `trusted_public_keys` set
   for that upstream. An unsigned or unverifiable `narinfo` is refused
   and the path is treated as a miss, so **read-through can never be a
   cache-poisoning vector**. This is why every upstream must list at least
   one key — an upstream with none could never contribute and is a config
   error caught at startup.
4. **Fetch and verify the NAR.** The NAR is streamed from the upstream,
   decompressed through the codec its `Compression` names (an unsupported
   codec like `br` is a clean miss), hashed, and written to the CAS via the
   same `ingest_nar_spooled` path uploads use — including the
   `max_nar_size_bytes` decompression-bomb cap. The ingested NAR must hash
   to the signed `NarHash`/`NarSize`; a mismatch
   means the upstream served bytes its own signature doesn't cover, so the
   path is refused (the ingested blob is unreferenced and evicts on its
   own) and no record is written.
5. **Store the record.** The upstream signatures are preserved verbatim and
   this instance's own `Sig` is added for every key that hasn't signed the
   fingerprint yet, so downstream clients can trust the mirror with its own
   public key alone. The served NAR is this cache's canonical uncompressed
   blob, so the re-rendered `narinfo` advertises `Compression: none`.

The NAR is fetched **eagerly** during the `narinfo` GET so the freshly
written record satisfies a `completeness_checking` `path_info_store`
immediately — the recommended composition (a lazy fetch would 404 its own
record until the NAR arrived). Read-through populates the stores regardless
of `read_only`, which only gates client PUTs, so a public read-only front
still fills itself. It fires on `HEAD` as well as `GET`: `nix copy --from`
(and `isValidPath`) probes a path's availability with a `HEAD` on the
`narinfo` before ever issuing the `GET`, so a `HEAD`-only read-through would
otherwise report the path missing and the client would never fetch it.

Two guards keep upstream traffic sane. Concurrent identical misses
**coalesce**: the first request for a hash becomes the leader and fetches
while the rest await its result on the same slot (the leader/`notify`
pattern the zstd transcoder uses), so a thundering herd is one fetch.
Misses are **negatively cached** for `upstream_negative_ttl_s`, because Nix
issues a `narinfo` probe for every path in a closure while planning a
build and most of those aren't on any upstream — without it, each plan
would re-query every upstream for every absent path. Only misses are
remembered; a hit is durable in the stores.

The `upstream_hits`, `upstream_misses`, `upstream_errors`,
`upstream_rejected_unsigned`, and `upstream_nar_bytes` metrics on the
instance make the read-through behavior observable.

## Deploying Alongside Remote Execution

`nix_cache` is an ordinary service entry: it mounts an HTTP router at `/nix/<instance_name>` on the same listener as the gRPC CAS, AC, and execution services, so one `NativeLink` process can be both a remote-execution endpoint and a Nix cache on one port. Its `cas_store` may reuse the same content-addressed store the gRPC CAS uses — Nix `sha256` NAR blobs and Bazel `blake3` blobs coexist in it, because a digest is an algorithm-blind 32 bytes keyed by `(hash, size)`. The `path_info_store` and `alias_store` are string-keyed and must be separate stores.

On a single machine this collapses to one process on one port — the [Single Node](../part7/single-node.md) deployment adds exactly this service to its public listener. And the closure most worth serving is often the toolchain itself: the Nix store paths [Local Remote Execution with Nix](../part8/lre-nix.md) pins — the `PATH`/`CC`/`RUST` lines in `lre.bazelrc`, generated as [Nix and LRE](../part5/nix-lre.md) describes — are exactly what this facade serves, so `nix develop` on a laptop or CI runner realizes the identical toolchain straight out of the cluster instead of rebuilding it.

`nativelink-config/examples/basic_cas_with_nix.json5` is a full remote-execution stack — CAS, AC, execution, capabilities, bytestream, a scheduler, and a worker — with a `nix_cache` service added to the same public listener, its NAR store a `verify`-wrapped reference to the shared CAS. The worker still references the raw fast/slow store rather than the `verify` wrapper, because a worker's `cas_fast_slow_store` must be a `FastSlowStore`.

## Validating Configuration

`nativelink <config> --check` parses a configuration and resolves every store and scheduler reference — catching a mistyped `cas_store` or `scheduler` name that would otherwise only fail at boot — then exits without binding a socket, connecting to a backend, or creating any store directory. Beyond reference resolution it also validates the `nix_cache` field invariants that would otherwise surface only at service boot: the `trusted_public_keys` of every upstream must be non-empty (read-through refuses to cache an unverifiable narinfo, so a keyless upstream could never contribute), a `compression_level` set alongside `serve_compression: "zstd"` must be in zstd's `1..=22` range, and `max_concurrent_nar_streams`/`max_concurrent_transcodes` must each be at least 1. On success it prints a one-line summary to standard output — `OK: <config> — N stores, N schedulers, N servers, all references resolve` — and on failure prints `FAIL: <config> — configuration is invalid` to `stderr` followed by one line per problem, exiting non-zero so a continuous-integration gate can read the result.

## Code Map

| File | Purpose |
|---|---|
| `nativelink-nix/src/nixbase32.rs` | Nix's base32 alphabet: encode, decode, character validation |
| `nativelink-nix/src/narinfo.rs` | `.narinfo` parse/render, signing fingerprint, store-path hash validation |
| `nativelink-nix/src/signing.rs` | Nix-compatible ed25519 keys (`nix key generate-secret` format) |
| `nativelink-nix/src/path_info.rs` | `NixPathInfo` ↔ `ActionResult` envelope, fingerprint, record encoding and decoding |
| `nativelink-nix/src/nar_url.rs` | Canonical NAR names, alias keys, compression detection |
| `nativelink-service/src/nix_cache_server.rs` | The HTTP service: routing, streaming, upload ingestion, signing |
| `nativelink-service/tests/nix_cache_server_test.rs` | Integration suite: the protocol behavior spec as tests |
| `nativelink-config/src/cas_server.rs` | `NixCacheConfig` schema and defaults |

## What's Next

One phase remains: builds-as-actions. Realisation becomes an ordinary REv2 action using the SHA-256 digest function, so a worker's outputs land under exactly the CAS keys this facade serves — a `nix build` on the cluster populates the substituter, listings and logs included, with no copy step at all.
