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

**Each NAR is an uncompressed CAS blob.** Every NAR is stored in its canonical uncompressed form under `DigestInfo(sha256(nar), nar_size)` — the same identity the `NarHash`/`NarSize` fields advertise. Uploads arrive compressed (default `nix copy` behavior) or raw; the service decompresses to the canonical form before storing (`nar_url::codec_for_name` maps `.nar`, `.nar.xz`, `.nar.zst`, `.nar.bz2`; bare `.nar` bodies get sniffed for the gzip magic bytes). Because the key is content-derived, the NAR store sits behind `verify{}` with both size and hash checks: a corrupt or truncated upload is rejected at write time instead of served to clients.

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
    serve_compression: "zstd",          // omit (or "none") to serve uncompressed NARs
    compression_level: 3,               // zstd level used at ingest
  }],
}
```

Store composition has sharp edges here because two of the three stores are string-keyed:

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

In `nix.conf`, the cache is an ordinary entry:

```
substituters = http://cache.example.com:50071/nix/main https://cache.nixos.org
trusted-public-keys = nix-cache.example.org-1:<base64> cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
netrc-file = /etc/nix/netrc   # on token-gated caches the token is the password
```

## Deploying Alongside Remote Execution

`nix_cache` is an ordinary service entry: it mounts an HTTP router at `/nix/<instance_name>` on the same listener as the gRPC CAS, AC, and execution services, so one `NativeLink` process can be both a remote-execution endpoint and a Nix cache on one port. Its `cas_store` may reuse the same content-addressed store the gRPC CAS uses — Nix `sha256` NAR blobs and Bazel `blake3` blobs coexist in it, because a digest is an algorithm-blind 32 bytes keyed by `(hash, size)`. The `path_info_store` and `alias_store` are string-keyed and must be separate stores.

`nativelink-config/examples/basic_cas_with_nix.json5` is a full remote-execution stack — CAS, AC, execution, capabilities, bytestream, a scheduler, and a worker — with a `nix_cache` service added to the same public listener, its NAR store a `verify`-wrapped reference to the shared CAS. The worker still references the raw fast/slow store rather than the `verify` wrapper, because a worker's `cas_fast_slow_store` must be a `FastSlowStore`.

## Validating Configuration

`nativelink --check <config>` parses a configuration and resolves every store and scheduler reference — catching a mistyped `cas_store` or `scheduler` name that would otherwise only fail at boot — then exits without binding a socket, connecting to a backend, or creating any store directory. It prints a one-line summary on success and names each unresolved reference on failure, with an exit code a continuous-integration gate can read.

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
