# OCI Registry over CAS — the Distribution API as a lens

- **Status:** Draft / Proposed (PROD-3)
- **Date:** 2026-08-04
- **Owner:** b7r6
- **Scope:** a new fork service (`oci_registry`), peer of `nix_cache`; no
  upstream offer until it lands (see `UPSTREAM-LEDGER.md`, the OCI→REAPI
  bridge line)

## Summary

Serve the **OCI Distribution Specification (pull *and* push)** directly from
NativeLink stores, as an HTTP facade over the CAS — the same move the Nix
substituter facade already made for the Nix binary-cache protocol. `skopeo`
(and crane, docker, podman, oras) push to and pull from the CAS itself; the
existing `FetchDirectory` OCI import stops crossing the network and becomes an
internal projection over blobs already resident; **zot retires**, along with
its second storage engine, its second GC, its invisible-default timeouts, and
the SSH tunnel we keep open to reach it.

Per the spine thesis ([The CAS Is the Spine](../book/src/part1/the-cas-is-the-spine.md)):
"The OCI registry stops being a system and becomes a naming convention — a few
hundred lines on how to lay out a manifest and address a layer." This document
is those few hundred lines, plus the three problems that are genuinely load-
bearing:

1. **the sha256↔BLAKE3 dual-digest index** — the graded CAS made concrete:
   one blob, one storage identity, per-lens names;
2. **the mutable tag ref-store** — the small, honest, separable concern of
   name→content bindings, reusing the `nix_cache` string-keyed record pattern;
3. **the conformance suite as a flake check** — never self-validate a wire
   format again (the §7 field-4 lesson).

Doc only; no implementation ships with this commit.

## Motivation

### What zot costs today

The current toolchain-publishing pipeline
(`cxx-rbe-oci-recipe`, and the trio/CUDA cells before it) is:

```
nix (floor cell) → skopeo push → [SSH tunnel] → zot (localhost-only)
                                                  │ (R2 storage backend)
                                                  ▼
                     FetchDirectory("oci://zot/…") → OCI pull over HTTP
                                                  → project → upload to CAS
```

Every arrow has drawn blood:

- **zot's 60s `readTimeout`/`writeTimeout` are injected at config load and
  invisible in the rendered config** — any layer that streams to R2 for more
  than 60s (≈440 MB) 502s until the values are set explicitly. Diagnosed only
  by bypassing nginx and reading zot's own journal.
- **zot is a second storage engine** with its own dedup, GC, and replication
  — exactly the column the spine thesis crosses out. Its R2 bucket duplicates
  bytes the CAS already holds (every pushed layer is re-uploaded to CAS by the
  projection minutes later).
- **The push path is a tunnel.** zot is localhost-only on watchtower; every
  toolchain publish opens an SSH tunnel first.
- **The pull path re-crosses the network.** `FetchDirectory` pulls layers over
  HTTP from a service colocated with the CAS it is about to upload into.

### What the facade buys

- **One store.** Layers, manifests, and the projected REAPI trees live in the
  same CAS, under the same GC, the same tiering (`fast_slow` write-back — the
  #35 payoff), the same replication.
- **Dedup across lenses.** A file that exists as a REAPI blob and arrives
  again inside a pushed layer is one object (post-projection; the layer tar
  itself is its own blob).
- **`FetchDirectory` becomes a graph walk**, not a network client: manifest
  from the ref-store, layers from the local store, projection as today,
  nothing fetched twice.
- **The Standard OCI Toolchain loop closes**: nix builds the cell, skopeo
  pushes it *into the CAS*, workers floor-load it *from the CAS* — one
  substrate end to end (the Gate E result, minus zot).
- **A public artifact surface for free.** Anything in CAS that has an OCI
  shape is servable to any OCI client; the registry is a read lens as much as
  a write path.

### Goals

- Full OCI Distribution Spec v1.1 **pull** and **push** (end-1..end-7 of the
  conformance suite's workflow categories, with content management config-
  gated), served from NativeLink stores by a fork service configured exactly
  like `nix_cache`: an entry in `services`, store references by name, an
  axum router mounted on an existing listener.
- `skopeo copy` to and from the service with no zot, no tunnel, no special
  flags beyond credentials.
- Blobs stored **once**, under the deployment's canonical digest function
  (BLAKE3 on the fleet), reachable by their OCI wire name (`sha256:…`).
- The official `opencontainers/distribution-spec` conformance suite running
  against the real binary as a `nix flake check`.

### Non-goals

- **Not a multi-tenant public registry.** Repo-level ACLs, quota, per-user
  namespaces: out of scope. Auth is the `nix_cache` token model (instance-
  level read/write tokens), sufficient for the fleet. Revisit if the surface
  ever goes public.
- **Not a Docker Hub proxy / pull-through cache** in v1. The upstream-read-
  through pattern from `nix_cache` maps cleanly (and `nativelink-oci`'s
  `RegistryClient` is the client half already), but it is additive and comes
  later.
- **No changes to the Distribution wire protocol.** Conformance is the point;
  extensions (if any) ride annotations and the existing REAPI surface.

## Background — what exists in the tree

| Piece | Where | What it gives this design |
|---|---|---|
| OCI Distribution **client** (auth challenge, manifest, blob fetch) | `nativelink-oci/src/registry.rs` | the wire vocabulary (`ImageReference`, `Descriptor`, `OciManifest`); later, the read-through client |
| OCI→REAPI **projection** (tar → FsNode → Directory tree, BLAKE3/SHA256) | `nativelink-oci/src/projection.rs` | unchanged; becomes the internal consumer of the registry's blobs |
| `FetchDirectory` OCI routing | `nativelink-service/src/fetch_server.rs` | the integration point that flips from network pull to local read |
| Nix substituter facade | `nativelink-service/src/nix_cache_server.rs` | **the pattern**: axum `Router` per instance mounted by prefix; digest-keyed blob store + string-keyed record stores; spool-decompress-verify ingest; token auth; per-instance concurrency caps; `--check` field validation |
| `NixPathInfo` record envelope | `nativelink-nix/src/path_info.rs` | mutable string-keyed records as prost messages in an `ActionResult` envelope, so `completeness_checking` can 404 a record whose blob was evicted |
| Config schema + validation | `nativelink-config/src/cas_server.rs` (`NixCacheConfig`, `validate_nix_cache_fields`) | the schema shape and the offline `--check` gate |
| Digest-function pinning | `nix_cache_server.rs` (`make_ctx_for_hash_func`, the "single choke point" SHA256 context) | proof that a facade can hash-key stores independently of the deployment default |
| Flake checks | `flake.nix` (`nix-crate-tests`, `check-example-configs`, `nix-substituter-e2e`) | the harness the conformance suite plugs into |

## Decision

Build **`oci_registry`**: a new fork service in `nativelink-service`, backed
by a new protocol crate **`nativelink-oci-registry`** (wire types, digest
index records, tag records — peer of `nativelink-nix`), configured by
`OciRegistryConfig` in `nativelink-config`, mounted like `nix_cache` in
`src/bin/nativelink.rs`. Blobs are stored once under the canonical digest
function with a sha256 alias index (Design 1); tags are string-keyed records
in the `nix_cache` envelope pattern (Design 2); correctness is gated by the
official conformance suite plus independent-client round-trips in
`flake check` (Design 3).

## API surface — Distribution endpoints mapped to CAS operations

Store roles (all resolved by name from `stores`, `nix_cache`-style):

- **`cas_store`** — digest-keyed. Layer blobs, config blobs, and manifest
  bodies, each under `DigestInfo(canonical_hash(bytes), size)`. Recommended
  wrapping: `verify` with size+hash.
- **`index_store`** — string-keyed. Digest-alias records:
  `oci-digest:sha256:<hex>` → (canonical hex, size). Immutable content
  (Design 1). Never wrap in `verify`/`size_partitioning` (string keys).
- **`ref_store`** — string-keyed. Mutable tag records and per-repo tag
  indexes (Design 2). Never wrap in `existence_cache` (drops overwrites —
  fatal for tags), `verify`, or `size_partitioning` — the exact constraints
  documented on `path_info_store`/`alias_store`.

| Endpoint | Method | CAS operation |
|---|---|---|
| `/v2/` | GET | static 200 (+auth challenge when tokens configured) |
| `/v2/<name>/blobs/<digest>` | HEAD/GET | `index_store` lookup `sha256:… → (canonical, size)`; stream from `cas_store`; `Docker-Content-Digest: sha256:…` (the wire name, always) |
| `/v2/<name>/blobs/uploads/` | POST | open upload session (uuid, spool file); `?digest=` → monolithic one-shot; `?mount=…&from=…` → index existence check (blobs are global — see below), 201 on hit, else fall through to a normal session |
| `/v2/<name>/blobs/uploads/<uuid>` | PATCH | append chunk to spool, feed **both** running hashers (sha256 + canonical); `Range` bookkeeping per spec |
| `/v2/<name>/blobs/uploads/<uuid>` | PUT | final chunk; verify computed sha256 == `?digest=` (else `BLOB_UPLOAD_INVALID`); `slow_update_store_with_file` into `cas_store` under the canonical digest; write the alias record; delete spool |
| `/v2/<name>/blobs/uploads/<uuid>` | GET/DELETE | session status / abort (drop spool) |
| `/v2/<name>/manifests/<ref>` | HEAD/GET | tag → `ref_store` record → descriptor; digest → `index_store`; body from `cas_store`; `Content-Type` from the stored media type |
| `/v2/<name>/manifests/<ref>` | PUT | body (capped, ~4 MiB per spec ecosystem norms) → hash both ways → `cas_store` + alias record; parse just enough to reject a manifest referencing unindexed blobs (`MANIFEST_BLOB_UNKNOWN`) — subject-of-truth for `completeness_checking`; if `<ref>` is a tag, write/overwrite the tag record and merge the repo tag index |
| `/v2/<name>/tags/list` | GET | per-repo tag index record (Design 2), `n`/`last` pagination in-handler |
| `/v2/<name>/manifests/<ref>` / `blobs/<digest>` | DELETE | config-gated (`enable_delete`); tag delete = ref record removal; digest delete = alias-record removal only (content GC belongs to the CAS, not the lens) |
| `/v2/<name>/referrers/<digest>` (OCI 1.1) | GET | **phase 4 / open question** — needs a reverse `subject` index maintained on manifest PUT |

Notes on the mapping:

- **Blob storage is global; repositories scope names, not bytes.** A blob
  reachable in one repo is the same CAS object everywhere — cross-repo mount
  is an index lookup, and "does repo X contain blob Y" is not tracked in v1
  (single-tenant fleet; the conformance suite does not require cross-repo
  404 isolation for blobs it didn't upload). If tenant isolation is ever
  needed, it is a per-repo membership record, not a storage change. Flagged
  in open questions.
- **Uploads spool to disk** (`spool_path`, `nix_cache` semantics: prefix-
  scoped startup pruning, idle timeout aborts, `max_blob_size_bytes` cap
  enforced on declared and actual size — the decompression-bomb discipline,
  minus decompression: layers are stored as-pushed, compressed).
- **Concurrency caps** mirror `max_concurrent_nar_streams`: a ceiling on
  in-flight blob GET streams and on open upload sessions, `503` beyond.
- **Auth** is the `nix_cache` token model verbatim: `read_token_files` /
  `write_token_files` / `read_only`, Bearer or Basic (skopeo/docker send
  Basic from credentials natively; a token realm is not required for
  conformance — `Basic` challenges are spec-legal). Same cleartext-listener
  warning, same plaintext-listener startup refusal.

## Design 1 — the sha256↔BLAKE3 dual-digest index

### The problem

OCI's wire identity is sha256: skopeo computes it, verifies it on pull, and
bakes it into manifests — it cannot be negotiated away. The fleet's CAS is
BLAKE3, and *must* be uniformly so: buck2 detects the digest algorithm by hex
length, and sha256 and BLAKE3 are both 32 bytes, so a deployment cannot mix
them per-blob (the Gate D lesson). A registry over this CAS must therefore
answer to sha256 names for content whose storage identity is BLAKE3.

### Chosen design: canonical storage + alias index

Every blob is stored **once**, keyed by the instance's canonical digest
function (configurable, `BLAKE3` on the fleet, `SHA256` legal for a
sha256-native deployment — where the index degenerates to identity and is
elided). At ingest, the spool stream feeds two hashers; on completion:

1. computed sha256 must equal the client's declared digest — hard reject
   otherwise (spec: `BLOB_UPLOAD_INVALID`); **the registry never trusts a
   declared digest it did not recompute**;
2. the bytes land in `cas_store` under `DigestInfo(blake3, size)`;
3. an alias record `oci-digest:sha256:<hex>` → `{canonical_hex, size}` lands
   in `index_store`.

Reads resolve the alias, then stream from `cas_store`. `HEAD` answers
`Content-Length` from the record without touching the blob. The wire never
sees a BLAKE3 name; the store never sees a sha256 key.

The record is a prost message in the crate (`nativelink-oci-registry`), not
ad-hoc strings — same discipline as `NixPathInfo`, and for the same reason:
the field tags are a wire contract.

**This is the graded CAS made concrete**: content has one identity (the
canonical digest); *names* are per-lens bindings. The alias record is not a
mutable ref — sha256(bytes)→blake3(bytes) is a content-derived, immutable
fact. It can be cached, replicated, and rebuilt without coordination; a lost
index entry is repaired by re-hashing the blob (self-healing = recompute), and
a *wrong* one is detectable the same way. Recommended wrapping is therefore a
plain durable store, optionally behind `completeness_checking` against
`cas_store` so an alias whose blob was evicted 404s instead of advertising a
blob that cannot be served — the `path_info_store` lesson applied unchanged.

A reverse index (canonical→sha256) is **not** written in v1; nothing on the
serving path needs it. The projection cache (manifest digest → REAPI root
digest, the bridge roadmap's "caching the mapping") will want the forward
direction only.

### Alternatives considered

- **(a) Store blobs under their sha256 wire digest, pinned-context style.**
  The `nix_cache` precedent: pin `make_ctx_for_hash_func(SHA256)` at the
  facade choke point and key NARs by sha256 even in a BLAKE3 deployment.
  Simplest possible design, zero index. Rejected: it creates a second blob
  population invisible to every BLAKE3-keyed consumer — `cas_artifact`
  can't name it, the projection can't dedup against it, and "one store, one
  identity" quietly becomes "one store, two disjoint keyspaces." The nix
  facade gets away with it because NARs are terminal artifacts no other lens
  consumes; OCI layers are explicitly *inputs* to the projection and the
  toolchain pipeline. (Worth noting: if the alias-index cost ever proves
  real, this alternative is a clean fallback — the wire surface is
  identical.)
- **(b) Re-hash on read.** No index; resolve sha256→blake3 by scanning or
  recomputing. Rejected: O(bytes) per request on the hot path; the index is
  O(1) and self-healing anyway.
- **(c) Store both copies.** Rejected without ceremony: "dedup cancels" is
  the thesis; storing bytes twice to avoid a 100-byte record is the
  confession.
- **(d) Teach the fleet sha256.** Rejected: Gate D showed digest functions
  are deployment-global for buck2; moving the fleet off BLAKE3 to appease a
  wire format inverts the entire priority order.

## Design 2 — the mutable tag ref-store

### The problem

Tags are the one genuinely mutable thing in the registry: `push :latest`
twice and the second write must win, immediately and durably. The spine
thesis names this exactly: mutable **name → content** bindings are the small
separable residue you invalidate explicitly — they must not contaminate the
immutable store, and they must not be silently dropped by store wrappers
built for immutable content.

### Chosen design: the `nix_cache` record pattern, applied

A tag is a string-keyed record `oci-tag:<name>:<tag>` in `ref_store`, valued
as a prost message: the manifest **descriptor** (media type, sha256 digest,
size) plus a write timestamp. Manifest-by-tag GET is: record → alias →
blob. The hard-won constraints from `path_info_store`/`alias_store` transfer
verbatim and become `--check`-time validation notes on the config fields:

- **never `existence_cache`** — it drops overwrites; a tag that cannot be
  overwritten is not a tag;
- **never `verify` / `size_partitioning`** — string keys;
- **recommended `completeness_checking`** against `cas_store`, so a tag
  whose manifest blob was evicted returns 404 (`MANIFEST_UNKNOWN`) instead
  of advertising a manifest that can no longer be served. This is why the
  record's envelope is the `NixPathInfo` `ActionResult` shape — one output
  file whose digest is the manifest's *canonical* digest — rather than raw
  bytes: completeness keys on the blob, eviction degrades to a clean miss.

`/v2/<name>/tags/list` needs enumeration, which string-keyed stores do not
offer. v1 maintains a **per-repo tag index record** `oci-tags:<name>`: a
sorted list of tag names, read-modify-merged on every tag PUT/DELETE.
Concurrent pushes of *different* tags to the same repo can race the index
write; the merge is idempotent (set-union on PUT, and a subsequent push or
delete of the lost tag repairs it), the authoritative per-tag records are
untouched by the race, and `tags/list` is explicitly a discovery endpoint,
not an integrity one. The race window and repair story are documented on the
config field. Pagination (`n`, `last`) is served from the record in-handler.

### Alternatives considered

- **(a) Require a listing-capable store** (filesystem/Redis scan) for
  `ref_store`. Rejected for v1: it forks the store contract for one
  endpoint and breaks the "any string-keyed store works" property that
  makes the facades composable. Reconsider if per-repo index contention
  ever shows up in practice (it requires sustained concurrent multi-tag
  pushes to one repo — not the fleet's shape).
- **(b) Tags in an external database.** Rejected: a second stateful system
  is the exact thing this program retires; the whole design fits in stores
  we already operate, snapshot, and replicate.
- **(c) Content-addressed tag history (tag → chain of descriptors).**
  Attractive for audit ("what did :latest mean last Tuesday") and cheap —
  each record can carry the digest it replaced — but it is additive. The
  record schema reserves a `previous` field; the chain is not walked by any
  v1 endpoint.

## Design 3 — the conformance suite as a flake check

### The problem, stated as the lesson

The §7 bug: `reapi_dir.py` encoded `FileNode.is_executable` at field 3 where
REAPI puts it at field 4 — and the validator written beside it *validated
against a reference generated with the same error*. Self-validation of a wire
format is structurally unable to catch exactly the class of bug it exists to
catch. The fix was found only when an independent implementation (the OCI
bridge test) read the bytes.

The rule this design adopts as a hard gate: **every wire surface is tested
against an implementation we did not write.**

### Chosen design

Three independent oracles, all in `nix flake check` beside `nix-crate-tests`
/ `nix-substituter-e2e`:

1. **`oci-conformance`** — the official
   `opencontainers/distribution-spec` conformance suite (a Go test binary;
   packaged from the spec repo, version-pinned in the flake) run against a
   real `nativelink` process serving an `oci_registry` instance over
   filesystem stores. All four workflow categories: **Pull**, **Push**,
   **Content Discovery**, **Content Management** (the last gated on
   `enable_delete: true` in the check's config). Sandboxed: both processes
   in one derivation on loopback, no KVM needed.
2. **`oci-client-roundtrip`** — `skopeo copy dir:… docker://localhost/…`
   then `docker://… dir:…`, byte-identical round trip diffed; repeated with
   `crane` as a second client lineage (different auth flow, different chunking
   behavior — skopeo does monolithic PUTs, crane exercises the session path).
3. **`oci-projection-differential`** — push a known toolchain-shaped image,
   run the internal projection (phase 3) against it, and assert the REAPI
   root Directory digest equals the digest produced by the *existing*
   network-path `FetchDirectory` against the same image served by the same
   instance. The two paths share the projection but not the acquisition
   code; divergence means the registry served different bytes than it
   accepted. (The §12 harness already gates toolchain images the same way
   before push; this brings that discipline into CI.)

The Rust unit layer (in-crate) still exists — parse/serialize property tests,
the digest-index records, upload session state — but **no Rust test is the
oracle for the wire**. Reference vectors (the `nix_cache` pattern of pinning
external-tool-generated fixtures in config tests) come from real registries:
manifests as pushed by skopeo/buildah, not manifests we synthesized.

### Alternatives considered

- **Hand-rolled HTTP tests asserting spec-quoted behavior.** Rejected as the
  oracle — this is precisely the self-validation shape (§7): our reading of
  the spec checking our reading of the spec. Kept only as fast unit-level
  regression pins *after* the conformance suite has established a behavior.
- **Testing against zot as the reference implementation** (differential:
  same requests to both, compare). Considered, and it is a genuinely
  independent oracle — but it tests zot-compatibility, not spec
  conformance, inherits zot's own quirks, and keeps zot alive in CI after
  we retired it in production. The official suite + two real clients
  dominates it.

## Config schema sketch

```json5
// nativelink-config/src/cas_server.rs — peer of NixCacheConfig
services: {
  oci_registry: [{
    instance_name: "main",

    // Store references (resolved by name; --check validates existence and
    // the wrapper constraints documented per field, nix_cache-style).
    cas_store: "OCI_BLOBS",        // digest-keyed; recommend verify{size,hash}
    index_store: "OCI_INDEX",      // string-keyed digest aliases;
                                   // recommend completeness_checking→cas_store
    ref_store: "OCI_REFS",         // string-keyed tags; NEVER existence_cache

    // Mount + identity
    path: "/v2",                   // default "/v2" — the spec's fixed root;
                                   // one instance per listener (open question
                                   // for multi-instance: host-based routing)
    digest_function: "BLAKE3",     // canonical storage identity; "SHA256"
                                   // elides the alias index (identity map)

    // Ingest (nix_cache spool semantics)
    spool_path: "/tmp/nativelink-oci-spool/main",
    max_blob_size_bytes: "32gb",
    upload_idle_timeout_s: 60,
    max_concurrent_blob_streams: 256,
    max_open_upload_sessions: 64,
    max_manifest_size_bytes: "4mb",

    // Mutation policy
    read_only: false,              // 405 on all writes (public listeners)
    enable_delete: false,          // Content Management endpoints

    // Auth (nix_cache token model; same TLS-listener startup refusal)
    read_token_files: [],
    write_token_files: ["/run/secrets/oci-push-token"],
  }]
}
```

`validate_oci_registry_fields` mirrors `validate_nix_cache_fields`: store-ref
resolution, `digest_function ∈ {BLAKE3, SHA256}`, size/timeout ranges, the
plaintext-listener-with-tokens refusal — all caught by `nativelink --check`
offline, and the example config joins `check-example-configs`.

## Migration plan — from zot to retired

Current fleet state: zot on watchtower (localhost-only, nginx, R2 backend),
holding the 8+ toolchain cell images; publishes via SSH tunnel; consumption
via `FetchDirectory` → CAS → `cas_toolchain` digests pinned in BUCK files.

1. **Coexist.** Deploy `oci_registry` on the existing watchtower listener
   beside `nix_cache`. zot untouched. Smoke: skopeo push/pull of a scratch
   image against the new service.
2. **Re-point publishing.** The toolchain push recipe drops the SSH tunnel
   and pushes to the CAS registry. Re-push the toolchain cells through it.
   **Digest invariant:** manifests pushed by the same skopeo from the same
   OCI layout keep their sha256 identity, and the §12 harness + the
   projection produce the same REAPI root digests — so BUCK `cas_toolchain`
   pins do not churn. Verify explicitly on the first cell before batching
   (the prelude-tools digest-churn cost is the memory that makes this a
   gate, not an assumption).
3. **Internalize the projection.** `fetch_server.rs` learns that an
   `oci://` URI whose host resolves to a local `oci_registry` instance (or a
   reserved `oci://self/…` form — open question) skips `RegistryClient`:
   manifest via `ref_store`/`index_store`, layer bytes via `cas_store` reads,
   projection unchanged. The network client remains for external registries.
4. **Retire zot.** Freeze zot read-only; confirm no consumer (grep fleet
   config + nginx logs over a week); decommission the service, the nginx
   vhost, the tunnel recipe, and the R2 bucket (contents re-pushed in
   phase 2; nothing referenced remains). Update the book's Part IX bridge
   chapter and the cxx-RBE recipe docs.

Rollback at every phase is "keep using zot" — nothing deletes zot state until
phase 4, and phase 4's freeze-then-watch makes even that reversible for the
observation window.

## Phased implementation with falsifiable gates

Each gate is a command that exits 0 or does not; no gate is "seems to work."

- **Phase A — push path.**
  Service skeleton, upload sessions (monolithic + chunked + mount), the
  dual-digest ingest, manifest PUT, tag records.
  **Gate A:** `skopeo copy oci:<layout> docker://localhost:<p>/gate-a:v1`
  exits 0 against a fresh instance, **and** every pushed blob is readable
  from the backing store by its BLAKE3 digest via REAPI `ByteStream.Read`
  (proving storage identity is canonical, not sha256-keyed by accident).
- **Phase B — pull path + conformance.**
  Blob/manifest GET/HEAD, tags/list, errors-as-spec (the `errors[]` JSON
  body with canonical codes), the three flake checks wired.
  **Gate B:** `nix flake check` passes with `oci-conformance` (all four
  categories) and `oci-client-roundtrip` (skopeo + crane, byte-identical)
  green. The conformance suite's own JUnit report is the artifact.
- **Phase C — internal projection.**
  The `fetch_server` local short-circuit; the projection-differential check.
  **Gate C:** `oci-projection-differential` green — identical REAPI root
  digest via the internal path and the network path, for a real toolchain
  cell image; plus a `cas_toolchain`-consuming buck2 build that resolves its
  toolchain with zot stopped.
- **Phase D — fleet cutover.**
  Migration phases 1–3 executed on watchtower; toolchain cells re-pushed;
  BUCK pins verified unchanged (or consciously re-locked).
  **Gate D:** the RBE trio suite (`buck2 build //...`, remote-only) green
  with zot's service stopped, on toolchains served end-to-end by the CAS
  registry.
- **Phase E — retirement + ledger.**
  zot decommissioned per migration phase 4; book chapters updated;
  `UPSTREAM-LEDGER.md` LOCAL section gains the `oci_registry` line and the
  bridge line's "candidate for a future feature offer once PROD-3 lands"
  note flips to actionable.
  **Gate E:** no zot process, vhost, tunnel script, or R2 bucket remains in
  fleet config (`grep -r zot` over the nixos/ tree is empty), and a cold
  toolchain publish→consume cycle runs with only NativeLink services.

Effort concentrates in A and B (the service and the conformance grind); C is
small (the projection already exists and the acquisition swap is localized);
D and E are operations.

## Risks and open questions

**For the owner (decisions needed):**

1. **OCI 1.1 Referrers API — in scope?** Not needed by skopeo push/pull or
   the toolchain pipeline today, but it is where attestation/SBOM artifacts
   attach — squarely on the genome thesis. Cost: a reverse `subject` index
   maintained on manifest PUT (same record machinery as Design 2). Proposal:
   schema-reserve now, implement as phase B+ only if conformance's optional
   referrers category is desired green.
2. **Repository namespace policy.** v1 is single-tenant: any valid write
   token pushes to any repo name, and blob existence is global. Acceptable
   for the fleet? If a public read surface is planned sooner than expected,
   per-repo membership records should be designed in *before* Gate D, not
   retrofitted.
3. **Mount point and multi-instance.** The spec fixes the root at `/v2/`;
   two instances on one listener need host-based routing or a path-prefix
   deviation (skopeo tolerates prefixes; the conformance suite assumes
   root). Proposal: one instance per listener, root-mounted; is that
   constraint acceptable for the watchtower topology?
4. **Delete semantics vs. CAS GC.** The design makes registry DELETE remove
   *names* (alias/tag records) and leaves content lifetime to CAS eviction —
   the lens has no GC, per the thesis. Confirm this matches operational
   expectations (i.e., nobody expects `skopeo delete` to reclaim R2 bytes
   synchronously).
5. **`oci://self` vs. host-match** for the internal projection short-circuit
   — a reserved scheme is explicit but leaks into BUCK strings; host
   matching is transparent but config-coupled. Preference?

**Engineering risks:**

- **Conformance suite fidelity** — the official suite has known gaps and
  version skew with the spec text; pin its rev, and treat the two-client
  round-trip as the compensating oracle (three oracles total is the point).
- **Chunked-upload state across restarts** — sessions are spool-file-backed
  and instance-local, like NAR spools; a restart aborts in-flight sessions
  (clients retry). Documented, not solved; matches zot's practical behavior.
- **Large-blob streaming** — the *registry* path streams via spool +
  `slow_update_store_with_file` and inherits none of the bridge's buffer-
  everything limitation; but phase C's internal projection still buffers
  (the bridge's known limitation). Unchanged by this design; the streaming-
  projection roadmap item stands on its own.
- **Index/blob write ordering** — alias record written after the blob lands;
  a crash between leaves an unreferenced blob (harmless, GC-able), never a
  dangling alias. The invariant is stated in the crate docs and tested.

## References

- Pattern source: `nativelink-service/src/nix_cache_server.rs`,
  `nativelink-nix/src/path_info.rs`, `NixCacheConfig` in
  `nativelink-config/src/cas_server.rs`.
- Existing OCI surface: `nativelink-oci/src/{registry,projection,oci_client}.rs`,
  `nativelink-service/src/fetch_server.rs`; book Part IX
  (`standard-oci-toolchain.md`, `oci-cas-bridge.md`), Appendix F.
- Thesis: `book/src/part1/the-cas-is-the-spine.md`.
- Ledger: `UPSTREAM-LEDGER.md` (OCI→REAPI bridge: LOCAL, "candidate for a
  future feature offer once PROD-3 lands the Distribution API").
- Spec: OCI Distribution Specification v1.1
  (`github.com/opencontainers/distribution-spec`), including the
  `conformance/` suite; OCI Image Format (manifest/index media types).
- Operational history motivating retirement: zot 60s readTimeout 502s; the
  localhost-only tunnel publish path; nativelink `fast_slow` write-back
  (#35) as the storage tier the registry inherits for free.
