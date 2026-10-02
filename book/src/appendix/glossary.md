# Appendix D: Glossary

## A

**AC (Action Cache)** — A cache that maps action digests to action results. When a client asks "has this action been run before?", the AC provides the answer. See [REAPI from First Principles](../part2/reapi.md).

**Action** — A unit of work: a command, its input tree, and its platform requirements. The fundamental unit of remote execution.

**Action Digest** — The hash of the `Action` proto (which itself contains the command digest, input root digest, and platform). This is the cache key for the AC.

## B

**Binary Cache** — A Nix term for an HTTP endpoint that serves prebuilt store paths so a client can substitute them instead of building locally. Each path is described by a `.narinfo` and its bytes ship as a NAR. The fork's `nix_cache` service is a binary cache backed by the CAS. See [The Nix Substituter Facade](../part10/nix-substituter.md).

**Blake3** — A hash function alternative to SHA-256. Faster on modern hardware. Supported by NativeLink via the Capabilities service negotiation.

**BEP (Build Event Protocol)** — A streaming protocol for build telemetry. Clients send events (action started, completed, failed) to a BEP server for monitoring and analytics.

**ByteStream** — gRPC service for streaming large blobs. Used when blobs exceed the batch API size limit (~4MB).

## C

**CacheLookupScheduler** — A scheduler *wrapper* (config key `cache_lookup`) that checks the Action Cache before dispatching; on a hit it returns the cached result without involving a worker, and on a miss it delegates to a nested `scheduler`. See [Appendix B: Scheduler Catalog](./scheduler-catalog.md).

**CAS (Content-Addressable Storage)** — Storage where every blob is keyed by its content hash. The fundamental storage primitive in REAPI. See [Why Content-Addressing Works](../part2/content-addressing.md).

**CAS Witness** — A fork-added TLS-intercepting caching forward proxy for raw network fetches (the `fetchurl`-style downloads a fixed-output derivation performs). A client points `HTTPS_PROXY` and `NIX_SSL_CERT_FILE` at it; it terminates TLS with a locally-generated CA, tees each `GET` body into the CAS keyed by `sha256(body)`, and maps the URL to that blob in an `alias_store`, so the first download of a URL makes the deployment a durable mirror of it. It is never trusted for integrity — `nix` still verifies every fetch against its declared hash — only for availability, and it can optionally emit a signed DSSE/in-toto attestation per fetch. See [The CAS Witness](../part10/cas-witness.md).

**Container Image** — A filesystem tree plus metadata packaged per the OCI Image Spec, addressed by digest and distributed through a registry. In NativeLink a container image plays two roles: it can be the toolchain a worker runs actions inside (named via the `container-image` platform property, passed through as a `priority` property — see [Container-Based Toolchains](../part5/containers.md)), or, through the OCI → CAS bridge, the source that `FetchDirectory` projects into an REAPI `Directory` tree.

**Content-Addressing** — A scheme where the name/key of data is derived from its content. Guarantees: same content = same key, same key = same content. See [Mental Model](../part1/mental-model.md).

## D

**Digest** — A `(hash, size_bytes)` pair that uniquely identifies a blob. The fundamental identity type in REAPI.

**DICE** — Deterministic Incremental Computation Engine. Buck2's internal graph evaluation framework. Not part of NativeLink itself, but NativeLink serves as the execution backend for DICE-computed actions.

**DSSE (Dead Simple Signing Envelope)** — A signature envelope format (`payloadType` + base64 `payload` + detached `signatures`) that signs over a pre-authentication encoding of the payload rather than the raw bytes. The CAS witness wraps each in-toto attestation in a DSSE envelope, signed with its ed25519 key. See [The CAS Witness](../part10/cas-witness.md).

## E

**Entrypoint** — A command prepended to every action's argv on a worker. Used to wrap actions in containers, apply resource limits, or handle timeouts.

**Eviction Policy** — Rules for removing data when a store is full. Configured with `max_bytes`, `max_count`, `max_seconds`, and `evict_bytes`.

## F

**FastSlowStore** — A composite store with a fast tier (typically memory or local disk) and a slow tier (typically cloud storage). Reads check fast first; writes go to both. See [Store Composition](../part3/store-composition.md).

**FetchDirectory** — The Remote Asset API RPC that resolves a URI to a root `Directory` digest. NativeLink serves it two ways: `oci://` and `docker://` URIs trigger the OCI → CAS bridge (pull, project, upload, return the root digest), while any other URI falls back to a stored remote-asset lookup (`fetch_server.rs:173-239`). See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

**FindMissingBlobs** — The most frequent REAPI call. Client sends a list of digests; server responds with which ones are missing. Drives efficient upload (only send what's new).

**Fingerprint (Nix)** — The exact byte string a binary cache signs to produce a narinfo `Sig`: `1;<store path>;sha256:<nixbase32 NAR hash>;<NAR size>;<comma-separated reference store paths>`. It is the signature preimage — sign or verify it with an ed25519 key, never the `.narinfo` text itself. Computed in `nativelink-nix/src/narinfo.rs`. See [The Nix Substituter Facade](../part10/nix-substituter.md).

## G

**gRPC** — The transport protocol for REAPI. NativeLink serves all services via gRPC (with optional HTTP/2 multiplexing and TLS).

## H

**Hermeticity** — The property that an action's outputs depend only on its declared inputs, not on external state. Critical for cache correctness. See [The Execution Contract](../part2/execution-contract.md).

## I

**in-toto Attestation** — A signed, machine-readable statement of the form "this subject (identified by digest) has this predicate," from the in-toto supply-chain framework. The CAS witness produces one per cached fetch — subject = the fetched body's `sha256`, predicate = the upstream URL, wall-clock time, witnessing host, and CAS keys — as an in-toto Statement wrapped in a DSSE envelope and stored in the CAS. The witness attests only that *it observed* the fetch; it is not non-repudiable proof the origin served those bytes. See [The CAS Witness](../part10/cas-witness.md).

**Instance Name** — An opaque namespace string in REAPI requests. Used to route to different service configurations. Bazel defaults to `""`, Buck2 defaults to `"main"`.

## L

**Layer (OCI)** — One tar archive (usually gzip-compressed) representing a filesystem diff in an OCI image. The bridge pulls each layer, decompresses it (gzip only — zstd is unimplemented), and folds its entries into the projected tree. Conforming toolchain images are expected to be additive and disjoint across layers (spec §5.2), so whiteouts are rejected.

**LRE (Local Remote Execution)** — NativeLink's framework for Nix-based hermetic toolchains that are identical locally and remotely. See [Nix and LRE](../part5/nix-lre.md).

## M

**Manifest (OCI)** — The JSON document that lists an image's config descriptor and its layer descriptors by digest. `RegistryClient::fetch_manifest` retrieves it; the `OciManifest` model (`registry.rs:130-140`) covers a single-image manifest only.

**Manifest List / Image Index** — A higher-level manifest that maps platforms (OS and architecture) to per-image manifests — how a multi-architecture tag is published. The bridge does not resolve these yet: `OciManifest.config` and `OciManifest.layers` are required fields and the `Accept` header omits the index media types (`registry.rs:130-137,277-279`), so an index fails to deserialize. Reference a single-platform image by digest instead.

**Merkle Tree** — A hash tree where each node's hash includes its children's hashes. Used to represent directory structures in CAS. Changing one leaf changes all ancestor hashes.

## N

**Namespace Isolation** — Linux kernel feature (`unshare()`) that isolates processes (PID namespace), filesystems (mount namespace), and IPC. Used by workers for action sandboxing.

**NAR (Nix ARchive)** — Nix's deterministic, reproducible serialization of a store path's file tree into a single byte stream — the payload a binary cache actually transfers. The `nix_cache` facade stores each NAR as one CAS blob under `DigestInfo(sha256(nar), nar_size)`. See [The Nix Substituter Facade](../part10/nix-substituter.md).

**narinfo** — The small text manifest (`<hash>.narinfo`) a Nix binary cache returns for a store path, describing where its NAR lives and how to trust it: `StorePath`, `URL`, `Compression`, `NarHash`, `NarSize`, `References`, `Deriver`, and one or more `Sig` lines. The `nix_cache` facade renders one per served path from a `NixPathInfo` record. See [The Nix Substituter Facade](../part10/nix-substituter.md).

**Negative Cache** — A cache entry that records a *miss* — "this key is known to be absent" — so a repeated lookup is answered without re-querying the backend. The `nix_cache` read-through path negative-caches a store path that every upstream definitively lacked (for `upstream_negative_ttl`), but deliberately does not cache an *indeterminate* miss, so a transient upstream outage cannot be frozen into a lasting `404`.

**Nix Substituter Facade (`nix_cache`)** — A fork-added service that fronts the Nix HTTP binary-cache protocol directly over NativeLink's store composition, so a stock `nix` client can list a NativeLink deployment as an ordinary `substituters` entry. It is the mirror image of the OCI → CAS bridge: where that bridge pulls foreign content *into* the CAS, the facade serves CAS content *out* in a foreign protocol. Unlike every other service — which speaks gRPC on the listener — `nix_cache` mounts a plain-HTTP router at `/nix/<instance_name>`, coexisting with the gRPC CAS, AC, and execution services on one port. It composes three stores (`NixCacheConfig`, `cas_server.rs:246`, `deny_unknown_fields`): a digest-keyed `cas_store` holding each NAR as an uncompressed CAS blob under `DigestInfo(sha256(nar), nar_size)`; a string-keyed `path_info_store` whose records are REv2 `ActionResult` envelopes carrying a `NixPathInfo` message (`nativelink-nix/src/path_info.rs`); and a string-keyed `alias_store` mapping client-chosen NAR URLs to `(digest, size)`. Because path-info records are `ActionResult` protos, wrapping `path_info_store` in `completeness_checking` makes garbage collection fall out of store composition — an evicted NAR turns its `.narinfo` into a clean 404 miss rather than a served-but-broken record. Implemented in `nativelink-service/src/nix_cache_server.rs`. See [The Nix Substituter Facade](../part10/nix-substituter.md).

**nixbase32 / nix32** — Nix's nonstandard base-32 encoding: LSB-first, high digit emitted first, over a 32-character alphabet that omits `e`, `o`, `u`, and `t` so hashes are unlikely to spell words. A 32-byte `sha256` digest encodes to 52 characters. It is how store-path hashes and NAR hashes are written, and the fork implements it byte-for-byte against Nix in `nativelink-nix/src/nixbase32.rs`.

## O

**OCI (Open Container Initiative)** — The standards body and image format for container images. An OCI image is a manifest plus a config plus layer blobs, each addressed by a `sha256` digest. NativeLink's `nativelink-oci` crate pulls OCI images and projects them into REAPI `Directory` trees; it is a registry *client*, not a registry server. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

**OCI Distribution (Registry)** — The HTTP API (OCI Distribution Spec v2) that serves image manifests and blobs — Docker Hub, `ghcr.io`, and the cloud providers' registries all speak it. `RegistryClient` (`registry.rs`) uses it to pull manifests and layer blobs. Authentication is anonymous except for a `Bearer`-token handshake against Docker Hub (`registry-1.docker.io`, `registry.rs:230-237`); there is no credential configuration, so private registries are not yet supported.

**`oci://` / `docker://` URI scheme** — The `FetchDirectory` URI prefixes that route a request to the OCI toolchain bridge instead of a plain remote-asset lookup (`fetch_server.rs:192`). The two are treated identically; the remainder is parsed as `registry/repo:tag` or `registry/repo@sha256:…` (`registry.rs:66-110`).

## P

**Platform Properties** — Key-value string pairs that describe what an action needs (client-side) or what a worker provides (server-side). The bridge between actions and workers. See [Platform Properties](../part4/platform-properties.md).

**PropertyModifierScheduler** — A scheduler *wrapper* (config key `property_modifier`) that rewrites platform properties — `add`, `remove`, or `replace`, applied in order — before forwarding to a nested `scheduler`. See [Appendix B: Scheduler Catalog](./scheduler-catalog.md).

**PropertyType** — NativeLink's classification of platform properties: `minimum` (numeric, >=), `exact` (string equality), `priority` (informational), `ignore` (allowed but unchecked).

## R

**Read-Through Cache** — A cache that, on a miss, fetches the item from a configured backing source, stores it, and serves it — so the caller sees a uniform interface and the first request populates the cache. The `nix_cache` facade can read through to upstream Nix binary caches: a store path absent locally is fetched from an upstream, persisted into the CAS, and served, turning the deployment into a durable mirror. See [The Nix Substituter Facade](../part10/nix-substituter.md).

**REAPI (Remote Execution API)** — The gRPC protocol for remote caching and execution, defined by the Bazel team. Implemented by NativeLink, Buildbarn, BuildFarm, EngFlow, and BuildBuddy. See [REAPI from First Principles](../part2/reapi.md).

**RefStore** — A store that references another named store by name. Enables sharing a single physical store across multiple service configurations without config duplication.

**Remote Asset API** — The gRPC API (`build.bazel.remote.asset.v1`) for resolving external URIs into CAS content, via `Fetch.FetchBlob` and `Fetch.FetchDirectory`. NativeLink's `FetchServer` implements it and hosts the OCI → CAS bridge on the `FetchDirectory` RPC. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

## S

**Scheduler** — The component that receives Execute RPCs and dispatches actions to matching workers. Maintains the action queue and worker pool.

**SimpleScheduler** — The primary *leaf* scheduler (config key `simple`) that owns the action queue, the connected-worker pool, and the capability index matching actions to workers via `supported_platform_properties`. It terminates any wrapper chain and is the only scheduler with the experimental Redis state backend. See [Appendix B: Scheduler Catalog](./scheduler-catalog.md).

**Store** — NativeLink's fundamental abstraction. Any component that implements `has`/`update`/`get_part` for content-addressed blobs. See [The Store Trait](../part3/store-trait.md).

**Store Path (Nix)** — A path of the form `/nix/store/<hash>-<name>`, where `<hash>` is a nixbase32-encoded digest that fixes the path's identity. It is the unit a binary cache serves: a `.narinfo` names it and a NAR carries its bytes. See [The Nix Substituter Facade](../part10/nix-substituter.md).

**StoreDriver** — The implementor trait for stores. Concrete store types implement `StoreDriver`; callers use the type-erased `Store` wrapper.

**Substituter** — In Nix, a binary cache a client is willing to fetch prebuilt store paths from, listed in the `substituters` setting. The fork's `nix_cache` service is a drop-in substituter over the CAS: add its URL and stock `nix` uses it with no plugin. See [The Nix Substituter Facade](../part10/nix-substituter.md).

## T

**Toolchain** — The compiler, linker, standard library, and supporting tools used to build software. Not a concept in REAPI (which only has platform properties), making toolchain management an operational challenge. See [The Toolchain Problem](../part5/toolchain-problem.md).

**Toolchain Projection (OCI → REAPI)** — The transformation of an OCI image into an REAPI `Directory` Merkle tree: iterate each layer's tar entries, re-hash every file's content with the configured digest function (BLAKE3 by default), build `Directory` protos bottom-up, and upload every blob to CAS. Implemented in `projection.rs`; the resulting root `Directory` digest is the toolchain's execution identity, which a client merges into an action's `input_root_digest`. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

## W

**Whiteout** — An OCI layer marker (a file whose name begins with `.wh.`) that deletes a path present in a lower layer. Because the toolchain projection treats conforming images as additive and disjoint (spec §5.2), it rejects whiteouts rather than applying them (`projection.rs:169-175`). Flatten the image to a single additive layer to avoid them.

**Worker** — A NativeLink process that executes actions. Fetches inputs from CAS, runs commands, uploads outputs. Stateless and disposable. See [Workers](../part4/workers.md).

**Worker API** — Internal gRPC API between workers and the scheduler. Not part of REAPI. Handles registration, heartbeat, and result reporting.
