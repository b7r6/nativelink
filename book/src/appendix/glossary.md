# Appendix D: Glossary

## A

**AC (Action Cache)** — A cache that maps action digests to action results. When a client asks "has this action been run before?", the AC provides the answer. See [REAPI from First Principles](../part2/reapi.md).

**Action** — A unit of work: a command, its input tree, and its platform requirements. The fundamental unit of remote execution.

**Action Digest** — The hash of the `Action` proto (which itself contains the command digest, input root digest, and platform). This is the cache key for the AC.

## B

**Blake3** — A hash function alternative to SHA-256. Faster on modern hardware. Supported by NativeLink via the Capabilities service negotiation.

**BEP (Build Event Protocol)** — A streaming protocol for build telemetry. Clients send events (action started, completed, failed) to a BEP server for monitoring and analytics.

**ByteStream** — gRPC service for streaming large blobs. Used when blobs exceed the batch API size limit (~4MB).

## C

**CAS (Content-Addressable Storage)** — Storage where every blob is keyed by its content hash. The fundamental storage primitive in REAPI. See [Why Content-Addressing Works](../part2/content-addressing.md).

**Container Image** — A filesystem tree plus metadata packaged per the OCI Image Spec, addressed by digest and distributed through a registry. In NativeLink a container image plays two roles: it can be the toolchain a worker runs actions inside (named via the `container-image` platform property, passed through as a `priority` property — see [Container-Based Toolchains](../part5/containers.md)), or, through the OCI → CAS bridge, the source that `FetchDirectory` projects into an REAPI `Directory` tree.

**Content-Addressing** — A scheme where the name/key of data is derived from its content. Guarantees: same content = same key, same key = same content. See [Mental Model](../part1/mental-model.md).

## D

**Digest** — A `(hash, size_bytes)` pair that uniquely identifies a blob. The fundamental identity type in REAPI.

**DICE** — Deterministic Incremental Computation Engine. Buck2's internal graph evaluation framework. Not part of NativeLink itself, but NativeLink serves as the execution backend for DICE-computed actions.

## E

**Entrypoint** — A command prepended to every action's argv on a worker. Used to wrap actions in containers, apply resource limits, or handle timeouts.

**Eviction Policy** — Rules for removing data when a store is full. Configured with `max_bytes`, `max_count`, `max_seconds`, and `evict_bytes`.

## F

**FastSlowStore** — A composite store with a fast tier (typically memory or local disk) and a slow tier (typically cloud storage). Reads check fast first; writes go to both. See [Store Composition](../part3/store-composition.md).

**FetchDirectory** — The Remote Asset API RPC that resolves a URI to a root `Directory` digest. NativeLink serves it two ways: `oci://` and `docker://` URIs trigger the OCI → CAS bridge (pull, project, upload, return the root digest), while any other URI falls back to a stored remote-asset lookup (`fetch_server.rs:173-239`). See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

**FindMissingBlobs** — The most frequent REAPI call. Client sends a list of digests; server responds with which ones are missing. Drives efficient upload (only send what's new).

## G

**gRPC** — The transport protocol for REAPI. NativeLink serves all services via gRPC (with optional HTTP/2 multiplexing and TLS).

## H

**Hermeticity** — The property that an action's outputs depend only on its declared inputs, not on external state. Critical for cache correctness. See [The Execution Contract](../part2/execution-contract.md).

## I

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

## O

**OCI (Open Container Initiative)** — The standards body and image format for container images. An OCI image is a manifest plus a config plus layer blobs, each addressed by a `sha256` digest. NativeLink's `nativelink-oci` crate pulls OCI images and projects them into REAPI `Directory` trees; it is a registry *client*, not a registry server. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

**OCI Distribution (Registry)** — The HTTP API (OCI Distribution Spec v2) that serves image manifests and blobs — Docker Hub, `ghcr.io`, and the cloud providers' registries all speak it. `RegistryClient` (`registry.rs`) uses it to pull manifests and layer blobs. Authentication is anonymous except for a `Bearer`-token handshake against Docker Hub (`registry-1.docker.io`, `registry.rs:230-237`); there is no credential configuration, so private registries are not yet supported.

**`oci://` / `docker://` URI scheme** — The `FetchDirectory` URI prefixes that route a request to the OCI toolchain bridge instead of a plain remote-asset lookup (`fetch_server.rs:192`). The two are treated identically; the remainder is parsed as `registry/repo:tag` or `registry/repo@sha256:…` (`registry.rs:66-110`).

## P

**Platform Properties** — Key-value string pairs that describe what an action needs (client-side) or what a worker provides (server-side). The bridge between actions and workers. See [Platform Properties](../part4/platform-properties.md).

**PropertyType** — NativeLink's classification of platform properties: `minimum` (numeric, >=), `exact` (string equality), `priority` (informational), `ignore` (allowed but unchecked).

## R

**REAPI (Remote Execution API)** — The gRPC protocol for remote caching and execution, defined by the Bazel team. Implemented by NativeLink, Buildbarn, BuildFarm, EngFlow, and BuildBuddy. See [REAPI from First Principles](../part2/reapi.md).

**RefStore** — A store that references another named store by name. Enables sharing a single physical store across multiple service configurations without config duplication.

**Remote Asset API** — The gRPC API (`build.bazel.remote.asset.v1`) for resolving external URIs into CAS content, via `Fetch.FetchBlob` and `Fetch.FetchDirectory`. NativeLink's `FetchServer` implements it and hosts the OCI → CAS bridge on the `FetchDirectory` RPC. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

## S

**Scheduler** — The component that receives Execute RPCs and dispatches actions to matching workers. Maintains the action queue and worker pool.

**Store** — NativeLink's fundamental abstraction. Any component that implements `has`/`update`/`get_part` for content-addressed blobs. See [The Store Trait](../part3/store-trait.md).

**StoreDriver** — The implementor trait for stores. Concrete store types implement `StoreDriver`; callers use the type-erased `Store` wrapper.

## T

**Toolchain** — The compiler, linker, standard library, and supporting tools used to build software. Not a concept in REAPI (which only has platform properties), making toolchain management an operational challenge. See [The Toolchain Problem](../part5/toolchain-problem.md).

**Toolchain Projection (OCI → REAPI)** — The transformation of an OCI image into an REAPI `Directory` Merkle tree: iterate each layer's tar entries, re-hash every file's content with the configured digest function (BLAKE3 by default), build `Directory` protos bottom-up, and upload every blob to CAS. Implemented in `projection.rs`; the resulting root `Directory` digest is the toolchain's execution identity, which a client merges into an action's `input_root_digest`. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

## W

**Whiteout** — An OCI layer marker (a file whose name begins with `.wh.`) that deletes a path present in a lower layer. Because the toolchain projection treats conforming images as additive and disjoint (spec §5.2), it rejects whiteouts rather than applying them (`projection.rs:169-175`). Flatten the image to a single additive layer to avoid them.

**Worker** — A NativeLink process that executes actions. Fetches inputs from CAS, runs commands, uploads outputs. Stateless and disposable. See [Workers](../part4/workers.md).

**Worker API** — Internal gRPC API between workers and the scheduler. Not part of REAPI. Handles registration, heartbeat, and result reporting.
