# The Standard OCI Toolchain

Part IX is where the CAS earns a second job. Beyond backing remote execution, it becomes a bridge between foreign content-addressing worlds, and the two services in this part run that bridge in opposite directions. The [OCI → CAS Bridge](./oci-cas-bridge.md) pulls foreign content *into* the CAS: it turns toolchain images pulled from an OCI registry into REAPI `Directory` trees, so a toolchain becomes data a worker fetches on demand rather than infrastructure an operator pre-installs. The [Nix Substituter Facade](./nix-substituter.md) serves CAS content back *out* in a foreign protocol — the Nix HTTP binary-cache protocol, fronted directly over store composition instead of re-implemented alongside it. This chapter opens the part with the contract the import side leans on: the Standard OCI Toolchain, a content-addressed identity that toolchain producers and NativeLink consumers can implement independently and still interoperate.

This chapter explains why a toolchain specification exists, what problem it solves for NativeLink operators, and what the integration agenda looks like. The full normative specification is in [Appendix F](../appendix/standard-oci-toolchain-spec.md).

## The Problem NativeLink Cannot Solve Alone

NativeLink is a remote execution server. It dispatches actions to workers, stores artifacts in CAS, and caches results in AC. It does all of this correctly — provided the inputs are correct.

But NativeLink has no opinion on what's inside a toolchain. It doesn't know whether the `clang` on worker A is the same `clang` on worker B. It can't verify that the `container-image` platform property actually corresponds to the image running on the worker. It trusts the client's action hash and the worker's self-reported capabilities.

This is by design — REAPI is intentionally toolchain-agnostic. But it means the entire correctness story for remote builds depends on something outside the protocol: the toolchain.

Part V of this book documents the three approaches to managing this (Nix/LRE, containers, hermetic downloads). All three work. None of them provide a **standard** — a contract that toolchain producers and consumers can implement independently and interoperate.

## What a Standard Provides

The Standard OCI Toolchain Specification defines:

1. **Identity.** A toolchain is identified by the content of its file tree — not by a tag, not by a Nix store path, not by a Dockerfile hash. Content-addressing. The same idea that makes CAS work, applied to the thing CAS stores.

2. **Projections.** The same toolchain content exists in three forms:
   - **OCI** (sha256, for registries — pull, push, sign, scan)
   - **REAPI** (BLAKE3, for execution — the Merkle tree workers consume)
   - **Container** (materialized rootfs — `docker run` for humans)

3. **Self-containedness.** A toolchain's own binaries resolve dependencies by content digest, not by path. No RPATH, no `LD_LIBRARY_PATH`, no ambient authority. A custom floor loader reads per-binary manifests and resolves from a content-addressed store.

4. **Layout.** A sysroot-compatible FHS structure. One `--sysroot` flag. No external configuration.

## Why NativeLink Cares

For NativeLink, the specification answers three operational questions:

### How does a toolchain become an action input?

Today: the toolchain lives on the worker (installed, or in a container image). The client doesn't send it — it just names it in platform properties. The worker already has it.

With the spec: the toolchain's **REAPI execution projection** is a `Directory` digest. The client merges that digest into its `input_root_digest`. The worker fetches it from CAS like any other input. The toolchain is data, not infrastructure.

This means:
- Workers are truly stateless — they don't need pre-installed toolchains
- Toolchain updates don't require redeploying workers
- Multiple toolchain versions coexist in CAS without conflict
- Cache correctness is structural — the toolchain digest is in the action hash

### How does identity flow through the system?

```
OCI Registry                    NativeLink CAS
     │                               │
     │  pull manifest (sha256)       │
     ▼                               │
  Unpack layers                      │
     │                               │
     │  rehash files (BLAKE3)        │
     ▼                               │
  REAPI Directory tree ─────────────►│  FindMissingBlobs + Upload
     │                               │
     │  root Directory digest        │
     ▼                               │
  Action.input_root_digest ─────────►│  Execute
     │                               │
     │  (worker fetches from CAS)    │
     ▼                               ▼
  Worker materializes tree      ActionResult cached
```

The OCI manifest digest (sha256) identifies the toolchain for distribution. The REAPI root Directory digest (BLAKE3) identifies it for execution. The spec's hints (§6.5) let you go from one to the other without re-fetching and re-hashing the entire tree.

### How does the BLAKE3 fix relate?

The REAPI execution projection requires BLAKE3. NativeLink previously defaulted unset `digest_function` to SHA256, making BLAKE3 clients get corrupted Directory digests. We fixed this on the `Execute` path (`straylight/straylight-nativelink@pr/sha-256-silent-default-fix`; `nativelink-service/src/execution_server.rs:336`): a request that leaves `digest_function` unset (the UNKNOWN default) is now rejected rather than silently defaulted. This is the behavior §6.6 requires and the interoperability notes in Appendix D describe.

With the fix shipped, the two-projection model works end-to-end:
- sha256 at the OCI/registry boundary (everyone already speaks it)
- BLAKE3 at the REAPI/execution boundary (faster, NativeLink-native)
- On the `Execute` path the worker uses the client-declared function or refuses the request — it never silently substitutes one for the other

One caveat: the OCI→CAS bridge (`FetchDirectory("oci://…")`) is a *different* consumer path, and it does **not** read the request's declared function. It projects with a static per-instance setting (`oci.digest_function`, default BLAKE3; `nativelink-service/src/fetch_server.rs:288`) and drops the request field, whose handler parameter is named `_digest_function_proto` (`fetch_server.rs:250`). The operator must set `oci.digest_function` to match the execution service (see [Known limitations](#known-limitations)). Spec §14.16 (honor the declared function, never assume a default) is therefore satisfied on `Execute` but not on the OCI `FetchDirectory` path.

## The Integration Agenda

What remains to make NativeLink a conforming consumer of Standard OCI Toolchains:

### Done

- **Digest function fix** — execution server rejects unset `digest_function`, global strict mode available
- **BLAKE3 support** — NativeLink already supports BLAKE3 as a digest function
- **Store composition** — CAS already stores content by digest; toolchain blobs are just more content
- **OCI → CAS bridge.** The `nativelink-oci` crate implements the pipeline: pull the image and gzip-decompress its layers, project them into an REAPI `Directory` Merkle tree (BLAKE3 by default, SHA256 optional), upload the blobs to CAS, and return the root digest. Registry reach is Docker Hub (anonymous pull token) plus any registry that serves anonymous pulls — there is no private-registry credential support (`nativelink-oci/src/registry.rs:224-237`). Exposed via `FetchDirectory("oci://…")` on the Remote Asset API. See [The OCI → CAS Bridge](./oci-cas-bridge.md).
- **FetchDirectory implementation.** The previously-unimplemented `FetchDirectory` RPC now handles both OCI URIs (via the bridge) and pre-pushed remote assets (via store lookup). Configured per-instance with a digest function and dedup settings (`OciFetchConfig`); the request's own `digest_function` is not consulted for the OCI path (see [Known limitations](#known-limitations)).

### Next

1. **Toolchain-as-input-root.** Today, clients build their own input trees and upload everything. A conforming integration would let a client say "my input root is the merge of my source tree + toolchain X's REAPI root digest" and NativeLink would handle the merge server-side. This avoids re-uploading the toolchain on every build.

2. **Hint verification.** When a toolchain image carries REAPI hints (the root Directory digest as an annotation), NativeLink should verify the hint against actual CAS content on first use, then trust it thereafter. This is a fast path — skip unpacking if the hint checks out.

3. **Worker toolchain pre-staging.** Workers that know they'll serve a particular toolchain can pre-fetch its REAPI tree into their local fast CAS tier before any action arrives. Platform properties identify which toolchains a worker should pre-stage.

4. **Conformance testing.** A `nativelink-toolchain-check` tool that validates:
   - Given an OCI image, can we produce the REAPI projection?
   - Given a REAPI root digest, does the content match the OCI layers?
   - Does the floor loader resolve correctly in a sandboxed execution?

### Known limitations

The bridge is real and exercised, but it is deliberately narrow. What it does *not* do today:

- **No private-registry credentials.** Authentication is a Docker Hub anonymous pull token for `registry-1.docker.io`; every other registry is contacted anonymously (`nativelink-oci/src/registry.rs:224-237`). There is no credential field on `OciFetchConfig`, so private repositories on other registries cannot be pulled.
- **gzip layers only.** `+gzip` (and uncompressed) layers import; a `+zstd` layer is rejected with `zstd decompression not yet implemented` (`nativelink-oci/src/oci_client.rs:356`). The spec permits zstd transport compression (§5.1), so this is a consumer gap, not a spec change.
- **No manifest-list tags.** The client parses a single image manifest only: `OciManifest.config` and `layers` are non-optional and the `Accept` header omits the index/list media types (`nativelink-oci/src/registry.rs:130-137,277`). A tag whose reference resolves to an image index — a manifest list, i.e. an image built for several architectures — fails to deserialize. Pull a single-image digest instead.
- **Request digest function ignored on the OCI path.** The projection uses the static `oci.digest_function` (default BLAKE3), not the request's declared function (`nativelink-service/src/fetch_server.rs:250`,`288`). The operator must set `oci.digest_function` to match the execution service — the field is documented as "Must match what the execution service expects" (`nativelink-config/src/cas_server.rs:203`). On `Execute` the server honors the declared function outright (§6.6); the OCI bridge instead relies on this matched-configuration discipline, so it is a conforming-consumer gap against §14.16 rather than an outright violation — the digests it produces are correct whenever the operator's config agrees with the caller.
- **Whole-image buffering.** A layer blob is read fully into memory (`registry.rs:349`) and the projected tree is held resident as a map of file content (`projection.rs:102`) before upload. Import is not streaming; a very large toolchain image is bounded by available memory.

### Out of Scope (for NativeLink)

- Building toolchains (that's Nix; the reference implementation is `straylight-toolchain`, a static-musl LLVM sysroot builder)
- Assembling toolchains (that's the C++ `std-oci-toolchain finalize` tool in `straylight-buck2-prelude`)
- The floor loader itself (`ld-std-oci-toolchain`; needed only by a *dynamically-linked* toolchain, §12)
- OCI image construction (crane/skopeo in Buck2 rules)

NativeLink's role is: store the content, serve the content, hash it correctly, and never lie about the digest function. The spec ensures that content is well-formed. Together they make toolchain management a solved problem rather than a perpetual source of cache misses.

## Reading the Spec

The full specification is [Appendix F: Standard OCI Toolchain Specification](../appendix/standard-oci-toolchain-spec.md). Key sections for NativeLink operators:

- **§6** — REAPI correspondence (how OCI verbs map to REAPI verbs)
- **§6.6** — Conforming RE server behavior (the digest function fix)
- **§9** — Container floor (what `docker run` gets you)
- **§14.16** — Conforming consumer requirement: honor the declared digest function, never assume a default (what NativeLink must do)
- **Appendix D** — Remote-execution interoperability notes
- **Appendix F** — Implementation status matrix
