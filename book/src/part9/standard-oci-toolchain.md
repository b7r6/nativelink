# The Standard OCI Toolchain

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

The REAPI execution projection requires BLAKE3. NativeLink previously defaulted unset `digest_function` to SHA256, making BLAKE3 clients get corrupted Directory digests. We fixed this (Appendix D of the spec, `pr/sha-256-silent-default-fix` in this repo).

With the fix shipped, the two-projection model works end-to-end:
- sha256 at the OCI/registry boundary (everyone already speaks it)
- BLAKE3 at the REAPI/execution boundary (faster, NativeLink-native)
- No confusion between them — the declared function is always honored

## The Integration Agenda

What remains to make NativeLink a conforming consumer of Standard OCI Toolchains:

### Done

- **Digest function fix** — execution server rejects unset `digest_function`, global strict mode available
- **BLAKE3 support** — NativeLink already supports BLAKE3 as a digest function
- **Store composition** — CAS already stores content by digest; toolchain blobs are just more content

### Done

- **OCI → CAS bridge.** The `nativelink-oci` crate implements the full pipeline: pull from any OCI registry, decompress layers, project into an REAPI `Directory` Merkle tree (BLAKE3), upload to CAS, and return the root digest. Exposed via `FetchDirectory("oci://…")` on the Remote Asset API. See [The OCI → CAS Bridge](./oci-cas-bridge.md).

- **FetchDirectory implementation.** The previously-unimplemented `FetchDirectory` RPC now handles both OCI URIs (via the bridge) and pre-pushed remote assets (via store lookup). Configured per-instance with digest function and dedup settings.

### Next

1. **Toolchain-as-input-root.** Today, clients build their own input trees and upload everything. A conforming integration would let a client say "my input root is the merge of my source tree + toolchain X's REAPI root digest" and NativeLink would handle the merge server-side. This avoids re-uploading the toolchain on every build.

2. **Hint verification.** When a toolchain image carries REAPI hints (the root Directory digest as an annotation), NativeLink should verify the hint against actual CAS content on first use, then trust it thereafter. This is a fast path — skip unpacking if the hint checks out.

3. **Worker toolchain pre-staging.** Workers that know they'll serve a particular toolchain can pre-fetch its REAPI tree into their local fast CAS tier before any action arrives. Platform properties identify which toolchains a worker should pre-stage.

4. **Conformance testing.** A `nativelink-toolchain-check` tool that validates:
   - Given an OCI image, can we produce the REAPI projection?
   - Given a REAPI root digest, does the content match the OCI layers?
   - Does the floor loader resolve correctly in a sandboxed execution?

### Out of Scope (for NativeLink)

- Building toolchains (that's Nix + Buck2 in `straylight-buck2-prelude`)
- Assembling toolchains (that's `std-oci-toolchain finalize`)
- The floor loader itself (that's `ld-std-oci-toolchain`, deployed in the container)
- OCI image construction (that's crane/skopeo in Buck2 rules)

NativeLink's role is: store the content, serve the content, hash it correctly, and never lie about the digest function. The spec ensures that content is well-formed. Together they make toolchain management a solved problem rather than a perpetual source of cache misses.

## Reading the Spec

The full specification is [Appendix F: Standard OCI Toolchain Specification](../appendix/standard-oci-toolchain-spec.md). Key sections for NativeLink operators:

- **§6** — REAPI correspondence (how OCI verbs map to REAPI verbs)
- **§6.6** — REAPI unblock status (the digest function fix)
- **§9** — Container floor (what `docker run` gets you)
- **§14.11** — Conforming consumer requirements (what NativeLink must do)
- **Appendix D** — The conformance gap we closed
- **Appendix F** — Implementation status matrix
