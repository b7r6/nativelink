# Why This Fork

This is a fork of `TraceMachina/nativelink`, and upstream is excellent — see the introduction for how genuinely we mean that. So the only question worth answering here is the one a skeptical operator actually asks: *why run this version instead of stock NativeLink?*

Five reasons. Each is a real capability you can exercise today, each has sharp edges we state plainly, and each points at the chapter that proves it. If a claim below isn't backed by code we'd want you to file a bug.

## 1. Toolchains as Content — the OCI → CAS Bridge

Stock remote execution assumes the toolchain already lives on the worker: installed, or baked into a container image the worker is running. That coupling is where cache correctness and worker fleets go to die.

This fork adds `nativelink-oci`, a bridge that projects an OCI toolchain image into an REAPI `Directory` tree *in the CAS*. A `FetchDirectory("oci://…")` call on the Remote Asset API pulls the image, folds every layer into one Merkle tree, uploads the blobs, and hands back a root `Directory` digest. That digest is the toolchain's execution identity — a client merges it into an action's `input_root_digest`, and the worker fetches the toolchain from `CAS` like any other input. The toolchain becomes data, not infrastructure: stateless workers, no redeploy on toolchain bumps, multiple versions coexisting by digest, and correctness that's structural because the toolchain digest is *in* the action hash.

**Honest scope:** it's a happy-path prototype today — `gzip` layers only, anonymous pulls (real auth is Docker Hub), single-platform manifests, whole images buffered in memory. All of that is documented, not hidden.

→ **Part IX, [The OCI → CAS Bridge](../part9/oci-cas-bridge.md)** for the projection algorithm, the `grpcurl` round-trip, and every limitation spelled out.

## 2. Your Nix Cache and Your Build Cache, One Store — the `nix_cache` Facade

`attic`-style Nix binary caches re-implement chunking, deduplication, tiering, and garbage collection — the exact machinery a serious `CAS` already has. This fork takes the opposite path: the `nix_cache` service fronts the Nix HTTP binary-cache protocol directly over NativeLink's store composition. Each NAR is a `CAS` blob. Path metadata rides inside standard REv2 `ActionResult` envelopes. Garbage collection isn't a bespoke sweep — it's eviction plus a `completeness_checking` pass, composed from wrappers that already exist. `sha256` Nix blobs and `blake3` Bazel blobs coexist in the same store, because a digest is an algorithm-blind 32 bytes keyed by `(hash, size)`.

It's a drop-in for `attic`, `harmonia`, or `nix-serve`: point a `substituters` entry at it and stock `nix` clients just work. By default it preserves the exact compression a client pushed, so a round-trip is byte-faithful. And it is torture-hardened — an adversarial stress pass found and fixed a decompression-bomb disk-DoS, an unbounded-concurrent-stream OOM, a zero-length-file panic, and more, each with a regression test.

**Honest scope:** pre-1.0, but the most finished of our additions and built to be run.

→ **Part IX, [The Nix Substituter Facade](../part9/nix-substituter.md)** for the data model, the protocol-discipline rules, and the round-trip compression story.

## 3. Never Lie About the Digest Function

REAPI lets a client declare which hash function computed its action digest. Stock NativeLink, when that field was left unset, silently defaulted to `SHA256`. A BLAKE3 client that omitted the field would get its output `Directory` trees hashed with the *wrong algorithm* — a quiet cross-hashing correctness hole that corrupts results without an error.

This fork closes it. The execution server now rejects an unset `digest_function` outright (`execution_server.rs:336`) instead of guessing, and a global `require_explicit_digest_function` mode (`nativelink-config/src/cas_server.rs:1341`) makes that strictness enforceable fleet-wide. The server uses the client-declared function or refuses the request — it never substitutes one hash for another. (A companion fix corrected the `digest_function` enum mapping itself, which a since-fixed bug had pointed at the wrong proto value.)

This is the smallest change on the list and arguably the most important, because it's the difference between a cache that's fast and a cache that's *trustworthy*.

→ **Part IX, [The Standard OCI Toolchain](../part9/standard-oci-toolchain.md)** (the fix in context) and **Part II, [Where REAPI Breaks Down](../part2/reapi-breaks.md)**.

## 4. It Builds, and It Tells You Before It Boots

Two unglamorous, load-bearing fixes round out the fork.

The **Bazel build works** again: regenerated LRE `rust` and `cc` toolchain pins for the current flake, plus the restored `BUILD` wiring the OCI crate needs. A build guide is worthless if the tree doesn't build.

And **offline config validation**: `nativelink --check <config>` parses a configuration and resolves every store and scheduler reference — catching a mistyped `cas_store` or `scheduler` name that would otherwise only surface at boot — then exits without binding a socket, touching a backend, or creating a store directory. It prints a one-line summary and returns an exit code a CI gate can read.

→ **Part V, [Nix and LRE](../part5/nix-lre.md)** and **Part VIII, [Local Remote Execution with Nix](../part8/lre-nix.md)** for the toolchain pins; the `--check` workflow is covered in **Part IX, [The Nix Substituter Facade](../part9/nix-substituter.md#validating-configuration)**.

## 5. Tee Every Network Fetch Into the CAS — the CAS Witness

Even with the two bridges above, a build still reaches past the `CAS` for raw bytes: `fetchurl` tarballs, release archives, anything a fixed-output derivation downloads before a store path exists. Those fetches are invisible to the cache and rot when an upstream URL moves. The CAS witness closes that gap. It is a TLS-intercepting caching *forward proxy* that stores every fetched body in the `CAS` under `sha256(body)` and serves the next fetch of that URL from the store. Point a build client's `HTTPS_PROXY` at it and trust its CA, and every `curl` or `fetchurl` becomes a durable, content-addressed artifact — with no integrity risk, because Nix re-verifies each fixed-output derivation against its declared hash regardless of what the proxy serves. Configure a witness key and it goes one step further: every cached fetch earns a signed DSSE/in-toto attestation, stored in the `CAS` and returned in an `X-Straylight-Witness` header — a verifiable record of what was fetched, from where, and when.

**Honest scope:** the newest and most experimental addition, and trusted-network-only. The proxy has no request authentication — anyone who can reach it can drive fetches and obtain attestations — and a cache hit re-signs its receipt after only an existence check. It is a *trusted-infrastructure attester*, not a public notary: run it inside your build network, never on the open internet.

→ **Part IX, [The CAS Witness](../part9/cas-witness.md)** for the proxy protocol, the attestation format, and the trust model in full.

## The Shape of the Bet

Read the five together and the thesis from the introduction stops being abstract. The OCI bridge pulls foreign content *into* the `CAS` as toolchains, and the CAS witness tees the build's raw network fetches into it too. The Nix facade serves `CAS` content *out* in a foreign protocol. The digest-function fix guarantees the hashing under all of it is honest. And `--check` means you find out your topology is wrong before it's serving traffic. One content-addressed store, many protocols speaking to it, correctness that's structural rather than hoped-for.

That's why this fork. Everything underneath it is TraceMachina's, and excellent. What we added is the part where the same `CAS` starts holding everything.
