# NativeLink vs. Everything Else

The remote execution space has a handful of serious implementations. Here is how they differ where it matters — stated fairly, because a skeptical evaluator reads this before trusting anything else in the book.

One framing note up front: this book documents a fork of `TraceMachina/nativelink`. In the comparisons below, "NativeLink" means the upstream project — the runtime and capabilities this fork inherits. What the fork *adds* on top gets its own section ([…and vs. upstream NativeLink](#and-vs-upstream-nativelink)), because the honest version of "why this build" is a comparison against upstream, not against the field.

## The Landscape

| System | Language | License | Self-host | Managed cloud |
|--------|----------|---------|-----------|---------------|
| **NativeLink** | Rust | `FSL-1.1-Apache-2.0` | Yes | NativeLink Cloud (TraceMachina) |
| **Buildbarn** | Go | `Apache-2.0` | Yes | — |
| **BuildFarm** | Java | `Apache-2.0` | Yes | — |
| **EngFlow** | — (closed) | Commercial | Yes (licensed) | Yes |
| **BuildBuddy** | Go | `Apache-2.0` + enterprise | Yes | Yes |

All five implement both remote caching and remote execution over REAPI; that is the price of admission, not a differentiator. What separates them is the runtime, the operational model, and the license. Every one of them can be self-hosted — including the commercial products, which offer an on-your-own-infrastructure deployment alongside their cloud. That cuts both ways for NativeLink too: TraceMachina runs **NativeLink Cloud** as a managed service, so "self-hosted or managed" is a deployment choice here, not a missing capability.

## Buildbarn

The most direct architectural comparison. Buildbarn is written in Go, is mature, and has been used at scale. The difference that shows up first is storage: Buildbarn has a fixed set of storage backends with fixed composition semantics. NativeLink's store algebra lets you compose arbitrary storage topologies in config without touching code.

Buildbarn's worker model is also more rigid — it splits into separate `bb-worker`, `bb-scheduler`, `bb-storage`, and `bb-runner` binaries. NativeLink is one binary with one config file. This matters less at scale (you deploy different configs per role anyway) but matters enormously for development, testing, and debugging.

Go's garbage collector is a real consideration at the tail. Modern Go targets sub-millisecond stop-the-world pauses, so the horror stories of multi-millisecond p99 spikes are mostly historical — but a collector you tune is still a variable in the latency budget. Rust has no garbage collector, so that variable is absent rather than minimized. Whether the difference is measurable depends on your scale: at billions of `CAS` requests per month it shows up in tail latency; below that it is mostly noise. This is a genuine engineering tradeoff, not a knockout.

## BuildFarm

BuildFarm is one of the oldest open-source Bazel remote-execution systems, long housed in the `bazelbuild` organization. It has real deployment history and it works. The costs are the ones a mature JVM codebase carries: the heaviest deployment story of the group, a larger operational footprint than the Go or Rust options, and a codebase that is the hardest of the five to modify when you need to.

If you already run BuildFarm and it works, the migration cost may not be worth it. If you are starting fresh, the Go and Rust options are lighter to operate.

## EngFlow and BuildBuddy

The commercial options. Both offer a managed cloud service *and* a self-hosted deployment, so "managed versus self-hosted" is a deployment choice with either, not a fork in the road.

**EngFlow** is closed source; you run it under a commercial license, on your own infrastructure or theirs. You get a polished product and vendor support; you do not get to read or patch the server when a failure mode is novel.

**BuildBuddy's** core is open-source Go under `Apache-2.0` and self-hostable, with a proprietary enterprise edition and a managed cloud layered on top. The open core means you can inspect and modify the parts that ship under `Apache-2.0`; the enterprise features are where the closed surface begins.

The tradeoff with either is the usual one: a supported product versus full control over your data, your performance characteristics, and your failure modes. If your organization cannot or will not operate infrastructure, a managed service — NativeLink Cloud included — is a legitimate answer. If you want that control, you operate your own. This book is about operating your own.

## …and vs. upstream NativeLink

Everything above compares implementations of the same protocol. For choosing *this* build specifically, the comparison that matters is against upstream NativeLink — because the runtime, the store algebra, and the single-binary operational model are all TraceMachina's work, and they are excellent. This fork adds a small, specific set of capabilities on top, and every one of them is something no implementation in the table above ships today.

| Capability | This fork | Upstream NativeLink | Buildbarn | BuildFarm | EngFlow | BuildBuddy |
|---|---|---|---|---|---|---|
| OCI → CAS toolchain bridge | Yes | No | No | No | No | No |
| Nix binary-cache facade | Yes | No | No | No | No | No |

**OCI → CAS toolchain bridge.** A `FetchDirectory("oci://…")` call on the Remote Asset API pulls an OCI toolchain image, folds every layer into one REAPI `Directory` tree, uploads the blobs, and returns a root `Directory` digest — the toolchain's execution identity. A client merges that digest into an action's `input_root_digest` and the worker fetches the toolchain from the `CAS` like any other input. The toolchain becomes content-addressed data instead of pre-installed infrastructure. It is a happy-path prototype today (`gzip` layers only, real auth for Docker Hub alone, single-platform manifests, whole images buffered in memory) — all documented, none hidden. See [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

**Nix binary-cache facade.** The `nix_cache` service fronts the Nix HTTP binary-cache protocol directly over the same store composition: each NAR is a `CAS` blob, path metadata rides in standard REv2 `ActionResult` envelopes, and garbage collection is eviction plus a completeness check rather than a bespoke sweep. Point a `substituters` entry at it and stock `nix` clients work with no plugin. Nix `sha256` NAR blobs and Bazel `blake3` blobs coexist in one store, because a digest is an algorithm-blind 32 bytes keyed by `(hash, size)`. See [The Nix Substituter Facade](../part9/nix-substituter.md).

Two smaller additions harden the base rather than extend it:

- **Strict digest-function safety.** The execution server refuses an `ExecuteRequest` whose digest function is unset (`execution_server.rs:336`) instead of silently defaulting it to `SHA256`, and the global `require_explicit_digest_function` (`cas_server.rs:1341`) makes that strictness enforceable fleet-wide. This closes a cross-hashing hole: a BLAKE3 client that omitted the field would otherwise get its output `Directory` trees hashed with the wrong algorithm — a quiet correctness bug, not an error.
- **Offline config validation.** `nativelink --check <config>` parses a configuration and resolves every store and scheduler reference, then exits without binding a socket or touching a backend. A mistyped `cas_store` fails in CI instead of at boot:

  ```console
  $ nativelink --check nativelink-config/examples/basic_cas_with_nix.json5
  OK: …/basic_cas_with_nix.json5 — 5 stores, 1 schedulers, 2 servers, all references resolve
  ```

None of this replaces upstream; it extends it. The runtime that makes the whole comparison favorable is TraceMachina's. What the fork adds is the part where the same `CAS` starts holding your toolchains and your Nix closures too. For the full argument, see [Why This Fork](./why-this-fork.md).

## What Actually Matters

The choice between remote execution backends comes down to four questions:

1. **Can it sustain your throughput?** At billions of requests, runtime overhead stops being theoretical. Rust's lack of a garbage collector removes tail-latency variance that a GC'd runtime has to tune for — not a knockout for Go, whose collector is genuinely good now, but one fewer variable in the budget.

2. **Can you compose the storage topology you need?** Tiered storage (fast SSD cache plus slow S3 backend), deduplication, compression, cross-region replication — these are not features you want to fork a codebase to add. NativeLink's store algebra handles all of them in config.

3. **Can you debug it when it breaks?** A single binary with a single config file and structured logging is easier to reason about than a mesh of five binaries, three config formats, and state scattered across multiple databases.

4. **Can it turn your existing OCI images and Nix closures into content-addressed inputs without a second system?** This is the fork's question. The OCI bridge pulls foreign content *into* the `CAS` as toolchains; the Nix facade serves `CAS` content *out* in a foreign protocol; both share one store with your build cache.

On the first three, the honest answer is that upstream NativeLink already competes hard — the runtime, the store algebra, and the single-binary model are its doing, and they are why NativeLink stands up well against the whole field. On the fourth, this fork is the only entry that answers *yes*.
