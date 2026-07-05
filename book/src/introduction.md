# Introduction

Content addressing is the best idea in build infrastructure, and almost nobody takes it all the way.

The pitch is simple: name everything by the hash of its bytes, and identity stops being a guess. A source file, a compiler output, an action result — if the hash matches, the data is correct, and no clock, path, tag, or promise gets a vote. Build systems have understood this for a decade. What they keep missing is how far it goes. Your source tree is content. Your build outputs are content. Your *toolchain* is content. A Nix closure is content. The thing your registry calls an "image" is content wearing a costume. Push the idea to its conclusion and one truth falls out: there should be one content-addressed store under all of it, and everything else — execution, toolchain distribution, binary caching — should be a protocol that speaks to that store.

This fork is a bet on that conclusion. NativeLink already gives you a serious Content-Addressable Storage engine and a Remote Execution service in front of it. We asked what else could live behind the same `CAS` if we stopped treating it as a build-cache implementation detail and started treating it as the ground truth it already is. The answer, so far: toolchains projected out of OCI registries as REAPI `Directory` trees, and the entire Nix binary-cache protocol served straight off store composition. Different clients, different wire formats, one store. That is the whole vision — content-addressed everything, converging.

This book is the map of that idea, and the honest field guide to the system that implements it.

## Standing on Their Shoulders

*NativeLink is TraceMachina's work, and it is superb. We did not write the engine — they did, in Rust, at a level of care that shows on every hot path: a store trait clean enough to compose storage topologies in config instead of code, a scheduling model that holds up at billions of requests a month, and a discipline about correctness that made everything we built on top of it possible. They designed it well and then gave it away under an open license, which is the rarer and more generous act. Everything load-bearing beneath this fork is theirs. We are admirers first and contributors second, and we would rather you understand their system than ours — most of this book is exactly that. When you read a claim in here about speed, store composition, or protocol fidelity, the credit runs upstream to `TraceMachina/nativelink`; we are extending their work, not competing with it.*

The sharp edges, the experiments, and any bugs in the new crates are ours.

## A Note on the Agents

Here is the wink, because you would work it out anyway: this fork — and this very book — is us having fun with the agents.

The `nativelink-oci` bridge, the `nix_cache` facade, the digest-function safety fix, and the chapters documenting all three were built and written largely by autonomous agents working the codebase directly. That is not a disclaimer, it is the point of the exercise. And the part we like best is the hardening. We pointed an adversarial agent at the Nix substituter and told it to break its own work. It did — it found a decompression-bomb that let a one-megabyte upload fill a disk, an unbounded-stream path that would OOM the process under slow readers, and a zero-length-file panic reachable by re-uploading an empty build log. Then it fixed all of them, added the tests, and re-ran the torture pass to confirm. Agents wrote the code, agents tried to destroy it, agents wrote this paragraph. We are enjoying ourselves, and we think it shows in how thoroughly the new surfaces got stress-tested.

## What's Solid and What's Experimental

We would rather tell you than have you find out.

Base NativeLink — `CAS`, `AC`, scheduling, workers, the store algebra — is production-grade and carries real load upstream. Treat it accordingly.

The `nix_cache` substituter facade is the most finished of our additions: it is a drop-in for `attic` or `nix copy`, it round-trips compression faithfully, and it has been through the torture sweep described above. It is pre-1.0 and we say so, but it is built to be run.

The `nativelink-oci` bridge is a deliberate happy-path prototype. It projects conforming, single-platform, `gzip`-layer OCI images pulled anonymously (real auth is Docker Hub only), and it buffers whole images in memory rather than streaming. It is real, it is exercised, and it is narrow. Every limitation is stated plainly in its chapter, because a bold claim we can back is powerful and one we can't is embarrassing.

## Who This Is For

You build software. You've heard of remote caching or remote execution — maybe you've fought with it. You use Bazel, Buck2, or another build system that speaks REAPI, and you want to understand what actually happens when your build talks to a remote backend, without cargo-culting YAML from a getting-started guide.

If you're evaluating NativeLink against Buildbarn, EngFlow, or BuildFarm, this book makes the architectural differences obvious. If you're trying to drag your cache hit rate above 90%, the toolchain chapters will save you weeks. And if the idea of serving your Nix cache and your Bazel `CAS` from one content-addressed store sounds good to you, Part IX is why this fork exists.

## What You'll Learn

By the end of this book you will understand:

- **REAPI** — the protocol Bazel, Buck2, and NativeLink all speak, from first principles
- **The store algebra** — how NativeLink composes storage backends into arbitrary topologies through a single trait
- **Scheduling and dispatch** — how actions match to workers, and why platform properties are the entire game
- **Toolchains** — the three approaches (Nix/LRE, containers, hermetic cross-compilers) and when each one wins
- **Client integration** — Bazel and Buck2 as equal first-class citizens, with real configs and real gotchas
- **Deployment** — from a single binary on localhost to a production Kubernetes fleet with observability
- **This fork's extensions** — the OCI → CAS bridge that turns registry images into content-addressed REAPI input trees, the Nix substituter that serves the same `CAS` as a Nix binary cache, and the digest-function safety fix that closes a cross-hashing correctness hole

## How to Read This

Parts I and II give you the mental model and the protocol. Parts III and IV are the internals — stores and scheduling. Part V is toolchains, which is where most teams get stuck. Part VI is client integration. Parts VII and VIII are deployment and worked examples. Part IX is this fork's own surface: the Standard OCI Toolchain and the two bridges that project content into and out of the `CAS`.

Read it front to back the first time. The code links point into the actual NativeLink source — follow them. When a link points at `github.com/straylight-prelude/straylight-nativelink`, that's our fork; when the credit runs to `TraceMachina/nativelink`, that's the foundation.

## A Note on Tone

This book is opinionated, because remote execution is too important and too poorly understood for hedging. When something is wrong, we say so. When there's a better way, we show it. When the protocol has gaps, we name them. When one of our own additions has a sharp edge, we point at it before you hit it.

The build-system world has spent a decade telling people to "just set `--remote_cache`" and wondering why cache hit rates are terrible. The answer is always toolchains. This book will make that obvious — and then it will show you what happens when you let the same `CAS` hold your toolchains, your Nix closures, and your build outputs all at once.

Let's go.
