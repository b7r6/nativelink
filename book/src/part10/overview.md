# Nix on NativeLink

Most of this book is about NativeLink as a Bazel/Buck2 remote-execution backend. This part is about a second, independent job the same deployment can do: be a **Nix team's entire cache backend** — the substituter their `nix` clients pull from, the durable mirror their builds populate, and the caching proxy their fixed-output fetches cross — all backed by the same CAS that answers REAPI, with nothing re-implemented alongside it.

Attic, cachix, `nix-serve`, and `harmonia` each rebuild the machinery a serious CAS already has: chunking, deduplication, tiering, garbage collection, signing. NativeLink already *is* that machinery. So the Nix surface here is thin — three services and a client crate that speak Nix's protocols directly over [store composition](../part3/store-composition.md), and inherit eviction, tiering, verification, and durability from the same store wrappers every other service uses.

## The pieces

| Component | Direction | What it moves | Chapter |
|---|---|---|---|
| **`nix_cache`** substituter facade | store paths **out** | serves `/nix/store` closures over the Nix HTTP binary-cache protocol; reads through to upstream caches on a miss | [The Nix Substituter Facade](./nix-substituter.md) |
| **`nl-nix` / `nl-watch-store`** client | store paths **in** | a `nix copy`-style pusher and a `fanotify` daemon that auto-mirrors every built path | [The Nix Cache Client](./nix-cache-client.md) |
| **`cas_witness`** proxy | raw fetches **in** | tees every `fetchurl`/`fetchTarball` download into the CAS as it crosses a TLS-intercepting proxy | [The CAS Witness](./cas-witness.md) |

The substituter is the server; stock `nix` needs only a `substituters` entry to use it. The client is what a build host runs to *fill* the cache — either explicitly (`nl-nix push`) or continuously (`nl-watch-store` catches paths the instant they land). The witness closes the gap the substituter can't: a build's *raw network fetches* — the tarballs a fixed-output derivation pulls before any store path exists — which no store-path cache can mirror until someone has already built and pushed the derivation.

## One CAS, the whole build footprint

The unifying idea is that **everything a Nix build touches becomes content in one store**. A store path's NAR is a CAS blob under `DigestInfo(sha256(nar), nar_size)`; the body of a fetched URL is a CAS blob under `sha256(body)`; and both coexist with Bazel's `blake3` action outputs, because a digest is an algorithm-blind 32 bytes keyed by `(hash, size)` (see [Content-Addressing](../part2/content-addressing.md)). Nix's own metadata — the per-path `.narinfo` — rides in a standard REv2 `ActionResult` envelope in a string-keyed store, so garbage collection is eviction plus a `completeness_checking` probe rather than a bespoke sweep.

That gives a single operational invariant across all three services: **fetch or build anything once, and it is available from the CAS thereafter** — durably, if the NAR store's slow tier is backed by object storage, in which case the local disk holds only a bounded hot set and the bulk lives in the bucket. Read-through populates it on the first *upstream* miss; the watch-store daemon populates it on the first *local* build; the witness populates it on the first *raw fetch*. Three fill paths, one durable mirror of exactly the bytes your builds actually pull.

## Where this fits

- **Toolchains.** The Nix store paths [Local Remote Execution with Nix](../part8/lre-nix.md) pins are exactly what the substituter serves, so `nix develop` on a laptop and the RE workers realize the identical toolchain closure from one CAS-backed cache instead of rebuilding it. The [OCI → CAS Bridge](../part9/oci-cas-bridge.md) is the non-Nix on-ramp to that same destination.
- **Deployment.** The substituter is an ordinary service entry that mounts alongside the gRPC CAS/AC/execution on one process (see [Single Node](../part7/single-node.md) and [Kubernetes Production](../part7/kubernetes.md)); the witness is the exception — it speaks the `CONNECT` proxy protocol, so it owns its own listener.
- **Metrics.** The client emits the same OTLP the server does; the `nl.nix.*` instruments land in the same pipeline as everything else ([Observability](../part7/observability.md)).

Read the three chapters in the order above: the facade is the protocol and the store model, the client is how you fill it, the witness is the raw-fetch complement.
