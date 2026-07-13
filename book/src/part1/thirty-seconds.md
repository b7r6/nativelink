# The 30-Second Model

Here's NativeLink in one paragraph:

Your build system hashes an action (a command plus its inputs). It asks NativeLink: "have you seen this before?" If yes, NativeLink hands back the cached outputs. If no, NativeLink dispatches the action to a worker that has the right toolchain, the worker runs the command, uploads the outputs, and NativeLink remembers the result for next time.

That's it. Everything else is details about how to make this fast, correct, and composable at scale.

## The Roles

NativeLink is a single binary that can play four core roles, independently or simultaneously — and this fork adds a fifth front door that reuses the first two:

```
┌─────────────────────────────────────────────────────────────┐
│  CAS (Content-Addressable Storage)                          │
│  Every artifact stored by its hash. The ground truth.       │
├─────────────────────────────────────────────────────────────┤
│  AC (Action Cache)                                          │
│  Maps hash(action) → result. The cache hits live here.      │
├─────────────────────────────────────────────────────────────┤
│  Scheduler                                                  │
│  Receives Execute RPCs, matches actions to workers.         │
├─────────────────────────────────────────────────────────────┤
│  Worker                                                     │
│  Fetches inputs, runs commands, uploads outputs.            │
├─────────────────────────────────────────────────────────────┤
│  Nix binary cache (fork)                                    │
│  Serves CAS+AC out as a Nix substituter over HTTP.          │
└─────────────────────────────────────────────────────────────┘
```

In development, you run everything in one process. In production, you scale the pieces independently: CAS behind cloud storage, the scheduler as a service that keeps no durable state — its action queue and worker roster live in memory, so you can restart or replace it freely — and workers as an autoscaling pool.

## The Data Flow

```
Client                NativeLink                    Worker
  │                      │                            │
  ├─── GetActionResult ──►│                            │
  │◄── cache miss ────────│                            │
  │                      │                            │
  ├─── Execute ──────────►│                            │
  │                      ├─── dispatch ──────────────►│
  │                      │                            │
  │                      │     fetch inputs from CAS  │
  │                      │     run command            │
  │                      │     upload outputs to CAS  │
  │                      │                            │
  │                      │◄── ActionResult ───────────┤
  │◄── ActionResult ─────│                            │
  │                      │                            │
  ├─── read outputs ─────►│  (ByteStream from CAS)    │
  │◄── data ─────────────│                            │
```

Every single interaction is content-addressed. The client doesn't trust the server, the server doesn't trust the worker, and nobody trusts the network. Integrity is structural — if the hash matches, the data is correct.

## The Key Insight

Remote execution is not a feature of your build system. It is a **service** that your build system consumes. NativeLink doesn't know or care whether you drive it from Bazel, Buck2, Pants, Reclient, or recc — they all speak the same Remote Execution API (REAPI), and NativeLink speaks it fast. This fork puts one serious CAS behind two front doors: REAPI for build systems, and the Nix binary-cache protocol for `nix` itself (with Remote Asset fetch and push alongside, for pulling external content into the CAS).

This means your choice of build system and your choice of remote execution backend are independent decisions. You can switch either one without affecting the other. The protocol is the contract.

> **Fork note.** Three additions turn "CAS as a service" into something bigger than a build cache, all in Part IX:
>
> - [The OCI → CAS Bridge](../part9/oci-cas-bridge.md) projects OCI toolchain images into REAPI `Directory` trees, so a worker's toolchain is content it fetches on demand rather than software an operator pre-installs.
> - [The Nix Substituter Facade](../part9/nix-substituter.md) serves that same CAS back out over the Nix HTTP binary-cache protocol, so stock `nix` treats NativeLink as a substituter with nothing but a `substituters` entry.
> - [The CAS Witness](../part9/cas-witness.md) is a TLS-intercepting caching proxy that tees raw `fetchurl`-style downloads into the CAS as they cross it, so the first fetch of a URL makes the deployment a durable mirror of it.

## Why NativeLink

Three things differentiate NativeLink from alternatives:

1. **Rust, not a garbage-collected runtime.** No GC pauses. Predictable latency at the p99. The hot paths are simple and auditable.

2. **Composable stores.** Storage backends compose algebraically — you build complex topologies (fast/slow tiering, deduplication, compression, verification) by nesting store configurations. No code changes, no plugins.

3. **One binary.** CAS, AC, scheduler, and worker are all the same binary with different config. Development and production use the same code path. There is no "dev mode" that works differently from production.

## The Next 30 Seconds

None of this is aspirational. Point the binary at any example config and it resolves every store and service reference offline — no server, no network, no workers:

```bash
nativelink --check nativelink-config/examples/basic_cas_with_nix.json5
```
