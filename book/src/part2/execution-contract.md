# The Execution Contract

Remote execution works because client and server agree on a contract. The contract is implicit in the protocol but never clearly stated in the official spec. Here it is.

## What the Client Promises

1. **All inputs are in CAS before calling Execute.** The client must upload the complete input tree. If the worker can't find an input blob, the action fails.

2. **The Command is deterministic.** Given the same inputs and platform, the command produces the same outputs. If it doesn't, the cache will serve stale results. REAPI has no mechanism to express non-determinism — the contract assumes hermetic actions.

3. **The Platform is complete.** The platform properties in the action fully specify the execution environment requirements. The client is responsible for ensuring its toolchain matches what the worker provides.

4. **Output paths are declared.** The client declares which output files and directories the action will produce. The worker only uploads these paths. Anything else the command writes is discarded.

## What the Server Guarantees

1. **Content integrity.** Blobs retrieved from CAS by digest are byte-for-byte identical to what was uploaded. If they're not, the hash wouldn't match.

2. **Execution isolation.** Each action runs in its own sandbox. Actions cannot observe each other's state. (The strength of this guarantee varies — NativeLink supports Linux namespaces for PID/IPC/mount isolation.)

3. **Platform matching.** The scheduler only dispatches an action to a worker whose declared capabilities satisfy the action's platform requirements.

4. **At-least-once execution.** If the scheduler dispatches an action, it will eventually either succeed, fail, or time out. Transient failures (worker crash, network partition) result in re-dispatch.

5. **Result caching is safe to use.** If the AC returns a result for an action digest, that result is valid — it was produced by a successful execution of that exact action on a matching platform.

## What Nobody Guarantees

Here's where it gets interesting — the things the protocol explicitly does **not** guarantee:

### No toolchain identity

The platform properties are key-value string pairs. There is no standard for what they mean. `OSFamily: Linux` and `container-image: docker://ubuntu:22.04` are both valid, but neither actually specifies the toolchain. Two workers with identical platform properties can have different compilers, different libc versions, different everything.

This is the fundamental gap in REAPI, and it's why toolchain management is an entire part of this book.

### No ordering between actions

Actions are independent. The protocol provides no mechanism for expressing "action B depends on action A's outputs." Build systems handle this by only submitting action B after action A completes and its outputs are in CAS.

### No output determinism verification

The server trusts the worker. If a worker produces non-deterministic outputs, the AC caches whichever result was stored first. There is no built-in mechanism to detect or prevent this. (NativeLink's `VerifyStore` can be configured to re-hash outputs, catching corruption but not non-determinism.)

### No cache invalidation

There is no `InvalidateAction` RPC. The AC is append-only by design. If you need to invalidate cached results (because of a toolchain bug, for example), you change the inputs (which changes the action hash) or you clear the entire AC.

## The Hermeticity Spectrum

In practice, builds exist on a spectrum of hermeticity:

| Level | What's Fixed | Cache Safety |
|-------|-------------|--------------|
| **None** | Nothing. Actions depend on host state. | Unsafe. Cache hits may be wrong. |
| **Container** | OS and packages pinned by image tag. | Mostly safe. Image tag ≠ image content. |
| **Container + digest** | OS and packages pinned by image digest. | Safe. Content-addressed. |
| **Nix** | Everything pinned by Nix store path (content-addressed). | Safe. Bit-for-bit reproducible. |

NativeLink doesn't enforce any particular level. It's a server — it does what the client asks. But your cache hit rate and your cache correctness are entirely determined by where you sit on this spectrum.

The toolchain chapters (Part V) explain how to get to each level. Structural correctness — the toolchain's identity living *inside* the action hash by content-addressing rather than by convention — is reachable more than one way. A digest-pinned container image gets there (the `Container + digest` row above): the digest is content, and it's part of the platform, so it's part of the action hash. This fork's OCI → CAS bridge gets there too, by projecting a registry image into a content-addressed REAPI input tree so the toolchain digest sits in `input_root_digest` (Part IX). What Nix/LRE adds *on top* is bit-for-bit reproducibility — not merely a stable identity for the toolchain, but the ability to rebuild that exact toolchain from source anywhere. That extra guarantee is why the Nix chapters get the most attention, but it is not the only route to a correctness that is structural rather than conventional. See [Part IX](../part9/standard-oci-toolchain.md) for how the OCI bridge makes a container toolchain a first-class action input.
