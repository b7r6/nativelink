# Platform Properties as the Bridge

Platform properties are the only link between client intent and worker capability. They are the bridge between "what the action needs" and "what the worker has." Every toolchain approach — Nix, containers, hermetic downloads — ultimately expresses its identity through platform properties.

Understanding how to use them correctly is the difference between a working cache and a broken one.

## The Chain of Trust

```
Toolchain Identity
       │
       ▼
Platform Property Value (string)
       │
       ▼
Action Hash (includes Platform proto)
       │
       ▼
Cache Key
```

If two actions have the same cache key, the protocol assumes they're identical and the cache can serve either result for the other. If the toolchain identity isn't captured in the platform property, two different toolchains produce the same cache key, and the cache is unsound.

## Property Design Patterns

### Pattern 1: Image Digest (Containers)

```json5
// Scheduler config
supported_platform_properties: {
  "container-image": "priority"  // passed through, not matched
}
```

Client sets:
```
container-image = docker://registry.example.com/toolchain@sha256:a1b2c3...
```

**Why it works:** The digest is content-addressed. Same digest = same image = same toolchain. Different digest = different action hash = cache miss.

**Why priority, not exact:** You don't want to restrict which workers accept the action — any worker that supports containers can run any image. The property tells the worker *what to run*, not *whether it can run*.

### Pattern 2: Nix Store Path (LRE)

```json5
// Scheduler config
supported_platform_properties: {
  "lre-cc": "exact"   // or "priority" depending on deployment
}
```

Client sets:
```
lre-cc = /nix/store/zms5771rx1yqb4wd6qbj5f9sb2paq75k-clang-17.0.6
```

**Why it works:** The Nix store path is derived from the derivation content. Same derivation = same path. Different derivation = different path = different action hash.

**exact vs priority:** Use `exact` when you have multiple worker pools with different Nix closures and you want to match actions to the right pool. Use `priority` when all workers have the same Nix closure.

### Pattern 3: Version String (Hermetic Downloads)

```json5
// Scheduler config
supported_platform_properties: {
  "toolchain-version": "exact"
}
```

Client sets:
```
toolchain-version = zig-0.11.0-clang-17
```

**Why it works (partially):** If you change the version string when you change the toolchain, action hashes change. But it's correctness-by-convention — nothing enforces that the string actually reflects the toolchain content. A typo or stale value breaks correctness silently.

### Pattern 4: Platform Pool (Multi-Tenant)

```json5
// Scheduler config
supported_platform_properties: {
  "pool": "exact",
  cpu_count: "minimum"
}
```

Client sets:
```
pool = ci-linux-x86
cpu_count = 4
```

**Why it works:** Actions land on workers in the specified pool. Pools are configured identically. But identity is still by convention — there's no structural guarantee that all workers in a pool have the same toolchain.

## Composing Properties

Properties compose additively. An action can set multiple properties that together define the execution environment:

```
container-image = docker://toolchain@sha256:abc123  # toolchain identity
cpu_count = 8                                        # resource requirement
OSFamily = Linux                                     # OS requirement
ISA = x86-64                                         # architecture requirement
pool = fast-compile                                  # worker pool routing
```

The action hash includes all of these. Change any one, and you get a different cache key.

## The PropertyModifierScheduler

Sometimes you need to normalize, add, or strip properties before matching. The `PropertyModifierScheduler` wraps another scheduler and transforms properties:

**Source:** [`nativelink-scheduler/src/property_modifier_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/property_modifier_scheduler.rs)

```json5
schedulers: [{
  name: "MAIN",
  property_modifier: {
    modifications: [
      // Add a default ISA if client doesn't specify
      { add: { name: "ISA", value: "x86-64" } },
      // Remove internal-only metadata
      { remove: "internal-trace-id" },
      // Resolve mutable tag to pinned digest
      { replace: { name: "container-image",
                   value: "docker://toolchain@sha256:pinned" } }
    ],
    scheduler: { simple: { /* ... */ } }
  }
}]
```

Use cases:
- **Default injection:** Clients that don't set `ISA` get it added automatically.
- **Tag resolution:** Replace mutable image tags with pinned digests server-side (so clients don't need to know the current digest).
- **Property stripping:** Remove properties that are meaningful for caching but not for worker matching.

## The Matching Algorithm in Detail

When the scheduler receives an action with platform properties, matching proceeds as follows:

**Source:** [`nativelink-scheduler/src/worker_capability_index.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/worker_capability_index.rs)

```
For each property in action.platform_properties:
  match property_type:
    Exact   → find workers where (name, value) matches exactly
    Minimum → find workers where key exists, then verify value >= requested
    Priority → skip (doesn't restrict matching)
    Ignore  → skip (not even recorded)

Result = intersection of all worker sets from Exact/Minimum checks
```

If the result set is empty, the action stays queued until a matching worker appears (or times out). This is important for auto-scaling: if no worker matches, you may need to provision new workers with the right capabilities.

## Anti-Patterns

### Using tags instead of digests

```
# BAD: tag is mutable, same action hash with different toolchains
container-image: docker://toolchain:latest

# GOOD: digest is immutable, action hash changes with toolchain
container-image: docker://toolchain@sha256:abc123
```

### Using "priority" for matching-relevant properties

```json5
# BAD: ISA as priority means any worker gets any architecture action
supported_platform_properties: { ISA: "priority" }

# GOOD: ISA as exact means only matching workers get the action
supported_platform_properties: { ISA: "exact" }
```

### Not including toolchain identity in properties at all

If you rely on "all workers have the same toolchain installed" without any property expressing the toolchain version, you have:
- No cache safety (same action hash with different toolchains)
- No way to detect when a worker has a stale toolchain
- No way to do rolling toolchain updates (old and new workers coexist during deployment)

Always include at least one property that changes when the toolchain changes. It doesn't matter which approach you use — the property value just needs to be a function of the toolchain content.
