# Platform Properties

Platform properties are how actions find workers. An action declares what it needs (`OSFamily: Linux`, `cpu_count: 4`). A worker declares what it has (`OSFamily: Linux`, `cpu_count: 8`). The scheduler matches them.

This sounds simple. In practice, it's where most deployments go wrong.

## The Matching Model

NativeLink's scheduler classifies each property into one of four types:

```rust
// nativelink-config/src/schedulers.rs

pub enum PropertyType {
    Minimum,  // u64 — worker must have >= requested
    Exact,    // string — must match exactly
    Priority, // informational — doesn't restrict matching
    Ignore,   // allowed but never checked
}
```

**Source:** [`nativelink-config/src/schedulers.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/schedulers.rs)

You declare these types in the scheduler config:

```json5
supported_platform_properties: {
  cpu_count: "minimum",
  memory_mb: "minimum",
  OSFamily: "priority",
  "container-image": "priority",
  ISA: "exact",
  "toolchain-version": "exact"
}
```

### Minimum

Numeric comparison. The action requests a minimum; the worker must meet or exceed it.

Action: `cpu_count: 4` → Worker with `cpu_count: 8` → **match**
Action: `cpu_count: 16` → Worker with `cpu_count: 8` → **no match**

Workers determine their values dynamically or statically:

```json5
platform_properties: {
  cpu_count: { query_cmd: "nproc" },     // dynamic: runs at startup
  memory_mb: { values: ["32768"] }        // static: fixed value
}
```

### Exact

String equality. The worker must advertise the exact same value.

Action: `ISA: x86-64` → Worker with `ISA: x86-64` → **match**
Action: `ISA: aarch64` → Worker with `ISA: x86-64` → **no match**

### Priority

Informational. The property is passed through to the worker but doesn't affect matching. Every worker is eligible regardless of whether it advertises the property.

This is typically used for `container-image` — the value tells the worker *what image to use* but doesn't restrict which workers can accept the action. The worker's entrypoint script reads the property and acts on it.

### Ignore

The property is allowed in requests (so clients don't get errors) but is completely ignored during matching. Use this for client-side metadata that has no server-side meaning.

## The Capability Index

Matching is performed by the `WorkerCapabilityIndex`, an inverted index that maps property values to worker sets:

**Source:** [`nativelink-scheduler/src/worker_capability_index.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/worker_capability_index.rs)

```rust
// Conceptual structure (simplified):
// (property_name, property_value) → HashSet<WorkerId>

// For an action with properties [ISA=x86-64, cpu_count=4]:
// 1. Look up workers where ISA=x86-64 (exact match in index)
// 2. Look up workers where cpu_count key exists (presence check)
// 3. Intersect the worker sets
// 4. For minimum properties, verify actual values >= requested
```

This gives O(P × log(W)) matching where P is the number of properties and W is the number of workers — fast even with thousands of workers.

## Common Patterns

### The Container Image Pattern

Workers accept any action and use the `container-image` property to select execution environment:

```json5
// Scheduler config
supported_platform_properties: {
  "container-image": "priority",  // doesn't restrict matching
  OSFamily: "priority",
  cpu_count: "minimum"
}

// Worker config
platform_properties: {
  cpu_count: { query_cmd: "nproc" },
  OSFamily: { values: ["Linux"] },
  "container-image": { values: [""] }  // accepts any
}
```

The worker's entrypoint script reads `container-image` from the action's environment (via `EnvironmentSource::Property`) and launches the action inside that container.

### The Pool Pattern

Separate worker pools for different workloads, matched by exact properties:

```json5
// Scheduler config
supported_platform_properties: {
  "pool": "exact",
  cpu_count: "minimum"
}

// Fast-compile workers
platform_properties: {
  pool: { values: ["compile"] },
  cpu_count: { query_cmd: "nproc" }
}

// Test workers (more memory, less CPU)
platform_properties: {
  pool: { values: ["test"] },
  cpu_count: { values: ["2"] }
}
```

Clients set `--remote_default_exec_properties=pool=compile` or `pool=test` to route actions.

### The Nix/LRE Pattern

Workers advertise Nix store paths as exact-match properties:

```json5
supported_platform_properties: {
  "lre-cc": "exact",
  "lre-rs": "exact",
  OSFamily: "priority"
}

platform_properties: {
  "lre-cc": { values: ["/nix/store/abc123-clang-17/bin/clang"] },
  "lre-rs": { values: ["/nix/store/def456-rust-1.75/bin/rustc"] },
  OSFamily: { values: ["Linux"] }
}
```

The toolchain path is content-addressed by Nix. If the toolchain changes, the Nix store path changes, the property changes, workers re-register with the new value, and actions with old toolchain paths get "no matching worker" (correctly — there's no worker with the old toolchain anymore).

This is structural correctness. You can't get a cache hit from a mismatched toolchain because the platform property is derived from the toolchain content.

## Environment Injection

Workers can inject platform property values into the action's environment, making them available to entrypoint scripts:

```json5
// Worker config (nativelink-config/src/cas_server.rs)
additional_environment: {
  CONTAINER_IMAGE: { property: "container-image" },
  ACTION_TIMEOUT: { timeout_millis: {} },
  WORK_DIR: { action_directory: {} },
  SIDE_CHANNEL: { side_channel_file: {} }
}
```

**Source:** [`nativelink-config/src/cas_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/cas_server.rs) — the `EnvironmentSource` enum.

This is how the `container-image` pattern works in practice: the platform property value flows from the action, through the scheduler, into the worker's environment, and the entrypoint script uses it to `docker run` the right image.

## Gotchas

1. **Unknown properties fail the request.** If a client sends a property not listed in `supported_platform_properties`, the action is rejected. Add `"new-property": "ignore"` before clients start sending it.

2. **Empty string values.** A worker with `container-image: { values: [""] }` matches actions that either don't set `container-image` or set it to empty string. It does NOT match actions with `container-image: docker://something` when the type is `exact`. Use `priority` type for pass-through properties.

3. **Dynamic queries run once.** `query_cmd: "nproc"` runs at worker startup, not per-action. If your worker's resources change at runtime, the scheduler won't know.
