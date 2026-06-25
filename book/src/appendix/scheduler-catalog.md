# Appendix B: Scheduler Catalog

Every scheduler type in NativeLink, its purpose, and configuration reference.

**Source:** [`nativelink-config/src/schedulers.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/schedulers.rs)

## SimpleScheduler

The primary scheduler. Maintains action queue, worker pool, and capability matching.

```json5
{
  name: "SCHEDULER",
  simple: {
    supported_platform_properties: {
      cpu_count: "minimum",
      OSFamily: "priority",
      ISA: "exact"
    },

    // Optional: Redis backend for multi-instance HA
    experimental_redis_scheduler_state: {
      addresses: ["redis://redis:6379"],
      key_prefix: "sched:",
      worker_timeout_s: 30,
      action_timeout_s: 600
    },

    // Optional: timeouts
    worker_timeout_s: 30,
    client_action_timeout_s: 600,

    // Optional: match logging
    experimental_log_matching: true
  }
}
```

**Source:** [`nativelink-scheduler/src/simple_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/simple_scheduler.rs)

## GrpcScheduler

Forwards Execute and WaitExecution to a remote NativeLink instance.

```json5
{
  name: "REMOTE",
  grpc: {
    endpoint: {
      address: "grpc://remote-scheduler:50051"
    },
    connections_per_endpoint: 4
  }
}
```

**Use when:** Distributed topology where a local NativeLink handles CAS/AC but forwards execution to a centralized scheduler.

**Source:** [`nativelink-scheduler/src/grpc_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/grpc_scheduler.rs)

## CacheLookupScheduler

Checks the Action Cache before dispatching. Returns cached result without involving a worker if the action was previously executed.

```json5
{
  name: "CACHED",
  cache_lookup: {
    ac_store: "AC_STORE_NAME",
    scheduler: {
      simple: { /* nested scheduler config */ }
    }
  }
}
```

**Use when:** Clients may not check AC themselves, or you want server-side deduplication of identical in-flight actions.

**Source:** [`nativelink-scheduler/src/cache_lookup_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/cache_lookup_scheduler.rs)

## PropertyModifierScheduler

Transforms platform properties before passing to a nested scheduler.

```json5
{
  name: "MODIFIED",
  property_modifier: {
    modifications: [
      { add: { name: "key", value: "value" } },
      { remove: "key-to-remove" },
      { replace: { name: "key", value: "new-value" } }
    ],
    scheduler: {
      simple: { /* nested scheduler config */ }
    }
  }
}
```

**Use when:**
- Add default properties for clients that don't set them
- Strip internal-only metadata before matching
- Resolve mutable tags to fixed digests server-side

**Source:** [`nativelink-scheduler/src/property_modifier_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/property_modifier_scheduler.rs)

## Composition Patterns

### Standard Production

```
CacheLookupScheduler
  └── SimpleScheduler (Redis backend)
```

### Multi-Pool with Defaults

```
CacheLookupScheduler
  └── PropertyModifierScheduler (add default ISA)
        └── SimpleScheduler
```

### Forwarding with Local Cache

```
CacheLookupScheduler (local AC)
  └── GrpcScheduler (remote execution)
```
