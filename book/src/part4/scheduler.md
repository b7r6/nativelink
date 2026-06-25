# The Scheduler

The scheduler receives `Execute` RPCs from clients and dispatches actions to workers. It is the brain of the execution system — it decides who runs what, when, and how to handle failures.

## SimpleScheduler

The primary scheduler implementation. Despite the name, it handles production workloads.

**Source:** [`nativelink-scheduler/src/simple_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/simple_scheduler.rs)

The SimpleScheduler maintains:
- A **queue** of pending actions (sorted by priority and insertion time)
- A **worker pool** of connected workers with their declared capabilities
- A **capability index** for O(P × log(W)) matching of actions to workers

When an action arrives:
1. Scheduler checks the capability index for workers matching the action's platform properties
2. If a matching worker is idle, dispatch immediately
3. If no matching worker is idle, enqueue the action
4. When a worker completes an action, scheduler checks the queue for the next matching action

### Redis Backend

For deployments with multiple scheduler instances (stateless horizontal scaling), the SimpleScheduler can use Redis as a shared state backend:

```json5
schedulers: [
  {
    name: "MAIN_SCHEDULER",
    simple: {
      supported_platform_properties: {
        cpu_count: "minimum",
        OSFamily: "priority",
        ISA: "exact"
      },
      experimental_redis_scheduler_state: {
        addresses: ["redis://redis:6379"],
        key_prefix: "sched:",
        worker_timeout_s: 30,
        action_timeout_s: 600
      }
    }
  }
]
```

Without Redis, scheduler state is in-memory (single instance only).

## GrpcScheduler

A forwarding scheduler that proxies `Execute` and `WaitExecution` calls to a remote NativeLink instance.

```json5
schedulers: [
  {
    name: "REMOTE_SCHEDULER",
    grpc: {
      endpoint: { address: "grpc://scheduler.internal:50051" },
      connections_per_endpoint: 4
    }
  }
]
```

**Use when:** You have a centralized scheduler and want local NativeLink instances (e.g., in CI runners) to forward execution requests to it.

## CacheLookupScheduler

A scheduler wrapper that checks the Action Cache before dispatching. If the AC already has a result for the action, it returns immediately without involving a worker.

```json5
schedulers: [
  {
    name: "CACHED_SCHEDULER",
    cache_lookup: {
      ac_store: "AC_STORE",
      scheduler: {
        simple: {
          supported_platform_properties: { /* ... */ }
        }
      }
    }
  }
]
```

This is useful when clients don't check the AC themselves (or when you want server-side deduplication of in-flight identical actions). Most build systems check the AC client-side, so this is mainly relevant for custom clients or pipelines.

## PropertyModifierScheduler

A wrapper that mutates platform properties before forwarding to a nested scheduler. Useful for:
- Adding default properties that clients don't set
- Removing properties that are irrelevant to worker matching
- Replacing values (e.g., normalizing `container-image` to a resolved digest)

```json5
schedulers: [
  {
    name: "MODIFIED_SCHEDULER",
    property_modifier: {
      modifications: [
        { add: { name: "ISA", value: "x86-64" } },
        { remove: "internal-only-property" },
        { replace: { name: "container-image", value: "docker://toolchain@sha256:abc123" } }
      ],
      scheduler: {
        simple: { /* ... */ }
      }
    }
  }
]
```

**Source:** [`nativelink-scheduler/src/property_modifier_scheduler.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-scheduler/src/property_modifier_scheduler.rs)

## Scheduler Composition

Like stores, schedulers compose. A typical production setup:

```
CacheLookupScheduler
  └── PropertyModifierScheduler
        └── SimpleScheduler (with Redis backend)
```

This gives you: AC dedup at the top, property normalization in the middle, and persistent multi-instance scheduling at the bottom.

## Timeouts and Failure

The scheduler handles several failure modes:

- **Worker disconnect:** If a worker disconnects while executing an action, the action is re-queued and dispatched to another worker.
- **Action timeout:** Actions have a timeout (from the client's `Action.timeout` field or a server-side default). If exceeded, the action fails with DEADLINE_EXCEEDED.
- **Worker timeout:** If a worker doesn't heartbeat within the configured interval, it's removed from the pool and its actions are re-queued.

```json5
simple: {
  supported_platform_properties: { /* ... */ },
  worker_timeout_s: 30,        // remove silent workers after 30s
  client_action_timeout_s: 600 // default action timeout: 10 minutes
}
```
