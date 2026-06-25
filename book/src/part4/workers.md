# Workers

Workers are the execution engines. They receive dispatched actions from the scheduler, fetch inputs from CAS, run commands, upload outputs, and report results. They are stateless and disposable — scale them up, kill them, replace them.

## Lifecycle of an Action

When a worker receives a dispatched action:

1. **Fetch inputs.** Download the input directory tree from CAS. NativeLink uses hardlinks from a local CAS cache when possible (zero-copy).
2. **Prepare sandbox.** Create the working directory with the input tree, set up namespace isolation if configured.
3. **Run command.** Execute the action's command via the configured entrypoint, with injected environment variables.
4. **Capture outputs.** Read declared output files and directories from the sandbox.
5. **Upload outputs.** Upload output blobs to CAS. Compute digests for the `ActionResult`.
6. **Report result.** Send `ActionResult` (output digests, exit code, stdout/stderr digests) back to the scheduler.
7. **Cleanup.** Tear down the sandbox. Ready for next action.

**Source:** [`nativelink-worker/src/running_actions_manager.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-worker/src/running_actions_manager.rs)

## Configuration

```json5
workers: [{
  local: {
    worker_api_endpoint: {
      uri: "grpc://${SCHEDULER_ENDPOINT:-127.0.0.1}:50061"
    },
    cas_fast_slow_store: "WORKER_FAST_SLOW_STORE",
    work_directory: "/data/work",
    platform_properties: {
      cpu_count: { query_cmd: "nproc" },
      OSFamily: { values: ["Linux"] },
      ISA: { values: ["x86-64"] },
      "container-image": { values: [""] }
    },
    entrypoint: "/usr/local/bin/worker-entrypoint.sh",
    use_namespaces: true,
    use_mount_namespace: true,
    max_action_timeout: { secs: 1200, nanos: 0 },
    graceful_shutdown_timeout: { secs: 30, nanos: 0 }
  }
}]
```

**Source:** [`nativelink-config/src/cas_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/cas_server.rs) — `LocalWorkerConfig`

## The Worker API

Workers connect to the scheduler via an internal gRPC API (not part of REAPI):

**Source:** [`nativelink-service/src/worker_api_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-service/src/worker_api_server.rs)

- `ConnectWorker` — register with the scheduler, declare capabilities
- `GoingAway` — graceful shutdown notification
- `KeepAlive` — heartbeat to avoid timeout removal
- `ExecutionResponse` — report action completion

The worker API endpoint is configured separately from client-facing services. Workers connect to it; clients never see it.

## The Entrypoint

Every action command is prepended with the configured `entrypoint`. If your action command is `["gcc", "-o", "hello", "hello.c"]` and the entrypoint is `/usr/local/bin/run.sh`, the actual executed command is:

```
/usr/local/bin/run.sh gcc -o hello hello.c
```

The entrypoint receives the action's environment (including injected variables from `additional_environment`) and can:
- Launch the command inside a container (`docker run`)
- Set up cgroups or additional isolation
- Handle timeouts (if `timeout_handled_externally: true`)
- Log execution metadata

A minimal entrypoint that just runs the command:

```bash
#!/bin/bash
exec "$@"
```

A container-based entrypoint:

```bash
#!/bin/bash
IMAGE="${CONTAINER_IMAGE:-ubuntu:22.04}"
exec docker run --rm \
  -v "${ACTION_DIRECTORY}:${ACTION_DIRECTORY}" \
  -w "${ACTION_DIRECTORY}" \
  "${IMAGE}" "$@"
```

## CAS Store for Workers

Workers need fast access to action inputs and a place to stage outputs before uploading. The `cas_fast_slow_store` config points to a store that should be:

- **Fast for reads** (inputs are downloaded from CAS here)
- **Hardlink-capable** (so inputs don't need to be copied into the sandbox)
- **Local** (latency matters — workers fetch many small files per action)

Typical pattern:

```json5
stores: [
  {
    name: "WORKER_FAST_SLOW_STORE",
    fast_slow: {
      fast: {
        filesystem: {
          content_path: "/data/worker-cas/content",
          temp_path: "/data/worker-cas/tmp",
          eviction_policy: { max_bytes: "50gb" }
        }
      },
      slow: {
        ref_store: { name: "CAS_STORE" }  // shared CAS backend
      }
    }
  }
]
```

The filesystem store supports hardlinks — when an action needs input file `/data/work/action-123/src/main.c`, the worker hardlinks it from `/data/worker-cas/content/{digest}` instead of copying. This makes input materialization nearly instant for cached files.

## Platform Properties

Workers advertise their capabilities via `platform_properties`. These can be:

**Static values:**
```json5
ISA: { values: ["x86-64"] },
OSFamily: { values: ["Linux"] }
```

**Dynamic queries (run at startup):**
```json5
cpu_count: { query_cmd: "nproc" },
memory_mb: { query_cmd: "free -m | awk '/Mem:/{print $2}'" }
```

A worker can advertise multiple values for a property. For exact-match properties, having multiple values means the worker matches actions requesting any of those values:

```json5
"supported-lang": { values: ["rust", "cpp", "go"] }
```

## Directory Cache

For actions that share common input subtrees (e.g., the same third-party dependencies across many compilation actions), the worker can cache directory materialization:

```json5
local: {
  // ...
  experimental_directory_cache: {
    max_bytes: "10gb",
    max_directories: 10000
  }
}
```

This avoids re-materializing (hardlinking) identical directory trees for every action. The cache maps directory digests to pre-prepared filesystem trees.

## Precondition Script

The worker can run a health check before accepting new actions:

```json5
experimental_precondition_script: "/usr/local/bin/check-resources.sh"
```

If the script exits non-zero, the worker pauses (stops pulling from the queue) until the next check passes. Use this for:
- Disk space checks (`df /data | awk ...`)
- Load average checks
- External dependency availability (can reach CAS? can reach container registry?)

## Graceful Shutdown

When a worker receives SIGTERM:
1. It sends `GoingAway` to the scheduler (stops receiving new actions)
2. It waits for in-flight actions to complete (up to `graceful_shutdown_timeout`)
3. It exits

This allows Kubernetes rolling updates without action failures.
