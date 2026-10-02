# Multi-Worker

When one machine isn't enough, you scale NativeLink horizontally: one scheduler, multiple workers, shared CAS. This is the standard production topology for small-to-medium teams.

## Architecture

```
                    ┌─────────────┐
                    │  Clients    │
                    │  (Bazel,    │
                    │   Buck2)    │
                    └──────┬──────┘
                           │ gRPC (port 50051)
                    ┌──────▼──────┐
                    │  NativeLink │
                    │  Scheduler  │
                    │  + CAS/AC   │
                    └──┬───┬───┬──┘
                       │   │   │  Worker API (port 50061)
              ┌────────┘   │   └────────┐
              ▼            ▼            ▼
         ┌─────────┐ ┌─────────┐ ┌─────────┐
         │ Worker 1│ │ Worker 2│ │ Worker 3│
         └─────────┘ └─────────┘ └─────────┘
```

## Docker Compose Setup

The compose file below is **simplified for the walkthrough**: it co-locates the
CAS and AC with the scheduler in a single container, and runs one horizontally
scaled `worker` service. The tested, production-shaped files — which split the
CAS onto its own server and give each worker a named data volume — live in the
deployment-examples directory and are what you should copy from.

**Source:** [`deployment-examples/docker-compose/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/deployment-examples/docker-compose) (`scheduler-multi-worker.json5`, `worker-shared-cas.json5`, `docker-compose-multi-worker.yml`)

```yaml
# docker-compose.yml (simplified; see the tested files linked above)
services:
  scheduler:
    # Built from this fork's Dockerfile — the OCI hook and nix_cache service
    # are fork additions not present in the upstream image.
    image: nativelink:latest
    command: nativelink /config/scheduler.json5
    ports:
      - "50051:50051"   # Client-facing (CAS, AC, Execution, Fetch)
      - "50061:50061"   # Worker API
    volumes:
      - ./scheduler.json5:/config/scheduler.json5
      - cas-data:/data/cas
      - ac-data:/data/ac

  # One service, scaled horizontally with `--scale worker=N`. Each replica
  # keeps its fast CAS cache and work directory in container-local storage,
  # which is fine: the fast tier is a cache and the work directory is purged
  # on startup. Replicas share nothing, so no named volume is needed here.
  worker:
    image: nativelink:latest
    command: nativelink /config/worker.json5
    volumes:
      - ./worker.json5:/config/worker.json5
    environment:
      SCHEDULER_ENDPOINT: scheduler
    depends_on:
      - scheduler

volumes:
  cas-data:
  ac-data:
```

Both config files are validated offline before anything binds a socket — see
[Validating Before You Deploy](#validating-before-you-deploy) below.

## Scheduler Config (Multi-Worker)

```json5
// scheduler.json5
{
  stores: [
    {
      name: "AC_STORE",
      filesystem: {
        content_path: "/data/ac/content",
        temp_path: "/data/ac/tmp",
        eviction_policy: { max_bytes: "2gb" }
      }
    },
    {
      name: "CAS_STORE",
      filesystem: {
        content_path: "/data/cas/content",
        temp_path: "/data/cas/tmp",
        eviction_policy: { max_bytes: "50gb" }
      }
    }
  ],

  schedulers: [{
    name: "MAIN_SCHEDULER",
    simple: {
      supported_platform_properties: {
        cpu_count: "minimum",
        OSFamily: "priority",
        "container-image": "priority",
        ISA: "exact"
      }
    }
  }],

  // No workers here — they run in separate containers

  servers: [
    {
      name: "public",
      listener: { http: { socket_address: "0.0.0.0:50051" } },
      services: {
        cas: [{ instance_name: "main", cas_store: "CAS_STORE" }],
        ac: [{ instance_name: "main", ac_store: "AC_STORE" }],
        execution: [{
          instance_name: "main",
          cas_store: "CAS_STORE",
          scheduler: "MAIN_SCHEDULER"
        }],
        capabilities: [{
          instance_name: "main",
          remote_execution: { scheduler: "MAIN_SCHEDULER" }
        }],
        bytestream: [{ instance_name: "main", cas_store: "CAS_STORE" }],

        // OCI → CAS bridge: a FetchDirectory on an oci:// or docker:// URI
        // projects the image into CAS_STORE as an REAPI Directory tree, so
        // toolchains become data the workers fetch on demand.
        fetch: [{
          instance_name: "main",
          fetch_store: "CAS_STORE",
          oci: {
            cas_store: "CAS_STORE",
            dedup_check: true,
            digest_function: "BLAKE3"
          }
        }]
      }
    },
    {
      name: "worker_api",
      listener: { http: { socket_address: "0.0.0.0:50061" } },
      services: {
        worker_api: { scheduler: "MAIN_SCHEDULER" },
        admin: {},
        health: {}
      }
    }
  ]
}
```

The `fetch` block wires the [OCI → CAS bridge](../part9/oci-cas-bridge.md): a
`FetchDirectory` request whose URI begins with `oci://` or `docker://` pulls the
image, projects its layers into a `Directory` tree, uploads the blobs to
`CAS_STORE`, and returns the root digest for a client to fold into an action's
input root. The bridge runs only on the scheduler/CAS process; workers then read
the projected tree from CAS like any other input, so no worker change is needed.
The projection `digest_function` is fixed by this config (the request's own field
is ignored) and **must match what your execution clients request** — so a
`BLAKE3` hook pairs with `BLAKE3` clients. Drop the `fetch` block if you do not
import toolchains this way; everything else in the deployment is unaffected.

## Worker Config (Multi-Worker)

```json5
// worker.json5
{
  stores: [
    {
      name: "WORKER_CAS",
      fast_slow: {
        fast: {
          filesystem: {
            content_path: "/data/cas/content",
            temp_path: "/data/cas/tmp",
            eviction_policy: { max_bytes: "20gb" }
          }
        },
        slow: {
          grpc: {
            instance_name: "main",
            store_type: "cas",   // required: routes to the CAS RPCs
            endpoints: [{
              address: "grpc://${SCHEDULER_ENDPOINT:-scheduler}:50051"
            }]
          }
        }
      }
    },
    {
      // The worker writes execution results here so identical actions hit the
      // cache instead of re-executing. Without this store and the
      // upload_action_result block below, the AC is never populated.
      name: "WORKER_AC",
      grpc: {
        instance_name: "main",
        store_type: "ac",        // required: routes to the Action Cache RPCs
        endpoints: [{
          address: "grpc://${SCHEDULER_ENDPOINT:-scheduler}:50051"
        }]
      }
    }
  ],

  workers: [{
    local: {
      worker_api_endpoint: {
        uri: "grpc://${SCHEDULER_ENDPOINT:-scheduler}:50061"
      },
      cas_fast_slow_store: "WORKER_CAS",
      upload_action_result: {
        ac_store: "WORKER_AC"    // publish results to the scheduler's AC
      },
      work_directory: "/data/work",
      platform_properties: {
        cpu_count: { query_cmd: "nproc" },
        OSFamily: { values: ["Linux"] },
        "container-image": { values: [""] },
        ISA: { values: ["x86-64"] }
      }
    }
  }],

  servers: []  // Workers don't serve client RPCs
}
```

Key points:
- Workers connect to the scheduler's worker API endpoint (port 50061)
- Workers have their own local CAS (fast) that proxies to the scheduler's CAS (slow) via gRPC. The `grpc` store's `store_type` is required — `"cas"` for the slow CAS tier, `"ac"` for the result-upload store — and picks which upstream RPCs the proxy calls
- `upload_action_result.ac_store` is what makes remote caching pay off: the worker writes each successful `ActionResult` back to the scheduler's Action Cache, so the next identical action is a cache hit instead of a re-execution. Omit it and workers execute every action from scratch. The default `upload_ac_results_strategy` is `success_only`
- Workers don't expose any server ports themselves
- `${SCHEDULER_ENDPOINT}` is expanded from environment at startup

## Validating Before You Deploy

With `deny_unknown_fields` on every config struct, a stale or misspelled field —
a `grpc` store missing its `store_type`, an `ac_store` pointing at a name no
store declares — is a hard load error, not a silent no-op. Catch these before a
container starts by running the offline check on each file:

```bash
nativelink --check scheduler.json5
nativelink --check worker.json5
```

`--check` parses the config and resolves every store and scheduler reference,
then exits **without** binding a socket, connecting to a backend, or creating a
store directory. Each file passes with a one-line summary:

```
OK: scheduler.json5 — 2 stores, 1 schedulers, 2 servers, all references resolve
OK: worker.json5 — 2 stores, 0 schedulers, 0 servers, all references resolve
```

A bad reference names the exact offending path (`workers[0].local.upload_action_result.ac_store`
and the like) and exits non-zero, so this is a clean continuous-integration gate:
fail the build before the config ever reaches a node. See [Appendix A: Configuration Reference](../appendix/config-reference.md) for the field-name cautions that most often trip `--check`.

## Scaling Workers

Because the compose file above defines a single `worker` service, scale it
horizontally with one flag — each replica registers itself with the scheduler
on startup:

```bash
docker compose up -d --scale worker=5
```

For heterogeneous pools, define one service per pool instead, each pointing at
its own worker config whose `platform_properties` differ so the scheduler routes
work to the right machines. `deploy.replicas` sets each pool's size:

```yaml
# docker-compose.yml
services:
  worker-compile:
    # 16-CPU machines for compilation
    image: nativelink:latest
    command: nativelink /config/worker-compile.json5
    volumes:
      - ./worker-compile.json5:/config/worker-compile.json5
    environment:
      SCHEDULER_ENDPOINT: scheduler
    depends_on:
      - scheduler
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: "16"
          memory: 32G

  worker-test:
    # Machines with more memory for test suites
    image: nativelink:latest
    command: nativelink /config/worker-test.json5
    volumes:
      - ./worker-test.json5:/config/worker-test.json5
    environment:
      SCHEDULER_ENDPOINT: scheduler
    depends_on:
      - scheduler
    deploy:
      replicas: 5
      resources:
        limits:
          cpus: "4"
          memory: 64G
```

Each pool's `worker-*.json5` is the worker config from above with its
`platform_properties` adjusted — for example a `pool: { values: ["compile"] }`
property that the scheduler advertises and clients request.

## Shared CAS Pattern

In the Docker Compose setup above, CAS lives on the scheduler's filesystem. Workers access it via gRPC (the `grpc` store type in the worker's slow tier). This works but has limitations:
- All CAS traffic flows through the scheduler process
- Scheduler becomes a network bottleneck for large artifacts
- Scheduler disk is a single point of failure

For production, replace the scheduler's filesystem CAS with cloud storage (S3) and give workers direct access:

```json5
// Both scheduler and worker configs reference the same S3 bucket:
{
  name: "SHARED_CAS",
  fast_slow: {
    fast: {
      filesystem: {
        content_path: "/data/cas/content",
        temp_path: "/data/cas/tmp",
        eviction_policy: { max_bytes: "20gb" }
      }
    },
    slow: {
      experimental_cloud_object_store: {
        provider: "aws",   // required tag; also gcs | azure | r2 | oci | ontap
        region: "us-east-1",
        bucket: "my-nativelink-cas",
        key_prefix: "cas/"
      }
    }
  }
}
```

`experimental_cloud_object_store` is a `provider`-tagged enum; the `provider`
field is what selects the backend and its shape (`aws` takes `region` + `bucket`,
`gcs` takes `bucket`, `azure` takes `account_name` + `container`). Omitting it is
a hard parse error.

Now workers upload directly to S3 (bypassing the scheduler for data plane traffic). The scheduler only handles control plane (action dispatch, result reporting).

## When to Use Multi-Worker

- **5-50 developers** sharing a cache with occasional remote execution
- **CI with parallelism** — distribute test suites across multiple workers
- **Mixed workloads** — compile actions need CPU, test actions need memory, route to different pools
- **Budget-constrained** — Docker Compose on a few VMs, no Kubernetes required

Multi-worker is wrong when:
- You need autoscaling (use Kubernetes instead)
- You need HA (the scheduler is a single point of failure without Redis backend)
- You're at 100+ developers (need sharded CAS, multiple scheduler instances)
