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

The reference implementation:

**Source:** [`deployment-examples/docker-compose/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/deployment-examples/docker-compose)

```yaml
# docker-compose.yml
services:
  scheduler:
    image: ghcr.io/tracemachina/nativelink:latest
    command: /config/scheduler.json5
    ports:
      - "50051:50051"   # Client-facing (CAS, AC, Execution)
      - "50061:50061"   # Worker API
    volumes:
      - ./scheduler.json5:/config/scheduler.json5
      - cas-data:/data/cas
      - ac-data:/data/ac

  worker-1:
    image: ghcr.io/tracemachina/nativelink:latest
    command: /config/worker.json5
    volumes:
      - ./worker.json5:/config/worker.json5
      - worker1-data:/data
    environment:
      SCHEDULER_ENDPOINT: scheduler

  worker-2:
    image: ghcr.io/tracemachina/nativelink:latest
    command: /config/worker.json5
    volumes:
      - ./worker.json5:/config/worker.json5
      - worker2-data:/data
    environment:
      SCHEDULER_ENDPOINT: scheduler

volumes:
  cas-data:
  ac-data:
  worker1-data:
  worker2-data:
```

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
        bytestream: [{ instance_name: "main", cas_store: "CAS_STORE" }]
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
            endpoints: [{
              address: "grpc://${SCHEDULER_ENDPOINT:-scheduler}:50051"
            }]
          }
        }
      }
    }
  ],

  workers: [{
    local: {
      worker_api_endpoint: {
        uri: "grpc://${SCHEDULER_ENDPOINT:-scheduler}:50061"
      },
      cas_fast_slow_store: "WORKER_CAS",
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
- Workers have their own local CAS (fast) that proxies to the scheduler's CAS (slow) via gRPC
- Workers don't expose any server ports themselves
- `${SCHEDULER_ENDPOINT}` is expanded from environment at startup

## Scaling Workers

Add more workers by scaling the Docker Compose service:

```bash
docker compose up --scale worker=5
```

Or add more worker entries with different platform properties:

```yaml
# docker-compose.yml
services:
  worker-compile:
    # 16-CPU machines for compilation
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: "16"
          memory: 32G

  worker-test:
    # Machines with more memory for test suites
    deploy:
      replicas: 5
      resources:
        limits:
          cpus: "4"
          memory: 64G
```

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
      filesystem: { /* local cache */ }
    },
    slow: {
      experimental_cloud_object_store: {
        region: "us-east-1",
        bucket: "my-nativelink-cas",
        key_prefix: "cas/"
      }
    }
  }
}
```

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
