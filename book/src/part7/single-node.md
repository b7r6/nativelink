# Single Node

The simplest NativeLink deployment: one binary, one config file, all four roles (CAS, AC, scheduler, worker) in one process. This is how you develop, how you test, and how you run CI without external dependencies.

## The Config

```json5
// nativelink-config.json5 — single-node, all-in-one
{
  stores: [
    {
      name: "AC_STORE",
      filesystem: {
        content_path: "/tmp/nativelink/ac/content",
        temp_path: "/tmp/nativelink/ac/tmp",
        eviction_policy: { max_bytes: "512mb" }
      }
    },
    {
      name: "CAS_STORE",
      filesystem: {
        content_path: "/tmp/nativelink/cas/content",
        temp_path: "/tmp/nativelink/cas/tmp",
        eviction_policy: { max_bytes: "5gb" }
      }
    }
  ],

  schedulers: [{
    name: "MAIN_SCHEDULER",
    simple: {
      supported_platform_properties: {
        cpu_count: "minimum",
        OSFamily: "priority",
        "container-image": "priority"
      }
    }
  }],

  workers: [{
    local: {
      worker_api_endpoint: { uri: "grpc://127.0.0.1:50061" },
      cas_fast_slow_store: "CAS_STORE",
      upload_action_result: { ac_store: "AC_STORE" },
      work_directory: "/tmp/nativelink/work",
      platform_properties: {
        cpu_count: { query_cmd: "nproc" },
        OSFamily: { values: ["Linux"] },
        "container-image": { values: [""] }
      }
    }
  }],

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
      listener: { http: { socket_address: "127.0.0.1:50061" } },
      services: {
        worker_api: { scheduler: "MAIN_SCHEDULER" },
        health: {}
      }
    }
  ]
}
```

## Running It

### From Source (Cargo)

```bash
cargo run --release --bin nativelink -- nativelink-config.json5
```

### From Nix

```bash
nix run github:TraceMachina/nativelink -- nativelink-config.json5
```

### From Docker

```bash
docker run -v $(pwd)/nativelink-config.json5:/config.json5 \
  -p 50051:50051 \
  ghcr.io/tracemachina/nativelink:latest /config.json5
```

### Verify It's Running

```bash
# Health check
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check

# Or with curl (NativeLink serves HTTP health on the same port)
curl http://localhost:50051/status
```

## Connecting Clients

### Bazel

```bash
# .bazelrc
build --remote_cache=grpc://127.0.0.1:50051
build --remote_executor=grpc://127.0.0.1:50051
build --remote_instance_name=main
build --remote_default_exec_properties=cpu_count=1
```

### Buck2

```ini
# .buckconfig
[buck2_re_client]
engine_address = 127.0.0.1:50051
action_cache_address = 127.0.0.1:50051
cas_address = 127.0.0.1:50051
tls = false
instance_name = main
```

## When to Use This

Single-node is right for:
- **Local development** — test remote execution behavior without a remote server
- **CI runners** — each CI job runs its own NativeLink instance, caching within the job
- **Small teams** — if everyone is on the same machine (or cache sharing isn't needed)
- **Testing NativeLink itself** — the integration tests use this pattern

Single-node is wrong for:
- **Cross-machine cache sharing** — you need a durable, shared backend (S3, etc.)
- **High throughput** — one worker saturates one machine's CPUs
- **Reliability** — no redundancy, no failover

## Upgrading to Shared Cache

The minimal change from single-node to shared cache: replace the filesystem CAS with cloud storage:

```json5
// Replace this:
{ name: "CAS_STORE", filesystem: { ... } }

// With this:
{
  name: "CAS_STORE",
  fast_slow: {
    fast: {
      filesystem: {
        content_path: "/tmp/nativelink/cas/content",
        temp_path: "/tmp/nativelink/cas/tmp",
        eviction_policy: { max_bytes: "5gb" }
      }
    },
    slow: {
      experimental_cloud_object_store: {
        region: "us-east-1",
        bucket: "my-team-nativelink-cas",
        key_prefix: "cas/"
      }
    }
  }
}
```

Now multiple machines can share the CAS (each with their own local fast tier). This is the bridge between single-node and multi-worker — you get cache sharing without a distributed scheduler.
