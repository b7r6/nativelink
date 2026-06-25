# Multi-Team Production Config

A realistic production configuration for an organization with multiple teams, multiple languages, and different performance requirements. This is not a tutorial — it's a reference architecture.

## The Scenario

- **3 teams:** Platform (Rust + C++), Backend (Go + Python), Mobile (Swift + Kotlin)
- **2 regions:** US-East, EU-West
- **~50 developers** + CI fleet
- **Requirements:** Shared cache, remote execution, team isolation, observability

## Architecture

```
                         ┌──────────────────┐
                         │  Global S3 (CAS) │
                         │  Cross-region    │
                         │  replication     │
                         └────────┬─────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
   ┌──────────▼──────────┐    ┌──▼──────────────┐   │
   │  US-East Cluster    │    │  EU-West Cluster │   │
   │                     │    │                  │   │
   │  Scheduler (HA, 2x) │    │  Scheduler (HA)  │   │
   │  Redis (state)      │    │  Redis (state)   │   │
   │                     │    │                  │   │
   │  Worker Pool:       │    │  Worker Pool:    │   │
   │    compile (8x)     │    │    compile (4x)  │   │
   │    test (12x)       │    │    test (6x)     │   │
   │    mobile (4x)      │    │                  │   │
   └─────────────────────┘    └──────────────────┘   │
                                                     │
                                     ┌───────────────▼──┐
                                     │  CI Runners      │
                                     │  (cache-only,    │
                                     │   no execution)  │
                                     └──────────────────┘
```

## NativeLink Config: Scheduler (US-East)

```json5
{
  stores: [
    // Action Cache with metrics and completeness checking
    {
      name: "AC_STORE",
      cache_metrics: {
        backend: {
          completeness_checking: {
            backend: {
              fast_slow: {
                fast: {
                  memory: { eviction_policy: { max_bytes: "2gb" } }
                },
                slow: {
                  experimental_cloud_object_store: {
                    region: "us-east-1",
                    bucket: "acme-nativelink-prod",
                    key_prefix: "ac/"
                  }
                }
              }
            },
            cas_store: "CAS_STORE"
          }
        }
      }
    },

    // CAS with tiered storage
    {
      name: "CAS_STORE",
      cache_metrics: {
        backend: {
          existence_cache: {
            backend: {
              compression: {
                backend: {
                  fast_slow: {
                    fast: {
                      memory: { eviction_policy: { max_bytes: "8gb" } }
                    },
                    slow: {
                      experimental_cloud_object_store: {
                        region: "us-east-1",
                        bucket: "acme-nativelink-prod",
                        key_prefix: "cas/"
                      }
                    }
                  }
                },
                compression_algorithm: { lz4: {} }
              }
            },
            eviction_policy: { max_count: 5000000 }
          }
        }
      }
    }
  ],

  schedulers: [
    {
      name: "MAIN_SCHEDULER",
      cache_lookup: {
        ac_store: "AC_STORE",
        scheduler: {
          simple: {
            supported_platform_properties: {
              cpu_count: "minimum",
              memory_gb: "minimum",
              OSFamily: "priority",
              "container-image": "priority",
              ISA: "exact",
              pool: "exact",
              "toolchain-hash": "exact"
            },
            experimental_redis_scheduler_state: {
              addresses: ["redis://redis-primary:6379"],
              key_prefix: "sched-us-east:",
              worker_timeout_s: 30,
              action_timeout_s: 1200
            }
          }
        }
      }
    }
  ],

  servers: [
    {
      name: "public",
      listener: {
        http: {
          socket_address: "0.0.0.0:50051",
          tls: {
            cert_file: "/certs/tls.crt",
            key_file: "/certs/tls.key",
            client_ca_file: "/certs/ca.crt"
          }
        }
      },
      services: {
        cas: [
          { instance_name: "main", cas_store: "CAS_STORE" },
          { instance_name: "", cas_store: "CAS_STORE" }
        ],
        ac: [
          { instance_name: "main", ac_store: "AC_STORE" },
          { instance_name: "", ac_store: "AC_STORE" }
        ],
        execution: [{
          instance_name: "main",
          cas_store: "CAS_STORE",
          scheduler: "MAIN_SCHEDULER"
        }],
        capabilities: [{
          instance_name: "main",
          remote_execution: { scheduler: "MAIN_SCHEDULER" }
        }],
        bytestream: [
          { instance_name: "main", cas_store: "CAS_STORE" },
          { instance_name: "", cas_store: "CAS_STORE" }
        ]
      }
    },
    {
      name: "worker_api",
      listener: {
        http: { socket_address: "0.0.0.0:50061" }
      },
      services: {
        worker_api: { scheduler: "MAIN_SCHEDULER" },
        health: {},
        admin: {}
      }
    }
  ],

  global: {
    max_open_files: 65536,
    default_digest_hash_function: "sha256"
  }
}
```

## Worker Config: Compile Pool

```json5
{
  stores: [{
    name: "WORKER_CAS",
    fast_slow: {
      fast: {
        filesystem: {
          content_path: "/data/cas/content",
          temp_path: "/data/cas/tmp",
          eviction_policy: { max_bytes: "100gb" }
        }
      },
      slow: {
        experimental_cloud_object_store: {
          region: "us-east-1",
          bucket: "acme-nativelink-prod",
          key_prefix: "cas/"
        }
      }
    }
  }],

  workers: [{
    local: {
      worker_api_endpoint: { uri: "grpc://scheduler:50061" },
      cas_fast_slow_store: "WORKER_CAS",
      work_directory: "/data/work",
      entrypoint: "/opt/nativelink/entrypoint.sh",
      use_namespaces: true,
      use_mount_namespace: true,
      max_action_timeout: { secs: 1200, nanos: 0 },
      graceful_shutdown_timeout: { secs: 60, nanos: 0 },
      platform_properties: {
        cpu_count: { query_cmd: "nproc" },
        memory_gb: { query_cmd: "echo $(($(free -b | awk '/Mem:/{print $2}') / 1073741824))" },
        OSFamily: { values: ["Linux"] },
        ISA: { values: ["x86-64"] },
        pool: { values: ["compile"] },
        "container-image": { values: [""] },
        "toolchain-hash": { values: ["lre-2024.1-abc123"] }
      },
      additional_environment: {
        CONTAINER_IMAGE: { property: "container-image" },
        ACTION_DIRECTORY: { action_directory: {} },
        TIMEOUT_MS: { timeout_millis: {} },
        SIDE_CHANNEL: { side_channel_file: {} }
      },
      experimental_precondition_script: "/opt/nativelink/check-disk.sh"
    }
  }],

  servers: []
}
```

## Worker Config: Test Pool

Same as compile pool but with:
- Higher memory allocation
- Lower CPU count
- Different pool name
- Network access for integration tests (no `--network=none` in entrypoint)

```json5
platform_properties: {
  cpu_count: { values: ["4"] },
  memory_gb: { values: ["64"] },
  pool: { values: ["test"] },
  // ...
}
```

## Client Configurations

### Platform Team (Bazel + LRE)

```bash
# .bazelrc
try-import %workspace%/lre.bazelrc
build --remote_cache=grpcs://nativelink.acme.internal:50051
build --remote_executor=grpcs://nativelink.acme.internal:50051
build --remote_instance_name=main
build --tls_certificate=/etc/nativelink/ca.crt
build --remote_default_exec_properties=pool=compile
build --remote_default_exec_properties=ISA=x86-64
```

### Backend Team (Bazel + zig-cc)

```bash
# .bazelrc
build --remote_cache=grpcs://nativelink.acme.internal:50051
build --remote_executor=grpcs://nativelink.acme.internal:50051
build --remote_instance_name=main
build --extra_toolchains=@zig_sdk//toolchain:linux_amd64_gnu.2.28
build --remote_default_exec_properties=pool=compile
build --remote_default_exec_properties=toolchain-hash=zig-0.11.0
build --remote_default_exec_properties=ISA=x86-64
```

### Mobile Team (Buck2)

```ini
# .buckconfig
[buck2_re_client]
engine_address = nativelink.acme.internal:50051
action_cache_address = nativelink.acme.internal:50051
cas_address = nativelink.acme.internal:50051
tls = true
instance_name = main
```

### CI Runners (Cache-Only)

```bash
# .bazelrc for CI
build --remote_cache=grpcs://nativelink.acme.internal:50051
build --remote_instance_name=main
# No --remote_executor: CI builds locally, uploads to shared cache
build --remote_upload_local_results=true
```

## Observability Setup

```yaml
# Prometheus scrape config
scrape_configs:
  - job_name: nativelink-scheduler
    static_configs:
      - targets:
          - scheduler-0:9090
          - scheduler-1:9090

  - job_name: nativelink-workers
    kubernetes_sd_configs:
      - role: pod
        selectors:
          - role: pod
            label: app=nativelink-worker
```

Key dashboards:
- **Cache hit rate** by team (use instance name or action metadata)
- **Queue depth** by pool (compile vs test)
- **Worker utilization** (actions in progress / total capacity)
- **Store latency** p50/p99 (memory tier vs S3 tier)
- **Bytes transferred** (upload/download volume)

## Key Design Decisions

1. **CAS in S3, not on scheduler disk.** Workers upload directly to S3. Scheduler is control-plane only.
2. **Redis for scheduler state.** Enables HA (multiple scheduler replicas) and survives scheduler restarts.
3. **CacheLookupScheduler.** Server-side AC check before dispatch. Prevents re-execution of already-cached actions.
4. **Pool-based routing.** `exact` match on `pool` ensures compile actions go to CPU-heavy workers, test actions go to memory-heavy workers.
5. **Both instance names.** `"main"` for Buck2, `""` for Bazel. Same underlying stores.
6. **Compression on CAS.** LZ4 reduces S3 storage and transfer by 50-80%.
7. **Completeness checking on AC.** Prevents cache hits that reference evicted CAS blobs.
8. **Existence cache.** Eliminates network round-trips for `FindMissingBlobs` on the hot path.
9. **mTLS.** All connections authenticated. Worker API on a separate port (not exposed to clients).
10. **Precondition script.** Workers self-heal by pausing when disk is low.
