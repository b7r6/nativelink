# Multi-Team Production Config

A production configuration for an organization with multiple teams, multiple languages, and different performance requirements. This is not a tutorial — it is a reference architecture, and every configuration block on this page passes `nativelink --check` (see [Validating this config](#validating-this-config)).

The spine of the design is one shared, content-addressed CAS. Once that substrate exists, this fork lets you build two more things on top of it without standing up new storage:

- The [OCI → CAS bridge](../part9/oci-cas-bridge.md) turns toolchain container images into REAPI `Directory` trees in the same CAS, so workers fetch toolchains as data instead of operators pre-installing them.
- The [Nix substituter facade](../part10/nix-substituter.md) serves the Nix HTTP binary-cache protocol out of NativeLink stores, so developer laptops and CI can substitute Nix closures from the same backend that answers Bazel and Buck2.

## The Scenario

- **3 teams:** Platform (Rust + C++), Backend (Go + Python), Mobile (Swift + Kotlin)
- **2 regions:** US-East, EU-West
- **~50 developers** + CI fleet
- **Requirements:** Shared cache, remote execution, team isolation, observability, hermetic toolchains

## Architecture

```
   ┌──────────────┐         ┌──────────────────┐
   │ OCI registry │────────▶│  Global S3 (CAS) │◀──── Nix substituter node
   │ (toolchains) │  fetch  │  Cross-region    │      (nix_cache facade,
   └──────────────┘         │  replication     │       nix/ key prefix)
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

This is the control-plane node: it serves the client-facing CAS/AC/Execution endpoints, holds scheduler state in Redis, and runs the OCI → CAS bridge. It does not run workers itself.

```json5
{
  stores: [
    // Action Cache: metrics -> completeness checking -> fast/slow tiers.
    {
      name: "AC_STORE",
      cache_metrics: {
        cache_type: "ac",
        backend: {
          completeness_checking: {
            backend: {
              fast_slow: {
                fast: {
                  memory: { eviction_policy: { max_bytes: "2gb" } }
                },
                slow: {
                  experimental_cloud_object_store: {
                    provider: "aws",
                    region: "us-east-1",
                    bucket: "acme-nativelink-prod",
                    key_prefix: "ac/"
                  }
                }
              }
            },
            // completeness_checking.cas_store is a StoreSpec, not a bare
            // name: wrap the shared CAS in a ref_store so it points at
            // CAS_STORE below.
            cas_store: {
              ref_store: { name: "CAS_STORE" }
            }
          }
        }
      }
    },

    // CAS: metrics -> existence cache -> compression -> fast/slow tiers.
    {
      name: "CAS_STORE",
      cache_metrics: {
        cache_type: "cas",
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
                        provider: "aws",
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
    },

    // Redis store holding scheduler state. The scheduler backend references
    // this store by name; addresses and key_prefix live here, on the store.
    {
      name: "SCHEDULER_REDIS",
      redis_store: {
        addresses: ["redis://redis-primary:6379"],
        key_prefix: "sched-us-east:"
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
            worker_timeout_s: 30,
            // Re-queue an action stuck Executing this long with no worker
            // update (a live worker stalled on one action).
            max_action_executing_timeout_s: 1200,
            experimental_backend: {
              redis: { redis_store: "SCHEDULER_REDIS" }
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
        ],
        // OCI -> CAS bridge. A FetchDirectory on an oci:// (or docker://)
        // URI pulls the image, projects its layers into a Directory tree,
        // and uploads the blobs into CAS_STORE. digest_function is SHA256
        // to match this cluster's default, so imported toolchains are
        // addressable by the same actions.
        fetch: [{
          instance_name: "main",
          fetch_store: "CAS_STORE",
          oci: {
            cas_store: "CAS_STORE",
            dedup_check: true,
            digest_function: "SHA256"
          }
        }]
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
    default_digest_hash_function: "sha256",
    // Reject requests whose digest function is unset (0/UNKNOWN) rather than
    // silently defaulting to SHA256. This catches a BLAKE3 client that omits
    // the field before it corrupts output Directory trees. Modern Bazel and
    // Buck2 always set the field, so this is safe to require here.
    require_explicit_digest_function: true
  }
}
```

The OCI bridge on this node is deliberately minimal. The current client pulls only public, single-platform images with gzip layers; see [The OCI → CAS Bridge](../part9/oci-cas-bridge.md) for the exact limits before you point `--extra_toolchains` at a private registry.

## Worker Config: Compile Pool

Workers are separate processes on separate machines. Each one owns a local filesystem `fast` tier and shares the same S3 `slow` tier as the scheduler, so a blob a worker writes is immediately readable through the scheduler's CAS.

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
          provider: "aws",
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
      // Durations are integer seconds (the *_s field), not {secs, nanos}.
      max_action_timeout_s: 1200,
      platform_properties: {
        cpu_count: { query_cmd: "nproc" },
        // query_cmd runs the command directly (parsed with shlex, then
        // exec'd — NOT a shell). A shell expression must be wrapped in
        // `sh -c "..."`, or command substitution and pipes are passed to
        // the program as literal argv strings.
        memory_gb: { query_cmd: "sh -c \"echo $(($(free -b | awk '/Mem:/{print $2}') / 1073741824))\"" },
        OSFamily: { values: ["Linux"] },
        ISA: { values: ["x86-64"] },
        pool: { values: ["compile"] },
        "container-image": { values: [""] },
        "toolchain-hash": { values: ["lre-2024.1-abc123"] }
      },
      additional_environment: {
        // Property(name) is a newtype variant: { property: "..." }.
        CONTAINER_IMAGE: { property: "container-image" },
        // The rest are unit variants, so they are bare strings, not maps.
        ACTION_DIRECTORY: "action_directory",
        TIMEOUT_MS: "timeout_millis",
        SIDE_CHANNEL: "side_channel_file"
      },
      experimental_precondition_script: "/opt/nativelink/check-disk.sh"
    }
  }],

  servers: []
}
```

Why `sh -c` matters: the worker splits `query_cmd` with `shlex` and `exec`s the first token as a program, feeding the rest as arguments (`worker_utils.rs:50-61`). Handed the bare `echo $(( ... ))`, it runs `echo` with `$(($(free`, `-b`, `|`, ... as literal arguments and reports that string as the property value. Wrapping in `sh -c` gives you a real shell to evaluate the substitution.

## Worker Config: Test Pool

Same shape as the compile pool, but tuned for memory-heavy integration tests:
- Higher memory allocation, lower CPU count
- Different `pool` name so the scheduler routes test actions here
- Network access for integration tests (no `--network=none` in the entrypoint)

```json5
// platform_properties fragment — the rest of the worker config is identical
// to the compile pool above.
platform_properties: {
  cpu_count: { values: ["4"] },
  memory_gb: { values: ["64"] },
  pool: { values: ["test"] },
  // ...
}
```

## Nix Substituter Node

The same S3 backend can serve Nix closures. This is a separate process on its own listener — Nix clients authenticate with tokens or `netrc`, not the mTLS client certificates the gRPC `public` server requires, so keeping it distinct is the honest deployment. Its NAR blobs land in the same bucket under a `nix/` prefix, sharing storage with the REAPI CAS without colliding.

```json5
{
  stores: [
    // NAR blobs: digest-keyed by DigestInfo(sha256(nar), nar_size). Wrapped
    // in verify so corrupt or truncated NAR uploads are rejected at write
    // time instead of being served back.
    {
      name: "NIX_NAR_STORE",
      verify: {
        verify_size: true,
        verify_hash: true,
        backend: {
          fast_slow: {
            fast: {
              filesystem: {
                content_path: "/data/nix/nar/content",
                temp_path: "/data/nix/nar/tmp",
                eviction_policy: { max_bytes: "20gb" }
              }
            },
            slow: {
              experimental_cloud_object_store: {
                provider: "aws",
                region: "us-east-1",
                bucket: "acme-nativelink-prod",
                key_prefix: "nix/"
              }
            }
          }
        }
      }
    },

    // Path-info records: string-keyed by the nixbase32 store-path hash.
    // completeness_checking makes a narinfo whose NAR was evicted return
    // 404 instead of advertising an unservable NAR.
    {
      name: "NIX_PATH_INFO_STORE",
      completeness_checking: {
        backend: {
          memory: { eviction_policy: { max_bytes: "256mb" } }
        },
        cas_store: {
          ref_store: { name: "NIX_NAR_STORE" }
        }
      }
    },

    // Alias records: client-chosen NAR URL names -> (digest, size).
    { name: "NIX_ALIAS_STORE", memory: { eviction_policy: { max_bytes: "128mb" } } }
  ],

  servers: [
    {
      name: "nix_substituter",
      listener: {
        http: { socket_address: "0.0.0.0:50071" }
      },
      services: {
        nix_cache: [{
          instance_name: "main",
          cas_store: "NIX_NAR_STORE",
          path_info_store: "NIX_PATH_INFO_STORE",
          alias_store: "NIX_ALIAS_STORE",
          store_dir: "/nix/store",
          // Lower sorts earlier among a client's substituters
          // (cache.nixos.org is 40).
          priority: 30,
          want_mass_query: true,
          read_only: false
        }],
        health: {}
      }
    }
  ],

  global: {
    max_open_files: 65536,
    default_digest_hash_function: "sha256"
  }
}
```

Developers add it to their Nix configuration alongside `cache.nixos.org`:

```ini
# /etc/nix/nix.conf
substituters = https://nix.acme.internal:50071/nix/main https://cache.nixos.org
trusted-public-keys = nix.acme.internal:<base64 ed25519 public key> cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY=
```

The store composition rules are load-bearing: `path_info_store` must not sit behind `existence_cache`, `verify`, or `size_partitioning` (all reject or rewrite its string keys), and `alias_store` must not share the `completeness_checking` wrapper. See [The Nix Substituter Facade](../part10/nix-substituter.md) for the full protocol, signing keys, and compression handling.

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

The `cache_metrics` wrapper on `AC_STORE` and `CAS_STORE` (with `cache_type` labels `ac` and `cas`) is what emits the per-store cache-operation metrics; stores that are not wrapped pay no timing cost and publish nothing. Key dashboards:

- **Cache hit rate** by `cache_type` (ac vs cas)
- **Queue depth** by pool (compile vs test)
- **Worker utilization** (actions in progress / total capacity)
- **Store latency** p50/p99 (memory tier vs S3 tier)
- **Bytes transferred** (upload/download volume)

## Validating this config

Every JSON5 block above deserializes and resolves its store and scheduler references under `nativelink --check`, the offline validation this fork adds. It constructs no stores, binds no sockets, and opens no connections — it only parses the config (with `deny_unknown_fields`, so a misspelled or stale field fails) and confirms every referenced name is declared:

```console
$ nativelink --check scheduler-us-east.json5
OK: scheduler-us-east.json5 — 3 stores, 1 schedulers, 2 servers, all references resolve

$ nativelink --check worker-compile.json5
OK: worker-compile.json5 — 1 stores, 0 schedulers, 0 servers, all references resolve

$ nativelink --check nix-substituter.json5
OK: nix-substituter.json5 — 3 stores, 0 schedulers, 1 servers, all references resolve
```

Run this in CI on every config change. See the [configuration reference](../appendix/config-reference.md) for the full flag list and the `require_explicit_digest_function` semantics.

## Key Design Decisions

1. **CAS in S3, not on scheduler disk.** Workers upload directly to S3. The scheduler is control-plane only.
2. **Redis for scheduler state.** `experimental_backend: { redis: { redis_store: "SCHEDULER_REDIS" } }` references a declared `redis_store`; the addresses and key prefix live on that store. This enables HA (multiple scheduler replicas) and survives scheduler restarts.
3. **`CacheLookupScheduler`.** Server-side AC check before dispatch. Prevents re-execution of already-cached actions.
4. **Pool-based routing.** `exact` match on `pool` sends compile actions to CPU-heavy workers and test actions to memory-heavy workers.
5. **Both instance names.** `"main"` and `""` map to the same underlying stores, so Buck2 (which sets `instance_name = main`) and a default Bazel client both hit the same cache.
6. **Compression on CAS.** LZ4 reduces S3 storage and transfer, and aborts early on incompressible blobs.
7. **Completeness checking on AC.** Prevents cache hits that reference evicted CAS blobs; the wrapper's `cas_store` is a `ref_store` pointing at `CAS_STORE`.
8. **Existence cache on CAS.** Eliminates network round-trips for `FindMissingBlobs` on the hot path.
9. **Strict digest safety.** `require_explicit_digest_function: true` rejects requests that leave the digest function unset instead of guessing SHA256 — the fork's guard against silent cross-hash corruption.
10. **mTLS on the gRPC port.** All build-client connections are authenticated, and the worker API sits on a separate port not exposed to clients.
11. **Toolchains as data.** The OCI → CAS bridge imports toolchain images into the shared CAS, and the Nix substituter serves closures from the same backend — no new storage tier for either.
12. **Precondition script.** Workers self-heal by pausing when disk is low.
```
