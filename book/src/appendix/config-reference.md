# Appendix A: Configuration Reference

NativeLink uses JSON5 configuration. All string values support shell expansion (`${VAR}` with optional `${VAR:-default}`). Size values accept human-friendly formats (`"10gb"`, `"500mb"`, `"4096"`).

## Top-Level Structure

```json5
{
  stores: [NamedConfig<StoreSpec>],        // Named store definitions
  schedulers: [NamedConfig<SchedulerSpec>], // Named scheduler definitions
  workers: [WorkerConfig],                 // Worker configurations
  servers: [ServerConfig],                 // Listener + service bindings
  global: GlobalConfig                     // Global settings
}
```

**Source:** [`nativelink-config/src/cas_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/cas_server.rs)

## GlobalConfig

```json5
global: {
  max_open_files: 65536,                    // ulimit -n equivalent
  default_digest_hash_function: "sha256",   // or "blake3"
  default_digest_size_health_check: 1048576 // health check blob size
}
```

## ServerConfig

```json5
{
  name: "server-name",
  listener: {
    http: {
      socket_address: "0.0.0.0:50051",
      advanced_http2: {                   // optional HTTP/2 tuning
        max_frame_size: 16384,
        max_header_list_size: 16384,
        max_pending_accept_resets: 64,
        http2_keep_alive_interval: 60,
        http2_keep_alive_timeout: 20,
      },
      tls: {                              // optional TLS
        cert_file: "/path/to/cert.pem",
        key_file: "/path/to/key.pem",
        client_ca_file: "/path/to/ca.pem", // mTLS
        client_auth_optional: false
      },
      compression: {                      // optional response compression
        accepted_compression_algorithms: ["gzip", "zstd"],
        send_compression_algorithm: "zstd"
      }
    }
  },
  services: {
    cas: [{
      instance_name: "main",
      cas_store: "STORE_NAME"
    }],
    ac: [{
      instance_name: "main",
      ac_store: "STORE_NAME"
    }],
    execution: [{
      instance_name: "main",
      cas_store: "STORE_NAME",
      scheduler: "SCHEDULER_NAME"
    }],
    capabilities: [{
      instance_name: "main",
      remote_execution: { scheduler: "SCHEDULER_NAME" }
    }],
    bytestream: [{
      instance_name: "main",
      cas_store: "STORE_NAME",
      max_bytes_per_stream: 0,          // 0 = unlimited
      persist_stream_on_disconnect: false
    }],
    worker_api: {
      scheduler: "SCHEDULER_NAME"
    },
    fetch: [{                            // Remote Asset API (Fetch)
      instance_name: "main",
      cas_store: "STORE_NAME"
    }],
    push: [{                             // Remote Asset API (Push)
      instance_name: "main",
      cas_store: "STORE_NAME"
    }],
    bep: [{                              // Build Event Protocol
      instance_name: "main",
      store: "STORE_NAME"
    }],
    health: {},
    admin: {}
  }
}
```

## WorkerConfig

```json5
{
  local: {
    worker_api_endpoint: { uri: "grpc://scheduler:50061" },
    cas_fast_slow_store: "STORE_NAME",
    upload_action_result: {
      ac_store: "STORE_NAME",
      historical_results_store: "STORE_NAME"  // optional
    },
    work_directory: "/data/work",
    entrypoint: "/path/to/entrypoint.sh",     // optional
    timeout_handled_externally: false,
    platform_properties: {
      key: { values: ["val1", "val2"] },
      key2: { query_cmd: "command" }
    },
    additional_environment: {
      VAR_NAME: { property: "property-name" },
      VAR_NAME: { value: "fixed-string" },
      VAR_NAME: { from_environment: {} },
      VAR_NAME: { timeout_millis: {} },
      VAR_NAME: { side_channel_file: {} },
      VAR_NAME: { action_directory: {} }
    },
    use_namespaces: true,
    use_mount_namespace: true,
    max_action_timeout: { secs: 1200, nanos: 0 },
    graceful_shutdown_timeout: { secs: 30, nanos: 0 },
    experimental_precondition_script: "/path/to/check.sh",
    experimental_directory_cache: {
      max_bytes: "10gb",
      max_directories: 10000
    }
  }
}
```

## StoreSpec Variants

See [The Store Catalog](../part3/store-catalog.md) for detailed usage of each.

| Variant | Config Key | Children |
|---------|-----------|----------|
| Memory | `memory` | None |
| Filesystem | `filesystem` | None |
| Cloud Object Store | `experimental_cloud_object_store` | None |
| Redis | `redis_store` | None |
| MongoDB | `experimental_mongo` | None |
| gRPC | `grpc` | None |
| Noop | `noop` | None |
| FastSlow | `fast_slow` | `fast`, `slow` |
| Compression | `compression` | `backend` |
| Dedup | `dedup` | `index_store`, `content_store` |
| Verify | `verify` | `backend` |
| ExistenceCache | `existence_cache` | `backend` |
| SizePartitioning | `size_partitioning` | `lower_store`, `upper_store` |
| Shard | `shard` | `stores[]` |
| CompletenessChecking | `completeness_checking` | `backend` + `cas_store` ref |
| CacheMetrics | `cache_metrics` | `backend` |
| RefStore | `ref_store` | None (references by name) |

## SchedulerSpec Variants

| Variant | Config Key | Description |
|---------|-----------|-------------|
| Simple | `simple` | Primary scheduler with property matching |
| gRPC | `grpc` | Forwarding proxy to remote scheduler |
| CacheLookup | `cache_lookup` | AC check before dispatch |
| PropertyModifier | `property_modifier` | Transform properties before nesting |

## EvictionPolicy

Used by memory, filesystem, and existence_cache stores:

```json5
eviction_policy: {
  max_bytes: "10gb",       // max total size (0 = unlimited)
  max_count: 1000000,      // max number of entries (0 = unlimited)
  max_seconds: 86400,      // max age in seconds (0 = no TTL)
  evict_bytes: "1gb"       // amount to evict when full
}
```

## PropertyType (Scheduler)

```json5
supported_platform_properties: {
  key: "minimum",   // u64, worker >= requested
  key: "exact",     // string, must match exactly
  key: "priority",  // informational, no matching restriction
  key: "ignore"     // allowed but not used
}
```

## Configuration Examples

See [`nativelink-config/examples/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/nativelink-config/examples) for 15+ working configuration files covering various deployment patterns.
