# Appendix A: Configuration Reference

NativeLink uses JSON5 configuration. Most string values support shell expansion (`${VAR}` with optional `${VAR:-default}`). Size values accept human-friendly formats (`"10gb"`, `"500mb"`, `"4096"`), and most duration values are plain seconds.

Every struct in the config is annotated `#[serde(deny_unknown_fields)]` (with a handful of documented exceptions), so a misspelled or stale field name is a hard load error rather than a silently-ignored key. Field names below are copied verbatim from the source.

**Source:** [`nativelink-config/src/cas_server.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/cas_server.rs)

## Top-Level Structure

The root object deserializes into `CasConfig` (`cas_server.rs:1350`):

```json5
{
  stores: [NamedConfig<StoreSpec>],         // Named store definitions (required)
  workers: [WorkerConfig],                  // Worker configurations (optional)
  schedulers: [NamedConfig<SchedulerSpec>], // Named scheduler definitions (optional)
  servers: [ServerConfig],                  // Listener + service bindings (required)
  experimental_origin_events: {             // optional; see below
    publisher: { store: "STORE_NAME" },
    max_event_queue_size: 65536             // default 65536 (0 also means 65536)
  },
  global: GlobalConfig                      // Global settings (optional)
}
```

`stores` and `schedulers` are lists of `NamedConfig` (`cas_server.rs:58`): each entry has a `name` plus the flattened spec (for example `{ name: "CAS", memory: {} }`). Service and worker configs reference stores and schedulers by that `name`.

## GlobalConfig

`GlobalConfig` (`cas_server.rs:1299`):

```json5
global: {
  max_open_files: 24576,                    // best-effort fd ceiling; must be > 10. Default: 24576
  default_digest_hash_function: "sha256",   // "sha256" (default) or "blake3"
  default_digest_size_health_check: 1048576,// health-check blob size. Default: 1 MiB
  require_explicit_digest_function: false   // reject requests with an unset (0/UNKNOWN) digest
                                            // function instead of defaulting. Default: false
}
```

`default_digest_hash_function` accepts only `"sha256"` or `"blake3"` (`ConfigDigestHashFunction`, `stores.rs`). When `require_explicit_digest_function` is `false` (the default), a request that omits the digest function falls through to `default_digest_hash_function`; when `true`, such a request is rejected — this guards against a BLAKE3 client that omits the field being hashed as SHA256.

## ServerConfig

`ServerConfig` (`cas_server.rs:919`). The listener is a tagged enum whose only current variant is `http` (`HttpListener`, `cas_server.rs:883`):

```json5
{
  name: "server-name",                      // optional; defaults to the server's index
  listener: {
    http: {
      socket_address: "0.0.0.0:50051",      // "IP:PORT", or ":PORT" for all interfaces
      freebind: false,                      // bind before the address is locally assigned. Default: false
      max_decoding_message_size: "4mb",     // max gRPC decode size. Default: 4 MiB
      compression: {                        // response compression (gzip only)
        send_compression_algorithm: "gzip",         // "none" (default) or "gzip"
        accepted_compression_algorithms: ["gzip"]    // subset of ["none", "gzip"]
      },
      advanced_http: {                      // optional hyper HTTP/2 tuning; all fields optional
        http2_keep_alive_interval: 60,
        experimental_http2_keep_alive_timeout_s: 20,
        experimental_http2_max_frame_size: 16384,
        experimental_http2_max_header_list_size: 16384,
        experimental_http2_max_concurrent_streams: 512,
        experimental_http2_initial_stream_window_size: 1048576,
        experimental_http2_initial_connection_window_size: 1048576,
        experimental_http2_adaptive_window: true,
        experimental_http2_max_pending_accept_reset_streams: 64,
        experimental_http2_max_send_buf_size: 1048576,
        experimental_http2_enable_connect_protocol: false
      },
      tls: {                                // optional; omit to serve plaintext
        cert_file: "/path/to/cert.pem",
        key_file: "/path/to/key.pem",
        client_ca_file: "/path/to/ca.pem",  // optional; enables mTLS
        client_crl_file: "/path/to/crl.pem" // optional; mTLS revocation list
      }
    }
  },
  experimental_identity_header: {           // optional; how the client identity is read
    header_name: "x-identity",              // Default: "x-identity"
    required: false                         // fail requests without the header. Default: false
  },
  services: { /* ServicesConfig — see below */ }
}
```

Notes:

- The compression algorithm enum (`HttpCompressionAlgorithm`) only supports `none` and `gzip`; there is no `zstd` HTTP transport compression.
- TLS has no `client_auth_optional` toggle. Presence of `client_ca_file` is what enables mTLS; `client_crl_file` supplies the revocation list.
- Every field under `advanced_http` is optional and defaults to hyper's own default. The keep-alive timeout field also accepts the alias `experimental_http2_keep_alive_timeout`.

### ServicesConfig

`ServicesConfig` (`cas_server.rs:686`). Each of `cas`, `ac`, `capabilities`, `execution`, `bytestream`, `fetch`, `push` and `nix_cache` is a list keyed by `instance_name`; `worker_api`, `experimental_bep`, `admin` and `health` are single objects.

```json5
services: {
  cas: [{
    instance_name: "main",
    cas_store: "STORE_NAME"
  }],
  ac: [{
    instance_name: "main",
    ac_store: "STORE_NAME",
    read_only: false                   // if true, the AC is read-only. Default: false
  }],
  execution: [{
    instance_name: "main",
    cas_store: "STORE_NAME",           // must resolve to a CAS store
    scheduler: "SCHEDULER_NAME"
  }],
  capabilities: [{
    instance_name: "main",
    remote_execution: { scheduler: "SCHEDULER_NAME" } // omit to advertise "no remote execution"
  }],
  bytestream: [{
    instance_name: "main",
    cas_store: "STORE_NAME",
    max_bytes_per_stream: "64kb",              // per-chunk cap. Default: 64 KiB
    persist_stream_on_disconnect_timeout_s: 10 // hold an interrupted upload open. Default: 10s
  }],
  fetch: [{                            // Remote Asset API (Fetch)
    instance_name: "main",
    fetch_store: "STORE_NAME",
    oci: { /* OciFetchConfig — see below; optional */ }
  }],
  push: [{                             // Remote Asset API (Push)
    instance_name: "main",
    push_store: "STORE_NAME",
    read_only: false                   // Default: false
  }],
  nix_cache: [{                        // Nix binary-cache (fork); see NixCacheConfig below
    instance_name: "main",
    cas_store: "NIX_NAR_STORE",
    path_info_store: "NIX_PATH_INFO_STORE",
    alias_store: "NIX_ALIAS_STORE"
  }],
  worker_api: {                        // single object; serve on a separate, non-public port
    scheduler: "SCHEDULER_NAME"
  },
  experimental_bep: {                  // Build Event Protocol; single object
    store: "STORE_NAME"
  },
  admin: { path: "/admin" },           // Default path: "/admin"
  health: { path: "/status", timeout_seconds: 5 } // Default path: "/status"
}
```

Field-name cautions (all of these were wrong in earlier drafts and fail under `deny_unknown_fields`):

- `fetch` uses `fetch_store`, not `cas_store`.
- `push` uses `push_store`, not `cas_store`.
- The Build Event Protocol service key is `experimental_bep` (a single object with a `store` field), not `bep` and not a list.
- `bytestream` uses `persist_stream_on_disconnect_timeout_s` (seconds, alias `persist_stream_on_disconnect_timeout`), not a boolean `persist_stream_on_disconnect`.

`worker_api` should be the only service on its listener: workers hold a different permission set than cache/execution clients, so co-hosting them is a security risk (`cas_server.rs:754`).

### OciFetchConfig (fetch → oci)

`OciFetchConfig` (`cas_server.rs:189`). When the `fetch` service has an `oci` block, a `FetchDirectory` request whose URI begins with `oci://` or `docker://` (`fetch_server.rs:192`) pulls the referenced image, projects its layers into an REAPI `Directory` tree, uploads the blobs to CAS, and returns the root `Directory` digest. Without this block, such a request fails with `Unimplemented` ("OCI toolchain support not configured for this instance").

```json5
oci: {
  cas_store: "STORE_NAME",   // where projected blobs + Directory protos are uploaded.
                             //   Optional; defaults to the service's fetch_store.
  dedup_check: true,         // check for existing blobs before uploading. Default: true
  digest_function: "BLAKE3"  // "BLAKE3" (default) or "SHA256"; validated at startup
}
```

Notes:

- `digest_function` is validated case-insensitively at service construction and accepts only `BLAKE3` or `SHA256`/`SHA-256`; any other value is a startup error (`fetch_server.rs:77`).
- The projection digest function comes from this config, not from the request: the incoming request's `digest_function` is ignored for OCI fetches (`fetch_server.rs:250`).

### NixCacheConfig (`nix_cache`)

`NixCacheConfig` (`cas_server.rs:246`) is a fork addition. Each entry mounts a Nix HTTP binary-cache endpoint (`nix-cache-info`, `*.narinfo`, `nar/*`) on the same listener as the gRPC services, so a Nix client can list this deployment in its `substituters`.

```json5
nix_cache: [{
  instance_name: "main",

  // Required stores.
  cas_store: "NIX_NAR_STORE",             // uncompressed NAR blobs, keyed by
                                          //   DigestInfo(sha256(nar), nar_size). Any CAS store works;
                                          //   wrapping in `verify` (size + hash) is strongly advised.
  path_info_store: "NIX_PATH_INFO_STORE", // one record per store path, string-keyed by the 32-char
                                          //   nixbase32 store-path hash. Do NOT wrap in existence_cache,
                                          //   verify, or size_partitioning; completeness_checking is advised.
  alias_store: "NIX_ALIAS_STORE",         // string-keyed map from client NAR URL name to (digest, size).

  // Advertised nix-cache-info fields.
  path: "/nix/main",                      // mount prefix. Default: "/nix/<instance_name>"
  store_dir: "/nix/store",                // must match clients' store dir. Default: "/nix/store"
  priority: 40,                           // lower sorts earlier. Default: 40
  want_mass_query: true,                  // Default: true

  // Signing.
  signing_key_files: [                    // `nix key generate-secret` files; multiple enables rotation.
    "/etc/nix/keys/cache.example.org-1.key"
  ],                                      // Default: [] (unsigned narinfo)

  // Upload spooling + limits.
  spool_path: "/var/spool/nativelink-nix",// staging dir for decompressing NAR uploads.
                                          //   Default: <system temp>/nativelink-nix-spool/<instance_name>
  read_only: false,                       // if true, PUT returns 405. Default: false
  read_token_files: [],                   // one token per file; non-empty makes reads require a read OR write token. Default: []
  write_token_files: [],                  // one token per file. Once ANY token is configured, a PUT requires a WRITE
                                          //   token — a read token alone cannot write (fail closed). Default: []

  // Serving compression.
  serve_compression: "zstd",              // unset/"none" serves uncompressed; "zstd" transcodes. Default: unset
  compression_level: 3,                   // zstd level 1..=22 when serve_compression is "zstd"; `nativelink --check`
                                          //   rejects out-of-range values. Default: 3
  preserve_upload_compression: true,      // store + serve a compressed upload's original bytes verbatim. Default: true

  // Hardening (defaults are deliberately generous).
  max_nar_size_bytes: "32gb",             // reject NARs over this size. Default: 34359738368 (32 GiB)
  max_concurrent_nar_streams: 256,        // concurrent NAR GET bodies (must be >= 1); over-limit gets 503. Default: 256
  max_concurrent_transcodes: 8,           // concurrent zstd transcodes (must be >= 1). Default: 8
  nar_upload_idle_timeout_s: 60,          // abort an idle upload with 408 after this many seconds. Default: 60

  // Read-through (optional): on a local miss, fetch + verify + cache a path from
  // an upstream binary cache. Omit `upstream_caches` to disable. Full example in
  // nativelink-config/examples/nix_cache_readthrough.json5.
  upstream_caches: [{
    url: "https://cache.nixos.org",       // upstream binary-cache base URL.
    trusted_public_keys: [                // MUST be non-empty: a fetched narinfo is cached only when it carries
      "cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="  // a valid signature from one of these keys.
    ]
  }],
  upstream_negative_ttl_s: 60,            // remember a proven-absent path this long; transient upstream failures
                                          //   (5xx/timeout/NAR mismatch) are NOT negative-cached. Default: 60
  upstream_timeout_s: 30                  // per-upstream narinfo-probe timeout, seconds. Default: 30
}]
```

Only `cas_store`, `path_info_store` and `alias_store` are required; every other field defaults as noted. Read-through (`upstream_caches`) is optional — omit it for a purely local cache. `cas_store` may reuse the same content-addressed store as the gRPC CAS, but `path_info_store` and `alias_store` are string-keyed and must be separate stores. See `nativelink-config/examples/basic_cas_with_nix.json5` for a combined deployment and `nix_cache_readthrough.json5` for read-through.

### CasWitnessConfig (`cas_witness`)

`CasWitnessConfig` (`cas_server.rs:604`) is a fork addition and a `services.cas_witness` entry. It runs a TLS-intercepting, caching **forward proxy** for the raw network fetches a build makes (`fetchurl` tarballs, release archives) — the bytes a fixed-output derivation downloads before any store path exists. Unlike the gRPC services and `nix_cache`, it speaks the `CONNECT` proxy protocol and so **owns its entire listener**: put it on a listener of its own, with no other services. See [The CAS Witness](../part9/cas-witness.md).

```json5
cas_witness: {
  // Required stores.
  cas_store: "FETCH_CAS",                 // fetched bodies, keyed by DigestInfo(sha256(body), size). Any CAS
                                          //   works and may be shared with the gRPC CAS; `verify` recommended.
  alias_store: "FETCH_ALIAS",             // string-keyed map: fetched URL -> (digest, size, content-type).
                                          //   A separate string-keyed store (do NOT wrap in verify).

  // TLS-intercepting CA (both files generated together if either is missing).
  ca_cert_file: "/var/lib/nativelink/cas-witness/ca.crt", // clients must trust this (nix: NIX_SSL_CERT_FILE).
  ca_key_file: "/var/lib/nativelink/cas-witness/ca.key",  // can impersonate any host — keep private (0600).

  // Witnessing (optional): sign a DSSE/in-toto attestation for every cached fetch.
  witness_key_file: "/var/lib/nativelink/cas-witness/witness.key", // ed25519 32-byte seed, base64; generated if
                                          //   absent. When set, each cached fetch gets a signed attestation in the
                                          //   CAS plus X-Straylight-Witness{,-Receipt} response headers.
                                          //   Unset = plain caching proxy. Default: unset

  // Limits.
  max_fetch_size_bytes: "2gb",            // bodies over this stream through to the client but are NOT cached. Default: 2 GiB
  fetch_timeout_s: 300                    // origin fetch timeout on a miss, seconds. Default: 300
}
```

`cas_store`, `alias_store`, `ca_cert_file` and `ca_key_file` are required; `witness_key_file` is optional (omit to disable attestations). The proxy is trusted only for **availability**, never integrity — Nix verifies every fixed-output derivation against its declared hash regardless of what the proxy serves. `nativelink-config/examples/cas_witness.json5` is a full runnable example.

## WorkerConfig

`WorkerConfig` is a tagged enum whose only variant is `local` (`LocalWorkerConfig`, `cas_server.rs:1104`). Durations are plain seconds/milliseconds, not `{ secs, nanos }` objects.

```json5
{
  local: {
    name: "worker-1",                         // optional; defaults to the worker's index
    worker_api_endpoint: {
      uri: "grpc://scheduler:50061",
      timeout: 5,                             // optional request timeout in seconds. Default: 5
      tls_config: { /* ClientTlsConfig */ }   // optional
    },
    cas_fast_slow_store: "STORE_NAME",        // must be a FastSlowStore whose fast tier is a FileSystemStore
    upload_action_result: {
      ac_store: "STORE_NAME",                 // optional; omit to disable result uploads
      upload_ac_results_strategy: "success_only", // success_only (default) | never | everything | failures_only
      historical_results_store: "STORE_NAME", // optional; defaults to the parent CAS store
      upload_historical_results_strategy: "failures_only",
      success_message_template: "",           // optional ExecuteResponse.message template
      failure_message_template: ""
    },
    work_directory: "/data/work",             // purged on startup; same filesystem as the fast store
    entrypoint: "/path/to/entrypoint.sh",     // optional command wrapper
    experimental_precondition_script: "/path/to/check.sh", // optional pause gate
    max_action_timeout_s: 1200,               // reject actions requesting more. Default: 1200 (20m)
    max_upload_timeout_s: 600,                // result-upload deadline. Default: 600 (10m)
    max_cleanup_wait_s: 30,                   // cleanup timeout. Default: 30
    max_cleanup_backoff_ms: 500,              // cleanup backoff ceiling. Default: 500
    max_inflight_tasks: 0,                    // 0 = unlimited. Default: 0
    timeout_handled_externally: false,        // Default: false
    platform_properties: {
      key: { values: ["val1", "val2"] },
      key2: { query_cmd: "command" }          // executed (not shell), split on newlines
    },
    additional_environment: {
      VAR1: { property: "property-name" },
      VAR2: { value: "fixed-string" },
      VAR3: "from_environment",
      VAR4: "timeout_millis",
      VAR5: "side_channel_file",
      VAR6: "action_directory"
    },
    use_namespaces: true,                     // Linux only. Default: false
    use_mount_namespace: true,                // Linux only, requires use_namespaces. Default: false
    directory_cache: {                        // optional input-directory cache
      max_entries: 1000,                      // Default: 1000
      max_size_bytes: "10gb",                 // 0 = unlimited. Default: 10 GB
      cache_root: "/data/directory_cache"     // Default: {work_directory}/../directory_cache
    }
  }
}
```

Field-name cautions:

- The action-timeout field is `max_action_timeout_s` (plain seconds, alias `max_action_timeout`), not a `{ secs, nanos }` object.
- The directory cache field is `directory_cache` with `max_entries` / `max_size_bytes` / `cache_root` — not `experimental_directory_cache` with `max_bytes` / `max_directories`.
- There is no `graceful_shutdown_timeout` field.

## StoreSpec Variants

See [The Store Catalog](../part3/store-catalog.md) for detailed usage of each. Config keys are the `snake_case` form of the variant name (`StoreSpec`, `stores.rs:52`).

| Variant | Config Key | Children |
|---------|-----------|----------|
| Memory | `memory` | None |
| Filesystem | `filesystem` | None |
| Cloud Object Store | `experimental_cloud_object_store` | None |
| ONTAP S3 Existence Cache | `ontap_s3_existence_cache` | `backend` (ONTAP S3 store) |
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
| RefStore | `ref_store` | None (references another store by name) |

## SchedulerSpec Variants

See [Appendix B: Scheduler Catalog](./scheduler-catalog.md) for the field-by-field reference for each variant. Config keys are the `snake_case` form of the variant name (`SchedulerSpec`, `schedulers.rs:30`).

| Variant | Config Key | Description |
|---------|-----------|-------------|
| Simple | `simple` | Primary scheduler with property matching |
| gRPC | `grpc` | Forwarding proxy to a remote scheduler |
| CacheLookup | `cache_lookup` | AC check before dispatch |
| PropertyModifier | `property_modifier` | Transform properties before nesting |
| HistoricalResource | `historical_resource` | Apply CPU/memory hints from a JSON file, then delegate to a nested `scheduler` |

## EvictionPolicy

Used by memory, filesystem, and existence_cache stores (`EvictionPolicy`, `stores.rs`):

```json5
eviction_policy: {
  max_bytes: "10gb",       // max total size (0 = unlimited)
  max_count: 1000000,      // max number of entries (0 = unlimited)
  max_seconds: 86400,      // max age in seconds (0 = no TTL)
  evict_bytes: "1gb"       // amount to evict once a limit is hit
}
```

## PropertyType (Scheduler)

`PropertyType` (`schedulers.rs`):

```json5
supported_platform_properties: {
  key: "minimum",   // u64, worker value >= requested
  key: "exact",     // string, must match exactly
  key: "priority",  // informational, no matching restriction
  key: "ignore"     // allowed but not used for matching
}
```

## Configuration Examples

See [`nativelink-config/examples/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/nativelink-config/examples) for working configuration files covering various deployment patterns, including `basic_cas_with_nix.json5` for a combined remote-execution and Nix binary-cache deployment.
