# Cloud Backends

NativeLink supports S3-compatible, GCS, Azure Blob, and R2 storage as CAS/AC backends through a unified cloud object store implementation. This chapter covers when to use which, and the configuration details that matter.

## The Unified Interface

All cloud backends go through one store type: `ExperimentalCloudObjectStore`. Despite the "experimental" prefix (a holdover from the initial implementation), this is production-ready and handles billions of requests.

**Source:** [`nativelink-store/src/experimental_cloud_object_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/experimental_cloud_object_store.rs)

Under the hood, it uses the `object_store` crate (from the Apache Arrow ecosystem) which provides a unified interface across cloud providers.

## AWS S3

```json5
{
  name: "S3_CAS",
  experimental_cloud_object_store: {
    region: "us-east-1",
    bucket: "my-nativelink-cas",
    key_prefix: "cas/",
    retry: { max_retries: 5, delay: 0.1, jitter: 0.5 },
    multipart_max_concurrent_uploads: 10
  }
}
```

Authentication uses the standard AWS credential chain (environment variables, instance profile, ECS task role). No explicit credentials in config.

**Key tuning parameters:**
- `multipart_max_concurrent_uploads` — parallel chunk uploads for large blobs. Higher = faster uploads, more memory.
- `key_prefix` — namespace within the bucket. Use this to separate CAS from AC, or to separate environments.

## Cloudflare R2

R2 is S3-compatible with no egress fees. Configuration is identical to S3 with an endpoint override:

```json5
{
  name: "R2_CAS",
  experimental_cloud_object_store: {
    region: "auto",
    bucket: "my-nativelink-cas",
    key_prefix: "cas/",
    additional_config: {
      "aws_endpoint": "https://ACCOUNT_ID.r2.cloudflarestorage.com"
    }
  }
}
```

R2 is compelling for remote caching because the primary cost driver is egress (downloading cached artifacts), which R2 eliminates. For teams with high cache hit rates downloading large artifacts, the savings are significant.

## Google Cloud Storage (GCS)

```json5
{
  name: "GCS_CAS",
  experimental_cloud_object_store: {
    bucket: "my-nativelink-cas",
    key_prefix: "cas/",
    additional_config: {
      "google_service_account": "/path/to/service-account.json"
    }
  }
}
```

GCS authentication uses service account JSON or workload identity (in GKE). The `additional_config` map passes provider-specific options through to the underlying `object_store` crate.

## Azure Blob Storage

```json5
{
  name: "AZURE_CAS",
  experimental_cloud_object_store: {
    bucket: "my-container",  // Azure calls these "containers"
    key_prefix: "cas/",
    additional_config: {
      "azure_storage_account_name": "mynativelink",
      "azure_storage_access_key": "${AZURE_STORAGE_KEY}"
    }
  }
}
```

Note the shell-expansion syntax `${AZURE_STORAGE_KEY}` — NativeLink expands environment variables in all string config values. Never hardcode secrets.

## When to Use Which

| Backend | Best For | Tradeoff |
|---------|----------|----------|
| **S3** | AWS-native deployments, high durability | Egress costs at scale |
| **R2** | Cost-sensitive, high egress workloads | Slightly higher latency than S3 in-region |
| **GCS** | GCP-native deployments | IAM complexity |
| **Azure Blob** | Azure-native deployments | Performance varies by region |
| **Filesystem** | Single-node, development, CI runners | No durability, no sharing |
| **Memory** | Cache tier only (ephemeral) | Lost on restart |

## The Tiering Pattern

In production, you almost never use a cloud backend directly. You tier it:

```json5
{
  name: "TIERED_CAS",
  existence_cache: {
    backend: {
      fast_slow: {
        fast: {
          memory: {
            eviction_policy: {
              max_bytes: "4gb"
            }
          }
        },
        slow: {
          compression: {
            backend: {
              experimental_cloud_object_store: {
                region: "us-east-1",
                bucket: "my-cas",
                key_prefix: "cas/"
              }
            },
            compression_algorithm: { lz4: {} }
          }
        }
      }
    },
    eviction_policy: { max_count: 1000000 }
  }
}
```

This gives you:
1. `ExistenceCache` — cheap `has` responses without hitting the network
2. `FastSlow(Memory, ...)` — hot artifacts served from RAM
3. `Compression` — reduce storage costs and transfer time
4. `CloudObjectStore` — durable, shared, scalable

The memory tier handles the hot working set (artifacts repeatedly accessed during a single build). The compression layer reduces S3 costs by 50-80% for typical build artifacts. The existence cache eliminates round-trips for `FindMissingBlobs` calls.

## Object Key Layout

NativeLink stores objects with keys derived from the digest:

```
{key_prefix}{hash}-{size}
```

For example: `cas/a1b2c3d4e5f6...789-12345`

This flat namespace works well with cloud object stores (which don't have true directories). The prefix allows multiple logical stores to share a bucket.

## Lifecycle and Eviction

Cloud object stores don't have built-in eviction by access pattern. Options:

1. **S3 Lifecycle Rules** — expire objects older than N days. Crude but effective.
2. **Memory/Filesystem tiers with eviction** — the fast tier handles hot data; cold data in cloud storage persists until lifecycle cleanup.
3. **The Noop pattern** — for CI use cases where you want cache hits within a pipeline but don't need cross-pipeline persistence, use `fast_slow: { fast: filesystem, slow: noop }`. The "slow" store silently discards everything.
