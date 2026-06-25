# The Store Catalog

Every store type in NativeLink, what it does, and when to use it.

**Source:** [`nativelink-config/src/stores.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-config/src/stores.rs)

## Leaf Stores (Terminal — No Children)

### `memory`

In-process hash map with LRU eviction.

```json5
memory: {
  eviction_policy: {
    max_bytes: "4gb",
    max_count: 0,        // 0 = unlimited count
    max_seconds: 0,      // 0 = no TTL
    evict_bytes: "512mb" // evict this much when full
  }
}
```

**Use when:** Hot cache tier, development, testing. Lost on restart.

### `filesystem`

Files on local disk. Content stored by digest filename. Supports hardlinks for zero-copy access by workers.

```json5
filesystem: {
  content_path: "/data/cas/content",
  temp_path: "/data/cas/tmp",
  eviction_policy: { max_bytes: "100gb" },
  block_size: 4096,
  read_buffer_size: 32768
}
```

**Use when:** Single-node deployments, workers (for local artifact cache), CI runners.

### `experimental_cloud_object_store`

S3, GCS, Azure Blob, R2. See [Cloud Backends](./cloud-backends.md).

### `redis_store`

Redis (or Redis Cluster) as a store backend.

```json5
redis_store: {
  addresses: ["redis://redis-1:6379", "redis://redis-2:6379"],
  key_prefix: "nl:",
  connection_pool_size: 16,
  experimental_pub_sub_channel: "nativelink-events"
}
```

**Use when:** Shared state that needs sub-millisecond access (scheduler state, existence metadata). Not ideal for large blobs (Redis stores everything in memory).

### `experimental_mongo`

MongoDB as a store backend. Stores blobs as GridFS documents.

```json5
experimental_mongo: {
  address: "mongodb://mongo:27017",
  database: "nativelink",
  collection: "cas"
}
```

**Use when:** You already have MongoDB infrastructure and want to avoid adding another storage system.

### `grpc`

Proxies store operations to a remote NativeLink instance (or any REAPI-compatible server).

```json5
grpc: {
  instance_name: "main",
  endpoints: [{ address: "grpc://remote-cas:50051" }],
  connections_per_endpoint: 4
}
```

**Use when:** Distributed deployments where CAS is centralized and other services reference it remotely. Also used to proxy to third-party REAPI servers.

### `noop`

Silently discards all writes. Returns "not found" for all reads.

```json5
noop: {}
```

**Use when:** The "slow" side of a `fast_slow` when you don't want durability. The "backend" of a test configuration.

## Composite Stores (Wrappers — Have Children)

### `fast_slow`

Two-tier cache. See [Store Composition](./store-composition.md).

```json5
fast_slow: {
  fast: { /* store */ },
  slow: { /* store */ }
}
```

### `compression`

Transparent compression/decompression layer.

```json5
compression: {
  backend: { /* store */ },
  compression_algorithm: {
    lz4: { block_size: 65536 }
    // or: zstd: { compression_level: 3 }
  }
}
```

### `dedup`

Content-defined chunking with separate index and content stores.

```json5
dedup: {
  index_store: { /* store for chunk manifests */ },
  content_store: { /* store for chunk data */ },
  min_size: 8192,
  normal_size: 32768,
  max_size: 131072
}
```

### `verify`

Re-computes hash on read to detect corruption.

```json5
verify: {
  backend: { /* store */ },
  verify_size: true,
  verify_hash: true
}
```

### `existence_cache`

Caches `has` results in memory.

```json5
existence_cache: {
  backend: { /* store */ },
  eviction_policy: { max_count: 1000000 }
}
```

### `size_partitioning`

Routes by blob size.

```json5
size_partitioning: {
  size: 1048576,  // threshold in bytes
  lower_store: { /* store for small blobs */ },
  upper_store: { /* store for large blobs */ }
}
```

### `shard`

Distributes across multiple stores by key hash.

```json5
shard: {
  stores: [
    { store: { /* store */ }, weight: 1 },
    { store: { /* store */ }, weight: 1 }
  ]
}
```

### `completeness_checking`

Verifies that all blobs referenced by a directory tree exist in CAS before returning success.

```json5
completeness_checking: {
  backend: { /* store (for AC) */ },
  cas_store: "CAS_STORE_NAME"
}
```

**Use when:** AC store, to ensure cached results reference blobs that still exist in CAS (haven't been evicted).

### `cache_metrics`

Wraps a store and records hit/miss/upload/download metrics.

```json5
cache_metrics: {
  backend: { /* store */ }
}
```

**Use when:** You want per-store observability. Metrics are exposed via the OpenTelemetry exporter.

### `ref_store`

References another named store (avoids deep nesting in config by using indirection).

```json5
ref_store: {
  name: "SOME_OTHER_STORE"
}
```

**Use when:** Multiple services need to share the same physical store without duplicating the config subtree. The reference is resolved after all stores are constructed.

## Composition Cheat Sheet

| I want to... | Use... |
|---|---|
| Cache hot artifacts in memory | `fast_slow: { fast: memory, slow: ... }` |
| Reduce storage costs | `compression: { backend: ... }` |
| Detect corruption | `verify: { backend: ... }` |
| Reduce latency on `has` calls | `existence_cache: { backend: ... }` |
| Save storage for similar artifacts | `dedup: { index_store: ..., content_store: ... }` |
| Route small/large blobs differently | `size_partitioning: { lower_store: ..., upper_store: ... }` |
| Scale storage horizontally | `shard: { stores: [...] }` |
| Ensure AC points to existing CAS data | `completeness_checking: { backend: ..., cas_store: "..." }` |
| Get metrics on store operations | `cache_metrics: { backend: ... }` |
| Reference a shared store | `ref_store: { name: "..." }` |
| Discard data intentionally | `noop: {}` |
