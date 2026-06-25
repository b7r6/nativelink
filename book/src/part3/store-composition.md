# Store Composition

NativeLink's stores compose. You build complex storage topologies by nesting stores inside stores in config. No code changes, no plugins. This is the store algebra.

## The Algebra

Every composite store takes one or more inner stores as children. The children can be any store — including other composites. This gives you a tree:

```
CacheMetrics
  └── Verify
        └── Compression
              └── FastSlow
                    ├── fast: Memory
                    └── slow: ExperimentalCloudObjectStore (S3)
```

This tree says: wrap S3 with a memory cache (fast/slow), compress everything going to S3, verify integrity on read, and collect metrics at the top.

In JSON5 config:

```json5
stores: [
  {
    name: "MY_CAS",
    cache_metrics: {
      backend: {
        verify: {
          backend: {
            compression: {
              backend: {
                fast_slow: {
                  fast: { memory: { eviction_policy: { max_bytes: 1073741824 } } },
                  slow: {
                    experimental_cloud_object_store: {
                      // S3 config...
                    }
                  }
                }
              },
              compression_algorithm: { lz4: {} }
            }
          },
          verify_size: true,
          verify_hash: true
        }
      }
    }
  }
]
```

## FastSlowStore: The Tier Pattern

The most common composition. A fast local cache in front of a slow durable backend.

**Source:** [`nativelink-store/src/fast_slow_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/fast_slow_store.rs)

Semantics:
- **`has`** → checks the **slow** store (source of truth)
- **`get`** → tries fast first; on miss, reads from slow and populates fast concurrently
- **`update`** → writes to **both** fast and slow simultaneously (multiplexed stream)

The get path uses leader/follower deduplication: if multiple concurrent requests miss the fast cache for the same digest, only one request reads from the slow store. The others wait for the leader to finish populating the fast cache, then read from fast.

```rust
// fast_slow_store.rs (simplified)
// Leader streams from slow → fast, followers block on leader completion
let loader = self.populating_digests.lock().entry(key.clone())
    .or_insert_with(|| Loader::new());
```

Configuration options:
- `fast_direction` / `slow_direction` — control whether each side participates in reads, writes, or both
- `bypass_dedup_threshold_bytes` — huge blobs skip the dedup map and read directly from slow

```json5
fast_slow: {
  fast: { memory: { eviction_policy: { max_bytes: "4gb" } } },
  slow: { filesystem: { content_path: "/data/cas", temp_path: "/data/tmp" } },
  fast_direction: "both",
  slow_direction: "both"
}
```

## CompressionStore: Transparent Compression

Wraps any store and compresses content on write, decompresses on read. The caller sees uncompressed bytes; the backend stores compressed bytes.

**Source:** [`nativelink-store/src/compression_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/compression_store.rs)

Supported algorithms: LZ4 (default, fast), ZSTD (better ratio). Compression is chunked — large blobs are split into frames so partial reads don't require decompressing the whole blob.

```json5
compression: {
  backend: { /* inner store */ },
  compression_algorithm: {
    lz4: { block_size: 65536 }
  }
}
```

## DedupStore: Content-Level Deduplication

Splits blobs into content-defined chunks (using FastCDC), stores each chunk separately, and stores an index mapping the original digest to its chunk sequence.

**Source:** [`nativelink-store/src/dedup_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/dedup_store.rs)

Two inner stores:
- `index_store` — maps original digest → list of chunk digests
- `content_store` — stores the actual chunks

If two blobs share byte sequences (e.g., two slightly different binaries), the shared chunks are stored once. This reduces storage significantly for incremental builds where most of the binary doesn't change between versions.

```json5
dedup: {
  index_store: { memory: { eviction_policy: { max_bytes: "100mb" } } },
  content_store: {
    experimental_cloud_object_store: { /* S3 */ }
  },
  min_size: 8192,
  normal_size: 32768,
  max_size: 131072
}
```

## VerifyStore: Trust But Verify

Wraps a store and re-computes the hash on read. If the stored content doesn't match the requested digest, it returns an error instead of corrupt data.

**Source:** [`nativelink-store/src/verify_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/verify_store.rs)

```json5
verify: {
  backend: { /* inner store */ },
  verify_size: true,
  verify_hash: true
}
```

Use this when your backend is not trusted (e.g., shared filesystem, network storage without checksums). The cost is reading the entire blob to compute the hash — do not put this inside a hot read path.

## ExistenceCacheStore: Cheap `has` Calls

Caches the results of `has` calls in memory. Useful when the backend `has` is expensive (e.g., S3 HEAD request with network latency).

**Source:** [`nativelink-store/src/existence_cache_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/existence_cache_store.rs)

```json5
existence_cache: {
  backend: { /* inner store */ },
  eviction_policy: { max_count: 1000000 }
}
```

This is pure optimization — it never changes correctness (a negative `has` result might be stale if content was uploaded by another path, but this only causes a redundant upload, not data loss).

## SizePartitioningStore: Route by Size

Sends small blobs to one store and large blobs to another. Useful for optimizing I/O patterns: small blobs to a low-latency store (memory, Redis), large blobs to a high-throughput store (S3).

**Source:** [`nativelink-store/src/size_partitioning_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/size_partitioning_store.rs)

```json5
size_partitioning: {
  size: 1048576,  // 1MB threshold
  lower_store: { memory: { eviction_policy: { max_bytes: "2gb" } } },
  upper_store: { experimental_cloud_object_store: { /* S3 */ } }
}
```

## ShardStore: Horizontal Scaling

Distributes blobs across multiple stores by hashing the key. Each shard handles a fraction of the keyspace.

**Source:** [`nativelink-store/src/shard_store.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/shard_store.rs)

```json5
shard: {
  stores: [
    { store: { grpc: { endpoint: "grpc://cas-1:50051" } }, weight: 1 },
    { store: { grpc: { endpoint: "grpc://cas-2:50051" } }, weight: 1 },
    { store: { grpc: { endpoint: "grpc://cas-3:50051" } }, weight: 1 },
  ]
}
```

## The Factory

Store trees are constructed recursively from config at startup:

```rust
// nativelink-store/src/default_store_factory.rs

pub fn store_factory<'a>(
    backend: &'a StoreSpec,
    store_manager: &'a Arc<StoreManager>,
    maybe_health_registry_builder: Option<&'a mut HealthRegistryBuilder>,
) -> Pin<FutureMaybeStore<'a>> {
    Box::pin(async move {
        let store: Arc<dyn StoreDriver> = match backend {
            StoreSpec::FastSlow(spec) => FastSlowStore::new(
                spec,
                store_factory(&spec.fast, store_manager, None).await?,
                store_factory(&spec.slow, store_manager, None).await?,
            ),
            StoreSpec::Compression(spec) => CompressionStore::new(
                &spec.clone(),
                store_factory(&spec.backend, store_manager, None).await?,
            )?,
            // ... recursive construction for all composite types
        };
        Ok(Store::new(store))
    })
}
```

**Source:** [`nativelink-store/src/default_store_factory.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-store/src/default_store_factory.rs)

The factory is recursive: composite stores call `store_factory` on their children, building the tree from leaves to root. After all named stores are constructed, a `post_init` pass resolves `RefStore` cross-references (stores that point to other named stores by name).

## Patterns

**The production CAS pattern:**
```
ExistenceCache → Compression → FastSlow(Memory, S3)
```

**The CI cache pattern:**
```
FastSlow(Filesystem, Noop)  // local disk, no durable backend
```

**The multi-region pattern:**
```
FastSlow(
  fast: Memory,
  slow: Shard([GrpcStore(region-1), GrpcStore(region-2)])
)
```

The algebra is small. The compositions are infinite.
