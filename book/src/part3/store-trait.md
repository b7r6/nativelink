# The Store Trait

Everything in NativeLink is built on one abstraction: the store. CAS is a store. AC is a store. Worker artifact storage is a store. The filesystem cache, the S3 backend, the compression layer, the deduplication layer — all stores. Understanding this trait is understanding NativeLink.

## The Interface

The store trait has two layers. `StoreDriver` is what you implement. `StoreLike` is what you call. They're separated to allow the outer layer (`Store`) to provide convenience methods and type erasure without burdening implementors.

The core operations are three:

```rust
// nativelink-util/src/store_trait.rs

/// Check if a key exists. Returns Some(size) if found, None if not.
async fn has(self: Pin<&Self>, key: StoreKey<'_>)
    -> Result<Option<u64>, Error>;

/// Upload content for a key.
async fn update(self: Pin<&Self>, key: StoreKey<'_>,
    reader: DropCloserReadHalf, upload_size: UploadSizeInfo)
    -> Result<u64, Error>;

/// Download content for a key (with optional range).
async fn get_part(self: Pin<&Self>, key: StoreKey<'_>,
    writer: &mut DropCloserWriteHalf, offset: u64, length: Option<u64>)
    -> Result<(), Error>;
```

That's it. `has`, `update`, `get_part`. Every store — from in-memory hash maps to S3 buckets to compression wrappers — implements these three operations.

**Source:** [`nativelink-util/src/store_trait.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-util/src/store_trait.rs)

## StoreKey

The key is not a raw hash — it's a `StoreKey` enum that can represent different types of content:

```rust
pub enum StoreKey<'a> {
    Digest(StoreKeyDigest<'a>),  // Content-addressed: hash + size
    Str(Cow<'a, str>),           // Named keys (for AC, metadata)
}
```

For CAS, keys are always digests. For AC, keys are string-encoded action digests (because the AC maps action hash → result, and the "content" is the serialized `ActionResult` proto).

## The Store Wrapper

Concrete store implementations are wrapped in a type-erased `Store` struct:

```rust
// nativelink-util/src/store_trait.rs

#[derive(Clone)]
#[repr(transparent)]
pub struct Store {
    inner: Arc<dyn StoreDriver>,
}
```

This is the handle you pass around. It's `Clone`, `Send`, `Sync`, and cheap to copy (it's an `Arc`). The type erasure means any code that accepts a `Store` works with any backend — callers don't know or care what's behind it.

## Streaming, Not Buffering

Notice that `update` takes a `DropCloserReadHalf` (a streaming reader) and `get_part` writes to a `DropCloserWriteHalf` (a streaming writer). Stores do not buffer entire blobs in memory. Data streams through the system — from the gRPC layer, through the store chain, to the backend.

This is critical for large artifacts. A 2GB compiled binary flows through CAS → compression → S3 as a stream, never fully materialized in memory. The `buf_channel` utility provides backpressure-aware channels that connect these streaming layers.

**Source:** [`nativelink-util/src/buf_channel.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-util/src/buf_channel.rs)

## Batch Operations

The trait also provides batch variants for efficiency:

```rust
async fn has_with_results(self: Pin<&Self>,
    digests: &[StoreKey<'_>], results: &mut [Option<u64>])
    -> Result<(), Error>;
```

`FindMissingBlobs` (the most frequent REAPI call) maps to `has_with_results` — checking hundreds of digests in one round-trip. Store backends can implement this with batched I/O (S3 batch HEAD, Redis MGET, etc.).

## Whole-File Optimization

Some stores can bypass streaming when the content is already on disk:

```rust
async fn update_with_whole_file(self: Pin<&Self>, key: StoreKey<'_>,
    path: OsString, file: fs::FileSlot, upload_size: UploadSizeInfo)
    -> Result<(u64, Option<fs::FileSlot>), Error>;

fn optimized_for(&self, optimization: StoreOptimizations) -> bool;
```

The filesystem store uses this to hardlink instead of copy. The caller checks `optimized_for(StoreOptimizations::FileUpdates)` and can pass the file handle directly, avoiding a read-copy-write cycle entirely.

## Health and Introspection

Every store participates in health checking:

```rust
async fn check_health(self: Pin<&Self>, namespace: Cow<'static, str>)
    -> HealthStatus;
```

And provides introspection for debugging:

```rust
fn inner_store(&self, digest: Option<StoreKey<'_>>) -> &dyn StoreDriver;
```

`inner_store` lets you walk the composition tree — useful for diagnostics when a specific layer is misbehaving.

## Why This Design Works

The power of this design is **substitutability**. Any store can wrap any other store. Any store can be used as CAS, as AC, or as worker artifact storage. The gRPC service layer doesn't know what's behind the `Store` handle it was given.

This means:
- You can add compression without changing any code — wrap the backend in a `CompressionStore`.
- You can add verification without changing any code — wrap in a `VerifyStore`.
- You can tier storage without changing any code — use a `FastSlowStore`.
- You can shard without changing any code — use a `ShardStore`.

The complexity lives in composition, not in individual implementations. Each store does one thing. The config assembles them into the topology you need.
