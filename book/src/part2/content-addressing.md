# Why Content-Addressing Works

Content-addressing is not a NativeLink implementation detail. It is the foundational design choice that makes the entire system possible. Every property you care about — correctness, deduplication, distribution, incremental updates — falls out of content-addressing for free.

This chapter explains why.

## The Alternative: Location-Addressing

Traditional storage is location-addressed. Files live at paths. Objects live at URLs. Rows live at primary keys. The name is independent of the content — you can change the content without changing the name.

This creates an entire class of problems:
- **Staleness.** Is the cached copy still valid? You need TTLs, ETags, last-modified headers, invalidation protocols.
- **Races.** Two writers update the same path. Who wins? You need locking, versioning, conflict resolution.
- **Duplication.** Two identical files at different paths. You store them twice. You need dedup logic.
- **Integrity.** Did the content arrive correctly? You need checksums, retries, verification.

Every distributed system built on location-addressing reinvents solutions to these problems. They're all partial, all fragile, and all add complexity.

## Content-Addressing Eliminates the Class

With content-addressing, the name *is* the content (or rather, a function of it). This eliminates every problem above:

- **Staleness is impossible.** The content for a given hash never changes. There is nothing to invalidate.
- **Races are impossible.** Two writers uploading the same content produce the same hash. The store is idempotent — writing the same bytes twice is a no-op.
- **Deduplication is structural.** Same content = same hash = stored once. No dedup algorithm needed.
- **Integrity is structural.** Verify the hash on read. If it matches, the content is correct. If it doesn't, the store is corrupt.

## Why This Matters for Build Caching

A build action is a pure function: `f(inputs, command, platform) → outputs`. If you content-address the inputs (Merkle tree of the input directory), the command (serialized proto), and the platform (key-value pairs), you get a unique digest that identifies the action.

If two developers on different machines run the same action with the same inputs, they compute the same action digest. The first one to complete populates the cache. The second one gets a cache hit. No coordination needed. No central server deciding what's "the same." The content determines identity.

This is why remote caching works at all. And it's why cache hit rates are determined by how precisely you control your inputs — especially your toolchain.

## The Merkle Tree: Content-Addressing at Scale

Individual file content-addressing is O(file count). But actions operate on directory trees with potentially millions of files. Re-hashing every file on every build would be prohibitive.

Merkle trees solve this. Each directory is hashed as a function of its children's hashes. Changing one file changes exactly one path from leaf to root — O(depth) rehashing instead of O(file count).

```
Root: H(Dir{src: H(Dir{main.c: H(content)}), lib: H(Dir{...})})
```

When you change `src/main.c`:
- `H(main.c content)` changes
- `H(src directory)` changes (because a child changed)
- `H(root)` changes (because a child changed)
- `H(lib directory)` does NOT change — it's in a different subtree

The client can compute the new root digest by rehashing only the changed path. The upload can skip all unchanged subtrees (they're already in CAS — same hash means same content, remember?). This is why incremental builds over remote cache are fast — you only upload what actually changed.

## Content-Addressing and the Store Trait

NativeLink's `Store` trait is defined in terms of content-addressing:

```rust
// nativelink-util/src/store_trait.rs

pub trait StoreLike: Send + Sync + Sized + Unpin + 'static {
    /// Check if a digest exists. Returns the size if found.
    fn has<'a>(&'a self, digest: impl Into<StoreKey<'a>>)
        -> impl Future<Output = Result<Option<u64>, Error>> + 'a;

    /// Upload content by digest.
    fn update<'a>(&'a self, digest: impl Into<StoreKey<'a>>,
        reader: DropCloserReadHalf, upload_size: UploadSizeInfo)
        -> impl Future<Output = Result<u64, Error>> + Send + 'a;

    /// Download content by digest.
    fn get_part<'a>(&'a self, digest: impl Into<StoreKey<'a>>,
        writer: impl BorrowMut<DropCloserWriteHalf> + Send + 'a,
        offset: u64, length: Option<u64>)
        -> impl Future<Output = Result<(), Error>> + Send + 'a;
}
```

The key is always a digest. The value is always bytes. This uniformity is why stores compose — any implementation of `has`/`update`/`get_part` is interchangeable with any other. Fast/slow tiering, compression, deduplication — they're all just stores wrapping stores, passing digests down.

## The Cost

Content-addressing is not free. The costs are:

1. **Hashing overhead.** Every blob must be hashed on upload. SHA-256 is ~500MB/s on modern hardware. Blake3 is ~5GB/s. For build artifacts this is negligible compared to I/O.

2. **No in-place mutation.** You can't "update" a cached result. You can only insert new content under a new hash. This means cache eviction is your only size control mechanism (there's no "overwrite with newer version").

3. **No enumeration by name.** You can't ask "what actions has this user cached?" because there are no user-associated names. You need external indexing for analytics.

These costs are small compared to the benefits. For build caching, content-addressing is not a tradeoff — it's the only correct design.
