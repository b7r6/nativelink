# Mental Model

There is exactly one idea in remote execution: **content-addressing**.

Everything else — the caching, the distribution, the deduplication, the integrity checking — is a consequence of this one idea. If you understand content-addressing, you understand the system. If you don't, no amount of configuration documentation will help you.

## Content-Addressing

A content-addressed store maps `hash(content) → content`. You don't choose where things go. You don't name them. You compute their identity from their bits.

This gives you three properties for free:

1. **Deduplication.** If two actions produce byte-identical outputs, they have the same hash. You store it once.

2. **Integrity.** If you retrieve content by hash and the hash matches, the content is correct. No checksums, no version numbers, no "did something change underneath me."

3. **Cacheability.** If you can deterministically compute the hash of an action's inputs, and the action is deterministic, then the hash of the action *is* the cache key. Cache lookup is a hash table lookup.

## The Action Cache Equation

The Action Cache is conceptually:

```
AC[hash(Command, InputRootDigest, Platform)] = ActionResult {
    output_files: [(path, digest)],
    output_directories: [(path, tree_digest)],
    stdout_digest,
    stderr_digest,
    exit_code,
}
```

The inputs to the hash are:
- **Command**: the argv, environment variables, output paths, working directory
- **InputRootDigest**: the Merkle tree hash of the entire input filesystem
- **Platform**: the execution platform requirements (OS, architecture, toolchain)

If any of these change, the hash changes, and you get a cache miss. This is why toolchain management is the entire game — if your toolchain isn't hermetically captured in the platform specification, your action hashes will differ between machines, and your cache is worthless.

## The Merkle Tree

CAS stores files. But actions don't operate on individual files — they operate on directory trees. REAPI represents directory trees as Merkle trees: each directory node contains the digests of its children (files and subdirectories). The root digest identifies the entire tree.

```
RootDigest: abc123
├── src/
│   ├── main.c  → digest: def456
│   └── util.c  → digest: 789abc
├── include/
│   └── util.h  → digest: fed321
└── Makefile    → digest: 112233
```

Changing one file changes its digest, which changes its parent directory's digest, which propagates up to the root. The root digest is a single hash that uniquely identifies the entire input tree. This is how "did my inputs change?" becomes a single comparison.

## Why This Matters for NativeLink

NativeLink's entire architecture follows from content-addressing:

- **Stores** are content-addressed — the key is always a digest (hash + size). This is why they compose: any store that maps `digest → bytes` is interchangeable with any other.

- **The Action Cache** is a content-addressed map from action digests to results. It doesn't need invalidation logic — if the inputs change, the key changes, and there's nothing to invalidate.

- **Workers are stateless.** They fetch inputs by digest, run a command, upload outputs by digest. They hold no state between actions. This is why they can be cattle, not pets — scale them up, kill them, replace them.

- **The scheduler is stateless.** It matches actions to workers based on platform properties. It doesn't need to know what happened before. Failed actions are simply re-dispatched.

Content-addressing turns distributed systems problems into hash table problems. That's the insight. That's the whole thing.
