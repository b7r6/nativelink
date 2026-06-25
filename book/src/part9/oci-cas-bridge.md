# The OCI → CAS Bridge

The `nativelink-oci` crate implements the OCI → REAPI projection described in §6 of the Standard OCI Toolchain Specification. It turns OCI toolchain images into REAPI `Directory` trees in CAS — making toolchains data that workers fetch on demand rather than infrastructure that operators pre-install.

## Architecture

```
         ┌─────────────────────────────────────────┐
         │            FetchServer                    │
         │       (Remote Asset API)                  │
         │                                           │
         │  FetchDirectory("oci://ghcr.io/…:v1")   │
         └────────────┬────────────────────────────┘
                      │
                      ▼
         ┌─────────────────────────────────────────┐
         │         OciToolchainClient                │
         │       (nativelink-oci/src/oci_client.rs)  │
         │                                           │
         │  1. Pull manifest + layers                │
         │  2. Decompress (gzip/zstd → raw tar)     │
         │  3. Project layers → REAPI tree          │
         │  4. Upload blobs to CAS store            │
         └────────────┬────────────────────────────┘
                      │
              ┌───────┴───────┐
              │               │
              ▼               ▼
    ┌─────────────────┐  ┌─────────────────┐
    │  RegistryClient  │  │   Projection     │
    │  (registry.rs)   │  │  (projection.rs) │
    │                  │  │                  │
    │  OCI Dist v2     │  │  tar → FsNode   │
    │  Auth (Bearer)   │  │  FsNode → Dir   │
    │  Blob streaming  │  │  BLAKE3 hash    │
    └─────────────────┘  └─────────────────┘
```

## The Projection Algorithm

The core of the bridge is `projection::project_layers()`. Given one or more uncompressed OCI layer tarballs:

### Phase 1: Build In-Memory Tree

Iterate all tar entries and build an `FsNode` tree:

```rust
enum FsNode {
    File { content: Bytes, executable: bool },
    Symlink { target: String },
    Directory { children: BTreeMap<String, FsNode> },
}
```

Rules:
- **Regular files** → `FsNode::File` with executable bit from mode `& 0o111`
- **Symlinks** → `FsNode::Symlink` with target as content (§4.2)
- **Directories** → `FsNode::Directory` created on demand
- **Hard links** → resolved to the content of their target (§10)
- **Whiteouts** → rejected (§5.2: conforming images are additive)
- **Special files** → skipped (§4.1: no device nodes, FIFOs, sockets)

### Phase 2: Convert to REAPI Merkle Tree

Walk the `FsNode` tree bottom-up:

1. For each **file**: hash content with BLAKE3 → `FileNode { name, digest, is_executable }`
2. For each **symlink**: `SymlinkNode { name, target }`
3. For each **subdirectory**: recurse, serialize the child `Directory` proto, hash it → `DirectoryNode { name, digest }`
4. Sort all node lists lexicographically by name (REAPI canonical form)
5. Assemble `Directory { files, directories, symlinks }`
6. Serialize + hash the `Directory` proto itself → that's its CAS address

The output is:
- A root `Directory` proto and its digest
- All child `Directory` protos (for `Tree.children`)
- All file content blobs (deduped by content hash)

### Phase 3: Upload to CAS

The orchestration layer (`oci_client.rs`) takes the projection output and:

1. **Dedup check** — calls `store.has_many()` on all file blob digests (configurable; saves bandwidth for incremental toolchain updates)
2. **Upload missing files** — `store.update_oneshot(digest, bytes)` for each
3. **Upload all Directory protos** — always uploaded (small, correctness-critical)
4. **Return root digest** — this is the toolchain's execution identity

## Integration via FetchServer

The bridge is exposed through the standard [Remote Asset API](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/asset/v1/remote_asset.proto) `FetchDirectory` RPC. When a client calls:

```
FetchDirectory(uris: ["oci://ghcr.io/myorg/toolchain:v2"])
```

NativeLink:
1. Recognizes the `oci://` URI scheme
2. Pulls the image, projects it, uploads blobs
3. Returns the root `Directory` digest in `FetchDirectoryResponse.root_directory_digest`

The client then merges this digest into its action's `input_root_digest`.

## Configuration

Enable OCI support in your NativeLink config by adding `oci` to the fetch service:

```json5
{
  services: {
    fetch: [{
      instance_name: "main",
      fetch_store: "CAS_STORE",
      oci: {
        cas_store: "CAS_STORE",    // where to upload (defaults to fetch_store)
        dedup_check: true,         // FindMissingBlobs before upload
        digest_function: "BLAKE3"  // must match your execution service
      }
    }]
  }
}
```

## Conformance with the Spec

The implementation honors:

| Spec Section | Requirement | Status |
|---|---|---|
| §5.2 | No whiteouts | Rejects `.wh.*` entries |
| §6.1 | Directory tree structure | FileNode, SymlinkNode, DirectoryNode |
| §6.2 | BLAKE3 digest function | Default; SHA256 fallback available |
| §6.4 | Per-layer projection + merge | `merge_projections()` for disjoint layers |
| §6.5 | Hint verification | Warns on mismatch, does not fail (per spec: "treat as absent") |
| §4.1 | No special files | Skipped silently |
| §4.2 | Symlinks as content | Stored as `SymlinkNode { target }` |
| §10 | Hardlink equivalence | Resolved to file content |

## Code Map

| File | Purpose |
|---|---|
| `nativelink-oci/src/lib.rs` | Crate root, module declarations |
| `nativelink-oci/src/registry.rs` | OCI Distribution v2 client (auth, manifest, blob fetch) |
| `nativelink-oci/src/projection.rs` | Core algorithm: tar → FsNode → REAPI Directory tree |
| `nativelink-oci/src/oci_client.rs` | Orchestration: pull → decompress → project → upload |
| `nativelink-config/src/cas_server.rs` | `OciFetchConfig` config types |
| `nativelink-service/src/fetch_server.rs` | `FetchDirectory` RPC handler with OCI routing |

## What's Next

The bridge as implemented handles the happy path for conforming Standard OCI Toolchain images. Future work:

- **Streaming layers** — currently buffers entire layers in memory; for large toolchains (>1GB), stream through tar entries without holding the full blob
- **zstd decompression** — add `zstd` crate for `+zstd` compressed layers
- **Manifest list / index resolution** — handle multi-arch images by selecting the platform-appropriate manifest
- **Caching the mapping** — store the `(OCI manifest digest) → (REAPI root digest)` mapping so repeat requests skip re-projection
- **Worker pre-staging** — notify workers of incoming toolchain digests via platform property signals
- **Conformance verification tool** — `nativelink-toolchain-check` that round-trips OCI→REAPI→materialize→verify
