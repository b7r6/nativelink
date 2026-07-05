# The OCI → CAS Bridge

The `nativelink-oci` crate implements the OCI → REAPI projection described in §6 of the Standard OCI Toolchain Specification. It turns OCI toolchain images into REAPI `Directory` trees in CAS — making toolchains data that workers fetch on demand rather than infrastructure that operators pre-install.

## Architecture

```
         ┌─────────────────────────────────────────┐
         │            FetchServer                    │
         │       (Remote Asset API, gRPC)            │
         │                                           │
         │  FetchDirectory("oci://…" | "docker://…") │
         └────────────┬────────────────────────────┘
                      │
                      ▼
         ┌─────────────────────────────────────────┐
         │         OciToolchainClient                │
         │       (nativelink-oci/src/oci_client.rs)  │
         │                                           │
         │  1. Pull manifest + layers (each blob     │
         │     fully buffered in RAM)                 │
         │  2. Decompress (gzip only; zstd rejected)  │
         │  3. Project all layers → REAPI tree       │
         │  4. Upload blobs to CAS store             │
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
    │  Auth: Docker    │  │  FsNode → Dir   │
    │   Hub only       │  │  BLAKE3 hash    │
    │  Blob → RAM      │  │  (whole tree    │
    │   (buffered)     │  │   in RAM)       │
    └─────────────────┘  └─────────────────┘
```

The diagram matches the shipped code, not an aspiration: layer blobs are read
whole into memory (`registry.rs:349-354`), the projection holds every file's
content in RAM (`projection.rs:102`), only `gzip` layers decompress
(`oci_client.rs:356-363`), and real registry authentication exists for Docker
Hub alone (`registry.rs:230-237`). The [Limitations](#limitations) section
spells out what each of these costs you.

## The Projection Algorithm

The core of the bridge is `projection::project_layers()`. The orchestration layer calls it exactly once, passing every decompressed layer at the same time (`oci_client.rs:152-153`), so all layers fold into a single tree in one pass — the separate `merge_projections()` helper is *not* used on this path (see [§6.4](#conformance-with-the-spec) below). Given one or more uncompressed OCI layer tarballs:

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
- **Hard links** → resolved to the content of their target (§10). The target must have appeared **earlier** in the stream; a forward reference (target not yet seen) is a hard error (`projection.rs:266-272`), not a deferred second pass.
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

The bridge is exposed through the standard [Remote Asset API](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/asset/v1/remote_asset.proto) `Fetch/FetchDirectory` RPC. A `FetchDirectoryRequest` whose `uris` list contains an `oci://` **or** `docker://` URI is routed to the OCI toolchain client (`fetch_server.rs:192`); every other URI scheme falls through to the ordinary remote-asset lookup.

The handler then pulls the image, projects it, uploads the blobs, and returns the root `Directory` digest in `FetchDirectoryResponse.root_directory_digest`. The caller merges that digest into its action's `input_root_digest`, and the worker fetches the tree from CAS like any other input.

### End to end with `grpcurl`

NativeLink does not register a gRPC reflection service, so point `grpcurl` at the proto files directly. `instance_name` must match a configured `fetch` entry:

```bash
grpcurl -plaintext \
  -import-path nativelink-proto \
  -proto build/bazel/remote/asset/v1/remote_asset.proto \
  -d '{
        "instance_name": "main",
        "uris": ["oci://ghcr.io/myorg/toolchain:v2"],
        "digest_function": "BLAKE3"
      }' \
  localhost:50051 \
  build.bazel.remote.asset.v1.Fetch/FetchDirectory
```

Two things about that request body:

- The request's `digest_function` is **ignored**. The handler drops it — the parameter is named `_digest_function_proto` (`fetch_server.rs:250`) — and projects with the digest function from the static `oci` config instead. To change the hash function, edit the config (below), not the request.
- `qualifiers` are accepted by the proto but unused on the OCI path.

A successful response (the projection ran with BLAKE3, so `digestFunction` is the proto enum `BLAKE3`, numeric value `9`):

```json
{
  "status": {
    "message": "OCI toolchain imported: 214 files (0 deduped), 37 directories"
  },
  "uri": "oci://ghcr.io/myorg/toolchain:v2",
  "rootDirectoryDigest": {
    "hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "sizeBytes": "412"
  },
  "digestFunction": "BLAKE3"
}
```

The `status.message` is assembled from the import counters (`fetch_server.rs:296-299`), and `rootDirectoryDigest` is the toolchain's execution identity.

### Observe the projected tree in CAS

After the import, the root `Directory` proto, every child `Directory`, and every file blob live in the CAS store named by `oci.cas_store` (defaulting to `fetch_store`). Walk the tree with the REAPI `ContentAddressableStorage/GetTree` RPC, keyed by the returned root digest:

```bash
grpcurl -plaintext \
  -import-path nativelink-proto \
  -proto build/bazel/remote/execution/v2/remote_execution.proto \
  -d '{
        "instance_name": "main",
        "root_digest": {
          "hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "size_bytes": "412"
        },
        "digest_function": "BLAKE3"
      }' \
  localhost:50051 \
  build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree
```

`GetTree` streams the root `Directory` and all of its descendants — the exact protos the projection wrote. Individual file blobs can be pulled with `BatchReadBlobs` or the byte-stream `Read` API using the digests inside those `Directory` protos.

### When OCI is not configured

If the `FetchConfig` for the target `instance_name` has no `oci` block, `handle_oci_fetch_directory` returns `Code::Unimplemented` (`fetch_server.rs:253-259`) with:

```
OCI toolchain support not configured for this instance; add 'oci' section to FetchConfig
```

The remedy is the configuration in the next section: add an `oci` block to the `fetch` service entry for that instance. (The `digest_function` itself is validated at startup — anything other than `BLAKE3` or `SHA256` is a hard configuration error, `fetch_server.rs:77-86`.)

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

Those three keys are the entire surface: `cas_store`, `dedup_check`, and `digest_function` (`OciFetchConfig`, `cas_server.rs:189-206`, with `deny_unknown_fields`). There is deliberately **no** credential or authentication field — the registry client only knows how to fetch a real token for Docker Hub and is anonymous everywhere else (see [Limitations](#limitations)). Likewise, `strict_hint_verification` exists on the internal `ImportConfig` but is fixed to `false` here (`fetch_server.rs:91`) and cannot be turned on from this config.

## Conformance with the Spec

The implementation honors:

| Spec Section | Requirement | Status |
|---|---|---|
| §5.2 | No whiteouts | Rejects `.wh.*` entries |
| §6.1 | Directory tree structure | FileNode, SymlinkNode, DirectoryNode |
| §6.2 | BLAKE3 digest function | Default; SHA256 fallback available |
| §6.4 | Multi-layer projection | `project_layers()` folds **all** layers into one tree in a single pass (`oci_client.rs:152`). A separate `merge_projections()` helper exists but is test-only and pins BLAKE3 (`projection.rs:530-582`, exercised only at `:841`); the import path never calls it. |
| §6.5 | Hint verification | Warns on mismatch, does not fail (per spec: "treat as absent") |
| §4.1 | No special files | Skipped silently |
| §4.2 | Symlinks as content | Stored as `SymlinkNode { target }` |
| §10 | Hardlink equivalence | Resolved to file content |

## Limitations

The bridge handles the happy path for conforming Standard OCI Toolchain images. Outside that path, the current implementation has sharp edges that are load-bearing enough to state plainly.

- **Everything is buffered in memory — twice.** `fetch_blob` reads each layer whole into a `Vec<u8>` (`resp.bytes().to_vec()`, `registry.rs:349-354`), and the projection keeps every file's content resident as `Bytes` in a `BTreeMap` for the entire import (`projection.rs:102`). Nothing streams. A multi-gigabyte toolchain image is held in RAM in its compressed form, its decompressed form, and again as the projected file set — so a large image or a memory-constrained instance can run out of memory. Size your `fetch` instance accordingly.
- **`gzip` only; `zstd` is rejected.** Layers with a `+gzip` (or the legacy Docker `.tar.gzip`) media type decompress (`oci_client.rs:225-229`). A layer whose media type contains `+zstd` reaches `decompress_zstd`, which unconditionally returns an error — `"zstd decompression not yet implemented"` (`oci_client.rs:356-363`). Publish toolchain layers as `gzip`.
- **Real authentication is Docker Hub only.** `authenticate` fetches a Bearer token exclusively for `registry-1.docker.io` (`registry.rs:230-237`); for every other registry it returns `Ok(None)` and the request goes out anonymously. There is no credential field in the config, so a **private** repository on `ghcr.io`, `ECR`, `quay.io`, or a self-hosted registry answers with `401`, which surfaces as `"Registry returned 401 ... for manifest"` (`registry.rs:290-294`). Only public images work off Docker Hub today.
- **Multi-architecture / manifest-list tags fail now, not later.** `OciManifest` requires non-optional `config` and `layers` fields (`registry.rs:133-137`), and the manifest `Accept` header advertises only the single-image manifest media types (`registry.rs:277-279`) — it does not request an image index or manifest list. Point the bridge at a multi-arch tag and manifest parsing fails outright. Pin a single-platform digest or a single-platform tag.
- **Forward-referencing hard links error out.** A hard link whose target has not yet been seen in the tar stream is a hard error (`projection.rs:266-272`); there is no deferred resolution pass despite the code comment musing about one.
- **`strict_hint_verification` is unreachable.** The field exists on `ImportConfig` (`oci_client.rs:76`) and, when set, would fail the import on a mismatched `dev.straylight.toolchain.reapi.root` hint. But `fetch_server.rs:91` wires it to `false` and `OciFetchConfig` exposes no knob for it, so hint mismatches are always downgraded to a warning and the hint is treated as absent (§6.5, `projection.rs:301-315`).

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

The roadmap is, largely, the [Limitations](#limitations) above turned into work items:

- **Streaming layers** — replace the whole-blob buffering with a streaming tar reader, so large toolchains (>1GB) no longer have to fit in RAM three times over
- **zstd decompression** — add a `zstd` decoder so `+zstd` layers stop failing
- **Manifest list / index resolution** — request the image-index media types and select the platform-appropriate manifest, so multi-arch tags resolve instead of failing to deserialize
- **Registry credentials** — an authentication field in `OciFetchConfig` so private `ghcr.io`, `ECR`, and `quay.io` images can be pulled
- **Caching the mapping** — store the `(OCI manifest digest) → (REAPI root digest)` mapping so repeat requests skip re-projection
- **Worker pre-staging** — notify workers of incoming toolchain digests via platform property signals
- **Conformance verification tool** — `nativelink-toolchain-check` that round-trips OCI→REAPI→materialize→verify
