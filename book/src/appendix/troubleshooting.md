# Appendix C: Troubleshooting

Common errors, what they mean, and how to fix them.

## Connection Errors

### "Connection refused" / "UNAVAILABLE"

**Cause:** Client can't reach NativeLink.

**Check:**
1. Is NativeLink running? `docker ps` or `systemctl status nativelink`
2. Is the port correct? Default is 50051
3. Is the address correct? No `http://` prefix for gRPC (use `grpc://` or bare `host:port`)
4. Firewall rules? `nc -z host 50051`
5. If Docker: is the port published? `-p 50051:50051`

### "TLS handshake failed" / "CERTIFICATE_VERIFY_FAILED"

**Cause:** TLS misconfiguration.

**Check:**
1. Does the cert cover the hostname being used? (check SAN)
2. Is the CA trusted? (pass `--tls_certificate` in Bazel, set `tls = true` in Buck2)
3. Are cert and key files readable by the NativeLink process?
4. Is the cert expired? `openssl x509 -in cert.pem -noout -dates`

## Request Errors

### "NOT_FOUND" on every request

**Cause:** Instance name mismatch.

**Fix:**
- Bazel default: `instance_name: ""` (empty string)
- Buck2 default: `instance_name: "main"`
- NativeLink config must match the client's instance name on ALL services (CAS, AC, execution, capabilities, bytestream)

### "INVALID_ARGUMENT: Unknown platform property"

**Cause:** Client sent a property not listed in `supported_platform_properties`.

**Fix:** Add the property to the scheduler config:
```json5
supported_platform_properties: {
  "new-property": "priority"  // or "exact", "minimum", "ignore"
}
```

### "RESOURCE_EXHAUSTED"

**Cause:** Request exceeds server limits (usually blob size for batch operations).

**Fix:** This is typically transparent — clients should fall back to ByteStream for large blobs. If it persists, check:
- `max_bytes_per_stream` in bytestream config (0 = unlimited)
- Client batch size settings

## Execution Errors

### "DEADLINE_EXCEEDED" on Execute

**Cause:** Action exceeded timeout.

**Check:**
1. Worker's `max_action_timeout` — is it long enough?
2. Client's timeout (`--remote_timeout` in Bazel)
3. The action itself — is it actually slow or hanging?
4. Worker connectivity — did the worker disconnect mid-execution? (check scheduler logs)

### Action queued forever (no workers)

**Cause:** No worker matches the action's platform properties.

**Check:**
1. Are workers connected? Check scheduler health endpoint or logs.
2. Do worker properties match action requirements? Compare `platform_properties` in worker config with properties in the action.
3. For `exact` properties: values must match exactly (case-sensitive, whitespace-sensitive).
4. For `minimum` properties: worker value must be >= requested value.
5. Workers may have disconnected (timeout). Check `worker_timeout_s`.

The `exact` / `minimum` / `priority` / `ignore` matching vocabulary is
declared per scheduler under `supported_platform_properties` — for the
field-by-field reference of each variant see
[Appendix B: Scheduler Catalog](./scheduler-catalog.md).

### "FAILED_PRECONDITION: Missing inputs"

**Cause:** Worker can't find input blobs in CAS.

**Check:**
1. CAS store connectivity — can the worker reach the CAS?
2. CAS eviction — were the blobs evicted between upload and execution? Increase CAS size.
3. Network partition — is there a proxy/firewall between worker and CAS?
4. If using `grpc` store in worker: is the endpoint correct?

## Cache Issues

### 0% cache hit rate

**Cause:** Action hashes differ between machines.

**Diagnosis:** See [Debugging Cache Misses](../part6/debugging-cache-misses.md).

**Most common causes:**
1. Different toolchain (not captured in action hash)
2. Absolute paths in actions
3. Environment variable leakage
4. Instance name mismatch (requests go to different AC namespaces)

### Cache hits return "NOT_FOUND" for output blobs

**Cause:** AC has a result but CAS doesn't have the referenced blobs (evicted).

**Fix:**
1. Increase CAS storage (larger eviction policy)
2. Use `completeness_checking` store wrapper on AC:
   ```json5
   completeness_checking: {
     backend: { /* AC store */ },
     cas_store: "CAS_STORE_NAME"
   }
   ```
3. Use a durable CAS backend (S3) so blobs survive restarts

### Stale cache results (wrong output)

**Cause:** Non-deterministic action, or toolchain mismatch.

**Fix:**
1. Identify the non-deterministic action (timestamps, random values, hostname)
2. Make it deterministic or mark it as non-cacheable
3. If toolchain mismatch: add toolchain identity to platform properties (Part V)
4. Nuclear option: clear the AC and rebuild

## OCI Toolchain / Fetch Errors

These cover the OCI → CAS bridge: a `FetchDirectory` request whose URI starts
with `oci://` or `docker://` pulls the image, projects its layers into an REAPI
`Directory` tree, and uploads the blobs to CAS. The bridge is an OCI registry
*client* — NativeLink pulls images, it does not serve a registry. See
[The OCI → CAS Bridge](../part9/oci-cas-bridge.md). Unless noted, these failures
surface to the gRPC client as `INVALID_ARGUMENT` on `FetchDirectory`.

### "OCI toolchain support not configured for this instance" (UNIMPLEMENTED)

**Cause:** A `FetchDirectory` request used an `oci://` or `docker://` URI, but
the instance's `fetch` service has no `oci` block, so the handler returns
`Code::Unimplemented` (`fetch_server.rs:253-259`).

**Fix:** Add an `oci` section to the `fetch` service for that instance:
```json5
services: {
  fetch: [{
    instance_name: "main",
    fetch_store: "CAS_STORE",
    oci: {
      cas_store: "CAS_STORE",     // where to upload; defaults to fetch_store
      dedup_check: true,          // has_many() before upload
      digest_function: "BLAKE3"   // must match your execution service
    }
  }]
}
```

### "Registry returned 401" / "403" pulling a private image

**Cause:** The OCI client only performs a real `Bearer`-token handshake for
Docker Hub (`registry-1.docker.io`); for every other registry it sends
anonymous requests (`registry.rs:230-237`). Even the Docker Hub token request
carries no credentials, so private Docker Hub repositories fail too. There is no
credential field in `OciFetchConfig` — only `cas_store`, `dedup_check`, and
`digest_function`.

**Fix:**
1. Pull only public, anonymously-readable images with `oci://` / `docker://`
   today.
2. For a private image, mirror it to a registry NativeLink can read
   anonymously, or project it out-of-band and publish it as a pre-pushed remote
   asset (the non-OCI `FetchDirectory` path serves stored `Directory` digests).
3. Credentialed pulls are a known gap, not a misconfiguration.

### "zstd decompression not yet implemented"

**Cause:** The layer is zstd-compressed (a `+zstd` media type). Only gzip layers
are decompressed today; `decompress_zstd()` is a stub that returns this error
(`oci_client.rs:356-363`).

**Fix:** Rebuild and republish the toolchain image with gzip-compressed layers —
the default for `docker build`, `crane`, and most CI image tooling.

### "Whiteout entry '…' found in layer N"

**Cause:** A layer carries a whiteout marker (a filename beginning with `.wh.`)
that deletes a path from a lower layer. The projection treats conforming
toolchain images as additive and disjoint (spec §5.2) and rejects whiteouts
rather than applying them (`projection.rs:169-175`).

**Fix:** Flatten the image to a single additive layer (for example a squashed
build, or `docker export` of a started container) so no path is deleted in a
later layer. Build toolchain images by adding files, never by removing them.

### Multi-architecture tag fails with "Parsing manifest JSON"

**Cause:** The tag points to a manifest list / image index (multi-architecture),
not a single-image manifest. `OciManifest.config` and `OciManifest.layers` are
required fields (`registry.rs:130-137`) and the `Accept` header omits the
index/list media types (`registry.rs:277-279`), so an index fails to
deserialize.

**Fix:**
1. Reference one platform's image by digest: `oci://registry/repo@sha256:…`
   pointing at the per-architecture manifest, not the multi-architecture tag.
2. Resolve the platform manifest out-of-band (`crane manifest`,
   `docker manifest inspect`) and fetch that digest.
3. Manifest-list resolution is future work — see
   [The OCI → CAS Bridge](../part9/oci-cas-bridge.md) ("What's Next").

### OCI import exhausts memory on a large image or layer

**Cause:** Nothing streams. Each blob is buffered whole in memory
(`registry.rs:349-354`), every decompressed layer is retained at once
(`oci_client.rs:210-247`), and the projection keeps every file's content
resident as `Bytes` — once in the in-memory tree and again in the file-blob map
(`projection.rs:102,457`). Peak memory is roughly the compressed blobs plus the
decompressed layers plus the whole file tree, all live at the same time.

**Fix:**
1. Keep toolchain images modest; split a very large toolchain across separate
   images and fetches.
2. Give the NativeLink process headroom for the largest single image you pull.
3. Streaming layer import is listed as future work in
   [The OCI → CAS Bridge](../part9/oci-cas-bridge.md).

### Root digest under the "wrong" hash after an OCI import

**Cause:** For OCI URIs the request's `digest_function` is ignored — the handler
parameter is discarded (`fetch_server.rs:250`) and the projection always uses
the static `digest_function` from `OciFetchConfig` (default `BLAKE3`,
`cas_server.rs:204`). The response reports the function actually used
(`fetch_server.rs:288-291`), but a client that assumes its own requested
function will look for the root `Directory` under a different hash.

**Fix:**
1. Set `oci.digest_function` to match what your execution service and clients
   expect. It must be `BLAKE3` or `SHA256`; any other value is a startup error
   (`fetch_server.rs:77-86`).
2. Read the `digest_function` field on `FetchDirectoryResponse` rather than
   assuming the request's value was honored.

## Performance Issues

### Slow uploads

**Check:**
1. Network bandwidth to CAS backend
2. Compression enabled? (reduces transfer size)
3. Large files going through batch API instead of ByteStream?
4. Client concurrency settings (`--remote_max_connections` in Bazel)

### Slow action execution

**Check:**
1. Worker CPU/memory — is it saturated?
2. Input fetch time — is the worker downloading large input trees?
3. Directory cache enabled? (worker `directory_cache`, `cas_server.rs:1233`)
4. Worker CAS has fast local tier? (filesystem, not just remote gRPC)

### High memory usage on scheduler

**Check:**
1. Number of in-flight actions — more actions = more state
2. Redis backend configured? (offloads state from memory)
3. Existence cache size (`max_count`) — too high?
4. Memory store eviction policy — is `max_bytes` appropriate?

## Startup Errors

### "Address already in use"

Another process (or previous NativeLink instance) holds the port.

```bash
lsof -i :50051  # find the process
kill <pid>       # or change the port in config
```

### "Permission denied" on file paths

NativeLink process doesn't have access to configured paths.

```bash
# Check ownership:
ls -la /data/cas/

# Fix:
chown -R nativelink:nativelink /data/
```

### "Store 'NAME' not found" during startup

A `ref_store` references a name that doesn't exist in the `stores` array. Check spelling and ensure the referenced store is defined before (or in the same config's) `stores` array.
