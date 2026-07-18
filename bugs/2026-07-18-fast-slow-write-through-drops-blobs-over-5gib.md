# `FastSlowStore` write-through silently drops every blob >5 GiB (S3 single-part PUT ceiling)

- **Date:** 2026-07-18
- **Reporter:** b7r6
- **Component:** `nativelink-store` (`fast_slow_store.rs`, `FastSlowStore::update` with `fast_direction = both`) + the S3/R2 store backend (`s3_store.rs`, no multipart upload)
- **Repo state:** branch `nix-cache`, `ea803264`
- **Topology:** `nl-nix push` → gossamer's local `nix_cache` on `127.0.0.1:50071` → `FastSlowStore` { fast = local-filesystem CAS, slow = R2 (S3-compatible) } with `fast_direction = "both"` (write-through). Rendered config: `max_nar_size_bytes = 34359738368` (34 GiB), `casFastBytes = 64 GiB`.
- **Severity:** **High** — the fleet cache **silently stops accepting any artifact larger than ~5 GiB**. That is exactly the size class we most need cached: the sovereign toolchain sysroots (`cxx-clang22-*`) are ~11–12 GiB each. Small blobs and reads are unaffected, so it presents as "the cache works" while quietly missing on everything big.

## Root cause (corrected after reproduction — 2026-07-18)

The "no multipart / 5 GiB single-part ceiling" hypothesis below is **wrong**:
`s3_store.rs` *does* implement multipart (`MAX_UPLOAD_SIZE = 48 TiB`,
`MAX_UPLOAD_PARTS = 10 000`), and `ingest_nar_direct` passes `ExactSize`.
Reproduced on idle x86 `weyl` against the same R2 bucket: a 10 MiB blob
(single multipart, 2 parts) and a 6 GiB blob (~1200 parts) both push clean; a
12 GiB blob **also succeeds** but takes **236 s** and floods the log with:

```
ERROR aws_smithy_runtime::client::http::connection_poisoning: unable to mark the
connection for closure because no connection was found! The underlying HTTP
connector never set a connection.
```

The real cause is **too many tiny parts**: the part-size math targeted the
5 MiB floor (`max_size / MIN_MULTIPART_SIZE`), so a 12 GiB blob became ~2300
concurrent 5 MiB PUTs. That churns/poisons R2's HTTP connection pool. On a good
link the retries eventually win (slowly); on `gossamer` (aarch64, mid-bootstrap,
flakier R2 path) the retries **exhaust**, the slow-store write fails, and
write-through cascades that into discarding the successful fast-store write too
(`all-three-false`). So it presents as "drops blobs over ~5 GiB" but is really
"the many-part path gets flaky as the part count climbs."

**Fix applied:** size parts at `TARGET_MULTIPART_PART_SIZE = 64 MiB` (grown only
to stay under 10 000 parts for very large objects), so 12 GiB → ~192 parts.
Far fewer connections, faster, reliable. A best-effort write-through (keep the
fast copy when the slow tier fails) remains a worthwhile follow-up for defence
in depth.

## Summary

With `fast_direction = both`, `FastSlowStore::update` tees the incoming stream to
**both** the fast (local) and slow (R2/S3) stores and requires **both** to
succeed. R2 is S3-compatible, and a plain `PutObject` (single-part) caps at
**5 GiB**. The S3 store backend does not do multipart upload, so any blob over
5 GiB fails its slow-store write partway through; because the write is
write-through, that failure aborts the **entire** update — the fast (local) write
is discarded too, and the client gets a failed ingest. The NAR never lands even
though there is 3.6 TiB free on the local fast store.

This is a **regression** introduced by `1e911f0` ("nativelink: fast_direction
get -> both (write-through) — fix slow cache read-back"). Before that change,
writes were fast-only, so multi-GiB pushes succeeded (verified working the same
morning, pre-deploy, on identical ~11–12 GiB sysroots).

## Observed (server side)

`journalctl -u nativelink-nix-cache`, pushing an 11.2 GiB sysroot
(`…-straylight-sysroot-libcxx-glibc-aarch64`, 12 GiB NAR = 12,024,840,208 B):

```
WARN nativelink_store::fast_slow_store: FastSlowStore::update: completed with error(s),
  key: Digest(DigestInfo("d155cdd6be9379cfdb8c98fba7f9a2250700a8ec68241d47caa0bd9b1f81507d-12024840208")),
  elapsed_ms: 69784, data_stream_ok: false, fast_store_ok: false, slow_store_ok: false
  in nativelink::services::http_connection with remote_addr: 127.0.0.1:55536, socket_addr: 0.0.0.0:50071
```

`data_stream_ok: false, fast_store_ok: false, slow_store_ok: false` — the slow
write fails, and write-through takes down the fast write and the data stream with
it. `elapsed_ms ≈ 70 s` ≈ the time to stream ~5 GiB to R2 before the PUT is
rejected.

## Client side

`nl-nix push --recursive` of the 11.2 GiB path: serializes the full 12 GiB NAR to
its spool, uploads, then the push fails — but with **no diagnostic**: exit is
non-zero and stdout/stderr are empty (progress is TTY-only; there is no error log
for a server-rejected write). `curl …/<hash>.narinfo` stays `404`. A fresh 1.2 KiB
path pushed in the same session lands `404 → 200` — confirming the cache is
healthy for small blobs and the failure is size-gated.

## Expected vs. actual

- **Expected:** a >5 GiB blob is stored. Either (a) the S3 store uses multipart
  upload for objects over the single-part ceiling, or (b) write-through does not
  let a slow-store failure discard a successful fast-store write for a
  best-effort cache, or (c) large blobs are routed fast-only by a size threshold.
- **Actual:** every blob >5 GiB is rejected in full; the local fast store (with
  ample space) never retains it; the client gets an opaque failure.

## Suggested fixes (in preference order)

1. **Multipart upload in the S3 store** (`s3_store.rs`) — the correct fix; S3/R2
   *require* multipart above 5 GiB. Add a part-size knob (e.g. 64–256 MiB).
2. **Size-thresholded write-through** — keep `1e911f0`'s read-back fix for the
   common (small) case, but route blobs above a configurable size fast-only, so a
   slow-store ceiling can't drop a cacheable artifact.
3. **Don't fail the fast write on a slow-write error** for a best-effort cache —
   surface `slow_store_ok: false` as a warning, keep the fast copy, let the
   slow tier backfill (or drop) asynchronously.

## Secondary: silent client failure

Independently of the store fix, `nl-nix push` should **report** a server-rejected
write (non-empty stderr with the digest + the server status) rather than exit
non-zero with no output — this cost real debugging time (a 12 GiB blob appears to
"push successfully" and then simply isn't in the cache).

## Repro

```
# on a host with the write-through nix_cache and an R2 slow tier:
P=$(nix build .#packages.aarch64-linux.cxx-clang22-libcxx-glibc --no-link --print-out-paths)   # ~11 GiB
nl-nix --to http://127.0.0.1:50071/nix/main --no-compress push --recursive "$P"                 # fails, no output
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:50071/nix/main/$(basename $P|cut -d- -f1).narinfo  # 404
journalctl -u nativelink-nix-cache | grep FastSlowStore   # the write-through error above
```
