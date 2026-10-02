# `Execute` rejects `digest_function = UNKNOWN/0`, blocking the reference REAPI clients (buck2, Bazel) that elide it for SHA256

- **Date:** 2026-07-18
- **Reporter:** b7r6
- **Component:** `nativelink-service` (`execution_server.rs`, the `ExecuteRequest.digest_function == 0` guard at `:336` / `:513`)
- **Repo state:** branch `nix-cache`, `8fe8f2ed`
- **Topology:** client `buck2` (`wasm-rules`, `b545db3169`) on `ultraviolence` → public scheduler endpoint `watchtower.sju1.s4.gl:50051` (`public` server, execution + capabilities + a CAS/AC shard ring), `instance_name = main`, `digest_algorithms = SHA256`
- **Severity:** Medium–High for RBE bring-up — **hard-blocks every remote *execution* request from a stock buck2/Bazel client using SHA256.** Zero actions reach a worker. Does *not* affect `cache` mode (CAS/AC/ByteStream length-inference works fine); it is specific to the `Execute` path.
- **Disposition:** This is **arguably correct server behaviour** (see Analysis) — the report exists to (a) document that it blocks the reference clients out of the box, (b) record that the primary fix is client-side, and (c) propose an *opt-in* server-side compatibility lever for operators who can't patch every client.

## Summary

A `buck2 build` forced onto remote execution fails at the very first `Execute`
RPC — before any input upload, before any worker dispatch:

```
Action failed: root//cxx:hello-cxx (cxx_compile hello-cxx-0)
Internal error (stage: remote_call_error): Remote Execution Error on Execute
  with digest 34a973d9…:142 …
Error: (Failed to start remote execution: code: 'Client specified an invalid
  argument', message: "ExecuteRequest.digest_function must be explicitly set
  (received UNKNOWN/0). The server cannot safely default the digest function
  for execution because it determines how output Directory trees are hashed.
  Clients MUST set this field to the digest function used to compute the
  action_digest (e.g. SHA256=1, BLAKE3=9).")
Commands: 1 (cached: 0, remote: 1, local: 0)
```

`remote: 1` confirms the action was dispatched remote and the scheduler is the
one rejecting it. The command tally never advances past this: nothing is
uploaded, nothing runs on a worker.

## Root cause (both sides)

**Server (`nativelink-service/src/execution_server.rs:329`):** the `Execute`
handler hard-rejects any request with `digest_function == 0`:

```rust
// The digest function MUST be explicitly declared by the client for
// execution requests. A value of 0 (UNKNOWN) means the client did not
// set the field, and we cannot safely default it — the worker will use
// this function to hash output Directory trees, and a mismatch corrupts
// results for clients expecting a different algorithm (e.g. BLAKE3
// clients getting SHA256 directory digests).
if request.digest_function == 0 {
    return Err(make_input_err!("ExecuteRequest.digest_function must be … set …"));
}
```

**Client (buck2 `remote_execution/oss/re_grpc/src/client.rs:125` +
`app/buck2_re_configuration/src/lib.rs:534`):** buck2 *deliberately* elides
`digest_function` for the "length-inferable legacy set". `execute_with_progress`
stamps `self.digest_function_value()` onto the `Execute` request (`client.rs:761`),
but that helper zeroes anything whose `must_announce()` is false:

```rust
fn digest_function_value(digest_function: Option<ReDigestFunction>) -> i32 {
    match digest_function {
        Some(df) if df.must_announce() => df.proto_value(),
        _ => 0,                       // ← SHA256 lands here
    }
}
// must_announce(Sha1 | Sha256) = false;  must_announce(Blake3 | Blake3Keyed) = true
```

buck2's own doc comment cites the REAPI rule it's relying on: the field *MAY* be
left unset for MD5/SHA1/SHA256/… and the server *SHOULD* infer from the digest
length; only BLAKE3 *must* be announced "whose 32-byte digest is indistinguishable
by length from SHA256." So buck2 sends `Execute{ digest_function: 0 }` and expects
the server to infer SHA256. NativeLink refuses.

## Analysis — who is right?

Both readings are spec-permitted, and **NativeLink's is the safer one:**

- REAPI (`ExecuteRequest.digest_function` docs) says a client *MAY* leave it unset
  for the length-inferable functions and the server *SHOULD* infer. "SHOULD," not
  "MUST" — a server may decline.
- The decline is *well-founded precisely for the default case*. SHA256 and BLAKE3
  both produce **32-byte** digests, so length inference **cannot** disambiguate
  them — the one collision buck2's own comment calls out. On the `Execute` path
  the choice isn't cosmetic: the worker uses this function to hash the **output**
  `Directory` trees it produces, so guessing wrong yields output digests the
  client can't address. NativeLink is right not to guess.

So this is not a NativeLink correctness bug. It *is* an **interop wall**: the two
reference REAPI clients both ship this behaviour by default —

- **buck2:** elides for SHA256 (confirmed above; `must_announce(Sha256) == false`).
- **Bazel:** historically omits `digest_function` for SHA256 as well (the "legacy
  inferable" convention is where buck2 inherited it). *(Not re-verified against a
  current Bazel here — flagged for confirmation, but it's the same lineage.)*

— which means a stock SHA256 client cannot drive execution against this server
without a client-side patch. Given SHA256 is the ecosystem default, the strict
guard rejects the common case on first contact.

## Expected vs. actual

- **Expected (ecosystem):** a SHA256 client that elides `digest_function` can run
  a remote action; the server uses SHA256 (its configured/announced function) for
  the output tree.
- **Actual:** `INVALID_ARGUMENT`, no execution. Correct per the letter of "SHOULD,"
  but breaks the default client.

## Proposed fixes

**Primary — client side (buck2), tracked separately:** always announce the digest
function on the `Execute` call, even for SHA256 — the output-tree-hashing rationale
means explicit *is* correct here. One-line change in buck2's `execute_with_progress`
to stamp `proto_value()` unconditionally, leaving CAS/ByteStream length-inference
untouched. This is the right long-term hygiene and unblocks us immediately; it does
**not** require any server change. *(This report does not ask NativeLink to change
for us — it documents the clash so the decision below is informed.)*

**Optional — server side (opt-in compatibility), your call:**

1. **Single-function inference, opt-in.** When an instance's CAS/AC is configured
   with exactly **one** digest function (no SHA256/BLAKE3 collision *for this
   server*), a `digest_function == 0` request is unambiguous — the server can
   safely default to its sole configured function. Gate behind a per-instance
   config flag (e.g. `permit_execute_digest_function_inference = false` by default)
   so strict mode stays the default and mixed-function instances never infer. This
   recovers stock buck2/Bazel SHA256 clients without weakening multi-function
   deployments.
2. **Keep the guard, but make it maximally actionable (done well already).** The
   current error is well-formed (`INVALID_ARGUMENT`, real message, proper trailers
   — a notable contrast to the transport-error bug in the sibling report) and names
   the fix. If the guard stays strict, consider adding the offending
   `instance_name` + the server's configured digest function(s) to the message so
   an operator sees "server speaks SHA256; client sent UNKNOWN" at a glance.
3. **Capabilities signalling.** Ensure `GetCapabilities` advertises the supported
   `digest_functions` (it likely does) so a well-behaved client *could* negotiate;
   note that neither buck2 nor Bazel currently consults it to decide whether to
   stamp the `Execute` field, so this helps future clients only.

## Reproduction

1. Point buck2 at the public scheduler with SHA256:
   `[buck2] digest_algorithms = SHA256`;
   `[buck2_re_client] engine_address = grpc://watchtower.sju1.s4.gl:50051`,
   `instance_name = main`, `tls = false`.
2. Use an `rbe`-mode execution platform (`local_fallback = False` so the compile
   is genuinely dispatched remote rather than winning a local hybrid race), and
   pre-materialize any `local_only` inputs so only the compile goes remote.
3. `buck2 build //cxx:hello-cxx`. The `Execute` RPC returns `INVALID_ARGUMENT`
   with the `digest_function must be explicitly set` message; `Commands:` shows
   `remote: 1` and no worker activity.

## Cross-references

- Server rationale + spec pointers: `book/src/part9/standard-oci-toolchain.md`
  §4.4, §14.16 (cited in the code comment at `execution_server.rs:334`).
- Existing server test asserting the guard:
  `nativelink-service/tests/execution_server_test.rs:151`.
- Sibling interop bug (different failure, same bring-up):
  `2026-07-18-cas-write-surfaces-unknown-transport-error-instead-of-unavailable.md`.
