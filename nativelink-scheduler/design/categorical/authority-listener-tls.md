# Local mTLS qualification of fixed authority listeners

`tools/test-authority-listener-tls.py` runs the actual fixed authority listener
test stack with disposable server, client and worker certificate authorities.
It reuses the certificate generator in `test-authority-listener-boundary.py`,
then supplies the files to the existing tonic TLS builders. No production
configuration, binary selector, external listener or deployed credential is used.

The explicit test has four cases: isolated client/worker trust and deliberately
shared client trust, each with SHA256 and BLAKE3. Each case mounts one worker
listener and one client listener through `AuthorityListeners`. It checks every
writable client listener declared by this fixture, which is one. That statement
does not enumerate external proxies or deployment routes.

An authorized client writes and reads ActionCache before and after the worker
credential attempt. The worker's certificate succeeds on its own CAS and
ByteStream services, while ActionCache is absent from its listener. Valid
serialized result bytes use their actual digest and size as the cache key;
successful uploads through both worker object APIs leave the client cache value
unchanged, and the client reads those object bytes through ByteStream.

With separate trust roots, the worker identity must fail on the client listener
and leave its cache value unchanged. The receipt records the actual connection
or RPC error; it does not assume a particular TLS failure code. A five-second
timeout fails the test instead of being counted as credential rejection.

With the combined client/worker trust roots, the worker must successfully write
through the client listener and the authorized client must observe the changed
value. The fixture classifies that topology as unsafe. Separate ports alone do
not establish the boundary. The earlier plaintext negative control remains in
the default test suite.

Both isolated-trust cases also run the existing accepted-publication scenario
over mTLS: client input uploads, Execute, authenticated Worker API registration
and dispatch, a manual candidate report, an accepted terminal while publication
is held, scheduler cache publication and a later cache hit. The manual report
does not provide physical process-quiescence evidence.

The TLS case is ignored during ordinary Cargo/Bazel test runs because certificate
generation requires OpenSSL. Explicit selection without the certificate and
report environment variables fails; it never silently skips. Build the test
binary, then run it through the certificate runner:

```sh
cargo test --locked --offline --profile smol -j8 \
  -p nativelink-service --test authority_listener_test --no-run
python3 tools/test-authority-listener-tls.py \
  --test-binary /path/reported/by/cargo/authority_listener_test-binary \
  --output /tmp/authority-listener-tls-evidence
```

The runner requires exactly one executed ignored test and all four structured
cases. It creates its private temporary directory under a restrictive `umask`,
retains no certificates or private keys, and removes that directory afterward.
The output contains public certificate hashes, source and binary hashes, the
observed RPC results, and test output with generated private key strings removed.
The build command and source-to-binary provenance accompany the execution
receipt separately. Existing evidence files are never overwritten.

This qualifies only the declared local trust and in-memory store arrangement.
Provider namespace aliases, worker backend sink credentials, certificate
distribution, every deployed client route, and production authority construction
remain separate requirements.

The [qualification receipt](evidence/authority-listener-tls/manifest.json) records
the explicit execution, all four observations, the missing-certificate failure
control, ordinary test results, strict Clippy and the build/source hashes. Its
local result does not replace merged-root Bazel and deployment qualification.
