# Local listener trust experiment

The real NativeLink binary passes a local boundary experiment using its existing
listener configuration, TLS verifier, CAS, ByteStream, and ActionCache services.
This qualifies the declared local trust and namespace arrangement. The new
authority coordinator is not mounted by this experiment, and no deployed fleet,
proxy, credential distribution, or external route is covered.

`tools/test-authority-listener-boundary.py` starts only its own child processes
on temporary loopback ports. OpenSSL creates separate server, client, and worker
certificate authorities and short-lived leaf certificates. Keys and configs
remain in a private temporary directory and are removed afterward. The script
uses `grpcurl` with checked server certificates and explicit client identities;
it never disables TLS verification.

The passing topology has one worker listener exposing CAS and ByteStream, plus
two writable client ActionCache listeners. The worker listener trusts only the
worker CA; both client listeners trust only the client CA. The two client routes
share the intended AC sink. Worker CAS and ByteStream use a different store.
This particular fixture omits WorkerApi and execution workloads.

Each writable client route first accepts a valid client write. A worker using
its own valid certificate cannot write through either client route. The worker
listener does not serve `UpdateActionResult`; the raw observed status is kept
in the receipt rather than assuming a particular unserved-route code. After
each rejection, a permitted client reads the original AC entry successfully.

The namespace probe uses valid protobuf bytes encoding `ActionResult` with a
different exit code. Its actual SHA256 and length are also the tested action
key. Both a CAS upload and a ByteStream upload succeed with those exact bytes,
but neither changes the AC entry. This rules out malformed input or a failed
upload as explanations for namespace isolation.

## Controls that must fail the boundary

The same driver runs three deliberately unsafe local arrangements:

- Trusting both client and worker CAs on the client listeners permits both
  worker-originated writes. Different ports alone do not establish exclusion.
- Mapping the worker object services to the AC sink lets both valid upload
  methods replace the cached action result without an AC RPC.
- Mounting writable ActionCache on the worker listener permits a direct write
  with the worker's otherwise valid credentials.

The harness requires these exact failures. All three controls are detected;
they are not accepted deployment configurations. The script terminates only
the server process it started for each arrangement.

Run after building the actual server:

```sh
bazel build //:nativelink
python3 tools/test-authority-listener-boundary.py \
  --binary bazel-bin/nativelink --output /tmp/listener-boundary-evidence
```

The output manifest records the binary and runner hashes, each RPC status and
response, and the observed violations. [Checkpoint receipts](evidence/physical-recovery-checkpoint/manifest.json)
retain this run and its server build log. This does not establish closure of
legacy worker publication paths, canonical entry-point refusal, mounted
authority recovery, or the M4a deployment gate. The actual rollout must still
enumerate and exclude workers from every writable client route and prevent
backend credential or namespace aliases.
