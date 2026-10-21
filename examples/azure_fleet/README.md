# Azure failed-fill fleet regression

Four separate NativeLink worker processes use the real scheduler over gRPC,
filesystem caches, `FastSlowStore`, `VerifyStore`, and `AzureBlobStore`. The Azure
Blob SDK talks to a shared loopback HTTP account. Two workers' private tmpfs
filesystems are filled to real `ENOSPC`, then repaired without restarting them.
The same Rust executable checks the fault, recovery, and retry behavior.

## Run

Linux with unprivileged user/mount namespaces, `unshare`, `mount`, `taskset`,
`sh`, `sleep`, and `wc` is required. Allow several GiB of free memory for the four
private tmpfs mounts. No Azure credentials or Python tools are used.

```bash
cargo test --example azure_fleet
cargo build --example azure_fleet
# Each output directory must not already exist.
target/debug/examples/azure_fleet --seed 2026101700 --scenario control \
  --output /tmp/azure-fleet-control
target/debug/examples/azure_fleet --seed 2026101700 --scenario failed-fill \
  --output /tmp/azure-fleet-failed-fill
```

Bazel also builds the executable and its ordinary unit tests:

```bash
bazel test //examples/azure_fleet:azure_fleet_test
bazel build //examples/azure_fleet:azure_fleet
```

Run the failed-fill command on the regression commit, then repeat it with the
same seed and a new output directory on the fix commit. The executable has no
runtime bug switch: the linked `AzureBlobStore` implementation determines the
behavior. The ordinary tests exercise the HTTP account and acceptance checks;
the explicit fleet run needs the Linux facilities listed above.

## Pass/fail contract

The failed-fill run must observe worker `ENOSPC` on both filled filesystems,
repair them, complete every offered action without a censored client attempt,
and complete a warm-cache sentinel in each original worker process. Scheduler
samples must respect the two-action limit and end with the original workers
holding zero reservations. All workers must shut down cleanly.

It fails at the first new outer download after the same read has observed a
failed send with a broken writer. Each worker records ordered read, attempt,
failed-send and completion events. Immutable SDK context carries the same read
and attempt identity to every HTTP partition, including tasks that outlive the
consuming download. The report joins these identities, checks complete trace
coverage, and separates bytes from forbidden new attempts from bytes belonging
to older SDK work. Missing or inconsistent evidence fails the run.

Both filled workers must actually reach the failed-send boundary. A final 503
cannot hide an earlier forbidden attempt, and legitimate origin retries ending
at the first failed send are not counted as forbidden retries. The old
retry-exhaustion log count remains a diagnostic, not the acceptance condition.
The control must satisfy the recovery checks without encountering `ENOSPC`.

The broken implementation should fail the failed-fill check even when all builds
succeed. The terminal-destination fix should pass the identical check. Byte
counts and elapsed times are observations, never pass/fail thresholds.
`run.json`, the workload manifest, node logs, per-worker `downloads.jsonl`
traces, and cleanup outcomes are retained
on both acceptance success and failure. Setup/session errors also follow the
report and cleanup path after the account has been constructed.

## Workload and measurement

The printed seed fixes the finite input catalogue, bounded log-normal sizes,
file counts and execution holds, Zipf input popularity, Poisson arrivals after
an initial burst, response residual draws, and retry jitter. A 128 MiB input
ensures the failed write can propagate before the whole download is buffered.
These distributions define the stress workload; their parameters are explicit
fixture inputs.
The account's response timing uses the existing Azure response-law fixture;
small metadata responses use a declared one-millisecond delay.

A 256 MiB/s token bucket with a 256 MiB burst admits requested ranges or returns
503. The SDK controls its real partition downloads. Each worker records its
process-visible CPU count. Seeds do not make OS scheduling or HTTP interleaving
deterministic.

The report retains HTTP ranges, response-body bytes, time buckets, all client
attempts, worker process IDs, filesystem allocation, and scheduler snapshots.
Byte ledgers are checked for conservation. Payload means bytes yielded by the
mock HTTP server to Hyper; it is not confirmed cache delivery or Azure billing.
Already-started SDK tasks may continue after the fix prevents a new outer retry.
Their requests retain the original attempt identity even if they begin after a
newer attempt. The most recent HEAD remains a separate observation.

The reproduction adds observation metadata and debug events to the Azure read
path while leaving its retry decisions unchanged. Only the replay client's
HTTP policy exports that metadata as identifying headers. Trace capture adds
measurement overhead; paired revisions use the same instrumentation. Four
processes on one host exercise the distributed paths while sharing a kernel
and loopback network; this is not four Azure VMs.
