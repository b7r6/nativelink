# Real worker fleet / shared Azure account fixture

This example exercises separate NativeLink worker processes, a real NativeLink
scheduler over gRPC, and the real Azure Blob SDK over loopback HTTP. The shared
HTTP service models an Azure account. Every worker has its own filesystem cache
and mount namespace. A controlled filesystem fill reaches real `ENOSPC`; removing
the filler repairs that same filesystem without replacing the worker.

The question is narrow: **after a cache destination fails, does the Azure store
start more origin downloads that cannot repair that destination, and what does
that cost other workers sharing the account?** The comparison changes only the
terminal-destination guard in `AzureBlobStore`. The worker registration/lost-kill
bug (N1), pod eviction, Redis, and a probabilistic worker wedge are not injected.

## Recorded paired result

The October 16 batch ran ten paired seeds per scenario, alternating the first
condition: 40 completed invocations on one host with four worker processes per
invocation. These are measured emulator HTTP body bytes, not Azure billing.

| Scenario / measure | Baseline mean ± SD | Fixed mean ± SD | Paired fixed − baseline, 95% CI |
| --- | ---: | ---: | ---: |
| Failed fill: payload, MiB | 1425.6 ± 44.6 | 1196.4 ± 78.4 | −229.2 [−272.4, −186.0] |
| Failed fill: work drain, s | 41.65 ± 2.62 | 6.33 ± 1.29 | −35.32 [−37.00, −33.64] |
| Failed fill: repair to last sentinel, s | 40.61 ± 2.61 | 5.29 ± 1.29 | −35.32 [−37.00, −33.64] |
| Control: payload, MiB | 969.2 ± 75.3 | 956.4 ± 86.5 | −12.8 [−59.1, 33.5] |
| Control: work drain, s | 5.23 ± 1.09 | 5.00 ± 0.97 | −0.23 [−1.01, 0.55] |

Every intended work item completed: 836 across the batch, with matching output
paths/digests in every pair. All 160 sentinel actions completed in the original
worker processes with warm-cache reuse. Every final scheduler snapshot retained
the original worker IDs with zero reservations; sampled counts never exceeded
two. There were no failed or censored client attempts, missing reports, or
requests outside the response fit's support. Failed internal preparation and
download attempts remain visible in the worker logs and request ledgers.

Both failed-fill arms reached `ENOSPC` on all 20 deliberately filled filesystems.
The baseline logged retry exhaustion against a known closed writer on all 20;
the fixed arm logged none. The fixed arm still had legitimate admission retries
and already-started range transfers. Its faster completion came with **more**
throttled GETs (means 2.8 → 17.0) and a higher 100 ms peak body-output rate
(1310.9 → 1424.8 MiB/s). The change reduces total bytes and recovery delay in this
fixture; it does not improve every traffic metric. Control difference intervals
include zero and are not an equivalence test.

The [failed-fill figure](results/failed-fill.svg) and
[control figure](results/control.svg) retain all paired seeds. Their upper traces
use the upper-median baseline payload selection specified before collection, not the largest
treatment effect. [The machine-readable summary](results/summary.json) contains
all 40 run hashes/metrics, full distribution summaries, paired intervals, and
scheduler audits. [The evidence archive](results/evidence.tar.gz) contains every
run/workload ledger, worker/controller log, independent account and lifecycle audits,
batch/build metadata, exact frozen Rust/Cargo/response-fixture snapshots, and a
SHA-256 inventory. Binaries and duplicate generated node configurations are
omitted. The published Cargo manifest differs from the measured snapshot only
in formatting and feature-list order; the archive preserves the exact input.

Recompute the report without an Azure account or a Rust build:

```bash
FLEET_EVIDENCE="$(mktemp -d)"
tar -xzf examples/azure_fleet/results/evidence.tar.gz -C "$FLEET_EVIDENCE"
(cd "$FLEET_EVIDENCE" && sha256sum --check --quiet SHA256SUMS)
python examples/azure_fleet/analyze.py "$FLEET_EVIDENCE" \
  --output "$FLEET_EVIDENCE/report"
```

## Topology and observation boundary

The controller starts four OS processes. Each uses `LocalWorker`, the normal
action lifecycle, `FilesystemStore`, `FastSlowStore`, `VerifyStore`, and
`AzureBlobStore`. Each worker advertises a two-action limit to the real scheduler;
the harness samples its reservation accounting every 200 ms. Node platform
properties hold placement fixed across the paired arms.
Executable inputs exercise actual `.exec` materialization; commands execute and
upload their results through the normal worker path.

All workers share one HTTP account with a token-bucket admission budget and
range-response timing: a **256 MiB/s** refill rate and **256 MiB** burst allowance.
An earlier pilot used a 64 MiB burst, which refused requests before the intended
failed cache write was exposed; that pilot is retained separately. The selected
burst admits the initial SDK waves so the destination failure can be exercised.
Its request ledger counts actual body chunks yielded to
the HTTP server. This is **emulator body production**, not Azure billing, network
wire bytes, or bytes successfully consumed by the cache. TCP buffering can place
yielded bytes ahead of client consumption. Already-started SDK range tasks may
continue even after the terminal retry fix stops subsequent attempts.

The account checks conservation between its range and time-bucket byte ledgers.
Admission respects `burst + rate × elapsed`; an instantaneous body-output rate
can exceed the nominal admission rate because of burst tokens and overlap.
Repeated bytes mean a previously yielded range for the **same worker and blob**.
A first download on another worker is a separate cache replica, not a repeat in
this metric. Repeated bytes alone do not establish futile or preventable work.
The ledger also links each GET to the latest observed HEAD sequence for that
node/key. In this fixture, a new Verify-wrapped fill issues a HEAD, whereas an
Azure read retry does not. That sequence is an observed boundary, **not universal
fill ownership or proof that a repeated range was futile**.

## Workload and calibration

The entire manifest is generated before either arm runs, hashed, and saved.
Seeds determine the finite blob catalogue, input popularity, arrival schedule,
file counts, execution holds, and response residual draws. The workload uses a
finite Zipf input catalogue, bounded log-normal sizes/counts/holds, and a fixed
initial burst followed by Poisson arrivals. These are declared stress workload
families, **not fits to the customer's build trace**. Work does not disappear when
one arm is slower: every scheduled action receives an outcome, including failure
or censoring. Failed work receives at most one explicit client resubmission after
disk repair; this recovery policy is identical in both arms. Each client attempt
is retained. Its counts are distinct from internal scheduler retries and Azure
download retries.

One catalogue entry is a declared 128 MiB stress anchor; the other seven retain
the bounded size distribution. The anchor exceeds the intermediate channel
buffers so the SDK can still be producing data when a cache write fails. Healthy
workers have a 512 MiB logical cache and a 1 GiB private filesystem, sufficient for
the generated catalogue and its executable variants. Smaller-blob pilot runs are
retained separately and are not mixed with this workload's paired batch.

Range response timing comes from
[`azure_download_response.json`](../../nativelink-store/tests/fixtures/azure_download_response.json).
The fixture records the original fit/data SHA-256 values, 64,810 complete reads
over 25 capture runs, and 1,728 residual observations from four runs. Its fitted
boundary is client/proxy complete-body time, including network time. It is not
Azure server-only latency. The 4 MiB response law is:

```text
milliseconds = 6.2384478 × 5^1.4462064 × (1 + local_inflight)^0.20929754
               × exp(empirical_log_residual)
```

The fitted overlap support is 1–32. The account's limits, burst capacity, small
metadata responses, private tmpfs size, and fault duration are explicit fixture
conditions. They are not inferred Azure limits. Refusals result from capacity
admission, rather than a sampled throttling percentage. Independent residual
draws do not reproduce the original within-run correlation. Requests outside the
fitted 4 MiB / 1–32 overlap support are counted as `timing_outside_fit_requests`;
larger overlaps extrapolate the law instead of inventing a per-node Azure limit.
Short metadata replies are also outside the fitted support.

The original fit's goodness-of-fit evidence is a whole-run bootstrap of held-out
90% prediction-interval coverage: 0.8746, 95% interval [0.8343, 0.9150]. Only five
heterogeneous held-out runs support that result. The descriptive KS distance is
0.1383; no independent-request p-value is valid for correlated requests. These
limits are not removed by collecting more simulation seeds.

NativeLink retry settings are six retries, delay 0.3 s, jitter 0.5. SDK automatic
retries are disabled and the separate throttle retry budget is zero, so throttle
responses consume the ordinary attempt budget. The throttle delay floor remains
2 s and its maximum requested wait is 60 s; the retry loop waits the larger of its
ordinary randomized backoff and that throttle delay. This is not a claim that all
errors follow only the unmodified exponential schedule. Record each
worker process's available CPU count: SDK range concurrency depends on that
figure, not the machine's advertised core count. All four workers share an
affinity mask containing the first eight CPUs available to the controller (or
fewer if its inherited mask is smaller). These are not four dedicated CPU
allocations. Seeded inputs do not make OS
scheduling or TCP completion order deterministic.

## Build and paired runs

This is a Linux harness requiring unprivileged user/mount namespaces, `unshare`,
`mount`, and a shell. It needs no Azure credentials and makes no cloud requests.
Keep each child's tracing output in a regular file; piping unconsumed log output
can introduce a different blocking failure.

Build two binaries from the same harness tree. The baseline removes only
`|| writer.is_pipe_broken()` from the terminal read-error branch; the fixed binary
includes it. Preserve the source diff, commit IDs, and binary SHA-256 values with
the result set. The historical two-commit reproduction/fix is
[`b7r6/nativelink#69`](https://github.com/b7r6/nativelink/pull/69): reproduction
`d6257655c39ae99697385a63bfdae05de861d26b`, fix
`b4bbc7cd32de7908800acc28f0ae8a8fd14e408f`. This harness extends that tree; it must
not be labelled an exact v1.7.6 release binary. The standard-library Python driver
builds both arms, restores the original source, then runs the preregistered batch:

```bash
python examples/azure_fleet/run_pairs.py --output /tmp/fleet-confirm --build-only
# Inspect the build evidence and perform separately labelled smoke runs first.
python examples/azure_fleet/run_pairs.py --output /tmp/fleet-confirm --skip-build
```

The defaults are seeds 2026101800–2026101809 and both `control` and `failed-fill`:
40 sequential invocations. `CARGO_TARGET_DIR` is inherited; builds use offline
Cargo with four jobs. `--first-seed`, `--count`, and `--scenarios` select a batch;
fewer than ten seeds require `--pilot`. Use a separate output directory for pilots.
The driver never resumes or overwrites an existing batch.
`--skip-build` verifies saved binary hashes and refuses changed Rust/Cargo/fixture
sources, so a workload change after a pilot requires a fresh build directory.

Coordinate exclusive source access during the two builds. The driver requires
exactly one occurrence of the guard, saves the original source and one-line diff,
and restores the source in `finally`, including a failed/interrupted build. It
compares Rust harness, Cargo, and response-fixture hashes across builds and records
`FLEET_VARIANT`, `FLEET_BASE_REVISION`, and `FLEET_AZURE_SOURCE_SHA256` in each
binary. If another process edits the source concurrently, it preserves that edit,
reverses only its own unique changed line when possible, and fails for inspection.
It never resets a branch or discards unrelated changes. Copied binaries have only
debug sections stripped when `strip` is available; pre/post hashes and the exact
command are retained. This reduces per-run binary hashing overhead without
changing executable code. A manually built binary without compiled provenance is
an unverified pilot and only accepted by the analyzer with `--exploratory`.

`batch.json` records **all planned invocations before execution**, including
process failures, controller timeouts, absent reports, and runs not started after an
interruption. Each invocation keeps its stdout/stderr, load observations, command,
exit status, and elapsed time. A failed invocation does not disappear or get
silently rerun; the batch exits nonzero if any invocation fails. Controller
timeouts terminate only that invocation's process group. Build logs, source
hashes, and binary hashes are in `build.json`.

For each of at least ten distinct seeds, run both binaries with the same scenario
and seed. Alternate which condition runs first, and keep arms sequential on the
shared host. `--pair-order` records the position within that seed's pair:

```bash
mkdir -p /tmp/fleet/2026101800
/path/to/azure_fleet-baseline --arm baseline --scenario failed-fill \
  --seed 2026101800 --pair-order 0 --output /tmp/fleet/2026101800/baseline
/path/to/azure_fleet-fixed --arm fixed --scenario failed-fill \
  --seed 2026101800 --pair-order 1 --output /tmp/fleet/2026101800/fixed

# Next seed: fixed first, then baseline. Also collect paired --scenario control.
```

`run.json` is the analysis contract. It contains the schema version, seed,
scenario, binary and workload hashes, pair order, wall-clock start, elapsed time,
host load, process-visible CPUs, fault/repair times, every job outcome, same-worker
sentinel evidence, and the shared account's full request/time-bucket ledgers.
`started_s` on a job is the client's submission time, not command execution start.
`planned_arrival_s` retains the generated phase-relative schedule; `arrival_s`
includes the phase's actual account-relative origin. The compiled `build` record
binds the arm, base revision, and Azure source-file hash to the executable rather
than relying only on the command-line label. Request, fault, job, and scheduler
sample `*_s` timings and `duration_s` use the account's monotonic origin, created
**after** workload generation. `start_unix_s` is a separate wall-clock reading
taken **before** workload generation; adding it to account-relative seconds does
not give an exact UTC timestamp. Worker logs and REAPI lifecycle timestamps use
their own absolute wall-clock readings. The plot aligns account-relative fault
or workload origins; it does not infer an absolute cross-clock offset. Keep raw
worker logs and the saved workload alongside this file.

## Analysis

Install `scipy` and `matplotlib` into an analysis environment. The analyzer does
not fit a curve to the runs or synthesize missing bytes:

```bash
python examples/azure_fleet/analyze.py /tmp/fleet-confirm --output /tmp/fleet-report
```

It checks matching manifests, per-process CPU counts, account settings, complete
range ledgers, opposite within-pair orders, alternating first conditions, and at
least ten complete seed pairs per scenario. Compiled arm/source provenance and
consistent binaries within each arm are required for a full batch.
The invocation plan must be complete and the batch finished. A relocated archive
is resolved only against its own `runs/<scenario>/<seed>/<arm>/run.json` files;
recorded absolute paths remain provenance and are never a fallback for missing
evidence. The build metadata hash and each run's compiled identity are checked.
`--exploratory` permits smaller pilot
batches or missing arms and labels them explicitly. It never permits mismatched
workloads or broken byte accounting. A nearby `batch.json` is automatically
included, so failed invocations without `run.json` cannot silently vanish from the
inventory. `--batch-manifest` supplies it explicitly when analyzing selected paths.

Outputs are `summary.json`, `summary.compact.json`, `summary.txt`, and an SVG/PNG
figure per scenario. The compact summary retains all per-run metrics and source
hashes, omitting repeated node/load details already present in raw evidence. The
figure shows a representative pair chosen by median **baseline** bytes before
examining its treatment difference; the bottom panels retain every paired seed.
The upper trace is not an average of unrelated arrival clocks. Every quantitative
summary uses independent seeded runs as replicates, with mean, sample SD,
Student-t 95% interval, median, p95, and p99. Differences are **fixed minus
baseline** on paired seeds. Run-level success/recovery proportions use Wilson
intervals; correlated actions and requests are not pooled as independent trials.
GET/action fractions are summarized as per-run rates with run-level intervals.
Sample p95/p99 estimates have little tail resolution at ten runs.

Failed and censored work stays visible. Payload per completed action is reported
only for runs completing the entire intended workload; its numerator includes
warmup and sentinel overhead. A lower byte total is not evidence of an efficiency
improvement if successful work or same-process recovery differs. Conditional
timings exclude runs that did not recover and must be read beside their failure/censoring
counts. Failed and censored client attempts also remain in the report when their
final work item succeeds after resubmission. A pair is eligible for the accepted
work comparison only if both arms meet the harness acceptance criterion, have no
censored client attempts, preserve warm-cache reuse, and return identical output
paths/digests for every intended work item. `accepted: false` remains a measured
outcome, with its run-level proportion reported. Repair-to-last-sentinel time
includes workload drain and bounds recovery
from above; it does not identify the instant the original worker first became
reusable. The report also preserves payload and GET counts between the pre-probe
snapshot and the final report. That sentinel window may include surviving
background work; it is not automatically all attributable to the probe.

The analyzer separately audits scheduler snapshots: the final post-sentinel
sample must contain the original worker IDs with zero reserved actions, and all
sampled per-worker counts must stay within the advertised two slots. Failures
remain in the report and exclude a pair from the accepted-work comparison.
Sampling does not bound activity between observations or reveal physical work
missing from scheduler accounting.

Measurement acceptance is not a teardown guarantee. `run.json` is written before
worker shutdown, and the controller currently discards shutdown results. An
abnormal controller kill can bypass normal descendant cleanup; workload commands
are bounded `sleep` (at most 100 ms) plus `wc`. Check process/PID cleanup separately
after a batch. Censored-run payload is the amount observed within its measurement
window, not an estimate of unobserved eventual transfers.

This experiment can reproduce and price the composed failure under declared
conditions. It does not establish which mechanism caused the customer's plateau,
how many production bytes it contributed, or why a historical worker stayed
silent for hours. Four processes on one host share a kernel, CPU scheduling, and
loopback network; they are not four Azure VMs. The scheduler state is in memory,
and the emulator implements the Blob operations exercised here, not the entire
Azure service.
