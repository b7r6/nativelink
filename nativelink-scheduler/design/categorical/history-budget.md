# Accepted-history budget experiment

`history_budget` is a bounded M0 refinement for the experimental durable
singleton. It leaves `authority_budget` unchanged. It uses serial ordinary
proposals, actual feature activation, OpenRaft application, redb Immediate
commits, persisted snapshots, and reopen checks. The topology has one authority
and one synthetic worker advertisement. No worker process or storage provider runs.

Run with a new output directory outside the checkout:

```bash
CARGO_BUILD_JOBS=8 cargo build --locked --offline --profile smol -j 8 \
  -p nativelink-scheduler --example history_budget
target/smol/examples/history_budget /tmp/history-budget-run smol "$PWD"
```

The Bazel target is `//nativelink-scheduler:history_budget`. The profile label is
supplied by the runner; it is not proof of compiler settings. Preserve the build
command, exact source and dependency hashes, profile, executable hash, and
host information alongside the output. Freeze inputs between compilation and
measurement. Run after known compilers finish, and retain failed attempts.

## Capacity before adjusted bounds

The first experiment uses the unchanged default limits. It admits 64 distinct
unfinished executions with feature set `{}` and rejects the next submission.
Activating feature 3 at that occupancy must fail after a committed Abort. The
canonical scheduler state must remain identical, preparation must be cleared,
and a subsequent ordinary observation must succeed.

A second authority activates feature 3 before admission. With the same default
512 effect slots, it admits 56 executions and rejects the 57th without changing
state. The fixed allowance grows from eight to nine slots per retained
execution. This cost is exposed before the comparison gives both members of
each pair the same adjusted allowance of `9 * width`.

## Fixed response matrix

The 24 cases cross SHA256 and BLAKE3, widths 4 and 16, stdout payloads of zero,
4096 and 32768 bytes, and feature sets `{}` and `{3}`. Width is the number of
simultaneously admitted executions. Each case captures a valid protobuf
ExecuteResponse, including the empty-output case. Zero stdout bytes still need
protobuf framing; every report records the actual captured response length.
No response is replaced with `None`.

Paired cases use the same requests, responses, limits, worker reports, and
ordinary AC acknowledgements. Complete result JSON size plus 1024 bytes sets
the payload bound, because the admission accounting unit is complete JSON,
not raw protobuf bytes. The additional history component prepays 1024 header
bytes plus 2048 bytes per retained execution. Reports distinguish this credit
from measured component, association, recipe, and prefix sizes.

The harness checks exact captured terminal bytes, history count, namespace,
algorithm, object length and digest. It persists a snapshot with pending
history, shuts down, and requires identical canonical state after reopening.
Reopening reconstructs the trusted admission registry from the closed journal
and admits a later incarnation. It does not substitute a fresh empty registry.
After retention time and synthetic AC acknowledgement, pending history must
still refuse execution retirement. An exact synthetic `HistoryOutcome::Stored`
acknowledgement must leave terminal results stable and release the pin. All
executions, effects and associations then drain, and a second reopen checks
the drained state.

Those acknowledgements are synthetic accepted events. They do not establish
provider storage, protection, collection safety, or delivery latency. The
retirement-refusal proposals are qualification probes and are labeled
separately from ordinary workload commands.

## Reading measurements

Each case retains every proposal latency, log index, command, reply, recorded
event length, and journal-operation counter delta. Proposal latency ends at
the durable apply reply. Commit-time deltas aggregate the journal commits
observed during that proposal; they are not device `fsync` counts. Activation
and both reopen phases are separate from ordinary proposal timing.

After shutdown, the harness reads actual persisted log and metadata envelopes.
It reports their lengths and hashes, the normal proposal bytes by exact log
index, core snapshot bytes, persisted snapshot bytes, and allocated redb file
size. These are different measurements. Retained row bytes do not measure
cumulative disk traffic, write amplification, or replication bandwidth.

The output includes timestamps, observed host load, source hashes, dependency
versions, compiler and machine information, filesystem options, profile label,
and executable hash. This is one fixed pass on a shared development host.
Order is SHA256 before BLAKE3, width 4 before 16, increasing stdout size, and
base before history. Timing differences do not isolate the feature from order,
cache, or changing host load.
CPU scheduling and filesystem caches are not isolated. Report raw
samples and scope with any comparison; do not infer throughput, an SLO,
production admission capacity, shared-publication capacity, or a GC budget.

## Recorded qualification

The October 23 run passed all 24 cases, including 240 completed executions and
120 exact synthetic history acknowledgements. The 12 paired cases have identical
response bytes and limits. Default admission falls from 64 to 56 retained
executions, a 12.5% reduction, while full-base activation refuses after a committed
Abort and preserves the scheduler state.

| Retained executions | Additional prepaid history bytes | Additional core snapshot bytes | Additional ordinary log-envelope bytes |
| --- | --- | --- | --- |
| 4 | 9,216 | 3,264–3,349 | 3,640–3,666 |
| 16 | 33,792 | 12,525–12,856 | 14,479–14,554 |

The ranges span both digest algorithms and all three payload sizes. Ordinary
log overhead excludes the explicit retirement-refusal probes; feature activation
adds another 1,179 bytes in its two persisted control envelopes per history case.
These figures measure retained encodings, not device writes. Large responses
already dominate the base snapshots: the SHA256 width-16 case with 32 KiB stdout
has a 4,228,239-byte pending core snapshot before adding history, versus
4,241,095 bytes with history.

[Receipts](evidence/history-budget/manifest.json) preserve all proposal samples,
source and binary hashes, interrupted attempts, and host observations. Known task
compilers were held during measurement; unrelated soak compilers began during
the run and continued unchanged. The fixed case order and shared load prevent an
isolated timing comparison. The initial harness used a fresh registry on reopen
and correctly failed; the corrected harness uses closed-journal recovery.
