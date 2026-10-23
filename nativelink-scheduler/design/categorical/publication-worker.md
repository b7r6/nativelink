# Actual worker output through physical publication

This Linux process experiment connects the qualified real-request publication
candidate to the actual namespace worker and the owned-filesystem closure
preparer. It uses one original scheduler state from request admission through
worker completion and publication. It remains test wiring, with no production
service, journal activation, or canonical M5 gateway claim.

## Path exercised

The fixture captures real Action, Command, and empty input-root bytes under
SHA-256 or BLAKE3. Its explicit trusted context fixes one principal and one
namespace execution configuration. Two operations share the real request and
actual execution. Match emits the original request and a real attempt,
reservation, and dispatch-effect identity.

The actual worker receives that dispatch and runs `/bin/sh` inside the existing
namespace fixture. The command produces one output file, stdout, and stderr with
known contents. The fixture requires a quiescent report whose full worker,
epoch, incarnation, execution, attempt, and dispatch-effect identity matches the
original effect. It keeps the exact returned ExecuteResponse for acceptance.
Legacy action-cache and history stores remain empty.

The expected closure has an ActionResult and three blobs. Its three typed edges
are declared directly from the known command outputs and checked against the
actual response and temporary CAS bytes. They are not derived from preparation
output. The fixture installs unready physical catalogue rows, acquires staging
protection and stable write identities, then starts real immutable filesystem
writes. Exact write receipts and whole verified reads precede observations.
Bounded typed preparation checks the entire closure before publication.

Publish accepts the actual worker result into the same original state, installs
the completed-cache head and both operation roots, and releases the real
reservation atomically. The cases check exact terminal payloads, physical reads
under operation and serving protection, a fresh cached response, and checkpoint
reopen of the complete outer authority and physical filesystem. The original
captured request survives reopen unchanged.

## Qualification boundary

The worker first uploads to a temporary legacy CAS outside the new collector
namespace. Copying those known bytes into staged physical generations does not
qualify canonical source-copy pins, worker pins, or gateway admission. The worker
retains its report without an authoritative acknowledgment at the end. No claim
is made about the complete M5 acknowledgment path.

These cases cover an empty input root, one output file, stdout, and stderr.
Directory and Tree traversal, replacement, late writes, and independent recovery
cuts remain covered by the separate physical-publication fixture. This process
experiment does not add those cases by implication.

The tests live under `publication::` in `authority_scheduler_test`. A run with
unavailable namespaces prints `SKIP` and does not qualify the process boundary.
Qualifying receipts require both `PUBLICATION_WORKER_PASS` markers and no such
skip. The existing candidate and physical implementations are reused unchanged
through a test-only Bazel `filegroup`.
