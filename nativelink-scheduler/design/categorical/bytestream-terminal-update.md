# ByteStream backend completion and retry

An uncompressed upload retains its backend future while the client can resume
pending work. Once that backend future completes, the upload guard removes its
registry entry when the stream ends with an error or cancellation. A fresh write
at offset zero can start new backend work; a nonzero resume cannot reuse the
completed future. The retained future records completion itself, including when
the backend succeeds before the client reports an error.

Previously, the guard could park a completed future. Queries then reported stale
progress, and retries could encounter a disconnected receiver or panic while
polling a completed async function. The repair wraps the retained future in
`Fuse` and checks its terminal state before parking it. A pending backend remains
available for exact resume. Both fixed and legacy uncompressed streaming paths
use this guard; compressed upload handling is unchanged.

The [focused evidence](evidence/bytestream-terminal-update/focused/README.md)
contains six actual HTTP/2 fixtures. Original source passes the pending-resume
control and fails five terminal-work cases, including two observed panics while
polling completed futures. Corrected source passes all six. Two compiled faults separately
retain completed work and discard pending work; each fails its intended progress
assertion. The restored six-test executable exactly matches the initial fixed
executable. Independent review checks the raw artifacts without claiming a
reviewer Rust rerun.

One fixture deliberately supplies a backend that returns success before EOF.
It checks ownership when the client subsequently fails; it does not establish
that such a backend stored a valid value. The pending-work fault fails its first
progress assertion, so its later resume assertion is not claimed reached.

The [combined root run](evidence/temporal-terminal-root-integration/manifest.json)
executes twelve Bazel targets: 421 Rust tests pass, two remain ignored, and the
server builds. This covers the temporal owner, filesystem prerequisites, fixed
listener, legacy ByteStream, compression and fixed-digest suites together.
A subsequent fixture-only qualification cleanup passes all 45 listener tests
with one ignored case and strict all-target service Clippy. Those tests overlap
the combined run; their counts are not additional unique coverage.

No retry is fabricated after backend completion. The caller must restart at zero
when no matching upload remains. This change does not bound the aggregate upload
registry, impose a backend completion deadline, verify content integrity, or
provide a durability acknowledgement. The active-upload metric decrement is
source-reviewed; these fixtures do not independently assert the metric value.
