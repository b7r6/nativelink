# Local authority memory observations

The worker records an `AuthorityMemoryBreach` when its sampler reports usage
above the ceiling captured from the immutable authority grant, the owned child
is still live, memory is the first selected stop decision, and the child kill
procedure returns successfully. The receipt exposes the exact charged vector,
the configured ceiling and the observed process-group RSS, both in KiB. Private
construction prevents callers from assembling a receipt from ordinary usage
telemetry.

This is an observed policy decision followed by a completed procedure. It does
not prove that this signal was the exclusive physical cause of exit. The actual
exit code, status, stdout, stderr and output files continue through their normal
capture path. A queued sample after an observed exit or failed live probe cannot
turn an otherwise successful action into a memory failure.

The authority stop decision is selected once. Timeout, cancellation or disk
enforcement selected first cannot be relabeled by a later memory observation.
Later termination requests still execute, including when an earlier procedure
failed, while the first result cause remains selected. Legacy actions retain
their existing behavior.

The receipt is retained before namespace closure, output drain and output
upload. Those later operations can fail independently. In particular, a failed
upload leaves the observation available without creating a finished response or
inventing output references. Namespace closure and directory cleanup remain
separate requirements; the receipt alone does not release a reservation or
authorize a retry. Unsupported action implementations return no receipt.

Deterministic cases cover first-stop ordering, rejected live probes, queued
observations, disabled and zero ceilings, pending cancellation and failed kill
procedures. Real namespace cases exercise the same original request with R and
2R grants under SHA-256 and BLAKE3, preserve output bytes, and retain the receipt
across a filesystem upload failure. A test supervisor that sends
SIGKILL to the observed outer action process while its allocation is near the
limit exercises the existing heuristic telemetry while producing no receipt.

The RPC protocol and coordinator classification remain unchanged. No receipt is
sent on the wire, and reports still enter the existing Complete path. A later
classification change requires protocol version 2 before reconnect inventory,
explicit evidence validation and preservation of the full captured response,
including its output graph. This change makes no hard memory isolation or M6
retention claim.
