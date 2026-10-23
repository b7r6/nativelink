# Explicit memory evidence in experimental protocol 2

Protocol 2 requires capability 7, `OBSERVED_MEMORY_BREACH`, before either side
accepts registration. The coordinator validates that negotiation before Register
or reconnect inventory can mutate authority. Older peers could silently treat a
new field as ordinary completion, so an additive capability on protocol 1 is
insufficient. The durable journal format and core memory retry policy are
unchanged.

The recipient attaches `AuthorityMemoryBreach` beside the captured failing
`ExecuteResponse` only after upload/get_finished_result, independently confirmed
process-domain quiescence and successful cleanup. It carries the exact original
reservation identifier, full charged resource vector, configured KiB ceiling and
observed KiB process-group RSS. Upload failure, uncertain domain closure, success
and the bounded oversized-report fallback carry no marker. Ordinary usage
telemetry, including heuristic `KilledMemory`, cannot create this assertion.
If the actual response cannot fit the recipient report bound, the existing
fallback is a nonzero `InternalError` with no graph, usage or memory marker.
That bounded ordinary size failure loses automatic memory retry and the full
output graph; it is an explicit qualification limitation, not MemoryFailure.
The broader graph/admission gate remains open.

The coordinator first checks the current session, epoch, worker incarnation,
execution, attempt and original delivery effect. Unauthorized old attempts can
only report idempotent cleanup. For a current authorized attempt, explicit
evidence is validated before acknowledgment, progress or decline. The original
reservation must remain unreleased; identifier and every charged dimension must
match. Memory and ceiling must be positive, the ceiling must cover the charged
bytes rounded upward to KiB, and the observed sample must exceed the ceiling.
The report must be quiescent and contain an actual `ActionResult` with a nonzero
exit code or non-OK status. Missing, internal-error, decline and success-only
results refuse without mutation. Neither mutable retry targets nor diagnostic
resource usage can stand in for the original reservation.

The adapter captures the full response exactly once, preserving stdout, stderr,
output digests, metadata and the actual status. Both the protobuf payload and its
committed JSON representation must fit the existing bounds. Once a bounded
marked report reaches the coordinator, a captured payload that cannot fit
refuses before mutation; the coordinator cannot erase its evidence into the
ordinary overflow fallback that omits the graph.
Eligible evidence enters the existing `MemoryFailure` event with `quiescent:
true` and no peak-memory hint: sampled RSS is not an exact peak. The existing
core uses the positive original charge, current eligible capacity and attempt
budget to choose growth. The original ExecuteRequest and CAS Action/Command
bytes stay unchanged across retries.

A retry produces no accepted-history association. Final resource failure keeps
the exact captured response and its graph for WaitExecution and accepted
history, including recovery, and cannot create an action-cache publication
intent. These guarantees are qualified with actual HTTP/2, real namespace
processes, both supported digest functions, changing advertised capacity,
reconnect inventory, stale reports and malformed markers. This remains an M4
qualification using temporary stores without garbage collection; it does not
claim M6 object retention or a production construction path.
