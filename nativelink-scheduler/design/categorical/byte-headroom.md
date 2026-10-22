# Bounded snapshot and finalization bytes

Status: reviewed design proposal, 2026-10-22. This document does not claim the
admission credit or journal metadata caps have been implemented. The current
bounded payload counter and 64 MiB snapshot writer prevent individual oversized
encodings; they do not yet reserve the bytes needed to finish admitted work.
Implementation follows the coordinated memory-policy checkpoint.

## Pure authority credits

Use a fixed **14 MiB** maximum canonical pure-state snapshot and a fixed
**512 KiB** maximum configured complete-payload bound. The default payload
bound remains 64 KiB. This is a byte
bound, not a bound on allocator RSS, temporary copies, reply clones, or total
journal disk usage. Retain the existing three-total-attempt and eight-effect
headroom per execution. All arithmetic below is checked; overflow refuses
admission and cannot become a saturated usable credit.

Let:

- `O`, `E`, and `W` be current retained operation, execution, and worker counts.
- `P` be `limits.max_payload_bytes`, the encoded bound on a complete
  `ResultPayload`, including captured response bytes and normalized fields.
- `RO` be the sum of exact compact JSON byte lengths of every operation's
  immutable `original_request`.
- `RE` be the sum of exact compact JSON byte lengths of every execution's
  immutable `request`.
- `WP` be the sum of exact compact JSON byte lengths of every worker's current
  property map.

Reserve:

```text
C = RO + 3*RE + P*(O + 2*E) + WP + 1024*(O + 15*E + 2*W + 1)
C <= 14*1024*1024
```

Configured count limits remain upper bounds, not a promise that all maxima can
be reached simultaneously. For example, 256 operations and 64 executions with
64 KiB maximum results would reserve 24 MiB for results alone and must encounter
byte backpressure earlier. With one operation per execution, 64 executions and
1 KiB requests reserve about 13.3 MiB when worker properties are small; large
property maps or additional coalesced handles consume the remaining credit.

Requests and property maps are counted with a bounded serialization sink;
counting must not allocate an oversized encoded copy before rejecting it.
Measure encoded bytes, including escaping, not Rust string lengths or raw
protobuf lengths.

The variable-payload terms cover every possible retained copy:

| Term | Copies covered |
| --- | --- |
| `RO` | Each operation's distinct accepted request, including its own ingress metadata and queued timestamp. Coalescing does not remove this copy. |
| `3*RE` | The execution request, its dispatch request, and a conservative additional action-key copy for lookup/publication. An encoded action key is a subtree of the encoded request and is no larger. |
| `P*O` | Each operation's terminal result, even when many handles share one execution. |
| `P*E` | Each execution's terminal result. |
| `P*E` | A pending accepted-result publication. Failed results consume their prepaid terminal copies but create no publication. |
| `WP` | The current durable worker property maps. Numeric capacity changes do not add variable payload. |

An execution cannot retain multiple dispatch payloads. Revocation removes the
old dispatch before creating a bounded cancellation intent, and acceptance
removes the dispatch before publication. The three-request term is deliberately
conservative across lookup, dispatch, and publication phases.

This requires restore invariants as well as reachable transition behavior.
Review found that the earlier restore checks validated each publication's
result association but permitted duplicate valid `PublishResult` intents under
different IDs. Such a restored state could contain unpaid result copies. The
correctness fix requires at most one publication per execution and bounds each
execution's total effects by its eight reserved rows, with a corrupt-snapshot
fixture. A missing publication is valid after acknowledgment; duplicate
publications are not.

The fixed rows cover identities, decimal map keys, punctuation, field names,
resource vectors, timestamps, deadlines, phases, optional scalar values, and
terminal reason tags:

```text
O rows                 operation records
15*E rows              1 execution + 3 attempts + 3 reservations + 8 effects
2*W rows               1 worker + at most 1 hold per worker
1 row                  snapshot wrapper, state fields, map wrappers, limits
```

No variable diagnostic string lives outside the separately bounded request,
result, or worker properties. `ResourceFailureReason` is a closed enum. The
1024-byte allowance applies to the current exhaustively reviewed schema; adding
fields or variants requires updating the proof and serialization guards.

An initial compact-JSON shape audit gives these upper bounds. It substitutes
20-digit unsigned values, the longest signed priority, present optional scalar
values, and the longest enum encoding. Variable subtrees paid above are
excluded; decimal map keys and a trailing comma are included. These are design
calculations, not a substitute for the planned Rust serializer assertions.

| Fixed row | Bytes |
| --- | ---: |
| Operation | 344 |
| Execution | 527 |
| Worker | 542 |
| Attempt | 370 |
| Reservation | 275 |
| Hold | 186 |
| Lookup effect | 143 |
| Dispatch effect, including its immutable charged vector | 388 |
| Cancellation effect | 229 |
| Publication effect | 160 |
| Snapshot/state global fields and empty map wrappers | 579 |

## Transition and restore obligations

`Submit` must check the prospective credit before changing the operation
watermark, allocating an identity, joining an execution, or creating lookup
effects. A join adds one original request, one maximum terminal payload, and
one fixed operation row. A new execution additionally adds its execution
credits. An individually valid request that would exceed aggregate credit is
`Overloaded`; it leaves state unchanged.

`Register` must check the prospective property-map replacement and any new
worker rows before revoking attempts for a new incarnation. Replacing a worker
does not increase `W`, and replaces rather than adds its old property credit.
Stale identity rejection still precedes this admission calculation. A refusal
cannot partially revoke authority or change properties.

All remaining accepted transitions fill prepaid bounded rows or payloads, or
change fixed-size fields. In particular, memory growth changes only fixed
numeric vectors; it does not mutate request bytes. Result acceptance cannot
need fresh byte admission after a worker has already executed. Cancellation,
quiescence cleanup, effect delivery, and terminal completion cannot be blocked by
an exhausted admission pool. Retiring an execution removes its retained
operations and releases their credits. Terminal records retain their credits
until retirement, even if their current concrete payloads are smaller.

Recompute credit from durable facts instead of introducing another mutable
counter that could drift. Restore must reject raw pure snapshots larger than
14 MiB before parsing, then validate the same checked credit and existing
relational invariants. Canonical snapshot serialization must fit the bound as
a consequence of these invariants. A bounded encoder should still enforce the
limit as defense against a future omitted field.

The proposed capability is pure-state snapshot version 4, following version 3's
memory-target semantics. This prevents an older writer from silently continuing
admission without the new byte rule. This proposal makes no rolling-upgrade, production
migration, or activation claim.

## A completed result must fit a commit command

Snapshot capacity alone is insufficient. A configured payload bound near or
above the journal's 1 MiB batch limit could admit an operation whose later valid
completion cannot fit even one `RecordedEvent` plus batch framing. The fixed
512 KiB complete-payload ceiling prevents that mismatch. Reject larger
configured values at fallible initialization and validate restored limits;
snapshot installation cannot bypass the rule. The pure reducer must enforce
the same admission contract. Its currently infallible constructor requires an
explicit initialization/API decision when implementing this validation.

A compact-JSON shape audit gives these maximum singleton command frames after
excluding the separately bounded payload value: `Complete` 193 bytes,
`MemoryFailure` 368, `LookupResult` 131, `Submit` 129, and `Register` 422. These
figures include batch/event wrappers and maximum-width epoch, identities,
resources, optional peak, and version. Reserve **1024 bytes** for framing and
guard every current payload-bearing variant using the real Rust serializer.

```text
maximum singleton command <= 512 KiB + 1024 bytes
                          = 525312 bytes
                          < 1 MiB
```

Larger multi-command batches must still be split or refused before submission;
the proof guarantees that each admitted lifecycle can submit its own finalizing
command. Raw protobuf capture limits are separate: a 256 KiB captured response
can encode to more than 256 KiB in its complete JSON DTO and must satisfy the
configured complete-payload bound as well. This remains an explicit
experimental size policy, not a qualified production response envelope.

## Journal envelope proof

The journal additionally needs these separately validated facts:

- Canonical serialized Raft `Membership` is at most **1 MiB**. Validate every
  membership entry before any append/apply batch mutation and validate restored
  applied state. Snapshot installation must not bypass this bound.
- A snapshot ID is at most **1024 UTF-8 bytes**. Validate restored and incoming
  snapshot metadata as well as generated snapshots.
- Envelope version, required section, checksum, epoch, and fixed Raft metadata
  have their existing closed representations. Membership is the only variable
  node-address collection in this proof.

Let `S <= 14 MiB` be canonical pure-state JSON. Serialized JSON contains no raw
control bytes. Encoding it as a JSON string therefore costs at most `2*S + 2`:
quotes and backslashes double; other bytes do not expand. Encoding that string
through the outer envelope costs at most `4*S + 6`.

`SavedSnapshot` contains two copies of membership, one in applied state and one
in snapshot metadata. With raw canonical membership `M <= 1 MiB`, outer
envelope escaping costs at most `4*M` for those two copies. A 1024-byte snapshot
ID can contain control bytes: allow sixfold JSON escaping and another doubling
at the envelope. Together with fixed IDs, wrappers, required-section text,
checksum, and punctuation, reserve a conservative **16 KiB** metadata margin.

```text
encoded saved snapshot <= 4*S + 4*M + 16 KiB
                       <= 56 MiB + 4 MiB + 16 KiB
                       = 60.015625 MiB
                       < 64 MiB
```

Applied-state metadata without the duplicate snapshot membership is smaller.
The 64 MiB bounded receive/write buffer remains an independent check. Neither
the envelope calculation nor that buffer claims a 64 MiB process RSS limit.

The initially suggested 15 MiB pure-state cap leaves only 4 MiB for metadata.
The existing log-record cap alone permits two membership copies to exceed that
margin, and it does not constrain membership arriving via snapshot install.
The 14 MiB cap plus explicit membership and snapshot-ID admission closes those
specific gaps.

## Qualification required before claiming the bound

Candidate and independent model tests must cover admission immediately below
and above the credit limit, many distinct coalesced request payloads, maximum
terminal copies after admission, worker replacement rejected before revocation,
escaping-heavy strings, numeric overflow, release on retirement, and restart
before finalization. Fixed-row tests must serialize every current variant using
maximum-width scalars and assert its unpaid bytes fit 1024. Generated histories
must compare actual canonical snapshot bytes to reserved credit throughout.

Journal tests must cover large membership in append/apply/install paths,
multi-entry atomic refusal, control characters in bounded snapshot IDs, and
near-limit core state followed by result completion, snapshot creation, restore,
and a suffix. Tests must distinguish aggregate admission refusal from an
individual malformed/oversized payload. Previous small synthetic throughput
measurements do not qualify the maximum-size path.
