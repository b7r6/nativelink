# Bounded snapshot and finalization bytes

Status: implemented in the experimental authority and journal, 2026-10-22.
Admission reserves finalization bytes before changing authority. Pure-state
snapshots use version 5; journal membership and snapshot metadata have separate
bounds. Candidate, independent-model, and real-journal boundary tests exercise
the contract below. Production activation and throughput qualification remain
separate requirements.

## Pure authority credits

The authority uses a fixed **14 MiB** maximum canonical pure-state snapshot and a fixed
**512 KiB** maximum configured complete-payload bound. The default payload
bound remains 64 KiB. This is a byte bound, not a bound on allocator RSS, temporary copies, reply clones, or total
journal disk usage. It retains three total attempts and eight effect rows
per execution. All arithmetic below is checked; overflow refuses
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
counting does not allocate an oversized encoded copy before rejecting it.
The counter measures encoded bytes, including escaping. Rust string lengths
and raw protobuf lengths are not the accounting unit.

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
calculations. Rust serializer assertions independently verify that every typed
row and current terminal/effect variant fits its 1024-byte allowance.

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

`Submit` checks the prospective credit before changing the operation
watermark, allocating an identity, joining an execution, or creating lookup
effects. A join adds one original request, one maximum terminal payload, and
one fixed operation row. A new execution additionally adds its execution
credits. An individually valid request that would exceed aggregate credit is
`Overloaded`; it leaves state unchanged.

`Register` checks the prospective property-map replacement and any new
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

Credit is recomputed from durable facts, with no additional mutable accounting
ledger. Restore rejects raw pure snapshots larger than 14 MiB before parsing,
then validates the same checked credit and existing relational invariants.
Canonical snapshot serialization fits the bound as a consequence of these
invariants. A bounded encoder also enforces the limit as defense against a
future omitted field. Encoding borrows the state and grows its output buffer
within the fixed limit.

The byte capability introduced pure-state snapshot version 4, following version
3's memory-target semantics. Version 5 additionally retains the original dispatch
effect identity on every attempt, even after acceptance consumes its outbox
intent. The maximum-width fixed-row serializer guard includes this field.
These historical byte checks do not qualify rolling upgrade or production
migration. The later experimental activation protocol is described separately
in [format-activation.md](format-activation.md).

## Activated recipient-retirement credits

Committed feature 2 introduces the optional receipt-retirement component and
pure-state snapshot version 6. Before activation, the component is absent and
version 5 bytes remain unchanged. Activation creates one dirty stream per
current worker; it does not add an unfunded reserve to an already admitted pool.

Each worker has at most one immutable pending certificate. Its retained bindings
pin their existing attempt records until exact acknowledgement or accepted
incarnation replacement. A released attempt named by a pending certificate
therefore cannot retire early and remove the row paying for that binding.
The attempt and its single retained binding fit together within the existing
1024-byte attempt allowance. The worker, optional scheduling hold, acknowledged
floors, and pending certificate header fit together within the existing two
1024-byte worker allowances. Maximum-width serializer guards check both groups;
restore checks their actual sizes. The optional component wrapper fits in the
existing global-header allowance. The aggregate credit equation is unchanged.

Bindings are limited to 1024. Their protobuf certificate fits 64 KiB; the
separately bounded compact-JSON certificate fits 256 KiB because it includes
field names. An acknowledgement command pays that certificate as its bounded
payload and retains the existing 1024-byte frame allowance, so it fits the
journal's 1 MiB batch. This certificate bound is independent of the configured
request/result payload bound. It does not permit larger requests or results.

Issuance also reserves every admitted execution's remaining finalization IDs
before allocating its certificate from the shared effect counter. Oversized
certificates and exhausted identity headroom refuse atomically. A fixture with
1026 uncertain attempts retains all charges and refuses issuance; two actual
cleanup transitions allow the same state to issue exactly 1024 exceptions.
Acknowledgement clears only the immutable retirement intent and its pins. It
cannot release a reservation or clear newer releases waiting for another
certificate.

An unreachable recipient may hold a pending certificate and its bounded pins
indefinitely. These checks establish space safety and eventual compaction under
delivery and acknowledgement, not a promise that disconnected workers keep
making progress. Production worker counts and traffic bounds remain unqualified.

## A completed result must fit a commit command

Snapshot capacity alone is insufficient. A configured payload bound near or
above the journal's 1 MiB batch limit could admit an operation whose later valid
completion cannot fit even one `RecordedEvent` plus batch framing. The fixed
512 KiB complete-payload ceiling prevents that mismatch. `State::new` returns
`Result<State, String>` and rejects larger configured values. Zero ceilings
remain valid and can prevent admission. Restore validates the same limit, so
snapshot installation cannot bypass the rule. Journal initialization validates
limits before creating a file.

A compact-JSON shape audit gives these maximum singleton command frames after
excluding the separately bounded payload value: `Complete` 193 bytes,
`MemoryFailure` 368, `LookupResult` 131, `Submit` 129, and `Register` 422. These
figures include batch/event wrappers and maximum-width epoch, identities,
resources, optional peak, and version. The contract reserves **1024 bytes** for
framing. Tests serialize the original event variants in real singleton batches
and verify their unpaid frames fit that allowance. The activated receipt
acknowledgement has the separately bounded certificate payload described above.

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

The journal separately validates these facts:

- Canonical serialized Raft `Membership` is at most **1 MiB**. Every membership
  entry is validated before append/apply batch mutation, and restored applied
  state is validated. Snapshot installation enforces the same bound.
- A snapshot ID is at most **1024 UTF-8 bytes**. Restored, incoming, and generated
  snapshot metadata enforce the bound.
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
Experimental format activation adds canonical metadata `F <= 64 KiB`;
the outer envelope can double it. The direct-object scheduler representation
uses less escaping than the base string, so the base bound covers both writers.

```text
encoded saved snapshot <= 4*S + 4*M + 2*F + 16 KiB
                       <= 56 MiB + 4 MiB + 128 KiB + 16 KiB
                       = 60.140625 MiB
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

## Qualification and remaining limits

The focused core fixtures fill aggregate admission with coalesced handles and
quote-heavy requests, then accept a maximum-sized result for every handle and
publication. The canonical state exceeds 90% of its 14 MiB bound and survives
restore, publication acknowledgment, retirement, and reuse of the previously
refused operation ID. A separate full-pool history proves worker replacement is
refused before revocation and that cancellation, cleanup, and retirement still
complete. Invalid payload ceilings, `usize::MAX`, raw oversized snapshots, and
an added unbudgeted handle are rejected.

Typed serializer guards cover every fixed state row, terminal/effect variant,
and command variant with maximum-width scalar fields. The independent model
uses its own credit calculation and includes an exact 14 MiB boundary fixture,
large captured successful responses copied to all coalesced handles, and
admission/retirement histories. Generated histories compare actual candidate
canonical bytes to independently reserved credit.

Real-journal tests cover membership append/apply/restore/install bounds,
atomic refusal of mixed batches, a 1 MiB membership and a 1024-byte control-text
snapshot ID, and successful finalization near the core admission bound. The
latter builds a snapshot larger than 40 MiB after escaping and recovers the
accepted result across two coordinator restarts.

These fixtures establish the bounded experimental encoding and admission
contract. They do not establish an allocator RSS bound, a total log-disk bound,
production response-size suitability, or fleet activation. Previous small
synthetic throughput measurements do not qualify the maximum-size path.
