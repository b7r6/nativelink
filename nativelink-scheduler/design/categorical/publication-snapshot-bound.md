# Publication snapshot byte bound

The finite publication candidate has a conservative canonical snapshot bound of
**2,026,948 bytes**, below its unchanged 2,097,152-byte limit by 70,204 bytes.
This covers the exposed synthetic, real-request, direct-authorization and
private-v4 terminal and private-v5 prepared memory alphabets, including canonical
serialization of every restored state accepted by the repaired outer validation. It is
a size bound on valid states, not a claim about
production memory, admission throughput, collection, or retry-stage availability.

## The restore premise

The outer [`fence_namespace`](../../tests/publication_candidate/engine.rs#L978)
applies `Seal` and `BeginClose` in one transaction. `BeginClose` changes the phase
and fence. Only the separate Coarse
[`FinishClose`](../../tests/retention/coarse.rs#L312) transition copies live root
closures into a cohort; `Mark` then records the processed prefix. Neither
transition belongs to the outer alphabet.

Previously, outer restore accepted valid child snapshots containing `Sealed` or
`Closed` cohorts, including captured roots and completed marks. Canonical JSON
and valid Coarse invariants did not establish the outer transition boundary.
The repaired [`validate`](../../tests/publication_candidate/validation.rs#L80)
requires `Allocating | Closing`, empty captured and marked collections, and a zero
cursor. All states produced through the outer API already satisfy that condition.
Restoration now preserves it as well. The checks do not add a collector.

The focused tests construct these foreign snapshots through actual Coarse
transitions, verify their independent child restoration and canonical encoding,
then replace only the outer snapshot's retention byte array. On the original
validation, the sealed, closed, captured and marked states were accepted. The
repair rejects them. Ordinary allocating and closing snapshots still restore and
continue. Immutable earlier qualification receipts remain unchanged.

## Reproduce the arithmetic

From the repository root:

```bash
python3 nativelink-scheduler/design/categorical/publication_snapshot_bound.py
```

The [calculator](publication_snapshot_bound.py) contains every fixed row's field
shape and checks its struct field inventory against the Rust source. It also
checks the numerical caps. It emits each row cost and component subtotal. Those
inventory checks detect added or removed fields; they do not prove Rust field
types, enum variants, semantic invariants, or the correctness of this argument.
Changes to any of those premises require another review.

The outer input retains child snapshots as byte arrays. Child restoration may
accept whitespace or other JSON spelling outside the canonical form within those
arrays; the
outer canonical check does not normalize those bytes. The bound describes
`snapshot()` of a validated state, including a restored state, rather than every
accepted input byte string. The unchanged 2 MiB input guard still bounds raw
restore input before parsing.

This ledger was derived against source baseline
`0318e72abb0ea6973ec136868019cd50ef44e8b6` plus the outer cohort restriction above. The private-v5 refresh adds a 518-byte
classification and decision allowance to each of the eight stages. It retains
the original payload, raw result and closure bounds.
Line links identify that baseline's definitions; the calculator checks the
current checkout's field inventory. The snapshot guard remains a separate
defense against future omissions. No claim here depends on killing its compiled
mutation, and no snapshot overflow witness is claimed.

## Encoding rules

The outer [`Snapshot`](../../tests/publication_candidate/engine.rs#L94) stores
the scheduler and retention snapshots as `Vec<u8>`, so their JSON is encoded
again as decimal byte arrays. For inner JSON bytes `b`, the calculator uses
`sum(decimal_digits(b) + 1)`. A surrounding array adds one byte, covered by header
slack. Every variable inner JSON byte costs at most four outer bytes.

All fixed `u64` values and external numeric map keys use 20 digits; `i32` uses
`-2147483648`. Longer enum wrappers and optional fields are included. Every label
or trusted-context string has at most 256 UTF-8 bytes and therefore at most 1,538
JSON bytes including quotes, using the six-character escape for every byte.
Numeric byte arrays use their longest elements. Rows include conservative map-key
and separator costs even where the real container is an array. Mutually
incompatible field maxima are allowed in this calculation.

## Scheduler ledger

[`payload_fits`](../../src/authority/mod.rs#L509) bounds the complete serialized
Request, ResultPayload and worker property map, including their nested captured
protobuf bytes. The outer validation fixes that limit at 4,096. Request internals
and response internals therefore do not need separate byte allowances.

| Rows and source | Count | Fixed outer bytes per row | Variable fields excluded from fixed cost |
| --- | ---: | ---: | --- |
| [`Operation` and `OperationView`](../../src/authority/mod.rs#L56) | 16 | 1,179 | `original_request`, terminal result |
| [`ExecutionView`](../../src/authority/types.rs#L481) | 8 | 1,756 | `request`, terminal result |
| [`WorkerView`](../../src/authority/types.rs#L513) | 8 | 1,786 | `properties` |
| [`HoldView`](../../src/authority/types.rs#L503) | 8 | 603 | None |
| [`AttemptView`](../../src/authority/types.rs#L530) | 24 | 1,349 | None |
| [`ReservationView`](../../src/authority/types.rs#L548) | 24 | 899 | None |
| [`Effect`, using the largest Dispatch wrapper](../../src/authority/types.rs#L430) | 24 | 1,269 | Request or lookup action |
| State, limits and snapshot header allowance | 1 | 4,096 | None |

The [scheduler invariants](../../src/authority/mod.rs#L1615) give three attempts
per execution, one reservation per attempt, and holds keyed by existing workers.
The exact lookup, dispatch and cancellation multiplicities at lines 1766, 1858
and 1863 constrain effects more tightly than the outer 64-effect cap: each attempt
has at most one dispatch or cancellation; lookup has no attempts. Thus an
execution has at most three effects, or 24 overall. Effect validation ties every
payload back to its execution. `PublishResult` cannot survive the outer boundary.

The state retains 24 original Request copies: 16 operations plus eight executions. For
each execution, a dispatch Request or lookup ActionKey can coexist with no
execution terminal result; a terminal result can coexist with neither. Charge
eight complete payloads for that group. Charge another 16 operation terminal
payloads and eight worker property maps. This is 56 units of 4,096 bytes, each
expanded by at most four: **917,504 bytes**. It does not require equality between
joined callers' original requests.

The fixed rows and header cost **140,528 bytes**, giving scheduler total
**1,058,032 bytes**. Receipt retirement and accepted history are absent and are
refused by the outer validation.

## Retention ledger

The fixed [caps](../../tests/publication_candidate/types.rs#L39) and
[`ROOT_CAP` / `WRITE_CAP`](../../tests/publication_candidate/engine.rs#L41) give
64 cohorts, 64 generations, 112 roots and 128 writes. Cross-component validation
requires contiguous internal root/write identities and maps every internal
cohort handle to its namespace entry. Therefore those IDs use at most three,
three and two digits respectively; external generations still use 20 digits.

| Fields and source | Count and bound | Outer bytes |
| --- | --- | ---: |
| [`Cohort`](../../tests/retention/protocol.rs#L107) | 64; no captures or marks; 20-digit fence and even an oversized three-digit zero-cursor allowance | 20,608 |
| [`Generation`](../../tests/retention/protocol.rs#L117) and map Key | 64; logical/cohort IDs at most 64; revision at most 128; no delete identities | 44,480 |
| [`Root`](../../tests/retention/protocol.rs#L98) | 64 bootstrap roots with one Key; 8 staging, 8 publication, 16 operation, 16 serving roots with eight Keys each; lease 1 | 108,560 |
| [`Write`](../../tests/retention/protocol.rs#L129) | 128; one Key and bounded outcome each | 36,352 |
| Limits, clock, empty deletes and snapshot header | 4,096-byte allowance | 4,096 |
| **Total** | | **214,096** |

The inner [Coarse invariants](../../tests/retention/coarse.rs#L627) require each
generation revision to equal its successful-write count. All physical rows are
outside retirement and the delete map is empty. Outer validation associates every retained
root with one of the listed owners, fixes its immutable closure and lease, and
rejects unaccounted roots or writes.

## Metadata ledger

The following fixed costs include maximal labels, object hashes, physical keys,
numeric IDs, maps and enum wrappers. Only the separately listed bounded JSON or
raw-byte subtrees are excluded. The calculator lists every field explicitly.

| Row and source | Count | Fixed JSON bytes per row |
| --- | ---: | ---: |
| [`NamespaceBinding`](../../tests/publication_candidate/engine.rs#L46) | 64 | 197 |
| [`Physical`](../../tests/publication_candidate/engine.rs#L53) | 64 | 437 |
| [`Credit`](../../tests/publication_candidate/engine.rs#L61), including two prior stage IDs | 8 | 152 |
| [`Stage`](../../tests/publication_candidate/types.rs#L141), candidate, eight members, eight writes, full preparation and private-v5 memory metadata | 8 | 11,506 |
| [`Accepted`](../../tests/publication_candidate/types.rs#L155) | 8 | 169 |
| [`Operation`](../../tests/publication_candidate/types.rs#L165) | 16 | 1,709 |
| [`Grant`](../../tests/publication_candidate/types.rs#L176) | 16 | 102 |
| [`UpdateAuthorization`](../../tests/publication_candidate/direct_client.rs#L21) | 8 | 1,652 |
| [`ClassifiedReport`](../../tests/publication_candidate/terminal_types.rs#L67), origin and full memory evidence | 24 | 2,112 |
| [`NoGraphAcceptance`](../../tests/publication_candidate/terminal_types.rs#L75) | 8 | 1,911 |
| **Fixed subtotal** | | **243,360** |

Additional fields and payloads contribute:

| Source and fields | Calculation | JSON bytes |
| --- | --- | ---: |
| `actions`, `heads`, `real.actions` label keys | 24 × (1,538 + 24) | 37,488 |
| Eight original [`ActionBinding`](../../tests/publication_candidate/real_request.rs#L51) values | 8 × 4,096 | 32,768 |
| Metadata, real/terminal headers, private-v5 activation position and scalar counters | Fixed allowance | 4,096 |
| Trusted principal and execution configuration | 2 × 1,538 | 3,076 |
| Terminal operation links | 16 × 44 | 704 |
| Synthetic client IDs | 8 × 21 | 168 |
| Stage responses, operation results, copied authorization bindings | (8 + 16 + 8) × 4,096 | 131,072 |
| Report results, terminal dispositions, acceptance results | (24 + 8 + 8) × 4,096 | 163,840 |
| Raw candidate `result_bytes` arrays | 8 × (4 × 4,096 + 1) | 131,080 |
| Retry disposition wrapper slack | 24 × 256 | 6,144 |
| **Metadata total, including fixed rows** | | **753,796** |

[`validate_candidate`](../../tests/publication_candidate/engine.rs#L641) bounds
each stage's raw result bytes and complete response. The array allowance above
does not assume that their encodings share bytes. Full operation results are
validated against actual scheduler results. A binding, including its context,
action and full selected platform, has a complete 4,096-byte JSON bound.
Authorization bindings must equal their registered originals.

[`validate_terminal_state`](../../tests/publication_candidate/terminal.rs#L291)
allows at most 24 reports, eight distinct terminal acceptances and 16 links.
Each terminal disposition has exactly one associated acceptance, so there are
at most eight such additional result copies. The
[`no_graph_payload`](../../tests/publication_candidate/terminal.rs#L67) check
bounds each result at 4,096 JSON bytes. A retry wrapper is 44 bytes larger than
the terminal placeholder used in the table; the calculation reserves 256 extra
bytes for every report. All graph and no-graph maxima are charged together.

Private-v5 memory stages retain the report ID, epoch, original charged vector,
optional peak and quiescence. Their decision retains the acceptance command
arguments, position, time and either a retry vector or terminal reason. The
retry wrapper is the larger branch. These fields cost at most 518 additional
bytes per stage. They reference the existing candidate response and closure;
there is no second retained graph payload or closure ledger. Validation may
build a temporary combined report view, which is absent from the snapshot.
The shared identity budget counts both graph stages and no-graph reports, while
this conservative ledger still charges all eight graph stages and all 24
no-graph rows together. Stage abandonment releases protection but never removes
the retained candidate or classification. History remains disabled.

## Scope and remaining obligations

The sum is `1,058,032 + 214,096 + 753,796 + 1,024 = 2,026,948` bytes. The final
1,024-byte allowance covers the outer wrapper; its actual empty shape is 57
bytes. Repeated worker registration replaces bounded properties, while capacity,
heartbeat, progress and clock changes alter fixed-width fields. Grants, clients,
reports, writes and physical identities have bounded retained maps. No event log
or accepted-history records can enter this private composition.

This proves that the unchanged byte guard cannot reject a state satisfying the
reviewed invariants. It does not reserve stage rows for every retry, make a failed
transition valid, or qualify production persistence. The earlier 1,082,981-byte
capacity witness remains a measurement, and its 64 cohort transitions are
fences, not captures. Any future `FinishClose`, `Mark`, larger cap, new enum
variant or retained field invalidates the relevant premises until reviewed.
