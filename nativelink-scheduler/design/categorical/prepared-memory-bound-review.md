# Prepared memory snapshot bound review

The private-v5 candidate at `1bccba3acb85e7153798ba2b29d264050584765c`
retains the conservative **2,026,948-byte canonical snapshot bound**, leaving
70,204 bytes below the unchanged 2 MiB guard. This review found no restore path
that violates the bound for a validated state's generated snapshot. It did
identify a distinction between accepted raw input and canonical output; the
candidate's bound document now states that distinction explicitly.

This review read the candidate and calculator, scheduler invariants and retention
invariants. It did not inspect the independent prepared-memory reference, edit
the candidate, run shared Cargo, change production code, or change a cap.
The exact [receipt manifest](evidence/prepared-memory-bound-review/manifest.json)
archives the source freeze, calculator output, independent dimension checks and
executable child-whitespace witness. The frozen calculator SHA-256 is
`7e0c3248c4239c41fa1fc0ec8c54cd9df1821cc46da30de00cadf3bd419f5f68`.

## Retained memory state and count inventory

A maximal `Resources` value occupies 100 JSON bytes. A memory report occupies
226 bytes; a retry disposition 121 bytes; and a decision 259 bytes. The enclosing
`PreparedMemoryStage` value occupies 508 bytes. Its field name, colon and
separator add 10 bytes, giving **518 bytes per stage**, or 4,144 across eight
stages. The independent checker enumerates 20 combinations of optional peak,
quiescence and decision branch. The retry branch dominates all three terminal
reasons. Twenty-digit IDs and resource values dominate their scalar encodings.

The report and decision contain only fixed fields. Their response and graph
remain in the original candidate. `all_classified_reports` builds a temporary
validation view; it is not a serialized field. An abandoned stage retains its
classification and candidate, so abandonment does not reset any size budget.

| Retained structure | Bound and restore check |
| --- | --- |
| Stages and memory fields | At most eight stages, each with at most one optional memory record. |
| Graph and no-graph report identities | `validate_prepared_memory` checks unique IDs and attempts, at most 24 combined identities, and at most three per execution. `validate_terminal_state` independently checks classified attempts and their execution counts. |
| No-graph terminal payload copies | Terminal dispositions reconstruct an exact acceptance map of at most eight executions; accepted operation links are capped at 16 and checked against scheduler operations. |
| Candidate payload and graph | Raw result and serialized response are each bounded at 4,096 bytes. Closure size is at most eight. Preparation must copy that closure; fence keys must equal its cohort keys. |
| Stage writes and prior stages | At most eight writes; internal write-map cardinality matches, outcome keys are a subset, and each execution has fewer than three prior-stage IDs. |
| Labels, bindings and authorization | Registered labels and trusted context strings have at most 256 UTF-8 bytes. Complete action bindings have a 4,096-byte JSON bound; authorization copies must equal their originals. |
| Retention roots and writes | Outer validation accounts for every root and write, requires contiguous IDs, and fixes the limits at 112 roots and 128 writes. Bootstrap closures contain one member; other owned closures contain at most eight. |
| Cohort expansion and history | Only allocating or closing cohorts survive outer validation, with no captured closures, marks or cursor. Accepted history, receipt retirement, retired generations and delete records are refused. |

Charging all eight graph stages together with all 24 no-graph reports is
conservative under the shared identity budget. A terminal graph acceptance adds
no serialized second report payload. Its operation results already belong to
the existing 16-operation allowance.

The scheduler still has at most eight executions, 16 operations, eight workers,
24 attempts and 24 reservations. Its effect invariants allow at most one
current dispatch with a request per execution; lookup requires zero attempts;
terminal execution results coexist with neither. Cancellation effects carry no
payload. These relations justify the existing 56 bounded payload units and the
24-effect fixed-row allowance, including after restore.

## Encoding and header audit

Each inner JSON byte becomes a decimal integer in the outer scheduler or
retention byte array. A byte contributes at most three decimal digits plus one
separator. The independent checker tests all 256 byte values, including repeated
4,096-byte arrays. A 256-byte string needs at most 1,538 JSON bytes, attained by
256 control bytes encoded as six-character escapes. The candidate calculator
uses exact decimal-array costs for fixed rows and four bytes per variable inner
byte. The longest current enum wrappers and optional values were checked against
the Rust definitions; field-name inventory checks alone would not establish that.

The header check pessimistically sets every numeric scalar and limit to the
maximum `u64`, even where validation permits smaller values. Row containers are
empty because their entries are charged separately. It includes the new
activation field, real and terminal headers, and null context placeholders;
context string bodies are charged separately.

| Header | Checked bytes | Existing allowance |
| --- | ---: | ---: |
| Scheduler, after outer decimal-array encoding | 2,162 | 4,096 |
| Retention, after outer decimal-array encoding | 1,573 | 4,096 |
| Metadata JSON | 498 | 4,096 |
| Empty outer envelope | 57 | 1,024 |

The exact frozen calculator and its field inventory pass. Its audited subtotals
are `1,058,032 + 214,096 + 753,796 + 1,024 = 2,026,948`. The independent helper
checks the memory-field dimensions, headers, escaping and final arithmetic; it
does not claim to independently generate every existing fixed-row subtotal.

## Raw-input witness and limits

A small Rust helper compiled the exact frozen retention modules with cached
Serde dependencies, without Cargo. It appended 680,000 spaces to a valid
261-byte child snapshot. Child restore accepted the 680,261-byte input and
regenerated the same 261 canonical bytes. The padded child's outer decimal-byte
array occupies **2,040,919 bytes**: above the canonical-state bound and below
2 MiB. The helper executes child restoration only; full outer acceptance was
reviewed in source, not executed by that helper.

Outer canonical encoding preserves child byte arrays. Both child restorers
parse JSON before reconstructing their state, so raw spelling need not match a
generated child snapshot. The separate outer 2 MiB input guard bounds those
bytes before parsing. No child-restore restriction or recovery semantics were
changed for this review.

The source freeze covers 19 files and all input hashes still matched at the end
of review. The calculator, independent dimension checks and child witness pass.
This is a finite canonical-state size argument, not a bound on parsing or
validation memory, proof of every transition's availability, power-loss evidence,
or production persistence qualification. A new retained field, enum variant,
cap or restore path requires reviewing these premises again.
