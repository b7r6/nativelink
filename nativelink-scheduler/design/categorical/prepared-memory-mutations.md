# Prepared memory candidate fault checks

Eight selected faults were compiled separately against the frozen private-v5
candidate at `1bccba3acb85e7153798ba2b29d264050584765c`, integrated unchanged at
`52b0cb22ce`. Every faulty executable failed its exact named test. The unmodified
baseline and restored source each passed all 72 candidate tests.

The [receipt](evidence/prepared-memory-candidate-mutations/manifest.json) archives
the runner, literal source replacements, original and changed sources, compiler
and test output, source and executable hashes, and the result classification.
All 140 recorded Rust/Cargo inputs matched again after restoration, and the
isolated checkout was clean. The shared target directory contains build products;
source changes were confined to the separate mutation checkout.

| Injected fault | Assertion exercised |
| --- | --- |
| Apply Complete instead of MemoryFailure | Current-capacity changes must select the actual retry or resource-failure policy. |
| Skip current prepared-graph evidence | Incomplete or stale proof cannot authorize classification. |
| Release the staging root on retry | Retry must preserve the original stage protection and write obligations. |
| Replace the final resource-failure reason | Operations and retained decisions must preserve the reducer's exact reason. |
| Omit operation roots after final acceptance | Newly terminated operations must receive graph protection in the same transaction. |
| Refuse an exact accepted-command replay | Duplicate acceptance must preserve the original outcome without a second mutation. |
| Skip restored memory-report relation checks | Inconsistent retained classification and acceptance arguments must refuse restoration. |
| Discard positive peak evidence | A zero original memory charge must use the supplied positive peak for growth. |

Each source replacement had a unique checked anchor. Compilation failure,
timeouts, missing tests, signals and other process errors were separate outcomes;
none counted as a detected semantic fault. A detection required the named test to
execute and fail an assertion or panic. Several faults are caught by the
candidate's final cross-component validation, causing an otherwise valid command
to refuse; that is an observable regression covered by the test.

These results qualify the listed checks on this source. They do not establish
arbitrary fault coverage, independently validate graph traversal, or qualify real
memory-pressure classification, worker RPC routing, physical storage, retained
history, production formats or deployment. The independent reference comparison
has its own source boundary and evidence.
