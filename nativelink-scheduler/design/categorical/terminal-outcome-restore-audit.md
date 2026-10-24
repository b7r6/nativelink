# Terminal outcome restore audit

This is a separate corrective qualification of the independent terminal outcome
reference. It preserves the original reference freeze and its 29 passing tests
and 12 compiled mutation kills as historical evidence. The new boundary addresses
contradictions in retained retry evidence that the original reader accepted.

## Counterexamples

Eight new probes compiled against the exact original frozen reference. All eight
failed their assertions. Seven canonical snapshots containing locally
contradictory facts restored successfully; the eighth probe demonstrated that a
retry report retained neither its recording position nor its recording clock.
The original probe sources, source hashes and failing compiler/test log remain in
the `before` receipt directory. The old case file and frozen lifecycle and
retention references are unchanged.

The contradictions included removing the only positive memory evidence from a
zero-grant retry, adding positive evidence to a missing-evidence final result,
claiming attempt-budget exhaustion after one actual attempt, deleting retry
records while retaining memory growth, and changing terminal ordering or time.
These are reader defects, independently of agreement with an implementation.

## Corrected boundary

Each classified report now records its outer transition position and clock.
Restore requires globally unique increasing report positions, clocks that do not
move backward, and report order consistent with retained actual attempts. A
terminal report must belong to the final actual attempt and agree with both its
accepted origin and the lifecycle terminal clock.

The reader starts with the immutable original requested resources and follows
retained attempts and their immutable reservation grants. Growth requires a
retained retry record and positive original grant or peak evidence. It must
preserve CPU and slots, exceed the prior memory grant, meet the original minimum,
and remain within twice the applicable evidence, using saturating arithmetic.
The retained final target must agree with that chain. Final memory reasons must
be locally possible for the actual attempt ordinal and available evidence.

Historical worker eligibility is deliberately not recomputed from current
workers. A positive test retains an accepted capacity-limited final result after
worker capacity increases. Another positive test establishes that the actual
third attempt has exhausted its budget even after earlier attempts ended through
expiry and the third report has no growth evidence.

The private snapshot frame is now `independent-no-graph-terminals-v2`. This is an
explicit incompatible version change for this test reference. Version-one
snapshots are refused; missing historical positions and clocks are not invented.
The report-record fields are additive source data, but their required presence
changes the persisted format. No production snapshot migration is introduced.

## Qualification and limits

The audit target includes all 22 original composition cases and all seven frozen
lifecycle cases without changing their assertions, plus 15 new audit cases. The
original 29-case target also remains independently runnable. Corrected receipts
record strict Clippy, explicit formatting of every changed Rust file, both Cargo
targets, and compiled mutation results. The old 29/12 qualification is not silently
relabelled as the corrected qualification.

These checks establish consistency of locally retained evidence. They do not
recover the worker eligibility set or capacity ceiling at every past report, nor
do they authenticate a fully fabricated but mutually consistent history. The
reference remains a finite no-graph terminal and retained-obligation model. This
change makes no claim about candidate agreement, production activation, complete
output-graph validation, provider I/O, collection, or full historical replay.
