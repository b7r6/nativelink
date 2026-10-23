# Recovery storage custody

Stopping the Raft core does not establish that all storage work has stopped.
Actual state-machine and snapshot tasks can retain a database after core shutdown
returns. A cancelled journal caller can also leave its blocking operation running.
Each surviving database reference retains the namespace ownership locks.

The private cleanup helper repeatedly attempts to take sole ownership of the
shared database. It keeps the reference after an unsuccessful attempt and waits
outside the journal queue and proposal lock. Once it owns the database, it
moves destruction to a blocking task and waits for that task to finish. A late
clone cannot race a separate reference-count check because taking ownership is
atomic.

Six baseline fixtures pass against the existing implementation. Two demonstrate
actual state-machine and snapshot work surviving core shutdown; four isolate
other reference and final-destruction boundaries. Six additional fixtures exercise
the helper at those boundaries. Two compiled faults return before the final
reference disappears or before blocking destruction finishes. They fail the
intended early-completion assertions. The first fault passes one control and fails
five; the second passes five and fails one. A secondary cancellation panic after
an assertion failure is not an additional detection.

The [sealed author packet](evidence/canonical-recovery-custody/author/package.json)
preserves every attempt, selected source, executable, and independent review.
Restored author source passes 128 library tests plus the external API test, with
one existing ignore; strict all-target checks pass. The restored executable
matches the positive control. An unused fixture import and subsequent strict-style fixes
remain separate compiler and lint attempts.

The [root integration](evidence/canonical-recovery-custody/root/manifest.json)
combines the helper with the already-qualified temporal owner audit. All five
selected Bazel targets execute uncached: 258 Rust tests pass, one remains ignored,
and the server builds. All 1,357 tracked non-evidence inputs remain unchanged and
match the recorded commit. Helper source and all twelve custody fixtures match
the author; three wiring files also include the temporal audit. Independent
review verifies the raw results and all five test executables against BEP hashes.

The helper requires a permanent supervisor that will continue polling it when a
public caller disappears. It does not provide that supervision itself and remains
unused by production constructors. The existing inert owner's release method is
unchanged. A blocked holder can leave cleanup pending indefinitely. Database
destruction does not expose a successful flush result, so completion is not a
durability acknowledgement. An independent namespace ownership alias can retain
its locks after the helper finishes. Runtime destruction and process loss are
outside an asynchronous completion guarantee.
