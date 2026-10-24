# Terminal-outcome reference checkpoint

The independently frozen [reference](../../tests/terminal_outcome/README.md)
relates scheduler terminal decisions to accepted worker provenance, each
operation's original request and terminal, and existing storage obligations.
Its new path requires an exact captured response with `result=None`. It never
creates an empty ActionResult, publication root, operation root or cache head.

The existing independent M1 oracle decides memory growth, final failure reasons,
cancellation, expiry and physical reservation release. The existing independent
retention oracle preserves declared stage and write ownership. Both inherited
source files remain unchanged. The new candidate reducer and tests were not read;
the candidate author received the separately frozen contract only.

All 29 cases pass in Cargo and the integrated root Bazel target: 22 new composition
cases and seven inherited M1 cases. Twelve compiled faulty variants fail unchanged
assertions. Strict Clippy and the recorded style checks pass. Root Bazel ran with
eight jobs and two local test jobs, reporting 29 passes, zero failures and zero
ignored cases in its Rust test log. Its XML contains one successful wrapper case.

The [receipt](evidence/terminal-outcome-reference/manifest.json) records exact
source hashes, the preserved 187050-byte author archive, the original contract,
all mutation sources and logs, and root test output. The initial root test passed
with the exact frozen Rust bytes. Full pre-commit then found formatting changes
in two child modules: the author's final formatter check had covered only the
entry point, while repository configuration disables child traversal. The failed
hook and formatting-only delta remain separate evidence. Every child now passes
an explicit formatter check, full hooks pass, and root Bazel again passes 29 cases.
The original artifacts and mutation hashes remain unchanged; two integrated Rust
hashes differ only through formatting. Semantic independence is preserved, but
integration no longer has byte identity for those two files. No comparison against
the successor candidate has been completed at this boundary.

A later [restore audit](terminal-outcome-restore-audit.md) preserved eight compiled
counterexamples against this exact original freeze. Its separate correction
requires local consistency of retained memory grants, report positions and clocks,
and final reasons. Private snapshot v2 refuses v1 without inventing missing
history. The original 29-case target and the 44-case audit target pass in Cargo
and root Bazel: the audit embeds all original cases, so there are 44 unique cases.
All 22 compiled mutations are detected, including reruns of the original twelve.
The [corrective receipt](evidence/terminal-outcome-restore-audit/manifest.json)
keeps the original 29/12 qualification distinct. Current worker capacity does not
replace historical evidence; full historical eligibility remains outside scope.

The bounded model checks the entire proposed 2 MiB snapshot before committing.
It reserves finite row and stage/member capacity, and tests refusal when a retry
would borrow another execution's reserved capacity. This does not prove prepaid
future snapshot bytes, retry capacity or eventual progress under permanent
capacity pressure. Initial protected roots and fresh stage closures are explicit
scenario evidence; provider I/O and graph traversal are separate qualifications.

History feature 3, graph-bearing resource failures, direct-client publication,
physical collection, execution-record retirement and durable publication formats
remain outside this reference. The checkpoint does not activate production
services or declare a production cutover gate complete.
