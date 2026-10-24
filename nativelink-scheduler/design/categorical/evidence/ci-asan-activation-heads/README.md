# Nightly activation-reference check

The original GitHub ASAN job fails before test execution at four Clippy
`assert_is_empty` diagnostics. Commit `b46336cd132b7db82ab982d2c815203a5bb11da0`
changes those assertions to equality with an empty typed vector. The reference
model and production runtime remain unchanged.

The affected target then passes all 27 tests, uncached, under `--config=asan`.
Independent review verifies the active nightly channel and address sanitizer
flags, successful Clippy and formatting aspects, all 1,387 unchanged inputs
against Git, and the actual executable against its build-event digest. The
executable also contains the expected sanitizer symbols.

The archives retain the original CI failure and source-only diagnosis, both
local commit-hook attempts, the focused run and executable, source copies, and
independent review. The first hook attempt corrected one line's indentation.
Reviewer script prerequisites are retained separately from runtime outcomes.
Run `python verify.py` to verify the sealed archives and every member.

This qualifies one target under the sanitizer configuration. It does not
establish a green full-workspace sanitizer run or replace GitHub's checks on
the final published revision.
