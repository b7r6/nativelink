# Closed recovery and worker integration

Commit `78ab50c3b632dc9a1b36db77f411a1aef277ef32` passes six uncached Bazel
targets and builds the server: 378 Rust test passes and one existing ignore.
The recorded invocation takes 70.394 seconds. All 1,387 tracked inputs remain
unchanged and match the commit. Independent review confirms the actual test
executions and all seven retained executables against build-event digests.

The targets contain 216 scheduler passes, two external API passes, 68 ownership
passes, 80 journal passes, and twelve journal bridge passes. The ownership and
journal targets both include the same thirty-three worker cases; these are
execution counts, not distinct-case counts.

Run `python verify.py` from any working directory to verify the sealed archives
and every member. Eight archives retain 1,431 members, including source copies,
raw logs, build events, actual executables, and independent review. The server
was built and retained; this check did not start it.

The source comparison retains the worker's final documentation update: 34 of
35 inputs match the earlier control baseline, and all 35 match its final author
commit. The separate capability drop-order review found no defect: struct field
declaration order retains ownership until its descriptors close.

This is focused integration, not a new full-workspace run. Author fault campaigns
remain in their separate packets. Production construction and execution are
unchanged; cold initialization, sealing, feature admission and canonical serving
remain open. The later nightly assertion correction has its own check.
