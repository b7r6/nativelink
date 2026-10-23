# Supervised recovery lifecycle

The private recovery supervisor retains the journal, database, and every returned
Raft handle independently of the caller awaiting recovery. Dropping that caller
requests shutdown; it does not abort the supervisor. Startup errors and caught
panics follow the same cleanup path. The supervisor awaits core shutdown and the
qualified final database drain before acknowledging completion.

Recovery requires an existing sealed owner and rejects newly created provenance.
It runs the temporal audit, starts the existing Raft state, and waits for leader
readiness without initializing membership. The handoff offers copied diagnostics
and uses a separate acceptance acknowledgement. The receiving owner arms its stop
control before acknowledging the offer. An unread offer cannot become an accepted
owner merely because it was queued.

The [author packet](evidence/canonical-recovery-lifecycle/manifest.json) preserves
21 new passing fixtures, the combined 184 library passes and one external API
pass, one existing ignore, strict checks, and six compiled fault controls. Each
control fails a reached assertion: caller-driven supervisor abortion, acceptance
of an unread offer, early startup-error return, early shutdown acknowledgement,
ignored abnormal core termination, and admitted newly created provenance. Loop
controls establish their first failing branch only. A fixture compilation error
and the initial strict-lint failures remain separate unsuccessful attempts.

Restored source passes the same regression and strict checks; both test
executables match the qualified positive binaries. Independent review checks the
source, actual executable bytes, reached assertions, and restoration. Root copied
and verified all four archives, 1,034 members, nine distinct executables, and
fourteen attempt bindings. All 21 selected integrated sources match the author
at root commit `c3beca65f8462ada711b73fbb7fa3234ca4b269a`. A combined root execution
with the clock remains a separate pending check.

This component exposes no public recovered owner or serving capability. It does
not yet capture the recovered clock or establish its application read barrier.
Early Raft construction failure has no returned handle and does not establish an
explicit tick-task join. Storage holders can keep cleanup pending indefinitely;
external ownership aliases can retain namespace locks after it finishes.
Database destruction is not a new durability acknowledgement, and process or
runtime destruction cannot acknowledge asynchronous cleanup. Production
construction remains unchanged.
