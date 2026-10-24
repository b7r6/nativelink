# Recovered canonical owner

`PreparedCanonicalOwner::recover_sealed` consumes an existing sealed journal and
returns a closed `RecoveredCanonicalOwner`. The owner exposes copied historical
diagnostics and consuming shutdown. Its private supervisor retains the matching
Raft, journal, database and clock. The public API exposes no sample, proposal,
query, raw storage handle, factory or conversion into the experimental singleton.

Recovery completes the actual applied read barrier and full temporal audit before
capturing a fresh monotonic origin and checking the first sample. The supervisor
stores the clock before another suspension point. Consumer cancellation requests
cleanup without aborting that required read. The existing handoff arms the
receiving owner's stop control before accepting the offer; the public wrapper
adds no later suspension.

Startup errors, caught panics, dropped owners and cancelled shutdown waiters
converge on supervised cleanup. Completion follows core shutdown, storage-holder
drain and actual final database destruction. A blocked holder can delay completion
indefinitely. Independent namespace aliases can retain locks after completion,
which supplies no new durability acknowledgement. Async cleanup requires a live
runtime.

The [author packet](evidence/canonical-recovery-composition/README.md) preserves
16 new composition tests, 216 passing library tests and two external API tests,
one existing ignore, strict checks, and exact restored binaries. Six compiled
fault variants reach direct assertions. Removing the applied wait retains the
position guard but prematurely shuts down the core and refuses a valid proposal;
that observation does not establish an early owner offer. The other controls
detect an early origin, a first-sample fallback, a wrong copied base, premature
shutdown acknowledgement and physical lock release after caller cancellation.

External compiler probes first compile the permitted API and then reject sixteen
invalid uses at their intended boundaries. The initial classifier counted fifteen
because one probe also reported a related private-type error. Both the original
diagnostics and the narrow classifier amendment remain preserved. The restored
run accepts the positive probe and classifies all sixteen rejections.

Root integrates all 44 selected author inputs byte for byte. This completes the
closed recovery boundary, while cold bootstrap, layout synchronization, the seal
writer, periodic time progress and protocol serving remain separate work. The
canonical journal still uses the Base reader; enabling later format features
requires its own admission and recovery qualification. Production factories
continue constructing the legacy runtime.
