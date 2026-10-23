# Clock capture after recovery

The private canonical clock starts from persisted applied time after a real Raft
application barrier and a complete, consistent temporal audit. The audit returns
the persisted position and time captured before staging any retained log suffix.
It returns those facts only after all existing metadata, snapshot, membership,
state, and log checks succeed. A staged suffix cannot masquerade as applied time.

The clock requires a present barrier and applied position. Applied position must
reach the barrier; equal indices require the complete log identity to match. A
fresh monotonic origin is captured after the validated read finishes. The first
sample is fallible. Later private samples floor elapsed seconds, use checked
addition, reject values beyond the finite cutoff, and reject movement before the
last accepted instant. Failed samples leave the last accepted instant unchanged.
Recovery carries neither downtime nor the old fractional remainder forward.

Sixteen fixtures cover numeric boundaries and real Raft recovery. One holds an
actual committed Base application before it acquires the journal queue: ordinary
validated reads still complete against the old applied state, while clock
recovery waits for application. Other fixtures distinguish replayed time from
preflight time, pause the actual validated transaction, and change sealed
metadata after the initial open.

The [sealed packet](evidence/canonical-recovery-clock/author/package.json) retains
the original tests, source revisions, raw attempts, actual executables, and
independent reviews. Qualified and restored source pass 167 library tests plus
one external API test, with one existing ignore. Strict all-target checks and
applicable hooks pass. Both restored executables match the qualified binaries.
Twelve compiled controls, across fourteen invocations, produce eleven direct
semantic detections and one expected survivor. Saturating addition survives
because the unchanged finite cutoff still rejects its overflow result. It is
not counted as a detected fault. Loop controls establish only reached branches.

Earlier unsuccessful attempts remain explicit: one command named a nonexistent
test target; another passed the library suite, then could not launch the API test
because generated binaries were missing. The latter is an interrupted attempt,
not a complete passing run. Strict-lint corrections and source-list formatting
retain their own boundaries. No missing API, compiler error, or setup timeout is
counted as a semantic detection.

This private component does not expose sampling, serving, or a periodic time
driver. Matching Raft and journal ownership belongs to the recovery supervisor;
numeric positions alone do not authenticate arbitrary pairs. The audit retains
its potentially unbounded history work, and recovery has no completion-time
guarantee. Combining this clock with permanent supervision and a closed public
owner remains a separate qualification step. Production construction is unchanged.
