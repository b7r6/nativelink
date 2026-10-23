# Accepted diagnostic history

This is an experimental authority component and test-constructed delivery path.
Production factories retain the legacy runtime. This document does not close
M4a.2, the four-part M4a deployment gate, or the M5 protection contract.

## Acceptance and stable results

Feature 3 adds one diagnostic association when the authority accepts a worker
terminal response. Success, nonzero exit status, and `do_not_cache` responses
receive the same fixed treatment. A final accepted resource failure also gets
an association; an intermediate memory retry does not. Cache hits, direct AC
updates, cancellation, expiry, stale candidates, and terminals accepted before
activation do not create history.

The authority retains the original attempt, worker, incarnation, execution,
cluster epoch, and globally allocated effect ID. Later worker replacement does
not revoke an already accepted association. A shared operation whose interest
was cancelled before acceptance does not acquire that association. Terminal
ExecuteResponse bytes remain fixed; diagnostic delivery adds no browser link or
replacement client-link interface.

The diagnostic is the pinned HistoricalExecuteResponse encoding: Action Digest
in field 1 and the exact captured ExecuteResponse bytes in field 3. The original
response is never decoded and re-encoded to construct it. The action digest's
canonical algorithm name is `sha256` or `blake3`; its lowercase hexadecimal hash
has 64 characters and its size fits protobuf int64. The object digest uses that
recorded algorithm. A separate SHA256 fingerprint binds the retained response.

The recipe stores the encoded prefix, response fingerprint, and exact object
namespace, algorithm, digest, and length. It refers to the existing terminal
payload instead of duplicating it in an outbox row. A pending association pins
execution retirement. Only an exact successful object outcome marks it
materialized; failure, uncertainty, and wrong-object responses leave it pending.
Materialization changes no reservation, worker authority, or terminal response.

## Prepaid bounds

The maximum prefix is 82 bytes: one bounded action Digest plus the field-3
length delimiter for a response of at most 256 KiB. Metadata reserves 2048 bytes
per retained execution plus a 1024-byte component header. Tests serialize the
widest identity fields, digests, prefix, and object length against this credit.
Existing variable response bytes remain covered by the authority's original
payload budget. The complete snapshot still obeys its 14 MiB bound.

Each unfinished execution reserves one additional finalization effect ID.
History and ordinary effects use the same monotonic allocation cursor; receipt
certificates share it too. The fixed effect allowance grows from eight to nine
slots per retained execution, so a small or already full authority may refuse
activation. The new credit is checked on a clone before activation changes any
state. Admission must preserve that credit for later completion.

Restoration checks association provenance, global identity separation, exact
recipe derivation, terminal origin, and the namespace binding. Pending history
and receipt pins compose: satisfying one does not release the other. A
materialized row retires with its execution under the existing retention rules.

## Activation and recovery

Feature 3 uses the existing preparation and committed decision protocol. The
logical history namespace is SHA256 of the versioned domain label, cluster
epoch, and authority namespace. It is a logical binding, not proof of physical
provider identity, immutable placement, or protection against collection.

Base and receipt-only snapshots retain their existing bytes until activation.
The active core uses snapshot version 7. Existing terminals are recorded as
terminals from before activation and receive no backfill. Existing unfinished
actions must be representable and able to fund their additional metadata and
identity credit before activation can succeed.

Normal proposals pause while a preparation adding history is unresolved. The
short enqueue gate orders proposal stamping and actual Raft enqueue; it is not
held during quorum reads or reply waits. Failure to prepay is followed by a
committed Abort, not a no-op activation or reinterpretation of a normal command.
Writers also wait for applied format state to catch up with a committed feature
decision before stamping subsequent commands. Append validation checks retained
prefixes and staged activation before persistence.

Resolving an abandoned preparation requires an explicit recovery or handoff
owner. Starting another service is not permission to abort a live activation
helper. The registry and local proposal gate are trusted in-process experiment
mechanisms; they are not distributed membership authentication.

## Delivery qualification

The experimental coordinator reads history metadata and terminal payloads from
the same committed state. It checks the explicit CAS namespace, response
fingerprint, and complete object bytes before delivery. The CAS write carries
the recorded digest algorithm in its context so verification stores use the
correct algorithm. Successful materialization is then acknowledged through the
authority with the original effect and exact object identity.

Ordinary AC/lookup work and diagnostic work share four delivery futures and
bounded retry bookkeeping. A rotating cursor across their combined identities
prevents repeated old failures from monopolizing delivery when journal work
outlasts the retry delay. A restart reconstructs pending work from the authority;
an externally successful write whose acknowledgement was lost remains pending
until an exact retry succeeds. This is logical CAS delivery, without M5 physical
generation or serving-protection claims.

The independent reference was frozen before its author inspected any history
transition implementation. Its real-State adapter caught a canonical lowercase
algorithm mismatch. The adapter's mistaken noncacheable sharing input was
corrected from the already frozen request contract; the reference itself did
not change. Compiler diagnostic exposure, failed runs, source hashes, and exact
qualification limits are preserved with the evidence.

Receipts: [core](evidence/accepted-history-core/manifest.json),
[format](evidence/accepted-history-format/manifest.json),
[independent adapter](evidence/accepted-history-differential/manifest.json),
and [delivery](evidence/accepted-history-delivery/manifest.json).

The eight receipt-retirement faults also remain detected on the composed source;
[composition receipts](evidence/history-receipt-composition/manifest.json) preserve
the restored fourteen-test baseline and exact source snapshot.

The original two [real worker histories](evidence/real-worker-history/manifest.json)
execute namespace processes for noncacheable success and a nonzero exit. The
[BLAKE3 extension](evidence/worker-history-blake3/manifest.json) runs both outcomes
with SHA256 and BLAKE3 through a hash-verifying CAS. Each case independently checks
the request algorithm, Action, Command, input root, outputs, stdout, stderr, and
exact diagnostic bytes. Pending delivery survives restart without changing the
accepted terminal, while the configured legacy upload policies remain unused.

The BLAKE3 process cases exposed a missing action-scoped digest context in the
authority worker: valid inputs launched successfully, but result upload presented
BLAKE3 output digests to a verifier still using SHA256. The worker now scopes
creation, preparation, execution, upload, and cleanup to the request algorithm;
spawned tasks inherit that context. An omitted algorithm remains SHA256 under the
authority protocol. No process-wide digest default changes. The receipts retain
the failing process run, source snapshots, and the passing regression checks.
