# Experimental recipient receipt retirement

The [authority contract](receipt-authority.md) describes committed eligibility,
prepaid state bounds, independent comparison, and snapshot validation.

This slice implements the worker recipient, wire primitive and experimental
automatic coordinator's connection to the committed receipt outbox. Existing
Base constructors retain their existing feature set. The explicit qualification
fixture admits a ReceiptRetirement reader and commits feature 2 through the real journal's
Raft Prepare/Activate path before starting the service. A computed view of
released reservations never becomes a source of external effects.

## Certificate and identity contract

`AuthorityReceiptRetirement` carries the current negotiated session and an
explicit certificate. Missing certificate fields never acknowledge anything.
The certificate contains a stable authority effect ID, cumulative attempt,
effect and reservation watermarks, and exact retained bindings. A binding names
the attempt, execution, original dispatch effect, reservation and any allocated
cancellation effect. Zero cancellation means none existed at this cut.

The effect ID is scoped by the cluster epoch, worker and process incarnation.
It also orders certificate revisions. The effect watermark includes that ID;
the ID cannot alias a retained or locally known command. A new certificate ID
must exceed the previous effect watermark. Reconnect changes only the session
envelope; the certificate body and identity remain unchanged.

Certificates are limited to 64 KiB and 1024 retained bindings. Bindings have
strictly increasing, nonzero attempt IDs; command effects and reservations are
unique within their respective domains. Every binding lies within its domain's
watermark. The constructor reserves certificate headroom within the existing
3 MiB reconciliation bound. Only the latest certificate is retained, together
with the bounded receipt and alias maps; there is no certificate history.

All watermarks advance monotonically. An old retained attempt can disappear
but cannot reappear after omission. Its execution, dispatch and reservation
bindings cannot change. An absent cancellation may become a newly allocated
effect above the previous effect watermark, after which it is immutable.
A new retained attempt above the previous attempt watermark must also have new
effect and reservation identities above the previous domain watermarks.

These extra identity floors matter after map removal. An attempt-only floor
would reject an old command but allow a new attempt to reuse the forgotten
command effect or reservation. Here an identity below a floor is accepted only
through an exact retained binding. Unknown local attempts in that retained set
can therefore receive their original first dispatch or cancellation safely.

## Atomic recipient behavior

Certificate validation and compaction share one ledger lock. Every covered,
omitted local receipt must be Quiescent and all its known identities must be
within the certificate cut. One uncertain or live receipt vetoes the entire
certificate. A certificate cannot partially remove later completed receipts
while accidentally discarding an older live process. Known retained bindings
must agree with the local receipt. A cancellation received after the cut may
remain locally when its effect is above the cut and the certificate's
cancellation field is still absent.

The recipient removes eligible receipts and their effect/reservation aliases,
stores the latest certificate, and publishes a capacity observation. It keeps
the process's single-use spent fact. None of these actions accepts output,
establishes cleanup or refunds an authority reservation. The issuer must already
have committed the decision that permits forgetting each candidate.

The current certificate repeated byte-for-byte is a no-op. A conflicting body
under the same ID, an invalid identity, a regressing watermark or a resurrected
exception is rejected. An older certificate and a command targeting a compacted
attempt return OutOfRange. The transport treats only that classification as a
harmless stale replay: it reports the current inventory and keeps the session
alive. It does not manufacture a decline, cleanup report or candidate result.

`AuthorityReconciliation.receipt_retirement` reports the latest applied
certificate explicitly. An absent value means none was applied. Process loss
still requires a new incarnation; reconnect to the same live recipient preserves
the floors, exceptions, candidates and spent fact. This does not provide durable
worker recovery under a reused incarnation or fence another process.

ReceiptRetirement is a mandatory capability in both directions. Missing support
fails negotiation; there is no downgrade to the previous worker protocol.

## Committed authority delivery

The driver reads receipt metadata from the same committed state as its ordinary
observable view, retaining it separately. When the activated authority marks a
stream dirty and no certificate is pending, the driver proposes IssueReceipts
with the current incarnation and prior acknowledgment identity. The reducer
decides eligibility and commits one immutable certificate. Only that persisted
pending body may be sent to the worker.

The current negotiated session supplies the envelope. Reconnect changes the
envelope but not the committed identity or body. An unacknowledged certificate
is retried every 250 milliseconds while a session is live. A successful queue
write does not acknowledge delivery. The driver's retry map is bounded by the
pending outbox, and idle timer ticks do not append rejected issuance proposals.

Registration and later reconciliation can acknowledge a certificate only when
the explicit body exactly matches the current incarnation's pending body.
Missing, stale or conflicting values do not clear pending intent. The driver
commits AcknowledgeReceipts before exposing the resulting acknowledgment. This
releases the authority's receipt pins; it does not establish cleanup, accept
another result or alter resource charges. Releases that occurred while an older
certificate was pending remain dirty and can produce a successor certificate.

Qualification gates stop issuance before its commit, transmission after commit,
or acknowledgment after worker application. These gates suppress real actions;
they do not manufacture certificates or cleanup evidence. The service itself
never activates the feature or bypasses the journal's format controls.

## Qualification boundary

The recipient tests exercise unknown first dispatch, cancel-before-dispatch,
identity aliases after map removal, wrong scope/session, absent certificates,
duplicate and stale certificates, malformed exceptions, monotonic cancellation
bindings, atomic uncertain-process veto and single-use capacity after compaction.

The actual tonic fixture uses a small file-backed trusted certificate driver.
It writes, syncs and replaces the latest certificate before transmission. The
driver is a test instrument, not the authority reducer, journal or outbox.
Its caller explicitly chooses a certificate after observing a candidate. The
fixture covers coordinator loss before that commit, after commit but before
delivery, and after application but before acknowledgment, then reconnects and
retransmits the exact committed identity.

With a four-receipt limit, the fixture completes 48 actual processes in PID namespaces
while the first attempt remains active. After every retirement it checks one
retained receipt, one effect alias, one reservation alias and one exception.
Delayed compacted commands cannot relaunch work or terminate the successor
session. The old attempt is removed only after physical quiescence and another
explicit certificate. Hosts without namespaces report an explicit skip; those
hosts do not qualify the physical process claim.

Six recipient mutations compile and fail their named assertions: removing the
retired attempt floor, the command effect floor, the certificate allocation
floor, changed-body rejection, the cancellation identity check, or preservation
of the single-use spent fact. The mutation runner restores the recipient source
between cases and reruns the complete recipient target afterward. These are
local guard checks, not an independent proof of authority eligibility.

The separate automatic RPC fixture uses the actual authority reducer, durable
journal and committed outbox. It stops before issuance, after issuance but
before delivery, and after worker application but before acknowledgment. Every
restart closes the previous singleton, reconstructs its admission registry from
the closed journal and admits a later capable incarnation. Pending bodies must
survive unchanged. The fixture then completes 16 processes beside the original
active attempt with a four-receipt limit, observing explicit acknowledgment and
one retained receipt after every completion. Releasing the old process allows
the final certificate to retire all receipts.

The longer automatic history has a dedicated Bazel target with a moderate
timeout: `bazel test //nativelink-worker:authority_receipt_rpc_test`. The existing
short RPC target retains its nine cases. For Cargo, select the
fixture with `cargo test -p nativelink-worker --test authority_rpc_test
activated_receipt_outbox_survives_rpc_restart_cutpoints_and_old_active_hole --
--exact`. Per-observation waits remain 20 seconds; the old process's
15-minute action limit keeps it active throughout the bounded test budget.

These checks qualify the bounded recipient and the activated local automatic
integration. They do not establish production authentication, replicated service
operation, a deployed durability contract, or automatic receipt retirement in
the canonical service. The standalone file-backed driver remains only a
recipient test instrument; it is not used by the automatic coordinator.
