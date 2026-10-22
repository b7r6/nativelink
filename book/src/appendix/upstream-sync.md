# Appendix G: Upstream as a Subscription

The fork tracks upstream NativeLink as a **subscription, not an event**:
merges happen on a cadence (per upstream release, or monthly, whichever
comes first), by a scripted procedure, with the fork's own test surface
as the gate.

The procedure lives in the repository — `scripts/upstream-sync.sh`
driven by the checklist in `UPSTREAM-SYNC.md` — and covers: fetch and
merge, the conflict-surface triage (our touched files in
`nativelink-service`/`nativelink-config`/`nativelink-store` are the
predictable hot spots), running the composition-sensitive suites (the
bytestream and worker tests have both caught upstream merges silently
dropping a fork behavior), and updating `UPSTREAM-LEDGER.md` — the
committed triage of every fork commit into *upstreamable* (PR waves),
*keep-local* (the facades and this book), and *superseded*.

The posture in one sentence: anything upstream can take, we offer as
PRs from the ledger's waves; anything they cannot, we keep cheap to
re-merge by scripting the merge and testing the composition, not the
pieces.
