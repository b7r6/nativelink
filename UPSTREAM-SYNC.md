# Upstream Sync Runbook

The fork tracks `TraceMachina/nativelink` as a **subscription, not an event**.
`scripts/upstream-sync.sh` scripts the proven merge procedure; this runbook is
the judgment that goes with it.

**Cadence:** per upstream release, or monthly — whichever comes first.
Budget: under an hour of runbook execution (excluding cold cargo builds).

## Procedure

```sh
scripts/upstream-sync.sh census   # 1. fetch + dry conflict census (read-only)
scripts/upstream-sync.sh merge    # 2. merge (stops on conflicts for you)
scripts/upstream-sync.sh gates    # 3. check / affected tests / clippy / fmt
# 4. re-audit UPSTREAM-LEDGER.md (below)
# 5. commit ledger updates, push
```

## Conflicts: compose, don't pick

When the merge stops, **never blind-pick a side** (`--ours`/`--theirs` are both
wrong by default). The resolution rule that has held across every sync:

- **Union our fix WITH upstream's change.** Upstream refactors and adds
  telemetry around the same code we patched; the correct resolution almost
  always keeps upstream's new shape *and* re-applies our semantic delta inside
  it. Precedent: the v1.6.3 merge composed our strict ByteStream checks with
  upstream's new mismatch telemetry (`ee7b9296`).
- If upstream's change makes ours redundant, that is a **supersession**, not a
  pick: take upstream's side, then *prove* it (run our regression test against
  upstream's code) and record it in the ledger. Precedent: upstream's
  `WriteRequestStreamWrapper` `write_finished` guard superseded our
  `cc69bb54` write-stream fix — proven by our draining-mock test.
- Cross-reference `UPSTREAM-LEDGER.md` for any conflicted file: if the file
  hosts a PR-wave commit, the wave's semantics must survive the merge (its
  test still passes).

## Gates

1. `cargo check --workspace`
2. `cargo test --no-fail-fast -p <each crate the merge touched>` — affected
   crates only
3. `cargo clippy --workspace --tests`
4. `cargo fmt --all -- --check`

All via `nix develop .#default --command ...`. Beware exit-code laundering:
never pipe the gate run through `grep | tail` without `pipefail` — the
2026-08-04 sync's first gate run reported success over a failed compile that
way.

### Known pre-existing failures — do NOT chase during a sync

- `filesystem_store_test`: `file_continues_to_stream_on_content_replace_test`,
  `file_gets_cleans_up_on_cache_eviction` (environmental).
- `nix-client` nar `cross_check` SIGKILL under parallel load (aborts the whole
  `cargo test` invocation — hence `--no-fail-fast`, and check every failure
  against this list rather than trusting the exit code alone).
- `s3_store_test`: `multipart_update_large_cas`,
  `multipart_chunk_size_clamp_min` (mock returns "error parsing XML: no root
  element"; fails identically at pre-merge commits).
- `running_actions_manager_test`:
  `tests::entrypoint_sends_timeout_via_side_channel` (wrapper script in the
  nix-shell tmpdir fails to exec; fails identically at pre-merge commits).
- `cargo clippy --tests` is red in the two LOCAL crates
  `nativelink-nix-client` and `nativelink-oci` (pre-existing lint drift,
  identical pre/post the 2026-08-04 sync; follow-up owed by those crates, not
  by the sync). The gate is: no NEW clippy findings in crates the merge
  touched.

To attribute a suspicious failure, re-run it at the pre-merge commit: stash
the sync-local edits, `git checkout <pre-merge>`, run the one test, restore.
Identical failure = pre-existing. New failure = merge fallout — and remember
the fallout can be *semantic* with a textually clean merge: the 2026-08-04
sync's gates caught two (a struct literal missing a fork-added field in
upstream-side tests, and the v1.6.3 merge having dropped upstream's
compressed-write dispatch when grafting our strictness knob into
`ByteStreamServer::write`). Both were fixed by composing, not picking.

### Pre-commit hooks

vale/statix carry pre-existing findings in untouched files. If they block the
merge commit, skip with a documented reason
(`git commit --no-verify`, and say so in the commit body).

## Ledger re-audit

After every sync, re-read `UPSTREAM-LEDGER.md` against what upstream just
landed:

- Did upstream solve something a **PR wave** carries? Move it to
  SUPERSEDED/PARTIAL, with the proof (our regression test passing against
  upstream's fix).
- Did upstream land adjacent work that changes a wave's pitch? Note it on the
  wave row (e.g. upstream fixing Redis retryability strengthens the case for
  our transport-retryability wave).
- Update the "Baseline at last audit" line (upstream tip + fork-ahead count).

## Commit shape

- Merge commit: `Merge upstream/main (<tip>, +<count>)` — the script writes
  this.
- Ledger/runbook follow-up: conventional style, ending with the standing
  `Co-Authored-By` trailer.
- Push: `git push origin nix-cache`.
