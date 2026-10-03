#!/usr/bin/env bash
# Repro for the inert "exec: on" class: extracts the setup-nativelink-cloud
# run script and executes it with exec requested but ci-mode=cache (the
# canary's pinned value and the common repo-var state). Before the fix the
# action printed "exec=false" buried in a success line with no warning and
# no assertable output; after the fix it emits a ::warning and exec=false
# in GITHUB_OUTPUT.
set -euo pipefail
cd "$(dirname "$0")/../.."
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
# Extract the composite step's bash from the action yaml.
python3 - "$tmp/step.sh" <<'PY'
import sys, yaml
a = yaml.safe_load(open(".github/actions/setup-nativelink-cloud/action.yaml"))
open(sys.argv[1], "w").write(a["runs"]["steps"][0]["run"])
PY
cd "$tmp"
export NL_API_KEY=dummy-key NL_CLAIM_BASE=claim.example.net NL_BES_RESULTS_URL= \
  NL_PUBLIC_KEY=pub NL_PUBLIC_CLAIM_BASE=pub.example.net \
  NL_CI_MODE=cache NL_MODE=read NL_EXEC=on NL_CONTAINER_IMAGE=debian:bookworm-slim \
  RUNNER_OS=Linux GITHUB_OUTPUT="$tmp/out"
: > "$GITHUB_OUTPUT"
out=$(bash step.sh)
echo "$out"
echo "--- GITHUB_OUTPUT ---"; cat "$GITHUB_OUTPUT"
echo "$out" | grep -q '::warning title=remote execution not honored::' \
  || { echo "FAIL: exec: on was silently ignored (no warning emitted)"; exit 1; }
grep -q '^exec=false' "$GITHUB_OUTPUT" \
  || { echo "FAIL: no assertable exec output"; exit 1; }
echo "PASS: inert exec: on is loud and assertable"
