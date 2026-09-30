#!/usr/bin/env bash
# Copyright 2026 The NativeLink Authors. All rights reserved.
#
# Three-box sessions-as-liveness proof, driven from anywhere with ssh to
# the fleet. Forms a 3-member embedded-keeper quorum, registers an
# ephemeral worker node from box A, watches it from box B, then SIGKILLs
# the process on box A and asserts the DELETED event reaches box B.
#
# Prereq on each box: $FORGE/nativelink-keeper with examples/node built
# (see fleet/dist.sh which fans the built artifacts out from the build
# host). Config below via env or defaults to the straylight lab.
set -uo pipefail

A=${A:-b7r6@100.71.82.73}       # ultraviolence — worker whose death we prove
B=${B:-b7r6@100.89.101.109}     # guccimane     — the observer
C=${C:-b7r6@100.111.80.81}      # weyl          — quorum ballast
PORT=${PORT:-29871}
FORGE=${FORGE:-keeper-forge}
ENSEMBLE="1=100.71.82.73:$PORT,2=100.89.101.109:$PORT,3=100.111.80.81:$PORT"
NODE="$FORGE/nativelink-keeper/target/release/examples/node"
SESSION_MS=${SESSION_MS:-3000}

run() { ssh -o BatchMode=yes -o ConnectTimeout=15 "$1" "${@:2}"; }
unit() { # host name args... — detached, ssh-flap-proof
  run "$1" "systemctl --user reset-failed nlk-$2 2>/dev/null; systemd-run --user --unit=nlk-$2 --collect bash -lc '$NODE ${*:3} > $FORGE/nlk-$2.log 2>&1'"
}

echo "== starting quorum"
unit "$C" c --dir "\$HOME/$FORGE/data-c" --id 3 --ensemble "$ENSEMBLE" --session-timeout-ms $SESSION_MS
unit "$B" b --dir "\$HOME/$FORGE/data-b" --id 2 --ensemble "$ENSEMBLE" --session-timeout-ms $SESSION_MS \
     --watch /workers/uv
unit "$A" a --dir "\$HOME/$FORGE/data-a" --id 1 --ensemble "$ENSEMBLE" --session-timeout-ms $SESSION_MS \
     --register /workers/uv

echo "== waiting for READY on all three"
for pair in "$A:a" "$B:b" "$C:c"; do
  h=${pair%:*}; n=${pair#*:}
  for _ in $(seq 1 60); do
    run "$h" "grep -q READY $FORGE/nlk-$n.log 2>/dev/null" && break
    sleep 2
  done
  run "$h" "grep -m1 READY $FORGE/nlk-$n.log" || { echo "FAIL: $n never READY"; exit 1; }
done

echo "== observer must see the registration"
for _ in $(seq 1 30); do
  run "$B" "grep -q 'EVENT CREATED /workers/uv' $FORGE/nlk-b.log 2>/dev/null" && break
  sleep 1
done
run "$B" "grep -m1 'EVENT CREATED /workers/uv' $FORGE/nlk-b.log" \
  || { echo "FAIL: observer never saw registration"; exit 1; }

echo "== SIGKILL the worker process on A (no goodbye)"
run "$A" "systemctl --user kill -s SIGKILL nlk-a"

echo "== ephemeral must be reaped by session expiry, observed on B"
deadline=$(( SESSION_MS / 1000 + 30 ))
for _ in $(seq 1 "$deadline"); do
  run "$B" "grep -q 'EVENT DELETED /workers/uv' $FORGE/nlk-b.log 2>/dev/null" && {
    echo "FLEET-PROOF-GREEN: ephemeral died with its session across the quorum"
    run "$B" "systemctl --user stop nlk-b" ; run "$C" "systemctl --user stop nlk-c"
    exit 0
  }
  sleep 1
done
echo "FAIL: DELETED never observed on B within ${deadline}s"
exit 1
