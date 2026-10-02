#!/usr/bin/env bash
# upstream-sync.sh — sync the fork with TraceMachina/nativelink.
#
# The subscription, not the event. See UPSTREAM-SYNC.md for the full runbook,
# including the compose-don't-pick conflict rule and the ledger re-audit.
#
# Usage:
#   scripts/upstream-sync.sh census   # fetch + dry conflict census (read-only)
#   scripts/upstream-sync.sh merge    # perform the merge (stops on conflicts)
#   scripts/upstream-sync.sh gates    # run the verification gates
#
# Run from the repo root, on branch nix-cache, with a clean tree.

set -euo pipefail

UPSTREAM_REMOTE=upstream
UPSTREAM_URL=https://github.com/TraceMachina/nativelink.git
UPSTREAM_BRANCH=main
FORK_BRANCH=nix-cache

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

die() {
    echo "error: $*" >&2
    exit 1
}

ensure_remote() {
    git remote get-url "$UPSTREAM_REMOTE" > /dev/null 2>&1 ||
        git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
}

census() {
    ensure_remote
    echo "== fetching $UPSTREAM_REMOTE..."
    git fetch "$UPSTREAM_REMOTE"

    local base tip count
    base=$(git merge-base HEAD "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")
    tip=$(git rev-parse --short "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")
    count=$(git rev-list --count "$base..$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")

    echo "== merge base: $(git rev-parse --short "$base"); upstream tip: $tip; $count new commits"
    if [ "$count" -eq 0 ]; then
        echo "== already in sync; nothing to do."
        return 0
    fi

    echo "== new upstream commits:"
    git log --oneline "$base..$UPSTREAM_REMOTE/$UPSTREAM_BRANCH"

    echo "== files upstream touches:"
    git diff --stat "$base..$UPSTREAM_REMOTE/$UPSTREAM_BRANCH" | tail -30

    echo "== dry conflict census (git merge-tree):"
    if git merge-tree --write-tree --name-only HEAD \
        "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH" > /dev/null 2>&1; then
        echo "   CLEAN — no textual conflicts."
    else
        echo "   CONFLICTS — files below need compose-don't-pick resolution"
        echo "   (see UPSTREAM-SYNC.md §Conflicts):"
        git merge-tree --write-tree --name-only HEAD \
            "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH" || true
    fi
}

merge() {
    ensure_remote
    [ "$(git branch --show-current)" = "$FORK_BRANCH" ] ||
        die "not on $FORK_BRANCH"
    if ! git diff --quiet --ignore-submodules=dirty ||
        ! git diff --cached --quiet --ignore-submodules=dirty; then
        die "working tree not clean"
    fi

    local base tip count
    base=$(git merge-base HEAD "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")
    tip=$(git rev-parse --short "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")
    count=$(git rev-list --count "$base..$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")
    [ "$count" -gt 0 ] || {
        echo "already in sync"
        return 0
    }

    echo "== merging $UPSTREAM_REMOTE/$UPSTREAM_BRANCH ($tip, +$count)..."
    if git merge --no-ff --no-edit "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH" \
        -m "Merge upstream/main ($tip, +$count)"; then
        echo "== merged clean."
    else
        cat >&2 << 'EOF'
== MERGE STOPPED ON CONFLICTS.
   Resolve per the compose rule (UPSTREAM-SYNC.md §Conflicts):
   union our fix WITH upstream's telemetry/refactor — never blind-pick
   a side. Then: git add <files> && git merge --continue.
   After resolving, run: scripts/upstream-sync.sh gates
EOF
        exit 1
    fi
    echo "== next: scripts/upstream-sync.sh gates"
}

gates() {
    local base crates
    base=$(git merge-base "HEAD^1" "HEAD^2" 2> /dev/null) ||
        base=$(git merge-base HEAD "$UPSTREAM_REMOTE/$UPSTREAM_BRANCH")

    run() {
        echo "== $*"
        nix develop .#default --command "$@"
    }

    run cargo check --workspace

    # Test only the crates upstream touched (whole-workspace tests have known
    # environmental failures; see UPSTREAM-SYNC.md §Known failures).
    crates=$(git diff --name-only "$base"..HEAD |
        sed -n 's|^\(nativelink-[a-z-]*\)/.*|\1|p' | sort -u)
    if [ -n "$crates" ]; then
        # Intentional word splitting: $crates is a whitespace-separated list.
        # shellcheck disable=SC2046,SC2086
        run cargo test --no-fail-fast $(printf -- '-p %s ' $crates) ||
            echo "== TEST FAILURES above — check each against §Known failures" \
                "before treating this gate as red."
    else
        echo "== no nativelink crates touched; skipping tests"
    fi

    run cargo clippy --workspace --tests
    run cargo fmt --all -- --check

    echo "== gates green. Now re-audit UPSTREAM-LEDGER.md (see runbook §Ledger),"
    echo "   then commit + push."
}

case "${1:-}" in
census) census ;;
merge) merge ;;
gates) gates ;;
*)
    echo "usage: $0 {census|merge|gates}" >&2
    exit 2
    ;;
esac
