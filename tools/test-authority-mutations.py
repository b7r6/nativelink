#!/usr/bin/env python3
"""Run semantic authority mutants in an isolated copy of the working tree.

The baseline and each mutant run real Cargo contract tests. A compilation error
is an invalid mutant, never a killed mutant. The caller's checkout is untouched.
M1's proposal gate is a simulator, so its early-apply mutant targets that gate;
the remaining mutants change the production reducer/snapshot implementation.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


CORE = Path("nativelink-scheduler/src/authority/mod.rs")
HARNESS = Path("nativelink-scheduler/tests/authority/harness.rs")
JOURNAL = Path("nativelink-scheduler/src/authority/journal.rs")
SNAPSHOT_BUFFER = Path("nativelink-scheduler/src/authority/snapshot_buffer.rs")
COARSE = Path("nativelink-scheduler/tests/retention/coarse.rs")


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError(f"mutation anchor must occur once: {old!r}")
    return source.replace(old, new, 1)


def remove_attempt_fence(source):
    start = source.index("    fn authorized_attempt(")
    body = source.index("        let entry = ", start)
    end = source.index("\n    fn worker_accepted(", body)
    return (
        source[:body]
        + "        let _ = (attempt, worker, incarnation);\n        Ok(())\n    }\n"
        + source[end:]
    )


def toggle_refund(source):
    start = source.index("            Event::Cleanup {")
    end = source.index("            Event::EffectOutcome", start)
    section = source[start:end]
    section = replace_once(
        section,
        "                if reservation.released {\n                    return Reply::Ignored;\n                }\n",
        "",
    )
    section = replace_once(
        section,
        "reservation.released = true;",
        "reservation.released = !reservation.released;",
    )
    return source[:start] + section + source[end:]


def conflate_lookup_policy(source):
    old = "    fn submit(&mut self, operation: OperationId, request: Request) -> Reply {"
    new = """    fn submit(&mut self, operation: OperationId, mut request: Request) -> Reply {
        if request.skip_cache_lookup { request.do_not_cache = true; }"""
    return replace_once(source, old, new)


MUTANTS = [
    (
        "remove_attempt_incarnation_fence",
        CORE,
        remove_attempt_fence,
        "stale_attempt_cannot_complete_successor_or_refund_unknown_work",
        "AUTH 1 / AUTH 2",
    ),
    (
        "refund_duplicate_cleanup_twice",
        CORE,
        toggle_refund,
        "stale_attempt_cannot_complete_successor_or_refund_unknown_work",
        "RES 1",
    ),
    (
        "omit_pending_effects_from_snapshot",
        CORE,
        lambda s: replace_once(
            s,
            "state: self.clone(),",
            "state: { let mut broken = self.clone(); broken.effects.clear(); broken },",
        ),
        "uncertain_dispatch_survives_snapshot_and_sender_loss",
        "REC 1 / EFFECT 2",
    ),
    (
        "apply_uncommitted_proposal",
        HARNESS,
        lambda s: replace_once(
            s,
            "        self.proposals.insert(id, event);",
            "        self.core.apply(event.clone());\n        self.proposals.insert(id, event);",
        ),
        "proposals_and_commits_cannot_dispatch_before_apply",
        "EFFECT 1 (M1 simulated boundary)",
    ),
    (
        "conflate_skip_lookup_and_cacheability",
        CORE,
        conflate_lookup_policy,
        "skip_lookup_never_joins_lookup_but_can_join_actual_work_and_publish",
        "LOOKUP 1",
    ),
    (
        "reverse_numeric_priority",
        CORE,
        lambda s: replace_once(s, "queue.sort_unstable();", "queue.sort_unstable_by(|a, b| b.cmp(a));"),
        "numeric_priority_and_fifo_age_survive_joins",
        "fixed priority/FIFO contract",
    ),
    (
        "refund_revocation_without_quiescence",
        CORE,
        lambda s: replace_once(s, "        if quiescent {", "        if true {"),
        "stale_attempt_cannot_complete_successor_or_refund_unknown_work",
        "RES 1 / RES 2",
    ),
    (
        "ignore_unknown_authority_section",
        JOURNAL,
        lambda s: replace_once(s, "        || envelope.required_sections != [REQUIRED_SECTION]\n", ""),
        "unsupported_snapshot_versions_sections_and_corruption_fail_closed",
        "REC 2 (required authority-bearing sections)",
    ),
    (
        "accept_unvalidated_response_facts",
        CORE,
        lambda s: replace_once(s,
            "        if !self.payload_fits(&result) || result.validate_response().is_err() {",
            "        if !self.payload_fits(&result) {"),
        "adversarial::captured_response_facts_refine_oracle_and_preserve_terminal_and_publication_bytes",
        "accepted response bytes agree with normalized authority facts",
    ),
    (
        "install_valid_prefix_after_failed_snapshot_stream",
        SNAPSHOT_BUFFER,
        lambda s: replace_once(s, "        self.failed = true;", "        self.failed = false;"),
        "oversized_snapshot_stream_cannot_install_an_earlier_valid_prefix",
        "failed snapshot streams cannot install an earlier valid prefix",
    ),
    (
        "backfill_reserved_hold_capacity",
        CORE,
        lambda s: replace_once(s, "                        && self.hold_permits(execution, worker.id)\n", ""),
        "adversarial::aged_one_completion_wait_cannot_be_starved_by_repeated_small_backfill",
        "aged work accumulates capacity despite repeated lower-priority backfill",
    ),
    (
        "expect_revoked_uncertain_charges_to_finish",
        CORE,
        lambda s: replace_once(s,
            "\n        let floor = self.immovable_charge(worker.id);",
            "\n        let floor = Resources::default();"),
        "adversarial::uncertain_capacity_is_an_immovable_floor_and_cleanup_can_place_holder_elsewhere",
        "holds cannot assume revoked uncertain work will release capacity",
    ),
    (
        "retry_memory_failure_before_quiescence",
        CORE,
        lambda s: replace_once(s,
            "        if !quiescent\n            || self.reservations[&entry.reservation].charged != charged",
            "        if self.reservations[&entry.reservation].charged != charged"),
        "adversarial::memory_failure_rejects_uncertain_or_contradictory_evidence_without_mutation",
        "memory retry cannot refund an uncertain old process",
    ),
    (
        "grow_memory_ignoring_immovable_cpu_and_slots",
        CORE,
        lambda s: replace_once(replace_once(s,
            "cpu_millis: worker.capacity.cpu_millis.checked_sub(floor.cpu_millis)?,",
            "cpu_millis: worker.capacity.cpu_millis,"),
            "slots: worker.capacity.slots.checked_sub(floor.slots)?,",
            "slots: worker.capacity.slots,"),
        "adversarial::memory_growth_subtracts_immovable_cpu_and_slots_before_choosing_a_ceiling",
        "one real worker must satisfy every resource dimension above uncertain charges",
    ),
    (
        "admit_forged_request_projection",
        CORE,
        lambda s: replace_once(s, "            || request.validate_payload().is_err()\n", ""),
        "adversarial::captured_request_raw_fields_reject_forgery_without_consuming_an_operation",
        "committed request facts agree with preserved raw ingress",
    ),
]

RETENTION_MUTANTS = [
    (
        "acquire_into_closed_cohort", COARSE,
        lambda s: replace_once(s,
            "if !matches!(record.phase, Phase::Allocating | Phase::Sealed)\n                        || fences.get(&cohort) != Some(&record.fence)",
            "if fences.get(&cohort) != Some(&record.fence)"),
        "staging_transfer_is_gapless_even_after_acquisition_closes",
        "closed cohort fences new protection",
    ),
    (
        "renew_expired_root_without_reacquisition", COARSE,
        lambda s: replace_once(s,
            "if existing.lease != lease || existing.until <= self.now {",
            "if existing.lease != lease {"),
        "expired_renewal_is_not_reacquisition_and_old_lease_cannot_revoke_new_one",
        "expired renewal cannot revive retired generations",
    ),
    (
        "release_erases_captured_manifest", COARSE,
        lambda s: replace_once(s,
            "                record.until = self.now;\n            }\n            Event::Transfer",
            "                record.until = self.now;\n                for cohort in self.cohorts.values_mut() {\n                    cohort.captured.retain(|entry| entry.root != root);\n                }\n            }\n            Event::Transfer"),
        "serving_coverage_survives_publication_removal_and_conservative_mark",
        "mark capture survives root removal",
    ),
    (
        "old_delete_ack_settles_new_orphan", COARSE,
        lambda s: replace_once(s,
            "if generation.current_delete == Some(delete)\n                    && generation.write_revision == record.write_revision\n                {",
            "if true {"),
        "late_write_requires_fresh_delete_and_old_locator_never_targets_recreation",
        "delete outcomes are generation and revision fenced",
    ),
    (
        "delete_success_forgets_unresolved_writes", COARSE,
        lambda s: replace_once(s,
            "                record.confirmed = true;\n                let generation = self.generations.get_mut(&record.key).ok_or(())?;",
            "                record.confirmed = true;\n                self.writes.retain(|_, write| write.key != record.key || write.outcome.is_some());\n                let generation = self.generations.get_mut(&record.key).ok_or(())?;"),
        "late_write_requires_fresh_delete_and_old_locator_never_targets_recreation",
        "successful delete does not establish write quiescence",
    ),
]


def run_test(checkout, evidence, name, test_filter, target_dir, test_target=None):
    command = [
        "cargo", "test", "--locked", "--offline", "-p", "nativelink-scheduler",
        "--profile=smol", "-j", "24",
    ]
    targets = ([test_target] if isinstance(test_target, str) else test_target) or ["authority_test", "authority_raft_test"]
    for target in targets:
        command.extend(["--test", target])
    if test_filter:
        command.append(test_filter)
    env = dict(os.environ, CARGO_TARGET_DIR=str(target_dir))
    env.pop("NL_AUTHORITY_EVIDENCE_DIR", None)
    start = time.monotonic()
    log = evidence / f"{name}.txt"
    with log.open("w") as output:
        status = subprocess.run(command, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT).returncode
    text = log.read_text()
    # Require evidence of execution and the selected assertion failing. Rustc
    # rejection, missing dependency, or an unmatched filter is not a kill.
    executed = all(f"Running tests/{target}.rs" in text for target in targets)
    failed_test = bool(test_filter) and f"test {test_filter} ... FAILED" in text
    return {
        "name": name, "command": command, "status": status,
        "seconds": time.monotonic() - start, "executed": executed,
        "selected_test_failed": failed_test, "log": log.name,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path)
    parser.add_argument("--suite", choices=("authority", "retention", "all"), default="authority")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    evidence = args.output.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    target = (args.target_dir or root / "target/categorical-mutations").resolve()
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    selected = (MUTANTS if args.suite != "retention" else []) + (RETENTION_MUTANTS if args.suite != "authority" else [])
    baseline_targets = (["authority_test", "authority_raft_test"] if args.suite != "retention" else []) + (["retention_test"] if args.suite != "authority" else [])
    report = {"repository_head": revision, "working_tree_included": True, "suite": args.suite, "mutants": []}
    with tempfile.TemporaryDirectory(prefix="nativelink-authority-mutants-") as scratch:
        checkout = Path(scratch)
        paths = subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root
        ).decode().split("\0")
        for relative in set(paths) - {""}:
            source = root / relative
            if not source.is_file():
                continue
            destination = checkout / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
        report["source_sha256"] = {
            relative: hashlib.sha256((checkout / relative).read_bytes()).hexdigest()
            for relative in sorted(set(paths) - {""})
            if (checkout / relative).is_file()
            and (relative in ("Cargo.lock", "Cargo.toml")
                 or relative.startswith("nativelink-scheduler/"))
        }
        baseline = run_test(checkout, evidence, "baseline", None, target, baseline_targets)
        report["baseline"] = baseline
        if baseline["status"] != 0 or not baseline["executed"]:
            (evidence / "mutations.json").write_text(json.dumps(report, indent=2) + "\n")
            raise SystemExit("mutation baseline failed; see baseline.txt")
        for name, relative, mutate, test_filter, property_name in selected:
            path = checkout / relative
            original = path.read_text()
            try:
                path.write_text(mutate(original))
                test_target = "retention_test" if relative == COARSE else ("authority_raft_test" if relative in (JOURNAL, SNAPSHOT_BUFFER) else "authority_test")
                outcome = run_test(checkout, evidence, name, test_filter, target, test_target)
                outcome["property"] = property_name
                outcome["killed"] = outcome["status"] != 0 and outcome["executed"] and outcome["selected_test_failed"]
                report["mutants"].append(outcome)
                print(json.dumps({"mutant": name, "killed": outcome["killed"]}), flush=True)
            finally:
                path.write_text(original)
    report["all_killed"] = all(item["killed"] for item in report["mutants"])
    (evidence / "mutations.json").write_text(json.dumps(report, indent=2) + "\n")
    raise SystemExit(0 if report["all_killed"] else 1)


if __name__ == "__main__":
    main()
