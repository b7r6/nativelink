#!/usr/bin/env python3
# Copyright 2026 The NativeLink Authors. All rights reserved.
#
# Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    See LICENSE file for details
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Analyze paired azure_fleet runs; the independent observation is a seed/run.

Requires scipy and matplotlib. The Rust executable writes every measured byte;
this tool does not fit, simulate, rescale, or extrapolate those measurements.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from itertools import pairwise
from pathlib import Path

ARMS = ("baseline", "fixed")
OUTCOMES = {"success", "failed", "censored"}
MIB = 1024**2


def require(condition, message):
    if not condition:
        raise ValueError(message)


def wilson(successes, total):
    """95% Wilson interval for independent run-level Bernoulli outcomes."""
    if total == 0:
        return {"successes": 0, "n": 0, "proportion": None, "ci95": None}
    z = 1.959963984540054
    p = successes / total
    divisor = 1 + z * z / total
    center = (p + z * z / (2 * total)) / divisor
    radius = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total**2)) / divisor
    return {
        "successes": successes,
        "n": total,
        "proportion": p,
        "ci95": [max(0, center - radius), min(1, center + radius)],
    }


def quantile(values, probability):
    """Linear sample quantile (the usual type-7 convention)."""
    values = sorted(values)
    position = probability * (len(values) - 1)
    low = math.floor(position)
    high = math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def describe(values):
    from scipy.stats import t

    values = [float(v) for v in values if v is not None]
    require(all(math.isfinite(v) for v in values), "nonfinite statistic input")
    n = len(values)
    if not n:
        return {
            "n": 0,
            "mean": None,
            "sd": None,
            "ci95": None,
            "median": None,
            "p95": None,
            "p99": None,
        }
    mean = statistics.mean(values)
    sd = statistics.stdev(values) if n > 1 else None
    radius = float(t.ppf(0.975, n - 1)) * sd / math.sqrt(n) if n > 1 else None
    return {
        "n": n,
        "mean": mean,
        "sd": sd,
        "ci95": [mean - radius, mean + radius] if radius is not None else None,
        "median": quantile(values, 0.5),
        "p95": quantile(values, 0.95),
        "p99": quantile(values, 0.99),
    }


def checked_run(path, exploratory=False):
    raw = path.read_bytes()
    run = json.loads(raw)
    prefix = str(path)
    for field in (
        "schema_version",
        "arm",
        "scenario",
        "seed",
        "pair_order",
        "binary_sha256",
        "workload_sha256",
        "start_unix_s",
        "duration_s",
        "load",
        "fault",
        "jobs",
        "nodes",
        "account",
    ):
        require(field in run, f"{prefix}: missing {field}")
    require(run["schema_version"] == 1, f"{prefix}: unsupported schema")
    require(run["arm"] in ARMS, f"{prefix}: unsupported arm")
    require(run["scenario"] in {"control", "failed-fill"}, f"{prefix}: scenario")
    require(run["pair_order"] in (0, 1), f"{prefix}: pair_order must be 0 or 1")
    require(isinstance(run["seed"], int), f"{prefix}: integer seed required")
    require(run["duration_s"] > 0, f"{prefix}: duration must be positive")
    require(
        run["load"].get("before") and run["load"].get("after"),
        f"{prefix}: before/after host-load observations required",
    )
    for field in ("binary_sha256", "workload_sha256"):
        require(
            len(run[field]) == 64 and all(c in "0123456789abcdef" for c in run[field]),
            f"{prefix}: invalid {field}",
        )
    build = run.get("build", {})
    if not exploratory or build.get("arm") is not None:
        require(
            build.get("arm") == run["arm"],
            f"{prefix}: compiled arm does not match run label",
        )
        require(build.get("revision"), f"{prefix}: compiled source revision missing")
        source_hash = build.get("azure_source_sha256", "")
        require(
            len(source_hash) == 64
            and all(c in "0123456789abcdef" for c in source_hash),
            f"{prefix}: compiled Azure source hash missing/invalid",
        )
    account = run["account"]
    require(account.get("bucket_width_s", 0) > 0, f"{prefix}: bucket width missing")
    require(account.get("measurement_boundary"), f"{prefix}: byte boundary missing")
    require(not any(account["active"]), f"{prefix}: requests active at report boundary")
    buckets = account["buckets"]
    reads = account["reads"]
    payload = sum(b["bytes"] for b in buckets.values())
    repeated = sum(b["repeated_bytes"] for b in buckets.values())
    require(
        payload == account["payload_bytes"] == sum(r["bytes"] for r in reads),
        f"{prefix}: body-byte ledgers disagree",
    )
    require(
        repeated == sum(r["repeated_bytes"] for r in reads),
        f"{prefix}: repeated-byte ledgers disagree",
    )
    require(
        len(reads) == sum(b["get"] for b in buckets.values()),
        f"{prefix}: GET ledgers disagree",
    )
    require(
        all(0 <= r["repeated_bytes"] <= r["bytes"] <= r["length"] for r in reads),
        f"{prefix}: impossible request byte accounting",
    )
    require(
        all(r["bytes"] == 0 for r in reads if r["status"] not in (200, 206)),
        f"{prefix}: unsuccessful GET carries unclassified payload",
    )
    require(all(r["end_s"] is not None for r in reads), f"{prefix}: unfinished read")
    require(
        payload <= account["admitted_payload"], f"{prefix}: payload exceeds admission"
    )
    require(
        account["admitted_payload"]
        <= account["burst_bytes"] + account["cap_bytes_s"] * run["duration_s"] + 1,
        f"{prefix}: token-bucket admission conservation failed",
    )
    ids = [(j["phase"], j["id"]) for j in run["jobs"]]
    require(len(ids) == len(set(ids)), f"{prefix}: duplicate job identity")
    for job in run["jobs"]:
        require(job["phase"] in {"warmup", "work", "sentinel"}, f"{prefix}: job phase")
        require(job["outcome"] in OUTCOMES, f"{prefix}: job outcome")
        if job["outcome"] == "success":
            require(
                job.get("exit_code") == 0 and job.get("status_code") in (None, 0),
                f"{prefix}: success contradicts command/status result",
            )
            require(
                job.get("finished_s") is not None, f"{prefix}: success has no finish"
            )
            if not exploratory:
                require(
                    isinstance(job.get("outputs"), list) and job["outputs"],
                    f"{prefix}: successful action lacks output evidence",
                )
                for output in job["outputs"]:
                    require(
                        output.get("path")
                        and output.get("digest")
                        and len(output["digest"].get("hash", "")) == 64
                        and output["digest"].get("size_bytes", -1) >= 0,
                        f"{prefix}: invalid output digest",
                    )
    require(
        any(j["phase"] == "work" for j in run["jobs"]), f"{prefix}: no intended work"
    )
    node_ids = {n["node"] for n in run["nodes"]}
    require(len(node_ids) == len(run["nodes"]) > 0, f"{prefix}: node identities")
    require(
        all(n["pid"] > 0 and n["process_cpus"] > 0 for n in run["nodes"]),
        f"{prefix}: process provenance missing",
    )
    require(
        {j["node"] for j in run["jobs"] if j["phase"] == "sentinel"} == node_ids,
        f"{prefix}: a node has no sentinel outcome",
    )
    if not exploratory:
        require(
            isinstance(run.get("accepted"), bool),
            f"{prefix}: acceptance result missing",
        )
    if run.get("accepted") is True:
        require(
            all(
                j["outcome"] == "success"
                and all(a["outcome"] != "censored" for a in j.get("attempts", [j]))
                for j in run["jobs"]
            )
            and all(
                n.get("sentinel_same_process") is True
                and n.get("sentinel_cache_reused") is True
                for n in run["nodes"]
            ),
            f"{prefix}: accepted contradicts uncensored completion/cache-reuse evidence",
        )
    if run["scenario"] == "failed-fill":
        require(
            run["fault"] is not None
            and run["fault"]["repaired_s"] >= run["fault"]["started_s"],
            f"{prefix}: fault timing missing",
        )
    run["_source"] = str(path.resolve())
    run["_source_sha256"] = hashlib.sha256(raw).hexdigest()
    return run


def metrics(run):
    work = [j for j in run["jobs"] if j["phase"] == "work"]
    sentinels = [j for j in run["jobs"] if j["phase"] == "sentinel"]
    counts = Counter(j["outcome"] for j in work)
    attempts = [attempt for j in work for attempt in j.get("attempts", [j])]
    attempt_counts = Counter(a["outcome"] for a in attempts)
    succeeded = counts["success"]
    a = run["account"]
    requests = a["reads"]
    refused = sum(r["status"] == 503 for r in requests)
    repeated = sum(b["repeated_bytes"] for b in a["buckets"].values())
    same_process = all(n.get("sentinel_same_process") is True for n in run["nodes"])
    cache_reused = all(n.get("sentinel_cache_reused") is True for n in run["nodes"])
    sentinel_success = all(j["outcome"] == "success" for j in sentinels)
    all_success = succeeded == len(work)
    work_finished = [j["finished_s"] for j in work if j["finished_s"] is not None]
    submitted = [j["started_s"] for j in work if j["started_s"] is not None]
    repair_time = run["fault"]["repaired_s"] if run["fault"] else None
    # This includes queueing and the finite workload drain before the sentinels.
    # It is an observed upper bound on reuse time, not the instant capacity freed.
    recovered_at = max(j["finished_s"] for j in sentinels) if sentinel_success else None
    pre_sentinel = run.get("before_sentinel")
    scheduler = scheduler_audit(run)
    return {
        "payload_mib": a["payload_bytes"] / MIB,
        "repeated_mib": repeated / MIB,
        "peak_body_mib_s": max((b["bytes"] for b in a["buckets"].values()), default=0)
        / MIB
        / a["bucket_width_s"],
        "get_requests": len(requests),
        "throttled_get_requests": refused,
        "throttled_get_fraction": refused / len(requests) if requests else 0,
        "timing_outside_fit_requests": a.get("timing_outside_fit"),
        "intended_work": len(work),
        "completed_work": succeeded,
        "failed_work": counts["failed"],
        "censored_work": counts["censored"],
        "client_attempts": len(attempts),
        "failed_client_attempts": attempt_counts["failed"],
        "censored_client_attempts": attempt_counts["censored"],
        "completed_work_fraction": succeeded / len(work),
        "payload_mib_per_completed_action": a["payload_bytes"] / MIB / succeeded
        if all_success
        else None,
        "work_drain_s": max(work_finished) - min(submitted)
        if all_success and submitted
        else None,
        "repair_to_last_sentinel_s": max(0, recovered_at - repair_time)
        if recovered_at is not None and same_process and repair_time is not None
        else None,
        "sentinel_roundtrip_max_s": max(
            j["finished_s"] - j["started_s"] for j in sentinels
        )
        if sentinel_success
        else None,
        "sentinel_window_payload_mib": (
            a["payload_bytes"] - pre_sentinel["payload_bytes"]
        )
        / MIB
        if pre_sentinel is not None
        else None,
        "sentinel_window_get_requests": len(requests) - len(pre_sentinel["reads"])
        if pre_sentinel is not None
        else None,
        "final_scheduler_reserved_actions": scheduler["final_reserved_actions"],
        "maximum_sampled_worker_actions": scheduler["maximum_sampled_worker_actions"],
        "all_work_succeeded": all_success,
        "all_sentinels_succeeded_same_process": sentinel_success and same_process,
        "all_sentinels_cache_reused": cache_reused,
        "accepted": run.get("accepted") is True,
        "scheduler_accounting_passed": scheduler["passed"],
        "has_repeated_payload": repeated > 0,
    }


NUMERIC_METRICS = (
    "payload_mib",
    "repeated_mib",
    "peak_body_mib_s",
    "get_requests",
    "throttled_get_requests",
    "throttled_get_fraction",
    "timing_outside_fit_requests",
    "intended_work",
    "completed_work",
    "failed_work",
    "censored_work",
    "client_attempts",
    "failed_client_attempts",
    "censored_client_attempts",
    "completed_work_fraction",
    "payload_mib_per_completed_action",
    "work_drain_s",
    "repair_to_last_sentinel_s",
    "sentinel_roundtrip_max_s",
    "sentinel_window_payload_mib",
    "sentinel_window_get_requests",
    "final_scheduler_reserved_actions",
    "maximum_sampled_worker_actions",
)
RUN_PROPORTIONS = (
    "all_work_succeeded",
    "all_sentinels_succeeded_same_process",
    "all_sentinels_cache_reused",
    "accepted",
    "scheduler_accounting_passed",
    "has_repeated_payload",
)


def scheduler_audit(run):
    snapshots = run.get("scheduler_snapshots", [])
    if not snapshots:
        return {
            "samples": 0,
            "passed": False,
            "final_reserved_actions": None,
            "maximum_sampled_worker_actions": None,
            "reason": "no scheduler snapshots",
        }
    final = snapshots[-1]
    workers = [w for snapshot in snapshots for w in snapshot["workers"]]
    original = {n["worker_id"] for n in run["nodes"]}
    final_ids = {w["id"] for w in final["workers"]}
    final_reserved = sum(w["running_actions"] for w in final["workers"])
    sentinel_finishes = [
        j["finished_s"]
        for j in run["jobs"]
        if j["phase"] == "sentinel" and j.get("finished_s") is not None
    ]
    after_sentinel = bool(sentinel_finishes) and final["time_s"] >= max(
        sentinel_finishes
    )
    limits = bool(workers) and all(
        w["max_inflight_tasks"] == 2 and 0 <= w["running_actions"] <= 2 for w in workers
    )
    passed = (
        final_ids == original
        and len(final["workers"]) == len(original)
        and final_reserved == 0
        and after_sentinel
        and limits
    )
    return {
        "samples": len(snapshots),
        "passed": passed,
        "final_snapshot_s": final["time_s"],
        "snapshot_age_at_report_s": run["duration_s"] - final["time_s"],
        "final_original_membership": final_ids == original,
        "final_reserved_actions": final_reserved,
        "final_snapshot_after_sentinel": after_sentinel,
        "sampled_counts_within_configured_two": limits,
        "maximum_sampled_worker_actions": max(
            (w["running_actions"] for w in workers), default=0
        ),
        "scope": "scheduler reservations sampled every 200 ms; does not bound between-sample or unaccounted physical execution",
    }


def pair_runs(runs, exploratory):
    groups = defaultdict(dict)
    for run in runs:
        key = (run["scenario"], run["seed"])
        require(run["arm"] not in groups[key], f"duplicate {key}/{run['arm']}")
        groups[key][run["arm"]] = run
    incomplete = [key for key, arms in groups.items() if set(arms) != set(ARMS)]
    require(exploratory or not incomplete, f"unpaired seeds: {incomplete}")
    paired = defaultdict(list)
    for (scenario, seed), arms in sorted(groups.items()):
        if set(arms) != set(ARMS):
            continue
        before, after = (arms[arm] for arm in ARMS)
        require(
            before["workload_sha256"] == after["workload_sha256"],
            f"{scenario}/{seed}: different workloads",
        )
        require(
            {before["pair_order"], after["pair_order"]} == {0, 1},
            f"{scenario}/{seed}: duplicate/missing condition order",
        )
        first, second = sorted((before, after), key=lambda r: r["pair_order"])
        require(
            first["start_unix_s"] < second["start_unix_s"],
            f"{scenario}/{seed}: pair_order contradicts wall-clock starts",
        )
        require(
            before["binary_sha256"] != after["binary_sha256"],
            f"{scenario}/{seed}: both arms have the same binary",
        )
        if not exploratory or all(
            r.get("build", {}).get("arm") is not None for r in (before, after)
        ):
            require(
                before["build"]["revision"] == after["build"]["revision"],
                f"{scenario}/{seed}: baseline revisions differ",
            )
            require(
                before["build"]["azure_source_sha256"]
                != after["build"]["azure_source_sha256"],
                f"{scenario}/{seed}: Azure source hashes are identical",
            )
        # arrival_s is the measured account-relative phase origin plus the fixed
        # schedule; only the manifest hash identifies equal intended schedules.
        manifest = lambda r: sorted((j["phase"], j["id"], j["node"]) for j in r["jobs"])
        require(
            manifest(before) == manifest(after),
            f"{scenario}/{seed}: job manifest differs",
        )
        for field in (
            "cap_bytes_s",
            "burst_bytes",
            "bucket_width_s",
            "measurement_boundary",
        ):
            require(
                before["account"][field] == after["account"][field],
                f"{scenario}/{seed}: different {field}",
            )
        cpus = lambda r: sorted((n["node"], n["process_cpus"]) for n in r["nodes"])
        require(
            cpus(before) == cpus(after), f"{scenario}/{seed}: process CPU counts differ"
        )
        paired[scenario].append(arms)
    require(paired, "no complete baseline/fixed pairs")
    warnings = [
        f"Excluded unpaired seed {key}; retained in source inventory"
        for key in incomplete
    ]
    for scenario, pairs in paired.items():
        for arm in ARMS:
            require(
                len({p[arm]["binary_sha256"] for p in pairs}) == 1,
                f"{scenario}/{arm}: binary changed within the batch",
            )
        require(
            exploratory or len(pairs) >= 10,
            f"{scenario}: {len(pairs)} pairs; need >=10 or --exploratory",
        )
        chronological = sorted(
            pairs, key=lambda p: min(r["start_unix_s"] for r in p.values())
        )
        first_arms = [
            next(a for a in ARMS if p[a]["pair_order"] == 0) for p in chronological
        ]
        alternating = all(a != b for a, b in pairwise(first_arms))
        require(
            exploratory or alternating,
            f"{scenario}: conditions were not alternated by pair",
        )
        if not alternating:
            warnings.append(f"{scenario}: condition order was not alternated")
        if len(pairs) < 10:
            warnings.append(f"{scenario}: only {len(pairs)} pairs; exploratory")
    return paired, warnings


def summarize(paired):
    summaries = {}
    for scenario, pairs in paired.items():
        per_arm = {arm: [metrics(p[arm]) for p in pairs] for arm in ARMS}

        def output_map(run):
            return {
                j["id"]: json.dumps(
                    sorted(j["outputs"], key=lambda o: o["path"]), sort_keys=True
                )
                for j in run["jobs"]
                if j["phase"] == "work"
                and j["outcome"] == "success"
                and isinstance(j.get("outputs"), list)
                and j["outputs"]
            }

        equal_outputs = [
            output_map(p["baseline"]) == output_map(p["fixed"])
            and len(output_map(p["baseline"]))
            == per_arm["baseline"][i]["intended_work"]
            for i, p in enumerate(pairs)
        ]
        compatible = [
            i
            for i in range(len(pairs))
            if all(
                per_arm[a][i]["all_work_succeeded"]
                and per_arm[a][i]["all_sentinels_succeeded_same_process"]
                and per_arm[a][i]["all_sentinels_cache_reused"]
                and per_arm[a][i]["accepted"]
                and per_arm[a][i]["scheduler_accounting_passed"]
                and per_arm[a][i]["censored_client_attempts"] == 0
                for a in ARMS
            )
            and equal_outputs[i]
        ]
        summaries[scenario] = {
            "paired_seeds": [p["baseline"]["seed"] for p in pairs],
            "representative_trace_seed": sorted(
                pairs, key=lambda p: p["baseline"]["account"]["payload_bytes"]
            )[len(pairs) // 2]["baseline"]["seed"],
            "trace_selection": "upper median baseline payload, stable seed order for ties; treatment difference is not consulted",
            "n_pairs": len(pairs),
            "fully_successful_recovered_pairs": len(compatible),
            "matching_final_output_pairs": sum(equal_outputs),
            "output_comparison": [
                {
                    "seed": p["baseline"]["seed"],
                    "all_work_output_digests_match": equal_outputs[i],
                }
                for i, p in enumerate(pairs)
            ],
            "arms": {
                arm: {
                    "metrics": {
                        key: describe([m[key] for m in per_arm[arm]])
                        for key in NUMERIC_METRICS
                    },
                    "run_proportions": {
                        key: wilson(sum(m[key] for m in per_arm[arm]), len(pairs))
                        for key in RUN_PROPORTIONS
                    },
                }
                for arm in ARMS
            },
            "paired_difference_fixed_minus_baseline": {
                key: describe(
                    [
                        per_arm["fixed"][i][key] - per_arm["baseline"][i][key]
                        for i in range(len(pairs))
                        if per_arm["fixed"][i][key] is not None
                        and per_arm["baseline"][i][key] is not None
                    ]
                )
                for key in NUMERIC_METRICS
            },
            # Bytes remain observable in failed runs; their economic benefit does not.
            "payload_difference_on_fully_successful_recovered_pairs_mib": describe(
                [
                    per_arm["fixed"][i]["payload_mib"]
                    - per_arm["baseline"][i]["payload_mib"]
                    for i in compatible
                ]
            ),
            "all_pairs_have_equal_successful_work_and_recovery": len(compatible)
            == len(pairs),
        }
    return summaries


def time_series(run):
    width = run["account"]["bucket_width_s"]
    count = (
        max(
            [int(k) for k in run["account"]["buckets"]]
            + [math.floor(run["duration_s"] / width)]
        )
        + 1
    )
    payload = [0.0] * count
    repeated = [0.0] * count
    for key, bucket in run["account"]["buckets"].items():
        payload[int(key)] = bucket["bytes"] / MIB / width
        repeated[int(key)] = bucket["repeated_bytes"] / MIB
    running = 0.0
    for i, value in enumerate(repeated):
        running += value
        repeated[i] = running
    reference = (
        run["fault"]["started_s"]
        if run["fault"]
        else min(
            j["started_s"]
            for j in run["jobs"]
            if j["phase"] == "work" and j["started_s"] is not None
        )
    )
    return [(i + 0.5) * width - reference for i in range(count)], payload, repeated


def normalize_svg(path):
    """Replace exporter hashes with stable IDs and remove trailing whitespace."""
    text = path.read_text()
    ids = re.findall(r'\bid="([^"]+)"', text)
    require(len(ids) == len(set(ids)), f"{path}: duplicate SVG IDs")
    names = {old: f"element{i:04d}" for i, old in enumerate(ids, 1)}
    text = re.sub(r'\bid="([^"]+)"', lambda m: f'id="{names[m[1]]}"', text)
    text = re.sub(r"url\(#([^)]+)\)", lambda m: f"url(#{names[m[1]]})", text)
    text = re.sub(
        r'((?:xlink:)?href=")#([^"]+)(")',
        lambda m: f"{m[1]}#{names[m[2]]}{m[3]}",
        text,
    )
    path.write_text("\n".join(line.rstrip() for line in text.splitlines()) + "\n")


def plot(paired, summaries, output):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "svg.fonttype": "none",
        }
    )
    colors = {"baseline": "#b74434", "fixed": "#167d8d"}
    for scenario, pairs in paired.items():
        # Select before inspecting the paired difference: median baseline bytes.
        selected = next(
            p
            for p in pairs
            if p["baseline"]["seed"] == summaries[scenario]["representative_trace_seed"]
        )
        fig, axes = plt.subplots(2, 2, figsize=(13, 8), layout="constrained")
        for arm in ARMS:
            run = selected[arm]
            time, rate, repeat = time_series(run)
            axes[0, 0].plot(time, rate, label=arm, color=colors[arm], linewidth=1.15)
            axes[0, 1].plot(time, repeat, label=arm, color=colors[arm], linewidth=1.6)
            if run["fault"]:
                repair = run["fault"]["repaired_s"] - run["fault"]["started_s"]
                for ax in axes[0]:
                    ax.axvline(repair, color=colors[arm], alpha=0.5, linestyle=":")
            ys = [metrics(p[arm])["payload_mib"] for p in pairs]
            xs = list(range(1, len(pairs) + 1))
            axes[1, 0].scatter(xs, ys, label=arm, color=colors[arm], s=24)
            completed = [metrics(p[arm])["completed_work"] for p in pairs]
            axes[1, 1].plot(
                xs,
                completed,
                "o" if arm == "baseline" else "x",
                label=f"{arm}: successful",
                color=colors[arm],
                markersize=7,
            )
        cap = selected["baseline"]["account"]["cap_bytes_s"] / MIB
        axes[0, 0].axhline(
            cap, color="#52616b", linestyle="--", label="nominal admission rate"
        )
        axes[0, 0].set(
            title="Shared account: body bytes yielded to HTTP",
            ylabel="MiB/s (100 ms bins)",
        )
        axes[0, 1].set(
            title="Repeated ranges within the same worker", ylabel="Cumulative MiB"
        )
        for ax in axes[0]:
            ax.set_xlabel(
                "Seconds since fault start"
                if scenario == "failed-fill"
                else "Seconds since work start"
            )
            ax.grid(alpha=0.16)
            ax.legend(frameon=False, fontsize=8)
        for i, pair in enumerate(pairs, 1):
            axes[1, 0].plot(
                [i, i],
                [metrics(pair[a])["payload_mib"] for a in ARMS],
                color="#b8c1c6",
                zorder=0,
            )
        intended = [metrics(p["baseline"])["intended_work"] for p in pairs]
        axes[1, 1].plot(
            range(1, len(pairs) + 1),
            intended,
            "_",
            color="#52616b",
            markersize=14,
            label="intended (same manifest)",
        )
        axes[1, 0].set(
            title="Every paired seed",
            xlabel="Pair (seed order)",
            ylabel="Total payload MiB",
        )
        axes[1, 1].set(
            title="Work completed; failures remain in denominator",
            xlabel="Pair (seed order)",
            ylabel="Actions",
        )
        axes[1, 1].set_ylim(0, max(intended) * 1.15)
        for ax in axes[1]:
            ax.grid(alpha=0.16)
            ax.legend(frameon=False, fontsize=8)
        summary = summaries[scenario]
        recovery = ", ".join(
            f"{a} {summary['arms'][a]['run_proportions']['all_sentinels_succeeded_same_process']['successes']}/{len(pairs)}"
            for a in ARMS
        )
        fig.suptitle(
            f"Azure fleet fixture · {scenario} · {len(pairs)} paired seeds\n"
            f"Trace seed {selected['baseline']['seed']} (median baseline bytes); same-process recovery: {recovery}",
            fontsize=13,
        )
        fig.text(
            0.5,
            -0.025,
            "Loopback HTTP body production, not Azure billing. Burst admission permits short peaks above the nominal rate.\n"
            "Dotted lines: disk repair. Requests already in flight may continue after destination failure.",
            ha="center",
            fontsize=9,
            color="#52616b",
        )
        for suffix in ("svg", "png"):
            path = output / f"{scenario}.{suffix}"
            fig.savefig(
                path,
                dpi=160,
                bbox_inches="tight",
                metadata={"Date": None} if suffix == "svg" else None,
            )
            if suffix == "svg":
                normalize_svg(path)
        plt.close(fig)


def formatted(stat):
    if not stat["n"]:
        return "unobserved"
    ci = (
        "unavailable"
        if stat["ci95"] is None
        else f"[{stat['ci95'][0]:.3f}, {stat['ci95'][1]:.3f}]"
    )
    sd = "unavailable" if stat["sd"] is None else f"{stat['sd']:.3f}"
    return (
        f"mean {stat['mean']:.3f}; sd {sd}; 95% CI {ci}; "
        f"median {stat['median']:.3f}; p95 {stat['p95']:.3f}; p99 {stat['p99']:.3f}; n={stat['n']}"
    )


def batch_evidence(inputs, explicit, selected, exploratory):
    candidates = set(explicit)
    for root in inputs:
        if root.is_dir():
            for parent in (root, root.parent):
                if (parent / "batch.json").is_file():
                    candidates.add(parent / "batch.json")
    evidence, warnings = [], []
    selected = {path.resolve() for path in selected}
    planned_reports = set()
    for path in sorted(candidates):
        raw = path.read_bytes()
        batch = json.loads(raw)
        require(
            exploratory or not batch["pilot"], f"{path}: pilot requires --exploratory"
        )
        require(
            exploratory or batch["status"] in {"success", "failed"},
            f"{path}: batch is {batch['status']}; wait for completion or use --exploratory",
        )
        identities = [
            (i["scenario"], i["seed"], i["arm"]) for i in batch["invocations"]
        ]
        require(
            len(identities) == len(set(identities)),
            f"{path}: duplicate planned invocation",
        )
        expected = {
            (scenario, seed, arm)
            for scenario in batch["scenarios"]
            for seed in range(batch["first_seed"], batch["first_seed"] + batch["count"])
            for arm in ARMS
        }
        require(
            set(identities) == expected,
            f"{path}: invocation plan differs from declared seeds/scenarios",
        )
        build = None
        if batch.get("build_metadata_sha256"):
            build_path = path.parent / "build.json"
            build_raw = build_path.read_bytes()
            require(
                hashlib.sha256(build_raw).hexdigest() == batch["build_metadata_sha256"],
                f"{path}: build metadata hash mismatch",
            )
            build = json.loads(build_raw)
            for relative, expected_hash in build.get("harness_file_sha256", {}).items():
                source = path.parent / "harness-source" / relative
                require(source.is_file(), f"{path}: frozen source missing: {relative}")
                require(
                    hashlib.sha256(source.read_bytes()).hexdigest() == expected_hash,
                    f"{path}: frozen source hash mismatch: {relative}",
                )
        missing = []
        for invocation in batch["invocations"]:
            # The recorded absolute path is provenance, never an archive fallback.
            report = (
                path.parent
                / "runs"
                / invocation["scenario"]
                / str(invocation["seed"])
                / invocation["arm"]
                / "run.json"
            ).resolve()
            planned_reports.add(report)
            if report not in selected:
                missing.append({**invocation, "resolved_run_json": str(report)})
                continue
            observed = json.loads(report.read_bytes())
            for field in ("scenario", "seed", "arm", "pair_order"):
                require(
                    observed[field] == invocation[field],
                    f"{report}: {field} differs from planned invocation",
                )
            if build is not None:
                binary = build["binaries"][invocation["arm"]]
                require(
                    observed["binary_sha256"] == binary["sha256"],
                    f"{report}: binary differs from build manifest",
                )
                require(
                    observed["build"]["azure_source_sha256"]
                    == binary["azure_source_sha256"],
                    f"{report}: compiled source differs from build manifest",
                )
                require(
                    observed["build"]["revision"] == build["base_revision"],
                    f"{report}: compiled revision differs from build manifest",
                )
        require(
            exploratory or not missing,
            f"{path}: {len(missing)} planned invocations lack supplied reports; use --exploratory to retain partial evidence",
        )
        failed = [i for i in batch["invocations"] if i["status"] != "success"]
        if missing or failed:
            warnings.append(
                f"{path}: {len(failed)} non-success invocations; {len(missing)} unavailable reports; all retained in batch inventory"
            )
        evidence.append(
            {
                "source": str(path.resolve()),
                "source_sha256": hashlib.sha256(raw).hexdigest(),
                "manifest": batch,
                "unavailable_reports": missing,
            }
        )
    unplanned = sorted(str(p) for p in selected - planned_reports) if evidence else []
    require(
        exploratory or not unplanned,
        f"reports outside supplied invocation plan: {unplanned}",
    )
    if unplanned:
        warnings.append(f"Reports outside supplied invocation plan: {unplanned}")
    return evidence, warnings


def text_report(report):
    lines = [
        "PAIRED AZURE FLEET REPORT",
        "",
        report["status"],
        "",
        "Independent unit: seed/run. Differences are fixed minus baseline.",
        "Run-level t intervals; run-level Bernoulli proportions use Wilson intervals.",
        "GET/action fractions are means of per-run rates; correlated requests are not Wilson trials.",
        "P95/p99 are sample quantiles across runs, with limited resolution at n=10.",
        "No requests or actions are pooled as independent statistical replicates.",
        "",
    ]
    for scenario, summary in report["scenarios"].items():
        lines += [scenario, f"Seeds: {summary['paired_seeds']}"]
        for arm in ARMS:
            lines.append(f"  {arm}")
            for key, stat in summary["arms"][arm]["metrics"].items():
                lines.append(f"    {key}: {formatted(stat)}")
            for key, p in summary["arms"][arm]["run_proportions"].items():
                lines.append(
                    f"    {key}: {p['successes']}/{p['n']}; Wilson 95% {p['ci95']}"
                )
        lines.append("  Paired differences")
        for key, stat in summary["paired_difference_fixed_minus_baseline"].items():
            lines.append(f"    {key}: {formatted(stat)}")
        lines.append(
            f"  Fully successful, same-process recovered pairs: {summary['fully_successful_recovered_pairs']}/{summary['n_pairs']}"
        )
        if not summary["all_pairs_have_equal_successful_work_and_recovery"]:
            lines.append(
                "  Do not interpret lower total traffic as an efficiency improvement: some pairs lack acceptance, matching output digests, or uncensored cache reuse."
            )
        lines.append("")
    lines.extend(f"WARNING: {w}" for w in report["warnings"])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "runs",
        type=Path,
        nargs="+",
        help="run.json files or directories recursively containing run.json",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--exploratory",
        action="store_true",
        help="permit fewer than 10 pairs, missing arms, or unbalanced order; label output exploratory",
    )
    parser.add_argument("--no-plots", action="store_true")
    parser.add_argument(
        "--batch-manifest",
        type=Path,
        action="append",
        default=[],
        help="include planned invocations, including runs without run.json (auto-detected at input roots)",
    )
    args = parser.parse_args()
    paths = sorted(
        {
            p.resolve()
            for root in args.runs
            for p in (root.rglob("run.json") if root.is_dir() else [root])
        }
    )
    require(paths, "no run.json inputs")
    batches, batch_warnings = batch_evidence(
        args.runs, args.batch_manifest, paths, args.exploratory
    )
    runs = [checked_run(p, args.exploratory) for p in paths]
    paired, warnings = pair_runs(runs, args.exploratory)
    warnings.extend(batch_warnings)
    warnings.extend(
        f"{r['scenario']}/{r['seed']}/{r['arm']}: scheduler accounting audit failed; retained as an outcome"
        for r in runs
        if not scheduler_audit(r)["passed"]
    )
    summaries = summarize(paired)
    report = {
        "schema_version": 1,
        "analysis_source_sha256": hashlib.sha256(
            Path(__file__).read_bytes()
        ).hexdigest(),
        "status": "EXPLORATORY; no confirmatory batch claim"
        if args.exploratory
        else ">=10 paired seeds per scenario; condition order alternated",
        "statistical_unit": "independent seeded run; pairing by scenario, seed, and manifest hash",
        "difference_direction": "fixed minus baseline",
        "inference_limits": [
            "Intervals describe stochastic variation under this fixture, not calibration or production uncertainty.",
            "Completed-only timings exclude censored outcomes; consult failed/censored counts and recovery proportions.",
            "Per-completed-action payload includes warmup and sentinel overhead and is reported only for fully successful runs.",
            "Repeated ranges include ordinary failed-read retry/recovery; they are not automatically futile or preventable bytes.",
            "Failed client attempts remain counted even when an explicit post-repair resubmission eventually succeeds; these are not scheduler attempt counts.",
            "Same-process sentinel completion bounds recovery time from above; it does not locate the first reusable instant.",
            "Sentinel-window traffic includes any surviving background requests; it is not automatically attributed to the sentinel action.",
            "A token bucket limits admitted bytes over time, not each short-window body-output rate.",
            "Scheduler snapshots sample reservations; they do not rule out unaccounted physical execution or prove process teardown after reporting.",
        ],
        "warnings": warnings,
        "batches": batches,
        "scenarios": summaries,
        "runs": [
            {
                "source": r["_source"],
                "source_sha256": r["_source_sha256"],
                "build": r.get("build"),
                **{
                    k: r[k]
                    for k in (
                        "arm",
                        "scenario",
                        "seed",
                        "pair_order",
                        "binary_sha256",
                        "workload_sha256",
                        "start_unix_s",
                        "duration_s",
                        "load",
                        "nodes",
                    )
                },
                "metrics": metrics(r),
                "scheduler_audit": scheduler_audit(r),
            }
            for r in runs
        ],
    }
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "summary.json").write_text(
        json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    compact = {
        **report,
        "batches": [
            {
                "source": b["source"],
                "source_sha256": b["source_sha256"],
                "status": b["manifest"]["status"],
                "planned_invocations": len(b["manifest"]["invocations"]),
                "non_success_invocations": [
                    i for i in b["manifest"]["invocations"] if i["status"] != "success"
                ],
                "unavailable_reports": b["unavailable_reports"],
            }
            for b in batches
        ],
        "runs": [
            {k: v for k, v in r.items() if k not in {"nodes", "load"}}
            for r in report["runs"]
        ],
    }
    (args.output / "summary.compact.json").write_text(
        json.dumps(compact, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    text = text_report(report)
    (args.output / "summary.txt").write_text(text)
    if not args.no_plots:
        plot(paired, summaries, args.output)
    print(text)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError) as error:
        sys.exit(f"Invalid or incomplete evidence: {error}")
