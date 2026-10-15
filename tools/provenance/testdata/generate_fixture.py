#!/usr/bin/env python3
"""Writes the synthetic provenance logs the analyzer tests read.

Usage: generate_fixture.py [OUTPUT_DIR]   (default: this directory)

The scenario is four Bazel invocations, ten minutes apart, against one worker
pool and one server. Each producer below exists to exercise one class the
analyzer reports; test_analyze_provenance.py asserts the exact numbers, and
its docstring walks through where each one comes from.

  //app:stamped    action and input root change every run, output changes
                   (stamping): declared_change_output_change
  //lib:codegen    action and command change every run, output identical:
                   declared_change_output_same
  //tools:archive  same action every run, different output each time:
                   same_action_output_change, and one action digest with
                   four manifests
  //lib:core       runs 1 and 3 only, identical: stable; run 3's has() says
                   absent although run 1 uploaded it
  //app:flip       A, B, A, B: novelty stops after run 2
  //pkg:dup        one digest at two paths in one attempt: counted once
  //docs:site      an output directory and a symlink
  //app:slow       run 2, upload fails: outputs_complete=false
  //app:hung       run 4, attempt_started with no attempt_finished
  //lib:shared     uploaded by Bazel itself (server events only) in runs 1-3:
                   rewrite amplification

worker-events.txt is NativeLink `NL_LOG=json` output, deliberately out of order in
places, with one non-JSON line, one non-provenance line, one event with an
unknown schema and one with an unknown kind. server-events.txt is flat JSON, the
shape a flattened OTLP log export takes, with some numbers as strings.
"""

from __future__ import annotations

import hashlib
import json
import sys
from calendar import timegm
from datetime import datetime, timezone
from pathlib import Path

BASE = timegm((2026, 10, 15, 10, 0, 0, 0, 0, 0))
OUT = "bazel-out/k8-opt/bin/"
CONFIG = "cfg-k8-opt"


def digest(label: str, size: int) -> str:
    return f"{hashlib.sha256(label.encode()).hexdigest()}-{size}"


def iso(seconds: float) -> str:
    whole = int(seconds)
    micros = int(round((seconds - whole) * 1_000_000))
    moment = datetime.fromtimestamp(whole, tz=timezone.utc)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{micros:06d}Z"


def fingerprint(manifest: list[tuple[str, str, str, bool]]) -> str:
    lines = sorted(manifest, key=lambda entry: (entry[1], entry[0]))
    text = "".join(
        f"{kind}\t{path}\t{value}\t{'x' if executable else '-'}\n"
        for kind, path, value, executable in lines
    )
    return hashlib.sha256(text.encode()).hexdigest()


def tracing_line(seconds: float, fields: dict, span: dict | None = None) -> str:
    record = {
        "timestamp": iso(seconds),
        "level": "DEBUG",
        "fields": fields,
        "target": "nativelink_provenance",
    }
    if span is not None:
        record["span"] = span
        record["spans"] = [span]
    return json.dumps(record, separators=(",", ":"))


def metadata(invocation: str, target: str, mnemonic: str, action_id: str) -> dict:
    return {
        "tool_name": "bazel",
        "tool_version": "8.4.2",
        "tool_invocation_id": invocation,
        "correlated_invocations_id": "build-" + invocation,
        "action_id": action_id,
        "action_mnemonic": mnemonic,
        "target_id": target,
        "configuration_id": CONFIG,
    }


# Output: (kind, path, content label, size, first_in_attempt, symlink target)
# Upload: (content label, size, has_result, disposition, has_error, error_code)
# Finish: outcome, or None for an attempt that never finishes.
ABSENT = ("absent", "uploaded", "", "")
PRESENT = ("present", "skipped_present", "", "")
SLOW_PROBE = ("error", "uploaded", "DeadlineExceeded: slow tier probe exceeded 3s", "")
RESET = ("error", "uploaded", "Unavailable: connection reset", "")
FAILED = ("absent", "upload_failed", "", "Unavailable")

HAS_ELAPSED_MS = {"absent": 12, "present": 8, "error": 3000}


def attempts() -> list[dict]:
    rows = []

    def add(
        run,
        slot,
        name,
        target,
        mnemonic,
        action,
        command,
        input_root,
        outputs,
        uploads,
        outcome="completed",
    ):
        rows.append(
            {
                "run": run,
                "slot": slot,
                "name": name,
                "target": target,
                "mnemonic": mnemonic,
                "action": digest("action:" + action, 140),
                "command": digest("command:" + command, 220),
                "input_root": digest("input:" + input_root, 90),
                "outputs": outputs,
                "uploads": uploads,
                "outcome": outcome,
            }
        )

    def file(path, label, size, first=True):
        return ("file", OUT + path, label, size, first, "")

    stamped = "app/version.txt"
    for run in range(1, 5):
        outputs = [file(stamped, f"stamp-{run}", 1000)]
        uploads = [(f"stamp-{run}", 1000, *ABSENT)]
        if run == 1:
            outputs.append(("stderr", "", "stderr-1", 100, True, ""))
            uploads.append(("stderr-1", 100, *ABSENT))
        add(
            run,
            1,
            f"a{run}-stamp",
            "//app:stamped",
            "Genrule",
            f"stamp-{run}",
            "stamp",
            f"stamp-{run}",
            outputs,
            uploads,
        )

    codegen_has = {1: ABSENT, 2: PRESENT, 3: RESET, 4: PRESENT}
    for run in range(1, 5):
        add(
            run,
            2,
            f"a{run}-codegen",
            "//lib:codegen",
            "CodeGen",
            f"gen-{run}",
            f"gen-{run}",
            "gen",
            [file("lib/gen.h", "gen-h", 2000)],
            [("gen-h", 2000, *codegen_has[run])],
        )

    archive_has = {1: ABSENT, 2: SLOW_PROBE, 3: ABSENT, 4: ABSENT}
    for run in range(1, 5):
        add(
            run,
            3,
            f"a{run}-archive",
            "//tools:archive",
            "SdkArchive",
            "archive",
            "archive",
            "archive",
            [file("tools/sdk.tar", f"archive-{run}", 10000)],
            [(f"archive-{run}", 10000, *archive_has[run])],
        )

    for run in (1, 3):
        add(
            run,
            4,
            f"a{run}-core",
            "//lib:core",
            "CppCompile",
            "core",
            "core",
            "core",
            [file("lib/core.o", "core-o", 5000)],
            [("core-o", 5000, *ABSENT)],
        )

    for run in range(1, 5):
        side = "A" if run % 2 else "B"
        add(
            run,
            5 if run in (1, 3) else 4,
            f"a{run}-flip",
            "//app:flip",
            "Genrule",
            f"flip-{side}",
            f"flip-{side}",
            f"flip-{side}",
            [file("app/flip.bin", f"flip-{side}", 3000)],
            [(f"flip-{side}", 3000, *(ABSENT if run <= 2 else PRESENT))],
        )

    add(
        1,
        6,
        "a1-dup",
        "//pkg:dup",
        "Copy",
        "dup",
        "dup",
        "dup",
        [file("pkg/a.dat", "dup", 7000, True), file("pkg/b.dat", "dup", 7000, False)],
        [("dup", 7000, *ABSENT)],
    )

    add(
        1,
        7,
        "a1-site",
        "//docs:site",
        "Zip",
        "site",
        "site",
        "site",
        [
            ("directory_file", OUT + "docs/html/index.html", "site-index", 800, True, ""),
            ("directory_file", OUT + "docs/html/style.css", "site-css", 200, True, ""),
            ("directory_index", OUT + "docs/html", "site-tree", 150, True, ""),
            ("symlink", OUT + "docs/latest", "", 0, True, "html"),
        ],
        [("site-index", 800, *ABSENT), ("site-css", 200, *ABSENT), ("site-tree", 150, *ABSENT)],
    )

    add(
        2,
        5,
        "a2-slow",
        "//app:slow",
        "Javac",
        "slow",
        "slow",
        "slow",
        [file("app/slow.jar", "slow-jar", 4000)],
        [("slow-jar", 4000, *FAILED)],
        outcome="upload_failed",
    )

    add(
        4,
        5,
        "a4-hung",
        "//app:hung",
        "CppCompile",
        "hung",
        "hung",
        "hung",
        [file("app/hung.o", "hung-o", 600)],
        [],
        outcome=None,
    )
    return rows


def attempt_lines(index: int, row: dict) -> list[str]:
    start = BASE + (row["run"] - 1) * 600 + row["slot"]
    attempt_id = f"00000000-0000-4000-8000-{index:012d}"
    operation_id = "op-" + row["name"]
    invocation = f"inv-{row['run']}"
    meta = metadata(invocation, row["target"], row["mnemonic"], "bazel-action-" + row["name"])
    span = {"name": "execute", "operation_id": operation_id}
    lines = [
        tracing_line(
            start,
            {
                "event": "attempt_started",
                "schema": 1,
                **meta,
                "attempt_id": attempt_id,
                "operation_id": operation_id,
                "worker_id": "worker-0",
                "instance_name": "main",
                "digest_function": "sha256",
                "action_digest": row["action"],
                "command_digest": row["command"],
                "input_root_digest": row["input_root"],
            },
            span,
        )
    ]
    manifest = []
    for offset, (kind, path, label, size, first, link) in enumerate(row["outputs"]):
        output_digest = digest(label, size) if label else ""
        if kind in ("file", "directory_file", "symlink"):
            manifest.append((kind, path, link if kind == "symlink" else output_digest, False))
        lines.append(
            tracing_line(
                start + 1 + offset / 100,
                {
                    "event": "output_observed",
                    "schema": 1,
                    "attempt_id": attempt_id,
                    "operation_id": operation_id,
                    "action_digest": row["action"],
                    "kind": kind,
                    "path": path,
                    "digest": output_digest,
                    "size_bytes": size,
                    "executable": False,
                    "symlink_target": link,
                    "first_in_attempt": first,
                },
                span,
            )
        )
    for offset, (label, size, has_result, disposition, has_error, error_code) in enumerate(
        row["uploads"]
    ):
        lines.append(
            tracing_line(
                start + 2 + offset / 100,
                {
                    "event": "output_upload",
                    "schema": 1,
                    "attempt_id": attempt_id,
                    "operation_id": operation_id,
                    "digest": digest(label, size),
                    "size_bytes": size,
                    "has_result": has_result,
                    "has_error": has_error,
                    "disposition": disposition,
                    "error_code": error_code,
                    "error": "connection refused" if error_code else "",
                    "has_elapsed_ms": HAS_ELAPSED_MS[has_result],
                    "upload_elapsed_ms": 40 if disposition == "uploaded" else 0,
                },
                span,
            )
        )
    if row["outcome"] is None:
        return lines
    complete = row["outcome"] == "completed"
    entries = [o for o in row["outputs"] if o[0] in ("file", "directory_file", "symlink")]
    unique = {}
    for _, _, label, size, _, _ in entries:
        if label:
            unique[label] = size
    lines.append(
        tracing_line(
            start + 3,
            {
                "event": "attempt_finished",
                "schema": 1,
                "attempt_id": attempt_id,
                "operation_id": operation_id,
                "action_digest": row["action"],
                "exit_code": 0,
                "outcome": row["outcome"],
                "error_code": "" if complete else "Unavailable",
                "error": "" if complete else "upload failed",
                "outputs_complete": complete,
                "output_count": len(entries),
                "output_bytes": sum(o[3] for o in entries),
                "unique_output_bytes": sum(unique.values()),
                "manifest_fingerprint": fingerprint(manifest) if complete else "",
                "stdout_digest": "",
                "stderr_digest": digest("stderr-1", 100) if row["name"] == "a1-stamp" else "",
            },
            span,
        )
    )
    return lines


def worker_log() -> list[str]:
    blocks = {
        row["name"]: attempt_lines(index, row) for index, row in enumerate(attempts(), start=1)
    }
    order = [row["name"] for row in attempts()]
    order.sort(key=lambda name: (int(name[1]), name))

    lines = ["Starting NativeLink worker (this line is not JSON)"]
    lines.append(
        json.dumps(
            {
                "timestamp": iso(BASE),
                "level": "INFO",
                "fields": {"message": "Worker registered with scheduler"},
                "target": "nativelink_worker::local_worker",
            },
            separators=(",", ":"),
        )
    )
    late = []
    for name in order:
        block = blocks[name]
        if name == "a3-core":
            # attempt_finished before attempt_started, outputs before both.
            block = list(reversed(block))
        if name == "a2-flip":
            late.extend(block)
            continue
        lines.extend(block)
    lines.append(
        tracing_line(
            BASE + 3600,
            {"event": "output_observed", "schema": 2, "attempt_id": "future", "size_bytes": 999999},
        )
    )
    lines.append(tracing_line(BASE + 900, {"event": "future_event", "schema": 1}))
    # Run 2's flip attempt arrives last, after runs 3 and 4.
    lines.extend(late)
    return lines


def flat(seconds: float, fields: dict, *, nanos: bool = False, target: bool = True) -> str:
    record: dict = {}
    if nanos:
        record["timeUnixNano"] = str(int(round(seconds * 1_000_000_000)))
    else:
        record["timestamp"] = iso(seconds)
    if target:
        record["target"] = "nativelink_provenance"
    record.update(fields)
    return json.dumps(record, separators=(",", ":"))


def server_log() -> list[str]:
    # A worker's own uploads carry no RequestMetadata.
    empty = {key: "" for key in metadata("", "", "", "")}
    shared = digest("shared-lib", 50000)
    stamp = digest("stamp-1", 1000)
    gen = digest("gen-h", 2000)
    slow = digest("slow-jar", 4000)
    site_result = digest("action-result:site", 300)

    def cas(seconds, source, value, size, meta, result="ok", error_code="", **kw):
        return flat(
            seconds,
            {
                "event": "cas_write",
                "schema": 1,
                **meta,
                "source": source,
                "instance_name": "main",
                "digest_function": "sha256",
                "digest": value,
                "size_bytes": size,
                "result": result,
                "error_code": error_code,
                "error": "backend unavailable" if error_code else "",
                "elapsed_ms": 30,
                "peer": "10.0.0.7:51234",
            },
            **kw,
        )

    def backend(seconds, key, size, meta, result="ok", error_code="", **kw):
        return flat(
            seconds,
            {
                "event": "backend_write",
                "schema": 1,
                **meta,
                "store": "azure_blob",
                "key": key,
                "size_bytes": size,
                "mode": "single",
                "block_count": 0,
                "result": result,
                "error_code": error_code,
                "error": "backend unavailable" if error_code else "",
                "elapsed_ms": 25,
            },
            **kw,
        )

    def bazel(run):
        return metadata(f"inv-{run}", "//lib:shared", "CppLink", f"bazel-action-shared-{run}")

    lines = [
        flat(
            BASE + 0.5,
            {
                "event": "find_missing",
                "schema": 1,
                **bazel(1),
                "instance_name": "main",
                "digest_function": "sha256",
                "requested_count": 3,
                "requested_bytes": 61000,
                "missing_count": 1,
                "missing_bytes": 50000,
                "result": "ok",
                "error_code": "",
                "error": "",
                "elapsed_ms": 4,
            },
        ),
        cas(BASE + 30, "batch_update", stamp, 1000, empty),
        cas(BASE + 40, "bytestream", gen, 2000, empty),
        backend(BASE + 41, "cas/" + gen, 2000, empty),
        cas(BASE + 60, "bytestream", shared, 50000, bazel(1)),
        backend(BASE + 61, "cas/" + shared, 50000, bazel(1)),
        backend(BASE + 300, "ac/" + site_result, 300, empty, nanos=True, target=False),
        json.dumps(
            {
                "timestamp": iso(BASE + 301),
                "level": "INFO",
                "fields": {"message": "ByteStream write committed"},
                "target": "nativelink_service::bytestream_server",
            },
            separators=(",", ":"),
        ),
        flat(
            BASE + 600.5,
            {
                "event": "find_missing",
                "schema": "1",
                **bazel(2),
                "instance_name": "main",
                "digest_function": "sha256",
                "requested_count": "2",
                "requested_bytes": "3000",
                "missing_count": "0",
                "missing_bytes": "0",
                "result": "ok",
                "error_code": "",
                "error": "",
                "elapsed_ms": "3",
            },
            target=False,
        ),
        cas(BASE + 660, "bytestream", shared, "50000", bazel(2), nanos=True),
        backend(BASE + 661, "cas/" + shared, 50000, bazel(2)),
        cas(BASE + 690, "bytestream", slow, 4000, empty, result="error", error_code="Unavailable"),
        backend(BASE + 691, "cas/" + slow, 4000, empty, result="error", error_code="Unavailable"),
        cas(BASE + 1240, "bytestream", gen, 2000, empty),
        backend(BASE + 1241, "cas/" + gen, 2000, empty),
        cas(BASE + 1260, "bytestream", shared, 50000, bazel(3), target=False),
        backend(BASE + 1261, "cas/" + shared, 50000, bazel(3)),
        backend(BASE + 1500, "ac/" + site_result, 300, empty),
    ]
    return lines


def seed_lines() -> list[str]:
    return [
        "# Blob inventory taken before the window.",
        "",
        "cas/" + digest("core-o", 5000) + ",5000,2026-10-14T23:59:00Z",
        digest("shared-lib", 50000).rsplit("-", 1)[0],
        digest("never-seen-in-logs", 123),
        "not-a-digest",
    ]


def main() -> int:
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
    for name, lines in (
        ("worker-events.txt", worker_log()),
        ("server-events.txt", server_log()),
        ("seed-digests.txt", seed_lines()),
    ):
        (out_dir / name).write_text("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
