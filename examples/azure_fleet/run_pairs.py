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

"""Build the guard-only ablation and run a preregistered, sequential paired batch.

Python orchestrates processes and preserves evidence; all workload, fault,
scheduler, store, SDK and HTTP behavior is implemented by the Rust example.
Coordinate exclusive access to the Azure source file during the two builds.
"""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "nativelink-store/src/azure_blob_store.rs"
GUARD = b" || writer.is_pipe_broken()"
ARMS = ("baseline", "fixed")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic_write(path, data, mode=None):
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        if mode is not None:
            temporary.chmod(mode)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def save(path, data):
    atomic_write(path, (json.dumps(data, indent=2, allow_nan=False) + "\n").encode())


def git(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=ROOT)


def load():
    result = {}
    for name in ("loadavg", "pressure/cpu", "pressure/io", "pressure/memory"):
        path = Path("/proc") / name
        result[name] = path.read_text() if path.exists() else None
    return result


def source_fingerprint():
    """Base plus tracked changes and untracked Rust, excluding the one ablation."""
    state = hashlib.sha256()
    state.update(git("rev-parse", "HEAD"))
    state.update(
        git(
            "diff",
            "--binary",
            "HEAD",
            "--",
            ".",
            ":(exclude)nativelink-store/src/azure_blob_store.rs",
        )
    )
    for name in sorted(
        git("ls-files", "--others", "--exclude-standard", "-z").split(b"\0")
    ):
        if name and name.endswith(b".rs"):
            state.update(name)
            state.update((ROOT / os.fsdecode(name)).read_bytes())
    return state.hexdigest()


def harness_hashes():
    paths = [ROOT / "Cargo.lock", ROOT / "Cargo.toml"]
    paths.extend(sorted((ROOT / "examples/azure_fleet").glob("*.rs")))
    paths.append(ROOT / "nativelink-store/tests/fixtures/azure_download_response.json")
    return {str(path.relative_to(ROOT)): file_digest(path) for path in paths}


def stop_group(process):
    """Only terminate the process group created for this invocation."""
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


def invoke(argv, stdout_path, stderr_path, timeout_s, env=None):
    record = {
        "argv": [str(a) for a in argv],
        "started_unix_s": time.time(),
        "load_before": load(),
        "status": "running",
        "exit_code": None,
        "stdout": str(stdout_path),
        "stderr": str(stderr_path),
    }
    began = time.monotonic()
    process = None
    interrupted = False
    try:
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            process = subprocess.Popen(
                argv,
                cwd=ROOT,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
            )
            record["pid"] = process.pid
            try:
                record["exit_code"] = process.wait(timeout=timeout_s)
                record["status"] = "success" if record["exit_code"] == 0 else "failed"
            except subprocess.TimeoutExpired:
                record["status"] = "censored"
                record["reason"] = f"controller deadline {timeout_s}s"
                stop_group(process)
                record["exit_code"] = process.returncode
    except KeyboardInterrupt:
        interrupted = True
        record["status"] = "interrupted"
        if process is not None:
            stop_group(process)
            record["exit_code"] = process.returncode
    except OSError as error:
        record["status"] = "failed"
        record["reason"] = str(error)
    finally:
        record["ended_unix_s"] = time.time()
        record["elapsed_s"] = time.monotonic() - began
        record["load_after"] = load()
    return record, interrupted


def restore_source(original, baseline, mode):
    current = SOURCE.read_bytes()
    if current == original:
        return
    if current == baseline:
        atomic_write(SOURCE, original, mode)
        return
    # Concurrent edits must not be silently overwritten by restoring a snapshot.
    # Reverse only our unique changed line when possible, then fail the batch.
    original_line = next(
        line for line in original.splitlines(keepends=True) if GUARD in line
    )
    baseline_line = original_line.replace(GUARD, b"", 1)
    if current.count(baseline_line) == 1 and original_line not in current:
        atomic_write(SOURCE, current.replace(baseline_line, original_line, 1), mode)
    raise RuntimeError(
        "Azure source changed concurrently; preserved other edits and stopped. Inspect original-source.rs and source-ablation.diff before continuing."
    )


def build(output):
    original = SOURCE.read_bytes()
    require(
        original.count(GUARD) == 1,
        "expected exactly one terminal destination guard in Azure source",
    )
    baseline = original.replace(GUARD, b"", 1)
    mode = SOURCE.stat().st_mode
    revision = git("rev-parse", "HEAD").decode().strip()
    fingerprint = source_fingerprint()
    hashes = harness_hashes()
    (output / "bins").mkdir()
    (output / "original-source.rs").write_bytes(original)
    for name in hashes:
        destination = output / "harness-source" / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, destination)
        require(
            file_digest(destination) == hashes[name],
            f"source changed while snapshotting {name}",
        )
    patch = "".join(
        difflib.unified_diff(
            original.decode().splitlines(keepends=True),
            baseline.decode().splitlines(keepends=True),
            fromfile="fixed/nativelink-store/src/azure_blob_store.rs",
            tofile="baseline/nativelink-store/src/azure_blob_store.rs",
        )
    )
    (output / "source-ablation.diff").write_text(patch)
    (output / "checkout-changes.diff").write_bytes(git("diff", "--binary", "HEAD"))
    metadata = {
        "schema_version": 1,
        "status": "building",
        "base_revision": revision,
        "harness_fingerprint": fingerprint,
        "harness_file_sha256": hashes,
        "guard_removed": GUARD.decode(),
        "source_hashes": {"baseline": digest(baseline), "fixed": digest(original)},
        "binaries": {},
        "builds": [],
        "strip_executable": shutil.which("strip"),
    }
    save(output / "build.json", metadata)
    try:
        for arm in ARMS:
            require(
                SOURCE.read_bytes() in (original, baseline),
                "Azure source changed before build",
            )
            require(
                source_fingerprint() == fingerprint,
                "other source changed between builds",
            )
            require(
                harness_hashes() == hashes,
                "harness or Cargo files changed between builds",
            )
            source = baseline if arm == "baseline" else original
            atomic_write(SOURCE, source, mode)
            env = dict(os.environ)
            env.update(
                FLEET_VARIANT=arm,
                FLEET_BASE_REVISION=revision,
                FLEET_AZURE_SOURCE_SHA256=digest(source),
            )
            print(f"Building {arm}; source={digest(source)}", flush=True)
            record, interrupted = invoke(
                [
                    "cargo",
                    "build",
                    "--offline",
                    "--example",
                    "azure_fleet",
                    "--jobs",
                    "4",
                ],
                output / f"build-{arm}.stdout.log",
                output / f"build-{arm}.stderr.log",
                timeout_s=3600,
                env=env,
            )
            record["arm"] = arm
            metadata["builds"].append(record)
            save(output / "build.json", metadata)
            if interrupted:
                raise KeyboardInterrupt
            require(
                record["status"] == "success", f"{arm} build failed; inspect build logs"
            )
            require(SOURCE.read_bytes() == source, "Azure source changed during build")
            require(
                source_fingerprint() == fingerprint, "other source changed during build"
            )
            require(
                harness_hashes() == hashes,
                "harness or Cargo files changed during build",
            )
            target = Path(env.get("CARGO_TARGET_DIR", "target"))
            if not target.is_absolute():
                target = ROOT / target
            if env.get("CARGO_BUILD_TARGET"):
                target = target / env["CARGO_BUILD_TARGET"]
            executable = target / "debug/examples/azure_fleet"
            destination = output / "bins" / f"azure_fleet-{arm}"
            shutil.copy2(executable, destination)
            unstripped_sha256 = file_digest(destination)
            stripped = {"status": "unavailable"}
            if metadata["strip_executable"]:
                stripped, interrupted = invoke(
                    [metadata["strip_executable"], "--strip-debug", str(destination)],
                    output / f"strip-{arm}.stdout.log",
                    output / f"strip-{arm}.stderr.log",
                    120,
                )
                if interrupted:
                    raise KeyboardInterrupt
                require(
                    stripped["status"] == "success",
                    f"{arm}: stripping debug sections failed",
                )
            metadata["binaries"][arm] = {
                "path": str(destination),
                "sha256": file_digest(destination),
                "unstripped_sha256": unstripped_sha256,
                "strip": stripped,
                "azure_source_sha256": digest(source),
            }
            save(output / "build.json", metadata)
        require(
            metadata["binaries"]["baseline"]["sha256"]
            != metadata["binaries"]["fixed"]["sha256"],
            "ablation produced identical binaries",
        )
        metadata["status"] = "success"
    except BaseException:
        metadata["status"] = "failed"
        raise
    finally:
        try:
            restore_source(original, baseline, mode)
        finally:
            metadata["source_restored_exactly"] = SOURCE.read_bytes() == original
            save(output / "build.json", metadata)
    return metadata


def existing_build(output):
    metadata = json.loads((output / "build.json").read_text())
    require(
        metadata["status"] == "success" and metadata["source_restored_exactly"],
        "build did not complete and restore its source",
    )
    require(
        harness_hashes() == metadata["harness_file_sha256"],
        "harness/Cargo/fixture changed after build; create a new build directory",
    )
    require(
        file_digest(SOURCE) == metadata["source_hashes"]["fixed"],
        "Azure source changed after build; create a new build directory",
    )
    for arm in ARMS:
        binary = metadata["binaries"][arm]
        require(
            file_digest(Path(binary["path"])) == binary["sha256"],
            f"{arm} binary changed",
        )
    return metadata


def run_batch(args, output, metadata):
    require(
        not (output / "batch.json").exists(),
        "batch already exists; never overwrite or selectively rerun it",
    )
    plan = []
    for offset in range(args.count):
        seed = args.first_seed + offset
        for scenario in args.scenarios:
            for order, arm in enumerate(ARMS if offset % 2 == 0 else ARMS[::-1]):
                plan.append(
                    {
                        "seed": seed,
                        "scenario": scenario,
                        "arm": arm,
                        "pair_order": order,
                        "status": "not_started",
                    }
                )
    batch = {
        "schema_version": 1,
        "status": "running",
        "pilot": args.pilot,
        "first_seed": args.first_seed,
        "count": args.count,
        "scenarios": args.scenarios,
        "created_unix_s": time.time(),
        "build_metadata_sha256": file_digest(output / "build.json"),
        "timeout_s": args.timeout_s,
        "invocations": plan,
    }
    save(output / "batch.json", batch)
    try:
        for invocation in plan:
            seed, scenario, arm = (
                invocation[key] for key in ("seed", "scenario", "arm")
            )
            parent = output / "runs" / scenario / str(seed)
            parent.mkdir(parents=True, exist_ok=True)
            run_dir = parent / arm
            require(not run_dir.exists(), f"run output already exists: {run_dir}")
            invocation["status"] = "running"
            invocation["run_json"] = str(run_dir / "run.json")
            save(output / "batch.json", batch)
            binary = metadata["binaries"][arm]
            require(
                file_digest(Path(binary["path"])) == binary["sha256"],
                f"{arm} binary changed",
            )
            print(
                f"seed={seed} scenario={scenario} arm={arm} pair_order={invocation['pair_order']}",
                flush=True,
            )
            record, interrupted = invoke(
                [
                    binary["path"],
                    "--arm",
                    arm,
                    "--scenario",
                    scenario,
                    "--seed",
                    str(seed),
                    "--pair-order",
                    str(invocation["pair_order"]),
                    "--output",
                    str(run_dir),
                ],
                parent / f"{arm}.stdout.log",
                parent / f"{arm}.stderr.log",
                args.timeout_s,
            )
            invocation.update(record)
            if invocation["status"] == "success":
                try:
                    report = json.loads((run_dir / "run.json").read_text())
                    require(
                        report["arm"] == arm
                        and report["seed"] == seed
                        and report["scenario"] == scenario,
                        "run identity differs",
                    )
                    require(
                        report["binary_sha256"] == binary["sha256"],
                        "reported binary hash differs",
                    )
                    require(
                        report["build"]["azure_source_sha256"]
                        == binary["azure_source_sha256"],
                        "compiled source hash differs",
                    )
                    require(
                        report["build"]["arm"] == arm
                        and report["build"]["revision"] == metadata["base_revision"],
                        "compiled build identity differs",
                    )
                except (OSError, ValueError, KeyError) as error:
                    invocation["status"] = "invalid_report"
                    invocation["reason"] = str(error)
            save(output / "batch.json", batch)
            if interrupted:
                raise KeyboardInterrupt
        batch["status"] = (
            "success" if all(i["status"] == "success" for i in plan) else "failed"
        )
    except BaseException:
        batch["status"] = "interrupted_or_failed"
        raise
    finally:
        batch["ended_unix_s"] = time.time()
        save(output / "batch.json", batch)
    return batch["status"] == "success"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--first-seed", type=int, default=2026101800)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument(
        "--scenarios",
        nargs="+",
        choices=["control", "failed-fill"],
        default=["control", "failed-fill"],
    )
    parser.add_argument("--timeout-s", type=float, default=600)
    parser.add_argument(
        "--pilot",
        action="store_true",
        help="allow fewer than ten pairs; permanently mark batch exploratory",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--build-only", action="store_true")
    mode.add_argument("--skip-build", action="store_true")
    args = parser.parse_args()
    require(
        args.count > 0 and (args.count >= 10 or args.pilot),
        "use >=10 seeds or explicitly --pilot",
    )
    require(
        args.first_seed >= 0 and args.first_seed + args.count - 1 <= 2**64 - 1,
        "seeds must fit Rust u64",
    )
    require(len(args.scenarios) == len(set(args.scenarios)), "duplicate scenarios")
    require(args.timeout_s > 0, "positive timeout required")
    output = args.output.resolve()
    require(
        not output.is_relative_to(ROOT),
        "keep build/run artifacts outside the source checkout",
    )
    if args.skip_build:
        metadata = existing_build(output)
    else:
        output.mkdir(parents=True, exist_ok=False)
        metadata = build(output)
    if not args.build_only:
        require(
            run_batch(args, output, metadata),
            "one or more invocations failed/censored; retained in batch.json",
        )
    print(f"Evidence: {output}", flush=True)


def interrupted(_signal, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(
            "Interrupted; invocation evidence retained and source restoration attempted."
        )
    except (ValueError, OSError, RuntimeError, subprocess.CalledProcessError) as error:
        sys.exit(f"Fleet batch failed: {error}")
