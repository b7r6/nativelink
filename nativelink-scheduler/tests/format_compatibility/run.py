#!/usr/bin/env python3
"""Freeze and qualify a synthesized Base executable against upgraded artifacts.

Both builds use immutable git archives. The frozen build removes the Direct
scheduler grammar; selecting a runtime Base profile alone is not the fixture.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import time


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
AUTHORITY = "nativelink-scheduler/src/authority/"


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def git(*arguments):
    return subprocess.check_output(["git", "-C", str(REPO), *arguments])


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def archive(revision, destination):
    with destination.open("wb") as stream:
        subprocess.run(
            ["git", "-C", str(REPO), "archive", "--format=tar", revision],
            stdout=stream,
            check=True,
        )


def relevant_sources(revision):
    paths = git("ls-tree", "-r", "--name-only", revision).decode().splitlines()
    selected = [
        path for path in paths
        if (path.startswith(AUTHORITY) and path.endswith(".rs") and not path.endswith("/tests.rs"))
        or path in {
            "Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
            "nativelink-scheduler/Cargo.toml", "nativelink-scheduler/src/lib.rs",
            "nativelink-util/src/digest_hasher.rs", "nativelink-util/Cargo.toml",
        }
    ]
    return {
        path: hashlib.sha256(git("show", f"{revision}:{path}")).hexdigest()
        for path in selected
    }


def freeze(args):
    destination = args.pin.resolve()
    if destination.exists():
        raise ValueError("refusing to replace an existing frozen pin")
    revision = git("rev-parse", "--verify", f"{args.revision}^{{commit}}").decode().strip()
    with tempfile.TemporaryDirectory(prefix="nativelink-base-pin-") as temporary:
        source = Path(temporary) / "source.tar"
        archive(revision, source)
        pin = {
            "description": "Synthesized Base envelope3/core5 fixture; not a historical released binary",
            "revision": revision,
            "tree": git("rev-parse", f"{revision}^{{tree}}").decode().strip(),
            "source_archive_sha256": sha256(source),
            "format_relevant_sources_sha256": relevant_sources(revision),
            "frozen_fixture_sha256": {
                name: sha256(HERE / name) for name in ["base_reader.rs", "base-only.patch"]
            },
        }
    write_json(destination, pin)
    print(json.dumps({"frozen_pin": str(destination), "revision": revision}))


def run(args):
    pin = json.loads(args.pin.read_text())
    for name, expected in pin["frozen_fixture_sha256"].items():
        if sha256(HERE / name) != expected:
            raise ValueError(f"frozen fixture changed: {name}")
    if relevant_sources(pin["revision"]) != pin["format_relevant_sources_sha256"]:
        raise ValueError("frozen source manifest mismatch")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    commands = []
    report = {
        "status": "running",
        "scope": "Synthesized frozen Base executable, envelope3/core5; no released format2/core4 compatibility",
        "pin": pin,
        "cases": [],
        "commands": commands,
        "fixture_source_sha256": {
            name: sha256(HERE / name)
            for name in ["base_reader.rs", "base-only.patch", "upgraded_writer.rs", "run.py"]
        },
    }
    write_json(output / "report.json", report)
    shutil.copy2(args.pin, output / "frozen-base.json")
    for name in ["base_reader.rs", "base-only.patch", "upgraded_writer.rs", "run.py"]:
        shutil.copy2(HERE / name, output / name)
    target = args.cargo_target_dir.resolve() if args.cargo_target_dir else output / "target"
    environment = os.environ.copy()
    environment["CARGO_TARGET_DIR"] = str(target)

    def command(argv, cwd, name, accepted=(0,), timeout=1200):
        started = time.monotonic()
        log = output / f"{name}.log"
        with log.open("wb") as stream:
            result = subprocess.run(
                [str(value) for value in argv], cwd=cwd, env=environment,
                stdout=stream, stderr=subprocess.STDOUT, timeout=timeout,
            )
        commands.append({
            "argv": [str(value) for value in argv], "cwd": str(cwd),
            "environment": {"CARGO_TARGET_DIR": environment["CARGO_TARGET_DIR"]},
            "returncode": result.returncode, "seconds": time.monotonic() - started,
            "log": log.name,
        })
        write_json(output / "report.json", report)
        if result.returncode not in accepted:
            raise RuntimeError(f"{name} failed with status {result.returncode}; see {log}")
        return result.returncode, log

    try:
        command(["rustc", "-Vv"], REPO, "rustc-version")
        command(["cargo", "-Vv"], REPO, "cargo-version")
        writer_revision = git("rev-parse", "--verify", f"{args.writer_revision or pin['revision']}^{{commit}}").decode().strip()
        report["writer_revision"] = writer_revision
        report["writer_format_relevant_sources_sha256"] = relevant_sources(writer_revision)
        binaries = {}
        for role, revision, fixture in [
            ("base", pin["revision"], "base_reader.rs"),
            ("upgraded", writer_revision, "upgraded_writer.rs"),
        ]:
            source_tar = output / f"{role}-source.tar"
            archive(revision, source_tar)
            if role == "base" and sha256(source_tar) != pin["source_archive_sha256"]:
                raise ValueError("frozen source archive hash mismatch")
            report[f"{role}_source_archive_sha256"] = sha256(source_tar)
            build_identity = hashlib.sha256((
                sha256(source_tar) + sha256(HERE / fixture)
                + (sha256(HERE / "base-only.patch") if role == "base" else "")
            ).encode()).hexdigest()
            role_target = target / f"{role}-{build_identity}"
            environment["CARGO_TARGET_DIR"] = str(role_target)
            report[f"{role}_target_directory"] = str(role_target)
            source = output / f"{role}-source"
            source.mkdir()
            with tarfile.open(source_tar) as contents:
                contents.extractall(source, filter="data")
            if role == "base":
                command(["git", "apply", "--check", HERE / "base-only.patch"], source, "base-overlay-check")
                command(["git", "apply", HERE / "base-only.patch"], source, "base-overlay")
                journal = (source / AUTHORITY / "journal.rs").read_text()
                if "SchedulerSection::Direct" in journal or "Self::Direct" in journal:
                    raise ValueError("frozen source still contains the Direct scheduler decoder")
            binary_name = f"authority-{role}-compatibility-fixture"
            binary_path = Path("src/bin") / f"{binary_name}.rs"
            destination = source / "nativelink-scheduler" / binary_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(HERE / fixture, destination)
            manifest = source / "nativelink-scheduler/Cargo.toml"
            with manifest.open("a") as stream:
                stream.write(f'\n[[bin]]\nname = "{binary_name}"\npath = "{binary_path}"\n')
            report[f"{role}_compiled_sources_sha256"] = {
                path: sha256(source / path)
                for path in pin["format_relevant_sources_sha256"]
                if (source / path).is_file()
            }
            report[f"{role}_compiled_fixture_sha256"] = sha256(destination)
            command([
                "cargo", "build", "--locked", "--profile", "smol",
                "-p", "nativelink-scheduler", "--bin", binary_name,
            ], source, f"{role}-build")
            binary = output / binary_name
            shutil.copy2(role_target / "smol" / binary_name, binary)
            binaries[role] = binary
            report[f"{role}_binary_sha256"] = sha256(binary)

        artifacts = output / "artifacts"
        command([binaries["upgraded"], artifacts], output, "upgraded-produce", timeout=120)
        produced = json.loads((artifacts / "artifacts.json").read_text())
        artifact_hashes = {name: sha256(artifacts / name) for name in produced["artifacts"]}
        report["artifact_sha256"] = artifact_hashes
        report["expected_state_sha256"] = hashlib.sha256(produced["state_json"].encode()).hexdigest()
        report["pending_effects"] = produced["pending_effects"]
        report["charged_reservations"] = produced["charged_reservations"]
        cases = [
            ("preactivation-journal", "open-journal", "preactivation.redb", True, "raft_recovered", None),
            ("preactivation-snapshot", "install-snapshot", "preactivation.snapshot.json", True, "snapshot_recovered", None),
            ("uncommitted-control-decode", "inspect-journal", "uncommitted-activation.redb", True, "artifact_decoded_without_admission", None),
            ("uncommitted-control-admission", "open-journal", "uncommitted-activation.redb", False, "journal_open", "process cannot participate in activated format"),
            ("committed-before-apply", "open-journal", "committed-before-apply.redb", False, "recover_admission", "unsupported or stale committed activation"),
            ("activated-journal", "open-journal", "activated.redb", False, "recover_admission", "data did not match any variant of untagged enum SchedulerSection"),
            ("activated-snapshot", "install-snapshot", "activated.snapshot.json", False, "install_snapshot", "data did not match any variant of untagged enum SchedulerSection"),
        ]
        for name, mode, artifact, accepted, phase, expected_error in cases:
            case_dir = output / name
            case_dir.mkdir()
            source_artifact = artifacts / artifact
            copy = case_dir / artifact
            shutil.copy2(source_artifact, copy)
            request = {
                "mode": mode, "path": str(copy), "epoch": produced["domain"]["epoch"],
                "namespace": produced["domain"]["namespace"], "node": produced["node"],
                "rebuilt_snapshot": str(case_dir / "base-rebuilt.snapshot.json") if name == "preactivation-journal" else None,
            }
            request_path = case_dir / "request.json"
            write_json(request_path, request)
            status, log = command([binaries["base"], request_path], output, name, accepted=(0, 2), timeout=60)
            observed = json.loads(log.read_text().splitlines()[-1])
            if observed["accepted"] != accepted or observed["phase"] != phase or status != (0 if accepted else 2):
                raise AssertionError(f"unexpected frozen reader outcome for {name}: {observed}")
            if not accepted and (observed["vote_reads"] or observed["raft_started"]):
                raise AssertionError(f"unsupported artifact reached voting/startup in {name}")
            if expected_error is not None and expected_error not in (observed["error"] or ""):
                raise AssertionError(f"unexpected rejection cause in {name}: {observed['error']}")
            if name in {"preactivation-journal", "preactivation-snapshot"} and observed["state_json"] is None:
                raise AssertionError(f"compatible frozen recovery did not return complete state in {name}")
            if observed["state_json"] is not None and observed["state_json"] != produced["state_json"]:
                raise AssertionError(f"authority state or pending effects changed in {name}")
            if name == "preactivation-journal":
                if not observed["quorum_read_completed"] or observed["vote_reads"] != 1:
                    raise AssertionError("frozen executable did not perform actual vote read and Raft quorum read")
                rebuilt = case_dir / "base-rebuilt.snapshot.json"
                envelope = json.loads(rebuilt.read_text())
                payload = json.loads(envelope["payload"])
                if not isinstance(payload["applied"]["scheduler"], str):
                    raise AssertionError("frozen Base snapshot writer emitted an upgraded section")
                observed["rebuilt_snapshot_sha256"] = sha256(rebuilt)
            observed["input_sha256"] = sha256(source_artifact)
            observed["working_artifact_sha256"] = sha256(copy)
            observed["name"] = name
            observed["exit_status"] = status
            if observed["state_json"] is not None:
                observed["state_sha256"] = hashlib.sha256(observed.pop("state_json").encode()).hexdigest()
            report["cases"].append(observed)
            write_json(output / "report.json", report)
        if artifact_hashes != {name: sha256(artifacts / name) for name in produced["artifacts"]}:
            raise AssertionError("captured source artifacts changed during qualification")
        report["status"] = "passed"
        report["limitations"] = [
            "Synthesized frozen Base fixture, not a historical released NativeLink executable",
            "No compatibility claim for public experimental format2/core4",
            "Uncommitted-control decoding does not authorize Base voting while admission remains unresolved",
            "Real singleton startup and storage trait calls; no deployed peer authentication or admission qualification",
        ]
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = str(error)
        raise
    finally:
        write_json(output / "report.json", report)
    print(json.dumps({"status": report["status"], "cases": len(report["cases"]), "report": str(output / "report.json")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze_parser = commands.add_parser("freeze", help="create a new reviewed immutable Base pin")
    freeze_parser.add_argument("--revision", required=True)
    freeze_parser.add_argument("--pin", type=Path, required=True)
    run_parser = commands.add_parser("run", help="compile frozen Base and upgraded writer independently")
    run_parser.add_argument("--pin", type=Path, default=HERE / "frozen-base.json")
    run_parser.add_argument("--writer-revision")
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--cargo-target-dir", type=Path)
    args = parser.parse_args()
    if args.command == "freeze":
        freeze(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
