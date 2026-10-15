"""Tests for analyze_provenance.py against a synthetic event log.

Run with either of:

  python3 -m unittest discover tools/provenance
  python3 -m pytest tools/provenance

The fixture under testdata/ is written by testdata/generate_fixture.py, whose
docstring lists the scenario. The numbers asserted here, and where they come
from (sizes in bytes):

  Outputs per run, digest-deduplicated:
    run 1: stamp-1 1000, stderr-1 100, gen-h 2000, archive-1 10000,
           core-o 5000, flip-A 3000, dup 7000 (at two paths), site-index 800,
           site-css 200, site-tree 150                      = 29250
    run 2: stamp-2 1000, gen-h, archive-2 10000, flip-B 3000,
           slow-jar 4000                     run 20000, new = 18000
    run 3: stamp-3 1000, gen-h, archive-3 10000, core-o, flip-A
                                             run 21000, new = 11000
    run 4: stamp-4 1000, gen-h, archive-4 10000, flip-B, hung-o 600
                                             run 16600, new = 11600
  First observed in the window: 29250 + 18000 + 11000 + 11600 = 69850 in 19
  digests. Seeding core-o removes 5000 of it from run 1.

  Server writes (result=ok), in time order:
    cas_write: stamp-1 batch 1000; gen-h 2000; shared 50000 (inv-1);
               shared (inv-2, rewrite); gen-h (rewrite); shared (inv-3,
               rewrite); plus slow-jar 4000 failed.
    backend_write: the same CAS keys under `cas/`, and one AC key under
               `ac/` written twice (300 each).
"""

from __future__ import annotations

import gzip
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

HERE = Path(__file__).resolve().parent
TESTDATA = HERE / "testdata"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(TESTDATA))

import generate_fixture as fixture  # noqa: E402

import analyze_provenance as ap  # noqa: E402

WORKER = str(TESTDATA / "worker-events.txt")
SERVER = str(TESTDATA / "server-events.txt")
SEED = str(TESTDATA / "seed-digests.txt")
OUT = "bazel-out/k8-opt/bin/"

DC_OC = ap.DECLARED_CHANGE_OUTPUT_CHANGE
DC_OS = ap.DECLARED_CHANGE_OUTPUT_SAME
SA_OC = ap.SAME_ACTION_OUTPUT_CHANGE
STABLE = ap.STABLE


def run_analysis(seed: bool = False) -> dict:
    hashes = ap.parse_seed_file(SEED)[0] if seed else set()
    with redirect_stderr(io.StringIO()):
        return ap.analyze([WORKER, SERVER], hashes, top=0)


def producer(report: dict, target: str, path: str) -> dict:
    matches = [
        row
        for row in report["producers"]["rows"]
        if row["target_id"] == target and row["path"] == path
    ]
    assert len(matches) == 1, (target, path, matches)
    return matches[0]


def churn_row(report: dict, target: str, path: str) -> dict:
    matches = [
        row for row in report["churn"]["rows"] if row["target_id"] == target and row["path"] == path
    ]
    assert len(matches) == 1, (target, path, matches)
    return matches[0]


def tracing(ts: str, **fields) -> str:
    return json.dumps(
        {
            "timestamp": ts,
            "level": "DEBUG",
            "fields": {"schema": 1, **fields},
            "target": "nativelink_provenance",
        }
    )


class FixtureInSyncTest(unittest.TestCase):
    def test_committed_fixture_matches_generator(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            fixture_dir = Path(tmp)
            with redirect_stdout(io.StringIO()):
                old_argv = sys.argv
                try:
                    sys.argv = ["generate_fixture.py", str(fixture_dir)]
                    fixture.main()
                finally:
                    sys.argv = old_argv
            for name in ("worker-events.txt", "server-events.txt", "seed-digests.txt"):
                self.assertEqual(
                    (fixture_dir / name).read_text(),
                    (TESTDATA / name).read_text(),
                    f"testdata/{name} is stale; run testdata/generate_fixture.py",
                )


class FixtureReportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = run_analysis()
        cls.seeded = run_analysis(seed=True)

    def test_inputs_and_window(self) -> None:
        inputs = self.report["inputs"]
        self.assertEqual(inputs["lines"], 116)
        self.assertEqual(inputs["provenance_events"], 111)
        self.assertEqual(inputs["non_json_lines"], 1)
        self.assertEqual(inputs["non_provenance_lines"], 2)
        self.assertEqual(inputs["unknown_schema_events"], 1)
        self.assertEqual(inputs["unknown_event_kinds"], 1)
        self.assertEqual(
            inputs["events_by_kind"],
            {
                "attempt_started": 22,
                "output_observed": 27,
                "output_upload": 24,
                "attempt_finished": 21,
                "cas_write": 7,
                "find_missing": 2,
                "backend_write": 8,
            },
        )
        window = self.report["window"]
        # The schema-2 event at 11:00 is skipped, so it does not stretch the window.
        self.assertEqual(window["first"], "2026-10-15T10:00:00.500Z")
        self.assertEqual(window["last"], "2026-10-15T10:30:07.000Z")
        self.assertEqual(window["duration_seconds"], 1806.5)

    def test_producer_totals(self) -> None:
        producers = self.report["producers"]
        self.assertEqual(producers["producer_count"], 14)
        self.assertEqual(producers["first_observed_bytes"], 69850)
        self.assertEqual(producers["first_observed_digests"], 19)
        self.assertEqual(producers["observed_bytes"], 93850)
        self.assertEqual(producers["observations"], 27)
        self.assertEqual(producers["uploads_without_output_observed"], 0)
        first_three = [(r["target_id"], r["path"]) for r in producers["rows"][:3]]
        self.assertEqual(
            first_three,
            [
                ("//tools:archive", OUT + "tools/sdk.tar"),
                ("//pkg:dup", OUT + "pkg/a.dat"),
                ("//app:flip", OUT + "app/flip.bin"),
            ],
        )

    def test_stamping_producer(self) -> None:
        row = producer(self.report, "//app:stamped", OUT + "app/version.txt")
        self.assertEqual(row["attempts"], 4)
        self.assertEqual(row["distinct_action_digests"], 4)
        self.assertEqual(row["distinct_input_root_digests"], 4)
        self.assertEqual(row["distinct_command_digests"], 1)
        self.assertEqual(row["distinct_output_digests"], 4)
        self.assertEqual(row["first_observed_bytes"], 4000)
        self.assertEqual(row["observed_bytes"], 4000)
        self.assertEqual(row["dispositions"], {"uploaded": 4})

    def test_stdout_and_stderr_get_a_pseudo_path(self) -> None:
        row = producer(self.report, "//app:stamped", "<stderr>")
        self.assertEqual(row["kinds"], ["stderr"])
        self.assertEqual(row["first_observed_bytes"], 100)

    def test_identical_output_counts_once(self) -> None:
        row = producer(self.report, "//lib:codegen", OUT + "lib/gen.h")
        self.assertEqual(row["distinct_action_digests"], 4)
        self.assertEqual(row["distinct_command_digests"], 4)
        self.assertEqual(row["distinct_input_root_digests"], 1)
        self.assertEqual(row["distinct_output_digests"], 1)
        self.assertEqual(row["first_observed_bytes"], 2000)
        self.assertEqual(row["observed_bytes"], 8000)
        self.assertEqual(row["dispositions"], {"skipped_present": 2, "uploaded": 2})

    def test_duplicate_content_at_two_paths_counts_once(self) -> None:
        first = producer(self.report, "//pkg:dup", OUT + "pkg/a.dat")
        second = producer(self.report, "//pkg:dup", OUT + "pkg/b.dat")
        self.assertEqual(first["first_observed_bytes"], 7000)
        self.assertEqual(second["first_observed_bytes"], 0)
        self.assertEqual(second["observed_bytes"], 7000)
        # One upload decision per digest per attempt, credited to the path the
        # worker deduplicated it under.
        self.assertEqual(first["dispositions"], {"uploaded": 1})
        self.assertEqual(second["dispositions"], {})

    def test_failed_and_unfinished_attempts_still_count_as_observed(self) -> None:
        slow = producer(self.report, "//app:slow", OUT + "app/slow.jar")
        self.assertEqual(slow["first_observed_bytes"], 4000)
        self.assertEqual(slow["dispositions"], {"upload_failed": 1})
        hung = producer(self.report, "//app:hung", OUT + "app/hung.o")
        self.assertEqual(hung["first_observed_bytes"], 600)
        self.assertEqual(hung["dispositions"], {})

    def test_directory_outputs_and_symlinks(self) -> None:
        index = producer(self.report, "//docs:site", OUT + "docs/html")
        self.assertEqual(index["kinds"], ["directory_index"])
        self.assertEqual(index["first_observed_bytes"], 150)
        link = producer(self.report, "//docs:site", OUT + "docs/latest")
        self.assertEqual(link["kinds"], ["symlink"])
        self.assertEqual(link["distinct_output_digests"], 0)
        self.assertEqual(link["observed_bytes"], 0)

    def test_exact_identity(self) -> None:
        exact = self.report["exact_identity"]
        self.assertEqual(exact["actions_checked"], 14)
        self.assertEqual(exact["actions_with_multiple_manifests"], 1)
        (row,) = exact["rows"]
        self.assertEqual(row["target_id"], "//tools:archive")
        self.assertEqual(row["action_digest"], fixture.digest("action:archive", 140))
        self.assertEqual(row["complete_attempts"], 4)
        self.assertEqual(row["distinct_fingerprints"], 4)
        self.assertEqual(row["invocations"], 4)
        self.assertEqual(row["differing_paths"], [OUT + "tools/sdk.tar"])
        self.assertEqual(row["varying_bytes"], 40000)

    def test_churn_summary(self) -> None:
        churn = self.report["churn"]
        self.assertEqual(churn["producers_compared"], 5)
        self.assertEqual(
            churn["summary"],
            {
                DC_OC: {"pairs": 6, "bytes": 12000, "producers": 2},
                DC_OS: {"pairs": 3, "bytes": 6000, "producers": 1},
                SA_OC: {"pairs": 3, "bytes": 30000, "producers": 1},
                STABLE: {"pairs": 1, "bytes": 5000, "producers": 1},
            },
        )
        self.assertEqual(
            churn["declared_change_components"],
            {"command": 3, "command+input_root": 3, "input_root": 3},
        )
        # Only producers with a non-stable pair are listed.
        self.assertEqual(
            [(r["target_id"], r["churn_bytes"]) for r in churn["rows"]],
            [
                ("//tools:archive", 30000),
                ("//app:flip", 9000),
                ("//lib:codegen", 6000),
                ("//app:stamped", 3000),
            ],
        )

    def test_churn_per_class(self) -> None:
        stamped = churn_row(self.report, "//app:stamped", OUT + "app/version.txt")
        self.assertEqual(stamped["pairs"], {DC_OC: 3, DC_OS: 0, SA_OC: 0, STABLE: 0})
        self.assertEqual(stamped["configuration_id"], fixture.CONFIG)
        self.assertEqual(stamped["invocations"], 4)
        codegen = churn_row(self.report, "//lib:codegen", OUT + "lib/gen.h")
        self.assertEqual(codegen["pairs"], {DC_OC: 0, DC_OS: 3, SA_OC: 0, STABLE: 0})
        archive = churn_row(self.report, "//tools:archive", OUT + "tools/sdk.tar")
        self.assertEqual(archive["pairs"], {DC_OC: 0, DC_OS: 0, SA_OC: 3, STABLE: 0})
        flip = churn_row(self.report, "//app:flip", OUT + "app/flip.bin")
        self.assertEqual(flip["pairs"], {DC_OC: 3, DC_OS: 0, SA_OC: 0, STABLE: 0})
        self.assertEqual(flip["complete_attempts"], 4)

    def test_novelty(self) -> None:
        runs = self.report["novelty"]["runs"]
        self.assertEqual(
            [
                (
                    r["invocation"],
                    r["attempts"],
                    r["distinct_digests"],
                    r["run_bytes"],
                    r["new_digests"],
                    r["new_bytes"],
                    r["cumulative_new_bytes"],
                )
                for r in runs
            ],
            [
                ("inv-1", 7, 10, 29250, 10, 29250, 29250),
                ("inv-2", 5, 5, 20000, 4, 18000, 47250),
                ("inv-3", 5, 5, 21000, 2, 11000, 58250),
                ("inv-4", 5, 5, 16600, 3, 11600, 69850),
            ],
        )
        self.assertEqual(runs[0]["first_event"], "2026-10-15T10:00:01.000Z")
        # Run 2's flip attempt is the last block in worker-events.txt; order comes
        # from timestamps, not from the file.
        self.assertEqual(runs[1]["first_event"], "2026-10-15T10:10:01.000Z")
        self.assertEqual(self.report["novelty"]["total_new_bytes"], 69850)

    def test_novelty_with_seed(self) -> None:
        novelty = self.seeded["novelty"]
        self.assertEqual(novelty["seeded_digests"], 3)
        self.assertEqual(
            [
                (r["new_digests"], r["new_bytes"], r["cumulative_new_bytes"])
                for r in novelty["runs"]
            ],
            [(9, 24250, 24250), (4, 18000, 42250), (2, 11000, 53250), (3, 11600, 64850)],
        )
        self.assertEqual(self.seeded["producers"]["first_observed_bytes"], 64850)
        core = producer(self.seeded, "//lib:core", OUT + "lib/core.o")
        self.assertEqual(core["first_observed_bytes"], 0)

    def test_cas_write_rewrites(self) -> None:
        cas = self.report["rewrites"]["cas_write"]
        self.assertEqual(
            cas["totals"],
            {
                "writes": 6,
                "bytes": 155000,
                "first_writes": 3,
                "first_write_bytes": 53000,
                "rewrites": 3,
                "rewrite_bytes": 102000,
                "prewindow_rewrites": 0,
                "prewindow_rewrite_bytes": 0,
                "errors": 1,
                "error_bytes": 4000,
            },
        )
        self.assertEqual(cas["distinct_written"], 3)
        by_source = {row["source"]: row for row in cas["by_group"]}
        self.assertEqual(by_source["bytestream"]["bytes"], 154000)
        self.assertEqual(by_source["bytestream"]["first_write_bytes"], 52000)
        self.assertEqual(by_source["bytestream"]["rewrite_bytes"], 102000)
        self.assertEqual(by_source["bytestream"]["error_codes"], {"Unavailable": 1})
        self.assertEqual(by_source["batch_update"]["first_write_bytes"], 1000)
        self.assertEqual(by_source["batch_update"]["rewrite_bytes"], 0)
        top = cas["top_rewritten"]
        self.assertEqual(
            [(r["digest"], r["writes"], r["rewrite_bytes"], r["invocations"]) for r in top],
            [
                (fixture.digest("shared-lib", 50000), 3, 100000, 3),
                (fixture.digest("gen-h", 2000), 2, 2000, 0),
            ],
        )

    def test_backend_write_rewrites(self) -> None:
        backend = self.report["rewrites"]["backend_write"]
        self.assertEqual(backend["totals"]["bytes"], 154600)
        self.assertEqual(backend["totals"]["first_write_bytes"], 52300)
        self.assertEqual(backend["totals"]["rewrite_bytes"], 102300)
        self.assertEqual(backend["totals"]["errors"], 1)
        groups = {(row["store"], row["prefix"]): row for row in backend["by_group"]}
        self.assertEqual(sorted(groups), [("azure_blob", "ac/"), ("azure_blob", "cas/")])
        self.assertEqual(groups[("azure_blob", "cas/")]["rewrite_bytes"], 102000)
        self.assertEqual(groups[("azure_blob", "ac/")]["first_write_bytes"], 300)
        self.assertEqual(groups[("azure_blob", "ac/")]["rewrite_bytes"], 300)
        top = backend["top_rewritten"]
        self.assertEqual(top[0]["key"], "cas/" + fixture.digest("shared-lib", 50000))
        self.assertEqual(top[0]["rewrite_bytes"], 100000)
        self.assertEqual(top[0]["invocations"], 3)
        self.assertEqual(len(top), 3)

    def test_rewrites_with_seed(self) -> None:
        cas = self.seeded["rewrites"]["cas_write"]["totals"]
        self.assertEqual(cas["first_write_bytes"], 3000)
        self.assertEqual(cas["rewrite_bytes"], 102000)
        self.assertEqual(cas["prewindow_rewrite_bytes"], 50000)
        backend = self.seeded["rewrites"]["backend_write"]["totals"]
        self.assertEqual(backend["first_write_bytes"], 2300)
        self.assertEqual(backend["prewindow_rewrite_bytes"], 50000)

    def test_has_health(self) -> None:
        health = self.report["has_health"]
        self.assertEqual(health["upload_events"], 24)
        self.assertEqual(health["uploaded_bytes"], 72250)
        rows = {row["has_result"]: row for row in health["by_has_result"]}
        self.assertEqual(sorted(rows), ["absent", "error", "present"])
        self.assertEqual(
            (
                rows["absent"]["events"],
                rows["absent"]["bytes"],
                rows["absent"]["uploaded"],
                rows["absent"]["uploaded_bytes"],
            ),
            (18, 64250, 17, 60250),
        )
        self.assertEqual(rows["absent"]["dispositions"], {"upload_failed": 1, "uploaded": 17})
        self.assertEqual(
            (rows["error"]["events"], rows["error"]["bytes"], rows["error"]["uploaded_bytes"]),
            (2, 12000, 12000),
        )
        self.assertEqual(rows["present"]["dispositions"], {"skipped_present": 4})
        self.assertEqual(rows["present"]["uploaded_bytes"], 0)
        self.assertEqual(rows["error"]["has_elapsed_ms_p95"], 3000)
        # core-o said absent in run 3 after run 1 uploaded it; gen-h was
        # re-uploaded after a has() error although it was already stored.
        self.assertEqual(
            health["uploads_of_already_stored_digests"],
            {"absent": {"uploads": 1, "bytes": 5000}, "error": {"uploads": 1, "bytes": 2000}},
        )
        self.assertEqual(
            [row["has_error"] for row in health["top_has_errors"]],
            ["DeadlineExceeded: slow tier probe exceeded 3s", "Unavailable: connection reset"],
        )
        self.assertEqual(
            health["find_missing"],
            {
                "requests": 2,
                "errors": 0,
                "requested_count": 5,
                "requested_bytes": 64000,
                "missing_count": 1,
                "missing_bytes": 50000,
            },
        )

    def test_coverage(self) -> None:
        coverage = self.report["coverage"]
        self.assertEqual(coverage["attempts_seen"], 22)
        self.assertEqual(coverage["attempts_started"], 22)
        self.assertEqual(coverage["attempts_finished"], 21)
        self.assertEqual(coverage["started_without_finished"], 1)
        (example,) = coverage["started_without_finished_examples"]
        self.assertEqual(example["target_id"], "//app:hung")
        self.assertEqual(example["started"], "2026-10-15T10:30:05.000Z")
        # a3-core's attempt_finished precedes its attempt_started in the file.
        self.assertEqual(coverage["finished_without_started"], 0)
        self.assertEqual(coverage["outputs_incomplete"], 1)
        self.assertEqual(coverage["outputs_incomplete_by_outcome"], {"upload_failed": 1})
        self.assertEqual(coverage["manifest_count_mismatches"], 0)
        self.assertEqual(coverage["outputs_without_attempt_started"], 0)
        self.assertEqual(coverage["outputs_without_metadata"], 0)
        self.assertEqual(coverage["unknown_schema"], {"2": 1})
        self.assertEqual(coverage["unknown_events"], {"future_event": 1})
        self.assertEqual(coverage["events_without_timestamp"], 0)

    def test_top_limits_ranked_tables_only(self) -> None:
        with redirect_stderr(io.StringIO()):
            report = ap.analyze([WORKER, SERVER], set(), top=2)
        self.assertEqual(len(report["producers"]["rows"]), 2)
        self.assertEqual(report["producers"]["producer_count"], 14)
        self.assertEqual(len(report["churn"]["rows"]), 2)
        self.assertEqual(len(report["novelty"]["runs"]), 4)


class CommandLineTest(unittest.TestCase):
    script = str(HERE / "analyze_provenance.py")

    def run_cli(self, *args: str, stdin: bytes | None = None) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, self.script, *args],
            input=stdin,
            capture_output=True,
            check=True,
        )

    def test_gzip_file_and_stdin_match_plain_files(self) -> None:
        plain = json.loads(self.run_cli("--json", "--top", "0", WORKER, SERVER).stdout)
        with tempfile.TemporaryDirectory() as tmp:
            compressed = Path(tmp) / "server-events.txt.gz"
            with open(SERVER, "rb") as source, gzip.open(compressed, "wb") as target:
                shutil.copyfileobj(source, target)
            worker_bytes = Path(WORKER).read_bytes()
            mixed = json.loads(
                self.run_cli(
                    "--json", "--top", "0", "-", str(compressed), stdin=worker_bytes
                ).stdout
            )
        plain["inputs"].pop("files")
        mixed["inputs"].pop("files")
        self.assertEqual(plain, mixed)

    def test_gzip_on_stdin(self) -> None:
        result = self.run_cli("--json", stdin=gzip.compress(Path(WORKER).read_bytes()))
        report = json.loads(result.stdout)
        self.assertEqual(report["inputs"]["events_by_kind"]["attempt_started"], 22)

    def test_warnings_go_to_stderr(self) -> None:
        result = self.run_cli("--seed-digests", SEED, WORKER)
        stderr = result.stderr.decode()
        self.assertIn("skipped 1 provenance event(s) with schema 2", stderr)
        self.assertIn("1 line(s)", stderr)
        empty = self.run_cli(stdin=b"not json\n")
        self.assertIn("no provenance events found", empty.stderr.decode())

    def test_text_report(self) -> None:
        text = self.run_cli("--seed-digests", SEED, WORKER, SERVER).stdout.decode()
        for heading in (
            "Window: 2026-10-15T10:00:00.500Z .. 2026-10-15T10:30:07.000Z (30m 06s)",
            "1. Producers by first-observed output bytes (14 of 14 shown)",
            "2. Exact identity",
            "3. Logical producer churn",
            "4. Novelty per invocation, in order of first event, seeded with 3 digests",
            "5. Rewrite amplification",
            "6. has() health",
            "7. Coverage",
        ):
            self.assertIn(heading, text)
        self.assertIn("39.1 KiB", text)
        self.assertIn("e2ee005cedae..-50000", text)
        wide = self.run_cli("--wide", WORKER, SERVER).stdout.decode()
        self.assertIn(fixture.digest("shared-lib", 50000), wide)


class LineShapeTest(unittest.TestCase):
    def test_tracing_json(self) -> None:
        line = tracing("2026-10-15T10:00:00Z", event="find_missing")
        status, record, raw_ts = ap.classify_line(line)
        self.assertEqual(status, "provenance")
        self.assertEqual(record["event"], "find_missing")
        self.assertEqual(raw_ts, "2026-10-15T10:00:00Z")

    def test_other_targets_are_not_provenance(self) -> None:
        line = json.dumps({"target": "nativelink_worker", "fields": {"event": "x", "schema": 1}})
        self.assertEqual(ap.classify_line(line)[0], "non_provenance")
        self.assertEqual(ap.classify_line("[1, 2]")[0], "non_provenance")
        self.assertEqual(ap.classify_line(json.dumps({"event": "cas_write"}))[0], "non_provenance")

    def test_prefixed_and_broken_lines(self) -> None:
        prefixed = "2026-10-15T10:00:00.1Z stdout F " + tracing(
            "2026-10-15T10:00:00Z", event="cas_write"
        )
        self.assertEqual(ap.classify_line(prefixed)[0], "provenance")
        self.assertEqual(ap.classify_line("worker-1  | {not json")[0], "non_json")
        self.assertEqual(ap.classify_line("plain text")[0], "non_json")
        self.assertEqual(ap.classify_line("   ")[0], "blank")

    def test_otlp_attributes(self) -> None:
        line = json.dumps(
            {
                "timeUnixNano": "1792058400000000000",
                "attributes": {"event": "find_missing", "schema": 1},
            }
        )
        status, record, raw_ts = ap.classify_line(line)
        self.assertEqual(status, "provenance")
        self.assertEqual(record["event"], "find_missing")
        self.assertEqual(ap.parse_timestamp(raw_ts), 1792058400 * 10**9)


class DefensiveTest(unittest.TestCase):
    def collect(self, lines: list[str]) -> dict:
        collector = ap.Collector()
        for line in lines:
            collector.feed_line(line)
        return ap.Analysis(collector, set(), 0).report()

    def test_events_with_missing_fields(self) -> None:
        report = self.collect(
            [
                tracing("2026-10-15T10:00:00Z", event="output_observed"),
                tracing("2026-10-15T10:00:01Z", event="output_upload"),
                tracing("2026-10-15T10:00:02Z", event="attempt_finished"),
                tracing("not a time", event="cas_write"),
                tracing("2026-10-15T10:00:03Z", event="backend_write", key="no-digest-here"),
                tracing("2026-10-15T10:00:04Z", event="find_missing"),
                json.dumps({"target": "nativelink_provenance", "fields": {"event": "cas_write"}}),
            ]
        )
        self.assertEqual(report["inputs"]["provenance_events"], 6)
        self.assertEqual(report["coverage"]["unknown_schema"], {"missing": 1})
        self.assertEqual(report["coverage"]["events_without_timestamp"], 1)
        self.assertEqual(report["coverage"]["finished_without_started"], 1)
        self.assertEqual(report["coverage"]["outputs_without_attempt_started"], 1)
        self.assertEqual(report["rewrites"]["cas_write"]["totals"]["errors"], 1)
        self.assertEqual(report["rewrites"]["backend_write"]["by_group"][0]["prefix"], "")
        ap.render_text(report, 0, False)

    def test_oscillation_stops_adding_novelty_after_two_runs(self) -> None:
        lines = []
        for run, side in enumerate("ABAB", start=1):
            attempt = f"attempt-{run}"
            ts = f"2026-10-15T1{run}:00:00Z"
            lines.append(
                tracing(
                    ts,
                    event="attempt_started",
                    attempt_id=attempt,
                    action_digest=f"{'a' * 64}-{run % 2}",
                    tool_invocation_id=f"run-{run}",
                    target_id="//app:flip",
                )
            )
            lines.append(
                tracing(
                    ts,
                    event="output_observed",
                    attempt_id=attempt,
                    kind="file",
                    path="flip.bin",
                    digest=f"{side.lower() * 64}-3000",
                    size_bytes=3000,
                    first_in_attempt=True,
                )
            )
            lines.append(
                tracing(
                    ts,
                    event="attempt_finished",
                    attempt_id=attempt,
                    outputs_complete=True,
                    manifest_fingerprint=side,
                )
            )
        report = self.collect(lines)
        self.assertEqual(
            [run["new_bytes"] for run in report["novelty"]["runs"]], [3000, 3000, 0, 0]
        )
        self.assertEqual(report["churn"]["summary"][DC_OC]["pairs"], 3)

    def test_unfinished_attempt_is_not_compared(self) -> None:
        lines = [
            tracing(
                "2026-10-15T10:00:00Z",
                event="attempt_started",
                attempt_id="x",
                action_digest=f"{'b' * 64}-1",
            ),
            tracing(
                "2026-10-15T10:00:01Z",
                event="output_observed",
                attempt_id="x",
                kind="file",
                path="p",
                digest=f"{'c' * 64}-5",
                size_bytes=5,
            ),
        ]
        report = self.collect(lines)
        self.assertEqual(report["churn"]["producers_compared"], 0)
        self.assertEqual(report["exact_identity"]["actions_checked"], 0)
        self.assertEqual(report["coverage"]["started_without_finished"], 1)
        self.assertEqual(report["producers"]["first_observed_bytes"], 5)


class HelperTest(unittest.TestCase):
    def test_parse_timestamp(self) -> None:
        base = 1792058400 * 10**9  # 2026-10-15T10:00:00Z
        self.assertEqual(ap.parse_timestamp("2026-10-15T10:00:00Z"), base)
        self.assertEqual(ap.parse_timestamp("2026-10-15T10:00:00.123456789Z"), base + 123456789)
        self.assertEqual(ap.parse_timestamp("2026-10-15T12:00:00.5+02:00"), base + 500_000_000)
        self.assertEqual(ap.parse_timestamp("2026-10-15 10:00:00"), base)
        self.assertEqual(ap.parse_timestamp(1792058400), base)
        self.assertEqual(ap.parse_timestamp(1792058400000), base)
        self.assertEqual(ap.parse_timestamp("1792058400000000"), base)
        self.assertEqual(ap.parse_timestamp(str(base)), base)
        self.assertEqual(ap.parse_timestamp(1792058400.25), base + 250_000_000)
        self.assertIsNone(ap.parse_timestamp("yesterday"))
        self.assertIsNone(ap.parse_timestamp(None))
        self.assertIsNone(ap.parse_timestamp(True))

    def test_format(self) -> None:
        self.assertEqual(
            ap.format_timestamp(1792058400 * 10**9 + 7_000_000), "2026-10-15T10:00:00.007Z"
        )
        self.assertEqual(ap.human_bytes(1023), "1023 B")
        self.assertEqual(ap.human_bytes(1536), "1.5 KiB")
        self.assertEqual(ap.human_bytes(15 * 1024**4), "15.0 TiB")
        self.assertEqual(ap.format_duration(1806.5), "30m 06s")
        self.assertEqual(ap.format_duration(90061), "1d 01h 01m")

    def test_percentile(self) -> None:
        values = list(range(1, 21))
        self.assertEqual(ap.percentile(values, 50), 10)
        self.assertEqual(ap.percentile(values, 95), 19)
        self.assertEqual(ap.percentile(values, 100), 20)
        self.assertEqual(ap.percentile([], 95), 0)
        self.assertEqual(ap.percentile([7], 50), 7)

    def test_digests_and_keys(self) -> None:
        self.assertEqual(ap.digest_hash("ABC123-42"), "abc123")
        self.assertEqual(ap.digest_hash("abc123"), "abc123")
        self.assertEqual(ap.digest_hash(""), "")
        digest = "f" * 64
        self.assertEqual(ap.split_backend_key(f"prod/cas/{digest}-10"), ("prod/cas/", digest))
        self.assertEqual(ap.split_backend_key("no-digest"), ("", ""))

    def test_field_coercion(self) -> None:
        self.assertEqual(ap.as_int("12"), 12)
        self.assertEqual(ap.as_int("12.0"), 12)
        self.assertEqual(ap.as_int(None), 0)
        self.assertEqual(ap.as_int("x"), 0)
        self.assertTrue(ap.as_bool("true"))
        self.assertFalse(ap.as_bool("false"))
        self.assertEqual(ap.as_schema("1"), 1)
        self.assertIsNone(ap.as_schema("one"))

    def test_seed_file(self) -> None:
        hashes, bad = ap.parse_seed_file(SEED)
        self.assertEqual(bad, 1)
        self.assertEqual(
            hashes,
            {
                ap.digest_hash(fixture.digest("core-o", 5000)),
                ap.digest_hash(fixture.digest("shared-lib", 50000)),
                ap.digest_hash(fixture.digest("never-seen-in-logs", 123)),
            },
        )


if __name__ == "__main__":
    unittest.main()
