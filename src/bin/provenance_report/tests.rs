// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests against a synthetic event log in `testdata/`.
//!
//! The scenario is four Bazel invocations, ten minutes apart, against one
//! worker pool and one server. Each producer exists to exercise one class:
//!
//! - `//app:stamped`: action and input root change every run, and so does the
//!   output (stamping): `declared_change_output_change`.
//! - `//lib:codegen`: action and command change every run, output identical:
//!   `declared_change_output_same`.
//! - `//tools:archive`: same action every run, different output each time:
//!   `same_action_output_change`, and one action digest with four manifests.
//! - `//lib:core`: runs 1 and 3 only, identical (`stable`); run 3's `has()`
//!   says absent although run 1 uploaded it.
//! - `//app:flip`: A, B, A, B, so novelty stops after run 2.
//! - `//pkg:dup`: one digest at two paths in one attempt, counted once.
//! - `//docs:site`: an output directory and a symlink.
//! - `//app:slow`: run 2, upload fails, `outputs_complete=false`.
//! - `//app:hung`: run 4, `attempt_started` with no `attempt_finished`.
//! - `//lib:shared`: uploaded by Bazel itself (server events only) in runs 1
//!   to 3, so no worker saw it: client uploads by target, and rewrite
//!   amplification.
//!
//! `worker-events.txt` is `NL_LOG=json` output, deliberately out of order in
//! places (`a3-core`'s `attempt_finished` comes first, and run 2's flip
//! attempt comes last), with one non-JSON line, one non-provenance line, one
//! event with an unknown schema and one with an unknown kind.
//! `server-events.txt` is flat JSON, the shape a flattened OTLP log export
//! takes, with some numbers as strings. Most backend writes carry a
//! `version_id`; the run-2 write of `//lib:shared` has an empty one, and the
//! two AC writes have none, as in logs from before the field existed.
//!
//! Where the numbers come from (sizes in bytes):
//!
//! ```text
//! Outputs per run, digest-deduplicated:
//!   run 1: stamp-1 1000, stderr-1 100, gen-h 2000, archive-1 10000,
//!          core-o 5000, flip-A 3000, dup 7000 (at two paths), site-index 800,
//!          site-css 200, site-tree 150                      = 29250
//!   run 2: stamp-2 1000, gen-h, archive-2 10000, flip-B 3000,
//!          slow-jar 4000                     run 20000, new = 18000
//!   run 3: stamp-3 1000, gen-h, archive-3 10000, core-o, flip-A
//!                                            run 21000, new = 11000
//!   run 4: stamp-4 1000, gen-h, archive-4 10000, flip-B, hung-o 600
//!                                            run 16600, new = 11600
//! First observed in the window: 69850 in 19 digests. Seeding core-o removes
//! 5000 of it from run 1.
//!
//! Server writes (result=ok), in time order:
//!   cas_write: stamp-1 batch 1000; gen-h 2000; shared 50000 (inv-1);
//!              shared (inv-2, rewrite); gen-h (rewrite); shared (inv-3,
//!              rewrite); plus slow-jar 4000 failed.
//!   backend_write: the same CAS keys under `cas/`, and one AC key under `ac/`
//!              written twice (300 each). Every successful `cas/` write but
//!              shared's run-2 one returns a version ID, so the rewrites
//!              retained by versioning are gen-h 2000 + shared (inv-3) 50000.
//! Client uploads by target: //lib:shared's three writes carry Bazel's
//! metadata and no worker ran it; stamp-1 and gen-h (twice) carry none.
//! ```

use std::collections::{BTreeSet, HashMap};
use std::io::Write as _;
use std::sync::LazyLock;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use crate::analysis::{Analysis, Report, percentile};
use crate::collect::Collector;
use crate::parse::{
    Line, as_bool, as_int, as_schema, classify_line, digest_hash, format_duration,
    format_timestamp, human_bytes, parse_seed, parse_timestamp, split_backend_key,
};
use crate::render::render_text;
use crate::{Args, run};

const WORKER: &str = include_str!("testdata/worker-events.txt");
const SERVER: &str = include_str!("testdata/server-events.txt");
const SEED: &str = include_str!("testdata/seed-digests.txt");
const OUT: &str = "bazel-out/k8-opt/bin/";
const CONFIG: &str = "cfg-k8-opt";
const DC_OC: &str = "declared_change_output_change";
const DC_OS: &str = "declared_change_output_same";
const SA_OC: &str = "same_action_output_change";
const STABLE: &str = "stable";
const BASE_NS: i64 = 1_792_058_400 * 1_000_000_000; // 2026-10-15T10:00:00Z

/// The fixture's digest of a content label: `sha256(label)-size`.
fn digest(label: &str, size: u64) -> String {
    format!("{}-{size}", hex::encode(Sha256::digest(label.as_bytes())))
}

fn seed() -> BTreeSet<String> {
    parse_seed(&mut SEED.as_bytes()).unwrap().0
}

fn analyze(sources: &[(&str, &str)], seed: &BTreeSet<String>, top: i64) -> Report {
    let mut collector = Collector::new();
    for (name, text) in sources {
        collector.read(name, text.as_bytes()).unwrap();
    }
    Analysis::new(&collector, seed, top).report()
}

fn fixture(seed: &BTreeSet<String>, top: i64) -> Report {
    analyze(
        &[("worker-events.txt", WORKER), ("server-events.txt", SERVER)],
        seed,
        top,
    )
}

static REPORT: LazyLock<Value> =
    LazyLock::new(|| serde_json::to_value(fixture(&BTreeSet::new(), 0)).unwrap());
static SEEDED: LazyLock<Value> =
    LazyLock::new(|| serde_json::to_value(fixture(&seed(), 0)).unwrap());

fn find<'a>(rows: &'a Value, fields: &[(&str, &str)]) -> &'a Value {
    let matches: Vec<&Value> = rows
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| fields.iter().all(|(key, want)| row[*key] == *want))
        .collect();
    assert_eq!(matches.len(), 1, "{fields:?}");
    matches[0]
}

fn producer<'a>(report: &'a Value, target: &str, path: &str) -> &'a Value {
    find(
        &report["producers"]["rows"],
        &[("target_id", target), ("path", path)],
    )
}

fn churn_row<'a>(report: &'a Value, target: &str, path: &str) -> &'a Value {
    find(
        &report["churn"]["rows"],
        &[("target_id", target), ("path", path)],
    )
}

/// One `NL_LOG=json` provenance line with schema 1 and these fields.
fn tracing(ts: &str, fields: &Value) -> String {
    let mut all = json!({"schema": 1});
    all.as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    json!({
        "timestamp": ts,
        "level": "DEBUG",
        "fields": all,
        "target": "nativelink_provenance",
    })
    .to_string()
}

fn collect(lines: &[String]) -> (Report, Value) {
    let mut collector = Collector::new();
    for line in lines {
        collector.feed_line(line);
    }
    let report = Analysis::new(&collector, &BTreeSet::new(), 0).report();
    let value = serde_json::to_value(&report).unwrap();
    (report, value)
}

fn args(argv: &[&str]) -> Args {
    use clap::Parser as _;
    Args::try_parse_from(core::iter::once("provenance_report").chain(argv.iter().copied())).unwrap()
}

/// Runs the tool; returns (exit code, stdout, stderr).
fn run_cli(argv: &[&str], stdin: &[u8]) -> (u8, String, String) {
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(&args(argv), &mut &stdin[..], &mut out, &mut err);
    (
        code,
        String::from_utf8(out).unwrap(),
        String::from_utf8(err).unwrap(),
    )
}

/// A directory under the system temp directory, removed on drop.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("provenance_report_{name}_{}", std::process::id()));
        drop(std::fs::remove_dir_all(&path));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn file(&self, name: &str, contents: &[u8]) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_string()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.0));
    }
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

// --------------------------------------------------------------------------
// The fixture itself.

/// Every completed attempt's `manifest_fingerprint` is the SHA-256 of the
/// canonical manifest built from its `output_observed` events, as the event
/// reference defines it. This keeps the hand-maintained fixture honest.
#[test]
fn fixture_fingerprints_match_their_manifests() {
    let mut entries: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    let mut fingerprints: Vec<(String, String)> = Vec::new();
    for line in WORKER.lines() {
        let Some(brace) = line.find('{') else {
            continue;
        };
        let event: Value = serde_json::from_str(&line[brace..]).unwrap();
        let fields = &event["fields"];
        if event["target"] != "nativelink_provenance" || fields["schema"] != 1 {
            continue;
        }
        let attempt = fields["attempt_id"].as_str().unwrap_or("").to_string();
        match fields["event"].as_str() {
            Some("output_observed") => {
                let kind = fields["kind"].as_str().unwrap();
                if !["file", "directory_file", "symlink"].contains(&kind) {
                    continue;
                }
                let value = if kind == "symlink" {
                    fields["symlink_target"].as_str().unwrap()
                } else {
                    fields["digest"].as_str().unwrap()
                };
                let executable = if fields["executable"] == true {
                    "x"
                } else {
                    "-"
                };
                entries.entry(attempt).or_default().push((
                    fields["path"].as_str().unwrap().to_string(),
                    kind.to_string(),
                    format!("{value}\t{executable}"),
                ));
            }
            Some("attempt_finished") if fields["outcome"] == "completed" => {
                fingerprints.push((
                    attempt,
                    fields["manifest_fingerprint"].as_str().unwrap().to_string(),
                ));
            }
            _ => {}
        }
    }
    assert_eq!(fingerprints.len(), 20);
    for (attempt, fingerprint) in fingerprints {
        let mut manifest = entries.remove(&attempt).unwrap_or_default();
        manifest.sort();
        let text = manifest
            .iter()
            .map(|(path, kind, rest)| format!("{kind}\t{path}\t{rest}\n"))
            .collect::<Vec<_>>()
            .concat();
        assert_eq!(
            hex::encode(Sha256::digest(text.as_bytes())),
            fingerprint,
            "{attempt}"
        );
    }
}

// --------------------------------------------------------------------------
// Reports on the fixture.

#[test]
fn inputs_and_window() {
    let inputs = &REPORT["inputs"];
    assert_eq!(inputs["lines"], 116);
    assert_eq!(inputs["provenance_events"], 111);
    assert_eq!(inputs["non_json_lines"], 1);
    assert_eq!(inputs["non_provenance_lines"], 2);
    assert_eq!(inputs["unknown_schema_events"], 1);
    assert_eq!(inputs["unknown_event_kinds"], 1);
    assert_eq!(
        inputs["events_by_kind"],
        json!({
            "attempt_started": 22,
            "output_observed": 27,
            "output_upload": 24,
            "attempt_finished": 21,
            "cas_write": 7,
            "find_missing": 2,
            "backend_write": 8,
        })
    );
    let window = &REPORT["window"];
    // The schema-2 event at 11:00 is skipped, so it does not stretch the window.
    assert_eq!(window["first"], "2026-10-15T10:00:00.500Z");
    assert_eq!(window["last"], "2026-10-15T10:30:07.000Z");
    assert_eq!(window["duration_seconds"], 1806.5);
}

#[test]
fn producer_totals() {
    let producers = &REPORT["producers"];
    assert_eq!(producers["producer_count"], 14);
    assert_eq!(producers["first_observed_bytes"], 69850);
    assert_eq!(producers["first_observed_digests"], 19);
    assert_eq!(producers["observed_bytes"], 93850);
    assert_eq!(producers["observations"], 27);
    assert_eq!(producers["uploads_without_output_observed"], 0);
    let first_three: Vec<(&Value, &Value)> = producers["rows"].as_array().unwrap()[..3]
        .iter()
        .map(|row| (&row["target_id"], &row["path"]))
        .collect();
    assert_eq!(
        first_three,
        [
            (
                &json!("//tools:archive"),
                &json!(format!("{OUT}tools/sdk.tar"))
            ),
            (&json!("//pkg:dup"), &json!(format!("{OUT}pkg/a.dat"))),
            (&json!("//app:flip"), &json!(format!("{OUT}app/flip.bin"))),
        ]
    );
}

#[test]
fn stamping_producer() {
    let row = producer(&REPORT, "//app:stamped", &format!("{OUT}app/version.txt"));
    assert_eq!(row["attempts"], 4);
    assert_eq!(row["distinct_action_digests"], 4);
    assert_eq!(row["distinct_input_root_digests"], 4);
    assert_eq!(row["distinct_command_digests"], 1);
    assert_eq!(row["distinct_output_digests"], 4);
    assert_eq!(row["first_observed_bytes"], 4000);
    assert_eq!(row["observed_bytes"], 4000);
    assert_eq!(row["dispositions"], json!({"uploaded": 4}));
}

#[test]
fn stdout_and_stderr_get_a_pseudo_path() {
    let row = producer(&REPORT, "//app:stamped", "<stderr>");
    assert_eq!(row["kinds"], json!(["stderr"]));
    assert_eq!(row["first_observed_bytes"], 100);
}

#[test]
fn identical_output_counts_once() {
    let row = producer(&REPORT, "//lib:codegen", &format!("{OUT}lib/gen.h"));
    assert_eq!(row["distinct_action_digests"], 4);
    assert_eq!(row["distinct_command_digests"], 4);
    assert_eq!(row["distinct_input_root_digests"], 1);
    assert_eq!(row["distinct_output_digests"], 1);
    assert_eq!(row["first_observed_bytes"], 2000);
    assert_eq!(row["observed_bytes"], 8000);
    assert_eq!(
        row["dispositions"],
        json!({"skipped_present": 2, "uploaded": 2})
    );
}

#[test]
fn duplicate_content_at_two_paths_counts_once() {
    let first = producer(&REPORT, "//pkg:dup", &format!("{OUT}pkg/a.dat"));
    let second = producer(&REPORT, "//pkg:dup", &format!("{OUT}pkg/b.dat"));
    assert_eq!(first["first_observed_bytes"], 7000);
    assert_eq!(second["first_observed_bytes"], 0);
    assert_eq!(second["observed_bytes"], 7000);
    // One upload decision per digest per attempt, credited to the path the
    // worker deduplicated it under.
    assert_eq!(first["dispositions"], json!({"uploaded": 1}));
    assert_eq!(second["dispositions"], json!({}));
}

#[test]
fn failed_and_unfinished_attempts_still_count_as_observed() {
    let slow = producer(&REPORT, "//app:slow", &format!("{OUT}app/slow.jar"));
    assert_eq!(slow["first_observed_bytes"], 4000);
    assert_eq!(slow["dispositions"], json!({"upload_failed": 1}));
    let hung = producer(&REPORT, "//app:hung", &format!("{OUT}app/hung.o"));
    assert_eq!(hung["first_observed_bytes"], 600);
    assert_eq!(hung["dispositions"], json!({}));
}

#[test]
fn directory_outputs_and_symlinks() {
    let index = producer(&REPORT, "//docs:site", &format!("{OUT}docs/html"));
    assert_eq!(index["kinds"], json!(["directory_index"]));
    assert_eq!(index["first_observed_bytes"], 150);
    let link = producer(&REPORT, "//docs:site", &format!("{OUT}docs/latest"));
    assert_eq!(link["kinds"], json!(["symlink"]));
    assert_eq!(link["distinct_output_digests"], 0);
    assert_eq!(link["observed_bytes"], 0);
}

#[test]
fn exact_identity() {
    let exact = &REPORT["exact_identity"];
    assert_eq!(exact["actions_checked"], 14);
    assert_eq!(exact["actions_with_multiple_manifests"], 1);
    let rows = exact["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["target_id"], "//tools:archive");
    assert_eq!(row["action_digest"], digest("action:archive", 140));
    assert_eq!(row["complete_attempts"], 4);
    assert_eq!(row["distinct_fingerprints"], 4);
    assert_eq!(row["invocations"], 4);
    assert_eq!(
        row["differing_paths"],
        json!([format!("{OUT}tools/sdk.tar")])
    );
    assert_eq!(row["varying_bytes"], 40000);
}

#[test]
fn churn_summary() {
    let churn = &REPORT["churn"];
    assert_eq!(churn["producers_compared"], 5);
    assert_eq!(
        churn["summary"],
        json!({
            DC_OC: {"pairs": 6, "bytes": 12000, "producers": 2},
            DC_OS: {"pairs": 3, "bytes": 6000, "producers": 1},
            SA_OC: {"pairs": 3, "bytes": 30000, "producers": 1},
            STABLE: {"pairs": 1, "bytes": 5000, "producers": 1},
        })
    );
    assert_eq!(
        churn["declared_change_components"],
        json!({"command": 3, "command+input_root": 3, "input_root": 3})
    );
    // Only producers with a non-stable pair are listed.
    let rows: Vec<(&Value, &Value)> = churn["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| (&row["target_id"], &row["churn_bytes"]))
        .collect();
    assert_eq!(
        rows,
        [
            (&json!("//tools:archive"), &json!(30000)),
            (&json!("//app:flip"), &json!(9000)),
            (&json!("//lib:codegen"), &json!(6000)),
            (&json!("//app:stamped"), &json!(3000)),
        ]
    );
}

#[test]
fn churn_per_class() {
    let pairs = |dc_oc: i64, dc_os: i64, sa_oc: i64, stable: i64| json!({DC_OC: dc_oc, DC_OS: dc_os, SA_OC: sa_oc, STABLE: stable});
    let stamped = churn_row(&REPORT, "//app:stamped", &format!("{OUT}app/version.txt"));
    assert_eq!(stamped["pairs"], pairs(3, 0, 0, 0));
    assert_eq!(stamped["configuration_id"], CONFIG);
    assert_eq!(stamped["invocations"], 4);
    let codegen = churn_row(&REPORT, "//lib:codegen", &format!("{OUT}lib/gen.h"));
    assert_eq!(codegen["pairs"], pairs(0, 3, 0, 0));
    let archive = churn_row(&REPORT, "//tools:archive", &format!("{OUT}tools/sdk.tar"));
    assert_eq!(archive["pairs"], pairs(0, 0, 3, 0));
    let flip = churn_row(&REPORT, "//app:flip", &format!("{OUT}app/flip.bin"));
    assert_eq!(flip["pairs"], pairs(3, 0, 0, 0));
    assert_eq!(flip["complete_attempts"], 4);
}

#[test]
fn novelty() {
    let runs = REPORT["novelty"]["runs"].as_array().unwrap();
    let summary: Vec<Value> = runs
        .iter()
        .map(|r| {
            json!([
                r["invocation"],
                r["attempts"],
                r["distinct_digests"],
                r["run_bytes"],
                r["new_digests"],
                r["new_bytes"],
                r["cumulative_new_bytes"]
            ])
        })
        .collect();
    assert_eq!(
        summary,
        [
            json!(["inv-1", 7, 10, 29250, 10, 29250, 29250]),
            json!(["inv-2", 5, 5, 20000, 4, 18000, 47250]),
            json!(["inv-3", 5, 5, 21000, 2, 11000, 58250]),
            json!(["inv-4", 5, 5, 16600, 3, 11600, 69850]),
        ]
    );
    assert_eq!(runs[0]["first_event"], "2026-10-15T10:00:01.000Z");
    // Run 2's flip attempt is the last block in worker-events.txt; order comes
    // from timestamps, not from the file.
    assert_eq!(runs[1]["first_event"], "2026-10-15T10:10:01.000Z");
    assert_eq!(REPORT["novelty"]["total_new_bytes"], 69850);
}

#[test]
fn novelty_with_seed() {
    let novelty = &SEEDED["novelty"];
    assert_eq!(novelty["seeded_digests"], 3);
    let summary: Vec<Value> = novelty["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| json!([r["new_digests"], r["new_bytes"], r["cumulative_new_bytes"]]))
        .collect();
    assert_eq!(
        summary,
        [
            json!([9, 24250, 24250]),
            json!([4, 18000, 42250]),
            json!([2, 11000, 53250]),
            json!([3, 11600, 64850]),
        ]
    );
    assert_eq!(SEEDED["producers"]["first_observed_bytes"], 64850);
    let core = producer(&SEEDED, "//lib:core", &format!("{OUT}lib/core.o"));
    assert_eq!(core["first_observed_bytes"], 0);
}

#[test]
fn cas_write_rewrites() {
    let cas = &REPORT["rewrites"]["cas_write"];
    assert_eq!(
        cas["totals"],
        json!({
            "writes": 6,
            "bytes": 155_000,
            "first_writes": 3,
            "first_write_bytes": 53000,
            "rewrites": 3,
            "rewrite_bytes": 102_000,
            "prewindow_rewrites": 0,
            "prewindow_rewrite_bytes": 0,
            "errors": 1,
            "error_bytes": 4000,
        })
    );
    assert_eq!(cas["distinct_written"], 3);
    let bytestream = find(&cas["by_group"], &[("source", "bytestream")]);
    assert_eq!(bytestream["bytes"], 154_000);
    assert_eq!(bytestream["first_write_bytes"], 52000);
    assert_eq!(bytestream["rewrite_bytes"], 102_000);
    assert_eq!(bytestream["error_codes"], json!({"Unavailable": 1}));
    let batch = find(&cas["by_group"], &[("source", "batch_update")]);
    assert_eq!(batch["first_write_bytes"], 1000);
    assert_eq!(batch["rewrite_bytes"], 0);
    let top: Vec<Value> = cas["top_rewritten"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| {
            json!([
                r["digest"],
                r["writes"],
                r["rewrite_bytes"],
                r["invocations"]
            ])
        })
        .collect();
    assert_eq!(
        top,
        [
            json!([digest("shared-lib", 50000), 3, 100_000, 3]),
            json!([digest("gen-h", 2000), 2, 2000, 0]),
        ]
    );
}

#[test]
fn backend_write_rewrites() {
    let backend = &REPORT["rewrites"]["backend_write"];
    assert_eq!(backend["totals"]["bytes"], 154_600);
    assert_eq!(backend["totals"]["first_write_bytes"], 52300);
    assert_eq!(backend["totals"]["rewrite_bytes"], 102_300);
    assert_eq!(backend["totals"]["errors"], 1);
    let groups = backend["by_group"].as_array().unwrap();
    let mut keys: Vec<(&Value, &Value)> = groups
        .iter()
        .map(|row| (&row["store"], &row["prefix"]))
        .collect();
    keys.sort_by_key(|(store, prefix)| (store.to_string(), prefix.to_string()));
    assert_eq!(
        keys,
        [
            (&json!("azure_blob"), &json!("ac/")),
            (&json!("azure_blob"), &json!("cas/"))
        ]
    );
    let cas = find(&backend["by_group"], &[("prefix", "cas/")]);
    let ac = find(&backend["by_group"], &[("prefix", "ac/")]);
    assert_eq!(cas["rewrite_bytes"], 102_000);
    assert_eq!(ac["first_write_bytes"], 300);
    assert_eq!(ac["rewrite_bytes"], 300);
    assert_eq!(backend["totals"]["versioned_writes"], 4);
    assert_eq!(backend["totals"]["retained_by_versioning_bytes"], 52000);
    // The AC lines carry no version_id key at all, as older logs do.
    assert_eq!(ac["versioned_writes"], 0);
    assert_eq!(ac["retained_by_versioning_bytes"], 0);
    assert!(
        REPORT["rewrites"]["cas_write"]["totals"]
            .get("versioned_writes")
            .is_none()
    );
    // Six backend lines carry the key (two of them empty); the AC lines lack it.
    assert_eq!(backend["version_id_reported"], 6);
    let top = backend["top_rewritten"].as_array().unwrap();
    assert_eq!(
        top[0]["key"],
        format!("cas/{}", digest("shared-lib", 50000))
    );
    assert_eq!(top[0]["rewrite_bytes"], 100_000);
    assert_eq!(top[0]["invocations"], 3);
    assert_eq!(top.len(), 3);
}

#[test]
fn rewrites_with_seed() {
    let cas = &SEEDED["rewrites"]["cas_write"]["totals"];
    assert_eq!(cas["first_write_bytes"], 3000);
    assert_eq!(cas["rewrite_bytes"], 102_000);
    assert_eq!(cas["prewindow_rewrite_bytes"], 50000);
    let backend = &SEEDED["rewrites"]["backend_write"]["totals"];
    assert_eq!(backend["first_write_bytes"], 2300);
    assert_eq!(backend["prewindow_rewrite_bytes"], 50000);
    // shared's first in-window write overwrote a pre-window blob and returned
    // a version ID, so that copy is retained too.
    assert_eq!(backend["retained_by_versioning_bytes"], 102_000);
}

#[test]
fn client_uploads() {
    let uploads = &REPORT["client_uploads"];
    assert_eq!(uploads["target_count"], 1);
    assert_eq!(uploads["bytes"], 155_000);
    assert_eq!(uploads["bytes_without_metadata"], 5000);
    assert_eq!(uploads["worker_events_present"], true);
    assert_eq!(uploads["bytes_from_targets_no_worker_ran"], 150_000);
    assert_eq!(
        uploads["rows"],
        json!([
            {
                "target_id": "//lib:shared",
                "action_mnemonic": "CppLink",
                "seen_by_worker": false,
                "writes": 3,
                "bytes": 150_000,
                "distinct_digests": 1,
                "first_write_bytes": 50000,
                "rewrite_bytes": 100_000,
                "prewindow_rewrite_bytes": 0,
                "invocations": 3,
            },
            {
                "target_id": "(no metadata)",
                "action_mnemonic": "",
                "seen_by_worker": null,
                "writes": 3,
                "bytes": 5000,
                "distinct_digests": 2,
                "first_write_bytes": 3000,
                "rewrite_bytes": 2000,
                "prewindow_rewrite_bytes": 0,
                "invocations": 0,
            },
        ])
    );
}

#[test]
fn client_uploads_with_seed() {
    let rows = SEEDED["client_uploads"]["rows"].as_array().unwrap();
    let targets: Vec<&Value> = rows.iter().map(|r| &r["target_id"]).collect();
    assert_eq!(targets, [&json!("(no metadata)"), &json!("//lib:shared")]);
    assert_eq!(
        json!([
            rows[1]["first_write_bytes"],
            rows[1]["rewrite_bytes"],
            rows[1]["prewindow_rewrite_bytes"]
        ]),
        json!([0, 100_000, 50000])
    );
}

#[test]
fn has_health() {
    let health = &REPORT["has_health"];
    assert_eq!(health["upload_events"], 24);
    assert_eq!(health["uploaded_bytes"], 72250);
    let rows = &health["by_has_result"];
    let mut names: Vec<&str> = rows
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["has_result"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["absent", "error", "present"]);
    let absent = find(rows, &[("has_result", "absent")]);
    let error = find(rows, &[("has_result", "error")]);
    let present = find(rows, &[("has_result", "present")]);
    assert_eq!(
        json!([
            absent["events"],
            absent["bytes"],
            absent["uploaded"],
            absent["uploaded_bytes"]
        ]),
        json!([18, 64250, 17, 60250])
    );
    assert_eq!(
        absent["dispositions"],
        json!({"upload_failed": 1, "uploaded": 17})
    );
    assert_eq!(
        json!([error["events"], error["bytes"], error["uploaded_bytes"]]),
        json!([2, 12000, 12000])
    );
    assert_eq!(present["dispositions"], json!({"skipped_present": 4}));
    assert_eq!(present["uploaded_bytes"], 0);
    assert_eq!(error["has_elapsed_ms_p95"], 3000);
    // core-o said absent in run 3 after run 1 uploaded it; gen-h was
    // re-uploaded after a has() error although it was already stored.
    assert_eq!(
        health["uploads_of_already_stored_digests"],
        json!({"absent": {"uploads": 1, "bytes": 5000}, "error": {"uploads": 1, "bytes": 2000}})
    );
    let errors: Vec<&Value> = health["top_has_errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| &r["has_error"])
        .collect();
    assert_eq!(
        errors,
        [
            &json!("DeadlineExceeded: slow tier probe exceeded 3s"),
            &json!("Unavailable: connection reset")
        ]
    );
    assert_eq!(
        health["find_missing"],
        json!({
            "requests": 2,
            "errors": 0,
            "requested_count": 5,
            "requested_bytes": 64000,
            "missing_count": 1,
            "missing_bytes": 50000,
        })
    );
}

#[test]
fn coverage() {
    let coverage = &REPORT["coverage"];
    assert_eq!(coverage["attempts_seen"], 22);
    assert_eq!(coverage["attempts_started"], 22);
    assert_eq!(coverage["attempts_finished"], 21);
    assert_eq!(coverage["started_without_finished"], 1);
    let examples = coverage["started_without_finished_examples"]
        .as_array()
        .unwrap();
    assert_eq!(examples.len(), 1);
    assert_eq!(examples[0]["target_id"], "//app:hung");
    assert_eq!(examples[0]["started"], "2026-10-15T10:30:05.000Z");
    // a3-core's attempt_finished precedes its attempt_started in the file.
    assert_eq!(coverage["finished_without_started"], 0);
    assert_eq!(coverage["outputs_incomplete"], 1);
    assert_eq!(
        coverage["outputs_incomplete_by_outcome"],
        json!({"upload_failed": 1})
    );
    assert_eq!(coverage["complete_but_not_completed"], 0);
    assert_eq!(coverage["manifest_count_mismatches"], 0);
    assert_eq!(coverage["outputs_without_attempt_started"], 0);
    assert_eq!(coverage["outputs_without_metadata"], 0);
    assert_eq!(coverage["unknown_schema"], json!({"2": 1}));
    assert_eq!(coverage["unknown_events"], json!({"future_event": 1}));
    assert_eq!(coverage["events_without_timestamp"], 0);
}

#[test]
fn top_limits_ranked_tables_only() {
    let report = fixture(&BTreeSet::new(), 2);
    assert_eq!(report.producers.rows.len(), 2);
    assert_eq!(report.producers.producer_count, 14);
    assert_eq!(report.churn.rows.len(), 2);
    assert_eq!(report.novelty.runs.len(), 4);
}

// --------------------------------------------------------------------------
// The command line.

#[test]
fn gzip_file_and_stdin_match_plain_files() {
    let dir = TempDir::new("gzip");
    let worker = dir.file("worker-events.txt", WORKER.as_bytes());
    let server = dir.file("server-events.txt", SERVER.as_bytes());
    let compressed = dir.file("server-events.txt.gz", &gzip(SERVER.as_bytes()));
    let (code, plain, _) = run_cli(&["--json", "--top", "0", &worker, &server], b"");
    assert_eq!(code, 0);
    let (code, mixed, _) = run_cli(
        &["--json", "--top", "0", "-", &compressed],
        WORKER.as_bytes(),
    );
    assert_eq!(code, 0);
    let mut plain: Value = serde_json::from_str(&plain).unwrap();
    let mut mixed: Value = serde_json::from_str(&mixed).unwrap();
    plain["inputs"].as_object_mut().unwrap().remove("files");
    mixed["inputs"].as_object_mut().unwrap().remove("files");
    assert_eq!(plain, mixed);
}

#[test]
fn gzip_on_stdin() {
    let (code, out, _) = run_cli(&["--json"], &gzip(WORKER.as_bytes()));
    assert_eq!(code, 0);
    let report: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(report["inputs"]["events_by_kind"]["attempt_started"], 22);
}

#[test]
fn warnings_go_to_stderr() {
    let dir = TempDir::new("warnings");
    let worker = dir.file("worker-events.txt", WORKER.as_bytes());
    let seed = dir.file("seed-digests.txt", SEED.as_bytes());
    let (_, _, err) = run_cli(&["--seed-digests", &seed, &worker], b"");
    assert!(
        err.contains("skipped 1 provenance event(s) with schema 2"),
        "{err}"
    );
    assert!(err.contains("1 line(s)"), "{err}");
    let (_, _, empty) = run_cli(&[], b"not json\n");
    assert!(empty.contains("no provenance events found"), "{empty}");
    let (code, _, missing) = run_cli(&[&dir.0.join("absent.log").to_string_lossy()], b"");
    assert_eq!(code, 2);
    assert!(missing.starts_with("error: "), "{missing}");
}

#[test]
fn text_report() {
    let dir = TempDir::new("text");
    let worker = dir.file("worker-events.txt", WORKER.as_bytes());
    let server = dir.file("server-events.txt", SERVER.as_bytes());
    let seed = dir.file("seed-digests.txt", SEED.as_bytes());
    let (_, text, _) = run_cli(&["--seed-digests", &seed, &worker, &server], b"");
    for heading in [
        "Window: 2026-10-15T10:00:00.500Z .. 2026-10-15T10:30:07.000Z (30m 06s)",
        "1. Producers by first-observed output bytes (14 of 14 shown)",
        "1b. Client uploads by target",
        "2. Exact identity",
        "3. Logical producer churn",
        "4. Novelty per invocation, in order of first event, seeded with 3 digests",
        "5. Rewrite amplification",
        "6. has() health",
        "7. Coverage",
    ] {
        assert!(text.contains(heading), "{heading}");
    }
    assert!(text.contains("39.1 KiB"));
    assert!(text.contains("e2ee005cedae..-50000"));
    assert!(text.contains(
        "1b. Client uploads by target: cas_write (result=ok) grouped by request metadata \
         (1 of 1 targets shown)"
    ));
    // Seeded: shared's pre-window overwrite is retained as well (102_000 B).
    assert!(text.contains("retained by versioning 99.6 KiB"));
    assert!(text.contains("(no metadata)"));
    let (_, wide, _) = run_cli(&["--wide", &worker, &server], b"");
    assert!(wide.contains(&digest("shared-lib", 50000)));
}

// --------------------------------------------------------------------------
// Line shapes.

fn status(line: &str) -> &'static str {
    match classify_line(line) {
        Line::Blank => "blank",
        Line::NonJson => "non_json",
        Line::NonProvenance => "non_provenance",
        Line::Provenance { .. } => "provenance",
    }
}

#[test]
fn tracing_json() {
    let line = tracing("2026-10-15T10:00:00Z", &json!({"event": "find_missing"}));
    let Line::Provenance {
        record,
        raw_timestamp,
    } = classify_line(&line)
    else {
        panic!("not provenance: {line}");
    };
    assert_eq!(record["event"], "find_missing");
    assert_eq!(raw_timestamp, Some(json!("2026-10-15T10:00:00Z")));
}

#[test]
fn other_targets_are_not_provenance() {
    let line =
        json!({"target": "nativelink_worker", "fields": {"event": "x", "schema": 1}}).to_string();
    assert_eq!(status(&line), "non_provenance");
    assert_eq!(status("[1, 2]"), "non_provenance");
    assert_eq!(
        status(&json!({"event": "cas_write"}).to_string()),
        "non_provenance"
    );
}

#[test]
fn prefixed_and_broken_lines() {
    let prefixed = format!(
        "2026-10-15T10:00:00.1Z stdout F {}",
        tracing("2026-10-15T10:00:00Z", &json!({"event": "cas_write"}))
    );
    assert_eq!(status(&prefixed), "provenance");
    assert_eq!(status("worker-1  | {not json"), "non_json");
    assert_eq!(status("plain text"), "non_json");
    assert_eq!(status("   "), "blank");
}

#[test]
fn otlp_attributes() {
    let line = json!({
        "timeUnixNano": "1792058400000000000",
        "attributes": {"event": "find_missing", "schema": 1},
    })
    .to_string();
    let Line::Provenance {
        record,
        raw_timestamp,
    } = classify_line(&line)
    else {
        panic!("not provenance: {line}");
    };
    assert_eq!(record["event"], "find_missing");
    assert_eq!(parse_timestamp(raw_timestamp.as_ref()), Some(BASE_NS));
}

// --------------------------------------------------------------------------
// Defensive cases.

#[test]
fn events_with_missing_fields() {
    let (report, value) = collect(&[
        tracing("2026-10-15T10:00:00Z", &json!({"event": "output_observed"})),
        tracing("2026-10-15T10:00:01Z", &json!({"event": "output_upload"})),
        tracing(
            "2026-10-15T10:00:02Z",
            &json!({"event": "attempt_finished"}),
        ),
        tracing("not a time", &json!({"event": "cas_write"})),
        tracing(
            "2026-10-15T10:00:03Z",
            &json!({"event": "backend_write", "key": "no-digest-here"}),
        ),
        tracing("2026-10-15T10:00:04Z", &json!({"event": "find_missing"})),
        json!({"target": "nativelink_provenance", "fields": {"event": "cas_write"}}).to_string(),
    ]);
    assert_eq!(value["inputs"]["provenance_events"], 6);
    assert_eq!(value["coverage"]["unknown_schema"], json!({"missing": 1}));
    assert_eq!(value["coverage"]["events_without_timestamp"], 1);
    assert_eq!(value["coverage"]["finished_without_started"], 1);
    assert_eq!(value["coverage"]["outputs_without_attempt_started"], 1);
    assert_eq!(value["rewrites"]["cas_write"]["totals"]["errors"], 1);
    assert_eq!(
        value["rewrites"]["backend_write"]["by_group"][0]["prefix"],
        ""
    );
    render_text(&report, 0, false);
}

#[test]
fn oscillation_stops_adding_novelty_after_two_runs() {
    let mut lines = Vec::new();
    for (run, side) in (1..).zip(['a', 'b', 'a', 'b']) {
        let attempt = format!("attempt-{run}");
        let ts = format!("2026-10-15T1{run}:00:00Z");
        lines.push(tracing(
            &ts,
            &json!({
                "event": "attempt_started",
                "attempt_id": attempt,
                "action_digest": format!("{}-{}", "a".repeat(64), run % 2),
                "tool_invocation_id": format!("run-{run}"),
                "target_id": "//app:flip",
            }),
        ));
        lines.push(tracing(
            &ts,
            &json!({
                "event": "output_observed",
                "attempt_id": attempt,
                "kind": "file",
                "path": "flip.bin",
                "digest": format!("{}-3000", side.to_string().repeat(64)),
                "size_bytes": 3000,
                "first_in_attempt": true,
            }),
        ));
        lines.push(tracing(
            &ts,
            &json!({
                "event": "attempt_finished",
                "attempt_id": attempt,
                "outcome": "completed",
                "outputs_complete": true,
                "manifest_fingerprint": side.to_string(),
            }),
        ));
    }
    let (_, report) = collect(&lines);
    let new: Vec<&Value> = report["novelty"]["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| &run["new_bytes"])
        .collect();
    assert_eq!(new, [&json!(3000), &json!(3000), &json!(0), &json!(0)]);
    assert_eq!(report["churn"]["summary"][DC_OC]["pairs"], 3);
}

#[test]
fn killed_attempt_with_complete_outputs_is_not_compared() {
    // The worker reports outputs_complete=true for an attempt it killed if it
    // uploaded what was there. Its manifest is not the action's output.
    let action = format!("{}-140", "d".repeat(64));
    let mut lines = Vec::new();
    for (run, (outcome, side)) in
        (1..).zip([("completed", 'a'), ("killed", 'b'), ("timed_out", 'c')])
    {
        let attempt = format!("attempt-{run}");
        let ts = format!("2026-10-15T1{run}:00:00Z");
        lines.push(tracing(
            &ts,
            &json!({
                "event": "attempt_started",
                "attempt_id": attempt,
                "action_digest": action,
                "tool_invocation_id": format!("run-{run}"),
                "target_id": "//app:slow",
            }),
        ));
        lines.push(tracing(
            &ts,
            &json!({
                "event": "output_observed",
                "attempt_id": attempt,
                "action_digest": action,
                "kind": "file",
                "path": "out.bin",
                "digest": format!("{}-10", side.to_string().repeat(64)),
                "size_bytes": 10,
            }),
        ));
        lines.push(tracing(
            &ts,
            &json!({
                "event": "attempt_finished",
                "attempt_id": attempt,
                "action_digest": action,
                "outcome": outcome,
                "outputs_complete": true,
                "output_count": 1,
                "manifest_fingerprint": side.to_string(),
            }),
        ));
    }
    let (report, value) = collect(&lines);
    assert_eq!(value["exact_identity"]["actions_checked"], 1);
    assert_eq!(
        value["exact_identity"]["actions_with_multiple_manifests"],
        0
    );
    assert_eq!(value["churn"]["producers_compared"], 0);
    let pairs: i64 = value["churn"]["summary"]
        .as_object()
        .unwrap()
        .values()
        .map(|data| data["pairs"].as_i64().unwrap())
        .sum();
    assert_eq!(pairs, 0);
    assert_eq!(value["coverage"]["complete_but_not_completed"], 2);
    assert_eq!(
        value["coverage"]["complete_but_not_completed_by_outcome"],
        json!({"killed": 1, "timed_out": 1})
    );
    // Their outputs were still produced and uploaded, so they still count.
    assert_eq!(value["producers"]["first_observed_bytes"], 30);
    let text = render_text(&report, 0, false);
    assert!(text.contains("outcome not completed (left out of 2 and 3): 2"));
}

#[test]
fn client_uploads_attribute_locally_executed_actions() {
    let cas_write = |digest: char, size: i64, target: &str, mnemonic: &str| {
        tracing(
            "2026-10-15T10:00:01Z",
            &json!({
                "event": "cas_write",
                "source": "bytestream",
                "digest": format!("{}-{size}", digest.to_string().repeat(64)),
                "size_bytes": size,
                "result": "ok",
                "target_id": target,
                "action_mnemonic": mnemonic,
                "tool_invocation_id": if target.is_empty() { "" } else { "inv-1" },
            }),
        )
    };
    let uploads = vec![
        cas_write('1', 100, "//app:remote", "Genrule"),
        cas_write('2', 5000, "//app:local", "CppLink"),
        cas_write('2', 5000, "//app:local", "CppLink"),
        cas_write('3', 7, "", "Genrule"),
    ];
    let started = tracing(
        "2026-10-15T10:00:00Z",
        &json!({
            "event": "attempt_started",
            "attempt_id": "x",
            "target_id": "//app:remote",
            "action_mnemonic": "Genrule",
        }),
    );
    let mut with_worker = vec![started];
    with_worker.extend(uploads.iter().cloned());
    let (_, value) = collect(&with_worker);
    let rows = &value["client_uploads"]["rows"];
    let remote = find(rows, &[("target_id", "//app:remote")]);
    let local = find(rows, &[("target_id", "//app:local")]);
    let anonymous = find(rows, &[("target_id", "(no metadata)")]);
    assert_eq!(remote["seen_by_worker"], true);
    assert_eq!(local["seen_by_worker"], false);
    assert_eq!(
        json!([local["first_write_bytes"], local["rewrite_bytes"]]),
        json!([5000, 5000])
    );
    // An empty target is one row whatever the mnemonic says.
    assert_eq!(anonymous["seen_by_worker"], Value::Null);
    assert_eq!(anonymous["action_mnemonic"], "");
    // Without worker events, nothing can be called unseen.
    let (alone_report, alone) = collect(&uploads);
    assert_eq!(alone["client_uploads"]["worker_events_present"], false);
    assert!(
        alone["client_uploads"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["seen_by_worker"].is_null())
    );
    assert!(!render_text(&alone_report, 0, false).contains(" worker "));
}

#[test]
fn backend_logs_without_version_id() {
    let line = tracing(
        "2026-10-15T10:00:00Z",
        &json!({
            "event": "backend_write",
            "store": "azure_blob",
            "key": format!("cas/{}-9", "e".repeat(64)),
            "size_bytes": 9,
            "result": "ok",
        }),
    );
    let (report, value) = collect(&[line.clone(), line]);
    let backend = &value["rewrites"]["backend_write"];
    assert_eq!(backend["version_id_reported"], 0);
    assert_eq!(backend["totals"]["rewrite_bytes"], 9);
    assert_eq!(backend["totals"]["retained_by_versioning_bytes"], 0);
    let text = render_text(&report, 0, false);
    assert!(text.contains("versioning unknown (no backend_write carries version_id)"));
    assert!(!text.contains("retained by versioning"));
}

#[test]
fn unfinished_attempt_is_not_compared() {
    let (_, report) = collect(&[
        tracing(
            "2026-10-15T10:00:00Z",
            &json!({
                "event": "attempt_started",
                "attempt_id": "x",
                "action_digest": format!("{}-1", "b".repeat(64)),
            }),
        ),
        tracing(
            "2026-10-15T10:00:01Z",
            &json!({
                "event": "output_observed",
                "attempt_id": "x",
                "kind": "file",
                "path": "p",
                "digest": format!("{}-5", "c".repeat(64)),
                "size_bytes": 5,
            }),
        ),
    ]);
    assert_eq!(report["churn"]["producers_compared"], 0);
    assert_eq!(report["exact_identity"]["actions_checked"], 0);
    assert_eq!(report["coverage"]["started_without_finished"], 1);
    assert_eq!(report["producers"]["first_observed_bytes"], 5);
}

// --------------------------------------------------------------------------
// Helpers.

#[test]
fn parse_timestamps() {
    let parse = |value: Value| parse_timestamp(Some(&value));
    assert_eq!(parse(json!("2026-10-15T10:00:00Z")), Some(BASE_NS));
    assert_eq!(
        parse(json!("2026-10-15T10:00:00.123456789Z")),
        Some(BASE_NS + 123_456_789)
    );
    assert_eq!(
        parse(json!("2026-10-15T12:00:00.5+02:00")),
        Some(BASE_NS + 500_000_000)
    );
    assert_eq!(parse(json!("2026-10-15 10:00:00")), Some(BASE_NS));
    assert_eq!(parse(json!(1_792_058_400)), Some(BASE_NS));
    assert_eq!(parse(json!(1_792_058_400_000_i64)), Some(BASE_NS));
    assert_eq!(parse(json!("1792058400000000")), Some(BASE_NS));
    assert_eq!(parse(json!(BASE_NS.to_string())), Some(BASE_NS));
    assert_eq!(parse(json!(1_792_058_400.25)), Some(BASE_NS + 250_000_000));
    assert_eq!(parse(json!("yesterday")), None);
    assert_eq!(parse_timestamp(None), None);
    assert_eq!(parse(Value::Null), None);
    assert_eq!(parse(json!(true)), None);
}

#[test]
fn formatting() {
    assert_eq!(
        format_timestamp(Some(BASE_NS + 7_000_000)),
        "2026-10-15T10:00:00.007Z"
    );
    assert_eq!(human_bytes(1023.0), "1023 B");
    assert_eq!(human_bytes(1536.0), "1.5 KiB");
    assert_eq!(human_bytes(15.0 * 1024.0_f64.powi(4)), "15.0 TiB");
    assert_eq!(format_duration(1806.5), "30m 06s");
    assert_eq!(format_duration(90061.0), "1d 01h 01m");
}

#[test]
fn percentiles() {
    let values: Vec<i64> = (1..=20).collect();
    assert_eq!(percentile(&values, 50), 10);
    assert_eq!(percentile(&values, 95), 19);
    assert_eq!(percentile(&values, 100), 20);
    assert_eq!(percentile(&[], 95), 0);
    assert_eq!(percentile(&[7], 50), 7);
}

#[test]
fn digests_and_keys() {
    assert_eq!(digest_hash("ABC123-42"), "abc123");
    assert_eq!(digest_hash("abc123"), "abc123");
    assert_eq!(digest_hash(""), "");
    let hash = "f".repeat(64);
    assert_eq!(
        split_backend_key(&format!("prod/cas/{hash}-10")),
        ("prod/cas/".to_string(), hash)
    );
    assert_eq!(
        split_backend_key("no-digest"),
        (String::new(), String::new())
    );
}

#[test]
fn field_coercion() {
    assert_eq!(as_int(Some(&json!("12"))), 12);
    assert_eq!(as_int(Some(&json!("12.0"))), 12);
    assert_eq!(as_int(None), 0);
    assert_eq!(as_int(Some(&json!("x"))), 0);
    assert!(as_bool(Some(&json!("true"))));
    assert!(!as_bool(Some(&json!("false"))));
    assert_eq!(as_schema(Some(&json!("1"))), Some(1));
    assert_eq!(as_schema(Some(&json!("one"))), None);
}

#[test]
fn seed_file() {
    let (hashes, bad) = parse_seed(&mut SEED.as_bytes()).unwrap();
    assert_eq!(bad, 1);
    let want: BTreeSet<String> = [
        digest("core-o", 5000),
        digest("shared-lib", 50000),
        digest("never-seen-in-logs", 123),
    ]
    .iter()
    .map(|d| digest_hash(d))
    .collect();
    assert_eq!(hashes, want);
}
