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

//! Text and JSON renderings of what the subcommands found.

use nativelink_proto::build::bazel::remote::execution::v2::Platform;
use nativelink_util::common::DigestInfo;
use serde_json::{Value, json};

use crate::local::{ActionSpec, InputSummary, declared_outputs};
use crate::manifest::{Comparison, Problem, Run, Summary, interpretation, short_digest};

/// Appends one formatted line to a `String`.
macro_rules! line {
    ($out:expr) => {
        $out.push('\n')
    };
    ($out:expr, $($arg:tt)+) => {{
        $out.push_str(&format!($($arg)+));
        $out.push('\n');
    }};
}

pub(crate) fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn opt_digest(digest: Option<&DigestInfo>) -> Value {
    digest.map_or(Value::Null, |d| Value::String(d.to_string()))
}

fn platform_pairs(platform: Option<&Platform>) -> Vec<(String, String)> {
    platform.map_or_else(Vec::new, |p| {
        p.properties
            .iter()
            .map(|prop| (prop.name.clone(), prop.value.clone()))
            .collect()
    })
}

fn timeout_text(spec: &ActionSpec) -> String {
    spec.action.timeout.as_ref().map_or_else(
        || "(none)".to_string(),
        |t| {
            if t.nanos == 0 {
                format!("{}s", t.seconds)
            } else {
                format!("{}.{:09}s", t.seconds, t.nanos)
            }
        },
    )
}

pub(crate) fn problem_json(problem: &Problem) -> Value {
    match problem {
        Problem::Mismatch {
            what,
            expected,
            actual,
            via,
        } => json!({
            "kind": "digest_mismatch",
            "what": what,
            "expected": expected.to_string(),
            "actual": actual.to_string(),
            "via": via,
        }),
        Problem::Missing { what, digest } => json!({
            "kind": "missing",
            "what": what,
            "expected": digest.to_string(),
        }),
        Problem::ReadFailed {
            what,
            digest,
            message,
        } => json!({
            "kind": "read_failed",
            "what": what,
            "expected": digest.to_string(),
            "message": message,
        }),
    }
}

fn problems_text(out: &mut String, problems: &[Problem]) {
    for problem in problems {
        if problem.is_mismatch() {
            line!(out, "!! {}", problem.describe());
        } else {
            line!(out, "   {}", problem.describe());
        }
    }
}

pub(crate) fn show_text(spec: &ActionSpec, inputs: &InputSummary, problems: &[Problem]) -> String {
    let command = &spec.command;
    let mut out = String::new();
    line!(out, "Action        {}", spec.action_digest);
    line!(out, "Command       {}", spec.command_digest);
    line!(out, "Input root    {}", spec.input_root_digest);
    line!(out, "Timeout       {}", timeout_text(spec));
    line!(out, "do_not_cache  {}", spec.action.do_not_cache);
    if !spec.action.salt.is_empty() {
        line!(out, "Salt          {} bytes", spec.action.salt.len());
    }
    line!(out);
    line!(out, "Arguments ({})", command.arguments.len());
    for (i, arg) in command.arguments.iter().enumerate() {
        line!(out, "  [{i}] {arg}");
    }
    line!(out, "Environment ({})", command.environment_variables.len());
    for var in &command.environment_variables {
        line!(out, "  {}={}", var.name, var.value);
    }
    let workdir = if command.working_directory.is_empty() {
        "(input root)"
    } else {
        &command.working_directory
    };
    line!(out, "Working directory  {workdir}");
    if command.output_paths.is_empty() {
        line!(out, "Output files ({})", command.output_files.len());
        for path in &command.output_files {
            line!(out, "  {path}");
        }
        line!(
            out,
            "Output directories ({})",
            command.output_directories.len()
        );
        for path in &command.output_directories {
            line!(out, "  {path}");
        }
    } else {
        line!(out, "Output paths ({})", command.output_paths.len());
        for path in &command.output_paths {
            line!(out, "  {path}");
        }
    }
    let action_platform = platform_pairs(spec.action.platform.as_ref());
    let command_platform = platform_pairs(command.platform.as_ref());
    line!(out, "Platform (Action) ({})", action_platform.len());
    for (name, value) in &action_platform {
        line!(out, "  {name}={value}");
    }
    if !command_platform.is_empty() {
        line!(
            out,
            "Platform (Command, deprecated) ({})",
            command_platform.len()
        );
        for (name, value) in &command_platform {
            line!(out, "  {name}={value}");
        }
    }
    line!(out);
    line!(out, "Input root");
    line!(
        out,
        "  files        {} ({} executable)",
        inputs.files,
        inputs.executables
    );
    line!(
        out,
        "  directories  {} (below the root)",
        inputs.directories
    );
    line!(out, "  symlinks     {}", inputs.symlinks);
    line!(
        out,
        "  total bytes  {} ({})",
        inputs.total_bytes,
        human_bytes(inputs.total_bytes)
    );
    line!(
        out,
        "  distinct     {} blobs, {} ({})",
        inputs.unique_blobs,
        inputs.unique_bytes,
        human_bytes(inputs.unique_bytes)
    );
    if !inputs.largest.is_empty() {
        line!(out, "  largest");
        for (path, size) in &inputs.largest {
            line!(out, "    {:>10}  {path}", human_bytes(*size));
        }
    }
    if !problems.is_empty() {
        line!(out);
        line!(out, "Problems reading the input root ({})", problems.len());
        problems_text(&mut out, problems);
    }
    out
}

pub(crate) fn show_json(spec: &ActionSpec, inputs: &InputSummary, problems: &[Problem]) -> Value {
    let command = &spec.command;
    let pairs = |pairs: Vec<(String, String)>| -> Value {
        pairs
            .into_iter()
            .map(|(name, value)| json!({"name": name, "value": value}))
            .collect()
    };
    json!({
        "action_digest": spec.action_digest.to_string(),
        "command_digest": spec.command_digest.to_string(),
        "input_root_digest": spec.input_root_digest.to_string(),
        "arguments": command.arguments,
        "environment": command
            .environment_variables
            .iter()
            .map(|v| json!({"name": v.name, "value": v.value}))
            .collect::<Vec<_>>(),
        "working_directory": command.working_directory,
        "output_paths": command.output_paths,
        "output_files": command.output_files,
        "output_directories": command.output_directories,
        "declared_outputs": declared_outputs(command),
        "platform": pairs(platform_pairs(spec.action.platform.as_ref())),
        "command_platform": pairs(platform_pairs(command.platform.as_ref())),
        "timeout": spec.action.timeout.as_ref().map(|t| json!({"seconds": t.seconds, "nanos": t.nanos})),
        "do_not_cache": spec.action.do_not_cache,
        "salt_bytes": spec.action.salt.len(),
        "input_root": {
            "files": inputs.files,
            "executables": inputs.executables,
            "directories": inputs.directories,
            "symlinks": inputs.symlinks,
            "total_bytes": inputs.total_bytes,
            "unique_blobs": inputs.unique_blobs,
            "unique_bytes": inputs.unique_bytes,
            "largest": inputs
                .largest
                .iter()
                .map(|(path, size)| json!({"path": path, "size_bytes": size}))
                .collect::<Vec<_>>(),
        },
        "problems": problems.iter().map(problem_json).collect::<Vec<_>>(),
    })
}

fn run_status(run: &Run) -> &'static str {
    if run.mismatches() > 0 {
        "MISMATCH"
    } else if run.outcome.is_some() {
        "ok"
    } else {
        "failed"
    }
}

pub(crate) fn runs_text(label: &str, runs: &[Run], summary: &Summary) -> String {
    let mut out = String::new();
    line!(out, "{label}: {} run(s)", runs.len());
    line!(
        out,
        "{:>4}  {:<8}  {:>5}  {:<24}  {:>7}  {:>10}  {:<12}  {:<20}  {:<20}",
        "run",
        "status",
        "exit",
        "worker",
        "outputs",
        "bytes",
        "fingerprint",
        "stdout",
        "stderr"
    );
    for run in runs {
        let (exit, outputs, bytes, fingerprint, stdout, stderr) = run.outcome.as_ref().map_or_else(
            || {
                (
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                    "-".to_string(),
                )
            },
            |o| {
                (
                    o.exit_code.to_string(),
                    o.manifest.entries().len().to_string(),
                    human_bytes(o.manifest.total_bytes()),
                    o.manifest.fingerprint()[..12].to_string(),
                    o.stdout
                        .as_ref()
                        .map_or_else(|| "-".to_string(), short_digest),
                    o.stderr
                        .as_ref()
                        .map_or_else(|| "-".to_string(), short_digest),
                )
            },
        );
        let chars = run.worker.chars().count();
        let worker = if chars > 24 {
            let tail: String = run.worker.chars().skip(chars - 22).collect();
            format!("..{tail}")
        } else {
            run.worker.clone()
        };
        line!(
            out,
            "{:>4}  {:<8}  {exit:>5}  {worker:<24}  {outputs:>7}  {bytes:>10}  {fingerprint:<12}  {stdout:<20}  {stderr:<20}",
            run.index,
            run_status(run),
        );
    }
    for run in runs {
        let has_details = run.error.is_some()
            || !run.problems.is_empty()
            || !run.missing_outputs.is_empty()
            || !run.notes.is_empty();
        if !has_details {
            continue;
        }
        line!(out, "run {}:", run.index);
        if let Some(error) = &run.error {
            line!(out, "   error: {error}");
        }
        problems_text(&mut out, &run.problems);
        if !run.missing_outputs.is_empty() {
            line!(
                out,
                "   declared outputs not produced: {}",
                run.missing_outputs.join(", ")
            );
        }
        for note in &run.notes {
            line!(out, "   {note}");
        }
    }
    line!(out);
    summary_text(&mut out, label, summary);
    out
}

fn summary_text(out: &mut String, label: &str, summary: &Summary) {
    line!(
        out,
        "Summary ({label}): {} run(s), {} completed",
        summary.runs,
        summary.completed
    );
    line!(
        out,
        "  distinct output manifests: {}",
        summary.fingerprints.len()
    );
    for (fingerprint, runs) in &summary.fingerprints {
        let runs: Vec<String> = runs.iter().map(ToString::to_string).collect();
        line!(out, "    {fingerprint}  run(s) {}", runs.join(", "));
    }
    let exit_codes: Vec<String> = summary.exit_codes.iter().map(ToString::to_string).collect();
    line!(
        out,
        "  exit codes: {}",
        if exit_codes.is_empty() {
            "-".to_string()
        } else {
            exit_codes.join(", ")
        }
    );
    line!(
        out,
        "  distinct stdout: {}, distinct stderr: {}",
        summary.distinct_stdout,
        summary.distinct_stderr
    );
    if summary.varied_paths.is_empty() {
        line!(out, "  paths that varied: none");
    } else {
        line!(out, "  paths that varied ({}):", summary.varied_paths.len());
        for varied in &summary.varied_paths {
            line!(out, "    {}", varied.path);
            for (run, value) in &varied.per_run {
                line!(out, "      run {run}: {value}");
            }
        }
    }
    if summary.mismatches > 0 {
        line!(
            out,
            "!! {} blob(s) failed digest verification: the CAS served bytes that do not match their digest",
            summary.mismatches
        );
    } else {
        line!(out, "  digest verification failures: none");
    }
}

fn run_json(run: &Run) -> Value {
    let outcome = run.outcome.as_ref().map(|o| {
        json!({
            "exit_code": o.exit_code,
            "stdout_digest": opt_digest(o.stdout.as_ref()),
            "stderr_digest": opt_digest(o.stderr.as_ref()),
            "manifest_fingerprint": o.manifest.fingerprint(),
            "output_bytes": o.manifest.total_bytes(),
            "outputs": o
                .manifest
                .entries()
                .iter()
                .map(|e| json!({
                    "kind": e.kind.as_str(),
                    "path": e.path,
                    "digest": opt_digest(e.digest.as_ref()),
                    "executable": e.executable,
                    "symlink_target": e.target,
                }))
                .collect::<Vec<_>>(),
        })
    });
    json!({
        "run": run.index,
        "status": run_status(run),
        "worker": run.worker,
        "outcome": outcome,
        "error": run.error,
        "problems": run.problems.iter().map(problem_json).collect::<Vec<_>>(),
        "missing_outputs": run.missing_outputs,
        "notes": run.notes,
        "duration_ms": run.duration_ms,
    })
}

pub(crate) fn summary_json(summary: &Summary) -> Value {
    json!({
        "runs": summary.runs,
        "completed": summary.completed,
        "distinct_manifests": summary.fingerprints.len(),
        "varies": summary.varies(),
        "fingerprints": summary
            .fingerprints
            .iter()
            .map(|(f, runs)| json!({"manifest_fingerprint": f, "runs": runs}))
            .collect::<Vec<_>>(),
        "exit_codes": summary.exit_codes,
        "distinct_stdout": summary.distinct_stdout,
        "distinct_stderr": summary.distinct_stderr,
        "varied_paths": summary
            .varied_paths
            .iter()
            .map(|v| json!({
                "path": v.path,
                "per_run": v
                    .per_run
                    .iter()
                    .map(|(run, value)| json!({"run": run, "value": value}))
                    .collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>(),
        "digest_mismatches": summary.mismatches,
    })
}

pub(crate) fn runs_json(mode: &str, action: &DigestInfo, runs: &[Run], summary: &Summary) -> Value {
    json!({
        "mode": mode,
        "action_digest": action.to_string(),
        "runs": runs.iter().map(run_json).collect::<Vec<_>>(),
        "summary": summary_json(summary),
    })
}

pub(crate) fn compare_text(comparison: &Comparison, oracle: &[Run], nativelink: &[Run]) -> String {
    let mut out = runs_text("oracle (local)", oracle, &comparison.oracle);
    line!(out);
    out.push_str(&runs_text(
        "NativeLink (execute)",
        nativelink,
        &comparison.nativelink,
    ));
    line!(out);
    line!(out, "Comparison");
    line!(
        out,
        "  (a) oracle varies:      {}",
        yes_no(comparison.oracle.varies())
    );
    line!(
        out,
        "  (b) NativeLink varies:  {}",
        yes_no(comparison.nativelink.varies())
    );
    if comparison.oracle.completed == 0 {
        line!(
            out,
            "  (c) NativeLink output digests never seen in the oracle: not computed, no oracle run \
             completed"
        );
    } else {
        line!(
            out,
            "  (c) NativeLink output digests never seen in the oracle: {}",
            comparison.unmatched.len()
        );
    }
    for unmatched in &comparison.unmatched {
        let runs: Vec<String> = unmatched.runs.iter().map(ToString::to_string).collect();
        line!(
            out,
            "      {}  {}  (NativeLink run(s) {})",
            unmatched.path,
            unmatched.digest,
            runs.join(", ")
        );
    }
    if !comparison.unmatched_exit_codes.is_empty() {
        let codes: Vec<String> = comparison
            .unmatched_exit_codes
            .iter()
            .map(ToString::to_string)
            .collect();
        line!(
            out,
            "  NativeLink exit codes the oracle never returned: {}",
            codes.join(", ")
        );
    }
    line!(out, "  verdict: {}", comparison.verdict.as_str());
    line!(out);
    line!(out, "Interpretation");
    for paragraph in interpretation(comparison) {
        line!(out, "  {paragraph}");
    }
    out
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

pub(crate) fn compare_json(
    action: &DigestInfo,
    comparison: &Comparison,
    oracle: &[Run],
    nativelink: &[Run],
) -> Value {
    json!({
        "action_digest": action.to_string(),
        "oracle": runs_json("local", action, oracle, &comparison.oracle),
        "nativelink": runs_json("execute", action, nativelink, &comparison.nativelink),
        "oracle_completed": comparison.oracle.completed,
        "nativelink_completed": comparison.nativelink.completed,
        "oracle_varies": comparison.oracle.varies(),
        "nativelink_varies": comparison.nativelink.varies(),
        "nativelink_digests_not_in_oracle": comparison
            .unmatched
            .iter()
            .map(|u| json!({"path": u.path, "digest": u.digest.to_string(), "runs": u.runs}))
            .collect::<Vec<_>>(),
        "nativelink_exit_codes_not_in_oracle": comparison.unmatched_exit_codes,
        "digest_mismatches": comparison.oracle.mismatches + comparison.nativelink.mismatches,
        "verdict": comparison.verdict.as_str(),
        "interpretation": interpretation(comparison),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn human_bytes_uses_binary_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(10 * 1024 * 1024 * 1024 * 1024), "10.0 TiB");
    }
}
