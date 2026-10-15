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

//! The text report: aligned tables, humanised sizes, shortened digests.

use std::collections::BTreeMap;

use crate::analysis::{ChurnClass, NO_METADATA, Report, RewriteSection};
use crate::parse::{format_duration, human_bytes, split_backend_key};

/// Formats a byte count, which is always an integer in the report.
#[expect(
    clippy::cast_precision_loss,
    reason = "a byte count in a log is far below 2^53"
)]
fn bytes(count: i64) -> String {
    human_bytes(count as f64)
}

/// An aligned table, indented three spaces, or `(none)` with no rows.
fn table(headers: &[&str], rows: &[Vec<String>], right: &[usize]) -> String {
    if rows.is_empty() {
        return "   (none)".to_string();
    }
    let mut widths: Vec<usize> = headers.iter().map(|h| h.chars().count()).collect();
    for row in rows {
        for (index, value) in row.iter().enumerate() {
            widths[index] = widths[index].max(value.chars().count());
        }
    }
    let line = |values: &[String]| -> String {
        let parts: Vec<String> = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let width = widths[index];
                if right.contains(&index) {
                    format!("{value:>width$}")
                } else {
                    format!("{value:<width$}")
                }
            })
            .collect();
        format!("   {}", parts.join("  ").trim_end())
    };
    let header: Vec<String> = headers.iter().map(ToString::to_string).collect();
    let rule: Vec<String> = widths.iter().map(|width| "-".repeat(*width)).collect();
    let mut out = vec![line(&header), line(&rule)];
    out.extend(rows.iter().map(|row| line(row)));
    out.join("\n")
}

/// Shortens digests and long text unless `--wide` was given.
#[derive(Debug, Clone, Copy)]
struct Shortener {
    wide: bool,
}

impl Shortener {
    fn digest(self, digest: &str) -> String {
        if digest.is_empty() {
            return "-".to_string();
        }
        if self.wide {
            return digest.to_string();
        }
        if let Some((head, tail)) = digest.rsplit_once('-')
            && head.chars().count() > 12
        {
            let start: String = head.chars().take(12).collect();
            return format!("{start}..-{tail}");
        }
        digest.to_string()
    }

    fn text(self, text: &str, width: usize) -> String {
        if text.is_empty() {
            return "-".to_string();
        }
        let chars: Vec<char> = text.chars().collect();
        if self.wide || chars.len() <= width {
            return text.to_string();
        }
        let keep = width - 3;
        let head: String = chars[..keep / 3].iter().collect();
        let tail: String = chars[chars.len() - (keep - keep / 3)..].iter().collect();
        format!("{head}...{tail}")
    }

    fn key(self, key: &str) -> String {
        let (prefix, hashed) = split_backend_key(key);
        if self.wide || hashed.is_empty() {
            return self.text(key, 64);
        }
        format!("{prefix}{}", self.digest(&key[prefix.len()..]))
    }
}

fn fmt_counter<'a>(counts: impl IntoIterator<Item = (&'a str, i64)>) -> String {
    let text: Vec<String> = counts
        .into_iter()
        .map(|(name, count)| format!("{name}:{count}"))
        .collect();
    if text.is_empty() {
        "-".to_string()
    } else {
        text.join(" ")
    }
}

fn fmt_map(counts: &BTreeMap<String, i64>) -> String {
    fmt_counter(counts.iter().map(|(name, count)| (name.as_str(), *count)))
}

#[expect(
    clippy::cast_precision_loss,
    reason = "a byte count in a log is far below 2^53"
)]
fn per_day(count: i64, duration: Option<f64>) -> String {
    match duration {
        Some(duration) if duration > 0.0 => {
            format!(
                " ({}/day at this rate)",
                human_bytes((count * 86_400) as f64 / duration)
            )
        }
        _ => String::new(),
    }
}

#[expect(
    clippy::cast_precision_loss,
    reason = "byte counts in a log are far below 2^53"
)]
fn ratio(part: i64, whole: i64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        100.0 * part as f64 / whole as f64
    }
}

fn header_lines(report: &Report, out: &mut Vec<String>) {
    let window = &report.window;
    let inputs = &report.inputs;
    out.push(format!(
        "NativeLink output provenance report (schema {})",
        report.provenance_schema
    ));
    if window.first.is_empty() {
        out.push("Window: no timestamped provenance events".to_string());
    } else {
        out.push(format!(
            "Window: {} .. {} ({})",
            window.first,
            window.last,
            format_duration(window.duration_seconds.unwrap_or(0.0))
        ));
    }
    out.push(format!(
        "Inputs: {} file(s), {} lines, {} provenance events, {} non-JSON, {} other JSON, \
         {} unknown schema, {} unknown kind",
        inputs.files.len(),
        inputs.lines,
        inputs.provenance_events,
        inputs.non_json_lines,
        inputs.non_provenance_lines,
        inputs.unknown_schema_events,
        inputs.unknown_event_kinds
    ));
    out.push(format!(
        "Events: {}",
        fmt_counter(
            inputs
                .events_by_kind
                .iter()
                .filter(|(_, count)| *count != 0)
        )
    ));
    if inputs.seeded_digests > 0 {
        out.push(format!(
            "Seeded digests (known before the window): {}",
            inputs.seeded_digests
        ));
    }
}

fn producer_lines(report: &Report, short: Shortener, out: &mut Vec<String>) {
    let producers = &report.producers;
    let duration = report.window.duration_seconds;
    out.push(String::new());
    out.push(format!(
        "1. Producers by first-observed output bytes ({} of {} shown)",
        producers.rows.len(),
        producers.producer_count
    ));
    out.push(format!(
        "   first observed: {} in {} digests{}; observed: {} in {} outputs",
        bytes(producers.first_observed_bytes),
        producers.first_observed_digests,
        per_day(producers.first_observed_bytes, duration),
        bytes(producers.observed_bytes),
        producers.observations
    ));
    let rows: Vec<Vec<String>> = producers
        .rows
        .iter()
        .map(|row| {
            vec![
                short.text(&row.target_id, 40),
                dash(&row.action_mnemonic),
                short.text(&row.path, 64),
                row.attempts.to_string(),
                row.distinct_action_digests.to_string(),
                row.distinct_input_root_digests.to_string(),
                row.distinct_command_digests.to_string(),
                row.distinct_output_digests.to_string(),
                bytes(row.first_observed_bytes),
                bytes(row.observed_bytes),
                fmt_map(&row.dispositions),
            ]
        })
        .collect();
    out.push(table(
        &[
            "target",
            "mnemonic",
            "path",
            "attempts",
            "actions",
            "inputs",
            "cmds",
            "digests",
            "first-observed",
            "observed",
            "uploads",
        ],
        &rows,
        &[3, 4, 5, 6, 7, 8, 9],
    ));
}

fn client_upload_lines(report: &Report, short: Shortener, out: &mut Vec<String>) {
    let uploads = &report.client_uploads;
    out.push(String::new());
    out.push(format!(
        "1b. Client uploads by target: cas_write (result=ok) grouped by request metadata \
         ({} of {} targets shown)",
        uploads
            .rows
            .iter()
            .filter(|row| row.target_id != NO_METADATA)
            .count(),
        uploads.target_count
    ));
    let show_worker = uploads.worker_events_present;
    let unseen = if show_worker {
        format!(
            "; {} from targets no worker attempt in this input produced (worker = no)",
            bytes(uploads.bytes_from_targets_no_worker_ran)
        )
    } else {
        String::new()
    };
    out.push(format!(
        "   {} uploaded; {} without metadata{unseen}",
        bytes(uploads.bytes),
        bytes(uploads.bytes_without_metadata)
    ));
    let mut headers = vec!["target", "mnemonic"];
    if show_worker {
        headers.push("worker");
    }
    headers.extend([
        "writes",
        "bytes",
        "digests",
        "first-write",
        "rewrite",
        "pre-window",
        "invocations",
    ]);
    let rows: Vec<Vec<String>> = uploads
        .rows
        .iter()
        .map(|row| {
            let mut cells = vec![short.text(&row.target_id, 40), dash(&row.action_mnemonic)];
            if show_worker {
                cells.push(
                    match row.seen_by_worker {
                        Some(true) => "yes",
                        Some(false) => "no",
                        None => "-",
                    }
                    .to_string(),
                );
            }
            cells.extend([
                row.writes.to_string(),
                bytes(row.bytes),
                row.distinct_digests.to_string(),
                bytes(row.first_write_bytes),
                bytes(row.rewrite_bytes),
                bytes(row.prewindow_rewrite_bytes),
                row.invocations.to_string(),
            ]);
            cells
        })
        .collect();
    let first_number = if show_worker { 3 } else { 2 };
    let right: Vec<usize> = (first_number..first_number + 7).collect();
    out.push(table(&headers, &rows, &right));
}

fn exact_identity_lines(report: &Report, short: Shortener, out: &mut Vec<String>) {
    let exact = &report.exact_identity;
    out.push(String::new());
    out.push(format!(
        "2. Exact identity: action digests with more than one manifest among complete \
         attempts ({} of {})",
        exact.actions_with_multiple_manifests, exact.actions_checked
    ));
    let rows: Vec<Vec<String>> = exact
        .rows
        .iter()
        .map(|row| {
            let shown: Vec<String> = row
                .differing_paths
                .iter()
                .take(3)
                .map(|path| short.text(path, 48))
                .collect();
            let more = if row.differing_paths.len() > 3 {
                " ..."
            } else {
                ""
            };
            vec![
                short.digest(&row.action_digest),
                short.text(&row.target_id, 40),
                dash(&row.action_mnemonic),
                row.complete_attempts.to_string(),
                row.distinct_fingerprints.to_string(),
                row.invocations.to_string(),
                bytes(row.varying_bytes),
                format!("{}: {}{more}", row.differing_paths.len(), shown.join(", ")),
            ]
        })
        .collect();
    out.push(table(
        &[
            "action_digest",
            "target",
            "mnemonic",
            "complete",
            "manifests",
            "invocations",
            "varying",
            "differing paths",
        ],
        &rows,
        &[3, 4, 5, 6],
    ));
}

fn churn_lines(report: &Report, short: Shortener, top_note: &str, out: &mut Vec<String>) {
    let churn = &report.churn;
    out.push(String::new());
    out.push(format!(
        "3. Logical producer churn: consecutive complete attempts per (target, mnemonic, \
         configuration, path) ({} producers compared)",
        churn.producers_compared
    ));
    let summary: Vec<Vec<String>> = ChurnClass::ALL
        .into_iter()
        .map(|class| {
            let data = churn.summary.get(class);
            vec![
                class.name().to_string(),
                data.pairs.to_string(),
                bytes(data.bytes),
                data.producers.to_string(),
            ]
        })
        .collect();
    out.push(table(
        &["class", "pairs", "bytes", "producers"],
        &summary,
        &[1, 2, 3],
    ));
    if !churn.declared_change_components.is_empty() {
        out.push(format!(
            "   declared changes by what moved: {}",
            fmt_map(&churn.declared_change_components)
        ));
    }
    out.push(format!("   Producers by churn bytes ({top_note}):"));
    let rows: Vec<Vec<String>> = churn
        .rows
        .iter()
        .map(|row| {
            let mut cells = vec![
                short.text(&row.target_id, 40),
                dash(&row.action_mnemonic),
                short.text(&row.configuration_id, 16),
                short.text(&row.path, 64),
                row.complete_attempts.to_string(),
            ];
            cells.extend(
                ChurnClass::ALL
                    .into_iter()
                    .map(|class| row.pairs.get(class).to_string()),
            );
            cells.push(bytes(row.churn_bytes));
            cells
        })
        .collect();
    out.push(table(
        &[
            "target",
            "mnemonic",
            "config",
            "path",
            "attempts",
            "dc_oc",
            "dc_os",
            "sa_oc",
            "stable",
            "churn bytes",
        ],
        &rows,
        &[4, 5, 6, 7, 8, 9],
    ));
}

fn novelty_lines(report: &Report, short: Shortener, out: &mut Vec<String>) {
    let novelty = &report.novelty;
    out.push(String::new());
    let seeded = if novelty.seeded_digests > 0 {
        format!(", seeded with {} digests", novelty.seeded_digests)
    } else {
        String::new()
    };
    out.push(format!(
        "4. Novelty per invocation, in order of first event{seeded}"
    ));
    let rows: Vec<Vec<String>> = novelty
        .runs
        .iter()
        .enumerate()
        .map(|(index, run)| {
            vec![
                (index + 1).to_string(),
                short.text(&run.invocation, 40),
                dash(&run.first_event),
                run.attempts.to_string(),
                run.distinct_digests.to_string(),
                bytes(run.run_bytes),
                run.new_digests.to_string(),
                bytes(run.new_bytes),
                bytes(run.cumulative_new_bytes),
            ]
        })
        .collect();
    out.push(table(
        &[
            "#",
            "invocation",
            "first event",
            "attempts",
            "digests",
            "run bytes",
            "new",
            "new bytes",
            "cumulative",
        ],
        &rows,
        &[0, 3, 4, 5, 6, 7, 8],
    ));
}

fn rewrite_lines(
    data: &RewriteSection,
    title: &str,
    group_headers: &[&str],
    ident: &str,
    context: (Shortener, Option<f64>, &str),
    out: &mut Vec<String>,
) {
    let (short, duration, top_note) = context;
    let totals = &data.totals;
    let versioning = totals.versioned_writes.is_some();
    // Without the field anywhere, "0 versioned" would read as "the account
    // keeps nothing", which these logs cannot say.
    let version_known = data.version_id_reported.unwrap_or(0) > 0;
    out.push(format!("   {title}:"));
    if totals.writes != 0 || totals.errors != 0 {
        let retained = match (versioning, version_known) {
            (true, true) => format!(
                ", retained by versioning {}",
                bytes(totals.retained_by_versioning_bytes.unwrap_or(0))
            ),
            (true, false) => {
                "; versioning unknown (no backend_write carries version_id)".to_string()
            }
            (false, _) => String::new(),
        };
        out.push(format!(
            "   total {}{}: first writes {}, rewrites {}, pre-window {}{retained}",
            bytes(totals.bytes),
            per_day(totals.bytes, duration),
            bytes(totals.first_write_bytes),
            bytes(totals.rewrite_bytes),
            bytes(totals.prewindow_rewrite_bytes)
        ));
    }
    let mut headers = group_headers.to_vec();
    headers.extend([
        "writes",
        "bytes",
        "first-write",
        "rewrite",
        "pre-window",
        "rewrite %",
        "errors",
        "error bytes",
    ]);
    if versioning {
        headers.extend(["versioned", "retained"]);
    }
    let rows: Vec<Vec<String>> = data
        .by_group
        .iter()
        .map(|row| {
            let mut cells: Vec<String> = row.group_columns().into_iter().map(dash).collect();
            cells.extend([
                row.writes.to_string(),
                bytes(row.bytes),
                bytes(row.first_write_bytes),
                bytes(row.rewrite_bytes),
                bytes(row.prewindow_rewrite_bytes),
                format!(
                    "{:.1}%",
                    ratio(row.rewrite_bytes + row.prewindow_rewrite_bytes, row.bytes)
                ),
                row.errors.to_string(),
                bytes(row.error_bytes),
            ]);
            if versioning && version_known {
                cells.push(row.versioned_writes.unwrap_or(0).to_string());
                cells.push(bytes(row.retained_by_versioning_bytes.unwrap_or(0)));
            } else if versioning {
                cells.extend(["-".to_string(), "-".to_string()]);
            }
            cells
        })
        .collect();
    let numbers = if versioning { 10 } else { 8 };
    let right: Vec<usize> = (group_headers.len()..group_headers.len() + numbers).collect();
    out.push(table(&headers, &rows, &right));
    out.push(format!("   Top rewritten {ident}s ({top_note}):"));
    let top: Vec<Vec<String>> = data
        .top_rewritten
        .iter()
        .map(|row| {
            let name = row.digest.as_ref().map_or_else(
                || short.key(row.key.as_deref().unwrap_or("")),
                |digest| short.digest(digest),
            );
            let via = row.sources.join(",");
            vec![
                name,
                bytes(row.size_bytes),
                row.writes.to_string(),
                bytes(row.rewrite_bytes),
                bytes(row.prewindow_rewrite_bytes),
                row.invocations.to_string(),
                dash(&via),
            ]
        })
        .collect();
    out.push(table(
        &[
            ident,
            "size",
            "writes",
            "rewrite",
            "pre-window",
            "invocations",
            "via",
        ],
        &top,
        &[1, 2, 3, 4, 5],
    ));
}

#[expect(
    clippy::cast_precision_loss,
    reason = "byte counts in a log are far below 2^53"
)]
fn amplification_lines(report: &Report, short: Shortener, top_note: &str, out: &mut Vec<String>) {
    let rewrites = &report.rewrites;
    let duration = report.window.duration_seconds;
    out.push(String::new());
    out.push("5. Rewrite amplification (result=ok writes; errors counted separately)".to_string());
    rewrite_lines(
        &rewrites.cas_write,
        "CAS writes (cas_write) by source",
        &["source"],
        "digest",
        (short, duration, top_note),
        out,
    );
    rewrite_lines(
        &rewrites.backend_write,
        "Backend writes (backend_write) by store and key prefix",
        &["store", "prefix"],
        "key",
        (short, duration, top_note),
        out,
    );
    let cas_bytes = rewrites.cas_write.totals.bytes;
    let backend_bytes = rewrites.backend_write.totals.bytes;
    if cas_bytes != 0 && backend_bytes != 0 {
        out.push(format!(
            "   backend bytes / CAS bytes: {:.2} (above 1 means the backend is written more \
             than the CAS service accepted)",
            backend_bytes as f64 / cas_bytes as f64
        ));
    }
}

fn has_health_lines(report: &Report, short: Shortener, out: &mut Vec<String>) {
    let health = &report.has_health;
    out.push(String::new());
    out.push(format!(
        "6. has() health ({} upload decisions, {} uploaded)",
        health.upload_events,
        bytes(health.uploaded_bytes)
    ));
    let rows: Vec<Vec<String>> = health
        .by_has_result
        .iter()
        .map(|row| {
            vec![
                row.has_result.clone(),
                row.events.to_string(),
                bytes(row.bytes),
                row.uploaded.to_string(),
                bytes(row.uploaded_bytes),
                fmt_map(&row.dispositions),
                row.has_elapsed_ms_p50.to_string(),
                row.has_elapsed_ms_p95.to_string(),
                row.has_elapsed_ms_max.to_string(),
            ]
        })
        .collect();
    out.push(table(
        &[
            "has_result",
            "events",
            "bytes",
            "uploaded",
            "uploaded bytes",
            "dispositions",
            "p50 ms",
            "p95 ms",
            "max ms",
        ],
        &rows,
        &[1, 2, 3, 4, 6, 7, 8],
    ));
    if !health.uploads_of_already_stored_digests.is_empty() {
        let parts: Vec<String> = health
            .uploads_of_already_stored_digests
            .iter()
            .map(|(name, data)| format!("after {name}: {} ({})", data.uploads, bytes(data.bytes)))
            .collect();
        out.push(format!(
            "   uploads of digests already stored earlier in the window: {}",
            parts.join(", ")
        ));
    }
    if !health.top_has_errors.is_empty() {
        out.push("   has() errors:".to_string());
        for row in &health.top_has_errors {
            out.push(format!(
                "     {:>6}  {}",
                row.count,
                short.text(&row.has_error, 100)
            ));
        }
    }
    let fm = &health.find_missing;
    if fm.requests != 0 {
        out.push(format!(
            "   client FindMissingBlobs: {} requests, {} digests / {} asked, {} / {} missing, \
             {} errors",
            fm.requests,
            fm.requested_count,
            bytes(fm.requested_bytes),
            fm.missing_count,
            bytes(fm.missing_bytes),
            fm.errors
        ));
    }
}

fn coverage_lines(report: &Report, short: Shortener, out: &mut Vec<String>) {
    let cov = &report.coverage;
    out.push(String::new());
    out.push("7. Coverage".to_string());
    out.push(format!(
        "   attempts: {} seen, {} started, {} finished",
        cov.attempts_seen, cov.attempts_started, cov.attempts_finished
    ));
    out.push(format!(
        "   attempt_started with no attempt_finished: {}",
        cov.started_without_finished
    ));
    for example in &cov.started_without_finished_examples {
        out.push(format!(
            "     {}  {}  {}",
            example.attempt_id,
            dash(&example.started),
            short.text(&example.target_id, 60)
        ));
    }
    out.push(format!(
        "   attempt_finished with no attempt_started: {}",
        cov.finished_without_started
    ));
    let by_outcome = |count: i64, counts: &BTreeMap<String, i64>| {
        if count == 0 {
            String::new()
        } else {
            format!(" ({})", fmt_map(counts))
        }
    };
    out.push(format!(
        "   attempts with outputs_complete=false: {}{}",
        cov.outputs_incomplete,
        by_outcome(cov.outputs_incomplete, &cov.outputs_incomplete_by_outcome)
    ));
    out.push(format!(
        "   outputs_complete=true but outcome not completed (left out of 2 and 3): {}{}",
        cov.complete_but_not_completed,
        by_outcome(
            cov.complete_but_not_completed,
            &cov.complete_but_not_completed_by_outcome
        )
    ));
    out.push(format!(
        "   complete attempts whose observed manifest entries != output_count: {}",
        cov.manifest_count_mismatches
    ));
    out.push(format!(
        "   output_observed with no attempt_started: {}",
        cov.outputs_without_attempt_started
    ));
    out.push(format!(
        "   output_observed with no target/mnemonic/invocation: {}",
        cov.outputs_without_metadata
    ));
    out.push(format!(
        "   events with an unknown schema: {}",
        if cov.unknown_schema.is_empty() {
            "0".to_string()
        } else {
            fmt_map(&cov.unknown_schema)
        }
    ));
    if !cov.unknown_events.is_empty() {
        out.push(format!(
            "   unknown event kinds: {}",
            fmt_map(&cov.unknown_events)
        ));
    }
    out.push(format!(
        "   events with no timestamp: {}",
        cov.events_without_timestamp
    ));
}

fn dash(text: &str) -> String {
    if text.is_empty() {
        "-".to_string()
    } else {
        text.to_string()
    }
}

/// The whole text report, ending in a newline.
pub(crate) fn render_text(report: &Report, top: i64, wide: bool) -> String {
    let short = Shortener { wide };
    let top_note = if top <= 0 {
        "all".to_string()
    } else {
        format!("top {top}")
    };
    let mut out = Vec::new();
    header_lines(report, &mut out);
    producer_lines(report, short, &mut out);
    client_upload_lines(report, short, &mut out);
    exact_identity_lines(report, short, &mut out);
    churn_lines(report, short, &top_note, &mut out);
    novelty_lines(report, short, &mut out);
    amplification_lines(report, short, &top_note, &mut out);
    has_health_lines(report, short, &mut out);
    coverage_lines(report, short, &mut out);
    let mut text = out.join("\n");
    text.push('\n');
    text
}
