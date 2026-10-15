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

//! The analyses. Each returns a serializable section of the report; field
//! order is the order of the keys in the JSON document.

use core::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use serde::Serialize;

use crate::collect::{
    Attempt, Collector, KNOWN_EVENTS, Meta, Order, Output, SUPPORTED_SCHEMA, Text, Write,
};
use crate::parse::{NO_TIMESTAMP, format_timestamp};

/// The output kinds that make up an attempt's manifest. stdout, stderr and the
/// Tree/Directory index of an output directory are uploaded too, but they are
/// not part of the manifest fingerprint, so churn and exact-identity
/// comparisons leave them out.
const MANIFEST_KINDS: [&str; 3] = ["file", "directory_file", "symlink"];

pub(crate) const NO_INVOCATION: &str = "(none)";
pub(crate) const NO_METADATA: &str = "(no metadata)";

fn is_manifest(output: &Output) -> bool {
    MANIFEST_KINDS.contains(&&*output.kind)
}

// --------------------------------------------------------------------------
// The report.

#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub(crate) provenance_schema: i64,
    pub(crate) window: Window,
    pub(crate) inputs: Inputs,
    pub(crate) producers: Producers,
    pub(crate) client_uploads: ClientUploads,
    pub(crate) exact_identity: ExactIdentity,
    pub(crate) churn: Churn,
    pub(crate) novelty: Novelty,
    pub(crate) rewrites: Rewrites,
    pub(crate) has_health: HasHealth,
    pub(crate) coverage: Coverage,
}

#[derive(Debug, Serialize)]
pub(crate) struct Window {
    pub(crate) first: String,
    pub(crate) last: String,
    pub(crate) first_unix_ns: Option<i64>,
    pub(crate) last_unix_ns: Option<i64>,
    pub(crate) duration_seconds: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct Inputs {
    pub(crate) files: Vec<String>,
    pub(crate) lines: i64,
    pub(crate) provenance_events: i64,
    pub(crate) non_json_lines: i64,
    pub(crate) non_provenance_lines: i64,
    pub(crate) unknown_schema_events: i64,
    pub(crate) unknown_event_kinds: i64,
    pub(crate) events_by_kind: EventsByKind,
    pub(crate) seeded_digests: usize,
}

/// Event counts, in the order an output meets the events.
#[derive(Debug, Serialize)]
pub(crate) struct EventsByKind {
    pub(crate) attempt_started: i64,
    pub(crate) output_observed: i64,
    pub(crate) output_upload: i64,
    pub(crate) attempt_finished: i64,
    pub(crate) cas_write: i64,
    pub(crate) find_missing: i64,
    pub(crate) backend_write: i64,
}

impl EventsByKind {
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&'static str, i64)> {
        KNOWN_EVENTS.into_iter().zip([
            self.attempt_started,
            self.output_observed,
            self.output_upload,
            self.attempt_finished,
            self.cas_write,
            self.find_missing,
            self.backend_write,
        ])
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Producers {
    pub(crate) producer_count: usize,
    pub(crate) first_observed_bytes: i64,
    pub(crate) first_observed_digests: i64,
    pub(crate) observed_bytes: i64,
    pub(crate) observations: usize,
    pub(crate) uploads_without_output_observed: i64,
    pub(crate) rows: Vec<ProducerRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ProducerRow {
    pub(crate) target_id: String,
    pub(crate) action_mnemonic: String,
    pub(crate) path: String,
    pub(crate) kinds: Vec<String>,
    pub(crate) attempts: usize,
    pub(crate) distinct_action_digests: usize,
    pub(crate) distinct_input_root_digests: usize,
    pub(crate) distinct_command_digests: usize,
    pub(crate) distinct_output_digests: usize,
    pub(crate) first_observed_bytes: i64,
    pub(crate) first_observed_digests: i64,
    pub(crate) observed_bytes: i64,
    pub(crate) observations: i64,
    pub(crate) dispositions: BTreeMap<String, i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ClientUploads {
    pub(crate) target_count: usize,
    pub(crate) bytes: i64,
    pub(crate) bytes_without_metadata: i64,
    pub(crate) worker_events_present: bool,
    pub(crate) bytes_from_targets_no_worker_ran: i64,
    pub(crate) rows: Vec<ClientRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ClientRow {
    pub(crate) target_id: String,
    pub(crate) action_mnemonic: String,
    pub(crate) seen_by_worker: Option<bool>,
    pub(crate) writes: i64,
    pub(crate) bytes: i64,
    pub(crate) distinct_digests: usize,
    pub(crate) first_write_bytes: i64,
    pub(crate) rewrite_bytes: i64,
    pub(crate) prewindow_rewrite_bytes: i64,
    pub(crate) invocations: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ExactIdentity {
    pub(crate) actions_checked: usize,
    pub(crate) actions_with_multiple_manifests: usize,
    pub(crate) rows: Vec<ExactRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ExactRow {
    pub(crate) action_digest: String,
    pub(crate) target_id: String,
    pub(crate) action_mnemonic: String,
    pub(crate) complete_attempts: usize,
    pub(crate) distinct_fingerprints: usize,
    pub(crate) invocations: usize,
    pub(crate) differing_paths: Vec<String>,
    pub(crate) varying_bytes: i64,
}

/// One value per churn class, in the order the report lists them.
#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct PerClass<T> {
    pub(crate) declared_change_output_change: T,
    pub(crate) declared_change_output_same: T,
    pub(crate) same_action_output_change: T,
    pub(crate) stable: T,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChurnClass {
    DeclaredChangeOutputChange,
    DeclaredChangeOutputSame,
    SameActionOutputChange,
    Stable,
}

impl ChurnClass {
    pub(crate) const ALL: [Self; 4] = [
        Self::DeclaredChangeOutputChange,
        Self::DeclaredChangeOutputSame,
        Self::SameActionOutputChange,
        Self::Stable,
    ];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::DeclaredChangeOutputChange => "declared_change_output_change",
            Self::DeclaredChangeOutputSame => "declared_change_output_same",
            Self::SameActionOutputChange => "same_action_output_change",
            Self::Stable => "stable",
        }
    }
}

impl<T> PerClass<T> {
    pub(crate) const fn get(&self, class: ChurnClass) -> &T {
        match class {
            ChurnClass::DeclaredChangeOutputChange => &self.declared_change_output_change,
            ChurnClass::DeclaredChangeOutputSame => &self.declared_change_output_same,
            ChurnClass::SameActionOutputChange => &self.same_action_output_change,
            ChurnClass::Stable => &self.stable,
        }
    }

    const fn get_mut(&mut self, class: ChurnClass) -> &mut T {
        match class {
            ChurnClass::DeclaredChangeOutputChange => &mut self.declared_change_output_change,
            ChurnClass::DeclaredChangeOutputSame => &mut self.declared_change_output_same,
            ChurnClass::SameActionOutputChange => &mut self.same_action_output_change,
            ChurnClass::Stable => &mut self.stable,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct ClassSummary {
    pub(crate) pairs: i64,
    pub(crate) bytes: i64,
    pub(crate) producers: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Churn {
    pub(crate) producers_compared: usize,
    pub(crate) summary: PerClass<ClassSummary>,
    pub(crate) declared_change_components: BTreeMap<String, i64>,
    pub(crate) rows: Vec<ChurnRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ChurnRow {
    pub(crate) target_id: String,
    pub(crate) action_mnemonic: String,
    pub(crate) configuration_id: String,
    pub(crate) path: String,
    pub(crate) complete_attempts: usize,
    pub(crate) invocations: usize,
    pub(crate) pairs: PerClass<i64>,
    pub(crate) bytes: PerClass<i64>,
    pub(crate) churn_bytes: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Novelty {
    pub(crate) runs: Vec<Run>,
    pub(crate) total_new_bytes: i64,
    pub(crate) seeded_digests: usize,
}

#[derive(Debug, Serialize)]
#[expect(
    clippy::struct_field_names,
    reason = "the field names are the keys of the JSON document"
)]
pub(crate) struct Run {
    pub(crate) invocation: String,
    pub(crate) first_event: String,
    pub(crate) attempts: usize,
    pub(crate) distinct_digests: usize,
    pub(crate) run_bytes: i64,
    pub(crate) new_digests: usize,
    pub(crate) new_bytes: i64,
    pub(crate) cumulative_new_bytes: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Rewrites {
    pub(crate) cas_write: RewriteSection,
    pub(crate) backend_write: RewriteSection,
}

#[derive(Debug, Serialize)]
pub(crate) struct RewriteSection {
    pub(crate) totals: WriteTotals,
    pub(crate) distinct_written: i64,
    pub(crate) by_group: Vec<GroupRow>,
    pub(crate) top_rewritten: Vec<TopRewritten>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) version_id_reported: Option<i64>,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct WriteTotals {
    pub(crate) writes: i64,
    pub(crate) bytes: i64,
    pub(crate) first_writes: i64,
    pub(crate) first_write_bytes: i64,
    pub(crate) rewrites: i64,
    pub(crate) rewrite_bytes: i64,
    pub(crate) prewindow_rewrites: i64,
    pub(crate) prewindow_rewrite_bytes: i64,
    pub(crate) errors: i64,
    pub(crate) error_bytes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) versioned_writes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retained_by_versioning_bytes: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GroupRow {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) store: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prefix: Option<String>,
    pub(crate) writes: i64,
    pub(crate) bytes: i64,
    pub(crate) first_writes: i64,
    pub(crate) first_write_bytes: i64,
    pub(crate) rewrites: i64,
    pub(crate) rewrite_bytes: i64,
    pub(crate) prewindow_rewrites: i64,
    pub(crate) prewindow_rewrite_bytes: i64,
    pub(crate) errors: i64,
    pub(crate) error_bytes: i64,
    pub(crate) error_codes: BTreeMap<String, i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) versioned_writes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) retained_by_versioning_bytes: Option<i64>,
}

impl GroupRow {
    /// The grouping columns, in order: the source, or the store and prefix.
    pub(crate) fn group_columns(&self) -> Vec<&str> {
        [&self.source, &self.store, &self.prefix]
            .into_iter()
            .filter_map(|column| column.as_deref())
            .collect()
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct TopRewritten {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) key: Option<String>,
    pub(crate) size_bytes: i64,
    pub(crate) writes: i64,
    pub(crate) rewrite_bytes: i64,
    pub(crate) prewindow_rewrite_bytes: i64,
    pub(crate) invocations: usize,
    pub(crate) sources: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct HasHealth {
    pub(crate) upload_events: usize,
    pub(crate) uploaded_bytes: i64,
    pub(crate) by_has_result: Vec<HasRow>,
    pub(crate) uploads_of_already_stored_digests: BTreeMap<String, Restored>,
    pub(crate) top_has_errors: Vec<HasError>,
    pub(crate) find_missing: FindMissing,
}

#[derive(Debug, Serialize)]
pub(crate) struct HasRow {
    pub(crate) has_result: String,
    pub(crate) events: i64,
    pub(crate) bytes: i64,
    pub(crate) uploaded: i64,
    pub(crate) uploaded_bytes: i64,
    pub(crate) dispositions: BTreeMap<String, i64>,
    pub(crate) has_elapsed_ms_p50: i64,
    pub(crate) has_elapsed_ms_p95: i64,
    pub(crate) has_elapsed_ms_max: i64,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub(crate) struct Restored {
    pub(crate) uploads: i64,
    pub(crate) bytes: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct HasError {
    pub(crate) has_error: String,
    pub(crate) count: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct FindMissing {
    pub(crate) requests: i64,
    pub(crate) errors: i64,
    pub(crate) requested_count: i64,
    pub(crate) requested_bytes: i64,
    pub(crate) missing_count: i64,
    pub(crate) missing_bytes: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Coverage {
    pub(crate) attempts_seen: usize,
    pub(crate) attempts_started: usize,
    pub(crate) attempts_finished: usize,
    pub(crate) started_without_finished: usize,
    pub(crate) started_without_finished_examples: Vec<UnfinishedAttempt>,
    pub(crate) finished_without_started: i64,
    pub(crate) outputs_incomplete: i64,
    pub(crate) outputs_incomplete_by_outcome: BTreeMap<String, i64>,
    pub(crate) complete_but_not_completed: i64,
    pub(crate) complete_but_not_completed_by_outcome: BTreeMap<String, i64>,
    pub(crate) manifest_count_mismatches: usize,
    pub(crate) manifest_count_mismatch_examples: Vec<String>,
    pub(crate) outputs_without_attempt_started: usize,
    pub(crate) outputs_without_metadata: usize,
    pub(crate) unknown_schema: BTreeMap<String, i64>,
    pub(crate) unknown_events: BTreeMap<String, i64>,
    pub(crate) events_without_timestamp: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct UnfinishedAttempt {
    pub(crate) attempt_id: String,
    pub(crate) operation_id: String,
    pub(crate) target_id: String,
    pub(crate) started: String,
}

// --------------------------------------------------------------------------
// Helpers.

/// How a write relates to earlier writes of the same digest or key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteLabel {
    FirstWrite,
    Rewrite,
    PrewindowRewrite,
    Failed,
}

/// Labels each write, in time order.
///
/// A successful write is a rewrite if the same digest or key was written
/// successfully earlier in the window, a pre-window rewrite if it is the first
/// in the window but the seed says the digest already existed, and a first
/// write otherwise.
fn classify_writes<'a>(
    writes: &'a [Write],
    seed: &BTreeSet<String>,
) -> Vec<(&'a Write, WriteLabel)> {
    let mut sorted: Vec<&Write> = writes.iter().collect();
    sorted.sort_by_key(|write| write.order);
    let mut written: HashSet<&str> = HashSet::new();
    sorted
        .into_iter()
        .map(|write| {
            let label = if !write.ok {
                WriteLabel::Failed
            } else if !written.insert(&write.ident) {
                WriteLabel::Rewrite
            } else if !write.seed_hash.is_empty() && seed.contains(&write.seed_hash) {
                WriteLabel::PrewindowRewrite
            } else {
                WriteLabel::FirstWrite
            };
            (write, label)
        })
        .collect()
}

fn limit<T>(mut rows: Vec<T>, top: i64) -> Vec<T> {
    if let Ok(top) = usize::try_from(top)
        && top > 0
    {
        rows.truncate(top);
    }
    rows
}

/// Nearest-rank percentile (`pct` in 1..=100); 0 for an empty list.
pub(crate) fn percentile(values: &[i64], pct: usize) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    let rank = ((pct * ordered.len()).div_ceil(100)).max(1);
    ordered[rank.min(ordered.len()) - 1]
}

fn output_identity(output: &Output) -> String {
    if &*output.kind == "symlink" {
        format!("symlink:{}", output.symlink_target)
    } else {
        output.hash.to_string()
    }
}

/// Whether (action, command, input root) differ in any component both know.
fn declared_differs(previous: [&str; 3], current: [&str; 3]) -> bool {
    previous
        .iter()
        .zip(current)
        .any(|(a, b)| !a.is_empty() && !b.is_empty() && *a != b)
}

/// Which declared inputs moved: `command`, `input_root`, both, or `other`.
///
/// The action digest also covers the platform, timeout and salt, so an action
/// change with the same command and input root is `other`.
fn declared_components(previous: [&str; 3], current: [&str; 3]) -> String {
    let mut changed = Vec::new();
    let mut known = true;
    for (name, (a, b)) in ["command", "input_root"]
        .into_iter()
        .zip(previous[1..].iter().zip(&current[1..]))
    {
        if a.is_empty() || b.is_empty() {
            known = false;
        } else if a != b {
            changed.push(name);
        }
    }
    if !changed.is_empty() {
        return changed.join("+");
    }
    if known { "other" } else { "unknown" }.to_string()
}

fn bump(counts: &mut BTreeMap<String, i64>, key: &str) {
    *counts.entry(key.to_string()).or_insert(0) += 1;
}

// --------------------------------------------------------------------------
// The analyses.

/// One pass over the collected events, borrowing them.
#[derive(Debug)]
pub(crate) struct Analysis<'a> {
    c: &'a Collector,
    seed: &'a BTreeSet<String>,
    top: i64,
    /// Every output, in time order.
    outputs: Vec<&'a Output>,
    /// Each attempt's outputs, in time order.
    by_attempt: Vec<Vec<&'a Output>>,
    empty_meta: Meta,
}

/// A producer: (target, mnemonic, output path).
type ProducerKey<'k> = (&'k str, &'k str, String);

/// One point in a logical producer's history, for churn.
#[derive(Debug)]
struct Point<'a> {
    order: Order,
    attempt: usize,
    declared: [&'a str; 3],
    identity: String,
    size: i64,
    invocation: &'a str,
}

#[derive(Debug, Default)]
struct ProducerAcc<'a> {
    attempts: HashSet<usize>,
    actions: HashSet<&'a str>,
    inputs: HashSet<&'a str>,
    commands: HashSet<&'a str>,
    digests: HashSet<&'a str>,
    kinds: BTreeSet<&'a str>,
    first_observed_bytes: i64,
    first_observed_digests: i64,
    observed_bytes: i64,
    observations: i64,
    dispositions: BTreeMap<String, i64>,
}

#[derive(Debug, Default)]
struct IdentAcc<'a> {
    display: &'a str,
    size_bytes: i64,
    writes: i64,
    rewrite_bytes: i64,
    prewindow_rewrite_bytes: i64,
    invocations: HashSet<&'a str>,
    groups: BTreeSet<String>,
}

impl<'a> Analysis<'a> {
    pub(crate) fn new(c: &'a Collector, seed: &'a BTreeSet<String>, top: i64) -> Self {
        let mut outputs: Vec<&Output> = c.outputs.iter().collect();
        outputs.sort_by_key(|output| output.order);
        let mut by_attempt = vec![Vec::new(); c.attempts.len()];
        for output in &outputs {
            by_attempt[output.attempt].push(*output);
        }
        let empty: Text = Rc::from("");
        Self {
            c,
            seed,
            top,
            outputs,
            by_attempt,
            empty_meta: Meta {
                target: empty.clone(),
                mnemonic: empty.clone(),
                config: empty.clone(),
                invocation: empty,
            },
        }
    }

    fn meta_of(&self, output: &'a Output) -> &Meta {
        if let Some(meta) = &output.meta {
            return meta;
        }
        self.c.attempts[output.attempt]
            .meta
            .as_deref()
            .unwrap_or(&self.empty_meta)
    }

    /// Attempts whose manifest can be trusted: the command ran to the end and
    /// every output was uploaded. A worker can report `outputs_complete` for an
    /// attempt it killed or timed out, if it uploaded what was there, so the
    /// outcome has to be `completed` too.
    fn complete_attempts(&self) -> impl Iterator<Item = (usize, &'a Attempt)> {
        self.c.attempts.iter().enumerate().filter(|(_, attempt)| {
            attempt.finished && attempt.outputs_complete && &*attempt.outcome == "completed"
        })
    }

    fn producer_path(output: &Output) -> String {
        if output.path.is_empty() {
            let kind = if output.kind.is_empty() {
                "unknown"
            } else {
                &output.kind
            };
            format!("<{kind}>")
        } else {
            output.path.to_string()
        }
    }

    // 1 ------------------------------------------------------------------

    fn producers(&self) -> Producers {
        let mut seen: HashSet<&str> = self.seed.iter().map(String::as_str).collect();
        let mut stats: HashMap<ProducerKey, ProducerAcc> = HashMap::new();
        let mut owner: HashMap<(usize, &str), (ProducerKey, bool)> = HashMap::new();
        let (mut total_first, mut total_first_digests, mut total_observed) = (0, 0, 0);
        for output in &self.outputs {
            let meta = self.meta_of(output);
            let key = (&*meta.target, &*meta.mnemonic, Self::producer_path(output));
            let row = stats.entry(key.clone()).or_default();
            let attempt = &self.c.attempts[output.attempt];
            row.attempts.insert(output.attempt);
            row.kinds.insert(&output.kind);
            let action = if output.action.is_empty() {
                &*attempt.action
            } else {
                &*output.action
            };
            if !action.is_empty() {
                row.actions.insert(action);
            }
            if !attempt.input_root.is_empty() {
                row.inputs.insert(&attempt.input_root);
            }
            if !attempt.command.is_empty() {
                row.commands.insert(&attempt.command);
            }
            row.observations += 1;
            row.observed_bytes += output.size;
            total_observed += output.size;
            if !output.hash.is_empty() {
                row.digests.insert(&output.hash);
                if seen.insert(&output.hash) {
                    row.first_observed_bytes += output.size;
                    row.first_observed_digests += 1;
                    total_first += output.size;
                    total_first_digests += 1;
                }
                // The upload event names a digest, not a path. Credit it to
                // the path the worker deduplicated it under
                // (`first_in_attempt`), or the earliest one if that flag is
                // missing everywhere.
                let upload_key = (output.attempt, &*output.hash);
                let replace = owner
                    .get(&upload_key)
                    .is_none_or(|(_, held_first)| output.first_in_attempt && !held_first);
                if replace {
                    owner.insert(upload_key, (key, output.first_in_attempt));
                }
            }
        }

        let mut unmatched_uploads = 0;
        for upload in &self.c.uploads {
            match owner.get(&(upload.attempt, &*upload.hash)) {
                Some((key, _)) => {
                    if let Some(row) = stats.get_mut(key) {
                        bump(&mut row.dispositions, &upload.disposition);
                    }
                }
                None => unmatched_uploads += 1,
            }
        }

        let mut rows: Vec<ProducerRow> = stats
            .into_iter()
            .map(|((target, mnemonic, path), row)| ProducerRow {
                target_id: target.to_string(),
                action_mnemonic: mnemonic.to_string(),
                path,
                kinds: row.kinds.iter().map(ToString::to_string).collect(),
                attempts: row.attempts.len(),
                distinct_action_digests: row.actions.len(),
                distinct_input_root_digests: row.inputs.len(),
                distinct_command_digests: row.commands.len(),
                distinct_output_digests: row.digests.len(),
                first_observed_bytes: row.first_observed_bytes,
                first_observed_digests: row.first_observed_digests,
                observed_bytes: row.observed_bytes,
                observations: row.observations,
                dispositions: row.dispositions,
            })
            .collect();
        rows.sort_by(|a, b| {
            (
                Reverse(a.first_observed_bytes),
                Reverse(a.observed_bytes),
                &a.target_id,
                &a.action_mnemonic,
                &a.path,
            )
                .cmp(&(
                    Reverse(b.first_observed_bytes),
                    Reverse(b.observed_bytes),
                    &b.target_id,
                    &b.action_mnemonic,
                    &b.path,
                ))
        });
        Producers {
            producer_count: rows.len(),
            first_observed_bytes: total_first,
            first_observed_digests: total_first_digests,
            observed_bytes: total_observed,
            observations: self.outputs.len(),
            uploads_without_output_observed: unmatched_uploads,
            rows: limit(rows, self.top),
        }
    }

    // 1b -----------------------------------------------------------------

    /// Successful `cas_write` events grouped by the target in their metadata.
    ///
    /// Bazel attaches `RequestMetadata` to its own uploads, so outputs of
    /// actions it ran locally are attributable here although no worker saw
    /// them. Uploads without metadata, such as a worker's own uploads through
    /// a `GrpcStore`, form one row.
    fn client_uploads(&self) -> ClientUploads {
        #[derive(Debug, Default)]
        struct Acc<'b> {
            writes: i64,
            bytes: i64,
            digests: HashSet<&'b str>,
            invocations: HashSet<&'b str>,
            first_write: i64,
            rewrite: i64,
            prewindow_rewrite: i64,
        }

        let worker_events = self.c.attempts.iter().any(|attempt| attempt.started);
        let executed: HashSet<(&str, &str)> = self
            .c
            .attempts
            .iter()
            .filter_map(|attempt| attempt.meta.as_deref())
            .map(|meta| (&*meta.target, &*meta.mnemonic))
            .collect();
        let mut rows: HashMap<(&str, &str), Acc> = HashMap::new();
        for (write, label) in classify_writes(&self.c.cas_writes, self.seed) {
            if label == WriteLabel::Failed {
                continue;
            }
            let key = if write.target.is_empty() {
                (NO_METADATA, "")
            } else {
                (&*write.target, &*write.mnemonic)
            };
            let row = rows.entry(key).or_default();
            row.writes += 1;
            row.bytes += write.size;
            match label {
                WriteLabel::Rewrite => row.rewrite += write.size,
                WriteLabel::PrewindowRewrite => row.prewindow_rewrite += write.size,
                WriteLabel::FirstWrite | WriteLabel::Failed => row.first_write += write.size,
            }
            row.digests.insert(&write.ident);
            if !write.invocation.is_empty() {
                row.invocations.insert(&write.invocation);
            }
        }

        let mut out: Vec<ClientRow> = rows
            .into_iter()
            .map(|((target, mnemonic), row)| ClientRow {
                target_id: target.to_string(),
                action_mnemonic: mnemonic.to_string(),
                seen_by_worker: (worker_events && target != NO_METADATA)
                    .then(|| executed.contains(&(target, mnemonic))),
                writes: row.writes,
                bytes: row.bytes,
                distinct_digests: row.digests.len(),
                first_write_bytes: row.first_write,
                rewrite_bytes: row.rewrite,
                prewindow_rewrite_bytes: row.prewindow_rewrite,
                invocations: row.invocations.len(),
            })
            .collect();
        out.sort_by(|a, b| {
            (
                Reverse(a.first_write_bytes),
                Reverse(a.bytes),
                &a.target_id,
                &a.action_mnemonic,
            )
                .cmp(&(
                    Reverse(b.first_write_bytes),
                    Reverse(b.bytes),
                    &b.target_id,
                    &b.action_mnemonic,
                ))
        });
        ClientUploads {
            target_count: out.iter().filter(|r| r.target_id != NO_METADATA).count(),
            bytes: out.iter().map(|r| r.bytes).sum(),
            bytes_without_metadata: out
                .iter()
                .filter(|r| r.target_id == NO_METADATA)
                .map(|r| r.bytes)
                .sum(),
            worker_events_present: worker_events,
            bytes_from_targets_no_worker_ran: out
                .iter()
                .filter(|r| r.seen_by_worker == Some(false))
                .map(|r| r.bytes)
                .sum(),
            rows: limit(out, self.top),
        }
    }

    // 2 ------------------------------------------------------------------

    fn exact_identity(&self) -> ExactIdentity {
        // Actions in the order their first complete attempt was seen.
        let mut order: Vec<&str> = Vec::new();
        let mut by_action: HashMap<&str, Vec<(usize, &Attempt)>> = HashMap::new();
        for (index, attempt) in self.complete_attempts() {
            if attempt.fingerprint.is_empty() || attempt.action.is_empty() {
                continue;
            }
            let entry = by_action.entry(&attempt.action).or_default();
            if entry.is_empty() {
                order.push(&attempt.action);
            }
            entry.push((index, attempt));
        }
        let mut rows = Vec::new();
        for action in &order {
            let attempts = &by_action[action];
            let fingerprints: HashSet<&str> =
                attempts.iter().map(|(_, a)| &*a.fingerprint).collect();
            if fingerprints.len() < 2 {
                continue;
            }
            // Per path, the first identity each attempt had there.
            let mut per_path: HashMap<&str, Vec<(usize, String, i64)>> = HashMap::new();
            for (index, _) in attempts {
                for output in &self.by_attempt[*index] {
                    if !is_manifest(output) {
                        continue;
                    }
                    let seen = per_path.entry(&output.path).or_default();
                    if !seen.iter().any(|(attempt, _, _)| attempt == index) {
                        seen.push((*index, output_identity(output), output.size));
                    }
                }
            }
            let mut differing = Vec::new();
            let mut varying_bytes = 0;
            for (path, seen) in &per_path {
                let identities: HashSet<&str> = seen.iter().map(|(_, id, _)| id.as_str()).collect();
                if identities.len() > 1 || seen.len() < attempts.len() {
                    differing.push((*path).to_string());
                    let mut sizes: HashMap<&str, i64> = HashMap::new();
                    for (_, identity, size) in seen {
                        sizes.insert(identity, *size);
                    }
                    varying_bytes += sizes.values().sum::<i64>();
                }
            }
            differing.sort();
            let meta = attempts
                .iter()
                .find_map(|(_, a)| a.meta.as_deref())
                .unwrap_or(&self.empty_meta);
            let invocations: HashSet<&str> = attempts
                .iter()
                .filter_map(|(_, a)| a.meta.as_deref())
                .map(|m| &*m.invocation)
                .filter(|invocation| !invocation.is_empty())
                .collect();
            rows.push(ExactRow {
                action_digest: (*action).to_string(),
                target_id: meta.target.to_string(),
                action_mnemonic: meta.mnemonic.to_string(),
                complete_attempts: attempts.len(),
                distinct_fingerprints: fingerprints.len(),
                invocations: invocations.len(),
                differing_paths: differing,
                varying_bytes,
            });
        }
        rows.sort_by(|a, b| {
            (
                Reverse(a.varying_bytes),
                Reverse(a.distinct_fingerprints),
                &a.action_digest,
            )
                .cmp(&(
                    Reverse(b.varying_bytes),
                    Reverse(b.distinct_fingerprints),
                    &b.action_digest,
                ))
        });
        ExactIdentity {
            actions_checked: by_action.len(),
            actions_with_multiple_manifests: rows.len(),
            rows: limit(rows, self.top),
        }
    }

    // 3 ------------------------------------------------------------------

    fn churn(&self) -> Churn {
        let mut series: HashMap<(&str, &str, &str, &str), Vec<Point>> = HashMap::new();
        for (index, attempt) in self.complete_attempts() {
            for output in &self.by_attempt[index] {
                if !is_manifest(output) {
                    continue;
                }
                let meta = self.meta_of(output);
                let key = (&*meta.target, &*meta.mnemonic, &*meta.config, &*output.path);
                let points = series.entry(key).or_default();
                if points.iter().any(|point| point.attempt == index) {
                    continue;
                }
                let action = if output.action.is_empty() {
                    &*attempt.action
                } else {
                    &*output.action
                };
                points.push(Point {
                    order: attempt.order(),
                    attempt: index,
                    declared: [action, &attempt.command, &attempt.input_root],
                    identity: output_identity(output),
                    size: output.size,
                    invocation: &meta.invocation,
                });
            }
        }

        let mut summary: PerClass<ClassSummary> = PerClass::default();
        let mut components = BTreeMap::new();
        let mut rows = Vec::new();
        for ((target, mnemonic, config, path), mut points) in series {
            points.sort_by(|a, b| {
                (a.order, &*self.c.attempts[a.attempt].id)
                    .cmp(&(b.order, &*self.c.attempts[b.attempt].id))
            });
            let mut pairs: PerClass<i64> = PerClass::default();
            let mut pair_bytes: PerClass<i64> = PerClass::default();
            let mut any_pair = false;
            for window in points.windows(2) {
                let (previous, current) = (&window[0], &window[1]);
                let declared = declared_differs(previous.declared, current.declared);
                let output_changed = previous.identity != current.identity;
                let class = match (declared, output_changed) {
                    (true, true) => ChurnClass::DeclaredChangeOutputChange,
                    (true, false) => ChurnClass::DeclaredChangeOutputSame,
                    (false, true) => ChurnClass::SameActionOutputChange,
                    (false, false) => ChurnClass::Stable,
                };
                any_pair = true;
                *pairs.get_mut(class) += 1;
                *pair_bytes.get_mut(class) += current.size;
                if declared {
                    bump(
                        &mut components,
                        &declared_components(previous.declared, current.declared),
                    );
                }
            }
            if !any_pair {
                continue;
            }
            for class in ChurnClass::ALL {
                if *pairs.get(class) > 0 {
                    let entry = summary.get_mut(class);
                    entry.pairs += *pairs.get(class);
                    entry.bytes += *pair_bytes.get(class);
                    entry.producers += 1;
                }
            }
            let churn_bytes = ChurnClass::ALL
                .into_iter()
                .filter(|class| *class != ChurnClass::Stable)
                .map(|class| *pair_bytes.get(class))
                .sum();
            let invocations: HashSet<&str> = points
                .iter()
                .map(|point| point.invocation)
                .filter(|invocation| !invocation.is_empty())
                .collect();
            rows.push(ChurnRow {
                target_id: target.to_string(),
                action_mnemonic: mnemonic.to_string(),
                configuration_id: config.to_string(),
                path: path.to_string(),
                complete_attempts: points.len(),
                invocations: invocations.len(),
                pairs,
                bytes: pair_bytes,
                churn_bytes,
            });
        }
        let total_pairs = |row: &ChurnRow| -> i64 {
            ChurnClass::ALL
                .into_iter()
                .map(|class| *row.pairs.get(class))
                .sum()
        };
        rows.sort_by(|a, b| {
            (
                Reverse(a.churn_bytes),
                Reverse(total_pairs(a)),
                &a.target_id,
                &a.action_mnemonic,
                &a.configuration_id,
                &a.path,
            )
                .cmp(&(
                    Reverse(b.churn_bytes),
                    Reverse(total_pairs(b)),
                    &b.target_id,
                    &b.action_mnemonic,
                    &b.configuration_id,
                    &b.path,
                ))
        });
        let producers_compared = rows.len();
        let churning: Vec<ChurnRow> = rows
            .into_iter()
            .filter(|row| {
                row.churn_bytes > 0
                    || ChurnClass::ALL
                        .into_iter()
                        .any(|class| class != ChurnClass::Stable && *row.pairs.get(class) > 0)
            })
            .collect();
        Churn {
            producers_compared,
            summary,
            declared_change_components: components,
            rows: limit(churning, self.top),
        }
    }

    // 4 ------------------------------------------------------------------

    fn novelty(&self) -> Novelty {
        let mut first: HashMap<&str, Order> = HashMap::new();
        let mut attempts_of: HashMap<&str, HashSet<usize>> = HashMap::new();
        // Per invocation, each digest with the size of its first output.
        let mut digests_of: HashMap<&str, HashMap<&str, i64>> = HashMap::new();
        for (index, attempt) in self.c.attempts.iter().enumerate() {
            let meta = attempt.meta.as_deref().unwrap_or_else(|| {
                self.by_attempt[index]
                    .first()
                    .and_then(|output| output.meta.as_deref())
                    .unwrap_or(&self.empty_meta)
            });
            let invocation = if meta.invocation.is_empty() {
                NO_INVOCATION
            } else {
                &meta.invocation
            };
            attempts_of.entry(invocation).or_default().insert(index);
            let earliest = first.entry(invocation).or_insert(attempt.first_order);
            if attempt.first_order < *earliest {
                *earliest = attempt.first_order;
            }
        }
        for output in &self.outputs {
            let meta = self.meta_of(output);
            let invocation = if meta.invocation.is_empty() {
                NO_INVOCATION
            } else {
                &meta.invocation
            };
            let earliest = first.entry(invocation).or_insert(output.order);
            if output.order < *earliest {
                *earliest = output.order;
            }
            attempts_of
                .entry(invocation)
                .or_default()
                .insert(output.attempt);
            if !output.hash.is_empty() {
                digests_of
                    .entry(invocation)
                    .or_default()
                    .entry(&output.hash)
                    .or_insert(output.size);
            }
        }
        let mut invocations: Vec<&str> = first.keys().copied().collect();
        invocations.sort_by(|a, b| (first[a], a).cmp(&(first[b], b)));

        let mut seen: HashSet<&str> = self.seed.iter().map(String::as_str).collect();
        let mut cumulative = 0;
        let empty = HashMap::new();
        let mut runs = Vec::new();
        for invocation in invocations {
            let digests = digests_of.get(invocation).unwrap_or(&empty);
            let new: Vec<i64> = digests
                .iter()
                .filter(|(hash, _)| !seen.contains(*hash))
                .map(|(_, size)| *size)
                .collect();
            let new_bytes: i64 = new.iter().sum();
            cumulative += new_bytes;
            seen.extend(digests.keys().copied());
            let order = first[invocation].0;
            runs.push(Run {
                invocation: invocation.to_string(),
                first_event: format_timestamp((order != NO_TIMESTAMP).then_some(order)),
                attempts: attempts_of.get(invocation).map_or(0, HashSet::len),
                distinct_digests: digests.len(),
                run_bytes: digests.values().sum(),
                new_digests: new.len(),
                new_bytes,
                cumulative_new_bytes: cumulative,
            });
        }
        Novelty {
            runs,
            total_new_bytes: cumulative,
            seeded_digests: self.seed.len(),
        }
    }

    // 5 ------------------------------------------------------------------

    fn rewrites(&self) -> Rewrites {
        let mut backend =
            self.rewrite_section(&self.c.backend_writes, &["store", "prefix"], "key", true);
        backend.version_id_reported = Some(self.c.version_id_reported);
        Rewrites {
            cas_write: self.rewrite_section(&self.c.cas_writes, &["source"], "digest", false),
            backend_write: backend,
        }
    }

    fn rewrite_section(
        &self,
        writes: &'a [Write],
        group_names: &[&str],
        ident_name: &str,
        versioning: bool,
    ) -> RewriteSection {
        let mut distinct_written = 0;
        let mut groups: HashMap<&[Text], (WriteTotals, BTreeMap<String, i64>)> = HashMap::new();
        let mut idents: HashMap<&str, IdentAcc> = HashMap::new();
        for (write, label) in classify_writes(writes, self.seed) {
            let (group, error_codes) = groups.entry(&write.group).or_insert_with(|| {
                let mut totals = WriteTotals::default();
                if versioning {
                    totals.versioned_writes = Some(0);
                    totals.retained_by_versioning_bytes = Some(0);
                }
                (totals, BTreeMap::new())
            });
            if label == WriteLabel::Failed {
                group.errors += 1;
                group.error_bytes += write.size;
                let code = if write.error_code.is_empty() {
                    "(empty)"
                } else {
                    &write.error_code
                };
                bump(error_codes, code);
                continue;
            }
            group.writes += 1;
            group.bytes += write.size;
            let ident = idents.entry(&write.ident).or_insert_with(|| IdentAcc {
                display: &write.display,
                size_bytes: write.size,
                ..IdentAcc::default()
            });
            ident.writes += 1;
            ident.groups.insert(
                write
                    .group
                    .iter()
                    .filter(|part| !part.is_empty())
                    .map(|part| &**part)
                    .collect::<Vec<_>>()
                    .join("/"),
            );
            if !write.invocation.is_empty() {
                ident.invocations.insert(&write.invocation);
            }
            match label {
                WriteLabel::Rewrite => {
                    group.rewrites += 1;
                    group.rewrite_bytes += write.size;
                    ident.rewrite_bytes += write.size;
                }
                WriteLabel::PrewindowRewrite => {
                    distinct_written += 1;
                    group.prewindow_rewrites += 1;
                    group.prewindow_rewrite_bytes += write.size;
                    ident.prewindow_rewrite_bytes += write.size;
                }
                WriteLabel::FirstWrite | WriteLabel::Failed => {
                    distinct_written += 1;
                    group.first_writes += 1;
                    group.first_write_bytes += write.size;
                }
            }
            if versioning && write.versioned {
                // A version ID on an overwrite means the account kept the copy
                // it replaced. Its size is taken to be this write's, which
                // holds for a CAS key.
                group.versioned_writes = Some(group.versioned_writes.unwrap_or(0) + 1);
                if label != WriteLabel::FirstWrite {
                    group.retained_by_versioning_bytes =
                        Some(group.retained_by_versioning_bytes.unwrap_or(0) + write.size);
                }
            }
        }

        let mut sorted_groups: Vec<_> = groups.into_iter().collect();
        sorted_groups.sort_by(|(a_key, (a, _)), (b_key, (b, _))| {
            (Reverse(a.bytes), *a_key).cmp(&(Reverse(b.bytes), *b_key))
        });
        let mut totals = WriteTotals::default();
        if versioning {
            totals.versioned_writes = Some(0);
            totals.retained_by_versioning_bytes = Some(0);
        }
        let mut by_group = Vec::new();
        for (key, (group, error_codes)) in sorted_groups {
            totals.writes += group.writes;
            totals.bytes += group.bytes;
            totals.first_writes += group.first_writes;
            totals.first_write_bytes += group.first_write_bytes;
            totals.rewrites += group.rewrites;
            totals.rewrite_bytes += group.rewrite_bytes;
            totals.prewindow_rewrites += group.prewindow_rewrites;
            totals.prewindow_rewrite_bytes += group.prewindow_rewrite_bytes;
            totals.errors += group.errors;
            totals.error_bytes += group.error_bytes;
            if versioning {
                totals.versioned_writes = Some(
                    totals.versioned_writes.unwrap_or(0) + group.versioned_writes.unwrap_or(0),
                );
                totals.retained_by_versioning_bytes = Some(
                    totals.retained_by_versioning_bytes.unwrap_or(0)
                        + group.retained_by_versioning_bytes.unwrap_or(0),
                );
            }
            let column = |name: &str| {
                group_names
                    .iter()
                    .position(|n| *n == name)
                    .map(|i| key[i].to_string())
            };
            by_group.push(GroupRow {
                source: column("source"),
                store: column("store"),
                prefix: column("prefix"),
                writes: group.writes,
                bytes: group.bytes,
                first_writes: group.first_writes,
                first_write_bytes: group.first_write_bytes,
                rewrites: group.rewrites,
                rewrite_bytes: group.rewrite_bytes,
                prewindow_rewrites: group.prewindow_rewrites,
                prewindow_rewrite_bytes: group.prewindow_rewrite_bytes,
                errors: group.errors,
                error_bytes: group.error_bytes,
                error_codes,
                versioned_writes: group.versioned_writes,
                retained_by_versioning_bytes: group.retained_by_versioning_bytes,
            });
        }

        let by_digest = ident_name == "digest";
        let mut top: Vec<TopRewritten> = idents
            .into_values()
            .filter(|ident| ident.rewrite_bytes != 0 || ident.prewindow_rewrite_bytes != 0)
            .map(|ident| TopRewritten {
                digest: by_digest.then(|| ident.display.to_string()),
                key: (!by_digest).then(|| ident.display.to_string()),
                size_bytes: ident.size_bytes,
                writes: ident.writes,
                rewrite_bytes: ident.rewrite_bytes,
                prewindow_rewrite_bytes: ident.prewindow_rewrite_bytes,
                invocations: ident.invocations.len(),
                sources: ident.groups.into_iter().collect(),
            })
            .collect();
        let name = |row: &TopRewritten| {
            row.digest
                .clone()
                .or_else(|| row.key.clone())
                .unwrap_or_default()
        };
        top.sort_by(|a, b| {
            (
                Reverse(a.rewrite_bytes + a.prewindow_rewrite_bytes),
                Reverse(a.writes),
                name(a),
            )
                .cmp(&(
                    Reverse(b.rewrite_bytes + b.prewindow_rewrite_bytes),
                    Reverse(b.writes),
                    name(b),
                ))
        });
        RewriteSection {
            totals,
            distinct_written,
            by_group,
            top_rewritten: limit(top, self.top),
            version_id_reported: None,
        }
    }

    // 6 ------------------------------------------------------------------

    fn has_health(&self) -> HasHealth {
        #[derive(Debug, Default)]
        struct Acc {
            events: i64,
            bytes: i64,
            uploaded: i64,
            uploaded_bytes: i64,
            dispositions: BTreeMap<String, i64>,
            elapsed: Vec<i64>,
        }

        let mut rows: HashMap<&str, Acc> = HashMap::new();
        let mut stored: HashSet<&str> = HashSet::new();
        let mut restored: BTreeMap<String, Restored> = BTreeMap::new();
        let mut errors: HashMap<&str, i64> = HashMap::new();
        let mut uploads: Vec<_> = self.c.uploads.iter().collect();
        uploads.sort_by_key(|upload| upload.order);
        for upload in uploads {
            let row = rows.entry(&upload.has_result).or_default();
            row.events += 1;
            row.bytes += upload.size;
            bump(&mut row.dispositions, &upload.disposition);
            row.elapsed.push(upload.has_elapsed_ms);
            if !upload.has_error.is_empty() {
                *errors.entry(&upload.has_error).or_insert(0) += 1;
            }
            if &*upload.disposition == "uploaded" {
                row.uploaded += 1;
                row.uploaded_bytes += upload.size;
                if !upload.hash.is_empty() && stored.contains(&*upload.hash) {
                    let entry = restored.entry(upload.has_result.to_string()).or_default();
                    entry.uploads += 1;
                    entry.bytes += upload.size;
                }
            }
            if !upload.hash.is_empty()
                && matches!(&*upload.disposition, "uploaded" | "skipped_present")
            {
                stored.insert(&upload.hash);
            }
        }

        let mut sorted: Vec<_> = rows.into_iter().collect();
        sorted.sort_by(|(a_name, a), (b_name, b)| {
            (Reverse(a.bytes), a_name).cmp(&(Reverse(b.bytes), b_name))
        });
        let by_has_result: Vec<HasRow> = sorted
            .into_iter()
            .map(|(name, row)| HasRow {
                has_result: name.to_string(),
                events: row.events,
                bytes: row.bytes,
                uploaded: row.uploaded,
                uploaded_bytes: row.uploaded_bytes,
                dispositions: row.dispositions,
                has_elapsed_ms_p50: percentile(&row.elapsed, 50),
                has_elapsed_ms_p95: percentile(&row.elapsed, 95),
                has_elapsed_ms_max: row.elapsed.iter().copied().max().unwrap_or(0),
            })
            .collect();
        let mut error_rows: Vec<HasError> = errors
            .into_iter()
            .map(|(text, count)| HasError {
                has_error: text.to_string(),
                count,
            })
            .collect();
        error_rows.sort_by(|a, b| {
            (Reverse(a.count), &a.has_error).cmp(&(Reverse(b.count), &b.has_error))
        });
        let fm = self.c.find_missing;
        HasHealth {
            upload_events: self.c.uploads.len(),
            uploaded_bytes: by_has_result.iter().map(|row| row.uploaded_bytes).sum(),
            by_has_result,
            uploads_of_already_stored_digests: restored,
            top_has_errors: limit(error_rows, self.top),
            find_missing: FindMissing {
                requests: fm.requests,
                errors: fm.errors,
                requested_count: fm.requested_count,
                requested_bytes: fm.requested_bytes,
                missing_count: fm.missing_count,
                missing_bytes: fm.missing_bytes,
            },
        }
    }

    // 7 ------------------------------------------------------------------

    fn coverage(&self) -> Coverage {
        let mut unfinished: Vec<&Attempt> = Vec::new();
        let mut unstarted = 0;
        let mut incomplete = BTreeMap::new();
        let mut not_completed = BTreeMap::new();
        let mut count_mismatch: Vec<String> = Vec::new();
        for (index, attempt) in self.c.attempts.iter().enumerate() {
            if attempt.started && !attempt.finished {
                unfinished.push(attempt);
            }
            if attempt.finished && !attempt.started {
                unstarted += 1;
            }
            if attempt.finished && !attempt.outputs_complete {
                bump(&mut incomplete, &attempt.outcome);
            }
            if attempt.finished && attempt.outputs_complete && &*attempt.outcome != "completed" {
                bump(&mut not_completed, &attempt.outcome);
            }
            if attempt.finished && attempt.outputs_complete {
                let observed = self.by_attempt[index]
                    .iter()
                    .filter(|output| is_manifest(output))
                    .count();
                if i64::try_from(observed).ok() != Some(attempt.output_count) {
                    count_mismatch.push(attempt.id.to_string());
                }
            }
        }
        unfinished.sort_by_key(|attempt| attempt.order());
        let stats = &self.c.stats;
        let mismatches = count_mismatch.len();
        let mut mismatch_examples = limit(count_mismatch, self.top);
        mismatch_examples.sort();
        Coverage {
            attempts_seen: self.c.attempts.len(),
            attempts_started: self.c.attempts.iter().filter(|a| a.started).count(),
            attempts_finished: self.c.attempts.iter().filter(|a| a.finished).count(),
            started_without_finished: unfinished.len(),
            started_without_finished_examples: limit(unfinished, self.top)
                .into_iter()
                .map(|attempt| UnfinishedAttempt {
                    attempt_id: attempt.id.to_string(),
                    operation_id: attempt.operation_id.to_string(),
                    target_id: attempt
                        .meta
                        .as_ref()
                        .map_or_else(String::new, |meta| meta.target.to_string()),
                    started: format_timestamp(attempt.start_order.map(|order| order.0)),
                })
                .collect(),
            finished_without_started: unstarted,
            outputs_incomplete: incomplete.values().sum(),
            outputs_incomplete_by_outcome: incomplete,
            complete_but_not_completed: not_completed.values().sum(),
            complete_but_not_completed_by_outcome: not_completed,
            manifest_count_mismatches: mismatches,
            manifest_count_mismatch_examples: mismatch_examples,
            outputs_without_attempt_started: self
                .outputs
                .iter()
                .filter(|output| !self.c.attempts[output.attempt].started)
                .count(),
            outputs_without_metadata: self
                .outputs
                .iter()
                .filter(|output| *self.meta_of(output) == self.empty_meta)
                .count(),
            unknown_schema: stats.unknown_schema.clone(),
            unknown_events: stats.unknown_events.clone(),
            events_without_timestamp: stats.missing_timestamp,
        }
    }

    // -------------------------------------------------------------------

    pub(crate) fn report(&self) -> Report {
        let c = self.c;
        let duration = match (c.first_ts, c.last_ts) {
            (Some(first), Some(last)) => Some((last - first) as f64 / 1e9),
            _ => None,
        };
        let stats = &c.stats;
        let count = |kind: &str| stats.events.get(kind).copied().unwrap_or(0);
        Report {
            provenance_schema: SUPPORTED_SCHEMA,
            window: Window {
                first: format_timestamp(c.first_ts),
                last: format_timestamp(c.last_ts),
                first_unix_ns: c.first_ts,
                last_unix_ns: c.last_ts,
                duration_seconds: duration,
            },
            inputs: Inputs {
                files: stats.files.clone(),
                lines: stats.lines,
                provenance_events: stats.provenance,
                non_json_lines: stats.non_json,
                non_provenance_lines: stats.non_provenance,
                unknown_schema_events: stats.unknown_schema.values().sum(),
                unknown_event_kinds: stats.unknown_events.values().sum(),
                events_by_kind: EventsByKind {
                    attempt_started: count("attempt_started"),
                    output_observed: count("output_observed"),
                    output_upload: count("output_upload"),
                    attempt_finished: count("attempt_finished"),
                    cas_write: count("cas_write"),
                    find_missing: count("find_missing"),
                    backend_write: count("backend_write"),
                },
                seeded_digests: self.seed.len(),
            },
            producers: self.producers(),
            client_uploads: self.client_uploads(),
            exact_identity: self.exact_identity(),
            churn: self.churn(),
            novelty: self.novelty(),
            rewrites: self.rewrites(),
            has_health: self.has_health(),
            coverage: self.coverage(),
        }
    }
}
