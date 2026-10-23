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

//! Ordered worker events joined to HTTP requests by SDK-carried identity.
//!
//! The witness is a failed send to a known broken writer followed by another
//! outer download of that read. Neither final error text nor cross-process
//! clock comparisons establish that ordering. Requests belonging to an older
//! download remain in that attempt, even when SDK tasks outlive the read.
//! The sent counter measures bytes accepted by the writer channel, not bytes
//! received by its downstream consumer.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tracing::field::{Field, Visit};
use tracing::{Event as TracingEvent, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

const TARGET: &str = "nativelink_store::azure_blob_store::download";

struct Capture {
    writer: BufWriter<File>,
    started: Instant,
    sequence: u64,
    error: Option<String>,
    finished: bool,
}

impl Capture {
    fn write(&mut self, mut fields: Map<String, Value>) {
        self.sequence += 1;
        fields.insert("sequence".into(), self.sequence.into());
        fields.insert(
            "elapsed_s".into(),
            self.started.elapsed().as_secs_f64().into(),
        );
        let result = serde_json::to_writer(&mut self.writer, &fields)
            .map_err(io::Error::other)
            .and_then(|()| self.writer.write_all(b"\n"));
        if let Err(error) = result {
            self.error.get_or_insert_with(|| error.to_string());
        }
    }
}

pub(crate) struct CaptureLayer(Arc<Mutex<Capture>>);
pub(crate) struct CaptureHandle(Arc<Mutex<Capture>>);

/// A fresh file avoids confusing two worker process generations.
pub(crate) fn capture(path: &Path) -> io::Result<(CaptureLayer, CaptureHandle)> {
    let writer = BufWriter::new(OpenOptions::new().write(true).create_new(true).open(path)?);
    let shared = Arc::new(Mutex::new(Capture {
        writer,
        started: Instant::now(),
        sequence: 0,
        error: None,
        finished: false,
    }));
    Ok((CaptureLayer(Arc::clone(&shared)), CaptureHandle(shared)))
}

impl CaptureHandle {
    /// Called after the worker drains. The marker closes event coverage, not
    /// the SDK's HTTP tasks; the account ledger measures those separately.
    pub(crate) fn finish(self) -> io::Result<()> {
        let mut state = self.0.lock().map_err(|e| io::Error::other(e.to_string()))?;
        let mut footer = Map::new();
        footer.insert("event".into(), "trace_finished".into());
        footer.insert("capture_error".into(), json!(state.error));
        state.write(footer);
        state.finished = true;
        if let Err(error) = state.writer.flush() {
            state.error.get_or_insert_with(|| error.to_string());
        }
        match &state.error {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
}

#[derive(Default)]
struct Fields(Map<String, Value>);

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &TracingEvent<'_>, _context: Context<'_, S>) {
        if event.metadata().target() != TARGET {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        // A poisoned mutex also makes finish fail. Do not let a diagnostic
        // layer panic inside the production operation being observed.
        if let Ok(mut state) = self.0.lock() {
            if state.finished {
                state
                    .error
                    .get_or_insert_with(|| "event after trace_finished".into());
            }
            state.write(fields.0);
            if state.finished {
                // Preserve late events so analysis rejects the early footer.
                if let Err(error) = state.writer.flush() {
                    state.error.get_or_insert_with(|| error.to_string());
                }
            }
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Event {
    sequence: u64,
    elapsed_s: f64,
    #[serde(rename = "event")]
    kind: String,
    read_id: u64,
    attempt: u64,
    sent: u64,
    writer_broken: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    blob: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
}

#[derive(Serialize)]
struct Attempt {
    started: Event,
    failed_sends: Vec<Event>,
    after_failed_send_sequence: Option<u64>,
    http_requests: Vec<Value>,
    body_bytes: u64,
}

#[derive(Serialize)]
struct Read {
    started: Option<Event>,
    finished: Option<Event>,
    first_failed_send: Option<Event>,
    attempts: BTreeMap<u64, Attempt>,
    last_sent: u64,
}

impl Read {
    const fn new() -> Self {
        Self {
            started: None,
            finished: None,
            first_failed_send: None,
            attempts: BTreeMap::new(),
            last_sent: 0,
        }
    }

    fn observe(&mut self, event: Event, errors: &mut Vec<String>) {
        let prefix = format!("read {} event {}", event.read_id, event.sequence);
        if self.finished.is_some() {
            errors.push(format!("{prefix}: event after read_finished"));
        }
        if event.sent < self.last_sent {
            errors.push(format!("{prefix}: writer byte counter decreased"));
        }
        if self
            .first_failed_send
            .as_ref()
            .is_some_and(|failed| event.sent != failed.sent)
        {
            errors.push(format!(
                "{prefix}: writer bytes changed after terminal send failure"
            ));
        }
        if self.first_failed_send.is_some() && !event.writer_broken {
            errors.push(format!("{prefix}: broken destination became writable"));
        }
        self.last_sent = event.sent;
        match event.kind.as_str() {
            "read_started" => {
                if self.started.is_some()
                    || !self.attempts.is_empty()
                    || event.attempt != 0
                    || event.blob.is_none()
                {
                    errors.push(format!("{prefix}: invalid or duplicate read_started"));
                }
                self.started.get_or_insert(event);
            }
            "attempt_started" => {
                let expected = self
                    .attempts
                    .last_key_value()
                    .map_or(Some(1), |(id, _)| id.checked_add(1));
                if self.started.is_none() || Some(event.attempt) != expected {
                    errors.push(format!(
                        "{prefix}: expected attempt {expected:?} after read_started"
                    ));
                }
                if event.writer_broken && self.first_failed_send.is_none() {
                    errors.push(format!(
                        "{prefix}: destination broke without observed failed send"
                    ));
                }
                let attempt = Attempt {
                    after_failed_send_sequence: self.first_failed_send.as_ref().map(|e| e.sequence),
                    started: event,
                    failed_sends: Vec::new(),
                    http_requests: Vec::new(),
                    body_bytes: 0,
                };
                match self.attempts.entry(attempt.started.attempt) {
                    Entry::Occupied(_) => {
                        errors.push(format!("{prefix}: duplicate attempt_started"));
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(attempt);
                    }
                }
            }
            "send_failed" => {
                if !matches!(event.stage.as_deref(), Some("data" | "eof")) {
                    errors.push(format!("{prefix}: unknown failed-send stage"));
                }
                if self.attempts.last_key_value().map(|(id, _)| *id) != Some(event.attempt) {
                    errors.push(format!("{prefix}: failed send outside current attempt"));
                }
                // Healthy EOF also leaves is_pipe_broken() true. Only a failed
                // send plus this flag establishes the terminal destination.
                if event.writer_broken {
                    self.first_failed_send.get_or_insert_with(|| event.clone());
                }
                if let Some(attempt) = self.attempts.get_mut(&event.attempt) {
                    attempt.failed_sends.push(event);
                }
            }
            "read_finished" => {
                if self.first_failed_send.is_some() && event.outcome.as_deref() != Some("error") {
                    errors.push(format!("{prefix}: terminal send failure was not returned"));
                }
                let last = self.attempts.last_key_value().map_or(0, |(id, _)| *id);
                if self.started.is_none()
                    || event.attempt != last
                    || !matches!(event.outcome.as_deref(), Some("ok" | "error"))
                    || event.error_code.is_none()
                {
                    errors.push(format!("{prefix}: invalid read_finished"));
                }
                if event.outcome.as_deref() == Some("error")
                    && event.writer_broken
                    && self.first_failed_send.is_none()
                {
                    errors.push(format!(
                        "{prefix}: error with broken writer but no observed failed send"
                    ));
                }
                self.finished.get_or_insert(event);
            }
            _ => errors.push(format!("{prefix}: unknown event {}", event.kind)),
        }
    }
}

fn load_trace(path: &Path, errors: &mut Vec<String>) -> BTreeMap<u64, Read> {
    let mut reads = BTreeMap::new();
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) => {
            errors.push(format!("reading {}: {error}", path.display()));
            return reads;
        }
    };
    if let Err(error) = std::str::from_utf8(&contents) {
        errors.push(format!("trace contains invalid UTF-8: {error}"));
    }
    let contents = String::from_utf8_lossy(&contents);
    let mut sequence = 0_u64;
    let mut elapsed = 0.0;
    let mut footer = false;
    for (line, contents) in contents.lines().enumerate() {
        let prefix = format!("trace line {}", line + 1);
        if footer {
            errors.push(format!("{prefix}: record after trace_finished"));
        }
        let value: Value = match serde_json::from_str(contents) {
            Ok(value) => value,
            Err(error) => {
                errors.push(format!("{prefix}: {error}"));
                continue;
            }
        };
        if value["sequence"].as_u64().is_none()
            || value["sequence"].as_u64() != sequence.checked_add(1)
        {
            errors.push(format!("{prefix}: missing or nonconsecutive sequence"));
        }
        sequence = value["sequence"].as_u64().unwrap_or(sequence);
        match value["elapsed_s"].as_f64() {
            Some(time) if time.is_finite() && time >= elapsed => elapsed = time,
            _ => errors.push(format!("{prefix}: missing or decreasing elapsed_s")),
        }
        if value["event"] == "trace_finished" {
            footer = true;
            if value.get("capture_error") != Some(&Value::Null) {
                errors.push(format!("{prefix}: missing or non-null capture_error"));
            }
            continue;
        }
        match serde_json::from_value::<Event>(value) {
            Ok(event) => reads
                .entry(event.read_id)
                .or_insert_with(Read::new)
                .observe(event, errors),
            Err(error) => errors.push(format!("{prefix}: {error}")),
        }
    }
    if !footer {
        errors.push("missing trace_finished marker".into());
    }
    for (id, read) in &reads {
        if read.started.is_none() || read.finished.is_none() {
            errors.push(format!("read {id}: incomplete lifecycle"));
        }
    }
    reads
}

#[derive(Deserialize)]
struct HttpIdentity {
    request_id: u64,
    read_id: u64,
    attempt: u64,
    sent: u64,
    writer_broken: bool,
    bytes: u64,
    blob: String,
}

fn attach_request(reads: &mut BTreeMap<u64, Read>, value: &Value) -> Result<(), String> {
    let identity: HttpIdentity =
        serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    let read = reads.get_mut(&identity.read_id).ok_or("unknown read_id")?;
    if read
        .started
        .as_ref()
        .and_then(|started| started.blob.as_deref())
        != Some(identity.blob.as_str())
    {
        return Err("HTTP blob differs from read_started".into());
    }
    let attempt = read
        .attempts
        .get_mut(&identity.attempt)
        .ok_or("unknown attempt")?;
    if identity.sent != attempt.started.sent
        || identity.writer_broken != attempt.started.writer_broken
    {
        return Err("SDK identity differs from attempt_started snapshot".into());
    }
    // No timestamp restriction: the SDK can keep an old request alive after
    // read_finished, and server and worker clocks are different boundaries.
    attempt.body_bytes = attempt
        .body_bytes
        .checked_add(identity.bytes)
        .ok_or("body byte overflow")?;
    attempt.http_requests.push(value.clone());
    Ok(())
}

/// Reads are the account's GET ledger for exactly one worker process. Missing
/// identities and unfinished traces stay visible and make `complete` false.
pub(crate) fn analyze(path: &Path, http_reads: &[Value]) -> Value {
    let mut errors = Vec::new();
    let mut reads = load_trace(path, &mut errors);
    let mut request_ids = BTreeSet::new();
    let mut uncorrelated = Vec::new();
    for request in http_reads {
        let parsed = serde_json::from_value::<HttpIdentity>(request.clone());
        let result = match parsed {
            Ok(identity) if !request_ids.insert(identity.request_id) => {
                Err("duplicate request_id".into())
            }
            Ok(_) => attach_request(&mut reads, request),
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = result {
            errors.push(format!("HTTP request {}: {error}", request["request_id"]));
            uncorrelated.push(request.clone());
        }
    }
    let new_attempts = reads
        .values()
        .flat_map(|r| r.attempts.values())
        .filter(|a| a.after_failed_send_sequence.is_some())
        .count();
    let failed_reads = reads
        .values()
        .filter(|r| r.first_failed_send.is_some())
        .count();
    json!({
        "complete": errors.is_empty(),
        "errors": errors,
        "failed_destination_reads": failed_reads,
        "new_attempts_after_failed_send": new_attempts,
        "reads": reads,
        "uncorrelated_requests": uncorrelated,
        "byte_boundary": "successful response payload yielded by account Body to Hyper, grouped by exact SDK outer attempt; not confirmed receipt or billed egress",
        "ordering_boundary": "worker capture sequence; server and worker elapsed times are not compared",
    })
}

/// Compact causal chain, with earlier SDK attempts kept separate from new ones.
pub(crate) fn explain(report: &Value) -> String {
    let mut lines = vec![format!(
        "complete={} failed_destination_reads={} new_attempts_after_failed_send={}",
        report["complete"],
        report["failed_destination_reads"],
        report["new_attempts_after_failed_send"],
    )];
    if let Some(reads) = report["reads"].as_object() {
        for (id, read) in reads {
            if read["first_failed_send"].is_null() {
                continue;
            }
            let failed = &read["first_failed_send"];
            let mut first_forbidden = None;
            let mut requests = 0;
            let mut bytes = 0_u128;
            let mut earlier_bytes = 0_u128;
            if let Some(attempts) = read["attempts"].as_object() {
                for attempt in attempts.values() {
                    if attempt["after_failed_send_sequence"].is_null() {
                        earlier_bytes += u128::from(attempt["body_bytes"].as_u64().unwrap_or(0));
                    } else {
                        let id = attempt["started"]["attempt"].as_u64().unwrap_or(0);
                        first_forbidden = Some(first_forbidden.map_or(id, |old: u64| old.min(id)));
                        requests += attempt["http_requests"].as_array().map_or(0, Vec::len);
                        bytes += u128::from(attempt["body_bytes"].as_u64().unwrap_or(0));
                    }
                }
            }
            let delta = read["finished"]["sent"]
                .as_u64()
                .zip(failed["sent"].as_u64())
                .and_then(|(finish, failed)| finish.checked_sub(failed));
            lines.push(format!(
                "read={id} failed_send(seq={},attempt={},sent={}) -> first_new_attempt={first_forbidden:?}, GETs={requests}, body_bytes={bytes}; earlier_attempt_body_bytes={earlier_bytes}; sent_delta={delta:?}, finish={}/{}",
                failed["sequence"], failed["attempt"], failed["sent"], read["finished"]["outcome"], read["finished"]["error_code"],
            ));
        }
    }
    if report["complete"] != true {
        lines.push(format!("incomplete evidence: {}", report["errors"]));
    }
    lines.join("; ")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use tracing_subscriber::prelude::*;

    use super::*;

    static FILE_ID: AtomicU64 = AtomicU64::new(0);

    struct TraceFile(std::path::PathBuf);

    impl TraceFile {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "azure-witness-{}-{}.jsonl",
                std::process::id(),
                FILE_ID.fetch_add(1, Ordering::Relaxed),
            )))
        }

        fn write(events: &[Value], footer: bool) -> Self {
            let file = Self::new();
            let mut text = String::new();
            for event in events {
                text.push_str(&event.to_string());
                text.push('\n');
            }
            if footer {
                text.push_str(
                    &json!({
                        "sequence": events.len() + 1,
                        "elapsed_s": events.len() + 1,
                        "event": "trace_finished",
                        "capture_error": null,
                    })
                    .to_string(),
                );
                text.push('\n');
            }
            fs::write(&file.0, text).unwrap();
            file
        }
    }

    impl Drop for TraceFile {
        fn drop(&mut self) {
            drop(fs::remove_file(&self.0));
        }
    }

    fn event(sequence: u64, event: &str, attempt: u64, sent: u64, broken: bool) -> Value {
        json!({
            "sequence": sequence, "elapsed_s": sequence, "event": event,
            "read_id": 9, "attempt": attempt, "sent": sent, "writer_broken": broken,
            "blob": "payload", "stage": "data", "outcome": "error", "error_code": "Unavailable",
        })
    }

    fn forbidden_retry() -> Vec<Value> {
        vec![
            event(1, "read_started", 0, 0, false),
            event(2, "attempt_started", 1, 0, false),
            event(3, "send_failed", 1, 4, true),
            event(4, "attempt_started", 2, 4, true),
            event(5, "read_finished", 2, 4, true),
        ]
    }

    fn request(id: u64, attempt: u64, sent: u64, broken: bool, bytes: u64) -> Value {
        json!({
            "request_id": id, "read_id": 9, "attempt": attempt,
            "sent": sent, "writer_broken": broken, "bytes": bytes,
            "blob": "payload", "offset": sent, "length": bytes,
            "status": 206, "start_s": 999, "end_s": 1000,
        })
    }

    #[test]
    fn final_throttle_does_not_hide_prior_forbidden_retry() {
        let file = TraceFile::write(&forbidden_retry(), true);
        let mut throttle = request(1, 2, 4, true, 0);
        throttle["status"] = 503.into();
        let report = analyze(&file.0, &[request(0, 1, 0, false, 32), throttle]);
        assert_eq!(report["complete"], true);
        assert_eq!(report["failed_destination_reads"], 1);
        assert_eq!(report["new_attempts_after_failed_send"], 1);
        assert_eq!(
            report["reads"]["9"]["attempts"]["2"]["after_failed_send_sequence"],
            3
        );
        assert!(explain(&report).contains("first_new_attempt=Some(2)"));
    }

    #[test]
    fn legitimate_retries_before_first_failed_send_are_not_violations() {
        let events = vec![
            event(1, "read_started", 0, 0, false),
            event(2, "attempt_started", 1, 0, false),
            event(3, "attempt_started", 2, 0, false),
            event(4, "send_failed", 2, 4, true),
            event(5, "read_finished", 2, 4, true),
        ];
        let file = TraceFile::write(&events, true);
        let mut throttle = request(0, 1, 0, false, 0);
        throttle["status"] = 503.into();
        let report = analyze(&file.0, &[throttle, request(1, 2, 0, false, 32)]);
        assert_eq!(report["complete"], true);
        assert_eq!(report["failed_destination_reads"], 1);
        assert_eq!(report["new_attempts_after_failed_send"], 0);
    }

    #[test]
    fn old_sdk_tasks_keep_their_original_attempt_identity() {
        let file = TraceFile::write(&forbidden_retry(), true);
        let report = analyze(
            &file.0,
            &[
                request(0, 2, 4, true, 8),
                // Observed after the later attempt and after read_finished. The
                // arbitrary server clock cannot turn this into a new retry.
                request(1, 1, 0, false, 32),
            ],
        );
        assert_eq!(report["complete"], true);
        assert_eq!(report["new_attempts_after_failed_send"], 1);
        assert_eq!(report["reads"]["9"]["attempts"]["1"]["body_bytes"], 32);
        assert_eq!(report["reads"]["9"]["attempts"]["2"]["body_bytes"], 8);
    }

    #[test]
    fn healthy_eof_and_nonterminal_send_failure_do_not_anchor_a_violation() {
        let mut events = forbidden_retry();
        events[2]["writer_broken"] = false.into();
        events[3]["writer_broken"] = false.into();
        events[4]["outcome"] = "ok".into();
        events[4]["error_code"] = "".into();
        let file = TraceFile::write(&events, true);
        let report = analyze(&file.0, &[]);
        assert_eq!(report["complete"], true);
        assert_eq!(report["failed_destination_reads"], 0);
        assert_eq!(report["new_attempts_after_failed_send"], 0);
    }

    #[test]
    fn missing_evidence_never_becomes_a_clean_pass() {
        let file = TraceFile::write(&forbidden_retry(), true);
        let mut missing_identity = request(0, 1, 0, false, 32);
        missing_identity["read_id"] = Value::Null;
        let mut bad_snapshot = request(0, 1, 4, false, 32);
        bad_snapshot["writer_broken"] = true.into();
        for request in [missing_identity, request(0, 3, 4, true, 32), bad_snapshot] {
            let report = analyze(&file.0, &[request]);
            assert_eq!(report["complete"], false);
            assert_eq!(report["new_attempts_after_failed_send"], 1);
            assert_eq!(report["uncorrelated_requests"].as_array().unwrap().len(), 1);
        }
        let no_footer = TraceFile::write(&forbidden_retry(), false);
        assert_eq!(analyze(&no_footer.0, &[])["complete"], false);
        let no_finish = TraceFile::write(&forbidden_retry()[..4], true);
        let report = analyze(&no_finish.0, &[]);
        assert_eq!(report["complete"], false);
        assert_eq!(report["new_attempts_after_failed_send"], 1);
        let mut hole = forbidden_retry();
        hole.remove(1);
        let hole = TraceFile::write(&hole, true);
        assert_eq!(analyze(&hole.0, &[])["complete"], false);
    }

    #[test]
    fn unexplained_closure_or_post_failure_delivery_is_incomplete() {
        let mut events = forbidden_retry();
        events.remove(2);
        for (index, event) in events.iter_mut().enumerate() {
            event["sequence"] = (index + 1).into();
        }
        let file = TraceFile::write(&events, true);
        let report = analyze(&file.0, &[]);
        assert_eq!(report["complete"], false);
        assert_eq!(report["new_attempts_after_failed_send"], 0);

        let events = vec![
            event(1, "read_started", 0, 0, false),
            event(2, "attempt_started", 1, 0, false),
            event(3, "read_finished", 1, 4, true),
        ];
        let file = TraceFile::write(&events, true);
        assert_eq!(analyze(&file.0, &[])["complete"], false);

        let mut events = forbidden_retry();
        events[4]["sent"] = 8.into();
        let file = TraceFile::write(&events, true);
        let report = analyze(&file.0, &[]);
        assert_eq!(report["complete"], false);
        assert_eq!(report["new_attempts_after_failed_send"], 1);
    }

    #[test]
    fn terminal_send_cannot_finish_writable_or_successful() {
        for (field, value) in [("writer_broken", json!(false)), ("outcome", json!("ok"))] {
            let mut events = forbidden_retry();
            events[4][field] = value;
            let file = TraceFile::write(&events, true);
            assert_eq!(analyze(&file.0, &[])["complete"], false);
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn capture_write_error_survives_until_finish() {
        let mut state = Capture {
            writer: BufWriter::with_capacity(
                0,
                OpenOptions::new().write(true).open("/dev/full").unwrap(),
            ),
            started: Instant::now(),
            sequence: 0,
            error: None,
            finished: false,
        };
        state.write(Map::new());
        assert!(state.error.is_some());
        let handle = CaptureHandle(Arc::new(Mutex::new(state)));
        assert!(handle.finish().is_err());
    }

    #[test]
    fn capture_orders_real_events_and_requires_explicit_finish() {
        let file = TraceFile::new();
        let (layer, handle) = capture(&file.0).unwrap();
        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::debug!(target: "unrelated", event = "ignored");
            tracing::debug!(target: TARGET, event = "read_started", read_id = 9_u64,
                attempt = 0_u64, sent = 0_u64, writer_broken = false, blob = %"payload");
            // A permit failure can finish before any SDK download starts.
            tracing::debug!(target: TARGET, event = "read_finished", read_id = 9_u64,
                attempt = 0_u64, sent = 0_u64, writer_broken = false,
                outcome = "error", error_code = %"ResourceExhausted");
        });
        handle.finish().unwrap();
        let report = analyze(&file.0, &[]);
        assert_eq!(report["complete"], true, "{report}");
        assert_eq!(report["reads"]["9"]["started"]["sequence"], 1);
        assert_eq!(report["reads"]["9"]["finished"]["sequence"], 2);
    }
}
