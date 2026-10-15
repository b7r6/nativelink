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

//! Turning provenance events into the compact records the analyses need.
//!
//! Strings are interned and records are small, because a day of a busy fleet
//! is tens of millions of output events.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{self, Read};
use std::rc::Rc;

use serde_json::{Map, Value};

use crate::parse::{
    Line, NO_TIMESTAMP, as_bool, as_int, as_schema, as_str, classify_line, digest_hash,
    for_each_line, maybe_gunzip, parse_timestamp, split_backend_key,
};

pub(crate) const SUPPORTED_SCHEMA: i64 = 1;

pub(crate) const KNOWN_EVENTS: [&str; 7] = [
    "attempt_started",
    "output_observed",
    "output_upload",
    "attempt_finished",
    "cas_write",
    "find_missing",
    "backend_write",
];

/// (timestamp in ns, input sequence number): a total order over events.
pub(crate) type Order = (i64, u64);

pub(crate) type Text = Rc<str>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Meta {
    pub(crate) target: Text,
    pub(crate) mnemonic: Text,
    pub(crate) config: Text,
    pub(crate) invocation: Text,
}

#[derive(Debug)]
pub(crate) struct Output {
    pub(crate) order: Order,
    pub(crate) attempt: usize,
    pub(crate) kind: Text,
    pub(crate) path: Text,
    pub(crate) hash: Text,
    pub(crate) size: i64,
    pub(crate) first_in_attempt: bool,
    pub(crate) symlink_target: Text,
    pub(crate) action: Text,
    pub(crate) meta: Option<Rc<Meta>>,
}

#[derive(Debug)]
pub(crate) struct Upload {
    pub(crate) order: Order,
    pub(crate) attempt: usize,
    pub(crate) hash: Text,
    pub(crate) size: i64,
    pub(crate) has_result: Text,
    pub(crate) has_error: Text,
    pub(crate) disposition: Text,
    pub(crate) has_elapsed_ms: i64,
}

#[derive(Debug)]
pub(crate) struct Write {
    pub(crate) order: Order,
    /// `(source)` for a CAS write, `(store, key prefix)` for a backend write.
    pub(crate) group: Vec<Text>,
    /// What makes two writes the same blob: the digest hash or the key.
    pub(crate) ident: Text,
    pub(crate) display: Text,
    pub(crate) size: i64,
    pub(crate) ok: bool,
    pub(crate) error_code: Text,
    pub(crate) invocation: Text,
    pub(crate) seed_hash: String,
    pub(crate) target: Text,
    pub(crate) mnemonic: Text,
    pub(crate) versioned: bool,
}

#[derive(Debug)]
pub(crate) struct Attempt {
    pub(crate) id: Text,
    pub(crate) started: bool,
    pub(crate) start_order: Option<Order>,
    pub(crate) first_order: Order,
    pub(crate) operation_id: Text,
    pub(crate) action: Text,
    pub(crate) command: Text,
    pub(crate) input_root: Text,
    pub(crate) meta: Option<Rc<Meta>>,
    pub(crate) finished: bool,
    pub(crate) outcome: Text,
    pub(crate) outputs_complete: bool,
    pub(crate) output_count: i64,
    pub(crate) fingerprint: Text,
}

impl Attempt {
    fn new(id: Text, empty: &Text) -> Self {
        Self {
            id,
            started: false,
            start_order: None,
            first_order: (NO_TIMESTAMP, 1 << 62),
            operation_id: empty.clone(),
            action: empty.clone(),
            command: empty.clone(),
            input_root: empty.clone(),
            meta: None,
            finished: false,
            outcome: empty.clone(),
            outputs_complete: false,
            output_count: 0,
            fingerprint: empty.clone(),
        }
    }

    /// When the attempt started, or its earliest event if it has no start.
    pub(crate) fn order(&self) -> Order {
        self.start_order.unwrap_or(self.first_order)
    }

    fn touch(&mut self, order: Order) {
        if order < self.first_order {
            self.first_order = order;
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct InputStats {
    pub(crate) files: Vec<String>,
    pub(crate) lines: i64,
    pub(crate) non_json: i64,
    pub(crate) non_provenance: i64,
    pub(crate) provenance: i64,
    pub(crate) unknown_schema: BTreeMap<String, i64>,
    pub(crate) unknown_events: BTreeMap<String, i64>,
    pub(crate) events: BTreeMap<String, i64>,
    pub(crate) missing_timestamp: i64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct FindMissingStats {
    pub(crate) requests: i64,
    pub(crate) errors: i64,
    pub(crate) requested_count: i64,
    pub(crate) requested_bytes: i64,
    pub(crate) missing_count: i64,
    pub(crate) missing_bytes: i64,
}

#[derive(Debug, Default)]
struct Interner {
    strings: HashSet<Text>,
    metas: HashSet<Rc<Meta>>,
}

impl Interner {
    fn text(&mut self, value: &str) -> Text {
        if let Some(found) = self.strings.get(value) {
            return found.clone();
        }
        let text: Text = Rc::from(value);
        self.strings.insert(text.clone());
        text
    }

    fn meta(&mut self, meta: Meta) -> Rc<Meta> {
        if let Some(found) = self.metas.get(&meta) {
            return found.clone();
        }
        let meta = Rc::new(meta);
        self.metas.insert(meta.clone());
        meta
    }
}

/// Every provenance event read so far, in compact form.
#[derive(Debug)]
pub(crate) struct Collector {
    pub(crate) stats: InputStats,
    /// In the order each attempt was first seen.
    pub(crate) attempts: Vec<Attempt>,
    attempt_index: HashMap<Text, usize>,
    pub(crate) outputs: Vec<Output>,
    pub(crate) uploads: Vec<Upload>,
    pub(crate) cas_writes: Vec<Write>,
    pub(crate) backend_writes: Vec<Write>,
    pub(crate) find_missing: FindMissingStats,
    /// `backend_write` events that carry a `version_id` key at all, empty or
    /// not. Zero means the logs predate the field, not that nothing is kept.
    pub(crate) version_id_reported: i64,
    pub(crate) first_ts: Option<i64>,
    pub(crate) last_ts: Option<i64>,
    seq: u64,
    interner: Interner,
    empty: Text,
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl Collector {
    pub(crate) fn new() -> Self {
        let mut interner = Interner::default();
        let empty = interner.text("");
        Self {
            stats: InputStats::default(),
            attempts: Vec::new(),
            attempt_index: HashMap::new(),
            outputs: Vec::new(),
            uploads: Vec::new(),
            cas_writes: Vec::new(),
            backend_writes: Vec::new(),
            find_missing: FindMissingStats::default(),
            version_id_reported: 0,
            first_ts: None,
            last_ts: None,
            seq: 0,
            interner,
            empty,
        }
    }

    /// Reads every line of `reader` (gunzipping it if needed), recording
    /// `name` in the input list.
    pub(crate) fn read(&mut self, name: &str, reader: impl Read) -> io::Result<()> {
        self.stats.files.push(name.to_string());
        let mut lines = maybe_gunzip(reader)?;
        for_each_line(&mut lines, |line| self.feed_line(line))
    }

    pub(crate) fn feed_line(&mut self, line: &str) {
        let (record, raw_timestamp) = match classify_line(line) {
            Line::Blank => return,
            Line::NonJson => {
                self.stats.lines += 1;
                self.stats.non_json += 1;
                return;
            }
            Line::NonProvenance => {
                self.stats.lines += 1;
                self.stats.non_provenance += 1;
                return;
            }
            Line::Provenance {
                record,
                raw_timestamp,
            } => (record, raw_timestamp),
        };
        self.stats.lines += 1;

        let schema = as_schema(record.get("schema"));
        if schema != Some(SUPPORTED_SCHEMA) {
            let label = schema.map_or_else(|| "missing".to_string(), |s| s.to_string());
            *self.stats.unknown_schema.entry(label).or_insert(0) += 1;
            return;
        }
        let kind = as_str(record.get("event"));
        if !KNOWN_EVENTS.contains(&kind.as_str()) {
            let label = if kind.is_empty() {
                "(empty)".to_string()
            } else {
                kind
            };
            *self.stats.unknown_events.entry(label).or_insert(0) += 1;
            return;
        }

        let timestamp = parse_timestamp(raw_timestamp.as_ref());
        self.seq += 1;
        let order = if let Some(ts) = timestamp {
            self.first_ts = Some(self.first_ts.map_or(ts, |first| first.min(ts)));
            self.last_ts = Some(self.last_ts.map_or(ts, |last| last.max(ts)));
            (ts, self.seq)
        } else {
            self.stats.missing_timestamp += 1;
            (NO_TIMESTAMP, self.seq)
        };
        self.stats.provenance += 1;
        *self.stats.events.entry(kind.clone()).or_insert(0) += 1;
        match kind.as_str() {
            "attempt_started" => self.on_attempt_started(&record, order),
            "output_observed" => self.on_output_observed(&record, order),
            "output_upload" => self.on_output_upload(&record, order),
            "attempt_finished" => self.on_attempt_finished(&record, order),
            "cas_write" => self.on_cas_write(&record, order),
            "backend_write" => self.on_backend_write(&record, order),
            _ => self.on_find_missing(&record),
        }
    }

    fn text(&mut self, record: &Map<String, Value>, key: &str) -> Text {
        let value = as_str(record.get(key));
        self.interner.text(&value)
    }

    /// The text of a field, or `fallback` when it is empty.
    fn text_or(&mut self, record: &Map<String, Value>, key: &str, fallback: &str) -> Text {
        let value = as_str(record.get(key));
        self.interner
            .text(if value.is_empty() { fallback } else { &value })
    }

    fn hash(&mut self, record: &Map<String, Value>) -> Text {
        let hash = digest_hash(&as_str(record.get("digest")));
        self.interner.text(&hash)
    }

    fn meta(&mut self, record: &Map<String, Value>) -> Option<Rc<Meta>> {
        let meta = Meta {
            target: self.text(record, "target_id"),
            mnemonic: self.text(record, "action_mnemonic"),
            config: self.text(record, "configuration_id"),
            invocation: self.text(record, "tool_invocation_id"),
        };
        if meta.target.is_empty()
            && meta.mnemonic.is_empty()
            && meta.config.is_empty()
            && meta.invocation.is_empty()
        {
            return None;
        }
        Some(self.interner.meta(meta))
    }

    fn attempt(&mut self, record: &Map<String, Value>, order: Order) -> usize {
        let attempt_id = self.text(record, "attempt_id");
        let index = if let Some(index) = self.attempt_index.get(&attempt_id) {
            *index
        } else {
            let index = self.attempts.len();
            self.attempts
                .push(Attempt::new(attempt_id.clone(), &self.empty));
            self.attempt_index.insert(attempt_id, index);
            index
        };
        self.attempts[index].touch(order);
        index
    }

    fn on_attempt_started(&mut self, record: &Map<String, Value>, order: Order) {
        let index = self.attempt(record, order);
        if self.attempts[index].started
            && self.attempts[index]
                .start_order
                .is_some_and(|start| start <= order)
        {
            return;
        }
        let operation_id = self.text(record, "operation_id");
        let action = self.text(record, "action_digest");
        let command = self.text(record, "command_digest");
        let input_root = self.text(record, "input_root_digest");
        let meta = self.meta(record);
        let attempt = &mut self.attempts[index];
        attempt.started = true;
        attempt.start_order = Some(order);
        attempt.operation_id = operation_id;
        if !action.is_empty() {
            attempt.action = action;
        }
        attempt.command = command;
        attempt.input_root = input_root;
        if meta.is_some() {
            attempt.meta = meta;
        }
    }

    fn on_output_observed(&mut self, record: &Map<String, Value>, order: Order) {
        let attempt = self.attempt(record, order);
        let action = self.text(record, "action_digest");
        if !action.is_empty() && self.attempts[attempt].action.is_empty() {
            self.attempts[attempt].action = action.clone();
        }
        let output = Output {
            order,
            attempt,
            kind: self.text(record, "kind"),
            path: self.text(record, "path"),
            hash: self.hash(record),
            size: as_int(record.get("size_bytes")),
            first_in_attempt: as_bool(record.get("first_in_attempt")),
            symlink_target: self.text(record, "symlink_target"),
            action,
            meta: self.meta(record),
        };
        self.outputs.push(output);
    }

    fn on_output_upload(&mut self, record: &Map<String, Value>, order: Order) {
        let attempt = self.attempt(record, order);
        let upload = Upload {
            order,
            attempt,
            hash: self.hash(record),
            size: as_int(record.get("size_bytes")),
            has_result: self.text_or(record, "has_result", "(empty)"),
            has_error: self.text(record, "has_error"),
            disposition: self.text_or(record, "disposition", "(empty)"),
            has_elapsed_ms: as_int(record.get("has_elapsed_ms")),
        };
        self.uploads.push(upload);
    }

    fn on_attempt_finished(&mut self, record: &Map<String, Value>, order: Order) {
        let index = self.attempt(record, order);
        let action = self.text(record, "action_digest");
        let outcome = self.text_or(record, "outcome", "(empty)");
        let fingerprint = self.text(record, "manifest_fingerprint");
        let meta = if self.attempts[index].meta.is_none() {
            self.meta(record)
        } else {
            None
        };
        let attempt = &mut self.attempts[index];
        attempt.finished = true;
        if !action.is_empty() && attempt.action.is_empty() {
            attempt.action = action;
        }
        attempt.outcome = outcome;
        attempt.outputs_complete = as_bool(record.get("outputs_complete"));
        attempt.output_count = as_int(record.get("output_count"));
        attempt.fingerprint = fingerprint;
        if attempt.meta.is_none() {
            attempt.meta = meta;
        }
    }

    fn on_cas_write(&mut self, record: &Map<String, Value>, order: Order) {
        let digest = self.text(record, "digest");
        let hashed = digest_hash(&digest);
        let write = Write {
            order,
            group: vec![self.text_or(record, "source", "(empty)")],
            ident: self.interner.text(&hashed),
            display: digest,
            size: as_int(record.get("size_bytes")),
            ok: as_str(record.get("result")) == "ok",
            error_code: self.text(record, "error_code"),
            invocation: self.text(record, "tool_invocation_id"),
            seed_hash: hashed,
            target: self.text(record, "target_id"),
            mnemonic: self.text(record, "action_mnemonic"),
            versioned: false,
        };
        self.cas_writes.push(write);
    }

    fn on_backend_write(&mut self, record: &Map<String, Value>, order: Order) {
        if record.contains_key("version_id") {
            self.version_id_reported += 1;
        }
        let key = self.text(record, "key");
        let (prefix, hashed) = split_backend_key(&key);
        let write = Write {
            order,
            group: vec![
                self.text_or(record, "store", "(empty)"),
                self.interner.text(&prefix),
            ],
            ident: key.clone(),
            display: key,
            size: as_int(record.get("size_bytes")),
            ok: as_str(record.get("result")) == "ok",
            error_code: self.text(record, "error_code"),
            invocation: self.text(record, "tool_invocation_id"),
            seed_hash: hashed,
            target: self.text(record, "target_id"),
            mnemonic: self.text(record, "action_mnemonic"),
            // Azure's x-ms-version-id. Logs from before the field existed
            // have no key at all, which reads the same as "".
            versioned: !as_str(record.get("version_id")).is_empty(),
        };
        self.backend_writes.push(write);
    }

    fn on_find_missing(&mut self, record: &Map<String, Value>) {
        let stats = &mut self.find_missing;
        stats.requests += 1;
        if !matches!(as_str(record.get("result")).as_str(), "ok" | "") {
            stats.errors += 1;
        }
        stats.requested_count += as_int(record.get("requested_count"));
        stats.requested_bytes += as_int(record.get("requested_bytes"));
        stats.missing_count += as_int(record.get("missing_count"));
        stats.missing_bytes += as_int(record.get("missing_bytes"));
    }
}
