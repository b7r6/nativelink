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

//! Summarise output provenance events (schema 1).
//!
//! The server and workers emit one `DEBUG` event on the `nativelink_provenance` tracing
//! target for each execution attempt, each output it hashes, each upload
//! decision, each CAS write the server accepts and each Azure Blob write.
//! This reads those events from log files and answers, weighted by bytes:
//!
//! 1. Which producers (target, mnemonic, output path) account for the output
//!    bytes first observed in the window.
//!
//!    1b. Which targets the client's own uploads came from, by the request
//!    metadata on `cas_write`. This covers actions the client ran locally,
//!    which no worker saw.
//! 2. Which action digests produced more than one output manifest.
//! 3. For each logical producer, whether consecutive complete attempts changed
//!    their declared identity, their output, both, or neither. Reports 2 and 3
//!    only use attempts whose outcome is `completed` and whose outputs were
//!    all uploaded.
//! 4. How many output bytes each invocation added that no earlier one had.
//! 5. How many CAS and backend write bytes were first writes, and how many
//!    re-wrote a digest or key already written; for the backend, how many of
//!    those rewrites returned an Azure version ID, so the account kept the
//!    copy they replaced.
//! 6. The distribution of the worker's `has()` results, and what was uploaded
//!    after each.
//! 7. How complete the log is: attempts that never finished, incomplete
//!    uploads, and events with a schema this tool does not read.
//!
//! Each FILE is a log, plain or gzip-compressed (detected from its first
//! bytes, not its name). With no FILE, or with `-`, it reads stdin. Collect
//! logs from every worker and every server process for the same window and
//! pass them all; the analysis deduplicates by digest across the whole set.
//!
//! Two line shapes are accepted: the `NL_LOG=json` output, where the
//! event's fields are nested under `"fields"` and `"target"` is
//! `"nativelink_provenance"`; and a flat JSON object with `event` and `schema`
//! at the top level (or under `"attributes"`), which is what an OTLP log
//! export usually becomes once flattened. Text before the first `{` on a line
//! is ignored. Lines that are not JSON, and JSON lines that are not provenance
//! events, are skipped and counted. Events with a schema other than 1 are
//! skipped, counted and reported, because a schema bump means a field changed
//! meaning.

mod analysis;
mod collect;
mod parse;
mod render;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, BufRead, Read, Write};
use std::process::ExitCode;

use clap::Parser;

use crate::analysis::{Analysis, Report};
use crate::collect::{Collector, SUPPORTED_SCHEMA};
use crate::parse::{maybe_gunzip, parse_seed};
use crate::render::render_text;

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(
    version,
    about = "Summarise NativeLink output provenance events (schema 1).",
    after_help = "Logs come from NativeLink run with RUST_LOG=info,nativelink_provenance=debug \
                  and NL_LOG=json, on workers and CAS servers."
)]
pub(crate) struct Args {
    /// Log files, plain or gzip; `-` or none for stdin.
    pub(crate) files: Vec<String>,

    /// Digests known before the window, one `<hash>-<size>` or `<hash>` per
    /// line (a blob inventory CSV works). They never count as new, and the
    /// first write of one in the window is a pre-window rewrite.
    #[arg(long, value_name = "FILE")]
    pub(crate) seed_digests: Option<String>,

    /// Rows per ranked table, 0 for all. The per-invocation table is
    /// chronological and never cut.
    #[arg(long, default_value_t = 20, allow_negative_numbers = true)]
    pub(crate) top: i64,

    /// Print one JSON document, one key per report, with raw byte counts.
    #[arg(long)]
    pub(crate) json: bool,

    /// Do not shorten digests or paths in the text report.
    #[arg(long)]
    pub(crate) wide: bool,
}

/// Opens a named input, or `stdin` for `-`.
fn open<'a>(path: &str, stdin: &'a mut dyn Read) -> io::Result<Box<dyn Read + 'a>> {
    if path == "-" {
        Ok(Box::new(stdin))
    } else {
        Ok(Box::new(File::open(path)?))
    }
}

fn read_seed(path: &str, stdin: &mut dyn Read) -> io::Result<(BTreeSet<String>, usize)> {
    let mut reader: Box<dyn BufRead> = maybe_gunzip(open(path, stdin)?)?;
    parse_seed(&mut reader)
}

/// Builds the report from parsed arguments, writing warnings to `err`.
pub(crate) fn build_report(
    args: &Args,
    stdin: &mut dyn Read,
    err: &mut dyn Write,
) -> Result<Report, String> {
    let seed = if let Some(path) = &args.seed_digests {
        let (hashes, bad) = read_seed(path, stdin).map_err(|e| format!("{path}: {e}"))?;
        if bad > 0 {
            writeln!(
                err,
                "warning: {bad} line(s) in {path} are not digests; ignored"
            )
            .map_err(|e| e.to_string())?;
        }
        hashes
    } else {
        BTreeSet::new()
    };

    let mut collector = Collector::new();
    let stdin_only = ["-".to_string()];
    let files = if args.files.is_empty() {
        &stdin_only[..]
    } else {
        &args.files[..]
    };
    for path in files {
        let reader = open(path, stdin).map_err(|e| format!("{path}: {e}"))?;
        collector
            .read(path, reader)
            .map_err(|e| format!("{path}: {e}"))?;
    }

    let warn = |err: &mut dyn Write, message: String| -> Result<(), String> {
        writeln!(err, "warning: {message}").map_err(|e| e.to_string())
    };
    for (schema, count) in &collector.stats.unknown_schema {
        warn(
            err,
            format!(
                "skipped {count} provenance event(s) with schema {schema}; this tool reads \
                 schema {SUPPORTED_SCHEMA}"
            ),
        )?;
    }
    if collector.stats.provenance == 0 {
        warn(
            err,
            "no provenance events found; NativeLink needs \
             RUST_LOG=info,nativelink_provenance=debug and NL_LOG=json"
                .to_string(),
        )?;
    }
    Ok(Analysis::new(&collector, &seed, args.top).report())
}

/// Runs the tool. Returns the exit status: 0, or 2 if an input cannot be
/// read.
pub(crate) fn run(
    args: &Args,
    stdin: &mut dyn Read,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> u8 {
    let report = match build_report(args, stdin, err) {
        Ok(report) => report,
        Err(message) => {
            // Nothing more useful can be done if stderr itself is gone.
            drop(writeln!(err, "error: {message}"));
            return 2;
        }
    };
    let written = if args.json {
        serde_json::to_writer_pretty(&mut *out, &report)
            .map_err(io::Error::from)
            .and_then(|()| out.write_all(b"\n"))
    } else {
        out.write_all(render_text(&report, args.top, args.wide).as_bytes())
    };
    match written.and_then(|()| out.flush()) {
        Ok(()) => 0,
        Err(error) => {
            drop(writeln!(err, "error: writing the report: {error}"));
            1
        }
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    let code = run(
        &args,
        &mut io::stdin().lock(),
        &mut io::stdout().lock(),
        &mut io::stderr().lock(),
    );
    ExitCode::from(code)
}
