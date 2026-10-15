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

//! Reading log lines: field normalisation, timestamps, digests, and telling a
//! provenance event from everything else on a line.

use std::collections::BTreeSet;
use std::io::{self, BufRead, BufReader, Read};

use flate2::read::MultiGzDecoder;
use serde_json::{Map, Value};

pub(crate) const PROVENANCE_TARGET: &str = "nativelink_provenance";

/// Sort key for an event with no usable timestamp: after everything else, in
/// input order.
pub(crate) const NO_TIMESTAMP: i64 = 1 << 62;

const TIMESTAMP_KEYS: [&str; 8] = [
    "timestamp",
    "time",
    "@timestamp",
    "ts",
    "timeUnixNano",
    "time_unix_nano",
    "observedTimeUnixNano",
    "observed_time_unix_nano",
];

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

// --------------------------------------------------------------------------
// Field normalisation. A flattened OTLP export may turn numbers and booleans
// into strings, and an emitter bug may leave a field out, so every read goes
// through one of these.

/// A field as text: `""` when absent or null.
pub(crate) fn as_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Bool(flag)) => if *flag { "true" } else { "false" }.to_string(),
        Some(other) => other.to_string(),
    }
}

/// Truncates toward zero, saturating, as Python's `int(float)` does for
/// every value a log can reasonably hold.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the value is truncated on purpose; `as` saturates out-of-range values"
)]
const fn truncate(value: f64) -> i64 {
    value as i64
}

/// A field as an integer: `0` when absent or not a number.
pub(crate) fn as_int(value: Option<&Value>) -> i64 {
    match value {
        None | Some(Value::Null | Value::Array(_) | Value::Object(_)) => 0,
        Some(Value::Bool(flag)) => i64::from(*flag),
        Some(Value::Number(number)) => number.as_i64().unwrap_or_else(|| {
            number.as_u64().map_or_else(
                || number.as_f64().map_or(0, truncate),
                |unsigned| i64::try_from(unsigned).unwrap_or(i64::MAX),
            )
        }),
        Some(Value::String(text)) => {
            let text = text.trim();
            text.parse::<i64>().unwrap_or_else(|_| {
                text.parse::<f64>()
                    .ok()
                    .filter(|number| number.is_finite())
                    .map_or(0, truncate)
            })
        }
    }
}

/// A field as a boolean: `"true"`, `"1"` and `"yes"` count as true.
pub(crate) fn as_bool(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => number.as_f64().is_some_and(|n| n != 0.0),
        Some(Value::String(text)) => {
            matches!(text.trim().to_lowercase().as_str(), "true" | "1" | "yes")
        }
        _ => false,
    }
}

/// The `schema` field as an integer, or `None` if it is missing or not one.
pub(crate) fn as_schema(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(number)) => number.as_i64().or_else(|| {
            number
                .as_u64()
                .map(|unsigned| i64::try_from(unsigned).unwrap_or(i64::MAX))
                .or_else(|| {
                    number
                        .as_f64()
                        .filter(|f| f.is_finite() && f.fract() == 0.0)
                        .map(truncate)
                })
        }),
        Some(Value::String(text)) => {
            let text = text.trim();
            if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
                text.parse().ok()
            } else {
                None
            }
        }
        _ => None,
    }
}

/// The hash part of a `<hash>-<size>` digest, lowercased.
pub(crate) fn digest_hash(digest: &str) -> String {
    let text = digest.trim();
    if let Some((head, tail)) = text.rsplit_once('-')
        && !head.is_empty()
        && !tail.is_empty()
        && tail.bytes().all(|b| b.is_ascii_digit())
    {
        return head.to_lowercase();
    }
    text.to_lowercase()
}

/// Splits a backend key into (prefix, digest hash), or two empty strings if
/// the key does not end in `<hash>-<size>` with a hash of at least 16 hex
/// digits.
///
/// The Azure store writes `<blob_prefix><hash>-<size>`, so the prefix is what
/// separates, for example, a CAS from an AC sharing one container.
pub(crate) fn split_backend_key(key: &str) -> (String, String) {
    let Some((head, tail)) = key.rsplit_once('-') else {
        return (String::new(), String::new());
    };
    if tail.is_empty() || !tail.bytes().all(|b| b.is_ascii_digit()) {
        return (String::new(), String::new());
    }
    let hex_len = head.bytes().rev().take_while(u8::is_ascii_hexdigit).count();
    if hex_len < 16 {
        return (String::new(), String::new());
    }
    let start = head.len() - hex_len;
    (head[..start].to_string(), head[start..].to_lowercase())
}

// --------------------------------------------------------------------------
// Timestamps, as integer nanoseconds since the epoch so that ordering is exact.

fn epoch_ns_from_int(value: i128) -> Option<i64> {
    let magnitude = value.abs();
    let scaled = if magnitude >= 100_000_000_000_000_000 {
        value
    } else if magnitude >= 100_000_000_000_000 {
        value * 1_000
    } else if magnitude >= 100_000_000_000 {
        value * 1_000_000
    } else {
        value * 1_000_000_000
    };
    i64::try_from(scaled).ok()
}

/// Days from 1970-01-01 to the first of `month` in `year` (proleptic
/// Gregorian), or `None` outside years 1 to 9999 and months 1 to 12.
fn days_to_month_start(year: i64, month: i64) -> Option<i64> {
    if !(1..=9999).contains(&year) || !(1..=12).contains(&month) {
        return None;
    }
    // Howard Hinnant's days_from_civil.
    let shifted_year = if month <= 2 { year - 1 } else { year };
    let era = shifted_year.div_euclid(400);
    let year_of_era = shifted_year - era * 400;
    let shifted_month = (month + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

fn digits(text: &str) -> Option<i64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// Parses `YYYY-MM-DD[T ]HH:MM:SS[.frac][Z|±HH[:]MM]`. Like the calendar
/// arithmetic it mirrors, it checks the year and month and lets the day and
/// time fields carry over.
fn parse_rfc3339(text: &str) -> Option<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 19
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = digits(text.get(0..4)?)?;
    let month = digits(text.get(5..7)?)?;
    let day = digits(text.get(8..10)?)?;
    let hour = digits(text.get(11..13)?)?;
    let minute = digits(text.get(14..16)?)?;
    let second = digits(text.get(17..19)?)?;
    let mut rest = text.get(19..)?;

    let mut nanos = 0;
    if let Some(after) = rest.strip_prefix(['.', ',']) {
        let fraction_len = after.bytes().take_while(u8::is_ascii_digit).count();
        if fraction_len == 0 {
            return None;
        }
        let fraction = &after[..fraction_len];
        let padded = format!("{:0<9}", &fraction[..fraction.len().min(9)]);
        nanos = padded.parse::<i64>().ok()?;
        rest = &after[fraction_len..];
    }
    let rest = rest.trim_start();
    let offset = match rest {
        "" | "Z" | "z" => 0,
        zone => {
            let sign = match zone.as_bytes().first() {
                Some(b'+') => 1,
                Some(b'-') => -1,
                _ => return None,
            };
            let body = &zone[1..];
            let compact = match body.len() {
                4 => body.to_string(),
                5 if body.as_bytes()[2] == b':' => body.replacen(':', "", 1),
                _ => return None,
            };
            let hours = digits(&compact[..2])?;
            let minutes = digits(&compact[2..])?;
            sign * (hours * 3600 + minutes * 60)
        }
    };
    let days = days_to_month_start(year, month)? + day - 1;
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second - offset;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

fn parse_float_timestamp(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    if value.abs() >= 1e11 {
        return epoch_ns_from_int(i128::from(truncate(value)));
    }
    let whole = value.trunc();
    let nanos = truncate(((value - whole) * 1e9).round_ties_even());
    truncate(whole)
        .checked_mul(1_000_000_000)?
        .checked_add(nanos)
}

/// Parses an RFC 3339 string or a Unix time (s, ms, us or ns) to ns.
pub(crate) fn parse_timestamp(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                epoch_ns_from_int(i128::from(integer))
            } else if let Some(unsigned) = number.as_u64() {
                epoch_ns_from_int(i128::from(unsigned))
            } else {
                parse_float_timestamp(number.as_f64()?)
            }
        }
        Value::String(text) => {
            let text = text.trim();
            if text.is_empty() {
                return None;
            }
            let unsigned = text.strip_prefix('-').unwrap_or(text);
            if !unsigned.is_empty() && unsigned.bytes().all(|b| b.is_ascii_digit()) {
                return epoch_ns_from_int(text.parse::<i128>().ok()?);
            }
            parse_rfc3339(text).or_else(|| parse_float_timestamp(text.parse::<f64>().ok()?))
        }
        _ => None,
    }
}

/// Civil date from days since 1970-01-01 (Howard Hinnant's `civil_from_days`).
const fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

/// `YYYY-MM-DDTHH:MM:SS.mmmZ` in UTC, or `""` for `None`.
pub(crate) fn format_timestamp(ns: Option<i64>) -> String {
    let Some(ns) = ns else {
        return String::new();
    };
    let seconds = ns.div_euclid(1_000_000_000);
    let remainder = ns.rem_euclid(1_000_000_000);
    let days = seconds.div_euclid(86_400);
    let of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        of_day / 3600,
        of_day % 3600 / 60,
        of_day % 60,
        remainder / 1_000_000
    )
}

pub(crate) fn format_duration(seconds: f64) -> String {
    let whole = truncate(seconds.round_ties_even());
    let days = whole.div_euclid(86_400);
    let rest = whole.rem_euclid(86_400);
    let (hours, minutes, secs) = (rest / 3600, rest % 3600 / 60, rest % 60);
    if days != 0 {
        format!("{days}d {hours:02}h {minutes:02}m")
    } else if hours != 0 {
        format!("{hours}h {minutes:02}m {secs:02}s")
    } else if minutes != 0 {
        format!("{minutes}m {secs:02}s")
    } else {
        format!("{secs}s")
    }
}

/// Binary units with one decimal, `"<n> B"` under 1 KiB.
pub(crate) fn human_bytes(count: f64) -> String {
    if count.abs() < 1024.0 {
        return format!("{} B", truncate(count));
    }
    let mut value = count;
    for unit in ["KiB", "MiB", "GiB", "TiB", "PiB"] {
        value /= 1024.0;
        if value.abs() < 1024.0 || unit == "PiB" {
            return format!("{value:.1} {unit}");
        }
    }
    unreachable!("the loop returns at PiB")
}

// --------------------------------------------------------------------------
// Lines.

/// What one log line turned out to be.
#[derive(Debug)]
pub(crate) enum Line {
    Blank,
    NonJson,
    NonProvenance,
    Provenance {
        record: Map<String, Value>,
        raw_timestamp: Option<Value>,
    },
}

/// Classifies one log line. A provenance event is either `NL_LOG=json`
/// output (fields under `fields`, target `nativelink_provenance`) or a flat
/// object with `event` and `schema` at the top level or under `attributes`.
pub(crate) fn classify_line(line: &str) -> Line {
    let mut text = line.trim();
    if text.is_empty() {
        return Line::Blank;
    }
    if !text.starts_with(['{', '[']) {
        let Some(brace) = text.find('{') else {
            return Line::NonJson;
        };
        text = &text[brace..];
    }
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return Line::NonJson;
    };
    let Value::Object(mut obj) = value else {
        return Line::NonProvenance;
    };

    let mut target = obj.get("target").cloned();
    let raw_timestamp = TIMESTAMP_KEYS.iter().find_map(|key| obj.get(*key)).cloned();
    let record = match obj.remove("fields") {
        Some(Value::Object(fields)) => fields,
        other => {
            if let Some(fields) = other {
                obj.insert("fields".to_string(), fields);
            }
            match obj.get("attributes") {
                Some(Value::Object(attributes)) if attributes.contains_key("event") => {
                    let attributes = attributes.clone();
                    if matches!(target, None | Some(Value::Null)) {
                        target = attributes.get("target").cloned();
                    }
                    obj.extend(attributes);
                    obj
                }
                _ => obj,
            }
        }
    };

    let is_provenance_target = match &target {
        None | Some(Value::Null) => false,
        Some(Value::String(name)) if name.is_empty() => false,
        Some(Value::String(name)) if name == PROVENANCE_TARGET => true,
        Some(_) => return Line::NonProvenance,
    };
    if !(is_provenance_target || (record.contains_key("event") && record.contains_key("schema"))) {
        return Line::NonProvenance;
    }
    Line::Provenance {
        record,
        raw_timestamp,
    }
}

/// Wraps a reader, gunzipping it if it starts with the gzip magic bytes.
pub(crate) fn maybe_gunzip<'a>(reader: impl Read + 'a) -> io::Result<Box<dyn BufRead + 'a>> {
    let mut buffered = BufReader::new(reader);
    let head = buffered.fill_buf()?;
    if head.starts_with(&GZIP_MAGIC) {
        Ok(Box::new(BufReader::new(MultiGzDecoder::new(buffered))))
    } else {
        Ok(Box::new(buffered))
    }
}

/// Calls `each` with every line of `reader`, decoded as UTF-8 with
/// replacement characters for invalid bytes.
pub(crate) fn for_each_line(
    reader: &mut dyn BufRead,
    mut each: impl FnMut(&str),
) -> io::Result<()> {
    let mut buffer = Vec::new();
    loop {
        buffer.clear();
        if reader.read_until(b'\n', &mut buffer)? == 0 {
            return Ok(());
        }
        each(&String::from_utf8_lossy(&buffer));
    }
}

/// Reads a digest inventory: returns the hashes, and the number of lines
/// that are not digests.
///
/// Each line's first comma- or space-separated field is read, after dropping
/// a `#` comment and anything up to the last `/`, as `<hash>-<size>` or
/// `<hash>`.
pub(crate) fn parse_seed(reader: &mut dyn BufRead) -> io::Result<(BTreeSet<String>, usize)> {
    let mut hashes = BTreeSet::new();
    let mut bad = 0;
    for_each_line(reader, |line| {
        let text = line.split('#').next().unwrap_or("").trim();
        if text.is_empty() {
            return;
        }
        let token = text
            .split(|c: char| c.is_whitespace() || c == ',')
            .next()
            .unwrap_or("")
            .trim_matches(['"', '\'']);
        let token = token.rsplit('/').next().unwrap_or(token);
        let candidate = digest_hash(token);
        if candidate.len() >= 16 && candidate.bytes().all(|b| b.is_ascii_hexdigit()) {
            hashes.insert(candidate);
        } else {
            bad += 1;
        }
    })?;
    Ok((hashes, bad))
}
