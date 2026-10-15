#!/usr/bin/env python3
"""Summarise NativeLink output provenance events (schema 1).

NativeLink emits one DEBUG event on the `nativelink_provenance` tracing target
for each execution attempt, each output it hashes, each upload decision, each
CAS write the server accepts and each Azure Blob write. This tool reads those
events from log files and answers, weighted by bytes:

  1. Which producers (target, mnemonic, output path) account for the output
     bytes first observed in the window.
 1b. Which targets the client's own uploads came from, by the request
     metadata on `cas_write`. This covers actions the client ran locally,
     which no worker saw.
  2. Which action digests produced more than one output manifest.
  3. For each logical producer, whether consecutive complete attempts changed
     their declared identity, their output, both, or neither. Reports 2 and 3
     only use attempts whose outcome is `completed` and whose outputs were all
     uploaded.
  4. How many output bytes each invocation added that no earlier one had.
  5. How many CAS and backend write bytes were first writes, and how many
     re-wrote a digest or key already written; for the backend, how many of
     those rewrites returned an Azure version ID, so the account kept the
     copy they replaced.
  6. The distribution of the worker's `has()` results, and what was uploaded
     after each.
  7. How complete the log is: attempts that never finished, incomplete
     uploads, and events with a schema this tool does not read.

Usage:

  analyze_provenance.py [--seed-digests FILE] [--top N] [--json] [--wide]
                        [FILE ...]

Each FILE is a log, plain or gzip-compressed (detected from its first bytes,
not its name). With no FILE, or with `-`, it reads stdin. Collect logs from
every worker and every server process for the same window and pass them all;
the analysis deduplicates by digest across the whole set.

Two line shapes are accepted:

  * NativeLink's `NL_LOG=json` output, where the event's fields are nested
    under `"fields"` and `"target"` is `"nativelink_provenance"`.
  * A flat JSON object with `event` and `schema` at the top level, which is
    what an OTLP log export usually becomes once flattened. A nested
    `"attributes"` object holding `event` is also read.

Text before the first `{` on a line (a `kubectl logs --timestamps` or Docker
Compose prefix) is ignored. Lines that are not JSON, and JSON lines that are
not provenance events, are skipped and counted. Events with a schema other
than 1 are skipped, counted and reported, because a schema bump means a field
changed meaning.

`--seed-digests FILE` names a list of digests that already existed before the
window, one `<hash>-<size>` or `<hash>` per line (a blob inventory export
works; text up to the last `/` on a line is dropped, as is anything after the
first space or comma, and `#` starts a comment). Seeded digests never count as
first observed or new, and the first write of one in the window is reported
as a rewrite of a pre-window blob.

`--top N` limits each ranked table to N rows (default 20, 0 for all). The
per-invocation novelty table is chronological and is never truncated.

`--json` prints one JSON document, one key per analysis, with raw byte counts.
The text report humanises sizes and shortens digests and paths; `--wide`
prints them in full.

Python 3.10 or newer, standard library only.
"""

from __future__ import annotations

import argparse
import calendar
import datetime
import gzip
import io
import json
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, NamedTuple

PROVENANCE_TARGET = "nativelink_provenance"
SUPPORTED_SCHEMA = 1

ATTEMPT_STARTED = "attempt_started"
OUTPUT_OBSERVED = "output_observed"
OUTPUT_UPLOAD = "output_upload"
ATTEMPT_FINISHED = "attempt_finished"
CAS_WRITE = "cas_write"
FIND_MISSING = "find_missing"
BACKEND_WRITE = "backend_write"
KNOWN_EVENTS = (
    ATTEMPT_STARTED,
    OUTPUT_OBSERVED,
    OUTPUT_UPLOAD,
    ATTEMPT_FINISHED,
    CAS_WRITE,
    FIND_MISSING,
    BACKEND_WRITE,
)

# The output kinds that make up an attempt's manifest. stdout, stderr and the
# Tree/Directory index of an output directory are uploaded too, but they are
# not part of the manifest fingerprint, so churn and exact-identity comparisons
# leave them out.
MANIFEST_KINDS = frozenset(("file", "directory_file", "symlink"))

DECLARED_CHANGE_OUTPUT_CHANGE = "declared_change_output_change"
DECLARED_CHANGE_OUTPUT_SAME = "declared_change_output_same"
SAME_ACTION_OUTPUT_CHANGE = "same_action_output_change"
STABLE = "stable"
CHURN_CLASSES = (
    DECLARED_CHANGE_OUTPUT_CHANGE,
    DECLARED_CHANGE_OUTPUT_SAME,
    SAME_ACTION_OUTPUT_CHANGE,
    STABLE,
)

# Sort key for an event with no usable timestamp: after everything else, in
# input order.
NO_TIMESTAMP = 1 << 62

TIMESTAMP_KEYS = (
    "timestamp",
    "time",
    "@timestamp",
    "ts",
    "timeUnixNano",
    "time_unix_nano",
    "observedTimeUnixNano",
    "observed_time_unix_nano",
)

NO_INVOCATION = "(none)"

# --------------------------------------------------------------------------
# Field normalisation. A flattened OTLP export may turn numbers and booleans
# into strings, and an emitter bug may leave a field out, so every read goes
# through one of these.


def as_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def as_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (bool, int)):
        return int(value)
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            try:
                return int(float(text))
            except ValueError:
                return 0
    return 0


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes")
    return False


def as_schema(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def digest_hash(digest: str) -> str:
    """The hash part of a `<hash>-<size>` digest, lowercased."""
    text = digest.strip()
    if not text:
        return ""
    head, sep, tail = text.rpartition("-")
    if sep and head and tail.isdigit():
        return head.lower()
    return text.lower()


_KEY_DIGEST_RE = re.compile(r"([0-9a-fA-F]{16,})-(\d+)$")


def split_backend_key(key: str) -> tuple[str, str]:
    """Splits a backend key into (prefix, digest hash).

    The Azure store writes `<blob_prefix><hash>-<size>`, so the prefix is what
    separates, for example, a CAS from an AC sharing one container.
    """
    match = _KEY_DIGEST_RE.search(key)
    if match is None:
        return "", ""
    return key[: match.start()], match.group(1).lower()


# --------------------------------------------------------------------------
# Timestamps, as integer nanoseconds since the epoch so that ordering is exact.

_ISO_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})"
    r"(?:[.,](\d+))?\s*([Zz]|[+-]\d{2}:?\d{2})?$"
)
_INT_RE = re.compile(r"^-?\d+$")


def _epoch_ns_from_int(value: int) -> int:
    magnitude = abs(value)
    if magnitude >= 10**17:
        return value
    if magnitude >= 10**14:
        return value * 1_000
    if magnitude >= 10**11:
        return value * 1_000_000
    return value * 1_000_000_000


def parse_timestamp(value: Any) -> int | None:
    """Parses an RFC 3339 string or a Unix time (s, ms, us or ns) to ns."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return _epoch_ns_from_int(value)
    if isinstance(value, float):
        if abs(value) >= 10**11:
            return _epoch_ns_from_int(int(value))
        whole = int(value)
        return whole * 1_000_000_000 + round((value - whole) * 1e9)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if _INT_RE.match(text):
        return _epoch_ns_from_int(int(text))
    match = _ISO_RE.match(text)
    if match is None:
        try:
            number = float(text)
        except ValueError:
            return None
        return parse_timestamp(number)
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    try:
        seconds = calendar.timegm((year, month, day, hour, minute, second, 0, 0, 0))
    except (ValueError, OverflowError):
        return None
    fraction = match.group(7) or ""
    nanos = int(fraction[:9].ljust(9, "0")) if fraction else 0
    zone = match.group(8)
    if zone and zone not in ("Z", "z"):
        sign = 1 if zone[0] == "+" else -1
        digits = zone[1:].replace(":", "")
        seconds -= sign * (int(digits[:2]) * 3600 + int(digits[2:4]) * 60)
    return seconds * 1_000_000_000 + nanos


def format_timestamp(ns: int | None) -> str:
    if ns is None:
        return ""
    seconds, remainder = divmod(ns, 1_000_000_000)
    moment = datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{remainder // 1_000_000:03d}Z"


def format_duration(seconds: float) -> str:
    whole = int(round(seconds))
    days, whole = divmod(whole, 86400)
    hours, whole = divmod(whole, 3600)
    minutes, secs = divmod(whole, 60)
    if days:
        return f"{days}d {hours:02d}h {minutes:02d}m"
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def human_bytes(count: int | float) -> str:
    value = float(count)
    if abs(value) < 1024:
        return f"{int(value)} B"
    for unit in ("KiB", "MiB", "GiB", "TiB", "PiB"):
        value /= 1024
        if abs(value) < 1024 or unit == "PiB":
            return f"{value:.1f} {unit}"
    return f"{int(count)} B"


# --------------------------------------------------------------------------
# Records kept in memory. Tuples and slotted classes, with interned strings,
# because a day of a busy fleet is tens of millions of output events.


class Meta(NamedTuple):
    target: str
    mnemonic: str
    config: str
    invocation: str


EMPTY_META = Meta("", "", "", "")


class Output(NamedTuple):
    order: tuple[int, int]
    attempt_id: str
    kind: str
    path: str
    hash: str
    size: int
    first_in_attempt: bool
    symlink_target: str
    action: str
    meta: Meta | None


class Upload(NamedTuple):
    order: tuple[int, int]
    attempt_id: str
    hash: str
    size: int
    has_result: str
    has_error: str
    disposition: str
    has_elapsed_ms: int


class Write(NamedTuple):
    order: tuple[int, int]
    group: tuple[str, ...]
    ident: str
    display: str
    size: int
    ok: bool
    error_code: str
    invocation: str
    seed_hash: str
    target: str
    mnemonic: str
    versioned: bool


class Attempt:
    __slots__ = (
        "attempt_id",
        "started",
        "start_order",
        "first_order",
        "operation_id",
        "worker_id",
        "action",
        "command",
        "input_root",
        "meta",
        "finished",
        "outcome",
        "outputs_complete",
        "output_count",
        "fingerprint",
    )

    def __init__(self, attempt_id: str) -> None:
        self.attempt_id = attempt_id
        self.started = False
        self.start_order: tuple[int, int] | None = None
        self.first_order: tuple[int, int] = (NO_TIMESTAMP, 1 << 62)
        self.operation_id = ""
        self.worker_id = ""
        self.action = ""
        self.command = ""
        self.input_root = ""
        self.meta: Meta | None = None
        self.finished = False
        self.outcome = ""
        self.outputs_complete = False
        self.output_count = 0
        self.fingerprint = ""

    @property
    def order(self) -> tuple[int, int]:
        return self.start_order if self.start_order is not None else self.first_order

    def touch(self, order: tuple[int, int]) -> None:
        if order < self.first_order:
            self.first_order = order


@dataclass
class InputStats:
    files: list[str] = field(default_factory=list)
    lines: int = 0
    non_json: int = 0
    non_provenance: int = 0
    provenance: int = 0
    unknown_schema: Counter = field(default_factory=Counter)
    unknown_events: Counter = field(default_factory=Counter)
    events: Counter = field(default_factory=Counter)
    missing_timestamp: int = 0


@dataclass
class FindMissingStats:
    requests: int = 0
    errors: int = 0
    requested_count: int = 0
    requested_bytes: int = 0
    missing_count: int = 0
    missing_bytes: int = 0


# --------------------------------------------------------------------------
# Reading.


def open_lines(path: str) -> Iterator[str]:
    """Yields the lines of a file or stdin, gunzipping if it starts gzipped."""
    if path == "-":
        raw: Any = sys.stdin.buffer
        owned = False
    else:
        raw = open(path, "rb")
        owned = True
    try:
        stream: Any = raw if hasattr(raw, "peek") else io.BufferedReader(raw)
        if stream.peek(2)[:2] == b"\x1f\x8b":
            stream = gzip.GzipFile(fileobj=stream)
        for line in stream:
            yield line.decode("utf-8", errors="replace")
    finally:
        if owned:
            raw.close()


def classify_line(line: str) -> tuple[str, dict[str, Any] | None, Any]:
    """Returns (status, record, raw timestamp) for one log line.

    Status is `blank`, `non_json`, `non_provenance` or `provenance`.
    """
    text = line.strip()
    if not text:
        return "blank", None, None
    if text[0] not in "{[":
        brace = text.find("{")
        if brace < 0:
            return "non_json", None, None
        text = text[brace:]
    try:
        obj = json.loads(text)
    except ValueError:
        return "non_json", None, None
    if not isinstance(obj, dict):
        return "non_provenance", None, None

    target = obj.get("target")
    fields = obj.get("fields")
    if isinstance(fields, dict):
        record = fields
    else:
        record = obj
        attributes = obj.get("attributes")
        if isinstance(attributes, dict) and "event" in attributes:
            record = {**obj, **attributes}
            if target is None:
                target = attributes.get("target")

    if target not in (None, "", PROVENANCE_TARGET):
        return "non_provenance", None, None
    if target != PROVENANCE_TARGET and not ("event" in record and "schema" in record):
        return "non_provenance", None, None

    raw_ts = None
    for key in TIMESTAMP_KEYS:
        if key in obj:
            raw_ts = obj[key]
            break
    return "provenance", record, raw_ts


def parse_seed_file(path: str) -> tuple[set[str], int]:
    """Reads a digest inventory. Returns (hashes, count of lines that are not digests)."""
    hashes: set[str] = set()
    bad = 0
    hash_re = re.compile(r"^[0-9a-fA-F]{16,}$")
    for line in open_lines(path):
        text = line.split("#", 1)[0].strip()
        if not text:
            continue
        token = re.split(r"[\s,]+", text, maxsplit=1)[0].strip("\"'")
        token = token.rsplit("/", 1)[-1]
        candidate = digest_hash(token)
        if hash_re.match(candidate):
            hashes.add(candidate)
        else:
            bad += 1
    return hashes, bad


# --------------------------------------------------------------------------
# Collection.


class Collector:
    """Turns provenance records into the compact state the analyses need."""

    def __init__(self) -> None:
        self.stats = InputStats()
        self.attempts: dict[str, Attempt] = {}
        self.outputs: list[Output] = []
        self.uploads: list[Upload] = []
        self.cas_writes: list[Write] = []
        self.backend_writes: list[Write] = []
        self.find_missing = FindMissingStats()
        # backend_write events that carry a version_id key at all, empty or
        # not. Zero means the logs predate the field, not that nothing is kept.
        self.version_id_reported = 0
        self.first_ts: int | None = None
        self.last_ts: int | None = None
        self._seq = 0
        self._strings: dict[str, str] = {}
        self._metas: dict[Meta, Meta] = {}

    def _intern(self, value: Any) -> str:
        text = as_str(value)
        return self._strings.setdefault(text, text)

    def _meta(self, record: dict[str, Any]) -> Meta | None:
        meta = Meta(
            self._intern(record.get("target_id")),
            self._intern(record.get("action_mnemonic")),
            self._intern(record.get("configuration_id")),
            self._intern(record.get("tool_invocation_id")),
        )
        if meta == EMPTY_META:
            return None
        return self._metas.setdefault(meta, meta)

    def _attempt(self, attempt_id: str) -> Attempt:
        attempt = self.attempts.get(attempt_id)
        if attempt is None:
            attempt = Attempt(attempt_id)
            self.attempts[attempt_id] = attempt
        return attempt

    def read(self, path: str) -> None:
        self.stats.files.append(path)
        for line in open_lines(path):
            self.feed_line(line)

    def feed_line(self, line: str) -> None:
        status, record, raw_ts = classify_line(line)
        if status == "blank":
            return
        self.stats.lines += 1
        if status == "non_json":
            self.stats.non_json += 1
            return
        if status == "non_provenance" or record is None:
            self.stats.non_provenance += 1
            return

        schema = as_schema(record.get("schema"))
        if schema != SUPPORTED_SCHEMA:
            self.stats.unknown_schema["missing" if schema is None else str(schema)] += 1
            return
        kind = as_str(record.get("event"))
        if kind not in KNOWN_EVENTS:
            self.stats.unknown_events[kind or "(empty)"] += 1
            return

        ts = parse_timestamp(raw_ts)
        self._seq += 1
        if ts is None:
            self.stats.missing_timestamp += 1
            order = (NO_TIMESTAMP, self._seq)
        else:
            order = (ts, self._seq)
            if self.first_ts is None or ts < self.first_ts:
                self.first_ts = ts
            if self.last_ts is None or ts > self.last_ts:
                self.last_ts = ts
        self.stats.provenance += 1
        self.stats.events[kind] += 1
        getattr(self, "_on_" + kind)(record, order)

    def _on_attempt_started(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        attempt = self._attempt(self._intern(record.get("attempt_id")))
        attempt.touch(order)
        if attempt.started and attempt.start_order is not None and attempt.start_order <= order:
            return
        attempt.started = True
        attempt.start_order = order
        attempt.operation_id = self._intern(record.get("operation_id"))
        attempt.worker_id = self._intern(record.get("worker_id"))
        attempt.action = self._intern(record.get("action_digest")) or attempt.action
        attempt.command = self._intern(record.get("command_digest"))
        attempt.input_root = self._intern(record.get("input_root_digest"))
        attempt.meta = self._meta(record) or attempt.meta

    def _on_output_observed(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        attempt_id = self._intern(record.get("attempt_id"))
        attempt = self._attempt(attempt_id)
        attempt.touch(order)
        action = self._intern(record.get("action_digest"))
        if action and not attempt.action:
            attempt.action = action
        self.outputs.append(
            Output(
                order=order,
                attempt_id=attempt_id,
                kind=self._intern(record.get("kind")),
                path=self._intern(record.get("path")),
                hash=self._intern(digest_hash(as_str(record.get("digest")))),
                size=as_int(record.get("size_bytes")),
                first_in_attempt=as_bool(record.get("first_in_attempt")),
                symlink_target=self._intern(record.get("symlink_target")),
                action=action,
                meta=self._meta(record),
            )
        )

    def _on_output_upload(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        attempt_id = self._intern(record.get("attempt_id"))
        self._attempt(attempt_id).touch(order)
        self.uploads.append(
            Upload(
                order=order,
                attempt_id=attempt_id,
                hash=self._intern(digest_hash(as_str(record.get("digest")))),
                size=as_int(record.get("size_bytes")),
                has_result=self._intern(record.get("has_result")) or "(empty)",
                has_error=self._intern(record.get("has_error")),
                disposition=self._intern(record.get("disposition")) or "(empty)",
                has_elapsed_ms=as_int(record.get("has_elapsed_ms")),
            )
        )

    def _on_attempt_finished(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        attempt = self._attempt(self._intern(record.get("attempt_id")))
        attempt.touch(order)
        attempt.finished = True
        action = self._intern(record.get("action_digest"))
        if action and not attempt.action:
            attempt.action = action
        attempt.outcome = self._intern(record.get("outcome")) or "(empty)"
        attempt.outputs_complete = as_bool(record.get("outputs_complete"))
        attempt.output_count = as_int(record.get("output_count"))
        attempt.fingerprint = self._intern(record.get("manifest_fingerprint"))
        if attempt.meta is None:
            attempt.meta = self._meta(record)

    def _on_cas_write(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        digest = self._intern(record.get("digest"))
        hashed = digest_hash(digest)
        self.cas_writes.append(
            Write(
                order=order,
                group=(self._intern(record.get("source")) or "(empty)",),
                ident=self._intern(hashed),
                display=digest,
                size=as_int(record.get("size_bytes")),
                ok=as_str(record.get("result")) == "ok",
                error_code=self._intern(record.get("error_code")),
                invocation=self._intern(record.get("tool_invocation_id")),
                seed_hash=hashed,
                target=self._intern(record.get("target_id")),
                mnemonic=self._intern(record.get("action_mnemonic")),
                versioned=False,
            )
        )

    def _on_backend_write(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        if "version_id" in record:
            self.version_id_reported += 1
        key = self._intern(record.get("key"))
        prefix, hashed = split_backend_key(key)
        self.backend_writes.append(
            Write(
                order=order,
                group=(self._intern(record.get("store")) or "(empty)", self._intern(prefix)),
                ident=key,
                display=key,
                size=as_int(record.get("size_bytes")),
                ok=as_str(record.get("result")) == "ok",
                error_code=self._intern(record.get("error_code")),
                invocation=self._intern(record.get("tool_invocation_id")),
                seed_hash=hashed,
                target=self._intern(record.get("target_id")),
                mnemonic=self._intern(record.get("action_mnemonic")),
                # Azure's x-ms-version-id. Logs from before the field existed
                # have no key at all, which reads the same as "".
                versioned=bool(as_str(record.get("version_id"))),
            )
        )

    def _on_find_missing(self, record: dict[str, Any], order: tuple[int, int]) -> None:
        stats = self.find_missing
        stats.requests += 1
        if as_str(record.get("result")) not in ("ok", ""):
            stats.errors += 1
        stats.requested_count += as_int(record.get("requested_count"))
        stats.requested_bytes += as_int(record.get("requested_bytes"))
        stats.missing_count += as_int(record.get("missing_count"))
        stats.missing_bytes += as_int(record.get("missing_bytes"))


# --------------------------------------------------------------------------
# Analyses. Each returns a JSON-serialisable dict or list.


FIRST_WRITE = "first_write"
REWRITE = "rewrite"
PREWINDOW_REWRITE = "prewindow_rewrite"
FAILED_WRITE = "failed"


def classify_writes(writes: list[Write], seed: set[str]) -> list[tuple[Write, str]]:
    """Labels each write, in time order, as one of the classes above.

    A successful write is a rewrite if the same digest or key was written
    successfully earlier in the window, a pre-window rewrite if it is the first
    in the window but the seed says the digest already existed, and a first
    write otherwise.
    """
    written: set[str] = set()
    labelled = []
    for write in sorted(writes, key=lambda w: w.order):
        if not write.ok:
            labelled.append((write, FAILED_WRITE))
        elif write.ident in written:
            labelled.append((write, REWRITE))
        else:
            written.add(write.ident)
            if write.seed_hash and write.seed_hash in seed:
                labelled.append((write, PREWINDOW_REWRITE))
            else:
                labelled.append((write, FIRST_WRITE))
    return labelled


NO_METADATA = "(no metadata)"


def limit(rows: list[Any], top: int) -> list[Any]:
    return rows if top <= 0 else rows[:top]


def percentile(values: list[int], pct: int) -> int:
    """Nearest-rank percentile (`pct` in 1..100); 0 for an empty list."""
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, (pct * len(ordered) + 99) // 100)
    return ordered[min(rank, len(ordered)) - 1]


class Analysis:
    def __init__(self, collector: Collector, seed: set[str], top: int) -> None:
        self.c = collector
        self.seed = seed
        self.top = top
        self.outputs = sorted(collector.outputs, key=lambda o: o.order)
        self.by_attempt: dict[str, list[Output]] = defaultdict(list)
        for output in self.outputs:
            self.by_attempt[output.attempt_id].append(output)

    def meta_of(self, output: Output) -> Meta:
        if output.meta is not None:
            return output.meta
        attempt = self.c.attempts.get(output.attempt_id)
        if attempt is not None and attempt.meta is not None:
            return attempt.meta
        return EMPTY_META

    def complete_attempts(self) -> list[Attempt]:
        """Attempts whose manifest can be trusted: the command ran to the end
        and every output was uploaded. A worker can report outputs_complete
        for an attempt it killed or timed out, if it uploaded what was there,
        so the outcome has to be `completed` too."""
        return [
            attempt
            for attempt in self.c.attempts.values()
            if attempt.finished and attempt.outputs_complete and attempt.outcome == "completed"
        ]

    @staticmethod
    def producer_path(output: Output) -> str:
        return output.path or f"<{output.kind or 'unknown'}>"

    # 1 ------------------------------------------------------------------

    def producers(self) -> dict[str, Any]:
        seen = set(self.seed)
        stats: dict[tuple[str, str, str], dict[str, Any]] = {}
        owner: dict[tuple[str, str], tuple[tuple[str, str, str], bool]] = {}
        total_first = 0
        total_first_digests = 0
        total_observed = 0
        for output in self.outputs:
            meta = self.meta_of(output)
            key = (meta.target, meta.mnemonic, self.producer_path(output))
            row = stats.get(key)
            if row is None:
                row = {
                    "attempts": set(),
                    "actions": set(),
                    "inputs": set(),
                    "commands": set(),
                    "digests": set(),
                    "kinds": set(),
                    "first_observed_bytes": 0,
                    "first_observed_digests": 0,
                    "observed_bytes": 0,
                    "observations": 0,
                    "dispositions": Counter(),
                }
                stats[key] = row
            attempt = self.c.attempts.get(output.attempt_id)
            row["attempts"].add(output.attempt_id)
            row["kinds"].add(output.kind)
            action = output.action or (attempt.action if attempt else "")
            if action:
                row["actions"].add(action)
            if attempt is not None:
                if attempt.input_root:
                    row["inputs"].add(attempt.input_root)
                if attempt.command:
                    row["commands"].add(attempt.command)
            row["observations"] += 1
            row["observed_bytes"] += output.size
            total_observed += output.size
            if output.hash:
                row["digests"].add(output.hash)
                if output.hash not in seen:
                    seen.add(output.hash)
                    row["first_observed_bytes"] += output.size
                    row["first_observed_digests"] += 1
                    total_first += output.size
                    total_first_digests += 1
                # The upload event names a digest, not a path. Credit it to the
                # path the worker deduplicated it under (`first_in_attempt`), or
                # the earliest one if that flag is missing everywhere.
                upload_key = (output.attempt_id, output.hash)
                held = owner.get(upload_key)
                if held is None or (output.first_in_attempt and not held[1]):
                    owner[upload_key] = (key, output.first_in_attempt)

        unmatched_uploads = 0
        for upload in self.c.uploads:
            held = owner.get((upload.attempt_id, upload.hash))
            if held is None:
                unmatched_uploads += 1
                continue
            stats[held[0]]["dispositions"][upload.disposition] += 1

        rows = []
        for (target, mnemonic, path), row in stats.items():
            rows.append(
                {
                    "target_id": target,
                    "action_mnemonic": mnemonic,
                    "path": path,
                    "kinds": sorted(row["kinds"]),
                    "attempts": len(row["attempts"]),
                    "distinct_action_digests": len(row["actions"]),
                    "distinct_input_root_digests": len(row["inputs"]),
                    "distinct_command_digests": len(row["commands"]),
                    "distinct_output_digests": len(row["digests"]),
                    "first_observed_bytes": row["first_observed_bytes"],
                    "first_observed_digests": row["first_observed_digests"],
                    "observed_bytes": row["observed_bytes"],
                    "observations": row["observations"],
                    "dispositions": dict(sorted(row["dispositions"].items())),
                }
            )
        rows.sort(
            key=lambda r: (
                -r["first_observed_bytes"],
                -r["observed_bytes"],
                r["target_id"],
                r["action_mnemonic"],
                r["path"],
            )
        )
        return {
            "producer_count": len(rows),
            "first_observed_bytes": total_first,
            "first_observed_digests": total_first_digests,
            "observed_bytes": total_observed,
            "observations": len(self.outputs),
            "uploads_without_output_observed": unmatched_uploads,
            "rows": limit(rows, self.top),
        }

    # 1b -----------------------------------------------------------------

    def client_uploads(self) -> dict[str, Any]:
        """Successful cas_write events grouped by the target in their metadata.

        Bazel attaches RequestMetadata to its own uploads, so outputs of
        actions it ran locally are attributable here although no worker saw
        them. Uploads without metadata, such as a worker's own uploads through
        a GrpcStore, form one row.
        """
        worker_events = any(attempt.started for attempt in self.c.attempts.values())
        executed = {
            (attempt.meta.target, attempt.meta.mnemonic)
            for attempt in self.c.attempts.values()
            if attempt.meta is not None
        }
        rows: dict[tuple[str, str], dict[str, Any]] = {}
        for write, label in classify_writes(self.c.cas_writes, self.seed):
            if label == FAILED_WRITE:
                continue
            key = (write.target, write.mnemonic) if write.target else (NO_METADATA, "")
            row = rows.get(key)
            if row is None:
                row = {
                    "writes": 0,
                    "bytes": 0,
                    "digests": set(),
                    "invocations": set(),
                    FIRST_WRITE: 0,
                    REWRITE: 0,
                    PREWINDOW_REWRITE: 0,
                }
                rows[key] = row
            row["writes"] += 1
            row["bytes"] += write.size
            row[label] += write.size
            row["digests"].add(write.ident)
            if write.invocation:
                row["invocations"].add(write.invocation)

        out = []
        for (target, mnemonic), row in rows.items():
            seen = None
            if worker_events and target != NO_METADATA:
                seen = (target, mnemonic) in executed
            out.append(
                {
                    "target_id": target,
                    "action_mnemonic": mnemonic,
                    "seen_by_worker": seen,
                    "writes": row["writes"],
                    "bytes": row["bytes"],
                    "distinct_digests": len(row["digests"]),
                    "first_write_bytes": row[FIRST_WRITE],
                    "rewrite_bytes": row[REWRITE],
                    "prewindow_rewrite_bytes": row[PREWINDOW_REWRITE],
                    "invocations": len(row["invocations"]),
                }
            )
        out.sort(
            key=lambda r: (
                -r["first_write_bytes"],
                -r["bytes"],
                r["target_id"],
                r["action_mnemonic"],
            )
        )
        return {
            "target_count": sum(1 for r in out if r["target_id"] != NO_METADATA),
            "bytes": sum(r["bytes"] for r in out),
            "bytes_without_metadata": sum(r["bytes"] for r in out if r["target_id"] == NO_METADATA),
            "worker_events_present": worker_events,
            "bytes_from_targets_no_worker_ran": sum(
                r["bytes"] for r in out if r["seen_by_worker"] is False
            ),
            "rows": limit(out, self.top),
        }

    # 2 ------------------------------------------------------------------

    def exact_identity(self) -> dict[str, Any]:
        by_action: dict[str, list[Attempt]] = defaultdict(list)
        for attempt in self.complete_attempts():
            if attempt.fingerprint and attempt.action:
                by_action[attempt.action].append(attempt)
        rows = []
        for action, attempts in by_action.items():
            fingerprints = {attempt.fingerprint for attempt in attempts}
            if len(fingerprints) < 2:
                continue
            per_path: dict[str, dict[str, tuple[str, int]]] = defaultdict(dict)
            for attempt in attempts:
                for output in self.by_attempt.get(attempt.attempt_id, ()):
                    if output.kind not in MANIFEST_KINDS:
                        continue
                    per_path[output.path].setdefault(
                        attempt.attempt_id, (output_identity(output), output.size)
                    )
            differing = []
            varying_bytes = 0
            for path, by_attempt in per_path.items():
                identities = {identity for identity, _ in by_attempt.values()}
                if len(identities) > 1 or len(by_attempt) < len(attempts):
                    differing.append(path)
                    varying_bytes += sum(dict(by_attempt.values()).values())
            meta = next((a.meta for a in attempts if a.meta is not None), EMPTY_META)
            invocations = {a.meta.invocation for a in attempts if a.meta and a.meta.invocation}
            rows.append(
                {
                    "action_digest": action,
                    "target_id": meta.target,
                    "action_mnemonic": meta.mnemonic,
                    "complete_attempts": len(attempts),
                    "distinct_fingerprints": len(fingerprints),
                    "invocations": len(invocations),
                    "differing_paths": sorted(differing),
                    "varying_bytes": varying_bytes,
                }
            )
        rows.sort(
            key=lambda r: (-r["varying_bytes"], -r["distinct_fingerprints"], r["action_digest"])
        )
        return {
            "actions_checked": len(by_action),
            "actions_with_multiple_manifests": len(rows),
            "rows": limit(rows, self.top),
        }

    # 3 ------------------------------------------------------------------

    def churn(self) -> dict[str, Any]:
        series: dict[tuple[str, str, str, str], dict[str, tuple[Any, ...]]] = defaultdict(dict)
        for attempt in self.complete_attempts():
            for output in self.by_attempt.get(attempt.attempt_id, ()):
                if output.kind not in MANIFEST_KINDS:
                    continue
                meta = self.meta_of(output)
                key = (meta.target, meta.mnemonic, meta.config, output.path)
                series[key].setdefault(
                    attempt.attempt_id,
                    (
                        attempt.order,
                        attempt.attempt_id,
                        output.action or attempt.action,
                        attempt.command,
                        attempt.input_root,
                        output_identity(output),
                        output.size,
                        meta.invocation,
                    ),
                )

        summary = {name: {"pairs": 0, "bytes": 0, "producers": 0} for name in CHURN_CLASSES}
        components: Counter = Counter()
        rows = []
        for (target, mnemonic, config, path), by_attempt in series.items():
            points = sorted(by_attempt.values())
            pairs = Counter()
            pair_bytes = Counter()
            for previous, current in zip(points, points[1:]):
                declared = declared_differs(previous[2:5], current[2:5])
                output_changed = previous[5] != current[5]
                if declared and output_changed:
                    name = DECLARED_CHANGE_OUTPUT_CHANGE
                elif declared:
                    name = DECLARED_CHANGE_OUTPUT_SAME
                elif output_changed:
                    name = SAME_ACTION_OUTPUT_CHANGE
                else:
                    name = STABLE
                pairs[name] += 1
                pair_bytes[name] += current[6]
                if declared:
                    components[declared_components(previous[2:5], current[2:5])] += 1
            if not pairs:
                continue
            for name in pairs:
                summary[name]["pairs"] += pairs[name]
                summary[name]["bytes"] += pair_bytes[name]
                summary[name]["producers"] += 1
            churn_bytes = sum(pair_bytes[name] for name in CHURN_CLASSES if name != STABLE)
            rows.append(
                {
                    "target_id": target,
                    "action_mnemonic": mnemonic,
                    "configuration_id": config,
                    "path": path,
                    "complete_attempts": len(points),
                    "invocations": len({p[7] for p in points if p[7]}),
                    "pairs": {name: pairs.get(name, 0) for name in CHURN_CLASSES},
                    "bytes": {name: pair_bytes.get(name, 0) for name in CHURN_CLASSES},
                    "churn_bytes": churn_bytes,
                }
            )
        rows.sort(
            key=lambda r: (
                -r["churn_bytes"],
                -sum(r["pairs"].values()),
                r["target_id"],
                r["action_mnemonic"],
                r["configuration_id"],
                r["path"],
            )
        )
        churning = [
            row
            for row in rows
            if row["churn_bytes"] > 0
            or any(row["pairs"][name] for name in CHURN_CLASSES if name != STABLE)
        ]
        return {
            "producers_compared": len(rows),
            "summary": summary,
            "declared_change_components": dict(sorted(components.items())),
            "rows": limit(churning, self.top),
        }

    # 4 ------------------------------------------------------------------

    def novelty(self) -> dict[str, Any]:
        first: dict[str, tuple[int, int]] = {}
        attempts_of: dict[str, set[str]] = defaultdict(set)
        digests_of: dict[str, dict[str, int]] = defaultdict(dict)
        for attempt in self.c.attempts.values():
            meta = attempt.meta
            if meta is None:
                outputs = self.by_attempt.get(attempt.attempt_id)
                meta = outputs[0].meta if outputs and outputs[0].meta else EMPTY_META
            invocation = meta.invocation or NO_INVOCATION
            attempts_of[invocation].add(attempt.attempt_id)
            if invocation not in first or attempt.first_order < first[invocation]:
                first[invocation] = attempt.first_order
        for output in self.outputs:
            invocation = self.meta_of(output).invocation or NO_INVOCATION
            if invocation not in first or output.order < first[invocation]:
                first[invocation] = output.order
            attempts_of[invocation].add(output.attempt_id)
            if output.hash:
                digests_of[invocation].setdefault(output.hash, output.size)
        seen = set(self.seed)
        cumulative = 0
        runs = []
        for invocation in sorted(first, key=lambda inv: (first[inv], inv)):
            digests = digests_of.get(invocation, {})
            new = {h: s for h, s in digests.items() if h not in seen}
            new_bytes = sum(new.values())
            cumulative += new_bytes
            seen.update(digests)
            run_bytes = sum(digests.values())
            order = first[invocation][0]
            runs.append(
                {
                    "invocation": invocation,
                    "first_event": format_timestamp(None if order == NO_TIMESTAMP else order),
                    "attempts": len(attempts_of[invocation]),
                    "distinct_digests": len(digests),
                    "run_bytes": run_bytes,
                    "new_digests": len(new),
                    "new_bytes": new_bytes,
                    "cumulative_new_bytes": cumulative,
                }
            )
        return {"runs": runs, "total_new_bytes": cumulative, "seeded_digests": len(self.seed)}

    # 5 ------------------------------------------------------------------

    def rewrites(self) -> dict[str, Any]:
        backend = self._rewrite_section(
            self.c.backend_writes, ("store", "prefix"), "key", versioning=True
        )
        backend["version_id_reported"] = self.c.version_id_reported
        return {
            "cas_write": self._rewrite_section(self.c.cas_writes, ("source",), "digest"),
            "backend_write": backend,
        }

    def _rewrite_section(
        self,
        writes: list[Write],
        group_names: tuple[str, ...],
        ident_name: str,
        versioning: bool = False,
    ) -> dict[str, Any]:
        distinct_written = 0
        groups: dict[tuple[str, ...], dict[str, Any]] = {}
        idents: dict[str, dict[str, Any]] = {}
        for write, label in classify_writes(writes, self.seed):
            group = groups.get(write.group)
            if group is None:
                group = {
                    "writes": 0,
                    "bytes": 0,
                    "first_writes": 0,
                    "first_write_bytes": 0,
                    "rewrites": 0,
                    "rewrite_bytes": 0,
                    "prewindow_rewrites": 0,
                    "prewindow_rewrite_bytes": 0,
                    "errors": 0,
                    "error_bytes": 0,
                    "error_codes": Counter(),
                }
                if versioning:
                    group["versioned_writes"] = 0
                    group["retained_by_versioning_bytes"] = 0
                groups[write.group] = group
            if label == FAILED_WRITE:
                group["errors"] += 1
                group["error_bytes"] += write.size
                group["error_codes"][write.error_code or "(empty)"] += 1
                continue
            group["writes"] += 1
            group["bytes"] += write.size
            ident = idents.get(write.ident)
            if ident is None:
                ident = {
                    ident_name: write.display,
                    "size_bytes": write.size,
                    "writes": 0,
                    "rewrite_bytes": 0,
                    "prewindow_rewrite_bytes": 0,
                    "invocations": set(),
                    "groups": set(),
                }
                idents[write.ident] = ident
            ident["writes"] += 1
            ident["groups"].add("/".join(part for part in write.group if part))
            if write.invocation:
                ident["invocations"].add(write.invocation)
            if label == REWRITE:
                group["rewrites"] += 1
                group["rewrite_bytes"] += write.size
                ident["rewrite_bytes"] += write.size
            elif label == PREWINDOW_REWRITE:
                distinct_written += 1
                group["prewindow_rewrites"] += 1
                group["prewindow_rewrite_bytes"] += write.size
                ident["prewindow_rewrite_bytes"] += write.size
            else:
                distinct_written += 1
                group["first_writes"] += 1
                group["first_write_bytes"] += write.size
            if versioning and write.versioned:
                # A version ID on an overwrite means the account kept the
                # copy it replaced. Its size is taken to be this write's,
                # which holds for a CAS key.
                group["versioned_writes"] += 1
                if label != FIRST_WRITE:
                    group["retained_by_versioning_bytes"] += write.size

        totals = Counter()
        group_rows = []
        for key, group in sorted(groups.items(), key=lambda item: (-item[1]["bytes"], item[0])):
            row = {name: value for name, value in zip(group_names, key)}
            for name, value in group.items():
                if name == "error_codes":
                    row[name] = dict(sorted(value.items()))
                else:
                    row[name] = value
                    totals[name] += value
            group_rows.append(row)

        top_rows = []
        for ident in idents.values():
            if ident["rewrite_bytes"] == 0 and ident["prewindow_rewrite_bytes"] == 0:
                continue
            top_rows.append(
                {
                    ident_name: ident[ident_name],
                    "size_bytes": ident["size_bytes"],
                    "writes": ident["writes"],
                    "rewrite_bytes": ident["rewrite_bytes"],
                    "prewindow_rewrite_bytes": ident["prewindow_rewrite_bytes"],
                    "invocations": len(ident["invocations"]),
                    "sources": sorted(ident["groups"]),
                }
            )
        top_rows.sort(
            key=lambda r: (
                -(r["rewrite_bytes"] + r["prewindow_rewrite_bytes"]),
                -r["writes"],
                r[ident_name],
            )
        )
        total_keys = (
            "writes",
            "bytes",
            "first_writes",
            "first_write_bytes",
            "rewrites",
            "rewrite_bytes",
            "prewindow_rewrites",
            "prewindow_rewrite_bytes",
            "errors",
            "error_bytes",
        )
        if versioning:
            total_keys += ("versioned_writes", "retained_by_versioning_bytes")
        return {
            "totals": {name: totals.get(name, 0) for name in total_keys},
            "distinct_written": distinct_written,
            "by_group": group_rows,
            "top_rewritten": limit(top_rows, self.top),
        }

    # 6 ------------------------------------------------------------------

    def has_health(self) -> dict[str, Any]:
        rows: dict[str, dict[str, Any]] = {}
        stored: set[str] = set()
        restored: dict[str, dict[str, int]] = defaultdict(lambda: {"uploads": 0, "bytes": 0})
        errors: Counter = Counter()
        for upload in sorted(self.c.uploads, key=lambda u: u.order):
            row = rows.get(upload.has_result)
            if row is None:
                row = {
                    "events": 0,
                    "bytes": 0,
                    "uploaded": 0,
                    "uploaded_bytes": 0,
                    "dispositions": Counter(),
                    "elapsed": [],
                }
                rows[upload.has_result] = row
            row["events"] += 1
            row["bytes"] += upload.size
            row["dispositions"][upload.disposition] += 1
            row["elapsed"].append(upload.has_elapsed_ms)
            if upload.has_error:
                errors[upload.has_error] += 1
            if upload.disposition == "uploaded":
                row["uploaded"] += 1
                row["uploaded_bytes"] += upload.size
                if upload.hash and upload.hash in stored:
                    restored[upload.has_result]["uploads"] += 1
                    restored[upload.has_result]["bytes"] += upload.size
            if upload.hash and upload.disposition in ("uploaded", "skipped_present"):
                stored.add(upload.hash)

        result_rows = []
        for name, row in sorted(rows.items(), key=lambda item: (-item[1]["bytes"], item[0])):
            elapsed = row.pop("elapsed")
            result_rows.append(
                {
                    "has_result": name,
                    "events": row["events"],
                    "bytes": row["bytes"],
                    "uploaded": row["uploaded"],
                    "uploaded_bytes": row["uploaded_bytes"],
                    "dispositions": dict(sorted(row["dispositions"].items())),
                    "has_elapsed_ms_p50": percentile(elapsed, 50),
                    "has_elapsed_ms_p95": percentile(elapsed, 95),
                    "has_elapsed_ms_max": max(elapsed) if elapsed else 0,
                }
            )
        fm = self.c.find_missing
        return {
            "upload_events": len(self.c.uploads),
            "uploaded_bytes": sum(row["uploaded_bytes"] for row in result_rows),
            "by_has_result": result_rows,
            "uploads_of_already_stored_digests": dict(sorted(restored.items())),
            "top_has_errors": [
                {"has_error": text, "count": count}
                for text, count in limit(
                    sorted(errors.items(), key=lambda kv: (-kv[1], kv[0])), self.top
                )
            ],
            "find_missing": {
                "requests": fm.requests,
                "errors": fm.errors,
                "requested_count": fm.requested_count,
                "requested_bytes": fm.requested_bytes,
                "missing_count": fm.missing_count,
                "missing_bytes": fm.missing_bytes,
            },
        }

    # 7 ------------------------------------------------------------------

    def coverage(self) -> dict[str, Any]:
        unfinished = []
        unstarted = 0
        incomplete: Counter = Counter()
        not_completed: Counter = Counter()
        count_mismatch = []
        for attempt in self.c.attempts.values():
            if attempt.started and not attempt.finished:
                unfinished.append(attempt)
            if attempt.finished and not attempt.started:
                unstarted += 1
            if attempt.finished and not attempt.outputs_complete:
                incomplete[attempt.outcome] += 1
            if attempt.finished and attempt.outputs_complete and attempt.outcome != "completed":
                not_completed[attempt.outcome] += 1
            if attempt.finished and attempt.outputs_complete:
                observed = sum(
                    1
                    for output in self.by_attempt.get(attempt.attempt_id, ())
                    if output.kind in MANIFEST_KINDS
                )
                if observed != attempt.output_count:
                    count_mismatch.append(attempt.attempt_id)
        unfinished.sort(key=lambda a: a.order)
        orphan_outputs = sum(
            1 for output in self.outputs if not self.c.attempts[output.attempt_id].started
        )
        no_metadata = sum(1 for output in self.outputs if self.meta_of(output) == EMPTY_META)
        stats = self.c.stats
        return {
            "attempts_seen": len(self.c.attempts),
            "attempts_started": sum(1 for a in self.c.attempts.values() if a.started),
            "attempts_finished": sum(1 for a in self.c.attempts.values() if a.finished),
            "started_without_finished": len(unfinished),
            "started_without_finished_examples": [
                {
                    "attempt_id": a.attempt_id,
                    "operation_id": a.operation_id,
                    "target_id": a.meta.target if a.meta else "",
                    "started": format_timestamp(a.start_order[0] if a.start_order else None),
                }
                for a in limit(unfinished, self.top)
            ],
            "finished_without_started": unstarted,
            "outputs_incomplete": sum(incomplete.values()),
            "outputs_incomplete_by_outcome": dict(sorted(incomplete.items())),
            "complete_but_not_completed": sum(not_completed.values()),
            "complete_but_not_completed_by_outcome": dict(sorted(not_completed.items())),
            "manifest_count_mismatches": len(count_mismatch),
            "manifest_count_mismatch_examples": sorted(limit(count_mismatch, self.top)),
            "outputs_without_attempt_started": orphan_outputs,
            "outputs_without_metadata": no_metadata,
            "unknown_schema": dict(sorted(stats.unknown_schema.items())),
            "unknown_events": dict(sorted(stats.unknown_events.items())),
            "events_without_timestamp": stats.missing_timestamp,
        }

    # -------------------------------------------------------------------

    def report(self) -> dict[str, Any]:
        c = self.c
        duration = None
        if c.first_ts is not None and c.last_ts is not None:
            duration = (c.last_ts - c.first_ts) / 1e9
        stats = c.stats
        return {
            "provenance_schema": SUPPORTED_SCHEMA,
            "window": {
                "first": format_timestamp(c.first_ts),
                "last": format_timestamp(c.last_ts),
                "first_unix_ns": c.first_ts,
                "last_unix_ns": c.last_ts,
                "duration_seconds": duration,
            },
            "inputs": {
                "files": stats.files,
                "lines": stats.lines,
                "provenance_events": stats.provenance,
                "non_json_lines": stats.non_json,
                "non_provenance_lines": stats.non_provenance,
                "unknown_schema_events": sum(stats.unknown_schema.values()),
                "unknown_event_kinds": sum(stats.unknown_events.values()),
                "events_by_kind": {kind: stats.events.get(kind, 0) for kind in KNOWN_EVENTS},
                "seeded_digests": len(self.seed),
            },
            "producers": self.producers(),
            "client_uploads": self.client_uploads(),
            "exact_identity": self.exact_identity(),
            "churn": self.churn(),
            "novelty": self.novelty(),
            "rewrites": self.rewrites(),
            "has_health": self.has_health(),
            "coverage": self.coverage(),
        }


def output_identity(output: Output) -> str:
    if output.kind == "symlink":
        return "symlink:" + output.symlink_target
    return output.hash


def declared_differs(previous: Iterable[str], current: Iterable[str]) -> bool:
    """Whether (action, command, input root) differ in any component both know."""
    return any(a and b and a != b for a, b in zip(previous, current))


def declared_components(previous: tuple[str, ...], current: tuple[str, ...]) -> str:
    """Which declared inputs moved: `command`, `input_root`, both, or `other`.

    The action digest also covers the platform, timeout and salt, so an action
    change with the same command and input root is `other`.
    """
    changed = []
    known = True
    for name, a, b in zip(("command", "input_root"), previous[1:], current[1:]):
        if not a or not b:
            known = False
        elif a != b:
            changed.append(name)
    if changed:
        return "+".join(changed)
    return "other" if known else "unknown"


def analyze(paths: list[str], seed: set[str], top: int) -> dict[str, Any]:
    collector = Collector()
    for path in paths:
        collector.read(path)
    return Analysis(collector, seed, top).report()


# --------------------------------------------------------------------------
# Text rendering.


def table(headers: list[str], rows: list[list[Any]], right: set[int] | None = None) -> str:
    if not rows:
        return "   (none)"
    right = right or set()
    cells = [[str(value) for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in cells:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def line(values: list[str]) -> str:
        parts = []
        for index, value in enumerate(values):
            if index in right:
                parts.append(value.rjust(widths[index]))
            else:
                parts.append(value.ljust(widths[index]))
        return "   " + "  ".join(parts).rstrip()

    out = [line(headers), line(["-" * width for width in widths])]
    out.extend(line(row) for row in cells)
    return "\n".join(out)


class Shortener:
    def __init__(self, wide: bool) -> None:
        self.wide = wide

    def digest(self, digest: str) -> str:
        if self.wide or not digest:
            return digest or "-"
        head, sep, tail = digest.rpartition("-")
        if sep and len(head) > 12:
            return f"{head[:12]}..-{tail}"
        return digest

    def text(self, text: str, width: int = 64) -> str:
        if not text:
            return "-"
        if self.wide or len(text) <= width:
            return text
        keep = width - 3
        return text[: keep // 3] + "..." + text[-(keep - keep // 3) :]

    def key(self, key: str) -> str:
        prefix, hashed = split_backend_key(key)
        if self.wide or not hashed:
            return self.text(key)
        return prefix + self.digest(key[len(prefix) :])


def fmt_counter(counts: dict[str, int]) -> str:
    return " ".join(f"{name}:{count}" for name, count in counts.items()) or "-"


def per_day(count: int, duration: float | None) -> str:
    if not duration or duration <= 0:
        return ""
    return f" ({human_bytes(count * 86400 / duration)}/day at this rate)"


def render_text(report: dict[str, Any], top: int, wide: bool) -> str:
    short = Shortener(wide)
    out: list[str] = []
    window = report["window"]
    inputs = report["inputs"]
    duration = window["duration_seconds"]
    top_note = "all" if top <= 0 else f"top {top}"

    out.append("NativeLink output provenance report (schema %d)" % report["provenance_schema"])
    if window["first"]:
        out.append(
            f"Window: {window['first']} .. {window['last']} ({format_duration(duration or 0)})"
        )
    else:
        out.append("Window: no timestamped provenance events")
    out.append(
        f"Inputs: {len(inputs['files'])} file(s), {inputs['lines']} lines, "
        f"{inputs['provenance_events']} provenance events, "
        f"{inputs['non_json_lines']} non-JSON, {inputs['non_provenance_lines']} other JSON, "
        f"{inputs['unknown_schema_events']} unknown schema, "
        f"{inputs['unknown_event_kinds']} unknown kind"
    )
    out.append("Events: " + fmt_counter({k: v for k, v in inputs["events_by_kind"].items() if v}))
    if inputs["seeded_digests"]:
        out.append(f"Seeded digests (known before the window): {inputs['seeded_digests']}")

    # 1
    producers = report["producers"]
    out.append("")
    out.append(
        "1. Producers by first-observed output bytes "
        f"({len(producers['rows'])} of {producers['producer_count']} shown)"
    )
    out.append(
        f"   first observed: {human_bytes(producers['first_observed_bytes'])} in "
        f"{producers['first_observed_digests']} digests"
        f"{per_day(producers['first_observed_bytes'], duration)}; observed: "
        f"{human_bytes(producers['observed_bytes'])} in {producers['observations']} outputs"
    )
    out.append(
        table(
            [
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
            [
                [
                    short.text(row["target_id"], 40),
                    row["action_mnemonic"] or "-",
                    short.text(row["path"]),
                    row["attempts"],
                    row["distinct_action_digests"],
                    row["distinct_input_root_digests"],
                    row["distinct_command_digests"],
                    row["distinct_output_digests"],
                    human_bytes(row["first_observed_bytes"]),
                    human_bytes(row["observed_bytes"]),
                    fmt_counter(row["dispositions"]),
                ]
                for row in producers["rows"]
            ],
            right={3, 4, 5, 6, 7, 8, 9},
        )
    )

    # 1b
    uploads = report["client_uploads"]
    out.append("")
    out.append(
        "1b. Client uploads by target: cas_write (result=ok) grouped by request metadata "
        f"({sum(1 for r in uploads['rows'] if r['target_id'] != NO_METADATA)} of "
        f"{uploads['target_count']} targets shown)"
    )
    summary = (
        f"   {human_bytes(uploads['bytes'])} uploaded; "
        f"{human_bytes(uploads['bytes_without_metadata'])} without metadata"
    )
    if uploads["worker_events_present"]:
        summary += (
            f"; {human_bytes(uploads['bytes_from_targets_no_worker_ran'])} from targets "
            "no worker attempt in this input produced (worker = no)"
        )
    out.append(summary)
    show_worker = uploads["worker_events_present"]
    out.append(
        table(
            ["target", "mnemonic"]
            + (["worker"] if show_worker else [])
            + ["writes", "bytes", "digests", "first-write", "rewrite", "pre-window", "invocations"],
            [
                [short.text(row["target_id"], 40), row["action_mnemonic"] or "-"]
                + (
                    [{True: "yes", False: "no", None: "-"}[row["seen_by_worker"]]]
                    if show_worker
                    else []
                )
                + [
                    row["writes"],
                    human_bytes(row["bytes"]),
                    row["distinct_digests"],
                    human_bytes(row["first_write_bytes"]),
                    human_bytes(row["rewrite_bytes"]),
                    human_bytes(row["prewindow_rewrite_bytes"]),
                    row["invocations"],
                ]
                for row in uploads["rows"]
            ],
            right=set(range(3 if show_worker else 2, 10 if show_worker else 9)),
        )
    )

    # 2
    exact = report["exact_identity"]
    out.append("")
    out.append(
        "2. Exact identity: action digests with more than one manifest among complete "
        f"attempts ({exact['actions_with_multiple_manifests']} of {exact['actions_checked']})"
    )
    out.append(
        table(
            [
                "action_digest",
                "target",
                "mnemonic",
                "complete",
                "manifests",
                "invocations",
                "varying",
                "differing paths",
            ],
            [
                [
                    short.digest(row["action_digest"]),
                    short.text(row["target_id"], 40),
                    row["action_mnemonic"] or "-",
                    row["complete_attempts"],
                    row["distinct_fingerprints"],
                    row["invocations"],
                    human_bytes(row["varying_bytes"]),
                    f"{len(row['differing_paths'])}: "
                    + ", ".join(short.text(p, 48) for p in row["differing_paths"][:3])
                    + (" ..." if len(row["differing_paths"]) > 3 else ""),
                ]
                for row in exact["rows"]
            ],
            right={3, 4, 5, 6},
        )
    )

    # 3
    churn = report["churn"]
    out.append("")
    out.append(
        "3. Logical producer churn: consecutive complete attempts per (target, mnemonic, "
        f"configuration, path) ({churn['producers_compared']} producers compared)"
    )
    out.append(
        table(
            ["class", "pairs", "bytes", "producers"],
            [
                [name, data["pairs"], human_bytes(data["bytes"]), data["producers"]]
                for name, data in churn["summary"].items()
            ],
            right={1, 2, 3},
        )
    )
    if churn["declared_change_components"]:
        out.append(
            "   declared changes by what moved: " + fmt_counter(churn["declared_change_components"])
        )
    out.append(f"   Producers by churn bytes ({top_note}):")
    out.append(
        table(
            [
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
            [
                [
                    short.text(row["target_id"], 40),
                    row["action_mnemonic"] or "-",
                    short.text(row["configuration_id"], 16),
                    short.text(row["path"]),
                    row["complete_attempts"],
                    row["pairs"][DECLARED_CHANGE_OUTPUT_CHANGE],
                    row["pairs"][DECLARED_CHANGE_OUTPUT_SAME],
                    row["pairs"][SAME_ACTION_OUTPUT_CHANGE],
                    row["pairs"][STABLE],
                    human_bytes(row["churn_bytes"]),
                ]
                for row in churn["rows"]
            ],
            right={4, 5, 6, 7, 8, 9},
        )
    )

    # 4
    novelty = report["novelty"]
    out.append("")
    seeded = (
        f", seeded with {novelty['seeded_digests']} digests" if novelty["seeded_digests"] else ""
    )
    out.append(f"4. Novelty per invocation, in order of first event{seeded}")
    out.append(
        table(
            [
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
            [
                [
                    index,
                    short.text(run["invocation"], 40),
                    run["first_event"] or "-",
                    run["attempts"],
                    run["distinct_digests"],
                    human_bytes(run["run_bytes"]),
                    run["new_digests"],
                    human_bytes(run["new_bytes"]),
                    human_bytes(run["cumulative_new_bytes"]),
                ]
                for index, run in enumerate(novelty["runs"], start=1)
            ],
            right={0, 3, 4, 5, 6, 7, 8},
        )
    )

    # 5
    rewrites = report["rewrites"]
    out.append("")
    out.append("5. Rewrite amplification (result=ok writes; errors counted separately)")
    for section, title, group_cols, ident in (
        ("cas_write", "CAS writes (cas_write) by source", ["source"], "digest"),
        (
            "backend_write",
            "Backend writes (backend_write) by store and key prefix",
            ["store", "prefix"],
            "key",
        ),
    ):
        data = rewrites[section]
        totals = data["totals"]
        versioning = "versioned_writes" in totals
        # Without the field anywhere, "0 versioned" would read as "the account
        # keeps nothing", which these logs cannot say.
        version_known = data.get("version_id_reported", 0) > 0
        out.append(f"   {title}:")
        if totals["writes"] or totals["errors"]:
            out.append(
                f"   total {human_bytes(totals['bytes'])}{per_day(totals['bytes'], duration)}: "
                f"first writes {human_bytes(totals['first_write_bytes'])}, "
                f"rewrites {human_bytes(totals['rewrite_bytes'])}, "
                f"pre-window {human_bytes(totals['prewindow_rewrite_bytes'])}"
                + (
                    f", retained by versioning "
                    f"{human_bytes(totals['retained_by_versioning_bytes'])}"
                    if versioning and version_known
                    else ""
                )
                + (
                    "; versioning unknown (no backend_write carries version_id)"
                    if versioning and not version_known
                    else ""
                )
            )
        out.append(
            table(
                group_cols
                + [
                    "writes",
                    "bytes",
                    "first-write",
                    "rewrite",
                    "pre-window",
                    "rewrite %",
                    "errors",
                    "error bytes",
                ]
                + (["versioned", "retained"] if versioning else []),
                [
                    [row[col] or "-" for col in group_cols]
                    + [
                        row["writes"],
                        human_bytes(row["bytes"]),
                        human_bytes(row["first_write_bytes"]),
                        human_bytes(row["rewrite_bytes"]),
                        human_bytes(row["prewindow_rewrite_bytes"]),
                        "%.1f%%"
                        % (
                            100.0
                            * (row["rewrite_bytes"] + row["prewindow_rewrite_bytes"])
                            / row["bytes"]
                            if row["bytes"]
                            else 0.0
                        ),
                        row["errors"],
                        human_bytes(row["error_bytes"]),
                    ]
                    + (
                        [row["versioned_writes"], human_bytes(row["retained_by_versioning_bytes"])]
                        if versioning and version_known
                        else ["-", "-"]
                        if versioning
                        else []
                    )
                    for row in data["by_group"]
                ],
                right=set(range(len(group_cols), len(group_cols) + (10 if versioning else 8))),
            )
        )
        out.append(f"   Top rewritten {ident}s ({top_note}):")
        out.append(
            table(
                [ident, "size", "writes", "rewrite", "pre-window", "invocations", "via"],
                [
                    [
                        short.digest(row[ident]) if ident == "digest" else short.key(row[ident]),
                        human_bytes(row["size_bytes"]),
                        row["writes"],
                        human_bytes(row["rewrite_bytes"]),
                        human_bytes(row["prewindow_rewrite_bytes"]),
                        row["invocations"],
                        ",".join(row["sources"]) or "-",
                    ]
                    for row in data["top_rewritten"]
                ],
                right={1, 2, 3, 4, 5},
            )
        )
    cas_bytes = rewrites["cas_write"]["totals"]["bytes"]
    backend_bytes = rewrites["backend_write"]["totals"]["bytes"]
    if cas_bytes and backend_bytes:
        out.append(
            f"   backend bytes / CAS bytes: {backend_bytes / cas_bytes:.2f} "
            "(above 1 means the backend is written more than the CAS service accepted)"
        )

    # 6
    health = report["has_health"]
    out.append("")
    out.append(
        f"6. has() health ({health['upload_events']} upload decisions, "
        f"{human_bytes(health['uploaded_bytes'])} uploaded)"
    )
    out.append(
        table(
            [
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
            [
                [
                    row["has_result"],
                    row["events"],
                    human_bytes(row["bytes"]),
                    row["uploaded"],
                    human_bytes(row["uploaded_bytes"]),
                    fmt_counter(row["dispositions"]),
                    row["has_elapsed_ms_p50"],
                    row["has_elapsed_ms_p95"],
                    row["has_elapsed_ms_max"],
                ]
                for row in health["by_has_result"]
            ],
            right={1, 2, 3, 4, 6, 7, 8},
        )
    )
    restored = health["uploads_of_already_stored_digests"]
    if restored:
        out.append(
            "   uploads of digests already stored earlier in the window: "
            + ", ".join(
                f"after {name}: {data['uploads']} ({human_bytes(data['bytes'])})"
                for name, data in restored.items()
            )
        )
    if health["top_has_errors"]:
        out.append("   has() errors:")
        for row in health["top_has_errors"]:
            out.append(f"     {row['count']:>6}  {short.text(row['has_error'], 100)}")
    fm = health["find_missing"]
    if fm["requests"]:
        out.append(
            f"   client FindMissingBlobs: {fm['requests']} requests, "
            f"{fm['requested_count']} digests / {human_bytes(fm['requested_bytes'])} asked, "
            f"{fm['missing_count']} / {human_bytes(fm['missing_bytes'])} missing, "
            f"{fm['errors']} errors"
        )

    # 7
    cov = report["coverage"]
    out.append("")
    out.append("7. Coverage")
    out.append(
        f"   attempts: {cov['attempts_seen']} seen, {cov['attempts_started']} started, "
        f"{cov['attempts_finished']} finished"
    )
    out.append(f"   attempt_started with no attempt_finished: {cov['started_without_finished']}")
    for example in cov["started_without_finished_examples"]:
        out.append(
            f"     {example['attempt_id']}  {example['started'] or '-'}  "
            f"{short.text(example['target_id'], 60)}"
        )
    out.append(f"   attempt_finished with no attempt_started: {cov['finished_without_started']}")
    out.append(
        f"   attempts with outputs_complete=false: {cov['outputs_incomplete']}"
        + (
            f" ({fmt_counter(cov['outputs_incomplete_by_outcome'])})"
            if cov["outputs_incomplete"]
            else ""
        )
    )
    out.append(
        "   outputs_complete=true but outcome not completed (left out of 2 and 3): "
        f"{cov['complete_but_not_completed']}"
        + (
            f" ({fmt_counter(cov['complete_but_not_completed_by_outcome'])})"
            if cov["complete_but_not_completed"]
            else ""
        )
    )
    out.append(
        f"   complete attempts whose observed manifest entries != output_count: "
        f"{cov['manifest_count_mismatches']}"
    )
    out.append(
        f"   output_observed with no attempt_started: {cov['outputs_without_attempt_started']}"
    )
    out.append(
        f"   output_observed with no target/mnemonic/invocation: {cov['outputs_without_metadata']}"
    )
    out.append(
        "   events with an unknown schema: "
        + (fmt_counter(cov["unknown_schema"]) if cov["unknown_schema"] else "0")
    )
    if cov["unknown_events"]:
        out.append("   unknown event kinds: " + fmt_counter(cov["unknown_events"]))
    out.append(f"   events with no timestamp: {cov['events_without_timestamp']}")
    return "\n".join(out) + "\n"


# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Summarise NativeLink output provenance events (schema 1).",
        epilog="See the module docstring for the accepted line formats.",
    )
    parser.add_argument("files", nargs="*", help="log files, plain or gzip; '-' or none for stdin")
    parser.add_argument(
        "--seed-digests",
        metavar="FILE",
        help="digests known before the window, one <hash>-<size> or <hash> per line",
    )
    parser.add_argument(
        "--top", type=int, default=20, help="rows per ranked table, 0 for all (default 20)"
    )
    parser.add_argument("--json", action="store_true", help="print one JSON document")
    parser.add_argument(
        "--wide", action="store_true", help="do not shorten digests or paths in text output"
    )
    args = parser.parse_args(argv)

    seed: set[str] = set()
    if args.seed_digests:
        seed, bad = parse_seed_file(args.seed_digests)
        if bad:
            print(
                f"warning: {bad} line(s) in {args.seed_digests} are not digests; ignored",
                file=sys.stderr,
            )

    collector = Collector()
    try:
        for path in args.files or ["-"]:
            collector.read(path)
    except OSError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    stats = collector.stats
    for schema, count in sorted(stats.unknown_schema.items()):
        print(
            f"warning: skipped {count} provenance event(s) with schema {schema}; "
            f"this tool reads schema {SUPPORTED_SCHEMA}",
            file=sys.stderr,
        )
    if stats.provenance == 0:
        print(
            "warning: no provenance events found; NativeLink needs "
            "RUST_LOG=info,nativelink_provenance=debug and NL_LOG=json",
            file=sys.stderr,
        )

    report = Analysis(collector, seed, args.top).report()
    if args.json:
        json.dump(report, sys.stdout, indent=2, sort_keys=False)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_text(report, args.top, args.wide))
    return 0


if __name__ == "__main__":
    sys.exit(main())
