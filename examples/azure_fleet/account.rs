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

//! Shared HTTP account. A declared admission budget causes every 503. Payload
//! counts are bytes yielded by the server body to Hyper, not socket receipts or
//! billed Azure egress. The fitted timing describes client/proxy complete-body
//! latency; it does not reproduce Azure's internal admission or network stack.

use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, Response, StatusCode};
use axum::routing::any;
use azure_storage_blob::models::BlockLookupList;
use bytes::Bytes;
use futures::Stream;
use serde::Serialize;
use serde_json::Value;
use tokio::time::Instant;

use crate::workload::{Blob, NODES, uniform};

const CHUNK: usize = 64 * 1024;
const PARTITION: usize = 4 * 1024 * 1024;
const CAP: f64 = 256.0 * 1024.0 * 1024.0;
// Admit two initial 8-range SDK waves across four workers, so the failed-fill
// scenario reaches its actual disk failure before admission retries dominate.
const BURST_BYTES: usize = 256 * 1024 * 1024;
const BURST: f64 = BURST_BYTES as f64;

#[derive(Default, Clone, Serialize)]
pub(crate) struct Bucket {
    pub(crate) bytes: u64,
    pub(crate) repeated_bytes: u64,
    pub(crate) get: u64,
    pub(crate) head: u64,
    pub(crate) put: u64,
    pub(crate) throttled: u64,
    pub(crate) ingress: u64,
}

#[derive(Serialize)]
pub(crate) struct HttpRead {
    /// Unique within this account ledger, independent of SDK attempt identity.
    pub(crate) request_id: u64,
    pub(crate) node: usize,
    pub(crate) blob: String,
    /// Context explicitly carried by the SDK, never inferred from a HEAD or time.
    pub(crate) read_id: Option<u64>,
    pub(crate) attempt: Option<u64>,
    pub(crate) sent: Option<u64>,
    pub(crate) writer_broken: Option<bool>,
    pub(crate) offset: u64,
    pub(crate) length: u64,
    pub(crate) start_s: f64,
    pub(crate) end_s: Option<f64>,
    pub(crate) bytes: u64,
    pub(crate) repeated_bytes: u64,
    pub(crate) status: u16,
    pub(crate) reason: &'static str,
    pub(crate) phase: String,
    pub(crate) local_inflight: u32,
    pub(crate) timing_outside_fit: bool,
    /// Most recent HEAD for this node/key. An observed request boundary, not a
    /// general assertion that every HEAD starts a fill or every GET belongs to it.
    pub(crate) head_sequence: Option<u64>,
}

#[derive(Serialize)]
pub(crate) struct Head {
    pub(crate) node: usize,
    pub(crate) blob: String,
    pub(crate) sequence: u64,
    pub(crate) time_s: f64,
    pub(crate) status: u16,
    pub(crate) length: Option<u64>,
    pub(crate) phase: String,
}

#[derive(Clone)]
struct StoredBlob {
    data: Bytes,
    etag: String,
}

struct Timing {
    intercept: f64,
    size_power: f64,
    concurrency_power: f64,
    residuals: Vec<f64>,
}

impl Timing {
    fn measured() -> Self {
        let fit: Value = serde_json::from_str(include_str!(
            "../../nativelink-store/tests/fixtures/azure_download_response.json"
        ))
        .unwrap();
        let residuals: Vec<f64> = fit["log_residual_quantiles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_f64().unwrap())
            .collect();
        assert_eq!(residuals.len(), 129);
        assert!(residuals.windows(2).all(|p| p[0] <= p[1]));
        Self {
            intercept: fit["baseline_ms"]["intercept"].as_f64().unwrap(),
            size_power: fit["baseline_ms"]["size_power"].as_f64().unwrap(),
            concurrency_power: fit["baseline_ms"]["concurrency_power"].as_f64().unwrap(),
            residuals,
        }
    }

    fn duration(&self, inflight: u32, quantile: f64, length: usize) -> Duration {
        // The workload's payloads are partition-aligned. Short metadata bodies
        // use a declared one-millisecond delay, independently of the fitted law.
        if length < PARTITION {
            return Duration::from_millis(1);
        }
        assert!((0.0..1.0).contains(&quantile));
        let position = quantile * 128.0;
        // Locate the lower empirical quantile without a float-to-index cast.
        let index = (1_u32..=128)
            .take_while(|&boundary| f64::from(boundary) <= position)
            .count();
        let residual = (self.residuals[index + 1] - self.residuals[index]).mul_add(
            position - f64::from(u32::try_from(index).unwrap()),
            self.residuals[index],
        );
        let size_mib = length as f64 / 1_048_576.0;
        let ms = self.intercept
            * (1.0 + size_mib).powf(self.size_power)
            * (1.0 + f64::from(inflight)).powf(self.concurrency_power)
            * residual.exp();
        Duration::from_secs_f64(ms / 1000.0)
    }
}

pub(crate) struct Account {
    pub(crate) seed: u64,
    pub(crate) start: Instant,
    pub(crate) state: Mutex<AccountState>,
    timing: Timing,
}

pub(crate) struct AccountState {
    blobs: HashMap<String, StoredBlob>,
    // Retain blocks after commit: a repeated commit must be idempotent.
    blocks: HashMap<(String, Vec<u8>), Bytes>,
    version: u64,
    tokens: f64,
    last: Instant,
    last_request: Instant,
    active: [u32; NODES],
    peak_active: [u32; NODES],
    ordinal: HashMap<(usize, String, u64), u64>,
    head_sequences: HashMap<(usize, String), u64>,
    // Each node has one cache generation. Replication to a different node is
    // first-use there, never classified as a within-generation repeat.
    seen: HashMap<(usize, String), Vec<(u64, u64)>>,
    pub(crate) phase: String,
    pub(crate) buckets: BTreeMap<u64, Bucket>,
    pub(crate) reads: Vec<HttpRead>,
    pub(crate) heads: Vec<Head>,
    pub(crate) admitted: u64,
    pub(crate) completed_actions: u64,
}

impl AccountState {
    fn insert_blob(&mut self, name: String, data: Bytes) {
        self.version += 1;
        self.blobs.insert(
            name,
            StoredBlob {
                data,
                etag: format!("\"uploaded-{}\"", self.version),
            },
        );
    }

    fn admit(&mut self, now: Instant, length: usize) -> bool {
        self.tokens = CAP
            .mul_add(now.duration_since(self.last).as_secs_f64(), self.tokens)
            .min(BURST);
        self.last = now;
        if self.tokens < length as f64 {
            return false;
        }
        self.tokens -= length as f64;
        self.admitted += u64::try_from(length).unwrap();
        true
    }
}

impl Account {
    pub(crate) fn new(seed: u64, blobs: &[Blob]) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            seed,
            start: now,
            timing: Timing::measured(),
            state: Mutex::new(AccountState {
                blobs: blobs
                    .iter()
                    .map(|b| {
                        (
                            b.name(),
                            StoredBlob {
                                data: b.data.clone(),
                                etag: format!("\"{}\"", b.name()),
                            },
                        )
                    })
                    .collect(),
                blocks: HashMap::new(),
                version: 0,
                tokens: BURST,
                last: now,
                last_request: now,
                active: [0; NODES],
                peak_active: [0; NODES],
                ordinal: HashMap::new(),
                head_sequences: HashMap::new(),
                seen: HashMap::new(),
                phase: "warmup".into(),
                buckets: BTreeMap::new(),
                reads: Vec::new(),
                heads: Vec::new(),
                admitted: 0,
                completed_actions: 0,
            }),
        })
    }

    pub(crate) fn phase(&self, value: &str) {
        self.state.lock().unwrap().phase = value.into();
    }

    pub(crate) fn elapsed(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    fn bin(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_millis() / 100).unwrap()
    }

    pub(crate) fn report(&self) -> Value {
        let state = self.state.lock().unwrap();
        let bytes: u64 = state.buckets.values().map(|b| b.bytes).sum();
        let read_bytes: u64 = state.reads.iter().map(|r| r.bytes).sum();
        let repeated: u64 = state.buckets.values().map(|b| b.repeated_bytes).sum();
        let (attributed_bytes, unknown_bytes) =
            state
                .reads
                .iter()
                .fold((0_u64, 0_u64), |(attributed, unknown), read| {
                    if read.read_id.is_some()
                        && read.attempt.is_some()
                        && read.sent.is_some()
                        && read.writer_broken.is_some()
                    {
                        (attributed + read.bytes, unknown)
                    } else {
                        (attributed, unknown + read.bytes)
                    }
                });

        // The time buckets and per-request ledger independently account for
        // the same yielded bytes. Neither substitutes requested range length.
        assert_eq!(bytes, read_bytes);
        assert_eq!(bytes, attributed_bytes + unknown_bytes);
        assert!(
            state
                .reads
                .iter()
                .enumerate()
                .all(|(index, read)| { read.request_id == u64::try_from(index).unwrap() })
        );
        assert_eq!(
            repeated,
            state.reads.iter().map(|r| r.repeated_bytes).sum::<u64>()
        );
        assert!(state.reads.iter().all(|r| {
            r.bytes <= r.length
                && r.repeated_bytes <= r.bytes
                && ((200..300).contains(&r.status) || r.bytes == 0)
        }));
        assert_eq!(
            state.reads.len() as u64,
            state.buckets.values().map(|b| b.get).sum::<u64>()
        );
        assert_eq!(
            state.heads.len() as u64,
            state.buckets.values().map(|b| b.head).sum::<u64>()
        );
        assert!(bytes <= state.admitted);
        assert!(state.admitted as f64 <= CAP.mul_add(self.elapsed(), BURST) + 1.0);

        serde_json::json!({
            "cap_bytes_s": CAP,
            "burst_bytes": BURST,
            "bucket_width_s": 0.1,
            "capacity_boundary": "requested payload reserved at HTTP admission; not a wire-rate shaper",
            "measurement_boundary": "successful response payload yielded by server Body to Hyper; excludes headers and error bodies; not confirmed socket receipt or billed egress",
            "timing_boundary": "conditional client/proxy complete-body fit; independent empirical residual draws; metadata delay is declared, not calibrated",
            "timing_outside_fit": state.reads.iter().filter(|r| r.timing_outside_fit).count(),
            "admitted_payload": state.admitted,
            "payload_bytes": bytes,
            "attributed_payload_bytes": attributed_bytes,
            "unknown_attribution_payload_bytes": unknown_bytes,
            "attribution_boundary": "explicit SDK context: node/process + read_id + attempt; sent and writer_broken sampled before that SDK download; missing or invalid headers remain unknown",
            "repeated_payload_bytes": repeated,
            "active": state.active,
            "peak_active": state.peak_active,
            "head_sequence_boundary": "latest observed HEAD per node/key; the fixture's Verify-wrapped cold fill performs HEAD, Azure read retries do not; not a universal fill identifier",
            "buckets": state.buckets,
            "reads": state.reads,
            "heads": state.heads,
            "completed_actions": state.completed_actions,
        })
    }

    /// Establish HTTP quiet, not absence of a sleeping application-level retry.
    /// Call only after all submitted actions have reached their observed outcome.
    pub(crate) async fn quiesce(&self) -> crate::Result<()> {
        let start = Instant::now();
        loop {
            let quiet = {
                let state = self.state.lock().unwrap();
                state.active.iter().all(|&n| n == 0)
                    && state.last_request.elapsed() >= Duration::from_secs(1)
                    && state
                        .reads
                        .iter()
                        .filter_map(|r| r.end_s)
                        .fold(0.0, f64::max)
                        + 1.0
                        <= self.elapsed()
            };
            if quiet {
                return Ok(());
            }
            if start.elapsed() >= Duration::from_secs(10) {
                return Err("account did not quiesce".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

fn insert_range(ranges: &mut Vec<(u64, u64)>, start: u64, end: u64) -> u64 {
    assert!(start <= end);
    if start == end {
        return 0;
    }
    let overlap = ranges
        .iter()
        .map(|&(a, b)| b.min(end).saturating_sub(a.max(start)))
        .sum();
    let mut lo = start;
    let mut hi = end;
    ranges.retain(|&(a, b)| {
        if a <= hi && b >= lo {
            lo = lo.min(a);
            hi = hi.max(b);
            false
        } else {
            true
        }
    });
    ranges.push((lo, hi));
    ranges.sort_unstable();
    overlap
}

struct Transfer {
    account: Arc<Account>,
    id: usize,
    data: Bytes,
    position: usize,
    start: Instant,
    duration: Duration,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for Transfer {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use core::future::Future;
        let transfer = self.get_mut();
        if transfer.position == transfer.data.len() {
            return Poll::Ready(None);
        }
        let end = (transfer.position + CHUNK).min(transfer.data.len());
        let when = transfer.start
            + transfer
                .duration
                .mul_f64(end as f64 / transfer.data.len() as f64);
        let sleep = transfer
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(when)));
        if sleep.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        transfer.sleep = None;
        let part = transfer.data.slice(transfer.position..end);

        // Charge only this yielded chunk. A requested range whose body is
        // dropped partway through must not be counted as a complete transfer.
        let mut state = transfer.account.state.lock().unwrap();
        let read = &state.reads[transfer.id];
        let (node, name, offset) = (read.node, read.blob.clone(), read.offset);
        let repeated = insert_range(
            state.seen.entry((node, name)).or_default(),
            offset + u64::try_from(transfer.position).unwrap(),
            offset + u64::try_from(end).unwrap(),
        );
        let len = u64::try_from(part.len()).unwrap();
        let read = &mut state.reads[transfer.id];
        read.bytes += len;
        read.repeated_bytes += repeated;
        let bucket = state.buckets.entry(transfer.account.bin()).or_default();
        bucket.bytes += len;
        bucket.repeated_bytes += repeated;
        transfer.position = end;
        Poll::Ready(Some(Ok(part)))
    }
}

impl Drop for Transfer {
    fn drop(&mut self) {
        // Releasing a body ends its active reservation even when the client
        // stopped early. end_s therefore records disposal, not successful EOF.
        let mut state = self.account.state.lock().unwrap();
        state.reads[self.id].end_s = Some(self.account.elapsed());
        let node = state.reads[self.id].node;
        state.active[node] -= 1;
    }
}

fn response(status: StatusCode, data: impl Into<Body>) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("last-modified", "Thu, 01 Jan 1970 00:00:00 GMT")
        .header("x-ms-blob-type", "BlockBlob")
        .body(data.into())
        .unwrap()
}

fn failure(status: StatusCode, code: &'static str) -> Response<Body> {
    let mut r = response(
        status,
        format!("<Error><Code>{code}</Code><Message>Fixture {code}</Message></Error>"),
    );
    r.headers_mut()
        .insert("x-ms-error-code", code.parse().unwrap());
    r
}

// Azure accepts a range ending beyond EOF, but an empty blob rejects any range.
fn byte_range(headers: &HeaderMap, length: usize) -> Result<(usize, usize, bool), StatusCode> {
    let Some(range) = headers.get("x-ms-range").or_else(|| headers.get("range")) else {
        return Ok((0, length, false));
    };
    let (lo, hi) = range
        .to_str()
        .ok()
        .and_then(|v| v.strip_prefix("bytes="))
        .and_then(|v| v.split_once('-'))
        .ok_or(StatusCode::BAD_REQUEST)?;
    let start: usize = lo.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let end = if hi.is_empty() {
        length
    } else {
        hi.parse::<usize>()
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .saturating_add(1)
            .min(length)
    };
    if start >= length || end <= start {
        return Err(StatusCode::RANGE_NOT_SATISFIABLE);
    }
    Ok((start, end, true))
}

fn stage_block(
    state: &mut AccountState,
    name: String,
    query: &HashMap<String, String>,
    data: Bytes,
) -> Result<(), Box<Response<Body>>> {
    let Some(id) = query
        .get("blockid")
        .and_then(|value| azure_core::base64::decode(value).ok())
    else {
        return Err(Box::new(failure(StatusCode::BAD_REQUEST, "InvalidBlockId")));
    };

    state.blocks.insert((name, id), data);
    Ok(())
}

fn commit_blocks(
    state: &mut AccountState,
    name: String,
    data: &[u8],
) -> Result<(), Box<Response<Body>>> {
    let Ok(list): Result<BlockLookupList, _> = azure_core::xml::from_xml(data) else {
        return Err(Box::new(failure(
            StatusCode::BAD_REQUEST,
            "InvalidXmlDocument",
        )));
    };

    // NativeLink uses Latest. Reject unsupported variants rather than commit
    // an empty or truncated object. Keep staged blocks for repeated commits.
    if list.committed.is_some() || list.uncommitted.is_some() {
        return Err(Box::new(failure(
            StatusCode::BAD_REQUEST,
            "UnsupportedBlockList",
        )));
    }

    let mut output = Vec::new();
    for id in list.latest.unwrap_or_default() {
        let Some(block) = state.blocks.get(&(name.clone(), id)) else {
            return Err(Box::new(failure(
                StatusCode::BAD_REQUEST,
                "InvalidBlockList",
            )));
        };
        output.extend_from_slice(block);
    }

    state.insert_blob(name, Bytes::from(output));
    Ok(())
}

async fn put(
    account: &Account,
    name: String,
    query: HashMap<String, String>,
    body: Body,
) -> Response<Body> {
    let Ok(data) = to_bytes(body, 64 * 1024 * 1024).await else {
        return failure(StatusCode::BAD_REQUEST, "InvalidRequestBody");
    };
    let mut state = account.state.lock().unwrap();
    let bucket = state.buckets.entry(account.bin()).or_default();
    // Even a rejected block-list operation has already delivered this body.
    bucket.ingress += u64::try_from(data.len()).unwrap();

    let result = match query.get("comp").map(String::as_str) {
        Some("block") => stage_block(&mut state, name, &query, data),
        Some("blocklist") => commit_blocks(&mut state, name, &data),
        None => {
            state.insert_blob(name, data);
            Ok(())
        }
        _ => Err(Box::new(failure(
            StatusCode::BAD_REQUEST,
            "UnsupportedQueryParameter",
        ))),
    };

    match result {
        Ok(()) => response(StatusCode::CREATED, Body::empty()),
        Err(response) => *response,
    }
}

fn head_response(
    account: &Account,
    state: &mut AccountState,
    node: usize,
    name: String,
    blob: Option<StoredBlob>,
) -> Response<Body> {
    state.buckets.entry(account.bin()).or_default().head += 1;
    let sequence = state
        .head_sequences
        .entry((node, name.clone()))
        .or_default();
    *sequence += 1;
    let sequence = *sequence;
    let phase = state.phase.clone();

    state.heads.push(Head {
        node,
        blob: name,
        sequence,
        time_s: account.elapsed(),
        status: if blob.is_some() { 200 } else { 404 },
        length: blob.as_ref().map(|blob| blob.data.len() as u64),
        phase,
    });

    let Some(blob) = blob else {
        return failure(StatusCode::NOT_FOUND, "BlobNotFound");
    };
    let mut result = response(StatusCode::OK, Body::empty());
    result.headers_mut().insert(
        "content-length",
        blob.data.len().to_string().parse().unwrap(),
    );
    result
        .headers_mut()
        .insert("etag", blob.etag.parse().unwrap());
    result
}

fn number_header(headers: &HeaderMap, name: &'static str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.parse().ok()
}

fn record_get(
    account: &Account,
    state: &mut AccountState,
    node: usize,
    name: &str,
    headers: &HeaderMap,
) -> usize {
    state.buckets.entry(account.bin()).or_default().get += 1;
    let id = state.reads.len();
    let phase = state.phase.clone();
    let local_inflight = state.active[node];
    let head_sequence = state.head_sequences.get(&(node, name.to_owned())).copied();

    // Record every GET before validation, including misses and refusals. Only
    // admission opens its lifetime; Transfer::drop later closes that lifetime.
    state.reads.push(HttpRead {
        request_id: u64::try_from(id).unwrap(),
        node,
        blob: name.to_owned(),
        read_id: number_header(headers, "x-nl-read-id"),
        attempt: number_header(headers, "x-nl-attempt"),
        sent: number_header(headers, "x-nl-sent"),
        writer_broken: headers
            .get("x-nl-writer-broken")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok()),
        offset: 0,
        length: 0,
        start_s: account.elapsed(),
        end_s: Some(account.elapsed()),
        bytes: 0,
        repeated_bytes: 0,
        status: 404,
        reason: "missing",
        phase,
        local_inflight,
        timing_outside_fit: false,
        head_sequence,
    });
    id
}

fn admit_read(
    account: &Account,
    state: &mut AccountState,
    id: usize,
    length: usize,
    ranged: bool,
    now: Instant,
) -> Result<StatusCode, Box<Response<Body>>> {
    if !state.admit(now, length) {
        state.reads[id].status = 503;
        state.reads[id].reason = "account_byte_budget";
        state.buckets.entry(account.bin()).or_default().throttled += 1;

        let mut result = failure(StatusCode::SERVICE_UNAVAILABLE, "ServerBusy");
        result
            .headers_mut()
            .insert("retry-after", "1".parse().unwrap());
        return Err(Box::new(result));
    }

    let node = state.reads[id].node;
    state.active[node] += 1;
    state.peak_active[node] = state.peak_active[node].max(state.active[node]);
    let local_inflight = state.active[node];
    let status = if ranged {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let read = &mut state.reads[id];
    read.status = status.as_u16();
    read.reason = "admitted";
    read.end_s = None;
    read.local_inflight = local_inflight;
    read.timing_outside_fit = length != PARTITION || local_inflight > 32;
    Ok(status)
}

fn read_duration(
    account: &Account,
    state: &mut AccountState,
    id: usize,
    length: usize,
) -> Duration {
    let read = &state.reads[id];
    let ordinal = state
        .ordinal
        .entry((read.node, read.blob.clone(), read.offset))
        .or_default();

    // Stable object/range occurrence, independent of other nodes' HTTP order.
    // The key and arithmetic here are part of replay compatibility.
    let hash = read.blob.as_bytes().iter().fold(0_u64, |v, b| {
        v.wrapping_mul(131).wrapping_add(u64::from(*b))
    });
    let quantile = uniform(
        account.seed,
        hash.wrapping_add((read.node as u64) << 56)
            .wrapping_add(read.offset << 8)
            .wrapping_add(*ordinal),
    );
    *ordinal += 1;

    account
        .timing
        .duration(read.local_inflight, quantile, length)
}

struct AdmittedDownload {
    id: usize,
    blob: StoredBlob,
    start: usize,
    end: usize,
    ranged: bool,
    status: StatusCode,
    requested: Instant,
    duration: Duration,
}

fn prepare_download(
    account: &Account,
    state: &mut AccountState,
    node: usize,
    name: &str,
    headers: &HeaderMap,
    blob: Option<StoredBlob>,
) -> Result<AdmittedDownload, Box<Response<Body>>> {
    let id = record_get(account, state, node, name, headers);
    let Some(blob) = blob else {
        return Err(Box::new(failure(StatusCode::NOT_FOUND, "BlobNotFound")));
    };

    if headers
        .get("if-match")
        .is_some_and(|value| value != "*" && value != blob.etag.as_str())
    {
        state.reads[id].status = 412;
        state.reads[id].reason = "etag_changed";
        return Err(Box::new(failure(
            StatusCode::PRECONDITION_FAILED,
            "ConditionNotMet",
        )));
    }

    let (start, end, ranged) = match byte_range(headers, blob.data.len()) {
        Ok(range) => range,
        Err(status) => {
            state.reads[id].status = status.as_u16();
            state.reads[id].reason = "invalid_range";
            return Err(Box::new(failure(status, "InvalidRange")));
        }
    };

    let length = end - start;
    state.reads[id].offset = u64::try_from(start).unwrap();
    state.reads[id].length = u64::try_from(length).unwrap();
    let requested = Instant::now();
    let status = admit_read(account, state, id, length, ranged, requested)?;
    let duration = read_duration(account, state, id, length);

    Ok(AdmittedDownload {
        id,
        blob,
        start,
        end,
        ranged,
        status,
        requested,
        duration,
    })
}

fn download_response(account: Arc<Account>, download: AdmittedDownload) -> Response<Body> {
    let AdmittedDownload {
        id,
        blob,
        start,
        end,
        ranged,
        status,
        requested,
        duration,
    } = download;

    let blob_length = blob.data.len();
    let length = end - start;
    let transfer = Transfer {
        account,
        id,
        data: blob.data.slice(start..end),
        position: 0,
        start: requested,
        duration,
        sleep: None,
    };
    let mut result = response(status, Body::from_stream(transfer));
    result
        .headers_mut()
        .insert("content-length", length.to_string().parse().unwrap());
    result
        .headers_mut()
        .insert("etag", blob.etag.parse().unwrap());
    if ranged {
        result.headers_mut().insert(
            "content-range",
            format!("bytes {start}-{}/{blob_length}", end - 1)
                .parse()
                .unwrap(),
        );
    }
    result
}

async fn handle(
    State(account): State<Arc<Account>>,
    Path((node, name)): Path<(usize, String)>,
    Query(query): Query<HashMap<String, String>>,
    method: Method,
    headers: HeaderMap,
    body: Body,
) -> Response<Body> {
    if node >= NODES {
        return response(StatusCode::BAD_REQUEST, "unknown node");
    }

    {
        let mut state = account.state.lock().unwrap();
        state.last_request = Instant::now();
        let bucket = state.buckets.entry(account.bin()).or_default();
        if method == Method::PUT {
            bucket.put += 1;
        }
    }

    // Upload collection can await the client. No account lock spans that wait.
    if method == Method::PUT {
        return put(&account, name, query, body).await;
    }

    let mut state = account.state.lock().unwrap();
    let blob = state.blobs.get(&name).cloned();
    if method == Method::HEAD {
        return head_response(&account, &mut state, node, name, blob);
    }
    if method != Method::GET {
        return response(StatusCode::METHOD_NOT_ALLOWED, Body::empty());
    }

    // Blob lookup, admission, request ownership, and the timing draw remain in
    // one critical section. Transfer then owns the active-read reservation.
    let download = match prepare_download(&account, &mut state, node, &name, &headers, blob) {
        Ok(download) => download,
        Err(response) => return *response,
    };
    drop(state);

    // Construct the streamed response after releasing the lock: dropping its
    // body also locks the account to release that reservation and record its end.
    download_response(account, download)
}

pub(crate) async fn serve(
    account: Arc<Account>,
) -> std::io::Result<(String, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let router = Router::new()
        .route("/{node}/cas/{name}", any(handle))
        .with_state(account);
    let task = nativelink_util::background_spawn!("fleet_http", async move {
        axum::serve(listener, router).await.unwrap();
    });
    Ok((format!("http://{address}"), task))
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use azure_core::http::policies::{Policy, PolicyResult};
    use azure_core::http::{Context as AzureContext, Request};
    use azure_storage_blob::models::{BlobClientDownloadOptions, HttpRange};
    use nativelink_store::azure_blob_store::AzureDownloadAttempt;
    use tokio::sync::Notify;

    use super::*;

    // Hold an SDK child task before it reaches the server. This gives the test
    // an exact old-task/new-attempt interleaving without timing-based guesses.
    #[derive(Debug, Default)]
    struct DelayedPartition {
        entered: Notify,
        release: Notify,
        forwarded: Notify,
    }

    #[async_trait]
    impl Policy for DelayedPartition {
        async fn send(
            &self,
            context: &AzureContext,
            request: &mut Request,
            next: &[Arc<dyn Policy>],
        ) -> PolicyResult {
            let delayed = context
                .value::<AzureDownloadAttempt>()
                .is_some_and(|identity| identity.read_id == 17 && identity.attempt == 1)
                && request.headers().get_optional_str(&"range".into())
                    == Some("bytes=4194304-8388607");
            if delayed {
                self.entered.notify_one();
                self.release.notified().await;
            }

            let result = next[0].send(context, request, &next[1..]).await;
            if delayed {
                self.forwarded.notify_one();
            }
            result
        }
    }

    fn download_options(identity: AzureDownloadAttempt) -> BlobClientDownloadOptions<'static> {
        let mut options = BlobClientDownloadOptions {
            parallel: std::num::NonZeroUsize::new(2),
            partition_size: std::num::NonZeroUsize::new(PARTITION),
            range: Some(HttpRange::new(
                identity.sent,
                2 * PARTITION as u64 - identity.sent,
            )),
            ..Default::default()
        };
        options.method_options.context.insert(identity);
        options
    }

    #[test]
    fn resumed_and_overlapping_reads_count_only_actual_repeated_bytes() {
        // Compare interval accounting against a simple independent byte bitmap,
        // including out-of-order, duplicate, adjoining, and bridging ranges.
        let intervals = [
            (20, 30),
            (0, 10),
            (5, 25),
            (3, 27),
            (30, 40),
            (0, 40),
            (10, 10),
        ];
        let mut seen = [false; 40];
        let mut ranges = Vec::new();
        for (start, end) in intervals {
            let expected = seen[start..end].iter().filter(|&&v| v).count();
            assert_eq!(
                insert_range(&mut ranges, start as u64, end as u64),
                expected as u64
            );
            seen[start..end].fill(true);
        }
        assert_eq!(ranges, vec![(0, 40)]);
    }

    #[test]
    fn admission_refuses_excess_and_recovers_without_random_failure_injection() {
        let account = Account::new(17, &[]);
        let mut state = account.state.lock().unwrap();
        let start = state.last;
        let initial_partitions = u64::try_from(BURST_BYTES / PARTITION).unwrap();
        for _ in 0..initial_partitions {
            assert!(state.admit(start, PARTITION));
        }
        assert!(!state.admit(start, PARTITION));
        assert!(!state.admit(start + Duration::from_millis(1), PARTITION));
        assert!(state.admit(start + Duration::from_secs_f64(1.0 / 64.0), PARTITION));
        assert_eq!(state.admitted, (initial_partitions + 1) * PARTITION as u64);
    }

    #[test]
    fn incomplete_or_invalid_identity_remains_unknown() {
        let account = Account::new(31, &[]);
        let mut headers = HeaderMap::new();
        headers.insert("x-nl-read-id", "17".parse().unwrap());
        headers.insert("x-nl-attempt", "invalid".parse().unwrap());
        headers.insert("x-nl-writer-broken", "maybe".parse().unwrap());
        let mut state = account.state.lock().unwrap();
        record_get(&account, &mut state, 0, "unknown", &headers);
        let read = &state.reads[0];
        assert_eq!(read.request_id, 0);
        assert_eq!(read.read_id, Some(17));
        assert_eq!(read.attempt, None);
        assert_eq!(read.sent, None);
        assert_eq!(read.writer_broken, None);
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "tokio::test creates its runtime and calls Runtime::block_on"
    )]
    #[tokio::test]
    async fn dropped_body_counts_only_yielded_bytes_and_separates_node_replication() {
        use futures::StreamExt;
        let account = Account::new(29, &[]);
        account
            .state
            .lock()
            .unwrap()
            .insert_blob("partial".into(), Bytes::from(vec![9; 2 * CHUNK]));
        for node in [0, 1, 0] {
            let response = handle(
                State(account.clone()),
                Path((node, "partial".into())),
                Query(HashMap::new()),
                Method::GET,
                HeaderMap::new(),
                Body::empty(),
            )
            .await;
            let mut body = response.into_body().into_data_stream();
            assert_eq!(body.next().await.unwrap().unwrap().len(), CHUNK);
            drop(body);
        }
        let report = account.report();
        assert_eq!(report["payload_bytes"], 3 * CHUNK as u64);
        assert_eq!(report["repeated_payload_bytes"], CHUNK as u64);
        assert_eq!(report["attributed_payload_bytes"], 0);
        assert_eq!(
            report["unknown_attribution_payload_bytes"],
            3 * CHUNK as u64
        );
        let state = account.state.lock().unwrap();
        assert!(state.active.iter().all(|&n| n == 0));
        assert!(
            state
                .reads
                .iter()
                .all(|r| r.bytes < r.length && r.end_s.is_some())
        );
        assert_eq!(state.reads[1].repeated_bytes, 0);
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "tokio::test creates its runtime and calls Runtime::block_on"
    )]
    #[tokio::test]
    async fn sdk_detached_partition_keeps_old_attempt_identity() -> crate::Result<()> {
        use azure_core::http::{RetryOptions, Transport, Url};
        use azure_storage_blob::clients::{BlobContainerClient, BlobContainerClientOptions};
        use futures::TryStreamExt;

        drop(tokio_rustls::rustls::crypto::ring::default_provider().install_default());
        let account = Account::new(20_261_021, &[]);
        account
            .state
            .lock()
            .unwrap()
            .insert_blob("shared".into(), Bytes::from(vec![7; 2 * PARTITION]));
        let (url, server) = serve(account.clone()).await?;
        let gate = Arc::new(DelayedPartition::default());
        let mut options = BlobContainerClientOptions::default();
        options.client_options.retry = RetryOptions::none();
        crate::transport::attach(&mut options.client_options);
        options.client_options.per_call_policies.push(gate.clone());
        options.client_options.transport = Some(Transport::new(Arc::new(
            reqwest::Client::builder().no_proxy().build()?,
        )));
        let client =
            BlobContainerClient::new(Url::parse(&format!("{url}/0/cas"))?, None, Some(options))?;
        let blob = client.blob_client("shared");

        // Context values below are test labels; this test proves SDK provenance,
        // while the store test proves when a real writer becomes broken.
        let old = download_options(AzureDownloadAttempt {
            read_id: 17,
            attempt: 1,
            sent: 0,
            writer_broken: false,
        });
        let mut old_body = blob.download(Some(old)).await?.body;
        assert_eq!(old_body.try_next().await?.unwrap().len(), PARTITION);
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        drop(old_body);

        // The resumed call reaches the same range first. The old partition is
        // still alive, but has not yet made its HTTP request.
        let resumed = download_options(AzureDownloadAttempt {
            read_id: 17,
            attempt: 2,
            sent: PARTITION as u64,
            writer_broken: true,
        });
        let chunks: Vec<Bytes> = blob
            .download(Some(resumed))
            .await?
            .body
            .try_collect()
            .await?;
        assert_eq!(chunks.concat(), vec![7; PARTITION]);
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), gate.forwarded.notified()).await?;
        account.quiesce().await?;

        let report = account.report();
        assert_eq!(report["payload_bytes"], 3 * PARTITION as u64);
        assert_eq!(report["attributed_payload_bytes"], 3 * PARTITION as u64);
        assert_eq!(report["unknown_attribution_payload_bytes"], 0);
        assert_eq!(report["repeated_payload_bytes"], PARTITION as u64);
        let state = account.state.lock().unwrap();
        let identities: Vec<_> = state
            .reads
            .iter()
            .map(|read| {
                (
                    read.request_id,
                    read.read_id,
                    read.attempt,
                    read.offset,
                    read.sent,
                    read.writer_broken,
                )
            })
            .collect();
        assert_eq!(
            identities,
            [
                (0, Some(17), Some(1), 0, Some(0), Some(false)),
                (
                    1,
                    Some(17),
                    Some(2),
                    PARTITION as u64,
                    Some(PARTITION as u64),
                    Some(true)
                ),
                (2, Some(17), Some(1), PARTITION as u64, Some(0), Some(false)),
            ]
        );
        assert!(state.heads.is_empty());
        drop(state);
        server.abort();
        Ok(())
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "tokio::test creates its runtime and calls Runtime::block_on"
    )]
    #[tokio::test]
    async fn sdk_round_trip_range_empty_blob_and_idempotent_commit() -> crate::Result<()> {
        use azure_core::http::{RequestContent, RetryOptions, Transport, Url};
        use azure_storage_blob::clients::{BlobContainerClient, BlobContainerClientOptions};
        use futures::TryStreamExt;
        drop(tokio_rustls::rustls::crypto::ring::default_provider().install_default());
        let account = Account::new(20_261_017, &[]);
        let (url, server) = serve(account.clone()).await?;
        let mut options = BlobContainerClientOptions::default();
        options.client_options.retry = RetryOptions::none();
        crate::transport::attach(&mut options.client_options);
        options.client_options.transport = Some(Transport::new(Arc::new(
            reqwest::Client::builder().no_proxy().build()?,
        )));
        let client =
            BlobContainerClient::new(Url::parse(&format!("{url}/0/cas"))?, None, Some(options))?;
        let blob = client.blob_client("payload");
        let block = blob.block_blob_client();
        let payload = Bytes::from(vec![7_u8; 2 * PARTITION + 17]);
        block
            .upload(RequestContent::from(payload.to_vec()), None)
            .await?;
        blob.get_properties(None).await?;
        let chunks: Vec<Bytes> = blob.download(None).await?.body.try_collect().await?;
        // The SDK pads its initial buffer for small objects; this object crosses
        // partitions and must round-trip exactly, including the final 17 bytes.
        assert_eq!(chunks.concat(), payload);
        blob.get_properties(None).await?;
        let committed = client.blob_client("committed");
        let block = committed.block_blob_client();
        block
            .stage_block(b"one", 3, RequestContent::from(b"abc".to_vec()), None)
            .await?;
        block
            .stage_block(b"two", 3, RequestContent::from(b"def".to_vec()), None)
            .await?;
        for _ in 0..2 {
            block
                .commit_block_list(
                    RequestContent::try_from(BlockLookupList {
                        latest: Some(vec![b"one".to_vec(), b"two".to_vec()]),
                        ..Default::default()
                    })?,
                    None,
                )
                .await?;
        }
        let chunks: Vec<Bytes> = committed.download(None).await?.body.try_collect().await?;
        assert_eq!(chunks.concat(), b"abcdef");
        let empty = client.blob_client("empty");
        empty
            .block_blob_client()
            .upload(RequestContent::from(Vec::new()), None)
            .await?;
        empty.get_properties(None).await?;
        let empty_options = download_options(AzureDownloadAttempt {
            read_id: 18,
            attempt: 1,
            sent: 0,
            writer_broken: false,
        });
        let chunks: Vec<Bytes> = empty
            .download(Some(empty_options))
            .await?
            .body
            .try_collect()
            .await?;
        assert!(chunks.concat().is_empty());
        account.quiesce().await?;
        let report = account.report();
        assert_eq!(report["payload_bytes"], payload.len() as u64 + 6);
        let state = account.state.lock().unwrap();
        let empty_statuses: Vec<u16> = state
            .reads
            .iter()
            .filter(|r| r.blob == "empty")
            .map(|r| r.status)
            .collect();
        assert_eq!(empty_statuses, [416, 200]);
        // The SDK's empty-blob fallback is a second HTTP request belonging to
        // the same outer download, not an application-level retry.
        assert!(state.reads.iter().filter(|r| r.blob == "empty").all(|r| {
            r.read_id == Some(18)
                && r.attempt == Some(1)
                && r.sent == Some(0)
                && r.writer_broken == Some(false)
        }));
        assert!(
            state
                .reads
                .iter()
                .filter(|r| r.blob == "payload")
                .all(|r| r.head_sequence == Some(1))
        );
        assert!(
            state
                .reads
                .iter()
                .filter(|r| r.blob == "empty")
                .all(|r| r.head_sequence == Some(1))
        );
        let payload_heads: Vec<_> = state
            .heads
            .iter()
            .filter(|h| h.blob == "payload")
            .map(|h| h.sequence)
            .collect();
        assert_eq!(payload_heads, [1, 2]);
        drop(state);
        server.abort();
        Ok(())
    }
}
