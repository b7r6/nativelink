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
pub(crate) struct Read {
    pub(crate) node: usize,
    pub(crate) blob: String,
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
    pub(crate) reads: Vec<Read>,
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
        let s = self.state.lock().unwrap();
        let bytes: u64 = s.buckets.values().map(|b| b.bytes).sum();
        let read_bytes: u64 = s.reads.iter().map(|r| r.bytes).sum();
        let repeated: u64 = s.buckets.values().map(|b| b.repeated_bytes).sum();
        assert_eq!(bytes, read_bytes);
        assert_eq!(
            repeated,
            s.reads.iter().map(|r| r.repeated_bytes).sum::<u64>()
        );
        assert!(s.reads.iter().all(|r| {
            r.bytes <= r.length
                && r.repeated_bytes <= r.bytes
                && ((200..300).contains(&r.status) || r.bytes == 0)
        }));
        assert_eq!(
            s.reads.len() as u64,
            s.buckets.values().map(|b| b.get).sum::<u64>()
        );
        assert_eq!(
            s.heads.len() as u64,
            s.buckets.values().map(|b| b.head).sum::<u64>()
        );
        assert!(bytes <= s.admitted);
        assert!(s.admitted as f64 <= CAP.mul_add(self.elapsed(), BURST) + 1.0);
        serde_json::json!({
            "cap_bytes_s": CAP, "burst_bytes": BURST, "bucket_width_s": 0.1,
            "capacity_boundary": "requested payload reserved at HTTP admission; not a wire-rate shaper",
            "measurement_boundary": "successful response payload yielded by server Body to Hyper; excludes headers and error bodies; not confirmed socket receipt or billed egress",
            "timing_boundary": "conditional client/proxy complete-body fit; independent empirical residual draws; metadata delay is declared, not calibrated",
            "timing_outside_fit": s.reads.iter().filter(|r| r.timing_outside_fit).count(),
            "admitted_payload": s.admitted, "payload_bytes": bytes,
            "repeated_payload_bytes": repeated, "active": s.active, "peak_active": s.peak_active,
            "head_sequence_boundary": "latest observed HEAD per node/key; the fixture's Verify-wrapped cold fill performs HEAD, Azure read retries do not; not a universal fill identifier",
            "buckets": s.buckets, "reads": s.reads, "heads": s.heads,
            "completed_actions": s.completed_actions
        })
    }

    /// Establish HTTP quiet, not absence of a sleeping application-level retry.
    /// Call only after all submitted actions have reached their observed outcome.
    pub(crate) async fn quiesce(&self) {
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
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "account did not quiesce"
            );
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
        let t = self.get_mut();
        if t.position == t.data.len() {
            return Poll::Ready(None);
        }
        let end = (t.position + CHUNK).min(t.data.len());
        let when = t.start + t.duration.mul_f64(end as f64 / t.data.len() as f64);
        let sleep = t
            .sleep
            .get_or_insert_with(|| Box::pin(tokio::time::sleep_until(when)));
        if sleep.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        t.sleep = None;
        let part = t.data.slice(t.position..end);
        let mut s = t.account.state.lock().unwrap();
        let r = &s.reads[t.id];
        let (node, name, offset) = (r.node, r.blob.clone(), r.offset);
        let repeated = insert_range(
            s.seen.entry((node, name)).or_default(),
            offset + u64::try_from(t.position).unwrap(),
            offset + u64::try_from(end).unwrap(),
        );
        let len = u64::try_from(part.len()).unwrap();
        let r = &mut s.reads[t.id];
        r.bytes += len;
        r.repeated_bytes += repeated;
        let b = s.buckets.entry(t.account.bin()).or_default();
        b.bytes += len;
        b.repeated_bytes += repeated;
        t.position = end;
        Poll::Ready(Some(Ok(part)))
    }
}

impl Drop for Transfer {
    fn drop(&mut self) {
        let mut s = self.account.state.lock().unwrap();
        s.reads[self.id].end_s = Some(self.account.elapsed());
        let node = s.reads[self.id].node;
        s.active[node] -= 1;
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

async fn put(
    a: &Account,
    name: String,
    query: HashMap<String, String>,
    body: Body,
) -> Response<Body> {
    let Ok(data) = to_bytes(body, 64 * 1024 * 1024).await else {
        return failure(StatusCode::BAD_REQUEST, "InvalidRequestBody");
    };
    let mut s = a.state.lock().unwrap();
    let b = s.buckets.entry(a.bin()).or_default();
    b.ingress += u64::try_from(data.len()).unwrap();
    match query.get("comp").map(String::as_str) {
        Some("block") => {
            let Some(id) = query
                .get("blockid")
                .and_then(|v| azure_core::base64::decode(v).ok())
            else {
                return failure(StatusCode::BAD_REQUEST, "InvalidBlockId");
            };
            s.blocks.insert((name, id), data);
        }
        Some("blocklist") => {
            let Ok(list): Result<BlockLookupList, _> = azure_core::xml::from_xml(&data) else {
                return failure(StatusCode::BAD_REQUEST, "InvalidXmlDocument");
            };
            // NativeLink uses Latest. Fail explicitly for unsupported protocol
            // variants rather than accepting an empty or truncated commit.
            if list.committed.is_some() || list.uncommitted.is_some() {
                return failure(StatusCode::BAD_REQUEST, "UnsupportedBlockList");
            }
            let mut out = Vec::new();
            for id in list.latest.unwrap_or_default() {
                let Some(block) = s.blocks.get(&(name.clone(), id)) else {
                    return failure(StatusCode::BAD_REQUEST, "InvalidBlockList");
                };
                out.extend_from_slice(block);
            }
            s.insert_blob(name, Bytes::from(out));
        }
        None => s.insert_blob(name, data),
        _ => return failure(StatusCode::BAD_REQUEST, "UnsupportedQueryParameter"),
    }
    response(StatusCode::CREATED, Body::empty())
}

async fn handle(
    State(a): State<Arc<Account>>,
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
        let mut s = a.state.lock().unwrap();
        s.last_request = Instant::now();
        let bucket = s.buckets.entry(a.bin()).or_default();
        if method == Method::PUT {
            bucket.put += 1;
        }
    }
    if method == Method::PUT {
        return put(&a, name, query, body).await;
    }
    let mut s = a.state.lock().unwrap();
    let blob = s.blobs.get(&name).cloned();
    if method == Method::HEAD {
        s.buckets.entry(a.bin()).or_default().head += 1;
        let sequence = s.head_sequences.entry((node, name.clone())).or_default();
        *sequence += 1;
        let sequence = *sequence;
        let phase = s.phase.clone();
        s.heads.push(Head {
            node,
            blob: name,
            sequence,
            time_s: a.elapsed(),
            status: if blob.is_some() { 200 } else { 404 },
            length: blob.as_ref().map(|b| b.data.len() as u64),
            phase,
        });
        let Some(blob) = blob else {
            return failure(StatusCode::NOT_FOUND, "BlobNotFound");
        };
        let mut r = response(StatusCode::OK, Body::empty());
        r.headers_mut().insert(
            "content-length",
            blob.data.len().to_string().parse().unwrap(),
        );
        r.headers_mut().insert("etag", blob.etag.parse().unwrap());
        return r;
    }
    if method != Method::GET {
        return response(StatusCode::METHOD_NOT_ALLOWED, Body::empty());
    }
    s.buckets.entry(a.bin()).or_default().get += 1;
    let id = s.reads.len();
    let phase = s.phase.clone();
    let local_inflight = s.active[node];
    let head_sequence = s.head_sequences.get(&(node, name.clone())).copied();
    s.reads.push(Read {
        node,
        blob: name.clone(),
        offset: 0,
        length: 0,
        start_s: a.elapsed(),
        end_s: Some(a.elapsed()),
        bytes: 0,
        repeated_bytes: 0,
        status: 404,
        reason: "missing",
        phase,
        local_inflight,
        timing_outside_fit: false,
        head_sequence,
    });
    let Some(blob) = blob else {
        return failure(StatusCode::NOT_FOUND, "BlobNotFound");
    };
    if headers
        .get("if-match")
        .is_some_and(|v| v != "*" && v != blob.etag.as_str())
    {
        s.reads[id].status = 412;
        s.reads[id].reason = "etag_changed";
        return failure(StatusCode::PRECONDITION_FAILED, "ConditionNotMet");
    }
    let (start, end, ranged) = match byte_range(&headers, blob.data.len()) {
        Ok(range) => range,
        Err(status) => {
            s.reads[id].status = status.as_u16();
            s.reads[id].reason = "invalid_range";
            return failure(status, "InvalidRange");
        }
    };
    let length = end - start;
    s.reads[id].offset = u64::try_from(start).unwrap();
    s.reads[id].length = u64::try_from(length).unwrap();
    let now = Instant::now();
    if !s.admit(now, length) {
        s.reads[id].status = 503;
        s.reads[id].reason = "account_byte_budget";
        s.buckets.entry(a.bin()).or_default().throttled += 1;
        let mut r = failure(StatusCode::SERVICE_UNAVAILABLE, "ServerBusy");
        r.headers_mut().insert("retry-after", "1".parse().unwrap());
        return r;
    }
    s.active[node] += 1;
    s.peak_active[node] = s.peak_active[node].max(s.active[node]);
    let local_inflight = s.active[node];
    let status = if ranged {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let r = &mut s.reads[id];
    r.status = status.as_u16();
    r.reason = "admitted";
    r.end_s = None;
    r.local_inflight = local_inflight;
    r.timing_outside_fit = length != PARTITION || local_inflight > 32;
    let ordinal = s
        .ordinal
        .entry((node, name.clone(), u64::try_from(start).unwrap()))
        .or_default();
    // Stable object/range occurrence, independent of other nodes' HTTP order.
    let hash = name.as_bytes().iter().fold(0_u64, |v, b| {
        v.wrapping_mul(131).wrapping_add(u64::from(*b))
    });
    let quantile = uniform(
        a.seed,
        hash.wrapping_add((node as u64) << 56)
            .wrapping_add((start as u64) << 8)
            .wrapping_add(*ordinal),
    );
    *ordinal += 1;
    let duration = a.timing.duration(local_inflight, quantile, length);
    drop(s);
    let len = blob.data.len();
    let transfer = Transfer {
        account: a,
        id,
        data: blob.data.slice(start..end),
        position: 0,
        start: now,
        duration,
        sleep: None,
    };
    let mut r = response(status, Body::from_stream(transfer));
    r.headers_mut()
        .insert("content-length", length.to_string().parse().unwrap());
    r.headers_mut().insert("etag", blob.etag.parse().unwrap());
    if ranged {
        r.headers_mut().insert(
            "content-range",
            format!("bytes {start}-{}/{len}", end - 1).parse().unwrap(),
        );
    }
    r
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
    use super::*;

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
    async fn sdk_round_trip_range_empty_blob_and_idempotent_commit() -> crate::Result<()> {
        use azure_core::http::{RequestContent, RetryOptions, Transport, Url};
        use azure_storage_blob::clients::{BlobContainerClient, BlobContainerClientOptions};
        use futures::TryStreamExt;
        drop(tokio_rustls::rustls::crypto::ring::default_provider().install_default());
        let account = Account::new(20_261_017, &[]);
        let (url, server) = serve(account.clone()).await?;
        let mut options = BlobContainerClientOptions::default();
        options.client_options.retry = RetryOptions::none();
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
        let chunks: Vec<Bytes> = empty.download(None).await?.body.try_collect().await?;
        assert!(chunks.concat().is_empty());
        account.quiesce().await;
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
