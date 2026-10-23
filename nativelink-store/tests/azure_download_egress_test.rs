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

//! Real store + SDK + retrier; only HTTP is simulated. The response distribution
//! comes from measured Azure reads (see the adjacent calibration fixture).
//! A declared token bucket causes 503s, then recovers. After recovery, compare
//! a complete read with a reader dropped after its first SDK partition.
//!
//! Each HTTP request carries the actual `NativeLink` download attempt from the
//! SDK context. No attempt may start with its writer already broken. Old SDK
//! tasks retain their original attempt when they finish or request another
//! partition; post-close bytes alone do not identify a new outer retry.
//! Retry counts and overlapping bytes remain independent secondary checks.
//! No copied store, Python, cloud access, or model of the expected amplification
//! is involved. Paused time preserves production retry delays. The thirty
//! seeded pairs are reproducible stress cases, not production confidence
//! intervals; scheduling and SDK parallelism still affect measured byte counts.

use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use azure_core::http::headers::Headers;
use azure_core::http::policies::{Policy, PolicyResult, TransportPolicy};
use azure_core::http::{
    AsyncRawResponse, Context as AzureContext, HttpClient, Method, Request, RetryOptions,
    StatusCode, Transport, Url,
};
use azure_storage_blob::clients::{BlobContainerClient, BlobContainerClientOptions};
use bytes::Bytes;
use futures::{Future, Stream};
use nativelink_config::stores::{CommonObjectSpec, ExperimentalAzureSpec, Retry};
use nativelink_error::Code;
use nativelink_macro::nativelink_test;
use nativelink_store::azure_blob_store::{AzureBlobStore, AzureDownloadAttempt};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::store_trait::StoreLike;
use serde_json::{Value, json};
use tokio::time::Instant;

const PARTITION: u32 = 4 * 1024 * 1024;
const CHUNK: u32 = 64 * 1024;
const BLOB: u32 = 32 * PARTITION;
const RECOVERY: Duration = Duration::from_secs(2);
const QUIET_WINDOW: Duration = Duration::from_millis(100);
const SEEDS: u32 = 30;

type TestStore = Arc<AzureBlobStore<fn() -> MockInstantWrapped>>;

// The production download context is per call and is cloned by the SDK for
// its partition requests. Only this test policy puts it on HTTP headers.
#[derive(Debug)]
struct AttemptHeaders;

#[async_trait::async_trait]
impl Policy for AttemptHeaders {
    async fn send(
        &self,
        context: &AzureContext,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        if let Some(attempt) = context.value::<AzureDownloadAttempt>() {
            request.insert_header("x-nativelink-test-read", attempt.read_id.to_string());
            request.insert_header("x-nativelink-test-attempt", attempt.attempt.to_string());
            request.insert_header("x-nativelink-test-sent", attempt.sent.to_string());
            request.insert_header(
                "x-nativelink-test-broken",
                attempt.writer_broken.to_string(),
            );
        }
        next[0].send(context, request, &next[1..]).await
    }
}

fn request_attempt(request: &Request) -> AzureDownloadAttempt {
    let header = |name: &str| {
        request
            .headers()
            .iter()
            .find(|(key, _)| key.as_str().eq_ignore_ascii_case(name))
            .unwrap_or_else(|| panic!("GET lost its NativeLink attempt context: {name}"))
            .1
            .as_str()
    };
    AzureDownloadAttempt {
        read_id: header("x-nativelink-test-read").parse().unwrap(),
        attempt: header("x-nativelink-test-attempt").parse().unwrap(),
        sent: header("x-nativelink-test-sent").parse().unwrap(),
        writer_broken: header("x-nativelink-test-broken").parse().unwrap(),
    }
}

#[nativelink_test]
async fn attempt_headers_preserve_an_older_context_after_a_newer_request() {
    let account = Account::new(17);
    let transport: Arc<dyn Policy> = Arc::new(TransportPolicy::new(Transport::new(Arc::new(
        AzureTransport(account.clone()),
    ))));
    let original = AzureContext::new().with_value(AzureDownloadAttempt {
        read_id: 41,
        attempt: 1,
        sent: 0,
        writer_broken: false,
    });
    let newer = original.clone().with_value(AzureDownloadAttempt {
        read_id: 41,
        attempt: 2,
        sent: u64::from(PARTITION),
        writer_broken: true,
    });
    let mut request = Request::new(
        Url::parse("https://fixture.blob.core.windows.net/cas/context").unwrap(),
        Method::Get,
    );
    request.insert_header("range", format!("bytes=0-{}", PARTITION - 1));

    // Reuse the request to make stale-header bugs visible. The third request
    // still belongs to attempt one even though attempt two was seen first.
    for context in [&original, &newer, &original] {
        let response = AttemptHeaders
            .send(context, &mut request, core::slice::from_ref(&transport))
            .await
            .unwrap();
        drop(response);
    }

    let state = account.state.lock().unwrap();
    let ledger = &state.warmup;
    ledger.assert_conserved();
    assert_eq!(
        ledger
            .requests
            .iter()
            .map(|request| request.attempt)
            .collect::<Vec<_>>(),
        vec![1, 2, 1],
    );
    assert_eq!(ledger.attempts[&(41, 1)].gets, 2);
    assert_eq!(ledger.attempts[&(41, 1)].sent, 0);
    assert!(!ledger.attempts[&(41, 1)].writer_broken);
    assert_eq!(ledger.attempts[&(41, 2)].gets, 1);
    assert_eq!(ledger.attempts[&(41, 2)].sent, u64::from(PARTITION));
    assert!(ledger.attempts[&(41, 2)].writer_broken);
    assert_eq!(ledger.retries(), 1);
    assert_eq!(ledger.jitter_draws, 0);
}

// SplitMix64: each (seed, range, occurrence) gets the same latency quantile in
// both arms, independent of HTTP task polling order. The duration also depends
// on observed overlap, which need not match across arms. Retry jitter uses a
// distinct stream and the production multiplier formula.
fn uniform(seed: u64, index: u64) -> f64 {
    let mut x = seed.wrapping_add(index.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    f64::from(u32::try_from((x ^ (x >> 31)) >> 32).unwrap()) / 4_294_967_296.0
}

#[derive(Debug)]
struct Timing {
    intercept: f64,
    size_power: f64,
    concurrency_power: f64,
    residuals: Vec<f64>,
}

impl Timing {
    fn measured() -> Self {
        let fixture: Value =
            serde_json::from_str(include_str!("fixtures/azure_download_response.json")).unwrap();
        let fit = &fixture["baseline_ms"];
        let residuals: Vec<f64> = fixture["log_residual_quantiles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        assert_eq!(residuals.len(), 129);
        assert!(residuals.windows(2).all(|p| p[0] <= p[1]));
        Self {
            intercept: fit["intercept"].as_f64().unwrap(),
            size_power: fit["size_power"].as_f64().unwrap(),
            concurrency_power: fit["concurrency_power"].as_f64().unwrap(),
            residuals,
        }
    }

    fn baseline_ms(&self, inflight: u32) -> f64 {
        assert!((1..=32).contains(&inflight), "outside measured overlap");
        // Fixed 4 MiB ranges: 1 + requested_MiB = 5.
        self.intercept
            * 5.0_f64.powf(self.size_power)
            * (1.0 + f64::from(inflight)).powf(self.concurrency_power)
    }

    fn duration(&self, inflight: u32, quantile: f64) -> Duration {
        let position = quantile * 128.0;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "quantile is in [0,1), so the index is in 0..128"
        )]
        let index = position as usize;
        let fraction = position - f64::from(u32::try_from(index).unwrap());
        let log_residual = (self.residuals[index + 1] - self.residuals[index])
            .mul_add(fraction, self.residuals[index]);
        Duration::from_secs_f64(self.baseline_ms(inflight) * log_residual.exp() / 1000.0)
    }
}

#[derive(Debug, Default)]
struct AttemptLedger {
    sent: u64,
    writer_broken: bool,
    gets: u32,
    refused: u32,
    bytes: u64,
    after_close: u64,
    repeated: u64,
}

#[derive(Debug)]
struct RequestLedger {
    read_id: u64,
    attempt: u64,
    offset: u32,
    end: u32,
    admitted: bool,
    bytes: u64,
}

#[derive(Debug, Default)]
struct Ledger {
    gets: u32,
    refused: u32,
    admitted: u64,
    bytes: u64,
    after_close: u64,
    repeated: u64,
    blocks: BTreeSet<u32>,
    // 100 ms bins expose retry-driven bursts after the initial transfer.
    bins: BTreeMap<u128, u64>,
    // An admitted range occurrence is not an outer NativeLink attempt.
    range_occurrences: BTreeMap<u32, u32>,
    jitter_draws: u32,
    // An attempt may own many HTTP requests, including requests that start
    // after a newer attempt. Neither timestamps nor a current-attempt counter
    // can recover that ownership; the immutable SDK context supplies it.
    attempts: BTreeMap<(u64, u64), AttemptLedger>,
    requests: Vec<RequestLedger>,
}

impl Ledger {
    fn retries(&self) -> usize {
        self.attempts
            .keys()
            .filter(|(_, attempt)| *attempt > 1)
            .count()
    }

    fn request_started(&mut self, identity: &AzureDownloadAttempt, offset: u32, end: u32) -> usize {
        let attempt = self
            .attempts
            .entry((identity.read_id, identity.attempt))
            .or_insert_with(|| AttemptLedger {
                sent: identity.sent,
                writer_broken: identity.writer_broken,
                ..Default::default()
            });
        assert_eq!(attempt.sent, identity.sent);
        assert_eq!(attempt.writer_broken, identity.writer_broken);
        attempt.gets += 1;

        let request_id = self.requests.len();
        self.requests.push(RequestLedger {
            read_id: identity.read_id,
            attempt: identity.attempt,
            offset,
            end,
            admitted: false,
            bytes: 0,
        });
        request_id
    }

    fn assert_conserved(&self) {
        assert_eq!(self.bytes, self.bins.values().sum::<u64>());
        assert_eq!(
            self.bytes,
            u64::try_from(self.blocks.len()).unwrap() * u64::from(CHUNK) + self.repeated
        );
        assert!(self.bytes <= self.admitted);
        assert_eq!(
            self.admitted,
            u64::from(self.gets - self.refused) * u64::from(PARTITION)
        );
        assert_eq!(
            self.bytes,
            self.requests.iter().map(|r| r.bytes).sum::<u64>()
        );
        assert_eq!(
            self.bytes,
            self.attempts.values().map(|a| a.bytes).sum::<u64>()
        );
        assert_eq!(
            self.gets,
            self.attempts.values().map(|a| a.gets).sum::<u32>()
        );
        assert_eq!(
            self.refused,
            self.attempts.values().map(|a| a.refused).sum::<u32>()
        );
        assert_eq!(usize::try_from(self.gets).unwrap(), self.requests.len());
        for request in &self.requests {
            assert!(request.bytes <= u64::from(request.end - request.offset));
            assert!(request.admitted || request.bytes == 0);
        }
    }
}

#[derive(Debug)]
struct State {
    tokens: f64,
    refilled: Instant,
    active: u32,
    peak_active: u32,
    probe: bool,
    closed: bool,
    probe_start: Instant,
    // Assign a logical read to warmup/probe only on its first request. Later
    // partition requests keep that assignment across a phase transition.
    read_phases: BTreeMap<u64, (bool, Instant)>,
    warmup: Ledger,
    read: Ledger,
}

impl State {
    const fn ledger(&mut self) -> &mut Ledger {
        self.ledger_for(self.probe)
    }

    const fn ledger_for(&mut self, probe: bool) -> &mut Ledger {
        if probe {
            &mut self.read
        } else {
            &mut self.warmup
        }
    }
}

#[derive(Debug)]
struct Account {
    seed: u64,
    start: Instant,
    timing: Timing,
    range_body: Bytes,
    state: Mutex<State>,
}

impl Account {
    fn new(seed: u64) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            seed,
            start: now,
            timing: Timing::measured(),
            // Each range has identical contents; the store's digest size is real.
            range_body: Bytes::from(vec![42; usize::try_from(PARTITION).unwrap()]),
            state: Mutex::new(State {
                tokens: f64::from(8 * PARTITION),
                refilled: now,
                active: 0,
                peak_active: 0,
                probe: false,
                closed: false,
                probe_start: now,
                read_phases: BTreeMap::new(),
                warmup: Ledger::default(),
                read: Ledger::default(),
            }),
        })
    }

    fn jitter(&self, nominal: Duration) -> Duration {
        let mut state = self.state.lock().unwrap();
        let phase = u64::from(state.probe);
        let index = state.ledger().jitter_draws;
        // A throttle can draw jitter even when its wait exceeds the budget.
        // Count draws for RNG indexing; SDK identities count actual retries.
        state.ledger().jitter_draws += 1;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "production jitter uses f32"
        )]
        let u = uniform(self.seed, 10_000 + phase * 100 + u64::from(index)) as f32;
        nominal.mul_f32(0.5_f32.mul_add(u - 0.5, 1.0))
    }

    fn refill(&self, state: &mut State, now: Instant) {
        let elapsed = now.duration_since(self.start).as_secs_f64();
        let previous = state.refilled.duration_since(self.start).as_secs_f64();
        let reference_rate = f64::from(BLOB) / (self.timing.baseline_ms(32) / 1000.0);
        let low = elapsed.min(2.0) - previous.min(2.0);
        let high = (elapsed - 2.0).max(0.0) - (previous - 2.0).max(0.0);

        // Integrate both rates if a request straddles recovery. The budget is
        // a declared fixture policy, not a fitted Azure account limit.
        let burst = if elapsed < 2.0 { 8 * PARTITION } else { BLOB };
        state.tokens = reference_rate
            .mul_add(0.1_f64.mul_add(low, 1.2 * high), state.tokens)
            .min(f64::from(burst));
        state.refilled = now;
    }

    async fn quiesce(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let before = {
                    let state = self.state.lock().unwrap();
                    (
                        state.active,
                        state.warmup.gets + state.read.gets,
                        state.warmup.bytes + state.read.bytes,
                    )
                };
                if before.0 != 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                }
                // SDK tasks spawned before the store returned may not have
                // entered HTTP yet. Require no requests or body progress over
                // a quiet interval, not merely one zero-active snapshot.
                tokio::time::sleep(QUIET_WINDOW).await;
                let state = self.state.lock().unwrap();
                let after = (
                    state.active,
                    state.warmup.gets + state.read.gets,
                    state.warmup.bytes + state.read.bytes,
                );
                if after == before {
                    break;
                }
            }
        })
        .await
        .expect("HTTP requests and SDK bodies must reach an observed quiet interval");
    }
}

struct Body {
    account: Arc<Account>,
    probe: bool,
    phase_start: Instant,
    request_id: usize,
    attempt_key: (u64, u64),
    offset: u32,
    sent: u32,
    start: Instant,
    duration: Duration,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Stream for Body {
    type Item = azure_core::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.sent == PARTITION {
            return Poll::Ready(None);
        }
        let end = this.sent + CHUNK;
        let sleep = this.sleep.get_or_insert_with(|| {
            Box::pin(tokio::time::sleep_until(
                this.start + this.duration.mul_f64(f64::from(end) / f64::from(PARTITION)),
            ))
        });
        if sleep.as_mut().poll(cx).is_pending() {
            return Poll::Pending;
        }
        this.sleep = None;
        let chunk = this
            .account
            .range_body
            .slice(usize::try_from(this.sent).unwrap()..usize::try_from(end).unwrap());
        let mut state = this.account.state.lock().unwrap();
        let closed = state.closed;
        let bin = this.phase_start.elapsed().as_millis() / 100;
        // This body keeps its originating request even when a later outer
        // attempt is active. Never charge it to the latest attempt or phase.
        let ledger = state.ledger_for(this.probe);
        // Charge bytes when this HTTP body yields them to the SDK. Admission
        // alone is not a transfer, and yielding does not prove cache delivery.
        ledger.bytes += u64::from(CHUNK);
        if closed {
            ledger.after_close += u64::from(CHUNK);
        }
        let repeated = !ledger.blocks.insert(this.offset + this.sent);
        if repeated {
            ledger.repeated += u64::from(CHUNK);
        }
        *ledger.bins.entry(bin).or_default() += u64::from(CHUNK);
        ledger.requests[this.request_id].bytes += u64::from(CHUNK);
        let attempt = ledger.attempts.get_mut(&this.attempt_key).unwrap();
        attempt.bytes += u64::from(CHUNK);
        if closed {
            attempt.after_close += u64::from(CHUNK);
        }
        if repeated {
            attempt.repeated += u64::from(CHUNK);
        }
        this.sent = end;
        Poll::Ready(Some(Ok(chunk)))
    }
}

impl Drop for Body {
    fn drop(&mut self) {
        self.account.state.lock().unwrap().active -= 1;
    }
}

#[derive(Debug)]
struct AzureTransport(Arc<Account>);

impl AzureTransport {
    fn requested_range(request: &Request) -> (u32, u32) {
        let range = request
            .headers()
            .iter()
            .find(|(key, _)| {
                key.as_str().eq_ignore_ascii_case("range")
                    || key.as_str().eq_ignore_ascii_case("x-ms-range")
            })
            .unwrap()
            .1
            .as_str();
        let (start, end) = range
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let offset: u32 = start.parse().unwrap();
        let end = (end.parse::<u32>().unwrap() + 1).min(BLOB);
        assert_eq!(end - offset, PARTITION, "outside calibrated range size");
        (offset, end)
    }

    fn download(&self, request: &Request, mut headers: Headers) -> AsyncRawResponse {
        let account = &self.0;
        let (offset, end) = Self::requested_range(request);
        let identity = request_attempt(request);
        let attempt_key = (identity.read_id, identity.attempt);

        // Keep refill, admission and the occurrence draw in one critical
        // section so concurrent SDK requests observe the same ordering.
        let mut state = account.state.lock().unwrap();
        let now = Instant::now();
        account.refill(&mut state, now);
        let current_phase = (state.probe, state.probe_start);
        let (probe, phase_start) = *state
            .read_phases
            .entry(identity.read_id)
            .or_insert(current_phase);
        let phase = u64::from(probe);
        state.ledger_for(probe).gets += 1;
        let request_id = state
            .ledger_for(probe)
            .request_started(&identity, offset, end);

        // Refusals come from the shared budget or active-body limit, never a
        // random throttle draw. Refused ranges spend no response quantile.
        if state.tokens < f64::from(PARTITION) || state.active >= 32 {
            state.ledger_for(probe).refused += 1;
            state
                .ledger_for(probe)
                .attempts
                .get_mut(&attempt_key)
                .unwrap()
                .refused += 1;
            headers.insert("x-ms-error-code", "ServerBusy");
            headers.insert("retry-after", "1");
            return AsyncRawResponse::from_bytes(
                StatusCode::ServiceUnavailable,
                headers,
                b"<Error><Code>ServerBusy</Code><Message>Fixture capacity exhausted</Message></Error>"
                    .to_vec(),
            );
        }

        state.tokens -= f64::from(PARTITION);
        state.active += 1;
        state.peak_active = state.peak_active.max(state.active);
        let inflight = state.active;
        let ledger = state.ledger_for(probe);
        ledger.admitted += u64::from(PARTITION);
        ledger.requests[request_id].admitted = true;
        let occurrence = ledger.range_occurrences.entry(offset).or_default();
        let quantile = uniform(
            account.seed,
            phase * 1_000 + u64::from(*occurrence) * 32 + u64::from(offset / PARTITION),
        );
        *occurrence += 1;
        let duration = account.timing.duration(inflight, quantile);
        drop(state);

        headers.insert("content-length", PARTITION.to_string());
        headers.insert(
            "content-range",
            format!("bytes {offset}-{}/{BLOB}", end - 1),
        );
        AsyncRawResponse::new(
            StatusCode::PartialContent,
            headers,
            Box::pin(Body {
                account: account.clone(),
                probe,
                phase_start,
                request_id,
                attempt_key,
                offset,
                sent: 0,
                start: now,
                duration,
                sleep: None,
            }),
        )
    }
}

#[async_trait::async_trait]
impl HttpClient for AzureTransport {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
        let mut headers = Headers::new();
        headers.insert("last-modified", "Thu, 01 Jan 1970 00:00:00 GMT");
        headers.insert("etag", "\"unchanged\"");

        if request.method() == Method::Head {
            headers.insert("content-length", BLOB.to_string());
            return Ok(AsyncRawResponse::from_bytes(
                StatusCode::Ok,
                headers,
                Vec::new(),
            ));
        }

        assert_eq!(request.method(), Method::Get);
        Ok(self.download(request, headers))
    }
}

fn create_store(account: &Arc<Account>) -> TestStore {
    let mut options = BlobContainerClientOptions::default();
    options
        .client_options
        .per_call_policies
        .push(Arc::new(AttemptHeaders));
    // Only NativeLink retries in this fixture. The real SDK still owns the
    // partition requests and the tasks that can outlive the consuming read.
    options.client_options.retry = RetryOptions::none();
    options.client_options.transport =
        Some(Transport::new(Arc::new(AzureTransport(account.clone()))));
    let client = BlobContainerClient::new(
        Url::parse("https://fixture.blob.core.windows.net/cas").unwrap(),
        None,
        Some(options),
    )
    .unwrap();
    let spec = ExperimentalAzureSpec {
        account_name: "fixture".into(),
        container: "cas".into(),
        common: CommonObjectSpec {
            // No separate throttle-budget retry path.
            throttle_retry_budget_s: 0.0,
            retry: Retry {
                max_retries: 6,
                delay: 0.3,
                jitter: 0.5,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let jitter = account.clone();
    AzureBlobStore::<fn() -> MockInstantWrapped>::new_with_client_and_jitter(
        &spec,
        client,
        Arc::new(move |delay| jitter.jitter(delay)),
        MockInstantWrapped::default,
    )
    .unwrap()
}

async fn warm_up(store: &TestStore, account: &Account, digest: DigestInfo) {
    assert_eq!(store.has(digest).await.unwrap(), Some(u64::from(BLOB)));

    // Exercise the cap first, then complete a real SDK read after recovery.
    let warmup = tokio::time::timeout(
        Duration::from_mins(1),
        store.get_part_unchunked(digest, 0, None),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(warmup.len(), usize::try_from(BLOB).unwrap());
    drop(warmup);
    account.quiesce().await;
    tokio::time::sleep_until(account.start + RECOVERY + Duration::from_secs(1)).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Separate the initial throttling from the assertion phase. Its bucket is
    // now large enough for a whole blob, and its counters start independently.
    {
        let mut state = account.state.lock().unwrap();
        assert!(state.warmup.refused > 0, "the cap must actually throttle");
        state.probe = true;
        state.probe_start = Instant::now();
    }
}

struct ProbeResult {
    delivered: u64,
    returned: Duration,
    bytes_written: u64,
}

async fn read_probe(
    store: &TestStore,
    account: &Account,
    digest: DigestInfo,
    close: bool,
) -> ProbeResult {
    let (mut writer, mut reader) = make_buf_channel_pair();
    let read = store.get_part(digest, &mut writer, 0, None);
    let consume = async {
        let mut delivered = 0_u64;
        loop {
            let chunk = reader.recv().await.unwrap();
            if chunk.is_empty() {
                break;
            }
            delivered += u64::try_from(chunk.len()).unwrap();
            if close {
                // Mark the consumer boundary immediately before dropping it.
                // Existing SDK tasks may still yield bytes after this point.
                account.state.lock().unwrap().closed = true;
                drop(reader);
                break;
            }
        }
        delivered
    };
    let (result, delivered) = tokio::time::timeout(Duration::from_secs(90), async {
        tokio::join!(read, consume)
    })
    .await
    .expect("bounded production read");
    let returned = account.state.lock().unwrap().probe_start.elapsed();
    if close {
        assert_eq!(result.unwrap_err().code, Code::Aborted);
        assert!(writer.is_pipe_broken());
        assert_eq!(delivered, u64::from(PARTITION));
    } else {
        result.unwrap();
        assert_eq!(delivered, u64::from(BLOB));
    }
    ProbeResult {
        delivered,
        returned,
        bytes_written: writer.get_bytes_written(),
    }
}

fn attempt_rows(ledger: &Ledger) -> Vec<Value> {
    ledger
        .attempts
        .iter()
        .map(|(&(read_id, attempt), entry)| {
            json!({
                "read_id": read_id,
                "attempt": attempt,
                "sent_at_start": entry.sent,
                "writer_broken_at_start": entry.writer_broken,
                "gets": entry.gets,
                "refused": entry.refused,
                "body_bytes": entry.bytes,
                "after_close_bytes": entry.after_close,
                "repurchased_bytes": entry.repeated,
            })
        })
        .collect()
}

fn request_rows(ledger: &Ledger) -> Vec<Value> {
    ledger
        .requests
        .iter()
        .enumerate()
        .map(|(request_id, request)| {
            json!({
                "request_id": request_id,
                "read_id": request.read_id,
                "attempt": request.attempt,
                "range_start": request.offset,
                "range_end_exclusive": request.end,
                "admitted": request.admitted,
                "body_bytes": request.bytes,
            })
        })
        .collect()
}

fn report(
    account: &Account,
    seed: u64,
    close: bool,
    probe: &ProbeResult,
    process_cpus: usize,
) -> Value {
    let state = account.state.lock().unwrap();
    for ledger in [&state.warmup, &state.read] {
        ledger.assert_conserved();
    }
    assert!(state.read.repeated <= state.read.after_close);
    assert!(probe.delivered <= state.read.bytes);
    if !close {
        assert_eq!(state.read.bytes, u64::from(BLOB));
    }

    let forbidden: Vec<_> = state
        .read
        .attempts
        .iter()
        .filter(|(_, attempt)| attempt.writer_broken)
        .collect();
    // Once a send has broken this writer, no later attempt can advance it.
    // This is separate from reader delivery: the channel may have buffered a
    // larger prefix before its consumer received one partition and closed.
    for (_, attempt) in &forbidden {
        assert_eq!(attempt.sent, probe.bytes_written);
    }
    let forbidden_bytes: u64 = forbidden.iter().map(|(_, attempt)| attempt.bytes).sum();
    let first_forbidden = forbidden.first().map(|((read_id, attempt), entry)| {
        json!({
            "read_id": read_id,
            "attempt": attempt,
            "sent_at_start": entry.sent,
            "writer_bytes_written_at_return": probe.bytes_written,
            "additional_written_bytes": probe.bytes_written - entry.sent,
            "body_bytes": entry.bytes,
        })
    });

    json!({
        "seed": seed,
        "closed": close,
        "delivered_bytes": probe.delivered,
        "writer_bytes_written": probe.bytes_written,
        "body_bytes": state.read.bytes,
        "after_close_bytes": state.read.after_close,
        "repurchased_bytes": state.read.repeated,
        "gets": state.read.gets,
        "refused": state.read.refused,
        "retries": state.read.retries(),
        "jitter_draws": state.read.jitter_draws,
        "return_seconds": probe.returned.as_secs_f64(),
        "warmup_refused": state.warmup.refused,
        "peak_inflight": state.peak_active,
        "bytes_by_100ms_bin": state.read.bins,
        "process_cpus_at_start": process_cpus,
        "sdk_parallelism_from_cpu_snapshot": process_cpus.clamp(8, 96),
        "fixture_active_body_limit": 32,
        "observed_quiet_window_ms": QUIET_WINDOW.as_millis(),
        "forbidden_attempts": forbidden.len(),
        "forbidden_attempt_body_bytes": forbidden_bytes,
        "max_observed_attempt": state.read.attempts.keys().map(|(_, attempt)| *attempt).max(),
        "first_forbidden_attempt": first_forbidden,
        "attempts": attempt_rows(&state.read),
        "requests": request_rows(&state.read),
        "warmup_attempts": attempt_rows(&state.warmup),
        "attempt_identity_boundary": "AzureBlobStore context at download start, copied into each SDK HTTP request",
    })
}

async fn run(seed: u64, close: bool, process_cpus: usize) -> Value {
    let account = Account::new(seed);
    let store = create_store(&account);
    let digest = DigestInfo::try_new(&"00".repeat(32), BLOB).unwrap();

    warm_up(&store, &account, digest).await;
    let probe = read_probe(&store, &account, digest, close).await;
    account.quiesce().await;

    report(&account, seed, close, &probe, process_cpus)
}

#[nativelink_test(start_paused = true)]
#[expect(
    clippy::print_stdout,
    reason = "print every seed and byte ledger for replay and comparison"
)]
async fn a_closed_destination_does_not_repurchase_input_ranges() {
    let mut violations = Vec::new();
    // The real SDK chooses its default from process-visible parallelism. The
    // fixture's independent 32-body admission limit keeps fitted overlap in
    // range; it does not replace or fix the SDK's concurrency.
    let process_cpus = std::thread::available_parallelism().map_or(1, core::num::NonZeroUsize::get);
    println!(
        "process_cpus={process_cpus}, sdk_default_parallelism={}",
        process_cpus.clamp(8, 96)
    );
    for index in 0..SEEDS {
        let seed = 2_026_101_700 + u64::from(index);
        // Rotate arm order even with virtual time; draws remain paired by key.
        for close in if index % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let row = run(seed, close, process_cpus).await;
            println!("{row}");
            let repurchased = row["repurchased_bytes"].as_u64().unwrap();
            let retries = row["retries"].as_u64().unwrap();
            let forbidden = row["forbidden_attempts"].as_u64().unwrap();
            if forbidden != 0 || repurchased != 0 || retries != 0 {
                violations.push(json!({
                    "seed": seed,
                    "closed": close,
                    "forbidden_attempts": forbidden,
                    "forbidden_attempt_body_bytes": row["forbidden_attempt_body_bytes"],
                    "first_forbidden_attempt": row["first_forbidden_attempt"],
                    "repurchased_bytes": repurchased,
                    "retries": retries,
                }));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "a download started with an already-broken writer, or a secondary retry/range invariant failed: {violations:#?}"
    );
}
