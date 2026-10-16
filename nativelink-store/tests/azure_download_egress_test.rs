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
//! The invariant is zero re-purchased bytes in the latter read. Requests already
//! in flight may finish; only overlapping bytes actually yielded to the SDK
//! count as re-purchased. No copied store, Python, cloud access, or model of the
//! expected amplification is involved. Paused time preserves production retry
//! delays while keeping thirty seeded pairs suitable for CI.

use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use azure_core::http::headers::Headers;
use azure_core::http::{
    AsyncRawResponse, HttpClient, Method, Request, RetryOptions, StatusCode, Transport, Url,
};
use azure_storage_blob::clients::{BlobContainerClient, BlobContainerClientOptions};
use bytes::Bytes;
use futures::{Future, Stream};
use nativelink_config::stores::{CommonObjectSpec, ExperimentalAzureSpec, Retry};
use nativelink_error::Code;
use nativelink_macro::nativelink_test;
use nativelink_store::azure_blob_store::AzureBlobStore;
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
const SEEDS: u32 = 30;

// SplitMix64: each (seed, range, occurrence) gets the same latency quantile in
// both arms, independent of HTTP task polling order. Retry jitter uses a distinct
// stream and the production multiplier formula.
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
    attempts: BTreeMap<u32, u32>,
    retries: u32,
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
    warmup: Ledger,
    read: Ledger,
}

impl State {
    const fn ledger(&mut self) -> &mut Ledger {
        if self.probe {
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
                warmup: Ledger::default(),
                read: Ledger::default(),
            }),
        })
    }

    fn jitter(&self, nominal: Duration) -> Duration {
        let mut state = self.state.lock().unwrap();
        let phase = u64::from(state.probe);
        let index = state.ledger().retries;
        state.ledger().retries += 1;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "production jitter uses f32"
        )]
        let u = uniform(self.seed, 10_000 + phase * 100 + u64::from(index)) as f32;
        nominal.mul_f32(0.5_f32.mul_add(u - 0.5, 1.0))
    }

    async fn quiesce(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if self.state.lock().unwrap().active == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // The SDK coordinator may observe a completion and start another
            // range. Yield before accepting quiescence.
            tokio::task::yield_now().await;
            assert_eq!(self.state.lock().unwrap().active, 0);
        })
        .await
        .expect("SDK bodies must finish");
    }
}

struct Body {
    account: Arc<Account>,
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
        let bin = state.probe_start.elapsed().as_millis() / 100;
        let ledger = state.ledger();
        ledger.bytes += u64::from(CHUNK);
        if closed {
            ledger.after_close += u64::from(CHUNK);
        }
        if !ledger.blocks.insert(this.offset + this.sent) {
            ledger.repeated += u64::from(CHUNK);
        }
        *ledger.bins.entry(bin).or_default() += u64::from(CHUNK);
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

#[async_trait::async_trait]
impl HttpClient for AzureTransport {
    async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
        let account = &self.0;
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

        let mut state = account.state.lock().unwrap();
        let now = Instant::now();
        let elapsed = now.duration_since(account.start).as_secs_f64();
        let previous = state.refilled.duration_since(account.start).as_secs_f64();
        let reference_rate = f64::from(BLOB) / (account.timing.baseline_ms(32) / 1000.0);
        let low = elapsed.min(2.0) - previous.min(2.0);
        let high = (elapsed - 2.0).max(0.0) - (previous - 2.0).max(0.0);
        // An explicit fixture policy, not a fitted Azure account limit. No
        // random 503s: all refusals follow from insufficient tokens/slots.
        let burst = if elapsed < 2.0 { 8 * PARTITION } else { BLOB };
        state.tokens = reference_rate
            .mul_add(0.1_f64.mul_add(low, 1.2 * high), state.tokens)
            .min(f64::from(burst));
        state.refilled = now;
        let phase = u64::from(state.probe);
        state.ledger().gets += 1;
        if state.tokens < f64::from(PARTITION) || state.active >= 32 {
            state.ledger().refused += 1;
            headers.insert("x-ms-error-code", "ServerBusy");
            headers.insert("retry-after", "1");
            return Ok(AsyncRawResponse::from_bytes(StatusCode::ServiceUnavailable, headers,
                b"<Error><Code>ServerBusy</Code><Message>Fixture capacity exhausted</Message></Error>".to_vec()));
        }
        state.tokens -= f64::from(PARTITION);
        state.active += 1;
        state.peak_active = state.peak_active.max(state.active);
        let inflight = state.active;
        let ledger = state.ledger();
        ledger.admitted += u64::from(PARTITION);
        let occurrence = ledger.attempts.entry(offset).or_default();
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
        Ok(AsyncRawResponse::new(
            StatusCode::PartialContent,
            headers,
            Box::pin(Body {
                account: account.clone(),
                offset,
                sent: 0,
                start: now,
                duration,
                sleep: None,
            }),
        ))
    }
}

async fn run(seed: u64, close: bool) -> Value {
    let account = Account::new(seed);
    let mut options = BlobContainerClientOptions::default();
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
    let store = AzureBlobStore::new_with_client_and_jitter(
        &spec,
        client,
        Arc::new(move |delay| jitter.jitter(delay)),
        MockInstantWrapped::default,
    )
    .unwrap();
    let digest = DigestInfo::try_new(&"00".repeat(32), BLOB).unwrap();
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
    {
        let mut state = account.state.lock().unwrap();
        assert!(state.warmup.refused > 0, "the cap must actually throttle");
        state.probe = true;
        state.probe_start = Instant::now();
    }
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
    account.quiesce().await;
    let state = account.state.lock().unwrap();
    for ledger in [&state.warmup, &state.read] {
        assert_eq!(ledger.bytes, ledger.bins.values().sum::<u64>());
        assert_eq!(
            ledger.bytes,
            u64::try_from(ledger.blocks.len()).unwrap() * u64::from(CHUNK) + ledger.repeated
        );
        assert!(ledger.bytes <= ledger.admitted);
        assert_eq!(
            ledger.admitted,
            u64::from(ledger.gets - ledger.refused) * u64::from(PARTITION)
        );
    }
    assert!(state.read.repeated <= state.read.after_close);
    assert!(delivered <= state.read.bytes);
    if !close {
        assert_eq!(state.read.bytes, u64::from(BLOB));
    }
    json!({"seed":seed,"closed":close,"delivered_bytes":delivered,"body_bytes":state.read.bytes,
        "after_close_bytes":state.read.after_close,"repurchased_bytes":state.read.repeated,
        "gets":state.read.gets,"refused":state.read.refused,"retries":state.read.retries,
        "return_seconds":returned.as_secs_f64(),"warmup_refused":state.warmup.refused,
        "peak_inflight":state.peak_active,"bytes_by_100ms_bin":state.read.bins})
}

#[nativelink_test(start_paused = true)]
#[expect(
    clippy::print_stdout,
    reason = "print every seed and byte ledger for replay and comparison"
)]
async fn a_closed_destination_does_not_repurchase_input_ranges() {
    let mut violations = Vec::new();
    println!(
        "process_cpus={}",
        std::thread::available_parallelism().unwrap()
    );
    for index in 0..SEEDS {
        let seed = 2_026_101_700 + u64::from(index);
        // Rotate arm order even with virtual time; draws remain paired by key.
        for close in if index % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let row = run(seed, close).await;
            println!("{row}");
            if row["repurchased_bytes"].as_u64().unwrap() != 0 {
                violations.push(seed);
            }
        }
    }
    assert!(
        violations.is_empty(),
        "re-purchased bytes after destination loss: {violations:?}"
    );
}
