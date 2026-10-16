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

//! Experiment driver (`test/fill-*` only): builds the stores of a config and
//! makes exactly one `get_part` of one digest through the named store, with a
//! consumer that drains and counts the bytes. No client retry wraps the read.
//! After the read returns, the runtime is kept alive until the Azure
//! transport has had no request in flight for `--quiet-secs`, so requests the
//! SDK left running in detached tasks are measured to their end. A sampler
//! logs the `fill_diag` counters every `--sample-ms`.

use core::sync::atomic::{AtomicU64, Ordering};
use core::time::Duration;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use nativelink_config::cas_server::{CasConfig, StoreConfig};
use nativelink_error::{Error, ResultExt, make_err};
use nativelink_store::default_store_factory::store_factory;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_util::common::DigestInfo;
use nativelink_util::common::fs::set_open_file_limit;
use nativelink_util::fill_diag::{self, get, now_ms};
use nativelink_util::store_trait::StoreLike;
use nativelink_util::telemetry::init_tracing;
use tracing::info;

#[derive(Parser, Debug)]
struct Args {
    /// `CasConfig` JSON5 file holding the stores.
    config: String,
    /// Store to read through.
    #[arg(long)]
    store: String,
    /// Digest hash of the blob.
    #[arg(long)]
    hash: String,
    /// Digest size of the blob.
    #[arg(long)]
    size: u64,
    /// After the read, stop once no HTTP request has been in flight this long.
    #[arg(long, default_value_t = 60)]
    quiet_secs: u64,
    /// Linger at least this long after the read returns.
    #[arg(long, default_value_t = 60)]
    min_linger_secs: u64,
    /// Never linger longer than this after the read returns.
    #[arg(long, default_value_t = 900)]
    max_linger_secs: u64,
    /// Counter sampling interval.
    #[arg(long, default_value_t = 100)]
    sample_ms: u64,
    /// Ablation 4: once the consumer has received this many bytes, drop the
    /// whole read future (caller cancellation, no disk error).
    #[arg(long)]
    cancel_after_bytes: Option<u64>,
    /// With `--cancel-after-bytes`, also wait until at least this many Azure
    /// HTTP requests are in flight, so the drop lands mid-transfer.
    #[arg(long, default_value_t = 0)]
    cancel_min_inflight: u64,
}

/// Pod network counters for eth0 (rx, tx bytes).
fn net_dev() -> (u64, u64) {
    std::fs::read_to_string("/proc/net/dev")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                let l = l.trim_start();
                let rest = l.strip_prefix("eth0:")?;
                let f: Vec<u64> = rest.split_whitespace().filter_map(|x| x.parse().ok()).collect();
                Some((*f.first()?, *f.get(8)?))
            })
        })
        .unwrap_or((0, 0))
}

/// Open file descriptors of this process.
fn fd_count() -> u64 {
    std::fs::read_dir("/proc/self/fd").map_or(0, |d| d.count() as u64)
}

/// Resident set size in bytes.
fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|s| s.split_whitespace().nth(1)?.parse::<u64>().ok())
        .map_or(0, |pages| pages * 4096)
}

/// Bytes the driver's consumer received.
static DELIVERED: AtomicU64 = AtomicU64::new(0);

fn sample(phase: &'static str) {
    let (rx, tx) = net_dev();
    let (sdk_spawned, sdk_alive, sdk_cancelled, sdk_aborts, sdk_last_end_ms, _) =
        nativelink_store::azure_blob_store::fill_sdk_task_counters();
    let tokio_alive = tokio::runtime::Handle::try_current()
        .map_or(0, |h| h.metrics().num_alive_tasks());
    info!(
        target: "fill",
        ev = "res",
        t_ms = now_ms(),
        rx,
        tx,
        fds = fd_count(),
        rss = rss_bytes(),
        tokio_alive,
        sdk_spawned,
        sdk_alive,
        sdk_cancelled,
        sdk_aborts,
        sdk_last_end_ms,
        loaders = get(&fill_diag::POPULATING_LOADERS),
        open_file_permits_used = nativelink_util::fs::get_open_files_for_test(),
        last_http_byte_ms = get(&fill_diag::LAST_HTTP_BYTE_MS),
    );
    info!(
        target: "fill",
        ev = "sample",
        phase,
        t_ms = now_ms(),
        delivered = DELIVERED.load(Ordering::Relaxed),
        az_delivered = get(&fill_diag::AZURE_DELIVERED_BYTES),
        fastslow_slow = get(&fill_diag::FASTSLOW_SLOW_BYTES),
        fs_accepted = get(&fill_diag::FS_BYTES_ACCEPTED),
        fs_failures = get(&fill_diag::FS_WRITE_FAILURES),
        az_attempts = get(&fill_diag::AZURE_READ_ATTEMPTS),
        http_requests = get(&fill_diag::HTTP_REQUESTS),
        http_inflight = get(&fill_diag::HTTP_INFLIGHT),
        http_inflight_max = get(&fill_diag::HTTP_INFLIGHT_MAX),
        http_bytes = get(&fill_diag::HTTP_RESPONSE_BYTES),
        http_completed = get(&fill_diag::HTTP_BODIES_COMPLETED),
        http_dropped = get(&fill_diag::HTTP_BODIES_DROPPED),
    );
}

async fn run(args: Args, cfg: CasConfig) -> Result<(), Error> {
    let available = std::thread::available_parallelism().map_or(0, core::num::NonZero::get);
    info!(
        target: "fill",
        ev = "driver_start",
        t_ms = now_ms(),
        available_parallelism = available,
        sdk_default_parallel = available.clamp(8, 96),
        terminal_downstream = nativelink_store::azure_blob_store::fill_terminal_downstream_enabled(),
        abort_on_drop = nativelink_store::azure_blob_store::fill_sdk_task_counters().5,
        cancel_after_bytes = args.cancel_after_bytes.unwrap_or(0),
        hash = %args.hash,
        size = args.size,
    );

    let store_manager = Arc::new(StoreManager::new());
    for StoreConfig { name, spec } in cfg.stores {
        let store = store_factory(&spec, &store_manager, None)
            .await
            .err_tip(|| format!("Failed to create store '{name}'"))?;
        store_manager.add_store(&name, store)?;
    }
    store_manager.run_post_init().await?;
    let store = store_manager
        .get_store(&args.store)
        .ok_or_else(|| make_err!(nativelink_error::Code::NotFound, "No store {}", args.store))?;
    let digest = DigestInfo::try_new(&args.hash, args.size)?;

    let sample_every = Duration::from_millis(args.sample_ms);
    let sampler = tokio::spawn(async move {
        let mut interval = tokio::time::interval(sample_every);
        loop {
            interval.tick().await;
            sample("run");
        }
    });

    let (tx, mut rx) = make_buf_channel_pair();
    let consumer = async move {
        loop {
            match rx.recv().await {
                Ok(buf) if buf.is_empty() => return Ok::<(), Error>(()),
                Ok(buf) => {
                    DELIVERED.fetch_add(buf.len() as u64, Ordering::Relaxed);
                }
                Err(err) => return Err(err),
            }
        }
    };

    let read_started = Instant::now();
    info!(target: "fill", ev = "read_start", t_ms = now_ms(), key = %digest);
    let mut read = Box::pin(async move {
        tokio::join!(store.get_part(digest, tx, 0, None), consumer)
    });
    let cancel_after = args.cancel_after_bytes;
    let min_inflight = args.cancel_min_inflight;
    let cancel = async move {
        match cancel_after {
            Some(n) => {
                while DELIVERED.load(Ordering::Relaxed) < n
                    || get(&fill_diag::HTTP_INFLIGHT) < min_inflight
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            None => core::future::pending::<()>().await,
        }
    };
    let outcome = tokio::select! {
        out = &mut read => Some(out),
        () = cancel => None,
    };
    let Some((get_res, consumer_res)) = outcome else {
        info!(
            target: "fill",
            ev = "caller_cancel",
            t_ms = now_ms(),
            delivered = DELIVERED.load(Ordering::Relaxed),
            http_inflight = get(&fill_diag::HTTP_INFLIGHT),
        );
        sample("caller_cancel");
        drop(read);
        info!(
            target: "fill",
            ev = "caller_dropped",
            t_ms = now_ms(),
            delivered = DELIVERED.load(Ordering::Relaxed),
        );
        sample("caller_dropped");
        return linger(&args, Instant::now(), sampler).await;
    };
    info!(
        target: "fill",
        ev = "read_end",
        t_ms = now_ms(),
        elapsed_ms = u64::try_from(read_started.elapsed().as_millis()).unwrap_or(u64::MAX),
        get_ok = get_res.is_ok(),
        get_error = %get_res.as_ref().err().map(Error::message_string).unwrap_or_default(),
        consumer_ok = consumer_res.is_ok(),
        consumer_error = %consumer_res.as_ref().err().map(Error::message_string).unwrap_or_default(),
        delivered = DELIVERED.load(Ordering::Relaxed),
    );
    sample("read_end");
    linger(&args, Instant::now(), sampler).await
}

/// Keeps the runtime alive until no HTTP request and no SDK range task has
/// been alive for `--quiet-secs`, so detached work is measured to its end.
async fn linger(
    args: &Args,
    read_ended: Instant,
    sampler: tokio::task::JoinHandle<()>,
) -> Result<(), Error> {
    let mut quiet_since: Option<Instant> = None;
    let reason = loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if get(&fill_diag::HTTP_INFLIGHT) == 0
            && nativelink_store::azure_blob_store::fill_sdk_task_counters().1 == 0
        {
            quiet_since.get_or_insert_with(Instant::now);
        } else {
            quiet_since = None;
        }
        let lingered = read_ended.elapsed();
        if lingered >= Duration::from_secs(args.max_linger_secs) {
            break "max_linger";
        }
        if lingered >= Duration::from_secs(args.min_linger_secs)
            && quiet_since.is_some_and(|q| q.elapsed() >= Duration::from_secs(args.quiet_secs))
        {
            break "quiet";
        }
    };
    sampler.abort();
    sample("final");
    info!(target: "fill", ev = "driver_end", t_ms = now_ms(), reason);
    Ok(())
}

fn main() -> Result<(), Box<dyn core::error::Error>> {
    drop(tokio_rustls::rustls::crypto::ring::default_provider().install_default());
    let args = Args::parse();

    #[expect(clippy::disallowed_methods, reason = "experiment driver runtime")]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    #[expect(clippy::disallowed_methods, reason = "tracing init on driver runtime")]
    runtime.block_on(async { tokio::spawn(async { init_tracing().await }).await? })?;

    let cfg = CasConfig::try_from_json5_file(&args.config)?;
    set_open_file_limit(
        cfg.global
            .as_ref()
            .map_or(4096, |global| global.max_open_files.max(256)),
    );
    #[expect(clippy::disallowed_methods, reason = "driver main")]
    runtime.block_on(run(args, cfg))?;
    Ok(())
}
