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

//! Experiment-only diagnostics for the fast-tier fill failure test
//! (`test/fill-*` branches). Process-wide counters that the Azure HTTP
//! transport, the Azure store's read retries, the fast/slow data pump and the
//! filesystem store's writes bump, so a driver can sample them on a timer.
//! Events go out on the `fill` tracing target.

use core::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// HTTP requests the Azure SDK handed to the transport.
pub static HTTP_REQUESTS: AtomicU64 = AtomicU64::new(0);
/// Requests started whose response body has not finished or been dropped.
pub static HTTP_INFLIGHT: AtomicU64 = AtomicU64::new(0);
/// High-water mark of `HTTP_INFLIGHT`.
pub static HTTP_INFLIGHT_MAX: AtomicU64 = AtomicU64::new(0);
/// Response body bytes the transport handed up to the SDK.
pub static HTTP_RESPONSE_BYTES: AtomicU64 = AtomicU64::new(0);
/// Response bodies read to their end.
pub static HTTP_BODIES_COMPLETED: AtomicU64 = AtomicU64::new(0);
/// Response bodies dropped before their end.
pub static HTTP_BODIES_DROPPED: AtomicU64 = AtomicU64::new(0);
/// Download attempts `AzureBlobStore::get_part` started, retries included.
pub static AZURE_READ_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
/// Bytes `AzureBlobStore::get_part` sent into its writer.
pub static AZURE_DELIVERED_BYTES: AtomicU64 = AtomicU64::new(0);
/// Bytes the fast/slow pump took from the slow store.
pub static FASTSLOW_SLOW_BYTES: AtomicU64 = AtomicU64::new(0);
/// Bytes the filesystem store accepted into temp files (tokio may still fail
/// the last one, it reports a write error on the next call).
pub static FS_BYTES_ACCEPTED: AtomicU64 = AtomicU64::new(0);
/// Filesystem store write or flush failures.
pub static FS_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);

/// Wall clock in milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Marks one more request in flight and returns the new count.
pub fn inflight_inc() -> u64 {
    let n = HTTP_INFLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
    HTTP_INFLIGHT_MAX.fetch_max(n, Ordering::Relaxed);
    n
}

/// Marks one request finished and returns the new count.
pub fn inflight_dec() -> u64 {
    HTTP_INFLIGHT.fetch_sub(1, Ordering::Relaxed).saturating_sub(1)
}

/// Adds `n` to `counter`.
pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// Reads `counter`.
#[must_use]
pub fn get(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
