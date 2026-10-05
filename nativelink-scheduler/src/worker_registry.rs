// Copyright 2024 The NativeLink Authors. All rights reserved.
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

use core::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_lock::RwLock;
use nativelink_util::action_messages::WorkerId;
use tracing::{debug, trace};

/// Ceiling for an action whose worker no instance here recognises. Long on
/// purpose: heartbeats never reach the shared state, so a healthy hour-long
/// action and an abandoned one look identical from here, and acting early
/// costs live work.
pub const ORPHANED_ACTION_TIMEOUT: Duration = Duration::from_hours(1);

/// What this scheduler instance knows about a worker's liveness.
///
/// A worker only appears in the registry of the instance holding its
/// `connect_worker` stream, so `Unknown` means "not mine to judge", not
/// "dead".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerLiveness {
    /// Registered here and has heartbeat within the timeout.
    Alive,
    /// Registered here, but has not been heard from within the timeout.
    Stale,
    /// Not registered here at all. Either connected to a different scheduler
    /// instance, or already evicted (in which case its actions were requeued
    /// at eviction time and no longer reference it).
    Unknown,
}

/// In-memory worker registry that tracks worker liveness.
///
/// Per-process: fed by the `connect_worker` streams this instance owns, not a
/// view of every worker in the deployment.
#[derive(Debug)]
pub struct WorkerRegistry {
    workers: RwLock<HashMap<WorkerId, SystemTime>>,
}

impl Default for WorkerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerRegistry {
    /// Creates a new worker registry.
    pub fn new() -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
        }
    }

    /// Advances a worker's heartbeat to `now`. Monotone: a late or out-of-order
    /// keepalive never regresses `last_seen`. The liveness signal is the MAX of
    /// receipt times — a monotone fact — and must not move backward and
    /// manufacture a false `Stale` (pairs with the TL1 fix in
    /// `ApiWorkerScheduler::refresh_worker`, which now stamps this on receipt,
    /// possibly ahead of `refresh_lifetime`'s own out-of-order rejection).
    pub async fn update_worker_heartbeat(&self, worker_id: &WorkerId, now: SystemTime) {
        let mut workers = self.workers.write().await;
        workers
            .entry(worker_id.clone())
            .and_modify(|prev| {
                if now > *prev {
                    *prev = now;
                }
            })
            .or_insert(now);
        trace!(?worker_id, now = %humantime::format_rfc3339(now), "FLOW: Worker heartbeat updated in registry");
    }

    pub async fn register_worker(&self, worker_id: &WorkerId, now: SystemTime) {
        let mut workers = self.workers.write().await;
        workers.insert(worker_id.clone(), now);
        debug!(?worker_id, "FLOW: Worker registered in registry");
    }

    pub async fn remove_worker(&self, worker_id: &WorkerId) {
        let mut workers = self.workers.write().await;
        workers.remove(worker_id);
        debug!(?worker_id, "FLOW: Worker removed from registry");
    }

    /// Liveness as far as THIS instance can tell. Callers acting on "dead"
    /// must handle `Unknown` separately.
    pub async fn check_liveness(
        &self,
        worker_id: &WorkerId,
        timeout: Duration,
        now: SystemTime,
    ) -> WorkerLiveness {
        let workers = self.workers.read().await;

        let Some(last_seen) = workers.get(worker_id) else {
            trace!(?worker_id, "FLOW: Worker not registered on this instance");
            return WorkerLiveness::Unknown;
        };

        // An overflowing deadline can only be clock skew; treat as alive.
        let liveness = match last_seen.checked_add(timeout) {
            Some(deadline) if deadline > now => WorkerLiveness::Alive,
            Some(_) => WorkerLiveness::Stale,
            None => WorkerLiveness::Alive,
        };
        trace!(
            ?worker_id,
            last_seen = %humantime::format_rfc3339(*last_seen),
            ?timeout,
            ?liveness,
            "FLOW: Worker liveness check"
        );
        liveness
    }

    pub async fn is_worker_alive(
        &self,
        worker_id: &WorkerId,
        timeout: Duration,
        now: SystemTime,
    ) -> bool {
        self.check_liveness(worker_id, timeout, now).await == WorkerLiveness::Alive
    }

    /// Test-only: hold the internal write lock so that concurrent liveness
    /// reads park at a deterministic await point.
    #[cfg(test)]
    pub(crate) async fn write_lock_for_test(
        &self,
    ) -> async_lock::RwLockWriteGuard<'_, HashMap<WorkerId, SystemTime>> {
        self.workers.write().await
    }

    pub async fn get_worker_last_seen(&self, worker_id: &WorkerId) -> Option<SystemTime> {
        let workers = self.workers.read().await;
        workers.get(worker_id).copied()
    }
}

pub type SharedWorkerRegistry = Arc<WorkerRegistry>;

#[cfg(test)]
mod tests {
    use nativelink_macro::nativelink_test;

    use super::*;

    #[nativelink_test]
    async fn test_worker_heartbeat() {
        let registry = WorkerRegistry::new();
        let worker_id = WorkerId::from(String::from("test"));
        let now = SystemTime::now();

        // Worker not registered yet
        assert!(
            !registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );

        // Register worker
        registry.register_worker(&worker_id, now).await;
        assert!(
            registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );

        // Check with expired timeout
        let future = now.checked_add(Duration::from_secs(10)).unwrap();
        assert!(
            !registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), future)
                .await
        );

        // Update heartbeat
        registry.update_worker_heartbeat(&worker_id, future).await;
        assert!(
            registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), future)
                .await
        );
    }

    #[nativelink_test]
    async fn test_check_liveness_distinguishes_stale_from_unknown() {
        let registry = WorkerRegistry::new();
        let mine = WorkerId::from(String::from("mine"));
        let theirs = WorkerId::from(String::from("belongs-to-another-instance"));
        let now = SystemTime::now();
        let timeout = Duration::from_secs(5);

        // Never registered here. This is the case that must NOT read as dead:
        // with several schedulers on shared state it is a peer's worker.
        assert_eq!(
            registry.check_liveness(&theirs, timeout, now).await,
            WorkerLiveness::Unknown
        );

        registry.register_worker(&mine, now).await;
        assert_eq!(
            registry.check_liveness(&mine, timeout, now).await,
            WorkerLiveness::Alive
        );

        // Registered here but gone quiet: genuinely ours and genuinely dead.
        let later = now.checked_add(Duration::from_secs(10)).unwrap();
        assert_eq!(
            registry.check_liveness(&mine, timeout, later).await,
            WorkerLiveness::Stale
        );

        // Eviction takes it back to Unknown, not Stale.
        registry.remove_worker(&mine).await;
        assert_eq!(
            registry.check_liveness(&mine, timeout, later).await,
            WorkerLiveness::Unknown
        );
    }

    #[nativelink_test]
    async fn test_remove_worker() {
        let registry = WorkerRegistry::new();
        let worker_id = WorkerId::from(String::from("test-worker"));
        let now = SystemTime::now();

        registry.register_worker(&worker_id, now).await;
        assert!(
            registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );

        registry.remove_worker(&worker_id).await;
        assert!(
            !registry
                .is_worker_alive(&worker_id, Duration::from_secs(5), now)
                .await
        );
    }

    // TL1 regression (monotone half): an out-of-order / late keepalive must not
    // regress `last_seen` and manufacture a false `Stale`. Fails WITHOUT the
    // monotone fix (plain `insert` overwrites with the older timestamp, so the
    // liveness check past the old deadline reads Stale).
    #[nativelink_test]
    async fn heartbeat_is_monotone_never_regresses() {
        let registry = WorkerRegistry::new();
        let w = WorkerId::from(String::from("w"));
        let timeout = Duration::from_secs(5);
        let t0 = SystemTime::now();
        let t_late = t0.checked_add(Duration::from_secs(10)).unwrap();

        registry.register_worker(&w, t0).await;
        registry.update_worker_heartbeat(&w, t_late).await; // advance
        registry.update_worker_heartbeat(&w, t0).await; // out-of-order (older) — must NOT regress

        assert_eq!(
            registry.get_worker_last_seen(&w).await,
            Some(t_late),
            "an older keepalive regressed last_seen — manufactures false Stale (TL1)"
        );
        // A check 1s past the OLD deadline (t0 + timeout) must still be Alive,
        // because the real last_seen is t_late.
        let past_old_deadline = t0
            .checked_add(timeout)
            .unwrap()
            .checked_add(Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            registry
                .check_liveness(&w, timeout, past_old_deadline)
                .await,
            WorkerLiveness::Alive
        );
    }
}
