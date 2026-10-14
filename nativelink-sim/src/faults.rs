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

//! The fault plan: every fault the run injects, drawn from the seed up front,
//! each one healed before the active phase ends.

use core::fmt;
use core::ops::Range;
use core::time::Duration;

use rand::Rng;
use rand::rngs::StdRng;

use crate::config::{Scenario, SimConfig};
use crate::scheduler::SCHEDULER_HOST;

/// One change the injector makes to the simulation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FaultAction {
    /// Kill a worker host (its connections close).
    CrashWorker(usize),
    /// Start a crashed worker host again (a new process, new sessions).
    RestartWorker(usize),
    /// Kill the scheduler host.
    CrashScheduler,
    /// Start the scheduler again on its durable state.
    RestartScheduler,
    /// Hold every message between the scheduler and a host; TCP survives.
    Hold(String),
    /// Deliver the held messages.
    Release(String),
    /// Drop every message between the scheduler and a host: open
    /// connections die silently, new ones cannot be made.
    Cut(String),
    /// Let messages through again.
    Repair(String),
    /// Step the scheduler's wall clock to `offset_ms` from true time.
    ClockOffset(i64),
    /// Freeze or thaw a worker's software.
    Stall(usize, bool),
}

impl fmt::Display for FaultAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CrashWorker(w) => write!(f, "crash w{w}"),
            Self::RestartWorker(w) => write!(f, "restart w{w}"),
            Self::CrashScheduler => write!(f, "crash {SCHEDULER_HOST}"),
            Self::RestartScheduler => write!(f, "restart {SCHEDULER_HOST}"),
            Self::Hold(host) => write!(f, "hold {SCHEDULER_HOST}<->{host}"),
            Self::Release(host) => write!(f, "release {SCHEDULER_HOST}<->{host}"),
            Self::Cut(host) => write!(f, "cut {SCHEDULER_HOST}<->{host}"),
            Self::Repair(host) => write!(f, "repair {SCHEDULER_HOST}<->{host}"),
            Self::ClockOffset(ms) => write!(f, "clock-offset {ms}ms"),
            Self::Stall(w, true) => write!(f, "stall w{w}"),
            Self::Stall(w, false) => write!(f, "unstall w{w}"),
        }
    }
}

/// Faults in time order.
#[derive(Clone, Debug, Default)]
pub struct FaultPlan {
    pub events: Vec<(Duration, FaultAction)>,
}

/// Up to `count` non-overlapping windows inside `[from, until)`, each
/// `length` long, separated by `gap`.
fn windows(
    rng: &mut StdRng,
    count: usize,
    from: Duration,
    until: Duration,
    gap: Range<u64>,
    length: Range<u64>,
) -> Vec<(Duration, Duration)> {
    let mut out = Vec::new();
    let mut at = from;
    for _ in 0..count {
        let start = at + Duration::from_millis(rng.random_range(gap.clone()));
        let end = start + Duration::from_millis(rng.random_range(length.clone()));
        if end >= until {
            break;
        }
        out.push((start, end));
        at = end;
    }
    out
}

impl FaultPlan {
    /// Draw the plan for `config.scenario`.
    pub fn generate(config: &SimConfig, rng: &mut StdRng) -> Self {
        let mut events = Vec::new();
        let from = Duration::from_secs(5);
        let until = config.active.saturating_sub(Duration::from_secs(5));
        let worker_name = |w: usize| format!("w{w}");
        let client_name = |c: usize| format!("c{c}");
        let scenario = config.scenario;
        let chaos = scenario == Scenario::Chaos;

        // Not in chaos: a clock step alone already breaks retention and
        // client abandonment today (see the README), and would mask what
        // the other faults do.
        if scenario == Scenario::ClockSkew {
            for (start, end) in windows(rng, 3, from, until, 5_000..40_000, 10_000..60_000) {
                let magnitude = rng.random_range(5_000_i64..120_000);
                let offset = if rng.random_bool(0.5) { magnitude } else { -magnitude };
                events.push((start, FaultAction::ClockOffset(offset)));
                events.push((end, FaultAction::ClockOffset(0)));
            }
        }
        if scenario == Scenario::WorkerCrash || chaos {
            for w in 0..config.workers {
                let count = rng.random_range(1..=2);
                for (start, end) in windows(rng, count, from, until, 5_000..90_000, 1_000..30_000) {
                    events.push((start, FaultAction::CrashWorker(w)));
                    events.push((end, FaultAction::RestartWorker(w)));
                }
            }
        }
        if scenario == Scenario::SchedulerRestart || chaos {
            let count = rng.random_range(1..=3);
            for (start, end) in windows(rng, count, from, until, 10_000..70_000, 500..20_000) {
                events.push((start, FaultAction::CrashScheduler));
                events.push((end, FaultAction::RestartScheduler));
            }
        }
        if scenario == Scenario::Partition || scenario == Scenario::Blackhole || chaos {
            let mut hosts: Vec<String> = (0..config.workers).map(worker_name).collect();
            hosts.extend((0..config.clients).map(client_name));
            for host in hosts {
                if !rng.random_bool(0.6) {
                    continue;
                }
                let cut = match scenario {
                    Scenario::Blackhole => true,
                    Scenario::Partition => false,
                    _ => rng.random_bool(0.5),
                };
                for (start, end) in windows(rng, 2, from, until, 5_000..80_000, 2_000..60_000) {
                    if cut {
                        events.push((start, FaultAction::Cut(host.clone())));
                        events.push((end, FaultAction::Repair(host.clone())));
                    } else {
                        events.push((start, FaultAction::Hold(host.clone())));
                        events.push((end, FaultAction::Release(host.clone())));
                    }
                }
            }
        }
        if scenario == Scenario::WorkerStall || scenario == Scenario::Zombie || chaos {
            let stalled: Vec<usize> = if scenario == Scenario::Zombie {
                vec![0]
            } else {
                (0..config.workers).filter(|_| rng.random_bool(0.5)).collect()
            };
            for w in stalled {
                let count = rng.random_range(1..=3);
                for (start, end) in windows(rng, count, from, until, 5_000..60_000, 5_000..90_000) {
                    events.push((start, FaultAction::Stall(w, true)));
                    events.push((end, FaultAction::Stall(w, false)));
                }
            }
        }
        // Stable order: by time, then by the order drawn.
        events.sort_by_key(|(at, _)| *at);
        Self { events }
    }
}
