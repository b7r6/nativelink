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

//! What a run simulates: the timeout mode, the scenario, and the workload.

use core::fmt;
use core::str::FromStr;
use core::time::Duration;

/// One day, the "absurd" value for every timeout that should not decide
/// correctness.
pub const ONE_DAY_S: u64 = 24 * 60 * 60;

/// Which scheduler timeouts the run uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Mode {
    /// Production-like values: timeouts are allowed to decide liveness.
    Normal,
    /// `worker_timeout_s`, `max_action_executing_timeout_s` and
    /// `client_action_timeout_s` set to a day: no timeout can fire within a
    /// run, so anything that only a timeout would unstick stays stuck.
    Absurd,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Normal => "normal",
            Self::Absurd => "absurd",
        })
    }
}

impl FromStr for Mode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "normal" => Ok(Self::Normal),
            "absurd" => Ok(Self::Absurd),
            other => Err(format!("unknown mode {other:?} (normal|absurd)")),
        }
    }
}

/// The faults a run injects. Every scenario runs the same workload; the
/// scenario picks which faults the seeded plan may draw.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scenario {
    /// No faults. Clients still detach and reattach (within retention).
    Calm,
    /// Clients also detach for longer than retention and resubmit.
    LongDetach,
    /// Some workers run every action ten times slower than its duration.
    SlowWorkers,
    /// The scheduler's wall clock steps forward and back.
    ClockSkew,
    /// Workers are killed (connections close) and restarted.
    WorkerCrash,
    /// The scheduler is killed and restarted on its durable state.
    SchedulerRestart,
    /// Links are held (messages delayed until heal; TCP survives).
    Partition,
    /// Links drop everything for a while: connections die silently.
    Blackhole,
    /// A worker stops sending anything on a live connection for a while,
    /// then resumes and reports what it ran.
    WorkerStall,
    /// A worker ignores kills and reports revoked attempts late.
    Zombie,
    /// Every fault above except clock steps, drawn at random.
    Chaos,
}

impl Scenario {
    pub const ALL: [Self; 11] = [
        Self::Calm,
        Self::LongDetach,
        Self::SlowWorkers,
        Self::ClockSkew,
        Self::WorkerCrash,
        Self::SchedulerRestart,
        Self::Partition,
        Self::Blackhole,
        Self::WorkerStall,
        Self::Zombie,
        Self::Chaos,
    ];

    /// Whether the scenario kills or partitions something. The fenced
    /// track's pass condition: with absurd timeouts, a scenario that does
    /// not must pass every check.
    pub const fn is_destructive(self) -> bool {
        !matches!(
            self,
            Self::Calm | Self::LongDetach | Self::SlowWorkers | Self::ClockSkew
        )
    }
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Calm => "calm",
            Self::LongDetach => "long-detach",
            Self::SlowWorkers => "slow-workers",
            Self::ClockSkew => "clock-skew",
            Self::WorkerCrash => "worker-crash",
            Self::SchedulerRestart => "scheduler-restart",
            Self::Partition => "partition",
            Self::Blackhole => "blackhole",
            Self::WorkerStall => "worker-stall",
            Self::Zombie => "zombie",
            Self::Chaos => "chaos",
        })
    }
}

impl FromStr for Scenario {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|scenario| scenario.to_string() == s)
            .ok_or_else(|| {
                let names: Vec<String> = Self::ALL.iter().map(ToString::to_string).collect();
                format!("unknown scenario {s:?} ({})", names.join("|"))
            })
    }
}

/// The scheduler settings that are timeouts, per mode.
#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub worker_timeout_s: u64,
    pub max_action_executing_timeout_s: u64,
    pub client_action_timeout_s: u64,
    pub retain_completed_for_s: u32,
    pub unacknowledged_kill_timeout_s: u64,
    pub dispatch_ack_timeout_s: u64,
}

impl Timeouts {
    pub const fn for_mode(mode: Mode) -> Self {
        match mode {
            Mode::Normal => Self {
                worker_timeout_s: 10,
                max_action_executing_timeout_s: 120,
                client_action_timeout_s: 60,
                retain_completed_for_s: 60,
                unacknowledged_kill_timeout_s: 60,
                dispatch_ack_timeout_s: 0,
            },
            Mode::Absurd => Self {
                worker_timeout_s: ONE_DAY_S,
                max_action_executing_timeout_s: ONE_DAY_S,
                client_action_timeout_s: ONE_DAY_S,
                retain_completed_for_s: 60,
                unacknowledged_kill_timeout_s: 60,
                dispatch_ack_timeout_s: 0,
            },
        }
    }

    /// How long a detached client keeps its interest in an operation: the
    /// operation may be collected (`NOT_FOUND`) or abandoned only after this.
    pub fn interest_window(&self) -> Duration {
        Duration::from_secs(
            u64::from(self.retain_completed_for_s).min(self.client_action_timeout_s),
        )
    }
}

/// Everything about a run except the seed-drawn details.
#[derive(Clone, Copy, Debug)]
pub struct SimConfig {
    pub mode: Mode,
    pub scenario: Scenario,
    pub timeouts: Timeouts,
    /// Clients see `EXECUTING` only once the worker accepted (P2 opt-in).
    pub report_executing_on_accept: bool,
    pub workers: usize,
    pub slots_per_worker: usize,
    pub clients: usize,
    pub jobs_per_client: usize,
    /// Faults are drawn in `[0, active)`; every fault is healed by `active`.
    pub active: Duration,
    /// After `active`, how long the run may go on for liveness.
    pub quiet: Duration,
    /// The simulation step.
    pub tick: Duration,
    /// Worker keepalive interval.
    pub worker_keepalive: Duration,
    /// Margin taken off the interest window before a `NOT_FOUND` counts as
    /// a lost operation (keepalive granularity and network delay).
    pub interest_slack: Duration,
}

impl SimConfig {
    pub const fn new(mode: Mode, scenario: Scenario) -> Self {
        Self {
            mode,
            scenario,
            timeouts: Timeouts::for_mode(mode),
            report_executing_on_accept: true,
            workers: 3,
            slots_per_worker: 2,
            clients: 3,
            jobs_per_client: 8,
            active: Duration::from_secs(240),
            quiet: Duration::from_secs(240),
            tick: Duration::from_millis(10),
            worker_keepalive: Duration::from_secs(2),
            interest_slack: Duration::from_secs(25),
        }
    }
}
