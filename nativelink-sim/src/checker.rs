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

//! Safety and liveness checks over a run's history.
//!
//! Safety:
//!
//! - `lost-operation`: a client that still holds interest (detached for less
//!   than the interest window, minus slack) is told `NOT_FOUND`.
//! - `inconsistent-result`: two different results are delivered for one
//!   scheduler operation (to one client or several), or a re-read of a
//!   finished operation shows a different result.
//! - `stale-attempt-result`: a client receives the result of attempt `n`
//!   after a worker had already received `StartExecute` for a later attempt
//!   of the same operation.
//! - `revoked-attempt-result`: a client receives the result of an attempt
//!   whose worker had been told to kill it before it sent the result.
//! - `done-regression`: a finished operation, read again within retention,
//!   is no longer finished.
//! - `capacity-overcommit`: a worker is offered more concurrent attempts
//!   than its slots.
//! - `unattributed-result`: a successful result no simulated attempt sent.
//! - `panic`, `scheduler-error`, `harness-error`.
//!
//! Liveness (after every fault is healed and the quiet period has passed):
//!
//! - `liveness-stuck`: a job without a final answer.
//! - `unexpected-failure`: a job that ended in an error other than running
//!   out of retries or being abandoned by its own client.
//!
//! Stage regressions clients may see: `EXECUTING -> QUEUED` is a retry and
//! is allowed (counted, not flagged); `COMPLETED -> anything` is not.

use core::fmt;
use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use crate::config::SimConfig;
use crate::history::{Event, Outcome, Record, ResultId};

/// A violated property.
#[derive(Clone, Debug)]
pub struct Violation {
    pub kind: String,
    pub detail: String,
    /// The `seq` of the records that show it.
    pub trace: Vec<usize>,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.detail)
    }
}

/// Counters that describe a run without judging it.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub jobs: usize,
    pub done_ok: usize,
    pub done_failed: usize,
    pub gave_up: usize,
    pub not_found: usize,
    pub not_found_after_window: usize,
    pub executing_to_queued: usize,
    pub worker_starts: usize,
    pub kills: usize,
    pub scheduler_boots: usize,
    pub faults: usize,
    /// Offers a worker declined only because of attempts it carried over
    /// from an earlier session (not counted against the scheduler).
    pub over_capacity_carried: usize,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "jobs={} ok={} failed={} gave_up={} not_found={} (after_window={}) exec->queued={} starts={} kills={} boots={} faults={}",
            self.jobs,
            self.done_ok,
            self.done_failed,
            self.gave_up,
            self.not_found,
            self.not_found_after_window,
            self.executing_to_queued,
            self.worker_starts,
            self.kills,
            self.scheduler_boots,
            self.faults
        )
    }
}

/// Why an operation may legitimately end in an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureCause {
    /// `max_job_retries` or `max_worker_loss_retries` ran out.
    RetriesExhausted,
    /// Cancelled for having no client listening (`client_action_timeout_s`).
    Abandoned,
    Other,
}

fn failure_cause(message: &str) -> FailureCause {
    if message.contains("attempted to execute too many times")
        || message.contains("max_worker_loss_retries")
    {
        FailureCause::RetriesExhausted
    } else if message.contains("no more clients listening") {
        FailureCause::Abandoned
    } else {
        FailureCause::Other
    }
}

/// Check every property over `records`.
pub fn check(config: &SimConfig, records: &[Record]) -> (Vec<Violation>, Stats) {
    let mut violations = Vec::new();
    let mut stats = Stats::default();
    let window = config.timeouts.interest_window();
    let strict_window = window.saturating_sub(config.interest_slack);

    // Worker-side facts, in history order.
    // op -> (seq, attempt, worker host, session) of every StartExecute.
    let mut started: BTreeMap<String, Vec<(usize, u64, String, u32)>> = BTreeMap::new();
    let mut sent_ok: BTreeMap<(String, u64), (usize, bool)> = BTreeMap::new();
    // Client-side facts.
    let mut delivered: BTreeMap<String, Vec<(usize, ResultId)>> = BTreeMap::new();
    let mut done_by_job: BTreeMap<String, (usize, Outcome)> = BTreeMap::new();
    let mut last_stage_by_job: BTreeMap<String, String> = BTreeMap::new();
    let mut jobs: BTreeSet<String> = BTreeSet::new();
    let mut detach_longest: BTreeMap<String, Duration> = BTreeMap::new();
    let mut panic_sites: BTreeMap<String, usize> = BTreeMap::new();

    for record in records {
        match &record.event {
            Event::Fault(_) => stats.faults += 1,
            Event::SchedulerBoot { .. } => stats.scheduler_boots += 1,
            Event::SchedulerError(err) => violations.push(Violation {
                kind: "scheduler-error".to_string(),
                detail: err.clone(),
                trace: vec![record.seq],
            }),
            Event::Panic(what) => {
                // One violation per panic site, however often it fired.
                let site = what.split(": ").next().unwrap_or_default().to_string();
                if let Some(seen) = panic_sites.get_mut(&site) {
                    *seen += 1;
                    continue;
                }
                panic_sites.insert(site.clone(), 1);
                violations.push(Violation {
                    kind: if what.starts_with("harness:") {
                        "harness-error".to_string()
                    } else {
                        format!("panic@{}", site.rsplit('/').next().unwrap_or_default())
                    },
                    detail: what.clone(),
                    trace: vec![record.seq],
                });
            }
            Event::Submit { job, .. } => {
                jobs.insert(job.clone());
            }
            Event::Detach { job, for_ms } => {
                let entry = detach_longest.entry(job.clone()).or_default();
                *entry = (*entry).max(Duration::from_millis(*for_ms));
            }
            Event::Reattach {
                job,
                after: Some(after),
                ..
            } => {
                let entry = detach_longest.entry(job.clone()).or_default();
                *entry = (*entry).max(*after);
            }
            Event::Stage { job, stage, .. } => {
                if let Some(previous) = last_stage_by_job.get(job)
                    && previous == "EXECUTING"
                    && stage == "QUEUED"
                {
                    stats.executing_to_queued += 1;
                }
                last_stage_by_job.insert(job.clone(), stage.clone());
            }
            Event::StartExecute {
                session,
                operation,
                attempt,
                ..
            } => {
                stats.worker_starts += 1;
                let op_starts = started.entry(operation.clone()).or_default();
                // An attempt id names one execution: the scheduler must not
                // hand the same (op, attempt) to two sessions.
                if let Some(first) = op_starts.iter().find(|(_, a, host, s)| {
                    *a == *attempt && *attempt != 0 && (*host != record.host || *s != *session)
                }) {
                    violations.push(Violation {
                        kind: "attempt-reused".to_string(),
                        detail: format!(
                            "op={operation} attempt={attempt} given to {}#{} and then to {}#{session}",
                            first.2, first.3, record.host
                        ),
                        trace: vec![first.0, record.seq],
                    });
                }
                op_starts.push((record.seq, *attempt, record.host.clone(), *session));
            }
            Event::Kill { .. } => stats.kills += 1,
            Event::OverCapacity {
                operation,
                attempt,
                running,
                slots,
                live_here,
                unacked_kills,
                ..
            } => {
                // Only attempts the scheduler handed this session count
                // against it: one a zombie carried over from a dead session
                // is invisible to the scheduler (no session resume today).
                let kind = if live_here >= slots {
                    "capacity-overcommit"
                } else if live_here + unacked_kills >= *slots {
                    // The slot was freed on revoke, before the worker acked
                    // the kill: the fenced design's revoke barrier.
                    "capacity-overcommit-unacked-kill"
                } else {
                    stats.over_capacity_carried += 1;
                    continue;
                };
                violations.push(Violation {
                    kind: kind.to_string(),
                    detail: format!(
                        "{} offered op={operation} attempt={attempt} while running {running} of {slots} slots ({live_here} live on this session, {unacked_kills} killed but unacknowledged)",
                        record.host
                    ),
                    trace: vec![record.seq],
                });
            }
            Event::ResultSent {
                operation,
                attempt,
                killed_before,
                ok: true,
                ..
            } => {
                sent_ok.insert((operation.clone(), *attempt), (record.seq, *killed_before));
            }
            Event::Done { job, outcome, .. } => {
                if let Some((seq, previous)) = done_by_job.get(job)
                    && previous != outcome
                {
                    violations.push(Violation {
                        kind: "inconsistent-result".to_string(),
                        detail: format!("{job}: {previous} then {outcome}"),
                        trace: vec![*seq, record.seq],
                    });
                }
                done_by_job.insert(job.clone(), (record.seq, outcome.clone()));
                match outcome {
                    Outcome::Ok(id) => {
                        stats.done_ok += 1;
                        delivered
                            .entry(id.operation.clone())
                            .or_default()
                            .push((record.seq, id.clone()));
                    }
                    Outcome::OkUnattributed(what) => {
                        stats.done_ok += 1;
                        violations.push(Violation {
                            kind: "unattributed-result".to_string(),
                            detail: format!("{job}: {what}"),
                            trace: vec![record.seq],
                        });
                    }
                    Outcome::Failed { message, .. } => {
                        stats.done_failed += 1;
                        let away = detach_longest.get(job).copied().unwrap_or_default();
                        let client_left = away
                            >= Duration::from_secs(config.timeouts.client_action_timeout_s)
                                .saturating_sub(config.interest_slack);
                        let expected = match failure_cause(message) {
                            FailureCause::RetriesExhausted => true,
                            FailureCause::Abandoned => client_left,
                            FailureCause::Other => false,
                        };
                        if !expected {
                            violations.push(Violation {
                                kind: "unexpected-failure".to_string(),
                                detail: format!(
                                    "{job}: {outcome} (longest detach {}ms)",
                                    away.as_millis()
                                ),
                                trace: vec![record.seq],
                            });
                        }
                    }
                }
            }
            Event::NotFound {
                job,
                name,
                detached_for,
            } => {
                stats.not_found += 1;
                let away = detached_for.unwrap_or_default();
                if away < strict_window {
                    violations.push(Violation {
                        kind: "lost-operation".to_string(),
                        detail: format!(
                            "{job} {name}: NOT_FOUND after {}ms detached (interest window {}s)",
                            away.as_millis(),
                            window.as_secs()
                        ),
                        trace: vec![record.seq],
                    });
                } else {
                    stats.not_found_after_window += 1;
                }
            }
            Event::Confirm { job, name, seen } => {
                let Some((done_seq, outcome)) = done_by_job.get(job) else {
                    continue;
                };
                if seen == "not-found" {
                    violations.push(Violation {
                        kind: "lost-operation".to_string(),
                        detail: format!("{job} {name}: finished, then NOT_FOUND within retention"),
                        trace: vec![*done_seq, record.seq],
                    });
                } else if seen.starts_with("stage") {
                    violations.push(Violation {
                        kind: "done-regression".to_string(),
                        detail: format!("{job} {name}: finished with {outcome}, re-read shows {seen}"),
                        trace: vec![*done_seq, record.seq],
                    });
                } else if seen.starts_with("done") && *seen != format!("done {outcome}") {
                    violations.push(Violation {
                        kind: "inconsistent-result".to_string(),
                        detail: format!("{job} {name}: finished with {outcome}, re-read shows {seen}"),
                        trace: vec![*done_seq, record.seq],
                    });
                }
            }
            Event::GaveUp { job, last_stage } => {
                stats.gave_up += 1;
                violations.push(Violation {
                    kind: "liveness-stuck".to_string(),
                    detail: format!("{job}: no final answer by the end of the run (last stage {last_stage})"),
                    trace: vec![record.seq],
                });
            }
            _ => {}
        }
    }
    stats.jobs = jobs.len();

    for (operation, deliveries) in &delivered {
        let distinct: BTreeSet<&ResultId> = deliveries.iter().map(|(_, id)| id).collect();
        if distinct.len() > 1 {
            violations.push(Violation {
                kind: "inconsistent-result".to_string(),
                detail: format!(
                    "op={operation}: {} different results delivered: {}",
                    distinct.len(),
                    distinct
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(" / ")
                ),
                trace: deliveries.iter().map(|(seq, _)| *seq).collect(),
            });
        }
        for (done_seq, id) in deliveries {
            // Stale: after the delivered attempt started, and before the
            // client got its result, a later attempt of the operation reached
            // a worker. (An earlier attempt's `StartExecute` arriving late, say
            // held in a partition or read by a worker after a stall, is not:
            // attempt numbers only grow, which `attempt-reused` checks.)
            let op_starts = started.get(operation).map(Vec::as_slice).unwrap_or_default();
            let own_start = op_starts
                .iter()
                .filter(|(seq, attempt, host, session)| {
                    *seq < *done_seq
                        && *attempt == id.attempt
                        && *host == id.worker
                        && *session == id.session
                })
                .map(|(seq, ..)| *seq)
                .max();
            if let Some(own_start) = own_start
                && let Some(later) = op_starts
                    .iter()
                    .find(|(seq, attempt, ..)| *seq > own_start && *seq < *done_seq && *attempt > id.attempt)
            {
                violations.push(Violation {
                    kind: "stale-attempt-result".to_string(),
                    detail: format!(
                        "{id} delivered after attempt {} had been started on {}#{}",
                        later.1, later.2, later.3
                    ),
                    trace: vec![own_start, later.0, *done_seq],
                });
            }
            match sent_ok.get(&(operation.clone(), id.attempt)) {
                Some((sent_seq, true)) => violations.push(Violation {
                    kind: "revoked-attempt-result".to_string(),
                    detail: format!("{id} delivered though the worker was told to kill it first"),
                    trace: vec![*sent_seq, *done_seq],
                }),
                Some((_, false)) => {}
                None => violations.push(Violation {
                    kind: "unattributed-result".to_string(),
                    detail: format!("{id} delivered but no worker sent it"),
                    trace: vec![*done_seq],
                }),
            }
        }
    }
    (violations, stats)
}

/// The records worth showing for `violation`: its own, every record naming
/// the same job, client operation or scheduler operation, and the faults,
/// capped at `limit`.
pub fn minimal_trace<'a>(violation: &Violation, records: &'a [Record], limit: usize) -> Vec<&'a Record> {
    let mut keys: BTreeSet<String> = BTreeSet::new();
    // (host, "#session ") pairs: every event of that worker session.
    let mut sessions: BTreeSet<(String, String)> = BTreeSet::new();
    for seq in &violation.trace {
        if let Some(record) = records.get(*seq) {
            if let Event::OverCapacity { session, .. } = &record.event {
                sessions.insert((record.host.clone(), format!("#{session} ")));
            }
            match &record.event {
                Event::Done { job, name, outcome } => {
                    keys.insert(format!("{job} "));
                    keys.insert(format!("name={name}"));
                    if let Outcome::Ok(id) = outcome {
                        keys.insert(format!("op={}", id.operation));
                    }
                }
                Event::Confirm { job, name, .. }
                | Event::NotFound { job, name, .. }
                | Event::Stage { job, name, .. } => {
                    keys.insert(format!("{job} "));
                    keys.insert(format!("name={name}"));
                }
                Event::GaveUp { job, .. } => {
                    keys.insert(format!("{job} "));
                }
                Event::StartExecute { operation, .. }
                | Event::ResultSent { operation, .. }
                | Event::OverCapacity { operation, .. }
                | Event::Kill { operation, .. } => {
                    keys.insert(format!("op={operation}"));
                }
                _ => {}
            }
        }
    }
    // A job's name also brings in the operations its results named, and
    // every operation started for its action.
    let mut digests = BTreeSet::new();
    for record in records {
        match &record.event {
            Event::Submit { job, digest } if keys.contains(&format!("{job} ")) => {
                digests.insert(format!("digest={digest} "));
            }
            _ => {}
        }
    }
    let mut extra = BTreeSet::new();
    for record in records {
        match &record.event {
            Event::Done {
                job,
                outcome: Outcome::Ok(id),
                ..
            } if keys.contains(&format!("{job} ")) => {
                extra.insert(format!("op={} ", id.operation));
            }
            Event::StartExecute {
                operation, action, ..
            } if digests.contains(&format!("digest={action} ")) => {
                extra.insert(format!("op={operation} "));
            }
            _ => {}
        }
    }
    keys.extend(extra);
    let mut out: Vec<&Record> = records
        .iter()
        .filter(|record| {
            violation.trace.contains(&record.seq)
                || matches!(record.event, Event::Fault(_) | Event::SchedulerBoot { .. })
                || {
                    let line = format!("{} ", record.event);
                    keys.iter().any(|key| line.contains(key.as_str()))
                        || sessions
                            .iter()
                            .any(|(host, tag)| *host == record.host && line.contains(tag.as_str()))
                }
        })
        .collect();
    if out.len() > limit {
        // Keep the records nearest the violation.
        let anchor = violation.trace.iter().copied().max().unwrap_or(0);
        out.sort_by_key(|record| record.seq.abs_diff(anchor));
        out.truncate(limit);
        out.sort_by_key(|record| record.seq);
    }
    out
}
