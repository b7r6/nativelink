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

//! Bounded synthetic measurement of the experimental durable singleton.
//!
//! Usage: `authority_budget OUTPUT_DIRECTORY CYCLES WIDTH PAYLOAD_BYTES PROFILE_LABEL`
//! WIDTH is simultaneously admitted work, not a proposal/log batching unit.
//! Events traverse real `OpenRaft` 0.9.25/redb 4.4.0. An optional final mode
//! selects serial execution, serial causal rounds, or concurrent causal rounds.
//! These are measurement workloads, not runtime scheduler policy selectors.
//! No external effects run. This is not a shared publication/lease/GC budget or
//! a production supported-load claim. Output includes every measured latency.

use std::collections::BTreeMap;
use std::env;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures::future::join_all;
use nativelink_scheduler::authority::journal::{
    Batch, MAX_BATCH_BYTES, MAX_BATCH_EVENTS, Singleton,
};
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, EffectKind, Event, IncarnationId, Limits, OperationId, Phase,
    RecordedEvent, Reply, Request, Resources, ResultPayload, SessionId, State, TERMINAL_RETENTION,
    WorkerId,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use openraft::storage::RaftStateMachine;
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug)]
struct Arguments {
    output: PathBuf,
    cycles: u64,
    width: usize,
    payload_bytes: usize,
    profile_label: String,
    mode: String,
}

#[derive(Debug, Serialize)]
struct WriteSample {
    command: &'static str,
    elapsed_ns: u128,
    recorded_event_bytes: usize,
    log_index: u64,
    event_ordinal: usize,
}

#[derive(Debug, Serialize)]
struct ProposalRound {
    commands: usize,
    concurrent: bool,
    elapsed_ns: u128,
    enqueue_ns: Option<u128>,
    max_unawaited_response_handles: usize,
    first_log_index: u64,
    last_log_index: u64,
}

#[derive(Debug, Default, Serialize)]
struct Samples {
    proposal_rounds: Vec<ProposalRound>,
    writes: Vec<WriteSample>,
    events: Vec<RecordedEvent>,
    proposal_encoded_bytes: Vec<usize>,
    pure_replay_apply_ns: Vec<u128>,
    pure_replay_snapshot_ns: Vec<u128>,
    pure_replay_restore_ns: Vec<u128>,
    raw_components: Vec<Value>,
    linearizable_read_ns: Vec<u128>,
    read_barrier_only_ns: Vec<u128>,
    snapshot_ns: Vec<u128>,
    core_snapshot_ns: Vec<u128>,
    core_snapshot_bytes: Vec<usize>,
    persisted_snapshot_bytes: Vec<usize>,
}

const fn command_name(event: &Event) -> &'static str {
    match event {
        Event::Register { .. } => "register",
        Event::Submit { .. } => "submit",
        Event::LookupResult { .. } => "lookup_miss",
        Event::Match => "match",
        Event::WorkerAccepted { .. } => "worker_accepted",
        Event::Progress {
            phase: Phase::Preparing,
            ..
        } => "preparing",
        Event::Progress {
            phase: Phase::Running,
            ..
        } => "running",
        Event::Progress {
            phase: Phase::Uploading,
            ..
        } => "uploading",
        Event::Complete { .. } => "complete",
        Event::EffectOutcome { .. } => "publication_handoff_outcome",
        Event::AdvanceTime { .. } => "advance_logical_time",
        Event::RetireExecution { .. } => "retire_terminal_execution",
        _ => "unexpected",
    }
}

impl Samples {
    async fn write(&mut self, singleton: &Singleton, event: Event) -> Result<Reply, String> {
        let command = command_name(&event);
        let recorded = RecordedEvent {
            epoch: ClusterEpoch(1),
            event,
        };
        let recorded_event_bytes = serde_json::to_vec(&recorded)
            .map_err(|e| e.to_string())?
            .len();
        self.events.push(recorded.clone());
        self.proposal_encoded_bytes
            .push(Batch::new([recorded.clone()])?.encoded_bytes()?);
        let start = Instant::now();
        let response = singleton.write(recorded).await?;
        let elapsed_ns = start.elapsed().as_nanos();
        self.writes.push(WriteSample {
            command,
            elapsed_ns,
            recorded_event_bytes,
            log_index: response.log_id.index,
            event_ordinal: 0,
        });
        if matches!(response.data, Reply::Rejected(_) | Reply::Ignored) {
            return Err(format!(
                "unexpected {command} reply: {}",
                serde_json::to_string(&response.data).map_err(|e| e.to_string())?
            ));
        }
        Ok(response.data)
    }

    async fn batch(
        &mut self,
        singleton: &Singleton,
        events: Vec<Event>,
        mode: &str,
    ) -> Result<(), String> {
        let count = events.len();
        let offset = self.writes.len();
        let round_start = Instant::now();
        let concurrent = mode == "concurrent";
        let enqueue_ns = if mode == "application-batch" {
            let mut groups = Vec::new();
            let mut current = Vec::new();
            // This synthetic caller forms only already available causal-round
            // events. It does not claim a production ingress aggregation actor.
            let mut group_event_bytes = 0;
            for event in events {
                let event = RecordedEvent {
                    epoch: ClusterEpoch(1),
                    event,
                };
                let bytes = serde_json::to_vec(&event)
                    .map_err(|error| error.to_string())?
                    .len();
                // Reserve more than the exact version/vector framing here;
                // Batch::new revalidates the exact serialized representation.
                if !current.is_empty()
                    && (current.len() == MAX_BATCH_EVENTS
                        || group_event_bytes + bytes + 128 > MAX_BATCH_BYTES)
                {
                    groups.push(Batch::new(std::mem::take(&mut current))?);
                    group_event_bytes = 0;
                }
                group_event_bytes += bytes + 1;
                current.push(event);
            }
            if !current.is_empty() {
                groups.push(Batch::new(current)?);
            }
            for batch in groups {
                self.proposal_encoded_bytes.push(batch.encoded_bytes()?);
                let recorded = batch.events().to_vec();
                let started = Instant::now();
                let response = singleton.write_batch(batch).await?;
                let elapsed_ns = started.elapsed().as_nanos();
                for (event_ordinal, (event, reply)) in
                    recorded.into_iter().zip(response.data.replies).enumerate()
                {
                    if matches!(reply, Reply::Rejected(_) | Reply::Ignored) {
                        return Err(format!("application batch event rejected: {reply:?}"));
                    }
                    self.writes.push(WriteSample {
                        command: command_name(&event.event),
                        elapsed_ns,
                        recorded_event_bytes: serde_json::to_vec(&event)
                            .map_err(|error| error.to_string())?
                            .len(),
                        log_index: response.log_id.index,
                        event_ordinal,
                    });
                    self.events.push(event);
                }
            }
            None
        } else if concurrent {
            let mut pending = Vec::with_capacity(count);
            for event in events {
                let recorded = RecordedEvent {
                    epoch: ClusterEpoch(1),
                    event,
                };
                let bytes = serde_json::to_vec(&recorded)
                    .map_err(|e| e.to_string())?
                    .len();
                let start = Instant::now();
                let batch = Batch::new([recorded.clone()])?;
                self.proposal_encoded_bytes.push(batch.encoded_bytes()?);
                // Ordered ingress does not await commit/apply: all response
                // handles remain outstanding when the next proposal is sent.
                // This preserves the internal OperationId ingress sequence.
                let receiver = singleton
                    .raft
                    .client_write_ff(batch)
                    .await
                    .map_err(|e| e.to_string())?;
                pending.push((recorded, bytes, start, receiver));
            }
            let enqueue_ns = round_start.elapsed().as_nanos();
            let completed = join_all(pending.into_iter().map(
                |(recorded, bytes, start, receiver)| async move {
                    let response = receiver
                        .await
                        .map_err(|e| e.to_string())?
                        .map_err(|e| e.to_string())?;
                    let elapsed_ns = start.elapsed().as_nanos();
                    if response.data.replies.len() != 1 {
                        return Err("concurrent singleton reply count mismatch".to_owned());
                    }
                    if matches!(
                        response.data.replies[0],
                        Reply::Rejected(_) | Reply::Ignored
                    ) {
                        return Err(format!(
                            "concurrent proposal rejected: {}",
                            serde_json::to_string(&response.data).map_err(|e| e.to_string())?
                        ));
                    }
                    Ok((response.log_id.index, recorded, bytes, elapsed_ns))
                },
            ))
            .await;
            let mut completed: Vec<_> = completed.into_iter().collect::<Result<_, String>>()?;
            // Pure replay and the raw event log follow actual committed order,
            // never assumption about future wakeup or response completion order.
            completed.sort_unstable_by_key(|entry| entry.0);
            for (log_index, recorded, recorded_event_bytes, elapsed_ns) in completed {
                let command = command_name(&recorded.event);
                self.events.push(recorded);
                self.writes.push(WriteSample {
                    command,
                    elapsed_ns,
                    recorded_event_bytes,
                    log_index,
                    event_ordinal: 0,
                });
            }
            Some(enqueue_ns)
        } else {
            for event in events {
                self.write(singleton, event).await?;
            }
            None
        };
        self.proposal_rounds.push(ProposalRound {
            commands: count,
            concurrent,
            elapsed_ns: round_start.elapsed().as_nanos(),
            enqueue_ns,
            max_unawaited_response_handles: if concurrent { count } else { 1 },
            first_log_index: self.writes[offset].log_index,
            last_log_index: self.writes.last().ok_or("empty proposal round")?.log_index,
        });
        Ok(())
    }

    async fn read(&mut self, singleton: &Singleton) -> Result<State, String> {
        let start = Instant::now();
        singleton
            .raft
            .ensure_linearizable()
            .await
            .map_err(|e| e.to_string())?;
        self.read_barrier_only_ns.push(start.elapsed().as_nanos());
        let (_, state) = singleton
            .journal
            .read_applied()
            .await
            .map_err(|e| e.to_string())?;
        self.linearizable_read_ns.push(start.elapsed().as_nanos());
        Ok(state)
    }

    async fn snapshot(&mut self, singleton: &Singleton) -> Result<(), String> {
        let (last_applied, state) = singleton
            .journal
            .read_applied()
            .await
            .map_err(|e| e.to_string())?;
        let last_applied = last_applied.ok_or("no applied state to snapshot")?;
        let start = Instant::now();
        let bytes = state.snapshot()?;
        self.core_snapshot_ns.push(start.elapsed().as_nanos());
        self.core_snapshot_bytes.push(bytes.len());
        let start = Instant::now();
        singleton
            .raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| e.to_string())?;
        singleton
            .raft
            .wait(Some(Duration::from_secs(30)))
            .snapshot(last_applied, "benchmark snapshot persisted")
            .await
            .map_err(|e| e.to_string())?;
        self.snapshot_ns.push(start.elapsed().as_nanos());
        let mut journal = singleton.journal.clone();
        let saved = journal
            .get_current_snapshot()
            .await
            .map_err(|e| e.to_string())?
            .ok_or("snapshot not found after completion")?;
        self.persisted_snapshot_bytes
            .push(saved.snapshot.get_ref().len());
        Ok(())
    }
}

fn latency(values: &[u128]) -> Value {
    if values.is_empty() {
        return json!({ "count": 0 });
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let percentile =
        |percent: usize| sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)];
    json!({ "count": sorted.len(), "min_ns": sorted[0], "p50_ns": percentile(50),
        "p95_ns": percentile(95), "p99_ns": percentile(99), "max_ns": sorted[sorted.len() - 1] })
}

fn measurement_limits(args: &Arguments) -> Result<Limits, String> {
    Ok(Limits {
        max_executions: args.width,
        max_operations: args.width,
        max_workers: 1,
        max_effects: args.width.checked_mul(8).ok_or("width overflow")?,
        max_payload_bytes: args
            .payload_bytes
            .checked_add(4096)
            .ok_or("payload overflow")?,
    })
}

async fn causal_rounds(
    singleton: &Singleton,
    samples: &mut Samples,
    args: &Arguments,
    properties: &BTreeMap<String, String>,
) -> Result<u64, String> {
    let mut operation = 0_u64;
    for cycle in 0..args.cycles {
        let mut submissions = Vec::with_capacity(args.width);
        for _ in 0..args.width {
            operation += 1;
            submissions.push(Event::Submit {
                operation: OperationId(operation),
                request: Request {
                    action: ActionKey {
                        instance: "synthetic-budget".into(),
                        digest_function: "sha256".into(),
                        digest: format!("{operation:064x}"),
                        digest_size_bytes: 128,
                    },
                    execution_scope: None,
                    do_not_cache: false,
                    skip_cache_lookup: false,
                    priority: 0,
                    properties: properties.clone(),
                    resources: Resources {
                        cpu_millis: 1000,
                        memory_bytes: 1024,
                        slots: 1,
                    },
                    execution_timeout: 60,
                    payload: None,
                },
            });
        }
        samples.batch(singleton, submissions, &args.mode).await?;
        let pending = samples.read(singleton).await?.view();
        let lookups = pending
            .effects
            .into_iter()
            .map(|effect| Event::LookupResult {
                effect: effect.id,
                result: None,
            })
            .collect();
        samples.batch(singleton, lookups, &args.mode).await?;
        samples
            .batch(singleton, vec![Event::Match; args.width], &args.mode)
            .await?;
        samples.snapshot(singleton).await?;
        let assigned = samples.read(singleton).await?.view();
        let acknowledgments = assigned
            .attempts
            .iter()
            .map(|attempt| Event::WorkerAccepted {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
            })
            .collect();
        samples
            .batch(singleton, acknowledgments, &args.mode)
            .await?;
        for (progress, phase) in [
            (1, Phase::Preparing),
            (2, Phase::Running),
            (3, Phase::Uploading),
        ] {
            let events = assigned
                .attempts
                .iter()
                .map(|attempt| Event::Progress {
                    attempt: attempt.id,
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    phase,
                    progress,
                })
                .collect();
            samples.batch(singleton, events, &args.mode).await?;
        }
        let completions = assigned
            .attempts
            .iter()
            .map(|attempt| Event::Complete {
                attempt: attempt.id,
                worker: attempt.worker,
                incarnation: attempt.incarnation,
                result: ResultPayload {
                    response: None,
                    digest: format!("{:064x}", attempt.id.0),
                    exit_code: 0,
                    error: None,
                },
            })
            .collect();
        samples.batch(singleton, completions, &args.mode).await?;
        let completed = samples.read(singleton).await?.view();
        let outcomes = completed
            .effects
            .into_iter()
            .map(|effect| Event::EffectOutcome {
                effect: effect.id,
                success: true,
            })
            .collect();
        samples.batch(singleton, outcomes, &args.mode).await?;
        samples
            .write(
                singleton,
                Event::AdvanceTime {
                    now: (cycle + 1) * TERMINAL_RETENTION,
                },
            )
            .await?;
        let retirements = completed
            .executions
            .into_iter()
            .map(|execution| Event::RetireExecution {
                execution: execution.id,
            })
            .collect();
        samples.batch(singleton, retirements, &args.mode).await?;
    }
    Ok(operation)
}

async fn measure(args: &Arguments) -> Result<(Value, Samples), String> {
    let width_u64 = u64::try_from(args.width).map_err(|e| e.to_string())?;
    let limits = measurement_limits(args)?;
    let path = args.output.join("authority.redb");
    let open_start = Instant::now();
    let singleton = Singleton::open(&path, 1, ClusterEpoch(1), limits).await?;
    let bootstrap_log_index = singleton.raft.metrics().borrow().last_log_index;
    singleton
        .raft
        .wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(bootstrap_log_index, "bootstrap applied before measurement")
        .await
        .map_err(|error| error.to_string())?;
    singleton.read().await?;
    singleton.journal.enable_measurements();
    let bootstrap_ns = open_start.elapsed().as_nanos();
    let initial_metrics = singleton.raft.metrics().borrow().clone();
    let mut samples = Samples::default();
    let properties = BTreeMap::from([
        ("os".to_owned(), "linux".to_owned()),
        (
            "benchmark_payload".to_owned(),
            "x".repeat(args.payload_bytes),
        ),
    ]);
    let capacity = Resources {
        cpu_millis: width_u64 * 1000,
        memory_bytes: width_u64 * 1024,
        slots: width_u64,
    };
    let workload_start = Instant::now();
    samples
        .write(
            &singleton,
            Event::Register {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: properties.clone(),
                capacity,
                available: capacity,
            },
        )
        .await?;
    let operation = if args.mode == "serial" {
        let mut operation = 0_u64;
        for cycle in 0..args.cycles {
            for _ in 0..args.width {
                operation += 1;
                samples
                    .write(
                        &singleton,
                        Event::Submit {
                            operation: OperationId(operation),
                            request: Request {
                                action: ActionKey {
                                    instance: "synthetic-budget".into(),
                                    digest_function: "sha256".into(),
                                    digest: format!("{operation:064x}"),
                                    digest_size_bytes: 128,
                                },
                                execution_scope: None,
                                do_not_cache: false,
                                skip_cache_lookup: false,
                                priority: 0,
                                properties: properties.clone(),
                                resources: Resources {
                                    cpu_millis: 1000,
                                    memory_bytes: 1024,
                                    slots: 1,
                                },
                                execution_timeout: 60,
                                payload: None,
                            },
                        },
                    )
                    .await?;
            }
            let pending = samples.read(&singleton).await?.view();
            for effect in pending.effects {
                if !matches!(effect.kind, EffectKind::Lookup { .. }) {
                    return Err("unexpected non-lookup intent".into());
                }
                samples
                    .write(
                        &singleton,
                        Event::LookupResult {
                            effect: effect.id,
                            result: None,
                        },
                    )
                    .await?;
            }
            for _ in 0..args.width {
                samples.write(&singleton, Event::Match).await?;
            }
            // Persist a real Raft snapshot while reservations and uncertain dispatch
            // intents exist. Snapshotting is included in workload wall-clock time.
            samples.snapshot(&singleton).await?;
            let assigned = samples.read(&singleton).await?.view();
            for attempt in assigned.attempts {
                samples
                    .write(
                        &singleton,
                        Event::WorkerAccepted {
                            attempt: attempt.id,
                            worker: attempt.worker,
                            incarnation: attempt.incarnation,
                        },
                    )
                    .await?;
                for (progress, phase) in [
                    (1, Phase::Preparing),
                    (2, Phase::Running),
                    (3, Phase::Uploading),
                ] {
                    samples
                        .write(
                            &singleton,
                            Event::Progress {
                                attempt: attempt.id,
                                worker: attempt.worker,
                                incarnation: attempt.incarnation,
                                phase,
                                progress,
                            },
                        )
                        .await?;
                }
                samples
                    .write(
                        &singleton,
                        Event::Complete {
                            attempt: attempt.id,
                            worker: attempt.worker,
                            incarnation: attempt.incarnation,
                            result: ResultPayload {
                                response: None,
                                digest: format!("{:064x}", attempt.id.0),
                                exit_code: 0,
                                error: None,
                            },
                        },
                    )
                    .await?;
            }
            let completed = samples.read(&singleton).await?.view();
            for effect in completed.effects {
                if !matches!(effect.kind, EffectKind::PublishResult { .. }) {
                    return Err("unexpected completion intent".into());
                }
                // This is only a recorded successful handoff observation. No actual
                // CAS, ActionCache, retention, graph traversal, or provider ran.
                samples
                    .write(
                        &singleton,
                        Event::EffectOutcome {
                            effect: effect.id,
                            success: true,
                        },
                    )
                    .await?;
            }
            samples
                .write(
                    &singleton,
                    Event::AdvanceTime {
                        now: (cycle + 1) * TERMINAL_RETENTION,
                    },
                )
                .await?;
            for execution in completed.executions {
                samples
                    .write(
                        &singleton,
                        Event::RetireExecution {
                            execution: execution.id,
                        },
                    )
                    .await?;
            }
        }
        operation
    } else {
        causal_rounds(&singleton, &mut samples, args, &properties).await?
    };
    let workload_ns = workload_start.elapsed().as_nanos();
    let final_state = samples.read(&singleton).await?;
    if !final_state.view().executions.is_empty() || !final_state.view().effects.is_empty() {
        return Err("bounded workload did not drain all execution/effect records".into());
    }
    let final_bytes = final_state.snapshot()?;
    let final_metrics = singleton.raft.metrics().borrow().clone();
    let journal_measurements = singleton.journal.measurements();
    let shutdown_start = Instant::now();
    singleton.shutdown().await?;
    let shutdown_ns = shutdown_start.elapsed().as_nanos();
    let reopen_start = Instant::now();
    let reopened = Singleton::open(&path, 1, ClusterEpoch(1), limits).await?;
    let recovered = reopened.read().await?;
    let reopen_and_barrier_ns = reopen_start.elapsed().as_nanos();
    if recovered.snapshot()? != final_bytes {
        return Err("restart changed canonical scheduler state".into());
    }
    let reopened_metrics = reopened.raft.metrics().borrow().clone();
    reopened.shutdown().await?;
    let mut by_command: BTreeMap<&str, Vec<u128>> = BTreeMap::new();
    for sample in &samples.writes {
        by_command
            .entry(sample.command)
            .or_default()
            .push(sample.elapsed_ns);
    }
    let command_latencies: BTreeMap<_, _> = by_command
        .into_iter()
        .map(|(name, values)| (name, latency(&values)))
        .collect();
    let all_writes: Vec<_> = samples
        .writes
        .iter()
        .map(|sample| sample.elapsed_ns)
        .collect();
    let event_bytes: usize = samples
        .writes
        .iter()
        .map(|sample| sample.recorded_event_bytes)
        .sum();
    let log_entries =
        final_metrics.last_log_index.unwrap_or(0) - initial_metrics.last_log_index.unwrap_or(0);
    let mut application_batch_sizes = BTreeMap::<u64, usize>::new();
    for sample in &samples.writes {
        let count = application_batch_sizes.entry(sample.log_index).or_default();
        if sample.event_ordinal != *count {
            return Err("recorded event ordinals are not a contiguous committed prefix".into());
        }
        *count += 1;
    }
    if usize::try_from(log_entries).map_err(|error| error.to_string())?
        != samples.proposal_encoded_bytes.len()
        || application_batch_sizes.len() != samples.proposal_encoded_bytes.len()
        || samples
            .writes
            .windows(2)
            .any(|pair| pair[0].log_index > pair[1].log_index)
        || samples.events.len() != samples.writes.len()
    {
        return Err("measured proposal/log/event counts or committed ordering differ".into());
    }
    let summary = json!({
        "scope": "bounded synthetic scheduler journal experiment; not a supported operating point",
        "dependencies": { "openraft": "0.9.25", "redb": "4.4.0", "durability": "redb Immediate, fsync-backed callbacks; no power-loss certification" },
        "profile_label_supplied_by_runner": args.profile_label,
        "debug_assertions": cfg!(debug_assertions),
        "runtime_worker_threads": 2,
        "topology": { "authority_members": 1, "physical_workers": 0, "simulated_worker_advertisements": 1 },
        "workload": { "cycles": args.cycles, "simultaneously_admitted_executions": args.width,
            "padding_bytes_per_request": args.payload_bytes, "completed_executions": operation,
            "proposal_submission": args.mode,
            "proposal_description": if args.mode == "application-batch" {
                "bounded ordered application batches of already available causal-round events; each batch awaits actual durable commit/apply reply"
            } else if args.mode == "concurrent" {
                "ordered client_write_ff ingress; join_all outstanding commit/apply replies; balanced causal rounds"
            } else if args.mode == "serial-rounds" {
                "serial client_write replies within the same balanced causal rounds as concurrent mode"
            } else { "serial client_write; each assigned execution traverses all phases before the next" },
            "log_batching": "bounded application batches only in application-batch mode; actual log entries, event ordinals, proposal bytes and append/apply counts recorded",
            "application_batch_limits": { "events": MAX_BATCH_EVENTS, "serialized_bytes": MAX_BATCH_BYTES },
            "application_batch_sizes_by_log_index": application_batch_sizes,
            "latency_interpretation": "elapsed proposal-to-durable-apply reply; events within one application batch share its latency; no production ingress wait time is modeled",
            "fsync_count": "not instrumented; proposal round count is NOT fsync count",
            "snapshot_schedule": "one persisted snapshot per cycle while all attempts have charged reservations and pending dispatch",
            "lookups": "recorded misses; no cache backend", "publication": "recorded handoff outcome; no external execution" },
        "counts": { "logical_commands": samples.writes.len(), "client_write_proposals": samples.proposal_encoded_bytes.len(),
            "raft_log_entries_during_workload": log_entries, "recorded_event_json_bytes": event_bytes, "proposal_encoded_bytes": samples.proposal_encoded_bytes.iter().sum::<usize>(),
            "linearizable_reads": samples.linearizable_read_ns.len(), "snapshots": samples.snapshot_ns.len(),
            "proposal_rounds": samples.proposal_rounds.len(),
            "max_unawaited_response_handles": samples.proposal_rounds.iter().map(|round| round.max_unawaited_response_handles).max().unwrap_or(1) },
        "timing_ns": { "bootstrap": bootstrap_ns, "workload_including_reads_and_snapshots": workload_ns,
            "shutdown": shutdown_ns, "reopen_elect_and_read_barrier": reopen_and_barrier_ns },
        "observed_rates": { "logical_commands_per_s": samples.writes.len() as f64 / (workload_ns as f64 / 1e9),
            "completed_executions_per_s": operation as f64 / (workload_ns as f64 / 1e9) },
        "latency": { "write_commit_apply_reply": latency(&all_writes), "per_command": command_latencies,
            "linearizable_read_including_persisted_state_decode": latency(&samples.linearizable_read_ns),
            "read_barrier_only_singleton_no_network_quorum": latency(&samples.read_barrier_only_ns),
            "snapshot_build_and_durable_write": latency(&samples.snapshot_ns), "core_snapshot_serialize": latency(&samples.core_snapshot_ns) },
        "state_sizes": { "peak_core_snapshot_bytes": samples.core_snapshot_bytes.iter().max(),
            "peak_persisted_snapshot_bytes": samples.persisted_snapshot_bytes.iter().max(), "drained_core_snapshot_bytes": final_bytes.len() },
        "raft_metrics": { "initial": initial_metrics, "final": final_metrics, "reopened": reopened_metrics },
        "journal_measurements": { "scope": "optional in-memory counters after singleton open through final read; counters are not authority state; successful Immediate commits are not individual device fsync counts", "operations": journal_measurements },
        "unmeasured": ["three-member quorum/replication bytes", "block-device write bytes", "production RPC/workers",
            "direct ActionCache updates", "immutable publications", "root/closure indexes", "serving leases/renewals",
            "retirement/GC and sustained-renewal collection", "provider latency/faults", "controller dynamics",
            "representative production workload distributions", "historical Lane B harness"],
        "qualification": "M0 production representation and persistent-format freeze remain gated. The adapter persists full state each apply batch; synthetic rates do not establish shared-authority capacity."
    });
    Ok((summary, samples))
}

/// Isolated component samples run after the measured Raft workload and shutdown,
/// so serialization/probe overhead is excluded from the reported workload rate.
fn component_probes(args: &Arguments, samples: &mut Samples) -> Result<Value, Box<dyn Error>> {
    const PROBE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("probe");
    let mut state = State::new(ClusterEpoch(1), measurement_limits(args)?);
    for event in &samples.events {
        let start = Instant::now();
        let reply = state.apply(event.clone());
        samples
            .pure_replay_apply_ns
            .push(start.elapsed().as_nanos());
        if matches!(reply, Reply::Rejected(_) | Reply::Ignored) {
            return Err("pure replay rejected measured event".into());
        }
        let start = Instant::now();
        let snapshot = state.snapshot()?;
        samples
            .pure_replay_snapshot_ns
            .push(start.elapsed().as_nanos());
        let start = Instant::now();
        let restored = State::restore(&snapshot)?;
        samples
            .pure_replay_restore_ns
            .push(start.elapsed().as_nanos());
        if restored.view() != state.view() {
            return Err("component replay restore differed".into());
        }
    }
    let peak = samples
        .core_snapshot_bytes
        .iter()
        .copied()
        .max()
        .unwrap_or(1024);
    let sizes = [1024, peak];
    let mut reports = Vec::new();
    for (index, size) in sizes.into_iter().enumerate() {
        let mut data = vec![b'x'; size];
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(args.output.join(format!("sync-probe-{index}.bin")))?;
        let database =
            redb::Database::create(args.output.join(format!("transaction-probe-{index}.redb")))?;
        // Initialize schema before measuring replacement transactions.
        {
            let mut tx = database.begin_write()?;
            tx.set_durability(redb::Durability::Immediate)?;
            tx.open_table(PROBE)?;
            tx.commit()?;
        }
        let mut sync_ns = Vec::new();
        let mut transaction_ns = Vec::new();
        for sample in 0_u8..20 {
            data[0] = sample;
            let start = Instant::now();
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&data)?;
            file.sync_all()?;
            sync_ns.push(start.elapsed().as_nanos());
            let start = Instant::now();
            let mut tx = database.begin_write()?;
            tx.set_durability(redb::Durability::Immediate)?;
            {
                let mut table = tx.open_table(PROBE)?;
                table.insert("state", data.as_slice())?;
            }
            tx.commit()?;
            transaction_ns.push(start.elapsed().as_nanos());
        }
        reports.push(
            json!({ "payload_bytes": size, "file_write_seek_sync_all": latency(&sync_ns),
            "redb_immediate_transaction_replace": latency(&transaction_ns) }),
        );
        samples.raw_components.push(
            json!({ "payload_bytes": size, "file_write_seek_sync_all_ns": sync_ns,
            "redb_immediate_transaction_replace_ns": transaction_ns }),
        );
    }
    Ok(
        json!({ "scope": "separate serial probes after Raft shutdown; raw bytes omit journal envelopes and runtime scheduling",
        "core_apply_same_recorded_workload": latency(&samples.pure_replay_apply_ns),
        "core_snapshot_including_validation": latency(&samples.pure_replay_snapshot_ns),
        "core_restore_including_validation": latency(&samples.pure_replay_restore_ns),
        "filesystem_and_redb": reports }),
    )
}

fn write_new(path: &Path, value: &impl Serialize) -> Result<(), Box<dyn Error>> {
    let output = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer_pretty(output, value)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let arguments: Vec<_> = env::args().collect();
    if arguments.len() != 6 && arguments.len() != 7 {
        return Err(
            "usage: authority_budget OUTPUT_DIRECTORY CYCLES WIDTH PAYLOAD_BYTES PROFILE_LABEL [serial|serial-rounds|concurrent|application-batch]"
                .into(),
        );
    }
    let args = Arguments {
        output: env::current_dir()?.join(&arguments[1]),
        cycles: arguments[2].parse()?,
        width: arguments[3].parse()?,
        payload_bytes: arguments[4].parse()?,
        profile_label: arguments[5].clone(),
        mode: arguments.get(6).cloned().unwrap_or_else(|| "serial".into()),
    };
    if !matches!(
        args.mode.as_str(),
        "serial" | "serial-rounds" | "concurrent" | "application-batch"
    ) {
        return Err("unsupported proposal submission mode".into());
    }
    if args.cycles == 0
        || args.cycles > 10_000
        || args.width == 0
        || args.width > 256
        || args.payload_bytes > 1024 * 1024
    {
        return Err("require cycles 1..10000, width 1..256, payload bytes <=1048576".into());
    }
    fs::create_dir(&args.output)?;
    let start = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let repository_dir = env::var_os("NATIVELINK_AUTHORITY_SOURCE_ROOT")
        .map_or_else(env::current_dir, |path| Ok(PathBuf::from(path)))?;
    let manifest_dir = repository_dir.join("nativelink-scheduler");
    let environment: Value = serde_json::from_slice(&fs::read(
        manifest_dir.join("design/categorical/evidence/environment.json"),
    )?)?;
    let revision = Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    let filesystem = Command::new("findmnt")
        .args([
            "-T",
            args.output.to_str().ok_or("non-UTF8 output path")?,
            "-o",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ])
        .output()?;
    let mut source_sha256 = BTreeMap::new();
    for relative in [
        "src/authority/mod.rs",
        "src/authority/types.rs",
        "src/authority/journal.rs",
        "examples/authority_budget.rs",
        "../Cargo.lock",
    ] {
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        hasher.update(&fs::read(manifest_dir.join(relative))?);
        source_sha256.insert(relative, hasher.finalize_digest().packed_hash().to_string());
    }
    #[expect(
        clippy::disallowed_methods,
        reason = "Standalone measurement owns a fixed runtime; task helpers require an existing runtime"
    )]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    #[expect(
        clippy::disallowed_methods,
        reason = "Standalone entry point runs one measurement future then writes reports synchronously"
    )]
    let (mut summary, mut samples) = runtime.block_on(measure(&args))?;
    drop(runtime);
    summary["component_probes"] = component_probes(&args, &mut samples)?;
    summary["provenance"] = json!({ "unix_time_seconds": start, "command_arguments": arguments,
        "git_head": String::from_utf8(revision.stdout)?.trim(), "environment": environment,
        "source_sha256": source_sha256, "runtime_source_repository": repository_dir,
        "source_hash_interpretation": "runtime source files; runner must freeze matching build inputs during measurement",
        "filesystem": String::from_utf8(filesystem.stdout)?,
        "ambient_load": "shared development host; concurrent builds/tests may run; no performance isolation" });
    summary["state_sizes"]["redb_allocated_file_bytes_after_restart"] =
        fs::metadata(args.output.join("authority.redb"))?
            .len()
            .into();
    summary["state_sizes"]["file_size_interpretation"] =
        "allocated database file size, NOT durable write volume or replication bytes".into();
    write_new(&args.output.join("samples.json"), &samples)?;
    write_new(&args.output.join("summary.json"), &summary)?;
    Ok(())
}
