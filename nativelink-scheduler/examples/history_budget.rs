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

//! Fixed accepted-history cost experiment using the actual durable singleton.
//! Usage: `history_budget NEW_OUTPUT_DIRECTORY PROFILE_LABEL SOURCE_ROOT`
//! Synthetic worker reports and storage acknowledgements execute no provider I/O.
//! No throughput, production capacity, shared publication or GC claim is made.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nativelink_proto::build::bazel::remote::execution::v2::{ActionResult, ExecuteResponse};
use nativelink_scheduler::authority::accepted_history::{
    HistoryAlgorithm, HistoryOutcome, HistoryStatus,
};
use nativelink_scheduler::authority::format::{
    ACCEPTED_HISTORY, AdmissionRegistry, AuthorityDomain, JournalProfile, ReaderCapabilities,
    accepted_history_namespace,
};
use nativelink_scheduler::authority::journal::{Journal, JournalOperationMeasurement, Singleton};
use nativelink_scheduler::authority::reapi::MAX_RESPONSE_BYTES;
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, EffectKind, Event, IncarnationId, Limits, OperationId, Phase,
    RecordedEvent, Rejection, Reply, Request, Resources, ResultPayload, SessionId, State,
    TERMINAL_RETENTION, TerminalResult, WorkerId,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use openraft::storage::RaftStateMachine;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const DOMAIN: AuthorityDomain = AuthorityDomain {
    epoch: ClusterEpoch(1),
    namespace: 1,
};
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("authority_logs");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("authority_meta");

fn digest(algorithm: DigestHasherFunc, bytes: &[u8]) -> String {
    let mut hasher = algorithm.hasher();
    hasher.update(bytes);
    hasher.finalize_digest().packed_hash().to_string()
}

fn host_sample() -> Result<Value> {
    Ok(json!({
        "unix_time_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        "loadavg": fs::read_to_string("/proc/loadavg")?,
        "uptime": fs::read_to_string("/proc/uptime")?,
    }))
}

fn write_new(path: &Path, value: &Value) -> Result<()> {
    serde_json::to_writer_pretty(
        OpenOptions::new().create_new(true).write(true).open(path)?,
        value,
    )?;
    Ok(())
}

async fn open(path: &Path, limits: Limits, incarnation: u64) -> Result<Singleton> {
    let admission = if incarnation == 1 {
        AdmissionRegistry::new(DOMAIN)
    } else {
        // All earlier singleton handles have shut down. Recover the trusted
        // fixture registry from its sole closed member before admitting a new one.
        Journal::recover_admission(
            DOMAIN,
            ReaderCapabilities::AcceptedHistory,
            vec![(1, path.to_owned())],
        )
        .await?
    };
    let node = Singleton::open_with_profile(
        path,
        1,
        limits,
        JournalProfile {
            domain: DOMAIN,
            incarnation,
            reader: ReaderCapabilities::AcceptedHistory,
            admission,
        },
    )
    .await?;
    node.read().await?;
    node.journal.enable_measurements();
    Ok(node)
}

fn request(algorithm: &str, operation: u64) -> Request {
    Request {
        action: ActionKey {
            instance: "synthetic-history-budget".into(),
            digest_function: algorithm.into(),
            digest: format!("{operation:064x}"),
            digest_size_bytes: 128,
        },
        execution_scope: None,
        do_not_cache: false,
        skip_cache_lookup: true,
        priority: 0,
        properties: BTreeMap::new(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            slots: 1,
        },
        execution_timeout: 1000,
        payload: None,
    }
}

fn measurement_delta(
    before: &BTreeMap<&str, JournalOperationMeasurement>,
    after: &BTreeMap<&str, JournalOperationMeasurement>,
) -> Value {
    let rows: BTreeMap<_, _> = after
        .iter()
        .filter_map(|(name, after)| {
            let before = &before[name];
            (after.calls != before.calls).then(|| {
                (
                    *name,
                    json!({
                        "calls": after.calls - before.calls,
                        "completed_calls": after.completed_calls - before.completed_calls,
                        "durable_commits": after.durable_commits - before.durable_commits,
                        "volatile_commits": after.volatile_commits - before.volatile_commits,
                        "batch_items": after.batch_items - before.batch_items,
                        "queue_wait_ns": after.queue_wait_ns - before.queue_wait_ns,
                        "io_elapsed_ns": after.io_elapsed_ns - before.io_elapsed_ns,
                        "commit_elapsed_ns": after.commit_elapsed_ns - before.commit_elapsed_ns,
                    }),
                )
            })
        })
        .collect();
    json!(rows)
}

async fn write(
    node: &Singleton,
    samples: &mut Vec<Value>,
    label: &str,
    event: Event,
) -> Result<Reply> {
    let recorded = RecordedEvent {
        epoch: DOMAIN.epoch,
        event,
    };
    let event_bytes = serde_json::to_vec(&recorded)?.len();
    let before = node.journal.measurements();
    let started = Instant::now();
    let response = node.write(recorded).await?;
    let elapsed = started.elapsed().as_nanos();
    let after = node.journal.measurements();
    samples.push(json!({
        "command": label, "proposal_to_durable_apply_ns": elapsed,
        "recorded_event_json_bytes": event_bytes, "log_index": response.log_id.index,
        "reply": response.data, "journal_operation_delta": measurement_delta(&before, &after),
    }));
    Ok(response.data)
}

async fn activate(node: &Singleton) -> Result<Value> {
    let before = node.journal.measurements();
    let started = Instant::now();
    let outcome = node
        .journal
        .activate_features(&node.raft, BTreeSet::from([ACCEPTED_HISTORY]))
        .await;
    let elapsed = started.elapsed().as_nanos();
    let format = node.journal.active_format().await?;
    Ok(json!({ "elapsed_ns": elapsed, "outcome": outcome,
        "format": format, "journal_operation_delta": measurement_delta(&before, &node.journal.measurements()) }))
}

async fn snapshot(node: &Singleton) -> Result<Value> {
    let state = node.read().await?;
    let started = Instant::now();
    let core = state.snapshot()?;
    let core_ns = started.elapsed().as_nanos();
    let last = node
        .raft
        .metrics()
        .borrow()
        .last_applied
        .ok_or("snapshot requires applied log")?;
    let started = Instant::now();
    node.raft.trigger().snapshot().await?;
    node.raft
        .wait(Some(Duration::from_secs(30)))
        .snapshot(last, "history budget snapshot")
        .await?;
    let persisted_ns = started.elapsed().as_nanos();
    let saved = node
        .journal
        .clone()
        .get_current_snapshot()
        .await?
        .ok_or("missing persisted snapshot")?;
    Ok(
        json!({ "core_snapshot_bytes": core.len(), "core_snapshot_ns": core_ns,
        "core_snapshot_sha256": digest(DigestHasherFunc::Sha256, &core),
        "persisted_snapshot_bytes": saved.snapshot.get_ref().len(),
        "persisted_snapshot_ns": persisted_ns, "last_applied": last }),
    )
}

// Inspect actual persisted envelopes only after this owned singleton shuts down.
// These are retained logical row bytes, not disk write volume or fsync counts.
fn persisted_rows(path: &Path) -> Result<Value> {
    let database = redb::Database::open(path)?;
    let transaction = database.begin_read()?;
    let logs = transaction.open_table(LOGS)?;
    let mut entries = Vec::new();
    for entry in logs.iter()? {
        let (index, bytes) = entry?;
        let envelope: Value = serde_json::from_slice(bytes.value())?;
        let entry: Value = serde_json::from_str(
            envelope["payload"]
                .as_str()
                .ok_or("log envelope lacks payload")?,
        )?;
        entries.push(json!({ "index": index.value(), "envelope_bytes": bytes.value().len(),
            "sha256": digest(DigestHasherFunc::Sha256, bytes.value()),
            "kind": if entry["payload"].get("Normal").is_some() { "normal" }
                else if entry["payload"].get("Membership").is_some() { "membership" } else { "blank" } }));
    }
    let mut metadata = BTreeMap::new();
    for entry in transaction.open_table(META)?.iter()? {
        let (key, bytes) = entry?;
        metadata.insert(
            key.value().to_owned(),
            json!({ "envelope_bytes": bytes.value().len(),
            "sha256": digest(DigestHasherFunc::Sha256, bytes.value()) }),
        );
    }
    Ok(json!({ "log_entries": entries, "metadata": metadata,
        "allocated_file_bytes": fs::metadata(path)?.len(),
        "log_envelope_bytes": entries.iter().map(|row| row["envelope_bytes"].as_u64().unwrap()).sum::<u64>() }))
}

async fn capacity(output: &Path) -> Result<()> {
    let mut report = Vec::new();
    for history in [false, true] {
        let path = output.join(if history {
            "capacity-history.redb"
        } else {
            "capacity-base.redb"
        });
        let node = open(&path, Limits::default(), 1).await?;
        let mut samples = Vec::new();
        let active = if history {
            Some(activate(&node).await?)
        } else {
            None
        };
        assert_eq!(node.read().await?.accepted_history_enabled(), history);
        let expected = if history { 56 } else { 64 };
        for operation in 1..=expected {
            assert!(matches!(
                write(
                    &node,
                    &mut samples,
                    "submit",
                    Event::Submit {
                        operation: OperationId(operation),
                        request: request("sha256", operation),
                    }
                )
                .await?,
                Reply::Submitted { .. }
            ));
        }
        let before = node.read().await?.snapshot()?;
        assert_eq!(
            write(
                &node,
                &mut samples,
                "over_capacity_submit",
                Event::Submit {
                    operation: OperationId(expected + 1),
                    request: request("sha256", expected + 1),
                }
            )
            .await?,
            Reply::Rejected(Rejection::Overloaded)
        );
        assert_eq!(node.read().await?.snapshot()?, before);
        let refused = if history {
            None
        } else {
            let refused = activate(&node).await?;
            assert!(
                refused["outcome"]["Err"]
                    .as_str()
                    .is_some_and(|error| error.contains("refused after committed abort"))
            );
            let format = node.journal.active_format().await?;
            assert_eq!(format.feature_epoch, 0);
            assert!(format.prepared.is_none());
            assert!(!format.decision.unwrap().activated);
            assert_eq!(node.read().await?.snapshot()?, before);
            assert!(matches!(
                write(
                    &node,
                    &mut samples,
                    "observe_after_abort",
                    Event::Observe {
                        operation: OperationId(1),
                    }
                )
                .await?,
                Reply::Observed(_)
            ));
            Some(refused)
        };
        node.shutdown().await?;
        report.push(
            json!({ "history": history, "default_limits": Limits::default(),
            "admitted": expected, "next_submit": "rejected unchanged", "samples": samples,
            "initial_activation": active, "full_base_activation": refused,
            "persisted": persisted_rows(&path)? }),
        );
    }
    write_new(&output.join("capacity.json"), &json!(report))
}

fn verify_terminals(state: &State, expected: &ResultPayload, width: usize) {
    let view = state.view();
    assert_eq!(view.executions.len(), width);
    assert_eq!(view.operations.len(), width);
    for execution in &view.executions {
        assert_eq!(
            execution.terminal.as_ref(),
            Some(&TerminalResult::Completed(expected.clone()))
        );
    }
    for operation in &view.operations {
        assert_eq!(
            operation.terminal.as_ref(),
            Some(&TerminalResult::Completed(expected.clone()))
        );
    }
    assert!(
        view.reservations
            .iter()
            .all(|reservation| reservation.released)
    );
}

fn verify_history(
    state: &State,
    result: &ResultPayload,
    algorithm: DigestHasherFunc,
    count: usize,
) -> Result<Value> {
    let mut recipes = Vec::new();
    if let Some(history) = state.accepted_history() {
        assert_eq!(history.associations.len(), count);
        for association in history.associations.values() {
            assert_eq!(association.status, HistoryStatus::Pending);
            assert_eq!(
                association.recipe.object.namespace,
                accepted_history_namespace(DOMAIN)
            );
            assert_eq!(
                association.recipe.object.algorithm,
                match algorithm {
                    DigestHasherFunc::Sha256 => HistoryAlgorithm::Sha256,
                    DigestHasherFunc::Blake3 => HistoryAlgorithm::Blake3,
                }
            );
            let bytes = state.history_object_bytes(association.effect)?;
            let response = result.response.as_ref().unwrap().encoded();
            assert_eq!(&bytes[association.recipe.prefix.len()..], response);
            assert_eq!(bytes.len() as u64, association.recipe.object.size_bytes);
            let mut hasher = algorithm.hasher();
            hasher.update(&bytes);
            assert_eq!(
                **hasher.finalize_digest().packed_hash(),
                association.recipe.object.hash
            );
            recipes.push(json!({ "effect": association.effect, "object": association.recipe.object,
                "recipe_json_bytes": serde_json::to_vec(&association.recipe)?.len(),
                "association_json_bytes": serde_json::to_vec(association)?.len(),
                "prefix_bytes": association.recipe.prefix.len(), "response_bytes": response.len() }));
        }
    } else {
        assert_eq!(count, 0);
    }
    Ok(
        json!({ "recipes": recipes, "component_json_bytes": state.accepted_history().map(serde_json::to_vec).transpose()?.map_or(0, |bytes| bytes.len()),
        "additional_prepaid_component_bytes_from_pinned_contract": if count == 0 { 0 } else { 1024 + 2048 * count } }),
    )
}

async fn measure(
    output: &Path,
    algorithm: &str,
    width: usize,
    padding: usize,
    history: bool,
) -> Result<Value> {
    let directory = output.join(format!(
        "{algorithm}-{width}-{padding}-{}",
        u8::from(history)
    ));
    fs::create_dir(&directory)?;
    let start_host = host_sample()?;
    let path = directory.join("authority.redb");
    let result = ResultPayload::from_response(
        &ExecuteResponse {
            result: Some(ActionResult {
                stdout_raw: vec![b'x'; padding].into(),
                ..ActionResult::default()
            }),
            ..ExecuteResponse::default()
        },
        MAX_RESPONSE_BYTES,
    )?;
    let captured = result.response.as_ref().unwrap().encoded();
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(directory.join("response.pb"))?
        .write_all(captured)?;
    let limits = Limits {
        max_executions: width,
        max_operations: width,
        max_workers: 1,
        // Both members of a pair receive the same nine-slot allowance.
        // capacity.json separately qualifies the default eight-slot consequence.
        max_effects: width * 9,
        max_payload_bytes: serde_json::to_vec(&result)?.len() + 1024,
    };
    let node = open(&path, limits, 1).await?;
    let activation = if history {
        Some(activate(&node).await?)
    } else {
        None
    };
    assert_eq!(node.read().await?.accepted_history_enabled(), history);
    let initial_index = node.raft.metrics().borrow().last_applied.unwrap().index;
    let mut samples = Vec::new();
    let capacity = Resources {
        cpu_millis: width as u64,
        memory_bytes: width as u64,
        slots: width as u64,
    };
    assert_eq!(
        write(
            &node,
            &mut samples,
            "register",
            Event::Register {
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                session: SessionId(1),
                properties: BTreeMap::new(),
                capacity,
                available: capacity,
            }
        )
        .await?,
        Reply::Applied
    );
    for operation in 1..=width as u64 {
        assert!(matches!(
            write(
                &node,
                &mut samples,
                "submit",
                Event::Submit {
                    operation: OperationId(operation),
                    request: request(algorithm, operation),
                }
            )
            .await?,
            Reply::Submitted { .. }
        ));
    }
    let admitted = snapshot(&node).await?;
    for _ in 0..width {
        assert!(matches!(
            write(&node, &mut samples, "match", Event::Match).await?,
            Reply::Matched { .. }
        ));
    }
    for attempt in node.read().await?.view().attempts {
        assert_eq!(
            write(
                &node,
                &mut samples,
                "worker_accepted",
                Event::WorkerAccepted {
                    attempt: attempt.id,
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                }
            )
            .await?,
            Reply::Applied
        );
        for (progress, phase) in [
            (1, Phase::Preparing),
            (2, Phase::Running),
            (3, Phase::Uploading),
        ] {
            assert_eq!(
                write(
                    &node,
                    &mut samples,
                    "progress",
                    Event::Progress {
                        attempt: attempt.id,
                        worker: attempt.worker,
                        incarnation: attempt.incarnation,
                        phase,
                        progress,
                    }
                )
                .await?,
                Reply::Applied
            );
        }
        assert_eq!(
            write(
                &node,
                &mut samples,
                "complete",
                Event::Complete {
                    attempt: attempt.id,
                    worker: attempt.worker,
                    incarnation: attempt.incarnation,
                    result: result.clone(),
                }
            )
            .await?,
            Reply::Applied
        );
    }
    let completed = node.read().await?;
    verify_terminals(&completed, &result, width);
    let digest_function = if algorithm == "sha256" {
        DigestHasherFunc::Sha256
    } else {
        DigestHasherFunc::Blake3
    };
    let metadata = verify_history(
        &completed,
        &result,
        digest_function,
        if history { width } else { 0 },
    )?;
    assert_eq!(completed.view().effects.len(), width);
    for effect in completed.view().effects {
        assert!(matches!(effect.kind, EffectKind::PublishResult { .. }));
        assert_eq!(
            write(
                &node,
                &mut samples,
                "synthetic_ac_ack",
                Event::EffectOutcome {
                    effect: effect.id,
                    success: true,
                }
            )
            .await?,
            Reply::Applied
        );
    }
    assert_eq!(
        write(
            &node,
            &mut samples,
            "advance_time",
            Event::AdvanceTime {
                now: TERMINAL_RETENTION
            }
        )
        .await?,
        Reply::Applied
    );
    let pending = node.read().await?;
    let pending_bytes = pending.snapshot()?;
    let pending_snapshot = snapshot(&node).await?;
    let first_measurements = node.journal.measurements();
    node.shutdown().await?;
    let pending_rows = persisted_rows(&path)?;
    let started = Instant::now();
    let reopened = open(&path, limits, 2).await?;
    let recovered = reopened.read().await?;
    let reopen_ns = started.elapsed().as_nanos();
    assert_eq!(recovered.snapshot()?, pending_bytes);
    verify_terminals(&recovered, &result, width);
    assert_eq!(
        verify_history(
            &recovered,
            &result,
            digest_function,
            if history { width } else { 0 }
        )?,
        metadata
    );
    if history {
        for execution in recovered.view().executions {
            assert_eq!(
                write(
                    &reopened,
                    &mut samples,
                    "pending_history_retirement_refusal",
                    Event::RetireExecution {
                        execution: execution.id,
                    }
                )
                .await?,
                Reply::Rejected(Rejection::Busy)
            );
        }
        assert_eq!(reopened.read().await?.snapshot()?, pending_bytes);
        for association in recovered.accepted_history().unwrap().associations.values() {
            // This is deliberately synthetic evidence; no provider accepted bytes.
            assert_eq!(
                write(
                    &reopened,
                    &mut samples,
                    "synthetic_history_stored_ack",
                    Event::HistoryPublication {
                        effect: association.effect,
                        object: association.recipe.object,
                        outcome: HistoryOutcome::Stored,
                    }
                )
                .await?,
                Reply::Applied
            );
        }
        let acknowledged = reopened.read().await?;
        verify_terminals(&acknowledged, &result, width);
        assert!(
            acknowledged
                .accepted_history()
                .unwrap()
                .associations
                .values()
                .all(|row| row.status == HistoryStatus::Materialized)
        );
    }
    for execution in recovered.view().executions {
        assert_eq!(
            write(
                &reopened,
                &mut samples,
                "retire",
                Event::RetireExecution {
                    execution: execution.id
                }
            )
            .await?,
            Reply::Applied
        );
    }
    let drained = reopened.read().await?;
    assert!(drained.view().executions.is_empty());
    assert!(drained.view().effects.is_empty());
    assert!(
        drained
            .accepted_history()
            .is_none_or(|rows| rows.associations.is_empty())
    );
    let drained_bytes = drained.snapshot()?;
    let drained_snapshot = snapshot(&reopened).await?;
    let second_measurements = reopened.journal.measurements();
    reopened.shutdown().await?;
    let final_rows = persisted_rows(&path)?;
    let started = Instant::now();
    let final_node = open(&path, limits, 3).await?;
    assert_eq!(final_node.read().await?.snapshot()?, drained_bytes);
    let drained_reopen_ns = started.elapsed().as_nanos();
    final_node.shutdown().await?;
    let entries = final_rows["log_entries"].as_array().unwrap();
    assert!(
        samples
            .windows(2)
            .all(|pair| pair[0]["log_index"].as_u64() < pair[1]["log_index"].as_u64())
    );
    for sample in &samples {
        assert!(
            entries
                .iter()
                .any(|entry| entry["index"] == sample["log_index"] && entry["kind"] == "normal")
        );
    }
    let command_bytes: u64 = entries
        .iter()
        .filter(|entry| {
            samples
                .iter()
                .any(|sample| sample["log_index"] == entry["index"])
        })
        .map(|entry| entry["envelope_bytes"].as_u64().unwrap())
        .sum();
    let report = json!({ "algorithm": algorithm, "width": width, "features": if history { vec![3] } else { vec![] },
        "stdout_payload_bytes": padding, "captured_response_bytes": captured.len(),
        "captured_response_sha256": digest(DigestHasherFunc::Sha256, captured), "result_json_bytes": serde_json::to_vec(&result)?.len(),
        "limits": limits, "effect_credit_per_execution": if history { 9 } else { 8 },
        "history_metadata": metadata, "activation": activation, "initial_log_index": initial_index,
        "admitted_snapshot": admitted, "pending_snapshot": pending_snapshot, "drained_snapshot": drained_snapshot,
        "pending_rows": pending_rows, "final_rows": final_rows, "reopen_with_barrier_ns": reopen_ns,
        "drained_reopen_with_barrier_ns": drained_reopen_ns,
        "normal_proposals": samples.len(), "measured_normal_log_envelope_bytes": command_bytes,
        "journal_before_reopen": first_measurements, "journal_after_reopen": second_measurements,
        "samples": samples, "host_start": start_host, "host_end": host_sample()? });
    write_new(&directory.join("measurement.json"), &report)?;
    Ok(report)
}

fn provenance(source: &Path, output: &Path, profile: &str) -> Result<Value> {
    let mut files = vec![
        "Cargo.toml".to_owned(),
        "Cargo.lock".to_owned(),
        "nativelink-scheduler/Cargo.toml".to_owned(),
        "nativelink-scheduler/BUILD.bazel".to_owned(),
        "nativelink-scheduler/examples/history_budget.rs".to_owned(),
    ];
    for entry in fs::read_dir(source.join("nativelink-scheduler/src/authority"))? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path.strip_prefix(source)?.to_string_lossy().into_owned());
        }
    }
    let hashes: BTreeMap<_, _> = files
        .into_iter()
        .map(|name| {
            Ok((
                name.clone(),
                digest(DigestHasherFunc::Sha256, &fs::read(source.join(name))?),
            ))
        })
        .collect::<Result<_>>()?;
    let mut commands = BTreeMap::new();
    for (name, args) in [
        ("git", vec!["rev-parse", "HEAD"]),
        ("rustc", vec!["-Vv"]),
        ("cargo", vec!["-V"]),
        ("uname", vec!["-a"]),
        ("lscpu", vec![]),
        (
            "findmnt",
            vec![
                "-T",
                output.to_str().ok_or("non-UTF8 output")?,
                "-o",
                "TARGET,SOURCE,FSTYPE,OPTIONS",
            ],
        ),
    ] {
        let value = Command::new(name).args(args).current_dir(source).output()?;
        assert!(value.status.success());
        commands.insert(name, String::from_utf8(value.stdout)?);
    }
    Ok(
        json!({ "source_sha256": hashes, "commands": commands, "profile_label_from_runner": profile,
        "debug_assertions": cfg!(debug_assertions), "runtime_threads": 2, "source_root": source,
        "executable_sha256": digest(DigestHasherFunc::Sha256, &fs::read(env::current_exe()?)?),
        "host": host_sample()?, "dependencies": { "openraft": "0.9.25", "redb": "4.4.0" },
        "interpretation": "runtime source hashes require unchanged build inputs; runner must independently freeze and verify sources" }),
    )
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 {
        return Err("usage: history_budget NEW_OUTPUT_DIRECTORY PROFILE_LABEL SOURCE_ROOT".into());
    }
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let source = fs::canonicalize(&args[3])?;
    write_new(
        &output.join("provenance.json"),
        &provenance(&source, output, &args[2])?,
    )?;
    #[expect(
        clippy::disallowed_methods,
        reason = "Standalone measurement owns its fixed two-thread runtime"
    )]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    #[expect(
        clippy::disallowed_methods,
        reason = "Standalone entry point measures one fixed matrix then writes reports"
    )]
    let measured: Result<Vec<Value>> = runtime.block_on(async {
        capacity(output).await?;
        let mut reports = Vec::new();
        for algorithm in ["sha256", "blake3"] {
            for width in [4, 16] {
                for padding in [0, 4096, 32768] {
                    for history in [false, true] {
                        reports.push(measure(output, algorithm, width, padding, history).await?);
                    }
                }
            }
        }
        Ok(reports)
    });
    match measured {
        Ok(reports) => write_new(
            &output.join("summary.json"),
            &json!({
                "scope": "bounded M0 refinement, actual singleton durability with synthetic reports and acknowledgements",
                "cases": reports.len(), "host_end": host_sample()?,
                "limitations": ["No provider I/O, shared publication, GC, replication quorum, throughput or SLO qualification",
                    "Proposal latency ends after durable apply; operation commit deltas aggregate calls during that proposal, not device fsync counts",
                    "Persisted row and snapshot bytes are logical storage bytes, not physical write traffic",
                    "One fixed pass on a shared development host; no CPU, filesystem cache or load isolation",
                    "Zero stdout bytes still produce a valid captured protobuf response; no missing response payload is substituted"],
                "measurements": reports
            }),
        ),
        Err(error) => {
            write_new(
                &output.join("failure.json"),
                &json!({ "error": error.to_string(), "host": host_sample()? }),
            )?;
            Err(error)
        }
    }
}
