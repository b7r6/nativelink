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

//! Real scheduler, worker processes, filesystem failures, and SDK HTTP downloads.

mod account;
mod checks;
mod node;
mod transport;
mod witness;
mod workload;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures::future::join_all;
use nativelink_config::cas_server::{ExecutionConfig, WithInstanceName, WorkerApiConfig};
use nativelink_config::schedulers::SchedulerSpec;
use nativelink_config::stores::MemorySpec;
use nativelink_proto::build::bazel::remote::execution::v2::execution_client::ExecutionClient;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ExecuteRequest, ExecuteResponse, ExecutedActionMetadata,
};
use nativelink_proto::google::longrunning::operation;
use nativelink_scheduler::default_scheduler_factory::scheduler_factory;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_service::execution_server::ExecutionServer;
use nativelink_service::worker_api_server::WorkerApiServer;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::time::{Instant, timeout};
use tracing_subscriber::prelude::*;
use workload::{Job, NODES, Workload};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[expect(
    clippy::print_stdout,
    reason = "the replay CLI prints its seed and result location"
)]
fn announce(message: std::fmt::Arguments<'_>) {
    println!("{message}");
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    node: Option<String>,
    #[arg(long, default_value_t = 2026101700)]
    seed: u64,
    #[arg(long, default_value = "failed-fill", value_parser = ["control", "failed-fill"])]
    scenario: String,
    #[arg(long)]
    output: Option<PathBuf>,
}

struct Node {
    child: Child,
    input: ChildStdin,
    output: Lines<BufReader<ChildStdout>>,
    evidence: Value,
}

impl Node {
    async fn spawn(
        config: node::Config,
        directory: &Path,
        executable: &Path,
        cpus: &str,
    ) -> Result<Self> {
        let config_path = directory.join(format!("node-{}.json", config.node));
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config)?)?;
        let log = std::fs::File::create(directory.join(format!("node-{}.log", config.node)))?;

        // Each child owns a private mount namespace. stdout is the control
        // protocol; all worker diagnostics go to its retained stderr log.
        let mut child = Command::new("unshare")
            .args([
                "--user",
                "--map-root-user",
                "--mount",
                "--propagation",
                "private",
                "taskset",
                "--cpu-list",
                cpus,
            ])
            .arg(executable)
            .arg("--node")
            .arg(config_path)
            .env("RUST_LOG", "warn,nativelink_worker::local_worker=debug,nativelink_worker::running_actions_manager=debug")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(log)
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("missing child stdin")?;
        let output = BufReader::new(child.stdout.take().ok_or("missing child stdout")?).lines();
        let mut node = Self {
            child,
            input,
            output,
            evidence: Value::Null,
        };

        match node.receive().await {
            Ok(evidence) if evidence["ready"] == true => {
                node.evidence = evidence;
                Ok(node)
            }
            received => {
                // A startup failure must not leave a private mount/worker alive.
                drop(node.child.kill().await);
                drop(node.child.wait().await);
                Err(received
                    .err()
                    .unwrap_or_else(|| "worker did not become ready".into()))
            }
        }
    }

    async fn receive(&mut self) -> Result<Value> {
        let line = timeout(Duration::from_secs(20), self.output.next_line())
            .await??
            .ok_or("worker control channel closed; inspect node log")?;
        Ok(serde_json::from_str(&line)?)
    }

    async fn command(&mut self, command: &str) -> Result<Value> {
        self.input
            .write_all(format!("{command}\n").as_bytes())
            .await?;
        self.input.flush().await?;
        self.receive().await
    }

    async fn stop(&mut self) -> Value {
        let command = self.command("stop").await;
        let mut forced = false;

        let status = if let Ok(status) = timeout(Duration::from_secs(8), self.child.wait()).await {
            status
        } else {
            forced = true;
            // kill() waits for the child, so even a failed drain is reaped.
            match self.child.kill().await {
                Ok(()) => self.child.wait().await,
                Err(error) => Err(error),
            }
        };

        json!({
            "node": self.evidence["node"],
            "pid": self.evidence["pid"],
            "success": command.is_ok() && status.as_ref().is_ok_and(std::process::ExitStatus::success) && !forced,
            "forced": forced,
            "reply": command.as_ref().ok(),
            "command_error": command.as_ref().err().map(ToString::to_string),
            "exit_code": status.as_ref().ok().and_then(std::process::ExitStatus::code),
            "wait_error": status.as_ref().err().map(ToString::to_string),
        })
    }
}

async fn scheduler(
    workload: &Workload,
) -> Result<(
    String,
    Arc<dyn WorkerScheduler>,
    tokio::task::JoinHandle<()>,
)> {
    let stores = StoreManager::new();
    let cas = Store::new(MemoryStore::new(&MemorySpec::default()));

    // The scheduler needs action metadata. Workers still fetch cold payloads
    // through their own Azure-backed stores and the HTTP account.
    for blob in &workload.blobs {
        cas.update_oneshot(blob.digest, blob.data.clone()).await?;
    }
    stores.add_store("cas", cas)?;

    let spec: SchedulerSpec = serde_json::from_value(json!({
        "simple": {
            "supported_platform_properties": {"fleet_node": "exact"},
            "max_job_retries": 3,
            "worker_timeout_s": 30,
            "client_action_timeout_s": 180,
        }
    }))?;
    let (actions, workers) = scheduler_factory(&spec, &stores, None).await?;
    let workers = workers.ok_or("missing worker scheduler")?;
    let execution = ExecutionServer::new(
        &[WithInstanceName {
            instance_name: String::new(),
            config: ExecutionConfig {
                experimental_buck2_invocation_isolation: false,
                cas_store: "cas".into(),
                scheduler: "scheduler".into(),
            },
        }],
        &HashMap::from([(
            "scheduler".into(),
            actions.ok_or("missing action scheduler")?,
        )]),
        &stores,
    )?;
    let api = WorkerApiServer::new(
        &WorkerApiConfig {
            scheduler: "scheduler".into(),
            disable_kill_revoked_operations: false,
            kill_revoked_operations_interval_s: 5,
        },
        &HashMap::from([("scheduler".into(), workers.clone())]),
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = format!("http://{}", listener.local_addr()?);
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let task = nativelink_util::background_spawn!("fleet_scheduler", async move {
        tonic::transport::Server::builder()
            .add_service(execution.into_service())
            .add_service(api.into_service())
            .serve_with_incoming(incoming)
            .await
            .expect("scheduler gRPC server");
    });

    Ok((address, workers, task))
}

async fn execute(address: &str, job: &Job, account: &account::Account) -> Result<Value> {
    let start = account.elapsed();
    let channel = tonic::transport::Endpoint::from_shared(address.to_owned())?
        .connect()
        .await?;
    let mut client = ExecutionClient::new(channel);
    let mut stream = client
        .execute(ExecuteRequest {
            action_digest: Some(job.digest().into()),
            skip_cache_lookup: true,
            digest_function: 1,
            ..Default::default()
        })
        .await?
        .into_inner();
    while let Some(operation) = stream.message().await? {
        if !operation.done {
            continue;
        }
        match operation
            .result
            .ok_or("terminal operation without result")?
        {
            operation::Result::Response(response) => {
                let response = ExecuteResponse::decode(response.value.as_slice())?;
                return Ok(execution_result(&operation.name, response, start, account));
            }
            operation::Result::Error(status) => {
                return Ok(json!({
                    "operation": operation.name,
                    "started_s": start,
                    "finished_s": account.elapsed(),
                    "outcome": "failed",
                    "status_code": status.code,
                    "status": status.message,
                }));
            }
        }
    }
    Err("Execute stream ended without terminal operation".into())
}

fn timestamp(stamp: &Option<prost_types::Timestamp>) -> Option<Value> {
    stamp
        .as_ref()
        .map(|stamp| json!({"seconds": stamp.seconds, "nanos": stamp.nanos}))
}

fn lifecycle(metadata: &ExecutedActionMetadata) -> Value {
    json!({
        "worker_start": timestamp(&metadata.worker_start_timestamp),
        "input_fetch_start": timestamp(&metadata.input_fetch_start_timestamp),
        "input_fetch_completed": timestamp(&metadata.input_fetch_completed_timestamp),
        "execution_start": timestamp(&metadata.execution_start_timestamp),
        "execution_completed": timestamp(&metadata.execution_completed_timestamp),
        "output_upload_start": timestamp(&metadata.output_upload_start_timestamp),
        "output_upload_completed": timestamp(&metadata.output_upload_completed_timestamp),
        "worker_completed": timestamp(&metadata.worker_completed_timestamp),
    })
}

fn execution_result(
    name: &str,
    response: ExecuteResponse,
    start: f64,
    account: &account::Account,
) -> Value {
    let result = response.result.as_ref();
    let status = response.status.as_ref().map_or(0, |status| status.code);
    let exit = result.map(|result| result.exit_code);
    let metadata = result.and_then(|result| result.execution_metadata.as_ref());
    let worker = metadata.map(|metadata| metadata.worker.clone());
    let lifecycle = metadata.map(lifecycle);
    let outputs = result.map(|result| {
        result
            .output_files
            .iter()
            .map(|file| {
                json!({
                    "path": file.path,
                    "digest": file.digest.as_ref().map(|digest| json!({
                        "hash": digest.hash,
                        "size_bytes": digest.size_bytes,
                    })),
                })
            })
            .collect::<Vec<_>>()
    });

    json!({
        "operation": name,
        "started_s": start,
        "finished_s": account.elapsed(),
        "outcome": if status == 0 && exit == Some(0) { "success" } else { "failed" },
        "exit_code": exit,
        "status_code": status,
        "status": response.status.map(|status| status.message),
        "worker_id": worker,
        "lifecycle": lifecycle,
        "outputs": outputs,
    })
}

async fn job_result(
    address: String,
    job: Job,
    account: Arc<account::Account>,
    phase: &'static str,
    origin: Instant,
    repair_at: Option<Instant>,
) -> Value {
    tokio::time::sleep_until(origin + Duration::from_secs_f64(job.arrival_s)).await;
    let mut attempts = Vec::new();
    // One explicit client resubmission after repair, identical in both arms.
    // Keep the original failed attempt in the record; never drop offered work.
    for attempt in 0..2 {
        let start = account.elapsed();
        let result = timeout(Duration::from_secs(130), execute(&address, &job, &account)).await;
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                json!({
                    "started_s": start,
                    "finished_s": account.elapsed(),
                    "outcome": "failed",
                    "status": error.to_string(),
                })
            }
            Err(_) => {
                json!({
                    "started_s": start,
                    "finished_s": account.elapsed(),
                    "outcome": "censored",
                    "status": "130 second client deadline",
                })
            }
        };
        let success = value["outcome"] == "success";
        attempts.push(value);
        if success || attempt == 1 || repair_at.is_none() {
            break;
        }
        tokio::time::sleep_until(repair_at.unwrap() + Duration::from_millis(500)).await;
    }

    // The top-level outcome describes the last attempt. The attempts array
    // preserves earlier failures and deadlines for the acceptance checks.
    let mut value = attempts.last().unwrap().clone();
    value["id"] = json!(job.id);
    value["node"] = json!(job.node);
    value["phase"] = json!(phase);
    value["arrival_s"] = json!(origin.duration_since(account.start).as_secs_f64() + job.arrival_s);
    value["planned_arrival_s"] = json!(job.arrival_s);
    value["started_s"] = attempts[0]["started_s"].clone();
    value["attempts"] = json!(attempts);

    if value["outcome"] == "success" {
        account.state.lock().unwrap().completed_actions += 1;
    }

    value
}

fn load() -> Value {
    json!({
        "loadavg": std::fs::read_to_string("/proc/loadavg").unwrap_or_default(),
        "pressure_cpu": std::fs::read_to_string("/proc/pressure/cpu").unwrap_or_default(),
        "pressure_io": std::fs::read_to_string("/proc/pressure/io").unwrap_or_default(),
    })
}

fn cpus() -> Result<String> {
    // Read the inherited affinity; do not assume host CPUs are available to us.
    let status = std::fs::read_to_string("/proc/self/status")?;
    let allowed = status
        .lines()
        .find_map(|l| l.strip_prefix("Cpus_allowed_list:\t"))
        .ok_or("missing affinity")?;
    let mut cpus = Vec::new();
    for piece in allowed.split(',') {
        let ends: Vec<_> = piece
            .split('-')
            .map(str::parse::<u32>)
            .collect::<std::result::Result<_, _>>()?;
        cpus.extend(ends[0]..=ends.get(1).copied().unwrap_or(ends[0]));
    }
    Ok(cpus
        .into_iter()
        .take(8)
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(","))
}

// Run identity is kept apart from session evidence so teardown can
// still publish a complete report after a setup or workload error.
struct RunContext {
    seed: u64,
    scenario: String,
    output: PathBuf,
    executable: PathBuf,
    binary_sha256: String,
    workload_sha256: String,
    start_unix_s: f64,
    load_before: Value,
}

impl RunContext {
    fn new(args: Args) -> Result<Self> {
        let output = args.output.ok_or("--output is required")?;
        std::fs::create_dir(&output)?;
        let output = std::fs::canonicalize(output)?;
        let executable = std::env::current_exe()?;
        let binary_sha256 = hex::encode(Sha256::digest(std::fs::read(&executable)?));
        let start_unix_s = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
        let load_before = load();

        announce(format_args!(
            "seed={} scenario={} output={}",
            args.seed,
            args.scenario,
            output.display()
        ));

        Ok(Self {
            seed: args.seed,
            scenario: args.scenario,
            output,
            executable,
            binary_sha256,
            workload_sha256: String::new(),
            start_unix_s,
            load_before,
        })
    }

    fn write_manifest(&mut self, workload: &Workload) -> Result<()> {
        let manifest = json!({
            "seed": self.seed,
            "catalogue_bytes": workload.catalogue_bytes,
            "warmup": workload.warmup,
            "jobs": workload.jobs,
            "sentinel": workload.sentinel,
        });
        let bytes = serde_json::to_vec_pretty(&manifest)?;
        self.workload_sha256 = hex::encode(Sha256::digest(&bytes));
        std::fs::write(self.output.join("workload.json"), bytes)?;
        Ok(())
    }
}

#[derive(Default)]
struct Session {
    nodes: Vec<Node>,
    services: Vec<tokio::task::JoinHandle<()>>,
    jobs: Vec<Value>,
    events: Vec<Value>,
    fault: Value,
    before_sentinel: Value,
    affinity: String,
    snapshots: Arc<Mutex<Vec<Value>>>,
}

impl Session {
    async fn start(
        &mut self,
        context: &RunContext,
        workload: &Workload,
        account: Arc<account::Account>,
    ) -> Result<(String, Arc<dyn WorkerScheduler>)> {
        // Register each resource immediately. If the next startup step fails,
        // the common teardown path still owns everything already started.
        let (azure, http_task) = account::serve(account).await?;
        self.services.push(http_task);
        let (address, scheduler, grpc_task) = scheduler(workload).await?;
        self.services.push(grpc_task);
        self.affinity = cpus()?;

        for node in 0..NODES {
            let root = context.output.join(format!("disk-{node}"));
            std::fs::create_dir(&root)?;
            self.nodes.push(
                Node::spawn(
                    node::Config {
                        seed: context.seed,
                        node,
                        root,
                        azure: azure.clone(),
                        scheduler: address.clone(),
                        metadata: workload.metadata.clone(),
                    },
                    &context.output,
                    &context.executable,
                    &self.affinity,
                )
                .await?,
            );
        }

        self.register_workers(&scheduler).await?;
        Ok((address, scheduler))
    }

    async fn register_workers(&mut self, scheduler: &Arc<dyn WorkerScheduler>) -> Result<()> {
        timeout(Duration::from_secs(15), async {
            loop {
                if scheduler.worker_snapshot().await.len() == NODES {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;

        let registered = scheduler.worker_snapshot().await;
        if !registered
            .iter()
            .all(|worker| worker.max_inflight_tasks == 2)
        {
            return Err("worker registered an unexpected inflight limit".into());
        }

        // Match the scheduler identity through the placement property, rather
        // than relying on registration order or process IDs.
        for (index, node) in self.nodes.iter_mut().enumerate() {
            let worker = registered
                .iter()
                .find(|worker| {
                    worker.platform_properties.get("fleet_node") == Some(&index.to_string())
                })
                .ok_or("missing placement property")?;
            node.evidence["worker_id"] = json!(worker.id);
        }
        Ok(())
    }

    fn start_sampler(
        &mut self,
        scheduler: Arc<dyn WorkerScheduler>,
        account: Arc<account::Account>,
    ) {
        let snapshots = self.snapshots.clone();
        let task = nativelink_util::background_spawn!("fleet_sampler", async move {
            loop {
                let workers = scheduler.worker_snapshot().await;
                snapshots.lock().unwrap().push(json!({
                    "time_s": account.elapsed(),
                    "workers": workers,
                    "load": load(),
                }));
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        self.services.push(task);
    }

    async fn warmup(
        &mut self,
        address: &str,
        context: &RunContext,
        workload: &Workload,
        account: &Arc<account::Account>,
    ) -> Result<()> {
        self.jobs = join_all(workload.warmup.iter().cloned().map(|job| {
            job_result(
                address.to_owned(),
                job,
                account.clone(),
                "warmup",
                Instant::now(),
                None,
            )
        }))
        .await;

        if self.jobs.iter().any(|job| job["outcome"] != "success") {
            std::fs::write(
                context.output.join("warmup-failed.json"),
                serde_json::to_vec_pretty(&self.jobs)?,
            )?;
            return Err("warmup failed; inspect logs".into());
        }

        account.quiesce().await?;
        Ok(())
    }

    async fn disk_command(&mut self, command: &str, account: &account::Account) -> Result<()> {
        // Only the first two workers receive the storage fault. Keep every
        // command reply with its timestamp, including partial setup failures.
        for node in self.nodes.iter_mut().take(2) {
            self.events.push(json!({
                "time_s": account.elapsed(),
                "node": node.evidence["node"],
                "reply": node.command(command).await?,
            }));
        }
        Ok(())
    }

    async fn work(
        &mut self,
        address: &str,
        failed_fill: bool,
        workload: &Workload,
        account: &Arc<account::Account>,
    ) -> Result<()> {
        if failed_fill {
            self.disk_command("fill", account).await?;
        }

        account.phase("work");
        let origin = Instant::now();
        let repair_at = origin + Duration::from_secs(2);
        self.fault = if failed_fill {
            json!({"started_s": account.elapsed(), "repaired_s": null, "nodes": [0, 1]})
        } else {
            Value::Null
        };

        let work = join_all(workload.jobs.iter().cloned().map(|job| {
            job_result(
                address.to_owned(),
                job,
                account.clone(),
                "work",
                origin,
                failed_fill.then_some(repair_at),
            )
        }));
        let repair = async {
            if failed_fill {
                tokio::time::sleep_until(repair_at).await;
                self.disk_command("repair", account).await?;
                self.fault["repaired_s"] = json!(account.elapsed());
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        };

        // Repair proceeds while actions are still running. Wait for both sides
        // and retain completed work even if the repair itself reports an error.
        let (completed, repaired) = tokio::join!(work, repair);
        self.jobs.extend(completed);
        repaired?;
        account.quiesce().await?;
        Ok(())
    }

    async fn sentinels(
        &mut self,
        address: &str,
        workload: &Workload,
        account: &Arc<account::Account>,
    ) -> Result<()> {
        self.before_sentinel = account.report();
        account.phase("sentinel");
        let sentinel = join_all(workload.sentinel.iter().cloned().map(|job| {
            job_result(
                address.to_owned(),
                job,
                account.clone(),
                "sentinel",
                Instant::now(),
                None,
            )
        }))
        .await;

        // Retain completed sentinels even if a subsequent snapshot fails.
        self.jobs.extend(sentinel.iter().cloned());
        for (index, node) in self.nodes.iter_mut().enumerate() {
            let evidence = node.command("snapshot").await?;
            node.evidence["final"] = evidence.clone();
            node.evidence["sentinel_same_process"] = json!(
                sentinel[index]["outcome"] == "success"
                    && evidence["pid"] == node.evidence["pid"]
                    && sentinel[index]["worker_id"] == node.evidence["worker_id"]
            );
        }

        account.quiesce().await?;
        Ok(())
    }

    async fn run(
        &mut self,
        context: &RunContext,
        workload: &Workload,
        account: Arc<account::Account>,
    ) -> Result<()> {
        let (address, scheduler) = self.start(context, workload, account.clone()).await?;
        self.start_sampler(scheduler.clone(), account.clone());

        self.warmup(&address, context, workload, &account).await?;
        self.work(
            &address,
            context.scenario == "failed-fill",
            workload,
            &account,
        )
        .await?;
        self.sentinels(&address, workload, &account).await?;

        // Join the sampler before taking the final snapshot; shutdown observations
        // must not be mistaken for lost workers during the experiment.
        let sampler = self
            .services
            .pop()
            .expect("sampler is the last started service");
        sampler.abort();
        drop(sampler.await);
        let workers = scheduler.worker_snapshot().await;
        self.snapshots.lock().unwrap().push(json!({
            "time_s": account.elapsed(),
            "workers": workers,
            "final": true,
        }));
        Ok(())
    }

    async fn stop(&mut self) -> Vec<Value> {
        let mut cleanup = Vec::new();
        for node in &mut self.nodes {
            cleanup.push(node.stop().await);
        }

        for service in self.services.drain(..) {
            service.abort();
            drop(service.await);
        }
        cleanup
    }

    fn collect_evidence(&mut self, output: &Path, account_report: &Value) {
        for node in &mut self.nodes {
            node.evidence["sentinel_cache_reused"] = json!(
                !account_report["reads"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|read| {
                        read["phase"] == "sentinel" && read["node"] == node.evidence["node"]
                    })
            );

            let log_path = output.join(format!("node-{}.log", node.evidence["node"]));
            match std::fs::read_to_string(log_path) {
                Ok(log) => {
                    node.evidence["error_witnesses"] = json!(
                        log.lines()
                            .filter(|line| {
                                line.contains("os error 28")
                                    || line.contains("Retries exhausted")
                                    || line.contains("Tried to send while stream is closed")
                                    || line.contains("receiver disconnected")
                            })
                            .collect::<Vec<_>>()
                    );
                    node.evidence["worker_enospc_observed"] = json!(log.contains("os error 28"));
                    node.evidence["closed_destination_retry_exhaustions"] =
                        json!(checks::closed_destination_exhaustions(&log));
                }
                Err(error) => node.evidence["log_error"] = json!(error.to_string()),
            }

            // Request ownership comes from the SDK context, not HEAD order or
            // matching clocks between worker and HTTP account processes.
            let reads: Vec<_> = account_report["reads"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|read| read["node"] == node.evidence["node"])
                .cloned()
                .collect();
            let trace_path = output.join(format!("node-{}.downloads.jsonl", node.evidence["node"]));
            node.evidence["download_witness"] = witness::analyze(&trace_path, &reads);
        }
    }

    fn report(
        &self,
        context: &RunContext,
        workload: &Workload,
        session_error: Option<&str>,
        cleanup: &[Value],
        account_report: &Value,
        duration_s: f64,
    ) -> Value {
        json!({
            "schema_version": 2,
            "scenario": context.scenario,
            "seed": context.seed,
            "session_error": session_error,
            "cleanup": cleanup,
            "expected_jobs": workload.warmup.len() + workload.jobs.len() + workload.sentinel.len(),
            "binary_sha256": context.binary_sha256,
            "workload_sha256": context.workload_sha256,
            "start_unix_s": context.start_unix_s,
            "duration_s": duration_s,
            "load": {"before": context.load_before, "after": load()},
            "fault": self.fault,
            "events": self.events,
            "jobs": self.jobs,
            "nodes": self.nodes.iter().map(|node| &node.evidence).collect::<Vec<_>>(),
            "account": account_report,
            "before_sentinel": self.before_sentinel,
            "scheduler_snapshots": *self.snapshots.lock().unwrap(),
            "cpu_affinity": self.affinity,
            "measurement": {
                "topology": "four separate LocalWorker processes, private mounts, real scheduler over gRPC, shared HTTP origin on one host",
                "retry": {
                    "max_retries": 6,
                    "delay": 0.3,
                    "jitter": 0.5,
                    "throttle_retry_delay_s": 2,
                    "throttle_retry_max_delay_s": 60,
                    "throttle_retry_budget_s": 0,
                    "sdk_retry": "disabled",
                },
                "client_policy": "one resubmission after repair for failed work; every attempt retained",
                "started_s": "client submission, not command start",
            }
        })
    }
}

fn write_report(output: &Path, mut report: Value) -> Result<()> {
    let failures = checks::failures(&report);
    report["accepted"] = json!(failures.is_empty());
    report["failures"] = json!(failures);
    let path = output.join("run.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&report)?)?;

    announce(format_args!(
        "payload_bytes={} completed={} results={}",
        report["account"]["payload_bytes"],
        report["account"]["completed_actions"],
        path.display()
    ));
    for node in report["nodes"].as_array().into_iter().flatten() {
        announce(format_args!(
            "node={}: {}",
            node["node"],
            witness::explain(&node["download_witness"])
        ));
    }
    if !failures.is_empty() {
        return Err(format!(
            "{}; results retained at {}",
            failures.join("; "),
            path.display()
        )
        .into());
    }
    Ok(())
}

async fn run(args: Args) -> Result<()> {
    let mut context = RunContext::new(args)?;
    let workload = Workload::new(context.seed);
    context.write_manifest(&workload)?;
    let account = account::Account::new(context.seed, &workload.blobs);
    let mut session = Session::default();
    let outcome = session.run(&context, &workload, account.clone()).await;

    // Acceptance errors and setup failures use the same teardown/report path.
    // Keep all observed work, HTTP ranges and raw logs even when the run fails.
    let cleanup = session.stop().await;
    let account_report = account.report();
    session.collect_evidence(&context.output, &account_report);
    let session_error = outcome.as_ref().err().map(ToString::to_string);
    let report = session.report(
        &context,
        &workload,
        session_error.as_deref(),
        &cleanup,
        &account_report,
        account.elapsed(),
    );
    write_report(&context.output, report)
}

#[expect(
    clippy::disallowed_methods,
    reason = "the standalone executable owns its Tokio runtime"
)]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    drop(tokio_rustls::rustls::crypto::ring::default_provider().install_default());
    let args = Args::parse();
    let capture = args
        .node
        .as_ref()
        .map(|path| witness::capture(&Path::new(path).with_extension("downloads.jsonl")))
        .transpose()?;
    let (capture_layer, capture_handle) = match capture {
        Some((layer, handle)) => (Some(layer), Some(handle)),
        None => (None, None),
    };
    let logs = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        );
    tracing_subscriber::registry()
        .with(logs)
        .with(capture_layer.map(|layer| {
            layer.with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.target() == "nativelink_store::azure_blob_store::download"
            }))
        }))
        .init();
    let result = if let Some(path) = args.node {
        node::run(&path).await
    } else {
        run(args).await
    };
    if let Some(handle) = capture_handle {
        handle.finish()?;
    }
    result
}
