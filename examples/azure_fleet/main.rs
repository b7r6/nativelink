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
mod node;
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
use nativelink_proto::build::bazel::remote::execution::v2::{ExecuteRequest, ExecuteResponse};
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
    #[arg(long, default_value = "fixed", value_parser = ["baseline", "fixed"])]
    arm: String,
    #[arg(long, default_value = "failed-fill", value_parser = ["control", "failed-fill"])]
    scenario: String,
    #[arg(long, default_value_t = 0)]
    pair_order: u8,
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
        node.evidence = node.receive().await?;
        if node.evidence["ready"] != true {
            return Err("worker did not become ready".into());
        }
        Ok(node)
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
    async fn stop(&mut self) {
        drop(self.command("stop").await);
        if timeout(Duration::from_secs(8), self.child.wait())
            .await
            .is_err()
        {
            drop(self.child.kill().await);
        }
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
    for blob in &workload.blobs {
        cas.update_oneshot(blob.digest, blob.data.clone()).await?;
    }
    stores.add_store("cas", cas)?;
    let spec: SchedulerSpec = serde_json::from_value(json!({"simple":{
        "supported_platform_properties":{"fleet_node":"exact"}, "max_job_retries":3,
        "worker_timeout_s":30, "client_action_timeout_s":180
    }}))?;
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
                let status = response.status.as_ref().map_or(0, |s| s.code);
                let exit = response.result.as_ref().map(|r| r.exit_code);
                let worker = response
                    .result
                    .as_ref()
                    .and_then(|r| r.execution_metadata.as_ref())
                    .map(|m| m.worker.clone());
                let lifecycle = response.result.as_ref().and_then(|r|r.execution_metadata.as_ref()).map(|m| {
                    let stamp = |s: &Option<prost_types::Timestamp>| s.as_ref().map(|t|json!({"seconds":t.seconds,"nanos":t.nanos}));
                    json!({"worker_start":stamp(&m.worker_start_timestamp),"input_fetch_start":stamp(&m.input_fetch_start_timestamp),"input_fetch_completed":stamp(&m.input_fetch_completed_timestamp),"execution_start":stamp(&m.execution_start_timestamp),"execution_completed":stamp(&m.execution_completed_timestamp),"output_upload_start":stamp(&m.output_upload_start_timestamp),"output_upload_completed":stamp(&m.output_upload_completed_timestamp),"worker_completed":stamp(&m.worker_completed_timestamp)})
                });
                let outputs = response.result.as_ref().map(|r|r.output_files.iter().map(|f|json!({"path":f.path,"digest":f.digest.as_ref().map(|d|json!({"hash":d.hash,"size_bytes":d.size_bytes}))})).collect::<Vec<_>>());
                return Ok(
                    json!({"operation":operation.name,"started_s":start,"finished_s":account.elapsed(),"outcome":if status==0 && exit==Some(0){"success"}else{"failed"},"exit_code":exit,"status_code":status,"status":response.status.map(|s|s.message),"worker_id":worker,"lifecycle":lifecycle,"outputs":outputs}),
                );
            }
            operation::Result::Error(status) => {
                return Ok(
                    json!({"operation":operation.name,"started_s":start,"finished_s":account.elapsed(),"outcome":"failed","status_code":status.code,"status":status.message}),
                );
            }
        }
    }
    Err("Execute stream ended without terminal operation".into())
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
                json!({"started_s":start,"finished_s":account.elapsed(),"outcome":"failed","status":error.to_string()})
            }
            Err(_) => {
                json!({"started_s":start,"finished_s":account.elapsed(),"outcome":"censored","status":"130 second client deadline"})
            }
        };
        let success = value["outcome"] == "success";
        attempts.push(value);
        if success || attempt == 1 || repair_at.is_none() {
            break;
        }
        tokio::time::sleep_until(repair_at.unwrap() + Duration::from_millis(500)).await;
    }
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
    json!({"loadavg":std::fs::read_to_string("/proc/loadavg").unwrap_or_default(),"pressure_cpu":std::fs::read_to_string("/proc/pressure/cpu").unwrap_or_default(),"pressure_io":std::fs::read_to_string("/proc/pressure/io").unwrap_or_default()})
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

async fn run(args: Args) -> Result<()> {
    if let Some(variant) = option_env!("FLEET_VARIANT")
        && variant != args.arm
    {
        return Err("--arm disagrees with compiled FLEET_VARIANT".into());
    }
    let output = args.output.ok_or("--output is required")?;
    std::fs::create_dir(&output)?;
    let output = std::fs::canonicalize(output)?;
    let executable = std::env::current_exe()?;
    let binary_sha256 = hex::encode(Sha256::digest(std::fs::read(&executable)?));
    let start_unix_s = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs_f64();
    let before = load();
    announce(format_args!(
        "seed={} arm={} scenario={} output={}",
        args.seed,
        args.arm,
        args.scenario,
        output.display()
    ));
    let workload = Workload::new(args.seed);
    let manifest = json!({"seed":args.seed,"catalogue_bytes":workload.catalogue_bytes,"warmup":workload.warmup,"jobs":workload.jobs,"sentinel":workload.sentinel});
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    let workload_sha256 = hex::encode(Sha256::digest(&manifest_bytes));
    std::fs::write(output.join("workload.json"), manifest_bytes)?;
    let account = account::Account::new(args.seed, &workload.blobs);
    let (azure, http_task) = account::serve(account.clone()).await?;
    let (address, scheduler, grpc_task) = scheduler(&workload).await?;
    let affinity = cpus()?;
    let mut nodes = Vec::new();
    for node in 0..NODES {
        let root = output.join(format!("disk-{node}"));
        std::fs::create_dir(&root)?;
        nodes.push(
            Node::spawn(
                node::Config {
                    seed: args.seed,
                    node,
                    root,
                    azure: azure.clone(),
                    scheduler: address.clone(),
                    metadata: workload.metadata.clone(),
                },
                &output,
                &executable,
                &affinity,
            )
            .await?,
        );
    }
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
    assert!(registered.iter().all(|w| w.max_inflight_tasks == 2));
    for (i, node) in nodes.iter_mut().enumerate() {
        let w = registered
            .iter()
            .find(|w| w.platform_properties.get("fleet_node") == Some(&i.to_string()))
            .ok_or("missing placement property")?;
        node.evidence["worker_id"] = json!(w.id);
    }
    let snapshots = Arc::new(Mutex::new(Vec::new()));
    let sample_task = {
        let scheduler = scheduler.clone();
        let account = account.clone();
        let snapshots = snapshots.clone();
        nativelink_util::background_spawn!("fleet_sampler", async move {
            loop {
                let workers = scheduler.worker_snapshot().await;
                snapshots
                    .lock()
                    .unwrap()
                    .push(json!({"time_s":account.elapsed(),"workers":workers,"load":load()}));
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    let mut jobs = join_all(workload.warmup.iter().cloned().map(|j| {
        job_result(
            address.clone(),
            j,
            account.clone(),
            "warmup",
            Instant::now(),
            None,
        )
    }))
    .await;
    if jobs.iter().any(|j| j["outcome"] != "success") {
        std::fs::write(
            output.join("warmup-failed.json"),
            serde_json::to_vec_pretty(&jobs)?,
        )?;
        return Err("warmup failed; inspect logs".into());
    }
    account.quiesce().await;
    let mut events = Vec::new();
    if args.scenario == "failed-fill" {
        for node in nodes.iter_mut().take(2) {
            events.push(json!({"time_s":account.elapsed(),"node":node.evidence["node"],"reply":node.command("fill").await?}));
        }
    }
    account.phase("work");
    let origin = Instant::now();
    let repair_at = origin + Duration::from_secs(2);
    let mut fault = if args.scenario == "failed-fill" {
        json!({"started_s":account.elapsed(),"repaired_s":null,"nodes":[0,1]})
    } else {
        Value::Null
    };
    let work: Vec<_> = workload
        .jobs
        .iter()
        .cloned()
        .map(|j| {
            nativelink_util::background_spawn!(
                "fleet_action",
                job_result(
                    address.clone(),
                    j,
                    account.clone(),
                    "work",
                    origin,
                    if args.scenario == "failed-fill" {
                        Some(repair_at)
                    } else {
                        None
                    },
                )
            )
        })
        .collect();
    if args.scenario == "failed-fill" {
        tokio::time::sleep_until(repair_at).await;
        for node in nodes.iter_mut().take(2) {
            events.push(json!({"time_s":account.elapsed(),"node":node.evidence["node"],"reply":node.command("repair").await?}));
        }
        fault["repaired_s"] = json!(account.elapsed());
    }
    for result in join_all(work).await {
        jobs.push(result?);
    }
    account.quiesce().await;
    let before_sentinel = account.report();
    account.phase("sentinel");
    let sentinel = join_all(workload.sentinel.iter().cloned().map(|j| {
        job_result(
            address.clone(),
            j,
            account.clone(),
            "sentinel",
            Instant::now(),
            None,
        )
    }))
    .await;
    for (i, node) in nodes.iter_mut().enumerate() {
        let evidence = node.command("snapshot").await?;
        node.evidence["final"] = evidence.clone();
        node.evidence["sentinel_same_process"] = json!(
            sentinel[i]["outcome"] == "success"
                && evidence["pid"] == node.evidence["pid"]
                && sentinel[i]["worker_id"] == node.evidence["worker_id"]
        );
    }
    jobs.extend(sentinel);
    account.quiesce().await;
    sample_task.abort();
    let account_report = account.report();
    for node in &mut nodes {
        node.evidence["sentinel_cache_reused"] = json!(
            !account_report["reads"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["phase"] == "sentinel" && r["node"] == node.evidence["node"])
        );
        let log =
            std::fs::read_to_string(output.join(format!("node-{}.log", node.evidence["node"])))?;
        node.evidence["error_witnesses"] = json!(
            log.lines()
                .filter(|l| l.contains("os error 28")
                    || l.contains("Retries exhausted")
                    || l.contains("Tried to send while stream is closed")
                    || l.contains("receiver disconnected"))
                .collect::<Vec<_>>()
        );
        node.evidence["worker_enospc_observed"] = json!(log.contains("os error 28"));
    }
    let valid = jobs.iter().all(|j| {
        j["outcome"] == "success"
            && j["attempts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|a| a["outcome"] != "censored")
    }) && nodes.iter().all(|n| {
        n.evidence["sentinel_same_process"] == true && n.evidence["sentinel_cache_reused"] == true
    });
    let report = json!({
        "schema_version": 1,
        "accepted": valid,
        "build": {
            "arm": option_env!("FLEET_VARIANT"),
            "revision": option_env!("FLEET_BASE_REVISION"),
            "azure_source_sha256": option_env!("FLEET_AZURE_SOURCE_SHA256")
        },
        "arm": args.arm, "scenario": args.scenario, "seed": args.seed,
        "pair_order": args.pair_order, "binary_sha256": binary_sha256,
        "workload_sha256": workload_sha256, "start_unix_s": start_unix_s,
        "duration_s": account.elapsed(), "load": {"before": before, "after": load()},
        "fault": fault, "events": events, "jobs": jobs,
        "nodes": nodes.iter().map(|n| &n.evidence).collect::<Vec<_>>(),
        "account": account_report, "before_sentinel": before_sentinel,
        "scheduler_snapshots": *snapshots.lock().unwrap(), "cpu_affinity": affinity,
        "measurement": {
            "topology": "four separate LocalWorker processes, private mounts, real scheduler over gRPC, shared HTTP origin on one host",
            "retry": {
                "max_retries": 6, "delay": 0.3, "jitter": 0.5,
                "throttle_retry_delay_s": 2, "throttle_retry_max_delay_s": 60,
                "throttle_retry_budget_s": 0, "sdk_retry": "disabled"
            },
            "client_policy": "one resubmission after repair for failed work; every attempt retained",
            "started_s": "client submission, not command start"
        }
    });
    std::fs::write(output.join("run.json"), serde_json::to_vec_pretty(&report)?)?;
    for node in &mut nodes {
        node.stop().await;
    }
    http_task.abort();
    grpc_task.abort();
    announce(format_args!(
        "payload_bytes={} completed={} results={}",
        report["account"]["payload_bytes"],
        report["account"]["completed_actions"],
        output.join("run.json").display()
    ));
    if !valid {
        return Err(
            "run failed completion or same-worker cache-reuse acceptance; results retained".into(),
        );
    }
    Ok(())
}

#[expect(
    clippy::disallowed_methods,
    reason = "the standalone executable owns its Tokio runtime"
)]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    drop(tokio_rustls::rustls::crypto::ring::default_provider().install_default());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let args = Args::parse();
    if let Some(path) = args.node {
        node::run(&path).await
    } else {
        run(args).await
    }
}
