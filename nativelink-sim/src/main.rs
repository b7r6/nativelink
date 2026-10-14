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

//! `nativelink-sim`: run one seed, check a seed replays identically, or sweep
//! many seeds in parallel processes. See `README.md`.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};
use std::sync::{Arc, Mutex};

use nativelink_sim::checker::minimal_trace;
use nativelink_sim::config::{Mode, Scenario, SimConfig};
use nativelink_sim::{history_hash, normalize_uuids};
use nativelink_sim::runner::{default_data_root, run};

const USAGE: &str = "\
usage:
  nativelink-sim run --seed N [--mode normal|absurd] [--scenario NAME] [--history FILE] [--quiet]
  nativelink-sim determinism --seed N [--mode M] [--scenario S] [--normalize]
  nativelink-sim sweep [--seeds A..B] [--mode M|all] [--scenario S|all] [--jobs J]

scenarios: calm long-detach slow-workers clock-skew worker-crash scheduler-restart
           partition blackhole worker-stall zombie chaos
env: SIM_SEEDS=A..B (sweep default), SIM_JOBS, SIM_DATA_DIR, SIM_LOG=<tracing filter>,
     SIM_DET_OFF=realtime,monotonic,clock,hashseed,ids,entropy,all (ablation: leave those unhooked)";

#[derive(Debug, Default)]
struct Args {
    command: String,
    flags: BTreeMap<String, String>,
}

fn parse_args() -> Result<Args, String> {
    let mut iter = std::env::args().skip(1);
    let command = iter.next().ok_or("missing command")?;
    let mut flags = BTreeMap::new();
    while let Some(flag) = iter.next() {
        let key = flag
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected argument {flag:?}"))?
            .to_string();
        let value = if key == "quiet" || key == "normalize" {
            "true".to_string()
        } else {
            iter.next().ok_or_else(|| format!("--{key} needs a value"))?
        };
        flags.insert(key, value);
    }
    Ok(Args { command, flags })
}

fn parse_range(text: &str) -> Result<Vec<u64>, String> {
    if let Some((from, to)) = text.split_once("..") {
        let from: u64 = from.parse().map_err(|e| format!("bad seed range {text:?}: {e}"))?;
        let to: u64 = to
            .trim_start_matches('=')
            .parse()
            .map_err(|e| format!("bad seed range {text:?}: {e}"))?;
        Ok((from..=to).collect())
    } else {
        text.split(',')
            .map(|s| s.parse().map_err(|e| format!("bad seed {s:?}: {e}")))
            .collect()
    }
}

fn modes(text: &str) -> Result<Vec<Mode>, String> {
    if text == "all" {
        return Ok(vec![Mode::Normal, Mode::Absurd]);
    }
    text.split(',').map(str::parse).collect()
}

fn scenarios(text: &str) -> Result<Vec<Scenario>, String> {
    if text == "all" {
        return Ok(Scenario::ALL.to_vec());
    }
    text.split(',').map(str::parse).collect()
}

fn init_tracing() {
    if let Ok(filter) = std::env::var("SIM_LOG") {
        drop(
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
                .with_writer(std::io::stderr)
                .try_init(),
        );
    }
}

fn cmd_run(args: &Args) -> Result<ExitCode, String> {
    let seed: u64 = args
        .flags
        .get("seed")
        .ok_or("--seed is required")?
        .parse()
        .map_err(|e| format!("bad --seed: {e}"))?;
    let mode: Mode = args.flags.get("mode").map_or(Ok(Mode::Normal), |m| m.parse())?;
    let scenario: Scenario = args
        .flags
        .get("scenario")
        .map_or(Ok(Scenario::Calm), |s| s.parse())?;
    let quiet = args.flags.contains_key("quiet");
    let output = run(seed, SimConfig::new(mode, scenario), &default_data_root());
    let history = output.rendered_history();
    if let Some(path) = args.flags.get("history") {
        std::fs::write(path, &history).map_err(|e| format!("writing {path}: {e}"))?;
    }
    let mut out = std::io::stdout().lock();
    let mut kinds: BTreeMap<&str, usize> = BTreeMap::new();
    for violation in &output.violations {
        *kinds.entry(violation.kind.as_str()).or_default() += 1;
    }
    if !quiet {
        for violation in &output.violations {
            drop(writeln!(out, "VIOLATION seed={seed} mode={mode} scenario={scenario} {violation}"));
            for record in minimal_trace(violation, &output.records, 40) {
                drop(writeln!(out, "    {record}"));
            }
        }
    }
    let kinds_text = kinds
        .iter()
        .map(|(kind, count)| format!("{kind}:{count}"))
        .collect::<Vec<_>>()
        .join(",");
    drop(writeln!(
        out,
        "SUMMARY seed={seed} mode={mode} scenario={scenario} hash={:016x} log_hash={:016x} events={} sim_s={:.1} wall_s={:.1} stats=[{}] violations={}",
        history_hash(&history),
        output.log_hash,
        output.records.len(),
        output.sim_time.as_secs_f64(),
        output.wall_time.as_secs_f64(),
        output.stats,
        if kinds_text.is_empty() { "none".to_string() } else { kinds_text },
    ));
    Ok(if output.violations.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn child(seed: u64, mode: Mode, scenario: Scenario, extra: &[&str]) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("own path"));
    command
        .arg("run")
        .arg("--seed")
        .arg(seed.to_string())
        .arg("--mode")
        .arg(mode.to_string())
        .arg("--scenario")
        .arg(scenario.to_string())
        .args(extra)
        .stdin(Stdio::null());
    command
}

fn cmd_determinism(args: &Args) -> Result<ExitCode, String> {
    let seed: u64 = args
        .flags
        .get("seed")
        .ok_or("--seed is required")?
        .parse()
        .map_err(|e| format!("bad --seed: {e}"))?;
    let mode: Mode = args.flags.get("mode").map_or(Ok(Mode::Normal), |m| m.parse())?;
    let scenario: Scenario = args
        .flags
        .get("scenario")
        .map_or(Ok(Scenario::Chaos), |s| s.parse())?;
    let normalize = args.flags.contains_key("normalize");
    let dir = default_data_root().join(format!("determinism-{}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    let mut histories = Vec::new();
    let mut log_hashes = Vec::new();
    for round in 0..2 {
        let path: PathBuf = dir.join(format!("history-{round}.txt"));
        let output = child(seed, mode, scenario, &["--quiet", "--history", &path.display().to_string()])
            .stderr(Stdio::null())
            .output()
            .map_err(|e| format!("running child: {e}"))?;
        if output.status.code().is_none_or(|code| code > 1) {
            return Err(format!("child run {round} failed: {}", output.status));
        }
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        log_hashes.push(
            stdout
                .split_whitespace()
                .find_map(|field| field.strip_prefix("log_hash="))
                .unwrap_or("?")
                .to_string(),
        );
        let history = std::fs::read_to_string(&path).map_err(|e| format!("reading {}: {e}", path.display()))?;
        histories.push(if normalize { normalize_uuids(&history) } else { history });
    }
    drop(std::fs::remove_dir_all(&dir));
    let mut out = std::io::stdout().lock();
    let what = if normalize { "normalized history" } else { "history" };
    if histories[0] == histories[1] && (normalize || log_hashes[0] == log_hashes[1]) {
        drop(writeln!(
            out,
            "DETERMINISTIC seed={seed} mode={mode} scenario={scenario} {what}: events={} hash={:016x}; raft log hash={} (both runs)",
            histories[0].lines().count(),
            history_hash(&histories[0]),
            log_hashes[0],
        ));
        return Ok(ExitCode::SUCCESS);
    }
    if histories[0] == histories[1] {
        drop(writeln!(
            out,
            "NONDETERMINISTIC seed={seed} mode={mode} scenario={scenario}: identical {what}, different raft logs ({} vs {})",
            log_hashes[0], log_hashes[1]
        ));
        return Ok(ExitCode::from(1));
    }
    let first_diff = histories[0]
        .lines()
        .zip(histories[1].lines())
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| histories[0].lines().count().min(histories[1].lines().count()));
    drop(writeln!(
        out,
        "NONDETERMINISTIC seed={seed} mode={mode} scenario={scenario} {what} differs first at line {first_diff} (of {} / {}):\n  run 1: {}\n  run 2: {}",
        histories[0].lines().count(),
        histories[1].lines().count(),
        histories[0].lines().nth(first_diff).unwrap_or("<end>"),
        histories[1].lines().nth(first_diff).unwrap_or("<end>")
    ));
    Ok(ExitCode::from(1))
}

#[derive(Debug, Default)]
struct Tally {
    runs: usize,
    failing_runs: usize,
    /// kind -> (runs showing it, first seed, first violation line)
    kinds: BTreeMap<String, (usize, u64, String)>,
}

fn cmd_sweep(args: &Args) -> Result<ExitCode, String> {
    let seeds_text = args
        .flags
        .get("seeds")
        .cloned()
        .or_else(|| std::env::var("SIM_SEEDS").ok())
        .unwrap_or_else(|| "1..20".to_string());
    let seeds = parse_range(&seeds_text)?;
    let modes = modes(args.flags.get("mode").map_or("all", String::as_str))?;
    let scenarios = scenarios(args.flags.get("scenario").map_or("all", String::as_str))?;
    let jobs: usize = args
        .flags
        .get("jobs")
        .cloned()
        .or_else(|| std::env::var("SIM_JOBS").ok())
        .unwrap_or_else(|| "4".to_string())
        .parse()
        .map_err(|e| format!("bad --jobs: {e}"))?;
    let mut work = Vec::new();
    for mode in &modes {
        for scenario in &scenarios {
            for seed in &seeds {
                work.push((*mode, *scenario, *seed));
            }
        }
    }
    let total = work.len();
    let queue = Arc::new(Mutex::new(work.into_iter()));
    let tallies: Arc<Mutex<BTreeMap<(Mode, Scenario), Tally>>> = Arc::default();
    let done = Arc::new(Mutex::new(0usize));
    let workers: Vec<_> = (0..jobs.max(1))
        .map(|_| {
            let queue = queue.clone();
            let tallies = tallies.clone();
            let done = done.clone();
            #[expect(
                clippy::disallowed_methods,
                reason = "a plain thread that waits on a child process; no runtime here"
            )]
            std::thread::spawn(move || {
                loop {
                    let next = queue.lock().unwrap_or_else(std::sync::PoisonError::into_inner).next();
                    let Some((mode, scenario, seed)) = next else {
                        return;
                    };
                    let output = child(seed, mode, scenario, &[]).stderr(Stdio::null()).output();
                    let text = output
                        .as_ref()
                        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
                        .unwrap_or_default();
                    let status = output.as_ref().map(|o| o.status.code());
                    let mut tallies = tallies.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    let tally = tallies.entry((mode, scenario)).or_default();
                    tally.runs += 1;
                    let mut kinds_seen: BTreeMap<String, String> = BTreeMap::new();
                    for line in text.lines().filter(|l| l.starts_with("VIOLATION")) {
                        let kind = line
                            .split_whitespace()
                            .nth(4)
                            .unwrap_or("?")
                            .trim_end_matches(':')
                            .to_string();
                        kinds_seen.entry(kind).or_insert_with(|| line.to_string());
                    }
                    if !text.contains("SUMMARY") {
                        kinds_seen.insert(
                            "process-died".to_string(),
                            format!("VIOLATION seed={seed} mode={mode} scenario={scenario} process-died: exit {status:?}"),
                        );
                    }
                    if !kinds_seen.is_empty() {
                        tally.failing_runs += 1;
                    }
                    for (kind, line) in kinds_seen {
                        let entry = tally.kinds.entry(kind).or_insert_with(|| (0, seed, line.clone()));
                        entry.0 += 1;
                        if seed < entry.1 {
                            entry.1 = seed;
                            entry.2 = line;
                        }
                    }
                    drop(tallies);
                    let mut done = done.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    *done += 1;
                    if done.is_multiple_of(10) || *done == total {
                        eprintln!("[sweep] {}/{total}", *done);
                    }
                }
            })
        })
        .collect();
    for worker in workers {
        drop(worker.join());
    }
    let tallies = tallies.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut out = std::io::stdout().lock();
    let mut any = false;
    for ((mode, scenario), tally) in tallies.iter() {
        drop(writeln!(
            out,
            "== mode={mode} scenario={scenario} runs={} failing={}",
            tally.runs, tally.failing_runs
        ));
        for (kind, (count, seed, line)) in &tally.kinds {
            any = true;
            drop(writeln!(out, "   {kind:<24} runs={count:<5} first_seed={seed}"));
            drop(writeln!(out, "      {line}"));
        }
    }
    Ok(if any { ExitCode::from(1) } else { ExitCode::SUCCESS })
}

fn main() -> ExitCode {
    init_tracing();
    let result = parse_args().and_then(|args| match args.command.as_str() {
        "run" => cmd_run(&args),
        "determinism" => cmd_determinism(&args),
        "sweep" => cmd_sweep(&args),
        other => Err(format!("unknown command {other:?}")),
    });
    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}
