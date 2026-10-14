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

//! One seeded run: build the simulation, drive it step by step while the
//! injector applies the fault plan, then check the history.

use core::cell::RefCell;
use core::time::Duration;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex, Once};
use std::time::{Instant as WallInstant, SystemTime};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::checker::{Stats, Violation, check};
use crate::client::ClientHost;
use crate::config::{Scenario, SimConfig};
use crate::det;
use crate::faults::{FaultAction, FaultPlan};
use crate::history::{Event, History, Record, set_runner_elapsed};
use crate::scheduler::{SCHEDULER_HOST, SchedulerHost};
use crate::worker::{WorkerControl, WorkerHost};
use crate::workload::Workload;

/// Everything a run produced.
#[derive(Debug)]
pub struct RunOutput {
    pub seed: u64,
    pub config: SimConfig,
    pub records: Vec<Record>,
    pub violations: Vec<Violation>,
    pub stats: Stats,
    /// Simulated time the run covered.
    pub sim_time: Duration,
    /// Real time it took (not part of the history).
    pub wall_time: Duration,
    /// FNV-1a over every file the scheduler left in its raft directory (the
    /// committed log), in path order: two runs of one seed must agree on it.
    pub log_hash: u64,
}

impl RunOutput {
    /// The history as text: what two runs of one seed must agree on.
    pub fn rendered_history(&self) -> String {
        use core::fmt::Write as _;
        let mut out = String::new();
        for record in &self.records {
            let _ = writeln!(out, "{record}");
        }
        out
    }
}

thread_local! {
    static PANIC_HISTORY: RefCell<Option<History>> = const { RefCell::new(None) };
}

fn install_panic_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let recorded = PANIC_HISTORY
                .try_with(|history| {
                    history.borrow().as_ref().is_some_and(|history| {
                        let location = info
                            .location()
                            .map(|l| format!("{}:{}", l.file(), l.line()))
                            .unwrap_or_default();
                        let message = info
                            .payload()
                            .downcast_ref::<&str>()
                            .map(ToString::to_string)
                            .or_else(|| info.payload().downcast_ref::<String>().cloned())
                            .unwrap_or_default();
                        history.record(
                            "panic",
                            Event::Panic(format!("{location}: {message}{}", backtrace_summary())),
                        );
                        true
                    })
                })
                .unwrap_or(false);
            if !recorded || std::env::var_os("SIM_PANIC_TRACE").is_some() {
                previous(info);
            }
        }));
    });
}

/// The `NativeLink` frames of the current backtrace, innermost first, so a
/// panic report says which task it killed.
fn backtrace_summary() -> String {
    let backtrace = std::backtrace::Backtrace::force_capture().to_string();
    let frames: Vec<String> = backtrace
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("at ") && line.contains("nativelink-"))
        .filter(|line| !line.contains("nativelink-sim/src/runner.rs"))
        .map(|line| {
            let path = line.trim_start_matches("at ");
            path.find("nativelink-")
                .map_or(path, |start| &path[start..])
                .to_string()
        })
        .take(6)
        .collect();
    if frames.is_empty() {
        String::new()
    } else {
        format!(" [at {}]", frames.join(" <- "))
    }
}

/// Where a run keeps the scheduler's raft directory.
pub fn default_data_root() -> PathBuf {
    std::env::var_os("SIM_DATA_DIR").map_or_else(
        || {
            // tmpfs when there is one: every commit fsyncs, and many runs
            // in parallel would otherwise queue on the disk.
            let shm = Path::new("/dev/shm");
            if shm.is_dir() {
                shm.join("nativelink-sim")
            } else {
                std::env::temp_dir().join("nativelink-sim")
            }
        },
        PathBuf::from,
    )
}

/// Run `seed` on a fresh thread (so its clocks, entropy and thread-locals
/// start clean) and check the history.
pub fn run(seed: u64, config: SimConfig, data_root: &Path) -> RunOutput {
    let data_dir = data_root.join(format!(
        "{}-{}-{seed}-{}",
        config.mode,
        config.scenario,
        std::process::id()
    ));
    let thread_config = config;
    let thread_dir = data_dir.clone();
    #[expect(
        clippy::disallowed_methods,
        reason = "the simulation needs a thread of its own, not a tokio task: turmoil runs every host's runtime on it"
    )]
    let output = std::thread::Builder::new()
        .name(format!("sim-{seed}"))
        .stack_size(256 * 1024 * 1024)
        .spawn(move || run_on_this_thread(seed, thread_config, &thread_dir))
        .expect("spawning the simulation thread")
        .join();
    let log_hash = hash_dir(&data_dir);
    drop(std::fs::remove_dir_all(&data_dir));
    match output {
        Ok(mut output) => {
            output.log_hash = log_hash;
            output
        }
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            let records = vec![Record {
                seq: 0,
                at: Duration::ZERO,
                host: "sim".to_string(),
                event: Event::Panic(format!("harness: simulation thread panicked: {message}")),
            }];
            let (violations, stats) = check(&config, &records);
            RunOutput {
                seed,
                config,
                records,
                violations,
                stats,
                sim_time: Duration::ZERO,
                wall_time: Duration::ZERO,
                log_hash,
            }
        }
    }
}

/// FNV-1a over the relative paths and contents of every file under `dir`.
fn hash_dir(dir: &Path) -> u64 {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(dir, &mut files);
    files.sort();
    let mut text = Vec::new();
    for file in files {
        text.extend_from_slice(
            file.strip_prefix(dir)
                .unwrap_or(&file)
                .display()
                .to_string()
                .as_bytes(),
        );
        text.push(0);
        text.extend_from_slice(&std::fs::read(&file).unwrap_or_default());
        text.push(0);
    }
    crate::history_hash_bytes(&text)
}

fn run_on_this_thread(seed: u64, config: SimConfig, data_dir: &Path) -> RunOutput {
    let wall_start = WallInstant::now();
    drop(std::fs::remove_dir_all(data_dir));
    std::fs::create_dir_all(data_dir).expect("creating the raft directory");
    install_panic_hook();
    // The raft log must not live on tokio's blocking pool: see the doc of
    // `set_inline_io_for_simulation`.
    nativelink_scheduler::raft_awaited_action_db::store::set_inline_io_for_simulation(true);
    det::activate(seed, det::Hooks::from_env());
    let history = History::default();
    PANIC_HISTORY.with(|h| *h.borrow_mut() = Some(history.clone()));

    let mut rng = StdRng::seed_from_u64(seed);
    let workload = Arc::new(Workload::generate(&config, &mut rng));
    let plan = FaultPlan::generate(&config, &mut rng);

    let controls: Vec<Arc<Mutex<WorkerControl>>> = (0..config.workers)
        .map(|_| Arc::new(Mutex::new(WorkerControl::default())))
        .collect();
    let set_control = |w: usize, f: &dyn Fn(&mut WorkerControl)| {
        let mut control = controls[w]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut control);
    };
    match config.scenario {
        Scenario::SlowWorkers => set_control(0, &|c| c.slow_factor = 10),
        Scenario::Zombie => set_control(0, &|c| c.zombie = true),
        Scenario::Chaos => {
            if rng.random_bool(0.5) {
                set_control(0, &|c| c.zombie = true);
            }
            if rng.random_bool(0.3) {
                set_control(1 % config.workers, &|c| c.slow_factor = 10);
            }
        }
        _ => {}
    }
    for (w, control) in controls.iter().enumerate() {
        let control = *control.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if control.zombie || control.slow_factor > 1 {
            history.record_at(
                Duration::ZERO,
                &format!("w{w}"),
                Event::Behavior(format!(
                    "zombie={} slow_factor={}",
                    control.zombie,
                    control.slow_factor.max(1)
                )),
            );
        }
    }

    let limit = config.active + config.quiet + Duration::from_secs(30);
    let mut sim = turmoil::Builder::new()
        .simulation_duration(limit + Duration::from_secs(3600))
        .tick_duration(config.tick)
        .epoch(SystemTime::UNIX_EPOCH + Duration::from_secs(det::SIM_EPOCH_SECS))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(30))
        .rng_seed(seed)
        .enable_random_order()
        .build();

    let scheduler = Arc::new(SchedulerHost {
        config,
        workload: workload.clone(),
        data_dir: data_dir.to_path_buf(),
        history: history.clone(),
        boots: AtomicU32::new(0),
    });
    sim.host(SCHEDULER_HOST, move || scheduler.clone().run());

    for (w, control) in controls.iter().enumerate() {
        let worker = Arc::new(WorkerHost {
            name: format!("w{w}"),
            slots: config.slots_per_worker,
            keepalive: config.worker_keepalive,
            workload: workload.clone(),
            history: history.clone(),
            control: control.clone(),
            sessions: AtomicU32::new(0),
        });
        sim.host(worker.name.clone(), move || worker.clone().run());
    }
    for (c, jobs) in workload.clients.iter().enumerate() {
        let client = Arc::new(ClientHost {
            name: format!("c{c}"),
            seed: seed.wrapping_mul(0x9e37_79b9).wrapping_add(c as u64),
            jobs: jobs.clone(),
            workload: workload.clone(),
            config,
            history: history.clone(),
        });
        let name = client.name.clone();
        sim.client(name, async move { client.run().await });
    }

    let mut elapsed = Duration::ZERO;
    let mut next_fault = 0;
    loop {
        while let Some((at, action)) = plan.events.get(next_fault) {
            if *at > elapsed {
                break;
            }
            history.record_at(elapsed, "sim", Event::Fault(action.to_string()));
            match action {
                FaultAction::CrashWorker(w) => sim.crash(format!("w{w}")),
                FaultAction::RestartWorker(w) => sim.bounce(format!("w{w}")),
                FaultAction::CrashScheduler => sim.crash(SCHEDULER_HOST),
                FaultAction::RestartScheduler => sim.bounce(SCHEDULER_HOST),
                FaultAction::Hold(host) => sim.hold(SCHEDULER_HOST, host.as_str()),
                FaultAction::Release(host) => sim.release(SCHEDULER_HOST, host.as_str()),
                FaultAction::Cut(host) => sim.partition(SCHEDULER_HOST, host.as_str()),
                FaultAction::Repair(host) => sim.repair(SCHEDULER_HOST, host.as_str()),
                FaultAction::ClockOffset(ms) => det::set_realtime_offset_ms(*ms),
                FaultAction::Stall(w, stalled) => set_control(*w, &|c| c.stalled = *stalled),
            }
            next_fault += 1;
        }
        match sim.step() {
            Ok(true) => break,
            Ok(false) => {}
            Err(err) => {
                history.record_at(
                    elapsed,
                    "sim",
                    Event::Panic(format!("harness: simulation step failed: {err}")),
                );
                break;
            }
        }
        elapsed += config.tick;
        set_runner_elapsed(elapsed);
        if elapsed > limit {
            history.record_at(
                elapsed,
                "sim",
                Event::Panic("harness: clients did not finish by the time limit".to_string()),
            );
            break;
        }
    }
    history.record_at(elapsed, "sim", Event::Fault("end of run".to_string()));
    let records = history.snapshot();
    // Tear the hosts down while the clocks are still simulated.
    drop(sim);
    PANIC_HISTORY.with(|h| *h.borrow_mut() = None);
    det::deactivate();

    let (violations, stats) = check(&config, &records);
    RunOutput {
        seed,
        config,
        records,
        violations,
        stats,
        sim_time: elapsed,
        wall_time: wall_start.elapsed(),
        log_hash: 0,
    }
}
