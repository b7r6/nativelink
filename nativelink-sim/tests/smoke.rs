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

//! The fast smoke set. Every run is a child process of the `nativelink-sim`
//! binary, so each seed starts from clean process state (its clocks, entropy
//! and every process-global the scheduler keeps).

use std::process::Command;

fn sim() -> Command {
    Command::new(env!("CARGO_BIN_EXE_nativelink-sim"))
}

fn run(seed: u64, mode: &str, scenario: &str) -> (bool, String) {
    let output = sim()
        .args(["run", "--seed", &seed.to_string(), "--mode", mode, "--scenario", scenario])
        .output()
        .expect("running nativelink-sim");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        stdout.contains("SUMMARY"),
        "seed {seed} {mode} {scenario} did not finish: {stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    (output.status.success(), stdout)
}

/// Scenarios that kill or partition nothing pass every check, with normal
/// timeouts and with every timeout set to a day.
#[test]
fn calm_passes_in_both_modes() {
    for mode in ["normal", "absurd"] {
        for seed in 1..=2 {
            let (ok, stdout) = run(seed, mode, "calm");
            assert!(ok, "seed {seed} mode {mode}:\n{stdout}");
        }
    }
}

/// The same seed gives the same history, byte for byte.
#[test]
fn a_seed_replays_identically() {
    for (seed, scenario) in [(3, "chaos"), (4, "scheduler-restart")] {
        let output = sim()
            .args(["determinism", "--seed", &seed.to_string(), "--scenario", scenario])
            .output()
            .expect("running nativelink-sim");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("DETERMINISTIC"),
            "seed {seed} {scenario}:\n{stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// Faults run to the end and the checker reports on them; which ones fail
/// today is the baseline, recorded in the README, not asserted here.
#[test]
fn fault_scenarios_run_to_completion() {
    for scenario in ["worker-crash", "scheduler-restart", "partition"] {
        drop(run(5, "normal", scenario));
    }
}

/// Many seeds in parallel processes: `SIM_SEEDS=1..1000 SIM_JOBS=4 cargo
/// test -p nativelink-sim -- --ignored --nocapture`. Prints violations by
/// kind with the first seed of each.
#[test]
#[ignore = "long: sweeps SIM_SEEDS (default 1..50) over every mode and scenario"]
#[expect(clippy::print_stdout, reason = "the sweep's report is the point of the test")]
fn sweep() {
    let seeds = std::env::var("SIM_SEEDS").unwrap_or_else(|_| "1..50".to_string());
    let mode = std::env::var("SIM_MODE").unwrap_or_else(|_| "all".to_string());
    let scenario = std::env::var("SIM_SCENARIO").unwrap_or_else(|_| "all".to_string());
    let output = sim()
        .args(["sweep", "--seeds", &seeds, "--mode", &mode, "--scenario", &scenario])
        .output()
        .expect("running nativelink-sim");
    println!("{}", String::from_utf8_lossy(&output.stdout));
}
