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

//! Bounded, reproducible refinement campaigns. These simulate committed inputs
//! and effect delivery, not a Raft implementation or actual worker processes.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;

use nativelink_scheduler::authority::*;
use serde_json::{Value, json};

use super::harness::{Harness, Input, Scenario, Schedule, shrink_inputs, shrink_scenario};
use super::reference::Reference;

struct Random(u64);

impl Random {
    const fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(2_685_821_657_736_338_717)
    }

    fn pick<'a, T>(&mut self, rows: &'a [T]) -> Option<&'a T> {
        if rows.is_empty() {
            None
        } else {
            Some(&rows[usize::try_from(self.next() % rows.len() as u64).unwrap()])
        }
    }
}

struct Recording {
    harness: Harness,
    scenario: Scenario,
    last_allocator: Option<u64>,
    last_clock: Option<u64>,
    last_hold_state: Option<u64>,
    attempt_states: BTreeMap<AttemptId, u64>,
    attempt_generations: BTreeMap<(AttemptId, u64), u64>,
    execution_deadlines: BTreeMap<AttemptId, u64>,
    interest_generations: BTreeMap<(OperationId, u64), u64>,
}

impl Recording {
    fn new(name: &str, seed: u64, limits: Limits) -> Self {
        Self {
            harness: Harness::new(ClusterEpoch(1), limits),
            scenario: Scenario {
                version: 1,
                name: name.to_owned(),
                seed,
                epoch: ClusterEpoch(1),
                limits,
                inputs: Vec::new(),
                schedule: Vec::new(),
            },
            last_allocator: None,
            last_clock: None,
            last_hold_state: None,
            attempt_states: BTreeMap::new(),
            attempt_generations: BTreeMap::new(),
            execution_deadlines: BTreeMap::new(),
            interest_generations: BTreeMap::new(),
        }
    }

    fn schedule(&mut self, step: Schedule) -> Result<Option<Reply>, String> {
        self.scenario.schedule.push(step);
        match step {
            Schedule::Propose(id) => {
                let event = self
                    .scenario
                    .inputs
                    .iter()
                    .find(|input| input.id == id)
                    .ok_or("recording input absent")?
                    .event
                    .clone();
                self.harness.propose(id, event)?;
            }
            Schedule::Commit(id) => self.harness.commit(id)?,
            Schedule::Apply => return self.harness.apply_next().map(Some),
            Schedule::Deliver(id) => {
                self.harness.deliver(id)?;
            }
            Schedule::Checkpoint => self.harness.checkpoint()?,
            Schedule::Crash => self.harness.crash_restore()?,
        }
        Ok(None)
    }

    fn input(&mut self, event: Event, adversary: u64) -> Result<Option<Reply>, String> {
        let before = self.harness.view();
        let id = self.scenario.inputs.len() as u64 + 1;
        let mut parents = BTreeSet::new();
        if let Some(parent) = self.last_allocator {
            parents.insert(parent);
        }
        if let Some(parent) = self.last_hold_state {
            parents.insert(parent);
        }
        let attempted = match &event {
            Event::WorkerAccepted { attempt, .. }
            | Event::Progress { attempt, .. }
            | Event::Complete { attempt, .. }
            | Event::MemoryFailure { attempt, .. }
            | Event::Decline { attempt, .. }
            | Event::ExpireAttempt { attempt, .. }
            | Event::ExpireExecution { attempt }
            | Event::Cleanup { attempt, .. } => Some(*attempt),
            _ => None,
        };
        if let Some(attempt) = attempted
            && let Some(parent) = self.attempt_states.get(&attempt)
        {
            // Keep the successful phase transition that enabled this input,
            // even when it allocated no new identity or effect.
            parents.insert(*parent);
        }
        match event {
            Event::ExpireAttempt {
                attempt,
                generation,
            } => {
                if let Some(parent) = self.attempt_generations.get(&(attempt, generation)) {
                    parents.insert(*parent);
                }
                if let Some(parent) = self.last_clock {
                    parents.insert(parent);
                }
            }
            Event::ExpireInterest {
                operation,
                generation,
            } => {
                if let Some(parent) = self.interest_generations.get(&(operation, generation)) {
                    parents.insert(*parent);
                }
                if let Some(parent) = self.last_clock {
                    parents.insert(parent);
                }
            }
            Event::ExpireExecution { attempt } => {
                if let Some(parent) = self.execution_deadlines.get(&attempt) {
                    parents.insert(*parent);
                }
                if let Some(parent) = self.last_clock {
                    parents.insert(parent);
                }
            }
            Event::RetireExecution { .. } | Event::Match => {
                if let Some(parent) = self.last_clock {
                    parents.insert(parent);
                }
            }
            _ => {}
        }
        self.scenario.inputs.push(Input {
            id,
            parents: parents.into_iter().collect(),
            event: RecordedEvent {
                epoch: self.scenario.epoch,
                event,
            },
        });
        self.schedule(Schedule::Propose(id))?;
        // Explicitly lose an uncommitted proposal, or crash after commit but
        // before apply. Neither path invents a successful proposal reply.
        if adversary % 37 == 1 {
            self.schedule(Schedule::Crash)?;
            return Ok(None);
        }
        self.schedule(Schedule::Commit(id))?;
        let reply = if adversary % 29 == 1 {
            self.schedule(Schedule::Crash)?
        } else {
            self.schedule(Schedule::Apply)?
        };
        let after = self.harness.view();
        if after
            .operations
            .iter()
            .any(|row| !before.operations.iter().any(|old| old.id == row.id))
            || after
                .executions
                .iter()
                .any(|row| !before.executions.iter().any(|old| old.id == row.id))
            || after
                .attempts
                .iter()
                .any(|row| !before.attempts.iter().any(|old| old.id == row.id))
            || after
                .effects
                .iter()
                .any(|row| !before.effects.iter().any(|old| old.id == row.id))
            || after.workers != before.workers
        {
            // Allocation ancestry is conservative: retaining a later producer
            // retains earlier ID producers, so shrinking never renumbers the
            // attempt/effect named by a surviving event.
            self.last_allocator = Some(id);
        }
        if after.now != before.now {
            self.last_clock = Some(id);
        }
        if after.holds != before.holds
            || after.executions.iter().any(|row| {
                before
                    .executions
                    .iter()
                    .find(|old| old.id == row.id)
                    .is_some_and(|old| {
                        old.hold_cooldown_until != row.hold_cooldown_until
                            || old.reservation_target != row.reservation_target
                    })
            })
        {
            // Holds and resource targets change later placement without
            // allocating an attempt or effect. Keep their producing events.
            self.last_hold_state = Some(id);
        }
        for trial in &after.attempts {
            if before.attempts.iter().find(|old| old.id == trial.id) != Some(trial) {
                self.attempt_states.insert(trial.id, id);
            }
            self.attempt_generations
                .entry((trial.id, trial.deadline.generation))
                .or_insert(id);
            if trial.running_deadline.is_some() {
                self.execution_deadlines.entry(trial.id).or_insert(id);
            }
        }
        for operation in &after.operations {
            self.interest_generations
                .entry((operation.id, operation.interest_deadline.generation))
                .or_insert(id);
        }
        Ok(reply)
    }
}

fn revision() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[allow(clippy::print_stdout)] // Acceptance evidence is consumed from test output.
fn evidence(name: &str, value: &Value) {
    println!("{}", serde_json::to_string(value).unwrap());
    if let Some(directory) = std::env::var_os("NL_AUTHORITY_EVIDENCE_DIR") {
        let directory = PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join(format!("{name}.json")),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .unwrap();
    }
}

fn committed_replay(scenario: &Scenario, inputs: &[Input]) -> Result<(), String> {
    let mut harness = Harness::new(scenario.epoch, scenario.limits);
    for input in inputs {
        harness.propose(input.id, input.event.clone())?;
        harness.commit(input.id)?;
        harness.apply_next()?;
    }
    Ok(())
}

fn failure_class(error: &str) -> &str {
    if error.contains("REFINEMENT_REPLY") {
        "REFINEMENT_REPLY"
    } else if error.contains("REFINEMENT_STATE") {
        "REFINEMENT_STATE"
    } else if error.contains("CORE_INVARIANT") {
        "CORE_INVARIANT"
    } else {
        "HARNESS"
    }
}

fn fail(recording: &Recording, error: &str) -> ! {
    let committed: BTreeSet<_> = recording
        .scenario
        .schedule
        .iter()
        .filter_map(|step| match step {
            Schedule::Commit(id) => Some(*id),
            _ => None,
        })
        .collect();
    let inputs: Vec<_> = recording
        .scenario
        .inputs
        .iter()
        .filter(|input| committed.contains(&input.id))
        .cloned()
        .collect();
    let same_failure = |candidate: &[Input]| {
        committed_replay(&recording.scenario, candidate)
            .is_err_and(|candidate_error| failure_class(&candidate_error) == failure_class(error))
    };
    let minimized = same_failure(&inputs).then(|| shrink_inputs(&inputs, same_failure));
    let minimized_schedule = (failure_class(error) != "HARNESS").then(|| {
        shrink_scenario(&recording.scenario, |candidate| {
            candidate.replay().is_err_and(|candidate_error| {
                failure_class(&candidate_error) == failure_class(error)
            })
        })
    });
    let sources: BTreeMap<_, _> = [
        (
            "src/authority/mod.rs",
            include_str!("../../src/authority/mod.rs"),
        ),
        (
            "src/authority/types.rs",
            include_str!("../../src/authority/types.rs"),
        ),
        (
            "src/authority/reapi.rs",
            include_str!("../../src/authority/reapi.rs"),
        ),
        (
            "src/authority/request.rs",
            include_str!("../../src/authority/request.rs"),
        ),
        (
            "tests/authority_test.rs",
            include_str!("../authority_test.rs"),
        ),
        ("tests/authority/harness.rs", include_str!("harness.rs")),
        ("tests/authority/reference.rs", include_str!("reference.rs")),
        ("tests/authority/campaign.rs", include_str!("campaign.rs")),
    ]
    .into_iter()
    .collect();
    let bundle = json!({
        "format_version": 1, "repository_revision": revision(), "error": error,
        "source_files": sources,
        "cargo_lock": include_str!("../../../Cargo.lock"),
        "scenario": recording.scenario,
        "actual": recording.harness.core.view(), "expected": recording.harness.oracle.view(),
        "minimized_committed_inputs": minimized,
        "minimized_scenario": minimized_schedule,
        "shrinking_scope": "Greedy causal-input and fault/delivery deletion; oracle validates transport prerequisites and replay retains the original invariant class. Not a globally shortest trace.",
    });
    let directory = std::env::var_os("NL_AUTHORITY_EVIDENCE_DIR").map_or_else(
        || std::env::temp_dir().join("nativelink-authority-failures"),
        PathBuf::from,
    );
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(format!(
        "{}-seed-{}-failure.json",
        recording.scenario.name, recording.scenario.seed
    ));
    std::fs::write(&path, serde_json::to_vec_pretty(&bundle).unwrap()).unwrap();
    panic!("{error}\nreplayable failure bundle: {}", path.display());
}

const fn kind(event: &Event) -> &'static str {
    match event {
        Event::Register { .. } => "Register",
        Event::Capacity { .. } => "Capacity",
        Event::Heartbeat { .. } => "Heartbeat",
        Event::WorkerUnavailable { .. } => "WorkerUnavailable",
        Event::Submit { .. } => "Submit",
        Event::LookupResult { .. } => "LookupResult",
        Event::Match => "Match",
        Event::WorkerAccepted { .. } => "WorkerAccepted",
        Event::Progress { .. } => "Progress",
        Event::Complete { .. } => "Complete",
        Event::MemoryFailure { .. } => "MemoryFailure",
        Event::Decline { .. } => "Decline",
        Event::Cancel { .. } => "Cancel",
        Event::Observe { .. } => "Observe",
        Event::RenewInterest { .. } => "RenewInterest",
        Event::AdvanceTime { .. } => "AdvanceTime",
        Event::ExpireAttempt { .. } => "ExpireAttempt",
        Event::ExpireExecution { .. } => "ExpireExecution",
        Event::ExpireInterest { .. } => "ExpireInterest",
        Event::Cleanup { .. } => "Cleanup",
        Event::EffectOutcome { .. } => "EffectOutcome",
        Event::RetireExecution { .. } => "RetireExecution",
    }
}

fn generated_event(
    random: &mut Random,
    step: usize,
    view: &ObservableState,
    next_operation: &mut u64,
) -> Event {
    // A declared seed prefix reaches every lifecycle stage before the
    // adversarial suffix. The suffix still independently chooses demand,
    // observation identity, time, process loss, and delivery ordering.
    if step < 11 {
        return match step {
            0 | 8 => {
                *next_operation += 1;
                let mut request = super::request(*next_operation);
                request.skip_cache_lookup = step == 0;
                Event::Submit {
                    operation: OperationId(*next_operation),
                    request,
                }
            }
            1 | 10 => Event::Match,
            2 => Event::WorkerAccepted {
                attempt: AttemptId(1),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
            3..=5 => Event::Progress {
                attempt: AttemptId(1),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                phase: [Phase::Preparing, Phase::Running, Phase::Uploading][step - 3],
                progress: step as u64,
            },
            6 => Event::Complete {
                attempt: AttemptId(1),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                result: super::result(1),
            },
            7 => Event::EffectOutcome {
                effect: EffectId(2),
                success: true,
            },
            9 => Event::LookupResult {
                effect: EffectId(3),
                result: None,
            },
            _ => unreachable!(),
        };
    }
    let chosen = random.pick(&view.attempts).copied();
    let active: Vec<_> = view
        .attempts
        .iter()
        .filter(|attempt| attempt.authorized)
        .copied()
        .collect();
    let trial = if random.next().is_multiple_of(4) {
        chosen
    } else {
        random.pick(&active).copied().or(chosen)
    };
    let attempt = trial.as_ref().map_or(AttemptId(99), |trial| trial.id);
    let worker = trial.as_ref().map_or(WorkerId(1), |trial| trial.worker);
    let incarnation = trial
        .as_ref()
        .map_or(IncarnationId(1), |trial| trial.incarnation);
    let node = random.pick(&view.workers);
    let operation = random
        .pick(&view.operations)
        .map_or(OperationId(99), |row| row.id);
    match step % 31 {
        0 | 1 | 2 | 23 | 25 => {
            *next_operation += 1;
            let mut request = if step % 31 == 25 {
                random
                    .pick(&view.executions)
                    .map_or_else(|| super::request(1), |row| row.request.clone())
            } else {
                super::request(random.next() % 8)
            };
            request.skip_cache_lookup = !random.next().is_multiple_of(3);
            request.do_not_cache = random.next().is_multiple_of(5);
            request.priority = [-5, 0, 5][(random.next() % 3) as usize];
            if random.next().is_multiple_of(7) {
                request.execution_scope = Some(format!("invocation-{}", random.next() % 3));
            }
            Event::Submit {
                operation: OperationId(*next_operation),
                request,
            }
        }
        3 | 4 | 26 => Event::Match,
        5 => Event::WorkerAccepted {
            attempt,
            worker,
            incarnation,
        },
        6 | 27 => {
            let old_phase = trial.as_ref().map_or(Phase::Accepted, |trial| trial.phase);
            let phase = if step % 31 == 27 {
                old_phase
            } else {
                match old_phase {
                    Phase::Preparing => Phase::Running,
                    Phase::Running => Phase::Uploading,
                    _ => Phase::Preparing,
                }
            };
            Event::Progress {
                attempt,
                worker,
                incarnation,
                phase,
                progress: trial
                    .as_ref()
                    .map_or(1, |trial| trial.progress + u64::from(step % 31 != 27)),
            }
        }
        7 | 24 => Event::Complete {
            attempt,
            worker,
            incarnation: if step % 31 == 24 {
                IncarnationId(incarnation.0 + 10)
            } else {
                incarnation
            },
            result: ResultPayload {
                response: None,
                digest: format!("result-{}", random.next() % 100),
                exit_code: i32::from(random.next().is_multiple_of(5)),
                error: None,
            },
        },
        8 => Event::Decline {
            attempt,
            worker,
            incarnation,
        },
        9 => Event::Cancel { operation },
        10 => Event::AdvanceTime {
            now: if random.next().is_multiple_of(5) {
                view.now.saturating_sub(5)
            } else {
                view.now.saturating_add(1 + random.next() % 50)
            },
        },
        11 => Event::ExpireAttempt {
            attempt,
            generation: trial.as_ref().map_or(1, |trial| {
                trial
                    .deadline
                    .generation
                    .saturating_sub(u64::from(random.next().is_multiple_of(3)))
            }),
        },
        12 => {
            let cleanup: Vec<_> = view
                .attempts
                .iter()
                .filter(|trial| !trial.authorized)
                .collect();
            if let Some(trial) = random.pick(&cleanup) {
                Event::Cleanup {
                    attempt: trial.id,
                    worker: trial.worker,
                    incarnation: trial.incarnation,
                }
            } else {
                Event::Cleanup {
                    attempt,
                    worker,
                    incarnation,
                }
            }
        }
        13 | 14 => node.map_or_else(
            || super::register(1, 1, 1, 2),
            |node| {
                super::register(
                    node.id.0,
                    node.incarnation.0 + u64::from(step % 31 == 14),
                    if step % 31 == 14 {
                        1
                    } else {
                        node.session.0 + 1
                    },
                    1 + random.next() % 3,
                )
            },
        ),
        15 => {
            let (worker, incarnation, session) = node
                .map_or((WorkerId(1), IncarnationId(1), SessionId(1)), |node| {
                    (node.id, node.incarnation, node.session)
                });
            Event::Capacity {
                worker,
                incarnation,
                session,
                capacity: Resources {
                    cpu_millis: 10_000,
                    memory_bytes: 10_000,
                    slots: random.next() % 4,
                },
                available: Resources {
                    cpu_millis: 10_000,
                    memory_bytes: 10_000,
                    slots: random.next() % 4,
                },
                draining: random.next().is_multiple_of(5),
            }
        }
        16 => node.map_or(
            Event::Heartbeat {
                worker,
                incarnation,
                session: SessionId(1),
            },
            |node| Event::Heartbeat {
                worker: node.id,
                incarnation: node.incarnation,
                session: SessionId(
                    node.session
                        .0
                        .saturating_sub(u64::from(random.next().is_multiple_of(4))),
                ),
            },
        ),
        17 => Event::Observe { operation },
        18 => Event::RenewInterest { operation },
        19 => Event::ExpireInterest {
            operation,
            generation: view
                .operations
                .iter()
                .find(|row| row.id == operation)
                .map_or(1, |row| {
                    row.interest_deadline
                        .generation
                        .saturating_sub(u64::from(random.next().is_multiple_of(3)))
                }),
        },
        20 => Event::LookupResult {
            effect: view
                .effects
                .iter()
                .find(|effect| matches!(effect.kind, EffectKind::Lookup { .. }))
                .map_or(EffectId(99), |effect| effect.id),
            result: (random.next().is_multiple_of(3)).then(|| super::result(99)),
        },
        21 => Event::EffectOutcome {
            effect: random
                .pick(&view.effects)
                .map_or(EffectId(99), |effect| effect.id),
            success: !random.next().is_multiple_of(3),
        },
        22 => Event::RetireExecution {
            execution: random
                .pick(&view.executions)
                .map_or(ExecutionId(99), |row| row.id),
        },
        28 => Event::ExpireExecution { attempt },
        29 => node.map_or(
            Event::WorkerUnavailable {
                worker,
                incarnation,
                session: SessionId(1),
            },
            |node| Event::WorkerUnavailable {
                worker: node.id,
                incarnation: node.incarnation,
                session: SessionId(
                    node.session
                        .0
                        .saturating_sub(u64::from(random.next().is_multiple_of(4))),
                ),
            },
        ),
        30 => Event::MemoryFailure {
            attempt,
            worker,
            incarnation,
            charged: view
                .reservations
                .iter()
                .find(|row| row.attempt == attempt)
                .map_or_else(Resources::default, |row| row.charged),
            peak_memory_bytes: Some(random.next() % 20),
            quiescent: !random.next().is_multiple_of(4),
            result: ResultPayload {
                exit_code: 137,
                ..super::result(137)
            },
        },
        _ => unreachable!(),
    }
}

#[test]
fn causal_reduction_keeps_acceptance_and_running_deadline_producers() {
    let mut recording = Recording::new("phase-dependency-regression", 0, Limits::default());
    let mut request = super::request(1);
    request.execution_timeout = 5;
    for event in [
        super::register(1, 1, 1, 1),
        Event::Submit {
            operation: OperationId(1),
            request,
        },
        Event::Match,
        Event::WorkerAccepted {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
        },
        Event::Observe {
            operation: OperationId(1),
        },
        Event::Progress {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            phase: Phase::Running,
            progress: 1,
        },
        Event::AdvanceTime { now: 5 },
        Event::ExpireExecution {
            attempt: AttemptId(1),
        },
    ] {
        recording.input(event, 0).unwrap();
    }
    assert!(!recording.harness.view().attempts[0].authorized);
    let reduced = shrink_inputs(&recording.scenario.inputs, |candidate| {
        candidate.iter().any(|input| input.id == 8)
    });
    assert_eq!(
        reduced.iter().map(|input| input.id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 6, 7, 8]
    );
    committed_replay(&recording.scenario, &reduced).unwrap();
}

#[test]
fn causal_reduction_keeps_hold_and_immutable_age_producers() {
    let mut recording = Recording::new("hold-dependency-regression", 0, Limits::default());
    recording.input(super::register(1, 1, 1, 4), 0).unwrap();
    for operation in 1..=3 {
        recording
            .input(
                Event::Submit {
                    operation: OperationId(operation),
                    request: super::request(operation),
                },
                0,
            )
            .unwrap();
        recording.input(Event::Match, 0).unwrap();
    }
    let mut large = super::request(4);
    large.resources.slots = 4;
    recording
        .input(
            Event::Submit {
                operation: OperationId(4),
                request: large,
            },
            0,
        )
        .unwrap();
    recording
        .input(Event::AdvanceTime { now: HOLD_AFTER }, 0)
        .unwrap();
    let age_input = recording.scenario.inputs.last().unwrap().id;
    assert_eq!(
        recording.input(Event::Match, 0).unwrap(),
        Some(Reply::Applied)
    );
    let hold_input = recording.scenario.inputs.last().unwrap().id;
    recording
        .input(
            Event::Observe {
                operation: OperationId(4),
            },
            0,
        )
        .unwrap();
    let observation = recording.scenario.inputs.last().unwrap().id;
    recording
        .input(
            Event::Submit {
                operation: OperationId(5),
                request: super::request(5),
            },
            0,
        )
        .unwrap();
    assert_eq!(
        recording.input(Event::Match, 0).unwrap(),
        Some(Reply::Ignored)
    );
    let blocked_match = recording.scenario.inputs.last().unwrap().id;
    let reduced = shrink_inputs(&recording.scenario.inputs, |candidate| {
        candidate.iter().any(|input| input.id == blocked_match)
    });
    for parent in [age_input, hold_input] {
        assert!(reduced.iter().any(|input| input.id == parent));
    }
    assert!(!reduced.iter().any(|input| input.id == observation));
    committed_replay(&recording.scenario, &reduced).unwrap();
}

#[test]
fn causal_reduction_keeps_memory_growth_that_produced_a_later_grant() {
    let mut recording = Recording::new("memory-target-dependency-regression", 0, Limits::default());
    let request = super::request(1);
    let charged = request.resources;
    for event in [
        super::register(1, 1, 1, 2),
        Event::Submit {
            operation: OperationId(1),
            request,
        },
        Event::Match,
        Event::MemoryFailure {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            charged,
            peak_memory_bytes: None,
            quiescent: true,
            result: ResultPayload {
                exit_code: 137,
                ..super::result(1)
            },
        },
        Event::Observe {
            operation: OperationId(1),
        },
        Event::Match,
    ] {
        recording.input(event, 0).unwrap();
    }
    assert_eq!(
        recording.harness.view().reservations[1]
            .charged
            .memory_bytes,
        2 * charged.memory_bytes
    );
    let reduced = shrink_inputs(&recording.scenario.inputs, |candidate| {
        candidate.iter().any(|input| input.id == 6)
    });
    assert_eq!(
        reduced.iter().map(|input| input.id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 6]
    );
    committed_replay(&recording.scenario, &reduced).unwrap();
}

#[test]
fn generated_histories_with_crashes_and_reordered_observations() {
    const SEEDS: u64 = 64;
    const STEPS: usize = 256;
    let mut coverage = BTreeMap::<String, u64>::new();
    let mut successful_commands = BTreeMap::<String, u64>::new();
    let mut replies = BTreeMap::<String, u64>::new();
    let mut phases = BTreeSet::new();
    let mut schedules = 0;
    for seed in 1..=SEEDS {
        let limits = Limits {
            max_executions: 24,
            max_operations: 96,
            max_workers: 3,
            max_effects: 192,
            ..Limits::default()
        };
        let mut recording = Recording::new("generated-authority", seed, limits);
        for event in [super::register(1, 1, 1, 2), super::register(2, 1, 1, 2)] {
            recording
                .input(event, 0)
                .unwrap_or_else(|error| fail(&recording, &error));
        }
        let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let mut next_operation = 0;
        for step in 0..STEPS {
            let view = recording.harness.view();
            let event = generated_event(&mut random, step, &view, &mut next_operation);
            let command = kind(&event);
            *coverage.entry(command.to_owned()).or_default() += 1;
            let adversary = if step < 11 { 0 } else { random.next() };
            let reply = recording
                .input(event, adversary)
                .unwrap_or_else(|error| fail(&recording, &error));
            if let Some(reply) = reply {
                if matches!(
                    reply,
                    Reply::Applied
                        | Reply::Submitted { .. }
                        | Reply::Matched { .. }
                        | Reply::Observed(_)
                ) {
                    *successful_commands.entry(command.to_owned()).or_default() += 1;
                }
                let name = match reply {
                    Reply::Applied => "Applied",
                    Reply::Ignored => "Ignored",
                    Reply::Rejected(_) => "Rejected",
                    Reply::Submitted { .. } => "Submitted",
                    Reply::Matched { .. } => "Matched",
                    Reply::Observed(_) => "Observed",
                };
                *replies.entry(name.to_owned()).or_default() += 1;
            }
            let view = recording.harness.view();
            for execution in &view.executions {
                phases.insert(format!("{:?}", execution.phase));
            }
            if step % 17 == 0 {
                recording
                    .schedule(Schedule::Checkpoint)
                    .unwrap_or_else(|error| fail(&recording, &error));
            }
            if step % 13 == 0 {
                recording
                    .schedule(Schedule::Crash)
                    .unwrap_or_else(|error| fail(&recording, &error));
            }
            if let Some(effect) = random.pick(&view.effects)
                && step.is_multiple_of(7)
            {
                recording
                    .schedule(Schedule::Deliver(effect.id))
                    .unwrap_or_else(|error| fail(&recording, &error));
                recording
                    .schedule(Schedule::Deliver(effect.id))
                    .unwrap_or_else(|error| fail(&recording, &error));
            }
        }
        let bytes = serde_json::to_vec(&recording.scenario).unwrap();
        let replay: Scenario = serde_json::from_slice(&bytes).unwrap();
        let replayed = replay
            .replay()
            .unwrap_or_else(|error| fail(&recording, &error));
        assert_eq!(
            replayed.core.snapshot().unwrap(),
            recording.harness.core.snapshot().unwrap()
        );
        assert_eq!(replayed.delivered, recording.harness.delivered);
        schedules += recording.scenario.schedule.len();
    }
    assert_eq!(
        coverage.len(),
        22,
        "every command variant must be generated"
    );
    for phase in [
        "Queued",
        "Dispatching",
        "Accepted",
        "Preparing",
        "Running",
        "Uploading",
        "Terminal",
        "Lookup",
    ] {
        assert!(phases.contains(phase), "campaign never entered {phase}");
    }
    evidence(
        "generated",
        &json!({ "campaign": "generated", "repository_revision": revision(),
        "seeds": SEEDS, "steps_per_seed": STEPS, "generated_inputs": SEEDS * STEPS as u64,
        "fixed_lifecycle_prefix_steps_per_seed": 11,
        "schedule_steps": schedules, "command_counts": coverage, "reply_counts": replies,
        "successful_command_counts": successful_commands,
        "phases": phases, "replayed": true,
        "scope": "Pure authority and explicit proposal/commit/apply/outbox simulation; no real Raft, RPC, or physical worker." }),
    );
}

#[test]
fn bounded_exhaustive_authority_state_space() {
    const DEPTH: usize = 5;
    let limits = Limits {
        max_executions: 2,
        max_operations: 2,
        max_workers: 1,
        max_effects: 16,
        ..Limits::default()
    };
    let mut urgent = super::request(1);
    urgent.priority = -1;
    let alphabet = vec![
        super::register(1, 1, 1, 1),
        Event::Submit {
            operation: OperationId(1),
            request: super::request(1),
        },
        Event::Submit {
            operation: OperationId(2),
            request: urgent,
        },
        Event::Match,
        Event::WorkerAccepted {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
        },
        Event::Complete {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
            result: super::result(1),
        },
        Event::Cancel {
            operation: OperationId(1),
        },
        Event::AdvanceTime { now: ACK_TIMEOUT },
        Event::ExpireAttempt {
            attempt: AttemptId(1),
            generation: 1,
        },
        Event::Cleanup {
            attempt: AttemptId(1),
            worker: WorkerId(1),
            incarnation: IncarnationId(1),
        },
        super::register(1, 2, 1, 1),
        Event::Observe {
            operation: OperationId(1),
        },
        Event::RenewInterest {
            operation: OperationId(1),
        },
        Event::EffectOutcome {
            effect: EffectId(1),
            success: true,
        },
    ];
    let initial = Harness::new(ClusterEpoch(1), limits);
    let initial_core = initial.core.snapshot().unwrap();
    let mut visited = BTreeSet::from([initial_core.clone()]);
    let mut queue = VecDeque::from([(
        initial_core,
        initial.oracle.snapshot().unwrap(),
        Vec::<Event>::new(),
    )]);
    let mut states_at_depth = vec![0_u64; DEPTH + 1];
    states_at_depth[0] = 1;
    let mut transitions = 0_u64;
    while let Some((core, oracle, path)) = queue.pop_front() {
        if path.len() == DEPTH {
            continue;
        }
        for event in &alphabet {
            let mut harness = Harness::new(ClusterEpoch(1), limits);
            harness.core = State::restore(&core).unwrap();
            harness.oracle = Reference::restore(&oracle).unwrap();
            let mut successor_path = path.clone();
            successor_path.push(event.clone());
            if let Err(error) = harness.event(event.clone()) {
                let mut recording = Recording::new("exhaustive-authority", 0, limits);
                for input in &successor_path {
                    if let Err(replay_error) = recording.input(input.clone(), 0) {
                        fail(&recording, &replay_error);
                    }
                }
                fail(&recording, &error);
            }
            transitions += 1;
            let snapshot = harness.core.snapshot().unwrap();
            if visited.insert(snapshot.clone()) {
                states_at_depth[successor_path.len()] += 1;
                queue.push_back((snapshot, harness.oracle.snapshot().unwrap(), successor_path));
            }
        }
    }
    assert!(
        visited.len() > 100,
        "unexpectedly trivial explored state space"
    );
    evidence(
        "exhaustive",
        &json!({ "campaign": "bounded-exhaustive", "depth": DEPTH,
        "alphabet_size": alphabet.len(), "states": visited.len(), "transitions": transitions,
        "states_first_reached_at_depth": states_at_depth, "limits": limits,
        "alphabet": alphabet, "visited_key": "exact complete production snapshot bytes",
        "reduction": "Only identical full states are merged at shortest BFS depth; no partial-order reduction or state-count cap.",
        "scope": "All words through depth five over the recorded 14-event alphabet; one worker, two incarnation labels, two operation labels, one action key. No general proof or replicated fault claim." }),
    );
}

#[test]
fn fair_drain_finishes_bounded_serviceable_work_and_clears_effects() {
    const JOBS: u64 = 16;
    // Five lifecycle observations, one dispatch and at most one publication
    // acknowledgment per job, plus registration/admission. No arrivals during drain.
    const STEP_BOUND: u64 = 2 + JOBS * 8;
    let mut harness = Harness::new(ClusterEpoch(1), Limits::default());
    super::apply(&mut harness, super::register(1, 1, 1, 1));
    super::apply(&mut harness, super::register(2, 1, 1, 1));
    for operation in 1..=JOBS {
        super::submit(&mut harness, operation, super::request(operation));
    }
    let mut steps = JOBS + 2;
    while harness
        .view()
        .operations
        .iter()
        .any(|operation| operation.terminal.is_none())
    {
        assert!(
            steps < STEP_BOUND,
            "fair service exceeded declared drain bound"
        );
        let attempt = super::assign(&mut harness);
        steps += 1;
        super::finish(&mut harness, &attempt, attempt.id.0);
        steps += 5;
        for effect in harness.view().effects {
            if matches!(effect.kind, EffectKind::PublishResult { .. }) {
                super::apply(
                    &mut harness,
                    Event::EffectOutcome {
                        effect: effect.id,
                        success: true,
                    },
                );
                steps += 1;
            }
        }
    }
    let view = harness.view();
    assert_eq!(view.operations.len(), usize::try_from(JOBS).unwrap());
    assert!(
        view.operations
            .iter()
            .all(|operation| matches!(operation.terminal, Some(TerminalResult::Completed(_))))
    );
    assert!(
        view.reservations
            .iter()
            .all(|reservation| reservation.released)
    );
    assert!(view.effects.is_empty());
    assert!(steps <= STEP_BOUND);
    evidence(
        "fair-drain",
        &json!({ "campaign": "fair-drain", "jobs": JOBS, "steps": steps,
        "step_bound": STEP_BOUND,
        "assumptions": "No new arrivals; fixed eligible workers; every selected dispatch receives acceptance, progress, a successful completion, and publication acknowledgment. This is a bounded scheduler progress witness, not GC or partition liveness." }),
    );
}
