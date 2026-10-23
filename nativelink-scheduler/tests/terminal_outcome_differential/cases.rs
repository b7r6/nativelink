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

use nativelink_scheduler::authority as a;

use super::adapter::{Outcome, Pair};
use super::{fixtures as f, types as r};

fn setup(algorithm: u8, memory_kb: u64, capacity: u64, seed: bool) -> Pair {
    let mut pair = Pair::new(algorithm, memory_kb, seed);
    assert_eq!(pair.event(f::register(capacity, 1)), Outcome::Applied);
    pair.admit(1, 0);
    pair
}

fn cleanup(pair: &mut Pair, attempt: u64) -> Outcome {
    let identity = pair.identity(attempt);
    pair.event(a::Event::Cleanup {
        attempt: identity.attempt,
        worker: identity.worker,
        incarnation: identity.incarnation,
    })
}

#[test]
fn exact_complete_joined_cancellation_late_writes_and_seeded_publication() {
    for algorithm in [1, 9] {
        let mut pair = setup(algorithm, 1, 16384, true);
        pair.admit(2, 7);
        assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
        let writes = pair.stage(100, 1);
        assert_eq!(
            pair.event(a::Event::Cancel {
                operation: a::OperationId(2)
            }),
            Outcome::Applied
        );
        let report = pair.report(10, 1, false, None);
        let mut graph = report.clone();
        graph.payload = f::response("graph-bearing refusal", true);
        assert_eq!(pair.step(r::Command::Report(graph)), Outcome::Rejected);
        let mut stale = report.clone();
        stale.identity.incarnation = a::IncarnationId(2);
        assert_eq!(pair.step(r::Command::Report(stale)), Outcome::Rejected);
        assert_eq!(
            pair.step(r::Command::Report(report.clone())),
            Outcome::Terminal(a::TerminalResult::Completed(report.payload.clone()))
        );
        assert_eq!(
            pair.event(a::Event::Cancel {
                operation: a::OperationId(1)
            }),
            Outcome::Ignored
        );
        assert_eq!(
            pair.event(a::Event::RenewInterest {
                operation: a::OperationId(1)
            }),
            Outcome::Ignored
        );
        assert_eq!(
            pair.step(r::Command::Report(report.clone())),
            Outcome::Duplicate
        );
        let mut conflict = report;
        conflict.payload = f::response("conflicting captured bytes", false);
        assert_eq!(pair.step(r::Command::Report(conflict)), Outcome::Rejected);
        assert_eq!(cleanup(&mut pair, 1), Outcome::Ignored);
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[0],
                success: true
            }),
            Outcome::Applied
        );
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[0],
                success: true
            }),
            Outcome::Ignored
        );
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[0],
                success: false
            }),
            Outcome::Rejected
        );
        assert_eq!(pair.step(r::Command::Abandon(100)), Outcome::Applied);
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[1],
                success: true
            }),
            Outcome::Applied
        );
        assert_eq!(
            pair.event(a::Event::AdvanceTime { now: 11 }),
            Outcome::Applied
        );
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[2],
                success: false
            }),
            Outcome::Applied
        );
        assert_eq!(
            pair.view().operations[1].terminal,
            Some(a::TerminalResult::Cancelled)
        );
    }
}

#[test]
fn memory_reasons_retry_evidence_exact_original_refunds_and_late_outcomes() {
    for algorithm in [1, 9] {
        for (memory, capacity, peak, attempts, reason) in [
            (
                0,
                16384,
                None,
                1,
                a::ResourceFailureReason::MissingGrowthEvidence,
            ),
            (
                1,
                1024,
                Some(99999),
                1,
                a::ResourceFailureReason::NoLargerEligibleWorker,
            ),
            (
                1,
                16384,
                None,
                3,
                a::ResourceFailureReason::AttemptBudgetExhausted,
            ),
            (
                0,
                16384,
                Some(1024),
                3,
                a::ResourceFailureReason::AttemptBudgetExhausted,
            ),
        ] {
            let mut pair = setup(algorithm, memory, capacity, false);
            let mut old_writes = vec![];
            for attempt in 1..=attempts {
                assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
                old_writes.extend(pair.stage(100 + attempt, attempt));
                let report = pair.report(10 + attempt, attempt, true, peak);
                let mut wrong_charge = report.clone();
                if let r::Classification::MemoryFailure {
                    original_charged, ..
                } = &mut wrong_charge.classification
                {
                    original_charged.memory_bytes += 1;
                }
                assert_eq!(
                    pair.step(r::Command::Report(wrong_charge)),
                    Outcome::Rejected
                );
                let mut running = report.clone();
                if let r::Classification::MemoryFailure { quiescent, .. } =
                    &mut running.classification
                {
                    *quiescent = false;
                }
                assert_eq!(pair.step(r::Command::Report(running)), Outcome::Rejected);
                let mut graph = report.clone();
                graph.payload = f::response("memory report with ActionResult", true);
                assert_eq!(pair.step(r::Command::Report(graph)), Outcome::Rejected);
                let expected = if attempt == attempts {
                    Outcome::Terminal(a::TerminalResult::ResourceFailed {
                        result: report.payload.clone(),
                        reason,
                    })
                } else {
                    Outcome::Retry
                };
                assert_eq!(pair.step(r::Command::Report(report.clone())), expected);
                assert_eq!(pair.step(r::Command::Report(report)), Outcome::Duplicate);
                assert_eq!(cleanup(&mut pair, attempt), Outcome::Ignored);
            }
            for write in old_writes {
                assert_eq!(
                    pair.step(r::Command::WriteDone {
                        write,
                        success: true
                    }),
                    Outcome::Applied
                );
            }
            assert!(pair.view().reservations.iter().all(|row| row.released));
        }
    }
}

#[test]
fn last_interest_cancel_and_expiry_retain_charges_until_cleanup() {
    for algorithm in [1, 9] {
        let mut queued = setup(algorithm, 1, 16384, false);
        assert_eq!(
            queued.event(a::Event::Cancel {
                operation: a::OperationId(1)
            }),
            Outcome::Applied
        );
        assert!(queued.view().attempts.is_empty());
        let mut active = setup(algorithm, 1, 16384, false);
        assert_eq!(active.event(a::Event::Match), Outcome::Applied);
        let active_writes = active.stage(100, 1);
        assert_eq!(
            active.event(a::Event::Cancel {
                operation: a::OperationId(1)
            }),
            Outcome::Applied
        );
        assert!(!active.view().reservations[0].released);
        assert_eq!(
            active.step(r::Command::WriteDone {
                write: active_writes[0],
                success: true
            }),
            Outcome::Applied
        );
        assert_eq!(cleanup(&mut active, 1), Outcome::Applied);
        assert_eq!(cleanup(&mut active, 1), Outcome::Ignored);
        let mut pair = setup(algorithm, 1, 16384, false);
        assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
        let writes = pair.stage(100, 1);
        let old = pair.view().operations[0].interest_deadline;
        assert_eq!(
            pair.event(a::Event::RenewInterest {
                operation: a::OperationId(1)
            }),
            Outcome::Applied
        );
        assert_eq!(
            pair.event(a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: old.generation
            }),
            Outcome::Ignored
        );
        let current = pair.view().operations[0].interest_deadline;
        assert_eq!(
            pair.event(a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: current.generation
            }),
            Outcome::Rejected
        );
        assert_eq!(
            pair.event(a::Event::AdvanceTime { now: current.at }),
            Outcome::Applied
        );
        assert!(
            pair.view().executions[0].terminal.is_none(),
            "Advance cannot invent expiry"
        );
        assert_eq!(
            pair.event(a::Event::ExpireInterest {
                operation: a::OperationId(1),
                generation: current.generation
            }),
            Outcome::Applied
        );
        assert!(!pair.view().reservations[0].released);
        assert!(
            pair.view()
                .effects
                .iter()
                .any(|row| matches!(row.kind, a::EffectKind::Cancel { .. }))
        );
        assert_eq!(
            pair.step(r::Command::Report(pair.report(10, 1, false, None))),
            Outcome::Rejected
        );
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[0],
                success: true
            }),
            Outcome::Applied
        );
        assert_eq!(cleanup(&mut pair, 1), Outcome::Applied);
        assert_eq!(cleanup(&mut pair, 1), Outcome::Ignored);
        assert_eq!(
            pair.event(a::Event::Cancel {
                operation: a::OperationId(1)
            }),
            Outcome::Ignored
        );
    }
}

#[test]
fn generation_fenced_attempt_and_execution_expiry_exhaustion() {
    for algorithm in [1, 9] {
        let mut pair = setup(algorithm, 1, 16384, false);
        for attempt in 1..=3 {
            assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
            let identity = pair.identity(attempt);
            let old_generation = pair.view().attempts[usize::try_from(attempt - 1).unwrap()]
                .deadline
                .generation;
            assert_eq!(
                pair.event(a::Event::WorkerAccepted {
                    attempt: identity.attempt,
                    worker: identity.worker,
                    incarnation: identity.incarnation
                }),
                Outcome::Applied
            );
            assert_eq!(
                pair.event(a::Event::Progress {
                    attempt: identity.attempt,
                    worker: identity.worker,
                    incarnation: identity.incarnation,
                    phase: a::Phase::Running,
                    progress: 1
                }),
                Outcome::Applied
            );
            let trial = pair.view().attempts[usize::try_from(attempt - 1).unwrap()];
            assert_eq!(
                pair.event(a::Event::ExpireAttempt {
                    attempt: trial.id,
                    generation: old_generation
                }),
                Outcome::Ignored
            );
            assert_eq!(
                pair.event(a::Event::ExpireExecution { attempt: trial.id }),
                Outcome::Rejected
            );
            assert_eq!(
                pair.event(a::Event::AdvanceTime {
                    now: trial.running_deadline.unwrap()
                }),
                Outcome::Applied
            );
            assert!(pair.view().attempts[usize::try_from(attempt - 1).unwrap()].authorized);
            let expiry = if attempt == 2 {
                a::Event::ExpireAttempt {
                    attempt: trial.id,
                    generation: trial.deadline.generation,
                }
            } else {
                a::Event::ExpireExecution { attempt: trial.id }
            };
            assert_eq!(pair.event(expiry), Outcome::Applied);
            assert!(!pair.view().reservations[usize::try_from(attempt - 1).unwrap()].released);
            if attempt < 3 {
                assert!(pair.view().executions[0].terminal.is_none());
            }
            assert_eq!(cleanup(&mut pair, attempt), Outcome::Applied);
            assert_eq!(
                pair.event(a::Event::ExpireExecution { attempt: trial.id }),
                Outcome::Ignored
            );
        }
        assert_eq!(
            pair.view().executions[0].terminal,
            Some(a::TerminalResult::Exhausted)
        );
        assert!(pair.actual.terminal_acceptances().unwrap().is_empty());
    }
}

#[test]
fn worker_revocation_and_replacement_keep_old_cleanup_separate_from_successor() {
    for algorithm in [1, 9] {
        let mut pair = setup(algorithm, 1, 16384, false);
        assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
        let writes = pair.stage(100, 1);
        assert_eq!(
            pair.event(a::Event::WorkerUnavailable {
                worker: a::WorkerId(1),
                incarnation: a::IncarnationId(1),
                session: a::SessionId(1)
            }),
            Outcome::Applied
        );
        assert_eq!(pair.event(f::register(16384, 2)), Outcome::Applied);
        // A replacement incarnation remains quarantined by the original charge.
        assert_eq!(pair.event(a::Event::Match), Outcome::Ignored);
        assert_eq!(cleanup(&mut pair, 1), Outcome::Applied);
        assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
        assert_eq!(cleanup(&mut pair, 1), Outcome::Ignored);
        assert!(!pair.view().reservations[1].released);
        assert_eq!(
            pair.step(r::Command::WriteDone {
                write: writes[0],
                success: true
            }),
            Outcome::Applied
        );
        assert_eq!(
            pair.event(a::Event::ExpireAttempt {
                attempt: a::AttemptId(1),
                generation: 1
            }),
            Outcome::Ignored
        );
        assert_eq!(pair.event(f::register(16384, 3)), Outcome::Applied);
        assert_eq!(pair.event(a::Event::Match), Outcome::Ignored);
        assert_eq!(cleanup(&mut pair, 2), Outcome::Applied);
        assert_eq!(pair.event(a::Event::Match), Outcome::Applied);
        assert_eq!(cleanup(&mut pair, 2), Outcome::Ignored);
        assert!(!pair.view().reservations[2].released);
        assert_eq!(pair.event(f::register(16384, 4)), Outcome::Applied);
        assert_eq!(
            pair.view().executions[0].terminal,
            Some(a::TerminalResult::Exhausted)
        );
        assert_eq!(cleanup(&mut pair, 3), Outcome::Applied);
    }
}
