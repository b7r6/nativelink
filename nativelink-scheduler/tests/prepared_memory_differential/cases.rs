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

fn start(algorithm: u8, memory: u64, capacity: u64) -> Pair {
    let mut pair = Pair::new(algorithm, memory);
    assert_eq!(
        pair.event(f::register(capacity, 1)),
        Outcome::Scheduler(a::Reply::Applied)
    );
    pair.admit(1, 1);
    pair
}
fn matched(pair: &mut Pair) {
    assert!(matches!(
        pair.event(a::Event::Match),
        Outcome::Scheduler(a::Reply::Matched { .. })
    ));
}
fn stage(pair: &mut Pair, attempt: u64, peak: Option<u64>) {
    assert_eq!(
        pair.step(pair.stage_command(attempt, peak)),
        Outcome::Applied
    );
}
fn capacity(pair: &mut Pair, memory: u64) {
    let resources = a::Resources {
        cpu_millis: 8000,
        memory_bytes: memory,
        slots: 8,
    };
    assert_eq!(
        pair.event(a::Event::Capacity {
            worker: a::WorkerId(1),
            incarnation: a::IncarnationId(1),
            session: a::SessionId(1),
            capacity: resources,
            available: resources,
            draining: false
        }),
        Outcome::Scheduler(a::Reply::Applied)
    );
}
fn retry(value: &Outcome) {
    assert!(matches!(
        value,
        Outcome::Classified(r::Disposition::RetryScheduled { .. })
    ));
}
fn terminal(value: &Outcome, reason: a::ResourceFailureReason, payload: &a::ResultPayload) {
    assert_eq!(
        value,
        &Outcome::Classified(r::Disposition::TerminalAccepted {
            terminal: a::TerminalResult::ResourceFailed {
                result: payload.clone(),
                reason
            }
        })
    );
}

#[test]
fn both_digests_all_resource_failure_reasons_and_zero_peak_restore_every_prefix() {
    for algorithm in [1, 9] {
        for (memory, maximum, peak, attempts, reason) in [
            (
                0,
                16384,
                None,
                1,
                a::ResourceFailureReason::MissingGrowthEvidence,
            ),
            (
                0,
                16384,
                Some(0),
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
            let mut pair = start(algorithm, memory, maximum);
            pair.admit(2, 7);
            for attempt in 1..=attempts {
                matched(&mut pair);
                stage(&mut pair, attempt, peak);
                assert_eq!(pair.prepare(300 + attempt, 100), Outcome::Applied);
                assert_eq!(pair.prepare(300 + attempt, 100), Outcome::Duplicate);
                let outcome = pair.accept(300 + attempt, 500 + attempt, 80);
                if attempt < attempts {
                    retry(&outcome);
                } else {
                    terminal(&outcome, reason, &pair.payloads[&attempt]);
                }
                assert_eq!(
                    pair.accept(300 + attempt, 500 + attempt, 80),
                    Outcome::Duplicate
                );
            }
            let last = 300 + attempts;
            for (write, success) in [(1, false), (2, true)] {
                let receipt = pair.receipt(last, write, success);
                assert_eq!(
                    pair.step(r::Command::Observe(receipt.clone())),
                    Outcome::Applied
                );
                assert_eq!(
                    pair.step(r::Command::Observe(receipt.clone())),
                    Outcome::Duplicate
                );
                let mut conflicting = receipt;
                conflicting.success = !success;
                assert_eq!(
                    pair.step(r::Command::Observe(conflicting)),
                    Outcome::Rejected
                );
            }
            assert_eq!(
                pair.event(a::Event::AdvanceTime { now: 101 }),
                Outcome::Scheduler(a::Reply::Applied)
            );
            assert_eq!(pair.accept(last, 500 + attempts, 80), Outcome::Duplicate);
        }
    }
}

#[test]
fn acceptance_uses_capacity_at_acceptance_in_both_directions() {
    for algorithm in [1, 9] {
        for (before, after) in [(1024, 16384), (16384, 1024)] {
            let mut pair = start(algorithm, 1, before);
            matched(&mut pair);
            stage(&mut pair, 1, None);
            assert_eq!(pair.prepare(301, 100), Outcome::Applied);
            capacity(&mut pair, after);
            let outcome = pair.accept(301, 501, 80);
            if after > before {
                retry(&outcome);
            } else {
                terminal(
                    &outcome,
                    a::ResourceFailureReason::NoLargerEligibleWorker,
                    &pair.payloads[&1],
                );
            }
        }
    }
}

#[test]
fn cancelled_sibling_keeps_original_request_without_result_root() {
    for algorithm in [1, 9] {
        let mut pair = start(algorithm, 1, 1024);
        pair.admit(2, 9);
        matched(&mut pair);
        assert_eq!(
            pair.event(a::Event::Cancel {
                operation: a::OperationId(2)
            }),
            Outcome::Scheduler(a::Reply::Applied)
        );
        stage(&mut pair, 1, None);
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        terminal(
            &pair.accept(301, 501, 80),
            a::ResourceFailureReason::NoLargerEligibleWorker,
            &pair.payloads[&1],
        );
        assert_eq!(
            pair.model
                .view()
                .operations
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![a::OperationId(1)]
        );
    }
}

#[test]
fn terminal_coverage_failure_is_atomic_and_retry_needs_only_its_acceptance_lease() {
    for algorithm in [1, 9] {
        let mut pair = start(algorithm, 1, 1024);
        matched(&mut pair);
        stage(&mut pair, 1, None);
        assert_eq!(pair.prepare(301, 80), Outcome::Applied);
        assert_eq!(pair.accept(301, 501, 80), Outcome::Rejected);
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        terminal(
            &pair.accept(301, 501, 80),
            a::ResourceFailureReason::NoLargerEligibleWorker,
            &pair.payloads[&1],
        );
        let mut pair = start(algorithm, 1, 16384);
        matched(&mut pair);
        stage(&mut pair, 1, None);
        assert_eq!(pair.prepare(301, 80), Outcome::Applied);
        retry(&pair.accept(301, 501, 80));
    }
}

#[test]
fn incomplete_stale_and_overlong_preparations_refuse_before_classification() {
    for algorithm in [1, 9] {
        let mut pair = start(algorithm, 1, 1024);
        matched(&mut pair);
        stage(&mut pair, 1, None);
        assert_eq!(pair.prepare(301, 101), Outcome::Rejected);
        let mut incomplete = pair.proof(301, 100);
        let removed = incomplete.typed.pop_first().unwrap();
        incomplete.protected.remove(&removed.key);
        assert_eq!(
            pair.step(r::Command::Prepare {
                stage: 301,
                evidence: incomplete
            }),
            Outcome::Rejected
        );
        let mut stale = pair.proof(301, 100);
        stale.catalogue_revision -= 1;
        assert_eq!(
            pair.step(r::Command::Prepare {
                stage: 301,
                evidence: stale
            }),
            Outcome::Rejected
        );
        let mut unfenced = pair.proof(301, 100);
        unfenced.fences.clear();
        assert_eq!(
            pair.step(r::Command::Prepare {
                stage: 301,
                evidence: unfenced
            }),
            Outcome::Rejected
        );
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        assert_eq!(
            pair.step(r::Command::Observe(pair.receipt(301, 1, false))),
            Outcome::Applied
        );
        assert_eq!(pair.accept(301, 501, 80), Outcome::Rejected);
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        terminal(
            &pair.accept(301, 501, 80),
            a::ResourceFailureReason::NoLargerEligibleWorker,
            &pair.payloads[&1],
        );
    }
}

#[test]
fn retry_publication_identifier_reuse_and_replay_precede_expiry_and_authority() {
    for algorithm in [1, 9] {
        let mut pair = start(algorithm, 1, 16384);
        matched(&mut pair);
        let command = pair.stage_command(1, None);
        assert_eq!(pair.step(command.clone()), Outcome::Applied);
        assert_eq!(pair.step(command), Outcome::Rejected);
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        retry(&pair.accept(301, 999, 80));
        matched(&mut pair);
        stage(&mut pair, 2, None);
        capacity(&mut pair, 2048);
        assert_eq!(pair.prepare(302, 100), Outcome::Applied);
        terminal(
            &pair.accept(302, 999, 80),
            a::ResourceFailureReason::NoLargerEligibleWorker,
            &pair.payloads[&2],
        );
        assert_eq!(
            pair.event(a::Event::AdvanceTime { now: 11 }),
            Outcome::Scheduler(a::Reply::Applied)
        );
        assert_eq!(
            pair.event(a::Event::WorkerUnavailable {
                worker: a::WorkerId(1),
                incarnation: a::IncarnationId(1),
                session: a::SessionId(1)
            }),
            Outcome::Scheduler(a::Reply::Applied)
        );
        assert_eq!(pair.accept(301, 999, 80), Outcome::Duplicate);
        assert_eq!(pair.accept(301, 998, 80), Outcome::Rejected);
        assert_eq!(pair.accept(301, 999, 81), Outcome::Rejected);
    }
}

#[test]
fn stage_local_write_ids_survive_reuse_abandonment_and_late_receipts() {
    for algorithm in [1, 9] {
        let mut pair = start(algorithm, 1, 16384);
        matched(&mut pair);
        stage(&mut pair, 1, None);
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        retry(&pair.accept(301, 501, 80));
        assert_eq!(pair.step(r::Command::Abandon(301)), Outcome::Applied);
        assert_eq!(pair.step(r::Command::Abandon(301)), Outcome::Rejected);
        matched(&mut pair);
        stage(&mut pair, 2, None);
        assert_eq!(
            pair.step(r::Command::Observe(pair.receipt(301, 1, true))),
            Outcome::Applied
        );
        assert_eq!(
            pair.step(r::Command::Observe(pair.receipt(302, 1, false))),
            Outcome::Applied
        );
        assert_eq!(
            pair.event(a::Event::AdvanceTime { now: 11 }),
            Outcome::Scheduler(a::Reply::Applied)
        );
        assert_eq!(
            pair.step(r::Command::Observe(pair.receipt(302, 2, true))),
            Outcome::Applied
        );
        assert_eq!(pair.accept(302, 502, 80), Outcome::Rejected);
    }
}

#[test]
fn graph_and_no_graph_paths_share_attempt_budget_and_report_identity() {
    for algorithm in [1, 9] {
        for graph_first in [false, true] {
            let mut pair = start(algorithm, 1, 16384);
            matched(&mut pair);
            if graph_first {
                stage(&mut pair, 1, None);
                assert_eq!(pair.prepare(301, 100), Outcome::Applied);
                retry(&pair.accept(301, 501, 80));
                assert_eq!(
                    pair.step(r::Command::NoGraph(pair.report(1, false, None))),
                    Outcome::Rejected
                );
                assert_eq!(pair.step(r::Command::Abandon(301)), Outcome::Applied);
            } else {
                retry(&pair.step(r::Command::NoGraph(pair.report(1, false, None))));
            }
            matched(&mut pair);
            capacity(&mut pair, 2048);
            if graph_first {
                let report = pair.report(2, false, None);
                terminal(
                    &pair.step(r::Command::NoGraph(report.clone())),
                    a::ResourceFailureReason::NoLargerEligibleWorker,
                    &report.payload,
                );
                assert_eq!(pair.step(r::Command::NoGraph(report)), Outcome::Duplicate);
                assert_eq!(
                    pair.step(r::Command::Observe(pair.receipt(301, 1, false))),
                    Outcome::Applied
                );
            } else {
                stage(&mut pair, 2, None);
                assert_eq!(pair.prepare(302, 100), Outcome::Applied);
                terminal(
                    &pair.accept(302, 502, 80),
                    a::ResourceFailureReason::NoLargerEligibleWorker,
                    &pair.payloads[&2],
                );
            }
        }
    }
}

#[test]
fn invalid_worker_classification_and_conflicting_report_identity_are_atomic() {
    for algorithm in [1, 9] {
        let mut pair = start(algorithm, 1, 16384);
        matched(&mut pair);
        for change in 0..4 {
            let mut command = pair.stage_command(1, None);
            let r::Command::Stage { report, epoch, .. } = &mut command else {
                unreachable!()
            };
            match change {
                0 => {
                    let r::Classification::MemoryFailure {
                        original_charged, ..
                    } = &mut report.classification
                    else {
                        unreachable!()
                    };
                    original_charged.memory_bytes += 1;
                }
                1 => {
                    let r::Classification::MemoryFailure { quiescent, .. } =
                        &mut report.classification
                    else {
                        unreachable!()
                    };
                    *quiescent = false;
                }
                2 => *epoch = a::ClusterEpoch(2),
                _ => report.identity.incarnation = a::IncarnationId(2),
            }
            assert_eq!(pair.step(command), Outcome::Rejected);
        }
        stage(&mut pair, 1, None);
        let mut command = pair.stage_command(1, None);
        let r::Command::Stage { id, report, .. } = &mut command else {
            unreachable!()
        };
        *id = 399;
        report.id = 199;
        assert_eq!(pair.step(command), Outcome::Rejected);
        assert_eq!(
            pair.step(r::Command::NoGraph(pair.report(1, false, None))),
            Outcome::Rejected
        );
    }
}

#[test]
fn ordinary_cache_miss_admission_and_duplicate_admission_keep_original_caller_bytes() {
    for algorithm in [1, 9] {
        let mut pair = Pair::new(algorithm, 1);
        assert_eq!(
            pair.event(f::register(1024, 1)),
            Outcome::Scheduler(a::Reply::Applied)
        );
        let command = r::Command::Admit {
            operation: a::OperationId(1),
            request: f::request_lookup(algorithm, 1, 1, f::LABEL, false),
        };
        assert_eq!(pair.step(command.clone()), Outcome::Applied);
        assert_eq!(pair.step(command), Outcome::Rejected);
        pair.admit(2, 13);
        matched(&mut pair);
        stage(&mut pair, 1, None);
        assert_eq!(pair.prepare(301, 100), Outcome::Applied);
        terminal(
            &pair.accept(301, 501, 80),
            a::ResourceFailureReason::NoLargerEligibleWorker,
            &pair.payloads[&1],
        );
    }
}
