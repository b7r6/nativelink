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

use super::*;

fn wire(model: &Model) -> serde_json::Value {
    serde_json::to_value(model).unwrap()
}

fn rejects_local_contradiction(changed: serde_json::Value) {
    let inner: m1::Reference = serde_json::from_value(changed["scheduler"].clone()).unwrap();
    assert!(
        m1::Reference::restore(&inner.snapshot().unwrap()).is_ok(),
        "probe must reach the outer retained-evidence boundary"
    );
    let changed: Model = serde_json::from_value(changed).unwrap();
    let canonical = changed.snapshot().unwrap();
    assert!(
        Model::restore(&canonical).is_err(),
        "locally contradictory retained evidence restored successfully"
    );
}

fn zero_grant_retry() -> Model {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("audit-memory", 0));
    let trial = assign(&mut model);
    let report = memory(&model, 1, trial, Some(1));
    assert_eq!(
        model.apply(&Command::Report(report)),
        Ok(Reply::Report(Disposition::RetryScheduled))
    );
    recover(&mut model);
    model
}

fn missing_evidence_final() -> Model {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("audit-memory", 0));
    let trial = assign(&mut model);
    let report = memory(&model, 1, trial, None);
    let accepted = accepted(&mut model, report);
    assert!(matches!(
        accepted.terminal,
        a::TerminalResult::ResourceFailed {
            reason: a::ResourceFailureReason::MissingGrowthEvidence,
            ..
        }
    ));
    recover(&mut model);
    model
}

#[test]
fn retry_with_zero_original_grant_cannot_lose_its_only_growth_evidence() {
    let model = zero_grant_retry();
    let mut changed = wire(&model);
    changed["reports"]["1"]["report"]["classification"]["MemoryFailure"]["peak_memory_bytes"] =
        serde_json::Value::Null;
    rejects_local_contradiction(changed);
}

#[test]
fn missing_growth_final_cannot_acquire_positive_peak_evidence() {
    let model = missing_evidence_final();
    let mut changed = wire(&model);
    changed["reports"]["1"]["report"]["classification"]["MemoryFailure"]["peak_memory_bytes"] =
        serde_json::json!(1);
    rejects_local_contradiction(changed);
}

#[test]
fn coherent_attempt_budget_reason_still_requires_three_actual_attempts() {
    let model = missing_evidence_final();
    let mut changed = wire(&model);
    changed["scheduler"]["jobs"][0]["result"]["ResourceFailed"]["reason"] =
        serde_json::json!("AttemptBudgetExhausted");
    changed["scheduler"]["interests"][0]["result"]["ResourceFailed"]["reason"] =
        serde_json::json!("AttemptBudgetExhausted");
    changed["accepted"]["1"]["terminal"]["ResourceFailed"]["reason"] =
        serde_json::json!("AttemptBudgetExhausted");
    changed["reports"]["1"]["disposition"]["TerminalAccepted"]["terminal"]["ResourceFailed"]["reason"] =
        serde_json::json!("AttemptBudgetExhausted");
    rejects_local_contradiction(changed);
}

#[test]
fn grown_target_cannot_survive_removal_of_its_retry_record() {
    let model = zero_grant_retry();
    let mut changed = wire(&model);
    changed["reports"].as_object_mut().unwrap().remove("1");
    rejects_local_contradiction(changed);
}

#[test]
fn successor_reservation_cannot_survive_removal_of_its_retry_record() {
    let mut model = zero_grant_retry();
    let next = assign(&mut model);
    assert_eq!(
        model
            .view()
            .unwrap()
            .scheduler
            .reservations
            .iter()
            .find(|row| row.id == next.reservation)
            .unwrap()
            .charged
            .memory_bytes,
        2
    );
    let mut changed = wire(&model);
    changed["reports"].as_object_mut().unwrap().remove("1");
    rejects_local_contradiction(changed);
}

#[test]
fn retry_record_must_retain_its_acceptance_position_and_time() {
    let model = zero_grant_retry();
    let row = &wire(&model)["reports"]["1"];
    assert!(
        row["position"]
            .as_u64()
            .is_some_and(|position| position > 0),
        "retry has no recorded acceptance position"
    );
    assert_eq!(
        row["now"].as_u64(),
        Some(0),
        "retry has no recorded acceptance clock"
    );
}

#[test]
fn terminal_origin_time_cannot_disagree_with_scheduler_terminal_time() {
    let mut model = missing_evidence_final();
    scheduler(&mut model, a::Event::AdvanceTime { now: 10 });
    let mut changed = wire(&model);
    changed["accepted"]["1"]["origin"]["now"] = serde_json::json!(5);
    changed["reports"]["1"]["disposition"]["TerminalAccepted"]["origin"]["now"] =
        serde_json::json!(5);
    rejects_local_contradiction(changed);
}

#[test]
fn terminal_reports_cannot_reuse_one_acceptance_position() {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("first-position", 0));
    admit(&mut model, 2, original("second-position", 0));
    let first = assign(&mut model);
    let one = accepted(&mut model, complete(1, first));
    let second = assign(&mut model);
    accepted(&mut model, complete(2, second));
    let mut changed = wire(&model);
    changed["accepted"]["2"]["origin"]["position"] = serde_json::json!(one.origin.position);
    changed["reports"]["2"]["disposition"]["TerminalAccepted"]["origin"]["position"] =
        serde_json::json!(one.origin.position);
    rejects_local_contradiction(changed);
}

fn two_retries_then_final() -> Model {
    let mut model = zero_grant_retry();
    let second = assign(&mut model);
    let report = memory(&model, 2, second, None);
    assert_eq!(
        model.apply(&Command::Report(report)),
        Ok(Reply::Report(Disposition::RetryScheduled))
    );
    let third = assign(&mut model);
    accepted(&mut model, complete(3, third));
    recover(&mut model);
    model
}

#[test]
fn retry_positions_must_follow_actual_attempt_order_even_at_one_clock_tick() {
    let model = two_retries_then_final();
    let mut changed = wire(&model);
    let first = changed["reports"]["1"]["position"].clone();
    let second = changed["reports"]["2"]["position"].clone();
    changed["reports"]["1"]["position"] = second;
    changed["reports"]["2"]["position"] = first;
    rejects_local_contradiction(changed);
}

#[test]
fn report_order_rejects_zero_future_and_reversed_clock_witnesses() {
    let mut model = two_retries_then_final();
    scheduler(&mut model, a::Event::AdvanceTime { now: 10 });
    for mutation in 0..4 {
        let mut changed = wire(&model);
        match mutation {
            0 => changed["reports"]["1"]["position"] = serde_json::json!(0),
            1 => {
                changed["reports"]["1"]["position"] =
                    serde_json::json!(model.view().unwrap().position + 1);
            }
            2 => changed["reports"]["1"]["now"] = serde_json::json!(11),
            3 => changed["reports"]["1"]["now"] = serde_json::json!(1),
            _ => unreachable!(),
        }
        rejects_local_contradiction(changed);
    }
}

#[test]
fn coherent_terminal_time_witness_cannot_rewrite_retained_scheduler_time() {
    let mut model = missing_evidence_final();
    scheduler(&mut model, a::Event::AdvanceTime { now: 10 });
    let mut changed = wire(&model);
    changed["reports"]["1"]["now"] = serde_json::json!(5);
    changed["accepted"]["1"]["origin"]["now"] = serde_json::json!(5);
    changed["reports"]["1"]["disposition"]["TerminalAccepted"]["origin"]["now"] =
        serde_json::json!(5);
    rejects_local_contradiction(changed);
}

#[test]
fn retained_growth_cannot_exceed_the_original_evidence_bound() {
    for successor in [false, true] {
        let mut model = zero_grant_retry();
        if successor {
            assign(&mut model);
        }
        let mut changed = wire(&model);
        changed["scheduler"]["jobs"][0]["target"]["memory_bytes"] = serde_json::json!(3);
        if successor {
            changed["scheduler"]["charges"][1]["original"]["memory_bytes"] = serde_json::json!(3);
        }
        rejects_local_contradiction(changed);
    }
}

#[test]
fn historical_no_larger_reason_survives_later_capacity_growth() {
    let mut model = model();
    register(&mut model, 1024);
    admit(&mut model, 1, original("historical-ceiling", 1));
    let trial = assign(&mut model);
    let report = memory(&model, 1, trial, None);
    let accepted = accepted(&mut model, report);
    assert!(matches!(
        accepted.terminal,
        a::TerminalResult::ResourceFailed {
            reason: a::ResourceFailureReason::NoLargerEligibleWorker,
            ..
        }
    ));
    let capacity = a::Resources {
        cpu_millis: 8000,
        memory_bytes: 8192,
        slots: 8,
    };
    scheduler(
        &mut model,
        a::Event::Capacity {
            worker: trial.worker,
            incarnation: trial.incarnation,
            session: a::SessionId(1),
            capacity,
            available: capacity,
            draining: false,
        },
    );
    recover(&mut model);
    assert_eq!(model.view().unwrap().accepted[&trial.execution], accepted);
}

#[test]
fn actual_third_attempt_budget_reason_precedes_missing_growth_evidence() {
    let mut model = model();
    register(&mut model, 8192);
    admit(&mut model, 1, original("real-third-attempt", 0));
    for _ in 0..2 {
        let trial = assign(&mut model);
        let current = *model.view().unwrap().scheduler.attempts.last().unwrap();
        scheduler(
            &mut model,
            a::Event::AdvanceTime {
                now: current.deadline.at,
            },
        );
        scheduler(
            &mut model,
            a::Event::ExpireAttempt {
                attempt: trial.id,
                generation: current.deadline.generation,
            },
        );
        cleanup(&mut model, trial);
    }
    let third = assign(&mut model);
    let report = memory(&model, 1, third, None);
    let accepted = accepted(&mut model, report);
    assert!(matches!(
        accepted.terminal,
        a::TerminalResult::ResourceFailed {
            reason: a::ResourceFailureReason::AttemptBudgetExhausted,
            ..
        }
    ));
    recover(&mut model);
}

#[test]
fn version_one_cannot_silently_invent_missing_retry_witnesses() {
    let model = zero_grant_retry();
    let mut state = wire(&model);
    for record in state["reports"].as_object_mut().unwrap().values_mut() {
        record.as_object_mut().unwrap().remove("position");
        record.as_object_mut().unwrap().remove("now");
    }
    let old = serde_json::to_vec(&("independent-no-graph-terminals-v1", state)).unwrap();
    assert!(Model::restore(&old).is_err());
}
