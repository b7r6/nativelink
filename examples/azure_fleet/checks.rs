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

//! Acceptance depends on lifecycle outcomes and correlated download attempts,
//! never on a fitted byte saving or elapsed-time threshold.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::workload::NODES;

pub(crate) fn closed_destination_exhaustions(log: &str) -> usize {
    // Retain the legacy diagnostic for comparison, never as the retry verdict.
    // Require one error chain: unrelated throttling and producer failures on
    // separate lines do not establish a retry against a closed destination.
    log.lines()
        .filter(|line| {
            line.contains("Retries exhausted")
                && line.contains("client=\"azure\"")
                && line.contains("Failed to send data to writer")
                && (line.contains("Tried to send while stream is closed")
                    || line.contains("receiver disconnected"))
        })
        .count()
}

fn require(failures: &mut Vec<String>, passed: bool, message: &str) {
    if !passed {
        failures.push(message.to_owned());
    }
}

fn check_jobs(report: &Value, failures: &mut Vec<String>) {
    let jobs: &[Value] = report["jobs"].as_array().map_or(&[], Vec::as_slice);
    require(
        failures,
        !jobs.is_empty() && Some(jobs.len() as u64) == report["expected_jobs"].as_u64(),
        "not every planned job has an observed outcome",
    );
    require(
        failures,
        jobs.iter().all(|job| {
            job["outcome"] == "success"
                && job["attempts"].as_array().is_some_and(|attempts| {
                    !attempts.is_empty() && attempts.iter().all(|a| a["outcome"] != "censored")
                })
        }),
        "work failed or a client attempt was censored",
    );
}

fn check_worker_recovery(nodes: &[Value], failures: &mut Vec<String>) {
    require(
        failures,
        nodes.len() == NODES,
        "not all four workers became ready",
    );
    require(
        failures,
        nodes.iter().all(|node| {
            node["sentinel_same_process"] == true && node["sentinel_cache_reused"] == true
        }),
        "same-worker sentinel or warm-cache reuse failed",
    );
}

fn check_terminal_retries(nodes: &[Value], failures: &mut Vec<String>) {
    // A final error can hide earlier bad attempts, or follow only legitimate
    // retries. Require a complete event/HTTP join before interpreting a zero.
    require(
        failures,
        nodes
            .iter()
            .all(|node| node["download_witness"]["complete"] == true),
        "download attempt evidence is incomplete",
    );
    require(
        failures,
        nodes
            .iter()
            .all(|node| node["download_witness"]["new_attempts_after_failed_send"] == 0),
        "Azure started a new download after destination failure",
    );
}

fn check_filesystem_fault(report: &Value, nodes: &[Value], failures: &mut Vec<String>) {
    // Fault and control need different evidence: recovery alone cannot prove
    // that either deliberately filled filesystem actually rejected a worker write.
    if report["scenario"] == "failed-fill" {
        require(
            failures,
            [0, 1].iter().all(|index| {
                nodes
                    .iter()
                    .any(|node| node["node"] == *index && node["worker_enospc_observed"] == true)
            }),
            "intended worker ENOSPC was not observed on both filled filesystems",
        );
        require(
            failures,
            report["fault"]["repaired_s"].is_number(),
            "filesystem repair was not observed",
        );
        require(
            failures,
            [0, 1].iter().all(|index| {
                nodes.iter().any(|node| {
                    node["node"] == *index
                        && node["download_witness"]["failed_destination_reads"]
                            .as_u64()
                            .is_some_and(|count| count > 0)
                })
            }),
            "Azure did not observe destination failure on both filled filesystems",
        );
    } else {
        require(
            failures,
            nodes
                .iter()
                .all(|node| node["worker_enospc_observed"] == false),
            "control unexpectedly encountered ENOSPC",
        );
    }
}

fn check_cleanup(report: &Value, node_count: usize, failures: &mut Vec<String>) {
    let cleanup: &[Value] = report["cleanup"].as_array().map_or(&[], Vec::as_slice);
    require(
        failures,
        cleanup.len() == node_count && cleanup.iter().all(|node| node["success"] == true),
        "worker shutdown failed or required forced termination",
    );
}

fn check_scheduler(report: &Value, nodes: &[Value], failures: &mut Vec<String>) {
    // Sampled reservations and the final release are independent checks.
    // The final snapshot must also retain every original worker identity.
    let snapshots: &[Value] = report["scheduler_snapshots"]
        .as_array()
        .map_or(&[], Vec::as_slice);
    require(
        failures,
        !snapshots.is_empty()
            && snapshots.iter().all(|sample| {
                sample["workers"].as_array().is_some_and(|workers| {
                    workers.iter().all(|worker| {
                        worker["max_inflight_tasks"] == 2
                            && worker["running_actions"]
                                .as_u64()
                                .is_some_and(|count| count <= 2)
                    })
                })
            }),
        "scheduler reservation samples are missing or exceed the advertised limit",
    );
    let originals: BTreeSet<_> = nodes
        .iter()
        .filter_map(|node| node["worker_id"].as_str())
        .collect();
    require(
        failures,
        originals.len() == NODES
            && snapshots.last().is_some_and(|sample| {
                sample["final"] == true
                    && sample["workers"].as_array().is_some_and(|workers| {
                        let ids: BTreeSet<_> = workers
                            .iter()
                            .filter_map(|worker| worker["id"].as_str())
                            .collect();
                        ids == originals
                            && workers.iter().all(|worker| worker["running_actions"] == 0)
                    })
            }),
        "final scheduler state did not retain the original workers with zero reservations",
    );
}

pub(crate) fn failures(report: &Value) -> Vec<String> {
    let mut failures = Vec::new();
    let nodes: &[Value] = report["nodes"].as_array().map_or(&[], Vec::as_slice);

    require(
        &mut failures,
        report["session_error"].is_null(),
        "fleet session failed",
    );
    check_jobs(report, &mut failures);
    check_worker_recovery(nodes, &mut failures);
    check_terminal_retries(nodes, &mut failures);
    check_filesystem_fault(report, nodes, &mut failures);
    check_cleanup(report, nodes.len(), &mut failures);
    check_scheduler(report, nodes, &mut failures);

    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_retry_witness_requires_the_same_azure_error_chain() {
        let prefix = "Retries exhausted client=\"azure\" attempt=7";
        assert_eq!(
            closed_destination_exhaustions(&format!(
                "{prefix} Failed to send data to writer: Tried to send while stream is closed"
            )),
            1
        );
        assert_eq!(
            closed_destination_exhaustions(&format!(
                "{prefix} Failed to send data to writer: receiver disconnected"
            )),
            1
        );
        assert_eq!(
            closed_destination_exhaustions(&format!(
                "{prefix} ServerBusy\nFailed to send data to writer: receiver disconnected"
            )),
            0
        );
        assert_eq!(
            closed_destination_exhaustions(
                "Retries exhausted client=\"s3\" Failed to send data to writer: receiver disconnected"
            ),
            0
        );
        assert_eq!(
            closed_destination_exhaustions("Sender dropped before sending EOF"),
            0
        );
    }

    #[test]
    fn complete_control_passes_but_terminal_retry_fails_without_a_byte_threshold() {
        let nodes: Vec<_> = (0..NODES)
            .map(|node| {
                serde_json::json!({
                    "node": node,
                    "worker_id": format!("worker-{node}"),
                    "sentinel_same_process": true,
                    "sentinel_cache_reused": true,
                    "closed_destination_retry_exhaustions": 0,
                    "download_witness": {
                        "complete": true,
                        "new_attempts_after_failed_send": 0,
                        "failed_destination_reads": 0
                    },
                    "worker_enospc_observed": false
                })
            })
            .collect();
        let workers: Vec<_> = (0..NODES)
            .map(|node| {
                serde_json::json!({
                    "id": format!("worker-{node}"),
                    "max_inflight_tasks": 2,
                    "running_actions": 0
                })
            })
            .collect();
        let mut report = serde_json::json!({
            "session_error": null,
            "scenario": "control",
            "expected_jobs": 1,
            "jobs": [{
                "outcome": "success",
                "attempts": [{"outcome": "success"}]
            }],
            "nodes": nodes,
            "cleanup": vec![serde_json::json!({"success": true}); NODES],
            "scheduler_snapshots": [{"final": true, "workers": workers}],
            "account": {"payload_bytes": u64::MAX},
            "duration_s": 100_000
        });
        assert!(failures(&report).is_empty());
        // An exhausted final failed send alone does not prove that a new
        // download followed it. Conversely, a final 503 cannot hide a witness.
        report["nodes"][0]["closed_destination_retry_exhaustions"] = 1.into();
        assert!(failures(&report).is_empty());
        report["nodes"][0]["closed_destination_retry_exhaustions"] = 0.into();
        report["nodes"][0]["download_witness"]["new_attempts_after_failed_send"] = 1.into();
        assert_eq!(
            failures(&report),
            ["Azure started a new download after destination failure"]
        );
        report["nodes"][0]["download_witness"]["complete"] = false.into();
        assert!(
            failures(&report)
                .iter()
                .any(|reason| reason == "download attempt evidence is incomplete")
        );
    }

    #[test]
    fn missing_outcomes_and_log_evidence_cannot_pass() {
        assert!(!failures(&serde_json::json!({})).is_empty());
    }
}
