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

//! Recovery ordering with a real journal and a deliberately delayed first tick.

use nativelink_config::stores::MemorySpec;
use nativelink_macro::nativelink_test;
use nativelink_scheduler::authority::{ClusterEpoch, Limits};
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::fs;

use super::*;

#[nativelink_test]
async fn restored_due_interest_expires_before_first_queued_observation() {
    let directory =
        std::env::temp_dir().join(format!("authority-deadlines-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&directory).await.unwrap();
    let epoch = ClusterEpoch(39);
    let operation = OperationId(1);
    let authority = Singleton::open(
        directory.join("authority.redb"),
        1,
        epoch,
        Limits::default(),
    )
    .await
    .unwrap();
    let submitted = authority
        .write(RecordedEvent {
            epoch,
            event: Event::Submit {
                operation,
                request: AuthorityRequest {
                    action: ActionKey {
                        instance: "deadline-recovery".to_owned(),
                        digest_function: "sha256".to_owned(),
                        digest: "a".repeat(64),
                        digest_size_bytes: 1,
                    },
                    execution_scope: None,
                    do_not_cache: true,
                    skip_cache_lookup: true,
                    priority: 0,
                    properties: BTreeMap::new(),
                    resources: Resources {
                        cpu_millis: 1,
                        memory_bytes: 1,
                        slots: 1,
                    },
                    execution_timeout: 5,
                    payload: None,
                },
            },
        })
        .await
        .unwrap();
    assert!(matches!(submitted.data, Reply::Submitted { .. }));
    assert_eq!(
        authority
            .write(RecordedEvent {
                epoch,
                event: Event::AdvanceTime {
                    now: INTEREST_TIMEOUT,
                },
            })
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let before = authority.read().await.unwrap().view().operations[0].clone();
    assert!(before.interested);
    assert!(before.terminal.is_none());
    authority.shutdown().await.unwrap();

    // A durable crash cutpoint after AdvanceTime and before ExpireInterest.
    // Reopen the actual journal; do not replace the driver's committed state.
    let authority = Singleton::open(
        directory.join("authority.redb"),
        1,
        epoch,
        Limits::default(),
    )
    .await
    .unwrap();
    let restored = authority.read().await.unwrap();
    let state = Arc::new(restored.view());
    assert_eq!(state.now, INTEREST_TIMEOUT);
    let (committed, observations) = watch::channel(state.clone());
    let receipts = Arc::new(restored.receipt_retirement().cloned());
    let (committed_receipts, _receipt_observations) = watch::channel(receipts.clone());
    let history = Arc::new(restored.accepted_history().cloned());
    let (committed_history, _history_observations) = watch::channel(history.clone());
    let mut driver = Driver {
        authority,
        state,
        watermark: restored.ingress_watermark(),
        committed,
        config: QualificationConfig {
            instance: "deadline-recovery".to_owned(),
            principal: "local-test".to_owned(),
            cas: Store::new(MemoryStore::new(&MemorySpec::default())),
            cas_namespace: [0; 32],
            publication_sink: Store::new(MemoryStore::new(&MemorySpec::default())),
            // Recovery cannot require another clock advance to sweep due work.
            now: Arc::new(|| 0),
            faults: Arc::new(QualificationFaults::default()),
        },
        workers: BTreeMap::new(),
        sent: BTreeMap::new(),
        effects: JoinSet::new(),
        in_flight: BTreeSet::new(),
        retry_at: BTreeMap::new(),
        storage_cursor: EffectId(0),
        restore_deadlines: true,
        receipts_enabled: restored.receipt_retirement_enabled(),
        receipts,
        committed_receipts,
        receipt_issue_due: true,
        receipt_sent: BTreeMap::new(),
        history,
        committed_history,
    };
    let (commands, inputs) = mpsc::channel(1);
    let (reply, response) = oneshot::channel();
    commands
        .send(Input::Observe(operation, reply))
        .await
        .unwrap();
    let (stop, stopped) = oneshot::channel();
    let ticker = tokio::time::interval_at(
        Instant::now() + Duration::from_mins(1),
        Duration::from_millis(20),
    );
    let (outcome, observed) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(driver.run_loop(inputs, stopped, ticker), async move {
            response.await.unwrap().unwrap();
            let observed = observations.borrow().clone();
            stop.send(()).unwrap();
            observed
        })
    })
    .await
    .unwrap();
    outcome.unwrap();
    let durable = driver.authority.read().await.unwrap().view();
    driver.authority.shutdown().await.unwrap();
    fs::remove_dir_all(&directory).await.unwrap();

    assert_eq!(observed.now, INTEREST_TIMEOUT);
    assert_eq!(
        observed.operations[0].interest_deadline,
        before.interest_deadline
    );
    assert!(
        !observed.operations[0].interested,
        "first queued observation must follow the recovery expiry commit"
    );
    assert!(observed.operations[0].terminal.is_some());
    assert_eq!(durable.operations[0], observed.operations[0]);
    eprintln!(
        "AUTHORITY_DEADLINES: reopened journal sweeps due interest before first queued observation independently of timer readiness"
    );
}
