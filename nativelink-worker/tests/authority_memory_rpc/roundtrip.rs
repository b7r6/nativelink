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

pub(super) fn faults() -> Arc<QualificationFaults> {
    let faults = Arc::new(QualificationFaults::default());
    faults.hold_publications.store(true, Ordering::Relaxed);
    faults
        .hold_history_publications
        .store(true, Ordering::Relaxed);
    faults
}

pub(super) fn assert_retry_has_no_history(runtime: &Runtime, state: &ObservableState) {
    assert!(
        runtime
            .service
            .subscribe_history()
            .borrow()
            .as_ref()
            .as_ref()
            .unwrap()
            .associations
            .is_empty()
    );
    assert!(
        !state
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    assert!(state.executions[0].terminal.is_none());
}

#[nativelink_test]
async fn memory_rpc_r_then_2r_preserves_original_request_and_outputs_both_hashes() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let (context, contents) = context().await;
        let request = request(&context, algorithm, GRANT, WORKLOAD_TARGET, 1, false).await;
        let clock = Arc::new(AtomicU64::new(0));
        let mut runtime = Runtime::start_history(&context, clock, faults(), None).await;
        let mut connection =
            Connection::open(&runtime, &context, 1, MEMORY_CAPACITY.memory_bytes).await;
        let mut operations = runtime
            .client
            .execute(request.request.clone())
            .await
            .unwrap()
            .into_inner();
        let _first = next_operation(&mut operations).await;
        let command = connection.dispatch().await;
        let first_state = runtime.wait(|state| state.attempts.len() == 1).await;
        let first = launch(&context, &connection, command, &request, &contents).await;
        assert_memory_report(&first, &first_state, GRANT);
        assert_graph(
            &contents,
            captured(&first),
            algorithm,
            request.original_memory,
        )
        .await;
        connection.report(first).await;
        let retry = runtime.wait(|state| state.attempts.len() == 2).await;
        assert_retry_has_no_history(&runtime, &retry);
        assert_eq!(retry.executions[0].request.resources.memory_bytes, GRANT);
        assert_eq!(
            retry.executions[0].reservation_target.memory_bytes,
            2 * GRANT
        );
        assert!(retry.reservations[0].released);
        assert_eq!(retry.reservations[1].charged.memory_bytes, 2 * GRANT);
        let command = connection.dispatch().await;
        let second = launch(&context, &connection, command, &request, &contents).await;
        assert!(second.memory_breach.is_none());
        assert_eq!(captured(&second).result.as_ref().unwrap().exit_code, 0);
        assert_eq!(
            captured(&second)
                .status
                .as_ref()
                .map_or(0, |status| status.code),
            0
        );
        assert_graph(
            &contents,
            captured(&second),
            algorithm,
            request.original_memory,
        )
        .await;
        let expected = captured(&second).encode_to_vec();
        connection.report(second).await;
        let terminal = terminal(&mut operations).await;
        let Some(operation::Result::Response(response)) = &terminal.result else {
            unreachable!()
        };
        assert_eq!(response.value, expected);
        let final_state = runtime
            .wait(|state| state.executions[0].terminal.is_some())
            .await;
        assert_eq!(final_state.executions[0].attempts_started, 2);
        assert!(
            final_state
                .reservations
                .iter()
                .all(|reservation| reservation.released)
        );
        runtime
            .history_association(final_state.executions[0].id, HistoryStatus::Pending)
            .await;
        assert_eq!(
            std::fs::read(context.directory.path().join("memory-launches")).unwrap(),
            b"xx"
        );
        assert_eq!(
            contents
                .get_part_unchunked(request.action, 0, None)
                .await
                .unwrap(),
            request.action_bytes
        );
        assert_eq!(
            contents
                .get_part_unchunked(request.command, 0, None)
                .await
                .unwrap(),
            request.command_bytes
        );
        drop(connection);
        drop(operations);
        runtime.shutdown().await;
    }
}

#[nativelink_test]
async fn memory_rpc_budget_failure_preserves_graph_and_history_across_restart_both_hashes() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let (context, contents) = context().await;
        let request = request(&context, algorithm, GRANT, BUDGET_WORKLOAD_TARGET, 1, false).await;
        let clock = Arc::new(AtomicU64::new(0));
        let faults = faults();
        let mut runtime =
            Runtime::start_history(&context, clock.clone(), faults.clone(), None).await;
        let mut connection =
            Connection::open(&runtime, &context, 1, MEMORY_CAPACITY.memory_bytes).await;
        let mut operations = runtime
            .client
            .execute(request.request.clone())
            .await
            .unwrap()
            .into_inner();
        let _first = next_operation(&mut operations).await;
        let mut final_response = None;
        for number in 1_usize..=3 {
            let command = connection.dispatch().await;
            let state = runtime.wait(|state| state.attempts.len() == number).await;
            let report = launch(&context, &connection, command, &request, &contents).await;
            assert_memory_report(&report, &state, (GRANT) << (number - 1));
            assert_graph(
                &contents,
                captured(&report),
                algorithm,
                request.original_memory,
            )
            .await;
            if number == 3 {
                final_response = Some(captured(&report).clone());
            }
            connection.report(report).await;
            if number < 3 {
                let retry = runtime
                    .wait(|state| state.attempts.len() == number + 1)
                    .await;
                assert_retry_has_no_history(&runtime, &retry);
            }
        }
        let final_response = final_response.unwrap();
        let accepted = terminal(&mut operations).await;
        let Some(operation::Result::Response(response)) = &accepted.result else {
            unreachable!()
        };
        assert_eq!(response.value, final_response.encode_to_vec());
        let state = runtime
            .wait(|state| state.executions[0].terminal.is_some())
            .await;
        let execution = &state.executions[0];
        assert_eq!(execution.attempts_started, 3);
        assert!(matches!(
            &execution.terminal,
            Some(TerminalResult::ResourceFailed {
                reason: ResourceFailureReason::AttemptBudgetExhausted,
                ..
            })
        ));
        assert!(
            state
                .reservations
                .iter()
                .all(|reservation| reservation.released)
        );
        assert!(
            !state
                .effects
                .iter()
                .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
        );
        assert_eq!(context.ac.len_for_test(), 0);
        assert_eq!(context.history.len_for_test(), 0);
        let pending = runtime
            .history_association(execution.id, HistoryStatus::Pending)
            .await;
        let expected_history = history::history_bytes(request.action, &response.value);
        let mut hasher = algorithm.hasher();
        hasher.update(&expected_history);
        let history_digest = hasher.finalize_digest();
        assert_eq!(pending.recipe.object.hash, **history_digest.packed_hash());
        assert_eq!(
            pending.recipe.object.size_bytes,
            history_digest.size_bytes()
        );
        assert_eq!(contents.has(history_digest).await.unwrap(), None);
        assert_eq!(
            std::fs::read(context.directory.path().join("memory-launches")).unwrap(),
            b"xxx"
        );
        drop(connection);
        drop(operations);
        runtime.shutdown().await;
        let mut runtime = Runtime::start_history(&context, clock, faults.clone(), None).await;
        assert_eq!(
            runtime
                .history_association(execution.id, HistoryStatus::Pending)
                .await,
            pending
        );
        runtime.assert_history_terminal(&accepted).await;
        faults
            .hold_history_publications
            .store(false, Ordering::Relaxed);
        assert_eq!(
            runtime
                .history_association(execution.id, HistoryStatus::Materialized)
                .await,
            HistoryAssociation {
                status: HistoryStatus::Materialized,
                ..pending
            }
        );
        assert_eq!(
            contents
                .get_part_unchunked(history_digest, 0, None)
                .await
                .unwrap()
                .as_ref(),
            expected_history
        );
        runtime.assert_history_terminal(&accepted).await;
        assert_eq!(context.ac.len_for_test(), 0);
        assert_eq!(context.history.len_for_test(), 0);
        runtime.shutdown().await;
    }
}

#[nativelink_test]
async fn memory_rpc_current_growth_and_lost_reply_reconnect_cannot_refund_successor() {
    let (context, contents) = context().await;
    let request = request(
        &context,
        DigestHasherFunc::Sha256,
        GRANT,
        WORKLOAD_TARGET,
        1,
        false,
    )
    .await;
    let mut runtime =
        Runtime::start_history(&context, Arc::new(AtomicU64::new(0)), faults(), None).await;
    let mut connection = Connection::open(&runtime, &context, 1, GRANT).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let state = runtime.wait(|state| state.attempts.len() == 1).await;
    let first = launch(&context, &connection, command, &request, &contents).await;
    assert_memory_report(&first, &state, GRANT);
    connection.capacity(&runtime, 2 * GRANT, false).await;
    connection.report(first.clone()).await;
    // The test observes the durable commit without reading its RPC reply.
    // Drop that unread response path only after delivery is established, so the
    // cut exercises an accepted report with a lost reply rather than a local
    // mpsc enqueue that might never reach HTTP/2.
    let retry = runtime.wait(|state| state.attempts.len() == 2).await;
    drop(connection);
    context.worker.disconnect(1);
    assert_retry_has_no_history(&runtime, &retry);
    assert_eq!(
        retry.executions[0].reservation_target.memory_bytes,
        2 * GRANT
    );
    let successor = retry.attempts[1];
    let mut connection = Connection::open(&runtime, &context, 2, 2 * GRANT).await;
    let command = connection.dispatch().await;
    let id = start(&context, &connection, command, &request, &contents).await;
    assert_eq!(id, successor.id.0);
    tokio::time::timeout(Duration::from_secs(5), async {
        while std::fs::read(context.directory.path().join("memory-launches"))
            .unwrap()
            .len()
            != 2
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let mut changed_old = first;
    changed_old
        .memory_breach
        .as_mut()
        .unwrap()
        .charged
        .as_mut()
        .unwrap()
        .memory_bytes = u64::MAX;
    changed_old.memory_breach.as_mut().unwrap().observed_kb = u64::MAX;
    connection.report(changed_old).await;
    let after = connection.barrier(&runtime).await;
    assert_eq!(after.attempts[1], successor);
    assert!(!after.reservations[1].released);
    assert_eq!(after.workers[0].charged.memory_bytes, 2 * GRANT);
    assert_eq!(after.executions[0].attempts_started, 2);
    assert_retry_has_no_history(&runtime, &after);
    let second = candidate(&context, id).await;
    assert!(second.memory_breach.is_none());
    assert_eq!(captured(&second).result.as_ref().unwrap().exit_code, 0);
    connection.report(second).await;
    assert_eq!(
        response(&terminal(&mut operations).await)
            .result
            .unwrap()
            .exit_code,
        0
    );
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn memory_rpc_capacity_shrink_preserves_original_failure_without_growth() {
    let (context, contents) = context().await;
    let request = request(
        &context,
        DigestHasherFunc::Sha256,
        GRANT,
        WORKLOAD_TARGET,
        1,
        false,
    )
    .await;
    let mut runtime =
        Runtime::start_history(&context, Arc::new(AtomicU64::new(0)), faults(), None).await;
    let mut connection = Connection::open(&runtime, &context, 1, 4 * GRANT).await;
    let mut operations = runtime
        .client
        .execute(request.request.clone())
        .await
        .unwrap()
        .into_inner();
    next_operation(&mut operations).await;
    let command = connection.dispatch().await;
    let report = launch(&context, &connection, command, &request, &contents).await;
    let expected = captured(&report).encode_to_vec();
    assert_graph(
        &contents,
        captured(&report),
        DigestHasherFunc::Sha256,
        request.original_memory,
    )
    .await;
    connection.capacity(&runtime, 3 * GRANT / 4, false).await;
    connection.report(report).await;
    let accepted = terminal(&mut operations).await;
    let Some(operation::Result::Response(response)) = accepted.result else {
        unreachable!()
    };
    assert_eq!(response.value, expected);
    let state = runtime
        .wait(|state| state.executions[0].terminal.is_some())
        .await;
    assert!(matches!(
        &state.executions[0].terminal,
        Some(TerminalResult::ResourceFailed {
            reason: ResourceFailureReason::NoLargerEligibleWorker,
            ..
        })
    ));
    assert_eq!(state.executions[0].attempts_started, 1);
    assert_eq!(state.executions[0].reservation_target.memory_bytes, GRANT);
    assert!(state.reservations[0].released);
    assert_eq!(state.workers[0].charged, Resources::default());
    assert!(
        !state
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    runtime
        .history_association(state.executions[0].id, HistoryStatus::Pending)
        .await;
    assert_eq!(context.ac.len_for_test(), 0);
    drop(connection);
    drop(operations);
    runtime.shutdown().await;
}
