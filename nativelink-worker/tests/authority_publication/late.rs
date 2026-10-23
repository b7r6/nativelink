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

//! Real prepared output cannot publish after its actual authority is revoked.

use super::*;

fn remains_unpublished(fixture: &Fixture) {
    assert!(fixture.state.heads().is_empty());
    assert!(fixture.state.accepted().is_empty());
    assert!(
        fixture
            .state
            .operations()
            .values()
            .all(|row| row.publication.is_none() && row.root.is_none() && row.payload.is_none())
    );
    let view = fixture.state.scheduler_view();
    assert!(view.operations.iter().all(|row| row.terminal.is_none()));
    assert!(
        !view
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
}
fn refused_unchanged(fixture: &mut Fixture) {
    let before = fixture.state.snapshot().unwrap();
    assert_eq!(
        fixture.state.publish(1, 1, 100),
        Err(types::Error::Scheduler(
            nativelink_scheduler::authority::Rejection::Stale
        ))
    );
    assert_eq!(fixture.state.snapshot().unwrap(), before);
    remains_unpublished(fixture);
}
async fn revoked_prepared_output(algorithm: Algorithm, function: DigestHasherFunc) {
    let context = Context::new(true).await;
    let (original, action_digest) = actual_request(&context, function)
        .with_context(make_ctx_for_hash_func(function).unwrap())
        .await;
    let mut fixture = Fixture::new();
    fixture.state = PublicationAuthority::new_real(
        trusted(),
        BTreeMap::from([(
            ACTION.into(),
            ActionBinding::capture(&trusted(), &original).unwrap(),
        )]),
    )
    .unwrap();
    fixture.state.scheduler_event(register(1)).unwrap();
    context.worker.negotiate(&negotiation(1)).unwrap();
    let execution = fixture
        .state
        .admit_request(1, ACTION.into(), &trusted(), original.clone(), 100)
        .unwrap();
    assert_eq!(
        fixture
            .state
            .admit_request(2, ACTION.into(), &trusted(), original, 100)
            .unwrap(),
        execution
    );
    let Reply::Matched { attempt, .. } = fixture.state.scheduler_event(Event::Match).unwrap()
    else {
        panic!("actual request did not dispatch");
    };
    let effect = fixture.state.scheduler_view().effects.into_iter().find(|effect| matches!(&effect.kind, EffectKind::Dispatch { attempt: id, .. } if *id == attempt)).unwrap();
    let EffectKind::Dispatch {
        charged,
        reservation,
        ..
    } = effect.kind
    else {
        unreachable!()
    };
    context
        .worker
        .command(1, dispatch_command(&effect, charged))
        .unwrap();
    let report = wait_report(&context.worker, |report| {
        report.phase == AuthorityAttemptPhase::Quiescent as i32 && report.result.is_some()
    })
    .await;
    let target = checked_target(&report, &effect).unwrap();
    assert_eq!(target.attempt_id, attempt.0);
    assert_eq!(target.execution_id, execution.0);
    fixture
        .state
        .scheduler_event(Event::WorkerAccepted {
            attempt,
            worker: WorkerId(IDENTITY.worker_id),
            incarnation: IncarnationId(IDENTITY.incarnation_id),
        })
        .unwrap();
    let Event::Complete { result, .. } = completed(&report, &effect).unwrap() else {
        unreachable!()
    };
    let graph = output_graph(&context, algorithm, result).await;
    fixture.stage(
        &graph,
        1,
        Publisher::Worker {
            execution,
            attempt,
            worker: WorkerId(IDENTITY.worker_id),
            incarnation: IncarnationId(IDENTITY.incarnation_id),
        },
    );
    fixture.write_all(&graph, 1);
    fixture.prepare(&graph, 1);
    let issued = fixture.state.stages()[&1].clone();
    assert_eq!(issued.writes.len(), 4);
    assert_eq!(issued.outcomes.len(), 4);
    assert!(issued.outcomes.values().all(|success| *success));
    assert!(issued.preparation.is_some());
    let retention = fixture.state.retention_view();
    remains_unpublished(&fixture);

    fixture
        .state
        .scheduler_event(Event::WorkerUnavailable {
            worker: WorkerId(IDENTITY.worker_id),
            incarnation: IncarnationId(IDENTITY.incarnation_id),
            session: SessionId(1),
        })
        .unwrap();
    let revoked = fixture.state.scheduler_view();
    let actual = revoked
        .executions
        .iter()
        .find(|row| row.id == execution)
        .unwrap();
    assert_eq!(actual.phase, Phase::Queued);
    assert_eq!(actual.attempt, None);
    assert_eq!(actual.attempts_started, 1);
    assert!(
        !revoked
            .attempts
            .iter()
            .find(|row| row.id == attempt)
            .unwrap()
            .authorized
    );
    let held = revoked
        .reservations
        .iter()
        .find(|row| row.id == reservation)
        .unwrap();
    assert_eq!(held.attempt, attempt);
    assert_eq!(held.charged, charged);
    assert!(!held.released);
    let worker = revoked
        .workers
        .iter()
        .find(|row| row.id.0 == IDENTITY.worker_id)
        .unwrap();
    assert!(worker.draining);
    assert_eq!(worker.charged, charged);
    assert!(revoked.effects.iter().any(|effect| matches!(effect.kind, EffectKind::Cancel { attempt: id, execution: owner, .. } if id == attempt && owner == execution)));
    refused_unchanged(&mut fixture);
    assert_eq!(fixture.state.stages()[&1], issued);
    assert_eq!(fixture.state.retention_view(), retention);
    for row in &graph.rows {
        assert!(fixture.state.protected_keys().contains(&row.key));
        fixture.verify_row(row);
    }

    // The original identity-checked report proves quiescence. It can release the
    // old reservation without accepting its result or acknowledging its receipt.
    assert_eq!(checked_target(&report, &effect), Some(target));
    assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
    fixture
        .state
        .scheduler_event(Event::Cleanup {
            attempt,
            worker: WorkerId(IDENTITY.worker_id),
            incarnation: IncarnationId(IDENTITY.incarnation_id),
        })
        .unwrap();
    let cleaned = fixture.state.scheduler_view();
    assert!(
        cleaned
            .reservations
            .iter()
            .find(|row| row.id == reservation)
            .unwrap()
            .released
    );
    assert_eq!(cleaned.workers[0].charged, Resources::default());
    fixture.state.scheduler_event(register(2)).unwrap();
    context.worker.negotiate(&negotiation(2)).unwrap();
    let Reply::Matched {
        execution: retry_execution,
        attempt: retry,
        ..
    } = fixture.state.scheduler_event(Event::Match).unwrap()
    else {
        panic!("fresh retry did not receive authority");
    };
    assert_eq!(retry_execution, execution);
    assert_ne!(retry, attempt);
    let retry_view = fixture.state.scheduler_view();
    let current = retry_view
        .attempts
        .iter()
        .find(|row| row.id == retry)
        .unwrap();
    assert!(current.authorized);
    assert_eq!(current.execution, execution);
    assert_ne!(current.reservation, reservation);
    assert!(
        retry_view
            .reservations
            .iter()
            .any(|row| row.id == current.reservation && !row.released && row.attempt == retry)
    );
    // Only the retry is placed; no second process is dispatched by this case.
    refused_unchanged(&mut fixture);
    assert_eq!(fixture.state.stages()[&1], issued);
    assert_eq!(fixture.state.retention_view(), retention);
    fixture.reopen();
    refused_unchanged(&mut fixture);
    assert_eq!(fixture.state.stages()[&1], issued);
    for row in &graph.rows {
        assert!(fixture.state.protected_keys().contains(&row.key));
        fixture.verify_row(row);
    }
    assert_eq!(context.worker.reconcile().attempts[0], report);
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_eq!(context.history.len_for_test(), 0);
    eprintln!(
        "PUBLICATION_REVOKED_WORKER_PASS {}: actual quiescent output prepared before revocation; Publish refused without mutation; charged old reservation released only by checked cleanup; distinct retry owns authority; issued stage writes survive reopen; no publication or worker acknowledgment",
        algorithm_name(algorithm)
    );
}
#[nativelink_test]
async fn actual_prepared_worker_output_cannot_publish_after_revocation() {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!("SKIP: namespaces unavailable; revoked worker publication remains unqualified");
        return;
    }
    for (algorithm, function) in [
        (Algorithm::Sha256, DigestHasherFunc::Sha256),
        (Algorithm::Blake3, DigestHasherFunc::Blake3),
    ] {
        revoked_prepared_output(algorithm, function).await;
    }
}
