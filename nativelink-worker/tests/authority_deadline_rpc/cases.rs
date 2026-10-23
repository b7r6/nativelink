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

//! Committed deadlines and cleanup fencing through actual worker RPCs.
//! The manual worker runs no process and provides no physical quiescence proof.

use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptReport, AuthorityCapacityObservation, AuthorityCommand,
    AuthorityCoordinatorUpdate, AuthoritySession, AuthorityWorkerUpdate, authority_command,
    authority_coordinator_update, authority_worker_update,
};
use nativelink_scheduler::authority::PROGRESS_TIMEOUT;
use tokio::sync::mpsc;

use super::*;

struct ManualWorker {
    session: AuthoritySession,
    sender: mpsc::Sender<AuthorityWorkerUpdate>,
    responses: Streaming<AuthorityCoordinatorUpdate>,
}

impl ManualWorker {
    async fn connect(context: &Context, runtime: &Runtime) -> Self {
        let session = AuthoritySession {
            worker: Some(IDENTITY),
            session_id: 1,
        };
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Registration(
                    context.worker.make_registration(1, Vec::new()).unwrap(),
                )),
            })
            .await
            .unwrap();
        let updates = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|value| (value, receiver))
        });
        let mut client = AuthorityWorkerApiClient::new(runtime.worker_channel.clone());
        let mut responses = client.connect(updates).await.unwrap().into_inner();
        let registered = tokio::time::timeout(Duration::from_secs(10), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            registered.update,
            Some(authority_coordinator_update::Update::Registration(_))
        ));
        Self {
            session,
            sender,
            responses,
        }
    }

    async fn command(&mut self) -> AuthorityCommand {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let update = self.responses.message().await.unwrap().unwrap();
                if let Some(authority_coordinator_update::Update::Command(command)) = update.update
                {
                    return command;
                }
            }
        })
        .await
        .unwrap()
    }

    async fn report(&self, report: AuthorityAttemptReport) {
        self.sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(report)),
            })
            .await
            .unwrap();
    }

    async fn barrier(&self, runtime: &Runtime, sequence: u64) -> Arc<ObservableState> {
        let available = Resources {
            memory_bytes: CAPACITY.memory_bytes - sequence,
            ..CAPACITY
        };
        self.sender
            .send(AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Capacity(
                    AuthorityCapacityObservation {
                        session: Some(self.session),
                        observation_sequence: sequence,
                        capacity: Some(resources(CAPACITY)),
                        admission_ceiling: Some(resources(available)),
                        // Keep retries queued so the original charge is isolated.
                        draining: true,
                    },
                )),
            })
            .await
            .unwrap();
        runtime
            .wait(|view| view.workers[0].available == available && view.workers[0].draining)
            .await
    }
}

#[nativelink_test]
async fn uploading_progress_deadline_survives_expired_running_deadline() {
    let context = Context::new(false).await;
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let mut runtime = Runtime::start(&context, clock.clone(), faults, None).await;
    let mut worker = ManualWorker::connect(&context, &runtime).await;
    let (request, _, _) = context.request(5).await;
    let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
    assert!(!next_operation(&mut operation).await.done);
    let dispatch = worker.command().await;
    assert!(matches!(
        dispatch.command,
        Some(authority_command::Command::Dispatch(_))
    ));
    let report = AuthorityAttemptReport {
        attempt: dispatch.attempt,
        dispatch_effect_id: dispatch.effect_id,
        observation_sequence: 1,
        phase: AuthorityAttemptPhase::Running as i32,
        ..AuthorityAttemptReport::default()
    };
    worker.report(report.clone()).await;
    let running = runtime
        .wait(|view| view.attempts[0].phase == Phase::Running)
        .await;
    assert_eq!(running.attempts[0].running_deadline, Some(5));
    let charge = running.workers[0].charged;
    assert_ne!(charge, Resources::default());
    worker
        .report(AuthorityAttemptReport {
            phase: AuthorityAttemptPhase::Uploading as i32,
            observation_sequence: 2,
            ..report.clone()
        })
        .await;
    let uploading = worker.barrier(&runtime, 1).await;
    assert_eq!(uploading.attempts[0].phase, Phase::Uploading);
    assert_eq!(uploading.attempts[0].running_deadline, Some(5));
    assert_eq!(uploading.attempts[0].deadline.at, PROGRESS_TIMEOUT);

    clock.store(5, Ordering::Relaxed);
    runtime.wait(|view| view.now == 5).await;
    // The driver finishes its entire due-event sweep before processing this
    // next input, so the barrier observes the expiry decision without a sleep.
    let still_uploading = worker.barrier(&runtime, 2).await;
    assert!(still_uploading.attempts[0].authorized);
    assert_eq!(still_uploading.workers[0].charged, charge);
    assert!(
        still_uploading
            .effects
            .iter()
            .all(|effect| !matches!(effect.kind, EffectKind::Cancel { .. }))
    );

    clock.store(PROGRESS_TIMEOUT, Ordering::Relaxed);
    runtime.wait(|view| view.now == PROGRESS_TIMEOUT).await;
    let expired = worker.barrier(&runtime, 3).await;
    assert!(
        !expired.attempts[0].authorized,
        "expired running deadline must not mask the uploading progress deadline"
    );
    assert_eq!(expired.workers[0].charged, charge);
    assert!(!expired.reservations[0].released);
    assert_eq!(expired.attempts.len(), 1);
    assert!(expired.operations[0].terminal.is_none());
    let cancel = worker.command().await;
    assert!(matches!(
        cancel.command,
        Some(authority_command::Command::Cancel(()))
    ));
    assert_eq!(cancel.attempt, dispatch.attempt);
    assert!(
        expired
            .effects
            .iter()
            .any(|effect| effect.id.0 == cancel.effect_id
                && matches!(effect.kind, EffectKind::Cancel { .. }))
    );

    let cleanup = AuthorityAttemptReport {
        dispatch_effect_id: 0,
        cancel_effect_id: cancel.effect_id + 1,
        phase: AuthorityAttemptPhase::Quiescent as i32,
        observation_sequence: 3,
        ..report
    };
    worker.report(cleanup.clone()).await;
    let wrong_fence = worker.barrier(&runtime, 4).await;
    assert_eq!(wrong_fence.workers[0].charged, charge);
    assert!(!wrong_fence.reservations[0].released);
    worker
        .report(AuthorityAttemptReport {
            cancel_effect_id: cancel.effect_id,
            observation_sequence: 4,
            ..cleanup
        })
        .await;
    let cleaned = worker.barrier(&runtime, 5).await;
    assert!(cleaned.reservations[0].released);
    assert_eq!(cleaned.workers[0].charged, Resources::default());
    assert!(
        cleaned
            .effects
            .iter()
            .all(|effect| !matches!(effect.kind, EffectKind::Cancel { .. }))
    );
    assert!(!context.directory.path().join("executed").exists());
    drop(worker);
    runtime.shutdown().await;
    eprintln!(
        "AUTHORITY_DEADLINES: actual RPC Running->Uploading retains execution deadline, expires at upload progress deadline, delivers committed Cancel, and retains charge until matching synthetic cleanup"
    );
}
