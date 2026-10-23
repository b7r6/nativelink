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

//! Actual RPC history delivery through the fixed CAS fixture, with synthetic workers.

use std::collections::BTreeSet;

use nativelink_config::stores::{StoreSpec, VerifySpec};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::HistoricalExecuteResponse;
use nativelink_scheduler::authority::accepted_history::{
    AcceptedHistory, HistoryAssociation, HistoryStatus,
};
use nativelink_scheduler::authority::format::{
    ACCEPTED_HISTORY, AdmissionRegistry, AuthorityDomain, JournalProfile, ReaderCapabilities,
    accepted_history_namespace,
};
use nativelink_scheduler::authority::journal::Journal;
use nativelink_scheduler::authority::{ExecutionId, State};
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::digest_hasher::DigestHasher;

use super::*;

const DOMAIN: AuthorityDomain = AuthorityDomain {
    epoch: EPOCH,
    namespace: 81,
};

impl Context {
    fn history() -> Self {
        let contents = Store::new(MemoryStore::new(&MemorySpec::default()));
        let (pending, receiver) = mpsc::unbounded_channel();
        Self {
            directory: TempDir::new().unwrap(),
            cas: Store::new(VerifyStore::new(
                &VerifySpec {
                    verify_hash: true,
                    verify_size: true,
                    backend: StoreSpec::Memory(MemorySpec::default()),
                },
                Store::new(Arc::new(GatedSink {
                    inner: contents.clone(),
                    pending,
                })),
            )),
            contents,
            sink: Store::new(MemoryStore::new(&MemorySpec::default())),
            pending: receiver,
        }
    }

    async fn history_request(
        &self,
        algorithm: DigestHasherFunc,
        do_not_cache: bool,
    ) -> (ExecuteRequest, DigestInfo) {
        let command = serialize_and_upload_message(
            &Command {
                arguments: vec!["/bin/true".into()],
                ..Command::default()
            },
            self.contents.as_pin(),
            &mut algorithm.hasher(),
        )
        .await
        .unwrap();
        let root = serialize_and_upload_message(
            &Directory::default(),
            self.contents.as_pin(),
            &mut algorithm.hasher(),
        )
        .await
        .unwrap();
        let action = serialize_and_upload_message(
            &Action {
                command_digest: Some(command.into()),
                input_root_digest: Some(root.into()),
                do_not_cache,
                timeout: Some(prost_types::Duration {
                    seconds: 30,
                    nanos: 0,
                }),
                ..Action::default()
            },
            self.contents.as_pin(),
            &mut algorithm.hasher(),
        )
        .await
        .unwrap();
        (
            ExecuteRequest {
                instance_name: INSTANCE.into(),
                action_digest: Some(action.into()),
                digest_function: match algorithm {
                    DigestHasherFunc::Sha256 => 1,
                    DigestHasherFunc::Blake3 => 9,
                },
                skip_cache_lookup: true,
                ..ExecuteRequest::default()
            },
            action,
        )
    }

    async fn history_authority(&self, incarnation: u64) -> Singleton {
        let path = self.directory.path().join("authority.redb");
        let admission = if path.exists() {
            Journal::recover_admission(
                DOMAIN,
                ReaderCapabilities::AcceptedHistory,
                vec![(1, path.clone())],
            )
            .await
            .unwrap()
        } else {
            AdmissionRegistry::new(DOMAIN)
        };
        let authority = Singleton::open_with_profile(
            path,
            1,
            Limits::default(),
            JournalProfile {
                domain: DOMAIN,
                incarnation,
                reader: ReaderCapabilities::AcceptedHistory,
                admission,
            },
        )
        .await
        .unwrap();
        // This fixture owns the old process and has stopped it before reopening.
        // A general service start must not abort another live activation helper.
        authority
            .journal
            .resolve_preparation(&authority.raft)
            .await
            .unwrap();
        if !authority.read().await.unwrap().accepted_history_enabled() {
            authority
                .journal
                .activate_features(&authority.raft, BTreeSet::from([ACCEPTED_HISTORY]))
                .await
                .unwrap();
        }
        authority
    }
}

impl Runtime {
    async fn start_history(
        context: &Context,
        incarnation: u64,
        clock: Arc<AtomicU64>,
        faults: Arc<QualificationFaults>,
    ) -> Self {
        let authority = context.history_authority(incarnation).await;
        let (service, task) = AuthorityService::start(
            authority,
            QualificationConfig {
                instance: INSTANCE.into(),
                principal: "trusted-synthetic-history-fixture".into(),
                cas: context.cas.clone(),
                cas_namespace: accepted_history_namespace(DOMAIN),
                publication_sink: context.sink.clone(),
                now: Arc::new(move || clock.load(Ordering::Relaxed)),
                faults,
            },
        )
        .await
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = stream::unfold(listener, |listener| async {
            Some((
                listener.accept().await.map(|(connection, _)| connection),
                listener,
            ))
        });
        let server = background_spawn!(
            "accepted_history_rpc",
            Server::builder()
                .add_service(ExecutionServer::new(service.clone()))
                .add_service(AuthorityWorkerApiServer::new(service.clone()))
                .serve_with_incoming(incoming)
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            service,
            task,
            client: ExecutionClient::new(channel.clone()),
            channel,
            server,
        }
    }

    async fn history_state(
        &self,
        predicate: impl Fn(&AcceptedHistory) -> bool + Send + Sync,
    ) -> Arc<Option<AcceptedHistory>> {
        let mut committed = self.service.subscribe_history();
        tokio::time::timeout(WAIT, async {
            loop {
                let state = committed.borrow_and_update().clone();
                if state.as_ref().as_ref().is_some_and(&predicate) {
                    return state;
                }
                committed.changed().await.unwrap();
            }
        })
        .await
        .unwrap()
    }

    async fn association(
        &self,
        execution: ExecutionId,
        status: HistoryStatus,
    ) -> HistoryAssociation {
        self.history_state(|state| {
            state
                .associations
                .get(&execution)
                .is_some_and(|a| a.status == status)
        })
        .await
        .as_ref()
        .as_ref()
        .unwrap()
        .associations[&execution]
            .clone()
    }
}

fn expected_history(operation: &Operation, action: DigestInfo) -> Vec<u8> {
    let Some(operation::Result::Response(response)) = &operation.result else {
        panic!("Missing accepted response");
    };
    // Fixture expectation comes from the externally observed terminal response
    // and submitted action; it never asks the candidate for object bytes.
    HistoricalExecuteResponse {
        action_digest: Some(action.into()),
        execute_response: Some(ExecuteResponse::decode(response.value.as_slice()).unwrap()),
    }
    .encode_to_vec()
}

fn assert_history_write(
    write: &PendingWrite,
    association: &HistoryAssociation,
    bytes: &[u8],
    algorithm: DigestHasherFunc,
) {
    let mut digest = algorithm.hasher();
    digest.update(bytes);
    let expected = digest.finalize_digest();
    assert_eq!(write.key, StoreKey::Digest(expected));
    assert_eq!(write.bytes.as_ref(), bytes);
    assert_eq!(association.recipe.object.hash, **expected.packed_hash());
    assert_eq!(association.recipe.object.size_bytes, expected.size_bytes());
    assert_eq!(
        association.recipe.object.namespace,
        accepted_history_namespace(DOMAIN)
    );
}

#[nativelink_test]
async fn accepted_history_retries_exact_bytes_after_restart_and_lost_ack_without_changing_terminal()
{
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        let mut context = Context::history();
        let clock = Arc::new(AtomicU64::new(0));
        let faults = Arc::new(QualificationFaults::default());
        faults
            .hold_history_publications
            .store(true, Ordering::Relaxed);
        let (request, action) = context.history_request(algorithm, true).await;
        let mut runtime = Runtime::start_history(&context, 1, clock.clone(), faults.clone()).await;
        let mut worker = Worker::connect(&runtime, 1).await;
        let result = ActionResult {
            exit_code: 17,
            ..result("accepted noncacheable failure")
        };
        let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
        let dispatch = worker.dispatch().await;
        let execution = ExecutionId(dispatch.attempt.unwrap().execution_id);
        worker.candidate(&dispatch, result.clone()).await;
        let accepted_terminal = terminal(&mut operation).await;
        assert_result(&accepted_terminal, &result);
        let association = runtime.association(execution, HistoryStatus::Pending).await;
        let expected = expected_history(&accepted_terminal, action);
        assert!(context.pending.try_recv().is_err());
        assert_eq!(context.sink.has(action).await.unwrap(), None);
        assert!(
            !runtime
                .service
                .subscribe()
                .borrow()
                .effects
                .iter()
                .any(|e| matches!(e.kind, EffectKind::PublishResult { .. }))
        );
        drop(worker);
        runtime.shutdown().await;

        // First restart is before send; the same accepted association drives it.
        let runtime = Runtime::start_history(&context, 2, clock.clone(), faults.clone()).await;
        assert_eq!(
            runtime.association(execution, HistoryStatus::Pending).await,
            association
        );
        let replacement = Worker::connect(&runtime, 2).await;
        faults
            .hold_history_publications
            .store(false, Ordering::Relaxed);
        let old = context.next_write().await;
        assert_history_write(&old, &association, &expected, algorithm);
        drop(replacement);
        runtime.shutdown().await;

        // This real in-memory provider request survives the coordinator's wait.
        // Its success is not an authority acknowledgement after the restart.
        old.complete().await;
        faults
            .hold_history_publications
            .store(true, Ordering::Relaxed);
        let mut runtime = Runtime::start_history(&context, 3, clock, faults.clone()).await;
        assert_eq!(
            runtime.association(execution, HistoryStatus::Pending).await,
            association
        );
        runtime.assert_terminal(&accepted_terminal).await;
        faults
            .hold_history_publications
            .store(false, Ordering::Relaxed);
        let retry = context.next_write().await;
        assert_history_write(&retry, &association, &expected, algorithm);
        retry.complete().await;
        let materialized = runtime
            .association(execution, HistoryStatus::Materialized)
            .await;
        assert_eq!(
            materialized,
            HistoryAssociation {
                status: HistoryStatus::Materialized,
                ..association.clone()
            }
        );
        runtime.assert_terminal(&accepted_terminal).await;
        let object = association.recipe.object;
        assert_eq!(
            context
                .contents
                .get_part_unchunked(DigestInfo::new(object.hash, object.size_bytes), 0, None)
                .await
                .unwrap()
                .as_ref(),
            expected
        );
        assert_eq!(context.sink.has(action).await.unwrap(), None);
        assert!(context.pending.try_recv().is_err());
        runtime.shutdown().await;
    }
}

#[nativelink_test]
async fn revoked_rpc_candidate_cannot_create_accepted_history() {
    let mut context = Context::history();
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let (request, _) = context
        .history_request(DigestHasherFunc::Sha256, false)
        .await;
    let mut runtime = Runtime::start_history(&context, 1, clock.clone(), faults).await;
    let mut worker = Worker::connect(&runtime, 1).await;
    let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
    let initial = next_operation(&mut operation).await;
    let dispatch = worker.dispatch().await;
    worker.capacity_barrier(&runtime, 1, true).await;
    clock.store(ACK_TIMEOUT + 1, Ordering::Relaxed);
    runtime
        .wait(|s| {
            s.attempts
                .iter()
                .any(|a| a.id.0 == dispatch.attempt.unwrap().attempt_id && !a.authorized)
        })
        .await;
    worker
        .candidate(&dispatch, result("stale history candidate"))
        .await;
    let after = worker.capacity_barrier(&runtime, 2, false).await;
    assert!(after.operations[0].terminal.is_none());
    assert!(after.reservations[0].released);
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
    assert!(context.pending.try_recv().is_err());
    let mut observed = runtime
        .client
        .wait_execution(WaitExecutionRequest { name: initial.name })
        .await
        .unwrap()
        .into_inner();
    assert!(!next_operation(&mut observed).await.done);
    drop(worker);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn history_service_refuses_a_different_logical_cas_namespace_before_delivery() {
    let mut context = Context::history();
    let authority = context.history_authority(1).await;
    let before: State = authority.read().await.unwrap();
    let result = AuthorityService::start(
        authority,
        QualificationConfig {
            instance: INSTANCE.into(),
            principal: "wrong-history-namespace".into(),
            cas: context.cas.clone(),
            cas_namespace: [0; 32],
            publication_sink: context.sink.clone(),
            now: Arc::new(|| 0),
            faults: Arc::new(QualificationFaults::default()),
        },
    )
    .await;
    assert!(matches!(result, Err(ref error) if error.contains("namespace")));
    assert!(context.pending.try_recv().is_err());
    let reopened = context.history_authority(2).await;
    assert_eq!(
        reopened.read().await.unwrap().snapshot().unwrap(),
        before.snapshot().unwrap()
    );
    reopened.shutdown().await.unwrap();
}

#[nativelink_test]
async fn failed_history_delivery_keeps_original_association_pending_until_exact_retry() {
    let mut context = Context::history();
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let (request, action) = context
        .history_request(DigestHasherFunc::Sha256, false)
        .await;
    let mut runtime = Runtime::start_history(&context, 1, clock, faults).await;
    let mut worker = Worker::connect(&runtime, 1).await;
    let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
    let dispatch = worker.dispatch().await;
    worker
        .candidate(&dispatch, result("retry the accepted diagnostic"))
        .await;
    let terminal = terminal(&mut operation).await;
    let execution = ExecutionId(dispatch.attempt.unwrap().execution_id);
    let accepted = runtime.association(execution, HistoryStatus::Pending).await;
    let expected = expected_history(&terminal, action);
    let failed = context.next_write().await;
    assert_history_write(&failed, &accepted, &expected, DigestHasherFunc::Sha256);
    // Dropping the release sender makes the owned provider request fail before
    // writing. Its error is observed by the coordinator's delivery future.
    drop(failed);
    let retry = context.next_write().await;
    assert_eq!(
        runtime.association(execution, HistoryStatus::Pending).await,
        accepted
    );
    assert_history_write(&retry, &accepted, &expected, DigestHasherFunc::Sha256);
    let object = accepted.recipe.object;
    assert_eq!(
        context
            .contents
            .has(DigestInfo::new(object.hash, object.size_bytes))
            .await
            .unwrap(),
        None
    );
    runtime.assert_terminal(&terminal).await;
    retry.complete().await;
    runtime
        .association(execution, HistoryStatus::Materialized)
        .await;
    runtime.assert_terminal(&terminal).await;
    drop(worker);
    runtime.shutdown().await;
}

#[nativelink_test]
async fn action_cache_hit_does_not_create_an_accepted_worker_history() {
    let mut context = Context::history();
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    let (mut request, action) = context
        .history_request(DigestHasherFunc::Sha256, false)
        .await;
    request.skip_cache_lookup = false;
    let cached = result("existing AC value has no worker acceptance");
    context
        .sink
        .update_oneshot(action, cached.encode_to_vec().into())
        .await
        .unwrap();
    let mut runtime = Runtime::start_history(&context, 1, clock, faults).await;
    let mut operation = runtime.client.execute(request).await.unwrap().into_inner();
    let terminal = terminal(&mut operation).await;
    let Some(operation::Result::Response(encoded)) = terminal.result else {
        panic!("Missing cached response");
    };
    let response = ExecuteResponse::decode(encoded.value.as_slice()).unwrap();
    assert!(response.cached_result);
    assert_eq!(response.result, Some(cached));
    assert!(runtime.service.subscribe().borrow().attempts.is_empty());
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
    assert!(context.pending.try_recv().is_err());
    runtime.shutdown().await;
}
