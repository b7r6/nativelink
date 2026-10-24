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

//! Cross-component experiment: real durable Raft, real recipient, and real
//! subprocesses. The bridge below is explicit test wiring, not a mounted REAPI
//! service, authenticated coordinator, or qualified publication/retention path.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Directory, ExecuteRequest, FileNode, Platform, RequestMetadata,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptIdentity, AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCommand,
    AuthorityDispatch, AuthorityRegistrationResult, AuthorityResources, AuthoritySession,
    AuthorityWorkerIdentity, authority_attempt_report, authority_command,
};
use nativelink_scheduler::authority::journal::{Batch, Singleton};
use nativelink_scheduler::authority::request::RequestPayload;
use nativelink_scheduler::authority::{
    AttemptId, ClusterEpoch, Effect, EffectKind, Event, IncarnationId, Limits, OperationId,
    PROGRESS_TIMEOUT, Phase, RecordedEvent, Reply, Request, Resources, ResultPayload, SessionId,
    TerminalResult, WorkerId,
};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::buf_channel::{DropCloserReadHalf, DropCloserWriteHalf};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::health_utils::{HealthStatusIndicator, default_health_status_indicator};
use nativelink_util::store_trait::{
    RemoveCallback, Store, StoreDriver, StoreKey, StoreLike, UploadSizeInfo,
};
use nativelink_worker::authority_worker::{
    AuthorityWorker, PROTOCOL_VERSION, REQUIRED_CAPABILITIES,
};
use nativelink_worker::namespace_utils;
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, PersistentWorkersSettings, RunningActionsManagerArgs,
    RunningActionsManagerImpl, UseNamespaces,
};
use prost::Message;
use prost_types::Timestamp;
use tempfile::TempDir;
use tokio::sync::Semaphore;

const EPOCH: ClusterEpoch = ClusterEpoch(1);
const IDENTITY: AuthorityWorkerIdentity = AuthorityWorkerIdentity {
    cluster_epoch: 1,
    worker_id: 2,
    incarnation_id: 3,
};
const CAPACITY: Resources = Resources {
    cpu_millis: 2000,
    memory_bytes: 1024 * 1024 * 1024,
    slots: 2,
};

const fn resources(value: Resources) -> AuthorityResources {
    AuthorityResources {
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        slots: value.slots,
    }
}

const fn recorded(event: Event) -> RecordedEvent {
    RecordedEvent {
        epoch: EPOCH,
        event,
    }
}

const fn register(session: u64) -> Event {
    Event::Register {
        worker: WorkerId(IDENTITY.worker_id),
        incarnation: IncarnationId(IDENTITY.incarnation_id),
        session: SessionId(session),
        properties: BTreeMap::new(),
        capacity: CAPACITY,
        available: CAPACITY,
    }
}

fn negotiation(session: u64) -> AuthorityRegistrationResult {
    AuthorityRegistrationResult {
        session: Some(AuthoritySession {
            worker: Some(IDENTITY),
            session_id: session,
        }),
        protocol_version: PROTOCOL_VERSION,
        required_capabilities: REQUIRED_CAPABILITIES.to_vec(),
    }
}

struct Context {
    directory: TempDir,
    cas: Arc<FastSlowStore>,
    ac: Arc<MemoryStore>,
    history: Arc<MemoryStore>,
    worker: Arc<AuthorityWorker<RunningActionsManagerImpl>>,
}

impl Context {
    async fn new(namespaced: bool) -> Self {
        Self::with_slow_store(
            namespaced,
            Store::new(MemoryStore::new(&MemorySpec::default())),
            Duration::from_secs(10),
        )
        .await
    }

    async fn with_slow_store(namespaced: bool, slow: Store, download_timeout: Duration) -> Self {
        let directory = TempDir::new().unwrap();
        let filesystem = FilesystemSpec {
            content_path: directory
                .path()
                .join("content")
                .to_string_lossy()
                .into_owned(),
            temp_path: directory.path().join("temp").to_string_lossy().into_owned(),
            ..FilesystemSpec::default()
        };
        let fast: Arc<FilesystemStore> = FilesystemStore::new(&filesystem).await.unwrap();
        let cas = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(filesystem),
                slow: StoreSpec::Memory(MemorySpec::default()),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                bypass_dedup_threshold_bytes: 0,
            },
            Store::new(fast),
            slow,
        );
        let ac = MemoryStore::new(&MemorySpec::default());
        let history = MemoryStore::new(&MemorySpec::default());
        let actions = directory.path().join("actions");
        std::fs::create_dir(&actions).unwrap();
        let manager = Arc::new(
            RunningActionsManagerImpl::new(RunningActionsManagerArgs {
                root_action_directory: actions.to_string_lossy().into_owned(),
                execution_configuration: ExecutionConfiguration {
                    persistent_workers: PersistentWorkersSettings {
                        enabled: false,
                        ..PersistentWorkersSettings::default()
                    },
                    ..ExecutionConfiguration::default()
                },
                cas_store: cas.clone(),
                // Deliberately configure the legacy publication facility. The new
                // recipient must not use it, even when the old policy says upload.
                ac_store: Some(Store::new(ac.clone())),
                historical_store: Store::new(history.clone()),
                upload_action_result_config: &UploadActionResultConfig {
                    upload_ac_results_strategy: UploadCacheResultsStrategy::Everything,
                    upload_historical_results_strategy: Some(
                        UploadCacheResultsStrategy::Everything,
                    ),
                    success_message_template: "legacy history link must not appear".to_owned(),
                    ..UploadActionResultConfig::default()
                },
                max_action_timeout: Duration::from_secs(30),
                max_upload_timeout: Duration::from_secs(10),
                max_download_timeout: download_timeout,
                max_cleanup_wait: Duration::from_secs(10),
                max_cleanup_backoff: Duration::from_millis(10),
                timeout_handled_externally: false,
                directory_cache: None,
                active_input_leases: true,
                readonly_input_mounts: Vec::new(),
                use_namespaces: if namespaced {
                    UseNamespaces::Yes
                } else {
                    UseNamespaces::No
                },
            })
            .unwrap(),
        );
        let worker =
            AuthorityWorker::new(IDENTITY, false, resources(CAPACITY), 8, 64 * 1024, manager)
                .unwrap();
        Self {
            directory,
            cas,
            ac,
            history,
            worker,
        }
    }

    async fn request(&self) -> (Request, DigestInfo) {
        let marker = self.directory.path().join("executed");
        self.request_command(
            format!(
                "printf x >> '{}'; printf artifact > output; printf stdout; printf stderr >&2",
                marker.display()
            ),
            5,
        )
        .await
    }

    async fn request_command(&self, script: String, timeout_seconds: i64) -> (Request, DigestInfo) {
        self.request_with_inputs(script, timeout_seconds, Directory::default())
            .await
    }

    async fn request_with_inputs(
        &self,
        script: String,
        timeout_seconds: i64,
        inputs: Directory,
    ) -> (Request, DigestInfo) {
        let command = Command {
            arguments: vec!["/bin/sh".to_owned(), "-c".to_owned(), script],
            output_files: vec!["output".to_owned()],
            ..Command::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let root = serialize_and_upload_message(
            &inputs,
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let action = Action {
            command_digest: Some(command_digest.into()),
            input_root_digest: Some(root.into()),
            timeout: Some(prost_types::Duration {
                seconds: timeout_seconds,
                nanos: 0,
            }),
            platform: Some(Platform {
                properties: vec![
                    Property {
                        name: "cpu_count".to_owned(),
                        value: "1".to_owned(),
                    },
                    Property {
                        name: "memory_kb".to_owned(),
                        value: "1024".to_owned(),
                    },
                ],
            }),
            ..Action::default()
        };
        let action_digest = serialize_and_upload_message(
            &action,
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let execute = ExecuteRequest {
            instance_name: "bridge-test".to_owned(),
            action_digest: Some(action_digest.into()),
            digest_function: 1,
            skip_cache_lookup: true,
            ..ExecuteRequest::default()
        };
        let payload = RequestPayload::capture(
            &execute,
            Some(&RequestMetadata {
                tool_invocation_id: "bridge-invocation".to_owned(),
                ..RequestMetadata::default()
            }),
            &Timestamp {
                seconds: 1_800_000_000,
                nanos: 17,
            },
            "bridge-principal",
            &action.encode_to_vec(),
            &command.encode_to_vec(),
            64 * 1024,
        )
        .unwrap();
        (Request::from_payload(payload).unwrap(), action_digest)
    }

    async fn authority(&self) -> Singleton {
        Singleton::open(
            self.directory.path().join("authority.redb"),
            1,
            EPOCH,
            Limits::default(),
        )
        .await
        .unwrap()
    }
}

fn dispatch_command(effect: &Effect, charged: Resources) -> AuthorityCommand {
    let EffectKind::Dispatch {
        execution,
        attempt,
        worker,
        incarnation,
        reservation,
        charged: granted,
        request,
    } = &effect.kind
    else {
        panic!("expected committed dispatch");
    };
    assert_eq!(*granted, charged, "dispatch uses its original reservation");
    let facts = request.require_payload().unwrap().dispatch_facts().unwrap();
    AuthorityCommand {
        attempt: Some(AuthorityAttemptIdentity {
            worker: Some(AuthorityWorkerIdentity {
                cluster_epoch: effect.epoch.0,
                worker_id: worker.0,
                incarnation_id: incarnation.0,
            }),
            execution_id: execution.0,
            attempt_id: attempt.0,
        }),
        effect_id: effect.id.0,
        command: Some(authority_command::Command::Dispatch(AuthorityDispatch {
            reservation_id: reservation.0,
            reserved_resources: Some(resources(charged)),
            execute_request: Some(facts.execute_request),
            queued_timestamp: Some(facts.queued_timestamp),
            platform: Some(facts.platform),
            request_metadata: facts.request_metadata,
        })),
    }
}

async fn wait_candidate(
    worker: &AuthorityWorker<RunningActionsManagerImpl>,
) -> AuthorityAttemptReport {
    let mut changed = worker.subscribe();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(report) = worker
                .reconcile()
                .attempts
                .into_iter()
                .find(|report| report.result.is_some())
            {
                return report;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

// Every observation is bound to the full identity of its committed dispatch.
fn checked_target(
    report: &AuthorityAttemptReport,
    expected: &Effect,
) -> Option<AuthorityAttemptIdentity> {
    let EffectKind::Dispatch {
        execution,
        attempt,
        worker,
        incarnation,
        ..
    } = &expected.kind
    else {
        return None;
    };
    let expected_target = AuthorityAttemptIdentity {
        worker: Some(AuthorityWorkerIdentity {
            cluster_epoch: expected.epoch.0,
            worker_id: worker.0,
            incarnation_id: incarnation.0,
        }),
        execution_id: execution.0,
        attempt_id: attempt.0,
    };
    let target = report.attempt?;
    if target != expected_target || report.dispatch_effect_id != expected.id.0 {
        return None;
    }
    let identity = target.worker?;
    if identity != IDENTITY {
        return None;
    }
    Some(target)
}

fn completed(report: &AuthorityAttemptReport, expected: &Effect) -> Option<Event> {
    // Complete refunds the reservation, so candidate output alone is insufficient.
    if report.phase != AuthorityAttemptPhase::Quiescent as i32 {
        return None;
    }
    let target = checked_target(report, expected)?;
    let identity = target.worker?;
    let authority_attempt_report::Result::ExecuteResponse(response) = report.result.as_ref()?
    else {
        return None;
    };
    Some(Event::Complete {
        attempt: AttemptId(target.attempt_id),
        worker: WorkerId(identity.worker_id),
        incarnation: IncarnationId(identity.incarnation_id),
        result: ResultPayload::from_response(response, 64 * 1024).unwrap(),
    })
}

fn progressed(report: &AuthorityAttemptReport, expected: &Effect) -> Option<Event> {
    let target = checked_target(report, expected)?;
    let identity = target.worker?;
    let phase = match AuthorityAttemptPhase::try_from(report.phase).ok()? {
        AuthorityAttemptPhase::Preparing => Phase::Preparing,
        AuthorityAttemptPhase::Running => Phase::Running,
        AuthorityAttemptPhase::Uploading => Phase::Uploading,
        _ => return None,
    };
    Some(Event::Progress {
        attempt: AttemptId(target.attempt_id),
        worker: WorkerId(identity.worker_id),
        incarnation: IncarnationId(identity.incarnation_id),
        phase,
        progress: report.phase_progress,
    })
}

async fn wait_report(
    worker: &AuthorityWorker<RunningActionsManagerImpl>,
    predicate: impl Fn(&AuthorityAttemptReport) -> bool + Send + Sync,
) -> AuthorityAttemptReport {
    let mut changed = worker.subscribe();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(report) = worker.reconcile().attempts.into_iter().find(&predicate) {
                return report;
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

async fn admit(context: &Context, authority: &Singleton, request: Request) -> (Effect, Resources) {
    assert_eq!(
        authority.write(recorded(register(1))).await.unwrap().data,
        Reply::Applied
    );
    context.worker.negotiate(&negotiation(1)).unwrap();
    let replies = authority
        .write_batch(
            Batch::new([
                recorded(Event::Submit {
                    operation: OperationId(1),
                    request,
                }),
                recorded(Event::Match),
            ])
            .unwrap(),
        )
        .await
        .unwrap()
        .data
        .replies;
    assert!(matches!(
        replies.as_slice(),
        [Reply::Submitted { .. }, Reply::Matched { .. }]
    ));
    let state = authority.read().await.unwrap().view();
    assert_eq!(state.effects.len(), 1);
    assert!(!state.reservations[0].released);
    (state.effects[0].clone(), state.reservations[0].charged)
}

fn assert_once(path: &Path) {
    assert_eq!(std::fs::read(path.join("executed")).unwrap(), b"x");
}

#[nativelink_test]
async fn real_process_progress_renews_inactivity_but_not_execution_timeout_or_reconnect() {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!("SKIP: namespaces unavailable; process-progress bridge remains unqualified");
        return;
    }
    let context = Context::new(true).await;
    let (request, action_digest) = context
        .request_command("exec /bin/sleep 30".to_owned(), 30)
        .await;
    let authority = context.authority().await;
    let (effect, charged) = admit(&context, &authority, request).await;
    context
        .worker
        .command(1, dispatch_command(&effect, charged))
        .unwrap();
    let first = wait_report(&context.worker, |report| {
        report.phase == AuthorityAttemptPhase::Running as i32
    })
    .await;
    let target = checked_target(&first, &effect).unwrap();
    let attempt = AttemptId(target.attempt_id);
    assert_eq!(
        authority
            .write(recorded(Event::WorkerAccepted {
                attempt,
                worker: WorkerId(IDENTITY.worker_id),
                incarnation: IncarnationId(IDENTITY.incarnation_id),
            }))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    assert_eq!(
        authority
            .write(recorded(progressed(&first, &effect).unwrap()))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let initial = authority.read().await.unwrap().view().attempts[0];
    assert_eq!(initial.running_deadline, Some(30));
    assert_eq!(initial.deadline.at, PROGRESS_TIMEOUT);

    authority
        .write(recorded(Event::AdvanceTime {
            now: PROGRESS_TIMEOUT - 1,
        }))
        .await
        .unwrap();
    // A newer transport observation with identical semantic progress does not
    // renew the attempt, even when accompanied by a healthy worker heartbeat.
    let mut transport_only = first.clone();
    transport_only.observation_sequence += 1;
    assert_eq!(
        authority
            .write(recorded(progressed(&transport_only, &effect).unwrap()))
            .await
            .unwrap()
            .data,
        Reply::Ignored
    );
    authority
        .write(recorded(Event::Heartbeat {
            worker: WorkerId(IDENTITY.worker_id),
            incarnation: IncarnationId(IDENTITY.incarnation_id),
            session: SessionId(1),
        }))
        .await
        .unwrap();
    assert_eq!(
        authority.read().await.unwrap().view().attempts[0].deadline,
        initial.deadline
    );

    // Same-incarnation reconciliation replays the retained counter. Neither a
    // registration nor replaying the coordinator's last report is fresh work.
    context.worker.disconnect(1);
    authority.write(recorded(register(2))).await.unwrap();
    context.worker.negotiate(&negotiation(2)).unwrap();
    assert_eq!(
        authority
            .write(recorded(progressed(&first, &effect).unwrap()))
            .await
            .unwrap()
            .data,
        Reply::Ignored
    );
    assert_eq!(
        authority.read().await.unwrap().view().attempts[0].deadline,
        initial.deadline
    );

    // The next report comes from try_wait on the owned, still-unreaped child.
    let alive = wait_report(&context.worker, |report| {
        report.phase == AuthorityAttemptPhase::Running as i32
            && report.phase_progress > first.phase_progress
    })
    .await;
    assert_eq!(
        authority
            .write(recorded(progressed(&alive, &effect).unwrap()))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let renewed = authority.read().await.unwrap().view().attempts[0];
    assert!(renewed.deadline.at > initial.deadline.at);
    assert_eq!(renewed.running_deadline, initial.running_deadline);
    authority
        .write(recorded(Event::AdvanceTime { now: 30 }))
        .await
        .unwrap();
    assert_eq!(
        authority
            .write(recorded(Event::ExpireAttempt {
                attempt,
                generation: initial.deadline.generation,
            }))
            .await
            .unwrap()
            .data,
        Reply::Ignored
    );
    assert_eq!(
        authority
            .write(recorded(Event::ExpireExecution { attempt }))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let expired = authority.read().await.unwrap().view();
    assert!(!expired.attempts[0].authorized);
    assert!(!expired.reservations[0].released);
    assert_eq!(expired.workers[0].charged, charged);
    assert_ne!(
        authority
            .write(recorded(progressed(&alive, &effect).unwrap()))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let cancel = expired
        .effects
        .iter()
        .find(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
        .unwrap();
    context
        .worker
        .command(
            2,
            AuthorityCommand {
                attempt: Some(target),
                effect_id: cancel.id.0,
                command: Some(authority_command::Command::Cancel(())),
            },
        )
        .unwrap();
    let closed = wait_report(&context.worker, |report| {
        report.phase == AuthorityAttemptPhase::Quiescent as i32
    })
    .await;
    assert_eq!(checked_target(&closed, &effect), Some(target));
    assert_eq!(
        authority
            .write(recorded(Event::Cleanup {
                attempt,
                worker: WorkerId(IDENTITY.worker_id),
                incarnation: IncarnationId(IDENTITY.incarnation_id),
            }))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    assert_eq!(
        authority.read().await.unwrap().view().workers[0].charged,
        Resources::default()
    );
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_eq!(context.history.len_for_test(), 0);
    authority.shutdown().await.unwrap();
}

#[nativelink_test]
async fn lost_candidate_and_coordinator_restart_preserve_one_launch_and_accepted_publication() {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!(
            "SKIP: namespaces unavailable; combined quiescent acceptance remains unqualified"
        );
        return;
    }
    let context = Context::new(true).await;
    let (request, action_digest) = context.request().await;
    let authority = context.authority().await;
    let (effect, charged) = admit(&context, &authority, request).await;
    let command = dispatch_command(&effect, charged);
    context.worker.command(1, command.clone()).unwrap();
    let candidate = wait_candidate(&context.worker).await;
    assert!(completed(&candidate, &effect).is_some());
    let mut wrong_execution = candidate.clone();
    wrong_execution.attempt.as_mut().unwrap().execution_id += 1;
    assert!(completed(&wrong_execution, &effect).is_none());
    let mut wrong_dispatch = candidate.clone();
    wrong_dispatch.dispatch_effect_id += 1;
    assert!(completed(&wrong_dispatch, &effect).is_none());
    assert_once(context.directory.path());
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_eq!(context.history.len_for_test(), 0);
    let before = authority.read().await.unwrap();
    assert!(before.view().operations[0].terminal.is_none());
    // Drop the candidate/acceptance reply at the coordinator boundary. The
    // committed dispatch remains recoverable and the live recipient retains it.
    authority.shutdown().await.unwrap();
    context.worker.disconnect(1);
    let authority = context.authority().await;
    assert_eq!(authority.read().await.unwrap(), before);
    assert_eq!(
        authority.write(recorded(register(2))).await.unwrap().data,
        Reply::Applied
    );
    let inventory = context.worker.negotiate(&negotiation(2)).unwrap();
    assert_eq!(inventory.attempts, vec![candidate.clone()]);
    let recovered = authority.read().await.unwrap().view().effects.remove(0);
    assert_eq!(recovered, effect);
    assert_eq!(
        context
            .worker
            .command(2, dispatch_command(&recovered, charged))
            .unwrap(),
        candidate
    );
    assert!(context.worker.command(1, command).is_err());
    assert_once(context.directory.path());
    let completion = completed(&candidate, &effect).unwrap();
    let response = authority
        .write_batch(
            Batch::new([
                recorded(Event::WorkerAccepted {
                    attempt: AttemptId(1),
                    worker: WorkerId(2),
                    incarnation: IncarnationId(3),
                }),
                recorded(completion.clone()),
            ])
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.data.replies, [Reply::Applied, Reply::Applied]);
    let accepted = authority.read().await.unwrap();
    let view = accepted.view();
    assert!(view.reservations[0].released);
    assert_eq!(view.workers[0].charged, Resources::default());
    let Some(TerminalResult::Completed(result)) = &view.operations[0].terminal else {
        panic!("missing accepted result")
    };
    let full = result.require_response().unwrap().decode().unwrap();
    let Some(authority_attempt_report::Result::ExecuteResponse(original)) = &candidate.result
    else {
        panic!("missing worker candidate")
    };
    assert_eq!(&full, original);
    assert_eq!(full.result.as_ref().unwrap().exit_code, 0);
    assert_eq!(full.result.as_ref().unwrap().output_files.len(), 1);
    assert!(!full.message.contains("legacy history"));
    assert_eq!(view.effects.len(), 1);
    let publication = &view.effects[0];
    let EffectKind::PublishResult {
        result: published, ..
    } = &publication.kind
    else {
        panic!("missing accepted publication")
    };
    assert_eq!(published, result);
    // Acceptance and physical publication are separate durable cutpoints.
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    authority.shutdown().await.unwrap();
    let authority = context.authority().await;
    assert_eq!(authority.read().await.unwrap(), accepted);
    context
        .ac
        .update_oneshot(
            action_digest,
            full.result.as_ref().unwrap().encode_to_vec().into(),
        )
        .await
        .unwrap();
    assert_eq!(
        authority
            .write(recorded(Event::EffectOutcome {
                effect: publication.id,
                success: true
            }))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let terminal = authority.read().await.unwrap().view().operations[0].clone();
    assert!(matches!(
        authority.write(recorded(completion)).await.unwrap().data,
        Reply::Rejected(_)
    ));
    let final_view = authority.read().await.unwrap().view();
    assert_eq!(final_view.operations[0], terminal);
    assert_eq!(final_view.effects, Vec::<Effect>::new());
    assert_eq!(
        ActionResult::decode(
            context
                .ac
                .get_part_unchunked(action_digest, 0, None)
                .await
                .unwrap()
        )
        .unwrap(),
        full.result.unwrap()
    );
    assert_once(context.directory.path());
    assert_eq!(context.history.len_for_test(), 0);
    authority.shutdown().await.unwrap();
    eprintln!(
        "AUTHORITY_BRIDGE: real namespaced candidate/restart/acceptance/publication exercised"
    );
}

#[nativelink_test]
async fn uncertain_process_closure_cannot_complete_or_publish_and_cancel_stays_charged() {
    let context = Context::new(false).await;
    let (request, action_digest) = context.request().await;
    let authority = context.authority().await;
    let (effect, charged) = admit(&context, &authority, request).await;
    context
        .worker
        .command(1, dispatch_command(&effect, charged))
        .unwrap();
    let candidate = wait_candidate(&context.worker).await;
    assert!(matches!(
        candidate.result,
        Some(authority_attempt_report::Result::ExecuteResponse(_))
    ));
    assert_eq!(
        candidate.phase,
        AuthorityAttemptPhase::TerminationUncertain as i32
    );
    assert!(completed(&candidate, &effect).is_none());
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_eq!(
        authority
            .write(recorded(Event::Cancel {
                operation: OperationId(1)
            }))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let view = authority.read().await.unwrap().view();
    assert_eq!(view.operations[0].terminal, Some(TerminalResult::Cancelled));
    assert!(!view.reservations[0].released);
    assert_eq!(view.workers[0].charged, charged);
    assert_eq!(view.effects.len(), 1);
    let cancel = &view.effects[0];
    let EffectKind::Cancel { .. } = cancel.kind else {
        panic!("expected cancellation intent")
    };
    let after_cancel = context
        .worker
        .command(
            1,
            AuthorityCommand {
                attempt: candidate.attempt,
                effect_id: cancel.id.0,
                command: Some(authority_command::Command::Cancel(())),
            },
        )
        .unwrap();
    assert!(completed(&after_cancel, &effect).is_none());
    let retained = authority.read().await.unwrap();
    authority.shutdown().await.unwrap();
    let authority = context.authority().await;
    assert_eq!(authority.read().await.unwrap(), retained);
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_once(context.directory.path());
    assert_eq!(context.history.len_for_test(), 0);
    authority.shutdown().await.unwrap();
}

#[derive(MetricsComponent)]
struct ByteGateStore {
    inner: Store,
    gated: DigestInfo,
    permits: Semaphore,
    forwarded: AtomicU64,
}

#[async_trait]
impl StoreDriver for ByteGateStore {
    async fn post_init(self: Arc<Self>) -> Result<(), Error> {
        Ok(())
    }
    async fn has_with_results(
        self: Pin<&Self>,
        keys: &[StoreKey<'_>],
        results: &mut [Option<u64>],
    ) -> Result<(), Error> {
        self.inner
            .as_store_driver_pin()
            .has_with_results(keys, results)
            .await
    }
    async fn update(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        reader: DropCloserReadHalf,
        size: UploadSizeInfo,
    ) -> Result<u64, Error> {
        self.inner
            .as_store_driver_pin()
            .update(key, reader, size)
            .await
    }
    async fn get_part(
        self: Pin<&Self>,
        key: StoreKey<'_>,
        writer: &mut DropCloserWriteHalf,
        offset: u64,
        length: Option<u64>,
    ) -> Result<(), Error> {
        if key != self.gated.into() {
            return self
                .inner
                .as_store_driver_pin()
                .get_part(key, writer, offset, length)
                .await;
        }
        let data = nativelink_util::work_progress::scope(
            None,
            self.inner.get_part_unchunked(key, offset, length),
        )
        .await?;
        for chunk in data.chunks(256 * 1024) {
            self.permits.acquire().await.unwrap().forget();
            writer.send(Bytes::copy_from_slice(chunk)).await?;
            self.forwarded
                .fetch_add(chunk.len() as u64, Ordering::SeqCst);
        }
        writer.send_eof()
    }
    fn inner_store(&self, _key: Option<StoreKey>) -> &'_ dyn StoreDriver {
        self
    }
    fn as_any(&self) -> &(dyn core::any::Any + Sync + Send + 'static) {
        self
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn core::any::Any + Sync + Send + 'static> {
        self
    }
    fn register_remove_callback(self: Arc<Self>, _callback: RemoveCallback) -> Result<(), Error> {
        Ok(())
    }
}

default_health_status_indicator!(ByteGateStore);

#[nativelink_test]
async fn actual_single_blob_bytes_renew_preparing_beyond_lease_then_chatter_cannot_hide_stall() {
    use nativelink_store::ac_utils::compute_buf_digest;
    let data = Bytes::from(vec![19; 2 * 1024 * 1024]);
    let digest = compute_buf_digest(&data, &mut DigestHasherFunc::Sha256.hasher());
    let inner = Store::new(MemoryStore::new(&MemorySpec::default()));
    inner.update_oneshot(digest, data).await.unwrap();
    let gate = Arc::new(ByteGateStore {
        inner,
        gated: digest,
        permits: Semaphore::new(0),
        forwarded: AtomicU64::new(0),
    });
    let context =
        Context::with_slow_store(false, Store::new(gate.clone()), Duration::from_mins(2)).await;
    let marker = context.directory.path().join("unexpected-execution");
    let (request, _) = context
        .request_with_inputs(
            format!("touch '{}'", marker.display()),
            30,
            Directory {
                files: vec![FileNode {
                    name: "large-input".to_owned(),
                    digest: Some(digest.into()),
                    ..FileNode::default()
                }],
                ..Directory::default()
            },
        )
        .await;
    let authority = context.authority().await;
    let (effect, charged) = admit(&context, &authority, request).await;
    let dispatch = dispatch_command(&effect, charged);
    let target = dispatch.attempt.unwrap();
    let attempt = AttemptId(target.attempt_id);
    context.worker.command(1, dispatch.clone()).unwrap();
    authority
        .write(recorded(Event::WorkerAccepted {
            attempt,
            worker: WorkerId(IDENTITY.worker_id),
            incarnation: IncarnationId(IDENTITY.incarnation_id),
        }))
        .await
        .unwrap();
    let mut previous = 0;
    let mut last = None;
    for (index, now) in [0, 20, 40].into_iter().enumerate() {
        authority
            .write(recorded(Event::AdvanceTime { now }))
            .await
            .unwrap();
        gate.permits.add_permits(1);
        tokio::time::timeout(Duration::from_secs(5), async {
            while gate.forwarded.load(Ordering::SeqCst) < (index as u64 + 1) * 256 * 1024 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let progress = wait_report(&context.worker, |report| {
            report.phase == AuthorityAttemptPhase::Preparing as i32
                && report.phase_progress > previous
        })
        .await;
        assert_eq!(
            authority
                .write(recorded(progressed(&progress, &effect).unwrap()))
                .await
                .unwrap()
                .data,
            Reply::Applied
        );
        let state = authority.read().await.unwrap().view();
        assert!(state.attempts[0].authorized);
        assert_eq!(state.attempts[0].deadline.at, now + PROGRESS_TIMEOUT);
        assert_eq!(state.attempts[0].running_deadline, None);
        previous = progress.phase_progress;
        last = Some(progress);
    }
    assert!(
        authority.read().await.unwrap().view().now > PROGRESS_TIMEOUT,
        "the same blob outlives its original phase lease"
    );
    assert!(gate.forwarded.load(Ordering::SeqCst) < digest.size_bytes());
    assert!(!marker.exists());
    // Allow a full reporting interval for already forwarded bytes to settle
    // through the bounded filesystem fill. Thereafter no chunks are released.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let stable = context.worker.reconcile().attempts[0].clone();
    assert_eq!(stable.phase, AuthorityAttemptPhase::Preparing as i32);
    assert!(stable.phase_progress >= last.unwrap().phase_progress);
    authority
        .write(recorded(progressed(&stable, &effect).unwrap()))
        .await
        .unwrap();
    let deadline = authority.read().await.unwrap().view().attempts[0].deadline;
    let mut session = 1;
    for now in [deadline.at - 2, deadline.at - 1, deadline.at] {
        authority
            .write(recorded(Event::AdvanceTime { now }))
            .await
            .unwrap();
        context.worker.disconnect(session);
        session += 1;
        authority.write(recorded(register(session))).await.unwrap();
        let inventory = context.worker.negotiate(&negotiation(session)).unwrap();
        let replay = &inventory.attempts[0];
        assert_eq!(replay.phase_progress, stable.phase_progress);
        assert_eq!(
            authority
                .write(recorded(progressed(replay, &effect).unwrap()))
                .await
                .unwrap()
                .data,
            Reply::Ignored
        );
        let mut chatty = context.worker.command(session, dispatch.clone()).unwrap();
        chatty.observation_sequence += now;
        assert_eq!(
            authority
                .write(recorded(progressed(&chatty, &effect).unwrap()))
                .await
                .unwrap()
                .data,
            Reply::Ignored
        );
        let capacity = context
            .worker
            .make_capacity_observation(session, now + 1)
            .unwrap();
        assert_eq!(capacity.capacity, Some(resources(CAPACITY)));
        authority
            .write(recorded(Event::Heartbeat {
                worker: WorkerId(IDENTITY.worker_id),
                incarnation: IncarnationId(IDENTITY.incarnation_id),
                session: SessionId(session),
            }))
            .await
            .unwrap();
        assert_eq!(
            authority.read().await.unwrap().view().attempts[0].deadline,
            deadline
        );
    }
    assert_eq!(
        authority
            .write(recorded(Event::ExpireAttempt {
                attempt,
                generation: deadline.generation
            }))
            .await
            .unwrap()
            .data,
        Reply::Applied
    );
    let expired = authority.read().await.unwrap().view();
    assert!(!expired.attempts[0].authorized);
    assert!(!expired.reservations[0].released);
    assert_eq!(expired.workers[0].charged, charged);
    let cancel = expired
        .effects
        .iter()
        .find(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
        .unwrap();
    context
        .worker
        .command(
            session,
            AuthorityCommand {
                attempt: Some(target),
                effect_id: cancel.id.0,
                command: Some(authority_command::Command::Cancel(())),
            },
        )
        .unwrap();
    wait_report(&context.worker, |report| {
        report.phase == AuthorityAttemptPhase::Quiescent as i32
    })
    .await;
    assert!(
        !marker.exists(),
        "the canceled preparation never launches the process"
    );
    authority.shutdown().await.unwrap();
}

#[path = "authority_publication/cases.rs"]
mod publication;
