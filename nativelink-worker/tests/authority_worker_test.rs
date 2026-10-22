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

//! Linux subprocess evidence for the experimental recipient. These tests use
//! the real action manager and CAS, not a simulated launch counter.
#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use nativelink_config::cas_server::{UploadActionResultConfig, UploadCacheResultsStrategy};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use futures::stream;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, ExecuteRequest, Platform,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptIdentity, AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCommand,
    AuthorityCoordinatorUpdate, AuthorityDispatch, AuthorityRegistration, AuthorityRegistrationResult, AuthorityResources, AuthoritySession,
    AuthorityWorkerIdentity, AuthorityWorkerUpdate, authority_attempt_report, authority_command, authority_coordinator_update, authority_worker_update,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_client::AuthorityWorkerApiClient;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::{AuthorityWorkerApi, AuthorityWorkerApiServer};
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_util::spawn;
use nativelink_util::task::JoinHandleDropGuard;
use nativelink_worker::authority_worker::{
    AuthorityWorker, PROTOCOL_VERSION, REQUIRED_CAPABILITIES,
};
use nativelink_worker::namespace_utils;
use nativelink_worker::authority_worker_transport::run_session;
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, PersistentWorkersSettings, RunningActionsManagerArgs,
    RunningActionsManagerImpl, UseNamespaces,
};
use tempfile::TempDir;
use tokio::io::unix::AsyncFd;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};

const IDENTITY: AuthorityWorkerIdentity = AuthorityWorkerIdentity {
    cluster_epoch: 1,
    worker_id: 2,
    incarnation_id: 3,
};
const RESOURCES: AuthorityResources = AuthorityResources {
    cpu_millis: 100,
    memory_bytes: 1024,
    slots: 1,
};

struct Context {
    directory: TempDir,
    cas: Arc<FastSlowStore>,
    worker: Arc<AuthorityWorker<RunningActionsManagerImpl>>,
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

async fn setup(single_use: bool, slots: u64, limit: usize, namespaced: bool) -> Context {
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
    let slow = MemoryStore::new(&MemorySpec::default());
    let cas = FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(filesystem),
            slow: StoreSpec::Memory(MemorySpec::default()),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            bypass_dedup_threshold_bytes: 0,
        },
        Store::new(fast),
        Store::new(slow),
    );
    let root = directory.path().join("actions");
    std::fs::create_dir(&root).unwrap();
    let manager = Arc::new(
        RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            root_action_directory: root.to_string_lossy().into_owned(),
            execution_configuration: ExecutionConfiguration {
                persistent_workers: PersistentWorkersSettings {
                    enabled: false,
                    ..PersistentWorkersSettings::default()
                },
                ..ExecutionConfiguration::default()
            },
            cas_store: cas.clone(),
            ac_store: None,
            historical_store: Store::new(cas.clone()),
            upload_action_result_config: &UploadActionResultConfig {
                upload_ac_results_strategy: UploadCacheResultsStrategy::Never,
                ..UploadActionResultConfig::default()
            },
            max_action_timeout: Duration::from_secs(30),
            max_upload_timeout: Duration::from_secs(10),
            max_download_timeout: Duration::from_secs(10),
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
    let worker = AuthorityWorker::new(
        IDENTITY,
        single_use,
        AuthorityResources {
            cpu_millis: RESOURCES.cpu_millis * slots,
            memory_bytes: RESOURCES.memory_bytes * slots,
            slots,
        },
        limit,
        64 * 1024,
        manager,
    )
    .unwrap();
    Context {
        directory,
        cas,
        worker,
    }
}

impl Context {
    async fn dispatch(&self, attempt: u64, arguments: Vec<String>) -> AuthorityCommand {
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        let command = serialize_and_upload_message(
            &Command {
                arguments,
                ..Command::default()
            },
            self.cas.as_pin(),
            &mut hasher,
        )
        .await
        .unwrap();
        let root = serialize_and_upload_message(
            &Directory::default(),
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        let action = serialize_and_upload_message(
            &Action {
                command_digest: Some(command.into()),
                input_root_digest: Some(root.into()),
                ..Action::default()
            },
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await
        .unwrap();
        AuthorityCommand {
            attempt: Some(target(attempt)),
            effect_id: attempt * 2,
            command: Some(authority_command::Command::Dispatch(AuthorityDispatch {
                reservation_id: attempt,
                reserved_resources: Some(RESOURCES),
                execute_request: Some(ExecuteRequest {
                    action_digest: Some(action.into()),
                    digest_function: 1,
                    ..ExecuteRequest::default()
                }),
                platform: Some(Platform::default()),
                queued_timestamp: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                ..AuthorityDispatch::default()
            })),
        }
    }

    async fn sleeping(&self, attempt: u64) -> AuthorityCommand {
        let marker = self.directory.path().join(format!("spawn-{attempt}"));
        self.dispatch(
            attempt,
            vec![
                "/bin/sh".to_owned(),
                "-c".to_owned(),
                format!("printf x >> '{}'; exec /bin/sleep 30", marker.display()),
            ],
        )
        .await
    }
}

const fn target(attempt: u64) -> AuthorityAttemptIdentity {
    AuthorityAttemptIdentity {
        worker: Some(IDENTITY),
        execution_id: 100,
        attempt_id: attempt,
    }
}

const fn cancel(attempt: u64) -> AuthorityCommand {
    AuthorityCommand {
        attempt: Some(target(attempt)),
        effect_id: attempt * 2 + 1,
        command: Some(authority_command::Command::Cancel(())),
    }
}

async fn wait_marker(path: &Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_result(
    worker: &AuthorityWorker<RunningActionsManagerImpl>,
    attempt: u64,
) -> AuthorityAttemptReport {
    let mut changes = worker.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(report) = worker
                .reconcile()
                .attempts
                .into_iter()
                .find(|report| report.attempt.unwrap().attempt_id == attempt)
                && report.result.is_some()
            {
                return report;
            }
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

#[nativelink_test]
async fn incompatible_protocol_and_unnegotiated_commands_are_rejected() {
    let context = setup(false, 1, 4, false).await;
    let registration = context.worker.make_registration(1, Vec::new()).unwrap();
    assert_eq!(registration.session.unwrap().worker, Some(IDENTITY));
    assert_eq!(registration.capacity, Some(RESOURCES));
    assert_eq!(registration.admission_ceiling, Some(RESOURCES));
    assert!(registration.reconciliation.unwrap().attempts.is_empty());
    let command = context.sleeping(1).await;
    assert!(context.worker.command(1, command).is_err());
    let mut incompatible = negotiation(1);
    incompatible.required_capabilities.pop();
    assert!(context.worker.negotiate(&incompatible).is_err());
    let mut incompatible = negotiation(1);
    incompatible.protocol_version += 1;
    assert!(context.worker.negotiate(&incompatible).is_err());
    let mut incompatible = negotiation(1);
    incompatible
        .session
        .as_mut()
        .unwrap()
        .worker
        .as_mut()
        .unwrap()
        .incarnation_id += 1;
    assert!(context.worker.negotiate(&incompatible).is_err());
    assert!(context.worker.reconcile().attempts.is_empty());
    context.worker.negotiate(&negotiation(1)).unwrap();
    assert!(context.worker.make_registration(1, Vec::new()).is_err());
}

#[nativelink_test]
async fn duplicate_real_launch_and_lost_receipt_survive_reconnect_without_relaunch() {
    let context = setup(false, 1, 4, false).await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    context.worker.lose_next_launch_receipt();
    let command = context.sleeping(1).await;
    context.worker.command(1, command.clone()).unwrap();
    wait_marker(&context.directory.path().join("spawn-1")).await;
    assert_eq!(
        context.worker.reconcile().attempts[0].phase,
        AuthorityAttemptPhase::LaunchUncertain as i32
    );
    context.worker.disconnect(1);
    let replay = context.worker.negotiate(&negotiation(2)).unwrap();
    assert!(!replay.single_use_spent);
    context.worker.command(2, command).unwrap();
    assert!(context.worker.command(1, cancel(1)).is_err());
    assert_eq!(
        std::fs::read(context.directory.path().join("spawn-1")).unwrap(),
        b"x"
    );
    context.worker.command(2, cancel(1)).unwrap();
    let report = wait_result(&context.worker, 1).await;
    assert_eq!(
        report.phase,
        AuthorityAttemptPhase::TerminationUncertain as i32,
        "unisolated directory cleanup cannot prove all descendants stopped"
    );
}

#[nativelink_test]
async fn reordered_cancel_closes_recipient_and_stale_cancel_cannot_kill_successor() {
    let context = setup(false, 2, 8, false).await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    let tombstone = context.worker.command(1, cancel(1)).unwrap();
    let delayed = context.sleeping(1).await;
    let after = context.worker.command(1, delayed).unwrap();
    assert_eq!(after.phase, AuthorityAttemptPhase::Quiescent as i32);
    assert!(after.observation_sequence > tombstone.observation_sequence);
    assert!(!context.directory.path().join("spawn-1").exists());
    context
        .worker
        .command(1, context.sleeping(2).await)
        .unwrap();
    wait_marker(&context.directory.path().join("spawn-2")).await;
    let duplicate = context.worker.command(1, cancel(1)).unwrap();
    assert_eq!(duplicate, after);
    assert!(
        context
            .worker
            .reconcile()
            .attempts
            .iter()
            .find(|report| report.attempt.unwrap().attempt_id == 2)
            .unwrap()
            .result
            .is_none()
    );
    context.worker.command(1, cancel(2)).unwrap();
    wait_result(&context.worker, 2).await;
}

#[nativelink_test]
async fn uncertain_termination_stays_charged_and_decline_installs_tombstone() {
    let context = setup(false, 1, 8, false).await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    context
        .worker
        .command(1, context.dispatch(1, vec!["/bin/true".to_owned()]).await)
        .unwrap();
    let first = wait_result(&context.worker, 1).await;
    assert_eq!(
        first.phase,
        AuthorityAttemptPhase::TerminationUncertain as i32
    );
    let second = context.sleeping(2).await;
    let declined = context.worker.command(1, second.clone()).unwrap();
    assert!(matches!(
        declined.result,
        Some(authority_attempt_report::Result::Decline(_))
    ));
    assert_eq!(declined.phase, AuthorityAttemptPhase::Quiescent as i32);
    assert_eq!(context.worker.command(1, second).unwrap(), declined);
    assert!(!context.directory.path().join("spawn-2").exists());
}

#[nativelink_test]
async fn real_spawn_failure_is_retained_and_single_use_spent_survives_reconnect() {
    let context = setup(true, 1, 4, false).await;
    let executable = context.directory.path().join("invalid-executable");
    std::fs::write(&executable, b"this is not an executable format").unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    context.worker.negotiate(&negotiation(1)).unwrap();
    let command = context
        .dispatch(1, vec![executable.to_string_lossy().into_owned()])
        .await;
    context.worker.command(1, command.clone()).unwrap();
    let failed = wait_result(&context.worker, 1).await;
    assert!(matches!(
        failed.result,
        Some(authority_attempt_report::Result::InternalError(_))
    ));
    assert_eq!(failed.phase, AuthorityAttemptPhase::Quiescent as i32);
    context.worker.disconnect(1);
    assert!(
        context
            .worker
            .negotiate(&negotiation(2))
            .unwrap()
            .single_use_spent
    );
    assert_eq!(context.worker.command(2, command).unwrap(), failed);
    let declined = context
        .worker
        .command(2, context.sleeping(2).await)
        .unwrap();
    assert!(matches!(
        declined.result,
        Some(authority_attempt_report::Result::Decline(_))
    ));
    let cancelled = context.worker.command(2, cancel(1)).unwrap();
    assert!(cancelled.observation_sequence > failed.observation_sequence);
}

#[nativelink_test]
async fn bounded_ledger_never_evicts_cleanup_tombstones() {
    let context = setup(false, 1, 1, false).await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    context.worker.command(1, cancel(1)).unwrap();
    assert!(context.worker.admission_closed());
    assert_eq!(
        context
            .worker
            .make_registration(2, Vec::new())
            .unwrap()
            .admission_ceiling,
        Some(AuthorityResources::default())
    );
    assert_eq!(
        context.worker.command(1, cancel(2)).unwrap_err().code,
        Code::ResourceExhausted
    );
    let delayed = context.sleeping(1).await;
    assert_eq!(
        context.worker.command(1, delayed).unwrap().phase,
        AuthorityAttemptPhase::Quiescent as i32
    );
    assert_eq!(context.worker.reconcile().attempts.len(), 1);
}

#[nativelink_test]
async fn normal_pid_namespace_exit_provides_domain_quiescence() {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!("SKIP: PID/user namespaces unavailable; domain quiescence remains unqualified");
        return;
    }
    let context = setup(false, 1, 4, true).await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    // The descendant leaves the action's process group. Only the namespace
    // closure, not a PGID signal or directory removal, can stop its writes.
    let marker = context.directory.path().join("detached-heartbeat");
    let setsid = which::which("setsid").unwrap();
    let script = format!(
        "'{}' /bin/sh -c 'i=0; while [ $i -lt 100 ]; do printf x >> \"{}\"; i=$((i+1)); /bin/sleep .01; done' </dev/null >/dev/null 2>&1 & while [ ! -s '{}' ]; do /bin/sleep .01; done",
        setsid.display(),
        marker.display(),
        marker.display()
    );
    context
        .worker
        .command(
            1,
            context
                .dispatch(1, vec!["/bin/sh".to_owned(), "-c".to_owned(), script])
                .await,
        )
        .unwrap();
    let report = wait_result(&context.worker, 1).await;
    assert_eq!(report.phase, AuthorityAttemptPhase::Quiescent as i32);
    let stopped = std::fs::read(&marker).unwrap();
    assert!(!stopped.is_empty());
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(std::fs::read(&marker).unwrap(), stopped);
    context
        .worker
        .command(1, context.dispatch(2, vec!["/bin/true".to_owned()]).await)
        .unwrap();
    assert_eq!(
        wait_result(&context.worker, 2).await.phase,
        AuthorityAttemptPhase::Quiescent as i32
    );
}

#[nativelink_test]
async fn hard_killed_namespace_waits_for_inner_domain_before_reusing_capacity() {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!("SKIP: PID/user namespaces unavailable; hard-kill closure remains unqualified");
        return;
    }
    let context = setup(false, 1, 4, true).await;
    context.worker.negotiate(&negotiation(1)).unwrap();
    let marker = context
        .directory
        .path()
        .join("hard-kill-detached-heartbeat");
    let setsid = which::which("setsid").unwrap();
    let script = format!(
        "'{}' /bin/sh -c 'i=0; while [ $i -lt 3000 ]; do printf x >> \"{}\"; i=$((i+1)); /bin/sleep .01; done' </dev/null >/dev/null 2>&1 & wait",
        setsid.display(),
        marker.display()
    );
    context
        .worker
        .command(
            1,
            context
                .dispatch(1, vec!["/bin/sh".to_owned(), "-c".to_owned(), script])
                .await,
        )
        .unwrap();
    wait_marker(&marker).await;
    context.worker.command(1, cancel(1)).unwrap();
    assert_eq!(
        wait_result(&context.worker, 1).await.phase,
        AuthorityAttemptPhase::Quiescent as i32
    );
    let stopped = std::fs::read(&marker).unwrap();
    assert!(!stopped.is_empty());
    context
        .worker
        .command(1, context.sleeping(2).await)
        .unwrap();
    wait_marker(&context.directory.path().join("spawn-2")).await;
    // A delayed cancel addresses the old exact domain after the slot is reused.
    context.worker.command(1, cancel(1)).unwrap();
    context.worker.command(1, cancel(2)).unwrap();
    assert_eq!(
        wait_result(&context.worker, 2).await.phase,
        AuthorityAttemptPhase::Quiescent as i32
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(std::fs::read(&marker).unwrap(), stopped);
}

/// A real child writes the first FIFO after entering the action and waits for
/// the second FIFO before returning. Nonblocking parent descriptors keep test
/// cancellation from stranding a blocking open/read in the runtime's I/O pool.
struct ProcessWitness {
    started: AsyncFd<File>,
    release: File,
    launches: PathBuf,
    finished: PathBuf,
}

fn fifo(path: &Path) -> File {
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: `name` is a valid NUL-terminated path; mkfifo retains no pointer.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .unwrap()
}

impl ProcessWitness {
    async fn new(context: &Context, attempt: u64) -> (Self, AuthorityCommand) {
        let started_path = context
            .directory
            .path()
            .join(format!("wire-started-{attempt}"));
        let release_path = context
            .directory
            .path()
            .join(format!("wire-release-{attempt}"));
        let launches = context
            .directory
            .path()
            .join(format!("wire-launches-{attempt}"));
        let finished = context
            .directory
            .path()
            .join(format!("wire-finished-{attempt}"));
        let witness = Self {
            started: AsyncFd::new(fifo(&started_path)).unwrap(),
            release: fifo(&release_path),
            launches,
            finished,
        };
        let script = format!(
            "printf x >> '{}'; printf x > '{}'; read token < '{}'; printf x >> '{}'",
            witness.launches.display(),
            started_path.display(),
            release_path.display(),
            witness.finished.display()
        );
        let command = context
            .dispatch(attempt, vec!["/bin/sh".to_owned(), "-c".to_owned(), script])
            .await;
        (witness, command)
    }

    async fn started(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut ready = self.started.readable().await.unwrap();
                if let Ok(result) = ready.try_io(|fd| {
                    let mut byte = [0];
                    let mut file = fd.get_ref();
                    file.read_exact(&mut byte).map(|()| byte[0])
                }) {
                    assert_eq!(result.unwrap(), b'x');
                    return;
                }
            }
        })
        .await
        .unwrap();
    }

    fn finish(&self) {
        let mut file = &self.release;
        file.write_all(b"continue\n").unwrap();
    }
}

struct WireConnection {
    requests: Streaming<AuthorityWorkerUpdate>,
    responses: mpsc::Sender<Result<AuthorityCoordinatorUpdate, Status>>,
    reports: BTreeMap<u64, AuthorityAttemptReport>,
}

#[derive(Clone)]
struct Coordinator {
    connections: mpsc::Sender<WireConnection>,
}

#[tonic::async_trait]
impl AuthorityWorkerApi for Coordinator {
    type ConnectStream = ReceiverStream<Result<AuthorityCoordinatorUpdate, Status>>;

    async fn connect(
        &self,
        request: Request<Streaming<AuthorityWorkerUpdate>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let (responses, stream) = mpsc::channel(8);
        self.connections
            .send(WireConnection {
                requests: request.into_inner(),
                responses,
                reports: BTreeMap::new(),
            })
            .await
            .map_err(|_| Status::unavailable("Test coordinator closed"))?;
        Ok(Response::new(ReceiverStream::new(stream)))
    }
}

struct WireServer {
    client: AuthorityWorkerApiClient<Channel>,
    connections: mpsc::Receiver<WireConnection>,
    _server: JoinHandleDropGuard<Result<(), tonic::transport::Error>>,
}

impl WireServer {
    async fn new() -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = stream::unfold(listener, |listener| async {
            Some((
                listener.accept().await.map(|(connection, _)| connection),
                listener,
            ))
        });
        let (connections, receiver) = mpsc::channel(4);
        let service = AuthorityWorkerApiServer::new(Coordinator { connections });
        let server = spawn!(
            "authority_wire_test_server",
            Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
        );
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self {
            client: AuthorityWorkerApiClient::new(channel),
            connections: receiver,
            _server: server,
        }
    }

    async fn connect(
        &mut self,
        worker: Arc<AuthorityWorker<RunningActionsManagerImpl>>,
        session: u64,
    ) -> (WireConnection, JoinHandleDropGuard<Result<(), Error>>) {
        let task = spawn!(
            "authority_wire_test_worker",
            run_session(worker, self.client.clone(), session, Vec::new())
        );
        let connection = tokio::time::timeout(Duration::from_secs(10), self.connections.recv())
            .await
            .unwrap()
            .unwrap();
        (connection, task)
    }
}

impl WireConnection {
    async fn next(&mut self) -> authority_worker_update::Update {
        tokio::time::timeout(Duration::from_secs(10), self.requests.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .update
            .unwrap()
    }

    async fn registration(&mut self) -> AuthorityRegistration {
        let authority_worker_update::Update::Registration(registration) = self.next().await else {
            panic!("first worker message was not registration");
        };
        registration
    }

    async fn send(&self, update: authority_coordinator_update::Update) {
        self.responses
            .send(Ok(AuthorityCoordinatorUpdate {
                update: Some(update),
            }))
            .await
            .unwrap();
    }

    async fn negotiate(&mut self, session: u64) {
        self.send(authority_coordinator_update::Update::Registration(
            negotiation(session),
        ))
        .await;
        loop {
            if let authority_worker_update::Update::Reconciliation(inventory) = self.next().await {
                assert_eq!(inventory.session.unwrap().session_id, session);
                for report in inventory.attempts {
                    self.reports
                        .insert(report.attempt.unwrap().attempt_id, report);
                }
                return;
            }
        }
    }

    async fn report(
        &mut self,
        attempt: u64,
        predicate: impl Fn(&AuthorityAttemptReport) -> bool + Send + Sync,
    ) -> AuthorityAttemptReport {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(report) = self.reports.get(&attempt)
                    && predicate(report)
                {
                    return report.clone();
                }
                let reports = match self.next().await {
                    authority_worker_update::Update::Report(report) => vec![report],
                    authority_worker_update::Update::Reconciliation(inventory) => {
                        inventory.attempts
                    }
                    authority_worker_update::Update::Capacity(_) => continue,
                    authority_worker_update::Update::Registration(_) => {
                        panic!("unexpected second registration")
                    }
                };
                for report in reports {
                    self.reports
                        .insert(report.attempt.unwrap().attempt_id, report);
                }
            }
        })
        .await
        .unwrap()
    }
}

async fn session_error(task: JoinHandleDropGuard<Result<(), Error>>) -> Error {
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err()
}

#[nativelink_test]
async fn wire_rejects_missing_handshake_and_wrong_capabilities_before_launch() {
    let mut server = WireServer::new().await;
    for invalid_handshake in 0..3 {
        let context = setup(false, 1, 4, false).await;
        let (witness, command) = ProcessWitness::new(&context, 1).await;
        let (mut connection, task) = server.connect(context.worker.clone(), 1).await;
        let registration = connection.registration().await;
        assert_eq!(registration.session.unwrap().worker, Some(IDENTITY));
        if invalid_handshake != 0 {
            let mut unsupported = negotiation(1);
            if invalid_handshake == 1 {
                unsupported.required_capabilities.pop();
            } else {
                unsupported.session.as_mut().unwrap().session_id = 2;
            }
            connection
                .send(authority_coordinator_update::Update::Registration(
                    unsupported,
                ))
                .await;
        }
        connection
            .send(authority_coordinator_update::Update::Command(command))
            .await;
        assert_eq!(session_error(task).await.code, Code::FailedPrecondition);
        assert!(context.worker.reconcile().attempts.is_empty());
        assert!(!witness.launches.exists());
    }
}

#[nativelink_test]
async fn wire_lost_reply_reconnects_same_process_and_retains_uncertain_candidate() {
    let context = setup(false, 1, 8, false).await;
    let mut server = WireServer::new().await;
    let (witness, command) = ProcessWitness::new(&context, 1).await;
    let (mut first, first_task) = server.connect(context.worker.clone(), 1).await;
    first.registration().await;
    first.negotiate(1).await;
    // Refusing a repeated session before opening another RPC must not run a
    // disconnect guard against the already healthy connection.
    assert_eq!(
        run_session(context.worker.clone(), server.client.clone(), 1, Vec::new())
            .await
            .unwrap_err()
            .code,
        Code::FailedPrecondition
    );
    first
        .send(authority_coordinator_update::Update::Command(
            command.clone(),
        ))
        .await;
    witness.started().await;
    // Lose the worker's response after a physical launch, without observing any
    // report that might let this fake coordinator infer delivery success.
    first
        .responses
        .send(Err(Status::unavailable("Injected lost response")))
        .await
        .unwrap();
    drop(first);
    assert_eq!(session_error(first_task).await.code, Code::Unavailable);
    assert!(context.worker.reconcile().attempts[0].result.is_none());

    let (mut second, second_task) = server.connect(context.worker.clone(), 2).await;
    let registration = second.registration().await;
    let inventory = registration.reconciliation.unwrap();
    assert_eq!(inventory.attempts.len(), 1);
    assert_eq!(inventory.attempts[0].attempt, Some(target(1)));
    second.negotiate(2).await;
    second
        .send(authority_coordinator_update::Update::Command(command))
        .await;
    second
        .report(1, |report| report.dispatch_effect_id == 2)
        .await;
    witness.finish();
    let candidate = second.report(1, |report| report.result.is_some()).await;
    assert_eq!(
        candidate.phase,
        AuthorityAttemptPhase::TerminationUncertain as i32
    );
    assert!(matches!(
        candidate.result,
        Some(authority_attempt_report::Result::ExecuteResponse(_))
    ));
    assert_eq!(std::fs::read(&witness.launches).unwrap(), b"x");
    assert_eq!(std::fs::read(&witness.finished).unwrap(), b"x");
    // Receipt delivery is not acceptance or physical cleanup: the original
    // charge still prevents a second attempt on this one-slot recipient.
    second
        .send(authority_coordinator_update::Update::Command(
            context.sleeping(2).await,
        ))
        .await;
    let declined = second.report(2, |report| report.result.is_some()).await;
    assert!(matches!(
        declined.result,
        Some(authority_attempt_report::Result::Decline(_))
    ));
    assert_eq!(context.worker.reconcile().attempts[0], candidate);
    second
        .responses
        .send(Err(Status::unavailable("Test complete")))
        .await
        .unwrap();
    session_error(second_task).await;
}

#[nativelink_test]
async fn wire_old_session_cancel_cannot_target_a_successor() {
    let context = setup(false, 2, 8, false).await;
    let mut server = WireServer::new().await;
    let (first_witness, first_command) = ProcessWitness::new(&context, 1).await;
    let (second_witness, second_command) = ProcessWitness::new(&context, 2).await;
    let (mut first, first_task) = server.connect(context.worker.clone(), 1).await;
    first.registration().await;
    first.negotiate(1).await;
    first
        .send(authority_coordinator_update::Update::Command(first_command))
        .await;
    first_witness.started().await;

    let (mut second, second_task) = server.connect(context.worker.clone(), 2).await;
    second.registration().await;
    second.negotiate(2).await;
    // A pending observation may already have fenced the old transport. If it
    // is still open, the delayed command must be rejected at the recipient.
    // Either path's scope guard must leave the newer session connected.
    let _delayed = first
        .responses
        .send(Ok(AuthorityCoordinatorUpdate {
            update: Some(authority_coordinator_update::Update::Command(cancel(1))),
        }))
        .await;
    assert_eq!(
        session_error(first_task).await.code,
        Code::FailedPrecondition
    );
    second
        .send(authority_coordinator_update::Update::Command(
            second_command,
        ))
        .await;
    second_witness.started().await;
    first_witness.finish();
    second_witness.finish();
    second.report(1, |report| report.result.is_some()).await;
    second.report(2, |report| report.result.is_some()).await;
    assert_eq!(std::fs::read(first_witness.finished).unwrap(), b"x");
    assert_eq!(std::fs::read(second_witness.finished).unwrap(), b"x");
    second
        .responses
        .send(Err(Status::unavailable("Test complete")))
        .await
        .unwrap();
    session_error(second_task).await;
}
