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

//! Experimental, test-constructed RPC coordinator. No production factory mounts
//! this service. The injected identity, stores, clock and fault gates are local
//! qualification fixtures, not a deployment or authentication interface.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::{Stream, stream};
use nativelink_error::{Error, make_err};
use nativelink_util::buf_channel::make_buf_channel_pair;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::Execution;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Digest, ExecuteOperationMetadata, ExecuteRequest, ExecuteResponse,
    RequestMetadata, WaitExecutionRequest, digest_function,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::authority_worker_api_server::AuthorityWorkerApi;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    AuthorityAttemptIdentity, AuthorityAttemptPhase, AuthorityAttemptReport, AuthorityCapability,
    AuthorityCapacityObservation, AuthorityReconciliation,
    AuthorityCommand, AuthorityCoordinatorUpdate, AuthorityDispatch, AuthorityRegistration,
    AuthorityRegistrationResult, AuthorityResources, AuthoritySession, AuthorityWorkerIdentity,
    AuthorityReceiptCertificate, AuthorityReceiptRetirement, AuthorityRetainedReceipt,
    AuthorityWorkerUpdate, authority_attempt_report, authority_command,
    authority_coordinator_update, authority_worker_update,
};
use nativelink_proto::google::longrunning::{Operation, operation};
use nativelink_scheduler::authority::journal::Singleton;
use nativelink_scheduler::authority::accepted_history::{
    AcceptedHistory, HistoryAlgorithm, HistoryAssociation, HistoryObject, HistoryOutcome, HistoryStatus,
};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc, make_ctx_for_hash_func};
use opentelemetry::context::FutureExt;
use nativelink_scheduler::authority::request::RequestPayload;
use nativelink_scheduler::authority::{
    ActionKey, AttemptId, AttemptView, Effect, EffectId, EffectKind, Event, ExecutionId, IncarnationId, ObservableState,
    OperationId, Phase, RecordedEvent, Reply, Request as AuthorityRequest, Resources,
    ResultPayload, SessionId, INTEREST_TIMEOUT, TERMINAL_RETENTION, TerminalResult, WorkerId,
    ReceiptBinding, ReceiptCertificate, ReceiptRetirement, ReservationId, MAX_RECEIPT_BINDINGS,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::background_spawn;
use nativelink_util::store_trait::{Store, StoreLike};
use parking_lot::Mutex;
use prost::Message;
use prost_types::{Any, Timestamp};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, interval};
use tonic::{Request, Response, Status, Streaming};

mod operations;

const FRAME_BYTES: usize = 64 * 1024;
const OPERATION_BYTES: usize = 2 * FRAME_BYTES;
const INTEREST_RENEWAL_TICKS: u64 = INTEREST_TIMEOUT / 3;
const QUEUE: usize = 64;
const VERSION: u32 = 2;
const CAPABILITIES: [i32; 7] = [
    AuthorityCapability::AttemptEffectIdentities as i32,
    AuthorityCapability::ReconnectReconciliation as i32,
    AuthorityCapability::CleanupTombstones as i32,
    AuthorityCapability::CandidateResults as i32,
    AuthorityCapability::MeaningfulProgress as i32,
    AuthorityCapability::ReceiptRetirement as i32,
    AuthorityCapability::ObservedMemoryBreach as i32,
];
const REQUEST_METADATA: &str = "build.bazel.remote.execution.v2.requestmetadata-bin";

type OperationStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send>>;
type WorkerStream = Pin<Box<dyn Stream<Item = Result<AuthorityCoordinatorUpdate, Status>> + Send>>;
type ReplyTo<T> = oneshot::Sender<Result<T, Status>>;
type WorkerSender = mpsc::Sender<Result<AuthorityCoordinatorUpdate, Status>>;

/// Explicit local cutpoints. These gates never manufacture accepted evidence.
#[derive(Debug, Default)]
pub struct QualificationFaults {
    pub hold_completions: AtomicBool,
    pub hold_dispatches: AtomicBool,
    pub hold_publications: AtomicBool,
    pub hold_history_publications: AtomicBool,
    pub candidate_reports: AtomicU64,
    pub hold_receipt_issues: AtomicBool,
    pub hold_receipt_delivery: AtomicBool,
    pub hold_receipt_acknowledgments: AtomicBool,
    pub receipt_inventories: AtomicU64,
}

/// Fixed test construction facts; not a runtime configuration selector.
pub struct QualificationConfig {
    pub instance: String,
    pub principal: String,
    pub cas: Store,
    /// Explicit logical binding for accepted diagnostics in this CAS role.
    /// This fixture fact does not certify provider placement or protection.
    pub cas_namespace: [u8; 32],
    pub publication_sink: Store,
    /// Fallible experimental ticks. An observation error stops the coordinator;
    /// it never fabricates a time advance, expiry or reservation release.
    /// The caller still owns the clock-unit and recovery binding.
    pub now: Arc<dyn Fn() -> Result<u64, String> + Send + Sync>,
    pub faults: Arc<QualificationFaults>,
}

#[derive(Clone, Debug)]
pub struct AuthorityService {
    commands: mpsc::Sender<Input>,
    committed: watch::Receiver<Arc<ObservableState>>,
    receipts: watch::Receiver<Arc<Option<ReceiptRetirement>>>,
    history: watch::Receiver<Arc<Option<AcceptedHistory>>>,
    instance: Arc<str>,
    principal: Arc<str>,
    cas: Store,
    permits: Arc<Semaphore>,
}

#[derive(Debug)]
pub struct CoordinatorTask {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), String>>,
}

impl CoordinatorTask {
    pub async fn shutdown(self) -> Result<(), String> {
        let _ignored = self.stop.send(());
        self.task.await.map_err(|error| error.to_string())?
    }
}

enum Input {
    Submit(Box<AuthorityRequest>, ReplyTo<OperationId>),
    Observe(OperationId, ReplyTo<()>),
    Cancel(OperationId, ReplyTo<()>),
    RenewInterest(OperationId, ReplyTo<()>),
    Register(
        Box<AuthorityRegistration>,
        WorkerSender,
        oneshot::Sender<()>,
        ReplyTo<()>,
    ),
    Worker(AuthoritySession, Box<AuthorityWorkerUpdate>),
    WorkerSnapshots(AuthoritySession, Arc<Mutex<WorkerSnapshots>>),
    Disconnected(AuthoritySession),
}

/// Complete inventories retain unresolved receipts. Coalesce them again after
/// transport decoding, before a slow journal can queue many obsolete copies.
/// Taking the snapshot and clearing its notification share the same lock as
/// replacement, so an observation arriving during a commit queues another turn.
#[derive(Default)]
struct WorkerSnapshots {
    inventory: Option<AuthorityReconciliation>,
    capacity: Option<AuthorityCapacityObservation>,
    queued: bool,
}

fn worker_input(
    session: AuthoritySession,
    snapshots: &mut Arc<Mutex<WorkerSnapshots>>,
    update: AuthorityWorkerUpdate,
) -> Option<Input> {
    if update.encoded_len() > 1024 * 1024 {
        return None;
    }
    let mut pending = snapshots.lock();
    match update.update {
        Some(authority_worker_update::Update::Reconciliation(inventory)) => {
            if inventory.session != Some(session) {
                return None;
            }
            pending.inventory = Some(inventory);
        }
        Some(authority_worker_update::Update::Capacity(capacity)) => {
            if capacity.session != Some(session) {
                return None;
            }
            pending.capacity = Some(capacity);
        }
        _ => {
            // A standalone report is an ordering barrier. Later snapshots must
            // not replace an earlier notification and overtake that report.
            drop(pending);
            *snapshots = Arc::new(Mutex::new(WorkerSnapshots::default()));
            return Some(Input::Worker(session, Box::new(update)));
        }
    }
    if pending.queued {
        return None;
    }
    pending.queued = true;
    Some(Input::WorkerSnapshots(session, snapshots.clone()))
}

struct Observation {
    receiver: watch::Receiver<Arc<ObservableState>>,
    _permit: OwnedSemaphorePermit,
    previous: Option<Operation>,
    heartbeat_at: u64,
}

struct Connection {
    session: AuthoritySession,
    sender: WorkerSender,
    capacity_sequence: u64,
    _stop: oneshot::Sender<()>,
}

struct Driver {
    authority: Singleton,
    state: Arc<ObservableState>,
    watermark: OperationId,
    committed: watch::Sender<Arc<ObservableState>>,
    config: QualificationConfig,
    workers: BTreeMap<WorkerId, Connection>,
    sent: BTreeMap<EffectId, u64>,
    effects: JoinSet<(EffectId, Result<Event, String>)>,
    in_flight: BTreeSet<EffectId>,
    retry_at: BTreeMap<EffectId, Instant>,
    storage_cursor: EffectId,
    restore_deadlines: bool,
    receipts_enabled: bool,
    receipts: Arc<Option<ReceiptRetirement>>,
    committed_receipts: watch::Sender<Arc<Option<ReceiptRetirement>>>,
    receipt_issue_due: bool,
    receipt_sent: BTreeMap<EffectId, (u64, Instant)>,
    history: Arc<Option<AcceptedHistory>>,
    committed_history: watch::Sender<Arc<Option<AcceptedHistory>>>,
}

impl AuthorityService {
    pub async fn start(
        authority: Singleton,
        config: QualificationConfig,
    ) -> Result<(Self, CoordinatorTask), String> {
        // Once polled, preparation owns authority independently of the caller's
        // future. A cancelled startup cannot drop its last shutdown handle while
        // a recovery read or durable proposal is still in flight.
        let (prepared, receiver) = oneshot::channel();
        let _prepare = background_spawn!("experimental_authority_prepare", async move {
            let result = Self::start_owned(authority, config).await;
            if let Err(Ok((_service, coordinator))) = prepared.send(result) {
                let _shutdown = coordinator.shutdown().await;
            }
        });
        receiver
            .await
            .map_err(|error| format!("Authority preparation task failed: {error}"))?
    }

    async fn start_owned(
        authority: Singleton,
        config: QualificationConfig,
    ) -> Result<(Self, CoordinatorTask), String> {
        let initial = match authority.read().await {
            Ok(initial) => initial,
            Err(error) => {
                let _shutdown = authority.shutdown().await;
                return Err(error);
            }
        };
        if initial.payload_limit_bytes() < FRAME_BYTES {
            authority.shutdown().await?;
            return Err(
                "Experimental RPC requires at least 64 KiB committed payload capacity".to_owned(),
            );
        }
        if initial
            .accepted_history_namespace()
            .is_some_and(|namespace| namespace != config.cas_namespace)
        {
            authority.shutdown().await?;
            return Err(
                "Accepted-history CAS namespace does not match committed authority".to_owned(),
            );
        }
        let history = Arc::new(initial.accepted_history().cloned());
        let (committed_history, history_receiver) = watch::channel(history.clone());
        let watermark = initial.ingress_watermark();
        let receipts_enabled = initial.receipt_retirement_enabled();
        let receipts = Arc::new(initial.receipt_retirement().cloned());
        let (committed_receipts, receipt_receiver) = watch::channel(receipts.clone());
        let state = Arc::new(initial.view());
        let (committed, receiver) = watch::channel(state.clone());
        let (commands, inputs) = mpsc::channel(QUEUE);
        let (stop, stopped) = oneshot::channel();
        let service = Self {
            commands,
            committed: receiver,
            receipts: receipt_receiver,
            history: history_receiver,
            instance: Arc::from(config.instance.as_str()),
            principal: Arc::from(config.principal.as_str()),
            cas: config.cas.clone(),
            permits: Arc::new(Semaphore::new(QUEUE)),
        };
        let mut driver = Driver {
            authority,
            state,
            watermark,
            committed,
            config,
            workers: BTreeMap::new(),
            sent: BTreeMap::new(),
            effects: JoinSet::new(),
            in_flight: BTreeSet::new(),
            retry_at: BTreeMap::new(),
            storage_cursor: EffectId(0),
            restore_deadlines: true,
            receipts_enabled,
            receipts,
            committed_receipts,
            receipt_issue_due: true,
            receipt_sent: BTreeMap::new(),
            history,
            committed_history,
        };
        // A failed initial clock or recovery sweep must refuse construction
        // before a listener can expose this service. run_loop samples again
        // before queued inputs, including after a delayed task start.
        if let Err(error) = driver.time().await {
            let shutdown = driver.authority.shutdown().await;
            return Err(format!("{error}; authority shutdown: {shutdown:?}"));
        }
        let task = background_spawn!(
            "experimental_authority_coordinator",
            driver.run(inputs, stopped)
        );
        Ok((service, CoordinatorTask { stop, task }))
    }

    pub fn subscribe(&self) -> watch::Receiver<Arc<ObservableState>> {
        self.committed.clone()
    }

    /// Committed qualification metadata, independent of the observable model.
    pub fn subscribe_receipts(&self) -> watch::Receiver<Arc<Option<ReceiptRetirement>>> {
        self.receipts.clone()
    }

    /// Committed materialization metadata; terminal responses never gain links.
    pub fn subscribe_history(&self) -> watch::Receiver<Arc<Option<AcceptedHistory>>> {
        self.history.clone()
    }

    fn name(&self, operation: OperationId) -> String {
        format!(
            "{}/authority/{}/{}",
            self.instance,
            self.committed.borrow().epoch.0,
            operation.0
        )
    }

    fn parse_name(&self, name: &str) -> Result<OperationId, Status> {
        let prefix = format!(
            "{}/authority/{}/",
            self.instance,
            self.committed.borrow().epoch.0
        );
        name.strip_prefix(&prefix)
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|id| *id != 0)
            .map(OperationId)
            .ok_or_else(|| Status::not_found("Unknown operation name"))
    }

    fn permit(&self) -> Result<OwnedSemaphorePermit, Status> {
        self.permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("Experimental RPC concurrency exhausted"))
    }

    fn stream(&self, operation: OperationId, permit: OwnedSemaphorePermit) -> OperationStream {
        let name = self.name(operation);
        let commands = self.commands.clone();
        Box::pin(stream::unfold(
            Some(Observation {
                receiver: self.committed.clone(),
                _permit: permit,
                previous: None,
                heartbeat_at: 0,
            }),
            move |state| {
                let name = name.clone();
                let commands = commands.clone();
                async move {
                    let mut observation = state?;
                    loop {
                        if commands.is_closed() || observation.receiver.has_changed().is_err() {
                            return Some((Err(Status::unavailable("Coordinator stopped")), None));
                        }
                        let mut view = observation.receiver.borrow_and_update().clone();
                        let mut response = match operation_response(&view, operation, name.clone())
                        {
                            Ok(response) => response,
                            Err(error) => return Some((Err(error), None)),
                        };
                        let first = observation.previous.is_none();
                        let heartbeat = !first && view.now >= observation.heartbeat_at;
                        if first {
                            observation.heartbeat_at = view
                                .operations
                                .iter()
                                .find(|value| value.id == operation)
                                .map_or(view.now, |value| renewal_at(value.interest_deadline.at));
                        } else if heartbeat && !response.done {
                            // This future is polled by the response body. There is no
                            // detached renewal task when downstream capacity is absent.
                            let (reply, renewed) = oneshot::channel();
                            if commands
                                .send(Input::RenewInterest(operation, reply))
                                .await
                                .is_err()
                            {
                                return Some((
                                    Err(Status::unavailable("Coordinator stopped")),
                                    None,
                                ));
                            }
                            match renewed.await {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => return Some((Err(error), None)),
                                Err(_) => {
                                    return Some((
                                        Err(Status::unavailable("Renewal interrupted")),
                                        None,
                                    ));
                                }
                            }
                            view = observation.receiver.borrow_and_update().clone();
                            response = match operation_response(&view, operation, name.clone()) {
                                Ok(response) => response,
                                Err(error) => return Some((Err(error), None)),
                            };
                            observation.heartbeat_at =
                                view.now.saturating_add(INTEREST_RENEWAL_TICKS);
                        }
                        if first || heartbeat || observation.previous.as_ref() != Some(&response) {
                            if response.done {
                                return Some((Ok(response), None));
                            }
                            observation.previous = Some(response.clone());
                            return Some((Ok(response), Some(observation)));
                        }
                        if observation.receiver.changed().await.is_err() {
                            return Some((Err(Status::unavailable("Coordinator stopped")), None));
                        }
                    }
                }
            },
        ))
    }

    async fn capture(&self, grpc: Request<ExecuteRequest>) -> Result<AuthorityRequest, Status> {
        let metadata = grpc
            .metadata()
            .get_bin(REQUEST_METADATA)
            .map(|value| {
                let bytes = value
                    .to_bytes()
                    .map_err(|_| Status::invalid_argument("Invalid metadata"))?;
                if bytes.len() > FRAME_BYTES {
                    return Err(Status::resource_exhausted("Metadata exceeds capture bound"));
                }
                RequestMetadata::decode(bytes)
                    .map_err(|_| Status::invalid_argument("Invalid metadata"))
            })
            .transpose()?;
        let execute = grpc.into_inner();
        if execute.instance_name != self.instance.as_ref() || execute.encoded_len() > FRAME_BYTES {
            return Err(Status::invalid_argument(
                "Unknown instance or oversized request",
            ));
        }
        // Match RequestPayload's fixed default, independently of the process
        // default or any ambient context inherited by the HTTP/2 task.
        let algorithm = match digest_function::Value::try_from(execute.digest_function) {
            Ok(digest_function::Value::Unknown | digest_function::Value::Sha256) => {
                DigestHasherFunc::Sha256
            }
            Ok(digest_function::Value::Blake3) => DigestHasherFunc::Blake3,
            _ => {
                return Err(Status::invalid_argument(
                    "Unsupported authority digest function",
                ));
            }
        };
        let context = make_ctx_for_hash_func(algorithm).map_err(Status::from)?;
        let queued = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("Clock precedes epoch"))?;
        let queued = Timestamp {
            seconds: i64::try_from(queued.as_secs())
                .map_err(|_| Status::internal("Clock overflow"))?,
            nanos: i32::try_from(queued.subsec_nanos())
                .map_err(|_| Status::internal("Clock overflow"))?,
        };
        let digest = execute
            .action_digest
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing Action digest"))?;
        let action_bytes = bounded_blob(
            &self.cas,
            DigestInfo::try_from(digest).map_err(Status::from)?,
        )
        .with_context(context.clone())
        .await
        .map_err(Status::from)?;
        if action_bytes.len() > FRAME_BYTES {
            return Err(Status::resource_exhausted("Action exceeds capture bound"));
        }
        let action = Action::decode(action_bytes.clone())
            .map_err(|_| Status::invalid_argument("Invalid Action"))?;
        let command_digest = action
            .command_digest
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing Command digest"))?;
        let command_bytes = bounded_blob(
            &self.cas,
            DigestInfo::try_from(command_digest).map_err(Status::from)?,
        )
        .with_context(context)
        .await
        .map_err(Status::from)?;
        let payload = RequestPayload::capture(
            &execute,
            metadata.as_ref(),
            &queued,
            &self.principal,
            &action_bytes,
            &command_bytes,
            FRAME_BYTES,
        )
        .map_err(Status::invalid_argument)?;
        AuthorityRequest::from_payload(payload).map_err(Status::invalid_argument)
    }
}

#[tonic::async_trait]
impl Execution for AuthorityService {
    type ExecuteStream = OperationStream;
    type WaitExecutionStream = OperationStream;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let permit = self.permit()?;
        let request = self.capture(request).await?;
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Input::Submit(Box::new(request), reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        let operation = response
            .await
            .map_err(|_| Status::unavailable("Admission interrupted"))??;
        Ok(Response::new(self.stream(operation, permit)))
    }

    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        let permit = self.permit()?;
        let operation = self.parse_name(&request.into_inner().name)?;
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Input::Observe(operation, reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        response
            .await
            .map_err(|_| Status::unavailable("Observation interrupted"))??;
        Ok(Response::new(self.stream(operation, permit)))
    }
}

#[tonic::async_trait]
impl AuthorityWorkerApi for AuthorityService {
    type ConnectStream = WorkerStream;

    async fn connect(
        &self,
        request: Request<Streaming<AuthorityWorkerUpdate>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let permit = self.permit()?;
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Missing registration"))?;
        let Some(authority_worker_update::Update::Registration(registration)) = first.update else {
            return Err(Status::failed_precondition("First update must register"));
        };
        let session = registration
            .session
            .ok_or_else(|| Status::invalid_argument("Missing session"))?;
        let (sender, receiver) = mpsc::channel(QUEUE);
        let (reply, response) = oneshot::channel();
        let (stop, mut stopped) = oneshot::channel();
        self.commands
            .send(Input::Register(Box::new(registration), sender, stop, reply))
            .await
            .map_err(|_| Status::unavailable("Coordinator stopped"))?;
        response
            .await
            .map_err(|_| Status::unavailable("Registration interrupted"))??;
        let commands = self.commands.clone();
        background_spawn!("experimental_authority_worker_ingress", async move {
            let _permit = permit;
            let mut snapshots = Arc::new(Mutex::new(WorkerSnapshots::default()));
            loop {
                let next =
                    tokio::select! { _ = &mut stopped => break, next = incoming.message() => next };
                let Ok(Some(update)) = next else {
                    break;
                };
                let Some(input) = worker_input(session, &mut snapshots, update) else {
                    continue;
                };
                if commands.send(input).await.is_err() {
                    return;
                }
            }
            let _ignored = commands.send(Input::Disconnected(session)).await;
        });
        Ok(Response::new(Box::pin(stream::unfold(
            receiver,
            |mut receiver| async move { receiver.recv().await.map(|update| (update, receiver)) },
        ))))
    }
}

impl Driver {
    async fn commit(&mut self, event: Event) -> Result<Reply, String> {
        let history_effect = if let Event::HistoryPublication { effect, .. } = &event {
            Some(*effect)
        } else {
            None
        };
        let reply = self
            .authority
            .write(RecordedEvent {
                epoch: self.state.epoch,
                event,
            })
            .await?
            .data;
        let state = self.authority.read().await?;
        if state
            .accepted_history_namespace()
            .is_some_and(|namespace| namespace != self.config.cas_namespace)
        {
            return Err(
                "Accepted-history CAS namespace does not match committed authority".to_owned(),
            );
        }
        self.history = Arc::new(state.accepted_history().cloned());
        self.committed_history.send_replace(self.history.clone());
        self.watermark = state.ingress_watermark();
        self.receipts_enabled = state.receipt_retirement_enabled();
        self.receipts = Arc::new(state.receipt_retirement().cloned());
        self.committed_receipts.send_replace(self.receipts.clone());
        self.receipt_issue_due = true;
        self.state = Arc::new(state.view());
        self.committed.send_replace(self.state.clone());
        if let Some(effect) = history_effect {
            let association = self.history.as_ref().as_ref().and_then(|history| {
                history
                    .associations
                    .values()
                    .find(|row| row.effect == effect)
            });
            let operation_ids =
                association.map(|row| history_operation_ids(&self.state, row.execution));
            tracing::info!(
                effect_id = effect.0,
                ?operation_ids,
                ?reply,
                "Accepted history materialization outcome committed"
            );
        }
        Ok(reply)
    }

    async fn run(
        mut self,
        inputs: mpsc::Receiver<Input>,
        stop: oneshot::Receiver<()>,
    ) -> Result<(), String> {
        let result = self
            .run_loop(inputs, stop, interval(Duration::from_millis(20)))
            .await;
        self.effects.abort_all();
        while self.effects.join_next().await.is_some() {}
        self.workers.clear();
        let shutdown = self.authority.shutdown().await;
        match (result, shutdown) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), shutdown) => Err(format!("{error}; authority shutdown: {shutdown:?}")),
            (Ok(()), Err(error)) => Err(error),
        }
    }

    async fn run_loop(
        &mut self,
        mut inputs: mpsc::Receiver<Input>,
        mut stop: oneshot::Receiver<()>,
        mut ticker: tokio::time::Interval,
    ) -> Result<(), String> {
        // A restored clock may already make retained deadlines due. Finish
        // that sweep before a queued observation or worker report can run.
        self.time().await?;
        loop {
            let changed = tokio::select! {
                _ = &mut stop => break,
                input = inputs.recv() => {
                    let Some(input) = input else { break; };
                    self.input(input).await?;
                    true
                }
                completion = self.effects.join_next(), if !self.effects.is_empty() => {
                    let (effect, outcome) = completion.ok_or("Missing effect task")?.map_err(|error| error.to_string())?;
                    self.in_flight.remove(&effect);
                    match outcome {
                        Ok(event) => { self.commit(event).await?; }
                        Err(_) => { self.retry_at.insert(effect, Instant::now() + Duration::from_millis(100)); }
                    }
                    true
                }
                _ = ticker.tick() => self.time().await?,
            };
            if changed {
                for _ in 0..QUEUE {
                    let reply = self.commit(Event::Match).await?;
                    if !matches!(reply, Reply::Matched { .. } | Reply::Applied) {
                        break;
                    }
                }
            }
            self.issue_receipts().await?;
            self.pump_effects()?;
            self.pump_receipts()?;
        }
        Ok(())
    }

    async fn time(&mut self) -> Result<bool, String> {
        let now = (self.config.now)()
            .map_err(|error| format!("Authority clock observation failed: {error}"))?
            .max(self.state.now);
        let advanced = now > self.state.now;
        if !advanced && !self.restore_deadlines {
            return Ok(false);
        }
        if advanced {
            self.commit(Event::AdvanceTime { now }).await?;
        }
        let mut due = Vec::new();
        for attempt in &self.state.attempts {
            if !attempt.authorized {
                continue;
            }
            // A fresh Running sample is rounded down to a whole tick. Wait
            // past the stored execution deadline to retain that lost fraction.
            // The independent progress window below remains a coarse boundary.
            if attempt.phase == Phase::Running
                && attempt.running_deadline.is_some_and(|at| at < now)
            {
                due.push(Event::ExpireExecution {
                    attempt: attempt.id,
                });
            } else if attempt.deadline.at <= now {
                due.push(Event::ExpireAttempt {
                    attempt: attempt.id,
                    generation: attempt.deadline.generation,
                });
            }
        }
        for operation in &self.state.operations {
            if operation.interested
                && operation.terminal.is_none()
                && operation.interest_deadline.at <= now
            {
                due.push(Event::ExpireInterest {
                    operation: operation.id,
                    generation: operation.interest_deadline.generation,
                });
            }
        }
        for execution in &self.state.executions {
            if execution
                .terminal_at
                .is_some_and(|at| now >= at.saturating_add(TERMINAL_RETENTION))
            {
                due.push(Event::RetireExecution {
                    execution: execution.id,
                });
            }
        }
        let changed = advanced || !due.is_empty();
        for event in due {
            self.commit(event).await?;
        }
        // A restored journal may end after AdvanceTime but before its expiry
        // proposals. Sweep it once even if the local clock has not advanced.
        // Rejected retirement proposals do not spin at an unchanged clock.
        self.restore_deadlines = false;
        Ok(changed)
    }

    async fn input(&mut self, input: Input) -> Result<(), String> {
        match input {
            Input::Submit(request, reply) => {
                let Some(next) = self.watermark.0.checked_add(1) else {
                    let _ignored = reply.send(Err(Status::resource_exhausted(
                        "Operation identity exhausted",
                    )));
                    return Ok(());
                };
                let operation = OperationId(next);
                let outcome = self
                    .commit(Event::Submit {
                        operation,
                        request: *request,
                    })
                    .await?;
                let result = if matches!(outcome, Reply::Submitted { .. }) {
                    Ok(operation)
                } else {
                    Err(Status::resource_exhausted(format!(
                        "Admission rejected: {outcome:?}"
                    )))
                };
                let _ignored = reply.send(result);
            }
            Input::Observe(operation, reply) => {
                let outcome = self.commit(Event::Observe { operation }).await?;
                let result = if matches!(outcome, Reply::Observed(_)) {
                    Ok(())
                } else {
                    Err(Status::not_found("Operation is not retained"))
                };
                let _ignored = reply.send(result);
            }
            Input::Cancel(operation, reply) => {
                let outcome = self.commit(Event::Cancel { operation }).await?;
                let result = if matches!(outcome, Reply::Applied | Reply::Ignored) {
                    Ok(())
                } else {
                    Err(Status::not_found("Operation is not retained"))
                };
                let _ignored = reply.send(result);
            }
            Input::RenewInterest(operation, reply) => {
                if reply.is_closed() {
                    return Ok(());
                }
                // Account for the current clock before considering renewal. A
                // queued delivery opportunity cannot revive an expired interest.
                self.time().await?;
                let due = self.state.operations.iter().any(|value| {
                    value.id == operation
                        && value.interested
                        && value.terminal.is_none()
                        && self.state.now < value.interest_deadline.at
                        && self.state.now >= renewal_at(value.interest_deadline.at)
                });
                if due && !reply.is_closed() {
                    self.commit(Event::RenewInterest { operation }).await?;
                }
                let _ignored = reply.send(Ok(()));
            }
            Input::Register(registration, sender, stop, reply) => {
                let result = self.register(*registration, sender, stop).await;
                match result {
                    Ok(result) => {
                        let _ignored = reply.send(result);
                    }
                    Err(error) => {
                        let _ignored =
                            reply.send(Err(Status::unavailable("Registration commit failed")));
                        return Err(error);
                    }
                }
            }
            Input::Worker(session, update) => {
                if self.current(session) {
                    self.update(session, *update).await?;
                }
            }
            Input::WorkerSnapshots(session, snapshots) => {
                let snapshots = std::mem::take(&mut *snapshots.lock());
                if self.current(session) {
                    if let Some(inventory) = snapshots.inventory {
                        self.update(
                            session,
                            AuthorityWorkerUpdate {
                                update: Some(authority_worker_update::Update::Reconciliation(
                                    inventory,
                                )),
                            },
                        )
                        .await?;
                    }
                    if let Some(capacity) = snapshots.capacity {
                        self.update(
                            session,
                            AuthorityWorkerUpdate {
                                update: Some(authority_worker_update::Update::Capacity(capacity)),
                            },
                        )
                        .await?;
                    }
                }
            }
            Input::Disconnected(session) => {
                if self.current(session)
                    && let Some(identity) = session.worker
                {
                    self.workers.remove(&WorkerId(identity.worker_id));
                }
                // Transport loss is not quiescence or an authority revocation.
            }
        }
        Ok(())
    }

    fn current(&self, session: AuthoritySession) -> bool {
        session
            .worker
            .and_then(|worker| self.workers.get(&WorkerId(worker.worker_id)))
            .is_some_and(|connection| connection.session == session)
    }

    async fn register(
        &mut self,
        registration: AuthorityRegistration,
        sender: WorkerSender,
        stop: oneshot::Sender<()>,
    ) -> Result<Result<(), Status>, String> {
        let validated = validate_registration(&registration, self.state.epoch.0);
        let (session, identity, properties, capacity, available) = match validated {
            Ok(value) => value,
            Err(error) => return Ok(Err(error)),
        };
        let outcome = self
            .commit(Event::Register {
                worker: WorkerId(identity.worker_id),
                incarnation: IncarnationId(identity.incarnation_id),
                session: SessionId(session.session_id),
                properties,
                capacity,
                available,
            })
            .await?;
        if outcome != Reply::Applied {
            return Ok(Err(Status::failed_precondition("Registration rejected")));
        }
        self.workers.insert(
            WorkerId(identity.worker_id),
            Connection {
                session,
                sender: sender.clone(),
                capacity_sequence: 0,
                _stop: stop,
            },
        );
        // Inventory is processed before new placement; missing rows establish nothing.
        if let Some(inventory) = registration.reconciliation {
            for report in inventory.attempts {
                self.report(session, report).await?;
            }
            self.acknowledge_receipts(session, inventory.receipt_retirement)
                .await?;
        }
        if sender
            .try_send(Ok(AuthorityCoordinatorUpdate {
                update: Some(authority_coordinator_update::Update::Registration(
                    AuthorityRegistrationResult {
                        session: Some(session),
                        protocol_version: VERSION,
                        required_capabilities: CAPABILITIES.to_vec(),
                    },
                )),
            }))
            .is_err()
        {
            self.workers.remove(&WorkerId(identity.worker_id));
            return Ok(Err(Status::unavailable("Registration connection ended")));
        }
        Ok(Ok(()))
    }

    async fn update(
        &mut self,
        session: AuthoritySession,
        update: AuthorityWorkerUpdate,
    ) -> Result<(), String> {
        if update.encoded_len() > 1024 * 1024 {
            return Ok(());
        }
        match update.update {
            Some(authority_worker_update::Update::Reconciliation(inventory))
                if inventory.session == Some(session) =>
            {
                for report in inventory.attempts {
                    self.report(session, report).await?;
                }
                self.acknowledge_receipts(session, inventory.receipt_retirement)
                    .await?;
            }
            Some(authority_worker_update::Update::Report(report)) => {
                self.report(session, report).await?;
            }
            Some(authority_worker_update::Update::Capacity(capacity))
                if capacity.session == Some(session) =>
            {
                let identity = session.worker.ok_or("Session lost identity")?;
                let connection = self
                    .workers
                    .get_mut(&WorkerId(identity.worker_id))
                    .ok_or("Session lost connection")?;
                if capacity.observation_sequence <= connection.capacity_sequence {
                    return Ok(());
                }
                let (Some(total), Some(ceiling)) = (capacity.capacity, capacity.admission_ceiling)
                else {
                    return Ok(());
                };
                connection.capacity_sequence = capacity.observation_sequence;
                let total = resources(total);
                let ceiling = resources(ceiling);
                if self.state.workers.iter().any(|worker| {
                    worker.id.0 == identity.worker_id
                        && worker.incarnation.0 == identity.incarnation_id
                        && worker.session.0 == session.session_id
                        && worker.capacity == total
                        && worker.available == ceiling
                        && worker.draining == capacity.draining
                        && worker.last_heartbeat == self.state.now
                }) {
                    return Ok(());
                }
                self.commit(Event::Capacity {
                    worker: WorkerId(identity.worker_id),
                    incarnation: IncarnationId(identity.incarnation_id),
                    session: SessionId(session.session_id),
                    capacity: total,
                    available: ceiling,
                    draining: capacity.draining,
                })
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn report(
        &mut self,
        session: AuthoritySession,
        report: AuthorityAttemptReport,
    ) -> Result<(), String> {
        let Ok(report_phase) = AuthorityAttemptPhase::try_from(report.phase) else {
            return Ok(());
        };
        if report_phase == AuthorityAttemptPhase::Unspecified
            || report.observation_sequence == 0
            || report.encoded_len() > FRAME_BYTES + 1024
        {
            return Ok(());
        }
        let Some(target) = report.attempt else {
            return Ok(());
        };
        let Some(identity) = target.worker else {
            return Ok(());
        };
        if session.worker != Some(identity) || identity.cluster_epoch != self.state.epoch.0 {
            return Ok(());
        }
        let Some(mut attempt) = self
            .state
            .attempts
            .iter()
            .find(|attempt| {
                attempt.id.0 == target.attempt_id
                    && attempt.execution.0 == target.execution_id
                    && attempt.worker.0 == identity.worker_id
                    && attempt.incarnation.0 == identity.incarnation_id
            })
            .copied()
        else {
            return Ok(());
        };
        let quiescent = report_phase == AuthorityAttemptPhase::Quiescent;
        let dispatch_matches = attempt.dispatch_effect.0 == report.dispatch_effect_id;
        let cancel_matches = !attempt.authorized && quiescent && report.cancel_effect_id != 0 && self.state.effects.iter().any(|effect| {
            effect.id.0 == report.cancel_effect_id && matches!(effect.kind, EffectKind::Cancel { execution, attempt: target, worker, incarnation } if execution == attempt.execution && target == attempt.id && worker == attempt.worker && incarnation == attempt.incarnation)
        });
        if !dispatch_matches && !cancel_matches {
            return Ok(());
        }
        let worker = attempt.worker;
        let incarnation = attempt.incarnation;
        if !attempt.authorized {
            if quiescent {
                self.commit(Event::Cleanup {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                })
                .await?;
            }
            return Ok(());
        }
        // Explicit memory evidence is a protocol assertion. Validate it before
        // any acknowledgment, phase change or decline can mutate authority.
        // Stale reports above retain only their idempotent cleanup effect.
        let memory_failure = if report.memory_breach.is_some() {
            let Some(candidate) = memory_failure_payload(&self.state, &attempt, &report) else {
                return Ok(());
            };
            Some(candidate)
        } else {
            None
        };
        if matches!(
            report.result,
            Some(authority_attempt_report::Result::Decline(_))
        ) {
            if quiescent {
                self.commit(Event::Decline {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                })
                .await?;
            }
            return Ok(());
        }
        if report_phase == AuthorityAttemptPhase::Running
            && attempt.running_deadline.is_none()
            && matches!(
                attempt.phase,
                Phase::Dispatching | Phase::Accepted | Phase::Preparing
            )
            && attempt.deadline.generation.checked_add(1).is_some()
        {
            // The first Running anchor must follow receipt of this valid report,
            // even when the periodic timer has been delayed. Already-due expiry
            // wins over the grant; reload authorization after the sweep.
            self.time().await?;
            let Some(current) = self
                .state
                .attempts
                .iter()
                .find(|value| value.id == attempt.id)
            else {
                return Ok(());
            };
            if !current.authorized {
                return Ok(());
            }
            attempt = *current;
        }
        if attempt.phase == Phase::Dispatching {
            self.commit(Event::WorkerAccepted {
                attempt: attempt.id,
                worker,
                incarnation,
            })
            .await?;
        }
        let phase = match AuthorityAttemptPhase::try_from(report.phase).ok() {
            Some(AuthorityAttemptPhase::Preparing) => Some(Phase::Preparing),
            Some(AuthorityAttemptPhase::Running) => Some(Phase::Running),
            Some(AuthorityAttemptPhase::Uploading) => Some(Phase::Uploading),
            _ => None,
        };
        if let Some(phase) = phase
            && (phase != attempt.phase || report.phase_progress > attempt.progress)
        {
            self.commit(Event::Progress {
                attempt: attempt.id,
                worker,
                incarnation,
                phase,
                progress: report.phase_progress,
            })
            .await?;
        }
        if let Some((charged, result)) = memory_failure {
            self.config
                .faults
                .candidate_reports
                .fetch_add(1, Ordering::Relaxed);
            if !self.config.faults.hold_completions.load(Ordering::Relaxed) {
                self.commit(Event::MemoryFailure {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                    charged,
                    // Process-group RSS is a policy sample, not an exact peak.
                    // The positive original charge drives the existing growth policy.
                    peak_memory_bytes: None,
                    quiescent: true,
                    result,
                })
                .await?;
            }
            return Ok(());
        }
        let candidate = match report.result {
            Some(authority_attempt_report::Result::ExecuteResponse(response)) => Some(response),
            Some(authority_attempt_report::Result::InternalError(status)) if status.code != 0 => {
                Some(ExecuteResponse {
                    status: Some(status),
                    ..ExecuteResponse::default()
                })
            }
            _ => None,
        };
        if let Some(response) = candidate {
            self.config
                .faults
                .candidate_reports
                .fetch_add(1, Ordering::Relaxed);
            if quiescent && !self.config.faults.hold_completions.load(Ordering::Relaxed) {
                let Some(result) = completion_payload(&response) else {
                    return Ok(());
                };
                self.commit(Event::Complete {
                    attempt: attempt.id,
                    worker,
                    incarnation,
                    result,
                })
                .await?;
            }
        }
        Ok(())
    }

    async fn issue_receipts(&mut self) -> Result<(), String> {
        if !self.receipts_enabled
            || !self.receipt_issue_due
            || self
                .config
                .faults
                .hold_receipt_issues
                .load(Ordering::Relaxed)
        {
            return Ok(());
        }
        let proposals: Vec<_> = self
            .receipts
            .as_ref()
            .as_ref()
            .map_or_else(Vec::new, |receipts| {
                receipts
                    .streams
                    .iter()
                    .filter_map(|(worker, stream)| {
                        (stream.dirty && stream.pending.is_none()).then_some(Event::IssueReceipts {
                            worker: *worker,
                            incarnation: stream.incarnation,
                            after_ack: stream.acknowledged.map(|floor| floor.effect),
                        })
                    })
                    .collect()
            });
        for event in proposals {
            self.commit(event).await?;
        }
        // A rejected proposal is retried after another input/commit, not every
        // idle timer tick. Fault release still retries the unsubmitted intent.
        self.receipt_issue_due = false;
        Ok(())
    }

    async fn acknowledge_receipts(
        &mut self,
        session: AuthoritySession,
        certificate: Option<AuthorityReceiptCertificate>,
    ) -> Result<(), String> {
        if !self.receipts_enabled || !self.current(session) {
            return Ok(());
        }
        self.config
            .faults
            .receipt_inventories
            .fetch_add(1, Ordering::Relaxed);
        if self
            .config
            .faults
            .hold_receipt_acknowledgments
            .load(Ordering::Relaxed)
        {
            return Ok(());
        }
        let (Some(identity), Some(certificate)) = (session.worker, certificate) else {
            return Ok(());
        };
        if certificate.encoded_len() > FRAME_BYTES
            || certificate.retain.len() > MAX_RECEIPT_BINDINGS
        {
            return Ok(());
        }
        let worker = WorkerId(identity.worker_id);
        let incarnation = IncarnationId(identity.incarnation_id);
        let certificate = receipt_certificate(certificate);
        let pending = self
            .receipts
            .as_ref()
            .as_ref()
            .and_then(|receipts| receipts.streams.get(&worker))
            .filter(|stream| stream.incarnation == incarnation)
            .and_then(|stream| stream.pending.as_ref());
        if pending != Some(&certificate) {
            return Ok(());
        }
        self.commit(Event::AcknowledgeReceipts {
            worker,
            incarnation,
            certificate,
        })
        .await?;
        Ok(())
    }

    fn pump_receipts(&mut self) -> Result<(), String> {
        let pending: BTreeMap<_, _> =
            self.receipts
                .as_ref()
                .as_ref()
                .map_or_else(BTreeMap::new, |receipts| {
                    receipts
                        .streams
                        .iter()
                        .filter_map(|(worker, stream)| {
                            stream.pending.as_ref().map(|certificate| {
                                (
                                    certificate.effect,
                                    (*worker, stream.incarnation, certificate),
                                )
                            })
                        })
                        .collect()
                });
        self.receipt_sent
            .retain(|effect, _| pending.contains_key(effect));
        if self
            .config
            .faults
            .hold_receipt_delivery
            .load(Ordering::Relaxed)
        {
            return Ok(());
        }
        for (effect, (worker, incarnation, certificate)) in pending {
            let Some(connection) = self.workers.get(&worker) else {
                continue;
            };
            if connection
                .session
                .worker
                .is_none_or(|identity| identity.incarnation_id != incarnation.0)
                || self
                    .receipt_sent
                    .get(&effect)
                    .is_some_and(|(session, sent)| {
                        *session == connection.session.session_id
                            && sent.elapsed() < Duration::from_millis(250)
                    })
            {
                continue;
            }
            let certificate = wire_receipt_certificate(certificate);
            if certificate.encoded_len() > FRAME_BYTES
                || certificate.retain.len() > MAX_RECEIPT_BINDINGS
            {
                return Err("Committed receipt certificate exceeds wire bounds".to_owned());
            }
            if connection
                .sender
                .try_send(Ok(AuthorityCoordinatorUpdate {
                    update: Some(authority_coordinator_update::Update::Retirement(
                        AuthorityReceiptRetirement {
                            session: Some(connection.session),
                            certificate: Some(certificate),
                        },
                    )),
                }))
                .is_ok()
            {
                // Queueing is not acknowledgment. The committed body remains
                // pending and is retried, including within a live session.
                self.receipt_sent
                    .insert(effect, (connection.session.session_id, Instant::now()));
            }
        }
        Ok(())
    }

    fn pump_effects(&mut self) -> Result<(), String> {
        let mut storage = BTreeMap::new();
        if let Some(history) = self.history.as_ref() {
            for association in history
                .associations
                .values()
                .filter(|h| h.status == HistoryStatus::Pending)
            {
                storage.insert(association.effect, PendingStorage::History(association));
            }
        }
        let live: BTreeSet<_> = self
            .state
            .effects
            .iter()
            .map(|effect| effect.id)
            .chain(storage.keys().copied())
            .collect();
        self.sent.retain(|effect, _| live.contains(effect));
        self.retry_at.retain(|effect, _| live.contains(effect));
        for effect in &self.state.effects {
            match &effect.kind {
                EffectKind::Dispatch {
                    worker,
                    incarnation,
                    ..
                }
                | EffectKind::Cancel {
                    worker,
                    incarnation,
                    ..
                } => {
                    if matches!(effect.kind, EffectKind::Dispatch { .. })
                        && self.config.faults.hold_dispatches.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    let Some(connection) = self.workers.get(worker) else {
                        continue;
                    };
                    if connection
                        .session
                        .worker
                        .is_none_or(|identity| identity.incarnation_id != incarnation.0)
                        || self
                            .sent
                            .get(&effect.id)
                            .is_some_and(|session| *session == connection.session.session_id)
                    {
                        continue;
                    }
                    let command = effect_command(effect)?;
                    if connection
                        .sender
                        .try_send(Ok(AuthorityCoordinatorUpdate {
                            update: Some(authority_coordinator_update::Update::Command(command)),
                        }))
                        .is_ok()
                    {
                        self.sent.insert(effect.id, connection.session.session_id);
                    }
                }
                EffectKind::Lookup { .. } | EffectKind::PublishResult { .. } => {
                    storage.insert(effect.id, PendingStorage::Ordinary(effect));
                }
            }
        }
        // The two outboxes share globally allocated identities, retry timing,
        // and one four-request bound. Rotate across their combined identities
        // so expired retry timers cannot let old failures monopolize every slot.
        let mut storage: Vec<_> = storage.into_iter().collect();
        let split = storage.partition_point(|(id, _)| *id <= self.storage_cursor);
        storage.rotate_left(split);
        for (id, pending) in storage {
            if self.in_flight.contains(&id)
                || self.in_flight.len() >= 4
                || self
                    .retry_at
                    .get(&id)
                    .is_some_and(|at| *at > Instant::now())
            {
                continue;
            }
            let (sink, work) = match pending {
                PendingStorage::Ordinary(effect) => {
                    if matches!(effect.kind, EffectKind::PublishResult { .. })
                        && self.config.faults.hold_publications.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    (
                        self.config.publication_sink.clone(),
                        StorageWork::Ordinary(effect.clone()),
                    )
                }
                PendingStorage::History(association) => {
                    if self
                        .config
                        .faults
                        .hold_history_publications
                        .load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    let bytes = history_bytes(&self.state, association, self.config.cas_namespace)?;
                    let operation_ids = history_operation_ids(&self.state, association.execution);
                    tracing::info!(effect_id = id.0, execution_id = association.execution.0,
                        ?operation_ids, object = ?association.recipe.object,
                        "Accepted history delivery started");
                    (
                        self.config.cas.clone(),
                        StorageWork::History(association.recipe.object, bytes),
                    )
                }
            };
            self.in_flight.insert(id);
            self.storage_cursor = id;
            self.effects.spawn(async move {
                let outcome = tokio::time::timeout(Duration::from_secs(5), async {
                    match work {
                        StorageWork::Ordinary(effect) => storage_effect(&sink, &effect).await,
                        StorageWork::History(object, bytes) => {
                            let algorithm = match object.algorithm {
                                HistoryAlgorithm::Sha256 => DigestHasherFunc::Sha256,
                                HistoryAlgorithm::Blake3 => DigestHasherFunc::Blake3,
                            };
                            sink.update_oneshot(
                                DigestInfo::new(object.hash, object.size_bytes),
                                bytes,
                            )
                            .with_context(
                                make_ctx_for_hash_func(algorithm)
                                    .map_err(|error| error.to_string())?,
                            )
                            .await
                            .map_err(|error| error.to_string())?;
                            Ok(Event::HistoryPublication {
                                effect: id,
                                object,
                                outcome: HistoryOutcome::Stored,
                            })
                        }
                    }
                })
                .await
                .map_err(|_| "Experimental sink timeout".to_owned())
                .and_then(|result| result);
                (id, outcome)
            });
        }
        Ok(())
    }
}

enum PendingStorage<'a> {
    Ordinary(&'a Effect),
    History(&'a HistoryAssociation),
}

enum StorageWork {
    Ordinary(Effect),
    History(HistoryObject, Bytes),
}

fn history_operation_ids(state: &ObservableState, execution: ExecutionId) -> Vec<u64> {
    state
        .operations
        .iter()
        .filter_map(|operation| {
            (operation.execution == execution
                && matches!(
                    operation.terminal,
                    Some(TerminalResult::Completed(_) | TerminalResult::ResourceFailed { .. })
                ))
            .then_some(operation.id.0)
        })
        .collect()
}

fn history_bytes(
    state: &ObservableState,
    association: &HistoryAssociation,
    namespace: [u8; 32],
) -> Result<Bytes, String> {
    let object = association.recipe.object;
    if object.namespace != namespace || association.epoch != state.epoch {
        return Err("History delivery does not match authority namespace".to_owned());
    }
    let execution = state
        .executions
        .iter()
        .find(|execution| execution.id == association.execution)
        .ok_or("History execution missing")?;
    let Some(TerminalResult::Completed(result) | TerminalResult::ResourceFailed { result, .. }) =
        &execution.terminal
    else {
        return Err("History requires an accepted worker terminal".to_owned());
    };
    let response = result.require_response()?.encoded();
    let mut fingerprint = DigestHasherFunc::Sha256.hasher();
    fingerprint.update(response);
    if **fingerprint.finalize_digest().packed_hash() != association.recipe.response_sha256 {
        return Err("History response fingerprint changed".to_owned());
    }
    let mut bytes = association.recipe.prefix.clone();
    bytes.extend_from_slice(response);
    let mut hasher = match object.algorithm {
        HistoryAlgorithm::Sha256 => DigestHasherFunc::Sha256.hasher(),
        HistoryAlgorithm::Blake3 => DigestHasherFunc::Blake3.hasher(),
    };
    hasher.update(&bytes);
    if **hasher.finalize_digest().packed_hash() != object.hash
        || bytes.len() as u64 != object.size_bytes
    {
        return Err("History object bytes changed".to_owned());
    }
    Ok(bytes.into())
}

fn receipt_certificate(value: AuthorityReceiptCertificate) -> ReceiptCertificate {
    ReceiptCertificate {
        effect: EffectId(value.effect_id),
        attempts_through: AttemptId(value.attempts_through),
        effects_through: EffectId(value.effects_through),
        reservations_through: ReservationId(value.reservations_through),
        retain: value
            .retain
            .into_iter()
            .map(|binding| ReceiptBinding {
                attempt: AttemptId(binding.attempt_id),
                execution: ExecutionId(binding.execution_id),
                dispatch_effect: EffectId(binding.dispatch_effect_id),
                reservation: ReservationId(binding.reservation_id),
                cancel_effect: (binding.cancel_effect_id != 0)
                    .then_some(EffectId(binding.cancel_effect_id)),
            })
            .collect(),
    }
}

fn wire_receipt_certificate(value: &ReceiptCertificate) -> AuthorityReceiptCertificate {
    AuthorityReceiptCertificate {
        effect_id: value.effect.0,
        attempts_through: value.attempts_through.0,
        effects_through: value.effects_through.0,
        reservations_through: value.reservations_through.0,
        retain: value
            .retain
            .iter()
            .map(|binding| AuthorityRetainedReceipt {
                attempt_id: binding.attempt.0,
                execution_id: binding.execution.0,
                dispatch_effect_id: binding.dispatch_effect.0,
                reservation_id: binding.reservation.0,
                cancel_effect_id: binding.cancel_effect.map_or(0, |effect| effect.0),
            })
            .collect(),
    }
}

const fn resources(value: AuthorityResources) -> Resources {
    Resources {
        cpu_millis: value.cpu_millis,
        memory_bytes: value.memory_bytes,
        slots: value.slots,
    }
}

type RegistrationFacts = (
    AuthoritySession,
    AuthorityWorkerIdentity,
    BTreeMap<String, String>,
    Resources,
    Resources,
);

fn validate_registration(
    registration: &AuthorityRegistration,
    epoch: u64,
) -> Result<RegistrationFacts, Status> {
    let session = registration
        .session
        .ok_or_else(|| Status::invalid_argument("Missing session"))?;
    let identity = session
        .worker
        .ok_or_else(|| Status::invalid_argument("Missing worker identity"))?;
    if identity.cluster_epoch != epoch
        || identity.worker_id == 0
        || identity.incarnation_id == 0
        || session.session_id == 0
        || registration.protocol_version != VERSION
        || !CAPABILITIES
            .iter()
            .all(|capability| registration.capabilities.contains(capability))
        || registration.encoded_len() > 1024 * 1024
    {
        return Err(Status::failed_precondition(
            "Unsupported authority identity or protocol",
        ));
    }
    let inventory = registration
        .reconciliation
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("Complete inventory required"))?;
    if inventory.session != Some(session) {
        return Err(Status::failed_precondition("Inventory session mismatch"));
    }
    let capacity = resources(
        registration
            .capacity
            .ok_or_else(|| Status::invalid_argument("Missing capacity"))?,
    );
    let available = resources(
        registration
            .admission_ceiling
            .ok_or_else(|| Status::invalid_argument("Missing admission ceiling"))?,
    );
    if inventory.single_use_spent && available != Resources::default() {
        return Err(Status::failed_precondition(
            "Spent worker must close admission",
        ));
    }
    let mut properties = BTreeMap::new();
    for property in &registration.properties {
        if properties
            .insert(property.name.clone(), property.value.clone())
            .is_some()
        {
            return Err(Status::invalid_argument("Duplicate worker property"));
        }
    }
    Ok((session, identity, properties, capacity, available))
}

fn effect_command(effect: &Effect) -> Result<AuthorityCommand, String> {
    let (execution, attempt, worker, incarnation, command) = match &effect.kind {
        EffectKind::Dispatch {
            execution,
            attempt,
            worker,
            incarnation,
            reservation,
            charged,
            request,
        } => {
            let facts = request.require_payload()?.dispatch_facts()?;
            (
                *execution,
                *attempt,
                *worker,
                *incarnation,
                authority_command::Command::Dispatch(AuthorityDispatch {
                    reservation_id: reservation.0,
                    reserved_resources: Some(AuthorityResources {
                        cpu_millis: charged.cpu_millis,
                        memory_bytes: charged.memory_bytes,
                        slots: charged.slots,
                    }),
                    execute_request: Some(facts.execute_request),
                    queued_timestamp: Some(facts.queued_timestamp),
                    platform: Some(facts.platform),
                    request_metadata: facts.request_metadata,
                }),
            )
        }
        EffectKind::Cancel {
            execution,
            attempt,
            worker,
            incarnation,
        } => (
            *execution,
            *attempt,
            *worker,
            *incarnation,
            authority_command::Command::Cancel(()),
        ),
        _ => return Err("Not a recipient command".to_owned()),
    };
    Ok(AuthorityCommand {
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
        command: Some(command),
    })
}

fn digest(action: &ActionKey) -> Result<DigestInfo, String> {
    DigestInfo::try_from(Digest {
        hash: action.digest.clone(),
        size_bytes: i64::try_from(action.digest_size_bytes).map_err(|error| error.to_string())?,
    })
    .map_err(|error| error.to_string())
}

// Effects outlive their originating RPC and may be replayed after restart.
// Select the namespace from committed action identity on every attempt.
fn action_context(action: &ActionKey) -> Result<opentelemetry::Context, String> {
    let algorithm = match action.digest_function.as_str() {
        "sha256" => DigestHasherFunc::Sha256,
        "blake3" => DigestHasherFunc::Blake3,
        _ => return Err("Unsupported committed action digest function".to_owned()),
    };
    make_ctx_for_hash_func(algorithm).map_err(|error| error.to_string())
}

async fn storage_effect(sink: &Store, effect: &Effect) -> Result<Event, String> {
    match &effect.kind {
        EffectKind::Lookup { action, .. } => {
            let result = match bounded_blob(sink, digest(action)?)
                .with_context(action_context(action)?)
                .await
            {
                Ok(bytes) if bytes.len() <= FRAME_BYTES => {
                    let result = ActionResult::decode(bytes).map_err(|error| error.to_string())?;
                    Some(ResultPayload::from_response(
                        &ExecuteResponse {
                            result: Some(result),
                            cached_result: true,
                            ..ExecuteResponse::default()
                        },
                        FRAME_BYTES,
                    )?)
                }
                Err(error) if error.code == tonic::Code::NotFound => None,
                _ => return Err("Lookup failed or exceeded bound".to_owned()),
            };
            Ok(Event::LookupResult {
                effect: effect.id,
                result,
            })
        }
        EffectKind::PublishResult { action, result, .. } => {
            let response = result.require_response()?.decode()?;
            let bytes = response
                .result
                .ok_or("Missing publication result")?
                .encode_to_vec();
            sink.update_oneshot(digest(action)?, bytes.into())
                .with_context(action_context(action)?)
                .await
                .map_err(|error| error.to_string())?;
            Ok(Event::EffectOutcome {
                effect: effect.id,
                success: true,
            })
        }
        _ => Err("Not a storage effect".to_owned()),
    }
}

fn operation_response(
    view: &ObservableState,
    id: OperationId,
    name: String,
) -> Result<Operation, Status> {
    let operation = view
        .operations
        .iter()
        .find(|operation| operation.id == id)
        .ok_or_else(|| Status::not_found("Operation is not retained"))?;
    let execution = view
        .executions
        .iter()
        .find(|execution| execution.id == operation.execution)
        .ok_or_else(|| Status::internal("Missing operation execution"))?;
    let stage = if operation.terminal.is_some() {
        4
    } else {
        match execution.phase {
            Phase::Lookup => 1,
            Phase::Queued => 2,
            Phase::Terminal => 4,
            _ => 3,
        }
    };
    let metadata = ExecuteOperationMetadata {
        stage,
        action_digest: Some(Digest {
            hash: execution.request.action.digest.clone(),
            size_bytes: i64::try_from(execution.request.action.digest_size_bytes)
                .map_err(|_| Status::internal("Digest size overflow"))?,
        }),
        ..ExecuteOperationMetadata::default()
    };
    let result = match &operation.terminal {
        Some(TerminalResult::Completed(result) | TerminalResult::ResourceFailed { result, .. }) => {
            let response = result.require_response().map_err(Status::internal)?;
            Some(operation::Result::Response(Any {
                type_url: "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteResponse"
                    .to_owned(),
                value: response.encoded().to_vec(),
            }))
        }
        Some(TerminalResult::Cancelled) => Some(operation::Result::Error(
            nativelink_proto::google::rpc::Status {
                code: tonic::Code::Cancelled as i32,
                message: "Operation cancelled".to_owned(),
                details: Vec::new(),
            },
        )),
        Some(TerminalResult::Exhausted) => Some(operation::Result::Error(
            nativelink_proto::google::rpc::Status {
                code: tonic::Code::ResourceExhausted as i32,
                message: "Execution attempt budget exhausted".to_owned(),
                details: Vec::new(),
            },
        )),
        None => None,
    };
    let response = Operation {
        name,
        metadata: Some(Any {
            type_url:
                "type.googleapis.com/build.bazel.remote.execution.v2.ExecuteOperationMetadata"
                    .to_owned(),
            value: metadata.encode_to_vec(),
        }),
        done: operation.terminal.is_some(),
        result,
    };
    if response.encoded_len() > OPERATION_BYTES {
        return Err(Status::resource_exhausted(
            "Operation exceeds delivery bound",
        ));
    }
    Ok(response)
}

const fn renewal_at(deadline: u64) -> u64 {
    deadline
        .saturating_sub(INTEREST_TIMEOUT)
        .saturating_add(INTEREST_RENEWAL_TICKS)
}

impl std::fmt::Debug for QualificationConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QualificationConfig")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}

async fn bounded_blob(store: &Store, digest: DigestInfo) -> Result<Bytes, Error> {
    let (mut writer, mut reader) = make_buf_channel_pair();
    let download = async move {
        let result = store.get_part(digest, &mut writer, 0, Some(65_537)).await;
        // Producer completion must close the channel even if EOF was omitted.
        drop(writer);
        result
    };
    let collect = async move {
        let mut bytes = Vec::new();
        loop {
            let chunk = reader.recv().await?;
            if chunk.is_empty() {
                break;
            }
            if chunk.len() > FRAME_BYTES.saturating_sub(bytes.len()) {
                return Err(make_err!(
                    tonic::Code::ResourceExhausted,
                    "Blob exceeds capture bound"
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(bytes))
    };
    let ((), bytes) = futures::try_join!(download, collect)?;
    Ok(bytes)
}

// Called only after current-session, exact-attempt/effect and authorization
// gates. It does not borrow the mutable execution target or telemetry grants.
fn memory_failure_payload(
    state: &ObservableState,
    attempt: &AttemptView,
    report: &AuthorityAttemptReport,
) -> Option<(Resources, ResultPayload)> {
    if report.phase != AuthorityAttemptPhase::Quiescent as i32 {
        return None;
    }
    let evidence = report.memory_breach.as_ref()?;
    let charged = resources(evidence.charged?);
    let reservation = state.reservations.iter().find(|reservation| {
        reservation.id == attempt.reservation
            && reservation.attempt == attempt.id
            && reservation.worker == attempt.worker
            && reservation.incarnation == attempt.incarnation
            && !reservation.released
    })?;
    if evidence.reservation_id != reservation.id.0
        || charged != reservation.charged
        || charged.memory_bytes == 0
        || evidence.limit_kb == 0
        || evidence.limit_kb < charged.memory_bytes.div_ceil(1024)
        || evidence.observed_kb <= evidence.limit_kb
    {
        return None;
    }
    let authority_attempt_report::Result::ExecuteResponse(response) = report.result.as_ref()?
    else {
        return None;
    };
    // Our eligible lane uploaded an actual action result. A status-only error
    // cannot stand in for missing output capture, even with a plausible marker.
    response.result.as_ref()?;
    let captured = ResultPayload::from_response(response, FRAME_BYTES).ok()?;
    if (captured.exit_code == 0 && captured.error.is_none())
        || serde_json::to_vec(&captured).ok()?.len() > FRAME_BYTES
    {
        return None;
    }
    // Never replace a graph-bearing memory candidate with the ordinary
    // completion overflow fallback. The exact response is captured once here.
    Some((charged, captured))
}

// The worker bounds protobuf bytes; the committed representation also includes
// JSON byte arrays and duplicated error text. Check that representation itself.
// Quiescent oversized output has one explicit non-cacheable terminal outcome.
fn completion_payload(response: &ExecuteResponse) -> Option<ResultPayload> {
    let captured = ResultPayload::from_response(response, FRAME_BYTES).ok()?;
    if serde_json::to_vec(&captured).ok()?.len() <= FRAME_BYTES {
        return Some(captured);
    }
    let overflow = ExecuteResponse {
        status: Some(nativelink_proto::google::rpc::Status {
            code: tonic::Code::ResourceExhausted as i32,
            message: "Candidate response exceeds the experimental committed payload bound"
                .to_owned(),
            details: Vec::new(),
        }),
        ..ExecuteResponse::default()
    };
    let result = ResultPayload::from_response(&overflow, FRAME_BYTES).ok()?;
    (serde_json::to_vec(&result).ok()?.len() <= FRAME_BYTES).then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> AuthoritySession {
        AuthoritySession {
            worker: Some(AuthorityWorkerIdentity {
                cluster_epoch: 1,
                worker_id: 2,
                incarnation_id: 3,
            }),
            session_id: 4,
        }
    }

    fn inventory(sequence: u64, phase: AuthorityAttemptPhase) -> AuthorityWorkerUpdate {
        AuthorityWorkerUpdate {
            update: Some(authority_worker_update::Update::Reconciliation(
                AuthorityReconciliation {
                    session: Some(session()),
                    attempts: vec![
                        AuthorityAttemptReport {
                            observation_sequence: 1,
                            phase: AuthorityAttemptPhase::Running as i32,
                            ..AuthorityAttemptReport::default()
                        },
                        AuthorityAttemptReport {
                            observation_sequence: sequence,
                            phase: phase as i32,
                            result: (phase == AuthorityAttemptPhase::Quiescent).then(|| {
                                authority_attempt_report::Result::ExecuteResponse(ExecuteResponse {
                                    result: Some(ActionResult::default()),
                                    ..ExecuteResponse::default()
                                })
                            }),
                            ..AuthorityAttemptReport::default()
                        },
                    ],
                    receipt_retirement: Some(AuthorityReceiptCertificate {
                        effect_id: 7,
                        attempts_through: 2,
                        effects_through: 7,
                        reservations_through: 2,
                        retain: vec![AuthorityRetainedReceipt {
                            attempt_id: 1,
                            execution_id: 1,
                            dispatch_effect_id: 1,
                            reservation_id: 1,
                            cancel_effect_id: 0,
                        }],
                    }),
                    ..AuthorityReconciliation::default()
                },
            )),
        }
    }

    #[test]
    fn slow_coordinator_observes_latest_candidate_with_one_pending_notification() {
        let mut snapshots = Arc::new(Mutex::new(WorkerSnapshots::default()));
        let mut notifications = Vec::new();
        // The journal consumer is stopped while ingress receives more complete
        // observations than the FIFO can hold. The terminal candidate must be
        // immediately available when it resumes, retaining the old live hole.
        for sequence in 1..=4 * QUEUE as u64 {
            let phase = if sequence == 4 * QUEUE as u64 {
                AuthorityAttemptPhase::Quiescent
            } else {
                AuthorityAttemptPhase::Uploading
            };
            for update in [
                inventory(sequence, phase),
                AuthorityWorkerUpdate {
                    update: Some(authority_worker_update::Update::Capacity(
                        AuthorityCapacityObservation {
                            session: Some(session()),
                            observation_sequence: sequence,
                            ..AuthorityCapacityObservation::default()
                        },
                    )),
                },
            ] {
                if let Some(input) = worker_input(session(), &mut snapshots, update) {
                    notifications.push(input);
                }
            }
        }
        assert_eq!(notifications.len(), 1);
        let Input::WorkerSnapshots(_, pending) = notifications.pop().unwrap() else {
            panic!("Expected one coalesced snapshot notification");
        };
        let latest = std::mem::take(&mut *pending.lock());
        let expected = inventory(4 * QUEUE as u64, AuthorityAttemptPhase::Quiescent);
        assert_eq!(
            latest
                .inventory
                .map(authority_worker_update::Update::Reconciliation),
            expected.update
        );
        assert_eq!(
            latest.capacity.unwrap().observation_sequence,
            4 * QUEUE as u64
        );
        // An update after the snapshot is taken must queue another turn, even
        // while the consumer is still awaiting the preceding journal commit.
        assert!(
            worker_input(
                session(),
                &mut snapshots,
                inventory(4 * QUEUE as u64 + 1, AuthorityAttemptPhase::Quiescent),
            )
            .is_some()
        );
    }

    #[test]
    fn standalone_report_seals_preceding_snapshot_before_later_capacity() {
        let mut snapshots = Arc::new(Mutex::new(WorkerSnapshots::default()));
        let first = worker_input(
            session(),
            &mut snapshots,
            inventory(1, AuthorityAttemptPhase::Preparing),
        )
        .unwrap();
        let report = AuthorityAttemptReport {
            observation_sequence: 2,
            phase: AuthorityAttemptPhase::Running as i32,
            ..AuthorityAttemptReport::default()
        };
        let barrier = worker_input(
            session(),
            &mut snapshots,
            AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Report(report.clone())),
            },
        )
        .unwrap();
        let last = worker_input(
            session(),
            &mut snapshots,
            AuthorityWorkerUpdate {
                update: Some(authority_worker_update::Update::Capacity(
                    AuthorityCapacityObservation {
                        session: Some(session()),
                        observation_sequence: 3,
                        ..AuthorityCapacityObservation::default()
                    },
                )),
            },
        )
        .unwrap();
        assert!(
            worker_input(
                session(),
                &mut snapshots,
                inventory(4, AuthorityAttemptPhase::Quiescent),
            )
            .is_none()
        );
        let Input::WorkerSnapshots(_, before) = first else {
            panic!("Missing first snapshot");
        };
        assert_eq!(
            before.lock().inventory.as_ref().unwrap().attempts[1].observation_sequence,
            1
        );
        assert!(before.lock().capacity.is_none());
        let Input::Worker(_, actual) = barrier else {
            panic!("Missing standalone report");
        };
        assert_eq!(
            actual.update,
            Some(authority_worker_update::Update::Report(report))
        );
        let Input::WorkerSnapshots(_, after) = last else {
            panic!("Missing later snapshot");
        };
        assert!(!Arc::ptr_eq(&before, &after));
        assert_eq!(
            after.lock().capacity.as_ref().unwrap().observation_sequence,
            3
        );
        assert_eq!(
            after.lock().inventory.as_ref().unwrap().attempts[1].observation_sequence,
            4
        );
    }

    #[test]
    fn completion_checks_committed_encoding_and_preserves_supported_response() {
        let response = ExecuteResponse {
            result: Some(ActionResult {
                stdout_raw: vec![255; 100].into(),
                ..ActionResult::default()
            }),
            ..ExecuteResponse::default()
        };
        assert_eq!(
            completion_payload(&response)
                .unwrap()
                .require_response()
                .unwrap()
                .decode()
                .unwrap(),
            response
        );
        for response in [
            ExecuteResponse {
                result: Some(ActionResult {
                    stdout_raw: vec![255; FRAME_BYTES - 128].into(),
                    ..ActionResult::default()
                }),
                ..ExecuteResponse::default()
            },
            ExecuteResponse {
                status: Some(nativelink_proto::google::rpc::Status {
                    code: tonic::Code::Internal as i32,
                    message: "\u{0001}".repeat(9000),
                    details: Vec::new(),
                }),
                ..ExecuteResponse::default()
            },
        ] {
            assert!(response.encoded_len() < FRAME_BYTES);
            let original = ResultPayload::from_response(&response, FRAME_BYTES).unwrap();
            assert!(serde_json::to_vec(&original).unwrap().len() > FRAME_BYTES);
            let result = completion_payload(&response).unwrap();
            assert!(serde_json::to_vec(&result).unwrap().len() < 1024);
            let terminal = result.require_response().unwrap().decode().unwrap();
            assert_eq!(
                terminal.status.unwrap().code,
                tonic::Code::ResourceExhausted as i32
            );
            assert!(terminal.result.is_none());
        }
        assert!(completion_payload(&ExecuteResponse::default()).is_none());
    }
}

#[cfg(test)]
#[path = "history_delivery_tests.rs"]
mod history_delivery_tests;

#[cfg(test)]
#[path = "authority_deadline_tests.rs"]
mod deadline_tests;

#[cfg(test)]
#[path = "authority_running_clock_tests.rs"]
mod running_clock_tests;

#[cfg(test)]
#[path = "authority_storage_read_tests.rs"]
mod storage_read_tests;
