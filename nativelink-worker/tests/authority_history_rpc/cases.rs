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

//! Accepted history from a real namespace process and retained RPC recipient.

use nativelink_config::stores::VerifySpec;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    HistoricalExecuteResponse, authority_attempt_report,
};
use nativelink_scheduler::authority::ExecutionId;
use nativelink_scheduler::authority::accepted_history::{
    HistoryAlgorithm, HistoryAssociation, HistoryStatus,
};
use nativelink_scheduler::authority::format::{ACCEPTED_HISTORY, accepted_history_namespace};
use nativelink_store::verify_store::VerifyStore;

use super::*;

const DOMAIN: AuthorityDomain = AuthorityDomain {
    epoch: EPOCH,
    namespace: 97,
};

impl Runtime {
    pub(super) async fn start_history(
        context: &Context,
        clock: Arc<AtomicU64>,
        faults: Arc<QualificationFaults>,
        worker_address: Option<SocketAddr>,
    ) -> Self {
        let path = context.directory.path().join("authority.redb");
        let admission = if path.exists() {
            // The fixture has joined the previous coordinator before reopening.
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
                incarnation: context
                    .authority_incarnation
                    .fetch_add(1, Ordering::Relaxed)
                    + 1,
                reader: ReaderCapabilities::AcceptedHistory,
                admission,
            },
        )
        .await
        .unwrap();
        let required = BTreeSet::from([RECEIPT_RETIREMENT, ACCEPTED_HISTORY]);
        if authority.journal.active_format().await.unwrap().required != required {
            authority
                .journal
                .activate_features(&authority.raft, required.clone())
                .await
                .unwrap();
        }
        assert_eq!(
            authority.journal.active_format().await.unwrap().required,
            required
        );
        Self::start_authority(
            context,
            clock,
            faults,
            worker_address,
            authority,
            accepted_history_namespace(DOMAIN),
        )
        .await
    }

    pub(super) async fn history_association(
        &self,
        execution: ExecutionId,
        status: HistoryStatus,
    ) -> HistoryAssociation {
        let mut history = self.service.subscribe_history();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let snapshot = history.borrow_and_update().clone();
                if let Some(state) = snapshot.as_ref()
                    && let Some(association) = state.associations.get(&execution)
                    && association.status == status
                {
                    assert_eq!(state.associations.len(), 1);
                    return association.clone();
                }
                history.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|error| panic!("History wait failed: {error}; {:?}", history.borrow()))
    }

    pub(super) async fn assert_history_terminal(&mut self, expected: &Operation) {
        let mut stream = self
            .client
            .wait_execution(WaitExecutionRequest {
                name: expected.name.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(&terminal(&mut stream).await, expected);
    }
}

// Wire recipe comes from the public HistoricalExecuteResponse protocol: Action
// Digest is field 1; the original ExecuteResponse bytes are field 3. Do not use
// the candidate history encoder or decode/re-encode the captured response.
pub(super) fn history_bytes(action: DigestInfo, response: &[u8]) -> Vec<u8> {
    let mut bytes = HistoricalExecuteResponse {
        action_digest: Some(action.into()),
        execute_response: None,
    }
    .encode_to_vec();
    bytes.push(0x1a);
    let mut length = response.len();
    while length >= 0x80 {
        bytes.push(u8::try_from(length & 0x7f).unwrap() | 0x80);
        length >>= 7;
    }
    bytes.push(u8::try_from(length).unwrap());
    bytes.extend_from_slice(response);
    bytes
}

async fn actual_process_history(algorithm: DigestHasherFunc, do_not_cache: bool, exit_code: i32) {
    process_history_with_manager(
        algorithm,
        do_not_cache,
        exit_code,
        ManagerConstruction::LegacyPublication,
    )
    .await;
}

async fn process_history_with_manager(
    algorithm: DigestHasherFunc,
    do_not_cache: bool,
    exit_code: i32,
    construction: ManagerConstruction,
) {
    if !namespaces() {
        return;
    }
    let contents = MemoryStore::new(&MemorySpec::default());
    let verified = VerifyStore::new(
        &VerifySpec {
            verify_hash: true,
            verify_size: true,
            backend: StoreSpec::Memory(MemorySpec::default()),
        },
        Store::new(contents.clone()),
    );
    let context = Context::with_manager(
        false,
        Store::new(verified),
        Duration::from_secs(10),
        8,
        Duration::from_secs(30),
        construction,
    )
    .await;
    let marker = context.directory.path().join("executed");
    let release = context.directory.path().join("release");
    let (request, action, action_bytes) = context
        .command_request_with_policy_and_algorithm(
            30,
            Command {
                arguments: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    format!(
                        "printf x >> '{}'; while [ ! -f '{}' ]; do /bin/sleep 0.02; done; printf artifact > output; printf stdout; printf stderr >&2; exit {exit_code}",
                        marker.display(),
                        release.display(),
                    ),
                ],
                output_files: vec!["output".into()],
                ..Command::default()
            },
            do_not_cache,
            algorithm,
        )
        .await;
    assert_eq!(
        request.digest_function,
        i32::from(algorithm.proto_digest_func())
    );
    let decoded_action = Action::decode(action_bytes.as_slice()).unwrap();
    for digest in [
        action,
        DigestInfo::try_from(decoded_action.command_digest.unwrap()).unwrap(),
        DigestInfo::try_from(decoded_action.input_root_digest.unwrap()).unwrap(),
    ] {
        let bytes = contents.get_part_unchunked(digest, 0, None).await.unwrap();
        let mut hasher = algorithm.hasher();
        hasher.update(&bytes);
        assert_eq!(
            hasher.finalize_digest(),
            digest,
            "uploaded input uses the requested digest function"
        );
    }
    let clock = Arc::new(AtomicU64::new(0));
    let faults = Arc::new(QualificationFaults::default());
    // Ordinary publication is held throughout, so an AC write can only have
    // come from the worker. The original fixture enables legacy Everything;
    // the additive construction has no publisher at all.
    faults.hold_publications.store(true, Ordering::Relaxed);
    faults
        .hold_history_publications
        .store(true, Ordering::Relaxed);
    let mut runtime = Runtime::start_history(&context, clock.clone(), faults.clone(), None).await;
    let address = runtime.worker_address;
    let (stop, stopped) = watch::channel(false);
    let supervisor = background_spawn!(
        "authority_history_real_process",
        run_reconnecting(
            context.worker.clone(),
            Endpoint::from_shared(format!("http://{address}"))
                .unwrap()
                .connect_timeout(Duration::from_secs(1)),
            1,
            Vec::new(),
            stopped,
        )
    );
    runtime.wait(|view| view.workers.len() == 1).await;
    let mut stream = runtime
        .client
        .execute(request.clone())
        .await
        .unwrap()
        .into_inner();
    let name = next_operation(&mut stream).await.name;
    let running = runtime
        .wait(|view| {
            view.attempts
                .first()
                .is_some_and(|attempt| attempt.phase == Phase::Running)
                && !view
                    .effects
                    .iter()
                    .any(|effect| matches!(effect.kind, EffectKind::Dispatch { .. }))
        })
        .await;
    let execution = running.attempts[0].execution;
    assert_eq!(
        running.executions[0].request.action.digest_function,
        algorithm.to_string().to_lowercase()
    );
    context.assert_once();
    runtime.shutdown().await;
    drop(stream);
    context.release();
    let mut changed = context.worker.subscribe();
    let candidate = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let inventory = context.worker.reconcile();
            if let Some(report) = inventory.attempts.iter().find(|report| {
                report.phase == AuthorityAttemptPhase::Quiescent as i32 && report.result.is_some()
            }) {
                let Some(authority_attempt_report::Result::ExecuteResponse(response)) =
                    &report.result
                else {
                    panic!("Real process failed before candidate creation: {report:?}");
                };
                break response.clone();
            }
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let result = candidate.result.as_ref().unwrap();
    assert_eq!(result.exit_code, exit_code);
    assert_eq!(result.output_files.len(), 1);
    assert_eq!(result.output_files[0].path, "output");
    assert!(
        candidate.message.is_empty(),
        "Legacy history template leaked"
    );
    assert_eq!(context.history.len_for_test(), 0);
    assert_eq!(context.ac.len_for_test(), 0);
    for (digest, expected) in [
        (
            result.output_files[0].digest.as_ref().unwrap(),
            b"artifact".as_slice(),
        ),
        (result.stdout_digest.as_ref().unwrap(), b"stdout".as_slice()),
        (result.stderr_digest.as_ref().unwrap(), b"stderr".as_slice()),
    ] {
        let mut hasher = algorithm.hasher();
        hasher.update(expected);
        assert_eq!(
            hasher.finalize_digest(),
            DigestInfo::try_from(digest).unwrap(),
            "worker output uses the requested digest function"
        );
        assert_eq!(
            contents
                .get_part_unchunked(DigestInfo::try_from(digest).unwrap(), 0, None)
                .await
                .unwrap()
                .as_ref(),
            expected
        );
    }

    // The offline retained candidate reaches the authority through the real
    // reconnect inventory. Only its committed acceptance creates the history.
    runtime = Runtime::start_history(&context, clock.clone(), faults.clone(), Some(address)).await;
    let mut stream = runtime
        .client
        .wait_execution(WaitExecutionRequest { name })
        .await
        .unwrap()
        .into_inner();
    let accepted = terminal(&mut stream).await;
    let Some(operation::Result::Response(captured)) = &accepted.result else {
        panic!("Accepted operation has no response");
    };
    assert_eq!(captured.value, candidate.encode_to_vec());
    let cacheable_success = !do_not_cache && exit_code == 0;
    assert_eq!(
        runtime
            .service
            .subscribe()
            .borrow()
            .effects
            .iter()
            .filter(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
            .count(),
        usize::from(cacheable_success)
    );
    let pending = runtime
        .history_association(execution, HistoryStatus::Pending)
        .await;
    let expected = history_bytes(action, &captured.value);
    let mut hasher = algorithm.hasher();
    hasher.update(&expected);
    let digest = hasher.finalize_digest();
    assert_eq!(
        pending.recipe.object.namespace,
        accepted_history_namespace(DOMAIN)
    );
    assert_eq!(pending.recipe.object.hash, **digest.packed_hash());
    assert_eq!(
        pending.recipe.object.algorithm,
        match algorithm {
            DigestHasherFunc::Sha256 => HistoryAlgorithm::Sha256,
            DigestHasherFunc::Blake3 => HistoryAlgorithm::Blake3,
        }
    );
    assert_eq!(pending.recipe.object.size_bytes, digest.size_bytes());
    assert_eq!(contents.has(digest).await.unwrap(), None);
    assert_eq!(context.history.len_for_test(), 0);
    assert_eq!(context.ac.len_for_test(), 0);
    drop(stream);
    runtime.shutdown().await;

    runtime = Runtime::start_history(&context, clock, faults.clone(), Some(address)).await;
    assert_eq!(
        runtime
            .history_association(execution, HistoryStatus::Pending)
            .await,
        pending
    );
    runtime.assert_history_terminal(&accepted).await;
    faults
        .hold_history_publications
        .store(false, Ordering::Relaxed);
    let materialized = runtime
        .history_association(execution, HistoryStatus::Materialized)
        .await;
    assert_eq!(
        materialized,
        HistoryAssociation {
            status: HistoryStatus::Materialized,
            ..pending
        }
    );
    let stored = contents.get_part_unchunked(digest, 0, None).await.unwrap();
    assert_eq!(stored.as_ref(), expected);
    let decoded = HistoricalExecuteResponse::decode(stored).unwrap();
    assert_eq!(decoded.action_digest, Some(action.into()));
    assert_eq!(decoded.execute_response, Some(candidate));
    runtime.assert_history_terminal(&accepted).await;
    context.assert_once();
    assert_eq!(context.history.len_for_test(), 0);
    assert_eq!(context.ac.len_for_test(), 0);
    if cacheable_success {
        // Only the authority has the sink handle. Its accepted effect survives
        // both restarts before publication is allowed to run.
        faults.hold_publications.store(false, Ordering::Relaxed);
        runtime
            .wait(|view| {
                !view
                    .effects
                    .iter()
                    .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
            })
            .await;
        let stored = ActionResult::decode(
            context
                .ac
                .get_part_unchunked(action, 0, None)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            Some(stored),
            decoded.execute_response.as_ref().unwrap().result
        );
        let mut lookup = request;
        lookup.skip_cache_lookup = false;
        let mut cached = runtime.client.execute(lookup).await.unwrap().into_inner();
        let cached_response = response(&terminal(&mut cached).await);
        assert!(cached_response.cached_result);
        assert_eq!(
            cached_response.result,
            decoded.execute_response.as_ref().unwrap().result
        );
        runtime.assert_history_terminal(&accepted).await;
        context.assert_once();
    }
    stop.send_replace(true);
    supervisor.await.unwrap().unwrap();
    runtime.shutdown().await;
    let construction = match construction {
        ManagerConstruction::LegacyPublication => "legacy-enabled",
        ManagerConstruction::AuthorityOnly | ManagerConstruction::AuthorityOnlyBounded { .. } => {
            "authority-only"
        }
    };
    eprintln!(
        "AUTHORITY_REAL_PROCESS_HISTORY: namespaces executed; algorithm={algorithm}; do_not_cache={do_not_cache}; exit_code={exit_code}; construction={construction}; inputs/output/stdout/stderr verified in CAS; worker did not publish; feature2+3 accepted candidate preserved; pending history survived restart; exact captured response history verified in CAS; terminal unchanged"
    );
}

#[nativelink_test]
async fn do_not_cache_real_process_publishes_accepted_history_after_restart() {
    actual_process_history(DigestHasherFunc::Sha256, true, 0).await;
}

#[nativelink_test]
async fn nonzero_exit_real_process_publishes_accepted_history_after_restart() {
    actual_process_history(DigestHasherFunc::Sha256, false, 7).await;
}

#[nativelink_test]
async fn blake3_do_not_cache_real_process_publishes_accepted_history_after_restart() {
    actual_process_history(DigestHasherFunc::Blake3, true, 0).await;
}

#[nativelink_test]
async fn blake3_nonzero_exit_real_process_publishes_accepted_history_after_restart() {
    actual_process_history(DigestHasherFunc::Blake3, false, 7).await;
}

#[nativelink_test]
async fn authority_only_manager_preserves_real_process_history_across_restart() {
    for algorithm in [DigestHasherFunc::Sha256, DigestHasherFunc::Blake3] {
        for (do_not_cache, exit_code) in [(true, 0), (false, 7), (false, 0)] {
            process_history_with_manager(
                algorithm,
                do_not_cache,
                exit_code,
                ManagerConstruction::AuthorityOnly,
            )
            .await;
        }
    }
}
