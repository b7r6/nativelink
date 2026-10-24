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

//! Captured Action durations at admission, journal recovery and driver expiry.
//! These fixtures run no worker process and do not qualify a canonical clock.

use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{Command, Platform};
use nativelink_scheduler::authority::State;
use nativelink_scheduler::authority::journal::{Batch, Journal};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt};
use openraft::{Entry, EntryPayload, LogId, RaftSnapshotBuilder};

use super::*;

const fn durations() -> [(Option<prost_types::Duration>, u64); 6] {
    [
        (
            Some(prost_types::Duration {
                seconds: 0,
                nanos: 0,
            }),
            1,
        ),
        (None, u64::MAX),
        (
            Some(prost_types::Duration {
                seconds: 0,
                nanos: 1,
            }),
            1,
        ),
        (
            Some(prost_types::Duration {
                seconds: 1,
                nanos: 123_456_789,
            }),
            2,
        ),
        (
            Some(prost_types::Duration {
                seconds: 315_576_000_000,
                nanos: 0,
            }),
            315_576_000_000,
        ),
        (
            Some(prost_types::Duration {
                seconds: 315_576_000_000,
                nanos: 999_999_999,
            }),
            315_576_000_001,
        ),
    ]
}

fn digest(bytes: &[u8]) -> Digest {
    let mut hasher = DigestHasherFunc::Sha256.hasher();
    hasher.update(bytes);
    hasher.finalize_digest().into()
}

fn captured_request(timeout: Option<prost_types::Duration>) -> AuthorityRequest {
    let command = Command {
        arguments: vec!["/bin/true".into()],
        ..Command::default()
    }
    .encode_to_vec();
    let action = Action {
        command_digest: Some(digest(&command)),
        input_root_digest: Some(digest(&[])),
        timeout,
        do_not_cache: true,
        platform: Some(Platform {
            properties: vec![
                Property {
                    name: "cpu_count".into(),
                    value: "1".into(),
                },
                Property {
                    name: "memory_kb".into(),
                    value: "1".into(),
                },
            ],
        }),
        ..Action::default()
    };
    let mut bytes = action.encode_to_vec();
    // Preserve a valid unknown Action field through capture and recovery too.
    bytes.extend_from_slice(&[0xf8, 0xf0, 0x04, 1]);
    let execute = ExecuteRequest {
        instance_name: "running-clock".into(),
        action_digest: Some(digest(&bytes)),
        digest_function: 1,
        skip_cache_lookup: true,
        ..ExecuteRequest::default()
    };
    AuthorityRequest::from_payload(
        RequestPayload::capture(
            &execute,
            None,
            &Timestamp {
                seconds: 1_700_000_000,
                nanos: 17,
            },
            "local-test",
            &bytes,
            &command,
            FRAME_BYTES,
        )
        .unwrap(),
    )
    .unwrap()
}

fn assert_grant(view: &ObservableState, request: &AuthorityRequest) {
    assert_eq!(view.executions.len(), 1);
    assert_eq!(view.executions[0].request, *request);
    assert_eq!(view.attempts.len(), 1);
    assert_eq!(view.attempts[0].phase, Phase::Dispatching);
    assert_eq!(view.attempts[0].running_deadline, None);
    assert!(view.attempts[0].authorized);
    assert_eq!(view.reservations[0].charged, request.resources);
    assert!(!view.reservations[0].released);
    assert_eq!(view.workers[0].charged, request.resources);
    let dispatch = view
        .effects
        .iter()
        .find_map(|effect| match &effect.kind {
            EffectKind::Dispatch {
                request,
                charged,
                reservation,
                ..
            } => Some((request, charged, reservation)),
            _ => None,
        })
        .unwrap();
    assert_eq!(dispatch.0, request);
    assert_eq!(*dispatch.1, request.resources);
    assert_eq!(*dispatch.2, view.reservations[0].id);
    assert_eq!(
        dispatch.0.require_payload().unwrap().action_bytes(),
        request.require_payload().unwrap().action_bytes()
    );
}

#[nativelink_test]
async fn captured_durations_replay_committed_submission_and_grant_before_worker_acceptance() {
    for (timeout, projected) in durations() {
        let request = captured_request(timeout);
        assert_eq!(request.execution_timeout, projected);
        let directory = std::env::temp_dir().join(format!(
            "authority-captured-timeout-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&directory).await.unwrap();
        let path = directory.join("authority.redb");
        let authority = Singleton::open(&path, 1, EPOCH, Limits::default())
            .await
            .unwrap();
        assert_eq!(
            authority
                .write(RecordedEvent {
                    epoch: EPOCH,
                    event: Event::Register {
                        worker: WorkerId(1),
                        incarnation: IncarnationId(1),
                        session: SessionId(1),
                        properties: request.properties.clone(),
                        capacity: request.resources,
                        available: request.resources,
                    }
                })
                .await
                .unwrap()
                .data,
            Reply::Applied
        );
        authority.journal.clone().build_snapshot().await.unwrap();
        authority.shutdown().await.unwrap();

        // Model a committed suffix persisted before state-machine application;
        // this does not claim a second consensus path or a power-loss test.
        let mut journal = Journal::open(&path, 1, EPOCH, Limits::default())
            .await
            .unwrap();
        let last = journal.get_log_state().await.unwrap().last_log_id.unwrap();
        let suffix = LogId::new(last.leader_id, last.index + 1);
        journal
            .blocking_append([Entry {
                log_id: suffix,
                payload: EntryPayload::Normal(
                    Batch::new([
                        RecordedEvent {
                            epoch: EPOCH,
                            event: Event::Submit {
                                operation: OperationId(1),
                                request: request.clone(),
                            },
                        },
                        RecordedEvent {
                            epoch: EPOCH,
                            event: Event::Match,
                        },
                    ])
                    .unwrap(),
                ),
            }])
            .await
            .unwrap();
        journal.save_committed(Some(suffix)).await.unwrap();
        assert_eq!(
            journal.read_applied().await.unwrap().1.view().operations,
            Vec::<nativelink_scheduler::authority::OperationView>::new()
        );
        drop(journal);

        let authority = Singleton::open(&path, 1, EPOCH, Limits::default())
            .await
            .unwrap();
        let state = authority.read().await.unwrap();
        assert_grant(&state.view(), &request);
        let restored = State::restore(&state.snapshot().unwrap()).unwrap();
        assert_grant(&restored.view(), &request);
        assert_eq!(
            Action::decode(
                restored.view().executions[0]
                    .request
                    .require_payload()
                    .unwrap()
                    .action_bytes()
            )
            .unwrap()
            .timeout,
            timeout
        );
        assert!(
            authority
                .journal
                .read_applied()
                .await
                .unwrap()
                .0
                .unwrap()
                .index
                >= suffix.index
        );
        authority.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

#[nativelink_test]
async fn captured_durations_keep_driver_boundary_and_request_bytes_on_reopen() {
    for (timeout, projected) in durations() {
        let request = captured_request(timeout);
        let (mut driver, directory) = Fixture::with_request(Phase::Running, 0, request.clone())
            .await
            .stop()
            .await;
        assert_eq!(driver.state.executions[0].request, request);
        assert_eq!(driver.state.attempts[0].running_deadline, Some(projected));
        let finite_short = projected <= 2;
        let boundary = if finite_short { projected } else { 2 };
        assert_eq!(
            recorded_sweep(&mut driver, boundary).await,
            vec![Event::AdvanceTime { now: boundary }]
        );
        assert!(driver.state.attempts[0].authorized);
        assert!(!driver.state.reservations[0].released);
        if finite_short {
            assert_eq!(
                recorded_sweep(&mut driver, boundary + 1).await,
                vec![
                    Event::AdvanceTime { now: boundary + 1 },
                    Event::ExpireExecution {
                        attempt: driver.state.attempts[0].id
                    },
                ]
            );
            assert!(!driver.state.attempts[0].authorized);
            assert!(!driver.state.reservations[0].released);
        }
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        let reopened = Singleton::open(
            directory.join("authority.redb"),
            1,
            EPOCH,
            Limits::default(),
        )
        .await
        .unwrap();
        // Recovery must preserve capture/projection; it must not reinterpret
        // explicit zero as omission or add a tick to the persisted v1 request.
        driver.config.now = Arc::new(|| Ok(0));
        let (service, coordinator) = AuthorityService::start(reopened, driver.config)
            .await
            .unwrap();
        let state = service.subscribe().borrow().clone();
        assert_eq!(state.executions[0].request, request);
        assert_eq!(state.attempts[0].running_deadline, Some(projected));
        assert_eq!(state.attempts[0].authorized, !finite_short);
        assert!(!state.reservations[0].released);
        coordinator.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
}

#[nativelink_test]
async fn diagnostic_finite_maximum_deadline_aliases_omission_without_clock_headroom() {
    let finite = Some(prost_types::Duration {
        seconds: 315_576_000_000,
        nanos: 999_999_999,
    });
    let base = u64::MAX - 315_576_000_001;
    for timeout in [finite, None] {
        let request = captured_request(timeout);
        let (mut driver, directory) =
            Fixture::with_request_at(Phase::Dispatching, base, request.clone(), base)
                .await
                .stop()
                .await;
        assert_grant(&driver.state, &request);
        assert_eq!(driver.state.now, base);
        assert_eq!(driver.state.attempts[0].deadline.at, base + 10);
        // The controlled high tick precedes submission and the resource grant.
        // No expired progress window is skipped or renewed in this diagnostic.
        for event in [
            Event::WorkerAccepted {
                attempt: AttemptId(1),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
            },
            Event::Progress {
                attempt: AttemptId(1),
                worker: WorkerId(1),
                incarnation: IncarnationId(1),
                phase: Phase::Running,
                progress: 1,
            },
        ] {
            assert_eq!(driver.commit(event).await.unwrap(), Reply::Applied);
        }
        assert_eq!(driver.state.attempts[0].running_deadline, Some(u64::MAX));
        assert_eq!(
            driver.state.executions[0].request.execution_timeout == u64::MAX,
            timeout.is_none()
        );
        assert!(
            driver.state.attempts[0]
                .running_deadline
                .unwrap()
                .checked_add(1)
                .is_none()
        );
        driver.restore_deadlines = true;
        assert_eq!(recorded_sweep(&mut driver, base).await, Vec::<Event>::new());
        assert!(driver.state.attempts[0].authorized);
        assert!(!driver.state.reservations[0].released);
        driver.workers.clear();
        driver.authority.shutdown().await.unwrap();
        let reopened = Singleton::open(
            directory.join("authority.redb"),
            1,
            EPOCH,
            Limits::default(),
        )
        .await
        .unwrap();
        let recovered = reopened.read().await.unwrap().view();
        assert_eq!(recovered.executions[0].request, request);
        assert_eq!(recovered.attempts[0].running_deadline, Some(u64::MAX));
        assert!(!recovered.reservations[0].released);
        reopened.shutdown().await.unwrap();
        fs::remove_dir_all(directory).await.unwrap();
    }
    // This diagnostic documents a missing canonical admission contract. It
    // neither selects saturation as policy nor asserts indefinite execution:
    // independent progress/interest expiry can still revoke authorization.
}
