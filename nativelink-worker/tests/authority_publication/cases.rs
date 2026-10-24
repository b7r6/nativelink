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

//! One original actual authority from captured request through physical publication.

use nativelink_proto::build::bazel::remote::execution::v2::Digest;
use nativelink_store::physical_closure::{Edge, EdgeKind};
use nativelink_store::physical_filesystem::{Algorithm, ObjectId};
use nativelink_util::digest_hasher::make_ctx_for_hash_func;
use opentelemetry::context::FutureExt;

use super::*;

#[path = "../../../nativelink-scheduler/tests/retention/coarse.rs"]
mod coarse;
#[allow(
    dead_code,
    reason = "This process experiment reuses a subset of the frozen candidate API."
)]
#[path = "../../../nativelink-scheduler/tests/publication_candidate/engine.rs"]
mod engine;
#[allow(
    dead_code,
    reason = "Shared physical fixture includes replacement and recovery cases in its owning suite."
)]
#[path = "../../../nativelink-scheduler/tests/publication_physical/fixture.rs"]
mod physical;
#[allow(
    dead_code,
    reason = "The frozen retention protocol includes collector operations outside this experiment."
)]
#[path = "../../../nativelink-scheduler/tests/retention/protocol.rs"]
mod protocol;
#[allow(
    dead_code,
    reason = "Shared candidate types include synthetic comparison variants."
)]
#[path = "../../../nativelink-scheduler/tests/publication_candidate/types.rs"]
mod types;

use engine::PublicationAuthority;
use physical::{ACTION, Fixture, Graph, INCARNATION, NAMESPACE, Row, algorithm_name, hex};
use types::{ActionBinding, Key, MAX_PAYLOAD, Publisher, Role, TrustedContext};

fn trusted() -> TrustedContext {
    TrustedContext {
        principal: "worker-publication-principal".into(),
        execution_config: "namespace-default-config-v1".into(),
    }
}
async fn actual_request(context: &Context, function: DigestHasherFunc) -> (Request, DigestInfo) {
    let command = Command {
        arguments: vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf artifact > output; printf stdout; printf stderr >&2".into(),
        ],
        output_files: vec!["output".into()],
        ..Command::default()
    };
    let command_digest =
        serialize_and_upload_message(&command, context.cas.as_pin(), &mut function.hasher())
            .await
            .unwrap();
    let root_digest = serialize_and_upload_message(
        &Directory::default(),
        context.cas.as_pin(),
        &mut function.hasher(),
    )
    .await
    .unwrap();
    let action = Action {
        command_digest: Some(command_digest.into()),
        input_root_digest: Some(root_digest.into()),
        timeout: Some(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        platform: Some(Platform {
            properties: vec![
                Property {
                    name: "cpu_count".into(),
                    value: "1".into(),
                },
                Property {
                    name: "memory_kb".into(),
                    value: "1024".into(),
                },
            ],
        }),
        ..Action::default()
    };
    let action_digest =
        serialize_and_upload_message(&action, context.cas.as_pin(), &mut function.hasher())
            .await
            .unwrap();
    let execute = ExecuteRequest {
        instance_name: "physical-process".into(),
        action_digest: Some(action_digest.into()),
        digest_function: match function {
            DigestHasherFunc::Sha256 => 1,
            DigestHasherFunc::Blake3 => 9,
        },
        skip_cache_lookup: true,
        ..ExecuteRequest::default()
    };
    let payload = RequestPayload::capture(
        &execute,
        Some(&RequestMetadata {
            tool_invocation_id: "physical-process-invocation".into(),
            ..RequestMetadata::default()
        }),
        &Timestamp {
            seconds: 1_800_000_000,
            nanos: 17,
        },
        &trusted().principal,
        &action.encode_to_vec(),
        &command.encode_to_vec(),
        MAX_PAYLOAD,
    )
    .unwrap();
    (Request::from_payload(payload).unwrap(), action_digest)
}
fn expected_digest(object: ObjectId) -> Digest {
    Digest {
        hash: hex(&object.hash),
        size_bytes: i64::try_from(object.size).unwrap(),
    }
}
async fn output_graph(context: &Context, algorithm: Algorithm, response: ResultPayload) -> Graph {
    let full = response.require_response().unwrap().decode().unwrap();
    assert_eq!(full.status.as_ref().map_or(0, |status| status.code), 0);
    assert!(!full.cached_result);
    let result = full.result.unwrap();
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.output_files.len(), 1);
    assert_eq!(result.output_files[0].path, "output");
    assert!(!result.output_files[0].is_executable);
    assert_eq!(
        result.output_directories,
        Vec::<nativelink_proto::build::bazel::remote::execution::v2::OutputDirectory>::new()
    );
    assert_eq!(
        result.output_symlinks,
        Vec::<nativelink_proto::build::bazel::remote::execution::v2::OutputSymlink>::new()
    );
    assert_eq!(
        result.output_file_symlinks,
        Vec::<nativelink_proto::build::bazel::remote::execution::v2::OutputSymlink>::new()
    );
    assert_eq!(
        result.output_directory_symlinks,
        Vec::<nativelink_proto::build::bazel::remote::execution::v2::OutputSymlink>::new()
    );
    assert!(result.stdout_raw.is_empty());
    assert!(result.stderr_raw.is_empty());
    let result_bytes = result.encode_to_vec();
    let mut rows = Vec::new();
    for (bytes, digest) in [
        (
            b"artifact".as_slice(),
            result.output_files[0].digest.as_ref(),
        ),
        (b"stdout".as_slice(), result.stdout_digest.as_ref()),
        (b"stderr".as_slice(), result.stderr_digest.as_ref()),
    ] {
        let object = ObjectId::for_bytes(algorithm, bytes);
        assert_eq!(digest, Some(&expected_digest(object)));
        let cas_bytes = context
            .cas
            .get_part_unchunked(DigestInfo::try_from(digest.unwrap()).unwrap(), 0, None)
            .await
            .unwrap();
        assert_eq!(cas_bytes.as_ref(), bytes);
        rows.push(Row {
            key: Key {
                namespace: NAMESPACE,
                incarnation: INCARNATION,
                generation: rows.len() as u64 + 1,
            },
            object,
            role: Role::Blob,
            bytes: cas_bytes.to_vec(),
        });
    }
    let result_object = ObjectId::for_bytes(algorithm, &result_bytes);
    let edges = [EdgeKind::OutputFile, EdgeKind::Stdout, EdgeKind::Stderr]
        .into_iter()
        .zip(&rows)
        .map(|(kind, row)| Edge {
            source: result_object,
            embedded_directory: None,
            target: row.object,
            kind,
        })
        .collect();
    rows.push(Row {
        key: Key {
            namespace: NAMESPACE,
            incarnation: INCARNATION,
            generation: 4,
        },
        object: result_object,
        role: Role::Result,
        bytes: result_bytes,
    });
    Graph {
        rows,
        retained: Vec::new(),
        edges,
        response,
    }
}
async fn worker_publication(algorithm: Algorithm, function: DigestHasherFunc) {
    if !namespace_utils::namespaces_supported(false, false) {
        eprintln!("SKIP: namespaces unavailable; worker physical publication remains unqualified");
        return;
    }
    let context = Context::new(true).await;
    // This temporary legacy CAS is outside the new collector namespace. The
    // later copy is not a canonical source-copy pin or authoritative gateway.
    let (original, action_digest) = actual_request(&context, function)
        .with_context(make_ctx_for_hash_func(function).unwrap())
        .await;
    let authority = PublicationAuthority::new_real(
        trusted(),
        BTreeMap::from([(
            ACTION.into(),
            ActionBinding::capture(&trusted(), &original).unwrap(),
        )]),
    )
    .unwrap();
    let mut fixture = Fixture::new();
    fixture.state = authority;
    fixture.state.scheduler_event(register(1)).unwrap();
    context.worker.negotiate(&negotiation(1)).unwrap();
    let execution = fixture
        .state
        .admit_request(1, ACTION.into(), &trusted(), original.clone(), 100)
        .unwrap();
    assert_eq!(
        fixture
            .state
            .admit_request(2, ACTION.into(), &trusted(), original.clone(), 100)
            .unwrap(),
        execution
    );
    let Reply::Matched { attempt, .. } = fixture.state.scheduler_event(Event::Match).unwrap()
    else {
        panic!("actual request did not dispatch");
    };
    let view = fixture.state.scheduler_view();
    let effect = view.effects.iter().find(|effect| matches!(&effect.kind, EffectKind::Dispatch { attempt: id, .. } if *id == attempt)).unwrap().clone();
    let EffectKind::Dispatch {
        charged,
        request: dispatched,
        ..
    } = &effect.kind
    else {
        unreachable!()
    };
    assert_eq!(dispatched, &original);
    context
        .worker
        .command(1, dispatch_command(&effect, *charged))
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
    let Event::Complete { result: exact, .. } = completed(&report, &effect).unwrap() else {
        unreachable!()
    };
    assert_eq!(exact.exit_code, 0);
    assert!(exact.error.is_none());
    assert!(fixture.state.heads().is_empty());
    assert!(
        fixture
            .state
            .scheduler_view()
            .operations
            .iter()
            .all(|row| row.terminal.is_none())
    );
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_eq!(context.history.len_for_test(), 0);
    let graph = output_graph(&context, algorithm, exact.clone()).await;
    let publisher = Publisher::Worker {
        execution,
        attempt,
        worker: WorkerId(IDENTITY.worker_id),
        incarnation: IncarnationId(IDENTITY.incarnation_id),
    };
    fixture.stage(&graph, 1, publisher);
    assert!(fixture.state.heads().is_empty());
    fixture.write_all(&graph, 1);
    assert!(fixture.state.heads().is_empty());
    fixture.prepare(&graph, 1);
    assert!(fixture.state.heads().is_empty());
    fixture.state.publish(1, 1, 100).unwrap();
    assert_eq!(fixture.state.heads().get(ACTION), Some(&1));
    let accepted = fixture.state.scheduler_view();
    assert!(
        accepted
            .operations
            .iter()
            .all(|row| row.terminal == Some(TerminalResult::Completed(exact.clone())))
    );
    assert!(accepted.reservations.iter().all(|row| row.released));
    assert!(
        !accepted
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    for row in &graph.rows {
        fixture.read_operation(1, row);
        fixture.read_operation(2, row);
    }
    fixture
        .state
        .serve(ACTION, 1, fixture.state.commit(), 1, 100)
        .unwrap();
    for row in &graph.rows {
        fixture.read_grant(1, row);
    }
    let payload = original.require_payload().unwrap();
    let mut facts = payload.dispatch_facts().unwrap();
    facts.execute_request.skip_cache_lookup = false;
    let cached_request = Request::from_payload(
        RequestPayload::capture(
            &facts.execute_request,
            facts.request_metadata.as_ref(),
            &facts.queued_timestamp,
            &trusted().principal,
            payload.action_bytes(),
            payload.command_bytes(),
            MAX_PAYLOAD,
        )
        .unwrap(),
    )
    .unwrap();
    fixture
        .state
        .admit_request(3, ACTION.into(), &trusted(), cached_request, 100)
        .unwrap();
    let TerminalResult::Completed(cached) = fixture
        .state
        .scheduler_view()
        .operations
        .iter()
        .find(|row| row.id.0 == 3)
        .unwrap()
        .terminal
        .clone()
        .unwrap()
    else {
        panic!("expected cache hit");
    };
    let cached_response = cached.require_response().unwrap().decode().unwrap();
    assert!(cached_response.cached_result);
    assert_eq!(
        cached_response.result,
        exact.require_response().unwrap().decode().unwrap().result
    );
    fixture.reopen();
    for row in &graph.rows {
        fixture.read_operation(1, row);
        fixture.read_operation(2, row);
        fixture.read_operation(3, row);
        fixture.read_grant(1, row);
    }
    assert_eq!(
        fixture
            .state
            .scheduler_view()
            .executions
            .iter()
            .find(|row| row.id == execution)
            .unwrap()
            .request,
        original
    );
    assert_eq!(context.worker.reconcile().attempts[0], report);
    assert!(context.ac.has(action_digest).await.unwrap().is_none());
    assert_eq!(context.history.len_for_test(), 0);
    eprintln!(
        "PUBLICATION_WORKER_PASS {}: actual namespace process; original real request and same State; quiescent exact report; staged real writes; typed closure; atomic publication; joined operations; cached response; protected reads; reopen; worker retained report still unacknowledged",
        algorithm_name(algorithm)
    );
}
#[nativelink_test]
async fn actual_sha256_worker_outputs_publish_with_physical_retention() {
    worker_publication(Algorithm::Sha256, DigestHasherFunc::Sha256).await;
}
#[nativelink_test]
async fn actual_blake3_worker_outputs_publish_with_physical_retention() {
    worker_publication(Algorithm::Blake3, DigestHasherFunc::Blake3).await;
}

#[path = "late.rs"]
mod late;
