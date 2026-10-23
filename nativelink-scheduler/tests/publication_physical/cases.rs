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

//! Scenario assertions use expected graphs and exact bytes, not another reducer.

use std::collections::BTreeSet;

use nativelink_scheduler::authority as a;
use nativelink_store::physical_closure::{Failure as ClosureFailure, Limits, Preparation};
use nativelink_store::physical_filesystem::{Algorithm, ObjectId};

use super::fixture::{ACTION, Fixture, Graph, INCARNATION, NAMESPACE, algorithm_name, object};
use super::types::{Error, Key, Publisher};

fn rejected_unchanged<T: std::fmt::Debug>(
    fixture: &mut Fixture,
    expected: Error,
    change: impl FnOnce(&mut super::engine::PublicationAuthority) -> Result<T, Error>,
) {
    let before = fixture.state.snapshot().unwrap();
    assert_eq!(change(&mut fixture.state).unwrap_err(), expected);
    assert_eq!(fixture.state.snapshot().unwrap(), before);
}
fn terminal(fixture: &Fixture, operation: u64) -> a::TerminalResult {
    fixture
        .state
        .scheduler_view()
        .operations
        .into_iter()
        .find(|row| row.id == a::OperationId(operation))
        .unwrap()
        .terminal
        .unwrap()
}
fn completed(fixture: &Fixture, operation: u64, graph: &Graph) {
    assert_eq!(
        terminal(fixture, operation),
        a::TerminalResult::Completed(graph.response.clone())
    );
}
fn publish_direct(fixture: &mut Fixture, graph: &Graph, stage: u64, publication: u64) {
    fixture.stage(graph, stage, Publisher::Client { principal: 9 });
    fixture.write_all(graph, stage);
    fixture.prepare(graph, stage);
    fixture.state.publish(stage, publication, 100).unwrap();
}

fn replacement_and_reopen(algorithm: Algorithm) {
    let mut fixture = Fixture::new();
    let publisher = fixture.register_and_assign(1);
    let old = Graph::new(algorithm, NAMESPACE, INCARNATION, 1, "old");
    // A cancelled joined observer gets no result root when the worker completes.
    fixture.state.admit(3, ACTION.into(), true, 100).unwrap();
    fixture.state.cancel_joined(3).unwrap();
    fixture.stage(&old, 1, publisher);
    rejected_unchanged(&mut fixture, Error::Incomplete, |state| {
        state.publish(1, 101, 100)
    });
    assert!(fixture.state.heads().is_empty());
    assert!(
        fixture
            .state
            .scheduler_view()
            .operations
            .iter()
            .find(|row| row.id == a::OperationId(1))
            .unwrap()
            .terminal
            .is_none()
    );
    fixture.write_all(&old, 1);
    fixture.prepare(&old, 1);
    let before = fixture.state.commit();
    fixture.state.publish(1, 101, 100).unwrap();
    assert_eq!(fixture.state.commit(), before + 1);
    assert_eq!(fixture.state.accepted()[&101].commit, before + 1);
    assert_eq!(fixture.state.heads()[ACTION], 101);
    completed(&fixture, 1, &old);
    assert_eq!(terminal(&fixture, 3), a::TerminalResult::Cancelled);
    assert!(fixture.state.operations()[&3].root.is_none());
    assert!(
        fixture
            .state
            .scheduler_view()
            .reservations
            .iter()
            .all(|row| row.released)
    );
    assert!(
        !fixture
            .state
            .scheduler_view()
            .effects
            .iter()
            .any(|effect| matches!(effect.kind, a::EffectKind::PublishResult { .. }))
    );
    fixture
        .state
        .serve(ACTION, 101, fixture.state.commit(), 701, 90)
        .unwrap();
    for row in &old.rows {
        fixture.read_operation(1, row);
        fixture.read_grant(701, row);
    }

    fixture.state.authorize_client(9).unwrap();
    // Same first fifteen namespace bytes, incarnation and generation numbers.
    // Selecting the backend by a shortened identity would read the old graph.
    let mut next_namespace = NAMESPACE;
    next_namespace[15] ^= 1;
    let new = Graph::new(algorithm, next_namespace, INCARNATION, 1, "new");
    publish_direct(&mut fixture, &new, 2, 102);
    assert_eq!(fixture.state.heads()[ACTION], 102);
    completed(&fixture, 1, &old);
    let old_root = fixture.state.accepted()[&101].root;
    assert_eq!(
        fixture.state.retention_view().roots[&super::protocol::RootId(old_root)].until,
        0
    );
    for row in &old.rows {
        fixture.read_operation(1, row);
        fixture.read_grant(701, row);
    }
    for row in &new.rows {
        assert!(!fixture.state.can_read(701, &row.member()));
    }
    rejected_unchanged(&mut fixture, Error::Miss, |state| {
        state.serve(ACTION, 101, state.commit(), 702, 90)
    });
    let old_barrier = fixture.state.commit();
    fixture
        .state
        .serve(ACTION, 102, old_barrier, 702, 90)
        .unwrap();
    rejected_unchanged(&mut fixture, Error::Miss, |state| {
        state.serve(ACTION, 102, old_barrier, 703, 90)
    });
    for row in &new.rows {
        fixture.read_grant(702, row);
    }

    assert_eq!(fixture.state.grants()[&701].publication, 101);
    assert_eq!(fixture.state.grants()[&702].publication, 102);
    fixture.state.lose_index();
    assert!(fixture.state.index().is_empty());
    fixture.state.admit(4, ACTION.into(), false, 80).unwrap();
    assert!(fixture.state.operations()[&4].cached);
    let a::TerminalResult::Completed(hit) = terminal(&fixture, 4) else {
        panic!("cache hit did not complete")
    };
    let response = hit.require_response().unwrap().decode().unwrap();
    assert!(response.cached_result);
    assert_eq!(
        response.result,
        new.response
            .require_response()
            .unwrap()
            .decode()
            .unwrap()
            .result
    );
    fixture.state.rebuild_index();
    assert_eq!(fixture.state.index()[ACTION], 102);
    fixture.reopen();
    assert_eq!(fixture.state.heads()[ACTION], 102);
    completed(&fixture, 1, &old);
    for row in &old.rows {
        fixture.read_operation(1, row);
        fixture.read_grant(701, row);
    }
    for row in &new.rows {
        fixture.read_operation(4, row);
        fixture.read_grant(702, row);
    }
    fixture.state.advance(91).unwrap();
    assert!(!fixture.state.can_read(701, &old.rows[0].member()));
    fixture.read_operation(1, &old.rows[0]);
    let algorithm = algorithm_name(algorithm);
    eprintln!(
        "PUBLICATION_PHYSICAL: {algorithm}; actual typed closure; atomic worker acceptance; replacement; old operation/serving reads; cache hit; outer checkpoint reopen"
    );
}

fn preparation_refusals(algorithm: Algorithm) {
    let mut fixture = Fixture::new();
    fixture.state.authorize_client(9).unwrap();
    let graph = Graph::new(algorithm, NAMESPACE, INCARNATION, 1, "refusals");
    fixture.stage(&graph, 1, Publisher::Client { principal: 9 });
    let receipt = fixture.finish_write(&graph, 1, 0);
    let mut wrong_key = graph.rows[0].key;
    wrong_key.incarnation[15] ^= 1;
    rejected_unchanged(&mut fixture, Error::Invalid, |state| {
        state.observe(1, receipt.write, wrong_key, object(receipt.object), true)
    });
    fixture.observe(1, &graph.rows[0], receipt);
    for index in 1..graph.rows.len() {
        let receipt = fixture.finish_write(&graph, 1, index);
        fixture.observe(1, &graph.rows[index], receipt);
    }
    assert_eq!(
        fixture.capture(&graph).job.finish(),
        Err(ClosureFailure::Incomplete)
    );
    let missing = graph
        .rows
        .iter()
        .skip(1)
        .map(super::fixture::Row::binding)
        .collect::<Vec<_>>();
    let mut job = Preparation::new(
        fixture.store(graph.result().key).clone(),
        fixture.state.catalogue_revision(),
        &missing,
        graph.result().object,
        &graph.retained,
        Limits::default(),
    )
    .unwrap();
    let before = fixture.state.snapshot().unwrap();
    assert_eq!(job.advance(16), Err(ClosureFailure::MissingBinding));
    assert_eq!(fixture.state.snapshot().unwrap(), before);
    drop(job);

    let mut omitted = fixture.capture(&graph).finish(&graph, 1);
    assert!(omitted.closure.remove(&graph.rows[0].member()));
    rejected_unchanged(&mut fixture, Error::Stale, |state| {
        state.prepare(1, omitted)
    });
    let mut substituted = fixture.capture(&graph).finish(&graph, 1);
    let mut member = graph.rows[0].member();
    substituted.closure.remove(&member);
    member.key.namespace[15] ^= 1;
    substituted.closure.insert(member);
    rejected_unchanged(&mut fixture, Error::Stale, |state| {
        state.prepare(1, substituted)
    });

    let mut capture = fixture.capture(&graph);
    assert!(!capture.job.advance(1).unwrap());
    let extra = ObjectId::for_bytes(algorithm, b"unrelated unready allocation");
    fixture
        .state
        .install_physical(
            Key {
                generation: 100,
                ..graph.result().key
            },
            object(extra),
            false,
        )
        .unwrap();
    let stale = capture.finish(&graph, 1);
    rejected_unchanged(&mut fixture, Error::Stale, |state| state.prepare(1, stale));
    fixture.prepare(&graph, 1);
    fixture
        .state
        .install_physical(
            Key {
                generation: 101,
                ..graph.result().key
            },
            object(extra),
            false,
        )
        .unwrap();
    rejected_unchanged(&mut fixture, Error::Stale, |state| {
        state.publish(1, 101, 100)
    });
    fixture.prepare(&graph, 1);
    let captured = fixture.capture(&graph).finish(&graph, 1);
    let handle = *captured.fences.keys().next().unwrap();
    fixture.state.fence_namespace(handle).unwrap();
    rejected_unchanged(&mut fixture, Error::Stale, |state| {
        state.prepare(1, captured)
    });
    rejected_unchanged(&mut fixture, Error::Stale, |state| {
        state.publish(1, 101, 100)
    });
    assert!(fixture.state.heads().is_empty());
    assert!(fixture.state.accepted().is_empty());
    for row in &graph.rows {
        fixture.verify_row(row);
    }
    let algorithm = algorithm_name(algorithm);
    eprintln!(
        "PUBLICATION_PHYSICAL: {algorithm}; exact observation identity; incomplete/missing closure; full namespace mismatch; stale revision and fence rejected atomically"
    );
}

fn abandoned_late_writes(algorithm: Algorithm) {
    let mut fixture = Fixture::new();
    fixture.state.authorize_client(9).unwrap();
    let old = Graph::new(algorithm, NAMESPACE, INCARNATION, 1, "initial");
    publish_direct(&mut fixture, &old, 1, 101);
    fixture
        .state
        .serve(ACTION, 101, fixture.state.commit(), 701, 90)
        .unwrap();
    let late = Graph::new(algorithm, NAMESPACE, INCARNATION, 10, "late");
    fixture.stage(&late, 2, Publisher::Client { principal: 9 });
    let writes = late
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            let mut write = fixture.begin(&late, 2, index);
            write.append(&row.bytes).unwrap();
            write
        })
        .collect::<Vec<_>>();
    fixture.state.abandon(2).unwrap();
    let mut incarnation = INCARNATION;
    incarnation[15] ^= 1;
    let current = Graph::new(algorithm, NAMESPACE, incarnation, 1, "current");
    publish_direct(&mut fixture, &current, 3, 103);
    let accepted = fixture.state.accepted().clone();
    let heads = fixture.state.heads().clone();
    for (row, write) in late.rows.iter().zip(writes) {
        let receipt = write.finish().unwrap();
        fixture.observe(2, row, receipt);
    }
    assert_eq!(fixture.state.accepted(), &accepted);
    assert_eq!(fixture.state.heads(), &heads);
    let evidence = fixture.capture(&late).finish(&late, 2);
    rejected_unchanged(&mut fixture, Error::Stale, |state| {
        state.prepare(2, evidence)
    });
    rejected_unchanged(&mut fixture, Error::Incomplete, |state| {
        state.publish(2, 102, 100)
    });
    for row in &late.rows {
        assert!(!fixture.state.can_read(701, &row.member()));
        fixture.verify_row(row);
    }
    for row in &old.rows {
        fixture.read_grant(701, row);
    }
    fixture.reopen();
    assert_eq!(fixture.state.heads()[ACTION], 103);
    fixture
        .state
        .serve(ACTION, 103, fixture.state.commit(), 703, 90)
        .unwrap();
    for row in &current.rows {
        fixture.read_grant(703, row);
    }
    assert_eq!(fixture.state.accepted().len(), 2);
    let algorithm = algorithm_name(algorithm);
    eprintln!(
        "PUBLICATION_PHYSICAL: {algorithm}; staging precedes real writes; late abandoned writes finish without choosing head; full incarnation mapping survives reopen"
    );
}

fn completed_write_recovery(algorithm: Algorithm) {
    let mut fixture = Fixture::new();
    let publisher = fixture.register_and_assign(1);
    let graph = Graph::new(algorithm, NAMESPACE, INCARNATION, 1, "recovery");
    fixture.stage(&graph, 1, publisher);
    // All local writes have returned before this checkpoint. No pending writer
    // or completion receipt is retained across reopen; exact physical reads
    // reconcile the stable write IDs contained in the committed outer snapshot.
    for (index, row) in graph.rows.iter().enumerate() {
        let receipt = fixture.finish_write(&graph, 1, index);
        if index < 2 {
            fixture.observe(1, row, receipt);
        }
    }
    assert_eq!(fixture.state.stages()[&1].outcomes.len(), 2);
    fixture.reopen();
    assert!(fixture.state.heads().is_empty());
    for row in &graph.rows {
        let write = fixture.state.stages()[&1]
            .writes
            .iter()
            .find_map(|(write, key)| (*key == row.key).then_some(*write))
            .unwrap();
        if !fixture.state.stages()[&1].outcomes.contains_key(&write) {
            fixture.verify_row(row);
            fixture
                .state
                .observe(1, write, row.key, object(row.object), true)
                .unwrap();
        }
    }
    fixture.prepare(&graph, 1);
    fixture.reopen();
    assert!(
        fixture.state.scheduler_view().operations[0]
            .terminal
            .is_none()
    );
    let before = fixture.state.commit();
    fixture.state.publish(1, 101, 100).unwrap();
    assert_eq!(fixture.state.commit(), before + 1);
    fixture.reopen();
    completed(&fixture, 1, &graph);
    assert_eq!(fixture.state.heads()[ACTION], 101);
    fixture
        .state
        .serve(ACTION, 101, fixture.state.commit(), 701, 90)
        .unwrap();
    for row in &graph.rows {
        fixture.read_operation(1, row);
        fixture.read_grant(701, row);
    }
    assert_eq!(
        fixture.state.protected_keys(),
        graph
            .rows
            .iter()
            .map(|row| row.key)
            .collect::<BTreeSet<_>>()
    );
    let algorithm = algorithm_name(algorithm);
    eprintln!(
        "PUBLICATION_PHYSICAL: {algorithm}; fully committed outer checkpoints before observation/prepared/accepted; reopen uses only snapshot and physical filesystem"
    );
}

#[test]
fn sha256_real_closure_replacement_and_reopen() {
    replacement_and_reopen(Algorithm::Sha256);
}
#[test]
fn blake3_real_closure_replacement_and_reopen() {
    replacement_and_reopen(Algorithm::Blake3);
}
#[test]
fn sha256_real_preparation_refusals_are_atomic() {
    preparation_refusals(Algorithm::Sha256);
}
#[test]
fn blake3_real_preparation_refusals_are_atomic() {
    preparation_refusals(Algorithm::Blake3);
}
#[test]
fn sha256_real_late_writes_cannot_choose_head() {
    abandoned_late_writes(Algorithm::Sha256);
}
#[test]
fn blake3_real_late_writes_cannot_choose_head() {
    abandoned_late_writes(Algorithm::Blake3);
}
#[test]
fn sha256_checkpoint_reconciles_completed_local_writes() {
    completed_write_recovery(Algorithm::Sha256);
}
#[test]
fn blake3_checkpoint_reconciles_completed_local_writes() {
    completed_write_recovery(Algorithm::Blake3);
}
