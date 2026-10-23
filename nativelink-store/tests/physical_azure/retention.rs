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

//! The existing singleton retention composition with actual Azurite effects.
//! No second catalogue, filesystem object mirror, or production policy.

#[path = "../../../nativelink-scheduler/tests/retention/coarse.rs"]
mod coarse;
#[path = "../../../nativelink-scheduler/tests/retention/protocol.rs"]
mod protocol;
#[path = "../../../nativelink-scheduler/tests/retention/reference.rs"]
mod reference;
// The shared bridge also exposes filesystem constructors and checkpoint fault
// injection exercised by its original, unmodified filesystem test suite.
#[allow(dead_code)]
#[path = "../../../nativelink-scheduler/tests/retention/typed_bridge.rs"]
mod typed_bridge;

use std::collections::BTreeSet;
use std::fs;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use azure_core::http::{Method, StatusCode};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, FileNode, OutputDirectory, Tree,
};
use nativelink_store::physical_closure::VerifiedReader;
use nativelink_store::physical_filesystem::{
    Algorithm, DeleteReceipt, Failure, FailureKind, ObjectId, PhysicalKey, VerifiedRead,
    WriteReceipt,
};
use prost::Message;
use tempfile::TempDir;
use tokio::runtime::{Builder, Handle, Runtime};

use self::protocol::{CohortId, Event, RootId};
use self::typed_bridge::{Bridge, Error, PhysicalBackend, Row};
use super::ObservedTransport;
use super::adapter::LocalAzurePhysical;
use super::azurite_runner::{AzuriteEmbedded, create_container};

struct FixtureRuntime(Runtime);

impl FixtureRuntime {
    #[expect(
        clippy::disallowed_methods,
        reason = "the plain test owns this bounded runtime, not an application task"
    )]
    fn new() -> Self {
        Self(
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
        )
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "the synchronous provider seam runs only on the plain test thread, never a Tokio blocking-pool thread"
    )]
    fn run<F: Future>(&self, future: F) -> F::Output {
        assert!(
            Handle::try_current().is_err(),
            "fixture SDK calls require the plain test thread"
        );
        self.0.block_on(future)
    }
}

#[derive(Clone)]
struct AzureBackend {
    client: LocalAzurePhysical,
    runtime: Arc<FixtureRuntime>,
}

impl VerifiedReader for AzureBackend {
    fn read(
        &self,
        key: PhysicalKey,
        object: ObjectId,
        offset: u64,
        length: Option<u64>,
    ) -> Result<VerifiedRead, Failure> {
        self.runtime
            .run(self.client.read(key, object, offset, length))
    }
}

impl PhysicalBackend for AzureBackend {
    fn write(
        &self,
        key: PhysicalKey,
        write: u64,
        object: ObjectId,
        bytes: &[u8],
    ) -> Result<WriteReceipt, Failure> {
        self.runtime
            .run(self.client.write(key, write, object, bytes))
    }

    fn delete(&self, key: PhysicalKey, delete: u64) -> Result<DeleteReceipt, Failure> {
        self.runtime.run(async {
            let prepared = self.client.prepare_delete(key, delete).await?;
            self.client.finish_delete(prepared).await
        })
    }
}

fn bootstrap(
    base: &Path,
    runtime: &Arc<FixtureRuntime>,
    endpoint: &str,
    transport: &Arc<ObservedTransport>,
) -> Bridge<AzureBackend> {
    Bridge::bootstrap_with(base, |incarnation| {
        runtime
            .run(LocalAzurePhysical::bootstrap(
                endpoint,
                incarnation,
                transport.clone(),
            ))
            .map(|client| AzureBackend {
                client,
                runtime: runtime.clone(),
            })
    })
    .unwrap()
}

fn reopen(
    base: &Path,
    runtime: &Arc<FixtureRuntime>,
    endpoint: &str,
    transport: &Arc<ObservedTransport>,
) -> Bridge<AzureBackend> {
    Bridge::reopen_with(base, |incarnation| {
        runtime
            .run(LocalAzurePhysical::open(
                endpoint,
                incarnation,
                transport.clone(),
            ))
            .map(|client| AzureBackend {
                client,
                runtime: runtime.clone(),
            })
    })
    .unwrap()
}

fn digest(row: Row) -> Digest {
    let object = row.binding().object;
    Digest {
        hash: hex::encode(object.hash),
        size_bytes: i64::try_from(object.size).unwrap(),
    }
}

fn output(bridge: &mut Bridge<AzureBackend>) -> ([Row; 3], [Vec<u8>; 3]) {
    let payload = b"the existing Serving grant reads these actual Azure bytes".to_vec();
    let file = bridge.upload(&payload, CohortId(1), false).unwrap();
    let tree_bytes = Tree {
        root: Some(Directory {
            files: vec![FileNode {
                name: "f".into(),
                digest: Some(digest(file)),
                ..FileNode::default()
            }],
            ..Directory::default()
        }),
        children: vec![],
    }
    .encode_to_vec();
    let tree = bridge.upload(&tree_bytes, CohortId(1), false).unwrap();
    let result_bytes = ActionResult {
        output_directories: vec![OutputDirectory {
            path: "out".into(),
            tree_digest: Some(digest(tree)),
            is_topologically_sorted: true,
        }],
        stdout_digest: Some(digest(file)),
        ..ActionResult::default()
    }
    .encode_to_vec();
    let result = bridge.upload(&result_bytes, CohortId(1), false).unwrap();
    ([file, tree, result], [payload, tree_bytes, result_bytes])
}

fn requests(transport: &ObservedTransport) -> usize {
    transport.records.lock().unwrap().len()
}

fn deletes(transport: &ObservedTransport) -> usize {
    transport
        .records
        .lock()
        .unwrap()
        .iter()
        .filter(|record| record.method == Method::Delete)
        .count()
}

pub(super) fn qualify() {
    // A plain #[test] owns the runtime. No blocking runtime entry occurs inside
    // an async task, and only its two I/O threads execute SDK futures.
    let runtime = Arc::new(FixtureRuntime::new());
    let emulator = AzuriteEmbedded::new();
    let process = runtime
        .run(emulator.start())
        .expect("pinned local Azurite prerequisite");
    runtime
        .run(create_container(&process.blob_endpoint, "retention"))
        .unwrap();
    let endpoint = emulator.sas_url(&process, "retention").unwrap();
    let transport = ObservedTransport::new();
    let base = TempDir::new().unwrap();
    fs::create_dir(base.path().join("authority")).unwrap();
    let checkpoint = base.path().join("authority/checkpoint");
    let mut bridge = bootstrap(base.path(), &runtime, &endpoint, &transport);
    let (rows, bytes) = output(&mut bridge);
    let keys: Vec<_> = rows.iter().map(|row| row.key).collect();
    let before_prepare = requests(&transport);
    let mut pending = bridge.begin(rows[2]).unwrap();
    while !bridge.advance(&mut pending, 1).unwrap() {}
    bridge.publish(pending, RootId(10), 1, 10).unwrap();
    let prepared_reads: BTreeSet<_> = transport.records.lock().unwrap()[before_prepare..]
        .iter()
        .filter(|request| request.method == Method::Get)
        .map(|request| {
            assert!(request.if_match.is_some());
            request.path.clone()
        })
        .collect();
    assert_eq!(
        prepared_reads.len(),
        3,
        "typed preparation read all exact provider objects"
    );
    assert_eq!(bridge.view().protected, keys.iter().copied().collect());
    let serving = bridge.serve(RootId(20), 20).unwrap();
    bridge.remove_head().unwrap();
    drop(bridge);

    let mut bridge = reopen(base.path(), &runtime, &endpoint, &transport);
    assert_eq!(bridge.head().unwrap(), None);
    for (row, expected) in rows.iter().zip(&bytes) {
        assert_eq!(
            bridge.read(serving, row.binding().object).unwrap(),
            *expected
        );
    }
    let before_refusal = requests(&transport);
    assert_eq!(
        bridge.read(
            serving,
            ObjectId::for_bytes(Algorithm::Sha256, b"outside closure")
        ),
        Err(Error::Rejected)
    );
    assert_eq!(
        requests(&transport),
        before_refusal,
        "wrong descriptor never reaches provider"
    );
    let fence = bridge.close(CohortId(1)).unwrap();
    let protected_checkpoint = fs::read(&checkpoint).unwrap();
    let protected_view = bridge.view();
    for key in &keys {
        assert_eq!(
            bridge.event(Event::Retire {
                cohort: CohortId(1),
                fence,
                keys: vec![*key]
            }),
            Err(Error::Rejected)
        );
    }
    assert_eq!(bridge.view(), protected_view);
    assert_eq!(fs::read(&checkpoint).unwrap(), protected_checkpoint);
    assert!(bridge.view().deletes.is_empty());
    assert_eq!(deletes(&transport), 0);

    bridge.event(Event::AdvanceTime { now: 20 }).unwrap();
    let before_expired_read = requests(&transport);
    assert_eq!(
        bridge.read(serving, rows[0].binding().object),
        Err(Error::Expired)
    );
    assert_eq!(
        requests(&transport),
        before_expired_read,
        "expiry rejects before HTTP"
    );
    // Captured marks conservatively survive expiry. The existing protocol
    // requires another close/mark cycle, with a fresh fence, before retirement.
    assert_eq!(
        bridge.event(Event::Retire {
            cohort: CohortId(1),
            fence,
            keys: keys.clone(),
        }),
        Err(Error::Rejected)
    );
    bridge
        .event(Event::Reopen {
            cohort: CohortId(1),
            fence,
        })
        .unwrap();
    let retirement_fence = bridge.close(CohortId(1)).unwrap();
    assert!(retirement_fence > fence);
    bridge
        .event(Event::Retire {
            cohort: CohortId(1),
            fence: retirement_fence,
            keys,
        })
        .unwrap();
    let retired = bridge.view();
    let retired_checkpoint = fs::read(&checkpoint).unwrap();
    assert!(retired.protected.is_empty());
    for row in rows {
        let generation = &retired.generations[&row.key];
        assert!(generation.retired);
        let intent = &retired.deletes[&generation.current_delete.unwrap()];
        assert_eq!(intent.key, row.key);
        assert!(!intent.confirmed);
    }

    // The real conditional DELETE commits; only its acknowledgement is lost.
    // The authority must preserve its unconfirmed intent and exact checkpoint.
    transport
        .lose_next_delete_response
        .store(true, Ordering::SeqCst);
    assert!(
        matches!(bridge.delete_current(rows[0]), Err(Error::Physical(error)) if error.kind == FailureKind::Unavailable)
    );
    assert_eq!(bridge.view(), retired);
    assert_eq!(fs::read(&checkpoint).unwrap(), retired_checkpoint);
    assert_eq!(deletes(&transport), 1);
    drop(bridge);

    let mut bridge = reopen(base.path(), &runtime, &endpoint, &transport);
    assert_eq!(
        bridge.view(),
        retired,
        "lost DELETE response survives authority reopen"
    );
    for row in rows {
        bridge.delete_current(row).unwrap();
        let view = bridge.view();
        let generation = &view.generations[&row.key];
        let delete = retired.generations[&row.key].current_delete.unwrap();
        assert!(view.deletes[&delete].confirmed);
        assert_eq!(generation.current_delete, None);
        assert_eq!(
            generation.delete_confirmed_revision,
            Some(generation.write_revision)
        );
    }
    drop(bridge);

    let bridge = reopen(base.path(), &runtime, &endpoint, &transport);
    for row in rows {
        let binding = row.binding();
        assert_eq!(
            bridge
                .store
                .read(binding.key, binding.object, 0, None)
                .unwrap_err()
                .kind,
            FailureKind::Missing
        );
        let view = bridge.view();
        let generation = &view.generations[&row.key];
        let delete = retired.generations[&row.key].current_delete.unwrap();
        assert!(view.deletes[&delete].confirmed);
        assert_eq!(generation.current_delete, None);
        assert_eq!(
            generation.delete_confirmed_revision,
            Some(generation.write_revision)
        );
    }
    assert!(
        !base.path().join("physical").exists(),
        "no filesystem object fallback"
    );
    let records = transport.records.lock().unwrap();
    let actual_deletes: Vec<_> = records
        .iter()
        .filter(|record| record.method == Method::Delete)
        .collect();
    assert_eq!(actual_deletes.len(), 3);
    assert!(
        actual_deletes
            .iter()
            .all(|request| request.if_match.is_some() && request.status == StatusCode::Accepted)
    );
    assert!(records.iter().all(|request| !request.multipart));
    eprintln!(
        "Qualified typed retention over {} actual SDK requests and {} exact conditional deletes",
        records.len(),
        actual_deletes.len()
    );
}
