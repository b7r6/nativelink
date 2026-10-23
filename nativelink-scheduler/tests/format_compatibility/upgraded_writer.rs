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

//! Produces real upgraded-writer artifacts for the separately compiled frozen
//! Base executable. No artifact is fabricated by rewriting JSON or checksums.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use nativelink_scheduler::authority::format::{
    AdmissionRegistry, AuthorityDomain, DIRECT_SCHEDULER, FormatControl, JournalProfile,
    ReaderCapabilities,
};
use nativelink_scheduler::authority::journal::{Batch, Journal, Singleton, TypeConfig};
use nativelink_scheduler::authority::{
    ActionKey, ClusterEpoch, EffectKind, Event, IncarnationId, Limits, OperationId, RecordedEvent,
    Reply, Request, Resources, ResultPayload, SessionId, WorkerId,
};
use openraft::storage::{RaftLogStorage, RaftLogStorageExt, RaftStateMachine};
use openraft::{Entry, EntryPayload, LogId, RaftSnapshotBuilder};

const DOMAIN: AuthorityDomain = AuthorityDomain {
    epoch: ClusterEpoch(211),
    namespace: 67,
};
const NODE: u64 = 1;

fn profile(registry: &Arc<AdmissionRegistry>, incarnation: u64) -> JournalProfile {
    JournalProfile {
        domain: DOMAIN,
        incarnation,
        reader: ReaderCapabilities::DirectScheduler,
        admission: registry.clone(),
    }
}

const fn record(event: Event) -> RecordedEvent {
    RecordedEvent {
        epoch: DOMAIN.epoch,
        event,
    }
}

fn submit(operation: u64, skip_cache_lookup: bool) -> RecordedEvent {
    record(Event::Submit {
        operation: OperationId(operation),
        request: Request {
            action: ActionKey {
                instance: "frozen-base-fixture".into(),
                digest_function: "sha256".into(),
                digest: format!("{operation:064x}"),
                digest_size_bytes: 1,
            },
            execution_scope: None,
            do_not_cache: false,
            skip_cache_lookup,
            priority: 0,
            properties: BTreeMap::new(),
            resources: Resources {
                cpu_millis: 100,
                memory_bytes: 1024,
                slots: 1,
            },
            execution_timeout: 100,
            payload: None,
        },
    })
}

#[tokio::main]
#[expect(
    clippy::disallowed_methods,
    reason = "standalone fixture owns its runtime"
)]
#[expect(
    clippy::print_stdout,
    reason = "JSON report is the subprocess protocol"
)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    assert!(
        ReaderCapabilities::DirectScheduler.supports(&BTreeSet::from([DIRECT_SCHEDULER])),
        "upgraded producer linked a frozen Base capability implementation"
    );
    let directory = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("artifact directory required")?,
    );
    std::fs::create_dir_all(&directory)?;
    let source = directory.join("source.redb");
    if source.exists() {
        return Err("artifact directory already contains a source journal".into());
    }
    let registry = AdmissionRegistry::new(DOMAIN);
    let node =
        Singleton::open_with_profile(&source, NODE, Limits::default(), profile(&registry, 1))
            .await?;
    let capacity = Resources {
        cpu_millis: 300,
        memory_bytes: 3072,
        slots: 3,
    };
    node.write(record(Event::Register {
        worker: WorkerId(1),
        incarnation: IncarnationId(1),
        session: SessionId(1),
        properties: BTreeMap::new(),
        capacity,
        available: capacity,
    }))
    .await?;
    for operation in 1..=3 {
        node.write(submit(operation, true)).await?;
        assert!(matches!(
            node.write(record(Event::Match)).await?.data,
            Reply::Matched { .. }
        ));
    }
    let first = node.read().await?.view().attempts[0];
    node.write(record(Event::WorkerAccepted {
        attempt: first.id,
        worker: first.worker,
        incarnation: first.incarnation,
    }))
    .await?;
    node.write(record(Event::Complete {
        attempt: first.id,
        worker: first.worker,
        incarnation: first.incarnation,
        result: ResultPayload {
            digest: "accepted-result".into(),
            exit_code: 0,
            error: None,
            response: None,
        },
    }))
    .await?;
    node.write(record(Event::Cancel {
        operation: OperationId(2),
    }))
    .await?;
    node.write(submit(4, false)).await?;
    let expected = node.read().await?;
    let view = expected.view();
    assert!(
        view.effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::Dispatch { .. }))
    );
    assert!(
        view.effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::Cancel { .. }))
    );
    assert!(
        view.effects
            .iter()
            .any(|effect| matches!(effect.kind, EffectKind::PublishResult { .. }))
    );
    assert_eq!(
        view.reservations
            .iter()
            .filter(|reservation| !reservation.released)
            .count(),
        2
    );
    let state_json = String::from_utf8(expected.snapshot()?)?;

    // The capable writer still emits the Base representation through ordinary
    // writes, snapshot replacement, and compaction before activation.
    let first_snapshot = node.journal.clone().build_snapshot().await?;
    node.journal
        .clone()
        .purge(
            first_snapshot
                .meta
                .last_log_id
                .ok_or("snapshot position missing")?,
        )
        .await?;
    let base_snapshot = node.journal.clone().build_snapshot().await?;
    assert_ne!(
        first_snapshot.meta.snapshot_id,
        base_snapshot.meta.snapshot_id
    );
    std::fs::write(
        directory.join("preactivation.snapshot.json"),
        base_snapshot.snapshot.get_ref(),
    )?;
    node.shutdown().await?;
    std::fs::copy(&source, directory.join("preactivation.redb"))?;

    let node =
        Singleton::open_with_profile(&source, NODE, Limits::default(), profile(&registry, 2))
            .await?;
    assert_eq!(node.read().await?, expected);
    let preparation = node
        .journal
        .prepare_features(&node.raft, BTreeSet::from([DIRECT_SCHEDULER]))
        .await?;
    node.raft
        .client_write(Batch::format(FormatControl::Prepare(preparation.clone()))?)
        .await?;
    let position = node
        .journal
        .read_applied()
        .await?
        .0
        .ok_or("applied position missing")?;
    let activation: Entry<TypeConfig> = Entry {
        log_id: LogId::new(position.leader_id, position.index + 1),
        payload: EntryPayload::Normal(Batch::format(FormatControl::Activate(preparation))?),
    };
    let mut journal = node.journal.clone();
    node.shutdown().await?;
    // Precise append/commit/apply cutpoints use the real shared journal. The
    // Prepare itself was authorized and committed through actual OpenRaft.
    journal.blocking_append([activation.clone()]).await?;
    assert_eq!(journal.active_format().await?.feature_epoch, 0);
    drop(journal);
    drop(registry);
    std::fs::copy(&source, directory.join("uncommitted-activation.redb"))?;

    let recovered = Journal::recover_admission(
        DOMAIN,
        ReaderCapabilities::DirectScheduler,
        vec![(NODE, source.clone())],
    )
    .await?;
    let mut journal =
        Journal::open_with_profile(&source, NODE, Limits::default(), profile(&recovered, 3))
            .await?;
    journal.save_committed(Some(activation.log_id)).await?;
    assert_eq!(
        journal.active_format().await?.feature_epoch,
        0,
        "committed decision is ahead of applied state"
    );
    drop(journal);
    std::fs::copy(&source, directory.join("committed-before-apply.redb"))?;

    let mut journal =
        Journal::open_with_profile(&source, NODE, Limits::default(), profile(&recovered, 4))
            .await?;
    journal.apply([activation]).await?;
    assert_eq!(journal.active_format().await?.feature_epoch, 1);
    assert_eq!(journal.read_applied().await?.1, expected);
    let activated_snapshot = journal.build_snapshot().await?;
    std::fs::write(
        directory.join("activated.snapshot.json"),
        activated_snapshot.snapshot.get_ref(),
    )?;
    drop(journal);
    std::fs::copy(&source, directory.join("activated.redb"))?;

    let manifest = serde_json::json!({
        "domain": { "epoch": DOMAIN.epoch.0, "namespace": DOMAIN.namespace },
        "node": NODE,
        "state_json": state_json,
        "pending_effects": view.effects.len(),
        "charged_reservations": 2,
        "artifacts": ["preactivation.redb", "preactivation.snapshot.json", "uncommitted-activation.redb", "committed-before-apply.redb", "activated.redb", "activated.snapshot.json"],
        "producer": "real Journal and OpenRaft Prepare; actual append/commit/apply transactions",
    });
    std::fs::write(
        directory.join("artifacts.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    println!(
        "{{\"generated\":6,\"pending_effects\":{}}}",
        view.effects.len()
    );
    Ok(())
}
