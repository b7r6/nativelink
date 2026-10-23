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

//! Synthesized frozen Base executable. Its isolated source build removes the
//! Direct scheduler decoder, rather than selecting a runtime reader profile.

use std::path::PathBuf;

use nativelink_scheduler::authority::format::{
    AdmissionRegistry, AuthorityDomain, JournalProfile, ReaderCapabilities,
};
use nativelink_scheduler::authority::journal::{
    Journal, MAX_SNAPSHOT_BYTES, Singleton, SnapshotBuffer,
};
use nativelink_scheduler::authority::{ClusterEpoch, Limits};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{BasicNode, RaftSnapshotBuilder, SnapshotMeta};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    mode: String,
    path: PathBuf,
    epoch: u64,
    namespace: u64,
    node: u64,
    rebuilt_snapshot: Option<PathBuf>,
}

#[derive(Default, Serialize)]
struct Report {
    accepted: bool,
    phase: &'static str,
    error: Option<String>,
    vote_reads: usize,
    raft_started: bool,
    quorum_read_completed: bool,
    state_json: Option<String>,
}

async fn run(request: Request, report: &mut Report) -> Result<(), String> {
    let domain = AuthorityDomain {
        epoch: ClusterEpoch(request.epoch),
        namespace: request.namespace,
    };
    if request.mode == "install-snapshot" {
        report.phase = "snapshot_input";
        if std::fs::metadata(&request.path)
            .map_err(|error| error.to_string())?
            .len()
            > MAX_SNAPSHOT_BYTES as u64
        {
            return Err("snapshot exceeds frozen input bound".into());
        }
        let bytes = std::fs::read(&request.path).map_err(|error| error.to_string())?;
        // Extract transport metadata only. The actual frozen Journal decoder
        // validates envelope, checksum, sections, state, and metadata agreement.
        let envelope: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        let payload: serde_json::Value = serde_json::from_str(
            envelope["payload"]
                .as_str()
                .ok_or("missing envelope payload")?,
        )
        .map_err(|error| error.to_string())?;
        let meta: SnapshotMeta<u64, BasicNode> =
            serde_json::from_value(payload["meta"].clone()).map_err(|error| error.to_string())?;
        let mut journal = Journal::fresh_volatile_with_profile(
            request.node,
            Limits::default(),
            JournalProfile {
                domain,
                incarnation: 100,
                reader: ReaderCapabilities::Base,
                admission: AdmissionRegistry::new(domain),
            },
        )
        .await
        .map_err(|error| error.to_string())?;
        report.phase = "install_snapshot";
        journal
            .install_snapshot(
                &meta,
                Box::new(SnapshotBuffer::from_bytes(bytes).map_err(|error| error.to_string())?),
            )
            .await
            .map_err(|error| error.to_string())?;
        report.phase = "read_vote";
        journal
            .read_vote()
            .await
            .map_err(|error| error.to_string())?;
        report.vote_reads += 1;
        report.state_json = Some(
            String::from_utf8(
                journal
                    .read_applied()
                    .await
                    .map_err(|error| error.to_string())?
                    .1
                    .snapshot()?,
            )
            .map_err(|error| error.to_string())?,
        );
        report.phase = "snapshot_recovered";
        return Ok(());
    }
    if request.mode != "inspect-journal" && request.mode != "open-journal" {
        return Err("unknown frozen fixture request".into());
    }
    report.phase = "recover_admission";
    let registry = Journal::recover_admission(
        domain,
        ReaderCapabilities::Base,
        vec![(request.node, request.path.clone())],
    )
    .await
    .map_err(|error| error.to_string())?;
    if request.mode == "inspect-journal" {
        // This establishes artifact decoding only. In particular, an unresolved
        // activation does not authorize a Base member to vote or serve reads.
        report.phase = "artifact_decoded_without_admission";
        return Ok(());
    }
    let profile = JournalProfile {
        domain,
        incarnation: 100,
        reader: ReaderCapabilities::Base,
        admission: registry,
    };
    report.phase = "journal_open";
    let mut journal = Journal::open_with_profile(
        &request.path,
        request.node,
        Limits::default(),
        profile.clone(),
    )
    .await
    .map_err(|error| error.to_string())?;
    report.phase = "read_vote";
    journal
        .read_vote()
        .await
        .map_err(|error| error.to_string())?;
    report.vote_reads += 1;
    if let Some(path) = request.rebuilt_snapshot {
        report.phase = "base_snapshot_replacement";
        let snapshot = journal
            .build_snapshot()
            .await
            .map_err(|error| error.to_string())?;
        std::fs::write(path, snapshot.snapshot.get_ref()).map_err(|error| error.to_string())?;
    }
    drop(journal);
    report.phase = "raft_start";
    let node = Singleton::open_with_profile(
        &request.path,
        request.node,
        Limits::default(),
        JournalProfile {
            incarnation: 101,
            ..profile
        },
    )
    .await?;
    report.raft_started = true;
    report.phase = "quorum_read";
    let state = node.read().await?;
    report.quorum_read_completed = true;
    report.state_json =
        Some(String::from_utf8(state.snapshot()?).map_err(|error| error.to_string())?);
    node.shutdown().await?;
    report.phase = "raft_recovered";
    Ok(())
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
async fn main() {
    assert!(
        !ReaderCapabilities::DirectScheduler.supports(&std::collections::BTreeSet::from([1])),
        "fixture linked an upgraded capability implementation"
    );
    let mut report = Report {
        phase: "request",
        ..Report::default()
    };
    let request = std::env::args_os()
        .nth(1)
        .ok_or("request path required")
        .and_then(|path| std::fs::read(path).map_err(|_| "cannot read request"))
        .and_then(|bytes| serde_json::from_slice::<Request>(&bytes).map_err(|_| "invalid request"));
    match request {
        Ok(request) => match run(request, &mut report).await {
            Ok(()) => report.accepted = true,
            Err(error) => report.error = Some(error),
        },
        Err(error) => report.error = Some(error.into()),
    }
    println!(
        "{}",
        serde_json::to_string(&report).expect("serialize fixture report")
    );
    if !report.accepted {
        std::process::exit(2);
    }
}
