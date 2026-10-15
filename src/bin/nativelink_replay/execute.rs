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

//! `execute`: run the action on the remote executor, one run at a time, and
//! turn each `ActionResult` into a manifest.

use std::collections::HashMap;
use std::time::Instant;

use nativelink_error::{Code, Error};
use nativelink_proto::build::bazel::remote::execution::v2::{ExecuteResponse, Tree};
use nativelink_util::common::DigestInfo;

use crate::manifest::{Run, RunOutcome, manifest_from_action_result, stream_digest, tree_digests};
use crate::remote::Remote;

/// Fills `run` from a finished operation's response: the worker, the
/// manifest (output directories expanded from their verified `Tree`s), the
/// exit code and the stdout and stderr digests.
pub(crate) async fn fill_run(
    remote: &Remote,
    run: &mut Run,
    operation: &str,
    response: ExecuteResponse,
) -> Result<(), Error> {
    if !operation.is_empty() {
        run.notes.push(format!("operation {operation}"));
    }
    if response.cached_result {
        run.notes.push(
            "the server answered from the action cache (cached_result) despite skip_cache_lookup"
                .to_string(),
        );
    }
    if !response.message.is_empty() {
        run.notes
            .push(format!("server message: {}", response.message));
    }
    let Some(result) = response.result else {
        run.error = Some(response.status.map_or_else(
            || "the ExecuteResponse has no ActionResult".to_string(),
            |status| {
                format!(
                    "{}: {}",
                    Code::from(status.code).description(),
                    status.message
                )
            },
        ));
        return Ok(());
    };
    if let Some(metadata) = &result.execution_metadata {
        run.worker.clone_from(&metadata.worker);
    }
    if let Some(status) = &response.status
        && status.code != 0
    {
        run.error = Some(format!(
            "execution status {}: {}",
            Code::from(status.code).description(),
            status.message
        ));
        return Ok(());
    }
    let mut trees: HashMap<DigestInfo, Tree> = HashMap::new();
    for digest in tree_digests(&result)? {
        if trees.contains_key(&digest) {
            continue;
        }
        match remote
            .read_proto::<Tree>(digest, "output directory Tree")
            .await
        {
            Ok(tree) => {
                trees.insert(digest, tree);
            }
            Err(problem) => run.problems.push(problem),
        }
    }
    if !run.problems.is_empty() {
        run.error = Some("could not read every output directory's Tree".to_string());
        return Ok(());
    }
    let manifest = manifest_from_action_result(&result, &trees, remote.func)?;
    run.outcome = Some(RunOutcome {
        manifest,
        exit_code: result.exit_code,
        stdout: stream_digest(
            result.stdout_digest.as_ref(),
            &result.stdout_raw,
            remote.func,
        )?,
        stderr: stream_digest(
            result.stderr_digest.as_ref(),
            &result.stderr_raw,
            remote.func,
        )?,
    });
    Ok(())
}

/// Executes the action `runs` times, one after another, each with
/// `skip_cache_lookup`.
pub(crate) async fn execute_runs(remote: &Remote, action: DigestInfo, runs: usize) -> Vec<Run> {
    let mut out = Vec::with_capacity(runs);
    for index in 1..=runs {
        eprintln!("remote run {index}/{runs}: executing");
        let started = Instant::now();
        let mut run = Run {
            index,
            ..Default::default()
        };
        match remote.execute(action).await {
            Ok((operation, response)) => {
                if let Err(e) = fill_run(remote, &mut run, &operation, response).await {
                    run.error = Some(e.message_string());
                }
            }
            Err(e) => run.error = Some(e.message_string()),
        }
        run.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        out.push(run);
    }
    out
}
