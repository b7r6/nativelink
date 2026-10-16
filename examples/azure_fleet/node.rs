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

//! One real `LocalWorker` per process and private mount namespace.
use std::collections::BTreeMap;
use std::io::{BufRead, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use azure_core::http::{RetryOptions, Transport, Url};
use azure_storage_blob::clients::{BlobContainerClient, BlobContainerClientOptions};
use bytes::Bytes;
use nativelink_config::cas_server::LocalWorkerConfig;
use nativelink_config::stores::{
    CommonObjectSpec, EvictionPolicy, ExperimentalAzureSpec, ExperimentalCloudObjectSpec,
    FastSlowSpec, FilesystemSpec, MemorySpec, Retry, StoreDirection, StoreSpec, VerifySpec,
};
use nativelink_store::azure_blob_store::AzureBlobStore;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::verify_store::VerifyStore;
use nativelink_util::common::DigestInfo;
use nativelink_util::shutdown_guard::{Priority, ShutdownGuard};
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::local_worker::new_local_worker;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use crate::Result;
use crate::workload::{MIB, Metadata, uniform};

#[derive(Serialize, Deserialize)]
pub(crate) struct Config {
    pub(crate) seed: u64,
    pub(crate) node: usize,
    pub(crate) root: PathBuf,
    pub(crate) azure: String,
    pub(crate) scheduler: String,
    pub(crate) metadata: Vec<Metadata>,
}

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
enum Allocation {
    Filler,
    Executable,
    Content,
    Work,
    Temporary,
    Other,
}

fn allocation(root: &Path, path: &Path) -> Allocation {
    let relative = path
        .strip_prefix(root)
        .expect("snapshot path is within root");
    match relative
        .components()
        .next()
        .and_then(|p| p.as_os_str().to_str())
    {
        Some("pressure") => Allocation::Filler,
        Some("content.exec") => Allocation::Executable,
        Some("content") => Allocation::Content,
        Some("work") => Allocation::Work,
        Some("temp") => Allocation::Temporary,
        _ => Allocation::Other,
    }
}

fn snapshot(root: &Path) -> Result<Value> {
    let mut stack = vec![root.to_path_buf()];
    let mut inodes = BTreeMap::<(u64, u64), (u64, Allocation)>::new();
    let mut vanished_entries = 0_u64;
    while let Some(path) = stack.pop() {
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                vanished_entries += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        for e in entries {
            let e = e?;
            let m = match e.metadata() {
                Ok(metadata) => metadata,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    vanished_entries += 1;
                    continue;
                }
                Err(e) => return Err(e.into()),
            };
            if m.is_dir() {
                stack.push(e.path());
                continue;
            }
            if !m.is_file() {
                continue;
            }
            let bytes = m.blocks() * 512;
            let category = allocation(root, &e.path());
            inodes
                .entry((m.dev(), m.ino()))
                .and_modify(|entry| {
                    // Charge shared input links to their cache, independent of traversal order.
                    // Concurrent writes can change allocation during this non-atomic snapshot.
                    entry.0 = entry.0.max(bytes);
                    entry.1 = entry.1.min(category);
                })
                .or_insert((bytes, category));
        }
    }
    let (mut content, mut exec, mut work, mut filler, mut temporary, mut other, mut total) =
        (0_u64, 0, 0, 0, 0, 0, 0);
    for (bytes, category) in inodes.values() {
        total += bytes;
        match category {
            Allocation::Filler => filler += bytes,
            Allocation::Executable => exec += bytes,
            Allocation::Content => content += bytes,
            Allocation::Work => work += bytes,
            Allocation::Temporary => temporary += bytes,
            Allocation::Other => other += bytes,
        }
    }
    Ok(
        json!({"content_allocated":content,"exec_allocated":exec,"work_unique_allocated":work,"filler_allocated":filler,"temporary_allocated":temporary,"other_allocated":other,"total_allocated":total,"unique_file_inodes":inodes.len(),"vanished_entries":vanished_entries,"atomic":false,"boundary":"allocated file blocks; unique device/inode"}),
    )
}

fn fill(root: &Path) -> Result<Value> {
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("pressure"))?;
    let block = vec![0_u8; 1024 * 1024];
    let mut bytes = 0_u64;
    loop {
        match f.write(&block) {
            Ok(0) => return Err(std::io::Error::other("zero write before ENOSPC").into()),
            Ok(n) => bytes += u64::try_from(n)?,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) if e.raw_os_error() == Some(libc::ENOSPC) => break,
            Err(e) => return Err(e.into()),
        }
    }
    let mut result = snapshot(root)?;
    result["filler_written"] = json!(bytes);
    result["errno"] = json!(libc::ENOSPC);
    Ok(result)
}

async fn reply(value: Value) -> Result<()> {
    let mut out = tokio::io::stdout();
    out.write_all(format!("{value}\n").as_bytes()).await?;
    out.flush().await?;
    Ok(())
}

async fn disk_snapshot(root: PathBuf) -> Result<Value> {
    nativelink_util::spawn_blocking!("fleet_disk_snapshot", move || snapshot(&root)).await?
}

pub(crate) async fn run(path: &str) -> Result<()> {
    let config: Config = serde_json::from_slice(&tokio::fs::read(path).await?)?;
    tokio::fs::create_dir_all(&config.root).await?;
    if tokio::fs::read_dir(&config.root)
        .await?
        .next_entry()
        .await?
        .is_some()
    {
        return Err(std::io::Error::other("worker mount point must be empty").into());
    }
    let status = tokio::process::Command::new("mount")
        .args(["-t", "tmpfs", "-o", "size=1g,nosuid,nodev", "tmpfs"])
        .arg(&config.root)
        .status()
        .await?;
    if !status.success() {
        return Err(std::io::Error::other("private tmpfs mount failed; start node through unshare --user --map-root-user --mount --propagation private").into());
    }
    let fs_spec = FilesystemSpec {
        content_path: config.root.join("content").to_string_lossy().into_owned(),
        temp_path: config.root.join("temp").to_string_lossy().into_owned(),
        eviction_policy: Some(EvictionPolicy {
            max_bytes: usize::try_from(512 * MIB)?,
            ..Default::default()
        }),
        ..Default::default()
    };
    let filesystem: Arc<FilesystemStore> = FilesystemStore::new(&fs_spec).await?;
    let fast = Store::new(filesystem);
    // Seed only metadata. Payloads always traverse HTTP on a cold miss.
    for m in &config.metadata {
        fast.update_oneshot(
            DigestInfo::try_new(&m.hash, m.size)?,
            Bytes::from(hex::decode(&m.hex)?),
        )
        .await?;
    }
    let retry = Retry {
        max_retries: 6,
        delay: 0.3,
        jitter: 0.5,
        ..Default::default()
    };
    let spec = ExperimentalAzureSpec {
        account_name: "fleet".into(),
        container: "cas".into(),
        common: CommonObjectSpec {
            retry,
            throttle_retry_budget_s: 0.0,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut options = BlobContainerClientOptions::default();
    options.client_options.retry = RetryOptions::none();
    options.client_options.transport = Some(Transport::new(Arc::new(
        reqwest::Client::builder().no_proxy().build()?,
    )));
    let client = BlobContainerClient::new(
        Url::parse(&format!("{}/{}/cas", config.azure, config.node))?,
        None,
        Some(options),
    )?;
    let count = AtomicU64::new(0);
    let seed = config.seed;
    let node = u64::try_from(config.node)?;
    let azure = AzureBlobStore::new_with_client_and_jitter(
        &spec,
        client,
        Arc::new(move |delay: Duration| {
            let index = count.fetch_add(1, Ordering::Relaxed);
            #[expect(
                clippy::cast_possible_truncation,
                reason = "The seeded draw is bounded to (0, 1); rounding to f32 preserves production jitter arithmetic"
            )]
            let q = uniform(seed, 100_000 + node * 10_000 + index) as f32;
            delay.mul_f32(0.5_f32.mul_add(q - 0.5, 1.0))
        }),
        SystemTime::now,
    )?;
    let verify_spec = VerifySpec {
        backend: StoreSpec::ExperimentalCloudObjectStore(ExperimentalCloudObjectSpec::Azure(spec)),
        verify_size: true,
        verify_hash: false,
    };
    let verify = VerifyStore::new(&verify_spec, Store::new(azure));
    let cas = Store::new(FastSlowStore::new(
        &FastSlowSpec {
            fast: StoreSpec::Filesystem(fs_spec),
            slow: StoreSpec::Verify(Box::new(verify_spec)),
            fast_direction: StoreDirection::default(),
            slow_direction: StoreDirection::default(),
            bypass_dedup_threshold_bytes: 0,
        },
        fast,
        Store::new(verify),
    ));
    let worker_config: LocalWorkerConfig = serde_json::from_value(json!({
        "name": format!("fleet-{}", config.node),
        "worker_api_endpoint": {"uri": config.scheduler},
        "max_inflight_tasks": 2,
        "cas_fast_slow_store": "cas",
        "work_directory": config.root.join("work"),
        "platform_properties": {"fleet_node": {"values": [config.node.to_string()]}},
        "max_action_timeout_s": 120,
        "max_download_timeout_s": 90,
        "max_upload_timeout_s": 30,
        "drain_on_shutdown": true,
        "upload_action_result": {"upload_ac_results_strategy": "never"}
    }))?;
    let worker = new_local_worker(
        Arc::new(worker_config),
        cas,
        None,
        Store::new(MemoryStore::new(&MemorySpec::default())),
    )
    .await?;
    let (tx, rx) = tokio::sync::broadcast::channel(1);
    let mut worker_task = nativelink_util::background_spawn!("fleet_worker", worker.run(rx));
    reply(json!({
        "ready": true,
        "pid": std::process::id(),
        "node": config.node,
        "process_cpus": std::thread::available_parallelism()?.get(),
        "disk": disk_snapshot(config.root.clone()).await?,
        "physical_limit_bytes": 1024*MIB,
        "logical_cache_bytes": 512*MIB
    }))
    .await?;
    // Tokio's stdin uses an uncancellable blocking read that can hold runtime
    // shutdown open after a worker failure. A detached reader thread does not.
    let (commands_tx, mut commands) = tokio::sync::mpsc::channel(16);
    #[expect(
        clippy::disallowed_methods,
        reason = "Uncancellable stdin must use a detached OS thread independent of Tokio runtime shutdown"
    )]
    let _control_reader = std::thread::Builder::new()
        .name("fleet-control".into())
        .spawn(move || {
            for line in std::io::stdin().lock().lines() {
                if commands_tx.blocking_send(line).is_err() {
                    break;
                }
            }
        })?;
    let mut worker_finished = false;
    let control_result: Result<()> = async {
        loop {
            let line = tokio::select! {
                result = &mut worker_task => {
                    worker_finished = true;
                    return Err(std::io::Error::other(format!("worker exited before controller stop: {result:?}")).into());
                },
                line = commands.recv() => line.transpose()?,
            };
            let Some(line) = line else {
                return Err(std::io::Error::other("node controller disconnected").into());
            };
            match line.as_str() {
                "fill" => {
                    let root = config.root.clone();
                    let disk = nativelink_util::spawn_blocking!("fleet_fill", move || fill(&root)).await??;
                    reply(json!({"event": "filled", "pid": std::process::id(), "disk": disk})).await?;
                }
                "repair" => {
                    tokio::fs::remove_file(config.root.join("pressure")).await?;
                    let disk = disk_snapshot(config.root.clone()).await?;
                    reply(json!({"event": "repaired", "pid": std::process::id(), "disk": disk})).await?;
                }
                "snapshot" => {
                    let disk = disk_snapshot(config.root.clone()).await?;
                    reply(json!({"event": "snapshot", "pid": std::process::id(), "disk": disk})).await?;
                }
                "stop" => return Ok(()),
                _ => return Err(std::io::Error::other("unknown node control command").into()),
            }
        }
    }.await;
    if !worker_finished {
        // LocalWorker acknowledges drain by dropping its ShutdownGuard; its
        // service loop stays alive until the process exits, just as in the server.
        let mut guard = ShutdownGuard::default();
        let sent = tx.send(guard.clone()).is_ok();
        let drained = sent
            && tokio::time::timeout(Duration::from_secs(5), guard.wait_for(Priority::P0))
                .await
                .is_ok();
        worker_task.abort();
        drop(worker_task.await);
        if !drained && control_result.is_ok() {
            return Err(std::io::Error::other("worker did not drain within five seconds").into());
        }
    }
    control_result?;
    reply(json!({"event":"stopped","pid":std::process::id()})).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_counts_shared_inputs_once_and_charges_them_to_the_cache() -> Result<()> {
        struct Directory(PathBuf);
        impl Drop for Directory {
            fn drop(&mut self) {
                drop(std::fs::remove_dir_all(&self.0));
            }
        }
        let root = Directory(std::env::temp_dir().join(format!(
            "azure-fleet-snapshot-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_nanos()
        )));
        for name in ["content", "content.exec", "work"] {
            std::fs::create_dir_all(root.0.join(name))?;
        }
        std::fs::write(root.0.join("content/blob"), vec![1_u8; 8192])?;
        std::fs::write(root.0.join("content.exec/blob"), vec![1_u8; 8192])?;
        std::fs::write(root.0.join("work/output"), vec![2_u8; 4096])?;
        std::fs::hard_link(root.0.join("content/blob"), root.0.join("work/input"))?;
        std::fs::hard_link(root.0.join("content.exec/blob"), root.0.join("work/tool"))?;
        let content = std::fs::metadata(root.0.join("content/blob"))?.blocks() * 512;
        let exec = std::fs::metadata(root.0.join("content.exec/blob"))?.blocks() * 512;
        let work = std::fs::metadata(root.0.join("work/output"))?.blocks() * 512;
        let measured = snapshot(&root.0)?;
        assert_eq!(measured["unique_file_inodes"], 3);
        assert_eq!(measured["content_allocated"], content);
        assert_eq!(measured["exec_allocated"], exec);
        assert_eq!(measured["work_unique_allocated"], work);
        assert_eq!(measured["total_allocated"], content + exec + work);
        Ok(())
    }
}
