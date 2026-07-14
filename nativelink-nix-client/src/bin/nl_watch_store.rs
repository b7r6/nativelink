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

//! `nl-watch-store` — a daemon that watches the local Nix store with `fanotify`
//! and auto-pushes every newly-committed path to a NativeLink `nix_cache`,
//! deduplicating against the cache.

use std::sync::Arc;

use clap::Parser;
use futures::StreamExt as _;
use nativelink_error::{Code, Error, make_err};
use nativelink_nix_client::client::{CacheClient, PushOutcome};
use nativelink_nix_client::config::{Auth, CacheConfig};
use nativelink_nix_client::metrics::Metrics;
use nativelink_nix_client::store::NixStore;
use nativelink_nix_client::watch::watch_store;
use tokio::sync::Semaphore;

#[derive(Parser)]
#[command(
    name = "nl-watch-store",
    version,
    about = "Watch the local Nix store and auto-push new paths to a NativeLink nix_cache"
)]
struct Cli {
    /// Base URL of the cache to push to.
    #[arg(long, env = "NL_NIX_CACHE")]
    to: String,
    /// Local Nix store directory to watch.
    #[arg(long, default_value = "/nix/store")]
    store: String,
    /// Bearer write token.
    #[arg(long, env = "NL_NIX_TOKEN")]
    token: Option<String>,
    /// Ed25519 secret-key file to sign pushed narinfo (repeatable).
    #[arg(long = "signing-key")]
    signing_keys: Vec<String>,
    /// zstd level for uploads.
    #[arg(long, default_value_t = 3)]
    compression_level: i32,
    /// Maximum concurrent path pushes.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    /// Use the inotify fallback instead of fanotify (unprivileged/older kernels).
    #[arg(long)]
    inotify: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    if let Err(err) = nativelink_util::telemetry::init_tracing().await {
        eprintln!("nl-watch-store: telemetry init failed (continuing without metrics): {err}");
    }
    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<(), Error> {
    let config = CacheConfig {
        base_url: cli.to.clone(),
        auth: cli.token.map(Auth::Bearer),
        signing_key_files: cli.signing_keys,
        store_dir: cli.store.clone(),
        compress: true,
        compression_level: cli.compression_level,
        concurrency: cli.concurrency,
        dedup: true,
    };
    let metrics = Arc::new(Metrics::new("nl-watch-store"));
    let client = Arc::new(CacheClient::new(config, metrics)?);
    // The DB reader holds a `!Sync` connection, so metadata is queried on this
    // task; only the (Send) push is spawned.
    let store = NixStore::open(&cli.store)?;
    let semaphore = Arc::new(Semaphore::new(cli.concurrency.max(1)));

    let mut events = watch_store(&cli.store, cli.inotify)?;
    tracing::info!(store = %cli.store, cache = %cli.to, "nl-watch-store watching");

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| make_err!(Code::Internal, "installing SIGTERM handler: {e}"))?;

    loop {
        let event = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT; draining and stopping");
                break;
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM; draining and stopping");
                break;
            }
            maybe = events.next() => match maybe {
                Some(event) => event,
                None => break,
            },
        };

        // Read metadata on this task (the DB connection is not `Sync`).
        let meta = match store.query_path_info(&event.path) {
            Ok(meta) => meta,
            Err(err) => {
                tracing::debug!(?err, path = %event.path.full_path(), "skipping (not a valid path yet)");
                continue;
            }
        };
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|e| make_err!(Code::Internal, "acquiring push permit: {e}"))?;
        let client = Arc::clone(&client);
        drop(nativelink_util::background_spawn!(
            "push_watched_path",
            async move {
                let _permit = permit;
                match client.push_path(&meta).await {
                    Ok(PushOutcome::Uploaded {
                        nar_size,
                        wire_bytes,
                    }) => {
                        tracing::info!(
                            path = %meta.path.full_path(),
                            nar_size,
                            wire_bytes,
                            "pushed"
                        );
                    }
                    Ok(PushOutcome::AlreadyPresent) => {
                        tracing::debug!(path = %meta.path.full_path(), "already present");
                    }
                    Err(err) => {
                        tracing::warn!(?err, path = %meta.path.full_path(), "push failed");
                    }
                }
            }
        ));
    }

    Ok(())
}
