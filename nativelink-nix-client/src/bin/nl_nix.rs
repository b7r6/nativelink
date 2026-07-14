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

//! `nl-nix` — a `nix copy`-style client that pushes and pulls store paths
//! to/from a NativeLink `nix_cache`, streaming zstd on the wire.

use std::collections::HashSet;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use futures::stream::{FuturesUnordered, StreamExt as _};
use nativelink_error::{Error, ResultExt, make_input_err};
use nativelink_nix_client::client::{CacheClient, PushOutcome};
use nativelink_nix_client::config::{Auth, CacheConfig};
use nativelink_nix_client::metrics::Metrics;
use nativelink_nix_client::store::{NixStore, StorePath};

#[derive(Parser)]
#[command(
    name = "nl-nix",
    version,
    about = "Nix binary-cache client for NativeLink: push/pull, streaming zstd, OTLP metrics"
)]
struct Cli {
    /// Base URL of the cache.
    #[arg(long, env = "NL_NIX_CACHE")]
    to: String,
    /// Local Nix store directory.
    #[arg(long, default_value = "/nix/store")]
    store: String,
    /// Bearer token (Basic uses it as the password with an empty user).
    #[arg(long, env = "NL_NIX_TOKEN")]
    token: Option<String>,
    /// Ed25519 secret-key file to sign pushed narinfo (repeatable).
    #[arg(long = "signing-key")]
    signing_keys: Vec<String>,
    /// zstd level for uploads.
    #[arg(long, default_value_t = 3)]
    compression_level: i32,
    /// Upload uncompressed instead of zstd.
    #[arg(long)]
    no_compress: bool,
    /// Maximum concurrent path transfers.
    #[arg(long, default_value_t = 8)]
    jobs: usize,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Push store paths (optionally their whole closure) to the cache.
    Push {
        /// Store paths to push.
        paths: Vec<String>,
        /// Also push the entire reference closure of each path.
        #[arg(long)]
        recursive: bool,
    },
    /// Pull store paths by 32-char hash from the cache, verifying + restoring
    /// each NAR into `--out/<hash>` (does not register them in the store).
    Pull {
        /// Store-path hashes to pull.
        hashes: Vec<String>,
        /// Directory to restore each path into.
        #[arg(long)]
        out: String,
    },
    /// Print the cache's `/nix-cache-info`.
    Info,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Best-effort OTLP telemetry, identical to the server; a missing collector
    // just means metrics are dropped, never that the client fails.
    if let Err(err) = nativelink_util::telemetry::init_tracing().await {
        eprintln!("nl-nix: telemetry init failed (continuing without metrics): {err}");
    }
    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<(), Error> {
    let config = CacheConfig {
        base_url: cli.to.clone(),
        auth: cli.token.clone().map(Auth::Bearer),
        signing_key_files: cli.signing_keys.clone(),
        store_dir: cli.store.clone(),
        compress: !cli.no_compress,
        compression_level: cli.compression_level,
        concurrency: cli.jobs,
        dedup: true,
    };
    let metrics = Arc::new(Metrics::new("nl-nix"));
    let client = Arc::new(CacheClient::new(config, metrics)?);

    match cli.command {
        Command::Info => {
            print!("{}", client.cache_info().await?);
            Ok(())
        }
        Command::Push { paths, recursive } => {
            let store = Arc::new(NixStore::open(&cli.store)?);
            let roots = parse_paths(&cli.store, &paths)?;
            let ordered = if recursive {
                closure(&store, roots)?
            } else {
                roots
            };
            push_all(&client, &store, ordered, cli.jobs).await
        }
        Command::Pull { hashes, out } => {
            let out = std::path::PathBuf::from(out);
            for hash in hashes {
                let dest = out.join(&hash);
                let size = client.pull_path(&hash, &[], &dest).await?;
                println!("pulled {hash} ({size} NAR bytes) -> {}", dest.display());
            }
            Ok(())
        }
    }
}

/// Parses CLI path arguments into validated store paths.
fn parse_paths(store_dir: &str, paths: &[String]) -> Result<Vec<StorePath>, Error> {
    if paths.is_empty() {
        return Err(make_input_err!("no store paths given"));
    }
    paths
        .iter()
        .map(|p| StorePath::from_full_path(store_dir, p))
        .collect()
}

/// Computes the reference closure of `roots`, returned references-first so a
/// path is always pushed after everything it depends on.
fn closure(store: &NixStore, roots: Vec<StorePath>) -> Result<Vec<StorePath>, Error> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut order: Vec<StorePath> = Vec::new();
    for root in roots {
        visit(store, root, &mut seen, &mut order)?;
    }
    Ok(order)
}

fn visit(
    store: &NixStore,
    path: StorePath,
    seen: &mut HashSet<String>,
    order: &mut Vec<StorePath>,
) -> Result<(), Error> {
    if !seen.insert(path.full_path().to_string()) {
        return Ok(());
    }
    let meta = store.query_path_info(&path)?;
    for reference in meta.references {
        // A path references itself; skip to avoid infinite recursion.
        if reference.full_path() != path.full_path() {
            visit(store, reference, seen, order)?;
        }
    }
    order.push(path);
    Ok(())
}

/// Pushes every path with bounded concurrency, reporting a one-line summary.
async fn push_all(
    client: &Arc<CacheClient>,
    store: &Arc<NixStore>,
    paths: Vec<StorePath>,
    jobs: usize,
) -> Result<(), Error> {
    let total = paths.len();
    let jobs = jobs.max(1);
    let mut iter = paths.into_iter();
    let mut inflight = FuturesUnordered::new();
    let mut uploaded = 0u64;
    let mut skipped = 0u64;
    let mut wire = 0u64;

    loop {
        while inflight.len() < jobs {
            let Some(path) = iter.next() else { break };
            let client = Arc::clone(client);
            let store = Arc::clone(store);
            inflight.push(async move {
                let meta = store.query_path_info(&path)?;
                let outcome = client.push_path(&meta).await?;
                Ok::<(StorePath, PushOutcome), Error>((path, outcome))
            });
        }
        let Some(result) = inflight.next().await else {
            break;
        };
        match result {
            Ok((path, PushOutcome::Uploaded { wire_bytes, .. })) => {
                uploaded += 1;
                wire += wire_bytes;
                println!("pushed {}", path.full_path());
            }
            Ok((path, PushOutcome::AlreadyPresent)) => {
                skipped += 1;
                println!("skipped (present) {}", path.full_path());
            }
            Err(err) => {
                eprintln!("push failed: {err}");
                return Err(err).err_tip(|| "one or more pushes failed");
            }
        }
    }

    println!(
        "done: {uploaded} uploaded, {skipped} already present, of {total} paths ({wire} wire bytes)"
    );
    Ok(())
}
