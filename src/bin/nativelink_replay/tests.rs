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

//! End-to-end tests: a `MemoryStore` behind the real `CasServer` and
//! `ByteStreamServer` on a localhost port, plus a fake `Execution` service
//! that answers with canned `ExecuteResponse`s (a real scheduler and worker
//! would be far heavier than what `execute` needs to be exercised).

use core::future::Future;
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use clap::Parser;
use futures::Stream;
use nativelink_config::cas_server::{ByteStreamConfig, CasStoreConfig, WithInstanceName};
use nativelink_config::stores::MemorySpec;
use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
use nativelink_proto::build::bazel::remote::execution::v2::execution_server::{
    Execution, ExecutionServer,
};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, ActionResult, Command, Directory, DirectoryNode, ExecuteRequest, ExecuteResponse,
    ExecutedActionMetadata, FileNode, OutputDirectory, OutputFile, SymlinkNode, Tree,
    WaitExecutionRequest,
};
use nativelink_proto::google::longrunning::Operation;
use nativelink_service::bytestream_server::ByteStreamServer;
use nativelink_service::cas_server::CasServer;
use nativelink_service::wire_compression::RemoteCacheCompressionInstances;
use nativelink_store::memory_store::MemoryStore;
use nativelink_store::store_manager::StoreManager;
use nativelink_util::background_spawn;
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use prost::Message;
use serde_json::Value;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};

use crate::local::{LocalOptions, Workspace, fetch_action, run_local};
use crate::manifest::{EntryKind, Problem, Run, digest_of, summarize};
use crate::remote::{ConnectOptions, FetchLimits, Remote, done_operation};
use crate::{Cli, execute, run};

const SHA256: DigestHasherFunc = DigestHasherFunc::Sha256;

fn block_on<F: Future>(future: F) -> F::Output {
    #[expect(
        clippy::disallowed_methods,
        reason = "each test owns a runtime, as #[tokio::test] would"
    )]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    #[expect(
        clippy::disallowed_methods,
        reason = "each test owns a runtime, as #[tokio::test] would"
    )]
    runtime.block_on(future)
}

/// `sh` from `PATH`, else `/bin/sh`. NixOS has no `/bin/bash`, and its
/// `/bin/sh` is the only fixed path.
fn find_sh() -> String {
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .map(|dir| dir.join("sh"))
                .find(|path| path.is_file())
        })
        .map_or_else(|| "/bin/sh".to_string(), |path| path.display().to_string())
}

fn test_path_var() -> String {
    std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_string())
}

fn sh_command(script: &str, outputs: &[&str], working_directory: &str) -> Command {
    Command {
        arguments: vec![find_sh(), "-c".to_string(), script.to_string()],
        environment_variables: vec![EnvironmentVariable {
            name: "PATH".to_string(),
            value: test_path_var(),
        }],
        output_paths: outputs.iter().map(ToString::to_string).collect(),
        working_directory: working_directory.to_string(),
        ..Default::default()
    }
}

fn file_node(name: &str, digest: DigestInfo, is_executable: bool) -> FileNode {
    FileNode {
        name: name.to_string(),
        digest: Some(digest.into()),
        is_executable,
        node_properties: None,
    }
}

fn dir_node(name: &str, digest: DigestInfo) -> DirectoryNode {
    DirectoryNode {
        name: name.to_string(),
        digest: Some(digest.into()),
    }
}

#[derive(Debug, Default)]
struct FakeState {
    responses: Mutex<VecDeque<ExecuteResponse>>,
    pending: Mutex<HashMap<String, ExecuteResponse>>,
    /// Answer `Execute` with an unfinished operation and finish it only on
    /// `WaitExecution`.
    split: bool,
    executes: AtomicUsize,
    waits: AtomicUsize,
    skipped_cache_lookup: AtomicBool,
}

#[derive(Debug, Clone, Default)]
struct FakeExecution {
    state: Arc<FakeState>,
}

type OperationStream = Pin<Box<dyn Stream<Item = Result<Operation, Status>> + Send>>;

fn running(name: &str) -> Operation {
    Operation {
        name: name.to_string(),
        done: false,
        ..Default::default()
    }
}

#[tonic::async_trait]
impl Execution for FakeExecution {
    type ExecuteStream = OperationStream;
    type WaitExecutionStream = OperationStream;

    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let request = request.into_inner();
        if request.skip_cache_lookup {
            self.state
                .skipped_cache_lookup
                .store(true, Ordering::SeqCst);
        }
        let n = self.state.executes.fetch_add(1, Ordering::SeqCst) + 1;
        let name = format!("operations/fake-{n}");
        let response = self
            .state
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| Status::resource_exhausted("no more canned responses"))?;
        let ops = if self.state.split {
            self.state
                .pending
                .lock()
                .unwrap()
                .insert(name.clone(), response);
            vec![Ok(running(&name))]
        } else {
            vec![Ok(running(&name)), Ok(done_operation(&name, &response))]
        };
        Ok(Response::new(Box::pin(futures::stream::iter(ops))))
    }

    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        self.state.waits.fetch_add(1, Ordering::SeqCst);
        let name = request.into_inner().name;
        let response = self
            .state
            .pending
            .lock()
            .unwrap()
            .remove(&name)
            .ok_or_else(|| Status::not_found(format!("no operation {name}")))?;
        Ok(Response::new(Box::pin(futures::stream::iter(vec![Ok(
            done_operation(&name, &response),
        )]))))
    }
}

struct TestEnv {
    port: u16,
    store: Store,
    fake: FakeExecution,
}

impl TestEnv {
    async fn start(responses: Vec<ExecuteResponse>, split: bool) -> Self {
        let store = Store::new(MemoryStore::new(&MemorySpec::default()));
        let store_manager = StoreManager::new();
        store_manager.add_store("main_cas", store.clone()).unwrap();
        let compression = RemoteCacheCompressionInstances::default();
        let bytestream = ByteStreamServer::new(
            &[WithInstanceName {
                instance_name: String::new(),
                config: ByteStreamConfig {
                    cas_store: "main_cas".to_string(),
                    ..Default::default()
                },
            }],
            &store_manager,
            &compression,
        )
        .unwrap();
        let cas = CasServer::new(
            &[WithInstanceName {
                instance_name: String::new(),
                config: CasStoreConfig {
                    cas_store: "main_cas".to_string(),
                    experimental_chunking: None,
                },
            }],
            &store_manager,
            &compression,
        )
        .unwrap();
        let fake = FakeExecution {
            state: Arc::new(FakeState {
                responses: Mutex::new(responses.into()),
                split,
                ..Default::default()
            }),
        };
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = incoming.local_addr().unwrap().port();
        let routes = tonic::service::Routes::new(bytestream.into_service())
            .add_service(cas.into_service())
            .add_service(ExecutionServer::new(fake.clone()));
        drop(background_spawn!(
            "nativelink_replay_test_server",
            async move {
                drop(
                    tonic::transport::Server::builder()
                        .add_routes(routes)
                        .serve_with_incoming(incoming)
                        .await,
                );
            }
        ));
        Self { port, store, fake }
    }

    fn url(&self) -> String {
        format!("grpc://127.0.0.1:{}", self.port)
    }

    async fn put(&self, data: &[u8]) -> DigestInfo {
        let digest = digest_of(SHA256, data);
        self.store
            .update_oneshot(digest, Bytes::copy_from_slice(data))
            .await
            .unwrap();
        digest
    }

    async fn put_proto<T: Message>(&self, message: &T) -> DigestInfo {
        self.put(&message.encode_to_vec()).await
    }

    /// Stores `data` under `digest` although it does not hash to it.
    async fn put_wrong(&self, digest: DigestInfo, data: &[u8]) {
        self.store
            .update_oneshot(digest, Bytes::copy_from_slice(data))
            .await
            .unwrap();
    }

    async fn put_action(&self, command: &Command, root: &Directory) -> DigestInfo {
        let action = Action {
            command_digest: Some(self.put_proto(command).await.into()),
            input_root_digest: Some(self.put_proto(root).await.into()),
            ..Default::default()
        };
        self.put_proto(&action).await
    }

    async fn remote(&self, limits: FetchLimits) -> Remote {
        let mut remote = Remote::connect(&ConnectOptions {
            cas: self.url(),
            connect_execution: true,
            ..Default::default()
        })
        .await
        .unwrap();
        remote.limits = limits;
        remote
    }
}

async fn local(env: &TestEnv, action: DigestInfo, options: LocalOptions) -> Vec<Run> {
    let remote = env.remote(FetchLimits::default()).await;
    let spec = fetch_action(&remote, action).await.unwrap().unwrap();
    let workspace = Workspace::create(None).unwrap();
    let runs = run_local(&remote, &spec, &options, &workspace)
        .await
        .unwrap();
    workspace.remove().unwrap();
    runs
}

fn options(runs: usize) -> LocalOptions {
    LocalOptions {
        runs,
        ..Default::default()
    }
}

async fn cli(args: &[&str]) -> (u8, String) {
    let cli =
        Cli::try_parse_from(core::iter::once("nativelink_replay").chain(args.iter().copied()))
            .unwrap();
    let output = run(cli).await.unwrap();
    (output.code, output.text)
}

fn hello_action_parts() -> (Command, &'static [u8]) {
    (
        sh_command("cat in.txt > out.txt", &["out.txt"], ""),
        b"hello\n",
    )
}

#[test]
fn local_is_stable_for_a_deterministic_command() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let (command, contents) = hello_action_parts();
        let input = env.put(contents).await;
        let root = Directory {
            files: vec![file_node("in.txt", input, false)],
            ..Default::default()
        };
        let action = env.put_action(&command, &root).await;
        let runs = local(&env, action, options(3)).await;
        let summary = summarize(&runs);
        assert_eq!(summary.completed, 3, "{runs:#?}");
        assert_eq!(summary.fingerprints.len(), 1);
        assert!(summary.varied_paths.is_empty());
        assert_eq!(summary.mismatches, 0);
        let outcome = runs[0].outcome.as_ref().unwrap();
        assert_eq!(outcome.exit_code, 0);
        assert_eq!(outcome.stdout, Some(digest_of(SHA256, b"")));
        let entries = outcome.manifest.entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "out.txt");
        assert_eq!(entries[0].kind, EntryKind::File);
        assert_eq!(entries[0].digest, Some(input));
    });
}

#[test]
fn local_varies_for_a_command_printing_the_time() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let command = sh_command("(date +%s%N; echo $$) > out.txt", &["out.txt"], "");
        let action = env.put_action(&command, &Directory::default()).await;
        let runs = local(&env, action, options(3)).await;
        let summary = summarize(&runs);
        assert_eq!(summary.completed, 3, "{runs:#?}");
        assert_eq!(summary.fingerprints.len(), 3);
        let paths: Vec<&str> = summary
            .varied_paths
            .iter()
            .map(|v| v.path.as_str())
            .collect();
        assert_eq!(paths, vec!["out.txt"]);
    });
}

#[test]
fn local_materializes_nested_inputs_modes_and_symlinks() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let script = b"set -e\n\
            mkdir -p outdir/sub\n\
            cat data/in.txt > outdir/a.txt\n\
            cat link > outdir/sub/b.txt\n\
            if test -x bin/gen.sh; then echo exec > outdir/exec.txt; fi\n\
            if test -x data/in.txt; then echo wrong > outdir/exec.txt; fi\n\
            ln -s a.txt outdir/l\n\
            echo x > deep/nested/file.txt\n\
            pwd > outdir/pwd.txt\n";
        let gen_digest = env.put(script).await;
        let hello = env.put(b"hello\n").await;
        let bin = Directory {
            files: vec![file_node("gen.sh", gen_digest, true)],
            ..Default::default()
        };
        let data = Directory {
            files: vec![file_node("in.txt", hello, false)],
            ..Default::default()
        };
        let work = Directory {
            directories: vec![
                dir_node("bin", env.put_proto(&bin).await),
                dir_node("data", env.put_proto(&data).await),
            ],
            symlinks: vec![SymlinkNode {
                name: "link".to_string(),
                target: "data/in.txt".to_string(),
                node_properties: None,
            }],
            ..Default::default()
        };
        let root = Directory {
            directories: vec![dir_node("work", env.put_proto(&work).await)],
            ..Default::default()
        };
        let command = Command {
            arguments: vec![find_sh(), "bin/gen.sh".to_string()],
            ..sh_command("", &["deep/nested/file.txt", "outdir"], "work")
        };
        let action = env.put_action(&command, &root).await;

        let runs = local(&env, action, options(2)).await;
        let summary = summarize(&runs);
        assert_eq!(summary.completed, 2, "{runs:#?}");
        // A directory per run, as on a worker: the recorded working
        // directory is the only difference.
        let paths: Vec<&str> = summary
            .varied_paths
            .iter()
            .map(|v| v.path.as_str())
            .collect();
        assert_eq!(paths, vec!["outdir/pwd.txt"], "{summary:#?}");
        let entries: Vec<(&str, &str, String)> = runs[0]
            .outcome
            .as_ref()
            .unwrap()
            .manifest
            .entries()
            .iter()
            .filter(|e| e.path != "outdir/pwd.txt")
            .map(|e| (e.path.as_str(), e.kind.as_str(), e.value()))
            .collect();
        assert_eq!(
            entries,
            vec![
                (
                    "deep/nested/file.txt",
                    "file",
                    digest_of(SHA256, b"x\n").to_string()
                ),
                ("outdir/a.txt", "directory_file", hello.to_string()),
                (
                    "outdir/exec.txt",
                    "directory_file",
                    digest_of(SHA256, b"exec\n").to_string()
                ),
                ("outdir/l", "symlink", "symlink -> a.txt".to_string()),
                ("outdir/sub/b.txt", "directory_file", hello.to_string()),
            ]
        );

        // Every run at one path: even pwd is stable.
        let same_path = LocalOptions {
            runs: 2,
            same_path: true,
            ..Default::default()
        };
        let runs = local(&env, action, same_path).await;
        let summary = summarize(&runs);
        assert_eq!(summary.completed, 2, "{runs:#?}");
        assert_eq!(summary.fingerprints.len(), 1, "{summary:#?}");
    });
}

#[test]
fn local_refuses_a_working_directory_outside_the_input_root() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let outside = std::env::temp_dir();
        let marker = format!("nativelink-replay-escape-{}", std::process::id());
        let root = Directory {
            symlinks: vec![SymlinkNode {
                name: "work".to_string(),
                target: outside.display().to_string(),
                node_properties: None,
            }],
            ..Default::default()
        };
        let output = format!("{marker}/out.txt");
        let command = sh_command("echo escaped > out.txt", &[output.as_str()], "work");
        let action = env.put_action(&command, &root).await;
        let runs = local(&env, action, options(1)).await;
        assert!(runs[0].outcome.is_none(), "{runs:#?}");
        assert!(
            runs[0]
                .error
                .as_deref()
                .unwrap()
                .contains("inside the input root"),
            "{runs:#?}"
        );
        assert!(!outside.join(&marker).exists());
    });
}

async fn corrupt_hello_action(env: &TestEnv) -> (DigestInfo, DigestInfo) {
    let (command, contents) = hello_action_parts();
    let input = digest_of(SHA256, contents);
    // Same length, other bytes.
    env.put_wrong(input, b"jello\n").await;
    let root = Directory {
        files: vec![file_node("in.txt", input, false)],
        ..Default::default()
    };
    (env.put_action(&command, &root).await, input)
}

fn assert_one_mismatch(json: &Value, via: &str) {
    let run = &json["runs"][0];
    assert_eq!(run["status"], "MISMATCH", "{json:#}");
    assert!(run["outcome"].is_null());
    let problem = &run["problems"][0];
    assert_eq!(problem["kind"], "digest_mismatch");
    assert_eq!(problem["what"], "input file 'in.txt'");
    assert_eq!(problem["via"], via);
    assert_eq!(problem["actual"], digest_of(SHA256, b"jello\n").to_string());
    assert_eq!(json["summary"]["digest_mismatches"], 1);
}

#[test]
fn local_reports_wrong_bytes_from_batch_read_blobs() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let (action, _) = corrupt_hello_action(&env).await;
        let url = env.url();
        let action = action.to_string();
        let (code, text) = cli(&[
            "local", "--cas", &url, "--action", &action, "--runs", "1", "--json",
        ])
        .await;
        assert_eq!(code, 2, "{text}");
        assert_one_mismatch(&serde_json::from_str(&text).unwrap(), "batch_read_blobs");
    });
}

#[test]
fn local_reports_wrong_bytes_from_bytestream() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let (action, _) = corrupt_hello_action(&env).await;
        let url = env.url();
        let action = action.to_string();
        let (code, text) = cli(&[
            "local",
            "--cas",
            &url,
            "--action",
            &action,
            "--runs",
            "1",
            "--max-batch-blob-bytes",
            "0",
            "--json",
        ])
        .await;
        assert_eq!(code, 2, "{text}");
        assert_one_mismatch(&serde_json::from_str(&text).unwrap(), "bytestream");

        // The text report says so loudly.
        let (code, text) = cli(&["local", "--cas", &url, "--action", &action, "--runs", "1"]).await;
        assert_eq!(code, 2);
        assert!(
            text.contains("!! DIGEST MISMATCH for input file 'in.txt'"),
            "{text}"
        );
    });
}

#[test]
fn fetch_verifies_and_keeps_wrong_bytes_aside() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let good = env.put(b"good bytes\n").await;
        let (_, bad) = corrupt_hello_action(&env).await;
        let workspace = Workspace::create(None).unwrap();
        let url = env.url();

        let out = workspace.base.join("good");
        let (code, text) = cli(&[
            "fetch",
            "--cas",
            &url,
            "--digest",
            &good.to_string(),
            "--out",
            out.to_str().unwrap(),
        ])
        .await;
        assert_eq!(code, 0, "{text}");
        assert_eq!(std::fs::read(&out).unwrap(), b"good bytes\n");

        let out = workspace.base.join("bad");
        let (code, text) = cli(&[
            "fetch",
            "--cas",
            &url,
            "--digest",
            &bad.to_string(),
            "--out",
            out.to_str().unwrap(),
            "--json",
        ])
        .await;
        assert_eq!(code, 2, "{text}");
        assert!(!out.exists());
        let kept = workspace.base.join("bad.mismatch");
        assert_eq!(std::fs::read(&kept).unwrap(), b"jello\n");
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["verified"], false);
        assert_eq!(json["problem"]["kind"], "digest_mismatch");
        workspace.remove().unwrap();
    });
}

#[test]
fn show_prints_the_command_and_input_summary() {
    block_on(async {
        let env = TestEnv::start(vec![], false).await;
        let (command, contents) = hello_action_parts();
        let input = env.put(contents).await;
        let sub = Directory {
            files: vec![file_node("tool", input, true)],
            ..Default::default()
        };
        let root = Directory {
            files: vec![file_node("in.txt", input, false)],
            directories: vec![dir_node("sub", env.put_proto(&sub).await)],
            symlinks: vec![SymlinkNode {
                name: "l".to_string(),
                target: "in.txt".to_string(),
                node_properties: None,
            }],
            ..Default::default()
        };
        let action = env.put_action(&command, &root).await;
        let url = env.url();
        let (code, text) = cli(&[
            "show",
            "--cas",
            &url,
            "--action",
            &action.to_string(),
            "--json",
        ])
        .await;
        assert_eq!(code, 0, "{text}");
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["arguments"][2], "cat in.txt > out.txt");
        assert_eq!(json["environment"][0]["name"], "PATH");
        assert_eq!(json["output_paths"][0], "out.txt");
        assert_eq!(json["do_not_cache"], false);
        let inputs = &json["input_root"];
        assert_eq!(inputs["files"], 2);
        assert_eq!(inputs["executables"], 1);
        assert_eq!(inputs["directories"], 1);
        assert_eq!(inputs["symlinks"], 1);
        assert_eq!(inputs["total_bytes"], 12);
        assert_eq!(inputs["unique_blobs"], 1);

        let (code, text) = cli(&["show", "--cas", &url, "--action", &action.to_string()]).await;
        assert_eq!(code, 0);
        assert!(text.contains("[2] cat in.txt > out.txt"), "{text}");
        assert!(text.contains("files        2 (1 executable)"), "{text}");
    });
}

fn canned(worker: &str, out: DigestInfo, tree: Option<DigestInfo>) -> ExecuteResponse {
    ExecuteResponse {
        result: Some(ActionResult {
            output_files: vec![OutputFile {
                path: "out.txt".to_string(),
                digest: Some(out.into()),
                ..Default::default()
            }],
            output_directories: tree
                .map(|digest| OutputDirectory {
                    path: "gen".to_string(),
                    tree_digest: Some(digest.into()),
                    ..Default::default()
                })
                .into_iter()
                .collect(),
            stdout_raw: Bytes::from_static(b"log\n"),
            execution_metadata: Some(ExecutedActionMetadata {
                worker: worker.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn execute_follows_the_operation_and_expands_output_trees() {
    block_on(async {
        let a = digest_of(SHA256, b"a");
        let b = digest_of(SHA256, b"b");
        let sub = Directory {
            files: vec![file_node("b", b, true)],
            ..Default::default()
        };
        let tree = Tree {
            root: Some(Directory {
                files: vec![file_node("a", a, false)],
                directories: vec![dir_node("sub", digest_of(SHA256, &sub.encode_to_vec()))],
                ..Default::default()
            }),
            children: vec![sub],
        };
        let out = digest_of(SHA256, b"out");
        let tree_digest = digest_of(SHA256, &tree.encode_to_vec());
        let env = TestEnv::start(
            vec![
                canned("worker-1", out, Some(tree_digest)),
                canned("worker-2", out, Some(tree_digest)),
            ],
            true,
        )
        .await;
        env.put_proto(&tree).await;
        let remote = env.remote(FetchLimits::default()).await;
        let action = digest_of(SHA256, b"any action");
        let runs = execute::execute_runs(&remote, action, 2).await;

        assert!(env.fake.state.skipped_cache_lookup.load(Ordering::SeqCst));
        assert_eq!(env.fake.state.executes.load(Ordering::SeqCst), 2);
        // Each Execute stream ended unfinished and was resumed.
        assert_eq!(env.fake.state.waits.load(Ordering::SeqCst), 2);
        let workers: Vec<&str> = runs.iter().map(|r| r.worker.as_str()).collect();
        assert_eq!(workers, vec!["worker-1", "worker-2"]);
        let outcome = runs[0].outcome.as_ref().unwrap();
        assert_eq!(outcome.stdout, Some(digest_of(SHA256, b"log\n")));
        let entries: Vec<(&str, EntryKind, Option<DigestInfo>, bool)> = outcome
            .manifest
            .entries()
            .iter()
            .map(|e| (e.path.as_str(), e.kind, e.digest, e.executable))
            .collect();
        assert_eq!(
            entries,
            vec![
                ("gen/a", EntryKind::DirectoryFile, Some(a), false),
                ("gen/sub/b", EntryKind::DirectoryFile, Some(b), true),
                ("out.txt", EntryKind::File, Some(out), false),
            ]
        );
        let summary = summarize(&runs);
        assert!(!summary.varies());
    });
}

#[test]
fn execute_reports_a_tree_served_with_wrong_bytes() {
    block_on(async {
        let tree = Tree {
            root: Some(Directory::default()),
            children: vec![],
        };
        let tree_digest = digest_of(SHA256, &tree.encode_to_vec());
        let env = TestEnv::start(vec![canned("w", tree_digest, Some(tree_digest))], false).await;
        let mut wrong = tree.encode_to_vec();
        wrong.push(0);
        env.put_wrong(tree_digest, &wrong).await;
        let remote = env.remote(FetchLimits::default()).await;
        let runs = execute::execute_runs(&remote, tree_digest, 1).await;
        assert!(runs[0].outcome.is_none());
        assert!(
            matches!(runs[0].problems[0], Problem::Mismatch { .. }),
            "{runs:#?}"
        );
    });
}

#[test]
fn compare_flags_a_nativelink_side_difference() {
    block_on(async {
        let (command, contents) = hello_action_parts();
        let hello = digest_of(SHA256, contents);
        let tampered = digest_of(SHA256, b"tampered\n");
        let env = TestEnv::start(
            vec![
                canned("worker-1", hello, None),
                canned("worker-2", tampered, None),
            ],
            false,
        )
        .await;
        env.put(contents).await;
        let root = Directory {
            files: vec![file_node("in.txt", hello, false)],
            ..Default::default()
        };
        let action = env.put_action(&command, &root).await;
        let url = env.url();
        let (code, text) = cli(&[
            "compare",
            "--cas",
            &url,
            "--action",
            &action.to_string(),
            "--runs",
            "2",
            "--json",
        ])
        .await;
        assert_eq!(code, 0, "{text}");
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["oracle_varies"], false, "{json:#}");
        assert_eq!(json["nativelink_varies"], true);
        assert_eq!(json["verdict"], "nativelink_varies");
        let unmatched = &json["nativelink_digests_not_in_oracle"];
        assert_eq!(unmatched.as_array().unwrap().len(), 1);
        assert_eq!(unmatched[0]["path"], "out.txt");
        assert_eq!(unmatched[0]["digest"], tampered.to_string());
        assert_eq!(unmatched[0]["runs"][0], 2);
        assert!(
            json["interpretation"][0]
                .as_str()
                .unwrap()
                .contains("on the NativeLink side")
        );
    });
}

#[test]
fn cli_parses_common_flags() {
    let digest = digest_of(SHA256, b"x").to_string();
    let cli = Cli::try_parse_from([
        "nativelink_replay",
        "execute",
        "--cas",
        "grpcs://cas.example:443",
        "--execution",
        "grpcs://exec.example:443",
        "--instance-name",
        "main",
        "--digest-function",
        "blake3",
        "--header",
        "x-api-key=secret=with=equals",
        "--header",
        "x-other=2",
        "--action",
        &digest,
        "--runs",
        "5",
    ])
    .unwrap();
    let crate::Commands::Execute(args) = cli.command else {
        panic!("not execute");
    };
    assert_eq!(args.runs, 5);
    assert_eq!(args.common.instance_name, "main");
    assert_eq!(
        DigestHasherFunc::from(args.common.digest_function),
        DigestHasherFunc::Blake3
    );
    assert_eq!(
        args.common.headers,
        vec![
            ("x-api-key".to_string(), "secret=with=equals".to_string()),
            ("x-other".to_string(), "2".to_string()),
        ]
    );
    for bad in [
        vec![
            "nativelink_replay",
            "execute",
            "--cas",
            "x",
            "--action",
            "nope",
        ],
        vec![
            "nativelink_replay",
            "local",
            "--cas",
            "x",
            "--action",
            &digest,
            "--runs",
            "0",
        ],
        vec![
            "nativelink_replay",
            "show",
            "--cas",
            "x",
            "--action",
            &digest,
            "--header",
            "novalue",
        ],
    ] {
        assert!(
            Cli::try_parse_from(bad.clone()).is_err(),
            "accepted {bad:?}"
        );
    }
}
