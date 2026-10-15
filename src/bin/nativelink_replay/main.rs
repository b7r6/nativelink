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

//! `nativelink_replay`: replay one Remote Execution action, locally and on
//! the executor, to tell an action that is nondeterministic from a fault on
//! the server side.

mod execute;
mod local;
mod manifest;
mod remote;
mod report;
#[cfg(test)]
mod tests;

use core::time::Duration;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand, ValueEnum};
use nativelink_error::{Error, make_err};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use serde_json::{Value, json};

use crate::local::{
    LocalOptions, Workspace, fetch_action, inspect_inputs, run_local, summarize_inputs,
};
use crate::manifest::{Problem, compare_runs, parse_digest, summarize};
use crate::remote::{ConnectOptions, FetchLimits, Remote};

const LONG_ABOUT: &str = "\
Replays one Remote Execution action to decide whether output that changes \
from run to run comes from the action itself or from NativeLink.

`local` is the oracle: it fetches the action's inputs from the CAS, verifies \
every blob against its digest, writes them with plain file writes into a \
fresh directory, runs the command with exactly the Command's environment on \
this host, and hashes the outputs. `execute` runs the same action on the \
executor. `compare` does both and says which side varies.

Run `local` and `compare` on a host with the same image and toolchain as the \
workers; anything else is a different environment and its outputs can differ \
for that reason alone.

show, fetch and local only read from the CAS (BatchReadBlobs and \
ByteStream.Read), though a fast_slow store on the server may copy what they \
read into its fast tier. execute and compare also execute the action remotely, \
which writes to the CAS and the Action Cache.

Exit status: 0 on success, 1 on an error, 2 when a blob failed digest \
verification.";

const EXECUTE_ABOUT: &str = "\
Executes the action on the executor K times, one after another, with \
skip_cache_lookup=true, and compares the results.

NOT READ-ONLY. Each run is a normal execution: it occupies a worker, the \
worker uploads its outputs to the CAS, and unless the action has \
do_not_cache the server writes the ActionResult to the Action Cache, \
replacing the entry clients get on their next cache hit. Against a \
production cluster that is real load and real writes.";

const LOCAL_ABOUT: &str = "\
The independent oracle. Runs the action K times on THIS host: for each run, \
a fresh directory, every input fetched from the CAS and verified against its \
digest (a mismatch alone proves the CAS served wrong bytes), plain file \
writes with no hardlinks or caches, then argv run with exactly the Command's \
environment, and the outputs hashed with the digest function.

Read-only towards the CAS. But it runs the action's command unsandboxed, as \
the current user, on this machine: run it in a disposable container or VM \
built from the workers' image, never on a machine you care about.";

const COMPARE_ABOUT: &str = "\
Runs `local` and then `execute` with the same K and reports whether (a) the \
oracle varies, (b) NativeLink varies, and (c) NativeLink produced an output \
digest no oracle run produced.

Everything `local` and `execute` warn about applies: it runs the command \
unsandboxed on this host, and its remote runs write to the CAS and the \
Action Cache.";

#[derive(Parser, Debug)]
#[command(
    name = "nativelink_replay",
    version,
    about = "Replay a Remote Execution action to tell a nondeterministic action from a NativeLink fault",
    long_about = LONG_ABOUT
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Print an action's argv, environment, outputs, platform, timeout and input-root summary (read-only).
    Show(ShowArgs),
    #[command(
        about = "Read one blob over ByteStream, verify its digest and write it to a file (read-only)"
    )]
    Fetch(FetchArgs),
    #[command(
        about = "Execute the action K times remotely with skip_cache_lookup (writes the CAS and the AC)",
        long_about = EXECUTE_ABOUT
    )]
    Execute(ExecuteArgs),
    /// Run the action K times on this host from verified CAS inputs (the oracle).
    #[command(long_about = LOCAL_ABOUT)]
    Local(LocalArgs),
    /// Run `local` and `execute` and say which side varies.
    #[command(long_about = COMPARE_ABOUT)]
    Compare(CompareArgs),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum DigestFunctionArg {
    Sha256,
    Blake3,
}

impl From<DigestFunctionArg> for DigestHasherFunc {
    fn from(value: DigestFunctionArg) -> Self {
        match value {
            DigestFunctionArg::Sha256 => Self::Sha256,
            DigestFunctionArg::Blake3 => Self::Blake3,
        }
    }
}

#[derive(Args, Debug, Clone)]
struct Common {
    /// CAS endpoint: grpc://, grpcs://, http:// or https://HOST:PORT. TLS
    /// (grpcs, https) verifies against the system roots.
    #[arg(long, value_name = "URL")]
    cas: String,

    /// Execution endpoint, for execute and compare [default: --cas].
    #[arg(long, value_name = "URL")]
    execution: Option<String>,

    /// REAPI instance name.
    #[arg(long, default_value = "")]
    instance_name: String,

    /// Digest function of the digests given and of the CAS.
    #[arg(long, value_enum, default_value_t = DigestFunctionArg::Sha256)]
    digest_function: DigestFunctionArg,

    /// Header sent with every request, as KEY=VALUE (repeatable), for
    /// example an API key: --header x-nativelink-api-key=SECRET.
    #[arg(long = "header", value_name = "KEY=VALUE", value_parser = parse_key_value)]
    headers: Vec<(String, String)>,

    /// PEM file of CA certificates to trust for TLS, in addition to the
    /// system roots.
    #[arg(long, value_name = "FILE")]
    tls_ca_cert: Option<PathBuf>,

    #[arg(
        long,
        value_name = "BYTES",
        default_value_t = 1024 * 1024,
        help = "Blobs up to this size are read with BatchReadBlobs, larger ones with ByteStream; 0 reads every blob with ByteStream."
    )]
    max_batch_blob_bytes: u64,

    /// Print one JSON document instead of text.
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug, Clone)]
struct ShowArgs {
    #[command(flatten)]
    common: Common,
    /// The action digest, HASH-SIZE.
    #[arg(long, value_parser = parse_digest_arg)]
    action: DigestInfo,
}

#[derive(Args, Debug, Clone)]
struct FetchArgs {
    #[command(flatten)]
    common: Common,
    /// The blob digest, HASH-SIZE.
    #[arg(long, value_parser = parse_digest_arg)]
    digest: DigestInfo,
    /// Where to write the blob. A blob that fails verification is written
    /// to OUT.mismatch instead, for inspection.
    #[arg(long, value_name = "PATH")]
    out: PathBuf,
}

#[derive(Args, Debug, Clone)]
struct ExecuteArgs {
    #[command(flatten)]
    common: Common,
    /// The action digest, HASH-SIZE.
    #[arg(long, value_parser = parse_digest_arg)]
    action: DigestInfo,
    /// How many times to execute.
    #[arg(long, value_name = "K", default_value_t = 3, value_parser = parse_runs)]
    runs: usize,
}

#[derive(Args, Debug, Clone)]
struct LocalFlags {
    /// Keep each run's directory, stdout and stderr instead of removing them.
    #[arg(long)]
    keep_dirs: bool,

    /// Directory under which this invocation creates its own directory
    /// [default: the system temporary directory].
    #[arg(long, value_name = "DIR")]
    workdir_base: Option<PathBuf>,

    /// Kill a local run after this many seconds [default: the action's
    /// timeout, else 600].
    #[arg(long, value_name = "SECONDS")]
    timeout: Option<u64>,

    /// Run every repetition at the same absolute path. By default each run
    /// gets its own directory, as each execution does on a worker, so an
    /// action that records its working directory shows up as varying. With
    /// this flag it does not, which separates that from other causes.
    #[arg(long)]
    same_path: bool,

    #[arg(
        long = "extra-env",
        value_name = "KEY=VALUE",
        value_parser = parse_key_value,
        help = "Extra environment variable for local runs, as KEY=VALUE (repeatable), to mirror a worker's additional_environment. The Command's own variables win."
    )]
    extra_env: Vec<(String, String)>,
}

#[derive(Args, Debug, Clone)]
struct LocalArgs {
    #[command(flatten)]
    common: Common,
    /// The action digest, HASH-SIZE.
    #[arg(long, value_parser = parse_digest_arg)]
    action: DigestInfo,
    /// How many times to run.
    #[arg(long, value_name = "K", default_value_t = 3, value_parser = parse_runs)]
    runs: usize,
    #[command(flatten)]
    local: LocalFlags,
}

#[derive(Args, Debug, Clone)]
struct CompareArgs {
    #[command(flatten)]
    common: Common,
    /// The action digest, HASH-SIZE.
    #[arg(long, value_parser = parse_digest_arg)]
    action: DigestInfo,
    /// How many times to run on each side.
    #[arg(long, value_name = "K", default_value_t = 3, value_parser = parse_runs)]
    runs: usize,
    #[command(flatten)]
    local: LocalFlags,
}

fn parse_key_value(input: &str) -> Result<(String, String), String> {
    let (key, value) = input
        .split_once('=')
        .ok_or_else(|| format!("'{input}' is not KEY=VALUE"))?;
    if key.is_empty() {
        return Err(format!("'{input}' has an empty key"));
    }
    Ok((key.to_string(), value.to_string()))
}

fn parse_digest_arg(input: &str) -> Result<DigestInfo, String> {
    parse_digest(input).map_err(|e| e.message_string())
}

fn parse_runs(input: &str) -> Result<usize, String> {
    let runs: usize = input
        .parse()
        .map_err(|_| format!("'{input}' is not a number"))?;
    if runs == 0 || runs > 1000 {
        return Err("must be between 1 and 1000".to_string());
    }
    Ok(runs)
}

/// What a subcommand prints and how the process exits.
#[derive(Debug)]
struct Output {
    text: String,
    code: u8,
}

impl Output {
    fn new(common: &Common, text: String, json: &Value, mismatches: usize) -> Self {
        let text = if common.json {
            let mut rendered = serde_json::to_string_pretty(json).unwrap_or_default();
            rendered.push('\n');
            rendered
        } else {
            text
        };
        Self {
            text,
            code: if mismatches > 0 { 2 } else { 0 },
        }
    }
}

async fn connect(common: &Common, with_execution: bool) -> Result<Remote, Error> {
    let mut remote = Remote::connect(&ConnectOptions {
        cas: common.cas.clone(),
        execution: common.execution.clone(),
        connect_execution: with_execution,
        instance_name: common.instance_name.clone(),
        digest_function: Some(common.digest_function.into()),
        headers: common.headers.clone(),
        tls_ca_cert: common.tls_ca_cert.clone(),
    })
    .await?;
    remote.limits = FetchLimits {
        batch_max_blob: common.max_batch_blob_bytes,
        ..FetchLimits::default()
    };
    Ok(remote)
}

/// The `Action`/`Command` could not be read: report it, exit 2 on a mismatch.
fn action_problem(common: &Common, problem: &Problem) -> Output {
    let text = format!("{}\n", problem.describe());
    let json = json!({"error": report::problem_json(problem)});
    let mut output = Output::new(common, text, &json, usize::from(problem.is_mismatch()));
    if output.code == 0 {
        output.code = 1;
    }
    output
}

async fn cmd_show(args: &ShowArgs) -> Result<Output, Error> {
    let remote = connect(&args.common, false).await?;
    let spec = match fetch_action(&remote, args.action).await? {
        Ok(spec) => spec,
        Err(problem) => return Ok(action_problem(&args.common, &problem)),
    };
    let (plan, problems) = inspect_inputs(&remote, spec.input_root_digest).await?;
    let inputs = summarize_inputs(&plan);
    let mismatches = problems.iter().filter(|p| p.is_mismatch()).count();
    Ok(Output::new(
        &args.common,
        report::show_text(&spec, &inputs, &problems),
        &report::show_json(&spec, &inputs, &problems),
        mismatches,
    ))
}

async fn cmd_fetch(args: &FetchArgs) -> Result<Output, Error> {
    let remote = connect(&args.common, false).await?;
    let partial = PathBuf::from(format!("{}.partial", args.out.display()));
    if partial.exists() {
        std::fs::remove_file(&partial)?;
    }
    let result = remote
        .read_bytestream_to_file(args.digest, "blob", &partial)
        .await;
    match result {
        Ok(()) => {
            std::fs::rename(&partial, &args.out)?;
            let text = format!(
                "verified {} and wrote {} bytes to {}\n",
                args.digest,
                args.digest.size_bytes(),
                args.out.display()
            );
            let json = json!({
                "digest": args.digest.to_string(),
                "verified": true,
                "out": args.out.display().to_string(),
                "size_bytes": args.digest.size_bytes(),
            });
            Ok(Output::new(&args.common, text, &json, 0))
        }
        Err(problem @ Problem::Mismatch { .. }) => {
            let kept = PathBuf::from(format!("{}.mismatch", args.out.display()));
            std::fs::rename(&partial, &kept)?;
            let text = format!(
                "!! {}\n   the bytes served were written to {} for inspection; {} was not written\n",
                problem.describe(),
                kept.display(),
                args.out.display()
            );
            let json = json!({
                "digest": args.digest.to_string(),
                "verified": false,
                "problem": report::problem_json(&problem),
                "mismatch_out": kept.display().to_string(),
            });
            Ok(Output::new(&args.common, text, &json, 1))
        }
        Err(problem) => {
            drop(std::fs::remove_file(&partial));
            Err(make_err!(
                nativelink_error::Code::Unavailable,
                "{}",
                problem.describe()
            ))
        }
    }
}

async fn cmd_execute(args: &ExecuteArgs) -> Result<Output, Error> {
    let remote = connect(&args.common, true).await?;
    let runs = execute::execute_runs(&remote, args.action, args.runs).await;
    let summary = summarize(&runs);
    Ok(Output::new(
        &args.common,
        report::runs_text("NativeLink (execute)", &runs, &summary),
        &report::runs_json("execute", &args.action, &runs, &summary),
        summary.mismatches,
    ))
}

fn local_options(runs: usize, flags: &LocalFlags) -> LocalOptions {
    LocalOptions {
        runs,
        keep_dirs: flags.keep_dirs,
        workdir_base: flags.workdir_base.clone(),
        timeout: flags.timeout.map(Duration::from_secs),
        same_path: flags.same_path,
        extra_env: flags.extra_env.clone(),
    }
}

async fn local_runs(
    remote: &Remote,
    common: &Common,
    action: DigestInfo,
    options: &LocalOptions,
) -> Result<Result<Vec<manifest::Run>, Output>, Error> {
    let spec = match fetch_action(remote, action).await? {
        Ok(spec) => spec,
        Err(problem) => return Ok(Err(action_problem(common, &problem))),
    };
    let workspace = Workspace::create(options.workdir_base.as_deref())?;
    let runs = run_local(remote, &spec, options, &workspace).await;
    if options.keep_dirs {
        eprintln!("run directories kept under {}", workspace.base.display());
    } else {
        workspace.remove()?;
    }
    Ok(Ok(runs?))
}

async fn cmd_local(args: &LocalArgs) -> Result<Output, Error> {
    let remote = connect(&args.common, false).await?;
    let options = local_options(args.runs, &args.local);
    let runs = match local_runs(&remote, &args.common, args.action, &options).await? {
        Ok(runs) => runs,
        Err(output) => return Ok(output),
    };
    let summary = summarize(&runs);
    Ok(Output::new(
        &args.common,
        report::runs_text("oracle (local)", &runs, &summary),
        &report::runs_json("local", &args.action, &runs, &summary),
        summary.mismatches,
    ))
}

async fn cmd_compare(args: &CompareArgs) -> Result<Output, Error> {
    let remote = connect(&args.common, true).await?;
    let options = local_options(args.runs, &args.local);
    let oracle = match local_runs(&remote, &args.common, args.action, &options).await? {
        Ok(runs) => runs,
        Err(output) => return Ok(output),
    };
    let nativelink = execute::execute_runs(&remote, args.action, args.runs).await;
    let comparison = compare_runs(&oracle, &nativelink);
    Ok(Output::new(
        &args.common,
        report::compare_text(&comparison, &oracle, &nativelink),
        &report::compare_json(&args.action, &comparison, &oracle, &nativelink),
        comparison.oracle.mismatches + comparison.nativelink.mismatches,
    ))
}

async fn run(cli: Cli) -> Result<Output, Error> {
    match &cli.command {
        Commands::Show(args) => cmd_show(args).await,
        Commands::Fetch(args) => cmd_fetch(args).await,
        Commands::Execute(args) => cmd_execute(args).await,
        Commands::Local(args) => cmd_local(args).await,
        Commands::Compare(args) => cmd_compare(args).await,
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    #[expect(
        clippy::disallowed_methods,
        reason = "the top-level runtime of a command-line tool"
    )]
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build();
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("error: starting the runtime: {e}");
            return ExitCode::from(1);
        }
    };
    #[expect(
        clippy::disallowed_methods,
        reason = "the top-level runtime of a command-line tool"
    )]
    let result = runtime.block_on(run(cli));
    match result {
        Ok(output) => {
            let mut stdout = std::io::stdout().lock();
            if stdout.write_all(output.text.as_bytes()).is_err() || stdout.flush().is_err() {
                return ExitCode::from(1);
            }
            ExitCode::from(output.code)
        }
        Err(e) => {
            eprintln!("error: {}", e.message_string());
            ExitCode::from(1)
        }
    }
}
