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

//! The independent oracle: fetch an action's inputs from the CAS with plain
//! reads, write them with plain file writes, run the command with exactly the
//! `Command`'s environment, and hash what it produced.
//!
//! Nothing here uses the worker crate's code. The point is a second,
//! unrelated implementation to hold the worker against.

use core::time::Duration;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, Permissions};
use std::io::{Read, Write};
use std::os::unix::fs::{PermissionsExt, symlink};
use std::os::unix::process::ExitStatusExt;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use nativelink_error::{Code, Error, ResultExt, make_err, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, NodeProperties,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::manifest::{
    Entry, EntryKind, MAX_DEPTH, Manifest, Problem, Run, RunOutcome, check_name,
    check_relative_path, digest_field,
};
use crate::remote::{Remote, problem_for};

/// The `Action` and its `Command`, fetched and verified once.
#[derive(Clone, Debug)]
pub(crate) struct ActionSpec {
    pub(crate) action_digest: DigestInfo,
    pub(crate) action: Action,
    pub(crate) command: Command,
    pub(crate) command_digest: DigestInfo,
    pub(crate) input_root_digest: DigestInfo,
}

/// Fetches the `Action` and `Command`. A problem with either is returned as
/// such, so a digest mismatch on them is reported like any other.
pub(crate) async fn fetch_action(
    remote: &Remote,
    action_digest: DigestInfo,
) -> Result<Result<ActionSpec, Problem>, Error> {
    let action: Action = match remote.read_proto(action_digest, "Action").await {
        Ok(action) => action,
        Err(problem) => return Ok(Err(problem)),
    };
    let command_digest = digest_field(action.command_digest.as_ref(), "Action.command_digest")?;
    let input_root_digest = digest_field(
        action.input_root_digest.as_ref(),
        "Action.input_root_digest",
    )?;
    let command: Command = match remote.read_proto(command_digest, "Command").await {
        Ok(command) => command,
        Err(problem) => return Ok(Err(problem)),
    };
    Ok(Ok(ActionSpec {
        action_digest,
        action,
        command,
        command_digest,
        input_root_digest,
    }))
}

/// The input root as fetched: every directory by path, in breadth-first
/// order, and whatever could not be fetched or verified.
#[derive(Clone, Debug, Default)]
pub(crate) struct InputTree {
    pub(crate) dirs: Vec<(String, Directory)>,
    pub(crate) problems: Vec<Problem>,
}

fn join_rel(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// Fetches every `Directory` of the input root, level by level, verifying
/// each. A directory that fails is recorded and not descended into.
pub(crate) async fn fetch_input_tree(
    remote: &Remote,
    root: DigestInfo,
) -> Result<InputTree, Error> {
    let mut tree = InputTree::default();
    let mut fetched: HashMap<DigestInfo, Directory> = HashMap::new();
    let mut level: Vec<(String, DigestInfo)> = vec![(String::new(), root)];
    let mut depth = 0;
    while !level.is_empty() {
        if depth > MAX_DEPTH {
            return Err(make_input_err!("input root is deeper than {MAX_DEPTH}"));
        }
        let wanted: Vec<DigestInfo> = level
            .iter()
            .map(|(_, digest)| *digest)
            .filter(|digest| !fetched.contains_key(digest))
            .collect();
        let mut results = remote.read_many(wanted).await;
        let mut failed: HashSet<DigestInfo> = HashSet::new();
        let mut next = Vec::new();
        for (path, digest) in level {
            if let Some(result) = results.remove(&digest) {
                let what = format!(
                    "input directory '{}'",
                    if path.is_empty() { "." } else { &path }
                );
                match result {
                    Ok(data) => match prost::Message::decode(data) {
                        Ok(directory) => {
                            fetched.insert(digest, directory);
                        }
                        Err(e) => {
                            tree.problems.push(Problem::ReadFailed {
                                what,
                                digest,
                                message: format!("not a Directory proto: {e}"),
                            });
                            failed.insert(digest);
                        }
                    },
                    Err(problem) => {
                        tree.problems.push(problem_for(problem, &what));
                        failed.insert(digest);
                    }
                }
            }
            if failed.contains(&digest) {
                continue;
            }
            let Some(directory) = fetched.get(&digest) else {
                continue;
            };
            for node in &directory.directories {
                check_name(&node.name)?;
                let child_path = join_rel(&path, &node.name);
                let child = digest_field(node.digest.as_ref(), &child_path)?;
                next.push((child_path, child));
            }
            tree.dirs.push((path, directory.clone()));
        }
        level = next;
        depth += 1;
    }
    Ok(tree)
}

/// One input file to write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedFile {
    pub(crate) path: String,
    pub(crate) digest: DigestInfo,
    pub(crate) executable: bool,
    pub(crate) unix_mode: Option<u32>,
    pub(crate) mtime: Option<SystemTime>,
}

/// Everything the input root holds, validated.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Plan {
    /// Every directory below the root, parents first.
    pub(crate) dirs: Vec<String>,
    pub(crate) files: Vec<PlannedFile>,
    /// `(path, target)`.
    pub(crate) symlinks: Vec<(String, String)>,
}

fn mtime_of(properties: Option<&NodeProperties>) -> Option<SystemTime> {
    let mtime = properties?.mtime.as_ref()?;
    let seconds = u64::try_from(mtime.seconds).ok()?;
    let nanos = u32::try_from(mtime.nanos).ok()?;
    UNIX_EPOCH.checked_add(Duration::new(seconds, nanos))
}

/// Validates every name and lays out what to create. Names that are not a
/// single path component, and a name used twice in one directory, are
/// refused: they would write outside the input root or over another input.
pub(crate) fn plan_inputs(tree: &InputTree) -> Result<Plan, Error> {
    let mut plan = Plan::default();
    for (path, directory) in &tree.dirs {
        let mut seen = HashSet::new();
        let names = directory
            .files
            .iter()
            .map(|f| &f.name)
            .chain(directory.directories.iter().map(|d| &d.name))
            .chain(directory.symlinks.iter().map(|s| &s.name));
        for name in names {
            check_name(name).err_tip(|| format!("in input directory '{path}'"))?;
            if !seen.insert(name.as_str()) {
                return Err(make_input_err!(
                    "input directory '{path}' names '{name}' more than once"
                ));
            }
        }
        if !path.is_empty() {
            plan.dirs.push(path.clone());
        }
        for file in &directory.files {
            let file_path = join_rel(path, &file.name);
            let digest = digest_field(file.digest.as_ref(), &file_path)?;
            plan.files.push(PlannedFile {
                path: file_path,
                digest,
                executable: file.is_executable,
                unix_mode: file.node_properties.as_ref().and_then(|p| p.unix_mode),
                mtime: mtime_of(file.node_properties.as_ref()),
            });
        }
        for link in &directory.symlinks {
            plan.symlinks
                .push((join_rel(path, &link.name), link.target.clone()));
        }
    }
    Ok(plan)
}

/// Counts for `show`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct InputSummary {
    pub(crate) files: usize,
    pub(crate) executables: usize,
    pub(crate) directories: usize,
    pub(crate) symlinks: usize,
    pub(crate) total_bytes: u64,
    pub(crate) unique_blobs: usize,
    pub(crate) unique_bytes: u64,
    /// The largest files, `(path, size)`, largest first.
    pub(crate) largest: Vec<(String, u64)>,
}

pub(crate) fn summarize_inputs(plan: &Plan) -> InputSummary {
    let mut unique: HashMap<DigestInfo, u64> = HashMap::new();
    for file in &plan.files {
        unique.insert(file.digest, file.digest.size_bytes());
    }
    let mut largest: Vec<(String, u64)> = plan
        .files
        .iter()
        .map(|f| (f.path.clone(), f.digest.size_bytes()))
        .collect();
    largest.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    largest.truncate(5);
    InputSummary {
        files: plan.files.len(),
        executables: plan.files.iter().filter(|f| f.executable).count(),
        directories: plan.dirs.len(),
        symlinks: plan.symlinks.len(),
        total_bytes: plan.files.iter().map(|f| f.digest.size_bytes()).sum(),
        unique_blobs: unique.len(),
        unique_bytes: unique.values().sum(),
        largest,
    }
}

/// The mode an input file gets: its `unix_mode` with the executable bits
/// added when it is executable, or `0755`/`0644`.
pub(crate) const fn input_mode(executable: bool, unix_mode: Option<u32>) -> u32 {
    match (unix_mode, executable) {
        (Some(mode), true) => mode | 0o111,
        (Some(mode), false) => mode,
        (None, true) => 0o755,
        (None, false) => 0o644,
    }
}

fn io_err(e: &std::io::Error, what: impl core::fmt::Display) -> Error {
    make_err!(Code::Internal, "{what}: {e}")
}

fn finish_file(path: &Path, file: &PlannedFile) -> Result<(), Error> {
    if let Some(mtime) = file.mtime {
        let handle = File::options()
            .write(true)
            .open(path)
            .map_err(|e| io_err(&e, format!("opening {}", path.display())))?;
        handle
            .set_modified(mtime)
            .map_err(|e| io_err(&e, format!("setting mtime of {}", path.display())))?;
    }
    fs::set_permissions(
        path,
        Permissions::from_mode(input_mode(file.executable, file.unix_mode)),
    )
    .map_err(|e| io_err(&e, format!("setting mode of {}", path.display())))
}

fn write_new(path: &Path, data: &[u8]) -> Result<(), Error> {
    let mut file = File::options()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|e| io_err(&e, format!("creating {}", path.display())))?;
    file.write_all(data)
        .map_err(|e| io_err(&e, format!("writing {}", path.display())))
}

/// Writes the input root under `root` (an empty directory): directories,
/// then files fetched from the CAS and verified, then symlinks, so no write
/// goes through a symlink. Returns the blobs that failed.
pub(crate) async fn materialize(
    remote: &Remote,
    plan: &Plan,
    root: &Path,
) -> Result<Vec<Problem>, Error> {
    for dir in &plan.dirs {
        let path = root.join(dir);
        fs::create_dir_all(&path)
            .map_err(|e| io_err(&e, format!("creating {}", path.display())))?;
    }
    let mut by_digest: BTreeMap<DigestInfo, Vec<&PlannedFile>> = BTreeMap::new();
    for file in &plan.files {
        by_digest.entry(file.digest).or_default().push(file);
    }
    let mut problems = Vec::new();

    let (small, large): (Vec<DigestInfo>, Vec<DigestInfo>) =
        by_digest.keys().copied().partition(|d| remote.batchable(d));

    // Small blobs in chunks, so memory stays bounded by a chunk.
    for chunk in small.chunks(4096) {
        let mut results = remote.read_many(chunk.to_vec()).await;
        for digest in chunk {
            let files = &by_digest[digest];
            match results.remove(digest) {
                Some(Ok(data)) => {
                    for file in files {
                        let path = root.join(&file.path);
                        write_new(&path, &data)?;
                        finish_file(&path, file)?;
                    }
                }
                Some(Err(problem)) => {
                    problems.push(problem_for(
                        problem,
                        &format!("input file '{}'", files[0].path),
                    ));
                }
                None => problems.push(Problem::ReadFailed {
                    what: format!("input file '{}'", files[0].path),
                    digest: *digest,
                    message: "no result".to_string(),
                }),
            }
        }
    }

    // Large blobs streamed to the first path, then copied (not linked).
    for digest in large {
        let files = &by_digest[&digest];
        let first = root.join(&files[0].path);
        let what = format!("input file '{}'", files[0].path);
        match remote.read_bytestream_to_file(digest, &what, &first).await {
            Ok(()) => {
                for file in &files[1..] {
                    let path = root.join(&file.path);
                    fs::copy(&first, &path)
                        .map_err(|e| io_err(&e, format!("copying to {}", path.display())))?;
                }
                for file in files {
                    finish_file(&root.join(&file.path), file)?;
                }
            }
            Err(problem) => problems.push(problem),
        }
    }

    for (path, target) in &plan.symlinks {
        let full = root.join(path);
        symlink(target, &full)
            .map_err(|e| io_err(&e, format!("creating symlink {}", full.display())))?;
    }
    Ok(problems)
}

/// The paths the command declares as outputs: `output_paths`, or the
/// deprecated `output_files` then `output_directories` when it has none.
pub(crate) fn declared_outputs(command: &Command) -> Vec<String> {
    if command.output_paths.is_empty() {
        command
            .output_files
            .iter()
            .chain(&command.output_directories)
            .cloned()
            .collect()
    } else {
        command.output_paths.clone()
    }
}

/// Creates the parent directories of an output path under `workdir`,
/// refusing to go through a symlink that leads outside `exec_root`.
fn create_output_parents(exec_root: &Path, workdir: &Path, output: &str) -> Result<(), Error> {
    let Some(parent) = Path::new(output).parent() else {
        return Ok(());
    };
    let mut current = workdir.to_path_buf();
    for component in parent.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.is_dir() => {}
            Ok(meta) if meta.file_type().is_symlink() => {
                let resolved = fs::canonicalize(&current)
                    .map_err(|e| io_err(&e, format!("resolving {}", current.display())))?;
                if !resolved.starts_with(exec_root) || !resolved.is_dir() {
                    return Err(make_input_err!(
                        "parent of output '{output}' goes through symlink {} to {}, outside the \
                         input root; refusing to create directories there",
                        current.display(),
                        resolved.display()
                    ));
                }
            }
            Ok(_) => {
                return Err(make_input_err!(
                    "parent of output '{output}' is a file: {}",
                    current.display()
                ));
            }
            Err(_) => fs::create_dir(&current)
                .map_err(|e| io_err(&e, format!("creating {}", current.display())))?,
        }
    }
    Ok(())
}

/// The environment the command runs with: `extra` first, then the
/// `Command`'s variables, which win. Nothing from this process.
pub(crate) fn build_env(command: &Command, extra: &[(String, String)]) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for (name, value) in extra {
        env.insert(name.clone(), value.clone());
    }
    for variable in &command.environment_variables {
        env.insert(variable.name.clone(), variable.value.clone());
    }
    env
}

/// The program to start. A relative path with a separator is resolved
/// against the working directory; a bare name is looked up in the `PATH` of
/// the command's environment; an absolute path is used as is.
pub(crate) fn resolve_program(argv0: &str, workdir: &Path) -> PathBuf {
    let path = Path::new(argv0);
    if argv0.contains('/') && !path.is_absolute() {
        workdir.join(path)
    } else {
        path.to_path_buf()
    }
}

/// What the command did.
#[derive(Clone, Debug)]
pub(crate) struct CommandResult {
    pub(crate) exit_code: i32,
    pub(crate) signal: Option<i32>,
    pub(crate) timed_out: bool,
    pub(crate) stdout: Option<DigestInfo>,
    pub(crate) stderr: Option<DigestInfo>,
}

async fn drain<R: AsyncRead + Unpin>(
    reader: Option<R>,
    path: &Path,
    func: DigestHasherFunc,
) -> Result<DigestInfo, Error> {
    let mut file =
        File::create(path).map_err(|e| io_err(&e, format!("creating {}", path.display())))?;
    let mut hasher = func.hasher();
    if let Some(mut reader) = reader {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = reader
                .read(&mut buf)
                .await
                .map_err(|e| io_err(&e, "reading the command's output"))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            file.write_all(&buf[..n])
                .map_err(|e| io_err(&e, format!("writing {}", path.display())))?;
        }
    }
    Ok(hasher.finalize_digest())
}

/// Runs the command in `workdir` with exactly `env`, capturing stdout and
/// stderr to files. On timeout the process is killed.
pub(crate) async fn run_command(
    command: &Command,
    workdir: &Path,
    env: &BTreeMap<String, String>,
    timeout: Duration,
    stdout_path: &Path,
    stderr_path: &Path,
    func: DigestHasherFunc,
) -> Result<CommandResult, Error> {
    let argv0 = command
        .arguments
        .first()
        .ok_or_else(|| make_input_err!("the Command has no arguments"))?;
    let mut builder = tokio::process::Command::new(resolve_program(argv0, workdir));
    builder
        .arg0(argv0)
        .args(&command.arguments[1..])
        .current_dir(workdir)
        .env_clear()
        .envs(env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = builder
        .spawn()
        .map_err(|e| io_err(&e, format!("starting '{argv0}' in {}", workdir.display())))?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let waited = tokio::time::timeout(timeout, async {
        tokio::join!(
            child.wait(),
            drain(stdout, stdout_path, func),
            drain(stderr, stderr_path, func)
        )
    })
    .await;
    if let Ok((status, out, err)) = waited {
        let status = status.map_err(|e| io_err(&e, "waiting for the command"))?;
        Ok(CommandResult {
            exit_code: status.code().unwrap_or(-1),
            signal: status.signal(),
            timed_out: false,
            stdout: Some(out?),
            stderr: Some(err?),
        })
    } else {
        // Only the direct child is killed; processes it started and
        // detached may outlive it.
        drop(child.kill().await);
        Ok(CommandResult {
            exit_code: -1,
            signal: None,
            timed_out: true,
            stdout: None,
            stderr: None,
        })
    }
}

fn hash_file(path: &Path, func: DigestHasherFunc) -> Result<DigestInfo, Error> {
    let mut file =
        File::open(path).map_err(|e| io_err(&e, format!("opening {}", path.display())))?;
    let mut hasher = func.hasher();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| io_err(&e, format!("reading {}", path.display())))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize_digest())
}

/// A symlink target as the worker reports it: an absolute target inside the
/// execution root is made relative to it.
fn reported_target(target: &Path, exec_root: &Path) -> String {
    target.strip_prefix(exec_root).map_or_else(
        |_| target.to_string_lossy().into_owned(),
        |rel| rel.to_string_lossy().into_owned(),
    )
}

fn walk_output_dir(
    dir: &Path,
    output: &str,
    rel: &str,
    exec_root: &Path,
    func: DigestHasherFunc,
    depth: usize,
    entries: &mut Vec<Entry>,
) -> Result<(), Error> {
    if depth > MAX_DEPTH {
        return Err(make_input_err!(
            "output directory '{output}' is deeper than {MAX_DEPTH}"
        ));
    }
    let mut children: Vec<_> = fs::read_dir(dir)
        .map_err(|e| io_err(&e, format!("listing {}", dir.display())))?
        .collect::<Result<_, _>>()
        .map_err(|e| io_err(&e, format!("listing {}", dir.display())))?;
    children.sort_by_key(fs::DirEntry::file_name);
    for child in children {
        let name = child.file_name().to_string_lossy().into_owned();
        let child_rel = join_rel(rel, &name);
        let path = child.path();
        let file_type = child
            .file_type()
            .map_err(|e| io_err(&e, format!("inspecting {}", path.display())))?;
        if file_type.is_dir() {
            walk_output_dir(
                &path,
                output,
                &child_rel,
                exec_root,
                func,
                depth + 1,
                entries,
            )?;
        } else if file_type.is_file() {
            let meta = fs::metadata(&path)
                .map_err(|e| io_err(&e, format!("inspecting {}", path.display())))?;
            entries.push(Entry::file(
                EntryKind::DirectoryFile,
                format!("{output}/{child_rel}"),
                hash_file(&path, func)?,
                meta.permissions().mode() & 0o111 != 0,
            ));
        } else if file_type.is_symlink() {
            let target = fs::read_link(&path)
                .map_err(|e| io_err(&e, format!("reading {}", path.display())))?;
            entries.push(Entry::symlink(
                format!("{output}/{child_rel}"),
                reported_target(&target, exec_root),
            ));
        }
    }
    Ok(())
}

/// Hashes the declared outputs the way REAPI reports them: a file as a file,
/// a directory as its member files and symlinks, a symlink with a relative
/// target as a symlink, and a symlink with an absolute target as what it
/// resolves to. Returns the manifest and the declared outputs that do not
/// exist.
pub(crate) fn hash_outputs(
    command: &Command,
    workdir: &Path,
    exec_root: &Path,
    func: DigestHasherFunc,
) -> Result<(Manifest, Vec<String>), Error> {
    let mut entries = Vec::new();
    let mut missing = Vec::new();
    for output in declared_outputs(command) {
        let full = if output.is_empty() {
            workdir.to_path_buf()
        } else {
            workdir.join(&output)
        };
        let Ok(meta) = fs::symlink_metadata(&full) else {
            missing.push(output);
            continue;
        };
        let meta = if meta.file_type().is_symlink() {
            let target = fs::read_link(&full)
                .map_err(|e| io_err(&e, format!("reading {}", full.display())))?;
            match fs::metadata(&full) {
                Ok(resolved) if target.is_absolute() => resolved,
                _ => {
                    entries.push(Entry::symlink(
                        output.clone(),
                        reported_target(&target, exec_root),
                    ));
                    continue;
                }
            }
        } else {
            meta
        };
        if meta.is_dir() {
            walk_output_dir(&full, &output, "", exec_root, func, 0, &mut entries)?;
        } else if meta.is_file() {
            entries.push(Entry::file(
                EntryKind::File,
                output.clone(),
                hash_file(&full, func)?,
                meta.permissions().mode() & 0o111 != 0,
            ));
        }
    }
    Ok((Manifest::new(entries), missing))
}

/// Removes a directory tree even if the command left parts of it read-only.
pub(crate) fn force_remove_dir(path: &Path) -> Result<(), Error> {
    if !path.exists() && fs::symlink_metadata(path).is_err() {
        return Ok(());
    }
    if fs::remove_dir_all(path).is_ok() {
        return Ok(());
    }
    make_writable(path, 0);
    fs::remove_dir_all(path).map_err(|e| io_err(&e, format!("removing {}", path.display())))
}

fn make_writable(path: &Path, depth: usize) {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return;
    };
    if !meta.is_dir() || depth > MAX_DEPTH {
        return;
    }
    drop(fs::set_permissions(path, Permissions::from_mode(0o755)));
    if let Ok(children) = fs::read_dir(path) {
        for child in children.flatten() {
            make_writable(&child.path(), depth + 1);
        }
    }
}

/// How `local` runs.
#[derive(Clone, Debug, Default)]
pub(crate) struct LocalOptions {
    pub(crate) runs: usize,
    pub(crate) keep_dirs: bool,
    pub(crate) workdir_base: Option<PathBuf>,
    pub(crate) timeout: Option<Duration>,
    /// Every run at one path instead of a directory per run.
    pub(crate) same_path: bool,
    pub(crate) extra_env: Vec<(String, String)>,
}

/// Where a `local` invocation keeps its files: a directory of its own,
/// created under `--workdir-base` or the system temporary directory.
#[derive(Clone, Debug)]
pub(crate) struct Workspace {
    pub(crate) base: PathBuf,
}

impl Workspace {
    pub(crate) fn create(parent: Option<&Path>) -> Result<Self, Error> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let name = format!("nativelink-replay-{}-{nanos}", std::process::id());
        let base = parent
            .map_or_else(std::env::temp_dir, Path::to_path_buf)
            .join(name);
        fs::create_dir_all(&base)
            .map_err(|e| io_err(&e, format!("creating {}", base.display())))?;
        let base = fs::canonicalize(&base)
            .map_err(|e| io_err(&e, format!("resolving {}", base.display())))?;
        Ok(Self { base })
    }

    /// Removes everything this invocation wrote.
    pub(crate) fn remove(&self) -> Result<(), Error> {
        force_remove_dir(&self.base)
    }
}

fn hostname() -> String {
    fs::read_to_string("/proc/sys/kernel/hostname")
        .map(|s| s.trim().to_string())
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "localhost".to_string())
}

/// The timeout a run gets: `--timeout`, else the action's, else ten minutes.
pub(crate) fn effective_timeout(action: &Action, requested: Option<Duration>) -> Duration {
    requested.unwrap_or_else(|| {
        action
            .timeout
            .as_ref()
            .and_then(|t| {
                let seconds = u64::try_from(t.seconds).ok()?;
                let nanos = u32::try_from(t.nanos).ok()?;
                Some(Duration::new(seconds, nanos))
            })
            .filter(|d| !d.is_zero())
            .unwrap_or(Duration::from_mins(10))
    })
}

/// Runs the action locally `options.runs` times, each in a fresh directory.
pub(crate) async fn run_local(
    remote: &Remote,
    spec: &ActionSpec,
    options: &LocalOptions,
    workspace: &Workspace,
) -> Result<Vec<Run>, Error> {
    let command = &spec.command;
    check_relative_path(&command.working_directory).err_tip(|| "Command.working_directory")?;
    let outputs = declared_outputs(command);
    for output in &outputs {
        check_relative_path(output).err_tip(|| "Command output path")?;
    }
    let env = build_env(command, &options.extra_env);
    let timeout = effective_timeout(&spec.action, options.timeout);
    let host = hostname();
    let mut runs = Vec::with_capacity(options.runs);

    for index in 1..=options.runs {
        eprintln!("local run {index}/{}: fetching inputs", options.runs);
        let started = Instant::now();
        let kept_root = workspace.base.join(format!("run-{index}"));
        let exec_root = if options.same_path {
            workspace.base.join("exec")
        } else {
            kept_root.clone()
        };
        force_remove_dir(&exec_root)?;
        fs::create_dir(&exec_root)
            .map_err(|e| io_err(&e, format!("creating {}", exec_root.display())))?;
        let mut run = Run {
            index,
            worker: format!("local:{host}"),
            ..Default::default()
        };

        let tree = fetch_input_tree(remote, spec.input_root_digest).await?;
        run.problems.extend(tree.problems.iter().cloned());
        if run.problems.is_empty() {
            let plan = plan_inputs(&tree)?;
            run.problems
                .extend(materialize(remote, &plan, &exec_root).await?);
        }

        if run.problems.is_empty() {
            let workdir = if command.working_directory.is_empty() {
                exec_root.clone()
            } else {
                exec_root.join(&command.working_directory)
            };
            // The working directory may go through an input symlink; it must
            // still land inside the input root, or the output directories
            // created below would land outside it.
            let inside = fs::canonicalize(&workdir)
                .is_ok_and(|resolved| resolved.is_dir() && resolved.starts_with(&exec_root));
            run.error = if inside {
                outputs.iter().find_map(|output| {
                    create_output_parents(&exec_root, &workdir, output)
                        .err()
                        .map(|e| e.message_string())
                })
            } else {
                Some(format!(
                    "working directory '{}' is not a directory inside the input root",
                    command.working_directory
                ))
            };
            if run.error.is_none() {
                eprintln!("local run {index}/{}: running", options.runs);
                let stdout_path = workspace.base.join(format!("run-{index}.stdout"));
                let stderr_path = workspace.base.join(format!("run-{index}.stderr"));
                match run_command(
                    command,
                    &workdir,
                    &env,
                    timeout,
                    &stdout_path,
                    &stderr_path,
                    remote.func,
                )
                .await
                {
                    Ok(result) if result.timed_out => {
                        run.error = Some(format!(
                            "timed out after {}s and was killed",
                            timeout.as_secs()
                        ));
                    }
                    Ok(result) => {
                        if let Some(signal) = result.signal {
                            run.notes.push(format!("killed by signal {signal}"));
                        }
                        let (manifest, missing) =
                            hash_outputs(command, &workdir, &exec_root, remote.func)?;
                        run.missing_outputs = missing;
                        run.outcome = Some(RunOutcome {
                            manifest,
                            exit_code: result.exit_code,
                            stdout: result.stdout,
                            stderr: result.stderr,
                        });
                    }
                    Err(e) => run.error = Some(e.message_string()),
                }
            }
        } else {
            run.error = Some(format!(
                "{} input blob(s) could not be fetched or verified; the command was not run",
                run.problems.len()
            ));
        }

        if options.keep_dirs {
            if exec_root != kept_root {
                fs::rename(&exec_root, &kept_root).map_err(|e| {
                    io_err(
                        &e,
                        format!("moving {} to {}", exec_root.display(), kept_root.display()),
                    )
                })?;
            }
            run.notes.push(format!("kept in {}", kept_root.display()));
        } else {
            force_remove_dir(&exec_root)?;
        }
        run.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        runs.push(run);
    }
    Ok(runs)
}

/// The input root of an action, fetched and planned, for `show`.
pub(crate) async fn inspect_inputs(
    remote: &Remote,
    root: DigestInfo,
) -> Result<(Plan, Vec<Problem>), Error> {
    let tree = fetch_input_tree(remote, root).await?;
    let plan = plan_inputs(&tree)?;
    Ok((plan, tree.problems))
}

#[cfg(test)]
mod tests {
    use nativelink_proto::build::bazel::remote::execution::v2::command::EnvironmentVariable;
    use nativelink_proto::build::bazel::remote::execution::v2::{
        DirectoryNode, FileNode, SymlinkNode,
    };

    use super::*;

    fn var(name: &str, value: &str) -> EnvironmentVariable {
        EnvironmentVariable {
            name: name.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn env_is_exactly_the_command_env_over_the_extras() {
        let command = Command {
            environment_variables: vec![var("PATH", "/cmd/bin"), var("LANG", "C")],
            ..Default::default()
        };
        let env = build_env(
            &command,
            &[
                ("PATH".to_string(), "/extra/bin".to_string()),
                ("TMPDIR".to_string(), "/t".to_string()),
            ],
        );
        let got: Vec<(&str, &str)> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        assert_eq!(
            got,
            vec![("LANG", "C"), ("PATH", "/cmd/bin"), ("TMPDIR", "/t")]
        );
        // Nothing leaks from this process.
        assert!(build_env(&Command::default(), &[]).is_empty());
    }

    #[test]
    fn resolves_the_program_like_reapi_v2_3() {
        let workdir = Path::new("/root/work");
        assert_eq!(resolve_program("sh", workdir), PathBuf::from("sh"));
        assert_eq!(
            resolve_program("/bin/sh", workdir),
            PathBuf::from("/bin/sh")
        );
        assert_eq!(
            resolve_program("bazel-out/bin/tool", workdir),
            PathBuf::from("/root/work/bazel-out/bin/tool")
        );
        assert_eq!(
            resolve_program("./tool", workdir),
            PathBuf::from("/root/work/./tool")
        );
    }

    #[test]
    fn declared_outputs_prefer_output_paths() {
        let legacy = Command {
            output_files: vec!["a".to_string()],
            output_directories: vec!["d".to_string()],
            ..Default::default()
        };
        assert_eq!(declared_outputs(&legacy), vec!["a", "d"]);
        let modern = Command {
            output_paths: vec!["p".to_string()],
            ..legacy
        };
        assert_eq!(declared_outputs(&modern), vec!["p"]);
    }

    #[test]
    fn input_modes_follow_the_node() {
        assert_eq!(input_mode(false, None), 0o644);
        assert_eq!(input_mode(true, None), 0o755);
        assert_eq!(input_mode(false, Some(0o600)), 0o600);
        assert_eq!(input_mode(true, Some(0o600)), 0o711);
    }

    fn file_node(name: &str, size: u64) -> FileNode {
        FileNode {
            name: name.to_string(),
            digest: Some(DigestInfo::new([1; 32], size).into()),
            is_executable: false,
            node_properties: None,
        }
    }

    #[test]
    fn plan_refuses_traversal_and_duplicates() {
        let tree = |dir: Directory| InputTree {
            dirs: vec![(String::new(), dir)],
            problems: vec![],
        };
        let traversal = Directory {
            files: vec![file_node("../x", 1)],
            ..Default::default()
        };
        assert!(plan_inputs(&tree(traversal)).is_err());
        let duplicate = Directory {
            files: vec![file_node("x", 1)],
            symlinks: vec![SymlinkNode {
                name: "x".to_string(),
                target: "/etc".to_string(),
                node_properties: None,
            }],
            ..Default::default()
        };
        assert!(plan_inputs(&tree(duplicate)).is_err());
    }

    #[test]
    fn plan_and_summary_cover_files_dirs_and_symlinks() {
        let sub = Directory {
            files: vec![file_node("b", 10), file_node("c", 10)],
            ..Default::default()
        };
        let root = Directory {
            files: vec![FileNode {
                is_executable: true,
                ..file_node("a", 5)
            }],
            directories: vec![DirectoryNode {
                name: "sub".to_string(),
                digest: Some(DigestInfo::new([2; 32], 1).into()),
            }],
            symlinks: vec![SymlinkNode {
                name: "l".to_string(),
                target: "sub/b".to_string(),
                node_properties: None,
            }],
            node_properties: None,
        };
        let tree = InputTree {
            dirs: vec![(String::new(), root), ("sub".to_string(), sub)],
            problems: vec![],
        };
        let plan = plan_inputs(&tree).unwrap();
        assert_eq!(plan.dirs, vec!["sub"]);
        let paths: Vec<&str> = plan.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["a", "sub/b", "sub/c"]);
        assert_eq!(plan.symlinks, vec![("l".to_string(), "sub/b".to_string())]);
        let summary = summarize_inputs(&plan);
        assert_eq!(summary.files, 3);
        assert_eq!(summary.executables, 1);
        assert_eq!(summary.directories, 1);
        assert_eq!(summary.symlinks, 1);
        assert_eq!(summary.total_bytes, 25);
        // b and c share a digest; a has its own.
        assert_eq!(summary.unique_blobs, 2);
        assert_eq!(summary.unique_bytes, 15);
        assert_eq!(summary.largest[0], ("sub/b".to_string(), 10));
    }

    #[test]
    fn effective_timeout_prefers_the_flag_then_the_action() {
        let mut action = Action::default();
        assert_eq!(effective_timeout(&action, None), Duration::from_mins(10));
        let mut timeout = Action::default().timeout.unwrap_or_default();
        timeout.seconds = 30;
        action.timeout = Some(timeout);
        assert_eq!(effective_timeout(&action, None), Duration::from_secs(30));
        assert_eq!(
            effective_timeout(&action, Some(Duration::from_secs(5))),
            Duration::from_secs(5)
        );
    }
}
