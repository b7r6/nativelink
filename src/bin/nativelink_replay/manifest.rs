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

//! The pure parts: digest parsing, output manifests and their fingerprint,
//! `Tree` expansion, and the comparison of runs.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use nativelink_error::{Error, ResultExt, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, Tree,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};
use prost::Message;

/// Deepest directory nesting accepted in an input root, a `Tree` or an output
/// directory on disk.
pub(crate) const MAX_DEPTH: usize = 512;

/// Parses `<hash>-<size>` (or `<hash>/<size>`). The hash is 64 hex
/// characters, which both SHA-256 and BLAKE3 produce.
pub(crate) fn parse_digest(input: &str) -> Result<DigestInfo, Error> {
    let input = input.trim();
    let (hash, size) = input
        .rsplit_once('-')
        .or_else(|| input.rsplit_once('/'))
        .ok_or_else(|| make_input_err!("digest '{input}' is not <hash>-<size>"))?;
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(make_input_err!(
            "digest '{input}': the hash must be 64 hex characters"
        ));
    }
    let size: u64 = size
        .parse()
        .map_err(|_| make_input_err!("digest '{input}': size '{size}' is not a number"))?;
    DigestInfo::try_new(&hash.to_ascii_lowercase(), size)
}

/// The digest of `data` under `func`.
pub(crate) fn digest_of(func: DigestHasherFunc, data: &[u8]) -> DigestInfo {
    let mut hasher = func.hasher();
    hasher.update(data);
    hasher.finalize_digest()
}

/// Converts a proto digest field, naming `what` when it is missing or bad.
pub(crate) fn digest_field(digest: Option<&Digest>, what: &str) -> Result<DigestInfo, Error> {
    let digest = digest.ok_or_else(|| make_input_err!("{what} has no digest"))?;
    DigestInfo::try_from(digest).err_tip(|| format!("bad digest on {what}"))
}

/// The first 12 hex characters of a digest plus its size, for tables.
pub(crate) fn short_digest(digest: &DigestInfo) -> String {
    let full = digest.to_string();
    let hash = digest.packed_hash().to_string();
    if hash.len() > 12 {
        format!("{}..-{}", &hash[..12], digest.size_bytes())
    } else {
        full
    }
}

/// Checks one path component from a `Directory` or a `Tree`.
pub(crate) fn check_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\0') {
        return Err(make_input_err!(
            "refusing node name '{}': not a single path component",
            name.escape_default()
        ));
    }
    Ok(())
}

/// Checks a path from a `Command` (working directory, output path): relative,
/// `/`-separated, with no `.`, `..` or empty components. `""` is allowed.
pub(crate) fn check_relative_path(path: &str) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    if path.starts_with('/') {
        return Err(make_input_err!("refusing absolute path '{path}'"));
    }
    for component in path.split('/') {
        check_name(component).err_tip(|| format!("in path '{path}'"))?;
    }
    Ok(())
}

/// What an output manifest entry is. The names match the `kind` field of the
/// worker's `output_observed` provenance event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum EntryKind {
    /// A declared output file.
    File,
    /// A file inside a declared output directory.
    DirectoryFile,
    /// A symlink kept as a symlink.
    Symlink,
}

impl EntryKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::DirectoryFile => "directory_file",
            Self::Symlink => "symlink",
        }
    }
}

/// One output: a file with a digest, or a symlink with a target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Entry {
    pub(crate) kind: EntryKind,
    /// Relative to the working directory, as the `ActionResult` names it. A
    /// file inside an output directory is `<output dir>/<path inside>`.
    pub(crate) path: String,
    /// `None` for a symlink.
    pub(crate) digest: Option<DigestInfo>,
    pub(crate) executable: bool,
    /// Empty unless a symlink.
    pub(crate) target: String,
}

impl Entry {
    pub(crate) fn file(
        kind: EntryKind,
        path: impl Into<String>,
        digest: DigestInfo,
        executable: bool,
    ) -> Self {
        Self {
            kind,
            path: path.into(),
            digest: Some(digest),
            executable,
            target: String::new(),
        }
    }

    pub(crate) fn symlink(path: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            kind: EntryKind::Symlink,
            path: path.into(),
            digest: None,
            executable: false,
            target: target.into(),
        }
    }

    /// What two runs compare for this path: the digest and the executable
    /// bit, or the symlink target.
    pub(crate) fn value(&self) -> String {
        match &self.digest {
            Some(digest) if self.executable => format!("{digest} (executable)"),
            Some(digest) => digest.to_string(),
            None => format!("symlink -> {}", self.target),
        }
    }
}

/// An attempt's outputs, in canonical order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Manifest {
    entries: Vec<Entry>,
}

impl Manifest {
    /// Sorts the entries the way the worker's provenance code does: by path,
    /// then kind, then digest, target and executable bit.
    pub(crate) fn new(mut entries: Vec<Entry>) -> Self {
        entries.sort_unstable_by(|a, b| {
            a.path
                .cmp(&b.path)
                .then_with(|| a.kind.as_str().cmp(b.kind.as_str()))
                .then_with(|| a.digest.cmp(&b.digest))
                .then_with(|| a.target.cmp(&b.target))
                .then_with(|| a.executable.cmp(&b.executable))
        });
        Self { entries }
    }

    pub(crate) fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Sum of the sizes of the file entries.
    pub(crate) fn total_bytes(&self) -> u64 {
        self.entries
            .iter()
            .filter_map(|entry| entry.digest.map(|digest| digest.size_bytes()))
            .sum()
    }

    /// The lowercase hex SHA-256 of the canonical manifest, one line per entry:
    ///
    /// ```text
    /// {kind}\t{path}\t{digest, or the symlink target}\t{x if executable, else -}\n
    /// ```
    ///
    /// The same definition as `manifest_fingerprint` in the worker's
    /// `attempt_finished` provenance event, so a fingerprint printed here can
    /// be looked up in those logs.
    pub(crate) fn fingerprint(&self) -> String {
        let mut hasher = DigestHasherFunc::Sha256.hasher();
        for entry in &self.entries {
            let digest = entry.digest.map(|d| d.to_string()).unwrap_or_default();
            let line = format!(
                "{}\t{}\t{}{}\t{}\n",
                entry.kind.as_str(),
                entry.path,
                digest,
                entry.target,
                if entry.executable { "x" } else { "-" },
            );
            hasher.update(line.as_bytes());
        }
        hasher.finalize_digest().packed_hash().to_string()
    }

    /// Every value each path has, for comparing runs path by path.
    pub(crate) fn values_by_path(&self) -> BTreeMap<&str, BTreeSet<String>> {
        let mut map: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
        for entry in &self.entries {
            map.entry(entry.path.as_str())
                .or_default()
                .insert(entry.value());
        }
        map
    }
}

/// `{dir}/{rel}`, the way the worker names a member of an output directory.
/// An output directory of `""` gives `/{rel}`, as the worker does.
pub(crate) fn member_path(dir: &str, rel: &str) -> String {
    format!("{dir}/{rel}")
}

fn join_rel(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// Expands an output directory's `Tree` into its member files and symlinks,
/// named `<dir_path>/<path inside>`. Empty directories produce no entries.
///
/// The children of a `Tree` are found by digest, the digest being that of the
/// child's serialized form under `func`.
pub(crate) fn expand_tree(
    dir_path: &str,
    tree: &Tree,
    func: DigestHasherFunc,
) -> Result<Vec<Entry>, Error> {
    let root = tree
        .root
        .as_ref()
        .ok_or_else(|| make_input_err!("Tree of output directory '{dir_path}' has no root"))?;
    let children: HashMap<DigestInfo, &Directory> = tree
        .children
        .iter()
        .map(|child| (digest_of(func, &child.encode_to_vec()), child))
        .collect();
    let mut entries = Vec::new();
    let mut stack: Vec<(String, &Directory, usize)> = vec![(String::new(), root, 0)];
    while let Some((rel, directory, depth)) = stack.pop() {
        if depth > MAX_DEPTH {
            return Err(make_input_err!(
                "Tree of output directory '{dir_path}' is deeper than {MAX_DEPTH}"
            ));
        }
        for file in &directory.files {
            check_name(&file.name)?;
            let path = join_rel(&rel, &file.name);
            let digest = digest_field(file.digest.as_ref(), &path)?;
            entries.push(Entry::file(
                EntryKind::DirectoryFile,
                member_path(dir_path, &path),
                digest,
                file.is_executable,
            ));
        }
        for symlink in &directory.symlinks {
            check_name(&symlink.name)?;
            let path = join_rel(&rel, &symlink.name);
            entries.push(Entry::symlink(
                member_path(dir_path, &path),
                symlink.target.clone(),
            ));
        }
        for node in &directory.directories {
            check_name(&node.name)?;
            let path = join_rel(&rel, &node.name);
            let digest = digest_field(node.digest.as_ref(), &path)?;
            let child = children.get(&digest).ok_or_else(|| {
                make_input_err!(
                    "Tree of output directory '{dir_path}' names child {digest} at '{path}', \
                     which is not among its children"
                )
            })?;
            stack.push((path, child, depth + 1));
        }
    }
    Ok(entries)
}

/// The `Tree` digests an `ActionResult`'s output directories name.
pub(crate) fn tree_digests(result: &ActionResult) -> Result<Vec<DigestInfo>, Error> {
    result
        .output_directories
        .iter()
        .map(|dir| {
            digest_field(
                dir.tree_digest.as_ref(),
                &format!("output directory '{}'", dir.path),
            )
        })
        .collect()
}

/// Builds the manifest of an `ActionResult`. `trees` holds the `Tree` of each
/// output directory, by digest.
pub(crate) fn manifest_from_action_result(
    result: &ActionResult,
    trees: &HashMap<DigestInfo, Tree>,
    func: DigestHasherFunc,
) -> Result<Manifest, Error> {
    let mut entries = Vec::new();
    for file in &result.output_files {
        let digest = digest_field(
            file.digest.as_ref(),
            &format!("output file '{}'", file.path),
        )?;
        entries.push(Entry::file(
            EntryKind::File,
            file.path.clone(),
            digest,
            file.is_executable,
        ));
    }
    // Servers fill the deprecated per-kind symlink lists, the v2.1 combined
    // list, or both; each symlink counts once.
    let symlinks: BTreeSet<(&str, &str)> = result
        .output_symlinks
        .iter()
        .chain(&result.output_file_symlinks)
        .chain(&result.output_directory_symlinks)
        .map(|symlink| (symlink.path.as_str(), symlink.target.as_str()))
        .collect();
    for (path, target) in symlinks {
        entries.push(Entry::symlink(path, target));
    }
    for dir in &result.output_directories {
        let digest = digest_field(
            dir.tree_digest.as_ref(),
            &format!("output directory '{}'", dir.path),
        )?;
        let tree = trees.get(&digest).ok_or_else(|| {
            make_input_err!("no Tree {digest} for output directory '{}'", dir.path)
        })?;
        entries.extend(expand_tree(&dir.path, tree, func)?);
    }
    Ok(Manifest::new(entries))
}

/// stdout or stderr of an `ActionResult`: the digest, or the digest of the
/// inlined bytes, or `None` when neither is set.
pub(crate) fn stream_digest(
    digest: Option<&Digest>,
    raw: &[u8],
    func: DigestHasherFunc,
) -> Result<Option<DigestInfo>, Error> {
    if let Some(digest) = digest {
        return Ok(Some(DigestInfo::try_from(digest)?));
    }
    if raw.is_empty() {
        return Ok(None);
    }
    Ok(Some(digest_of(func, raw)))
}

/// A blob that did not arrive as its digest says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Problem {
    /// The bytes the CAS returned hash to something else. Proof the CAS
    /// served wrong bytes for this digest.
    Mismatch {
        what: String,
        expected: DigestInfo,
        actual: DigestInfo,
        via: &'static str,
    },
    /// The CAS does not have the blob.
    Missing { what: String, digest: DigestInfo },
    /// The read failed for another reason.
    ReadFailed {
        what: String,
        digest: DigestInfo,
        message: String,
    },
}

impl Problem {
    pub(crate) const fn is_mismatch(&self) -> bool {
        matches!(self, Self::Mismatch { .. })
    }

    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Mismatch {
                what,
                expected,
                actual,
                via,
            } => format!(
                "DIGEST MISMATCH for {what}: asked for {expected}, the CAS served bytes that hash \
                 to {actual} (via {via})"
            ),
            Self::Missing { what, digest } => format!("missing from the CAS: {what} ({digest})"),
            Self::ReadFailed {
                what,
                digest,
                message,
            } => format!("read failed for {what} ({digest}): {message}"),
        }
    }
}

/// The comparable result of one run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunOutcome {
    pub(crate) manifest: Manifest,
    pub(crate) exit_code: i32,
    pub(crate) stdout: Option<DigestInfo>,
    pub(crate) stderr: Option<DigestInfo>,
}

/// One execution, local or remote.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Run {
    /// 1-based.
    pub(crate) index: usize,
    /// `ExecutedActionMetadata.worker` for a remote run, the host for a local one.
    pub(crate) worker: String,
    /// `None` when the run produced no result to compare.
    pub(crate) outcome: Option<RunOutcome>,
    /// Why there is no outcome, or what went wrong around it.
    pub(crate) error: Option<String>,
    pub(crate) problems: Vec<Problem>,
    /// Declared outputs the command did not create.
    pub(crate) missing_outputs: Vec<String>,
    pub(crate) notes: Vec<String>,
    pub(crate) duration_ms: u64,
}

impl Run {
    pub(crate) fn mismatches(&self) -> usize {
        self.problems.iter().filter(|p| p.is_mismatch()).count()
    }
}

/// A path whose value is not the same in every completed run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VariedPath {
    pub(crate) path: String,
    /// The value in each completed run, by run index; `(absent)` if the run
    /// did not produce the path.
    pub(crate) per_run: Vec<(usize, String)>,
}

/// What a set of runs has in common and where it differs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Summary {
    pub(crate) runs: usize,
    pub(crate) completed: usize,
    /// Each distinct manifest fingerprint with the runs that produced it, in
    /// order of first appearance.
    pub(crate) fingerprints: Vec<(String, Vec<usize>)>,
    pub(crate) varied_paths: Vec<VariedPath>,
    pub(crate) exit_codes: BTreeSet<i32>,
    pub(crate) distinct_stdout: usize,
    pub(crate) distinct_stderr: usize,
    pub(crate) mismatches: usize,
}

impl Summary {
    /// More than one distinct output manifest among the completed runs.
    pub(crate) const fn varies(&self) -> bool {
        self.fingerprints.len() > 1
    }
}

pub(crate) const ABSENT: &str = "(absent)";

pub(crate) fn summarize(runs: &[Run]) -> Summary {
    let completed: Vec<(usize, &RunOutcome)> = runs
        .iter()
        .filter_map(|run| run.outcome.as_ref().map(|outcome| (run.index, outcome)))
        .collect();

    let mut fingerprints: Vec<(String, Vec<usize>)> = Vec::new();
    for (index, outcome) in &completed {
        let fingerprint = outcome.manifest.fingerprint();
        if let Some((_, indices)) = fingerprints.iter_mut().find(|(f, _)| *f == fingerprint) {
            indices.push(*index);
        } else {
            fingerprints.push((fingerprint, vec![*index]));
        }
    }

    let per_run: Vec<(usize, BTreeMap<&str, BTreeSet<String>>)> = completed
        .iter()
        .map(|(index, outcome)| (*index, outcome.manifest.values_by_path()))
        .collect();
    let all_paths: BTreeSet<&str> = per_run
        .iter()
        .flat_map(|(_, values)| values.keys().copied())
        .collect();
    let mut varied_paths = Vec::new();
    for path in all_paths {
        let values: Vec<(usize, String)> = per_run
            .iter()
            .map(|(index, values)| {
                let value = values.get(path).map_or_else(
                    || ABSENT.to_string(),
                    |set| set.iter().cloned().collect::<Vec<_>>().join(", "),
                );
                (*index, value)
            })
            .collect();
        let distinct: BTreeSet<&String> = values.iter().map(|(_, v)| v).collect();
        if distinct.len() > 1 {
            varied_paths.push(VariedPath {
                path: path.to_string(),
                per_run: values,
            });
        }
    }

    let distinct_stdout = completed
        .iter()
        .map(|(_, o)| o.stdout)
        .collect::<BTreeSet<_>>()
        .len();
    let distinct_stderr = completed
        .iter()
        .map(|(_, o)| o.stderr)
        .collect::<BTreeSet<_>>()
        .len();

    Summary {
        runs: runs.len(),
        completed: completed.len(),
        fingerprints,
        varied_paths,
        exit_codes: completed.iter().map(|(_, o)| o.exit_code).collect(),
        distinct_stdout,
        distinct_stderr,
        mismatches: runs.iter().map(Run::mismatches).sum(),
    }
}

/// An output digest the executor produced that no oracle run produced at
/// the same path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnmatchedOutput {
    pub(crate) path: String,
    pub(crate) digest: DigestInfo,
    /// The remote runs that produced it.
    pub(crate) runs: Vec<usize>,
}

/// What `compare` concludes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// A fetched blob failed digest verification.
    CasServedWrongBytes,
    /// Not enough completed runs on one side to say anything.
    Inconclusive,
    /// The oracle's own runs differ.
    ActionNondeterministic,
    /// The oracle is stable and the remote runs differ.
    NativeLinkVaries,
    /// Both are stable, and they disagree.
    StableButDifferent,
    /// Both are stable and the executor produced nothing the oracle did not.
    Agree,
}

impl Verdict {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CasServedWrongBytes => "cas_served_wrong_bytes",
            Self::Inconclusive => "inconclusive",
            Self::ActionNondeterministic => "action_nondeterministic",
            Self::NativeLinkVaries => "nativelink_varies",
            Self::StableButDifferent => "stable_but_different",
            Self::Agree => "agree",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Comparison {
    pub(crate) oracle: Summary,
    pub(crate) nativelink: Summary,
    pub(crate) unmatched: Vec<UnmatchedOutput>,
    /// Exit codes the executor returned that the oracle never did.
    pub(crate) unmatched_exit_codes: BTreeSet<i32>,
    pub(crate) verdict: Verdict,
}

pub(crate) fn compare_runs(oracle_runs: &[Run], nativelink_runs: &[Run]) -> Comparison {
    let oracle = summarize(oracle_runs);
    let nativelink = summarize(nativelink_runs);

    let oracle_digests: BTreeSet<(&str, DigestInfo)> = oracle_runs
        .iter()
        .filter_map(|run| run.outcome.as_ref())
        .flat_map(|outcome| outcome.manifest.entries())
        .filter_map(|entry| entry.digest.map(|d| (entry.path.as_str(), d)))
        .collect();
    let mut unmatched: BTreeMap<(String, DigestInfo), Vec<usize>> = BTreeMap::new();
    // With no completed oracle run there is nothing to hold the remote
    // outputs against, and every one of them would look unmatched.
    let comparable_runs = if oracle.completed == 0 {
        &[][..]
    } else {
        nativelink_runs
    };
    for run in comparable_runs {
        let Some(outcome) = &run.outcome else {
            continue;
        };
        for entry in outcome.manifest.entries() {
            if let Some(digest) = entry.digest
                && !oracle_digests.contains(&(entry.path.as_str(), digest))
            {
                let runs = unmatched.entry((entry.path.clone(), digest)).or_default();
                if !runs.contains(&run.index) {
                    runs.push(run.index);
                }
            }
        }
    }
    let unmatched: Vec<UnmatchedOutput> = unmatched
        .into_iter()
        .map(|((path, digest), runs)| UnmatchedOutput { path, digest, runs })
        .collect();
    let unmatched_exit_codes: BTreeSet<i32> = if oracle.completed == 0 {
        BTreeSet::new()
    } else {
        nativelink
            .exit_codes
            .difference(&oracle.exit_codes)
            .copied()
            .collect()
    };

    let verdict = if oracle.mismatches + nativelink.mismatches > 0 {
        Verdict::CasServedWrongBytes
    } else if oracle.completed < 2 || nativelink.completed < 1 {
        Verdict::Inconclusive
    } else if oracle.varies() {
        Verdict::ActionNondeterministic
    } else if nativelink.varies() {
        Verdict::NativeLinkVaries
    } else if !unmatched.is_empty() || !unmatched_exit_codes.is_empty() {
        Verdict::StableButDifferent
    } else {
        Verdict::Agree
    };

    Comparison {
        oracle,
        nativelink,
        unmatched,
        unmatched_exit_codes,
        verdict,
    }
}

/// The factual reading of a comparison, one paragraph per line.
pub(crate) fn interpretation(comparison: &Comparison) -> Vec<String> {
    let mut lines = Vec::new();
    if comparison.oracle.mismatches + comparison.nativelink.mismatches > 0 {
        lines.push(
            "A fetched blob failed digest verification: the CAS served bytes that do not match \
             their digest. The mismatches are listed above. Nothing else can be concluded about \
             this action until that is fixed: every run, local or remote, may have used those \
             bytes."
                .to_string(),
        );
    }
    match comparison.verdict {
        Verdict::CasServedWrongBytes => {}
        Verdict::Inconclusive => lines.push(format!(
            "Inconclusive: {} oracle run(s) and {} NativeLink run(s) completed; at least 2 and 1 \
             are needed. See the errors above.",
            comparison.oracle.completed, comparison.nativelink.completed
        )),
        Verdict::ActionNondeterministic => lines.push(format!(
            "The oracle varies ({} distinct output manifests over {} local runs from the same \
             verified inputs and environment): the action itself is nondeterministic. That is the \
             build's problem, not NativeLink's. If `local --same-path` is stable, the only thing \
             that varies is the absolute path the action runs at, which it records in its output.",
            comparison.oracle.fingerprints.len(),
            comparison.oracle.completed
        )),
        Verdict::NativeLinkVaries => lines.push(format!(
            "The oracle is stable ({} local runs, one manifest) and NativeLink varies ({} distinct \
             manifests over {} runs): the difference is on the NativeLink side (inputs \
             materialized, worker environment, or output handling). Check whether the manifests \
             follow the worker column, and investigate those attempts with the worker's \
             provenance events (input_audit, if the deployed NativeLink emits them).",
            comparison.oracle.completed,
            comparison.nativelink.fingerprints.len(),
            comparison.nativelink.completed
        )),
        Verdict::StableButDifferent => lines.push(
            "Both are stable, but NativeLink produced outputs (or exit codes) the oracle never \
             did. The worker's environment or toolchain differs from this host's, or NativeLink \
             materializes inputs differently. Check that this host runs the workers' image."
                .to_string(),
        ),
        Verdict::Agree => lines.push(format!(
            "Neither varies over these runs ({} local, {} NativeLink) and NativeLink produced \
             nothing the oracle did not. Variation rarer than this sample cannot be ruled out.",
            comparison.oracle.completed, comparison.nativelink.completed
        )),
    }
    lines
}

#[cfg(test)]
mod tests {
    use nativelink_proto::build::bazel::remote::execution::v2::{
        DirectoryNode, FileNode, OutputDirectory, OutputFile, OutputSymlink, SymlinkNode,
    };

    use super::*;

    const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn d(byte: u8, size: u64) -> DigestInfo {
        DigestInfo::new([byte; 32], size)
    }

    #[test]
    fn parses_hash_dash_size_and_hash_slash_size() {
        let digest = parse_digest(&format!("{HASH}-42")).unwrap();
        assert_eq!(digest.to_string(), format!("{HASH}-42"));
        assert_eq!(parse_digest(&format!("{HASH}/42")).unwrap(), digest);
        assert_eq!(
            parse_digest(&format!("  {}-42\n", HASH.to_uppercase())).unwrap(),
            digest
        );
    }

    #[test]
    fn rejects_bad_digests() {
        for bad in [
            "",
            HASH,
            &format!("{HASH}-"),
            &format!("{HASH}-x"),
            &format!("{HASH}--1"),
            &format!("{}-1", &HASH[..63]),
            &format!("{}g-1", &HASH[..63]),
        ] {
            assert!(parse_digest(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn checks_names_and_relative_paths() {
        for good in ["a", "a.txt", "..a", "a b"] {
            check_name(good).unwrap();
        }
        for bad in ["", ".", "..", "a/b", "a\0"] {
            assert!(check_name(bad).is_err(), "accepted {bad}");
        }
        for good in ["", "a", "a/b/c"] {
            check_relative_path(good).unwrap();
        }
        for bad in ["/a", "a/../b", "a//b", "a/", "./a"] {
            assert!(check_relative_path(bad).is_err(), "accepted {bad}");
        }
    }

    #[test]
    fn fingerprint_matches_the_worker_definition() {
        // The worked example from the worker's own fingerprint test, which
        // was hashed outside the code base.
        let manifest = Manifest::new(vec![
            Entry::file(EntryKind::File, "out/a.txt", d(1, 3), false),
            Entry::symlink("link", "out/a.txt"),
            Entry::file(EntryKind::DirectoryFile, "dir/b", d(2, 5), true),
        ]);
        assert_eq!(
            manifest.fingerprint(),
            "1a80e77c3ea0478f1dc05835dd1bda1b51cff2df1277d34a0e5205f31948b775"
        );
        assert_eq!(
            Manifest::new(vec![]).fingerprint(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(manifest.total_bytes(), 8);
    }

    fn dir_with(
        files: Vec<FileNode>,
        dirs: Vec<DirectoryNode>,
        links: Vec<SymlinkNode>,
    ) -> Directory {
        Directory {
            files,
            directories: dirs,
            symlinks: links,
            node_properties: None,
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

    #[test]
    fn expands_a_tree_into_member_files() {
        let func = DigestHasherFunc::Sha256;
        let leaf = dir_with(vec![file_node("c", d(3, 1), false)], vec![], vec![]);
        let leaf_digest = digest_of(func, &leaf.encode_to_vec());
        let middle = dir_with(
            vec![file_node("b", d(2, 2), true)],
            vec![DirectoryNode {
                name: "leaf".to_string(),
                digest: Some(leaf_digest.into()),
            }],
            vec![SymlinkNode {
                name: "l".to_string(),
                target: "b".to_string(),
                node_properties: None,
            }],
        );
        let middle_digest = digest_of(func, &middle.encode_to_vec());
        let root = dir_with(
            vec![file_node("a", d(1, 3), false)],
            vec![
                DirectoryNode {
                    name: "m".to_string(),
                    digest: Some(middle_digest.into()),
                },
                // The same subtree twice: children are found by digest.
                DirectoryNode {
                    name: "m2".to_string(),
                    digest: Some(middle_digest.into()),
                },
            ],
            vec![],
        );
        let tree = Tree {
            root: Some(root),
            children: vec![middle, leaf],
        };
        let manifest = Manifest::new(expand_tree("out", &tree, func).unwrap());
        let got: Vec<(String, &str, String)> = manifest
            .entries()
            .iter()
            .map(|e| (e.path.clone(), e.kind.as_str(), e.value()))
            .collect();
        let expect = vec![
            ("out/a".to_string(), "directory_file", d(1, 3).to_string()),
            (
                "out/m/b".to_string(),
                "directory_file",
                format!("{} (executable)", d(2, 2)),
            ),
            ("out/m/l".to_string(), "symlink", "symlink -> b".to_string()),
            (
                "out/m/leaf/c".to_string(),
                "directory_file",
                d(3, 1).to_string(),
            ),
            (
                "out/m2/b".to_string(),
                "directory_file",
                format!("{} (executable)", d(2, 2)),
            ),
            (
                "out/m2/l".to_string(),
                "symlink",
                "symlink -> b".to_string(),
            ),
            (
                "out/m2/leaf/c".to_string(),
                "directory_file",
                d(3, 1).to_string(),
            ),
        ];
        assert_eq!(got, expect);
    }

    #[test]
    fn tree_expansion_rejects_a_missing_child_and_bad_names() {
        let func = DigestHasherFunc::Sha256;
        let tree = Tree {
            root: Some(dir_with(
                vec![],
                vec![DirectoryNode {
                    name: "x".to_string(),
                    digest: Some(d(9, 9).into()),
                }],
                vec![],
            )),
            children: vec![],
        };
        assert!(expand_tree("out", &tree, func).is_err());
        let tree = Tree {
            root: Some(dir_with(
                vec![file_node("../escape", d(1, 1), false)],
                vec![],
                vec![],
            )),
            children: vec![],
        };
        assert!(expand_tree("out", &tree, func).is_err());
        assert!(expand_tree("out", &Tree::default(), func).is_err());
    }

    #[test]
    fn manifest_from_action_result_merges_files_symlinks_and_trees() {
        let func = DigestHasherFunc::Sha256;
        let tree = Tree {
            root: Some(dir_with(
                vec![file_node("f", d(5, 5), false)],
                vec![],
                vec![],
            )),
            children: vec![],
        };
        let tree_digest = digest_of(func, &tree.encode_to_vec());
        let link = OutputSymlink {
            path: "ln".to_string(),
            target: "x".to_string(),
            node_properties: None,
        };
        let result = ActionResult {
            output_files: vec![OutputFile {
                path: "bin/tool".to_string(),
                digest: Some(d(4, 4).into()),
                is_executable: true,
                ..Default::default()
            }],
            // The same symlink in the deprecated and the combined list.
            output_file_symlinks: vec![link.clone()],
            output_symlinks: vec![link],
            output_directories: vec![OutputDirectory {
                path: "gen".to_string(),
                tree_digest: Some(tree_digest.into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(tree_digests(&result).unwrap(), vec![tree_digest]);
        let trees = HashMap::from([(tree_digest, tree)]);
        let manifest = manifest_from_action_result(&result, &trees, func).unwrap();
        let paths: Vec<(&str, EntryKind)> = manifest
            .entries()
            .iter()
            .map(|e| (e.path.as_str(), e.kind))
            .collect();
        assert_eq!(
            paths,
            vec![
                ("bin/tool", EntryKind::File),
                ("gen/f", EntryKind::DirectoryFile),
                ("ln", EntryKind::Symlink),
            ]
        );
        assert!(manifest_from_action_result(&result, &HashMap::new(), func).is_err());
    }

    #[test]
    fn stream_digest_prefers_the_digest_then_the_inline_bytes() {
        let func = DigestHasherFunc::Sha256;
        let digest = d(7, 3);
        assert_eq!(
            stream_digest(Some(&digest.into()), b"abc", func).unwrap(),
            Some(digest)
        );
        assert_eq!(
            stream_digest(None, b"abc", func).unwrap(),
            Some(digest_of(func, b"abc"))
        );
        assert_eq!(stream_digest(None, b"", func).unwrap(), None);
    }

    fn run(index: usize, entries: Vec<Entry>, exit_code: i32) -> Run {
        Run {
            index,
            worker: "w".to_string(),
            outcome: Some(RunOutcome {
                manifest: Manifest::new(entries),
                exit_code,
                stdout: None,
                stderr: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn summary_groups_fingerprints_and_lists_varied_paths() {
        let stable = Entry::file(EntryKind::File, "same", d(1, 1), false);
        let runs = vec![
            run(
                1,
                vec![
                    stable.clone(),
                    Entry::file(EntryKind::File, "v", d(2, 1), false),
                ],
                0,
            ),
            run(
                2,
                vec![
                    stable.clone(),
                    Entry::file(EntryKind::File, "v", d(3, 1), false),
                ],
                0,
            ),
            run(
                3,
                vec![
                    stable.clone(),
                    Entry::file(EntryKind::File, "v", d(2, 1), false),
                ],
                0,
            ),
            run(4, vec![stable], 0),
            Run {
                index: 5,
                error: Some("failed".to_string()),
                ..Default::default()
            },
        ];
        let summary = summarize(&runs);
        assert_eq!(summary.runs, 5);
        assert_eq!(summary.completed, 4);
        assert!(summary.varies());
        let groups: Vec<&Vec<usize>> = summary.fingerprints.iter().map(|(_, r)| r).collect();
        assert_eq!(groups, vec![&vec![1, 3], &vec![2], &vec![4]]);
        assert_eq!(summary.varied_paths.len(), 1);
        let varied = &summary.varied_paths[0];
        assert_eq!(varied.path, "v");
        assert_eq!(
            varied.per_run,
            vec![
                (1, d(2, 1).to_string()),
                (2, d(3, 1).to_string()),
                (3, d(2, 1).to_string()),
                (4, ABSENT.to_string()),
            ]
        );
        assert_eq!(summary.exit_codes, BTreeSet::from([0]));
    }

    #[test]
    fn an_executable_bit_or_symlink_target_change_is_a_variation() {
        let runs = vec![
            run(
                1,
                vec![
                    Entry::file(EntryKind::File, "f", d(1, 1), false),
                    Entry::symlink("l", "a"),
                ],
                0,
            ),
            run(
                2,
                vec![
                    Entry::file(EntryKind::File, "f", d(1, 1), true),
                    Entry::symlink("l", "b"),
                ],
                0,
            ),
        ];
        let summary = summarize(&runs);
        let paths: Vec<&str> = summary
            .varied_paths
            .iter()
            .map(|v| v.path.as_str())
            .collect();
        assert_eq!(paths, vec!["f", "l"]);
    }

    #[test]
    fn compare_names_each_outcome() {
        let out = |digest| vec![Entry::file(EntryKind::File, "out", digest, false)];
        let stable_oracle = vec![run(1, out(d(1, 1)), 0), run(2, out(d(1, 1)), 0)];
        let varying_oracle = vec![run(1, out(d(1, 1)), 0), run(2, out(d(2, 1)), 0)];

        let agree = compare_runs(&stable_oracle, &[run(1, out(d(1, 1)), 0)]);
        assert_eq!(agree.verdict, Verdict::Agree);
        assert!(agree.unmatched.is_empty());

        let nondeterministic = compare_runs(&varying_oracle, &[run(1, out(d(3, 1)), 0)]);
        assert_eq!(nondeterministic.verdict, Verdict::ActionNondeterministic);
        assert_eq!(nondeterministic.unmatched.len(), 1);

        let nl_varies = compare_runs(
            &stable_oracle,
            &[
                run(1, out(d(1, 1)), 0),
                run(2, out(d(4, 1)), 0),
                run(3, out(d(4, 1)), 0),
            ],
        );
        assert_eq!(nl_varies.verdict, Verdict::NativeLinkVaries);
        assert_eq!(
            nl_varies.unmatched,
            vec![UnmatchedOutput {
                path: "out".to_string(),
                digest: d(4, 1),
                runs: vec![2, 3],
            }]
        );

        let differs = compare_runs(&stable_oracle, &[run(1, out(d(5, 1)), 0)]);
        assert_eq!(differs.verdict, Verdict::StableButDifferent);
        let exit_differs = compare_runs(&stable_oracle, &[run(1, out(d(1, 1)), 1)]);
        assert_eq!(exit_differs.verdict, Verdict::StableButDifferent);
        assert_eq!(exit_differs.unmatched_exit_codes, BTreeSet::from([1]));

        let mut corrupt = stable_oracle.clone();
        corrupt[0].problems.push(Problem::Mismatch {
            what: "input".to_string(),
            expected: d(1, 1),
            actual: d(2, 1),
            via: "bytestream",
        });
        let corrupt = compare_runs(&corrupt, &[run(1, out(d(1, 1)), 0)]);
        assert_eq!(corrupt.verdict, Verdict::CasServedWrongBytes);
        assert!(interpretation(&corrupt)[0].contains("failed digest verification"));

        let inconclusive = compare_runs(&stable_oracle[..1], &[run(1, out(d(1, 1)), 0)]);
        assert_eq!(inconclusive.verdict, Verdict::Inconclusive);

        // No completed oracle run: nothing to hold the remote outputs against.
        let failed = Run {
            index: 1,
            error: Some("inputs failed".to_string()),
            ..Default::default()
        };
        let nothing = compare_runs(&[failed], &[run(1, out(d(1, 1)), 3)]);
        assert_eq!(nothing.verdict, Verdict::Inconclusive);
        assert!(nothing.unmatched.is_empty());
        assert!(nothing.unmatched_exit_codes.is_empty());
    }
}
