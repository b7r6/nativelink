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

//! Check a filesystem store's blobs against the digests in their names.
//!
//! A filesystem store keeps each blob at `<content_path>/d2/<hash>-<size>[-<n>]`
//! and, for inputs a worker hardlinks as executable, a read-only copy at
//! `<content_path>.exec/d2/<hash>-<size>`. Workers hardlink these files
//! straight into action input trees, so an action that makes an input
//! writable and changes it in place changes the stored blob too. Every later
//! action on that worker then gets the changed bytes under the original
//! digest, and its result is cached.
//!
//! This walks both directories (the `.exec` sibling is found automatically)
//! and reports every file whose size, or with `--hash` whose content, does not
//! match its name. The size check reads no file data and takes seconds even on
//! a large store; `--hash` reads every byte. It only reads; it never changes
//! the store.
//!
//! Exit status: 0 if every checked file matches, 1 if any does not, 2 on bad
//! usage or when a store directory cannot be read.

use std::fs::{self, DirEntry, File, Metadata};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use nativelink_util::digest_hasher::{DigestHasher, DigestHasherFunc};

/// The digest function a store's names were computed with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Algorithm {
    Sha256,
    Blake3,
}

impl Algorithm {
    const fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Blake3 => "blake3",
        }
    }

    const fn hasher_func(self) -> DigestHasherFunc {
        match self {
            Self::Sha256 => DigestHasherFunc::Sha256,
            Self::Blake3 => DigestHasherFunc::Blake3,
        }
    }
}

/// Command-line arguments.
#[derive(Parser, Debug)]
#[command(
    version,
    about = "Check a NativeLink filesystem store's blobs against the digests in their names."
)]
struct Args {
    /// A filesystem store's `content_path`; its `.exec` sibling is checked too.
    #[arg(required = true, value_name = "CONTENT_PATH")]
    content_paths: Vec<PathBuf>,

    /// Also hash every blob (reads all data).
    #[arg(long)]
    hash: bool,

    /// The digest function the store uses.
    #[arg(long, value_enum, default_value_t = Algorithm::Sha256)]
    algorithm: Algorithm,
}

/// Parses `<64 hex digits>-<size>[-<n>]` into (hash, size).
fn parse_name(name: &str) -> Option<(&str, u128)> {
    let mut parts = name.splitn(3, '-');
    let hash = parts.next()?;
    let size = parts.next()?;
    let digits = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    if hash.len() != 64 || !hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    if !digits(size) || parts.next().is_some_and(|n| !digits(n)) {
        return None;
    }
    Some((hash, size.parse().ok()?))
}

/// The `d2` directories of a content path and of its `.exec` sibling, where
/// they exist.
fn directories(content_path: &Path) -> Vec<PathBuf> {
    let text = content_path.to_string_lossy();
    let exec = PathBuf::from(format!("{}.exec", text.trim_end_matches('/')));
    [content_path.to_path_buf(), exec]
        .into_iter()
        .map(|base| base.join("d2"))
        .filter(|dir| dir.is_dir())
        .collect()
}

fn file_digest(path: &Path, algorithm: Algorithm) -> io::Result<String> {
    let mut hasher = algorithm.hasher_func().hasher();
    let mut file = File::open(path)?;
    let mut buffer = vec![0; 1 << 20];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize_digest().packed_hash().to_string())
}

/// Mode, link count and times, for telling how a blob was changed.
#[cfg(unix)]
fn describe(metadata: &Metadata) -> String {
    use std::os::unix::fs::MetadataExt;
    format!(
        "mode {:o}, links {}, modified {}, changed {}",
        metadata.mode() & 0o7777,
        metadata.nlink(),
        metadata.mtime(),
        metadata.ctime()
    )
}

/// Mode, link count and times, for telling how a blob was changed. Only the
/// modification time is portable.
#[cfg(not(unix))]
fn describe(metadata: &Metadata) -> String {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or_else(|| "?".to_string(), |since| since.as_secs().to_string());
    format!(
        "readonly {}, modified {modified}",
        metadata.permissions().readonly()
    )
}

/// Checks one directory entry; returns `None` for a file that is not a blob,
/// or `Some(problem)` where `problem` is empty when the blob matches.
fn check_entry(
    entry: &DirEntry,
    do_hash: bool,
    algorithm: Algorithm,
) -> io::Result<Option<String>> {
    let name = entry.file_name();
    let Some((want_hash, want_size)) = name.to_str().and_then(parse_name) else {
        return Ok(None);
    };
    if !entry.file_type()?.is_file() {
        return Ok(None);
    }
    let metadata = entry.metadata()?;
    let problem = if u128::from(metadata.len()) != want_size {
        format!("size {}, name says {want_size}", metadata.len())
    } else if do_hash {
        let got = file_digest(&entry.path(), algorithm)?;
        if got == want_hash {
            String::new()
        } else {
            format!("content hashes to {}…", &got[..16])
        }
    } else {
        String::new()
    };
    if problem.is_empty() {
        return Ok(Some(problem));
    }
    Ok(Some(format!(
        "MISMATCH {}: {problem}; {}",
        entry.path().display(),
        describe(&metadata)
    )))
}

/// Checks every blob under the content paths, writing one line per mismatch
/// and a summary to `out`. Returns the number of mismatched blobs.
fn check(
    content_paths: &[PathBuf],
    do_hash: bool,
    algorithm: Algorithm,
    out: &mut dyn Write,
) -> io::Result<usize> {
    let (mut checked, mut bad) = (0, 0);
    for content_path in content_paths {
        for dir in directories(content_path) {
            for entry in fs::read_dir(&dir)? {
                match check_entry(&entry?, do_hash, algorithm)? {
                    None => {}
                    Some(problem) if problem.is_empty() => checked += 1,
                    Some(problem) => {
                        checked += 1;
                        bad += 1;
                        writeln!(out, "{problem}")?;
                    }
                }
            }
        }
    }
    let mode = if do_hash {
        format!("size and {} content", algorithm.name())
    } else {
        "size only".to_string()
    };
    writeln!(out, "checked {checked} blobs ({mode}); {bad} mismatched")?;
    Ok(bad)
}

fn main() -> ExitCode {
    let args = Args::parse();
    match check(
        &args.content_paths,
        args.hash,
        args.algorithm,
        &mut io::stdout().lock(),
    ) {
        Ok(0) => ExitCode::SUCCESS,
        Ok(_) => ExitCode::from(1),
        Err(error) => {
            drop(writeln!(io::stderr(), "error: {error}"));
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use sha2::{Digest as _, Sha256};

    use super::{Algorithm, check, parse_name};

    /// A content path under a fresh temporary directory, removed on drop.
    struct Store {
        root: PathBuf,
        content: PathBuf,
    }

    impl Store {
        fn new(name: &str) -> Self {
            let root = std::env::temp_dir()
                .join(format!("verify_fast_store_{name}_{}", std::process::id()));
            drop(std::fs::remove_dir_all(&root));
            let content = root.join("content_path-cas");
            Self { root, content }
        }

        fn d2(&self) -> PathBuf {
            self.content.join("d2")
        }

        fn exec_d2(&self) -> PathBuf {
            PathBuf::from(format!("{}.exec", self.content.display())).join("d2")
        }

        fn run(&self, do_hash: bool) -> (usize, String) {
            let mut out = Vec::new();
            let bad = check(
                core::slice::from_ref(&self.content),
                do_hash,
                Algorithm::Sha256,
                &mut out,
            )
            .unwrap();
            (bad, String::from_utf8(out).unwrap())
        }
    }

    impl Drop for Store {
        fn drop(&mut self) {
            drop(std::fs::remove_dir_all(&self.root));
        }
    }

    /// Writes `data` into `directory` under its digest name (or `name`), with
    /// `suffix` appended; returns the file's path.
    fn put(directory: &Path, data: &[u8], name: Option<&str>, suffix: &str) -> PathBuf {
        std::fs::create_dir_all(directory).unwrap();
        let name = name.map_or_else(
            || {
                format!(
                    "{}-{}{suffix}",
                    hex::encode(Sha256::digest(data)),
                    data.len()
                )
            },
            ToString::to_string,
        );
        let path = directory.join(name);
        std::fs::write(&path, data).unwrap();
        path
    }

    #[test]
    fn intact_store_passes() {
        let store = Store::new("intact");
        put(&store.d2(), b"hello", None, "-10");
        put(&store.exec_d2(), b"tool", None, "");
        let (bad, out) = store.run(true);
        assert_eq!(bad, 0);
        assert!(out.contains("checked 2 blobs"), "{out}");
    }

    #[test]
    fn appended_exec_variant_fails_on_size() {
        let store = Store::new("appended");
        let path = put(&store.exec_d2(), b"input", None, "");
        let mut data = std::fs::read(&path).unwrap();
        data.extend_from_slice(b"scribbled\n");
        std::fs::write(&path, data).unwrap();
        let (bad, out) = store.run(false);
        assert_eq!(bad, 1);
        assert!(out.contains("size 15, name says 5"), "{out}");
    }

    #[test]
    fn same_size_rewrite_needs_hash() {
        let store = Store::new("rewrite");
        let path = put(&store.d2(), b"aaaa", None, "");
        std::fs::write(&path, b"bbbb").unwrap();
        assert_eq!(store.run(false).0, 0);
        let (bad, out) = store.run(true);
        assert_eq!(bad, 1);
        assert!(out.contains("content hashes to"), "{out}");
    }

    #[test]
    fn ignores_files_that_are_not_digests() {
        let store = Store::new("ignores");
        put(&store.d2(), b"x", Some("not-a-digest"), "");
        assert_eq!(
            store.run(true),
            (
                0,
                "checked 0 blobs (size and sha256 content); 0 mismatched\n".to_string()
            )
        );
    }

    #[test]
    fn blake3_stores_are_hashed_with_blake3() {
        // BLAKE3("hello"), from the reference implementation.
        let hello = "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f";
        let store = Store::new("blake3");
        put(&store.d2(), b"hello", Some(&format!("{hello}-5")), "");
        put(&store.d2(), b"hellp", Some(&format!("{hello}-5-1")), "");
        let mut out = Vec::new();
        let bad = check(
            core::slice::from_ref(&store.content),
            true,
            Algorithm::Blake3,
            &mut out,
        )
        .unwrap();
        let out = String::from_utf8(out).unwrap();
        assert_eq!(bad, 1, "{out}");
        assert!(
            out.contains(&format!("{hello}-5-1: content hashes to")),
            "{out}"
        );
        assert!(out.ends_with("checked 2 blobs (size and blake3 content); 1 mismatched\n"));
    }

    #[test]
    fn names() {
        let hash = "a".repeat(64);
        assert_eq!(parse_name(&format!("{hash}-5")), Some((hash.as_str(), 5)));
        assert_eq!(
            parse_name(&format!("{hash}-5-12")),
            Some((hash.as_str(), 5))
        );
        assert_eq!(parse_name(&format!("{hash}-5-")), None);
        assert_eq!(parse_name(&format!("{hash}-5-x")), None);
        assert_eq!(parse_name(&format!("{}-5", "A".repeat(64))), None);
        assert_eq!(parse_name(&format!("{}-5", "a".repeat(63))), None);
        assert_eq!(parse_name("not-a-digest"), None);
    }
}
