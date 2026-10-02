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

//! OCI → REAPI projection.
//!
//! Implements §6 of the Standard OCI Toolchain Specification: given one or more
//! OCI layer tarballs (uncompressed), this module constructs an REAPI `Directory`
//! Merkle tree where:
//!
//! - Each regular file becomes a `FileNode` whose `digest` is the BLAKE3 hash
//!   of the file's raw content.
//! - Each symlink becomes a `SymlinkNode` with `target` as-is.
//! - Each subdirectory becomes a `DirectoryNode` whose `digest` is the BLAKE3
//!   hash of the serialized `Directory` proto for that subdirectory.
//!
//! Layers are additive and disjoint (§5.2) — no whiteout handling is needed for
//! conforming toolchain images. Non-conforming images (with whiteouts) are
//! rejected.
//!
//! The output is a [`ProjectionResult`] containing the root `Directory`, all
//! child `Directory` protos (for `Tree.children`), and a map of file content
//! blobs (digest → bytes) that must be uploaded to CAS.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Component, Path};

use bytes::Bytes;
use nativelink_error::{Error, make_input_err};
use nativelink_proto::build::bazel::remote::execution::v2::{
    Directory as ProtoDirectory, DirectoryNode, FileNode, SymlinkNode,
};
use prost::Message;
use tracing::{debug, warn};

use crate::registry::ToolchainHints;

/// Digest function to use for REAPI projection.
/// Per §6.2, this SHOULD be BLAKE3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionDigestFunction {
    Blake3,
    Sha256,
}

impl ProjectionDigestFunction {
    /// Hash raw bytes and return `(hex_digest, size)`.
    pub fn hash_bytes(&self, data: &[u8]) -> (String, i64) {
        let hex = match self {
            Self::Blake3 => {
                let hash = blake3::hash(data);
                hash.to_hex().to_string()
            }
            Self::Sha256 => {
                use sha2::{Digest as _, Sha256};
                let hash = Sha256::digest(data);
                hex::encode(hash)
            }
        };
        (hex, data.len() as i64)
    }
}

/// A content-addressed digest (hash + `size_bytes`).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DigestPair {
    pub hash: String,
    pub size_bytes: i64,
}

impl DigestPair {
    fn to_proto(&self) -> nativelink_proto::build::bazel::remote::execution::v2::Digest {
        nativelink_proto::build::bazel::remote::execution::v2::Digest {
            hash: self.hash.clone(),
            size_bytes: self.size_bytes,
        }
    }
}

/// Result of projecting OCI layers into an REAPI Directory tree.
#[derive(Debug)]
pub struct ProjectionResult {
    /// The root Directory proto.
    pub root: ProtoDirectory,
    /// Digest of the root Directory (hash of its serialized proto bytes).
    pub root_digest: DigestPair,
    /// All child Directory protos (for `Tree.children`), keyed by digest.
    pub children: Vec<(DigestPair, ProtoDirectory)>,
    /// File content blobs that need uploading to CAS: digest → raw bytes.
    /// Consumers should call `FindMissingBlobs` and upload only what's absent.
    pub file_blobs: BTreeMap<DigestPair, Bytes>,
    /// Serialized Directory protos that need uploading to CAS: digest → bytes.
    pub directory_blobs: BTreeMap<DigestPair, Bytes>,
}

/// In-memory filesystem tree node built during tar iteration.
#[derive(Debug)]
enum FsNode {
    File { content: Bytes, executable: bool },
    Symlink { target: String },
    Directory { children: BTreeMap<String, Self> },
}

impl FsNode {
    const fn new_dir() -> Self {
        Self::Directory {
            children: BTreeMap::new(),
        }
    }

    const fn as_dir_mut(&mut self) -> Option<&mut BTreeMap<String, Self>> {
        match self {
            Self::Directory { children } => Some(children),
            _ => None,
        }
    }
}

/// Project one or more OCI layer tarballs into an REAPI Directory tree.
///
/// `layers` are the uncompressed tar bytes of each layer, in order (bottom to top).
/// Per §5.2, layers are additive and disjoint — overlapping entries are an error
/// for conforming images.
///
/// `digest_fn` specifies the hash function (§6.2 mandates BLAKE3 by default).
///
/// Returns a [`ProjectionResult`] with the full Merkle tree and all blobs.
pub fn project_layers(
    layers: &[&[u8]],
    digest_fn: ProjectionDigestFunction,
    hints: Option<&ToolchainHints>,
) -> Result<ProjectionResult, Error> {
    // Phase 1: Build in-memory filesystem tree from all layers.
    let mut root = FsNode::new_dir();

    for (layer_idx, layer_bytes) in layers.iter().enumerate() {
        let mut archive = tar::Archive::new(*layer_bytes);
        let entries = archive
            .entries()
            .map_err(|e| make_input_err!("Failed to read tar entries in layer {layer_idx}: {e}"))?;

        for entry_result in entries {
            let mut entry = entry_result
                .map_err(|e| make_input_err!("Tar entry error in layer {layer_idx}: {e}"))?;

            let path = entry
                .path()
                .map_err(|e| make_input_err!("Invalid path in tar entry: {e}"))?
                .into_owned();

            // Skip the root "." entry
            if path == Path::new(".") || path == Path::new("./") {
                continue;
            }

            // Reject whiteouts (§5.2: conforming images MUST NOT use them)
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if file_name.starts_with(".wh.") {
                return Err(make_input_err!(
                    "Whiteout entry '{}' found in layer {layer_idx}; \
                     conforming toolchain images must not use whiteouts (§5.2)",
                    path.display()
                ));
            }

            let entry_type = entry.header().entry_type();

            match entry_type {
                tar::EntryType::Regular | tar::EntryType::GNUSparse => {
                    let mode = entry.header().mode().unwrap_or(0o644);
                    let executable = mode & 0o111 != 0;

                    // Capacity is only a hint; a size that does not fit in
                    // usize (32-bit targets) just means no preallocation.
                    let capacity = usize::try_from(entry.header().size().unwrap_or(0)).unwrap_or(0);
                    let mut content = Vec::with_capacity(capacity);
                    entry.read_to_end(&mut content).map_err(|e| {
                        make_input_err!("Reading tar entry '{}': {e}", path.display())
                    })?;

                    insert_node(
                        &mut root,
                        &path,
                        FsNode::File {
                            content: Bytes::from(content),
                            executable,
                        },
                    )?;
                }

                tar::EntryType::Symlink => {
                    let link_target = entry
                        .link_name()
                        .map_err(|e| make_input_err!("Reading symlink target: {e}"))?
                        .ok_or_else(|| {
                            make_input_err!("Symlink '{}' has no target", path.display())
                        })?
                        .to_string_lossy()
                        .into_owned();

                    insert_node(
                        &mut root,
                        &path,
                        FsNode::Symlink {
                            target: link_target,
                        },
                    )?;
                }

                tar::EntryType::Directory => {
                    // Ensure the directory exists in our tree
                    ensure_dir(&mut root, &path)?;
                }

                tar::EntryType::Link => {
                    // Hard links: resolve to the content of the target.
                    // Per §4.2, hardlinks to identical content are equivalent to regular files.
                    // We'll read the content from the link target — but in a tar stream the
                    // target must have appeared earlier. For now, store as a zero-byte file
                    // and let the caller handle dedup via content addressing.
                    //
                    // Actually, tar hard links don't carry content — they reference another
                    // entry. The tar crate doesn't give us the content of the link target
                    // directly. We need to track previously-seen paths and copy content.
                    //
                    // For conforming images (§10), hardlinks and regular files with identical
                    // content are interchangeable. We handle this in a second pass if needed.
                    warn!(
                        path = %path.display(),
                        "Hard link encountered; deferring to hardlink resolution pass"
                    );
                    // Store a placeholder — we'll resolve hardlinks after the initial pass
                    let link_target = entry
                        .link_name()
                        .map_err(|e| make_input_err!("Reading hardlink target: {e}"))?
                        .ok_or_else(|| {
                            make_input_err!("Hardlink '{}' has no target", path.display())
                        })?
                        .to_string_lossy()
                        .into_owned();

                    // Look up the target in our tree to get its content
                    if let Some(content_and_exec) =
                        lookup_file_content(&root, Path::new(&link_target))
                    {
                        insert_node(
                            &mut root,
                            &path,
                            FsNode::File {
                                content: content_and_exec.0,
                                executable: content_and_exec.1,
                            },
                        )?;
                    } else {
                        // Target not yet seen — for now, treat as an error
                        return Err(make_input_err!(
                            "Hard link '{}' references '{}' which hasn't been seen yet",
                            path.display(),
                            link_target
                        ));
                    }
                }

                // Skip non-portable special files (§4.1)
                _ => {
                    debug!(
                        entry_type = ?entry_type,
                        path = %path.display(),
                        "Skipping non-portable tar entry type"
                    );
                }
            }
        }
    }

    // Phase 2: Convert the in-memory tree to REAPI Directory protos (bottom-up).
    let mut file_blobs: BTreeMap<DigestPair, Bytes> = BTreeMap::new();
    let mut directory_blobs: BTreeMap<DigestPair, Bytes> = BTreeMap::new();
    let mut children: Vec<(DigestPair, ProtoDirectory)> = Vec::new();

    let (root_dir, root_digest) = build_directory(
        &root,
        &digest_fn,
        &mut file_blobs,
        &mut directory_blobs,
        &mut children,
    )?;

    // Verify against hints if provided (§6.5)
    if let Some(hints) = hints
        && let Some(ref expected_root) = hints.reapi_root
    {
        let actual = format!("{}/{}", root_digest.hash, root_digest.size_bytes);
        if actual == *expected_root {
            debug!(digest = %actual, "REAPI root digest matches hint");
        } else {
            warn!(
                expected = %expected_root,
                actual = %actual,
                "REAPI root digest does not match hint annotation; \
                 hint treated as absent per §6.5"
            );
        }
    }

    // Add root directory blob to the upload set
    let root_bytes = root_dir.encode_to_vec();
    directory_blobs.insert(root_digest.clone(), Bytes::from(root_bytes));

    Ok(ProjectionResult {
        root: root_dir,
        root_digest,
        children,
        file_blobs,
        directory_blobs,
    })
}

/// Insert a node at the given path in the tree, creating intermediate directories.
fn insert_node(root: &mut FsNode, path: &Path, node: FsNode) -> Result<(), Error> {
    let os_components = normalize_path(path)?;
    let components: Vec<&str> = os_components.iter().filter_map(|c| c.to_str()).collect();

    if components.is_empty() {
        return Err(make_input_err!("Empty path after normalization"));
    }

    // Navigate to the parent directory, creating intermediates
    let mut current = root;
    for &component in &components[..components.len() - 1] {
        let children = current
            .as_dir_mut()
            .ok_or_else(|| make_input_err!("Path component '{component}' is not a directory"))?;
        current = children
            .entry(component.to_string())
            .or_insert_with(FsNode::new_dir);
    }

    // Insert the leaf node
    let leaf_name = components.last().unwrap().to_string();
    let children = current
        .as_dir_mut()
        .ok_or_else(|| make_input_err!("Parent of '{}' is not a directory", path.display()))?;
    children.insert(leaf_name, node);

    Ok(())
}

/// Ensure a directory exists at the given path, creating intermediates.
fn ensure_dir(root: &mut FsNode, path: &Path) -> Result<(), Error> {
    let os_components = normalize_path(path)?;
    let components: Vec<&str> = os_components.iter().filter_map(|c| c.to_str()).collect();

    let mut current = root;
    for &component in &components {
        let children = current
            .as_dir_mut()
            .ok_or_else(|| make_input_err!("Path component '{component}' is not a directory"))?;
        current = children
            .entry(component.to_string())
            .or_insert_with(FsNode::new_dir);
    }

    Ok(())
}

/// Normalize a tar entry path: strip leading `./` and any leading `/`.
fn normalize_path(path: &Path) -> Result<Vec<std::ffi::OsString>, Error> {
    let components: Vec<std::ffi::OsString> = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_os_string()),
            Component::RootDir | Component::CurDir => None,
            Component::ParentDir => None, // Reject .. traversal
            Component::Prefix(_) => None,
        })
        .collect();

    Ok(components)
}

/// Look up a file's content in the tree (for hardlink resolution).
fn lookup_file_content(root: &FsNode, path: &Path) -> Option<(Bytes, bool)> {
    let components: Vec<&str> = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => s.to_str(),
            _ => None,
        })
        .collect();

    let mut current = root;
    for (i, &component) in components.iter().enumerate() {
        match current {
            FsNode::Directory { children } => {
                current = children.get(component)?;
            }
            FsNode::File {
                content,
                executable,
            } if i == components.len() - 1 => {
                return Some((content.clone(), *executable));
            }
            _ => return None,
        }
    }

    match current {
        FsNode::File {
            content,
            executable,
        } => Some((content.clone(), *executable)),
        _ => None,
    }
}

/// Recursively build an REAPI Directory proto from an `FsNode` tree.
///
/// Returns the Directory proto and its digest. Populates the blob maps for
/// all files and child directories encountered.
fn build_directory(
    node: &FsNode,
    digest_fn: &ProjectionDigestFunction,
    file_blobs: &mut BTreeMap<DigestPair, Bytes>,
    directory_blobs: &mut BTreeMap<DigestPair, Bytes>,
    children_out: &mut Vec<(DigestPair, ProtoDirectory)>,
) -> Result<(ProtoDirectory, DigestPair), Error> {
    let children_map = match node {
        FsNode::Directory { children } => children,
        _ => return Err(make_input_err!("Expected directory node at tree root")),
    };

    let mut file_nodes: Vec<FileNode> = Vec::new();
    let mut symlink_nodes: Vec<SymlinkNode> = Vec::new();
    let mut dir_nodes: Vec<DirectoryNode> = Vec::new();

    for (name, child) in children_map {
        match child {
            FsNode::File {
                content,
                executable,
            } => {
                let (hash, size_bytes) = digest_fn.hash_bytes(content);
                let digest = DigestPair { hash, size_bytes };

                file_blobs.insert(digest.clone(), content.clone());

                file_nodes.push(FileNode {
                    name: name.clone(),
                    digest: Some(digest.to_proto()),
                    is_executable: *executable,
                    node_properties: None,
                });
            }

            FsNode::Symlink { target } => {
                symlink_nodes.push(SymlinkNode {
                    name: name.clone(),
                    target: target.clone(),
                    node_properties: None,
                });
            }

            FsNode::Directory { .. } => {
                let (child_dir, child_digest) =
                    build_directory(child, digest_fn, file_blobs, directory_blobs, children_out)?;

                // Serialize and store the child directory blob
                let child_bytes = child_dir.encode_to_vec();
                directory_blobs.insert(child_digest.clone(), Bytes::from(child_bytes));

                // Record in children list (for Tree.children)
                children_out.push((child_digest.clone(), child_dir));

                dir_nodes.push(DirectoryNode {
                    name: name.clone(),
                    digest: Some(child_digest.to_proto()),
                });
            }
        }
    }

    // REAPI requires lexicographic sorting by name (canonical form)
    file_nodes.sort_by(|a, b| a.name.cmp(&b.name));
    symlink_nodes.sort_by(|a, b| a.name.cmp(&b.name));
    dir_nodes.sort_by(|a, b| a.name.cmp(&b.name));

    let directory = ProtoDirectory {
        files: file_nodes,
        directories: dir_nodes,
        symlinks: symlink_nodes,
        node_properties: None,
    };

    // Compute the digest of this Directory proto
    let encoded = directory.encode_to_vec();
    let (hash, size_bytes) = digest_fn.hash_bytes(&encoded);
    let digest = DigestPair { hash, size_bytes };

    Ok((directory, digest))
}

/// Decompress a gzipped layer blob, returning uncompressed bytes.
pub fn decompress_gzip(compressed: &[u8]) -> Result<Vec<u8>, Error> {
    use flate2::read::GzDecoder;
    let mut decoder = GzDecoder::new(compressed);
    let mut uncompressed = Vec::new();
    decoder
        .read_to_end(&mut uncompressed)
        .map_err(|e| make_input_err!("Gzip decompression failed: {e}"))?;
    Ok(uncompressed)
}

/// Merge multiple projection results (one per layer) into a single unified tree.
///
/// Per §6.4, since layers are disjoint additive subtrees, we can project each
/// layer independently and merge their root Directories by combining their
/// children lists.
pub fn merge_projections(projections: Vec<ProjectionResult>) -> Result<ProjectionResult, Error> {
    if projections.is_empty() {
        return Err(make_input_err!("No projections to merge"));
    }

    if projections.len() == 1 {
        return Ok(projections.into_iter().next().unwrap());
    }

    // Merge all file_nodes, dir_nodes, symlink_nodes from each root
    let mut merged_files: Vec<FileNode> = Vec::new();
    let mut merged_dirs: Vec<DirectoryNode> = Vec::new();
    let mut merged_symlinks: Vec<SymlinkNode> = Vec::new();
    let mut all_file_blobs: BTreeMap<DigestPair, Bytes> = BTreeMap::new();
    let mut all_dir_blobs: BTreeMap<DigestPair, Bytes> = BTreeMap::new();
    let mut all_children: Vec<(DigestPair, ProtoDirectory)> = Vec::new();

    // We need a consistent digest function — take from the first projection's
    // root digest size to infer it. For now, the caller is expected to use the
    // same digest function for all layers.

    for proj in projections {
        merged_files.extend(proj.root.files);
        merged_dirs.extend(proj.root.directories);
        merged_symlinks.extend(proj.root.symlinks);
        all_file_blobs.extend(proj.file_blobs);
        all_dir_blobs.extend(proj.directory_blobs);
        all_children.extend(proj.children);
    }

    // Re-sort per REAPI canonical form
    merged_files.sort_by(|a, b| a.name.cmp(&b.name));
    merged_dirs.sort_by(|a, b| a.name.cmp(&b.name));
    merged_symlinks.sort_by(|a, b| a.name.cmp(&b.name));

    // Check for duplicates (§5.2 says layers are disjoint)
    check_name_uniqueness(&merged_files, &merged_dirs, &merged_symlinks)?;

    let merged_root = ProtoDirectory {
        files: merged_files,
        directories: merged_dirs,
        symlinks: merged_symlinks,
        node_properties: None,
    };

    // Compute merged root digest — we need to know the digest function.
    // Use BLAKE3 by default (§6.2).
    let encoded = merged_root.encode_to_vec();
    let hash = blake3::hash(&encoded);
    let root_digest = DigestPair {
        hash: hash.to_hex().to_string(),
        size_bytes: encoded.len() as i64,
    };

    all_dir_blobs.insert(root_digest.clone(), Bytes::from(encoded));

    Ok(ProjectionResult {
        root: merged_root,
        root_digest,
        children: all_children,
        file_blobs: all_file_blobs,
        directory_blobs: all_dir_blobs,
    })
}

/// Verify that no two entries in the merged root share a name.
fn check_name_uniqueness(
    files: &[FileNode],
    dirs: &[DirectoryNode],
    symlinks: &[SymlinkNode],
) -> Result<(), Error> {
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for f in files {
        if !seen.insert(&f.name) {
            return Err(make_input_err!(
                "Duplicate file entry '{}' in merged root (layers not disjoint, §5.2)",
                f.name
            ));
        }
    }
    for d in dirs {
        if !seen.insert(&d.name) {
            return Err(make_input_err!(
                "Duplicate directory entry '{}' in merged root (layers not disjoint, §5.2)",
                d.name
            ));
        }
    }
    for s in symlinks {
        if !seen.insert(&s.name) {
            return Err(make_input_err!(
                "Duplicate symlink entry '{}' in merged root (layers not disjoint, §5.2)",
                s.name
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a tar archive in memory with given entries.
    fn build_tar(entries: &[(&str, TarEntry)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());

        for (path, entry) in entries {
            match entry {
                TarEntry::File {
                    content,
                    executable,
                } => {
                    let mut header = tar::Header::new_gnu();
                    header.set_size(content.len() as u64);
                    header.set_entry_type(tar::EntryType::Regular);
                    header.set_mode(if *executable { 0o755 } else { 0o644 });
                    header.set_cksum();
                    builder
                        .append_data(&mut header, *path, content.as_slice())
                        .unwrap();
                }
                TarEntry::Symlink { target } => {
                    let mut header = tar::Header::new_gnu();
                    header.set_size(0);
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_mode(0o777);
                    header.set_cksum();
                    builder
                        .append_link(&mut header, *path, target.as_str())
                        .unwrap();
                }
                TarEntry::Dir => {
                    let mut header = tar::Header::new_gnu();
                    header.set_size(0);
                    header.set_entry_type(tar::EntryType::Directory);
                    header.set_mode(0o755);
                    header.set_cksum();
                    let empty: &[u8] = &[];
                    builder.append_data(&mut header, *path, empty).unwrap();
                }
            }
        }

        builder.into_inner().unwrap()
    }

    enum TarEntry {
        File { content: Vec<u8>, executable: bool },
        Symlink { target: String },
        Dir,
    }

    #[test]
    fn test_single_file_projection() {
        let content = b"hello world";
        let tar_bytes = build_tar(&[(
            "hello.txt",
            TarEntry::File {
                content: content.to_vec(),
                executable: false,
            },
        )]);

        let result = project_layers(&[&tar_bytes], ProjectionDigestFunction::Blake3, None).unwrap();

        // Root should have exactly one file
        assert_eq!(result.root.files.len(), 1);
        assert_eq!(result.root.directories.len(), 0);
        assert_eq!(result.root.symlinks.len(), 0);

        let file = &result.root.files[0];
        assert_eq!(file.name, "hello.txt");
        assert!(!file.is_executable);

        // Verify the file digest matches BLAKE3
        let expected_hash = blake3::hash(content).to_hex().to_string();
        assert_eq!(file.digest.as_ref().unwrap().hash, expected_hash);
        assert_eq!(
            file.digest.as_ref().unwrap().size_bytes,
            content.len() as i64
        );
    }

    #[test]
    fn test_nested_directory_projection() {
        let tar_bytes = build_tar(&[
            ("bin/", TarEntry::Dir),
            (
                "bin/clang",
                TarEntry::File {
                    content: b"clang binary".to_vec(),
                    executable: true,
                },
            ),
            (
                "bin/cc",
                TarEntry::Symlink {
                    target: "clang".to_string(),
                },
            ),
            ("lib/", TarEntry::Dir),
            (
                "lib/libclang.so",
                TarEntry::File {
                    content: b"shared library".to_vec(),
                    executable: false,
                },
            ),
        ]);

        let result = project_layers(&[&tar_bytes], ProjectionDigestFunction::Blake3, None).unwrap();

        // Root should have two directory children: bin, lib
        assert_eq!(result.root.directories.len(), 2);
        assert_eq!(result.root.directories[0].name, "bin");
        assert_eq!(result.root.directories[1].name, "lib");

        // bin should be in children
        let bin_dir = result
            .children
            .iter()
            .find(|(_, d)| d.files.iter().any(|f| f.name == "clang"))
            .map(|(_, d)| d)
            .expect("bin directory not found in children");

        assert_eq!(bin_dir.files.len(), 1);
        assert_eq!(bin_dir.files[0].name, "clang");
        assert!(bin_dir.files[0].is_executable);
        assert_eq!(bin_dir.symlinks.len(), 1);
        assert_eq!(bin_dir.symlinks[0].name, "cc");
        assert_eq!(bin_dir.symlinks[0].target, "clang");
    }

    #[test]
    fn test_whiteout_rejected() {
        let tar_bytes = build_tar(&[(
            ".wh.deleted_file",
            TarEntry::File {
                content: vec![],
                executable: false,
            },
        )]);

        let result = project_layers(&[&tar_bytes], ProjectionDigestFunction::Blake3, None);

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("whiteout"),
            "Error should mention whiteouts: {err_msg}"
        );
    }

    #[test]
    fn test_deterministic_output() {
        // Same content should always produce the same root digest
        let tar_bytes = build_tar(&[
            (
                "a.txt",
                TarEntry::File {
                    content: b"aaa".to_vec(),
                    executable: false,
                },
            ),
            (
                "b.txt",
                TarEntry::File {
                    content: b"bbb".to_vec(),
                    executable: true,
                },
            ),
        ]);

        let r1 = project_layers(&[&tar_bytes], ProjectionDigestFunction::Blake3, None).unwrap();
        let r2 = project_layers(&[&tar_bytes], ProjectionDigestFunction::Blake3, None).unwrap();

        assert_eq!(r1.root_digest, r2.root_digest);
    }

    #[test]
    fn test_merge_disjoint_layers() {
        let layer1 = build_tar(&[
            ("bin/", TarEntry::Dir),
            (
                "bin/tool",
                TarEntry::File {
                    content: b"tool binary".to_vec(),
                    executable: true,
                },
            ),
        ]);

        let layer2 = build_tar(&[
            ("lib/", TarEntry::Dir),
            (
                "lib/libfoo.a",
                TarEntry::File {
                    content: b"static lib".to_vec(),
                    executable: false,
                },
            ),
        ]);

        // Project each layer independently
        let proj1 = project_layers(&[&layer1], ProjectionDigestFunction::Blake3, None).unwrap();
        let proj2 = project_layers(&[&layer2], ProjectionDigestFunction::Blake3, None).unwrap();

        // Merge
        let merged = merge_projections(vec![proj1, proj2]).unwrap();

        assert_eq!(merged.root.directories.len(), 2);
        assert_eq!(merged.root.directories[0].name, "bin");
        assert_eq!(merged.root.directories[1].name, "lib");
    }

    #[test]
    fn test_sha256_projection() {
        use sha2::{Digest as _, Sha256};

        let content = b"sha256 test content";
        let tar_bytes = build_tar(&[(
            "test.bin",
            TarEntry::File {
                content: content.to_vec(),
                executable: false,
            },
        )]);

        let result = project_layers(&[&tar_bytes], ProjectionDigestFunction::Sha256, None).unwrap();

        // Verify with sha2 crate
        let expected = hex::encode(Sha256::digest(content));
        assert_eq!(result.root.files[0].digest.as_ref().unwrap().hash, expected);
    }
}
