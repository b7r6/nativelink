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

//! Experimental, bounded preparation of a typed REAPI output closure.
//!
//! This accepts only the local schema's canonical prost encoding. The local
//! REAPI Digest contract already requires canonical hashed messages; acceptance
//! here does not establish general REAPI compatibility. Original physical bytes are
//! verified before decoding; a different encoding is rejected, never normalized
//! into a different hashed identity. Raw blobs, inline streams, and `Any.value`
//! are opaque. `Tree` directories are embedded records, not separate CAS objects.
//!
//! Preparation is invisible until complete. The result is evidence about exact
//! generations read from a caller-supplied catalogue snapshot, not protection,
//! a retirement fence, publication acceptance, or an authority transaction. A
//! generation can disappear immediately after verification. No runtime factory
//! uses this module. Like the physical adapter, its bounded I/O is synchronous.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::{self, Debug, Formatter};

use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, NodeProperties, Tree,
};
use prost::Message;

use crate::physical_filesystem::{
    Algorithm, Failure as PhysicalFailure, ObjectId, PhysicalFilesystem, PhysicalKey,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Invalid(&'static str),
    UnsupportedEncoding,
    Capacity(&'static str),
    MissingBinding,
    Physical(PhysicalFailure),
    Incomplete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Binding {
    pub object: ObjectId,
    pub key: PhysicalKey,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Role {
    ActionResult,
    Blob,
    Directory,
    Tree,
}

/// Additional caller-selected retention roots, never inferred from provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedRoot {
    pub object: ObjectId,
    pub role: Role,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EdgeKind {
    OutputFile,
    OutputTree,
    Stdout,
    Stderr,
    File,
    Directory,
    /// Target bytes are embedded in the source Tree, not a physical CAS edge.
    EmbeddedDirectory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Edge {
    pub source: ObjectId,
    pub embedded_directory: Option<ObjectId>,
    pub target: ObjectId,
    pub kind: EdgeKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedObject {
    pub binding: Binding,
    pub roles: BTreeSet<Role>,
}

/// Complete evidence only. The revision is echoed, not validated by this module.
#[derive(Debug, Eq, PartialEq)]
pub struct PreparedClosure {
    pub catalogue_revision: u64,
    pub action_result: ObjectId,
    pub retained: Vec<RetainedRoot>,
    pub objects: Vec<PreparedObject>,
    pub edges: Vec<Edge>,
    pub read_bytes: u64,
}

/// Independent finite caps; defaults are experiment sizes, not workload claims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub catalogue_objects: usize,
    pub physical_objects: usize,
    pub queued_tasks: usize,
    pub retained_roots: usize,
    pub read_bytes: u64,
    pub message_bytes: usize,
    pub wire_fields: usize,
    pub messages: usize,
    pub edges: usize,
    pub path_bytes: usize,
    pub depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            catalogue_objects: 4096,
            physical_objects: 4096,
            queued_tasks: 8192,
            retained_roots: 128,
            read_bytes: 256 * 1024 * 1024,
            message_bytes: 4 * 1024 * 1024,
            wire_fields: 65_536,
            messages: 16_384,
            edges: 32_768,
            path_bytes: 4096,
            depth: 128,
        }
    }
}

impl Limits {
    fn validate(self) -> Result<(), Failure> {
        if [
            self.catalogue_objects,
            self.physical_objects,
            self.queued_tasks,
            self.retained_roots,
            self.wire_fields,
            self.messages,
            self.edges,
            self.path_bytes,
        ]
        .into_iter()
        .any(|count| count == 0 || count > 65_536)
            || self.read_bytes == 0
            || self.read_bytes > 1024 * 1024 * 1024
            || self.message_bytes == 0
            || self.message_bytes > 16 * 1024 * 1024
            || self.depth == 0
            || self.depth > 256
        {
            return Err(Failure::Invalid("limits"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Id(u8, [u8; 32], u64);

impl From<ObjectId> for Id {
    fn from(object: ObjectId) -> Self {
        Self(
            match object.algorithm {
                Algorithm::Sha256 => 1,
                Algorithm::Blake3 => 2,
            },
            object.hash,
            object.size,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Mode {
    Result,
    Blob,
    Directory,
    Tree,
    TopologicalTree,
}

impl Mode {
    const fn role(self) -> Role {
        match self {
            Self::Result => Role::ActionResult,
            Self::Blob => Role::Blob,
            Self::Directory => Role::Directory,
            Self::Tree | Self::TopologicalTree => Role::Tree,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectoryId {
    tree: Option<Id>,
    directory: Id,
}

#[derive(Debug)]
struct DirectoryInfo {
    children: Vec<(DirectoryId, usize)>,
    height: usize,
    path: usize,
}

#[derive(Clone, Copy, Debug)]
enum Root {
    Directory(DirectoryId),
    Tree(Id),
}

/// No accessor exposes partial objects or edges. Any failed advance is terminal.
pub struct Preparation {
    store: PhysicalFilesystem,
    algorithm: Algorithm,
    limits: Limits,
    catalogue_revision: u64,
    catalogue: BTreeMap<Id, Binding>,
    action_result: ObjectId,
    retained: Vec<RetainedRoot>,
    pending: VecDeque<(ObjectId, Mode)>,
    scheduled: BTreeSet<(Id, Mode)>,
    objects: BTreeMap<PhysicalKey, PreparedObject>,
    edges: Vec<Edge>,
    directories: BTreeMap<DirectoryId, DirectoryInfo>,
    trees: BTreeMap<Id, DirectoryId>,
    roots: Vec<(Root, usize, usize)>,
    read_bytes: u64,
    fields: usize,
    messages: usize,
    complete: bool,
    failure: Option<Failure>,
}

impl Debug for Preparation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Preparation")
            .field("pending_tasks", &self.pending.len())
            .field("read_bytes", &self.read_bytes)
            .field("complete", &self.complete)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

impl Preparation {
    pub fn new(
        store: PhysicalFilesystem,
        catalogue_revision: u64,
        catalogue: &[Binding],
        action_result: ObjectId,
        retained: &[RetainedRoot],
        limits: Limits,
    ) -> Result<Self, Failure> {
        limits.validate()?;
        if catalogue_revision == 0 {
            return Err(Failure::Invalid("catalogue revision"));
        }
        if catalogue.len() > limits.catalogue_objects || retained.len() > limits.retained_roots {
            return Err(Failure::Capacity("catalogue or retained roots"));
        }
        let mut bindings = BTreeMap::new();
        let mut keys = BTreeSet::new();
        for &binding in catalogue {
            if binding.object.algorithm != action_result.algorithm
                || bindings.insert(binding.object.into(), binding).is_some()
                || !keys.insert(binding.key)
            {
                return Err(Failure::Invalid("ambiguous catalogue or digest function"));
            }
        }
        let mut preparation = Self {
            store,
            algorithm: action_result.algorithm,
            limits,
            catalogue_revision,
            catalogue: bindings,
            action_result,
            retained: retained.to_vec(),
            pending: VecDeque::new(),
            scheduled: BTreeSet::new(),
            objects: BTreeMap::new(),
            edges: Vec::new(),
            directories: BTreeMap::new(),
            trees: BTreeMap::new(),
            roots: Vec::new(),
            read_bytes: 0,
            fields: 0,
            messages: 0,
            complete: false,
            failure: None,
        };
        preparation.schedule(action_result, Mode::Result)?;
        for root in retained {
            let mode = match root.role {
                Role::Blob => Mode::Blob,
                Role::Directory => {
                    preparation.roots.push((
                        Root::Directory(DirectoryId {
                            tree: None,
                            directory: root.object.into(),
                        }),
                        0,
                        0,
                    ));
                    Mode::Directory
                }
                Role::Tree => {
                    preparation
                        .roots
                        .push((Root::Tree(root.object.into()), 0, 0));
                    Mode::Tree
                }
                Role::ActionResult => return Err(Failure::Invalid("retained root role")),
            };
            preparation.schedule(root.object, mode)?;
        }
        Ok(preparation)
    }

    /// Each step reads one object in one role, or performs the final linear graph
    /// pass. A decode is bounded by message bytes and a pre-decode field/message
    /// census. The final pass is bounded by total messages and edges, never the
    /// number of expanded directory paths. At most 1024 steps per call.
    pub fn advance(&mut self, steps: usize) -> Result<bool, Failure> {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        let result = self.advance_inner(steps);
        if let Err(failure) = result {
            self.failure = Some(failure);
        }
        result
    }

    fn advance_inner(&mut self, steps: usize) -> Result<bool, Failure> {
        if steps == 0 || steps > 1024 {
            return Err(Failure::Invalid("batch steps"));
        }
        for _ in 0..steps {
            if self.complete {
                break;
            }
            if let Some((object, mode)) = self.pending.pop_front() {
                self.process(object, mode)?;
            } else {
                self.check_graph()?;
                self.complete = true;
            }
        }
        Ok(self.complete)
    }

    pub fn finish(self) -> Result<PreparedClosure, Failure> {
        if let Some(failure) = self.failure {
            return Err(failure);
        }
        if !self.complete {
            return Err(Failure::Incomplete);
        }
        Ok(PreparedClosure {
            catalogue_revision: self.catalogue_revision,
            action_result: self.action_result,
            retained: self.retained,
            objects: self.objects.into_values().collect(),
            edges: self.edges,
            read_bytes: self.read_bytes,
        })
    }

    fn schedule(&mut self, object: ObjectId, mode: Mode) -> Result<(), Failure> {
        if object.algorithm != self.algorithm {
            return Err(Failure::Invalid("digest function"));
        }
        let binding = *self
            .catalogue
            .get(&object.into())
            .ok_or(Failure::MissingBinding)?;
        if self.scheduled.contains(&(object.into(), mode)) {
            return Ok(());
        }
        // Bound the lifetime set as well as the live queue; role changes must
        // never bypass validation because the same bytes were seen as a Blob.
        if self.scheduled.len() >= self.limits.queued_tasks {
            return Err(Failure::Capacity("typed tasks"));
        }
        if !self.objects.contains_key(&binding.key)
            && self.objects.len() >= self.limits.physical_objects
        {
            return Err(Failure::Capacity("physical objects"));
        }
        self.objects
            .entry(binding.key)
            .or_insert_with(|| PreparedObject {
                binding,
                roles: BTreeSet::new(),
            })
            .roles
            .insert(mode.role());
        self.scheduled.insert((object.into(), mode));
        self.pending.push_back((object, mode));
        Ok(())
    }

    fn process(&mut self, object: ObjectId, mode: Mode) -> Result<(), Failure> {
        let total = self
            .read_bytes
            .checked_add(object.size)
            .ok_or(Failure::Capacity("read bytes"))?;
        if total > self.limits.read_bytes {
            return Err(Failure::Capacity("read bytes"));
        }
        if mode != Mode::Blob && object.size > self.limits.message_bytes as u64 {
            return Err(Failure::Capacity("message bytes"));
        }
        self.read_bytes = total;
        let binding = self.catalogue[&object.into()];
        let read = self
            .store
            .read(binding.key, object, 0, (mode == Mode::Blob).then_some(0))
            .map_err(Failure::Physical)?;
        match mode {
            Mode::Blob => Ok(()),
            Mode::Result => {
                let result = self.decode::<ActionResult>(&read.bytes, Schema::Result)?;
                self.result(&result)
            }
            Mode::Directory => {
                let directory = self.decode::<Directory>(&read.bytes, Schema::Directory)?;
                self.directory(object, object, None, &directory)
            }
            Mode::Tree | Mode::TopologicalTree => {
                let tree = self.decode::<Tree>(&read.bytes, Schema::Tree)?;
                self.tree(object, tree, mode == Mode::TopologicalTree)
            }
        }
    }

    fn decode<T: Message + Default>(&mut self, bytes: &[u8], schema: Schema) -> Result<T, Failure> {
        census(
            bytes,
            schema,
            &mut self.fields,
            &mut self.messages,
            self.limits,
        )?;
        let message = T::decode(bytes).map_err(|_| Failure::Invalid("protobuf"))?;
        if message.encode_to_vec() != bytes {
            return Err(Failure::UnsupportedEncoding);
        }
        Ok(message)
    }

    fn digest(&self, digest: Option<&Digest>) -> Result<ObjectId, Failure> {
        let digest = digest.ok_or(Failure::Invalid("missing digest"))?;
        if digest.size_bytes < 0
            || digest.hash.len() != 64
            || !digest
                .hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(Failure::Invalid("digest"));
        }
        let mut hash = [0; 32];
        hex::decode_to_slice(&digest.hash, &mut hash).map_err(|_| Failure::Invalid("digest"))?;
        Ok(ObjectId {
            algorithm: self.algorithm,
            hash,
            size: digest.size_bytes.cast_unsigned(),
        })
    }

    fn edge(&mut self, edge: Edge) -> Result<(), Failure> {
        if self.edges.len() >= self.limits.edges {
            return Err(Failure::Capacity("edges"));
        }
        self.edges.push(edge);
        Ok(())
    }

    fn result(&mut self, result: &ActionResult) -> Result<(), Failure> {
        let mut paths = BTreeSet::new();
        for file in &result.output_files {
            path(&file.path, false, self.limits)?;
            if !paths.insert(file.path.as_str()) {
                return Err(Failure::Invalid("duplicate output path"));
            }
            properties(file.node_properties.as_ref())?;
            let target = self.digest(file.digest.as_ref())?;
            self.inline(&file.contents, target)?;
            self.schedule(target, Mode::Blob)?;
            self.edge(Edge {
                source: self.action_result,
                embedded_directory: None,
                target,
                kind: EdgeKind::OutputFile,
            })?;
        }
        for directory in &result.output_directories {
            let (length, depth) = path(&directory.path, true, self.limits)?;
            if !paths.insert(directory.path.as_str()) {
                return Err(Failure::Invalid("duplicate output path"));
            }
            let target = self.digest(directory.tree_digest.as_ref())?;
            self.schedule(
                target,
                if directory.is_topologically_sorted {
                    Mode::TopologicalTree
                } else {
                    Mode::Tree
                },
            )?;
            self.roots.push((Root::Tree(target.into()), length, depth));
            self.edge(Edge {
                source: self.action_result,
                embedded_directory: None,
                target,
                kind: EdgeKind::OutputTree,
            })?;
        }
        let mut symlinks = BTreeMap::new();
        for (group, links) in [
            &result.output_symlinks,
            &result.output_file_symlinks,
            &result.output_directory_symlinks,
        ]
        .into_iter()
        .enumerate()
        {
            let mut names = BTreeSet::new();
            for link in links {
                path(&link.path, false, self.limits)?;
                symlink(&link.target, self.limits)?;
                properties(link.node_properties.as_ref())?;
                if !names.insert(link.path.as_str()) {
                    return Err(Failure::Invalid("duplicate symlink in output list"));
                }
                if let Some((previous, previous_group, aliased)) =
                    symlinks.get_mut(link.path.as_str())
                {
                    // Only modern/deprecated duplication is a compatibility alias.
                    if *previous != link || *previous_group != 0 || group == 0 || *aliased {
                        return Err(Failure::Invalid("conflicting symlink aliases"));
                    }
                    *aliased = true;
                } else {
                    if !paths.insert(link.path.as_str()) {
                        return Err(Failure::Invalid("duplicate output path"));
                    }
                    symlinks.insert(link.path.as_str(), (link, group, false));
                }
            }
        }
        for (raw, digest, kind) in [
            (
                &result.stdout_raw,
                result.stdout_digest.as_ref(),
                EdgeKind::Stdout,
            ),
            (
                &result.stderr_raw,
                result.stderr_digest.as_ref(),
                EdgeKind::Stderr,
            ),
        ] {
            if let Some(digest) = digest {
                let target = self.digest(Some(digest))?;
                self.inline(raw, target)?;
                self.schedule(target, Mode::Blob)?;
                self.edge(Edge {
                    source: self.action_result,
                    embedded_directory: None,
                    target,
                    kind,
                })?;
            }
        }
        Ok(())
    }

    fn inline(&self, bytes: &[u8], object: ObjectId) -> Result<(), Failure> {
        // Empty proto3 bytes cannot distinguish absent data from an inline empty
        // stream. A declared digest remains a required physical edge either way.
        if !bytes.is_empty() && ObjectId::for_bytes(self.algorithm, bytes) != object {
            return Err(Failure::Invalid("inline bytes disagree with digest"));
        }
        Ok(())
    }

    fn tree(&mut self, object: ObjectId, tree: Tree, topological: bool) -> Result<(), Failure> {
        let root = tree.root.ok_or(Failure::Invalid("missing Tree root"))?;
        let directories: Vec<_> = std::iter::once(root).chain(tree.children).collect();
        let mut index = BTreeMap::new();
        let mut ids = Vec::new();
        for (position, directory) in directories.iter().enumerate() {
            let id = ObjectId::for_bytes(self.algorithm, &directory.encode_to_vec());
            if position > 0 && id == ids[0] {
                return Err(Failure::Invalid("Tree root repeated in children"));
            }
            let entry = index.entry(Id::from(id));
            match entry {
                std::collections::btree_map::Entry::Occupied(existing) => {
                    if topological || directories[*existing.get()] != *directory {
                        return Err(Failure::Invalid("duplicate topological Tree directory"));
                    }
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(position);
                }
            }
            ids.push(id);
        }
        let root = DirectoryId {
            tree: Some(object.into()),
            directory: ids[0].into(),
        };
        let mut reached = BTreeSet::from([root.directory]);
        let mut pending = vec![root.directory];
        while let Some(current) = pending.pop() {
            let position = index[&current];
            for child in &directories[position].directories {
                let child_id = Id::from(self.digest(child.digest.as_ref())?);
                let child_position = *index
                    .get(&child_id)
                    .ok_or(Failure::Invalid("missing embedded directory"))?;
                if topological && child_position <= position {
                    return Err(Failure::Invalid("Tree topological order"));
                }
                if reached.insert(child_id) {
                    pending.push(child_id);
                }
            }
        }
        if reached.len() != index.len() {
            return Err(Failure::Invalid("unreachable Tree directory"));
        }
        self.trees.insert(object.into(), root);
        for (directory, id) in directories.into_iter().zip(ids) {
            self.directory(object, id, Some(object.into()), &directory)?;
        }
        Ok(())
    }

    fn directory(
        &mut self,
        source: ObjectId,
        object: ObjectId,
        tree: Option<Id>,
        directory: &Directory,
    ) -> Result<(), Failure> {
        let identity = DirectoryId {
            tree,
            directory: object.into(),
        };
        if self.directories.contains_key(&identity) {
            return Ok(());
        }
        properties(directory.node_properties.as_ref())?;
        let mut names = BTreeSet::new();
        let mut info = DirectoryInfo {
            children: Vec::new(),
            height: 0,
            path: 0,
        };
        let embedded_directory = tree.map(|_| object);
        let mut previous = None;
        for file in &directory.files {
            name(&file.name, &mut previous, &mut names, self.limits)?;
            properties(file.node_properties.as_ref())?;
            let target = self.digest(file.digest.as_ref())?;
            self.schedule(target, Mode::Blob)?;
            self.edge(Edge {
                source,
                embedded_directory,
                target,
                kind: EdgeKind::File,
            })?;
            info.height = 1;
            info.path = info.path.max(file.name.len());
        }
        previous = None;
        for child in &directory.directories {
            name(&child.name, &mut previous, &mut names, self.limits)?;
            let target = self.digest(child.digest.as_ref())?;
            let kind = if tree.is_some() {
                EdgeKind::EmbeddedDirectory
            } else {
                self.schedule(target, Mode::Directory)?;
                EdgeKind::Directory
            };
            self.edge(Edge {
                source,
                embedded_directory,
                target,
                kind,
            })?;
            info.children.push((
                DirectoryId {
                    tree,
                    directory: target.into(),
                },
                child.name.len(),
            ));
        }
        previous = None;
        for link in &directory.symlinks {
            name(&link.name, &mut previous, &mut names, self.limits)?;
            symlink(&link.target, self.limits)?;
            properties(link.node_properties.as_ref())?;
            info.height = 1;
            info.path = info.path.max(link.name.len());
        }
        self.directories.insert(identity, info);
        Ok(())
    }

    fn check_graph(&self) -> Result<(), Failure> {
        let mut metrics: BTreeMap<DirectoryId, (usize, usize)> = BTreeMap::new();
        for &(root, prefix, depth) in &self.roots {
            let root = match root {
                Root::Directory(root) => root,
                Root::Tree(tree) => *self
                    .trees
                    .get(&tree)
                    .ok_or(Failure::Invalid("missing prepared Tree"))?,
            };
            let mut active = BTreeSet::new();
            let mut stack = vec![(root, false)];
            while let Some((current, finishing)) = stack.pop() {
                if metrics.contains_key(&current) {
                    continue;
                }
                let directory = self
                    .directories
                    .get(&current)
                    .ok_or(Failure::Invalid("missing prepared directory"))?;
                if finishing {
                    let mut height = directory.height;
                    let mut length = directory.path;
                    for &(child, name_length) in &directory.children {
                        let (child_height, child_length) = metrics[&child];
                        height = height.max(child_height + 1);
                        length = length.max(joined_length(name_length, child_length));
                    }
                    if height > self.limits.depth || length > self.limits.path_bytes {
                        return Err(Failure::Capacity("directory depth or path"));
                    }
                    active.remove(&current);
                    metrics.insert(current, (height, length));
                } else {
                    if !active.insert(current) {
                        return Err(Failure::Invalid("directory cycle"));
                    }
                    // Fail before constructing an unbounded chain, even though
                    // the graph's total size is independently message-bounded.
                    if active.len() > self.limits.depth + 1 {
                        return Err(Failure::Capacity("directory depth"));
                    }
                    stack.push((current, true));
                    for &(child, _) in directory.children.iter().rev() {
                        stack.push((child, false));
                    }
                }
            }
            let (height, length) = metrics[&root];
            if depth + height > self.limits.depth
                || joined_length(prefix, length) > self.limits.path_bytes
            {
                return Err(Failure::Capacity("output depth or path"));
            }
        }
        Ok(())
    }
}

const fn joined_length(prefix: usize, suffix: usize) -> usize {
    prefix + suffix + if prefix > 0 && suffix > 0 { 1 } else { 0 }
}

fn properties(properties: Option<&NodeProperties>) -> Result<(), Failure> {
    if let Some(properties) = properties
        && properties
            .properties
            .windows(2)
            .any(|pair| pair[0].name > pair[1].name)
    {
        return Err(Failure::Invalid("node property order"));
    }
    Ok(())
}

fn path(value: &str, empty: bool, limits: Limits) -> Result<(usize, usize), Failure> {
    if value.is_empty() && empty {
        return Ok((0, 0));
    }
    if value.len() > limits.path_bytes {
        return Err(Failure::Capacity("path bytes"));
    }
    let mut depth = 0;
    for segment in value.split('/') {
        if segment.is_empty() || segment == "." || segment == ".." || segment.contains('\0') {
            return Err(Failure::Invalid("relative output path"));
        }
        depth += 1;
    }
    if depth > limits.depth {
        return Err(Failure::Capacity("path depth"));
    }
    Ok((value.len(), depth))
}

fn name<'a>(
    value: &'a str,
    previous: &mut Option<&'a str>,
    names: &mut BTreeSet<&'a str>,
    limits: Limits,
) -> Result<(), Failure> {
    if path(value, false, limits)?.1 != 1 {
        return Err(Failure::Invalid("directory child name"));
    }
    if previous.is_some_and(|previous| previous >= value) || !names.insert(value) {
        return Err(Failure::Invalid("directory child order or duplicate"));
    }
    *previous = Some(value);
    Ok(())
}

fn symlink(target: &str, limits: Limits) -> Result<(), Failure> {
    if target.is_empty() || target.contains('\0') {
        return Err(Failure::Invalid("symlink target"));
    }
    if target.len() > limits.path_bytes {
        return Err(Failure::Capacity("symlink target bytes"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Schema {
    Result,
    OutputFile,
    OutputDirectory,
    Symlink,
    Tree,
    Directory,
    File,
    DirectoryNode,
    Properties,
    Metadata,
    Leaf,
}

impl Schema {
    /// Only schema-defined message fields are traversed. In particular, Leaf
    /// includes Digest, Timestamp, Duration, `UInt32Value`, `NodeProperty` and Any;
    /// none contains another typed message or a strong retention edge.
    const fn nested(self, field: u64) -> Option<Self> {
        match (self, field) {
            (Self::Result, 2) => Some(Self::OutputFile),
            (Self::Result, 3) => Some(Self::OutputDirectory),
            (Self::Result, 9) => Some(Self::Metadata),
            (Self::Result, 10..=12) | (Self::Directory, 3) => Some(Self::Symlink),
            (Self::Tree, 1 | 2) => Some(Self::Directory),
            (Self::Directory, 1) => Some(Self::File),
            (Self::Directory, 2) => Some(Self::DirectoryNode),
            (Self::OutputFile, 7) | (Self::Symlink, 4) | (Self::Directory, 5) | (Self::File, 6) => {
                Some(Self::Properties)
            }
            (Self::Result, 6 | 8)
            | (Self::OutputFile | Self::File | Self::DirectoryNode, 2)
            | (Self::OutputDirectory, 3)
            | (Self::Properties, 1..=3)
            | (Self::Metadata, 2..=12) => Some(Self::Leaf),
            _ => None,
        }
    }
}

/// Count structured allocations and fields before prost allocates repeated
/// messages. Recursion follows the finite schema (at most six nested messages),
/// never arbitrary bytes, directory graph depth, or user-controlled type URLs.
fn census(
    mut bytes: &[u8],
    schema: Schema,
    fields: &mut usize,
    messages: &mut usize,
    limits: Limits,
) -> Result<(), Failure> {
    if *messages >= limits.messages {
        return Err(Failure::Capacity("decoded messages"));
    }
    *messages += 1;
    while !bytes.is_empty() {
        if *fields >= limits.wire_fields {
            return Err(Failure::Capacity("wire fields"));
        }
        *fields += 1;
        let tag = varint(&mut bytes)?;
        if tag >> 3 == 0 || tag >> 3 > 0x1fff_ffff {
            return Err(Failure::Invalid("protobuf tag"));
        }
        match tag & 7 {
            0 => {
                varint(&mut bytes)?;
            }
            1 => {
                take(&mut bytes, 8)?;
            }
            2 => {
                let length = usize::try_from(varint(&mut bytes)?)
                    .map_err(|_| Failure::Invalid("protobuf length"))?;
                let content = take(&mut bytes, length)?;
                if let Some(schema) = schema.nested(tag >> 3) {
                    census(content, schema, fields, messages, limits)?;
                }
            }
            5 => {
                take(&mut bytes, 4)?;
            }
            _ => return Err(Failure::UnsupportedEncoding),
        }
    }
    Ok(())
}

fn varint(bytes: &mut &[u8]) -> Result<u64, Failure> {
    let mut value = 0;
    for shift in (0..70).step_by(7) {
        let byte = take(bytes, 1)?[0];
        if shift == 63 && byte > 1 {
            return Err(Failure::Invalid("protobuf varint"));
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            return Ok(value);
        }
    }
    Err(Failure::Invalid("protobuf varint"))
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], Failure> {
    let (content, rest) = bytes
        .split_at_checked(length)
        .ok_or(Failure::Invalid("truncated protobuf"))?;
    *bytes = rest;
    Ok(content)
}
