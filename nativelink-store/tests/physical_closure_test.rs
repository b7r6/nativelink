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

use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use bytes::Bytes;
use nativelink_proto::build::bazel::remote::execution::v2::{
    ActionResult, Digest, Directory, DirectoryNode, ExecutedActionMetadata, FileNode,
    NodeProperties, NodeProperty, OutputDirectory, OutputFile, OutputSymlink, SymlinkNode, Tree,
};
use nativelink_store::physical_closure::{
    Binding, EdgeKind, Failure, Limits, Preparation, PreparedClosure, RetainedRoot, Role,
};
use nativelink_store::physical_filesystem::{
    Algorithm, FailureKind, Incarnation, Limits as PhysicalLimits, ObjectId, PhysicalFilesystem,
    PhysicalKey,
};
use prost::Message;
use tempfile::TempDir;

struct Fixture {
    _directory: TempDir,
    store: PhysicalFilesystem,
    algorithm: Algorithm,
    bindings: Vec<Binding>,
}

impl Fixture {
    fn new(algorithm: Algorithm) -> Self {
        let directory = TempDir::new().unwrap();
        let store = PhysicalFilesystem::bootstrap(
            directory.path(),
            Incarnation::new([1; 16]).unwrap(),
            PhysicalLimits::default(),
        )
        .unwrap();
        Self {
            _directory: directory,
            store,
            algorithm,
            bindings: Vec::new(),
        }
    }

    fn put(&mut self, bytes: &[u8]) -> ObjectId {
        let object = ObjectId::for_bytes(self.algorithm, bytes);
        if self.bindings.iter().any(|binding| binding.object == object) {
            return object;
        }
        let key = PhysicalKey::new(
            Incarnation::new([1; 16]).unwrap(),
            self.bindings.len() as u64 + 1,
        )
        .unwrap();
        let mut stage = self.store.begin_write(key, 1, object).unwrap();
        stage.append(bytes).unwrap();
        stage.finish().unwrap();
        self.bindings.push(Binding { object, key });
        object
    }

    fn message(&mut self, message: &impl Message) -> ObjectId {
        self.put(&message.encode_to_vec())
    }

    fn directory_id(&self, directory: &Directory) -> ObjectId {
        ObjectId::for_bytes(self.algorithm, &directory.encode_to_vec())
    }

    fn prepare(
        &self,
        result: ObjectId,
        retained: &[RetainedRoot],
        limits: Limits,
    ) -> Result<Preparation, Failure> {
        Preparation::new(
            self.store.clone(),
            42,
            &self.bindings,
            result,
            retained,
            limits,
        )
    }

    fn run(
        &mut self,
        result: &ActionResult,
        retained: &[RetainedRoot],
        limits: Limits,
    ) -> Result<PreparedClosure, Failure> {
        let result = self.message(result);
        finish(self.prepare(result, retained, limits)?)
    }
}

fn finish(mut preparation: Preparation) -> Result<PreparedClosure, Failure> {
    while !preparation.advance(1)? {}
    preparation.finish()
}

fn digest(object: ObjectId) -> Digest {
    Digest {
        hash: hex::encode(object.hash),
        size_bytes: i64::try_from(object.size).unwrap(),
    }
}

fn file(name: &str, object: ObjectId) -> FileNode {
    FileNode {
        name: name.into(),
        digest: Some(digest(object)),
        ..FileNode::default()
    }
}

fn child(name: &str, object: ObjectId) -> DirectoryNode {
    DirectoryNode {
        name: name.into(),
        digest: Some(digest(object)),
    }
}

fn output_file(path: &str, object: ObjectId) -> OutputFile {
    OutputFile {
        path: path.into(),
        digest: Some(digest(object)),
        ..OutputFile::default()
    }
}

fn tree_result(object: ObjectId, topological: bool) -> ActionResult {
    ActionResult {
        output_directories: vec![OutputDirectory {
            path: "out".into(),
            tree_digest: Some(digest(object)),
            is_topologically_sorted: topological,
        }],
        ..ActionResult::default()
    }
}

#[test]
fn typed_edges_protect_tree_container_and_streams_without_parsing_arbitrary_blobs() {
    for algorithm in [Algorithm::Sha256, Algorithm::Blake3] {
        let mut fixture = Fixture::new(algorithm);
        // Valid Directory bytes with an invalid required FileNode digest. These
        // bytes must never be parsed just because a file or stream contains them.
        let opaque = Directory {
            files: vec![FileNode {
                name: "missing".into(),
                ..FileNode::default()
            }],
            ..Directory::default()
        }
        .encode_to_vec();
        let blob = fixture.put(&opaque);
        let unrelated = fixture.put(b"not rooted by catalogue membership");
        let leaf = Directory {
            files: vec![file("f", blob)],
            symlinks: vec![SymlinkNode {
                name: "s".into(),
                target: "/outside/../input".into(),
                ..SymlinkNode::default()
            }],
            ..Directory::default()
        };
        let embedded = fixture.directory_id(&leaf);
        let tree = fixture.message(&Tree {
            root: Some(Directory {
                directories: vec![child("d", embedded)],
                ..Directory::default()
            }),
            children: vec![leaf],
        });
        // Field 11 containing one empty Any, using the pinned generated type.
        let mut metadata = ExecutedActionMetadata::decode([0x5a, 0x00].as_slice()).unwrap();
        metadata.auxiliary_metadata[0].type_url = "type.googleapis.com/Directory".into();
        metadata.auxiliary_metadata[0].value.clone_from(&opaque);
        let mut result = tree_result(tree, true);
        // ActionResult output lists are explicitly unordered, unlike Directory.
        result.output_files = vec![output_file("z", blob), output_file("a", blob)];
        result.output_files[0].contents = Bytes::copy_from_slice(&opaque);
        result.stdout_raw = Bytes::copy_from_slice(&opaque);
        result.stderr_raw = opaque.into();
        result.stderr_digest = Some(digest(blob));
        result.execution_metadata = Some(metadata);
        let closure = fixture.run(&result, &[], Limits::default()).unwrap();
        assert_eq!(closure.catalogue_revision, 42);
        assert_eq!(closure.objects.len(), 3);
        assert!(closure.objects.iter().all(|object| {
            object.binding.object != embedded && object.binding.object != unrelated
        }));
        assert!(closure.edges.iter().any(|edge| {
            edge.kind == EdgeKind::EmbeddedDirectory
                && edge.source == tree
                && edge.target == embedded
        }));
        assert!(closure.edges.iter().any(|edge| {
            edge.kind == EdgeKind::File
                && edge.source == tree
                && edge.embedded_directory == Some(embedded)
                && edge.target == blob
        }));
        assert!(
            !closure
                .edges
                .iter()
                .any(|edge| edge.kind == EdgeKind::Stdout)
        );
    }
}

#[test]
fn optional_retained_roots_are_explicit_and_blob_role_cannot_hide_directory_edges() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let blob = fixture.put(b"required only by retained directory");
    let directory = fixture.message(&Directory {
        files: vec![file("f", blob)],
        ..Directory::default()
    });
    let result = ActionResult {
        output_files: vec![output_file("raw-directory-bytes", directory)],
        ..ActionResult::default()
    };
    let unretained = fixture.run(&result, &[], Limits::default()).unwrap();
    assert_eq!(unretained.objects.len(), 2);
    let retained = [RetainedRoot {
        object: directory,
        role: Role::Directory,
    }];
    let closure = fixture.run(&result, &retained, Limits::default()).unwrap();
    assert_eq!(closure.retained, retained);
    assert_eq!(closure.objects.len(), 3);
    let entry = closure
        .objects
        .iter()
        .find(|entry| entry.binding.object == directory)
        .unwrap();
    assert_eq!(
        entry.roles.iter().copied().collect::<Vec<_>>(),
        [Role::Blob, Role::Directory]
    );
    assert!(closure.edges.iter().any(|edge| {
        edge.source == directory && edge.target == blob && edge.kind == EdgeKind::File
    }));
    let missing = [RetainedRoot {
        object: ObjectId::for_bytes(Algorithm::Sha256, b"absent optional root once requested"),
        role: Role::Blob,
    }];
    assert_eq!(
        fixture.run(&result, &missing, Limits::default()),
        Err(Failure::MissingBinding)
    );
}

#[test]
fn partial_batches_are_invisible_and_failure_poisoning_survives_physical_repair() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let blob = fixture.put(b"late repair");
    let result = fixture.message(&ActionResult {
        output_files: vec![output_file("f", blob)],
        ..ActionResult::default()
    });
    let mut preparation = fixture.prepare(result, &[], Limits::default()).unwrap();
    assert!(!preparation.advance(1).unwrap());
    assert_eq!(preparation.finish(), Err(Failure::Incomplete));
    let binding = fixture.bindings[0];
    fixture.store.delete(binding.key, 1).unwrap();
    let mut preparation = fixture.prepare(result, &[], Limits::default()).unwrap();
    assert!(!preparation.advance(1).unwrap());
    let failure = preparation.advance(1).unwrap_err();
    assert!(matches!(failure, Failure::Physical(error) if error.kind == FailureKind::Missing));
    let mut stage = fixture.store.begin_write(binding.key, 2, blob).unwrap();
    stage.append(b"late repair").unwrap();
    stage.finish().unwrap();
    assert_eq!(preparation.advance(1024), Err(failure));
    assert_eq!(preparation.finish(), Err(failure));
    assert!(finish(fixture.prepare(result, &[], Limits::default()).unwrap()).is_ok());
}

#[test]
fn omitted_outputs_are_legal_but_present_missing_or_malformed_digests_are_not() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    assert_eq!(
        fixture
            .run(&ActionResult::default(), &[], Limits::default())
            .unwrap()
            .objects
            .len(),
        1
    );
    let blob = fixture.put(b"content");
    for bad in [
        None,
        Some(Digest {
            hash: "A".repeat(64),
            size_bytes: 7,
        }),
        Some(Digest {
            hash: "0".repeat(63),
            size_bytes: 7,
        }),
        Some(Digest {
            hash: "0".repeat(64),
            size_bytes: -1,
        }),
    ] {
        let result = ActionResult {
            output_files: vec![OutputFile {
                path: "f".into(),
                digest: bad,
                ..OutputFile::default()
            }],
            ..ActionResult::default()
        };
        assert!(matches!(
            fixture.run(&result, &[], Limits::default()),
            Err(Failure::Invalid(_))
        ));
    }
    let mut result = tree_result(blob, false);
    result.output_directories[0].tree_digest = None;
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("missing digest"))
    );
    let result = ActionResult {
        stdout_digest: Some(Digest {
            hash: "not a digest".into(),
            size_bytes: 0,
        }),
        ..ActionResult::default()
    };
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("digest"))
    );
}

#[test]
fn inline_bytes_are_checked_but_do_not_substitute_for_declared_cas_edges() {
    let mut fixture = Fixture::new(Algorithm::Blake3);
    let unbound = ObjectId::for_bytes(Algorithm::Blake3, b"inline");
    let result = ActionResult {
        output_files: vec![OutputFile {
            contents: Bytes::from_static(b"inline"),
            ..output_file("f", unbound)
        }],
        ..ActionResult::default()
    };
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::MissingBinding)
    );
    let blob = fixture.put(b"inline");
    assert!(fixture.run(&result, &[], Limits::default()).is_ok());
    for (stdout_raw, stderr_raw, contents) in [
        (
            b"mismatch".as_slice(),
            b"inline".as_slice(),
            b"inline".as_slice(),
        ),
        (
            b"inline".as_slice(),
            b"mismatch".as_slice(),
            b"inline".as_slice(),
        ),
        (
            b"inline".as_slice(),
            b"inline".as_slice(),
            b"mismatch".as_slice(),
        ),
    ] {
        let result = ActionResult {
            output_files: vec![OutputFile {
                contents: Bytes::copy_from_slice(contents),
                ..output_file("f", blob)
            }],
            stdout_raw: Bytes::copy_from_slice(stdout_raw),
            stdout_digest: Some(digest(blob)),
            stderr_raw: Bytes::copy_from_slice(stderr_raw),
            stderr_digest: Some(digest(blob)),
            ..ActionResult::default()
        };
        assert_eq!(
            fixture.run(&result, &[], Limits::default()),
            Err(Failure::Invalid("inline bytes disagree with digest"))
        );
    }
    let result = ActionResult {
        stdout_digest: Some(digest(blob)),
        ..ActionResult::default()
    };
    assert!(fixture.run(&result, &[], Limits::default()).is_ok());
    let empty = fixture.put(b"");
    let result = ActionResult {
        stderr_digest: Some(digest(empty)),
        ..ActionResult::default()
    };
    assert!(fixture.run(&result, &[], Limits::default()).is_ok());
}

#[test]
fn tree_reachability_missing_records_duplicates_and_topological_claims_are_checked() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let leaf = Directory::default();
    let leaf_id = fixture.directory_id(&leaf);
    let middle = Directory {
        directories: vec![child("leaf", leaf_id)],
        ..Directory::default()
    };
    let middle_id = fixture.directory_id(&middle);
    let root = Directory {
        directories: vec![child("middle", middle_id)],
        ..Directory::default()
    };
    for (tree, expected) in [
        (Tree::default(), "missing Tree root"),
        (
            Tree {
                root: Some(leaf.clone()),
                children: vec![leaf.clone()],
            },
            "Tree root repeated in children",
        ),
        (
            Tree {
                root: Some(root.clone()),
                children: vec![leaf.clone()],
            },
            "missing embedded directory",
        ),
        (
            Tree {
                root: Some(leaf.clone()),
                children: vec![middle.clone()],
            },
            "unreachable Tree directory",
        ),
    ] {
        let object = fixture.message(&tree);
        assert_eq!(
            fixture.run(&tree_result(object, false), &[], Limits::default()),
            Err(Failure::Invalid(expected))
        );
    }
    let duplicate = fixture.message(&Tree {
        root: Some(middle.clone()),
        children: vec![leaf.clone(), leaf.clone()],
    });
    assert!(
        fixture
            .run(&tree_result(duplicate, false), &[], Limits::default())
            .is_ok()
    );
    assert_eq!(
        fixture.run(&tree_result(duplicate, true), &[], Limits::default()),
        Err(Failure::Invalid("duplicate topological Tree directory"))
    );
    let reversed = fixture.message(&Tree {
        root: Some(root.clone()),
        children: vec![leaf.clone(), middle.clone()],
    });
    assert!(
        fixture
            .run(&tree_result(reversed, false), &[], Limits::default())
            .is_ok()
    );
    assert_eq!(
        fixture.run(&tree_result(reversed, true), &[], Limits::default()),
        Err(Failure::Invalid("Tree topological order"))
    );
    let mut mixed_claims = tree_result(reversed, false);
    mixed_claims.output_directories.push(OutputDirectory {
        path: "strict".into(),
        tree_digest: Some(digest(reversed)),
        is_topologically_sorted: true,
    });
    assert_eq!(
        fixture.run(&mixed_claims, &[], Limits::default()),
        Err(Failure::Invalid("Tree topological order"))
    );
    let ordered = fixture.message(&Tree {
        root: Some(root),
        children: vec![middle, leaf],
    });
    assert!(
        fixture
            .run(&tree_result(ordered, true), &[], Limits::default())
            .is_ok()
    );
}

#[test]
fn directory_names_cross_kind_uniqueness_order_properties_and_missing_files_are_checked() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let blob = fixture.put(b"f");
    let leaf = Directory::default();
    let leaf_id = fixture.directory_id(&leaf);
    let bad_directories = [
        Directory {
            files: vec![file("z", blob), file("a", blob)],
            ..Directory::default()
        },
        Directory {
            files: vec![file("a", blob), file("a", blob)],
            ..Directory::default()
        },
        Directory {
            files: vec![file("a/b", blob)],
            ..Directory::default()
        },
        Directory {
            files: vec![file("..", blob)],
            ..Directory::default()
        },
        Directory {
            files: vec![file("", blob)],
            ..Directory::default()
        },
        Directory {
            files: vec![FileNode {
                name: "f".into(),
                ..FileNode::default()
            }],
            ..Directory::default()
        },
        Directory {
            files: vec![file("a", blob)],
            symlinks: vec![SymlinkNode {
                name: "a".into(),
                target: "../input".into(),
                ..SymlinkNode::default()
            }],
            ..Directory::default()
        },
        Directory {
            files: vec![file("a", blob)],
            directories: vec![child("a", leaf_id)],
            ..Directory::default()
        },
        Directory {
            node_properties: Some(NodeProperties {
                properties: vec![
                    NodeProperty {
                        name: "z".into(),
                        value: String::new(),
                    },
                    NodeProperty {
                        name: "a".into(),
                        value: String::new(),
                    },
                ],
                ..NodeProperties::default()
            }),
            ..Directory::default()
        },
    ];
    for directory in bad_directories {
        let children = if directory.directories.is_empty() {
            vec![]
        } else {
            vec![leaf.clone()]
        };
        let tree = fixture.message(&Tree {
            root: Some(directory),
            children,
        });
        assert!(matches!(
            fixture.run(&tree_result(tree, false), &[], Limits::default()),
            Err(Failure::Invalid(_))
        ));
    }
    let missing = ObjectId::for_bytes(Algorithm::Sha256, b"not uploaded");
    let tree = fixture.message(&Tree {
        root: Some(Directory {
            files: vec![file("f", missing)],
            ..Directory::default()
        }),
        children: vec![],
    });
    assert_eq!(
        fixture.run(&tree_result(tree, false), &[], Limits::default()),
        Err(Failure::MissingBinding)
    );
    let tree = fixture.message(&Tree {
        root: Some(Directory {
            files: vec![file("A", blob), file("a", blob)],
            ..Directory::default()
        }),
        children: vec![],
    });
    assert!(
        fixture
            .run(&tree_result(tree, false), &[], Limits::default())
            .is_ok()
    );
}

#[test]
fn symlinks_are_opaque_leaves_and_only_exact_compatibility_aliases_are_allowed() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let link = OutputSymlink {
        path: "link".into(),
        target: "/outside/../input".into(),
        ..OutputSymlink::default()
    };
    let mut result = ActionResult {
        output_symlinks: vec![link.clone()],
        output_file_symlinks: vec![link.clone()],
        ..ActionResult::default()
    };
    assert_eq!(
        fixture
            .run(&result, &[], Limits::default())
            .unwrap()
            .objects
            .len(),
        1
    );
    result.output_directory_symlinks.push(link.clone());
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("conflicting symlink aliases"))
    );
    result.output_directory_symlinks.clear();
    result.output_file_symlinks[0].target = "different".into();
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("conflicting symlink aliases"))
    );
    result.output_file_symlinks.clear();
    result.output_symlinks.push(link);
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("duplicate symlink in output list"))
    );
    result.output_symlinks.pop();
    result.output_symlinks[0].target = "nul\0target".into();
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("symlink target"))
    );
    let blob = fixture.put(b"f");
    for path in ["../f", "/f", "a//f", "a/./f", "", "a\0b"] {
        let result = ActionResult {
            output_files: vec![output_file(path, blob)],
            ..ActionResult::default()
        };
        assert!(matches!(
            fixture.run(&result, &[], Limits::default()),
            Err(Failure::Invalid(_))
        ));
    }
    let result = ActionResult {
        output_files: vec![output_file("f", blob), output_file("f", blob)],
        ..ActionResult::default()
    };
    assert_eq!(
        fixture.run(&result, &[], Limits::default()),
        Err(Failure::Invalid("duplicate output path"))
    );
}

#[test]
fn canonical_subset_rejects_other_encodings_and_counts_allocations_before_decode() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    // Unknown field, duplicate singular field, and nonminimal varint respectively.
    for bytes in [
        vec![0xa0, 0x06, 1],
        vec![0x20, 1, 0x20, 2],
        vec![0x20, 0x81, 0x00],
    ] {
        let object = fixture.put(&bytes);
        assert_eq!(
            finish(fixture.prepare(object, &[], Limits::default()).unwrap()),
            Err(Failure::UnsupportedEncoding)
        );
    }
    // One OutputFile with an invalid UTF-8 path: the structured-message census
    // exhausts its budget before prost gets to UTF-8 validation or allocation.
    let object = fixture.put(&[0x12, 0x03, 0x0a, 0x01, 0xff]);
    assert_eq!(
        finish(
            fixture
                .prepare(
                    object,
                    &[],
                    Limits {
                        messages: 1,
                        ..Limits::default()
                    }
                )
                .unwrap()
        ),
        Err(Failure::Capacity("decoded messages"))
    );
    assert_eq!(
        finish(fixture.prepare(object, &[], Limits::default()).unwrap()),
        Err(Failure::Invalid("protobuf"))
    );
    let object = fixture.put(&[0x12, 0x80]);
    assert_eq!(
        finish(fixture.prepare(object, &[], Limits::default()).unwrap()),
        Err(Failure::Invalid("truncated protobuf"))
    );
}

#[test]
fn object_byte_field_edge_task_catalogue_and_path_caps_fail_closed() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let blob = fixture.put(b"payload");
    let result = ActionResult {
        output_files: vec![output_file("abc", blob), output_file("def", blob)],
        ..ActionResult::default()
    };
    let object = fixture.message(&result);
    for (limits, expected) in [
        (
            Limits {
                physical_objects: 1,
                ..Limits::default()
            },
            "physical objects",
        ),
        (
            Limits {
                queued_tasks: 1,
                ..Limits::default()
            },
            "typed tasks",
        ),
        (
            Limits {
                read_bytes: object.size + blob.size - 1,
                ..Limits::default()
            },
            "read bytes",
        ),
        (
            Limits {
                message_bytes: usize::try_from(object.size).unwrap() - 1,
                ..Limits::default()
            },
            "message bytes",
        ),
        (
            Limits {
                wire_fields: 1,
                ..Limits::default()
            },
            "wire fields",
        ),
        (
            Limits {
                edges: 1,
                ..Limits::default()
            },
            "edges",
        ),
        (
            Limits {
                path_bytes: 2,
                ..Limits::default()
            },
            "path bytes",
        ),
    ] {
        assert_eq!(
            finish(fixture.prepare(object, &[], limits).unwrap()),
            Err(Failure::Capacity(expected))
        );
    }
    assert!(matches!(
        fixture.prepare(
            object,
            &[],
            Limits {
                catalogue_objects: 1,
                ..Limits::default()
            }
        ),
        Err(Failure::Capacity("catalogue or retained roots"))
    ));
    let closure = finish(
        fixture
            .prepare(
                object,
                &[],
                Limits {
                    read_bytes: object.size + blob.size,
                    ..Limits::default()
                },
            )
            .unwrap(),
    )
    .unwrap();
    assert_eq!(closure.read_bytes, object.size + blob.size);
    let result = ActionResult {
        output_files: vec![output_file("a/b", blob)],
        ..ActionResult::default()
    };
    assert_eq!(
        fixture.run(
            &result,
            &[],
            Limits {
                depth: 1,
                ..Limits::default()
            }
        ),
        Err(Failure::Capacity("path depth"))
    );
}

#[test]
fn dag_reconvergence_uses_longest_depth_and_path_even_after_short_route_was_seen() {
    for embedded in [false, true] {
        let mut fixture = Fixture::new(Algorithm::Sha256);
        let blob = fixture.put(b"f");
        let leaf = Directory {
            files: vec![file("f", blob)],
            ..Directory::default()
        };
        let leaf_id = fixture.directory_id(&leaf);
        let middle = Directory {
            directories: vec![child("longsegment", leaf_id)],
            ..Directory::default()
        };
        let middle_id = fixture.directory_id(&middle);
        let root = Directory {
            directories: vec![child("a", leaf_id), child("z", middle_id)],
            ..Directory::default()
        };
        let (result, retained) = if embedded {
            let tree = fixture.message(&Tree {
                root: Some(root),
                children: vec![leaf, middle],
            });
            let mut result = tree_result(tree, false);
            result.output_directories[0].path.clear();
            (result, vec![])
        } else {
            fixture.message(&leaf);
            fixture.message(&middle);
            let root = fixture.message(&root);
            (
                ActionResult::default(),
                vec![RetainedRoot {
                    object: root,
                    role: Role::Directory,
                }],
            )
        };
        assert_eq!(
            fixture.run(
                &result,
                &retained,
                Limits {
                    depth: 2,
                    ..Limits::default()
                }
            ),
            Err(Failure::Capacity("directory depth or path"))
        );
        assert_eq!(
            fixture.run(
                &result,
                &retained,
                Limits {
                    path_bytes: 14,
                    ..Limits::default()
                }
            ),
            Err(Failure::Capacity("directory depth or path"))
        );
        assert!(
            fixture
                .run(
                    &result,
                    &retained,
                    Limits {
                        depth: 3,
                        path_bytes: 15,
                        ..Limits::default()
                    }
                )
                .is_ok()
        );
        if embedded {
            let mut prefixed = result;
            prefixed.output_directories[0].path = "out".into();
            assert_eq!(
                fixture.run(
                    &prefixed,
                    &[],
                    Limits {
                        depth: 3,
                        ..Limits::default()
                    }
                ),
                Err(Failure::Capacity("output depth or path"))
            );
            assert_eq!(
                fixture.run(
                    &prefixed,
                    &[],
                    Limits {
                        path_bytes: 15,
                        ..Limits::default()
                    }
                ),
                Err(Failure::Capacity("output depth or path"))
            );
        }
    }
}

#[test]
fn exponentially_many_directory_paths_require_only_linear_graph_work() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let blob = fixture.put(b"f");
    let mut root = Directory {
        files: vec![file("f", blob)],
        ..Directory::default()
    };
    let mut children = Vec::new();
    for _ in 0..30 {
        let identity = fixture.directory_id(&root);
        children.push(root);
        root = Directory {
            directories: vec![child("a", identity), child("b", identity)],
            ..Directory::default()
        };
    }
    let tree = fixture.message(&Tree {
        root: Some(root),
        children,
    });
    let closure = fixture
        .run(
            &tree_result(tree, false),
            &[],
            Limits {
                depth: 32,
                ..Limits::default()
            },
        )
        .unwrap();
    assert_eq!(closure.objects.len(), 3);
    assert_eq!(closure.edges.len(), 62);
}

#[test]
fn catalogue_ambiguity_and_wrong_digest_function_are_rejected() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let result = fixture.message(&ActionResult::default());
    fixture.bindings.push(fixture.bindings[0]);
    assert!(matches!(
        fixture.prepare(result, &[], Limits::default()),
        Err(Failure::Invalid("ambiguous catalogue or digest function"))
    ));
    fixture.bindings.pop();
    let mut other = fixture.bindings[0];
    other.object = ObjectId::for_bytes(Algorithm::Sha256, b"same physical key");
    fixture.bindings.push(other);
    assert!(matches!(
        fixture.prepare(result, &[], Limits::default()),
        Err(Failure::Invalid("ambiguous catalogue or digest function"))
    ));
    fixture.bindings.pop();
    fixture.bindings[0].object.algorithm = Algorithm::Blake3;
    assert!(matches!(
        fixture.prepare(result, &[], Limits::default()),
        Err(Failure::Invalid("ambiguous catalogue or digest function"))
    ));
}

#[test]
fn physical_corruption_is_detected_before_typed_decoding() {
    let mut fixture = Fixture::new(Algorithm::Sha256);
    let result = fixture.message(&ActionResult {
        exit_code: 1,
        ..ActionResult::default()
    });
    let path = fixture
        .store
        .namespace_path()
        .join("objects-v1")
        .join("0000000000000001");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    }
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[0xff]).unwrap();
    file.sync_all().unwrap();
    let failure = finish(fixture.prepare(result, &[], Limits::default()).unwrap()).unwrap_err();
    assert!(matches!(failure, Failure::Physical(error) if error.kind == FailureKind::Corrupt));
}
