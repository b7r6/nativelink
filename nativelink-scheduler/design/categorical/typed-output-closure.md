# Typed output closure preparation experiment

`nativelink-store/src/physical_closure.rs` prepares a bounded, complete set of
exact physical generations from an immutable `ActionResult` object. It uses
the [physical filesystem experiment](physical-filesystem.md) by default and the
existing generated REAPI types. An explicit `VerifiedReader` parameter also
lets the [local Azure fixture](physical-azure.md) run this same traversal against
provider objects. Every reader must verify the whole object before returning a
range, including an empty range. No store factory, service, scheduler, worker, or live
publication path selects it. This is an M5/M6 preparation experiment, not a
completed publication or retention implementation.

## Evidence, not protection

The caller supplies an immutable snapshot mapping qualified object descriptors
to exact physical keys, a nonzero catalogue revision, and optional additional
typed retention roots. Duplicate logical bindings, duplicate physical bindings,
and mixed digest functions are rejected. SHA-256 and BLAKE3 are supported; the
result descriptor selects one explicit function for the entire preparation.
The revision is echoed as evidence, not validated as a retirement fence.

Every required physical object is read and verified against its original size
and digest before successful completion. The output includes exact bindings,
typed roles, edges, the result identity, and the explicit retained roots. An
embedded-directory edge is distinguished from a physical object edge. Repeated
logical references share one physical binding; reading the same bytes first as
a blob does not suppress later validation as a Directory or Tree.

Nothing in this module acquires a lease, orders protection against retirement,
publishes a result, marks a generation ready, or ensures that a verified object
still exists. The later authority integration must validate the complete
manifest against the preparation/retirement fence and acquire protection in the
same decision history as retirement. The output here cannot replace that step.

## Typed edges

The result object itself is required. Its output-file digests and declared
stdout/stderr digests require physical blob objects. Output-directory digests
require physical Tree objects. A Tree contains its root and child Directory
records; these embedded records do **not** require separate CAS objects. Each
DirectoryNode must resolve to a record inside that Tree. Each FileNode requires
a physical blob. A caller-selected standalone Directory instead resolves its
DirectoryNodes through the catalogue as physical Directory objects.

A Tree without a root, with a missing child, or with an unreachable distinct
child is rejected. Identical duplicate child records are allowed when the
topological flag is false; the root cannot be repeated as a child. A true
topological flag additionally requires uniqueness and a parent-before-child
record order. The local schema has no `root_directory_digest` field.

File contents, inline stdout/stderr, and auxiliary metadata `Any.value` are
opaque bytes, even when they happen to encode a protobuf message. Provenance
does not implicitly retain action inputs. Additional retained Blob, Directory,
or Tree roots are optional caller choices, but every edge from a supplied root
is required. An omitted output entry is legal; a present entry with a missing
digest is rejected rather than skipped.

Nonempty inline file or stream data must agree with its declared digest. A
declared digest remains required even when its contents are inline. Raw-only
stdout/stderr are contained in the protected result object and add no CAS edge.
Empty proto3 byte fields do not distinguish absent data from inline empty data.
Even the empty CAS blob requires an explicit physical binding in this experiment.

Symlinks are opaque leaves. Their bounded, nonempty targets may be absolute or
contain `..`; the preparer preserves them in the immutable source object and
performs no filesystem traversal or target lookup. Exact modern/deprecated
output-symlink aliases are accepted. Duplicate entries within one output list,
conflicting aliases, and a path present in both deprecated target-type lists
are rejected.

## Accepted encoding and structural checks

This experiment accepts only canonical prost encoding for the local schema.
Unknown fields, duplicate singular fields, different field ordering, and other
different protobuf encodings are rejected. These can be valid protobuf wire
forms, but the local REAPI [Digest contract](../../../nativelink-proto/build/bazel/remote/execution/v2/remote_execution.proto#L1059)
already requires tag order, no unknown or duplicate fields, and default
serialization semantics for hashed messages. Bytes are verified
before decoding, and an encoding mismatch is rejected rather than silently
normalized into a different digest identity. Matching this contract is not a
claim of general REAPI qualification; the schema, structural checks, explicit
physical catalogue, and finite limits still define this experiment's scope.

Directory child names must be single nonempty segments, unique across kinds,
with each kind sorted by UTF-8 bytes. Node properties are sorted by name;
repeated equal property names are allowed. Output lists need not be sorted.
Output paths are normalized relative paths without empty, `.` or `..`
segments; an output directory may use the empty working-directory path. Names
are case-sensitive. Exact output-path duplicates are rejected except for the
symlink aliases above. This is not Command validation, a permission policy, or
validation of every optional execution-metadata field.

## Bounds and incomplete batches

Independent caps cover catalogue entries, unique physical objects, lifetime
typed tasks, extra roots, total read bytes, bytes per metadata message, wire
fields, decoded messages, graph edges, path bytes, and path depth. A schema-
directed wire census checks field and structured-message budgets before prost
allocates repeated messages. Its fixed schema recursion never enters file
payloads, raw streams, or `Any.value`. Length-delimited bytes remain bounded by
the metadata-message cap. The preparer retains no body for raw physical blobs.
The filesystem reader streams verification; the local Azure reader uses its
separate bounded whole-object buffer.

`advance` accepts 1–1024 steps. A step processes one physical object in one role
or performs the final bounded graph pass. A step is not a constant latency or
constant byte unit: its caps are the per-message and total graph bounds. The
final pass computes maximum descendant depth and path length with cached DAG
metrics, so a shared subtree first visited through a short path cannot hide a
later longer path. Graph work visits nodes and edges, with ordered-map lookup
cost, rather than expanding every possible directory path. A fixture with
over a billion paths requires only 31 distinct Directory records.

The API exposes no partial manifest. `finish` rejects incomplete work;
any failed advance permanently poisons that preparation, even if an object is
later repaired. Dropping or crashing the preparer discards its private state;
a fresh preparation starts from a new supplied snapshot. This module has no
durable batch-resume format or authority checkpoint.

## Verification scope

The integration fixtures cover both digest functions, typed and opaque edges,
embedded directories, inline mismatch, omitted/malformed/missing outputs,
symlink aliases, canonical encoding limits, pre-decode allocation budgets,
physical corruption before decode, partial-batch invisibility, role-sensitive
deduplication, explicit retained roots, graph/path limits, and shared directed
acyclic graphs.
They do not establish serving protection, retirement fencing, Azure semantics,
production workload capacity, or acceptance atomicity.
