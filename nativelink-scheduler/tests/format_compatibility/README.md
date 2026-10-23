# Frozen Base executable qualification

This fixture compiles two separate executables from immutable source archives.
The Base executable uses a synthesized earlier capability level for the
experimental envelope-3/core-5 baseline. It is not a historical released
NativeLink binary, and it makes no compatibility claim for format 2 or core 4.

The Base source is pinned by `frozen-base.json`. The small `base-only.patch`
removes the direct-object scheduler variant, decoder, and encoder from the
actual journal implementation. Its capability predicate rejects every nonempty
active feature set. The known peer-advertisement and feature-control vocabulary
remains readable, including a target the Base executable cannot activate.
Changing a runtime reader flag in an upgraded executable is not this test.

The reader links the frozen `State::restore`, journal, snapshot buffer, and
format-control implementation from its own extracted tree. It never links the
working checkout's upgraded journal. The complete source archive, the individual
format-relevant modules, manifests, dependency lock, fixture source, compiled
executables, generated artifacts, and rebuilt snapshot have recorded SHA-256
hashes. The report also records compiler versions and exact build/run commands.

## Run the pinned qualification

From the repository root:

```sh
python3 nativelink-scheduler/tests/format_compatibility/run.py run \
  --output /tmp/nativelink-frozen-base-evidence \
  --cargo-target-dir /tmp/nativelink-frozen-base-target
```

The output directory must not already exist. The supplied Cargo target root
contains physically separate Base and upgraded target directories, each keyed
by its complete source archive, fixture, and overlay hashes. Cargo can otherwise
reuse stale package artifacts when extracting an older archive gives source
files older modification times than a previously compiled library. Both binaries
assert their compiled capability at startup and are copied and hashed before
the next build. The source trees remain separate and immutable inputs.
The build uses the pinned `Cargo.lock` with `--locked`.

To test a later upgraded writer against the same frozen reader, add
`--writer-revision <commit>`. The revision resolves to an immutable commit before
the writer's source archive is created. Reader source and decoder capabilities
remain pinned. `report.json` records both revisions and their source hashes.

## What crosses the boundary

The upgraded producer uses real journals and actual singleton OpenRaft commands.
Its state contains pending dispatch, cancellation with resources still charged,
an accepted result awaiting publication, and a pending cache lookup. It builds
and replaces a snapshot, compacts the log, restarts, and commits preparation
through Raft. Actual journal transactions then expose the precise activation
append, durable-commit, and apply cutpoints. No artifact is created by editing
JSON or recomputing its checksum.

| Artifact | Frozen Base behavior |
| --- | --- |
| Journal before activation, after snapshot replacement, compaction, and restart | Opens the actual frozen journal, reads its persisted vote, writes another Base snapshot, starts actual singleton Raft, and completes a quorum read with identical complete state. |
| Snapshot before activation | Installs through the frozen snapshot decoder and transaction path, then reads the vote and verifies identical complete state. |
| Committed preparation and uncommitted activation | Decodes and preserves the known control and old state; inspection alone does not authorize participation. |
| The same unresolved journal offered for Base admission | Refuses admission before `read_vote` or Raft construction. |
| Activation durably committed but not applied, with an old snapshot | Detects the committed suffix and refuses before `read_vote` or Raft construction. |
| Activated journal | Refuses the unsupported active representation before `read_vote` or Raft construction. |
| Activated snapshot | Refuses installation through the actual frozen decoder before any vote read or Raft construction. |

Every subprocess receives a copy. The original captured artifacts remain
unchanged and retain their hashes. Accepted state is compared as complete
canonical core JSON, including effect identities, resource charges, and terminal
associations. The compatible journal case proves actual recovery and quorum
reading, rather than only reporting that a parser accepted some bytes.

An uncommitted activation on one member is not proof that the cluster has not
committed it. The fixture therefore distinguishes successful artifact decoding
from permission for an unsupported process to vote. The separate format tests
qualify capable-quorum recovery, committed abort, and later Base admission.

## Changing the frozen baseline

A new freeze is an explicit reviewed source change. The tool refuses to replace
an existing pin:

```sh
python3 nativelink-scheduler/tests/format_compatibility/run.py freeze \
  --revision <reviewed-source-commit> \
  --pin /tmp/new-frozen-base.json
```

Review the source revision, the Base-only overlay, and the new hashes before
replacing the committed pin. Updating the writer under test does not require
changing the Base pin. Replacing that pin for every candidate would erase the
compatibility boundary this fixture is intended to test.

## Receipt-retirement writer modes

`--writer-mode direct` remains the default feature-1 qualification and can build
the original pinned writer revision. A receipt-capable writer revision can also
run `--writer-mode receipt` for feature 2 with the string scheduler section, or
`--writer-mode both` for features 1 and 2 with the direct-object section. Use a
separate output directory for each mode. The frozen Base source, pin, overlay,
and executable capability remain unchanged.

For example, after committing the upgraded implementation:

```sh
python3 nativelink-scheduler/tests/format_compatibility/run.py run \
  --writer-revision <upgraded-commit> --writer-mode receipt \
  --output /tmp/nativelink-receipt-base-evidence \
  --cargo-target-dir /tmp/nativelink-frozen-base-target
```

The runner enables a fixture-only Cargo feature for the receipt modes. Its
conditional producer code lets the default fixture compile against the
older pinned writer. This is a build capability of the test executable, not a
runtime authority-format selector. The selected feature set still crosses the
real Prepare, append, commit, and apply boundaries.

Each mode records its feature set and the complete activated state separately
from the core-5 state used for pre-activation compatibility comparisons. Before
activation, even the receipt-capable executable must emit core 5. The producer
also checks that preparation evidence still contains the `DirectScheduler`
reader spelling. The frozen reader must preserve the uncommitted receipt
control, refuse admission while unresolved, and reject committed activation
before a vote read or Raft startup. For feature 2 alone, rejection follows the
unsupported active feature; for the combined mode, the frozen decoder also
lacks the direct-object representation.

This is a bounded experimental compatibility gate. It does not qualify a
deployed transport, authenticate remote capabilities, fence old processes, or
replace a production admission and membership protocol. Compiler versions and
locked dependency sources are recorded; the fixture is not a hermetic toolchain or
a proof of bit-for-bit reproducible binaries across different hosts.
