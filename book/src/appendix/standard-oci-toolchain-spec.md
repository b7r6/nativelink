<!-- SPDX-FileCopyrightText: 2026 Straylight Software -->
<!-- SPDX-License-Identifier: MIT -->

# Standard OCI Toolchain Specification

**Version:** RC3
**Status:** Draft

> **This appendix is a verbatim mirror; it is not the source of truth.** The
> canonical, authoritative copy of this specification lives in the
> `straylight-toolchain` repository at `spec/standard-oci-toolchain-spec.md` —
> the static-musl LLVM sysroot builder that is also the spec's reference
> implementation. Make edits there and sync them here; on any discrepancy the
> `straylight-toolchain` copy is authoritative. It is reproduced in full so this
> book stays self-contained.

---

## Abstract

A **toolchain** is a file tree at a known FHS-like layout, identified by the content
of that tree, and carried in three content-addressed **projections** of one logical
artifact:

- an **OCI distribution projection** — tar layers and a manifest, addressed by
  **sha256**, the ubiquitous registry-portable serialization;
- a **REAPI execution projection** — a Merkle `Directory` tree of per-file blobs,
  addressed by **BLAKE3**, consumed as a remote-execution input root; and
- a **container projection** — the content materialized into a rootfs with a
  baseline FHS, pleasant to `docker run`.

OCI is already a content-addressed store and is used as one. It is the tooling-
friendly wrapper that everyone can already pull, push, sign, and scan; it is related
by defined correspondence and verifiable hints to the REAPI verbs that actually
execute work. The failure this specification prevents is identifying content by the
recipe that produced it (input-addressing); the discipline it requires is one
logical content tree, projected faithfully, addressed by the algorithm each consumer
needs.

---

## Requirements language

**MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD
NOT**, **MAY**, **OPTIONAL** per RFC 2119 / RFC 8174 when capitalized. §1–§14 are
**normative**; Appendices A–D are **informative**.

---

## 1. Scope

1.1. Defines the **identity**, **projections** (OCI distribution, REAPI execution,
container), **layout**, **naming**, **self-containedness**, **use contract**, and
**conformance** of a toolchain artifact and its consumers.

1.2. Does not define how components are built (App. A), assembled (App. B),
materialized (App. C), or which execution backend is used (App. D). None of these
MUST affect identity.

1.3. Assumes a **hosted Linux target** with a C library. Freestanding, bare-metal,
and no-std toolchains are out of scope.

---

## 2. Terminology

- **Logical content tree** — the intended files of a toolchain (paths → content,
  including symlink targets). The artifact. Identity is a function of this tree and
  nothing else.
- **Projection** — a content-addressed serialization of the logical content tree for
  a particular consumer. This specification defines three.
- **OCI distribution projection** — tar layers + image manifest/config, addressed by
  **sha256**.
- **REAPI execution projection** — a REAPI `Directory`/`FileNode`/`Tree` Merkle
  serialization of per-file blobs, addressed by the declared RE digest function
  (**BLAKE3** by preference).
- **Container projection** — the content materialized into a runnable rootfs with an
  FHS floor (§9).
- **Layer (blob) digest** — sha256 over the (possibly compressed) layer blob; what an
  OCI registry validates.
- **diff_id** — sha256 over the _uncompressed_ layer tar; the OCI content identity of
  a component.
- **File-blob digest** — the RE-digest-function digest over an individual file's
  bytes; the REAPI content identity of a file.
- **Hint** — an annotation relating the OCI projection to the REAPI projection (e.g.
  the REAPI root `Directory` digest), used to plan fetches without recomputation.
  Always verifiable; never trusted for correctness unverified (§6.5).
- **Input-addressed identifier** — identity derived from the recipe/inputs (nix
  derivation hash, non-content-addressed store-path hash, NAR of a non-CA output).
  Prohibited as identity (§11).
- **Materialization** — presenting content on a filesystem (overlay/copy/hardlink/
  symlink). Downstream of identity (§10).
- **Host environment** — the library environment in which the toolchain's own
  binaries execute, including `dlopen` targets such as proc macros.
- **Target environment** — the library environment the toolchain links into the
  artifacts it produces, provided by the sysroot (§13).

---

## 3. The model (normative framing)

3.1. The artifact is the **logical content tree**. Its identity is a function of its
bytes — not the recipe, environment, build path, or time.

3.2. The tree has multiple **projections**, each serializing the _same_ content for a
different consumer, each addressed in the algorithm that consumer needs. The
projections are bound by the shared logical content, **not** by a shared digest.

3.3. The **OCI distribution projection** is the tooling-friendly serialization:
addressed by **sha256**, the registry interoperability floor. Every registry and
every `docker`/`podman`/`buildah`/`skopeo`/cosign already speaks it. It is the
representation that does not scare people, and the one in which toolchains are
stored, signed, scanned, and distributed.

3.4. The **REAPI execution projection** is the Merkle serialization a remote-execution
worker consumes: a `Directory` tree whose files are content-addressed blobs.
Addressed by the declared RE digest function (**BLAKE3** by preference, §6.2),
because execution benefits from the faster hash and the worker's CAS is
BLAKE3-native. This is where work is actually scheduled and cached.

3.5. The **container projection** is the runnable form (§9): content materialized into
a rootfs with an FHS floor, usable under `docker run`.

3.6. OCI already separates content (the manifest + content-addressed layers) from a
materialized rootfs. This specification inherits that separation as load-bearing
(§10): identity is the projection's content addressing; symlink/hardlink/copy/overlay
are materialization and never affect it.

---

## 4. Content addressing (the invariant)

4.1. A component's **logical content** is fixed by a **canonical serialization**: a
reproducible (normalized) tar with the following fixed values:

| field      | value                | rationale                                    |
| ---------- | -------------------- | -------------------------------------------- |
| `mtime`    | 0 (epoch)            | reproducibility                              |
| `uid/gid`  | 0/0                  | no ownership semantics in a CAS              |
| `uname`    | `""`                 | no ownership semantics in a CAS              |
| `gname`    | `""`                 | no ownership semantics in a CAS              |
| `mode`     | 0755 (executable)    | content-relevant: the execute bit             |
|            | 0644 (regular file)  | content-relevant: regular file                |
|            | 0777 (symlink)       | tar convention; target string is the content  |
| `devmajor` | 0                    | no devices in a toolchain                     |
| `devminor` | 0                    | no devices in a toolchain                     |
| `typeflag` | `'0'` regular, `'2'` symlink, `'5'` directory | per entry type          |

Entries MUST be byte-lexicographically ordered by path. No non-portable special
files. Two conforming serializers given the same logical content tree MUST produce
byte-identical output. (Same canonicalization NAR provides for nix, in the ubiquitous
format. NAR MUST NOT be the serialization.)

4.2. Serialization is over the **intended layout**, not the storage representation.
Intended symlinks (e.g. `cc → clang`) are content (their target string is hashed);
assembler-internal links into a backing store MUST be resolved to content before
serialization. Trees with identical intended content MUST yield identical identity
regardless of regular-file/hardlink/symlink-to-identical-content storage (§10).

4.3. **The digest algorithm is per projection** (§3.2). Within the OCI distribution
projection it is sha256 (§5); within the REAPI execution projection it is the declared
RE digest function (§6).

4.4. **No consumer MUST assume a default digest algorithm.** A consumer that hashes or
rehashes content — including the child `Directory`/`Tree` nodes of the REAPI
projection — MUST use the algorithm **declared** by the descriptor or execute request,
and MUST NOT substitute a default. Defaulting the digest function for tree-node
hashing is non-conformant (§14.16).

---

## 5. The OCI distribution projection (sha256)

5.1. **Component = layer.** Each component MUST be exactly one OCI layer. Its
**diff_id** (sha256 over the uncompressed canonical tar of §4.1) is the component's
OCI content identity. Transport compression (`+gzip`, `+zstd`) MAY be applied; it
changes the blob digest, not the diff_id, and MUST NOT be part of identity.

5.2. **Additive, disjoint, no whiteouts.** Components MUST occupy disjoint subtrees;
layers are additive; a conforming image MUST NOT use whiteouts. Layers MUST be listed
in a deterministic canonical order so the manifest digest is a stable function of
content.

5.3. **Digest algorithm.** All OCI-projection digests (layer blob, diff_id, config,
manifest) MUST be **sha256**. sha256 is the registry interoperability floor and is
REQUIRED. sha512 MAY be used only where every registry and tool in the deployment
supports it; other algorithms MUST NOT appear at the OCI/registry boundary.

5.4. **Toolchain distribution identity = manifest digest** (sha256), transitively
covering config and all layer descriptors in canonical order.

5.5. **Dedup.** Shared components MUST share a layer blob; a registry/CAS MUST store it
once. One image MAY carry multiple toolchains as disjoint subtrees sharing component
layers.

5.6. **Annotations** (reverse-DNS) carry role and metadata, including the REAPI hints
of §6.5:

```
dev.straylight.toolchain.role                  = "cxx"
dev.straylight.toolchain.layout-version        = "2"
dev.straylight.toolchain.oci.digest            = "sha256"
dev.straylight.toolchain.reapi.digest-function = "BLAKE3"
dev.straylight.toolchain.reapi.root            = "<hash>/<size>"
```

The role label MUST NOT be used as identity; identity is the manifest digest (§5.4).

---

## 6. The REAPI execution projection (BLAKE3) and its correspondence to OCI

6.1. The same logical content tree MUST be expressible as a REAPI `Directory` tree:
files as `FileNode{name, digest, is_executable}`, intended symlinks as
`SymlinkNode{name, target}`, subdirectories as `DirectoryNode{name, digest}`. The root
`Directory` digest is the toolchain's **execution identity**.

6.2. **Algorithm.** File-blob and `Directory`/`Tree` digests in this projection MUST
use the digest function declared by the execute request and advertised in server
capabilities, and that function SHOULD be **BLAKE3**. Per §4.4 a consumer MUST NOT
default it.

6.3. **Verb correspondence.** The OCI projection is the friendly distribution
serialization; it relates to REAPI verbs as follows, and a conforming consumer SHOULD
use the relation rather than re-deriving content:

| intent                     | OCI / registry                     | REAPI                                                                               |
| -------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------- |
| fetch only what's missing  | manifest + blob `GET` after diff   | `FindMissingBlobs` → `BatchReadBlobs`/ByteStream `Read`                             |
| use a toolchain in a build | reference image by manifest digest | merge its root `Directory` digest into the action `input_root_digest` for `Execute` |
| expand / inspect the tree  | unpack layers                      | `GetTree` from the root `Directory` digest                                          |
| dedup check                | registry blob existence            | `FindMissingBlobs` over file-blobs                                                  |

6.4. **Merkle-friendliness.** Because OCI-projection components are disjoint additive
subtrees (§5.2), each maps to a disjoint subtree of the REAPI `Directory`; the root
`Directory` is the ordered merge of component subtrees. A consumer MAY construct the
REAPI projection per component and compose, mirroring the layer structure.

6.5. **Hints.** The OCI image SHOULD carry annotations (§5.6) relating it to the REAPI
projection — at minimum the RE digest function and the root `Directory` digest, and
OPTIONALLY per-component `Directory` digests — so a consumer can plan fetches
(`FindMissingBlobs`) and assemble an input root **without unpacking and rehashing**.
A hint is an optimization: a consumer MAY verify it by recomputation, MUST treat a
hint that fails verification as absent, and MUST NOT rely on an unverified hint for
correctness (only for fetch planning).

6.6. **Conforming RE server behavior.** A conforming REAPI execution server MUST
reject an `ExecuteRequest` with `digest_function = 0` (UNKNOWN) and return
`INVALID_ARGUMENT`. Silently defaulting the digest function is non-conformant
(§14.16). See Appendix D for interoperability notes.

---

## 7. Cross-projection agreement

7.1. A producer MUST ensure every projection serializes the **same** logical content
tree (§4). The OCI diff_ids, the REAPI file-blobs, and the materialized rootfs MUST
all reduce to the same path→content mapping.

7.2. A consumer MUST be able to verify agreement — e.g. by materializing the OCI
projection and reserializing the REAPI projection (or comparing per-file content).
A mismatch MUST be treated as a corrupt image.

---

## 8. Toolchain layout and naming

8.1. A toolchain MUST present a sysroot-compatible layout rooted at one directory:

```
<toolchain-root>/
  bin/        executables (driver, linker, archiver, …)
  lib/        binary-relative support (e.g. compiler resource dir)
  sysroot/
    include/  headers searched via --sysroot
    lib/      libraries, target dynamic linker, CRT objects searched via --sysroot
```

> The `sysroot/lib/ld-linux-x86-64.so.2` is the **target** interpreter — the one the
> compiler stamps into _programs it builds_. It is NOT the runtime loader for the
> toolchain's own binaries; that is the floor loader (§12.2). The toolchain's own
> `ld-linux` (from its specific glibc) lives in the CAS and is resolved by manifest.

8.2. **The layout is the interface.** No in-tree manifest file MUST be required to
interpret it; discoverable metadata is carried as annotations (§5.6).

8.3. Adjacency expectations (driver finds linker in `bin/`; resource dir relative to
the driver) MUST be satisfiable within the root without external configuration.

8.4. In a multi-toolchain container, each toolchain MUST reside at a content-
discriminated path:

```
/toolchains/<canonical-name>-<content-suffix>/
```

where `<canonical-name>` is the canonical toolchain name (§8.5) and
`<content-suffix>` is a truncation of the toolchain's content digest — the first 16
hexadecimal characters (64 bits) of the BLAKE3 hash of the REAPI root `Directory`
digest. The suffix MUST be present. A path without a content-derived suffix is
non-conformant (§14.8).

The content suffix ensures monotonicity: adding a layer to the image never shadows
an existing toolchain, because two distinct toolchains cannot collide on path
regardless of layer ordering. The human-readable prefix preserves discoverability.

A default symlink SHOULD be provided:

```
/toolchains/default → /toolchains/<primary-canonical-name>-<content-suffix>
```

8.5. **Canonical toolchain naming.** The canonical name is a structured encoding of
the toolchain's configuration axes, in fixed order. It is human-readable,
machine-parseable, and deterministic — the same configuration MUST produce the same
canonical name regardless of producer.

**Grammar:**

```
toolchain-name  = role "-" driver version "-" cxxlib "-" clib
                  [ "-on-" host-cxxlib "-" host-clib ]
                  [ "-" target ]
role            = "cxx" | "cc" | "rust" | "go" | "zig" | ALPHA+
driver          = ALPHA+
version         = DIGIT+ [ "." DIGIT+ ]
cxxlib          = "libstdcxx" | "libcxx"
host-cxxlib     = cxxlib
clib            = "glibc" | "musl"
host-clib       = clib
target          = arch "-" vendor "-" os
```

**Axis table:**

| axis    | values (closed where noted)                  | required?                      |
| ------- | -------------------------------------------- | ------------------------------ |
| role    | `cxx`, `cc`, `rust`, `go`, `zig`, … (open)   | REQUIRED                       |
| driver  | `clang`, `gcc`, `rustc`, `gccgo`, … (open)   | REQUIRED                       |
| version | bare major or major.minor, no `v` prefix     | REQUIRED                       |
| cxxlib  | `libstdcxx`, `libcxx` (closed)               | REQUIRED                       |
| clib    | `glibc`, `musl` (closed)                     | REQUIRED                       |
| target  | GNU target triple (open)                     | OPTIONAL (omit = native)       |

All components MUST be lowercase. Delimiter is `-` between axes, no delimiter
between driver and version. No special characters (`+` in `libstdc++` becomes
`libstdcxx`; `libc++` becomes `libcxx`).

`role` and `driver` are **open** — a new language does not require a spec revision.
`cxxlib` and `clib` are **closed** — a new C or C++ standard library is a significant
ABI event and requires a spec amendment to register its canonical short name.

**The cxxlib axis is always REQUIRED.** Every hosted toolchain carries a C++ standard
library in its closure because everything FFIs: Rust `-sys` crates link C++
transitively; Go's cgo path links against the system C++ stdlib; Zig bundles its own
libc++ for C++ interop. The C++ stdlib determines ABI compatibility when outputs from
different toolchains link into the same process.

**Host/target split.** The naming grammar distinguishes the **target** library
environment (what the toolchain links into produced artifacts, governed by §13) from
the **host** library environment (what the toolchain's own binaries are linked
against, governed by §12).

The `-on-` clause is REQUIRED when the host libraries differ from the target,
and OMITTED when they match. A name without `-on-` asserts host == target.

The host/target split is not hypothetical. A Rust toolchain targeting musl requires
glibc on the host side because proc macros are dylibs `dlopen`'d into `rustc`'s
address space — they must be ABI-compatible with the compiler process, which requires
the host clib. Two toolchains with the same target but different host libraries are
NOT interchangeable.

**Ordering rule.** Axes appear in the fixed order given by the grammar. The order is
by decreasing significance for human scanning: role (what are you building?), driver
(what compiles it?), runtime libraries (what does it link against?), host override
(what does the compiler itself run on?), target (what architecture?).

**Examples:**

```
cxx-clang21-libstdcxx-glibc
cxx-clang21-libcxx-musl
cxx-gcc14-libstdcxx-glibc
cxx-clang21-libstdcxx-glibc-aarch64-linux-gnu
rust-rustc1.80-libstdcxx-musl-on-libstdcxx-glibc
go-go1.23-libstdcxx-glibc
cc-clang21-libstdcxx-musl-on-libstdcxx-glibc
```

With content suffix (§8.4):

```
rust-rustc1.80-libstdcxx-musl-on-libstdcxx-glibc-a7f3e291b04c8d12
```

Full multi-toolchain paths:

```
/toolchains/cxx-clang21-libstdcxx-glibc-a7f3e291b04c8d12/
/toolchains/rust-rustc1.80-libstdcxx-musl-on-libstdcxx-glibc-3c9f20e8a1b74d06/
/toolchains/default → /toolchains/cxx-clang21-libstdcxx-glibc-a7f3e291b04c8d12
```

**Validation.** A conforming producer MUST emit names matching the grammar. A
conforming consumer SHOULD parse the name for display and filtering but MUST NOT use
the parsed components as identity (§11, §5.4 — identity is the manifest digest,
never the name).

---

## 9. The container projection floor

9.1. When materialized as a runnable OCI container, a conforming image MUST be
_usable inside_: a freshly `docker run` (or `podman run`) container MUST present a
baseline FHS such that `which <driver>` and `<driver> --version` succeed without
further setup.

9.2. The floor REQUIRES: a POSIX shell at `/bin/sh`; **bash** at `/usr/bin/bash`;
core utilities (`ls`, `cat`, `env`, `which`, `find`, `grep`, `sed`, `awk` or modern
equivalents); `/etc` essentials sufficient for name resolution and TLS (`passwd`,
`group`, `nsswitch.conf`, and a CA-certificate bundle); a writable `/tmp`; and
environment (`PATH`, `SSL_CERT_FILE`/`SSL_CERT_DIR`, `TERM`, `EDITOR`) such that the
toolchain driver(s) resolve on `PATH`.

9.2.1. The floor SHOULD additionally include a **productive interactive environment**:
a text editor (e.g., `nvim`), a recursive search tool (e.g., `ripgrep`), a file
finder (e.g., `fd`), a JSON processor (e.g., `jq`), version control (e.g., `git`),
an HTTP client (e.g., `curl`), and a shell prompt (e.g., `starship`). The rationale:
the container is a workspace, not merely an execution environment. 75 MB of developer
tools is negligible next to a 1.2 GB toolchain and unconditionally worth the ability
to work inside the container when debugging, testing, or renting compute.

9.3. The floor MUST be provided by an additive, content-addressed **base layer**,
disjoint from the toolchain component layers (§5.2). The minimal REAPI execution
input root (§6) MAY omit the base layer; the runnable/developer image MUST include
it.

---

## 10. Identity is independent of materialization

10.1. Presenting content on a filesystem (overlay/copy/hardlink/symlink) is
materialization and MUST NOT affect identity (§4, §5, §6).

10.2. A consumer MUST derive identity from a projection's content addressing, never
from a materialized rootfs's inode representation. Symlink-, hardlink-, and
copy-materializations of one image MUST present identical identity.

10.3. (Consequence.) A toolchain consumed locally and the same toolchain on a remote
worker MUST derive the same execution identity (§6.1), and therefore the same action
cache key, so cache entries are shared across environments rather than split by
materialization.

10.4. (Recommended materialization.) When multiple toolchains share content (e.g.
identical glibc, identical `ld-linux`), the materializer SHOULD use **hardlinks** into
a content-addressed store (`/cas/<blake3-hex>`) for file-level dedup. Hardlinks
preserve inode identity, incur zero dereference cost, and do not confuse the ELF
loader or build tooling. The `/cas/` directory is an extraction-time optimization
and MUST NOT appear in any identity computation or OCI layer content.

---

## 11. Prohibition on input-addressing

11.1. Toolchain and component identity MUST be content-derived (§4–§6).

11.2. Input-addressed identifiers — nix derivation hashes, non-content-addressed
store-path hashes, NAR addressing of non-CA outputs — MUST NOT be used as identity,
and MUST NOT appear in any manifest, config, identity-bearing annotation, or layer
content.

11.3. (Rationale.) Input-addressing ties identity to provenance: identical content
from two recipes fails to deduplicate, and a recipe is _trusted_ to reproduce rather
than _verified_. Content-hashing machinery used for input-addressed identity is the
appearance of a CAS without its guarantee. This specification requires the guarantee.

---

## 12. Self-containedness (content-closure)

**Principle.** A conforming toolchain's own ELF objects MUST resolve every dynamic
dependency by **content** — a digest recorded in the object's manifest (§12.3) —
and MUST NOT resolve any dependency by an ambient or embedded **path**, with
exactly one permitted external path reference: the floor dynamic loader (§12.2).
Self-containedness is closure over the manifest, not over the tree.

A **fully static** object — no `PT_INTERP` and empty `DT_NEEDED` — has no dynamic
dependency to resolve and no interpreter to anchor. It trivially satisfies this
section, a strictly stronger property than the manifest closure §12.3 would otherwise
record, and requires neither the floor loader (§12.2) nor a manifest note. §12.1–§12.6
constrain objects that carry dynamic dependencies; a static-PIE toolchain is
conformant with the applicable subset being empty.

> This section governs the runtime resolution of the **toolchain's own binaries**
> (e.g. `clang` finding `libstdc++.so.6` in order to _run_). It does **not** govern
> the toolchain's library _search_ when used as a compiler to build a user program
> (§13, sysroot search) — that is a separate path and is unaffected.

12.1. **RPATH/RUNPATH MUST be absent (strip, not set).** Every executable and shared
object in the artifact MUST carry no `DT_RPATH` and no `DT_RUNPATH`. They are
**stripped**, not redirected. With no private search path on any object, resolution
has one and only one locus (§12.4), and load order cannot vary across the closure.

```
//  receipt (empty ⇒ conformant):
patchelf --print-rpath <each-elf>                      // → ""
readelf -d <each-elf> | grep -E 'RPATH|RUNPATH'       // → no output
```

12.2. **A dynamically-linked executable's `PT_INTERP` MUST name the floor loader at
one well-known path.** Every executable that carries an interpreter MUST set it to a
single deployment-wide path (`/lib/ld-std-oci-toolchain.so`), identical across all
toolchains. A fully static executable carries no `PT_INTERP` and is exempt (§12
principle). This is the **sole
permitted external path reference** of §12 — the one input-addressed _location_
anchor in an otherwise content-addressed system. The kernel resolves `PT_INTERP`
during `execve` before any CAS-aware code runs, so the loader itself cannot be named
by content at the interpreter slot.

The floor loader (`ld-std-oci-toolchain.so`) MUST:

- Have no dynamic dependencies (fully statically linked).
- Be a position-independent executable.
- Be provided by the container floor base layer (§9.3).
- Be content-addressed as a file (its own digest is in the CAS) even though its
  location is fixed.

The floor loader is a dispatch shim: it reads the executed binary's PT_NOTE manifest
(§12.3.1), extracts the loader entry (soname_len == 0), resolves the per-toolchain
`ld-linux-x86-64.so.2` from `/cas/<blake3-hex>`, verifies the hash, and `execve`s it
with the original binary as argv[0].

This allows multiple glibc versions to coexist: each toolchain's real `ld-linux` is
content in the CAS, resolved by hash. The floor loader is universal across all
toolchain versions.

> **§12.2 vs §11 (prohibition on input-addressing).** The floor-loader path is a
> fixed _location_, not a fixed _identity_. Its identity is still its content digest;
> only its PT_INTERP location is fixed. Location ≠ identity. This is a deliberate,
> audited exception — the kernel requires it — not an §11 violation.

12.3. **The `DT_NEEDED` → content manifest.** Each ELF object MUST carry a manifest
binding **every** `DT_NEEDED` entry to (a) the **content digest** of the providing
object the build resolved, and (b) the **versioned-symbol requirements** bound
against that object. The manifest MUST be carried as an ELF note in a `PT_NOTE`
segment with note name `"dev.straylight.cas"` and note type `0x01`.

The manifest MUST be **complete**: a `DT_NEEDED` with no manifest row is a
non-conforming object.

#### 12.3.1. Binary wire format (normative)

The PT_NOTE descriptor uses the following layout (all multi-byte integers are
little-endian):

```
ELF Note header:
  n_namesz  = 19                          // strlen("dev.straylight.cas") + 1 = 19
                                          //   (the name FIELD is then padded to 20
                                          //    bytes for 4-byte alignment; n_namesz
                                          //    itself is 19. The shipped floor loader
                                          //    checks n_namesz == 19.)
  n_descsz  = <variable>                  // total descriptor bytes
  n_type    = 0x01                        // CAS_MANIFEST
  n_name    = "dev.straylight.cas\0"      // padded to 4-byte alignment

Descriptor header:
  version       : u8   = 1               // format version
  digest_fn     : u8   = 0x01            // 0x01 = BLAKE3
  num_entries   : u16                    // total manifest entries
  digest_len    : u16  = 32              // bytes per digest (BLAKE3 = 32)
  _reserved     : u16  = 0

Per entry (repeated num_entries times):
  soname_len    : u16                    // 0 = loader entry (special)
  soname        : [soname_len]           // DT_NEEDED string, verbatim
  digest        : [digest_len]           // raw hash bytes (NOT hex-encoded)
  num_ver_reqs  : u8                     // GNU version requirements count (0 = none)
  per version requirement:
    vername_len : u16
    vername     : [vername_len]          // e.g. "GLIBC_2.34"
```

**Forward compatibility.** A parser encountering `version > 1` MUST treat the note
as absent (skip it) and MUST NOT attempt to parse the descriptor body. A floor loader
that skips a manifest note MUST refuse to load the binary (fail closed). This gives a
clean upgrade path: bump version, old loaders refuse, new loaders parse.

**Integrity.** Before walking entries, a parser MUST verify that `n_descsz` is at
least `8 + num_entries * (2 + digest_len + 1)` (the minimum possible descriptor size
assuming zero-length sonames and zero version requirements). If the check fails, the
note MUST be treated as corrupt and the loader MUST refuse to load the binary.

**Special entries:**

- `soname_len == 0`: the **loader entry** — its digest identifies the per-toolchain
  `ld-linux` that the floor loader (§12.2) should resolve and transfer control to.
  Every executable MUST have exactly one loader entry. Shared objects MUST NOT.

**Digest function declaration:** The `digest_fn` byte in the descriptor header
declares the algorithm for ALL digests in this note. A consumer MUST NOT assume a
default (§4.4). Currently defined values:

| Value | Algorithm | Digest length |
| ----- | --------- | ------------- |
| 0x01  | BLAKE3    | 32            |
| 0x02  | SHA-256   | 32            |

**Alignment:** The note descriptor is NOT required to be internally aligned beyond
the ELF note alignment (4 bytes). Parsers MUST handle unaligned reads within the
descriptor.

12.4. **Resolution is performed solely by the floor loader, solely through the
CAS.** The loader MUST, for each `DT_NEEDED` soname, read the manifest row (§12.3),
resolve to `/cas/<digest>`, **verify** the resolved bytes hash to the manifest
digest, and load. The loader MUST NOT consult `DT_RPATH`, `DT_RUNPATH`,
`LD_LIBRARY_PATH`, `/etc/ld.so.cache`, or any default search path. Resolution is
content lookup, not path search.

12.5. **Ambient authority MUST be refused by construction.** The floor loader MUST
ignore `LD_PRELOAD`, `LD_LIBRARY_PATH`, `LD_AUDIT`, and every `LD_*` environment
influence on resolution and interposition. The loader **has no code path** that reads
the environment for library location or preload. The mechanism that would express the
attack is removed, not forbidden.

12.6. **`dlopen` closure SHOULD be recorded and staged; resolution SHOULD fail
closed.** Any object that may `dlopen` at runtime — including glibc's own NSS
(`libnss_*`) and any plugin mechanism such as Rust proc macros — SHOULD have its
runtime-reachable set recorded in the manifest as a **dlopen-closure**, and every
object in that closure SHOULD be staged in the CAS. A `dlopen` of a name absent from
the recorded closure SHOULD fail closed — it SHOULD NOT fall back to a path search.

> (Informative.) The dlopen-closure is the hard operational part of this model.
> `DT_NEEDED` is statically present; `dlopen` targets may be computed at runtime.
> Producing a correct closure requires either observing the build/runtime `dlopen` set
> or constraining the configuration (e.g. a fixed `nsswitch.conf` whose modules are
> enumerable). This specification recommends recording, staging, and failing closed on
> a miss. Promotion to MUST is expected in a future revision when tooling matures.

> (Informative.) The host/target split (§8.5) makes the dlopen-closure especially
> critical for Rust toolchains. Proc macros are the primary `dlopen` use case; they
> must be compiled against the HOST clib. The dlopen-closure must record that `rustc`'s
> `dlopen` targets are host-ABI, and the floor loader must resolve them from the
> host-clib CAS entries, not the target-clib sysroot.

12.7. **No factory path anywhere.** Factory store paths (e.g. `/nix/store/...`) MUST
NOT appear in any layer content, in any form, including inside binaries, text
configuration (`.pc`, `.la`, wrappers), or manifests.

12.8. **Determinism of resolution is structural.** Because resolution lives solely
in the floor loader (§12.4) and no object carries a private search path (§12.1),
every object in a process resolves a given soname identically; load order cannot
diverge across the closure.

---

## 13. Use contract

13.1. A conforming toolchain MUST compile and link through its layout with a single
sysroot argument:

```
<toolchain-root>/bin/<driver> --sysroot=<toolchain-root>/sysroot [-fuse-ld=<linker>] …
```

13.2. The sysroot MUST be identical for compile and link; the dynamic linker, CRT
objects, and libraries MUST be discoverable via sysroot search; a peer linker MUST be
discoverable by name in `bin/`.

> **Target vs host (clarification).** The "dynamic linker" discoverable via sysroot is
> the **target** interpreter — the one the compiler stamps into _programs it builds_
> (via `-Wl,-dynamic-linker,<sysroot>/lib/ld-linux-x86-64.so.2`). This is distinct
> from the resolution of the _toolchain's own binaries_ at runtime, which is governed
> by §12 (the floor loader + CAS). §13 is about the compiler's search behavior when
> building user code; §12 is about the toolchain binary's own library resolution.

13.3. Compilation databases SHOULD record the toolchain-root-relative invocation
verbatim (one greppable toolchain token).

---

## 14. Conformance

**Conforming toolchain image:**

- **14.1** Each component is one additive layer; no whiteouts (§5.1–§5.2).
- **14.2** OCI-projection digests (blob, diff_id, config, manifest) are sha256;
  sha256 is supported as the floor (§5.3).
- **14.3** The logical content is expressible as a REAPI `Directory` tree whose root
  digest is the execution identity, in the declared RE digest function (§6.1–§6.2).
- **14.4** OCI and REAPI projections (and any materialization) agree on the same
  logical content (§7); recomputation reproduces the declared diff_ids and file-blob
  digests.
- **14.5** Components are disjoint; the materialized rootfs is order-independent
  (§5.2, §6.4).
- **14.6** No RPATH/RUNPATH on any ELF object (stripped, not redirected); no factory
  store path in any content (§12.1, §12.7).
- **14.7** No input-addressed identifier is used as identity or present (§11).
- **14.8** The toolchain path includes a content-derived suffix; a path with only
  human-readable components is non-conformant (§8.4). The canonical name matches the
  grammar of §8.5.
- **14.9** Every **dynamically-linked** executable's PT_INTERP is the single
  deployment-wide floor-loader path; the floor loader is provided by the base layer,
  has no dynamic dependencies, and is itself content-addressed as a file (§12.2). A
  fully static (PIE) executable with no PT_INTERP and empty DT_NEEDED trivially
  satisfies §12 and is exempt.
- **14.10** Every DT_NEEDED entry has a complete manifest row (soname → declared-fn
  digest + version-reqs); no DT_NEEDED is unmanifested (§12.3).
- **14.11** The floor loader resolves solely via the CAS, verifies each resolved
  object against its manifest digest, and consults no RPATH/RUNPATH/LD\_\*/ld.so.cache
  (§12.4, §12.5).
- **14.12** Resolution is structurally deterministic: no ambiguous used-symbol binding
  ships without an ordering edge (§12.8).
- **14.13** Identity from symlink-, hardlink-, and copy-materializations is identical
  (§10).
- **14.14** Usable via the single-sysroot contract (§13).
- **14.15** A runnable image satisfies the container floor (§9): `/bin/sh`, core
  utilities, `/etc` essentials (passwd/group/nsswitch/CA bundle), writable `/tmp`,
  and `PATH` such that `which <driver>` and `<driver> --version` succeed.

**Conforming consumer:**

- **14.16** Honors the **declared** digest function for all hashing, including REAPI
  child `Directory`/`Tree` nodes, and **never assumes a default** (§4.4). Defaulting
  the tree-node digest function is non-conformant.
- **14.17** Derives identity from a projection, never from a materialized rootfs's
  inodes (§10.2).
- **14.18** Treats hints as optimization only: verifies before relying, uses an
  unverified hint solely for fetch planning (§6.5).
- **14.19** Includes the toolchain's execution identity (REAPI root `Directory`
  digest) in the action cache key when using it as a build input (§10.3).

---

---

# Appendices (informative)

## Appendix A. Building components with nix

`nix build` produces a component; its realized **output content** is serialized
canonically (§4.1) and addressed by content. Its store path, `.drv`, and NAR
addressing never enter any projection (§11). Normalize embedded timestamps/build
paths before serialization. Same-filesystem reflink (`cp --reflink=auto`) is a
build-time optimization, not identity.

## Appendix B. Assembly and self-containedness (satisfying §12)

Assembly is a two-phase pipeline:

**Phase 1: assembly** places component content from Nix into the §8 FHS layout.
Runs `patchelf` to rewrite interpreter and RPATH.

**Pre-finalization RPATH convention.** Phase 1 MUST set RUNPATH on each ELF object to
`$ORIGIN`-relative paths that resolve within the §8 layout (e.g., `$ORIGIN/../lib`).
This ensures finalization can resolve DT_NEEDED entries from the assembled tree
without depending on the build system's output directory structure.

The Phase 1 output (assembled but not finalized) is NOT a conforming toolchain,
MUST NOT be distributed, and MUST NOT appear in any OCI layer.

**Phase 2: finalization** performs the §12 closure on every ELF object in the
assembled tree:

1. Resolves each `DT_NEEDED` soname via RUNPATH (the `$ORIGIN`-relative path).
   An unresolvable `DT_NEEDED` is a **fatal error** — it indicates a broken assembly
   and MUST NOT produce a manifest entry.
2. BLAKE3-hashes each resolved library.
3. Strips `DT_RUNPATH`/`DT_RPATH` (zeros d_val to point at strtab[0]='\0').
4. Overwrites `PT_INTERP` in-place to `/lib/ld-std-oci-toolchain.so`.
5. Appends a `PT_NOTE` segment with the content manifest (§12.3.1).
6. Stages resolved libraries to `/cas/<blake3-hex>`.

This is an **atomic** operation per binary — either all steps succeed or none do. The
output is a finalized tree (`toolchain/`) plus a CAS directory (`cas/`).

Receipts (empty / exit-0 ⇒ conformant):

```
patchelf --print-rpath <each-elf>                          // → ""   (stripped)
readelf -l <driver> | grep interpreter                     // → /lib/ld-std-oci-toolchain.so
grep -rIl /nix/store <toolchain>/                          // → empty (text refs too)
bwrap --ro-bind <floor> / --ro-bind <toolchain> /toolchains/<name> \
      --ro-bind ./cas /cas \
      --tmpfs /tmp -- /toolchains/<name>/bin/<driver> --version   // → exit 0
```

## Appendix C. Materialization and the three projections (illustrating §10)

One logical content tree, three projections, all sharing identity by content:
**OCI distribution** (sha256 tar layers + manifest; registries, signing, scanning) —
**REAPI execution** (BLAKE3 Merkle `Directory`; the action input root) — **container**
(materialized rootfs with the §9 floor; `docker run`, bwrap rootfs source, worker
base). Overlay/copy/hardlink/symlink are materialization and never enter a digest.

## Appendix D. Remote-execution interoperability notes

The two-projection model (sha256 at the OCI boundary, BLAKE3 at the RE boundary)
requires that the REAPI execution server honor the declared digest function (§6.6,
§14.16). Implementations that silently default an unset `digest_function` to SHA256
produce corrupted `Directory` digests when the client expects BLAKE3.

A conforming RE server MUST reject `ExecuteRequest` with `digest_function = 0`
(UNKNOWN), returning `INVALID_ARGUMENT` (§6.6). A global strict-mode configuration
option that extends this rejection to all services (CAS, AC, ByteStream) is
RECOMMENDED for new deployments.

## Appendix E. Changelog

A summary of substantive changes between released versions — not a normative diff.
The spec body always states the current requirements; git carries the authoritative
history.

**RC3** (2026-07-05)

- §12, §12.2, §14.9 — carve out the fully static (PIE) case: an object with no
  `PT_INTERP` and empty `DT_NEEDED` trivially satisfies self-containedness (a strictly
  stronger property than the manifest closure it would otherwise carry) and requires
  no floor loader. §14.9 previously asserted `PT_INTERP == floor path` on _every_
  executable, which a conforming sovereign static-PIE toolchain — the best-case
  object — could never satisfy; the floor-loader requirement now applies to
  dynamically-linked executables.
- Folded the standalone amendments ledger into this changelog; the spec body now
  reads as current state rather than a running diff.

**RC2** (2026-07-02)

- §12.3.1 note format reconciled against the _shipped_ floor loader (the oracle),
  not the drifted source header. `n_namesz = 19` (not 20 — the name field is padded to
  20 bytes for 4-byte alignment, but the length is 19; a note written with `namesz=20`
  is silently skipped). The descriptor header is 8 bytes:
  `version:u8, digest_fn:u8, num_entries:u16, digest_len:u16, _reserved:u16`. Verified
  by disassembling the shipped loader, byte-diffing the C++ and Python finalizers, and
  running a Python-finalized binary through the real loader to `exit 0`.
