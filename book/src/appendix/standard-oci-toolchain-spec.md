<!-- SPDX-FileCopyrightText: 2026 Straylight Software -->
<!-- SPDX-License-Identifier: MIT -->

# Standard OCI Toolchain Specification

**Version:** RC1, revision 3 (NativeLink track — implementation-informed amendments)
**Canonical:** `straylight-buck2-prelude/docs/TOOLCHAIN-SPEC.md` (RC1 rev 2)
**This copy:** NativeLink book track — adds §12.3.1 (binary format), §6.6 (REAPI
unblock status), renames paths to match implementation, narrows manifest carriage.
Reconcile with canonical before ratification.

> **Revision 3 (NativeLink track).** Incorporates implementation feedback from
> `straylight-buck2-prelude/tools/`:
>
> - §12.2: floor loader path updated to `/lib/ld-std-oci-toolchain.so`
> - §12.3: manifest carriage narrowed to PT_NOTE only; adds §12.3.1 normative
>   binary wire format
> - §6.6: REAPI execution projection unblocked (NativeLink digest function fix
>   shipped on `pr/sha-256-silent-default-fix`)
> - Appendix B: tool names corrected (`std-oci-toolchain`, not `straylight-cas`)
> - Appendix D: conformance gap status updated (fix shipped)
> - Appendix F (new): implementation status matrix

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
**normative**; Appendices A–E are **informative**.

---

## 1. Scope

1.1. Defines the **identity**, **projections** (OCI distribution, REAPI execution,
container), **layout**, **self-containedness**, **use contract**, and **conformance**
of a toolchain artifact and its consumers.

1.2. Does not define how components are built (App. A), assembled (App. B),
materialized (App. C), or which execution backend is used (App. D). None of these
MUST affect identity.

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
Addressed by **BLAKE3** (the declared RE digest function), because execution wants
the faster hash and the worker's CAS is BLAKE3-native. This is where work is
actually scheduled and cached.

3.5. The **container projection** is the runnable form (§9): content materialized into
a rootfs with an FHS floor, usable under `docker run`.

3.6. OCI already separates content (the manifest + content-addressed layers) from a
materialized rootfs. This specification inherits that separation as load-bearing
(§10): identity is the projection's content addressing; symlink/hardlink/copy/overlay
are materialization and never affect it.

---

## 4. Content addressing (the invariant)

4.1. A component's **logical content** is fixed by a **canonical serialization**: a
reproducible (normalized) tar — entries byte-lexicographically ordered, fixed mtimes,
fixed uid/gid/uname/gname, content-relevant mode bits only, no non-portable special
files. (Same canonicalization NAR provides for nix, in the ubiquitous format. NAR
MUST NOT be the serialization.)

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
and MUST NOT substitute a default. _Defaulting the digest function for tree-node
hashing is non-conformant (§14.11; Appendix D)._

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
dev.straylight.toolchain.role             = "cxx"        // the role label, not identity
dev.straylight.toolchain.layout-version   = "1"
dev.straylight.toolchain.oci.digest        = "sha256"
dev.straylight.toolchain.reapi.digest-function = "BLAKE3"     // hint (§6.5)
dev.straylight.toolchain.reapi.root        = "<hash>/<size>"  // hint: REAPI root Directory digest
```

The role label MUST NOT be used as identity; identity is the manifest digest (5.4).

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

6.6. **REAPI projection status (rev 3, informative).** The REAPI execution projection
was previously blocked by a conformance gap in NativeLink (Appendix D): the server
defaulted `digest_function` to SHA256 when unset, causing BLAKE3 clients to receive
corrupted Directory digests. This gap is now closed:

- The NativeLink execution server unconditionally rejects `ExecuteRequest` with
  `digest_function = 0` (UNKNOWN), returning `INVALID_ARGUMENT`.
- A global config option (`require_explicit_digest_function`) extends this to all
  services (CAS, AC, ByteStream).
- Legacy mode emits a diagnostic warning on first unset digest_function.

With this fix shipped (`sensenet-ai/nativelink@pr/sha-256-silent-default-fix`), the
REAPI BLAKE3 projection is unblocked for end-to-end deployment. The two-projection
model (sha256 at OCI boundary, BLAKE3 at RE boundary) is now implementable without
requiring matched-configuration discipline (Appendix E, item E3 retired by E4).

---

## 7. Cross-projection agreement

7.1. A producer MUST ensure every projection serializes the **same** logical content
tree (§4). The OCI diff_ids, the REAPI file-blobs, and the materialized rootfs MUST
all reduce to the same path→content mapping.

7.2. A consumer MUST be able to verify agreement — e.g. by materializing the OCI
projection and reserializing the REAPI projection (or comparing per-file content).
A mismatch MUST be treated as a corrupt image.

---

## 8. Toolchain layout

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

8.4. In a multi-toolchain container, each toolchain MUST reside at a named path:

```
/toolchains/<toolchain-name>/
```

where `<toolchain-name>` is a human-readable identifier encoding the significant
configuration axes (e.g. `cxx-clang21-libstdcxx-glibc`, `rust-stable`). Multiple
toolchains MAY coexist in one image as disjoint subtrees. A default symlink
(`/toolchains/default → /toolchains/<primary>`) SHOULD be provided for `PATH`
resolution.

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
an editor (`nvim`), a search tool (`ripgrep`), a find tool (`fd`), a JSON tool
(`jq`), version control (`git`), a network tool (`curl`), and a shell prompt
(`starship`). The rationale: the container is a workspace, not merely an execution
environment. 75 MB of developer tools is negligible next to a 1.2 GB toolchain and
unconditionally worth the ability to work inside the container when debugging,
testing, or renting compute.

9.3. The floor MUST be provided by an additive, content-addressed **base layer**,
disjoint from the toolchain component layers (§5.2). The minimal REAPI execution
input root (§6) MAY omit the base layer; the runnable/developer image MUST include
it.

9.4. (Rationale, informative.) One image serves distribution, execution, and
interactive use. Execution wants a minimal input root; a human wants a pleasant
shell. Disjoint additive layering serves both from one content-addressed image
without forking it. If you `docker run` one of these, it should be nice inside.

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

> **Revision 3 note.** This replaces the prior §12 wholesale and corrects Appendix B.
> The prior §12 closed the artifact over its _directory tree_ (path-closure); this
> revision closes it over its _content manifest_ (content-closure). The distinction
> is load-bearing and is the whole point of the floor loader.

**Principle.** A conforming toolchain's own ELF objects MUST resolve every dynamic
dependency by **content** — a digest recorded in the object's manifest (§12.3) —
and MUST NOT resolve any dependency by an ambient or embedded **path**, with
exactly one permitted external path reference: the floor dynamic loader (§12.2).
Self-containedness is closure over the manifest, not over the tree.

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

12.2. **`PT_INTERP` MUST name the floor loader at one well-known path.** Every
executable's ELF interpreter MUST be a single deployment-wide path
(`/lib/ld-std-oci-toolchain.so`), identical across all toolchains. This is the **sole
permitted external path reference** of §12 — the one input-addressed _location_
anchor in an otherwise content-addressed system. The kernel resolves `PT_INTERP`
during `execve` before any CAS-aware code runs, so the loader itself cannot be named
by content at the interpreter slot.

The floor loader (`ld-std-oci-toolchain.so`) is:

- Statically linked against musl (no dependencies of its own)
- Compiled as a static-pie C++23 binary
- Provided by the container floor base layer (§9.3)
- Content-addressed _as a file_ (its own digest is in the CAS) even though its
  _location_ is fixed
- A dispatch shim: it reads the executed binary's PT_NOTE manifest (§12.3.1),
  extracts the loader entry (soname_len == 0), resolves the per-toolchain
  `ld-linux-x86-64.so.2` from `/cas/<blake3-hex>`, verifies the hash, and
  `execve`s it with the original binary as argv[0]

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

> **Revision 3.** The section (`.straylight.cas`) and sidecar (`<binary>.cas`)
> carriage options from RC1 rev 2 are withdrawn. The implementation uses PT_NOTE
> exclusively; multiple carriage formats add complexity without benefit.

The manifest MUST be **complete**: a `DT_NEEDED` with no manifest row is a
non-conforming object.

#### 12.3.1. Binary wire format (normative)

The PT_NOTE descriptor uses the following layout (all multi-byte integers are
little-endian):

```
ELF Note header:
  n_namesz  = 20                          // strlen("dev.straylight.cas") + 1
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
  digest        : [digest_len]           // raw hash bytes
  num_ver_reqs  : u8                     // GNU version requirements count
  per version requirement:
    vername_len : u16
    vername     : [vername_len]          // e.g. "GLIBC_2.34"
```

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

12.6. **`dlopen` closure MUST be recorded and staged; resolution MUST fail closed.**
Any object that may `dlopen` at runtime — including glibc's own NSS (`libnss_*`) and
any plugin mechanism — MUST have its runtime-reachable set recorded in the manifest
as a **dlopen-closure**, and every object in that closure MUST be staged in the CAS.
A `dlopen` of a name absent from the recorded closure MUST fail closed — it MUST NOT
fall back to a path search.

> (Informative.) The dlopen-closure is the hard operational part of this model.
> `DT_NEEDED` is statically present; `dlopen` targets may be computed at runtime.
> Producing a correct closure requires either observing the build/runtime `dlopen` set
> or constraining the configuration (e.g. a fixed `nsswitch.conf` whose modules are
> enumerable). This spec mandates recording, staging, and failing closed on a miss.

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

> **Target vs own (clarification).** The "dynamic linker" discoverable via sysroot is
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
- **14.15** PT_INTERP on every executable is the single deployment-wide floor-loader
  path; the floor loader is provided by the base layer and is itself content-addressed
  as a file (§12.2).
- **14.16** Every DT_NEEDED entry has a complete manifest row (soname → declared-fn
  digest + version-reqs); no DT_NEEDED is unmanifested (§12.3).
- **14.17** The floor loader resolves solely via the CAS, verifies each resolved
  object against its manifest digest, and consults no RPATH/RUNPATH/LD\_\*/ld.so.cache
  (§12.4, §12.5).
- **14.18** The dlopen-closure (incl. NSS) is recorded and staged; an unrecorded
  dlopen fails closed (§12.6).
- **14.19** Resolution is structurally deterministic: no ambiguous used-symbol binding
  ships without an ordering edge (§12.8).
- **14.8** Identity from symlink-, hardlink-, and copy-materializations is identical
  (§10).
- **14.9** Usable via the single-sysroot contract (§13).
- **14.10** A runnable image satisfies the container floor (§9): `/bin/sh`, core
  utilities, `/etc` essentials (passwd/group/nsswitch/CA bundle), writable `/tmp`,
  and `PATH` such that `which <driver>` and `<driver> --version` succeed.

**Conforming consumer:**

- **14.11** Honors the **declared** digest function for all hashing, including REAPI
  child `Directory`/`Tree` nodes, and **never assumes a default** (§4.4). _Defaulting
  the tree-node digest function is non-conformant (Appendix D)._
- **14.12** Derives identity from a projection, never from a materialized rootfs's
  inodes (§10.2).
- **14.13** Treats hints as optimization only: verifies before relying, uses an
  unverified hint solely for fetch planning (§6.5).
- **14.14** Includes the toolchain's execution identity (REAPI root `Directory`
  digest) in the action cache key when using it as a build input (§10.3).

---

---

# Appendices (informative)

## Appendix A. Building components with nix (the factory, and where it stops)

`nix build` produces a component; its realized **output content** is serialized
canonically (§4.1) and addressed by content. Its store path, `.drv`, and NAR
addressing never enter any projection (§11). Normalize embedded timestamps/build
paths before serialization. Same-filesystem reflink (`cp --reflink=auto`) is a
build-time optimization, not identity.

## Appendix B. Assembly and self-containedness (satisfying §12)

> **Revision 3.** This appendix is corrected: assembly **strips** RPATH (not sets it),
> sets PT_INTERP to the floor loader, and emits the content manifest.

Assembly is a two-phase pipeline:

**Phase 1: `toolchain_assemble`** (Buck2 rule: `prelude/nix/toolchain_assemble.bzl`)
places component content from Nix into the §8 FHS layout. Runs `patchelf` to rewrite
interpreter and RPATH to temporary paths for assembly-time linking verification.

**Phase 2: `std-oci-toolchain finalize`** (Buck2 rule: `prelude/oci/cas_toolchain.bzl`)
performs the §12 finalization on every ELF object in the assembled tree:

1. Resolves each `DT_NEEDED` soname via RUNPATH (the assembly-time path)
2. BLAKE3-hashes each resolved library
3. Strips `DT_RUNPATH`/`DT_RPATH` (zeros d_val to point at strtab[0]='\0')
4. Overwrites `PT_INTERP` in-place to `/lib/ld-std-oci-toolchain.so`
5. Appends a `PT_NOTE` segment with the content manifest (§12.3.1)
6. Stages resolved libraries to `/cas/<blake3-hex>`

This is an **atomic** operation per binary — either all five steps succeed or none
do. The output is a finalized tree (`toolchain/`) plus a CAS directory (`cas/`).

```
//  per ELF object — the `finalize` subcommand does all of this:
std-oci-toolchain finalize <each-elf> \
  --interpreter /lib/ld-std-oci-toolchain.so \
  --cas-dir ./cas

//  equivalent to (but atomic):
//    resolve DT_NEEDED → paths
//    blake3sum each resolved lib → digest
//    strip RPATH (zero d_val)
//    overwrite PT_INTERP
//    inject PT_NOTE (§12.3.1 binary format)
//    cp resolved libs → cas/<digest>
```

Receipts (empty / exit-0 ⇒ conformant):

```
patchelf --print-rpath <each-elf>                          // → ""   (stripped)
readelf -l <driver> | grep interpreter                     // → /lib/ld-std-oci-toolchain.so
std-oci-toolchain dump-manifest <each-elf>                 // → every DT_NEEDED has a row
grep -rIl /nix/store <toolchain>/                          // → empty (text refs too)
bwrap --ro-bind <floor> / --ro-bind <toolchain> /toolchains/<name> \
      --ro-bind ./cas /cas \
      --tmpfs /tmp -- /toolchains/<name>/bin/<driver> --version   // → exit 0
```

**Implementation:** `straylight-buck2-prelude/tools/std-oci-toolchain/` (C++23, linked
against glibc — it is a build-time tool, not a deployment artifact).

## Appendix C. Materialization and the three projections (illustrating §10)

One logical content tree, three projections, all sharing identity by content:
**OCI distribution** (sha256 tar layers + manifest; registries, signing, scanning) —
**REAPI execution** (BLAKE3 Merkle `Directory`; the action input root) — **container**
(materialized rootfs with the §9 floor; `docker run`, bwrap rootfs source, worker
base). Overlay/copy/hardlink/symlink are materialization and never enter a digest.

## Appendix D. Remote-execution interop — conformance gap (NativeLink) — CLOSED

> **Revision 3 (NativeLink track): this gap is now closed.** The fix shipped on
> branch `pr/sha-256-silent-default-fix` at `sensenet-ai/nativelink`.

**The bug (now fixed):** A worker hashed child `Directory` nodes with the action's
digest function, which arrived unset (protobuf default `0`) and **defaulted to
SHA256 server-side** via `TryFrom<i32>` in `nativelink-util/src/digest_hasher.rs`.
A BLAKE3 client then disagreed on child digests and directory outputs corrupted.

**The fix (three layers):**

1. **Execution server** (`nativelink-service/src/execution_server.rs`):
   unconditionally rejects `ExecuteRequest.digest_function == 0` with
   `INVALID_ARGUMENT`. This is the critical path — where Directory tree hashing
   happens and where the mismatch manifests as corruption.

2. **Global strict mode** (`nativelink-config/src/cas_server.rs`):
   `global.require_explicit_digest_function = true` makes ALL services reject
   unset digest functions. Recommended for new deployments.

3. **Legacy warning** (`nativelink-util/src/digest_hasher.rs`): when strict mode is
   off, the first unset `digest_function` emits a one-time `warn!` identifying the
   risk and pointing operators to the config fix.

**Regression test:** `execute_rejects_unset_digest_function` in
`nativelink-service/tests/execution_server_test.rs` asserts that the execution
server returns `INVALID_ARGUMENT` for `digest_function = 0`, preventing regression.

**Consequence for this specification:** With E4 shipped (Appendix E), the
two-projection model is fully implementable: sha256 stays at the OCI/registry
boundary, BLAKE3 is the RE digest function, and NativeLink no longer silently
substitutes one for the other. Matched-configuration discipline (E3) is retired.

## Appendix E. Migration sequencing

```
//  E1  self-containedness (ELF interp/RPATH; §12)         gates EXEC    — ✅ DONE
//  E2  identity over content, not inodes (§4.2, §10)      gates CACHE   — ✅ DONE
//  E3  match the digest function end-to-end per projection discipline, today (§4.4)  — RETIRED by E4
//  E4  RE consumer honors the declared function (§14.11)   the fix; retires E3  — ✅ DONE (pr/sha-256-silent-default-fix)
//  E5  container floor base layer (§9)                     gates "nice inside"  — ✅ DONE
//
//  ALL BLOCKING ITEMS COMPLETE.
//  sha256 floors the OCI projection; BLAKE3 lands at the REAPI Merkle projection;
//  the logical content binds them; hints relate them.
//
//  Remaining work: dlopen-closure tooling (§12.6), verify-manifest (§7.2),
//  REAPI projection end-to-end test (Stage 5 in buck2-prelude).
```

## Appendix F. Implementation status (rev 3, NativeLink track)

| Spec section | Component                                | Status                                | Location                                               |
| ------------ | ---------------------------------------- | ------------------------------------- | ------------------------------------------------------ |
| §4.1         | Canonical tar serialization              | Implemented                           | `prelude/oci/oci_image.bzl` (crane)                    |
| §5           | OCI distribution projection              | Implemented                           | `prelude/oci/oci_image.bzl`                            |
| §6.1–6.4     | REAPI execution projection               | **Unblocked** (was blocked on App. D) | Pending end-to-end test                                |
| §6.5         | Hints (annotations)                      | Implemented                           | `prelude/oci/oci_image.bzl` annotations                |
| §6.6         | REAPI unblock (NativeLink fix)           | **Shipped**                           | `sensenet-ai/nativelink@pr/sha-256-silent-default-fix` |
| §8           | Toolchain layout (FHS)                   | Implemented                           | `prelude/nix/toolchain_assemble.bzl`                   |
| §9           | Container floor                          | Implemented                           | `prelude/oci/container_floor.bzl`                      |
| §10          | Identity ≠ materialization               | Implemented                           | Design invariant (no inode refs in identity)           |
| §11          | No input-addressing                      | Implemented                           | `grep -rIl /nix/store` receipt passes                  |
| §12.1        | Strip RPATH/RUNPATH                      | Implemented                           | `std-oci-toolchain finalize`                           |
| §12.2        | Floor loader (`ld-std-oci-toolchain`)    | **Fully implemented**                 | `tools/ld-std-oci-toolchain/` (C++23, musl static-pie) |
| §12.3        | PT_NOTE content manifest                 | **Fully implemented**                 | `tools/std-oci-toolchain/` (emit, dump, finalize)      |
| §12.3.1      | Binary wire format                       | **Fully implemented**                 | `tools/ld-std-oci-toolchain/note_parse.h`              |
| §12.4        | CAS-only resolution                      | **Fully implemented**                 | Floor loader resolves from `/cas/<blake3>`             |
| §12.5        | No ambient authority                     | **Fully implemented**                 | No `LD_*` code paths in loader                         |
| §12.6        | dlopen-closure                           | **Specified only**                    | Not yet tooled                                         |
| §12.7        | No factory paths                         | Implemented                           | Finalize strips; receipt verifies                      |
| §13          | Single-sysroot contract                  | Implemented                           | `prelude/nix/toolchain_rules.bzl`                      |
| §14.11       | Consumer honors declared digest          | **Fixed (NativeLink)**                | Execution server rejects unset                         |
| —            | Property tests (ELF manifest)            | 12 properties                         | `tools/std-oci-toolchain/tests/`                       |
| —            | Property tests (finalize, libelf oracle) | 8 invariants                          | `tools/std-oci-toolchain/tests/`                       |
| —            | Floor loader unit tests                  | 7 tests                               | `tools/ld-std-oci-toolchain/tests/`                    |
| —            | Sandboxed execution (bwrap)              | Stage 6 + Stage 7                     | `prelude/oci/sandboxed_toolchain.bzl`                  |
| —            | Test matrix (clang 19/21, glibc/musl)    | Full                                  | `tests/toolchains/BUCK`                                |

**Unimplemented (tracked):**

- `std-oci-toolchain verify-manifest` (§7.2 consumer verification) — declared, returns "not yet implemented"
- dlopen-closure recording and `--check-closure` (§12.6) — needs `strace`/static analysis tooling
- REAPI projection end-to-end test (NativeLink BLAKE3 worker → client verifies Directory digests)
