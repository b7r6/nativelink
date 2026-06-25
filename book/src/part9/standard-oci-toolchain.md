<!-- SPDX-FileCopyrightText: 2026 Straylight Software -->
<!-- SPDX-License-Identifier: MIT -->

# Standard OCI Toolchain Specification

**Version:** RC1, revision 2 (feature-frozen; seeking implementation feedback prior to ratification)
**Supersedes:** RC1 rev 1, and *Toolchain-as-CAS — Design Proposal* (now informative, Appendices A–E).

> **Revision 2.** Splits the digest model into two content-addressed **projections**:
> an OCI **distribution** projection floored at **sha256** (registry-portable, the
> serialization that does not scare people) and a REAPI **execution** projection in
> **BLAKE3** (the worker-side Merkle tree). Adds §6 (REAPI correspondence — verbs,
> Merkle, hints), §7 (cross-projection agreement), and §9 (the container projection
> floor — `docker run` must be *nice inside*). Conformance (§14) expanded.

______________________________________________________________________

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

______________________________________________________________________

## Requirements language

**MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD
NOT**, **MAY**, **OPTIONAL** per RFC 2119 / RFC 8174 when capitalized. §1–§14 are
**normative**; Appendices A–E are **informative**.

______________________________________________________________________

## 1. Scope

1.1. Defines the **identity**, **projections** (OCI distribution, REAPI execution,
container), **layout**, **self-containedness**, **use contract**, and **conformance**
of a toolchain artifact and its consumers.

1.2. Does not define how components are built (App. A), assembled (App. B),
materialized (App. C), or which execution backend is used (App. D). None of these
MUST affect identity.

______________________________________________________________________

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
- **diff_id** — sha256 over the *uncompressed* layer tar; the OCI content identity of
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

______________________________________________________________________

## 3. The model (normative framing)

3.1. The artifact is the **logical content tree**. Its identity is a function of its
bytes — not the recipe, environment, build path, or time.

3.2. The tree has multiple **projections**, each serializing the *same* content for a
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

______________________________________________________________________

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
and MUST NOT substitute a default. *Defaulting the digest function for tree-node
hashing is non-conformant (§14.11; Appendix D).*

______________________________________________________________________

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

______________________________________________________________________

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

| intent | OCI / registry | REAPI |
|---|---|---|
| fetch only what's missing | manifest + blob `GET` after diff | `FindMissingBlobs` → `BatchReadBlobs`/ByteStream `Read` |
| use a toolchain in a build | reference image by manifest digest | merge its root `Directory` digest into the action `input_root_digest` for `Execute` |
| expand / inspect the tree | unpack layers | `GetTree` from the root `Directory` digest |
| dedup check | registry blob existence | `FindMissingBlobs` over file-blobs |

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

______________________________________________________________________

## 7. Cross-projection agreement

7.1. A producer MUST ensure every projection serializes the **same** logical content
tree (§4). The OCI diff_ids, the REAPI file-blobs, and the materialized rootfs MUST
all reduce to the same path→content mapping.

7.2. A consumer MUST be able to verify agreement — e.g. by materializing the OCI
projection and reserializing the REAPI projection (or comparing per-file content).
A mismatch MUST be treated as a corrupt image.

______________________________________________________________________

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
> compiler stamps into *programs it builds*. It is NOT the runtime loader for the
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

______________________________________________________________________

## 9. The container projection floor

9.1. When materialized as a runnable OCI container, a conforming image MUST be
*usable inside*: a freshly `docker run` (or `podman run`) container MUST present a
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

______________________________________________________________________

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

______________________________________________________________________

## 11. Prohibition on input-addressing

11.1. Toolchain and component identity MUST be content-derived (§4–§6).

11.2. Input-addressed identifiers — nix derivation hashes, non-content-addressed
store-path hashes, NAR addressing of non-CA outputs — MUST NOT be used as identity,
and MUST NOT appear in any manifest, config, identity-bearing annotation, or layer
content.

11.3. (Rationale.) Input-addressing ties identity to provenance: identical content
from two recipes fails to deduplicate, and a recipe is *trusted* to reproduce rather
than *verified*. Content-hashing machinery used for input-addressed identity is the
appearance of a CAS without its guarantee. This specification requires the guarantee.

______________________________________________________________________

## 12. Self-containedness (content-closure)

> **Revision 3 note.** This replaces the prior §12 wholesale and corrects Appendix B.
> The prior §12 closed the artifact over its *directory tree* (path-closure); this
> revision closes it over its *content manifest* (content-closure). The distinction
> is load-bearing and is the whole point of the floor loader.

**Principle.** A conforming toolchain's own ELF objects MUST resolve every dynamic
dependency by **content** — a digest recorded in the object's manifest (§12.3) —
and MUST NOT resolve any dependency by an ambient or embedded **path**, with
exactly one permitted external path reference: the floor dynamic loader (§12.2).
Self-containedness is closure over the manifest, not over the tree.

> This section governs the runtime resolution of the **toolchain's own binaries**
> (e.g. `clang` finding `libstdc++.so.6` in order to *run*). It does **not** govern
> the toolchain's library *search* when used as a compiler to build a user program
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
(`/lib/ld-straylight.so`), identical across all toolchains. This is the **sole
permitted external path reference** of §12 — the one input-addressed *location*
anchor in an otherwise content-addressed system. The kernel resolves `PT_INTERP`
during `execve` before any CAS-aware code runs, so the loader itself cannot be named
by content at the interpreter slot.

The floor loader (`ld-straylight.so`) is:
- Statically linked (no dependencies of its own)
- Provided by the container floor base layer (§9.3)
- Content-addressed *as a file* (its own digest is in the CAS) even though its
  *location* is fixed
- A dispatch shim: it reads the binary's manifest, resolves the binary's *real*
  dynamic linker (the per-toolchain `ld-linux-x86-64.so.2` from that toolchain's
  glibc) from the CAS, and transfers control to it

This allows multiple glibc versions to coexist: each toolchain's real `ld-linux` is
content in the CAS, resolved by hash. The floor loader is universal across all
toolchain versions.

> **§12.2 vs §11 (prohibition on input-addressing).** The floor-loader path is a
> fixed *location*, not a fixed *identity*. Its identity is still its content digest;
> only its PT_INTERP location is fixed. Location ≠ identity. This is a deliberate,
> audited exception — the kernel requires it — not an §11 violation.

12.3. **The `DT_NEEDED` → content manifest.** Each ELF object MUST carry a manifest
binding **every** `DT_NEEDED` entry to (a) the **content digest** of the providing
object the build resolved, and (b) the **versioned-symbol requirements** bound
against that object. The manifest MUST be carried in exactly one of (precedence
order):

```
1. an ELF note in a PT_NOTE segment, note name  "dev.straylight.cas"   (preferred)
2. a dedicated section                          ".straylight.cas"
3. a sidecar file adjacent to the binary        "<binary>.cas"
```

Manifest schema (one row per `DT_NEEDED`), normative fields:

```
soname            DT_NEEDED string, verbatim          e.g. "libstdc++.so.6"
digest            <fn>:<hex> of the providing object   e.g. "blake3:abc123…"
version-reqs      sorted set of required version nodes e.g. ["GLIBC_2.14","GLIBC_2.34"]
```

The digest function MUST be declared per-entry (`<fn>:<hex>`) and a consumer MUST NOT
assume a default (cf. §4.4). The manifest MUST be **complete**: a `DT_NEEDED` with no
manifest row is a non-conforming object.

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

______________________________________________________________________

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
> the **target** interpreter — the one the compiler stamps into *programs it builds*
> (via `-Wl,-dynamic-linker,<sysroot>/lib/ld-linux-x86-64.so.2`). This is distinct
> from the resolution of the *toolchain's own binaries* at runtime, which is governed
> by §12 (the floor loader + CAS). §13 is about the compiler's search behavior when
> building user code; §12 is about the toolchain binary's own library resolution.

13.3. Compilation databases SHOULD record the toolchain-root-relative invocation
verbatim (one greppable toolchain token).

______________________________________________________________________

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
  object against its manifest digest, and consults no RPATH/RUNPATH/LD_*/ld.so.cache
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
  child `Directory`/`Tree` nodes, and **never assumes a default** (§4.4). *Defaulting
  the tree-node digest function is non-conformant (Appendix D).*
- **14.12** Derives identity from a projection, never from a materialized rootfs's
  inodes (§10.2).
- **14.13** Treats hints as optimization only: verifies before relying, uses an
  unverified hint solely for fetch planning (§6.5).
- **14.14** Includes the toolchain's execution identity (REAPI root `Directory`
  digest) in the action cache key when using it as a build input (§10.3).

______________________________________________________________________
______________________________________________________________________

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

`toolchain_assemble` places component content into the §8 layout, then makes every
ELF object resolution-free: strip RPATH/RUNPATH, set PT_INTERP to the floor loader,
and emit the content manifest. It does **not** set an RPATH.

```
//  per ELF object — STRIP, anchor, manifest:
patchelf --remove-rpath --set-interpreter /lib/ld-straylight.so  <each-elf>
straylight-cas emit-manifest <each-elf>       // writes PT_NOTE "dev.straylight.cas"
                                              // rows: soname → blake3 + version-reqs
straylight-cas emit-dlopen-closure <toolchain>  // records NSS + plugin closure (§12.6)
```

Receipts (empty / exit-0 ⇒ conformant):

```
patchelf --print-rpath <each-elf>                          // → ""   (stripped)
readelf -l <driver> | grep interpreter                     // → /lib/ld-straylight.so
straylight-cas verify-manifest <each-elf>                  // → every DT_NEEDED has a row
grep -rIl /nix/store <toolchain>/                          // → empty (text refs too)
ld-straylight --check-closure <toolchain>                  // → dlopen-closure complete
bwrap --ro-bind <floor> / --ro-bind <toolchain> /toolchains/<name> \
      --tmpfs /tmp -- /toolchains/<name>/bin/<driver> --version   // → exit 0
```

## Appendix C. Materialization and the three projections (illustrating §10)

One logical content tree, three projections, all sharing identity by content:
**OCI distribution** (sha256 tar layers + manifest; registries, signing, scanning) —
**REAPI execution** (BLAKE3 Merkle `Directory`; the action input root) — **container**
(materialized rootfs with the §9 floor; `docker run`, bwrap rootfs source, worker
base). Overlay/copy/hardlink/symlink are materialization and never enter a digest.

## Appendix D. Remote-execution interop and a conformance gap (NativeLink)

A worker hashes child `Directory` nodes with the action's digest function, which
arrives unset and **defaults to SHA256 server-side**; a BLAKE3 client then disagrees
on child digests and directory outputs corrupt. This violates **§4.4 / §14.11**: the
consumer assumed a default instead of honoring the declared function. Remediation:
thread the declared function from the execute request into tree-node hashing, with a
regression asserting it is request-sourced, not a constant. Until then, run only
matched configurations (both ends the same function); that is discipline, not the
fix. Note the two-projection model makes this clean: **sha256 stays at the OCI
boundary, BLAKE3 is the intended RE function**, and the bug is precisely a consumer
refusing the declared BLAKE3.

## Appendix E. Migration sequencing

```
//  E1  self-containedness (ELF interp/RPATH; §12)         gates EXEC    — blocking
//  E2  identity over content, not inodes (§4.2, §10)      gates CACHE   — blocking
//  E3  match the digest function end-to-end per projection discipline, today (§4.4)
//  E4  RE consumer honors the declared function (§14.11)   the fix; retires E3
//  E5  container floor base layer (§9)                     gates "nice inside"
//
//  ORDER:  E1 ∥ E2  (blocking) → E3 (today) → E4 (when patchable; E4 ⟹ ¬E3) ;  E5 ∥ rest
//  E1 → anything RUNS.  E2 → local and RE SHARE CACHE.  E5 → docker run is nice inside.
//  sha256 floors the OCI projection; BLAKE3 lands at the REAPI Merkle projection;
//  the logical content binds them; hints relate them.
```
