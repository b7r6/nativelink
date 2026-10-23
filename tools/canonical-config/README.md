# Experimental canonical schema and offline converter

This package proposes the first local singleton/filesystem manifest and converts
one narrow legacy layout into an explicit proposal. **No runtime consumes this
format.** Every report says `deployable: false` and `cutover_qualified: false`,
including reports with a complete candidate. This is the parser/converter slice
of M4a.4, not its runtime refusal, physical preflight, or deployment gate.

The library contains only the canonical schema and a bounded data parser. The
legacy DTO and converter compile only into the offline binary. This standalone
Cargo workspace depends on `serde`, `serde_json`, and `serde_json5`; its lockfile
uses registry versions/checksums from the repository lockfile. Neither the
server dependency closure nor a Redis client is linked. Root `cargo test --all`
does not discover this standalone workspace; run its tests explicitly.

```bash
cargo test --manifest-path tools/canonical-config/Cargo.toml --locked --offline --profile smol
cargo clippy --manifest-path tools/canonical-config/Cargo.toml --locked --offline --profile smol --all-targets --all-features -- -D warnings
cargo tree --manifest-path tools/canonical-config/Cargo.toml --locked --offline --all-features --target x86_64-unknown-linux-gnu

cargo run --manifest-path tools/canonical-config/Cargo.toml --locked --offline --profile smol \
  --bin experimental-canonical-convert -- \
  tools/canonical-config/fixtures/legacy.json tools/canonical-config/fixtures/mapping.json

bazel test //tools/canonical-config:parser_test //tools/canonical-config:conversion_test
bazel build //tools/canonical-config:experimental-canonical-convert
```

The Bazel targets are manual and use the existing `@crates` dependencies. Exit
code 0 means a complete experimental mapping was emitted, 2 means blocking
diagnostics, and 1 means command-line, input I/O, or output I/O failure. The tool
opens only its two input files and stdout. It does not open any referenced path,
read credentials or environment variables, connect to services, create stores,
move data, initialize namespaces, or grant collection ownership.

## Canonical declarations

[`fixtures/canonical.json`](fixtures/canonical.json) demonstrates the full
`experimental_singleton_filesystem_v1` schema. These are fixture declarations;
the paths, credentials, identities, and evidence references are not deployment
instructions or verified resources.

| Field | Declared fact |
| --- | --- |
| `instance_name` | One REAPI instance, including the explicitly empty instance. |
| `authority` | Node, epoch, authority namespace, trusted client principal and journal root. |
| `cas` | Physical namespace identity and an owned filesystem root. |
| `temporary_action_cache` | Distinct physical namespace identity and fresh sink root. |
| `listeners.client` | Loopback socket and server certificate/key/client-CA references. |
| `listeners.worker` | Distinct loopback socket and separate declared client trust reference. |
| `worker` | Worker/process identity, execution-configuration identity, local root and capacity. |
| `worker.transfer` | The worker listener address and explicit client/server TLS references. |

The proposed data roots have fixed `content` and `temp` children. The schema
has no store/scheduler enums, recursive composition, listener service lists,
clock or feature selectors, publication policy, template, or collector switch.
The fixed proposed client role names Execution, Operations, ActionCache, CAS,
ByteStream and Capabilities. The fixed worker role names AuthorityWorkerApi,
CAS and ByteStream. These describe proposed construction, not runtime exposure.

`parse_canonical` rejects any top-level `stores` or `schedulers` key, including
null/empty/scalar values, before canonical deserialization. Duplicate keys also
fail. Every struct rejects unknown fields. The parser performs no I/O or runtime
construction. **The existing server has not been wired to this parser**, so
these tests do not establish the CFG1 process-startup gate.

Pure validation checks nonzero identities/capacity, literal absolute normalized
paths, distinct 64-hex physical namespace identities, lexically disjoint roots,
and fixed worker transfer routing. It cannot establish directory ownership,
resolve symlinks or bind mounts, inspect trust-store contents, settle outstanding
requests, or validate incarnation reuse. Authority domain identity and physical
namespace identity are different facts. A future bootstrap must bind the
domain, principal, instance and physical role identities durably; a filename
or accepted manifest supplies no such proof.

## Supported conversion and explicit blockers

The first source shape is exactly two direct filesystem stores with literal
content/temp paths; one `simple: {}` scheduler; two explicitly named listeners;
one instance; and no embedded legacy workers. The client listener has one each
of CAS, AC, ByteStream, Execution and Capabilities. The worker listener has
WorkerApi and may have CAS/ByteStream. The explicitly named worker transfer
listener must provide CAS/ByteStream and cannot expose AC. References must
resolve uniquely. Source content/temp paths must be lexically distinct.

[`fixtures/mapping.json`](fixtures/mapping.json) requires explicit source role
bindings, a complete candidate, and declarations identifying old namespaces,
data encoding, old AC/collector owners, proposed namespace owner, encoding and
ownership evidence, credential locations, every listener's
worker exclusion, old-domain fencing, new worker cohort, and retirement of
external worker publication/templates. Evidence references are opaque declared
references; the tool never follows them. Fresh namespaces with old data left
untouched are the only supported data disposition. Both proposed CAS and AC
start cold; no import or relocation is implemented.

The deterministic diff covers authority, CAS, temporary AC, both listener roles,
worker transfers/local execution, publication/diagnostic ownership and collection.
It includes every accepted source field, proposed provider/path/namespace/encoding,
credential reference and listener trust declaration. Filesystem-only fields such
as cloud endpoints/buckets and shard/dedup representations are explicitly absent.

Any additional source field or unsupported shape blocks conversion and emits
no candidate. Shards, size partitioning, dedup, compressed data, cloud placement,
gRPC stores, Redis authority, memory-only layouts, aliases, legacy worker
publication and diagnostic templates are blocked. The diagnostic walk visits
all branches within its bounds; unsupported input values are withheld, while
recognized feature names and JSON Pointer locations remain visible. A caller
cannot make an unsupported source safe merely by supplying mapping declarations.

Identity and capacity fields require integer tokens; floating-point or exponent
spellings are rejected rather than coerced. The pinned JSON5 reader currently
rejects decimal integer tokens outside the signed 64-bit range and hexadecimal
integer tokens above the unsigned 32-bit range. Accepted decimal integers retain
exact precision, including values above the IEEE-754 exact-integer range.

Both inputs are limited to 1 MiB, 32 nesting levels, 16,384 values and 4,096 bytes
per string/key. Duplicate keys, nonfinite numbers, non-object roots and strings
containing `$` or `~` fail; no interpolation is resolved. Unsupported-feature
diagnostics are bounded to 128 entries and 2,048-byte paths, with an explicit
incomplete-inventory blocker on exhaustion. Raw parser errors and unsupported
credential values are never emitted. Accepted literal credential **file
references**, placement paths, names and evidence references are intentionally
included in the reviewable diff; do not put secrets in those fields.

Before a runtime entry point can use a future qualified version, it must perform
CFG1 refusal before Tokio/tracing or component effects, prove physical namespace
identity/ownership/isolation, verify the TLS boundary, qualify the durable clock
and admission/recovery path, and pass the combined cold-sink/fencing/new-cohort
cutover gate. Collection stays disabled. None of these gates is discharged by
the parser or converter.
