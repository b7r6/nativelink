# Experimental offline migration inventory

This example reads a legacy JSON5 file and writes a deterministic JSON refusal
report to stdout. It emits no candidate configuration. Every report has
`deployable: false` and `candidate_configuration: null`; successful report
generation exits with code **2**, while command-line or I/O errors exit with 1.
It opens only the supplied input file for reading and stdout for the report.
It does not construct stores or schedulers, open credential files, contact
services, write configuration, move data, or grant collection ownership.

```bash
cargo run --locked -p nativelink-config \
  --example experimental_migration_inventory -- /path/to/legacy.json5
cargo test --locked -p nativelink-config \
  --example experimental_migration_inventory
bazel test //nativelink-config:experimental-migration-inventory-test
```

The experiment implements an inventory slice of the categorical source plan's
[configuration refusal and offline migration requirements](../../../nativelink-scheduler/design/categorical/source-plan.md#configuration-refusal-and-offline-migration).
The [source map](../../../nativelink-scheduler/design/categorical/source-map.md)
records the intended dispositions. Neither document makes this example a
supported converter or establishes a cutover gate.

The report enumerates named and nested constructions, all current store and
scheduler selectors, recognized cloud providers, reference edges, role
routes, missing or ambiguous references, and cyclic edges. Paths are JSON
Pointers into the original input. Both branches of size partitioning, every
shard, and both dedup index/content destinations remain visible. Unused declared
constructions remain in the inventory. Roles describe configured routes and
potential connections, not observed live traffic or verified ownership.
Each reference reports `matching_declarations`; `targets` contains only a unique
resolved target. Ambiguous references do not duplicate every candidate into
every edge. Missing/null worker history-store settings receive an explicit
question about the implicit parent-CAS fallback in the schema, not an inferred
live-publication claim.

Only component/reference names, reviewed selector/provider labels, and input
field locations are emitted. Endpoint, bucket, prefix, local path, credential,
script, property, and diagnostic-template values are withheld. Review those
values in the original file; the report does not replace a placement diff.
Store names and field names remain visible, so avoid embedding
secrets in them. Unknown topology, namespace ownership, physical encoding,
resource semantics, direct worker publication, and listener trust remain
blocking questions. A shared worker/writable-AC listener is explicitly flagged;
separate listeners still require evidence of worker exclusion, including any
external ingress proxies.

The input is bounded to 1 MiB and 64 nesting levels. JSON5 duplicate keys and
numbers that are not finite are rejected. At most 4096 constructions are inventoried;
exhaustion marks the report incomplete. Inventory traversal also has a global
65,536-item budget, and graph analysis has a separate
65,536-step budget. Graph expansion stops before exceeding its budget; repeated
ambiguous references never materialize a target cross product. Budget exhaustion
sets `inventory_complete: false` and emits a blocking diagnostic. Unresolved
references and absent cycle/ownership findings in a partial report are not
evidence that the topology is safe. Completion of inventory never means schema
validity or cutover qualification. The current Rust `CasConfig`
deserializer validates literal input syntax and field types. Its acceptance is
**not** semantic runtime validation: this tool does not run factory constraints,
store-conflict checks, single-use worker validation, default normalization, or
endpoint probes. Any string containing `$` or `~` prevents typed deserialization
and records unresolved interpolation; no environment or home expansion occurs.
The raw syntactic inventory is still useful but cannot establish schema validity.

This example intentionally uses the existing config crate, whose current
dependency closure includes `nativelink-error` and Redis-related dependencies.
It has no Redis client calls or runtime construction path, but it does **not**
satisfy the final requirement for a legacy parser with no Redis dependency.
Separating that parser, producing a qualified canonical schema and complete
placement diff, implementing runtime refusal, verifying provider contracts,
and rehearsing externally fenced authority/storage cutover remain later work.
