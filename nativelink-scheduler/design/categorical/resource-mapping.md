# Fixed resource mapping inventory and proposed v1 boundary

Status: source inventory and implementation proposal, 2026-10-22. This is not an
implemented RPC adapter, fleet survey, converter certification, or support claim.
The existing authority model has `cpu_millis`, `memory_bytes`, and `slots` only.
Its raw typed fixtures do not establish the interpretation of REAPI properties.
This document fills the inventory obligation in [contract.md](contract.md); the
remaining adapter and migration fixtures below are still required.

## Observed sources of meaning

The existing protocol does not carry a resource-unit declaration. The scheduler
configuration selects both property names and matching rules. Consequently a
number on its own cannot identify either a resource or its unit.

| Source | Observed behavior and implication |
| --- | --- |
| `nativelink-service/src/execution_server.rs:220`, `build_action_info` | Uses the complete Action platform when its property map is nonempty. Only an empty map triggers fetching Command and using Command platform. It does not merge the two. Repeated names currently overwrite earlier values. |
| `nativelink-scheduler/src/platform_property_manager.rs:89` | Configured `Minimum` parses `u64` without trimming and reserves the numeric value; there is no name or unit registry. `Exact`, `Priority`, and `Ignore` are configuration facts; undeclared names become `Unknown`. |
| `nativelink-util/src/platform_properties.rs:47` | `Unknown` requires equal values only on workers that advertise that name; a missing name passes. `Exact` requires presence and equality; `Priority` requires presence but accepts any value; `Ignore` accepts missing names. |
| `nativelink-scheduler/src/api_worker_scheduler.rs:66`, `:1215` | The `BestFit` strategy actually prefers workers with equal `Priority` values. The older TODO describing preference as unimplemented is not the implementation. |
| `nativelink-config/src/cas_server.rs:756`, `CpuUnit` and `CapacityConfig` | Default `cpu_count` means whole cores. The same name may instead mean millicores when configured with `cpu_unit: millicores`. Both property names are configurable. Default `memory_kb` means KiB, not decimal kilobytes. |
| `nativelink-worker/src/capacity.rs:105`, `advertised` | CPU observation is millicores; subtracts worker overhead, then either retains millicores or floors to whole cores. Memory observation is KiB; subtracts overhead, then divides by enforcement headroom. These operations currently saturate. |
| `nativelink-worker/src/capacity.rs:130`, `without_cgroup` | Missing usable cgroup limits preserve configured CPU and memory properties only if both names exist. Presence alone does not establish valid numbers, units, or physically safe capacity. |
| `nativelink-config/src/cas_server.rs:925`, `ResourceEnforcementConfig` | Memory and disk reservations are KiB under configurable names, default `memory_kb` and `disk_kb`. Default memory headroom is 20 percent; disk guarding is separately configured. Missing memory property disables enforcement for that action. |
| `nativelink-worker/src/worker_utils.rs:29` | Worker `values` emits one property per value. `query_cmd` emits one trimmed property per output line. These are not sets in the scheduler: `nativelink-service/src/worker_api_server.rs:193` inserts them into a map, keeping the last value. |
| `nativelink-config/src/cas_server.rs:1356`; `nativelink-worker/src/local_worker.rs:1457` | `max_inflight_tasks` is a separate count, not a platform property. Zero currently means unlimited. `single_use` advertises one slot independently of the configured count and also has a lifetime restriction. |
| `nativelink-worker/src/local_worker.rs:468` | Keepalive reports `free_memory_kb`, a free-memory observation in KiB. It is not the authority model's aggregate admission ceiling including existing charges. |
| `nativelink-worker/src/running_actions_manager.rs:232`, `reservation_of` | Measurement metadata reads the literal `cpu_count` name, but accepts configured memory/disk names. CPU has no unit tag. It trims quantities, unlike scheduler minimum parsing. |
| `nativelink-worker/src/running_actions_manager.rs:5390` | Worker construction reads Action properties, with no Command fallback, then overlays dispatch platform properties only when resource enforcement is configured. With enforcement disabled, scheduler-normalized reservations are not necessarily what worker environment or measurement sees. |

The cgroup derivation is useful evidence, but the target must still validate all
arithmetic and reconcile existing charges. The present nearest-limited-ancestor
observation is not a proof that no more restrictive parent, CPU affinity, or
other physical obligation applies. This inventory does not qualify discovery
of physical capacity.

## Names actually present in configurations

`nativelink-config/examples/basic_cas.json5:45` and the Buck2/BuildStream
integration configurations declare the following numeric `Minimum` names:

| Name | Meaning established by implementation | Fits current authority vector? |
| --- | --- | --- |
| `cpu_count` | Whole cores by default; configurable millicore scale on the same name | Yes, only after resolving scale |
| `memory_kb` | KiB in capacity, hints, memory admission, enforcement, and measurement | Yes, multiply by 1024 with overflow rejection |
| `disk_kb` | KiB in hints, disk admission, enforcement, and measurement; not in that basic example's property map | No |
| `network_kbps` | Arbitrary configured numeric minimum; this source review establishes no consistent accounting unit beyond the name | No |
| `disk_read_iops`, `disk_write_iops` | Arbitrary configured numeric minima | No |
| `disk_read_bps`, `disk_write_bps` | Arbitrary configured numeric minima | No |
| `shm_size`, `gpu_count` | Arbitrary configured numeric minima | No |

Ordinary exact names in those examples include `gpu_model`, `cpu_vendor`,
`cpu_arch`, `cpu_model`, `kernel_version`, and `ISA`. `OSFamily`,
`container-image`, and `lre-rs` are configured as `Priority`, often with an empty
worker value. `InputRootAbsolutePath` is configured as `Ignore`. None can be
silently changed to an exact requirement while claiming equivalent placement.
`kubernetes/nativelink/nativelink-config.json5:101` goes further and declares CPU,
memory, and most of the same names as `Priority`; those numeric-looking values
do not currently charge resources there. The Mongo example even declares
`cpu_arch` as `Minimum`; a converter must report the literal unsupported rule,
not infer an intended correction from the name.

Docker Compose examples commonly advertise `cpu_count` from `nproc`, omit
memory entirely, and leave the slot count at its unlimited default. The checked
self-test invocation explicitly asks for `cpu_count=1`. These examples are
source evidence of the default core unit; they cannot certify an unseen fleet.

Other ordinary properties affect execution as well as placement. In particular
`supports-workers=1` and `requires-worker-protocol` select the worker's persistent
worker path (`running_actions_manager.rs:684`). Additional environment entries
can refer to arbitrary property names. They must remain in the recorded dispatch
payload even if a separate canonical projection supplies matching requirements.
Under the proposed exact-property rule, workers must explicitly advertise any
such requested properties; absent-worker matching is a migration change.

## Historical hint behavior that must not be guessed

`nativelink-scheduler/src/historical_resource_scheduler.rs` establishes:

1. Hint lookup is ordered: target plus mnemonic, target alone, action digest,
   command digest, then mnemonic alone. Digest text accepts `hash/size` and
   `hash-size`; this is a hint-key normalization, not a resource alias.
2. A hint can provide `cpu_count`, `memory_kb`, `memory_mib`, `disk_kb`, or a
   class. Explicit hint numbers override the class for the corresponding
   dimension. `memory_kb` wins when both memory spellings exist;
   `memory_mib * 1024` saturates. These are hint-file fields; `memory_mib` is not
   an implemented REAPI platform-property alias.
3. An applicable nonzero hint raises a client's minimum to the larger number;
   it never lowers a valid client number. A malformed client number can
   currently be overwritten by the hint before the inner parser sees it.
4. With no usable hint, cold defaults fill absent properties only. A client
   value of zero is present and prevents a cold default. The class order is
   mnemonic, largest matching timeout threshold, default class, then raw cold
   numbers. A missing timeout is not interpreted consistently by service and
   worker construction, so timeout-based classes also need explicit migration.
5. When classes or nonzero cold defaults define the reserved dimensions, hint
   numbers on dimensions not named there are discarded. Without either, all
   dimensions are eligible. Reloaded external JSON and class selection are
   mutable inputs, not replay-safe authority facts.

Relevant existing tests are `historical_resource_scheduler_test.rs`:
`add_action_applies_historical_resource_hint`,
`cold_start_fills_only_absent_properties`,
`a_hint_dimension_the_ladder_does_not_reserve_is_dropped`, and
`a_mnemonic_alone_does_not_outrank_a_digest_hint`. CPU unit behavior is explicitly
tested by `capacity_test.rs::cpu_is_advertised_in_whole_cores_unless_told_millicores`.
These tests were inspected for this inventory; this document makes no new test
execution claim.

## Proposed one-version mapping

The initial adapter should implement the following single mapping, without a
configurable name/unit selector. New spellings require a future explicit
protocol version and migration, not a runtime guess.

| Accepted selected-platform input | Canonical meaning |
| --- | --- |
| `cpu_count=N` | `cpu_millis = checked(N * 1000)`; `N` is whole cores |
| `memory_kb=N` | `memory_bytes = checked(N * 1024)`; `N` is KiB |
| Missing CPU or memory property | Zero declared minimum on that dimension; it does not declare zero physical use |
| Every admitted execution | Exactly one slot, independent of CPU/memory declarations |
| Other supported ordinary name/value | Exact case-sensitive string requirement; absence on the worker fails |

Use canonical ASCII unsigned decimal grammar `0` or `[1-9][0-9]*`. Reject
leading/trailing whitespace, signs, fractional values, unit suffixes, leading
zeroes, empty values, and values or products that overflow `u64`. These are
intentional stricter boundary rules; conversion reports any affected inputs.
Explicit zero is valid and does not disable the mandatory one-slot charge.
The mapping supplies neither a fabricated memory estimate nor a machine-dependent
CPU default. A zero declared minimum is not a hard memory limit or a fit guarantee.

`cpu_millis`, `cpu_millicores`, `memory_bytes`, `memory_mib`, `memory_mb`, and
`slots` are not additional client aliases in v1. Reserve these spellings and
reject them with a precise unsupported-unit/property error instead of silently
interpreting them as exact strings. Native worker messages already carry typed
CPU, memory, and slot fields; their encoding needs no platform aliases. Reject
the unsupported resource names in the inventory table, including `disk_kb`,
rather than silently dropping their requirements or matching them as strings.
A previously unknown arbitrary property remains exact; its name does not become
a resource merely because it contains a number or looks like a unit.

Select Action platform if its raw property list is nonempty, otherwise select
Command platform. Do not merge, and do not fall back because Action validation
failed. Validate the selected list before converting to a map. Reject every
repeated name, including identical duplicates, so order never chooses semantics.
An unselected Command platform does not override or add Action requirements;
conflicts between the two lists therefore have the existing whole-list winner.
Preserve the original accepted Action/Command/ExecuteRequest facts separately
from this projection; validation must not rewrite CAS objects or digests.

Worker registration requires one unambiguous value per exact property and
explicit typed finite capacity. `max_inflight_tasks > 0` maps to slot capacity;
`single_use` maps to one slot plus its existing separate lifetime fence. Zero
unlimited slots require a concrete new finite capacity and block automatic
conversion. Resource properties are removed only from the matching projection,
not from the accepted payload needed for diagnostics or an explicitly preserved
worker environment. Do not expose two independently writable authorities for
the same reservation: the charged typed vector in dispatch is authoritative.

The worker's typed aggregate admission ceiling must include existing charges,
as required by the authority contract. A free-memory sample cannot be copied
into that field. Translation requires a reconciled observation of the existing
charges and physical obligations; without it, close admission conservatively.
Neither reconnect nor a newly observed capacity releases an old reservation.

## Requirements, estimates, and strict migration blockers

Validate client quantities first. An accepted-history estimate may subsequently
raise CPU/memory reservation using the checked maximum of each dimension and the
client minimum. It cannot lower the minimum, alter exact properties, or remove
the one-slot charge. Record the selected normalized vector with the committed
request/dispatch; never recompute it from a live hint file during replay or
retry. Until a typed accepted-history path exists, there is no estimate input
and the vector is the declared minima plus one slot. Separating immutable
client requirements from later estimate changes is an additional core/schema
obligation; the present model stores a single immutable vector.

A converter may emit a non-deployable candidate and a list of exact reasons.
It must block automatic activation for:

- `cpu_count` on a millicore scale, custom resource names, or a mixed/unknown
  fleet unit. Do not infer scale from magnitude. A fractional-core legacy
  request cannot be expressed exactly by this initial client mapping.
- `Minimum` on a dimension other than the two accepted names; `Priority` or
  `Ignore` on any name; resource names configured as non-minimum rules; and
  arbitrary property additions/removals/replacements lacking an equivalence
  proof against the fixed normalization.
- Multiple values or multi-line property commands, even when the old worker
  effectively kept the last value. The offline converter does not execute
  commands, contact the fleet, or turn `nproc` into a machine-independent fact.
- Unlimited or unresolved slot capacity, unresolved worker memory/capacity
  units, and any assumption that old free-memory observations release charges.
- Historical hint files, class ladders, mnemonic/timeout selectors, cold
  defaults, or memory-escalation rules treated as automatically preserved.
  Finite import needs explicit unit/provenance validation; ambiguous duplicate
  hint keys, both memory aliases, overflow, and unknown classes are errors.
  These policy wrappers are removed, not hidden inside the new adapter.
- Existing clients that rely on missing-worker `Unknown` matching, ignored
  `InputRootAbsolutePath`, permissive `OSFamily`/image properties, or implicit
  persistent-worker properties. Report the concrete changed name and required
  worker/client migration; do not discard the constraint to make work run.
- A worker that reconstructs its effective requirements from Action alone,
  conditionally applies normalized platform values only when enforcement is
  enabled, or infers the charged vector from mutable properties after dispatch.

Before activation, adapter tests must cover Action/Command whole-list selection,
duplicate and malformed input rejection, all numeric boundaries and unsupported
names, CPU scale conflicts, mandatory slot charging, strict exact-property
matching, immutable reservation delivery with enforcement on and off, and
maximum-with-accepted-estimate behavior. Converter fixtures must exercise the
actual example shapes above and report the removed dimensions/policies. This
is a deliberately bounded initial mapping; broader resource support requires
new typed accounting and qualification, not silent compatibility claims.
