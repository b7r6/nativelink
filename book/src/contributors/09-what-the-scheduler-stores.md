# What the Scheduler Stores

The previous chapter argued about who should own the scheduler's state. This one says what that state *is*, field by field, because every argument about ownership, durability and high availability is really an argument about this inventory. You cannot replicate what you have not enumerated.

The short version: the scheduler's entire durable state is **one struct, one per operation**, and everything else it knows — every worker, every connection, every reservation — lives only in the memory of one process and is never written down anywhere.

## The record

`AwaitedAction`, in `nativelink-scheduler/src/awaited_action_db/awaited_action.rs:54`. Twelve fields. That is the whole of it.

| Field | Type | Written by | Read for |
| ----- | ---- | ---------- | -------- |
| `version` | `i64` (`:57`) | the store, on every write | optimistic concurrency; see below |
| `action_info` | `Arc<ActionInfo>` (`:61`) | once at submission, and again on escalation (`:230`) | matching, priority, timeout, dedupe identity |
| `operation_id` | `OperationId` (`:67`) | once | the primary key |
| `sort_key` | `PersistedSortKey` (`:72`) | once | nothing — see [Two sort keys](#two-sort-keys-and-why) |
| `last_worker_updated_timestamp` | `SystemTime` (`:76`) | every worker keepalive and state change | the executing sweep's liveness fallback |
| `last_client_keepalive_timestamp` | `SystemTime` (`:80`) | every client keepalive | abandonment: no client for `client_action_timeout_s` means do not dispatch |
| `worker_id` | `Option<WorkerId>` (`:84`) | dispatch and requeue | **the only link from shared state to a worker** |
| `state` | `Arc<ActionState>` (`:88`) | every transition | what the client is served |
| `maybe_origin_metadata` | `Option<OriginMetadata>` (`:91`) | once, from OTel baggage | attribution in logs and metrics |
| `attempts` | `usize` (`:95`) | on a failed attempt | retry budget |
| `worker_losses` | `usize` (`:102`) | when the worker running it vanished | a separate budget: the action did nothing wrong |
| `escalations` | `usize` (`:108`) | on a memory kill that re-reserves larger | a third budget, same reason |

`ActionState` (`action_messages.rs:1168`) is four more fields — `stage`, `last_transition_timestamp`, `client_operation_id`, `action_digest` — and is deliberately shaped to be a `google.longrunning.Operation`, which is why the client-facing id lives in there rather than beside `operation_id`.

Three of those fields are requeue budgets rather than one, and the comments say why. `attempts` is the action's own failures; `worker_losses` is "requeues after the worker running the action was lost — not an attempt: the action did nothing wrong"; `escalations` is a requeue with a larger memory reservation after a memory kill, "not an attempt either". Collapsing them would fail actions for the fleet's problems. The two later ones carry `#[serde(default)]`, which is both how they were added without a migration and how you can tell which came first.

## What it looks like on the wire

The record is serialised with `serde_json` (`:253`), and the repository asserts the exact bytes. From `nativelink-scheduler/tests/store_awaited_action_db_test.rs:145`, reformatted and with the test's filler digests elided, but otherwise as the assertion spells it:

```json
{
  "version": 0,
  "action_info": {
    "command_digest": "0101…0101-10",
    "input_root_digest": "0202…0202-10",
    "timeout": { "secs": 1, "nanos": 0 },
    "platform_properties": {},
    "priority": 0,
    "load_timestamp":   { "secs_since_epoch": 0, "nanos_since_epoch": 0 },
    "insert_timestamp": { "secs_since_epoch": 0, "nanos_since_epoch": 0 },
    "unique_qualifier": {
      "Uncacheable": {
        "instance_name": "foo",
        "digest_function": "Sha256",
        "digest": "0303…0303-10"
      }
    }
  },
  "operation_id": { "String": "DEMO_OPERATION_ID" },
  "sort_key": 9223372041149743103,
  "last_worker_updated_timestamp":   { "secs_since_epoch": 0, "nanos_since_epoch": 0 },
  "last_client_keepalive_timestamp": { "secs_since_epoch": 0, "nanos_since_epoch": 0 },
  "worker_id": null,
  "state": {
    "stage": "Queued",
    "last_transition_timestamp": { "secs_since_epoch": 0, "nanos_since_epoch": 0 },
    "client_operation_id": { "String": "DEMO_OPERATION_ID" },
    "action_digest": "0303…0303-10"
  },
  "maybe_origin_metadata": null,
  "attempts": 0,
  "worker_losses": 0,
  "escalations": 0
}
```

Four things are worth noticing in those bytes.

**The schema is externally tagged serde.** `operation_id` is `{"String": …}` or `{"Uuid": …}`; `unique_qualifier` is `{"Cacheable": …}` or `{"Uncacheable": …}`; `stage` is the bare string `"Queued"` only because that variant has no payload. Renaming a variant is a wire break, which is why `ActionUniqueQualifier` still carries `#[serde(alias = "Cachable")]` for the pre-0.7.0 spelling (`action_messages.rs:179`).

**`execution_scope` is absent, not null.** `ActionUniqueKey::execution_scope` is `#[serde(default, skip_serializing_if = "Option::is_none")]` (`action_messages.rs:260`), so a record with no scope is byte-identical to one written before the field existed.

**`sort_key` is 9223372041149743103 and nothing reads it.** That is `0x80000000_FFFFFFFF`: priority 0 biased to the middle of the `u32` range, and zero seconds inverted. Keep reading.

**A queued record is small; a completed one is not.** Everything above is a queued action with no platform properties, and it serialises to 1,131 bytes. A completed record carries the whole `ActionResult`.

## Three names for one piece of work

The record holds three identifiers and they are not interchangeable.

**`operation_id`** (`OperationId`, `action_messages.rs:55`) is the scheduler's primary key: a `Uuid` or an arbitrary `String`, whichever the submitter supplied, with `From<&str>` parsing a UUID if it can and keeping the string if it cannot. This is the key in both backends.

**`client_operation_id`**, inside `ActionState`, is what the client uses on `WaitExecution`. The constructor sets it equal to `operation_id` and explains why at `awaited_action.rs:116`:

> We don't use the real `client_operation_id` here because the only place `AwaitedAction::new` should ever be called is when the action is first created and this struct will be stored in the database, so we don't want to accidentally leak the `client_operation_id` to all clients.

That is the whole reason for the split. Two clients submitting the same cacheable action are joined to one `operation_id`, and each is served a record whose `client_operation_id` has been overwritten with their own on the way out (`:234`). The field in the stored bytes is a placeholder; treating it as the client's id leaks one client's handle to another.

**`unique_qualifier`** (`ActionUniqueQualifier` of `ActionUniqueKey`, `:177` and `:257`) is the dedupe identity: instance name, digest function, action digest, and an optional `execution_scope`. The scope is the escape hatch for "same action, deliberately not joined" — it changes the in-flight identity without changing the action digest or the Action Cache key.

And the `Cacheable` / `Uncacheable` split is not advisory. It gates whether the record is indexed for dedupe at all (`store_awaited_action_db.rs:754`):

```rust
let maybe_unique_qualifier = match &unique_qualifier {
    ActionUniqueQualifier::Cacheable(_) => Some(unique_qualifier),
    ActionUniqueQualifier::Uncacheable(_) => None,
};
```

An uncacheable action — `skip_cache_lookup` on the `ExecuteRequest` — gets no `unique_qualifier` index field in Redis and, by the same check at `memory_awaited_action_db.rs:775`, no entry in the memory backend's hash-key map, so nothing can ever be joined to it. One `ExecuteRequest` boolean decides whether a submission is deduplicable for its whole life.

## Two sort keys, and why

The queue is ordered by priority first, then arrival. Two types implement that, and the difference is a rolling-upgrade story worth reading in full at `awaited_action.rs:262`.

- **`AwaitedActionSortKey`** (`:286`) is a `u128`: priority in bits 64–95, and the insert time in *nanoseconds*, inverted, in the low 64 bits. Descending order over it gives highest priority first, oldest first. This is the key everything orders by, and it is **computed from `action_info`, never read from the record** (`:183`).
- **`PersistedSortKey`** (`:296`) is a `u64` in the layout every released scheduler reads: priority over inverted whole *seconds*. This is the key the record carries.

The reason the record carries the one nothing uses: an older scheduler parsing a record written by this one would fail on a `u128` where it expects a `u64`, and that failure takes queue reads and operation lookups with it. So the record keeps the old shape for a rollback, while ordering uses the new one.

The upgrade itself was a real bug fix. The low half used to hold whole seconds, so **a burst of actions submitted within the same second had no defined order at all** and each backend picked its own. Nanoseconds order every arrival a clock can distinguish.

During a rolling upgrade the Redis index holds both: an older scheduler writes 16 hex digits, this one writes 32 with leading zeros, and RediSearch sorts those strings lexicographically. Under a descending read the 16-digit entries sort first — which means the actions queued before the upgrade run first, which is their correct order anyway. That is luck documented as luck, and eight `const_assert!`s at the bottom of the file pin the whole layout, including `AwaitedActionSortKey::new(0, 1_000) > AwaitedActionSortKey::new(0, 1_001)`.

## The result lives in the record

`ActionStage::Completed(ActionResult)` (`action_messages.rs:762`, `:713`) means the terminal stage *contains* the result, and the result contains:

- `output_files: Vec<FileInfo>` — one path, digest and executable bit per output file (`:438`)
- `output_folders`, `output_directory_symlinks`, `output_file_symlinks`
- `stdout_digest`, `stderr_digest`, `exit_code`, `message`
- `execution_metadata: ExecutionMetadata` — a worker name and nine timestamps (`:597`)
- `server_logs: HashMap<String, DigestInfo>`
- `error: Option<Error>`

So a completed record's size is proportional to its action's output count, and it sits in the same Redis hash, under the same search index, as the queued records the matcher walks. That is the tension behind `retain_completed_for_s`: too short and a client that reconnects late finds nothing, too long and the scheduler's Redis fills with results that only exist to be read once.

One asymmetry to know about. `ActionStage::CompletedFromCache(ProtoActionResult)` is annotated `#[serde(serialize_with = "serialize_proto_result", skip_deserializing)]` (`:776`) — it serialises *as* an `ActionResult` and cannot be deserialised at all, on the stated assumption that "serializing this should never happen". That assumption holds today for a specific reason: the only producer is `CacheLookupScheduler` (`cache_lookup_scheduler.rs:270`), which resolves its waiting clients through in-process channels and never puts the stage in a record. It is a latent trap rather than a live bug, and worth remembering before routing a cache hit through the awaited-action database.

## The memory backend's indexes

`AwaitedActionDbImpl` (`memory_awaited_action_db.rs:304`) is the whole of Configuration A's state, behind one `async_lock::Mutex`:

| Structure | Type | Keyed by |
| --------- | ---- | -------- |
| `client_operation_to_awaited_action` (`:307`) | `EvictingMap` | client operation id → operation id |
| `operation_id_to_awaited_action` (`:312`) | `BTreeMap` | operation id → `watch::Sender<AwaitedAction>` |
| `action_info_hash_key_to_awaited_action` (`:316`) | `HashMap` | `ActionUniqueKey` → operation id (dedupe) |
| `sorted_action_info_hash_keys` (`:323`) | five `BTreeSet`s | one per stage, of `(sort_key, operation_id)` |
| `connected_clients_for_operation_id` (`:327`) | `HashMap` | operation id → client count |

Two details repay attention.

**The record is the value of a `watch` channel.** Not a record plus a notification mechanism — the `watch::Sender` *is* the storage. Subscribing is `tx.subscribe()`, publishing a transition is replacing the value, and a client that calls `changed()` is parked on the channel. There is no polling in Configuration A because there is nothing to poll.

**The stage index is five sets, not one.** `SortedAwaitedActions` (`:226`) keeps a separate `BTreeSet` for `unknown`, `cache_check`, `queued`, `executing` and `completed`, and a transition is a remove from one and an insert into another. That is why a stage transition can fail with "out of sync" errors: the stage in the record and the set the entry sits in are two facts that must be kept equal by hand.

## The Redis keyspace

Configuration B puts the same record in Redis. Four key prefixes, in `store_awaited_action_db.rs`:

| Prefix | Holds | Versioned | Expiry |
| ------ | ----- | --------- | ------ |
| `aa_<operation_id>` (`:482`) | the `AwaitedAction` JSON | yes | `retain_completed_for` once completed, none before |
| `cid_<client_operation_id>` (`:483`) | the operation id it maps to | no | 24 h (`:487`) |
| `ck_<operation_id>` (`:489`) | a client keepalive, as decimal seconds | no | `client_action_timeout_s` |
| `fc_<scheduler_id>` (`:492`) | one scheduler's worker shapes | no | 15 s — three exchange intervals (`simple_scheduler.rs:78,82`) |

Each is a Redis **hash** with a `data` field and a `version` field (`redis_store.rs:1726`), plus one hash field per index. The `aa_` records carry up to three index fields (`:754`): `state` (one of `cache_check`, `queued`, `executing`, `completed`), `sort_key` (fixed-width hex so lexicographic order is numeric order), and `unique_qualifier` — the last only for cacheable actions.

Those index fields are backed by RediSearch indexes whose names are built by `get_index_name!` (`redis_store.rs:1870`) as `{prefix}_{field}_{sortkey}_{fingerprint}`, where the fingerprint is a CRC32 of the `FT.CREATE` command template, hardcoded as `3e762c15` (`:1821`). So the live index names are:

```
aa_state_sort_key_3e762c15
aa_unique_qualifier__3e762c15
fc_fleet__3e762c15
```

The fingerprint in the name is a schema version: change the `FT.CREATE` template and every scheduler starts using a differently-named index, so an old index is abandoned rather than silently reused with the wrong schema. A test asserts the constant still matches the template it was computed from.

Two things about these indexes are scar tissue, and both are commented as such in the tree. The index is explicitly **never `TEMPORARY`**, because RediSearch 2.10 does not garbage collect a temporary index — every update of an indexed hash left a dead entry behind for good, until a scheduler Redis that had run for days needed 100–200 ms to count an *empty* queue. And the version-set script does `DEL` before `HSET` rather than just overwriting, because Redis was observed not updating the search index on a plain field update.

### The write path is a compare-and-swap in Lua

`LUA_VERSION_SET_SCRIPT` (`redis_store.rs:1741`) is how every versioned record is written:

```lua
local new_version = redis.call('HINCRBY', key, 'version', 1)
if new_version-1 ~= expected_version then
    local reverted = redis.call('HINCRBY', key, 'version', -1)
    -- HINCRBY creates the key before the version can be checked …
    if reverted == 0 and redis.call('HEXISTS', key, 'data') == 0 then
        redis.call('DEL', key)
    end
    return { 0, new_version-1 }
end
redis.call('DEL', key)
redis.call('HSET', key, 'data', new_data, 'version', new_version, unpack(indexes))
```

Read it as: increment first, check second, roll back if you lost. The incrementing-first part has a consequence the comment spells out — `HINCRBY` creates the key, so a caller holding a stale version for a record that has since expired leaves behind a hash with a zeroed version, no data and no expiry, which still matches the index prefix and therefore sits in the search index forever as an empty document, slowing every query that reads it. The `DEL` guard cleans up exactly the keys the failing call brought into existence, and no others.

This script is the entire concurrency control for Configuration B. Every retry loop and backoff in `simple_scheduler_state_manager.rs` exists to handle it returning `{0, …}`.

### Reads are cursored aggregations

A queue listing is `FT.AGGREGATE` over `aa_state_sort_key_3e762c15`, loading `data` and `version`, sorted by `@sort_key` descending, through a cursor (`redis_store.rs:2361`). Each row then needs a second round trip for its `ck_` keepalive, which is why `LISTING_READ_CONCURRENCY = 64` exists (`simple_scheduler_state_manager.rs:1677`): read one at a time, a 7,000-deep queue took several seconds to list, on every matching pass and on every admin query the provisioner gives five seconds.

A row the index lists but the store no longer has — expired between the search and the read — is skipped and counted rather than returned as an error (`:1693`). An eventually-consistent index will do that, and one stale row must not end a listing.

## What is not stored

Everything in this section lives in one process's memory, is not `Serialize`, and vanishes when that process does.

**The worker.** `Worker` (`worker.rs:106`) holds the worker's id, its `platform_properties` (reduced as actions are placed) *and* its `total_platform_properties` (what it offers when idle), the `tx: Sender<UpdateForWorker>` that is the live gRPC stream, `running_action_infos`, `last_update_timestamp`, `is_paused`, `pause_needs_kb`, `admits_when_idle`, `idle_declined_kb`, `is_draining`, `max_inflight_tasks` and `last_load`. It contains a channel sender, so it could not be serialised even if someone wanted to.

**What the worker was told, and when.** `PendingActionInfoData` (`worker.rs:68`) records `dispatched_at`, `accepted` (did the worker acknowledge), `kill_requested_at` and the last reported resource usage, per running operation. None of this reaches the shared store. The shared record's `worker_id` says *which* worker; this says *what happened with it*.

**Liveness.** `WorkerRegistry` (`worker_registry.rs:52`) is a `HashMap<WorkerId, SystemTime>` of last-heartbeat times, fed only by the `connect_worker` streams this process owns. This is the single most consequential omission from the shared state, and the one [appendix H](../appendix/scheduler-ownership.md) is mostly about.

**Placement bookkeeping.** `ApiWorkerSchedulerImpl` (`api_worker_scheduler.rs:277`) adds the LRU of workers, `holds` and `holds_by_action` (a worker reserved for one queued action), `hold_cooldowns`, `pass_generation`, `capacity_generation`, and a `WorkerCapabilityIndex` for narrowing candidates before a linear scan. All derived, all per-process, all lost on restart.

**Unsatisfiability.** `UnsatisfiableTracker` (`unsatisfiable_tracker.rs:83`) keeps a `HashMap<PropertyShape, ShapeState>` of how long each shape has had no capable worker. Restarting a scheduler restarts that clock.

**In-flight cache checks.** `CacheLookupScheduler` (`cache_lookup_scheduler.rs:63`) holds `inflight_cache_checks` under a `parking_lot::Mutex`, joining concurrent submissions of the same action while the AC is queried. A restart mid-check loses the join, and both clients resubmit.

There is exactly one exception, and its shape is informative. `fc_<scheduler_id>` (`:492`) publishes `FleetCapabilities { scheduler_id, workers: Vec<PlatformProperties> }` (`:583`) — the *shapes* a scheduler's workers can run, so peers can tell an impossible action from a merely unlucky one. It exists because the unsatisfiable tracker needed fleet-wide knowledge to avoid failing actions that another scheduler's workers could run.

Look at how it is maintained, because the mechanism is exactly what the rest of the fleet state lacks. Every scheduler republishes its record every `FLEET_EXCHANGE_INTERVAL` (5 s, `simple_scheduler.rs:78`) with a TTL of three intervals (`:82`), and reads its peers back out of the `fc_fleet__3e762c15` index. A peer that stops publishing drops out of the index on its own, and the search comment says so: "records that have expired on peers that stopped publishing are gone from the search already, so everything found here is live."

That is a 15-second, TTL-based membership protocol, already running, already correct — for *shapes*. It carries no worker identity and no per-worker liveness, so it cannot tell you that worker `w-7` is gone, only that nobody can currently run `OSFamily=linux, container-image=…`. Which is why a shared record can be adjacent to a dead worker for an hour (`ORPHANED_ACTION_TIMEOUT`) while the thing that would have detected it refreshes twelve times a minute. Extending `fc_` to carry liveness is not a small change — that turns it into a failure detector, and a failure detector that can requeue other schedulers' work needs to be right about who is dead, not merely current. But the gap is 15 seconds versus one hour, in a mechanism that already exists, and [appendix H](../appendix/scheduler-ownership.md) is largely about how to close it.

So, stated plainly: **the shared store's entire knowledge of the worker fleet is one nullable string per record.** `worker_id: Option<WorkerId>`. Which process owns that worker, whether it is alive, what else it is running, and what it last reported — none of that is written down.

## Lifetimes: who deletes what

| Constant | Value | Where | Governs |
| -------- | ----- | ----- | ------- |
| `DEFAULT_RETAIN_COMPLETED_FOR_S` | 60 s | `default_scheduler_factory.rs:42` | how long a completed record is kept, both backends |
| `DEFAULT_CLIENT_ACTION_TIMEOUT_S` | 60 s | `simple_scheduler.rs:114` | how long an action survives with no client |
| `CLIENT_KEEPALIVE_DURATION` | 10 s | `awaited_action_db/mod.rs:33` | how often a listening client refreshes `ck_` |
| `CLIENT_ID_MAPPING_TTL` | 24 h | `store_awaited_action_db.rs:487` | ceiling on a `cid_` mapping outliving its `aa_` |
| `ORPHANED_ACTION_TIMEOUT` | 1 h | `worker_registry.rs:28` | the fallback when no instance recognises the worker |
| `LISTING_READ_CONCURRENCY` | 64 | `simple_scheduler_state_manager.rs:1677` | listing round trips in flight |

`retain_completed_for_s` is normalised in one place for both backends (`default_scheduler_factory.rs:247`), and that function exists because of a specific bug: passed through as the configured `0`, the Lua script read it as "no expiry", so the scheduler's Redis kept **every action ever run** and the search index grew without bound.

The two backends then reach the same number by opposite routes, and this is the part that surprises people.

**Redis deletes by TTL.** `EXPIRE` on the `aa_` key once the record is completed. Time passes, the key goes, and the index entry goes with it.

**Memory deletes by loss of interest.** The record is removed when its last client goes away, not when the action finishes. `ClientAwaitedAction` (`memory_awaited_action_db.rs:52`) sends `ActionEvent::ClientDroppedOperation` from its `Drop` impl (`:76`); the handler (`:375`) decrements `connected_clients_for_operation_id` and, only when it reaches zero, removes the record from the `BTreeMap`, the dedupe map and the stage set. `retain_completed_for_s` enters as `EvictionPolicy { max_seconds }` on the `client_operation_to_awaited_action` `EvictingMap` (`:934`) — so what it bounds is **how long the client's handle is remembered**, and the record dies as a consequence of that handle expiring.

Which means: under Configuration A, the record for a completed action with a client still attached is retained indefinitely, and an action whose client vanished is collected about a minute later whatever stage it was in. The configured number is the same; what it measures is not.

One guard in the drop handler is worth reading, because it is the kind of thing that is only ever written after a bug (`:430`):

```rust
// Once this operation finished, a newer operation for the
// same action key may have claimed the entry; removing it
// unconditionally here would orphan that operation's
// deduplication entry.
let owned_by_this_operation = self
    .action_info_hash_key_to_awaited_action
    .get(action_key)
    .is_some_and(|id| id == &operation_id);
```

A dedupe map keyed by action rather than by operation needs every removal to check that it still owns the entry it is removing. This is the same class of hazard as the stage-set bookkeeping above: two structures holding one fact, kept equal by hand.

## Looking at it

The admin API serves the queue as `QueuedDemand` (`admin.rs:39`): operation id, `queued_since_ms`, priority, the platform properties *as the scheduler matches them* including reservations, and `client_last_seen_ms`. It reads through `ClientStateManager::filter_operations`, so it works on both backends and shows what the matcher sees rather than what the record literally holds. Note that an entry whose `client_last_seen_ms` is older than the client timeout is one the scheduler will **not** dispatch — a queue listing can therefore contain actions that are waiting for nothing.

On Redis you can also look directly, and it is worth doing once:

```bash
redis-cli --scan --pattern 'aa_*' | head
redis-cli HGETALL aa_<operation_id>          # data, version, state, sort_key, unique_qualifier
redis-cli FT.INFO aa_state_sort_key_3e762c15 # num_docs, and whether it is still indexing
```

If `FT.INFO` reports a `num_docs` far larger than the queue you believe you have, read the two scar-tissue notes above: empty documents from lost compare-and-swaps, and dead entries from an index created `TEMPORARY` by an older release. The second needs `FT.DROPINDEX` without `DD`; this code recreates the index on next use.

## The smell test, for state

Before adding a field to the scheduler's state, ask:

1. **Does it go in the record, or in the process?** If a second scheduler would need it to make a correct decision, it goes in the record — and then it needs a serde default, because records written before it exists will still be read.
2. **Is it derived?** Anything computable from `action_info` should be computed, like `AwaitedActionSortKey`, not stored. Stored derived state is state that can disagree with itself.
3. **Does an older scheduler still parse the record?** A rolling upgrade reads your record with the previous release's code. `PersistedSortKey` exists entirely because the answer was no.
4. **Who deletes it?** Both backends must agree, and they delete by different mechanisms — TTL in Redis, loss of client interest in memory.
5. **Does it need a second structure to find it?** Every index is a fact duplicated, and the duplicate is maintained by hand. The stage sets and the dedupe map each have a comment about being out of sync.
6. **How big does it get?** It shares a Redis hash and a search index with the queue the matcher walks on every pass.
