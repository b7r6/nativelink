# Timeouts, retries and clocks: an inventory for the fenced scheduler

Stream B of the fenced-scheduler track (`nativelink-scheduler/src/raft_awaited_action_db/FENCED.md`). This page lists every place where time or retrying influences behavior in the scheduler, the worker, and the services between them. For each one it says what fires, whether it decides correctness, and what replaces it under FENCED. It drives what gets deleted, kept or rewritten.

- **Base:** `design/raft-fenced` at `b3d37331bb`. Line numbers refer to that tree.
- **Read in full:** `nativelink-scheduler/src/**`; `nativelink-worker/src/**`; `nativelink-service/src/{execution_server.rs,worker_api_server.rs}`.
- **Read in part:** `nativelink-util/src/{instant_wrapper,evicting_map,retry,action_messages,operation_state_manager,connection_manager,task}.rs`; the scheduler and worker knobs in `nativelink-config/src/{schedulers,cas_server}.rs`.
- **Also searched:** `Duration`, `timeout`, `sleep`, `interval`, `Instant`, `SystemTime`, `now_fn`/`now()`, `retry`/`Retrier`/`backoff`, `keepalive`, `deadline`, `expire`, `evict`, `tokio::time`, `select!`, retry-on-conflict loops.
- **History:** `git log` on every file, and the tip commits of the unmerged `fix/*`, `b7r6/*` and `repro/*` branches.
- **Evidence:** read-only. Where a row says **by reading**, the behavior was derived from the code and not reproduced by a test.

## 0. Classes

| Class | Meaning |
|---|---|
| **C** | Correctness-deciding. When it fires the system changes who owns work, what result counts, whether an operation exists, or what the client sees (requeue, fail, evict, charge a retry, drop a client or a result). |
| **L** | Liveness only. It makes something be retried or rechecked sooner or later; firing early or late changes latency, never the outcome. |
| **D** | Detector input. It feeds suspicion of a peer (worker, client, scheduler connection, leader). |
| **N** | Noise or ops: logging, metrics, health, garbage collection with no semantic effect, dead code. |
| **W** | Wall clock in a decision: a `SystemTime` value a decision depends on (ordering, or a comparison that can cross nodes). As a primary class it marks a site whose only role is to be such a clock source. As a suffix (`C·W`, `D·W`) it flags a decision that reads one. |

A row has one primary class (the first letter). Counts below are by primary class; the last column counts rows carrying a W anywhere.

## 1. Summary

### 1.1 Counts

| Crate | Rows | C | L | D | N | W (primary) | Rows with a W |
|---|---|---|---|---|---|---|---|
| `nativelink-scheduler` | 144 | 51 | 24 | 23 | 34 | 12 | 34 |
| `nativelink-service` | 15 | 5 | 1 | 5 | 3 | 1 | 1 |
| `nativelink-util` | 11 | 1 | 2 | 0 | 7 | 1 | 1 |
| `nativelink-worker` | 60 | 24 | 15 | 6 | 14 | 1 | 5 |
| `nativelink-config` (knobs, §1.2) | 40 | 25 | 4 | 4 | 7 | 0 | 2 |
| **Total** | **270** | **106** | **46** | **38** | **65** | **15** | **43** |

What happens to the 81 C sites outside the config crate:

- **Deleted (13).** They decide today and have no place in the fenced design:
  - scheduler-side action timeouts (SM9, SM11, WR1)
  - the client-poll timeout loop (SM4, SM5)
  - client keepalive comparisons and cancel-on-read (SM6, SM13, EX5)
  - the kill sweep and the result-dropping kill rule (AW28, AW10)
  - retry-on-conflict re-application (SM14)
  - `worker_timeout_s`'s extra meanings (SS12)
  - the scheduler reading `Action.timeout` (EX1)
- **Become a logged decision (38):**
  - `ExpireSession` proposed by the detector (8): SM12, AW13, AW17, AW18, AW26, AW27, SW1, WA5.
  - Session and leadership facts (5): `OpenSession`, `CloseSession`, `LeaderChanged` (AW4, AW24, AW25, WA7, RA7).
  - Attempt commands (9): `Revoke(op, n)`, `Accepted(n)`, `Declined(n)`, `RevokeOffer`, and stale-attempt rejection (SS13, AW9, AW12, AW14, AW15, AW16, AW23, SM22, WA9).
  - `FailQueued` (4): SS3, SS7, UT2, UT3.
  - `Collect` / `RequestCancel` (7): MD4, MD8, UX3, RA1, RA4, RA5, DF1.
  - Retry budgets decided in apply (5): SM19, SM20, SM21, RA15, DF4.
- **Stay on the worker (23):** `Action.timeout` and resource enforcement, I/O bounds, cleanup and persistent-worker timeouts (RM1–RM17, PW1, PW2, PW5, LW2, LW6, LW11). Most need fixing first because they mis-report or re-run accepted attempts (§3 Steps 5 and 6).
- **Out of scope or undecided (7):** the Redis backend (SA5, SA8, SA10), the `grpc` proxy (GS1–GS3), and the worker writing the Action Cache outside the fence (LW12, §4.9).

Knobs at a glance (full table in §1.2):

| Knob | Becomes |
|---|---|
| `client_action_timeout_s`, `max_action_executing_timeout_s`, `kill_revoked_operations_interval_s` | deleted |
| `worker_timeout_s` | detector threshold only |
| `unacknowledged_kill_timeout_s`, `dispatch_ack_timeout_s` | detector inputs (revoke-ack and offer deadlines), not named in FENCED.md |
| `unsatisfiable_action_timeout_s`, `no_worker_action_timeout_s` | leader policy timers proposing `FailQueued`, not named in FENCED.md |
| `retain_completed_for_s` | retention GC of terminal ops only |
| `max_job_retries`, `max_worker_loss_retries`, `memory_escalation.max_steps` | budgets counted in apply on accepted attempts |
| `report_executing_on_accept` | unconditional |
| `max_action_timeout_s`, `kill_grace_ms`, `timeout_handled_externally` | stay on the worker as the bound |
| `max_upload_timeout_s`, `max_download_timeout_s`, `max_cleanup_wait_s`, `precondition_timeout_ms`, persistent `acquire_timeout_s` | stay on the worker but must stop re-running or mis-reporting accepted attempts |
| raft heartbeat and election timers | liveness only |

### 1.2 Config knobs and what each becomes

Every time-, retry- or deadline-shaped knob in `nativelink-config/src/schedulers.rs` and `nativelink-config/src/cas_server.rs` that sits on the scheduler, worker or worker-API path. Class is the class of the decision the knob parameterizes today.

| ID | Knob (`file:line`), default | Today it decides | Class | Becomes |
|---|---|---|---|---|
| K1 | `retain_completed_for_s` (`schedulers.rs:147`), 60 s | How long a completed op (and, on memory/raft, a detached client's op **in any stage**, MD4/RA4) exists for `WaitExecution`. Redis: TTL on completed records. | C | Retention GC of terminal ops from log time of the last `ClientDetach` (`Collect`). Non-terminal ops are never collected without a `RequestCancel` behind the revoke barrier (§4.7). |
| K2 | `client_action_timeout_s` (`schedulers.rs:153`), 60 s | Cancels queued ops (SM6, sweep) and any non-finished op on a read (SM13) once the client keepalive is older than this; Redis keepalive TTL. | C | **Deleted** (FENCED): interest is a watcher count. |
| K3 | `worker_timeout_s` (`schedulers.rs:177`), 10 s | Worker eviction (AW26), silent-worker filter (AW6), client-stream poll period (SM3), Stale requeue (SM12), `Action.timeout` grace (SM9), Redis abandonment (SA8). | C | The failure detector's suspicion threshold only (FENCED). Its verdict is a proposed `ExpireSession`. |
| K4 | `max_action_executing_timeout_s` (`schedulers.rs:187`), off | Requeues (and charges) an executing op older than this since its last state write (SM11). | C | **Deleted** (FENCED). |
| K5 | `unacknowledged_kill_timeout_s` (`schedulers.rs:197`), 60 s | Evicts a worker holding a kill unacknowledged this long (AW27). | C | Revoke-ack deadline: a detector input that proposes `ExpireSession`. Not in FENCED.md (§4.5). |
| K6 | `dispatch_ack_timeout_s` (`schedulers.rs:209`), off | Requeues an unacknowledged dispatch untried and pauses the worker (AW16). | C | Offer deadline: a detector input that proposes `RevokeOffer`/`ExpireSession`; the op is requeued only behind the revoke barrier. Not in FENCED.md (§4.5). |
| K7 | `unsatisfiable_action_timeout_s` (`schedulers.rs:264`), off | Fails a queued action no connected worker could ever run (SS3). | C | Policy timer on the leader proposing `FailQueued`, measured in log time. Not in FENCED.md (§4.6). |
| K8 | `no_worker_action_timeout_s` (`schedulers.rs:276`), off | Fails queued actions while no worker is connected anywhere (SS7). | C | As K7. |
| K9 | `max_job_retries` (`schedulers.rs:288`), 3 (0 → 3) | Failure budget (SM19); raft apply's own cap uses the raw value (RA15, DF4). | C | One budget, decided in apply, counting attempts that reached `Accepted` (FENCED); one default. |
| K10 | `max_worker_loss_retries` (`schedulers.rs:298`), 10 | Worker-loss budget (SM20), spent by every eviction. | C | Charged when `ExpireSession` commits, for attempts that reached `Accepted`. |
| K11 | `memory_escalation.max_steps` (`schedulers.rs:558`), 8 | Escalation budget (SM21). | C | Budget in apply; the kill outcome arrives in `Finished(attempt)`. |
| K12 | `live_memory_veto` (`schedulers.rs:313`), off | Skips a worker whose last keepalive reported less free memory than the action reserves (AW7). | D | Leader-local placement input (record the sample in `Offer` if offers must replay). |
| K13 | `report_executing_on_accept` (`schedulers.rs:334`), false | Whether clients see `EXECUTING` from assignment or from the worker's ack. | C | Unconditional (FENCED offer barrier). |
| K14 | `worker_match_logging_interval_s` (`schedulers.rs:347`), 10 s | Full matching logs (SS21). | N | Kept. |
| K15 | `fallback_match_interval_s` (`schedulers.rs:361`), 5 s | Periodic matching pass (SS14). | L | Optional; FENCED deletes the periodic sweep. |
| K16 | `enable_active_action_count_metric` (`schedulers.rs:169`), false | Redis active-count query every 15 s (SA7). | N | Unchanged (Redis). |
| K17 | `GrpcSpec.retry` (`schedulers.rs:422`) | Retries upstream `Execute` with side effects (GS1). | C | Proxy only: retry idempotent calls; resume with `WaitExecution`. |
| K18 | `historical_resource.refresh_interval_s` (`schedulers.rs:656`), 30 s | Hint file reload (HR1). | N | Kept. |
| K19 | `historical_resource.class_by_timeout` (`schedulers.rs:707`) | Sizing class from `Action.timeout` (HR2). | N | Kept. |
| K20 | raft `cluster.heartbeat_interval_ms` / `election_timeout_{min,max}_ms` (FENCED cluster contract; not on this branch yet: hard-coded 100/200/400 ms, RA6) | Raft heartbeats and elections. | L | Liveness only (FENCED), defaults 250 / 1000 / 2000. |
| K21 | `disable_kill_revoked_operations`, `kill_revoked_operations_interval_s` (`cas_server.rs:410`, `:421`), on / 5 s | The revoked-operation kill sweep (AW28, WA2). | C | **Deleted**: revokes are obligations sent at once. |
| K22 | `health.timeout_seconds` (`cas_server.rs:451`), 5 s; `readiness_path` (`:462`) | Health check bound; readiness. | N | Readiness becomes the leadership-barrier route (FENCED contract). |
| K23 | `http2_keep_alive_interval`, `experimental_http2_keep_alive_timeout_s` (`cas_server.rs:632`, `:672`), hyper default | Server-side PINGs: an unanswered PING closes the worker's stream → eviction (WA5, AW24). | D | Detector input only. |
| K24 | `worker_api_endpoint.timeout` (`cas_server.rs:888`), 5 s | Keepalive cadence (`timeout/2`), connect/request timeout, `GoingAway` bound (LW4, LW5, LW7). | D | Detector input cadence (worker side). |
| K25 | `tcp_keepalive_s`, `http2_keepalive_interval_s`, `http2_keepalive_timeout_s` (`cas_server.rs:894`, `:901`, `:907`), 30 / 30 / 20 s | Worker detects a dead scheduler connection → `kill_all` (LW2, LW4). | D | Worker's detector of the leader: reconnect and resume, not kill (wave 2). |
| K26 | `max_action_timeout_s` (`cas_server.rs:1281`), 20 min | Clamp for, and default of, `Action.timeout` (RM6); drain deadline (LW13). | C | **Stays on the worker**: the bound FENCED relies on for hung actions. |
| K27 | `max_upload_timeout_s` (`cas_server.rs:1293`), 10 min | Discards a finished execution and re-runs it (RM14). | C | `Finished(attempt, err)`; no silent re-run. |
| K28 | `max_download_timeout_s` (`cas_server.rs:1308`), 10 min | Fails input fetch → charged retry (RM13). | C | `Finished(attempt, err)`. |
| K29 | `max_cleanup_wait_s` (`cas_server.rs:1315`), 30 s | Turns a successful result into an error (RM16, LW11); fails registration after the ack (RM17); orphan-sweep age gate (RM19). | C | Cleanup after the result; directories keyed by `(op, attempt)`. |
| K30 | `max_cleanup_backoff_ms` (`cas_server.rs:1322`), 500 ms | Backoff while waiting for a previous directory (RM17). | L | Deleted with RM17, or kept as liveness. |
| K31 | `kill_tombstone_ttl_s` (`cas_server.rs:1331`), 60 s | Whether a kill that beat its dispatch still applies (RM12), on the wall clock. | C·W | GC on a monotonic clock, ≥ the offer deadline; correctness from apply. |
| K32 | `timeout_handled_externally` (`cas_server.rs:1375`), false | The wrapper enforces the timeout; the worker only enforces `max_action_timeout_s` (RM6, RM7). | C | Stays on the worker. |
| K33 | `drain_on_shutdown` (`cas_server.rs:1386`), false | Whether `GoingAway` requeues everything at once or drains (WA7). | C | Session attribute; `CloseSession` after drain. |
| K34 | `precondition_timeout_ms` (`cas_server.rs:1425`), 30 s | Script timeout → requeue untried after the ack (LW6). | C | Run before `Accepted`; `Declined(attempt)`. |
| K35 | `orphan_sweep_interval_s` (`cas_server.rs:1440`), off | Orphan directory sweep (RM19, LW15). | N·W | Kept (local GC). |
| K36 | `kill_grace_ms` (`cas_server.rs:1449`), 5 s | SIGTERM → SIGKILL grace on timeout or kill (RM4). | C | Stays on the worker; the revoke ack follows reaping. |
| K37 | `persistent_workers.{idle_timeout_s, shutdown_grace_ms, acquire_timeout_s}` (`cas_server.rs:1170`, `:1183`, `:1190`), 300 s / 5 s / 30 s | Warm-process reuse across attempts (PW4); acquire timeout → one-shot fallback with a second full `Action.timeout` (RM8, PW5). | C | Fallback gets the remaining time; process reuse across attempts is declared (§4.10). |
| K38 | `experimental_buck2_file_capture.finalize_timeout_s` (`cas_server.rs:1251`), 120 s | Capture finalization wait (BC2). | L | Kept. |
| K39 | `resource_enforcement` (`cas_server.rs:928-978`) | Memory/disk soft kills on sampled usage (RM1, RM10). | C | Stays on the worker. |
| K40 | `persist_stream_on_disconnect_timeout_s` (`cas_server.rs:362`), 10 s | ByteStream upload resume window (CAS, out of scope). | N | Unchanged. |

### 1.3 Hard-coded constants

| Constant | Location | Value | Feeds | Fate |
|---|---|---|---|---|
| `DEFAULT_WORKER_TIMEOUT_S` | `simple_scheduler.rs:74` | 10 s | K3 | Detector threshold default. |
| `FLEET_EXCHANGE_INTERVAL`, `FLEET_RECORD_TTL_INTERVALS` | `simple_scheduler.rs:78`, `:82` | 5 s, ×3 (15 s TTL) | SS16, AW1, SA13 | Deleted. |
| `MAX_UNSATISFIABLE_FAILS_PER_PASS`, `UNSATISFIABLE_FAIL_BACKLOG_DIVISOR` | `simple_scheduler.rs:88`, `:94` | 16, 4 | SS3 | Kept with `FailQueued` (rate of proposals). |
| `DEFAULT_UNACKNOWLEDGED_KILL_TIMEOUT_S` | `simple_scheduler.rs:109` | 60 s | K5 | Revoke-ack deadline default. |
| `DEFAULT_CLIENT_ACTION_TIMEOUT_S` | `simple_scheduler.rs:114` | 60 s | K2 | Deleted. |
| `DEFAULT_MAX_JOB_RETRIES` | `simple_scheduler.rs:118` | 3 | K9 | One default, in apply. |
| Matching retry after a failed pass; post-pass yield | `simple_scheduler.rs:1368`, `:1071` | 100 ms; 1 ms | SS18, SS9 | Kept (L). |
| `MAX_UPDATE_RETRIES`, `BASE_RETRY_DELAY_MS`, `MAX_RETRY_JITTER_MS` | `simple_scheduler_state_manager.rs:87`, `:90`, `:93` | 5; 10 ms; 20 ms | SM1, SM5, SM7, SM13, SM16 | Deleted. |
| `RetryLimits::DEFAULT_WORKER_LOSS_RETRIES` | `simple_scheduler_state_manager.rs:427` | 10 | K10 | Loss budget default. |
| `ORPHANED_ACTION_TIMEOUT` | `worker_registry.rs:28` | 1 h | SM11, SA8 | Deleted. |
| `CLIENT_KEEPALIVE_DURATION` | `awaited_action_db/mod.rs:35` | 10 s | MD1, SA2, RA3 | Deleted. |
| `WARN_INTERVAL`, `MIN_RECHECK`, `MAX_DUE_BACKOFF_DOUBLINGS` | `unsatisfiable_tracker.rs:33`, `:36`, `:40` | 60 s; 1 s; 5 (32 s) | UT1, UT4 | Kept. |
| `DEFAULT_RETAIN_COMPLETED_FOR_S` | `default_scheduler_factory.rs:46` | 60 s | K1 | Retention default. |
| `MAX_RETRIES_FOR_CLIENT_KEEPALIVE`, `ACTIVE_COUNT_REFRESH_INTERVAL`, `CLIENT_ID_MAPPING_TTL`, `SUBSCRIBE_RACE_RETRY_DELAY` | `store_awaited_action_db.rs:54`, `:61`, `:487`, `:988` | 8 (dead); 15 s; 24 h; 20 ms | SA3, SA7, SA5, SA9 | Redis, unchanged. |
| openraft `heartbeat_interval`, `election_timeout_{min,max}` | `raft_awaited_action_db/mod.rs:488-490` | 100 ms; 200–400 ms | RA6 | Config (K20). |
| Raft readiness deadline and poll | `raft_awaited_action_db/mod.rs:597`, `:603` | 30 s; 20 ms | RA9 | Readiness route, no deadline. |
| `RaftDbConfig::default` retain / retry; `DEFAULT_RETRY_LIMIT` | `raft_awaited_action_db/mod.rs:145-146`; `state.rs:75` | 60 s; 5 | RA1, RA15 | One retention and one budget default. |
| `DEFAULT_KILL_REVOKED_OPERATIONS_INTERVAL_S`; eviction tick | `worker_api_server.rs:54`; `:92` | 5 s; 1 s | AW28; AW26 | Deleted; detector loop. |
| `ACTIONS_IN_TRANSIT_TIMEOUT_S` | `local_worker.rs:67` | 10 s | LW1 | Kept. |
| `CONNECTION_RETRY_DELAY_S`, `CONNECTION_RETRY_MAX_DELAY_S` | `local_worker.rs:124-125` | 0.5 s, 30 s | LW3 | Kept (seeded jitter). |
| `DEFAULT_ENDPOINT_TIMEOUT_S`; TCP and HTTP/2 keepalive defaults | `local_worker.rs:141`; `:145-147` | 5 s; 30 s, 30 s / 20 s | LW4, LW7 | Detector inputs. |
| `DEFAULT_MAX_ACTION_TIMEOUT`, `…_UPLOAD_…`, `…_DOWNLOAD_…`, `…_CLEANUP_WAIT`, `…_CLEANUP_BACKOFF`, `DEFAULT_PRECONDITION_TIMEOUT`, `DEFAULT_KILL_GRACE` | `local_worker.rs:188-197` | 20 min, 10 min, 10 min, 30 s, 500 ms, 30 s, 5 s | K26–K30, K34, K36 | Stay on the worker. |
| Sampler period, `OUTPUT_DRAIN_TIMEOUT`, `DISK_SAMPLE_EVERY` | `running_actions_manager.rs:109`, `:162`, `:289` | 250 ms, 5 s, 40 | RM1, RM3 | Stay on the worker. |
| `DEFAULT_KILL_TOMBSTONE_TTL`, `MAX_KILL_TOMBSTONES` | `running_actions_manager.rs:4299-4304` | 60 s, 4096 | RM12 | Monotonic GC. |
| Pool idle, max requests, acquire, shutdown grace | `persistent_worker/pool.rs:44`, `:48`, `:51`, `:152` | 5 min, 200, 30 s, 5 s | PW4–PW6 | Stay on the worker. |
| `REAP_INTERVAL` | `reaper.rs:34` | 5 s | RP1 | Kept. |

### 1.4 The short version

1. **Executing-action timeouts are driven by the client's watch loop (SM3).** The only path that acts on `timeout_cause` is a client's `changed()` poll (reached only from `execution_server.rs:336`). An executing op nobody watches is never timed out there; N watchers on N schedulers are N deciders.
2. **The scheduler's `Action.timeout` check retries a timed-out action (SM9).** It fires `Action.timeout + 10 s` after assignment, before a worker that spent minutes fetching inputs has even started its own timer. The action is retried three times and ends `FAILED_PRECONDITION`, where the worker would have reported one completed `DEADLINE_EXCEEDED`. FENCED does not name this branch (§4.1).
3. **Worker liveness measures scheduler processing, not worker arrival (AW2, AW20, WA4, AC1).** The stamp is taken after a global lock that is held across store writes, and the eviction sweep has a check-then-act race (AW26). A 15 s store stall can evict a healthy fleet (T2a).
4. **Retention deletes running operations (MD4, RA4).** On memory and raft, a client gone for `retain_completed_for_s` takes its queued or executing op with it, worker still running. FENCED's wording keeps this (§4.7).
5. **Reads write (SM13).** `WaitExecution`, `GetOperation`, the admin listing and the matcher's queue listing all complete actions whose client keepalive is old.
6. **The raft backend's production retry cap is 0 when `max_job_retries` is unset (RA15, DF4, by reading).** The first internal error strands the op in `Executing`. The test suite builds raft with a different default and does not see it.
7. **The Action Cache is outside the fence (LW12, RM15).** The worker writes the AC before the scheduler hears the result, and a timed-out action that exits 0 on SIGTERM is cached as a success.
8. **The worker's `Action.timeout` bound has holes (RM3, RM8, RM9, PW1, PW2).** A persistent-worker action can run ~2× its timeout, and a clean exit can hang forever on a held pipe. FENCED relies on this bound alone (§4.2).
9. **Worker timers re-run executed actions (RM14, RM16, LW11).** The upload timeout throws a finished execution away, and a cleanup failure turns success into failure: retries with side effects.
10. **Wall-clock values are inside replicated state (AA3, AA4, RA12, EX2).** They order the queue and age actions. Raft snapshot bytes also depend on `HashMap` order (§5.6).
11. **Two detector inputs decide directly and are missing from FENCED (AW16, AW27, SW1).** The dispatch-ack timeout requeues while the `StartExecute` may still be in flight. The kill-ack deadline is measured from the wrong instant and fires early.
12. **A retried timeout can hit a newer attempt (SM14, by reading).** The retry-on-conflict path re-applies a timeout's `UpdateWithError` (no worker named) to whatever attempt is current after the conflict.

## 2. Inventory by file

Columns: **ID** (stable handle used in §3–§5), **Location** (`path:line`, function), **What fires and what it decides**, **Class** (primary first; `·W` marks a wall-clock read the decision depends on), **Fenced replacement**, **Bugs, evidence** (commits are short shas on this branch's history; `branch:` names an unmerged fork branch whose tip commit documents the bug).

### 2.1 `nativelink-scheduler/src/simple_scheduler.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| SS1 | `:433-443` `unsatisfiable_deadline` | Next wake for the unsatisfiable and no-worker trackers (min of both `next_deadline`). Only schedules a pass; the pass decides (SS3, SS7). | L | Kept as a leader-local wake timer that triggers a proposal. | |
| SS2 | `:448-451` `do_try_match` | `std::time::Instant` around the pass, `record_matching_pass` metric. | N | Kept. | |
| SS3 | `:470-559` `handle_unsatisfiable` | Shape unsatisfiable for `unsatisfiable_action_timeout_s` **and** the action itself queued that long (`now` vs `ActionInfo.insert_timestamp`, a wall-clock value set by whichever execution server received `Execute`) → `fail_queued_operation` completes it with `FAILED_PRECONDITION`. Guarded by a fleet-generation re-check (`:520`) and a per-pass blast-radius cap (`:528`, `unsatisfiable_fail_cap`). | C·W | Logged decision `FailQueued { op, expected_revision, reason }` proposed by the leader's policy timer; apply checks the op is still `Queued` at that revision. Both clocks must be log time (`AddAction`'s committed time for the action's age). **Not covered by FENCED.md** (§4.6). | 67cebb5bb8 (#2784), 31be88082f (#2799). On Redis the age compares this node's clock with another node's `insert_timestamp`. |
| SS4 | `:658-671` `dispatch_to_worker` | An assignment that loses the version CAS (`Aborted`) is reported as `LostRace`; nothing is sent and nothing is retried. | L | `Offer` CAS rejected in apply (`RevisionConflict`); no retry. Already the right shape. | 0b5d3030c2: a retried stale assignment had dispatched attempt 3 to a worker lost two requeues earlier. |
| SS5 | `:687-690` `dispatch_to_worker` | `dispatched_at = now_fn()` in whole seconds starts the dispatch-ack clock (AW16). | D·W | Deleted. The offer deadline is the detector's, on the leader's monotonic clock, and only proposes (AW16). | Doc drift: `worker.rs:103` and `schedulers.rs:201-203` say the dispatch is dated by the *worker's last message*; the code uses the scheduler clock at the send. |
| SS6 | `:839-847` `do_try_match_inner` | One `now` is read at pass start and reused for every silent-worker check (AW6) and unsatisfiable observation in the pass; a pass can run for seconds (warned at 5 s, SS8). | L·W | Leader reads its clock per decision; decisions use log time. | |
| SS7 | `:851-867` `do_try_match_inner` | No worker connected here and (trusted census) on any peer → every queued action is judged by the no-worker tracker → `FAILED_PRECONDITION` after `no_worker_action_timeout_s`. | C·W | As SS3: `FailQueued` from log time; "no worker anywhere" becomes "no open session in the applied state" (no peer census under one leader). Not in FENCED.md. | 31be88082f (#2799). |
| SS8 | `:875-881`, `:1039-1046` | Slow query (>1 s) and slow pass (>5 s) warnings. | N | Kept. | |
| SS9 | `:1062-1072` `new` | `on_matching_engine_run` sleeps 1 ms after every pass so other tasks get the lock. | L | Kept or replaced by a yield; under simulation it is a tokio timer per pass. | |
| SS10 | `:1074` `new` | Production `now_fn` is `SystemTime::now`. Every scheduler decision below reads this. | W | One injected clock with two faces: monotonic for local timers, log time for anything replicated. | |
| SS11 | `:1128-1156` `new_with_fleet_exchange_interval` | Zero means default: `worker_timeout_s`→10, `client_action_timeout_s`→60 (error log if ≤ 10), `max_job_retries`→3, `unacknowledged_kill_timeout_s`→60. | N | Knob table §1.2. | `max_job_retries: 0` cannot mean "never retry"; it means 3. The raft backend does **not** apply this substitution (DF4). |
| SS12 | `:1201-1209` `new_with_fleet_exchange_interval` | `worker_timeout_s` becomes the state manager's `no_event_action_timeout`. One knob therefore sets: worker eviction (AW26), the silent-worker filter (AW6), the client-stream timeout poll (SM3), the Stale→`WorkerSilent` requeue (SM12), the `Action.timeout` grace (SM9) and Redis abandonment (SA8). | C | `worker_timeout_s` becomes the failure detector's suspicion threshold (AW26, and the suspected-session filter AW6) and nothing else; SM3, SM9 and SM12 are deleted (SA8 stays on Redis). | |
| SS13 | `:1229-1247` revoke hook | A state-manager write that takes an executing action from its worker `background_spawn`s `kill_revoked_attempt` (one task per revoke, unordered). | C | `Revoke(op, attempt)` obligation derived from applied state, sent by the effect dispatcher; the op is requeued only behind the revoke barrier. | 0b5d3030c2 introduced it; the 5 s sweep (AW28) stays as backstop. |
| SS14 | `:1251-1255`, `:1354-1363` matching loop | `fallback_match_interval_s` (5 s) reruns a pass with no notification (missed notifies, Redis search lag). | L | Optional. FENCED deletes "the periodic sweep"; on a log-driven leader the apply hook wakes matching. Harmless if kept. | 7a4279b4e5 (#2557). |
| SS15 | `:1263-1278` abandoned sweep task | Every `client_action_timeout_s` runs `sweep_abandoned_queued_actions` (SM6). | L | Deleted with SM6. | 090d9722aa (#2726). |
| SS16 | `:1280-1338` fleet exchange task | Every 5 s (or on a local join/leave) publishes this scheduler's worker shapes with a 15 s TTL and reads peers'; `set_peer_fleet(…, now)` stamps census freshness. Feeds SS3/SS7 via AW1. | D·W | Deleted: one leader holds every session, so there is no peer fleet. | 31be88082f (#2799). Peer liveness is decided by Redis key TTL (server clock, SA13). |
| SS17 | `:1284` | `Uuid::new_v4()` scheduler id for the fleet record. | N | Deleted with SS16. | Randomness (§5). |
| SS18 | `:1364-1369` matching loop | After a failed pass, retry in 100 ms. | L | Kept. | e691bea24b (#1892). |
| SS19 | `:1370-1386` matching loop | Sleep until `min(max_wait, unsatisfiable deadline)` or a task/worker notify. | L | Kept (leader-local). | |
| SS20 | `:1388-1469` matching loop | `weak_inner.upgrade()` failing is read as shutdown and the task returns for good. It can fail while `Arc::new_cyclic` is still constructing, if a `Notify` permit is already stored. | L | Not a timer: a scheduling race. Gate the loop on construction (branch fix). | branch:`fix/matching-engine-startup` 373d2bae94: a scheduler that never matches. |
| SS21 | `:1390-1399`, `:1481-1495` | `worker_match_logging_interval_s` (std `Instant`) turns on full logging, which also bypasses the static-verdict cache (`api_worker_scheduler.rs:644`). Same verdict either way. | N | Kept. | |
| SS22 | `:288-292` `origin_event_id` | `Uuid::now_v6` for origin events. | N | Kept. | Randomness and clock (§5). |

### 2.2 `nativelink-scheduler/src/simple_scheduler_state_manager.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| SM1 | `:85-105` `version_conflict_backoff` | `MAX_UPDATE_RETRIES` = 5; backoff `10 ms·2^min(k-2,4)` plus "jitter" = `SystemTime::now()` nanos mod 20 ms. Used by SM7 and SM16. | L | Deleted: no retry-on-conflict anywhere; CAS lives in apply. | Jitter from the wall clock is a determinism leak (§5). |
| SM2 | `:252-254`, `:315-322` `client_last_seen` | Exposes `last_client_keepalive_timestamp` to the admin API. | N | Replaced by the watcher count. | |
| SM3 | `:324-341` `MatchingEngineActionStateResult::changed` | While a **client** polls its stream, every `no_event_action_timeout` (= `worker_timeout_s`) with no change it re-reads the op and, unless `Queued` (`:349-353`), asks `timeout_cause`. This loop is the *only* driver of executing-action timeouts: an op nobody is watching never times out here, and N watchers on N schedulers are N concurrent deciders (the mutex in SM14 is per process). | D | Deleted. Executing ops are bounded by the worker (`Action.timeout`) and by session expiry. | 93f4eaddad (#2083), 10ed3c57b2 (#2232). |
| SM4 | `:361-383` `changed` | `timeout_cause` → `timeout_operation_id` from inside a client read; an update error propagates out (`?` at `:383`) and fails the client's `Execute`/`WaitExecution` stream. | C | Deleted. | |
| SM5 | `:385-393` `changed` | After 5 timeouts that changed nothing the client's stream fails with `Internal`. | C | Deleted. | |
| SM6 | `:627-714` `sweep_abandoned_queued_actions` | Queued op whose `last_client_keepalive_timestamp + client_action_timeout < now` → completed `DEADLINE_EXCEEDED` ("no more clients listening"); a version conflict leaves it for the next sweep (`:692-702`). | C·W | Deleted (`client_action_timeout_s` deleted). Interest is a watcher count; a queued op with no watcher is cancelled by a logged `RequestCancel`, then collected (§4.7). | 090d9722aa (#2726). On Redis the keepalive was stamped by another node (SA2); branch:`fix/store-keepalive-retry` 67505a6c78: a Redis blip retired a live client's action. |
| SM7 | `:728-795` `inner_fail_queued_operation` | Retry-on-conflict ×5 with SM1 backoff; reloads and re-checks `Queued` each time. | L | Deleted: one CAS log entry (`FailQueued`). | |
| SM8 | `:803-808` `timeout_cause` | Reads `now` (this node's wall clock) for every comparison below. | W | Deleted with SM9–SM12. | |
| SM9 | `:810-834` `timeout_cause` (ActionTimeout) | `now − max(last_transition, last_worker_updated) > Action.timeout + worker_timeout_s` → `UpdateWithError(DEADLINE_EXCEEDED)` → `attempts += 1` and **requeue**. The anchor is the assignment write (nothing refreshes it, SM18), but the worker's own timer starts only after input fetch (up to `max_download_timeout_s`, 10 min), so the scheduler pre-empts the worker's own result. Client-visible difference: the worker reports a completed `DEADLINE_EXCEEDED` (not retried); the scheduler path retries up to `max_job_retries` and then returns `FAILED_PRECONDITION`. | C·W | **Deleted.** The worker enforces `Action.timeout` and reports `Finished(attempt)` with `DEADLINE_EXCEEDED` (RM5). FENCED.md deletes `max_action_executing_timeout_s` but does not name this branch (§4.1). | 5b4e555070 (#2826) added the grace. |
| SM10 | `:836-845` `timeout_cause` | Registry liveness (`check_liveness(worker, worker_timeout_s, now)`); no registry or no worker → treated as `Stale`. | D·W | Detector input only. | |
| SM11 | `:847-876` `timeout_cause` (Alive, Unknown) | `Alive` and `max_action_executing_timeout_s > 0` → requeue if `now − last_worker_updated > max`. `Unknown` (worker not registered on this process: a peer's worker, or an orphan) → the same with `max`, or `ORPHANED_ACTION_TIMEOUT` = 1 h. `last_worker_updated` is only written by state changes (the per-op `KeepAlive` update has no production sender, SM18), so both are "has run longer than X", not "no worker update": any action running longer than 1 h on a peer's healthy worker is requeued and charged an attempt. | C·W | Deleted (FENCED deletes `max_action_executing_timeout_s`; `Unknown` disappears with a single owner). | 5549a969bd (#2153), d4fa6af626 (#2680), a50385e27a (#2685). Config doc (`schedulers.rs:179-183`) says "without any worker update", which the code cannot observe. |
| SM12 | `:878-880` `timeout_cause` (Stale) | Registered here, silent longer than `worker_timeout_s`, not yet evicted → `WorkerSilent` → `UpdateWithDisconnect` (`worker_losses += 1`), requeued. A second path to the same verdict as AW26, on a different trigger (a client poll). | C·D | Detector input. The requeue happens only when `ExpireSession { worker, epoch }` commits. | 4d0538333a. |
| SM13 | `:884-970` `apply_filter_predicate` | Any filtered **read** (`WaitExecution`, `GetOperation`, admin `queued_demand`, the matcher's queue listing) completes a non-finished op whose client keepalive is older than `client_action_timeout_s` with `DEADLINE_EXCEEDED`, retries ×5 on conflict with a re-check (`:914-965`, no sleep), and revokes the holder (`:928`). The read path writes. | C·W | Deleted (no `client_action_timeout_s`; reads are side-effect free and go through read-index). | 9fcf5b1de4 (#1965), 0c41992c42 (#2765). |
| SM14 | `:1041-1097` `timeout_operation_id` | Per-process mutex; re-checks `timeout_cause` on a fresh read; `WorkerSilent` → `UpdateWithDisconnect` naming the old worker; anything else → `assign_operation(Err(DEADLINE_EXCEEDED))` → `UpdateWithError` with **no worker id** and retry-on-conflict on. | C | Deleted. | **Bug, by reading:** if the conflict that forces the retry was a requeue plus redispatch, the retry re-reads and applies `UpdateWithError` to the *new* attempt: the worker-id check (`:1218`) is skipped because no worker is named, so the new attempt is requeued and charged. |
| SM15 | `:1008-1019` `apply_filter_predicate` | `worker_update_before` / `completed_before` filters compare stored wall-clock times. No caller sets them. | N | Delete. | Dead code. |
| SM16 | `:1175-1192` `inner_update_operation_attempt_retrying` | Every worker update except assignments: on `Aborted`, sleep (SM1) and re-read, then re-apply the same update to the newer record (×5). | L | Deleted. Each worker message is a CAS command keyed on `(attempt, epoch)`; a stale one is rejected, not re-applied. | Re-applying a decision computed from a stale read is a retry with side effects (SM14). |
| SM17 | `:1161-1164`, `:1264-1283` `Accepted` | Recorded only with `report_executing_on_accept`; sets `accepted`, retried on conflict. | L | `Accepted(attempt)` log command, unconditional (offer barrier). | 20aa57241a. |
| SM18 | `:1284-1297` `KeepAlive` update | Writes `last_worker_updated_timestamp = now`. No production code sends `UpdateOperationType::KeepAlive`. | N | Delete (per-op heartbeats are not a thing in FENCED). | Its absence is why SM9/SM11 measure from assignment. |
| SM19 | `:1314-1345` `UpdateWithError` | `attempts += 1` unless `RESOURCE_EXHAUSTED` (backpressure); `attempts > max_job_retries` → completed `FAILED_PRECONDITION`. Charged by scheduler timeouts (SM9, SM11), worker-side timeouts reported as `InternalError` (RM rows), and worker errors. | C | One budget decider, in apply, counting attempts that reached `Accepted` and ended without a committed result (FENCED). §4.3 for what changes. | 71bb05a0ee (#2798), 3c52ee1a70 (#2829). |
| SM20 | `:1346-1377` `UpdateWithDisconnect` | `worker_losses += 1`; over `max_worker_loss_retries` → `FAILED_PRECONDITION`. Charged by every eviction, so by every detector timeout (AW26, AW27, SM12). | C | Charged in apply when `ExpireSession` commits, for `Accepted` attempts only. | 3c52ee1a70 (#2829), 4d0538333a. |
| SM21 | `:1378-1425` `UpdateWithEscalation` | `escalations += 1`; over `memory_escalation.max_steps` → `FAILED_PRECONDITION`. The trigger is the worker's sampled memory kill (RM1). | C | Logged with the result (`Finished(attempt)` carries the kill outcome); budget in apply. | 3c52ee1a70 (#2829). |
| SM22 | `:1427-1433` `UpdateWithDecline` | Requeue untried, nothing charged. Reached from a worker decline, the dispatch-ack timeout (AW16), a full channel (AW12) and a send failure (AW13). | C | `Declined(attempt)` command; the timer-driven callers go through the revoke barrier (§3 step 4). | a21d82c8bd (#2808). |
| SM23 | `:1450-1469` | Every write stamps `last_transition_timestamp` and `last_worker_updated_timestamp` with this node's `now`; the record is persisted (Redis, raft) and read later by SM9/SM11/SA8 on any node. | W | Timestamps in records become informational, or are set in apply from log time. | |
| SM24 | `:1534-1547` `inner_add_operation` | Passes `no_event_action_timeout` to `add_action` (only Redis uses it, SA8). | N | Deleted. | |

### 2.3 `nativelink-scheduler/src/api_worker_scheduler.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| AW1 | `:271-307` `peer_census_trusted`; `:585-594` | Peer absence may fail an action only when the census has been known for `record_ttl` (warm) and refreshed within `record_ttl` (fresh), both on this node's wall clock. A backwards step reads as not warm; a future-dated refresh as fresh. | D·W | Deleted (no peer census). | 31be88082f (#2799). |
| AW2 | `:333-361` `refresh_lifetime` | `last_update_timestamp = max(old, received)`: the scheduler's processing-time stamp of any worker message. Records the keepalive gap metric. | D | Detector input, stamped **at receipt** on a monotonic clock. | 76b1f93075 (#2900) (backwards step used to drop the message). The stamp is taken when the per-worker queue is processed (WA4), not when the message arrived. |
| AW3 | `:362-419` `refresh_lifetime` | Keepalive-only effects: store the reported `free_memory_kb`; lift a backpressure pause ("a pause lasts one keepalive interval"), or a load pause once free memory covers `pause_needs_kb`; lift `idle_declined_kb`. Changes which worker gets the next action. | L | Leader-local placement state, not logged (offers are CAS; a wrong pause costs latency). | 71bb05a0ee (#2798), 7d3d20ca83 (#2876). |
| AW4 | `:433-472` `add_worker` | `LruCache::put` silently replaces a live entry with the same id; the replaced worker's `running_action_infos` are dropped, not requeued; a later timeout (SM11) requeues them while the old process may still run them. | C | `OpenSession` supersession in apply (raft `WorkerJoin` already requeues the old epoch, `state.rs:856-890`). | branch:`fix/worker-replacement-evicts` a7a190daef. Needs an id collision (ids are per-connection UUIDv6 plus prefix). |
| AW5 | `:525-566` `set_peer_fleet` | Stamps `peer_fleet_known_since` / `peer_fleet_last_refresh` with the exchange task's `now`. | W | Deleted. | |
| AW6 | `:673-702` `find_available_worker` | A worker not heard from for `worker_timeout_s` (by this node's clock vs `last_update_timestamp`) is not offered work. | D·W | Detector suspicion: the leader stops offering to a suspected session. Not logged; safe because offers are CAS. | d9d0fa46f9. |
| AW7 | `:716-754` `find_available_worker` | `idle_declined_kb` and `live_memory_veto` compare the action's reservation with the last keepalive's `free_memory_kb` (sampled every keepalive, CP2). | D | Leader-local placement input. If offers must be replayable, the sample goes into the `Offer` entry. | 4c06bb81ec (#2796). |
| AW8 | `:854-859` `inner_find_worker_for_action` | Census not trusted → `WaitingForCapacity` instead of `Unsatisfiable` (fail open). | W | Deleted. | |
| AW9 | `:1085-1096` `update_action` | An update for an op this worker no longer holds is refused (`Internal`); the worker is kept. Drops a late result. | C | Apply rejects a non-current attempt (`StaleAttempt`) and answers with `Kill(op, n)`. | |
| AW10 | `:1121-1127` `update_action` | An update for an op with `kill_requested_at` set is ignored (worker bookkeeping still completes). Drops the result, including a valid one if the kill landed on the wrong attempt (AW28). | C | Delete. Validity is `(attempt current ∧ epoch live)` in apply. | repro:`repro/kill-revoked-toctou`. |
| AW11 | `:1155-1176` `update_action` | Backpressure or at-capacity pauses the worker until its next keepalive. | L | Leader-local. | 71bb05a0ee (#2798). |
| AW12 | `:1201-1229` `worker_notify_run_action` | Worker channel full → requeue untried (`Decline "channel_full"`) and pause. An ownership change with no log fact. | C | The dispatcher keeps the obligation (op stays `Offered`) and resends, or proposes `RevokeOffer`; never a silent requeue. | |
| AW13 | `:1230-1271` `worker_notify_run_action` | Send failure → evict the worker (every op `UpdateWithDisconnect`, `worker_losses += 1`) and requeue this one untried. | C | A closed stream is a detector input; eviction only via committed `ExpireSession`. | 1055cd1504 (#1972). |
| AW14 | `:1299-1330` `dispatch_accepted` | An ack for an attempt this worker does not hold → `kill_unknown_operation(op, attempt)`. | C | Apply rejects `Accepted(n)` for non-current `n` and emits `Kill(op, n)` (DESIGN §4.2). | dbd6557830. |
| AW15 | `:1352-1406` `dispatch_declined` | Decline of the held attempt → requeue untried, pause (`pause_needs_kb` / `idle_declined_kb`). | C | `Declined(attempt)` command. | a21d82c8bd (#2808). |
| AW16 | `:1410-1420`, `:1680-1717` `refresh_worker` | With `dispatch_ack_timeout_s` set: on each later message from the worker, every unaccepted dispatch with `dispatched_at ≤ now − timeout` is declined as `"unacknowledged"` (attempt 0) → requeued untried, worker paused. The `StartExecute` may still be in flight: until the late ack is answered with a kill (AW14), two attempts can be live. Iteration order is `HashMap` order (`worker.rs:424`). | C | Offer deadline = detector input on the leader's clock → `RevokeOffer { op, attempt }` (or `ExpireSession`); requeue only after the worker acks the revoke or the session expiry commits (revoke barrier). **Not named in FENCED.md** (§4.5). | a21d82c8bd (#2808). Off by default. |
| AW17 | `:1422-1482` `worker_notify_kill_operation` | Full channel → forget the kill request so the next sweep resends (L); any other send failure → evict the worker (C). | C | Kill is a dispatcher obligation resent at-least-once until a log fact discharges it. | |
| AW18 | `:1485-1526` `immediate_evict_worker` | Removes the worker and requeues every op it held (`UpdateWithDisconnect`), draining a `HashMap` (random order). | C | `ExpireSession` apply requeues the session's ops in sorted order (`state.rs:407-432` already does). | 1055cd1504 (#1972). |
| AW19 | `:1627-1632` `no_worker_anywhere` | No local workers ∧ census trusted (`now`) ∧ peers empty. Gates SS7. | W | "No open session" in applied state. | |
| AW20 | `:1719-1722` `refresh_worker` | Registry heartbeat is stamped after the `inner` lock (held across the unack sweep's awaits). A punctual keepalive behind a contended lock refreshes neither liveness source before AW26 reads them. | D | Stamp at receipt (AW2). | branch:`b7r6/tl1-keepalive-liveness-starvation` 548788fe13. |
| AW21 | `:1797`, `:1826` | `find_worker_time_ns` metric. | N | Kept. | |
| AW22 | `:1975-2002` `add_worker` | Registers the worker in the registry at its connect time. | D | `OpenSession { worker, epoch }` committed before the session serves. | |
| AW23 | `:2014-2046` `update_action_for_attempt` | A message naming an attempt older than the one held is refused (`FAILED_PRECONDITION`). Attempt only, no epoch. | C | Apply checks `(attempt, epoch)`. | dbd6557830, 0b5d3030c2. |
| AW24 | `:2109-2135` `remove_worker`, `worker_disconnected` | `GoingAway` (non-drain) or stream end → registry removal and eviction (requeue all). | C | `GoingAway` → `CloseSession { worker, epoch }` (a fact). Stream end without `GoingAway` → detector input → `ExpireSession`. | 2f54cc3bcb (#2797), 2bfa0207f8 (#2807). |
| AW25 | `:2137-2158` `shutdown` | Scheduler shutdown evicts every worker, requeueing all their work. | C | A stopping or deposed leader drops its streams and requeues nothing; sessions resume on the next leader (leadership barrier). | 17088593e5 (#2012). |
| AW26 | `:2160-2256` `remove_timedout_workers` | A worker is evicted (all ops requeued, `worker_losses += 1`) when neither `last_update_timestamp > now − worker_timeout_s` nor the registry says alive. Snapshot under the lock, release, await the registry, re-lock and evict **without re-checking**: a keepalive in the window does not save the worker. | C·D | The failure detector. It proposes `ExpireSession { worker, epoch }`; nothing is requeued until that commits; late messages with that epoch are then rejected. | 2f54cc3bcb (#2797), 76b1f93075 (#2900). branch:`fix/eviction-rechecks-heartbeat` f7483a11ec (TOCTOU). |
| AW27 | `:2174-2178`, `:2191-2199` | `kill_overdue`: any op whose `kill_requested_at` is older than `unacknowledged_kill_timeout_s` → evict the worker (requeue its other ops). | C·D | Revoke-ack deadline is a detector input proposing `ExpireSession`. **Not named in FENCED.md** (§4.5). | 4ae820a8e9 (#2707). The deadline starts from the worker's *last message*, not the kill (SW1), so it fires early. |
| AW28 | `:2314-2412` `kill_revoked_operations` | Every 5 s (WA2): snapshot `(worker, op, attempt)` with no kill requested, check `is_executing_on_worker` (op level, no attempt) twice with `buffer_unordered`, then kill under the lock if the held attempt still matches. Killing sets `kill_requested_at`, which makes AW10 drop that attempt's result. | C | Deleted (FENCED: kills are obligations sent at once, discharged by log facts). | a85114732a (#2693). repro:`repro/kill-revoked-toctou`; branch:`fix/kill-revoked-revalidates` 2760207eb1 (the attempt guard at `:1435` is the merged half). |

### 2.4 `nativelink-scheduler/src/worker.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| SW1 | `:95-102`, `:309-323` `notify_update(KillOperation)` | `kill_requested_at = last_update_timestamp` (the worker's last message before the kill), the start of AW27's deadline. The comment says it "fires at most one worker timeout late, never early"; since `last_seen ≤ send time`, it fires up to `send − last_seen` (< `worker_timeout_s`) **early**, never late. | C·W | Revoke deadline counted from the revoke's send on the leader's monotonic clock. | Comment is wrong. |
| SW2 | `:137`, `:424-430` | `running_action_infos: HashMap`; `unacknowledged()` returns ops in random order; drained in random order by AW18. | N | Sorted or log-ordered. | Determinism (§5). |
| SW3 | `:262` `Worker::new` | `connected_timestamp` metric from `SystemTime::now()`. | N | Kept. | |
| SW4 | `:392-407` `keep_alive` | Scheduler→worker `KeepAlive`, dropped when the channel is full. Only `send_keep_alive_to_worker_for_test` calls it. | N | Delete. | Dead in production. |

### 2.5 `nativelink-scheduler/src/worker_registry.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| WR1 | `:24-28` `ORPHANED_ACTION_TIMEOUT` | 1 h ceiling for an op whose worker this process does not know (SM11, SA8). | C | Deleted (single owner; `Unknown` cannot happen). | a50385e27a (#2685). |
| WR2 | `:70-81` `update_worker_heartbeat`, `register_worker` | Plain insert of `now`; a later-processed older stamp regresses it. | D·W | Monotone, stamped at receipt (AW2). | branch:`b7r6/tl1-keepalive-liveness-starvation` makes it monotone. |
| WR3 | `:87-118` `check_liveness` | `last_seen + timeout > now` → `Alive`, else `Stale`; overflow → `Alive`; not registered → `Unknown` ("not mine to judge"). | D·W | The detector's local view. | d4fa6af626 (#2680). |
| WR4 | `:120-127` `is_worker_alive` | Used by AW26. | D | As WR3. | |

### 2.6 `nativelink-scheduler/src/unsatisfiable_tracker.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| UT1 | `:33`, `:141-146` | `WARN_INTERVAL` 60 s log rate limit per shape. | N | Kept. | |
| UT2 | `:102-177` `observe` | Per shape: `first_unsatisfiable`, idle-gap carry-over (`:128-131`), `waited`, `is_due`; per action: `action_eligible = now − insert_timestamp ≥ timeout`, `earliest_eligible`. The inputs to SS3/SS7. | C·W | Same logic over log time. `insert_timestamp` must be the `AddAction` commit time, not the execution server's wall clock. | 31be88082f (#2799). A clock that steps back reads as "not yet" (safe); one that steps forward fails early. |
| UT3 | `:204-220` `end_pass` | A shape not seen this pass lingers for one timeout if the fleet did not change, so it stays due for actions that arrive later. | C | As UT2. | |
| UT4 | `:227-257` `next_deadline` | Wake schedule: time to due or to the next eligible action, else backoff `1 s·2^n` up to 32 s for stuck due shapes. | L | Kept (leader-local). | |

### 2.7 `nativelink-scheduler/src/memory_awaited_action_db.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| MD1 | `:182-214` `MemoryAwaitedActionSubscriber::changed` | While a client's subscriber is polled, every `CLIENT_KEEPALIVE_DURATION` (10 s) it sends `ClientKeepAlive`. It is only polled while the execution server pulls the response stream (EX3), so it measures "the server task polled", not "the client is there". | D | Deleted: a watcher count (`ClientAttach`/`ClientDetach`), no keepalive. | DESIGN: `client_action_timeout_s` must exceed twice the keepalive (a 10 s sleep sends every 10–20 s). |
| MD2 | `:158` `new_with_client` | `last_keep_alive = I::from_secs(0)` means "long ago" so the first poll sends at once; `MockInstantWrapped::from_secs` ignores its argument (UX2), so in mock-clock tests the first keepalive waits 10 s. | N | — | Mock and real clocks behave differently (§5). |
| MD3 | `:365-368`, `:865-866`, `:938-939`; `:491-502` | A waiting client leases its entry (no eviction); `StoppedWaiting` on subscriber drop refreshes it and releases the lease, so retention counts from when the client stopped waiting. | N | Becomes `ClientAttach`/`ClientDetach`. | f05ce8b122 (fixed eviction of a waited-on op). |
| MD4 | `:392-490` `ClientDroppedOperation` | When a client entry is evicted (retention elapsed since last touch, MD8) or replaced and it was the last client, the operation is removed **in any stage**: queued and executing ops vanish with the worker still running them; its result is then dropped ("probably dropped", `simple_scheduler_state_manager.rs:1202`) and AW28 kills the attempt. | C | Retention collects terminal ops only. A non-terminal op with no watcher is cancelled by a logged `RequestCancel` and goes through the revoke barrier before it is collected (§4.7). | |
| MD5 | `:503-524` `ClientKeepAlive` | `update_client_keep_alive(now)` without waking watchers. | D·W | Deleted. | |
| MD6 | `:631-666` `update_awaited_action` | Version CAS: mismatch → `ABORTED`. The source of every retry-on-conflict loop (SM7, SM13, SM16). | L | CAS in apply, no retry. | |
| MD7 | `:806-811` `add_action` | `OperationId::default()` (UUIDv4) and `AwaitedAction::new(…, now)` (SM23 timestamps). | W | Operation id minted from the log (or seeded RNG); timestamps from log time. | |
| MD8 | `:967` `new`; `nativelink-util/src/evicting_map.rs:517-538` | Client map is an `EvictingMap` anchored at construction; an entry older than `retain_completed_for_s` (whole seconds) is evicted lazily on the next `get`/`insert` (no sweep). Triggers MD4. | C | Retention as GC from log time (terminal ops). | |
| MD9 | `:917-925` `try_subscribe` | A second client joining an op stamps a keepalive at once. | D | Becomes `ClientAttach`. | |
| MD10 | `:979-994` `new` | Background task draining client events (`recv_many`). | N | — | Task scheduling (§5). |
| MD11 | `:1103-1117` `add_action` | `no_event_action_timeout` ignored. | N | — | |

### 2.8 `nativelink-scheduler/src/store_awaited_action_db.rs` (Redis backend)

DESIGN §1 keeps the Redis backend as is, so none of this is replaced by the fenced track; the column says what it would become if it were.

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| SA1 | `:196-228` `merge_client_keepalive` | Client keepalive = max of the record's stamp and the `ck_` key, either written by whichever replica holds the client. Every reader compares it with its own clock (SM6, SM13). | D·W | Out of scope (Redis stays). Under fenced: deleted. | Cross-node wall-clock comparison. |
| SA2 | `:327-362` `OperationSubscriber::changed` | Every 10 s, a client-backed subscriber writes `ck_<op>` with TTL `client_action_timeout_s`. On a write failure it still advances `last_known_keepalive_ts` and sleeps a full interval. | D·W | Out of scope. | branch:`fix/store-keepalive-retry` 67505a6c78: a Redis blip let a peer retire a live client's action. |
| SA3 | `:53-57`, `:380-424` | Versioned keepalive write with 8 retries at 100 ms. Dead: `USE_SEPARATE_CLIENT_KEEPALIVE_KEY` is `true`. | N | Delete. | |
| SA4 | `:433-450` | Sleep until the next keepalive is due (floor 100 ms); with an unreliable subscription manager, also polls the stage. | L | Out of scope. | |
| SA5 | `:484-487`, `:1225-1235` `add_action` | `cid_<client> → op` written with a 24 h TTL: after 24 h a `WaitExecution` for a still-running op is `NOT_FOUND`. | C | Out of scope. Under fenced: client mappings live as long as the op. | d9403b0a55 (#2417). |
| SA6 | `:877-897` `new` | Pub/sub error → sleep 1 s, then notify the matcher anyway. | L | Out of scope. | |
| SA7 | `:898-914` `new` | Active-count metric every 15 s (opt-in). | N | — | f42cc42ec5 (#2772). |
| SA8 | `:927-970` `executing_action_is_abandoned`; `:1020-1051` `try_subscribe` | An executing record is "abandoned" if its worker is `Stale` here and silent `worker_timeout_s`, or `Unknown` here and silent 1 h (since the last state write, SM18). Then `try_subscribe` **recreates** the action under a new operation id instead of joining: a second execution while the first may still be running on a peer's worker. | C·W | Out of scope. Raft's `AddAction` dedup has no abandonment clause (`state.rs:734-755`). | a50385e27a (#2685). |
| SA9 | `:985-1019` `try_subscribe` | Retry the unique-key search once after 20 ms to narrow the RediSearch visibility race (two `Execute`s both create). | L | Out of scope; raft dedups in apply. | 7e79dea424 (#2367); branch:`fix/awaited-action-prefer-live` faed855970 (N7 still open). |
| SA10 | `:407-411`, `:1153-1159`, `:1204-1209` | Completed records written with a Redis TTL of `retain_completed_for_s` (Redis server clock). | C | Out of scope (GC of terminal records). | 43ab01dbaa (#2315), 5ef7a8315d (#2803). |
| SA11 | `:1168-1223` `add_action` | On a version mismatch, `continue` the loop: no cap, no sleep. | L | Out of scope. | Unbounded retry-on-conflict. |
| SA12 | `:1202` `add_action` | Stamps the client keepalive at add. | D | — | |
| SA13 | `:1252-1284` `exchange_fleet_capabilities` | Publishes this scheduler's shapes with TTL `record_ttl` (15 s); a peer that stops publishing disappears when Redis expires the key. | D·W | Deleted with SS16. | 31be88082f (#2799). |

### 2.9 `nativelink-scheduler/src/awaited_action_db/{mod.rs,awaited_action.rs}`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| AA1 | `mod.rs:34-35` | `CLIENT_KEEPALIVE_DURATION` = 10 s, used by MD1, SA2, RA3. | D | Deleted. | |
| AA2 | `mod.rs:89-95` `SortedAwaitedAction::cmp` | Queue order: sort key, then `operation_id` (a random UUIDv4) as tie-break. | N | Tie-break on the `AddAction` log index. | Randomness in queue order (§5). |
| AA3 | `awaited_action.rs:72-80`, `:183-211`, `:335-386` | `last_worker_updated_timestamp`, `last_client_keepalive_timestamp`, `state.last_transition_timestamp` are wall-clock values set by the writer and persisted; consumed by SM6, SM9, SM11, SM13, SA8. | W | Informational only, or set in apply from log time. | |
| AA4 | `awaited_action.rs:302-307`, `:510-514` `sort_key` | Priority, then `ActionInfo.insert_timestamp` in nanoseconds (EX2: the receiving node's wall clock). FIFO across nodes is ordered by their clocks. | W | Order by `AddAction` log index within a priority. | Nanosecond key; exact ties fall to the random op id (AA2). |
| AA5 | `awaited_action.rs:451-460`, `:510-514` | `duration_since(UNIX_EPOCH).unwrap()` panics on a pre-epoch `insert_timestamp`. | N | — | Robustness. |

### 2.10 `nativelink-scheduler/src/raft_awaited_action_db/` (P1/P2 raft backend)

Production proposes only `AddAction`, `UpdateAwaitedAction`, `DropClients` and `ReplicaRestarted`. `AssignToWorker`, `Heartbeat`, `Expire`, `WorkerJoin` and `WorkerLeave` are reached from tests only (`grep` over `nativelink-scheduler/src` and `nativelink-service/src`), so `sessions`, `operation_owner` and `leases` in `AppliedState` are dead state in production.

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| RA1 | `mod.rs:152-266` `ClientTracker` | Leader-local, wall-clock: last use per client id, newest keepalive per op, attached count. A client detached and unused for `retain_client` is proposed for removal (`take_stale`, `:234-244`). The decision is logged (RA4). | C·W | Kept as retention, but measured from log time of the last `ClientDetach`, and only for terminal ops (§4.7). | f05ce8b122, 39360ad252. |
| RA2 | `mod.rs:286-293` subscriber `Drop` | Detach at `now`: retention starts. | N | `ClientDetach`. | |
| RA3 | `mod.rs:311-345` `RaftAwaitedActionSubscriber::changed` | Client keepalive every 10 s into the tracker (local, not logged). | D | Deleted (watcher count). | |
| RA4 | `mod.rs:769-809` `drop_clients`, `evict_stale_clients`; `state.rs:315-329`, `:917-928` | `DropClients` commits; apply removes every op no client refers to, **any stage** (same hazard as MD4). Eviction runs only on `add_action` (lazy). | C | `Collect { op }` for terminal ops; `RequestCancel` + revoke barrier first for non-terminal ones. | |
| RA5 | `mod.rs:1018-1035` `get_awaited_action_by_id` | A stale client looked up is dropped through the log and the lookup returns `None` (`NOT_FOUND` to the client). | C | As RA4. | |
| RA6 | `mod.rs:486-496` `new` | openraft timers hard-coded: heartbeat 100 ms, election 200–400 ms. Election jitter uses openraft's own RNG. | L | Config (`heartbeat_interval_ms` 250, `election_timeout_{min,max}_ms` 1000/2000, FENCED cluster contract). Liveness only. | §5 for the RNG. |
| RA7 | `mod.rs:536-551` `open`; `state.rs:891-916` | On start with anything executing: `ReplicaRestarted { now }` requeues every executing op (uncharged, sorted order) and clears sessions. | C | Wave 1: `LeaderChanged { node_id, term, now }` with the same semantics after the leadership barrier. Wave 2: session resume replaces the blanket requeue. | 36f5f46442. |
| RA8 | `mod.rs:552-571` `open` | Every restored client gets a fresh keepalive and retention window from `now`. | N | The new leader starts retention for every op at its ready time (watchers died with the old leader; §4.8). | 39360ad252. |
| RA9 | `mod.rs:575-608` `start_raft` | Poll `ensure_linearizable` every 20 ms for up to 30 s; past that the backend is `Unavailable` for the life of the process. | L | Readiness route reports not-ready and keeps trying; no fixed deadline (leadership barrier). | |
| RA10 | `mod.rs:666-720` `spawn_proposer` | Group commit: whatever proposals are queued (up to 256) become one `Command::Batch`. Batch boundaries depend on arrival timing. | N | Kept. Apply is deterministic given the log; a simulator must replay the log, not the inputs. | b08ae84565. |
| RA11 | `mod.rs:811-823`, `:846-884`, `:897-932`; `state.rs:633-701`, `:828-855` | `expire` (`Command::Expire`, lease sweep by log time), `heartbeat` (per-op lease), `worker_join`/`worker_leave`. Test-only drivers. | N | `Heartbeat`/`Expire`/leases deleted (FENCED: no per-op leases). `WorkerJoin`/`WorkerLeave` become `OpenSession`/`ExpireSession`. | |
| RA12 | `mod.rs:1108-1131` `update_awaited_action` | Proposes the caller's whole record plus `now_unix_nanos`. The record's own timestamps (AA3) were taken on the proposing node and are **not** clamped to `log_now`. | W | Apply sets record timestamps from log time, or they stop feeding decisions. | |
| RA13 | `mod.rs:1133-1170` `add_action` | `OperationId::default()` (UUIDv4), `now` into `AddAction`. | W | Id from the log index or a seeded RNG; time is log time. | |
| RA14 | `state.rs:705-714` `apply_one` | `log_now = max(log_now, cmd.now)`; every command's `now` is clamped up to it (a slow-clock leader cannot move time back; a fast one moves it forward). | L | Kept: this is FENCED's log time. | DESIGN §6. |
| RA15 | `state.rs:584-591` `apply_update` | Requeue with `attempts > retry_limit` is rejected (`RetryCapExceeded`). A second retry-cap decider beside SM19. | C | The only decider, counting `Accepted` attempts. | **Bug, by reading (DF4):** production passes `max_job_retries` raw, so an unset knob gives `retry_limit = 0`: the first `UpdateWithError` requeue is rejected, the state manager retries it as a conflict ×5 and gives up, and the op is left `Executing` with no worker holding it. The parametrized suite uses `RaftDbConfig::default()` (5) and does not see it. |
| RA16 | `state.rs:99-100`; `:390`, `:533`, `:620`, `:683`, `:776` | `leases` updated on every apply, read only by `Expire` (test-only). | N | Delete. | |
| RA17 | `state.rs:404-432` `requeue_sessions_operations` | Requeues a session's ops in operation-id order so replicas agree. | N | Kept (the template for `ExpireSession`). | |
| RA18 | `store.rs:198-260` `WalWriter::start`; `:580` | WAL writer on a `spawn_blocking` thread with group `fdatasync`; snapshot write on `spawn_blocking`. Real OS threads. | N | Kept; a simulator uses the in-memory `LogStore` (`data_dir` unset). | b08ae84565. |

### 2.11 `nativelink-scheduler/src/default_scheduler_factory.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| DF1 | `:44-46`, `:286-293` `retain_completed_for_s` | 0 → 60 s for every backend. | C | Retention GC (terminal ops). | 5ef7a8315d (#2803). |
| DF2 | `:68`, `:176`, `:241` | `SystemTime::now` is the clock for every backend. | W | Injected clock (SS10). | |
| DF3 | `:214-223` | Redis client-keepalive TTL = `client_action_timeout_s` (0 → 60). | D | Out of scope. | |
| DF4 | `:259-284` `raft_db_config` | `retain_client` from `retain_completed_for_s`; `retry_limit: spec.max_job_retries` **without** the 0→3 substitution SS11 applies. | C | One retry budget, one default, decided in apply. | See RA15. |

### 2.12 Other scheduler files

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| GS1 | `grpc_scheduler.rs:98-140` `perform_request` | `Retrier` (exponential backoff, random jitter) around upstream `Execute`, `WaitExecution` and `GetCapabilities`, retrying on `UNKNOWN`, `CANCELLED`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS` by default. A retried `Execute` whose first attempt reached the upstream creates a second upstream operation: for an uncacheable action, a second execution. | C | Out of the fenced core (proxy). Retry `Execute` only on errors that prove it was not received; resume with `WaitExecution(name)`. | Retry with side effects. |
| GS2 | `grpc_scheduler.rs:160-196` `stream_state` | Upstream stream error or close ends the downstream stream; no resume. | C | As GS1. | |
| GS3 | `grpc_scheduler.rs:327-331` `inner_filter_operations` | Upstream error → empty stream (the client sees `NOT_FOUND`). | C | As GS1. | |
| CL1 | `cache_lookup_scheduler.rs:272` | `last_transition_timestamp: SystemTime::now()` on a cache hit. | N | — | |
| HR1 | `historical_resource_scheduler.rs:274-283` `refresh_hints` | Reloads the hints file every `refresh_interval_s` (std `Instant`); changes the reservations of later actions, so their placement. | N | Kept. | |
| HR2 | `historical_resource_scheduler.rs:497-503` | `class_by_timeout`: sizing class from `Action.timeout` (a request field, not a clock). | N | Kept. | |
| AD1 | `admin.rs:50`, `:55-91` `queued_demand`, `:114-121` `epoch_ms` | `client_last_seen_ms` and `epoch_ms`. `queued_demand` lists through `filter_operations`, so an admin read runs SM13 and can complete actions. | N | Reads become side-effect free (SM13 deleted). | 02a64f2b4c (#2804). |

Files read with **no sites**: `lib.rs`, `mock_scheduler.rs`, `known_platform_property_provider.rs`, `platform_property_manager.rs`, `property_modifier_scheduler.rs`, `worker_capability_index.rs`, `match_outcome.rs`, `worker_scheduler.rs` (trait only), `either_awaited_action_db.rs` (pass-through; its test factory builds raft with `RaftDbConfig::default()`, see RA15), `raft_awaited_action_db/wal.rs`, `raft_awaited_action_db/types.rs` (the `now_unix_nanos` fields, `now_mut` at `:183-195`, are covered by RA14), `raft_awaited_action_db/state_model_tests.rs` (seeded RNG, synthetic time).

### 2.13 `nativelink-service/src/worker_api_server.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| WA1 | `:71-79` `new` | `rand::rng()` node id for the UUIDv6 worker ids. | N | Session ids from the log (`OpenSession` epoch). | Randomness (§5). |
| WA2 | `:86-124` `new` | Per scheduler, a background loop: a 1 s `interval` → `remove_timedout_workers(SystemTime::now())` (AW26; the clock bypasses the injectable `now_fn` and `expect`s a post-epoch clock), and a `kill_revoked_operations_interval_s` (5 s) `interval` → AW28. Default `MissedTickBehavior::Burst`. Spawned for every scheduler in the map by every `WorkerApiServer` built, so two `worker_api` listeners sweep twice. | L | The detector loop runs on the leader and proposes `ExpireSession`; the kill ticker is deleted. | a85114732a (#2693). Tests drive keepalives through `now_fn` and eviction through the real clock (§5). |
| WA3 | `:209-228` `inner_connect_worker` | Each connection gets a fresh id (prefix + UUIDv6) and `Worker::new(…, now)`. Reconnect = new worker, so nothing survives a reconnect. | D | `OpenSession { worker, epoch }`; wave 2: resume `(worker, epoch, running attempts)`. | e753b8d4dc (#1578). |
| WA4 | `:306-336` `WorkerConnection::start` | Reader task + in-order processor per worker. The processor stamps liveness when it gets to a message (`:387`, `:423`), so a slow store write ahead of a keepalive makes a punctual worker look late. | D | Stamp detector input at receipt. | 2f54cc3bcb (#2797), 389cf73574 (#2856); branch:`b7r6/tl1-keepalive-liveness-starvation`. |
| WA5 | `:337-379` | Stream end without a non-drain `GoingAway` → `worker_disconnected` (AW24: evict, requeue all). | C·D | Detector input → `ExpireSession`. | 1055cd1504 (#1972), e9250ee832 (#1977). |
| WA6 | `:383-393` `inner_keep_alive` | `worker_keep_alive_received(now_fn() secs, load)` (AW2, AW3, AW16). | D | Detector input. | |
| WA7 | `:395-408` `inner_going_away` | `drain` → mark draining; otherwise `remove_worker` (requeue all). | C | `CloseSession { worker, epoch }` / a drain attribute on the session, both logged. | 2bfa0207f8 (#2807). |
| WA8 | `:410-434` `touch_liveness_best_effort` | Every other worker message refreshes liveness (best effort, so a result is never lost to a failed refresh). | D | Detector input. | 2f54cc3bcb (#2797). |
| WA9 | `:490-501` `dispatch_accepted` | The ack is recorded before the liveness refresh, because the refresh runs the unacked sweep (AW16) and would requeue an ack arriving right at the timeout. | C | Ordering vanishes once the offer deadline only proposes. | a21d82c8bd (#2808). |

### 2.14 `nativelink-service/src/execution_server.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| EX1 | `:232-234` `build_action_info` | No `Action.timeout` → `Duration::MAX` (SM9 never fires); `0` → zero (SM9 skipped). The worker treats both as `max_action_timeout_s` (RM6). | C | The scheduler stops reading it (SM9 deleted); the worker is the only reader. | `ActionInfo::try_from_action_and_execute_request` (UX8) defaults the same field to zero, not MAX. |
| EX2 | `:272-273` `build_action_info` | `insert_timestamp = SystemTime::now()` on the node that received `Execute`. Orders the queue (AA4) and ages unsatisfiable actions (UT2). | W | The `AddAction` commit (log index for order, log time for age). | |
| EX3 | `:326-347` `to_execute_stream` | Pull-based `unfold`: `changed()` (hence client keepalives MD1/RA3/SA2 and the timeout poll SM3) runs only while the transport polls the response. | D | Watcher attached while the stream exists; no polling dependence. | |
| EX4 | `:464-467` `inner_execute` | `OperationId::default()` (UUIDv4) as the client operation id. | N | Seeded or log-derived (§5). | |
| EX5 | `:484-509` `inner_wait_execution` | `filter_operations(client_operation_id)` runs SM13 (cancel on read). | C | Read-index read, no side effects (FENCED read barrier). | |
| EX6 | `:617-669` `wait_operation` | Client-supplied `timeout` bounds the wait with `tokio::time::timeout_at`; returns the last state seen. | N | Kept (a client-chosen bound on its own read). | |

### 2.15 `nativelink-util/src` (pieces on these paths)

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| UX1 | `instant_wrapper.rs:32-54` `impl InstantWrapper for SystemTime` | `elapsed()` = `SystemTime::elapsed().unwrap()` and `unix_timestamp()` unwrap: **panic** when the wall clock steps back past the instant (MD1's `last_keep_alive.elapsed()`, `EvictingMap::elapsed_seconds`). `sleep` is `tokio::time::sleep`. | W | Monotonic clock for elapsed; log time for replicated values. | Robustness bug; 76b1f93075 fixed the same class only in `refresh_lifetime`. |
| UX2 | `instant_wrapper.rs:60-96` `MockInstantWrapped` | Thread-local `MockClock`; `from_secs` ignores its argument (returns now); `sleep` busy-yields until `MockClock` advances. | N | Replace with a clock that a simulator drives and whose `sleep` is a tokio timer. | §5: livelocks under paused tokio time; diverges from production (MD2). |
| UX3 | `evicting_map.rs:517-538` `should_evict`, `elapsed_seconds` | Age in whole seconds since an anchor taken at construction; evaluated on access only. Leased keys exempt (`:862-875`). Used for the memory backend's client map (MD8). | C | Retention GC from log time (terminal ops). | |
| UX4 | `retry.rs:25-182` `Retrier` | Exponential backoff starting at `2·delay`, jitter fn, `max_retries`, retryable-code set. | L | Kept for idempotent calls; GS1 is the side-effecting user. | |
| UX5 | `nativelink-config/src/stores.rs:2043-2052` `make_jitter_fn` | `rand::rng()` jitter. | N | Seeded RNG (§5). | |
| UX6 | `connection_manager.rs:160-175`, `:255-320` | Reconnect backoff through `Retrier`, plus a 1 s sleep when the retry config is zero. | L | Kept. | 14b2cc684e (#2067). |
| UX7 | `action_messages.rs:297-309`, `:599-622`, `:631-703` | `ActionInfo.{timeout, load_timestamp, insert_timestamp}`; `ExecutionMetadata` timestamps (worker clocks, reported to the client). | N | Reported only. | |
| UX8 | `action_messages.rs:328-381` `try_from_action_and_execute_request` | Absent `Action.timeout` → `Duration::ZERO` (the execution server uses MAX, EX1). | N | One rule (the worker's). | Inconsistency. |
| UX9 | `action_messages.rs:1195-1214` `Ord for ActionState` | Ordered by `last_transition_timestamp`; `Eq` ignores it. Used for the "oldest actions" log (SS21). | N | — | `Ord`/`Eq` disagree; logging only. |
| UX10 | `action_messages.rs:69-72` `OperationId::default` | `Uuid::new_v4()`. | N | Injectable id source (§5). | |
| UX11 | `operation_state_manager.rs:58-60`, `:88-93` | `client_last_seen`; `worker_update_before` / `completed_before` filter fields (unused, SM15). | N | Delete the filters. | |

### 2.16 `nativelink-worker/src/local_worker.rs`

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| LW1 | `:65-67`, `:1623-1651` `LocalWorker::run` | After the run loop fails, wait up to 10 s (1000 × 10 ms through `sleep_fn`) for actions in transit to register before killing everything. | L | Kept (wave 1). | bafafc78f8 (#2658): overrunning it used to abort the worker. |
| LW2 | `:1653-1663` `LocalWorker::run` | Any run-loop error (stream closed, keepalive send failed, HTTP/2 PING timeout) → `kill_all()`: every running action is killed, and the worker reconnects under a new id. A single-use worker exits. | C | Wave 1 unchanged. Wave 2: keep running and resume the session with `(worker, epoch, running attempts)`; kill only on a committed `ExpireSession` seen by the worker. | |
| LW3 | `:120-137`, `:1573-1580` `reconnect_delay` | Reconnect backoff `0.5 s·2^n` capped at 30 s, jitter from `SystemTime` sub-second nanos. | L | Kept; jitter from a seeded RNG (§5). | 2bfa0207f8 (#2807). |
| LW4 | `:139-156`, `:1381-1412` connection factory | Endpoint connect and request timeout (5 s), TCP keepalive (30 s), HTTP/2 PING every 30 s with a 20 s timeout. A dead or stalled connection fails the run loop → LW2. | D | The worker's detector of the leader: input to reconnect and resume, not a reason to kill. | 389cf73574 (#2856). |
| LW5 | `:158-184`, `:421-433`, `:556-571`, `:946-993` | `GoingAway` bounded by the endpoint timeout (single-use exit, shutdown, drain). | L | Kept. `GoingAway` becomes `CloseSession`. | 2bfa0207f8 (#2807). |
| LW6 | `:297-359`, `:700-736` `preconditions_met` | Precondition script bounded by `precondition_timeout_ms` (30 s). Timeout or non-zero exit → `RESOURCE_EXHAUSTED` → sent as `InternalError` **after** `ExecuteAccepted` → the scheduler requeues untried and pauses the worker (SM19 exempts `RESOURCE_EXHAUSTED`). | C | Run the precondition before `Accepted` and answer `Declined(attempt)`. Under FENCED's "budget counts accepted attempts" this would otherwise start costing budget (§4.3). | f10c7b70c1 (#2810) added the bound. |
| LW7 | `:478-521` `start_keep_alive` | Keepalive every `endpoint.timeout / 2` (2.5 s), `MissedTickBehavior::Skip`, carrying `free_memory_kb` (CP2). A failed send fails the run loop → LW2. | D | Detector input, tagged `(worker, epoch)`. | 2f54cc3bcb (#2797). |
| LW8 | `:392-417`, `:661-672` `admission` | `max_inflight_tasks` and a cgroup free-memory sample decide a decline before the ack. | D | `Declined(attempt)` (uncharged). | 7d3d20ca83 (#2876). |
| LW9 | `:604-622` `run` | A `StartExecute` for a live `(op, attempt)` is re-acked, not run again (the scheduler may resend). | L | Kept: the dispatcher's at-least-once resend relies on it. | dbd6557830. |
| LW10 | `:729-758` | `ExecutionComplete` after the process exits (the scheduler releases nothing on it). | N | — | 8527f258f7 (#1975). |
| LW11 | `:769-775` | A cleanup error is merged into the action's result: a successful action becomes an error (RM16). | C | Cleanup runs after the result is reported and never changes it. | |
| LW12 | `:785-798` `make_publish_future` | The worker writes the `ActionResult` to the Action Cache **before** telling the scheduler. An attempt whose session has expired, or a superseded attempt, still publishes to the AC. | C | Not a timer, but it bypasses the fence entirely (§4.9). | |
| LW13 | `:946-993` shutdown | Drain waits up to `max_action_timeout_s` (default 20 min) for in-flight actions, then shuts down anyway. | L | Kept. | |
| LW14 | `:1144-1168`, `:1297-1301`, `:1330-1333`, `:1741-1755` | Defaults: action 20 min, upload 10 min, download 10 min, cleanup wait 30 s, cleanup backoff 500 ms, kill grace 5 s, kill tombstone 60 s, persistent idle 300 s / shutdown grace 5 s / acquire 30 s. | N | Knob table §1.2. | |
| LW15 | `:1342-1374` | Zombie reaper loop (`REAP_INTERVAL` 5 s, `spawn_blocking` `/proc` scan) and orphan directory sweep (`orphan_sweep_interval_s`, default off). | N | Kept (local GC). | 4fe0a27ba9 (#2823), f10c7b70c1. |

### 2.17 `nativelink-worker/src/running_actions_manager.rs`

Read in full (5279 lines). Line numbers verified against this branch.

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| RM1 | `:109`, `:289`, `:292-303`, `:413-544` resource sampler | Every 250 ms (disk walk every 40th sample, ~10 s): a memory kill needs two consecutive samples over reservation+headroom or one at twice it; a disk soft-limit kill likewise. Sample timing decides whether and when an action is killed (`KILLED_MEMORY` → escalation, SM21). | C | Stays on the worker; the outcome travels in `Finished(attempt)`. | fcdccc81fd, 7bd3c5424d. |
| RM2 | `:315-338`, `:352-373`, `:2844-2847` disk accounting | A file counts as action-written if its mtime ≥ a kernel-clock stamp (`futimens(UTIME_NOW)`; `SystemTime::now()` fallback). Decides the disk kill. | C·W | Stays on the worker; a monotonic marker would be better. | Inputs materialized with a client-supplied future mtime (`:1273-1284`) count as action-written: false disk kill. |
| RM3 | `:162`, `:3060-3078` output drain | The stdout/stderr read is capped at 5 s only if the action was killed or left stragglers. A clean exit whose `setsid` descendant still holds the pipe waits **forever**, and the action timer is not polled in that arm. | C | Stays on the worker; must be bounded by `Action.timeout` too. | Without PID namespaces (and off Linux). |
| RM4 | `:201-225`, `:2917-2924`, `:2955-2962` `stop_action`, `hard_kill` | SIGTERM to the group, SIGKILL after `kill_grace` (5 s; Linux only). `hard_kill` waits unbounded for exit (D-state). A second stop re-arms the grace and overwrites `kill_reason`. | C | Stays on the worker. A revoke ack must be sent only after the process group is reaped (§4.4). | 9a95e29153 (#2822). |
| RM5 | `:2928-2954` `inner_execute` | **The `Action.timeout` timer**: `sleep_fn(self.timeout)` from process spawn. Fires: `task_timeouts++`, `KillReason::Timeout`, SIGTERM+grace, `DEADLINE_EXCEEDED` on the result; the action completes and is reported (not retried). | C | **Stays on the worker: the bound for hung actions** FENCED relies on. Reported as `Finished(attempt)` with `DEADLINE_EXCEEDED`; commits only if the attempt is current and its epoch live. | Message uses `action_info.timeout` (`:2939`, `:2950`), not the enforced `self.timeout`: "0 seconds" when unset, wrong when clamped. |
| RM6 | `:4972-4989` `create_and_add_action` | Effective timeout: `Action.timeout`, or `max_action_timeout` if it is 0 or `timeout_handled_externally`; then clamped to `max_action_timeout` (20 min default). | C | Stays on the worker. | 9a95e29153 changed reject → clamp; `cas_server.rs:1272-1273` still says "will be rejected". |
| RM7 | `:2349-2381`, `:2472-2476`, `:1750-1792`, `:3180-3188` | `TimeoutMillis` env carries the *unclamped* requested timeout; a side-channel file (`Uuid::new_v4` name) with `{"failure":"timeout"}` turns the result into `DEADLINE_EXCEEDED` (`timeout_handled_externally`). | C | Stays on the worker. | Can advertise more time than is enforced. |
| RM8 | `:2505-2534`, `:2655-2662` persistent acquire | Wait for a pooled process bounded by `min(acquire_timeout, self.timeout)`; on timeout (or the pool's own, PW5) **fall back to one-shot with a fresh full `self.timeout`**: total runtime up to ~2× `Action.timeout`, possibly succeeding. | C | Stays on the worker; the fallback must get the *remaining* time. | 035dd63165 (#2830). |
| RM9 | `:2560-2589` persistent dispatch | `dispatch_with_timeout(remaining)` (PW2): SIGKILL, no grace, `Err(DEADLINE_EXCEEDED)` → reported as `InternalError` → the scheduler charges an attempt and **re-runs** a deterministic hang. `remaining` uses tokio `Instant` (`:2509`), not the injected clock. | C | Report `Finished(attempt)` with `DEADLINE_EXCEEDED` like one-shot. | |
| RM10 | `:2963-3028`, `:3094-3143` | Over-limit arm: immediate SIGKILL, `FAILED_PRECONDITION`; OOM classification from the last sample (within 10%). | C | Stays on the worker. | |
| RM11 | `:3201-3214`, `:3675-3681`, `:4858-4878`, `:5070-5097` kill | Scheduler kill: `kill_token`, SIGTERM+grace during execution, `ABORTED`; ends a fetch or upload in progress. No ack is sent: the scheduler learns of it from the `ABORTED` result after output read, upload and cleanup. | C | Revoke ack once reaped (§4.4). | dbd6557830. |
| RM12 | `:4299-4387`, `:4569-4574`, `:4944-4951`, `:5000-5027` `KillTombstones` | A kill for an attempt not yet registered is held `kill_tombstone_ttl_s` (60 s; max 4096), expiry on `SystemTime`; checked twice at registration → `ABORTED` → `InternalError` (legacy attempt 0 is charged). | C·W | GC on a monotonic clock. Correctness should come from apply rejecting a stale `Accepted`/`Finished` and answering `Kill(op, n)`; TTL ≥ offer deadline. | dbd6557830. TTL is independent of `dispatch_ack_timeout_s` (DESIGN §5.2 says equal). A clock step decides whether a pre-killed action runs. |
| RM13 | `:3692-3732` `prepare_action` | `max_download_timeout_s` (10 min) around input fetch → `DEADLINE_EXCEEDED` → `InternalError` → attempt charged, re-run. Minute stall warnings. | C | Local I/O bound; report `Finished(attempt, err)`; the attempt counts because it was `Accepted`, not because a timer fired. | 82240c6409, 94e7e3f134. |
| RM14 | `:3766-3809` `upload_results` | `max_upload_timeout_s` (10 min) throws away a **finished** execution → `InternalError` → re-run: the action's side effects happen twice. | C | Report `Finished(attempt, err)`; never re-run an executed action silently (§4.3). | 24cc324b21. |
| RM15 | `:4095-4111` `should_cache_result` | `SuccessOnly` (default) caches when `exit_code == 0`, ignoring `error`: a timed-out or killed action that exits 0 on SIGTERM within the grace is cached as a success. | C | Never cache a result with `error` set. | By reading; since 9a95e29153 introduced the grace. |
| RM16 | `:1804-1806`, `:1833-1846` `do_cleanup` | Retry `remove_dir_all` every 100 ms until `max_cleanup_wait_s` (30 s); failure is merged into the result (LW11): success → `InternalError` → re-run. `take_cleanup_mark` waits unbounded. | C·L | Cleanup after `Finished(attempt)` is sent; never changes the result. | 82240c6409, 9a03932c0f. |
| RM17 | `:4579-4690` `wait_for_cleanup_if_needed` | A previous directory for the same op id: backoff 10 ms → `max_cleanup_backoff_ms` (500 ms); past `max_cleanup_wait_s` → `DEADLINE_EXCEEDED`; still running → `ALREADY_EXISTS`. Both fail registration **after** the ack → charged. One stale-dir retry after 100 ms (`:4642-4655`). | C·L | Deleted if action directories are keyed by `(op, attempt)` (DESIGN §5.2); otherwise `Declined(attempt)`. | 47602d1d83 (#1868), f25f68d30e. |
| RM18 | `:2199-2212` disk guard | `statvfs` free space minus admitted reservations → `RESOURCE_EXHAUSTED` refusal (after the ack) → uncharged requeue. | L | `Declined(attempt)`. | |
| RM19 | `:4767-4833` `sweep_orphaned_directories` | `SystemTime::now()` (bypasses `now_fn`); removes an unowned action dir once `now − mtime ≥ max_cleanup_wait_s`. A forward clock jump can delete a live action's directory in the window between creation (`:4959`) and registration (`:5011`). | N·W | Register before creating the directory; then no age gate is needed. | f10c7b70c1. Opt-in (interval 0 = off). |
| RM20 | `:5100-5133` `kill_all` | Kills in `FuturesUnordered` order and waits unbounded for `running_actions` to drain; a stuck cleanup blocks reconnect. | L | Kept. | |
| RM21 | `:1014-1025`, `:1382-1396`, `:1452-1533` | Local idempotent fallbacks: exec-file resolve retry, directory-cache failure → plain download, per-digest upload `OnceCell` retry. | L | Kept. | No `Retrier` in this file. |
| RM22 | `:2216`, `:2297`, `:2418`, `:2425`, `:2650`, `:3197`, `:3236`, `:3574`, `:3584`, `:4719`, `:4934-4937` | `ExecutionMetadata` timestamps from `now_fn` (`queued_timestamp` from the scheduler's `StartExecute`). Reported, never decided on. | N | Unchanged. | |
| RM23 | `:1467-1506`, `:2539`, `:2628-2631`, `:2925`, `:3166`, `:3229-3603` | `std::time::Instant` / tokio `Instant` timings for logs and `wall_time_ms`. | N | — | |
| RM24 | `:774-807`, `:3620-3660` | Lease release and `Drop` cleanup in background tasks. | N | Kept (GC). | df7de0dca2. |
| RM25 | `:3894-3906`, `:4558-4566` `Callbacks` | `now_fn` / `sleep_fn` injection exists, but only RM5 uses `sleep_fn`; every other timer calls tokio directly. | W | Route every worker timer through one injected clock (§5). | |

### 2.18 Other `nativelink-worker` files

| ID | Location | What fires and what it decides | Class | Fenced replacement | Bugs, evidence |
|---|---|---|---|---|---|
| PW1 | `persistent_worker/live_worker.rs:213-220` `dispatch_with_timeout` | Writing the request to the tool's stdin is outside the timeout: a tool that stops reading hangs the action, and RM9's select has only dispatch and kill arms. `Action.timeout` is not enforced. | C | Bounded by the worker's `Action.timeout`. | |
| PW2 | `live_worker.rs:222-235` | `tokio::time::timeout(remaining, read)` → SIGKILL the **pid** (not the group, despite `process_group(0)` at `:106`) → `Err(DEADLINE_EXCEEDED)` (RM9). | C | As RM9; kill the group. | |
| PW3 | `live_worker.rs:66`, `:160`, `:249`; `:256-272` | `last_used` (std `Instant`) for the idle sweep; shutdown: close stdin, wait `shutdown_grace`, SIGKILL pid. | L | Kept (local). | |
| PW4 | `persistent_worker/pool.rs:44`, `:130`, `:437-449`, `:463-483` | Idle sweeper every `max(idle_timeout/4, 1 s)` shuts down processes idle `idle_timeout_s` (300 s; std `Instant`). Decides whether attempt n+1 reuses attempt n's warm process. | N | Kept local (§4.10). | 035dd63165 (#2830). `checked_sub` underflow within `idle_timeout` of boot evicts every idle process each sweep. |
| PW5 | `pool.rs:51`, `:138`, `:363`, `:398-405` `acquire` | `acquire_timeout_s` (30 s) → `RESOURCE_EXHAUSTED` → RM8 fallback (second full timeout). | C | As RM8. | |
| PW6 | `pool.rs:48`, `:298-309`, `:364-406` | Recycle after `max_requests_per_worker` (count); shutdown awaited inline (up to `shutdown_grace`); acquire loop with `notify_waiters` herd. | L | Kept. | |
| PW7 | `live_worker.rs:49`, `:194-197` | `DEFAULT_DISPATCH_TIMEOUT` (10 min): tests only. | N | Delete. | |
| BC1 | `buck2_file_capture.rs:61-66`, `:101-138` | `Uuid::new_v4` session; 30 s start timeout → the action runs without capture (after the ack). | L | Kept. | b00fb48ae5 (#2791). |
| BC2 | `buck2_file_capture.rs:144-166` | Wait `finalize + 10 s`, then kill; result unchanged, but cleanup (and slot release) is delayed up to ~130 s. | L | Kept. | |
| CP1 | `capacity.rs:186-191`, `:233-290`, `:390-402` | At each (re)registration: cgroup and `/proc` reads set the advertised CPU/memory properties and `admits_when_idle`. | D | Session attributes in `OpenSession`. | d86ff4f38c (#2820), b4a5468248 (#2845), 7d3d20ca83 (#2876). |
| CP2 | `capacity.rs:353-382`, `:411-422` `free_memory_kb` | Non-atomic `memory.current`/`memory.stat` reads (can over-report); sampled on every keepalive (LW7) and at admission (LW8); drives the scheduler's live veto and pause lifting (AW3, AW7). | D | Placement input; record it in the `Offer` if offers must replay. | db375f0053 (#2873), 4c06bb81ec (#2796). |
| RP1 | `reaper.rs:34`, `:41-97`, `:146-167` | Zombie reaped only if it was a zombie at the previous 5 s sweep and is not owned; process-global registry and subreaper. | N | Kept (local GC). | 4fe0a27ba9 (#2823). |
| WU1 | `worker_utils.rs:62-71`, `:111` | `QueryCmd` property commands run with **no timeout** on every (re)registration; a failure fails registration (reconnect loop retries). | L | Kept; should be bounded. | 8f2c8847d2 (#2780). |
| AC1 | `worker_api_client_wrapper.rs:75-91`, `:109`, `:131-133` | Every upstream message (keepalives, acks, results) goes through one capacity-1 channel with no send timeout: keepalives queue behind results, and a stalled stream silently stalls the keepalive loop until the scheduler's `worker_timeout_s` fires. | D | Detector input; a separate liveness path (HTTP/2 PING or a dedicated stream) so data-plane stalls do not read as death (§4.11). | e9250ee832 (#1977), a21d82c8bd (#2808). |
| DC1 | `directory_cache.rs:21`, `:74-78`, `:149`, `:650`, `:769-783`, `:1296-1311` | LRU by wall-clock `last_access`; victim = `HashMap` scan `min_by_key` (ties by `RandomState`). | N·W | Kept (local GC; a logical access counter would make it deterministic). | |
| DC2 | `directory_cache.rs:70`, `:375-399` | Summary log at most once per 60 s of wall time. | N | — | |
| DC3 | `directory_cache.rs:333-360`, `:1240-1288`, `:1322-1360`, `:638-641`, `:669-670` | Startup sweep; size/count eviction with background deletes; pinned entries are never evicted under a running action. | N | Kept. | a00bf8c0e1 (#2347), bff444d06b (#2366). |
| DC4 | `directory_cache.rs:484-499`, `:517-546`, `:593-598`, `:821-891`, `:1038-1056` | Single-flight construction retried by each waiter; hardlink failure → rebuild; GetTree fallback; CAS `NotFound` → refetch. | L | Kept. | d48a7f0695 (#2750), 2cbf21ef52 (#2546), df7de0dca2 (#2675). |
| DC5 | `directory_cache.rs:195-201`, `:619-625`, `:642-665`, `:687-705` | An outer timeout (RM13) or kill cancelling a construction mid-way leaves size accounting wrong, a leaked lock entry or an untracked tree until restart. | N | Kept (local); a timeout with lasting side effects. | |
| DC6 | `directory_cache.rs:364-369`, `:965-968` | Fetch semaphore with no timeout; `buffer_unordered(64)` decides which error surfaces. | L | Kept. | |

No sites: `lib.rs`, `qos.rs`, `namespace_utils.rs` (EINTR restarts only), `persistent_worker/mod.rs`, `persistent_worker/protocol.rs` (`WorkRequest.cancel` is never set).

## 3. Correctness-deciding sites, in dependency order

Every C row and every W-flagged row of §2, grouped into steps. A step can start once the steps it names are done. Each step names its replacement and at least one test that fails on this branch and passes after the change. "Sim" means the stream-E turmoil harness; everything else can be written as a `nativelink-scheduler/tests` integration test with the existing `make_scheduler`/`MockInstantWrapped` helpers, or as a rechaos scenario where noted.

### Step 0. One clock, log time, and no wall clock inside replicated records

Nothing else can be tested deterministically until this lands, and the later steps' "measured in log time" depends on it.

| Sites | Replacement |
|---|---|
| SS10, DF2, SM8, SS6, SS5 | One injected `Clock` (monotonic `now`, `sleep`) used by every timer; decisions that replicate read `log_now` from applied state, never the local clock. |
| SM23, AA3, RA12, RA13, MD7 | Timestamps inside `AwaitedAction` are set in apply from `log_now` (or are informational and feed no decision). Operation ids come from the log index or a seeded generator. |
| EX2, AA4 | `insert_timestamp` stops ordering the queue: order is priority, then `AddAction` log index (Step 9). |
| UX1 | `elapsed` on a monotonic clock; no `unwrap` on `SystemTime` differences. |
| RM25, RM2, RM12, RM19, DC1, K31, K35 | Worker timers through `Callbacks` (already half there); kill tombstones and orphan ages on a monotonic clock. |

Tests:

- **T0a (fails today):** build a memory-backed `SimpleScheduler` (`new_with_callback` plus `memory_awaited_action_db_factory`) whose `now_fn` returns `SystemTime::now() + 1 h` (a forward-skewed injected clock), add an action, and wait on its subscriber's `changed()` for 11 s (paused tokio time) with no state change. The first loop sets `last_keep_alive` from the skewed clock; the second computes `last_keep_alive.elapsed()`, which is `SystemTime::elapsed` against the real clock, returns `Err`, and is unwrapped (UX1): the client's stream task panics. After: no panic, and no decision moves.
- **T0b (sim, fails today):** run the same seeded scenario twice and compare the raft log byte for byte, and the canonical state (`canonical_json`) at each index. Today `OperationId::default()` (UUIDv4), `Uuid::now_v6` worker ids with a random node id (WA1), wall-clock timestamps inside records (AA3), and proposer batch boundaries (RA10) differ between runs.

### Step 1. Sessions are log facts

Depends on Step 0 (epochs and ids from the log).

| Sites | Replacement |
|---|---|
| AW4, WA3 (D), AW22 (D) | `OpenSession { worker, epoch }` committed before the session is offered anything; a second session for the same worker id supersedes the first in apply (raft `WorkerJoin`, `state.rs:856-890`, is the template). |
| AW24, WA5, WA7, K33 | `GoingAway` → `CloseSession { worker, epoch }` (drain is a session attribute). A stream that ends without `GoingAway` is a detector input (Step 2), not an eviction. |
| AW25, RA7 | A stopping or deposed leader drops streams and requeues nothing. A new leader proposes `LeaderChanged` after its blank entry (wave 1, `ReplicaRestarted` semantics); wave 2 replaces the blanket requeue with session resume. |

Tests:

- **T1a (fails today):** register worker `w`, dispatch op `o` to it, then register a second worker under the same id (connect with a fixed `worker_id_prefix` and a test hook that pins the UUID). Today `LruCache::put` drops the first incarnation's `running_action_infos` (AW4): `o` stays `Executing` with no holder until SM11 requeues it an hour later (or never, without peers). After: `o` is requeued in the same apply that opens the new session, and a result from the old epoch is rejected.
- **T1b (sim):** `leader-kill` during execution in a 3-node cluster: after the new leader is ready, every op that was `Executing` is either resumed or requeued exactly once, and the deposed leader proposes nothing (AW25 today would requeue every op on shutdown).

### Step 2. One failure detector; its verdict is a committed `ExpireSession`

Depends on Step 1.

| Sites | Replacement |
|---|---|
| AW26, WR1, WR2, WR3, AW6, SM10 | Leader-local detector: liveness stamped **at receipt** on a monotonic clock (WA4, AW2, AW20 today stamp at processing). Silence past `worker_timeout_s` *proposes* `ExpireSession { worker, epoch }`; until it commits, nothing is requeued and the session's messages are accepted. A suspected session is not offered new work (AW6), which is local and safe. `Unknown` liveness and the 1 h orphan ceiling disappear (single owner). |
| AW27, SW1, K5 | The revoke-ack deadline is a detector input too, counted from the revoke's send on the leader's clock; it proposes `ExpireSession`. |
| AW13, AW17, AW18, WA5 | A failed send or a closed stream is a detector input. `ExpireSession` apply requeues the session's attempts in sorted order (`state.rs:407-432`). |
| SM12 | Deleted: a second, client-poll-driven path to the same verdict. |
| LW2 | Worker side, wave 2: a worker that loses the leader keeps running, reconnects and resumes `(worker, epoch, attempts)`; it kills only when told its epoch expired. |

Tests:

- **T2a (fails today, by reading):** three workers send keepalives every 2.5 s; wrap the state manager's db so one `update_awaited_action` (a worker's result) takes 15 s. `update_action_for_attempt` holds the global `ApiWorkerScheduler` lock across that write (`api_worker_scheduler.rs:2021-2045` → `:1129-1133`), so every keepalive's `refresh_worker` and its registry stamp (AW20, WA4) wait behind it; the first eviction sweep to get the lock sees all three stamps ~15 s old and evicts every worker, requeueing their work and charging `worker_losses`. After: liveness is stamped at receipt, outside any lock, and nothing is evicted.
- **T2b (detector quality, fails today):** a test hook parks `remove_timedout_workers` between its unlocked registry check and the eviction (`api_worker_scheduler.rs:2205-2226`); a keepalive arrives in the window. Today the worker is evicted with a fresh keepalive on record (branch `fix/eviction-rechecks-heartbeat`). After: a proposal is formed from receipt stamps read under one lock, so a keepalive received before the proposal prevents it; one received after the commit is rejected for its expired epoch.
- **T2c (fails today):** worker silent for 9 s, then a kill is sent; the worker acks at send + 55 s with `unacknowledged_kill_timeout_s` = 60. Today `kill_requested_at` is the worker's last-seen (send − 9 s), so 64 s > 60 and the worker is evicted (SW1). After: not evicted.
- **T2d (sim, determinism, fails today):** evict a worker holding 50 ops on the raft backend in two runs with one seed; compare the order of the requeue entries in the log. Today it is `HashMap` drain order (AW18). After: sorted, identical.
- **T2e (pass condition, passes today, must keep passing):** rechaos absurd-timeouts mode: `SIGSTOP` a worker for 30 s mid-action, `SIGCONT`. With `worker_timeout_s` = 1 day nothing is requeued and the op completes once.

### Step 3. Results are fenced in apply

Depends on Step 1 (epochs) and the attempt ids already on the wire (dbd6557830).

| Sites | Replacement |
|---|---|
| AW9, AW10, AW23, AW14, WA9 | Every worker message is a command carrying `(op, attempt, epoch)`. Apply accepts it only if the attempt is current and the epoch live; otherwise it rejects (`StaleAttempt` / `StaleSession`) and emits `Kill(op, n)`. The `kill_requested_at` rule that drops results (AW10) goes. |
| SM14, SM16, SM4, SM5 | No retry-on-conflict: a decision computed from a read is a CAS on the attempt it read; a conflict means the decision is void. |
| SM22, AW15 | `Declined(attempt)` command (uncharged requeue). |

Tests:

- **T3a (fails today, by reading):** a legacy worker (echoes attempt 0) runs op `o` as attempt 1 with a client waiting. Silence the worker for 11 s so SM12 requeues `o` (and the revoke hook sends a kill), then resume its keepalives so the matcher may hand `o` back to the same worker as attempt 2 (same-worker redispatch is allowed: the N2 guard, 337d589f9d, is not merged here; `run_action` overwrites the attempt-1 entry). The worker's next message about attempt 1 (the `ABORTED` from the kill, or its result) carries attempt 0, which `attempt_match` treats as `Current` (`worker.rs:363`), so it is applied to attempt 2: requeued and charged, or completed with attempt 1's result. DESIGN §5.1 calls this safe "because apply forbids a same-session redispatch", which nothing enforces today. After: the leader fills in the attempt per session and apply rejects a same-session redispatch, or legacy workers are refused.
- **T3b (fails today, by reading):** wrap the memory db so that `timeout_operation_id`'s `UpdateWithError` (no worker id) sees one `ABORTED`, during which the op is requeued and dispatched to another worker. Today the retry applies the error to the new attempt (requeue, `attempts += 1`) (SM14). After: the timeout decision is void, the new attempt keeps running.

### Step 4. Revoke barrier

Depends on Steps 2 and 3, and on the worker acking a revoke only after the process group is reaped (RM4, RM11).

| Sites | Replacement |
|---|---|
| SS13, AW28, K21 | `Revoke(op, attempt)` is an obligation from applied state, sent at once by the dispatcher and resent until discharged by a log fact (`Finished(n)`, the revoke ack, or `ExpireSession`). The 5 s sweep is deleted. |
| AW16, K6 | Offer deadline (on the leader's clock) → `RevokeOffer { op, attempt }`; the op returns to `Queued` only on the worker's ack of the revoke or the session's expiry. |
| AW12 | A full channel leaves the op `Offered`; the dispatcher resends (at-least-once; LW9 re-acks duplicates). |
| RM11, RM4, K36 | Worker sends a revoke ack after reaping the attempt's process group (after the `kill_grace_ms` SIGTERM window). |
| K13 | `report_executing_on_accept` becomes unconditional: clients see `EXECUTING` only from `Accepted` (offer barrier). |

Tests:

- **T4a (fails today):** `dispatch_ack_timeout_s` = 1 and a worker channel that holds the `StartExecute` for 3 s before delivering it, while the worker keeps sending other messages (so the unacked sweep runs). Today the op is requeued and dispatched to worker B, then A receives and runs it too until its late ack draws a kill (AW16, AW14); with a legacy worker (attempt 0, no acks) both run to completion. A history checker asserting "never two live attempts of one op" fails. After: B is offered the op only after A acks the revoke.
- **T4b (worker):** kill an action that traps SIGTERM and forks a child; assert the revoke ack is sent only after every process in the group has exited.

### Step 5. Delete the scheduler-side action timeouts

Depends on Step 2 (lost workers are handled by the detector) and on the worker bounding every execution path (the worker sub-steps below).

| Sites | Replacement |
|---|---|
| SM9, SM11, WR1, K4 | Deleted. A hung action on a live worker is bounded by `Action.timeout` on the worker (RM5, RM6, K26). |
| SM4, SM5, SS12, K3 | Deleted: the client-stream poll stops deciding anything; `worker_timeout_s` keeps one meaning. |
| EX1 | The scheduler stops reading `Action.timeout`. |
| RM3 | Worker sub-step: bound the output drain after a clean exit by `Action.timeout`. |
| RM8, RM9, PW1, PW2, PW5, K37 | Worker sub-step: the persistent path falls back with the *remaining* time, bounds the request write, kills the process group, and reports `DEADLINE_EXCEEDED` as a completed result, like one-shot. |
| RM5, RM6, RM7, K26, K32 | Unchanged: these are the bound. |

Tests:

- **T5a (fails today):** `Action.timeout` = 30 s, input fetch delayed 60 s by a slow CAS (test store with a delay), command sleeps 40 s, client waiting on its `Execute` stream (SM3 needs a watcher). Today the scheduler fires `ActionTimeout` at assignment + 40 s (30 + 10 s grace, SM9), requeues with `attempts += 1`, and after `max_job_retries` completes it `FAILED_PRECONDITION`. After: the worker reports a completed `DEADLINE_EXCEEDED` once, 30 s after the command started, and nothing is retried.
- **T5b (fails today, Redis or peers):** an action running 61 min (mock clock) on a healthy worker connected to another scheduler instance: SM11's `Unknown` branch requeues it. After: not requeued.
- **T5c (worker, fails today):** persistent action with `Action.timeout` = 10 s and `persistent_workers.max_workers_per_key` = 1 already leased: today the acquire waits 10 s and falls back to one-shot with a fresh 10 s (RM8), total ~20 s, and can succeed. After: `DEADLINE_EXCEEDED` at ~10 s.
- **T5d (worker, fails today):** an action that starts `setsid sleep 1000 &` (inheriting stdout) and exits 0, `Action.timeout` = 5 s, no PID namespace. Today `inner_execute` waits on the drain forever (RM3). After: completes within 5 s + grace + drain cap.

### Step 6. One retry budget, decided in apply

Depends on Step 3 (attempts that reached `Accepted` are log facts).

| Sites | Replacement |
|---|---|
| SM19, SM20, SM21, RA15, DF4, K9, K10, K11 | Apply is the only decider. Failure budget: attempts that reached `Accepted` and ended in an error `Finished`. Loss budget: `Accepted` attempts ended by `ExpireSession`. Escalation budget: `Finished` with a memory-kill outcome. One default per budget, in one place. |
| LW6, K34, RM17, K29, K30 | Admission that can refuse (precondition script, previous directory still being cleaned, RM18 disk guard) runs **before** `Accepted` and answers `Declined(attempt)`, so FENCED's rule does not start charging backpressure (§4.3). |
| RM13, RM14, RM16, LW11, K27, K28 | Worker-side I/O timeouts end the attempt with `Finished(attempt, err)`; cleanup happens after the result and never changes it. Whether a failed upload of an executed action may be re-run is a policy decision to state, not a side effect of a timer (§4.3). |

Tests:

- **T6a (fails today, by reading):** build the raft backend through `default_scheduler_factory::raft_db_config` with `max_job_retries` unset; the first attempt returns `InternalError`. Today apply rejects the requeue (`RetryCapExceeded`, limit 0), the state manager retries it as a conflict five times, gives up, and the op is left `Executing` with no worker holding it. After: requeued and retried up to the default.
- **T6b (fails today):** an action whose cleanup fails (make the action directory undeletable for 31 s) after a successful run. Today the success becomes `InternalError` and the action runs again (RM16, LW11). After: the success commits; the cleanup failure is logged.
- **T6c:** with every scheduler timeout at 1 day and no fault injected, no attempt counter ever moves.

### Step 7. Client interest is a watcher count

Depends on Step 0. Independent of Steps 1–6.

| Sites | Replacement |
|---|---|
| SM6, SM13, EX5, K2 | Deleted: no keepalive comparison anywhere, and reads (`WaitExecution`, `GetOperation`, admin listing, matcher listing) have no side effects (FENCED read barrier). |
| MD5, SA1, SA2 (Redis) | Deleted on the fenced backend: `ClientAttach`/`ClientDetach` on the leader, refcount per client op id. Redis keeps its keepalives (out of scope). |

Tests:

- **T7a (fails today):** a client whose `Execute` stream is not polled for 90 s (a reader that stops reading; the server's `unfold` is pull-based, EX3, so no keepalive goes out) on an op still queued, then any `WaitExecution` for it. Today `apply_filter_predicate` completes it `DEADLINE_EXCEEDED` during that read (SM13), if the abandoned-queue sweep (SM6) has not already. After: the watcher is attached; nothing changes.
- **T7b (fails today):** an op whose client keepalive is older than `client_action_timeout_s`; call the admin `queued_demand`. Today the read completes the op. After: the read changes nothing.

### Step 8. Retention collects terminal operations only

Depends on Steps 4 and 7.

| Sites | Replacement |
|---|---|
| MD4, MD8, UX3, RA1, RA4, RA5, DF1, K1 | `Collect { op }` for a terminal op once no watcher has been attached for the retention, from the log time of the last `ClientDetach`. A non-terminal op with no watcher for the retention gets `RequestCancel`, goes through the revoke barrier, and is collected only once terminal (§4.7). |
| SA5, SA10 (Redis) | Out of scope; under fenced, client mappings live as long as their op. |

Tests:

- **T8a (fails today):** client calls `Execute` and disconnects; `retain_completed_for_s` = 5; the action runs 30 s; any other `Execute` arrives at 6 s (eviction is lazy). Today, on memory and raft, the op is removed while executing (MD4/RA4), the worker's result is dropped ("probably dropped"), and the kill sweep kills the attempt. After: the op is either kept until its result (then collected 5 s later) or cancelled through the revoke barrier; never removed while an attempt is live.

### Step 9. Policy timeouts and queue order on log time

Depends on Step 0 (log time) and Step 1 (the fleet is the set of open sessions).

| Sites | Replacement |
|---|---|
| SS3, SS7, UT2, UT3, K7, K8 | Leader policy timer proposes `FailQueued { op, expected_revision, reason }`; apply checks `Queued` at that revision. Shape age and action age in log time; action age from its `AddAction`. |
| SS16, AW1, AW5, AW8, AW19, SA13 | Deleted: no peer census under one leader. |
| AA4, EX2 | Queue order: priority, then `AddAction` log index. |

Tests:

- **T9a (fails today):** shape becomes due; a second action of the same shape arrives with `ActionInfo.insert_timestamp` set `2 × timeout` in the past (as an execution server with a slow clock would stamp it). Today it is failed on sight (`action_eligible` compares the scheduler's `now` with that stamp, UT2), though it has waited nothing. After: it waits its own timeout from its `AddAction`.
- **T9b (fails today):** two same-priority actions; the later one carries an earlier `insert_timestamp` (another node's clock). Today it sorts first (AA4). After: commit order wins.
- **T9c:** with `unsatisfiable_action_timeout_s` = 1 day nothing is ever failed.

### Step 10. Outside the fence: proxy, Redis, the Action Cache, worker-local outcomes

These do not block Steps 0–9 but must be decided, because they change client-visible results.

| Sites | Replacement |
|---|---|
| LW12 | The worker writes the AC before reporting, so an expired or superseded attempt still publishes. Either the leader writes the AC entry on commit, or the AC is declared last-writer-wins outside the fence (§4.9). |
| RM15 | Never cache a result whose `error` is set. Test **T10a (fails today, by reading):** an action that traps SIGTERM and exits 0, `Action.timeout` = 1 s, sleeping 10 s: today the default `SuccessOnly` strategy caches it as a success. After: not cached. |
| GS1, GS2, GS3, K17 | Proxy: retry `Execute` only when the upstream provably did not receive it; resume a broken stream with `WaitExecution(name)`. Test **T10b:** upstream accepts `Execute`, then the response stream is reset before the first message; today the retry creates a second upstream operation (for an uncacheable action, a second execution). |
| SA8 (and SA1, SA2, SA5, SA10, SA13 above) | Redis backend: unchanged by DESIGN §1. Under fenced none of these exist. SA8 is the worst: a cacheable action is recreated (second execution) when its record looks abandoned by this node's clock. |
| RM1, RM10, RM2, K39 | Worker-local enforcement: stays on the worker; outcomes travel in `Finished(attempt)`. |

## 4. Things FENCED.md gets wrong or misses

### 4.1 The executing-action timeouts are not where FENCED deletes them

FENCED deletes `max_action_executing_timeout_s` and "the periodic sweep". Neither is where executing actions are timed out today. `timeout_cause` (`simple_scheduler_state_manager.rs:803-882`) has four branches: `Action.timeout` plus a `worker_timeout_s` grace (SM9), the stuck-but-alive ceiling (SM11, the deleted knob), a 1 h `ORPHANED_ACTION_TIMEOUT` for workers this process does not know (SM11/WR1), and the `Stale` worker requeue (SM12). Its only caller loop is a **client's** `changed()` poll (SM3): no sweep drives it. The plan must delete all four branches and the poll, not one knob. SM9 in particular changes what the client sees (a retried and eventually `FAILED_PRECONDITION` action instead of a completed `DEADLINE_EXCEEDED`, T5a) and is not named anywhere in FENCED.md.

### 4.2 "Bounded by `Action.timeout`, enforced by the worker" is not true on every path

FENCED makes the worker the only bound on a hung action. Today the worker's bound has holes:

- **Persistent workers** (on by default for `supports-workers=1` actions): a pool-acquire timeout falls back to one-shot with a fresh full timeout, so ~2× `Action.timeout` (RM8, PW5); writing the request is outside any timeout (PW1); the timeout kills the pid, not the group (PW2), and is reported as `InternalError`, so the scheduler re-runs a deterministic hang (RM9).
- **Output drain** after a clean exit with a `setsid` descendant holding the pipe: unbounded, timer not polled (RM3).
- **Unbounded waits** that hold the slot: `hard_kill` on an unkillable process (RM4), `take_cleanup_mark` (RM16), `kill_all` (RM20), `QueryCmd` at registration (WU1).
- The bound is `min(Action.timeout, max_action_timeout_s)` with a 20 min default clamp (RM6), and `timeout_handled_externally` trusts a wrapper (RM7).

Deleting the scheduler-side backstop is right, but only after these are fixed (§3 Step 5). DESIGN.md §6 still proposes an `exec_deadline` on `Tick`, which contradicts FENCED; if anything like it stays it can only be a detector input, and it must exceed `Action.timeout + kill_grace + 5 s drain + max_upload_timeout_s + cleanup`.

### 4.3 "The retry budget counts attempts that reached `Accepted`" changes more than FENCED says

- There are three budgets, not one: `max_job_retries`, `max_worker_loss_retries`, `memory_escalation.max_steps` (SM19–SM21), plus a fourth copy of the first inside raft apply with a different default (RA15, DF4: `retry_limit` = 0 when the knob is unset, a live bug by reading).
- Several worker refusals happen **after** `ExecuteAccepted` and are deliberately uncharged today because they are `RESOURCE_EXHAUSTED`: the precondition script and its timeout (LW6), the disk guard (RM18). Under FENCED's rule they would start spending budget. Others after the ack are charged today: cleanup-wait timeout and `ALREADY_EXISTS` (RM17), a kill tombstone (RM12), download timeout (RM13). FENCED must say which post-`Accepted` endings are charged, or move admission before `Accepted`.
- Two worker timers re-run work that already executed: the upload timeout discards a finished execution (RM14), and a cleanup failure turns a success into an error (RM16, LW11). Both re-run an action whose side effects already happened. That is a retry with side effects caused by a timer, on the worker.

### 4.4 The revoke barrier assumes an ack that does not exist, and "acked" does not mean "dead"

"Requeued only after the holding worker acks the revoke." No revoke ack exists. The scheduler learns a kill took effect from the attempt's `ABORTED` result, which arrives after the 5 s drain, the upload (up to 10 min) and cleanup (RM11). The ack has to be defined as "every process of the attempt has been reaped", and that is not achievable everywhere today: the persistent-worker kill is pid-only (PW2, PW3), and without PID namespaces a `setsid` descendant survives any kill (RM3). Until then the barrier's guarantee is "the worker says so", not "nothing of attempt n still runs".

### 4.5 Two detector inputs are missing from FENCED: the kill-ack and dispatch-ack deadlines

`unacknowledged_kill_timeout_s` (AW27) and `dispatch_ack_timeout_s` (AW16) both decide directly today (evict a worker; requeue a dispatch untried while the `StartExecute` may still be in flight). FENCED lists only `worker_timeout_s` as the detector's input. Both must become detector inputs that propose (`ExpireSession`, `RevokeOffer`), measured on the leader's clock from the send. Today the kill deadline is measured from the worker's last message *before* the kill and fires early (SW1, comment says the opposite). The worker's kill-tombstone TTL (RM12, 60 s, wall clock) is supposed to equal the dispatch-ack deadline (DESIGN §5.2); the two knobs are independent and the second is off by default.

### 4.6 Policy timeouts are neither detector nor GC

`unsatisfiable_action_timeout_s` and `no_worker_action_timeout_s` (SS3, SS7) fail client actions on a timer. They are not a failure detector and not retention, so they fall outside FENCED's list of "what remains time-based". They also compare this node's clock with `insert_timestamp`, a wall-clock value from whichever execution server received the action (UT2). They need a stated class: a leader policy timer whose verdict is a logged `FailQueued` CAS, measured in log time, and whose early firing (a fast-clock leader moving `log_now` forward) is accepted as a client-visible effect.

### 4.7 Retention, as written, collects running operations

"An operation is collected only when nobody watches it and its retention has passed since the last watcher left." Memory (MD4) and raft (RA4) do exactly that today, **in any stage**: a client that disconnects from an executing action and does not come back within `retain_completed_for_s` (60 s) loses the operation; the worker keeps running, its result is dropped as "probably dropped", and the kill sweep kills it. Collecting a non-terminal op is a cancellation, and a cancellation of a live attempt must go through the revoke barrier. FENCED should say: retention collects terminal ops; a non-terminal op with no watcher for the retention gets `RequestCancel`, is revoked, and is collected once terminal. (Whether 60 s is long enough for a Bazel client to come back with `WaitExecution` after a network blip is a separate, client-visible choice.)

### 4.8 Watcher counts die with the leader

The watcher count is leader-local by construction (streams cannot be replicated). A deposed leader "drops every client stream at once", so the new leader sees zero watchers on every op. If retention counts from "the last watcher left", failover starts every op's retention clock at once, and any op whose clock was already running may be collected immediately. The raft backend handles this today by giving every restored client a fresh window (RA8); FENCED should make it the rule: retention restarts at leader-ready for every op. Re-attachment also assumes clients come back with `WaitExecution`, not a new `Execute` (DESIGN open question 3); a re-`Execute` of an uncacheable action is a second execution.

### 4.9 The Action Cache is written outside the fence

The worker writes the `ActionResult` to the Action Cache **before** it reports to the scheduler (LW12, `local_worker.rs:790-798`). An attempt whose session has expired, or that was superseded, still publishes; for a non-deterministic action the AC can then disagree with the one result the log committed. A timed-out action that exits 0 on SIGTERM is cached as a success (RM15). "At most one attempt's result commits per operation" is true of the log and false of the AC. Either the leader writes AC entries on commit (it has the result digest), or FENCED states that the AC is last-writer-wins and outside the fence.

### 4.10 Worker-local state outlives an attempt

Fencing results by `(attempt, epoch)` does not fence side effects that stay on the worker:

- Action directories are keyed by operation id, not attempt (`running_actions_manager.rs:4697`). A retried attempt on the same worker waits on, collides with or reuses the previous attempt's directory (RM17); grandchildren of a killed attempt can still write into it (§4.4).
- Persistent tool processes are reused across attempts, chosen by `idle_timeout_s` (a timer) and LIFO pop (PW4), with no input digests in the work request.

DESIGN §5.2 asks for dedup by `(op, attempt)`; `running_actions` is still keyed by op only.

### 4.11 The detector's input is coupled to the data plane

FENCED's "one failure detector" assumes its input means "the worker is alive". Today it means "the scheduler got around to processing a message from the worker":

- The worker sends keepalives through one capacity-1 channel shared with results, with no send timeout (AC1).
- The scheduler processes each worker's messages in order behind store writes (WA4).
- It stamps liveness after taking the global `ApiWorkerScheduler` lock, which `update_action_for_attempt` holds across the store write (AW20, T2a).

A slow store write can therefore make a whole healthy fleet look dead. FENCED tolerates a wrong detector for safety, but every false expiry costs `worker_losses` and, through `kill_all` on the worker (LW2), all of that worker's running work. The detector must stamp at receipt on a path a result write cannot block (HTTP/2 PING or a dedicated stream).

### 4.12 Wall-clock values travel inside the replicated state

DESIGN §3 says no node ever compares its clock to a timestamp another node wrote. The replicated `AwaitedAction` carries `last_transition_timestamp`, `last_worker_updated_timestamp` and `last_client_keepalive_timestamp` from the proposing node's clock (AA3), and `UpdateAwaitedAction` embeds them unclamped beside the clamped `now_unix_nanos` (RA12). `ActionInfo.insert_timestamp` (EX2) orders the queue (AA4) and ages unsatisfiable actions (UT2). After a failover every remaining reader (SM6, SM9, SM11, SM13, UT2) compares the new leader's clock with the old leader's stamps. Either those readers go (most do under FENCED) or apply rewrites the stamps from `log_now`. Separately, `log_now` only clamps backwards: a leader with a fast clock moves log time forward, and every log-time deadline (retention, `FailQueued`) fires early by the skew.

### 4.13 Reads write today

`apply_filter_predicate` completes operations as a side effect of `WaitExecution`, `GetOperation`, the admin listing and the matcher's queue listing (SM13, EX5, AD1). FENCED's read barrier makes reads fresh; it does not by itself make them side-effect free. The cancel-on-read has to be deleted, not just routed through read-index.

### 4.14 "No timeout decides correctness" holds only for the fenced backend

DESIGN keeps the Redis backend as is: every SA row (cross-node keepalives, 24 h client-mapping TTL, abandonment-driven recreation with a second execution, fleet records decided by Redis TTL) and every cross-node branch of SM6/SM11/SM13 stays. The `grpc` scheduler proxy retries `Execute` with side effects (GS1) and drops a broken stream without resuming (GS2, GS3). FENCED should state its scope.

### 4.15 Ownership truth is in memory, not in the log

In production the raft backend never proposes `WorkerJoin`, `WorkerLeave`, `AssignToWorker`, `Heartbeat` or `Expire`; `sessions`, `operation_owner` and `leases` in `AppliedState` are dead (§2.10). The truth about which worker holds which attempt, which dispatch was acked, which kill was sent, the capacity ledger and pauses live in `ApiWorkerScheduler`'s memory (`running_action_infos`, `kill_requested_at`, `accepted`, `is_paused`). FENCED's dispatcher must recompute obligations from applied state after a leader change, so ownership, offers and acks have to be log facts. Capacity reserved at `Offered` has to be rebuilt from applied state by the new leader. Leases are still written on every apply (RA16) and should be deleted with `Heartbeat`/`Expire`.

### 4.16 The absurd-timeouts pass condition leaves out the worker's timeouts

"Every scheduler timeout in the config set to an absurd value" leaves these at normal values: download and upload (10 min), cleanup wait (30 s), precondition (30 s), kill grace (5 s), tombstone TTL (60 s), persistent acquire (30 s) and idle (300 s), and the worker's HTTP/2 keepalive (20 s). With no fault injected, a slow CAS makes the download timeout end an accepted attempt as a charged `InternalError` and re-run it (RM13). An upload past 10 min re-runs an executed action (RM14). Either the absurd mode sets the worker knobs too, or the pass condition should name them as worker-local policy that is allowed to fail an attempt.

### 4.17 Smaller ones

- The matching task can exit permanently during construction if a notify permit is already stored (SS20, branch `fix/matching-engine-startup`). "Events drive everything" needs the dispatcher driven from applied state, not from one-shot notifications.
- `max_job_retries: 0` means 3 (SS11), so "never retry" cannot be configured. The raft apply cap reads the raw 0 (DF4).
- Execution server and `ActionInfo` conversion disagree on a missing `Action.timeout` (MAX vs zero, EX1, UX8).
- `InstantWrapper for SystemTime` panics on a backwards step (UX1). FENCED's log time protects replicated decisions, not this.
- Doc drift that a reader of FENCED will trip on:
  - `schedulers.rs:179-183` says `max_action_executing_timeout_s` measures "without any worker update", which is never recorded.
  - `schedulers.rs:201-203` and `worker.rs:103` say a dispatch is dated by the worker's last message; it is dated by the scheduler clock.
  - `worker.rs:95-99` says the kill deadline is "never early"; it is never late.
  - `cas_server.rs:1272-1273` says an over-long `Action.timeout` is rejected; it is clamped.
  - DESIGN §5.2 says a kill for an unregistered action is "logged and dropped" at `running_actions_manager.rs:4903`; tombstones exist now.

## 5. Determinism: what stops a simulator from replaying the scheduler exactly

Turmoil drives tokio's timer and a simulated network. It does not control `std::time::SystemTime`, `std::time::Instant`, OS threads, thread-local RNGs, `HashMap` seeds, the filesystem, `/proc`, signals or external services. Every item below is something stream E has to inject, seed, or replace. Line references are this branch.

### 5.1 Wall-clock reads (`SystemTime::now()`)

Injectable today (through a `now_fn` / `InstantWrapper`), production passes `SystemTime::now`:
- `SimpleScheduler` and `SimpleSchedulerStateManager` (`default_scheduler_factory.rs:68`, `:176`, `:241`; `simple_scheduler.rs:1074`, `:1175-1184`).
- Memory, Redis and raft db constructors (`memory_awaited_action_db.rs:967`, `store_awaited_action_db.rs:863`, `raft_awaited_action_db/mod.rs:466-469`).
- `WorkerApiServer::new_with_now_fn` (`worker_api_server.rs:142`).
- `RunningActionsManagerImpl` `Callbacks.now_fn` (`running_actions_manager.rs:3894-3906`, `:4558-4566`).

**Not** injectable (direct calls in or near decision paths):
- `simple_scheduler_state_manager.rs:99`: the version-conflict backoff "jitter" (SM1).
- `worker_api_server.rs:97`: the eviction sweep's clock (WA2). In tests this is a *different* clock from the `now_fn` that stamps keepalives.
- `execution_server.rs:273`: `insert_timestamp`, which orders the queue and ages actions (EX2).
- `worker.rs:262` (metric), `cache_lookup_scheduler.rs:272`.
- `local_worker.rs:132`: reconnect jitter (LW3).
- `running_actions_manager.rs:373`, `:2846`, `:4773` (RM2, RM19); `directory_cache.rs:75` (DC1).
- `nativelink-util/src/instant_wrapper.rs:47-49`: `SystemTime::elapsed()` reads the real clock even when the instant came from an injected `now_fn` (UX1, T0a).

### 5.2 `std::time::Instant` (ignores tokio's paused clock)

- Scheduler: `simple_scheduler.rs:449`, `:839`, `:1345`, `:1390` (metrics, logging cadence that also bypasses the static-verdict cache); `api_worker_scheduler.rs:1797`; `historical_resource_scheduler.rs:275` (hint reload changes later reservations).
- Worker: `running_actions_manager.rs:1467-1506`, `:2925`, `:3229-3603` (timings); `persistent_worker/live_worker.rs:160`, `:249` and `pool.rs:464-466` (idle eviction decisions). `pool.rs` mixes clocks: tokio `sleep` at `:442`, std `Instant` comparison at `:464`.

### 5.3 tokio timers: fine under turmoil, but note

- Matching loop, abandoned sweep, fleet exchange, worker-API tickers, Redis pull loop and active count, version-conflict sleeps, `try_subscribe`'s 20 ms, raft start-up polling (SS9, SS14-SS19, SA4-SA9, RA9), worker keepalive interval, timeouts and sleeps (LW, RM, PW, BC rows).
- `worker_api_server.rs:92-93` uses `interval` with the default `MissedTickBehavior::Burst`: after a stall the sweep runs back to back. `local_worker.rs:493` uses `Skip`.
- `InstantWrapper for SystemTime::sleep` is `tokio::time::sleep` (`instant_wrapper.rs:51-53`), so the scheduler's timers *sleep* on tokio time but *compare* on the wall clock.

### 5.4 Mock clocks are a third, incompatible clock

`MockInstantWrapped` (`instant_wrapper.rs:60-96`) reads a thread-local `mock_instant::MockClock`:
- `from_secs` ignores its argument, so code that means "long ago" gets "now" (MD2, `store_awaited_action_db.rs:328`, `:436`).
- `sleep` busy-yields until `MockClock` advances. Under tokio paused time or turmoil, nothing advances it, and the task spins forever.
- The `MockClock` is thread-local, so it is shared by every simulated host on the one turmoil thread, and it is unrelated to tokio's clock.

Tests today combine three clocks: `MockClock` for the state manager and dbs, tokio time for sleeps, and the real wall clock for eviction (WA2) and `insert_timestamp` (EX2). A simulator needs one `Clock` trait with `now` and `sleep` both backed by turmoil's time.

### 5.5 Randomness

- **Operation ids:** `OperationId::default()` = `Uuid::new_v4()` (`action_messages.rs:70-71`), minted at `execution_server.rs:466`, `memory_awaited_action_db.rs:806`, `raft_awaited_action_db/mod.rs:1140`, `grpc_scheduler.rs:154`, `cache_lookup_scheduler.rs:269`. Op ids break ties in queue order (`awaited_action_db/mod.rs:89-95`, AA2) and key every map. The Redis db already takes an `operation_id_creator` (`store_awaited_action_db.rs:864`); memory and raft do not.
- **Worker ids:** `Uuid::now_v6(node_id)` with `node_id` from `rand::rng()` (`worker_api_server.rs:75-79`, `:213`). Origin event ids: `Uuid::now_v6` (`simple_scheduler.rs:289`, `api_worker_scheduler.rs:1955`).
- **Other UUIDs:** scheduler id for the fleet exchange `Uuid::new_v4` (`simple_scheduler.rs:1284`); worker side-channel file and Buck2 session `Uuid::new_v4` (`running_actions_manager.rs:2369`, `buck2_file_capture.rs:61`).
- **Retry jitter:** `rand::rng()` (`nativelink-config/src/stores.rs:2049`, GS1/UX6); wall-clock nanos as jitter (SM1, LW3).
- **openraft:** randomizes election timeouts internally with its own RNG; it has to be seeded, or the simulated cluster's elections differ run to run.
- **Unbiased `tokio::select!`:** when several arms are ready, tokio picks one at random. Decision-relevant ones:
  - `simple_scheduler_state_manager.rs:327` (change vs timeout check)
  - `memory_awaited_action_db.rs:204`, `store_awaited_action_db.rs:439`, `raft_awaited_action_db/mod.rs:332` (change vs keepalive)
  - `simple_scheduler.rs:1332`
  - `worker_api_server.rs:95` (eviction tick vs kill tick)
  - `local_worker.rs:572`: the worker's whole run loop. A `StartExecute`, a kill and a shutdown that arrive together are taken in random order.
  - `running_actions_manager.rs:2930`: an action that exits exactly when its timeout fires gets `DEADLINE_EXCEEDED` or not at random.
  - `running_actions_manager.rs:2521`, `:2566`, `:3062`, `:3707`, `:3789`, `:4682`.

  Only `running_actions_manager.rs:1954` is `biased`.

### 5.6 `HashMap` / `HashSet` iteration that reaches a decision or the log

`RandomState` is reseeded per process, so these differ run to run:

- `Worker::running_action_infos` (`worker.rs:137`), and everything that walks it:
  - drained by eviction, so the order of requeue writes (AW18) and of log entries on raft;
  - `unacknowledged()` (`worker.rs:424-430`), the order of dispatch-ack requeues (AW16);
  - the kill sweep's snapshot (AW28), whose checks then complete in `buffer_unordered` order (`api_worker_scheduler.rs:2371`).
- `directory_cache.rs:1299-1303`: LRU victim ties (DC1).
- `running_actions_manager.rs:2349`: `additional_environment` order, which matters for a duplicated `SideChannelFile`; `:5106`, kill order.
- `worker_api_server.rs:86`: one sweep task per scheduler in `HashMap` order.
- Raft snapshots: `to_portable` (`raft_awaited_action_db/state.rs:188-225`, written at `store.rs:606`) collects the `AppliedState` `HashMap`s in iteration order, so two replicas holding the same state write different snapshot bytes. Only `canonical_json` (`state.rs:231`, used by the model test) sorts. DESIGN §7's per-snapshot `hash(PortableState)` is not implemented; when it is, it must hash the canonical form.

Order-insensitive (checked, fine):
- `static_verdict`'s `any` (`api_worker_scheduler.rs:606`).
- The unsatisfiable tracker's `min`/`retain` (`unsatisfiable_tracker.rs:206`, `:229-256`).
- Raft apply itself: session requeues are sorted (`state.rs:419-421`), `Expire` and `ReplicaRestarted` walk ordered sets, and lookups in the `HashMap`s do not depend on order.
- Typed-property cache keys: hashed over sorted pairs with a zero-keyed `DefaultHasher` (`simple_scheduler.rs:261-285`).

### 5.7 Task scheduling assumptions and spawned loops

- **Background loops with timers:**
  - Scheduler: matching (`simple_scheduler.rs:1343`), abandoned sweep (`:1266`), fleet exchange (`:1288`), eviction and kill tickers per scheduler (`worker_api_server.rs:91`), memory-db event handler (`memory_awaited_action_db.rs:979`), Redis pull and active-count loops (`store_awaited_action_db.rs:877`, `:903`), raft init and proposer (`raft_awaited_action_db/mod.rs:501`, `:670`).
  - Worker: keepalive (`local_worker.rs:504`), zombie reaper (`:1342`), orphan sweep (`:1363`), resource sampler (`running_actions_manager.rs:298`), persistent pool sweeper (`pool.rs:440`).
- **Per-event spawns whose order versus other work is unspecified:**
  - one `background_spawn!` per revoked attempt (SS13)
  - per-worker reader and processor tasks (`worker_api_server.rs:306`, `:319`)
  - cache-lookup per `Execute` (`cache_lookup_scheduler.rs:233`)
  - upstream stream relay (`grpc_scheduler.rs:160`)
  - action tasks on the worker (`local_worker.rs:882`)
- **OS threads outside the simulator:**
  - raft WAL writer and snapshot writer (`raft_awaited_action_db/store.rs:211`, `:580`); use the in-memory `LogStore` (`data_dir` unset) in simulation.
  - worker `spawn_blocking` for `/proc` scans, disk walks and every `nativelink_util::fs` call.
- **Lock and notify behavior:**
  - `async_lock::Mutex` FIFO fairness decides whether the eviction sweep or a queued keepalive gets `ApiWorkerScheduler`'s lock first (T2a).
  - `Notify::notify_one` stores one permit, and the matching task can consume it during construction and exit (SS20).
  - `watch` channels coalesce: a client can miss an intermediate stage (fine for REAPI, but a history checker must read the log, not the client streams).
- **Proposer group commit** (RA10): batch boundaries follow arrival timing, so two runs with the same inputs can produce different logs. Apply is deterministic given the log; the simulator's determinism check must hash the log and the canonical state, not re-derive them from inputs.

### 5.8 Outside the process

- **Redis:** server-side TTLs (SA2, SA5, SA10, SA13) and pub/sub delivery (polled when unreliable, SA4).
- **Worker host:** filesystem mtimes (RM2, RM19), `statvfs` (RM18), `/proc` and cgroup reads (CP1, CP2), process exit and signal timing (RM4), the reaper's process-global state (`reaper.rs:41`, `:92`). These are shared by every simulated worker in one process.

### 5.9 What the simulator needs, in order

1. One `Clock` (monotonic `now`, `sleep`, and a wall `now` used only for reporting) threaded through the scheduler, the worker API server, the execution server and the worker. Delete direct `SystemTime::now()` and `std::time::Instant::now()` from decision paths (§5.1, §5.2). Retire `MockInstantWrapped`.
2. An id source for operation ids, worker/session ids and origin events, seeded from the simulation seed. Better: session epochs and attempt ids from the log.
3. A seeded RNG for retry and reconnect jitter, and for openraft's election timeouts.
4. `biased;` on decision-relevant `select!`s, or an explicit tie-break.
5. Sorted iteration (or `BTreeMap`) wherever a map walk produces writes or log entries (§5.6).
6. In-memory raft log; no `spawn_blocking` in the scheduler path under simulation.
7. Determinism is checked on the committed log and a hash of the *canonical* state per index (`canonical_json`, not the snapshot bytes), not on client-observed streams.

## Appendix A. Evidence cited

Commits on this branch's history (short sha, PR where upstream):

| Commit | Subject (abridged) | Rows |
|---|---|---|
| 20aa57241a | scheduler: opt-in `report_executing_on_accept` (the Offered stage) | SM17, K13 |
| 0b5d3030c2 | scheduler: kill a revoked attempt at once; never retry a stale assignment | SS4, SS13, AW23 |
| 4d0538333a | scheduler: losing a silent worker costs `worker_losses`, not the action's retries | SM12, SM20 |
| d9d0fa46f9 | scheduler: a silent worker is not offered work (both backends) | AW6 |
| f05ce8b122 | scheduler: a waiting client keeps its operation, on both backends | MD3, RA1 |
| 39360ad252 | scheduler(raft): restart grace for clients; model test; two state-machine fixes | RA1, RA8 |
| 36f5f46442 | scheduler(raft): a restarted replica requeues what its dead process handed out | RA7 |
| b08ae84565 | scheduler(raft): group commit | RA10, RA18 |
| dbd6557830 | worker api: attempt identity on the wire, worker dedup, kill tombstones | AW14, AW23, RM11, RM12, LW9 |
| 76b1f93075 | fix(scheduler): tolerate backwards clock steps in worker liveness refresh (#2900) | AW2 |
| 7d3d20ca83 | Let paused idle workers accept new work (#2876) | AW3, LW8, CP1 |
| db375f0053 | Change worker to consider reclaimable page cache as usable memory (#2873) | CP2 |
| 389cf73574 | Worker: a stalled scheduler connection reconnects (#2856) | LW4, WA4 |
| 82240c6409 | Worker: bound the input fetch with `max_download_timeout` (#2846) | RM13, RM16 |
| b4a5468248, d86ff4f38c | Worker: capacity and free memory from its own cgroup (#2845, #2820) | CP1 |
| 9a03932c0f | Worker: build the cleanup mark's guard after the lock is released (#2843) | RM16 |
| 035dd63165 | Persistent workers: a configurable pool with an idle sweeper | RM8, PW4 |
| 3c52ee1a70 | Scheduler: memory escalation with retry budgets by cause (#2829) | SM19–SM21 |
| 5b4e555070 | Give a live worker a grace period to report its own timeout (#2826) | SM9 |
| 7bd3c5424d | Worker: a disk soft limit (#2825) | RM1, RM2 |
| 4fe0a27ba9 | Worker reaps zombies; stragglers no longer hold output open (#2823) | LW15, RP1 |
| 9a95e29153 | Worker: SIGTERM grace before the kill, … (#2822) | RM4, RM6, RM15 |
| fcdccc81fd | Worker: soft memory enforcement, disk guard (#2812) | RM1 |
| f10c7b70c1 | Worker: bound output capture, upload fan-out, precondition scripts, orphaned directories (#2810) | LW6, RM19 |
| a21d82c8bd | Dispatch is acknowledged or declined by the worker (#2808) | SM22, AW15, AW16, WA9 |
| 2bfa0207f8 | GoingAway with a drain flag, reconnect backoff with jitter (#2807) | AW24, WA7, LW3, LW5 |
| 02a64f2b4c | Admin API: list connected workers and queued demand (#2804) | AD1 |
| 5ef7a8315d | Redis scheduler store: RESP3 pages, the retention default, … (#2803) | DF1, SA10 |
| 31be88082f | Unsatisfiable actions: the clock follows the fleet's shapes; no-worker timeout (#2799) | SS3, SS7, SS16, UT2 |
| 71bb05a0ee | Scheduler worker state: budget released at completion, pause until next keepalive, retry cap fails with FailedPrecondition (#2798) | SM19, AW3, AW11 |
| 2f54cc3bcb | Worker liveness: any message counts, a 10 s default, a dead keepalive reconnects (#2797) | AW24, AW26, WA4, WA8, LW7 |
| 4c06bb81ec | Serve the queue in order; workers report load on the keepalive (#2796) | AW7, CP2 |
| 67cebb5bb8 | scheduler: fail queued actions with criteria no worker can meet (#2784) | SS3 |
| 8f2c8847d2 | Fix signal-terminated worker property commands (#2780) | WU1 |
| f42cc42ec5 | Emit cache size; keep active counts for Redis schedulers (#2772) | SA7 |
| 0c41992c42 | scheduler: fix active counts when a client disappears (#2765) | SM13 |
| d48a7f0695 | worker: keep directory construction retries on the same lock (#2750) | DC4 |
| 090d9722aa | Retire queued actions no client is waiting on (#2726) | SM6, SS15 |
| 4ae820a8e9 | Evict a worker that never acknowledges a kill (#2707) | AW27 |
| a85114732a | Tell workers to kill operations the scheduler no longer has executing on them (#2693) | AW28, WA2 |
| a50385e27a | Reap orphaned actions and check worker liveness before recreating one (#2685) | SM11, WR1, SA8 |
| d4fa6af626 | Don't let one scheduler instance time out another's in-flight actions (#2680) | SM11, WR3 |
| df7de0dca2 | fix(worker): lease active action inputs across eviction (#2675) | RM24, DC4 |
| bafafc78f8 | fix(worker): don't abort the worker when a disconnect catches an action in transit (#2658) | LW1 |
| 7a4279b4e5 | Add fallback match interval to simple scheduler (#2557) | SS14 |
| 2cbf21ef52 | Prefetch directory-cache tree protos with one GetTree stream (#2546) | DC4 |
| f25f68d30e | Make cleanup wait timeout configurable (#2456) | RM17 |
| d9403b0a55 | `store_awaited_action_db`: bound `cid_*` lifetime (#2417) | SA5 |
| 7e79dea424 | `store_awaited_action_db`: retry `try_subscribe` once on miss (#2367) | SA9 |
| a00bf8c0e1, bff444d06b | Directory cache eviction fixes (#2347, #2366) | DC3 |
| 43ab01dbaa | Add expiry to completed Redis actions (#2315) | SA10 |
| 10ed3c57b2 | Log additional timeout cases (#2232) | SM3 |
| 94e7e3f134 | Add logs for stall detection (#2155) | RM13 |
| 5549a969bd | Add max action executing timeouts to scheduler (#2153) | SM11, K4 |
| 24cc324b21 | Add max upload timeout (#2150) | RM14 |
| 93f4eaddad | Fix the scheduler timeouts and errors (#2083) | SM3 |
| 14b2cc684e | Recoverable connection pool (#2067) | UX6 |
| 17088593e5 | fix: guard shutting down in scheduler while SIGTERM (#2012) | AW25 |
| e9250ee832 | Single worker stream (#1977) | AC1, WA5 |
| 8527f258f7 | Notify execution complete (#1975) | LW10 |
| 1055cd1504 | Harden worker disconnect (#1972) | AW13, AW18, WA5 |
| 9fcf5b1de4 | Sweep forgotten client operation IDs (#1965) | SM13 |
| e691bea24b | Retry matching on failure (#1892) | SS18 |
| 47602d1d83 | Fix directory collision on action retries (#1868) | RM17 |
| b00fb48ae5 | feat(worker): opt-in Buck2 capture (#2791) | BC1, BC2 |
| e753b8d4dc | Add ability to prefix `worker_id` in config (#1578) | WA3 |

Unmerged fork branches whose tip commit documents a bug listed here:

| Branch | Tip | What it shows | Rows |
|---|---|---|---|
| `b7r6/tl1-keepalive-liveness-starvation` | 548788fe13 | Registry heartbeat stamped after the `inner` lock; a punctual keepalive is evicted; heartbeat not monotone. | AW20, WR2, WA4 |
| `fix/eviction-rechecks-heartbeat` | f7483a11ec | `remove_timedout_workers` evicts on a stale snapshot. | AW26 |
| `fix/kill-revoked-revalidates` | 2760207eb1 | Kill sweep TOCTOU; `dispatched_at` (seconds) is not an identity. | AW28 |
| `repro/kill-revoked-toctou` | — | Repro of the same. | AW10, AW28 |
| `fix/worker-replacement-evicts` | a7a190daef | Same-id re-registration orphans the old incarnation's ops; double execution. | AW4 |
| `fix/matching-engine-startup` | 373d2bae94 | Matching task exits if polled during construction. | SS20 |
| `fix/store-keepalive-retry` | 67505a6c78 | Failed `ck_` write still advances the local stamp; a peer retires a live client's action. | SA2, SM6 |
| `fix/awaited-action-prefer-live` | faed855970 | `try_subscribe` returns a completed record before a live one (N7 half). | SA9 |
| `fix/astra-n-wave` | 337d589f9d (N2), 4e57c0844d (N1) | Same-worker redispatch guard (N2) not merged here. | T3a |
