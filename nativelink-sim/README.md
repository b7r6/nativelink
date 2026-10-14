# nativelink-sim

Deterministic simulation of the NativeLink scheduler (stream E of
[`FENCED.md`](../nativelink-scheduler/src/raft_awaited_action_db/FENCED.md)).
The real scheduler, built by the production factory on the raft backend with a
real data directory, serves the real Execution and `WorkerApi` services over
[turmoil](https://docs.rs/turmoil)'s simulated network to simulated workers and
clients. A seeded fault plan crashes, partitions and skews them; every
client-visible and worker-visible event goes into a history; a checker reads the
history for safety and liveness violations. A run is a function of its seed.

## Commands

```sh
export CARGO_BUILD_JOBS=6
cargo test -p nativelink-sim                          # smoke set, ~15 s
cargo run -p nativelink-sim -- run --seed 7 --mode absurd --scenario blackhole
cargo run -p nativelink-sim -- run --seed 7 --scenario chaos --history /tmp/h.txt
cargo run -p nativelink-sim -- determinism --seed 7 --scenario chaos
cargo run -p nativelink-sim -- sweep --seeds 1..200 --mode all --scenario all --jobs 4
SIM_SEEDS=1..1000 cargo test -p nativelink-sim -- --ignored --nocapture sweep
```

`run` prints every violation with a minimal trace (the events of that job,
client operation, scheduler operation or worker session, plus every fault) and
a `SUMMARY` line with the history hash, the raft-log hash and counters. Its exit
status is 1 when any check fails. `sweep` runs each seed in its own child
process (at most `--jobs` at once, about 50 MB each) and prints, per mode and
scenario, every violation kind with how many runs showed it and the first seed.

Environment: `SIM_DATA_DIR` (default `/dev/shm/nativelink-sim`), `SIM_LOG`
(a tracing filter for the scheduler's own logs), `SIM_PANIC_TRACE=1` (also print
panics), `SIM_DET_OFF` (ablations, below).

## Architecture

```
            turmoil (one OS thread; every host a current-thread tokio runtime,
                     paused clock, advanced 10 ms per step)
 ┌───────────────────────────────┐        ┌────────┐ ┌────────┐ ┌────────┐
 │ sched                         │◀──────▶│  w0    │ │  w1    │ │  w2    │  simulated workers
 │  scheduler_factory(Simple +   │  gRPC  └────────┘ └────────┘ └────────┘  (worker API proto)
 │   raft{data_dir})             │  over
 │  ExecutionServer              │ turmoil ┌────────┐ ┌────────┐ ┌────────┐
 │  WorkerApiServer::new         │◀──────▶│  c0    │ │  c1    │ │  c2    │  simulated clients
 │  memory CAS, seeded at boot   │   TCP   └────────┘ └────────┘ └────────┘  (Execute / WaitExecution)
 └───────────────────────────────┘
   runner: applies the fault plan between steps; history ← every host
```

- `scheduler.rs`: each boot builds the stack exactly as the server binary
  would (`scheduler_factory` with a `SimpleSpec` whose backend is
  `raft { data_dir }`, `ExecutionServer::new`, `WorkerApiServer::new` with its
  own eviction and kill tickers) and serves it with tonic's
  `serve_with_incoming` over a turmoil `TcpListener` (`net.rs`). The raft
  directory is a real directory kept across restarts. The CAS is an in-memory
  store seeded with the workload's actions on every boot; it stands in for the
  separate storage service of a real deployment.
- `worker.rs`: speaks `worker_api.proto` directly, like `LocalWorker`:
  `ConnectWorkerRequest`, keepalives, `StartExecute` → `ExecuteAccepted` (or
  `ExecuteDeclined` when full, re-ack for a resend), `ExecuteComplete` +
  `ExecuteResult` at the end, a kill stops the attempt and reports it as an
  error, a lost connection drops every attempt (`kill_all`) and reconnects as a
  new session. Misbehaviors (`WorkerControl`): `stalled` (frozen software on a
  live connection), `zombie` (ignores kills, keeps attempts across reconnects
  and reports them late), `slow_factor`. Every result carries
  `simw;w=<host>;s=<session>;op=<operation>;a=<attempt>` in
  `execution_metadata.worker`, so a client can tell which attempt produced it.
- `client.rs`: `Execute`, follow the stream; detach (drop the stream) and
  reattach with `WaitExecution`; retry on `UNAVAILABLE` and transport errors
  with backoff; resubmit on `NOT_FOUND`. After `done`, half the jobs read the
  operation once more within retention.
- `faults.rs`: the plan, drawn from the seed up front: worker crash/restart
  (`sim.crash`/`sim.bounce`), scheduler crash/restart, `hold`/`release` (TCP
  survives: messages are delayed), `partition`/`repair` ("blackhole":
  connections die silently, as TCP does across a long outage), scheduler wall
  clock steps (`det::set_realtime_offset_ms`), worker stalls. Every fault is
  healed by the end of the active phase (240 s); the run then has a quiet phase
  (240 s) for liveness.
- `det.rs`: deterministic clocks and entropy (below).
- `history.rs`, `checker.rs`: the history and its checks (below).
- `runner.rs`: one seed on a fresh thread; `main.rs`: `run`, `determinism`,
  `sweep`.

Workload: 3 workers × 2 slots, 3 clients × (8 private jobs + 1 job shared by
all three clients, cacheable, so the scheduler merges them), durations 1–20 s,
submitted over the first 120 s, half of them `skip_cache_lookup`.

## Modes and scenarios

| Mode | `worker_timeout_s` | `max_action_executing_timeout_s` | `client_action_timeout_s` | `retain_completed_for_s` |
|---|---|---|---|---|
| `normal` | 10 | 120 | 60 | 60 |
| `absurd` | 86400 | 86400 | 86400 | 60 |

Both modes: `report_executing_on_accept: true`, `unacknowledged_kill_timeout_s`
60, `dispatch_ack_timeout_s` off, no server-side HTTP/2 keepalive (the
binary's default). Retention stays at 60 s in both: it is garbage collection.

Scenarios: `calm`, `long-detach` (clients also detach for longer than
retention), `slow-workers` (one worker 10× slower), `clock-skew` (scheduler wall
clock steps ±5–120 s) do not kill or partition anything; `worker-crash`,
`scheduler-restart`, `partition`, `blackhole`, `worker-stall`, `zombie` (one
worker ignores kills, stalls), `chaos` (all of them) do.

## Checks

Safety (on the history):

| Kind | Fires when |
|---|---|
| `lost-operation` | `WaitExecution` says `NOT_FOUND` to a client detached for less than the interest window (`min(retain_completed_for_s, client_action_timeout_s)` − 25 s slack), or a finished operation re-read within a third of the window is gone. The client's detach time is never later than the server's, so this errs lenient. |
| `inconsistent-result` | Two different results (attempt identity) delivered for one scheduler operation, to one client or several, or a re-read shows another result. |
| `stale-attempt-result` | A client gets the result of an attempt after the operation was started again elsewhere (a later `StartExecute` reached a worker). |
| `revoked-attempt-result` | A client gets the result of an attempt whose worker had received the kill for it before sending the result. |
| `attempt-reused` | One `(operation, attempt)` handed to two worker sessions. |
| `done-regression` | A finished operation, re-read within retention, is not finished. (Allowed: `EXECUTING → QUEUED`, a retry; counted as `exec->queued`.) |
| `capacity-overcommit` | A worker is offered work while every slot holds an attempt the scheduler handed to that same session and did not revoke. |
| `capacity-overcommit-unacked-kill` | Same, counting attempts the scheduler revoked but the worker never acknowledged (zombie): FENCED's revoke barrier. Attempts a zombie carried over from a dead session are not counted against the scheduler. |
| `unattributed-result` | A successful result no simulated attempt sent. |
| `panic@<file:line>` | Any panic in the process (scheduler tasks included); the report lists the `NativeLink` frames. |

Liveness (after every fault is healed and the quiet phase ran out):
`liveness-stuck` (a job with no final answer) and `unexpected-failure` (a job
that ended in an error other than running out of retries, or than
"no more clients listening" when its client really was away that long).

## Determinism

<!-- filled in below -->
