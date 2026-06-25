# Observability

You can't operate what you can't observe. NativeLink exports metrics via OpenTelemetry, supports structured logging, and provides health endpoints. This chapter covers what to monitor and how.

## Metrics via OpenTelemetry

NativeLink exports metrics via OTLP (OpenTelemetry Protocol). Configure an OTLP endpoint in your deployment:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317 nativelink config.json5
```

**Source:** [`nativelink-util/src/telemetry.rs`](https://github.com/straylight-prelude/straylight-nativelink/blob/main/nativelink-util/src/telemetry.rs)

## The Metrics Stack

A reference monitoring stack:

**Source:** [`deployment-examples/metrics/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/deployment-examples/metrics)

```
NativeLink → OTLP → OpenTelemetry Collector → Prometheus → Grafana
```

The deployment-examples include Docker Compose configs for this entire stack.

## Key Metrics to Monitor

### Store Metrics (via `cache_metrics` wrapper)

Wrap your stores with `cache_metrics` to get hit/miss counters:

```json5
{
  name: "AC_WITH_METRICS",
  cache_metrics: {
    backend: { ref_store: { name: "AC_STORE" } }
  }
}
```

Key metrics:
- **`cache_hits` / `cache_misses`** — cache effectiveness. The ratio is your hit rate.
- **`bytes_uploaded` / `bytes_downloaded`** — throughput. Sustained high download = good cache utilization.
- **`upload_latency` / `download_latency`** — per-store latency. Helps identify slow backends.

### Scheduler Metrics

- **Queue depth** — actions waiting for a worker. High = need more workers or faster workers.
- **Dispatch latency** — time from submission to worker assignment. High = matching or capacity problem.
- **Worker count** — connected workers. Drops = worker crashes or network issues.
- **Action completion time** — end-to-end execution time. Trends reveal regression.

### Worker Metrics

- **Actions in progress** — concurrent executions per worker. Saturated = add workers.
- **Execution time** — per-action time. Outliers = specific actions to investigate.
- **CAS fetch time** — time downloading inputs. High = network bottleneck or CAS latency.
- **Upload time** — time uploading outputs. High = large outputs or slow backend.

## Health Endpoints

NativeLink exposes health on configured health service ports:

```json5
services: {
  health: {}
}
```

```bash
# gRPC health check
grpcurl -plaintext localhost:50061 grpc.health.v1.Health/Check

# HTTP health (same port)
curl http://localhost:50061/status
```

Use these as Kubernetes liveness/readiness probes:

```yaml
livenessProbe:
  grpc:
    port: 50061
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  grpc:
    port: 50061
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Structured Logging

NativeLink uses `tracing` for structured logging. Control verbosity with `RUST_LOG`:

```bash
# Default: info
RUST_LOG=info nativelink config.json5

# Verbose store operations:
RUST_LOG=nativelink_store=debug nativelink config.json5

# Trace all gRPC calls:
RUST_LOG=nativelink_service=trace nativelink config.json5
```

Logs are JSON-structured when OTLP is configured, making them queryable in log aggregation systems (Loki, Elasticsearch, CloudWatch).

## Alerting Rules

Recommended Prometheus alerting rules:

```yaml
# Cache hit rate below threshold
- alert: LowCacheHitRate
  expr: |
    rate(cache_hits_total[5m]) /
    (rate(cache_hits_total[5m]) + rate(cache_misses_total[5m])) < 0.5
  for: 10m
  annotations:
    summary: "Cache hit rate below 50% — likely toolchain mismatch"

# Worker pool unhealthy
- alert: WorkerCountLow
  expr: nativelink_connected_workers < 2
  for: 5m
  annotations:
    summary: "Fewer than 2 workers connected — actions will queue"

# Action queue growing
- alert: ActionQueueBacklog
  expr: nativelink_queued_actions > 100
  for: 5m
  annotations:
    summary: "Over 100 actions queued — need more workers"

# Store latency spike
- alert: StoreLatencyHigh
  expr: histogram_quantile(0.99, rate(store_operation_duration_seconds_bucket[5m])) > 5
  for: 5m
  annotations:
    summary: "p99 store operation latency > 5s — backend degradation"
```

## Debugging with Metrics

### "Why is my build slow?"

Check:
1. **Queue depth** — actions waiting? Add workers.
2. **CAS fetch time** — slow input download? Check network, check CAS backend latency.
3. **Action execution time** — the action itself is slow? Check worker resources.
4. **Upload time** — slow output upload? Large outputs, check bandwidth.

### "Why is my cache hit rate low?"

Check:
1. **Different action hashes** — use `cache_misses` broken down by action type. Which actions miss?
2. **AC eviction** — are results being evicted before reuse? Increase AC size.
3. **CAS eviction** — are referenced blobs missing? Use `completeness_checking`, increase CAS size.
4. **Client comparison** — compare action hashes between machines (see Debugging Cache Misses chapter).

### "Why did the scheduler stop dispatching?"

Check:
1. **Worker count** — did workers disconnect? Check worker logs for crash/timeout.
2. **Platform matching** — are queued actions requesting properties no worker has?
3. **Redis connectivity** — if using Redis backend, is Redis healthy?
4. **Scheduler health** — is the scheduler process healthy? Check liveness probe.
