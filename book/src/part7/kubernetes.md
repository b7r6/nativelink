# Kubernetes Production

For production deployments — autoscaling, high availability, rolling updates, observability — NativeLink runs on Kubernetes. This chapter covers the architecture, the Helm chart, and the operational patterns.

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  Kubernetes Cluster                                           │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │  Ingress (TLS termination, load balancing)              │  │
│  └───────────────────────┬─────────────────────────────────┘  │
│                          │                                    │
│  ┌───────────────────────▼─────────────────────────────────┐  │
│  │  Scheduler Deployment (2+ replicas, Redis backend)      │  │
│  │  Services: CAS, AC, Execution, Capabilities, ByteStream │  │
│  └───────────────────────┬─────────────────────────────────┘  │
│                          │                                    │
│  ┌───────────────────────▼─────────────────────────────────┐  │
│  │  Worker StatefulSet / Deployment (autoscaled)           │  │
│  │  Connects to scheduler Worker API                       │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │  Redis (scheduler state)                                │  │
│  └─────────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │  Cloud Storage (S3/GCS/R2) — shared CAS + AC backend    │  │
│  └─────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

## Helm Chart

NativeLink publishes a Helm chart on Artifact Hub:

**Source:** [`kubernetes/`](https://github.com/straylight-prelude/straylight-nativelink/tree/main/kubernetes)

```bash
helm repo add nativelink https://tracemachina.github.io/nativelink
helm install nativelink nativelink/nativelink \
  --values my-values.yaml
```

## Multiple Scheduler Instances (HA)

For high availability, run multiple scheduler replicas with shared state in Redis:

```json5
// Scheduler config
schedulers: [{
  name: "MAIN_SCHEDULER",
  simple: {
    supported_platform_properties: { /* ... */ },
    experimental_redis_scheduler_state: {
      addresses: ["redis://redis:6379"],
      key_prefix: "sched:",
      worker_timeout_s: 30,
      action_timeout_s: 600
    }
  }
}]
```

With Redis backend:
- Workers can connect to any scheduler instance
- Actions queued on one scheduler are visible to all
- Worker heartbeats are shared across instances
- Scheduler instances are stateless and can be restarted independently

## Worker Autoscaling

### Horizontal Pod Autoscaler (HPA)

Scale workers based on queue depth or CPU utilization:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nativelink-workers
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nativelink-worker
  minReplicas: 2
  maxReplicas: 50
  metrics:
    - type: Pods
      pods:
        metric:
          name: nativelink_scheduler_queue_depth
        target:
          type: AverageValue
          averageValue: 5
```

### KEDA (Event-Driven Autoscaling)

For more responsive scaling, use KEDA with custom metrics from NativeLink's Prometheus exporter:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: nativelink-worker-scaler
spec:
  scaleTargetRef:
    name: nativelink-worker
  minReplicaCount: 1
  maxReplicaCount: 100
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus:9090
        metricName: nativelink_queued_actions
        query: nativelink_queued_actions{scheduler="MAIN_SCHEDULER"}
        threshold: "10"
```

## Storage Configuration for Kubernetes

Workers in Kubernetes use ephemeral storage for their fast CAS tier:

```yaml
# Worker pod spec
spec:
  containers:
    - name: nativelink-worker
      volumeMounts:
        - name: work-dir
          mountPath: /data/work
        - name: cas-cache
          mountPath: /data/cas
  volumes:
    - name: work-dir
      emptyDir:
        sizeLimit: 50Gi
    - name: cas-cache
      emptyDir:
        sizeLimit: 100Gi
```

Or with persistent volumes for cross-restart cache persistence:

```yaml
  volumes:
    - name: cas-cache
      persistentVolumeClaim:
        claimName: worker-cas-pvc
```

The slow tier (S3/GCS) is configured in the NativeLink JSON5 config and doesn't need Kubernetes volumes.

## TLS and mTLS

For production, use TLS on all connections:

```json5
// Server config
listener: {
  http: {
    socket_address: "0.0.0.0:50051",
    tls: {
      cert_file: "/certs/tls.crt",
      key_file: "/certs/tls.key",
      client_ca_file: "/certs/ca.crt"  // mTLS
    }
  }
}
```

Mount certificates from Kubernetes secrets or cert-manager:

```yaml
spec:
  containers:
    - name: nativelink
      volumeMounts:
        - name: tls-certs
          mountPath: /certs
          readOnly: true
  volumes:
    - name: tls-certs
      secret:
        secretName: nativelink-tls
```

## Rolling Updates

NativeLink supports graceful shutdown (handles `SIGTERM`):
1. Workers drain in-flight actions before exiting
2. Schedulers deregister from the load balancer

Configure the pod disruption budget:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: nativelink-workers
spec:
  minAvailable: "50%"
  selector:
    matchLabels:
      app: nativelink-worker
```

And set appropriate termination grace period:

```yaml
spec:
  terminationGracePeriodSeconds: 120  # Match worker's graceful_shutdown_timeout
```

## Resource Requests and Limits

Scheduler (CPU-light, memory for connection state):
```yaml
resources:
  requests:
    cpu: "500m"
    memory: "1Gi"
  limits:
    cpu: "2"
    memory: "4Gi"
```

Workers (CPU-heavy for action execution):
```yaml
resources:
  requests:
    cpu: "4"
    memory: "8Gi"
  limits:
    cpu: "16"
    memory: "32Gi"
```

## Multi-Cluster / Multi-Region

For global teams, deploy NativeLink per-region with shared cloud storage:

```
Region A: Scheduler + Workers → S3 (us-east-1)
Region B: Scheduler + Workers → S3 (eu-west-1)
Cross-region replication: S3 CRR between buckets
```

Each region has its own scheduler and workers. CAS blobs are replicated between regions via cloud provider replication. A cache hit from region A is available in region B after replication lag (usually seconds).

Alternatively, use NativeLink's `grpc` store to proxy between regions:

```json5
// Region B's CAS, falls back to Region A
{
  name: "CAS_STORE",
  fast_slow: {
    fast: { filesystem: { /* local cache */ } },
    slow: {
      grpc: {
        instance_name: "main",
        endpoints: [{ address: "grpc://nativelink-region-a:50051" }]
      }
    }
  }
}
```
