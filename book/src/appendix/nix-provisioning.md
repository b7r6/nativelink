# Appendix E: Nix Provisioning — Design Proposal

**Status:** Partially shipped. The NixOS module layer exists in this
repository (`nixosModules.nativelink`: `nixos/module.nix` + the §12
`floor-entrypoint.sh`), and fleet hosts deploy from it — the fork owns
its ops surface, and the downstream fleet config is thin imports plus
host facts. The Dhall-typed configuration layer described below remains
a proposal.

This appendix describes the target architecture for correct-by-construction NativeLink provisioning via Nix, Dhall-typed configuration, and NixOS modules.

______________________________________________________________________

## The Problem

NativeLink's JSON5 configuration is powerful but untyped. You can construct configurations that:
- Reference store names that don't exist
- Set `cas_fast_slow_store` to a store that isn't actually a `fast_slow` store
- Configure a worker with platform properties that the scheduler doesn't list in `supported_platform_properties`
- Forget to set `instance_name` consistently across CAS, AC, execution, capabilities, and bytestream
- Deploy a CAS ring where weights don't match across nodes

These are all runtime failures discovered during startup or — worse — during operation. The configuration surface is too large and too interconnected for humans to get right by hand.

> **Part of this already shipped.** The fork now carries an offline validation pass, `nativelink --check <config>`, that answers the *first* bullet above (and the analogous scheduler-name typo) head-on. It resolves every store and scheduler name a worker or server service references — following the `ref_store` wrappers across the transitive closure of stores actually wired to a consumer — and reports each dangling reference, plus any duplicate store or scheduler name, without binding a socket, touching a backend, or creating a store directory (`CasConfig::validate_references`, `cas_server.rs:1450`; wired into the binary at `src/bin/nativelink.rs:850`). A mistyped `cas_store` or `scheduler` name now fails in CI instead of at boot. The workflow is written up in **Part I, [Why This Fork](../part1/why-this-fork.md)** and **Part X, [The Nix Substituter Facade](../part10/nix-substituter.md#validating-configuration)**.
>
> It is a *partial* answer, which is exactly why the rest of this proposal stands. `--check` is a name resolver, not a type system: it confirms that a name resolves, not that the store behind `cas_fast_slow_store` is actually a `fast_slow` store; it does not reconcile `instance_name` across the CAS, AC, execution, capabilities, and bytestream services, align worker platform properties with a scheduler's `supported_platform_properties`, or check that shard weights agree across nodes. Those are the remaining four bullets — the ones the Dhall layer below is built to rule out by construction rather than merely detect.

## The Solution: Three Layers

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 3: nix run .#<topology>                              │
│  Pre-baked correct topologies. Zero config for common cases.│
├─────────────────────────────────────────────────────────────┤
│  Layer 2: NixOS Module (flake.nixosModules.nativelink)      │
│  Systemd service, TLS, firewall, state management.          │
│  Takes a rendered config; handles the operational wrapper.   │
├─────────────────────────────────────────────────────────────┤
│  Layer 1: Dhall Schema + Renderer                           │
│  Typed configuration. Compile-time validation.              │
│  Source of truth → rendered to JSON5.                       │
└─────────────────────────────────────────────────────────────┘
```

Each layer is independently useful:
- Layer 1 alone gives you typed configs you render to JSON5 and use however you want
- Layer 1 + Layer 2 gives you a full NixOS service with typed config
- Layer 3 gives you instant gratification with no understanding required

______________________________________________________________________

## Layer 1: Dhall Schema and Renderer

### Why Dhall

1. **Total functions only.** Dhall programs always terminate — no infinite loops, no runtime errors. Configuration that typechecks is configuration that renders.

2. **Imports are content-addressed.** `https://example.com/schema.dhall sha256:abc123` — if the hash matches, the import is trusted. Immutable. Cacheable. No supply-chain confusion.

3. **Union types for Store/Scheduler variants.** The `StoreSpec` enum in Rust maps directly to a Dhall union. Dhall's exhaustive pattern matching ensures every variant is handled.

4. **Record types for composition contracts.** A `FastSlowSpec` requires `fast: StoreSpec` and `slow: StoreSpec`. This is checked at the type level — you cannot accidentally pass a scheduler where a store is expected.

5. **Functions for topology constructors.** `shardedCas : List { host : Text, weight : Natural } → StoreSpec` — a function that builds a correct shard config from a host list. The function body enforces invariants (non-empty list, matching instance names, etc.).

### Schema (types)

```dhall
-- schema.dhall

let EvictionPolicy = { max_bytes : Natural, max_count : Natural, max_seconds : Natural }

let MatchMode = < Minimum | Exact | Priority | Ignore >

let StoreSpec =
      < Memory : { eviction_policy : EvictionPolicy }
      | Filesystem : { content_path : Text, temp_path : Text, eviction_policy : EvictionPolicy }
      | CloudObjectStore : { region : Text, bucket : Text, key_prefix : Text }
      | FastSlow : { fast : StoreSpec, slow : StoreSpec }
      | Compression : { backend : StoreSpec, algorithm : Text }
      | ExistenceCache : { backend : StoreSpec, eviction_policy : EvictionPolicy }
      | Verify : { backend : StoreSpec, verify_size : Bool, verify_hash : Bool }
      | Shard : { stores : List { store : StoreSpec, weight : Natural } }
      | Grpc : { instance_name : Text, endpoints : List Text }
      | Ref : { name : Text }
      | Noop
      >

let SchedulerSpec =
      < Simple : { supported_platform_properties : List { name : Text, mode : MatchMode } }
      | CacheLookup : { ac_store : Text, scheduler : SchedulerSpec }
      | PropertyModifier : { modifications : List Modification, scheduler : SchedulerSpec }
      >

let WorkerConfig =
      { worker_api_endpoint : Text
      , cas_fast_slow_store : Text
      , work_directory : Text
      , platform_properties : List { name : Text, values : List Text }
      }

let ServerConfig =
      { name : Text
      , listen : Text
      , services : Services
      }

let Config =
      { stores : List { name : Text, spec : StoreSpec }
      , schedulers : List { name : Text, spec : SchedulerSpec }
      , workers : List WorkerConfig
      , servers : List ServerConfig
      , global : GlobalConfig
      }
```

### Constructors (smart builders)

```dhall
-- constructors.dhall

let tieredCas =
      λ(memory_bytes : Natural) →
      λ(disk_path : Text) →
      λ(disk_bytes : Natural) →
      λ(cloud : StoreSpec) →
        StoreSpec.ExistenceCache
          { backend =
              StoreSpec.FastSlow
                { fast =
                    StoreSpec.FastSlow
                      { fast = StoreSpec.Memory { eviction_policy = { max_bytes = memory_bytes, max_count = 0, max_seconds = 0 } }
                      , slow = StoreSpec.Filesystem { content_path = "${disk_path}/content", temp_path = "${disk_path}/tmp", eviction_policy = { max_bytes = disk_bytes, max_count = 0, max_seconds = 0 } }
                      }
                , slow =
                    StoreSpec.Compression
                      { backend = cloud
                      , algorithm = "lz4"
                      }
                }
          , eviction_policy = { max_bytes = 0, max_count = 5000000, max_seconds = 0 }
          }

let shardedCas =
      λ(nodes : List { host : Text, port : Natural, weight : Natural }) →
        StoreSpec.Shard
          { stores =
              List/map
                { host : Text, port : Natural, weight : Natural }
                { store : StoreSpec, weight : Natural }
                (λ(n : { host : Text, port : Natural, weight : Natural }) →
                  { store = StoreSpec.Grpc { instance_name = "main", endpoints = ["grpc://${n.host}:${Natural/show n.port}"] }
                  , weight = n.weight
                  })
                nodes
          }
```

### Fleet topology

```dhall
-- fleet.dhall (example)

let Host =
      { name : Text
      , arch : Text
      , cas_weight : Natural
      , is_scheduler : Bool
      , fast_bytes : Natural
      , memory_bytes : Natural
      }

let fleet : List Host =
      [ { name = "node-1", arch = "x86_64", cas_weight = 4, is_scheduler = True,  fast_bytes = 68719476736, memory_bytes = 34359738368 }
      , { name = "node-2", arch = "x86_64", cas_weight = 4, is_scheduler = False, fast_bytes = 68719476736, memory_bytes = 34359738368 }
      , { name = "node-3", arch = "x86_64", cas_weight = 2, is_scheduler = False, fast_bytes = 17179869184, memory_bytes = 8589934592 }
      ]

let renderHost = λ(host : Host) → λ(fleet : List Host) →
      -- Builds the full Config for this host given the fleet context
      -- Scheduler host gets: scheduler + CAS + worker
      -- Non-scheduler gets: CAS (shard member) + worker + grpc store to scheduler
      ...
```

### Rendering

```
dhall-to-json --file fleet.dhall | jq '.[] | select(.host=="node-1") | .json'
→ valid NativeLink JSON5 config
```

In Nix, this happens at eval time via IFD:

```nix
renderedConfig = pkgs.runCommand "nativelink-config-${host}.json5" {
  nativeBuildInputs = [ pkgs.dhall-json pkgs.jq ];
} ''
  dhall-to-json --file ${./data/render-all.dhall} \
    | jq -e --arg h "${host}" '.[] | select(.host==$h) | .json' -r \
    > $out
'';
```

### What this prevents

| Error class | Without Dhall | With Dhall |
|-------------|--------------|------------|
| Typo in store name reference | Runtime crash | Type error at render time |
| Missing instance_name on one service | Silent misrouting | Enforced by constructor |
| Shard weights inconsistent across nodes | Corrupt distribution | Single source of truth (fleet.dhall) |
| Worker properties not in scheduler list | Actions queue forever | Computed from same data |
| Wrong store type for worker CAS | Runtime error | Type constraint |

______________________________________________________________________

## Layer 2: NixOS Module

### Interface

```nix
# flake.nixosModules.nativelink

options.services.nativelink = {
  enable = mkEnableOption "NativeLink remote execution service";

  package = mkOption {
    type = types.package;
    default = inputs.self.packages.${system}.nativelink;
  };

  # Typed config (preferred)
  dhallHost = mkOption {
    type = types.nullOr types.str;
    default = null;
    description = "Fleet host name; renders typed config from Dhall.";
  };

  # Escape hatch
  configFile = mkOption {
    type = types.nullOr types.path;
    default = null;
    description = "Manual JSON5 config file (bypasses Dhall).";
  };

  # Operational options
  tls = {
    enable = mkEnableOption "TLS on public listener";
    certFile = mkOption { type = types.nullOr types.path; default = null; };
    keyFile = mkOption { type = types.nullOr types.path; default = null; };
  };

  r2 = {
    enable = mkEnableOption "R2 cloud storage backend";
    accountId = mkOption { type = types.str; default = ""; };
    bucket = mkOption { type = types.str; default = ""; };
    environmentFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "File with R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY.";
    };
  };

  openFirewall = mkOption {
    type = types.bool;
    default = false;
  };

  trustedInterfaces = mkOption {
    type = types.listOf types.str;
    default = [ "tailscale0" ];
  };
};
```

### What it produces

```nix
config = mkIf cfg.enable {
  systemd.services.nativelink = {
    description = "NativeLink Remote Execution Service";
    wantedBy = [ "multi-user.target" ];
    after = [ "network-online.target" ];
    wants = [ "network-online.target" ];

    serviceConfig = {
      ExecStart = "${cfg.package}/bin/nativelink ${configFile}";
      DynamicUser = true;
      StateDirectory = "nativelink";
      CacheDirectory = "nativelink";
      EnvironmentFile = mkIf (cfg.r2.environmentFile != null) cfg.r2.environmentFile;
      Restart = "on-failure";
      RestartSec = "5s";

      # Hardening
      ProtectSystem = "strict";
      ProtectHome = true;
      NoNewPrivileges = true;
      PrivateTmp = true;
      ReadWritePaths = [ "/var/lib/nativelink" "/var/cache/nativelink" ];
      LimitNOFILE = 65536;
    };
  };

  networking.firewall.interfaces = genAttrs cfg.trustedInterfaces (_: {
    allowedTCPPorts = [ 50051 50061 ];
  });
};
```

______________________________________________________________________

## Layer 3: Runnable Topologies

Pre-baked `nix run` targets for common deployment patterns:

```nix
apps = {
  # Zero-config: all-in-one, filesystem store, localhost
  single-node = writeShellScript "nativelink-single-node" ''
    exec ${nativelink}/bin/nativelink ${./topologies/single-node.json5}
  '';

  # Two workers + shared CAS (requires Docker Compose or multi-process)
  multi-worker = writeShellScript "nativelink-multi-worker" ''
    echo "Starting scheduler + 2 workers..."
    ${nativelink}/bin/nativelink ${./topologies/multi-worker-scheduler.json5} &
    sleep 1
    ${nativelink}/bin/nativelink ${./topologies/multi-worker-worker.json5} &
    ${nativelink}/bin/nativelink ${./topologies/multi-worker-worker.json5} &
    wait
  '';

  # Cache-only (no execution, just CAS + AC)
  cache-only = writeShellScript "nativelink-cache-only" ''
    exec ${nativelink}/bin/nativelink ${./topologies/cache-only.json5}
  '';
};
```

Each topology config is a committed JSON5 file rendered from the Dhall schema — verified correct by the type system, tested in CI by the VM check.

### Usage

```bash
# Instant remote cache for local development:
nix run github:straylight-prelude/straylight-nativelink#cache-only

# Full single-node remote execution:
nix run github:straylight-prelude/straylight-nativelink#single-node

# Then in your project:
echo 'build --remote_cache=grpc://127.0.0.1:50051' >> .bazelrc
bazel build //...
```

______________________________________________________________________

## Layer Integration

The layers compose:

```
Layer 1 (Dhall)
  │
  │  dhall-to-json
  ▼
Layer 2 (NixOS Module)
  │
  │  systemd, firewall, TLS, state
  ▼
Running system
```

Or for the quick path:

```
Layer 3 (nix run)
  │
  │  pre-rendered JSON5 (from Dhall, committed)
  ▼
Running process (no NixOS required)
```

### Invariants enforced across layers

| Invariant | Enforced by |
|-----------|------------|
| Store names are consistent | Dhall type system (Layer 1) |
| Instance names match across services | Constructor function (Layer 1) |
| CAS shard ring is consistent | Single `fleet` definition (Layer 1) |
| Platform properties align | Computed from shared data (Layer 1) |
| Ports don't conflict | NixOS module assertions (Layer 2) |
| State directories exist | systemd StateDirectory (Layer 2) |
| Secrets never in nix store | EnvironmentFile + shell expansion (Layer 2) |
| Firewall allows needed ports | Module `openFirewall` (Layer 2) |
| Config is valid JSON5 | Dhall rendering succeeds (CI check) |
| Service starts and serves | VM test (NixOS check) |

______________________________________________________________________

## Multi-Machine Topologies

The fleet topology (Layer 1) must express not just "what config does each host get" but the **relationships** between hosts: who is the scheduler, who are the CAS ring members, who are the workers, and how they discover each other.

### The Topology Type

```dhall
-- topology.dhall

let Role = < Scheduler | CasRing | Worker | Gateway >

let Host =
      { name : Text
      , address : Text           -- tailnet FQDN or IP
      , arch : Text              -- x86_64 / aarch64
      , roles : List Role        -- a host can play multiple roles
      , cas_weight : Natural     -- 0 if not in the CAS ring
      , fast_bytes : Natural     -- local NVMe tier size
      , memory_bytes : Natural   -- in-memory hot tier
      , worker_cpus : Natural    -- 0 if not a worker
      , worker_memory : Natural  -- worker memory for platform props
      }

let Topology =
      { hosts : List Host
      , scheduler : Text         -- must name a host with Role.Scheduler
      , instance_name : Text     -- shared across the fleet
      , cloud_backend : Optional CloudBackend
      , autoscaling : Optional AutoscalingConfig
      }
```

### Rendering a Multi-Machine Fleet

The renderer takes the topology and produces **per-host configs** where:
- Every host in `roles = [CasRing]` gets a CAS server config with its local fast tier + shared cloud slow tier
- The scheduler host gets a `SimpleScheduler` with Redis backend and a `ShardStore` pointing to all CAS ring members
- Workers get a `grpc` store pointing to the scheduler, platform properties derived from their hardware specs
- Cross-references (worker → scheduler endpoint, scheduler → CAS ring members) are computed from the topology, not hand-written

```dhall
let renderTopology : Topology → List { host : Text, json : Text } =
      λ(topo : Topology) →
        let casRing = List/filter Host (λ(h : Host) → List/any Role (λ(r : Role) → merge { Scheduler = False, CasRing = True, Worker = False, Gateway = False } r) h.roles) topo.hosts

        let schedulerHost = List/head Host (List/filter Host (λ(h : Host) → h.name == topo.scheduler) topo.hosts)

        let workerHosts = List/filter Host (λ(h : Host) → List/any Role (λ(r : Role) → merge { Scheduler = False, CasRing = False, Worker = True, Gateway = False } r) h.roles) topo.hosts

        -- For each host, build its config based on roles:
        -- CAS ring member: serves CAS/AC/ByteStream on :50051
        -- Scheduler: serves Execution/Capabilities, has ShardStore over CAS ring
        -- Worker: connects to scheduler:50061, local fast_slow CAS
        in List/map Host { host : Text, json : Text }
             (λ(h : Host) → { host = h.name, json = renderHostConfig h topo casRing })
             topo.hosts
```

### Concrete Example: 4-Node Production Fleet

```dhall
let production : Topology =
  { hosts =
      [ { name = "scheduler-1"
        , address = "scheduler-1.internal"
        , arch = "x86_64"
        , roles = [ Role.Scheduler, Role.CasRing ]
        , cas_weight = 4
        , fast_bytes = 107374182400   -- 100 GiB
        , memory_bytes = 34359738368  -- 32 GiB
        , worker_cpus = 0
        , worker_memory = 0
        }
      , { name = "cas-1"
        , address = "cas-1.internal"
        , arch = "x86_64"
        , roles = [ Role.CasRing ]
        , cas_weight = 4
        , fast_bytes = 214748364800   -- 200 GiB
        , memory_bytes = 17179869184  -- 16 GiB
        , worker_cpus = 0
        , worker_memory = 0
        }
      , { name = "worker-1"
        , address = "worker-1.internal"
        , arch = "x86_64"
        , roles = [ Role.Worker ]
        , cas_weight = 0
        , fast_bytes = 53687091200    -- 50 GiB
        , memory_bytes = 8589934592   -- 8 GiB
        , worker_cpus = 16
        , worker_memory = 34359738368
        }
      , { name = "worker-2"
        , address = "worker-2.internal"
        , arch = "aarch64"
        , roles = [ Role.Worker ]
        , cas_weight = 0
        , fast_bytes = 53687091200
        , memory_bytes = 8589934592
        , worker_cpus = 8
        , worker_memory = 17179869184
        }
      ]
  , scheduler = "scheduler-1"
  , instance_name = "main"
  , cloud_backend = Some { type = "r2", bucket = "my-cas", region = "auto", account_id = "\${R2_ACCOUNT_ID}" }
  , autoscaling = None AutoscalingConfig
  }
```

### What the renderer produces (per host)

**scheduler-1** gets:
- CAS server (local fast tier + R2 slow tier, weight 4 in shard)
- Scheduler with `ShardStore` over `[grpc://scheduler-1:50051 (weight 4), grpc://cas-1:50051 (weight 4)]`
- `CacheLookupScheduler` wrapping `SimpleScheduler` with Redis
- Worker API on `:50061`
- All services on `instance_name = "main"`

**cas-1** gets:
- CAS server (local fast tier + R2 slow tier)
- No scheduler, no workers
- CAS/AC/ByteStream only

**worker-1** gets:
- Worker connecting to `grpc://scheduler-1.internal:50061`
- Local `fast_slow` CAS (filesystem fast, `grpc://scheduler-1:50051` slow)
- Platform properties: `{ cpu_count = 16, memory_bytes = 34359738368, OSFamily = "Linux", ISA = "x86-64" }`

**worker-2** gets:
- Same structure but `{ cpu_count = 8, ISA = "aarch64" }`

### Cross-Host Invariants (enforced by the type system)

| Invariant | How |
|-----------|-----|
| Scheduler endpoint is consistent across all workers | Computed from `topo.scheduler` address |
| CAS shard weights are identical on scheduler and ring members | Computed from same `hosts` list |
| Worker platform properties include their arch | Derived from `host.arch` |
| Instance name is uniform | Single `topo.instance_name` propagates everywhere |
| All CAS ring members are reachable from scheduler | Addresses from the topology |
| No orphan workers (scheduler knows about all property types) | `supported_platform_properties` computed from union of all worker properties |

______________________________________________________________________

## Auto-Scaling Topologies

Static fleets work for bare-metal and small teams. For cloud deployments, you need workers that scale with queue depth. The Dhall topology expresses scaling intent; the NixOS module and a controller actuate it.

### The Scaling Model

```dhall
let ScalingPolicy =
      < Fixed : { count : Natural }
      | Range : { min : Natural, max : Natural, target_queue_depth_per_worker : Natural }
      | Cron  : { schedule : Text, count : Natural }  -- e.g. "0 9 * * MON-FRI" → 20
      >

let WorkerPool =
      { name : Text
      , arch : Text
      , platform_properties : List { name : Text, values : List Text }
      , scaling : ScalingPolicy
      , instance_type : Text           -- cloud instance type (informational)
      , fast_bytes : Natural
      , memory_bytes : Natural
      , entrypoint : Optional Text
      , container_image : Optional Text
      }

let AutoscalingConfig =
      { controller : Text              -- "kubernetes" | "nixos-fleet" | "cloud-api"
      , metrics_endpoint : Text        -- prometheus endpoint for queue depth
      , cooldown_seconds : Natural     -- prevent flapping
      , worker_pools : List WorkerPool
      }
```

### Example: Kubernetes Auto-Scaling

```dhall
let k8s_topology : Topology =
  { hosts =
      [ { name = "scheduler"
        , address = "nativelink-scheduler.nativelink.svc.cluster.local"
        , arch = "x86_64"
        , roles = [ Role.Scheduler, Role.CasRing ]
        , cas_weight = 1
        , fast_bytes = 107374182400
        , memory_bytes = 34359738368
        , worker_cpus = 0
        , worker_memory = 0
        }
      ]
  , scheduler = "scheduler"
  , instance_name = "main"
  , cloud_backend = Some { type = "s3", bucket = "nativelink-cas", region = "us-east-1", account_id = "" }
  , autoscaling = Some
      { controller = "kubernetes"
      , metrics_endpoint = "http://prometheus:9090"
      , cooldown_seconds = 60
      , worker_pools =
          [ { name = "compile"
            , arch = "x86_64"
            , platform_properties =
                [ { name = "pool", values = ["compile"] }
                , { name = "ISA", values = ["x86-64"] }
                ]
            , scaling = ScalingPolicy.Range { min = 2, max = 50, target_queue_depth_per_worker = 3 }
            , instance_type = "c6i.4xlarge"
            , fast_bytes = 107374182400
            , memory_bytes = 8589934592
            , entrypoint = None Text
            , container_image = Some "docker://registry/lre-cc@sha256:abc123"
            }
          , { name = "test"
            , arch = "x86_64"
            , platform_properties =
                [ { name = "pool", values = ["test"] }
                , { name = "ISA", values = ["x86-64"] }
                ]
            , scaling = ScalingPolicy.Range { min = 1, max = 20, target_queue_depth_per_worker = 1 }
            , instance_type = "r6i.2xlarge"
            , fast_bytes = 53687091200
            , memory_bytes = 17179869184
            , entrypoint = Some "/usr/local/bin/test-entrypoint.sh"
            , container_image = Some "docker://registry/test-toolchain@sha256:def456"
            }
          , { name = "arm-compile"
            , arch = "aarch64"
            , platform_properties =
                [ { name = "pool", values = ["compile"] }
                , { name = "ISA", values = ["aarch64"] }
                ]
            , scaling = ScalingPolicy.Cron { schedule = "0 9 * * MON-FRI", count = 10 }
            , instance_type = "c7g.4xlarge"
            , fast_bytes = 107374182400
            , memory_bytes = 8589934592
            , entrypoint = None Text
            , container_image = Some "docker://registry/lre-cc-arm@sha256:789abc"
            }
          ]
      }
  }
```

### What the renderer produces for auto-scaling

The Dhall renderer produces **two artifacts** per topology:

1. **NativeLink configs** (per static host) — same as before
2. **Scaling manifests** — per controller type:

**For Kubernetes:**
```yaml
# Generated: worker-pool-compile.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nativelink-worker-compile
spec:
  replicas: 2  # min from ScalingPolicy.Range
  template:
    spec:
      containers:
        - name: nativelink-worker
          image: ghcr.io/tracemachina/nativelink:latest
          args: ["/config/worker.json5"]
          volumeMounts:
            - name: config
              mountPath: /config
            - name: cas-cache
              mountPath: /data/cas
      volumes:
        - name: config
          configMap:
            name: nativelink-worker-compile-config
        - name: cas-cache
          emptyDir:
            sizeLimit: 100Gi
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nativelink-worker-compile-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nativelink-worker-compile
  minReplicas: 2
  maxReplicas: 50
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 60
  metrics:
    - type: External
      external:
        metric:
          name: nativelink_queued_actions
          selector:
            matchLabels:
              pool: compile
        target:
          type: AverageValue
          averageValue: 3  # target_queue_depth_per_worker
```

**For NixOS fleet** (bare-metal/VM auto-scaling via a controller service):
```nix
# Generated: scaling-controller config
{
  pools.compile = {
    min = 2;
    max = 50;
    metric = "nativelink_queued_actions{pool=\"compile\"}";
    target_per_instance = 3;
    cooldown = 60;
    provision = "nixos-rebuild switch --flake .#worker-compile --target-host";
    deprovision = "ssh $host 'systemctl stop nativelink && shutdown -h now'";
  };
}
```

### The Scheduler's View

Auto-scaling is transparent to the scheduler. Workers connect and disconnect; the scheduler's capability index updates dynamically. But the scheduler's `supported_platform_properties` must be a **superset** of all possible worker properties across all pools:

```dhall
let allPlatformProperties : Topology → List { name : Text, mode : MatchMode } =
      λ(topo : Topology) →
        let workerProps = Optional/fold AutoscalingConfig (topo.autoscaling)
              (List { name : Text, mode : MatchMode })
              (λ(ac : AutoscalingConfig) →
                List/concatMap WorkerPool { name : Text, mode : MatchMode }
                  (λ(pool : WorkerPool) → pool.platform_properties)
                  ac.worker_pools)
              ([] : List { name : Text, mode : MatchMode })
        let staticProps = List/concatMap Host { name : Text, mode : MatchMode }
              (λ(h : Host) → hostPlatformProperties h)
              (List/filter Host isWorker topo.hosts)
        in dedup (workerProps # staticProps)
```

This is computed from the topology — not hand-maintained. Add a new pool with a new property, and the scheduler config updates automatically.

### Scaling Policies

| Policy | Use Case | Behavior |
|--------|----------|----------|
| `Fixed { count = N }` | Predictable load, bare-metal | Always N workers. No scaling. |
| `Range { min, max, target }` | Variable load, cloud | Scale between min and max based on queue depth / target. |
| `Cron { schedule, count }` | Business-hours load | Scale to count during schedule, min otherwise. |

Policies compose: a pool can have a `Range` base with a `Cron` override for known peak times. The controller applies the most permissive (highest count) active policy.

### Controller Interface

The topology is declarative (what you want). The controller is imperative (makes it happen). The interface between them:

```
Topology (Dhall)
    │
    │  render
    ▼
Scaling Manifest (YAML/JSON/Nix)
    │
    │  controller reads
    ▼
Controller (runs continuously)
    │
    │  observes: prometheus metrics (queue depth, worker count)
    │  actuates: kubectl scale / nixos-rebuild / cloud API
    ▼
Running Workers (connect to scheduler)
```

The controller is NOT part of NativeLink. It is a thin loop that:
1. Reads the desired scaling policy
2. Queries Prometheus for current queue depth
3. Computes desired replica count: `ceil(queue_depth / target_per_worker)`
4. Clamps to `[min, max]`
5. Actuates if different from current count
6. Sleeps for `cooldown_seconds`

For Kubernetes, this is just an HPA (no custom controller needed). For bare-metal NixOS fleets, it's a small systemd timer + script.

______________________________________________________________________

## Multi-Architecture Topologies

The topology type includes `arch` per host and per pool. This enables:

- **x86_64 + aarch64 workers** in the same fleet, matched by `ISA` platform property
- **Cross-architecture CAS sharing** — one CAS ring serves all architectures (content is arch-independent at the blob level)
- **Per-arch toolchain images** — `container_image` per pool ensures the right toolchain lands on the right arch

The scheduler's `ISA: "exact"` matching ensures an aarch64 action never lands on an x86_64 worker. The topology type makes this explicit:

```dhall
-- Invalid: worker with arch mismatch (caught by assertion)
let bad_pool = { name = "arm-on-x86", arch = "aarch64", ... }
-- rendered on a host with arch = "x86_64" → assertion failure
```

______________________________________________________________________

## Migration Path

### From existing JSON5 configs

The Dhall schema accepts the same logical structure. Migration is:
1. Express your config in Dhall (the constructors make common patterns one-liners)
2. Verify: `dhall-to-json --file my-config.dhall | diff - existing-config.json5`
3. Switch the module to `dhallHost` mode

### From the nixos-config module

The existing `hyper-modern-nixos.nativelink` module becomes the upstream module. The fleet.dhall and schema.dhall move into this repository. Per-host secrets and host-specific overrides stay in the downstream nixos-config as imports of the upstream module.

### For new deployments

Start with `nix run .#single-node`. When you outgrow it, adopt the Dhall schema. When you go to production, use the NixOS module.

______________________________________________________________________

## Implementation Sequence

```
Phase 1 — Single-machine (independently shippable)
  1. Port schema.dhall + render.dhall into this repo (nix/dhall/)
  2. Add nix run .#single-node and nix run .#cache-only (committed JSON5)
  3. Port the NixOS module (flake.nixosModules.nativelink)
  4. Add VM check (nix flake check proves service starts)

Phase 2 — Multi-machine
  5. Add Topology type and multi-host renderer
  6. Port fleet.dhall from nixos-config as example topology
  7. Multi-node VM test (scheduler + worker in separate NixOS VMs)
  8. Document the topology schema in this appendix

Phase 3 — Auto-scaling
  9. Add AutoscalingConfig + WorkerPool + ScalingPolicy types
 10. Kubernetes manifest renderer (Deployment + HPA from topology)
 11. NixOS fleet controller (systemd timer + scaling script)
 12. Example: 3-pool Kubernetes topology with mixed arch

Phase 4 — Polish
 13. Wire Layer 3 topologies to render from Dhall (not hand-written JSON5)
 14. nix run .#fleet-render — CLI to render any topology to per-host JSON5
 15. Grafana dashboard template derived from topology (auto-panel per pool)
 16. This appendix becomes the reference
```

Phases are independently shippable. Phase 1 delivers value on day one. Phase 2 replaces hand-written multi-node configs. Phase 3 enables cloud-native deployments.
