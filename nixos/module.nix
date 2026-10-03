# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#                                            // hypermodern // nix // nativelink
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# NativeLink remote-execution / remote-cache, OFF BY DEFAULT.
#
# There is NO upstream NixOS module for NativeLink (its flake only ships
# bazelrc-generators), so this is a hand-rolled systemd service around the
# `nativelink` binary from the flake input. The binary takes a single JSON5
# config path argument.
#
# Multi-arch RE: remote execution runs NATIVE binaries, so you need one native
# worker per architecture. The intended topology:
#
#   - role = "monolithic" on an x86_64 host (e.g. ultraviolence): CAS + scheduler
#     + an x86_64 worker. Exposes the public API (50051) and worker_api (50061).
#   - role = "worker"     on shimmer (aarch64 DGX): an aarch64-only worker that
#     dials the monolithic host's worker_api over the tailnet.
#
# ── THE TOOLCHAIN PROBLEM (read this before expecting remote EXECUTION) ──────
#
# Standing up CAS+scheduler+worker (this module) is the easy part. Making remote
# ACTIONS actually execute correctly is the hard part, and it is NOT a server
# concern — it lives in the CLIENT (Buck2/Bazel) repo:
#
#   1. Property matching: the scheduler matches an action's requested platform
#      properties against what a worker advertises (workerProperties +
#      cpu_arch/OSFamily/ISA/container-image/lre-rs, see below). Request a
#      property no worker advertises -> the action never schedules. This routes
#      work; it does NOT make it reproducible.
#
#   2. Hermeticity: a remote worker runs the action's command on ITS filesystem.
#      If the action says `/usr/bin/gcc`, you get the worker's gcc (or none) —
#      non-reproducible or broken. The fix (per NativeLink's LRE docs) is a
#      content-addressed toolchain: every tool referenced is a `/nix/store/...`
#      path. Because our workers ARE NixOS and share /nix/store, a Buck2 toolchain
#      that points at nix-store binaries is hermetic AND cache-shareable: the
#      action hash matches everywhere. A toolchain that references FHS paths is
#      not. THIS is "the trick" with NativeLink.
#
#   3. Buck2 specifics (see configurations/.../README or the host comment):
#      .buckconfig sets engine/action_cache/cas_address + tls=false +
#      instance_name=main (matches `instanceName` here); an ExecutionPlatformInfo
#      with remote_enabled=True; remote_execution_use_case="buck2-default".
#
# This module gets the SERVER right (instance_name, the full property set Buck2
# negotiates, the worker advertisement). The hermetic toolchain is per-repo.
#
# The scheduler matches actions to workers by the platform properties; cpu_arch/
# OSFamily/ISA/container-image/lre-rs are derived from the host, the rest come
# from `workerProperties`.
#
# Build note: NativeLink is not in nixpkgs and builds ~640 derivations from
# source. Add nativelink.cachix.org to nix.settings.substituters before enabling
# on any host, or expect a very long first build.
#
# Nothing here is active until a host sets enable = true, so this module is inert
# and cannot brick a box.
#
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
{
  config,
  lib,
  pkgs,
  nativelinkFlake,
  ...
}: let
  cfg = config.hyper-modern-nixos.nativelink;

  # The RE server runs THIS flake's nativelink (the straylight fork), so its
  # fixes reach the CAS/AC/execution path too — e.g. the
  # transport-error -> Unavailable coding and the widened buf_channel that let
  # multi-GiB toolchain uploads land instead of resetting mid-stream.
  nativelinkPkg =
    if cfg.package != null
    then cfg.package
    else nativelinkFlake.packages.${pkgs.stdenv.hostPlatform.system}.nativelink;

  # Local store root (matches storeRoot in nativelink/fleet.dhall); used by the
  # systemd tmpfiles / service wiring below.
  storeRoot = "/var/lib/nativelink";

  # ── Config source: the TYPED Dhall fleet, rendered at eval via IFD ──────────

  # nativelink/fleet.dhall IS the real program: it computes each host's config as
  # a typed schema.Config and renders it (render.dhall, JSON.render). We render it
  # at eval time via import-from-derivation (enabled fleet-wide; see nix.nix) — no
  # committed out/*.json, no render/check staleness dance, no nixlang config
  # generation. `render-all.dhall` emits [{host, json}]; we select this host's
  # entry by name with jq (the .json is already-rendered JSON text) and write it.
  #
  # buildPackages keeps the render an x86_64 build, so a cross-arch host (aarch64
  # shimmer) doesn't demand an aarch64 dhall build at eval; the config text is
  # host-independent given the host name.
  buildPkgs = pkgs.buildPackages;

  # Content-address JUST the dhall data dir, not a sub-path of flake.self.
  # `flake.self + "/…"` drags the WHOLE flake source into the render's inputs,
  # so every unrelated commit gives the rendered config a new store path and
  # `restartIfChanged` bounces nativelink + nix-cache on every switch. This
  # copy is hashed only by the dhall files, so the config path — and the
  # restart — is stable unless the fleet definition itself changes.
  fleetDir = builtins.path {
    name = "nativelink-fleet-dhall";
    path = cfg.fleetDir;
  };

  # §12 floor-loader worker entrypoint. Standard OCI Toolchain finalizes a
  # toolchain's binaries with PT_INTERP = /lib/ld-std-oci-toolchain.so and a
  # glibc closure under an absolute /cas ("carried, not baked"). A remote worker
  # must bind that loader + closure just-in-time or floor binaries can't exec
  # (exit 127). This wrapper scans the action's inputs for floor cells and, when
  # present, bwraps loader→/lib + cas→/cas (unioned); non-floor actions run
  # unchanged. Mirrors buck2's local forkserver sandbox (service.rs/local.rs).
  # The pinned baseline toolchain the sandbox puts on PATH (remote actions get no
  # usable PATH, and bwrap's fresh root drops the worker's ambient profile).
  floorBaseline = buildPkgs.symlinkJoin {
    name = "nativelink-floor-baseline";
    # tar/gzip/xz: the action-time tools shell out to them — hackage-build
    # untars package sources (observed live as a shelly exec failure: `tar xzf`
    # with no tar on the baseline PATH).
    paths = with buildPkgs; [bash coreutils findutils gnused gnugrep gawk gnutar gzip xz];
  };
  floorEntrypoint = buildPkgs.runCommand "nativelink-floor-entrypoint" {} ''
    install -Dm755 ${./floor-entrypoint.sh} $out/bin/nativelink-floor-entrypoint
    substituteInPlace $out/bin/nativelink-floor-entrypoint \
      --replace '@bash@' ${buildPkgs.bash}/bin/bash \
      --replace '@bwrap@' ${buildPkgs.bubblewrap}/bin/bwrap \
      --replace '@find@' ${buildPkgs.findutils}/bin/find \
      --replace '@basename@' ${buildPkgs.coreutils}/bin/basename \
      --replace '@baseline@' ${floorBaseline}/bin
    ${buildPkgs.bash}/bin/bash -n $out/bin/nativelink-floor-entrypoint
  '';

  # The wrapper every worker action runs through: the operator override
  # (`workerEntrypoint`) when set, else the §12 floor entrypoint above.
  effectiveWorkerEntrypoint =
    if cfg.workerEntrypoint != null
    then cfg.workerEntrypoint
    else "${floorEntrypoint}/bin/nativelink-floor-entrypoint";

  renderedConfig =
    buildPkgs.runCommand "nativelink-${cfg.dhallHost}.json"
    {
      nativeBuildInputs = [
        buildPkgs.dhall-json
        buildPkgs.jq
        buildPkgs.cacert
      ];
      LANG = "C.UTF-8";
      LC_ALL = "C.UTF-8";
      LOCALE_ARCHIVE = "${buildPkgs.glibcLocales}/lib/locale/locale-archive";
      SSL_CERT_FILE = "${buildPkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
    }
    ''
      export XDG_CACHE_HOME="$TMPDIR/dhall-cache"
      export HOME="$TMPDIR"
      mkdir -p "$XDG_CACHE_HOME"
      host=${lib.escapeShellArg cfg.dhallHost}
      json=$(dhall-to-json --file ${fleetDir}/render-all.dhall \
               | jq -e --arg h "$host" '.[] | select(.host==$h) | .json' -r)
      if [ -z "$json" ]; then
        echo "nativelink: host '$host' not found in the Dhall fleet (render-all)" >&2
        exit 1
      fi
      # Inject the §12 floor worker entrypoint into every local executor (the
      # objects carrying a `local.work_directory`); no-op on non-worker hosts.
      # Also validates the JSON is well-formed before accepting it.
      printf '%s' "$json" \
        | jq --arg ep ${lib.escapeShellArg effectiveWorkerEntrypoint} '
            walk(
              if type == "object"
                 and (.local | type) == "object"
                 and (.local | has("work_directory"))
              then .local += { entrypoint: $ep }
              else . end
            )' > "$out"
    '';

  # Config source: operator escape hatch, else the rendered typed-Dhall config.
  configFile =
    if cfg.configFile != null
    then cfg.configFile
    else renderedConfig;

  # ── Nix binary cache (substituter) ──────────────────────────────────────────

  # A SECOND, independent nativelink instance serving the Nix HTTP binary-cache
  # protocol from this flake's nativelink (which carries the
  # nix_cache service upstream lacks). Its config is a trivial 3-store + 1-service
  # JSON5, generated here in Nix — no Dhall fleet involved. Runs alongside the RE
  # service and attic.
  nixCachePkg =
    if cfg.nixCache.package != null
    then cfg.nixCache.package
    else nativelinkFlake.packages.${pkgs.stdenv.hostPlatform.system}.nativelink;

  # The client crate (nl-nix + nl-watch-store), built native-glibc — used for the
  # watch-store auto-push daemon below.
  nixCacheClientPkg =
    nativelinkFlake.packages.${pkgs.stdenv.hostPlatform.system}.nativelink-nix-client;

  # ── nix_cache config: marshal the module options into a Dhall `Params` ──────
  # The store composition (verify / completeness / fast_slow / R2), the R2 store
  # shape, and the service rendering live ONCE in the typed Dhall
  # (data/{schema,render,nix-cache}.dhall) — shared with the RE fleet. Here we
  # only turn the per-host NixOS options into a Dhall record and render it via
  # IFD (below), exactly like the RE config, so the two can't drift.
  # The per-host values as a plain Nix attrset, serialized to JSON. `json-to-dhall`
  # (in the render below) type-checks this JSON against `(nix-cache.dhall).Params`,
  # so the Nix -> Dhall conversion is validated, not string-spliced. `null` maps to
  # the `Optional` fields' `None`. No secrets appear here: render.dhall emits the
  # `${R2_ACCESS_KEY_ID}` placeholders for runtime shellexpand.
  nixCacheParamsJson = builtins.toJSON {
    stateDir = cfg.nixCache.stateDir;
    storeDir = cfg.nixCache.storeDir;
    instanceName = cfg.nixCache.instanceName;
    listen = cfg.nixCache.listen;
    priority = cfg.nixCache.priority;
    maxNarUpload = cfg.nixCache.maxNarUploadBytes;
    maxNarBytes = cfg.nixCache.maxNarBytes;
    fastBytes = cfg.nixCache.fastCacheBytes;
    r2 =
      if cfg.r2.enable
      then {
        account = cfg.r2.accountId;
        bucket = cfg.r2.bucket;
      }
      else null;
    signingKeyFile = cfg.nixCache.signingKeyFile; # null | agenix path
    upstreams =
      map (u: {
        url = u.url;
        trusted_public_keys = u.trustedPublicKeys;
      })
      cfg.nixCache.upstreamCaches;
    fetchProxy =
      if cfg.nixCache.fetchProxy.enable
      then {
        listen = cfg.nixCache.fetchProxy.listen;
        maxFetch = cfg.nixCache.fetchProxy.maxFetchBytes;
        caCert = "${casWitnessDir}/ca.crt";
        caKey = "${casWitnessDir}/ca.key";
      }
      else null;
  };
  nixCacheParamsFile = buildPkgs.writeText "nix-cache-params.json" nixCacheParamsJson;

  # Rendered from the typed Dhall (data/nix-cache.dhall) via IFD — the same
  # mechanism, prelude, and buildPkgs as the RE renderedConfig above. `render`
  # returns the config as JSON text, so we unwrap the string (jq -r) then
  # re-validate it as JSON.
  nixCacheConfigFile =
    buildPkgs.runCommand "nativelink-nix-cache.json"
    {
      nativeBuildInputs = [
        buildPkgs.dhall-json
        buildPkgs.jq
        buildPkgs.cacert
      ];
      LANG = "C.UTF-8";
      LC_ALL = "C.UTF-8";
      LOCALE_ARCHIVE = "${buildPkgs.glibcLocales}/lib/locale/locale-archive";
      SSL_CERT_FILE = "${buildPkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
    }
    ''
      export XDG_CACHE_HOME="$TMPDIR/dhall-cache"
      export HOME="$TMPDIR"
      mkdir -p "$XDG_CACHE_HOME"
      # Type-check the NixOS options (JSON) into the typed Params, then render.
      json-to-dhall "(${fleetDir}/nix-cache.dhall).Params" \
        < ${nixCacheParamsFile} > "$TMPDIR/params.dhall"
      json=$(echo "(${fleetDir}/nix-cache.dhall).render ($TMPDIR/params.dhall)" \
               | dhall-to-json | jq -r .)
      printf '%s' "$json" | jq . > "$out"
    '';

  nixCachePort = lib.last (lib.splitString ":" cfg.nixCache.listen);

  # cas_witness proxy paths + the nix-only CA trust bundle.
  casWitnessDir = "${cfg.nixCache.stateDir}/cas-witness";
  nixCaBundle = "${casWitnessDir}/nix-ca-bundle.crt";
  fetchProxyUrl = "http://${cfg.nixCache.fetchProxy.listen}";
  # Substituter CDNs that must bypass the proxy: store-path substitution is
  # already mirrored (read-through + watch-store), so routing it through the MITM
  # would only churn narinfo/NAR URLs. The local cache (127.0.0.1) bypasses too.
  fetchProxyNoProxy = "127.0.0.1,localhost,::1,cache.nixos.org,nix-community.cachix.org,nix-postgres-artifacts.s3.amazonaws.com";
in {
  options.hyper-modern-nixos.nativelink = {
    enable = lib.mkEnableOption "NativeLink remote execution / cache (off by default)";

    package = lib.mkOption {
      type = lib.types.nullOr lib.types.package;
      default = null;

      defaultText = lib.literalExpression "nativelink flake packages.\${system}.nativelink";

      description = "NativeLink package. Defaults to this flake's nativelink for this host's architecture.";
    };

    fleetDir = lib.mkOption {
      type = lib.types.path;
      description = ''
        Path to the typed Dhall fleet definition (fleet.dhall, schema.dhall,
        render.dhall, nix-cache.dhall, render-all.dhall). The fleet topology is
        the CONSUMER's: this module renders whatever fleet it is pointed at.
      '';
    };

    role = lib.mkOption {
      type = lib.types.enum [
        "monolithic"
        "scheduler"
        "worker"
      ];

      default = "monolithic";

      description = ''
        monolithic = CAS + scheduler + local worker (single x86_64 host).
        scheduler  = CAS + scheduler only (workers dial in from other hosts).
        worker     = local worker only; dials workerApiEndpoint (use on aarch64).
        IGNORED when dhallHost is set (the typed fleet config decides the roles).
      '';
    };

    # ── Dhall fleet config (the source of truth) ────────────────────────────────

    # When set, render this host's NativeLink config from the typed Dhall fleet
    # (nativelink/fleet.dhall) at eval time via IFD. The Dhall fleet encodes the
    # sharded multi-arch topology (scheduler@watchtower, 4-node weighted CAS ring,
    # per-arch workers). See docs/infrastructure/nativelink-production.md.
    dhallHost = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "watchtower";

      description = "Fleet host name whose typed Dhall config to run (null = legacy role generator).";
    };

    instanceName = lib.mkOption {
      type = lib.types.str;
      default = "main";

      description = ''
        RE-API instance name served on the public gRPC services. MUST match the
        client. Buck2's `.buckconfig` uses `instance_name = main` and the
        upstream Buck2 integration test uses "main", so that's the default.
        (Bazel/Buck2 clients send this with every request; a mismatch surfaces
        as instance-name errors and zero cache hits.)
      '';
    };

    # ── Telemetry: OTLP push to the local otel collector ──────────────────────
    # This nativelink build has NO Prometheus /metrics scrape endpoint — the
    # `admin` service only exposes /admin/scheduler/.../set_drain_worker. Metrics
    # (and traces) are emitted via OTLP gRPC, but ONLY when the NL_OTEL_ENDPOINT
    # env var is set (nativelink-util/src/telemetry.rs: no endpoint → no exporter
    # → silent no-op). So without this the dashboards can only count journald log
    # lines. Point it at the host-local otel agent's OTLP receiver and the
    # metrics flow agent → gateway → ClickHouse like every other fleet service.
    otlpEndpoint = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "http://127.0.0.1:4319";
      description = ''
        gRPC OTLP endpoint nativelink pushes metrics/traces to (sets
        NL_OTEL_ENDPOINT). Defaults to the host-local otel agent's OTLP receiver.
        Set to null to disable OTLP export entirely.
      '';
    };

    publicListen = lib.mkOption {
      type = lib.types.str;
      default = "0.0.0.0:50051";

      description = "Public gRPC API (CAS/AC/Execution/ByteStream) listen address.";
    };

    trustedInterfaces = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = ["tailscale0"];

      description = "Interfaces on which openFirewall opens the RE ports. Defaults to tailscale0 (tailnet-only).";
    };

    # ── TLS termination at the public listener ────────────────────────────────

    tls = {
      enable = lib.mkEnableOption "TLS on the public gRPC listener (clients use grpcs://, tls=true)";

      certFile = lib.mkOption {
        type = lib.types.str;
        default = "/var/lib/nativelink-tls/cert.pem";

        description = "Path to the TLS certificate (PEM). Provisioned by tls.tailscale, or supply your own.";
      };

      keyFile = lib.mkOption {
        type = lib.types.str;
        default = "/var/lib/nativelink-tls/key.pem";

        description = "Path to the TLS private key (PEM).";
      };

      tailscale = {
        enable = lib.mkEnableOption "provision+renew the cert via `tailscale cert` for the node's MagicDNS name";

        domain = lib.mkOption {
          type = lib.types.str;
          example = "ultraviolence.example.ts.net";

          description = "MagicDNS name to issue the cert for (must be this node's name; tailnet HTTPS must be enabled).";
        };
      };
    };

    workerApiListen = lib.mkOption {
      type = lib.types.str;
      default = "0.0.0.0:50061";

      description = "Private worker_api listen address (scheduler/monolithic roles).";
    };

    workerApiEndpoint = lib.mkOption {
      type = lib.types.str;
      default = "grpc://127.0.0.1:50061";
      example = "grpc://weyl.example.ts.net:50061";

      description = "Where a `worker` role dials the scheduler. Point this at the monolithic/scheduler host over the tailnet.";
    };

    maxStoreBytes = lib.mkOption {
      type = lib.types.int;
      default = 53687091200; # 50 GiB
      description = "Max bytes for the local-only CAS filesystem store before eviction (used when r2.enable = false).";
    };

    # ── Worker platform properties (the toolchain/RE matching handshake) ───────

    # The scheduler matches an action's requested platform properties against
    # what each worker ADVERTISES here. A Buck2/Bazel action that requests
    # properties the worker doesn't advertise will never schedule; an action
    # that requests nothing runs on whatever worker matches the defaults. For
    # REAL hermeticity the toolchain itself must be content-addressed (Nix
    # store) — these properties only route actions to capable workers, they do
    # not make a non-hermetic toolchain reproducible. See the docs note in the
    # module header / configurations comment.
    workerProperties = lib.mkOption {
      type = lib.types.attrsOf (lib.types.listOf lib.types.str);

      default = {
        cpu_count = ["1"];
        memory_kb = ["1000000"];
        # cpu_arch / OSFamily / container-image / lre-rs / ISA filled below from
        # the host; merged with whatever the host overrides here.
      };

      description = ''
        Platform properties this worker advertises to the scheduler. Keys must
        be a subset of the scheduler's supported_platform_properties. cpu_arch,
        OSFamily, container-image, lre-rs and ISA are set automatically from the
        host; override/extend here (e.g. cpu_count, memory_kb, gpu_model).
      '';
    };

    workerEntrypoint = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;

      description = ''
        Path to a wrapper script the worker runs every action command through
        ("entrypoint script arg..."). When null, a built-in wrapper that puts a
        baseline nix-pinned toolchain (coreutils/bash/findutils/sed/grep/awk) on
        PATH is used — required because remote actions run with NO inherited
        PATH and otherwise fail on bare `mkdir`/`sh` in the prelude scaffolding.
        Override to point at your own LRE toolchain wrapper.
      '';
    };

    # ── Local fast-cache sizing (fronting R2 when r2.enable) ──────────────────

    localCacheBytes = lib.mkOption {
      type = lib.types.int;
      default = 274877906944; # 256 GiB

      description = ''
        Size of the local NVMe filesystem fast tier that fronts R2 (per content
        store). Default 256 GiB; override down on space-constrained hosts.
      '';
    };

    memoryCacheBytes = lib.mkOption {
      type = lib.types.int;
      default = 34359738368; # 32 GiB
      description = ''
        Size of the in-memory index/hot tier fronting R2. Default 32 GiB;
        override down on memory-constrained hosts.
      '';
    };

    # ── R2 (S3-compatible) backing store ──────────────────────────────────────

    # When enabled, the CAS (dedup index + content) and AC stores become
    # fast_slow: a local filesystem/memory fast tier in front of R2 as the
    # durable slow tier. Credentials are read from `r2.environmentFile` via
    # shellexpand (${R2_ACCESS_KEY_ID}/${R2_SECRET_ACCESS_KEY}) so they never
    # enter the nix store; account_id/bucket are not secret.
    r2 = {
      enable = lib.mkEnableOption "back the CAS/AC stores with Cloudflare R2 (local fast tier + R2 slow tier)";

      accountId = lib.mkOption {
        type = lib.types.str;

        default = "";
        example = "6063b6652178f5cf1cfb87e7e41acf1e";

        description = "Cloudflare account ID (endpoint derives from it).";
      };

      bucket = lib.mkOption {
        type = lib.types.str;

        default = "";
        example = "straylight-nativelink-cas";

        description = "R2 bucket for the CAS/AC backend. nativelink namespaces within it via key_prefix.";
      };

      environmentFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;

        default = null;
        example = "/run/agenix/nativelink-r2-env";

        description = ''
          Env file defining R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY, fed to the
          service and referenced by the config via shellexpand. NEVER a store path.
        '';
      };
    };

    configFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;

      default = null;

      description = "Override the generated JSON5 config with your own file.";
    };

    openFirewall = lib.mkOption {
      type = lib.types.bool;

      default = false;

      description = "Open the public/worker_api ports in the firewall (if enabled).";
    };

    # ── Nix binary cache (substituter) ─────────────────────────────────────────
    # An independent nativelink instance serving the Nix HTTP binary-cache
    # protocol (nix_cache) from the straylight fork. Enable per-host; it runs
    # alongside the RE service (`enable`) and attic. Off by default.
    nixCache = {
      enable = lib.mkEnableOption "NativeLink Nix binary-cache substituter (off by default)";

      package = lib.mkOption {
        type = lib.types.nullOr lib.types.package;
        default = null;

        defaultText = lib.literalExpression "nativelink flake packages.\${system}.nativelink";

        description = "Package providing the nix_cache-capable nativelink binary. Defaults to the straylight fork input.";
      };

      listen = lib.mkOption {
        type = lib.types.str;
        default = "0.0.0.0:50071";

        description = "Nix binary-cache HTTP listen address. Cache root is http://<host>:<port>/nix/<instanceName>. Bound on all interfaces; exposure is gated at the firewall (trustedInterfaces).";
      };

      instanceName = lib.mkOption {
        type = lib.types.str;
        default = "main";
        description = "Mount instance; the cache root path is /nix/<instanceName>.";
      };

      stateDir = lib.mkOption {
        type = lib.types.str;
        default = "/var/lib/nativelink-nix-cache";

        description = "Directory holding the NAR / path-info / alias filesystem stores.";
      };

      storeDir = lib.mkOption {
        type = lib.types.str;
        default = "/nix/store";
        description = "StoreDir served in nix-cache-info; must match clients' store dir.";
      };

      priority = lib.mkOption {
        type = lib.types.int;
        default = 40;
        description = "Substituter priority served in nix-cache-info (lower = tried earlier).";
      };

      maxNarBytes = lib.mkOption {
        type = lib.types.int;
        default = 214748364800; # 200 GiB

        description = ''
          Eviction cap for the NAR store ONLY when R2 is disabled (the store is
          then a local-only filesystem CAS, so this is its whole capacity and
          evicting means LOSING the mirror). When r2.enable is set the NAR store
          is a fast_slow over R2 and this is ignored — `fastCacheBytes` bounds the
          local tier instead, while R2 holds everything durably.
        '';
      };

      fastCacheBytes = lib.mkOption {
        type = lib.types.int;
        default = 68719476736; # 64 GiB

        description = ''
          Eviction cap for the LOCAL fast tier of the R2-backed NAR store (used
          when r2.enable). Bounds local disk so the cache can never fill the box;
          anything evicted from the fast tier is still durable in R2, so eviction
          here costs a re-fetch from R2, never the data. Keep it comfortably below
          free disk.
        '';
      };

      maxNarUploadBytes = lib.mkOption {
        type = lib.types.int;
        default = 34359738368; # 32 GiB (the fork's own default)

        description = ''
          Per-upload cap: an upload whose UNCOMPRESSED NAR exceeds this is
          rejected with HTTP 413. It also bounds the spool when decompressing a
          compressed upload, so it doubles as a decompression-bomb guard — which
          is why the default is a conservative 32 GiB. Raise it (well under
          maxNarBytes) to accept genuinely large artifacts. Note: `-g3` debug
          builds produce enormous NARs (60-70+ GiB per output) that need this
          raised; the more durable fix is to shrink those artifacts (`-g`,
          separateDebugInfo, stripping).
        '';
      };

      signingKeyFile = lib.mkOption {
        type = lib.types.nullOr lib.types.str;

        default = null;
        example = "/run/agenix/nativelink-nix-cache-key";

        description = ''
          Path to a `nix key generate-secret` secret key. When set, the server
          signs every narinfo with it and clients trust the matching public key.
          When null, narinfos are UNSIGNED — pullers then need require-sigs =
          false (fine for a tailnet-internal workout cache). Provide via agenix;
          NEVER a store path.
        '';
      };

      trustedInterfaces = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = ["tailscale0"];

        description = "Interfaces on which openFirewall opens the cache port. Tailnet-only by default.";
      };

      openFirewall = lib.mkOption {
        type = lib.types.bool;
        default = true;

        description = "Open the cache listen port, but only on trustedInterfaces (tailscale0 by default).";
      };

      watchStore = lib.mkOption {
        type = lib.types.bool;
        default = false;

        description = ''
          Run the `nl-watch-store` daemon (from the client crate): a fanotify
          watcher on the store that streams every newly-committed path into this
          cache over loopback, zstd-compressed. Unlike a nix post-build-hook it
          is decoupled from builds — it never blocks a build's completion and
          catches paths however they arrive (built or substituted), so the local
          cache becomes a full mirror of this host's closure. `watchConcurrency`
          bounds its parallel pushes so a big substitution can't saturate the box.
        '';
      };

      watchConcurrency = lib.mkOption {
        type = lib.types.ints.positive;
        default = 4;

        description = ''
          Maximum concurrent path pushes for the `watchStore` daemon. Kept
          conservative because the watcher mirrors every committed path,
          including bursts substituted from upstream during a large rebuild —
          this is the knob that keeps that from saturating disk/CPU. Raise it on
          a dedicated builder with headroom.
        '';
      };

      reconcileInterval = lib.mkOption {
        type = lib.types.str;
        default = "1h";

        description = ''
          systemd interval for a complete Nix DB → cache reconciliation. This
          heals paths that predate the event watcher, missed events, malformed
          historical narinfos, and all outputs of multi-output derivations.
        '';
      };

      upstreamCaches = lib.mkOption {
        type = lib.types.listOf (
          lib.types.submodule {
            options = {
              url = lib.mkOption {
                type = lib.types.str;
                description = "Upstream Nix binary-cache base URL.";
              };
              trustedPublicKeys = lib.mkOption {
                type = lib.types.listOf lib.types.str;
                description = "Public keys this upstream's narinfos must verify against.";
              };
            };
          }
        );
        default = [];

        example = [
          {
            url = "https://cache.nixos.org";
            trustedPublicKeys = ["cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY="];
          }
        ];

        description = ''
          Upstream caches to read through on a local narinfo miss. On a miss the
          server fetches the path from the first upstream that has it, verifies
          its signature against `trustedPublicKeys` BEFORE storing anything,
          mirrors the NAR into the CAS, and serves it — so an upstream path is
          fetched once and then served locally forever. Each entry MUST list at
          least one key (read-through refuses an unverifiable narinfo, so this
          can never be a cache-poisoning vector). Empty disables read-through.
        '';
      };

      # ── cas_witness: fetchurl-granularity mirror (the raw-fetch gap) ──────────
      fetchProxy = {
        enable = lib.mkOption {
          type = lib.types.bool;
          default = false;

          description = ''
            Run the `cas_witness` caching HTTP forward proxy in the SAME
            nativelink process, on `listen`. It TLS-intercepts (MITM) a client's
            fetches and mirrors every fetched body into the nix_cache's NAR CAS,
            so a fixed-output derivation's download (fetchurl/fetchTarball) is
            fetched from upstream once and served from the CAS forever — the
            raw-URL gap the store-path substituter can't cover. Safe because nix
            re-hashes every fixed-output derivation regardless of what the proxy
            serves (trusted for availability, never integrity). Requires
            nixCache.enable (shares the process and the CAS).
          '';
        };

        listen = lib.mkOption {
          type = lib.types.str;
          default = "127.0.0.1:50080";

          description = ''
            Proxy listen address. Loopback by default: the proxy is an
            UNAUTHENTICATED MITM, so it must only be reachable by the local build
            clients it serves. This host's nix-daemon points HTTP(S)_PROXY here.
          '';
        };

        maxFetchBytes = lib.mkOption {
          type = lib.types.int;
          default = 10737418240; # 10 GiB

          description = ''
            Largest response body the proxy caches; a larger one is streamed
            through uncached. Raised above the fork's 2 GiB default so large
            release archives still get mirrored.
          '';
        };

        wireNixDaemon = lib.mkOption {
          type = lib.types.bool;
          default = true;

          description = ''
            Point this host's nix-daemon at the proxy: set HTTP(S)_PROXY to
            `listen`, trust the proxy's generated CA via a nix-scoped
            NIX_SSL_CERT_FILE bundle (the MITM CA is trusted ONLY by nix's own
            fetches, never the system trust store), and no_proxy the substituter
            CDNs so store-path substitution bypasses the proxy. Turn off to run
            the proxy for external clients only.
          '';
        };
      };
    };
  };

  config = lib.mkMerge [
    (lib.mkIf cfg.enable {
      assertions = [
        {
          assertion = cfg.role != "worker" || cfg.workerApiEndpoint != "grpc://127.0.0.1:50061";
          message = "nativelink role=worker but workerApiEndpoint still points at localhost. Set it to the scheduler host's worker_api over the tailnet.";
        }
        {
          assertion =
            !cfg.r2.enable || (cfg.r2.accountId != "" && cfg.r2.bucket != "" && cfg.r2.environmentFile != null);
          message = ''
            hyper-modern-nixos.nativelink.r2.enable is true but accountId/bucket/
            environmentFile are not all set. Provide r2.accountId, r2.bucket, and
            r2.environmentFile (with R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY).
          '';
        }
        {
          assertion = !cfg.tls.tailscale.enable || cfg.tls.tailscale.domain != "";
          message = "nativelink.tls.tailscale.enable is true but tls.tailscale.domain (the node's MagicDNS name) is unset.";
        }
      ];

      # Provision + renew the listener cert via `tailscale cert`. Writes to the
      # configured cert/key paths (root:nativelink-readable), and a daily timer
      # refreshes before the 90-day Let's Encrypt expiry. nativelink waits on it.
      systemd.services.nativelink-tls-cert = lib.mkIf cfg.tls.tailscale.enable {
        description = "Provision nativelink TLS cert via tailscale (${cfg.tls.tailscale.domain})";
        after = ["tailscaled.service"];
        wants = ["tailscaled.service"];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''
          set -euo pipefail
          certdir="$(dirname ${lib.escapeShellArg cfg.tls.certFile})"
          ${pkgs.coreutils}/bin/mkdir -p "$certdir"
          # Retry until tailscale DNS/cert provisioning is ready.
          for i in $(seq 1 30); do
            if ${config.services.tailscale.package}/bin/tailscale cert \
                 --cert-file ${lib.escapeShellArg cfg.tls.certFile} \
                 --key-file ${lib.escapeShellArg cfg.tls.keyFile} \
                 ${lib.escapeShellArg cfg.tls.tailscale.domain}; then
              break
            fi
            echo "tailscale cert not ready yet ($i/30), retrying in 10s..."
            ${pkgs.coreutils}/bin/sleep 10
          done
          # nativelink runs as DynamicUser=no/root here, so root-readable is fine;
          # widen if you later run nativelink as a dedicated user.
          ${pkgs.coreutils}/bin/chmod 0640 ${lib.escapeShellArg cfg.tls.keyFile}
        '';
      };

      systemd.timers.nativelink-tls-cert = lib.mkIf cfg.tls.tailscale.enable {
        wantedBy = ["timers.target"];
        timerConfig = {
          OnCalendar = "daily";
          Persistent = true;
          RandomizedDelaySec = "1h";
        };
      };

      systemd.services.nativelink = {
        description = "NativeLink remote execution (${cfg.role})";
        wantedBy = ["multi-user.target"];
        after =
          [
            "network-online.target"
          ]
          ++ lib.optional cfg.tls.tailscale.enable "nativelink-tls-cert.service";
        wants =
          [
            "network-online.target"
          ]
          ++ lib.optional cfg.tls.tailscale.enable "nativelink-tls-cert.service";
        # OTLP push target (metrics + traces). Empty list when otlpEndpoint is
        # null, so export stays off unless a collector is wired up.
        environment = lib.optionalAttrs (cfg.otlpEndpoint != null) {
          NL_OTEL_ENDPOINT = cfg.otlpEndpoint;
        };
        serviceConfig = {
          ExecStart = "${nativelinkPkg}/bin/nativelink ${configFile}";
          # Self-heal ANY exit — a clean-exit crash (e.g. the R2 clock-skew
          # abort) must not leave a fleet node dark until a human notices.
          Restart = "always";
          RestartSec = 5;
          # The CAS fast tier holds one fd per hot blob plus the gRPC fan-out;
          # pin a durable ceiling instead of relying on a runtime prlimit.
          LimitNOFILE = 524288;
          StateDirectory = "nativelink";
          # R2 creds for shellexpand in the config (${R2_ACCESS_KEY_ID} etc.).
          EnvironmentFile = lib.mkIf (cfg.r2.enable && cfg.r2.environmentFile != null) cfg.r2.environmentFile;
          # Workers exec arbitrary build actions in work_directory, so we cannot
          # apply the strict sandbox we'd use for a pure cache. Keep it modest.
          NoNewPrivileges = lib.mkDefault (cfg.role == "scheduler");
        };
      };

      systemd.tmpfiles.rules = ["d ${storeRoot} 0750 root root - -"];

      # Open the public + worker_api ports, but ONLY on the trusted (tailscale)
      # interfaces — the RE endpoint is tailnet-reachable, never internet-exposed,
      # even though publicListen binds all interfaces.
      networking.firewall = lib.mkIf cfg.openFirewall (
        let
          # dhallHost: the typed fleet uses fixed ports — public 50051, CAS 50052,
          # worker_api 50061. Open all three on the trusted interfaces (every node
          # runs a CAS server the shard ring grpc's to; the scheduler also serves
          # public + worker_api). Legacy path opens publicListen + workerApiListen.
          ports =
            if cfg.dhallHost != null
            then [
              50051
              50052
              50061
            ]
            else [
              (lib.toInt (lib.last (lib.splitString ":" cfg.publicListen)))
              (lib.toInt (lib.last (lib.splitString ":" cfg.workerApiListen)))
            ];
        in {
          interfaces = lib.genAttrs cfg.trustedInterfaces (_: {
            allowedTCPPorts = ports;
          });
        }
      );
    })

    # ── Nix binary-cache substituter (independent of the RE service above) ──────

    (lib.mkIf cfg.nixCache.enable {
      systemd.tmpfiles.rules = ["d ${cfg.nixCache.stateDir} 0750 root root - -"];

      systemd.services.nativelink-nix-cache = {
        description = "NativeLink Nix binary-cache substituter";

        wantedBy = ["multi-user.target"];
        after = ["network-online.target"];
        wants = ["network-online.target"];

        serviceConfig = {
          ExecStart = "${nixCachePkg}/bin/nativelink ${nixCacheConfigFile}";
          Restart = "on-failure";
          RestartSec = 5;
          StateDirectory = "nativelink-nix-cache";
          # R2 creds for shellexpand (${R2_ACCESS_KEY_ID} / ${R2_SECRET_ACCESS_KEY})
          # in the R2-backed NAR store; same env file the RE service uses.
          EnvironmentFile = lib.mkIf (cfg.r2.enable && cfg.r2.environmentFile != null) cfg.r2.environmentFile;
        };
      };

      # Substitute from this host's own cache first — the client side of the
      # loopback push above, and the drop-in for the old attic replica's
      # mkBefore injection. Its public key is trusted fleet-wide (nix.nix).
      nix.settings.substituters = lib.mkBefore [
        "http://127.0.0.1:${nixCachePort}/nix/${cfg.nixCache.instanceName}"
      ];

      # Open the cache port only on the trusted (tailscale) interfaces.
      networking.firewall = lib.mkIf cfg.nixCache.openFirewall {
        interfaces = lib.genAttrs cfg.nixCache.trustedInterfaces (_: {
          allowedTCPPorts = [(lib.toInt nixCachePort)];
        });
      };

      # Populate the cache with the nl-watch-store daemon: a fanotify watcher
      # that streams every newly-committed store path into the cache over
      # loopback, decoupled from the build (no post-build-hook fork, never blocks
      # a build). Bounded by watchConcurrency so it can't saturate the box.
      systemd.services.nativelink-nix-cache-watch = lib.mkIf cfg.nixCache.watchStore {
        description = "Auto-push new store paths to the local NativeLink nix_cache";

        wantedBy = ["multi-user.target"];
        after = [
          "network-online.target"
          "nativelink-nix-cache.service"
        ];
        wants = ["network-online.target"];

        serviceConfig = {
          ExecStart = lib.escapeShellArgs (
            [
              "${nixCacheClientPkg}/bin/nl-watch-store"
              "--to"
              "http://127.0.0.1:${nixCachePort}/nix/${cfg.nixCache.instanceName}"
              "--store"
              cfg.nixCache.storeDir
              "--concurrency"
              (toString cfg.nixCache.watchConcurrency)
            ]
            ++ lib.optionals (cfg.nixCache.signingKeyFile != null) [
              "--signing-key"
              cfg.nixCache.signingKeyFile
            ]
          );
          Restart = "on-failure";
          RestartSec = 5;

          # fanotify (FAN_MARK on the store dir) needs CAP_SYS_ADMIN. Runs as root
          # to read the store, db.sqlite, and the signing key; grant only that cap.
          AmbientCapabilities = ["CAP_SYS_ADMIN"];
          CapabilityBoundingSet = ["CAP_SYS_ADMIN"];
        };
      };

      systemd.services.nativelink-nix-cache-reconcile = lib.mkIf cfg.nixCache.watchStore {
        description = "Reconcile every valid local Nix path into NativeLink nix_cache";
        after = ["nativelink-nix-cache.service"];
        requires = ["nativelink-nix-cache.service"];

        serviceConfig = {
          Type = "oneshot";
          ExecStartPre = pkgs.writeShellScript "wait-for-nativelink-nix-cache" ''
            for attempt in $(seq 1 60); do
              if ${lib.getExe pkgs.curl} --fail --silent --show-error \
                "http://127.0.0.1:${nixCachePort}/nix/${cfg.nixCache.instanceName}/nix-cache-info" \
                >/dev/null; then
                exit 0
              fi
              sleep 1
            done
            echo "NativeLink nix_cache did not become ready within 60 seconds" >&2
            exit 1
          '';
          ExecStart = lib.escapeShellArgs (
            [
              "${nixCacheClientPkg}/bin/nl-nix"
              "--to"
              "http://127.0.0.1:${nixCachePort}/nix/${cfg.nixCache.instanceName}"
              "--store"
              cfg.nixCache.storeDir
              "--jobs"
              (toString cfg.nixCache.watchConcurrency)
            ]
            ++ lib.optionals (cfg.nixCache.signingKeyFile != null) [
              "--signing-key"
              cfg.nixCache.signingKeyFile
            ]
            ++ ["reconcile"]
          );
        };
      };

      systemd.timers.nativelink-nix-cache-reconcile = lib.mkIf cfg.nixCache.watchStore {
        description = "Periodic NativeLink Nix cache completeness gate";
        wantedBy = ["timers.target"];
        timerConfig = {
          OnBootSec = "2m";
          OnUnitActiveSec = cfg.nixCache.reconcileInterval;
          Persistent = true;
          Unit = "nativelink-nix-cache-reconcile.service";
        };
      };
    })

    # ── cas_witness proxy: route this host's nix through it ──────────────────
    (lib.mkIf (cfg.nixCache.enable && cfg.nixCache.fetchProxy.enable && cfg.nixCache.fetchProxy.wireNixDaemon) {
      systemd.tmpfiles.rules = [
        # 0755 (not 0750): unprivileged nix build users (nixbld*) must be able to
        # traverse in and read nix-ca-bundle.crt so Go's `go mod download` — which
        # honours SSL_CERT_FILE, not NIX_SSL_CERT_FILE — trusts the MITM proxy CA.
        # The interception CA *cert* is public by design; ca.key stays 0600 root.
        "d ${casWitnessDir} 0755 root root - -"
        # Seed the nix CA bundle with the system CAs so nix TLS never breaks even
        # before the proxy CA exists; the oneshot below rewrites it with both.
        "C ${nixCaBundle} 0644 root root - ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
      ];

      # Assemble a nix-ONLY trust bundle = system CAs + the proxy's generated CA,
      # once the proxy has minted it. NIX_SSL_CERT_FILE points here, so only nix
      # trusts the interception CA — never the system trust store.
      systemd.services.nativelink-cas-witness-trust = {
        description = "Assemble the nix CA bundle trusting the cas_witness proxy CA";
        wantedBy = ["multi-user.target"];
        after = ["nativelink-nix-cache.service"];
        wants = ["nativelink-nix-cache.service"];
        serviceConfig = {
          Type = "oneshot";
          RemainAfterExit = true;
        };
        script = ''
          for _ in $(seq 1 60); do
            [ -f ${casWitnessDir}/ca.crt ] && break
            sleep 1
          done
          if [ -f ${casWitnessDir}/ca.crt ]; then
            cat ${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt ${casWitnessDir}/ca.crt > ${nixCaBundle}.tmp
            mv -f ${nixCaBundle}.tmp ${nixCaBundle}
          fi
        '';
      };

      # Route nix's fetches through the proxy (fixed-output derivations inherit
      # these via impureEnvVars), trusting the proxy CA via the nix-only bundle.
      # Substituter CDNs bypass via no_proxy, so only raw FOD fetches are teed.
      systemd.services.nix-daemon = {
        after = ["nativelink-cas-witness-trust.service"];
        wants = ["nativelink-cas-witness-trust.service"];
        environment = {
          http_proxy = fetchProxyUrl;
          https_proxy = fetchProxyUrl;
          HTTP_PROXY = fetchProxyUrl;
          HTTPS_PROXY = fetchProxyUrl;
          no_proxy = fetchProxyNoProxy;
          NO_PROXY = fetchProxyNoProxy;
          NIX_SSL_CERT_FILE = lib.mkForce nixCaBundle;
        };
      };
    })
  ];
}
