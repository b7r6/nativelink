# Debugging Cache Misses

Your cache hit rate is 12%. It should be 95%. Something is different between the two machines computing the same action, and the action hashes are diverging. This chapter is a systematic debugging guide.

## The Debugging Process

Cache misses happen for exactly one reason: the action hash on machine A differs from the action hash on machine B. The action hash is `hash(Command, InputRootDigest, Platform)`. So something in the command, the inputs, or the platform is different.

## Step 1: Identify the Diverging Action

### Bazel

```bash
# On machine A:
bazel aquery //target:name --output=jsonproto > /tmp/actions_a.json

# On machine B:
bazel aquery //target:name --output=jsonproto > /tmp/actions_b.json

# Compare:
diff <(jq '.actions[0]' /tmp/actions_a.json) <(jq '.actions[0]' /tmp/actions_b.json)
```

Alternatively, use Bazel's execution log:

```bash
bazel build //target --execution_log_json_file=/tmp/exec_log.json
```

The execution log records every action with its inputs, command, and platform. Compare logs between machines.

### Buck2

```bash
# Show action details:
buck2 aquery "//target:name" --output-format=json
```

Buck2's `aquery` shows the command, inputs, and platform for each action. Compare outputs across machines.

## Step 2: Identify What's Different

The difference is always in one of three places:

### A. Command Differences

The `Command` proto includes:
- `arguments` (argv)
- `environment_variables`
- `output_paths`
- `working_directory`
- `platform`

Common command divergences:
- **Absolute paths in argv.** `/home/alice/project/...` vs `/home/bob/project/...`. Fix: use relative paths.
- **Environment variable leakage.** `HOME`, `USER`, `TMPDIR` differ between machines. Fix: `--incompatible_strict_action_env` (Bazel) or explicit env filtering.
- **Toolchain path differs.** `/usr/bin/gcc` resolves to different binaries. Fix: use a hermetic toolchain (Part V).
- **Output path ordering.** Glob patterns expand in filesystem order, which may differ. Fix: sort explicitly.

### B. Input Differences

The `InputRootDigest` is the Merkle tree of the action's input files. If any input file differs, the root digest differs.

Common input divergences:
- **Generated files differ.** A genrule that embeds timestamps, hostnames, or random values. Fix: make generators deterministic.
- **Source file encoding.** CRLF vs LF across operating systems. Fix: `.gitattributes` with `* text=auto eol=lf`.
- **Different dependency versions.** Lock files diverge between machines. Fix: commit lock files, ensure deterministic resolution.
- **Toolchain binaries in input tree.** If the toolchain is an input (hermetic approach), different toolchain versions = different inputs. Fix: pin the toolchain version.

### C. Platform Differences

The `Platform` in the action includes `exec_properties` / platform properties. If these differ between machines:
- Different `container-image` value
- Different resource requirements (`cpu_count`)
- Extra/missing properties

## Step 3: Common Root Causes and Fixes

### Cause: Host toolchain leaking into action hash

**Symptom:** Different machines produce different action hashes for the same source.
**Diagnosis:** The action's command references a host-installed binary (`/usr/bin/gcc`) that isn't captured in the action hash.
**Fix:** Use a hermetic toolchain (zig-cc, Nix/LRE, container image).

### Cause: Timestamps in generated code

**Symptom:** Actions that depend on generated files always miss the cache.
**Diagnosis:** A code generator embeds `__DATE__`, `__TIME__`, or uses `Date.now()`.
**Fix:** Strip timestamps from code generators. Use `SOURCE_DATE_EPOCH` for reproducible timestamps.

### Cause: Absolute paths

**Symptom:** Same code, same toolchain, different cache keys between users.
**Diagnosis:** Build tool embeds workspace root (`/home/username/...`) in command args or environment.
**Fix:**
- Bazel: `--incompatible_strict_action_env`, ensure sandboxed execution
- Buck2: use relative paths in rule implementations
- Both: avoid `ctx.workspace_root` in action commands

### Cause: Non-hermetic environment variables

**Symptom:** Cache hits in CI but not locally (or vice versa).
**Diagnosis:** Actions inherit env vars from the shell that differ between environments.
**Fix:**
- Bazel: `--incompatible_strict_action_env` (blocks most env inheritance)
- Buck2: explicit environment in `CommandExecutorConfig`
- Both: audit which env vars the action actually needs

### Cause: Platform property mismatch

**Symptom:** Actions never hit the cache, even when built twice on the same machine.
**Diagnosis:** Platform properties change between invocations (mutable image tag, dynamic value).
**Fix:** Pin all platform property values. Never use `:latest` or unpinned tags.

### Cause: Action cache eviction

**Symptom:** Cache hit rate decreases over time. Old actions always miss.
**Diagnosis:** AC or CAS is too small; entries are evicted before reuse.
**Fix:** Increase store sizes, add durable backend (S3), use `existence_cache` to reduce CAS churn.

## Step 4: Verify the Fix

After fixing the root cause:

```bash
# Build on machine A:
bazel build //target --execution_log_json_file=/tmp/exec_a.json

# Build on machine B:
bazel build //target --execution_log_json_file=/tmp/exec_b.json

# Verify action hashes match:
jq '.[] | .actionKey' /tmp/exec_a.json | sort > /tmp/keys_a
jq '.[] | .actionKey' /tmp/exec_b.json | sort > /tmp/keys_b
diff /tmp/keys_a /tmp/keys_b
# Should be empty (all keys match)
```

For Buck2, compare `buck2 aquery` output:
```bash
buck2 aquery "//..." --output-format=json | jq '.[] | .digest'
```

## The Cache Hit Rate Formula

```
hit_rate = cache_hits / (cache_hits + cache_misses)
```

Target hit rates:
- **< 50%:** Something is fundamentally broken. Probably toolchain mismatch.
- **50-80%:** Partial sharing. Some actions are hermetic, some aren't. Find the non-hermetic ones.
- **80-95%:** Good. Remaining misses are likely cold cache (first build) or genuinely changed inputs.
- **> 95%:** Excellent. You have a well-configured hermetic build.

If you're using NativeLink's `cache_metrics` store wrapper, hit/miss rates are exported as metrics:

```json5
{
  name: "AC_WITH_METRICS",
  cache_metrics: {
    backend: { ref_store: { name: "AC_STORE" } }
  }
}
```

These metrics appear in your Prometheus/Grafana dashboard (see the Observability chapter).
