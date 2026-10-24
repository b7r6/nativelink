# Canonical filesystem boundary review receipt

This independent review examined author commit
`a967496c59ae8890e4be018bce5be13d71a3056a` without modifying its code.
The exact library SHA-256 was
`e64d69404346b2520e19a06e84f3a57c94a3e02d2242362ebf21f7249ce880d1`.
The follow-up changes documentation and API comments only; these witnesses
record the original implementation and its current limits.

## Observed boundaries

- **Inode reuse:** claim four empty roots, drop the guard, save the CAS record,
  delete that record and directory, recreate the directory, and restore the
  saved record. On this local filesystem the first allocation reused device
  `2050`, inode `8009987`; reopen accepted. This was a stopped namespace, not a
  concurrent lock bypass. Device/inode equality is not a persistent physical
  generation proof. Replacement, restoration, cloning and relocation remain
  runtime integration blockers without stronger reviewed evidence.
- **Final sync failures:** `strace` injected `EIO` on `fsync` call 7 (last record)
  and call 8 (last directory). Claim returned `sync_record` and `sync_directory`
  respectively. All four records remained readable, and reopen accepted both
  states without rewriting them. Matching readable records cannot prove a
  historical successful sync sequence. Reopen performs no sync; durable recovery
  remains unqualified.

The 14 executed cases also inject errors at the other six sync sites and all
four writes (`ENOSPC`), and replace a record with a FIFO. Earlier failures retain
partial evidence and refuse reopen. Cold retry refuses every failed-claim
state. FIFO open returns promptly and rejects the FIFO. Every reopen
leaves record bytes unchanged. Exact per-case results are in `results.json.gz`;
all twelve injected-error syscall traces are retained.

## Reproduction and archive

Every archived file is gzip-compressed with a reproducible header. `manifest.json`
contains hashes of both its original and compressed bytes. It includes the
original source freeze, the external Cargo harness and lock, the 14-case Python
runner, full results, syscall traces, and review metadata. No credential file or
private key was opened or archived; manifest fixtures contain example paths.

After extracting each `.gz` file to its corresponding path in a scratch
folder, build the frozen harness with:

```sh
CARGO_TARGET_DIR=/tmp/nativelink-authority-namespace-target cargo build --manifest-path harness/Cargo.toml --locked --offline --profile smol
python3 run.py
```

The runner's `BIN` constant uses that target directory. The original run used
Linux and the local `/tmp` filesystem. Inode allocation is filesystem-dependent;
another machine need not reproduce the same allocation count or identity.
The runner reports no inode reuse if its bounded 10,000 allocation attempts do
not observe it. These are syscall-error experiments, not power-loss tests.

The evidence does not qualify mount topology, hostile concurrent mutation,
network filesystems, runtime store construction, journal recovery, process or
worker admission, old-domain fencing, TLS exclusion, or complete M4a cutover.
The namespace helper still has no runtime consumer. No recovery semantics were
changed to accommodate these witnesses.
