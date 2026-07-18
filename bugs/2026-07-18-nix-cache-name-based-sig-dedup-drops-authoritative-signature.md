# nix_cache re-sign dedup keys on signature NAME, so a foreign same-name signature suppresses the cache's own authoritative signature → substituter untrusted, clients rebuild from source

- **Date:** 2026-07-18
- **Reporter:** b7r6
- **Component:** `nativelink-service` (`nix_cache_server.rs`, the re-sign dedup filter at `:1389` (upstream ingest) and `:1846` (client push/narinfo PUT))
- **Repo state:** branch `nix-cache`, `8fe8f2ed`
- **Topology:** client `nix develop` on `ultraviolence` → local substituter `http://127.0.0.1:50071/nix/main` (`nix_cache`, `store_dir=/nix/store`, `signing_key_files=[/run/agenix/nativelink-nix-cache-key]`). Fleet signing key is a single shared agenix secret (`mkGlobalSecret`); its public half is `nativelink-nix-cache-1:ccYfraJDD/wVIFzw6LJ7psrYahwv4Wztad4XHJcdG4M=`, present in every host's `trusted-public-keys`.
- **Severity:** High for cache usefulness — a poisoned narinfo is served with an **untrusted** signature, so `nix` ignores the substitute and **rebuilds from source**. Silent: only a `warning:` line, then a full local build.

## Summary

`nix develop .#` on `ultraviolence` refuses the local substituter and rebuilds
a large Haskell/WASM closure from source. The tell:

```
warning: ignoring substitute for '/nix/store/…-buck2-unstable-b545db3'
  from 'http://127.0.0.1:50071/nix/main', as it's not signed by any of the
  keys in 'trusted-public-keys'
```

The narinfo **is** signed and the client **does** trust a key of that name —
yet it is rejected:

```
$ curl -s .../mqda5346….narinfo | grep Sig
Sig: nativelink-nix-cache-1:u7K62TqP0zq1G0NKJJarB6pbLFTPAOlXuJ+Nt+h/W45oj…

$ grep trusted-public-keys /etc/nix/nix.conf
… nativelink-nix-cache-1:ccYfraJDD/wVIFzw6LJ7psrYahwv4Wztad4XHJcdG4M= …
```

Same key **name** (`nativelink-nix-cache-1`), different **keypair**
(`u7K62…` signature does not verify against the trusted `ccYf…` public key).
The served narinfo carries **only** the foreign `u7K62` signature — the cache's
own `ccYf` signature is **absent**, even though `signing_key_files` is
configured with exactly that key.

## Root cause

Both re-sign sites decide whether to add the cache's signature by comparing the
existing signatures' **key name** against the configured key's name, instead of
checking whether the configured key has actually signed this fingerprint.

`nix_cache_server.rs:1843` (client push / `PUT …narinfo`; identical logic at
`:1386`, upstream ingest):

```rust
let new_sigs: Vec<String> = instance
    .signing_keys
    .iter()
    .filter(|key| {
        !path_info
            .signatures
            .iter()
            .any(|sig| sig.split(':').next() == Some(key.name()))  // ← NAME match
    })
    .map(|key| key.sign(&fingerprint))
    .collect();
path_info.signatures.extend(new_sigs);
```

Nix signature names are **not** unique to a keypair — they are an arbitrary
label chosen by whoever generated the key. The fleet's canonical cache key and
some *other* signer (a build host's `nix.conf` `secret-key-files`, or a
pre-regeneration copy of the secret) can both be named `nativelink-nix-cache-1`
while holding different Ed25519 keypairs. When a narinfo already bears the
foreign `nativelink-nix-cache-1:u7K62…` signature, `sig.split(':').next() ==
Some(key.name())` is **true**, so the filter drops the cache's key and the
authoritative `ccYf…` signature is never appended. The record is stored — and
subsequently served on GET verbatim — with only the untrusted signature.

The design intent ("sign the nix_cache, trust it fleet-wide", commits `3a443fb`
/ `564ea4d`) is that the cache stamps its own trusted signature on everything it
serves. Name-based dedup silently defeats that whenever an inbound signature
collides on name.

## Expected vs. actual

- **Expected:** the served narinfo always carries a signature that verifies
  against the cache's configured key (`ccYf…`), regardless of any other
  signatures present; clients using the fleet `trusted-public-keys` accept the
  substitute.
- **Actual:** a name-colliding foreign signature suppresses the cache's own
  signature; the narinfo is served signed only by an untrusted key; clients
  reject it and rebuild from source.

## Fix

Dedup on *cryptographic identity*, not on the name label. Sign with each
configured key, then append only signatures not already present verbatim —
Ed25519 (RFC 8032) signatures over the same fingerprint are deterministic, so a
key that already signed reproduces its exact existing signature and is naturally
deduped, while a foreign same-name signature never matches and no longer
suppresses ours:

```rust
let new_sigs: Vec<String> = instance
    .signing_keys
    .iter()
    .map(|key| key.sign(&fingerprint))
    .filter(|our_sig| !path_info.signatures.contains(our_sig))
    .collect();
path_info.signatures.extend(new_sigs);
```

Apply at both `:1386` (upstream ingest) and `:1843` (client push). Equivalent
alternative: keep the filter shape but test `key.verify(&fingerprint, sig)`
(skip only if *this* key already produced a verifying signature).

### Already-poisoned records

The two sites above only run on ingest/push, and GET serves the stored record
verbatim, so records already stored signed only by `u7K62…` stay untrusted
until re-pushed. Two options:
1. **Re-push** the affected closures (`nl-nix push` / `nix copy`) after the fix
   — each PUT now stamps `ccYf…`.
2. **Also stamp on GET** — re-sign the stored `NixPathInfo` with the configured
   keys when serving a narinfo, so existing records self-heal on first fetch
   (adds per-request Ed25519 signing cost; the fingerprint is already computed
   for verification).

## Prevention

- The foreign `u7K62…` signer should be identified: some host is signing pushes
  with a `nativelink-nix-cache-1`-named key that is **not** the shared agenix
  secret (or the secret was regenerated without re-pushing). Fleet pushers
  should use the shared key, or the cache's re-sign (post-fix) makes their label
  irrelevant.
- Consider warning once when an inbound signature shares a configured key's name
  but fails to verify against it — that is exactly the collision that used to be
  silent.

## Cross-references

- Re-sign sites: `nix_cache_server.rs:1386` (upstream ingest, comment "add this
  instance's own for every key that has not already signed this fingerprint"),
  `:1843` (client push, comment "add ours for any configured key that has not
  signed this fingerprint yet").
- Verify helper already used for upstream trust: `nix_cache_server.rs:1319`
  (`key.verify(&fingerprint, sig)` over `upstream.public_keys`).
- Sibling bring-up interop bugs:
  `2026-07-18-execute-rejects-unset-digest-function-blocks-sha256-reference-clients.md`,
  `2026-07-18-cas-write-surfaces-unknown-transport-error-instead-of-unavailable.md`.
