# The CAS-Backed Registry

The [OCI → CAS bridge](./oci-cas-bridge.md) is the *client* side: pulling
toolchain images from an external registry and projecting them into CAS.
The fork also carries the *server* side: `nativelink-oci-registry`, an
OCI Distribution API service (`oci_registry` in the services config) that
serves push and pull **directly out of the same CAS** — so the registry
stops being a second store with its own storage, GC, and failure modes.

## Why

With the OCI bridge alone, a producer pushes to some external registry
(historically a zot instance backed by object storage) and the bridge
imports from it — meaning every toolchain blob lived twice, and the
"registry" was one more service to operate, wedge, and time out. Since
the CAS's slow tier already *was* the object store, the registry could be
a facade over the CAS like the nix substituter is. Now it is: `skopeo`
and `crane` speak Distribution API to nativelink itself, and the old
standalone registry is retired.

## What it does

- **Dual-digest ingest.** OCI addresses everything by sha256; the CAS is
  BLAKE3-canonical. Pushed blobs land in the CAS under their BLAKE3
  digest and become readable via `ByteStream.Read`
  (`blake3/<hex>/<size>`); a digest-alias index maps the sha256 names
  the Distribution API requires onto the canonical blobs. This is the
  "graded CAS" made concrete: one store, two digest coordinates.
- **Tags as a ref store.** Mutable tag references live in a small ref
  store (the same pattern as the nix cache's signature index), separate
  from the immutable content.
- **Conformance as a check.** The service passes the official OCI
  distribution-spec v1.1.1 conformance suite — including the referrers
  API — as a flake check, because §7 taught us never to self-validate a
  wire format.
- **`oci://self`.** The fetch service short-circuits the reserved
  `oci://self/...` reference to the colocated registry instance, so a
  FetchDirectory of a just-pushed toolchain is an internal projection —
  no network round-trip through a registry endpoint. The producer
  release cycle uses exactly this for its verify leg: push, then have
  the registry reproduce the REAPI root bit-for-bit.

## Configuration

The service takes three stores — the blob store (`cas_store`), the
digest-alias index, and the tag ref store; see the service config in
`nativelink-config/src/cas_server.rs` and the working deployment in
`examples/oci_registry.json5`. The [store catalog](../part3/store-catalog.md)
chapter's composition rules apply unchanged — the registry is a
consumer of stores, not a store.
