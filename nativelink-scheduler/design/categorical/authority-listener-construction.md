# Fixed authority listener construction

`nativelink-service/src/authority_listener.rs` constructs the experimental
authority with existing service primitives. It is used by local tests; the
production binary and JSON5 configuration do not select it.

`AuthorityListeners::start` consumes an opened `Singleton` and the existing
`QualificationConfig`. The coordinator, CAS, ByteStream and ActionCache services
receive the same declared stores. It returns the coordinator task and two opaque
listener handles with fixed routes:

| Listener | Services |
| --- | --- |
| Worker | AuthorityWorkerApi, CAS, ByteStream |
| Client | Execution, ActionCache, CAS, ByteStream, Capabilities |

The handles expose only `serve`, accepting an existing tonic transport builder,
bound TCP listener and shutdown future. They do not expose a route builder or a
way to add ActionCache to the worker handle. Both roles share the object service
instances, so clients can upload inputs and read outputs through their own
listener. No legacy scheduler is constructed. The additive
`CapabilitiesServer::new_authority` advertises execution, SHA256 and BLAKE3, and
writable client ActionCache, with no optional node properties, compression or
chunking. The existing configurable capabilities constructor is unchanged.
Operations cancellation/listing is not mounted. Existing capabilities behavior
for unknown instances is unchanged and is not a strict instance admission gate.

The constructor rejects exactly the same immediate `Arc<dyn StoreDriver>` in
both object and sink roles. Separate `Store` handles around that same driver are
also rejected. On construction failure it shuts down the consumed authority,
matching the existing `AuthorityService::start` ownership contract. The test
requires journal reopening within a bounded wait: pinned OpenRaft shutdown joins
the core and timer, but its state-machine worker releases storage asynchronously.
Immediate reopening initially exposed that existing shutdown race; this slice
does not strengthen `Singleton::shutdown`. This check cannot detect distinct wrappers that
reach a shared backend, provider prefix or filesystem location.

`nativelink-service/tests/authority_listener_test.rs` exercises the actual routes
for both digest algorithms. A valid client cache write succeeds, and the same
write on the worker listener returns tonic's observed `Unimplemented` status.
Valid bytes encoding a different `ActionResult` are hashed and uploaded through
both worker object APIs using the same digest as the cache key. The uploads and
reads succeed while the ActionCache entry stays unchanged. The client object
routes upload the actual Action, Command and Directory used by Execution.

A manual wire worker registers its complete inventory and capabilities, receives
the actual dispatch, and reports a candidate with the dispatched identity and
effect. A publication gate holds the accepted effect: the terminal operation is
available while the cache is absent. Releasing the gate lets the scheduler write
the accepted result, and another Execute observes a cache hit. This fixture
qualifies transport and publication wiring; its manual quiescent report does not
prove physical process cleanup. Real process evidence remains in the worker RPC
fixtures.

Transport security remains caller owned. The plaintext fixture deliberately
checks that its unauthenticated peer can write through the client route. Separate
ports and opaque route handles do not prove worker credential exclusion. The
existing [TLS listener experiment](listener-boundary.md) separately exercises
credential rejection on the legacy binary; its result does not automatically
qualify these authority listeners. A mounted authority TLS fixture, every client
route's worker exclusion, provider namespace isolation, and absence of worker
sink credentials remain required before a deployment claim. No runtime selector,
configuration converter, production entry point, or deployment is supplied here.

The [qualification receipt](evidence/authority-listener-construction/manifest.json)
records source hashes, the initial shutdown-race failure, strict Clippy, the
three listener cases and 68 existing service regression cases. Root integration
and Bazel verification are separate from this isolated Cargo receipt.
