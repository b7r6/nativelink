# Authority grants at worker execution

Authority dispatch carries the original per-attempt `AuthorityResources` grant
through both authority manager creation methods. The manager retains that vector
separately from the immutable Action, Command, selected platform and environment.
Changing the typed grant for a retry does not rewrite any of those original facts.
Implementations without the authority creation methods refuse by default.

When the existing worker memory sampler is enabled, its authority threshold uses
the typed memory-byte grant and configured headroom. The legacy property selector
is used only by legacy actions. This is an execution-boundary prerequisite; it
does not introduce the eventual fixed canonical enforcement policy or remove the
existing configuration selectors.

The percentage calculation uses checked wide arithmetic. It rounds upward to
whole bytes, rejects a result outside the byte range before Action fetch,
directory creation or process launch, then rounds upward again to the sampler's
KiB resolution. Grants of 1023, 1024 and 1025 bytes with zero headroom yield
ceilings of 1, 1 and 2 KiB. Zero memory means no per-action memory ceiling;
disabled enforcement also leaves the ceiling absent. The mandatory positive slot
charge remains distinct from the memory quantity.

`AuthorityResourceLimits` exposes a copy of the exact charged vector and optional
configured KiB threshold through the Rust action interface. It does not confer
acceptance authority or establish that sampling ran. Existing usage telemetry
reports authority memory in rounded-up KiB and CPU in rounded-up whole cores;
those coarse fields cannot replace the exact vector. The existing disk platform
property remains a diagnostic and is outside the typed authority grant. No CPU
enforcement or hard OS memory cap follows from them. Sampling and process-group coverage retain their
existing limitations. Namespace closure and directory cleanup keep their separate
quiescence requirements. This change qualifies the one-shot execution path. The
pooled persistent-worker branch retains its existing absent memory ceiling; the
authority recipient's launch-observation requirement rejects that branch.

The regression runs a real worker in a namespace with one immutable request declaring
128 MiB. Two distinct attempts receive typed grants of 128 and 256 MiB, each with
20 percent configured headroom. A child touches 192 MiB. The first attempt is
killed; the second completes while still observing the original platform value
in its environment. Both SHA-256 and BLAKE3 cases retain identical original
Action and Command bytes, preserve the exact request, verify usage against the
grant, and reject a changed grant on duplicate dispatch, including reconnect.
The saved pre-fix execution killed the second attempt under the original
131072 KiB platform value. Separate creation cases check exact byte retention,
rounding, disabled enforcement, zero and maximum values, headroom overflow and
refusal before launch.

These distinct-attempt dispatches qualify the manager boundary. RPC memory
classification remains unchanged: the coordinator still treats candidates as
Complete. This change does not activate automatic memory retry, reinterpret
heuristic SIGKILL telemetry as authority evidence, change the protocol, or claim
M6 publication/retention guarantees. A later explicit classification must preserve
all exact response outputs, establish termination causality and quiescence, and
refuse old protocol readers before processing reconnect inventory.
