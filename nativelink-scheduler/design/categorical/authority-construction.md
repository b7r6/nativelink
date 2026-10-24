# Authority worker construction

The experimental authority worker can now construct its action manager without
an action-cache publisher, accepted-history destination, upload policy, or
diagnostic response template. This closes a construction gap in M4a.1/M4a.2:
the recipient already avoided the legacy publication method, but its manager
previously retained that capability.

`AuthorityRunningActionsManagerArgs` contains only execution and CAS inputs.
`RunningActionsManagerImpl::new_authority` runs the same execution initialization
with no `UploadActionResults` instance. Calling the legacy `cache_action_result`
trait method on this manager returns `FailedPrecondition` before constructing
publication work or changing the supplied result. It does not use a dummy store
or a policy that merely disables writes.

The legacy constructor retains its existing arguments, destinations, policies,
templates, and initialization order. A private common constructor transfers every
execution field and defers publisher construction to its original position after
execution validation. Existing legacy worker and publication tests remain positive
controls. The binary and production worker factory continue to use that legacy
constructor; this change exposes no runtime selection switch.

## Executed checks

The manager regression supplies rich successful and failed results for both
SHA-256 and BLAKE3. Repeated accidental publication calls are refused while the
entire result and sentinel CAS contents remain identical. The authority
constructor receives no AC or history handle.

The RPC fixture retains its original legacy-enabled construction and adds
authority-only construction. Six real process histories cover both digest
algorithms, with successful cacheable, nonzero-exit, and `do_not_cache` results.
They verify input, output, stdout, and stderr bytes in CAS. With the coordinator
unavailable, the worker finishes without publishing. After reconnect, the exact
accepted response remains stable while history is pending, through two authority
restarts, and after the captured historical response is materialized. For the
successful cacheable cases, a held AC effect remains pending across restart;
releasing it writes the accepted result and enables a positive cached Execute
response. The other cases create no AC effect. Every process executes once.

The focused Cargo histories emitted all six process markers without namespace
skips. The subsequent Cargo regression passed all 163 cases across manager,
legacy worker, authority worker, scheduler bridge, and RPC suites. That run
preceded a logging-only change required by strict Clippy: the process marker now
uses explicit construction labels instead of debug formatting. Separate source
snapshots and the original lint failure are retained in the
[receipts](evidence/authority-construction/manifest.json).
All 17 worker Bazel targets then passed on the corrected source, including
strict lint checks; 16 executed and one was cached.

## Remaining boundary

This is an additive construction seam, not completion of M4a. Removing a publisher
handle does not establish that an arbitrary supplied CAS store cannot alias a
mutable AC destination. Fixed service roles, listener trust exclusion, namespace
isolation, strict CFG1 refusal, and the offline migration converter remain
separate requirements before canonical product construction. This fixture also
does not qualify Operations RPCs, worker memory-failure classification, deployed
credentials, or a production cutover.
