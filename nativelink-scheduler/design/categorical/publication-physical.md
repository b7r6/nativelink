# Physical preparation and atomic publication

This bounded composition connects the frozen `PublicationAuthority` candidate to
real `PhysicalFilesystem` writes and reads and the typed `Preparation` walker.
The candidate embeds the actual scheduler `State` and coarse retention engine.
The fixture adds no second publication reducer and reads no publication reference
implementation. The candidate's supplied restore-validation repair is included;
its implementation remains unchanged by this composition.

## Write and preparation boundary

Each scenario independently specifies an ActionResult, a Tree with embedded
parent and child directories, an explicit retained Directory, and artifact,
stdout, and stderr blobs. The expected physical members, roles, and eight typed
edges come from those known messages. Embedded Tree directories remain embedded;
they do not become invented physical objects.

Every new generation enters the catalogue with `ready=false`. The fixture commits
a staging root and stable write identities before calling `begin_write`. A real
write must finish with the expected identity and descriptor, and a whole-object
verified read must return the expected bytes, before the exact `observe` call can
mark it ready. The fixture never uses the candidate's trusted `ready=true`
bootstrap path.

`Preparation` reads the physical filesystem, one bounded step at a time. The
fixture compares its complete member roles, exact bindings, read-byte count, and
edge list with the independently declared scenario. Only that completed evidence
can reach candidate preparation. The catalogue revision and retention fences are
captured before the reads, so an intervening allocation or fence remains visible
as stale evidence. Expected members supplied before writing provide conservative
staging protection; they are not accepted as completed physical preparation.

The full sixteen-byte namespace selects an owned filesystem root. The full
sixteen-byte incarnation and generation select a physical object. Replacement
cases differ only in the final namespace or incarnation byte while reusing
physical generation numbers. No prefix, generation alone, or digest alone chooses
a backend.

## Publication, reads, and reopen

The worker scenario reaches the actual scheduler's running attempt before
publication. One successful outer commit installs the exact captured terminal,
accepted provenance, selected head, and retention roots. An incomplete or stale
publication leaves the entire canonical outer snapshot byte-for-byte unchanged.
No temporary legacy publication effect escapes that boundary.

Replacing the head preserves the old operation and serving roots. Reads first
check the exact member against its operation or serving grant, then perform the
real whole-object verified filesystem read. An old grant cannot read the new
namespace's members. A stale serving barrier or selected head cannot acquire a
new grant. Losing and rebuilding the disposable index does not select a result.

Another scenario keeps real staged writes open, abandons their publication, and
commits a replacement. The delayed writes then finish and their exact outcomes
are recorded. Their bytes can exist physically without selecting a head or
changing accepted provenance.

Checkpoints contain one fully committed outer candidate snapshot, including both
embedded engines and all identity/protection metadata. The fixture writes and
syncs a temporary checkpoint, renames it, and syncs its owned directory. Reopen
reconstructs the candidate from those bytes and opens the physical incarnations
named by its retained stages. It does not combine separately captured scheduler
and retention snapshots.

The recovery scenario checkpoints after physical writes finish but before all
observations, after preparation, and after acceptance. All local write calls have
returned before these reopen cuts. Stable write identities come from the outer
snapshot; verified physical reads reconcile the missing outcomes. No completion
receipt or pending writer survives in fixture memory across reopen.

## Qualification boundary

The standalone target is `//nativelink-scheduler:publication_physical_test`, also
available as `cargo test -p nativelink-scheduler --test publication_physical_test`.
Its receipts preserve exact sources, commands, failures, and results under
[evidence/publication-physical](evidence/publication-physical/manifest.json).

The scenarios cover both SHA256 and BLAKE3 physical objects. The frozen
candidate's scheduler admission still uses its SHA256 placeholder ActionKey and
opaque qualified-action string. These cases do not qualify BLAKE3 REAPI ingress.

This is a local singleton fixture. The serving barrier is its outer commit
counter, and checkpoint publication is its owned filesystem transaction. Neither
establishes a replicated read barrier, a Raft publication boundary, remote
provider write quiescence, provider deletion, production sizing, or activation.
The closure inputs are known finite scenarios; this does not establish that an
arbitrary RPC-supplied closure is complete. Existing candidate promotion blockers,
including errors without an ActionResult and active accepted-history composition, remain.
