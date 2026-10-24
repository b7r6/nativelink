# Authority storage hash context

The fixed authority reads an Action and its Command in the digest namespace
selected by the Execute request. It reads and publishes an ActionResult in the
namespace recorded in the committed Action key. This binding applies each time
an effect runs, including publication replay after a coordinator restart.

The capture boundary maps an unspecified digest function to SHA-256, as the
captured request contract requires. It selects BLAKE3 explicitly when requested
and refuses other functions. Neither an inherited task context nor the process
default selects the namespace. The two CAS reads carry this context across their
asynchronous work. Ordinary cache lookup and publication reconstruct their
context from the committed `ActionKey.digest_function`.

The canonical store captures this context before starting blocking work and
includes the algorithm in its filesystem leaf. Its mutable sink stores an
ActionResult under an Action digest: the key's hash and size describe the Action,
not the ActionResult bytes. This patch keeps that contract and the existing M4
publication rules. It changes no persisted schema, effect payload or protocol
version.

## Counterexample and qualification

The original authority left the capture reads and ordinary storage effects in
their ambient context. Canonical BLAKE3 objects could therefore be absent from
the namespace selected by a SHA-256 default. A BLAKE3 cache lookup could instead
find a different value under the same opaque key in the SHA-256 sink. Publication
could write that other namespace. MemoryStore fixtures did not expose the
algorithm distinction.

The canonical-store fixtures exercise these boundaries separately:

- Actual fixed HTTP/2 listeners capture an Action and Command for both supported
  algorithms.
- A client seeds two different AC values under the same opaque key, one in each
  algorithm namespace. Execute returns the selected cached value and preserves
  the other entry. The SHA-256 case also uses an unspecified request function.
- A manual wire worker submits a candidate result. Publication stays held until
  the accepted result and effect are committed. The fixture shuts down the
  coordinator, reopens the same singleton journal, and checks publication through
  the client AC listener. The other algorithm's entry remains unchanged.
- A direct service call runs an unspecified-function request inside an explicit
  BLAKE3 task context. SHA-256 capture and cache lookup still succeed. This last
  fixture controls task context directly; it does not traverse HTTP/2 or change
  the process's global default.

The original three HTTP/2 fixtures fail at BLAKE3 capture before reaching their
later lookup and publication checks. Selective compiled mutations remove the
Action, Command, lookup and publication contexts independently, allowing each
later check to run. A separate mutation makes the unspecified request function
follow the ambient context and is checked by the direct service fixture.

These are local storage namespace and journal replay witnesses. The manual
worker does not prove process quiescence. The coordinator restart retains the
canonical filesystem capabilities and does not simulate power loss. The shared
bounded-read helper's writer lifetime, storage byte limits and production
construction remain separate work.
