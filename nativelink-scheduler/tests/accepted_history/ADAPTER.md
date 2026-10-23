# Accepted-history actual-State comparison

The adapter in `../accepted_history_differential_test.rs` exercises the real
experimental `State` through public methods. The adjacent `types.rs`,
`reference.rs`, `README.md`, and the reference-only test remain frozen at their
independently qualified hashes. The adapter author did not inspect the candidate
history transition implementation, authority reducer, receipt transition
implementation, candidate unit tests, or earlier authority/retention oracles.
Candidate changes were applied opaquely from versioned patches. Public data types,
request projections, documented memory policy, and the author's earlier receipt
adapter fixture provided the API contract. An initial Clippy diagnostic printed
the small payload-selection helper matching Completed and ResourceFailed; the
author saw that excerpt after all expectations were written. No history
transition or receipt algorithm was read, and no expectation changed from that
diagnostic.

## Sources of expectations

The scenario owns every execution, operation, attempt, reservation, dispatch,
cancellation, AC publication, receipt certificate, and history effect identity.
Allocator cursors advance explicitly from the scenario events. Candidate
observations never supply a fact, cursor, expected digest, or recipe.

Responses are public protobuf inputs. The fixture encodes and freezes their
bytes using the external protobuf library before calling the candidate's public
capture API. The frozen reference independently frames and hashes each history
object. The candidate's recipe is mapped field by field into the reference
vocabulary solely for comparison. Actual object bytes must equal the reference
prefix followed by the independently supplied immutable response.

Every step compares complete history associations, exclusions for terminals
predating activation, operation-to-history projections, ordinary effects,
operation terminals,
execution requests and targets, original attempt/reservation identities,
release status, and aggregate worker charges. History publication observations
must leave the entire ordinary `ObservableState` unchanged. Exact duplicate
completion and wrong-epoch observations must preserve the canonical snapshot.
Receipt composition also checks the independently constructed pending body and
acknowledgment identity; it does not reuse the candidate's certificate as input.

The history oracle receives independently computed non-history future-ID
obligations and a conservative slot allowance. An 8 MiB allowance covers the
ordinary lifecycle in these small fixtures. These scenarios stay away from
resource limits: they do not qualify the candidate's exact byte-credit proof,
near-exhaustion allocator arithmetic, or admission-capacity boundary. Those
remain separate reference and candidate budget qualifications.

## Histories

- Success, failure, infrastructure error, and noncacheable completion under
  SHA-256 and BLAKE3, including zero caller-supplied identities and an exact
  duplicate completion.
- Activation with an old terminal and existing unfinished work; later cache
  hits and terminal retirement do not invent history.
- Two accepted memory retries followed by final `ResourceFailed`, with only
  the final exact response becoming history.
- Shared operations with one handle canceled before acceptance, followed by
  incarnation replacement after acceptance.
- Incarnation replacement before completion, stale output, original cleanup,
  and later accepted replacement output.
- Failed, unknown, wrong-object, wrong-epoch, duplicate, and retired publication
  observations; pending history blocks retirement.
- Receipt issuance, AC publication, and history sharing the global allocator;
  history and receipt pins release independently.
- Snapshot restoration with accepted and unfinished work plus a pending receipt,
  followed by identical suffixes and complete canonical-State byte comparison.

## Qualification corrections

The initial candidate rejected canonical lowercase action algorithms. The
adapter retained its independently chosen `sha256` and `blake3` inputs; a
candidate correction made those histories pass. The first shared-operation
scenario incorrectly requested that two noncacheable actions join. The request
matrix in `../../design/categorical/contract.md` forbids that behavior. That
scenario now supplies cacheable actions with lookup bypass; its existing
independent allocator also accounts for their AC publication. The original
failing sources and traces are retained with the qualification evidence. The
frozen reference did not change.

## Scope

This is finite comparison evidence for the actual deterministic State against
the frozen history component. It uses direct feature activation and in-memory
snapshots, not a journal format transition, replicated commit, process crash,
authenticated worker transport, production namespace binding, or CAS provider.
It does not implement worker history relocation, an I/O recipient, estimator
consumption, or production cutover. Fair delivery and external store durability
are outside these traces.

```bash
cargo test --locked --profile smol -j 8 -p nativelink-scheduler \
  --test accepted_history_differential_test
bazel test --jobs=8 //nativelink-scheduler:accepted_history_differential_test
```
