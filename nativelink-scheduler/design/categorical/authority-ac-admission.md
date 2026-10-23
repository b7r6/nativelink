# Fixed Action Cache admission

The fixed client listener constructs a private Action Cache service with a
1 MiB stored-value and encoded-response limit, derived from the same constant as
its wire ceiling. Its worker listener still exposes no Action Cache methods.
The legacy configurable service keeps its existing behavior. These are finite
qualification limits, not a selected workload budget or a production activation.

A fixed read requests bytes `0..1 MiB + 1`, rejects excess before extending its
collector, and requires both EOF and a successful producer result. The store must
deliver every available byte in that requested range. Under that explicit
premise, a successful result of at most 1 MiB is the complete value. The generic
Store interface cannot prove this property of an arbitrary producer. A separate
metadata probe would not prove it either, because mutable writes can race it.

The extra byte prevents a valid protobuf prefix from hiding a trailing field.
The previous 10 MiB helper could decode a valid unknown-field prefix as an empty
ActionResult while omitting a following nonzero exit code. The fixed path checks
raw bytes before decoding, so unknown fields cannot disappear before admission.
It also checks the decoded result's encoded length: valid protobuf integer
encodings can normalize to a larger response.

A fixed update checks the ActionResult's encoded length before encoding or
calling any store. An accepted result is encoded once and returned unchanged.
The Action digest remains an opaque mutable-cache key: its hash and serialized
Action size do not describe the ActionResult's bytes. A zero-size or very large
Action key does not change the value limit. Client updates remain authorized;
this change adds no immutable publication or latest-writer ordering contract.

The fixed path uses the store facade instead of the direct GrpcStore shortcut.
It requests no extra inline stdout, stderr, or output-file content. Existing raw
fields are retained. The facade's upstream requests use false/empty inline hints,
but GrpcStore still decodes and normalizes its upstream response before the fixed
collector sees it. Its upstream decoder, retries, allocations, and concurrency
require their own bounds. The legacy shortcut continues forwarding inline hints.

Both fixed operations have a thirty-second deadline and return bounded
diagnostics. A read must finish its producer successfully even after EOF. Excess,
failure, or timeout drops the remaining future. A blocking or remote operation
may continue after cancellation; a timed-out mutable update can still finish
later. This does not establish process quiescence or a definitive write outcome.

The bounded range avoids asking the canonical sink to retain a complete value
that may be 64 MiB. That backend still scans the complete file, can read an
additional bounded chunk while detecting corruption, and may allocate spare
vector capacity. Other stores may already hold large chunks or upstream decoded
messages. The collector limit is not an exact heap, aggregate concurrency,
physical IO, disk-lifetime, or cold-recovery guarantee.

The qualification fixtures include direct handler checks, actual HTTP/2, a real
GrpcStore with an upstream request observer, and fixed-listener canonical stores.
They cover opaque keys, SHA256/BLAKE3 contexts, exact and excessive boundaries,
unknown fields, a valid prefix with a meaningful tail, normalized response
growth, EOF/error behavior, producer cancellation, pre-write refusal, and inline
hint isolation. Existing fixed listener tests retain the absent worker AC route
and client mutable-write behavior. No action process is executed by these tests.
