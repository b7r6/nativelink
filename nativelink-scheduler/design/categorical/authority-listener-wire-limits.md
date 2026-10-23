# Fixed authority listener wire limits

The experimental `AuthorityListeners` constructor installs finite protobuf
message ceilings on every route. These are constructor constants, with no
production factory selection or configuration fields. The legacy service
constructors and production listener limits remain unchanged.

| Fixed route | Decode ceiling | Encode ceiling |
| --- | --- | --- |
| Worker API, worker role | 1 MiB | 1 MiB |
| CAS, both roles | 1 MiB | 1 MiB |
| ByteStream, both roles | 1 MiB per message | 1 MiB per message |
| Action Cache, client role | 1 MiB | 1 MiB |
| Execution, client role | 64 KiB | 128 KiB per Operation |
| Operations, client role | 64 KiB | 128 KiB |
| Capabilities, client role | 64 KiB | 64 KiB |

The ceilings include the protobuf envelope. A ByteStream message containing
exactly 1 MiB of data exceeds its ceiling once its other fields are included.
The constructor explicitly sets ByteStream read chunks to 64 KiB. Clients
can use 64 KiB chunks to transfer objects larger than one message, subject to
the backing store's object limit.

Tonic 0.14.6 checks the declared incoming message length before its
length-directed buffer reservation or invocation of the protobuf decoder.
Transport data frames may already be buffered at that point. Streaming RPC
handlers receive the stream before decoding and may acquire a permit before polling
its first message. The outgoing limit is checked after protobuf encoding.
Consequently, the encode ceilings bound messages
sent successfully; they do not prevent the handler or encoder from allocating
an oversized response. Neither ceiling bounds concurrent connections, HTTP/2
streams, metadata, protobuf object expansion, total upload length or work over
the lifetime of the server.

The authority retains its existing semantic checks: Execute requests and
candidate result payloads are bounded at 64 KiB, report envelopes at 64 KiB
plus 1 KiB, worker update and registration messages at 1 MiB, and rendered
Operations at 128 KiB. The wire limits run before those checks. The worker
limit applies to the outer update envelope, so a registration that itself
occupies the entire semantic limit cannot also fit that envelope.

## Canonical store compatibility and remaining gaps

The canonical CAS and sink cap each object at 64 MiB, each incoming data chunk
at 1 MiB, and shared active operations at four. A store admission failure has
no waiting queue. These are backend limits; service decoding and other work
may occur before backend admission. This patch establishes no global memory
or workload bound.

Both fixed listener roles keep remote cache compression disabled. CAS batch
decompression otherwise occurs before the backend sees the upload, so its
object ceiling cannot be used as a decompression admission argument. The
same constructor also refuses compressed ByteStream reads and writes. The
separate canonical-store generic-service tests that enable compressed reads
do not qualify compressed reads on these fixed roles.

The current Capabilities response advertises a 64 KiB batch total. That total
fits the new message ceilings with ordinary digest envelopes, but the generic
CAS handler does not enforce a batch byte or entry count limit. Empty blobs
and large per-entry envelopes also consume message bytes independently of
the advertised data total. `BatchReadBlobs` collects all selected results
before response encoding; a small request can select many large objects.
Canonical-store admission limits four reads at once but does not bound the
total retained batch response. The new encode refusal happens after this
work. `FindMissingBlobs` reaches the canonical backend's existing 64-key limit.

`GetTree` traverses and accumulates directories without a total traversal
budget. Its shared decode helper requests at most 10 MiB of each stored
Directory; that per-object read limit does not bound the number of directories
or their decoded aggregate. Action Cache reads use the same 10 MiB helper for
an ActionResult. The helper and protobuf decode run before the 1 MiB outgoing
message check. A canonical read still verifies the complete object before
returning the requested range.

Capabilities currently reports `max_cas_blob_size_bytes = 0`. The local REAPI
schema explicitly defines zero as no limit and allows clients to assume
arbitrarily large objects. That advertisement does not describe the canonical
backend's 64 MiB ceiling. It is preserved here because `AuthorityListeners`
accepts arbitrary existing stores and does not yet carry a canonical storage
profile. A later canonical construction needs one source of truth for the
backend limit and its advertisement, plus service-level batch/traversal
admission if those aggregate bounds are claimed. Changing the reported
number cannot establish that admission.

## Qualification

The service listener tests send raw protobuf bodies through actual local
HTTP/2. Every fixed service in each role refuses an oversized message at the
decoder, while a malformed short message reaches protobuf validation. A valid
Capabilities request padded with an unknown field to exactly 64 KiB succeeds.
The authority retains no worker, execution or operation from these refusals.

The canonical-store fixture serves the actual fixed listener roles over local
TCP with the canonical CAS and sink. For SHA-256 and BLAKE3 it transfers a
2 MiB plus 17-byte object in both directions between the roles using 64 KiB
upload chunks and verifies all returned chunks are at most 64 KiB. Oversized
CAS, ByteStream and Action Cache requests, malformed requests, and disabled
compression requests leave both canonical content and temporary directories
empty. A separate response test establishes that existing large CAS and AC
values can be read internally and then refused by the outgoing encoder. It
does not relabel that late refusal as allocation admission.

The fixture uses a separate ordinary journal test file. It does not qualify
canonical journal construction, process isolation, TLS credentials, metadata
limits, concurrent request admission, service aggregation or production
selection. Those retain their separate evidence and open obligations.
