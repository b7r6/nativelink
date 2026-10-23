# Additive response-projection audit of the frozen head reference

The frozen head reference qualifies publication ordering, retained terminal
stability, lookup policy, and explicit cached-versus-worker terminal tags over
opaque scenario bytes. It does not qualify byte-for-byte REAPI response
projection for cached operations derived from worker-originated publications.

The reference currently copies Candidate.response into both a worker completion
terminal (cached=false) and a later cache-hit terminal (cached=true). Its type
comment calls those bytes an expected operation encoding. That wording is too
broad for an actual ExecuteResponse differential: the `cached_result` wire field
alone can require different bytes, and a cached response must not accidentally
inherit source-worker response fields that are not part of the cached projection.
This follows from distinguishing ActionResult from ExecuteResponse and is
consistent with the root-reported M4 lookup construction. No candidate source was
read for this audit.

An actual REAPI differential must provide separately scenario-owned source
response bytes and cached-operation response bytes, or an independently specified
projection from the immutable ActionResult. Worker completion freezes the source
operation response. A later cache lookup freezes its own cached response and
serving protection. Both responses refer to the same accepted result publication;
they need not be byte-identical. Direct UpdateActionResult supplies ActionResult,
not a source worker ExecuteResponse. Existing frozen tests remain useful within
the abstract response-token interpretation but cannot claim this concrete wire
projection gate.

No frozen source, patch, or archive changed. Original patch SHA256 is
90ecc2e29cf1b58383d67ee44c69466f8715ba01cf271ec9c8cf836fe7c48bef;
original evidence archive SHA256 is
4a0f5aafff496f7ba1602aef02da78fc2fcb4286f451af977e89e0ddf1c126bd.
A successor contract must add distinct `source_response` and `cached_response`
fixtures before comparing actual REAPI bytes.
