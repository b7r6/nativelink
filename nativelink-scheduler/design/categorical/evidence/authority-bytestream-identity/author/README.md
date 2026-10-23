# Fixed ByteStream upload identity: author evidence

This packet qualifies commit `7b1feba8deb858bd8aafd80ec7d2fa7c2e0e9272`.
The 17 selected source inputs are frozen by `qualified-source.json` inside
the archives, SHA256
`c51ca3b0ae213d7924769d6d8d5ba856667870e7cd72e06a2161d160af93ceba`.
These are selected inputs, not a complete hermetic dependency closure.

The original runtime compiled all 24 new HTTP/2 tests: three passed and 21
failed. Nineteen failures directly observed incorrect results or acceptance;
two were bounded progress failures after the operation under test. The fixed
runtime passed all 24. After 13 successfully compiled semantic mutations,
restoration passed 144 service tests with one existing ignored TLS test, plus
strict all-target service Clippy. A separate standalone fixed-digest test also
passed. Final hooks passed. Every mutant has its exact source, compiler log,
copied ELF, test log and predeclared assertion site; compiler failures and
timeouts do not count as detection. The final listener ELF matches the
pre-mutation ELF, and the other four final regression ELFs match the earlier
regression binaries.

`qualification.json` states the result and its limits. `verification.json`
records author artifact verification. Both complete independent review trees
are archived, including the final 13-control review and restoration review.
Independent reviewers inspected source and artifacts; their review does not
claim an independent Rust rerun. Root's separate Bazel integration packet
records 169 Rust passes, one ignored test and a server build against its own
source freeze. Four existing prerequisite differences separate that root
freeze from this author's selected inputs.

Run `python3 packet.py` to verify the seal, every archive and chunk, every
reconstructed original file, and all receipt mappings. Run
`python3 packet.py --output /tmp/identity-evidence-restored` to also restore
the exact tree into a new directory. Then run
`python3 /tmp/identity-evidence-restored/verify-evidence.py` to recheck recorded
qualification results and artifact relationships without executing Rust.

`package.json` maps original paths and modes to ordered SHA256 chunks. Chunks
are at most 16 MiB, and every archive is smaller than 45 MiB. Chunking retains
the complete large original-baseline log without truncation. Absolute paths
inside original receipts describe the execution environment; restored files
are addressed by the relative mapping. The verifier checks bytes, not a
claim of reproducible compilation or production correctness.

This slice does not bound aggregate upload-registry memory, add credential
partitioning, qualify backend-future terminality after cancellation, activate
production authority, or complete the broader categorical-elimination plan.
