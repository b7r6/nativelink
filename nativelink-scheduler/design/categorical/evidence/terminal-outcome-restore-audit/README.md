# Independent terminal restore correction receipts

`frozen-evidence.tar.gz` preserves the exact author receipts. Its `before/`
directory contains the original eight compiled failing probes and the exact
original reference source. Its `corrected-freeze/` directory contains the frozen
corrected source and source manifest. Its `mutants/` directory contains each
compiled mutation, source diff, failing test log and result. The final author
manifest hashes every archived evidence file.

The readable manifests beside the archive are exact copies of their author
counterparts. `mutation-results.json` records 22 compiled assertion kills,
including reruns of the original 12 mutations. The original 29 passing cases and
12 kills remain a separate historical qualification. This correction adds 15
audit cases, preserving all 22 original composition assertions and seven frozen
lifecycle cases unchanged.

The integration manifest records the focused repository Bazel tests and actual
integrated source hashes. It distinguishes the repository's previously recorded
formatting of the original case file from the exact author case bytes retained
in the archive. Candidate comparison is qualified separately.

The root Bazel log and Rust test logs are stored as deterministic gzip files to
preserve their exact whitespace. The first commit hook trimmed a plain copy of
the Bazel log; that failed hook is preserved separately. No Rust file changed.
