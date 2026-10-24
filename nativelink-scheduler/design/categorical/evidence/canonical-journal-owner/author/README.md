# Canonical journal owner author evidence

The author source is `f1847e64c39203ae81c73d05edfd177d0dba5ce2`, with tests frozen first at `411ba3698853f9b52782b032b781c7be3c864610`. This packet qualifies inert Linux namespace-bound journal preparation and sealed reopening. It does not activate a runtime or establish a production seal.

`verification.json` states the tested claims and limits. `receipts.json` identifies 1,133 original evidence artifacts. `package.json` maps those artifacts plus the receipts file onto 680 unique SHA-256 blobs in nine archives. Every retained executable and copied API rlib is included; the shared Cargo dependency directory is not copied. Each archive is smaller than 45 MiB.

Verify the archives and every mapped file without extracting:

```sh
python3 packet.py
```

Materialize the exact evidence tree, preserving recorded modes, into a new directory:

```sh
python3 packet.py --output /tmp/canonical-owner-evidence
```

The final restored source passes 116 scheduler unit tests (one syscall witness is intentionally ignored by the regular runner), one external owner API test, 47 filesystem tests, three strict Clippy configurations, and repository hooks. The separately invoked syscall witness passes three current-barrier baselines, six targeted EIO refusals, and nine seed or crash setup invocations. All 11 compiled runtime mutants are detected. Compiler API controls comprise one positive and 14 expected diagnostic refusals; they are not semantic mutants.

The final executable matches the frozen pre-mutation executable. Every mutation's exact source, executable, build receipt, oracle receipt, raw output, and relevant syscall traces is retained. Independent source, mutation, syscall, API, and final-restoration reviews are included. The existing-as-created mutant fails during sealed reopening before subsequent provenance/history assertions; the packet does not claim those later assertions were reached. Initial compile, lint, and hook corrections are retained as prerequisites, not mutation detections.

Root integration has a separate evidence supplement: the root added the external test's explicit Bazel tracing dependency at `595ece120c`, while the author source and Cargo mutation boundary remained unchanged. Root Bazel and independent Bazel-binary syscall evidence are not presented as author-executed qualification.
