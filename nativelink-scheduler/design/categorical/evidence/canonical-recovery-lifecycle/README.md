# Private canonical recovery lifecycle evidence

Source commit `327d41eb37285192e0b249ac78fe5f6cacad9fdf` adds a private supervised lifecycle. It exposes no public recovery method, recovered clock, read barrier, serving authority, cold initialization or seal writer.

The focused suite passes all 21 new tests. The qualified and restored combined runs pass 184 library tests and one external owner API test, with one existing ignored test. Strict checks pass on the corrected and restored source. All six compiled control mutations fail at their declared, reached assertions. The records identify the first failing case and preserve the limits of each observation.

The first implementation compile failed on a fixture ownership error and executed no tests. Its correction retains the intended external ownership alias. The initial strict attempt reported seven diagnostics from four distinct lints; the corrections preserve the resource and acknowledgement rules. These attempts, the original tests, every source boundary, raw logs, independent reviews and every retained test executable are included.

Run `python3 verify-bundle.py` from this directory to check the relative archive contents, all executable aliases, and all 14 Cargo attempt bindings. Add `--extract /path/to/output` to reconstruct the source, logs, reviews and executable paths. The verifier reads only this bundle and the selected extraction destination.

Actual final database destruction precedes completion. Early startup storage failure has no returned Raft handle and does not prove an explicit tick join. Blocked storage work can keep cleanup pending. External ownership aliases may retain namespace locks, and database destruction is not a new durability acknowledgement. Process or runtime destruction cannot acknowledge asynchronous cleanup.
