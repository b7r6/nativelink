# ByteStream terminal backend future evidence

This focused packet records runtime commit
`a8fc2c0a2735ccbb6c8fcc700ffbc5365548eb28` and the unchanged six-test fixture
at SHA256
`6909dd9d889484ed64b548bb28ebef43ba0d33e665bd5fc361dbf3e0d0b0d5f6`.
The runtime SHA256 is
`d8b0cea01d6211e2075b1685e681fbf37eaea7e6a89f57badb2ef2a4b060ad15`.

The original runtime passed one test and failed five. Two failures contain
production future-repoll panics; the others directly observed disconnected
writer state or stale progress. The fixed and restored executions each passed
all six tests. Two compiled faults failed their intended direct assertions:
keeping a terminated fused future reported stale progress, while discarding
pending work lost resumable progress. Neither timeout nor compilation failure
counts as detection. Every test execution was uncached. The restored ELF is
byte-identical to the first fixed ELF.

Root executed the Bazel commands. Independent storage review checked the
source, full raw logs, BEP execution records, executable hashes and restoration
without running Rust. The complete source/proposal, original-runtime and
compiled-control review directory is included. Twelve selected source inputs
are copied; all five runs record 1,350 unchanged input hashes. Independent
review matched the original and restored hash sets to their git commits.

Run `python3 packet.py` to verify the seal, archives, ordered content chunks,
every reconstructed original file and receipt mapping. This verifies all file
bytes without expanding duplicate ELF files. To restore the evidence tree,
use `python3 packet.py --output /tmp/terminal-evidence-restored` with a new
directory. Its `verify-evidence.py` rechecks recorded qualification relations
without compiling or executing Rust. Absolute paths in historical receipts
describe the original execution environment; package mappings are relative.

`qualification.json` explains the exact boundary. A later broad root run
reported an unused qualification warning in the fixture; its planned spelling
correction and strict/combined qualification belong to a separate receipt.
This packet preserves the original fixture and claims no strict Clippy pass
or later combined result. Metrics are source-reviewed only. The deliberately
invalid early-success backend tests future ownership, not storage integrity.

The archive format uses deduplicated SHA256 chunks at most 16 MiB each. Every
archive is smaller than 45 MiB. Package staging and reviewer copies share
immutable artifact inodes to conserve disk; the archives are self-contained.
