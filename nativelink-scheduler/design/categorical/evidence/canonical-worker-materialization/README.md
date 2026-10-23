# Private worker staging qualification

This packet qualifies the private worker filesystem prerequisite in
`tools/canonical-filesystem` against base commit
`255fa9c0fb3d2abeb170aee9f7deff8f100b17ec`. It adds no public capability,
production factory, action execution, cleanup, durable publication or deployment
authorization. Host namespace protection, aggregate resource budgets, digest
validation and restart recovery remain separate prerequisites.

The unchanged 33 worker tests run in both suites. Ownership passes 68 tests and
journal passes 80: 148 executions of 80 distinct tests. Both normal libraries
build. Restored suites pass uncached with default Bazel formatting and Clippy.
Relevant source hooks and direct pinned nightly formatting pass; the two Cargo
wrappers were omitted because that build lane was reserved. No dependencies
changed. The initial four Clippy diagnostics and two README spelling errors,
their exact corrections, and the successful reruns remain in the packet.

All 16 approved runtime variants compile. Their 17 exact test invocations each
reach a named assertion failure with exit 101, without timeout or zero-test
results. The shared family check has two independent invocations and counts as
one variant. C08 and C13 still refuse completion through the final size check;
their assertions fail on the error code. C13's overwritten prefix is inferred
from the executed source path because the subsequent byte assertion is never
reached. C04 loses descriptor-relative availability while retaining
`O_NOFOLLOW`. No outside mutation is claimed. Every result records the first
observed branch and distinguishes later, unobserved assertions.

The campaign preserves actual executable bytes, matching Bazel event digests,
commands, raw logs and 1,363 source hashes before and after each build and test.
Every variant is restored before the next. The full restored manifest matches
before and after the final tests and library builds. Only the README changes
afterward to describe qualification and fix spelling. Runtime and frozen tests
remain byte-identical. The test file's opening comment records its original
tests-first checkpoint; the later compiled results are recorded here.

Archive member names begin with the bundle shown below. All paths, file sizes,
modes and SHA-256 values are recorded in `package.json`. Archives contain regular
files only. Verification reads and hashes them without executing or extracting
their contents:

```sh
python3 verify.py
```

| Bundle | Contents |
| --- | --- |
| `source-audit` | Original source audit and narrow proposal |
| `tests-first` | First 30 tests and source freeze |
| `tests-addendum` | Approved cross-family and finite interrupted-read tests |
| `runtime-source` | Reviewed private runtime source before lint correction |
| `positive` | Initial diagnostic failure, correction, positive builds and tests |
| `controls` | Approved exact patches, classifications and guarded apply helper |
| `mutations` | Actual variants, results, restoration, final tests, hooks and source |
| `independent-root` | Parent source and contract reviews |
| `independent-storage` | Independent source, control and executed-evidence reviews |

The mutation bundle's `qualification.json` and `stage-classifications.json`
state the claims and limits. Its `seal.json` authenticates the 434-file evidence
receipt list. The independent review evaluates that immutable packet separately.
Absolute temporary paths in captured commands describe the original run and are
not portable execution instructions.
