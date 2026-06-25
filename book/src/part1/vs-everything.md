# NativeLink vs. Everything Else

The remote execution space has a handful of serious implementations. Here's how they differ where it matters.

## The Landscape

| System | Language | License | Cache | Execution | Managed |
|--------|----------|---------|-------|-----------|---------|
| **NativeLink** | Rust | FSL-1.1-Apache-2.0 | Yes | Yes | No |
| **Buildbarn** | Go | Apache-2.0 | Yes | Yes | No |
| **BuildFarm** | Java | Apache-2.0 | Yes | Yes | No |
| **EngFlow** | Java/C++ | Proprietary | Yes | Yes | Yes |
| **BuildBuddy** | Go | Apache-2.0 + EE | Yes | Yes | Yes |

## Buildbarn

The most direct comparison. Buildbarn is written in Go, is mature, and has been used at scale. The architectural difference is in storage: Buildbarn has a fixed set of storage backends with fixed composition semantics. NativeLink's store algebra means you can compose arbitrary storage topologies in config without touching code.

Buildbarn's worker model is also more rigid — it has a separate `bb-worker`, `bb-scheduler`, `bb-storage`, `bb-runner` binary split. NativeLink is one binary with one config file. This matters less at scale (you'll deploy different configs anyway) but matters enormously for development, testing, and debugging.

Go's GC is a real tradeoff at the tail. When you're serving billions of CAS requests per month, p99 latency spikes from GC pauses are observable. Rust eliminates this class of problem entirely.

## BuildFarm

Java, old, complex. BuildFarm has the most deployment history in the open-source world (it was Google's original reference implementation). It works. It is also the slowest option by a wide margin, has the most complex deployment story, and its codebase is difficult to modify.

If you're already running BuildFarm and it works, the migration cost may not be worth it. If you're starting fresh, there's no reason to choose BuildFarm today.

## EngFlow / BuildBuddy

Managed services. They handle deployment, scaling, and operations for you. The tradeoff is vendor lock-in, cost, and the inability to inspect or modify the system when something goes wrong.

If your organization cannot or will not operate infrastructure, a managed service is the right answer. If you want control — over your data, your performance characteristics, your failure modes — you operate your own.

NativeLink is what you operate yourself.

## What Actually Matters

The choice between remote execution backends comes down to three questions:

1. **Can it sustain your throughput?** At billions of requests, language runtime overhead is not theoretical. Rust's zero-cost abstractions and lack of GC matter.

2. **Can you compose the storage topology you need?** Tiered storage (fast SSD cache + slow S3 backend), deduplication, compression, cross-region replication — these are not features you want to fork the codebase to add. NativeLink's store algebra handles all of them in config.

3. **Can you debug it when it breaks?** A single binary with a single config file and structured logging is easier to reason about than a microservice mesh with five different binaries, three different config formats, and state scattered across multiple databases.

NativeLink wins on all three.
