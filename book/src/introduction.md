# Introduction

This is the book that should have existed from day one.

NativeLink is the fastest open-source implementation of the Remote Execution API. It handles billions of requests per month in production. It is written in Rust, configurable in JSON5, and deployable from a single binary to a Kubernetes fleet. It is also, like most infrastructure software, inadequately documented.

The existing docs tell you what flags to set. They don't tell you why the system is shaped the way it is, how the pieces compose, what the actual failure modes are, or how to reason about your deployment. They don't explain the protocol, the store algebra, the scheduling model, or the three fundamentally different approaches to toolchain management that determine whether your cache hit rate is 95% or 5%.

This book fixes that.

## Who This Is For

You build software. You've probably heard of remote caching or remote execution — maybe you've even tried it. You might be using Bazel, Buck2, or another build system that speaks REAPI. You want to understand what's actually happening when your build tool talks to a remote backend, and you want to operate that backend without cargo-culting YAML from a getting-started guide.

If you're evaluating NativeLink against Buildbarn, Engflow, or BuildFarm, this book will make the architectural differences obvious.

If you're trying to get your cache hit rate above 90%, the toolchain chapters will save you weeks.

## What You'll Learn

By the end of this book you will understand:

- **REAPI** — the protocol that Bazel, Buck2, and NativeLink all speak, from first principles
- **The store algebra** — how NativeLink composes storage backends into arbitrarily complex topologies using a single trait
- **Scheduling and dispatch** — how actions get matched to workers, and why platform properties are the entire game
- **Toolchains** — the three approaches (Nix/LRE, containers, hermetic cross-compilers) and when each one wins
- **Client integration** — Bazel and Buck2 as equal first-class citizens, with real configs and real gotchas
- **Deployment** — from a single binary on localhost to a production Kubernetes fleet with observability

## How to Read This

Parts I and II give you the mental model and the protocol. Parts III and IV are the internals — stores and scheduling. Part V is toolchains, which is where most teams get stuck. Part VI is client integration. Parts VII and VIII are deployment and worked examples.

Read it front to back the first time. The code links point into the actual NativeLink source — follow them.

## A Note on Tone

This book is opinionated. Remote execution is too important and too poorly understood for hedging. When something is wrong, we say so. When there's a better way, we show it. When the protocol has gaps, we name them.

The build system world has spent a decade telling people to "just set `--remote_cache`" and wondering why cache hit rates are terrible. The answer is always toolchains. This book will make that obvious.

Let's go.
