# ADR 0001: Rust-first, benchmark Zig on selected hot paths

- Status: Accepted for HomeKV v1
- Date: 2026-08-24

## Context

HomeKV's goal is to become a very low-latency, strongly consistent distributed in-memory KV database. Zig is attractive because it offers explicit memory control, a small runtime model, straightforward C interoperability, and strong potential for systems-level optimization.

However, HomeKV's current performance bottlenecks are architectural rather than language-bound. The prototype uses whole-store copy-on-write semantics around an ordered map and shared synchronization. Replacing those mechanisms with shard-owned execution, better memory layout, replication batching, and a smaller data-plane protocol is expected to dominate any language-level difference.

Rust also provides a mature ecosystem for networking, async/runtime work, testing, profiling, fuzzing, atomics, serialization, and consensus-oriented development.

## Decision

HomeKV v1 remains Rust-first.

Zig will be evaluated experimentally for isolated, measurable hot components after the architecture and benchmark harness are stable.

Candidate experiments include:

- hash table / index implementation
- arena or slab allocator
- protocol parser/encoder
- WAL record encoder/checksummer
- specialized network or I/O path

A wholesale rewrite is explicitly not the first optimization step.

## Decision criteria

A Zig component should be considered for production use only if it demonstrates a repeatable material improvement in one or more of:

- p99/p99.9 latency
- cycles per operation
- throughput per core
- memory per key/value
- allocation rate
- recovery or WAL throughput

while preserving equivalent correctness and operational semantics.

The evaluation must also account for:

- FFI cost
- build complexity
- debugging/tooling quality
- maintenance burden
- contributor accessibility
- safety/regression risk

## Consequences

Positive:

- engineering effort stays focused on the highest-leverage database architecture work;
- HomeKV can use the existing Rust codebase and ecosystem;
- Zig remains available as a serious performance tool rather than a speculative rewrite;
- language comparisons become publishable benchmark results.

Negative:

- HomeKV may temporarily leave some low-level optimization opportunities unexplored;
- introducing Zig later could create a mixed-language build and FFI boundary.

## Revisit trigger

Revisit this ADR when all of the following are true:

1. M0 baseline benchmarks exist;
2. the shard-owned memory engine exists;
3. at least one replicated-shard benchmark exists;
4. profiling identifies a stable CPU/memory/I/O hotspot suitable for an isolated comparison.

At that point, implement the same component/algorithm in Rust and Zig and decide from measurements.
