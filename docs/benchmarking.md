# HomeKV Benchmarking Contract

HomeKV treats performance claims as reproducible engineering results, not marketing language.

## Required metrics

Every meaningful benchmark should report at least:

- throughput (ops/sec)
- p50 latency
- p95 latency
- p99 latency
- p99.9 latency where sample size permits
- CPU utilization and/or cycles/op
- memory usage and memory/key
- allocation rate where measurable
- request concurrency
- key/value sizes
- read/write mix
- dataset cardinality
- node count and replica count
- consistency mode
- durability mode

## Benchmark tiers

### Tier 0 — microbenchmarks

Used for components such as:

- hashing
- key lookup
- allocator behavior
- protocol encode/decode
- WAL record encode/checksum
- queueing/dispatch

Microbenchmarks may guide implementation but are not system-level performance claims.

### Tier 1 — single-node data plane

Measure the local engine without distributed replication.

Primary purpose:

- establish storage-engine cost
- detect lock/allocation regressions
- quantify protocol overhead
- determine scaling by client concurrency

### Tier 2 — replicated shard

Run a three-node replication group and measure:

- strongly consistent GET
- durable PUT/DELETE
- leader-local read fast path
- ReadIndex/barrier path
- replication batching
- group-commit sensitivity

### Tier 3 — multi-shard cluster

Measure many logical shards distributed over multiple nodes.

Include:

- uniform key distribution
- hot-shard/skew workload
- client-side routing
- leader distribution
- rebalance behavior
- scale-up by nodes and cores

### Tier 4 — failure benchmarks

Performance under failure is part of database behavior.

Measure:

- leader failover duration
- latency during election
- throughput degradation with one replica unavailable
- replica catch-up time
- snapshot installation time
- restart/recovery time
- rebalance impact on foreground traffic

## Baseline first

M0 must benchmark the existing implementation before the storage engine is replaced.

At minimum collect:

1. single-key GET
2. single-key SET
3. DELETE
4. mixed 80/20 read/write
5. multiple dataset sizes
6. multiple client concurrency levels

This baseline is intentionally expected to expose the cost of whole-store copy-on-write and shared synchronization.

## Workload shapes

Default benchmark matrix should cover at least:

- 16-byte keys / 64-byte values
- 16-byte keys / 256-byte values
- 32-byte keys / 1 KiB values
- read-only
- 95/5 read/write
- 80/20 read/write
- 50/50 read/write
- write-heavy

Use both uniform and skewed/Zipf-like key access.

## Tail-latency discipline

Average latency is insufficient.

Tests should:

- include warm-up
- run long enough for stable tail samples
- report saturation behavior
- avoid hiding pauses by trimming inconvenient samples
- record CPU saturation and queue depth
- distinguish client-side queueing from server time where possible

## Comparative benchmarks

Comparisons with other systems must state semantic differences prominently.

A result is not apples-to-apples if, for example:

- HomeKV performs quorum-durable writes while the comparison uses memory-only writes;
- HomeKV serves linearizable reads while the comparison serves follower/stale reads;
- persistence/fsync settings differ;
- replication factors differ;
- key/value sizes or pipeline depths differ.

Potential comparison categories:

- Redis/Valkey-style in-memory systems for data-plane context
- Dragonfly-class memory engines for throughput context
- etcd/TiKV-class systems for strongly consistent replication context

The goal is understanding trade-offs, not manufacturing a leaderboard.

## Rust vs. Zig experiments

Language experiments must share:

- equivalent algorithms
- equivalent memory ownership
- equivalent protocol semantics
- same compiler optimization level
- same hardware/core pinning
- same input corpus

A Zig implementation should replace Rust in a production hot path only if the gain is repeatable and materially outweighs integration and maintenance cost.

## Reproducibility

Published results should include:

- exact HomeKV commit SHA
- benchmark source/configuration
- hardware model
- CPU/core count
- RAM
- operating system/kernel
- compiler/toolchain versions
- relevant kernel/network settings
- command lines
- raw result artifacts where practical

CI should eventually run lightweight regression benchmarks, while dedicated hardware runs provide authoritative performance numbers.
