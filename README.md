# HomeKV

**HomeKV is a memory-first, strongly consistent distributed key-value database focused on predictable low latency, high throughput, and rigorous failure semantics.**

The project started as an in-memory Rust KV store with copy-on-write MVCC, gossip-based membership, a phi-accrual failure detector, and consistent hashing. The next generation of HomeKV is intentionally more database-centric: shard-local execution, replicated state machines, durable consensus, explicit consistency contracts, and reproducible performance engineering.

## North star

HomeKV aims to explore the same class of systems problems that appear in high-performance managed in-memory databases:

- sub-millisecond in-memory reads on the healthy-path
- linearizable writes and strongly consistent reads
- partitioned scale-out with independent replication groups
- durable replicated commit before acknowledging durable writes
- fast leader failover without split brain
- memory-efficient storage and predictable tail latency
- snapshots, recovery, rebalancing, and online membership changes
- benchmark-driven optimization of networking, batching, allocation, and CPU locality

"Fastest" is treated as a measurable engineering goal, not a claim: every optimization must be backed by reproducible benchmarks with consistency and durability settings stated explicitly.

## Architecture direction

The v1 architecture is based on **shared-nothing shard workers + per-shard consensus**:

```text
client
  |
  | shard-map-aware routing
  v
+---------------- HomeKV node ----------------+
|                                             |
|  shard worker 0   shard worker 1   ...      |
|  +------------+   +------------+            |
|  | state      |   | state      |            |
|  | machine    |   | machine    |            |
|  | raft group |   | raft group |            |
|  +------------+   +------------+            |
|                                             |
|  WAL / snapshots / transport / metadata     |
+---------------------------------------------+
```

Core decisions:

- **Shard-owned state:** one execution owner per shard; avoid locks on the normal data path.
- **Stable logical shards:** key placement maps to logical shards, not directly to process liveness.
- **Consensus-owned leadership and membership:** gossip may provide health signals, but it never authoritatively changes ownership.
- **Raft-first replication:** writes are ordered through the shard leader and acknowledged according to the configured durability contract.
- **Strong reads:** leader-local fast path when safe; quorum-backed barrier/ReadIndex fallback when required.
- **Native data-plane protocol:** a compact pipelined protocol is the long-term hot path; gRPC/Tonic remains useful for control-plane APIs.
- **Single-shard atomicity first:** arbitrary cross-shard transactions are deliberately out of v1 scope.

See [docs/architecture.md](docs/architecture.md) and [docs/consistency.md](docs/consistency.md).

## Rust vs. Zig

HomeKV remains **Rust-first** for v1.

The largest expected performance gains come from architecture: eliminating whole-store copy-on-write, reducing synchronization, batching replication, improving memory layout, and optimizing the network path. Zig is kept as a controlled experiment for hot components after the architecture stabilizes.

A language change must earn its complexity through repeatable improvements in metrics such as p99 latency, cycles/op, throughput/core, memory/key, or recovery time.

See [docs/adr/0001-rust-vs-zig.md](docs/adr/0001-rust-vs-zig.md).

## Roadmap

| Milestone | Goal |
| --- | --- |
| M0 | Establish reproducible single-node and distributed baselines |
| M1 | Replace whole-tree COW MVCC with shard-owned in-memory execution |
| M2 | Build a low-overhead pipelined data plane |
| M3 | Add a correct 3-node replicated state machine |
| M4 | Scale to many logical shards / Multi-Raft |
| M5 | Add WAL, group commit, snapshots, restart and recovery |
| M6 | Add linearizability and failure-injection testing |
| M7 | Optimize CPU locality, memory layout, batching and I/O |
| M8 | Run controlled Rust-vs-Zig hot-path experiments |
| M9 | Publish reproducible comparative benchmark results |

Benchmark methodology is defined in [docs/benchmarking.md](docs/benchmarking.md).

## Current prototype

The existing codebase contains useful experiments that will be evolved rather than treated as the final architecture:

- in-memory `BTreeMap` storage
- copy-on-write MVCC snapshots
- Tonic/gRPC service and CLI
- gossip-based discovery
- phi-accrual failure detection
- consistent hashing

The current COW MVCC design clones the underlying store on the first mutation of a write transaction. That gives clean snapshot semantics but makes write amplification grow with the dataset, so replacing that path is the first major storage-engine milestone.

## Build

```bash
cargo build --release
```

Run the server:

```bash
./target/release/homekv -h 127.0.0.1 -p 20001
```

Use the CLI:

```bash
./target/release/hkvctl -h 127.0.0.1 -p 20001 --cmd set --kvs hello=world
./target/release/hkvctl -h 127.0.0.1 -p 20001 --cmd get --keys hello
./target/release/hkvctl -h 127.0.0.1 -p 20001 --cmd metrics
```

## Engineering principles

1. **Correctness before benchmark wins.** No performance result is meaningful if the compared semantics differ silently.
2. **Tail latency matters.** p99/p99.9, CPU saturation behavior, and recovery spikes are first-class metrics.
3. **Failure behavior is part of the API.** Partitions, stale leaders, retries, and reconfiguration must have explicit semantics.
4. **Measure before rewriting.** Architecture and language choices are benchmarked against baselines.
5. **Keep the hot path small.** Data-plane dependencies and allocations should be justified by profiling.

## License

See [LICENSE](LICENSE).
