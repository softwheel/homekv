# ADR 0003 — io_uring for the HomeKV Data Plane

- Status: Accepted for evaluation; production backend not yet selected
- Scope: M2 data plane, M7 performance optimization

## Context

HomeKV's v1 performance target requires high request throughput with predictable tail latency while preserving the accepted per-shard consistency and durability contracts.

The current prototype uses Tokio/Tonic. Tonic remains useful for control and administration, but the hot data plane will use a compact framed protocol under M2. The network runtime behind that protocol is a performance decision and must not leak into command, shard, or consistency semantics.

Linux `io_uring` is a strong candidate because it can reduce syscall overhead and supports submission/completion batching, registered/fixed buffers, multishot operations on sufficiently recent kernels, and zero-copy send variants. It also aligns naturally with HomeKV's planned shard/worker ownership model.

Tokio proper uses the conventional readiness/evented model on Linux. `tokio-uring` is a separate current-thread Tokio-compatible runtime with io_uring-backed resources; its resources are optimized for single-threaded use and many are `!Send`/`!Sync`. That is compatible with a thread-per-core data plane, but means HomeKV must not assume Tokio `AsyncRead`/`AsyncWrite` is the universal abstraction.

## Decision

HomeKV will design M2 so the wire protocol and request dispatcher are **network-backend independent**.

The first production-capable baseline backend will use regular Tokio networking. An `io_uring` backend will be implemented and benchmarked against the same protocol/parser/shard engine before it can become the preferred Linux data-plane backend.

HomeKV will not make `tokio-uring` a mandatory architectural dependency. The M2/M7 implementation may use either:

1. `tokio-uring` for the initial safe io_uring prototype; or
2. the lower-level `io-uring` crate when advanced operations or tighter reactor control are needed and the extra implementation complexity is justified by measured gains.

The backend boundary is at decoded request / encoded response messages, not at socket traits. This avoids forcing submission-based io_uring semantics into a readiness-oriented `AsyncRead`/`AsyncWrite` abstraction.

## Proposed data-plane topology

```text
                         control plane
                   Tokio multi-thread runtime
             admin / gossip / OpenRaft orchestration
                              |
                              |
               +--------------+--------------+
               |                             |
        I/O reactor core 0              I/O reactor core N
      Tokio or io_uring backend       Tokio or io_uring backend
               |                             |
       connection state                 connection state
       frame decode/encode              frame decode/encode
               |                             |
               +--------- bounded -----------+
                         dispatch
                            |
                 shard-owner workers
                            |
                    in-memory state
```

A connection remains owned by one network reactor. Requests are decoded in batches and sent to the owning shard worker through bounded mailboxes. Responses return to the originating network reactor for encoding and transmission.

No design should require one heavyweight runtime task per individual request. Connection-level state machines should process multiple pipelined frames per receive/completion when available.

## Thread-per-core direction

For the io_uring backend, prefer one current-thread reactor per selected CPU core, pinned where the benchmark environment allows it.

The design should evaluate:

- `SO_REUSEPORT` / per-reactor listeners where appropriate;
- per-core connection ownership;
- bounded cross-core request/response queues;
- submission/completion batching;
- registered/fixed receive buffers;
- `writev`/batched writes;
- multishot accept/receive when exposed and supported by the target kernel;
- zero-copy send only when payload size and profiling show a real benefit;
- explicit SQ/CQ/backpressure metrics.

Advanced io_uring features are kernel-gated optimizations, not protocol requirements.

## Control plane and OpenRaft

OpenRaft and administrative APIs do not need to move to io_uring merely because the client data plane does.

M3 should prioritize consensus correctness and use the simplest reliable transport compatible with the accepted OpenRaft spec. Raft transport may adopt the same io_uring reactor later if profiling shows networking/runtime overhead is material.

This keeps the highest-risk correctness path independent from an unproven network optimization.

## Benchmark gate

Tokio and io_uring backends must be compared with identical:

- protocol framing and parser;
- command semantics;
- shard engine;
- key/value distributions;
- connection counts;
- pipeline depths;
- request mixes;
- CPU affinity and machine configuration.

Required measurements include:

- requests/sec and throughput/core;
- p50/p95/p99/p99.9 latency;
- CPU utilization and cycles/op where practical;
- syscalls/op;
- context switches;
- allocations/op and copied bytes/op where practical;
- queue depth/backpressure events;
- network utilization;
- performance as connections and pipeline depth increase.

The matrix should include small KV requests, where syscall/runtime cost is most relevant, plus larger values where copy avoidance may matter.

The io_uring backend becomes the preferred Linux data plane only if it demonstrates a material repeatable improvement at equivalent semantics without unacceptable tail-latency or operational regressions. The exact numerical promotion threshold belongs to the M2 performance acceptance spec after the baseline is available.

## Why not select io_uring unconditionally now?

`io_uring` is not automatically faster for every in-memory KV workload. With small values and modest concurrency, protocol parsing, cache misses, cross-core routing, shard execution, or replication can dominate. Tokio's epoll-based networking is mature and provides a strong baseline.

The `tokio-uring` crate also has a slower release cadence and still describes itself as young, while the lower-level `io-uring` crate exposes newer kernel functionality more directly. HomeKV therefore treats the runtime/backend choice as a measured implementation decision rather than a semantic dependency.

## Expected upside

If HomeKV reaches high connection counts and deep pipelining, io_uring may improve throughput by reducing syscall/notification overhead and allowing more explicit batching and buffer lifecycle control.

It is also potentially useful later for the durable path: WAL writes and sync operations can share an io_uring-oriented I/O architecture, although WAL durability optimization requires its own accepted persistence/performance spec.

## Consequences

- M2 must keep protocol semantics independent from Tokio socket traits.
- The first M2 implementation should establish a regular Tokio baseline before promotion of io_uring.
- io_uring-specific buffer ownership must stay inside the backend/reactor layer.
- Linux kernel feature probing is required before advanced opcodes are enabled.
- Non-Linux builds can retain the normal Tokio backend.
- A failure or regression in the io_uring backend must not alter consistency, durability, retry, or routing semantics.

## References

- Tokio `tokio-uring` project and documentation
- Rust `io-uring` crate
- Linux io_uring documentation
