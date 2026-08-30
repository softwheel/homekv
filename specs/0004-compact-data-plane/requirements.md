# Spec 0004 — Compact Pipelined Data Plane Requirements

- Status: Accepted
- Parent: `specs/0001-homekv-v1/requirements.md`
- Tracking issue: #27

## 1. Purpose

M2 replaces transport/protocol overhead on the foreground data path with a compact, bounded, versioned protocol while preserving the verified M1 shard semantics. M2 does not add replicated consensus or durable-write semantics.

## 2. Scope

M2 covers:

- GET, SET/PUT, DELETE, and deterministic single-shard batches;
- compact binary framing over a reliable byte stream;
- request pipelining and response correlation;
- bounded decode/admission behavior;
- logical-shard routing metadata and explicit safe routing failures;
- benchmark comparison with the existing gRPC path under equivalent local M1 semantics.

M2 excludes OpenRaft, WAL/snapshots, distributed placement/reconfiguration, lease reads, cross-shard transactions, TLS/authentication policy, and public release performance claims.

## 3. Protocol requirements

**REQ-M2-PROTO-001** — Every frame MUST be self-delimiting with a fixed-size prefix containing protocol version, frame kind, flags/reserved bits, and payload length.

**REQ-M2-PROTO-002** — v1 of the M2 wire protocol MUST use an explicit magic/version combination and MUST reject unsupported versions deterministically before interpreting operation payloads.

**REQ-M2-PROTO-003** — Integers in the wire format MUST use one documented byte order. M2 chooses network byte order (big-endian) for fixed-width integers.

**REQ-M2-PROTO-004** — Request frames MUST carry a non-zero `request_id` chosen by the client and a `shard_id` in `0..1024`.

**REQ-M2-PROTO-005** — Supported request operations are GET, SET, DELETE, and deterministic single-shard BATCH. Unknown operation codes MUST return a protocol error or close the connection according to the malformed-frame rules; they MUST NOT be interpreted as another operation.

**REQ-M2-PROTO-006** — Keys and values remain arbitrary byte strings. The codec MUST be length-delimited and MUST NOT require UTF-8.

**REQ-M2-PROTO-007** — BATCH MUST contain only SET/DELETE mutations and MUST be rejected before application if any encoded key maps to a shard different from the request `shard_id`.

## 4. Framing and bounds

**REQ-M2-BOUND-001** — The server MUST enforce a configured maximum frame size before allocating payload storage for the full frame.

**REQ-M2-BOUND-002** — The server MUST enforce configured maximum key size, value size, batch mutation count, and aggregate batch payload size.

**REQ-M2-BOUND-003** — Invalid lengths, integer overflow, truncated frames, impossible enum values, and reserved-bit violations MUST fail safely and MUST NOT panic the process.

**REQ-M2-BOUND-004** — A client MUST NOT be able to create an unbounded per-connection queue through pipelining. The maximum number of admitted in-flight requests per connection MUST be explicit and bounded.

## 5. Pipelining and ordering

**REQ-M2-PIPE-001** — A connection MAY carry multiple outstanding requests without waiting for earlier responses.

**REQ-M2-PIPE-002** — Responses MUST echo the request `request_id`, allowing responses to be correlated independently of transport order.

**REQ-M2-PIPE-003** — The server MAY complete independent requests out of response order, but operations admitted to one shard MUST retain the M1 owner-queue ordering/atomicity semantics.

**REQ-M2-PIPE-004** — Duplicate simultaneous in-flight `request_id` values on the same connection MUST be rejected as a protocol/request error. M2 does not add exactly-once execution semantics.

**REQ-M2-PIPE-005** — Closing a connection MUST NOT revoke a mutation that was already successfully admitted to its shard owner. Cancellation before successful admission MUST not apply the mutation, preserving M1 semantics.

## 6. Routing and errors

**REQ-M2-ROUTE-001** — The server MUST recompute the expected logical shard from raw key bytes and reject a request whose supplied `shard_id` is inconsistent.

**REQ-M2-ROUTE-002** — The protocol MUST define explicit status codes for at least: success, not-found, wrong-shard, stale-route/not-owner, overloaded, closed/unavailable, malformed-request, unsupported-version, and internal-error.

**REQ-M2-ROUTE-003** — Routing failures MAY include a newer route/map version and endpoint hint, but such hints are advisory in M2 and MUST NOT claim Raft leadership before M3/M4.

**REQ-M2-ROUTE-004** — PUT, DELETE, and deterministic single-shard BATCH remain retry-safe because their command semantics are idempotent. M2 MUST NOT advertise general exactly-once execution.

## 7. Backpressure and resource ownership

**REQ-M2-BP-001** — Socket read progress MUST be coupled to bounded per-connection admission so a slow shard or saturated server cannot accumulate unbounded decoded requests.

**REQ-M2-BP-002** — Shard `QueueFull`/`Closed` outcomes MUST map to explicit protocol statuses rather than silent drop or indefinite buffering.

**REQ-M2-BP-003** — Response buffering MUST be bounded. A persistently slow client MUST eventually experience bounded backpressure or connection termination without unbounded server memory growth.

**REQ-M2-BP-004** — The implementation SHOULD reuse buffers where practical, but zero-copy is not a correctness requirement and MUST NOT weaken ownership/cancellation safety.

## 8. Compatibility

**REQ-M2-COMPAT-001** — The existing gRPC API remains available during M2 as the compatibility/control comparison path unless a later accepted spec removes it.

**REQ-M2-COMPAT-002** — Wire-format changes after Spec 0004 is Verified require a protocol-version change or an accepted backward-compatible amendment.

**REQ-M2-COMPAT-003** — The M2 server MUST keep protocol decoding isolated from shard execution behind a narrow request/response adapter so M3 consensus can replace local execution authority without redesigning framing.

## 9. Performance and observability

**REQ-M2-PERF-001** — The compact path SHOULD reduce healthy local request overhead relative to the existing gRPC path under equivalent M1 semantics; no fixed compact-vs-gRPC speedup is required for acceptance unless amended before verification.

**REQ-M2-PERF-002** — Benchmark evidence MUST include GET/SET/DELETE and a mixed workload, at least 1 and 32 pipeline depth, p50/p95/p99, throughput, failures, payload sizes, and host/toolchain metadata.

**REQ-M2-PERF-003** — Benchmark comparisons MUST use equivalent local consistency/durability semantics and MUST retain the existing gRPC path as the comparison point.

**REQ-M2-PERF-004** — Before Spec 0004 may become Verified, the compact depth-32 path MUST NOT retain the repeatable transport cliff observed in M2-T6. On the frozen 16B-key/64B-value/50k-key local matrix, for every GET/SET/DELETE/80-20 workload the median compact depth-32 throughput across three complete runs MUST be at least the corresponding compact depth-1 throughput, with zero benchmark failures. This is a pipeline-health regression gate, not a public performance claim and not a license to weaken any bound, ordering, cancellation, consistency, or durability semantic.

**REQ-M2-PERF-005** — Any transport tuning used to satisfy `REQ-M2-PERF-004` MUST be explicitly configured or deterministic, exercised on both accepted server and benchmark-client paths as applicable, and verified not to bypass per-connection in-flight limits, bounded response buffering, shard backpressure, or admitted-work cancellation semantics.

**REQ-M2-OPS-001** — The server MUST expose counters/gauges sufficient to observe accepted/rejected frames, active connections, in-flight requests, protocol errors, overload responses, and bytes read/written.

## 10. Acceptance

Spec 0004 may become Verified only when the implementation and tests establish all mandatory protocol, bound, pipelining, routing, backpressure, compatibility, observability, and pipeline-health requirements and retain reproducible benchmark evidence. Passing M2 MUST NOT be interpreted as proving distributed linearizability or durable replicated writes; those belong to M3+.

## 11. Accepted amendment — depth-32 pipeline cliff

M2-T6 retained a repeatable compact depth-32 result of roughly 40.9 ms p50 / 779 ops/s while compact depth 1 sustained roughly 11.2k ops/s and gRPC depth 32 sustained roughly 20–22k ops/s. Because this contradicts the intended healthy pipelined data-plane behavior, verification is intentionally blocked until the cause is diagnosed and corrected under `REQ-M2-PERF-004/005`.

The approximately 40 ms signature is consistent with a small-write TCP delayed-ACK/Nagle interaction, and the current compact benchmark/server paths do not explicitly configure `TCP_NODELAY`; this is a hypothesis to test, not an implementation mandate. A fix MUST remain bounded to transport/pipeline mechanics and MUST NOT weaken the verified M1 semantics or M2 resource bounds.