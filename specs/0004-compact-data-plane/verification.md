# Spec 0004 — Compact Pipelined Data Plane Verification

- Status: Verified
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #27
- Final M2 implementation merge: `20a80e8ec31332ded09893a4d397f85ece4be699`
- Final M2 implementation head: `f4749a4252871ecdf5da2cd654899cf6043d2dfd`

## Verification conclusion

Spec 0004 satisfies every mandatory M2 requirement. The compact data plane provides checked versioned framing, bounded per-connection pipelining and response buffering, deterministic request correlation, raw-key shard validation, explicit routing/overload/protocol outcomes, shared M1 local-store semantics with the existing gRPC compatibility path, protocol observability, and retained comparative benchmark evidence.

The M2-T6 depth-32 transport cliff was retained as evidence rather than hidden. The accepted T6A amendment isolated the compact small-write path and applied the smallest supported transport correction: explicit `TCP_NODELAY` on accepted compact server sockets and on the compact benchmark client. Post-fix three-run evidence clears the mandatory pipeline-health gate for GET, SET, DELETE, and 80/20 independently with zero failures, without changing queue bounds, response bounds, shard admission, owner ordering, cancellation, consistency, or durability semantics.

M0 and M1 remain unchanged and Verified. M2 does **not** claim replicated durability, distributed linearizability, Raft leadership, placement authority, lease reads, or cross-shard transactions; those remain M3+ concerns.

## Requirement matrix

| Requirement | Verification evidence | Result |
| --- | --- | --- |
| REQ-M2-PROTO-001..007 | `src/data_plane.rs`: fixed 12-byte prefix, magic/version/kind/reserved/payload length, big-endian fixed-width integers, non-zero request IDs, bounded shard IDs, GET/SET/DELETE/BATCH codec, arbitrary byte keys/values; golden/round-trip/invalid-operation tests from M2-T1 | PASS |
| REQ-M2-BOUND-001..003 | checked prefix/body lengths before full-frame acceptance; key/value/batch/aggregate limits; overflow/truncation/reserved/enum rejection; malformed-input no-panic coverage from M2-T1 | PASS |
| REQ-M2-BOUND-004 | `src/data_plane_runtime.rs`: explicit per-connection in-flight semaphore; deterministic saturation tests prove application reads stop advancing when permits are exhausted | PASS |
| REQ-M2-PIPE-001/002 | compact runtime accepts multiple outstanding requests and responses echo `request_id`; benchmark depth 32 exercises actual pipelining/correlation | PASS |
| REQ-M2-PIPE-003 | compact adapter submits through verified M1 `ShardStore`; same-shard mutations retain M1 owner-queue ordering and atomic batch semantics | PASS |
| REQ-M2-PIPE-004 | active request-ID set is bounded by in-flight admission; duplicate simultaneous IDs return `DuplicateInflightRequestId`; reuse is permitted only after completion/removal | PASS |
| REQ-M2-PIPE-005 | deterministic runtime tests cover disconnect before admission vs after successful shard admission; accepted work retains M1 exactly-once application-in-owner-order behavior | PASS |
| REQ-M2-ROUTE-001 | adapter recomputes the logical shard from raw key bytes and rejects mismatched supplied shard IDs before mutation admission | PASS |
| REQ-M2-ROUTE-002 | stable statuses cover OK, NOT_FOUND, WRONG_SHARD, STALE_ROUTE_OR_NOT_OWNER, OVERLOADED, CLOSED_OR_UNAVAILABLE, MALFORMED_REQUEST, UNSUPPORTED_VERSION, INTERNAL_ERROR, plus duplicate active request ID | PASS |
| REQ-M2-ROUTE-003 | route-hint abstraction remains advisory/non-authoritative and M2 introduces no consensus leadership claim | PASS |
| REQ-M2-ROUTE-004 | SET/DELETE/single-shard BATCH remain idempotent command semantics; no general exactly-once protocol claim is introduced | PASS |
| REQ-M2-BP-001 | socket application reads are coupled to the bounded in-flight semaphore; saturation test prevents unbounded decoded staging | PASS |
| REQ-M2-BP-002 | M1 `QueueFull`/`Closed` map to explicit compact overload/unavailable responses | PASS |
| REQ-M2-BP-003 | bounded response channel and single writer impose finite slow-client buffering/backpressure | PASS |
| REQ-M2-BP-004 | transport tuning is limited to `TCP_NODELAY`; ownership/cancellation safety remains unchanged | PASS |
| REQ-M2-COMPAT-001 | compact listener runs beside the unchanged gRPC service against the same verified M1 `ShardStore`; Rust/M0/M1 gates remain green | PASS |
| REQ-M2-COMPAT-002 | wire constants below are frozen by this verification; future incompatible changes require a version change or accepted compatible amendment | PASS |
| REQ-M2-COMPAT-003 | protocol decode/runtime is isolated from shard execution behind the compact request handler/adapter boundary | PASS |
| REQ-M2-PERF-001..003 | PR #34 retained a complete three-run compact-vs-gRPC matrix on one exact commit under equivalent local M1 semantics | PASS |
| REQ-M2-PERF-004 | PR #36 post-fix three-run medians satisfy compact depth32 throughput >= compact depth1 for all four frozen workloads, independently; zero failures | PASS |
| REQ-M2-PERF-005 | explicit `TCP_NODELAY` is configured on accepted server and benchmark-client sockets only; all normal Rust/M0 regression gates remain green | PASS |
| REQ-M2-OPS-001 | M2-T5 metric tests verify accepted/rejected frames/requests, protocol/version errors, overload/closed responses, connection/in-flight gauges, bytes and completion/latency accounting | PASS |

## V1 — Wire compatibility

Verified and frozen for compact protocol version 1:

- magic: `0x484b`;
- version: `1`;
- frame kind: Request=`1`, Response=`2`;
- operation: GET=`1`, SET=`2`, DELETE=`3`, BATCH=`4`;
- mutation kind: SET=`1`, DELETE=`2`;
- status: OK=`0`, NOT_FOUND=`1`, WRONG_SHARD=`2`, STALE_ROUTE_OR_NOT_OWNER=`3`, OVERLOADED=`4`, CLOSED_OR_UNAVAILABLE=`5`, MALFORMED_REQUEST=`6`, UNSUPPORTED_VERSION=`7`, INTERNAL_ERROR=`8`, DUPLICATE_INFLIGHT_REQUEST_ID=`9`;
- fixed-width integers use big-endian network byte order;
- request IDs must be non-zero and shard IDs are `0..=1023`.

Golden vectors and codec round trips from M2-T1 establish the exact checked-in encoding. Any incompatible post-verification change requires a protocol version change or an accepted backward-compatible amendment.

Result: **PASS**.

## V2 — Decoder safety and bounds

M2-T1 verifies configured maximum frame/key/value/batch mutation/aggregate sizes, prefix-first frame-size validation, truncated frame/payload handling, overflow-safe arithmetic, invalid enum/reserved/flag rejection, and arbitrary non-UTF8 key/value bytes. Malformed inputs fail as codec errors rather than panicking the process.

Result: **PASS**.

## V3 — Pipelining and correlation

M2-T2 establishes an explicit per-connection in-flight semaphore, active request-ID set, bounded response channel, and a single response writer. Deterministic tests exercise saturation, duplicate active IDs, correlation, request-ID lifecycle, and admitted-work behavior. M2-T6/T6A additionally exercise depth-32 operation on the real compact benchmark path.

Independent requests may complete out of transport order and are correlated by `request_id`; same-shard mutation execution continues through the verified M1 owner queue.

Result: **PASS**.

## V4 — Cancellation and backpressure

Deterministic T2/T3 coverage verifies:

- disconnect/cancel before successful shard admission does not apply the mutation;
- disconnect after successful admission does not revoke accepted work;
- shard saturation returns explicit `OVERLOADED` rather than creating unbounded staging;
- response buffering is bounded and a slow reader propagates backpressure;
- application reads stop advancing while all per-connection permits are occupied.

No M2 performance change bypasses these boundaries.

Result: **PASS**.

## V5 — Routing and shard safety

M2-T3 verifies raw-key shard recomputation before admission, wrong-shard rejection, atomic pre-rejection of a batch containing any foreign-shard key, M1 `QueueFull`/`Closed` status translation, and advisory routing-hint behavior. Source/scope review confirms no Raft leadership, quorum state, placement authority, WAL, snapshot durability, lease-read, or distributed durability code was introduced by M2.

Result: **PASS**.

## V6 — Existing API compatibility

M2-T4 runs compact and gRPC listeners against the same verified M1 local shard engine. Existing gRPC GET/SET/DELETE semantics remain available and unchanged. Every required Rust workflow for M2-T1 through T6A retained the M0 storage benchmark smoke, M0 storage-memory smoke, and M0 server-memory smoke gates.

Result: **PASS**.

## V7 — Observability

M2-T5 exposes bounded atomic metrics for accepted/rejected frames and requests, protocol/malformed/unsupported-version errors, overload/closed responses, active/peak connections, current/peak in-flight requests, bytes read/written, completed requests, and aggregate handler latency. Deterministic tests cover success, rejection, overload and connection lifecycle transitions.

Result: **PASS**.

## V8 — Original repeated comparative benchmark

Exact retained evidence from PR #34:

- implementation head: `2e2cd3e4de3f26796591ce33a43bccc1bdf1c97a`;
- merge: `ad5c7b6c1be9fefb8a2e6796ed7953b30502c5ed`;
- Rust workflow: `33281024510` — success;
- M2 Comparative Benchmark workflow: `33281024527` — success;
- artifact id: `9723043016`;
- artifact name: `homekv-m2-comparative-5f2566c3d76c1144ee00a6d9add47e49143172b7`;
- artifact digest: `sha256:c8482e6c55bf94938e4370d9395014072ea557a5b1f3de0ec600095d95117fb0`;
- three complete repetitions, >=10,000 measured operations/cell, zero failures;
- matrix: 16B key / 64B value / 50k keys; GET/SET/DELETE/80-20; outstanding depth 1 and 32; compact and gRPC; p50/p95/p99, throughput and environment metadata retained.

This evidence exposed the original repeatable compact depth-32 cliff (~40.9 ms p50 / ~779 ops/s) and is deliberately retained as the before-state.

Result: **PASS as evidence collection; T6A required before final M2 verification**.

## V9 — Depth-32 pipeline health and T6A correction

Exact retained post-fix evidence from PR #36:

- implementation head: `f4749a4252871ecdf5da2cd654899cf6043d2dfd`;
- merge: `20a80e8ec31332ded09893a4d397f85ece4be699`;
- Rust workflow: `33285602793` — success;
- M2 Comparative Benchmark workflow: `33285602860` — success;
- artifact id: `9724348039`;
- artifact name: `homekv-m2-comparative-be8ee5cb5f3907d872bfc4fe4014875df6875cb7`;
- artifact digest: `sha256:67bb4cd59c7813efc3be61b0a5f4e29d1c441b2e15242bda0f5808096c999f46`;
- three complete post-fix repetitions, >=10,000 measured operations/cell, zero failures.

The bounded correction is explicit `TCP_NODELAY` on accepted compact server `TcpStream`s and the compact benchmark client connection. The prior ~40 ms stall disappears. Three-run median throughput is:

| Workload | Compact depth 1 | Compact depth 32 | depth32/depth1 | Gate |
| --- | ---: | ---: | ---: | --- |
| GET | 19,125.5 ops/s | 55,184.5 ops/s | 2.885x | PASS |
| SET | 18,973.5 ops/s | 54,750.5 ops/s | 2.886x | PASS |
| DELETE | 19,425.3 ops/s | 55,988.2 ops/s | 2.883x | PASS |
| 80/20 | 18,965.9 ops/s | 54,860.9 ops/s | 2.893x | PASS |

Corresponding compact depth-32 p50 latency is approximately 250.0 us GET, 255.1 us SET, 245.1 us DELETE, and 254.3 us 80/20. These are local single-node engineering measurements under M1 semantics, not replicated-durability or distributed-linearizability performance claims.

Result: **PASS**.

## M2 acceptance checklist

- [x] wire golden compatibility verified
- [x] malformed/oversized/truncated decoding is safe and bounded
- [x] key/value/batch resource limits verified
- [x] per-connection in-flight and response buffering are bounded
- [x] request pipelining/correlation and duplicate-ID semantics verified
- [x] M1 same-shard ordering and batch atomicity preserved
- [x] cancellation before/after admission semantics preserved
- [x] shard-ID recomputation and cross-shard batch rejection verified
- [x] explicit overload/closed/routing/protocol statuses verified
- [x] existing gRPC compatibility path remains green
- [x] protocol metrics verified
- [x] original 3-run compact-vs-gRPC benchmark evidence retained
- [x] depth-32 pipeline-health regression gate passes on 3 post-fix runs
- [x] no OpenRaft/WAL/Multi-Raft/lease-read code mixed into M2

## Verification decision

**Spec 0004 is Verified.** M2-T1 through M2-T6A satisfy their Accepted requirements and M2-T7 is this verification handoff. After this verification PR merges, issue #27 may close and the v1 umbrella may mark M2 Verified. M3 becomes eligible for specification work only; no M3 semantic implementation is authorized until its own child spec is written and Accepted.
