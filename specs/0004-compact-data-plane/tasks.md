# Spec 0004 — Compact Pipelined Data Plane Tasks

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #27

Implementation may begin only after this spec is merged to `main` in Accepted state.

## M2-T1 — Wire types and codec

Requirements: `REQ-M2-PROTO-*`, `REQ-M2-BOUND-001/002/003`

- define constants, frame prefix, operation/status enums, request/response models
- implement checked encode/decode over byte slices/buffers
- validate magic/version/reserved fields, lengths, key/value/batch bounds
- add golden wire vectors and malformed/truncation/overflow tests

Completion: deterministic round-trip/golden tests pass and fuzz/property-style malformed inputs do not panic or allocate beyond configured bounds.

## M2-T2 — Bounded connection runtime

Requirements: `REQ-M2-BOUND-004`, `REQ-M2-PIPE-*`, `REQ-M2-BP-*`

- TCP listener and incremental frame reader
- per-connection in-flight semaphore and duplicate request-ID tracking
- bounded response channel and single writer task
- cancellation-before-admission / accepted-work-after-disconnect tests
- slow-client and saturation tests

Completion: tests prove bounded in-flight/request/response storage and M1 cancellation boundary preservation.

Prerequisite: M2-T1.

## M2-T3 — Shard execution adapter and routing errors

Requirements: `REQ-M2-ROUTE-*`, `REQ-M2-COMPAT-003`

- translate decoded requests to the existing `ShardStore` API
- recompute/validate shard mapping before admission
- prevalidate BATCH shard membership
- map `QueueFull`, `Closed`, wrong-shard and application results to stable protocol statuses
- keep routing-hint provider abstract/non-authoritative in M2

Completion: integration tests exercise GET/SET/DELETE/BATCH and all mandatory status mappings without introducing consensus/placement authority.

Prerequisite: M2-T1/T2.

## M2-T4 — Compatibility and server integration

Requirements: `REQ-M2-COMPAT-001/002`, parent `REQ-SDD-003`

- run compact server beside current gRPC service
- configuration for bind address and resource bounds
- retain gRPC behavior/tests unchanged
- graceful lifecycle integration without changing M1 shard shutdown semantics

Completion: both protocols operate against the same local shard engine and existing gRPC tests remain green.

Prerequisite: M2-T2/T3.

## M2-T5 — Observability

Requirements: `REQ-M2-OPS-001`

- counters for frames/requests accepted and rejected
- protocol/malformed/unsupported-version errors
- overload/closed responses
- active connections and current/peak in-flight requests
- bytes read/written

Completion: deterministic metric tests cover success, protocol rejection, overload, disconnect and lifecycle paths.

Prerequisite: M2-T2/T3.

## M2-T6 — Repeated comparative benchmark

Requirements: `REQ-M2-PERF-*`

- compact client benchmark harness
- equivalent gRPC comparison path
- 16B/64B GET/SET/DELETE/80-20
- pipeline depth 1 and 32
- >=10k measured operations/cell
- 3 repetitions with exact commit and host metadata
- retain all artifacts and run spread

Completion: reproducible comparative evidence exists; conclusions explicitly state local M1 semantics and do not claim replicated durability/linearizability.

Prerequisite: M2-T4/T5.

## M2-T7 — Verification handoff

- execute `verification.md` matrix
- record exact commits, CI/workflow runs and benchmark artifacts
- document compatibility surface (magic/version/op/status values)
- separate correctness evidence from performance observations
- move Spec 0004 to Verified only when every mandatory gate passes
- close #27 and unblock M3 only after verification merge

Prerequisite: M2-T1..T6.
