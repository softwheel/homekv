# Spec 0004 — Compact Pipelined Data Plane Verification

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #27

## Requirement matrix

| Requirement | Verification |
| --- | --- |
| REQ-M2-PROTO-001..007 | golden vectors, round-trip codec tests, operation/body validation |
| REQ-M2-BOUND-001..004 | oversized/truncated/overflow/boundary tests; bounded in-flight stress |
| REQ-M2-PIPE-001..005 | pipelining/correlation/order/duplicate-ID/cancellation tests |
| REQ-M2-ROUTE-001..004 | shard recomputation, cross-shard batch rejection, status mapping, retry-semantics review |
| REQ-M2-BP-001..004 | saturated shard, slow reader/writer, queue bound and disconnect tests |
| REQ-M2-COMPAT-001..003 | gRPC regression suite plus adapter/source review |
| REQ-M2-PERF-001..003 | repeated compact-vs-gRPC benchmark under equivalent M1 semantics |
| REQ-M2-PERF-004/005 | before/after depth-32 pipeline diagnosis plus three-run compact pipeline-health gate |
| REQ-M2-OPS-001 | metric state-transition tests |

## V1 — Wire compatibility

Verify exact checked-in bytes for representative GET, SET, DELETE, BATCH, success, not-found, overload, wrong-shard and unsupported-version frames. Golden vectors freeze:

- magic `0x484b`;
- version `1`;
- request/response kind values;
- operation codes;
- status codes;
- big-endian integer encoding.

Changing these values after verification requires a version change or accepted compatible amendment.

## V2 — Decoder safety and bounds

Exercise zero/maximum/maximum+1 lengths and malformed prefixes/bodies. Include:

- oversized `payload_len` rejected before full payload allocation;
- key/value/batch count/aggregate limits;
- truncated prefix/body;
- integer arithmetic overflow attempts;
- invalid op/mutation/status/reserved bits;
- arbitrary non-UTF8 key/value bytes.

No input may panic the process. Stream-desynchronizing malformed frames must close the connection deterministically.

## V3 — Pipelining and correlation

At configurable in-flight limit N:

- send N distinct requests without awaiting responses;
- prove the server never admits >N connection requests simultaneously;
- permit cross-shard completion/response reordering and correlate by `request_id`;
- prove same-shard mutation history remains explainable by M1 owner order;
- reject duplicate active request IDs;
- allow request-ID reuse only after the earlier request has completed and left the in-flight set.

## V4 — Cancellation and backpressure

With tiny deterministic bounds:

- disconnect before shard admission and prove mutation does not apply;
- disconnect after successful shard admission and prove accepted mutation applies exactly once;
- saturate a shard and prove explicit OVERLOADED instead of unbounded staging;
- stop reading responses and prove response memory remains bounded / connection eventually backpressures or closes;
- prove socket application reads stop advancing when per-connection permits are exhausted.

## V5 — Routing and shard safety

Verify:

- correct raw key + shard is admitted;
- incorrect supplied shard is rejected before mutation;
- a BATCH containing any foreign-shard key is rejected atomically before admission;
- routing hints are absent/advisory unless supplied by an actual provider;
- M2 does not claim Raft leadership, quorum state or distributed durability.

## V6 — Existing API compatibility

Run the existing gRPC GET/SET/DELETE and M0/M1 smoke gates unchanged. Both gRPC and compact paths must reach the same M1 local store semantics. M2 must not remove or silently alter the external gRPC contract.

## V7 — Observability

For controlled connection/request sequences, assert exact or monotonic expected changes to:

- active connections;
- accepted/rejected frames;
- protocol errors;
- unsupported versions;
- overload responses;
- current/peak in-flight requests;
- bytes read/written.

## V8 — Repeated benchmark

Capture at least three complete runs from one exact commit for compact and gRPC local paths.

Primary cells:

- 16B key / 64B value;
- GET / SET / DELETE / 80% GET + 20% SET;
- pipeline depth 1 and 32;
- >=10,000 measured operations/cell;
- p50/p95/p99, throughput and failures;
- host/toolchain metadata.

Report median and run spread. Do not select only the fastest repetition. Any claimed speedup must compare equivalent M1 local semantics and must be labeled as a local data-plane engineering result, not durable replicated performance.

## V9 — Depth-32 pipeline health

M2-T6 established a repeatable compact depth-32 cliff (~40.9 ms p50 / ~779 ops/s) that is incompatible with calling the compact path healthy under pipelining. Before verification:

- preserve the original T6 artifact and measurements unchanged as the before-state;
- classify the stall using bounded diagnostics; the ~40 ms shape and lack of explicit `TCP_NODELAY` make delayed-ACK/Nagle interaction a leading hypothesis, not a pre-decided fix;
- prove any correction does not bypass the in-flight semaphore, active request-ID tracking, bounded response channel, shard admission/backpressure, or M1 cancellation/order semantics;
- capture three complete post-fix repetitions from one exact commit with the same frozen T6 matrix;
- require zero benchmark failures;
- for GET, SET, DELETE and 80/20 independently, require median compact depth-32 throughput >= median compact depth-1 throughput.

This gate establishes that request pipelining is not pathologically serialized by transport mechanics. It is not a public speed claim and does not require compact to beat gRPC at depth 32.

## M2 acceptance checklist

Spec 0004 becomes Verified only when all mandatory items are checked:

- [ ] wire golden compatibility verified
- [ ] malformed/oversized/truncated decoding is safe and bounded
- [ ] key/value/batch resource limits verified
- [ ] per-connection in-flight and response buffering are bounded
- [ ] request pipelining/correlation and duplicate-ID semantics verified
- [ ] M1 same-shard ordering and batch atomicity preserved
- [ ] cancellation before/after admission semantics preserved
- [ ] shard-ID recomputation and cross-shard batch rejection verified
- [ ] explicit overload/closed/routing/protocol statuses verified
- [ ] existing gRPC compatibility path remains green
- [ ] protocol metrics verified
- [ ] original 3-run compact-vs-gRPC benchmark evidence retained
- [ ] depth-32 pipeline-health regression gate passes on 3 post-fix runs
- [ ] no OpenRaft/WAL/Multi-Raft/lease-read code mixed into M2

Only after this verification PR merges may #27 close and M3 become unblocked.