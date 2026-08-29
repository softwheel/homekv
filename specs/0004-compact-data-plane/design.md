# Spec 0004 — Compact Pipelined Data Plane Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #27

## 1. Architecture

M2 adds a dedicated compact TCP data-plane server beside the existing gRPC service. Both terminate at the same verified M1 `ShardStore` adapter. The protocol layer owns framing, decoding, per-connection bounds, response correlation, and routing/error translation; it does not own shard state or distributed authority.

```text
client
  -> TCP connection
  -> bounded frame decoder
  -> per-connection in-flight permit
  -> request decoder + shard validation
  -> M1 ShardStore adapter
  -> response encoder
  -> bounded writer
```

The gRPC path remains intact for compatibility and apples-to-apples local benchmarking.

## 2. Wire format

All fixed-width integers are big-endian.

### Frame prefix (12 bytes)

- `magic: u16` — fixed HomeKV marker `0x484b` (`HK`)
- `version: u8` — M2 protocol version `1`
- `kind: u8` — request=`1`, response=`2`
- `flags: u16` — zero in v1; non-zero reserved bits are rejected
- `payload_len: u32`
- `reserved: u16` — zero in v1

The prefix is parsed into stack/local scalar fields before any payload allocation. `payload_len` is checked against the configured maximum first.

### Request payload

Common header:

- `request_id: u64` (non-zero)
- `shard_id: u16`
- `op: u8` — GET=`1`, SET=`2`, DELETE=`3`, BATCH=`4`
- `op_flags: u8` — zero in v1

Operation bodies are explicit length-delimited byte sequences:

- GET/DELETE: `key_len:u32 | key`
- SET: `key_len:u32 | value_len:u32 | key | value`
- BATCH: `mutation_count:u16`, followed by mutation records `{kind:u8, reserved:u8, key_len:u32, value_len:u32, key, value}`; DELETE requires `value_len=0`.

### Response payload

- `request_id: u64`
- `status: u16`
- `response_flags: u16` (zero in v1)
- optional status-specific body

GET success includes `value_len:u32 | value`. `not_found` has no value. Routing statuses may include `route_version:u64` and an optional length-delimited endpoint hint. Error strings are not required on the hot path; diagnostics belong in server logs/metrics.

## 3. Connection execution model

Each accepted socket owns:

1. one incremental frame reader;
2. a bounded semaphore limiting admitted in-flight requests;
3. a set/map of currently in-flight request IDs bounded by the semaphore;
4. a bounded response channel;
5. one writer task that serializes complete response frames.

The reader acquires an in-flight permit before admitting another decoded request. When all permits are consumed it stops advancing application reads until a request completes. This couples network ingestion to bounded execution without creating an unbounded staging queue.

Requests may execute concurrently across shards. The protocol does not impose connection-wide execution ordering. A request's response carries its ID, so response order may differ from request order. Within one shard, all state changes still serialize through the M1 owner queue.

## 4. Cancellation semantics

Before a command is successfully admitted to `ShardStore`, connection cancellation can discard it. Once admitted, dropping the connection/response path does not revoke the command. This preserves the M1 accepted-work boundary exactly.

Writer failure drops pending response delivery but must not cancel already admitted state-machine work.

## 5. Routing model

M2 validates logical shard identity using the verified `shard_for_key(raw_key)` function. For BATCH every key must map to the supplied shard before the batch is admitted.

M2 introduces protocol representations for `wrong_shard` and `stale_route/not_owner`, but it does not invent distributed authority. Before M3/M4, the active local server may only emit route metadata that it actually has. The framing and adapter are designed so a later authoritative placement/leader resolver can be inserted without changing request encoding.

## 6. Status mapping

Suggested stable v1 status numbers:

- `0` OK
- `1` NOT_FOUND
- `2` WRONG_SHARD
- `3` STALE_ROUTE_OR_NOT_OWNER
- `4` OVERLOADED
- `5` CLOSED_OR_UNAVAILABLE
- `6` MALFORMED_REQUEST
- `7` UNSUPPORTED_VERSION
- `8` INTERNAL_ERROR
- `9` DUPLICATE_INFLIGHT_REQUEST_ID

Status values are protocol compatibility surface once Verified.

Malformed framing that destroys stream synchronization (bad magic, impossible prefix, oversized frame) closes the connection after metrics/logging. A well-framed but semantically invalid request may receive a correlated error response and keep the connection open.

## 7. Bounds

Initial defaults are implementation configuration, not permanent wire guarantees:

- max frame: 8 MiB
- max key: 64 KiB
- max value: 4 MiB
- max batch mutations: 1,024
- max aggregate batch payload: 8 MiB
- max in-flight requests/connection: 256
- bounded response queue: at most the in-flight limit

Tests must use much smaller configurable bounds to deterministically prove saturation behavior.

## 8. Codec implementation

Use explicit encode/decode code over byte slices/buffers rather than introducing a schema runtime on the hot path. `bytes::BytesMut` or equivalent reusable buffers are preferred. Parsing must use checked arithmetic and reject trailing/short data according to each operation schema.

No `unsafe` code is required for M2. No zero-copy optimization is accepted if it makes request lifetime extend ambiguously across cancellation or shard ownership.

## 9. Benchmark design

Add a compact-protocol client/harness that drives localhost TCP and compare against the existing gRPC benchmark path under the same M1 local semantics.

Primary matrix:

- 16B keys / 64B values
- GET, SET, DELETE, 80/20 GET/SET
- pipeline depth 1 and 32
- at least 10,000 measured operations/cell
- 3 repeated runs for comparative evidence
- p50/p95/p99, throughput, failures, bytes, host/toolchain metadata

M2 has no mandatory speedup threshold at acceptance time; correctness/bounds are mandatory. Performance results guide M3/M7 and must not be presented as durable replicated performance.

## 10. Future integration boundary

M3 replaces the local execution-authority implementation behind the request adapter with a one-shard OpenRaft-backed path. M4 adds authoritative placement/routing for many groups. Neither should require a new frame format merely to add consensus.
