# Spec 0005 — One-Shard Three-Node OpenRaft Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #38

## 1. Architecture

M3 introduces exactly one replicated HomeKV shard and deliberately keeps the M4 many-group problem out of scope.

```text
compact/gRPC ingress
        |
        v
 replicated-shard adapter
        |
        +---- GET ----> OpenRaft safe read barrier ----> applied-state read
        |
        +-- mutation -> OpenRaft client_write
                         |       |       |
                         v       v       v
                       node1   node2   node3
                         |       |       |
                      durable HomeKV Raft log/vote store
                         |       |       |
                         +--- committed entry -----------+
                                      |
                                      v
                            HomeKV shard state machine
```

The compact M2 framing remains isolated from execution authority. The M3 adapter replaces local-only authority for the replicated shard without changing the frozen compact wire format.

## 2. OpenRaft pin and owned boundaries

M3 pins `openraft = "=0.9.25"`. The stable 0.9 line is selected rather than a 0.10 alpha, and the exact patch is frozen because OpenRaft documents pre-1.0 API instability.

HomeKV defines a dedicated Raft type configuration with stable `u64` node IDs and application-owned command/response, node metadata, entry, snapshot-data, async runtime, and responder types as required by OpenRaft 0.9.25.

HomeKV owns:

- log/vote persistence and durable flush scheduling;
- state-machine application and membership tracking;
- snapshot bytes, metadata, integrity and atomic install;
- Raft RPC transport/connection limits;
- client routing/error translation;
- metrics translation;
- all database semantics.

No OpenRaft type becomes part of HomeKV's public data-plane compatibility contract.

## 3. Command model

The replicated application command is deterministic and contains only logical operation data required by the state machine:

```text
Command::Set { key, value }
Command::Delete { key }
Command::Batch { mutations: [Set|Delete, ...] }
```

M2 `request_id` remains correlation metadata and is not used as an exactly-once token. Cross-shard batches remain rejected before Raft admission.

Raft membership/blank entries are handled according to OpenRaft entry semantics and never decoded as HomeKV commands.

Application responses map to the existing HomeKV result vocabulary. A committed SET/DELETE/BATCH response becomes externally successful only after the durable/apply boundary described below.

## 4. State-machine adapter

The M3 state machine wraps a single M1-compatible logical shard state image and serializes committed application through OpenRaft's `RaftStateMachine::apply` contract.

For each applied entry it updates:

1. last-applied log identity;
2. membership metadata when the entry is membership-bearing;
3. HomeKV application state for normal commands;
4. the corresponding application response.

The state-machine lock/owner boundary must permit a coherent snapshot point. No client mutation can directly modify the replicated shard state outside committed `apply`.

The M1 local `ShardStore` remains available for its verified tests and non-M3 local code paths, but the M3 replicated adapter is the only authority for the M3 shard.

## 5. Durable log storage

M3 implements the split OpenRaft storage API (`RaftLogStorage` + `RaftStateMachine`) instead of the deprecated monolithic storage API.

### 5.1 Persistent records

The first M3 durable store uses a simple correctness-first append-oriented format with explicit record envelope:

```text
magic | format_version | record_kind | payload_len | payload | checksum
```

Record kinds cover at least vote, Raft entry, truncation/purge metadata as required by the chosen representation, committed-progress metadata if used for restart reconstruction, and snapshot metadata/pointer state.

The implementation may use separate files for vote metadata and log segments if that makes atomic replacement/flush behavior clearer. Format details that affect recovery are frozen in implementation tests before M3 verification.

### 5.2 Flush contract

OpenRaft 0.9.25 requires `save_vote()` to persist the vote before returning. HomeKV therefore writes the new vote representation and performs the configured durable flush before reporting success.

For `append(entries, LogFlushed)`, HomeKV:

1. validates ordering/no-hole constraints;
2. appends encoded records to the in-process/log-file state;
3. returns from the async append once entries are readable through the log reader, as OpenRaft permits;
4. schedules/executes the durable flush;
5. invokes `LogFlushed` only after the OS durable operation succeeds for the relevant bytes/metadata;
6. surfaces I/O failure without firing a false durable-success callback.

A follower is counted by Raft toward durable replicated progress only according to OpenRaft's persisted-log callback protocol. HomeKV does not synthesize acknowledgement ahead of that callback.

### 5.3 Truncate and purge

Truncate/purge must be crash-safe enough that restart yields one coherent hole-free logical log. M3 favors a simple serialized metadata/update path over aggressive reclamation. Advanced segment recycling belongs to M5.

## 6. Write path and acknowledgement

Mutation flow:

1. ingress validates frame, key/shard mapping and bounds as in M2;
2. replicated adapter verifies the request targets the M3 shard;
3. request is submitted to OpenRaft's client-write API;
4. OpenRaft establishes current leader authority and replicates the entry;
5. HomeKV storage callbacks certify durable persistence only at the accepted disk boundary;
6. OpenRaft commits and invokes state-machine application;
7. leader returns the state-machine response after its local apply completes;
8. HomeKV translates leader-forward/not-leader/fatal/storage errors to safe existing statuses.

The implementation must test the actual OpenRaft callback/completion ordering instead of assuming `client_write` alone proves HomeKV's durability semantics.

## 7. Linearizable GET

M3 uses OpenRaft 0.9.25's quorum-backed read mechanism. The default implementation calls `ensure_linearizable().await` or an equivalent composition of `get_read_log_id()` plus an applied-index wait.

Only after the barrier succeeds and local applied progress reaches the required log position does HomeKV read the in-memory state.

Follower-local strong reads and lease-only shortcuts are disabled. A non-leader returns a safe routing/not-owner result rather than reading stale state.

## 8. Three-node formation

The M3 verification topology is fixed to three voters with node IDs 1, 2, and 3 by default in deterministic tests. Node endpoints are explicit configuration.

One bootstrap coordinator attempts OpenRaft initialization with the complete initial three-node membership. Initialization is treated as an idempotent cluster-formation operation: already-initialized responses are reconciled with the expected cluster identity/membership; incompatible bootstrap identity fails closed.

M3 does not implement a general placement service. The three-node membership is configuration for one group only.

## 9. Raft network adapter

The network layer implements the RPCs required by OpenRaft 0.9.25, including vote, append entries/heartbeats, and snapshot transfer.

Production/local runtime transport may initially use a dedicated internal RPC codec rather than reuse the public compact data plane. The critical design requirements are:

- one bounded connection pool/endpoint state per peer;
- bounded outstanding RPCs and snapshot-transfer buffers;
- explicit connect/request deadlines;
- transport errors returned to OpenRaft unchanged in meaning;
- no authority inferred from transport/gossip state.

Tests use an in-process deterministic transport facade with per-link drop/partition/delay switches. The facade exercises the same OpenRaft network abstraction and is never a separate consensus implementation.

## 10. Snapshot model

Snapshot bytes encode a coherent image of:

- format/version;
- logical shard ID;
- last applied log identity;
- stored membership;
- complete key/value state;
- integrity metadata.

Snapshot building obtains a coherent state-machine view. The first implementation may copy one M3 shard to build the snapshot; avoiding that copy is an optimization, not a correctness prerequisite.

Installation is staged:

1. receive into a bounded temporary object/file;
2. validate envelope, version, length and checksum;
3. decode full state plus metadata;
4. atomically swap the HomeKV state-machine image and snapshot pointer under the state-machine ownership boundary;
5. only then expose the installed applied/membership position;
6. remove obsolete snapshot state when safe.

Crash-safe snapshot-file replacement uses temp-write + durable flush + atomic rename + parent-directory sync where filesystem semantics require it. M5 may optimize layout but not weaken this boundary.

## 11. Recovery

On process start:

1. validate durable vote/log metadata and latest complete snapshot;
2. restore snapshot state if present;
3. expose the correct last-applied and membership metadata to OpenRaft;
4. make valid post-snapshot durable logs readable;
5. let OpenRaft restore/reconcile committed state and apply only justified committed entries;
6. join elections/replication only after storage/state-machine initialization succeeds.

Corruption, holes, impossible log ordering, or incompatible versions are startup errors. HomeKV never silently truncates unknown corruption and serves traffic.

## 12. Backpressure and cancellation

The M2 connection in-flight bound remains intact. The replicated adapter adds a separate configured bound for client writes/reads awaiting Raft completion.

Raft network pools and per-peer outstanding work are bounded. Snapshot transfer has explicit chunk/buffer bounds.

Once a mutation is admitted to OpenRaft, client disconnect does not cancel Raft commitment/application. Before admission, normal request cancellation may prevent submission. This preserves the established retry model.

## 13. Failure behavior

### Leader isolation

A former leader isolated from both peers cannot form a quorum and therefore cannot durably commit/acknowledge new writes. Its uncommitted suffix may later be overwritten through normal Raft reconciliation.

### One-node failure

Two healthy voters retain quorum, elect/retain a leader, and continue strong operations subject to configured timeouts.

### Quorum loss

Strong reads/writes fail or remain pending only within bounded request deadlines; no fallback to local stale reads or local writes occurs.

### Storage failure

Vote/log flush errors surface as storage/fatal errors into OpenRaft and HomeKV health. They never trigger success callbacks.

## 14. Observability

HomeKV consumes OpenRaft metrics/watch state but translates it to stable database-oriented fields:

- node/replica ID and role;
- leader ID;
- vote/term-equivalent identity;
- last log, committed and applied positions;
- replication progress/lag per peer;
- membership;
- snapshot state;
- election/leadership-change counters;
- Raft RPC failures/latency;
- durable append/flush latency and errors;
- state-machine apply latency;
- admission/network queue depth and rejections.

## 15. Test architecture

A deterministic three-node harness owns three independent durable directories, clocks/timeouts where injectable, network link controls, and client helpers. It must be able to:

- wait for exactly one leader;
- partition arbitrary directed/bidirectional links;
- kill/restart a node preserving disk state;
- inspect committed/applied progress;
- force enough log growth to exercise snapshot transfer;
- capture operation histories for a linearizability checker.

Fault tests assert state/history invariants, not merely sleep-based expectations.

## 16. Alternatives

### Memory-only OpenRaft store

Rejected for M3 verification because Spec 0001 requires durable replicated acknowledgements.

### `raft-rs::RawNode`

Rejected by the parent accepted spec for M3. It remains an escape option only through a future accepted amendment if the M4 scaling gate justifies it.

### OpenRaft 0.10 alpha

Rejected for the first M3 implementation because 0.9.25 is the latest stable 0.9 release at spec acceptance; M3 values a reproducible stable integration baseline over alpha API churn.

### Lease reads

Deferred because M3 first proves quorum-backed linearizable reads without relying on timing/lease assumptions.

### Reusing gossip for elections

Rejected. Gossip is advisory only; Raft membership and leadership are authoritative.

## 17. Requirement mapping

- `REQ-M3-RAFT-*` -> sections 2, 9
- `REQ-M3-SM-*` -> sections 3–4
- `REQ-M3-WRITE-*` -> sections 6, 12–13
- `REQ-M3-DUR-*` -> sections 5, 11
- `REQ-M3-READ-*` -> section 7
- `REQ-M3-MEM-*` -> section 8
- `REQ-M3-NET-*` -> sections 9, 12
- `REQ-M3-SNAP-*` -> sections 10–11
- `REQ-M3-FAIL-*` -> section 13
- `REQ-M3-OPS-*` -> section 14
- `REQ-M3-PERF-*` -> correctness-first boundaries throughout; benchmarks in tasks/verification