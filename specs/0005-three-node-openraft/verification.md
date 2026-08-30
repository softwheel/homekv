# Spec 0005 — One-Shard Three-Node OpenRaft Verification

- Status: Accepted verification plan; implementation not yet verified
- Requirements: `requirements.md`
- Design: `design.md`
- Tasks: `tasks.md`
- Tracking issue: #38

## 1. Verification rule

M3 is Verified only when every mandatory requirement below has retained evidence on one exact implementation commit and the required repository regression gates pass. Timing-only distributed tests are insufficient for consistency claims; histories and persisted state must be checked against explicit invariants/models.

The verified scope is exactly one logical shard replicated by three voters. Evidence MUST NOT be generalized to M4's many-group architecture or to public performance claims.

## 2. Required environment record

The M3 verification record must capture:

- exact HomeKV commit SHA;
- exact OpenRaft version and Cargo lock resolution;
- Rust toolchain and target;
- OS/kernel;
- CPU and memory metadata;
- filesystem used for durable tests;
- three-node topology and endpoint configuration;
- Raft election/heartbeat/request timeouts;
- foreground/Raft network queue bounds;
- benchmark key/value sizes, keyspace, concurrency and operation mix;
- durability mode = quorum durable + leader apply;
- read mode = quorum-backed linearizable;
- replication factor = 3.

## 3. Requirement-to-evidence matrix

| Requirement | Required evidence | State |
|---|---|---|
| `REQ-M3-RAFT-001` | lockfile/dependency test shows exact OpenRaft 0.9.25 | Pending |
| `REQ-M3-RAFT-002..003` | adapter boundary review + tests proving no direct strong mutation bypass | Pending |
| `REQ-M3-SM-001..005` | deterministic apply/order/membership/applied-state unit + property tests | Pending |
| `REQ-M3-WRITE-001..005` | replicated CRUD/batch, non-leader, minority, cancellation tests | Pending |
| `REQ-M3-DUR-001` | vote crash/reopen and injected flush-failure tests | Pending |
| `REQ-M3-DUR-002` | append readability + `LogFlushed` durability-order tests | Pending |
| `REQ-M3-DUR-003..005` | truncate/purge/no-hole plus corruption/truncation/version tests | Pending |
| `REQ-M3-DUR-006..007` | restart/replay model-state tests excluding uncommitted suffix | Pending |
| `REQ-M3-READ-001..004` | safe-barrier integration + linearizable history checks + follower rejection | Pending |
| `REQ-M3-MEM-001..004` | deterministic 3-voter bootstrap/repeat/incompatible-init tests | Pending |
| `REQ-M3-NET-001..004` | RPC coverage, bounded queues and deterministic partition controls | Pending |
| `REQ-M3-SNAP-001..005` | snapshot round-trip/install/catch-up/corruption/crash tests | Pending |
| `REQ-M3-FAIL-001..004` | leader loss, old-leader isolation, quorum loss, restart tests | Pending |
| `REQ-M3-OPS-001..003` | metrics/state assertions during role/failure/storage transitions | Pending |
| `REQ-M3-PERF-001..003` | retained RF=3 durable/linearizable 3-run engineering benchmark | Pending |

## 4. State-machine verification

Tests must cover at minimum:

1. SET replaces/creates exactly one key;
2. DELETE of present and absent keys is deterministic;
3. BATCH applies all mutations atomically in one committed entry;
4. committed entries applied in log order yield the same model state;
5. membership/blank entries never mutate KV data;
6. last-applied identity advances consistently with application;
7. snapshot restore plus subsequent committed entries yields the same model state as uninterrupted application.

Property tests should generate command sequences and compare the HomeKV state machine against a simple reference map/batch model.

## 5. Persistence-boundary verification

### Vote durability

Test sequence:

1. persist vote A;
2. wait for `save_vote()` success;
3. simulate immediate process loss/reopen;
4. assert vote A is recovered;
5. inject write/flush failure and assert `save_vote()` returns failure and no success state is claimed.

### Log-flush ordering

Instrument the store with deterministic hooks:

1. call append with entry E and capture `LogFlushed`;
2. establish E is readable after append returns;
3. hold the underlying durable-flush completion;
4. assert callback has not fired;
5. complete durable flush successfully;
6. assert callback fires once;
7. reopen storage and assert E is valid;
8. repeat with injected flush failure and assert no false successful callback.

### Corruption and holes

Exercise:

- truncated final record;
- bit-corrupted payload/checksum;
- unknown format version;
- impossible record length;
- invalid/hole-producing log sequence;
- crash around truncate/purge metadata update.

All must fail closed or recover to the last explicitly valid state allowed by the storage format; none may silently apply corrupt content.

## 6. Three-node consensus verification

Healthy-cluster tests must establish:

- exactly one leader becomes authoritative;
- writes submitted to the leader replicate and apply on all healthy replicas eventually;
- application order is identical across replicas;
- follower/non-leader strong mutations return safe failure/redirect and never mutate local application state directly;
- one unavailable voter still permits the two-node quorum to make progress;
- no test helper grants authority outside OpenRaft.

## 7. Linearizability verification

Use a model/history checker over operation intervals and results for concurrent GET/SET/DELETE (and deterministic batches where supported by the checker model).

Mandatory histories:

1. healthy leader with concurrent readers/writers;
2. leader isolated during active operations, followed by a new leader;
3. old leader reconnecting with an uncommitted suffix;
4. one follower slow/delayed while quorum remains healthy.

A GET result is accepted only if the completed operation history has a valid per-shard linearization respecting real-time precedence. Tests must also assert the implementation actually traverses the safe OpenRaft read barrier; a passing history by luck is not enough.

Follower-local reads presented as default strong GET must be rejected by test.

## 8. Failure matrix

| Fault | Expected invariant |
|---|---|
| kill current leader | healthy quorum elects new leader; acknowledged state remains available |
| isolate leader from both peers | isolated node cannot acknowledge new strong writes |
| isolate one follower | remaining quorum continues safely |
| split 1+2 | only 2-node side may make progress after election |
| lose quorum | no successful strong write; no stale-read fallback |
| heal partition | replicas converge; conflicting uncommitted suffix is not applied |
| restart follower | catches up from log or snapshot without speculative state |
| restart former leader | recovers durable state and rejoins under current consensus authority |
| storage flush failure | affected operation does not receive false durable success |
| corrupt log/snapshot | node fails closed rather than serving corrupt state |

Tests should use explicit cluster-state predicates and bounded waits, not fixed sleeps as proof.

## 9. Snapshot/recovery verification

Mandatory scenarios:

- build snapshot at a known applied position and round-trip contents/metadata;
- continue writes while/after snapshot creation and confirm snapshot remains coherent;
- install snapshot on a lagging replica and then catch up subsequent entries;
- restart from snapshot + subsequent durable log and compare with committed reference state;
- interrupt snapshot reception before completion and assert old state remains authoritative;
- corrupt/truncate snapshot and assert install/restart rejection;
- incompatible snapshot version fails closed;
- exercise atomic replacement/crash boundary where the platform test harness permits.

## 10. Backpressure and resource verification

Tests must deliberately saturate:

- foreground operations awaiting Raft;
- per-peer Raft RPC capacity;
- slow/unreachable peer transport;
- snapshot transfer buffers.

Evidence must show configured bounds are respected and saturation surfaces through backpressure/error/timeout without an unbounded queue. Existing M2 connection bounds remain active.

## 11. Observability verification

During deterministic transitions, assert HomeKV's exposed state changes coherently for:

- follower/candidate/leader role;
- leader ID;
- log/commit/applied progress;
- replication lag;
- elections/leadership changes;
- RPC failures;
- durable flush latency/error;
- snapshot build/install;
- admission/transport saturation.

Tests should verify HomeKV-owned representations rather than snapshot-testing OpenRaft internal Rust debug formats.

## 12. Engineering benchmark gate

M3-T6 retains three complete runs of an RF=3 configuration for at least:

- GET;
- SET;
- DELETE;
- 80/20 read/write mix;

using documented key/value sizes and at least low and moderate client concurrency. Report p50/p95/p99, throughput and failures for each cell.

This benchmark is a regression/engineering characterization. Spec 0005 does not impose a fixed speedup or latency number because the first M3 objective is proving the strong durable contract. Any later optimization must preserve the exact tested consistency/durability semantics.

Zero benchmark failures are required for a run to count as retained evidence.

## 13. Regression gates

Every implementation/verification PR must run the repository's normal Rust CI and preserve the existing M0/M1/M2 required smoke/regression gates. M3 work must not rewrite the frozen M0 baseline or relax previously Verified assertions.

Before the final verification PR:

- M0 remains Verified and unchanged except non-semantic test harness maintenance if separately justified;
- M1 shard semantics tests pass;
- M2 codec/runtime/routing/pipeline tests pass;
- the prior M2 pipeline-health regression remains passing where exercised by required CI.

## 14. Final M3 verification record

M3-T7 will replace `Pending` states with PASS/FAIL and record:

- exact tested SHA;
- PRs for T1–T6;
- workflow run IDs and retained artifact IDs/digests;
- requirement-specific test names/evidence;
- three-run benchmark summary;
- residual risks and explicitly deferred M4/M5 work.

Only then may the spec status change to **Verified** and issue #38 close.