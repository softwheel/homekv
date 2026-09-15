# Spec 0005 — One-Shard Three-Node OpenRaft Verification

- Status: Verified
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
| `REQ-M3-RAFT-001` | lockfile/dependency test shows exact OpenRaft 0.9.25 | PASS |
| `REQ-M3-RAFT-002..003` | adapter boundary review + tests proving no direct strong mutation bypass | PASS |
| `REQ-M3-SM-001..005` | deterministic apply/order/membership/applied-state unit + property tests | PASS |
| `REQ-M3-WRITE-001..005` | replicated CRUD/batch, non-leader, minority, cancellation tests | PASS |
| `REQ-M3-DUR-001` | vote crash/reopen and injected flush-failure tests | PASS |
| `REQ-M3-DUR-002` | append readability + `LogFlushed` durability-order tests | PASS |
| `REQ-M3-DUR-003..005` | truncate/purge/no-hole plus corruption/truncation/version tests | PASS |
| `REQ-M3-DUR-006..007` | restart/replay model-state tests excluding uncommitted suffix | PASS |
| `REQ-M3-READ-001..004` | safe-barrier integration + linearizable history checks + follower rejection | PASS |
| `REQ-M3-MEM-001..004` | deterministic 3-voter bootstrap/repeat/incompatible-init tests | PASS |
| `REQ-M3-NET-001..004` | RPC coverage, bounded queues and deterministic partition controls | PASS |
| `REQ-M3-SNAP-001..005` | snapshot round-trip/install/catch-up/corruption/crash tests | PASS |
| `REQ-M3-FAIL-001..004` | leader loss, old-leader isolation, quorum loss, restart tests | PASS |
| `REQ-M3-OPS-001..003` | metrics/state assertions during role/failure/storage transitions | PASS |
| `REQ-M3-PERF-001..003` | retained RF=3 durable/linearizable 3-run engineering benchmark | PASS |

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

### 14.1 Decision and exact implementation identity

**Decision: PASS. Spec 0005 is Verified.**

The exact implementation candidate was PR #68 head
`6e9ac6991c7e4f09145db89b9a870ae3c57d367a` on base
`bf515cf0d9e422a806bacd2eac59f9cef9f7161b`. Authoritative Rust workflow
[35016295781](https://github.com/softwheel/homekv/actions/runs/35016295781)
tested merge checkout `47a7d19534612d26a3c811e2c65c5946f40c44af`.
PR #68 merged as `5e2155fbfbfe6672358bd5dd3347e6d08b0285da`.
The tested checkout and merged commit have the same tree
`bfeddca60bb4c6f24a9fd823e9343057bae115c7` and the same ordered parents,
so the merged implementation is byte-for-byte the tested code tree.

Workflow 35016295781 passed the locked build, complete Rust test suite, three
RF=3 benchmark runs, M0 storage benchmark smoke, M0 storage-memory accounting
smoke, and M0 server-memory accounting smoke. Evidence artifact
[10415324683](https://github.com/softwheel/homekv/actions/runs/35016295781/artifacts/10415324683)
has digest
`sha256:264f004c0aff847b9ad8a75c0b91c5a2b7b35384c6193a28577571ad5fae234f`.

### 14.2 Environment and semantic configuration

- OpenRaft: exact `0.9.25`; Cargo.lock SHA-256
  `0e251c98a3a3ce31eaf4c986d92909d37e884ca48ca3a16c5477fbc06015f7d8`.
- Rust: `rustc 1.98.1 (48a229cea 2026-09-01)`,
  `x86_64-unknown-linux-gnu`; Cargo 1.98.1.
- Host: Linux 6.17.0-1022-azure, AMD EPYC 7763, 4 logical CPUs,
  16,766,414,848 bytes memory, ext4 durable-test filesystem.
- Topology: exactly three voters, node IDs 1/2/3, explicit loopback endpoints.
- Benchmark Raft timing: 100 ms heartbeat, 5-10 s election window; per-peer
  outstanding RPC capacity 64.
- Verified resource bounds: foreground capacity-one saturation tests; per-peer
  capacity-one saturation tests; snapshot receive default 64 MiB and a 32-byte
  deterministic saturation fixture.
- Semantics: RF=3, quorum-durable writes plus current-leader apply,
  quorum-backed linearizable reads; no lease/follower strong-read shortcut.
- Benchmark: seed 1597463007; 16-byte keys, 64-byte values, 128-key dataset,
  16 warmups and 100 measured operations per cell; concurrency 1 and 8.

### 14.3 Requirement evidence

- **RAFT/SM:** `applies_commands_in_committed_order`,
  `membership_entry_updates_metadata_only`,
  `replay_is_deterministic_and_duplicate_identity_is_not_reapplied`,
  `lower_log_index_fails_closed`, and
  `factory_is_the_exact_openraft_0925_network_factory`.
- **WRITE/READ:** `leader_client_write_replicates_set_delete_and_batch_to_all_voters`,
  `strong_get_barrier_is_leader_authoritative_and_observes_applied_write`,
  `compact_contract_routes_strong_operations_through_raft_authority`,
  `admitted_write_survives_transport_future_cancellation`,
  `concurrent_writes_and_strong_gets_admit_a_linearizable_history`, and
  `quorum_loss_cannot_acknowledge_a_write_or_serve_a_stale_strong_read`.
- **DURABILITY:** `vote_is_durable_before_save_returns`,
  `log_and_committed_progress_survive_reopen`,
  `truncate_and_purge_are_hole_free_across_reopen`,
  `injected_persist_failure_never_advances_vote_or_log_state`,
  `checksum_truncation_and_version_corruption_fail_closed`, and
  `durable_append_helper_persists_before_return`.
- **MEMBERSHIP/NETWORK:** exact-three/idempotent/incompatible bootstrap tests,
  exact OpenRaft network-factory coverage, directional partition/drop/delay
  controls, and bounded slow/unreachable-peer saturation.
- **SNAPSHOT/RECOVERY:** coherent round trip, durable reopen plus subsequent log
  replay, interrupted temp-file safety, corrupt/truncated fail-closed tests,
  forced lagging-follower snapshot install plus later log replay, abandoned or
  corrupt receive preservation, bounded receive rejection, committed-prefix
  restart replay, and durable uncommitted-suffix exclusion.
- **FAILURE MATRIX:** healthy quorum failover, isolated/minority leader write
  rejection, quorum-loss read/write failure, delayed and unavailable follower
  model-checked progress, heal convergence, former-leader conflicting-suffix
  reconciliation, follower restart, and literal former-leader process
  restart/rejoin.
- **OPERABILITY/BOUNDS:** stable replica status plus client/admission, per-peer
  RPC, storage, apply, snapshot, election and leadership metrics; deterministic
  saturation/failure assertions cover every mandatory metric family.
- **PERFORMANCE:** PR #68's three retained RF=3 bundles contain all eight required
  workload/concurrency cells, p50/p95/p99, throughput, failures and exact
  environment/semantic metadata. Aggregate: 2,400 attempted, 2,400 successful,
  zero failures. Results are explicitly engineering-only and
  `authoritative_performance_result=false`.

### 14.4 Engineering benchmark summary

Ranges below span the three retained zero-failure runs.

| Workload | Concurrency | Throughput ops/s | p50 ms | p95 ms | p99 ms |
|---|---:|---:|---:|---:|---:|
| GET | 1 | 63,968-70,658 | 0.012-0.014 | 0.023-0.025 | 0.030-0.035 |
| GET | 8 | 163,976-215,358 | 0.029-0.037 | 0.052-0.061 | 0.064-0.069 |
| SET | 1 | 337-397 | 2.490-2.937 | 2.811-3.434 | 3.046-3.760 |
| SET | 8 | 723-821 | 9.680-10.532 | 10.769-12.300 | 10.779-12.413 |
| DELETE | 1 | 335-378 | 2.636-2.969 | 2.948-3.349 | 3.036-3.559 |
| DELETE | 8 | 714-744 | 10.610-11.087 | 12.365-13.609 | 12.399-13.615 |
| 80/20 | 1 | 1,548-1,750 | 0.034-0.037 | 2.651-3.002 | 2.716-3.273 |
| 80/20 | 8 | 2,554-2,974 | 1.087-1.886 | 8.639-8.863 | 9.198-11.281 |

These are regression/engineering observations on a shared CI runner, not a
release claim or external comparison.

### 14.5 SDD/CI ledger

| Slice | PRs | Authoritative Rust workflows |
|---|---|---|
| S0 | #39 | 33290290079 |
| T1 | #40 | 33303926982 |
| T2 | #41 | 33314215573 |
| T3 | #42-#44 | 33316855535, 33319667942, 33322313510 |
| T4 | #45-#49 | 33325197349, 33330871661, 33336467000, 33341876488, 33344510367 |
| T5 | #50-#55 | 33353844266, 33360312059, 34318460750, 34377197112, 34395319532, 34401474538 |
| T6 | #56-#68 | 34407129832, 34412365110, 34413150589, 34416938602, 34421512155, 34425591848, 34429361337, 34802584393, 34925299536, 34943441806, 34965003312, 34990282985, 35016295781 |

Artifact retention was added during T5. Key retained artifacts are #52
`10090962522`; #53 `10114398939`; #54 `10121305520`; #55
`10123749219`; #56 `10125804314`; #58 `10128051596`; #59
`10129470340`; #60 `10131092978`; #61 `10132563883`; #62
`10133895267`; #63 `10331858692`; #64 `10379641461`; #65
`10386650170`; #66 `10395037237`; #67 `10405481973`; and final
#68 `10415324683`. The final artifact supersedes earlier partial evidence by
retaining the complete suite and all preserved M0 gates on the exact final code
tree.

### 14.6 Residual risks and deferred work

- M3 proves one shard and exactly three voters. M4 owns 1,024-group placement,
  scheduling, membership changes, movement/rebalance and many-group cost.
- M3 uses a correctness-first durable image. M5 owns segmented WAL, group
  commit, reclamation and production recovery-time/storage optimization.
- Long-running randomized histories, broader crash injection and exhaustive
  distributed fault campaigns belong to M6.
- TLS/authentication and production network deployment policy are outside M3.
- Lease reads, relaxed durability and public comparative performance claims
  remain forbidden unless a later accepted spec owns and verifies them.

No residual item weakens or leaves unresolved a mandatory Spec 0005 requirement.
Issue #38 may close after the verification PR merges. The v1 tracker proceeds to
an accepted M4 child spec; Spec 0001 remains Accepted.
