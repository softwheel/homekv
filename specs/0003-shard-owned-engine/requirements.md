# Spec 0003 — Shard-Owned In-Memory Engine Requirements

- Status: Review
- Parent spec: `../0001-homekv-v1/`
- Depends on: verified Spec 0002 / M0 baseline
- Tracking issue: #9

## 1. Purpose

M1 replaces the prototype `Mvcc<BTreeStore>` whole-store copy-on-write path with a local shard-owned in-memory execution engine while preserving deterministic GET/PUT/DELETE and single-shard atomic-batch semantics.

M1 is deliberately local-only. It creates the state-machine and ownership boundary that later OpenRaft integration will drive, but it MUST NOT add distributed consensus, WAL durability, or network protocol redesign.

The verified M0 comparison point is commit `bc613b74e8c718a7d002f1cacbd8d51cddbf3067` and `specs/0002-baseline-benchmark/m0-analysis.md`.

## 2. Required behavior

### Logical shards

**REQ-M1-SHARD-001** — The M1 engine MUST expose stable logical shard IDs in the v1 space `0..1024`.

**REQ-M1-SHARD-002** — Key-to-shard mapping MUST use `XXH3_64(raw_key_bytes) & 1023`, matching `REQ-SHARD-004/005`.

**REQ-M1-SHARD-003** — Each instantiated shard MUST have exactly one mutation owner at a time. Mutation ordering for that shard MUST be deterministic.

**REQ-M1-SHARD-004** — Independent shards MUST NOT share a foreground storage lock or mutation queue.

### Data semantics

**REQ-M1-DATA-001** — GET MUST distinguish present values from absent keys and return owned bytes without exposing internal mutable references.

**REQ-M1-DATA-002** — PUT/SET MUST atomically create or replace one key/value pair.

**REQ-M1-DATA-003** — DELETE MUST atomically remove a key and MUST succeed deterministically when the key is already absent.

**REQ-M1-DATA-004** — A batch whose commands all target one logical shard MUST be atomic with respect to other operations on that shard: observers MUST see either the pre-batch or post-batch state, never a partial batch.

**REQ-M1-DATA-005** — A batch spanning multiple logical shards MUST be rejected by the M1 shard-engine API rather than implying cross-shard atomicity.

**REQ-M1-DATA-006** — The state-machine-facing mutation representation MUST be deterministic and suitable for later replicated apply: no operation may depend on wall-clock time, thread identity, randomized iteration order, or process-local pointer identity for semantic results.

### Mutation cost

**REQ-M1-PERF-001** — A single PUT or DELETE MUST NOT clone or copy the entire shard dataset.

**REQ-M1-PERF-002** — Primary 16-byte-key/64-byte-value storage PUT p50 at 50k keys SHOULD improve by at least 10x relative to the verified M0 median (about 7.2 ms), and PUT throughput SHOULD improve by at least 10x relative to the M0 median (about 113 ops/s), on comparable GitHub-hosted engineering runs. These are M1 engineering gates, not public release claims.

**REQ-M1-PERF-003** — Mutation latency scaling from 1k to 50k keys MUST no longer exhibit the M0 whole-dataset cliff. The 50k/1k PUT p50 ratio MUST be <= 4.0 on the M1 engineering comparison unless a spec amendment documents a different measured bottleneck.

**REQ-M1-PERF-004** — GET p99 SHOULD remain below 1 ms in the non-saturated local storage benchmark for the primary profile on the engineering host.

**REQ-M1-PERF-005** — Candidate comparisons MUST preserve identical key/value sizes, workload generation, dataset cardinalities, and semantic behavior.

### Queueing and backpressure

**REQ-M1-QUEUE-001** — Any cross-task/thread foreground mutation dispatch MUST use a bounded queue with an explicit configured capacity.

**REQ-M1-QUEUE-002** — Queue saturation MUST fail or apply backpressure explicitly; it MUST NOT allocate an unbounded backlog.

**REQ-M1-QUEUE-003** — Cancellation of a caller waiting for a mutation response MUST NOT corrupt shard state or reorder already accepted mutations.

**REQ-M1-QUEUE-004** — Queue depth/capacity and rejected-or-backpressured operation counts MUST be observable by the engine API or metrics snapshot.

### Memory accounting

**REQ-M1-MEM-001** — Each shard MUST track logical resident key/value bytes across inserts, replacements, deletes, and atomic batches without double-counting replaced values.

**REQ-M1-MEM-002** — Engine metrics MUST expose at least key count and logical key/value bytes per shard.

**REQ-M1-MEM-003** — Process-RSS bytes/key MUST be measured with the Spec 0002 memory methodology for the M1 comparison. Allocator-exact overhead MAY remain approximate, but unknown overhead MUST NOT be fabricated.

### Ownership and lifecycle

**REQ-M1-LIFE-001** — Shard creation and shutdown MUST be explicit and testable.

**REQ-M1-LIFE-002** — After shutdown begins, new operations MUST fail predictably; already accepted mutations MUST either complete before shutdown returns or be reported as not applied.

**REQ-M1-LIFE-003** — M1 need not migrate live data between owners. Online ownership transfer/rebalancing belongs to later placement/reconfiguration milestones.

## 3. Candidate architectures

M1 MUST implement and evaluate Candidate A before Candidate B can be promoted.

### Candidate A — owner-only map

One shard owner serializes GET/PUT/DELETE/batch operations over a non-concurrent point map. This is the M1 correctness baseline and initial production architecture.

The initial map MAY use `std::collections::HashMap` or an equivalent safe point map. Hash-function tuning is a later optimization unless profiling shows it blocks the M1 gate.

### Candidate B — single writer + Crossbeam direct reads

Mutations remain single-writer. GET may bypass the owner queue using a concurrent `crossbeam_skiplist::SkipMap` plus a sequence/version validation protocol that proves atomic-batch visibility.

Candidate B is optional for M1 completion if Candidate A already satisfies M1 requirements. It MUST NOT replace Candidate A without passing the promotion rule below.

## 4. Crossbeam promotion rule

Candidate B may become the primary read architecture only when all of the following hold on repeated equivalent-semantics engineering runs:

1. correctness/model tests show no partial atomic-batch visibility and no stale-reference/reclamation failures;
2. for both 100/0 and 95/5 read/write mixes, Candidate B improves either throughput/core or p99 latency by **>= 20%** versus Candidate A at concurrency 8 or 32 on at least two primary dataset cardinalities;
3. it does not regress 80/20 or 50/50 throughput by more than **10%** at the same load points;
4. resident memory/key does not increase by more than **25%** unless a reviewed amendment accepts the trade-off;
5. sequence-read retries remain bounded/observable and no foreground unbounded queue is introduced.

If these gates are not met, Candidate A remains primary and the Crossbeam experiment is retained as evidence rather than forced into production.

`crossbeam::queue::ArrayQueue` remains an independent bounded-dispatch candidate and may be adopted even when `SkipMap` direct reads are not promoted.

## 5. Accepted M1 benchmark matrix

### M0-compatible comparison

Repeat the exact primary M0 storage dimensions:

- workloads: GET / SET / DELETE / 80% GET + 20% SET
- key/value: 16 B / 64 B
- cardinalities: 1k / 10k / 50k
- deterministic seed
- p50/p95/p99, throughput, sample count
- repeated runs with run-to-run spread retained

Use an operation count large enough for meaningful p99 after the COW bottleneck is removed; target at least 10,000 measured operations per local storage cell unless runtime constraints are documented.

### Candidate concurrency matrix

For Candidate A vs Candidate B experiments, additionally cover:

- read/write mixes: 100/0, 95/5, 80/20, 50/50
- concurrency/readers: 1 / 8 / 32 where applicable
- uniform and hot-key/skewed access
- single-key mutations and atomic batches
- queue saturation/backpressure cases

Report queue wait/depth/rejections and direct-read retries where applicable.

## 6. Non-goals

M1 MUST NOT add:

- Raft/OpenRaft replication
- WAL/snapshot durability
- distributed leader/placement changes
- a new wire protocol
- cross-shard atomic transactions
- custom unsafe lock-free hash tables
- public comparative performance claims

## 7. M1 acceptance gate

Spec 0003 becomes `Verified` only when:

1. stable 1,024-shard XXH3 mapping has golden/property tests;
2. Candidate A shard-owned engine passes deterministic GET/PUT/DELETE/batch tests;
3. concurrent histories prove no partial single-shard batch visibility;
4. queue capacity/backpressure/shutdown semantics are tested;
5. memory accounting tests pass for insert/replace/delete/batch paths;
6. profiling/code inspection proves no whole-shard clone on mutation;
7. repeated M1 benchmark evidence is captured and compared against verified M0;
8. the M1 performance gates in REQ-M1-PERF-002/003 pass or the spec is amended before verification;
9. any Candidate B promotion satisfies the explicit Crossbeam promotion rule;
10. no M2/M3 protocol/consensus/durability work is mixed into M1.
