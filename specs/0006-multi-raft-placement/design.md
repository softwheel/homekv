# Spec 0006 — Multi-Raft Placement and Rebalancing Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #70

## 1. Architecture

M4 composes the Verified M3 group implementation rather than replacing it.

```text
client
  -> compact ingress
  -> key/shard validation
  -> committed placement resolver
  -> bounded group registry
  -> data OpenRaft group (0..1023)
       -> shared peer transport
       -> HomeKV durable log/snapshot/state machine

placement controller
  -> placement catalog OpenRaft group
  -> idempotent movement reconciler
  -> bounded learner/snapshot/membership operations
```

There are exactly 1,024 data groups and one system catalog group. The catalog is not a source of data-group leadership: it records stable placement and movement intent; each data group's committed OpenRaft membership and leader state remain the immediate consensus authority.

## 2. Accepted invariants

1. Key mapping is always `XXH3_64(raw_key) & 1023`.
2. Stable data placement is RF=3 with distinct voter nodes.
3. The catalog is quorum-durable and linearizable.
4. Data groups retain the Verified M3 durable-write and safe-read boundaries.
5. Cached routes, gossip and desired leaders never confer authority.
6. No dedicated OS thread or TCP connection exists per data group.
7. All admission, transport, snapshot and movement resource use is bounded.
8. A movement has one durable operation identity and one active reconciler outcome.
9. A placement epoch is published only after the corresponding data-group membership transition is committed and old-replica removal is reconciled.
10. Cross-shard atomicity remains out of scope.

## 3. Placement catalog

The catalog state machine stores:

- cluster identity and immutable shard-map descriptor;
- global `placement_epoch: u64`;
- eligible nodes and failure-domain labels;
- one record per shard:
  - `group_id` derived injectively from shard ID;
  - stable voter set;
  - desired leader hint;
  - stable record epoch;
  - optional movement record;
- movement operation records and bounded plan status.

The catalog uses a dedicated RF=3 OpenRaft group and the M3 persistence/snapshot contract. A catalog command is deterministic and idempotent by operation ID. Epoch increment and stable-record publication occur in one committed catalog command.

The catalog's three voters are explicitly bootstrapped from validated controller node IDs. Controller placement is not automatically changed by data rebalancing in M4.

## 4. Deterministic bootstrap and balancing

Bootstrap sorts eligible nodes and failure domains, validates at least three distinct nodes, then assigns each shard's three voters with a deterministic round-robin/permutation seeded by cluster identity and shard ID. The algorithm minimizes voter-count skew, prevents duplicate voters, and spreads replicas across failure domains when possible.

The equal-weight rebalancer computes desired voter and leader counts using floor/ceiling division, compares the desired state with committed stable placement, and emits a deterministic minimal plan. Heterogeneous capacity weighting is deferred.

A planner never mutates authority. It submits catalog intents; the reconciler performs one bounded movement at a time per shard.

## 5. Group registry and runtime

A node owns a `GroupRegistry` keyed by system/data group ID. Registry admission reserves:

- one group slot;
- an accounted memory budget;
- bounded event and foreground queues;
- handles into shared runtime and transport services.

Groups are recovered before publication into the serviceable registry. Failed recovery keeps the group unavailable and observable.

The process uses a configured Tokio worker pool. OpenRaft group tasks/timers may exist per group, but M4 does not create an OS thread per group; counts and memory are measured by the scaling gate. Group event polling/admission is fairness-bounded so a hot group cannot indefinitely starve others.

## 6. Shared transport

One node-pair transport manager multiplexes envelopes carrying `group_id`, RPC kind, request identity and bounded payload over a small configured connection pool. It reuses the Verified M3 RPC payloads and per-peer capacity accounting.

Bounds exist for total and per-peer in-flight RPCs/bytes, response buffers, and snapshot traffic. A full peer/group budget rejects or backpressures explicitly. Cancellation releases permits; admitted consensus work retains the M3 cancellation semantics.

`openraft-multi` is not selected by this spec. Introducing it requires an Accepted amendment with an exact pin and the same gates.

## 7. Routing

The M2 request format already carries `shard_id`; routing error bodies can carry `route_version` and endpoint hint. M4 therefore requires no wire-version change.

For each request, ingress:

1. recomputes shard ID from every key;
2. rejects wrong-shard or cross-shard batch input;
3. reads a committed catalog view;
4. rejects with `STALE_ROUTE_OR_NOT_OWNER` if the node is not a stable member or cannot address the group;
5. submits to the local group;
6. lets the group's current OpenRaft authority decide leader/read-barrier success;
7. returns route epoch and best committed hint on safe redirect.

The catalog's desired leader is advisory. A node may return a hint only; it cannot serve a strong operation without current group authority.

## 8. Movement state machine

A movement record contains `operation_id`, shard, source membership, target membership, source catalog epoch, phase, retry/error metadata and observed committed membership.

Phases are:

1. **Intent** — commit pending target in the catalog; stable placement is unchanged.
2. **Learner** — create/recover the target replica and add it via OpenRaft learner API.
3. **CatchUp** — transfer valid snapshot/log under existing byte/concurrency bounds and prove required progress.
4. **Promote** — use OpenRaft safe membership change to commit target voter membership.
5. **Lead** — optionally request leadership transfer; never assume it succeeded from intent alone.
6. **Remove** — commit safe removal of the source voter and wait for authoritative membership observation.
7. **Publish** — atomically replace stable catalog placement, increment epoch and clear pending intent.
8. **Cleanup** — tombstone old local replica and reclaim only after retention/safety checks.

The reconciler is level-triggered. On restart it reads committed catalog intent and the data group's committed membership, then repeats the next idempotent action. If membership is ahead of catalog intent, it completes publication; it does not roll consensus membership back by guess. Irreconcilable identity/configuration fails closed for operator review.

## 9. Failure handling

- Catalog quorum loss freezes new plans/mutations; data groups continue under last committed memberships.
- Data-group quorum loss affects only that shard's strong operations.
- Target failure before promotion leaves stable placement unchanged and permits safe cancellation.
- Failure after membership commit requires forward reconciliation.
- Node restart recovers hosted groups before route admission.
- Corruption isolates the affected group/catalog authority and surfaces an error.
- Gossip may propose action but catalog consensus and data-group consensus perform it.

## 10. Observability

The management contract has two layers:

- bounded aggregate snapshot: catalog health/epoch, placement/leader skew, group counts/status, runtime/connection/queue/memory totals, movement counts and saturation;
- paginated/filterable per-shard records: placement, role/leader/term/config, commit/apply/snapshot progress, health and active movement.

Metrics default to aggregate/bucketed labels; an explicit bounded allow-list enables detailed shard labels. Existing M3 metric types remain stable inputs.

## 11. Verification and scaling strategy

Correctness tests use small deterministic clusters/bounds and explicit hooks. They compare catalog, OpenRaft membership and visible data against reference models during concurrent traffic and crash/retry at every movement phase.

The scaling harness measures 1/64/256/1,024 groups, idle/uniform/skew, and one delayed/unavailable/moving group. It records resource growth, connection sharing, scheduling contention, latency and throughput with exact semantics/environment. Three complete 1,024-group runs are retained.

No numeric latency claim is accepted in M4. Verification requires semantic correctness, zero unexpected failures, enforced configured ceilings, fixed-topology connection sharing, complete resource reporting, and no unexplained correctness or liveness collapse. Evidence that OpenRaft cannot pass triggers the parent-defined adapter-amendment path; it never authorizes a semantic downgrade.

## 12. Compatibility and deferred work

M4 does not change the M2 frame version/status numbers or M1 shard mapping. It does not optimize M3's durable image. Segmented WAL/group commit belongs to M5; long randomized campaigns to M6; performance optimization to M7; release comparisons to M9.
