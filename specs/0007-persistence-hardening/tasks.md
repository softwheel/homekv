# Spec 0007 — Persistence/Snapshot Hardening Tasks

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #99
- Dependency: Spec 0005 Verified, Spec 0006 Verified

Every semantic slice requires its own focused PR, requirement/task
traceability, complete required CI, and review of exact tested
head/base/merge identity. A later task may begin only after its dependency
is merged. No task may weaken a Verified M0–M4 assertion. A material
semantic change after Accepted requires a spec amendment in the PR before
merge.

## M5-S0 — Accept child spec

- Author/review `requirements.md`, `design.md`, `tasks.md`, and
  `verification.md` together.
- Confirm the parent T-0006 boundary and the Spec 0005/0006 residual
  handoff (M4-BASE-003 behavior contract).
- Apply the strict acceptance checklist (spec PR self-review) and record
  Accepted state with tracker traceability.
- No semantic implementation.

Completion: all four documents are internally consistent, testable,
Accepted, and merged with green repository gates.

## M5-T1 — Segmented WAL write path

Depends on: S0 (spec Accepted)

Requirements: `REQ-M5-BASE-*`, `REQ-M5-WAL-*`, `REQ-M5-COR-001/002` (framing
detection), `REQ-M5-OPS-003` (WAL/segment counters)

Deliver:

- record framing (`HKVWAL01`, length, `xxh3_64` per record) and
  segment files `seg_<first>_<seq>.wal` with bounded configured size;
- small atomic metadata image (`HKVMTA01`: vote, committed,
  last-purged, segment inventory) with temp+sync+rename discipline;
- append path with per-append fsync (M3 durability semantics preserved;
  group commit arrives in T2), rotation without loss/duplication,
  inventory-authoritative reconciliation at open;
- recovery: index-ordered replay from purged+1, torn-tail truncation to
  last complete record, mid-state corruption fails closed, existing
  hole/committed-range validation preserved;
- purge deletes only fully-covered sealed segments after the metadata
  advance; truncation rules unchanged;
- unit tests: framing round-trip, rotation boundary, inventory
  reconciliation, torn-tail truncation, mid-segment corruption fail-closed,
  purge-deletion safety, reopen equivalence with the old image layout's
  observable behavior;
- keep the existing `RaftLogStorage` trait surface and all M0–M4
  assertions green.

No group commit batching, no snapshot-path changes, no tooling binary in
T1.

Completion: full `cargo test` green; clippy clean on touched code;
`rustfmt` on touched leaf files; new tests ≥ 15 covering framing,
rotation, recovery, purge.

## M5-T2 — Group commit

Depends on: T1

Requirements: `REQ-M5-GC-*`, `REQ-M5-BASE-001/002`, `REQ-M5-OPS-003`
(batch metrics)

Deliver:

- per-store group-commit batcher: coalesced single fsync for concurrent
  appends, configurable `max_linger` and `max_batch_bytes`, arrival-order
  fairness, no unbounded buffering;
- callback ordering proof: no per-append callback fires before the batch
  fsync succeeds; batch failure fails every callback in the batch with no
  partial durable visibility and no in-memory advance;
- ack-boundary preservation test: success still implies local durable
  flush before callback (the M3 measurable ack point);
- batch metrics (batches, batched entries, bytes, wait histogram) and an
  assertion that concurrent appends actually coalesce;
- linger default chosen with a measured justification recorded in the PR.

No snapshot-path changes in T2.

Completion: full `cargo test` green; clippy/fmt as above; new tests ≥ 8
covering coalescing, ordering, failure atomicity, and bound enforcement.

## M5-T3 — Snapshot generation/install hardening and truncation coordination

Depends on: T1 (T2 independent; merge order T2 before T3 preferred)

Requirements: `REQ-M5-SNAP-*`, `REQ-M5-BASE-003`, `REQ-M5-FAIL-005`
(via parent)

Deliver:

- generation: staging temp + fsync + atomic rename + dir sync (keep),
  plus re-decode cross-check (envelope, version, shard, image-vs-meta
  `last_applied`/membership) before the current-snapshot pointer
  advances;
- install: full staged validation (envelope checksum, version, shard,
  image-vs-`SnapshotMeta` agreement, monotonic `last_applied`) before live
  state is touched; failed install leaves live state and prior snapshot
  untouched;
- crash-injection tests: crash during generation leaves the previous
  complete snapshot authoritative; crash during install leaves old-complete
  or new-complete state, never a torn mix (inject at stage, rename, and
  swap points);
- purge-after-durable-snapshot coordination: purge to a snapshot's
  last-included index only after the snapshot pointer advanced; existing
  purge identity rules preserved; truncation-never-crosses-committed
  property tests;
- receive byte-bound enforcement tests.

Completion: full `cargo test` green; clippy/fmt as above; new tests ≥ 10
covering staging, cross-checks, monotonicity, crash points, and
purge coordination.

## M5-T4 — Corruption detection and fail-closed recovery

Depends on: T1–T3

Requirements: `REQ-M5-COR-*`, `REQ-M5-BASE-003`, `REQ-DUR-004`
(via parent)

Deliver:

- bit-flip injection harness covering: flipped bit in WAL record body,
  in WAL record frame (length/checksum), in metadata image bytes, in
  snapshot image at rest, and in snapshot bytes during install;
- each case asserts fail-closed: open/install returns an error, the group
  does not serve, and no partial or skipped state is exposed;
- torn-tail vs mid-state discrimination tests: only a torn tail at the
  newest segment end truncates; a corrupt middle record never skips;
- extend the `fail_next_persist`-style injection hooks to the WAL write,
  metadata advance, group-commit fsync, and snapshot stage/rename steps
  so each phase in the crash matrix is precisely injectable;
- multi-group spot check: corruption in one group's WAL fails only that
  group closed; a sibling group still opens and serves.

Completion: full `cargo test` green; clippy/fmt as above; new tests ≥ 12
covering every injection point in `REQ-M5-COR-004` plus discrimination and
isolation.

## M5-T5 — Recovery-time bounds and operational tooling

Depends on: T1–T4

Requirements: `REQ-M5-REC-*`, `REQ-M5-OPS-001/002/003`

Deliver:

- recovery-time measurement: automated test that builds a documented-size
  log (e.g. 100k small entries across segments with rotation), restarts,
  asserts recovery completes within a stated budget on the test identity,
  and records the measured time; the budget is a regression tripwire,
  reported honestly per the §9 honesty rule;
- no-newer-than-committed recovery equivalence test: recovered applied
  state matches the pre-crash committed prefix exactly;
- offline read-only WAL utility (`hkvwal` binary or `hkvctl wal`
  subcommand — decided here): `inspect` lists segments with index
  ranges, record counts, sizes, checksum status, and the metadata
  vote/committed/purged view; `verify` cross-checks inventory vs files,
  distinguishes torn-tail candidates from mid-state corruption, and exits
  non-zero on non-truncatable corruption;
- utility tests: verify passes on a healthy directory, fails on
  bit-flipped segment, reports a torn tail distinctly; inspect output is
  stable and parseable;
- docs: utility usage in the PR description and a short section in the
  verification evidence.

Completion: full `cargo test` green; clippy/fmt as above; utility builds
without warnings; new tests ≥ 8.

## M5-T6 — Verification handoff

Depends on: every prior task

Reconcile every requirement, task, test, and residual risk. Run the
complete Rust suite, the M3 RF=3 replicated gates, and the M4 many-group
gates on one exact candidate identity. Write `docs/m5-verification-evidence.md`
with the exact identity, PR/workflow ledger, test counts, recovery-time
measurements with digests, and an honest requirement-by-requirement
verdict.

Promote Spec 0007 to Verified only when every mandatory verification.md
row is PASS on that identity. Otherwise keep it Accepted, record the
precise blocker and next action, and do not start M6.
