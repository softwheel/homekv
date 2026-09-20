# Spec 0007 — Persistence/Snapshot Hardening Verification

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tasks: `tasks.md`
- Tracking issue: #99

## 1. Verification rule

M5 becomes Verified only when every mandatory row below is PASS on one
exact implementation identity. Tests must inspect durable bytes on disk,
recovered in-memory state, callback ordering, and persisted recovery
state; timing-only success is insufficient. Crash-injection tests must
fail the operation at the injected point and then prove the recovered
state, not merely assert an error was returned.

All evidence inherits the M3 ack boundary: quorum-flushed Raft
persistence plus leader apply for durable writes. M0–M4 gates remain
mandatory and unchanged.

## 2. Required environment record

Retained evidence records:

- exact HomeKV commit, branch head/base and tested merge checkout;
- exact Rust toolchain, OpenRaft version, and Cargo.lock identity;
- OS/kernel, CPU, memory, filesystem type and mount options;
- group storage layout parameters: segment size bound, group-commit
  `max_linger` / `max_batch_bytes`, snapshot receive byte bound;
- key/value sizes, entry counts, and segment counts used in recovery
  tests;
- raw artifact identity/digest and all test commands.

## 3. Requirement-to-evidence matrix

| Requirement | Required evidence | State |
|---|---|---|
| `REQ-M5-BASE-001..005` | ack-boundary tests + complete unchanged M0–M4 regression gates | pending |
| `REQ-M5-WAL-001..006` | framing/rotation/inventory/replay/torn-tail/purge tests (§4) | pending |
| `REQ-M5-GC-001..004` | coalescing/ordering/failure-atomicity/bound tests (§5) | pending |
| `REQ-M5-SNAP-001..006` | staging/cross-check/monotonicity/crash/purge-coordination tests (§6) | pending |
| `REQ-M5-COR-001..004` | bit-flip injection matrix, fail-closed + discrimination tests (§7) | pending |
| `REQ-M5-REC-001..003` | bounded-replay, recovery-time budget, no-newer-than-committed tests (§8) | pending |
| `REQ-M5-OPS-001..003` | utility inspect/verify tests + metrics coverage assertions (§9) | pending |

## 4. Segmented WAL verification

Mandatory tests:

1. record framing round-trip: encode/decode preserves entry bytes;
   corrupt magic, length, or CRC each fail decode;
2. appends across a rotation boundary are lossless and ordered; the new
   segment appears in the durable metadata inventory atomically with the
   data;
3. reopen replays segments in index order from `purged+1`; hole,
   key/index-mismatch, and purged-overlap validations still reject;
4. crash injection during WAL append (fail the write before fsync, and
   separately after write but before fsync): recovered state equals the
   last fully-flushed batch prefix; no callback fired for the failed
   batch; reopen succeeds;
5. torn tail: truncate the last segment mid-record on disk; open
   truncates to the last complete record, reports the truncation, and
   serves the prefix;
6. inventory reconciliation: a segment file absent from the inventory is
   not replayed; an inventory entry without a file fails closed;
7. purge deletes only fully-covered sealed segments and only after the
   metadata advance; deletion is idempotent; reopen after partial
   deletion reconciles;
8. vote and committed-position durability across reopen unchanged from
   M3 semantics.

## 5. Group-commit verification

Mandatory tests:

1. N concurrent appends coalesce into fewer than N fsyncs (assert via
   batch metrics), and every callback fires exactly once with success;
2. no callback fires before its batch's fsync completes: instrument the
   fsync barrier and assert callback-after-flush ordering under
   concurrency;
3. injected batch failure: every callback in the batch observes the
   error; the durable view and reopened state show no partial batch;
4. `max_linger` bound: an isolated append is flushed no later than the
   linger bound after arrival;
5. `max_batch_bytes` bound: a large burst flushes in multiple batches
   without unbounded buffering;
6. ack-boundary preservation: a successful append's bytes are on stable
   storage before its callback fires (reopen-before-callback would see
   them — asserted via barrier test, the M3 measurable ack point).

## 6. Snapshot verification

Mandatory tests:

1. generation staging: kill the process between staging write and rename
   (inject at each point); the previous complete snapshot remains the
   authority and the new image is either complete or absent;
2. generation cross-check: corrupt the staged bytes before pointer
   advance; the pointer does not advance and the old snapshot stays live;
3. install validation: bit-flipped snapshot bytes, wrong version, shard
   mismatch, and `SnapshotMeta`/image disagreement each fail install with
   live state and prior snapshot untouched;
4. install monotonicity: installing a snapshot older than current
   `last_applied` is rejected;
5. crash during install (inject at stage, rename, and swap points):
   recovery yields the old complete state or the new complete state,
   never a torn mix; applied data matches exactly one of the two;
6. purge coordination: purge to a snapshot's last-included index succeeds
   only after the snapshot pointer advanced; purge past committed without
   snapshot cover is rejected; truncation never crosses committed;
7. receive byte bound: an oversized declared snapshot is rejected
   without unbounded allocation.

## 7. Corruption verification

Mandatory bit-flip injection cases (`REQ-M5-COR-004`), each asserting
fail-closed (open/install error; group does not serve; no skipped or
partial state):

1. flipped bit in a WAL record body (middle segment);
2. flipped bit in a WAL record frame (length field, then checksum field);
3. flipped bit in the metadata image;
4. flipped bit in the snapshot image at rest (open fails);
5. flipped bit in snapshot bytes during install (install fails, live
   state untouched);
6. discrimination: a torn tail truncates (§4.5) while a corrupt middle
   record fails closed — both behaviors asserted in one matrix;
7. isolation: corruption in one group's WAL fails only that group
   closed; a sibling group opens and serves normally.

## 8. Recovery-time verification

Mandatory tests:

1. build a documented log (e.g. 100,000 small entries, multiple rotated
   segments), restart, and assert recovery completes within the stated
   budget on the exact identity; record the measured wall time;
2. recovery replays only post-purge segments: with a purged prefix and
   many sealed segments, assert the number of records replayed equals the
   post-purge count (no unbounded scan);
3. no-newer-than-committed: after crash injection at several append
   points, recovered applied state equals the pre-crash committed prefix
   exactly (byte-identical data map and applied index).

## 9. Tooling and metrics verification

Mandatory tests:

1. `inspect` on a healthy multi-segment directory lists every segment
   with correct index ranges, record counts, sizes, and checksum-ok
   status, plus the metadata vote/committed/purged view;
2. `verify` exits zero on the healthy directory;
3. `verify` exits non-zero on a bit-flipped segment and classifies it as
   mid-state corruption (not a torn tail);
4. `verify` on a torn-tail directory reports the truncatable tail
   distinctly from corruption;
5. the utility never modifies the directory (assert mtimes/checksums
   unchanged);
6. metrics assertions: append/persist/recovery counters and latency
   histograms, group-commit batch stats, segment rotation/deletion
   counters, truncation/purge counters, and snapshot build/install stats
   are all observable and bounded-cardinality per group.

## 10. Honesty rule for measurements

Recovery-time, batching, and any latency/throughput numbers are recorded
with the exact identity from §2 and a content digest of the raw output.
They are regression tripwires and characterizations on that identity —
not production guarantees and not comparative claims. Any number quoted
outside this spec's evidence ledger MUST carry its identity or be
withheld.

## 11. Known limitations and residual risk

- The M5 on-disk layout (`wal/` + `meta`) is one-way: the old
  single-image layout is not read by the new code. Pre-release test
  clusters start fresh; a release-migration requirement needs a separate
  Accepted spec.
- Bit-flip injection covers representative corruption, not all possible
  media failure modes (e.g. silent whole-device loss is outside
  `REQ-DUR-*` assumptions).
- Crash injection is at software phase boundaries (write/fsync/rename/
  swap); true power-loss at arbitrary instruction points is approximated,
  not exhaustively covered.
- Recovery-time budgets are tripwires on the documented identity; CI
  hardware variance is addressed by re-baselining the budget on the
  recorded identity, never by weakening the assertion silently.
