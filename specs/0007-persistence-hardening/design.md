# Spec 0007 — Persistence/Snapshot Hardening Design

- Status: Accepted
- Requirements: `requirements.md`
- Tracking issue: #99

## 1. Architecture

M5 keeps the Verified M3/M4 component shape — one `RaftLogStorage` +
`RaftStateMachine` per group — and replaces the internals of the log
store's durability path. The state machine's apply semantics and the
snapshot image content are unchanged.

```text
per group storage directory
  meta                    atomic metadata image (vote, committed, purged, segment inventory)
  wal/
    seg_<first>_<seq>.wal append-only segments, bounded size
  snapshot                staged snapshot image (temp + atomic rename, as today)
  snapshot.tmp            staging file, never authoritative

append path
  caller -> group-commit batcher -> active WAL segment (write + batched fsync)
           -> metadata image advance (temp + sync + rename + dir sync)
           -> per-entry completion callback

recovery path
  open -> read+validate metadata -> replay segments in order from purged+1
       -> torn tail truncation | mid-state corruption fail-closed
       -> in-memory durable view

snapshot path
  build  -> consistent image -> staging temp + fsync + rename + dir sync
            -> current-snapshot pointer advance
  install -> staged decode + full validation -> persist staged -> swap live state
```

The batcher, segment manager, and metadata writer are all inside
`HomeKvRaftLogStore`; no new crate, no new trait surface, no wire change.

## 2. Accepted invariants

1. The M3 ack boundary is intact: the per-append callback fires only after
   the entry's bytes are flushed on the local replica; Raft quorum and
   leader-apply behavior above the store are untouched
   (`REQ-M5-BASE-001/002`, `REQ-M5-GC-004`).
2. The durable metadata image is the single authority for vote, committed
   position, purged prefix, and segment inventory. In-memory durable state
   advances only after the corresponding durable write completes
   (`REQ-M5-WAL-002`).
3. The WAL is append-only within a segment. Records are never modified in
   place; segments are only created, sealed by rotation, replayed, and —
   once durably purged — deleted (`REQ-M5-WAL-001/006`).
4. Recovery replays a strict index-ordered prefix of segments; a torn tail
   truncates, mid-state corruption fails closed, and nothing is ever
   skipped (`REQ-M5-WAL-004/005`, `REQ-M5-COR-002/003`).
5. Snapshot staging files are never authoritative: only the renamed live
   image counts, and the pointer advances only after durable persist
   (`REQ-M5-SNAP-001/004`).
6. Purge never crosses a non-durably-persisted snapshot and never crosses
   committed (`REQ-M5-SNAP-005`).
7. All resource use is bounded: segment size, batch bytes, linger time,
   snapshot receive bytes, segment inventory size
   (`REQ-M5-WAL-001`, `REQ-M5-GC-003`, `REQ-M5-SNAP-006`).

## 3. Segmented WAL layout

Directory `<group>/wal/` holds segment files named
`seg_<first_index>_<seq>.wal`, where `<first_index>` is the log index of
the first record in the segment and `<seq>` is a monotonically increasing
rotation counter. Both fields are zero-padded decimal so lexicographic
order matches logical order.

### Record framing

Each record is:

```text
MAGIC      8 bytes  "HKVWAL01"
LEN        4 bytes  LE u32, payload length
CRC        8 bytes  LE xxh3_64(payload)
PAYLOAD    LEN bytes, bincode(Entry<HomeKvRaftConfig>) — same encoding as today
```

The header is fixed 20 bytes. A reader validates magic, then length
sanity (nonzero, below a configured per-record cap), then CRC. Any
mismatch is corruption, handled per §7.

### Rotation and retention

- The active segment accepts appends until its size would exceed the
  configured segment bound (default to be fixed in M5-T1; candidate 64
  MiB), at which point it is sealed (a final flush) and a new segment is
  opened. The new segment is recorded in the metadata inventory in the
  same atomic metadata write that advances durable state, so inventory and
  data never disagree about a segment's existence
  (`REQ-M5-WAL-003`).
- `purge(log_id)` advances the metadata's `last_purged_log_id` with the
  existing identity validation, then deletes segment files whose entire
  index range is `<= last_purged_log_id`. Deletion is best-effort after the
  metadata write: a leftover segment file on the next open is simply
  ignored if fully covered by the purged prefix, or replayed if not
  (inventory is authoritative; the file set is reconciled, never trusted
  blindly).

### Append path detail

`append(entries, callback)`:

1. Validate the batch (non-empty, consecutive indexes, contiguous with the
   durable tail — the same checks as today).
2. Encode records into the active segment's file buffer.
3. Hand the batch to the group-commit batcher (§4), which performs one
   `fsync` for the coalesced bytes of all in-flight batches.
4. On fsync success: atomically advance the metadata image (new durable
   tail; inventory update if rotated), then advance the in-memory durable
   view, then fire each entry's callback with success.
5. On fsync or metadata-write failure: fire every callback in the failed
   batch with the error; the in-memory durable view does not advance; the
   partially written tail bytes remain in the file and are truncated on
   the next open per §7 (they were never acknowledged).

Note the ordering guarantee that preserves the M3 boundary: no callback
fires before the batch fsync completes, and no metadata advance precedes
the data flush.

## 4. Group commit

A per-store batcher coalesces appends that arrive while a flush is in
flight (and optionally up to a linger bound):

- `max_linger`: upper bound a batch waits for more appends before flushing
  (configurable, default sub-millisecond; exact value fixed in M5-T2 with
  measurement).
- `max_batch_bytes`: flush immediately when the buffered bytes exceed the
  bound.
- Fairness: appends are flushed in arrival order; a slow batch does not
  starve later batches beyond one linger window.

Failure semantics: the batch is the unit of durability. If the fsync or
the follow-on metadata write fails, the whole batch fails — every
callback in the batch receives the error and the durable view stays at
the last good batch (`REQ-M5-GC-002`). There is no partial-batch
visibility: either all of a batch's bytes are durable and callbacks fire,
or none are.

Metrics record per-batch entry count, batch bytes, and wait time so the
batching behavior is observable and the verification can assert coalescing
actually happened.

## 5. Metadata image

`<group>/meta`: the same envelope discipline as today's storage image:

```text
MAGIC 8 bytes "HKVMTA01" | VERSION u32 | LEN u64 | CRC xxh3_64 u64 | bincode(Metadata)
```

`Metadata = { vote, committed, last_purged_log_id, segments: Vec<SegmentRef> }`,
`SegmentRef = { seq, first_index, sealed: bool }`. The file is tiny
(hundreds of bytes), so full-image rewrite per advance keeps the proven
temp+sync+rename+dir-sync discipline (`REQ-M5-WAL-002`).

Validation at open mirrors today's `validate_state` plus inventory checks:
consecutive segment `first_index` ordering, no segment referenced twice,
every inventory segment file present, every segment file on disk either in
the inventory or fully covered by the purged prefix (otherwise fail
closed) (`REQ-M5-WAL-003`).

## 6. Snapshot hardening

Generation (`RaftSnapshotBuilder::build_snapshot`):

1. Capture a consistent image at the current `last_applied` under the
   state-machine write lock (unchanged).
2. Encode with the existing envelope (magic, length, xxh3) — unchanged.
3. Write to `snapshot.tmp`, `fsync`, atomic rename to `snapshot`, sync
   parent dir (this staging discipline already exists in
   `persist_snapshot`; M5 keeps it and adds step 4).
4. Cross-check before pointer advance: re-decode the staged bytes and
   verify envelope, version, shard id, and that image `last_applied` /
   membership match the captured meta. Only then advance
   `current_snapshot` (`REQ-M5-SNAP-001/002`).

Installation (`install_snapshot`):

1. Enforce the receive byte bound before buffering (already present via
   `snapshot_receive_limit_bytes`; M5 keeps it, `REQ-M5-SNAP-006`).
2. Decode and validate fully: envelope checksum, version, shard id,
   image-vs-`SnapshotMeta` agreement on `last_log_id` and `last_membership`,
   and monotonic `last_applied` (reject a snapshot older than current
   state — new in M5).
3. Stage to `snapshot.tmp`, fsync, rename, dir sync.
4. Swap live state (last_applied, membership, data, current_snapshot) and
   record the install in metrics.
5. Steps 2–3 complete before step 4 touches live state, so a failed or
   crashed install leaves the old state and old snapshot intact
   (`REQ-M5-SNAP-003/004`).

Truncation/purge coordination: `build_snapshot` records the last-included
log index in the snapshot meta path; the store's `purge` to that index is
issued only after step 4 of generation completes. The existing purge
identity rules (no purge past committed without snapshot cover, no
identity change, snapshot-install advance) are preserved
(`REQ-M5-SNAP-005`).

## 7. Corruption model and recovery

Recovery (`open`) performs, in order:

1. Read and validate the metadata image. Any envelope/CRC/version failure
   → open fails closed (`REQ-M5-COR-002`).
2. Reconcile inventory vs files on disk (§3).
3. Replay segments in index order from `last_purged_log_id + 1`:
   - For each record: validate framing and CRC, check index continuity.
   - **Torn tail**: if the last segment ends mid-record (short read at the
     very end, with all prior records valid), truncate the file to the
     last complete record boundary, record a `torn_tail_truncated` metric,
     and continue. This is the only truncation case
     (`REQ-M5-WAL-005`, `REQ-M5-COR-003`).
   - **Mid-state corruption**: any CRC/length/magic/index violation that
     is not a torn tail at the newest segment end → open fails closed.
     The implementation never skips a bad record and continues
     (`REQ-M5-COR-002/003`).
4. Enforce the no-newer-than-committed rule: `committed` must lie within
   the recovered durable range; applied state is reconstructed only up to
   committed (`REQ-M5-REC-003`).

Crash-during-append therefore yields exactly the last fully-flushed
batch's prefix — the same prefix whose callbacks fired — satisfying
`REQ-FAIL-005`: no acknowledged entry is lost, no unacknowledged entry is
silently resurrected, and no torn record is ever applied.

The existing `fail_next_persist` test hook is extended to cover the WAL
write path, the metadata advance, the group-commit fsync, and the
snapshot stage/rename steps, so crash injection is precise per phase.

## 8. Concurrency model

Unchanged in shape from M3: one mutex serializes the store's durable
mutations (metadata advance, rotation, purge/truncate). The group-commit
batcher adds a small condition-variable/queue handoff between appending
threads and the flush; appends remain ordered by arrival and the flush
thread performs the single fsync. No new cross-group shared mutable state:
every group keeps its own segments, metadata, batcher, and metrics.

## 9. Observability

Extend the existing `RaftStorageMetricsSnapshot` with:

- group-commit: batches, batched entries, batch bytes, batch wait
  histogram, coalesced appends;
- WAL: segments open/sealed, rotations, segment deletions, torn-tail
  truncations;
- truncation/purge counters;
- snapshot build/install attempts, failures, bytes, durations (already
  partially present in the state-machine metrics; M5 unifies the naming).

The offline utility (`REQ-M5-OPS-001/002`) is read-only: it parses the
metadata image and walks segments exactly as recovery does, but instead of
failing closed it reports — per segment: index range, record count, size,
checksum status; torn-tail vs mid-state corruption classification; and a
non-zero exit code on any non-truncatable corruption.

## 10. Alternatives considered

- **Keep full-image rewrite, add fsync batching only.** Rejected: write
  amplification grows with log size; recovery must deserialize the whole
  log; segment-bounded recovery time is unachievable.
- **mmap'd WAL with in-place headers.** Rejected: torn-write semantics are
  harder to reason about than append+truncate; no measurable need yet.
- **Per-record fsync without group commit (M3 behavior, new layout).**
  Kept as the M5-T1 intermediate step, not the end state: it isolates
  layout-correctness testing from batching-correctness testing.
- **LSM / new storage engine.** Out of scope per non-goals; the durable
  content model is unchanged.

## 11. Compatibility and deferred work

- On-disk format: M5 introduces the `wal/` + `meta` layout. A one-way
  forward migration is out of scope for v1 pre-release: M5 test clusters
  start fresh, and the old single-image layout is not read by the new
  code. This is recorded here (not silent) and revisited only if a
  release-migration requirement is Accepted.
- Record payload encoding (`bincode(Entry)`) is unchanged; the WAL magic
  `HKVWAL01` and metadata magic `HKVMTA01` carry format versions for
  future layout changes.
- Compression, encryption, tiered storage, and cross-group WAL sharing
  are deferred; none is required by `REQ-DUR-*` or `REQ-FAIL-005`.
