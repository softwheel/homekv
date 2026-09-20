# Spec 0007 — Persistence/Snapshot Hardening Requirements

- Status: Accepted
- Parent: `specs/0001-homekv-v1/requirements.md`
- Depends on: Verified Spec 0005, Verified Spec 0006
- Planned child of: `specs/0001-homekv-v1/tasks.md` T-0006
- Tracking issue: #99

## 1. Purpose

M5 hardens the persistence layer that Verified M3 proved correct for one
replicated group and Verified M4 composed across 1,024 groups. The M3
implementation is deliberately correctness-first: every mutation rewrites a
complete versioned/checksummed storage image (temp file, `fsync`, atomic
rename, parent-directory sync). That design is correct but does not bound
write amplification, does not batch concurrent appends, and its crash and
corruption coverage is limited to whole-image faults.

M5 replaces the per-mutation full-image rewrite with a **segmented,
per-record-checksummed write-ahead log** for Raft entries plus a **small
atomic metadata image** for vote/committed/purged state; adds **group
commit** for concurrent appends; hardens **snapshot generation and
installation** against crashes and corruption; tightens **log truncation and
purge** coordination with snapshots; proves **corruption detection and
fail-closed** behavior under bit-flip injection; bounds and measures
**recovery time**; and ships **operational tooling** to inspect and verify
WAL state offline.

M5 MUST preserve every Verified M0–M4 contract, in particular the M3
durable-write acknowledgement boundary (quorum-flushed Raft persistence plus
current-leader local apply, `REQ-DUR-005`) and the M4 composition behavior
(`REQ-M4-BASE-003`: each data group retains the Verified M3
log/state-machine/snapshot/recovery/corruption/fail-closed behavior).

## 2. Inherited contract and scope

M5 covers exactly the per-group Raft persistence of every HomeKV group (the
1,024 data groups and the placement-catalog group), which is the same
`RaftLogStorage` / `RaftStateMachine` surface M3 verified:

- vote, committed-position, log append/truncate/purge durability;
- snapshot build, persist, install, receive;
- recovery ordering, hole-freedom, and no-newer-than-committed reconstruction;
- corruption detection that fails closed instead of silently applying.

M5 does not change the Raft protocol, the OpenRaft trait model, routing,
placement, membership, the compact data plane, or any wire format.

**REQ-M5-BASE-001** — M5 MUST preserve the exact Verified M3 write
acknowledgement boundary: a durable write is acknowledged only after the
entry is committed by a quorum whose Raft persistence boundary has been
durably flushed, and after the leader has applied the committed entry
locally (`REQ-DUR-001`, `REQ-DUR-002`, `REQ-DUR-005`).

**REQ-M5-BASE-002** — The acknowledgement point MUST remain explicitly
documented and measurable: the per-append completion callback MUST fire only
after the entry's bytes are durably flushed on the local replica
(`REQ-DUR-002`).

**REQ-M5-BASE-003** — Recovery MUST reconstruct a state no newer than what
is justified by committed replicated log/snapshot state
(`REQ-DUR-003`, `REQ-FAIL-005`).

**REQ-M5-BASE-004** — All Verified M0–M4 regression gates MUST remain green
and unweakened. No M5 task may weaken or delete an existing assertion.

**REQ-M5-BASE-005** — M5 MUST NOT introduce a relaxed memory-only durable
mode, named or otherwise (`REQ-DUR-006`). If one is ever introduced it
requires a separate Accepted spec.

## 3. Segmented write-ahead log

The current full-image rewrite gives way to an append-only segmented WAL
for Raft log entries. Small consensus metadata (vote, committed position,
purged prefix, segment inventory) remains in a small atomic image so it can
still be replaced with the proven temp+sync+rename discipline.

**REQ-M5-WAL-001** — Raft log entries MUST be persisted in an append-only
segmented WAL. Each segment has a bounded configured size; record framing
carries a magic, a length, and a per-record checksum (`xxh3_64` or
equivalent, documented in design).

**REQ-M5-WAL-002** — Vote, committed position, last-purged log id, and the
segment inventory MUST live in a small atomic metadata image written with
the existing discipline (versioned envelope, whole-image checksum, temp
file, file sync, atomic rename, parent-directory sync). Corruption of the
metadata image MUST fail closed at open.

**REQ-M5-WAL-003** — Segment rotation MUST NOT lose or duplicate entries.
The segment inventory recorded in durable metadata is the authority for
which segments exist; segments absent from the inventory MUST NOT be
replayed, and inventory entries without a matching segment file MUST fail
closed at open.

**REQ-M5-WAL-004** — Recovery MUST replay WAL records in log-index order
starting after the purged prefix, enforcing the existing ordering rules:
indexes are consecutive, each record's index matches its key, and no record
overlaps the purged prefix (`REQ-DUR-003`).

**REQ-M5-WAL-005** — A torn write at the tail of the newest segment (the
observable effect of a crash during WAL append) MUST be truncated to the
last complete record. Recovered state MUST be a prefix of the attempted
appends: it MUST NOT expose entries that were never durably completed, and
MUST NOT skip a partial record and continue (`REQ-FAIL-005`,
`REQ-DUR-003`).

**REQ-M5-WAL-006** — A segment file MAY be deleted only after the metadata
image durably records a purged prefix that fully covers the segment's index
range. Deletion MUST be idempotent and MUST NOT remove a segment the
inventory still references.

## 4. Group commit

**REQ-M5-GC-001** — Concurrent appends arriving within a bounded window MUST
be coalesced into a single `fsync` batch. The per-append completion callback
for every entry in the batch MUST fire only after the batch's `fsync`
succeeds (`REQ-DUR-002`).

**REQ-M5-GC-002** — If a batch fails, every entry in the batch MUST observe
the failure through its callback, and in-memory durable state MUST NOT
advance past the last successfully flushed batch (fail closed; no partial
batch visibility) (`REQ-FAIL-005`).

**REQ-M5-GC-003** — Maximum linger time and maximum batch bytes MUST be
configurable with documented defaults. The batcher MUST NOT hold an append
past the linger bound, and MUST NOT buffer unboundedly: exceeding the byte
bound flushes immediately.

**REQ-M5-GC-004** — Group commit MUST NOT change the acknowledgement
boundary: success still means the entry is durably flushed on this replica
before the callback fires. Quorum/commit/apply semantics above the storage
layer are unchanged (`REQ-DUR-005`).

## 5. Snapshot generation, installation, and truncation

**REQ-M5-SNAP-001** — Snapshot generation MUST write the snapshot image to
a staging temporary file, `fsync` it, atomically rename it over the live
snapshot, and sync the parent directory. The current-snapshot pointer MUST
advance only after the staged image is durably persisted.

**REQ-M5-SNAP-002** — A generated snapshot MUST be cross-checked before the
pointer advances: envelope magic/length/checksum, format version, shard
identity, and image-vs-metadata agreement on `last_applied` and membership.

**REQ-M5-SNAP-003** — Snapshot installation MUST be staged: the received
bytes are fully decoded, checksum-verified, version-checked, and
cross-checked against the supplied `SnapshotMeta` (`last_log_id`,
`last_membership`) and for monotonic `last_applied` BEFORE live state is
touched. A failed install MUST leave live state and the prior durable
snapshot untouched.

**REQ-M5-SNAP-004** — A crash during snapshot generation MUST leave the
previous complete snapshot as the authority (the staging temp file MUST NOT
be mistaken for a snapshot on recovery). A crash during installation MUST
leave either the old complete state or the new complete state — never a
torn mix of the two (`REQ-FAIL-005`).

**REQ-M5-SNAP-005** — Log truncation/purge coordination: purge of the log
prefix up to a snapshot's last-included index MUST occur only after that
snapshot is durably persisted. Truncation MUST never cross the committed
position; purge target identity MUST be validated against the durable log
or the validated snapshot being installed (the existing purge rules are
preserved, not relaxed).

**REQ-M5-SNAP-006** — Snapshot receive/install paths MUST enforce the
configured byte bound and MUST NOT allocate unboundedly for a declared
snapshot size.

## 6. Corruption detection and fail-closed behavior

**REQ-M5-COR-001** — Corruption (bit flips, truncation, length or magic
mismatch, version mismatch, ordering violation) in any WAL record frame or
body, in the metadata image, or in a snapshot image MUST be detectable at
open, recovery, or install time (`REQ-DUR-004`).

**REQ-M5-COR-002** — Detected corruption in non-tail durable state MUST fail
closed: the affected group MUST NOT open or serve, and the corruption MUST
NOT be silently skipped, truncated away, or repaired from cached client
data, gossip, or peer hints (`REQ-DUR-004`).

**REQ-M5-COR-003** — Only a torn tail at the very end of the newest WAL
segment — bytes that could only have come from an interrupted append —
MAY be truncated per `REQ-M5-WAL-005`. Corruption anywhere else MUST follow
`REQ-M5-COR-002`. The implementation MUST NOT skip a corrupt middle record
and continue replay.

**REQ-M5-COR-004** — Bit-flip injection tests MUST cover: a flipped bit in a
WAL record body, in a WAL record frame (length/checksum), in the metadata
image, in a snapshot image at rest, and in snapshot bytes during install.
Each case MUST assert fail-closed behavior (open/install error; no partial
state served).

## 7. Recovery time

**REQ-M5-REC-001** — Recovery replays only WAL segments newer than the
purged prefix. Segment count per group is bounded by the configured segment
size and the purge discipline; recovery MUST NOT scan unbounded history.

**REQ-M5-REC-002** — Recovery time for a documented log size MUST be
measured on the exact verification identity, recorded in the evidence
ledger, and asserted by an automated test against a stated budget. The
budget is a regression tripwire on the documented identity, not a
production guarantee (see §9 honesty rule).

**REQ-M5-REC-003** — Recovery MUST reconstruct the same applied state as
the pre-crash committed prefix: applied position and data MUST match the
durable log/snapshot state, never newer (`REQ-DUR-003`).

## 8. Observability and operational tooling

**REQ-M5-OPS-001** — HomeKV MUST ship an offline, read-only WAL
inspection/verification utility that, given a group's storage directory,
lists segments with their index ranges, record counts, sizes, and
per-record checksum status, plus the metadata image's vote/committed/purged
view.

**REQ-M5-OPS-002** — The utility's `verify` mode MUST cross-check the
metadata segment inventory against the segment files on disk, distinguish
torn-tail truncation candidates from mid-state corruption, and exit
non-zero on any corruption that is not a truncatable torn tail.

**REQ-M5-OPS-003** — Storage metrics MUST cover: append attempts/entries/
failures, durable-persist attempts/failures and latency, recovery
attempts/failures and latency, group-commit batch size/wait stats, segment
count and rotation/deletion counters, truncation/purge counters, and
snapshot build/install attempts/failures/bytes. Cardinality MUST remain
bounded per group (no per-record metrics).

## 9. Non-goals and honesty rules

M5 explicitly does NOT include:

1. A new storage engine or a change to the OpenRaft `RaftLogStorage` /
   `RaftStateMachine` trait model; the durable content (entries, vote,
   committed position, snapshots) is unchanged, only the layout is
   hardened.
2. Any change to the M3 acknowledgement boundary (`REQ-DUR-005`); group
   commit changes batching, not the ack point.
3. A relaxed memory-only durable mode (`REQ-DUR-006`).
4. Cross-shard transactions, encryption at rest, WAL compression, or tiered
   storage.
5. Changes to routing, placement, membership, the compact data plane, or
   any wire format.
6. Production latency or throughput guarantees. **Honesty rule:** every
   latency/throughput/recovery-time number produced by M5 is a measured
   characterization on the documented exact identity (toolchain, OS,
   filesystem, hardware), recorded with its digest. No number may be
   presented as a production guarantee or used for comparative claims
   without a separately Accepted benchmark spec.

## 10. Open questions

1. Default segment size (candidate: 64 MiB) and default group-commit linger
   (candidate: sub-millisecond) — decided in M5-T1/T2 with measured
   justification, documented in design.
2. Whether the WAL utility ships as a `hkvctl` subcommand or a standalone
   binary — decided in M5-T5; the spec requires the capability either way.
3. WAL record payload encoding stays `bincode` as today; a format-version
   bump policy for future layout changes is documented in design but the
   bump itself is out of scope.
