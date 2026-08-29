# Spec 0003 — Shard-Owned In-Memory Engine Tasks

- Status: Accepted
- Requirements: `requirements.md`
- Design: `design.md`
- Tracking issue: #9

Implementation may begin because this spec is Accepted.

## M1-T1 — Logical shard identity and mapping

Requirements: `REQ-M1-SHARD-001/002`, parent `REQ-SHARD-004/005`

- add `ShardId` bounded to `0..1024`
- add `shard_for_key(raw_bytes)` using XXH3-64 low 10 bits
- add golden vectors and broad deterministic/property tests
- keep mapping independent of membership/node state

Completion: compatibility tests prove stable mapping for checked-in vectors and all returned IDs are `< 1024`.

## M1-T2 — Candidate A owner engine core

Requirements: `REQ-M1-SHARD-003/004`, `REQ-M1-DATA-001/002/003/006`, `REQ-M1-PERF-001`

- introduce shard engine/handle module
- bounded Tokio MPSC request channel
- single owner task with shard-local `HashMap<Vec<u8>, Vec<u8>>`
- GET/PUT/DELETE
- owned reply values
- deterministic mutation representation
- explicit engine-local errors

Completion: unit/integration tests prove GET/PUT/DELETE semantics and code inspection/tests show no whole-map clone on mutation.

Prerequisite: M1-T1.

## M1-T3 — Atomic single-shard batches

Requirements: `REQ-M1-DATA-004/005/006`

- define non-nested mutation batch
- reject cross-shard batches before admission
- apply accepted batch in owner loop with no await/interleaving point
- concurrent observer tests proving no partial visibility
- repeated/idempotent PUT/DELETE batch behavior

Completion: concurrent histories only observe pre-batch or post-batch states.

Prerequisite: M1-T2.

## M1-T4 — Memory accounting and metrics

Requirements: `REQ-M1-MEM-001/002`, `REQ-M1-QUEUE-004`

- logical key/value byte accounting
- correct insert/replace/delete/batch deltas
- key count
- queue capacity/depth snapshot
- accepted/applied mutation counter
- overload rejection counter

Completion: table-driven accounting tests cover insert, replace smaller/larger, delete present/absent, and batches with repeated keys.

Prerequisite: M1-T2/T3.

## M1-T5 — Backpressure, cancellation, and lifecycle

Requirements: `REQ-M1-QUEUE-001/002/003`, `REQ-M1-LIFE-001/002`

- explicit queue capacity
- async bounded admission
- `try_*` overload path returning `QueueFull`
- cancellation tests before and after enqueue acceptance
- explicit drain-and-shutdown behavior
- reject operations after shutdown boundary

Completion: stress tests prove bounded admission and deterministic accepted-command behavior under saturation/cancellation/shutdown.

Prerequisite: M1-T2.

## M1-T6 — Server/storage migration adapter

Requirements: parent `REQ-DATA-001/002/003/004`, `REQ-SDD-002`

- introduce a narrow adapter so current server/service logic can execute against Candidate A without changing the external wire contract
- retain prototype path only where required for immutable M0 tooling
- do not add M2 protocol redesign or distributed consensus
- add integration tests through the existing public API

Completion: existing GET/SET/DELETE API tests pass using the shard-owned engine path, while M0's frozen comparison remains reproducible from its immutable commit/artifact.

Prerequisite: M1-T2..T5.

## M1-T7 — M1 benchmark and memory comparison

Requirements: `REQ-M1-PERF-002/003/004/005`, `REQ-M1-MEM-003`

- add `shard` benchmark layer using existing deterministic generator/result schema
- primary 16B/64B, 1k/10k/50k, GET/SET/DELETE/80-20 matrix
- >=10k measured operations/cell unless documented otherwise
- 3 repeated runs
- process RSS/logical bytes evidence
- compute M1/M0 ratios and 50k/1k mutation scaling
- retain all repetitions and host metadata

Completion: repeatable evidence is sufficient to execute the M1 performance gate.

Prerequisite: M1-T2..T6.

## M1-T8 — Candidate B Crossbeam experiment

Requirements: Candidate B and promotion rule in `requirements.md`; ADR 0004

- benchmark bounded `ArrayQueue` independently where useful
- optionally implement single-writer `SkipMap` + sequence-validated direct GET
- concurrency/model tests for batch visibility and sequence publication
- direct-read retry metrics
- equivalent-semantics comparison at 1/8/32 reader concurrency, 100/0, 95/5, 80/20, 50/50, uniform/skewed
- memory/reclamation evidence

Completion: either Candidate B satisfies every promotion criterion and is promoted by an explicit spec/result update, or Candidate A remains primary and the experiment is recorded as non-promoted evidence.

Prerequisite: M1-T7. This task is optional for M1 verification if Candidate A already satisfies all mandatory M1 requirements.

## M1-T9 — Verification handoff

- execute `verification.md` requirement matrix
- record exact M1 comparison commits/workflows/artifacts
- separate measured observations from hypotheses
- mark Candidate A/B decision explicitly
- move Spec 0003 to `Verified` only if every mandatory gate passes
- update #9 and unblock M2 only after merge

Prerequisite: all mandatory preceding tasks.
