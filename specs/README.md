# HomeKV Spec-Driven Development

HomeKV is developed spec-first. Significant behavior, architecture, protocol, persistence, consistency, or performance work must be specified before implementation begins.

## Workflow

```text
problem / proposal
      |
      v
Requirements  -->  Design  -->  Tasks  -->  Implementation  -->  Verification
      ^              |                         |                    |
      +--------------+-------------------------+--------------------+
                    feedback / amendment
```

A GitHub issue alone is not an implementation contract. The normative contract lives under `specs/`.

## Spec states

Every spec declares one of these states:

- **Draft** — requirements/design are still changing. No production implementation should depend on it.
- **Review** — complete enough for technical review; open questions are explicit.
- **Accepted** — requirements, invariants, design, and verification plan are approved. Implementation may begin.
- **Implementing** — code is being developed against the accepted spec.
- **Verified** — implementation satisfies the spec's acceptance and verification criteria.
- **Superseded** — replaced by another spec; the replacement must be linked.

A material semantic change after `Accepted` requires a spec amendment before the implementation is merged.

## Directory structure

Each significant feature gets a numbered directory:

```text
specs/
  0001-homekv-v1/
    requirements.md
    design.md
    tasks.md
    verification.md
```

The four files serve different purposes.

### `requirements.md`

Defines **what must be true**, without prematurely choosing implementation details.

It should contain:

- motivation and scope
- user/system-visible behavior
- functional requirements (`REQ-*`)
- consistency/durability requirements
- failure behavior
- performance requirements/budgets where applicable
- explicit non-goals
- unresolved questions

Requirements use stable IDs such as `REQ-CONS-001` so tests, tasks, PRs, and benchmarks can trace back to them.

### `design.md`

Defines **how the accepted requirements will be satisfied**.

It should contain:

- architecture and component boundaries
- state machines and ownership
- data structures and memory model
- protocols / wire formats
- persistence model
- concurrency model
- invariants
- failure/recovery paths
- alternatives considered
- operational/observability considerations

Design decisions should trace to requirement IDs.

### `tasks.md`

Breaks accepted design into implementation slices.

Each task must:

- reference the requirements/design sections it implements
- be independently reviewable where practical
- define completion criteria
- identify prerequisite tasks
- avoid mixing unrelated architectural changes

### `verification.md`

Defines **how we prove the implementation satisfies the spec**.

It should include:

- requirement-to-test matrix
- unit/property/integration tests
- distributed fault-injection scenarios
- linearizability tests where applicable
- benchmark gates and workloads
- recovery tests
- known limitations and residual risk

A performance optimization is not verified solely because a microbenchmark improves.

## Traceability

HomeKV uses end-to-end traceability:

```text
REQ-CONS-001
   -> design: per-shard Raft ordering
   -> task: implement proposal/commit/apply pipeline
   -> tests: leader partition + history checker
   -> benchmark: durable 3-replica PUT p99
```

Implementation PRs should name the spec and requirement/task IDs they satisfy.

## Pull request policy

### Spec PR

A spec PR changes `specs/` and related architecture documents. It should normally be merged before implementation begins.

A spec PR answers:

- Are the requirements complete and falsifiable?
- Are consistency and failure semantics explicit?
- Is the design internally consistent?
- Are alternatives/trade-offs understood?
- Can the verification plan actually prove the requirements?

### Implementation PR

An implementation PR must link to an **Accepted** spec and list the requirement/task IDs it implements.

It should not silently change the spec. If implementation discovers a flawed assumption, update/review the spec first.

### Verification PR/result

A milestone is complete only after its verification criteria pass. The spec state then moves to `Verified`.

## Performance-specific rule

HomeKV's north star includes extreme performance, but correctness semantics are part of the benchmark configuration.

Performance requirements must specify:

- consistency mode
- durability mode
- replication factor
- workload
- key/value sizes
- concurrency
- hardware assumptions where relevant
- p50/p95/p99 (and p99.9 where practical)
- throughput and CPU/memory cost

No optimization may weaken the accepted consistency/durability contract without a new or amended spec.

## Small changes

Typos, comments, dependency patches, and straightforward refactors that do not alter observable behavior or architectural invariants do not require a new numbered spec.

When uncertain, write the spec. Distributed databases are expensive places for implicit assumptions.
