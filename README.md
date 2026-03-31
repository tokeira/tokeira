# Tokeira minimal foundation workspace

This workspace is intentionally **small** and **architecturally opinionated**.
It is not meant to be a feature-complete Temporal replacement. It is meant to be:

- small enough for a new contributor or Codex to understand quickly,
- explicit enough that architectural invariants are visible in code,
- structured enough that new features have obvious homes,
- incomplete enough that the next steps are obvious rather than hidden.

## Included crates

- `tokeira-types` — shared identifiers and durable-domain value types.
- `tokeira-kernel` — pure workflow transition engine.
- `tokeira-storage` — persistence and lease interfaces plus an in-memory dev store.
- `tokeira-runtime` — lane-based orchestration and a broker for workflow task delivery.
- `tokeira-projection` — projection worker and a small in-memory visibility sink.
- `tokeirad` — a tiny binary shell that wires the pieces together for local exploration.

## Deliberately omitted

The following are intentionally **not** included in this minimal foundation set:

- full gRPC/HTTP edge transport,
- proto generation,
- SQL/DSQL implementation,
- autoscaler,
- placement controller,
- archival service,
- production observability plumbing.

Those pieces are important, but they are easier to add once the kernel/runtime/storage seams are clear.

## Architectural invariants

These are the invariants Codex should treat as design constraints, not accidents.

1. **A workflow run is the unit of correctness.**
   A shard, lane, broker, or projector may move or fail, but correctness lives at the run.
2. **The kernel is pure.**
   It should not know about DSQL, ECS, load balancers, or worker connections.
3. **History is authoritative.**
   Dispatch and projection are derived effects.
4. **Projection is outside the correctness path.**
   A lagging projection is acceptable; a corrupt commit path is not.
5. **Lanes are execution-locality tools, not correctness boundaries.**
   They exist to reduce coordination and improve cache locality.
6. **Configuration surface should stay minimal.**
   Prefer policies and auto-tuning over exposed mechanical knobs.

## Suggested first contributions

See `docs/CODEX_START_HERE.md` for a contribution map and safe next tasks.
