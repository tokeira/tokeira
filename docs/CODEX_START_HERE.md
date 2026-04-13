# Codex start here

This document is the intended entry point for machine-assisted
contributions. It captures what has been built, what remains,
and where to contribute safely.

## Codebase snapshot

| Crate | Source lines | Tests | Status |
|-------|-------------|-------|--------|
| `tokeira-types` | 725 | — | Stable. Fully documented. |
| `tokeira-kernel` | 3,811 | 221 (153 golden + 68 property) | Stable. All 10 kernel features implemented. |
| `tokeira-storage` | 2,705 | 21 | Stable. In-memory store with shard-filtered queries. |
| `tokeira-runtime` | 13,231 | 125 unit + 37 integration | Complete. All 15 runtime features implemented. |
| `tokeira-edge` | 3,010 | 15 + 1 integration | Stable. Workflow gRPC transport. |
| `tokeira-proto` | 508 | — | Stable. Protobuf codegen. |
| `tokeira-projection` | 137 | — | Stub. |

Total: ~37k lines of Rust, 162 runtime tests + 221 kernel tests + 21 storage tests + 16 edge tests, all passing.

## What has been built

### `tokeira-kernel` — complete

All 10 features from the kernel master spec are implemented:

1. Foundation + WFT lifecycle
2. WFT failure/timeout recovery
3. Cancel and terminate
4. Continue-as-new + workflow timeout
5. Child workflows
6. External signals and cancel requests
7. Updates (two-phase)
8. Markers and execution options
9. Nexus operations
10. Reset

The kernel is pure, deterministic, and has no I/O. Every
command variant in the `Command` enum is handled. Every
`WorkflowCommand` variant is processed. Property tests
cover all correctness properties.

### `tokeira-storage::memory` — complete

The in-memory dev store implements the full storage contract:

- OCC fencing with conflict injection for tests
- Request dedup
- History append with pagination
- Independent activity state and timer bucket tables
- Activity task dispatch tracking and sweep
- Explicit-persist dispatch backlog (Tier C model)
- Projection log with partition/fanout
- Lease management with epoch fencing
- `AllowAfterClose` conflict policy
- Shard-to-run mapping with deterministic assignment
- Six shard-filtered query methods for sweep reconstruction
- Epoch validation on `commit_transition`
- 21 property and unit tests

### `tokeira-runtime` — all 15 features implemented

The runtime crate is organized into focused modules:

```
tokeira-runtime/src/
  runtime.rs    — TokeiraRuntime facade
  lane.rs       — lane executor, OCC retry, mailbox coalescing
  broker.rs     — workflow + activity task brokers
  publisher.rs  — RuntimeDispatchPublisher (all dispatch ops)
  backlog.rs    — grace scanner, drain loop, durable backlog
  retry.rs      — activity retry evaluation (pure functions)
  timeout.rs    — workflow timeout tracking + scanner
  scanner.rs    — timer scanner + lane routing helpers
  nexus.rs      — Nexus types, endpoint registry, timeout scanner
  query.rs      — query dispatch (QueryTask, QueryResult)
  update.rs     — update lifecycle (UpdateRegistry, UpdateOutcome)
  fairness.rs   — delivery metrics, drain share, control loop
  activity_timeout.rs — activity tracking + timeout scanner
  shard.rs      — shard ownership, epoch fencing
  recovery.rs   — sweep_shard, lease renewer
  worker_registry.rs — worker version metadata
```

Implemented features:

| # | Feature | Status |
|---|---------|--------|
| 1 | Lane OCC retry + mailbox coalescing | ✅ Implemented |
| 2 | Activity pump (dispatch, poll, complete, retry) | ✅ Implemented |
| 3 | Activity heartbeat + timeouts | ✅ Implemented (1 optional integration test remaining) |
| 4 | Timer scanner | ✅ Implemented |
| 5 | Workflow timeouts | ✅ Implemented |
| 6 | Child workflow orchestration | ✅ Implemented |
| 7 | External signal + cancel delivery | ✅ Implemented |
| 8 | Continue-as-new | ✅ Implemented |
| 9 | Nexus operation dispatch | ✅ Implemented |
| 10 | Worker versioning + deployment routing | ✅ Implemented |
| 11 | Sweeper and recovery | ✅ Implemented (1 optional integration test remaining) |
| 12 | Durable backlog integration | ✅ Implemented |
| 13 | Query dispatch | ✅ Implemented |
| 14 | Update two-phase lifecycle | ✅ Implemented (2 optional tests remaining) |
| 15 | Broker fairness and admission | ✅ Implemented (12 property tests remaining) |

### `tokeira-edge` — workflow transport complete

The gRPC edge layer handles:

- StartWorkflowExecution
- SignalWorkflowExecution
- PollWorkflowTaskQueue
- RespondWorkflowTaskCompleted
- DescribeWorkflowExecution
- ListWorkflowExecutions (stub)
- GetSystemInfo
- Operator service (namespace CRUD)

Missing edge endpoints (not yet specced):

- PollActivityTaskQueue gRPC handler
- RespondActivityTaskCompleted/Failed gRPC handler
- RecordActivityTaskHeartbeat gRPC handler
- TerminateWorkflowExecution gRPC handler
- RequestCancelWorkflowExecution gRPC handler
- QueryWorkflow gRPC handler
- UpdateWorkflow gRPC handler

## What remains to be built

### Remaining optional tests

Several completed features have optional tests that were
skipped during implementation:

- Feature 3 (Activity timeouts): 1 optional integration test
- Feature 11 (Sweeper and recovery): 1 optional integration test
- Feature 14 (Update lifecycle): 2 optional tests
- Feature 15 (Broker fairness): 12 property tests

### Edge layer gaps

The edge layer needs activity and advanced workflow gRPC
endpoints. These are not covered by any existing spec:

- `PollActivityTaskQueue` → `runtime.poll_activity_task`
- `RespondActivityTaskCompleted` → `runtime.complete_activity_task`
- `RespondActivityTaskFailed` → `runtime.fail_activity_task`
- `RecordActivityTaskHeartbeat` → `runtime.record_activity_heartbeat`
- `TerminateWorkflowExecution` → `runtime.terminate_workflow`
- `RequestCancelWorkflowExecution` → `runtime.cancel_workflow`
- `QueryWorkflow` → query dispatch (Feature 13)
- `UpdateWorkflow` → update lifecycle (Feature 14)

The existing `grpc-edge-transport` spec could be extended,
or new edge specs created for each endpoint group.

### Projection plane

`tokeira-projection` is a stub (137 lines). The storage
layer has `ProjectionLog` and `ProjectionRecord` types, and
the kernel emits `ProjectionOp`s, but no projection sink or
visibility query engine exists yet.

Work needed:
- Projection sink that consumes `ProjectionOp`s and
  materializes visibility rows
- SQL visibility query planner
- Page tokens for list queries
- Rollup aggregations
- Search attribute indexing

## The safest places to contribute next

### Add activity gRPC endpoints to `tokeira-edge`

The runtime already has `poll_activity_task`,
`complete_activity_task`, `fail_activity_task`, and
`record_activity_heartbeat`. The edge layer just needs gRPC
handlers that translate proto ↔ internal types.

### Add advanced workflow gRPC endpoints to `tokeira-edge`

The runtime now has `query_workflow`, `update_workflow`,
`terminate_workflow`, and `cancel_workflow`. The edge layer
needs gRPC handlers for these.

### Complete remaining optional tests

Features 3, 11, 14, and 15 have optional tests that were
skipped. These are safe, isolated contributions.

### Extend `tokeira-projection`

The projection plane is independent of the runtime features.
It consumes committed `ProjectionOp`s and materializes
visibility rows. Safe to work on in parallel.

## Invariants to preserve

- Never let transport or storage details leak into the
  kernel.
- Never make the projection path authoritative.
- Never let pollers or waiters become durable correctness
  objects.
- Never assume a lane owns a run forever.
- Never make inactivity expensive.
- No state visible to the system unless explained by a
  committed transition (010-history-as-authority).

## TODO style

The codebase uses rich TODO comments:

- `TODO(correctness): ...`
- `TODO(perf): ...`
- `TODO(storage): ...`
- `TODO(edge): ...`
- `TODO(ops): ...`
- `TODO(runtime): ...`

Extend that convention instead of adding generic TODOs.

## Spec structure

Each feature has a spec directory under `.kiro/specs/`:

```
.kiro/specs/{feature-name}/
  .config.kiro      — spec metadata
  requirements.md   — user stories + acceptance criteria
  design.md         — architecture, data models, properties
  tasks.md          — implementation plan with checkpoints
```

The master specs provide the dependency graph:

- `.kiro/specs/kernel-complete-implementation/requirements.md`
- `.kiro/specs/runtime-complete-implementation/requirements.md`
