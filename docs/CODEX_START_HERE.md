# Codex start here

This document is the intended entry point for machine-assisted
contributions. It captures what has been built, what remains,
and where to contribute safely.

Last updated: 2026-04-22

## Codebase snapshot

| Crate | Source lines | Tests | Status |
|-------|-------------|-------|--------|
| `tokeira-types` | 794 | — | Stable. Fully documented. |
| `tokeira-kernel` | 4,969 | 230 | Stable. All kernel features implemented. |
| `tokeira-storage` | 3,306 | 24 | Stable. In-memory store with shard-filtered queries. |
| `tokeira-runtime` | 19,907 | 246 | Complete. All runtime features + schedule/batch/nexus/versioning stores. |
| `tokeira-edge` | 17,460 | 141 | Active. All gRPC handlers implemented except eager dispatch. |
| `tokeira-proto` | 439 | — | Stable. Protobuf codegen. |
| `tokeira-projection` | 3,957 | 30 | Working. Visibility sink, rollups, filter compilation, query service. |

Total: ~67k lines of Rust, 671 tests, all passing.

## What has been built

### Kernel — complete

Pure deterministic state machine. All command variants handled.
Start, signal, WFT lifecycle, activities, timers, children,
external signals/cancels, nexus, updates, continue-as-new,
reset, pause/unpause, markers, execution options. 230 tests
(golden + property).

### Storage — in-memory complete

Full storage contract: OCC fencing, request dedup, history
append with pagination, activity/timer/nexus side tables,
dispatch backlog, projection log, lease management, shard
mapping, epoch validation. 24 tests.

### Runtime — complete

Lane executors, workflow + activity brokers, dispatch publisher,
timer/WFT/activity/nexus/workflow timeout scanners, child
orchestration, external signal/cancel delivery, nexus dispatch
(HTTP + worker-targeted), continue-as-new, OCC retry, recovery
sweeper, worker registry, versioning rule store, schedule store
+ execution engine, batch operation store, shard-aware lane
routing. 246 tests.

### Edge — nearly complete

All WorkflowService gRPC handlers implemented:

**Working handlers (tested end-to-end with SDK):**
- StartWorkflowExecution, SignalWorkflowExecution, SignalWithStartWorkflowExecution
- PollWorkflowTaskQueue, RespondWorkflowTaskCompleted, RespondWorkflowTaskFailed
- PollActivityTaskQueue, RespondActivityTaskCompleted, RespondActivityTaskFailed
- RecordActivityTaskHeartbeat
- TerminateWorkflowExecution, RequestCancelWorkflowExecution
- QueryWorkflow, UpdateWorkflowExecution, PollWorkflowExecutionUpdate
- DescribeWorkflowExecution, ListWorkflowExecutions, CountWorkflowExecutions
- GetWorkflowExecutionHistory, GetWorkflowExecutionHistoryReverse
- DeleteWorkflowExecution, ResetWorkflowExecution
- RegisterNamespace, DescribeNamespace, ListNamespaces
- DescribeTaskQueue, GetClusterInfo, GetSystemInfo
- ResetStickyTaskQueue, RespondQueryTaskCompleted

**Implemented in this cycle (spec'd + coded):**
- CreateSchedule, DescribeSchedule, UpdateSchedule, DeleteSchedule
- PatchSchedule, ListSchedules, ListScheduleMatchingTimes
- StartBatchOperation, StopBatchOperation, DescribeBatchOperation, ListBatchOperations
- UpdateWorkerVersioningRules, GetWorkerVersioningRules, GetWorkerTaskReachability
- ShutdownWorker
- PollNexusTaskQueue (broker + worker-targeted dispatch)
- RespondNexusTaskCompleted, RespondNexusTaskFailed

**Returning UNIMPLEMENTED (documented):**
- Legacy versioning (UpdateWorkerBuildIdCompatibility, GetWorkerBuildIdCompatibility)
- Deployment management (5 handlers)
- Legacy listing (4 handlers)
- Activity by-ID (5 handlers)
- Namespace mutation (UpdateNamespace, DeprecateNamespace)
- ExecuteMultiOperation, GetSearchAttributes, ListTaskQueuePartitions
- Activity/WF options (5 handlers)

**Not yet implemented (spec'd, ready for implementation):**
- Eager dispatch (Feature 9) — spec complete

### Projection — working

Visibility sink, rollups, filter compilation, query service,
worker — all working against InMemoryVisibilityStore. 30 tests.

### Architecture documentation — organized

- 005-decisions-and-boundaries.md: ground truth for resolved decisions
- 9 docs promoted to "accepted" status
- 3 docs marked as "future direction"
- All umbrella features (1-9) have complete specs

### SDK examples working

- hello_world, message_passing, continue_as_new, child_workflows,
  timers, schedules — all running against tokeirad

## Completion assessment

| Plane | Estimate | Notes |
|-------|----------|-------|
| Compatibility Edge | ~85% | All handlers except eager dispatch. Proto field fidelity Features 1-4 done. |
| Runtime & Storage | ~55% | Runtime complete for in-memory. DSQL storage not started. |
| Projection | ~65% | Working against in-memory. SQL visibility (DSQL) not started. |
| Platform / Ops | ~15% | Missing placement, autoscaling, telemetry, admin tooling. |
| **Overall** | **~45%** | Core correctness works end-to-end. DSQL + ops are the remaining bulk. |

## What to work on next (priority order)

### P0: DSQL Storage Layer

The single largest remaining work item. Everything else runs
against InMemoryStore. Production requires Aurora DSQL.

- Schema implementation (schema already designed in docs/dsql/)
- DSQL plugin for tokeira-storage (connection reservoir already built)
- Transaction mapping: one workflow transition = one DSQL transaction
- OCC retry with DSQL conflict detection
- Shard lease management via DynamoDB
- Migration tooling

**Why P0:** Nothing else matters for production without durable storage.

### P1: Eager Dispatch (Feature 9)

Spec complete, ready for implementation. Eliminates a poll
round-trip for workflow starts and activity scheduling.

- Broker try_claim methods (targeted by run_key/activity_id)
- PollerRegistry compatible-poller check
- Eager WFT on StartWorkflowExecution
- Eager activity tasks on RespondWorkflowTaskCompleted

**Why P1:** Direct latency improvement. SDK expects it.

### P1: SQL Visibility on DSQL

The projection plane works against in-memory. Production needs
DSQL-backed visibility.

- SQL query planner for DSQL
- Visibility table schema
- Projection sink writing to DSQL
- Search attribute indexing

**Why P1:** ListWorkflowExecutions and CountWorkflowExecutions
need durable visibility for production.

### P2: Connection Reservoir Integration

The reservoir is already built (in temporal-dsql workspace).
Needs integration with tokeira-storage's DSQL plugin.

- Wire reservoir into DSQL connection pool
- Token bucket rate limiter integration
- Slot block manager for distributed connection limiting

**Why P2:** Required for DSQL at scale (10K connection limit).

### P2: Shard Placement and Membership

Currently single-node with in-memory shard ownership. Production
needs distributed shard assignment.

- DynamoDB-based shard lease table
- Shard acquisition/relinquishment protocol
- Multi-node membership discovery
- Shard rebalancing on node join/leave

**Why P2:** Required for horizontal scaling.

### P3: Telemetry and Observability

Metrics, tracing, and logging for production operations.

- Prometheus metrics export
- OpenTelemetry tracing integration
- DSQL-specific metrics (conflicts, retries, latency)
- Broker/scheduler/scanner operational metrics

### P3: Archival to S3

Deferred but needed for long-term history retention.

- Archival service design
- S3 sink for completed workflow histories
- Archival configuration per namespace

### P3: Remaining Proto Field Gaps

Features 1-4 from the edge-complete-implementation umbrella
are spec'd but some have remaining implementation work:

- Feature 4 (Describe and Operational Responses) — pending fields
- History serializer completeness for all event types

## Invariants to preserve

- Never let transport or storage details leak into the kernel.
- Never make the projection path authoritative.
- Never let pollers or waiters become durable correctness objects.
- Never assume a lane owns a run forever.
- Never make inactivity expensive.
- No state visible to the system unless explained by a committed
  transition (010-history-as-authority).

## Spec structure

Each feature has a spec directory under `.kiro/specs/`:

```
.kiro/specs/{feature-name}/
  .config.kiro      — spec metadata
  requirements.md   — user stories + acceptance criteria
  design.md         — architecture, data models, properties
  tasks.md          — implementation plan with checkpoints
```

Architecture decisions are recorded in:
- `docs/architecture/005-decisions-and-boundaries.md`

## TODO style

The codebase uses rich TODO comments:

- `TODO(correctness): ...`
- `TODO(perf): ...`
- `TODO(storage): ...`
- `TODO(edge): ...`
- `TODO(ops): ...`
- `TODO(runtime): ...`

Extend that convention instead of adding generic TODOs.
