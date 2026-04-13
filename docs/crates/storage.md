# tokeira-storage

Storage interfaces and an in-memory development store. Defines the durable persistence contract that the runtime depends on, and provides `InMemoryStore` for tests, examples, and development. No real DSQL implementation yet — the goal is to make contracts explicit first.

## Dependencies

- `tokeira-kernel` — `WorkflowState`, `HistoryEvent`, `Transition`, `LoadedRun`, `ActivityState`, `TimerState`
- `tokeira-types` — identity types, queue keys, tokens
- External: `anyhow`, `async-trait`, `time`, `tokio`, `tracing`

## Module Structure

| File | Contents |
|---|---|
| `api.rs` | `RunRepository` trait, `ProjectionLog` trait, `LeaseRepository` trait, `ConnectionDirector` trait, plus all supporting types |
| `memory.rs` | `InMemoryStore` — full implementation of all repository traits |

## RunRepository Trait

Core storage contract. Key methods:

| Method | Purpose |
|---|---|
| `resolve_execution` | Map `ExecutionRef` → `RunKey` (current open run lookup) |
| `find_latest_run` | Resolve closed workflows by namespace + workflow_id |
| `load_run` | Load `LoadedRun` (Absent or Existing with full `WorkflowState`) |
| `read_history` | Paginated history read (after_event_id, limit) |
| `lookup_request_dedupe` | Check if a request ID was already applied |
| `read_transition_audit` | Read transition audit log for a run |
| `commit_transition` | Fenced commit: OCC check on `TransitionSeq`, epoch validation on `ShardEpoch` |
| `materialize_reset_successor` | Create a new run by replaying a history prefix up to a fork point |
| `list_dispatchable_workflow_tasks` | Queue-scoped query for pending WFTs |
| `list_dispatchable_activity_tasks` | Queue-scoped query for pending activity tasks |
| `persist_to_backlog` / `drain_backlog` | Durable backlog for overflow dispatch |
| `list_due_timers` | Global timer scan |
| `list_*_for_shard` | Six shard-filtered sweep queries: workflow tasks, activity tasks, timers, workflow timeouts, open activities, pending Nexus operations |

## Supporting Types

- `CommitResult` — `Applied { transition_seq, last_event_id, execution_status, new_run_id }` or `Conflict`
- `DispatchableWorkflowTask` / `DispatchableActivityTask` — task descriptors for broker dispatch
- `BacklogEntry` / `BacklogPayload` — durable overflow entries
- `DueTimer`, `WorkflowTimeoutSweepEntry`, `ActivitySweepEntry`, `NexusSweepEntry` — sweep query results
- `CurrentExecutionConflictPolicy` — `Reject` (default) or `AllowAfterClose`
- `ProjectionRecord`, `ProjectionContext`, `ProjectionBatch` — projection log types
- `RequestRecord`, `TransitionAuditRecord` — dedup and audit types

## ProjectionLog Trait

- `read_from(cursor, limit)` → `ProjectionBatch` — ordered log consumption for the projection worker

## LeaseRepository Trait

- `try_acquire_bundle(shard_id, owner)` → `LeaseOutcome` — shard lease acquisition with epoch bumping
- `renew_bundle(shard_id, owner, epoch)` → `LeaseOutcome` — lease renewal with epoch fencing

## ConnectionDirector Trait

- `acquire(DbClass)` → `DbPermit` — connection admission control (placeholder for DSQL reservoir)

## InMemoryStore

Full implementation of `RunRepository`, `ProjectionLog`, `LeaseRepository`, and `ConnectionDirector`. Features:

- OCC fencing with `inject_conflict()` for test scenarios
- Configurable `CurrentExecutionConflictPolicy` (Reject or AllowAfterClose)
- Deterministic shard assignment via `run_key % shard_count`
- Epoch validation on `commit_transition`
- History append with pagination support
- Independent activity state (with `started_event_id`) and timer bucket tables
- Activity task dispatch tracking
- Projection log with partition/fanout
- Lease management with epoch fencing
- Request dedup persistence
- Transition audit log

## Tests

21 property and unit tests in `memory.rs` covering OCC conflicts, backlog ordering, reset materialisation, conflict policies, timer bucketing, and sweep queries.
