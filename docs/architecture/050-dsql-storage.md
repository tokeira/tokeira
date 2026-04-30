# 050 DSQL Storage

**Status:** accepted — resolved questions recorded in [005-decisions-and-boundaries](005-decisions-and-boundaries.md)  
**Related docs:** [010-history-as-authority](010-history-as-authority.md), [020-kernel](020-kernel.md), [060-connection-management](060-connection-management.md)

## Purpose

`tokeira-storage` is where the architecture becomes deliberately **Aurora DSQL-shaped**.

The goal is not to hide DSQL behind a generic “database abstraction.” The goal is to design a storage layer that treats DSQL’s actual constraints as first-class architecture inputs:

- fixed `Repeatable Read` isolation,
- optimistic concurrency control,
- conflict detection at commit,
- 3,000-row mutation limit,
- 5-minute transaction-age limit,
- 60-minute connection lifetime,
- one database per cluster,
- small schema/table budgets,
- no temporary tables,
- primary-key-sensitive distribution,
- asynchronous index creation.[^dsql-migration][^dsql-quotas][^dsql-pk][^dsql-create-index]

## Storage design goal

The storage layer should make this statement true:

> **One workflow transition is one fenced DSQL transaction.**

That transaction should be narrow, retryable, and fully explainable.

## Why DSQL changes the design

Aurora DSQL is not “PostgreSQL with infinite scale.” AWS’s migration guide explicitly calls out architectural differences:

- use CTEs/subqueries instead of temp tables,
- move trigger/procedural logic into the application,
- design for optimistic conflicts and retry,
- use random primary keys for hot write tables,
- separate DDL and DML transactions,
- expect a single built-in database per cluster.[^dsql-migration]

Those are not incidental details. They push Tokeira toward:

- application-managed state transitions,
- narrow write sets,
- single-writer hot paths,
- shared schemas instead of table explosion,
- typed side tables instead of ad hoc temp-table query plans.

## Core schemas

A first-pass storage shape should stay compact:

```text
core/
  shard_lease
  current_execution
  workflow_hot
  history_batch
  request_dedupe
  activity_state
  timer_bucket

delivery/
  dispatch_backlog

proj/
  projection_log
  projector_checkpoint
  vis_execution
  vis_attr_*
```

This is intentionally small because DSQL limits schemas and tables.[^dsql-quotas]

## Table roles

### `core.shard_lease`

Fenced ownership of a shard.

### `core.current_execution`

Maps `(namespace_id, workflow_id)` to the current run identity and open/closed status.

### `core.workflow_hot`

The small current summary row for an open run.

### `core.history_batch`

Immutable append-only event batches.

### `core.request_dedupe`

Idempotency and duplicate suppression.

### `core.activity_state`

Normalized current state of open activities.

### `core.timer_bucket`

Bucketed wakeup records for due-time scanning.

### `delivery.dispatch_backlog`

Durable fallback backlog only for work that was not delivered inline or via the live-ready window.

### `proj.projection_log`

Typed durable mutations for visibility and custom sinks.

## Primary key design

AWS is unusually explicit here: on high-write tables, avoid monotonically increasing primary keys because they drive inserts into one hot partition; prefer random/distributed keys.[^dsql-pk]

Tokeira should follow that directly.

### Good keys

- `workflow_hot.run_key = UUID`
- `history_batch(run_key, first_event_id)`
- `activity_state(run_key, schedule_event_id)`

These keys cluster by run, which is helpful because the run already has single-writer behavior.

### Special case: append-like system tables

For `dispatch_backlog` and `projection_log`, we should *not* lead with an ascending timestamp alone. Instead, add a **fanout/hash dimension before the time-like key**:

```text
(partition_or_queue_hash, fanout, slot, run_key, ...)
```

This gives DSQL multiple write ranges instead of one hot edge.

## Transaction boundary

Every state mutation should be committed with an explicit expectation of:

- shard epoch,
- current transition sequence,
- possibly current pending logical task sequence.

If any expectation fails, the transaction aborts and runtime decides whether to retry, reload, or reject.

This fits DSQL’s optimistic concurrency model: let transactions proceed, detect conflicts at commit, and retry in application logic where appropriate.[^dsql-migration]

## Representative transaction recipes

### 1. `StartWorkflowExecution`

Transaction:

1. check `current_execution` conflict policy,
2. insert `current_execution`,
3. insert `workflow_hot`,
4. append `WorkflowExecutionStarted`,
5. append `WorkflowTaskScheduled`,
6. arm run/workflow timeout timers as needed,
7. emit projection mutation,
8. optionally inline-start WFT if a waiter exists.

Key point: starting the workflow and creating the first WFT are one authoritative transition.

### 2. `SignalWorkflowExecution`

Transaction:

1. load `current_execution`,
2. load `workflow_hot`,
3. validate / record request dedupe,
4. append one or more `WorkflowExecutionSignaled` events,
5. schedule WFT **only if one is not already pending**,
6. emit projection mutations if visible fields changed,
7. optionally inline-start WFT.

Key point: signal bursts should coalesce into one transition when practical.

### 3. `StartWorkflowTask`

Transaction:

1. load `workflow_hot`,
2. validate `pending_wft.logical_seq`,
3. append `WorkflowTaskStarted`,
4. update started-event ID / attempt / sticky hint,
5. remove backlog record if this came from backlog.

Key point: task start is authoritative only after history says it started.

### 4. `RespondWorkflowTaskCompleted`

Transaction:

1. load `workflow_hot`,
2. validate token against started-event ID / attempt / epoch,
3. apply kernel transition,
4. append history batch,
5. patch `workflow_hot`,
6. upsert/delete activity state,
7. upsert/delete timer rows,
8. emit dispatch ops,
9. emit projection ops.

Key point: this is the primary write path and must stay bounded.

### 5. `TimerDue`

Timer scanning is not authoritative. The authoritative transition is:

1. load run,
2. revalidate that timer is still pending,
3. append timer-fired/timeout event(s),
4. schedule WFT if necessary,
5. delete the timer row.

## Retry policy

Tokeira should not hide OCC behavior from the runtime. The storage layer should classify outcomes into:

- success,
- duplicate,
- retryable conflict,
- fatal validation conflict.

That lets runtime decide whether to:

- immediately retry,
- reload and recompute,
- reject to caller,
- fail shard ownership.

## Why no temp tables

Aurora DSQL’s migration guide explicitly recommends CTEs, subqueries, or regular tables with cleanup instead of temporary tables.[^dsql-migration] That strongly affects visibility and reporting queries. Tokeira should therefore standardize on:

- CTE-based query compilation,
- typed side-index tables,
- background-maintained rollups where necessary.

## Why no trigger logic

Aurora DSQL supports SQL functions but not PL/pgSQL, and the migration guide recommends moving trigger-like behavior to the application layer.[^dsql-migration] Tokeira is already doing that: the kernel and projection plane define semantics, storage only commits them.

## Why no foreign-key-heavy cascade semantics in the hot path

Aurora DSQL’s migration guide explicitly warns that large cascading operations can create undesirable transaction size and performance characteristics, and recommends application-managed referential integrity patterns.[^dsql-migration] For Tokeira, that reinforces the design choice to keep workflow transition logic in application code and maintain denormalized side state explicitly.

## DDL strategy

Because DSQL separates DDL and DML transaction concerns and offers non-blocking `CREATE INDEX ASYNC`, Tokeira should adopt a conservative migration discipline:

- schema migrations happen separately from hot-path runtime traffic,
- each migration file contains exactly one DDL statement (DSQL requires one DDL per transaction),
- visibility/index evolution uses async index creation,
- rollout state is tracked operationally, not via runtime hot path.[^dsql-create-index]

DSQL supports CHECK constraints in CREATE TABLE, but Tokeira uses application-level validation in Rust for testability and flexibility. Foreign keys are not used in the hot path; referential integrity is application-managed.

### Serialization format

All BYTEA columns use `postcard` (compact binary serde encoding). Domain types derive `Serialize, Deserialize`; no separate schema definitions or mapping code is needed. Postcard's varint encoding produces smaller payloads for typical workflow data (small integers, short strings) while staying within DSQL's 1 MiB BYTEA limit.

### Implementation reference

The complete schema DDL, migration tooling, connection pool, and codec are specified in `.kiro/specs/dsql-schema-connection/` and implemented in `tokeira-storage/src/dsql/`.

## Suggested internal API shape

The storage API should expose the real contract, not a fake ORM abstraction:

- `load_current_execution`
- `load_hot`
- `commit_transition`
- `start_workflow_task`
- `start_activity_task`
- `renew_shard_lease`
- `find_dispatchable_*`
- `read_projection_substream`
- `advance_projector_checkpoint`

## Debuggability

Storage should make it easy to answer:

- which transition sequence last committed,
- which history batch belongs to which transition,
- which epoch fenced the write,
- which task token started the current WFT/activity,
- which projection substream checkpoint lags.

That means keeping identifiers explicit in rows, not buried in opaque blobs alone.

## Review questions

1. Do we want `dispatch_backlog` as a typed outbox table or keep it intentionally minimal and reconstructible?
2. Should `workflow_hot` include more denormalized counters to reduce reads, or should we keep it aggressively small?
3. Should the first milestone physically separate `core` and `proj` schemas, or begin with a smaller schema count and split later?

## References

[^dsql-migration]: Aurora DSQL migration guide: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html  
[^dsql-quotas]: Aurora DSQL quotas and limits: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html  
[^dsql-pk]: Aurora DSQL primary key guidance: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html  
[^dsql-create-index]: Aurora DSQL asynchronous index creation: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html
