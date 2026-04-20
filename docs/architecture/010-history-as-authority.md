# 010 History as Authority

**Status:** accepted — resolved questions recorded in [005-decisions-and-boundaries](005-decisions-and-boundaries.md)  
**Related docs:** [000-overview](000-overview.md), [020-kernel](020-kernel.md), [040-delivery-broker](040-delivery-broker.md), [050-dsql-storage](050-dsql-storage.md)

## The problem this document solves

Temporal’s current architecture gives durable workflow state to the **History** subsystem and task delivery to the **Matching** subsystem. The History docs describe the core state transition path as: determine new history events, append them, update mutable state, and enqueue transfer or timer tasks that later result in Matching queue operations.[^history-service] The Matching docs then describe Task Queues that hold work for many Workflow Executions and are polled by workers.[^matching]

That arrangement works, but it forces the server to maintain **consistency between workflow state and a second durable delivery subsystem**. Tokeira’s first structural simplification is to remove that second authority.

## The architectural claim

The authoritative record of workflow progress in Tokeira is:

> **the committed per-run transition that appends history and updates the run summary under a fencing epoch.**

Everything else is derived:

- workflow task dispatch,
- activity task dispatch,
- timers becoming runnable,
- visibility mutations,
- archival events,
- custom projections,
- background catch-up work.

This is what “history as authority” means in practice.

## Be precise: history-only replay is not the design

“History as authority” does **not** mean “recompute everything from the full event log on every request.”

Temporal’s event history is semantically sufficient to explain workflow execution, and the docs explicitly tie durable execution and crash recovery to event history.[^event-history] But Temporal’s History Service architecture docs also explain why a persisted mutable summary exists: recomputing the full workflow state from history alone on every request would be too slow, so current state summaries are persisted and cached.[^history-service]

Tokeira keeps that operational truth:

- **history** is the semantic record,
- **`workflow_hot`** is the persisted current summary of a history prefix,
- **activity/timer rows** are normalized side state derived from the same committed prefix,
- **dispatch and projection records** are downstream effects derived from the same commit.

The invariant is therefore stronger than “history only”:

> **No state visible to the rest of the system may exist unless it can be explained by a committed history transition and its fenced summary update.**

## Why per-run authority is enough

Temporal’s own docs make two key points:

1. A single Task Queue is responsible for delivering tasks for many Workflow Executions.[^matching]
2. On partitioned Task Queues, tasks are assigned to random partitions, and backlog causes sync-match rate to collapse toward zero because async backlog dispatch takes precedence.[^task-queue]

Those facts make it clear that queue ordering is not the fundamental correctness boundary. A global message order would be stronger than Temporal’s public contract and much more expensive than necessary.

The right authority is instead:

- **total order within one workflow run**,
- **explicit causal edges across runs or activities**,
- **prefix-consistent projections of committed transitions**.

Lamport’s “happened-before” framing is useful here: preserve causality where it matters, and only impose stronger order where semantics require it.[^lamport]

## Authoritative state tuple

For an open run, the authoritative state in storage is conceptually:

```text
RunAuthority(run) =
  (
    workflow_hot(run),
    history_batches(run),
    activity_state(run, *),
    timer_rows(run, *),
    request_dedupe(run, *)
  )
```

where:

- `workflow_hot` contains the latest transition sequence, last event ID, status, wait bits, sticky hints, and small summary fields,
- `history_batches` are immutable append-only event batches,
- `activity_state` holds the normalized current state of each open activity,
- `timer_rows` hold outstanding due-time obligations,
- `request_dedupe` prevents duplicate state transitions on retry.

The crucial point is that all of these are updated in **one fenced transaction per transition**.

## The core invariant

For every run `r`, let `seq(r)` be the committed transition sequence number.

Tokeira maintains the following invariant:

1. `seq(r)` increases strictly monotonically.
2. Every committed transition `seq = n + 1` appends a contiguous range of history events after `last_event_id` at `seq = n`.
3. Every open activity, timer, and pending task recorded after `seq = n + 1` is derivable from the history prefix through `seq = n + 1`.
4. Every dispatch or projection effect associated with `seq = n + 1` is emitted in the same transaction or reconstructible from that transaction’s durable outcome.
5. A stale owner, identified by an old shard epoch, cannot commit `seq = n + 1`.

If this invariant holds, then crash recovery reduces to “load the latest durable prefix and resume.”

## Command -> transition -> commit

The state machine boundary is:

```text
LoadedRun + Command
  -> Transition {
       history_events,
       hot_patch,
       activity_ops,
       timer_ops,
       dispatch_ops,
       projection_ops
     }
  -> fenced DSQL commit
```

This is a better boundary than “write queue row + later make history consistent” because it makes the unit of correctness identical to the unit of persistence.

## Transition anatomy

A transition typically does all of the following atomically:

1. append a batch of history events,
2. update `workflow_hot`,
3. upsert/delete affected activity rows,
4. upsert/delete affected timer rows,
5. record request dedupe,
6. emit dispatch intents,
7. emit projection mutations.

Example: a worker completes a workflow task and asks to schedule an activity.

In Tokeira, that single transition can produce:

- `WorkflowTaskCompleted`
- `ActivityTaskScheduled`
- updated `workflow_hot` with no outstanding WFT,
- `activity_state` for the new activity,
- a derived `DispatchActivityTask` effect,
- optional visibility changes.

There is no second durable handoff that must later “make it true.”

## What becomes non-authoritative

The following become explicitly non-authoritative:

### Worker waiters

Long polls are in-memory reservations. If the process dies, no workflow correctness changes.

### Live-ready delivery queues

Fresh tasks may sit in memory briefly before being persisted to backlog. If that memory disappears, the sweeper rebuilds them from run state.

### Sticky routing hints

Sticky identity is a performance hint attached to authoritative run state, not a durable queue authority.

### Visibility rows

Visibility is a projection. If it lags or must be rebuilt, the workflow state remains authoritative.

## Why this is better than a history/matching split

### Smaller correctness surface

The number of subsystems that can “be wrong” in a semantically visible way drops sharply.

### Easier formal reasoning

Per-run single-writer transitions are easier to model than cross-service queue consistency. This is particularly important if we want a TLA+ model later.

### Easier rebuild

If dispatch backlog disappears, rebuild it from `workflow_hot` and `activity_state`.  
If visibility disappears, rebuild it from projection logs or even from history plus summary state.

### Better fit for Aurora DSQL

Aurora DSQL wants narrow transactions, distributed write heat, and retryable conflicts, not multi-stage correctness pipelines.[^dsql-migration][^dsql-pk]

## Ordering policy

Tokeira should preserve **three** kinds of ordering:

### 1. Total order within a run

Enforced by:

- shard ownership,
- lane routing,
- single active actor per run,
- transition sequence / last-event-ID checks.

### 2. Causal order across related entities

Examples:

- parent workflow schedules child,
- activity completes and awakens run,
- signal arrives before the workflow task that processes it.

These are modeled as history edges and token validation, not as one global message stream.

### 3. Prefix order in projections

A projection sink may lag, but it should only observe a prefix of committed transitions in each substream.

## Authority and dedupe

Duplicate suppression happens at two levels:

- **request layer**: request IDs / update IDs / signal IDs,
- **task layer**: task tokens with run key, logical task sequence, attempt, started event ID, and owner epoch.

This is important because optimistic retry is expected under Aurora DSQL’s concurrency model.[^dsql-migration]

## What to persist in the same transaction

The following should be transactionally tied to the authoritative transition:

- history append,
- hot state patch,
- activity/timer normalization,
- request dedupe,
- minimal dispatch/projection intent records.

The following should **not** be required in the same transaction:

- worker waiter registration,
- poller-to-task assignment in memory,
- SQL visibility row materialization,
- custom sink application.

## Relationship to Temporal semantics

This design preserves the user-visible part of Temporal because the semantic source of truth remains workflow event history and run state, not queue order. Temporal’s own Event History docs frame history as the thing that lets execution recover after failures.[^event-history]

The main internal change is simply this:

- today: History must later make Matching and visibility consistent,
- in Tokeira: matching and visibility consume the committed transition.

## Review questions

1. Should dispatch intents themselves be stored in a typed outbox table inside the authoritative transaction, or should some be reconstructible from `workflow_hot` only?
2. For activities and timers, is the normalized side-state table enough, or do we also want an explicit “effect log” for debugging?
3. Do we want to reserve the phrase “history as authority” for the *transition* rather than for raw event blobs, to avoid misreading this as history-only replay?

## References

[^history-service]: Temporal History Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/history-service.md  
[^matching]: Temporal Matching Service architecture doc: https://github.com/temporalio/temporal/blob/main/docs/architecture/matching-service.md  
[^task-queue]: Temporal Task Queues docs: https://docs.temporal.io/task-queue  
[^event-history]: Temporal Event History docs: https://docs.temporal.io/workflow-execution/event  
[^dsql-migration]: Aurora DSQL migration guide: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html  
[^dsql-pk]: Aurora DSQL primary key guidance: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html  
[^lamport]: Lamport, *Time, Clocks, and the Ordering of Events in a Distributed System* (1978): https://lamport.azurewebsites.net/pubs/time-clocks.pdf
