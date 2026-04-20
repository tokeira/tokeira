# 040 Delivery Broker

**Status:** accepted — resolved questions recorded in [005-decisions-and-boundaries](005-decisions-and-boundaries.md)  
**Related docs:** [010-history-as-authority](010-history-as-authority.md), [030-runtime-lanes](030-runtime-lanes.md), [055-admission-control](055-admission-control.md), [060-connection-management](060-connection-management.md)

## Purpose

`tokeira-runtime` needs a subsystem that handles worker polling, sync matching, sticky routing, and durable backlog **without becoming a second source of truth**. That subsystem is the **delivery broker**.

Its job is to answer:

> **Given currently pending work and currently waiting pollers, what should be delivered now, what should stay live in memory, and what must be persisted to backlog?**

## The architectural claim

Tokeira should treat task delivery as **reservation-based** and **ephemeral-first**.

The durable fact is that a run has a pending workflow task or an activity has a pending attempt. The durable fact is **not** that a queue row exists.

This is a major departure from service structure, but not from semantics. Temporal’s docs already distinguish task queue behavior from workflow history semantics, and they document sync match rate, poll success rate, sticky execution, backlog behavior on partitioned queues, and fairness/priority as delivery concerns.[^task-queue][^worker-health][^worker-performance][^sticky-execution][^fairness]

## Queue family identity

The broker should key waiters and ready tasks by more than queue name:

```text
QueueFamilyKey =
  (
    namespace,
    task_queue_name,
    task_kind,
    deployment/build compatibility,
    optional sticky target
  )
```

This matters because:

- workflow tasks and activity tasks have different performance/capacity behavior,
- worker versioning/build compatibility can change who is allowed to receive a task,
- sticky workflow tasks target one worker identity preferentially.

## Three-tier delivery model

Tokeira should have **three delivery tiers**.

### Tier A: inline start

If a compatible poller is already waiting *at the moment a task is created*, the run transition can schedule and start that task in one logical flow. For a workflow task, that means appending `WorkflowTaskStarted` immediately and returning the token and history delta to the poller.

This is the best possible path because it avoids durable backlog entirely.

### Tier B: live-ready memory tier

If no poller is waiting yet, the task should first live in a short-lived in-memory ready structure.

This gives the system a chance to match a near-future poll without incurring a durable backlog insert followed by a delete moments later.

### Tier C: durable backlog

Only if a task survives past the live-ready grace window, or the node is under pressure, or the shard is being unloaded, should it be materialized in `dispatch_backlog`.

## Why this is safe

This is safe because the authoritative pending-task state lives with the run:

- `workflow_hot.pending_wft`,
- `activity_state` for open activities.

If the broker process dies before durable backlog is written, a sweeper reconstructs delivery candidates from authoritative state. That means live-ready is an optimization, not a correctness dependency.

## Reservation-based sync matching

A sync match should work like this:

1. worker long-polls,
2. broker registers an in-memory waiter,
3. broker finds a compatible pending task,
4. broker asks the owning lane/storage path to perform a **start-task transaction**,
5. only if that transaction commits does the worker receive the token.

This matters because `WorkflowTaskStarted` and `ActivityTaskStarted` are part of history semantics. The broker does not get to “hand out” work independently; it only brokers a reservation that becomes real after the authoritative start transaction.

## Sticky-first, not sticky-only

Temporal documents sticky execution as a performance optimization that routes workflow tasks back to the same worker to reuse cached workflow state.[^sticky-execution] Tokeira should preserve that optimization, but not let it become a source of duplicate starts or starvation.

Broker policy should be:

1. sticky exact match if healthy,
2. general live waiter,
3. live-ready,
4. backlog.

Sticky expiration should be a hint, not a permanent claim.

## Fairness belongs to backlog

Temporal’s fairness docs focus on dispatch ordering and make clear that tasks of the same priority are FIFO within that priority band.[^fairness] Tokeira should apply priority and fairness **only** on the durable backlog path.

That keeps the fast path simple:

- fresh sync-matchable work should not pay the cost of backlog fairness machinery,
- fairness should prevent starvation among persisted backlog items,
- live-ready should remain a latency optimization.

## Poller tuning

Temporal’s worker health and worker performance docs point to the key signals:

- `ScheduleToStart` latency,
- sync match rate,
- poll success rate,
- poll timeouts,
- worker task slots available,
- sticky cache metrics,
- backlog count/age.[^worker-health][^worker-performance]

Temporal’s docs also provide useful heuristics:

- sync match rate should typically remain very high,
- poll success rate should usually be >90%, and often >95% for high-volume low-latency systems,
- low poll success plus low schedule-to-start plus low worker utilization usually means too many pollers/workers.[^worker-performance]

Tokeira should encode these signals into a server-side control loop.

## Suggested broker control loop

The broker should maintain weighted service budgets across:

- sticky offers,
- live-ready offers,
- backlog offers.

Example policy:

- low backlog age -> heavily prefer sticky/live,
- moderate backlog age -> balanced live vs backlog,
- high backlog age -> increase backlog share but never starve fresh sync-matchable work.

The explicit goal is to avoid the documented Temporal behavior where backlog can drive sync-match rate near zero on partitioned queues.[^task-queue]

## Workflow tasks vs activity tasks

The broker should tune these separately.

### Workflow tasks

Sensitive to:

- sticky execution,
- history replay cost,
- worker cache health.

### Activity tasks

Sensitive to:

- explicit worker capacity,
- slot-based admission,
- activity execution rate and timeouts.

The same poll API exists for both, but the controller inputs are meaningfully different.

## Long polls should stay out of storage

Long polls are not durable facts. Therefore:

- they should not allocate DSQL connections,
- they should not create durable rows,
- they should be admitted and timed out at the edge/broker layers only.

This is one of the biggest structural simplifications in Tokeira compared to any design that lets queue persistence sit directly on the hot path of every poll.

## Suggested internal API

Conceptually:

```rust
register_waiter(queue_family, waiter)
publish_live_ready(queue_family, task_ref)
try_match(queue_family) -> reservation
start_task(reservation) -> committed token
fallback_to_backlog(task_ref)
```

The key thing is that the broker works with **task references** and **reservations**, not with “authoritative queue rows.”

## Sweeper contract

The sweeper is what makes ephemeral-first delivery safe. After restart or shard failover, it scans authoritative state for:

- pending WFTs,
- dispatchable activity attempts,
- expired sticky claims that should be general-delivery again.

It then republishes to live-ready or backlog. Because the broker never owned correctness, rebuild is straightforward.

## Review questions

1. How long should the live-ready grace window be before durable backlog is written?
2. Do we want separate broker instances for workflow and activity tasks internally, or one broker with distinct policies?
3. Should sticky preference be modeled directly in `workflow_hot`, or partly in an auxiliary runtime cache?

## References

[^task-queue]: Temporal Task Queues docs: https://docs.temporal.io/task-queue  
[^worker-health]: Temporal worker health docs: https://docs.temporal.io/cloud/worker-health  
[^worker-performance]: Temporal worker performance docs: https://docs.temporal.io/develop/worker-performance  
[^sticky-execution]: Temporal Sticky Execution docs: https://docs.temporal.io/sticky-execution  
[^fairness]: Temporal Task Queue Priority and Fairness docs: https://docs.temporal.io/develop/task-queue-priority-fairness
