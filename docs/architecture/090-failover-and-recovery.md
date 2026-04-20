# 090 Failover and Recovery

**Status:** accepted — resolved questions recorded in [005-decisions-and-boundaries](005-decisions-and-boundaries.md)  
**Related docs:** [030-runtime-lanes](030-runtime-lanes.md), [040-delivery-broker](040-delivery-broker.md), [060-connection-management](060-connection-management.md)

## Purpose

Durable execution is only real if the system can fail in messy ways and still recover to a coherent prefix of truth. This document describes how Tokeira should recover from:

- node crashes,
- shard ownership changes,
- stale workers and stale task tokens,
- lost in-memory waiters/live-ready state,
- projection lag or sink loss,
- DSQL connection pressure.

## Recovery model

Tokeira’s recovery story is intentionally built around the same invariant as the rest of the design:

> **Recover from the last committed per-run transition prefix.**

Temporal’s Event History docs emphasize that event history is what allows workflow execution to recover after crashes.[^event-history] Tokeira keeps that principle but adds a persisted hot summary and normalized activity/timer state so recovery is operationally cheap.

## Shard failover

Shard ownership should be fenced by a monotonically increasing epoch.

On normal operation:

- owner renews lease using the control pool,
- every run transition checks the current shard epoch.

On owner failure:

1. lease expires,
2. a new node acquires the shard and increments epoch,
3. stale owners may still have memory state, but they can no longer commit,
4. the new owner runs a sweeper to rebuild volatile delivery state.

This is the core protection against split brain.

## Lane and actor recovery

Actors are intentionally disposable. If a runtime process dies:

- actor memory is lost,
- waiters are lost,
- live-ready entries are lost,
- sticky hints may become stale.

But the authoritative state remains:

- `workflow_hot`,
- `history_batch`,
- `activity_state`,
- `timer_bucket`.

So actor recovery is simple:

1. reload run on demand,
2. continue from last committed transition,
3. let the sweeper republish dispatchable work.

This is why actors should park quickly and avoid holding semantic state only in memory.

## Sweeper responsibilities

The sweeper is the bridge from durable truth back to volatile delivery state.

On shard acquisition or restart, it should:

- find runs with pending workflow tasks,
- find open activity attempts ready to dispatch,
- identify expired or invalid sticky preferences,
- republish to live-ready or durable backlog.

The sweeper is also what makes the delivery broker’s ephemeral-first design safe.

## Timer recovery

Timer scanning itself is not authoritative. The authoritative state is the timer row plus run state.

After failover:

- new owner resumes scanning timer buckets,
- due timers are revalidated against current run state before firing,
- canceled/already-fired timers become harmless no-ops.

This is important because timers are one of the easiest places for duplicate work to appear after a crash.

## Task token safety

A task token should encode enough information to reject stale completions:

- run key,
- logical task sequence,
- started event ID,
- attempt,
- owner epoch.

If a worker completes a task after failover or retry has already invalidated that token, the runtime/storage path should reject it cleanly without mutating state.

This is a major part of making worker failure and failover idempotent.

## Projection recovery

The projection plane is designed to lag safely.

If a projector crashes:

1. restart worker,
2. read from last sink/substream checkpoint,
3. reapply idempotently,
4. continue.

If a sink is lost completely:

- rebuild from retained projection log, or
- rebuild from authoritative state/history when the retained log is insufficient.

Workflow correctness is unaffected throughout.

## Continue-As-New as structural recovery for long histories

Temporal documents Continue-As-New as a way to start a fresh run in the same chain with a new Run ID and fresh history.[^continue-as-new] Tokeira should treat this as both a workflow feature and an operational scaling valve:

- very long-lived entity-style workflows should periodically compact themselves through Continue-As-New,
- recovery then starts from a shorter current-run history,
- chain metadata keeps logical continuity.

This is not a substitute for snapshots, but it is a very useful pressure-release valve.

## Snapshot + suffix recovery

For frequently touched long-lived runs, runtime/storage should eventually support:

- a persisted snapshot ref in `workflow_hot`,
- replay from snapshot + suffix, not from origin.

This idea is strongly validated by Netherite, which uses partitioning, asynchronous snapshots, and recovery logs to improve durable workflow execution efficiency.[^netherite]

## DSQL connection failure and recovery

Aurora DSQL has hard connection-duration, active-connection, and new-connection-rate limits.[^dsql-quotas] That means failover and recovery have to be careful not to become reconnect storms.

Recovery policy should therefore be:

- protect control traffic first,
- shape connection creation with budget tokens,
- allow projections and maintenance to fall behind,
- prefer reload-on-demand over eager mass rehydration of actors.

This is a direct consequence of DSQL’s connection model.

## Projection lag is acceptable; stale ownership is not

It is useful to draw a firm line:

- **projection lag** is tolerable and recoverable,
- **stale shard owner committing** is not.

That informs operational priorities:

1. keep lease renewal and fencing healthy,
2. keep transition commits healthy,
3. recover delivery state,
4. catch up projections later.

## What must survive restart

After restart or ownership transfer, the system must be able to reconstruct:

- which run is current for a workflow ID,
- the latest committed transition sequence,
- the latest event history prefix,
- whether a WFT is pending or started,
- which activity attempts are open,
- which timers are still live,
- which requests have already been deduped.

If any one of those requires in-memory state to be correct, the design is wrong.

## Operational recovery sequence

A good shard-acquisition sequence is:

1. acquire shard lease and epoch,
2. start control tasks (lease renewer, timer scanner, sweeper),
3. rebuild dispatchable work into live-ready/backlog,
4. admit new commands,
5. let demand load actors lazily.

The key is that new owners should not try to rehydrate every actor eagerly.

## Future formal model

Recovery is a good fit for formalization. Interesting invariants include:

- one owner per shard epoch,
- no double start for one logical task sequence,
- every durable pending task is rediscoverable,
- projector checkpoints are prefixes,
- stale completions do not mutate state.

This is a natural place for TLA+ or Stateright work later.

## Review questions

1. Should the sweeper rebuild live-ready first and only later write backlog, or immediately materialize backlog after failover?
2. Do we want snapshot support in the first milestone, or rely on Continue-As-New + hot state only at first?
3. How aggressively should shard acquisition delay new traffic until sweeper progress reaches a threshold?

## References

[^event-history]: Temporal Event History docs: https://docs.temporal.io/workflow-execution/event  
[^continue-as-new]: Temporal Continue-As-New docs: https://docs.temporal.io/workflow-execution/continue-as-new  
[^dsql-quotas]: Aurora DSQL quotas and limits: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html  
[^netherite]: Burckhardt et al., *Netherite: Efficient Execution of Serverless Workflows*: https://www.vldb.org/pvldb/vol15/p1591-burckhardt.pdf
