# 042 Durable Actionable State

**Status:** proposed — not yet implemented; spec authors should treat as direction, not decided architecture\
**Basis:** Tokeira `main` at `0f09e974`; Temporal server v1.31.0\
**Related docs:** [000-overview](000-overview.md), [010-history-as-authority](010-history-as-authority.md), [030-runtime-lanes](030-runtime-lanes.md), [040-delivery-broker](040-delivery-broker.md), [050-dsql-storage](050-dsql-storage.md), [070-projection-plane](070-projection-plane.md), [090-failover-and-recovery](090-failover-and-recovery.md)

## Intent

This document proposes one architecture for everything a run's commit leaves to happen later: dispatching workflow and activity tasks, firing timers, starting children and successor runs, delivering signals, cancellations and results, and projecting visibility. Each consequence becomes a row committed in the same transaction as the transition that creates it, found by a level-triggered pass, and removed by one named authority.

If accepted, it revises design principle 4 of [000-overview](000-overview.md), "Delivery is ephemeral-first". Matching stays in memory, but every task's dispatch intent is durable from its commit, and the durable backlog retires. It answers that document's review question 3: delivery state is derived from run state, with a narrow outbox only for what a close still owes. It would change the durable backlog tier of [040-delivery-broker](040-delivery-broker.md), the checkpoint model of [070-projection-plane](070-projection-plane.md) and the sweeper contract of [090-failover-and-recovery](090-failover-and-recovery.md).

Two of its protocols are modelled in `spec/tla`: `40_dispatch_handoff.tla`, the hand-off from a dispatch row to a started task, and `30_bundle_lease.tla`, the shard lease fence on DSQL. See [Validation](#validation).

## The four rules

1. **Durable intent is state, not a queue message.** Every asynchronous consequence that must survive failure has a durable representation, committed in the same transaction as the transition that creates it. A row says what should happen now. It isn't a message waiting to be consumed.
2. **Discovery is level-triggered; notification is acceleration.** Correctness never depends on receiving a notification or advancing a cursor. A consumer asks what actionable work exists now, starting from the head each time. State that only a notification announces is allowed only where another recoverable transition later turns it into discoverable state: `NotificationOnly(x) ⇒ ◇(Discoverable(x) ∨ NotWanted(x))`.
3. **Run commits and consumers never contend on a mutable row.** Consumers read actionable state. Lifecycle changes belong to run transitions, except where this design names another single writer for rows that no later commit writes. On DSQL, two transactions writing one row conflict and the later commit aborts, so this rule keeps consumers off the commit path.
4. **Every actionable representation has one lifecycle authority.** Before code exists, each kind of row names its incarnation, discoverer, executor, remover, retry authority and deduplication identity. The [inventory](#inventory) is that list.

Rule 1's representations come in two kinds:

- **Derived state** can always be rebuilt from the run's committed state.
- **Intent state** starts when a close commits. The close event in history says what the consequence is: the parent to notify, the children to terminate. What only the intent row records is that the consequence is still outstanding. History has no record of delivery, because delivery happens after the close.

So history stays the authority for what happened, and an intent row is the authority only for what is still owed.

```text
history
  ├── run state ────────▶ derived actionable state   (rebuildable; reconciled at acquisition)
  └── close transition ─▶ intent state               (authoritative for what is still owed; never rebuilt)
```

## Why

Six gaps in today's code share one cause: a consequence of a commit that lives only in memory or behind a cursor.

| What can be lost | Today |
|---|---|
| A workflow task whose publish failed, until the shard is next acquired | The lane logs the publish error and carries on (`run_activation_with_cache` in `crates/tokeira-runtime/src/lane.rs`). The sweep runs only at acquisition. |
| Commits whose run key sorts below the projection cursor, so List and Count go stale | The reader pages `projection_log` by `(run_key, transition_seq)` after a cursor (`read_from` in `crates/tokeira-storage/src/dsql/projection_log.rs`). |
| Child starts, terminations and cancellations; external signals and cancellations; a Nexus operation's first attempt | Spawned after the commit by the `DispatchOp` arms of `publish` in `crates/tokeira-runtime/src/publisher.rs`. Nothing rebuilds them. |
| A closed child's result, so the parent waits for ever | Spawned by the lane after the child's close. Failures are only logged. |
| A successor run (continue-as-new, retry or cron), so the chain silently ends | Started in a separate transaction after the close: by the lane for continue-as-new, by `start_retry_successor` in `crates/tokeira-runtime/src/runtime/workflow_task.rs`, and by `start_timeout_retry_successor` and `start_timeout_cron_successor` in `crates/tokeira-runtime/src/timeout.rs`. Errors are only logged. |
| Reset's follow-up: post-reset options, and failing the fork-point workflow task with the reapplied events | Spawned by the lane after the reset. |

Each gap could get a remedy of its own: a reconciliation pass for failed publishes, an ordering protocol for the projection, a rebuild pass for spawned deliveries. They all ask where the durable record of "this still needs to happen" lives. This design answers that once.

## Terms

- **Slot:** what a row is about: a run (its workflow task), a run and an activity, or a run and an operation.
- **Incarnation:** which instance of the slot's work the row describes. A reschedule replaces the incarnation in place. The start transition compares an offer's incarnation with committed state, never with the row.
- **Derived row:** a pure function of its run's committed state, `derive(state)`, written in the same transaction. It can be rebuilt at any time, so a new owner reconciles it at acquisition.
- **Intent row:** written by a close whose consequence the source run never records as delivered. Its content follows from history; the row is the only record that the consequence is still owed. So it is never re-derived, and only its remover deletes it.
- **Execution home:** the bundle that owns every run of a workflow id, `execution_home_bundle(namespace, workflow_id)` in `crates/tokeira-types/src/routing.rs`. The DSQL commit fences by it and writes it as `shard_id` (`do_commit_transition` in `crates/tokeira-storage/src/dsql/run_repository/commit.rs`). Admission and in-memory tracking still use `shard_for(run_key)`.
- **Discoverer:** what finds the row: a queue home, a shard owner or a projector.
- **Executor:** what acts on it: a poller through the start transition, an operation executor or the projector.
- **Remover:** the single authority that deletes or replaces the row.
- **Serving:** a shard whose owner has fenced it, reconciled its derived rows and admits commands: `ShardState::Active` in `crates/tokeira-runtime/src/shard.rs`.

## Inventory

Every consequence a commit leaves behind, built from the code: each `DispatchOp` arm of `publish` in `crates/tokeira-runtime/src/publisher.rs`, each task the lane spawns after a commit, and each scanner. Each entry answers rule 4's six questions.

### Tasks

**Workflow task.** Derived: a new table, `workflow_dispatch`, with one row per run holding the queue, routing class, priority, `scheduled_at` and `logical_seq`.

- *Today:* the in-memory broker. The grace scanner demotes a task unclaimed for its grace window (5 s by default) to `dispatch_backlog` (`crates/tokeira-runtime/src/backlog.rs`), and the sweep rebuilds tasks only at acquisition. A task whose publish failed is lost until then.
- *Discoverer · executor:* the queue home · a poller, through the start transition.
- *Incarnation · dedup identity:* `logical_seq`, which the start transition checks (`apply_workflow_task_started` in `crates/tokeira-kernel/src/kernel.rs`) · `(run, logical_seq)`.
- *Remover · retry authority:* the start transition, or the close or reset that ends the task · the broker offers the task again when an offer's in-flight lease ends.

**Activity task.** Derived: `activity_dispatch`, kept.

- *Today:* `activity_dispatch`, already indexed by queue, deployment, build id and `dispatch_at` (V027). The backlog stores a second copy, and a per-shard pass re-offers due rows (`reconcile_due_activity_dispatches_once` in `crates/tokeira-runtime/src/runtime/activity.rs`).
- *Discoverer · executor:* the queue home · a poller, through the start transition.
- *Incarnation · dedup identity:* attempt and stamp, which the start checks (`start_activity_task_inner`) · `(run, activity, attempt, stamp)`.
- *Remover · retry authority:* the start, cancel or close transition · retry backoff is the row's `dispatch_at`, set by the retry transition.

### Time

**Timers.** Derived: `timer_bucket`, kept.

- *Today:* `timer_bucket` rows for user timers, start delay and backoff, scanned per shard by `fire_at`.
- *Discoverer · executor:* the shard owner, by deadline · the fire transition.
- *Incarnation · dedup identity:* `(run, timer)` · the fire transition rejects a stale row.
- *Remover · retry authority:* the firing or cancelling transition · none needed.

**Timeouts.** Derived in memory; no row.

- *Today:* activity, workflow-task (sticky schedule-to-start included), run, execution and Nexus timeouts are tracked in memory, armed on commit and rebuilt at acquisition (`crates/tokeira-runtime/src/recovery.rs`). The recovery flag (V072, indexed by V073) makes the run discoverable at acquisition.
- *Discoverer · executor:* the shard owner · the timeout transition.
- *Incarnation · dedup identity:* the timed attempt or sequence · the timeout transition rejects a stale deadline.
- *Remover · retry authority:* the transition that ends the timed state · none needed.

### Operations the source records

**Child start.** Derived: a new table, `run_operation`.

- *Today:* spawned by `DispatchOp::StartChildWorkflow`. A transient error becomes a failed start, and a failed confirmation is only logged (`handle_start_child_workflow` in `crates/tokeira-runtime/src/publisher.rs`). The start is lost on a crash.
- *Discoverer · executor:* the parent's shard owner · an operation executor.
- *Incarnation · dedup identity:* the initiated event id · a request id derived from the parent run and the initiated id.
- *Remover · retry authority:* the parent's started or start-failed transition · the executor, in memory. A failure is recorded only where v1.31.0 records one, for an existing workflow or a missing namespace; transient errors are retried (`service/history/transfer_queue_active_task_executor.go:1050-1074 @ v1.31.0`).

**External signal and cancellation.** Derived: `run_operation`.

- *Today:* spawned by `DispatchOp::SignalExternalWorkflow` and `DispatchOp::RequestCancelExternalWorkflow`, and lost on a crash.
- *Discoverer · executor:* the source's shard owner · an operation executor.
- *Incarnation · dedup identity:* the initiated event id · a request id.
- *Remover · retry authority:* the source's signaled, cancel-requested or failed transition · the executor, in memory.

**Nexus operation and cancellation.** Derived: `run_operation`, due at `next_attempt_at`.

- *Today:* the first attempt is spawned by `DispatchOp::ScheduleNexusOperation` or `DispatchOp::CancelNexusOperation`. The scanner retries only operations already backing off, and tracks only those with a timeout (`scan_nexus_timeouts_once` in `crates/tokeira-runtime/src/nexus.rs`). A first attempt is lost on a crash.
- *Discoverer · executor:* the source's shard owner · the Nexus executor.
- *Incarnation · dedup identity:* the scheduled event id and attempt · a request id.
- *Remover · retry authority:* the transition that records the outcome · run state: each attempt is a run transition, as today.

**Completion callbacks.** Derived: `run_operation`, due at `next_attempt_at`.

- *Today:* the first attempt is spawned by `DispatchOp::DispatchCompletionCallback`. A scanner retries callbacks that are backing off, and their tracking is rebuilt at acquisition. The scanner stays until callbacks move.
- *Discoverer · executor:* the source's shard owner · the callback executor.
- *Incarnation · dedup identity:* the callback index and attempt · a request id.
- *Remover · retry authority:* the transition that records the outcome · run state.

### Consequences of a close, in another execution home

**Child resolution to its parent.** Intent: a new table, `run_intent`, with the row on the closed child.

- *Today:* spawned by the lane after the child's close. Failures are only logged, so the resolution is lost on a crash or an error.
- *Discoverer · executor:* the child's shard owner · an operation executor, which follows reset redirects.
- *Incarnation · dedup identity:* the child run and the parent's initiated id · the parent rejects a child it has already resolved.
- *Remover · retry authority:* the executor, after a definitive outcome · the executor, in memory.

**Parent-close actions.** Intent: `run_intent`, with the row on the closed parent.

- *Today:* terminate and cancel are spawned by `DispatchOp::TerminateChild` and `DispatchOp::CancelChild` under the parent close policy, which takes the children out of the parent's state (`apply_parent_close_policy` in `crates/tokeira-kernel/src/kernel.rs`). They are lost on a crash.
- *Discoverer · executor:* the parent's shard owner · an operation executor.
- *Incarnation · dedup identity:* the child run · a request id derived from the parent's close and the child.
- *Remover · retry authority:* the executor, after a definitive outcome · the executor, in memory.

### Consequences of a close or reset, in the same execution home

**Successor run: continue-as-new, retry or cron.** Target: written in the close's own transaction, as v1.31.0 does (`updateWorkflowExecutionTx`, `common/persistence/sql/execution.go:360-439 @ v1.31.0`). Both runs share an execution home, so this needs only admission on the execution home and a commit that writes two runs; no data moves. Until then: intent, with a `run_intent` row on the predecessor.

- *Today:* started in a separate transaction after the close (see [Why](#why)). Errors are only logged, so the successor is lost on a crash or an error.
- *Discoverer · executor:* none once atomic · meanwhile, the predecessor's shard owner and an operation executor.
- *Incarnation · dedup identity:* the successor run id · the request id `continue-as-new:{predecessor}:{successor}`, already in use.
- *Remover · retry authority:* nothing to remove once atomic · meanwhile, the executor, after a definitive outcome.

**Reset follow-up.** Target: written with the reset and the new run in one transaction, as v1.31.0 applies post-reset operations before it persists (`service/history/ndc/workflow_resetter.go:239-247, 414 @ v1.31.0`). It has the same home and the same prerequisites as a successor. Until then: intent, with a `run_intent` row on the base run.

- *Today:* spawned by the lane after the reset: post-reset options, then the fork-point failure with the reapplied events. Lost on a crash or an error.
- *Discoverer · executor, remover · retry authority:* as for a successor.
- *Incarnation · dedup identity:* the reset request id · `post-reset-options:{request}:{index}`, already in use.

### Projection

**Visibility.** Derived: `projection_log`, kept and redefined as an unordered set of unapplied, immutable images. Each row is a full image (`ProjectionRecord` in `crates/tokeira-storage/src/api.rs`).

- *Today:* `projection_log` is read through a key cursor, and every commit reads the run's latest row for its accumulator (`insert_projection_log` in `crates/tokeira-storage/src/dsql/run_repository/commit.rs`). A commit that lands below the cursor is skipped.
- *Discoverer · executor:* a projector per partition · a monotonic upsert (`upsert_execution_row` in `crates/tokeira-projection/src/dsql_store.rs`).
- *Incarnation · dedup identity:* `(authority_epoch, transition_seq)` · the strictly-newer guard.
- *Remover · retry authority:* the projector deletes exactly the images it observed, once each is applied or dominated by an image it applied, and never a range. It is the named second writer, on rows no commit writes again · the projector.

### Memory only, by design

Speculative workflow tasks, queries, eager dispatch and updates before acceptance keep no row. Each has a caller retry or a timeout derived from state.

### Two operation tables, with a firewall

`run_operation` holds derived operations: rows that must correspond to run state, removed by the source's recording transition. `run_intent` holds intent rows: the only record of what a close still owes, removed by their executor. Keeping them apart keeps their invariants apart: the acquisition reconcile touches `run_operation` and never `run_intent`.

Neither table may hold delivery lifecycle: no claim, acknowledgement, executor ownership, attempt status or dead-letter state. Every column is the operation's identity, its payload, or a projection of run state. A Nexus row's `next_attempt_at` is the last kind, because the run records each attempt. The only write an executor ever makes is deleting an intent row it has resolved. A column that breaks this rule is the first step back to a transfer queue.

CHASM components and worker-compute actions already keep durable outboxes of their own: a CHASM root's metadata outbox, rebuilt by the CHASM sweeper, and `worker_compute_action`. They are outside this design; check them against the four rules later.

## Lifecycles

A derived row, the workflow task: created and removed by the run's own transitions, and replaced in place on reschedule.

```text
commit: schedule workflow task 17  ┐ one
        upsert workflow_dispatch   ┘ transaction
              │
notify queue home ── lost? the next
              │      pass finds the row
offer to poller A ┐ in-flight lease,
offer to poller B ┘ duplicates allowed
              │
start: 17 is current?
  yes → mark started, delete row
        (one transaction)
  no  → stale; discard the offer
              │
reschedule: upsert the row with 18
an offer still carrying 17 → stale
```

An intent row, a child's resolution: the child never records delivery, so the executor is the remover.

```text
commit: child closes             ┐ one
        insert run_intent        ┘ transaction
              │
executor (child's shard owner):
  ChildResolved → parent
  (follows reset redirects)
              │
applied, or rejected for good
  (the parent already resolved it,
   or is gone) → executor deletes the row
ambiguous or transient
  → retry; the parent dedupes
```

An intent row is never re-derived, because a closed run's state can't say whether delivery happened. That is why its executor removes it, rather than a history-free transition written only for uniformity. Deleting it is safe after an ambiguous attempt, because the receiver deduplicates the repeat.

## Discovery

```text
discovery pass (queue):
  begin at the durable head: (priority, scheduled_at, slot)
  while the budget remains:
    fetch a page
    skip rows already held or in flight locally
    admit eligible rows not yet held
    continue past held or unservable rows
  stop at the budget

next pass: the head again
never:     WHERE key > durable_cursor
```

- **Continuation lives inside one pass.** A row committed late with a better key is at the head on the next pass. One with a worse key isn't due yet.
- **Paging past held rows** stops a head full of in-flight or version-pinned rows from hiding servable work. v1.31.0 avoids the same problem with a physical queue per version within each partition (`service/matching/physical_task_queue_key.go:24-42 @ v1.31.0`). `activity_dispatch` is already indexed by deployment and build id (V027), and `workflow_dispatch` should be too. Measurement decides whether routing-class index ranges replace paging.
- **Notification** after the commit is the fast path. Losing one costs at most one pass period.
- **Sticky queues are notification-only.** They are per worker, so a pass per sticky queue would cost too much. The lane arms the sticky schedule-to-start deadline on commit (`run_activation_with_cache` in `crates/tokeira-runtime/src/lane.rs`), and recovery rebuilds it. When it fires, the kernel moves the task to the normal queue (`apply_workflow_task_timed_out` in `crates/tokeira-kernel/src/kernel.rs`), where passes find it. This is the only notification-only state. Rule 2 allows it because a recoverable transition converts it.
- **One home per queue.** A Tokeira queue maps to exactly one partition by hash (`route_task_queue` in `crates/tokeira-edge/src/routing.rs`). A queue home discovers rows from every shard for its queues, so publishing to the right node becomes a notification rather than a correctness step.
- **Queue-home ownership is for efficiency; it isn't a correctness fence.** While a queue moves between homes, both may discover and offer the same incarnation. The run's fenced start transition lets only one start commit, so no queue-home fencing is needed.
- **Operations and timers** are found by their source's shard owner, through an index by shard and due time.
- **The projector** runs the same passes over its partition. It deletes exactly the images it observed, once each is applied or dominated. A late image it never saw survives to the next pass, because every delete names a key the pass read. Deleting only successfully applied images would leak: an image dominated by a newer one is never applied, so it would never go.

## Hand-off

- **Claiming writes nothing.** The broker gives an offer to a poller under an in-flight lease held in memory. An offer that is lost is offered again when the lease ends.
- **The start transition is the only fence.** It checks the offer's incarnation against committed state, marks the task started and deletes the row, in one transaction. An incarnation may get any number of offers; at most one start commits.
- **A start can commit while its reply is lost.** Then only the start-to-close timeout recovers the task, so the model includes that timeout.
- **Ownership is unchanged.** Every run-mutating transaction stays fenced by its shard's lease epoch.

## Ownership and serving

```text
acquire lease ─▶ fenced ─▶ reconcile derived rows ─▶ serving ─▶ admit commands and discovery
                           insert what is missing,
                           delete what is stale
```

- **Invariants while serving, for derived rows:** `WantedRowsExist(shard)`, so no row the state wants is missing, and `RowsAreWanted(shard)`, so no row is stale. Both may be false during acquisition or migration, and both must hold before a shard serves.
- **The two failures differ.** A missing row can stop work for ever, so `WantedRowsExist` is what liveness rests on. A stale dispatch row costs only an offer that the start transition rejects. A stale operation row is not harmless: it could start a child or send a signal the run no longer wants. So every operation executor re-checks the source's committed state before it acts, as v1.31.0's transfer executors do. That leaves the same narrow race v1.31.0 accepts, where the source changes between the check and the act.
- **Tokeira already has this shape.** A shard stays `Sweeping`, refusing commands, until its sweep succeeds (`ShardState` in `crates/tokeira-runtime/src/shard.rs`). The sweep gains the reconcile step.
- **Intent rows are never reconciled.** A new owner finds them by scanning its shards' `run_intent` rows.
- **Retention** mustn't delete a run while it has intent rows.

## What retires, what arrives

Retires:

- `dispatch_backlog`, its grace scanner, drain loop and persisted fairness keys (V009, `crates/tokeira-runtime/src/backlog.rs`).
- Ten kinds of delivery made after the commit with nothing to rebuild them: seven `DispatchOp` arms, a child's resolution, successor starts and reset's follow-up.
- The projection cursor and checkpoints, and the per-commit projection lookup.
- `republish_queue`, which has no caller (`crates/tokeira-runtime/src/runtime/mod.rs`).
- The per-shard activity reconciliation pass, absorbed by the queue home's pass.

Arrives:

- `workflow_dispatch`.
- `run_operation` for derived operations and `run_intent` for what a close still owes, each indexed by shard and due time, and neither holding delivery lifecycle.
- Later, a commit that writes a closing run and its successor together, which retires the interim successor and reset intents.
- One discovery loop per queue home, and one executor framework for operations.
- The reconcile step in the acquisition sweep.

Today, work is delivered or healed by eight separate mechanisms: the broker, grace demotion, the backlog drain, the activity pass, the sweep, the Nexus scanner, the callback scanner and the projection cursor. Fire-and-forget spawns sit on top of them. After this design there is one rule and three loops: queue homes for tasks, shard owners for operations and timers, and projectors for visibility.

## Cost

| Path | Added | Removed |
|---|---|---|
| Workflow task | An upsert in the scheduling commit and a delete in the starting commit, inside existing transactions | Demotion inserts and drain reads for tasks unclaimed after their grace window |
| Activity task | Nothing | The backlog's second copy of the task and its input |
| Operation | An insert in the source's commit. A delete in the recording transition (derived), or a small transaction (intent). | Nothing: today's delivery isn't cheaper, only unreliable |
| Visibility | A delete per applied image, batched by the projector | The per-commit lookup and the checkpoint writes |
| Discovery | A page per active queue per pass, and a page per shard for operations | The per-shard activity pass, and whole-namespace republish listings |

The per-commit lookup goes because run state carries the accumulator it reads. That change needs nothing else from this design and can land first.

Two choices need measuring on live DSQL. `workflow_dispatch` can be a narrow table, or columns of `workflow_hot` written in the same upsert as `recovery_needed`. The columns add no statements, but every schedule and start changes the hot row's index entry, and each discovery page reads whole hot rows. The pass period, page size and budget trade discovery latency against reads.

Expect a small net cost in steady state: about one more statement for each workflow-task schedule and start, against the lookups, scans and backlog writes that go. The gain comes when work backs up: an idle queue has nothing to spill, because the row is already the backlog. This design is for correctness and scale-out, not throughput.

## What this takes from Temporal, and what it doesn't

Behavioural parity with v1.31.0 stays the goal wherever users can observe it: start fences, which failures are recorded, retry rules and timeouts. The mechanism is deliberately different.

| Concern | Temporal v1.31.0 | This design |
|---|---|---|
| Where intent lives | Immutable task rows in per-category queues (transfer, timer, visibility), each with a task id | Mutable rows holding the current desired state, at most one per slot |
| How work is found | Queue readers load task-id ranges and advance ack levels (`service/history/queues/queue_base.go`, `reader.go @ v1.31.0`) | Level-triggered passes from the head, with no durable cursor anywhere |
| Stale work | A task is loaded, checked against mutable state, then dropped | The row disappears when the need does |
| Waiting tasks | Matching persists them again in its own task tables (`service/matching/task_writer.go @ v1.31.0`) | The row is the backlog |
| Retries | Executables are rescheduled, and a dead-letter queue takes those that keep failing (`service/history/queues/executable.go`, `dlq_writer.go @ v1.31.0`) | Executor memory, or attempts the run records |
| Start fence | Scheduled event id, stamp and request id (`service/history/api/recordworkflowtaskstarted/api.go:69-111 @ v1.31.0`) | The same identities: `logical_seq`, attempt and stamp |
| Successor runs | The same transaction, since every run of a workflow id shares a shard (`common/persistence/sql/execution.go:360-439 @ v1.31.0`) | Every run of a workflow id already shares an execution home. The same transaction follows once admission uses that home and a commit can write two runs. Until then, an intent row and an idempotent start. |

The closer analogy is a level-triggered controller: an event prompts a look, and a periodic resync makes sure that a missed event costs latency, never correctness.

## Validation

**Property tests**

- Derived rows equal `derive(state)` after every transition, on both stores.
- Executors are idempotent under duplicate and ambiguous delivery.
- The projector converges whatever order it applies images in.

**TLA+ (`spec/tla`)**

- `40_dispatch_handoff.tla` models the hand-off. Tasks are scheduled, started, timed out and closed; notifications are delivered or lost; discovery passes read from the head with a budget; offers are made, lost and expire; brokers restart; owners change and reconcile; queue homes move and overlap; a node of the previous release commits without rows. Its invariants are `AtMostOneStartPerIncarnation`, `NoStaleIncarnationStart`, `NoStartWithoutIntent`, and, while serving, `WantedRowsExist` and `RowsAreWanted`. Its liveness properties are `EventuallyResolved`, under which an incarnation that stays actionable and serviceable is eventually started or no longer wanted, and `StickyConverts`, the notification-only obligation for sticky queues. TLC and tla-rs agree on all four configurations, six negative controls each fail on the property they target, and weak fairness suffices, with none on notifications.
- `30_bundle_lease.tla` encodes DSQL's documented conflict rules for the shard lease. A `FOR KEY SHARE` fence keeps stale owners from committing through creation, deletion, failed sweeps and failed takeovers, and renewals never abort commits; today's fences fail it. An owner whose lease has expired must stop starting commits, because a commit that lands first aborts a concurrent takeover.
- Still to model: intent rows, with `EventuallyRemoved` under executor fairness, and at most one effect given receiver deduplication.

**Engine tests**

- Fail one publish: the task is offered within one pass period, without a restart.
- Crash between a close and its consequence: the consequence is still delivered.

**Live DSQL**

- Statement costs, ordered index scans for head pages, `ON CONFLICT` on the backlog while it lasts, and the `FOR KEY SHARE` conflict rules the lease model assumes.

## Migration

- **Derived rows** converge through the reconcile at acquisition, so a store written by an older release, or a rollback, is repaired before its shards serve.
- **Intent rows** are written only by new close and reset transitions. Runs closed by an older release keep today's behaviour. A one-off repair can find their broken chains: continue-as-new runs without a successor, and parents waiting on closed children.
- **The backlog** drains and retires once no node of an older release can run. Until then its writes must be idempotent: storing an entry whose identity is already held must not fail the batch.
- **The projector** switches from the cursor to passes that apply and delete. Checkpoints go afterwards.
- **Mixed versions:** a node of an older release commits without writing these rows, just as a 0.5.1 node rewrites `workflow_hot` rows without updating the recovery flag. Either upgrade with every node stopped, or reconcile once the upgrade completes.

## Dependencies

- Projection passes that delete applied images need the accumulator in run state first. That change can land on its own.
- Operation executors reuse the discovery and executor framework that `workflow_dispatch` introduces.
- Atomic successors and reset follow-ups need admission on the execution home and a commit that writes two runs. Until both exist, those kinds use intent rows.
- Retiring the backlog needs every node on a release that writes `workflow_dispatch`.

## Review questions

1. Should `workflow_dispatch` be a narrow table, or columns of `workflow_hot` (see [Cost](#cost))?
2. Does any operation kind need columns the others don't, and so a table of its own? The firewall applies either way.
3. Homing by workflow id puts a long continue-as-new chain on one home. What do chain-heavy workloads cost there, before atomic successors land?
4. Should Nexus operations and callbacks move into `run_operation` with the rest, or close their gaps in place first?
5. Can materialization apply reset's follow-up in the new run's first transition?
6. What pass period, page size and budget, and should routing-class index ranges replace paging?
7. How should retention wait for a run's intent rows?
