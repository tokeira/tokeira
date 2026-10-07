# Requirements Document: Durable Workflow-Task Dispatch

## Introduction

A committed workflow task must remain discoverable when its post-commit publication
fails or its broker disappears. Today publication failure is logged, and recovery
reconstructs the task only when its shard is next acquired. This feature makes the
current workflow-task dispatch intent a durable projection of committed run state,
maintained by the same transaction, and continuously discovered by queue homes.

This is a requirements-first architecture change implementing the workflow-task
portion of [042 Durable Actionable State](../../../docs/architecture/042-durable-actionable-state.md).
History and committed run state remain authoritative. Storage owns durable rows;
runtime owns discovery, delivery, and acquisition; the kernel remains pure. The
internal mechanism changes, while observable workflow-task behavior continues to
target Temporal server **v1.31.0**. The vendored API **v1.62.11** supplies wire shape;
its newer fields do not extend the behavioral compatibility claim.

The first spec covers workflow dispatch and retirement of workflow-task use of
`dispatch_backlog`. A subsequent activity-discovery spec will reuse these discovery
interfaces, absorb the activity pass into queue homes, and retire activity backlog
use. That subsequent work depends on this feature; this feature does not depend on it.
Operation rows, close intents, atomic successors, projection discovery, admitted-update
recovery, and redesign of speculative workflow tasks are outside this feature.

## Glossary

- **Slot:** one workflow-task dispatch position, identified by the run key.
- **Incarnation:** one startable delivery generation of that slot. Retrying a task or
  superseding its delivery invalidates earlier offers. This is an internal identity,
  distinct from the worker-visible attempt and history event IDs; `logical_seq`
  alone is not a sufficient mapping in the current implementation.
- **Dispatch row:** the logical `workflow_dispatch` record for a slot. Its physical
  representation may be a narrow table or indexed columns of `workflow_hot`.
- **Derive:** a deterministic function of committed run state yielding either the
  slot's complete dispatch row or no row, independent of broker state and notifications.
- **Wanted:** a running run has a committed, unstarted, non-speculative pending
  workflow task. Paused and closed runs are not dispatchable. Transient retries of
  normal tasks remain in scope even when their schedule/start events are suppressed.
- **Routing class:** the version-routing coordinates needed to find compatible work,
  including deployment and build ID; this is distinct from normal/sticky queue kind.
- **Queue home:** the node serving a task queue's placement partition. It discovers
  work for that queue across execution-home shards, not only shards local to the node.
- **Pass:** one discovery traversal beginning at the durable head for its selected
  queue and routing class. Page continuation exists only within this traversal.
- **Held:** an incarnation retained in the broker's ready set. **In flight:** an offer
  retained under a bounded, memory-only delivery lease while a start is attempted.
- **Serving shard:** an execution-home shard whose acquisition reconciliation and
  timeout reconstruction have succeeded and which admits run commands.
- **Sticky deadline:** the pending task's stored absolute schedule-to-start deadline,
  separate from the run's mutable sticky affinity and broker poller observations.

## Target State

- After every committed run transition, the slot's durable dispatch representation
  equals `Derive(committed_state)`. Scheduling and starting never leave a gap between
  the run's state and its dispatch representation.
- Notifications accelerate delivery; losing one never removes durable intent.
  Normal queues receive periodic passes registered independently by polling.
- Claims, ready-set deduplication, and offer leases remain volatile. A fenced start
  validates the current incarnation against run state, not against a claimed row.
- Sticky rows do not receive periodic per-worker queue scans. Their pending deadlines
  are restored on acquisition, so timeout conversion makes abandoned work discoverable
  on the normal queue. Existing compatible normal fallback remains possible.
- Acquisition repairs both missing and stale rows before serving. Repairs are bounded,
  restartable, fenced storage operations; they author no workflow history.
- Workflow tasks cease using the durable backlog. Activity rows, activity backlog,
  its required loops, and the per-shard activity pass remain available.
- Upgrade with every node stopped.

This feature does not introduce a mixed-release serving protocol, new public RPCs,
poll fields, or user-facing queue tuning options. Mechanical defaults belong to the
runtime under [engineering-reference](../../../docs/agents/engineering-reference.md).

## Evidence From Current Code

Repository evidence below is pinned to **`13b34ac64661efd1cc8745dc4934a1ac0872c01e`**.
Named functions are the durable anchors when subsequent edits move line numbers.

| Evidence | Consequence for this feature |
|---|---|
| `run_activation_with_cache`, [lane.rs](../../../crates/tokeira-runtime/src/lane.rs), around 936: publication errors only log; `sweep_shard`, [recovery.rs](../../../crates/tokeira-runtime/src/recovery.rs), runs at acquisition | Continuous recovery must come from durable intent, independently of publication. |
| `scan_grace_once` and `drain_once`, [backlog.rs](../../../crates/tokeira-runtime/src/backlog.rs) | Workflow removal must preserve the activity branches of shared loops. |
| `apply_workflow_task_started`, [kernel.rs](../../../crates/tokeira-kernel/src/kernel.rs), around 1671, checks sequence and unstarted state; `apply_workflow_task_failed`, around 3530, retains a sequence across an incremented retry attempt | Define a complete incarnation mapping before adopting the model's at-most-one-start property. |
| `dispatchable_workflow_task`, [api.rs](../../../crates/tokeira-storage/src/api.rs), around 1285, derives a delivery envelope; `PendingWorkflowTask`, [state.rs](../../../crates/tokeira-kernel/src/state.rs), stores the deadline | Row derivation must classify task modes explicitly and keep the pending deadline independent of affinity. |
| `recovery_entries`, [recovery_index.rs](../../../crates/tokeira-storage/src/recovery_index.rs), and `sweep_shard` reconstruct started-task timers; cold recovery republishes through the normal fallback | Rebuilding pending sticky deadlines is new required work, not an existing guarantee. |
| `do_list_recovery_candidates_for_shard`, [visibility.rs](../../../crates/tokeira-storage/src/dsql/run_repository/visibility.rs), visits `recovery_needed IS NULL` and `true` | A candidate-only walk cannot find every stale dispatch row of an excluded run. |
| `publish_workflow_task` and the workflow take path, [broker.rs](../../../crates/tokeira-runtime/src/broker.rs), deduplicate by `(run_key, logical_seq)` and remove that key on take | Model-held and in-flight state require an explicit implementation mapping. |
| `reconcile_due_activity_dispatches_once`, [activity.rs](../../../crates/tokeira-runtime/src/runtime/activity.rs), and [V027](../../../crates/tokeira-storage/migrations/V027__idx_activity_dispatch_queue.sql) | Reuse the durable-discovery pattern and version index coordinates; its single bounded head page is not the workflow starvation solution. |
| `route_task_queue`, [routing.rs](../../../crates/tokeira-edge/src/routing.rs), and [routing types](../../../crates/tokeira-types/src/routing.rs) | Queue placement and execution-home ownership are separate responsibilities. |
| [40_dispatch_handoff.tla](../../../spec/tla/40_dispatch_handoff.tla): `Admit` budgets new admissions after known-row exclusion; `Reconcile` is atomic; the environment has bounded faults and at most two runs | The model does not establish a wall-clock latency bound, bounded SQL scanning, or correctness of a multi-transaction repair without a refinement argument. |

### Observable contract authorities

- Wire shape: `PollWorkflowTaskQueueRequest`, `PollWorkflowTaskQueueResponse`, and
  `ResetStickyTaskQueueRequest` in
  [workflowservice request/response protos](../../../proto/upstream/temporal/api/workflowservice/v1/request_response.proto);
  `StickyExecutionAttributes` in
  [taskqueue protos](../../../proto/upstream/temporal/api/taskqueue/v1/message.proto).
- Version-separated physical queues: `service/matching/physical_task_queue_key.go:24-42
  @ v1.31.0`; normal versus sticky deployment polling:
  `service/matching/task_queue_partition_manager.go:519-550 @ v1.31.0`.
- Start validation and a repeated start request:
  `service/history/api/recordworkflowtaskstarted/api.go:69-111 @ v1.31.0`.
  Internal duplicate discovery does not authorize duplicate committed starts or a
  change to the public repeated-request contract.
- Failure attempt handling and event suppression:
  `service/history/workflow/workflow_task_state_machine.go:309-410, 556-576,
  1005-1045 @ v1.31.0`.
- Sticky timeout guards and rescheduling:
  `service/history/timer_queue_active_task_executor.go:439-469 @ v1.31.0` and
  `service/history/workflow/workflow_task_state_machine.go:263-305 @ v1.31.0`.
- Resetting sticky affinity: `service/history/api/resetstickytaskqueue/api.go @ v1.31.0`.
  It clears affinity without scheduling a new task; timeout handling separately
  checks the pending task rather than requiring affinity to remain set.

### Storage authority and verification boundary

[Storage AGENTS](../../../crates/tokeira-storage/AGENTS.md) governs forward-only
migrations and asynchronous indexes. The
[AWS limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)
specify at most 3,000 mutated table rows and 10 MiB of modified data per transaction;
these are not discovery read limits. The approved task supplies prior playground
observations about transaction abortion, `FOR KEY SHARE`, and writable CTEs.
SQLx checks on real DSQL verified correct affected-row counts for
`ON CONFLICT DO NOTHING` and a skipping `DO UPDATE ... WHERE`. `RETURNING`
may be selected for its returned data; it is not required to correct row counts.
Feature-specific execution results belong in the implementation evidence record.
Statement acceptance alone does not establish the concurrency behavior of a repair.

## Field and Lifecycle Policy

This table defines the logical storage contract, not a finalized SQL schema. Physical
columns, serialization, index layout, and precise Rust error types belong in the design.
No upstream protobuf fields are added or reinterpreted.

| Field or contract element | Target policy | Invalid or stale handling | Persistence or side effect |
|---|---|---|---|
| Run key and execution-home shard | One slot per run; shard derived by the same routing rule as the authoritative commit | Identity mismatch fails repair; stale offers fail start validation | Primary identity and acquisition scan scope |
| Queue namespace/name/kind | Derived destination and normal/sticky classification | Never cross a namespace or deliver to an incompatible queue | Queue discovery scope |
| Deployment and build ID | Preserve the distinction between unversioned and version-directed work | Incompatible work is excluded from admission; obsolete offers cannot override current routing | Indexed routing-class selection |
| Incarnation, including `logical_seq` | Identify one startable generation, including retry and supersession; exact encoding is a required design decision | Earlier generations cannot start a later generation | Included in the row, offer, deduplication key, and authoritative start check |
| Priority | Preserve effective priority and fairness metadata from run state | Preserve existing validation and delivery-policy semantics | Durable metadata; no persisted broker fairness counter |
| `scheduled_at` | Derive from the committed pending task; never replace with discovery time | Invalid stored values fail decoding instead of being silently replaced | Stable age and deterministic discovery ordering |
| Sticky worker, normal fallback, pending deadline | Derive delivery information without expiring durable affinity during a read | Timeout validation uses the pending task even after affinity reset | No durable delivery lease; deadline remains in run state |
| Stable order tie-breaker | A unique slot key completes the per-range discovery order | Repeated equal-priority timestamps cannot omit a row | Deterministic keyset pagination |
| Claims, acknowledgements, offer leases, broker ownership | Disposable memory only | Restart or expiry permits rediscovery | No dispatch-row writes |
| Pass continuation | Valid only within its originating pass | Discard on pass completion, cancellation, or restart | Never a durable cursor |

| Committed state or delivery path | Dispatch representation |
|---|---|
| Running, pending, unstarted normal task, including transient retries | Exactly `Derive(state)` |
| Wanted task with sticky delivery | Row remains durable; per-worker sticky passes are excluded; recoverable deadline provides fallback |
| Started task, no pending task, paused run, or closed run | No actionable dispatch row |
| Priority/version redispatch, retry, resume, or reset materialization | Re-evaluate the complete row from the resulting committed state |
| Speculative task or query | No new durable dispatch row solely for this volatile work; existing conversion and retry contracts remain owned by their specs |
| Eager or reserved direct delivery | A committed unstarted normal task still has a row; a committed started task does not; delivery mode adds no independent row |

## Requirements

### Requirement 1: Atomic derived dispatch

**User Story:** As a workflow operator, I want pending work to survive publication
failure, so that delivery never depends on a successful post-commit notification.

#### Acceptance Criteria

1. THE storage layer SHALL implement one pure `Derive` contract shared by both backends.
2. WHEN a run transition commits, THE transaction SHALL leave the slot equal to `Derive(committed_state)`.
3. WHEN a run is materialized by reset, THE materialization transaction SHALL establish the same derived-row invariant.
4. IF a transition is rejected, duplicated without mutation, or rolled back, THEN THE storage layer SHALL preserve its prior dispatch representation.
5. WHEN priority, routing, retry, pause, resume, close, or reset changes committed task eligibility or content, THE storage layer SHALL apply the corresponding row replacement or removal atomically with that state change.
6. THE commit path SHALL derive dispatch without reading prior dispatch rows for lifecycle authority.
7. THE kernel SHALL remain free of storage and broker operations.

### Requirement 2: Incarnation and the start fence

**User Story:** As a runtime developer, I want delayed offers to remain distinguishable
from newer attempts, so that rediscovery cannot start the wrong task generation.

#### Acceptance Criteria

1. THE design SHALL define a durable incarnation identity that distinguishes every new startable generation, including failures retaining today's `logical_seq`.
2. WHEN a retry or supersession creates a new startable generation, THE transition SHALL invalidate offers for every previous generation of the slot.
3. WHEN an offer is started, THE authoritative transition SHALL validate its incarnation against the pending run state.
4. IF the offer is stale or the pending task has already started, THEN THE transition SHALL reject a new start without mutating run state.
5. WHEN a start commits, THE same transaction SHALL remove its dispatch row.
6. THE broker SHALL deduplicate held and in-flight offers using the same incarnation contract as the start fence.
7. THE implementation SHALL preserve Temporal v1.31.0's observable attempt, event-suppression, and repeated-request behavior independently of internal incarnation encoding.

### Requirement 3: Discovery without a durable cursor

**User Story:** As a worker operator, I want compatible tasks discovered after missed
notifications, so that restarting a shard is unnecessary for normal delivery.

#### Acceptance Criteria

1. WHEN a pass begins, THE queue home SHALL start at the durable head of each selected queue and routing-class range.
2. THE pass SHALL use a deterministic total order completed by a unique slot tie-breaker.
3. WHEN a page ends before traversal completion, THE pass SHALL continue strictly beyond that page using only pass-local keyset state.
4. WHEN a pass ends or is interrupted, THE queue home SHALL discard its continuation.
5. THE queue home SHALL discover its queues' rows across all execution-home shards.
6. WHEN a row is already held or in flight locally, THE pass SHALL continue past it without admitting another local copy.
7. THE discovery path SHALL leave dispatch rows unchanged.
8. WHEN a notification arrives, THE runtime SHALL use it only as an acceleration of delivery governed by the same incarnation checks.

### Requirement 4: Bounded resources with progress

**User Story:** As an operator, I want discovery to use bounded resources without
trapping eligible work behind a repeatedly scanned prefix.

#### Acceptance Criteria

1. THE design SHALL define separate bounds for new admissions, retained broker state, and scan work.
2. THE design SHALL specify how routing-class selection or traversal reaches compatible work behind an unservable prefix.
3. IF a held or unservable prefix exceeds the page size and admission budget, THEN THE discovery protocol SHALL still reach a later eligible row under the stated progress assumptions.
4. IF a scan-work limit ends a pass, THEN THE discovery protocol SHALL preserve eventual progress despite restarting its next pass at the head.
5. WHEN broker capacity is exhausted, THE runtime SHALL retain undispatched intent durably for a later pass.
6. THE design SHALL give positive mechanical defaults and reasons for the pass period, page size, and each budget.
7. THE design SHALL state its fairness assumptions, including eventual storage availability, compatible polling, recurring admission capacity, and the absence of an indefinitely replenished higher-priority prefix.

### Requirement 5: Poll-driven registration and home changes

**User Story:** As a worker, I want polling to activate discovery even when every
publication was lost, so that my queue does not remain invisible to the runtime.

#### Acceptance Criteria

1. WHEN a compatible normal-queue poll is admitted, THE queue home SHALL register its queue and routing class for discovery independently of publication.
2. WHILE compatible poll demand remains active, THE runtime SHALL keep discovery for that registration scheduled.
3. WHEN broker state is recreated, THE next admitted poll SHALL recreate the required discovery registration.
4. THE design SHALL define registration retirement and memory bounds without stranding continuing or renewed poll demand.
5. WHEN queue homes overlap during a placement change, THE runtime SHALL tolerate duplicate offers through the authoritative start fence.
6. WHEN an old queue home retires, THE runtime SHALL discard its volatile discovery state without deleting durable dispatch intent.

### Requirement 6: Memory-only offer lifecycle

**User Story:** As a storage maintainer, I want delivery claims off the write path,
so that polling does not contend with run commits on mutable dispatch rows.

#### Acceptance Criteria

1. WHEN an offer is claimed, THE broker SHALL record its delivery lease only in memory.
2. WHEN an unconfirmed offer lease expires, THE broker SHALL make its incarnation eligible for rediscovery.
3. WHEN a broker restarts, THE runtime SHALL recover normal dispatch through registered passes without requiring a successful notification.
4. WHEN a start commits but its reply is lost, THE runtime SHALL recover through the task's start-to-close timeout rather than recreating the consumed incarnation.
5. THE queue consumer SHALL perform no durable claim, acknowledgement, retry-counter, or delivery-ownership update.
6. THE design SHALL map ready, in-flight, expiry, cancellation, and start-outcome handling to the hand-off model's actions.

### Requirement 7: Recoverable sticky dispatch

**User Story:** As a workflow operator, I want abandoned sticky work to reach the
normal queue even across a restart, so that affinity cannot strand execution.

#### Acceptance Criteria

1. THE queue home SHALL exclude per-worker sticky queues from periodic discovery.
2. WHEN a pending non-speculative task has a sticky deadline, THE runtime SHALL arm that absolute deadline from committed state.
3. WHEN acquisition reconstructs a pending sticky deadline, THE runtime SHALL restore it before the shard begins serving.
4. IF a restored deadline is already overdue, THEN THE runtime SHALL make its timeout eligible at the first timeout-processing opportunity after serving begins.
5. WHEN `ResetStickyTaskQueue` clears affinity, THE runtime SHALL preserve the pending task's existing deadline.
6. WHEN a valid schedule-to-start timeout commits for an unstarted sticky task in a running run, THE resulting transition SHALL make its replacement discoverable on the normal queue.
7. IF a timeout targets an already-started or superseded task, THEN THE transition SHALL leave the newer task unchanged.
8. THE delivery path SHALL preserve compatible normal fallback without using a broker read to mutate durable sticky affinity.

### Requirement 8: Complete fenced acquisition

The initial implementation supports a single execution-home owner. Requirements
8.4–8.6 remain deferred until the transaction-local lease fence is implemented and
verified. Single-owner scope includes complete reconciliation, cancellation,
failure handling, and the non-serving gate; it does not discharge competing-owner
correctness or the corresponding Property 9 checks.

**User Story:** As a shard owner, I want a complete repaired dispatch view before
serving, so that old storage, partial repair, and ownership changes cannot hide work.

#### Acceptance Criteria

1. WHEN a shard is acquired, THE runtime SHALL keep it non-serving until dispatch reconciliation and required timeout reconstruction succeed.
2. THE reconciliation SHALL inspect enough authoritative states to insert every missing wanted dispatch row.
3. THE reconciliation SHALL inspect enough existing dispatch rows to remove every unwanted row, including rows whose run is absent or excluded by the recovery index.
4. WHEN a repair writes a row, THE storage transaction SHALL validate the execution-home ownership fence protecting that repair.
5. IF ownership changes during repair, THEN THE storage layer SHALL prevent stale repair writes from overwriting the successor owner's dispatch state.
6. WHEN ownership is lost, THE runtime SHALL abandon that acquisition without marking the shard serving.
7. THE repair protocol SHALL be restartable after any committed batch without depending on a durable continuation.
8. THE repair transactions SHALL remain within DSQL's aggregate row and modified-data limits.
9. WHEN a repair query, decode, or write fails, THE runtime SHALL leave the shard non-serving for retry or acquisition failure handling.
10. THE repair path SHALL change neither workflow history nor authoritative run state merely to reconcile derived dispatch.
11. WHEN acquisition completes, THE shard SHALL satisfy both `WantedRowsExist` and `RowsAreWanted`.

### Requirement 9: Retire only workflow backlog use

**User Story:** As an operator upgrading a stopped cluster, I want workflow dispatch
rebuilt from committed state without retaining a second workflow delivery lifecycle.

#### Acceptance Criteria

1. WHEN the new release schedules or retains a workflow task, THE runtime SHALL perform no workflow-task write to `dispatch_backlog`.
2. THE new release SHALL stop workflow-task grace demotion and regular workflow backlog draining.
3. WHEN upgrading stopped nodes, THE acquisition protocol SHALL rebuild workflow dispatch from committed run state.
4. THE migration design SHALL specify an idempotent bounded disposal or drain procedure for legacy workflow backlog entries after authoritative reconstruction covers their runs.
5. THE legacy workflow cleanup SHALL preserve activity backlog entries.
6. THE implementation SHALL retain activity dispatch, activity backlog handling, and the per-shard activity reconciliation pass.
7. THE upgrade note SHALL contain the line "Upgrade with every node stopped."

### Requirement 10: Storage and scope preservation

**User Story:** As a maintainer, I want this dispatch change to compose with existing
state, routing, and delivery contracts without expanding into unrelated mechanisms.

#### Acceptance Criteria

1. THE design SHALL decide between a narrow table and indexed hot-state columns with explicit statement-cost, row-width, and lifecycle reasons.
2. THE design SHALL specify the queue and acquisition indexes, including deployment/build selection, deterministic pagination, and reasons for using or rejecting partial indexes.
3. THE implementation SHALL follow the storage migration contract without modifying existing migration bytes.
4. THE implementation SHALL preserve the projection accumulator's atomic commit behavior and frozen state extensions.
5. THE implementation SHALL preserve existing workflow priority, fairness metadata, version-routing, query, and eager-delivery contracts at their public boundaries.
6. THE implementation SHALL leave speculative scheduling and admitted-update recovery mechanisms to their owning specs.
7. THE discovery interfaces SHALL permit later activity integration without removing today's activity recovery path.
8. THE design SHALL record the transaction-local lease-fencing dependency and retain competing-owner verification as deferred until that fence is implemented and verified.

### Requirement 11: Recovery latency and progress

**User Story:** As an operator, I want a testable recovery guarantee and honest
progress assumptions, so that a missed notification has an understood consequence.

#### Acceptance Criteria

1. WHEN one publication fails for an eligible normal task with a registered compatible poller, healthy storage, a serving shard, and sufficient scan and admission capacity, THE runtime SHALL offer the task by completion of the next pass without restarting.
2. WHEN faults cease and the assumptions of Requirement 4.7 hold for a continuously wanted serviceable incarnation, THE runtime SHALL eventually start that incarnation or observe that it is no longer wanted.
3. THE next-pass regression SHALL synchronize pass boundaries without explicit test sleeps.
4. THE design SHALL distinguish the pass period from traversal duration and offer/start timeout durations.

### Requirement 12: Executable verification and model mapping

**User Story:** As a reviewer, I want the implementation's invariants checked against
both storage backends and the actual protocol, so that model assumptions stay visible.

#### Acceptance Criteria

1. THE verification plan SHALL require property tests of `dispatch = Derive(state)` after committed transitions on both backends.
2. THE verification plan SHALL cover retries, delayed old offers, duplicate discovery, lost offers, ambiguous starts, broker restart, sticky deadline recovery, and partial acquisition repair.
3. THE verification plan SHALL include starvation cases with blocked prefixes larger than both the page size and admission budget.
4. THE design SHALL map concrete incarnation and protocol actions to `40_dispatch_handoff.tla`, including its weak-fairness and eventual-fault-cessation assumptions.
5. THE design SHALL explain how fenced multi-transaction acquisition refines the model's atomic `Reconcile` action.
6. THE design SHALL identify model changes or additional checks required where the current finite abstraction does not cover the implementation.
7. THE task plan SHALL assign every design correctness property a required property-test task with requirement references.
8. THE live DSQL validation plan SHALL cover chosen statement shapes, ordered index scans, aggregate repair limits, and conflicting ownership/repair transactions on an ephemeral cluster.
9. THE spec SHALL distinguish prior reported DSQL evidence from experiments actually performed for this feature.

## Design Decisions and Related Specifications

The [design](design.md) records the incarnation representation and legacy decoding,
physical dispatch layout, routing and repair indexes, resource bounds, timing
defaults, and lease-fence/error interfaces. The [implementation plan](tasks.md)
assigns their code changes and required verification. These are target contracts,
not claims that the existing broker or acquisition sweep already implements them.

The following existing specs carry scoped alignment notes for the replacement
workflow contract, keeping activity and unrelated API contracts intact:

- [runtime-durable-backlog](../runtime-durable-backlog/requirements.md): workflow
  grace/drain lifecycle and duplicate-offer claims.
- [runtime-broker-fairness](../runtime-broker-fairness/requirements.md): workflow
  backlog-source budgeting and metric sources after removal.
- [runtime-sweeper-recovery](../runtime-sweeper-recovery/requirements.md): read-only
  recovery, sticky reconstruction, and the serving gate.
- [recovery-index](../recovery-index/requirements.md): recovery coverage for pending
  sticky deadlines and the separate stale-dispatch sweep.
- [transient-wft](../transient-wft/requirements.md): internal retry identity while
  preserving the existing observable transient-task contract.
- [speculative-wft](../speculative-wft/requirements.md) and
  [task-queue-priority-fairness](../task-queue-priority-fairness/requirements.md):
  explicit preservation boundaries wherever dispatch derivation intersects them.

042 remains the broader direction. This feature adopts its workflow-task portion;
its activity, operation, successor, and projection changes are not implied deliverables.
