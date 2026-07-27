# Requirements Document

Feature: Task Queue Priority and Fairness

## Introduction

Temporal server v1.31.0 makes task-queue priority generally available through
`temporal.api.common.v1.Priority`. Priority ordering is active under the stock
`matching.useNewMatcher = true` default. Weighted fairness is an additional delivery
policy whose stock default is disabled (`matching.enableFairness = false`), and
automatic per-queue activation is separately disabled
(`matching.autoEnableV2 = false`).

Tokeira already accepts and persists workflow-start priority, echoes part of the
public task-queue configuration surface, and has a delivery pipeline spanning the pure
kernel's declarative `DispatchOp`, the runtime brokers, and the durable DSQL backlog.
It does not yet carry priority through activity and child commands, order broker or
backlog delivery by priority/fairness, enforce stored task-queue limits, or report real
per-priority statistics.

This feature completes that behavior in Tokeira's architecture:

- the pure kernel records priority as authoritative workflow/activity metadata and
  stamps effective priority onto declarative dispatch effects;
- the runtime delivery plane owns normalization, queue-local fair-pass counters,
  rate limiting, sticky/normal selection, and all broker ordering;
- storage preserves the runtime-selected delivery order while work is parked in the
  disposable durable backlog;
- the edge validates and translates the public contract without owning scheduling
  policy.

The compatibility authority is Temporal server `v1.31.0` under root
`AGENTS.md §8`. Wire shape comes from `proto/upstream/`; behavioral details were
verified against the local Temporal checkout at tag `v1.31.0`. The implementation is
original and SHALL NOT reproduce Temporal's internal matching-service or partition
architecture.

This is a follow-on to:

- `.kiro/specs/runtime-broker-tiered-delivery/`, which names Tokeira's sticky,
  live-ready, and durable-backlog delivery tiers;
- `.kiro/specs/runtime-broker-fairness/`, whose existing `FairnessState` allocates
  drain capacity *between queue families* and remains a distinct mechanical policy;
- `.kiro/specs/api-conformance-task-queue/`, which owns the public
  `DescribeTaskQueue` surface;
- `.kiro/specs/temporal-api-v1.62-sync/`, which introduced the current volatile
  `TaskQueueConfigStore`.

## Glossary

- **Priority:** The three-field public `temporal.api.common.v1.Priority` message.
- **Raw Priority:** Priority metadata exactly as supplied on a workflow, activity, or
  child command. Zero/empty fields retain their inherit/default meaning.
- **Effective Priority:** The field-wise merge of inherited and raw priority, with
  delivery-time defaults and clipping applied.
- **Priority Key:** Integer priority band; lower numbers are served first.
- **Default Priority Key:** Key 3 when the configured range is 1 through 5.
- **Fairness Key:** At most 64 bytes identifying a share group within one priority
  band.
- **Fairness Weight:** Relative service share for a fairness key.
- **Fair Pass:** Runtime-selected monotonically comparable value used to approximate
  weighted round-robin within one priority band.
- **Delivery Order:** The tuple `(priority_key, fair_pass, insertion_tie)` attached to
  one dispatchable task.
- **Queue Family:** A `QueueKey`'s namespace, task-queue name, task kind, Deployment,
  and Build ID identity. A sticky workflow queue additionally declares its normal
  queue family.
- **Sticky Tie Preference:** When equally prioritized workflow tasks are available
  from the polling worker's sticky queue and its normal queue, the sticky task wins.
- **Direct Match:** Delivery to an already-waiting poller when no competing queued work
  requires ordering.
- **User Fairness:** Fairness-key/weight policy *within* a queue family.
- **Drain Fairness:** Existing Tokeira `FairnessState` capacity allocation *between*
  queue families. It is not User Fairness.
- **Conformance Override:** A live, test-only dynamic-config value available only in
  `--features conformance` builds.
- **Auto-Enabled Queue:** A normal queue family whose process-local runtime policy has
  transitioned to fairness-enabled after observing qualifying priority metadata while
  `matching.autoEnableV2` is enabled.
- **Superseded Dispatch:** A previously published task whose authoritative logical
  sequence or activity stamp was replaced by a later transition.

## Target State

1. Priority keys 1 through 5 influence workflow and activity dispatch by default; key
   3 is the effective default.
2. Activities and child workflows inherit workflow priority field by field unless
   their command supplies non-zero/non-empty overrides.
3. Continue-as-new, workflow retries, and cron successors retain the predecessor
   workflow's priority as v1.31.0 does.
4. Fairness remains disabled under the stock default. A conformance build can enable
   it through the exact `matching.enableFairness` key or exercise
   `matching.autoEnableV2`.
5. When fairness is enabled, tasks within one priority band tend toward weighted
   service by fairness key while retaining FIFO order within one
   `(priority_key, fairness_key)` group.
6. Priority is the outer ordering dimension across sticky and normal workflow work.
   Sticky affinity wins only among otherwise equal candidates.
7. `UpdateWorkflowExecutionOptions` and `UpdateActivityOptions` can change priority;
   an unstarted superseded delivery is fenced and regenerated without duplicating the
   public schedule history.
8. The live-ready brokers and durable backlog preserve the same delivery-order
   metadata. Restart/failover may reconstruct queue-local fair counters from
   authoritative run state; fairness is best-effort, but task correctness is not.
9. Stored queue-wide and per-fairness-key rate limits shape task handout, and stored
   fairness-weight overrides take precedence over task-carried weights.
10. `DescribeTaskQueue.stats_by_priority_key` reports the bands Tokeira actually holds,
    rather than mirroring every task into key 3.
11. The kernel remains stateless between calls and free of I/O, clocks, mutable
    configuration, rate limiters, queues, and fair-pass counters.

The following boundaries remain explicit:

- The production configuration surface for enabling fairness is governed by
  `docs/conformance/v1.31.0/temporal-configuration.md` and the approved
  `.kiro/specs/configuration-policy/reference/configuration-policy-proposal.md`.
  This feature preserves the v1.31.0
  stock-default disabled posture and wires only the already-sanctioned conformance
  overrides until that production decision is accepted.
- `TaskQueueConfigStore` durability remains owned by the configuration-surface
  decision. This feature consumes and enforces its current values; it does not silently
  convert the existing volatile store into a new production policy database.
- Temporal's classic/new matcher migration topology, partition draining internals,
  and in-process matching metrics are not product contracts. Corpus leaves that assert
  those internals are classified by name in the fork rather than emulated in Tokeira.

## Evidence From Current Code

### Contract shape

- `proto/upstream/temporal/api/common/v1/message.proto` defines `Priority` fields
  `priority_key = 1`, `fairness_key = 2`, and `fairness_weight = 3`, their inheritance
  semantics, the five-level/default-key behavior, the 64-byte fairness-key limit, and
  weight precedence.
- `proto/upstream/temporal/api/command/v1/message.proto` carries priority on activity
  and child-workflow commands.
- `proto/upstream/temporal/api/history/v1/message.proto` carries priority on workflow
  start, activity schedule, child initiation, and workflow-options-updated events.
- `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` carries
  priority on start/signal-with-start, workflow/activity option updates, poll
  responses, and `stats_by_priority_key`; it defines `UpdateTaskQueueConfig`.
- `proto/upstream/temporal/api/taskqueue/v1/message.proto` defines queue-wide rate,
  per-fairness-key default rate, and fairness-weight overrides.

### Temporal v1.31.0 behavior

- `common/priorities/priority_util.go @ v1.31.0` validates raw priority and performs
  field-wise inheritance.
- `common/dynamicconfig/constants.go @ v1.31.0` sets
  `matching.useNewMatcher = true`, `matching.enableFairness = false`,
  `matching.priorityLevels = 5`, `matching.autoEnableV2 = false`, and
  `matching.maxFairnessKeyWeightOverrides = 1000`.
- `service/matching/config.go`, `fair_level.go`, `fair_task_writer.go`,
  `fairness_util.go`, `pri_matcher.go`, and `fairness.md @ v1.31.0` establish priority
  as the outer band, fair-pass/stride ordering within a band, weight precedence, and
  best-effort behavior.
- `service/matching/task_queue_partition_manager.go @ v1.31.0` disables fairness for
  sticky queues, makes fairness imply the priority-aware matcher, and defines
  auto-enable qualification.
- `service/history/api/command_attr_validator.go`,
  `recordactivitytaskstarted/api.go`, and
  `service/history/transfer_queue_active_task_executor.go @ v1.31.0` establish raw
  command persistence and effective activity/child inheritance.
- `service/history/api/updateworkflowoptions/api.go`,
  `updateactivityoptions/api.go`, and
  `service/history/workflow/mutable_state_impl.go @ v1.31.0` establish option updates,
  pending-task invalidation, and continue-as-new priority retention.
- `service/frontend/workflow_handler.go`, `validators.go`,
  `service/matching/ratelimit_manager.go`, and `fairness_util.go @ v1.31.0` establish
  task-queue config validation, immediate application, per-key rate scaling, and
  override precedence.
- `tests/priority_fairness_test.go @ v1.31.0` asserts ordering tendencies, sticky versus
  normal priority, fairness activation, and update invalidation. Its migration and
  metric assertions refer to Temporal internals rather than public wire behavior.

### Current Tokeira implementation

- `crates/tokeira-kernel/src/state.rs` already stores `WorkflowState.priority`, but
  `ActivityState` has no raw priority.
- `crates/tokeira-kernel/src/command.rs`, `event.rs`, and `transition.rs` omit priority
  from activity/child commands, their history events, and dispatch effects.
- `crates/tokeira-edge/src/grpc/translate.rs` drops command and activity-option
  priority, rejects workflow priority option masks, and drops sticky
  `TaskQueue.normal_name` after validating it.
- `crates/tokeira-runtime/src/publisher.rs` starts children with `priority: None`.
- `crates/tokeira-storage/src/api.rs` dispatchable/backlog types carry no delivery
  priority, and `crates/tokeira-storage/src/dsql/run_repository/dispatch.rs` drains only
  by `insertion_seq`.
- `crates/tokeira-runtime/src/broker.rs` uses one FIFO deque per sticky/general/activity
  queue and cannot compare a sticky poll's normal queue.
- `crates/tokeira-runtime/src/task_queue_config.rs` stores rate limits and weight
  overrides but has no dispatch-path consumer.
- `crates/tokeira-edge/src/grpc/translate.rs` currently mirrors aggregate task-queue
  stats into default key 3 rather than reporting real bands.

## Field and Contract Policy

### `temporal.api.common.v1.Priority`

| Field | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `priority_key` | Zero inherits/defaults; positive values are retained raw and clipped to delivery bands 1–5 when dispatched | Negative returns `INVALID_ARGUMENT` with v1.31.0's priority validation meaning | Raw value is durable; effective value selects the outer delivery band |
| `fairness_key` | Empty inherits/defaults; non-empty value identifies the within-band share group | More than 64 bytes returns `INVALID_ARGUMENT` | Raw value is durable; effective value keys queue-local fairness/rate state |
| `fairness_weight` | Zero inherits/defaults; queue override wins over positive task value, then default 1.0; effective delivery weight is clamped to `[0.001, 1000]` | Negative on a Priority returns `INVALID_ARGUMENT`; non-positive queue override returns `INVALID_ARGUMENT` | Raw task value is durable; effective value determines stride and per-key rate |

### Priority-bearing lifecycle surfaces

| Surface | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| Start / SignalWithStart | Validate and retain workflow raw priority | Priority validation error → `INVALID_ARGUMENT` | Workflow start history/state; WFT dispatch inherits it |
| Continue-as-new / retry / cron | Retain predecessor workflow priority | Not applicable | Successor start history/state |
| ScheduleActivity command | Retain raw activity override; merge it with workflow priority for dispatch | Priority validation error fails the WFT command with the v1.31.0 bad-schedule-attributes path | Activity schedule history/state; effective activity dispatch |
| StartChildWorkflow command | Retain raw child override; merge it with parent priority for the child start | Priority validation error fails the WFT command with v1.31.0 command validation | Child initiated history; effective child start |
| UpdateWorkflowExecutionOptions | Support whole `priority` and its field-mask subpaths according to v1.31.0 merge rules | Unsupported/invalid mask or resulting priority → `INVALID_ARGUMENT` | Options-updated history/state; replacement WFT dispatch when pending and unstarted |
| UpdateActivityOptions | Support `priority` and restore-original priority | Invalid resulting priority → `INVALID_ARGUMENT` | Activity state; replacement activity dispatch when pending and unstarted |
| Workflow poll / Describe | Return the workflow priority | Not applicable | Read-only projection |
| Activity poll | Return effective workflow-plus-activity priority | Not applicable | Read-only delivery response |
| Pending activity / ActivityOptions | Return the raw activity priority override | Not applicable | Read-only projection |

### `UpdateTaskQueueConfig`

| Field | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `update_queue_rate_limit` | Set/unset queue-wide handout rate; zero pauses handout | Negative RPS → `INVALID_ARGUMENT`; setting on workflow TQ → `INVALID_ARGUMENT` | Current config store; live activity/Nexus handout limiter |
| `update_fairness_key_rate_limit_default` | Set/unset default per-key rate, scaled by effective weight | Negative RPS → `INVALID_ARGUMENT`; setting on workflow TQ → `INVALID_ARGUMENT` | Current config store; live per-key activity/Nexus limiter |
| `set_fairness_weight_overrides` | Merge named overrides; max 1000 under stock config | Empty or >64-byte key, non-positive weight, request/result above limit, or overlap with unset → `INVALID_ARGUMENT` | Current config store; new scheduling/rate decisions use override |
| `unset_fairness_weight_overrides` | Remove named overrides | Empty or >64-byte key, request above limit, or overlap with set → `INVALID_ARGUMENT` | Current config store; future decisions fall back to task/default weight |
| `identity` / update metadata | Preserve existing v1.31.0 validation and response echo | Existing ID/reason length validation → `INVALID_ARGUMENT` | Audit metadata only |

### Conformance policy keys

| Key | v1.31.0 default | Tokeira policy | Persistence / side effect |
|---|---:|---|---|
| `matching.useNewMatcher` | `true` | Priority-aware delivery is the production default; conformance reads the live override for migration/auto-enable behavior | Runtime delivery only |
| `matching.enableFairness` | `false` | Conformance-wired Boolean; absent in production pending configuration decision | Runtime delivery only |
| `matching.autoEnableV2` | `false` | Conformance-wired Boolean with queue-local activation | Volatile queue-local activation |
| `matching.priorityLevels` | `5` | Pinned behavioral constant, not a production knob | Normalization only |
| `matching.maxFairnessKeyWeightOverrides` | `1000` | Existing conformance-wired admission limit | Edge validation only |

## Requirements

### Requirement 1: Validate and Resolve Priority

**User Story:** As an SDK author, I want priority values validated and inherited like
Temporal v1.31.0, so that the same request has the same effective delivery policy.

#### Acceptance Criteria

1. WHEN a Priority has a negative `priority_key`, THE Edge SHALL return
   `INVALID_ARGUMENT`.
2. WHEN a Priority has a fairness key longer than 64 bytes, THE Edge SHALL return
   `INVALID_ARGUMENT`.
3. WHEN a Priority has a negative `fairness_weight`, THE Edge SHALL return
   `INVALID_ARGUMENT`.
4. WHEN an override field is zero or empty, THE priority resolver SHALL inherit that
   field from the base Priority.
5. WHEN neither an override nor a base supplies a priority key, THE delivery policy
   SHALL use key 3.
6. WHEN a positive priority key lies outside the configured five bands, THE delivery
   policy SHALL clip it to the nearest band.
7. WHEN no effective fairness weight is supplied, THE delivery policy SHALL use weight
   1.0.
8. WHEN an effective fairness weight is below 0.001 or above 1000, THE delivery policy
   SHALL clamp it to that range.
9. WHEN a task-queue weight override exists for the effective fairness key, THE
   delivery policy SHALL prefer it over the task-carried weight.

### Requirement 2: Capture Priority Across Workflow Lineage

**User Story:** As a workflow author, I want priority to survive every workflow lineage
transition, so that successors do not silently lose dispatch policy.

#### Acceptance Criteria

1. WHEN a workflow starts with Priority, THE kernel SHALL record that raw Priority in
   authoritative workflow state and start history.
2. WHEN a signal-with-start creates a workflow, THE kernel SHALL apply the same
   Priority behavior as an ordinary start.
3. WHEN a workflow continues as new, THE runtime SHALL supply the predecessor
   workflow's Priority to the successor start.
4. WHEN a workflow retry successor is created, THE runtime SHALL supply the
   predecessor workflow's Priority to the successor start.
5. WHEN a cron successor is created, THE runtime SHALL supply the predecessor
   workflow's Priority to the successor start.
6. WHEN an unstarted workflow task is dispatched, THE kernel SHALL stamp the
   workflow's effective Priority onto the declarative dispatch effect.
7. WHEN a workflow task poll succeeds, THE Edge SHALL return the workflow's Priority
   on the public poll response.
8. WHEN a workflow is described, THE Edge SHALL return the workflow's Priority on
   `WorkflowExecutionInfo`.

### Requirement 3: Capture Activity and Child Overrides

**User Story:** As a workflow author, I want activities and children to inherit or
override priority field by field, so that local policy composes with workflow policy.

#### Acceptance Criteria

1. WHEN a ScheduleActivity command carries Priority, THE kernel SHALL record the raw
   activity override in schedule history and `ActivityState`.
2. WHEN an activity command omits a Priority field, THE kernel SHALL derive that
   field's dispatch value from workflow Priority.
3. WHEN an activity is dispatched, THE kernel SHALL stamp the effective merged
   Priority onto the declarative activity dispatch effect.
4. WHEN an activity poll succeeds, THE Edge SHALL return the effective merged
   Priority.
5. WHEN a pending activity is described, THE Edge SHALL return the raw activity
   Priority on `PendingActivityInfo.priority`.
6. WHEN a pending activity is described, THE Edge SHALL return the raw activity
   Priority on `PendingActivityInfo.activity_options.priority`.
7. WHEN a StartChildWorkflow command carries Priority, THE kernel SHALL record the raw
   child override in the initiated event.
8. WHEN a child start is dispatched, THE runtime SHALL merge the raw child override
   with parent workflow Priority.
9. WHEN the child run is created, THE runtime SHALL persist the merged Priority as the
   child's workflow Priority.

### Requirement 4: Prioritize Competing Work

**User Story:** As a task-queue operator, I want higher-priority work delivered first,
so that urgent work tends to precede lower-priority backlog.

#### Acceptance Criteria

1. WHILE priority-aware delivery is enabled, THE runtime broker SHALL choose the
   lowest available priority key before a higher key.
2. WHILE two tasks share one effective priority key and fairness is disabled, THE
   runtime broker SHALL preserve their enqueue order.
3. WHEN an already-waiting poller can directly receive fresh work without competing
   queued work, THE runtime broker SHALL permit direct delivery without waiting for
   priority backlog machinery.
4. WHEN competing work already exists for a queue family, THE runtime broker SHALL
   compare the fresh task against queued priority bands before handout.
5. WHEN a sticky workflow poll declares its normal queue, THE runtime broker SHALL
   compare eligible tasks from both queues by effective priority.
6. WHEN equally prioritized sticky and normal workflow tasks are eligible for the
   same sticky poll, THE runtime broker SHALL select the sticky task.
7. WHEN a normal task outranks a sticky task, THE runtime broker SHALL select the
   normal task.
8. WHEN a sticky task outranks a normal task, THE runtime broker SHALL select the
   sticky task.
9. WHEN a task is speculative, THE runtime SHALL retain the existing direct-delivery
   exception.

### Requirement 5: Apply Weighted User Fairness

**User Story:** As a multi-tenant queue user, I want fairness keys served in proportion
to weight, so that one continuously backlogged key cannot monopolize a priority band.

#### Acceptance Criteria

1. WHERE fairness is disabled, THE runtime broker SHALL ignore fairness keys and
   weights for ordering.
2. WHERE fairness is enabled, THE runtime broker SHALL assign each task a fair pass at
   publication time.
3. WHERE fairness is enabled, THE runtime broker SHALL order tasks within one priority
   band by fair pass.
4. WHERE fairness is enabled, THE runtime broker SHALL advance a fairness key's pass
   inversely to its effective weight.
5. WHILE two tasks share one priority key and fairness key, THE runtime broker SHALL
   preserve their enqueue order.
6. WHILE only one fairness key has runnable work, THE runtime broker SHALL allow that
   key to consume all available service.
7. WHEN a previously absent fairness key enters an active band, THE runtime broker
   SHALL initialize its pass at the band's current service frontier.
8. WHERE a workflow queue is sticky, THE runtime broker SHALL disable fairness-key
   scheduling for that sticky queue.
9. THE runtime broker SHALL keep User Fairness independent from the existing
   inter-queue Drain Fairness budget.

### Requirement 6: Preserve Delivery Order Through Backlog

**User Story:** As an operator using DSQL-backed delivery, I want broker and durable
backlog order to agree, so that parking work does not erase priority policy.

#### Acceptance Criteria

1. WHEN live-ready work is demoted, THE runtime SHALL persist its Delivery Order with
   the backlog row.
2. WHEN backlog work is drained, THE storage implementation SHALL return the lowest
   priority key first.
3. WHEN backlog work shares a priority key, THE storage implementation SHALL return
   the lowest fair pass first.
4. WHEN backlog work shares a priority key and fair pass, THE storage implementation
   SHALL return it by stable insertion tie.
5. WHEN a drained task is republished, THE runtime broker SHALL preserve its existing
   Delivery Order.
6. WHEN a broker is reconstructed after restart or shard takeover, THE runtime SHALL
   derive raw Priority from authoritative workflow/activity state.
7. WHEN fair-pass counters are lost, THE runtime broker SHALL preserve task correctness
   while allowing best-effort fairness to restart from a new frontier.
8. THE DSQL backlog key SHALL distinguish separate runnable tasks even when their
   insertion sequence values are equal.
9. THE in-memory backlog implementation SHALL apply the same ordering tuple as the
   DSQL implementation.

### Requirement 7: Update Priority and Fence Superseded Dispatch

**User Story:** As an operator, I want priority option updates to affect pending work
without duplicating public history, so that policy changes take effect promptly.

#### Acceptance Criteria

1. WHEN `UpdateWorkflowExecutionOptions` masks `priority`, THE Edge SHALL accept and
   validate the supplied Priority.
2. WHEN a workflow Priority update changes authoritative state, THE kernel SHALL emit
   a `WorkflowExecutionOptionsUpdated` event carrying the new Priority.
3. WHEN a workflow Priority update targets an unstarted pending workflow task, THE
   kernel SHALL advance its internal delivery fence without adding another public
   `WorkflowTaskScheduled` event.
4. WHEN a workflow Priority update supersedes an unstarted pending workflow task, THE
   kernel SHALL emit one replacement dispatch carrying the new Priority.
5. WHEN a superseded workflow task reaches start admission, THE runtime SHALL reject
   it as stale.
6. WHEN `UpdateActivityOptions` masks `priority`, THE Edge SHALL accept and validate
   the supplied raw activity Priority.
7. WHEN an accepted activity Priority update targets an activity before start, THE
   kernel SHALL advance the activity stamp, including when the selected value is
   equivalent to current state.
8. WHEN an accepted activity Priority update targets an activity before start, THE
   runtime SHALL publish one replacement dispatch carrying the new effective Priority.
9. WHEN a superseded activity task reaches start admission, THE runtime SHALL discard
   it as stale.
10. WHEN restore-original is requested, THE runtime SHALL restore raw activity
    Priority from the original schedule event.
11. WHEN a workflow option update produces no effective change, THE kernel SHALL
    author no options-updated event.
12. WHEN an activity Priority mask selects nested fields, THE kernel SHALL merge those
    fields independently against every matched activity's current raw Priority.

### Requirement 8: Enforce Task Queue Configuration

**User Story:** As a task-queue operator, I want accepted task-queue configuration to
shape handout, so that the API is not accepted-but-inert.

#### Acceptance Criteria

1. WHEN an activity or Nexus queue has a queue-wide rate limit, THE runtime broker
   SHALL delay handout until the queue-wide limiter permits one task.
2. WHEN a queue-wide rate limit is zero, THE runtime broker SHALL withhold task
   handout.
3. WHEN an activity or Nexus queue has a per-fairness-key default rate, THE runtime
   broker SHALL scale that key's rate by its effective fairness weight.
4. WHEN either a queue-wide or per-key rate limit changes, THE runtime broker SHALL
   apply the new value to subsequent handout decisions without restart.
5. IF a queue-wide rate is set on a workflow task queue, THEN THE Edge SHALL return
   `INVALID_ARGUMENT`.
6. IF a per-fairness-key rate is set on a workflow task queue, THEN THE Edge SHALL
   return `INVALID_ARGUMENT`.
7. IF a supplied rate is negative, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
8. IF a fairness-weight update contains an empty key, THEN THE Edge SHALL return
   `INVALID_ARGUMENT`.
9. IF a fairness-weight update contains a key longer than 64 bytes, THEN THE Edge
   SHALL return `INVALID_ARGUMENT`.
10. IF a fairness-weight update sets a non-positive weight, THEN THE Edge SHALL return
    `INVALID_ARGUMENT`.
11. IF a fairness key occurs in both set and unset collections, THEN THE Edge SHALL
    return `INVALID_ARGUMENT`.
12. IF a fairness-weight update exceeds the active override-count limit, THEN THE Edge
    SHALL return `INVALID_ARGUMENT`.
13. WHEN a valid fairness-weight update is accepted, THE TaskQueueConfigStore SHALL
    merge set and unset operations atomically.
14. WHEN task-queue configuration is stored, THE TaskQueueConfigStore SHALL isolate it
    by namespace, task-queue name, and task kind.
15. WHEN config persistence is lost on process restart, THE runtime SHALL fall back to
    built-in defaults without affecting authoritative workflow correctness.

### Requirement 9: Report Priority Statistics

**User Story:** As an operator, I want per-priority backlog statistics, so that I can
observe whether urgent work is accumulating.

#### Acceptance Criteria

1. WHEN enhanced task-queue statistics are requested, THE runtime SHALL compute
   backlog statistics separately for each populated effective priority key.
2. WHEN default-priority work is present, THE Edge SHALL report it under key 3.
3. WHEN non-default priority work is present, THE Edge SHALL report it under its
   effective clipped key.
4. WHEN a priority band is absent, THE Edge SHALL avoid fabricating its count from
   aggregate statistics.
5. WHEN aggregate statistics are returned, THE Edge SHALL make their count equal the
   sum of reported priority-band counts for the same observed broker image.
6. WHEN task-queue config is described, THE Edge SHALL continue returning the current
   stored rate and weight policy.

### Requirement 10: Honor Stock Defaults and Conformance Modes

**User Story:** As a compatibility maintainer, I want feature modes to match v1.31.0
defaults and test overrides, so that the same suite exercises the intended behavior.

#### Acceptance Criteria

1. WHERE no conformance override is active, THE runtime SHALL enable priority-aware
   delivery.
2. WHERE no conformance override is active, THE runtime SHALL disable User Fairness.
3. WHERE no conformance override is active, THE runtime SHALL disable automatic
   fairness activation.
4. WHERE `matching.enableFairness` is true, THE runtime SHALL enable User Fairness on
   normal queue families.
5. WHERE `matching.enableFairness` is true, THE runtime SHALL treat priority-aware
   delivery as enabled.
6. WHERE `matching.useNewMatcher` is false and fairness has not otherwise been enabled,
   THE conformance runtime SHALL use FIFO delivery for that queue.
7. WHERE `matching.autoEnableV2` is true, WHEN a fairness key is observed on a normal
   root queue, THE runtime SHALL mark that queue Auto-Enabled.
8. WHERE `matching.autoEnableV2` is true and priority-aware delivery is disabled, WHEN
   a non-zero priority key is observed on a normal root queue, THE runtime SHALL mark
   that queue Auto-Enabled.
9. WHERE `matching.autoEnableV2` is true and priority-aware delivery is enabled, WHEN
   only a priority key is observed, THE runtime SHALL leave that queue's fairness state
   unchanged.
10. WHEN a queue is Auto-Enabled, THE runtime SHALL keep User Fairness enabled for that
    process lifetime.
11. THE conformance override registry SHALL classify all three Boolean mode keys as
    wired only in the same change that adds their runtime consult sites.

### Requirement 11: Preserve Tokeira Architecture

**User Story:** As a Tokeira maintainer, I want priority/fairness to remain delivery
policy, so that conformance does not contaminate the authoritative transition model.

#### Acceptance Criteria

1. THE kernel SHALL remain free of I/O, async execution, storage clients, metrics,
   network calls, and mutable configuration reads.
2. THE kernel SHALL retain no process-local state between `apply` calls.
3. THE kernel SHALL retain no fair-pass counters, token buckets, poller observations,
   or ready queues.
4. THE kernel SHALL express delivery only through declarative post-commit effects.
5. THE runtime SHALL treat broker and durable-backlog ordering metadata as
   reconstructible delivery policy rather than workflow authority.
6. THE runtime SHALL leave lane routing based on run ownership rather than task
   priority.
7. THE existing inter-queue Drain Fairness controller SHALL remain mechanically
   derived rather than user-configurable.
8. THE implementation SHALL introduce no Temporal history-service, matching-service,
   or task-queue-partition object.

### Requirement 12: Verify the Public Behavior Honestly

**User Story:** As a conformance reviewer, I want the upstream corpus exercised without
product-side internal-service emulation, so that green results demonstrate public
behavior.

#### Acceptance Criteria

1. WHEN the Priority activity leaf waits for backlog population through
   `DescribeTaskQueuePartition`, THE conformance harness SHALL answer that read-only
   observation from public `DescribeTaskQueue` data.
2. WHEN the sticky-priority leaf observes sticky pollers/backlog through
   `DescribeTaskQueuePartition`, THE conformance harness SHALL answer that read-only
   observation from public `DescribeTaskQueue` data.
3. WHEN the auto-enable leaf waits through `GetTaskQueueTasks`, THE conformance harness
   SHALL answer only the count observation required by the leaf from public task-queue
   data.
4. THE conformance harness SHALL avoid changing `test_env.go` for these observation
   adapters.
5. THE conformance harness SHALL avoid editing upstream corpus test bodies.
6. WHEN a leaf asserts Temporal's classic/new matcher migration topology, THE skip
   registry SHALL classify that leaf by full name with a cited architectural reason.
7. WHEN a leaf asserts in-process Temporal matching-client metrics unavailable over
   the public wire, THE skip registry SHALL classify that leaf by full name with a
   cited harness reason.
8. WHEN a leaf asserts priority or fairness ordering tendencies over the public poll
   API, THE conformance run SHALL keep that leaf active.
9. WHEN implementation is complete, THE campaign ledger SHALL record two consecutive
   clean fresh-process runs of the active Priority and Fairness leaves.
10. WHEN a pinned corpus leaf deterministically reuses workflow IDs that an earlier
    leaf leaves running under the default `WORKFLOW_ID_CONFLICT_POLICY_FAIL`, THE skip
    registry SHALL classify the suite-lifecycle defect by full name, and THE campaign
    evidence SHALL exercise that same public-behavior leaf in an isolated fresh-process
    run without changing workflow-ID conflict semantics.

## Iteration and Feedback Notes

- The earlier research statement that `DescribeTaskQueue.stats_by_priority_key` was
  empty was stale. Current Tokeira mirrors aggregate statistics into key 3; this spec
  replaces that placeholder with real bands.
- The earlier "sticky first" shorthand was too broad. The v1.31.0 corpus requires
  priority to be the outer dimension across normal and sticky work, with sticky only
  winning an equal-priority tie.
- The DSQL backlog currently receives placeholder `insertion_seq = 0` from runtime
  while its deterministic row key is derived from that value and queue identity. This
  spec treats distinct task identity and stable ordering as separate requirements so
  priority work does not preserve an existing collision hazard.
- Requirements 8.15 and the Target State intentionally preserve the current volatile
  `TaskQueueConfigStore` boundary. Durable public policy storage remains a visible
  configuration-surface decision rather than being smuggled into this conformance
  change.
