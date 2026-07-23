# Requirements Document: Client Miscellaneous Conformance

## Introduction

This spec owns the remaining Tokeira behavior required by Tier 9.44's
`TestClientMiscTestSuite` at Temporal server `v1.31.0`. The neighboring
`TestClientDataConverterTestSuite` has three test methods that are skipped by the
upstream corpus itself; Tokeira does not add a conformance-registry exclusion for
them.

The implementation preserves Tokeira's architecture rather than reproducing
Temporal's history/matching service topology. Policy is resolved by the runtime,
workflow-transition semantics remain in the pure kernel, broker liveness remains
disposable runtime state, and durable summaries are replay-derived from history.

This spec supersedes the following older assumptions for this surface:

- sticky affinity does not expire merely because its per-task
  schedule-to-start timeout elapsed; that timeout belongs to each dispatched
  workflow task;
- `WorkflowExecutionInfo.auto_reset_points` is no longer truthfully empty once
  reset-point tracking lands; and
- batch reset by build ID is not equivalent to resetting to the first workflow
  task. It resolves the recorded first completion for the requested build ID.

## Compatibility Ground Truth

- Pending-command limits and their defaults are defined by
  `common/dynamicconfig/constants.go:336-360 @ v1.31.0`; command admission uses
  the current pending count in
  `service/history/api/respondworkflowtaskcompleted/workflow_size_checker.go`
  and fails the workflow task in
  `workflow_task_completed_handler.go:479,898,1132,1182 @ v1.31.0`.
- Embedded activity timeout validation is defined by
  `chasm/lib/activity/validator.go:51-206 @ v1.31.0` and is surfaced as
  `BAD_SCHEDULE_ACTIVITY_ATTRIBUTES` by
  `service/history/api/command_attr_validator.go:75-134 @ v1.31.0`.
- Sticky worker unavailability and normal-queue fallback are defined by
  `service/matching/matching_engine.go:70-72,568-581` and
  `service/history/transfer_queue_active_task_executor.go:318-338 @ v1.31.0`;
  mutable stickiness remains set until a normal-queue task starts, as described
  by `service/history/api/recordworkflowtaskstarted/api.go:115-149 @ v1.31.0`.
- Reset points are created by
  `service/history/workflow/workflow_task_state_machine.go:1368-1393` and
  `mutable_state_impl.go:3347-3372 @ v1.31.0`; batch build-ID targeting reads
  them in `service/worker/batcher/activities.go:762-779,856-890 @ v1.31.0`.
- Activity starts for retry-policy activities remain transient until a terminal
  resolution materializes the start immediately before that terminal event
  (`service/history/workflow/mutable_state_impl.go:4039-4174 @ v1.31.0`);
  retryable failures persist neither the transient start nor the failure.
- Wire shapes and enum numbers come from vendored
  `proto/upstream/temporal/api/` sources. In particular, pending-limit causes
  are values 26-29 in `enums/v1/failed_cause.proto`, and reset-point fields are
  defined in `workflow/v1/message.proto`.

## Glossary

- **Completion limits:** Concrete per-workflow limits supplied with one
  workflow-task completion: pending child workflows, activities, external
  signals, and external cancellation requests.
- **Provisional command state:** The transition builder's state after earlier
  commands in the same completion have been applied but before the transition
  commits.
- **Sticky affinity:** Durable workflow metadata naming the sticky task queue and
  its cache-owning worker.
- **Sticky availability:** A runtime-only observation that the sticky queue has
  a recent or currently waiting poller and has not been unloaded by worker
  shutdown.
- **Sticky task deadline:** The schedule-to-start deadline attached to one
  pending sticky workflow task, not an expiry of the workflow's affinity.
- **Auto-reset point:** Durable, replay-derived metadata identifying the first
  successful workflow-task completion processed by a binary checksum/build-ID
  pair.

## Requirements

### Requirement 1: Runtime-Resolved Completion Limits

**User Story:** As an operator, I want pending-command limits to be enforced
without introducing mutable configuration or I/O into the kernel.

#### Acceptance Criteria

1. THE Runtime SHALL resolve concrete completion limits before invoking the
   Kernel.
2. IN a production build, THE Runtime SHALL use the v1.31.0 default of `2000`
   for each of the four limits.
3. IN a conformance build, THE Runtime SHALL read the following integer
   overrides live at workflow-task completion and SHALL otherwise use `2000`:
   `limit.numPendingChildExecutions.error`,
   `limit.numPendingActivities.error`, `limit.numPendingSignals.error`, and
   `limit.numPendingCancelRequests.error`.
4. WHEN an override or configured limit is zero or negative, THE Runtime SHALL
   represent that limit as disabled, matching v1.31.0 `withinLimit` behavior.
5. THE Runtime SHALL pass the already-resolved values to the Kernel as explicit
   transition input. THE Kernel SHALL NOT read global configuration, perform
   I/O, or retain the completion-limit policy in `WorkflowState`.
6. THE conformance override registry SHALL classify the four keys as wired only
   in the same change that adds their real runtime consult site.

### Requirement 2: Atomic Pending-Command Limit Enforcement

**User Story:** As a workflow author, I want an over-limit command batch to fail
atomically so earlier commands from the same failed completion do not leak into
history or state.

#### Acceptance Criteria

1. BEFORE admitting a `StartChildWorkflow` command, THE Kernel SHALL compare the
   provisional count of pending children with the child-execution limit.
2. BEFORE admitting a `ScheduleActivity` command, THE Kernel SHALL compare the
   provisional count of pending activities with the activity limit.
3. BEFORE admitting a `SignalExternalWorkflowExecution` command, THE Kernel
   SHALL compare the provisional count of pending external signals with the
   signal limit.
4. BEFORE admitting a `RequestCancelExternalWorkflowExecution` command, THE
   Kernel SHALL compare the provisional count of pending external cancellation
   requests with the cancellation-request limit.
5. WHEN the current count is lower than the enabled limit, THE Kernel SHALL
   admit the command. WHEN the current count is equal to or greater than the
   enabled limit, THE Kernel SHALL reject the workflow-task completion.
6. THE provisional count SHALL include commands admitted earlier in the same
   completion.
7. WHEN any command exceeds a limit, THE rejected completion SHALL commit none
   of its `WorkflowTaskCompleted` event, command events, state mutations, or
   dispatch effects.
8. THE Runtime SHALL route the rejection through its existing server-decided
   invalid-command path so a `WorkflowTaskFailed` event is authored and the
   completion RPC reports `INVALID_ARGUMENT`.
9. THE failure cause and composed message SHALL be exactly:
   - cause 26, `PendingChildWorkflowsLimitExceeded: the number of pending child workflow executions, {count}, has reached the per-workflow limit of {limit}`;
   - cause 27, `PendingActivitiesLimitExceeded: the number of pending activities, {count}, has reached the per-workflow limit of {limit}`;
   - cause 28, `PendingSignalsLimitExceeded: the number of pending signals to external workflows, {count}, has reached the per-workflow limit of {limit}`; and
   - cause 29, `PendingRequestCancelLimitExceeded: the number of pending requests to cancel external workflows, {count}, has reached the per-workflow limit of {limit}`.
10. New serialized `WorkflowTaskFailedCause` variants SHALL be appended so all
    existing postcard discriminants remain unchanged.

### Requirement 3: Schedule-Activity Attribute Validation

**User Story:** As a workflow author, I want invalid activity commands to fail
the workflow task rather than create activities with unusable timeout policy.

#### Acceptance Criteria

1. WHEN a `ScheduleActivity` command has neither a positive
   `ScheduleToCloseTimeout` nor a positive `StartToCloseTimeout`, THE Kernel
   SHALL reject the completion with `BadScheduleActivityAttributes`.
2. THE missing-timeout diagnostic SHALL match v1.31.0:
   `a valid StartToClose or ScheduleToCloseTimeout is not set on ScheduleActivityTaskCommand. ActivityId={activity_id} ActivityType={activity_type}`.
3. WHEN `ScheduleToCloseTimeout` is present, omitted schedule-to-start and
   start-to-close timeouts SHALL inherit it, and explicitly supplied values
   SHALL be capped by it.
4. WHEN only `StartToCloseTimeout` is present, schedule-to-close and omitted
   schedule-to-start SHALL inherit the workflow run timeout.
5. WHEN a positive workflow run timeout exists, activity timeouts SHALL be
   capped by it; heartbeat timeout SHALL additionally be capped by
   start-to-close.
6. WHEN the activity task queue is omitted or empty, THE command SHALL use the
   workflow's normal task queue, matching v1.31.0 embedded-activity
   normalization.
7. Validation and normalization that depend on authoritative run state SHALL
   occur inside the pure Kernel transition. THE edge SHALL preserve the wire
   values without converting a command-level validation failure into an
   early whole-RPC transport rejection.

### Requirement 4: Sticky Affinity and Worker Availability

**User Story:** As an SDK worker, I want workflow tasks to fall back promptly
from an unavailable sticky worker while preserving the upstream sticky-state
lifecycle.

#### Acceptance Criteria

1. A successful workflow-task completion with sticky attributes SHALL retain
   the sticky queue, worker identity, and schedule-to-start timeout as durable
   affinity metadata.
2. THE schedule-to-start timeout SHALL create a deadline only for each pending
   sticky workflow task. Its passage SHALL NOT independently expire or clear
   the run's sticky affinity.
3. THE Broker SHALL maintain only volatile sticky-poller liveness sufficient to
   distinguish a recent/active sticky poller from an unavailable one. This
   observation SHALL NOT become workflow authority and MAY be lost on process
   restart.
4. A sticky poller SHALL be considered unavailable after the v1.31.0 ten-second
   observation window without a poll, or immediately when worker shutdown
   unloads/denies that sticky worker.
5. WHEN publishing either a normal or speculative WFT to an unavailable sticky
   worker, THE Runtime SHALL publish the derived task to the workflow's normal
   queue immediately rather than waiting for the sticky schedule-to-start
   deadline.
6. Immediate fallback SHALL NOT mutate or clear durable sticky affinity.
7. WHEN the fallback task starts from the normal queue, THE authoritative start
   transition SHALL clear sticky affinity and the poll response SHALL carry
   full history from event 1.
8. WHEN the sticky worker remains available, THE task SHALL retain sticky
   routing and the per-task deadline.
9. Recovery and storage dispatch scans SHALL derive sticky task expiry from the
   pending WFT's schedule-to-start deadline, not from an affinity expiry.
10. Query sticky-first fallback SHALL use a deadline derived for that query,
    not the age of the last workflow-task completion.

### Requirement 5: Durable Auto-Reset Points

**User Story:** As an operator, I want reset-by-build-ID to target the first WFT
processed by that build rather than an unrelated workflow-task boundary.

#### Acceptance Criteria

1. AFTER a successful workflow-task completion, THE Kernel SHALL derive the
   completion's `(binary_checksum, build_id)` pair from the fields recorded on
   that completion. A Worker Deployment version build ID SHALL take precedence
   over the deprecated worker-version stamp build ID when both exist.
2. WHEN no existing point has the same pair, THE Kernel SHALL append an
   auto-reset point containing the pair, current run ID, completed event ID,
   completion time, and resettable status.
3. WHEN a point already has the same pair, THE Kernel SHALL preserve the
   existing first-completion point and SHALL NOT append a duplicate.
4. THE default maximum retained point count SHALL be 20, matching v1.31.0
   `DefaultHistoryMaxAutoResetPoints`; when exceeded, the oldest points SHALL be
   discarded.
5. Resettable status SHALL be false when the pre-command state has pending child
   workflows, pending external cancellation requests, or pending external
   signals; otherwise it SHALL be true.
6. THE same reset-point sequence SHALL be reconstructed when committed history
   is replayed. Auto-reset points are a durable summary derived from history,
   never an authority independent of it.
7. `DescribeWorkflowExecution` SHALL expose retained points through
   `WorkflowExecutionInfo.auto_reset_points` with the v1.31.0 wire fields.
8. Existing BuildIds search-attribute derivation and Worker Deployment routing
   metadata SHALL remain unchanged.

### Requirement 6: Batch Reset by Build ID

**User Story:** As an operator, I want a batch reset targeted by build ID to use
the corresponding auto-reset point.

#### Acceptance Criteria

1. WHEN a batch reset targets a build ID present in the selected execution's
   auto-reset points, THE engine SHALL use that point's
   `first_workflow_task_completed_id` as the concrete reset target.
2. WHEN the matching point is not resettable, expired, or absent, THE individual
   batch item SHALL fail with a descriptive error and SHALL NOT reset the run.
3. THE implementation SHALL NOT consult `WorkflowState.build_id` as a proxy for
   reset-point membership and SHALL NOT substitute the first workflow task.
4. Resetting to a recorded build point SHALL preserve the existing reset
   transition's fork/reapply semantics and allow a new worker build to replay
   from that boundary.
5. Cross-run reset-point rollover and `current_run_only` remain governed by the
   existing batch/reset field-support classification; this tier SHALL not
   fabricate cross-run points it cannot resolve.

### Requirement 7: Upstream Data-Converter Skips

**User Story:** As a conformance maintainer, I want the ledger to distinguish
upstream-disabled tests from Tokeira exclusions.

#### Acceptance Criteria

1. THE three `TestClientDataConverterTestSuite` methods that call upstream
   `SkipNow` SHALL remain upstream skips.
2. THE Temporal fork's Tokeira conformance skip registry SHALL NOT add entries
   for those methods.
3. Tier 9.44 reporting SHALL identify the three skips as corpus-authored rather
   than unsupported Tokeira behavior.

### Requirement 8: Regression and Documentation Gates

**User Story:** As a maintainer, I want the fixes to preserve existing workflow,
sticky, reset, and command-validation behavior.

#### Acceptance Criteria

1. Property tests SHALL prove that enabled pending limits reject exactly at the
   boundary, disabled limits never reject, and a rejected multi-command batch
   produces no transition.
2. Tests SHALL prove exact cause/message rendering for all four pending-limit
   failures.
3. Tests SHALL prove schedule-activity missing-timeout rejection and valid
   timeout normalization.
4. Tests SHALL prove sticky affinity survives worker unavailability, fallback is
   immediate, and normal-queue start clears it while returning full history.
5. Tests SHALL prove reset-point first-observation deduplication, maximum-count
   retention, replay equivalence, Describe translation, and batch build-ID
   resolution.
6. Tests SHALL avoid explicit sleeps by injecting observation times or using
   synchronization.
7. `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` and the v1.31.0 conformance
   ledger SHALL be updated when reset-point support lands.
8. Two consecutive isolated Tier 9.44 suite runs SHALL complete with all
   testable leaves passing and only the three upstream data-converter skips.
9. WHEN an activity has a retry policy, THE Runtime SHALL keep its current
   start transient, SHALL persist neither that start nor a retryable failure,
   and SHALL materialize the start immediately before a terminal activity
   result. Activities without a retry policy SHALL retain their existing
   immediate-start history behavior.
10. THE out-of-process conformance seam SHALL forward the four suite-global
    pending-limit overrides through the existing scoped override bridge. For
    the one direct mutable-state assertion in this suite, THE fork SHALL expose
    only a read-only `GetWorkflowExecution` adapter backed by Tokeira's existing
    `DescribeMutableState` response; unrelated persistence methods SHALL remain
    unavailable and corpus test bodies SHALL remain unchanged.
