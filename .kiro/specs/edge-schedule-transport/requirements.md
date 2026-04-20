# Requirements Document: Edge Schedule Transport

## Introduction

This spec implements the Schedule Transport layer — the 7 gRPC handlers for Temporal's Schedule feature in the `tokeira-edge` crate, plus the backing schedule store and execution engine in `tokeira-runtime`. Schedules provide cron-like recurring workflow execution with rich calendar/interval specs, overlap policies, catchup windows, and operational controls (pause, trigger, backfill).

This is Feature 6 from the umbrella spec `edge-complete-implementation`. It has no dependencies on other features in the umbrella spec. The work covers 7 gRPC handlers across two categories:

1. **CRUD** (Phase 1): `create_schedule`, `describe_schedule`, `update_schedule`, `delete_schedule` — storage and retrieval of schedule configuration.
2. **Execution Engine** (Phase 2): A background component that evaluates schedule specs and triggers workflow starts at the correct times, respecting overlap policies and catchup windows.
3. **Operational** (Phase 3): `patch_schedule` (trigger immediately, pause, unpause, backfill), `list_schedules` (paginated listing), `list_schedule_matching_times` (compute future action times from a spec).
4. **Integration** (Phase 4): Wire `cron_schedule` field on `WorkflowExecutionStartedEventAttributes` for schedule-triggered workflows, and ensure schedule-triggered starts flow through the existing `StartWorkflowExecution` path.

The schedule store is in-memory for MVP — a `DashMap<(NamespaceId, ScheduleId), ScheduleEntry>` with conflict tokens for optimistic concurrency (same pattern as `VersioningRuleStore`). Durable persistence is deferred to the DSQL storage spec.

The kernel stays pure. Schedule evaluation and execution are edge/runtime-layer concerns. Schedule-triggered workflow starts use the existing `StartWorkflowExecution` path.

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **ScheduleStore**: The in-memory store (in `tokeira-runtime`) that persists schedule entries per (namespace, schedule_id) with conflict tokens for optimistic concurrency.
- **ScheduleId**: A string identifier for a schedule, unique within a namespace.
- **ScheduleEntry**: The full stored state of a schedule: spec, action, policies, state, info, memo, and search attributes.
- **ScheduleSpec**: The proto `temporal.api.schedule.v1.ScheduleSpec` describing when actions fire — composed of structured calendars, intervals, cron strings, exclusions, start/end times, jitter, and timezone.
- **ScheduleAction**: The proto `temporal.api.schedule.v1.ScheduleAction` describing what to do — currently only `start_workflow`.
- **SchedulePolicies**: The proto `temporal.api.schedule.v1.SchedulePolicies` controlling overlap behavior, catchup window, pause-on-failure, and workflow ID uniqueness.
- **ScheduleState**: The proto `temporal.api.schedule.v1.ScheduleState` holding paused flag, notes, and remaining actions for limited-action schedules.
- **ScheduleInfo**: The proto `temporal.api.schedule.v1.ScheduleInfo` holding action count, missed/skipped counts, running workflows, recent actions, and future action times.
- **SchedulePatch**: The proto `temporal.api.schedule.v1.SchedulePatch` for operational mutations: trigger immediately, backfill, pause, unpause.
- **ConflictToken**: An opaque token for optimistic concurrency on schedule updates. Each mutation increments the token; updates with a stale token are rejected.
- **OverlapPolicy**: The enum `temporal.api.enums.v1.ScheduleOverlapPolicy` controlling behavior when a new action is due while a previous one is still running (SKIP, BUFFER_ONE, BUFFER_ALL, CANCEL_OTHER, TERMINATE_OTHER, ALLOW_ALL).
- **CatchupWindow**: The duration within which missed actions (due to server downtime) will be executed on recovery. Defaults to one year; minimum 10 seconds.
- **ScheduleExecutionEngine**: The background component in `tokeira-runtime` that periodically evaluates schedule specs and triggers workflow starts at the correct times.
- **MatchingTimesComputation**: The pure function that computes future action timestamps from a `ScheduleSpec` within a given time range, without executing any actions.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

---

## Phase 1: Schedule Storage and CRUD

### Requirement 1: ScheduleStore — In-Memory Schedule Storage

**User Story:** As a Tokeira developer, I want to store schedule entries per (namespace, schedule_id), so that schedule CRUD operations have a backing store.

#### Acceptance Criteria

1. THE ScheduleStore SHALL store a `ScheduleEntry` per (namespace_id, schedule_id) pair.
2. THE ScheduleStore SHALL maintain a conflict token per schedule entry, initialized to a deterministic value on creation.
3. WHEN a mutation is applied to a schedule entry, THE ScheduleStore SHALL increment the conflict token.
4. WHEN an update request carries a non-empty conflict token that does not match the current stored token, THE ScheduleStore SHALL reject the mutation with a `FAILED_PRECONDITION` error.
5. WHEN an update request carries an empty conflict token, THE ScheduleStore SHALL apply the update unconditionally (no optimistic concurrency check).
6. THE ScheduleStore SHALL be safe for concurrent access from multiple gRPC handler threads.
7. WHEN a schedule entry does not exist for a (namespace_id, schedule_id) pair, THE ScheduleStore SHALL return a `NOT_FOUND` error for describe, update, patch, and delete operations.

### Requirement 2: create_schedule Handler

**User Story:** As a Temporal SDK user, I want to create a new schedule via the `create_schedule` gRPC endpoint, so that I can set up recurring workflow executions.

#### Acceptance Criteria

1. WHEN the `create_schedule` endpoint is called with a valid namespace, schedule_id, and schedule definition, THE handler SHALL store the schedule entry and return a conflict token.
2. WHEN the `create_schedule` endpoint is called with a schedule_id that already exists in the namespace, THE handler SHALL return `ALREADY_EXISTS`.
3. WHEN the request includes an `initial_patch`, THE handler SHALL apply the patch to the newly created schedule before returning (e.g., trigger immediately on creation).
4. WHEN the request includes `memo` and `search_attributes`, THE handler SHALL store them alongside the schedule entry.
5. THE handler SHALL initialize `ScheduleInfo` with zero action counts, empty recent actions, and a `create_time` set to the current timestamp.
6. THE handler SHALL initialize `ScheduleState` from the provided schedule state (or defaults: not paused, no limited actions).
7. WHEN the schedule_id is empty, THE handler SHALL return `INVALID_ARGUMENT`.
8. WHEN the schedule definition is missing (no spec or no action), THE handler SHALL return `INVALID_ARGUMENT`.

### Requirement 3: describe_schedule Handler

**User Story:** As a Temporal SDK user, I want to describe an existing schedule via the `describe_schedule` gRPC endpoint, so that I can inspect its configuration, state, and recent activity.

#### Acceptance Criteria

1. WHEN the `describe_schedule` endpoint is called with a valid namespace and schedule_id, THE handler SHALL return the schedule definition, info, memo, search attributes, and conflict token.
2. WHEN the schedule does not exist, THE handler SHALL return `NOT_FOUND`.
3. THE response SHALL include computed `future_action_times` (next 10 scheduled action times from the current moment).
4. THE response SHALL include `recent_actions` (most recent 10 action results).

### Requirement 4: update_schedule Handler

**User Story:** As a Temporal SDK user, I want to update an existing schedule via the `update_schedule` gRPC endpoint, so that I can change its spec, action, policies, or state.

#### Acceptance Criteria

1. WHEN the `update_schedule` endpoint is called with a valid namespace, schedule_id, and new schedule definition, THE handler SHALL replace the schedule's spec, action, policies, and state with the provided values.
2. WHEN the request carries a non-empty `conflict_token` that does not match the stored token, THE handler SHALL return `FAILED_PRECONDITION`.
3. WHEN the request carries an empty `conflict_token`, THE handler SHALL apply the update unconditionally.
4. WHEN the schedule does not exist, THE handler SHALL return `NOT_FOUND`.
5. WHEN the request includes `search_attributes`, THE handler SHALL update the stored search attributes.
6. THE handler SHALL update `ScheduleInfo.update_time` to the current timestamp.

### Requirement 5: delete_schedule Handler

**User Story:** As a Temporal SDK user, I want to delete a schedule via the `delete_schedule` gRPC endpoint, so that I can remove schedules that are no longer needed.

#### Acceptance Criteria

1. WHEN the `delete_schedule` endpoint is called with a valid namespace and schedule_id, THE handler SHALL remove the schedule entry from the store.
2. WHEN the schedule does not exist, THE handler SHALL return `NOT_FOUND`.
3. WHEN the schedule is deleted, THE ScheduleExecutionEngine SHALL stop evaluating that schedule for future actions.

---

## Phase 2: Schedule Execution Engine

### Requirement 6: Schedule Spec Evaluation — Matching Times Computation

**User Story:** As a Tokeira developer, I want a pure function that computes the next action times from a `ScheduleSpec`, so that both the execution engine and the `list_schedule_matching_times` handler can determine when actions should fire.

#### Acceptance Criteria

1. THE MatchingTimesComputation SHALL accept a `ScheduleSpec` and a time range (start_time, end_time) and return a list of timestamps within that range when actions should fire.
2. WHEN the spec contains `structured_calendar` entries, THE computation SHALL match timestamps where all fields of at least one `StructuredCalendarSpec` match.
3. WHEN the spec contains `interval` entries, THE computation SHALL match timestamps of the form `epoch + n * interval + phase` for integer n.
4. WHEN the spec contains both calendar and interval entries, THE computation SHALL return the union of all matching times.
5. WHEN the spec contains `exclude_structured_calendar` entries, THE computation SHALL exclude any timestamps that match the exclusion specs.
6. WHEN the spec has a `start_time` set, THE computation SHALL exclude timestamps before `start_time`.
7. WHEN the spec has an `end_time` set, THE computation SHALL exclude timestamps after `end_time`.
8. WHEN the spec has `jitter` set, THE computation SHALL add a deterministic random offset between 0 and `jitter` to each nominal action time (using schedule_id + nominal_time as seed for determinism).
9. WHEN the spec has a `timezone_name` set, THE computation SHALL interpret calendar specs in that timezone.
10. FOR ALL valid ScheduleSpec values, computing matching times for a range and then computing for a sub-range SHALL return a subset of the original result (monotonicity property).

### Requirement 7: Schedule Execution Engine — Background Ticker

**User Story:** As a Tokeira operator, I want schedules to automatically trigger workflow starts at the configured times, so that recurring workflows execute without manual intervention.

#### Acceptance Criteria

1. THE ScheduleExecutionEngine SHALL periodically evaluate all active (non-paused, non-deleted) schedules to determine if any actions are due.
2. WHEN an action time has passed and is within the schedule's catchup window, THE engine SHALL trigger the action.
3. WHEN an action time has passed and is outside the catchup window, THE engine SHALL skip the action and increment `ScheduleInfo.missed_catchup_window`.
4. WHEN the OverlapPolicy is `SKIP` and a previous action is still running, THE engine SHALL skip the new action and increment `ScheduleInfo.overlap_skipped`.
5. WHEN the OverlapPolicy is `BUFFER_ONE`, THE engine SHALL buffer at most one pending action while a previous action is running.
6. WHEN the OverlapPolicy is `BUFFER_ALL`, THE engine SHALL buffer all pending actions while a previous action is running.
7. WHEN the OverlapPolicy is `CANCEL_OTHER`, THE engine SHALL cancel the running workflow before starting the new action.
8. WHEN the OverlapPolicy is `TERMINATE_OTHER`, THE engine SHALL terminate the running workflow before starting the new action.
9. WHEN the OverlapPolicy is `ALLOW_ALL`, THE engine SHALL start the new action regardless of running actions.
10. WHEN a schedule has `limited_actions` set to true and `remaining_actions` reaches zero, THE engine SHALL stop triggering actions for that schedule.
11. WHEN the engine triggers a `start_workflow` action, THE engine SHALL invoke the existing `StartWorkflowExecution` path with the workflow configuration from `ScheduleAction.start_workflow`.

### Requirement 8: Schedule-Triggered Workflow ID Generation

**User Story:** As a Tokeira developer, I want schedule-triggered workflows to have unique workflow IDs by default, so that multiple executions of the same schedule do not conflict.

#### Acceptance Criteria

1. WHEN the engine triggers a workflow start and `SchedulePolicies.keep_original_workflow_id` is false, THE engine SHALL append a timestamp suffix to the workflow ID from the action definition.
2. WHEN the engine triggers a workflow start and `SchedulePolicies.keep_original_workflow_id` is true, THE engine SHALL use the workflow ID from the action definition without modification.
3. THE timestamp suffix format SHALL be deterministic and based on the nominal schedule time (not wall clock), ensuring idempotent retries produce the same workflow ID.

### Requirement 9: Pause-on-Failure and Workflow Completion Observation

**User Story:** As a Temporal operator, I want schedules to automatically pause when a triggered workflow fails, so that I can investigate failures before more actions are triggered.

#### Acceptance Criteria

1. WHEN `SchedulePolicies.pause_on_failure` is true and a schedule-triggered workflow reaches a terminal failed state (after all retries are exhausted), THE engine SHALL set `ScheduleState.paused` to true and update `ScheduleState.notes` with a message indicating the failure.
2. WHEN `SchedulePolicies.pause_on_failure` is false, THE engine SHALL NOT pause the schedule regardless of workflow outcomes.
3. THE ScheduleExecutionEngine SHALL periodically reconcile `ScheduleInfo.running_workflows` by querying the runtime for workflow execution status, removing entries that have reached a terminal state (completed, failed, terminated, cancelled, timed out).
4. WHEN a running workflow reaches a terminal state, THE engine SHALL update `ScheduleInfo.running_workflows` to remove it, and drain any buffered actions that were waiting for the workflow to complete.
5. THE reconciliation interval SHALL be configurable (default: same as tick interval) and SHALL NOT block the main evaluation loop.

---

## Phase 3: Operational Handlers

### Requirement 10: patch_schedule Handler

**User Story:** As a Temporal SDK user, I want to patch a schedule via the `patch_schedule` gRPC endpoint, so that I can trigger immediate actions, pause, unpause, or backfill without replacing the entire schedule definition.

#### Acceptance Criteria

1. WHEN the `patch_schedule` endpoint is called with `trigger_immediately` set, THE handler SHALL enqueue an immediate action for the schedule, respecting the overlap policy (or the override policy if specified in the trigger request).
2. WHEN the `patch_schedule` endpoint is called with `backfill_request` entries, THE handler SHALL compute all matching times within each backfill time range and enqueue actions for each, respecting the overlap policy (or the override policy if specified).
3. WHEN the `patch_schedule` endpoint is called with `pause` set to a non-empty string, THE handler SHALL set `ScheduleState.paused` to true and `ScheduleState.notes` to the provided string.
4. WHEN the `patch_schedule` endpoint is called with `unpause` set to a non-empty string, THE handler SHALL set `ScheduleState.paused` to false and `ScheduleState.notes` to the provided string.
5. WHEN the schedule does not exist, THE handler SHALL return `NOT_FOUND`.
6. WHEN `trigger_immediately` or `backfill_request` actions result in a workflow start being attempted (not buffered), THE handler SHALL record the result in `ScheduleInfo.recent_actions` and increment `ScheduleInfo.action_count`. Actions that are buffered due to overlap policy SHALL NOT be recorded until they are actually executed.
7. WHEN `limited_actions` is true, triggered-immediately and backfill actions SHALL NOT decrement `remaining_actions` (only scheduled actions count against the limit).

### Requirement 11: list_schedules Handler

**User Story:** As a Temporal SDK user, I want to list schedules in a namespace via the `list_schedules` gRPC endpoint, so that I can discover and monitor existing schedules.

#### Acceptance Criteria

1. WHEN the `list_schedules` endpoint is called with a namespace, THE handler SHALL return a paginated list of `ScheduleListEntry` items for all schedules in that namespace.
2. EACH `ScheduleListEntry` SHALL include `schedule_id`, `memo`, `search_attributes`, and `ScheduleListInfo` (abbreviated spec, workflow type, notes, paused state, recent actions, future action times).
3. WHEN the request includes `maximum_page_size`, THE handler SHALL return at most that many entries per page.
4. WHEN more entries exist beyond the page, THE handler SHALL return a `next_page_token` that can be used to fetch the next page.
5. WHEN the request includes a `next_page_token`, THE handler SHALL return the next page of results starting after the previous page's last entry.
6. WHEN no schedules exist in the namespace, THE handler SHALL return an empty list with no next_page_token.

### Requirement 12: list_schedule_matching_times Handler

**User Story:** As a Temporal SDK user, I want to compute matching times for a schedule spec via the `list_schedule_matching_times` gRPC endpoint, so that I can preview when a schedule will fire without creating or modifying it.

#### Acceptance Criteria

1. WHEN the `list_schedule_matching_times` endpoint is called with a namespace, schedule_id, start_time, and end_time, THE handler SHALL retrieve the schedule's spec and compute all matching times within the specified range.
2. THE handler SHALL return the matching times as a list of timestamps.
3. WHEN the schedule does not exist, THE handler SHALL return `NOT_FOUND`.
4. WHEN start_time is after end_time, THE handler SHALL return an empty list.

---

## Phase 4: Integration

### Requirement 13: cron_schedule Field on WorkflowExecutionStarted

**User Story:** As an SDK user, I want `WorkflowExecutionStartedEventAttributes` to include the `cron_schedule` field for schedule-triggered workflows, so that the SDK can identify workflows that were started by a schedule.

#### Acceptance Criteria

1. WHEN the ScheduleExecutionEngine triggers a workflow start, THE engine SHALL set a `cron_schedule` field on the start request indicating the schedule ID that triggered it.
2. WHEN the History_Serializer serializes a `WorkflowExecutionStarted` event for a schedule-triggered workflow, THE History_Serializer SHALL populate the `cron_schedule` field from the kernel event data.
3. WHEN a workflow is not triggered by a schedule, THE History_Serializer SHALL leave `cron_schedule` empty.

### Requirement 14: Schedule-Triggered Starts Use Existing StartWorkflowExecution Path

**User Story:** As a Tokeira developer, I want schedule-triggered workflow starts to flow through the same `StartWorkflowExecution` code path as client-initiated starts, so that all workflow start validation, versioning, and routing logic applies uniformly.

#### Acceptance Criteria

1. WHEN the ScheduleExecutionEngine triggers a workflow start, THE engine SHALL construct a `StartRequest` from the `ScheduleAction.start_workflow` configuration and submit it through `TokeiraRuntime::start_workflow_with_policy()`, which is the same runtime entry point used by the edge gRPC handler after translation. This ensures ID-conflict/reuse policy handling matches SDK-initiated starts.
2. THE engine SHALL record the result (workflow execution info) in `ScheduleInfo.recent_actions` as a `ScheduleActionResult`, including the `start_workflow_status` field reflecting the outcome.
3. THE engine SHALL add the started workflow to `ScheduleInfo.running_workflows`.
4. WHEN a schedule-triggered start fails (e.g., due to workflow ID conflict with `REJECT_DUPLICATE` policy), THE engine SHALL record the failure in `ScheduleInfo.recent_actions` with the appropriate status and continue evaluating the schedule for future actions.

> **NOTE:** The execution engine lives in `tokeira-runtime` and calls `TokeiraRuntime::start_workflow_with_policy()` directly. It does NOT call through the edge gRPC handler (which would create a crate cycle). Versioning rule evaluation (assignment rules) is performed by the engine before calling `start_workflow_with_policy()` using the same `VersioningRuleStore::evaluate_assignment()` function that the edge layer uses. This ensures schedule-triggered starts get the same versioning behavior without requiring the edge crate. Schedules do not support pinned versioning overrides — schedule actions always use assignment rule evaluation (equivalent to `AutoUpgrade` behavior).

### Requirement 15: Proto Translation for Schedule Types

**User Story:** As a Tokeira developer, I want proto translation functions for all schedule-related types, so that the gRPC handlers can convert between proto messages and internal domain types.

#### Acceptance Criteria

1. THE Edge_Layer SHALL provide translation functions between proto `Schedule`, `ScheduleSpec`, `ScheduleAction`, `SchedulePolicies`, `ScheduleState`, `ScheduleInfo`, `SchedulePatch`, `ScheduleListEntry`, and their internal domain representations.
2. THE translation functions SHALL preserve all proto fields that have corresponding internal domain fields. The following fields are intentionally not round-tripped and are documented as lossy: `ScheduleSpec.timezone_data` (dropped on describe/list per proto documentation), original `CalendarSpec`/`cron_string` (compiled to `StructuredCalendarSpec` on ingest), and `NewWorkflowExecutionInfo` fields not modeled internally (headers, user_metadata — documented as unsupported in UNSUPPORTED_FIELDS.md). `NewWorkflowExecutionInfo.versioning_override` is intentionally not supported: schedules always use assignment rule evaluation (equivalent to `AutoUpgrade`); pinned versioning overrides on schedule actions are rejected with `INVALID_ARGUMENT`.
3. WHEN a proto field contains an invalid value (e.g., negative interval duration), THE translation function SHALL return a descriptive error rather than silently defaulting.
4. THE translation functions SHALL handle `CalendarSpec` and `cron_string` compilation into `StructuredCalendarSpec` on the create/update path.
5. THE translation functions SHALL handle `ScheduleListInfo` construction from full schedule data for the list endpoint (dropping `timezone_data` from the spec copy per proto documentation).
