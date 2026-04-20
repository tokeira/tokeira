# Implementation Plan: Edge Schedule Transport

## Overview

Implement the Schedule Transport layer — 7 gRPC handlers for Temporal's Schedule feature, plus the backing `ScheduleStore` and `ScheduleExecutionEngine` in `tokeira-runtime`. The schedule store is in-memory (`DashMap`) with conflict tokens for optimistic concurrency (same pattern as `VersioningRuleStore`). The execution engine is a background ticker that evaluates schedule specs and triggers workflow starts. Proto translation stays in `tokeira-edge`. The kernel stays pure.

Depends on: nothing (no dependencies on other umbrella features).

## Tasks

- [x] 1. ScheduleStore — core data structures and schedule store
  - [x] 1.1 Create schedule module with domain types
    - Create `crates/tokeira-runtime/src/schedule.rs`
    - Define `ScheduleId(pub String)` newtype with `Clone, Debug, PartialEq, Eq, Hash`
    - Define `ScheduleEntry` struct with all fields: schedule_id, namespace_id, spec, action, policies, state, info, memo, search_attributes, conflict_token
    - Define `ScheduleSpec` struct with structured_calendars, intervals, exclude_calendars, start_time, end_time, jitter, timezone_name
    - Define `StructuredCalendarSpec` struct with second, minute, hour, day_of_month, month, year, day_of_week, comment
    - Define `Range` struct with start, end, step
    - Define `IntervalSpec` struct with interval, phase
    - Define `ScheduleAction` struct with `start_workflow: StartWorkflowAction`
    - Define `StartWorkflowAction` struct with workflow_id, workflow_type, task_queue, input, timeouts, retry_policy, memo, search_attributes
    - Define `SchedulePolicies` struct with overlap_policy, catchup_window, pause_on_failure, keep_original_workflow_id
    - Define `OverlapPolicy` enum: Skip, BufferOne, BufferAll, CancelOther, TerminateOther, AllowAll
    - Define `ScheduleState` struct with notes, paused, limited_actions, remaining_actions
    - Define `ScheduleInfo` struct with action_count, missed_catchup_window, overlap_skipped, buffer_dropped, buffer_size, buffered_actions, running_workflows, recent_actions, future_action_times, create_time, update_time
    - Define `BufferedAction` struct with nominal_time, overlap_policy_override
    - Define `WorkflowExecution` struct with workflow_id, run_id, run_key (RunKey needed for reconciliation and cancel/terminate)
    - Define `ScheduleActionResult` struct with schedule_time, actual_time, start_workflow_result, start_workflow_status
    - Define `WorkflowExecutionStatus` enum: Running, Completed, Failed, Cancelled, Terminated, ContinuedAsNew, TimedOut, StartFailed
    - Define `ScheduleError` enum: AlreadyExists, NotFound, StaleConflictToken, InvalidArgument(String)
    - Add `pub mod schedule;` to `crates/tokeira-runtime/src/lib.rs`
    - _Requirements: 1.1, 1.2, 1.6_

  - [x] 1.2 Implement ScheduleStore with DashMap
    - Implement `ScheduleStore` with `DashMap<(NamespaceId, ScheduleId), ScheduleEntry>`
    - Implement `create(&self, entry: ScheduleEntry) -> Result<Vec<u8>, ScheduleError>` — inserts new entry, initializes conflict token to `1_u64.to_be_bytes()`, returns token; errors with `AlreadyExists` if key exists
    - Implement `describe(&self, ns, id) -> Result<ScheduleEntry, ScheduleError>` — returns full entry clone; errors with `NotFound` if absent
    - Implement `update(&self, ns, id, token, updater: impl FnOnce(&mut ScheduleEntry)) -> Result<ScheduleEntry, ScheduleError>` — validates token (empty = unconditional, non-empty must match), applies updater closure, increments token, returns updated entry
    - Implement `delete(&self, ns, id) -> Result<(), ScheduleError>` — removes entry; errors with `NotFound` if absent
    - Implement `list(&self, ns, page_size, page_token) -> (Vec<ScheduleEntry>, Option<Vec<u8>>)` — paginated listing for a namespace, sorted by schedule_id for deterministic pagination
    - Implement `all_active_schedules(&self) -> Vec<ScheduleEntry>` — returns all non-paused schedules for engine tick
    - Conflict token encoded as `(counter as u64).to_be_bytes().to_vec()`, incremented by 1 on each mutation
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_


- [ ] 2. Property tests for schedule store
  - [ ]* 2.1 Write property test for schedule store CRUD correctness (Property 1)
    - Generate random sequences of create, update, and delete operations
    - Apply operations to `ScheduleStore` and a reference model (HashMap)
    - Verify: (a) create then describe returns same data; (b) create with existing ID returns `AlreadyExists`; (c) describe/update/delete on non-existent returns `NotFound`; (d) delete causes subsequent describe to return `NotFound`
    - Tag: `// Feature: edge-schedule-transport, Property 1: Schedule store CRUD correctness`
    - **Property 1: Schedule store CRUD correctness**
    - **Validates: Requirements 1.1, 1.7, 2.1, 2.2, 3.1, 3.2, 4.1, 4.4, 5.1, 5.2**

  - [x]* 2.2 Write property test for conflict token monotonicity (Property 2)
    - Generate random sequences of successful mutations (updates) on a single schedule entry
    - Capture conflict token after each mutation
    - Verify tokens are strictly increasing (each > previous as u64)
    - Attempt mutation with stale token, verify rejection with `StaleConflictToken`
    - Attempt mutation with empty token, verify unconditional success
    - Tag: `// Feature: edge-schedule-transport, Property 2: Conflict token monotonicity`
    - **Property 2: Conflict token monotonicity and optimistic concurrency**
    - **Validates: Requirements 1.2, 1.3, 1.4, 1.5, 4.2, 4.3**

- [ ] 3. Checkpoint — Ensure schedule store tests pass
  - Run `cargo test -p tokeira-runtime` and verify all property and unit tests pass

- [x] 4. MatchingTimesComputation — pure function for computing action times
  - [x] 4.1 Implement compute_matching_times function
    - Add `chrono` and `chrono-tz` dependencies to `tokeira-runtime/Cargo.toml` for IANA timezone support (calendar spec interpretation in non-UTC timezones)
    - Add `cron` crate dependency (or implement inline cron parser) for `cron_string` compilation in the proto translation layer — note: cron compilation happens at ingest time in `tokeira-edge`, producing `StructuredCalendarSpec`; the runtime only works with compiled specs
    - Implement `compute_matching_times(spec, range_start, range_end, schedule_id) -> Vec<OffsetDateTime>`
    - For `structured_calendars`: iterate time range, match timestamps where all fields of at least one `StructuredCalendarSpec` match. Use `chrono-tz` to interpret calendar fields in the spec's timezone before converting to UTC.
    - For `intervals`: compute timestamps of the form `epoch + n * interval + phase` within range
    - Union calendar and interval results
    - Exclude timestamps matching `exclude_calendars`
    - Exclude timestamps before `start_time` or after `end_time`
    - Apply jitter: deterministic random offset in `[0, jitter]` using `schedule_id + nominal_time` as seed
    - Handle `timezone_name` for calendar interpretation via `chrono-tz` lookup; if timezone is empty or unset, default to UTC
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 6.9_

  - [x] 4.2 Implement compute_next_times helper
    - Implement `compute_next_times(spec, now, count, schedule_id) -> Vec<OffsetDateTime>`
    - Compute next N action times from `now` by iterating forward until `count` results found
    - Used by `describe_schedule` to populate `future_action_times` (next 10)
    - _Requirements: 3.3, 6.1_

  - [x] 4.3 Implement overlap policy decision function
    - Implement `decide_overlap(policy, running_workflows, current_buffer_size) -> OverlapDecision`
    - Define `OverlapDecision` enum: Allow, Skip, Buffer, CancelOther(Vec<WorkflowExecution>), TerminateOther(Vec<WorkflowExecution>)
    - Logic: empty running_workflows → always Allow; SKIP → Skip; BUFFER_ONE with buffer < 1 → Buffer, else Skip; BUFFER_ALL → Buffer; CANCEL_OTHER → CancelOther(running); TERMINATE_OTHER → TerminateOther(running); ALLOW_ALL → Allow
    - _Requirements: 7.4, 7.5, 7.6, 7.7, 7.8, 7.9_

  - [x] 4.4 Implement schedule_workflow_id function
    - Implement `schedule_workflow_id(base_workflow_id, nominal_time, keep_original) -> String`
    - If `keep_original` is true, return base_workflow_id unchanged
    - If false, return `format!("{}-{}", base_workflow_id, nominal_time.unix_timestamp())`
    - _Requirements: 8.1, 8.2, 8.3_

- [ ] 5. Property tests for matching times and pure functions
  - [x]* 5.1 Write property test for matching times range containment and monotonicity (Property 3)
    - Generate random valid `ScheduleSpec` values and time ranges
    - Verify all returned timestamps are within `[range_start, range_end]`
    - Generate sub-ranges, verify sub-range results are a subset of full-range results
    - Tag: `// Feature: edge-schedule-transport, Property 3: Matching times range containment and monotonicity`
    - **Property 3: Matching times range containment and monotonicity**
    - **Validates: Requirements 6.1, 6.6, 6.7, 6.10**

  - [ ]* 5.2 Write property test for matching times union and exclusion correctness (Property 4)
    - Generate specs with both calendar and interval entries
    - Compute matching times for full spec, calendar-only spec, interval-only spec
    - Verify full result equals union of calendar-only and interval-only minus exclusions
    - Tag: `// Feature: edge-schedule-transport, Property 4: Matching times union and exclusion correctness`
    - **Property 4: Matching times union and exclusion correctness**
    - **Validates: Requirements 6.2, 6.3, 6.4, 6.5**

  - [x]* 5.3 Write property test for jitter determinism and bounds (Property 5)
    - Generate random specs with jitter set, random schedule IDs and nominal times
    - Compute jittered time twice with same inputs, verify identical results
    - Verify offset is in `[0, jitter]` for all results
    - Tag: `// Feature: edge-schedule-transport, Property 5: Jitter determinism and bounds`
    - **Property 5: Jitter determinism and bounds**
    - **Validates: Requirements 6.8**

  - [x]* 5.4 Write property test for overlap policy decision correctness (Property 6)
    - Generate random policy/running_workflows/buffer_size combinations
    - Verify: SKIP with non-empty running → Skip; BUFFER_ONE with buffer < 1 → Buffer; BUFFER_ONE with buffer >= 1 → Skip; BUFFER_ALL → Buffer; ALLOW_ALL → Allow; CANCEL_OTHER → CancelOther(running); TERMINATE_OTHER → TerminateOther(running); empty running → Allow for all policies
    - Tag: `// Feature: edge-schedule-transport, Property 6: Overlap policy decision correctness`
    - **Property 6: Overlap policy decision correctness**
    - **Validates: Requirements 7.4, 7.5, 7.6, 7.7, 7.8, 7.9**

  - [x]* 5.5 Write property test for workflow ID generation determinism (Property 7)
    - Generate random base workflow IDs and nominal times
    - Call `schedule_workflow_id` twice with same inputs, verify identical results
    - When `keep_original` is false, verify result differs from base ID
    - When `keep_original` is true, verify result equals base ID
    - Tag: `// Feature: edge-schedule-transport, Property 7: Workflow ID generation determinism`
    - **Property 7: Workflow ID generation determinism**
    - **Validates: Requirements 8.1, 8.2, 8.3**

- [ ] 6. Checkpoint — Ensure matching times and pure function tests pass
  - Run `cargo test -p tokeira-runtime` and verify all tests pass


- [x] 7. ScheduleExecutionEngine — background ticker
  - [x] 7.1 Implement run_schedule_engine background loop
    - Implement `ScheduleEngineConfig` with `tick_interval: tokio::time::Duration` (default 1 second)
    - Implement `run_schedule_engine(store, runtime, config, cancel)` async function
    - Use `CancellationToken` + `tokio::select!` loop pattern (same as `run_timer_scanner`)
    - On each tick: call `evaluate_all_schedules(&store, &runtime, last_tick, now)`
    - Track `last_tick` to determine which actions are due since last evaluation
    - _Requirements: 7.1, 7.11_

  - [x] 7.2 Implement evaluate_all_schedules logic
    - Iterate `store.all_active_schedules()`
    - For each schedule: compute matching times in `[last_tick, now]` using `compute_matching_times`
    - For each due action time: check catchup window — if `now - action_time > catchup_window`, skip and increment `missed_catchup_window`
    - For each valid due action: call `decide_overlap` to determine action
    - On `Allow`: trigger workflow start, record in `recent_actions`, add to `running_workflows`, increment `action_count`
    - On `Skip`: increment `overlap_skipped`
    - On `Buffer`: push `BufferedAction { nominal_time, overlap_policy_override }` to `buffered_actions` queue (BUFFER_ONE caps at 1 entry, drops oldest if full and increments `buffer_dropped`)
    - On `CancelOther`/`TerminateOther`: cancel/terminate running workflows, then start new action
    - Check `limited_actions`: if true and `remaining_actions` reaches 0, stop triggering
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10_

  - [x] 7.3 Implement schedule-triggered workflow start
    - Construct `StartRequest` (runtime-level DTO) from `ScheduleAction.start_workflow`
    - Generate workflow ID via `schedule_workflow_id(base_id, nominal_time, keep_original)`
    - Evaluate versioning assignment rules via `VersioningRuleStore::evaluate_assignment()` and set `build_id` on `StartRequest`
    - Set `cron_schedule` to schedule_id on the `StartRequest`
    - Call `TokeiraRuntime::start_workflow_with_policy(start_request)` directly (NOT the edge gRPC handler — avoids crate cycle). Uses `start_workflow_with_policy` to get the same ID-conflict/reuse behavior as SDK-initiated starts. The conflict policy is a field on `StartRequest`.
    - Record result as `ScheduleActionResult` with `start_workflow_status` in `ScheduleInfo.recent_actions` (keep last 10)
    - Add started workflow to `ScheduleInfo.running_workflows`
    - On start failure (e.g., workflow ID conflict): record with `start_workflow_status = StartFailed`, continue evaluating
    - _Requirements: 7.11, 8.1, 8.2, 8.3, 14.1, 14.2, 14.3, 14.4_

  - [x] 7.4 Implement workflow completion reconciliation
    - Implement `reconcile_running_workflows(store, runtime)` — called each tick before evaluation
    - For each schedule with non-empty `running_workflows`: query runtime for workflow execution status
    - Remove workflows that reached terminal state (completed, failed, terminated, cancelled, timed out)
    - When a workflow completes and `buffered_actions` is non-empty: pop front action and trigger it
    - When a workflow fails and `pause_on_failure` is true: set `ScheduleState.paused = true`, update `ScheduleState.notes`
    - When `pause_on_failure` is false: do not pause regardless of outcome
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

- [x] 8. Proto translation for schedule types
  - [x] 8.1 Create schedule proto translation module
    - Create `crates/tokeira-edge/src/translate/schedule.rs`
    - Add `pub mod schedule;` to `crates/tokeira-edge/src/translate/mod.rs`
    - Implement `schedule_spec_to_domain(proto) -> Result<ScheduleSpec, Status>` — convert proto `ScheduleSpec` to internal, validate fields (negative intervals → error)
    - Implement `schedule_spec_to_proto(domain) -> proto::ScheduleSpec` — convert internal to proto
    - Implement `compile_calendar_spec(proto) -> Result<StructuredCalendarSpec, Status>` — compile `CalendarSpec` and `cron_string` into `StructuredCalendarSpec`
    - Implement `schedule_action_to_domain(proto) -> Result<ScheduleAction, Status>`
    - Implement `schedule_action_to_proto(domain) -> proto::ScheduleAction`
    - Implement `schedule_policies_to_domain(proto) -> SchedulePolicies`
    - Implement `schedule_policies_to_proto(domain) -> proto::SchedulePolicies`
    - Implement `schedule_state_to_domain(proto) -> ScheduleState`
    - Implement `schedule_state_to_proto(domain) -> proto::ScheduleState`
    - Implement `schedule_info_to_proto(domain) -> proto::ScheduleInfo`
    - _Requirements: 15.1, 15.2, 15.3, 15.4_

  - [x] 8.2 Implement request/response translation functions
    - Implement `create_schedule_request_to_edge(proto) -> Result<(NamespaceId, ScheduleId, ScheduleEntry, Option<SchedulePatch>), Status>` — parse `CreateScheduleRequest`, validate schedule_id non-empty, validate spec and action present; reject `versioning_override` on action with `INVALID_ARGUMENT`
    - Implement `update_schedule_request_to_edge(proto) -> Result<(NamespaceId, ScheduleId, Vec<u8>, ScheduleEntry), Status>` — parse `UpdateScheduleRequest`; reject `versioning_override` on action with `INVALID_ARGUMENT`
    - Implement `patch_schedule_request_to_edge(proto) -> Result<(NamespaceId, ScheduleId, SchedulePatch), Status>` — parse `PatchScheduleRequest`
    - Implement `describe_schedule_response_to_proto(entry) -> DescribeScheduleResponse`
    - Implement `list_schedules_response_to_proto(entries, next_page_token) -> ListSchedulesResponse` — build `ScheduleListEntry` items, drop `timezone_data` from spec copy per proto docs
    - Implement `matching_times_response_to_proto(times) -> ListScheduleMatchingTimesResponse`
    - _Requirements: 15.1, 15.2, 15.5_

  - [x] 8.3 Update UNSUPPORTED_FIELDS.md for schedule types
    - Add schedule-specific lossy/unsupported fields: `ScheduleSpec.timezone_data` (dropped on describe/list), `CalendarSpec`/`cron_string` (compiled to StructuredCalendarSpec), `NewWorkflowExecutionInfo.header`, `NewWorkflowExecutionInfo.user_metadata`, `NewWorkflowExecutionInfo.versioning_override` (rejected — schedules use assignment rules)
    - Remove `StartWorkflowExecutionRequest.cron_schedule` from unsupported list (now populated by schedule-triggered starts)
    - _Requirements: 15.2_

- [ ] 9. Property tests for pagination and proto translation
  - [ ]* 9.1 Write property test for pagination completeness (Property 8)
    - Generate random sets of schedules in a namespace (varying sizes)
    - Generate random page sizes
    - Iterate through all pages using `next_page_token`
    - Verify every schedule appears exactly once, no duplicates, no omissions
    - Tag: `// Feature: edge-schedule-transport, Property 8: Pagination completeness`
    - **Property 8: Pagination completeness**
    - **Validates: Requirements 11.1, 11.3, 11.4, 11.5**

  - [ ]* 9.2 Write property test for proto translation round-trip (Property 9)
    - Generate random valid internal `ScheduleSpec`, `ScheduleAction`, `SchedulePolicies`, `ScheduleState`, `ScheduleInfo` values
    - Convert to proto and back to domain
    - Verify round-trip produces equivalent values (all fields preserved)
    - Tag: `// Feature: edge-schedule-transport, Property 9: Proto translation round-trip`
    - **Property 9: Proto translation round-trip**
    - **Validates: Requirements 15.1, 15.2**

- [ ] 10. Checkpoint — Ensure proto translation tests pass
  - Run `cargo test -p tokeira-edge` and `cargo test -p tokeira-runtime` to verify all tests pass


- [x] 11. gRPC handlers — CRUD (Phase 1)
  - [x] 11.1 Implement create_schedule handler
    - Replace the `Err(Status::unimplemented(...))` stub in `workflow_service.rs`
    - Extract namespace, schedule_id from request via `create_schedule_request_to_edge()`
    - Validate: empty schedule_id → `INVALID_ARGUMENT`; missing spec or action → `INVALID_ARGUMENT`
    - Initialize `ScheduleInfo` with zero counters, empty recent_actions, `create_time = now`
    - Initialize `ScheduleState` from request (or defaults: not paused, no limited actions)
    - Call `ScheduleStore::create(entry)`
    - If `initial_patch` is present, apply patch to newly created schedule (trigger_immediately, pause, etc.)
    - Map `AlreadyExists` → gRPC `ALREADY_EXISTS`
    - Return conflict token in response
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [x] 11.2 Implement describe_schedule handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, schedule_id from request
    - Call `ScheduleStore::describe(ns, id)`
    - Compute `future_action_times` via `compute_next_times(spec, now, 10, schedule_id)`
    - Map `NotFound` → gRPC `NOT_FOUND`
    - Return full schedule via `describe_schedule_response_to_proto()`
    - _Requirements: 3.1, 3.2, 3.3, 3.4_

  - [x] 11.3 Implement update_schedule handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, schedule_id, conflict_token, new schedule definition via `update_schedule_request_to_edge()`
    - Call `ScheduleStore::update(ns, id, token, updater)` — updater replaces spec, action, policies, state, search_attributes and sets `info.update_time = now`
    - Map `NotFound` → `NOT_FOUND`, `StaleConflictToken` → `FAILED_PRECONDITION`
    - Return empty `UpdateScheduleResponse` (upstream proto response is empty; callers use DescribeSchedule to fetch updated state)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [x] 11.4 Implement delete_schedule handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, schedule_id from request
    - Call `ScheduleStore::delete(ns, id)`
    - Map `NotFound` → `NOT_FOUND`
    - Return success response
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 11.5 Wire ScheduleStore into WorkflowService
    - Add `schedule_store: Arc<ScheduleStore>` field to `WorkflowService`
    - Initialize in `WorkflowService::new()` and related constructors
    - Pass through `WorkflowServiceGrpc` to handlers
    - _Requirements: 1.6_

- [x] 12. gRPC handlers — Operational (Phase 3)
  - [x] 12.1 Implement patch_schedule handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, schedule_id, patch via `patch_schedule_request_to_edge()`
    - Call `ScheduleStore::describe()` to verify existence → `NOT_FOUND` if absent
    - If `trigger_immediately` set: compute overlap decision; if `Allow` → start workflow immediately, record in `recent_actions`, increment `action_count`; if `Buffer` → push to `buffered_actions` queue (do NOT record in `recent_actions` until actually executed); if `Skip` → increment `overlap_skipped`
    - If `backfill_request` entries present: for each backfill range, compute matching times via `compute_matching_times`, for each time apply overlap decision — only record in `recent_actions` and increment `action_count` when a workflow start is actually attempted (buffered actions are recorded when drained)
    - If `pause` set (non-empty string): set `state.paused = true`, `state.notes = pause_string`
    - If `unpause` set (non-empty string): set `state.paused = false`, `state.notes = unpause_string`
    - Trigger-immediately and backfill actions do NOT decrement `remaining_actions`
    - Update store via `ScheduleStore::update()`
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_

  - [x] 12.2 Implement list_schedules handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, maximum_page_size, next_page_token from request
    - Call `ScheduleStore::list(ns, page_size, page_token)`
    - Build response via `list_schedules_response_to_proto()` with `ScheduleListEntry` items
    - Return `next_page_token` if more entries exist
    - Return empty list with no token if no schedules in namespace
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6_

  - [x] 12.3 Implement list_schedule_matching_times handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, schedule_id, start_time, end_time from request
    - Call `ScheduleStore::describe()` to get schedule spec → `NOT_FOUND` if absent
    - If `start_time > end_time`, return empty list
    - Call `compute_matching_times(spec, start_time, end_time, schedule_id)`
    - Return times via `matching_times_response_to_proto()`
    - _Requirements: 12.1, 12.2, 12.3, 12.4_

- [ ] 13. Checkpoint — Ensure all handler tests pass
  - Run `cargo test -p tokeira-edge` and `cargo test -p tokeira-runtime` to verify all tests pass

- [x] 14. Integration wiring (Phase 4)
  - [x] 14.1 Add cron_schedule field to kernel StartRequest
    - Add `pub cron_schedule: Option<String>` field to `StartRequest` in `tokeira-kernel`
    - Default to `None` for all existing start paths
    - The execution engine sets this to `Some(schedule_id)` when triggering a start
    - _Requirements: 13.1_

  - [x] 14.2 Wire cron_schedule through history serializer
    - In the history serializer, when emitting `WorkflowExecutionStartedEventAttributes`: populate `cron_schedule` field from kernel event data
    - When workflow is not schedule-triggered (field is None): leave `cron_schedule` empty in proto
    - _Requirements: 13.2, 13.3_

  - [x] 14.3 Wire ScheduleStore and ScheduleExecutionEngine into runtime construction
    - Create a single `Arc<ScheduleStore>` in the application assembly point
    - Pass to both `WorkflowService::new()` (CRUD handlers) and `ScheduleExecutionEngine` (background evaluation)
    - Spawn `run_schedule_engine` as a background task with the shared store and runtime reference
    - Wire `CancellationToken` for graceful shutdown
    - _Requirements: 7.1, 14.1_

- [x] 15. Unit tests for edge cases and integration
  - [x]* 15.1 Write unit tests for schedule store edge cases
    - `test_create_with_initial_patch`: create with trigger_immediately patch, verify action recorded
    - `test_create_initializes_info`: create schedule, verify zero counters and create_time set
    - `test_create_default_state`: create without explicit state, verify defaults (not paused, no limited actions)
    - `test_empty_schedule_id_rejected`: empty schedule_id returns INVALID_ARGUMENT
    - `test_missing_spec_rejected`: missing spec returns INVALID_ARGUMENT
    - `test_describe_includes_future_times`: describe returns computed future_action_times (10 entries)
    - `test_update_sets_update_time`: update sets ScheduleInfo.update_time to current timestamp
    - `test_delete_stops_engine_evaluation`: delete schedule, verify engine skips it on next tick
    - _Requirements: 2.3, 2.5, 2.6, 2.7, 2.8, 3.3, 4.6, 5.3_

  - [x]* 15.2 Write unit tests for matching times and execution engine
    - `test_timezone_calendar_matching`: calendar spec in America/New_York produces correct UTC times
    - `test_catchup_window_triggers_missed`: past action within catchup window is triggered
    - `test_catchup_window_skips_old`: past action outside catchup window is skipped, missed_catchup_window incremented
    - `test_limited_actions_stops_at_zero`: remaining_actions=0 stops triggering
    - `test_engine_uses_start_workflow_path`: engine calls StartWorkflowExecution internal path
    - `test_pause_on_failure`: workflow failure pauses schedule when pause_on_failure is true
    - `test_backfill_computes_correct_times`: backfill patch triggers correct number of actions
    - `test_manual_triggers_dont_decrement`: trigger-immediately doesn't decrement remaining_actions
    - `test_matching_times_empty_for_inverted_range`: start > end returns empty list
    - _Requirements: 6.9, 7.2, 7.3, 7.10, 7.11, 9.1, 10.2, 10.7, 12.4_

  - [x]* 15.3 Write unit tests for gRPC handler responses and proto translation
    - `test_list_empty_namespace`: empty namespace returns empty list with no next_page_token
    - `test_cron_schedule_field_set`: schedule-triggered start has cron_schedule set to schedule_id
    - `test_non_schedule_start_empty_cron`: normal start has empty cron_schedule
    - `test_invalid_proto_returns_error`: negative duration in proto returns descriptive INVALID_ARGUMENT error
    - `test_list_info_drops_timezone_data`: ScheduleListInfo omits timezone_data from spec copy
    - _Requirements: 11.6, 13.1, 13.3, 15.3, 15.5_

- [ ] 16. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-edge`, `cargo test -p tokeira-runtime`, and `cargo lint` to verify everything passes

## Notes

- The `ScheduleStore` uses `DashMap` (same dependency as `VersioningRuleStore`) for lock-free concurrent reads
- All property-based tests use `proptest` (already a project dependency) with minimum 100 iterations
- Each property test is tagged with `// Feature: edge-schedule-transport, Property N: <title>`
- The execution engine uses the `CancellationToken` + `tokio::select!` loop pattern (same as `run_timer_scanner`)
- Durable persistence of schedule entries is deferred to the DSQL storage spec
- The kernel stays pure — schedule evaluation and execution are edge/runtime-layer concerns
- Schedule-triggered workflow starts use the existing `StartWorkflowExecution` path
- Tasks reference specific requirements for traceability
- Tasks marked with `*` are optional and can be skipped for faster MVP
