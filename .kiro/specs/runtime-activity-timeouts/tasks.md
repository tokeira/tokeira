# Implementation Plan: Activity Heartbeat and Timeouts

## Overview

Implement activity heartbeat recording and four timeout detection mechanisms in `tokeira-runtime`. This adds `ActivityTrackingState` (in-memory per-activity timestamps and cancellation status), a `record_activity_heartbeat` facade method, a pure `evaluate_activity_timeout` function, and an `ActivityTimeoutScanner` background task. Existing lifecycle methods are updated to populate and clean up tracking state.

All code lives in `tokeira-runtime`, in a new `activity_timeout.rs` module (following the recent crate refactoring that split runtime.rs into focused modules). The kernel already supports `ActivityResolution::TimedOut { timeout_type }` — no kernel changes needed.

## Tasks

- [x] 1. Add dependencies and define core types
  - [x] 1.1 Add `tokio-util` dependency to `tokeira-runtime/Cargo.toml` for `CancellationToken`
    - Add `tokio-util = { version = "0.7", features = ["rt"] }` to `[dependencies]`
    - _Requirements: 8.1, 8.2_

  - [x] 1.2 Implement `ActivityTrackingEntry` and `ActivityTrackingState` in a new `activity_timeout.rs` module
    - Define `ActivityTrackingEntry` struct with fields: `run_key`, `activity_id`, `original_scheduled_at`, `last_dispatched_at`, `started_at`, `last_heartbeat_at`, `cancel_requested`
    - Define `ActivityTrackingState` with `Arc<Mutex<HashMap<(RunKey, String), ActivityTrackingEntry>>>`
    - Implement methods: `record_scheduled` (sets both `original_scheduled_at` and `last_dispatched_at`), `record_retry` (updates `last_dispatched_at`, clears `started_at` and `last_heartbeat_at`, preserves `original_scheduled_at`), `record_started`, `record_heartbeat`, `mark_cancel_requested`, `is_cancel_requested`, `remove`, `snapshot`
    - Add `pub mod activity_timeout;` to `lib.rs` and re-export public types
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [x] 1.3 Write property tests for `ActivityTrackingState`
    - **Property 5: Dispatch publication records scheduled_at in tracking state**
    - **Validates: Requirements 2.1**
    - Generate random `RunKey`, `activity_id`, and `OffsetDateTime`; call `record_scheduled`; assert entry exists with `scheduled_at` set and `started_at == None`

    - **Property 6: Activity resolution removes tracking entry**
    - **Validates: Requirements 2.4**
    - Generate random entries, call `record_scheduled` then `remove`; assert entry is gone from snapshot

  - [x] 1.4 Implement `TimeoutViolation` enum and `evaluate_activity_timeout` pure function
    - Define `TimeoutViolation` enum: `ScheduleToClose`, `ScheduleToStart`, `StartToClose`, `Heartbeat`
    - Implement `evaluate_activity_timeout(entry: &ActivityTrackingEntry, activity: &ActivityState, now: OffsetDateTime) -> Option<TimeoutViolation>`
    - Follow precedence: schedule-to-close first, then heartbeat (started only), start-to-close (started only), schedule-to-start (unstarted only)
    - _Requirements: 3.1, 3.2, 3.3, 4.1, 4.2, 5.1, 5.2, 6.1, 6.2_

  - [x] 1.5 Write property tests for `evaluate_activity_timeout`
    - **Property 7: Heartbeat timeout fires for started activities with expired heartbeat**
    - **Validates: Requirements 3.1, 3.2, 3.3**
    - Generate random `heartbeat_timeout`, `elapsed`, `has_heartbeat`, `has_started`; verify heartbeat timeout fires only when started and elapsed > timeout (unless schedule-to-close takes precedence); verify `started_at == None` never returns `Heartbeat`

    - **Property 8: Schedule-to-start timeout fires only for unstarted activities**
    - **Validates: Requirements 4.1, 4.2**
    - Generate random `schedule_to_start_timeout`, `elapsed`, `started_at`; verify fires only when `started_at == None` and elapsed > timeout (unless schedule-to-close takes precedence); verify `started_at == Some(_)` never returns `ScheduleToStart`

    - **Property 9: Start-to-close timeout fires only for started activities**
    - **Validates: Requirements 5.1, 5.2**
    - Generate random `start_to_close_timeout`, `elapsed`, `started_at`; verify fires only when `started_at == Some(_)` and elapsed > timeout (unless higher-precedence timeout fires); verify `started_at == None` never returns `StartToClose`

    - **Property 10: Schedule-to-close timeout fires regardless of start state**
    - **Validates: Requirements 6.1**
    - Generate random `schedule_to_close_timeout`, `elapsed`, `started_at`; verify fires when elapsed > timeout regardless of start state

    - **Property 11: Schedule-to-close takes precedence over all other timeouts**
    - **Validates: Requirements 6.2**
    - Generate entries where schedule-to-close fires AND at least one other timeout fires; verify result is always `ScheduleToClose`

    - **Property 12: No timeout fires when no timeout is configured**
    - **Validates: Requirements 3.1, 4.1, 5.1, 6.1 (inverse)**
    - Generate random entries with all four timeout fields set to `None`; verify result is always `None`

- [x] 2. Checkpoint - Verify core types and pure function
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Implement `record_activity_heartbeat` and lifecycle hooks
  - [x] 3.1 Add `activity_tracking: ActivityTrackingState` field to `TokeiraRuntime` and `RuntimeDispatchPublisher`
    - Update `TokeiraRuntime` struct to include `activity_tracking`, `scanner_handle: Option<tokio::task::JoinHandle<()>>`, `scanner_cancel: tokio_util::sync::CancellationToken`
    - Update `RuntimeDispatchPublisher` struct to include `activity_tracking: ActivityTrackingState`
    - Update `TokeiraRuntime::new` to create shared `ActivityTrackingState` and pass it to each `RuntimeDispatchPublisher`
    - _Requirements: 2.6, 8.1_

  - [x] 3.2 Implement `record_activity_heartbeat` on `TokeiraRuntime`
    - Validate token via existing `validate_activity_token`
    - Update `last_heartbeat_at` in `ActivityTrackingState`
    - Read and return `cancel_requested` from `ActivityTrackingState`
    - If token valid but entry missing from tracking, return `false` (no-op, benign race)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [x] 3.3 Write property tests for `record_activity_heartbeat`
    - **Property 1: Heartbeat cancellation indicator reflects tracking state**
    - **Validates: Requirements 1.3, 1.4**
    - Set up `ActivityTrackingState` with `cancel_requested` set to random bool; call heartbeat; assert return value matches `cancel_requested`

    - **Property 2: Heartbeat updates last_heartbeat_at**
    - **Validates: Requirements 1.2, 2.3**
    - Call heartbeat with valid token; assert `last_heartbeat_at` in tracking state is >= time before call

    - **Property 3: Stale heartbeat token rejection preserves tracking state**
    - **Validates: Requirements 1.5**
    - Call heartbeat with invalid token (attempt mismatch, etc.); assert error returned and tracking state unchanged

    - **Property 4: Heartbeat produces no kernel commands**
    - **Validates: Requirements 1.6**
    - Call heartbeat; assert no `Command` was submitted to the lane (use mock or command counter)

  - [x] 3.4 Add `record_scheduled` hook in `RuntimeDispatchPublisher::publish`
    - In the `EnqueueActivityTask` arm, call `self.activity_tracking.record_scheduled(run_key, activity_id, OffsetDateTime::now_utc())`
    - _Requirements: 2.1_

  - [x] 3.5 Add `record_started` hook in `start_activity_task`
    - After successful commit in `start_activity_task`, call `self.activity_tracking.record_started(run_key, &activity_id, OffsetDateTime::now_utc())`
    - _Requirements: 2.2_

  - [x] 3.6 Add `remove` hooks in `complete_activity_task` and `fail_activity_task`
    - After successful completion, call `self.activity_tracking.remove(run_key, &activity_id)`
    - After terminal failure (exhausted retries), call `self.activity_tracking.remove(run_key, &activity_id)`
    - On retry, call `self.activity_tracking.record_retry(run_key, &activity_id, OffsetDateTime::now_utc())` to update `last_dispatched_at` and clear `started_at`/`last_heartbeat_at`
    - _Requirements: 2.4, 2.7_

  - [x] 3.7 Add `mark_cancel_requested` hook in lane dispatch path
    - After a successful commit in the lane, if the transition's history events contain `ActivityTaskCancelRequested`, call `activity_tracking.mark_cancel_requested(run_key, &activity_id)`
    - This requires passing `ActivityTrackingState` to the publisher or adding a post-commit callback
    - _Requirements: 2.5_

- [x] 4. Checkpoint - Verify heartbeat and lifecycle hooks
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement `ActivityTimeoutScanner` and wire into runtime lifecycle
  - [x] 5.1 Implement `ActivityTimeoutScannerConfig` with defaults
    - `scan_interval: tokio::time::Duration` (default 1s)
    - `max_timeouts_per_scan: usize` (default 100)
    - _Requirements: 7.2, 7.6_

  - [x] 5.2 Implement the scanner background task loop
    - Sleep for `scan_interval`, take snapshot of `ActivityTrackingState`
    - For each entry (up to `max_timeouts_per_scan`): load run from storage, get `ActivityState`, call `evaluate_activity_timeout`
    - If activity no longer in run's activities map, remove from tracking and skip
    - If timeout detected, submit `Command::ActivityResolved(ActivityResolvedRequest { activity_id, resolution: ActivityResolution::TimedOut { timeout_type }, ... })` via lane submit
    - On success or kernel rejection, remove entry from tracking
    - On transient storage error, log at warn and continue
    - On lane submission error, log at warn and continue (entry stays for next cycle)
    - Use `CancellationToken` for graceful shutdown
    - _Requirements: 7.1, 7.3, 7.4, 7.5, 7.6, 8.3_

  - [x] 5.3 Spawn scanner in `TokeiraRuntime::new` and wire shutdown
    - Create `CancellationToken`, spawn scanner task, store `JoinHandle` and cancel token
    - On shutdown (or drop), cancel the token
    - _Requirements: 8.1, 8.2_

  - [x] 5.4 Write property tests for scanner behavior
    - **Property 13: Scanner batch bound**
    - **Validates: Requirements 7.6**
    - Set up `N > max_timeouts_per_scan` timed-out entries; run one scan cycle; assert at most `max_timeouts_per_scan` commands submitted

    - **Property 14: Scanner resilience to kernel rejections**
    - **Validates: Requirements 7.5**
    - Configure mock storage/lane to reject `ActivityResolved` commands; run scan cycle; assert scanner continues without crashing and processes remaining entries

    - **Property 15: Scanner resilience to transient storage errors**
    - **Validates: Requirements 8.3**
    - Configure mock storage to return errors on `load_run`; run scan cycle; assert scanner logs and continues to next entry without crashing

- [x] 6. Checkpoint - Verify scanner and full feature
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Update module exports and write integration tests
  - [x] 7.1 Update `tokeira-runtime/src/lib.rs` re-exports
    - Export `ActivityTrackingState`, `ActivityTrackingEntry`, `ActivityTimeoutScannerConfig`, `TimeoutViolation`, `evaluate_activity_timeout`
    - _Requirements: all_

  - [ ]* 7.2 Write integration tests with `InMemoryStore`
    - Test heartbeat timeout: schedule activity, poll (start), wait without heartbeating, verify scanner submits heartbeat timeout producing `ActivityTaskTimedOut` history event
    - Test schedule-to-start timeout: schedule activity, do not poll, verify scanner submits schedule-to-start timeout
    - Test heartbeat then stop: schedule, poll, send heartbeats, stop heartbeating, verify heartbeat timeout fires
    - Test schedule-to-close precedence: schedule with `schedule_to_close_timeout`, poll, verify schedule-to-close fires and takes precedence over start-to-close
    - Test cancellation indicator: process `RequestCancelActivity` command, verify `record_activity_heartbeat` returns `true`
    - _Requirements: 1.3, 3.1, 4.1, 5.1, 6.1, 6.2_

- [x] 8. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- All property test sub-tasks are REQUIRED (not optional) — this is a correctness feature where the properties are the point
- Each property test references a specific property from the design document
- The scanner submits through the lane's `submit` path, preserving single-writer serialization
- `tokio-util` is needed for `CancellationToken` (graceful scanner shutdown)
- The kernel already handles `ActivityResolution::TimedOut { timeout_type }` — no kernel changes required
