# Implementation Plan: Workflow Timeouts

## Overview

Add a background workflow timeout scanner to `tokeira-runtime` that periodically checks runtime-local tracking state for open runs exceeding their configured execution or run timeouts, and injects `Command::WorkflowExecutionTimedOut` commands into the owning run's lane. The implementation uses runtime-local tracking state (populated on `Start` commit, cleaned up on run close or kernel rejection) and a `tokio::spawn` loop with `CancellationToken` lifecycle, mirroring the timer scanner pattern.

## Tasks

- [x] 1. Define tracking state types and pure evaluation function
  - [x] 1.1 Define `WorkflowTimeoutEntry` struct in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Fields: `run_key: RunKey`, `workflow_execution_timeout: Option<Duration>`, `workflow_run_timeout: Option<Duration>`, `started_at: OffsetDateTime`, `has_retry_policy: bool`
    - Derive `Clone, Debug`
    - _Requirements: 4.3, 4.5_

  - [x] 1.2 Define `WorkflowTimeoutTrackingState` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Wrap `Arc<Mutex<HashMap<RunKey, WorkflowTimeoutEntry>>>` with `Default` and `Clone`
    - Methods: `insert(&self, entry: WorkflowTimeoutEntry)`, `remove(&self, run_key: RunKey)`, `snapshot(&self) -> Vec<WorkflowTimeoutEntry>`
    - _Requirements: 4.3_

  - [x] 1.3 Define `WorkflowTimeoutViolation` enum in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Variants: `ExecutionTimeout`, `RunTimeout`
    - Derive `Clone, Debug, PartialEq`
    - _Requirements: 1.1, 2.1_

  - [x] 1.4 Implement `evaluate_workflow_timeout` pure function in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Signature: `pub fn evaluate_workflow_timeout(entry: &WorkflowTimeoutEntry, now: OffsetDateTime) -> Option<WorkflowTimeoutViolation>`
    - Check execution timeout first (precedence), then run timeout
    - Return `None` if neither is configured or neither has elapsed
    - _Requirements: 1.1, 1.4, 2.1, 2.3, 2.4_

  - [x] 1.5 Write unit tests for `evaluate_workflow_timeout`
    - Both timeouts configured and both expired: returns `ExecutionTimeout` (precedence)
    - Zero-duration timeout fires immediately
    - No timeouts configured: returns `None`
    - Only run timeout configured and expired: returns `RunTimeout`
    - Only execution timeout configured and expired: returns `ExecutionTimeout`
    - Neither expired: returns `None`
    - _Requirements: 1.1, 1.4, 2.1, 2.3, 2.4_

  - [x] 1.6 Write property test for timeout evaluation correctness (Property 1)
    - **Property 1: Workflow timeout evaluation correctness**
    - Generate random `WorkflowTimeoutEntry` values with random `Option<Duration>` for both timeouts, random `started_at`, and random `now`
    - Verify: if execution timeout is `Some(d)` and `now - started_at > d`, result is `Some(ExecutionTimeout)` regardless of run timeout
    - Verify: if execution timeout is `None` or not elapsed, and run timeout is `Some(d2)` and `now - started_at > d2`, result is `Some(RunTimeout)`
    - Verify: if neither configured or neither elapsed, result is `None`
    - **Validates: Requirements 1.1, 1.4, 2.1, 2.3, 2.4**

  - [x] 1.7 Write property test for retry state derivation (Property 2)
    - **Property 2: Retry state derivation from retry policy presence**
    - Generate random `WorkflowTimeoutEntry` values where a timeout violation is detected, with random `has_retry_policy`
    - Verify: if `has_retry_policy` is `true`, retry state is `RetryState::Timeout`
    - Verify: if `has_retry_policy` is `false`, retry state is `RetryState::RetryPolicyNotSet`
    - **Validates: Requirements 1.2**

- [x] 2. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 3. Define scanner config and implement scanner loop
  - [x] 3.1 Define `WorkflowTimeoutScannerConfig` struct in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Fields: `scan_interval: tokio::time::Duration`, `max_timeouts_per_scan: usize`
    - Implement `Default` with `scan_interval = 1s` and `max_timeouts_per_scan = 100`
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 3.2 Write unit test for `WorkflowTimeoutScannerConfig::default()` values
    - Verify `scan_interval == Duration::from_secs(1)` and `max_timeouts_per_scan == 100`
    - _Requirements: 6.2, 6.3_

  - [x] 3.3 Implement `scan_workflow_timeouts_once` helper function in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Signature: `async fn scan_workflow_timeouts_once<F, Fut>(tracking: &WorkflowTimeoutTrackingState, config: &WorkflowTimeoutScannerConfig, mut submit_timeout: F)` where `F: FnMut(WorkflowTimeoutEntry, WorkflowTimeoutViolation, OffsetDateTime) -> Fut`, `Fut: Future<Output = Result<()>>`
    - Capture `now = OffsetDateTime::now_utc()` once at the start
    - Snapshot entries from tracking state
    - For each entry (up to `max_timeouts_per_scan`): evaluate timeout, call `submit_timeout` on violation
    - On `Ok`: remove entry from tracking
    - On `Err` containing "kernel rejected": log debug, remove entry from tracking
    - On other `Err`: log warn, keep entry for next cycle
    - _Requirements: 1.1, 1.3, 2.1, 2.3, 3.2, 3.3, 5.4, 5.5, 8.1, 8.2_

  - [x] 3.4 Implement `run_workflow_timeout_scanner` async function in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Signature: `async fn run_workflow_timeout_scanner(tracking: WorkflowTimeoutTrackingState, lanes: Vec<LaneHandle>, lane_count: usize, config: WorkflowTimeoutScannerConfig, cancel: CancellationToken)`
    - Loop: `tokio::select!` on `cancel.cancelled()` vs `tokio::time::sleep(config.scan_interval)`
    - Each cycle: call `scan_workflow_timeouts_once` with a closure that builds `Command::WorkflowExecutionTimedOut` and submits via `pick_lane`
    - _Requirements: 5.1, 5.2, 5.3, 7.1, 7.3_

  - [x] 3.5 Write property test for consistent now timestamp (Property 3)
    - **Property 3: All commands in a scan cycle share the same now timestamp**
    - Generate random sets of `WorkflowTimeoutEntry` values (2..10 entries) with expired timeouts
    - Use a mock submit closure that captures the `now` value from each call
    - Verify all `now` values in a single cycle are identical
    - **Validates: Requirements 1.3, 5.5**

  - [x] 3.6 Write property test for scanner batch bound (Property 6)
    - **Property 6: Scanner batch bound**
    - Generate random entry counts (N > max_timeouts_per_scan) and random `max_timeouts_per_scan` values (1..50)
    - Populate tracking state with N entries that all have expired timeouts
    - Run one scan cycle and count submitted commands
    - Verify submitted count <= `max_timeouts_per_scan`
    - **Validates: Requirements 5.4**

  - [x] 3.7 Write property test for scanner continues after kernel rejections (Property 7)
    - **Property 7: Scanner continues after kernel rejections and removes entries**
    - Generate random batches of expired entries and random failure patterns (which entries get kernel rejection errors containing "kernel rejected")
    - Verify: rejected entries are removed from tracking state
    - Verify: all entries in the batch are processed (scanner does not stop early)
    - **Validates: Requirements 3.2, 3.3, 8.2**

  - [x] 3.8 Write property test for scanner continues after lane errors (Property 8)
    - **Property 8: Scanner continues after lane errors**
    - Generate random batches of expired entries and random failure patterns (which entries get non-rejection lane errors)
    - Verify: failed entries remain in tracking state for retry
    - Verify: all entries in the batch are processed (scanner does not stop early)
    - **Validates: Requirements 8.1**

- [x] 4. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Integrate into `TokeiraRuntime` lifecycle
  - [x] 5.1 Add `workflow_timeout_tracking`, `workflow_timeout_scanner_handle`, and `workflow_timeout_scanner_cancel` fields to `TokeiraRuntime` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - `workflow_timeout_tracking: WorkflowTimeoutTrackingState`
    - `workflow_timeout_scanner_handle: Option<tokio::task::JoinHandle<()>>`
    - `workflow_timeout_scanner_cancel: CancellationToken`
    - _Requirements: 4.3, 7.1_

  - [x] 5.2 Update `TokeiraRuntime::new` to accept `WorkflowTimeoutScannerConfig` and spawn the scanner
    - Add `workflow_timeout_config: WorkflowTimeoutScannerConfig` parameter
    - Create `WorkflowTimeoutTrackingState::default()` and `CancellationToken`
    - Spawn `run_workflow_timeout_scanner` via `tokio::spawn`
    - Store all three new fields in the struct
    - _Requirements: 7.1_

  - [x] 5.3 Add `shutdown_workflow_timeout_scanner` method to `TokeiraRuntime`
    - Cancel the token, await the join handle with a 5-second timeout
    - _Requirements: 7.2, 7.3_

  - [x] 5.4 Expose `workflow_timeout_tracking` accessor on `TokeiraRuntime`
    - `pub fn workflow_timeout_tracking(&self) -> WorkflowTimeoutTrackingState`
    - Needed for lifecycle hooks and integration tests
    - _Requirements: 4.3_

  - [x] 5.5 Update all existing call sites of `TokeiraRuntime::new` to pass `WorkflowTimeoutScannerConfig::default()`
    - Search for all usages across the workspace (tests, examples, apps) and add the new parameter
    - _Requirements: 6.1_

  - [x] 5.6 Write unit test verifying `workflow_timeout_scanner_handle` is `Some` after `TokeiraRuntime::new`
    - _Requirements: 7.1_

  - [x] 5.7 Write unit test verifying workflow timeout scanner shutdown completes within bounded time
    - Create a runtime, call `shutdown_workflow_timeout_scanner`, assert the handle completes
    - _Requirements: 7.2_

- [x] 6. Implement lifecycle hooks for tracking state population and cleanup
  - [x] 6.1 Update `start_workflow` to insert into tracking state after successful commit
    - After `submit` returns `CommitResult::Applied`, check if `StartRequest` has non-None `workflow_execution_timeout` or `workflow_run_timeout`
    - If so, insert a `WorkflowTimeoutEntry` with matching fields into `workflow_timeout_tracking`
    - `started_at` comes from `request.now`, `has_retry_policy` from `request.retry_policy.is_some()`
    - _Requirements: 4.1, 4.5_

  - [x] 6.2 Write property test for start with timeout config populates tracking state (Property 4)
    - **Property 4: Start with timeout config populates tracking state**
    - Generate random `StartRequest` values with random timeout configurations (Some/None for both timeouts)
    - After successful commit, verify: if either timeout is non-None, tracking state contains an entry with matching `run_key`, `workflow_execution_timeout`, `workflow_run_timeout`, `started_at`, and `has_retry_policy`
    - Verify: if both timeouts are None, tracking state does not contain an entry
    - **Validates: Requirements 4.1**

  - [x] 6.3 Add tracking state cleanup on run closure in the lane's `run_activation` post-commit path
    - After a successful commit in `run_activation`, if `new_state.closed_at.is_some()`, call `workflow_timeout_tracking.remove(run_key)`
    - Pass `WorkflowTimeoutTrackingState` to `spawn_lane` (or to a post-commit callback) so the lane can perform cleanup
    - Do NOT use `RuntimeDispatchPublisher` for this — the publisher only sees `DispatchOp`s, not `next_state`
    - This covers all close paths: completion, failure, cancellation, termination, timeout, and reset
    - _Requirements: 4.2, 4.4_

  - [x] 6.4 Write property test for run closure removes tracking entry (Property 5)
    - **Property 5: Run closure removes tracking entry**
    - Generate random runs tracked in `WorkflowTimeoutTrackingState`
    - Simulate run closure (committed transition with `closed_at` set to `Some`)
    - Verify: tracking state no longer contains an entry for that `RunKey`
    - **Validates: Requirements 4.2, 4.4**

  - [x] 6.5 Write unit tests for tracking state lifecycle
    - Insert and remove basic CRUD operations on `WorkflowTimeoutTrackingState`
    - Scanner removes tracking entry after successful timeout submission
    - Scanner removes tracking entry after kernel rejection
    - Scanner keeps tracking entry after lane error
    - _Requirements: 4.1, 4.2, 4.4, 8.1, 8.2_

- [x] 7. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Update re-exports in `lib.rs`
  - Re-export `WorkflowTimeoutEntry`, `WorkflowTimeoutTrackingState`, `WorkflowTimeoutScannerConfig`, `WorkflowTimeoutViolation`, and `evaluate_workflow_timeout` from `tokeira/crates/tokeira-runtime/src/lib.rs`
  - _Requirements: 6.1_

- [x] 9. Integration tests
  - [x] 9.1 Write integration test: execution timeout fires end-to-end
    - Start a workflow via `TokeiraRuntime` with `InMemoryStore` and `workflow_execution_timeout` set to a very short duration (e.g., 1ms)
    - Wait a few scan cycles and verify the scanner submits a `WorkflowExecutionTimedOut` command that produces a `WorkflowExecutionTimedOut` history event with `timeout_type: ExecutionTimeout` and closes the run with `ExecutionStatus::TimedOut`
    - _Requirements: 1.1, 1.2, 1.3_

  - [x] 9.2 Write integration test: run timeout fires end-to-end
    - Start a workflow with `workflow_run_timeout` set to a very short duration
    - Verify the scanner produces a `WorkflowExecutionTimedOut` history event with `timeout_type: RunTimeout`
    - _Requirements: 2.1, 2.2_

  - [x] 9.3 Write integration test: both timeouts configured, execution timeout takes precedence
    - Start a workflow with both `workflow_execution_timeout` and `workflow_run_timeout` set to very short durations
    - Verify only one `WorkflowExecutionTimedOut` event is produced with `timeout_type: ExecutionTimeout`
    - _Requirements: 2.3_

  - [x] 9.4 Write integration test: no timeout configuration produces no timeout events
    - Start a workflow with no timeout configuration
    - Wait several scan cycles and verify no `WorkflowExecutionTimedOut` events are produced
    - _Requirements: 1.4, 2.4_

  - [x] 9.5 Write integration test: manually terminated workflow does not produce timeout event
    - Start a workflow with timeout config, terminate it manually via `Command::Terminate`
    - Verify the scanner does not produce a timeout event (tracking entry was cleaned up on close)
    - _Requirements: 4.2, 4.4_

- [x] 10. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property tests are required (not optional) per project convention
- Each property test references a specific correctness property from the design document
- The scanner uses Rust with `proptest` for property-based testing, consistent with existing `tokeira-runtime` test infrastructure
- Property tests should run a minimum of 100 iterations (proptest default is 256)
- Tag format for property tests: `// Feature: runtime-workflow-timeouts, Property N: <title>`
- The `scan_workflow_timeouts_once` helper is extracted (like `scan_due_timers_once` for the timer scanner) to enable unit-level property testing without spawning the full async loop
- `WorkflowTimeoutTrackingState` is `Clone` (wraps `Arc<Mutex<...>>`) so it can be shared between the runtime, lifecycle hooks, and the scanner task
