# Implementation Plan: Timer Scanner

## Overview

Add a background timer scanner to `tokeira-runtime` that periodically discovers due timers from storage and injects `Command::TimerDue` commands into the owning run's lane. The implementation mirrors the activity timeout scanner pattern: a `tokio::spawn` loop with `CancellationToken` lifecycle, configurable interval and batch size, and non-authoritative delivery through the lane's `submit` path.

## Tasks

- [x] 1. Add `tokio-util` dependency and create `TimerScannerConfig`
  - [x] 1.1 Add `tokio-util` with `rt` feature to `[dependencies]` in `tokeira/crates/tokeira-runtime/Cargo.toml`
    - Ensure `tokio-util = { version = "0.7", features = ["rt"] }` is present in both `[dependencies]` and `[dev-dependencies]`
    - _Requirements: 4.1, 4.2_

  - [x] 1.2 Define `TimerScannerConfig` struct in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Add `pub struct TimerScannerConfig` with `scan_interval: tokio::time::Duration` and `max_timers_per_scan: usize`
    - Implement `Default` with `scan_interval = 200ms` and `max_timers_per_scan = 100`
    - _Requirements: 3.1, 3.2, 3.3_

  - [x] 1.3 Write unit test for `TimerScannerConfig::default()` values
    - Verify `scan_interval == Duration::from_millis(200)` and `max_timers_per_scan == 100`
    - _Requirements: 3.2, 3.3_

- [x] 2. Implement `pick_lane` helper and `run_timer_scanner` async function
  - [x] 2.1 Derive `Clone` on `LaneHandle` in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - The inner `mpsc::Sender` is already `Arc`-backed, so cloning is cheap
    - Required so the scanner can hold its own `Vec<LaneHandle>` without borrowing the runtime
    - _Requirements: 1.2_

  - [x] 2.2 Extract a free `pick_lane` function in `runtime.rs`
    - `fn pick_lane(lanes: &[LaneHandle], lane_count: usize, run_key: RunKey) -> &LaneHandle`
    - Uses `DefaultHasher` + `hash(run_key) mod lane_count`, consistent with `TokeiraRuntime::lane_index`
    - _Requirements: 1.2_

  - [x] 2.2 Write property test for deterministic lane routing (Property 6)
    - **Property 6: Deterministic lane routing**
    - Generate random `RunKey` values and lane counts (1..16)
    - Verify `pick_lane` returns the same index as `TokeiraRuntime::lane_index` for the same inputs
    - Verify repeated calls with the same `run_key` produce the same result
    - **Validates: Requirements 1.2**

  - [x] 2.3 Implement `run_timer_scanner` async function in `runtime.rs`
    - Signature: `async fn run_timer_scanner<R>(repo: Arc<R>, lanes: Vec<LaneHandle>, lane_count: usize, config: TimerScannerConfig, cancel: CancellationToken)` where `R: RunRepository + 'static`
    - Loop: `tokio::select!` on `cancel.cancelled()` vs `tokio::time::sleep(config.scan_interval)`
    - Each cycle: capture `now`, call `repo.list_due_timers(now, config.max_timers_per_scan)`
    - On storage error: `tracing::warn!`, continue to next cycle
    - For each `DueTimer`: route via `pick_lane`, submit `Command::TimerDue(TimerDueRequest { timer_id, fired_at: now })`
    - On submit error from kernel rejection (contains "kernel rejected"): `tracing::debug!`, continue to next entry
    - On submit error from lane failure (channel closed, OCC exhaustion): `tracing::warn!`, continue to next entry
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 3.4, 5.1, 5.2, 5.3_

  - [x] 2.4 Write property test for command shape (Property 1)
    - **Property 1: Each due timer produces a correctly shaped TimerDue command**
    - Generate random `Vec<DueTimer>` entries (0..20 entries with random `RunKey` and `timer_id`)
    - Use a mock `RunRepository` returning these from `list_due_timers` and a mock lane capturing submitted commands
    - Verify each `DueTimer` produces exactly one `Command::TimerDue` with matching `timer_id`
    - Verify each command is routed to the lane determined by `hash(due.run_key) mod lane_count`
    - **Validates: Requirements 1.2**

  - [x] 2.5 Write property test for batch limit (Property 2)
    - **Property 2: Batch limit is respected**
    - Generate random `max_timers_per_scan` values (1..200)
    - Use a mock `RunRepository` that records the `limit` parameter passed to `list_due_timers`
    - Verify the recorded limit equals the configured `max_timers_per_scan`
    - **Validates: Requirements 1.4, 3.4**

  - [x] 2.6 Write property test for consistent fired_at (Property 3)
    - **Property 3: All commands in a scan cycle share the same fired_at**
    - Generate random `Vec<DueTimer>` entries (2..10 entries)
    - Use a mock lane that captures all submitted `TimerDueRequest` payloads
    - Verify all `fired_at` values in a single cycle are identical
    - **Validates: Requirements 1.5**

  - [x] 2.7 Write property test for per-entry failure resilience (Property 4)
    - **Property 4: Scanner continues processing after per-entry failures**
    - Generate random batches of `DueTimer` entries and random failure patterns (which entries fail submission)
    - Configure mock lanes to fail on specific entries
    - Verify all non-failing entries are still submitted
    - **Validates: Requirements 2.2, 2.3, 2.4, 5.2, 5.3**

  - [x] 2.8 Write property test for storage error resilience (Property 5)
    - **Property 5: Scanner survives transient storage errors**
    - Generate random sequences of success/failure for `list_due_timers` (e.g., fail then succeed)
    - Verify the scanner loop continues after a `list_due_timers` error and processes timers in subsequent successful cycles
    - **Validates: Requirements 5.1**

- [x] 3. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Integrate timer scanner into `TokeiraRuntime` lifecycle
  - [x] 4.1 Add `timer_scanner_handle` and `timer_scanner_cancel` fields to `TokeiraRuntime`
    - Add `timer_scanner_handle: Option<tokio::task::JoinHandle<()>>` field
    - Add `timer_scanner_cancel: tokio_util::sync::CancellationToken` field
    - _Requirements: 4.1, 4.2_

  - [x] 4.2 Update `TokeiraRuntime::new` to accept `TimerScannerConfig` and spawn the scanner
    - Add `timer_config: TimerScannerConfig` parameter to `new`
    - Create a `CancellationToken`, spawn `run_timer_scanner` via `tokio::spawn`
    - Store the `JoinHandle` and `CancellationToken` in the struct
    - _Requirements: 4.1, 4.3_

  - [x] 4.3 Add `shutdown_timer_scanner` method to `TokeiraRuntime`
    - Cancel the token, await the join handle with a 5-second timeout
    - _Requirements: 4.2_

  - [x] 4.4 Update all existing call sites of `TokeiraRuntime::new` to pass `TimerScannerConfig::default()`
    - Search for all usages across the workspace (tests, examples, apps) and add the new parameter
    - _Requirements: 3.1_

  - [x] 4.5 Write unit test verifying `timer_scanner_handle` is `Some` after `TokeiraRuntime::new`
    - _Requirements: 4.1_

  - [x] 4.6 Write unit test verifying scanner shutdown completes within bounded time
    - Create a runtime, call `shutdown_timer_scanner`, assert the handle completes
    - _Requirements: 4.2_

- [x] 5. Update re-exports in `lib.rs`
  - Re-export `TimerScannerConfig` from `tokeira/crates/tokeira-runtime/src/lib.rs`
  - _Requirements: 3.1_

- [x] 6. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Integration tests
  - [x] 7.1 Write integration test: timer fires end-to-end
    - Start a workflow via `TokeiraRuntime` with `InMemoryStore`
    - Schedule a timer with `fire_at` in the past via `WorkflowTaskCompleted` with a `StartTimer` command
    - Wait a few scan cycles and verify the scanner submits a `TimerDue` that produces a `TimerFired` history event
    - _Requirements: 1.1, 1.2, 2.1_

  - [x] 7.2 Write integration test: canceled timer is harmlessly rejected
    - Start a workflow, schedule a timer, cancel it via `CancelTimer`
    - Verify the scanner does not produce a `TimerFired` event (the `TimerDue` is rejected with `Reject::UnknownTimer`)
    - _Requirements: 2.2_

  - [x] 7.3 Write integration test: multiple due timers all fire
    - Start a workflow, schedule multiple timers with past deadlines
    - Verify all timers produce `TimerFired` history events within a few scan cycles
    - _Requirements: 1.1, 1.4_

- [x] 8. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property tests are required (not optional) per project convention
- Each property test references a specific correctness property from the design document
- The scanner uses Rust with `proptest` for property-based testing, consistent with existing `tokeira-runtime` test infrastructure
- Property tests should run a minimum of 100 iterations (proptest default is 256)
- Tag format for property tests: `// Feature: runtime-timer-scanner, Property N: <title>`
- `tokio-util` must be in `[dependencies]` (not just `[dev-dependencies]`) because `CancellationToken` is used in production code
