# Implementation Plan: Lane OCC Retry and Mailbox Coalescing

## Overview

Harden the `tokeira-runtime` lane execution path by adding an OCC retry loop, mailbox coalescing, and dispatch op publication. The implementation proceeds bottom-up: configuration and traits first, then the core `handle_message` retry loop, then the lane task loop with coalescing, then the `RuntimeDispatchPublisher` and runtime wiring, and finally removal of the facade-level publication. Property tests are placed immediately after the code they validate.

## Tasks

- [x] 1. Add `proptest` dev-dependency and `LaneConfig` struct
  - [x] 1.1 Add `proptest = "1"` to `[dev-dependencies]` in `tokeira/crates/tokeira-runtime/Cargo.toml`
    - _Requirements: 5.1_

  - [x] 1.2 Define `LaneConfig` struct in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - Add `LaneConfig` with `max_occ_retries: u32` (default 5) and `max_drain_per_activation: u32` (default 16)
    - Implement `Default` for `LaneConfig`
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 1.3 Write unit tests for `LaneConfig` defaults and edge values
    - Verify `Default::default()` yields `max_occ_retries == 5` and `max_drain_per_activation == 16`
    - Verify `max_occ_retries = 0` and `max_drain_per_activation = 1` are representable
    - _Requirements: 5.1, 5.2, 5.3_

- [x] 2. Define `DispatchPublisher` trait and `RuntimeDispatchPublisher`
  - [x] 2.1 Define the `DispatchPublisher` async trait in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - `async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()>`
    - Require `Send + Sync`
    - _Requirements: 4.1, 4.7_

  - [x] 2.2 Implement `RuntimeDispatchPublisher` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Holds an `InMemoryBroker`
    - `EnqueueWorkflowTask` → `broker.publish_workflow_task` (fully wired)
    - `EnqueueActivityTask` → `tracing::info` logged stub (wired in Feature 2)
    - All other variants → `tracing::info` logged stub (wired in Features 6, 7, 9)
    - Return `Ok(())` on success
    - _Requirements: 4.2, 4.3, 4.4, 4.7_

- [x] 3. Implement `handle_message` OCC retry loop
  - [x] 3.1 Rewrite `handle_message` in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - Signature: `async fn handle_message<K, R>(kernel, repo, run_key, command, max_retries) -> Result<(CommitResult, SmallVec<[DispatchOp; 4]>)>`
    - On `Applied`: return result with `transition.dispatch_ops`
    - On `Duplicate`: return immediately with empty ops vec
    - On `Conflict`: reload via `repo.load_run`, reapply same `command`, increment attempt counter
    - On attempt > max_retries: return `anyhow::Error` for retry exhaustion
    - On `Kernel::apply` returning `Reject`: return error without retry
    - On storage I/O error from `load_run` or `commit_transition`: return error without retry
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [x] 3.2 Write property test: Reload-and-recompute on conflict
    - **Property 1: Reload-and-recompute on conflict**
    - Mock repo returns `Conflict` for first K attempts then `Applied`; verify `load_run` called K+1 times, `kernel.apply` called K+1 times, final result is `Applied`
    - **Validates: Requirements 1.1, 1.2, 1.6**

  - [x] 3.3 Write property test: Same command across retries
    - **Property 2: Same command across retries**
    - Mock kernel records each `Command` received; verify all are bitwise equal to the original
    - **Validates: Requirements 1.3**

  - [x] 3.4 Write property test: Retry bound and exhaustion error
    - **Property 3: Retry bound and exhaustion error**
    - For any `max_occ_retries` N (0..=255) with always-Conflict repo, verify exactly N+1 commit calls and error result
    - **Validates: Requirements 1.4, 1.5**

  - [x] 3.5 Write property test: Duplicate passthrough without retry
    - **Property 4: Duplicate passthrough without retry**
    - Repo returns `Duplicate`; verify result is `Duplicate` and `load_run` called exactly once
    - **Validates: Requirements 1.8**

- [x] 4. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement lane task loop with mailbox coalescing
  - [x] 5.1 Update `spawn_lane` signature in `tokeira/crates/tokeira-runtime/src/lane.rs`
    - Accept `kernel: K`, `repo: R`, `publisher: P`, `config: LaneConfig`
    - Where `P: DispatchPublisher + 'static`
    - _Requirements: 5.1_

  - [x] 5.2 Implement the coalescing lane task loop in `spawn_lane`
    - Receive first message via blocking `rx.recv()`
    - Process via `handle_message` with OCC retry
    - On success, call `publisher.publish(run_key, &ops)` and log-swallow any `Err`
    - Reply to caller with `CommitResult`
    - Drain up to `config.max_drain_per_activation - 1` additional messages via `rx.try_recv()`, but ONLY messages targeting the same `run_key`
    - Messages for different run_keys encountered during drain SHALL be put back (buffered locally and re-sent to the channel) so they are processed in the next activation
    - Each same-run drained message gets its own `handle_message` + publish + reply cycle
    - On error during coalesced drain, return error for that item and stop draining
    - After drain batch completes or limit reached, yield back to blocking recv
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 4.5, 4.6_

  - [x] 5.3 Write property test: Mailbox coalescing drains pending items
    - **Property 5: Mailbox coalescing drains pending items**
    - Pre-fill channel with N commands (1..=drain_limit), verify all N processed in one activation without blocking recv between them
    - **Validates: Requirements 2.1, 2.2**

  - [x] 5.4 Write property test: Drain limit enforcement
    - **Property 6: Drain limit enforcement**
    - Send D+K messages (K > 0) with drain limit D; verify at most D processed before yielding
    - **Validates: Requirements 2.3, 2.6**

  - [x] 5.5 Write property test: Sequential processing with fresh state
    - **Property 7: Sequential processing with fresh state**
    - For coalesced commands, verify kernel input for command i+1 uses the state from commit of command i
    - **Validates: Requirements 2.4**

  - [x] 5.6 Write property test: Fail-stop on coalesced drain error
    - **Property 8: Fail-stop on coalesced drain error**
    - Sequence of N commands where command K fails; verify commands K+1..N are not processed
    - **Validates: Requirements 2.5**

- [x] 6. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Wire `RuntimeDispatchPublisher` into `TokeiraRuntime` and remove facade publication
  - [x] 7.1 Update `TokeiraRuntime::new` in `tokeira/crates/tokeira-runtime/src/runtime.rs`
    - Accept `LaneConfig` parameter (with default)
    - Construct `RuntimeDispatchPublisher` from `broker.clone()` for each lane
    - Pass `publisher` and `config` to `spawn_lane`
    - _Requirements: 5.1_

  - [x] 7.2 Remove `publish_pending_workflow_task` calls from facade methods
    - Remove calls from `start_workflow`, `signal_workflow`, `complete_workflow_task`
    - Remove the `publish_pending_workflow_task` private method itself
    - Remove the `workflow_queue_for` helper if no longer used
    - Keep `republish_queue` (sweep helper) intact
    - _Requirements: 4.1, 4.5, 4.6_

  - [x] 7.3 Write property test: All dispatch ops published after commit
    - **Property 10: All dispatch ops published after commit**
    - For any committed Transition with N dispatch ops, verify publisher receives exactly those N ops; holds after OCC retries too
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.6**

  - [x] 7.4 Write property test: No publication on failed commit
    - **Property 11: No publication on failed commit**
    - When commit fails or retries exhaust, verify publisher receives zero ops
    - **Validates: Requirements 4.5**

  - [x] 7.5 Write property test: Graceful publication failure
    - **Property 12: Graceful publication failure**
    - Mock publisher returns `Err`; verify lane logs the error and still returns `Applied` to caller
    - **Validates: Requirements 4.7**

- [x] 8. Update re-exports and fix downstream compilation
  - [x] 8.1 Update `tokeira/crates/tokeira-runtime/src/lib.rs` re-exports
    - Ensure `LaneConfig`, `DispatchPublisher`, and `RuntimeDispatchPublisher` are publicly accessible
    - _Requirements: 5.1_

  - [x] 8.2 Fix any downstream callers of `TokeiraRuntime::new` and `spawn_lane`
    - Update call sites in `tokeira/apps/tokeirad/src/main.rs` and any integration tests to pass `LaneConfig` (or use `Default`)
    - Verify compilation across the workspace
    - _Requirements: 5.1_

- [x] 9. Write routing and broker dedup property tests
  - [x] 9.1 Write property test: Deterministic hash routing
    - **Property 9: Deterministic hash routing**
    - For any `RunKey` and `lane_count` ≥ 1, verify `hash(run_key) mod lane_count` is stable and in range `[0, lane_count)`
    - **Validates: Requirements 3.1, 3.3**

  - [x] 9.2 Write property test: Idempotent workflow task publication
    - **Property 13: Idempotent workflow task publication**
    - Publish same `(run_key, logical_seq)` twice to `InMemoryBroker`; verify broker contains at most one copy
    - **Validates: Requirements 6.4**

- [x] 10. Integration tests with DevStore
  - [x] 10.1 Write integration test: start + signal with dispatch op publication
    - Start a workflow via `TokeiraRuntime`, signal it, verify both workflow task and signal-derived dispatch ops are published to the broker
    - _Requirements: 4.1, 4.2, 4.6_

  - [x] 10.2 Write integration test: OCC conflict resolution under concurrent signals
    - Race two signals to the same run via the runtime; verify both eventually commit successfully
    - _Requirements: 1.1, 1.6_

  - [x] 10.3 Write integration test: burst coalescing produces correct final state
    - Send a burst of signals to the same run; verify all are processed and the final workflow state reflects all signals
    - _Requirements: 2.1, 2.2, 2.4_

- [x] 11. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property tests are required (not optional) — this is a correctness feature where the properties are the point.
- Property tests use `proptest` crate with mock `Kernel`, `RunRepository`, and `DispatchPublisher` implementations.
- Each property test references its design property number and the requirements it validates.
- The design uses Rust throughout; no language selection needed.
- Checkpoints are placed after the OCC retry loop, after coalescing, and at the end.
