# Implementation Plan: Update Two-Phase Lifecycle

## Overview

Add update dispatch and two-phase lifecycle coordination to `tokeira-runtime`. The runtime routes `Command::Update` through the kernel via lanes (same path as signals), maintains an in-memory `UpdateRegistry` mapping `(RunKey, update_id)` to response channels, extracts update resolution events from committed `WorkflowTaskCompleted` transitions, and notifies waiting callers. Callers choose between waiting for acceptance only or full completion, with per-call timeout enforcement. All new types and the `update_workflow` method live in `tokeira-runtime`, but the feature also modifies lane activation semantics: `run_activation` and `spawn_lane` gain an `UpdateRegistry` parameter, and the post-commit path gains event-scanning and registry-drain logic. The kernel types (`Command::Update`, `UpdateRequest`, `PendingUpdate`, `WorkflowCommand::UpdateCompleted/UpdateRejected/ProtocolMessage`, and the corresponding `HistoryEventKind` variants) already exist and are unchanged.

## Tasks

- [x] 1. Define new types and UpdateRegistry
  - [x] 1.1 Create `tokeira/crates/tokeira-runtime/src/update.rs` with `UpdateOutcome`, `UpdateWaitPolicy`, `UpdateResolution`, `UpdateRegistryEntry`, and `UpdateRegistry`
    - `UpdateOutcome` enum: `Accepted { accepted_event_id: i64 }`, `Completed { accepted_event_id: i64, result: Payloads }`, `Rejected { accepted_event_id: i64, failure: String }`
    - `UpdateOutcome` derives `Clone`, `Debug`, `PartialEq`
    - `UpdateWaitPolicy` enum: `Accepted`, `Completed`
    - `UpdateWaitPolicy` derives `Clone`, `Debug`, `PartialEq`
    - `UpdateResolution` enum (crate-internal): `Completed { result: Payloads }`, `Rejected { failure: String }`, `RunClosed`
    - `UpdateRegistryEntry` struct (crate-internal): `complete_tx: oneshot::Sender<UpdateResolution>`
    - `UpdateRegistry` struct: `inner: Arc<Mutex<HashMap<(RunKey, String), UpdateRegistryEntry>>>`
    - `UpdateRegistry` derives `Clone`
    - Implement `UpdateRegistry::new()`, `register()`, `notify()`, `remove()`, `drain_for_run()`
    - `register` takes `run_key: RunKey`, `update_id: String`, `complete_tx: oneshot::Sender<UpdateResolution>`
    - `notify` takes `run_key: RunKey`, `update_id: &str`, `resolution: UpdateResolution` → returns `bool`
    - `remove` takes `run_key: RunKey`, `update_id: &str`
    - `drain_for_run` takes `run_key: RunKey` → returns `usize`, sends `RunClosed` to all entries for that run
    - _Requirements: 2.1, 2.3, 2.4, 2.5, 9.1, 9.2_
  - [x] 1.2 Register `pub mod update;` and `pub use update::*;` in `lib.rs`
    - _Requirements: 2.1_

- [x] 2. Implement `update_workflow` on `TokeiraRuntime`
  - [x] 2.1 Add `update_registry: UpdateRegistry` field to `TokeiraRuntime`
    - Initialize in `new_with_nexus_and_shards` with `UpdateRegistry::new()`
    - Add `pub fn update_registry(&self) -> UpdateRegistry` accessor
    - _Requirements: 2.1, 2.4_
  - [x] 2.2 Add the `update_workflow` method
    - Signature: `pub async fn update_workflow(&self, execution: ExecutionRef, update_id: String, update_name: String, input: Payloads, request: RequestContext, timeout: Duration, wait_policy: UpdateWaitPolicy) -> Result<UpdateOutcome>`
    - Step 1: `self.repo.resolve_execution(&execution)` → `RunKey`, return `anyhow!("execution not found")` if `None`
    - Step 2: Construct `Command::Update(UpdateRequest { update_id, update_name, input, request, now: OffsetDateTime::now_utc() })`
    - Step 3: If `wait_policy == Completed`, create `oneshot::channel::<UpdateResolution>()` and pre-register: `self.update_registry.register(run_key, update_id.clone(), complete_tx)`. This MUST happen before `submit()` to close the race window where a fast worker completes the update between dispatch-op publication and `submit()` return.
    - Step 4: `self.submit(run_key, command).await?` — routes through lane, kernel applies, storage commits
    - Step 5: Match on `CommitResult`:
      - `Applied { new_state }` → extract `accepted_event_id` from `new_state.pending_updates` entry for the `update_id`
      - `Duplicate` → remove pre-registered entry (if any) via `self.update_registry.remove(run_key, &update_id)`, return `UpdateOutcome::Accepted { accepted_event_id: 0 }` immediately. Do NOT wait for completion. The bare `Duplicate` carries no state — the original update may be pending, completed, or rejected. The caller must poll history for the completion result if needed.
      - `Conflict { reason }` → remove pre-registered entry (if any) via `self.update_registry.remove(run_key, &update_id)`, return error
    - Step 6: If submit returns Err, remove pre-registered entry (if any) via `self.update_registry.remove(run_key, &update_id)`, propagate error
    - Step 7: If `wait_policy == Accepted`, return `UpdateOutcome::Accepted { accepted_event_id }`
    - Step 8: `tokio::time::timeout(timeout, complete_rx).await` (channel was created in step 3):
      - `Ok(Ok(UpdateResolution::Completed { result }))` → `UpdateOutcome::Completed { accepted_event_id, result }`
      - `Ok(Ok(UpdateResolution::Rejected { failure }))` → `UpdateOutcome::Rejected { accepted_event_id, failure }`
      - `Ok(Ok(UpdateResolution::RunClosed))` → return error "run closed before update completed"
      - `Ok(Err(_))` → return error "update response channel closed"
      - `Err(_)` → `self.update_registry.remove(run_key, &update_id)`, return timeout error
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 2.2, 2.3, 3.1, 3.2, 3.3, 3.4, 3.5, 4.5, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.2, 6.3, 8.1, 8.2, 8.3, 8.4_

- [x] 3. Checkpoint — Verify types and `update_workflow` compile
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Lane activation changes for update resolution notification
  - [x] 4.1 Thread `UpdateRegistry` into lane activation
    - Add `update_registry: &UpdateRegistry` parameter to `run_activation` and `spawn_lane`
    - Pass `update_registry` from `TokeiraRuntime` when spawning lanes in `new_with_nexus_and_shards`
    - _Requirements: 4.1, 4.6, 7.1_
  - [x] 4.2 Scan committed history events for update resolutions
    - After a successful commit (`CommitResult::Applied`), inside the existing `history_events` scan loop, add match arms for:
      - `HistoryEventKind::WorkflowExecutionUpdateCompleted { update_id, result }` → `update_registry.notify(run_key, &update_id, UpdateResolution::Completed { result: result.clone() })`
      - `HistoryEventKind::WorkflowExecutionUpdateRejected { update_id, failure }` → `update_registry.notify(run_key, &update_id, UpdateResolution::Rejected { failure: failure.clone() })`
    - Notification happens in the same activation cycle as the commit, before the next mailbox item
    - If `notify` returns `false` (no caller waiting), silently discard — no error
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 7.1, 7.2, 7.3, 7.4, 7.5_
  - [x] 4.3 Drain update registry on run close
    - In the existing `if new_state.closed_at.is_some()` block (where `workflow_timeout_tracking.remove` and `nexus_timeout_tracking.remove_all_for_run` are called), add `update_registry.drain_for_run(message.run_key)`
    - This notifies all waiting callers with `RunClosed` and removes all entries for the closed run
    - _Requirements: 9.1, 9.2, 9.3_

- [x] 5. Checkpoint — Verify lane changes compile and existing tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Property tests for update lifecycle
  - [x] 6.1 Write property test: update command preserves caller parameters
    - **Property 1: Update command preserves caller parameters**
    - Generate random `update_id`, `update_name`, `input` payload, and `RequestContext`
    - Call `update_workflow`, intercept the `Command::Update(UpdateRequest)` submitted to the lane
    - Assert the `UpdateRequest` carries the exact same `update_id`, `update_name`, `input`, and `request` values
    - **Validates: Requirements 1.2, 1.4**
  - [x] 6.2 Write property test: kernel rejections propagate and clean up pre-registered entry
    - **Property 2: Kernel rejections propagate and clean up pre-registered entry**
    - Generate random rejection scenarios (`Reject::WorkflowPaused`, `Reject::DuplicateUpdateId`, `Reject::RunClosed`)
    - Call `update_workflow` with `wait_policy = Completed`, verify error is returned and `UpdateRegistry` contains no entry for that `(RunKey, update_id)` — the pre-registered entry must have been cleaned up
    - **Validates: Requirements 1.6, 8.1, 8.2, 8.3, 8.4**
  - [x] 6.3 Write property test: acceptance notification carries correct event ID
    - **Property 3: Acceptance notification carries correct event ID**
    - Generate random updates, commit successfully, verify `accepted_event_id` in the returned `UpdateOutcome` matches the `pending_updates` entry
    - **Validates: Requirements 3.1, 3.2**
  - [x] 6.4 Write property test: update resolution notification round-trip
    - **Property 4: Update resolution notification round-trip**
    - Generate random update resolutions (completions and rejections with random payloads/failures)
    - Register callers in the `UpdateRegistry`, simulate committed history events with `WorkflowExecutionUpdateCompleted`/`WorkflowExecutionUpdateRejected`
    - Verify each caller receives the exact `result` or `failure` payload
    - Include multi-resolution transitions (2–4 updates resolved in one WFT)
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 7.2, 7.3, 7.5**
  - [x] 6.5 Write property test: silent discard when no caller is waiting
    - **Property 5: Silent discard when no caller is waiting**
    - Generate random updates, remove callers from registry (simulating timeout), then process a resolution transition
    - Verify no error and the transition commits normally — only the notification is skipped
    - **Validates: Requirements 4.5, 5.6**
  - [x] 6.6 Write property test: timeout enforcement without kernel state mutation
    - **Property 6: Timeout enforcement without kernel state mutation**
    - Generate random updates with short timeouts, accept them but don't complete
    - Verify timeout error is returned, registry entry is removed, and `pending_updates` still contains the entry
    - **Validates: Requirements 5.1, 5.3, 5.4**
  - [x] 6.7 Write property test: concurrent updates are independent
    - **Property 7: Concurrent updates are independent**
    - Generate N (2–8) concurrent updates to the same `RunKey` with distinct `update_id` values
    - Resolve a random subset, verify unresolved updates remain pending and unaffected
    - **Validates: Requirements 6.1, 6.2, 6.4**
  - [x] 6.8 Write property test: run close drains all waiting callers
    - **Property 8: Run close drains all waiting callers**
    - Generate runs with K (1–8) registered update callers
    - Close the run, verify all K callers receive `RunClosed` notification and registry is empty for that `RunKey`
    - **Validates: Requirements 9.1, 9.2**
  - [x] 6.9 Write property test: registry cleanup on all resolution paths
    - **Property 9: Registry cleanup on all resolution paths**
    - Generate updates and resolve them via each path (completion, rejection, timeout, run close)
    - Verify registry is empty after each resolution path
    - **Validates: Requirements 2.3**

- [x] 7. Checkpoint — Verify all property tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 8. Unit tests for UpdateRegistry and update_workflow
  - [x]* 8.1 Write unit tests for UpdateRegistry
    - `register` + `notify` round-trip returns `true` and delivers resolution
    - `notify` returns `false` when no entry exists
    - `remove` prevents subsequent `notify` from finding the entry
    - `drain_for_run` returns correct count and sends `RunClosed` to all entries
    - `drain_for_run` on empty registry returns 0
    - Concurrent `register` + `notify` from different threads
    - _Requirements: 2.1, 2.3, 2.4, 9.1, 9.2_
  - [x]* 8.2 Write unit tests for update_workflow edge cases
    - `update_workflow` returns error when `ExecutionRef` cannot be resolved (Req 1.3)
    - `update_workflow` with `wait_policy = Accepted` returns immediately after acceptance (Req 3.4)
    - `update_workflow` with `wait_policy = Completed` waits for worker resolution (Req 3.5)
    - `CommitResult::Duplicate` returns `UpdateOutcome::Accepted { accepted_event_id: 0 }` immediately without registering in the UpdateRegistry (Req 3.3)
    - Registry cleanup happens in the same activation cycle as the close commit (Req 9.3)
    - _Requirements: 1.3, 3.3, 3.4, 3.5, 9.3_

- [ ] 9. Integration tests — end-to-end update lifecycle
  - [x]* 9.1 Write integration test: update accepted and completed by worker
    - Start runtime with in-memory repo, start a workflow
    - Call `update_workflow` with `wait_policy = Completed`
    - Spawn a task that polls `poll_workflow_task`, builds a `WorkflowTaskCompletedRequest` with `WorkflowCommand::UpdateCompleted { update_id, result }`, calls `complete_workflow_task`
    - Assert caller receives `UpdateOutcome::Completed` with the expected result payload
    - _Requirements: 1.1–1.5, 3.1, 3.2, 4.1, 4.6_
  - [x]* 9.2 Write integration test: update rejected by worker
    - Same setup, but worker returns `WorkflowCommand::UpdateRejected { update_id, failure }`
    - Assert caller receives `UpdateOutcome::Rejected` with the expected failure
    - _Requirements: 4.2_
  - [x]* 9.3 Write integration test: update timeout then late worker completion
    - Call `update_workflow` with a short timeout, no worker responds
    - Assert caller receives timeout error
    - Then have the worker complete the update — assert the transition commits normally but no caller notification occurs
    - _Requirements: 5.1, 5.3, 5.6_
  - [x]* 9.4 Write integration test: run close with pending updates
    - Start a workflow, submit an update with `wait_policy = Completed`
    - Close the workflow via `CompleteWorkflow` command before the update is completed
    - Assert the waiting caller receives a run-closed error
    - _Requirements: 9.1, 9.2, 9.3_
  - [x]* 9.5 Write integration test: multiple updates resolved in single WFT
    - Submit two updates to the same workflow
    - Worker completes both in a single `WorkflowTaskCompleted` with two `UpdateCompleted` commands
    - Assert both callers receive their respective results independently
    - _Requirements: 6.1, 6.4, 7.5_

- [x] 10. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- All property test tasks (6.1–6.9) are REQUIRED per project guidance — not marked optional
- Unit tests (8.x) and integration tests (9.x) are marked optional with `*`
- The design uses Rust — no language selection needed
- `UpdateRegistry` uses `Arc<Mutex<HashMap<...>>>` matching the pattern of `WorkflowTimeoutTrackingState` and `ActivityTrackingState`
- `UpdateRegistryEntry` does NOT implement `Clone` (contains oneshot sender)
- `UpdateOutcome` and `UpdateWaitPolicy` implement `Clone`, `Debug`, `PartialEq`
- `rustfmt max_width = 90` — keep lines within 90 columns
- Follow existing patterns in `query.rs`, `runtime.rs`, and `lane.rs`
- Property tests use `proptest` (already used in `broker.rs` and `query.rs`)
- Each task references specific requirements for traceability
- Kernel types (`Command::Update`, `UpdateRequest`, `PendingUpdate`, `HistoryEventKind::WorkflowExecutionUpdate*`) already exist in `tokeira-kernel` — no kernel changes needed
