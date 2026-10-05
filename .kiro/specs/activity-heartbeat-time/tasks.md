# Implementation Plan: Activity Heartbeat Time

## Overview

Add the serde-skipped time to the kernel, persist it through the State_Extension and the Snapshot_Extension, then record, clear, restore and report it in the runtime, edge and engine. Order: kernel, storage, runtime, edge and engine. Each step leaves the workspace green; the bar of root `AGENTS.md` §10.4 runs at each checkpoint.

## Tasks

- [x] 1. Kernel
  - [x] 1.1 Add `ActivityState.last_heartbeat_at: Option<OffsetDateTime>` with `#[serde(skip)]` and its doc comment; set `None` wherever an `ActivityState` is constructed
    - _Requirements: 3.1, 4.8_
  - [x] 1.2 Clear it next to `heartbeat_details` in `apply_unpause_activity` (reset heartbeat) and `apply_reset_activity` (scheduled activity, reset heartbeat)
    - _Requirements: 4.6, 4.8_
  - [x] 1.3 Unit tests: each kernel Heartbeat_Reset clears the time; a reset of a started activity leaves it for retry preparation
    - _Requirements: 4.6_

- [x] 2. State_Extension codec
  - [x] 2.1 Add `WORKFLOW_STATE_EXTENSION_MAGIC`, `ACTIVITY_HEARTBEAT_SECTION`, `ExtensionSection`, `encode_state_extension`, `apply_state_extension` and `StateExtensionError` to `crates/tokeira-storage/src/codec.rs`; re-export the public names from `dsql/codec.rs`
    - _Requirements: 1.3, 1.5, 1.7, 1.8, 3.2, 3.3, 3.4, 3.5_
  - [x] 2.2 `encode_workflow_state` appends the State_Extension; `decode_workflow_state` decodes the state with `take_from_bytes` and applies any remaining bytes
    - _Requirements: 1.1, 1.2, 1.4, 1.6, 2.1_
  - [x] 2.3 Update the codec module documentation: the layout, the section rules of Requirements 1.9–1.11, and continue-as-new-advice Requirement 10.8
    - _Requirements: 1.9, 1.10, 1.11_
  - [x] 2.4 Property test: hot-state blobs round-trip with their times
    - **Property 1: Hot-state blobs round-trip with their times**
    - **Validates: Requirements 1.1, 1.2, 1.6, 3.2, 3.3**
  - [x] 2.5 Property test: without times, the bytes are unchanged
    - **Property 2: Without times, the bytes are unchanged**
    - **Validates: Requirements 1.4, 2.1, 2.2**
  - [x] 2.6 Property test: pre-extension readers see the state, using a copy of the 0.5.1 decode function
    - **Property 3: Pre-extension readers see the state**
    - **Validates: Requirement 2.3**
  - [x] 2.7 Property test: malformed extensions are rejected
    - **Property 4: Malformed extensions are rejected**
    - **Validates: Requirements 1.7, 3.5**
  - [x] 2.8 Property test: unknown sections and unknown activities are ignored
    - **Property 5: Unknown sections and unknown activities are ignored**
    - **Validates: Requirements 1.8, 3.4**
  - [x] 2.9 Unit tests: a frozen-layout fixture of a deterministic `WorkflowState` with every activity field set; `encode_activity_state` bytes are the same with and without a time
    - _Requirements: 1.11, 3.6_

- [x] 3. Snapshot_Extension
  - [x] 3.1 Add `SNAPSHOT_EXTENSION_MAGIC`, `RUN_STATE_EXTENSION_SECTION` and `SnapshotError::Extension`; `snapshot()` appends the extension when any run has a State_Extension; `from_snapshot()` parses and applies it; `SNAPSHOT_FORMAT_VERSION` stays 4 and its bump-rule comment says why
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_
  - [x] 3.2 Property test: snapshots carry the times and stay compatible
    - **Property 9: Snapshots carry the times and stay compatible**
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.4, 7.5**
  - [x] 3.3 Property test: malformed snapshot extensions are rejected
    - **Property 10: Malformed snapshot extensions are rejected**
    - **Validates: Requirements 7.6, 7.7**

- [x] 4. Checkpoint: kernel and storage compile, lint and pass their tests

- [x] 5. Runtime
  - [x] 5.1 Heartbeat commit: one `now` per OCC attempt sets `last_heartbeat_at` and is passed to `activity_tracking.record_heartbeat`
    - _Requirements: 4.1, 4.2_
  - [x] 5.2 Failure with details: `fail_activity_task` takes `last_heartbeat_details`; `commit_activity_retry` takes `progress` and applies it before the `reset_heartbeats` check; the timeout scanner passes `None`
    - _Requirements: 4.3, 4.5_
  - [x] 5.3 Retry preparation clears `last_heartbeat_at` with `heartbeat_details` when `reset_heartbeats` is set
    - _Requirements: 4.5, 4.6_
  - [x] 5.4 `evaluate_activity_timeout` runs the heartbeat clock from `max(started_at, last_heartbeat_at)`
    - _Requirements: 5.1, 5.2_
  - [x] 5.5 `ActivitySweepEntry.last_heartbeat_at`, filled by `recovery_entries`; the Sweep and the reset-successor path insert tracking with the state's time
    - _Requirements: 5.3, 5.4_
  - [x] 5.6 Property test: the heartbeat deadline is v1.31.0's
    - **Property 6: The heartbeat deadline is v1.31.0's**
    - **Validates: Requirements 5.1, 5.2**
  - [x] 5.7 Property test: the time follows progress records and resets, against a reference model
    - **Property 7: The time follows progress records and resets**
    - **Validates: Requirements 4.1, 4.3, 4.5, 4.6, 4.7**
  - [x] 5.8 Property test: the Sweep restores the time (runtime-sweeper-recovery Property 9 extended)
    - **Property 8: The Sweep restores the time**
    - **Validates: Requirement 5.3**
  - [x] 5.9 Unit test: the heartbeat commit's persisted time equals the tracking time
    - _Requirements: 4.2_
  - [x] 5.10 Integration tests: after a takeover, an activity that heartbeated within its timeout is not timed out at the first scan, and one that stopped heartbeating still is
    - _Requirements: 5.1, 5.3_

- [x] 6. Edge and engine
  - [x] 6.1 `translate::RespondActivityTaskFailedRequest.last_heartbeat_details`, mapped by `respond_activity_failed_to_edge`; `WorkflowRuntimeApi::fail_activity_task` and its implementations take it; the by-id path passes `None`
    - _Requirements: 4.3, 4.4_
  - [x] 6.2 `PendingActivityDescription.last_heartbeat_at`, filled by the engine's Describe builder; `pending_activity_to_proto` sets `last_heartbeat_time`
    - _Requirements: 6.1, 6.2, 6.3_
  - [x] 6.3 Unit tests: the failure's details reach the runtime; a by-id failure with details changes neither details nor time; Describe with and without a time, and with details but no time
    - _Requirements: 4.4, 6.1, 6.2, 6.3_

- [x] 7. Documentation: describe the State_Extension in `docs/crates/storage.md`, and the heartbeat time in the kernel's activity documentation
  - _Requirements: 1.9, 1.10, 1.11_

- [x] 8. Final checkpoint: the full §10.4 bar

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["1.3", "2.2", "2.3"] },
    { "id": 3, "tasks": ["2.4", "2.5", "2.6", "2.7", "2.8", "2.9", "3.1"] },
    { "id": 4, "tasks": ["3.2", "3.3"] },
    { "id": 5, "tasks": ["4"] },
    { "id": 6, "tasks": ["5.1", "5.2", "5.3", "5.4", "5.5"] },
    { "id": 7, "tasks": ["5.6", "5.7", "5.8", "5.9", "5.10", "6.1", "6.2"] },
    { "id": 8, "tasks": ["6.3", "7"] },
    { "id": 9, "tasks": ["8"] }
  ]
}
```

## Notes

- All property tests are required.
- No schema migration. The envelope version, `WorkflowState`'s positional layout, the snapshot format version, the `activity_state` blob and the history batch blob do not change.
- The change needs a `Fixed` release note: a takeover no longer times out an activity that is heartbeating, and Describe reports `last_heartbeat_time`.
