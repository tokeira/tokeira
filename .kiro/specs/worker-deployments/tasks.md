# Implementation Plan: Worker Deployments (v2 surface + versioning routing)

## Overview

Implement the Worker Deployment v2 surface and make Tokeira the owner of worker-versioning
routing application, strictly per `design.md`. Work flows in the design's dependency order:
durable storage first (`WorkerDeploymentRepository`), then pure per-run kernel state, then the
runtime registry state machine and dispatch routing, then the edge handlers/adapter and the
describe projection, finishing with the cleanup, compatibility-matrix, and integration work.
Every mutation path follows load → validate (pure) → CAS-commit; the kernel stays pure; the edge
talks to the runtime only through the new `WorkerDeploymentRuntimeApi` adapter. All 25 correctness
properties from the design are implemented as required `proptest` tasks (minimum 100 iterations),
each placed in the crate the design specifies.

## Tasks

- [x] 1. Storage: durable `WorkerDeploymentRepository`
  - [x] 1.1 Define registry storage models and trait in `crates/tokeira-storage/src/api.rs`
    - Add `StoredWorkerDeployment`, `StoredRoutingConfig`, `StoredVersion`, `DrainageInfo`, and the supporting key/value types (`DeploymentKey`, `DeploymentName`, `BuildId`, `ConflictToken`, `DeploymentCasResult`) derived field-for-field from `proto/upstream/temporal/api/deployment/v1/message.proto` as enumerated in the design's Data Models section (embed the version map inside the parent deployment record so one CAS write covers a routing change plus the version-status changes it implies).
    - Add the `#[async_trait] WorkerDeploymentRepository` trait with `load_deployment`, CAS `put_deployment(record, expected: Option<ConflictToken>)` (`expected == None` means must-not-exist), `delete_deployment(key, expected)`, `list_deployments(namespace_id, after, limit)`, and `list_all_for_namespace(namespace_id)` for restart recovery.
    - Model `ConflictToken` as an opaque `[u8; N]` encoding of a per-deployment monotonic generation counter; all public types derive `Debug` and serializable types derive `Serialize, Deserialize`; library errors via `thiserror` (no `.unwrap()` outside tests).
    - _Requirements: 1.1, 1.4, 1.5, 2.1, 13.1, 13.2, 13.6_
  - [x] 1.2 Implement the in-memory store in `crates/tokeira-storage/src/memory.rs`
    - Hold a `HashMap<DeploymentKey, StoredWorkerDeployment>` under the existing `Mutex<StoreState>`; implement the trait's CAS semantics: `put_deployment` applies only when the stored generation equals `expected` (or the record is absent for create), returning `DeploymentCasResult::{Applied,Conflict,NotFound,AlreadyExists}`; `delete_deployment` is conditional on the supplied token; `list_*` honor `after`/`limit` ordering.
    - _Requirements: 1.1, 1.5, 1.6, 2.1, 13.1, 13.4_
  - [x] 1.3 Implement the DSQL store in `crates/tokeira-storage/src/dsql/`
    - Add a `worker_deployments` table keyed by `(namespace_id, deployment_name)` with a `conflict_token` column for conditional writes; implement the trait with a single-document-per-deployment CAS write conditioned on `conflict_token`, plus `list_deployments` (paged) and `list_all_for_namespace` (full reload).
    - Mirror the in-memory CAS result mapping so both backends are behaviorally identical.
    - _Requirements: 1.5, 2.1, 13.1, 13.2, 13.4_
  - [x] 1.4 Property test: registry restart-recovery round-trip
    - Add alongside `crates/tokeira-storage/src/preservation_property_tests.rs` using the workspace-standard `proptest` (≥100 iterations); do not hand-roll property infrastructure.
    - **Property 17: Registry restart-recovery round-trip**
    - Generator: arbitrary registry state (deployments, versions, routing configs with `revision_number`, version metadata, compute configs, manager identities, drainage state). Invariant: persisting then reloading via `list_all_for_namespace` yields a registry equal to the original, and a `ConflictToken` issued before reload is evaluated against the reloaded state with identical CAS accept/reject semantics.
    - **Validates: Requirements 13.1, 13.2, 13.3, 13.4**
  - [x] 1.5 Unit tests for store CAS and pagination
    - In `crates/tokeira-storage/src/memory.rs` (and a DSQL-gated equivalent), cover: create-on-existing returns `AlreadyExists`; stale-token write returns `Conflict` and leaves state unchanged; current-token/`None`-token write applies and advances the generation; `list_deployments` pages exactly once per record with no duplicates/omissions and an empty continuation marks exhaustion.
    - _Requirements: 1.5, 1.6, 13.4_

- [x] 2. Kernel: per-run versioning state (pure)
  - [x] 2.1 Replace the `VersioningOverride` placeholder with populated per-run state in `crates/tokeira-kernel/src/state.rs`
    - Replace the fieldless `VersioningOverride` and define `WorkflowVersioningInfo` (`behavior`, `deployment_version`, `versioning_override`, `version_transition`, `revision_number`, `continue_as_new_initial_versioning_behavior`), `WorkerDeploymentVersionRef { deployment_name, build_id }`, and the populated `VersioningOverride` enum (`Pinned { version }`, `AutoUpgrade`) exactly as in the design's Components section.
    - Add `#[serde(default)] pub versioning_info: Option<WorkflowVersioningInfo>` (absent == unversioned) and `#[serde(default)] pub worker_deployment_name: Option<String>` to `WorkflowState`; initialize both at every construction site (start, signal-with-start, continue-as-new/reset successor). No I/O, async, metrics, or storage.
    - _Requirements: 9.2, 9.3, 10.1, 13.5_
  - [x] 2.2 Add the pure versioning transition methods in `crates/tokeira-kernel/src/state.rs`
    - `start_version_transition(target, revision_number)`: set `version_transition`, clear sticky affinity, mark the pending WFT for reschedule, set `revision_number`; reject when effective behavior is PINNED (Tokeira analog of `ErrPinnedWorkflowCannotTransition`, `mutable_state_impl.go @ v1.31.0`).
    - `apply_wft_versioning(behavior, deployment_version, worker_deployment_name)`: clear `version_transition` when its target equals the completing version; UNSPECIFIED behavior clears `deployment_version` (unversioned); otherwise set `behavior`, `deployment_version`, and `worker_deployment_name` (analog of `afterAddWorkflowTaskCompletedEvent @ v1.31.0`).
    - `effective_deployment()` / `effective_behavior()`: pure precedence functions (transition > override > behavior + deployment_version), the Tokeira analog of `GetEffectiveDeployment` / `GetEffectiveVersioningBehavior` (`service/history/workflow/util.go @ v1.31.0`).
    - _Requirements: 9.1, 9.2, 9.3, 9.5, 9.6, 9.8_
  - [x] 2.3 Author versioning into history and restore on replay in `command.rs`, `event.rs`, `kernel.rs`
    - Extend `StartRequest` `ExecutionOptions` in `crates/tokeira-kernel/src/command.rs` so `versioning_override` carries the populated `VersioningOverride`; add defaulted versioning fields to the `HistoryEventKind::WorkflowExecutionStarted` envelope in `crates/tokeira-kernel/src/event.rs`.
    - In `crates/tokeira-kernel/src/kernel.rs`, author the versioning fields onto `WorkflowExecutionStarted` from the start request and restore them by destructuring the start event in `replay_history_prefix`; runs whose start event lacks the fields restore to unversioned (defaults).
    - Extend `WorkflowTaskCompletedRequest` in `command.rs` with `deployment_version` and `versioning_behavior` so `apply_wft_versioning` runs deterministically on both the live transition and replay.
    - _Requirements: 9.2, 13.5_
  - [x] 2.4 Property test: per-run versioning replay round-trip
    - Add to `crates/tokeira-kernel/tests/property_tests.rs` using `proptest` (≥100 iterations).
    - **Property 18: Per-run versioning replay round-trip**
    - Generator: arbitrary `WorkflowVersioningInfo` authored into a `WorkflowExecutionStarted` plus a sequence of WFT completions. Invariant: replaying the history restores an equal `WorkflowVersioningInfo`, so `effective_deployment()` / `effective_behavior()` post-restart match the pre-restart decisions.
    - **Validates: Requirements 13.5**
  - [x] 2.5 Unit tests for precedence, transition rules, and serde migration
    - In `crates/tokeira-kernel/src/state.rs` `#[cfg(test)]`: PINNED precedence over override and override over behavior; PINNED rejects `start_version_transition`; `apply_wft_versioning` with UNSPECIFIED clears `deployment_version`; transition cleared only when target matches completing version.
    - Serde migration: an older `WorkflowState` document lacking `versioning_info` / `worker_deployment_name` deserializes with defaults (unversioned).
    - _Requirements: 9.5, 9.6, 10.4, 13.5_

- [x] 3. Checkpoint — storage + kernel foundations
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p tokeira-storage` and `cargo test -p tokeira-kernel`.
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Runtime: deployment registry state machine
  - [x] 4.1 Scaffold `DeploymentRegistry` and `RegistryError` in `crates/tokeira-runtime/src/deployment_registry.rs` (new)
    - Create the module holding `Arc<dyn WorkerDeploymentRepository>`, a handle to the live `WorkerRegistry` (`crates/tokeira-runtime/src/worker_registry.rs`) for poller presence, and a clock; register it in `crates/tokeira-runtime/src/lib.rs`.
    - Define the command/view DTOs (`CreateDeployment`, `DescribeVersion`, `SetCurrent`, `SetRamping`, `DeploymentView`, `VersionView`, `DeploymentPage`, `SetCurrentOutcome`, `SetRampingOutcome`, `SetManagerOutcome`, `VersionMetadataView`) and the `thiserror` `RegistryError` with variants `AlreadyExists`, `NotFound`, `FailedPrecondition(reason)`, `ResourceExhausted`, `InvalidArgument(reason)`.
    - Implement the shared `load → validate (pure) → CAS-commit` loop helper: load the record with its current `conflict_token`, evaluate preconditions on the loaded snapshot, persist with `put_deployment` keyed on the loaded token, and on CAS failure reload and re-validate so a rejected request never partially mutates state.
    - _Requirements: 12.4, 13.1, 13.4_
  - [x] 4.2 Implement deployment CRUD on `DeploymentRegistry`
    - `create_deployment` (idempotent on `request_id`, `ALREADY_EXISTS` on existing name), `describe_deployment` (project every stored field), `delete_deployment` (`FAILED_PRECONDITION` when versions remain, success no-op when missing), `list_deployments` (opaque page token over `list_deployments`, clamping out-of-range `page_size` to max).
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.9, 1.10_
  - [x] 4.3 Implement version CRUD on `DeploymentRegistry`
    - `create_version` (non-empty `build_id` + name under an existing parent → `CREATED`, `NOT_FOUND` when the parent deployment is missing, `ALREADY_EXISTS` on dup, `RESOURCE_EXHAUSTED` past the max-versions limit, idempotent `request_id`, empty `request_id` generated), `describe_version` (project all fields incl. `version_task_queues` with stats gated on `report_task_queue_stats`), `delete_version` (success no-op when missing; otherwise precondition: not Current/Ramping, no active pollers, drained unless `skip_drainage`).
    - Add the legacy `"<deployment_name>.<build_id>"` / `"<deployment_name>:<build_id>"` parse/format helper plus `__unversioned__` and empty-string handling used by describe/delete/metadata when `deployment_version` is absent.
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.7, 2.8, 2.9, 2.10, 2.11, 2.12, 2.13, 2.15_
  - [x] 4.4 Implement routing-config selection on `DeploymentRegistry`
    - `set_current_version`: set `current_deployment_version`, update `current_version_changed_time`, bump `revision_number`; empty `build_id` sets nil; setting Current to the currently-Ramping version atomically unsets Ramping; return fresh token + deprecated `previous_*`.
    - `set_ramping_version`: set `ramping_deployment_version`, `ramping_version_percentage` (validate [0,100]), update changed-times, bump `revision_number`; reject ramping == non-nil Current with `FAILED_PRECONDITION`; return fresh token + deprecated `previous_*`.
    - _Requirements: 3.1, 3.2, 3.3, 3.7, 3.8, 4.1, 4.2, 4.3, 4.4, 4.8_
  - [x] 4.5 Implement conflict-token CAS and manager-identity enforcement on `DeploymentRegistry`
    - On every mutating method, reject a supplied non-nil token that mismatches the loaded generation with `FAILED_PRECONDITION` (no mutation); a nil token bypasses the check; a successful commit yields a new distinct token.
    - Enforce `manager_identity` only on set-current-version, set-ramping-version, and delete-version when set (`FAILED_PRECONDITION`, Tokeira analog of `ErrManagerIdentityMismatch`); implement `set_manager` for the `manager_identity` / empty-unset / `self=true` oneof arms returning fresh token + deprecated `previous_manager_identity` without requiring the acting identity to equal the existing manager.
    - _Requirements: 3.4, 4.5, 7.1, 7.2, 7.3, 7.5, 7.6, 7.7, 13.4_
  - [x] 4.6 Implement poller-presence preconditions on `DeploymentRegistry`
    - Derive "task queues polled by a version" from the durable `polled_task_queues` set (updated via the existing `WorkerRegistry` registration hook).
    - `allow_no_pollers=false` → unknown build_id rejected as `NOT_FOUND` (`errVersionNotFound` mapping); `true` → auto-create the version. `ignore_missing_task_queues=false` for set-current → versioned new version must poll every task queue the previous current version polled (`FAILED_PRECONDITION`); for set-ramping the same check runs only when the ramping version changes, compared against the current version.
    - _Requirements: 3.5, 3.6, 4.6, 4.7_
  - [x] 4.7 Implement compute-config update/validate and version metadata on `DeploymentRegistry`
    - `update_compute_config`: apply each `ComputeConfigScalingGroupUpdate` honoring `update_mask` (empty mask on existing group = no-op; mask paths restricted to the documented set; mask ignored for a new group); apply removals; reject a group in both update and remove and unknown mask paths with `InvalidArgument`; idempotent on `request_id`.
    - `validate_compute_config`: run the same validation without applying (stored registry state byte-identical after the call) and without requiring the Version to exist.
    - `update_version_metadata`: apply `upsert_entries` / `remove_entries` (reject a key in both sets), record `last_modifier_identity`, return full metadata.
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7, 5.8, 5.9, 6.1, 6.2, 6.3, 6.4, 6.5_
  - [x] 4.8 Implement the drainage lifecycle on `DeploymentRegistry`
    - On a version leaving Current/Ramping with open pinned workflows targeting it → `DRAINING` + `last_changed_time`; once none remain → `DRAINED` + `last_changed_time`; becoming Current/Ramping again clears `drainage_info`; a recompute records `last_checked_time`; never populate `drainage_info` while Current/Ramping (`version_workflow.go @ v1.31.0`).
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_
  - [x] 4.9 Record identity propagation on every write path
    - Set `last_modifier_identity` on the affected deployment/version for every write carrying a non-empty `identity`; `set_manager` with `self=true` records it as `manager_identity`.
    - _Requirements: 12.1, 6.5_
  - [x] 4.10 Property test: deployment CRUD correctness
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations); reference-model comparison.
    - **Property 1: Deployment CRUD correctness**
    - Generator: sequences of create/describe/delete (incl. duplicate names, repeated `request_id`, deletes of version-free and version-bearing deployments, reads/mutations on unknown names, and missing-target delete no-ops). Invariant: observable state matches the reference model and error mapping (`ALREADY_EXISTS` / `FAILED_PRECONDITION` / `NOT_FOUND` / delete success no-op).
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6, 1.7, 1.9, 1.10**
  - [x] 4.11 Property test: deployment list pagination round-trip
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 2: Deployment list pagination round-trip**
    - Generator: a set of deployments and any `page_size` including non-positive and over-max values. Invariant: paging with the returned `next_page_token` yields exactly one summary per deployment (no duplicates/omissions), an empty token marks exhaustion, and out-of-range `page_size` is clamped rather than rejected.
    - **Validates: Requirements 1.5**
  - [x] 4.12 Property test: version CRUD and deletion-precondition correctness
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations); reference-model comparison.
    - **Property 3: Version CRUD and deletion-precondition correctness**
    - Generator: version create/describe/delete sequences incl. missing parent deployment, duplicate (name,build_id), empty and repeated `request_id`, `report_task_queue_stats` toggles, missing-target delete no-op, and deletes under each precondition combination. Invariant: observable state and error mapping match the reference model.
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.7, 2.8, 2.10, 2.11, 2.12, 2.13, 2.15**
  - [x] 4.13 Property test: deprecated version-string round-trip
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 4: Deprecated version-string round-trip**
    - Generator: (deployment_name, build_id) pairs without delimiter conflicts, both `.` and `:` delimiters, `__unversioned__`, empty string, plus malformed strings. Invariant: format-then-resolve identifies the same version; unversioned sentinels resolve to nil; only non-matching strings are rejected with `INVALID_ARGUMENT`.
    - **Validates: Requirements 2.9**
  - [x] 4.14 Property test: routing-config state machine
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 5: Routing-config state machine**
    - Generator: sequences of set-current / set-ramping (incl. empty build_id, ramping==current, percentages in/out of range). Invariant: routing config evolves per v1.31.0 rules (set-current-to-ramping unsets ramping; revision bumps; fresh token + correct `previous_*`).
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.7, 3.8, 4.1, 4.2, 4.4, 4.8**
  - [x] 4.15 Property test: conflict-token CAS rejects stale writes without mutation
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 6: Conflict-token CAS rejects stale writes without mutation**
    - Generator: any mutating RPC with stale / current / nil tokens. Invariant: stale non-nil token → `FAILED_PRECONDITION` with state unchanged; current-or-nil token → accepted with a new distinct token.
    - **Validates: Requirements 3.4, 4.5, 7.6, 13.4**
  - [x] 4.16 Property test: poller-presence preconditions
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 7: Poller-presence preconditions**
    - Generator: set-current/set-ramping with `allow_no_pollers` and `ignore_missing_task_queues` toggles over varied poller/task-queue sets. Invariant: the v1.31.0 guard semantics hold (`allow_no_pollers=false` unknown version → `NOT_FOUND`, true → auto-create; missing-task-queue rejection; ramping check only on change vs current).
    - **Validates: Requirements 3.5, 3.6, 4.6, 4.7**
  - [x] 4.17 Property test: compute-config update and validate
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations); reference-model comparison.
    - **Property 8: Compute-config update and validate**
    - Generator: sequences of compute-config updates/removals with varied `update_mask` paths and validate calls naming existing and missing versions. Invariant: resulting scaling-group map matches the reference model under mask semantics; validate leaves stored state byte-identical and does not require version existence.
    - **Validates: Requirements 5.1, 5.2, 5.5, 5.6, 5.7, 5.9**
  - [x] 4.18 Property test: version metadata CRUD
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations); reference key-value model.
    - **Property 9: Version metadata CRUD**
    - Generator: upsert/remove sequences. Invariant: resulting `VersionMetadata.entries` match the reference model and the response equals the stored entries.
    - **Validates: Requirements 6.1, 6.2, 6.4**
  - [x] 4.19 Property test: manager identity and authorization
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 10: Manager identity and authorization**
    - Generator: deployments with/without a set manager, set-current/set-ramping/delete-version under varied identities, plus the three `set_manager` oneof arms. Invariant: mismatched identity is rejected only for set-current/set-ramping/delete-version; set-manager is not gated by the existing manager; each oneof arm yields the corresponding stored `manager_identity`; success returns fresh token + prior manager.
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.5, 7.7**
  - [x] 4.20 Property test: drainage lifecycle
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 11: Drainage lifecycle**
    - Generator: versions transitioning in/out of Current/Ramping with varied open-pinned-workflow counts and recompute events. Invariant: DRAINING→DRAINED lifecycle, clear-on-reactivate, `last_checked_time` on recompute, and never-populated-while-Current/Ramping all hold.
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.5, 8.6**
  - [x] 4.21 Property test: no mutation on rejected request
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 16: No mutation on rejected request**
    - Generator: requests rejected for every reason (invalid argument, not found, failed precondition, already exists, resource exhausted, manager mismatch, conflict-token mismatch), plus delete requests against missing targets. Invariant: the durable registry state is byte-identical before and after rejected calls, and missing-target deletes are accepted success no-ops with no state change.
    - **Validates: Requirements 12.4**
  - [x] 4.22 Property test: identity propagation
    - In `crates/tokeira-runtime/src/deployment_registry.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 15: Identity propagation**
    - Generator: write RPCs carrying non-empty `identity` (incl. `set_manager` `self=true`). Invariant: the affected record's `last_modifier_identity` equals that identity (and `manager_identity` for `self=true`).
    - **Validates: Requirements 12.1, 6.5**

- [x] 5. Checkpoint — runtime registry state machine
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint`.
  - Run `cargo test -p tokeira-runtime`.
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Runtime: dispatch routing integration
  - [x] 6.1 Resolve the target version from routing config in `crates/tokeira-runtime/src/runtime/workflow_task.rs`
    - At task-start, resolve the workflow's target version from the deployment registry routing config: AUTO_UPGRADE / unversioned traffic follows `current_deployment_version`, with `ramping_version_percentage`% bucketed deterministically by workflow id (reuse the FNV-1a `deterministic_bucket` in `crates/tokeira-runtime/src/versioning.rs`) routed to `ramping_deployment_version`; PINNED runs (or PINNED override) resolve to their pinned version regardless of routing config; a nil Current routes AUTO_UPGRADE/unversioned traffic to unversioned workers.
    - _Requirements: 9.1, 9.3, 9.4, 9.8_
  - [x] 6.2 Start the version transition at workflow-task start in `crates/tokeira-runtime/src/runtime/workflow_task.rs`
    - When the polling worker's deployment version differs from the workflow's effective version and the run is not pinned, call `start_version_transition` gated on the dispatch `revision_number` (task-start by a differing poller, matching `recordworkflowtaskstarted/api.go @ v1.31.0`); pinned runs do not transition.
    - _Requirements: 9.5, 9.6_
  - [x] 6.3a Add a kernel command to start a deployment-version transition independently of WFT-start, in `crates/tokeira-kernel/src/command.rs` and `kernel.rs`
    - Add `Command::StartDeploymentTransition(StartDeploymentTransitionRequest { target: WorkerDeploymentVersionRef, revision_number: i64 })`. This is the design resolution for 6.3: the only existing path that calls `state.start_version_transition` is `Command::WorkflowTaskStarted`, which requires an existing `pending_workflow_task` (else `Reject::NoPendingWorkflowTask`); the activity-start path has no such pending WFT. A dedicated command keeps transition-initiation authoritative in the kernel (AGENTS.md §2/§3) rather than mutating per-run state from the runtime.
    - Kernel handler: load the open run; if effective behavior is PINNED → `Reject::PinnedWorkflowCannotTransition` (reuse existing reject). Otherwise call `state.start_version_transition(target, revision_number)` (existing pure transition — sets `version_transition`, clears sticky, marks an existing pending WFT for reschedule). **If there is no pending WFT**, additionally call the existing private `schedule_workflow_task()` so a WFT exists to drive the transition; if one is already pending, do not double-schedule. This composes two existing kernel primitives — nothing new is invented.
    - This is the faithful analog of v1.31.0 `recordactivitytaskstarted/api.go:75 @ v1.31.0`, which returns `UpdateWorkflowAction{ CreateWorkflowTask: rejectCode == rejectCodeStartedTransition }` after `StartDeploymentTransition` (`mutable_state_impl.go @ v1.31.0`): start the transition and ensure a WFT exists to drive it, in one authoritative mutation. Author event-sourced and replay-safe; cite the anchors in the command doc comment per §8/§9.
    - Unit tests: pinned-run reject; no-pending-WFT schedules a WFT; existing-pending-WFT does not double-schedule; `version_transition` and `revision_number` set correctly.
    - _Requirements: 9.5, 9.6_
  - [x] 6.3 Reject transition-triggering activity-task starts in `crates/tokeira-runtime/src/runtime/activity.rs`
    - Apply the differing-poller transition trigger with the `revision_number > wft_dispatch_revision` gate; when activity start triggers a transition, start it by submitting `Command::StartDeploymentTransition` (6.3a) on the run's owned shard, then reject/drop the activity task for later reschedule; reject activity starts while a transition is already in flight; pinned-workflow independent activities do not transition (matching `recordactivitytaskstarted/api.go:188 @ v1.31.0`). The WFT-target version + revision operands are computed live from the routing config (reuse `resolve_workflow_task_target_version` / `load_worker_deployment_routing_config` from `workflow_task.rs`); the activity dispatch revision is threaded through `DispatchableActivityTask` / `DispatchOp::EnqueueActivityTask` (serde-default). Do NOT store a revision on `ActivityState`.
    - Depends on 6.3a.
    - _Requirements: 9.5, 9.6_
  - [x] 6.4 Apply versioning at WFT completion and route eager tasks in `crates/tokeira-runtime/src/runtime/workflow_task.rs` and `crates/tokeira-runtime/src/publisher.rs`
    - On WFT completion call `apply_wft_versioning` to update the run's `behavior`, `deployment_version`, and `worker_deployment_name` and clear the in-flight transition when the completing version matches the transition target. Do **NOT** touch the run's `revision_number` here. CORRECTION (verified against v1.31.0): the run's `WorkflowExecutionVersioningInfo.revision_number` is **set** only at transition-start (`mutable_state_impl.go:9108 @ v1.31.0`, from the task's `TaskDispatchRevisionNumber`) and on start-time auto-upgrade inheritance (`mutable_state_impl.go:2963 @ v1.31.0`); `afterAddWorkflowTaskCompletedEvent` (`workflow_task_state_machine.go:1283-1396 @ v1.31.0`) never assigns `RevisionNumber`. The kernel's `apply_wft_versioning` in `state.rs` already does not touch `revision_number` — keep it that way. (The earlier "increment `revision_number` when the run routes to a new deployment version" instruction conflated the run revision with the registry-level `RoutingConfig.revision_number`, which IS bumped per set-current/set-ramping in 4.4 — that registry counter is unrelated to this task.)
    - When `eager_worker_deployment_options` is present and `request_eager_execution` is true, route the eager first task per those deployment options, otherwise no routing effect. Routing decisions remain derived effects of durable registry + per-run state (no correctness weight on transient queues).
    - _Requirements: 9.2, 9.6, 9.7, 13.6_
  - [x] 6.5 Property test: routing determinism and effective-version precedence
    - In a routing module under `crates/tokeira-runtime/src/` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 12: Routing determinism and effective-version precedence**
    - Generator: routing configs, per-run versioning state, and workflow ids. Invariant: deterministic target; precedence transition > override > behavior + deployment_version; ramp fraction split by id; nil Current → unversioned.
    - **Validates: Requirements 9.1, 9.3, 9.4, 9.8**
  - [x] 6.6 Property test: deployment-version transition lifecycle
    - In a routing/dispatch module under `crates/tokeira-runtime/src/` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 13: Deployment-version transition lifecycle**
    - Generator: runs and workflow/activity task-starts by pollers with differing deployment versions, plus WFT completions. Invariant: unpinned WFT starts start a revision-gated transition that **sets** the run's `revision_number` to the task's dispatch revision; transition-triggering activity starts are rejected/dropped and later rescheduled; activity starts during an in-flight transition are rejected; pinned-workflow independent activities do not transition; WFT completion updates effective behavior/deployment/`worker_deployment_name` (UNSPECIFIED → unversioned) and clears the transition on target match **without modifying the run's `revision_number`** (per v1.31.0 `afterAddWorkflowTaskCompletedEvent`, which never assigns `RevisionNumber`).
    - **Validates: Requirements 9.2, 9.5, 9.6**

- [x] 7. Edge: adapter, errors, and translation
  - [x] 7.1 Add the `WorkerDeploymentRuntimeApi` adapter trait and outcome type
    - Define `WorkerDeploymentRuntimeApi` in `crates/tokeira-edge/src/workflow_service.rs` (analogous to `WorkflowRuntimeApi`) with one async method per v2 RPC taking translated request DTOs and returning view DTOs or `EdgeError`; define the edge-adapter outcome `DeploymentMutationOutcome { conflict_token, view }`, distinct from the concrete runtime `CommitResult` (mirroring `WorkflowMutationOutcome` vs `CommitResult`).
    - Implement the trait on `RuntimeAdapter` in `crates/tokeira-edge/src/grpc/runtime_adapter.rs`, delegating to `DeploymentRegistry`; the edge never touches storage or runtime internals directly.
    - _Requirements: 12.4, 13.1_
  - [x] 7.2 Add new `EdgeError` variants in `crates/tokeira-edge/src/errors.rs`
    - Add `AlreadyExists` and `ResourceExhausted` with `status_code` + `action_name`; reuse `FailedPrecondition`, `NamespaceNotFound`, and the existing not-found/invalid-argument variants. Do not use `EdgeError::Internal` for any of these user-facing conditions.
    - _Requirements: 1.2, 2.4, 2.5_
  - [x] 7.3 Wire the new variants in `crates/tokeira-edge/src/grpc/errors.rs`
    - Map `AlreadyExists` → tonic `ALREADY_EXISTS`, `ResourceExhausted` → `RESOURCE_EXHAUSTED`, and confirm `FailedPrecondition`/`NamespaceNotFound`/not-found/invalid-argument map to `FAILED_PRECONDITION`/`NOT_FOUND`/`NOT_FOUND`/`INVALID_ARGUMENT`; confirm `grpc_error_code` emits the matching labels.
    - _Requirements: 1.2, 1.11, 2.4, 2.5, 12.2_
  - [x] 7.4 Add free translation functions for the deployment DTOs in `crates/tokeira-edge/src/grpc/translate.rs`
    - Add request→DTO and view→proto free functions (matching the `respond_activity_completed_to_edge` pattern; no `TryFrom`) for `WorkerDeploymentInfo`, `WorkerDeploymentSummary`, `WorkerDeploymentVersionInfo`, `VersionTaskQueue`, `RoutingConfig`, `VersionDrainageInfo`, `VersionMetadata`, `ComputeConfig`, and the set-current/ramping/manager responses incl. the deprecated `previous_*` fields.
    - _Requirements: 1.4, 1.5, 2.7, 2.8, 3.7, 4.8, 6.4, 7.7, 8.6_
  - [x] 7.5 Replace the 13 `deferred_unary!("worker-deployments")` handlers with real handlers in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Implement `create_worker_deployment`, `describe_worker_deployment`, `delete_worker_deployment`, `list_worker_deployments`, `create_worker_deployment_version`, `describe_worker_deployment_version`, `delete_worker_deployment_version`, `set_worker_deployment_current_version`, `set_worker_deployment_ramping_version`, `update_worker_deployment_version_compute_config`, `validate_worker_deployment_version_compute_config`, `update_worker_deployment_version_metadata`, `set_worker_deployment_manager`. Each handler resolves the namespace via `resolve_namespace_id` (→ `NOT_FOUND`), validates required identifiers where v1.31.0 does so (`deployment_name`, `build_id`, legacy `version` string, percentage range, oneof set, non-empty identity) → `INVALID_ARGUMENT` before any mutation, lets list `page_size` clamp rather than error, lets validate-compute skip version-existence lookup, calls the adapter, and translates the view with the free functions. None of the 13 returns `UNIMPLEMENTED`.
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.8, 1.11, 2.1, 2.7, 2.8, 2.14, 3.1, 3.2, 4.1, 4.3, 5.8, 5.9, 6.1, 7.4, 7.8, 12.2, 12.3, 12.5_
  - [x] 7.6 Replace the 5 deprecated `Deployment` companion handlers in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Make `describe_deployment`, `list_deployments`, `get_deployment_reachability`, `get_current_deployment`, `set_current_deployment` each return `Status::unimplemented("Deployments are deprecated and no longer supported, use Worker Deployments instead")` (the exact v1.31.0 message) before any state access; they do not route through the adapter.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_
  - [x] 7.7 Unit tests for edge validation and deprecated-companion messages in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Cover: each deprecated companion returns the exact `UNIMPLEMENTED` message and touches no registry state; empty `deployment_name` / unset oneof / empty identity / unresolvable version → `INVALID_ARGUMENT`; namespace-not-found → `NOT_FOUND`; max-version admission evicts the oldest eligible Version before returning `RESOURCE_EXHAUSTED` when none qualifies; overlapping upsert/remove and update/remove → `INVALID_ARGUMENT`; `eager_worker_deployment_options` applied iff `request_eager_execution`; all 13 v2 RPCs accept valid input without `UNIMPLEMENTED`.
    - _Requirements: 1.8, 2.5, 2.14, 5.3, 6.3, 7.4, 7.8, 9.7, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 12.2, 12.5_

- [x] 8. Edge: describe versioning-info projection
  - [x] 8.1 Fill `versioning_info` + `worker_deployment_name` in the describe projection in `crates/tokeira-edge/src/grpc/translate.rs`
    - Populate `WorkflowExecutionInfo.versioning_info` (behavior, deployment_version, versioning_override, version_transition, revision_number, continue_as_new_initial_versioning_behavior) and `worker_deployment_name` from the per-run `WorkflowVersioningInfo` (the seam `api-conformance-workflow-describe` leaves default), using the same run snapshot; leave deprecated `assigned_build_id` / `inherited_build_id` / `most_recent_worker_version_stamp` default; absent versioning state ⇒ leave `versioning_info` and `worker_deployment_name` default with no fabricated placeholders.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_
  - [x] 8.2 Property test: versioning-info projection fidelity
    - In `crates/tokeira-edge/src/grpc/translate.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 14: Versioning-info projection fidelity**
    - Generator: arbitrary per-run versioning state incl. the absent case. Invariant: `DescribeWorkflowExecution` projects `versioning_info` + `worker_deployment_name` exactly, leaves the deprecated build-id/version-stamp fields default, and leaves both default (no placeholders) when there is no versioning state.
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4**

- [x] 9. Cleanup: re-point mis-grouped worker-observability RPCs
  - [x] 9.1 Move `describe_worker` / `list_workers` out of the `worker-deployments` block in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Re-point `describe_worker` and `list_workers` from the `deferred_unary!("worker-deployments")` block to their owning worker-observability feature key (they are NOT deployment RPCs), and update the `deferred_handler_blocks_return_tracked_unimplemented_messages` test (and the `assert_deferred_rpc!` usages) so the two RPCs are asserted under their correct owning feature rather than `worker-deployments`.
    - _Requirements: 12.5_

- [x] 10. Compatibility matrix
  - [x] 10.1 Move the `worker-deployments` `FeatureEntry` off `Unsupported` in `crates/tokeira-compatibility/src/matrix.rs`
    - Set the `worker-deployments` entry (id `"worker-deployments"`) to its supported state with evidence (13 v2 RPCs implemented; the 5 deprecated companions counted conformant via their v1.31.0 `UNIMPLEMENTED` behavior); keep `WORKER_DEPLOYMENT_RPCS` accurate.
    - _Requirements: 11.7, 12.5_
  - [x] 10.2 Update the deferred-RPC edge test for the 13 v2 RPCs in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Update `deferred_handler_blocks_return_tracked_unimplemented_messages` so the 13 v2 RPCs are no longer asserted as deferred placeholders (they now have real handlers) and the 5 deprecated companions assert the exact v1.31.0 `UNIMPLEMENTED` message.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 12.5_

- [x] 11. Integration tests
  - [x] 11.1 Edge → adapter → registry → storage integration in `crates/tokeira-edge/tests/`
    - Exercise the full path for a representative RPC of each family: create/describe deployment, create version, set-current with ramp-unset, set-ramping, manager mismatch, drainage describe; assert responses and durable state.
    - _Requirements: 1.1, 1.4, 2.1, 3.1, 3.3, 4.1, 7.5, 8.6_
  - [x] 11.2 Restart-recovery integration in `crates/tokeira-edge/tests/` (or `crates/tokeira-runtime/tests/`)
    - Mutate the registry, drop the in-memory runtime, reload from the store via `list_all_for_namespace`, and assert describe/list return the pre-restart state and a pre-restart conflict token is evaluated with identical CAS semantics.
    - _Requirements: 13.1, 13.2, 13.3, 13.4_
  - [x] 11.3 Routing cycle integration in `crates/tokeira-runtime/tests/` (or `apps/tokeirad/tests/`)
    - Drive a start → dispatch → WFT-completion → describe cycle: confirm the version transition is started by a differing poller, the run's `revision_number` is **set** to the task's dispatch revision at transition-start (it is set there, never incremented at WFT completion — see task 6.4 and `mutable_state_impl.go:9108 @ v1.31.0`), the transition is applied/cleared at WFT completion **without** mutating `revision_number` (`afterAddWorkflowTaskCompletedEvent`, `workflow_task_state_machine.go:1283-1396 @ v1.31.0` never assigns it), and the projected `versioning_info` / `worker_deployment_name` reflect the completed routing.
    - _Requirements: 9.1, 9.2, 9.5, 9.6, 10.1, 10.2_

- [x] 12. Final checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint` and `cargo test-lint`.
  - Run `cargo check --workspace` and `cargo test --workspace`.
  - Run `cargo doc --workspace --no-deps` (`RUSTDOCFLAGS="-D warnings"`).
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Tier 8.39 corrective conformance: poll limits, eviction, and drainage timing
  - [x] 13.1 Correct the runtime registry's configurable admission limits
    - Add live runtime accessors for v1.31.0's `MatchingMaxVersionsInDeployment`, `MatchingMaxTaskQueuesInDeploymentVersion`, and `PollerHistoryTTL` defaults; classify the corresponding conformance-only keys as wired without exposing production configuration or involving the kernel.
    - Make poll registration count distinct task-queue family names, reject a new family at the configured limit with the v1.31.0 message, and return the reason through `RegistryError::ResourceExhausted(reason)`.
    - _Requirements: 2.16, 2.17_
  - [x] 13.2 Implement atomic oldest-eligible Version eviction at the configured limit
    - Apply the v1.31.0 `tryDeleteVersion` behavior to explicit create, poll auto-create, and `allow_no_pollers` auto-create: deterministic oldest-first selection, normal routing/poller/drainage gates, manager bypass, and one CAS mutation covering delete + insert.
    - Add Property 19 (at least 100 cases) plus focused examples for exact exhaustion messages and unchanged state on rejection.
    - _Requirements: 2.5, 2.18, 12.4; Property 19_
  - [x] 13.3 Implement due drainage recomputation in the Tokeira runtime shape
    - Add live runtime accessors for the v1.31.0 visibility-grace and refresh-interval defaults and their conformance-only overrides.
    - Before returning a public registry observation, lazily recompute only due `DRAINING` Versions through `RunRepository` and CAS-commit the result; preserve the stale-result reactivation guard and start a fresh grace cycle after rollback/re-demotion.
    - Extend Property 11 and focused tests for before-due no-op, due transition, repeated refresh, and rollback/re-demotion.
    - _Requirements: 8.7, 8.8, 8.9; Property 11_
  - [x] 13.4 Make poll admission authoritative and prove Tier 8.39 clean
    - Propagate Worker Deployment registration errors from workflow/activity poll admission instead of logging them as best-effort bookkeeping; successful registration remains before the long poll.
    - Track each admitted poll with a cancellation-aware runtime liveness guard: client cancellation removes the exact live registration, normal completion retains the recent observation, and the edge's bounded diagnostic history remains a separate concern. Aggregate physical Deployment-Version pollers into the public `DescribeTaskQueue` family view.
    - Remove the four obsolete override-class skips for `TestDeploymentVersionLimits`, `TestDeleteVersion_ServerDeleteMaxVersionsReached`, `TestSetRampingVersion_AfterDrained`, and `TestDrainRollbackedVersion`; retain the internal Force-CAN override-state skip.
    - Run focused runtime/edge/conformance tests, then two consecutive clean `TestWorkerDeploymentSuite` runs and update the functional-conformance ledger.
    - _Requirements: 2.16, 2.17, 2.18, 2.19, 8.7, 8.8, 8.9_

- [x] 14. Tier 8.40 pinned routing, membership, and reactivation
  - [x] 14.1 Add runtime-scoped membership and reactivation caches
    - Construct one shared `DeploymentRegistry` per runtime and reuse it from adapters,
      poll admission, and the dispatch publisher.
    - Cache positive and negative workflow-task membership results by namespace, task
      queue family, deployment, and build id using the v1.31.0 one-second default; cache
      reactivation deduplication by target Version using the ten-second default. Deliver
      the three conformance-only dynamic-config keys at their consult sites without
      exposing production configuration or involving the kernel.
    - _Requirements: 9.11, 14.1, 14.2, 14.4_
  - [x] 14.2 Validate explicit pinned targets across public write paths
    - Apply the shared membership check to start, signal-with-start, direct update,
      batch update, and post-reset option updates before their run mutation commits.
      Preserve negative answers until TTL expiry and return `FAILED_PRECONDITION` with
      no mutation when membership is absent.
    - _Requirements: 14.1, 14.2_
  - [x] 14.3 Apply best-effort reactivation only after successful persistence
    - After a successful concrete pinned operation, use the shared TTL gate and a durable
      registry CAS to change `INACTIVE`/`DRAINED` to `DRAINING`. Disabled, duplicate,
      failed, cleared, auto-upgrade, and already-active paths are no-ops; reactivation
      errors never roll back the committed workflow operation.
    - _Requirements: 14.3, 14.4, 14.5, 14.6_
  - [x] 14.4 Author pinned start routing state and derive broker publication
    - Put the pinned deployment name in live start state and
      `WorkflowExecutionStarted` so replay agrees. Resolve every workflow-task physical
      queue from authoritative run state plus durable registry immediately before broker
      publication; keep the broker disposable.
    - Add focused kernel replay and end-to-end pinned-start routing coverage.
    - On versioned workflow/activity poll admission, re-key any disposable unversioned
      backlog when the newly registered Version is selected by durable Current/Ramping
      routing, so registration racing start publication cannot strand the first task.
    - _Requirements: 9.9, 9.10, 9.12, 13.5, 13.6_
  - [x] 14.5 Complete Tier 8.40 validation and fidelity follow-ups
    - Add/retain deterministic tests for positive and negative cache TTL behavior,
      enable gating, concurrent deduplication, post-commit ordering, and all public
      operation paths (Properties 20 and 21; at least 100 cases for each property).
    - Resolve any remaining drainage/open-pinned count discrepancy against v1.31.0,
      classify only genuinely undeliverable private-harness leaves, and run the relevant
      `TestDeploymentVersionSuite` leaves twice consecutively before updating the ledger.
    - Correct missing-task-queue admission to combine durable historical membership with
      live runtime pressure: idle missing queues pass; a still-owned missing queue with
      backlog/add-rate rejects atomically with the exact current/ramping v1.31.0 message.
      Cover both outcomes and ensure the check is re-evaluated after a CAS conflict.
    - _Requirements: 8.1-8.9, 9.9-9.11, 14.1-14.6; Properties 20 and 21_

- [x] 15. Tier 8.41 kernel transition and history foundations
  - [x] 15.1 Extend the serializable per-run versioning vocabulary
    - Add the documented tri-state `VersionTarget` representation and defaulted
      last-notified / declined-target lineage to `WorkflowVersioningInfo`. Extend
      `ContinueAsNewVersioningBehavior` with `Unknown(i32)` so an unknown proto3 enum
      value is not collapsed.
    - Extend workflow-task-start, Continue-as-New, successor-start, pending-WFT, and
      internal history types with the pre-resolved operands and decisions from the
      design. The internal WFT-start event retains the policy and target as private
      replay metadata in addition to its public Boolean. Append/default serializable
      fields and add old-shape deserialization guards; do not add registry handles,
      async work, I/O, clocks, or retained kernel state.
    - _Requirements: 15.2, 15.11, 15.13, 15.22, 15.27, 15.28, 15.29, 15.33_
  - [x] 15.2 Apply target-change notification in the pure WFT-start transition
    - Implement the v1.31.0 five-way decision over the runtime-supplied policy and target:
      disabled preserves lineage; override suppresses and clears lineage; AutoUpgrade or
      unversioned suppresses; equal effective/target suppresses and clears; declined target
      suppresses; otherwise notify, store last-notified, and clear the previous decline.
    - Store the Boolean and private policy/target replay operands on the pending WFT
      before any transient/speculative materialization and author the same values on the
      internal `WorkflowTaskStarted`; replay must restore the lineage used by later
      Continue-as-New decisions without consulting the registry.
    - _Requirements: 15.2-15.11, 15.27, 15.30-15.32_
  - [x] 15.3 Apply inherited Continue-as-New state in pure close/start transitions
    - Preserve the command's initial behavior and runtime-resolved successor decision on
      `WorkflowExecutionContinuedAsNew`. Initialize the successor from
      `StartRequest.inherited_versioning_info`, combine it with any explicit compatible
      override, and author the inherited pinned/AutoUpgrade/declined values into the
      internal started event for replay and public serialization.
    - Ensure an explicit initial behavior applies only to this successor's initial WFT
      and retries; a later Continue-as-New command starts from its own wire value.
    - _Requirements: 15.22-15.29, 15.33_
  - [x] 15.4 Property test: Property 22 — target-change notification state machine
    - Add a `proptest` reference model in `tokeira-kernel` with at least 100 cases over
      enablement, effective behavior, override, effective/routing/declined targets, and
      absent versus unversioned versus concrete lineage.
    - Tag: `// Feature: worker-deployments, Property 22: target-change notification state machine`.
    - _Requirements: 15.2-15.11, 15.30-15.32_

- [x] 16. Tier 8.41 runtime target resolution and successor preparation
  - [x] 16.1 Supply the WFT notification target and scoped policy input
    - Reuse the existing durable routing resolver to retain both the effective dispatch
      destination and the Current/Ramping target offered to the workflow. Supply that
      target to `StartWorkflowTaskRequest` before invoking the kernel.
    - Use the v1.31.0 production default `true` for
      `system.enableSendTargetVersionChanged`; under the conformance feature, register
      and consult the live namespace Boolean override at this call site. Do not expose a
      production dynamic-config knob.
    - _Requirements: 15.1, 15.2, 15.30, 15.31_
  - [x] 16.2 Add the runtime-only Continue-as-New membership resolver
    - Add a boolean-returning workflow-task-family membership lookup to the shared
      `DeploymentRegistry`, reusing its positive/negative cache. Same-task-queue
      inheritance needs no repository read; missing cross-task-queue membership is a
      normal `false`, while storage failure aborts WFT completion with `INTERNAL`.
    - Implement the pure `resolve_continue_as_new_versioning` reference-shaped helper
      over the loaded predecessor, routing config, initial behavior, and pre-resolved
      membership booleans. Unknown non-zero initial behaviors take the non-ramping
      AutoUpgrade path.
    - _Requirements: 15.14-15.21, 15.28, 15.34_
  - [x] 16.3 Enrich the terminal command and start its successor from committed history
    - In the serialized WFT-completion path, enrich the single terminal Continue-as-New
      command before the authoritative transition commits. First project the same
      completion's reported behavior, Deployment Version, and Worker Deployment name
      onto an ephemeral predecessor clone, matching v1.31.0's
      completion-before-command ordering. Resolve same/cross-queue pinned inheritance,
      source Version/revision AutoUpgrade state, UseRamping initial placement, override
      precedence, and last-notified-or-existing decline. For non-UseRamping inherited
      AutoUpgrade first tasks, apply v1.31.0's same-Deployment revision comparison so a
      current/equal-or-newer routing target wins while an older routing view cannot
      bounce the run backward.
    - Make lane successor creation read the committed close event and copy its concrete
      decision into `StartRequest`; do not use a volatile side channel. A retried
      derived start must reproduce the same request and remain request-id idempotent.
    - _Requirements: 15.14-15.27, 15.34-15.36_
  - [x] 16.4 Preserve v1.31.0 versioning state across workflow retries
    - Extend retry successor preparation to read the predecessor's started event:
      inherit pinned only when that run began inherited-pinned; carry current source
      Version/revision and stored initial behavior for AutoUpgrade; preserve the decline
      recorded on the started event. Keep UseRamping limited to the first WFT of each
      retry.
    - _Requirements: 15.25-15.27_
  - [x] 16.5 Property test: Property 23 — Continue-as-New versioning decision
    - Add a runtime `proptest` reference model with at least 100 cases covering
      same/cross-queue membership, override precedence, known and unknown initial
      behaviors, worker-reported same-completion behavior/Version changes,
      Current/Ramping fallback, notification lineage, later CaN isolation, and retry
      inheritance.
    - Tag: `// Feature: worker-deployments, Property 23: Continue-as-New versioning decision`.
    - _Requirements: 15.14-15.18, 15.20-15.23, 15.25, 15.26, 15.34-15.36_
  - [x] 16.6 Property test: Property 25 — runtime-resolved boundary determinism
    - Generate loaded runs, routing configs, policy values, and membership results;
      assert runtime resolves every mutable operand before invocation and repeated
      kernel evaluation returns equal next state/history without registry, cache,
      clock, queue, randomness, or I/O access.
    - Tag: `// Feature: worker-deployments, Property 25: runtime-resolved boundary determinism`.
    - _Requirements: 15.1, 15.2, 15.19, 15.22, 15.28_
  - [x] 16.7 Derive child-start versioning inheritance from the committed parent
    - Load the parent after its WFT completion commits, resolve same/cross-task-queue
      Version compatibility through the shared runtime registry, and pass only the
      concrete inherited pinned/override/AutoUpgrade state on the child's StartRequest.
      Never carry a parent's `USE_RAMPING_VERSION` instruction into the child.
    - Add focused pure/runtime coverage for post-completion AutoUpgrade inheritance,
      namespace and membership rejection, override precedence, and the unspecified
      child initial behavior.
    - _Requirements: 15.37-15.39_
  - [x] 16.8 Preserve target-specific routing revisions
    - Append durable Current/Ramping revision operands to `StoredRoutingConfig`, stamp
      them on their respective mutations, and make runtime routing return the revision
      belonging to the selected target. Preserve aggregate `revision_number` as the
      public routing-config value and default older internal records safely.
    - Extend storage round-trip and no-bounce tests so a later Ramping change does not
      make an older Current target appear newer than an inherited source.
    - _Requirements: 15.36, 15.40_
  - [x] 16.9 Correct speculative, transient-activity, sticky-migration, and poll validation edges
    - Suppress durable deployment-transition input for speculative WFT starts while
      retaining normal/transient start behavior and completion-time Version adoption.
    - When an unversioned run has no Deployment lookup source, resolve its workflow-task
      routing through the versioned activity poller's Deployment before deciding whether
      that activity must start a transition and be withheld. When this starts a transition
      with an unstarted non-speculative WFT already pending, deterministically fence the
      prior offer with a new logical sequence and emit its replacement dispatch without
      adding history; retain timeout-driven handling for speculative WFTs.
    - Resolve sticky work against the normal task-queue family; when the routing target
      differs from the sticky worker Version, publish on the normal physical target and
      clear the disposable sticky preference. Hydrate missing sticky queue deployment
      coordinates from the run's committed effective Version before comparing, so a
      pinned sticky task is not mistaken for Current merely because its envelope is old.
    - Reject workflow and activity versioned sticky polls with empty `normal_name` at the
      gRPC boundary, before CHASM fallback or long-poll admission, with v1.31.0's exact
      `INVALID_ARGUMENT` text.
    - Record bounded Deployment-Version poller presence before awaiting durable poll
      registration, while retaining registration as the gate before task delivery, so a
      concurrent query cannot race into a false drained-Version blackhole. Retain
      separate physical-Version observations when one SDK worker identity polls more
      than one Version.
    - Extend Property 13 and focused runtime/edge tests for all five corrections.
    - _Requirements: 2.20, 15.41-15.44_

- [x] 17. Tier 8.41 edge translation and public-history fidelity
  - [x] 17.1 Preserve Continue-as-New enum values and serialize versioning history
    - Map known `initial_versioning_behavior` values to named internal variants and all
      other integers to `Unknown(i32)`; emit the same raw value on
      `WorkflowExecutionContinuedAsNew`.
    - Serialize the stored WFT notification Boolean verbatim without exposing its
      private policy/target replay operands. Map inherited pinned,
      inherited AutoUpgrade source/revision/initial behavior, and the declined target
      onto `WorkflowExecutionStarted`, preserving wrapper-present unversioned targets
      and never emitting mutually exclusive inheritance fields together.
    - _Requirements: 15.12, 15.13, 15.24, 15.29, 15.33_
  - [x] 17.2 Property test: Property 24 — versioning history and replay round-trip
    - Add the shared generated case model across kernel event/replay and edge history
      serialization, with at least 100 cases covering absent/unversioned/concrete
      targets, pinned/AutoUpgrade inheritance, overrides, known/unknown initial
      behavior, and late WFT-start materialization.
    - Tag: `// Feature: worker-deployments, Property 24: versioning history and replay round-trip`.
    - _Requirements: 15.12, 15.13, 15.24, 15.27, 15.29, 15.33_

- [x] 18. Tier 8.41 integration and conformance
  - [x] 18.1 Add focused cross-plane integration coverage
    - Drive pinned target change → WFT notification → Continue-as-New → successor start
      and assert agreement between polled/public history, replayed state, Describe, and
      physical Version placement. Cover same-queue inheritance, cross-queue membership
      acceptance/rejection, override precedence, AutoUpgrade, UseRamping, unknown
      behavior, restart recovery, and the scoped notification-disabled mode.
    - _Requirements: 2.20, 15.1-15.44_
  - [x] 18.2 Prove the Tier 8.41 corpus clean
    - Run focused crate tests and the exact previously failing Continue-as-New leaves,
      then two consecutive clean `TestVersioning3FunctionalSuite` conformance runs. Remove
      only skips made obsolete by delivered public behavior and update the functional
      conformance ledger with the command and evidence.
    - _Requirements: 2.20, 15.1-15.44_

- [x] 19. Tier 8.41 final checkpoint
  - Run focused kernel/runtime/edge tests, then the enforced repository bar:
    `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`,
    `cargo check --workspace`, `cargo test --workspace`, and
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, using `--locked` where
    the command accepts it.
  - Verify every new public item and non-obvious decision has a WHY comment and a checked
    v1.31.0 source anchor, and mark tasks complete only after their code and tests land.

## Notes

- Tasks follow the design's strict dependency order: storage → kernel → runtime registry → runtime dispatch → edge → describe projection → cleanup/matrix → integration. No new architecture is introduced beyond `design.md`.
- Property tests are REQUIRED, not optional (no `*` markers). All 25 design properties are covered exactly once, each placed in the crate the design's Testing Strategy specifies: Properties 1–13, 15, 16, 19, 21, 23, and 25 in `tokeira-runtime` (registry/routing/boundary); Property 17 in `tokeira-storage`; Properties 18 and 22 in `tokeira-kernel`; Properties 14 and 20 in their existing edge/runtime placements; and Property 24 across kernel replay and edge history serialization. Each uses the workspace-standard `proptest` with ≥100 iterations and reference models/generators per the design; no hand-rolled property infrastructure.
- The kernel stays pure: tasks under sections 2 and 15 add only serializable per-run
  transition vocabulary and deterministic evaluation — no I/O, async, metrics, storage,
  randomness, registry access, or internally retained state. Runtime/storage retain the
  returned state and history between invocations.
- The edge talks to the runtime only through the new `WorkerDeploymentRuntimeApi` adapter; `DeploymentMutationOutcome` (edge adapter) is kept distinct from the concrete runtime `CommitResult`. Translation uses free functions, not `TryFrom`.
- Every mutating registry method follows load → validate (pure) → CAS-commit so a rejected request never partially mutates state (Property 16).
- The 13 v2 RPCs never return `UNIMPLEMENTED`; the 5 deprecated `Deployment` companions return the exact v1.31.0 message, the single sanctioned `UNIMPLEMENTED` case (Requirement 11, matching `service/frontend/workflow_handler.go @ v1.31.0`).
- Cleanup task 9.1 re-points `describe_worker` / `list_workers` to their owning worker-observability feature; they are not deployment RPCs and are out of scope for implementation here.
- Enforced commands per AGENTS.md are run at each checkpoint: `cargo +nightly fmt --all --check`, `cargo lint`, `cargo test-lint`, `cargo check --workspace`, `cargo test --workspace`, `cargo doc --workspace --no-deps`.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "1.3", "2.2", "2.3"] },
    { "id": 2, "tasks": ["1.4", "1.5", "2.4", "2.5", "4.1"] },
    { "id": 3, "tasks": ["4.2", "4.3", "4.4", "4.5", "4.6", "4.7", "4.8", "4.9"] },
    { "id": 4, "tasks": ["4.10", "4.11", "4.12", "4.13", "4.14", "4.15", "4.16", "4.17", "4.18", "4.19", "4.20", "4.21", "4.22"] },
    { "id": 5, "tasks": ["6.1", "6.2", "6.3", "6.4"] },
    { "id": 6, "tasks": ["6.5", "6.6", "7.1", "7.2"] },
    { "id": 7, "tasks": ["7.3", "7.4"] },
    { "id": 8, "tasks": ["7.5", "7.6", "8.1"] },
    { "id": 9, "tasks": ["7.7", "8.2", "9.1", "10.1", "10.2"] },
    { "id": 10, "tasks": ["11.1", "11.2", "11.3"] },
    { "id": 11, "tasks": ["13.1", "13.2", "13.3", "13.4"] },
    { "id": 12, "tasks": ["14.1", "14.2", "14.3", "14.4"] },
    { "id": 13, "tasks": ["14.5", "16.9"] },
    { "id": 14, "tasks": ["15.1"] },
    { "id": 15, "tasks": ["15.2", "15.3"] },
    { "id": 16, "tasks": ["15.4", "16.1", "16.2"] },
    { "id": 17, "tasks": ["16.3", "16.4"] },
    { "id": 18, "tasks": ["16.5", "16.6", "17.1"] },
    { "id": 19, "tasks": ["17.2"] },
    { "id": 20, "tasks": ["18.1"] },
    { "id": 21, "tasks": ["18.2", "19"] }
  ]
}
```
