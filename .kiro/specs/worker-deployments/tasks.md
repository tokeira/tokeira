# Implementation Plan: Worker Deployments (v2 surface + versioning routing)

## Overview

Implement the Worker Deployment v2 surface and make Tokeira the owner of worker-versioning
routing application, strictly per `design.md`. Work flows in the design's dependency order:
durable storage first (`WorkerDeploymentRepository`), then pure per-run kernel state, then the
runtime registry state machine and dispatch routing, then the edge handlers/adapter and the
describe projection, finishing with the cleanup, compatibility-matrix, and integration work.
Every mutation path follows load → validate (pure) → CAS-commit; the kernel stays pure; the edge
talks to the runtime only through the new `WorkerDeploymentRuntimeApi` adapter. All 18 correctness
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

- [ ] 6. Runtime: dispatch routing integration
  - [x] 6.1 Resolve the target version from routing config in `crates/tokeira-runtime/src/runtime/workflow_task.rs`
    - At task-start, resolve the workflow's target version from the deployment registry routing config: AUTO_UPGRADE / unversioned traffic follows `current_deployment_version`, with `ramping_version_percentage`% bucketed deterministically by workflow id (reuse the FNV-1a `deterministic_bucket` in `crates/tokeira-runtime/src/versioning.rs`) routed to `ramping_deployment_version`; PINNED runs (or PINNED override) resolve to their pinned version regardless of routing config; a nil Current routes AUTO_UPGRADE/unversioned traffic to unversioned workers.
    - _Requirements: 9.1, 9.3, 9.4, 9.8_
  - [x] 6.2 Start the version transition at workflow-task start in `crates/tokeira-runtime/src/runtime/workflow_task.rs`
    - When the polling worker's deployment version differs from the workflow's effective version and the run is not pinned, call `start_version_transition` gated on the dispatch `revision_number` (task-start by a differing poller, matching `recordworkflowtaskstarted/api.go @ v1.31.0`); pinned runs do not transition.
    - _Requirements: 9.5, 9.6_
  - [ ] 6.3 Reject transition-triggering activity-task starts in `crates/tokeira-runtime/src/runtime/activity.rs`
    - Apply the differing-poller transition trigger with the `revision_number > wft_dispatch_revision` gate; when activity start triggers a transition, reject/drop the activity task for later reschedule, and reject activity starts while a transition is already in flight; pinned-workflow independent activities do not transition (matching `recordactivitytaskstarted/api.go:188 @ v1.31.0`).
    - _Requirements: 9.5, 9.6_
  - [ ] 6.4 Apply versioning at WFT completion and route eager tasks in `crates/tokeira-runtime/src/runtime/workflow_task.rs` and `crates/tokeira-runtime/src/publisher.rs`
    - On WFT completion call `apply_wft_versioning` and increment `revision_number` when the run routes to a new deployment version; when `eager_worker_deployment_options` is present and `request_eager_execution` is true, route the eager first task per those deployment options, otherwise no routing effect. Routing decisions remain derived effects of durable registry + per-run state (no correctness weight on transient queues).
    - _Requirements: 9.2, 9.6, 9.7, 13.6_
  - [x] 6.5 Property test: routing determinism and effective-version precedence
    - In a routing module under `crates/tokeira-runtime/src/` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 12: Routing determinism and effective-version precedence**
    - Generator: routing configs, per-run versioning state, and workflow ids. Invariant: deterministic target; precedence transition > override > behavior + deployment_version; ramp fraction split by id; nil Current → unversioned.
    - **Validates: Requirements 9.1, 9.3, 9.4, 9.8**
  - [ ] 6.6 Property test: deployment-version transition lifecycle
    - In a routing/dispatch module under `crates/tokeira-runtime/src/` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 13: Deployment-version transition lifecycle**
    - Generator: runs and workflow/activity task-starts by pollers with differing deployment versions, plus WFT completions. Invariant: unpinned WFT starts start a revision-gated transition; transition-triggering activity starts are rejected/dropped and later rescheduled; activity starts during an in-flight transition are rejected; pinned-workflow independent activities do not transition; WFT completion updates effective behavior/deployment/`worker_deployment_name` (UNSPECIFIED → unversioned), clears the transition on target match, and bumps `revision_number` when routing to a new version.
    - **Validates: Requirements 9.2, 9.5, 9.6**

- [ ] 7. Edge: adapter, errors, and translation
  - [ ] 7.1 Add the `WorkerDeploymentRuntimeApi` adapter trait and outcome type
    - Define `WorkerDeploymentRuntimeApi` in `crates/tokeira-edge/src/workflow_service.rs` (analogous to `WorkflowRuntimeApi`) with one async method per v2 RPC taking translated request DTOs and returning view DTOs or `EdgeError`; define the edge-adapter outcome `DeploymentMutationOutcome { conflict_token, view }`, distinct from the concrete runtime `CommitResult` (mirroring `WorkflowMutationOutcome` vs `CommitResult`).
    - Implement the trait on `RuntimeAdapter` in `crates/tokeira-edge/src/grpc/runtime_adapter.rs`, delegating to `DeploymentRegistry`; the edge never touches storage or runtime internals directly.
    - _Requirements: 12.4, 13.1_
  - [ ] 7.2 Add new `EdgeError` variants in `crates/tokeira-edge/src/errors.rs`
    - Add `AlreadyExists` and `ResourceExhausted` with `status_code` + `action_name`; reuse `FailedPrecondition`, `NamespaceNotFound`, and the existing not-found/invalid-argument variants. Do not use `EdgeError::Internal` for any of these user-facing conditions.
    - _Requirements: 1.2, 2.4, 2.5_
  - [ ] 7.3 Wire the new variants in `crates/tokeira-edge/src/grpc/errors.rs`
    - Map `AlreadyExists` → tonic `ALREADY_EXISTS`, `ResourceExhausted` → `RESOURCE_EXHAUSTED`, and confirm `FailedPrecondition`/`NamespaceNotFound`/not-found/invalid-argument map to `FAILED_PRECONDITION`/`NOT_FOUND`/`NOT_FOUND`/`INVALID_ARGUMENT`; confirm `grpc_error_code` emits the matching labels.
    - _Requirements: 1.2, 1.11, 2.4, 2.5, 12.2_
  - [ ] 7.4 Add free translation functions for the deployment DTOs in `crates/tokeira-edge/src/grpc/translate.rs`
    - Add request→DTO and view→proto free functions (matching the `respond_activity_completed_to_edge` pattern; no `TryFrom`) for `WorkerDeploymentInfo`, `WorkerDeploymentSummary`, `WorkerDeploymentVersionInfo`, `VersionTaskQueue`, `RoutingConfig`, `VersionDrainageInfo`, `VersionMetadata`, `ComputeConfig`, and the set-current/ramping/manager responses incl. the deprecated `previous_*` fields.
    - _Requirements: 1.4, 1.5, 2.7, 2.8, 3.7, 4.8, 6.4, 7.7, 8.6_
  - [ ] 7.5 Replace the 13 `deferred_unary!("worker-deployments")` handlers with real handlers in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Implement `create_worker_deployment`, `describe_worker_deployment`, `delete_worker_deployment`, `list_worker_deployments`, `create_worker_deployment_version`, `describe_worker_deployment_version`, `delete_worker_deployment_version`, `set_worker_deployment_current_version`, `set_worker_deployment_ramping_version`, `update_worker_deployment_version_compute_config`, `validate_worker_deployment_version_compute_config`, `update_worker_deployment_version_metadata`, `set_worker_deployment_manager`. Each handler resolves the namespace via `resolve_namespace_id` (→ `NOT_FOUND`), validates required identifiers where v1.31.0 does so (`deployment_name`, `build_id`, legacy `version` string, percentage range, oneof set, non-empty identity) → `INVALID_ARGUMENT` before any mutation, lets list `page_size` clamp rather than error, lets validate-compute skip version-existence lookup, calls the adapter, and translates the view with the free functions. None of the 13 returns `UNIMPLEMENTED`.
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.8, 1.11, 2.1, 2.7, 2.8, 2.14, 3.1, 3.2, 4.1, 4.3, 5.8, 5.9, 6.1, 7.4, 7.8, 12.2, 12.3, 12.5_
  - [ ] 7.6 Replace the 5 deprecated `Deployment` companion handlers in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Make `describe_deployment`, `list_deployments`, `get_deployment_reachability`, `get_current_deployment`, `set_current_deployment` each return `Status::unimplemented("Deployments are deprecated and no longer supported, use Worker Deployments instead")` (the exact v1.31.0 message) before any state access; they do not route through the adapter.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_
  - [ ] 7.7 Unit tests for edge validation and deprecated-companion messages in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Cover: each deprecated companion returns the exact `UNIMPLEMENTED` message and touches no registry state; empty `deployment_name` / unset oneof / empty identity / unresolvable version → `INVALID_ARGUMENT`; namespace-not-found → `NOT_FOUND`; exceeding max-versions → `RESOURCE_EXHAUSTED`; overlapping upsert/remove and update/remove → `INVALID_ARGUMENT`; `eager_worker_deployment_options` applied iff `request_eager_execution`; all 13 v2 RPCs accept valid input without `UNIMPLEMENTED`.
    - _Requirements: 1.8, 2.5, 2.14, 5.3, 6.3, 7.4, 7.8, 9.7, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 12.2, 12.5_

- [ ] 8. Edge: describe versioning-info projection
  - [ ] 8.1 Fill `versioning_info` + `worker_deployment_name` in the describe projection in `crates/tokeira-edge/src/grpc/translate.rs`
    - Populate `WorkflowExecutionInfo.versioning_info` (behavior, deployment_version, versioning_override, version_transition, revision_number, continue_as_new_initial_versioning_behavior) and `worker_deployment_name` from the per-run `WorkflowVersioningInfo` (the seam `api-conformance-workflow-describe` leaves default), using the same run snapshot; leave deprecated `assigned_build_id` / `inherited_build_id` / `most_recent_worker_version_stamp` default; absent versioning state ⇒ leave `versioning_info` and `worker_deployment_name` default with no fabricated placeholders.
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_
  - [ ] 8.2 Property test: versioning-info projection fidelity
    - In `crates/tokeira-edge/src/grpc/translate.rs` `#[cfg(test)]` with `proptest` (≥100 iterations).
    - **Property 14: Versioning-info projection fidelity**
    - Generator: arbitrary per-run versioning state incl. the absent case. Invariant: `DescribeWorkflowExecution` projects `versioning_info` + `worker_deployment_name` exactly, leaves the deprecated build-id/version-stamp fields default, and leaves both default (no placeholders) when there is no versioning state.
    - **Validates: Requirements 10.1, 10.2, 10.3, 10.4**

- [ ] 9. Cleanup: re-point mis-grouped worker-observability RPCs
  - [ ] 9.1 Move `describe_worker` / `list_workers` out of the `worker-deployments` block in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Re-point `describe_worker` and `list_workers` from the `deferred_unary!("worker-deployments")` block to their owning worker-observability feature key (they are NOT deployment RPCs), and update the `deferred_handler_blocks_return_tracked_unimplemented_messages` test (and the `assert_deferred_rpc!` usages) so the two RPCs are asserted under their correct owning feature rather than `worker-deployments`.
    - _Requirements: 12.5_

- [ ] 10. Compatibility matrix
  - [ ] 10.1 Move the `worker-deployments` `FeatureEntry` off `Unsupported` in `crates/tokeira-compatibility/src/matrix.rs`
    - Set the `worker-deployments` entry (id `"worker-deployments"`) to its supported state with evidence (13 v2 RPCs implemented; the 5 deprecated companions counted conformant via their v1.31.0 `UNIMPLEMENTED` behavior); keep `WORKER_DEPLOYMENT_RPCS` accurate.
    - _Requirements: 11.7, 12.5_
  - [ ] 10.2 Update the deferred-RPC edge test for the 13 v2 RPCs in `crates/tokeira-edge/src/grpc/workflow_service.rs`
    - Update `deferred_handler_blocks_return_tracked_unimplemented_messages` so the 13 v2 RPCs are no longer asserted as deferred placeholders (they now have real handlers) and the 5 deprecated companions assert the exact v1.31.0 `UNIMPLEMENTED` message.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 12.5_

- [ ] 11. Integration tests
  - [ ] 11.1 Edge → adapter → registry → storage integration in `crates/tokeira-edge/tests/`
    - Exercise the full path for a representative RPC of each family: create/describe deployment, create version, set-current with ramp-unset, set-ramping, manager mismatch, drainage describe; assert responses and durable state.
    - _Requirements: 1.1, 1.4, 2.1, 3.1, 3.3, 4.1, 7.5, 8.6_
  - [ ] 11.2 Restart-recovery integration in `crates/tokeira-edge/tests/` (or `crates/tokeira-runtime/tests/`)
    - Mutate the registry, drop the in-memory runtime, reload from the store via `list_all_for_namespace`, and assert describe/list return the pre-restart state and a pre-restart conflict token is evaluated with identical CAS semantics.
    - _Requirements: 13.1, 13.2, 13.3, 13.4_
  - [ ] 11.3 Routing cycle integration in `crates/tokeira-runtime/tests/` (or `apps/tokeirad/tests/`)
    - Drive a start → dispatch → WFT-completion → describe cycle: confirm the version transition is started by a differing poller, applied at WFT completion, `revision_number` advances on routing to a new version, and the projected `versioning_info` / `worker_deployment_name` reflect the completed routing.
    - _Requirements: 9.1, 9.2, 9.5, 9.6, 10.1, 10.2_

- [ ] 12. Final checkpoint
  - Run `cargo +nightly fmt --all --check`.
  - Run `cargo lint` and `cargo test-lint`.
  - Run `cargo check --workspace` and `cargo test --workspace`.
  - Run `cargo doc --workspace --no-deps` (`RUSTDOCFLAGS="-D warnings"`).
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks follow the design's strict dependency order: storage → kernel → runtime registry → runtime dispatch → edge → describe projection → cleanup/matrix → integration. No new architecture is introduced beyond `design.md`.
- Property tests are REQUIRED, not optional (no `*` markers). All 18 design properties are covered exactly once, each placed in the crate the design's Testing Strategy specifies: Properties 1–13 and 16 in `tokeira-runtime` (registry/routing), Property 17 in `tokeira-storage`, Property 18 in `tokeira-kernel`, Property 14 in `tokeira-edge`, and Property 15 in `tokeira-runtime`. Each uses the workspace-standard `proptest` with ≥100 iterations and reference models/generators per the design; no hand-rolled property infrastructure.
- The kernel stays pure: tasks under section 2 add only serializable per-run state and pure transition methods — no I/O, async, metrics, or storage.
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
    { "id": 10, "tasks": ["11.1", "11.2", "11.3"] }
  ]
}
```
