# Implementation Plan: Edge Worker Versioning Transport

## Overview

Implement the rule management layer for Worker Versioning v2. Core versioning types, rule store, and evaluation functions live in `tokeira-runtime/src/versioning.rs` (so both edge and runtime can depend on them). gRPC handlers and proto translation stay in `tokeira-edge`. This adds assignment rules, redirect rules, reachability computation, and 11 gRPC handlers. The rule store is in-memory (`DashMap`) with conflict tokens for optimistic concurrency. Rule evaluation happens in the edge/runtime layer — the kernel stays pure.

Depends on `runtime-worker-versioning` (already complete).

## Tasks

- [x] 1. VersioningRuleStore — core data structures and rule store
  - [x] 1.1 Create versioning module with core types
    - Add `dashmap` to `crates/tokeira-runtime/Cargo.toml` dependencies
    - Create `crates/tokeira-runtime/src/versioning.rs`
    - Define `AssignmentRule` struct with `target_build_id: String`, `percentage_ramp: Option<f32>`, `create_time: OffsetDateTime`
    - Define `RedirectRule` struct with `source_build_id: String`, `target_build_id: String`, `create_time: OffsetDateTime`
    - Define `VersioningRules` struct with `assignment_rules: Vec<AssignmentRule>`, `redirect_rules: Vec<RedirectRule>`, `conflict_token: Vec<u8>`
    - Define `VersioningMutation` enum with all 7 operation variants
    - Define `VersioningError` enum for `StaleConflictToken`, `OutOfBounds`, `EmptyBuildId`, `LastUnconditionalRule`, `RedirectCycle`, `RedirectChainTooDeep`, `DuplicateRedirectSource`, `UnknownRedirectSource`
    - Add `pub mod versioning;` to `crates/tokeira-runtime/src/lib.rs`
    - NOTE: Core types live in `tokeira-runtime` so both `tokeira-edge` and `tokeira-runtime` can use them without circular dependencies. Proto translation stays in `tokeira-edge`.
    - _Requirements: 1.1, 1.2_

  - [x] 1.2 Implement VersioningRuleStore with DashMap (in `crates/tokeira-runtime/src/versioning.rs`)
    - Implement `VersioningRuleStore` with `DashMap<(NamespaceId, TaskQueueName), VersioningRules>`
    - Implement `get_rules(&self, ns, tq) -> VersioningRules` returning empty defaults with initial conflict token if absent
    - Implement `apply_mutation(&self, ns, tq, conflict_token, mutation, now) -> Result<VersioningRules, VersioningError>` with conflict token validation, mutation application, server-authored timestamps, and token increment
    - Implement conflict token as big-endian u64 counter, initialized to 1 on first access, incremented by 1 on each mutation
    - Implement `all_task_queues_with_rules(&self) -> Vec<(NamespaceId, TaskQueueName)>`
    - Handle all 7 mutation variants: insert/replace/delete assignment rules, add/replace/delete redirect rules, commit build ID
    - Validate: out-of-bounds index for replace/delete → `OutOfBounds` (insert clamps to end), empty target_build_id → `EmptyBuildId`, delete/replace last unconditional without force → `LastUnconditionalRule`, duplicate source_build_id on add → `DuplicateRedirectSource`, replace/delete redirect with unknown source → `UnknownRedirectSource`
    - `AddRedirectRule` sets `RedirectRule.create_time` to the `now` argument passed to `apply_mutation`
    - `ReplaceRedirectRule` replaces the full redirect rule and sets `RedirectRule.create_time` to the `now` argument passed to `apply_mutation`
    - `CommitBuildId` mutates rule state only; recent-poller validation is handled by the gRPC handler before calling `apply_mutation`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.9, 4.10, 4.11, 4.16, 4.17_

  - [x] 1.3 Implement assignment rule evaluation (in `crates/tokeira-runtime/src/versioning.rs`)
    - Implement `evaluate_assignment(&self, ns, tq, workflow_id: &str) -> Option<String>`
    - Evaluate rules in index order (index 0 first)
    - For rules with `percentage_ramp: None` or `percentage_ramp: Some(100.0)`, always apply (unconditional)
    - For rules with `percentage_ramp: Some(p)` where 0 < p < 100, use deterministic hash: `hash(workflow_id) % 10000 < (p * 100.0) as u64`
    - Return first applicable rule's `target_build_id`, or `None` if no rule applies
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [x] 1.4 Implement redirect resolution (in `crates/tokeira-runtime/src/versioning.rs`)
    - Implement `resolve_redirect(&self, ns, tq, build_id: &str) -> Result<String, VersioningError>`
    - Follow redirect chain: look up `source_build_id` match, follow to target, repeat
    - Track visited build IDs in `HashSet` for cycle detection → return `RedirectCycle` error
    - Limit chain depth to 10 hops → return `RedirectChainTooDeep` error
    - If no redirect matches, return original build ID unchanged
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 2. Property tests for rule store
  - [x] 2.1 Write property test for assignment evaluation determinism (Property 1)
    - Generate random `Vec<AssignmentRule>` with varying `percentage_ramp` values
    - Generate random workflow ID strings
    - Evaluate twice with same inputs, verify identical results
    - Verify result matches first applicable rule in index order
    - Tag: `// Feature: edge-worker-versioning-transport, Property 1: Assignment rule evaluation determinism`
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5**

  - [x] 2.2 Write property test for redirect chain resolution (Property 2)
    - Generate random acyclic redirect chains (build DAGs), verify resolution follows chain to final target
    - Generate random cyclic redirect sets, verify error returned
    - Generate build IDs not in any redirect rule, verify original returned unchanged
    - Tag: `// Feature: edge-worker-versioning-transport, Property 2: Redirect chain resolution`
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.5**

  - [x] 2.3 Write property test for conflict token monotonicity (Property 3)
    - Generate random sequences of valid mutations
    - Apply each mutation, capture conflict token after each
    - Verify tokens are strictly increasing (each > previous)
    - Attempt mutation with stale token, verify rejection
    - Tag: `// Feature: edge-worker-versioning-transport, Property 3: Conflict token monotonicity`
    - **Validates: Requirements 1.3, 1.4, 4.14**

  - [x] 2.4 Write property test for rule CRUD correctness (Property 4)
    - Generate random sequences of CRUD operations (insert/replace/delete assignment, add/replace/delete redirect, commit)
    - Apply operations to `VersioningRuleStore`
    - Apply same operations to a reference `Vec` model
    - Compare resulting rule sets for equality
    - Tag: `// Feature: edge-worker-versioning-transport, Property 4: Rule CRUD correctness`
    - **Validates: Requirements 1.1, 4.1, 4.2, 4.3, 4.5, 4.6, 4.7, 4.8, 4.9, 4.15, 5.1, 5.3**

- [x] 3. Checkpoint — Ensure all rule store tests pass
  - Run `cargo test -p tokeira-runtime` and verify all property and unit tests pass

- [x] 3.5 Extend WorkerRegistry for recent build-ID poller checks
  - Add `last_seen_at: OffsetDateTime` to `WorkerVersionMetadata`
  - Update `TokeiraRuntime::register_worker()` so every poll registration writes the current time
  - Add `WorkerRegistry::has_recent_poller_for_build_id(namespace_id, task_queue, build_id, now, recent_window) -> bool`
  - The query scans registered workers for matching namespace/task_queue, matching `build_id`, `deployment == None`, and `now - last_seen_at <= recent_window`
  - Add unit tests for matching poller, stale poller, wrong task queue, wrong build ID, and deployment-based poller not satisfying build-ID-only validation
  - _Requirements: 4.12_

- [x] 4. gRPC handlers — Phase 1 (rule management)
  - [x] 4.1 Add proto translation for versioning types
    - Add `versioning_mutation_from_proto()` in `crates/tokeira-edge/src/grpc/translate.rs` to parse `UpdateWorkerVersioningRulesRequest` operation variants into a parsed operation wrapper
    - The parsed operation wrapper carries `mutation: VersioningMutation` plus commit metadata needed by the handler, including `commit_force: Option<bool>`
    - Add `versioning_rules_to_proto()` to convert `VersioningRules` to proto response (assignment rules, redirect rules, conflict token)
    - Add `assignment_rule_to_proto()` and `redirect_rule_to_proto()` helpers
    - `redirect_rule_to_proto()` emits the stored `RedirectRule.create_time` into `TimestampedCompatibleBuildIdRedirectRule`
    - Handle proto `oneof` for the 7 operation variants + `commit_build_id`
    - _Requirements: 4.1–4.17_

  - [x] 4.2 Implement update_worker_versioning_rules handler
    - Replace the `Err(Status::unimplemented(...))` stub in `workflow_service.rs`
    - Extract namespace, task_queue from request
    - Parse operation via `versioning_mutation_from_proto()`
    - If the parsed operation is `CommitBuildId` with `force=false`, call `WorkerRegistry::has_recent_poller_for_build_id(namespace_id, task_queue, build_id, now, RECENT_POLLER_WINDOW)` before mutating rules
    - If no recent poller is found for `CommitBuildId` and `force=false`, return `FAILED_PRECONDITION` and do not call `VersioningRuleStore::apply_mutation()`
    - If `CommitBuildId.force=true`, skip recent-poller validation
    - Call `VersioningRuleStore::apply_mutation()` with conflict token from request and a single handler-captured `now`
    - Map `VersioningError` variants to gRPC status codes (`FAILED_PRECONDITION`, `INVALID_ARGUMENT`)
    - Return updated rules via `versioning_rules_to_proto()`
    - _Requirements: 4.1–4.17_

  - [x] 4.3 Implement get_worker_versioning_rules handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract namespace, task_queue from request
    - Call `VersioningRuleStore::get_rules()`
    - Return rules via `versioning_rules_to_proto()`
    - _Requirements: 5.1, 5.2, 5.3_

  - [x] 4.4 Wire VersioningRuleStore into WorkflowService
    - Add `versioning_rule_store: Arc<VersioningRuleStore>` field to `WorkflowService`
    - Add `worker_registry: WorkerRegistry` field to `WorkflowService` for CommitBuildId recent-poller validation
    - Initialize in `WorkflowService::new()` and related constructors
    - Pass through `WorkflowServiceGrpc` to handlers
    - Add `versioning_rule_store` to `RuntimeDispatchPublisher` for redirect resolution (Phase 4)
    - _Requirements: 1.5_

  - [x] 4.5 Wire shared VersioningRuleStore through runtime construction
    - Create a single `Arc<VersioningRuleStore>` in `tokeirad/src/main.rs` (or wherever the application is assembled). Pass it to both `WorkflowService::new()` and `TokeiraRuntime::new()`. The runtime passes it through to `RuntimeDispatchPublisher` via lane construction. This ensures CRUD updates through edge handlers are visible to the dispatch redirect path.
    - Pass `runtime.worker_registry()` into `WorkflowService::new()` so edge CommitBuildId validation uses the same registry populated by poll registrations
    - _Requirements: 1.5, 12.1_

- [x] 5. gRPC handlers — Phase 2 (reachability)
  - [x] 5.1 Implement reachability computation (rule-only for MVP)
    - Add `compute_reachability()` function in `crates/tokeira-runtime/src/versioning.rs`
    - Accept: build_id, assignment_rules, redirect_rules
    - Classify with `[NewWorkflows]` if build_id is target of any assignment rule
    - Classify with `[NewWorkflows]` if build_id is the effective target of a redirect rule whose source is reachable for `NewWorkflows` (new workflows are effectively delivered to the redirect target)
    - Classify with `[ExistingWorkflows]` if build_id is reachable via redirect chain from a rule-reachable source that is not itself reachable for `NewWorkflows`
    - Return empty reachability list if not rule-reachable (unreachable per proto convention: "If reachability is empty, this worker is considered unreachable")
    - NOTE: Open-workflow-based classification deferred to DSQL storage spec
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [x] 5.2 Write property test for reachability classification (Property 5)
    - Generate random assignment rules and redirect rules
    - Compute reachability for random build IDs
    - Verify: rule-referenced build IDs get `[NewWorkflows]`, redirect targets of `NewWorkflows`-reachable sources also get `[NewWorkflows]`, other redirect-reachable get `[ExistingWorkflows]`, non-referenced get empty list
    - Tag: `// Feature: edge-worker-versioning-transport, Property 5: Reachability classification`
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4**

  - [x] 5.3 Implement get_worker_task_reachability handler
    - Replace the `Err(Status::unimplemented(...))` stub
    - Extract build_ids and optional task_queues from request
    - If task_queues specified, limit to those; otherwise use `all_task_queues_with_rules()`
    - For each (build_id, task_queue) pair: get rules, compute rule-based reachability (no storage query)
    - Add proto translation for reachability request/response using proto `TaskReachability` enum values
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

- [x] 6. gRPC handlers — Phase 3 (legacy, shutdown, deployment stubs)
  - [x] 6.1 Implement legacy handler messages
    - Update `update_worker_build_id_compatibility` to return `UNIMPLEMENTED` with message "Legacy worker versioning API (v1 version sets) is not supported. Use UpdateWorkerVersioningRules (v2 rule-based API) instead."
    - Update `get_worker_build_id_compatibility` to return `UNIMPLEMENTED` with message "Legacy worker versioning API (v1 version sets) is not supported. Use GetWorkerVersioningRules (v2 rule-based API) instead."
    - _Requirements: 8.1, 8.2_

  - [x] 6.2 Implement shutdown_worker handler (sticky-queue-only)
    - Add `denied_workers: Arc<Mutex<HashSet<(NamespaceId, TaskQueueName, WorkerIdentity)>>>` to `InMemoryBroker` (NOT `InMemoryActivityBroker`)
    - The deny list key uses `TaskQueueName` from the proto's `sticky_task_queue` field
    - Update `poll_workflow_task` only to check deny list before delivering (activity polls are not affected)
    - Implement `shutdown_worker` gRPC handler: extract identity/namespace/sticky_task_queue, insert into deny list, log reason if provided
    - Return success even if worker is not currently polling (idempotent)
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 6.3 Implement deployment management handler stubs
    - Update `describe_deployment` → `UNIMPLEMENTED` with "Deployment management is not yet supported. Worker versioning via assignment and redirect rules is available."
    - Update `list_deployments` → `UNIMPLEMENTED` with "Deployment management is not yet supported. Worker versioning via assignment and redirect rules is available."
    - Update `get_deployment_reachability` → `UNIMPLEMENTED` with "Deployment management is not yet supported. Use GetWorkerTaskReachability for build ID reachability."
    - Update `get_current_deployment` → `UNIMPLEMENTED` with "Deployment management is not yet supported."
    - Update `set_current_deployment` → `UNIMPLEMENTED` with "Deployment management is not yet supported."
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

- [ ] 7. Checkpoint — Ensure all handler tests pass
  - Run `cargo test -p tokeira-edge` and `cargo test -p tokeira-runtime` to verify all tests pass

- [x] 8. Integration wiring — Phase 4
  - [x] 8.1 Wire assignment rule evaluation into workflow start path
    - In `to_internal::start_request()`: accept `Option<&VersioningRuleStore>` parameter
    - Add `versioning_override: Option<VersioningOverrideDto>` to `StartWorkflowExecutionRequest` edge DTO, where `VersioningOverrideDto` is `Pinned { deployment_series: String, build_id: String }` or `AutoUpgrade`
    - In `start_request_to_edge()`: extract and parse `versioning_override` from the proto request into `VersioningOverrideDto`
    - In `to_internal::start_request()`: if `Pinned`, set both `deployment` and `build_id` on `StartRequest` and skip rule evaluation; if `AutoUpgrade` or `None`, call `evaluate_assignment(ns, tq, workflow_id)` and set result on `StartRequest.build_id`
    - Update `WorkflowService::start_workflow_execution()` to pass rule store to translation
    - _Requirements: 11.1, 11.2, 11.3, 11.4_

  - [x] 8.2 Wire assignment rule evaluation into signal-with-start path
    - Apply same logic as 8.1 to `to_internal::signal_with_start_request()`
    - Update `WorkflowService::signal_with_start_workflow_execution()` to pass rule store
    - _Requirements: 11.5_

  - [x] 8.3 Wire redirect resolution into RuntimeDispatchPublisher
    - Add `versioning_rule_store: Option<Arc<VersioningRuleStore>>` field to `RuntimeDispatchPublisher`
    - In `publish()` method, for `EnqueueWorkflowTask` and `EnqueueActivityTask` arms:
      - If `queue.build_id.is_some()` AND `queue.deployment.is_none()` and rule store is available, call `resolve_redirect(ns, tq, build_id)`
      - On success with different build_id: clone queue, update `build_id`, publish with updated queue
      - On error (cycle/depth): log warning, publish with original queue
      - If `queue.build_id.is_none()`: skip redirect resolution
      - If `queue.deployment.is_some()`: skip redirect resolution (deployment-pinned workflows are not subject to build-ID redirect rules)
    - Update `RuntimeDispatchPublisher::new()` to accept optional rule store
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6_

- [x] 9. Unit tests for edge cases and integration
  - [x] 9.1 Write unit tests for rule store edge cases
    - `test_empty_rule_set_returns_valid_token`: query non-existent (ns, tq), verify empty rules + valid token
    - `test_out_of_bounds_index_rejected`: replace/delete at invalid index, verify error; insert at out-of-bounds index, verify clamped to end
    - `test_empty_target_build_id_rejected`: submit empty target, verify error
    - `test_delete_last_unconditional_without_force`: verify `FAILED_PRECONDITION`
    - `test_replace_last_unconditional_without_force`: verify `FAILED_PRECONDITION` when replacing last unconditional rule with a ramped rule without force
    - `test_delete_last_unconditional_with_force`: verify success
    - `test_redirect_chain_depth_limit`: chain of 11 redirects, verify error
    - `test_redirect_none_build_id_skipped`: resolve with None, verify None
    - `test_duplicate_redirect_source_rejected`: add a redirect rule, then add another with the same `source_build_id`, verify `FAILED_PRECONDITION`
    - `test_replace_absent_redirect_rejected`: replace a redirect rule for a `source_build_id` that has no existing rule, verify `INVALID_ARGUMENT`
    - `test_delete_absent_redirect_rejected`: delete a redirect rule for a `source_build_id` that has no existing rule, verify `INVALID_ARGUMENT`
    - `test_redirect_create_time_set_on_add_and_replace`: add and replace a redirect rule with distinct `now` values, verify the stored and proto-emitted timestamps match the mutation time
    - _Requirements: 1.6, 3.4, 3.6, 4.4, 4.6, 4.8, 4.10, 4.11, 4.16, 4.17, 5.2_

  - [x] 9.2 Write unit tests for gRPC handler responses
    - `test_legacy_handlers_return_unimplemented`: call both legacy endpoints, verify status and messages
    - `test_deployment_handlers_return_unimplemented`: call all 5 deployment endpoints, verify status and messages
    - `test_commit_build_id_no_poller_without_force`: call handler with `force=false` when no recent poller has been seen for that build ID, verify `FAILED_PRECONDITION`; then call with `force=true`, verify success
    - `test_commit_build_id_recent_poller_without_force`: register a recent build-ID-only poller, call handler with `force=false`, verify success
    - `test_commit_build_id_stale_poller_without_force`: register a stale poller, call handler with `force=false`, verify `FAILED_PRECONDITION`
    - `test_shutdown_worker_idempotent`: shutdown non-polling worker, verify success
    - `test_shutdown_worker_prevents_delivery`: shutdown worker on sticky queue, verify workflow task delivery blocked but activity polls unaffected
    - _Requirements: 8.1, 8.2, 9.1, 9.3, 9.4, 9.5, 10.1–10.5_

  - [x] 9.3 Write unit tests for integration wiring
    - `test_assignment_integration_explicit_override`: start with `Pinned` versioning override, verify rules skipped and both deployment + build_id set
    - `test_assignment_integration_rule_evaluation`: start without build_id with rules configured, verify build_id set
    - `test_redirect_integration_dispatch`: dispatch with redirect rules, verify effective build_id
    - `test_redirect_integration_none_skipped`: dispatch with None build_id, verify no redirect
    - `test_redirect_skipped_for_deployment_pinned`: dispatch with `queue.deployment.is_some()` and `queue.build_id.is_some()` and a matching redirect rule, verify redirect is NOT applied
    - _Requirements: 11.1–11.4, 12.1–12.6_

- [ ] 10. Final checkpoint — Ensure all tests pass
  - Run `cargo test -p tokeira-edge`, `cargo test -p tokeira-runtime`, and `cargo lint` to verify everything passes

## Notes

- The `VersioningRuleStore` uses `DashMap` (added as a dependency to `tokeira-runtime` in task 1.1) for lock-free concurrent reads
- All property tests use `proptest` (already a project dependency) with minimum 100 iterations
- Each property test is tagged with `// Feature: edge-worker-versioning-transport, Property N: <title>`
- The broker deny list for `shutdown_worker` is in-memory and resets on restart — acceptable for MVP
- Durable persistence of versioning rules is deferred to the DSQL storage spec
- The kernel remains pure — no versioning rule awareness in `tokeira-kernel`
- Tasks reference specific requirements for traceability

## Deferred

- **Task 8.4 — Build ID recording after first WFT completion** (Requirement 13): Deferred to a follow-up spec. The current implementation sets `build_id` on `WorkflowState` at start time via assignment rules. Recording the worker-reported `build_id` after first WFT requires extending the edge DTO for WFT completion to carry `build_id`, adding a kernel command to update `WorkflowState.build_id`, and adding a state transition — changes that cross the kernel purity boundary.
