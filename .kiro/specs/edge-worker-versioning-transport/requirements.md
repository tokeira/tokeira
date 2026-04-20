# Requirements Document: Edge Worker Versioning Transport

## Introduction

This spec implements the rule management layer for Temporal's Worker Versioning feature — assignment rules, redirect rules, reachability computation, and the gRPC handlers that expose them. It sits on top of the broker routing infrastructure already delivered by the `runtime-worker-versioning` spec, which threads `deployment`/`build_id` through `QueueKey`, `WorkflowState`, and kernel dispatch ops.

This is Feature 5 from the umbrella spec `edge-complete-implementation`. It depends on Feature 4 (`edge-describe-pending`) for PollerInfo versioning fields. The work covers 11 gRPC handlers across four categories:

1. **Rule management** (Phase 1): `update_worker_versioning_rules` (7 operation variants) and `get_worker_versioning_rules` — the core CRUD for assignment and redirect rules per (namespace, task_queue).
2. **Reachability** (Phase 2): `get_worker_task_reachability` — on-demand computation of build ID reachability from assignment rules and redirect chains (rule-only for MVP; open-workflow-based classification deferred to DSQL storage spec).
3. **Legacy and operational** (Phase 3): `update_worker_build_id_compatibility` and `get_worker_build_id_compatibility` (legacy v1 — return unimplemented with clear messages), `shutdown_worker` (graceful drain via broker).
4. **Integration** (Phase 4): Wire rule evaluation into workflow start path and redirect resolution into task dispatch, so that versioning rules actually drive routing.

Deployment management handlers (`describe_deployment`, `list_deployments`, `get_deployment_reachability`, `get_current_deployment`, `set_current_deployment`) are a newer Temporal feature that builds on top of versioning. They return unimplemented with clear messages and documentation of what they would need.

The rule store is in-memory for MVP — a `DashMap<(NamespaceId, TaskQueueName), VersioningRules>` with conflict tokens for optimistic concurrency. Core versioning types and the rule store live in `tokeira-runtime` so both edge and runtime crates can depend on them; gRPC handlers and proto translation stay in `tokeira-edge`. Durable persistence is deferred to the DSQL storage spec.

The kernel stays pure. Rule evaluation happens in the edge/runtime layer when constructing `StartRequest` (assignment rules) and when the broker publishes tasks (redirect resolution).

## Glossary

- **Edge_Layer**: The `tokeira-edge` crate providing gRPC transport between SDK clients and the Tokeira runtime.
- **Runtime**: The `tokeira-runtime` crate that orchestrates kernel transitions, storage, and task dispatch.
- **Kernel**: The pure state-machine in `tokeira-kernel` that computes all workflow state transitions with zero I/O.
- **Broker**: The in-memory delivery subsystem (`InMemoryBroker`) that matches pending tasks with waiting pollers, keyed by `QueueKey`.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **VersioningRuleStore**: The in-memory store (in `tokeira-runtime`) that persists assignment rules and redirect rules per (namespace, task_queue) with conflict tokens for optimistic concurrency. Lives in `tokeira-runtime` so both edge and runtime crates can depend on it.
- **AssignmentRule**: A rule that determines which Build ID a new workflow execution gets assigned to. Has `target_build_id: String`, `percentage_ramp: Option<f32>` (0–100), and `create_time: Timestamp`. Evaluated in index order; first applicable rule wins.
- **RedirectRule**: A rule that moves workflows from one Build ID (source) to another (target). Has `source_build_id: String`, `target_build_id: String`, and `create_time: Timestamp`. Can be chained.
- **ConflictToken**: An opaque token for optimistic concurrency on rule updates. Each mutation increments the token; updates with a stale token are rejected.
- **BuildId**: Immutable build identifier baked into a worker binary, used for versioned task routing.
- **DeploymentId**: Identifier grouping a set of workers into a deployment, used together with BuildId for versioned task routing.
- **Reachability**: The computed state of a Build ID — REACHABLE (may get new work via assignment rules or redirect chains) or unreachable (empty reachability list; not referenced by any rule; open-workflow-based classification deferred to DSQL storage spec).
- **WorkflowState**: The `tokeira_kernel::state::WorkflowState` struct that holds durable summary state for a workflow run, including `deployment` and `build_id`.
- **WorkerRegistry**: The `tokeira_runtime::worker_registry::WorkerRegistry` that tracks active worker version metadata and last-seen times per (identity, namespace, task_queue), and can answer recent-poller queries by build ID.
- **Upstream_Proto**: The Temporal API protobuf definitions at version 1.43.0.

## Requirements

---

## Phase 1: Rule Storage and Evaluation

### Requirement 1: VersioningRuleStore — In-Memory Rule Storage

**User Story:** As a Tokeira operator, I want to store assignment rules and redirect rules per task queue, so that I can control how new workflows are assigned to build IDs and how existing workflows are redirected.

#### Acceptance Criteria

1. THE VersioningRuleStore SHALL store a set of assignment rules and a set of redirect rules per (namespace, task_queue) pair.
2. THE VersioningRuleStore SHALL maintain a conflict token per (namespace, task_queue) pair, initialized to a deterministic value (1) on first access.
3. WHEN a mutation is applied to the rules for a (namespace, task_queue) pair, THE VersioningRuleStore SHALL increment the conflict token.
4. WHEN a mutation request carries a conflict token that does not match the current stored token, THE VersioningRuleStore SHALL reject the mutation with a `FAILED_PRECONDITION` error.
5. THE VersioningRuleStore SHALL be safe for concurrent access from multiple gRPC handler threads.
6. WHEN no rules exist for a (namespace, task_queue) pair, THE VersioningRuleStore SHALL return an empty rule set with the current conflict token.

### Requirement 2: Assignment Rule Evaluation

**User Story:** As a Tokeira developer, I want to evaluate assignment rules when a new workflow starts, so that the workflow is assigned to the correct build ID based on the operator's configured rules.

#### Acceptance Criteria

1. WHEN assignment rules exist for a task queue, THE rule evaluator SHALL evaluate rules in index order (index 0 first) and return the first applicable rule's `target_build_id`.
2. WHEN an assignment rule has no `percentage_ramp` (or `percentage_ramp` is 100.0), THE rule evaluator SHALL treat the rule as unconditional and always apply it.
3. WHEN an assignment rule has a `percentage_ramp` between 0.0 and 100.0 (exclusive), THE rule evaluator SHALL apply the rule with the specified probability, using a deterministic hash of the workflow ID to ensure consistent assignment for the same workflow ID.
4. WHEN no assignment rule applies (all percentage ramps miss), THE rule evaluator SHALL return `None`, indicating no build ID assignment.
5. WHEN no assignment rules exist for a task queue, THE rule evaluator SHALL return `None`.

### Requirement 3: Redirect Resolution

**User Story:** As a Tokeira developer, I want to resolve redirect chains when dispatching tasks, so that workflows are routed to the effective target build ID after following all redirect rules.

#### Acceptance Criteria

1. WHEN redirect rules exist and a task's build ID matches a redirect rule's `source_build_id`, THE redirect resolver SHALL return the redirect rule's `target_build_id` as the effective build ID.
2. WHEN redirect rules are chained (A→B, B→C), THE redirect resolver SHALL follow the chain and return the final target (C).
3. WHEN a redirect chain forms a cycle, THE redirect resolver SHALL detect the cycle and return an error rather than looping indefinitely.
4. THE redirect resolver SHALL limit chain depth to 10 hops to prevent excessive traversal.
5. WHEN no redirect rule matches a task's build ID, THE redirect resolver SHALL return the original build ID unchanged.
6. WHEN the task's build ID is `None`, THE redirect resolver SHALL return `None` without consulting redirect rules.

### Requirement 4: update_worker_versioning_rules Handler

**User Story:** As a Temporal SDK user, I want to create, update, and delete assignment and redirect rules via the `update_worker_versioning_rules` gRPC endpoint, so that I can manage how tasks are routed to different worker versions.

#### Acceptance Criteria

1. WHEN the request contains an `insert_assignment_rule` operation, THE handler SHALL insert a new assignment rule at the specified index, shifting existing rules at that index and above by one position; if the index exceeds the current rule count, the rule SHALL be inserted at the end of the list.
2. WHEN the request contains a `replace_assignment_rule` operation, THE handler SHALL replace the assignment rule at the specified index with the new rule. The operation carries a `force` flag; if the replacement would leave no unconditional assignment rule and `force` is not set, THE handler SHALL reject the operation with `FAILED_PRECONDITION`.
3. WHEN the request contains a `delete_assignment_rule` operation, THE handler SHALL remove the assignment rule at the specified index, shifting remaining rules down.
4. WHEN a `delete_assignment_rule` operation would remove the last unconditional assignment rule and the `force` flag is not set, THE handler SHALL reject the operation with `FAILED_PRECONDITION`.
5. WHEN the request contains an `add_redirect_rule` operation, THE handler SHALL add a new redirect rule with the specified source and target build IDs.
6. WHEN the request contains an `add_redirect_rule` operation and a redirect rule with the same `source_build_id` already exists, THE handler SHALL reject the operation with `FAILED_PRECONDITION`. To change an existing redirect, use `replace_redirect_rule`.
7. WHEN the request contains a `replace_redirect_rule` operation, THE handler SHALL replace the redirect rule matching the specified source build ID with the new target.
8. WHEN the request contains a `replace_redirect_rule` operation and no redirect rule exists for the specified `source_build_id`, THE handler SHALL return `INVALID_ARGUMENT`.
9. WHEN the request contains a `delete_redirect_rule` operation, THE handler SHALL remove the redirect rule matching the specified source build ID.
10. WHEN the request contains a `delete_redirect_rule` operation and no redirect rule exists for the specified `source_build_id`, THE handler SHALL return `INVALID_ARGUMENT`.
11. WHEN the request contains a `commit_build_id` operation, THE handler SHALL add an unconditional assignment rule for the specified build ID at the END of the list, remove all previously added assignment rules targeting the same build ID, and remove any unconditional assignment rules for OTHER build IDs.
12. WHEN the request contains a `commit_build_id` operation and `force` is false, THE handler SHALL validate the target build ID before mutating rules by calling `WorkerRegistry::has_recent_poller_for_build_id(namespace_id, task_queue, build_id, now, RECENT_POLLER_WINDOW)`. If the query returns false, THE handler SHALL reject with `FAILED_PRECONDITION` and SHALL NOT call `VersioningRuleStore::apply_mutation`.
13. WHEN the request contains a `commit_build_id` operation and `force` is true, THE handler SHALL skip recent-poller validation and SHALL call `VersioningRuleStore::apply_mutation`.
14. WHEN the request carries a `conflict_token` that does not match the current stored token, THE handler SHALL return `FAILED_PRECONDITION`.
15. WHEN the operation succeeds, THE handler SHALL return the updated assignment rules, redirect rules, and the new conflict token.
16. WHEN the request specifies an out-of-bounds index for replace or delete operations, THE handler SHALL return `INVALID_ARGUMENT`. (Insert operations clamp to the end of the list instead of rejecting.)
17. WHEN the request specifies a `target_build_id` that is empty, THE handler SHALL return `INVALID_ARGUMENT`.

### Requirement 5: get_worker_versioning_rules Handler

**User Story:** As a Temporal SDK user, I want to read the current assignment and redirect rules for a task queue, so that I can inspect the versioning configuration.

#### Acceptance Criteria

1. WHEN the `get_worker_versioning_rules` endpoint is called with a valid namespace and task queue, THE handler SHALL return the current assignment rules, redirect rules, and conflict token.
2. WHEN no rules exist for the specified task queue, THE handler SHALL return empty rule lists with a valid conflict token.
3. THE handler SHALL return assignment rules in their current index order.

---

## Phase 2: Reachability

### Requirement 6: Build ID Reachability Computation

**User Story:** As a Temporal operator, I want to query the reachability of a build ID, so that I can determine when it is safe to decommission old workers.

#### Acceptance Criteria

1. WHEN a build ID is the target of any unconditional assignment rule (no percentage ramp or 100%), THE reachability computation SHALL classify the build ID as REACHABLE.
2. WHEN a build ID is the target of any assignment rule with a percentage ramp between 0 and 100 (exclusive), THE reachability computation SHALL classify the build ID as REACHABLE.
3. WHEN a build ID is the effective target of any redirect rule whose source build ID is itself REACHABLE for new workflows (i.e., targeted by an assignment rule), THE reachability computation SHALL include `NewWorkflows` for the redirect target as well, since new workflows are effectively delivered there.
4. WHEN a build ID is not referenced by any assignment rule or reachable redirect chain, THE reachability computation SHALL return an empty reachability list (unreachable per proto convention: "If reachability is empty, this worker is considered unreachable").
5. THE reachability computation SHALL be performed on-demand when the handler is called, with no background scanning.

> **NOTE:** Open-workflow-based classification (distinguishing CLOSED_WORKFLOWS_ONLY vs UNREACHABLE) requires a `RunRepository` query to count open workflows by `build_id`. This is deferred to the DSQL storage spec. For MVP, builds not referenced by any rule return an empty reachability list (unreachable).

### Requirement 7: get_worker_task_reachability Handler

**User Story:** As a Temporal operator, I want to query the `get_worker_task_reachability` gRPC endpoint, so that I can check whether it is safe to decommission workers for a specific build ID.

#### Acceptance Criteria

1. WHEN the `get_worker_task_reachability` endpoint is called with a list of build IDs and task queues, THE handler SHALL compute and return the reachability for each requested build ID on each requested task queue.
2. WHEN the request specifies task queues, THE handler SHALL limit reachability computation to those task queues.
3. WHEN the request does not specify task queues, THE handler SHALL compute reachability across all task queues that have versioning rules.
4. THE handler SHALL return a `BuildIdReachability` entry for each requested build ID, containing a list of `TaskQueueReachability` entries.

---

## Phase 3: Legacy and Operational

### Requirement 8: Legacy Version Set API — Unimplemented

**User Story:** As a Temporal SDK user using the legacy v1 versioning API, I want clear error messages indicating the API is not supported, so that I can migrate to the v2 rule-based API.

#### Acceptance Criteria

1. WHEN the `update_worker_build_id_compatibility` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Legacy worker versioning API (v1 version sets) is not supported. Use UpdateWorkerVersioningRules (v2 rule-based API) instead."
2. WHEN the `get_worker_build_id_compatibility` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Legacy worker versioning API (v1 version sets) is not supported. Use GetWorkerVersioningRules (v2 rule-based API) instead."

### Requirement 9: shutdown_worker Handler

**User Story:** As a Temporal operator, I want to gracefully drain a specific worker's sticky queue, so that it stops receiving new workflow tasks on its sticky queue and can be safely decommissioned.

#### Acceptance Criteria

1. WHEN the `shutdown_worker` endpoint is called with a worker identity, namespace, and sticky task queue, THE handler SHALL signal the Broker to stop delivering workflow tasks to that worker identity on that sticky queue.
2. WHEN the `shutdown_worker` endpoint is called with a `reason` field, THE handler SHALL log the reason for the shutdown.
3. WHEN the specified worker identity is not currently polling, THE handler SHALL return success (idempotent).
4. THE deny list key SHALL be `(NamespaceId, TaskQueueName, WorkerIdentity)` where `TaskQueueName` is the sticky queue name from the proto's `sticky_task_queue` field.
5. ONLY the workflow task poll path SHALL check the deny list. Activity task polls are not affected by `shutdown_worker`.

### Requirement 10: Deployment Management Handlers — Unimplemented

**User Story:** As a Temporal SDK user, I want clear error messages for deployment management endpoints, so that I understand these features are not yet available.

#### Acceptance Criteria

1. WHEN the `describe_deployment` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Deployment management is not yet supported. Worker versioning via assignment and redirect rules is available."
2. WHEN the `list_deployments` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Deployment management is not yet supported. Worker versioning via assignment and redirect rules is available."
3. WHEN the `get_deployment_reachability` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Deployment management is not yet supported. Use GetWorkerTaskReachability for build ID reachability."
4. WHEN the `get_current_deployment` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Deployment management is not yet supported."
5. WHEN the `set_current_deployment` endpoint is called, THE handler SHALL return `UNIMPLEMENTED` with the message "Deployment management is not yet supported."

---

## Phase 4: Integration

### Requirement 11: Wire Rule Evaluation into Workflow Start

**User Story:** As a Tokeira developer, I want the edge layer to evaluate assignment rules when a workflow starts, so that the workflow is automatically assigned to the correct build ID based on the operator's configured rules.

#### Acceptance Criteria

1. WHEN a `StartWorkflowExecution` request does not carry an explicit `build_id`, THE Edge_Layer SHALL evaluate assignment rules for the request's task queue to determine the build ID.
2. WHEN assignment rule evaluation returns a build ID, THE Edge_Layer SHALL set the `build_id` field on the `StartRequest` before passing it to the Runtime.
3. WHEN assignment rule evaluation returns `None` (no rules or no rule applies), THE Edge_Layer SHALL leave the `build_id` field as `None` on the `StartRequest`.
4. WHEN a `StartWorkflowExecution` request carries a versioning override, THE Edge_Layer SHALL inspect the override's behavior: if `Pinned`, use the override's `deployment_series` and `build_id` directly on the `StartRequest` and skip rule evaluation; if `AutoUpgrade`, evaluate assignment rules normally (auto-upgrade means "use latest version per rules").
5. WHEN a `SignalWithStartWorkflowExecution` request does not carry an explicit `build_id`, THE Edge_Layer SHALL evaluate assignment rules for the request's task queue, following the same logic as `StartWorkflowExecution`.

### Requirement 12: Wire Redirect Resolution into Task Dispatch

**User Story:** As a Tokeira developer, I want the runtime to resolve redirect rules when dispatching tasks, so that workflows are routed to the effective target build ID.

#### Acceptance Criteria

1. WHEN the RuntimeDispatchPublisher publishes a workflow task with a non-None `build_id` in the QueueKey, THE publisher SHALL resolve redirect rules for the task queue to find the effective build ID.
2. WHEN redirect resolution returns a different build ID, THE publisher SHALL update the QueueKey's `build_id` to the effective target before publishing to the Broker.
3. WHEN redirect resolution returns the same build ID (no redirect applies), THE publisher SHALL publish with the original QueueKey unchanged.
4. WHEN the QueueKey's `build_id` is `None`, THE publisher SHALL skip redirect resolution.
5. WHEN the RuntimeDispatchPublisher publishes an activity task with a non-None `build_id`, THE publisher SHALL apply the same redirect resolution logic.
6. WHEN the QueueKey has `deployment.is_some()`, THE publisher SHALL skip redirect resolution. Redirect rules apply only to build-ID-only versioning (deployment=None), not to deployment-pinned workflows.

### Requirement 13: Record Build ID Assignment After First WFT — DEFERRED

**User Story:** As a Tokeira developer, I want the build ID to be recorded on the workflow state after the first workflow task completion, so that the workflow stays pinned to that build ID for its lifetime.

> **NOTE — DEFERRED:** This requirement is deferred to a follow-up spec. The current implementation already sets `build_id` on `WorkflowState` at start time via assignment rules. Recording the worker-reported `build_id` after first WFT is a refinement that requires: (1) extending the edge DTO for WFT completion to carry `build_id`, (2) adding a kernel command to update `WorkflowState.build_id`, and (3) adding a state transition in the kernel. These changes cross the kernel purity boundary and are better addressed in a dedicated spec.

#### Acceptance Criteria

1. ~~WHEN a workflow completes its first workflow task and the worker reports a `build_id` in the completion, THE Runtime SHALL record the `build_id` on the `WorkflowState`.~~
2. ~~WHEN the `WorkflowState` already has a `build_id` set, THE Runtime SHALL NOT overwrite it with a different value from subsequent workflow task completions.~~
3. ~~WHEN the worker does not report a `build_id` in the completion, THE Runtime SHALL leave the `WorkflowState.build_id` unchanged.~~
