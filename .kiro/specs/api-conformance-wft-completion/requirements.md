# Requirements Document

## Introduction

This spec completes field-level conformance for `RespondWorkflowTaskCompleted`. The handler is Partial. Field accounting is anchored to the v1.62.11 proto (`RespondWorkflowTaskCompletedRequest`, 20 fields) and behaviour to Temporal server [tag `v1.31.0`](https://github.com/temporalio/temporal/tree/v1.31.0) per AGENTS.md §8. The remaining gaps in `UNSUPPORTED_FIELDS.md` are sticky attributes, SDK metadata, metering metadata, deployment metadata, versioning behavior, and complete `return_new_workflow_task` semantics.

## Glossary

- **WFT completion:** A worker response that completes a workflow task and may carry commands, metadata, and polling preferences.
- **Sticky attributes:** Worker-provided sticky task queue data used for cache-affine workflow task dispatch.
- **Return-new-WFT:** Temporal behavior where a completion response can include the next workflow task immediately.
- **Deployment options:** The current (`v1.62`) worker deployment/versioning fields (`deployment_options`, `versioning_behavior`) that supersede the deprecated `binary_checksum`, `worker_version_stamp`, and `deployment` fields.

## Target State

`Implemented`. Completion metadata, sticky attributes, metering metadata, and
the current worker deployment/versioning fields are translated and persisted
where durable; `return_new_workflow_task` is implemented to Temporal semantics.
**Applying** deployment/versioning fields to dispatch routing is owned by
`worker-deployments`; this spec only preserves and threads them into
history/state. The deprecated `binary_checksum` / `worker_version_stamp` /
`deployment` fields are accepted for back-compat and do not drive new behavior.

## Evidence From Current Code

- Proto message inspected against the vendored v1.62.11 surface: `RespondWorkflowTaskCompletedRequest` (20 fields) in `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`.
- Current handler: `respond_workflow_task_completed`.
- Current translation: `respond_completed_request_to_edge` already threads `sdk_metadata`, `worker_version_stamp.build_id`, `query_results`, `messages` (update protocol), `force_create_new_workflow_task`, and `return_new_workflow_task`; this spec completes durable history persistence and adds the remaining fields.
- Current DTO/kernel request: `WorkflowTaskCompletedRequest`.
- Unsupported-field entry: `RespondWorkflowTaskCompletedRequest` in `UNSUPPORTED_FIELDS.md` (missing the current `deployment_options`, `resource_id`, `worker_instance_key`, `worker_control_task_queue`, and `capabilities`; this spec re-anchors accounting to the proto).
- Related runtime areas: query consistency model, broker, worker registry, versioning rule store.

## Completion Field Policy

| Proto field | Current state | Target policy | Error if invalid | Persistence/history impact |
|---|---|---|---|---|
| `task_token`, `commands`, `identity`, `namespace` | Supported | Preserve | existing token/command errors | Kernel transition/history |
| `query_results` (8) | Supported | Preserve | existing query errors | Query response delivery |
| `messages` (11) | Supported | Preserve (update protocol transport) | existing update errors | Owned jointly with `api-conformance-update-lifecycle` |
| `force_create_new_workflow_task` (6) | Supported | Preserve forced-WFT behavior | n/a | Runtime WFT scheduling |
| `return_new_workflow_task` (5) | Partial | Implement inline next-WFT delivery when safely available | n/a | Runtime response |
| `sdk_metadata` (12) | Partially threaded | Complete durable persistence into `WorkflowTaskCompleted` | n/a | `WorkflowTaskCompleted` event |
| `metering_metadata` (13) | Not supported | Preserve as informational completion metadata | n/a | `WorkflowTaskCompleted` event |
| `sticky_attributes` (4) | Not supported | Persist sticky task queue attributes and update sticky routing | validation errors only | Sticky routing state |
| `deployment_options` (17) | Not supported | Persist as the current worker deployment/versioning metadata; routing application owned by `worker-deployments` | validation errors only | History/state metadata |
| `versioning_behavior` (16) | Not supported | Persist requested versioning behavior; routing application owned by `worker-deployments` | validation errors for unknown enum | History/state metadata |
| `capabilities` (14) | Decoded at edge | Preserve `discard_speculative_workflow_task_with_events`; consumed by `speculative-wft` | n/a | Edge decision input |
| `resource_id` (18) | Not modeled | Accept as routing envelope (carries workflow id); no new semantics | n/a | Routing only |
| `worker_instance_key` (19) | Not modeled | Empty until worker-lifecycle tracking exists | n/a | Owned by worker lifecycle/heartbeat |
| `worker_control_task_queue` (20) | Not modeled | Empty until per-worker Nexus control transport exists | n/a | Owned by Nexus task transport |
| `binary_checksum` (7, deprecated) | Accepted | Accept for back-compat only; no new behavior | n/a | None |
| `worker_version_stamp` (10, deprecated) | Partial | Accept `build_id` for back-compat; superseded by `deployment_options` | validation errors only | History metadata (legacy) |
| `deployment` (15, deprecated) | Not supported | Accept for back-compat only; superseded by `deployment_options` | n/a | None |

## Requirements

### Requirement 1: Completion Metadata Preservation

**User Story:** As an SDK worker, I want completion metadata preserved, so that server history and diagnostics reflect worker behavior.

#### Acceptance Criteria

1. WHEN `sdk_metadata` is present, THE Edge SHALL serialize and thread it into `WorkflowTaskCompletedRequest` and durably persist it on the `WorkflowTaskCompleted` history event.
2. WHEN `metering_metadata` is present, THE Edge SHALL preserve it as informational completion metadata on the history event.
3. WHEN `deployment_options` (current) or `versioning_behavior` is present, THE Edge SHALL persist it as worker deployment/versioning metadata; applying it to dispatch routing is owned by `worker-deployments` and is out of scope here.
4. WHEN the deprecated `binary_checksum`, `worker_version_stamp`, or `deployment` fields are present, THE Edge SHALL accept them for back-compat only and SHALL NOT drive new behavior; they are superseded by `deployment_options`.
5. WHEN `resource_id` is present, THE Edge SHALL treat it as routing-envelope data only and SHALL NOT derive new workflow semantics from it.
6. WHERE `worker_instance_key` or `worker_control_task_queue` is present, THE Edge SHALL leave the corresponding worker-lifecycle / Nexus-control behavior default; these are owned by worker lifecycle and Nexus task transport respectively.

### Requirement 2: Sticky, Versioning, and Speculative Capability

**User Story:** As an SDK worker, I want sticky, versioning, and capability fields handled deterministically, so that cache and deployment features do not behave unpredictably.

#### Acceptance Criteria

1. WHEN `sticky_attributes` are present, THE runtime SHALL update sticky routing for the workflow execution.
2. WHEN `versioning_behavior` is present, THE Edge SHALL persist the requested versioning behavior on durable state/history; applying it to subsequent dispatch is owned by `worker-deployments`.
3. WHEN `capabilities.discard_speculative_workflow_task_with_events` is present, THE Edge SHALL preserve it for the `speculative-wft` feature to consume.
4. THE handler SHALL NOT silently ignore non-default sticky, versioning, or capability fields; unknown `versioning_behavior` enum values SHALL be rejected as `INVALID_ARGUMENT`.

### Requirement 3: Return-New-Workflow-Task Semantics

**User Story:** As an SDK worker, I want `return_new_workflow_task` to match Temporal semantics, so that worker poll loops can use completion response optimization safely.

#### Acceptance Criteria

1. WHEN `return_new_workflow_task` is false, THE response SHALL preserve existing behavior.
2. WHEN `return_new_workflow_task` is true and an immediately available WFT exists for the same worker/task queue, THE response SHALL include it.
3. WHEN no immediate WFT exists, THE response SHALL return without inventing an empty started task.
4. Returned inline WFTs SHALL carry the same token, history, sticky, and versioning metadata as a normal poll response.

### Requirement 4: Error and Token Validation

**User Story:** As an operator, I want invalid WFT completions rejected consistently, so that workers cannot corrupt history.

#### Acceptance Criteria

1. Malformed task tokens SHALL return `INVALID_ARGUMENT`.
2. Stale shard epoch or ownership failures SHALL continue to return the existing not-owner error.
3. Invalid non-default fields SHALL return `INVALID_ARGUMENT` or `FAILED_PRECONDITION` before command mutation.
