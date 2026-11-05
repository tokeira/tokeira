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
history/state. Deprecated metadata remains accepted; the structured `worker_version_stamp` also
participates in the pinned inline-task compatibility gate below.

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
| `force_create_new_workflow_task` (6) | Partial | Preserve forced scheduling and apply the heartbeat-only limit in transient-wft Item C | `NOT_FOUND` after a committed heartbeat timeout | Original scheduled time and timeout transition |
| `return_new_workflow_task` (5) | Partial | Start an eligible follow-up in the completion transition for the completing worker | Existing completion errors | Started pending state; no dispatch row or offer |
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
| `worker_version_stamp` (10, deprecated) | Partial | Preserve stamp and use the pinned assigned-build compatibility gate for inline return | validation errors only | History metadata and inline eligibility |
| `deployment` (15, deprecated) | Not supported | Accept for back-compat only; superseded by `deployment_options` | n/a | None |

## Requirements

### Requirement 1: Completion Metadata Preservation

**User Story:** As an SDK worker, I want completion metadata preserved, so that server history and diagnostics reflect worker behavior.

#### Acceptance Criteria

1. WHEN `sdk_metadata` is present, THE Edge SHALL serialize and thread it into `WorkflowTaskCompletedRequest` and durably persist it on the `WorkflowTaskCompleted` history event.
2. WHEN `metering_metadata` is present, THE Edge SHALL preserve it as informational completion metadata on the history event.
3. WHEN `deployment_options` (current) or `versioning_behavior` is present, THE Edge SHALL persist it as worker deployment/versioning metadata; applying it to dispatch routing is owned by `worker-deployments` and is out of scope here.
4. WHEN deprecated metadata is present, THE Edge SHALL preserve its existing treatment; THE runtime SHALL additionally use the structured `worker_version_stamp` for Requirement 3.5’s inline compatibility gate.
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

1. WHEN `return_new_workflow_task` is false, THE completion SHALL dispatch any required follow-up normally and return no real inline task; an update-driven speculative candidate SHALL become Normal when task generation cannot be bypassed.
2. WHEN `return_new_workflow_task` is true, the completion succeeds, and an eligible follow-up is required, THE kernel SHALL schedule and start it for the completing identity on the new task's selected queue within the completion's transition.
3. WHEN no follow-up is required or the run closes, THE response SHALL contain no fabricated real started task.
4. WHEN the response carries a real inline task, THE task SHALL carry the committed token, attempt, scheduled/started IDs and times, incremental history after the previous started event, and applicable versioning, query and update metadata.
5. WHEN the completion stamp has `use_versioning=true`, the assigned build ID is nonempty, and the stamp build differs, THE completion SHALL dispatch the normal follow-up rather than start it inline; an ongoing deployment transition SHALL likewise prevent inline start.
6. WHEN the workflow uses only old stamp-based versioning with no assigned build ID, THE inline start SHALL omit that stamp from its start operands, as v1.31.0 does.
7. WHEN the completion fails the workflow task, THE runtime SHALL expose no inline task and SHALL preserve the pinned failure/retry dispatch behavior.
8. WHEN the completion starts a real task inline, THE kernel SHALL emit no enqueue operation and THE runtime/storage SHALL create neither offer nor workflow dispatch row for it, independent of parked normal or sticky polls.
9. WHEN outgoing updates alone require a speculative follow-up and inline return is permitted, THE completion SHALL start that speculative task for its caller without an offer or dispatch row, preserving virtual IDs, discard/materialization and timeout behavior.
10. IF the inline response is lost, THEN THE task SHALL stay started until its applicable timeout; the runtime SHALL NOT immediately make it available to other polls.
11. WHEN an inline task is exposed, THE runtime/edge SHALL preserve update delivery ordering, buffered-query barriers and scoped worker provenance; a query-only response SHALL NOT substitute for a required real WFT.

Authority: `service/history/api/respondworkflowtaskcompleted/api.go:512-600,702-725,731-773 @ v1.31.0`.
The build-ID comparison is inside the `use_versioning` and assigned-build guards,
not a blanket comparison of every build string. A deployment transition forces a
Normal follow-up before speculative selection. Temporal creates/starts speculative
tasks after persistence; Tokeira's durable pending-state mapping is explained in
[the design](design.md#return-new-wft-safety). The heartbeat-timeout successor
follows Requirement 3 but is not returned; its exact timeout, sticky, update/query
and retry behavior is owned by
[transient-wft Item C](../transient-wft/requirements.md#item-c--wft-heartbeat-timeout).

### Requirement 4: Error and Token Validation

**User Story:** As an operator, I want invalid WFT completions rejected consistently, so that workers cannot corrupt history.

#### Acceptance Criteria

1. Malformed task tokens SHALL return `INVALID_ARGUMENT`.
2. Stale shard epoch or ownership failures SHALL continue to return the existing not-owner error.
3. Invalid non-default fields SHALL return `INVALID_ARGUMENT` or `FAILED_PRECONDITION` before command mutation.
