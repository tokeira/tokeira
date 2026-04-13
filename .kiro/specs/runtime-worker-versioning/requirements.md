# Requirements Document: Worker Versioning and Deployment Routing

## Introduction

This document captures the requirements for Feature 10 of the Tokeira runtime: Worker Versioning and Deployment Routing. The feature enables the Broker to route tasks based on `deployment` and `build_id` fields in QueueKey, so that workers running different code versions receive only compatible tasks.

The QueueKey type already carries `deployment: Option<DeploymentId>` and `build_id: Option<BuildId>` fields, and the InMemoryBroker already keys its ready queues by full QueueKey (including these fields). The main gaps are:

1. The `poll_workflow_task` and `poll_activity_task` APIs do not accept deployment/build_id from the worker — they construct QueueKey with `None` for both fields.
2. There is no worker registration mechanism that records a worker's version metadata.
3. The broker's existing exact-match behavior is correct for versioned routing but is untested for non-None deployment/build_id values.

This feature depends on Feature 1 (Lane OCC Retry and Mailbox Coalescing) and Feature 2 (Activity Pump).

The authoritative specifications are [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md) for queue family identity and the three-tier delivery model.

## Glossary

- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, kernel invocation, storage commits, and derived-effect publication.
- **Broker**: The in-memory delivery subsystem (`InMemoryBroker`) that matches pending tasks with waiting pollers. Keyed by QueueKey.
- **Activity_Broker**: The in-memory activity-task delivery subsystem (`InMemoryActivityBroker`) that matches activity tasks with activity pollers. Keyed by QueueKey.
- **QueueKey**: Composite key `(namespace_id, task_queue_name, task_kind, deployment, build_id)` used to route tasks to compatible workers.
- **DeploymentId**: Identifier grouping a set of workers into a deployment. Used together with BuildId for versioned task routing.
- **BuildId**: Immutable build identifier baked into a worker binary. Used to ensure replay determinism by routing tasks to workers running compatible code.
- **Worker_Versioning**: Deployment-aware task routing where workers register with version/deployment metadata and the Broker matches tasks to compatible workers.
- **Worker_Registration**: The mechanism by which a worker declares its deployment and build_id to the Runtime, so that subsequent poll requests use the correct QueueKey.
- **Unversioned_Worker**: A worker that registers without deployment or build_id metadata. Receives only tasks with `None` deployment and `None` build_id in the QueueKey.
- **Versioned_Worker**: A worker that registers with a specific deployment and/or build_id. Receives only tasks whose QueueKey deployment and build_id match exactly.
- **Edge_Layer**: The translation layer (`tokeira-edge`) that converts external gRPC requests into internal Runtime types, including QueueKey construction for poll requests.

## Requirements

---

### Requirement 1: Worker Registration with Version Metadata

**User Story:** As a Tokeira developer, I want workers to register with optional deployment and build_id metadata, so that the Runtime can track active workers and their versions for observability and future sweeper use.

#### Acceptance Criteria

1. THE Runtime SHALL accept worker registration that includes an optional DeploymentId and an optional BuildId.
2. THE Runtime SHALL store the registered DeploymentId and BuildId for each worker, keyed by WorkerIdentity, namespace, and task queue.
3. WHEN a worker registers without a DeploymentId and without a BuildId, THE Runtime SHALL record the worker as an Unversioned_Worker.
4. WHEN a worker registers with a DeploymentId or BuildId, THE Runtime SHALL record the worker as a Versioned_Worker.
5. WHEN a worker re-registers with different version metadata, THE Runtime SHALL update the stored metadata to reflect the new values.
6. THE WorkerRegistry is observational — it does not drive routing. Routing is request-carried: the edge layer constructs the QueueKey from the poll request's deployment/build_id fields.

---

### Requirement 2: Deployment-Aware Poll Request Construction

**User Story:** As a Tokeira developer, I want poll requests to carry deployment and build_id directly, so that the Broker matches tasks using the correct QueueKey without depending on a registry lookup.

#### Acceptance Criteria

1. THE Runtime's `poll_workflow_task` and `poll_activity_task` methods SHALL accept a `QueueKey` that already includes the caller-provided `deployment` and `build_id` values. Routing is request-carried — the caller (edge layer) is responsible for constructing the correct QueueKey.
2. WHEN an Unversioned_Worker polls for tasks, THE caller SHALL construct the QueueKey with `None` for deployment and `None` for build_id.
3. WHEN a Versioned_Worker polls for tasks, THE caller SHALL construct the QueueKey with the worker's deployment and build_id values from the poll request.

---

### Requirement 3: Edge Layer Version Metadata Propagation (Workflow Polls)

**User Story:** As a Tokeira developer, I want the Edge layer to propagate deployment and build_id from incoming workflow poll requests, so that versioned workers are correctly identified at the API boundary.

#### Acceptance Criteria

1. WHEN a `PollWorkflowTaskQueueRequest` includes deployment and build_id fields, THE Edge_Layer SHALL include the DeploymentId and BuildId in the translated QueueKey.
2. WHEN a `PollWorkflowTaskQueueRequest` omits deployment and build_id fields, THE Edge_Layer SHALL set deployment and build_id to `None` in the translated QueueKey.
3. Activity poll edge translation is deferred until the activity poll gRPC endpoint is added to the edge layer. The runtime's `poll_activity_task` already accepts a `QueueKey` — the caller constructs it correctly.

---

### Requirement 4: Deployment-Aware Broker Routing

**User Story:** As a Tokeira developer, I want the Broker to route tasks based on the full QueueKey including deployment and build_id, so that versioned tasks reach only compatible workers.

#### Acceptance Criteria

1. WHEN a task is published with a non-None DeploymentId or BuildId in its QueueKey, THE Broker SHALL only match the task with pollers registered for a QueueKey with the same DeploymentId and BuildId.
2. WHEN a task is published with `None` deployment and `None` build_id in its QueueKey, THE Broker SHALL match the task with any poller whose QueueKey has `None` deployment and `None` build_id for the same namespace and task_queue.
3. THE Broker SHALL maintain separate ready queues per QueueKey, including the deployment and build_id dimensions.
4. THE Activity_Broker SHALL apply the same deployment-aware routing rules as the Broker for activity tasks.
5. THE Broker SHALL NOT deliver a task to a worker whose QueueKey deployment or build_id does not match the task's QueueKey deployment or build_id.

---

### Requirement 5: Version-Aware Task Holding

**User Story:** As a Tokeira developer, I want the Broker to hold tasks when no compatible poller is available, so that versioned tasks are not lost or misdelivered.

#### Acceptance Criteria

1. WHEN a versioned task is published and no poller with a compatible QueueKey is waiting, THE Broker SHALL hold the task in the ready queue keyed by the task's full QueueKey.
2. WHEN a compatible poller arrives after a versioned task has been held, THE Broker SHALL deliver the held task to the poller.
3. WHEN no compatible poller arrives, THE Broker SHALL continue holding the task in the in-memory ready queue (existing behavior). Durable backlog fallback after a grace window is deferred to Feature 12 (Durable Backlog Integration).
4. THE Activity_Broker SHALL apply the same task-holding behavior for versioned activity tasks.

---

### Requirement 6: Kernel Dispatch Op Version Propagation

**User Story:** As a Tokeira developer, I want the Kernel's dispatch ops to carry deployment and build_id from the workflow's configuration, so that tasks are published to the correct versioned queue.

#### Acceptance Criteria

1. THE `StartRequest` SHALL carry `deployment: Option<DeploymentId>` and `build_id: Option<BuildId>` fields so that workflows can be pinned to a deployment at start time.
2. THE kernel's `apply_start` SHALL populate `WorkflowState.deployment` and `WorkflowState.build_id` from the `StartRequest`.
3. THE Edge_Layer SHALL propagate deployment and build_id from the `StartWorkflowExecutionRequest` gRPC message into the `StartRequest`.
4. WHEN the Kernel emits a DispatchOp::EnqueueWorkflowTask, THE Kernel SHALL populate the QueueKey deployment and build_id fields from the workflow run's `WorkflowState.deployment` and `WorkflowState.build_id`.
5. WHEN the Kernel emits a DispatchOp::EnqueueActivityTask, THE Kernel SHALL populate the QueueKey deployment and build_id fields from the activity's configured deployment and build_id, falling back to the workflow run's values when the activity does not specify its own.
6. WHEN a workflow run has no configured deployment or build_id, THE Kernel SHALL set the QueueKey deployment and build_id to `None` in the dispatch op.

---

### Requirement 7: Versioned Routing Isolation

**User Story:** As a Tokeira developer, I want versioned and unversioned task queues to be fully isolated, so that a deployment mismatch never causes incorrect task delivery.

#### Acceptance Criteria

1. THE Broker SHALL treat QueueKeys that differ only in deployment or build_id as distinct queues with no cross-delivery.
2. WHEN a Versioned_Worker polls with a specific DeploymentId and BuildId, THE Broker SHALL NOT deliver tasks from the unversioned queue (deployment=None, build_id=None) to that worker.
3. WHEN an Unversioned_Worker polls with `None` deployment and `None` build_id, THE Broker SHALL NOT deliver tasks from any versioned queue to that worker.
4. THE Broker SHALL enforce isolation for both workflow tasks and activity tasks.

---

### Requirement 8: Activity Retry Version Preservation

**User Story:** As a Tokeira developer, I want activity retries to preserve the original deployment and build_id, so that retried activities are dispatched to the same versioned queue as the original attempt.

#### Acceptance Criteria

1. WHEN the Runtime re-dispatches a failed activity for retry, THE Runtime SHALL construct the retry dispatch QueueKey with the same DeploymentId and BuildId as the original activity dispatch.
2. THE Runtime SHALL NOT default retry dispatch QueueKey deployment and build_id to `None` when the original dispatch carried non-None values.
