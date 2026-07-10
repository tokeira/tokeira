# Requirements Document

## Introduction

This spec covers `WorkflowService.ExecuteMultiOperation`. The RPC is IMPLEMENTED
(commit `05798cb1`, Tier 2.12 wave B): the edge handler lives at
`crates/tokeira-edge/src/grpc/workflow_service.rs:1231` and the compatibility
matrix classifies `multi-operation` as `FeatureState::Implemented`. The spec is
retained as the behavioral contract the implementation satisfies.

`ExecuteMultiOperation` is **not** a general batch-of-arbitrary-operations RPC. At the targeted
release (`TEMPORAL_SERVER_COMPAT = 1.31.0`) it carries exactly one shape — the **Update-with-Start**
composition: a `[StartWorkflow, UpdateWorkflow]` pair, in that order, targeting one workflow id. This
is a hard, deliberate limitation in the release, not a simplification we are choosing:

- The vendored proto states it: `ExecuteMultiOperationRequest.Operation` is a two-arm `oneof`
  (`start_workflow`, `update_workflow`) and its doc says "The only valid list of operations at this
  time is [StartWorkflow, UpdateWorkflow], in this order."
  (`proto/upstream/temporal/api/workflowservice/v1/request_response.proto` → `ExecuteMultiOperationRequest`).
- The server enforces it: `len(operations) != 2`, `operations[0]` not Start, or `operations[1]` not
  Update each return `errMultiOpNotStartAndUpdate` ("Operations have to be exactly [Start, Update].")
  (`service/frontend/workflow_handler.go:718-726 @ v1.31.0`; `service/frontend/errors.go:82 @ v1.31.0`).

There is **no** signal operation variant in this release. Any requirement mentioning "signal-style"
multi-operations would contradict ground truth and is out of scope (see `design.md` → Scope and
Non-Goals).

## Glossary

- **Update-with-Start (UwS):** The `[StartWorkflow, UpdateWorkflow]` composition — the only operation
  list `ExecuteMultiOperation` accepts at v1.31.0.
- **Atomic admission:** For the fresh-start path, the start and the update admission commit in a single
  durable transition, or neither does. There is no window in which the workflow exists without the
  update having been admitted.
- **Attach path:** When the target workflow already exists (or the start dedupes), the update is
  applied to the existing run rather than starting a new one.
- **Per-operation status:** One status entry per requested operation, in request order, returned inside
  the structured multi-operation failure when the request fails.
- **Aborted sibling:** In a failed request, the operation that did *not* itself fail is reported with
  gRPC code `Aborted` and a `MultiOperationExecutionAborted` detail, signalling it was not attempted.

## Target State

`Implemented`. `ExecuteMultiOperation` accepts exactly the `[StartWorkflow, UpdateWorkflow]`
composition, validates it before any mutation, executes it via the runtime — starting a new run with
the update admitted in one atomic transition, or attaching the update to an existing/deduped run — and
returns the ordered `[start_response, update_response]` pair. On failure it returns the structured
`MultiOperationExecution` error with per-operation statuses and the `Aborted` sibling, matching the
targeted release.

## Evidence From Current Code

- **Edge handler (implemented):** `execute_multi_operation` translates/validates and calls the
  runtime, including the structured `MultiOperationExecutionFailure` path
  (`crates/tokeira-edge/src/grpc/workflow_service.rs:1231`).
- **Kernel/runtime (implemented):** `Command::StartAndUpdate` + `apply_start_and_update`
  (`crates/tokeira-kernel`), `TokeiraRuntime::execute_multi_operation`
  (`crates/tokeira-runtime/src/runtime/lifecycle.rs:431`).
- **Matrix classification:** `multi-operation` is `FeatureState::Implemented`
  (`crates/tokeira-compatibility/src/matrix.rs:452`), consistent with
  `docs/readiness/command-surface.md` #28 and `docs/readiness/edge-unimplemented.md`.
- **Composed-start precedent (tokeira):** `Command::SignalWithStart` (`crates/tokeira-kernel/src/command.rs`)
  applied by `apply_signal_with_start` (`crates/tokeira-kernel/src/kernel.rs`) is a single atomic
  transition that emits `WorkflowExecutionStarted` plus the composed event. The runtime orchestrates
  path selection in `signal_with_start_workflow` (`crates/tokeira-runtime/src/runtime/lifecycle.rs`)
  via `resolve_conflict`. Update-with-Start mirrors this precedent (see design).
- **Update wait-stage machinery (tokeira):** `TokeiraRuntime::update_workflow`
  (`crates/tokeira-runtime/src/runtime/query.rs`) owns the `UpdateWaitPolicy`
  (Admitted/Accepted/Completed) lifecycle the update leg must reuse.
- **Upstream behaviour:** frontend validation/conversion `service/frontend/workflow_handler.go:704-895 @ v1.31.0`;
  history executor `service/history/api/multioperation/api.go @ v1.31.0`; error detail wire types
  `proto/upstream/temporal/api/errordetails/v1/message.proto` (`MultiOperationExecutionFailure`) and
  `proto/upstream/temporal/api/failure/v1/message.proto` (`MultiOperationExecutionAborted`).

## Operation Composition Policy

The request is a fixed two-tuple. There is no per-variant matrix to maintain.

| Position | Operation | Policy |
|---|---|---|
| `operations[0]` | `StartWorkflowExecutionRequest` | Required. Opens the composition. Subject to start-specific restrictions (Req 1). |
| `operations[1]` | `UpdateWorkflowExecutionRequest` | Required. Same target workflow id as `operations[0]`. Subject to update-specific restrictions (Req 1). |
| any other shape | — | Reject before mutation with `INVALID_ARGUMENT` ("Operations have to be exactly [Start, Update]."). |

## Requirements

### Requirement 1: Request Validation (validate before mutate)

**User Story:** As an SDK client issuing Update-with-Start, I want the request validated in full before
anything mutates, so that a malformed request never leaves a started workflow or a partially applied
update.

#### Acceptance Criteria

1. WHEN `operations` does not have length 2, OR `operations[0]` is not a `start_workflow`, OR
   `operations[1]` is not an `update_workflow`, THE Edge SHALL return `INVALID_ARGUMENT` with message
   "Operations have to be exactly [Start, Update]." and SHALL NOT call any runtime mutation.
   _(service/frontend/workflow_handler.go:718-726 @ v1.31.0; errors.go:82.)_
2. WHEN the start operation sets `cron_schedule`, `request_eager_execution`, or `workflow_start_delay`,
   THE Edge SHALL return `INVALID_ARGUMENT` (one distinct message per field, matching the release).
   _(workflow_handler.go:808-820 @ v1.31.0; errors.go — `errMultiOpStartCronSchedule`,
   `errMultiOpEagerWorkflow`, `errMultiOpStartDelay`.)_
3. WHEN the update operation sets `first_execution_run_id` or `workflow_execution.run_id`, THE Edge
   SHALL return `INVALID_ARGUMENT` (one distinct message per field).
   _(workflow_handler.go:836-842 @ v1.31.0; errors.go — `errMultiOpUpdateFirstExecutionRunId`,
   `errMultiOpUpdateExecutionRunId`.)_
4. WHEN either operation sets a non-empty `namespace` that differs from the request namespace, THE Edge
   SHALL return `INVALID_ARGUMENT` ("Operation namespace did not match request's namespace.").
   _(workflow_handler.go:803-806, 833-835 @ v1.31.0; errors.go — `errMultiOpNamespaceMismatch`.)_
5. WHEN the start operation's `workflow_id` and the update operation's `workflow_execution.workflow_id`
   differ, THE Edge SHALL return `INVALID_ARGUMENT` (workflow-id inconsistency) as part of the
   structured failure (Req 4), and SHALL NOT mutate. _(workflow_handler.go:766-778 @ v1.31.0;
   errors.go — `errMultiOpWorkflowIdInconsistent`.)_
6. THE Edge SHALL apply the same start-request and update-request validations it already applies to
   standalone `StartWorkflowExecution` and `UpdateWorkflowExecution` (field presence, retry policy,
   payload limits, etc.) before admitting the composition.
   _(workflow_handler.go:807, 831 @ v1.31.0 — `prepareStartWorkflowRequest`/`prepareUpdateWorkflowRequest`.)_
7. THE Edge SHALL NOT implement a per-namespace enablement gate: v1.31.0 has none. The
   `EnableExecuteMultiOperation` dynamic config and the handler's gate were removed upstream
   (#8818, "GA for a while now"); `errMultiOperationAPINotAllowed` (errors.go:129 @ v1.31.0) is
   dead code with zero usages at the tag, so no configuration of the pinned release can emit it.

### Requirement 2: Execution Paths and Atomicity

**User Story:** As an SDK client, I want Update-with-Start to either start-and-update atomically or
correctly attach the update to an existing run, so that the workflow and update semantics match
Temporal exactly and no partial state is ever observable.

#### Acceptance Criteria

1. WHEN the target workflow does not exist (or the start's reuse/conflict policy warrants a fresh run),
   THE runtime SHALL create the run and admit the update in a **single durable transition** (one
   kernel command, one storage commit). _(Property 3; mirrors `apply_signal_with_start` atomicity;
   multioperation/api.go → `startAndUpdateWorkflow` @ v1.31.0.)_
2. WHEN the target workflow is already running and the start operation dedupes against the existing run
   (same start request id / reuse policy), THE runtime SHALL attach the update to the existing run and
   SHALL NOT start a new run. _(multioperation/api.go → `canDedup` path @ v1.31.0.)_
3. WHEN the target workflow is already running and the start's
   `WorkflowIdConflictPolicy = USE_EXISTING` (or the update id is already present in the run's update
   registry), THE runtime SHALL apply the update to the existing run and SHALL NOT start a new run.
   _(multioperation/api.go → `WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING` / registry-find path @ v1.31.0.)_
4. WHEN the target workflow already exists and the requested update id has already **completed**, THE
   runtime SHALL return the stored update outcome together with a start response whose `started = false`
   and whose `status` reflects the workflow's current state, performing **no** mutation.
   _(multioperation/api.go → `GetUpdateOutcome` early return @ v1.31.0; proto note on
   `ExecuteMultiOperationResponse`.)_
5. IF validation of any operation fails (Req 1), THEN no runtime mutation method SHALL be invoked for
   any operation. _(Property 1.)_
6. IF the composition fails after validation (e.g. start conflict, update rejected), THEN the system
   SHALL NOT leave a run started without its admitted update, nor an update admitted without its run.
   _(Property 3.)_

### Requirement 3: Response Shape and Semantics

**User Story:** As an SDK client, I want a well-formed, correctly ordered response, so that the SDK can
map each result back to the operation that produced it.

#### Acceptance Criteria

1. On success THE Edge SHALL return exactly two responses: `responses[0]` a `StartWorkflowExecution`
   response and `responses[1]` an `UpdateWorkflowExecution` response, in that order.
   _(proto `ExecuteMultiOperationResponse`; workflow_handler.go:863-895 @ v1.31.0.)_
2. THE start response SHALL carry `run_id`, a `started` boolean indicating whether a new run was
   created (false on dedup/attach/already-complete paths), and a `status` reflecting the workflow's
   current execution status. _(proto `StartWorkflowExecutionResponse.status` doc; multioperation/api.go.)_
3. THE update response SHALL carry the update outcome and the reached lifecycle stage consistent with
   the update's requested `wait_policy` (Req 5).

### Requirement 4: Structured Failure (MultiOperationExecution)

**User Story:** As an SDK client, I want failures reported as a structured multi-operation error, so
that the SDK can surface which operation failed and that the other was aborted, not silently dropped.

#### Acceptance Criteria

1. WHEN the request fails after passing shape validation, THE Edge SHALL return a single gRPC error
   carrying a `MultiOperationExecutionFailure` detail with one `OperationStatus` per requested
   operation, in request order.
   _(proto `errordetails/v1/message.proto` → `MultiOperationExecutionFailure`;
   workflow_handler.go:785, 738-746 @ v1.31.0.)_
2. THE status for the operation that actually failed SHALL carry that operation's own error (same
   details as if it had been executed standalone).
3. THE status for the operation that did not fail SHALL carry gRPC code `Aborted` with a
   `MultiOperationExecutionAborted` detail. _(proto `failure/v1/message.proto` →
   `MultiOperationExecutionAborted`; errors.go:83 — `errMultiOpAborted`.)_
4. THE top-level gRPC status code SHALL equal the status code of the **first** operation that failed
   (e.g. a start `WorkflowExecutionAlreadyStarted` surfaces as `ALREADY_EXISTS`, not `INVALID_ARGUMENT`).
   _(service.proto:116-117 @ v1.31.0.)_
5. THE top-level error message SHALL be "Update-with-Start could not be executed."
   _(workflow_handler.go:741, 785 @ v1.31.0.)_
6. Existing start/update conflict and OCC/fencing mappings SHALL be preserved and surfaced through the
   per-operation status of the failing operation (they are not flattened to `INVALID_ARGUMENT`).

### Requirement 5: Update Wait-Stage Semantics

**User Story:** As an SDK client, I want the update leg of Update-with-Start to honour the update's
requested wait policy exactly as a standalone update does, so that blocking/return semantics are
identical.

#### Acceptance Criteria

1. THE update leg SHALL honour the update's requested lifecycle stage (Admitted / Accepted / Completed)
   using the same wait-stage machinery as standalone `UpdateWorkflowExecution`
   (`TokeiraRuntime::update_workflow`), including returning before completion for non-`Completed`
   stages.
2. WHEN the requested stage is not reached before the RPC deadline, THE Edge SHALL surface the same
   timeout/stage behaviour as standalone update.

### Requirement 6: Closing-Workflow Retry (deliberate scope decision)

**User Story:** As a maintainer, I want the "update aborted by a closing workflow" retry either
implemented to the release default or explicitly deferred with a cited reason, so it is not mistaken
for an oversight.

#### Acceptance Criteria

1. THE spec SHALL treat the two dynamic-config gates for this behaviour
   (`EnableUpdateWithStartRetryOnClosedWorkflowAbort`,
   `EnableUpdateWithStartRetryableErrorOnClosedWorkflowAbort`) as pinned constants at their v1.31.0
   defaults, per the config-as-constant convention. _(multioperation/api.go:130-160 @ v1.31.0.)_
2. THE implementation SHALL either (a) implement the retry-once path and the `NotFound → Aborted`
   second-operation error conversion for client retry, matching the release; or (b) classify-skip the
   specific corpus sub-cases that exercise it, recording a cited reason in the conformance skip
   registry. The design SHALL state which, and why. _(This is a scope decision, not a silent gap.)_

### Requirement 7: Observability

**User Story:** As an operator, I want Update-with-Start failures and paths to be observable through the
existing start/update signals, so that invalid requests and conflicts are visible without a new metric
surface.

#### Acceptance Criteria

1. THE composition SHALL emit the metrics already emitted by its constituent start and update paths; no
   new multi-operation-specific metric is required unless the targeted corpus asserts one. IF the
   corpus asserts a multi-operation-specific metric, THE spec SHALL add it under the metrics-manifest
   discipline. _(No multi-operation-specific metric assertion was found in the targeted suites; verify
   during implementation.)_
