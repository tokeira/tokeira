# Requirements Document

## Introduction

This spec completes `UpdateWorkflowExecution` and `PollWorkflowExecutionUpdate` conformance.
Both handlers are Partial; `UNSUPPORTED_FIELDS.md` documents the missing `update_ref` and `stage`
response fields. But the gap is larger than two response fields: both RPCs are **blocking calls
governed by a `WaitPolicy`** — the call returns when the update reaches the client-requested
lifecycle stage, or when the server's maximum wait time expires (whichever comes first), and
reports the actual stage reached. This spec specifies that wait contract, the lifecycle-stage
ladder and its defaulting/validation rules, the durable source of truth for each stage, and the
poll consistency and error behaviour — all verified against the targeted Temporal release.

Behaviour is verified against Temporal server
[tag `v1.31.0`](https://github.com/temporalio/temporal/tree/v1.31.0) per AGENTS.md §8
(`service/frontend/workflow_handler.go`, `service/history/api/pollupdate/api.go`,
`common/enums/defaults.go`), and proto shape against the vendored API `v1.62.11`
(`proto/upstream/temporal/api/update/v1/message.proto`,
`proto/upstream/temporal/api/enums/v1/update.proto`, and the update RPC messages in
`proto/upstream/temporal/api/workflowservice/v1/request_response.proto`).

## Glossary

- **Update:** A workflow update request, identified workflow-scoped by `update_id`
  (`update/v1/Meta.update_id`).
- **Update ref (`UpdateRef`):** The stable reference `{workflow_execution, update_id}` returned on
  the update response and required on the poll request. Per the proto it is **never null** on the
  `UpdateWorkflowExecutionResponse`.
- **Lifecycle stage:** A position on the ordered ladder
  `UNSPECIFIED < ADMITTED < ACCEPTED < COMPLETED`
  (`enums/v1/update.proto` `UpdateWorkflowExecutionLifecycleStage`). The stage is distinct from the
  **outcome**.
- **Outcome (`Outcome`):** The terminal result of an update — `success` (Payloads) or `failure`
  (Failure) — set if and only if the update has COMPLETED. A worker **rejection** of an update is
  represented as a COMPLETED stage carrying a failure outcome; it is not a separate stage.
- **Wait policy (`WaitPolicy`):** The client's requested `lifecycle_stage` to block for. Present on
  both `UpdateWorkflowExecutionRequest` and `PollWorkflowExecutionUpdateRequest`.
- **Server maximum wait time:** The server-side long-poll soft timeout that bounds how long the
  call blocks waiting for the requested stage; on expiry the call returns the actual reached stage
  (which may be `UNSPECIFIED` relative to the request) without an error.
- **Update registry:** The runtime's in-flight tracking of updates that have been admitted but are
  not yet durably resolved.

## Target State

`ImplementedSubset`. `UpdateWorkflowExecution` and `PollWorkflowExecutionUpdate` honour the
`WaitPolicy` blocking contract for the stages Tokeira supports, populate `update_ref`, `stage`, and
`outcome` from durable lifecycle state, default and validate the wait stage exactly as the targeted
release does, and return a single committed error behaviour for unknown updates.

## Evidence From Current Code

- **Proto (authoritative; vendored API v1.62.11):**
  - `UpdateWorkflowExecutionRequest` (`request_response.proto`): `namespace` (1),
    `workflow_execution` (2), `first_execution_run_id` (3), `wait_policy` (4), `request` (5).
  - `UpdateWorkflowExecutionResponse`: `update_ref` (1, "Never null"), `outcome` (2, set only when
    COMPLETED), `stage` (3, "most advanced lifecycle stage known to have been reached").
  - `PollWorkflowExecutionUpdateRequest`: `namespace` (1), `update_ref` (2), `identity` (3),
    `wait_policy` (4, "Omit to request a non-blocking poll").
  - `PollWorkflowExecutionUpdateResponse`: `outcome` (1), `stage` (2), `update_ref` (3).
  - `WaitPolicy.lifecycle_stage`, `UpdateRef {workflow_execution, update_id}`,
    `Outcome {success | failure}` (`update/v1/message.proto`);
    `UpdateWorkflowExecutionLifecycleStage` ladder (`enums/v1/update.proto`).
- **Behaviour (authoritative; Temporal server v1.31.0):**
  - Frontend validation/defaulting (`service/frontend/workflow_handler.go @ v1.31.0`):
    `update_id` is generated (UUID) when the client omits it (≈5255-5257); an absent `wait_policy`
    is treated as empty (≈5266); `SetDefaultUpdateWorkflowExecutionLifecycleStage` defaults an
    unspecified stage to **COMPLETED** (`common/enums/defaults.go:71-75 @ v1.31.0`); a wait stage of
    **ADMITTED is rejected** with `errUpdateWorkflowExecutionAsyncAdmittedNotAllowed`
    (`workflow_handler.go:5277 @ v1.31.0`); ACCEPTED async is gated behind a config flag.
  - Poll handler (`service/history/api/pollupdate/api.go @ v1.31.0`): an unknown `update_id` →
    `serviceerror.NewNotFoundf("update %q not found")` (hard `NOT_FOUND`); otherwise
    `upd.WaitLifecycleStage(ctx, waitStage, softTimeout)` blocks until the requested stage or the
    soft timeout, then returns `{outcome, stage, update_ref}` (the response echoes the ref).
- **Current handlers:** `update_workflow_execution`, `poll_workflow_execution_update`.
- **Runtime/kernel:** update registry, update protocol messages, and the history events
  `WorkflowExecutionUpdateAccepted` / `WorkflowExecutionUpdateCompleted` /
  `WorkflowExecutionUpdateRejected` (already present in `crates/tokeira-kernel/src/event.rs`).

## Supported-Stage Policy (Tokeira)

Tokeira supports synchronous waits up to the stage it can durably observe. Each stage maps to a
durable source of truth, reconciling with history-as-authority (AGENTS.md §3):

| Lifecycle stage | Source of truth | Supported as a wait target |
|---|---|---|
| `UNSPECIFIED` | n/a (defaulting input only) | n/a — defaults to COMPLETED |
| `ADMITTED` | transient update registry | **No** — rejected as `INVALID_ARGUMENT` (matches v1.31.0 `errUpdateWorkflowExecutionAsyncAdmittedNotAllowed`) |
| `ACCEPTED` | committed `WorkflowExecutionUpdateAccepted` history event | Yes |
| `COMPLETED` | committed `WorkflowExecutionUpdateCompleted` / `WorkflowExecutionUpdateRejected` history event (carries the outcome) | Yes (default) |

The admitted-but-not-accepted state is transient registry state used only to drive the wait path
and to detect duplicate `update_id`s; it is not required to survive runtime restart. ACCEPTED and
COMPLETED/rejected are recoverable from committed history on replay, so polls remain consistent
after restart.

## Requirements

### Requirement 1: Update Response Metadata and Outcome

**User Story:** As an SDK client, I want the update response to carry a stable `update_ref`, the
reached `stage`, and the `outcome` when complete, so that I can correlate, poll, and reason about
an update.

#### Acceptance Criteria

1. WHEN `UpdateWorkflowExecution` admits an update, THE response SHALL populate `update_ref` with
   `{workflow_execution, update_id}`; `update_ref` SHALL never be absent on the response.
2. WHEN the client omits `request.meta.update_id`, THE Edge SHALL generate a unique `update_id` and
   use it in `update_ref` (matching v1.31.0 `workflow_handler.go @ v1.31.0`).
3. WHEN the update has reached COMPLETED before the call returns, THE response SHALL populate
   `outcome` (success or failure); WHILE the update has not COMPLETED, THE response SHALL leave
   `outcome` unset.
4. THE response `stage` SHALL be the most advanced lifecycle stage the update is known to have
   reached at return time, drawn from the durable source of truth in the Supported-Stage Policy.
5. IF a supported stage cannot be represented because the runtime lacks the lifecycle state, THE
   implementation SHALL add the runtime/history-derived state rather than inventing an edge-only
   stage value.

### Requirement 2: Wait Policy and Blocking Contract

**User Story:** As an SDK client, I want `UpdateWorkflowExecution` to block until my requested
lifecycle stage (or the server's maximum wait time), so that synchronous update semantics match
Temporal.

#### Acceptance Criteria

1. WHEN `wait_policy` is absent or its `lifecycle_stage` is `UNSPECIFIED`, THE Edge SHALL default
   the wait stage to `COMPLETED` (matching `SetDefaultUpdateWorkflowExecutionLifecycleStage`,
   `common/enums/defaults.go:71-75 @ v1.31.0`).
2. WHEN the requested `lifecycle_stage` is `ADMITTED`, THE Edge SHALL return `INVALID_ARGUMENT`
   (the Tokeira analog of `errUpdateWorkflowExecutionAsyncAdmittedNotAllowed`,
   `workflow_handler.go:5277 @ v1.31.0`); ADMITTED is not a supported async wait target.
3. WHILE the update has not yet reached the requested (or defaulted) stage, THE call SHALL block up
   to the server maximum wait time.
4. WHEN the update reaches the requested stage before the server maximum wait time, THE call SHALL
   return with `stage` at least the requested stage and `outcome` set if COMPLETED.
5. WHEN the server maximum wait time expires before the requested stage is reached, THE call SHALL
   return without error, reporting the actual reached `stage` (per the proto, `UNSPECIFIED` relative
   to the request when the requested stage was not reached), so the client may retry.

### Requirement 3: Poll Update Consistency

**User Story:** As an SDK client, I want `PollWorkflowExecutionUpdate` to observe the same
lifecycle state as `UpdateWorkflowExecution`, with poll's own wait-stage rules (no
UNSPECIFIED→COMPLETED defaulting, ADMITTED allowed as a non-blocking current-stage request), so
that polling is deterministic and consistent across restart.

#### Acceptance Criteria

1. WHEN polling a known update, THE response SHALL return the current `stage`, the `outcome` if
   COMPLETED, and `update_ref` echoing `{workflow_execution, update_id}`, using the same `update_id`
   as the original update.
2. WHEN polling an unknown `update_id`, THE Edge SHALL return `NOT_FOUND`
   (matching `pollupdate/api.go @ v1.31.0` `NewNotFoundf("update %q not found")`). This is the
   single committed behaviour; there is no alternative pending-poll behaviour for unknown updates.
3. WHEN `PollWorkflowExecutionUpdateRequest.update_ref` is absent, THE Edge SHALL return
   `INVALID_ARGUMENT` (analog of `errUpdateRefNotSet`).
4. WHEN the ref's `run_id` is non-empty and malformed, THE Edge SHALL return `INVALID_ARGUMENT`;
   WHEN the referenced workflow execution does not exist, THE Edge SHALL return `NOT_FOUND`.
5. WHEN `wait_policy` is omitted, or its `lifecycle_stage` is `UNSPECIFIED` or `ADMITTED`, THE
   poll SHALL be non-blocking and return the update's current most-advanced stage immediately. THE
   poll SHALL NOT apply the update-path defaulting (UNSPECIFIED → COMPLETED) and SHALL NOT reject
   `ADMITTED`. WHEN `wait_policy.lifecycle_stage` is `ACCEPTED` or `COMPLETED`, THE poll SHALL block
   up to the server maximum wait time for that stage and, on expiry, return the actual reached stage
   without error (matching `pollupdate/api.go` → `Update.WaitLifecycleStage`,
   `service/history/workflow/update/update.go:148-238 @ v1.31.0`, where UNSPECIFIED/ADMITTED fall
   through to returning the current/ADMITTED stage and only ACCEPTED/COMPLETED block).
6. Polling SHALL NOT submit any workflow mutation command.

### Requirement 4: Stage and Outcome Fidelity

**User Story:** As an SDK and history consumer, I want stage and outcome derived from durable truth
and never to regress, so that lifecycle observation is monotonic and recoverable.

#### Acceptance Criteria

1. THE reported `stage` SHALL be monotonic on the ladder `UNSPECIFIED < ADMITTED < ACCEPTED <
   COMPLETED`: for a given `update_id`, a later observation SHALL NOT report a lower stage than an
   earlier one.
2. THE `outcome` SHALL be set at most once, when the update reaches COMPLETED, and SHALL be derived
   from the committed `WorkflowExecutionUpdateCompleted` (success) or
   `WorkflowExecutionUpdateRejected` (failure) history event — not from client-supplied values.
3. A worker rejection SHALL be represented as `stage = COMPLETED` with a failure `outcome`, not as a
   distinct stage; this preserves the stage/outcome separation.
4. ACCEPTED and COMPLETED stages and their outcomes SHALL be recoverable from committed history
   after runtime restart, so a post-restart poll returns the same `stage`/`outcome`. An
   admitted-only update MAY be reported as not found after restart, since the admitted state is
   transient registry state.

### Requirement 5: Protocol Compatibility

**User Story:** As a worker implementation, I want the existing update protocol transport to keep
working, so that the new lifecycle metadata does not break message delivery.

#### Acceptance Criteria

1. Existing accepted/completed/rejected protocol message bodies SHALL continue to translate
   unchanged.
2. New lifecycle metadata (`update_ref`, `stage`, `outcome`) SHALL be derived from the runtime
   update registry (for the transient admitted/wait state) and committed history (for
   accepted/completed/rejected), not from client-supplied guesses.
3. THE `PollWorkflowExecutionUpdate` read path SHALL NOT depend on transient broker/queue state for
   correctness of accepted/completed/rejected observations, consistent with history-as-authority
   (AGENTS.md §3).

### Requirement 6: Exact-Run Targeting

**User Story:** As an SDK client, I want an update or poll that names a specific `run_id` to target
exactly that run, so that I can address a historical or specific run rather than always the current
one.

#### Acceptance Criteria

1. WHEN `UpdateWorkflowExecutionRequest.workflow_execution.run_id` is non-empty and well-formed,
   THE Edge SHALL target that exact run when resolving the execution and SHALL NOT silently
   redirect to the current open run (matching v1.31.0, which builds the workflow key from
   `request.WorkflowExecution.RunId`, `service/history/api/updateworkflow/api.go:77-81 @ v1.31.0`).
   The current Tokeira handler hardcodes `run_id: None` (`crates/tokeira-edge/src/workflow_service.rs`),
   dropping the requested run; this SHALL be fixed to thread the request `run_id` into the
   `ExecutionRef`.
2. WHEN `PollWorkflowExecutionUpdateRequest.update_ref.workflow_execution.run_id` is non-empty and
   well-formed, THE Edge SHALL target that exact run when resolving the update.
3. WHEN `run_id` is empty on either RPC, THE Edge SHALL resolve to the current open run for the
   `(namespace, workflow_id)` pair (the existing current-run fallback). Current-run fallback applies
   only to an empty `run_id`.
4. WHEN `run_id` is non-empty and malformed, THE Edge SHALL return `INVALID_ARGUMENT`; WHEN it is
   well-formed but names a run that does not exist, THE Edge SHALL return `NOT_FOUND`. (This
   subsumes the malformed/missing-execution criteria in Requirement 3.4.)
5. WHEN `UpdateWorkflowExecutionRequest.first_execution_run_id` (field 3) is non-empty AND the
   resolved run's `first_execution_run_id` does not equal it, THE Edge/runtime SHALL return
   `NOT_FOUND`. This is the definitive v1.31.0 behaviour: `updateworkflow/api.go:109-111 @ v1.31.0`
   returns `ErrWorkflowExecutionNotFound` (a `NewNotFound`, `service/history/consts/const.go:55 @
   v1.31.0`) when the named first-execution run does not match the resolved run's chain. The
   comparison field already exists on Tokeira's `WorkflowState` (`first_execution_run_id`, authored
   onto `WorkflowExecutionStarted` and propagated across continue-as-new/retry), so this is a field
   equality check, not new chain-tracking machinery. WHEN `first_execution_run_id` is empty, no
   chain check is performed.
