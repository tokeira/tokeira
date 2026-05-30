# Requirements Document

## Introduction

This spec completes field-level conformance for `StartWorkflowExecution` and `SignalWithStartWorkflowExecution`. Both RPCs are Partial. The fields currently documented in `crates/tokeira-edge/UNSUPPORTED_FIELDS.md` become implemented start semantics, except the server-internal continue-as-new fields that remain invalid for external callers.

## Glossary

- **Start attributes:** The upstream request fields that become `WorkflowExecutionStarted` history and initial run state.
- **Start field:** A Temporal start-request proto field that must either become durable workflow state/history or, for server-internal fields, be rejected as invalid external input.
- **Signal-with-start:** An RPC that either signals the current run or starts a new run with initial signal metadata.

## Target State

`Implemented`. All externally-authored start fields are translated, committed,
and exposed through history/describe behavior. Server-internal continue-as-new
fields remain `INVALID_ARGUMENT` for normal clients.

## Evidence From Current Code

- Proto messages inspected: `StartWorkflowExecutionRequest`, `SignalWithStartWorkflowExecutionRequest`.
- Current handlers: `start_workflow_execution`, `signal_with_start_workflow_execution`.
- Existing translation: `to_internal::start_request` and signal-with-start helpers.
- Unsupported-field entry: `StartWorkflowExecutionRequest` in `UNSUPPORTED_FIELDS.md`.
- Runtime/kernel: `StartRequest`, `SignalWithStartRequest`, `WorkflowExecutionStarted`.

## Start Field Policy

| Proto field | Current state | Target policy | Error if invalid | Persistence/history impact |
|---|---|---|---|---|
| `namespace`, `workflow_id`, `workflow_type`, `task_queue`, `input` | Supported | Preserve | `INVALID_ARGUMENT` on invalid required data | Start state/history |
| `workflow_id_reuse_policy` | Not supported | Map to kernel current-run conflict handling | `INVALID_ARGUMENT` for unknown enum | Current-run conflict handling |
| `workflow_id_conflict_policy` | Not supported | Map to kernel current-run conflict handling | `INVALID_ARGUMENT` for unknown enum | Current-run conflict handling |
| `request_eager_execution` | Supported | Preserve existing eager path | n/a | Runtime eager dispatch |
| `workflow_start_delay` | Not supported | Commit delayed-start state and durable first-WFT timer | `INVALID_ARGUMENT` for invalid duration | Delayed start state/timer |
| `completion_callbacks` | Not supported | Persist callback registrations and fire on terminal transition | validation errors for malformed callback targets | Callback state/effects |
| `user_metadata` | Not supported | Thread into start history and describe response | validation errors only | Start history/projection |
| `links` | Not supported | Thread into start history | validation errors only | Start history |
| `versioning_override` | Not supported | Persist routing override and apply to WFT dispatch | validation errors for unsupported enum values | Start state/dispatch routing |
| `cron_schedule` | Server-managed | Register durable client-authored recurring-start policy | validation errors for invalid cron | Cron state/next-fire timer |
| `continued_failure`, `last_completion_result` | Internal-only | Reject normal client values | `INVALID_ARGUMENT` | Continue-as-new/schedule internals |

## Requirements

### Requirement 1: Start Field Accounting

**User Story:** As an SDK client, I want Temporal start fields preserved and enforced, so that workflow start behavior matches SDK expectations.

#### Acceptance Criteria

1. WHEN `workflow_id_reuse_policy` or `workflow_id_conflict_policy` is supplied, THE Edge SHALL map it to kernel current-run conflict handling.
2. WHEN `request_eager_execution` is true, THE existing eager/sync-match path SHALL continue to request eager workflow task delivery.
3. WHEN `workflow_start_delay` is non-zero, THE runtime SHALL commit the workflow in a delayed-start state and SHALL NOT schedule the first workflow task until the durable delay timer fires.
4. WHEN `completion_callbacks` are supplied, THE kernel SHALL persist callback registrations and THE runtime SHALL dispatch them exactly once after a terminal workflow transition.
5. WHEN `user_metadata` is supplied, THE Edge SHALL thread it into start history and describe/projection metadata.
6. WHEN `links` are supplied, THE Edge SHALL thread them into the start history event.
7. WHEN `versioning_override` is supplied, THE kernel SHALL persist it and THE runtime SHALL apply it to workflow task routing/dispatch decisions.
8. WHEN client-supplied `cron_schedule` is supplied, THE runtime SHALL register durable recurring-start state using the timer/schedule machinery.
9. WHEN server-internal `continued_failure` or `last_completion_result` is supplied by a normal client start, THE Edge SHALL return `INVALID_ARGUMENT` unless the request is produced by an internal continue/schedule path.
10. WHEN a non-empty malformed field requires parsing, THE Edge SHALL return `INVALID_ARGUMENT`.

### Requirement 2: SignalWithStart Field Parity

**User Story:** As an SDK client using signal-with-start, I want the start portion to behave exactly like `StartWorkflowExecution`, so that the combined RPC does not drop start metadata.

#### Acceptance Criteria

1. WHEN `SignalWithStartWorkflowExecution` starts a new run, THE start attributes SHALL follow all criteria in Requirement 1.
2. WHEN it signals an existing run, THE signal attributes SHALL follow the `api-conformance-signal-headers` contract.
3. WHEN the start portion contains Temporal start fields, THE RPC SHALL apply the same translation, persistence, and validation behavior as `StartWorkflowExecution`.
4. WHEN the signal portion is invalid, THE RPC SHALL fail before starting a new run.

### Requirement 3: History and State Fidelity

**User Story:** As an operator, I want accepted start fields to be durable in history/state, so that describe/history reads can reconstruct the original request.

#### Acceptance Criteria

1. WHEN a supported field is accepted, THE kernel transition SHALL persist it in deterministic state or history.
2. THE Edge SHALL NOT store start semantics in transient queues or projection-only state.
3. THE start response SHALL preserve run id and eager task response semantics already supported by the existing handler.
4. Delayed-start, cron, callback, and versioning override state SHALL survive process restart and continue from durable state.
