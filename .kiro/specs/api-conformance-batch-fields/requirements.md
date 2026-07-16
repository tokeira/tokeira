# Requirements Document

## Introduction

This spec completes field-level conformance for batch operation RPCs: start, stop, describe, and list. Current handlers are Partial and `UNSUPPORTED_FIELDS.md` lists unsupported batch fields for signal headers, workflow execution option updates, and reset reapply options.

## Glossary

- **Batch operation:** A server-managed operation over a visibility-selected set of workflow executions.
- **Batch action:** Signal, cancel, terminate, delete, reset, or update-options action applied to each target.
- **Batch progress:** Counts and state exposed through describe/list.
- **Batch activity-options action:** `BatchOperationUpdateActivityOptions`, which applies one
  field-mask-controlled activity-options mutation to every visibility-selected workflow and every
  matching pending activity within each workflow.

## Target State

`Implemented`. Batch actions preserve signal headers, workflow execution option
updates, reset reapply configuration, and lifecycle metadata. Dependencies on
signal and workflow option semantics are ordering requirements, not runtime
escape hatches.

## Evidence From Current Code

- Proto messages inspected: `StartBatchOperationRequest`, `StopBatchOperationRequest`, `DescribeBatchOperationResponse`, `ListBatchOperationsResponse`.
- Current handlers: batch methods in `workflow_service.rs`.
- Unsupported-field entry: `Batch Operations` in `UNSUPPORTED_FIELDS.md`.
- Runtime/store: `BatchOperationStore`, batch dispatcher, visibility paging.
- Target activity-options behavior: `service/history/api/updateactivityoptions/api.go @ v1.31.0`;
  batch wire shape: `proto/upstream/temporal/api/batch/v1/message.proto`.

## Batch Action Field Policy

| Action/field | Current state | Target policy | Dependency |
|---|---|---|---|
| Signal basic fields | Partial | Preserve | Existing signal path |
| `BatchOperationSignal.header` | Dropped | Thread through batch signal dispatch | `api-conformance-signal-headers` |
| Cancel/terminate/delete | Partial | Preserve supported fields | Existing runtime paths |
| Reset basic fields | Partial | Preserve supported reset only | Reset conformance |
| Reset reapply/current-run-only/exclude fields | Not supported | Thread to kernel reset command | Reset command support |
| Update workflow execution options | Not supported | Apply per target using workflow-options runtime path | `api-conformance-workflow-options` |
| Update activity options | Not supported | Preserve selector, options, mask, restore flag, and apply the single-workflow activity-options path to every selected workflow | `api-conformance-activity-by-id` |

### `BatchOperationUpdateActivityOptions` Field Policy

| Field | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `identity` | Preserve on every per-workflow mutation | None | Caller attribution |
| `activity.type` | Update each matching pending activity in each target workflow | Per-target activity-not-found contributes a batch failure | Selects matching activities |
| `activity.match_all` | Update every pending activity in each target workflow | `INVALID_ARGUMENT` when false/absent as a selector | Selects all pending activities |
| `activity_options` | Preserve the full activity-options message | `INVALID_ARGUMENT` for malformed selected fields | Updates selected options |
| `update_mask` | Apply only selected fields | `INVALID_ARGUMENT` for unsupported paths | Controls mutation fields |
| `restore_original` | Restore options from each activity's first schedule event | `INVALID_ARGUMENT` when combined with replacements | Restores selected activities |

## Requirements

### Requirement 1: Batch Action Field Accounting

**User Story:** As an operator, I want every batch action field handled explicitly, so that batch operations do not silently drop request semantics.

#### Acceptance Criteria

1. WHEN `BatchOperationSignal.header` is present, THE batch signal path SHALL thread it to `SignalRequest` for every target workflow.
2. WHEN `BatchOperationUpdateWorkflowExecutionOptions` is present, THE batch dispatcher SHALL apply the update options runtime path to every target workflow.
3. WHEN reset reapply fields are present, THE batch reset path SHALL thread reapply configuration to the kernel reset command.
4. Batch fields SHALL never be silently dropped.
5. WHEN `BatchOperationUpdateActivityOptions` is present, THE batch dispatcher SHALL preserve its
   identity, selector, activity options, update mask, and restore-original flag for every target
   workflow.

### Requirement 2: Lifecycle Response Completeness

**User Story:** As an operator, I want batch describe/list responses to reflect current progress and config, so that batch operations are observable.

#### Acceptance Criteria

1. `StartBatchOperation` SHALL persist all supported request fields in batch state.
2. `DescribeBatchOperation` SHALL return state, reason, progress counts, and original request metadata.
3. `ListBatchOperations` SHALL paginate stable summaries.
4. `StopBatchOperation` SHALL be idempotent for already terminal operations.

### Requirement 3: Safety

**User Story:** As an operator, I want invalid batch operations rejected before they enqueue work, so that broad mutations are safe.

#### Acceptance Criteria

1. Invalid visibility queries SHALL return `INVALID_ARGUMENT`.
2. Invalid action payloads SHALL return `INVALID_ARGUMENT` before batch state is created.
3. Batch dispatch SHALL preserve the existing review/confirm model where applicable.
