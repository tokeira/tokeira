# Requirements Document

## Introduction

This spec completes field-level conformance for schedule RPCs: create, describe, update, patch, list matching times, delete, list, and count. The current implementation is Partial and `UNSUPPORTED_FIELDS.md` identifies schedule transport gaps: timezone data, original calendar/cron round-trip, action headers, user metadata, and versioning override.

## Glossary

- **Schedule transport:** The upstream schedule request/response proto model.
- **Structured calendar:** Tokeira's normalized schedule representation after parsing calendar and cron inputs.
- **Schedule action metadata:** Headers, user metadata, and versioning override applied when a schedule starts a workflow.

## Target State

`Implemented`. Schedule lifecycle RPCs preserve authored schedule fields,
timezone data, action headers, user metadata, and versioning override through
storage, describe/list responses, and scheduled workflow start firing.

## Evidence From Current Code

- Proto messages inspected: `CreateScheduleRequest`, `DescribeScheduleResponse`, `UpdateScheduleRequest`, `PatchScheduleRequest`, `ListScheduleMatchingTimesRequest`, `ListSchedulesRequest`, `CountSchedulesRequest`.
- Current handlers: schedule methods in `crates/tokeira-edge/src/grpc/workflow_service.rs`.
- Existing translation/runtime: `crates/tokeira-edge/src/translate/schedule.rs`, `crates/tokeira-runtime/src/schedule.rs`.
- Unsupported-field entry: `Schedule Transport` in `UNSUPPORTED_FIELDS.md`.

## Schedule Field Policy

| Field group | Current state | Target policy | Error if invalid | Storage/firing impact |
|---|---|---|---|---|
| Schedule id, namespace, action, policies | Partial | Preserve existing behavior | validation errors | Schedule state |
| `ScheduleSpec.timezone_data` | Dropped | Preserve authored bytes and return them in describe/list | validation errors | Schedule state |
| Original calendar/cron strings | Normalized | Store authored form plus normalized form | n/a | Describe/list round trip |
| `NewWorkflowExecutionInfo.header` | Not supported | Thread into scheduled workflow start | validation errors | Scheduled start history |
| `NewWorkflowExecutionInfo.user_metadata` | Not supported | Thread into scheduled workflow start | validation errors | Start history/describe |
| `NewWorkflowExecutionInfo.versioning_override` | Not supported | Thread into scheduled workflow start versioning override | validation errors | Worker versioning/routing |

## Requirements

### Requirement 1: Schedule Spec Round Trip

**User Story:** As an operator, I want schedule describe/list responses to preserve authored schedule fields, so that editing a schedule does not lose user intent.

#### Acceptance Criteria

1. WHEN a schedule is created with `timezone_data`, THE system SHALL preserve the authored bytes in schedule state and return them from describe/list responses.
2. WHEN a schedule is created with calendar or cron strings, THE describe/list response SHALL round-trip the authored representation or explicitly document normalized output.
3. WHEN structured calendar fields are stored, THE response SHALL remain semantically equivalent to the input.
4. Patch and update SHALL preserve fields that are not selected for mutation.

### Requirement 2: Schedule Action Metadata

**User Story:** As an SDK user, I want scheduled workflow starts to carry the same metadata as direct starts, so that scheduled executions behave consistently.

#### Acceptance Criteria

1. WHEN `NewWorkflowExecutionInfo.header` is supplied, THE schedule action SHALL store it and pass it to the internal workflow start when the schedule fires.
2. WHEN `user_metadata` is supplied, THE schedule action SHALL store it and pass it to the internal workflow start when the schedule fires.
3. WHEN `versioning_override` is supplied, THE schedule action SHALL store it and pass it to the internal workflow start's versioning field when the schedule fires.
4. Header, user metadata, and versioning override SHALL survive schedule update, patch, list, describe, and process restart.

### Requirement 3: Existing Schedule RPC Semantics

**User Story:** As an operator, I want current schedule lifecycle behavior preserved while fields become complete.

#### Acceptance Criteria

1. Create SHALL reject duplicate schedule ids.
2. Describe/Delete/Patch/Update SHALL return `NOT_FOUND` for missing schedules.
3. List SHALL honor namespace and pagination behavior already implemented.
4. Count SHALL count schedules in the requested namespace.
5. List matching times SHALL use the same schedule spec as describe/update.
