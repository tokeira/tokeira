# Requirements Document

## Introduction

This spec completes `SignalWorkflowExecution` field conformance. The current handler is Partial and `UNSUPPORTED_FIELDS.md` documents that `header` and `links` are not threaded.

## Glossary

- **Signal header:** SDK-authored metadata attached to a signal request.
- **Signal links:** Upstream link metadata used to correlate signals with external entities.
- **Signal event:** The durable history event that records the signal.

## Target State

`Implemented`. Headers and links are preserved byte-for-byte on the internal
signal request, durable signal history event, and serialized history response.

## Evidence From Current Code

- Proto message inspected: `SignalWorkflowExecutionRequest`.
- Current handler: `signal_workflow_execution`.
- Unsupported-field entry: `SignalWorkflowExecutionRequest` in `UNSUPPORTED_FIELDS.md`.
- Translation helpers: existing header/payload conversion helpers in `tokeira_proto::conversions::common`.
- Kernel/history target: `SignalRequest` and workflow signaled history event.

## Signal Field Policy

| Proto field | Current state | Target policy | Error if invalid | Persistence/history impact |
|---|---|---|---|---|
| `namespace`, `workflow_execution`, `signal_name`, `input`, `identity`, `request_id` | Supported | Preserve | existing validation errors | Signal command/history |
| `header` | Not threaded | Preserve byte-for-byte | `INVALID_ARGUMENT` on conversion failure | Signal event attributes |
| `links` | Not threaded | Preserve in signal history | `INVALID_ARGUMENT` on conversion failure | Signal event attributes |

## Requirements

### Requirement 1: Signal Header Preservation

**User Story:** As an SDK client, I want signal headers preserved in history, so that workflow code receives the same metadata it sent.

#### Acceptance Criteria

1. WHEN `SignalWorkflowExecutionRequest.header` is present, THE Edge SHALL translate it into the internal signal request.
2. WHEN the signal commits, THE kernel SHALL persist the header on the signal history event.
3. WHEN history is serialized, THE signal event proto SHALL include the header.
4. IF header payload conversion fails, THE Edge SHALL return `INVALID_ARGUMENT`.

### Requirement 2: Signal Links Handling

**User Story:** As an SDK client, I want signal links handled explicitly, so that link metadata is not silently dropped.

#### Acceptance Criteria

1. WHEN `links` are present, THE Edge SHALL translate them into the internal signal request.
2. WHEN the signal commits, THE kernel SHALL persist links on the signal history event.
3. WHEN history is serialized, THE signal event proto SHALL include the links.
4. IF link conversion fails, THE Edge SHALL return `INVALID_ARGUMENT`.

### Requirement 3: Existing Signal Semantics

**User Story:** As a workflow author, I want adding headers to preserve current signal behavior, so that existing signal delivery remains correct.

#### Acceptance Criteria

1. WHEN the target execution exists, THE signal SHALL still append a durable transition.
2. WHEN the target execution is missing, THE Edge SHALL return `NOT_FOUND`.
3. WHEN `run_id` is non-empty and malformed, THE Edge SHALL return `INVALID_ARGUMENT`.
4. Signal-with-start signal attributes SHALL use the same header/link policy when signaling an existing run.
