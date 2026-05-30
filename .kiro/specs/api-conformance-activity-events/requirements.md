# Requirements Document

## Introduction

This spec completes activity poll and heartbeat field conformance for `PollActivityTaskQueue` and `RecordActivityTaskHeartbeat`. The current implementation is Partial and `UNSUPPORTED_FIELDS.md` lists missing poll response fields: heartbeat details, scheduled time, current attempt scheduled time, and started time.

## Glossary

- **Activity dispatch snapshot:** Runtime data used to build a `PollActivityTaskQueueResponse`.
- **Heartbeat details:** Worker-supplied payloads from heartbeat calls, returned to future activity attempts.
- **Attempt timing:** Scheduled and started timestamps for the current activity attempt.

## Target State

`ImplementedSubset`. The spec completes poll response fields backed by durable
activity state and preserves heartbeat details. Unknown legacy timestamps remain
default and are not fabricated.

## Evidence From Current Code

- Proto messages inspected: `PollActivityTaskQueueResponse`, `RecordActivityTaskHeartbeatRequest`.
- Current handlers: `poll_activity_task_queue`, `record_activity_task_heartbeat`.
- Unsupported-field entries: `PollActivityTaskQueueResponse` and activity history event notes in `UNSUPPORTED_FIELDS.md`.
- Runtime/kernel: `StartedActivityTask`, activity broker, activity tracking, `ActivityState`.

## Poll Response Field Policy

| Response field | Current state | Target policy | Source | Tests |
|---|---|---|---|---|
| token/execution/id/type/input/header/attempt | Supported | Preserve | Dispatch snapshot | Regression |
| `heartbeat_details` | Not populated | Populate latest persisted heartbeat details | Activity tracking/storage | Restart |
| `scheduled_time` | Not populated | Populate when schedule event time is known | History/activity state | Property |
| `current_attempt_scheduled_time` | Not populated | Populate from current attempt state | Activity retry state | Property |
| `started_time` | Not populated | Populate server-authored poll/start time | Activity start state | Property |

## Heartbeat Storage Policy

Heartbeat details must survive runtime restart for retry/resume semantics. Store
them in the same durable activity tracking/state path used to reconstruct
dispatchable activity attempts.

## Requirements

### Requirement 1: PollActivityTaskQueue Response Completeness

**User Story:** As an activity worker, I want poll responses to include Temporal-compatible timing and heartbeat fields, so that retries and resumed activities behave correctly.

#### Acceptance Criteria

1. WHEN an activity task is dispatched, THE response SHALL include task token, workflow execution, activity id, type, input, headers, attempt, and task queue data as today.
2. WHEN heartbeat details exist for the activity, THE response SHALL populate `heartbeat_details`.
3. WHEN the activity scheduled event timestamp is known, THE response SHALL populate `scheduled_time`.
4. WHEN the current attempt scheduled timestamp is known, THE response SHALL populate `current_attempt_scheduled_time`.
5. WHEN the task is started by polling, THE response SHALL populate `started_time` with the server-authored start time.
6. THE response SHALL NOT invent timestamps for old state that lacks them.

### Requirement 2: Heartbeat Details Preservation

**User Story:** As an activity worker, I want heartbeat details accepted and persisted, so that retries can resume from last progress.

#### Acceptance Criteria

1. WHEN `RecordActivityTaskHeartbeat` includes `details`, THE runtime SHALL persist them in activity tracking state.
2. WHEN the same activity is retried or repolled according to Temporal semantics, THE latest heartbeat details SHALL be returned in poll response.
3. WHEN heartbeat token validation fails, THE Edge SHALL return `INVALID_ARGUMENT` or the existing token validation error.
4. WHEN cancellation is requested, THE heartbeat response SHALL preserve the existing `cancel_requested` behavior.

### Requirement 3: History Linkage

**User Story:** As an SDK and history consumer, I want activity event link fields to be populated, so that history replay can link scheduled, started, and terminal activity events.

#### Acceptance Criteria

1. WHEN activity scheduled/start event ids are known, THE serializer SHALL populate corresponding proto fields on activity events.
2. WHEN older state lacks event ids, THE serializer SHALL leave fields default and SHALL NOT invent values.
3. Kernel state changes SHALL remain deterministic and I/O-free.
