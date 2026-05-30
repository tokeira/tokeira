# Design Document: Schedule Field API Conformance

## Overview

Schedules exist but some transport fields are normalized or dropped. This design introduces authored-field preservation for timezone data and schedule action metadata, then threads action metadata into the workflow start fired by the schedule engine.

## Dependencies and Non-Goals

- Depends on `api-conformance-start-fields` for scheduled workflow start metadata policy.
- Depends on `api-conformance-signal-headers` for shared header/link handling.
- Depends on `api-conformance-start-fields` versioning override support; this spec threads the schedule-authored value into that start path.
- Accepted metadata must not fail later during firing.

## Authored Versus Normalized Storage

Schedule matching can use normalized structured calendar data, but describe/list
must retain enough authored data to avoid surprising edits. Store both authored
calendar/cron/timezone data and normalized runtime form when inputs are accepted.

## Architecture

```mermaid
flowchart LR
    Client --> Grpc["Schedule RPCs"]
    Grpc --> Translate["schedule translation"]
    Translate --> Store["ScheduleStore"]
    Store --> Engine["Schedule engine"]
    Engine --> Runtime["StartWorkflow"]
```

## Components and Interfaces

- `crates/tokeira-edge/src/translate/schedule.rs`: preserve schedule proto fields.
- `crates/tokeira-runtime/src/schedule.rs`: store authored schedule representation if round-trip requires it.
- `crates/tokeira-edge/src/grpc/workflow_service.rs`: maintain existing lifecycle handlers and error mapping.
- `crates/tokeira-edge/src/translate/to_internal.rs`: reuse start field policy for schedule actions.

## Correctness Properties

### Property 1: Schedule Round Trip

For any accepted schedule spec, create followed by describe/list returns an equivalent or documented normalized spec.

**Validates:** Requirements 1.1, 1.2, 1.3.

### Property 2: Metadata Firing Fidelity

Schedule action header, user metadata, and versioning override stored at create/update are passed unchanged to the internal workflow start when the schedule fires.

**Validates:** Requirements 2.1, 2.2, 2.3, 2.4.

### Property 3: Lifecycle Preservation

Field conformance changes do not break duplicate, not-found, pagination, count, or matching-time behavior.

**Validates:** Requirements 3.1-3.5.

## Error Handling

| Condition | Error | gRPC status |
|---|---|---|
| Missing schedule | schedule not-found error | `NOT_FOUND` |
| Duplicate schedule | already exists | `ALREADY_EXISTS` |
| Invalid spec | bad request/proto conversion | `INVALID_ARGUMENT` |

## Testing Strategy

- Golden tests for create-describe-list round trips.
- Property tests for patch/update preserving unselected fields.
- Tests that action metadata survives persistence and is passed to start on firing.
- Existing schedule lifecycle regression tests remain required.
