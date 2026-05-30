# Design Document: Activity Events API Conformance

## Overview

Activity poll and heartbeat conformance requires carrying heartbeat payloads and attempt timestamps through runtime tracking and storage-derived dispatch entries. The edge remains a translator; activity state and token validation stay in runtime/kernel.

## Dependencies and Non-Goals

- `api-conformance-workflow-describe` consumes the same pending activity snapshot.
- This spec does not implement by-id activity handlers; those are covered by `api-conformance-activity-by-id`.
- Timestamp fields are authored by committed history/runtime state, not by edge wall-clock guesses.

## Architecture

```mermaid
flowchart LR
    Worker --> Heartbeat["RecordActivityTaskHeartbeat"]
    Heartbeat --> Runtime["activity_tracking"]
    Runtime --> Store["Run/activity tracking state"]
    Worker --> Poll["PollActivityTaskQueue"]
    Poll --> Broker["Activity broker"]
    Broker --> Response["PollActivityTaskQueueResponse"]
```

## Components and Interfaces

- `crates/tokeira-edge/src/grpc/translate.rs`: preserve heartbeat `details` and project poll response fields.
- `crates/tokeira-runtime/src/runtime/activity.rs`: persist heartbeat details and authored started time.
- `crates/tokeira-runtime/src/heartbeat.rs` or activity tracking module: extend tracking entries with latest details.
- `crates/tokeira-storage/src/memory.rs` and DSQL dispatch reads: expose scheduled/start timing when known.
- `crates/tokeira-edge/src/translate/history_serializer.rs`: populate activity event linkage fields when present.

## Correctness Properties

### Property 1: Heartbeat Round Trip

For any heartbeat details payload, a later eligible activity poll returns the latest details.

**Validates:** Requirements 1.2, 2.1, 2.2.

### Property 2: Timing Authorship

Scheduled/current-attempt/started timestamps in poll responses equal server-authored state and are absent when unknown.

**Validates:** Requirements 1.3, 1.4, 1.5, 1.6.

### Property 3: Event Link Fidelity

When activity scheduled/start ids exist, terminal activity event protos preserve them exactly.

**Validates:** Requirements 3.1, 3.2.

## Error Handling

| Condition | Error | gRPC status |
|---|---|---|
| Malformed heartbeat token | proto conversion error | `INVALID_ARGUMENT` |
| Stale activity token | runtime validation error | existing mapped status |
| Missing activity | runtime validation error | existing mapped status |

## Testing Strategy

- Unit tests for heartbeat translator preserving details.
- Runtime tests for heartbeat details persistence and retry poll response.
- Serializer property tests for activity event id linkage.
- DSQL/memory parity tests for scheduled/start timestamps where both stores expose dispatch state.
