# Requirements Document

## Introduction

This spec completes task queue conformance for `DescribeTaskQueue` and implements `ListTaskQueuePartitions`, currently stubbed. Existing `DescribeTaskQueue` is Partial: pollers are listed, but worker reachability, backlog detail, and build-id completeness are not fully proven.

## Glossary

- **Task queue partition:** Temporal partition metadata for normal and sticky task queues.
- **Reachability:** Whether workers/build ids can receive tasks for a queue.
- **Backlog detail:** Count, age, and rate information about queued workflow/activity tasks.

## Target State

`Implemented`. Single-partition topology, normal and sticky task queue kinds,
pollers, backlog diagnostics, reachability, and build-id fields are populated
from runtime diagnostics and worker/versioning stores.

## Evidence From Current Code

- Proto messages inspected: `DescribeTaskQueueRequest`, `DescribeTaskQueueResponse`, `ListTaskQueuePartitionsRequest`.
- Current handlers: `describe_task_queue`, `list_task_queue_partitions`.
- Runtime sources: broker poller registry, backlog scanner, worker registry, versioning rule store.

## Response Field Policy

| Field group | Current state | Target policy | Source |
|---|---|---|---|
| Pollers | Partial | Preserve existing active poller listing | Poller registry |
| Task queue status/backlog | Partial | Populate count/age/rate where available | Broker/backlog diagnostics |
| Build id reachability | Partial | Populate from worker/versioning stores or omit only when allowed | Worker registry/versioning |
| Partitions | Stubbed | Return one normal partition for current model | Queue config |
| Sticky partitions | Underspecified | Return only supported sticky topology | Sticky broker/config |

## Requirements

### Requirement 1: ListTaskQueuePartitions

**User Story:** As an SDK client, I want task queue partition metadata, so that worker clients can discover compatible queue topology.

#### Acceptance Criteria

1. WHEN a task queue exists, THE RPC SHALL return partition metadata compatible with Tokeira's current non-partitioned or configured partition model.
2. WHEN a namespace or task queue is invalid, THE RPC SHALL return `INVALID_ARGUMENT`.
3. IF Tokeira only supports one partition, THE response SHALL explicitly represent one normal partition and any sticky partition semantics supported.

### Requirement 2: DescribeTaskQueue Completeness

**User Story:** As an operator, I want task queue describe to include pollers, backlog, reachability, and build-id data, so that worker health is diagnosable.

#### Acceptance Criteria

1. The response SHALL preserve existing poller listing behavior.
2. The response SHALL populate backlog status fields when runtime broker/backlog data is available.
3. The response SHALL populate worker/build-id reachability using the worker registry/versioning rule store.
4. Status fields SHALL be populated from runtime diagnostics and worker/versioning stores; fields that the proto marks optional may be absent only when the runtime has no data.
5. Deprecated request fields SHALL be handled according to existing compatibility behavior and covered by tests.

### Requirement 3: Metrics and Validation

**User Story:** As an operator, I want task queue errors observable, so that invalid queue requests are easy to debug.

#### Acceptance Criteria

1. Missing namespace SHALL return `INVALID_ARGUMENT`.
2. Missing task queue SHALL return `INVALID_ARGUMENT`.
3. `TASK_QUEUE_KIND_NORMAL` and `TASK_QUEUE_KIND_STICKY` SHALL be accepted where the RPC supports them.
4. Unrecognized task queue kind enum values SHALL return `INVALID_ARGUMENT`.
