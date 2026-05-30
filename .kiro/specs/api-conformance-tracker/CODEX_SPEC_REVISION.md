# Codex Instruction: Revise API Conformance Specs

## Problem

Several generated specs return `UNIMPLEMENTED` for fields or variants they are supposed to implement. The tracker's purpose is to close the API gap — every RPC must move to Implemented. If implementing a field requires new kernel commands, new storage, or new runtime methods, the spec must include that work.

## Principle

**If a spec's RPC list says "move to Implemented", the spec MUST implement the RPC — including all proto fields.** Returning `UNIMPLEMENTED` is the current state — it's what we're fixing, not what we're shipping.

The only acceptable uses of `UNIMPLEMENTED` in these specs:
1. Proto oneof variants that genuinely require a separate feature not in this tracker (e.g., `ActivityTarget::Type` in UpdateActivityOptions)
2. Fields that are semantically impossible in single-cluster mode (e.g., replication config) — but these should use `INVALID_ARGUMENT` with "single-cluster mode does not support X", not `UNIMPLEMENTED`

## Specs Requiring Revision

### Spec 7: `api-conformance-signal-headers`

**Problem:** Returns `UNIMPLEMENTED` for `links` field.

**Fix:** Implement link threading. Links are metadata attached to signals — they need:
- A `links` field on the internal `SignalRequest` DTO
- Threading through the kernel into the `WorkflowExecutionSignaled` history event
- Storage in the history event attributes
- Return in `GetWorkflowExecutionHistory`

This is the same pattern as threading `header` — just a different field. The spec already implements headers; extend the same approach to links. Remove the "reject non-empty links with UNIMPLEMENTED" criterion and replace with "thread links into signal history event."

### Spec 8: `api-conformance-schedule-fields`

**Problem:** Returns `UNIMPLEMENTED` for `timezone_data`, `header`, `user_metadata`, and `versioning_override`.

**Fix for each:**
- `timezone_data` → Implement. Store the authored timezone bytes in schedule state. Return them in DescribeSchedule. Use them for time computation if the schedule engine supports it, otherwise store-and-return (round-trip fidelity).
- `header` → Implement. Thread into the `NewWorkflowExecutionInfo` that the schedule fires. When the schedule triggers a workflow start, the header is passed to `StartWorkflowExecution` internally.
- `user_metadata` → Implement. Same as header — thread into the scheduled workflow start.
- `versioning_override` → Implement. Thread into the scheduled workflow start's versioning field. The start-fields spec (3) implements the kernel support for versioning override; this spec just threads it through the schedule path.

Remove all "UNIMPLEMENTED until X" language. These fields are stored and threaded — that's the implementation.

### Spec 9: `api-conformance-namespace-full`

**Problem:** Returns `UNIMPLEMENTED` for "global namespace / replication config" fields.

**Fix:** Tokeira is single-cluster. The correct behaviour is:
- If `replication_config` specifies clusters beyond the local cluster → return `INVALID_ARGUMENT` with message "Tokeira operates in single-cluster mode; multi-cluster replication is not supported"
- If `replication_config` is absent or specifies only the local cluster → accept (no-op for replication, just store the namespace config)
- If `is_global_namespace` is true → return `INVALID_ARGUMENT` with "global namespaces require multi-cluster replication"

This is NOT `UNIMPLEMENTED` — it's a validated rejection of an unsupported configuration. The RPC itself works; it just doesn't support multi-cluster mode. Change the spec from "return UNIMPLEMENTED" to "return INVALID_ARGUMENT for multi-cluster configurations."

For other namespace config fields (retention, archival config, etc.) — implement them. Store in namespace registry, return in DescribeNamespace.

### Spec 14: `api-conformance-task-queue`

**Problem:** Returns `UNIMPLEMENTED` for "unsupported task queue kinds."

**Fix:** Temporal has `TASK_QUEUE_KIND_NORMAL` and `TASK_QUEUE_KIND_STICKY`. Tokeira supports both (sticky task queues exist). If there are other kinds in the proto enum, return `INVALID_ARGUMENT` (not `UNIMPLEMENTED`) for unknown enum values. Change the criterion from "return UNIMPLEMENTED" to "return INVALID_ARGUMENT for unrecognized task queue kind enum values."

### Spec 15: `api-conformance-batch-fields`

**Problem:** Returns `UNIMPLEMENTED` for reset reapply fields, update workflow options, and signal headers — deferring to other specs.

**Fix:** This spec has legitimate cross-spec dependencies. The correct approach:
- `BatchOperationSignal.header` → This spec MUST implement it. The signal-headers spec (7) implements the signal path; this spec threads headers through the batch signal dispatch path. If spec 7 hasn't landed yet, implement both together or order spec 7 before spec 15.
- `BatchOperationUpdateWorkflowExecutionOptions` → This spec MUST implement it. The workflow-options spec (16) implements the single-workflow path; this spec applies it in batch. Same ordering: spec 16 before spec 15, or implement together.
- Reset reapply fields → Implement. The kernel already has reset; the batch path needs to thread the reapply configuration through to the kernel's reset command.

Change all "return UNIMPLEMENTED before creating the batch" criteria to implementation criteria. If there's a dependency on another spec, note the ordering requirement but still spec the implementation.

## Execution Order

Given the dependencies:
1. Spec 7 (signal-headers with links) — no dependencies
2. Spec 8 (schedule-fields) — depends on spec 3 (start-fields) for versioning override kernel support
3. Spec 9 (namespace-full) — no dependencies
4. Spec 14 (task-queue) — no dependencies
5. Spec 15 (batch-fields) — depends on specs 7, 16

Revise specs 7, 9, 14 first (independent). Then 8 (after 3). Then 15 (after 7 and 16).

## Do NOT

- Return `UNIMPLEMENTED` for a proto field the spec claims to implement
- Use `UNIMPLEMENTED` where `INVALID_ARGUMENT` is the correct status (invalid config, unknown enum)
- Defer field implementation to "future specs" when the field is in this spec's scope
- Leave "UNIMPLEMENTED until X lands" language — either implement X in this spec or note the ordering dependency and still write the implementation criteria
