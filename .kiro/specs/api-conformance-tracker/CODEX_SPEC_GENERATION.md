# Codex Instruction: Generate API Conformance Child Specs

This document instructs Codex on how to generate the remaining api-conformance child specs (2–16). Each spec follows the pattern established by `api-conformance-activity-by-id` after five rounds of review.

## Reference Materials

- **Tracker:** `.kiro/specs/api-conformance-tracker/tracker.md` — lists all 16 specs with their RPCs and gaps
- **Audit:** `.kiro/specs/api-conformance-tracker/reference/temporal_api_audit.md` — authoritative RPC-level status
- **Pattern spec:** `.kiro/specs/api-conformance-activity-by-id/` — the template (requirements.md, design.md, tasks.md)
- **Proto definitions:** `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` and `service.proto`
- **Edge handlers:** `crates/tokeira-edge/src/grpc/workflow_service.rs`
- **Edge DTOs:** `crates/tokeira-edge/src/translate/mod.rs`
- **Edge errors:** `crates/tokeira-edge/src/errors.rs` and `crates/tokeira-edge/src/grpc/errors.rs`
- **Runtime adapter:** `crates/tokeira-edge/src/grpc/runtime_adapter.rs`
- **Kernel state:** `crates/tokeira-kernel/src/state.rs`
- **Kernel commands:** `crates/tokeira-kernel/src/command.rs`
- **Unsupported fields:** `crates/tokeira-edge/UNSUPPORTED_FIELDS.md`
- **AGENTS.md:** workspace root — coding standards and invariants

## Spec Directory Structure

Each spec lives at `.kiro/specs/api-conformance-{name}/` with:
```
.config.kiro        — {"specId": "<uuid>", "workflowType": "requirements-first", "specType": "feature"}
requirements.md     — EARS-format requirements
design.md           — architecture, components, correctness properties, error handling, testing
tasks.md            — implementation plan with dependency graph
```

## Generation Process (per spec)

### Step 1: Read the actual code

Before writing anything, read:
1. The proto definition for each RPC in the spec's scope
2. The current handler stub in `workflow_service.rs` (find the line from the audit)
3. The existing edge DTO in `translate/mod.rs` (if one exists)
4. The `UNSUPPORTED_FIELDS.md` entry (if applicable)
5. Any existing runtime/kernel support for the feature

This grounds the spec in reality rather than assumptions.

### Step 2: Write requirements.md

Follow this structure:

```markdown
# Requirements Document

## Introduction
One paragraph: what RPCs this spec implements, current state (Stubbed/Partial), target state (Implemented).
Reference the api-conformance-tracker umbrella.

## Glossary
Define domain terms specific to this spec's scope.

## Requirements

### Requirement N: <Title>
**User Story:** As a [role], I want [feature], so that [benefit].
#### Acceptance Criteria
1. EARS-format criteria (WHEN/IF/THE/SHALL)
```

**Quality rules for requirements:**
- Every proto field that the RPC accepts MUST be accounted for (either threaded or explicitly marked unsupported with expected error)
- Every error condition MUST have a criterion (not found, invalid argument, failed precondition, unimplemented for unsupported variants)
- If the proto has a oneof with multiple variants, each variant needs a criterion (implement or return UNIMPLEMENTED)
- Non-empty string fields that need parsing (run_id, namespace) must specify validation behaviour
- Identity propagation must be specified if the proto carries an `identity` field

### Step 3: Write design.md

Follow this structure:

```markdown
# Design Document: <Title>

## Overview
What this implements and the approach.

## Architecture
Mermaid diagram showing the handler flow. Keep it simple — show the edge → runtime → kernel path.

## Components and Interfaces
- New/modified edge methods
- New/modified DTOs (reference existing ones by file path if they exist)
- New/modified runtime methods (distinguish concrete runtime vs adapter layer)
- New/modified kernel commands (only if needed)
- Error variants (with gRPC status mapping)

## Data Models
Proto message shapes, DTO shapes, any new storage types.

## Correctness Properties
### Property N: <Name>
<Description of the universal property>
**Validates: Requirements X.Y**

## Error Handling
Table: condition → error → gRPC status

## Testing Strategy
- Unit tests (example-based)
- Property tests (proptest, required not optional)
- Integration tests
```

**Quality rules for design:**
- NEVER reference `current_shard_epoch_for_run` from the edge — epoch is only accessible via runtime
- NEVER propose new kernel types if they already exist (check `command.rs` first)
- NEVER use `EdgeError::Internal` for user-facing errors — use specific variants
- ALWAYS distinguish the concrete runtime API (`CommitResult`) from the edge adapter API (`WorkflowMutationOutcome`)
- ALWAYS use free translation functions (not `TryFrom`) — match existing pattern in `translate.rs`
- ALWAYS check if the existing edge DTO already covers the RPC before defining a new one
- If a field is in `UNSUPPORTED_FIELDS.md`, the spec must either implement it or add a criterion saying it returns UNIMPLEMENTED/is ignored with a TODO

### Step 4: Write tasks.md

Follow this structure:

```markdown
# Implementation Plan: <Title>

## Overview
One paragraph summary.

## Tasks
- [ ] 1. <Top-level task>
  - [ ] 1.1 <Sub-task with specific file paths and changes>
    - Bullet points describing the change
    - _Requirements: X.Y_
  - [ ] 1.2 <Property test sub-task> (REQUIRED, not optional)
    - **Property N: <Name>**
    - **Validates: Requirements X.Y**

## Task Dependency Graph
```json
{ "waves": [...] }
```
```

**Quality rules for tasks:**
- Property tests are REQUIRED, not optional — remove all `*` markers
- Every task must reference specific file paths
- Every task must cite requirement numbers
- Include checkpoints after major milestones
- Include gRPC error mapping in `grpc/errors.rs` explicitly
- Include metrics mapping verification explicitly
- Follow the existing free-function translation pattern

## Lessons from spec 1 (mistakes to avoid)

1. **Don't assume edge can access runtime internals.** The edge talks to the runtime through `RuntimeAdapter` / `WorkflowRuntimeApi`. If you need state that's private to the runtime (shard epoch, run state), add an adapter method.

2. **Don't invent new kernel types.** Check `crates/tokeira-kernel/src/command.rs` and `state.rs` first. The kernel likely already has the command you need.

3. **Don't use `EdgeError::Internal` for expected errors.** Add specific variants (`ActivityNotFound`, `ActivityNotStarted`, etc.) that map to appropriate gRPC statuses.

4. **Don't omit proto fields.** Read the actual proto definition. If a field exists in the request, the spec must account for it (thread it, or explicitly say it's unsupported with the expected behaviour).

5. **Don't mark property tests as optional.** These are externally-visible correctness contracts.

6. **Don't conflate "before runtime delegation" with "before mutation."** Resolution calls to the runtime are reads; the guarantee is no mutation happens if validation fails.

7. **Don't assume `TryFrom` for proto translation.** The codebase uses free functions like `respond_activity_completed_to_edge`. Follow that pattern.

8. **Don't forget the gRPC error mapping file.** New `EdgeError` variants need entries in both `errors.rs` (status_code, action_name) AND `grpc/errors.rs` (tonic Status mapping).

## Spec List (generate in this order)

| # | Spec name | Directory |
|---|-----------|-----------|
| 2 | api-conformance-workflow-describe | `.kiro/specs/api-conformance-workflow-describe/` |
| 3 | api-conformance-start-fields | `.kiro/specs/api-conformance-start-fields/` |
| 4 | api-conformance-wft-completion | `.kiro/specs/api-conformance-wft-completion/` |
| 5 | api-conformance-activity-events | `.kiro/specs/api-conformance-activity-events/` |
| 6 | api-conformance-update-lifecycle | `.kiro/specs/api-conformance-update-lifecycle/` |
| 7 | api-conformance-signal-headers | `.kiro/specs/api-conformance-signal-headers/` |
| 8 | api-conformance-schedule-fields | `.kiro/specs/api-conformance-schedule-fields/` |
| 9 | api-conformance-namespace-full | `.kiro/specs/api-conformance-namespace-full/` |
| 10 | api-conformance-visibility-legacy | `.kiro/specs/api-conformance-visibility-legacy/` |
| 11 | api-conformance-nexus-admin | `.kiro/specs/api-conformance-nexus-admin/` |
| 12 | api-conformance-remote-cluster | `.kiro/specs/api-conformance-remote-cluster/` |
| 13 | api-conformance-multi-operation | `.kiro/specs/api-conformance-multi-operation/` |
| 14 | api-conformance-task-queue | `.kiro/specs/api-conformance-task-queue/` |
| 15 | api-conformance-batch-fields | `.kiro/specs/api-conformance-batch-fields/` |
| 16 | api-conformance-workflow-options | `.kiro/specs/api-conformance-workflow-options/` |

## Execution

Generate one spec at a time. For each:
1. Read the relevant code (proto, handler, DTO, kernel)
2. Write requirements.md
3. Write design.md
4. Write tasks.md
5. Write .config.kiro
6. Move to the next spec

Do NOT batch-generate without reading code first. Each spec must be grounded in the actual implementation state.
