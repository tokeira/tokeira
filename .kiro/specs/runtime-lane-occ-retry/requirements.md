# Requirements Document: Lane OCC Retry and Mailbox Coalescing

## Introduction

This document captures the requirements for Feature 1 of the Tokeira runtime implementation: Lane OCC Retry and Mailbox Coalescing. This is the foundational feature that all other runtime features depend on.

The current `lane.rs` implements a simple load → kernel apply → commit path with no retry on OCC conflict, no mailbox coalescing, and no dispatch op publication from the lane. The `runtime.rs` facade publishes workflow tasks after commit, but only for workflow tasks and only from the facade level.

This feature hardens the lane execution path by adding:
- An OCC retry loop so that concurrent writes to the same run resolve transparently.
- Mailbox coalescing so that bursty command floods are processed efficiently.
- Deterministic lane routing so that per-run serialization is maintained by construction.
- Dispatch op publication so that all derived effects from a committed transition are acted upon.

The authoritative specifications are [030-runtime-lanes](../../../docs/architecture/030-runtime-lanes.md), [040-delivery-broker](../../../docs/architecture/040-delivery-broker.md), and [010-history-as-authority](../../../docs/architecture/010-history-as-authority.md).

## Glossary

- **Lane**: A single-thread serial command processor hosting many run actors. Commands for a run are routed to one Lane via `hash(run_key) mod lane_count`.
- **Run_Actor**: A demand-loaded in-memory object representing one workflow run on a Lane. Loads state, drains mailbox, invokes kernel, commits, publishes effects, then parks or evicts.
- **Mailbox**: The per-run message queue on a Lane. Multiple commands for the same run can be drained in one activation cycle.
- **Mailbox_Coalescing**: Draining multiple Mailbox items for the same run before parking, subject to fairness and transaction-size bounds.
- **OCC_Conflict**: An optimistic concurrency control conflict returned by storage when `expected_seq` does not match the durable transition sequence. Indicates another writer committed first.
- **CommitResult**: The outcome of a storage commit — `Applied` (with new WorkflowState), `Conflict` (OCC failure with reason), or `Duplicate` (request already processed).
- **Kernel**: The pure state-transition engine (`tokeira-kernel`). Given a `LoadedRun` and a `Command`, the Kernel produces a `Transition` without performing I/O.
- **Transition**: The full result of one authoritative state transition, containing history events, state patch, activity ops, timer ops, dispatch ops, and projection ops.
- **DispatchOp**: A value emitted by the Kernel telling the Runtime what task delivery action must follow from a committed Transition.
- **Broker**: The in-memory delivery subsystem (`InMemoryBroker`) that matches pending workflow tasks with waiting pollers. The Broker is not authoritative — the sweeper can reconstruct its state from durable storage.
- **Runtime**: The execution shell (`tokeira-runtime`) that orchestrates command routing, Kernel invocation, storage commits, and derived-effect publication.
- **RunKey**: The durable identity of a workflow run, used for routing and storage lookups.
- **LoadedRun**: The current authoritative state of a run as loaded from storage, used as input to the Kernel.

## Requirements

---

### Requirement 1: OCC Retry Loop on Commit Conflict

**User Story:** As a Tokeira developer, I want the Lane to automatically retry on OCC conflicts, so that concurrent writes to the same run are resolved without surfacing transient failures to callers.

#### Acceptance Criteria

1. WHEN `handle_message` receives a `CommitResult::Conflict` from storage, THE Lane SHALL reload the run state from storage and recompute the Transition via the Kernel.
2. WHEN `handle_message` retries after an OCC_Conflict, THE Lane SHALL use the freshly loaded state for the retry attempt.
3. WHEN `handle_message` retries after an OCC_Conflict, THE Lane SHALL pass the same original Command to the Kernel on each retry.
4. THE Lane SHALL bound the number of OCC retry attempts to a configurable maximum (default 5).
5. IF the retry count exceeds the configured maximum, THEN THE Lane SHALL return an error indicating retry exhaustion.
6. WHEN an OCC retry succeeds, THE Lane SHALL return the successful CommitResult to the caller.
7. WHEN `handle_message` receives a `CommitResult::Applied` on the first attempt, THE Lane SHALL return the result without entering the retry path.
8. WHEN `handle_message` receives a `CommitResult::Duplicate`, THE Lane SHALL return the Duplicate result without retrying.

---

### Requirement 2: Mailbox Coalescing

**User Story:** As a Tokeira developer, I want the Lane to drain multiple Mailbox items for the same run before parking, so that bursty signal floods are processed efficiently without repeated load/park cycles.

#### Acceptance Criteria

1. WHEN a Run_Actor is activated to process a command, THE Lane SHALL check for additional pending Mailbox items targeting the same RunKey before parking.
2. WHEN additional Mailbox items exist targeting the same RunKey, THE Lane SHALL drain and process them in the same activation cycle. Messages targeting different RunKey values SHALL remain in the channel for subsequent activations.
3. THE Lane SHALL bound the number of same-run Mailbox items drained per activation to a configurable maximum (default 16) to preserve fairness across runs.
4. WHEN multiple Mailbox items are drained in one activation, THE Lane SHALL process each item sequentially (load, Kernel apply, commit) using the latest committed state.
5. WHEN a Mailbox item fails during a coalesced drain, THE Lane SHALL return the error for that item and stop draining further items for that run in the current activation.
6. WHEN the coalescing drain limit is reached and additional Mailbox items remain, THE Lane SHALL yield control to allow other runs on the same Lane to make progress.

---

### Requirement 3: Lane Message Routing

**User Story:** As a Tokeira developer, I want commands to be routed to a deterministic Lane based on run identity, so that per-run serialization is maintained by construction.

#### Acceptance Criteria

1. THE Runtime SHALL route commands to Lanes using `hash(run_key) mod lane_count`.
2. THE Runtime SHALL maintain at least one Lane at all times.
3. WHEN a command is submitted for a run, THE Runtime SHALL always route the command to the same Lane for a given lane_count.
4. WHEN two commands target different RunKey values that hash to the same Lane, THE Lane SHALL process the commands serially without interleaving their Kernel invocations.

---

### Requirement 4: Dispatch Op Publication After Commit

**User Story:** As a Tokeira developer, I want the Runtime to publish all dispatch ops from a committed Transition, so that derived effects are acted upon by the appropriate subsystem or logged for future wiring.

#### Acceptance Criteria

1. WHEN a Transition is committed successfully, THE Lane SHALL forward the Transition's `dispatch_ops` to the `DispatchPublisher`.
2. WHEN a `DispatchOp::EnqueueWorkflowTask` is present, THE `RuntimeDispatchPublisher` SHALL publish the workflow task to the Broker. This is the only variant fully wired in this feature.
3. WHEN a `DispatchOp::EnqueueActivityTask` is present, THE `RuntimeDispatchPublisher` SHALL log the op at info level. Full activity delivery wiring is deferred to Feature 2 (Activity Pump).
4. WHEN a DispatchOp for child workflows (`StartChildWorkflow`, `TerminateChild`, `CancelChild`), external signals (`SignalExternalWorkflow`), external cancels (`RequestCancelExternalWorkflow`), or Nexus operations (`ScheduleNexusOperation`, `CancelNexusOperation`) is present, THE `RuntimeDispatchPublisher` SHALL log the op at info level. Full orchestration handler wiring is deferred to Features 6, 7, and 9.
5. THE Lane SHALL publish dispatch ops only after the storage commit succeeds, not before.
6. THE Lane SHALL publish dispatch ops for every committed Transition, including Transitions committed after OCC retries.
7. WHEN the `DispatchPublisher::publish` method returns an error, THE Lane SHALL log the error and continue processing rather than failing the commit result back to the caller.

---

### Requirement 5: Lane Configuration

**User Story:** As a Tokeira developer, I want Lane behavior to be configurable, so that operators can tune retry and coalescing parameters for their deployment.

#### Acceptance Criteria

1. THE Lane SHALL accept a configuration struct that specifies the maximum OCC retry count (default 5) and the maximum Mailbox coalescing drain count (default 16).
2. WHEN the configuration specifies a maximum OCC retry count of zero, THE Lane SHALL not retry on OCC_Conflict and SHALL return the conflict as an error.
3. WHEN the configuration specifies a maximum coalescing drain count of one, THE Lane SHALL process exactly one Mailbox item per activation (no coalescing).

---

### Requirement 6: Cross-Cutting Invariants

**User Story:** As a Tokeira developer, I want the Lane and Runtime to preserve the system's cross-cutting invariants, so that correctness is maintained as features are layered on top.

#### Acceptance Criteria

1. THE Lane SHALL NOT hold authoritative state only in memory; all state visible to the rest of the system SHALL be explained by a committed Transition in storage (CC.1: no in-memory-only authority).
2. THE Lane SHALL NOT pass transport or storage types into the Kernel; the Kernel interface SHALL remain `LoadedRun + Command → Transition` (CC.2: no transport/storage leakage).
3. THE Lane SHALL NOT assume it owns a run forever; a run that is idle after processing SHALL be eligible for eviction without correctness impact (CC.4: no permanent run ownership).
4. WHEN dispatch ops are published after commit, THE Lane SHALL tolerate duplicate publication; the Broker and downstream subsystems SHALL deduplicate by task identity (CC.7: idempotent derived-effect publication).
