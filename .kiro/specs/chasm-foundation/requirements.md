# Requirements Document

## Introduction

This document derives the requirements for the **CHASM Foundation** feature from the approved
design (`design.md`), which is itself built on `reference/chasm-mapping.md` and ground-truthed to
Temporal `v1.31.0` (`TEMPORAL_SERVER_COMPAT`). It does not re-derive the design; it extracts the
testable behaviour the design specifies.

CHASM ("Coordinated Heterogeneous Application State Machines") generalizes the durable-execution
engine so that Workflow becomes one Application State Machine among many, riding a reusable substrate:
registry → libraries → typed components → atomic clock-stamped transitions → transactional-outbox
tasks → built-in visibility. This feature lands that substrate in tokeira as a **parallel plane**
beside the existing workflow kernel/runtime (the workflow engine is left untouched), and proves it
with the **standalone-activity** archetype as component #1.

Scope is deliberately staged: the substrate plus the activity component only. Re-expressing the
existing Workflow engine as a CHASM component is **out of scope** (a future, separately-designed
migration). Where this document references behaviour, it defers to the targeted Temporal release per
`AGENTS §8`; a requirement that contradicts `v1.31.0` is wrong and is to be corrected.

Because the `v1.31.0` functional corpus cannot enable this feature through the out-of-process
conformance harness (it relies on an in-process `OverrideDynamicConfig` seam — foundation §7),
correctness is gated by tokeira's own unit and property tests, not the Go corpus. The named
correctness properties in the design are traced to the acceptance criteria below via
`Verified by: Property N` annotations, so the design's property → requirement back-references are
bidirectional.

## Glossary

- **CHASM**: Coordinated Heterogeneous Application State Machines — Temporal's generalization of the
  durable-execution engine into a reusable substrate (foundation §0).
- **CHASM_Crate**: The new pure library `tokeira-chasm` (peer of `tokeira-kernel`): component traits,
  field types, node tree, transition close, VersionedTransition, registry/library, ComponentRef. No
  I/O, no async, no storage.
- **Derive_Crate**: The new proc-macro crate `tokeira-chasm-derive` providing `#[derive(Component)]`.
- **Activity_Crate**: The new pure library `tokeira-chasm-activity` (component #1): the activity state
  enum, transition table, task definitions, and validation.
- **Component**: The unit of durable state and behaviour; reports a `LifecycleState` computed from its
  own state (`chasm/component.go:16,40 @ v1.31.0`).
- **LifecycleState**: One of `Running`, `Completed`, `Failed`; `is_closed` is true for `Completed`/
  `Failed`.
- **Root_Component**: A component at the root of an execution; additionally supports `Terminate` and
  carries `ContextMetadata`. When the root closes, the Execution closes.
- **Data_Field**: A component field whose payload is a proto message (a leaf). A component has exactly
  one data field.
- **Component_Field**: A component field whose payload is a child `Component` (a subtree root).
- **Map_Field**: A keyed collection field (`Map<K, T>`); each entry persists as its own node.
- **ParentPtr**: Upward access to a parent component that skips map nodes in the ancestry walk
  (`parent_pointer.go @ v1.31.0`).
- **Node**: A single persisted unit (`ChasmNode { metadata, data }`); every `Field`/`Map` child is its
  own node.
- **Node_Tree**: The tree of nodes forming an Execution; rooted at an `ExecutionKey`.
- **ExecutionKey**: `{ namespace_id, business_id, run_id }` — identifies an execution.
- **Encoded_Path**: The prefix-range-scannable byte key for a node; `$` introduces a child field, `#`
  introduces a collection (map) child (`path_encoder.go:25-75 @ v1.31.0`).
- **Path_Encoder**: The tokeira-owned implementation of the path-encoding sort/separator contract.
- **Transition**: The atomic unit of change, closed over the whole execution; on commit every dirty
  node is stamped with a new VersionedTransition (`tree.go:1423 CloseTransaction @ v1.31.0`).
- **VersionedTransition** (**VT**): A logical per-execution clock `{ namespace_failover_version,
  transition_count }` (`hsm.proto:114 @ v1.31.0`); total order via staleness check yielding
  Advanced / Same / Behind (`transition_history.go:9 @ v1.31.0`).
- **CHASM_Engine**: The async, stateful runtime façade in `tokeira-runtime` exposing
  Start / UpdateWithStart / Update / Read / Poll / Delete / Notify.
- **Context**: Read-only access to components and fields (no mutation, no task scheduling).
- **MutableContext**: Read-write access (field mutation + `add_task`), available only inside a
  transition.
- **Task**: Scheduled future work persisted in the owning node's transactional outbox; identity is
  `(VersionedTransition, offset)`.
- **Pure_Task**: A task that runs in-transaction, holds the write lock, and performs no I/O.
- **SideEffect_Task**: A task that runs post-commit, may do I/O, and performs no direct state mutation.
- **Task_Validator**: The drop-if-stale gate re-evaluated on every dirty close (validate-then-drop).
- **Physical_Timer**: The single armed engine-local timer corresponding to the earliest valid pure
  task tree-wide; `physical_task_status` is engine-local and never VT-stamped.
- **Registry**: The startup-built, immutable index of components/tasks/handlers, keyed by FQN, by
  `u32` type id, and by Rust type.
- **Archetype**: The FQN of a root component; **Archetype_Id** is the `u32` type id of that FQN; id `0`
  is reserved for legacy Workflow.
- **Component_Ref**: A serialized return-address to a specific component node, as of a specific
  execution VT (`ref.go:16 @ v1.31.0`).
- **Node_Store**: The new node table in `tokeira-storage`, keyed by encoded path on Aurora DSQL.
- **Visibility_Component**: A built-in child component contributing search attributes to the
  projection plane (`visibility.go @ v1.31.0`).
- **Edge_Bridge**: The `tokeira-edge` translation of the public `*ActivityExecution` RPCs to CHASM
  engine calls, gated by `activity.enableStandalone`.
- **Activity_Component**: The standalone-activity root component; library `activity`, archetype
  `"activity.activity"` (`lib/activity/library.go @ v1.31.0`).
- **Stamp**: The per-attempt fencing token an activity transition or task validates against the live
  attempt.

## Requirements

### Requirement 1: CHASM substrate crate identity and kernel purity

**User Story:** As a tokeira architect, I want CHASM to land as a pure crate that is a peer of the
kernel and is identifiable at the crate graph, so that the substrate is adopted as a generalization of
the existing engine without compromising kernel purity.

#### Acceptance Criteria

1. THE CHASM_Crate SHALL be a pure library that performs no I/O, no async, no storage access, and no
   metrics emission.
2. THE CHASM_Crate SHALL depend only on `tokeira-types` and `tokeira-proto` for value and wire types.
3. THE Derive_Crate SHALL provide the `#[derive(Component)]` proc-macro and depend only on
   `syn`, `quote`, and `proc-macro2`.
4. THE Activity_Crate SHALL depend on the CHASM_Crate, the Derive_Crate, `tokeira-types`, and
   `tokeira-proto`, and SHALL NOT contain engine internals.
5. THE CHASM Foundation SHALL leave `tokeira-kernel` unmodified and SHALL NOT add the CHASM_Crate as a
   dependency of `tokeira-kernel`.
6. WHERE the crate graph is inspected, THE CHASM Foundation SHALL expose `tokeira-chasm`,
   `tokeira-chasm-derive`, and `tokeira-chasm-activity` as distinct crates identifying the framework,
   the derive mechanism, and component #1 respectively.

### Requirement 2: Component model and lifecycle

**User Story:** As a component author, I want a typed component model with a well-defined lifecycle, so
that a component's closure deterministically drives execution closure.

#### Acceptance Criteria

1. THE Component SHALL report a LifecycleState of `Running`, `Completed`, or `Failed` computed from its
   own state (`chasm/component.go:16,40 @ v1.31.0`).
2. THE Component SHALL treat a LifecycleState of `Completed` or `Failed` as closed and `Running` as not
   closed.
3. WHEN a Root_Component's LifecycleState becomes closed, THE CHASM_Engine SHALL close the Execution.
   *Verified by: Property 11*
4. WHILE an Execution is closed, THE CHASM_Engine SHALL reject any further mutating transition on that
   Execution. *Verified by: Property 11*
5. THE Component SHALL declare its persistent children as typed fields of `Field<T>`, `Map<K, T>`, or
   `ParentPtr<T>`, plus transient plain fields that are not persisted.
6. THE Component SHALL contain exactly one Data_Field and any number of Component_Field or Map_Field
   children.
7. THE CHASM_Crate SHALL persist each `Field` and `Map` child as its own Node.
8. WHEN a `ParentPtr<T>` ancestry walk is performed, THE CHASM_Crate SHALL skip map nodes in the walk
   (`parent_pointer.go @ v1.31.0`).
9. THE Root_Component SHALL support a `Terminate` operation and SHALL carry `ContextMetadata`.

### Requirement 3: Compile-time component derivation replacing reflection

**User Story:** As a tokeira engineer bound by the no-reflection rule, I want `#[derive(Component)]` to
generate a static field registry and enforce the component rules at compile time, so that reflection
is replaced by monomorphized code and rule violations fail the build.

#### Acceptance Criteria

1. THE Derive_Crate SHALL generate a `Component::fields()` implementation as a static field registry
   from a struct's declared `Field`/`Map`/`ParentPtr` members, performing no runtime type inspection.
2. IF a component declares zero or more than one `#[chasm(data)]` field, THEN THE Derive_Crate SHALL
   raise a compile error.
3. IF a persistent field is declared as a bare reference, `Box`, or `Rc` rather than `Field`, `Map`, or
   `ParentPtr`, THEN THE Derive_Crate SHALL raise a compile error.
4. THE Derive_Crate SHALL emit a trait bound requiring a `Map<K, T>` value type `T` to be a valid field
   payload (a proto data message or a `Component`).
5. IF a field is neither a recognised field type nor explicitly marked `#[chasm(transient)]`, THEN THE
   Derive_Crate SHALL raise a compile error.
6. THE Derive_Crate SHALL emit no `unsafe` code.

### Requirement 4: Node tree and path encoding

**User Story:** As a storage and engine author, I want nodes keyed by an encoded path whose sort order
makes subtrees, collections, and ancestor chains single prefix range scans, so that loads and writes
are lean on DSQL.

#### Acceptance Criteria

1. THE Node_Tree SHALL identify its root by an ExecutionKey of `{ namespace_id, business_id, run_id }`.
2. THE Path_Encoder SHALL key each Node by an Encoded_Path in which `$` introduces a child field and
   `#` introduces a collection (map) child (`path_encoder.go:25-75 @ v1.31.0`).
3. THE Path_Encoder SHALL order separators below normal path-segment bytes so that the prefix range
   `[encode(path), encode(path)+0xFF)` returns exactly the subtree rooted at `path`.
   *Verified by: Property 8*
4. WHEN a subtree, a collection, or an ancestor chain is loaded, THE CHASM_Engine SHALL load it as a
   single prefix range scan over the Encoded_Path. *Verified by: Property 8*
5. THE Path_Encoder SHALL be a tokeira-owned implementation that reproduces the `v1.31.0` sort and
   separator contract without porting Temporal code.

### Requirement 5: Atomic transition and the VersionedTransition clock

**User Story:** As a correctness owner, I want each transition to commit the whole execution atomically
and stamp every dirty node with a monotonic VersionedTransition, so that field writes and task
schedules commit or roll back together and staleness is detectable.

#### Acceptance Criteria

1. WHEN a Transition is closed, THE CHASM_Crate SHALL commit all field writes and all task schedules
   for that Transition together, or roll them all back together.
2. WHEN a Transition is closed, THE CHASM_Crate SHALL stamp every node marked dirty during that
   Transition with a new VersionedTransition.
3. THE CHASM_Engine SHALL commit a Transition through tokeira's fenced `commit_transition` path.
4. THE VersionedTransition SHALL be `{ namespace_failover_version, transition_count }` and SHALL be
   monotonic per Execution. *Verified by: Property 3*
5. WHEN two VersionedTransitions are compared, THE CHASM_Crate SHALL yield exactly one of `Advanced`,
   `Same`, or `Behind`, comparing `namespace_failover_version` first and then `transition_count`
   (`transition_history.go:9 @ v1.31.0`). *Verified by: Property 3*
6. THE VersionedTransition SHALL round-trip through its wire encoding without loss
   (`hsm.proto:114 @ v1.31.0`).

### Requirement 6: Engine surface

**User Story:** As an engine caller, I want explicit Start/UpdateWithStart/Update/Read/Poll/Delete/
Notify operations with read versus read-write contexts and explicit error results, so that I can drive
components without ambient injection or panic-as-control-flow.

#### Acceptance Criteria

1. THE CHASM_Engine SHALL expose `StartExecution`, `UpdateWithStartExecution`, `UpdateComponent`,
   `ReadComponent`, `PollComponent`, `DeleteExecution`, and `NotifyExecution` operations
   (`engine.go:17 @ v1.31.0`).
2. WHEN `UpdateWithStartExecution` is invoked, THE CHASM_Engine SHALL start the execution if absent and
   then apply the update within a single Transition, honouring the business-id reuse/conflict policy.
3. THE CHASM_Crate SHALL provide a read-only `Context` that exposes neither field mutation nor task
   scheduling, and a `MutableContext` that adds field mutation and `add_task`.
4. THE CHASM_Engine SHALL make `MutableContext` available only inside an `UpdateComponent` or
   `StartExecution` Transition.
5. WHILE a `PollComponent` long-poll is blocked, THE CHASM_Engine SHALL resolve it when the component's
   VersionedTransition advances past the caller's token, or return an empty outcome when the deadline
   (minus the long-poll buffer) elapses without the VT advancing. *Verified by: Property 7*
6. WHEN `PollComponent` returns a non-empty outcome, THE CHASM_Engine SHALL return a state whose
   VersionedTransition is `Advanced` past the caller's token and SHALL NOT return a state whose VT is
   `Behind` the caller's token. *Verified by: Property 7*
7. THE CHASM_Engine SHALL accept the engine as an explicit handle rather than an ambient context value,
   and component transition functions SHALL return `Result<_, ChasmError>` rather than panicking.
8. THE CHASM_Crate SHALL NOT use `unwrap` or `expect` outside test code.

### Requirement 7: Task model and transactional outbox

**User Story:** As an engine author, I want pure and side-effect tasks stored as a transactional outbox
inside the owning node, re-validated on every close, with a single tree-wide physical timer, so that
dispatch is a derived effect and stale work is fenced.

#### Acceptance Criteria

1. THE Task SHALL be classified as either a Pure_Task that runs in-transaction with no I/O, or a
   SideEffect_Task that runs post-commit, may do I/O, and performs no direct state mutation.
2. THE CHASM_Crate SHALL persist tasks in the owning component node's outbox as `pure_tasks[]` and
   `side_effect_tasks[]`, with each task identified by `(VersionedTransition, offset)`.
3. WHEN a dirty close occurs, THE CHASM_Crate SHALL re-validate every task in the outbox through its
   Task_Validator.
4. IF a Task_Validator returns `Drop` at close, THEN THE CHASM_Crate SHALL drop the task without
   executing it. *Verified by: Property 5*
5. WHEN a Task_Validator returns `Valid` at close, THE CHASM_Crate SHALL retain the task with its stable
   `(VersionedTransition, offset)` identity. *Verified by: Property 5*
6. THE CHASM_Engine SHALL arm at most one Physical_Timer per execution tree at any time, corresponding
   to the earliest `fire_at()` among valid pure tasks tree-wide (`tree.go:1699-2041 @ v1.31.0`).
   *Verified by: Property 6*
7. THE CHASM_Engine SHALL hold `physical_task_status` as engine-local, non-replicated state that does
   not bump the VersionedTransition.
8. WHEN a Transition commits, THE CHASM_Engine SHALL dispatch surviving side-effect tasks only
   post-commit.

### Requirement 8: Registry, Library, and ComponentRef

**User Story:** As a substrate operator, I want components indexed by FQN, type id, and Rust type in an
immutable registry with archetype id 0 reserved for legacy Workflow, and an addressable ComponentRef
that detects staleness, so that archetypes never collide and stale references are never silently
followed.

#### Acceptance Criteria

1. THE Registry SHALL index components by fully-qualified name, by a `u32` type id derived from the
   FQN, and by Rust type.
2. THE Registry SHALL reserve Archetype_Id `0` for legacy Workflow and SHALL NOT assign it to a CHASM
   archetype.
3. THE Registry SHALL be built once at startup from registered libraries and SHALL be immutable
   thereafter.
4. THE Component_Ref SHALL carry `execution_key`, `archetype_id`, `execution_versioned_transition`,
   `component_path[]`, and `component_initial_versioned_transition` (`ref.go:16 @ v1.31.0`).
5. THE Component_Ref SHALL identify a node by `(component_path, component_initial_versioned_transition)`
   and SHALL round-trip through its wire encoding without loss. *Verified by: Property 9*
6. IF a Component_Ref's `execution_versioned_transition` is `Behind` the live execution
   VersionedTransition, THEN THE CHASM_Engine SHALL report the reference as stale rather than following
   it to a moved or closed node. *Verified by: Property 9*

### Requirement 9: Node storage on DSQL

**User Story:** As a storage owner, I want a node table keyed by encoded path that writes only dirty
nodes under OCC/CAS fencing on the VT, under the forward-only DSQL migration discipline, so that write
amplification and conflict surface stay small.

#### Acceptance Criteria

1. THE Node_Store SHALL persist each Node as one row keyed by `(namespace_id, business_id, run_id,
   encoded_path)`, carrying the node's VersionedTransition stamp, its initial VersionedTransition,
   `metadata`, and a nullable `data` payload.
2. THE Node_Store SHALL serialize and deserialize a Node — including its `metadata` task outboxes and
   its `data` payload — without loss across a round-trip. *Verified by: Property 1*
3. WHEN a Transition commits, THE Node_Store SHALL persist exactly the set of nodes marked dirty during
   that Transition, rewriting no clean node and skipping no dirty node. *Verified by: Property 4*
4. WHEN persisting a dirty node, THE Node_Store SHALL condition the write on the node's stored
   VersionedTransition matching the VT read by the Transition (compare-and-set).
5. IF a compare-and-set conflict occurs, THEN THE CHASM_Engine SHALL reload and re-run the Transition
   rather than force-overwriting.
6. THE Node_Store SHALL store each component's `pure_tasks[]` and `side_effect_tasks[]` inside the
   node's `metadata` rather than in a separate task table.
7. THE Node_Store migrations SHALL obey the DSQL-safe subset: one statement per file, secondary indexes
   created `ASYNC`, no `CHECK` constraints, and no `BIGSERIAL`.
8. WHILE the initial build phase is in effect, THE Node_Store SHALL define the node table as a single
   base `CREATE TABLE` migration without `ALTER TABLE`.

### Requirement 10: Visibility as a built-in component

**User Story:** As an operator, I want activity executions to contribute search attributes to
visibility through a built-in component, so that they are discoverable without putting visibility on
the correctness path.

#### Acceptance Criteria

1. THE Visibility_Component SHALL be a built-in child component through which a component declares the
   search attributes it contributes (`visibility.go @ v1.31.0`).
2. WHEN a Transition closes, THE CHASM_Engine SHALL collect the contributing component's declared search
   attributes for projection.
3. THE Visibility_Component SHALL emit its outputs as derived projection writes to `tokeira-projection`
   and SHALL NOT place them on the correctness path.
4. THE Activity_Component SHALL contribute the search attributes `ActivityType`, `ExecutionStatus`, and
   `TaskQueue` (foundation §5).

### Requirement 11: Standalone-activity archetype (component #1)

**User Story:** As a user of standalone activities, I want the activity archetype with its states,
legal transitions, timers, public RPC surface, validation, and per-namespace gate, so that I can run
activities directly on the CHASM substrate matching the targeted release.

#### Acceptance Criteria

1. THE Activity_Component SHALL register under library `activity` with archetype FQN
   `"activity.activity"` (`lib/activity/library.go @ v1.31.0`).
2. THE Activity_Component SHALL define the states `SCHEDULED`, `STARTED`, `CANCEL_REQUESTED`,
   `COMPLETED`, `FAILED`, `CANCELED`, `TERMINATED`, and `TIMED_OUT` (`activity_state.proto @ v1.31.0`).
3. THE Activity_Component SHALL map `COMPLETED` to `Completed`; `FAILED`, `CANCELED`, `TERMINATED`, and
   `TIMED_OUT` to `Failed`; and `SCHEDULED`, `STARTED`, and `CANCEL_REQUESTED` to `Running`
   (`activity.go:90 @ v1.31.0`).
4. WHEN an activity event `(from, event)` is in the legal transition table, THE Activity_Component SHALL
   apply the resulting `to` state. *Verified by: Property 2*
5. IF an activity event `(from, event)` is not in the legal transition table, THEN THE Activity_Component
   SHALL return `Err(IllegalTransition)` and leave the state unchanged. *Verified by: Property 2*
6. IF an activity transition or task is evaluated against a Stamp that does not match the live attempt,
   THEN THE Activity_Component SHALL treat it as superseded and SHALL NOT apply it.
7. THE Activity_Component SHALL define one SideEffect_Task `dispatch` that enqueues the activity task to
   matching, and the Pure_Tasks `scheduleToStart`, `scheduleToClose`, `startToClose`, and `heartbeat`
   timers, all stamp-fenced (`activity_tasks.go @ v1.31.0`).
8. THE Edge_Bridge SHALL translate the public `StartActivityExecution`, `DescribeActivityExecution`,
   `PollActivityExecution`, `RequestCancelActivityExecution`, `TerminateActivityExecution`, and
   `DeleteActivityExecution` RPCs to the corresponding CHASM_Engine calls, and route
   `ListActivityExecutions` / `CountActivityExecutions` to the projection plane.
9. WHEN admitting an activity request, THE Edge_Bridge SHALL require a user-defined task queue, require
   `activityId` and `activityType` to be non-empty and within `MaxIDLengthLimit`, apply retry-policy
   defaulting, and normalize timeouts such that schedule-to-start, schedule-to-close, and
   start-to-close are capped to the run timeout and heartbeat is no greater than start-to-close
   (`validator.go @ v1.31.0`).
10. WHERE `activity.enableStandalone` is disabled for a namespace, THE Edge_Bridge SHALL NOT admit the
    activity request and SHALL return the status the targeted release returns for the disabled feature.
11. THE Activity_Component configuration SHALL default `enableStandalone` to `false` per namespace,
    `longPollTimeout` to 20 seconds, and `longPollBuffer` to 1 second (foundation §7).
12. THE Activity_Component configuration SHALL reject unknown fields and SHALL round-trip through
    serialization without loss. *Verified by: Property 10*

### Requirement 12: Verification gated by tokeira's own tests

**User Story:** As a verification owner, I want correctness gated by tokeira's unit and property tests
rather than the Go corpus, so that this feature is verifiable despite the out-of-process harness seam.

#### Acceptance Criteria

1. THE CHASM Foundation SHALL gate correctness on tokeira's own unit and property-based tests against
   the CHASM substrate and the activity component.
2. THE CHASM Foundation SHALL NOT depend on the Temporal `v1.31.0` functional Go corpus to verify the
   standalone-activity feature, because the out-of-process conformance harness cannot deliver the
   in-process `OverrideDynamicConfig` override (foundation §7).
3. THE CHASM Foundation SHALL implement each named correctness property as a `proptest` property test
   running at least 100 iterations and carrying a `// Feature: chasm-foundation, Property N` tag.
