# Requirements Document

## Introduction

Let an embedding application register its own CHASM archetype into the embedded Tokeira
engine and run it as a long-lived, server-owned state machine: commands arrive as fenced
transitions through an in-process typed handle; durable work leaves the component as
standalone activities executed by the embedder's own scoped, versioned, stock-SDK workers;
completion returns into the component as one more fenced transition; timers and dispatch
survive an engine restart; and the whole loop can be driven with an injected clock.

This is the "second non-workflow archetype" the foundation spec names as the step that
"validates the registry/library generality and shakes out any activity-specific assumptions"
(`.kiro/specs/chasm-foundation/design.md`, roadmap). **No embedding application's code
enters this spec**: the acceptance vehicle is a test-only archetype inside the engine
workspace, and an embedder's library is authored later, in its own repository, against the
surface this spec defines.

**Scope boundary.** Engine repository only: `tokeira-chasm`, `tokeira-chasm-derive`,
`tokeira-chasm-activity`, `tokeira-runtime` (the `chasm` module and the publisher's callback
delivery), `tokeira-storage` (one new table above `V068`), `tokeira-edge` (the standalone
activity bridge and the scoped poll admission), `tokeira-config` (one gate), and
`tokeira-engine` (the embedder surface). The kernel is untouched (root `AGENTS.md §2`). The
foundation's single-root-node materialization is kept: components remain one root proto
(decision D4 below). Any embedder's own archetype, its repository, and its own API surface
are out of scope.

**Behaviour authority.**

- The standalone-activity archetype keeps matching Temporal **v1.31.0**
  (`chasm/lib/activity @ v1.31.0`) for every behaviour it has today.
- Standalone-activity **completion callbacks** do not exist at v1.31.0; their authority is
  the release that introduces them, **v1.32.0** (`chasm/lib/activity/activity.go`,
  `chasm/lib/activity/frontend.go`, `chasm/lib/callback @ v1.32.0`). They are gated off by
  default exactly as upstream gates them, so the v1.31.0 behavioural claim is unchanged.
- The registration model mirrors upstream's `chasm.Library` / `RegistrableTask` shape
  (`chasm/library.go`, `chasm/task.go`, `chasm/registrable_task.go @ v1.31.0`) in
  semantics, not in surface: the embedding API is Tokeira's own, because upstream has no
  embedded engine.
- Three mechanisms have **no upstream analog** and are Tokeira-owned by design: the embedder
  registration surface, the deployment-version target for standalone dispatch (upstream
  has not decided how to version standalone activities even at v1.32.0 —
  `chasm/lib/activity/activity.go:268 @ v1.32.0`, "TODO: Need to fill in VersionDirective once
  we decide how to handle versioning for standalone activities"), and the internal callback
  target that lands inside a CHASM execution.

**Decisions this spec fixes.**

- **D1 — versioned standalone dispatch is internal.** A scoped worker receives a standalone
  task only through a deployment-version target set by the staging component's executor;
  the public start request never carries one. Upstream has not decided how to version
  standalone activities, so exposing a target publicly would invent behaviour the targeted
  release may later contradict.
- **D2 — the internal callback target never crosses the wire.** It addresses a CHASM
  component reference and is delivered in process by the runtime; the wire rejects the
  `Internal` variant exactly as upstream does.
- **D3 — side-effect execution lives in the runtime.** The pure crate keeps validation and
  the outcome transition; the effect itself is performed by a registered runtime executor.
  This is the one deliberate divergence from upstream's library shape, forced by the
  substrate's no-I/O rule.
- **D4 — components stay one root proto.** Multi-node materialization is out of scope; a
  bounded collection inside the root proto is enforced by the transition that appends.
- **D5 — an embedder's library lives outside this repository.** The engine ships the
  surface and a test-only acceptance archetype; it never links an embedder's archetype.

**Foundational.** New durable state (an archetype-keyed pointer table), a new embedder
surface, and cross-cutting runtime changes. Sibling specs: `chasm-foundation` (substrate
contract), `chasm-activity-timeouts-and-retry` (its task 4.3, the recovery scan, is
absorbed here), `activity-executions-first-class` (the pointer table this spec re-keys),
`scoped-worker-authorization` (its Requirement 10.4 is amended by Requirement 7 below),
and `v132-standalone-activities`, which inherits Requirement 5 rather than rebuilding it.

## Glossary

- **Archetype:** a root component type — one that implements `RootComponent` — and
  therefore the unit that owns an execution: a business-id space, a lifecycle, a visibility
  type, and a current-run pointer. Identified by the 32-bit id derived from its FQN.
- **Component:** a typed durable state unit implementing `Component`; a non-root component
  is reusable inside a tree and has no execution identity of its own.
- **Library:** the registration unit a domain implements (`Library` trait): a stable name
  plus the components, task handlers, and search-attribute definitions it declares.
- **Built-in library:** a library the engine registers unconditionally because the Temporal
  surface depends on it (today: `activity`). Its name is reserved.
- **Extension library:** a library registered by the embedding application through the
  builder. It may not use a reserved name.
- **Registry:** the immutable index built once at startup from all registered libraries.
- **Pure task:** a task executed inside a fenced transition against the component, with no
  I/O; timers are pure tasks with a `fire_at` deadline.
- **Side-effect task:** a task whose effect is external to the transition; it is executed
  after the commit that staged it, never inside it.
- **Task handler:** the typed, registered code for a task type: a validator, and for pure
  tasks an `execute` transition, for side-effect tasks an `on_outcome` transition.
- **Side-effect executor:** the runtime component that performs a side-effect task's effect
  after commit (start an activity, enqueue a worker task). Registered by task type. Must be
  idempotent under re-execution.
- **Outbox:** the per-node list of staged tasks persisted with the node's metadata.
- **Rebuild scan:** the runtime pass that derives every due pure deadline and every pending
  side-effect task from committed node state, on start and periodically.
- **Armed timer:** the single earliest pure-task deadline the engine holds per execution so
  the sweeper knows when to look.
- **Current-run pointer:** the row resolving a business id to its current run; today keyed
  by namespace and business id only.
- **Standalone activity (SA):** the CHASM-backed activity execution of the `activity`
  library, started by `StartActivityExecution` or, under this spec, by a side-effect
  executor on behalf of another component.
- **Completion callback:** a callback attached to an SA that fires when the SA reaches a
  terminal state. **Nexus-URL variant:** delivered by HTTP to the URL. **Internal
  component target:** delivered in-process to the CHASM execution that staged the SA.
- **Deployment-version target:** the `(deployment_name, build_id)` pair a staged SA task
  carries so that only a scoped worker of that exact version may take it.
- **Scoped worker:** a poller whose credential carries a `WorkerScope` binding it to one
  namespace, one task queue, and one exact deployment version
  (`.kiro/specs/scoped-worker-authorization`).
- **Typed handle:** `TypedEngine<C>` for a registered root component `C`: start, update,
  read, poll, delete against that archetype, in process.
- **Clock:** the CHASM engine's source of "now", a closure the embedder may replace.
- **Acceptance archetype:** the test-only extension library in the engine workspace that
  exercises every mechanism of this spec end to end.

## Target State

Supported after this spec:

- An extension library registers root and non-root components, typed pure and side-effect
  task handlers, and its search-attribute definitions; the registry rejects reserved names,
  duplicate FQNs, and duplicate task types at build time.
- Pure tasks execute through their registered handler under a fenced transition; the
  activity archetype's existing evaluator path keeps working unchanged alongside.
- Side-effect tasks execute through registered runtime executors after commit; the activity
  archetype's worker dispatch becomes the first executor with no observable change; a
  second executor starts a standalone activity on behalf of any component, with an internal
  completion callback back to that component.
- Every due deadline and every pending side-effect task is re-derived from committed node
  state on engine start and by a periodic scan, so a restart loses no timer and no dispatch.
- Business ids are scoped per archetype: an activity and an extension execution may share a
  business id in one namespace.
- Standalone activities accept completion callbacks behind a gate that defaults off; with
  the gate on, start, describe, terminal scheduling and delivery match v1.32.0.
- A staged standalone activity may carry a deployment-version target; a scoped worker of
  that exact version takes it; unversioned standalone tasks remain denied to scoped workers.
- The embedded engine exposes a builder that accepts extension libraries, side-effect
  executors and a clock, and hands back typed handles, behind an explicitly unstable
  cargo feature.
- A test-only acceptance archetype proves create/update/read semantics, the external-work
  round trip through a scoped versioned worker, a restart in the middle of it, and
  deterministic replay under a virtual clock.

Explicitly out of scope: any embedder's archetype, repository and API;
multi-node materialization (D4); Nexus; kernel changes; `Callback.Internal` authored on the
wire (rejected upstream: "This variant is not settable in the API and will be rejected by
the service with an INVALID_ARGUMENT error", `temporal/api/common/v1/message.proto` @ API
v1.62.11); versioning on the public `StartActivityExecution` request (upstream undecided,
`activity.go:268 @ v1.32.0`); callback variants other than Nexus (upstream returns
INVALID_ARGUMENT "unsupported callback variant", `activity.go:459-468 @ v1.32.0`);
`RemoveSearchAttributes` (unimplemented today and untouched).

Sanctioned behaviour with the callbacks gate **off**: `completion_callbacks` on
`StartActivityExecution` is ignored and `DescribeActivityExecution.callbacks` is empty.
This is v1.31.0's behaviour — its activity frontend and validator contain no callback
handling at all (`chasm/lib/activity/frontend.go`, `chasm/lib/activity/validator.go @
v1.31.0`) — and it is what the current compatibility claim requires. v1.32.0's
gate-off behaviour (INVALID_ARGUMENT "completion callbacks are not enabled for this
namespace", `frontend.go:417-420 @ v1.32.0`) is recorded here for the compatibility bump
to v1.32.0; it is not adopted while the claim is v1.31.0.

## Evidence From Current Code

**Substrate (`crates/tokeira-chasm`).**
- `src/registry.rs:86-99` `Library { const NAME; fn register(&mut RegistryBuilder) }`;
  `:122` `register::<C: Component>()` rejecting FQN/id/type collisions; `:164`
  `register_library::<L>()`; `:54` `archetype_id_for_fqn` (FNV-1a/32); `:37`
  `LEGACY_WORKFLOW_ARCHETYPE_ID = 0` reserved. Registration covers components only; there is
  no task-handler or search-attribute registration.
- `src/component.rs:89` `Component` (with `const FQN`), `:140` `RootComponent`, `:163`
  `EngineComponent { from_data, into_data }` — the single-root-node bridge.
- `src/task.rs:219` `TaskValidator<C, T>::validate(&self, &C, &T, &dyn Context) -> TaskValidity`;
  `src/context.rs:71` `MutableContext::add_task(kind, task_type_id, payload, fire_at)`.
- `src/node.rs:418-531` `close_transaction` — validate-then-drop with a supplied validator.
- `src/visibility.rs:38-98` `SearchAttributeProvider`, `VisibilityContributor`,
  `VisibilitySnapshot`; reserved-field contract at `:50-57, 81-83`.

**Runtime (`crates/tokeira-runtime/src/chasm`).**
- `engine.rs:40-55` `DispatchSink` — its doc says the real implementation enqueues to
  matching; `engine.rs:655, 734` close with `RetainAllValidator`, so typed validators are never
  consulted and pure tasks accumulate; `engine.rs:261, 306` the clock closure and
  `with_clock`; `engine.rs:267` the process-local armed-timer map; `engine.rs:568-577` the
  start path reads the pointer by `(namespace, business_id)` and never compares archetype.
- `typed.rs:87-129` `TypedEngine::update` — closure error aborts before persist, re-run on
  fenced conflict; `:178` `read`; `:138` two-phase `update_with_start`.
- `sweeper.rs:29-36` `TimeoutEvaluator` — the only thing that fires an armed deadline, and
  it re-derives the activity's timeout instead of executing the staged pure task; nothing
  re-arms on start (the recovery scan is `chasm-activity-timeouts-and-retry` task 4.3, open).
- `mod.rs:218-238` the `Engine` trait: start, update, read, poll, delete, notify.
- Callback delivery for workflows: `crates/tokeira-runtime/src/publisher.rs:1598-1774`
  `deliver_completion_callback` (mirrors `components/callbacks/nexus_invocation.go @ v1.31.0`),
  retry config `crates/tokeira-runtime/src/nexus.rs:2163-2187` (1 s initial, 1 h max, 2.0
  coefficient, unbounded attempts), re-fire scanner `publisher.rs:2555, 2675`. Kernel callback
  state `crates/tokeira-kernel/src/state.rs:1250-1327` (`CallbackSpec` has the single variant
  `Nexus { url, header }`).

**Edge (`crates/tokeira-edge`).**
- `src/chasm_activity.rs:473-552` `ActivityDispatchQueue` — the only `DispatchSink`: a
  process-local map of FIFOs, routes `DISPATCH_TASK_ID` only (`:535`), and no code rebuilds
  it despite its doc; `:317` `ActivityTaskToken` and `:418` `ProtoTaskToken` carrying a
  component reference; `:1129` `respond_activity_task_completed` → token → `record_completed`
  on the activity component (hard-wired to one archetype).
- `src/grpc/workflow_service.rs:976-988` scoped workers are denied the standalone bridge
  ("standalone tasks are unversioned and therefore fail the fixed exact Deployment-Version
  admission model"); `src/workflow_service.rs:6690-6742` `admit_activity_task_queue_poll`.
- `src/grpc/workflow_service.rs:3147-3151` standalone start rejects unregistered
  search-attribute keys via `validate_search_attribute_keys` (`src/workflow_service.rs:2299`).
- `src/grpc/translate.rs:315-348` `callback_to_edge` (Nexus only; `Internal` rejected citing
  the proto comment) and `validate_completion_callbacks` (cites
  `service/frontend/workflow_handler.go:6299 @ v1.31.0`).

**Engine bootstrap (`crates/tokeira-engine/src/lib.rs`).**
- `:3419-3500` builds registry (`ActivityLibrary::register` at `:3427`), dispatch queue,
  visibility sink, `ChasmEngine`, `ActivityBridge`, repair scanner and timer sweeper; only the
  sweeper is gated (`enable_standalone_activities` or the harness force flag). No builder type
  exists; `:806` `start_with_embedded_config(EmbeddedEngineConfig)`. The `Engine` exposes no
  runtime or CHASM handle.

**Storage (`crates/tokeira-storage`).**
- `migrations/V056__chasm_current_run.sql` `PRIMARY KEY (namespace_id, business_id)`;
  `migrations/V049__chasm_node.sql`; `src/dsql/chasm_node.rs:387-413` `current_run` query;
  `:487-505` `scan_executions` (root nodes, deterministic order, no status filter, no index).
- `AGENTS.md:8-32`: one statement per file; forward-only, checksum-verified, no gaps;
  baseline locked through V067, V068 is the first post-baseline migration and the next is
  V069; DSQL subset (indexes `ASYNC`, no `CHECK`, no `BIGSERIAL`); `DdlValidator` enforces.
  `migrations/V068__workflow_hot_history_size.sql` is the post-baseline model: one idempotent
  statement.

**Config (`crates/tokeira-config`).**
- `src/lib.rs:817-828` `CompatibilityConfig { enable_standalone_activities: bool }` with
  `deny_unknown_fields`; `src/documentation.rs:449-456` the catalog entry, `:912` the fixture
  line; tests `:1052-1104` bind catalog, fixture and defaults together.

**Projection (`crates/tokeira-projection`).**
- `src/query_service.rs:222-236` a key is defined iff the store resolves it;
  `src/dsql_store.rs:458-478` `resolve_attr` / `register_attr` over `sa_registry`
  (`V029`, `V030`); system keys seeded at namespace registration
  (`crates/tokeira-edge/src/operator_service.rs:139-147`); custom keys arrive only via
  `AddSearchAttributes` (`src/grpc/operator_service.rs:40-60`).
- `crates/tokeira-runtime/src/chasm/visibility_adapter.rs:30-41, 100-103` reserved system
  fields enforced on CHASM snapshots.

**Upstream (authoritative).**
- `chasm/library.go @ v1.31.0` `Library { Name, Components, Tasks, RegisterServices,
  NexusServices, NexusServiceProcessors }`; `chasm/task.go:30-53 @ v1.31.0`
  `SideEffectTaskHandler { Execute, Discard }`, `PureTaskHandler { Execute }`,
  `TaskValidator { Validate }`; `chasm/registrable_task.go:35, 75 @ v1.31.0`;
  `chasm/registrable_component.go:204 @ v1.31.0` archetype id = `farm.Fingerprint32(fqn)`.
- `chasm/lib/activity/frontend.go:417-422 @ v1.32.0` (gate check, validator);
  `activity.go:111-113` (`Callbacks chasm.Map`), `:421-426` (terminal scheduling),
  `:429-475` (`addCompletionCallbacks`: closed → FAILED_PRECONDITION; over cap →
  FAILED_PRECONDITION; Nexus only; id = `requestID-idx`); `config.go:37-40` (`activity.enableCallbacks`,
  namespace bool, default false); `chasm/lib/callback/config.go @ v1.32.0`
  (`callback.maxPerExecution` 2000, `callback.request.timeout` 10 s,
  `callback.retryPolicy.initialInterval` 1 s, `callback.retryPolicy.maxInterval` 1 h);
  `tests/activity_standalone_test.go:117, 278-319 @ v1.32.0`.
- `proto/upstream/temporal/api/workflowservice/v1/request_response.proto:2996-2998`
  (`completion_callbacks = 19`), `:3052-3053` (`callbacks = 6`);
  `temporal/api/activity/v1/message.proto:207-221` `CallbackInfo { trigger: ActivityClosed,
  info: callback.v1.CallbackInfo }`; `temporal/api/common/v1/message.proto:178-206`
  `Callback { Nexus | Internal }`; `temporal/api/enums/v1/common.proto:37` `CallbackState`.

## Field Policy

### Configuration keys (`policy.compatibility`)

| Key | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `enable_standalone_activities` (existing) | Unchanged. | Unchanged. | Unchanged. |
| `enable_standalone_activity_callbacks` (new, bool, default `false`) | Off: v1.31.0 behaviour (callbacks ignored, describe empty). On: v1.32.0 behaviour per Requirement 5. Requires `enable_standalone_activities`. | Non-boolean or unknown sibling key → config load error (existing `deny_unknown_fields`). On with standalone activities off → validation error naming both keys. | Catalog entry, fixture line, and defaults test updated together; restart required. |

### `StartActivityExecutionRequest` (fields affected; all others unchanged)

| Field (id) | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `completion_callbacks` (19), gate off | Ignored (`chasm/lib/activity/frontend.go @ v1.31.0` has no handling). | None. | None. |
| `completion_callbacks` (19), gate on, `Nexus` variant | Validated with the same validator as `StartWorkflowExecution` (`translate.rs:342-348`), then stored on the activity with id `<request_id>-<index>` and state `STANDBY`, trigger `ActivityClosed` (`activity.go:457-475 @ v1.32.0`). | Invalid URL/header → INVALID_ARGUMENT as for workflows; count exceeds the per-execution cap → FAILED_PRECONDITION "cannot attach more than N callbacks…" (`activity.go:443-448`). | Callback state persisted in the activity's root proto; visible in describe. |
| `completion_callbacks` (19), gate on, `Internal` variant | Rejected. | INVALID_ARGUMENT "unsupported callback variant" (`activity.go:466-467 @ v1.32.0`; proto comment). | None. |
| `request_id` (existing) | Additionally seeds callback ids. | Unchanged. | Unchanged. |

### `DescribeActivityExecutionResponse` (field affected)

| Field (id) | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `callbacks` (6), gate off | Empty. | — | — |
| `callbacks` (6), gate on | One `activity.v1.CallbackInfo` per attached callback: `trigger.activity_closed`, `info.callback` (Nexus url/header), `info.state`, `info.attempt`, `info.last_attempt_complete_time`, `info.last_attempt_failure`, `info.next_attempt_schedule_time`, mapped as the workflow describe path maps its callbacks. Internal component targets are **never** listed. | — | Read-only projection of persisted state. |

### Embedder surface (`tokeira-engine`, cargo feature `chasm-extensions`, unstable)

| Item | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `Engine::builder()` | Returns a builder seeded with the built-in libraries and executors. | — | — |
| `.library::<L: Library>()` | Registers an extension library before the registry is frozen. | Reserved name, duplicate FQN, duplicate task type → build error naming the library and the collision. | Registry contents; archetype ids become persisted. |
| `.side_effect_executor(E)` | Registers a runtime executor for one task type. | Duplicate task type → build error. | None until a task of that type is staged. |
| `.clock(Arc<dyn Fn() -> i64 + Send + Sync>)` | Replaces the CHASM plane's source of now. | — | Every CHASM deadline and delayed dispatch reads it. |
| `.build().await` | Starts the engine as `start_with_embedded_config` does today. | Existing config errors; plus: storage holds an archetype id no registered library claims → refuse to start (fail closed). | Registry frozen. |
| `engine.chasm::<C: RootComponent>()` | Returns `TypedEngine<C>` for a registered archetype. | Unregistered `C` → error naming the FQN. | None. |
| `RegistryBuilder::register_pure_task::<H>` / `register_side_effect_task::<H>` | Registers a typed handler for `(component, task type)`. | Duplicate → `ChasmError` at build. | Task type ids become persisted in outboxes. |
| `RegistryBuilder::register_search_attributes::<C>(defs)` | Declares the keys and types a component emits. | Reserved system field name, or type mismatch with an existing definition → build error. | Keys registered in every namespace's `sa_registry` at engine start. |

### Scoped-worker poll admission for standalone tasks

| Case | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| Scoped worker; queued standalone task carries no deployment-version target | Denied, unchanged (`scoped-worker-authorization` Req 10.4). | Poll returns no task from the bridge; falls through as today. | None. |
| Scoped worker; target equals the worker's exact `(deployment_name, build_id)` | Served. | — | Task token records the target; start transition unchanged. |
| Scoped worker; target differs | Not served; task stays queued for its version. | — | None. |
| Unscoped worker; task carries a target | Not served (mirrors "unversioned poll must not consume exact-version work", `workflow_service.rs:7389`). | — | None. |
| Unscoped worker; no target | Served, unchanged. | — | Unchanged. |
| Public `StartActivityExecution` | Never sets a target. | — | Unchanged. |

## Requirements

### Requirement 1: Typed task registration

**User Story:** As an archetype author, I want to register my task handlers and
search-attribute definitions alongside my components, so that the engine can validate,
execute and index my tasks without knowing what they mean.

#### Acceptance Criteria
1. THE registry builder SHALL accept a pure-task handler typed by `(Component, Task)`
   carrying `validate` and `execute`.
2. THE registry builder SHALL accept a side-effect task handler typed by `(Component, Task)`
   carrying `validate` and `on_outcome`.
3. THE registry SHALL derive each task type id from the task's FQN with the same function
   that derives archetype ids, so the id is stable across restarts and releases.
4. IF two registrations resolve to the same task type id or the same task FQN, THEN THE
   registry builder SHALL fail with an error naming both.
5. IF an extension library registers under a reserved built-in name, THEN THE registry
   builder SHALL fail with an error naming the reserved name.
6. THE registry builder SHALL accept search-attribute definitions per component, each a key
   name and type.
7. IF a declared search-attribute key equals a reserved system field, THEN THE registry
   builder SHALL fail with an error naming the field.
8. WHEN the engine starts, THE runtime SHALL register every declared search-attribute key in
   each existing namespace's registry with the declared type, idempotently.
9. IF a declared key already exists in a namespace with a different type, THEN THE engine
   SHALL refuse to start and name the namespace, key and both types.
10. WHEN a transition closes, THE engine SHALL validate every staged task with its registered
    validator and drop tasks whose validator returns invalid.
11. IF a staged task's type has no registered handler, THEN THE transition SHALL fail with
    `ChasmError` naming the task type.
12. IF a transition fails for the reason in criterion 11, THEN THE engine SHALL persist
    nothing from that transition.
13. THE activity library SHALL register its existing task types through the same mechanism
    with their existing ids preserved.

### Requirement 2: Pure-task execution and timer durability

**User Story:** As an archetype author, I want a timer I stage to fire under a fenced
transition, including after the engine restarts, so that my reconciler can rely on time.

#### Acceptance Criteria
1. WHEN an execution's earliest pure-task deadline elapses, THE runtime SHALL load the
   execution, run each due pure task's registered `execute` under one fenced transition,
   and drop each executed task from the outbox.
2. IF a due pure task's validator returns invalid, THEN THE runtime SHALL drop it without
   running `execute`.
3. WHILE a pure task is not yet due, THE runtime SHALL NOT execute it.
4. WHEN the engine starts, THE runtime SHALL re-arm every execution's earliest pending pure
   deadline from committed node state before serving requests.
5. THE runtime SHALL periodically re-derive armed deadlines from committed node state, so
   that a lost armed entry is healed within one scan interval.
6. THE runtime SHALL execute a pure task at most once per staging.
7. IF a pure task was executed in a committed transition, THEN THE runtime SHALL NOT execute
   it again after a restart.
8. WHILE the activity library's executions are served by the existing timeout evaluator,
   THE runtime SHALL keep that path unchanged.
9. THE runtime SHALL use handler execution only for task types that have a registered pure
   handler.
10. THE sweeper SHALL expose a single-pass entry point that fires everything due at the
    engine's current clock reading, callable without the periodic loop.

### Requirement 3: Side-effect execution and durable dispatch

**User Story:** As an archetype author, I want a side-effect I stage to be performed after
my transition commits and to be performed again if the engine loses it, so that dispatch
is never the thing my correctness depends on.

#### Acceptance Criteria
1. WHEN a transition commits with staged side-effect tasks, THE runtime SHALL pass each
   task to the executor registered for its task type, after the commit and never before.
2. IF no executor is registered for a staged side-effect task's type, THEN THE transition
   SHALL fail at close with `ChasmError` naming the task type.
3. THE runtime SHALL treat executor failure as non-fatal to the transition: the commit
   stands and the task remains pending for re-execution.
4. WHEN the engine starts, THE runtime SHALL re-execute every pending side-effect task whose
   validator still holds and whose `fire_at` has elapsed.
5. THE runtime SHALL periodically re-execute pending side-effect tasks that satisfy criterion
   4, so a lost in-memory dispatch is healed within one scan interval.
6. THE side-effect executor contract SHALL state idempotence under re-execution as a
   requirement on every implementation.
7. WHEN an activity dispatch is re-executed with an unchanged attempt stamp, THE activity
   dispatch executor SHALL produce no second worker task.
8. WHEN a standalone-activity start is re-executed with an unchanged request id, THE start
   executor SHALL return the existing run.
9. WHEN a side-effect task's outcome arrives, THE runtime SHALL run the registered
   `on_outcome` under a fenced transition against the staging component and drop the task
   from the outbox in that transition.
10. IF an outcome arrives for a task the outbox no longer holds, THEN THE runtime SHALL
    treat it as a no-op that leaves the component unchanged.
11. THE activity library's worker dispatch SHALL be re-expressed as the first registered
    side-effect executor, with every existing standalone-activity behaviour preserved.
12. THE runtime SHALL ship a second executor that starts a standalone activity on behalf of
    the staging component from the task's payload: activity id, type, task queue, input,
    timeouts, retry policy, optional deployment-version target, and an internal completion
    callback addressed to the staging component and task.
13. THE dispatch sink SHALL route by task type across all registered executors.
14. THE hard-coded single-task-id sink SHALL be removed.
15. THE rebuild scan SHALL iterate executions in a deterministic order.

### Requirement 4: Archetype-scoped business ids

**User Story:** As an embedder, I want my archetype's business ids to be independent of
activity ids, so that a deployment named `x` and an activity named `x` can coexist as they
do upstream.

#### Acceptance Criteria
1. THE storage layer SHALL resolve a current run by `(namespace_id, archetype_id,
   business_id)`.
2. THE new pointer table SHALL be introduced as a forward-only migration above `V068`,
   one statement per file, within the DSQL subset, with any secondary index created
   `ASYNC`.
3. WHEN the engine starts against storage holding rows in the previous pointer table, THE
   storage layer SHALL backfill them into the new table with the activity archetype id,
   idempotently, before serving requests.
4. WHILE the backfill has not completed for a row, THE storage layer SHALL fall back to the
   previous table for that lookup, so no existing activity becomes unreachable.
5. WHEN a start names a business id current under a different archetype, THE engine SHALL
   treat the id as absent for the starting archetype.
6. WHEN a start names a business id current under the same archetype, THE engine SHALL
   apply the reuse and conflict policy exactly as today.
7. THE previous pointer table SHALL be retired only by a later migration once no fallback
   read has been served for a full release, recorded in the storage crate's rules.
8. THE visibility index SHALL continue to key rows by `(namespace_id, archetype_id, run_key)`
   unchanged.

### Requirement 5: Standalone-activity completion callbacks

**User Story:** As a component author, I want a standalone activity I start to tell its
starter when it finishes, so that the starter can advance without polling.

#### Acceptance Criteria
1. WHERE `enable_standalone_activity_callbacks` is off, THE edge SHALL ignore
   `completion_callbacks` on `StartActivityExecution` and return an empty `callbacks` list
   on describe (`chasm/lib/activity/frontend.go @ v1.31.0`).
2. WHERE the gate is on, WHEN `StartActivityExecution` carries `completion_callbacks`, THE
   edge SHALL validate each callback with the workflow start validator and reject invalid
   ones with INVALID_ARGUMENT.
3. WHERE the gate is on, IF a callback uses the `Internal` variant, THEN THE edge SHALL
   return INVALID_ARGUMENT "unsupported callback variant" (`activity.go:466-467 @ v1.32.0`).
4. WHERE the gate is on, IF attaching would exceed the per-execution cap (default 2000,
   `callback.maxPerExecution @ v1.32.0`), THEN THE engine SHALL return FAILED_PRECONDITION
   with the upstream message shape (`activity.go:443-448 @ v1.32.0`).
5. WHERE the gate is on, WHEN callbacks are attached, THE activity component SHALL persist
   each with id `<request_id>-<index>`, registration time from the CHASM clock, state
   `STANDBY`, and trigger `ActivityClosed` (`activity.go:449-475 @ v1.32.0`).
6. WHEN an activity with callbacks reaches a terminal state, THE activity component SHALL
   move each `STANDBY` callback to `SCHEDULED` in the terminal transition and stage delivery
   as a side-effect (`activity.go:421-426 @ v1.32.0`).
7. WHEN delivery of a Nexus-URL callback is executed, THE runtime SHALL use the workflow
   plane's completion-callback delivery: the same request timeout, backoff (1 s initial, 1 h
   max, coefficient 2.0), attempt accounting and terminal-failure rules
   (`publisher.rs:1598-1774`, `nexus.rs:2163-2187`).
8. WHEN a delivery attempt completes, THE activity component SHALL record the attempt,
   state (`BACKING_OFF`, `SUCCEEDED`, `FAILED`), last failure and next attempt time under a
   fenced transition.
9. WHERE the gate is on, THE edge SHALL populate `DescribeActivityExecution.callbacks`
   from persisted state per the field policy above.
10. THE edge SHALL never list an internal component target in `callbacks`.
11. THE config crate SHALL reject a configuration enabling callbacks while standalone
    activities are disabled, naming both keys.

### Requirement 6: Internal callback target

**User Story:** As a component author, I want the activity I started to complete into my
component as a fenced transition, so that the engine, not a worker, owns the outcome.

#### Acceptance Criteria
1. THE activity component SHALL accept a callback whose target is a component reference
   plus a task type id and task id, only when attached by a side-effect executor in
   process.
2. IF an internal component target arrives on any public request, THEN THE edge SHALL
   return INVALID_ARGUMENT exactly as for the `Internal` variant.
3. WHEN an activity with an internal target reaches a terminal state, THE runtime SHALL
   deliver the outcome by running the target component's registered `on_outcome` for the
   named task under a fenced transition, carrying the activity's terminal status, result or
   failure, and the activity's execution key.
4. WHEN an internal delivery's `on_outcome` transition commits, THE runtime SHALL mark the
   callback `SUCCEEDED` in the activity under a fenced transition.
5. WHILE a callback is `SCHEDULED` after a restart, THE runtime SHALL re-deliver it, so that
   a restart between the terminal commit and the delivery commit loses no delivery.
6. THE runtime SHALL apply each internal delivery to the target component at most once per
   terminal transition, the outbox drop in Requirement 3.9 being the fence.
7. IF the target component's outbox no longer holds the task, THEN THE runtime SHALL mark
   the callback `SUCCEEDED` without mutating the component.
8. IF the target execution does not exist, THEN THE runtime SHALL mark the callback `FAILED`
   with a non-retryable failure naming the execution key.
9. IF the `on_outcome` transition fails with a fenced conflict, THEN THE runtime SHALL
   retry it up to the engine's configured commit-retry bound before recording a failed
   attempt.
10. THE internal delivery SHALL NOT use HTTP, the Nexus client, or any queue.

### Requirement 7: Versioned standalone dispatch and scoped admission

**User Story:** As an embedder, I want the standalone activity my component
starts to run only on my worker release of the matching version, so that historical
platform code is executed by the release that owns it.

#### Acceptance Criteria
1. THE standalone-activity start executor SHALL accept an optional deployment-version target
   `(deployment_name, build_id)` from the staging task's payload.
2. THE public `StartActivityExecution` path SHALL NOT accept or set a deployment-version
   target.
3. WHEN a standalone task carries a target, THE dispatch executor SHALL enqueue it under the
   task queue and the exact target version.
4. WHEN a scoped worker polls, THE edge SHALL complete scoped admission before consulting
   the bridge, unchanged.
5. WHEN a scoped worker polls and a queued standalone task's target equals the worker's
   exact deployment version, THE bridge SHALL serve that task.
6. IF a queued standalone task carries no target, THEN THE bridge SHALL NOT serve it to a
   scoped worker (`scoped-worker-authorization` Req 10.4, unchanged).
7. IF a queued standalone task's target differs from the scoped worker's version, THEN THE
   bridge SHALL NOT serve it to that worker.
8. IF an unscoped worker polls a queue holding only targeted tasks, THEN THE bridge SHALL
   NOT serve them.
9. THE task token for a targeted task SHALL carry the target.
10. IF a completion, failure, cancellation or heartbeat for a targeted task arrives from a
    worker whose admitted version differs from the token's target, THEN THE edge SHALL
    reject it with PERMISSION_DENIED, as scoped-worker denials are mapped today.
11. THE `scoped-worker-authorization` requirements SHALL be amended to state criteria 5
    through 10 as the sanctioned exception to its Requirement 10.4.

### Requirement 8: Embedded engine builder, typed handle, clock

**User Story:** As an embedder, I want to register my library, executors and clock when I
construct the engine and get a typed handle back, so that my application drives its
archetype in process without an RPC surface.

#### Acceptance Criteria
1. THE engine crate SHALL expose `Engine::builder()` behind a cargo feature named
   `chasm-extensions`, documented as unstable with no semver promise.
2. THE builder SHALL register the built-in libraries and executors unconditionally.
3. THE builder SHALL accept extension libraries, side-effect executors and a clock closure
   per the field policy above.
4. WHEN `build` runs, THE builder SHALL freeze the registry and start the engine with the
   same semantics as `start_with_embedded_config`.
5. IF storage holds a current-run pointer whose archetype id matches no registered library,
   THEN `build` SHALL fail with an error naming the archetype id and the count of affected
   executions.
6. THE engine SHALL expose `chasm::<C: RootComponent>()` returning a `TypedEngine<C>` for a
   registered archetype.
7. IF `C` is not registered, THEN `chasm::<C>()` SHALL return an error naming `C::FQN`.
8. THE clock SHALL be the source of now for every CHASM transition, pure-task deadline,
   delayed dispatch, callback registration time and sweeper pass.
9. THE clock SHALL be documented as the CHASM plane's clock only; the workflow plane's time
   source is unchanged.
10. THE engine SHALL re-export the substrate types an extension library needs (`Component`,
    `RootComponent`, `Library`, `RegistryBuilder`, the task handler traits, `TypedEngine`,
    `ComponentRef`, `ChasmError`) under the same feature.

### Requirement 9: Acceptance archetype

**User Story:** As the engine owner, I want a test-only second archetype that exercises every
mechanism above, so that the substrate's generality is proven before any embedder depends
on it.

#### Acceptance Criteria
1. THE workspace SHALL contain a test-only extension library (publish = false) with a root
   component holding a create request id, a desired generation, an observed generation, an
   active operation, and a bounded operation history.
2. WHEN the acceptance archetype is created with request id `r1`, THE typed handle SHALL
   return generation 1.
3. WHEN the create is repeated with request id `r1` and identical input, THE typed handle
   SHALL return the same run and generation 1.
4. IF the create is repeated with request id `r1` and different input, THEN THE typed
   handle SHALL return a conflict error.
5. IF a create with request id `r2` names the same business id, THEN THE typed handle
   SHALL return already-started carrying the existing run id.
6. WHEN an update with expected generation 1 is applied, THE typed handle SHALL return
   generation 2.
7. IF a second update with expected generation 1 is applied after criterion 6, THEN THE
   typed handle SHALL return a generation mismatch error.
8. WHEN a read is issued, THE typed handle SHALL return the current view.
9. WHEN an update raises the desired generation above the observed generation, THE
   component SHALL stage a standalone-activity start carrying a deployment-version target
   and an internal callback.
10. WHEN a scoped worker of the exact target version polls the queue, THE bridge SHALL
    serve it the activity task.
11. IF an unscoped worker, or a scoped worker of another version, polls the queue, THEN THE
    bridge SHALL NOT serve it the activity task.
12. WHEN the worker completes the activity, THE component SHALL advance observed generation
    to the desired generation through `on_outcome`.
13. WHEN the worker fails the activity terminally, THE component SHALL record the failure
    and stage a retry timer through `on_outcome`.
14. WHEN the engine is dropped after the terminal activity commit and rebuilt over the same
    repository, THE rebuilt engine SHALL deliver the internal callback and advance observed
    generation without any request.
15. WHEN the engine is dropped with a retry timer armed and rebuilt, THE rebuilt engine
    SHALL fire the timer at its deadline.
16. WHEN the acceptance scenario runs under an injected clock with the sweeper driven by
    its single-pass entry, THE resulting transition sequence SHALL be identical across runs.
17. THE acceptance library SHALL share no code with the activity library's tests, so that
    an activity-specific assumption in the substrate fails here rather than passing by
    reuse.

### Requirement 10: Conformance stance, documentation and amendments

**User Story:** As the engine owner, I want this work to leave the v1.31.0 claim and the
corpus exactly as they are, so that the compatibility bump to v1.32.0 inherits it cleanly.

#### Acceptance Criteria
1. WHILE every gate keeps its default, THE public API SHALL behave exactly as before this
   spec on every request the v1.31.0 corpus issues.
2. THE `v132-standalone-activities` spec SHALL reference Requirement 5 as already
   implemented and gated.
3. THE `scoped-worker-authorization` spec SHALL be amended per Requirement 7.10 in the same
   change that lands Requirement 7.
4. THE `chasm-activity-timeouts-and-retry` tasks file SHALL mark its task 4.3 as delivered by
   this spec, with the record required by the house rules.
5. THE architecture documentation SHALL describe the extension surface, the executor split,
   the internal callback target and the deployment-version target.
6. THE CHASM plane diagram SHALL be updated so it no longer states that standalone
   activities are the only component.
7. Every new public item and module SHALL carry the documentation root `AGENTS.md §9`
   requires, with the upstream citations named in this document placed at the decision
   sites.

## Iteration and Feedback Notes

- Deferred by decision D4: multi-node materialization (child components as separate nodes) and
  therefore child callback components as upstream lays them out; callbacks are held inside
  the activity's root proto here, which the corpus cannot distinguish because it checks the
  describe output, not the layout.
- Deferred by scope: any embedder's archetype, its repository, and its API. The acceptance
  archetype's fields are chosen to match the shape of a long-lived resource entity, a
  desired and an observed generation, one active operation and a bounded history, so that
  an embedder's library can be written against the same surface without a second engine
  change.
- The `enable_standalone_activity_callbacks` gate mirrors `activity.enableCallbacks`
  (namespace-scoped upstream; engine-wide here, like the sibling gate). Per-namespace
  gating is not adopted because the sibling gate is engine-wide and the compatibility
  policy has no per-namespace keys.
- Open for the design phase: whether the deployment-version target is stored on the
  activity's root proto (so describe could show it) or only on the dispatch entry and token;
  the requirements only constrain admission and completion.
