# CHASM → tokeira: design foundation

> Status: design foundation (input to `chasm-foundation/design.md`). Ground-truthed to Temporal
> `v1.31.0` (`TEMPORAL_SERVER_COMPAT`). Citations are repo-relative paths in the sibling checkout
> `../temporal` `@ v1.31.0`. This doc captures the mapping, the hard parts, the crate shape, the
> performance thesis, and the verification reality — so the design can be written without
> re-deriving the investigation.

## 0. Thesis

CHASM ("Coordinated Heterogeneous Application State Machines") is Temporal's generalization of the
durable-execution engine: Workflow becomes "just one Application State Machine among many," running
on a reusable substrate (registry → libraries → typed components → atomic clock-stamped transitions
→ transactional-outbox tasks → built-in visibility). Replay 2026 framed CHASM as Temporal's forward
direction.

**tokeira is already, in effect, a single-archetype CHASM engine.** Its core invariants are CHASM's
invariants: atomic fenced transitions stamped by a monotonic clock, history-as-authority, dispatch
as a *derived* effect (AGENTS §3), epoch/revision fencing. So adopting CHASM is **generalization of
an existing engine**, not grafting a foreign architecture. That is the strategic reason to invest now.

The investment is staged: stand up a CHASM substrate as a **parallel** plane with the standalone
**activity** archetype as component #1; re-express Workflow as a CHASM component later (the arc
Temporal itself took).

## 1. CHASM contract (the surface tokeira must reproduce)

Ground truth: `chasm/` core + `chasm/lib/activity/` + `docs/architecture/chasm.md @ v1.31.0`.

- **Component** (`component.go:16,40`): `LifecycleState(Context) -> {Running|Completed|Failed}`
  (bitmask; `IsClosed = state >= Completed`). Root component closing closes the Execution.
  `RootComponent` adds `Terminate` + `ContextMetadata`.
- **Fields** (`field.go`, `field_internal.go`, `map.go`, `parent_pointer.go`): `Field[T]` (single
  value), `Map[K,T]`, `ParentPtr[T]` (parent access, skips map nodes), plus transient plain fields.
  `T` is either a proto message (**Data field**) or a child component (**Component field**). Each
  `Field`/`Map` child persists as its **own node**.
- **Node tree** (`tree.go:85,129`): an Execution is a tree of `Node`s; root identified by
  `ExecutionKey{NamespaceID, BusinessID, RunID}`. Each node persists as one `ChasmNode{metadata, data}`
  keyed by an **encoded path**; the path encoding (`path_encoder.go:25-75`) uses adjacent ASCII
  separators (`$` child, `#` collection child) so a subtree/collection/ancestor load is a single
  prefix **range scan**.
- **Transition** (`tree.go:1423` `CloseTransaction`): the atomic unit. Whole-execution; on commit,
  every dirty node is stamped with a new `VersionedTransition{namespace_failover_version,
  transition_count}` (`hsm.proto:114`). All field writes + task schedules commit together or roll back.
- **Engine** (`engine.go:17`): `StartExecution`, `UpdateWithStartExecution`, `UpdateComponent`,
  `ReadComponent`, `PollComponent` (monotonic long-poll; empty-on-deadline = "resubmit"),
  `DeleteExecution`, `NotifyExecution`. `Context` (read) vs `MutableContext` (adds `AddTask`).
  Injected on the request context; component code may panic and the framework recovers.
- **Tasks** (`task.go:15`, `task_handler_base.go`): **Pure** (in-transaction, holds write lock, no
  I/O) vs **Side-Effect** (post-commit, I/O, no direct state mutation). Each handler has a
  `Validate` gate (drop-if-stale) + `Execute`. Persisted as a **transactional outbox inside the
  owning component node** (`side_effect_tasks[]` / `pure_tasks[]`), identity `(VersionedTransition,
  offset)`, re-validated on every dirty close; `physical_task_status` is cluster-local /
  non-replicated / no-VT-bump; only the single earliest pure task tree-wide gets a physical timer
  (`tree.go:1699-2041`).
- **VersionedTransition** (`transition_history.go:9`): logical clock; `Compare`/`StalenessCheck`
  give `-1/0/1` (advanced/same/behind) — the long-poll change-detection + ref staleness primitive.
- **Registry / Library** (`registry.go`, `library.go`): a Library groups components + tasks + service
  handlers; components indexed by FQN / `uint32` type id (`farm.Fingerprint32(fqn)`) / Go type.
  `Archetype` = FQN of root component, `ArchetypeID` = type id of that FQN (`0` reserved = legacy
  Workflow).
- **ComponentRef** (`ref.go:16`): serialized return-address = `ExecutionKey` + `archetypeID` +
  `execution_versioned_transition` + `component_path[]` + `component_initial_versioned_transition`.
  Node identity = `(path, initial_vt)`; staleness via `execution_versioned_transition`.
- **Visibility** (`visibility.go`): a built-in child component; search attributes are user-defined
  and/or component-defined (provider interface); same for memo.

## 2. Mapping to tokeira

| CHASM (v1.31.0) | tokeira substrate | Fit |
|---|---|---|
| `CloseTransaction`, atomic, VT-stamped dirty nodes | fenced `commit_transition`, monotonic revision/epoch | Strong — same model |
| Task transactional outbox inside the node | "history is authority; dispatch is a derived effect" (AGENTS §3) | Strong — already tokeira's pattern |
| `stamp` task fencing (drop stale attempts) | epoch/revision fencing | Direct |
| Per-`Field`-as-node, range-queryable path encoding | DSQL rows keyed by encoded path; subtree = range scan | Strong + perf win |
| `VersionedTransition` clock + staleness | transition sequence + revision (+ failover) | Strong — wire format must match |
| `ComponentRef` consistency (initial/last-update VT) | fencing tokens | Direct |
| `Visibility` child component, component-defined SAs | `tokeira-projection` (+ C3 SA work) | Strong |
| `StateMachine[S]` + `Transition[S,SM,E]` table | (new) Rust enum-state + transition table | Easier in Rust |

## 3. The hard parts (and the Rust answer)

1. **Reflection-driven field iteration** (`fields_iterator.go`, `tree.go:810 syncSubComponents`):
   Go sniffs `"chasm.Field["` type names and mutates user structs in place. tokeira forbids runtime
   reflection. **Answer:** `#[derive(Component)]` macro + trait-based field registry reproducing the
   static rules (exactly one data field; fields non-pointer; map values are `Field<T>`; warn on
   unmanaged fields). Biggest divergence from the Go layout — concepts survive, mechanism is a macro.
2. **Generic node tree as range-queryable storage** (`path_encoder.go`, `tree.go` valueState
   machine): DSQL node table keyed by encoded path; lazy per-node (de)serialization. Where the
   **write-only-dirty-nodes** DSQL win lives.
3. **Task outbox semantics**: `(VT, offset)` identity, validate-then-drop each dirty close, single
   earliest pure task tree-wide gets a physical timer, `physical_task_status` cluster-local/
   non-replicated/no-VT-bump. Maps to derived dispatch; the exact rules are load-bearing.
4. **VT staleness + monotonic long-poll**: `-1/0/1` contract + empty-on-deadline "resubmit" +
   deadline buffer; ref-token wire format must round-trip for SDK long-poll compatibility.
5. **Engine-on-context + panic-as-framework-error**: becomes explicit `Result` plumbing through the
   generic engine wrappers (tokeira `no-unwrap` discipline).

Secondary subtleties: `ParentPtr` map-skipping ancestry + `detached` components; speculative
transitions (`WithSpeculative`); `UpdateWithStartExecution` atomicity with business-id reuse/conflict
policies.

## 4. Crate / module shape

- **`tokeira-chasm`** (pure, peer of `tokeira-kernel`): `Component` trait + `LifecycleState`,
  `Field`/`Map`/`ParentPtr`, Node tree + transition close, `VersionedTransition`, `Registry`/
  `Library`, `ComponentRef`, `StateMachine`/`Transition` helper, `#[derive(Component)]` macro. No
  I/O, no async.
- **Engine** in `tokeira-runtime` (+ persistence in `tokeira-storage`): `StartExecution`/
  `UpdateComponent`/`PollComponent`/`DeleteExecution`, node-table persistence, task-outbox dispatch,
  long-poll. Visibility hooks into `tokeira-projection`.
- **`tokeira-chasm-activity`** (component #1): the activity archetype — states/transitions/tasks
  from `lib/activity/statemachine.go` + `activity_tasks.go`, frontend handler bridging the public
  `*ActivityExecution` RPCs, `activity.enableStandalone` gate, visibility SAs `ActivityType` /
  `ExecutionStatus` / `TaskQueue`.

Identifiability: CHASM is obvious at the crate-graph level; the framework is one crate; each ASM is
its own crate; the pure/engine split mirrors the existing `kernel`/`runtime` boundary.

## 5. Standalone-activity component (component #1) — v1.31.0 spec

- **Library** `activity`, **Archetype** `"activity.activity"` (`lib/activity/library.go`).
- **States** (`activity_state.proto`): `SCHEDULED, STARTED, CANCEL_REQUESTED, COMPLETED, FAILED,
  CANCELED, TERMINATED, TIMED_OUT`. Lifecycle: `COMPLETED→Completed`; `FAILED|CANCELED|TERMINATED|
  TIMED_OUT→Failed`; else `Running` (`activity.go:90`).
- **Transitions** (`statemachine.go`): Scheduled / Rescheduled / Started / Completed / Failed /
  CancelRequested / Canceled / Terminated / TimedOut — each fences on an attempt `stamp`.
- **Tasks** (`activity_tasks.go`): side-effect `dispatch` (→ matching `AddActivityTask`); pure timers
  `scheduleToStart`, `scheduleToClose`, `startToClose`, `heartbeat`; all stamp-fenced.
- **RPC surface** (`lib/activity/proto/v1/service.proto`): `Start / Describe / Poll (long-poll) /
  RequestCancel / Terminate / Delete ActivityExecution`; requests wrap the public
  `workflowservice.*ActivityExecutionRequest` with a `namespace_id`. Handler→Engine map in
  `handler.go`; frontend gating + validation in `frontend.go` / `validator.go`.
- **Validation**: user-defined task queue required; activityId/activityType non-empty within
  `MaxIDLengthLimit`; retry-policy defaults; timeout normalization (S2S/S2C/HB rules, cap to
  runTimeout, HB ≤ S2C).

## 6. Performance thesis

- **Lean executions**: a standalone activity is a tiny state machine (scheduled→started→terminal),
  skipping the workflow-task/replay machinery — far fewer transitions / no WFT-history overhead.
- **Write-only-dirty-nodes on DSQL** (the biggest lever): per-`Field`-as-node persistence means a
  transition writes only touched nodes, cutting write amplification and OCC conflict surface — which
  matters acutely under DSQL's 100/sec connection budget and OCC retry model.
- **Pure tasks in-transaction**: fold immediate effects into the commit → fewer DSQL transactions;
  single-earliest-pure-task physical timer reduces timer churn.
- **Rust monomorphization**: no GC, no Go-interface/reflection cost on field access on the hot path.
- **Substrate reuse**: activities ride tokeira's existing lane/shard/routing unchanged.

Cost: a generic Node-tree state machine is more complex than the workflow-specialized kernel, and two
engines coexist until Workflow is migrated onto CHASM. Staged parallel-substrate-first bounds this.

## 7. Dynamic config + verification reality

- `history.enableChasm` defaults **true** (framework on) (`common/dynamicconfig/constants.go:2831`).
- `activity.enableStandalone` defaults **false** per-namespace (`lib/activity/config.go`) — the
  feature gate; `activity.longPollTimeout` 20s, `longPollBuffer` 1s.
- The v1.31.0 corpus suite `TestStandaloneActivityTestSuite` enables both via `OverrideDynamicConfig`
  in `SetupSuite` — which the **out-of-process conformance harness cannot deliver** (the seam writes
  to the in-process onebox config client the external `tokeirad` never reads; see
  `DECISION-callback-validation.md`). **So C1 is not verifiable through the Go corpus.** Verification
  for this spec is tokeira's own unit + property tests against the CHASM substrate and the activity
  component (round-trip serialization, transition legality, VT monotonicity, task validate-then-drop,
  long-poll monotonicity, node range-scan correctness), with the corpus as an aspirational target.

## 8. Key citations (`@ v1.31.0`)

- Architecture: `docs/architecture/chasm.md`
- Core: `chasm/component.go:16,40`, `field.go`, `field_internal.go`, `map.go`, `parent_pointer.go`,
  `tree.go:85,129,810,1168,1423,1699-2041`, `path_encoder.go:25-75`, `engine.go:17`, `context.go`,
  `task.go:15`, `task_handler_base.go`, `transition_history.go:9`, `registry.go`, `library.go`,
  `ref.go:16`, `archetype.go`, `statemachine.go:14-55`, `visibility.go`
- Activity: `chasm/lib/activity/{activity.go:50-104, statemachine.go, activity_tasks.go, handler.go,
  frontend.go, validator.go, library.go, config.go, proto/v1/*.proto}`
- Config: `common/dynamicconfig/constants.go:2831,2837`; `chasm/lib/activity/config.go:9-29`
- Persistence: `proto/internal/.../persistence/v1/chasm.proto`; `hsm.proto:114`
