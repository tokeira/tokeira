# Implementation Plan: CHASM Extension Archetypes

## Overview

Nine stages in dependency order: storage → substrate → runtime → activity library → edge →
config and engine → acceptance archetype → documentation and sibling amendments →
conformance rerun. Each stage ends in a checkpoint that a single implementer can reach and
verify alone, and no task asks the implementer to make a design choice: every behaviour is
fixed by `requirements.md`, every interface by `design.md`, and every upstream fact is
cited at the decision site (`chasm/lib/activity @ v1.31.0` for existing activity
behaviour, `chasm/lib/activity`, `chasm/lib/callback @ v1.32.0` for callbacks, `chasm/task.go
@ v1.31.0` for handler semantics). Every correctness property is a required property-based
test with `proptest`, ≥100 cases, tagged `// Feature: chasm-extension-archetypes, Property N:
<name>`. Inner loop per crate: `cargo check -p`, `cargo clippy -p <crate> --all-targets`,
`cargo nextest run -p <crate>`; the finishing bar is root `AGENTS.md §10.4`. Builds run
`--locked`; no external dependency is added by this plan.

## Tasks

### Stage 1 — Storage: the archetype-keyed pointer (`tokeira-storage`)

- [x] 1.1 Add migrations `V069__chasm_current_execution.sql` (the table in `design.md`,
  spread-key `PRIMARY KEY (namespace_id, archetype_id, business_id)`) and
  `V070__idx_chasm_current_execution_status.sql` (`CREATE INDEX ASYNC … ON
  chasm_current_execution (namespace_id, status, archetype_id, business_id)` on one line).
  One statement per file; unit-test both through `DdlValidator::validate`. Update the
  baseline note in `crates/tokeira-storage/AGENTS.md` so "the next schema change" reads V071.
  - _Requirements: 4.2_

  **DONE (2026-09-16):** V069–V071 add the scoped pointer, asynchronous status index
  and dedicated marker table. V070 includes `IF NOT EXISTS` for the migration runner's
  restart-safety contract. All three pass `DdlValidator`; all 71 embedded migrations
  pass the idempotence check. The storage rules now name V072 as next.

- [x] 1.2 Extend `ChasmNodeRepository`: `persist_new_execution(key, archetype_id, batch,
  current)` writing the pointer to the new table; `current_run(namespace_id, archetype_id,
  business_id)` reading the new table first and falling back to `chasm_current_run` while the
  backfill marker is unset; `scan_current_executions(status, after, limit)` in
  `(namespace_id, archetype_id, business_id)` order over the V070 index;
  `backfill_current_executions(archetype_id, batch)` as an idempotent `INSERT … SELECT … ON
  CONFLICT DO NOTHING` bounded per transaction; `distinct_archetypes()`. Mirror every method
  in the in-memory repository. Update the DSQL `persist_new_execution` upsert to the new key.
  - _Requirements: 4.1, 4.3, 4.4, 4.5, 4.6_

  **DONE (2026-09-16):** Both repositories implement scoped atomic pointer writes,
  exclusive cursor scans, bounded restartable backfill and archetype counts. Legacy
  fallback requires a matching existing root and an unset marker; a missing or
  mismatched root is absent. Runtime and bridge call sites pass the archetype id.

- [x] 1.3 Add the backfill marker (a row in `chasm_backfill_marker`, keyed
  `chasm_current_execution_backfill`) and `ChasmNodeRepository::backfill_marker_set()` /
  `set_backfill_marker()`. Unit-test: fallback read served only while the marker is unset.
  - _Requirements: 4.3, 4.4_

  **DONE (2026-09-16):** V071 owns `chasm_backfill_marker`; named marker reads/writes
  and `run_current_execution_backfill` are implemented. Engine bootstrap invokes the
  driver before CHASM construction, using the registered activity id and batches of
  500. Marker-complete boots skip copying; zero-sized batches are rejected.

- [x] 1.4 Record the retirement rule in `crates/tokeira-storage/AGENTS.md`: `chasm_current_run`
  is dropped by a later migration only after a full release with the marker set; add a test
  asserting no code path writes `chasm_current_run` after this stage.
  - _Requirements: 4.7_

  **DONE (2026-09-16):** The storage rules require a full release with the marker set
  and no fallback reads before retirement. `legacy_pointer_sql_is_read_only` guards
  production SQL; new starts and deletes affect only the scoped pointer table.

- [x] 1.5 Property test: Property 8 — archetype-scoped business ids
  - Reference-model PBT over interleaved starts across two archetype ids sharing business
    ids, with and without pre-seeded `chasm_current_run` rows and the marker toggled; run
    against the in-memory repository, and under `dsql-integration` against DSQL.
  - Tag: `// Feature: chasm-extension-archetypes, Property 8: archetype-scoped business ids`
  - _Requirements: 4.1, 4.3, 4.4, 4.5, 4.6_

  **DONE (2026-09-16):** The in-memory reference model passes 128 generated traces
  across independent archetypes, reuse/conflict policies, live root lifecycle changes,
  legacy seeds and marker positions. The DSQL variant supplies 100 shorter traces in
  an isolated schema and is environment-gated alongside the round-trip/fencing test.
  Both live DSQL paths were **not exercised**: `TOKEIRA_DSQL_TEST_DATABASE_URL` was unset.

- [x] 2. Checkpoint: `cargo clippy -p tokeira-storage --all-targets` clean; `cargo nextest run
  -p tokeira-storage` green; migrations validate; `chasm-foundation` visibility properties
  12–14 still green.
  - _Requirements: 4.8_

  **DONE (2026-09-16):** All six root finishing-bar commands pass, with nextest run
  serially: 3,361 passed and two existing SDK integration tests skipped. This includes
  the storage suite, migration DDL/restart-safety checks and unchanged CHASM visibility
  properties 12–14. Live DSQL execution remains unexercised as recorded in 1.5; the
  earlier parallel-run backlog timeout and successful rerun are recorded in the PR.

### Stage 3 — Substrate: task identity, typed handlers, registry (`tokeira-chasm`, `tokeira-chasm-derive`)

- [x] 3.1 Task identity: add `Task::FQN`, `RESERVED_TASK_ID_LIMIT = 1024`,
  `task_type_id_for_fqn` (the archetype hash function), `TaskOutcome`, `StartActivityTask`
  and `DeploymentVersionTarget` in `task.rs`. Give the activity's five existing tasks FQNs
  (`activity.dispatch`, `activity.schedule_to_start`, …) without changing their ids.
  - _Requirements: 1.3_

  **DONE (2026-09-16):** Task FQNs, the reserved range, task-owned codecs, outcome
  values and prost start/version payloads are implemented and re-exported. Codec
  regression tests preserve activity ids 1–5 and their existing postcard bytes; no
  dependencies changed.

- [x] 3.2 Handlers and registry: new `handler.rs` with `PureTaskHandler` and
  `SideEffectTaskHandler`; `TaskEntry` + `ErasedTaskHandler` (closures over encoded bytes,
  decode with `EngineComponent::from_data` and `Task::decode`, re-encode on mutation);
  `RegistryBuilder::{register_pure_task, register_side_effect_task,
  register_reserved_pure_task, register_reserved_side_effect_task,
  register_search_attributes, seal_built_ins}`; `Registry::{task_for_id, validate_task,
  execute_pure, apply_outcome, search_attribute_defs, is_built_in}`; build-time rejection of
  sealed names, FQN/id collisions, derived ids in the reserved range, reserved
  search-attribute names. New `ChasmError` variants: `UnknownTaskType`,
  `ReservedLibraryName`, `TaskTypeCollision`, `ReservedSearchAttribute`,
  `UnregisteredArchetype`.
  - _Requirements: 1.1, 1.2, 1.4, 1.5, 1.6, 1.7_

  **DONE (2026-09-16):** Typed pure and side-effect handlers are registered through
  monomorphized codec closures, with component/task lookup, identity checks, sealed
  built-in names and search definitions. All five error variants and the shared reserved
  system-field list are present. Typed dispatch and rejection tests pass.

- [x] 3.3 Add `MutableContext::resolve_task(TaskId)` and implement it on the runtime's
  transition context (stage 5.2 wires it). Add `RegistryOutboxValidator` implementing
  `OutboxValidator` over `Registry::validate_task` for one component's encoded data.
  - _Requirements: 1.10_

  **DONE (2026-09-16):** Outbox validation is fallible and registry-backed for a single
  root. A before-image journal restores changed nodes and clears dirty/pending state on
  validation errors. The runtime stages task resolutions for stage 5; its two close
  sites still use `RetainAllValidator`.

- [x] 3.4 Derive and compile-time guards: a `trybuild` case in `tokeira-chasm-derive/tests/ui`
  rejecting a handler whose `Component` is not a `RootComponent`; module docs for
  `handler.rs` and the extended `registry.rs` per root `AGENTS.md §9`.
  - _Requirements: 1.1, 1.2_

  **DONE (2026-09-16):** Both handler traits require `RootComponent`. The new trybuild
  non-root rejection and its generated stderr pass alongside the 12 existing UI cases;
  it compiles the actual trait source for portable diagnostic spans. Module/public-item
  docs and substrate re-exports are updated.

- [x] 3.5 Property test: Property 1 — registry validity and id stability
  - Generated library sets (names, component FQNs, task FQNs, reserved ids, search-attribute
    names); build succeeds iff the design's conditions hold; derived ids equal across builds.
  - Tag: `// Feature: chasm-extension-archetypes, Property 1: registry validity and id stability`
  - _Requirements: 1.3, 1.4, 1.5, 1.7_

  **DONE (2026-09-16):** Property 1 passes 128 generated registration traces against an
  independent reference model, checking each rejection and its named item, searched
  reserved-range hashes, component/task collisions, sealing and stable ids across two
  builds.

- [x] 3.6 Property test: Property 2 — validate-then-drop at close
  - Generated outboxes and validator decision functions; `close_transaction` retains exactly
    the `Valid` tasks in order; an unregistered task type fails the close with nothing dirty.
  - Tag: `// Feature: chasm-extension-archetypes, Property 2: validate-then-drop at close`
  - _Requirements: 1.10, 1.11, 1.12, 1.13, 3.2_

  **DONE (2026-09-16):** Property 2 passes 128 generated old/pending outboxes, retaining
  exactly valid tasks with stable ids, per-kind staging order, timer minima and dispatch
  sets. Unknown handlers roll back data, lifecycle and task counters with no dirty
  nodes; a separate late-failure test restores earlier nodes and removes newly created
  nodes.

- [x] 4. Checkpoint: `cargo clippy -p tokeira-chasm -p tokeira-chasm-derive --all-targets`
  clean; `cargo nextest run -p tokeira-chasm -p tokeira-chasm-derive` green; `cargo test -p
  tokeira-chasm --doc` green.

  **DONE (2026-09-16):** All six root finishing-bar commands pass with `--locked`
  where applicable: nightly fmt, workspace lint/check, 3,368 nextest tests passed
  (serial; two existing SDK integration tests skipped), workspace doctests and docs
  with warnings denied. The focused substrate/derive/activity suite passes 114 tests;
  all existing runtime CHASM tests pass. Live DSQL was not exercised (URL gate unset);
  the two CHASM live cases and their feature/env gates are recorded in the live-suite guide.

### Stage 5 — Runtime: executors, outcome application, execution, rebuild (`tokeira-runtime`)

- [ ] 5.1 New `chasm/executor.rs`: `SideEffectExecutor` trait and `DispatchMultiplexer`
  implementing `DispatchSink`, duplicate task type rejected at registration, unknown task
  type at dispatch logged and left pending. Keep `CollectingDispatchSink` for tests.
  - _Requirements: 3.1, 3.2, 3.13_
- [ ] 5.2 Close-time validation: replace `RetainAllValidator` at `engine.rs:655` and `:734`
  with `RegistryOutboxValidator`; return `ChasmError::UnknownTaskType` before any persist
  when a staged task has no entry; implement `resolve_task` on `TransitionContext` and
  honour it at close.
  - _Requirements: 1.10, 1.11, 1.12, 3.2_
- [ ] 5.3 `ChasmEngine::apply_side_effect_outcome(target, task_type_id, task_id, outcome) ->
  OutcomeApplied`: load, `NotHeld` if the outbox lacks the id, `ExecutionMissing` if absent,
  otherwise `Registry::apply_outcome` with `resolve_task(task_id)` in the same transition,
  reload-and-rerun on `Conflict` up to `max_commit_retries`.
  - _Requirements: 3.9, 3.10, 6.6, 6.7, 6.9_
- [ ] 5.4 Sweeper: `ChasmTimerSweeper::new(engine)` + `with_evaluator(archetype_id,
  evaluator)`; in `sweep_once`, use the evaluator when one is installed for the execution's
  archetype (unchanged behaviour), otherwise execute due, valid pure tasks through
  `Registry::execute_pure` in one fenced transition, resolving each, and re-arm to the
  earliest remaining deadline. Update the engine bootstrap call site.
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.8, 2.9, 2.10_
- [ ] 5.5 New `chasm/rebuild.rs`: `OutboxRebuildScanner::rebuild_once` over
  `scan_current_executions(Running)` in deterministic order: set the armed timer to the
  outbox's earliest pure deadline; hand every pending side-effect task whose validator holds
  and whose `fire_at` has elapsed to the multiplexer. Add `CHASM_REBUILD_INTERVAL` beside
  `VISIBILITY_REPAIR_INTERVAL` and a `spawn_outbox_rebuild` helper mirroring
  `spawn_visibility_repair`.
  - _Requirements: 2.4, 2.5, 2.7, 3.4, 3.5, 3.15_
- [ ] 5.6 Extract `invoke_nexus_callback(client, config, url, header, completion, links)` from
  `deliver_completion_callback` in `publisher.rs`; the workflow path calls it; behaviour and
  its `components/callbacks/nexus_invocation.go @ v1.31.0` citation unchanged. Make
  `nexus_completion_backoff` reachable from the edge (`pub`).
  - _Requirements: 5.7_
- [ ] 5.7 `TypedEngine<C>` owns `Arc<ChasmEngine>` (drop the lifetime); update the bridge's
  construction sites.
  - _Requirements: 8.6_
- [ ] 5.8 Property test: Property 3 — pure-task execution model
  - Generated staged pure tasks with deadlines and monotone clock sequences against a
    reference model; each executed once, in deadline order, never early; run with the
    injected clock and `sweep_once` over the in-memory repository.
  - Tag: `// Feature: chasm-extension-archetypes, Property 3: pure-task execution model`
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.9, 2.10_
- [ ] 5.9 Property test: Property 4 — timer rehydration round-trip
  - Generated committed states and crash points; rebuild over the same repository re-arms
    the model's earliest deadline; executed tasks never re-execute.
  - Tag: `// Feature: chasm-extension-archetypes, Property 4: timer rehydration round-trip`
  - _Requirements: 2.4, 2.5, 2.7_
- [ ] 5.10 Property test: Property 5 — dispatch derived from state
  - Generated committed states and in-memory losses; `rebuild_once` hands the sink exactly
    the model's pending effects in deterministic order; re-executing a `CollectingDispatchSink`
    counterpart of each shipped executor with an unchanged task is a no-op.
  - Tag: `// Feature: chasm-extension-archetypes, Property 5: dispatch derived from state`
  - _Requirements: 3.1, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.15_
- [ ] 5.11 Property test: Property 6 — outcome application fence
  - Generated delivery sequences (duplicates, after-drop, under injected conflicts);
    `on_outcome` applies at most once; other deliveries leave the node bytes unchanged.
  - Tag: `// Feature: chasm-extension-archetypes, Property 6: outcome application fence`
  - _Requirements: 3.9, 3.10, 6.6, 6.7, 6.9_

- [ ] 6. Checkpoint: `cargo clippy -p tokeira-runtime --all-targets` clean; `cargo nextest run
  -p tokeira-runtime` green including every existing `chasm` and `publisher` test.

### Stage 7 — Activity library: handlers, callbacks, version target (`tokeira-chasm-activity`)

- [ ] 7.1 `ActivityState` fields 39 `version_target` and 40 `callbacks`; messages
  `DeploymentVersionTarget`, `ActivityCallback`, `NexusTarget`, `InternalTarget` as in
  `design.md`; `lifecycle_for` unchanged. Cite `activity.go:111-113 @ v1.32.0` at the field.
  - _Requirements: 5.5, 7.1_
- [ ] 7.2 Register the existing task types as reserved handlers in `ActivityLibrary::register`
  (validators = the crate's existing validators; `execute` = the existing `apply` events).
  Existing timeout behaviour is unchanged because the evaluator path remains installed
  (stage 5.4).
  - _Requirements: 1.13, 2.8_
- [ ] 7.3 Callback state machine: events `CallbacksAttached`, `CallbackAttempted`,
  `CallbackRetryDue`; terminal transitions set `STANDBY → SCHEDULED` and stage one
  `DeliverCallback` per callback (`activity.go:421-426 @ v1.32.0`); `DeliverCallbackHandler`
  and `CallbackRetryHandler` with ids 6 and 7; attempt recording per the design's state
  machine with backoff from the shared policy; the attach rules: closed →
  FAILED_PRECONDITION, over cap → FAILED_PRECONDITION with the upstream message, ids
  `<request_id>-<idx>`, registration time from the context clock (`activity.go:429-475 @
  v1.32.0`).
  - _Requirements: 5.4, 5.5, 5.6, 5.8_
- [ ] 7.4 `validate_and_normalize` gains `callbacks` and `version_target`; the `Internal`
  wire variant is never constructed here (the edge rejects it); an `InternalTarget` is
  accepted only through the executor path. `ActivityRequest` documents both fields.
  - _Requirements: 5.2, 5.3, 6.1, 7.1_
- [ ] 7.5 Property test: Property 10 — callback delivery state machine
  - Generated terminal transitions and per-callback outcome sequences against the kernel
    callback model's state graph; attempts counted once; next attempt time equals the shared
    backoff; non-retryable is terminal.
  - Tag: `// Feature: chasm-extension-archetypes, Property 10: callback delivery state machine`
  - _Requirements: 5.6, 5.7, 5.8_

- [ ] 8. Checkpoint: `cargo clippy -p tokeira-chasm-activity --all-targets` clean; `cargo
  nextest run -p tokeira-chasm-activity` green; every existing statemachine test unchanged.

### Stage 9 — Edge: executors, versioned queue, provenance, RPC surface (`tokeira-edge`)

- [ ] 9.1 `ActivityDispatchExecutor` (task type 1) wrapping the queue: entries carry
  `target`; a served-stamp set makes re-execution a no-op; `ActivityBridge::with_dispatch_executor`
  replaces `with_dispatch_queue`; delete the `DISPATCH_TASK_ID`-only `DispatchSink` impl.
  - _Requirements: 3.11, 3.14, 7.3_
- [ ] 9.2 `StartActivityExecutor` (`chasm.start_activity`): map every `StartActivityTask`
  field onto the bridge's start path with request id = the staging task id, attach the
  `InternalTarget { component_ref, task_type_id, task_id }`, carry `version_target`.
  `NexusCallbackExecutor` and `InternalCallbackExecutor` (task type 6, routed by variant):
  the first calls `invoke_nexus_callback` then records the attempt through
  `TypedEngine<ActivityExecution>`; the second calls `apply_side_effect_outcome` with the
  activity's terminal `TaskOutcome`, then records `SUCCEEDED`, `FAILED` (non-retryable, missing
  target) or a retryable attempt on `RetriesExhausted`.
  - _Requirements: 3.12, 5.7, 5.8, 6.3, 6.4, 6.5, 6.8, 6.10_
- [ ] 9.3 Poll admission: `poll_activity_task_waiting(task_queue, identity, admitted:
  Option<&DeploymentVersionTarget>)` selects the first due entry whose target equals
  `admitted`; the gRPC branch at `grpc/workflow_service.rs:976-988` passes the scoped
  worker's exact version (`Some`) or `None`; an untargeted entry never matches `Some`.
  - _Requirements: 7.3, 7.4, 7.5, 7.6, 7.7, 7.8_
- [ ] 9.4 Token and provenance: `ProtoTaskToken` field 15 `version_target`; when a targeted
  task is served, write the token digest to `worker_task_provenance` with origin
  `{namespace, normal task queue, task_class: Activity, deployment, build_id}`; call
  `authorize_scoped_task_token` on the standalone completed / failed / canceled / heartbeat
  paths before the bridge.
  - _Requirements: 7.9, 7.10_
- [ ] 9.5 RPC surface: with the gate on, `start_activity_execution` validates
  `completion_callbacks` through `validate_completion_callbacks`, rejects `Internal` with
  INVALID_ARGUMENT "unsupported callback variant" (`activity.go:466-467 @ v1.32.0`) and passes
  the rest to the library; with the gate off the field is never read.
  `describe_activity_execution` maps persisted Nexus callbacks to `activity.v1.CallbackInfo`
  through the workflow path's `CallbackInfo` mapping and never lists internal targets. The
  edge never populates `version_target`.
  - _Requirements: 5.1, 5.2, 5.3, 5.9, 5.10, 6.2, 7.2_
- [ ] 9.6 Property test: Property 7 — activity dispatch equivalence
  - Generated standalone-activity lifecycle sequences; the executor-backed bridge produces
    the same worker-visible task sequence and describe outcomes as a recorded pre-change
    model (`CollectingDispatchSink` snapshot of the old queue semantics).
  - Tag: `// Feature: chasm-extension-archetypes, Property 7: activity dispatch equivalence`
  - _Requirements: 2.8, 3.11, 3.13, 3.14_
- [ ] 9.7 Property test: Property 9 — callback attachment model
  - Generated callback lists with the gate off and on; gate off ≡ no-callback start; gate on
    matches the v1.32.0 attach model and describe lists exactly the Nexus callbacks.
  - Tag: `// Feature: chasm-extension-archetypes, Property 9: callback attachment model`
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.9, 5.10, 6.2_
- [ ] 9.8 Property test: Property 12 — versioned admission
  - Generated queues mixing untargeted and targeted entries and generated pollers
    (unscoped, scoped to random versions); served target always equals the admitted version;
    completion with a mismatched targeted token is denied.
  - Tag: `// Feature: chasm-extension-archetypes, Property 12: versioned admission`
  - _Requirements: 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10_
- [ ] 9.9 Property test: Property 17 — gate-off invariance
  - Differential test: the recorded v1.31.0 standalone-activity request set, plus generated
    starts carrying `completion_callbacks`, replayed with gates at default against the
    pre-change and post-change bridge; responses and describes byte-identical.
  - Tag: `// Feature: chasm-extension-archetypes, Property 17: gate-off invariance`
  - _Requirements: 5.1, 10.1_

- [ ] 10. Checkpoint: `cargo clippy -p tokeira-edge --all-targets` clean; `cargo nextest run
  -p tokeira-edge` green including every existing standalone-activity and scoped-worker test.

### Stage 11 — Config and engine: gate, builder, handle, clock, seeding (`tokeira-config`, `tokeira-engine`)

- [ ] 11.1 `CompatibilityConfig::enable_standalone_activity_callbacks` (default false, doc
  citing `activity.enableCallbacks @ v1.32.0`); `TokeiraConfig::validate` emits
  `ValidationError::Field` when callbacks are on and standalone activities off; `field!`
  catalog entry and fixture line; the two catalog tests pass.
  - _Requirements: 5.11_
- [ ] 11.2 Feature `chasm-extensions` in `crates/tokeira-engine/Cargo.toml` and the
  `pub mod chasm` re-export module under it.
  - _Requirements: 8.1, 8.10_
- [ ] 11.3 `EngineBuilder` and `Engine::builder(config)`: `library::<L>()`,
  `side_effect_executor`, `clock`, `build` in the design's order (built-ins, `seal_built_ins`,
  extensions, freeze, storage, backfill until zero, `distinct_archetypes` fail-closed check,
  multiplexer with the four edge executors then extensions, `ChasmEngine::with_clock`,
  search-attribute seeding, `rebuild_once`, then the existing start path);
  `start_with_embedded_config` delegates to it. `Engine` gains `chasm_engine` and `registry`.
  - _Requirements: 1.8, 1.9, 2.4, 3.4, 8.2, 8.3, 8.4, 8.5, 8.8, 8.9_
- [ ] 11.4 `Engine::chasm::<C>()` returning `TypedEngine<C>`, error naming `C::FQN` when
  unregistered.
  - _Requirements: 8.6, 8.7_
- [ ] 11.5 Search-attribute seeding: at start for every namespace and inside
  `seed_predefined_search_attributes` for later namespaces, register each declared
  definition via the projection store's `register_attr`; a type mismatch aborts start with
  namespace, key and both types.
  - _Requirements: 1.8, 1.9_
- [ ] 11.6 Property test: Property 13 — fail-closed build
  - Generated storage pointer sets over archetype ids and generated registered library
    sets; `build` succeeds iff every stored id is registered; the error names the first
    unregistered id and its count.
  - Tag: `// Feature: chasm-extension-archetypes, Property 13: fail-closed build`
  - _Requirements: 8.4, 8.5, 8.7_
- [ ] 11.7 Property test: Property 16 — search-attribute registration
  - Generated definitions and pre-existing registry states across restarts and namespace
    creations; seeding idempotent; start fails iff a declared key exists with another type.
  - Tag: `// Feature: chasm-extension-archetypes, Property 16: search-attribute registration`
  - _Requirements: 1.6, 1.8, 1.9_

- [ ] 12. Checkpoint: the full root `AGENTS.md §10.4` bar on the workspace, with
  `--features chasm-extensions` added to the lint, check, test and doc steps for
  `tokeira-engine`.

### Stage 13 — Acceptance archetype (`crates/tokeira-chasm-acceptance`, publish = false)

- [ ] 13.1 Crate skeleton in the activity crate's shape: `ResourceState` proto, `Resource`
  root component, `AcceptanceLibrary` registering the component, `ReconcileHandler`
  (`SideEffectTaskHandler` over `StartActivityTask`), `RetryHandler` (`PureTaskHandler` over
  `RetryTimer`), and `DeploymentStatus`, `DesiredGeneration`, `ObservedGeneration` search
  attributes. Dev-dependency on `tokeira-engine` with `chasm-extensions`; no dependency on
  the activity crate's test modules (assert with a workspace test).
  - _Requirements: 9.1, 9.17_
- [ ] 13.2 Command semantics through the typed handle: create with request id and digest
  (idempotent repeat, conflicting repeat, second request id → already-started), update with
  expected generation (match / mismatch), read.
  - _Requirements: 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_
- [ ] 13.3 Reconcile: an update raising desired above observed stages `StartActivityTask`
  with a version target and the internal callback; `on_outcome` advances observed on
  completion or records the failure and stages `RetryTimer` on terminal failure;
  `RetryHandler` re-stages the start; history bounded to 8 entries by the appending
  transition.
  - _Requirements: 9.9, 9.12, 9.13_
- [ ] 13.4 Integration scenario (`tests/acceptance.rs`): `Engine::builder(in-memory config)
  .library::<AcceptanceLibrary>().clock(virtual)`; a scoped worker of the target version, an
  unscoped worker and a scoped worker of another version polling through the in-process
  gRPC service; admission proofs; completion and failure paths; restart proof A (drop the
  engine after the activity's terminal commit, rebuild over the same `Arc` repository, no
  request issued, observed advances); restart proof B (drop with a retry timer armed,
  rebuild, timer fires at its deadline under the virtual clock).
  - _Requirements: 9.10, 9.11, 9.14, 9.15, 9.16_
- [ ] 13.5 Property test: Property 15 — acceptance reference model
  - Generated command sequences against a generation reference model; responses equal the
    model's at every step.
  - Tag: `// Feature: chasm-extension-archetypes, Property 15: acceptance reference model`
  - _Requirements: 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_
- [ ] 13.6 Property test: Property 11 — internal delivery exactly once
  - Generated crash points between the activity's terminal commit and the delivery commit;
    the target receives the outcome exactly once; a missing target marks the callback
    `FAILED` non-retryably.
  - Tag: `// Feature: chasm-extension-archetypes, Property 11: internal delivery exactly once`
  - _Requirements: 6.1, 6.3, 6.4, 6.5, 6.8, 6.10_
- [ ] 13.7 Property test: Property 14 — clock determinism
  - Generated seeds; the scenario run twice under the injected clock with `sweep_once` and
    `rebuild_once` yields identical transition sequences; every observed deadline,
    registration time and delayed dispatch equals a value read from the injected clock.
  - Tag: `// Feature: chasm-extension-archetypes, Property 14: clock determinism`
  - _Requirements: 8.8, 8.9, 9.16_

- [ ] 14. Checkpoint: `cargo clippy -p tokeira-chasm-acceptance --all-targets` clean; `cargo
  nextest run -p tokeira-chasm-acceptance` green; the workspace bar green.

### Stage 15 — Documentation and sibling amendments

- [ ] 15.1 Crate docs: `docs/crates/chasm.md` (handlers, ids, the sealed built-in rule),
  `docs/crates/chasm-activity.md` (callbacks, version target), `docs/crates/runtime.md`
  (executors, multiplexer, rebuild scan, outcome primitive), `docs/crates/storage.md` (the
  pointer table, backfill, retirement), `docs/crates/edge.md` (the four executors, versioned
  poll, provenance), `docs/crates/engine.md` (the builder, handle, clock, feature);
  `docs/readiness/corpus-evidence.md` no longer states that standalone activities are the
  only component; `docs/diagrams/chasm-plane.svg` updated in house style. Module and public
  item docs per root `AGENTS.md §9` with the citations named in `requirements.md`;
  `RUSTDOCFLAGS="-D warnings" cargo doc` clean.
  - _Requirements: 10.5, 10.6, 10.7_
- [ ] 15.2 Amend `.kiro/specs/scoped-worker-authorization/requirements.md`: add the sanctioned
  exception to Requirement 10.4 stating criteria 7.5–7.10 of this spec; leave criterion 10.4
  itself unchanged. (`AGENTS.md §6` snapshot before editing.)
  - _Requirements: 7.11, 10.3_
- [ ] 15.3 Amend `.kiro/specs/chasm-activity-timeouts-and-retry/tasks.md`: mark task 4.3 done by
  this spec with a DONE record; add to `.kiro/specs/v132-standalone-activities` a one-line
  reference that Requirement 5 of this spec implements standalone-activity callbacks, gated.
  - _Requirements: 10.2, 10.4_

### Stage 16 — Conformance rerun and finish

- [ ] 16.1 Rerun the functional harness's standalone-activity tier at v1.31.0 with gates at
  default (operator-invoked, per `docs/testing/functional-conformance-harness.md`); record the
  outcome in `docs/readiness/conformance.md`.
  - _Requirements: 10.1_
- [ ] 16.2 Finish: the root `AGENTS.md §10.4` bar green on the workspace; rebase once onto
  `origin/main`; PR per `§10.6`.

## Task Dependency Graph

```text
1.1 → 1.2 → 1.3 → 1.4 → 1.5 → 2
3.1 → 3.2 → 3.3 → 3.4 → 3.5, 3.6 → 4
2, 4 → 5.1 → 5.2 → 5.3 → 5.4 → 5.5
4 → 5.6 ; 4 → 5.7
5.1–5.7 → 5.8, 5.9, 5.10, 5.11 → 6
4 → 7.1 → 7.2 → 7.3 → 7.4 → 7.5 → 8
6, 8 → 9.1 → 9.2 → 9.3 → 9.4 → 9.5 → 9.6, 9.7, 9.8, 9.9 → 10
10 → 11.1 → 11.2 → 11.3 → 11.4 → 11.5 → 11.6, 11.7 → 12
12 → 13.1 → 13.2 → 13.3 → 13.4 → 13.5, 13.6, 13.7 → 14
14 → 15.1, 15.2, 15.3 → 16.1 → 16.2
```

Stages 1 and 3 are independent and may run in parallel; stage 7 depends on stage 3 only;
stage 5 depends on stages 1 and 3; everything from stage 9 on is serial.

## Notes

- **Slices for handoff.** Each stage is one implementer slice with its checkpoint as the
  acceptance test: S1 storage; S2 substrate; S3 runtime; S4 activity library; S5 edge; S6
  config and engine; S7 acceptance archetype; S8 documentation and amendments; the
  conformance rerun is operator-invoked.
- **No design choices in tasks.** Where a task names a behaviour, `requirements.md` fixes it
  and `design.md` fixes the interface; where a task names an upstream fact, the citation is
  the ground truth to read before writing. An implementer who finds the two in conflict
  stops and reports rather than choosing.
- **Reserved ids.** The activity library's ids 1–7 are explicit because outboxes persisted
  before this plan already carry 1–5; derived ids never collide with them by construction
  (the reserved range is rejected for derived ids).
- **Evaluator path stays.** Stage 7.2 registers the activity's timer handlers so its
  outboxes become bounded, but stage 5.4 keeps the evaluator serving activity executions;
  timing behaviour must not change and Property 7 is the guard.
- **Storage rules.** V069 and V070 follow `crates/tokeira-storage/AGENTS.md`: one statement
  per file, forward-only above V068, `ASYNC` index, spread-key primary key, no `CHECK`, no
  `BIGSERIAL`, no `BYTEA` in a key. The backfill is bounded per transaction and idempotent;
  the marker ends the fallback read.
- **Feature flag.** `chasm-extensions` is unstable and carries no semver promise; the bar
  runs the engine crate with and without it (stage 12).
- **Clock.** The injected clock is the CHASM plane's clock only (Requirement 8.9); tests
  replace time with the clock plus `sweep_once` and `rebuild_once`, never with sleeps.
- **Lockfile.** No external dependency changes; `Cargo.lock` changes only for the new
  workspace crate and must be committed with it (root `AGENTS.md §10.3`).
