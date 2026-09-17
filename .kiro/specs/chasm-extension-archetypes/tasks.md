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

- [x] 5.1 New `chasm/executor.rs`: `SideEffectExecutor` trait and `DispatchMultiplexer`
  implementing `DispatchSink`, duplicate task type rejected at registration, unknown task
  type at dispatch logged and left pending. Keep `CollectingDispatchSink` for tests.
  - _Requirements: 3.1, 3.2, 3.13_

  **DONE (2026-09-16):** The executor contract requires idempotent effects. The multiplexer
  rejects duplicate ids and logs failed or unknown dispatches without failing an already
  committed transition. Its two-executor/unknown-type regression passes; bootstrap keeps
  `ActivityDispatchQueue` until stage 9.

- [x] 5.2 Close-time validation: replace `RetainAllValidator` at `engine.rs:655` and `:734`
  with `RegistryOutboxValidator`; return `ChasmError::UnknownTaskType` before any persist
  when a staged task has no entry; implement `resolve_task` on `TransitionContext` and
  honour it at close.
  - _Requirements: 1.10, 1.11, 1.12, 3.2_

  **DONE (2026-09-16):** Start/update and generic transitions validate final root bytes
  through the registry. `UpdateRequest.resolved`, typed context draining and journaled
  `NodeTree::resolve_task` make explicit resolutions durable. Regression tests verify
  unknown-task atomic rejection, idempotent removal and before-image rollback.

- [x] 5.3 `ChasmEngine::apply_side_effect_outcome(target, task_type_id, task_id, outcome) ->
  OutcomeApplied`: load, `NotHeld` if the outbox lacks the id, `ExecutionMissing` if absent,
  otherwise `Registry::apply_outcome` with `resolve_task(task_id)` in the same transition,
  reload-and-rerun on `Conflict` up to `max_commit_retries`.
  - _Requirements: 3.9, 3.10, 6.6, 6.7, 6.9_

  **DONE (2026-09-16):** Outcome handlers run inside bounded CAS retries; the engine
  resolves the exact held task in the same commit. Missing, mismatched and duplicate
  deliveries are inert, and handler errors persist nothing. `register_root` now supplies
  visibility, search-attribute and lifecycle adapters; the accepted lifecycle addendum
  is covered by roots that close while returning no visibility snapshot.

- [x] 5.4 Sweeper: `ChasmTimerSweeper::new(engine)` + `with_evaluator(archetype_id,
  evaluator)`; in `sweep_once`, use the evaluator when one is installed for the execution's
  archetype (unchanged behaviour), otherwise execute due, valid pure tasks through
  `Registry::execute_pure` in one fenced transition, resolving each, and re-arm to the
  earliest remaining deadline. Update the engine bootstrap call site.
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.8, 2.9, 2.10_

  **DONE (2026-09-16):** The sweeper routes by root archetype. The installed activity
  evaluator retains its path; other roots execute due tasks in deadline/id order with
  validation against each preceding mutation, resolution, rollback and conflict retry.
  Newly staged tasks wait for the next pass, and the surviving earliest timer is re-armed.

- [x] 5.5 New `chasm/rebuild.rs`: `OutboxRebuildScanner::rebuild_once` over
  `scan_current_executions(Running)` in deterministic order: set the armed timer to the
  outbox's earliest pure deadline; hand every pending side-effect task whose validator holds
  and whose `fire_at` has elapsed to the dispatch sink. Add `CHASM_REBUILD_INTERVAL` beside
  `VISIBILITY_REPAIR_INTERVAL` and a `spawn_outbox_rebuild` helper mirroring
  `spawn_visibility_repair`.
  - _Requirements: 2.4, 2.5, 2.7, 3.4, 3.5, 3.15_

  **DONE (2026-09-16):** Ordered 500-pointer pages reconstruct timers and due validated
  effects from committed roots before adapter startup and every 30 seconds thereafter.
  Visibility repair uses registry adapters. The task 9.1 queue dedupe portion moved into
  this slice: `(execution, stamp)` stays seen through pickup, failed pickups are requeued,
  and terminal/deleted observations forget entries. Queue and paged rebuild tests pass.

- [x] 5.6 Extract `invoke_nexus_callback(client, config, url, header, completion, links)` from
  `deliver_completion_callback` in `publisher.rs`; the workflow path calls it; behaviour and
  its `components/callbacks/nexus_invocation.go @ v1.31.0` citation unchanged. Make
  `nexus_completion_backoff` reachable from the edge (`pub`).
  - _Requirements: 5.7_

  **DONE (2026-09-16):** The shared invocation preserves case-insensitive token lookup,
  deterministic duplicate-header selection, system-URL resolution and completion links.
  Workflow retry/outcome recording remains in the publisher. Completion config and
  backoff remain available through the existing runtime exports.

- [x] 5.7 `TypedEngine<C>` owns `Arc<ChasmEngine>` (drop the lifetime); update the bridge's
  construction sites.
  - _Requirements: 8.6_

  **DONE (2026-09-16):** The typed engine owns an `Arc`; activity bridge construction
  sites and typed-engine fixtures use shared ownership and registry-backed handlers.

- [x] 5.8 Property test: Property 3 — pure-task execution model
  - Generated staged pure tasks with deadlines and monotone clock sequences against a
    reference model; each executed once, in deadline order, never early; run with the
    injected clock and `sweep_once` over the in-memory repository.
  - Tag: `// Feature: chasm-extension-archetypes, Property 3: pure-task execution model`
  - _Requirements: 2.1, 2.2, 2.3, 2.6, 2.9, 2.10_

  **DONE (2026-09-16):** Property 3 passes 128 generated deadline/expiry/monotone-clock
  traces against an independent ordered model over the runtime's own test root.

- [x] 5.9 Property test: Property 4 — timer rehydration round-trip
  - Generated committed states and crash points; rebuild over the same repository re-arms
    the model's earliest deadline; executed tasks never re-execute.
  - Tag: `// Feature: chasm-extension-archetypes, Property 4: timer rehydration round-trip`
  - _Requirements: 2.4, 2.5, 2.7_

  **DONE (2026-09-16):** Property 4 passes 128 generated commit/crash/restart sequences,
  reconstructing the model's earliest deadline over the same repository and proving
  consumed tasks never execute again.

- [x] 5.10 Property test: Property 5 — dispatch derived from state
  - Generated committed states and in-memory losses; `rebuild_once` hands the sink exactly
    the model's pending effects in deterministic order; re-executing a `CollectingDispatchSink`
    counterpart of each shipped executor with an unchanged task is a no-op.
  - Tag: `// Feature: chasm-extension-archetypes, Property 5: dispatch derived from state`
  - _Requirements: 3.1, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.15_

  **DONE (2026-09-16):** Property 5 passes 128 generated pending-effect sets across
  reversed insertion order, paged scans and lost in-memory state. Unchanged scans yield
  the same ordered set; a fake idempotent executor records each task's effect once.

- [x] 5.11 Property test: Property 6 — outcome application fence
  - Generated delivery sequences (duplicates, after-drop, under injected conflicts);
    `on_outcome` applies at most once; other deliveries leave the node bytes unchanged.
  - Tag: `// Feature: chasm-extension-archetypes, Property 6: outcome application fence`
  - _Requirements: 3.9, 3.10, 6.6, 6.7, 6.9_

  **DONE (2026-09-16):** Property 6 passes 128 generated delivery traces covering
  duplicates, dropped/missing tasks, mismatched types and injected conflicts through
  retry exhaustion. Only held tasks mutate the root, at most once per task id.

- [x] 6. Checkpoint: `cargo clippy -p tokeira-runtime --all-targets` clean; `cargo nextest run
  -p tokeira-runtime` green including every existing `chasm` and `publisher` test.

  **DONE (2026-09-16):** All six root finishing-bar commands pass (`--locked` where
  applicable): nightly fmt, workspace lint/check, 3,382 nextest tests passed in the
  parallel run (two existing SDK integration tests skipped), workspace doctests and
  documentation with warnings denied. The focused CHASM/activity suite passes 168 tests,
  including Properties 3–6 at 128 cases each. Nextest flagged one passing callback test
  as leaky; its isolated rerun passed without the flag. Live DSQL and the operator-invoked
  functional corpus were not run; this slice changes no storage, and the corpus rerun
  remains task 16.1. The changed fragment renders in the `0.4.0` dry run.

### Stage 7 — Activity library: handlers, callbacks, version target (`tokeira-chasm-activity`)

- [x] 7.1 `ActivityState` fields 39 `version_target` and 40 `callbacks`; messages
  `DeploymentVersionTarget`, `ActivityCallback`, `NexusTarget`, `InternalTarget` as in
  `design.md`; `lifecycle_for` unchanged. Cite `activity.go:111-113 @ v1.32.0` at the field.
  - _Requirements: 5.5, 7.1_

  **DONE (2026-09-16, stage 7):** Field 39 reuses the substrate's version target;
  field 40 stores attach-ordered callbacks, repeated encoded links and deterministic
  BTreeMap Nexus headers, with opaque ComponentRef/TaskId codecs documented for Internal
  targets. Proto round trips cover both variants and legacy bytes omit both fields.
  `lifecycle_for` is unchanged; `lifecycle_of` retains Running until all callbacks settle,
  shared by Lifecycle and visibility. Tests cover every activity status with no callbacks,
  pending callbacks and all-settled callbacks, plus terminal status/close-time visibility.
- [x] 7.2 Register the existing task types as reserved handlers in `ActivityLibrary::register`
  (validators = the crate's existing validators; `execute` = the existing `apply` events).
  Existing timeout behaviour is unchanged because the evaluator path remains installed
  (stage 5.4).
  - _Requirements: 1.13, 2.8_

  **DONE (2026-09-16, stage 5):** Pulled forward because registry validation at close
  would otherwise reject every existing activity task. Reserved ids 1–5 are registered;
  all timer handlers and the unchanged evaluator share `timeout_event`. The accepted
  validator addendum retains start-to-close/heartbeat timers during `CancelRequested`,
  rejects stale stamps and superseded heartbeat anchors, and stages a replacement on
  every positive-timeout heartbeat. `started_time_nanos` is the per-attempt start (set by
  `Started`, reset on retry); the anchor uses the later of it and the last heartbeat.
  Unit tests cover Started/CancelRequested, stale stamps, supersession, bounded
  replacement and evaluator-equivalent events, grounded in
  `chasm/lib/activity/activity_tasks.go:166–168,227–247` and
  `chasm/lib/activity/activity.go:576–585 @ v1.31.0`. The slice's changed fragment covers it.
- [x] 7.3 Callback state machine: events `CallbacksAttached`, `CallbackAttempted`,
  `CallbackRetryDue`; terminal transitions set `STANDBY → SCHEDULED` and stage one
  `DeliverCallback` per callback (`activity.go:421-426 @ v1.32.0`); `DeliverCallbackHandler`
  and `CallbackRetryHandler` with ids 6 and 7; attempt recording per the design's state
  machine with backoff from the shared policy; the attach rules: closed →
  FAILED_PRECONDITION, over cap → FAILED_PRECONDITION with the upstream message, ids
  `<request_id>-<idx>`, registration time from the context clock (`activity.go:429-475 @
  v1.32.0`).
  - _Requirements: 5.4, 5.5, 5.6, 5.8_

  **DONE (2026-09-16, stage 7):** Pure attachment derives ids/time and preserves upstream
  empty-batch, closed-status, cap-before-upsert and replacement behavior. Added the
  default-2000 config cap and message-preserving FailedPrecondition error/edge mapping.
  All five terminal transitions schedule Standby callbacks; reserved handlers 6/7 validate
  callback state and attempt, record outcomes and retry deadlines, and stage the next task.
  The exported postcard outcome envelope carries executor-computed deadlines; pure retry
  helpers support stage 9's evaluator without changing activity timeout helpers. Exact
  errors, task codecs, stale/invalid states, absorption and original close time are tested.
  Ground truth: `chasm/lib/activity/activity.go:421–475` and
  `chasm/lib/callback/{statemachine.go,component.go,tasks.go,config.go} @ v1.32.0`.
- [x] 7.4 `validate_and_normalize` gains `callbacks` and `version_target`; the `Internal`
  wire variant is never constructed here (the edge rejects it); an `InternalTarget` is
  accepted only through the executor path. `ActivityRequest` documents both fields.
  - _Requirements: 5.2, 5.3, 6.1, 7.1_

  **DONE (2026-09-16, stage 7):** ActivityRequest documents D1/D2 and validates the
  deployment/build pair and callback target shape, naming invalid callback indices.
  Internal plain event inputs are accepted here; stage 9 enforces executor-only provenance
  at edge admission and rejects the public wire variant. The edge request still supplies
  an empty callback list and no version target. Existing timeout normalization is unchanged.
- [x] 7.5 Property test: Property 10 — callback delivery state machine
  - Generated terminal transitions and per-callback outcome sequences against the kernel
    callback model's state graph; attempts counted once; next attempt time equals the shared
    backoff; non-retryable is terminal.
  - Tag: `// Feature: chasm-extension-archetypes, Property 10: callback delivery state machine`
  - _Requirements: 5.6, 5.7, 5.8_

  **DONE (2026-09-16, stage 7):** 128 generated cases in `callbacks/tests.rs` cover 1–5
  mixed-target callbacks, all five legal terminal paths and 0–4 retries followed by pending,
  success or permanent failure. The model is the upstream callback graph at v1.32.0;
  assertions cover exactly staged tasks, attempts, stale fences, failures, settlement,
  lifecycle and supplied retry deadlines recorded verbatim. Shared-backoff calculation is
  the executor's responsibility in stage 9, not performed by the pure component.

- [x] 8. Checkpoint: `cargo clippy -p tokeira-chasm-activity --all-targets` clean; `cargo
  nextest run -p tokeira-chasm-activity` green; every existing statemachine test unchanged.

  **DONE (2026-09-16, stage 7):** Locked check and all-target Clippy pass for activity,
  substrate, edge and runtime. Their focused nextest suite passes all 1,232 tests, including
  Property 10, extension Properties 1–6 and visibility repair Property 14. Existing activity
  tests retain their state/task assertions and now also assert callbacks remain absent and
  lifecycle equals `lifecycle_for(status)`. The full workspace bar passes: fmt, lint,
  check, nextest (3,396 passed, 2 skipped; one subprocess-leak diagnostic in an existing
  architecture test), doctests and warnings-denied docs. The unchanged visibility
  Properties 12–14 all pass. Live DSQL and the functional Go corpus were not run for this
  pure-library slice; executor/wire integration remains stage 9.

### Stage 9 — Edge: executors, versioned queue, provenance, RPC surface (`tokeira-edge`)

- [x] 9.1 `ActivityDispatchExecutor` (task type 1) wrapping the queue: entries carry
  `target`; a served-stamp set makes re-execution a no-op; `ActivityBridge::with_dispatch_executor`
  replaces `with_dispatch_queue`; delete the `DISPATCH_TASK_ID`-only `DispatchSink` impl.
  - _Requirements: 3.11, 3.14, 7.3_

  **DONE (stage 9):** `ActivityDispatchExecutor` reads the root target without changing the
  persisted DispatchTask payload. The queue sink is removed; bootstrap and rebuild share the
  late-bound multiplexer. `executor_dedupes_before_and_after_pickup` proves one delivery
  across replay.

- [x] 9.2 `StartActivityExecutor` (`chasm.start_activity`): map every `StartActivityTask`
  field onto the bridge's start path with request id = the staging task id, attach the
  `InternalTarget { component_ref, task_type_id, task_id }`, carry `version_target`.
  `DeliverCallbackExecutor` (task type 6) branches on Nexus/Internal: the first calls
  `invoke_nexus_callback`, the second applies the activity's terminal `TaskOutcome` to the
  target. Both record the attempt through `apply_side_effect_outcome` on the activity's
  held task; missing targets fail permanently, exhausted commit retries back off.
  - _Requirements: 3.12, 5.7, 5.8, 6.3, 6.4, 6.5, 6.8, 6.10_

  **DONE (2026-09-16):** All four roles execute through three weak-engine executors. Tests
  cover idempotent staged starts and field mapping, permanent start rejection classes,
  Internal applied/already-applied/missing/rejected/conflict outcomes, Nexus first-payload
  success, shared backoff/limits and all three namespace-cache results. The executor shares
  the edge cache; absent/tombstoned namespaces omit only the back-link, cache errors retry.
  Accepted addenda: TaskId codecs live in activity callbacks (round-trip tested, no edge
  postcard dependency); every start uses atomic `TypedEngine::start_with`, so rejected
  attachment persists nothing. Concurrent same-request create conflicts return the winner;
  timer hints are armed before nested dispatch so a newer callback retry survives. Dedicated
  regression tests cover both hazards.

  Accepted storage addendum (2026-09-16): the creating transaction carries the admission
  expectation (absent or the superseded run/epoch), fences the archetype pointer and rolls
  back nodes on a miss. The engine reloads the pointer/live root and re-evaluates policy up
  to its retry bound, returning the same-request winner with `created: false`. In-memory
  and env-gated live DSQL tests cover paired creates, stale superseding fences and rollback;
  engine pairs cover idempotency, conflict/reuse verdicts and superseding starts. A bounded
  retry test verifies pristine initializer input and one post-commit dispatch. This correction
  lands in its own commit and has a third `fixed` fragment with a 139-character body.

- [x] 9.3 Poll admission: `poll_activity_task_waiting(task_queue, identity, admitted:
  Option<&DeploymentVersionTarget>)` selects the first due entry whose target equals
  `admitted`; the gRPC branch at `grpc/workflow_service.rs:976-988` passes the scoped
  worker's exact version (`Some`) or `None`; an untargeted entry never matches `Some`.
  - _Requirements: 7.3, 7.4, 7.5, 7.6, 7.7, 7.8_

  **DONE (stage 9):** Queue selection, due deadlines and waiting admission use exact
  optional target equality. Authenticated worker scope supplies the target; raw unscoped
  deployment fields cannot admit targeted work
  (`unscoped_poll_fields_do_not_admit_targeted_work_and_internal_callbacks_are_hidden`).

- [x] 9.4 Token and provenance: `ProtoTaskToken` field 15 `version_target`; when a targeted
  task is served, write the token digest to `worker_task_provenance` with origin
  `{namespace, normal task queue, task_class: Activity, deployment, build_id}`; call
  `authorize_scoped_task_token` on the standalone completed / failed / canceled / heartbeat
  paths before the bridge.
  - _Requirements: 7.9, 7.10_

  **DONE (stage 9):** Field 15 round-trips targets and absent targets preserve literal v1.31
  token bytes. Scoped pickup writes expiring provenance; all four token handlers use worker
  preflight followed by provenance authorization. Completion/failure/cancellation consume
  evidence; heartbeat retains it. Wrong-release calls leave state and VT unchanged.

- [x] 9.5 RPC surface: with the gate on, `start_activity_execution` validates
  `completion_callbacks` through `validate_callback_specs`, rejects `Internal` with
  INVALID_ARGUMENT "unsupported callback variant: *common.Callback_Internal_" (`activity.go:466-467 @ v1.32.0`) and passes
  the rest to the library; with the gate off the field is never read.
  `describe_activity_execution` maps persisted Nexus callbacks to `activity.v1.CallbackInfo`
  through the workflow path's `CallbackInfo` mapping and never lists internal targets. The
  edge never populates `version_target`.
  - _Requirements: 5.1, 5.2, 5.3, 5.9, 5.10, 6.2, 7.2_

  **DONE (stage 9):** Default-false ActivityConfig gate, shared per-callback validation and
  component-owned cap are wired. Headers and links round-trip verbatim; public starts never
  set a target and describe hides Internal callbacks. The evaluator folds due callback
  retries and returns the minimum live deadline, including after activity completion.

- [x] 9.6 Property test: Property 7 — activity dispatch equivalence
  - Generated standalone-activity lifecycle sequences; the executor-backed bridge produces
    the same worker-visible task sequence and describe outcomes as a recorded pre-change
    model (`CollectingDispatchSink` snapshot of the old queue semantics).
  - Tag: `// Feature: chasm-extension-archetypes, Property 7: activity dispatch equivalence`
  - _Requirements: 2.8, 3.11, 3.13, 3.14_

  **DONE (stage 9):** `dispatch_matches_legacy_lifecycle_scripts` runs 128 generated scripts
  against executor and test-only legacy sinks, comparing served tokens/describes after every
  step and inserting a rebuild at a generated point.

- [x] 9.7 Property test: Property 9 — callback attachment model
  - Generated callback lists with the gate off and on; gate off ≡ no-callback start; gate on
    matches the v1.32.0 attach model and describe lists exactly the Nexus callbacks.
  - Tag: `// Feature: chasm-extension-archetypes, Property 9: callback attachment model`
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.9, 5.10, 6.2_

  **DONE (stage 9):** `callback_attachment_matches_wire_model` runs 128 generated gRPC
  lists, checking cap rollback, exact Internal error, ids, standby state, order,
  activity-closed trigger, links and headers; gate-off generated starts independently check
  persisted bytes and describes.

- [x] 9.8 Property test: Property 12 — versioned admission
  - Generated queues mixing untargeted and targeted entries and generated pollers
    (unscoped, scoped to random versions); served target always equals the admitted version;
    completion with a mismatched targeted token is denied.
  - Tag: `// Feature: chasm-extension-archetypes, Property 12: versioned admission`
  - _Requirements: 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10_

  **DONE (stage 9):** `queue_admission_matches_reference_model` runs 128 generated
  queues/poll sequences. `scoped_wire_tokens_reject_other_releases` runs 128 real scoped
  pickups and completion/failure/cancellation/heartbeat attempts, proving wrong-version
  denial before mutation and correct provenance lifetime.

- [x] 9.9 Property test: Property 17 — gate-off invariance
  - Differential test: the recorded v1.31.0 standalone-activity request set, plus generated
    starts carrying `completion_callbacks`, replayed with gates at default against the
    pre-change and post-change bridge; responses and describes byte-identical.
  - Tag: `// Feature: chasm-extension-archetypes, Property 17: gate-off invariance`
  - _Requirements: 5.1, 10.1_


  **DONE (stage 9):** The first commit records 42 fixed-clock responses from the pre-change
  engine, including bridge lifecycle, idempotent gRPC repeats and fresh gRPC starts. Fresh
  UUIDs normalize to a placeholder; the approved atomic-start correction normalizes public
  state_transition_count and the opaque token VT while retaining the execution key. The
  golden preserves every other byte.
  `atomic_wire_start_reports_one_transition_and_advances_on_updates` checks 1 after start,
  idempotent stability and later increments; the existing long-poll tests remain guards.
  `gate_off_generated_starts_ignore_callbacks` adds 128 generated cases. The count
  correction has a separate fixed changie fragment alongside the slice's added entry.

- [x] 10. Checkpoint: `cargo clippy -p tokeira-edge --all-targets` clean; `cargo nextest run
  -p tokeira-edge` green including every existing standalone-activity and scoped-worker test.

  **DONE (2026-09-16):** The full workspace bar passes: nightly formatting, lint with
  zero warnings, workspace check, nextest (3428 passed, 2 skipped), doctests (1 passed,
  20 ignored), and documentation with warnings denied. Golden and all existing standalone,
  scoped-worker, callback, long-poll and runtime tests pass, including the storage/engine
  race regressions and nested callback timer regression. The earlier known backlog property
  flake passed both its serial rerun and the final full suite. The new live DSQL variant
  compiles but skips its database work because `TOKEIRA_DSQL_TEST_DATABASE_URL` is absent.
  Changelog dry-run and diff checks pass. No dependency, lockfile, schema or kernel changes.

### Stage 11 — Config and engine: gate, builder, handle, clock, seeding (`tokeira-config`, `tokeira-engine`)

- [x] 11.1 `CompatibilityConfig::enable_standalone_activity_callbacks` (default false, doc
  citing `activity.enableCallbacks @ v1.32.0`); `TokeiraConfig::validate` emits
  `ValidationError::Field` when callbacks are on and standalone activities off; `field!`
  catalog entry and fixture line; the two catalog tests pass.
  - _Requirements: 5.11_

  **DONE (stage 11):** The default-false key, catalog and fixture are wired to
  `ActivityConfig.enable_callbacks`. The four gate combinations round-trip TOML, and the
  invalid combination names the exact field and prerequisite. All 68 config tests pass.

- [x] 11.2 Feature `chasm-extensions` in `crates/tokeira-engine/Cargo.toml` and the
  `pub mod chasm` re-export module under it.
  - _Requirements: 8.1, 8.10_

  **DONE (stage 11):** The empty opt-in feature gates the public builder, typed handle and
  `chasm` re-exports, including `SearchAttrKind` and `TaskOutcome`; its instability is
  documented. No dependency or lockfile changes.

- [x] 11.3 `EngineBuilder` and `Engine::builder(config)`: `library::<L>()`,
  `side_effect_executor`, `clock`, `build` in the design's order (built-ins, `seal_built_ins`,
  extensions, freeze, backfill until zero, `distinct_archetypes` fail-closed check,
  empty multiplexer, `ChasmEngine::with_clock`, bridge and three built-in executors then
  extensions, search-attribute seeding, `rebuild_once`, spawns). Private `ChasmExtensions`
  threads through both existing start paths; the public start supplies an empty value.
  `Engine` gains unconditional `chasm_engine` and `registry` fields.
  - _Requirements: 1.8, 1.9, 2.4, 3.4, 8.2, 8.3, 8.4, 8.5, 8.8, 8.9_

  **DONE (stage 11):** The private extensions value feeds both existing storage paths.
  Built-ins are sealed before extensions; executors precede seeding and rebuild. The optional
  clock reaches CHASM while workflow time stays unchanged. Named registration, archetype,
  attribute and outbox errors survive embedded error redaction. Unserviceable outboxes carry
  an explicit cause, never a sentinel task id. Integration tests prove reserved-name and
  duplicate-executor rejection and activity scheduling at the supplied clock.

- [x] 11.4 `Engine::chasm::<C>()` returning `TypedEngine<C>`, error naming `C::FQN` when
  unregistered.
  - _Requirements: 8.6, 8.7_

  **DONE (stage 11):** Registered roots start and read through the typed handle; an absent
  root names its FQN. The built-in activity handle remains available with no extension
  libraries. The approved builder fixture defines a prost-backed root and no task types; stage
  13 retains the extension-task and Property 14 clock proofs.

- [x] 11.5 Search-attribute seeding: at start for every namespace and inside
  `seed_predefined_search_attributes` for later namespaces, register each declared
  definition by resolving its existing type before `register_attr`; a mismatch aborts start with
  namespace, key and both types.
  - _Requirements: 1.8, 1.9_

  **DONE (stage 11):** Projection keys resolve before registration, preserving matching ids
  and rejecting mismatched types. Startup seeds every listed namespace; the operator wrapper
  carries the same declarations for later namespaces. Real-start and operator tests cover both
  paths, including later-namespace conflicts.

- [x] 11.6 Property test: Property 13 — fail-closed build
  - Generated storage pointer sets over archetype ids and generated registered library
    sets drive `check_registered_archetypes` for 128 cases; admission succeeds iff every
    stored id is registered; the error names the first unregistered id and its count.
    A focused real-start example proves the wiring without building engines per case.
  - Tag: `// Feature: chasm-extension-archetypes, Property 13: fail-closed build`
  - _Requirements: 8.4, 8.5, 8.7_

  **DONE (stage 11):** `stored_archetypes_require_registered_libraries` runs 128 generated
  storage/registry cases against the pure check. One real-start example rejects an
  unregistered persisted root with its count. The seeding example independently accepts
  persisted state with the matching library; generated cases never build engines.

- [x] 11.7 Property test: Property 16 — search-attribute registration
  - Generated definitions and pre-existing registry states across restarts and namespace
    creations drive the seeding helper for 128 cases; seeding is idempotent and fails iff a
    declared key exists with another type. A real-start example proves mismatch propagation.
  - Tag: `// Feature: chasm-extension-archetypes, Property 16: search-attribute registration`
  - _Requirements: 1.6, 1.8, 1.9_

  **DONE (stage 11):** `declared_attributes_seed_idempotently` runs 128 cases across all seven
  kinds, three existing namespaces and a fresh namespace. It checks ids/types on repeats and
  exact mismatch details; a real-start example checks propagation of a pre-registered
  conflicting type.

  **Review corrections (stage 11):** The live-suite table and storage filter include the
  stage-9 concurrent-start test. A superseded reused run id now reports a business-id
  conflict; current-run collisions reload policy. Both generic paths reject anomalous
  held work on a closed root while stale delivery stays inert. The existing internal
  callback completion test confirms that pending activity callbacks still apply. Rebuild
  checks all persisted tasks before publishing work, isolates missing data/unknown
  handlers/undecodable payloads with an explicit cause and count, and continues healing
  healthy executions. Runtime regressions cover all three causes and both conflict/guard
  branches; startup refuses a nonzero unserviceable count while periodic passes log it.

- [x] 12. Checkpoint: the full root `AGENTS.md §10.4` bar on the workspace, with
  `--features chasm-extensions` added to the lint, check, test and doc steps for
  `tokeira-engine`.

  **DONE (2026-09-16):** Workspace nightly formatting, `cargo lint --locked` (zero
  warnings), `cargo check --workspace --locked`, nextest (3438 passed, 2 skipped),
  doctests (1 passed, 20 ignored), and documentation with warnings denied all pass.
  The workspace run marked `tui::tests::non_terminal_mode_omits_spinner` leaky; its isolated
  serial nextest rerun passed cleanly. No backlog timeout occurred. The four engine steps
  with `chasm-extensions` also pass: all-target clippy with zero warnings, check, nextest
  (108 passed), and documentation with warnings denied. The documented storage filter
  selects all three CHASM tests; database work is skipped because
  `TOKEIRA_DSQL_TEST_DATABASE_URL` is unset. Test links emit existing native-library macOS
  deployment-target warnings; no toolchain or cache settings changed. Changelog dry-run and
  diff checks pass. The four review corrections have separate commits; the slice carries
  one added and one fixed fragment. Requirements, dependencies and Cargo.lock are unchanged.

### Stage 13 — Acceptance archetype (`crates/tokeira-chasm-acceptance`, publish = false)

- [x] 13.1 Crate skeleton in the activity crate's shape: `ResourceState` proto, `Resource`
  root component, `AcceptanceLibrary` registering the component, `ReconcileHandler`
  (`SideEffectTaskHandler` over `StartActivityTask`), `RetryHandler` (`PureTaskHandler` over
  `RetryTimer`), and `DeploymentStatus`, `DesiredGeneration`, `ObservedGeneration` search
  attributes. Dev-dependency on `tokeira-engine` with `chasm-extensions`; no dependency on
  the activity crate's test modules (assert with a workspace test).
  - _Requirements: 9.1, 9.17_

  **DONE (2026-09-16):** Added the unpublished workspace crate, prost resource/operation
  state, root registration, typed visibility attributes and the two handlers. Library
  dependencies remain substrate-only; the source-isolation test checks that path includes
  stay inside this crate and the activity library is a feature-free dev-dependency.
  Cargo.lock adds only this crate's package entry; no new resolved packages.

- [x] 13.2 Pure command decisions in the library, composed with the typed handle by
  `tests/support/commands.rs`: create with request id and immutable create digest
  (idempotent repeat, conflicting repeat, second request id → already-started), update with
  expected generation (match / mismatch) and separate desired digest (field 11), read.
  - _Requirements: 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_

  **DONE (2026-09-16):** Pure create/repeat/update/read decisions live in the library;
  the embedding test application owns async typed-handle composition. Generation checks
  abort the transaction on mismatch. `desired_digest` is field 11 and updates preserve
  `create_digest`, so a create retry still compares against the original input.

- [x] 13.3 Reconcile: an update raising desired above observed stages `StartActivityTask`
  with a version target and the internal callback; `on_outcome` advances observed on
  completion or records the failure and stages `RetryTimer` on terminal failure;
  `RetryHandler` re-stages the start; history bounded to 8 entries by the appending
  transition.
  - _Requirements: 9.9, 9.12, 9.13_

  **DONE (2026-09-16):** Every staging uses its generation/attempt activity id, persisted
  queue/target, one encoded ReconcileInput payload and activity maximum-attempts one.
  Completion advances only the captured generation and stages any newer desired input;
  unsuccessful outcomes retain structured failures and stage the component's capped
  exponential retry. Unit tests cover stale validators, all outcome kinds, bounded
  history, older-generation completion and long-lived termination behavior.

- [x] 13.4 Two integration harnesses: `tests/acceptance.rs` uses `Engine::builder(in-memory config)
  .library::<AcceptanceLibrary>().clock(virtual)`; a scoped worker of the target version, an
  unscoped worker and a scoped worker of another version polling through the in-process
  gRPC service; admission proofs; completion and failure paths; seeded keys accepted by
  workflow visibility without listing extension rows. `tests/runtime.rs` uses a shared
  node store, both libraries, real executors, activity evaluator and single-pass scanners:
  restart proof A (drop after the activity's terminal commit, rebuild over the same `Arc`
  repository, no request issued, observed advances); restart proof B (drop with a retry
  timer armed, rebuild, timer fires at its deadline); resource visibility through the
  real projection adapter/store. Embedded snapshots omit CHASM state; closing that gap
  is a separate integration-seat item.
  - _Requirements: 9.10, 9.11, 9.14, 9.15, 9.16_

  **DONE (2026-09-16):** The public builder scenario uses the real policy authenticator,
  scoped v1/v2 workers and an authenticated unscoped worker. Only v1 receives the task;
  v2 cannot complete its token. Completion/failure return only after the internal outcome
  reaches the parent, with public callbacks disabled. Paused Tokio time expires empty
  transport polls while CHASM uses its injected clock. Restart A delivers pending work
  without a request; restart B re-arms the retry, does nothing before its deadline and
  starts a fresh try-2 run exactly at it. The builder accepts seeded query keys while
  workflow listing stays empty; the real projection adapter/store returns the resource's
  typed attributes. Projection is an approved workspace-pinned dev-dependency.

- [x] 13.5 Property test: Property 15 — acceptance reference model
  - Generated command sequences against a generation reference model; responses equal the
    model's at every step.
  - Tag: `// Feature: chasm-extension-archetypes, Property 15: acceptance reference model`
  - _Requirements: 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_

  **DONE (2026-09-16):** 128 generated command sequences pass against an independent
  generation/idempotency model with no executors. Responses, reads, the single retained
  run and its current pointer agree, including create retries after updates.

- [x] 13.6 Property test: Property 11 — internal delivery exactly once
  - Generated crash points between the activity's terminal commit and the delivery commit;
    the target receives the outcome exactly once; a missing target marks the callback
    `FAILED` non-retryably.
  - Tag: `// Feature: chasm-extension-archetypes, Property 11: internal delivery exactly once`
  - _Requirements: 6.1, 6.3, 6.4, 6.5, 6.8, 6.10_

  **DONE (2026-09-16):** 128 cases cross completed/failed outcomes with crashes before
  delivery, between parent and callback commits, and after both. Rebuild leaves one
  parent history entry and a succeeded callback; deleting a pending target fails delivery
  non-retryably with its full key. A callback settled before deletion remains succeeded.
  A second rebuild leaves the parent unchanged and the activity outbox is empty.

- [x] 13.7 Property test: Property 14 — clock determinism
  - Generated seeds; the scenario run twice under the injected clock with `sweep_once` and
    `rebuild_once` yields identical transition sequences up to relabelling executor-minted
    run UUIDs by staged activity ids. Unknown identities fail normalization; root bytes,
    transition counts, lifecycle, deadlines, timestamps and queue order compare exactly.
    Registration/transition times equal clock readings; deadlines and delayed dispatch
    equal clock-derived anchors plus the specified timeout/backoff.
  - Tag: `// Feature: chasm-extension-archetypes, Property 14: clock determinism`
  - _Requirements: 8.8, 8.9, 9.16_

  **DONE (2026-09-16):** 128 generated scripts replay over fresh stores through the real
  executors, bridge, sweeper and rebuild. Sequences are identical up to relabelling only
  executor-minted run UUIDs by each unique staged activity id: that is the full claim
  because production starts intentionally mint random identities. Unmapped identities
  fail normalization. Root bytes/metadata, transition counts, lifecycle, clock timestamps,
  armed deadlines and actual FIFO queue entries compare exactly. A mandatory retry path
  and delayed-dispatch probe prevent vacuous clock checks. The approved owned queue
  snapshot sorts by queue name then FIFO and has an observational/no-notification test.

- [x] 14. Checkpoint: `cargo clippy -p tokeira-chasm-acceptance --all-targets` clean; `cargo
  nextest run -p tokeira-chasm-acceptance` green; the workspace bar green.

  **DONE (2026-09-16):** Focused all-target clippy is clean and acceptance nextest passes
  all 15 tests, including 128 cases each for Properties 11/14/15. The workspace bar passes:
  nightly format, `cargo lint --locked` with zero warnings, `cargo check --workspace
  --locked`, nextest (3456 passed, 2 skipped), doctests (1 passed, 20 ignored), and
  documentation with warnings denied. The four engine `chasm-extensions` checks also
  pass: all-target clippy with zero warnings, check, nextest (108 passed), and documentation
  with warnings denied. All cargo checks use `--locked`; formatting uses nightly.
  No backlog timeout or serial rerun was needed. Existing native-library macOS linker
  deployment-target warnings remain; no toolchain/cache settings changed. Changelog
  dry-run, exact lockfile comparison and diff checks pass. The internal fragment has no
  body because the repository's `skipBody: true` rejects one. Stage 15 documentation and
  sibling amendments, stage 16 corpus execution, live DSQL and embedded CHASM snapshot
  persistence remain outside this slice. Requirements are unchanged.

### Stage 15 — Documentation and sibling amendments

- [x] 15.4 Clarify Requirements 8.9 and 8.11 and the design's clock/provenance boundary:
  CHASM decisions use the injected clock; component long-poll transport deadlines and
  task-token provenance storage lifetimes use real time. Anchor targeted-pickup provenance
  expiry at real time sampled after pickup plus start-to-close, guarding non-positive
  durations and overflow through the existing denial. This correction precedes
  documentation tasks 15.1–15.3 so they describe the corrected contract. Storage behavior
  is unchanged.
  - _Requirements: 7.9, 7.10, 8.9, 8.11_

  **DONE (2026-09-16):** Requirements 8.9/8.11 and the design distinguish CHASM decision
  time from transport waiting and provenance storage lifetime. The targeted-pickup path
  samples real time after pickup, then adds start-to-close with checked arithmetic.
  The approved refinement uses serving time, because admission time can precede a waiting
  poll by more than the timeout. Invalid durations reach the existing provenance denial
  and metric. This lands before 15.1–15.3 so the documentation describes the corrected
  contract. The handoff's reported store disagreement was checked and is not present:
  both memory and DSQL `WorkerTaskProvenanceStore::get` filter expired rows on real time;
  DSQL's unfiltered private `load_any` is wrapped by that public read filter. Storage code
  and dependency manifests remain unchanged.

- [x] 15.5 Prove the provenance anchor beside the scoped-worker tests: a pickup with CHASM
  time far from wall time records serving-based expiry; a stale admission instant cannot
  spend the lifetime, and non-positive start-to-close/overflow are denied. Remove the
  acceptance scenario's wall-time freeze and use a simulated clock;
  retain all scoped admission, completion and failure assertions. Run the workspace bar
  and the four engine `chasm-extensions` checks.
  - _Requirements: 7.9, 7.10, 8.8, 8.9, 8.11, 9.10, 9.11_

  **DONE (2026-09-16):** Both simulated-clock tests first failed with PermissionDenied
  against the pre-fix computation. After correction, the real scoped pickup registers
  readable provenance anchored to serving time while CHASM remains at 1,000 seconds after
  the epoch; zero and negative timeouts retain the standard denial. A deterministic
  boundary test covers serving after stale admission, non-positive durations and timestamp
  overflow. The acceptance scenario's wall-time freeze is gone: its atomic clock starts
  at 100 seconds after the epoch, and matching-release admission, wrong-release denial,
  completion and failure pass unchanged.

  Edge check/all-target clippy and all 558 edge tests pass; all 15 acceptance tests pass.
  The workspace bar passes: nightly format, lint with zero warnings, check, nextest
  (3458 passed, 2 skipped), doctests (1 passed, 20 ignored), and documentation with warnings
  denied. The engine passes nextest without the feature (106 tests) and all four
  `chasm-extensions` steps: all-target clippy with zero warnings, check, nextest (108 tests),
  and documentation with warnings denied. Cargo steps use `--locked`; no backlog timeout
  or serial rerun occurred. Existing native-library macOS linker warnings remain; no
  toolchain/cache settings changed. Changelog dry-run and diff checks pass; the fixed
  fragment is 149 characters. No dependency or storage changes. Live DSQL, stage 15.1–15.3
  documentation/sibling amendments and the stage 16 corpus rerun remain outside this slice.

- [x] 15.1 Crate docs: `docs/crates/chasm.md` (handlers, ids, the sealed built-in rule),
  `docs/crates/chasm-activity.md` (callbacks, version target), `docs/crates/runtime.md`
  (executors, multiplexer, rebuild scan, outcome primitive), `docs/crates/storage.md` (the
  pointer table, backfill, retirement), `docs/crates/edge.md` (the four executors, versioned
  poll, provenance), `docs/crates/engine.md` (the builder, handle, clock, feature);
  `docs/readiness/corpus-evidence.md` no longer states that standalone activities are the
  only component; `docs/diagrams/chasm-plane.svg` updated in house style. Module and public
  item docs per root `AGENTS.md §9` with the citations named in `requirements.md`;
  `RUSTDOCFLAGS="-D warnings" cargo doc` clean.
  - _Requirements: 10.5, 10.6, 10.7_
  - DONE: all six crate pages amended in place — `chasm.md` gains typed handlers, the
    stable-id scheme and the sealing rule; `chasm-activity.md` gains completion callbacks
    (including why a terminal activity stays Running until they settle) and the version
    target; `runtime.md` gains the multiplexer, the outcome primitive and the rebuild scan;
    `storage.md` gains the archetype-keyed pointer, the backfill and its marker, and why
    `chasm_current_run` is left in place; `edge.md` gains the three executors and four roles,
    the versioned poll and the provenance anchor; `engine.md` gains the builder, the typed
    handle and the clock. `corpus-evidence.md` no longer says standalone activities are the
    only component: upstream's four others remain unsupported, and registered components are
    named as Tokeira-native surface outside the v1.31.0 claim. `chasm-plane.svg` refreshed in
    house style — the boundary card becomes registration, the sweeper card gains the rebuild
    scan, the engine card names the multiplexer, and the pointer table is renamed.
    `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` is clean; the
    module and public-item docs written in stages 1–13 already carried their §9 citations.
    Also folded in the stage 14 review nit: the wall-clock pin in Property 12's harness is
    gone, so its 128 cases run under the simulated clock.
- [x] 15.2 Amend `.kiro/specs/scoped-worker-authorization/requirements.md`: add the sanctioned
  exception to Requirement 10.4 stating criteria 7.5–7.10 of this spec; leave criterion 10.4
  itself unchanged. (`AGENTS.md §6` snapshot before editing.)
  - _Requirements: 7.11, 10.3_
  - DONE: criterion 10.4 is untouched. A "Sanctioned exception to criterion 10.4" subsection
    follows Requirement 10's criteria, restating 7.4–7.10 and showing the exception is
    narrow: exact-release routing is added, and nothing 10.4 refuses becomes servable — an
    untargeted task is still withheld from a scoped worker (7.6).
- [ ] 15.3 Amend `.kiro/specs/chasm-activity-timeouts-and-retry/tasks.md`: mark task 4.3 done by
  this spec with a DONE record; add to `.kiro/specs/v132-standalone-activities` a one-line
  reference that Requirement 5 of this spec implements standalone-activity callbacks, gated.
  - _Requirements: 10.2, 10.4_
  - PARTIAL: `chasm-activity-timeouts-and-retry` task 4.3 is ticked with a DONE record naming
    `OutboxRebuildScanner` as the recovery scan that satisfies its Requirement 9, generalized
    to every registered archetype. The second half is a **cross-branch amendment that is still
    owed**: `.kiro/specs/v132-standalone-activities` is not on `main`, but it exists on the
    compatibility branch as a placeholder whose full `requirements.md`, `design.md` and
    `tasks.md` are not yet authored. Its scope is broader than callbacks — standalone
    activities on by default, start delay, paused status, the operator RPCs, activity batch
    operations — and it inherits the callback capability rather than needing to build it.
    The line it should carry: standalone-activity completion callbacks are already
    implemented behind a default-off gate by this spec's Requirement 5. The amendment belongs
    on the branch that owns the placeholder, not duplicated onto `main`, and this task stays
    open until it lands there.

### Stage 16 — Conformance rerun and finish

- [x] 16.1 Rerun the functional harness's standalone-activity tier at v1.31.0 with gates at
  default (operator-invoked, per `docs/testing/functional-conformance-harness.md`); record the
  outcome in `docs/readiness/conformance.md`.
  - _Requirements: 10.1_
  - DONE (2026-09-17): engine `28d0b3af` (merged native visibility), conformance fork
    `423ae614` and Go 1.26.2. Two consecutive fresh-server runs with wire capture each
    produced 174 pass / 0 fail / 0 native skips / 0 unfinished and the same 52 RPC/status
    pairs. Two preceding runs without wire capture had the identical passing outcomes.
    Three existing size-limit registry exclusions remain separately classified; no corpus
    body or exclusion changed. Servers booted with default policy gates; the unmodified
    upstream suite enabled standalone activities through its live override. Callbacks
    stayed default-gated. `docs/readiness/conformance.md` records the pins, counting
    convention, exclusions, binary hash, and evidence location. This is the in-memory
    public-wire corpus, not a live-DSQL or embedded-snapshot-recovery claim.
- [x] 16.2 Finish: the root `AGENTS.md §10.4` bar green on the workspace; rebase once onto
  `origin/main`; PR per `§10.6`.
  - DONE (2026-09-17): nightly formatting, workspace lint/check, workspace nextest
    (3,475 passed, 2 skipped), doctests (1 passed, 20 ignored), and warnings-as-errors
    workspace rustdoc all passed. Offline links for the edited Markdown and
    `git diff --check` passed. The evidence-only slice changes this task record, the
    readiness ledger, and an internal changelog fragment; no production code, dependency,
    lockfile, corpus, or skip-registry change. Task 15.3 remains open for its separately
    owned compatibility-branch reference.

## Task Dependency Graph

```text
1.1 → 1.2 → 1.3 → 1.4 → 1.5 → 2
3.1 → 3.2 → 3.3 → 3.4 → 3.5, 3.6 → 4
2, 4 → 5.1 ; 4 → 7.2 ; 5.1, 7.2 → 5.2 → 5.3 → 5.4 → 5.5
4 → 5.6 ; 4 → 5.7
5.1–5.7 → 5.8, 5.9, 5.10, 5.11 → 6
4 → 7.1 ; 7.1, 7.2 → 7.3 → 7.4 → 7.5 → 8
6, 8 → 9.1 → 9.2 → 9.3 → 9.4 → 9.5 → 9.6, 9.7, 9.8, 9.9 → 10
10 → 11.1 → 11.2 → 11.3 → 11.4 → 11.5 → 11.6, 11.7 → 12
12 → 13.1 → 13.2 → 13.3 → 13.4 → 13.5, 13.6, 13.7 → 14
14 → 15.4 → 15.5 → 15.1, 15.2, 15.3 → 16.1 → 16.2
```

Stages 1 and 3 are independent and may run in parallel; stage 7 depends on stage 3 only;
stage 5 depends on stages 1 and 3 and includes 7.2; everything from stage 9 on is serial.

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
- **Evaluator path stays.** Task 7.2, delivered in stage 5, registers the activity's timer handlers so its
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
