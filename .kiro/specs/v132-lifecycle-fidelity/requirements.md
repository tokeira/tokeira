# Requirements Document: v132 Lifecycle Fidelity (D6)

## Introduction

This is delta spec D6 of the Temporal `1.32.0` campaign
(`.kiro/specs/temporal-v1.32-compatibility/`, Requirement 9.1). It owns the workflow
lifecycle and response-fidelity differences between Temporal server `1.31.0` and
`1.32.0` that are visible through the public `WorkflowService` API and measured by the
`v1.32.0` functional corpus: execution lineage on start paths, continue-as-new backoff,
update-with-start admission and retry, response links, nil memo and search-attribute
omission, the workflow-pause gate and its fidelity, schedule list/describe/backfill
semantics, reset linkage, and service-error truncation.

The compatibility authority is Temporal server at tag `v1.32.0` in the reference fork
(`AGENTS.md` §8). Every behavioural claim below cites `path:line @ v1.32.0`. Corpus
assertions cite `tests/<file>.go:line @ v1.32.0`. Tokeira anchors cite the campaign
branch `compat/temporal-1.32` at `d019b605`.

Baseline evidence: `../temporal-v1.32-compatibility/reference/FINDINGS-v1.32.0.md`
(per-suite counts and owners) and the focused replays recorded in this spec's
`design.md` § Replay signatures. D6 owns these suites: `TestWorkflowTestSuite` (Tier
1.1), `TestContinueAsNewTestSuite` (3.15), `TestUpdateWithStartSuite` and
`TestUpdateWorkflowSdkSuite` (2.12), `TestScheduleV1` (5.30) and the CHASM-mode
schedule suites, `TestResetWorkflowTestSuite` and `TestWorkflowResetTestSuite` (3.17),
`TestLinksTestSuite` (5.31, shared with D5), `TestNilSearchAttributeSuite`,
`TestPauseWorkflowExecutionSuite`, `TestHttpApiTestSuite` (9.43), and the three
already-clean suites `TestWorkflowTypeEncodingSuite`, `TestPrematureEosTestSuite`,
`TestRelayTaskTestSuite`.

This spec is a fidelity delta on top of implemented features. It adds no crate, no
dependency, and no kernel transition kind. Where a change needs a new kernel command
field (continue-as-new backoff) or a new kernel-visible flag (manual schedule
actions live in the runtime, not the kernel), the design says so explicitly.

### Sibling work this spec depends on or defers to

- `.kiro/specs/kernel-pause-workflow/` (complete at `1.31.0`) implements
  pause/unpause; D6 adds the stock gate and the fidelity the corpus measures.
- `.kiro/specs/kernel-updates/`, `runtime-update-lifecycle/`,
  `api-conformance-update-lifecycle/` implement updates; D6 adds the total-updates
  limit, the update-with-start retry, and response links.
- `.kiro/specs/edge-schedule-transport/` and `api-conformance-schedule-fields/`
  implement schedules; D6 adds the `1.32.0` semantics listed in Requirements 12–15.
- `.kiro/specs/workflow-reset/`, `kernel-reset/`, `runtime-reset-replay-support/`
  implement reset; D6 adds `reset_run_id` and chain reapply.
- `.kiro/specs/edge-eager-dispatch/` implements eager dispatch; D6 records the stock
  posture and adds the server's guards.
- `.kiro/specs/api-conformance-start-fields/` implements start-field policy; D6 adds
  `first_execution_run_id` on the response paths.
- D5 (`v132-nexus`) owns the `__temporal_system` Nexus endpoint, signal-with-start
  from a workflow, and update completion callbacks. D4 (`v132-worker-deployments`)
  owns versioning-override semantics; D6 consumes one deprecated-field rule from it.
  D3 (`v132-visibility-query-converter`) owns the visibility converter; D6 owns the
  schedule list filter, which is a separate parser.

## Glossary

- **Chain_Head**: the run id recorded as `first_execution_run_id` on event 1 of a run;
  the first run of a retry, cron, or continue-as-new chain. A reset run inherits the
  base run's Chain_Head because its event 1 is replayed from the base
  (`service/history/ndc/workflow_resetter.go:501 @ v1.32.0`).
- **Start_Path**: `StartWorkflowExecution`, `SignalWithStartWorkflowExecution`, and
  the start leg of `ExecuteMultiOperation`.
- **AlreadyStarted_Failure**: the `WorkflowExecutionAlreadyStartedFailure` error
  detail carried by an `ALREADY_EXISTS` status on a Start_Path.
- **Started_Event_Ref_Link**: a `Link.WorkflowEvent` naming the run with
  `EventRef{event_id = 1, event_type = WORKFLOW_EXECUTION_STARTED}`
  (`service/history/api/link_util.go:12-31 @ v1.32.0`).
- **Request_Id_Ref_Link**: a `Link.WorkflowEvent` naming the run with
  `RequestIdRef{request_id, event_type}` (`link_util.go:34-50 @ v1.32.0`).
- **Total_Updates_Limit**: `history.maxTotalUpdates` (default 2000,
  `common/dynamicconfig/constants.go:2561-2565 @ v1.32.0`), enforced at update
  admission (`service/history/workflow/update/registry.go:438-452`).
- **Closing_Abort**: the update outcome when the workflow closes before the update is
  accepted (`multioperation/api.go:126-163 @ v1.32.0`).
- **Backlinks_Override**: `history.enableCHASMSignalBacklinks` (default `false`,
  `constants.go:3224-3231 @ v1.32.0`); gates only whether signal request ids are
  recorded in `WorkflowExtendedInfo.request_id_infos`
  (`mutable_state_impl.go:708-710, 6174-6185`).
- **Pause_Gate**: `frontend.WorkflowPauseEnabled` (default `false`,
  `constants.go:3634-3638 @ v1.32.0`), checked by `PauseWorkflowExecution` only
  (`service/frontend/workflow_handler.go:7554-7556`).
- **Manual_Action**: a schedule action produced by a backfill or an immediate trigger,
  as opposed to a spec-driven action (`service/worker/scheduler/workflow.go:750-753,
  727 @ v1.32.0`).
- **Catchup_Window**: the resolved value Temporal describes: default 365 days when
  unset or non-positive, at least 10 seconds otherwise
  (`chasm/lib/scheduler/spec_processor.go:243-249`, `config.go:87-95 @ v1.32.0`;
  V1 `workflow.go:1333-1342`).
- **Override_Bridge**: the conformance dynamic-config bridge
  (`crates/tokeira-conformance`, `.kiro/specs/conformance-config-override/`); a key is
  `Wired` when the bridge delivers it to a typed consult site.
- **Registry_Skip**: a fork skip-registry entry by exact test name with a cited reason.
- **Tier**: a row of `docs/readiness/functional-test-order.md`.

## Target State

- Every Start_Path response and AlreadyStarted_Failure carries the Chain_Head, and
  `SignalWithStartWorkflowExecutionResponse.signal_link` and
  `SignalWorkflowExecutionResponse.link` carry a Request_Id_Ref_Link.
- Continue-as-new honours the command's `backoff_start_interval`.
- Update admission enforces the Total_Updates_Limit; update-with-start re-executes
  once on a Closing_Abort; the running-workflow start leg carries a
  Started_Event_Ref_Link; `UpdateWorkflowExecutionResponse.link` is populated.
- All-nil memo and search-attribute maps are omitted, not emitted empty.
- `PauseWorkflowExecution` is gated exactly as `1.32.0` stock; when enabled through
  the Override_Bridge, the pause suite passes.
- Schedules: Manual_Actions bypass pause, remaining-actions, and catchup; paused
  schedules keep reconciling; list entries are capped; `ScheduleId` supports the
  full operator set; the resolved Catchup_Window is described; pause-on-failure keys
  on failure and timeout only; `state_size_bytes` is positive; the three schedule
  search attributes are queryable; interval and phase durations are validated.
- Reset surfaces `reset_run_id`, reapplies along the surviving chain, tolerates a
  deleted current run, and batch reset carries post-reset options.
- Service error messages truncate at 4000 bytes.
- The Ledger rows for the D6 tiers are clean or classified.

Out of scope: the `__temporal_system` endpoint and every leaf that drives it (D5);
update completion callback registration and delivery (D5); versioning-override
semantics beyond the deprecated-field rule (D4); the visibility query converter (D3);
in-process metric assertions (Registry_Skip).

Sanctioned exceptions, each returning exactly what `1.32.0` returns:

- `PauseWorkflowExecution` with the Pause_Gate off:
  `UNIMPLEMENTED` `"workflow pause is not enabled for namespace: <ns>"`
  (`workflow_handler.go:7554-7556 @ v1.32.0`).
- Update `completion_callbacks` with `history.enableUpdateCallbacks` off: accepted and
  silently not registered (`mutable_state_impl.go:3355-3367 @ v1.32.0`); a request
  with callbacks and no `request_id` fails `INVALID_ARGUMENT`
  `"invalid *update.Request: request_id is required when completion_callbacks are set"`
  (`service/history/workflow/update/update.go:390-395 @ v1.32.0`).
- `TestWorkflowResetTestSuite/TestRepeatedResets_FailedWorkflowDoesNotDoubleCountFailedMetric`
  reads an in-process metric capture (`tests/workflow_reset_test.go:181-199 @ v1.32.0`);
  it is a Registry_Skip.

## Evidence From Current Code

- **Lineage stored, not surfaced.** `crates/tokeira-kernel/src/state.rs:139` holds
  `first_execution_run_id`; it is inherited on continue-as-new
  (`crates/tokeira-runtime/src/lane.rs:1133-1137`), retry and cron
  (`runtime/workflow_task.rs:2095, 2264`), and forked by reset
  (`kernel.rs:1435-1445`). `StartWorkflowResult` and `SignalWithStartResult`
  (`runtime/mod.rs:360-383, 447-460`) carry only `run_id`; the edge emits empty
  strings (`crates/tokeira-edge/src/grpc/translate.rs:3257-3264, 4762-4769`,
  `grpc/errors.rs:282-289`).
- **Continue-as-new backoff dropped.** `WorkflowCommand::ContinueAsNew`
  (`crates/tokeira-kernel/src/command.rs:1925-1945`) has no backoff field; the edge
  drops `backoff_start_interval` (`grpc/translate.rs:4969-5000`); the kernel computes
  the successor delay with `None` (`kernel.rs:5399-5410`).
- **Updates.** `history.maxTotalUpdates` is read only for the suggest-continue-as-new
  threshold (`runtime/workflow_task.rs:104-117`); `execute_multi_operation`
  (`crates/tokeira-edge/src/workflow_service.rs:6555-6670`) maps a Closing_Abort to
  the typed abort without re-execution; the update DTO has no `request_id` or
  `completion_callbacks` (`translate/mod.rs:972-982`); update and query responses
  emit `link: None` (`grpc/translate.rs:6432, 6569`).
- **Signals.** Signal and signal-with-start responses emit no link
  (`grpc/translate.rs:3269-3270, 4762-4767`); `request_id_infos` covers start and
  attach only (`:5608-5624`); internally authored requests set `links: Vec::new()`
  (`workflow_service.rs:3470, 3501`).
- **Nil payloads.** Domain filtering exists (`crates/tokeira-proto/src/conversions/common.rs:113-160`,
  predicate `:169-178`); the history serializer always wraps memo and search
  attributes in `Some` (`translate/history_serializer.rs:617-618`); describe likewise
  (`grpc/translate.rs:5551-5552`).
- **Pause.** Edge pause/unpause have no gate and no length validation
  (`workflow_service.rs:7759-7830`); storage regenerates the `TemporalPauseInfo`
  keyword list for visibility rows (`crates/tokeira-storage/src/api.rs:2009-2038`)
  but `DescribeWorkflowExecution` builds `search_attributes` from the run state only
  (`grpc/translate.rs:3933`); the bridge rejects `frontend.WorkflowPauseEnabled`
  (`crates/tokeira-conformance/src/lib.rs:489`).
- **Schedules.** `reconcile_running_workflows` iterates `all_active_schedules()`,
  which excludes paused schedules (`crates/tokeira-runtime/src/schedule.rs:422-428,
  1229`); runtime replay re-applies remaining-actions and catchup gates to manual
  starts (`:1107-1114`) and decrements `remaining_actions` for every start
  (`:1447-1449`); pause-on-failure fires on any completion (`:1252-1255`); the list
  memo copies `recent_actions` uncapped (`translate/schedule.rs:188-193`); the
  catchup window is echoed raw (`:455-472`); `state_size_bytes` is `0`
  (`:181, 499`); the schedule filter supports `=`, `IN`, `AND`, `OR` only
  (`crates/tokeira-projection/src/filter.rs:60-71, 183-226`); durations are not
  validated (`translate/schedule.rs:808-812`); memo updates are rejected before size
  validation (`workflow_service.rs:2103-2110`); no request-id length check.
- **Reset.** Missing current run tolerated (`runtime/lifecycle.rs:1407-1466`); reapply
  reads only the base run's post-fork history (`lane.rs:642-681`); the kernel sets
  `reset_run_id` (`kernel.rs:1474-1477`) but the edge never emits it
  (`grpc/translate.rs:5600-5630`); post-reset options are applied on direct reset
  (`grpc/translate.rs:4607-4640`, `lane.rs:806-815`) but the batch reset translation
  has no post-reset operations (`translate/batch.rs`, no match); describe emits the
  modern `pinned` override only.
- **Error truncation.** No equivalent in `crates/tokeira-edge/src/grpc/`.
- **Eager activities.** Always eager, capped at 3 per response
  (`workflow_service.rs:9503-9531, 1420-1428`), no paused-workflow or versioning
  guard.
- **Behaviour (authoritative) for each item:** cited inline in the requirements below.

## Contract Policy

### Response fields owned by D6

| Field | `1.32.0` behaviour | Policy |
|---|---|---|
| `StartWorkflowExecutionResponse.first_execution_run_id` (6) | Chain_Head of the resolved run on every outcome: new start (`startworkflow/api.go:545`), dedup (`:353, 579, 600`), use-existing (`:755`), eager (`:817, 846`) | populate |
| `SignalWithStartWorkflowExecutionResponse.first_execution_run_id` (4) | `signalwithstartworkflow/api.go:96-99` from the resolved run | populate |
| `WorkflowExecutionAlreadyStartedFailure.first_execution_run_id` (3), `.start_request_id` (1) | `workflow_id_dedup.go:236-256` | populate both |
| `StartWorkflowExecutionResponse.link` (4) on the multi-operation running-workflow leg | Started_Event_Ref_Link, `started = false`, `status = RUNNING` (`multioperation/api.go:334-339`) | populate |
| `SignalWorkflowExecutionResponse.link` (1) | Request_Id_Ref_Link(`request_id`, `WORKFLOW_EXECUTION_SIGNALED`), unconditional (`signalworkflow/api.go:115-122`) | populate |
| `SignalWithStartWorkflowExecutionResponse.signal_link` | same rule (`workflow_handler.go:2386`) | populate |
| `UpdateWorkflowExecutionResponse.link` (4) | rejected → `Link.Workflow{reason "Update rejected"}`; accepted or completed → Request_Id_Ref_Link(`request_id`, `WORKFLOW_EXECUTION_UPDATE_ACCEPTED`) (`updateworkflow/api.go:277-303`) | populate |
| `QueryWorkflowResponse.link` (3) | not populated at `1.32.0` (`queryworkflow/api.go:95-110, 223-226, 371-374, 418-419`) | keep `None` |
| `WorkflowExtendedInfo.request_id_infos[signal request id]` | present only with the Backlinks_Override on | populate iff the override is on |
| `WorkflowExtendedInfo.reset_run_id` | set on the base run by a reset | populate |
| `WorkflowExecutionInfo.versioning_info.versioning_override` deprecated `behavior`, `pinned_version` | populated alongside `pinned` (`common/worker_versioning/worker_versioning.go:1141`) | populate (rule owned by D4) |
| `WorkflowExecutionStartedEventAttributes.memo` / `.search_attributes`, `DescribeWorkflowExecution` memo / search attributes | absent when every value is nil (`historybuilder/event_factory.go:73-74, 498-499, 872-873`; `common/payload/payload.go:97-131`) | omit |
| `ScheduleListEntry.info.recent_actions`, `.future_action_times` | last 5 / first 5 (`scheduler/workflow.go:207-209, 1218`) | cap |
| `DescribeScheduleResponse.schedule.policies.catchup_window` | resolved Catchup_Window (`scheduler.go:718-720`) | resolve |
| `DescribeScheduleResponse.info.state_size_bytes` | positive approximate size (`scheduler.go:745`) | populate |
| `DescribeWorkflowExecutionResponse.workflow_execution_info.search_attributes["TemporalPauseInfo"]` | `Workflow:<id>`, `Reason:<reason>`, `property:activityType=<type>` entries (`mutable_state_impl.go:6960-6962`) | populate |

### Dynamic-config keys owned or consulted by D6

| Key | `1.32.0` stock | Ledger disposition at `d019b605` | D6 policy |
|---|---|---|---|
| `frontend.WorkflowPauseEnabled` | `false` | pinned-behavioral-constant, override none | change to conformance-only override, `Wired`; edge gate default `false` |
| `history.maxTotalUpdates` | `2000` | conformance-only override, `Wired` | enforce at admission |
| `history.enableCHASMSignalBacklinks` | `false` | pinned-behavioral-constant | change to conformance-only override, `Wired`; gates signal `request_id_infos` |
| `history.enableUpdateCallbacks` | `false` | pinned false | unchanged; request-id precondition only |
| `history.enableSignalWithStartFromWorkflow` | `false` | pinned false | unchanged (D5) |
| `history.enableUpdateWithStartRetryOnClosedWorkflowAbort`, `…RetryableErrorOnClosedWorkflowAbort` | `true` | pinned | behave as `true` |
| `system.enableActivityEagerExecution` | `true` | pinned true | behave as `true` |
| `system.maxServiceErrorMessageLength` | `4000` | pinned | implement at the edge |
| `frontend.enforceScheduleDurationValidation` | `true` | pinned true | implement |
| `limit.blobSize.error`, `limit.blobSize.warn` | `2 MiB` / `512 KiB` | verify `Wired` | consulted by schedule payload validation |
| `history.workflowIdReuseMinimalInterval` | `1s` | kernel-excluded | unchanged (`kernel.rs:138`) |

## Requirements

### Requirement 1: First execution run id on start paths

**User Story:** As an SDK user, I want every start-path response and already-started
error to name the head of the execution chain, so that clients can address a
workflow's lineage the way `1.32.0` lets them.

#### Acceptance Criteria

1. WHEN a Start_Path creates a new run, THE response SHALL carry
   `first_execution_run_id` equal to the new run id
   (`startworkflow/api.go:545 @ v1.32.0`; `tests/workflow_test.go:1950`).
2. WHEN a Start_Path resolves to an existing run by request-id dedup or by
   `USE_EXISTING`, THE response SHALL carry that run's Chain_Head
   (`:353, 579, 600, 755`; `tests/workflow_test.go:1974, 2020`).
3. WHEN a Start_Path fails with `ALREADY_EXISTS`, THE AlreadyStarted_Failure SHALL
   carry `run_id` of the current run, `first_execution_run_id` of its Chain_Head, and
   `start_request_id` of its start request (`workflow_id_dedup.go:236-256`;
   `tests/workflow_test.go:1998, 2091-2092, 2162-2173`).
4. WHEN the resolved run is a retry, cron, or continue-as-new successor, THE
   Chain_Head SHALL be the first run of that chain (`mutable_state_impl.go:1993-2011,
   6256-6272`; `retry.go:158, 315`; `tests/continue_as_new_test.go:1279-1290`).
5. WHEN the resolved run is a reset run, THE Chain_Head SHALL be the base run's
   Chain_Head, including a base run from an older chain (`workflow_resetter.go:501`;
   `tests/continue_as_new_test.go:1350, 1402-1403, 1496`).
6. WHEN `SignalWithStartWorkflowExecution` signals an existing run or rejects a
   duplicate on a completed run, THE response or failure SHALL carry the Chain_Head
   (`signalwithstartworkflow/api.go:96-99`; `tests/workflow_test.go:2197, 2232, 2279`).
7. THE kernel SHALL NOT change: the value is read from the existing
   `first_execution_run_id` state field.

### Requirement 2: Continue-as-new backoff pass-through

**User Story:** As a workflow author, I want a continue-as-new `backoff_start_interval`
to delay the successor's first workflow task exactly as requested, so that delayed
restarts behave like `1.32.0`.

#### Acceptance Criteria

1. WHEN a `ContinueAsNewWorkflowExecution` command carries `backoff_start_interval`,
   THE `WorkflowExecutionContinuedAsNew` event SHALL carry the same value
   (`historybuilder/event_factory.go:491 @ v1.32.0`; `tests/continue_as_new_test.go:559`).
2. WHEN the successor run starts, THE successor's `first_workflow_task_backoff` SHALL
   equal the command backoff, raised to `min_interval − lifetime` only when
   `lifetime + backoff < history.workflowIdReuseMinimalInterval`
   (`mutable_state_impl.go:2786, 2868-2894`; `tests/continue_as_new_test.go:580`).
3. WHEN the successor's first workflow task is scheduled, THE scheduled time SHALL be
   at least the backoff after the run start (`:592-594`).
4. THE kernel `WorkflowCommand::ContinueAsNew` SHALL gain a
   `backoff_start_interval: Option<Duration>` field, threaded from the edge; the
   existing minimum-interval arithmetic SHALL be preserved.

### Requirement 3: Update-with-start admission and retry

**User Story:** As an SDK user, I want update-with-start to enforce the update budget,
retry once when the target closes underneath it, and describe a running target with a
link, so that the multi-operation contract matches `1.32.0`.

#### Acceptance Criteria

1. WHEN an update with a new update id is admitted and the count of admitted plus
   completed updates for the run is at least the Total_Updates_Limit, THE admission
   SHALL fail `FAILED_PRECONDITION` with the message of `registry.go:445-450 @ v1.32.0`
   (contains `"limit on the total number of distinct updates in this workflow has been reached"`).
2. WHEN that failure occurs inside `ExecuteMultiOperation`, THE response SHALL be the
   multi-operation error whose first entry is `"Operation was aborted."` and whose
   second entry carries the limit message (`tests/update_workflow_test.go:5809-5812`).
3. WHEN an update-with-start with `USE_EXISTING` is aborted by a Closing_Abort and no
   workflow was started by the request, THE service SHALL re-execute the whole
   multi-operation exactly once, so a fresh run starts and the update proceeds there
   (`multioperation/api.go:126-147`; `tests/update_workflow_test.go:5645-5692`).
4. IF the re-execution is aborted again, THEN THE service SHALL return `ABORTED`
   (`multioperation/api.go:151-163`).
5. WHEN an update-with-start with `USE_EXISTING` attaches to a running workflow
   without starting one, THE start leg SHALL report `started = false`,
   `status = RUNNING`, and a Started_Event_Ref_Link for the running run
   (`multioperation/api.go:334-339`; `tests/update_workflow_test.go:5169-5175, 5226`).
6. WHEN a retried request with the same request id and update id arrives, THE retried
   request SHALL NOT count against the Total_Updates_Limit.

### Requirement 4: Update response link

**User Story:** As an SDK user, I want `UpdateWorkflowExecution` to return the link
`1.32.0` returns, so that callers can reference the update outcome.

#### Acceptance Criteria

1. WHEN an update is rejected, THE response `link` SHALL be
   `Link.Workflow{namespace, workflow_id, run_id, reason = "Update rejected"}`
   (`updateworkflow/api.go:277-291 @ v1.32.0`).
2. WHEN an update reaches `ACCEPTED` or `COMPLETED`, THE response `link` SHALL be a
   Request_Id_Ref_Link with the update's request id and
   `WORKFLOW_EXECUTION_UPDATE_ACCEPTED` (`:292-303`;
   `tests/nexus_workflow_update_test.go:112-124`).
3. THE `QueryWorkflowResponse.link` SHALL stay unset.

### Requirement 5: Signal links and request-id infos

**User Story:** As an SDK user, I want signal and signal-with-start responses to carry
links, attached links to land on the events they describe, and signal request ids to
be describable when backlinks are enabled, so that `1.32.0` link semantics hold.

#### Acceptance Criteria

1. WHEN `SignalWorkflowExecution` succeeds, THE response `link` SHALL be a
   Request_Id_Ref_Link with the request id and `WORKFLOW_EXECUTION_SIGNALED`,
   regardless of the Backlinks_Override (`signalworkflow/api.go:115-122 @ v1.32.0`;
   `tests/links_test.go:141-207`).
2. WHEN a signal request is deduplicated by request id, THE response SHALL carry the
   same link and THE history SHALL NOT gain a second `WorkflowExecutionSignaled`
   event (`tests/links_test.go:141-207`).
3. WHEN `SignalWithStartWorkflowExecution` succeeds, THE response `signal_link` SHALL
   be the same Request_Id_Ref_Link, for a new and for an existing run
   (`workflow_handler.go:2386`; `tests/links_test.go:627-756`).
4. WHEN a request carries `links`, THE links SHALL be attached to the event the request
   produces: `WorkflowExecutionStarted`, `WorkflowExecutionSignaled`,
   `WorkflowExecutionCancelRequested`, `WorkflowExecutionTerminated`
   (`tests/links_test.go:73-139, 627-756`).
5. WHILE the Backlinks_Override is on, WHEN a signal is recorded, THE
   `WorkflowExtendedInfo.request_id_infos[request_id]` SHALL name
   `WORKFLOW_EXECUTION_SIGNALED` with `buffered = true` and no event id while a
   workflow task is in flight, and the real event id once flushed
   (`mutable_state_impl.go:708-710, 6174-6185`; `tests/links_test.go:488-534`).
6. WHILE the Backlinks_Override is on, WHEN a run is reset, THE reset run's
   `request_id_infos` SHALL be rebuilt from the reapplied signals
   (`tests/links_test.go:286-365`).
7. WHILE the Backlinks_Override is off, THE signal request ids SHALL be absent from
   `request_id_infos`.

### Requirement 6: Nil memo and search-attribute omission

**User Story:** As an SDK user, I want a memo or search-attribute map whose values are
all nil to be absent from history and describe responses, so that clients see what
`1.32.0` emits.

#### Acceptance Criteria

1. WHEN every value of a start request's memo is a nil payload, THE
   `WorkflowExecutionStartedEventAttributes.memo` SHALL be absent
   (`payload.go:97-131 @ v1.32.0`; `tests/nil_search_attribute_test.go:296`).
2. WHEN every value of a start request's search attributes is a nil payload, THE
   `WorkflowExecutionStartedEventAttributes.search_attributes` SHALL be absent
   (`:130`).
3. WHEN a memo or search-attribute map mixes nil and non-nil values, THE nil keys
   SHALL be dropped and the message SHALL be present (`:72-75, 247`).
4. THE same rule SHALL apply to continue-as-new and child-start events
   (`event_factory.go:498-499, 872-873`) and to `DescribeWorkflowExecution`.
5. THE nil predicate SHALL match `isNilPayload` (`payload.go:88-93`): a nil payload, a
   payload with nil data, or data equal to `null` or `[]`, regardless of encoding.

### Requirement 7: Update completion callbacks, stock posture

**User Story:** As an operator running stock configuration, I want update completion
callbacks to behave as `1.32.0` does when the feature is off, so that no undocumented
behaviour appears.

#### Acceptance Criteria

1. WHEN an `UpdateWorkflowExecutionRequest` carries `completion_callbacks` and an empty
   `request_id`, THE request SHALL fail `INVALID_ARGUMENT` with
   `"invalid *update.Request: request_id is required when completion_callbacks are set"`
   (`update.go:390-395 @ v1.32.0`).
2. WHEN it carries both, THE update SHALL proceed and THE callbacks SHALL NOT be
   registered (`mutable_state_impl.go:3355-3367`), and `DescribeNamespace` SHALL keep
   advertising `workflow_update_callbacks = false`.
3. THE leaf `TestUpdateWorkflowSdkSuite/TestUpdateSameRequestIDDeduplicatesCallbacks`
   SHALL be reassigned to D5 with the Nexus update-callback suite; it is a positive
   test of the gated feature.

### Requirement 8: Eager activity execution, stock posture

**User Story:** As an operator, I want eager activity dispatch to follow `1.32.0` stock
behaviour, which is now enabled, including its guards.

#### Acceptance Criteria

1. WHEN a `ScheduleActivityTask` command requests eager execution, THE activity task
   SHALL be returned inline unless the workflow is paused or the task queue routing
   is versioned without `use_workflow_build_id`
   (`respondworkflowtaskcompleted/workflow_task_completed_handler.go:542-549 @ v1.32.0`).
2. THE per-response cap of three eager tasks (`workflow_service.rs:1420-1428`) SHALL
   be removed; `1.32.0` has no cap (`:672`).
3. THE returned eager task SHALL carry the activity's retry policy (`:663-669`).

### Requirement 9: Service error message truncation

**User Story:** As an operator, I want oversized error messages truncated the way
`1.32.0` truncates them, so that clients never receive unbounded status text.

#### Acceptance Criteria

1. WHEN a gRPC status message exceeds 4000 bytes, THE edge SHALL return
   `TruncateUTF8(message, 4000 − len(suffix)) + "... <truncated>"`
   (`common/rpc/interceptor/service_error_interceptor.go:17, 54-60 @ v1.32.0`).
2. THE truncation SHALL preserve the status code and error details.

### Requirement 10: Workflow pause gate and validation

**User Story:** As an operator running stock configuration, I want
`PauseWorkflowExecution` refused the way `1.32.0` refuses it, and request validation in
the same order, so that pause is stock-compatible.

#### Acceptance Criteria

1. WHILE the Pause_Gate is off, WHEN `PauseWorkflowExecution` is called, THE edge SHALL
   return `UNIMPLEMENTED` `"workflow pause is not enabled for namespace: <ns>"` before
   resolving the namespace or the workflow
   (`workflow_handler.go:7554-7556 @ v1.32.0`;
   `tests/pause_workflow_execution_test.go:1129-1143`).
2. WHEN `UnpauseWorkflowExecution` is called, THE edge SHALL reject `reason`,
   `request_id`, and `identity` longer than 1000 bytes with `INVALID_ARGUMENT`
   `"reason is too long."`, `"request id is too long."`, `"identity is too long."`, in
   that order, before resolving the workflow (`:7583-7595`;
   `tests/pause_workflow_execution_test.go:1150-1175`).
3. WHEN `PauseWorkflowExecution` is called with the gate on, THE same length
   validation SHALL apply before resolution.
4. THE key `frontend.WorkflowPauseEnabled` SHALL be `Wired` in the Override_Bridge and
   consulted by an edge gate whose production default is `false`.

### Requirement 11: Workflow pause fidelity

**User Story:** As an SDK user with pause enabled, I want a paused workflow to describe
itself as `1.32.0` does, so that tooling built on `TemporalPauseInfo` works.

#### Acceptance Criteria

1. WHEN a workflow is paused, THE `DescribeWorkflowExecution` response SHALL report
   status `PAUSED`, `WorkflowExtendedInfo.pause_info` with the identity and reason,
   and a `TemporalPauseInfo` keyword-list search attribute containing
   `Workflow:<workflow_id>` and `Reason:<reason>` (`mutable_state_impl.go:6960-6962`;
   `tests/pause_workflow_execution_test.go:2288-2310`).
2. WHEN a workflow is unpaused, THE `Workflow:` and `Reason:` entries SHALL be removed
   while paused-activity entries remain.
3. WHEN a workflow is paused, THE history SHALL contain no `WorkflowTaskScheduled`
   event between the pause and unpause events (`:2312-2330`).
4. WHEN `PauseWorkflowExecution` repeats with the same request id, THE second call
   SHALL succeed without a second pause event (`TestPauseIdempotentSameRequestId`).
5. WHEN the whole `TestPauseWorkflowExecutionSuite` runs with the Pause_Gate wired on,
   THE suite SHALL pass except leaves classified by name; each remaining defect SHALL
   be recorded against `1.32.0` source before it is fixed.

### Requirement 12: Schedule manual actions and paused reconciliation

**User Story:** As an operator, I want backfills and immediate triggers to run on
paused schedules and complete their bookkeeping, so that catch-up operations behave
like `1.32.0`.

#### Acceptance Criteria

1. WHEN a backfill or trigger produces a Manual_Action, THE action SHALL run even if
   the schedule is paused or has no remaining actions, and SHALL ignore the catchup
   window (`scheduler/workflow.go:727, 750-753, 1414 @ v1.32.0`;
   `tests/schedule_test.go:4449-4507, 5070-5122`).
2. WHEN a Manual_Action starts a workflow, THE `remaining_actions` counter SHALL NOT
   be decremented.
3. WHEN a scheduled workflow completes, THE schedule SHALL update `recent_actions`
   and clear `running_workflows` whether or not the schedule is paused
   (`workflow.go:900-921`; `tests/schedule_test.go:4510-4512`).
4. WHEN a backfill range is given, THE first matching time SHALL be inclusive of the
   range start (`workflow.go:481-484`).
5. WHEN buffered Manual_Actions are released, THE same bypass rules SHALL apply.

### Requirement 13: Schedule request validation and list caps

**User Story:** As an SDK user, I want schedule requests validated and list entries
shaped as `1.32.0` does.

#### Acceptance Criteria

1. WHEN `ListSchedules` returns an entry, THE `info.recent_actions` SHALL be the last
   5 actions and `info.future_action_times` the first 5 (`workflow.go:207-209, 1218`;
   `tests/schedule_test.go:1810-1812`).
2. WHEN `UpdateSchedule` or `PatchSchedule` carries a `request_id` longer than 1000
   bytes, THE request SHALL fail `INVALID_ARGUMENT` `"RequestId length exceeds limit."`
   before any other validation (`workflow_handler.go:4643-4645`, `errors.go:31`;
   `tests/schedule_test.go:4670-4676`).
3. WHEN `CreateSchedule` carries an empty `request_id`, THE request SHALL fail as
   `1.32.0` does (`:3835-3837`).
4. WHEN an `UpdateSchedule` memo plus action input exceeds `limit.blobSize.error`,
   THE request SHALL fail `INVALID_ARGUMENT` before the memo-update rejection
   (`:4677-4682, 6880-6905`; `tests/schedule_test.go:4750-4766`).
5. WHEN a schedule spec interval or phase is not a valid protobuf duration, THE
   request SHALL fail `INVALID_ARGUMENT`
   `"Invalid schedule spec: interval is not a valid duration: …"` or
   `"… phase is not a valid duration: …"` (`:6907-6934`).

### Requirement 14: Schedule-id filter and schedule search attributes

**User Story:** As an operator, I want `ListSchedules` and `CountSchedules` to accept
the `ScheduleId` operators and schedule search attributes `1.32.0` accepts.

#### Acceptance Criteria

1. WHEN a schedule query compares `ScheduleId` with `=`, `!=`, `IN`, `NOT IN`,
   `STARTS_WITH`, `NOT STARTS_WITH`, `IS NULL`, or `IS NOT NULL`, THE filter SHALL
   evaluate it exactly (`service/worker/scheduler/schedule_id_query_rewriter.go:29-74,
   98-135, 146-150 @ v1.32.0`; `tests/schedule_test.go:2235-2364`).
2. WHEN a schedule query references `ScheduleNextActionTime`,
   `ScheduleRunningWorkflowCount`, or `ScheduleBufferedStartsCount`, THE filter SHALL
   evaluate it against the schedule's live values (`chasm/lib/scheduler/scheduler.go:75-86,
   1012`; `tests/schedule_test.go:5157-5236, 5460-5525`).
3. IF a query uses an unsupported field or operator, THEN THE request SHALL fail
   `INVALID_ARGUMENT` as today.

### Requirement 15: Schedule describe fidelity

**User Story:** As an SDK user, I want schedule pause-on-failure, catchup window, and
state size described as `1.32.0` describes them.

#### Acceptance Criteria

1. WHEN a scheduled workflow closes and `pause_on_failure` is set, THE schedule SHALL
   pause only for `FAILED` or `TIMED_OUT`; canceled and terminated runs SHALL NOT
   pause it (`scheduler/workflow.go:923-928 @ v1.32.0`;
   `tests/schedule_test.go:314-402`).
2. WHEN the schedule pauses for a failure, THE note SHALL be
   `"paused due to workflow failure: <workflow_id>: <message>"` (`workflow.go:923-928`);
   the CHASM-mode note `"paused, workflow <status>: <workflow_id>"`
   (`scheduler.go:669-678`) SHALL be recorded as a mode difference and verified
   against the CHASM leaves before the text is fixed.
3. WHEN a schedule is described, THE `policies.catchup_window` SHALL be the resolved
   Catchup_Window (`spec_processor.go:243-249`; `tests/schedule_test.go:454-503`).
4. WHEN a schedule is described, THE `info.state_size_bytes` SHALL be a positive
   deterministic approximation of the stored state (`scheduler.go:745`;
   `tests/schedule_test.go:2861-2903`).
5. WHEN a scheduled run continues-as-new or is reset, THE schedule SHALL track the
   successor run for overlap and completion (`tests/schedule_workflow_pause_interaction_test.go:432-578`).

### Requirement 16: Reset linkage and chain reapply

**User Story:** As an operator, I want a reset to describe its successor and to reapply
signals from the whole surviving chain, so that reset semantics match `1.32.0`.

#### Acceptance Criteria

1. WHEN a run is reset, THE base run's `WorkflowExtendedInfo.reset_run_id` SHALL name
   the new run (`tests/reset_workflow_test.go:1136-1141 @ v1.32.0`).
2. WHEN a reset targets an explicit base run and the current run has been deleted, THE
   reset SHALL succeed, the new run SHALL become current, and `DescribeWorkflowExecution`
   by workflow id SHALL resolve it (`resetworkflow/api.go:86-99, 218-231`;
   `tests/reset_workflow_test.go:1093-1148`).
3. WHEN a reset reapplies events, THE signals recorded on every later run of the
   continue-as-new chain SHALL be reapplied in order, and a deleted run SHALL truncate
   the chain (`workflow_resetter.go:644 @ v1.31.0` and the `1.32.0` diff;
   `tests/reset_workflow_test.go:1150-1160`).
4. WHEN `StartBatchOperation` carries a reset operation with `post_reset_operations`,
   THE options SHALL be applied to each reset run and a
   `WorkflowExecutionOptionsUpdated` event SHALL be written
   (`tests/workflow_reset_test.go:318-437`).
5. WHEN a pinned versioning override is described, THE deprecated `behavior` and
   `pinned_version` fields SHALL be populated alongside `pinned`
   (`worker_versioning.go:1141`; `tests/workflow_reset_test.go:310-316`); the rule
   is D4's and D6 applies it on the describe path only.

### Requirement 17: Suites kept clean

**User Story:** As a maintainer, I want the suites that already pass to stay green
through this delta.

#### Acceptance Criteria

1. WHEN D6 lands, THE suites `TestWorkflowTypeEncodingSuite`,
   `TestPrematureEosTestSuite`, and `TestRelayTaskTestSuite` SHALL remain clean.
2. WHEN D6 lands, THE `TestHttpApiTestSuite` basics leaves SHALL pass with the
   fork's synchronized close-history read; the in-process
   `HTTPServiceRequests` metric assertion requires a namespace-tagged sample from the
   metrics bridge (`tests/http_api_test.go:100-114`, `tests/testcore/test_env.go:576-578 @ v1.32.0`).
3. THE `TestHTTPAPIHeaders` leaf SHALL stay `expected-until-flip`.

### Requirement 18: Registry and ledger dispositions

**User Story:** As the integration seat, I want every D6 skip and configuration
decision recorded where the campaign records them.

#### Acceptance Criteria

1. THE `TestRepeatedResets_FailedWorkflowDoesNotDoubleCountFailedMetric` leaf SHALL be
   a Registry_Skip citing the in-process capture (`tests/workflow_reset_test.go:181-199`).
2. THE classification ledger SHALL change `frontend.WorkflowPauseEnabled` and
   `history.enableCHASMSignalBacklinks` to conformance-only overrides with `Wired`
   dispositions, with evidence anchors and the D6 owner.
3. THE `limit.blobSize.error` and `limit.blobSize.warn` dispositions SHALL be verified
   `Wired` before Requirement 13.4 is implemented.
4. WHEN each D6 tier is clean, THE Ledger row SHALL be updated with counts and the
   commit, per the campaign's Requirement 10.

## Iteration and Feedback Notes

- Replays on 2026-09-15 against the campaign head reproduced the baseline exactly for
  every D6 suite; the signatures are in `design.md` § Replay signatures.
- Brief B expected the reset-with-options leaves to fail at setup on an internal
  matching RPC; the replay shows the fork's matching adapter already bridges it and the
  leaves fail on the deprecated describe fields and the missing batch post-reset
  options. Requirement 16.4–16.5 follow the replay.
- The pause suite existed at `1.31.0` but was classed experimental and never run
  (`docs/readiness/functional-test-order.md:150`); it enters the surface here because
  Tokeira implements pause and `1.32.0` gates it identically.
