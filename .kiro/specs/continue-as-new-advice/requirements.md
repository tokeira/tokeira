# Continue-As-New Advice — Requirements

## Introduction

Temporal tells a workflow when it should continue-as-new soon. The advice travels in the
`WorkflowTaskStarted` history event as a flag, a list of reasons, and the run's history
size in bytes. SDKs surface the flag to workflow code; the Rust SDK 0.8.0 exposes it as
`continue_as_new_suggested()`. Tokeira records the flag as `false` and the size as `0` on
every workflow task today, so a workflow that relies on the server's advice never
continues because of growing history.

This feature makes the advice real. It adds the one durable statistic Tokeira lacks, the
per-run persisted history size, derives the advice deterministically in the kernel at
every workflow-task start from the v1.31.0 thresholds, carries the recorded values through
every delivery path including transient and speculative task synthesis and history
rebuild, and reports the same size from `DescribeWorkflowExecution` and visibility.

The feature is foundational: it adds a storage-maintained per-run statistic and changes
the persisted layout of kernel state and history events. That layout change is a
state-compatibility break for hot-state rows and history batches written by earlier
releases; Requirement 10 makes the break explicit, loud, and the last of its kind by
introducing versioned blob envelopes.

Compatibility authority: Temporal server v1.31.0 (`TEMPORAL_SERVER_COMPAT`), read from
the pinned reference checkout, and the vendored API v1.62.11 protos under
`proto/upstream/`. Sibling specs: [conformance-config-override](../conformance-config-override/requirements.md)
owns the override bridge the thresholds are wired through;
[transient-wft](../transient-wft/requirements.md) and
[speculative-wft](../speculative-wft/requirements.md) own virtual-task synthesis;
[workflow-reset](../workflow-reset/requirements.md) owns successor materialization;
[embedded-engine-listener](../embedded-engine-listener/requirements.md) provides the
second transport for the transport-independence evidence.

## Glossary

- **Advice:** The triple recorded on a `WorkflowTaskStarted` event:
  `suggest_continue_as_new` (bool), `suggest_continue_as_new_reasons` (list), and
  `history_size_bytes` (int64).
- **Reason:** A value of `temporal.api.enums.v1.SuggestContinueAsNewReason`:
  `HISTORY_SIZE_TOO_LARGE` (1), `TOO_MANY_HISTORY_EVENTS` (2), `TOO_MANY_UPDATES` (3).
  Value 4 is reserved and never emitted
  (`proto/upstream/temporal/api/enums/v1/workflow.proto:208-226`).
- **History Size:** The number of bytes of history batches storage has committed for a
  run, as encoded in the run's own store. Temporal calls this
  `ExecutionStats.HistorySize`.
- **History Count:** The event id the `WorkflowTaskStarted` event receives; Temporal uses
  `nextEventID` at the moment of the decision, which is that id.
- **Size Threshold, Count Threshold, Update Threshold:** The v1.31.0 defaults
  `limit.historySize.suggestContinueAsNew` = 4 MiB,
  `limit.historyCount.suggestContinueAsNew` = 4096, and
  ceil(`history.maxTotalUpdates` × `history.maxTotalUpdates.suggestContinueAsNewThreshold`)
  = ceil(2000 × 0.9) = 1800.
- **Pinned Constant:** A v1.31.0 default compiled into the runtime with no production
  configuration field, overridable only through the conformance override bridge in a
  `--features conformance` build.
- **Pending Task Record:** `PendingWorkflowTask` in kernel state, which already retains
  per-start decisions for later synthesis and replay.
- **Virtual Task:** A transient (attempt > 1) or speculative workflow task whose
  scheduled and started events are not persisted and are synthesized on read.
- **Rebuild:** `BasicKernel::replay_history_prefix`, which reconstructs kernel state from
  committed events for reset successors.
- **Stats Read:** A repository read that returns a run's state and its History Size in one
  round trip.
- **Blob Envelope:** A version-prefixed postcard encoding of a persisted kernel value.

## Target State

| Area | Verified current behaviour | Required behaviour |
|---|---|---|
| Polled task start | The runtime submits `history_size_bytes: 0, suggest_continue_as_new: false` ([workflow_task.rs:1701-1716](../../../crates/tokeira-runtime/src/runtime/workflow_task.rs)). | The runtime supplies the measured History Size and the thresholds; the kernel derives the Advice. |
| Eager sync-match start | The kernel stamps the event count into the bytes field and `false` ([kernel.rs:6958-6968](../../../crates/tokeira-kernel/src/kernel.rs)). | Bytes are `0` for a fresh run, as v1.31.0 reports for an eager first task; the Advice is derived by the same kernel rule. |
| Reasons | The kernel event has no reasons field; the serializer emits none ([event.rs:210-228](../../../crates/tokeira-kernel/src/event.rs), [history_serializer.rs:824-846](../../../crates/tokeira-edge/src/translate/history_serializer.rs)). | The event, the Pending Task Record, and proto field 8 carry the reasons. |
| Virtual task synthesis | History read and poll response synthesize the started event with `0/false` ([workflow_service.rs:9057-9078](../../../crates/tokeira-edge/src/workflow_service.rs), [from_internal.rs:79-97](../../../crates/tokeira-edge/src/translate/from_internal.rs)). | Both synthesize from the Pending Task Record's values recorded at that attempt's start. |
| Late materialization | The kernel materializes a virtual task's started event with `0/false` at completion, failure, timeout, and forced close ([kernel.rs:2024](../../../crates/tokeira-kernel/src/kernel.rs), [kernel.rs:3134](../../../crates/tokeira-kernel/src/kernel.rs), [kernel.rs:3654](../../../crates/tokeira-kernel/src/kernel.rs), [kernel.rs:6675](../../../crates/tokeira-kernel/src/kernel.rs)). | Materialization copies the Advice from the Pending Task Record, as v1.31.0 materializes from the stored task info. |
| Accounting | No persisted History Size exists. Describe reads the whole history and sizes the public proto per call ([lib.rs:4213-4222](../../../crates/tokeira-engine/src/lib.rs)); every visibility producer writes `0`. | Storage maintains one per-run History Size at commit; Describe, visibility, and the Advice read it. |
| Policy seam | `limit.historySize.suggestContinueAsNew` is `NotEnforced` in the override bridge ([lib.rs:397-401](../../../crates/tokeira-conformance/src/lib.rs)); the ledger marks it excluded and the count key a constant with no consult site. | Both keys plus the two update keys become Pinned Constants with `Wired` conformance accessors; the ledger rows say so. |
| Update reason | No completed-update count exists; `history.maxTotalUpdates` is unenforced. | The kernel counts completed updates; the Advice includes `TOO_MANY_UPDATES` at the Update Threshold. |
| Rebuild | `replay_history_prefix` re-derives the target-version decision and ignores the Advice fields ([kernel.rs:3976-4000](../../../crates/tokeira-kernel/src/kernel.rs)). | Rebuild copies the recorded Advice into the Pending Task Record; it never recomputes it. |
| Conformance | `TestTransientTaskSuite/TestTransientWorkflowTaskHistorySize` is a registry skip because the size threshold cannot be overridden. | The test is un-skipped and CLEAN; Tier 1.6 in the ledger is updated. |
| State format | Kernel state and history batches are unversioned positional postcard; a trailing field addition makes older blobs undecodable (verified by a scratch round trip: old→new fails with `DeserializeUnexpectedEnd`). | Hot state and history batches carry Blob Envelopes; pre-envelope blobs fail loudly with a named error; startup refuses to migrate a cluster whose hot table holds pre-envelope rows. |

Out of scope: enforcing `limit.historySize.error`, `limit.historyCount.error`, or
`history.maxTotalUpdates` as hard limits; forcing continuation; changing SDK behaviour;
warning-level thresholds; envelopes for activity, timer, and other side-table blobs.

## Evidence From Current Code

- **Contract shape (authoritative):** `WorkflowTaskStartedEventAttributes` fields
  `suggest_continue_as_new = 4`, `history_size_bytes = 5`,
  `suggest_continue_as_new_reasons = 8`
  (`proto/upstream/temporal/api/history/v1/message.proto:297-327`); the reason enum
  (`proto/upstream/temporal/api/enums/v1/workflow.proto:208-226`);
  `WorkflowExecutionInfo.history_size_bytes = 15`
  (`proto/upstream/temporal/api/workflow/v1/message.proto:45`).
  `PollWorkflowTaskQueueResponse` carries no advice field; SDKs read the history event.
- **Behaviour (authoritative, v1.31.0):**
  - Decision at task start: `service/history/workflow/workflow_task_state_machine.go:487-491`
    (flag = any reason; update reason appended) and `:1440-1466` (`getHistorySizeInfo`:
    `historySize >= sizeLimit`, `historyCount = nextEventID >= countLimit`).
  - Update reason: `service/history/workflow/update/registry.go:182-184, 496-505`
    (`inFlight + completed >= ceil(maxTotal × threshold)`; threshold 0 disables).
  - Defaults: `common/dynamicconfig/constants.go:370-375, 412-417, 2299-2308`.
  - Reset at schedule and recompute at start:
    `workflow_task_state_machine.go:98-99, 158-159`; late materialization of a virtual
    task's started event from the stored task info in `AddWorkflowTaskCompletedEvent`,
    `AddWorkflowTaskFailedEvent`, and `AddWorkflowTaskTimedOutEvent`:
    `:790-794, 881-885, 954-958`; reset of a scheduled-but-unstarted task adds a real
    started event with a fresh decision: `service/history/ndc/workflow_resetter.go:533-550`.
  - Stored on the execution: `proto/internal/temporal/server/api/persistence/v1/executions.proto:90-93`
    (`workflow_task_suggest_continue_as_new = 69`, `_reasons = 110`,
    `workflow_task_history_size_bytes = 70`).
  - Rebuild copies recorded values: `service/history/workflow/mutable_state_rebuilder.go:235-239`.
  - History Size is a persistence-maintained statistic starting at 0:
    `service/history/workflow/mutable_state_impl.go:380, 6610-6616`; Describe reads it:
    `service/history/api/describeworkflow/api.go:126`; eager start records the fresh
    statistic: `service/history/api/create_workflow_util.go:106`.
  - Metric: `WorkflowSuggestContinueAsNewCount` with per-reason tags,
    `workflow_task_state_machine.go:539-548`.
  - Functional tests: `tests/transient_task_test.go:140-300` (per-attempt recompute,
    monotone size, threshold change visible only from the next start),
    `tests/describe_test.go:77` (Describe size positive),
    `tests/update_workflow_test.go:4925-4960` (update reason).
- **SDK consumption (pinned):** `temporalio-sdk-core-0.8.0/src/worker/workflow/machines/workflow_machines.rs:892-894`
  copies the three attributes from the started event; `:464-473` puts them on the
  activation; `temporalio-workflow-0.8.0/src/workflow_context.rs:1591-1597, 2098`
  exposes `continue_as_new_suggested()`.
- **Current handlers / code:** the sites in the Target State table; the kernel request
  shape [command.rs:1137-1167](../../../crates/tokeira-kernel/src/command.rs); the kernel
  emit sites [kernel.rs:1735-1760](../../../crates/tokeira-kernel/src/kernel.rs) (polled
  start, normal and transient-with-new-events) and
  [kernel.rs:6950-6975](../../../crates/tokeira-kernel/src/kernel.rs) (sync-match); the
  DSQL hot row and batch writes
  [commit.rs:445-530](../../../crates/tokeira-storage/src/dsql/run_repository/commit.rs);
  the OCC read `SELECT transition_seq FROM workflow_hot ... FOR UPDATE`
  ([commit.rs:73](../../../crates/tokeira-storage/src/dsql/run_repository/commit.rs));
  the load path [load.rs:90](../../../crates/tokeira-storage/src/dsql/run_repository/load.rs);
  reset materialization [load.rs:264-330](../../../crates/tokeira-storage/src/dsql/run_repository/load.rs);
  the visibility record [api.rs:2025-2060](../../../crates/tokeira-storage/src/api.rs);
  the codec [codec.rs:60-115](../../../crates/tokeira-storage/src/dsql/codec.rs) and its
  versioned `BacklogEnvelope` precedent ([codec.rs:22-38](../../../crates/tokeira-storage/src/dsql/codec.rs));
  the in-memory snapshot version [memory.rs:428](../../../crates/tokeira-storage/src/memory.rs);
  the runtime accessor pattern
  [workflow_task.rs:58-68](../../../crates/tokeira-runtime/src/runtime/workflow_task.rs);
  the lane cache that consumes `CommitResult::Applied { new_state }`
  ([lane.rs:161-184](../../../crates/tokeira-runtime/src/lane.rs)).
- **Dependencies:** the kernel's `external_payload_count` / `external_payload_size_bytes`
  are already state-maintained statistics
  ([kernel.rs:448-449](../../../crates/tokeira-kernel/src/kernel.rs)); Describe can read
  them instead of re-deriving from history once the History Size no longer needs the
  full read.

## Field Policy

### `WorkflowTaskStartedEventAttributes` (history/v1/message.proto)

| Field (id) | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `scheduled_event_id` (1), `identity` (2), `request_id` (3), `worker_version` (6), `build_id_redirect_counter` (7), `target_worker_deployment_version_changed` (9) | Unchanged; owned by existing specs. | n/a | none |
| `suggest_continue_as_new` (4) | `true` iff the reasons list is non-empty at this attempt's start. | n/a (server-authored) | Recorded on the event and the Pending Task Record. |
| `history_size_bytes` (5) | The History Size read with the run before the start command; `0` for a sync-matched first task. | n/a | Recorded on the event and the Pending Task Record. |
| `suggest_continue_as_new_reasons` (8) | Exactly the reasons whose threshold was met, in enum order; empty iff the flag is false. | n/a | Recorded on the event and the Pending Task Record. |

### Dynamic-config keys (override bridge disposition)

| Key | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `limit.historySize.suggestContinueAsNew` | Pinned Constant 4 MiB; `Wired` accessor read at each start. | Non-integer override rejected by the bridge as today. | none |
| `limit.historyCount.suggestContinueAsNew` | Pinned Constant 4096; `Wired`. | as above | none |
| `history.maxTotalUpdates` | Pinned Constant 2000, used only to derive the Update Threshold; `Wired`. | as above | none; the hard limit stays unenforced |
| `history.maxTotalUpdates.suggestContinueAsNewThreshold` | Pinned Constant 0.9; `Wired` (`Float`); `0` disables the update reason. | as above | none |

### `WorkflowExecutionInfo` and visibility

| Field | Target policy | Error if invalid | Persistence/side-effect impact |
|---|---|---|---|
| `WorkflowExecutionInfo.history_size_bytes` (15) | The History Size from the Stats Read; positive for any run with committed history. | n/a | Removes the per-Describe full-history read. |
| Visibility `HistorySizeBytes` | The History Size at the transition that produced the record. | n/a | Storage fills the visibility record at commit. |

## Requirements

### Requirement 1: Storage maintains the History Size

**User Story:** As the engine, I want one durable per-run byte count maintained where the
bytes are written, so that the Advice, Describe, and visibility agree and no reader has to
scan history.

#### Acceptance Criteria

1.1 WHEN a transition with history events is committed, THE storage layer SHALL add the
encoded size of the committed history batch to the run's History Size in the same
atomic commit.

1.2 THE DSQL repository SHALL keep the History Size in a `workflow_hot` column added by
migration V068 and SHALL read the prior value under the same `FOR UPDATE` row lock it
uses for the OCC fence.

1.3 WHEN a `workflow_hot` row predates V068 and the column is NULL, THE DSQL repository
SHALL read the History Size as `0`.

1.4 THE in-memory store SHALL maintain the History Size using the same encoded-size
function as the DSQL codec.

1.5 WHEN a run is created, THE storage layer SHALL start its History Size at `0`,
including continue-as-new, retry, and cron successors.

1.6 WHEN a reset successor is materialized, THE storage layer SHALL set its History Size
to the encoded size of the copied history prefix.

1.7 WHEN a run is deleted, THE storage layer SHALL remove its History Size with the run.

1.8 THE `RunRepository` trait SHALL provide a Stats Read that returns the loaded run and
its History Size in one repository round trip.

1.9 THE storage layer SHALL populate `history_size_bytes` on every visibility record it
builds from the committed state.

1.10 THE History Size SHALL be non-decreasing across the committed transitions of one run.

### Requirement 2: The kernel derives the Advice at every task start

**User Story:** As a workflow author, I want the server's advice computed by one
deterministic rule at every task start, so that replay and every delivery path agree.

#### Acceptance Criteria

2.1 WHEN a workflow task starts, THE kernel SHALL include `HISTORY_SIZE_TOO_LARGE` in the
reasons iff the supplied History Size is greater than or equal to the Size Threshold.

2.2 WHEN a workflow task starts, THE kernel SHALL include `TOO_MANY_HISTORY_EVENTS` in the
reasons iff the event id assigned to the `WorkflowTaskStarted` event is greater than or
equal to the Count Threshold.

2.3 WHEN a workflow task starts, THE kernel SHALL include `TOO_MANY_UPDATES` in the
reasons iff the Update Threshold is non-zero and the number of in-flight updates
(admitted or accepted) plus the run's completed-update count is greater than or equal to
the Update Threshold.

2.4 WHEN a workflow task starts, THE kernel SHALL set `suggest_continue_as_new` to `true`
iff the reasons list is non-empty.

2.5 THE kernel SHALL record the flag, the reasons, and the History Size on the emitted
`WorkflowTaskStarted` event.

2.6 THE kernel SHALL record the same three values on the Pending Task Record at start.

2.7 WHEN a workflow task is scheduled, THE kernel SHALL clear the Pending Task Record's
Advice so a later start recomputes it.

2.8 WHEN a transient task starts a later attempt, THE kernel SHALL recompute the Advice
from the values supplied for that attempt.

2.9 WHEN a run starts with a sync-matched first workflow task, THE kernel SHALL derive the
Advice with a History Size of `0`.

2.10 WHEN the reset path synthesizes a started event for a fork-point task that was
scheduled but not started, THE kernel SHALL derive the Advice by the same rule from the
History Size and thresholds supplied with the failing command.

2.11 THE kernel SHALL count completed updates on the run's state, incrementing when an
update reaches its completed outcome.

2.12 THE kernel SHALL take the History Size and the three thresholds as command operands
and SHALL NOT read configuration or storage itself.

2.13 WHEN a virtual task's started event materializes at completion, failure, timeout, or
forced close, THE kernel SHALL copy the Advice from the Pending Task Record and SHALL NOT
recompute it.

### Requirement 3: Thresholds are Pinned Constants with a conformance override

**User Story:** As an operator, I want the v1.31.0 thresholds fixed in the release with no
production knob, so that Tokeira behaves like the targeted release and the conformance
harness can still drive test-sized thresholds.

#### Acceptance Criteria

3.1 THE runtime SHALL resolve the Size Threshold, Count Threshold, and Update Threshold
from Pinned Constants equal to the v1.31.0 defaults before each start command.

3.2 WHERE the runtime is built with the `conformance` feature, THE runtime SHALL read the
four keys in the Field Policy through the override read seam, falling back to the Pinned
Constants when no override is installed.

3.3 THE `tokeira-conformance` key table SHALL mark the four keys `Wired` in the same
change that adds their accessors.

3.4 WHEN the runtime is built without the `conformance` feature, THE accessors SHALL
return the Pinned Constants with no override surface.

3.5 THE configuration ledger `docs/conformance/v1.31.0/temporal-configuration.md` SHALL
classify the four keys as conformance-only overrides with a wired consult site.

3.6 THE runtime SHALL NOT add a production configuration field for any of the four keys.

### Requirement 4: Virtual tasks carry the recorded Advice

**User Story:** As an SDK worker, I want a transient or speculative task's synthesized
started event to carry the same advice the server recorded when that attempt started, so
that what I act on is what the server decided.

#### Acceptance Criteria

4.1 WHEN `GetWorkflowExecutionHistory` appends a virtual scheduled/started pair, THE edge
SHALL fill the started event's Advice from the Pending Task Record.

4.2 WHEN a poll response synthesizes a virtual started event, THE edge SHALL fill its
Advice from the Pending Task Record carried on the runtime's started-task result.

4.3 THE runtime's started-task result SHALL carry the Advice recorded by the start
transition.

4.4 WHEN a threshold changes after an attempt started, THE synthesized started event for
that attempt SHALL still carry the values recorded at its start.

### Requirement 5: Wire fidelity

**User Story:** As an SDK, I want the history event to carry all three advice fields, so
that `continue_as_new_suggested()` and the reasons list are populated.

#### Acceptance Criteria

5.1 THE history serializer SHALL emit `suggest_continue_as_new_reasons` (field 8) from
the kernel event, mapping each kernel reason to its proto enum value.

5.2 THE history serializer SHALL emit an empty reasons list iff the flag is `false`.

5.3 WHEN the pinned Rust SDK worker processes a task whose started event carries the
flag, THE worker's `continue_as_new_suggested()` SHALL return `true`.

5.4 THE Advice observed by a worker SHALL NOT depend on whether it connected through the
in-process endpoint or a network listener.

### Requirement 6: Rebuild and replay preserve recorded values

**User Story:** As the engine, I want reconstructed state to carry the advice history
recorded, so that changing a threshold never rewrites an emitted event or a rebuilt
pending task.

#### Acceptance Criteria

6.1 WHEN `replay_history_prefix` consumes a `WorkflowTaskStarted` event, THE kernel SHALL
copy the event's Advice into the rebuilt Pending Task Record without recomputing it.

6.2 THE kernel SHALL NOT read thresholds during rebuild.

6.3 WHEN a run's state is reloaded from storage, THE Pending Task Record's Advice SHALL
equal the values recorded at the last start.

### Requirement 7: Describe and visibility report the same size

**User Story:** As an operator, I want `DescribeWorkflowExecution` and visibility to show
the same history size the workflow was told, so that dashboards and workflow decisions
agree.

#### Acceptance Criteria

7.1 WHEN `DescribeWorkflowExecution` is served, THE edge SHALL fill
`history_size_bytes` from the Stats Read.

7.2 WHEN `DescribeWorkflowExecution` is served, THE edge SHALL fill the external-payload
statistics from kernel state and SHALL NOT read the full history for statistics.

7.3 WHEN a run has committed at least one history batch, THE Describe
`history_size_bytes` SHALL be positive.

7.4 WHEN a visibility record is projected, THE `HistorySizeBytes` system search attribute
SHALL equal the History Size at that transition and SHALL be queryable.

### Requirement 8: The Advice is advisory and observable

**User Story:** As a workflow author, I want the advice to change nothing but the advice,
so that my workflow decides when to continue.

#### Acceptance Criteria

8.1 THE engine SHALL NOT continue, close, fail, or reset a run because the Advice is
`true`.

8.2 WHEN the Advice differs between two otherwise identical transition sequences, THE
kernel's resulting state SHALL differ only in the recorded Advice fields and the
completed-update count.

8.3 WHEN a start transition records `suggest_continue_as_new = true`, THE runtime SHALL
increment a bounded-cardinality counter metric labelled by reason, mirroring
`WorkflowSuggestContinueAsNewCount`.

### Requirement 9: Conformance evidence

**User Story:** As the engine owner, I want the v1.31.0 functional test for this behaviour
to run against Tokeira, so that the claim is verified by the targeted release's own test.

#### Acceptance Criteria

9.1 THE fork skip registry entry for
`TestTransientTaskSuite/TestTransientWorkflowTaskHistorySize` SHALL be removed.

9.2 WHEN Tier 1.6 (`TestTransientTaskSuite`) runs against a `--features conformance`
`tokeirad`, THE suite SHALL report the test as PASS and the tier as CLEAN.

9.3 THE conformance ledger `docs/readiness/conformance.md` Tier 1.6 row SHALL record the
new outcome and remove the skip note.

9.4 THE Describe positivity assertion in the describe functional test
(`tests/describe_test.go:77`, `s.Positive(wfInfo.GetHistorySizeBytes())`) SHALL hold on
the same build.

### Requirement 10: State-format compatibility is explicit and loud

**User Story:** As an operator upgrading a cluster, I want an unreadable-state condition
to fail startup with a named error rather than misread history, and I want this to be the
last unversioned layout change.

#### Acceptance Criteria

10.1 THE storage codec SHALL encode `workflow_hot.state_data` as a Blob Envelope whose
version prefix is a 32-bit magic constant, following the `BacklogEnvelope` precedent.

10.2 THE storage codec SHALL encode `history_batch.events_data` as a Blob Envelope with
its own magic constant.

10.3 IF a hot-state or history blob lacks a recognised envelope version, THEN THE storage
layer SHALL return an error naming the blob kind, the run, and the observed version and
SHALL NOT return a decoded value.

10.4 WHEN startup would apply migration V068 to a cluster whose `workflow_hot` table
already holds rows, THE engine SHALL fail the Schema phase with an error stating that
pre-0.1.3 hot state cannot be read and that the cluster must be recreated.

10.5 THE in-memory snapshot format version SHALL be incremented so snapshots written by
earlier releases fail startup as unsupported.

10.6 THE change SHALL carry a `Changed` release note stating the state-format break and
the recreate requirement.

10.7 THE state-format break SHALL be approved explicitly by the integration seat before
implementation begins, per the root change classification for state-compatibility
breaks.

## Iteration and Feedback Notes

- Temporal's own comment on the size statistic: it "doesn't have to be 100% accurate"
  but must be consistent between the recorded event and what the SDK receives
  (`workflow_task_state_machine.go:481-485 @ v1.31.0`). Tokeira's units are
  persisted-encoding bytes of its own history batches, which is the same definition
  Temporal uses for its store. Describe changes from live public-proto size to this
  counter; no v1.31.0 test asserts the units, only positivity and relative growth.
- The Count Threshold compares the started event's own id, so a run with 4095 committed
  events whose next started event is 4096 already meets it, exactly as `nextEventID` does
  in v1.31.0.
- The hard limits `limit.historySize.error` and `limit.historyCount.error` stay
  `NotEnforced`; enforcing them is a separate decision.
- The scratch postcard round trip that verified the layout break is not committed; the
  design records the result.
