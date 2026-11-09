# History task types: v1.31.0 and Tokeira

Temporal server v1.31.0 turns every consequence of a committed transition into a history task. Its
task types are the `TaskType` enum in `proto/internal/temporal/server/api/enums/v1/task.proto @
v1.31.0`, in the server repository, not in `proto/upstream/`. The enum names 31 task types besides
`TASK_TYPE_UNSPECIFIED`; values 9, 11 and 23 are reserved.

This page lists each one: what it does in v1.31.0, which Tokeira mechanism produces the same effect,
when that mechanism acts, and the checks each side makes before acting. Use it when a change touches
dispatch, timers, timeouts, outbound work or visibility: a check that v1.31.0 makes and Tokeira
doesn't is a place where a stale decision can commit.

How to read it:

- Tokeira paths are relative to `crates/`. Temporal paths are in the server repository at tag
  `v1.31.0`; each section names its executor file, and cells name the function in that file.
- **When** says whether Tokeira acts at the task's time (a deadline wake), on the commit that creates
  the work, or on a periodic pass that can be late by up to one period.
- **Checks** lists only the checks particular to the task type. The common ones are below.

## Checks every task gets

In v1.31.0, every active executor loads mutable state through `loadMutableStateForTask`
(`service/history/ndc_task_util.go @ v1.31.0`). It drops a task from another shard range
(`validateTaskByClock`), a task written before the run's state was rebuilt (`validateTaskGeneration`),
and a task whose event id is beyond the loaded state, after one reload. Most executors then compare
the task's failover version with the state's (`CheckTaskVersion`, same file); that check serves
multi-cluster replication, which Tokeira doesn't implement.

In Tokeira, every effect commits as a run transition through the run's lane. The commit is fenced by
the shard lease and by the run's expected transition sequence, so a write against a moved run
conflicts and is retried from a fresh read. Most kernel commands refuse a closed run (`expect_open`
in `tokeira-kernel/src/kernel.rs`). A decision computed outside the lane must also carry what it
read, so the commit can check that it still holds: rule 5 of
[042 Durable Actionable State](../architecture/042-durable-actionable-state.md#the-five-rules).

## Transfer queue

v1.31.0 executor: `service/history/transfer_queue_active_task_executor.go @ v1.31.0`.

| Task type | v1.31.0 effect | Tokeira mechanism | When | v1.31.0 checks | Tokeira checks |
|---|---|---|---|---|---|
| `TRANSFER_WORKFLOW_TASK` (3) | Pushes a scheduled workflow task to matching. | `workflow_dispatch` row, found by queue-home discovery (`tokeira-runtime/src/discovery.rs`) and offered by the broker. | On commit; a discovery pass recovers a lost offer. | `processWorkflowTask`: run open; task exists at its scheduled id; stamp matches. | The start checks the run is open and the task's `logical_seq` (`apply_workflow_task_started` in `tokeira-kernel/src/kernel.rs`). |
| `TRANSFER_ACTIVITY_TASK` (4) | Pushes a scheduled activity to matching. | `activity_dispatch` row, found by activity discovery (`tokeira-runtime/src/activity_discovery.rs`). | On commit; a discovery pass recovers a lost offer. | `processActivityTask`: activity exists; stamp matches; not paused; run open. | Attempt, schedule event, stamp, unstarted, not paused, run running (`prepare_activity_dispatch`, `start_activity_task_inner` in `tokeira-runtime/src/runtime/activity.rs`). |
| `TRANSFER_CLOSE_EXECUTION` (5) | After a close, reports the result to the parent and applies the parent-close policy to children. | Close intent rows (`IntentKind` in `tokeira-storage/src/outbound.rs`), delivered by the outbound driver (`tokeira-runtime/src/outbound/driver.rs`). | On commit; a driver round recovers lost delivery. | `processCloseExecution`: run closed; close version. | The destination commit checks the intent row still exists, and for parent close the child run it chose (`DeliveryGuard::Intent`, `DeliveryGuard::CurrentIntent`). |
| `TRANSFER_CANCEL_EXECUTION` (6) | Requests cancellation of another workflow, then records requested or failed. | `run_operation` row of kind `ExternalCancel` (`tokeira-kernel/src/outbound.rs`), delivered by the outbound driver. | On commit; a driver round recovers lost delivery. | `processCancelExecution`: run open; request-cancel info exists for the initiated id. | The destination commit re-reads the source's initiated event and attempt (`DeliveryGuard::Operation` in `tokeira-storage/src/outbound.rs`). |
| `TRANSFER_START_CHILD_EXECUTION` (7) | Starts a child, records started or start-failed, then schedules the child's first workflow task. | `run_operation` rows `ChildStart` and `ChildRelease`, retried by `tokeira-runtime/src/child_start.rs`. | On commit; a driver round recovers lost delivery. | `processStartChildExecution`: child info exists; parent open, unless the child already started under `ABANDON`. | The destination commit re-reads the initiation and the current run it judged reuse against (`DeliveryGuard::ChildStart`). |
| `TRANSFER_SIGNAL_EXECUTION` (8) | Signals another workflow, then records signaled or failed. | `run_operation` row of kind `ExternalSignal`, delivered by the outbound driver. | On commit; a driver round recovers lost delivery. | `processSignalExecution`: signal info exists for the initiated id; run open. | The destination commit re-reads the source's initiated event and attempt (`DeliveryGuard::Operation`). |
| `TRANSFER_RESET_WORKFLOW` (10) | Resets a run whose auto-reset point names a bad binary (`service/history/workflow/mutable_state_impl.go @ v1.31.0`). | Not implemented: namespaces carry no bad binaries (`bad_binaries: None` in `tokeira-edge/src/grpc/translate.rs`), so no run qualifies. | — | `processResetWorkflow`: run is current; reset point and base run resolve. | — |
| `TRANSFER_DELETE_EXECUTION` (24) | Deletes a closed run's state, history and visibility. | `DeleteWorkflowExecution` makes the run unreachable and records a purge; the purger removes the rest in pages (`tokeira-runtime/src/purge.rs`). | A background queue, retried with backoff. | `processDeleteExecutionTask` (`transfer_queue_task_executor_base.go`): no close task pending; version. | A purge page is refused while the deletion record or a hold forbids it (`PurgePage::Refused` in `tokeira-storage/src/dsql/run_repository/bulk_write.rs`). |

## Timer queue

v1.31.0 executor: `service/history/timer_queue_active_task_executor.go @ v1.31.0`, with shared
checks in `service/history/timer_queue_task_executor_base.go @ v1.31.0`. "Time has come" is
`IsTimeExpired` in `service/history/queues/queue_scheduled.go @ v1.31.0`.

| Task type | v1.31.0 effect | Tokeira mechanism | When | v1.31.0 checks | Tokeira checks |
|---|---|---|---|---|---|
| `WORKFLOW_TASK_TIMEOUT` (12) | Times out a workflow task: schedule-to-start (sticky or speculative) or start-to-close. | In-memory deadline index rebuilt at acquisition (`tokeira-runtime/src/wft_timeout.rs`); speculative tasks on in-memory timers (`tokeira-runtime/src/speculative_timer.rs`). | At its time. | `executeWorkflowTaskTimeoutTask`: run open; task exists; stamp and schedule attempt match; a started task ignores schedule-to-start. | Run open; `logical_seq` and started event id match; schedule-to-start only while unstarted (`apply_workflow_task_timed_out`). |
| `ACTIVITY_TIMEOUT` (13) | Times out an activity on any of its four timeouts, retrying when the policy allows. | In-memory deadline index rebuilt at acquisition (`tokeira-runtime/src/activity_timeout.rs`). | At its time. | `processSingleActivityTimeoutTask`: run open; attempt not older; time has come; paused skipped (`service/history/workflow/timer_sequence.go`); stamp not checked. | Run open; same attempt and schedule event; not paused; violation recomputed; commit fenced to the read (`ActivityTimeoutCheck::validate`). |
| `USER_TIMER` (14) | Fires a user timer. | `timer_bucket` row, scanned per shard by `fire_at` (`run_timer_scanner` in `tokeira-runtime/src/scanner.rs`). | At its time; a full scan recovers a missed wake. | `executeUserTimerTimeoutTask`: run open; timer exists; time has come. | Run open; timer pending under its id; its `fire_at` has come, in floored milliseconds (`apply_timer_due`). |
| `WORKFLOW_RUN_TIMEOUT` (15) | Times out a run and starts its retry or cron successor if one applies. | In-memory tracker rebuilt at acquisition (`tokeira-runtime/src/timeout.rs`). | At its time. | `executeWorkflowRunTimeoutTask`: run open; the run's current expiration has passed (`isValidWorkflowRunTimeoutTask`). | The scanner evaluates its tracked deadline (`evaluate_workflow_timeout`); the commit checks only that the run is open (`apply_workflow_execution_timed_out`). |
| `DELETE_HISTORY_EVENT` (16) | Deletes a closed run once the namespace retention has passed. | Not implemented: a closed run stays until `DeleteWorkflowExecution`. | — | `executeDeleteHistoryEventTask` (base): run closed; close version. | — |
| `ACTIVITY_RETRY_TIMER` (17) | After the retry backoff, pushes the next attempt to matching. | The retry transition sets the `activity_dispatch` row's `dispatch_at`; activity discovery offers it once due. | A discovery pass; can be late by up to `DISCOVERY_PERIOD` (`tokeira-runtime/src/discovery.rs`). | `executeActivityRetryTimerTask`: activity exists; stamp matches; not paused; attempt current and unstarted; run open. | Same attempt, stamp and schedule event; unstarted; not paused; run running; `dispatch_at` reached (`prepare_activity_dispatch`). |
| `WORKFLOW_BACKOFF_TIMER` (18) | Schedules the first workflow task after a start delay, retry backoff or cron backoff. | Reserved timer `WORKFLOW_START_DELAY_TIMER_ID` (`tokeira-kernel/src/kernel.rs`) in `timer_bucket`, fired by `command_for_due_timer` (`tokeira-runtime/src/scanner.rs`). | At its time; a full scan recovers a missed wake. | `executeWorkflowBackoffTimerTask`: run open; no workflow task yet. | Run open; reserved timer still pending; schedules only if no task is pending (`apply_workflow_start_delay_elapsed`). |
| `WORKFLOW_EXECUTION_TIMEOUT` (29) | Times out the whole chain on whichever run is current. | Same tracker as the run timeout; each run carries the chain's deadline from `first_run_started_at`. | At its time. | `executeWorkflowExecutionTimeoutTask`: run open; first run id matches; expiration has passed. | As for the run timeout: the commit checks only that the run is open. |

## Visibility queue

v1.31.0 executor: `service/history/visibility_queue_task_executor.go @ v1.31.0`.

| Task type | v1.31.0 effect | Tokeira mechanism | When | v1.31.0 checks | Tokeira checks |
|---|---|---|---|---|---|
| `VISIBILITY_START_EXECUTION` (19) | Writes the open run's visibility record. | The commit writes a full image to `projection_log` (`insert_projection_log` in `tokeira-storage/src/dsql/run_repository/commit.rs`); the projector applies it (`tokeira-projection/src/dsql_store.rs`). | A projector pass; lag is by design. | `processStartExecution`: run open; start version. | Applied only if strictly newer by `(authority_epoch, source_transition_seq)` (`upsert_execution_row`). |
| `VISIBILITY_UPSERT_EXECUTION` (20) | Rewrites the open run's record after a search-attribute or memo change. | Same: every commit's image goes to `projection_log`. | A projector pass. | `processUpsertExecution`: run open. | Same newer-image guard. |
| `VISIBILITY_CLOSE_EXECUTION` (21) | Writes the closed run's record. | Same: the close commit's image. | A projector pass. | `processCloseExecution`: run closed; close version. | Same newer-image guard. |
| `VISIBILITY_DELETE_EXECUTION` (22) | Removes a deleted run's record. | Deletion commits a tombstone image (`deleted_workflow_projection_context` in `tokeira-storage/src/api.rs`). | A projector pass. | `processDeleteExecution`: waits while a close visibility task is pending. | Same newer-image guard, so the tombstone outranks every earlier image. |

## Archival queue

v1.31.0 executor: `service/history/archival_queue_task_executor.go @ v1.31.0`.

| Task type | v1.31.0 effect | Tokeira mechanism | When | v1.31.0 checks | Tokeira checks |
|---|---|---|---|---|---|
| `ARCHIVAL_ARCHIVE_EXECUTION` (26) | Archives a closed run's history and visibility, then schedules its deletion. | Not implemented: namespaces report archival disabled (`tokeira-edge/src/grpc/translate.rs`). | — | `processArchiveExecutionTask`: archival enabled for the cluster and the namespace. | — |

## State-machine tasks: Nexus operations and callbacks

v1.31.0 runs these through hierarchical state machines. The executors are
`service/history/outbound_queue_active_task_executor.go @ v1.31.0` and
`executeStateMachineTimerTask` in the timer executor. The reference check is in
`service/history/hsm/tasks.go @ v1.31.0`; each task's `Validate` is in
`components/nexusoperations/tasks.go` and `components/callbacks/tasks.go @ v1.31.0`.

| Task type | v1.31.0 effect | Tokeira mechanism | When | v1.31.0 checks | Tokeira checks |
|---|---|---|---|---|---|
| `STATE_MACHINE_OUTBOUND` (27) | Sends a Nexus start or cancel request, or delivers a completion callback. | Nexus: `run_operation` rows `NexusStart`, `NexusCancel` via the outbound driver. Callbacks: first attempt on commit, retries by `run_completion_callback_scanner` (`tokeira-runtime/src/publisher.rs`). | On commit; retries at their time. | The ref's machine transition count matches; `Validate`: the operation, cancellation or callback is still scheduled. | Nexus: scheduled event id and attempt match, not backing off (`apply_nexus_operation_resolved`). Callbacks: index known and not terminal; attempt not checked (`apply_completion_callback_attempted`). |
| `STATE_MACHINE_TIMER` (28) | Fires Nexus timeouts and backoffs, and callback backoffs. | Nexus timeouts: in-memory index (`run_nexus_timeout_scanner` in `tokeira-runtime/src/nexus.rs`). Backoffs: `next_attempt_at` on the operation, or the callback scanner. | Timeouts at their time; a Nexus backoff on a driver round. | `executeStateMachineTimerTask`: run not a zombie; ref valid; `Validate`, e.g. start-to-close only while started. | Timeout: operation pending under its scheduled event id; attempt not checked. Retry: still backing off (`apply_nexus_operation_retry`). |

## CHASM tasks

v1.31.0 runs CHASM tasks from several queues and validates each through its component's registered
validator (`validateTask` in `chasm/tree.go @ v1.31.0`; the history side is
`service/history/chasm_task_util.go @ v1.31.0`).

| Task type | v1.31.0 effect | Tokeira mechanism | When | v1.31.0 checks | Tokeira checks |
|---|---|---|---|---|---|
| `CHASM_PURE` (32) | Applies a component's due pure tasks in one transition. | `ChasmTimerSweeper` runs due pure tasks under a fenced transition (`tokeira-runtime/src/chasm/sweeper.rs`). | At its time. | `executeChasmPureTimerTask`: run not a zombie; time has come; each task's validator. | Each task's validator; an invalid task is dropped (`TaskValidator` in `tokeira-chasm/src/task.rs`). |
| `CHASM` (33) | Runs a component's side-effect task from the transfer, timer, outbound or visibility queue. | Post-commit executors; the committed outbox stays the authority and is rebuilt on recovery (`tokeira-runtime/src/chasm/executor.rs`). | On commit. | The task's validator, run when the executor accesses the component (`ExecuteSideEffectTask` in `chasm/tree.go`). | The outbox is re-validated on every close; an invalid task is dropped (`TaskValidator`). |

## Replication tasks

v1.31.0 executors: `service/history/replication/ @ v1.31.0`. All six are not applicable: Tokeira
doesn't replicate between clusters. A run has one authority, its committed transitions in DSQL (see
[What is excluded](v1.31.0/excluded.md)).

| Task type | v1.31.0 effect |
|---|---|
| `REPLICATION_HISTORY` (1) | Ships new history events to a standby cluster. |
| `REPLICATION_SYNC_ACTIVITY` (2) | Ships an activity's latest state to a standby cluster. |
| `REPLICATION_SYNC_WORKFLOW_STATE` (25) | Ships a closed run's whole state to a standby cluster. |
| `REPLICATION_SYNC_HSM` (30) | Ships state-machine nodes to a standby cluster. |
| `REPLICATION_SYNC_VERSIONED_TRANSITION` (31) | Ships the state changed by one versioned transition to a standby cluster. |
| `REPLICATION_DELETE_EXECUTION` (34) | Deletes a run on standby clusters. |
