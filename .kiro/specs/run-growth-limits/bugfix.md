# Bugfix Requirements Document

## Introduction

Temporal v1.31.0 bounds how much a run may accumulate. Before each write to a running workflow it checks the run's history size (50 MiB, `limit.historySize.error`), its event count (51,200, `limit.historyCount.error`) and its mutable state size (8 MiB, `limit.mutableStateSize.error`), and terminates a run over any of them. It refuses to persist a history batch whose serialized size is over 4 MiB (`system.transactionSizeLimit`), and terminates the run when that batch came from a workflow task completion. It force-closes a started workflow task when more than 100 events, or more than 2 MiB of them, are buffered (`history.maximumBufferedEventsBatch`, `history.maximumBufferedEventsSizeInBytes`). And it stores a retrying activity's last failure cut down to 4 KiB (`limit.mutableStateActivityFailureSize.error`) (`common/dynamicconfig/constants.go:138-142, 360-406, 2340-2350 @ v1.31.0`).

Tokeira enforces none of these except the buffered event count, and that only when a signal is buffered. A run's history and state grow without bound until a write fails at DSQL's limits with a storage error, or, in memory, without ever failing. An activity's stored failure keeps whatever size the worker sent.

This spec covers these limits on a run's growth. The limits on what a request or a workflow task's completion carries are in [payload-admission-limits](../payload-admission-limits/bugfix.md) and [workflow-task-command-limits](../workflow-task-command-limits/bugfix.md); the limits on a run's signals and updates are a separate change.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a run's history is over 50 MiB or 51,200 events THEN the system keeps writing to it

1.2 WHEN a run's state is over 8 MiB THEN the system keeps writing it

1.3 WHEN one write's history batch encodes to more than 4 MiB THEN the system writes it, or fails with a storage error on DSQL

1.4 WHEN events other than signals take a started workflow task's buffer past 100 events, or any events take it past 2 MiB, THEN the system keeps buffering

1.5 WHEN an activity retries after a failure over 4 KiB THEN the system stores the whole failure as its last failure

### Expected Behavior (Correct)

2.1 WHEN a write to a run that stays open finds the run's stored History Size already over the history size limit THEN the system SHALL write nothing of it, SHALL terminate the run with the reason `Workflow history size exceeds limit.`, and SHALL answer the caller `InvalidArgument` with that text. The History Size is the stored size before this write, so the write that crosses the limit succeeds and the next one terminates the run, as in v1.31.0 (`service/history/workflow/context.go:1002-1038`; `common/persistence/execution_manager.go:149 @ v1.31.0`).

2.2 WHEN a write would leave an open run with more events than the history count limit THEN the system SHALL write nothing of it, SHALL terminate the run with the reason `Workflow history count exceeds limit.`, and SHALL answer the caller `InvalidArgument` with that text (`context.go:1040-1076 @ v1.31.0`). The count SHALL be of the events v1.31.0 numbers before it finishes the write. v1.31.0 numbers an externally originated event, such as a signal or an activity's result, only when it flushes the event, and it finishes a write after the check: it force-closes a workflow task for buffered events (2.7), schedules a workflow task for a Nexus operation's event, converts a speculative workflow task, and flushes the events left to flush (`service/history/historybuilder/event_store.go:74-95`; `mutable_state_impl.go:7086-7100, 7191-7250, 7800 @ v1.31.0`). So with a limit of 20, a signal that takes a run to 21 events while its workflow task is scheduled succeeds, and the next signal terminates the run (`TestTerminateWorkflowCausedByHistoryCountLimit`, `tests/sizelimit_test.go:40-224 @ v1.31.0`).

2.3 WHEN a write would leave an open run's state over the state size limit THEN the system SHALL write nothing of it, SHALL terminate the run with the reason `Workflow mutable state size exceeds limit.`, and SHALL answer the caller `InvalidArgument` with that text (`context.go:1080-1113 @ v1.31.0`). The state is measured without its activities' inputs, which v1.31.0 keeps only in history.

2.4 The checks of 2.1-2.3 SHALL run in that order, and at most one SHALL fire (`context.go:406-463 @ v1.31.0`). They SHALL apply to every write that leaves an existing run open, whatever caused it: a client call, a worker's response, a workflow task's completion, or a timer or scanner. They SHALL NOT apply to a run's first write, nor to a write that closes the run or continues it as a new run (`context.go:465-481 @ v1.31.0`). WHEN the refused write starts an activity for a worker's poll THEN the poll SHALL go on to the next task, as v1.31.0's matching does with that error (`service/matching/matching_engine.go:982, 1060-1069 @ v1.31.0`).

2.5 WHEN the system terminates a run under 2.1-2.3 THEN it SHALL do so on the run as stored before the refused write, and SHALL record: if a workflow task is started, WorkflowTaskFailed with the cause `ForceCloseCommand` and the identity `history-service`, or nothing for a transient task; then the buffered events; then WorkflowExecutionTerminated with the reason, no details and the identity `history-service` (`forceTerminateWorkflow`, context.go:1115-1155; `TerminateWorkflow`, `service/history/workflow/util.go:105-147 @ v1.31.0`). The termination SHALL have every effect any termination has, such as the parent close policy, the parent's child resolution and update waiters failing.

2.6 WHEN a write's history batch encodes to more than the history batch size limit THEN the system SHALL write nothing of it and SHALL answer the caller `InvalidArgument` `transaction size of {size} bytes exceeds limit of {limit} bytes`. WHEN the write was a workflow task's completion THEN the system SHALL also terminate the run as 2.5 does, with the reason `Transaction size exceeds limit.` and details of one JSON payload holding that message (`common/persistence/history_manager.go:362-373`; `service/history/handler.go:2309-2310`; `service/history/api/respondworkflowtaskcompleted/api.go:645-674 @ v1.31.0`). This check SHALL apply to every write with history events, a run's first write included, and SHALL follow the checks of 2.1-2.3.

2.7 WHEN a write leaves more buffered events than the buffered event count limit, or buffered events larger than the buffered event size limit, while a workflow task is started THEN the system SHALL force-close the task as it does today for 100 buffered signals, whichever kind of event was buffered, and the write SHALL succeed (`closeTransactionHandleBufferedEventsLimit`, `service/history/workflow/mutable_state_impl.go:8191-8231 @ v1.31.0`).

2.8 WHEN an activity is retried after a failure whose encoded size is over the stored activity failure limit THEN the system SHALL store as its last failure a server failure `Failure exceeds size limit.`, not marked non-retryable, whose cause is the original cut down so that the stored failure fits that limit (`truncateRetryableActivityFailure`, mutable_state_impl.go:6587-6608 @ v1.31.0). The stored failure is what the next ActivityTaskStarted event's last failure, DescribeWorkflowExecution and workflow rules see. The activity's final ActivityTaskFailed event SHALL keep the worker's failure.

2.9 Sizes SHALL be measured as the stores store them, as v1.31.0 measures its own: the History Size as `continue-as-new-advice` defines it, the sum of the run's stored history batch sizes; the event count as 2.2 counts it; the state as its encoded size less each activity's encoded input; a history batch as its encoded size; buffered events by the payloads they carry; and an activity failure as its protobuf-encoded size. A size or count SHALL exceed a limit only when it is greater than the limit.

2.10 The limits SHALL be v1.31.0's default values: history size 50 MiB (warn 10 MiB), history count 51,200 (warn 10,240), state size 8 MiB (warn 1 MiB), history batch 4 MiB, 100 buffered events, 2 MiB of buffered events, and a 4 KiB stored activity failure. They SHALL be fixed: Tokeira offers no setting to change them. Above a warn limit the system SHALL only log.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN a run stays within every limit THEN the system SHALL CONTINUE TO handle its writes as today

3.2 The continue-as-new advice SHALL CONTINUE TO use its own thresholds (4 MiB, 4,096 events) as [continue-as-new-advice](../continue-as-new-advice/requirements.md) requires

3.3 The force-close for buffered events SHALL CONTINUE TO record what it records today: WorkflowTaskFailed with `ForceCloseCommand`, the flushed events and a new workflow task

3.4 A reset SHALL CONTINUE TO write its successor's copied history as today

### Out of Scope

- v1.31.0's state check on a write that closes the run. v1.31.0 still persists the closing write and then answers `InvalidArgument` (`context.go:423-460, 1093-1122 @ v1.31.0`); Tokeira checks only writes that leave the run open.
- DSQL's 1 MiB column and 2 MiB row limits, which a state or batch below these limits can still exceed, and a reset's copied history, which is written as one batch. Separate changes bound both.
- What a timer or scanner does with a refused write. v1.31.0's task executors retry the error and eventually move the task to a dead-letter queue.
- The limits on a run's signals and updates.
- Standalone activities and other CHASM executions, which v1.31.0 checks the same way and terminates through their component tree (`context.go:1138-1145 @ v1.31.0`). Tokeira stores them apart from runs.
