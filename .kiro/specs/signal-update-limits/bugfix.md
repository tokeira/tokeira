# Bugfix Requirements Document

## Introduction

Temporal v1.31.0 bounds how many signals and updates a run may take. It refuses a signal once the run has recorded 10,000 (`history.maximumSignalsPerExecution`). It refuses an update when the run already has 10 updates in flight (`history.maxInFlightUpdates`), when the requests of its in-flight updates would reach 20 MiB (`history.maxInFlightUpdatePayloads`), or when its updates in flight and completed number 2,000 (`history.maxTotalUpdates`) (`common/dynamicconfig/constants.go:2289-2303, 2351-2355 @ v1.31.0`).

Tokeira enforces none of these. A run takes any number of signals and updates. Tokeira already counts a run's updates in flight and completed, but only to advise continue-as-new, and a reset run's count of completed updates starts at zero whatever history it copies.

This spec covers the limits on a run's signals and updates. The limits on what a request carries are in [payload-admission-limits](../payload-admission-limits/bugfix.md), those on a workflow task's commands in [workflow-task-command-limits](../workflow-task-command-limits/bugfix.md), and those on a run's history, state and buffered events in [run-growth-limits](../run-growth-limits/bugfix.md).

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a client signals a running run that has recorded 10,000 signals, by SignalWorkflowExecution, by SignalWithStartWorkflowExecution or through a batch operation, THEN the system records the signal

1.2 WHEN a workflow signals another workflow's running run that has recorded 10,000 signals THEN the system records the signal on the target, and the sender records it as delivered

1.3 WHEN an update reaches a running run that has 10 updates in flight THEN the system admits it

1.4 WHEN an update reaches a running run whose in-flight update requests, with its own, measure 20 MiB or more THEN the system admits it

1.5 WHEN an update reaches a running run whose updates in flight and completed number 2,000 or more THEN the system admits it

1.6 WHEN a workflow task's completion accepts or rejects an update that the run doesn't hold, and the run's updates in flight and completed number 2,000 or more, THEN the system records the acceptance, or ignores the rejection

1.7 WHEN a run is reset THEN the new run counts none of the updates its copied history completed

### Expected Behavior (Correct)

2.1 WHEN a signal reaches a running run whose recorded signals number the signal limit or more THEN the system SHALL record nothing of it and SHALL answer `InvalidArgument` "exceeded workflow execution limit for signal events" (`service/history/api/signal_workflow_util.go:53-61`; `service/history/consts/const.go:60-61 @ v1.31.0`). This SHALL apply to SignalWorkflowExecution, to the signals of a batch operation, which sends them through SignalWorkflowExecution, and to SignalWithStartWorkflowExecution whose signal goes to a running run.

2.2 WHEN SignalWorkflowExecution to a running run carries a request id that the run has already applied THEN the system SHALL answer it as a duplicate, with success and nothing recorded, whatever the run's signal count: v1.31.0 checks the request id before the count (`service/history/api/signalworkflow/api.go:40-66 @ v1.31.0`). WHEN SignalWithStartWorkflowExecution's signal goes to a running run at the signal limit THEN the system SHALL refuse it under 2.1 even when the run has already applied its request id: v1.31.0 checks this signal's count before its request id (`service/history/api/signalwithstartworkflow/signal_with_start_workflow.go:273-300 @ v1.31.0`).

2.3 WHEN a signal reaches a running run at the signal limit while the run is closing, that is, while a workflow task is started after a close command was refused, THEN the system SHALL answer 2.1's error, not the closing error: v1.31.0 checks the count first (`signal_workflow_util.go:53-70 @ v1.31.0`).

2.4 WHEN SignalWithStartWorkflowExecution starts a new run THEN the system SHALL NOT refuse its signal for the count. The new run's only signal is its own (`signal_with_start_workflow.go:57-94`; `service/history/api/create_workflow_util.go:76-88 @ v1.31.0`).

2.5 WHEN a workflow's SignalExternalWorkflowExecution command reaches a running run at the signal limit THEN the target SHALL record nothing, and the sending run SHALL record SignalExternalWorkflowExecutionFailed with the cause `SIGNAL_COUNT_LIMIT_EXCEEDED` (`service/history/transfer_queue_active_task_executor.go:708-736 @ v1.31.0`). WHEN the target has already recorded that signal, as on a redelivery, THEN the sending run SHALL record ExternalWorkflowExecutionSignaled, since the target answers a duplicate before it checks the count (2.2).

2.6 A run's signal count SHALL be the number of WorkflowExecutionSignaled events the run has recorded, a buffered signal counting when it is admitted, not again when it is flushed (`AddWorkflowExecutionSignaledEvent` and `ApplyWorkflowExecutionSignaled`, `service/history/workflow/mutable_state_impl.go:5635-5668 @ v1.31.0`). A refused or duplicate signal SHALL NOT count. A run that continues as new, retries or runs its next cron iteration SHALL start its successor at zero. A run that SignalWithStartWorkflowExecution starts SHALL start at one.

2.7 WHEN a run is reset THEN the new run's signal count SHALL be the number of signals in the history it copies plus each signal the reset reapplies, and its count of completed updates the number of updates that copied history completes. A signal the reset reapplies SHALL NOT be refused for the count (`replayResetWorkflow` and `reapplyEvents`, `service/history/ndc/workflow_resetter.go:434-480, 841-880`; `service/history/workflow/mutable_state_rebuilder.go:511-516, 654-657`; `NewRegistry`, `service/history/workflow/update/registry.go:168-224 @ v1.31.0`).

2.8 A run's signal count SHALL be stored with the run and SHALL survive a reload. A run stored before the count existed SHALL count from zero.

2.9 WHEN an update whose id the run doesn't hold reaches a running run that has the in-flight update limit's number of updates in flight or more THEN the system SHALL admit nothing and SHALL answer `ResourceExhausted` with the cause `CONCURRENT_LIMIT`, the scope `NAMESPACE` and the message "limit on number of concurrent in-flight updates has been reached (10)", 10 being the limit (`FindOrCreate` and `checkInFlightLimit`, `registry.go:226-236, 398-413 @ v1.31.0`). An update SHALL be in flight from its admission until it is completed or rejected, accepted or not (`registry.go:372-389, 508-510 @ v1.31.0`). An admitted update that the workflow hasn't accepted SHALL count only while Tokeira holds its request. v1.31.0 keeps such updates only in memory and forgets them when it reloads a run (`NewRegistry`, `registry.go:168-224 @ v1.31.0`), so an update whose request a restart lost SHALL NOT keep its run from taking new updates.

2.10 WHEN such an update reaches a running run whose updates in flight and completed number the total update limit or more THEN the system SHALL admit nothing and SHALL answer `FailedPrecondition` "The limit on the total number of distinct updates in this workflow has been reached (2000). Make sure any duplicate updates share an Update ID so the server can deduplicate them, and consider rejecting updates that you aren't going to process. You can also Continue-as-New to avoid this; we recommend you check Continue-as-New Suggested in your Workflow.", 2000 being the limit (`checkTotalLimit`, `registry.go:438-453 @ v1.31.0`). An update SHALL count as completed when the workflow completes it after accepting it, with a result or a failure. A rejected update SHALL never count (`registry.go:372-389 @ v1.31.0`).

2.11 WHEN such an update reaches a running run, and the encoded sizes of the requests Tokeira holds for the run's admitted updates that the workflow hasn't yet accepted or rejected, with this update's own, reach the in-flight update payload limit, THEN the system SHALL admit nothing and SHALL answer `ResourceExhausted` with the cause `CONCURRENT_LIMIT`, the scope `NAMESPACE` and the message "limit on total payload size of in-flight updates has been reached (20971520 bytes)", 20971520 being the limit (`Admit`, `service/history/workflow/update/update.go:301-311`; `payloadSizeLimiter`, `registry.go:415-436 @ v1.31.0`). A request's size SHALL be its protobuf-encoded size as a `temporal.api.update.v1.Request`.

2.12 The checks of 2.9-2.11 SHALL run in that order, and at most one SHALL answer (`registry.go:391-396`; `update.go:301-311 @ v1.31.0`). They SHALL apply to UpdateWorkflowExecution and to the update in ExecuteMultiOperation when it goes to a running run (`service/history/api/multioperation/api.go:281, 318 @ v1.31.0`). They SHALL follow the checks v1.31.0 makes before them, in its order: that the run is open, that it isn't paused, that its workflow task isn't failing repeatedly, and that it isn't closing (`service/history/api/updateworkflow/api.go:118-155 @ v1.31.0`). Tokeira makes the first two (Out of Scope). They SHALL NOT apply to a request whose update id the run holds or has completed, which joins the update it repeats (`registry.go:226-230, 455-481 @ v1.31.0`). In ExecuteMultiOperation the refusal SHALL be the update operation's error, the start operation's being "Operation was aborted." (`TestReturnUpdateRateLimitError`, `TestReturnUpdateInFlightLimitError`, `tests/update_workflow_test.go:5767-5858 @ v1.31.0`). An update-with-start that starts a new run SHALL NOT be refused for these limits.

2.13 WHEN a workflow task's completion accepts or rejects an update that the run has neither admitted nor accepted, and the run's updates in flight and completed number the total update limit or more, THEN the system SHALL record nothing of the completion and SHALL answer `FailedPrecondition` with 2.10's message. The in-flight and payload limits SHALL NOT apply to such an update (`TryResurrect`, `registry.go:238-249`; `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:367-387, 1450-1459 @ v1.31.0`).

2.14 The limits SHALL be v1.31.0's default values: 10,000 signals, 10 updates in flight, 20 MiB of in-flight update requests and 2,000 updates in all. They SHALL be fixed: Tokeira offers no setting to change them.

### Unchanged Behavior (Regression Prevention)

3.1 WHEN a run stays within every limit THEN the system SHALL CONTINUE TO handle its signals and updates as today

3.2 The continue-as-new advice SHALL CONTINUE TO suggest continuing once a run's updates in flight and completed reach 1,800, as [continue-as-new-advice](../continue-as-new-advice/requirements.md) requires; on a reset run it counts the completed updates of 2.7

3.3 A rejected update SHALL CONTINUE TO leave no event, and its id SHALL CONTINUE TO be admissible again

3.4 A signal that arrives while a workflow task is started SHALL CONTINUE TO be buffered, under the buffered event limits of [run-growth-limits](../run-growth-limits/bugfix.md)

3.5 A run's stored state SHALL CONTINUE TO encode to the same bytes as today while the run has recorded no signal

### Out of Scope

- Byte-exact in-flight payload accounting. v1.31.0 also counts each in-flight update's id twice and each request's type wrapper (`GetSize`, `registry.go:483-490`; `update.go:665-677 @ v1.31.0`); Tokeira measures the requests alone (design).
- The rest of what a restart does to admitted updates whose requests it lost. Tokeira keeps their ids in the run's state: a retry with the same id waits for an update that is never delivered, and the continue-as-new advice still counts them. v1.31.0 forgets them when it reloads a run, so a retry is admitted again. A separate change handles them; 2.9 only keeps them from counting against the limits.
- Updates a reset reapplies. v1.31.0 records their admission in history, so they count in flight after a reload (`registry.go:189-204 @ v1.31.0`). Tokeira holds no request for them, so under 2.9 they don't count.
- v1.31.0's refusal of an update while the run's workflow task keeps failing, or while the run is closing (`updateworkflow/api.go:132-155 @ v1.31.0`), which Tokeira doesn't make. A separate change adds them.
- A completion that accepts or rejects an update the run has already completed. v1.31.0 finds the completed update and fails the workflow task for a bad update message (`registry.go:455-481 @ v1.31.0`); Tokeira's kernel keeps no completed update ids, so 2.13 treats such an update as one the run doesn't hold.
- A signal to a closed run. v1.31.0 answers a duplicate of an applied request id before it checks that the run is open (`signalworkflow/api.go:40-53 @ v1.31.0`); Tokeira answers `NotFound` either way, as today.
- A signal limit of one or less, under which v1.31.0 refuses a signal-with-start that starts a run, since the new run's count includes its own signal (`signal_with_start_workflow.go:85-94 @ v1.31.0`). Tokeira's limit is fixed at 10,000.
- Counting the signals that runs stored before this change have already recorded (2.8).
