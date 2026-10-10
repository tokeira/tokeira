# Bugfix Requirements Document

## Introduction

A workflow names the namespace of a workflow it signals or cancels, or of a child it starts, in the command's `namespace` field. Temporal v1.31.0 treats an empty field as the workflow's own namespace in all three commands (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go @ v1.31.0`):
- RequestCancelExternalWorkflowExecution, lines 870-881;
- StartChildWorkflowExecution, lines 1045-1058, which also writes the parent's namespace name into the command;
- SignalExternalWorkflowExecution, lines 1155-1168.

For a signal or a cancel request, v1.31.0's initiated event keeps the command's namespace as given, empty here, and records the target namespace's id, the workflow's own (`service/history/historybuilder/event_factory.go:606-631, 687-714 @ v1.31.0`). The event that resolves the request records the name and id of the namespace that v1.31.0 looks up from that id (`service/history/transfer_queue_active_task_executor.go:484-560, 610-700, 1926-1957 @ v1.31.0`).

Tokeira handles the three commands differently:
- **Child start** already defaults. The edge writes the parent's namespace name into the command, and the kernel defaults a nil namespace id to the parent's, so the child starts in the parent's namespace and its initiated event records the parent's namespace name and id.
- **Cancel** defaults the target namespace id in the kernel, so the request reaches the right workflow. But the event that resolves it records an empty namespace name.
- **Signal** doesn't default at all. The edge maps an empty namespace to the nil id. The initiated event records the nil id, the signal goes to the nil namespace and fails as not found, and the event that resolves it records the nil id and an empty name.

## Bug Analysis

### Current Behavior (Defect)

1.1 WHEN a workflow task completes with a SignalExternalWorkflowExecution command whose namespace is empty THEN the signal goes to the nil namespace and fails with `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`, even when the workflow it names runs in the workflow's own namespace.

1.2 WHEN such a command is recorded THEN its SignalExternalWorkflowExecutionInitiated event, and the event that resolves it, record the nil namespace id.

1.3 WHEN a signal or a cancel request whose command named no namespace is resolved THEN its ExternalWorkflowExecutionSignaled, SignalExternalWorkflowExecutionFailed, ExternalWorkflowExecutionCancelRequested or RequestCancelExternalWorkflowExecutionFailed event records an empty namespace name.

### Expected Behavior (Correct)

2.1 WHEN a SignalExternalWorkflowExecution command's namespace is empty THEN its target SHALL be the workflow's own namespace, as in v1.31.0 (`workflow_task_completed_handler.go:1155-1168 @ v1.31.0`):
- The signal SHALL be sent to the workflow it names in that namespace.
- A signal that names the workflow itself SHALL fail with `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`, as v1.31.0 fails a workflow's signal to itself (`transfer_queue_active_task_executor.go:683-697 @ v1.31.0`).

2.2 WHEN such a command is recorded THEN its SignalExternalWorkflowExecutionInitiated event SHALL record the command's namespace as given, empty, and the workflow's own namespace id.

2.3 WHEN a signal or a cancel request whose command named no namespace is resolved THEN the event that resolves it SHALL record the name and id of the target namespace, the workflow's own. The name SHALL be the one the runtime's namespace resolver gives for the target namespace id, as v1.31.0 records the name of the namespace it looks up from the id.

### Unchanged Behavior (Regression Prevention)

3.1 A StartChildWorkflowExecution command whose namespace is empty SHALL CONTINUE TO start the child in the parent's namespace, and its StartChildWorkflowExecutionInitiated event SHALL CONTINUE TO record the parent's namespace name and id.

3.2 A RequestCancelExternalWorkflowExecution command whose namespace is empty SHALL CONTINUE TO target the workflow's own namespace, and its initiated event SHALL CONTINUE TO record an empty namespace and the workflow's own namespace id.

3.3 A command that names a namespace SHALL CONTINUE TO target that namespace, and its events SHALL CONTINUE TO record the name the command gives.

3.4 A signal or cancel request that names another workflow in another namespace SHALL CONTINUE TO be delivered and resolved as today.

### Out of Scope

- A command that names a namespace that doesn't exist. v1.31.0 fails the workflow task completion with the registry's error (`workflow_task_completed_handler.go:875-880, 1161-1166 @ v1.31.0`); Tokeira derives the namespace id from the name and fails the request as not found when it delivers it.
- Events that runs already recorded with the nil namespace id. They stay as they are.
