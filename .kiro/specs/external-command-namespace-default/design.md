# External Command Namespace Default — Bugfix Design

## Overview

An empty namespace in a SignalExternalWorkflowExecution command means the workflow's own namespace, as it already does in a RequestCancelExternalWorkflowExecution or StartChildWorkflowExecution command:
- **The signal's target.** The kernel defaults the signal's nil target namespace id to the workflow's own, as it does for a cancel request. The initiated event, the pending signal and the delivery all use that id.
- **The resolving events' name.** When the runtime resolves a signal or a cancel request, it looks up the target namespace's name from the target id and hands it to the kernel. The resolving event records the command's namespace name when it gave one, and that looked-up name when it didn't.

## Glossary

- **Initiated event:** the SignalExternalWorkflowExecutionInitiated or RequestCancelExternalWorkflowExecutionInitiated event that a command records.
- **Resolving event:** the ExternalWorkflowExecutionSignaled, SignalExternalWorkflowExecutionFailed, ExternalWorkflowExecutionCancelRequested or RequestCancelExternalWorkflowExecutionFailed event that records how a request ended.
- **Own namespace:** the namespace of the workflow that issues the command.
- **Namespace resolver:** the runtime's `NexusNamespaceResolver`, which gives a namespace's name for its id. The engine wires one backed by the edge's namespace cache.

## How this maps onto Tokeira's architecture

v1.31.0 resolves the empty namespace in the workflow task completion handler, and the transfer executor looks up the target namespace's entry from the initiated event's id when it delivers the request, recording that entry's name in the resolving event. Tokeira does the same with one departure:
- **The name comes from the runtime's resolver.** A Tokeira namespace id is a function of its name that can't be inverted, and the kernel holds only the workflow's namespace id. So the runtime's publisher, which delivers the request, looks up the name with the namespace resolver and passes it in the resolution command, as v1.31.0's executor looks up the entry with its registry.

## Bug Details

### Bug Condition

The bug condition holds for a SignalExternalWorkflowExecution or RequestCancelExternalWorkflowExecution command whose namespace is empty:
- For a signal, the target namespace id is the nil id, so the initiated event records it, delivery looks for the workflow in the nil namespace, finds nothing, and fails as not found, and the resolving event records the nil id.
- For a signal or a cancel request, the resolving event records an empty namespace name, since the pending request holds the command's name, which is empty.

### Examples

- **A signal within a namespace.** Workflow A, in namespace `orders`, signals workflow B, also in `orders`, without naming a namespace. Today A's history records SignalExternalWorkflowExecutionFailed with `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`, and B never receives the signal. After the fix, B receives it, and A's history records ExternalWorkflowExecutionSignaled with the namespace `orders` and its id.
- **A cancel request within a namespace.** A asks to cancel B without naming a namespace. Today B is asked to cancel, but A's ExternalWorkflowExecutionCancelRequested event has an empty namespace. After the fix it records `orders`.
- **A signal to itself.** A signals A without naming a namespace. Today the signal fails as not found because the nil namespace holds no A. After the fix it fails as not found because a workflow can't signal itself, as in v1.31.0, and the resolving event records `orders`.

## Expected Behavior

### Preservation Requirements

- A child start whose namespace is empty still starts the child in the parent's namespace, and its initiated event still records the parent's namespace name and id (3.1).
- A cancel request whose namespace is empty still targets the workflow's own namespace, and its initiated event still records an empty namespace and the own namespace id (3.2).
- A command that names a namespace still targets it, and its events still record the name it gives (3.3).
- Requests to workflows in other namespaces are delivered and resolved as today (3.4).

## Root Cause

- **Signal.** The edge maps an empty namespace to the nil namespace id (`namespace_name_to_domain`), as it does for a cancel request. The kernel's handler for `SignalExternalWorkflowExecution` uses that id as given, while its handlers for cancel requests and child starts default a nil id to the workflow's own.
- **Resolving events.** `apply_external_signal_resolved` and `apply_external_cancel_resolved` record the pending request's namespace name, which is the command's, empty when it named none. Nothing looks up the target namespace's name.

## Correctness Properties

Property 1: An empty namespace means the workflow's own

_For any_ workflow and any SignalExternalWorkflowExecution or RequestCancelExternalWorkflowExecution command, the target namespace id SHALL be the command's when it names a namespace, and the workflow's own when it doesn't. The initiated event, the pending request and the delivery SHALL all use that id, and the initiated event SHALL record the command's namespace as given.

**Validates: Requirements 2.1, 2.2, 3.2, 3.3**

Property 2: A resolving event names the target namespace

_For any_ signal or cancel request and any outcome, the resolving event SHALL record the target namespace id and:
- the command's namespace name when it named one;
- otherwise, the name the runtime looked up for the target id, or an empty name when the lookup found none.

**Validates: Requirements 2.3, 3.3, 3.4**

Property 3: A signal within a namespace reaches its workflow

_For any_ two workflows in the same namespace, a signal from one to the other that names no namespace SHALL reach the other, and a signal from a workflow to itself SHALL fail with `EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND`.

**Validates: Requirements 2.1**

## Fix Implementation

### Kernel

- **The signal's target.** `apply_workflow_command` for `SignalExternalWorkflowExecution` defaults a nil `target_namespace_id` to the workflow's namespace id, as the cancel handler does (`workflow_task_completed_handler.go:1155-1168 @ v1.31.0`). The initiated event records that id and the command's namespace as given.
- **The resolution commands.** `ExternalSignalResolvedRequest` and `ExternalCancelResolvedRequest` gain `target_namespace: Option<String>`: the name the runtime looked up for the target namespace id, or none.
- **The resolving events.** `apply_external_signal_resolved` and `apply_external_cancel_resolved` record the pending request's namespace name, and the request's `target_namespace` when the pending request has none.

### Runtime

- **The lookup.** The publisher's `handle_signal_external_workflow` and `handle_cancel_external_workflow` look up the target namespace's name with the namespace resolver, once per delivery, and put it in every resolution command they submit, the self-signal and self-cancel refusals included.
- **Without a resolver.** A runtime built without a namespace resolver, as some tests build one, passes no name, so a resolving event whose command named no namespace keeps an empty name.
- **The resolver's role.** The `NexusNamespaceResolver` documentation names its second user.

### Edge

Nothing changes. The edge maps an empty namespace to the nil id, which the kernel now defaults for signals as it does for cancel requests and child starts.

### Specs this changes

- [runtime-external-signals](../runtime-external-signals/requirements.md): criteria 3.3, 4.7 and 4.8 default a nil target namespace id to the workflow's own; the glossary entries for the two resolution commands add the target namespace's name; and the [design](../runtime-external-signals/design.md)'s paragraph on the dispatch op's namespace says where the default is applied.

## Testing Strategy

### Exploratory Bug Condition Checking

These tests are written and run on the code before the fix, and each must fail there:
- A workflow signals another in its namespace without naming the namespace: the other must receive the signal, and the resolving event must name the namespace.
- A workflow asks to cancel another in its namespace without naming the namespace: the resolving event must name the namespace.

### Property-Based Tests

- Properties 1 and 2 run in `tokeira-kernel`, over generated namespaces, commands with and without a namespace, outcomes, and looked-up names present or absent.
- Property 3 runs in `tokeira-runtime` against the in-memory store with a namespace resolver, over generated workflow ids and signal names.

### Unit Tests

- The kernel defaults a signal's nil target to the workflow's own namespace, and keeps a named one.
- A resolving event keeps the command's name over the looked-up one.
- The runtime passes the looked-up name, and no name without a resolver.
- A child start with an empty namespace starts in the parent's namespace and records its name and id.

### Negative controls

Each control is patched in alone, then run, then reversed so that the tree is byte-identical. Each must fail the test that covers it:
- the signal's nil target left as it is;
- the default applied to a named namespace;
- the looked-up name preferred over the command's;
- the resolving event recording no name;
- the publisher passing no name;
- the child start's default removed from the edge.

### Preservation Checking

The existing external signal, cancel and child workflow tests in the kernel, the runtime and the edge stay green.
