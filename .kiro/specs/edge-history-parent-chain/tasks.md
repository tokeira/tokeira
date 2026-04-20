# Tasks: Edge History Parent Chain

## Group 1: Kernel model enrichment (bottom-up foundation)

- [x] 1.1 Add `ContinueAsNewInitiator` enum to `crates/tokeira-kernel/src/command.rs`
  - Add `ContinueAsNewInitiator` enum with variants `Workflow`, `Retry`, `CronSchedule`
  - File: `crates/tokeira-kernel/src/command.rs`

- [x] 1.2 Add new fields to `StartRequest` and `SignalWithStartRequest`
  - Add `parent_run_id: Option<RunId>`, `parent_namespace_id: Option<NamespaceId>`, `parent_initiated_event_id: i64`, `original_execution_run_id: Option<RunId>`, `continued_failure: Option<Payload>`, `last_completion_result: Option<Payloads>` to `StartRequest`
  - Add same fields to `SignalWithStartRequest`
  - File: `crates/tokeira-kernel/src/command.rs`

- [x] 1.3 Add `retry_policy` to `WorkflowCommand::ContinueAsNew`
  - Add `retry_policy: Option<RetryPolicy>` field
  - File: `crates/tokeira-kernel/src/command.rs`

- [x] 1.4 Add `control: String` to `WorkflowCommand::SignalExternalWorkflowExecution` and `WorkflowCommand::RequestCancelExternalWorkflowExecution`
  - File: `crates/tokeira-kernel/src/command.rs`

- [x] 1.5 Add new fields to `WorkflowState`
  - Add `original_execution_run_id: Option<RunId>`, `parent_run_id: Option<RunId>`, `parent_namespace_id: Option<NamespaceId>`, `parent_initiated_event_id: i64`, `last_completion_result: Option<Payloads>`
  - File: `crates/tokeira-kernel/src/state.rs`

- [x] 1.6 Enrich `HistoryEventKind::WorkflowExecutionStarted` with parent and chain fields
  - Add `parent_workflow_id: Option<WorkflowId>`, `parent_run_id: Option<RunId>`, `parent_namespace_id: Option<NamespaceId>`, `parent_initiated_event_id: i64`, `original_execution_run_id: Option<RunId>`, `continued_failure: Option<Payload>`, `last_completion_result: Option<Payloads>`
  - File: `crates/tokeira-kernel/src/event.rs`

- [x] 1.7 Enrich `HistoryEventKind::WorkflowExecutionContinuedAsNew` with new fields
  - Add `retry_policy: Option<RetryPolicy>`, `initiator: ContinueAsNewInitiator`, `failure: Option<Payload>`, `last_completion_result: Option<Payloads>`
  - File: `crates/tokeira-kernel/src/event.rs`

- [x] 1.8 Add `control: String` to `SignalExternalWorkflowExecutionInitiated` and `RequestCancelExternalWorkflowExecutionInitiated` event variants
  - File: `crates/tokeira-kernel/src/event.rs`

- [x] 1.9 Add `parent_run_id: RunId` and `parent_namespace_id: NamespaceId` to `DispatchOp::StartChildWorkflow`
  - File: `crates/tokeira-kernel/src/transition.rs`

**Checkpoint: All kernel model types compile. Run `cargo build -p tokeira-kernel` — expect compilation errors in kernel.rs (apply methods) and downstream crates. The types are correct.**

## Group 2: Kernel apply methods

- [x] 2.1 Update `apply_start` to thread new fields from `StartRequest` into `WorkflowState` and `WorkflowExecutionStarted` event
  - Populate `original_execution_run_id` as `req.original_execution_run_id.or(Some(req.run_id))`
  - Populate parent fields, continued_failure, last_completion_result from req
  - File: `crates/tokeira-kernel/src/kernel.rs`

- [x] 2.2 Update `apply_signal_with_start` to thread new fields (same pattern as apply_start)
  - File: `crates/tokeira-kernel/src/kernel.rs`

- [x] 2.3 Update `apply_workflow_task_completed` ContinueAsNew arm to emit enriched event
  - Set `retry_policy` from command field, falling back to `builder.state.retry_policy`
  - Set `initiator: ContinueAsNewInitiator::Workflow`
  - Set `failure: None`, `last_completion_result: None`
  - File: `crates/tokeira-kernel/src/kernel.rs`

- [x] 2.4 Update `apply_workflow_task_completed` SignalExternalWorkflowExecution arm to thread `control`
  - Thread `control` from command into `SignalExternalWorkflowExecutionInitiated` event
  - File: `crates/tokeira-kernel/src/kernel.rs`

- [x] 2.5 Update `apply_workflow_task_completed` RequestCancelExternalWorkflowExecution arm to thread `control`
  - Thread `control` from command into `RequestCancelExternalWorkflowExecutionInitiated` event
  - File: `crates/tokeira-kernel/src/kernel.rs`

- [x] 2.6 Update `apply_workflow_task_completed` StartChildWorkflow arm to populate new `DispatchOp` fields
  - Add `parent_run_id: builder.state.run_id` and `parent_namespace_id: builder.state.namespace_id`
  - File: `crates/tokeira-kernel/src/kernel.rs`

- [x] 2.7 Update `replay_history_prefix` and `apply_replayed_event` to handle new fields
  - Extract new fields from `WorkflowExecutionStarted` during replay
  - Handle new fields on `WorkflowExecutionContinuedAsNew`, `SignalExternalWorkflowExecutionInitiated`, `RequestCancelExternalWorkflowExecutionInitiated`
  - File: `crates/tokeira-kernel/src/kernel.rs`

**Checkpoint: `cargo build -p tokeira-kernel` compiles. Kernel tests may need updating but the core logic is correct.**

## Group 3: Fix kernel tests

- [x] 3.1 Update golden tests to populate new fields on `StartRequest`, `WorkflowCommand::ContinueAsNew`, signal/cancel-external commands
  - Add default values for new fields at all construction sites
  - File: `crates/tokeira-kernel/tests/golden_tests.rs`

- [x] 3.2 Update property tests to populate new fields
  - Update generators for `StartRequest`, `WorkflowCommand`, event kinds
  - File: `crates/tokeira-kernel/tests/property_tests.rs`

**Checkpoint: `cargo test -p tokeira-kernel` passes.**

## Group 4: History serializer updates

- [x] 4.1 Update `WorkflowExecutionStarted` serializer arm to populate parent and chain fields
  - Populate `parent_workflow_execution`, `parent_workflow_namespace_id`, `parent_initiated_event_id`, `original_execution_run_id`, `continued_failure`, `last_completion_result`
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs`

- [x] 4.2 Update `WorkflowExecutionContinuedAsNew` serializer arm to populate new fields
  - Keep `workflow_execution_timeout: _` wildcard (proto doesn't have this field — kernel carries it for runtime use only)
  - Keep `retry_policy: _` wildcard (proto doesn't have this field either — kernel carries it for runtime use only)
  - `initiator`, `failure`, `last_completion_result` are already populated
  - `continue_as_new_initiator_i32` helper already exists
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs`

- [x] 4.3 Update `SignalExternalWorkflowExecutionInitiated` serializer arm to populate `control`
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs`

- [x] 4.4 Update `RequestCancelExternalWorkflowExecutionInitiated` serializer arm to populate `control`
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs`

- [x] 4.5 Update proptest generators in `history_serializer.rs::tests` for new fields
  - Update `arb_history_event_kind` to generate new fields on all modified variants
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs`

**Checkpoint: `cargo test -p tokeira-edge` — history serializer tests pass.**

## Group 5: Edge inbound translation

- [x] 5.1 Update `proto_command_to_workflow_command` for `ContinueAsNewWorkflowExecutionCommandAttributes` to extract `retry_policy`
  - File: `crates/tokeira-edge/src/grpc/translate.rs`

- [x] 5.2 Update `proto_command_to_workflow_command` for `SignalExternalWorkflowExecutionCommandAttributes` to extract `control`
  - File: `crates/tokeira-edge/src/grpc/translate.rs`

- [x] 5.3 Update `proto_command_to_workflow_command` for `RequestCancelExternalWorkflowExecutionCommandAttributes` to extract `control`
  - File: `crates/tokeira-edge/src/grpc/translate.rs`

- [x] 5.4 Update `workflow_command_to_proto` reverse direction for ContinueAsNew, signal-external, cancel-external
  - File: `crates/tokeira-edge/src/grpc/translate.rs`

**Checkpoint: `cargo build -p tokeira-edge` compiles.**

## Group 6: Runtime threading

- [x] 6.1 Update `handle_start_child_workflow` in `publisher.rs` to populate new `StartRequest` fields
  - Set `parent_run_id`, `parent_namespace_id`, `parent_initiated_event_id` from dispatch op
  - Set `original_execution_run_id: None`, `continued_failure: None`, `last_completion_result: None`
  - File: `crates/tokeira-runtime/src/publisher.rs`

- [x] 6.2 Update `DispatchOp::StartChildWorkflow` handling in `publisher.rs` to pass new fields
  - File: `crates/tokeira-runtime/src/publisher.rs`

- [x] 6.3 Update continue-as-new successor creation in `lane.rs` to populate new `StartRequest` fields
  - Set `original_execution_run_id` from predecessor state
  - Set `continued_failure` from `new_state.close_failure`
  - Set `last_completion_result` from `new_state.close_result`
  - Set `retry_policy` from the CAN event's `retry_policy` field (command override with state fallback — the kernel already resolved this when emitting the event)
  - Set parent fields to `None`/0
  - File: `crates/tokeira-runtime/src/lane.rs`

- [x] 6.4 Fix any remaining compilation errors in runtime crate
  - Update all `StartRequest` construction sites with default values for new fields
  - File: `crates/tokeira-runtime/src/*.rs`

**Checkpoint: `cargo build -p tokeira-runtime` compiles.**

## Group 7: Integration and property tests

- [x] 7.1 [PBT] WorkflowExecutionStarted parent metadata serialization (Property 1, 2)
  - Generate arbitrary events with random parent fields (Some and None)
  - Assert proto parent_workflow_execution, parent_workflow_namespace_id, parent_initiated_event_id match
  - Assert None parent produces empty/default proto fields
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs` (in `mod tests`)

- [x] 7.2 [PBT] WorkflowExecutionStarted chain fields serialization (Property 3, 4, 5)
  - Generate arbitrary events with random original_execution_run_id, continued_failure, last_completion_result
  - Assert proto fields match
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs` (in `mod tests`)

- [x] 7.3 [PBT] WorkflowExecutionContinuedAsNew enriched fields serialization (Property 6)
  - Generate arbitrary events with random retry_policy, initiator, failure, last_completion_result
  - Assert proto fields are populated
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs` (in `mod tests`)

- [x] 7.4 [PBT] Signal/cancel-external control field serialization (Property 7)
  - Generate arbitrary events with random control strings
  - Assert proto control field matches
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs` (in `mod tests`)

- [x] 7.5 Verify ActivityTaskScheduled timeout completeness (Property 8)
  - Confirm existing `prop_history_serialization_round_trip` covers all four timeout fields
  - Add explicit assertion if not already covered
  - File: `crates/tokeira-edge/src/translate/history_serializer.rs` (in `mod tests`)

**Checkpoint: All property tests pass with 100 iterations. `cargo test -p tokeira-edge` passes. `cargo test -p tokeira-kernel` passes. `cargo lint` passes.**
