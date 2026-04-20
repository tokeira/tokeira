# Tasks: Edge Describe & Operational Response Completeness

## Task 1: Add pending entity edge DTOs and enrich WorkflowExecutionDescription

- [x] 1.1 Add `PendingActivityDescription` struct to `crates/tokeira-edge/src/translate/mod.rs` with fields: `activity_id: String`, `activity_type: String`, `is_started: bool`, `attempt: u32`, `maximum_attempts: u32`, `scheduled_at: OffsetDateTime`, `started_at: Option<OffsetDateTime>`
- [x] 1.2 Add `PendingChildDescription` struct to `crates/tokeira-edge/src/translate/mod.rs` with fields: `workflow_id: String`, `run_id: Option<String>`, `workflow_type: String`, `initiated_event_id: i64`, `parent_close_policy: ParentClosePolicy`
- [x] 1.3 Add `PendingWorkflowTaskDescription` struct to `crates/tokeira-edge/src/translate/mod.rs` with fields: `is_started: bool`, `scheduled_at: OffsetDateTime`, `started_at: Option<OffsetDateTime>`, `attempt: u32`
- [x] 1.4 Add `pending_activities: Vec<PendingActivityDescription>`, `pending_children: Vec<PendingChildDescription>`, and `pending_workflow_task: Option<PendingWorkflowTaskDescription>` fields to `WorkflowExecutionDescription` in `crates/tokeira-edge/src/translate/mod.rs`
- [x] 1.5 Fix all compilation errors from the new fields on `WorkflowExecutionDescription` — update all construction sites to provide default empty/None values: `InMemoryExecutionResolver::set_description` callers, test helpers in `grpc_properties.rs`, `grpc_new_endpoints.rs`, `grpc_roundtrip.rs`, and `tokeirad/src/main.rs`

## Task 2: Proto translation for pending entities

- [x] 2.1 Add `pending_activity_to_proto` helper function in `crates/tokeira-edge/src/grpc/translate.rs` that maps `PendingActivityDescription` to proto `PendingActivityInfo` with correct `state` (SCHEDULED/STARTED — note: CANCEL_REQUESTED cannot be surfaced from durable state), `activity_id`, `activity_type`, `attempt`, `maximum_attempts`, `scheduled_time`, and `last_started_time`
- [x] 2.2 Add `pending_child_to_proto` helper function in `crates/tokeira-edge/src/grpc/translate.rs` that maps `PendingChildDescription` to proto `PendingChildExecutionInfo` with `workflow_id`, `run_id`, `workflow_type_name`, `initiated_id`, and `parent_close_policy`
- [x] 2.3 Add `pending_wft_to_proto` helper function in `crates/tokeira-edge/src/grpc/translate.rs` that maps `PendingWorkflowTaskDescription` to proto `PendingWorkflowTaskInfo` with correct `state` (SCHEDULED/STARTED), `scheduled_time`, `started_time`, and `attempt`
- [x] 2.4 Update `describe_response_to_proto` in `crates/tokeira-edge/src/grpc/translate.rs` to extract pending data before passing ownership to `workflow_execution_info_from_description`, and populate `pending_activities`, `pending_children`, and `pending_workflow_task` on the proto response

## Task 3: ExecutionResolver implementations — extract pending data from WorkflowState

- [x] 3.1 Update `describe_execution` in `apps/tokeirad/src/main.rs` to extract `pending_activities` from `state.activities`, `pending_children` from `state.children`, and `pending_workflow_task` from `state.pending_workflow_task` when building `WorkflowExecutionDescription`
- [x] 3.2 Update `describe_execution` in `apps/tokeirad/tests/grpc_roundtrip.rs` to extract pending data from `WorkflowState` (same pattern as 3.1)
- [x] 3.3 Update `describe_execution` in `crates/tokeira-edge/tests/grpc_new_endpoints.rs` to extract pending data from `WorkflowState` (same pattern as 3.1)

## Task 4: Namespace and cluster info cosmetic fixes

- [x] 4.1 Add `description: String`, `owner_email: String`, `cluster_name: String`, and `custom_search_attribute_aliases: BTreeMap<String, String>` fields to `NamespaceDescription` in `crates/tokeira-edge/src/translate/mod.rs`
- [x] 4.2 Update `namespace_to_proto` in `crates/tokeira-edge/src/grpc/translate.rs` to set `history_archival_state` and `visibility_archival_state` to `ArchivalState::Disabled` (1), populate `clusters` with a single entry using `namespace.cluster_name`, set `failover_version` to 1, and use `namespace.description`, `namespace.owner_email`, `namespace.custom_search_attribute_aliases`
- [x] 4.3 Update `namespace_to_description` helper in `crates/tokeira-edge/src/workflow_service.rs` to populate the new `NamespaceDescription` fields with defaults (empty strings for description/owner_email, "local" for cluster_name, empty map for aliases)
- [x] 4.4 Add `shard_count: i32` and `supported_clients: BTreeMap<String, String>` fields to `ClusterInfo` in `crates/tokeira-edge/src/operator_service.rs`
- [x] 4.5 Update `InMemoryOperatorApi::new` in `crates/tokeira-edge/src/operator_service.rs` to populate `shard_count: 1` and `supported_clients` with initial SDK version map
- [x] 4.6 Update `cluster_info_to_proto` in `crates/tokeira-edge/src/grpc/translate.rs` to populate `supported_clients` from `ClusterInfo`, set `version_info` with server version, and set `history_shard_count` from `ClusterInfo.shard_count`
- [x] 4.7 Fix all compilation errors from the new fields on `NamespaceDescription` and `ClusterInfo` — update all construction sites in tests and application code

## Task 5: DescribeTaskQueue documentation

- [x] 5.1 Add documentation comments to `describe_task_queue_response_to_proto` in `crates/tokeira-edge/src/grpc/translate.rs` explaining that `worker_version_capabilities` and `versions_info` are intentionally unsupported, referencing Feature 5 (edge-worker-versioning-transport)

## Task 6: Property-based tests for pending entity proto translation

- [x] 6.1 [PBT] Add property test `property_pending_activities_count_and_fields` in `crates/tokeira-edge/tests/grpc_properties.rs` that generates arbitrary `WorkflowExecutionDescription` values with 0–10 `PendingActivityDescription` entries, converts to proto via `describe_response_to_proto`, and asserts: (a) proto `pending_activities` count equals input count, (b) each entry's `activity_id`, `activity_type`, `attempt`, and `state` match the input
- [x] 6.2 [PBT] Add property test `property_pending_children_count_and_fields` in `crates/tokeira-edge/tests/grpc_properties.rs` that generates arbitrary descriptions with 0–5 `PendingChildDescription` entries, converts to proto, and asserts: (a) proto `pending_children` count equals input count, (b) each entry's `workflow_id`, `initiated_id`, and `parent_close_policy` match the input
- [x] 6.3 [PBT] Add property test `property_pending_wft_presence_and_fields` in `crates/tokeira-edge/tests/grpc_properties.rs` that generates arbitrary descriptions with `pending_workflow_task` as `Some` or `None`, converts to proto, and asserts: (a) proto `pending_workflow_task` presence matches input, (b) when present, `state`, `attempt`, and timestamp presence match the input

## Task 7: Unit tests for cosmetic fixes

- [x] 7.1 Add unit test `namespace_archival_disabled` in `crates/tokeira-edge/src/grpc/translate.rs` tests module that verifies `namespace_to_proto` produces `history_archival_state = 1` and `visibility_archival_state = 1`
- [x] 7.2 Add unit test `namespace_clusters_populated` in `crates/tokeira-edge/src/grpc/translate.rs` tests module that verifies `namespace_to_proto` produces non-empty `clusters` list with the local cluster name
- [x] 7.3 Add unit test `cluster_info_populated` in `crates/tokeira-edge/src/grpc/translate.rs` tests module that verifies `cluster_info_to_proto` produces non-empty `supported_clients`, non-None `version_info`, and `history_shard_count >= 1`
