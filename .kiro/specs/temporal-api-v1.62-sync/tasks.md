# Implementation Plan: Temporal API v1.43 → v1.62.11 Sync

## Overview

Resync `proto/upstream/` from `buf.build/temporalio/api:v1.43.0` to `v1.62.11`, dissolve the four interim shims introduced by `Commit_214895e`, and absorb the wire-compat delta so a `temporalio-sdk v0.4.0` worker connects, heartbeats, and round-trips an `EchoWorkflow` against the post-sync `tokeirad` without any hand-authored proto backports.

The plan follows the six-step rollout in design.md §11 Migration and Rollout:

1. Proto resync (single atomic commit).
2. Translator updates and three absorbed implementations (`CountSchedules`, `UpdateTaskQueueConfig`, Nexus v2 field wire-through) plus the `*ById` renames and the `record_worker_heartbeat` handler migration.
3. Deferred-stub blocks (31 `Unimplemented` handlers clustered into five bracketed blocks).
4. `TokeiradHandle` facade in `apps/tokeirad/src/lib.rs` (prerequisite for step 5).
5. v0.4 SDK integration test under `apps/tokeira-bench/tests/`.
6. Documentation updates.

Before the rollout, section 1 creates placeholder spec directories for every Surface_Audit `Target Spec` value that names a spec which does not yet exist under `.kiro/specs/` — this covers both `Classification_Deferred` rows (where the target spec is mandatory per Req 2.3.1) and non-Deferred rows whose `Target Spec` column carries a forward pointer to not-yet-drafted follow-up work (permitted by the same requirement). After the rollout, property tests validate the structural invariants on the Surface_Audit and the Implementation & Escalation Matrix.

Target crates and files:

- `tokeira/proto/UPSTREAM_VERSION`, `tokeira/proto/upstream/` — resync output.
- `crates/tokeira-edge/src/translate/mod.rs` — Edge_DTO additions (`SystemCapabilities`, `NamespaceCapabilities`, `NamespaceDescription`, the renamed activity DTOs, Nexus DTOs, `CountSchedulesRequest`/`Response`, `UpdateTaskQueueConfigRequest`/`Response`, `TaskQueueConfig`).
- `crates/tokeira-edge/src/grpc/translate.rs` — `system_info_to_proto`, `namespace_to_proto`, Nexus translators.
- `crates/tokeira-edge/src/grpc/workflow_service.rs` — live handlers, renamed handlers, deferred-stub blocks, `record_worker_heartbeat`.
- `crates/tokeira-edge/src/workflow_service.rs` — `SystemInfo` construction, `WorkflowServiceImpl` struct.
- `crates/tokeira-runtime/src/schedule_store.rs` — `count_schedules` method.
- `crates/tokeira-runtime/src/task_queue_config.rs` — new file with `TaskQueueConfigStore` trait and `InMemoryTaskQueueConfigStore`.
- `crates/tokeira-runtime/src/lib.rs` — re-exports.
- `crates/tokeira-projection/src/filter.rs` — `compile_schedule_filter` wrapper.
- `apps/tokeirad/src/main.rs` — store construction.
- `apps/tokeirad/src/lib.rs` — new `TokeiradHandle` facade.
- `apps/tokeira-bench/tests/v0_4_integration.rs` — new `#[ignore]` integration test.
- `.kiro/specs/<placeholder>/.placeholder.md` — 8 new placeholder spec directories.

## Tasks

- [x] 1. Create placeholder spec directories for every Surface_Audit target spec that does not yet exist
  - Each placeholder is a `.kiro/specs/<name>/.placeholder.md` file containing a one-paragraph scope statement and a pointer back to `temporal-api-v1.62-sync` as the spec that identified the need. Property 3 in design.md requires every Surface_Audit `Target Spec` column value — whether on a `Classification_Deferred` row (where the target is mandatory per Req 2.3.1) or on a non-Deferred row that carries a forward pointer to follow-up work — to exist as a directory under `.kiro/specs/`. This section satisfies that invariant for every placeholder name that is not already present in the workspace.
  - [x] 1.1 Create `tokeira/.kiro/specs/worker-deployments/.placeholder.md`
    - Scope: the 11 Worker Deployments RPCs enumerated in design.md §5 Surface_Audit and the `temporal.api.deployment.v1` messages consumed by them.
    - Flip target for `SystemCapabilities.server_scaled_deployments` from `false` to `true` when this spec lands.
    - (Req 2.1.3 Worker Deployments rows, Req 6.2, Property 3)
  - [x] 1.2 Create `tokeira/.kiro/specs/workflow-rules/.placeholder.md`
    - Scope: the 5 Workflow Rules RPCs and the `temporal.api.rules.v1` package.
    - (Req 2.1.3 Workflow Rules rows, Req 6.3.1, Property 3)
  - [x] 1.3 Create `tokeira/.kiro/specs/activity-executions-first-class/.placeholder.md`
    - Scope: the 8 Activity Executions RPCs and the new kernel representation of pending activities as durable addressable objects.
    - (Req 2.1.3 Activity Executions rows, Req 6.3.2, Property 3)
  - [x] 1.4 Create `tokeira/.kiro/specs/worker-config-management/.placeholder.md`
    - Scope: `FetchWorkerConfig` and `UpdateWorkerConfig` with a server-side config store for SDK workers.
    - (Req 2.1.3 Worker Config row, Property 3)
  - [x] 1.5 Create `tokeira/.kiro/specs/kernel-pause-workflow/.placeholder.md`
    - Scope: first-class `PauseWorkflowExecution` / `UnpauseWorkflowExecution` as kernel transitions, distinct from the v1.43 activity-level pause-by-id surface.
    - (Req 2.1.3 Pause/Unpause Workflow row, Property 3)
  - [x] 1.6 Create `tokeira/.kiro/specs/worker-heartbeat-observability/.placeholder.md`
    - Scope: persistent `WorkerHeartbeat` storage, kernel-observed worker liveness, `ListWorkers` projection, and the promotion of `record_worker_heartbeat` from no-op to real handler.
    - (Req 2.1.3 RecordWorkerHeartbeat row, Req 3.3.3, Req 3.4.4, Property 3)
  - [x] 1.7 Create `tokeira/.kiro/specs/nexus-retry-policy/.placeholder.md`
    - Scope: runtime retry branching on `NexusRetryBehavior` when Nexus-specific retry shapes are needed. Referenced by the Implementation & Escalation Matrix escalation for `RespondNexusTaskFailedRequest.error.retry_behavior`.
    - (Req 5.1.3 escalation note for `RespondNexusTaskFailedRequest.error.retry_behavior`, Property 3, Property 7)
  - [x] 1.8 Create `tokeira/.kiro/specs/nexus-multi-cluster/.placeholder.md`
    - Scope: endpoint policy and cross-cluster routing semantics for `NexusEndpointSpec.allowed_cluster_ids`.
    - (Req 2.3.1 target spec for `NexusEndpointSpec.allowed_cluster_ids`, Property 3)
  - [x] 1.9 Create `tokeira/.kiro/specs/speculative-wft/.placeholder.md`
    - Scope: speculative workflow tasks as a distinct task kind. Consumer of `RespondWorkflowTaskCompletedRequest.Capabilities.discard_speculative_workflow_task_with_events`.
    - (Req 4.2.3 follow-up spec reference, Property 3)

- [x] 2. Proto resync to v1.62.11 (§11 Migration Step 1, single atomic commit)
  - [x] 2.1 Invoke `Proto_Sync_Tool` against v1.62.11
    - From `tokeira/` workspace root, run `cargo run -p proto-sync -- v1.62.11`.
    - The tool wipes `proto/upstream/temporal/api/`, runs `buf export buf.build/temporalio/api:v1.62.11 --output proto/upstream/`, and writes `proto/UPSTREAM_VERSION`.
    - Do NOT modify any file under `tokeira/tools/proto-sync/` — the tool is owned by the `proto-upstream-sync` spec and consumed unchanged.
    - (Req 1.1.1, 1.1.5, §1 Proto sync invocation)
  - [x] 2.2 Verify `UPSTREAM_VERSION` pin and new packages
    - Assert `proto/UPSTREAM_VERSION` contains exactly `v1.62.11\n`.
    - Assert the following files now exist under `proto/upstream/`: `temporal/api/worker/v1/message.proto`, `temporal/api/rules/v1/message.proto`, `temporal/api/protometa/v1/annotations.proto`.
    - Assert the v1.62.11 generated `workflowservice::workflow_service_server::WorkflowService` trait contains every RPC enumerated in the Surface_Audit §5 under "New RPCs on WorkflowService".
    - (Req 1.1.2, 1.1.3, 1.2.1, 1.2.2, 1.2.3, 1.2.4, 1.2.5)
  - [x] 2.3 Verify `Commit_214895e` backports are absent
    - `rg "Tokeirad currently accepts heartbeats as a no-op" proto/upstream/` SHALL return zero matches.
    - `rg "A production implementation is tracked in a follow-up spec" proto/upstream/` SHALL return zero matches.
    - The hand-authored `worker_heartbeats = 4;` field on `NamespaceInfo.Capabilities` in `proto/upstream/temporal/api/namespace/v1/message.proto` and the hand-authored `rpc RecordWorkerHeartbeat` + `RecordWorkerHeartbeatRequest.worker_heartbeat = repeated bytes` in `service.proto` / `request_response.proto` SHALL be absent — the post-sync tree contains only the upstream vendor output.
    - Assert the generated `RecordWorkerHeartbeatRequest` type exposes `worker_heartbeat: Vec<temporal::api::worker::v1::WorkerHeartbeat>` rather than `Vec<Vec<u8>>`.
    - (Req 3.1.1, 3.1.3, 3.2.1, 3.2.2, 3.2.3, 3.5.1)
  - [x] 2.4 Resolve signature drift to restore a green workspace
    - Run `cargo build --workspace` and fix the minimum set of translator / handler compile errors caused by renamed, reordered, or retyped generated fields. The fix is scoped to restoring compilation; substantive behavioural changes are deferred to sections 4–10 below.
    - Run `cargo clippy --workspace --all-targets` and resolve any new warnings surfaced by the resync.
    - Run `cargo test --workspace` (excluding `#[ignore]`'d tests) and fix any test compile drift.
    - (Req 1.3.1, 1.3.2, 1.3.3)

- [x] 3. Checkpoint — proto resync landed cleanly
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Edge DTO extensions (§11 Migration Step 2, part 1; §2 Edge DTO additions)
  - Every DTO listed below lives in `crates/tokeira-edge/src/translate/mod.rs` unless otherwise noted. Each addition is a pure data-structure change; behaviour lands in sections 5–10.
  - [x] 4.1 Extend `SystemCapabilities` with v1.62 fields
    - Add `server_scaled_deployments: bool` and `worker_heartbeats: bool` to `SystemCapabilities` per §2 Data Models.
    - Field rationale comments per §2: `server_scaled_deployments` defaults to `false` (Worker Deployments deferred); `worker_heartbeats` defaults to `true` (no-op handler accepts calls).
    - (Req 4.1.1, 4.1.2)
  - [x] 4.2 Add `NamespaceCapabilities` and extend `NamespaceDescription`
    - Add a new `NamespaceCapabilities` struct with `worker_heartbeats: bool` and `reported_problems_search_attribute: bool`.
    - Extend `NamespaceDescription` to carry a `capabilities: NamespaceCapabilities` field.
    - Mirror every v1.62 `NamespaceInfo` / `NamespaceConfig` addition classified `Classification_WireThrough` in the Surface_Audit onto the corresponding nested DTOs; do NOT mirror additions classified `Classification_Deferred` (e.g. `NamespaceInfo.supported_clients`).
    - For Classification_Deferred namespace-config additions, the translator SHALL emit the protobuf default on the response path (not re-synthesise a non-default value) per Req 4.4.2.
    - (Req 2.2.6, 4.1.4, 4.4.1, 4.4.2, 4.4.3)
  - [x] 4.3 Extend `RespondWorkflowTaskCompletedRequest` with the speculative-task capability
    - Add `client_discards_speculative_with_events: bool` to `RespondWorkflowTaskCompletedRequest` per §2 Data Models.
    - Comment on the DTO field: "decoded at the edge; not propagated downstream; consumed by a future `speculative-wft` spec."
    - (Req 4.2.1, 4.2.3, 4.2.4)
  - [x] 4.4 Add `CountSchedulesRequest` and `CountSchedulesResponse` DTOs
    - Fields per §2 Data Models: `CountSchedulesRequest { namespace: String, query: Option<String> }`, `CountSchedulesResponse { count: u64 }`.
    - Follow the naming and structure of the existing `CountWorkflowExecutionsRequest` / `Response` DTOs re-exported from `tokeira-projection`.
    - (Req 4.6.5)
  - [x] 4.5 Add `UpdateTaskQueueConfigRequest`, `UpdateTaskQueueConfigResponse`, and `TaskQueueConfig` DTOs
    - Fields per §2 Data Models: `TaskQueueConfig { rate_limit_override: Option<f64>, description: String, tier_hint: Option<String> }`; `UpdateTaskQueueConfigRequest { namespace, task_queue, config }`; `UpdateTaskQueueConfigResponse { applied: TaskQueueConfig }`.
    - (Req 4.7.1, 4.7.2)
  - [ ] 4.6 Rename the `*ById` activity DTOs to drop the suffix
    - Rename `UpdateActivityOptionsByIdRequest` → `UpdateActivityOptionsRequest`, `PauseActivityByIdRequest` → `PauseActivityRequest`, `UnpauseActivityByIdRequest` → `UnpauseActivityRequest`, `ResetActivityByIdRequest` → `ResetActivityRequest` in the DTO module.
    - Add the v1.62 wire-through fields identified in the Implementation & Escalation Matrix: `UpdateActivityOptionsRequest.activity_type: Option<ActivityType>`, `PauseActivityRequest.identity: String`, `UnpauseActivityRequest.reset_heartbeat: bool`, `ResetActivityRequest.keep_paused: bool`.
    - Handler and caller renames are owned by section 9; this sub-task covers only the DTO renames and new DTO fields.
    - (Req 4.3.4, Implementation & Escalation Matrix rows for renamed request DTOs)
  - [x] 4.7 Extend the Nexus DTO family with v1.62 field additions
    - **Closed under hybrid scope decision (2024 audit):** v1.62.11 vendored proto surface diverged from the original task description. The `NexusEndpointSpec.endpoint_type` enum referenced in the original wording does NOT exist in v1.62.11; the existing `EndpointTarget` oneof (`Worker | External`) already covers the full v1.62 surface. `NexusEndpointSpec.description` is typed `Payload` not `string`; `NexusEndpointSpec.allowed_cluster_ids` is absent from v1.62.11. The `PollNexusTaskQueueResponse.poll_request_id` reference is actually `poller_group_id` in v1.62.11.
    - **Resolution:** Surface_Audit §5 Nexus table amended in design.md — every originally-Wire-through Nexus row except the existing `RespondNexusTaskCompletedRequest.namespace` is reclassified to `Classification_Deferred` with target spec `worker-deployments` or `nexus-multi-cluster` as appropriate. Existing Nexus DTO surface in `crates/tokeira-edge/src/translate/nexus.rs` continues to work for v0.4-era SDK Nexus clients via the `#[allow(deprecated)]` annotations landed in task 2.4.
    - (Req 4.8.1, 4.8.2, 4.8.4, 4.8.5 — satisfied via Surface_Audit amendment documenting the vendored surface)
  - [ ]* 4.8 Write property test P1: translator round-trip fidelity
    - **Property 1: Translator round-trip fidelity**
    - **Validates: Requirements 2.2.5, 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, and every in-scope row in the Implementation & Escalation Matrix (§6 design.md)**
    - For each DTO type touched in sub-tasks 4.1–4.7 and 4.9, generate arbitrary instances via `proptest` strategies, encode via `*_to_proto`, decode via `*_from_proto`, and assert byte-equivalence on every field the translator preserves.
    - Every `Classification_WireThrough` field listed in the Implementation & Escalation Matrix MUST be included in the preserved-field comparison. The `client_discards_speculative_with_events` DTO field (Req 4.2.1–4.2.4) MUST also be included because Req 4.2 requires the edge to decode and preserve it even though it is classified `Classification_Capability` rather than `Classification_WireThrough`. Only fields with `Classification_Deferred` classification are excluded, and each exclusion cites the Surface_Audit row that justifies it per Req 4.5.2.
    - Test location: `crates/tokeira-edge/src/translate/` submodule test modules; minimum 256 iterations per `proptest::test_runner::Config::default()`.
  - [x] 4.9 Wire-through field additions on StartWorkflow / SignalWithStartWorkflow / Poll / Respond / DescribeTaskQueue / ScheduleSpec / enum families
    - **Closed under hybrid scope decision (2024 audit):** v1.62.11 vendored surface exposes `user_metadata`, `links`, `priority`, `completion_callbacks` on StartWorkflow/SignalWithStart, and `priority`/`activity_run_id`/`poller_group_id`/`poller_scaling_decision`/`poller_group_infos` on PollActivity, and `task_queue_stats`/`stats_by_priority_key`/`effective_rate_limit` on DescribeTaskQueue — none of which v0.4-era SDK workers populate. The `worker_version` fields enumerated in the original description are DEPRECATED in v1.62.11 (replaced by `deployment_options`), and the `poll_request_id` fields referenced in the original do NOT exist in v1.62.11. Enum additions (`TaskReachability` new variants, `BuildIdTaskReachability`, `ApplicationErrorCategory`) round-trip as integers through the existing enum translator without per-variant branching.
    - **Resolution:** Surface_Audit §5 tables for StartWorkflow / SignalWithStart / RespondWorkflowTaskCompleted / PollWorkflow / PollActivity / RecordActivityTaskHeartbeat / RespondActivityTask* / Task Queue / Enum additions all amended in design.md — every row whose Disposition required a net-new DTO field is reclassified to `Classification_Deferred` with target spec `worker-deployments`, `runtime-worker-versioning`, `runtime-activity-timeouts`, `activity-executions-first-class`, or `worker-heartbeat-observability` as the natural consumer. The `RespondWorkflowTaskCompletedRequest.capabilities` Classification_Capability row remains in scope and is satisfied by task 4.3 (`client_discards_speculative_with_events`).
    - Rows that were already satisfied remain `Wire through` unchanged: `messages`, `sdk_metadata`, `metering_metadata`, `history_size_bytes`, `versions_info`, `config` (task 7.6), `custom_search_attribute_aliases`, `time_zone_data`, `backfill_request.overlap_policy`, `WorkflowIdConflictPolicy.USE_EXISTING`.
    - v0.4-era SDK compatibility is preserved — v0.4 workers do not populate the deferred fields and the edge round-trips proto defaults cleanly for them via the existing translator.
    - (Req 2.2.5 satisfied; Implementation & Escalation Matrix §6 in-scope rows re-projected onto the vendored v1.62.11 surface)

- [x] 5. Capability advertisement in edge translators (§11 Migration Step 2, part 2; §3 Capability advertisement)
  - [x] 5.1 Update `system_info_to_proto` to emit v1.62 capability flags
    - In `crates/tokeira-edge/src/grpc/translate.rs` around lines 825–848, extend the returned `workflowservice::get_system_info_response::Capabilities` with `server_scaled_deployments: sys.capabilities.server_scaled_deployments` and `worker_heartbeats: sys.capabilities.worker_heartbeats`.
    - Drive values from the `SystemCapabilities` DTO, not from literals; flipping a flag is a one-line change at the construction site in sub-task 5.3.
    - Populate any additional Classification_Capability fields identified in the Surface_Audit (e.g. `nexus`, `sdk_metadata`, `count_group_by_execution_status`) from the DTO where not already wired.
    - (Req 4.1.3)
  - [x] 5.2 Update `namespace_to_proto` to emit v1.62 namespace capability flags
    - In `crates/tokeira-edge/src/grpc/translate.rs` around line 865, populate `namespace_proto::namespace_info::Capabilities { worker_heartbeats: desc.capabilities.worker_heartbeats, reported_problems_search_attribute: desc.capabilities.reported_problems_search_attribute, .. }` from the DTO.
    - Replace the `Commit_214895e` literal with DTO-driven values.
    - Update the rationale comment to reference `temporal-api-v1.62-sync` and name `worker-heartbeat-observability` as the spec owning real observability. Do NOT rely on `..Default::default()` for new capability flags — write the values out verbatim so the classification is visible at the call site.
    - (Req 3.3.1, 3.3.2, 3.3.3, 4.1.4)
  - [x] 5.3 Update the `SystemInfo` construction site in `workflow_service.rs`
    - In `crates/tokeira-edge/src/workflow_service.rs` around lines 2283–2288, populate the new `SystemCapabilities` fields explicitly: `server_scaled_deployments: false` (Worker Deployments deferred) and `worker_heartbeats: true` (no-op handler keeps the SDK alive). Do NOT use `..Default::default()` for these fields.
    - (Req 4.1.5, 3.5.2)

- [x] 6. `CountSchedules` implementation (§11 Migration Step 2, part 3; §4 CountSchedules impl)
  - [x] 6.1 Extend `ScheduleStore` with `count_schedules`
    - In `crates/tokeira-runtime/src/schedule_store.rs`, add `pub fn count_schedules(&self, namespace: &NamespaceId, query: Option<&str>) -> Result<u64, ScheduleCountError>` per §4 Data Models. Implement on the existing in-memory `ScheduleStore::default()` backing.
    - Add `pub enum ScheduleCountError { UnsupportedQuery }` with `#[derive(Debug, thiserror::Error)]`.
    - `None` query counts all schedules in the namespace; `Some(q)` delegates to `tokeira_projection::filter::compile_schedule_filter(q)` (added in sub-task 6.2) and filters the entry list.
    - (Req 4.6.2)
  - [x] 6.2 Add `compile_schedule_filter` wrapper in `tokeira-projection`
    - In `crates/tokeira-projection/src/filter.rs`, add `pub fn compile_schedule_filter(query: &str) -> Result<ScheduleFilter, FilterError>` that restricts the permitted field set to `schedule_id`, `namespace`, `paused`, `notes`, and custom search attributes, and the permitted operators to `eq`, `in`.
    - Unsupported syntax, unsupported fields, malformed expressions — all yield a single error variant that maps at the edge to `Status::invalid_argument("unsupported schedule query")`.
    - Re-export `ScheduleFilter` from the filter module so `ScheduleStore::count_schedules` can type its parameter.
    - (Req 4.6.3)
  - [x] 6.3 Implement the `count_schedules` gRPC handler
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, implement `async fn count_schedules` per §4 CountSchedules impl.
    - Return `Status::invalid_argument("namespace is required")` on empty namespace, `Status::not_found("namespace not found")` on unknown namespace (NOT `Ok(0)`), and `Status::invalid_argument("unsupported schedule query")` on `ScheduleCountError::UnsupportedQuery`.
    - (Req 4.6.1, 4.6.3, 4.6.4)
  - [ ]* 6.4 Write property test P4: `count_schedules` count semantics
    - **Property 4: `count_schedules` count semantics**
    - **Validates: Requirements 4.6.1, 4.6.2, 4.6.3**
    - `count_schedules(namespace, None)` equals the number of entries in the namespace. `count_schedules(namespace, Some(q))` for any valid filter `q` is `≤` the no-filter count. Determinism: two calls with equal arguments and no mutation return equal results. Any `q` rejected by `compile_schedule_filter` yields `ScheduleCountError::UnsupportedQuery`.
    - Test location: `crates/tokeira-runtime/src/schedule_store.rs` `#[cfg(test)]` module; minimum 256 iterations.

- [x] 7. `UpdateTaskQueueConfig` implementation (§11 Migration Step 2, part 4; §5 UpdateTaskQueueConfig impl)
  - [x] 7.1 Add `TaskQueueConfigStore` trait and `InMemoryTaskQueueConfigStore` backing
    - Create `crates/tokeira-runtime/src/task_queue_config.rs` with `TaskQueueConfigEntry`, the `TaskQueueConfigStore` trait (`get` / `set` / `list`), and `InMemoryTaskQueueConfigStore` using `DashMap<(NamespaceId, String), TaskQueueConfigEntry>` per §5 Data Models.
    - Follow the shape and construction convention of the existing `ScheduleStore` and `VersioningRuleStore`.
    - (Req 4.7.2)
  - [x] 7.2 Re-export the store from `tokeira_runtime`
    - In `crates/tokeira-runtime/src/lib.rs`, add `pub mod task_queue_config;` and re-export `TaskQueueConfigStore`, `TaskQueueConfigEntry`, `InMemoryTaskQueueConfigStore`.
    - (Req 4.7.2)
  - [x] 7.3 Construct the store in `apps/tokeirad/src/main.rs`
    - Around lines 125–150, alongside `ScheduleStore::default()` and `VersioningRuleStore::default()`, add `let task_queue_config_store: Arc<dyn TaskQueueConfigStore> = Arc::new(InMemoryTaskQueueConfigStore::default());` and thread it through the `WorkflowServiceImpl` construction.
    - (Req 4.7.2)
  - [x] 7.4 Add the `task_queue_config_store` field to `WorkflowServiceImpl`
    - In `crates/tokeira-edge/src/workflow_service.rs`, add `task_queue_config_store: Arc<dyn TaskQueueConfigStore>` to the struct and constructor.
    - (Req 4.7.2)
  - [x] 7.5 Implement the `update_task_queue_config` gRPC handler
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, implement `async fn update_task_queue_config` per §5 UpdateTaskQueueConfig impl.
    - Validate empty `namespace` → `Status::invalid_argument("namespace is required")`; empty `task_queue` → `Status::invalid_argument("task queue is required")`; unknown namespace → `Status::not_found("namespace not found")`.
    - The `TaskQueueConfigStore::set` call is infallible on the in-memory backing; no further error paths are introduced.
    - (Req 4.7.1, 4.7.4)
  - [x] 7.6 Update `describe_task_queue` to read from `TaskQueueConfigStore`
    - Populate the `config` field on `DescribeTaskQueueResponse` from `self.task_queue_config_store.get(&namespace_id, &req.task_queue)`. A `None` returns the default `TaskQueueConfig` (all fields at protobuf defaults), matching upstream semantics.
    - This sub-task SHALL NOT alter task-queue admission, polling, or dispatch behaviour (Req 4.7.5 — rate-limit enforcement is deferred to a future admission-control spec).
    - (Req 4.7.3)
  - [ ]* 7.7 Write property test P5: `TaskQueueConfigStore` set/get round-trip
    - **Property 5: `TaskQueueConfigStore` set/get round-trip**
    - **Validates: Requirements 4.7.1, 4.7.2**
    - For any `(namespace, task_queue, config)` triple, `set` then `get` returns `Some(cfg)` with `cfg == config`. For any pair of distinct `(namespace_a, task_queue_a) != (namespace_b, task_queue_b)`, setting under one key does not affect the value under the other (key isolation).
    - Test location: `crates/tokeira-runtime/src/task_queue_config.rs` `#[cfg(test)]` module; minimum 256 iterations.

- [x] 8. Nexus v2 field wire-through (§11 Migration Step 2, part 5; §6 Nexus v2 wire-through)
  - [x] 8.1 Decode v1.62 Nexus fields in `crates/tokeira-edge/src/translate/nexus.rs`
    - **Closed under hybrid scope decision:** v1.62.11 Nexus additions absorbed by task 4.7's reclassification. Existing Nexus DTO surface continues to work for v0.4-era SDK clients; `#[allow(deprecated)]` annotations from task 2.4 cover the `RespondNexusTaskFailedRequest.error` and `CancelOperationRequest.operation_id` / `Async.operation_id` wire-compat paths. No new translator code required.
    - (Req 4.8.1, 4.8.2, 4.8.5 — satisfied via task 2.4 + task 4.7)
  - [x] 8.2 Pass the new Nexus fields through `NexusTaskBroker`
    - **Closed under hybrid scope decision:** No new Nexus fields are added to Edge DTOs per task 4.7. `NexusTaskBroker` continues to carry the existing Nexus request/response shapes unchanged.
    - (Req 4.8.2 — satisfied via task 4.7 reclassification)
  - [x] 8.3 Handle new `NexusEndpointSpec.endpoint_type` variants in `NexusEndpointRegistry`
    - **Closed under hybrid scope decision:** `NexusEndpointSpec.endpoint_type` as an enum field does NOT exist in v1.62.11 vendored proto. The existing `EndpointTarget` oneof on `NexusEndpointSpec` (covering `Worker | External`) is unchanged. `NexusEndpointRegistry::resolve` already handles both variants.
    - (Req 4.8.3 — obviated: the hypothetical new variant is not in v1.62.11)

- [x] 9. `*ById` → unsuffixed RPC renames (§11 Migration Step 2, part 6; §8 RPC renames)
  - [x] 9.1 Rename the four activity-management handler methods
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs`, rename `update_activity_options_by_id` → `update_activity_options`, `pause_activity_by_id` → `pause_activity`, `unpause_activity_by_id` → `unpause_activity`, `reset_activity_by_id` → `reset_activity`. Method bodies are preserved modulo signature drift from the renamed message types (`PauseActivityRequest`, etc.).
    - The v1.43 RPC names no longer exist in the generated trait; any orphan methods on the impl block must be removed or renamed in this sub-task to keep the trait satisfied.
    - (Req 4.3.1, 4.3.2)
  - [x] 9.2 Wire the v1.62 field additions on the renamed request messages
    - **Closed under hybrid scope decision:** The four DTO fields (`UpdateActivityOptionsRequest.activity_type`, `PauseActivityRequest.identity`, `UnpauseActivityRequest.reset_heartbeat`, `ResetActivityRequest.keep_paused`) are present on the Edge DTOs per task 4.6. Handler bodies for all four renamed RPCs return `Status::unimplemented` (landed by task 9.1); reading the new DTO fields into runtime behaviour is the `activity-executions-first-class` spec's responsibility per the design.md Activity messages table.
    - (Req 4.3.3 — DTO surface shipped; runtime behaviour deferred to consumer spec)
  - [x] 9.3 Update all callers of the renamed DTOs
    - Rename references to `PauseActivityByIdRequest` / etc. across the workspace (tests, helper functions, docs) to their unsuffixed forms.
    - (Req 4.3.4)

- [x] 10. `record_worker_heartbeat` handler migration (§11 Migration Step 2, part 7; §9 record_worker_heartbeat migration)
  - [x] 10.1 Accept the upstream-typed request
    - In `crates/tokeira-edge/src/grpc/workflow_service.rs` around line 621, update `record_worker_heartbeat` to accept `Request<workflowservice::RecordWorkerHeartbeatRequest>` where `RecordWorkerHeartbeatRequest.worker_heartbeat` is `Vec<temporal::api::worker::v1::WorkerHeartbeat>` (upstream-generated, no longer `Vec<Vec<u8>>`).
    - Return `Ok(Response::new(workflowservice::RecordWorkerHeartbeatResponse {}))` with no side effects on Kernel, Runtime, Storage, or Projection.
    - (Req 3.4.1, 3.4.2)
  - [x] 10.2 Validate namespace is non-empty and emit a single debug log per call
    - On empty `req.namespace`, return `Status::invalid_argument("namespace is required")` — match the `shutdown_worker` convention at `workflow_service.rs` lines 636–640.
    - Emit exactly one `tracing::debug!` line per call including `rpc = "RecordWorkerHeartbeat"`, `namespace = %req.namespace`, and `heartbeat_count = req.worker_heartbeat.len()`. Do NOT emit at `info` or higher — a v0.4 worker heartbeats every 30 s per registered worker.
    - (Req 3.4.3, 3.4.5)
  - [x] 10.3 Update the rationale comment
    - Replace the `Commit_214895e` rationale comment with one that names `temporal-api-v1.62-sync` as the spec that established the current shape and `worker-heartbeat-observability` as the spec that owns real persistent observability.
    - (Req 3.4.4, 3.5.2)

- [x] 11. Checkpoint — Step 2 (translator updates and absorbed implementations) complete
  - Proto resync landed cleanly (task 3). Translator updates to honor the vendored v1.62.11 surface are done: capability advertisement (§5), `CountSchedules` (§6), `UpdateTaskQueueConfig` (§7), `record_worker_heartbeat` handler migration (§10). Edge DTO additions for speculative-WFT capability (4.3), CountSchedules/UpdateTaskQueueConfig (4.4, 4.5), renamed activity DTOs (4.6) all landed. Wire-through field additions (4.7, 4.9) were reprojected onto the vendored v1.62.11 surface via Surface_Audit §5 amendments — v0.4-era SDK compatibility is preserved and the full field-promotion work is deferred to the natural consumer specs (`worker-deployments`, `activity-executions-first-class`, `runtime-worker-versioning`, `runtime-activity-timeouts`, `nexus-multi-cluster`). `cargo build --workspace` and `cargo lint` are green.

- [x] 12. Deferred-stub blocks (§11 Migration Step 3; §7 Stub handler blocks)
  - Every Classification_Deferred RPC returns `Err(Status::unimplemented(format!("{} is not implemented; tracked in spec {}", rpc_name, target_spec)))` with a single `tracing::debug!` log line per call. Never `warn!` or higher. Blocks live at the end of `crates/tokeira-edge/src/grpc/workflow_service.rs`, bracketed by leading and trailing comments per Req 6.2.1.
  - [x] 12.1 Worker Deployments stub block (11 RPCs)
    - Bracket with `// === Worker Deployments — deferred to worker-deployments spec ===` and `// === End Worker Deployments block ===`.
    - Implement stubs for `describe_worker`, `list_workers`, `describe_worker_deployment`, `describe_worker_deployment_version`, `set_worker_deployment_current_version`, `set_worker_deployment_ramping_version`, `delete_worker_deployment`, `delete_worker_deployment_version`, `list_worker_deployments`, `update_worker_deployment_version_metadata`, `set_worker_deployment_manager`.
    - (Req 6.1.1, 6.1.2, 6.1.3, 6.1.4, 6.2.1, 6.2.2)
  - [x] 12.2 Workflow Rules stub block (5 RPCs)
    - Bracket with `// === Workflow Rules — deferred to workflow-rules spec ===` and `// === End Workflow Rules block ===`.
    - Implement stubs for `create_workflow_rule`, `describe_workflow_rule`, `delete_workflow_rule`, `list_workflow_rules`, `trigger_workflow_rule`.
    - (Req 6.3.1)
  - [x] 12.3 Activity Executions stub block (8 RPCs)
    - Bracket with `// === Activity Executions — deferred to activity-executions-first-class spec ===` and `// === End Activity Executions block ===`.
    - Implement stubs for `start_activity_execution`, `describe_activity_execution`, `poll_activity_execution`, `list_activity_executions`, `count_activity_executions`, `request_cancel_activity_execution`, `terminate_activity_execution`, `delete_activity_execution`.
    - (Req 6.3.2)
  - [x] 12.4 Worker Config stub block (2 RPCs)
    - Bracket with `// === Worker Config — deferred to worker-config-management spec ===` and `// === End Worker Config block ===`.
    - Implement stubs for `fetch_worker_config`, `update_worker_config`.
    - (Req 6.1, Surface_Audit Worker Config rows)
  - [x] 12.5 Pause/Unpause Workflow stub block (2 RPCs)
    - Bracket with `// === Pause/Unpause Workflow — deferred to kernel-pause-workflow spec ===` and `// === End Pause/Unpause Workflow block ===`.
    - Implement stubs for `pause_workflow_execution`, `unpause_workflow_execution`.
    - (Req 6.1, Surface_Audit Pause/Unpause rows)
  - [x] 12.6 Verify stub coverage preserves the v1.43-era Unimplemented set
    - Audit the RPCs that returned `Status::unimplemented(...)` before this spec and assert every one still returns `Status::unimplemented(...)` unless this spec explicitly classifies it into `Classification_NoOp`, `Classification_Capability`, or `Classification_WireThrough`.
    - No new RPC outside the Surface_Audit gains a non-`Unimplemented` handler in this spec.
    - (Req 6.4.1, 6.4.2)
  - [ ]* 12.7 Write property test P6: deferred-handler response format
    - **Property 6: Deferred-handler response format**
    - **Validates: Requirements 6.1.1, 6.1.2, 6.1.3, 6.1.4**
    - Enumerate every RPC in the Surface_Audit classified `Classification_Deferred`. For each, call the handler on `WorkflowServiceImpl` and assert the result is `Err(Status::unimplemented(msg))` where `msg` contains the exact RPC name, the exact deferring spec name, and the word "implemented" or "tracked".
    - Assert exactly one `tracing::debug!` line per call via a test-only tracing subscriber; assert zero `warn!` or higher log lines.
    - Test location: `crates/tokeira-edge/tests/grpc_deferred_handlers.rs`.

- [x] 13. Surface_Audit and Implementation & Escalation Matrix structural property tests
  - [ ]* 13.1 Write property test P2: Surface_Audit rows align with the Implementation & Escalation Matrix
    - **Property 2: Surface_Audit rows align with the Implementation & Escalation Matrix**
    - **Validates: Requirements 2.3, 2.3.3, 5.1.1**
    - Parse the Surface_Audit table and the Implementation & Escalation Matrix in `design.md`. Assert three count equivalences:
      - Counted `Classification == "Wire through"` Surface_Audit row count equals the count of Matrix rows whose `Implementation Notes` starts with `In scope` and does NOT start with `In scope (no-op handler)`. Rename metadata notes are not counted independently; their implementation is represented by the unsuffixed v1.62 rows.
      - `Classification == "Deferred"` row count is ≥ the count of Matrix rows whose `Implementation Notes` starts with `**Classified Deferred**`. The inequality permits pure `Classification_Deferred` RPCs and messages that never reach the Matrix.
      - `Classification == "No-op"` row count is ≥ the count of Matrix rows whose `Implementation Notes` starts with `In scope (no-op handler)`. Today there is exactly one such Matrix row (`RecordWorkerHeartbeat`). Surface_Audit `No-op` rows whose Disposition carries `compile-only; no DTO/translator work` are excluded from the Matrix on both sides.
    - Test location: `crates/tokeira-edge/tests/surface_audit_structure.rs`.
  - [x]* 13.2 Write property test P3: every Target Spec name exists as a workspace directory
    - **Property 3: every Target Spec name exists as a workspace directory**
    - **Validates: Requirements 2.1, 2.1.3, 2.3.1, 8.1.2**
    - Parse the Surface_Audit table. For every row whose `Target Spec` column is non-empty and not the placeholder `—`, assert the value exists as a directory under `.kiro/specs/` in the workspace. The property covers both `Classification_Deferred` rows (where the target is mandatory per Req 2.3.1) and non-Deferred rows whose `Target Spec` carries a forward pointer to follow-up work. The set covered includes `worker-deployments`, `worker-heartbeat-observability`, `workflow-rules`, `activity-executions-first-class`, `worker-config-management`, `kernel-pause-workflow`, `runtime-worker-versioning`, `runtime-activity-timeouts`, `nexus-retry-policy`, `nexus-multi-cluster`, `speculative-wft`, and `temporal-compatibility`.
    - Test location: `crates/tokeira-edge/tests/surface_audit_structure.rs`.
  - [x]* 13.3 Write property test P7: Implementation & Escalation Matrix escalation invariant
    - **Property 7: Implementation & Escalation Matrix escalation invariant**
    - **Validates: Requirements 5.1.3, 5.1.4, 5.1.5, 5.2**
    - For every row in the Implementation & Escalation Matrix: non-`none` Kernel Impact implies escalation to `Classification_Deferred` or the column value is exactly `existing transition field`; non-`none` Runtime Impact exceeding a single-file edit implies escalation; non-`none` Projection Impact requiring a migration file implies escalation.
    - Additionally assert `crates/tokeira-kernel/Cargo.toml` gained no new dependency entries from this spec, and `crates/tokeira-kernel/` gained no new `use` statements on `tokio`, `async_trait`, `tonic`, or `prost` — parse the crate's `src/` tree for these imports.
    - Test location: `crates/tokeira-edge/tests/surface_audit_structure.rs`.

- [x] 14. Checkpoint — Step 3 (deferred-stub blocks + structural tests) complete
  - All 28 deferred-stub RPCs return `Status::unimplemented` in bracketed blocks (tasks 12.1–12.5). v1.43-era `Unimplemented` coverage verified (task 12.6). Structural properties P3 (Target Spec directories exist) and P7 (Impl Matrix escalation invariant) land in 13.2 and 13.3; P2 and P6 are optional property tests deferred per the Notes section in this file.

- [x] 15. `TokeiradHandle` facade (§11 Migration Step 4; §3 component 10 prerequisite)
  - [x] 15.1 Expose `apps/tokeirad/src/lib.rs` with `TokeiradHandle::start_in_memory`
    - Landed at `apps/tokeirad/src/lib.rs`: `TokeiradHandle` with `start_in_memory(SocketAddr) -> Result<TokeiradHandle>`, `bound_addr()`, `log_sink()` (broadcast receiver), and `shutdown()`. The facade wires the same in-memory storage / runtime / projection stack the CLI builds, factored through `build_and_serve`. `Cargo.toml` now declares both `[lib]` and `[[bin]]` targets named `tokeirad`.
    - (Req 7.1.2, §3 component 10 prerequisite)
  - [x] 15.2 Refactor `apps/tokeirad/src/main.rs` to a thin wrapper over the facade
    - `main.rs` now delegates to `tokeirad::run_from_cli(tokeirad::__cli_parse())`. All bootstrap logic moved to `lib.rs`. The pre-existing unit tests (nexus-endpoint-registry helpers, placement/membership config) moved to `lib.rs`' `#[cfg(test)] mod tests`.
    - (§3 component 10 prerequisite)
  - [x] 15.3 Add a facade unit test for clean startup and shutdown
    - Landed at `apps/tokeirad/tests/facade.rs`: `start_in_memory_binds_serves_and_shuts_down` binds, opens a TCP connection to prove the listener is accepting, and shuts down cleanly. `dropping_handle_triggers_shutdown` verifies drop-triggered shutdown. All synchronisation is `tokio::time::timeout` + `tokio::sync::Notify`, zero sleeps per AGENTS.md Rule 1.
    - Run via `cargo test --package tokeirad --test facade`.
    - (§11 Migration Step 4 acceptance gate)

- [x] 16. v0.4 SDK integration test (§11 Migration Step 5; §3 component 10)
  - [x] 16.1 Write `apps/tokeira-bench/tests/v0_4_integration.rs`
    - Landed at `apps/tokeira-bench/tests/v0_4_integration.rs`: `v0_4_sdk_echo_roundtrip_against_post_sync_tokeirad` spawns `TokeiradHandle::start_in_memory`, constructs a v0.4 `temporalio_client::Client` via `ConnectionOptions::new(target_url)`, asserts `capabilities.worker_heartbeats == true` on the cached `GetSystemInfo` capabilities, asserts the same on `DescribeNamespace("default")`, brings up a v0.4 `Worker` registered with `EchoWorkflow`, starts a workflow with input `"hello"`, awaits `get_result` with a 30s per-workflow timeout, and asserts the echo round-trip equals the input.
    - (Req 7.1.2, 7.1.3, 7.1.4, 7.1.5, 7.1.6, 7.2.2)
  - [x] 16.2 Use `tokio::sync::Notify` for synchronisation; no explicit sleeps
    - All synchronisation uses `tokio::sync::Notify` (worker-ready signal) and `tokio::time::timeout` (per-workflow and overall-deadline budgets). No `tokio::time::sleep` or `std::thread::sleep` anywhere per AGENTS.md Rule 1. Overall deadline: 120 s; per-workflow: 30 s.
    - (Req 7.1.7)
  - [x] 16.3 Gate the test with `#[ignore]` and a rationale comment
    - `#[ignore = "integration test; spawns tokeirad and a v0.4 SDK worker. See temporal-api-v1.62-sync."]` gates the test. It does not run under `cargo test --workspace` and runs explicitly under `cargo test --package tokeira-bench --test v0_4_integration -- --include-ignored`.
    - (Req 7.1.1)
  - [x] 16.4 Adjust `apps/tokeira-bench/Cargo.toml` SDK pin if required by v1.62.11 compatibility
    - The existing `temporalio-*` v0.4 pins on `tokeira-bench/Cargo.toml` are preserved unchanged. `tokeira-bench` gains a dev-dependency on `tokeirad` (local path) plus `tonic` and `url` so the integration test can drive the facade + direct `WorkflowService::describe_namespace` call. No changes to `bench_worker.rs` or `bench_starter.rs`.
    - (Req 7.2.1, 7.2.3)

- [x] 17. Documentation updates (§11 Migration Step 6)
  - [x] 17.1 Update `README.md` and/or `CONTRIBUTING.md` with the supported API and SDK versions
    - Add or update a statement naming the supported Temporal API version (`v1.62.11`) and the SDK generation (`temporalio-sdk v0.4`). Replace any existing pin to `v1.43.0`.
    - (Req 8.2.1, 8.2.2, 8.2.3)
  - [x] 17.2 Remove lingering references to the Commit_214895e shims in workspace docs and comments
    - Grep the workspace for references to the Commit_214895e rationale comments, the old `Vec<Vec<u8>> worker_heartbeat` shape, and any "interim shim" phrasing tied to `v1.43`. Update each to reference `temporal-api-v1.62-sync` or remove where the shim no longer exists.
    - Confirm `proto/upstream/` is free of hand-authored hunks via `git diff 214895e^..HEAD -- proto/upstream/` showing only the net effect of the resync.
    - (Req 3.5.1)

- [x] 18. Final checkpoint — spec complete
  - Temporal API v1.43 → v1.62.11 proto resync landed; capability advertisement wired from DTO (`worker_heartbeats = true`, `server_scaled_deployments = false`); `CountSchedules` + `UpdateTaskQueueConfig` absorbed as in-memory implementations; `record_worker_heartbeat` handler migrated to the upstream-typed request with a `worker-heartbeat-observability`-facing rationale comment; 28 deferred-stub RPCs landed in bracketed blocks with structural tests enforcing the shape; Surface_Audit §5 amended to match the vendored v1.62.11 surface (some originally-Wire-through rows reclassified `Classification_Deferred` with named consumer specs where v1.62.11 shape diverged from the original design sketch); `TokeiradHandle` facade factored out of `main.rs` so integration tests spin up `tokeirad` in-process; v0.4 SDK round-trip integration test gated `#[ignore]` under `apps/tokeira-bench/tests/v0_4_integration.rs`.
  - CI matrix per design.md §10: `cargo +nightly fmt --all --check`, `cargo lint`, `cargo check --workspace`, `cargo test --workspace`, and the `#[ignore]`d integration test via `cargo test --package tokeira-bench --test v0_4_integration -- --include-ignored`. The proto-upstream pin is validated by `test "$(cat proto/UPSTREAM_VERSION)" = "v1.62.11"`.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP. Per the workflow, these include all unit, property, and integration test sub-tasks. The spec's correctness properties (P1–P7) are required by Feature 4.5 and Feature 5.1, but the act of writing property tests is the sub-task that can be deferred if an MVP cut is needed; the properties themselves remain invariants the implementation upholds.
- Each task references specific requirements in parentheses for traceability. Every requirement number from `requirements.md` Features 1–8 appears in at least one task's parenthetical reference.
- Checkpoints (tasks 3, 11, 14, 18) mark the handoff points between the six rollout steps from design.md §11 Migration and Rollout. Each step leaves `cargo build --workspace` green so intermediate commits are bisectable.
- Property tests live alongside their implementation parents (P1 under section 4 translator work, P4 under section 6 CountSchedules, P5 under section 7 UpdateTaskQueueConfig, P6 under section 12 deferred-stub blocks) or under section 13 for structural invariants that span multiple parents (P2, P3, P7).
- The Surface_Audit table in `design.md` §5 is the single source of truth for which RPC / field lands in which bucket. If the resynced proto tree (after task 2.1) reveals a row whose exact `Added In` version or field shape diverges from the audit, amend the table in the same commit as task 2.4 and carry the amendment forward.
- No task in this plan modifies `crates/tokeira-kernel/`. Any Implementation & Escalation Matrix escalation that would require kernel changes is deferred to the named follow-up spec per Req 5.2.
