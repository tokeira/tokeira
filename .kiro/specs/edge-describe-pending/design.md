# Design Document: Edge Describe & Operational Response Completeness

## Overview

This design threads pending entity data from the kernel's `WorkflowState` through the `ExecutionResolver` → edge DTO → proto translation pipeline for `DescribeWorkflowExecutionResponse`, and sets sensible defaults on `DescribeNamespaceResponse`, `GetClusterInfoResponse`, and `DescribeTaskQueueResponse` where the current code hardcodes empty/zero values.

The work is organized into five components:
1. Edge DTO enrichment — add pending entity DTOs and fields to `WorkflowExecutionDescription`
2. Proto translation — map pending DTOs to proto `PendingActivityInfo`, `PendingChildExecutionInfo`, `PendingWorkflowTaskInfo`
3. ExecutionResolver implementations — extract pending data from `WorkflowState` when building descriptions
4. Namespace and cluster info cosmetic fixes — set archival to disabled, populate clusters, version info, shard count
5. DescribeTaskQueue documentation — explicit comments on unsupported versioning fields

The kernel is not modified. All data already exists in `WorkflowState` — the changes are purely in the edge layer and the `ExecutionResolver` implementations that bridge runtime state to edge DTOs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Kernel (unchanged)                                              │
│  WorkflowState.activities: BTreeMap<String, ActivityState>       │
│  WorkflowState.children: BTreeMap<WorkflowId, ChildWorkflowState>│
│  WorkflowState.pending_workflow_task: Option<PendingWorkflowTask>│
└──────────────────────────────┬──────────────────────────────────┘
                               │ WorkflowState (via load_run)
┌──────────────────────────────▼──────────────────────────────────┐
│  ExecutionResolver implementations                               │
│  ─ tokeirad/src/main.rs: describe_execution                      │
│  ─ tokeirad/tests/grpc_roundtrip.rs: describe_execution          │
│  ─ tokeira-edge/tests/grpc_new_endpoints.rs: describe_execution  │
│  Extract pending_activities, pending_children,                   │
│  pending_workflow_task from WorkflowState into edge DTOs         │
└──────────────────────────────┬──────────────────────────────────┘
                               │ WorkflowExecutionDescription (enriched)
┌──────────────────────────────▼──────────────────────────────────┐
│  Edge Layer — Proto Translation (grpc/translate.rs)              │
│  ─ describe_response_to_proto: maps pending DTOs to proto        │
│  ─ namespace_to_proto: archival disabled, clusters populated     │
│  ─ cluster_info_to_proto: version_info, supported_clients, shards│
│  ─ describe_task_queue_response_to_proto: doc comments           │
└─────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### Component 1: Edge DTO enrichment — Pending entity DTOs

**Problem:** `WorkflowExecutionDescription` has no fields for pending activities, children, or workflow task. The `describe_response_to_proto` function uses `..Default::default()` which silently drops these proto fields.

**Design:**

Add three new DTO structs and corresponding fields to `WorkflowExecutionDescription`:

```rust
// New DTOs in translate/mod.rs

/// Edge-facing description of a pending activity.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingActivityDescription {
    pub activity_id: String,
    pub activity_type: String,
    pub is_started: bool,
    pub attempt: u32,
    pub scheduled_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub maximum_attempts: u32,
}

/// Edge-facing description of a pending child workflow.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingChildDescription {
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub workflow_type: String,
    pub initiated_event_id: i64,
    pub parent_close_policy: ParentClosePolicy,
}

/// Edge-facing description of a pending workflow task.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingWorkflowTaskDescription {
    pub is_started: bool,
    pub scheduled_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub attempt: u32,
}
```

Add fields to `WorkflowExecutionDescription`:

```rust
pub struct WorkflowExecutionDescription {
    // ... existing fields ...
    pub pending_activities: Vec<PendingActivityDescription>,
    pub pending_children: Vec<PendingChildDescription>,
    pub pending_workflow_task: Option<PendingWorkflowTaskDescription>,
}
```

**Files changed:**
- `crates/tokeira-edge/src/translate/mod.rs` — add 3 new structs, add 3 fields to `WorkflowExecutionDescription`

### Component 2: Proto translation — Map pending DTOs to proto

**Problem:** `describe_response_to_proto` only populates `workflow_execution_info` and uses `..Default::default()` for `pending_activities`, `pending_children`, and `pending_workflow_task`.

**Design:**

Update `describe_response_to_proto` to map the new DTO fields to proto messages:

```rust
pub fn describe_response_to_proto(
    resp: WorkflowExecutionDescription,
) -> workflowservice::DescribeWorkflowExecutionResponse {
    let pending_activities = resp
        .pending_activities
        .iter()
        .map(pending_activity_to_proto)
        .collect();
    let pending_children = resp
        .pending_children
        .iter()
        .map(pending_child_to_proto)
        .collect();
    let pending_workflow_task = resp
        .pending_workflow_task
        .as_ref()
        .map(pending_wft_to_proto);

    workflowservice::DescribeWorkflowExecutionResponse {
        workflow_execution_info: Some(workflow_execution_info_from_description(resp)),
        pending_activities,
        pending_children,
        pending_workflow_task,
        ..Default::default()
    }
}
```

Note: `workflow_execution_info_from_description` takes ownership of `resp`, so the pending fields must be extracted before that call. The actual implementation will clone or restructure to avoid the ownership conflict — either by extracting pending data first, or by splitting the function.

Add three helper functions:

```rust
fn pending_activity_to_proto(
    act: &PendingActivityDescription,
) -> workflow::PendingActivityInfo {
    workflow::PendingActivityInfo {
        activity_id: act.activity_id.clone(),
        activity_type: Some(tokeira_proto::common::ActivityType {
            name: act.activity_type.clone(),
        }),
        // NOTE: CANCEL_REQUESTED state cannot be surfaced because cancel
        // tracking lives in runtime-local state, not durable WorkflowState.
        state: if act.is_started {
            enums::PendingActivityState::Started as i32
        } else {
            enums::PendingActivityState::Scheduled as i32
        },
        attempt: act.attempt as i32,
        maximum_attempts: act.maximum_attempts as i32,
        scheduled_time: Some(to_proto_timestamp(act.scheduled_at)),
        last_started_time: act.started_at.map(to_proto_timestamp),
        // Fields not yet available from kernel state:
        // last_heartbeat_time, heartbeat_details, last_failure,
        // last_worker_identity, expiration_time — left as default
        ..Default::default()
    }
}

fn pending_child_to_proto(
    child: &PendingChildDescription,
) -> workflow::PendingChildExecutionInfo {
    workflow::PendingChildExecutionInfo {
        workflow_id: child.workflow_id.clone(),
        run_id: child.run_id.clone().unwrap_or_default(),
        workflow_type_name: child.workflow_type.clone(),
        initiated_id: child.initiated_event_id,
        parent_close_policy: parent_close_policy_from_domain(child.parent_close_policy),
    }
}

fn pending_wft_to_proto(
    wft: &PendingWorkflowTaskDescription,
) -> workflow::PendingWorkflowTaskInfo {
    workflow::PendingWorkflowTaskInfo {
        state: if wft.is_started {
            enums::PendingWorkflowTaskState::Started as i32
        } else {
            enums::PendingWorkflowTaskState::Scheduled as i32
        },
        scheduled_time: Some(to_proto_timestamp(wft.scheduled_at)),
        started_time: wft.started_at.map(to_proto_timestamp),
        attempt: wft.attempt as i32,
        ..Default::default()
    }
}
```

Note on `PendingChildExecutionInfo`: The proto has a `workflow_type_name: String` field (not a `WorkflowType` message). The `ChildWorkflowState` in the kernel does not currently carry the child's workflow type name — it only has `child_workflow_id`, `namespace_id`, `child_run_id`, `initiated_event_id`, `started_event_id`, and `parent_close_policy`. To populate `workflow_type_name`, we would need to either:
1. Add `workflow_type: WorkflowType` to `ChildWorkflowState` (kernel change), or
2. Load the child's `WorkflowState` to read its `workflow_type` (expensive), or
3. Leave `workflow_type_name` empty for now.

Option 3 is the pragmatic choice for this spec — the field is informational and the UI can still display the child workflow ID. Option 1 is the correct long-term fix but requires a kernel change that is out of scope for this cosmetic/operational spec. We'll use an empty string and add a TODO comment.

**Files changed:**
- `crates/tokeira-edge/src/grpc/translate.rs` — update `describe_response_to_proto`, add `pending_activity_to_proto`, `pending_child_to_proto`, `pending_wft_to_proto`

### Component 3: ExecutionResolver implementations — Extract pending data

**Problem:** All `ExecutionResolver::describe_execution` implementations build `WorkflowExecutionDescription` from `WorkflowState` but don't extract pending entity data.

**Design:**

Each implementation that loads `WorkflowState` via `repo.load_run(run_key)` already has access to the full state. Add extraction of the three pending fields:

```rust
LoadedRun::Existing(state) => Ok(Some(WorkflowExecutionDescription {
    // ... existing fields ...
    pending_activities: state
        .activities
        .values()
        .map(|act| PendingActivityDescription {
            activity_id: act.activity_id.clone(),
            activity_type: act.activity_type.clone(),
            is_started: act.started_at.is_some(),
            attempt: act.attempt,
            scheduled_at: act.scheduled_at,
            started_at: act.started_at,
            heartbeat_timeout: act.heartbeat_timeout,
            schedule_to_close_timeout: act.schedule_to_close_timeout,
            start_to_close_timeout: act.start_to_close_timeout,
        })
        .collect(),
    pending_children: state
        .children
        .values()
        .map(|child| PendingChildDescription {
            workflow_id: child.child_workflow_id.0.clone(),
            run_id: child.child_run_id.as_ref().map(|r| r.0.to_string()),
            workflow_type: String::new(), // TODO: ChildWorkflowState doesn't carry workflow_type
            initiated_event_id: child.initiated_event_id,
            parent_close_policy: child.parent_close_policy,
        })
        .collect(),
    pending_workflow_task: state.pending_workflow_task.as_ref().map(|pwt| {
        PendingWorkflowTaskDescription {
            is_started: pwt.started_event_id.is_some(),
            scheduled_at: pwt.scheduled_at,
            started_at: pwt.started_at,
            attempt: pwt.attempt,
        }
    }),
})),
```

The `InMemoryExecutionResolver` in `workflow_service.rs` stores pre-built descriptions, so callers that use it must populate the pending fields when calling `set_description`. For test code that doesn't care about pending data, the fields default to empty/None.

**Files changed:**
- `apps/tokeirad/src/main.rs` — update `describe_execution` to extract pending data
- `apps/tokeirad/tests/grpc_roundtrip.rs` — update `describe_execution` to extract pending data
- `crates/tokeira-edge/tests/grpc_new_endpoints.rs` — update `describe_execution` to extract pending data
- `crates/tokeira-edge/tests/grpc_properties.rs` — update `arb_description` proptest generator

### Component 4: Namespace and cluster info cosmetic fixes

**Problem:** `namespace_to_proto` hardcodes archival state to 0 (unspecified), clusters to empty, failover_version to 0. `cluster_info_to_proto` hardcodes `supported_clients` to empty, `version_info` to None, `history_shard_count` to 0.

**Design:**

**Namespace:**

```rust
pub fn namespace_to_proto(
    namespace: NamespaceDescription,
) -> workflowservice::DescribeNamespaceResponse {
    workflowservice::DescribeNamespaceResponse {
        namespace_info: Some(namespace_proto::NamespaceInfo {
            name: namespace.name,
            state: if namespace.deleted {
                enums::NamespaceState::Deleted as i32
            } else {
                enums::NamespaceState::Registered as i32
            },
            description: namespace.description,
            owner_email: namespace.owner_email,
            data: std::collections::BTreeMap::new(),
            id: namespace.namespace_id.unwrap_or_default(),
            capabilities: Some(namespace_proto::namespace_info::Capabilities {
                eager_workflow_start: false,
                sync_update: true,
                async_update: true,
            }),
            supports_schedules: false,
        }),
        config: Some(namespace_proto::NamespaceConfig {
            workflow_execution_retention_ttl: None,
            bad_binaries: None,
            // Tokeira does not support archival — set to DISABLED (1)
            history_archival_state: enums::ArchivalState::Disabled as i32,
            history_archival_uri: String::new(),
            visibility_archival_state: enums::ArchivalState::Disabled as i32,
            visibility_archival_uri: String::new(),
            custom_search_attribute_aliases: namespace.custom_search_attribute_aliases,
        }),
        replication_config: Some(replication_proto::NamespaceReplicationConfig {
            active_cluster_name: namespace.cluster_name.clone(),
            clusters: vec![replication_proto::ClusterReplicationConfig {
                cluster_name: namespace.cluster_name,
            }],
            state: 0,
        }),
        failover_version: 1, // Single-cluster, no failover
        is_global_namespace: namespace.is_global,
        failover_history: Vec::new(),
    }
}
```

Add fields to `NamespaceDescription`:

```rust
pub struct NamespaceDescription {
    // ... existing fields ...
    pub description: String,
    pub owner_email: String,
    pub cluster_name: String,
    pub custom_search_attribute_aliases: std::collections::BTreeMap<String, String>,
}
```

The `namespace_to_description` helper in `workflow_service.rs` that converts `ResolvedNamespace` to `NamespaceDescription` needs to populate the new fields. For now, `description` and `owner_email` come from `ResolvedNamespace` (which may not have them yet — default to empty). `cluster_name` comes from the `ClusterInfo` or a configured cluster name.

**Cluster info:**

Add fields to `ClusterInfo`:

```rust
pub struct ClusterInfo {
    pub cluster_name: String,
    pub version: String,
    pub notes: Vec<String>,
    pub shard_count: i32,                                    // NEW
    pub supported_clients: BTreeMap<String, String>,         // NEW
}
```

Update `cluster_info_to_proto`:

```rust
pub fn cluster_info_to_proto(
    resp: ClusterInfo,
) -> workflowservice::GetClusterInfoResponse {
    workflowservice::GetClusterInfoResponse {
        supported_clients: resp.supported_clients.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        server_version: resp.version.clone(),
        cluster_id: resp.cluster_name.clone(),
        version_info: Some(version::VersionInfo {
            current: Some(version::ReleaseInfo {
                version: resp.version.clone(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        cluster_name: resp.cluster_name,
        history_shard_count: resp.shard_count,
        persistence_store: "in-memory".to_string(),
        visibility_store: "in-memory".to_string(),
    }
}
```

Update `InMemoryOperatorApi::new` to populate defaults:

```rust
pub fn new(cluster_name: impl Into<String>) -> Self {
    let name = cluster_name.into();
    Self {
        cluster_info: RwLock::new(ClusterInfo {
            cluster_name: name,
            version: "0.1.0-dev".to_string(),
            notes: vec!["in-memory operator api".to_string()],
            shard_count: 1,
            supported_clients: [
                ("temporal-go".to_string(), "1.26.0".to_string()),
                ("temporal-java".to_string(), "1.22.0".to_string()),
                ("temporal-python".to_string(), "1.6.0".to_string()),
                ("temporal-typescript".to_string(), "1.10.0".to_string()),
            ].into_iter().collect(),
        }),
        attrs: RwLock::new(BTreeMap::new()),
    }
}
```

**Files changed:**
- `crates/tokeira-edge/src/translate/mod.rs` — add fields to `NamespaceDescription`
- `crates/tokeira-edge/src/grpc/translate.rs` — update `namespace_to_proto`, `cluster_info_to_proto`
- `crates/tokeira-edge/src/operator_service.rs` — add fields to `ClusterInfo`, update `InMemoryOperatorApi::new`
- `crates/tokeira-edge/src/workflow_service.rs` — update `namespace_to_description` helper

### Component 5: DescribeTaskQueue documentation

**Problem:** `describe_task_queue_response_to_proto` silently sets `worker_version_capabilities: None` and `versions_info: Default::default()` without explaining why.

**Design:**

Add explicit documentation comments:

```rust
pub fn describe_task_queue_response_to_proto(
    resp: EdgeDescribeTaskQueueResponse,
) -> workflowservice::DescribeTaskQueueResponse {
    workflowservice::DescribeTaskQueueResponse {
        pollers: resp
            .pollers
            .into_iter()
            .map(|poller| {
                tokeira_proto::public::temporal::api::taskqueue::v1::PollerInfo {
                    last_access_time: poller.last_access_time.map(to_proto_timestamp),
                    identity: poller.identity,
                    rate_per_second: poller.rate_per_second,
                    // Tokeira does not yet support worker versioning capabilities
                    // on pollers. This field will be populated when worker versioning
                    // transport is implemented (Feature 5: edge-worker-versioning-transport).
                    worker_version_capabilities: None,
                }
            })
            .collect(),
        task_queue_status: resp.backlog_count_hint.map(|backlog_count_hint| {
            tokeira_proto::public::temporal::api::taskqueue::v1::TaskQueueStatus {
                backlog_count_hint,
                ..Default::default()
            }
        }),
        // Tokeira does not yet support task queue versioning info.
        // This field will be populated when worker versioning transport
        // is implemented (Feature 5: edge-worker-versioning-transport).
        versions_info: Default::default(),
    }
}
```

**Files changed:**
- `crates/tokeira-edge/src/grpc/translate.rs` — add documentation comments to `describe_task_queue_response_to_proto`

## Data Models

### New: `PendingActivityDescription` (edge DTO)

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct PendingActivityDescription {
    pub activity_id: String,
    pub activity_type: String,
    pub is_started: bool,
    pub attempt: u32,
    pub scheduled_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub heartbeat_timeout: Option<Duration>,
    pub schedule_to_close_timeout: Option<Duration>,
    pub start_to_close_timeout: Option<Duration>,
}
```

### New: `PendingChildDescription` (edge DTO)

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct PendingChildDescription {
    pub workflow_id: String,
    pub run_id: Option<String>,
    pub workflow_type: String,
    pub initiated_event_id: i64,
    pub parent_close_policy: ParentClosePolicy,
}
```

### New: `PendingWorkflowTaskDescription` (edge DTO)

```rust
#[derive(Clone, Debug, PartialEq)]
pub struct PendingWorkflowTaskDescription {
    pub is_started: bool,
    pub scheduled_at: OffsetDateTime,
    pub started_at: Option<OffsetDateTime>,
    pub attempt: u32,
}
```

### Modified: `WorkflowExecutionDescription` (edge DTO)

```rust
pub struct WorkflowExecutionDescription {
    // ... existing fields ...
    pub pending_activities: Vec<PendingActivityDescription>,     // NEW
    pub pending_children: Vec<PendingChildDescription>,          // NEW
    pub pending_workflow_task: Option<PendingWorkflowTaskDescription>, // NEW
}
```

### Modified: `NamespaceDescription` (edge DTO)

```rust
pub struct NamespaceDescription {
    pub name: String,
    pub namespace_id: Option<String>,
    pub is_global: bool,
    pub visibility_enabled: bool,
    pub deleted: bool,
    pub description: String,                                     // NEW
    pub owner_email: String,                                     // NEW
    pub cluster_name: String,                                    // NEW
    pub custom_search_attribute_aliases: BTreeMap<String, String>, // NEW
}
```

### Modified: `ClusterInfo` (operator service)

```rust
pub struct ClusterInfo {
    pub cluster_name: String,
    pub version: String,
    pub notes: Vec<String>,
    pub shard_count: i32,                                        // NEW
    pub supported_clients: BTreeMap<String, String>,             // NEW
}
```

## Correctness Properties

### Property 1: Pending activities count preservation

*For any* `WorkflowExecutionDescription` with N `pending_activities` entries (N ≥ 0), `describe_response_to_proto` SHALL produce a `DescribeWorkflowExecutionResponse` with exactly N `PendingActivityInfo` entries.

**Validates:** Requirement 1 (AC 1.1, 1.6)

### Property 2: Pending activity field projection

*For any* `PendingActivityDescription` with `activity_id`, `activity_type`, `attempt`, `scheduled_at`, and `is_started`, `pending_activity_to_proto` SHALL produce a `PendingActivityInfo` where:
- `activity_id` equals the input `activity_id`
- `activity_type.name` equals the input `activity_type`
- `attempt` equals the input `attempt` as i32
- `scheduled_time` is a valid proto `Timestamp`
- `state` is `STARTED` when `is_started` is true, `SCHEDULED` when false
- `last_started_time` is `Some` when `started_at` is `Some`, `None` when `None`

**Validates:** Requirement 1 (AC 1.2, 1.3, 1.4, 1.5)

### Property 3: Pending children count preservation

*For any* `WorkflowExecutionDescription` with M `pending_children` entries (M ≥ 0), `describe_response_to_proto` SHALL produce a `DescribeWorkflowExecutionResponse` with exactly M `PendingChildExecutionInfo` entries.

**Validates:** Requirement 2 (AC 2.1, 2.3)

### Property 4: Pending child field projection

*For any* `PendingChildDescription` with `workflow_id`, `run_id`, `initiated_event_id`, and `parent_close_policy`, `pending_child_to_proto` SHALL produce a `PendingChildExecutionInfo` where:
- `workflow_id` equals the input `workflow_id`
- `run_id` equals the input `run_id` (or empty string if None)
- `initiated_id` equals the input `initiated_event_id`
- `parent_close_policy` maps correctly from the domain enum

**Validates:** Requirement 2 (AC 2.2)

### Property 5: Pending workflow task presence/absence

*For any* `WorkflowExecutionDescription` where `pending_workflow_task` is `Some(pwt)`, `describe_response_to_proto` SHALL produce a `DescribeWorkflowExecutionResponse` where `pending_workflow_task` is `Some`. *For any* description where `pending_workflow_task` is `None`, the proto response SHALL have `pending_workflow_task` as `None`.

**Validates:** Requirement 3 (AC 3.1, 3.4)

### Property 6: Pending workflow task field projection

*For any* `PendingWorkflowTaskDescription` with `is_started`, `scheduled_at`, `started_at`, and `attempt`, `pending_wft_to_proto` SHALL produce a `PendingWorkflowTaskInfo` where:
- `state` is `STARTED` when `is_started` is true, `SCHEDULED` when false
- `scheduled_time` is a valid proto `Timestamp`
- `started_time` is `Some` when `started_at` is `Some`, `None` when `None`
- `attempt` equals the input `attempt` as i32

**Validates:** Requirement 3 (AC 3.2, 3.3)

## Error Handling

No new error paths are introduced. All changes add data to existing success paths:

- Pending entity extraction reads from `WorkflowState` fields that are always populated (the `BTreeMap`s default to empty, `Option<PendingWorkflowTask>` defaults to `None`).
- The proto translation helpers produce valid proto messages for all input values — empty strings, zero attempts, and None timestamps all map to valid proto defaults.
- Namespace and cluster info changes replace hardcoded values with different hardcoded values (archival disabled, failover_version 1, shard_count 1) — no runtime failure possible.
- If `ChildWorkflowState` doesn't carry `workflow_type`, the `workflow_type_name` field on `PendingChildExecutionInfo` is set to empty string. This is a known limitation documented with a TODO.

## Testing Strategy

### Property-based tests (proptest, 100 iterations)

1. **Pending activities count and field projection** — Generate arbitrary `WorkflowExecutionDescription` values with 0–10 `PendingActivityDescription` entries (varying activity_id, activity_type, is_started, attempt, timestamps, timeouts). Convert to proto via `describe_response_to_proto`. Assert the proto has the same count of `PendingActivityInfo` entries, and each entry's fields match the input. (Properties 1, 2)

2. **Pending children count and field projection** — Generate arbitrary descriptions with 0–5 `PendingChildDescription` entries (varying workflow_id, run_id presence, initiated_event_id, parent_close_policy). Convert to proto. Assert count and field matching. (Properties 3, 4)

3. **Pending workflow task presence and field projection** — Generate arbitrary descriptions with `pending_workflow_task` as `Some` or `None`. When `Some`, vary is_started, timestamps, attempt. Convert to proto. Assert presence/absence matches, and fields match when present. (Properties 5, 6)

### Unit tests (example-based)

- `describe_response_to_proto` with 2 activities (one scheduled, one started) produces correct `PendingActivityInfo` entries with correct states
- `describe_response_to_proto` with 1 child (started, with run_id) produces correct `PendingChildExecutionInfo`
- `describe_response_to_proto` with pending WFT (started) produces correct `PendingWorkflowTaskInfo`
- `describe_response_to_proto` with no pending entities produces empty lists and None
- `namespace_to_proto` produces `history_archival_state = 1` (DISABLED) and non-empty `clusters`
- `cluster_info_to_proto` produces non-empty `supported_clients`, non-None `version_info`, and `history_shard_count = 1`
