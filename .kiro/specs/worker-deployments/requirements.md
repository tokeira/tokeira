# Requirements Document

## Introduction

This spec implements the **Worker Deployment** surface (P10 in the api-conformance
tracker umbrella). It moves the worker-deployment RPCs from `Deferred`/`Stubbed`
to `Implemented`, matching Temporal server v1.31.0.

IMPORTANT (verified against v1.31.0 `service/frontend/workflow_handler.go`): in
v1.31.0 the five pre-release `Deployment` companion RPCs — `DescribeDeployment`,
`ListDeployments`, `GetCurrentDeployment`, `SetCurrentDeployment`, and
`GetDeploymentReachability` — return hard `UNIMPLEMENTED`
("Deployments are deprecated and no longer supported, use Worker Deployments instead").
They are NOT projected over the v2 registry. To match the targeted release per
AGENTS.md §8, this spec therefore returns `UNIMPLEMENTED` for those five RPCs — this
is the one sanctioned `UNIMPLEMENTED` case, because the targeted release itself returns
it. The tracker lists `DescribeDeployment` inside the v2 set; that is a tracker
classification error (it is one of the deprecated companions) and is flagged for tracker
correction.

Temporal's worker-versioning surface is split between **v1** (deprecated, build-id
based: assignment rules, redirect rules, `WorkerVersionStamp`) and **v2** (deployment
based: Worker Deployments, Deployment Versions, routing config). This spec makes the
**v2 deployment-based** surface the primary `Implemented` capability. The v1
build-id RPCs remain Partial under existing handlers and are out of scope here.

This spec is also the **owner of worker deployment/versioning routing application**.
Three sibling specs persist and thread versioning fields but explicitly defer the
*application* of those fields to dispatch routing to this spec:

- `api-conformance-start-fields` defers `versioning_override` and
  `eager_worker_deployment_options` routing application.
- `api-conformance-wft-completion` defers `deployment_options` / `versioning_behavior`
  dispatch routing application (it only persists/threads them onto history).
- `api-conformance-workflow-describe` defers populating the
  `WorkflowExecutionInfo` versioning fields (`versioning_info`,
  `worker_deployment_name`, and the deprecated `assigned_build_id` /
  `inherited_build_id` / `most_recent_worker_version_stamp`).

This spec owns the durable Worker Deployment registry, the routing-config state
machine (current version, ramping version, ramp percentage), the version drainage /
reachability calculation, and the workflow-task dispatch routing that consumes that
state. It does not re-implement the field translation the sibling specs already own;
it consumes their persisted state and applies it.

This is a foundational, large feature: a new durable registry plus changes to
workflow-task routing across the runtime. The registry and all routing-config state
are durable and must survive process restart.

## Glossary

- **Worker Deployment:** A namespace-scoped record (`WorkerDeploymentInfo`) identified
  by `deployment_name`, representing all workers serving a shared set of task queues.
  Holds the routing config and the set of tracked Deployment Versions.
- **Deployment Version (Version):** A record (`WorkerDeploymentVersionInfo`) identified
  by the pair (`deployment_name`, `build_id`), i.e. `WorkerDeploymentVersion`,
  representing all workers of the same code/config within a Worker Deployment.
- **Routing Config:** The per-deployment record (`RoutingConfig`) describing the
  Current Version, the Ramping Version, the ramp percentage, and the monotonic
  `revision_number` used for eventual-consistency staleness detection.
- **Current Version:** The Deployment Version that receives new workflow executions and
  tasks of existing unversioned or AutoUpgrade workflows. A nil Current Version routes
  that traffic to unversioned workers.
- **Ramping Version:** A Deployment Version receiving a percentage of new traffic
  shifted away from the Current Version. A nil Ramping Version represents unversioned
  workers. Must differ from the Current Version unless both are nil.
- **Conflict token:** An opaque optimistic-concurrency token returned by read/write
  worker-deployment APIs. A write that supplies a non-nil token fails if the
  deployment state changed since the token was issued.
- **Drainage:** The lifecycle of a Version that is no longer Current or Ramping:
  `DRAINING` (still used by open pinned workflows) → `DRAINED` (no open pinned
  workflows remain). Captured in `VersionDrainageInfo`.
- **Reachability:** The deprecated coarse equivalent of drainage
  (`DeploymentReachability`: REACHABLE / CLOSED_WORKFLOWS_ONLY / UNREACHABLE). The
  standalone `GetDeploymentReachability` RPC is `UNIMPLEMENTED` in v1.31.0; the v2
  surface exposes drainage via `DescribeWorkerDeploymentVersion.drainage_info` instead.
- **Versioning Behavior:** Per-workflow setting (`VersioningBehavior`: PINNED /
  AUTO_UPGRADE / UNSPECIFIED) that determines how the server routes a workflow when its
  deployment's Current Version changes.
- **Versioning Override:** An execution-scoped override (`VersioningOverride`) that
  takes precedence over the SDK-sent behavior; PINNED overrides pin a workflow to a
  specific Version.
- **Manager Identity:** The client identity stored on a Worker Deployment
  (`WorkerDeploymentInfo.manager_identity`). In v1.31.0 it gates set-current,
  set-ramping, and delete-version mutations; `SetWorkerDeploymentManager` itself is
  gated by conflict-token and no-change checks, not by the existing manager identity.
- **Compute Config:** Per-Version worker scale-management configuration
  (`ComputeConfig`), a map of named scaling groups.
- **Unversioned worker:** A worker with `WORKER_VERSIONING_MODE_UNVERSIONED` (or
  unspecified), represented by the reserved string `__unversioned__` in APIs.
- **Legacy Version string:** Deprecated string form accepted by v1.31.0 as either
  `"<deployment>.<build_id>"` or `"<deployment>:<build_id>"`; `__unversioned__` and
  an empty string resolve to an unversioned (nil) Version.
- **Routing application:** The act of consuming persisted versioning state
  (Current/Ramping version, per-workflow behavior, overrides) to decide which
  Deployment Version a workflow task or activity task is dispatched to. Owned by this
  spec.

## Target State

`Implemented` for the v2 deployment-based RPCs and the routing application they drive;
`UNIMPLEMENTED` for the deprecated `Deployment` companions, matching v1.31.0.

- The 13 v2 worker-deployment RPCs are fully implemented: durable registry CRUD,
  routing-config state machine, drainage/reachability, compute config, metadata,
  manager identity, and the dispatch routing that consumes the registry. (These are
  the worker-deployment-prefixed RPCs: Create/Describe/Delete/ListWorkerDeployment(s),
  Create/Describe/DeleteWorkerDeploymentVersion, Set Current/Ramping Version,
  Update/ValidateWorkerDeploymentVersionComputeConfig,
  UpdateWorkerDeploymentVersionMetadata, SetWorkerDeploymentManager.)
- The 5 deprecated `Deployment` companion RPCs (`DescribeDeployment`, `ListDeployments`,
  `GetDeploymentReachability`, `GetCurrentDeployment`, `SetCurrentDeployment`) return
  `UNIMPLEMENTED` with the message "Deployments are deprecated and no longer supported,
  use Worker Deployments instead", exactly as Temporal server v1.31.0 does
  (`service/frontend/workflow_handler.go`). They are NOT projected over the v2 registry.
  This is the single sanctioned `UNIMPLEMENTED` case in this spec, justified because the
  targeted release returns `UNIMPLEMENTED` for these RPCs (AGENTS.md §8). `GetDeploymentReachability`
  being deprecated means the v2 reachability surface is exposed only via
  `DescribeWorkerDeploymentVersion`'s drainage info, not via a standalone reachability RPC.
- The deprecated **build-id-based v1** RPCs (`UpdateWorkerBuildIdCompatibility`,
  `GetWorkerBuildIdCompatibility`, assignment/redirect rule RPCs) are explicitly
  **out of scope**; they remain under their existing handlers per the tracker.

Behaviour is verified against Temporal server
[tag `v1.31.0`](https://github.com/temporalio/temporal/tree/v1.31.0)
(`service/worker/workerdeployment/` for the deployment lifecycle and
`service/history/workflow/` for routing) per AGENTS.md §8, and proto shape against the
vendored API `v1.62.11`.

## Evidence From Current Code

- **Proto shape (authoritative; vendored API v1.62.11):**
  - RPC requests/responses: `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`
    — the v2 worker-deployment messages span lines 2241–2632 (`DescribeWorkerDeploymentVersionRequest`
    through `SetWorkerDeploymentManagerResponse`); the deprecated companions are
    `DescribeDeployment` (2232), `ListDeployments` (2284), `SetCurrentDeployment`
    (2299), `GetCurrentDeployment` (2644), `GetDeploymentReachability` (2654).
  - RPC service registration: `proto/upstream/temporal/api/workflowservice/v1/service.proto`
    (worker-deployment RPC block beginning ~line 949).
  - Message types: `proto/upstream/temporal/api/deployment/v1/message.proto`
    (`WorkerDeployment`, `WorkerDeploymentVersion`, `WorkerDeploymentInfo`,
    `WorkerDeploymentInfo.WorkerDeploymentVersionSummary`, `WorkerDeploymentVersionInfo`,
    `VersionDrainageInfo`, `VersionMetadata`, `RoutingConfig`, `Deployment`,
    `DeploymentInfo`, `DeploymentListInfo`, `UpdateDeploymentMetadata`,
    `InheritedAutoUpgradeInfo`).
  - Compute config: `proto/upstream/temporal/api/compute/v1/config.proto`
    (`ComputeConfig`, `ComputeConfigScalingGroup`, `ComputeConfigScalingGroupUpdate`
    with `update_mask`, `ComputeConfigSummary`).
  - Workflow versioning info: `proto/upstream/temporal/api/workflow/v1/message.proto`
    (`WorkflowExecutionVersioningInfo`, `DeploymentVersionTransition`,
    `VersioningOverride`/`PinnedOverride`, and the `WorkflowExecutionInfo` fields
    `versioning_info` (22), `worker_deployment_name` (23), deprecated
    `assigned_build_id` (19), `inherited_build_id` (20),
    `most_recent_worker_version_stamp` (16)).
  - Enums: `proto/upstream/temporal/api/enums/v1/deployment.proto`
    (`DeploymentReachability`, `VersionDrainageStatus`, `WorkerVersioningMode`,
    `WorkerDeploymentVersionStatus`); `RoutingConfigUpdateState` in
    `proto/upstream/temporal/api/enums/v1/task_queue.proto`; `VersioningBehavior` and
    `ContinueAsNewVersioningBehavior` in `proto/upstream/temporal/api/enums/v1/workflow.proto`.
- **Behaviour (authoritative; Temporal server v1.31.0):** `github.com/temporalio/temporal`
  at tag `v1.31.0`, `service/worker/workerdeployment/` (deployment + version workflows,
  current/ramping selection, drainage), `service/frontend/workflow_handler.go`,
  `common/worker_versioning/worker_versioning.go`, and `service/history/api/` task-start
  handlers. These are the source of truth for defaulting, lifecycle ordering, error
  mapping, and reachability calculation.
- **Current handlers (`crates/tokeira-edge/src/grpc/workflow_service.rs`, verified):**
  - The 13 v2 worker-deployment RPCs are wired via the `deferred_unary!` macro pointing
    at `"worker-deployments"` in the block beginning at line 1323. NOTE: that same
    `deferred_unary!` block also contains `describe_worker` (1401) and `list_workers`
    (1407), which are worker-observability RPCs, NOT deployment-management RPCs and are
    NOT in scope for this spec — they belong to `worker-config-management`/observability.
    This spec owns only the 13 deployment RPCs in that block.
  - The 5 deprecated `Deployment` companions are explicit `Status::unimplemented`
    handlers at lines 1069–1109, with three distinct current messages — `describe_deployment`
    / `list_deployments`: "Deployment management is not yet supported. Worker versioning
    via assignment and redirect rules is available."; `get_deployment_reachability`:
    "...Use GetWorkerTaskReachability for build ID reachability."; `get_current_deployment`
    / `set_current_deployment`: "Deployment management is not yet supported." NONE of these
    match the v1.31.0 message. To conform to the targeted release (§8), this spec replaces
    all five with the v1.31.0 message "Deployments are deprecated and no longer supported,
    use Worker Deployments instead" (Requirement 11). `DescribeDeployment` is one of these
    explicit handlers (line 1069) — it is NOT in the deferred v2 block, confirming it is a
    deprecated companion, not part of the 13-RPC v2 set.
- **Compatibility matrix:** `crates/tokeira-compatibility/src/matrix.rs` — the
  `worker-deployments` `FeatureEntry` (id `"worker-deployments"`, line 514) is
  `FeatureState::Unsupported`; `WORKER_DEPLOYMENT_RPCS` (line 203) lists 18 RPC
  identifiers = the 13 v2 deployment RPCs + the 5 deprecated `Deployment` companions
  (it does NOT include `DescribeWorker`/`ListWorkers`, which belong to the `worker-config`
  feature). This spec moves that entry to its supported state with evidence; the 5
  deprecated companions are counted as conformant via their v1.31.0 `UNIMPLEMENTED`
  behavior.
- **Existing deferred-RPC test (verified):** `deferred_handler_blocks_return_tracked_unimplemented_messages`
  (line 2372, using the `assert_deferred_rpc!` macro at line 2353) asserts the deferred
  placeholder behavior for all 13 deployment RPCs — and currently also for
  `describe_worker`/`list_workers` under `"worker-deployments"`. Implementing this spec
  requires updating that test for the 13 RPCs and re-pointing the two worker RPCs to
  their correct owning spec.
- **Sibling specs that defer to this one:**
  - `api-conformance-start-fields/requirements.md` — `versioning_override` "persist
    routing override and apply to WFT dispatch" and `eager_worker_deployment_options`
    "Owned by `worker-deployments`".
  - `api-conformance-wft-completion/requirements.md` — `deployment_options` /
    `versioning_behavior` "routing application owned by `worker-deployments`".
  - `api-conformance-workflow-describe/requirements.md` — `versioning_info`,
    `worker_deployment_name`, deprecated build-id/version-stamp fields "Owned by
    `worker-deployments`".
- **Existing v1 worker-versioning surface (out of scope, retained):** assignment-rule
  and redirect-rule RPCs (`UpdateWorkerVersioningRules`, `GetWorkerVersioningRules`,
  `GetWorkerTaskReachability`, `UpdateWorkerBuildIdCompatibility`,
  `GetWorkerBuildIdCompatibility`) remain under their current handlers.
- **No existing durable registry:** there is currently no kernel/runtime/storage
  representation of Worker Deployments or Deployment Versions; this spec introduces it.

## Field Policy

Every proto field of every in-scope request and response message is accounted for
below. `Target policy` describes the implemented behavior; deprecated alias fields
are accepted on input for back-compat and populated on output only where the proto
marks them populated. All field numbers are taken from the vendored v1.62.11 proto.

### Deployment CRUD

#### `CreateWorkerDeploymentRequest` / `CreateWorkerDeploymentResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace; admit | `INVALID_ARGUMENT` if empty; `NOT_FOUND` if namespace absent | Registry scope |
| `deployment_name` (req 2) | Required key; create new deployment record | `INVALID_ARGUMENT` if empty/over length; `ALREADY_EXISTS` if a deployment with this name exists | New `WorkerDeploymentInfo` record |
| `identity` (req 4) | Persist as `last_modifier_identity` | none | Registry record |
| `request_id` (req 5) | Idempotency key: empty is accepted and defaulted to a generated UUID; a repeat with the same id is a successful no-op; no format validation (`service/frontend/workflow_handler.go:185 @ v1.31.0`) | none | Idempotency dedupe state |
| `conflict_token` (resp 1) | Return token for the created record | n/a | Read/write concurrency |

#### `DescribeWorkerDeploymentRequest` / `DescribeWorkerDeploymentResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if namespace absent | none (read) |
| `deployment_name` (req 2) | Look up deployment | `INVALID_ARGUMENT` if empty; `NOT_FOUND` if deployment absent | none (read) |
| `conflict_token` (resp 1) | Emit current optimistic-concurrency token | n/a | Read/write concurrency |
| `worker_deployment_info` (resp 2) | Project full `WorkerDeploymentInfo` (name, version_summaries, create_time, routing_config, last_modifier_identity, manager_identity, routing_config_update_state) | n/a | Derived from registry |

#### `DeleteWorkerDeploymentRequest` / `DeleteWorkerDeploymentResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if namespace absent | none |
| `deployment_name` (req 2) | Delete deployment only if it has no Versions; missing target is a success no-op (`service/worker/workerdeployment/client.go:1089 @ v1.31.0`) | `INVALID_ARGUMENT` if empty; `FAILED_PRECONDITION` if it still has Versions | Removes `WorkerDeploymentInfo` record when present |
| `identity` (req 3) | Record initiating identity for audit | none | none |
| (response is empty) | Return empty on success | n/a | n/a |

#### `ListWorkerDeploymentsRequest` / `ListWorkerDeploymentsResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | none (read) |
| `page_size` (req 2) | Bound page size; non-positive or over-max values are clamped to the server max (`service/frontend/workflow_handler.go:4078 @ v1.31.0`) | none | none |
| `next_page_token` (req 3) | Resume pagination from opaque token | `INVALID_ARGUMENT` if malformed/expired | none |
| `next_page_token` (resp 1) | Emit continuation token, empty when exhausted | n/a | none |
| `worker_deployments` (resp 2, `WorkerDeploymentSummary`) | One summary per deployment: `name`, `create_time`, `routing_config`, `latest_version_summary`, `current_version_summary`, `ramping_version_summary` | n/a | Derived from registry |

### Version CRUD

#### `CreateWorkerDeploymentVersionRequest` / `CreateWorkerDeploymentVersionResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | Registry scope |
| `deployment_version` (req 2, `WorkerDeploymentVersion`: `build_id`, `deployment_name`) | Required; create Version record only under an existing parent deployment (`service/worker/workerdeployment/client.go:1238` uses `util.go` `updateWorkflow`, not update-with-start, @ v1.31.0) | `INVALID_ARGUMENT` if missing/empty `build_id` or `deployment_name`; `ALREADY_EXISTS` if a Version with this name+build_id already exists; `RESOURCE_EXHAUSTED` if the deployment's max-versions limit is reached; `NOT_FOUND` if the named parent deployment lookup fails | New `WorkerDeploymentVersionInfo` (status `CREATED`) |
| `compute_config` (req 4, `ComputeConfig`) | Persist initial compute config (scaling groups) | `INVALID_ARGUMENT` for malformed scaling-group map | Version record |
| `identity` (req 3) | Persist as `last_modifier_identity` of the Version | none | Version record |
| `request_id` (req 5) | Idempotency: empty is accepted and defaulted to a generated UUID; same id for the same name+build_id → no-op success; no format validation (`workflow_handler.go:185 @ v1.31.0`) | none | Idempotency dedupe state |
| (response is empty) | Return empty on success | n/a | n/a |

#### `DescribeWorkerDeploymentVersionRequest` / `DescribeWorkerDeploymentVersionResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | none (read) |
| `version` (req 2, deprecated) | Accept legacy `"<deployment>.<build_id>"` or `"<deployment>:<build_id>"` when `deployment_version` is absent; `__unversioned__` and empty resolve to nil (`common/worker_versioning/worker_versioning.go:1103 @ v1.31.0`) | `INVALID_ARGUMENT` only when neither delimiter is present and the string is not empty/`__unversioned__` | none |
| `deployment_version` (req 3) | Preferred key; look up Version | `NOT_FOUND` if Version absent | none |
| `report_task_queue_stats` (req 4) | When true, populate per-task-queue stats in the response | none | none |
| `worker_deployment_version_info` (resp 1, `WorkerDeploymentVersionInfo`) | Project full version info (status, deployment_version, create_time, routing_changed_time, current_since_time, ramping_since_time, first_activation_time, last_current_time, last_deactivation_time, ramp_percentage, drainage_info, metadata, compute_config, last_modifier_identity); deprecated `version`/`deployment_name`/`task_queue_infos` populated for back-compat | n/a | Derived from registry |
| `version_task_queues` (resp 2, `VersionTaskQueue`: `name`, `type`, `stats`, `stats_by_priority_key`) | One entry per task queue ever polled by the Version; `stats`/`stats_by_priority_key` set only when `report_task_queue_stats` is true | n/a | Derived from poller tracking |

#### `DeleteWorkerDeploymentVersionRequest` / `DeleteWorkerDeploymentVersionResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | none |
| `version` (req 2, deprecated) | Accept legacy `"<deployment>.<build_id>"` or `"<deployment>:<build_id>"` when `deployment_version` absent; `__unversioned__` and empty resolve to nil | `INVALID_ARGUMENT` if both unset or malformed | none |
| `deployment_version` (req 5) | Preferred key; delete Version only if not Current/Ramping, no active pollers, and (unless `skip_drainage`) drained; missing target is a success no-op (`service/worker/workerdeployment/client.go:1037 @ v1.31.0`) | `FAILED_PRECONDITION` if Current/Ramping, has pollers, or still draining | Removes `WorkerDeploymentVersionInfo` when present |
| `skip_drainage` (req 3) | When true, bypass the not-draining precondition | none | Lifecycle gate |
| `identity` (req 4) | Record initiating identity; enforce against `manager_identity` when set | `FAILED_PRECONDITION` if manager mismatch (Temporal `ErrManagerIdentityMismatch`, `workflow.go:1109 @ v1.31.0`) | none |
| (response is empty) | Return empty on success | n/a | n/a |

### Current / Ramping version selection

#### `SetWorkerDeploymentCurrentVersionRequest` / `SetWorkerDeploymentCurrentVersionResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | Routing config |
| `deployment_name` (req 2) | Target deployment | `INVALID_ARGUMENT` if empty; `NOT_FOUND` if absent | Routing config |
| `version` (req 3, deprecated) | Accept legacy `"<deployment>.<build_id>"` or `"<deployment>:<build_id>"` when `build_id` unset; `__unversioned__` and empty resolve to nil | `INVALID_ARGUMENT` if both unset/conflicting or malformed | Routing config |
| `build_id` (req 7) | Build id of new Current Version; empty value sets Current to nil (unversioned) | `INVALID_ARGUMENT` for malformed; `NOT_FOUND` if the named Version does not exist | `RoutingConfig.current_deployment_version`, `current_version_changed_time`, `revision_number++` |
| `conflict_token` (req 4) | If non-nil, reject when deployment changed since token issued | `FAILED_PRECONDITION` on token mismatch (Temporal `errFailedPrecondition`) | Optimistic concurrency |
| `identity` (req 5) | Persist as `last_modifier_identity`; enforced against `manager_identity` when set | `FAILED_PRECONDITION` if manager mismatch (Temporal `ErrManagerIdentityMismatch`) | Registry record |
| `ignore_missing_task_queues` (req 6) | When false (default), reject if not all expected task queues are polled by the new Version; true bypasses | `FAILED_PRECONDITION` when expected pollers missing and flag false | Pre-write validation |
| `allow_no_pollers` (req 9) | When false (default), an unknown proposed Version is rejected; true bypasses by allowing auto-create | `NOT_FOUND` when the proposed Version is unknown and flag false (`workflow.go:1230/1244` + `client.go:384 @ v1.31.0`) | Pre-write validation |
| `conflict_token` (resp 1) | Emit post-write token | n/a | Optimistic concurrency |
| `previous_version` (resp 2, deprecated) | Populate legacy string of prior Current | n/a | Back-compat |
| `previous_deployment_version` (resp 3, deprecated) | Populate prior Current `WorkerDeploymentVersion` | n/a | Back-compat |

Side effect: setting Current to the Version that is currently Ramping automatically
unsets the Ramping Version (per v1.31.0 and the `SetWorkerDeploymentCurrentVersion`
RPC doc comment).

#### `SetWorkerDeploymentRampingVersionRequest` / `SetWorkerDeploymentRampingVersionResponse`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | Routing config |
| `deployment_name` (req 2) | Target deployment | `INVALID_ARGUMENT`/`NOT_FOUND` | Routing config |
| `version` (req 3, deprecated) | Accept legacy `"<deployment>.<build_id>"` or `"<deployment>:<build_id>"` when `build_id` unset; `__unversioned__` and empty resolve to nil | `INVALID_ARGUMENT` if both unset/conflicting or malformed | Routing config |
| `build_id` (req 8) | Build id to ramp to; empty sets Ramping to nil (unversioned) | `INVALID_ARGUMENT`/`NOT_FOUND`; `FAILED_PRECONDITION` if equal to Current Version while both non-nil | `RoutingConfig.ramping_deployment_version`, `ramping_version_changed_time`, `revision_number++` |
| `percentage` (req 4) | Ramp percentage; valid range [0,100] | `INVALID_ARGUMENT` if outside [0,100] | `RoutingConfig.ramping_version_percentage`, `ramping_version_percentage_changed_time` |
| `conflict_token` (req 5) | Optimistic-concurrency guard | `FAILED_PRECONDITION` on mismatch (Temporal `errFailedPrecondition`) | Concurrency |
| `identity` (req 6) | Persist; enforce against `manager_identity` | `FAILED_PRECONDITION` if manager mismatch (Temporal `ErrManagerIdentityMismatch`) | Registry record |
| `ignore_missing_task_queues` (req 7) | Same protection as current-version set; checked only when ramping version changes | `FAILED_PRECONDITION` when missing pollers and flag false | Pre-write validation |
| `allow_no_pollers` (req 10) | When false (default), an unknown proposed Version is rejected; true bypasses by allowing auto-create | `NOT_FOUND` when the proposed Version is unknown and flag false (`workflow.go:1230/1244` + `client.go:384 @ v1.31.0`) | Pre-write validation |
| `conflict_token` (resp 1) | Emit post-write token | n/a | Concurrency |
| `previous_version` (resp 2, deprecated) | Legacy string of prior Ramping | n/a | Back-compat |
| `previous_deployment_version` (resp 4, deprecated) | Prior Ramping `WorkerDeploymentVersion` | n/a | Back-compat |
| `previous_percentage` (resp 3, deprecated) | Prior ramp percentage | n/a | Back-compat |

### Compute config + validation

#### `UpdateWorkerDeploymentVersionComputeConfigRequest` / `...Response`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | Version record |
| `deployment_version` (req 2) | Required; target Version | `INVALID_ARGUMENT` if unset; `NOT_FOUND` if absent | Version record |
| `compute_config_scaling_groups` (req 6, map of `ComputeConfigScalingGroupUpdate`) | Add/update named scaling groups, honoring each update's `update_mask` semantics (empty mask on existing group = no-op; mask paths limited to the documented set) | `INVALID_ARGUMENT` for unknown mask paths or malformed group | `WorkerDeploymentVersionInfo.compute_config` |
| `remove_compute_config_scaling_groups` (req 7, repeated) | Remove named scaling groups | `INVALID_ARGUMENT` if a group is both updated and removed | `compute_config` |
| `identity` (req 3) | Persist as `last_modifier_identity` of the Version | none | Version record |
| `request_id` (req 4) | Idempotency: empty is accepted and defaulted to a generated UUID; repeat = no-op; no format validation | none | Idempotency dedupe |
| (response is empty) | Return empty on success | n/a | n/a |

#### `ValidateWorkerDeploymentVersionComputeConfigRequest` / `...Response`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | none (validation only) |
| `deployment_version` (req 2) | Version existence is not required; no lookup is performed (`workflow_handler.go:258`, `client.go:2037 @ v1.31.0`) | none for missing/unknown Version; no `NOT_FOUND` | none |
| `compute_config_scaling_groups` (req 6) | Validate the proposed update without applying it | `INVALID_ARGUMENT` for malformed group/mask | none |
| `remove_compute_config_scaling_groups` (req 7) | Validate removals without applying | `INVALID_ARGUMENT` for inconsistent set | none |
| `identity` (req 3) | Record initiating identity | none | none |
| (response is empty) | Return empty when the proposed config is valid | n/a | none |

### Metadata

#### `UpdateWorkerDeploymentVersionMetadataRequest` / `...Response`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | Version metadata |
| `version` (req 2, deprecated) | Accept legacy `"<deployment>.<build_id>"` or `"<deployment>:<build_id>"` when `deployment_version` absent; `__unversioned__` and empty resolve to nil | `INVALID_ARGUMENT` if both unset or malformed | Version metadata |
| `deployment_version` (req 5) | Preferred key; target Version | `NOT_FOUND` if absent | Version metadata |
| `upsert_entries` (req 3, map<string, Payload>) | Insert/replace metadata entries | `INVALID_ARGUMENT` for invalid keys/payloads | `VersionMetadata.entries` |
| `remove_entries` (req 4, repeated string) | Remove metadata keys | `INVALID_ARGUMENT` if a key is both upserted and removed | `VersionMetadata.entries` |
| `identity` (req 6) | Persist as `last_modifier_identity` | none | Version record |
| `metadata` (resp 1, `VersionMetadata`) | Return full metadata after the update | n/a | Derived |

### Manager identity

#### `SetWorkerDeploymentManagerRequest` / `...Response`

| Proto field | Target policy | Error if invalid | Persistence impact |
|---|---|---|---|
| `namespace` (req 1) | Resolve namespace | `NOT_FOUND` if absent | Deployment record |
| `deployment_name` (req 2) | Target deployment | `INVALID_ARGUMENT`/`NOT_FOUND` | Deployment record |
| `new_manager_identity` oneof — `manager_identity` (req 3) | Set `manager_identity` to the provided value; empty string unsets it | `INVALID_ARGUMENT` if oneof unset | `WorkerDeploymentInfo.manager_identity` |
| `new_manager_identity` oneof — `self` (req 4, bool) | When true, set `manager_identity` to the request `identity` | `INVALID_ARGUMENT` if `self=true` with empty `identity` | `manager_identity` |
| `conflict_token` (req 5) | Optimistic-concurrency guard | `FAILED_PRECONDITION` on mismatch (Temporal `errFailedPrecondition`) | Concurrency |
| `identity` (req 6, required) | Required acting identity; set-manager does not require it to equal the existing `manager_identity` (`workflow.go:1177 @ v1.31.0`) | `INVALID_ARGUMENT` if empty | Audit / self-manager source |
| `conflict_token` (resp 1) | Emit post-write token | n/a | Concurrency |
| `previous_manager_identity` (resp 2, deprecated) | Populate the prior manager identity | n/a | Back-compat |

### Deprecated `Deployment` companion RPCs (UNIMPLEMENTED in v1.31.0)

Verified against v1.31.0 `service/frontend/workflow_handler.go`: each of these five RPCs
returns `UNIMPLEMENTED` ("Deployments are deprecated and no longer supported, use Worker
Deployments instead"). Tokeira matches this exactly; it does NOT project them over the v2
registry. The request fields are therefore not translated.

| RPC | Target policy | Status |
|---|---|---|
| `DescribeDeployment` | Return `UNIMPLEMENTED` with the v1.31.0 message | gRPC `UNIMPLEMENTED` |
| `ListDeployments` | Return `UNIMPLEMENTED` with the v1.31.0 message | gRPC `UNIMPLEMENTED` |
| `GetDeploymentReachability` | Return `UNIMPLEMENTED` with the v1.31.0 message | gRPC `UNIMPLEMENTED` |
| `GetCurrentDeployment` | Return `UNIMPLEMENTED` with the v1.31.0 message | gRPC `UNIMPLEMENTED` |
| `SetCurrentDeployment` | Return `UNIMPLEMENTED` with the v1.31.0 message | gRPC `UNIMPLEMENTED` |

### Routing application (sibling-deferred; consumed here)

| Source field (owning spec) | Target policy in this spec | Persistence/dispatch impact |
|---|---|---|
| `RespondWorkflowTaskCompleted.deployment_options` / `versioning_behavior` (persisted by `api-conformance-wft-completion`) | Apply persisted behavior + worker deployment to subsequent workflow-task routing; update the run's effective `deployment_version` and `behavior` after the task completes on the transition target | Workflow-task dispatch routing; `WorkflowExecutionVersioningInfo` |
| `StartWorkflowExecution.versioning_override` (persisted by `api-conformance-start-fields`) | Apply the execution-scoped override (PINNED pins to a Version; AUTO_UPGRADE follows Current) to first-WFT and subsequent routing; precedence over SDK-sent behavior | Dispatch routing; `versioning_info.versioning_override` |
| `StartWorkflowExecution.eager_worker_deployment_options` (owned here) | When `request_eager_execution=true`, route the eager task per deployment options; otherwise no-op | Eager dispatch routing |
| `WorkflowExecutionInfo.versioning_info` (deferred by `api-conformance-workflow-describe`) | Populate from this spec's durable per-run versioning state (behavior, effective `deployment_version`, override, `version_transition`, `revision_number`) | Describe projection |
| `WorkflowExecutionInfo.worker_deployment_name` | Populate from the deployment that completed the most recent workflow task | Describe projection |
| `WorkflowExecutionInfo.assigned_build_id` / `inherited_build_id` / `most_recent_worker_version_stamp` (deprecated) | Leave default; superseded by v2 deployment-based fields per v1.31.0 | Describe projection (left default) |

## Requirements

### Requirement 1: Worker Deployment CRUD

**User Story:** As a deployment operator, I want to create, describe, list, and delete
Worker Deployments, so that I can manage the set of versioned worker fleets in a
namespace.

#### Acceptance Criteria

1. WHEN `CreateWorkerDeployment` is called with a non-empty `deployment_name` that does
   not yet exist in the namespace, THE Edge SHALL create a `WorkerDeploymentInfo`
   record and return its `conflict_token`.
2. IF `CreateWorkerDeployment` names a `deployment_name` that already exists, THEN THE
   Edge SHALL return `ALREADY_EXISTS`.
3. WHEN `CreateWorkerDeployment` is retried with a `request_id` previously seen for the
   same `deployment_name`, THE Edge SHALL treat the request as a successful no-op and
   return the existing record's `conflict_token`.
4. WHEN `DescribeWorkerDeployment` is called for an existing `deployment_name`, THE Edge
   SHALL return a `conflict_token` and a `worker_deployment_info` projecting `name`,
   `version_summaries`, `create_time`, `routing_config`, `last_modifier_identity`,
   `manager_identity`, and `routing_config_update_state` from durable state.
5. WHEN `ListWorkerDeployments` is called, THE Edge SHALL return one
   `WorkerDeploymentSummary` per deployment in the namespace, populating `name`,
   `create_time`, `routing_config`, `latest_version_summary`, `current_version_summary`,
   and `ramping_version_summary`, SHALL page results using `page_size` and an opaque
   `next_page_token`, and SHALL clamp non-positive or over-max `page_size` to the
   server max without error (`workflow_handler.go:4078 @ v1.31.0`).
6. WHEN `DeleteWorkerDeployment` is called for a deployment that has no Versions, THE
   Edge SHALL remove the deployment record and return an empty response.
7. IF `DeleteWorkerDeployment` targets a deployment that still has one or more Versions,
   THEN THE Edge SHALL return `FAILED_PRECONDITION` and SHALL NOT remove the record.
8. IF any deployment-CRUD RPC supplies an empty `deployment_name`, THEN THE Edge SHALL
   return `INVALID_ARGUMENT`.
9. IF a read deployment-CRUD RPC or a non-delete deployment mutation targets a
   `deployment_name` that does not exist, THEN THE Edge SHALL return `NOT_FOUND`.
10. WHEN `DeleteWorkerDeployment` targets a `deployment_name` that does not exist, THE
    Edge SHALL return an empty success response (no-op), matching v1.31.0
    (`client.go:1089 @ v1.31.0`).
11. IF any deployment-CRUD RPC names a namespace that does not exist, THEN THE Edge
    SHALL return `NOT_FOUND`.

### Requirement 2: Worker Deployment Version CRUD

**User Story:** As a deployment operator, I want to create, describe, and delete
Deployment Versions, so that I can track and retire individual worker builds within a
deployment.

#### Acceptance Criteria

1. WHEN `CreateWorkerDeploymentVersion` is called with a `deployment_version` whose
   `build_id` and `deployment_name` are non-empty AND the parent deployment exists, THE
   Edge SHALL create a `WorkerDeploymentVersionInfo` record with status
   `WORKER_DEPLOYMENT_VERSION_STATUS_CREATED`; IF the named parent deployment does not
   exist, THEN THE Edge SHALL return `NOT_FOUND` (`client.go:1238` + `util.go`
   `updateWorkflow @ v1.31.0`).
2. WHEN `CreateWorkerDeploymentVersion` includes a `compute_config`, THE Edge SHALL
   persist its scaling groups on the new Version.
3. WHEN `CreateWorkerDeploymentVersion` is called with an empty `request_id`, THE Edge
   SHALL accept it and use a server-generated id; WHEN retried with a `request_id`
   previously seen for the same `deployment_name` + `build_id`, THE Edge SHALL treat it
   as a successful no-op. The Edge SHALL NOT reject empty or malformed `request_id`
   values solely for id format (`workflow_handler.go:185 @ v1.31.0`).
4. IF `CreateWorkerDeploymentVersion` targets a `deployment_name` + `build_id` that
   already exists (and is not an idempotent `request_id` retry), THEN THE Edge SHALL
   return `ALREADY_EXISTS` (Temporal `ErrWorkerDeploymentVersionAlreadyExists`,
   `client.go:1296 @ v1.31.0`).
5. IF adding a Version would exceed the deployment's configured maximum number of
   Versions, THEN THE runtime SHALL first try to delete the oldest eligible Version,
   ordered by `create_time`; eligibility SHALL use the normal delete-version routing,
   active-poller, and drainage preconditions while bypassing manager-identity checks for
   this server-initiated maintenance. WHEN an eligible Version is deleted, THE runtime
   SHALL admit the new Version atomically in the same CAS mutation; IF no Version is
   eligible, THEN THE Edge SHALL return `RESOURCE_EXHAUSTED` with the configured limit in
   the message (`service/worker/workerdeployment/workflow.go:541-556, 1485-1504 @
   v1.31.0`).
6. IF `CreateWorkerDeploymentVersion` supplies a malformed `compute_config`, THEN THE
   Edge SHALL return `INVALID_ARGUMENT`.
7. WHEN `DescribeWorkerDeploymentVersion` is called for an existing Version, THE Edge
   SHALL populate `worker_deployment_version_info` with `status`, `deployment_version`,
   `create_time`, `routing_changed_time`, `current_since_time`, `ramping_since_time`,
   `first_activation_time`, `last_current_time`, `last_deactivation_time`,
   `ramp_percentage`, `drainage_info`, `metadata`, `compute_config`, and
   `last_modifier_identity` from durable state.
8. WHEN `DescribeWorkerDeploymentVersion` is called, THE Edge SHALL populate
   `version_task_queues` with one entry per task queue ever polled by the Version, and
   SHALL populate `stats` and `stats_by_priority_key` only when `report_task_queue_stats`
   is true.
9. WHERE `DescribeWorkerDeploymentVersion`, `DeleteWorkerDeploymentVersion`, or
   `UpdateWorkerDeploymentVersionMetadata` supplies the deprecated `version` string and
   omits `deployment_version`, THE Edge SHALL resolve the Version from either legacy
   `"<deployment_name>.<build_id>"` or `"<deployment_name>:<build_id>"`; THE Edge SHALL
   treat `__unversioned__` and empty string as unversioned (nil), and SHALL reject only
   strings with neither delimiter that are not `__unversioned__`/empty
   (`common/worker_versioning/worker_versioning.go:1103 @ v1.31.0`).
10. WHEN `DeleteWorkerDeploymentVersion` targets a Version that is neither Current nor
    Ramping, has no active pollers, and is drained, THE Edge SHALL remove the Version
    record and return an empty response.
11. IF `DeleteWorkerDeploymentVersion` targets a Version that is Current or Ramping, THEN
    THE Edge SHALL return `FAILED_PRECONDITION`.
12. IF `DeleteWorkerDeploymentVersion` targets a Version with active pollers, THEN THE
    Edge SHALL return `FAILED_PRECONDITION`.
13. IF `DeleteWorkerDeploymentVersion` targets a Version that is still draining AND
    `skip_drainage` is false, THEN THE Edge SHALL return `FAILED_PRECONDITION`; WHEN
    `skip_drainage` is true, THE Edge SHALL bypass the drainage precondition.
14. IF a Version-CRUD RPC supplies neither `deployment_version` nor a resolvable
    deprecated `version`, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
15. IF a read Version-CRUD RPC or a non-delete Version mutation targets a Version that
    does not exist (other than create), THEN THE Edge SHALL return `NOT_FOUND`; WHEN
    `DeleteWorkerDeploymentVersion` targets a Version that does not exist, THE Edge
    SHALL return an empty success response (no-op), matching v1.31.0
    (`client.go:1037 @ v1.31.0`).
16. WHEN a versioned worker first polls a task queue, THE runtime SHALL durably register
    its Deployment, Version, and task-queue family before admitting the poll; THE Edge
    SHALL propagate a registration rejection to the poller rather than treating registry
    failure as best-effort bookkeeping (`physical_task_queue_manager.go:768-786` and
    `service/worker/workerdeployment/client.go:320-355 @ v1.31.0`).
17. IF poll registration would add a new task-queue family after the configured
    `MatchingMaxTaskQueuesInDeploymentVersion` limit is reached, THEN THE Edge SHALL
    return `RESOURCE_EXHAUSTED` naming the task queue and configured limit and SHALL NOT
    mutate the Version. A second task-queue type under an already-registered family SHALL
    remain idempotently admissible because v1.31.0 counts distinct family names, not
    `(name,type)` pairs (`version_workflow.go:625-642 @ v1.31.0`).
18. WHEN poll registration would add a new Version at the configured maximum, THE runtime
    SHALL apply criterion 2.5's oldest-eligible deletion before admitting it; IF no
    Version is eligible, THEN THE Edge SHALL return `RESOURCE_EXHAUSTED` naming the
    requested Version and configured limit and SHALL leave durable state unchanged.
19. WHILE a versioned worker poll RPC is outstanding, THE runtime SHALL treat its exact
    Deployment-Version task-queue registration as live for delete and server-eviction
    preconditions. WHEN that poll is cancelled by the client, THE runtime SHALL remove
    that live registration before a later delete decision; WHEN it completes normally,
    THE runtime SHALL retain its most-recent observation for the configured poller-history
    window. This liveness lifecycle is distinct from `DescribeTaskQueue`'s diagnostic
    poller history, which MAY retain a cancelled worker until its bounded history expires
    (`matching_engine.go:1194-1206` and `task_queue_partition_manager.go:601, 617-621 @
    v1.31.0`).

### Requirement 3: Current Version Selection

**User Story:** As a deployment operator, I want to set or unset the Current Version of
a deployment, so that new and AutoUpgrade workflow traffic routes to the intended worker
build.

#### Acceptance Criteria

1. WHEN `SetWorkerDeploymentCurrentVersion` is called with a `build_id` naming an
   existing Version, THE runtime SHALL set that Version as the
   `RoutingConfig.current_deployment_version`, update `current_version_changed_time`,
   and increment `revision_number`.
2. WHEN `SetWorkerDeploymentCurrentVersion` is called with an empty `build_id`, THE
   runtime SHALL set the Current Version to nil, routing affected traffic to unversioned
   workers.
3. WHEN the Version being set as Current is the deployment's current Ramping Version, THE
   runtime SHALL automatically unset the Ramping Version as part of the same transition.
4. IF `SetWorkerDeploymentCurrentVersion` supplies a non-nil `conflict_token` that does
   not match the deployment's current token, THEN THE Edge SHALL return
   `FAILED_PRECONDITION` and SHALL NOT mutate routing state.
5. IF `ignore_missing_task_queues` is false AND not all expected task queues are polled
   by the proposed Current Version, THEN THE Edge SHALL return `FAILED_PRECONDITION`;
   WHEN `ignore_missing_task_queues` is true, THE Edge SHALL bypass this check.
6. IF `allow_no_pollers` is false AND the proposed Current Version is unknown, THEN THE
   Edge SHALL return `NOT_FOUND` (`workflow.go:1230/1244` + `client.go:384 @ v1.31.0`);
   WHEN `allow_no_pollers` is true, THE Edge SHALL bypass this check.
7. WHEN `SetWorkerDeploymentCurrentVersion` succeeds, THE Edge SHALL return the new
   `conflict_token` and populate the deprecated `previous_version` and
   `previous_deployment_version` with the prior Current Version.
8. IF the named `build_id` does not correspond to an existing Version, THEN THE Edge
   SHALL return `NOT_FOUND`.

### Requirement 4: Ramping Version Selection

**User Story:** As a deployment operator, I want to set a Ramping Version with a ramp
percentage, so that I can shift a controlled fraction of traffic to a new build before
promoting it.

#### Acceptance Criteria

1. WHEN `SetWorkerDeploymentRampingVersion` is called with a `build_id` and a
   `percentage` in [0,100], THE runtime SHALL set the
   `RoutingConfig.ramping_deployment_version` and `ramping_version_percentage`, update
   `ramping_version_changed_time` and `ramping_version_percentage_changed_time`, and
   increment `revision_number`.
2. WHEN `SetWorkerDeploymentRampingVersion` is called with an empty `build_id`, THE
   runtime SHALL set the Ramping Version to nil (unversioned workers).
3. IF `percentage` is outside the range [0,100], THEN THE Edge SHALL return
   `INVALID_ARGUMENT`.
4. IF the proposed Ramping Version equals the Current Version while both are non-nil,
   THEN THE Edge SHALL return `FAILED_PRECONDITION`.
5. IF `SetWorkerDeploymentRampingVersion` supplies a non-nil `conflict_token` that does
   not match the deployment's current token, THEN THE Edge SHALL return
   `FAILED_PRECONDITION` and SHALL NOT mutate routing state.
6. WHILE the ramping version is changing, IF `ignore_missing_task_queues` is false AND
   expected task queue pollers are missing, THEN THE Edge SHALL return
   `FAILED_PRECONDITION`; WHEN `ignore_missing_task_queues` is true, THE Edge SHALL
   bypass this check.
7. IF `allow_no_pollers` is false AND the proposed Ramping Version is unknown, THEN THE
   Edge SHALL return `NOT_FOUND` (`workflow.go:1230/1244` + `client.go:384 @ v1.31.0`);
   WHEN `allow_no_pollers` is true, THE Edge SHALL bypass this check.
8. WHEN `SetWorkerDeploymentRampingVersion` succeeds, THE Edge SHALL return the new
   `conflict_token` and populate the deprecated `previous_version`,
   `previous_deployment_version`, and `previous_percentage` with the prior Ramping
   Version state.

### Requirement 5: Compute Config Update and Validation

**User Story:** As a deployment operator, I want to update and validate the compute
config of a Version, so that worker scale management is configured without applying
invalid configuration.

#### Acceptance Criteria

1. WHEN `UpdateWorkerDeploymentVersionComputeConfig` supplies
   `compute_config_scaling_groups`, THE runtime SHALL add or update each named scaling
   group on the Version, applying each entry's `update_mask` so that an empty mask on an
   existing group is a no-op and a non-empty mask updates only the named paths.
2. WHEN `UpdateWorkerDeploymentVersionComputeConfig` supplies
   `remove_compute_config_scaling_groups`, THE runtime SHALL remove each named scaling
   group from the Version's compute config.
3. IF a scaling group name appears in both `compute_config_scaling_groups` and
   `remove_compute_config_scaling_groups`, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
4. IF an `update_mask` path is outside the documented accepted set ("task_queue_types",
   "provider", "provider.type", "provider.details", "provider.nexus_endpoint",
   "scaler", "scaler.type", "scaler.details"), THEN THE Edge SHALL return
   `INVALID_ARGUMENT`.
5. WHEN `UpdateWorkerDeploymentVersionComputeConfig` is retried with a previously seen
   `request_id` for the same Version, THE Edge SHALL treat it as a successful no-op.
6. WHEN `ValidateWorkerDeploymentVersionComputeConfig` is called, THE Edge SHALL validate
   the proposed scaling-group updates and removals and return an empty response without
   mutating any Version's compute config.
7. IF `ValidateWorkerDeploymentVersionComputeConfig` receives a malformed scaling group
   or an unaccepted mask path, THEN THE Edge SHALL return `INVALID_ARGUMENT`.
8. IF `UpdateWorkerDeploymentVersionComputeConfig` omits the required
   `deployment_version`, THEN THE Edge SHALL return `INVALID_ARGUMENT`; IF it names a
   Version that does not exist, THEN THE Edge SHALL return `NOT_FOUND`.
9. `ValidateWorkerDeploymentVersionComputeConfig` SHALL NOT require the named Version to
   exist and SHALL NOT return `NOT_FOUND` for a missing Version; it SHALL only return
   `INVALID_ARGUMENT` for malformed scaling groups, inconsistent update/remove sets, or
   unaccepted mask paths (`workflow_handler.go:258`, `client.go:2037 @ v1.31.0`).

### Requirement 6: Version Metadata

**User Story:** As a deployment operator, I want to attach and remove user-defined
metadata on a Version, so that I can record operational context such as pipeline links.

#### Acceptance Criteria

1. WHEN `UpdateWorkerDeploymentVersionMetadata` supplies `upsert_entries`, THE runtime
   SHALL insert or replace those entries in the Version's `VersionMetadata`.
2. WHEN `UpdateWorkerDeploymentVersionMetadata` supplies `remove_entries`, THE runtime
   SHALL remove those keys from the Version's `VersionMetadata`.
3. IF a key appears in both `upsert_entries` and `remove_entries`, THEN THE Edge SHALL
   return `INVALID_ARGUMENT`.
4. WHEN `UpdateWorkerDeploymentVersionMetadata` succeeds, THE Edge SHALL return the full
   `metadata` after applying the update.
5. WHEN `UpdateWorkerDeploymentVersionMetadata` includes an `identity`, THE runtime SHALL
   record it as the Version's `last_modifier_identity`.

### Requirement 7: Manager Identity

**User Story:** As a deployment operator, I want to claim or release exclusive write
rights to a deployment, so that concurrent operators do not make conflicting changes.

#### Acceptance Criteria

1. WHEN `SetWorkerDeploymentManager` supplies `new_manager_identity.manager_identity`
   with a non-empty value, THE runtime SHALL set the deployment's `manager_identity` to
   that value.
2. WHEN `SetWorkerDeploymentManager` supplies an empty `manager_identity` value, THE
   runtime SHALL unset the deployment's `manager_identity`.
3. WHEN `SetWorkerDeploymentManager` supplies `new_manager_identity.self = true`, THE
   runtime SHALL set the deployment's `manager_identity` to the request `identity`.
4. IF `SetWorkerDeploymentManager` is called with an empty required `identity`, THEN THE
   Edge SHALL return `INVALID_ARGUMENT`.
5. IF a `manager_identity` is already set AND the request `identity` does not match it,
   THEN THE Edge SHALL reject only set-current-version, set-ramping-version, and
   delete-version with `FAILED_PRECONDITION` (Temporal `ErrManagerIdentityMismatch`,
   `workflow.go:775/1244/1109 @ v1.31.0`). `SetWorkerDeploymentManager` itself is
   gated only by conflict-token and no-change checks, not by an existing-manager check
   (`workflow.go:1177 @ v1.31.0`).
6. IF `SetWorkerDeploymentManager` supplies a non-nil `conflict_token` that does not
   match, THEN THE Edge SHALL return `FAILED_PRECONDITION`.
7. WHEN `SetWorkerDeploymentManager` succeeds, THE Edge SHALL return the new
   `conflict_token` and populate the deprecated `previous_manager_identity`.
8. IF the oneof `new_manager_identity` is unset, THEN THE Edge SHALL return
   `INVALID_ARGUMENT`.

### Requirement 8: Drainage and Reachability

**User Story:** As a deployment operator, I want to know when a Version is safe to
decommission, so that I can retire old worker builds without breaking open workflows.

#### Acceptance Criteria

1. WHEN a Version stops being Current or Ramping AND open pinned workflows still target
   it, THE runtime SHALL set its `VersionDrainageInfo.status` to
   `VERSION_DRAINAGE_STATUS_DRAINING` and record `last_changed_time`.
2. WHEN a draining Version has no remaining open pinned workflows, THE runtime SHALL set
   its drainage status to `VERSION_DRAINAGE_STATUS_DRAINED` and record `last_changed_time`.
3. WHEN a Version becomes Current or Ramping again, THE runtime SHALL clear its drainage
   info.
4. WHEN drainage state is recomputed, THE runtime SHALL record `last_checked_time` on the
   Version's `VersionDrainageInfo`.
5. WHILE a Version is Current or Ramping, THE runtime SHALL NOT populate `drainage_info`
   for that Version.
6. WHEN `DescribeWorkerDeploymentVersion` is called, THE Edge SHALL surface the
   Version's drainage state (`VersionDrainageInfo`: status, last_changed_time,
   last_checked_time) in `worker_deployment_version_info.drainage_info`. The deprecated
   standalone `GetDeploymentReachability` RPC is `UNIMPLEMENTED` in v1.31.0 (see
   Requirement 11); reachability is therefore exposed only through the v2 Version's
   drainage info, not via a separate reachability RPC.
7. WHEN a Version first enters `DRAINING`, THE runtime SHALL defer its first reachability
   recomputation until the configured
   `VersionDrainageStatusVisibilityGracePeriod`; WHILE it remains `DRAINING`, THE runtime
   SHALL defer subsequent recomputations until the configured
   `VersionDrainageStatusRefreshInterval` (`version_workflow.go:1020-1052 @ v1.31.0`).
8. WHEN a public registry operation observes that a draining Version's recomputation is
   due, THE runtime MAY perform the due recomputation lazily instead of running
   Temporal's internal Version workflow, provided the response and all following
   operations observe the same durably CAS-committed drainage state. This is Tokeira's
   architecture-preserving equivalent; it SHALL NOT create a synthetic history or put
   drainage correctness in the edge.
9. WHEN a previously drained Version becomes Current or Ramping and is later demoted
   again, THE runtime SHALL clear the old drainage timestamps on reactivation and start a
   fresh grace-period cycle on the later demotion.

### Requirement 9: Workflow Versioning Routing Application

**User Story:** As an SDK user, I want workflows to route to the correct Deployment
Version based on versioning behavior and overrides, so that pinned workflows stay on
their version and AutoUpgrade workflows follow the Current Version.

#### Acceptance Criteria

1. WHEN a new workflow execution starts AND its deployment has a Current Version, THE
   runtime SHALL route the first workflow task per the effective versioning behavior:
   AUTO_UPGRADE (and unversioned) workflows follow the Current Version (subject to ramp),
   and PINNED workflows route to their pinned Version.
2. WHEN a `RespondWorkflowTaskCompleted` carrying `deployment_options` and
   `versioning_behavior` (persisted by `api-conformance-wft-completion`) completes the
   workflow task, THE runtime SHALL update the run's effective `deployment_version`,
   `behavior`, and `worker_deployment_name` in durable per-run versioning state.
3. WHEN a `StartWorkflowExecution` `versioning_override` (persisted by
   `api-conformance-start-fields`) is present, THE runtime SHALL apply it with precedence
   over the SDK-sent behavior: a PINNED override pins routing to the override's Version,
   and an AUTO_UPGRADE override follows the Current Version.
4. WHILE a deployment has a non-nil Ramping Version with a non-zero `percentage`, THE
   runtime SHALL route that percentage of eligible new traffic to the Ramping Version and
   the remainder to the Current Version.
5. WHEN a workflow task is started by a poller whose Deployment Version differs from the
   workflow's effective Deployment Version AND the workflow is AUTO_UPGRADE (unpinned),
   THE runtime SHALL begin a deployment version transition (`DeploymentVersionTransition`)
   toward the poller's Version, gated on the task's dispatch revision number, and
   complete the transition when a workflow task completes on the target Version. WHEN an
   activity task start would trigger such a transition, THE runtime SHALL reject that
   activity start, drop the task for later reschedule, and complete the transition on WFT
   completion; if a transition is already in flight, the activity start is also rejected.
   Pinned-workflow independent activities do not trigger the transition
   (`service/history/api/recordactivitytaskstarted/api.go:188 @ v1.31.0`).
6. WHEN a workflow task is started by a poller whose Deployment Version differs from the
   run's effective Deployment Version (and the run is unpinned), THE runtime SHALL **set**
   the run's `WorkflowExecutionVersioningInfo.revision_number` to that task's dispatch
   revision number as part of starting the transition. The run's `revision_number` is set
   only at transition-start and on start-time auto-upgrade inheritance; it is never
   incremented at workflow-task completion. (In v1.31.0 the run revision is assigned only
   in `StartDeploymentTransition` (`mutable_state_impl.go:9108 @ v1.31.0`, from
   `req.TaskDispatchRevisionNumber`) and on inheritance (`mutable_state_impl.go:2963`);
   `afterAddWorkflowTaskCompletedEvent` (`workflow_task_state_machine.go:1283-1396 @
   v1.31.0`) does not touch it.) This is distinct from the registry-level
   `RoutingConfig.revision_number`, which is incremented on every set-current /
   set-ramping mutation (Requirements 3.1, 4.1).
7. WHERE `StartWorkflowExecution.eager_worker_deployment_options` is present AND
   `request_eager_execution` is true, THE runtime SHALL route the eager workflow task per
   those deployment options; otherwise THE field SHALL have no routing effect.
8. WHERE a deployment has no Current Version, THE runtime SHALL route AUTO_UPGRADE and
   unversioned traffic (after any ramp) to unversioned workers.

### Requirement 10: Describe Versioning Field Projection

**User Story:** As an operator using `DescribeWorkflowExecution`, I want the versioning
fields populated from durable state, so that I can see how a workflow is versioned.

#### Acceptance Criteria

1. WHEN a workflow execution has durable versioning state, THE Edge SHALL populate
   `WorkflowExecutionInfo.versioning_info` (`behavior`, `deployment_version`,
   `versioning_override`, `version_transition`, `revision_number`,
   `continue_as_new_initial_versioning_behavior`) from that state.
2. WHEN the most recent workflow task was completed by a versioned worker, THE Edge SHALL
   populate `WorkflowExecutionInfo.worker_deployment_name` from the completing
   deployment.
3. THE Edge SHALL leave the deprecated `assigned_build_id`, `inherited_build_id`, and
   `most_recent_worker_version_stamp` fields default, as they are superseded by the v2
   deployment-based fields in Temporal v1.31.0.
4. WHERE a workflow execution has no versioning state, THE Edge SHALL leave
   `versioning_info` and `worker_deployment_name` default and SHALL NOT fabricate
   placeholder values.
5. THE versioning-field projection SHALL be derived from the same run snapshot used by
   the rest of the `DescribeWorkflowExecution` response, consistent with the
   `api-conformance-workflow-describe` single-snapshot contract.

### Requirement 11: Deprecated `Deployment` Companion RPCs Return UNIMPLEMENTED

**User Story:** As a Tokeira maintainer, I want the deprecated pre-release deployment
RPCs to behave exactly as Temporal v1.31.0 does, so that Tokeira's surface matches the
targeted release rather than inventing back-compat behavior the release does not provide.

#### Acceptance Criteria

1. WHEN `DescribeDeployment` is called, THE Edge SHALL return gRPC `UNIMPLEMENTED` with
   the message "Deployments are deprecated and no longer supported, use Worker
   Deployments instead".
2. WHEN `ListDeployments` is called, THE Edge SHALL return gRPC `UNIMPLEMENTED` with the
   same message.
3. WHEN `GetCurrentDeployment` is called, THE Edge SHALL return gRPC `UNIMPLEMENTED` with
   the same message.
4. WHEN `SetCurrentDeployment` is called, THE Edge SHALL return gRPC `UNIMPLEMENTED` with
   the same message.
5. WHEN `GetDeploymentReachability` is called, THE Edge SHALL return gRPC `UNIMPLEMENTED`
   with the same message.
6. THE five deprecated `Deployment` companion RPCs SHALL NOT read or mutate the v2
   registry; they return `UNIMPLEMENTED` before any state access, matching v1.31.0
   `service/frontend/workflow_handler.go`.
7. THE v2 deployment surface (Requirements 1–10) SHALL be the only supported way to
   manage worker deployments; clients are expected to migrate off the deprecated
   `Deployment` RPCs, as Temporal v1.31.0 requires.

### Requirement 12: Identity Propagation and Validation

**User Story:** As an auditor, I want client identity recorded and inputs validated, so
that changes are attributable and malformed requests are rejected before any mutation.

#### Acceptance Criteria

1. WHEN any write RPC carries an `identity`, THE runtime SHALL record it as the
   `last_modifier_identity` of the affected deployment or Version (or `manager_identity`
   for `SetWorkerDeploymentManager.self`).
2. IF any worker-deployment RPC names a namespace that does not exist, THEN THE Edge
   SHALL return `NOT_FOUND` before any mutation.
3. IF a request requires parsing a non-empty identifier (`deployment_name`, `build_id`,
   deprecated `version` string, `series_name`) and the value is malformed, THEN THE Edge
   SHALL return `INVALID_ARGUMENT` before any mutation.
4. THE Edge SHALL validate all inputs and reject invalid requests before mutating any
   durable registry or routing state, so that a rejected request leaves state unchanged.
5. THE Edge SHALL NOT return `UNIMPLEMENTED` for any of the 13 v2 worker-deployment
   RPCs. The 5 deprecated `Deployment` companion RPCs DO return `UNIMPLEMENTED` per
   Requirement 11, matching v1.31.0 — this is the only sanctioned `UNIMPLEMENTED` case.

### Requirement 13: Durable State and Restart Recovery

**User Story:** As an operator, I want the deployment registry and routing config to
survive process restart, so that versioning decisions remain consistent across
restarts.

#### Acceptance Criteria

1. WHEN a Worker Deployment or Deployment Version is created or mutated, THE runtime
   SHALL persist the change as durable state (registry record and/or per-run transition)
   before acknowledging the write.
2. WHEN the process restarts, THE runtime SHALL reload all Worker Deployments, Deployment
   Versions, routing configs (Current/Ramping version, ramp percentage,
   `revision_number`), version metadata, compute configs, manager identities, and
   drainage state from durable storage.
3. WHEN a describe/list RPC is served after restart, THE Edge SHALL return the same
   registry state that existed before the restart.
4. WHEN a conflict token issued before a restart is presented after the restart, THE
   Edge SHALL evaluate it against the reloaded state with the same semantics as before
   the restart.
5. WHEN per-run versioning state (behavior, effective `deployment_version`, override,
   `version_transition`, `revision_number`) is reloaded after restart, THE runtime SHALL
   apply routing decisions consistent with the pre-restart state.
6. THE runtime SHALL NOT store deployment registry or routing-config correctness state
   solely in transient queues or projection-only state.

## Iteration and Feedback Notes

- 13 v2 worker-deployment RPCs are implemented; the 5 deprecated `Deployment` companion
  RPCs return `UNIMPLEMENTED` to match v1.31.0 (verified against
  `service/frontend/workflow_handler.go`). All 18 are moved off the `deferred_unary!`
  placeholder (13 to real handlers, 5 to the v1.31.0 `UNIMPLEMENTED` response). The
  deprecated build-id v1 RPCs remain out of scope per the tracker.
- Tracker correction needed: the tracker lists `DescribeDeployment` inside the 14-RPC v2
  set, but in v1.31.0 it is a deprecated companion that returns `UNIMPLEMENTED`. The v2
  set is therefore 13 RPCs, and `DescribeDeployment` belongs with the deprecated
  companions. Flag this to the tracker owner.
- Routing application (Requirement 9) is the boundary this spec owns on behalf of
  `api-conformance-start-fields`, `api-conformance-wft-completion`, and
  `api-conformance-workflow-describe`; those specs persist/thread the fields and this
  spec applies them.
- Verification status (per AGENTS.md §8, against v1.31.0): CONFIRMED — set-current
  unsets ramping (`workflow.go`); manager mismatch → `FAILED_PRECONDITION`
  (`ErrManagerIdentityMismatch`); conflict-token mismatch → `FAILED_PRECONDITION`
  (`errFailedPrecondition`); CreateVersion already-exists → `ALREADY_EXISTS`,
  too-many-versions → `RESOURCE_EXHAUSTED`; drainage DRAINING→DRAINED lifecycle
  (`version_workflow.go`); override precedence (`ExtractVersioningBehaviorFromOverride`);
  AUTO_UPGRADE transition triggered at task-start by a differing poller
  (`recordworkflowtaskstarted`/`recordactivitytaskstarted` → `StartDeploymentTransition`);
  the 5 deprecated companions are `UNIMPLEMENTED`. Also CONFIRMED during design (against
  v1.31.0, now captured in `design.md`): poller-presence validation semantics —
  `allow_no_pollers` false → unknown build_id rejected with `NOT_FOUND`, true →
  auto-create (`client.go:384`); `ignore_missing_task_queues` false → new versioned
  current/ramping version must poll every task queue the comparison version polled,
  else `FAILED_PRECONDITION` (ramping checks against the current version, only when the
  ramping version changes); version task-queue stats sourced from the durable
  `polled_task_queues` set on each Version (`report_task_queue_stats` gates
  `stats`/`stats_by_priority_key`); and effective-deployment recomputation via the pure
  `effective_deployment()` / `effective_behavior()` precedence functions
  (transition > override > behavior+deployment_version), the analog of
  `GetEffectiveDeployment` / `GetEffectiveVersioningBehavior`. No open design-phase
  confirmations remain.
