# Requirements Document: Worker Compute Controller

## Introduction

This spec defines Tokeira's engine-side Worker Compute Controller: the control-plane
component that turns a Worker Deployment Version's durable `ComputeConfig` into
idempotent requests for worker capacity. It consumes Tokeira's existing
version-aware delivery signals and invokes a remote compute provider through a
registered Nexus endpoint. The worker continues to receive ordinary Temporal
workflow, activity, and Nexus tasks by polling the public WorkflowService API; Nexus
is used only for the capacity request.

The behavioral reference is the experimental Worker Controller Instance (WCI) module
that Temporal server v1.31.0 pins directly in `go.mod`:
`go.temporal.io/auto-scaled-workers` at commit `edd947d743d2`. This feature is still
outside Tokeira's Temporal v1.31.0 compatibility claim: Temporal labels it
pre-release, gates its task hook behind `workercontroller.enabled`, and defaults that
gate to `false` (`wci/client/config.go @ auto-scaled-workers edd947d743d2`). The
reference therefore establishes compute-controller behavior, not an obligation to
copy Temporal's system-Workflow implementation.

Tokeira deliberately uses its own architecture:

- `worker-deployments` remains the durable owner of ComputeConfig and Deployment
  Version identity.
- the runtime broker emits derived demand observations after task publication;
- a dedicated DSQL control-plane store holds controller status and a durable action
  outbox;
- provider calls happen from the effectful runtime/control plane through the existing
  Nexus transport;
- the kernel, workflow history, lane routing, and projection correctness path do not
  participate.

The first implementation supports remote Nexus compute providers using the pinned
`no-sync` scaler and its `invoke-worker` action. Direct AWS/GCP/Kubernetes/subprocess
providers, `rate-based`, worker-set sizing, scale-down policy, and provider-specific
placement are separate features. A sibling worker-compute provider is the first intended remote provider, but
the contract in this spec is provider-neutral. Queue-scoped credentials for the
untrusted workers that a provider starts are owned by the sibling
`scoped-worker-authorization` spec and tracked by `tokeira/tokeira#29`; this spec
never issues or broadens worker credentials.

## Glossary

- **Worker_Compute_Controller:** Tokeira's control-plane component that evaluates
  Deployment-Version demand and creates Provider_Actions.
- **Controller_Instance:** Durable controller state for one
  `(namespace_id, deployment_name, build_id)` tuple.
- **Scaling_Group:** A named `ComputeConfigScalingGroup` within one Deployment Version.
- **Effective_Task_Types:** The explicit task queue types assigned to a Scaling_Group,
  or the task types not claimed by another group when the group is the catch-all.
- **Remote_Nexus_Provider:** A `ComputeProvider` with a non-empty `nexus_endpoint`;
  its `type` and opaque `details` are interpreted by the remote service.
- **Eligible_Scaling_Group:** A Scaling_Group whose provider is a
  Remote_Nexus_Provider and whose scaler type is `no-sync`.
- **Demand_Observation:** A non-authoritative runtime observation that a versioned
  task was published with or without a compatible waiting poller.
- **No_Sync_Observation:** A Demand_Observation made when no compatible poller was
  waiting at publication.
- **Metrics_Snapshot:** A periodic aggregate of backlog count and dispatch rate by
  task type for the task queues observed for one Deployment Version.
- **Provider_Action:** A durable decision to ask a remote provider to invoke worker
  capacity.
- **Action_Outbox:** The durable queue of Provider_Actions awaiting or undergoing
  Nexus delivery.
- **Configuration_Fingerprint:** A deterministic digest of the exact Scaling_Group
  configuration used to decide a Provider_Action.
- **Action_Request_ID:** The stable idempotency key assigned to one Provider_Action
  and reused for every delivery retry.
- **Activation_Action:** A one-worker Provider_Action emitted when an eligible
  Scaling_Group first becomes active so that its worker can begin polling and register
  its task queues.
- **Controller_Health:** Operator-visible status describing whether a Scaling_Group
  is active, unsupported, misconfigured, capacity-limited, or failing delivery.
- **WCI_Pin:** `go.temporal.io/auto-scaled-workers` commit `edd947d743d2`, the exact
  module version composed by Temporal server v1.31.0.
- **Tokeira_Config:** The strict typed startup configuration owned by
  `tokeira-config`.
- **Feature_Catalog:** The generated operator-facing inventory of feature state,
  enablement, defaults, and evidence.
- **Worker_Deployment_Registry:** The durable runtime registry that owns Worker
  Deployments, Deployment Versions, routing configuration, and ComputeConfig.
- **Workflow_Broker:** The runtime broker that delivers workflow tasks.
- **Activity_Broker:** The runtime broker that delivers activity tasks.
- **Nexus_Task_Broker:** The runtime broker that delivers Nexus tasks and correlates
  their responses.
- **Controller_State_Store:** The DSQL repository for controller revisions, scaler
  state, health, and the Action_Outbox.
- **Tokeira_Proto:** Tokeira's separate extension-proto tree under `proto/tokeira/`;
  upstream Temporal protos are never modified for Tokeira-owned contracts.

## Target State

With empty Tokeira configuration, the Worker Compute Controller is disabled and
ComputeConfig remains a durable, readable, inert Worker Deployment field. An operator
opts in cluster-wide with:

```toml
[policy.worker_compute]
enabled = true
```

When enabled, each Deployment Version with an Eligible_Scaling_Group has durable,
restart-safe controller state. Versioned task publication produces non-blocking demand
observations. The controller applies the WCI_Pin's `no-sync` defaults and trigger rules,
persists each invocation decision before I/O, and sends a provider-neutral
`tokeira.compute.v1.InvokeWorkerRequest` to the configured Nexus endpoint using the
fixed service `tokeira.worker.compute.v1.ComputeProvider` and operation
`invoke-worker`.

The remote provider acknowledges synchronously and deduplicates by
`Action_Request_ID`. It may then launch Lambda, containers, microVMs, or another worker
substrate. A launched worker uses ordinary Temporal SDK polling with the exact
namespace, task queue, Deployment name, and Build ID. Provider failure can delay new
capacity but can never reject, roll back, or block workflow task publication.

The implementation introduces no kernel command, kernel state, workflow event, system
Workflow, lane-affinity rule, or provider-specific infrastructure. The
`scoped-worker-authorization` feature is a separate prerequisite for running
untrusted workers safely; its absence does not change the controller's provider
contract.

## Evidence From Current Code

- **Public compute contract (authoritative shape):**
  `proto/upstream/temporal/api/compute/v1/{config.proto,provider.proto,scaler.proto}`
  at `TEMPORAL_PROTO_VERSION = v1.62.11`. `ComputeProvider.type` is explicitly
  implementation-specific, `details` is opaque, and field 10 names an optional Nexus
  endpoint.
- **Worker poll identity (authoritative shape):**
  `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` and
  `proto/upstream/temporal/api/deployment/v1/message.proto` at API v1.62.11. Workflow,
  activity, and Nexus poll requests carry `WorkerDeploymentOptions`.
- **Pinned WCI behavior:** `wci/client/{config.go,hook.go}`,
  `wci/workflow/{activities.go,workflow.go}`,
  `wci/workflow/iface/{spec.go,spec_update.go}`, and
  `wci/workflow/scaling_algorithm/{registry.go,no_sync_match.go}` at
  `auto-scaled-workers edd947d743d2`. These sources define the disabled default,
  per-version controller key, sticky/unversioned exclusions, signal batching,
  activation invocation, group resolution, metrics aggregation, action vocabulary,
  `no-sync` defaults, validation, and decision rules.
- **Temporal integration evidence:** `go.mod` and `service/worker/fx.go @ v1.31.0`
  pin and compose the WCI module; `service/worker/workerdeployment/compute_util.go` and
  `client.go @ v1.31.0` translate Worker Deployment ComputeConfig into that module.
- **Current durable config:** `crates/tokeira-runtime/src/deployment_registry.rs`
  persists ComputeConfig, applies its update masks, and currently restricts provider
  type to the WCI_Pin's built-in list even though the vendored proto permits
  implementation-specific remote types.
- **Current demand seams:** `crates/tokeira-runtime/src/broker.rs` already determines
  compatible waiter presence and records sync/non-sync metrics after deduplication for
  workflow and activity publication. `QueueKey` already carries Deployment and Build
  ID.
- **Current Nexus gap:** `PollNexusTaskQueueRequest.deployment_options` exists in the
  vendored proto, but `crates/tokeira-edge/src/translate/nexus.rs` currently drops it
  and `NexusTaskBroker` keys readiness only by namespace and task queue.
- **Current provider transport:** `crates/tokeira-runtime/src/nexus.rs` owns the
  endpoint registry, Worker-target broker, and effectful external HTTP client. The
  `runtime-nexus-http-client`, `edge-nexus-task-transport`, and
  `edge-nexus-http-dispatch` specs establish the existing transport semantics.
- **Configuration authority:** `crates/tokeira-config/src/lib.rs` owns strict
  startup-static policy and `crates/tokeira-compatibility/src/matrix.rs` owns the
  generated Feature Catalog rendered into
  `docs/conformance/v1.31.0/tokeira-configuration.md`.
- **Downstream consumer:** the sibling worker-compute provider's architecture and
  engine-contract documents require an
  engine-pushed, exact-version, idempotent scaling request while task delivery remains
  ordinary Temporal polling.
- **Authorization dependency:** `tokeira/tokeira#29` records the need for
  fail-closed namespace/task-queue/Deployment-Version worker credentials. It is
  intentionally not owned by this spec.

## Contract Policy

### Production Configuration

| Field | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `policy.worker_compute` | Optional strict typed table; omission selects defaults | Unknown table members fail startup configuration parsing | Startup-static cluster policy only |
| `policy.worker_compute.enabled` | Boolean, default `false` | Non-boolean value fails startup configuration parsing | Enables controller reconciliation after startup; never changes stored ComputeConfig |

No batching interval, controller-count limit, retry interval, or scaler-tuning field is
added to production TOML. Batching and controller-count values are fixed to the
WCI_Pin defaults; scaler tuning remains in the public `ComputeScaler.details` payload.

### Existing `ComputeConfigScalingGroup`

| Field | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| map key / Scaling Group ID | Non-empty and unique within the map | `INVALID_ARGUMENT` | Durable group identity and controller-state key |
| `task_queue_types` (1) | Zero or more unique Workflow, Activity, or Nexus values; empty means catch-all for unclaimed types | `INVALID_ARGUMENT` for UNSPECIFIED, duplicates, or a second catch-all | Determines Effective_Task_Types |
| `provider` (3) | Required for an active group | Existing non-Nexus provider rules remain unchanged | Durable provider configuration |
| `scaler` (4) | Optional for existing direct providers; required for a Remote_Nexus_Provider | `INVALID_ARGUMENT` when a Remote_Nexus_Provider omits it | Durable scaler configuration |

### Existing `ComputeProvider`

| Field | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `type` (1) | Non-empty; a Remote_Nexus_Provider may use any implementation-specific string | `INVALID_ARGUMENT` when empty; existing built-in-only validation remains for providers without `nexus_endpoint` | Echoed to provider and included in Configuration_Fingerprint |
| `details` (2) | Opaque `Payload`; stored and forwarded byte-for-byte | Existing payload-size validation | Forwarded only to the configured provider |
| `nexus_endpoint` (10) | Empty means existing direct/inert provider behavior; non-empty names a registered Nexus endpoint | No mutation-time existence check; an unresolved endpoint sets Controller_Health to misconfigured | Selects the effectful provider transport |

The endpoint is resolved when an action is delivered rather than during ComputeConfig
mutation. This preserves the existing ability to create resources in either order and
handles endpoint deletion without making Worker Deployment state dependent on a
second registry transaction.

### Existing `ComputeScaler`

| Field | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `type` (1) | `no-sync` is active in this feature; `rate-based` remains accepted and round-tripped but controller-ineligible | Existing unknown type returns `INVALID_ARGUMENT` | Selects controller implementation |
| `details` (2) | For `no-sync`, decode with the default Temporal payload converter into an object containing only the keys below | Malformed payload, wrong value type, or unknown key returns `INVALID_ARGUMENT` | Supplies durable scaler policy |

### `no-sync` Scaler Details

| Key | Type / default | Target policy | Error if invalid |
|---|---|---|---|
| `scale_up_cooloff_ms` | JSON number or base-10 integer string / `100` | Decode with the WCI_Pin's int64 helper (JSON numbers truncate toward zero); minimum elapsed milliseconds between invoke decisions; `0` disables cooloff | `INVALID_ARGUMENT` when below zero or of another type |
| `scale_up_backlog_threshold` | JSON number or base-10 integer string / `0` | Decode with the WCI_Pin's int64 helper; metrics path invokes only when backlog is strictly greater | `INVALID_ARGUMENT` when below zero or of another type |
| `max_worker_lifetime_ms` | JSON number or base-10 integer string / `600000` | Decode with the WCI_Pin's int64 helper; backlog-present refresh interval; `0` disables refresh | `INVALID_ARGUMENT` when below zero or of another type |
| `scale_up_dispatch_rate_epsilon` | JSON number or numeric string / `0` | Decode with the WCI_Pin's float64 helper; suppresses a metrics-driven invoke when current and prior dispatch rates differ by no more than epsilon; `0` disables suppression | `INVALID_ARGUMENT` when negative or of another type |
| `metrics_poll_interval_ms` | JSON number or base-10 integer string / `60000` | Decode with the WCI_Pin's int64 helper; periodic Metrics_Snapshot interval | `INVALID_ARGUMENT` when below `10000` or of another type |
| any other key | not applicable | Rejected to expose misspellings and unsupported policy | `INVALID_ARGUMENT` naming the key |

When `scale_up_cooloff_ms` is positive,
`metrics_poll_interval_ms < scale_up_cooloff_ms` is invalid because the WCI_Pin rejects
that cross-field combination.

### Tokeira-Owned `InvokeWorkerRequest`

The request is defined in `proto/tokeira/compute/v1/provider.proto` and encoded as one
protobuf Temporal `Payload`.

| Field (id) | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `request_id` (1) | Required opaque Action_Request_ID | Provider returns Nexus handler `BAD_REQUEST` when empty | Outbox primary idempotency key |
| `namespace` (2) | Required public namespace name | Provider returns Nexus handler `BAD_REQUEST` when empty | Worker connection scope |
| `deployment_name` (3) | Required exact Worker Deployment name | Provider returns Nexus handler `BAD_REQUEST` when empty | Worker version scope |
| `build_id` (4) | Required exact Build ID | Provider returns Nexus handler `BAD_REQUEST` when empty | Worker version scope |
| `scaling_group` (5) | Required Scaling Group ID | Provider returns Nexus handler `BAD_REQUEST` when empty | Correlates provider action and controller status |
| `count` (6) | Required positive count; this feature emits exactly `1` | Provider returns Nexus handler `BAD_REQUEST` when zero | Number of workers requested |
| `task_queues` (7) | Deterministically sorted, duplicate-free observed queue bindings; may be empty for Activation_Action | Provider returns Nexus handler `BAD_REQUEST` for empty names or UNSPECIFIED types | Advisory worker-poll configuration; never task payload |
| `provider_type` (8) | Exact `ComputeProvider.type` from the decided config | Provider returns Nexus handler `BAD_REQUEST` when empty | Selects provider-specific interpretation |
| `provider_details` (9) | Exact opaque `ComputeProvider.details` from the decided config | Provider-specific validation may reject with Nexus handler `BAD_REQUEST` | Provider-specific launch configuration |
| `configuration_fingerprint` (10) | Required deterministic digest of group ID, Effective_Task_Types, provider, and scaler | Provider returns Nexus handler `BAD_REQUEST` when empty | Fences stale configuration and aids deduplication |
| `reason` (11) | Required enum: CONFIGURATION_ACTIVATION, NO_SYNC_MATCH, BACKLOG, or WORKER_REFRESH | Provider returns Nexus handler `BAD_REQUEST` for UNSPECIFIED | Explains the decision without exposing internal metrics |

### `TaskQueueBinding`

| Field (id) | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `name` (1) | Required exact task queue family name | Provider returns Nexus handler `BAD_REQUEST` when empty | Advises which queue the worker should poll |
| `type` (2) | Required Workflow, Activity, or Nexus | Provider returns Nexus handler `BAD_REQUEST` for UNSPECIFIED | Advises the poll RPC family |

### `InvokeWorkerResponse`

| Field (id) | Target policy | Error if invalid | Persistence / side-effect impact |
|---|---|---|---|
| `request_id` (1) | Must equal the request's Action_Request_ID | Mismatch is a non-retryable invalid-provider-response failure | Marks the matching outbox action delivered |

## Requirements

### Requirement 1: Feature Boundary and Enablement

**User Story:** As an operator, I want worker compute to be an explicit opt-in
control-plane feature, so that empty configuration never starts external capacity.

#### Acceptance Criteria

1. WHEN `policy.worker_compute.enabled` is absent, THE Tokeira_Config SHALL resolve it
   to `false`.
2. WHILE `policy.worker_compute.enabled` is `false`, THE Worker_Compute_Controller SHALL
   create no Provider_Action.
3. WHILE `policy.worker_compute.enabled` is `false`, THE Worker_Deployment_Registry
   SHALL continue to validate, persist, update, and describe ComputeConfig.
4. WHEN `policy.worker_compute.enabled` is `true`, THE Runtime SHALL start one
   cancellable Worker_Compute_Controller reconciliation service.
5. THE Worker_Compute_Controller SHALL NOT create a kernel command, kernel state field,
   workflow event, system Workflow, lane-affinity rule, or projection prerequisite.
6. THE Tokeira_Config SHALL reject unknown fields under `policy.worker_compute`.
7. THE Feature_Catalog SHALL classify `worker-compute-controller` as experimental,
   outside the v1.31.0 conformance surface, disabled by Temporal default, and disabled
   by Empty Configuration.
8. THE generated Tokeira configuration reference SHALL document the exact enablement
   TOML and the requirement that `ComputeProvider.nexus_endpoint` name a reachable
   registered endpoint.

### Requirement 2: Remote Provider and Scaling Group Eligibility

**User Story:** As a platform integrator, I want implementation-specific remote
providers to compose with existing scaling groups, so that Tokeira does not hard-code a
specific remote-provider or cloud-provider type.

#### Acceptance Criteria

1. WHEN `ComputeProvider.nexus_endpoint` is non-empty, THE
   Worker_Deployment_Registry SHALL accept any non-empty
   `ComputeProvider.type`.
2. WHEN `ComputeProvider.nexus_endpoint` is empty, THE
   Worker_Deployment_Registry SHALL preserve the existing built-in provider validation.
3. WHEN a Remote_Nexus_Provider omits `ComputeScaler`, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT`.
4. WHEN a Remote_Nexus_Provider uses the `no-sync` scaler, THE
   Worker_Compute_Controller SHALL classify its Scaling_Group as eligible.
5. WHEN a stored Scaling_Group uses `rate-based`, THE
   Worker_Compute_Controller SHALL classify it as unsupported without deleting or
   rewriting it.
6. WHEN a stored Scaling_Group uses a direct provider without a Nexus endpoint, THE
   Worker_Compute_Controller SHALL classify it as unsupported without deleting or
   rewriting it.
7. WHEN task queue types overlap between Scaling_Groups, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT`.
8. WHEN more than one Scaling_Group is a catch-all, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT`.
9. WHEN one Scaling_Group is a catch-all, THE Worker_Compute_Controller SHALL assign it
   only the task types not explicitly assigned to another group.
10. WHEN a Remote_Nexus_Provider names an endpoint that cannot be resolved, THE
    Worker_Compute_Controller SHALL set its Controller_Health to misconfigured.
11. WHEN provider eligibility validation fails, THE
    Worker_Deployment_Registry SHALL leave the previously committed ComputeConfig
    unchanged.

### Requirement 3: Scaler Decoding and Validation

**User Story:** As an operator, I want the pinned `no-sync` policy to reject malformed
settings, so that a misspelled or impossible scaler does not silently govern capacity.

#### Acceptance Criteria

1. WHEN `ComputeScaler.details` is absent for `no-sync`, THE
   Worker_Compute_Controller SHALL use all WCI_Pin defaults from the Contract Policy.
2. WHEN `ComputeScaler.details` cannot be decoded into a string-keyed object, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT`.
3. WHEN `ComputeScaler.details` contains an unknown key, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT` naming that key.
4. WHEN a numeric scaler field is a JSON number or numeric string, THE
   Worker_Deployment_Registry SHALL apply the WCI_Pin's conversion rule: int64
   fields truncate accepted JSON numbers toward zero and parse base-10 integer
   strings, float64 fields preserve accepted numeric values, and another type or
   invalid numeric string returns `INVALID_ARGUMENT` naming that field.
5. WHEN a numeric scaler value is below its documented minimum, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT` naming that field.
6. WHEN a positive `scale_up_cooloff_ms` exceeds `metrics_poll_interval_ms`, THE
   Worker_Deployment_Registry SHALL return `INVALID_ARGUMENT`.
7. WHEN a `no-sync` scaler passes validation, THE
   Worker_Deployment_Registry SHALL preserve its original `Payload` byte-for-byte.

### Requirement 4: Activation Reconciliation

**User Story:** As an operator, I want newly configured compute groups to start one
worker, so that the worker can register the task queues that later demand observations
will name.

#### Acceptance Criteria

1. WHEN the controller first observes an Eligible_Scaling_Group, THE
   Worker_Compute_Controller SHALL decide one Activation_Action with count `1`.
2. WHEN an Eligible_Scaling_Group's Configuration_Fingerprint changes, THE
   Worker_Compute_Controller SHALL decide one new Activation_Action for the new
   fingerprint.
3. WHEN Tokeira starts with the controller enabled and pre-existing eligible groups,
   THE Worker_Compute_Controller SHALL reconcile one Activation_Action per group that
   has no activation record for its current fingerprint.
4. WHEN an activation record already exists for the current fingerprint, THE
   Worker_Compute_Controller SHALL NOT create another Activation_Action during ordinary
   reconciliation.
5. WHEN ComputeConfig mutation commits, THE Worker_Deployment_RPC SHALL return without
   waiting for Activation_Action delivery.
6. WHEN an Activation_Action is created before any queue is observed, THE
   InvokeWorkerRequest SHALL contain an empty `task_queues` list.
7. WHEN an eligible group is removed, THE Worker_Compute_Controller SHALL stop creating
   actions for that group.
8. WHEN an eligible group is removed, THE Worker_Compute_Controller SHALL retain prior
   action records for audit and idempotent retry resolution.

### Requirement 5: Versioned Demand Observation

**User Story:** As a compute controller, I want exact-version task demand from the
delivery plane, so that capacity is requested only for the worker version that can
process the task.

#### Acceptance Criteria

1. WHEN a unique versioned workflow task is published, THE Workflow_Broker SHALL emit
   one Demand_Observation after deduplication.
2. WHEN a unique versioned activity task is published, THE Activity_Broker SHALL emit
   one Demand_Observation after deduplication.
3. WHEN a unique version-routed Nexus task is published, THE Nexus_Task_Broker SHALL
   emit one Demand_Observation after deduplication.
4. WHEN a task has no Deployment name or Build ID, THE Runtime SHALL emit no
   Demand_Observation.
5. WHEN a workflow task remains sticky-routed, THE Runtime SHALL emit no
   Demand_Observation.
6. WHEN a sticky workflow task falls back to its normal versioned queue, THE Runtime
   SHALL emit its Demand_Observation for that normal queue.
7. WHEN a compatible poller is waiting at publication, THE Demand_Observation SHALL
   identify a sync match.
8. WHEN no compatible poller is waiting at publication, THE Demand_Observation SHALL
   identify a no-sync match.
9. THE Demand_Observation SHALL identify namespace, task queue family, task type,
   Deployment name, and Build ID.
10. THE Demand_Observation path SHALL NOT wait for controller storage or Nexus I/O.
11. WHEN the controller observation channel is unavailable or full, THE task
    publication path SHALL continue normally.
12. THE runtime SHALL bound observation-channel memory independently of task backlog
    size.

### Requirement 6: Nexus Poll Version Identity

**User Story:** As a Nexus worker operator, I want Nexus polls to retain Deployment
Version identity, so that Nexus demand can participate in the same exact-version
controller policy as workflow and activity demand.

#### Acceptance Criteria

1. WHEN `PollNexusTaskQueueRequest.deployment_options` selects VERSIONED mode, THE Edge
   SHALL require non-empty Deployment name and Build ID.
2. WHEN a versioned Nexus poll is admitted, THE Edge SHALL preserve its Deployment name
   and Build ID in the runtime request.
3. WHEN a versioned Nexus poll waits for work, THE Nexus_Task_Broker SHALL register the
   waiter under its exact Deployment name and Build ID.
4. WHEN a Nexus task is version-routed, THE Nexus_Task_Broker SHALL match it only to a
   compatible versioned waiter.
5. WHEN a Nexus task is not version-routed, THE Nexus_Task_Broker SHALL preserve the
   existing unversioned delivery behavior.
6. WHEN a version-routed Nexus task has no compatible waiter, THE Nexus_Task_Broker
   SHALL create a No_Sync_Observation for its exact Deployment Version.
7. THE Nexus versioning changes SHALL NOT alter Nexus task tokens, response
   correlation, or workflow operation resolution.

### Requirement 7: Observation Batching and Group Routing

**User Story:** As an operator, I want bursty task additions coalesced per version, so
that provider decisions remain responsive without amplifying delivery traffic.

#### Acceptance Criteria

1. THE Worker_Compute_Controller SHALL batch Demand_Observations independently per
   Controller_Instance.
2. THE Worker_Compute_Controller SHALL use a fixed minimum no-sync batch interval of
   `500` milliseconds.
3. THE Worker_Compute_Controller SHALL use a fixed minimum sync-only batch interval of
   `60000` milliseconds.
4. WHEN a batch contains at least one No_Sync_Observation, THE
   Worker_Compute_Controller SHALL make it eligible at the no-sync interval.
5. WHEN a batch contains only sync-match observations, THE
   Worker_Compute_Controller SHALL make it eligible at the sync-only interval.
6. WHEN a batch becomes eligible, THE Worker_Compute_Controller SHALL retain the exact
   sync and no-sync counts accumulated since the prior eligible batch.
7. WHEN a Demand_Observation's task type maps to an Eligible_Scaling_Group, THE
   Worker_Compute_Controller SHALL evaluate only that group.
8. WHEN a Demand_Observation's task type has no Eligible_Scaling_Group, THE
   Worker_Compute_Controller SHALL create no Provider_Action.
9. WHEN a controller restart loses an in-memory batch, THE periodic Metrics_Snapshot
   path SHALL remain able to observe durable backlog demand.

### Requirement 8: Periodic Metrics Snapshot

**User Story:** As a compute controller, I want durable backlog levels and dispatch
rates sampled periodically, so that dropped observations or worker expiry cannot leave
queued work without capacity.

#### Acceptance Criteria

1. THE Worker_Compute_Controller SHALL schedule a Metrics_Snapshot for each
   Controller_Instance at the shortest active group's `metrics_poll_interval_ms`.
2. WHEN a Metrics_Snapshot is built, THE Worker_Compute_Controller SHALL aggregate all
   observed queues belonging to that Deployment Version.
3. THE Worker_Compute_Controller SHALL sum backlog counts separately for Workflow,
   Activity, and Nexus task types.
4. THE Worker_Compute_Controller SHALL sum dispatch rates separately for Workflow,
   Activity, and Nexus task types.
5. WHEN a Scaling_Group does not own a task type, THE Worker_Compute_Controller SHALL
   exclude that task type's aggregate from the group's scaler input.
6. WHEN no queue has yet been observed for a Deployment Version, THE
   Worker_Compute_Controller SHALL evaluate an all-zero Metrics_Snapshot.
7. WHEN a broker's live-ready memory is lost, THE Metrics_Snapshot SHALL derive backlog
   level from reconstructible delivery state rather than controller memory.
8. THE Metrics_Snapshot path SHALL NOT mutate workflow state or task ordering.

### Requirement 9: `no-sync` Decision Semantics

**User Story:** As an operator, I want scale-up decisions to match the pinned WCI
policy, so that cooloff, backlog, refresh, and rate suppression are predictable.

#### Acceptance Criteria

1. WHEN an eligible observation batch contains a no-sync match and the shared group
   cooloff has elapsed, THE `no-sync` scaler SHALL decide one Provider_Action.
2. WHEN an eligible observation batch contains only sync matches, THE `no-sync` scaler
   SHALL decide no Provider_Action.
3. WHEN an eligible no-sync batch arrives before cooloff elapses, THE `no-sync` scaler
   SHALL decide no Provider_Action.
4. WHEN any task-type backlog is strictly greater than
   `scale_up_backlog_threshold` and cooloff has elapsed, THE `no-sync` scaler SHALL
   mark that Metrics_Snapshot as requiring scale-up.
5. WHEN backlog is positive and elapsed time since the last scale-up is at least
   positive `max_worker_lifetime_ms`, THE `no-sync` scaler SHALL mark that task type as
   requiring worker refresh.
6. WHEN `max_worker_lifetime_ms` is zero, THE `no-sync` scaler SHALL disable the worker
   refresh rule.
7. WHEN `scale_up_dispatch_rate_epsilon` is positive and a prior rate exists, THE
   `no-sync` scaler SHALL suppress a metrics-driven task-type decision whose absolute
   dispatch-rate delta is no greater than epsilon.
8. WHEN no prior dispatch rate exists, THE `no-sync` scaler SHALL NOT apply epsilon
   suppression.
9. WHEN one or more task types require scale-up in one group snapshot, THE `no-sync`
   scaler SHALL decide exactly one Provider_Action for that group.
10. WHEN a Provider_Action is decided, THE `no-sync` scaler SHALL set its count to `1`.
11. WHEN a Provider_Action is decided, THE `no-sync` scaler SHALL advance the group's
    last-scale-up time to the decision time.
12. WHEN a Metrics_Snapshot is evaluated, THE `no-sync` scaler SHALL retain the latest
    dispatch rate separately for each task type.
13. WHEN a group changes between explicit and catch-all task types, THE `no-sync`
    scaler SHALL apply the newly computed Effective_Task_Types on its next evaluation.

### Requirement 10: Durable Controller State and Fencing

**User Story:** As an operator, I want controller decisions to survive process and
ownership changes, so that restart does not bypass cooloff or multiply provider calls.

#### Acceptance Criteria

1. THE Controller_State_Store SHALL persist state under namespace ID, Deployment name,
   Build ID, and Scaling Group ID.
2. THE Controller_State_Store SHALL persist Configuration_Fingerprint, activation
   status, last scale-up time, per-task-type prior dispatch rates, next metrics poll
   time, health, and a monotonic revision.
3. WHEN two controller owners evaluate the same revision concurrently, THE
   Controller_State_Store SHALL allow at most one decision commit.
4. WHEN a decision commit creates a Provider_Action, THE Controller_State_Store SHALL
   persist the updated scaler state and outbox row atomically.
5. WHEN Tokeira restarts, THE Worker_Compute_Controller SHALL resume from the persisted
   cooloff and prior dispatch-rate state.
6. WHEN ownership changes, THE Worker_Compute_Controller SHALL fence writes from the
   stale owner.
7. WHEN ComputeConfig changes, THE Worker_Compute_Controller SHALL prevent an
   undelivered action for the old fingerprint from being newly sent.
8. WHEN an old-fingerprint action is already in flight, THE provider request's
   Configuration_Fingerprint SHALL expose that staleness to the provider.
9. WHEN a Deployment Version is deleted, THE Worker_Compute_Controller SHALL stop
   future reconciliation for its Controller_Instance.
10. WHEN all Scaling_Groups are removed, THE Worker_Compute_Controller SHALL retain
    historical action records without retaining an active controller lease.
11. THE Controller_State_Store SHALL NOT be required to reconstruct or deliver a
    workflow, activity, or Nexus task.
12. THE Worker_Compute_Controller SHALL enforce a fixed soft limit of `100` active
    Controller_Instances per namespace.
13. WHEN the namespace soft limit is reached, THE Worker_Compute_Controller SHALL mark
    an additional instance capacity-limited without blocking its ComputeConfig commit.

### Requirement 11: Provider Action Contract

**User Story:** As a remote compute provider, I want a stable, provider-neutral Nexus
operation, so that I can launch the exact worker version without depending on Tokeira
internals.

#### Acceptance Criteria

1. THE Tokeira_Proto SHALL define `InvokeWorkerRequest`, `TaskQueueBinding`,
   `InvokeReason`, and `InvokeWorkerResponse` in
   `proto/tokeira/compute/v1/provider.proto`.
2. THE Worker_Compute_Controller SHALL invoke Nexus service
   `tokeira.worker.compute.v1.ComputeProvider`.
3. THE Worker_Compute_Controller SHALL invoke Nexus operation `invoke-worker`.
4. THE Worker_Compute_Controller SHALL encode one `InvokeWorkerRequest` with protobuf
   payload metadata.
5. THE Worker_Compute_Controller SHALL use Action_Request_ID as the Nexus request ID.
6. THE InvokeWorkerRequest SHALL preserve the exact namespace name, Deployment name,
   Build ID, Scaling Group ID, provider type, and provider details from its decision.
7. THE InvokeWorkerRequest SHALL contain only task queue identities and no workflow,
   activity, or Nexus task payload.
8. THE InvokeWorkerRequest SHALL contain no worker credential, bearer token, or
   authorization grant.
9. WHEN an observed task queue list is included, THE Worker_Compute_Controller SHALL
   sort it by task type and name.
10. WHEN duplicate queue bindings were observed, THE Worker_Compute_Controller SHALL
    include each unique binding once.
11. WHEN the provider returns synchronous success with the matching request ID, THE
    Worker_Compute_Controller SHALL mark the Provider_Action delivered.
12. WHEN the provider returns asynchronous acceptance, THE
    Worker_Compute_Controller SHALL mark the response invalid for this contract.
13. WHEN the provider returns a mismatched response request ID, THE
    Worker_Compute_Controller SHALL record a non-retryable invalid-provider-response
    failure.
14. THE remote compute provider SHALL treat Action_Request_ID as an idempotency key.

### Requirement 12: Durable Delivery and Failure Isolation

**User Story:** As an operator, I want provider calls to be retryable and isolated from
task delivery, so that an unavailable compute provider cannot damage workflow
correctness.

#### Acceptance Criteria

1. WHEN a Provider_Action is decided, THE Action_Outbox SHALL contain it before any
   Nexus request begins.
2. WHEN an Action_Outbox item is retried, THE Worker_Compute_Controller SHALL reuse its
   original Action_Request_ID.
3. WHEN endpoint resolution or Nexus transport fails transiently, THE
   Worker_Compute_Controller SHALL retry with bounded exponential backoff.
4. WHEN the provider returns a retryable Nexus handler error, THE
   Worker_Compute_Controller SHALL retry with bounded exponential backoff.
5. WHEN the provider returns a non-retryable Nexus handler error, THE
   Worker_Compute_Controller SHALL mark the action terminally failed.
6. WHEN an action becomes terminally failed, THE Worker_Compute_Controller SHALL expose
   the failure through Controller_Health.
7. WHEN a prior action failed terminally, THE `no-sync` scaler SHALL remain able to
   decide a later action from later demand after cooloff.
8. WHEN Tokeira restarts with an undelivered outbox item, THE
   Worker_Compute_Controller SHALL resume delivery with the same Action_Request_ID.
9. WHEN the provider accepts the same Action_Request_ID more than once, THE provider
   contract SHALL require one logical capacity action.
10. WHEN provider delivery is slow or unavailable, THE workflow, activity, and Nexus
    publication paths SHALL remain independent of that latency.
11. WHEN provider delivery fails, THE Worker_Compute_Controller SHALL NOT delete,
    reschedule, or reorder the task that produced demand.
12. WHEN shutdown begins, THE Worker_Compute_Controller SHALL stop claiming new outbox
    work before its shutdown deadline.
13. WHEN shutdown interrupts an in-flight action, THE Action_Outbox SHALL leave that
    action eligible for retry by a future owner.

### Requirement 13: Endpoint Resolution and Nexus Reuse

**User Story:** As a Tokeira operator, I want compute providers to use the existing
Nexus endpoint model, so that capacity delivery has one transport and one endpoint
administration surface.

#### Acceptance Criteria

1. WHEN `nexus_endpoint` resolves to an External endpoint, THE
   Worker_Compute_Controller SHALL use the existing outbound Nexus HTTP client.
2. WHEN `nexus_endpoint` resolves to a Worker endpoint, THE
   Worker_Compute_Controller SHALL use the existing Nexus task broker.
3. THE Worker_Compute_Controller SHALL NOT add a provider-specific HTTP route.
4. THE Worker_Compute_Controller SHALL NOT add AWS, GCP, Kubernetes, Firecracker, or
   remote-provider client logic.
5. WHEN endpoint metadata changes before an undelivered retry, THE
   Worker_Compute_Controller SHALL resolve the endpoint's current target.
6. WHEN an endpoint is deleted before an undelivered retry, THE
   Worker_Compute_Controller SHALL retain the outbox action and report a
   misconfigured endpoint.
7. THE provider invocation path SHALL preserve existing Nexus request-size, timeout,
   failure-category, and telemetry policies.

### Requirement 14: Observability and Operator Truth

**User Story:** As an operator, I want bounded controller telemetry and honest feature
documentation, so that I can distinguish demand, throttling, configuration errors, and
provider failures.

#### Acceptance Criteria

1. THE Worker_Compute_Controller SHALL record counts for observations, decisions,
   cooloff suppressions, epsilon suppressions, action retries, deliveries, and terminal
   failures.
2. THE Worker_Compute_Controller SHALL record action-delivery latency.
3. THE Worker_Compute_Controller SHALL expose Controller_Health by namespace,
   Deployment Version, and Scaling Group through a control-plane diagnostic API or
   operator command.
4. THE Worker_Compute_Controller SHALL bound metric labels independently of task queue
   name and Action_Request_ID.
5. WHEN logging one action, THE Worker_Compute_Controller SHALL include namespace,
   Deployment name, Build ID, Scaling Group, action reason, and Action_Request_ID.
6. THE Worker_Compute_Controller SHALL NOT log provider details or worker credentials.
7. THE generated Feature_Catalog SHALL state that only Remote_Nexus_Provider plus
   `no-sync` is active in this release.
8. THE generated Feature_Catalog SHALL state that `rate-based`, direct cloud providers,
   worker-set sizing, and scale-down are unavailable.
9. THE configuration example SHALL warn that enabling the controller can cause
   external capacity and cost through configured Nexus providers.
10. THE public documentation SHALL state that provider success acknowledges a capacity
    request rather than proof that a worker reached poll-ready state.

### Requirement 15: Cross-Repository and Regression Boundaries

**User Story:** As a maintainer, I want the controller contract independently testable
from the sibling worker-compute provider and authorization, so that either repository can evolve without hidden
engine coupling.

#### Acceptance Criteria

1. THE Tokeira test suite SHALL provide a provider-neutral Nexus test double that
   records InvokeWorkerRequest values and deduplicates Action_Request_ID.
2. THE controller tests SHALL cover activation, no-sync observation, backlog,
   worker-refresh, cooloff, epsilon suppression, restart recovery, stale configuration,
   retry, and terminal failure.
3. THE controller tests SHALL property-test that concurrent evaluation produces at
   most one outbox action for one state revision.
4. THE controller tests SHALL property-test that retries preserve Action_Request_ID
   and request payload.
5. THE controller tests SHALL property-test that supported scaler payloads either
   decode deterministically or fail with `INVALID_ARGUMENT`.
6. THE workflow and activity broker regression tests SHALL prove that a blocked or
   failed observation sink does not block publication.
7. THE Nexus regression tests SHALL prove that version identity does not alter task
   token correlation or response completion.
8. THE Worker Deployment regression tests SHALL retain existing ComputeConfig
   update-mask and round-trip behavior.
9. THE default workspace test suite SHALL require no live Nexus provider, sibling-provider
   process, cloud credentials, or Docker.
10. THE sibling-provider integration test SHALL remain outside Tokeira's default workspace bar.
11. THE `scoped-worker-authorization` spec SHALL own all guest-worker JWT/STS claim and
    RPC authorization changes.
12. THE Worker_Compute_Controller SHALL NOT grant a provider or launched worker access
    to another namespace, task queue, Deployment, or Build ID.

## Iteration and Feedback Notes

- **Ground-truth correction:** `docs/architecture/130-firecracker-worker-placement.md`
  and `131-firecracker-worker-placement-implementation.md` say v1.31.0 has no WCI
  component because the code is not in the main Temporal repository. The exact
  v1.31.0 `go.mod` pins `auto-scaled-workers edd947d743d2`, and
  `service/worker/fx.go @ v1.31.0` composes it. The architecture documents remain right
  that Tokeira must derive demand from its own broker and must not port a system
  Workflow; their source-history statement and proposed in-repo Firecracker actuation
  need supersession when this spec is implemented.
- **Scaler-number correction:** the WCI_Pin's
  `wci/workflow/iface/map_access.go` accepts JSON `float64` values for int64 fields
  when the pre-truncation value meets the minimum, then converts with `int64(val)`;
  it also accepts base-10 integer strings. The spec preserves that observable
  conversion instead of imposing a stricter integral-JSON rule.
- **Deliberate architectural adaptation:** the WCI_Pin performs provider validation and
  activation invocation inside its system-Workflow update. Tokeira already commits
  ComputeConfig independently and must not put remote I/O on that RPC path. This spec
  therefore reconciles activation asynchronously through a durable outbox.
- **Initial action surface:** the WCI_Pin declares both `invoke-worker` and
  `update-worker-set-size`, but only `no-sync` is registered at the pin and it emits
  `invoke-worker`. This spec does not invent scale-down or desired-size behavior.
- **Remote provider contract:** the vendored proto provides `nexus_endpoint` but neither
  the WCI_Pin nor the current upstream module defines a canonical Nexus operation
  schema. `tokeira.compute.v1.InvokeWorkerRequest` is therefore a Tokeira-owned public
  extension, not a claimed Temporal wire contract.
- **Provider boundary:** the sibling worker-compute provider may map every `invoke-worker` request to placement of one
  fleet-version worker. Slot lifetime, scale-down, Firecracker placement, and
  poll-ready reporting remain provider concerns.
- **Security boundary:** issue `tokeira/tokeira#29` blocks safe untrusted guest polling,
  not provider invocation. The separate `scoped-worker-authorization` spec will make
  that dependency explicit and fail closed.
- **Requirements-first gate:** design decisions about crate placement, DSQL schema,
  controller ownership leases, outbox retry constants, and the diagnostic read surface
  belong in `design.md` after these requirements are approved.
