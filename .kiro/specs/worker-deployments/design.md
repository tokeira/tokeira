# Design Document: Worker Deployments (v2 surface + versioning routing)

## Overview

This design implements the **Worker Deployment v2 surface** (P10 in the
api-conformance tracker) and makes Tokeira the owner of **worker-versioning routing
application**. It does four things:

1. **Implements the 13 v2 worker-deployment RPCs** — deployment CRUD, version CRUD,
   current/ramping selection, compute-config update/validate, version metadata,
   manager identity, and drainage — backed by a new **durable, namespace-scoped
   deployment registry**.
2. **Applies the persisted per-run versioning state to workflow-task dispatch
   routing** — consuming the fields that three sibling specs persist but explicitly
   defer the *application* of (versioning override, SDK-sent versioning behavior +
   deployment options, and the describe projection).
3. **Returns `UNIMPLEMENTED`** for the 5 deprecated pre-release `Deployment`
   companion RPCs, exactly matching Temporal server v1.31.0
   (`service/frontend/workflow_handler.go` returns `UNIMPLEMENTED` with the message
   "Deployments are deprecated and no longer supported, use Worker Deployments
   instead"; verified at [`github.com/temporalio/temporal` tag `v1.31.0`](https://github.com/temporalio/temporal/tree/v1.31.0)).
4. **Validates and reactivates pinned targets** through runtime-scoped TTL caches shared
   by every public path, while keeping queue publication a derived effect of durable
   registry and per-run state.
5. **Completes v1.31.0 Continue-as-New and target-change semantics** by resolving
   deployment and task-queue inputs in runtime, applying the notification state machine
   through the pure kernel transition, and recording inherited/declined decisions in
   authoritative history.

The wire shapes are derived from the vendored API `v1.62.11`
(`proto/upstream/temporal/api/deployment/v1/message.proto`,
`proto/upstream/temporal/api/workflow/v1/message.proto`,
`proto/upstream/temporal/api/compute/v1/config.proto`, and the worker-deployment
messages in `proto/upstream/temporal/api/workflowservice/v1/request_response.proto`).
The behaviour — defaulting, error mapping, lifecycle ordering, and routing precedence
— is derived from the v1.31.0 server source per AGENTS.md §8 and cited inline.

### Architectural placement (why a registry, not a system workflow)

Temporal implements Worker Deployments as **system workflows**
(`service/worker/workerdeployment/workflow.go` and `version_workflow.go`): the
deployment record is a long-running workflow whose `State` holds the routing config
and version map, and the version record is a second workflow holding drainage state.
**Tokeira must not port that.** AGENTS.md §Mission forbids porting Temporal code, and
the system-workflow approach would put control-plane correctness weight on per-run
history of a synthetic workflow — which collides with "history is authority" for
*user* runs and with the pure kernel.

Instead Tokeira models the registry as **namespace-scoped control-plane state** in a
new durable store in `tokeira-storage` (a `WorkerDeploymentRepository`), with the
state machine implemented as **pure transition functions** in the runtime
(`tokeira-runtime`). This mirrors the existing split used for shard leases and the
control/budget rows (`LeaseRepository`, `ControlRepository` in
`crates/tokeira-storage/src/api.rs`): durable single-document records guarded by
compare-and-swap, with the decision logic living above storage. The registry is
*not* per-run kernel state.

The **per-run versioning state** (effective behavior, effective deployment version,
versioning override, version transition, revision number, completing worker
deployment name) is genuinely per-run correctness state authored into history and
restored on replay — so it belongs on the kernel `WorkflowState`, exactly like the
recently added `cancel_requested` and `root_*` fields
(`crates/tokeira-kernel/src/state.rs`). It is consumed at dispatch time to decide
routing and projected by `DescribeWorkflowExecution`.

The kernel does not retain that state between invocations. Runtime/storage load a
`WorkflowState`, invoke the stateless transition evaluator, and durably commit the
returned `next_state` and events. Target-notification lineage follows this existing
model: it is part of the stored per-run state image because it changes future history,
but the kernel owns no cache, registry, connection, or background process.

## Dependencies and Non-Goals

### Owning relationships (this spec consumes sibling-persisted state)

- **`api-conformance-start-fields`** persists `StartWorkflowExecution.versioning_override`
  and threads `eager_worker_deployment_options`. This spec consumes the persisted
  override and **applies** it to first-WFT and subsequent routing (Requirement 9.3,
  9.7). It does not re-specify the start-field translation.
- **`api-conformance-wft-completion`** persists `RespondWorkflowTaskCompleted.deployment_options`
  / `versioning_behavior` onto history. This spec consumes that and **updates** the
  run's effective `deployment_version`, `behavior`, and `worker_deployment_name` after
  the task completes (Requirement 9.2). It does not re-specify WFT-completion
  persistence.
- **`api-conformance-workflow-describe`** owns the `DescribeWorkflowExecution`
  single-snapshot translation and explicitly leaves `versioning_info`,
  `worker_deployment_name`, and the deprecated build-id/version-stamp fields default.
  This spec fills those from the per-run versioning state, using the same run snapshot
  (Requirement 10).

### Non-goals

- **The deprecated build-id v1 surface** (`UpdateWorkerBuildIdCompatibility`,
  `GetWorkerBuildIdCompatibility`, assignment/redirect rule RPCs) stays under its
  existing handlers and the existing `VersioningRuleStore`
  (`crates/tokeira-runtime/src/versioning.rs`). This spec adds no v1 behaviour.
- **`describe_worker` / `list_workers`** are worker-observability RPCs that currently
  share the `deferred_unary!("worker-deployments")` block but are NOT deployment
  RPCs. This spec re-points them to their owning observability feature and does not
  implement them.
- **The 5 deprecated `Deployment` companions** are implemented only as the v1.31.0
  `UNIMPLEMENTED` response (Requirement 11). They are not projected over the v2
  registry.
- **Cross-task-queue propagation latency** (`RoutingConfigUpdateState` transitioning
  `IN_PROGRESS` → `COMPLETED` asynchronously) is an intentional compatibility-equivalent
  deviation: v1.31.0 derives `IN_PROGRESS` iff `len(PropagatingRevisions) > 0`, else
  `COMPLETED` (`service/worker/workerdeployment/client.go:1759 @ v1.31.0`). Tokeira
  commits routing synchronously, so there are no propagating revisions and the field is
  reported `COMPLETED` once the registry write commits.
- **Deployment-registry or task-queue-membership access from the kernel** remains
  prohibited. Runtime resolves all such inputs before the transition. The kernel only
  applies deterministic rules to the supplied target and inherited-versioning decision.

## Architecture

Two distinct paths. The **control-plane path** (registry CRUD + routing-config state
machine) and the **routing-application path** (per-run versioning consumed at
dispatch).

### Control-plane path (13 v2 RPCs)

```mermaid
flowchart LR
    Client["Temporal SDK / operator"] --> Grpc["WorkflowServiceGrpc::&lt;rpc&gt;"]
    Grpc --> Edge["WorkflowService deployment handlers<br/>(validate + translate)"]
    Edge --> NsRes["resolve_namespace_id"]
    Edge --> Adapter["WorkerDeploymentRuntimeApi<br/>(adapter)"]
    Adapter --> RtReg["runtime::deployment_registry<br/>(pure state machine + CAS)"]
    RtReg --> Store[("WorkerDeploymentRepository<br/>(tokeira-storage, durable)")]
    RtReg --> Pollers["WorkerRegistry<br/>(poller-presence, live)"]
    RtReg --> Outcome["DeploymentMutationOutcome / DeploymentView"]
    Outcome --> Translate["deployment DTO → proto<br/>(free functions)"]
    Translate --> Client
```

The edge handler validates inputs, resolves the namespace (`NOT_FOUND` if absent),
and calls the runtime adapter. The runtime loads the current registry record, runs
the **pure** transition (CAS / precondition checks / state mutation), and persists via
the repository's compare-and-swap write. Poller-presence checks read the live
`WorkerRegistry`. The runtime returns a view DTO; the edge translates it to proto with
free functions.

### Routing-application path (Requirement 9, consumed by dispatch)

```mermaid
flowchart TD
    subgraph Start["Workflow start (start-fields persists override)"]
        S1["StartRequest carries versioning_override + deployment options"] --> S2["kernel authors versioning_info into WorkflowExecutionStarted"]
    end
    subgraph Dispatch["WFT / activity dispatch"]
        D1["poller polls task queue"] --> D2["runtime resolves target version<br/>from registry routing_config + ramp"]
        D2 --> D3{"effective behavior?"}
        D3 -->|PINNED| D4["dispatch pinned tasks;<br/>pinned independent activities do not transition"]
        D3 -->|AUTO_UPGRADE / unversioned| D5{"poller version != effective version?"}
        D5 -->|WFT yes, gated on revision| D6["kernel.start_version_transition<br/>(set VersionTransition, reschedule pending WFT)"]
        D5 -->|activity yes| D8["start transition and reject activity;<br/>task dropped for later reschedule"]
        D5 -->|already transitioning| D9["reject activity start"]
        D5 -->|no| D7["dispatch to effective version"]
    end
    subgraph Complete["RespondWorkflowTaskCompleted (wft-completion persists behavior+options)"]
        C1["WFT completes on target version"] --> C2["kernel.apply_wft_versioning:<br/>set behavior, deployment_version,<br/>worker_deployment_name; clear transition<br/>if target matches (does NOT touch revision_number)"]
    end
    Start --> Dispatch --> Complete
    C2 -.-> Describe["DescribeWorkflowExecution projects versioning_info"]
```

This mirrors v1.31.0: the transition is started at **task-start** by a poller whose
deployment differs from the workflow's effective deployment
(`service/history/api/recordworkflowtaskstarted/api.go` and
`recordactivitytaskstarted/api.go @ v1.31.0` call
`MutableState.StartDeploymentTransition`), **not** at task creation; workflow-task
starts proceed through the transition path, while an activity start that triggers a
transition is rejected/dropped for later reschedule, and an activity start during an
already in-flight transition is also rejected (`recordactivitytaskstarted/api.go:188 @
v1.31.0`). The transition is completed at WFT completion
(`service/history/workflow/workflow_task_state_machine.go`
`afterAddWorkflowTaskCompletedEvent @ v1.31.0`).

Three dispatch refinements preserve that behavior without importing Temporal's
matching/history service topology:

1. A speculative WFT may calculate the differing poller Version for its response, but
   its start must not commit the transition. v1.31.0 marks the mutable-state write as a
   no-op for speculative starts and applies the completing worker's Version only if the
   speculative WFT commits (`recordworkflowtaskstarted/api.go:178-197 @ v1.31.0`).
   Tokeira therefore suppresses the transition operand before invoking the pure kernel;
   its existing speculative completion/rollback transition decides whether the
   worker-reported Version becomes authoritative.
2. An activity-start lookup normally derives the workflow task queue's routing config
   from the run's effective/recorded Deployment. An unversioned run has neither. In that
   case the runtime uses the activity poller's Deployment solely as the registry lookup
   hint, still resolving the workflow task queue (not the activity queue) before it
   decides whether to start a transition. This is Tokeira's registry-shaped equivalent
   of `getDeploymentVersionAndRevisionNumberForWorkflowID`
   (`recordactivitytaskstarted/api.go:195-225 @ v1.31.0`). Starting that transition also
   regenerates delivery for an unstarted, non-speculative pending WFT. v1.31.0 bumps its
   workflow-task stamp and generates another scheduling task without adding history
   (`mutable_state_impl.go:9178-9212 @ v1.31.0`). Tokeira uses a new internal
   `logical_seq` as the equivalent stale-offer fence and emits a replacement
   `EnqueueWorkflowTask` effect from the pure transition; the runtime remains the only
   layer that performs the queue write.
3. Sticky queues have no versioned physical queues in v1.31.0. If Current/Ramping moves
   away from the sticky worker's Version, matching returns `StickyWorkerUnavailable`
   and history retries the task on its normal queue
   (`task_queue_partition_manager.go:1918-1930 @ v1.31.0`). Tokeira compares the
   normal-family resolved target with the sticky offer's Version. If an older dispatch
   envelope omits deployment coordinates, the runtime first hydrates that sticky Version
   from the run's committed effective Version; resolving the sticky side through Current
   would erase the old-vs-new comparison and incorrectly migrate pinned work. On a real
   mismatch Tokeira clears the disposable sticky coordinate and publishes the normal task
   directly at the new target. The committed pending WFT remains authoritative throughout.

At the gRPC boundary, both poll request translators retain the task-queue kind long
enough to enforce v1.31.0's shared frontend validation: a sticky queue with empty
`normal_name` is invalid whenever deployment options are present or legacy
`use_versioning` is true (`workflow_handler.go:6356-6375 @ v1.31.0`). Validation occurs
before standalone-activity fallback, poller registration, or long-poll admission.

### Target notification and Continue-as-New path (Requirement 15)

```mermaid
flowchart TD
    Registry[("WorkerDeploymentRepository")] --> Resolve["runtime resolves current/ramping target<br/>and cross-TQ Version membership"]
    Policy["runtime policy<br/>system.enableSendTargetVersionChanged"] --> WftInput["StartWorkflowTaskRequest<br/>target + enabled"]
    Resolve --> WftInput
    State["storage-loaded WorkflowState"] --> Kernel["pure kernel transition"]
    WftInput --> Kernel
    Kernel --> WftEvent["WorkflowTaskStarted<br/>target-change flag"]
    Kernel --> NextState["next_state<br/>notified/declined lineage"]
    EdgeCommand["edge preserves CaN<br/>initial_versioning_behavior"] --> Prepare["runtime prepares successor decision"]
    Registry --> Prepare
    State --> Prepare
    Prepare --> Kernel
    Kernel --> CloseEvent["WorkflowExecutionContinuedAsNew<br/>initial behavior + successor decision"]
    CloseEvent --> Successor["runtime submits StartRequest<br/>with inherited versioning info"]
    Successor --> KernelStart["pure kernel Start transition"]
    KernelStart --> StartedEvent["WorkflowExecutionStarted<br/>inherited / declined fields"]
    Kernel --> ChildDispatch["committed child-start dispatch"]
    ChildDispatch --> ChildResolve["runtime loads committed parent<br/>and resolves Version membership"]
    Registry --> ChildResolve
    ChildResolve --> KernelStart
```

The runtime resolves the mutable control-plane facts before invoking the kernel. At WFT
start it supplies the concrete target (`None` means the unversioned target) and the
notification-policy boolean. At WFT completion it first projects the completion's
worker-reported behavior, Deployment Version, and Worker Deployment name onto a clone
of the loaded predecessor, then enriches the single terminal Continue-as-New command
with a concrete successor decision after reading cross-task-queue Version membership.
This ordering is observable when a workflow's first WFT reports `PINNED` and issues
Continue-as-New in that same completion: v1.31.0 applies
`afterAddWorkflowTaskCompletedEvent` before handling commands, so the successor sees
the reported `PINNED` state (`workflow_task_state_machine.go` and
`mutable_state_impl.go:2485-2630 @ v1.31.0`). The kernel never performs the mutable
reads; it atomically applies the already-resolved inputs to the same transition that
authors the public history.

Child starts use the same architectural boundary without pretending that they are
Continue-as-New successors. The derived child-start publisher loads the already-
committed parent, so a behavior or Version reported by the completion that issued the
child command is visible. It resolves cross-task-queue Version membership in the
runtime and supplies concrete `inherited_versioning_info` on the child's `StartRequest`.
This is Tokeira's equivalent of the observable child-start inheritance assembled in
`service/history/transfer_queue_active_task_executor.go:915-979 @ v1.31.0`; it does not
introduce Temporal's transfer/matching architecture.

The production policy is the v1.31.0 default `true`
(`common/dynamicconfig/constants.go:931-935 @ v1.31.0`). Conformance builds consult the
live override for `system.enableSendTargetVersionChanged` at the runtime call site so
the corpus's scoped `true`/`false` modes exercise the same transition without exposing
Temporal dynamic configuration as a Tokeira production setting.

## Components and Interfaces

### Edge handlers (`crates/tokeira-edge/src/grpc/workflow_service.rs`)

Replace the 13 `deferred_unary!("worker-deployments")` entries with real handlers, and
re-point `describe_worker` / `list_workers` out of this block. Each handler:

- resolves the namespace via `WorkflowService::resolve_namespace_id` (→ `NOT_FOUND`),
- validates required identifiers (`deployment_name`, `build_id`, deprecated `version`
  string) → `INVALID_ARGUMENT` before any mutation,
- calls the `WorkerDeploymentRuntimeApi` adapter method,
- translates the runtime view to proto with **free functions** (matching the
  `respond_activity_completed_to_edge` pattern; no `TryFrom`).

The 13 handlers: `create_worker_deployment`, `describe_worker_deployment`,
`delete_worker_deployment`, `list_worker_deployments`,
`create_worker_deployment_version`, `describe_worker_deployment_version`,
`delete_worker_deployment_version`, `set_worker_deployment_current_version`,
`set_worker_deployment_ramping_version`,
`update_worker_deployment_version_compute_config`,
`validate_worker_deployment_version_compute_config`,
`update_worker_deployment_version_metadata`, `set_worker_deployment_manager`.

Replace the 5 deprecated companion handlers (`describe_deployment`,
`list_deployments`, `get_deployment_reachability`, `get_current_deployment`,
`set_current_deployment`, currently at lines ~1069–1109) so each returns
`Status::unimplemented("Deployments are deprecated and no longer supported, use Worker
Deployments instead")` — the exact v1.31.0 message — before any state access
(Requirement 11). These do not route through the runtime adapter.

### Edge ↔ runtime seam: `WorkerDeploymentRuntimeApi` (adapter trait)

A new edge-side trait analogous to `WorkflowRuntimeApi`
(`crates/tokeira-edge/src/workflow_service.rs`), implemented by `RuntimeAdapter`
(`crates/tokeira-edge/src/grpc/runtime_adapter.rs`). It exposes one async method per
RPC, taking translated request DTOs and returning view DTOs or `EdgeError`. The
adapter is the only edge access to the registry; the edge never touches storage or
runtime internals directly (matching the CODEX rule that the edge talks to the runtime
through the adapter).

Method outcomes use a dedicated edge-adapter outcome type
(`DeploymentMutationOutcome { conflict_token, view }`), distinct from the concrete
runtime API result, mirroring the `WorkflowMutationOutcome` vs `CommitResult`
distinction the workflow path uses.

### Runtime registry API (`crates/tokeira-runtime/src/deployment_registry.rs`, new)

The **concrete runtime API**. Holds an `Arc<dyn WorkerDeploymentRepository>` plus a
handle to the live `WorkerRegistry` for poller presence. Implements the deployment +
version state machine as pure transition functions over a loaded record:

```rust
pub struct DeploymentRegistry<R> { /* repo + worker_registry + clock */ }

impl<R: WorkerDeploymentRepository> DeploymentRegistry<R> {
    // Deployment CRUD
    async fn create_deployment(&self, cmd: CreateDeployment) -> Result<DeploymentView, RegistryError>;
    async fn describe_deployment(&self, key: DeploymentKey) -> Result<DeploymentView, RegistryError>;
    async fn delete_deployment(&self, cmd: DeleteDeployment) -> Result<(), RegistryError>;
    async fn list_deployments(&self, page: ListPage) -> Result<DeploymentPage, RegistryError>;
    // Version CRUD
    async fn create_version(&self, cmd: CreateVersion) -> Result<(), RegistryError>;
    async fn register_polled_deployment(&self, cmd: RegisterPolledDeployment) -> Result<(), RegistryError>;
    async fn describe_version(&self, cmd: DescribeVersion) -> Result<VersionView, RegistryError>;
    async fn delete_version(&self, cmd: DeleteVersion) -> Result<(), RegistryError>;
    // Routing config
    async fn set_current_version(&self, cmd: SetCurrent) -> Result<SetCurrentOutcome, RegistryError>;
    async fn set_ramping_version(&self, cmd: SetRamping) -> Result<SetRampingOutcome, RegistryError>;
    // Compute config + metadata + manager
    async fn update_compute_config(&self, cmd: UpdateComputeConfig) -> Result<(), RegistryError>;
    async fn validate_compute_config(&self, cmd: ValidateComputeConfig) -> Result<(), RegistryError>;
    async fn update_version_metadata(&self, cmd: UpdateMetadata) -> Result<VersionMetadataView, RegistryError>;
    async fn set_manager(&self, cmd: SetManager) -> Result<SetManagerOutcome, RegistryError>;
}
```

The mutation methods follow a **load → validate (pure) → CAS-commit** loop: load the
record (with its current `conflict_token`), evaluate all preconditions on the loaded
snapshot, and persist with `compare_and_swap` keyed on the loaded `conflict_token`. A
CAS failure (another writer advanced the token) reloads and re-validates so a rejected
request never partially mutates state. This matches the OCC model already used by
`RunRepository::commit_transition` and the lease/budget CAS rows.

`RegistryError` is `thiserror`-based with variants mapping to the v1.31.0 error
contract: `AlreadyExists`, `NotFound`, `FailedPrecondition(reason)`,
`ResourceExhausted(reason)`, `InvalidArgument(reason)`. The edge maps these to `EdgeError`
(see Error Handling).

#### Poller-presence semantics (resolved against v1.31.0)

- `allow_no_pollers` (set-current/set-ramping): when `false`, an unknown target
  build_id is rejected as `NOT_FOUND` (`validateStateBeforeAcceptingSetCurrent` /
  `...Ramping` raise `errVersionNotFound`; `handleUpdateVersionFailures` maps it to
  `NewNotFoundf(ErrWorkerDeploymentVersionNotFound)`, `workflow.go:1230/1244` +
  `client.go:384 @ v1.31.0`). When `true`, an unknown build_id is auto-created as a
  Version by the set-current/set-ramping update path.
- `ignore_missing_task_queues` (set-current): when `false` and both the previous and
  new current versions are versioned (not unversioned), first compare the durable
  historical `polled_task_queues` memberships. A queue missing from the target is a
  rejection only when it has not moved to another deployment and its current physical
  queue has backlog or non-zero add-rate. Empty historical memberships therefore do not
  block promotion (`IsVersionMissingTaskQueues` / `isTaskQueueExpectedInNewVersion`,
  `service/worker/workerdeployment/client.go:1822-1926 @ v1.31.0`). For set-ramping the
  same check runs **only when the ramping version changes** and is evaluated against the
  deployment's **current** version. Tokeira keeps historical membership durable on the
  Version record and resolves live pressure from its disposable runtime task brokers;
  neither source enters per-run kernel state.

#### Poll-registration limits and server-initiated eviction

Versioned poll admission is a correctness-bearing registry mutation, not best-effort
observability. The edge invokes `register_polled_deployment` before entering the long
poll and propagates a rejected registration. The registry reads the v1.31.0 defaults
(`MatchingMaxVersionsInDeployment=100`,
`MatchingMaxTaskQueuesInDeploymentVersion=100`, `PollerHistoryTTL=5m`) through live
runtime accessors. Production builds return those constants; conformance builds may
read a delivered override at the same consult site. No mutable configuration reaches
the kernel.

Poller presence and durable registration deliberately have different ordering and
weight. Once the edge has resolved the physical Deployment-Version queue it records
the poll in the runtime's bounded `WorkerRegistry`, then awaits the durable registry
mutation before entering broker delivery. This prevents a query submitted concurrently
with a newly-started poll from observing a false blackhole. It also matches v1.31.0's
ordering, where `UpdatePollerInfo` precedes
`ensureRegisteredInDeploymentVersion`; a rejected registration can leave an expiring
poller-history observation but cannot create durable Deployment state
(`task_queue_partition_manager.go:601` and
`physical_task_queue_manager.go:462-475 @ v1.31.0`).
Observations are keyed by physical Deployment Version as well as identity: one SDK
process may poll v1 and v2 under the same worker identity, and the later v2 poll must
not erase v1's still-live history. This is the direct Tokeira equivalent of v1.31.0's
per-physical-queue `pollerHistory` ownership (`poller_history.go @ v1.31.0`).

At the Version limit, every add path (explicit create, poll auto-create, and
`allow_no_pollers` auto-create) sorts Versions by `(create_time, build_id)` and removes
the first candidate satisfying the normal current/ramping, recent-poller, and drainage
delete preconditions. The server-initiated path bypasses manager identity and does not
replace `last_modifier_identity` with an internal identity, matching
`tryDeleteVersion` (`workflow.go:1485-1504 @ v1.31.0`). Deletion and insertion occur in
one loaded-record CAS mutation, so a conflict reloads and re-evaluates both decisions.
If no candidate qualifies, the mutation rejects without a write and carries the exact
configured-limit message through `RegistryError::ResourceExhausted(reason)`.

Task-queue capacity counts distinct task-queue family names. Adding a second type for
an existing family is allowed at the limit; adding a new family rejects before status,
task-queue, or conflict-token mutation (`version_workflow.go:625-642 @ v1.31.0`).

The runtime keeps deletion liveness separate from the edge's diagnostic poller history.
Poll admission creates an exact `WorkerRegistry` registration guard. Normal completion
disarms the guard and leaves the latest observation eligible for the configured history
window; cancellation drops the guard and removes that exact live observation, so a
stopped worker cannot fence version deletion. `DescribeTaskQueue` continues to retain
bounded diagnostic history and aggregates identities from Tokeira's physical
Deployment-Version queues into the public task-queue-family view. This preserves the
observable v1.31.0 distinction between shutdown removal and recent poller history
without importing a matching-service architecture (`matching_engine.go:1194-1206` and
`task_queue_partition_manager.go:601, 617-621 @ v1.31.0`).

#### Due drainage recomputation

Tokeira does not create Temporal's internal Version entity workflow. Instead, the
runtime registry treats a `DRAINING` record as due when `now - last_checked_time`
reaches the visibility grace period for the first check
(`last_checked_time == last_changed_time`) or the refresh interval for later checks.
A public registry operation that loads the record runs this due check, reads open
pinned-workflow presence from `RunRepository`, and CAS-commits the resulting
`DRAINING`/`DRAINED` state before returning its view. This makes the observable state
identical without tainting the edge or manufacturing history. A racing reactivation is
safe: the CAS closure rechecks Current/Ramping state and clears rather than applying a
stale drainage result.

### Storage: `WorkerDeploymentRepository` (`crates/tokeira-storage/src/api.rs`, new trait; `memory.rs` + `dsql/` impls)

A new durable, namespace-scoped repository. Single-document-per-deployment with CAS,
backend-agnostic like `CasStore`:

```rust
#[async_trait]
pub trait WorkerDeploymentRepository: Send + Sync {
    async fn load_deployment(&self, key: &DeploymentKey)
        -> Result<Option<StoredWorkerDeployment>>;
    /// CAS write: succeeds only if the durable conflict_token equals `expected`.
    /// `expected == None` means "must not already exist" (create).
    async fn put_deployment(
        &self,
        record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult>;
    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult>;
    async fn list_deployments(
        &self,
        namespace_id: NamespaceId,
        after: Option<DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>>;
    /// Reload every deployment for a namespace (restart recovery).
    async fn list_all_for_namespace(&self, namespace_id: NamespaceId)
        -> Result<Vec<StoredWorkerDeployment>>;
}

pub enum DeploymentCasResult { Applied { token: ConflictToken }, Conflict, NotFound, AlreadyExists }
```

`StoredWorkerDeployment` embeds the routing config and the full version map (Versions
are stored inside their parent deployment record so a single CAS write atomically
covers a routing change + the version-status changes it implies, exactly as the
Temporal deployment workflow holds `State.Versions` and `State.RoutingConfig`
together). The dev `memory.rs` store keeps a
`HashMap<DeploymentKey, StoredWorkerDeployment>` under the existing `Mutex<StoreState>`;
the DSQL store adds a `worker_deployments` table keyed by `(namespace_id,
deployment_name)` with a `conflict_token` column for conditional writes.

### Kernel: per-run versioning state (`crates/tokeira-kernel/src/state.rs`)

The current `VersioningOverride` is a fieldless placeholder. Replace it and the
`Option<VersioningOverride>` field with a populated per-run versioning state, following
the `#[serde(default)]` + replay-restoration pattern used for `cancel_requested` /
`root_*`:

```rust
/// Per-run worker-versioning state, authored into history and restored on replay.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowVersioningInfo {
    /// Effective SDK-sent behavior (PINNED / AUTO_UPGRADE / UNSPECIFIED).
    pub behavior: VersioningBehavior,
    /// Deployment version that completed the last workflow task.
    pub deployment_version: Option<WorkerDeploymentVersionRef>,
    /// Execution-scoped override (precedence over behavior).
    pub versioning_override: Option<VersioningOverride>,
    /// In-flight transition target while a WFT/AT is pending on a new version.
    pub version_transition: Option<WorkerDeploymentVersionRef>,
    /// Monotonic routing-decision counter; staleness fence for dispatch.
    pub revision_number: i64,
    /// CaN initial behavior, only for the first task of this run / its retries.
    pub continue_as_new_initial_versioning_behavior: ContinueAsNewVersioningBehavior,
    /// Target most recently exposed through WorkflowTaskStarted.
    #[serde(default)]
    pub last_notified_target_version: Option<VersionTarget>,
    /// Target declined by this Continue-as-New chain.
    #[serde(default)]
    pub declined_target_version_upgrade: Option<VersionTarget>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkerDeploymentVersionRef { pub deployment_name: String, pub build_id: String }

/// `None`, unversioned, and concrete Version are observably distinct on the wire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum VersionTarget {
    Unversioned,
    Deployment(WorkerDeploymentVersionRef),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum VersioningOverride {
    Pinned { version: WorkerDeploymentVersionRef },
    AutoUpgrade,
}
```

`Option<VersionTarget>` deliberately uses both layers: outer `None` means no target has
been notified/declined, `Some(Unversioned)` represents the protobuf wrapper being
present with no deployment Version, and `Some(Deployment(_))` represents a concrete
Version. Collapsing the first two would break
`TestPinnedCaN_NeverSignaled_NewRunGetsSignalForUnversioned` and the wrapper semantics of
`WorkflowExecutionStartedEventAttributes.declined_target_version_upgrade`
(`history/v1/message.proto:198-216`). These values are retained by storage as part of
`WorkflowState`; the kernel instance retains nothing.

`ContinueAsNewVersioningBehavior` gains `Unknown(i32)` so proto3 unknown numeric enum
values round-trip through the command and close event. v1.31.0 does not validate this
field: any non-zero value prevents pinned inheritance, and only the known
`USE_RAMPING_VERSION` value selects ramping-first routing
(`mutable_state_impl.go:2494-2532,2621-2629,9130-9141 @ v1.31.0`).

`WorkflowState` gains `#[serde(default)] pub versioning_info: Option<WorkflowVersioningInfo>`
(absent == unversioned, matching the proto: "Absent value means the workflow execution
is not versioned") and `#[serde(default)] pub worker_deployment_name: Option<String>`.
These are populated/cleared by pure kernel transitions only — no I/O. New kernel
methods (pure):

- `start_version_transition(target, revision_number)` — sets `version_transition`,
  clears sticky affinity, and marks the pending WFT for reschedule; rejects when
  effective behavior is PINNED (mirrors `StartDeploymentTransition`'s
  `ErrPinnedWorkflowCannotTransition`, `mutable_state_impl.go @ v1.31.0`). Sets
  `revision_number`.
- `apply_wft_versioning(behavior, deployment_version, worker_deployment_name)` — at WFT
  completion: clears `version_transition` when its target equals the completing
  version; sets `behavior` (UNSPECIFIED ⇒ clear `deployment_version`, i.e.
  unversioned); sets `deployment_version` and `worker_deployment_name` otherwise
  (mirrors `afterAddWorkflowTaskCompletedEvent`).
- `effective_deployment()` / `effective_behavior()` — pure precedence functions
  (transition > override > behavior+deployment_version), the Tokeira analog of
  `GetEffectiveDeployment` / `GetEffectiveVersioningBehavior`
  (`service/history/workflow/util.go @ v1.31.0`).
- `apply_target_version_observation(enabled, target)` — applies the five-way v1.31.0
  notification decision (override suppression, AutoUpgrade suppression, equal-target
  reset, declined-target suppression, otherwise notify) and returns the event boolean.
  It mutates only the supplied state image and is called inside the existing WFT-start
  transition (`workflow_task_state_machine.go:495-532 @ v1.31.0`).

### Kernel command + event additions (`command.rs`, `event.rs`, `kernel.rs`)

- `StartRequest` already carries `deployment` / `build_id`; extend its
  `ExecutionOptions` so `versioning_override` is the populated `VersioningOverride`
  (start-fields persists it; this spec authors it into the started event and restores
  on replay). The `WorkflowExecutionStarted` history envelope gains defaulted
  versioning fields, authored from the start request and restored in
  `replay_history_prefix` — the same envelope pattern workflow-describe used for
  `root_*`.
- `WorkflowTaskCompletedRequest` (`command.rs`) already carries `worker_version`;
  extend it with `deployment_version` and `versioning_behavior` (persisted by
  wft-completion) so `apply_wft_versioning` runs deterministically on both the live
  transition and replay.
- `StartWorkflowTaskRequest` gains `target_version_changed_enabled: bool` and
  `target_deployment_version: Option<WorkerDeploymentVersionRef>`. The latter is a
  concrete runtime-resolved Version; `None` means the unversioned target whenever the
  enable bit is true. The kernel does not receive a registry handle or a resolver.
- `PendingWorkflowTask` and `HistoryEventKind::WorkflowTaskStarted` gain the resolved
  `target_worker_deployment_version_changed: bool` plus private replay operands
  `target_version_changed_enabled: bool` and
  `target_deployment_version: Option<WorkerDeploymentVersionRef>`. Retaining all three
  on the pending task is necessary because transient/speculative WFT starts may
  materialize their started event later; the materialized internal history must carry
  both the public decision and enough input to reconstruct its lineage effects without
  consulting the mutable registry. The edge exposes only the public Boolean
  (`workflow_task_state_machine.go:190-224,485-532 @ v1.31.0`).
- `WorkflowCommand::ContinueAsNew` gains
  `initial_versioning_behavior: ContinueAsNewVersioningBehavior` and
  `successor_versioning_info: Option<WorkflowVersioningInfo>`. Edge initializes the
  behavior from the wire; runtime enriches the successor info before submitting the
  completion; kernel passes both through to
  `HistoryEventKind::WorkflowExecutionContinuedAsNew`.
- `StartRequest` gains
  `inherited_versioning_info: Option<WorkflowVersioningInfo>`. Runtime copies the
  committed Continue-as-New event's concrete decision into the successor start. The
  kernel combines that inheritance state with the existing explicit
  `versioning_override`, initializes `next_state.versioning_info`, and authors the same
  values on `WorkflowExecutionStartedV2`.

All new serializable fields are appended and `#[serde(default)]` where an older
postcard shape can omit them. The existing pre-baseline posture remains the migration
reason; old-shape deserialization tests are retained as forward guards rather than
assuming postcard is self-describing.

### Edge command and history translation (`crates/tokeira-edge/src/grpc/translate.rs`, `translate/history_serializer.rs`)

`proto_command_to_workflow_command` preserves the raw
`ContinueAsNewWorkflowExecutionCommandAttributes.initial_versioning_behavior`, mapping
known values to named variants and every other integer to `Unknown(i32)`. The reverse
translator and `WorkflowExecutionContinuedAsNew` serializer emit the same integer.

The history serializer maps the internal start decision to the mutually exclusive
public fields:

- PINNED inherited behavior → `inherited_pinned_version`;
- AUTO_UPGRADE inherited behavior with a source Version and non-zero revision →
  `inherited_auto_upgrade_info` containing the source Version, revision, and CaN
  initial behavior;
- `declined_target_version_upgrade` outer presence follows
  `Option<VersionTarget>`, with `Unversioned` encoded as a present wrapper whose
  `deployment_version` is absent;
- `WorkflowTaskStarted.target_worker_deployment_version_changed` is copied verbatim
  from the internal event. Its private policy/target replay operands are never exposed
  on the Temporal event.

The internal event continues to carry the full `WorkflowVersioningInfo` used by replay;
the edge does not recompute a placement or notification decision while serializing.

### Runtime dispatch integration (`crates/tokeira-runtime/src/runtime/workflow_task.rs`, `runtime/activity.rs`, `publisher.rs`)

At task-start, the runtime resolves the **target version** for the workflow's task
queue from the deployment registry's routing config:

- compute the target version using the routing config: AUTO_UPGRADE / unversioned
  traffic follows `current_deployment_version`, with the ramp split sending
  `ramping_version_percentage`% (bucketed deterministically by workflow id, reusing
  the `deterministic_bucket` FNV-1a approach already in
  `crates/tokeira-runtime/src/versioning.rs`) to `ramping_deployment_version`; PINNED
  runs resolve to their pinned version regardless of routing config;
- if the polling worker's deployment version differs from the workflow's effective
  version and the workflow is not pinned, call `start_version_transition` gated on the
  dispatch `revision_number` (the activity path additionally gates on
  `revision_number > wft_dispatch_revision` matching
  `recordactivitytaskstarted/api.go @ v1.31.0`); `start_version_transition` **sets** the
  run's `revision_number` to the task's dispatch revision (it is set, never incremented);
- on WFT completion call `apply_wft_versioning`, which updates `behavior`,
  `deployment_version`, and `worker_deployment_name` and clears the transition on target
  match. It does **not** touch the run's `revision_number`: in v1.31.0 the run's
  `WorkflowExecutionVersioningInfo.revision_number` is set only at transition-start
  (`mutable_state_impl.go:9108` via `req.TaskDispatchRevisionNumber`) and on start-time
  inheritance (`mutable_state_impl.go:2963`); `afterAddWorkflowTaskCompletedEvent`
  (`workflow_task_state_machine.go:1283-1396 @ v1.31.0`) never assigns it. (The
  registry-level `RoutingConfig.revision_number` is a separate counter, bumped on every
  set-current / set-ramping mutation — see the routing-config state machine above.)

Routing decisions are derived effects of durable registry + per-run state; no
correctness weight rests on transient queues (Requirement 13.6).

#### WFT target-notification input

`start_polled_workflow_task` already calls `resolve_polled_workflow_task_target` before
submitting `Command::WorkflowTaskStarted`. It reuses that routing result as the
notification target and supplies `target_version_changed_enabled()` alongside it. The
production helper returns the v1.31.0 default `true`; under the `conformance` feature it
reads `system.enableSendTargetVersionChanged` from `tokeira-conformance` with the same
default. The key is added to the conformance override catalog as a namespace Boolean.

The routing target used for this notification is the task queue's Current/Ramping target,
not the pinned dispatch destination. Accordingly the runtime keeps two values in the
resolved-start structure: the existing effective dispatch target and the routing-config
target offered for notification comparison. This mirrors the observable operands of
`AddWorkflowTaskStartedEvent(..., targetDeploymentVersion)` without importing
Temporal's matching/history split.

#### Continue-as-New successor preparation

Before `complete_workflow_task` submits the completion, it loads the token-validated run
state and enriches the terminal Continue-as-New command with a pure
`resolve_continue_as_new_versioning` decision. The resolver takes:

```rust
fn resolve_continue_as_new_versioning(
    post_completion_predecessor: &WorkflowState,
    successor_task_queue: &TaskQueueName,
    initial_behavior: ContinueAsNewVersioningBehavior,
    source_version_has_successor_queue: bool,
    pinned_override_has_successor_queue: bool,
) -> Option<WorkflowVersioningInfo>;
```

Runtime obtains the two membership booleans from the shared `DeploymentRegistry`. Same
task queue means membership is already established and needs no repository read;
cross-task-queue membership is checked at workflow-task-family granularity. A new
boolean-returning registry method shares the existing positive/negative membership cache
but treats absence as `false` rather than turning normal non-inheritance into a public
`FAILED_PRECONDITION`.

The `post_completion_predecessor` is an ephemeral clone, not retained kernel or runtime
state. Runtime applies the same pure `WorkflowState::apply_wft_versioning` operation the
kernel will apply in the authoritative completion transition before it determines the
effective source Version or performs membership reads. This preserves v1.31.0's
completion-before-command ordering without moving registry access into the kernel or
making the clone authoritative.

The resolver follows `mutable_state_impl.go:2485-2630 @ v1.31.0`:

1. An effective PINNED predecessor plus `UNSPECIFIED` initial behavior inherits its
   effective Version only when the successor queue belongs to that Version.
2. An effective AUTO_UPGRADE predecessor, or a PINNED predecessor with any non-zero
   initial behavior, carries source Version + revision as initial AutoUpgrade state only
   when both exist and the successor queue belongs to that source Version.
3. A compatible pinned override is carried independently and retains effective
   precedence over inherited AutoUpgrade/ramping behavior.
4. For ordinary inherited AutoUpgrade first tasks, normal Current/Ramping selection
   wins when its target belongs to a different Deployment or its routing revision is at
   least the inherited source revision. An older target revision in the same Deployment
   retains the inherited source Version to prevent bounce-back
   (`chooseTargetQueueByFlag`, `task_queue_partition_manager.go:2061-2078 @ v1.31.0`).
   Tokeira's centralized `StoredRoutingConfig` retains separate Current and Ramping
   field revisions alongside the aggregate revision. This preserves the observable
   comparison without introducing Temporal's independently propagated matching data.
5. `USE_RAMPING_VERSION` makes only the successor's first WFT (and retry first WFTs)
   select the ramping Version directly, falling back to Current when no ramping target
   exists. Later WFTs resume normal percentage routing.
6. The successor's declined target is `last_notified_target_version` when present,
   otherwise the predecessor's existing `declined_target_version_upgrade`.

The enriched command commits the decision with the predecessor close. Lane successor
creation reads the committed event, not a volatile map, and copies its decision into
`StartRequest.inherited_versioning_info`. If the derived start is retried after a crash,
the event supplies identical operands and the deterministic successor request id still
deduplicates the start.

Workflow retry successors use the same start input without pretending that a retry is a
new CaN decision. `start_retry_successor` already reads the predecessor's
`WorkflowExecutionStarted` event for original input; it also reads that event's initial
versioning info. A retry inherits pinned placement only when that predecessor itself
started with inherited pinned state, while an AutoUpgrade predecessor carries its
current effective source Version/revision and the stored
`continue_as_new_initial_versioning_behavior`. The retry preserves the declined target
from the predecessor's started event, not a target merely notified later in that failed
run (`service/history/workflow/retry.go:253-338 @ v1.31.0`). This makes
`USE_RAMPING_VERSION` apply to the first WFT of every retry as specified, without
propagating it to an unrelated future CaN command.

#### Child-start versioning inheritance

After the parent completion commits, `RuntimeDispatchPublisher` loads that committed
parent and calls a pure `resolve_child_versioning` helper with runtime-resolved
membership booleans. Same-queue, same-namespace inheritance needs no registry read;
cross-queue inheritance requires workflow-task-family membership in the effective
source Version, and cross-namespace children inherit no v3 versioning state. Effective
PINNED state carries the source Version, a compatible pinned override is copied, and
effective AUTO_UPGRADE carries source Version plus revision when both are concrete.

The child helper always writes
`continue_as_new_initial_versioning_behavior = UNSPECIFIED`. The parent's
`USE_RAMPING_VERSION` flag was an instruction for that parent's initial WFT only and is
not lineage state. This matches
`transfer_queue_active_task_executor.go:950-979 @ v1.31.0`, which constructs child
`InheritedAutoUpgradeInfo` from the committed parent but explicitly sets the initial
behavior to unspecified. Registry/storage failure leaves the derived child start
unpublished rather than silently starting an incorrectly routed execution; the parent
history remains authoritative for recovery.

### Pinned membership and reactivation (`deployment_registry.rs`, edge/runtime adapters)

Temporal validates explicit pinned overrides through a cache of task-queue Version
membership and, after successful persistence, asynchronously signals a potentially
inactive Version workflow (`common/worker_versioning/worker_versioning.go` and
`service/history/api/worker_versioning_util.go @ v1.31.0`). Tokeira preserves the
observable ordering without importing that internal workflow architecture:

1. The runtime-scoped `DeploymentRegistry` owns both caches. Every adapter and the
   dispatch publisher shares this single instance; constructing one per request would
   discard the contractually observable negative-cache and dedup windows.
2. Explicit pinned inputs are checked against the durable Version's workflow
   task-queue-family membership before the run command is submitted. Both positive and
   negative results are cached by `(namespace, task queue family, deployment, build id)`
   for the delivered TTL (default one second, minimum one second).
3. Only after the workflow mutation commits, the caller asks the registry to reactivate
   the concrete pinned target. When enabled, one caller per
   `(namespace, deployment, build id)` TTL window may CAS `INACTIVE`/`DRAINED` to
   `DRAINING`; other states are no-ops. Errors are deliberately best-effort and cannot
   invalidate the already committed run transition.
4. Start, signal-with-start, update-options, batch updates, and post-reset updates all
   use this shared ordering. The reactivation TTL defaults to ten seconds and is clamped
   to at least one second (`common/dynamicconfig/constants.go` and
   `service/history/fx.go @ v1.31.0`).

The caches are derived runtime accelerators, not correctness state. Expiry causes a
fresh durable membership read or permits another idempotent CAS reactivation. Kernel
commands carry only deterministic per-run intent and never consult either cache.

### Start-history and derived publication fidelity

A concrete pinned start initializes `worker_deployment_name` in live state and authors
the same name in `WorkflowExecutionStarted`. Replay therefore reconstructs the routing
operand before any later WFT completion. Before broker publication, the runtime loads
the authoritative run, reads the shared registry's durable routing config, resolves the
target with the pure routing function, and selects the physical Deployment-Version
queue. Publication does not become an authority: loss or duplication of that queue
effect is repaired from committed run history and registry state.

Poll admission closes the inverse ordering race. A start may publish while the task
queue has no registered Deployment-Version membership and therefore initially place
the derived offer on the unversioned queue. Once a versioned poll commits membership,
the runtime checks the durable routing config and, when that poller's Version is Current
or an active Ramping target, re-keys any disposable unversioned workflow/activity offer
onto the physical queue before blocking the poll. This is the architecture-appropriate
equivalent of v1.31.0 interrupting and re-resolving spooled work when task-queue user data
changes (`ProcessSpooledTask`, `service/matching/task_queue_partition_manager.go @
v1.31.0`): Tokeira keeps the authoritative pending task in run history and repairs only
its delivery index.

### Compatibility matrix (`crates/tokeira-compatibility/src/matrix.rs`)

Move the `worker-deployments` `FeatureEntry` (id `"worker-deployments"`) from
`Unsupported` to its supported state with evidence. The 5 deprecated companions are
counted conformant via their v1.31.0 `UNIMPLEMENTED` behaviour. Update the existing
edge test `deferred_handler_blocks_return_tracked_unimplemented_messages` for the 13
RPCs and re-point `describe_worker` / `list_workers`.

## Data Models

### Durable registry model (derived from `deployment/v1/message.proto`)

`StoredWorkerDeployment` is the durable analog of `WorkerDeploymentInfo` plus the
embedded version map. All fields trace to the proto:

```rust
pub struct StoredWorkerDeployment {
    pub namespace_id: NamespaceId,
    pub name: DeploymentName,                  // WorkerDeploymentInfo.name (req 1)
    pub create_time: OffsetDateTime,           // create_time (3)
    pub routing_config: StoredRoutingConfig,   // routing_config (4)
    pub last_modifier_identity: String,        // last_modifier_identity (5)
    pub manager_identity: Option<String>,      // manager_identity (6); None == empty/unset
    pub routing_config_update_state: RoutingConfigUpdateState, // (7)
    pub versions: BTreeMap<BuildId, StoredVersion>,            // version_summaries source (2)
    pub conflict_token: ConflictToken,         // CAS guard (see below)
    pub create_request_ids: BTreeMap<RequestId, ()>,           // idempotent create dedupe
}

pub struct StoredRoutingConfig {
    pub current_version: Option<BuildId>,      // current_deployment_version (7); None == unversioned
    pub ramping_version: Option<BuildId>,      // ramping_deployment_version (9); None == unversioned
    pub ramping_version_percentage: f32,       // ramping_version_percentage (3); [0,100]
    pub current_version_changed_time: Option<OffsetDateTime>,            // (4)
    pub ramping_version_changed_time: Option<OffsetDateTime>,            // (5)
    pub ramping_version_percentage_changed_time: Option<OffsetDateTime>, // (6)
    pub revision_number: i64,                  // (10) monotonic, bumped on every mutation
}

pub struct StoredVersion {
    pub build_id: BuildId,                     // WorkerDeploymentVersion.build_id
    pub status: WorkerDeploymentVersionStatus, // CREATED/INACTIVE/CURRENT/RAMPING/DRAINING/DRAINED
    pub create_time: OffsetDateTime,
    pub routing_changed_time: Option<OffsetDateTime>,
    pub current_since_time: Option<OffsetDateTime>,    // unset if not current
    pub ramping_since_time: Option<OffsetDateTime>,    // unset if not ramping
    pub first_activation_time: Option<OffsetDateTime>,
    pub last_current_time: Option<OffsetDateTime>,
    pub last_deactivation_time: Option<OffsetDateTime>,
    pub ramp_percentage: f32,                  // 0 unless ramping
    pub drainage_info: Option<DrainageInfo>,   // None while current/ramping (8.5)
    pub metadata: BTreeMap<String, Payload>,   // VersionMetadata.entries
    pub compute_config: BTreeMap<String, ComputeScalingGroup>, // ComputeConfig.scaling_groups
    pub last_modifier_identity: String,
    pub polled_task_queues: BTreeMap<(TaskQueueName, TaskQueueType), OffsetDateTime>, // for stats + ignore_missing
    pub create_request_ids: BTreeMap<RequestId, ()>,
}

pub struct DrainageInfo {
    pub status: VersionDrainageStatus,         // DRAINING (1) | DRAINED (2)
    pub last_changed_time: OffsetDateTime,
    pub last_checked_time: OffsetDateTime,
}
```

Enum values mirror the proto exactly (`enums/v1/deployment.proto`,
`enums/v1/task_queue.proto`, `enums/v1/workflow.proto`):
`WorkerDeploymentVersionStatus` = UNSPECIFIED/INACTIVE/CURRENT/RAMPING/DRAINING/DRAINED/CREATED;
`VersionDrainageStatus` = UNSPECIFIED/DRAINING/DRAINED; `RoutingConfigUpdateState` =
UNSPECIFIED/IN_PROGRESS/COMPLETED; `VersioningBehavior` =
UNSPECIFIED/PINNED/AUTO_UPGRADE.

`ComputeScalingGroup` mirrors `ComputeConfigScalingGroup` (task_queue_types, provider,
scaler). `UpdateWorkerDeploymentVersionComputeConfigRequest.compute_config_scaling_groups`
carries `ComputeConfigScalingGroupUpdate` with a `FieldMask`; the accepted mask paths
are exactly `["task_queue_types", "provider", "provider.type", "provider.details",
"provider.nexus_endpoint", "scaler", "scaler.type", "scaler.details"]`
(`compute/v1/config.proto`). An empty mask on an existing group is a no-op; a mask on a
new group is ignored (the proto's documented semantics).

### Conflict-token model

The conflict token is an opaque optimistic-concurrency token. In v1.31.0 it is the
last-mutation timestamp marshaled to binary
(`d.State.ConflictToken, _ = updateTime.AsTime().MarshalBinary()` in `handleSetCurrent`
/ `handleSetRampingVersion`, and `workflow.Now(ctx).MarshalBinary()` in
`handleSetManager`, `workflow.go @ v1.31.0`). Tokeira does not need to reproduce the
exact bytes (the token is opaque to clients), only the **semantics**: a token uniquely
identifies a deployment-state generation, a write supplying a stale non-nil token is
rejected, and a successful write yields a new distinct token.

Tokeira models `ConflictToken` as an original `[u8; N]` encoding of a per-deployment
monotonic generation counter incremented on every mutating commit:
`ConflictToken = encode(generation)`. CAS compares the supplied token (when non-nil) to
the stored generation; the storage `put_deployment(expected)` performs the conditional
write. A nil/absent supplied token bypasses the check (matching `args.ConflictToken !=
nil` guards in `validateStateBeforeAccepting*`). The describe/create/set responses
return `encode(current_generation)`.

### Per-run versioning state model

As defined in Components (`WorkflowVersioningInfo`). Maps to
`WorkflowExecutionVersioningInfo` (`workflow/v1/message.proto`): `behavior` (1),
`deployment_version` (7), `versioning_override` (3), `version_transition` (6),
`revision_number` (8), `continue_as_new_initial_versioning_behavior` (9). The
deprecated `deployment` (2), `version` (5), and `deployment_transition` (4) are not
stored; describe leaves them default. `worker_deployment_name` projects from the
run's `worker_deployment_name` (the deployment that completed the most recent WFT,
`WorkflowExecutionInfo.worker_deployment_name`, field 23).

The two internal target-lineage fields are not members of public
`WorkflowExecutionVersioningInfo`; they map instead to history behavior:

| Internal field | Public/history contract |
|---|---|
| `last_notified_target_version: Option<VersionTarget>` | Determines whether a CaN that does not accept the offered target records it as declined; public WFT history records only the corresponding Boolean, while internal history retains the policy/target replay operands |
| `declined_target_version_upgrade: Option<VersionTarget>` | `WorkflowExecutionStartedEventAttributes.declined_target_version_upgrade` (40); outer absence versus present-unversioned is preserved |
| `PendingWorkflowTask.target_worker_deployment_version_changed` + private policy/target operands | `WorkflowTaskStartedEventAttributes.target_worker_deployment_version_changed` (9), including late materialization; private operands make lineage replay independent of the registry |

These fields participate in internal event/replay equality but do not by themselves
manufacture a non-empty `WorkflowExecutionInfo.versioning_info` for an otherwise
unversioned run. `has_execution_versioning_info()` continues to consider only the public
versioning-info fields; replay still retains the internal lineage.

### Continue-as-New start mapping

`WorkflowExecutionStartedV2.versioning_info` is the single internal event payload used
for replay. Serialization derives the public start attributes as follows:

| Internal decision | Started-event fields | Initial effective state |
|---|---|---|
| behavior PINNED + deployment Version | `inherited_pinned_version` | PINNED on that Version |
| behavior AUTO_UPGRADE + source Version + non-zero revision | `inherited_auto_upgrade_info` | AUTO_UPGRADE with source Version/revision; initial CaN behavior retained |
| compatible pinned override | `versioning_override` in addition to either row above | Override wins effective routing |
| declined `Unversioned` | present `declined_target_version_upgrade`, absent nested Version | lineage distinguishes unversioned from never notified |
| declined concrete Version | present wrapper + nested Version | same concrete declined target |
| no inherited behavior/override/lineage | all fields absent | unversioned default |

`WorkflowExecutionContinuedAsNew.initial_versioning_behavior` always carries the raw
known/unknown numeric value from the command. The internal close event additionally
carries the full successor decision because Tokeira's runtime creates the successor
from that committed event rather than from a Temporal history-service request object.

### Describe projection mapping (Requirement 10)

| Proto field (`WorkflowExecutionInfo`) | Source |
|---|---|
| `versioning_info.behavior` | `WorkflowVersioningInfo.behavior` |
| `versioning_info.deployment_version` | `.deployment_version` |
| `versioning_info.versioning_override` | `.versioning_override` |
| `versioning_info.version_transition` | `.version_transition` |
| `versioning_info.revision_number` | `.revision_number` |
| `versioning_info.continue_as_new_initial_versioning_behavior` | `.continue_as_new_initial_versioning_behavior` |
| `worker_deployment_name` (23) | `WorkflowState.worker_deployment_name` |
| `assigned_build_id` (19), `inherited_build_id` (20), `most_recent_worker_version_stamp` (16) | left default — superseded in v1.31.0 |

Absent versioning state ⇒ `versioning_info` and `worker_deployment_name` left default
(no fabricated placeholders, Requirement 10.4). Derived from the same `WorkflowState`
snapshot the rest of describe uses (10.5).

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid
executions of a system — essentially, a formal statement about what the system should
do. Properties serve as the bridge between human-readable specifications and
machine-verifiable correctness guarantees.*

This feature is well suited to property-based testing. The registry state machine, the
routing-config transitions, the drainage lifecycle, conflict-token CAS, the
effective-deployment precedence, and the serialization/restart round-trips are pure
logic with large generated input spaces. The properties below were derived from the
prework analysis and deduplicated so each provides unique validation value. Each is
implemented as a single property-based test (proptest) with a minimum of 100
iterations. Edge/example criteria (input validation, exact `UNIMPLEMENTED` messages,
namespace-not-found) are covered by generators feeding these properties or by
example-based unit tests (see Testing Strategy), not by standalone properties.

### Property 1: Deployment CRUD correctness

*For any* sequence of deployment create/describe/delete operations against a registry,
the observable state matches a reference model: a create on a fresh name succeeds and
makes the deployment describable; a create on an existing name fails with
`ALREADY_EXISTS`; a repeat create with a previously-seen `request_id` is a no-op
returning the existing token; a describe projects every stored field faithfully; a
delete of a version-free deployment removes it; a delete of a deployment with versions
fails with `FAILED_PRECONDITION` and leaves it present; read and non-delete mutations
on an unknown name fail with `NOT_FOUND`, while delete on an unknown target is a
success no-op (`client.go:1089 @ v1.31.0`).

**Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6, 1.7, 1.9, 1.10**

### Property 2: Deployment list pagination round-trip

*For any* set of deployments in a namespace and any generated `page_size` (including
non-positive and over-max values), paging through `ListWorkerDeployments` with the
returned `next_page_token` yields exactly one summary per deployment with no duplicates
and no omissions, an empty continuation token marks exhaustion, and out-of-range
`page_size` values are clamped to the server max rather than rejected
(`workflow_handler.go:4078 @ v1.31.0`).

**Validates: Requirements 1.5**

### Property 3: Version CRUD and deletion-precondition correctness

*For any* sequence of version create/describe/delete operations, the observable state
matches a reference model: creating a version with non-empty `build_id` + name yields a
`CREATED` record only when the parent deployment exists, and fails with `NOT_FOUND` if
the parent deployment is missing (`client.go:1238` + `util.go updateWorkflow @
v1.31.0`); a duplicate (name,build_id) fails with `ALREADY_EXISTS`
(`client.go:1296 @ v1.31.0`); an empty `request_id` is accepted and generated by the
server, and a repeat with a previously-seen `request_id` is a no-op; describe projects
every stored version field faithfully including `version_task_queues` (with
`stats`/`stats_by_priority_key` populated iff `report_task_queue_stats` is true); a
delete succeeds only when the version is neither Current nor Ramping, has no active
pollers, and is drained (or `skip_drainage` is set), otherwise failing with
`FAILED_PRECONDITION` and leaving the version present; read and non-delete mutations on
an unknown version fail with `NOT_FOUND`, while delete on an unknown target is a success
no-op (`client.go:1037 @ v1.31.0`).

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.7, 2.8, 2.10, 2.11, 2.12, 2.13, 2.15**

### Property 4: Deprecated version-string round-trip

*For any* (deployment_name, build_id) pair where the deployment name contains no
delimiter conflict, formatting either legacy `"<deployment_name>.<build_id>"` or
`"<deployment_name>:<build_id>"` and resolving it back identifies the same version;
`__unversioned__` and empty string resolve to unversioned (nil); and only a string with
neither delimiter that is not `__unversioned__`/empty is rejected with
`INVALID_ARGUMENT` (`common/worker_versioning/worker_versioning.go:1103 @ v1.31.0`).

**Validates: Requirements 2.9**

### Property 5: Routing-config state machine

*For any* deployment and any sequence of set-current / set-ramping operations, the
routing config evolves per the v1.31.0 rules: setting Current to an existing version
sets `current_deployment_version`, updates `current_version_changed_time`, and bumps
`revision_number`; an empty `build_id` sets Current/Ramping to nil; setting Current to
the version that is currently Ramping atomically unsets the Ramping version (and its
percentage); a ramp percentage in [0,100] sets the ramping version, percentage, and
times; a Ramping version equal to a non-nil Current version is rejected with
`FAILED_PRECONDITION`; and a successful mutation returns a fresh conflict token plus the
correct deprecated `previous_*` values.

**Validates: Requirements 3.1, 3.2, 3.3, 3.7, 3.8, 4.1, 4.2, 4.4, 4.8**

### Property 6: Conflict-token CAS rejects stale writes without mutation

*For any* deployment and any mutating worker-deployment RPC, a request supplying a
non-nil conflict token that does not match the deployment's current token is rejected
with `FAILED_PRECONDITION` and leaves the durable state unchanged, while a request
supplying the current token (or a nil token) is accepted and yields a new, distinct
token.

**Validates: Requirements 3.4, 4.5, 7.6, 13.4**

### Property 7: Poller-presence preconditions

*For any* set-current or set-ramping request, the poller-presence guards hold per
v1.31.0: with `allow_no_pollers` false a target build_id that is not a tracked version
is rejected with `NOT_FOUND`, while with it true the version is auto-created; with
`ignore_missing_task_queues` false, each historical comparison-version queue missing
from the target is rejected only if it has not moved to another deployment and has
current backlog/add-rate pressure. Missing but idle queues do not reject. With the flag
true the check is bypassed (for ramping, the check runs only when the ramping version
changes and compares against the Current version). Rejections use the exact
current-versus-ramping v1.31.0 message and leave registry state unchanged.

**Validates: Requirements 3.5, 3.6, 4.6, 4.7**

### Property 8: Compute-config update and validate

*For any* version and any sequence of compute-config update operations, the resulting
scaling-group map matches a reference model under `update_mask` semantics — an empty
mask on an existing group is a no-op, a non-empty mask updates only the named accepted
paths, the mask is ignored for a newly added group, and named removals delete groups —
and `ValidateWorkerDeploymentVersionComputeConfig` evaluating the same proposed update
leaves stored state byte-identical, rejects malformed groups/masks, and does not require
or assert Version existence (`workflow_handler.go:258`, `client.go:2037 @ v1.31.0`).

**Validates: Requirements 5.1, 5.2, 5.5, 5.6, 5.7, 5.9**

### Property 9: Version metadata CRUD

*For any* version and any sequence of metadata upsert/remove operations, the resulting
`VersionMetadata.entries` match a reference key-value model (upserts insert or replace,
removals delete), and the response returns the full metadata equal to the stored
entries.

**Validates: Requirements 6.1, 6.2, 6.4**

### Property 10: Manager identity and authorization

*For any* deployment with a set `manager_identity` M, set-current-version,
set-ramping-version, and delete-version carrying an identity other than M are rejected
with `FAILED_PRECONDITION`, while a request whose identity equals M succeeds; setting
the manager via a non-empty value, an empty value (unset), or `self=true` (manager :=
request identity) is not gated by the existing manager and produces the corresponding
stored `manager_identity`, returning a fresh token plus the prior manager identity
(`workflow.go:1177`, `:775/:1244/:1109 @ v1.31.0`).

**Validates: Requirements 7.1, 7.2, 7.3, 7.5, 7.7**

### Property 11: Drainage lifecycle

*For any* version, the drainage state follows the v1.31.0 lifecycle: a version that
stops being Current or Ramping while open pinned workflows target it is set to
`DRAINING` with `last_changed_time`; once no open pinned workflows remain it becomes
`DRAINED` with `last_changed_time`; a version that becomes Current or Ramping again has
its drainage info cleared; a recompute records `last_checked_time`; and while a version
is Current or Ramping its `drainage_info` is never populated. A recomputation before
the configured first-check grace period or later refresh interval is a no-op; once due,
the first observing registry operation commits the recomputed state. Reactivation and a
later demotion start a fresh grace-period cycle.

**Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9**

### Property 12: Routing determinism and effective-version precedence

*For any* routing config, per-run versioning state, and workflow id, the routing target
is deterministic and follows the v1.31.0 precedence (transition > override > behavior +
deployment_version): a PINNED run (or PINNED override) routes to its pinned version; an
AUTO_UPGRADE / unversioned run follows the Current version, with the
`ramping_version_percentage` fraction of workflow ids deterministically routed to the
Ramping version and the remainder to the Current version; and when the Current version
is nil, AUTO_UPGRADE / unversioned traffic resolves to unversioned workers.

**Validates: Requirements 9.1, 9.3, 9.4, 9.8**

### Property 13: Deployment-version transition lifecycle

*For any* run and any workflow/activity task-start by a poller whose deployment version
differs from the run's effective version: if the run is unpinned, a workflow-task start
starts a transition toward the poller's version gated on the dispatch `revision_number`,
while an activity-task start that triggers the transition is rejected and the task is
dropped for later reschedule; if a transition is already in flight, activity starts are
rejected; pinned-workflow independent activities do not transition; and when a workflow
task completes carrying a versioning behavior and deployment, the run's effective
`behavior`, `deployment_version`, and `worker_deployment_name` are updated (an
UNSPECIFIED behavior clears the deployment version to unversioned) and the transition is
cleared when its target matches the completing version. The run's `revision_number` is
**set** to the task's dispatch revision at transition-start and is **not** mutated at WFT
completion: in v1.31.0 it is assigned only in `StartDeploymentTransition`
(`mutable_state_impl.go:9108 @ v1.31.0`) and on start-time inheritance
(`mutable_state_impl.go:2963 @ v1.31.0`); `afterAddWorkflowTaskCompletedEvent`
(`workflow_task_state_machine.go:1283-1396 @ v1.31.0`) never assigns it.

The generated cases additionally distinguish normal/transient WFT starts (which may
commit a transition) from speculative starts (which must not), and cover the
unversioned-run activity lookup fallback plus sticky-target migration. Poll translation
tests cover workflow and activity requests with deployment options and legacy
capabilities.

**Validates: Requirements 9.2, 9.5, 9.6, 15.41, 15.42, 15.43, 15.44**

### Property 14: Versioning-info projection fidelity

*For any* per-run versioning state, `DescribeWorkflowExecution` projects
`WorkflowExecutionInfo.versioning_info` (behavior, deployment_version,
versioning_override, version_transition, revision_number,
continue_as_new_initial_versioning_behavior) and `worker_deployment_name` exactly from
that state, leaves the deprecated `assigned_build_id` / `inherited_build_id` /
`most_recent_worker_version_stamp` default, and — when there is no versioning state —
leaves `versioning_info` and `worker_deployment_name` default without fabricating
placeholder values.

**Validates: Requirements 10.1, 10.2, 10.3, 10.4**

### Property 15: Identity propagation

*For any* write worker-deployment RPC carrying a non-empty `identity`, the affected
deployment or version records that identity as its `last_modifier_identity` (and
`SetWorkerDeploymentManager.self=true` records it as `manager_identity`).

**Validates: Requirements 12.1, 6.5**

### Property 16: No mutation on rejected request

*For any* worker-deployment request that is rejected for any reason (invalid argument,
not found, failed precondition, already exists, resource exhausted, manager mismatch,
conflict-token mismatch), the durable registry state is identical before and after the
call. Delete requests for missing targets are accepted success no-ops, not rejected
requests, and the reference model treats them as accepted-with-no-state-change.

**Validates: Requirements 12.4**

### Property 17: Registry restart-recovery round-trip

*For any* registry state (deployments, versions, routing configs with
`revision_number`, version metadata, compute configs, manager identities, and drainage
state), persisting it and reloading from durable storage yields a registry equal to the
original, and conflict tokens issued before the reload are evaluated against the
reloaded state with identical CAS semantics.

**Validates: Requirements 13.1, 13.2, 13.3, 13.4**

### Property 18: Per-run versioning replay round-trip

*For any* per-run versioning state authored into history, replaying the history restores
an equal `WorkflowVersioningInfo`, so post-restart routing decisions
(`effective_deployment` / `effective_behavior`) match the pre-restart decisions.

**Validates: Requirements 13.5**

### Property 19: Poll-registration limits and atomic oldest-eligible eviction

*For any* deployment with generated Versions, create times, routing roles, drainage
states, recent-poller observations, task-queue family/type memberships, and configured
limits, a poll registration below both limits succeeds; a new family at the family
limit rejects without mutation while a new type for an existing family succeeds; and a
new Version at the Version limit atomically removes exactly the oldest eligible Version
before insertion, or rejects without mutation when none is eligible. The manager
identity does not block server-initiated eviction and the deployment's
`last_modifier_identity` is not replaced by an internal maintenance identity.

**Validates: Requirements 2.5, 2.16, 2.17, 2.18, 2.19, 12.4**

### Property 20: Pinned membership cache fidelity

*For any* sequence of Version/task-queue membership changes and validation times,
positive and negative answers remain stable until the configured TTL expires and are
refreshed afterward. A missing membership rejects without changing the run, and all
public callers observe the same cache instance.

**Validates: Requirements 14.1, 14.2, 9.11**

### Property 21: Post-commit reactivation deduplication

*For any* sequence of pinned operation outcomes, target statuses, enable values, and
times, only successful concrete pinned commits may change `INACTIVE`/`DRAINED` to
`DRAINING`; at most one logical change occurs per target inside the TTL; and expiry
permits a later reactivation. Failures and non-pinned/no-change inputs never reactivate.

**Validates: Requirements 14.3, 14.4, 14.5, 14.6**

### Property 22: Target-change notification state machine

*For any* notification-enable value, effective behavior, optional override, effective
Version target, routing-config Version target (including unversioned), last-notified
target, and declined target, applying a workflow-task-start transition matches the
v1.31.0 reference model: disabled and unversioned/AutoUpgrade executions never notify;
an override suppresses notification and clears both lineage values; an effective target
equal to the routing target suppresses notification and clears both lineage values; a
target equal to the declined target suppresses notification without changing that
decline; every other pinned target difference emits true, stores that target as
last-notified, and clears the prior decline. Repeating the transition with identical
inputs is deterministic, and concrete/unversioned/absent targets remain distinct.

**Validates: Requirements 15.2, 15.3, 15.4, 15.5, 15.6, 15.7, 15.8, 15.9, 15.10, 15.11, 15.30, 15.31, 15.32**

### Property 23: Continue-as-New versioning decision

*For any* predecessor versioning state, worker-reported completion behavior/Version,
same/cross-task-queue choice, generated source and override membership booleans, routing
config, known or unknown initial behavior, and notification lineage,
`resolve_continue_as_new_versioning` matches a reference model after applying the same
completion-before-command projection: unspecified PINNED inherits only a compatible
source Version; AUTO_UPGRADE or a non-zero PINNED initial behavior carries compatible
source Version/revision as initial AutoUpgrade state; a compatible pinned override
retains precedence; known
ordinary inherited AutoUpgrade first-task placement follows the same-Deployment
revision comparison; `USE_RAMPING_VERSION` selects ramping then Current only for the
initial WFT; unknown non-zero values take non-ramping AutoUpgrade; and the successor decline is
last-notified-or-existing-declined. A later CaN with unspecified behavior does not
inherit an earlier command's explicit behavior. For retries, the same model carries the
stored initial behavior/source revision and started-event decline, while pinned
inheritance occurs only when the failed run itself began with inherited pinned state.

**Validates: Requirements 15.14, 15.15, 15.16, 15.17, 15.18, 15.20, 15.21, 15.22, 15.23, 15.25, 15.26, 15.34, 15.35, 15.36**

### Property 24: Versioning history and replay round-trip

*For any* target-notification result and private policy/target replay operands, inherited
pinned/AutoUpgrade decision, declined target (absent, unversioned, or concrete),
override, and known/unknown CaN initial behavior, serializing the internal events emits the exact public
`WorkflowTaskStarted`, `WorkflowExecutionContinuedAsNew`, and
`WorkflowExecutionStarted` fields; replaying those internal events restores equal
per-run versioning and lineage state. Mutually exclusive inherited fields are never
emitted together, private replay operands never leak to the public event, and a
late-materialized WFT-start event preserves its original decision and operands.

**Validates: Requirements 15.12, 15.13, 15.24, 15.27, 15.29, 15.33**

### Property 25: Runtime-resolved boundary determinism

*For any* loaded run, routing config, task queue, membership results, and policy value,
the runtime supplies concrete target/successor operands before kernel invocation, and
repeated pure-kernel evaluation of the same loaded state and command yields equal next
state and history without consulting a registry, cache, clock, queue, or I/O source.

**Validates: Requirements 15.1, 15.2, 15.19, 15.22, 15.28, 15.37, 15.38, 15.39, 15.40**

## Error Handling

The runtime registry returns `RegistryError`; the edge maps it to `EdgeError`, which
maps to a tonic `Status` via `crates/tokeira-edge/src/grpc/errors.rs`. New `EdgeError`
variants are added where no existing variant fits (`AlreadyExists`,
`ResourceExhausted`); `FailedPrecondition`, `NamespaceNotFound`, and the existing
not-found/invalid-argument paths are reused. Both `errors.rs` (status_code +
action_name) and `grpc/errors.rs` (tonic mapping) get entries for the new variants.

| Condition | RegistryError / source | EdgeError | gRPC status |
|---|---|---|---|
| Namespace does not exist | resolved at edge | `NamespaceNotFound` | `NOT_FOUND` |
| Empty `deployment_name` / `build_id`; malformed legacy `version` string; name with `.`/`:`/`__` prefix (v1.31.0 `validateVersionWfParams`) | `InvalidArgument` | `InvalidArgument` (new or reused) | `INVALID_ARGUMENT` |
| `percentage` outside [0,100] | `InvalidArgument` | `InvalidArgument` | `INVALID_ARGUMENT` |
| Unknown mask path / malformed compute group; key in both upsert+remove; group in both update+remove; oneof unset; empty required identity | `InvalidArgument` | `InvalidArgument` | `INVALID_ARGUMENT` |
| Deployment/version does not exist (read or non-delete mutation; delete is the exception and returns success no-op) | `NotFound` | `WorkflowNotFound`-style not-found / new `DeploymentNotFound` | `NOT_FOUND` |
| Set current/ramping to unknown build_id with `allow_no_pollers` false | `NotFound` (v1.31.0 `errVersionNotFound`) | not-found | `NOT_FOUND` |
| Create deployment name exists | `AlreadyExists` | `AlreadyExists` (new) | `ALREADY_EXISTS` |
| Create version (name,build_id) exists | `AlreadyExists` (v1.31.0 `ErrWorkerDeploymentVersionAlreadyExists`) | `AlreadyExists` | `ALREADY_EXISTS` |
| Add version at max with no eligible eviction candidate; add task-queue family at max | `ResourceExhausted(reason)` (v1.31.0 `errTooManyVersions` / `errMaxTaskQueuesInVersionType`) | `ResourceExhausted(reason)` | `RESOURCE_EXHAUSTED` |
| Delete deployment with versions; delete current/ramping/pollered/draining version | `FailedPrecondition` | `FailedPrecondition` | `FAILED_PRECONDITION` |
| Conflict-token mismatch | `FailedPrecondition` (v1.31.0 `errFailedPrecondition`) | `FailedPrecondition` | `FAILED_PRECONDITION` |
| Manager-identity mismatch | `FailedPrecondition` (v1.31.0 `ErrManagerIdentityMismatch`) | `FailedPrecondition` | `FAILED_PRECONDITION` |
| Ramping version equals non-nil Current | `FailedPrecondition` | `FailedPrecondition` | `FAILED_PRECONDITION` |
| Missing target membership for a still-owned queue with backlog/add-rate, guard flag false | `FailedPrecondition` (v1.31.0 `ErrCurrentVersionDoesNotHaveAllTaskQueues` / `ErrRampingVersionDoesNotHaveAllTaskQueues`) | `FailedPrecondition` | `FAILED_PRECONDITION` |
| Pinned run cannot transition (dispatch path) | kernel `ErrPinnedWorkflowCannotTransition` → drop stale task | n/a (matching drops) | n/a |
| Cross-task-queue CaN Version membership absent | normal non-inheritance decision | n/a | no error; successor follows its eligible initial routing |
| Registry/storage read fails while preparing CaN operands | runtime error before WFT completion submission | `Internal` | `INTERNAL`; predecessor WFT remains uncompleted |
| 5 deprecated `Deployment` companions | n/a (no state access) | `Unimplemented` (exact v1.31.0 message) | `UNIMPLEMENTED` |

The 13 v2 RPCs never return `UNIMPLEMENTED` (Requirement 12.5). `EdgeError::Internal`
is not used for any of these user-facing conditions.

## Testing Strategy

### Dual testing approach

- **Property tests (proptest, required)** implement Properties 1–25, each tagged
  `// Feature: worker-deployments, Property N: <text>` and configured for a minimum of
  100 iterations. They use a reference model for the CRUD/state-machine properties
  (Properties 1, 3, 5, 8, 9), deterministic generators for routing and ids
  (Properties 12, 13), and serialization round-trips for recovery (Properties 17, 18).
  Generators deliberately include the edge/example inputs (empty names, names with
  `.`/`:`/`__`, out-of-range percentages, bad mask paths, overlapping upsert/remove
  sets, unknown build_ids) so the validation and `NO mutation on rejection` properties
  (Properties 6, 7, 16) exercise them.
  The target-notification model generates all combinations of enabled state, behavior,
  override, effective/target/declined values (Property 22); the CaN model generates
  same/cross-queue membership, known/unknown initial behavior, source revision, routing
  config, and lineage (Property 23); event/replay generators cover
  absent/unversioned/concrete wrappers and late WFT materialization (Property 24); and
  the boundary property repeats pure transitions over pre-resolved operands (Property
  25).
- **Unit tests (example-based)** cover the deterministic edge/example criteria that are
  not input-varying: the exact `UNIMPLEMENTED` message for each of the 5 deprecated
  companions and that they touch no registry state (Requirement 11.1–11.6); empty
  `deployment_name` / unset oneof / empty identity → `INVALID_ARGUMENT` (1.8, 7.4, 7.8,
  2.14); namespace-not-found (1.11, 12.2); max-version eviction/exhaustion and
  task-queue-family exhaustion with exact messages (2.5, 2.17, 2.18); overlapping
  upsert/remove and update/remove → `INVALID_ARGUMENT` (6.3, 5.3);
  `eager_worker_deployment_options` applied iff `request_eager_execution` (9.7); and
  that all 13 v2 RPCs accept valid input without `UNIMPLEMENTED` (12.5). Tier 8.41
  examples cover notification-disabled lineage preservation, pinned-override
  suppression, target-to-unversioned, same/cross-task-queue pinned CaN, override
  precedence, AutoUpgrade/UseRamping first-task placement, unknown enum round-trip,
  committed-parent child inheritance, and reset of the child's initial behavior to
  unspecified.
- **Integration tests** exercise the full edge → runtime adapter → registry → storage
  path for a representative RPC of each family (create/describe deployment, create
  version, set-current with ramp-unset, set-ramping, manager mismatch, drainage
  describe), plus a restart-recovery integration test that mutates the registry,
  drops the in-memory runtime, reloads from the store, and asserts describe/list return
  the pre-restart state (Requirements 13.2, 13.3). Routing integration covers a
  start → dispatch → WFT-completion → describe cycle confirming the transition and
  projected `versioning_info`. Tier 8.41 integration runs a pinned workflow through
  target change → WFT notification → Continue-as-New → successor start and asserts
  agreement between polled history, stored history, Describe, and physical Version
  placement. A cross-task-queue pair proves membership-controlled inheritance, and a
  conformance-policy case proves the scoped disabled value reaches the runtime consult
  site.

### Property-test placement and libraries

Properties for the pure registry state machine and routing logic live in
`crates/tokeira-runtime` (`deployment_registry.rs` and dispatch routing modules);
the storage round-trip (Property 17) lives in `crates/tokeira-storage` alongside the
existing preservation/round-trip property tests; the per-run replay round-trip
(Property 18) and projection fidelity (Property 14) live in `crates/tokeira-kernel`
and `crates/tokeira-edge` respectively. Use the workspace-standard `proptest`
(matching `crates/tokeira-runtime/src/versioning.rs` and
`crates/tokeira-storage/src/preservation_property_tests.rs`); do not hand-roll
property infrastructure.

Property 22 lives in `crates/tokeira-kernel` beside the pure WFT-start state transition.
Property 23 lives in `crates/tokeira-runtime/src/runtime/workflow_task.rs` beside the
pure CaN resolver. Property 24 is split across kernel event/replay and edge history
serialization while retaining one generated shared case model. Property 25 lives in
runtime and invokes the kernel only with pre-resolved generated operands; its code and
crate dependency graph make registry access from the kernel unrepresentable.

### Behaviour-conformance anchors

Each property's expected behaviour is anchored to the v1.31.0 source cited in this
document so reviewers can confirm against the same ground truth: create-version uses
`service/worker/workerdeployment/client.go:1238` + `util.go updateWorkflow`; delete
no-ops use `client.go:1037` and `client.go:1089`; duplicate version mapping uses
`client.go:1296`; routing-config and scoped manager/conflict-token checks use
`service/worker/workerdeployment/workflow.go:1177`, `:775`, `:1244`, `:1109`, plus
`client.go:384`; routing update state derives from `client.go:1759`; max-version
oldest-eligible eviction is in `workflow.go:541-556,1485-1504`; task-queue-family
limits are in `version_workflow.go:625-642`; drainage grace/refresh timing is in
`version_workflow.go:1020-1052`; request-id defaulting and compute validation use
`service/frontend/workflow_handler.go:185/:258/:4078` and
`service/worker/workerdeployment/client.go:2037`; legacy version parsing uses
`common/worker_versioning/worker_versioning.go:1103`; effective-version precedence is
in `service/history/workflow/util.go`; transition start/complete is in
`service/history/workflow/mutable_state_impl.go` and `workflow_task_state_machine.go`
and task-start triggers in `service/history/api/recordworkflowtaskstarted` /
`recordactivitytaskstarted/api.go:188`; and deprecated-companion `UNIMPLEMENTED`
responses are in `service/frontend/workflow_handler.go` — all at tag `v1.31.0`.
Target-change notification and lineage are anchored to
`service/history/workflow/workflow_task_state_machine.go:495-532`; CaN inheritance and
declined-target propagation to
`service/history/workflow/mutable_state_impl.go:2485-2630,2658-2674`; retry carry to
`service/history/workflow/retry.go:253-338`; history field copying to
`service/history/historybuilder/event_factory.go:82-86,146`; and the default-enabled
policy to `common/dynamicconfig/constants.go:931-935` — all at tag `v1.31.0`.
