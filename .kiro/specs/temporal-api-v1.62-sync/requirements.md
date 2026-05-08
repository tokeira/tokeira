# Requirements Document: Temporal API v1.43 → v1.62.11 Sync

## Introduction

Tokeira currently vendors the Temporal API protos at `v1.43.0`, pinned in `tokeira/proto/UPSTREAM_VERSION`. The Rust SDK the workspace now targets — `temporalio-sdk v0.4.0`, pulled in by `apps/tokeira-bench` (see `apps/tokeira-bench/Cargo.toml`, `Cargo.lock` line 5387) — expects server features introduced between v1.43 and the latest Temporal API release, `v1.62.11` (2026-04-24). When pointed at a v1.43-era server surface, the v0.4 SDK's `SharedNamespaceWorker` shuts its worker stack down at startup because `NamespaceInfo.capabilities.worker_heartbeats` is absent, and its periodic 30 s heartbeat loop would hit `Unimplemented` on `RecordWorkerHeartbeat` if the stack kept running.

The previous commit `214895e` ("feat(bench, edge): add client-side SDK bench + interim v0.4 SDK compat shims") landed four tightly scoped shims to keep v0.4 workers alive against a v1.43-vendored `tokeirad`:

1. `proto/upstream/temporal/api/namespace/v1/message.proto` — a hand-backported `bool worker_heartbeats = 4;` on `NamespaceInfo.Capabilities`.
2. `proto/upstream/temporal/api/workflowservice/v1/service.proto` — a hand-backported `rpc RecordWorkerHeartbeat (RecordWorkerHeartbeatRequest) returns (RecordWorkerHeartbeatResponse)` with empty `Request`/`Response` types in `request_response.proto`. The request declares `repeated bytes worker_heartbeat = 3` to decode the SDK's real `temporal.api.worker.v1.WorkerHeartbeat` as opaque bytes without pulling in the full `temporal.api.worker.v1` package.
3. `crates/tokeira-edge/src/grpc/translate.rs` around line 865 — the `describe_namespace` translator advertises `worker_heartbeats: true` on `namespace_info::Capabilities`.
4. `crates/tokeira-edge/src/grpc/workflow_service.rs` around line 621 — a no-op `record_worker_heartbeat` handler returning `Ok(Response::new(RecordWorkerHeartbeatResponse {}))`.

These are explicitly interim. This spec owns their proper replacements by resyncing the vendored proto tree at `v1.62.11`, classifying every surface delta, and landing the translation-layer, domain-DTO, and runtime/kernel/projection changes needed so a v0.4-era SDK worker can connect to a post-sync `tokeirad` with no hand-authored backports in `proto/upstream/`.

This spec is deliberately not the place where large new features (Worker Deployments, real worker-heartbeat observability, Workflow Rules, Activity Executions as first-class objects, Pause/Unpause-workflow, Worker Config) get implemented. Those are enumerated as "Full implementation" items in the surface audit and deferred to their own specs. This spec's job is to absorb the full v1.43 → v1.62.11 wire-compatibility delta, implement a small set of low-complexity additions where deferring would cost more than absorbing (`CountSchedules`, `UpdateTaskQueueConfig`, Nexus v2 field wire-through), and leave a clear, classified, prioritised backlog of what remains to be built.

### What this spec additionally absorbs (beyond wire-compat)

Three v1.62 surfaces that could have been deferred but are absorbed into this spec because the implementation is small and deferring them would leave gaps a v0.4 SDK or Temporal UI would feel immediately:

- **`CountSchedules` RPC** — implemented against the existing `ScheduleStore` with namespace + optional filter query support (Requirement 4.6). Tokeira already has schedule machinery; counting is a trivial addition.
- **`UpdateTaskQueueConfig` RPC** — implemented as a setter against a new in-memory `TaskQueueConfigStore` (Requirement 4.7). Persistence to DSQL deferred; rate-limit enforcement deferred to admission-control.
- **Nexus v2 field wire-through on existing Nexus RPCs** — any v1.62-added fields on `PollNexusTaskQueueResponse`, `RespondNexusTaskCompletedRequest`, `RespondNexusTaskFailedRequest`, and `NexusEndpointSpec` are decoded, propagated through `NexusTaskBroker` and `NexusEndpointRegistry`, and re-emitted (Requirement 4.8). No new Nexus RPCs or kernel transitions.

### What this spec delivers

- A proto resync from `v1.43.0` to `v1.62.11` via the existing `tools/proto-sync` tool, with `proto/UPSTREAM_VERSION` updated and all hand-backported shims from commit `214895e` removed.
- A complete classified audit of every v1.43 → v1.62.11 proto addition (new RPC, new field, new message, new enum, new capability flag), with each item placed into one of five buckets: `Ignore`, `No-op stub`, `Capability advertise`, `Wire through`, or `Full implementation (deferred)`.
- Wire-through propagation of every field classified as `Wire through`: edge translator updates in `crates/tokeira-edge/src/grpc/translate.rs`, internal DTO additions in `crates/tokeira-edge/src/translate/mod.rs`, and downstream propagation into `tokeira-runtime`, `tokeira-kernel`, or `tokeira-projection` according to each field's classification.
- Replacement implementations of the four shims from commit `214895e`: the `worker_heartbeats` capability advertisement in `describe_namespace` keeps its semantics but sources from the upstream `namespace::v1::namespace_info::Capabilities` generated by the resync; `record_worker_heartbeat` keeps its no-op behaviour but accepts the real `RecordWorkerHeartbeatRequest` type with its `repeated temporal.api.worker.v1.WorkerHeartbeat worker_heartbeat` field; and the backports in `message.proto`/`service.proto`/`request_response.proto` are removed because the upstream re-export carries them natively.
- `Unimplemented` stub handlers for every new RPC classified as `Ignore` or `Full implementation (deferred)`, so the server speaks the full v1.62 gRPC surface without SDKs getting `NotFound`-style errors when calling unknown methods.
- Advertised v1.62 capability flags on `GetSystemInfoResponse.Capabilities` and `NamespaceInfo.Capabilities` so SDKs take the right code paths (e.g., `discard_speculative_workflow_task_with_events`, `server_scaled_deployments`, `worker_heartbeats`).
- A kernel and runtime impact assessment recording, for each wire-through item, whether the field drives a kernel transition change, a runtime state change, or only edge-level struct propagation.
- An integration test under `apps/tokeira-bench/` that exercises a v0.4-era SDK worker against a locally spawned `tokeirad` with the post-sync proto surface and no interim shims present.

### What this spec explicitly defers

- **Worker Deployments feature** — the full versioning and deployment-routing API surface (`DescribeWorker`, `ListWorkers`, `DescribeWorkerDeployment`, `DescribeWorkerDeploymentVersion`, `SetWorkerDeploymentCurrentVersion`, `SetWorkerDeploymentRampingVersion`, `DeleteWorkerDeployment`, `DeleteWorkerDeploymentVersion`, `ListWorkerDeployments`, `UpdateWorkerDeploymentVersionMetadata`, `SetWorkerDeploymentManager`, and the `temporal.api.deployment.v1.WorkerDeploymentOptions` / `WorkerDeploymentVersionInfo` / `VersionDrainageInfo` / `WorkerDeploymentInfo` / `WorkerDeploymentVersion` / `VersionMetadata` / `RoutingConfig` / `InheritedAutoUpgradeInfo` messages). Stubs only in this spec — full implementation is tracked in the separate `worker-deployments` spec (backlog P13).
- **Real worker-heartbeat observability** — persistent storage of `WorkerHeartbeat` records, kernel-observed worker liveness, metrics exposure, and the `ListWorkers` projection. This spec keeps `record_worker_heartbeat` as an accept-and-discard handler. Full implementation tracked in the separate `worker-heartbeat-observability` spec (backlog P6).
- **Schedule v1.62 behavioural revisions beyond `CountSchedules`** — message- and RPC-level compatibility is delivered by this spec at the wire-compat level. `CountSchedules` is implemented (Requirement 4.6). Any behavioural change in how schedules are evaluated, paused, or triggered relative to v1.43 semantics is out of scope. Full behavioural alignment remains a Temporal-compatibility concern tracked by the `temporal-compatibility` spec (backlog P1).
- **Workflow Rules feature** — `CreateWorkflowRule`, `DescribeWorkflowRule`, `DeleteWorkflowRule`, `ListWorkflowRules`, `TriggerWorkflowRule`, and the new `temporal.api.rules.v1` package. Stubs only — full implementation deferred to `workflow-rules` (backlog P14).
- **Activity executions as first-class objects** — `StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`, `ListActivityExecutions`, `CountActivityExecutions`, `RequestCancelActivityExecution`, `TerminateActivityExecution`, `DeleteActivityExecution`. Stubs only — full implementation deferred to `activity-executions-first-class` (backlog P11).
- **Worker-config management** — `FetchWorkerConfig`, `UpdateWorkerConfig`. Stubs only — deferred to `worker-config-management` (backlog P7).
- **Pause/unpause workflow executions as a first-class pause, distinct from the v1.43 activity-level pause-by-id surface** — `PauseWorkflowExecution`, `UnpauseWorkflowExecution`. Stubs only — deferred to `kernel-pause-workflow` (backlog P10).
- **Full Nexus dispatch behaviour changes** — this spec wire-throughs v1.62-added Nexus fields (Requirement 4.8) but does not alter Nexus dispatch, retry, or endpoint-resolution semantics.
- **`protometa` annotations** — informational only on the wire; no code change required beyond successful compile of the resynced tree.

### Cross-references

- `.kiro/specs/proto-upstream-sync/requirements.md` owns the `tools/proto-sync` tool, the `proto/UPSTREAM_VERSION` convention, and the `crates/tokeira-proto/build.rs` pipeline. This spec depends on that infrastructure being in place and does not modify it.
- `.kiro/specs/compose-dsql/requirements.md` depends on `tokeirad` accepting SDK traffic, which in turn depends on this spec's post-sync wire-compat work being complete.

## Glossary

- **Upstream_Version_File**: The `proto/UPSTREAM_VERSION` file at the workspace root. Currently contains `v1.43.0`; after this spec is applied, it contains `v1.62.11`.
- **Upstream_Proto_Tree**: The directory `proto/upstream/temporal/api/` at the workspace root containing vendored `.proto` files from the Temporal API. Re-exported by `tools/proto-sync` from `buf.build/temporalio/api:<version>`.
- **Proto_Sync_Tool**: The Rust workspace binary at `tokeira/tools/proto-sync/`, invoked as `cargo run -p proto-sync -- <version>`. Wipes `proto/upstream/` and re-exports via `buf export`, then writes the version string to `Upstream_Version_File`. Requires `buf` installed.
- **Commit_214895e**: The commit hash `214895e` ("feat(bench, edge): add client-side SDK bench + interim v0.4 SDK compat shims") where the interim shims this spec must replace were introduced. Referenced concretely rather than paraphrased because the shims are source-located by that commit.
- **Interim_Shims**: The four hand-authored compatibility additions introduced by Commit_214895e: the `worker_heartbeats` field backport in `proto/upstream/temporal/api/namespace/v1/message.proto`, the `RecordWorkerHeartbeat` RPC and empty `Request`/`Response` messages in `proto/upstream/temporal/api/workflowservice/v1/service.proto` and `request_response.proto`, the `worker_heartbeats: true` literal in `crates/tokeira-edge/src/grpc/translate.rs`, and the `record_worker_heartbeat` no-op handler in `crates/tokeira-edge/src/grpc/workflow_service.rs`.
- **Edge_Translate**: The module at `crates/tokeira-edge/src/grpc/translate.rs` containing per-RPC translator functions between generated proto types and edge DTOs.
- **Edge_DTOs**: The wire-agnostic DTOs declared in `crates/tokeira-edge/src/translate/mod.rs`, including `SystemCapabilities`, `SystemInfo`, `NamespaceDescription`, and the per-RPC request/response structs.
- **Workflow_Service_Impl**: The gRPC service implementation at `crates/tokeira-edge/src/grpc/workflow_service.rs` containing ~40 `async fn` RPC handlers.
- **Operator_Service_Impl**: The gRPC service implementation at `crates/tokeira-edge/src/grpc/operator_service.rs`.
- **Proto_Build_Script**: `crates/tokeira-proto/build.rs`. Globs `proto/upstream/` and `proto/tokeira/` and runs `tonic_build` with `btree_map(["."])`. Emits generated code to `OUT_DIR`.
- **Kernel**: The `tokeira-kernel` crate — pure deterministic state machine. See `crates/tokeira-kernel/` and Rule 2 of `tokeira/AGENTS.md`.
- **Runtime**: The `tokeira-runtime` crate — owns lanes, brokers, schedulers. See `crates/tokeira-runtime/`.
- **Projection**: The `tokeira-projection` crate — owns visibility and projection workers. See `crates/tokeira-projection/`.
- **SDK_v04**: `temporalio-sdk v0.4.0` and its transitive crates (`temporalio-client`, `temporalio-common`, `temporalio-macros`, `temporalio-sdk-core`). This is the SDK generation `apps/tokeira-bench` targets.
- **Bench_Binaries**: The `bench-worker` and `bench-starter` executables declared in `apps/tokeira-bench/Cargo.toml`. The bench runs `EchoWorkflow` (a zero-activity echo) over a full gRPC → edge → runtime → kernel → storage → projection round-trip.
- **Surface_Audit**: The enumerated table this spec produces, listing every v1.43 → v1.62.11 proto addition, change, or renaming with a bucket classification. Required by Feature 2.
- **Classification_Ignore**: The `Ignore` bucket in Surface_Audit — new RPC `tokeirad` does not serve; handler returns `Unimplemented`.
- **Classification_NoOp**: The `No-op stub` bucket in Surface_Audit — new RPC SDKs call at startup or periodically and that must return `Ok` to keep workers healthy, but carries no runtime state change.
- **Classification_Capability**: The `Capability advertise` bucket in Surface_Audit — new boolean field on a capability message that must be set to `true` (or explicitly `false` with documented intent) so SDKs take the right code path.
- **Classification_WireThrough**: The `Wire through` bucket in Surface_Audit — new field on an existing request or response message that `tokeirad` must decode and propagate into runtime, kernel, or projection.
- **Classification_Deferred**: The `Full implementation (deferred)` bucket in Surface_Audit — feature large enough to deserve its own dedicated spec; this spec stubs the wire surface only.
- **Capability_Flag**: A boolean field on `GetSystemInfoResponse.Capabilities` or `NamespaceInfo.Capabilities`. SDKs read these at startup and branch behaviour.
- **v0.4_Liveness_Invariant**: The property that an `SDK_v04` worker connecting to a post-sync `tokeirad` must (a) not shut down its worker stack at startup because of a missing capability flag, (b) not receive `Unimplemented` on any RPC it calls during normal polling, heartbeat, or task completion loops, and (c) successfully start a workflow, receive a workflow task, complete it, and receive the workflow completion.

## Requirements

---

## Feature 1: Proto Resync to v1.62.11

### Requirement 1.1: Run the proto-sync tool against v1.62.11

**User Story:** As a Tokeira developer, I want the vendored Temporal API proto tree refreshed from `v1.43.0` to `v1.62.11` via the existing `Proto_Sync_Tool`, so that the generated Rust bindings carry the full upstream v1.62 surface without hand-authored backports.

#### Acceptance Criteria

1. WHEN the implementation task for this requirement runs, THE Proto_Sync_Tool SHALL be invoked as `cargo run -p proto-sync -- v1.62.11` from the workspace root.
2. WHEN the Proto_Sync_Tool completes successfully, THE Upstream_Version_File SHALL contain exactly the string `v1.62.11` followed by a trailing newline.
3. WHEN the Proto_Sync_Tool completes successfully, THE Upstream_Proto_Tree SHALL contain all `temporal/api/*/v1/*.proto` files from the `buf.build/temporalio/api:v1.62.11` module, including the new `temporal/api/worker/v1/message.proto`, `temporal/api/rules/v1/message.proto`, and `temporal/api/protometa/v1/annotations.proto` packages that did not exist in the v1.43 vendor.
4. WHEN the Proto_Sync_Tool completes successfully, THE Upstream_Proto_Tree SHALL NOT contain any files, fields, RPCs, or messages beyond those exported by `buf.build/temporalio/api:v1.62.11` — specifically, any hand-authored backports introduced by Commit_214895e SHALL be absent after the resync.
5. THE implementation SHALL NOT modify the `Proto_Sync_Tool` source under `tokeira/tools/proto-sync/` — the tool is owned by the `proto-upstream-sync` spec and is consumed unchanged here.
6. IF `buf` is not installed on the implementer's PATH, THEN the Proto_Sync_Tool invocation SHALL fail with the descriptive error message emitted by `Command::new("buf")` failure (see `tokeira/tools/proto-sync/src/main.rs` line 73, `"failed to invoke buf; ensure buf is installed and on PATH"`), and the implementer SHALL install `buf` (e.g. `brew install bufbuild/buf/buf` on macOS) before retrying.

### Requirement 1.2: Proto build script produces the full v1.62.11 surface

**User Story:** As a Tokeira developer, I want `cargo build --workspace` to compile the resynced proto tree successfully and emit the full v1.62 generated code surface to `OUT_DIR`, so that downstream crates can reference `temporal::api::worker::v1`, `temporal::api::rules::v1`, `temporal::api::protometa::v1`, and every v1.62-added message without manual module declarations.

#### Acceptance Criteria

1. WHEN `cargo build --workspace` runs after the resync, THE Proto_Build_Script SHALL compile every `.proto` file in the Upstream_Proto_Tree via `tonic_build` without errors.
2. WHEN the build completes, THE generated Rust module at `OUT_DIR` for `crates/tokeira-proto/` SHALL contain the `temporal::api::worker::v1` module with the `WorkerHeartbeat`, `WorkerPollerInfo`, `WorkerSlotsInfo`, `WorkerHostInfo`, `WorkerInfo`, `WorkerListInfo`, `PluginInfo`, and `StorageDriverInfo` message types.
3. WHEN the build completes, THE generated `temporal::api::deployment::v1` module SHALL contain the `WorkerDeploymentOptions`, `WorkerDeploymentVersionInfo`, `VersionDrainageInfo`, `WorkerDeploymentInfo`, `WorkerDeploymentVersion`, `VersionMetadata`, `RoutingConfig`, and `InheritedAutoUpgradeInfo` message types.
4. WHEN the build completes, THE generated `temporal::api::workflowservice::v1` module SHALL contain client and server stubs for every RPC declared in the v1.62.11 `service.proto`, including the 30 RPCs added between v1.43 and v1.62.11 enumerated in Feature 2.
5. WHEN the build completes, THE `tokeira_proto::public` module SHALL expose all v1.62 packages under the `temporal::api` hierarchy per the existing `public.rs` re-export pattern established by the `proto-upstream-sync` spec, with no hand-authored module declarations for v1.62-introduced packages.

### Requirement 1.3: Workspace compile remains green after resync

**User Story:** As a Tokeira developer, I want the full workspace to compile cleanly after the resync even before downstream translator and handler updates land, so that the resync is a reviewable atomic step separable from the wire-through work.

#### Acceptance Criteria

1. WHEN `cargo build --workspace` runs immediately after the resync but before any edge translator or workflow service handler updates, THE build SHALL succeed — any compilation errors caused by signature drift on types the edge already references (e.g. renamed fields, altered enum variants, moved message types) SHALL be resolved within this feature's implementation, not deferred to Feature 3.
2. WHEN `cargo clippy --workspace --all-targets` runs after the resync, THE command SHALL exit with status zero — new-but-unused generated types from v1.62 SHALL NOT trigger `dead_code` warnings because `#[allow(dead_code)]` is already applied crate-wide by the `tonic_build` generator, and any warnings surfaced by the resync SHALL be resolved in this feature.
3. WHEN `cargo test --workspace` runs after the resync, THE existing test suite SHALL pass — test drift caused by signature changes on types the test suite references SHALL be resolved within this feature.

---

## Feature 2: Surface Audit — v1.43 → v1.62.11

### Requirement 2.1: Enumerate every RPC added between v1.43 and v1.62.11

**User Story:** As a Tokeira developer, I want a complete enumeration of every RPC added to `WorkflowService` and `OperatorService` between v1.43 and v1.62.11, each classified into one of the five buckets, so that implementation work is explicit and nothing is accidentally left as `Unimplemented` when it must be `No-op stub` or `Capability advertise`.

#### Acceptance Criteria

1. THE Surface_Audit SHALL enumerate every RPC declared in `proto/upstream/temporal/api/workflowservice/v1/service.proto` and `proto/upstream/temporal/api/operatorservice/v1/service.proto` in the v1.62.11 vendor that does not appear in the v1.43 vendor.
2. THE Surface_Audit SHALL classify each such RPC into exactly one of the five buckets: Classification_Ignore, Classification_NoOp, Classification_Capability, Classification_WireThrough, or Classification_Deferred.
3. THE Surface_Audit SHALL include at minimum the following RPCs, each with its classification fixed by this spec:
   - `RecordWorkerHeartbeat` — Classification_NoOp (accept and discard; real observability deferred to the `worker-heartbeat-observability` spec).
   - `CountSchedules` — Classification_WireThrough (implemented against Tokeira's existing `ScheduleStore`; see Requirement 4.6).
   - `UpdateTaskQueueConfig` — Classification_WireThrough (implemented as setter-only against a new in-memory `TaskQueueConfigStore`; see Requirement 4.7). Persistence to DSQL is deferred.
   - Every new Nexus-v2 field on existing Nexus RPCs — Classification_WireThrough (wire-through decoding and re-emission only; see Requirement 4.8). No new Nexus RPCs are declared in v1.62.11, so this row applies to field additions on existing surfaces.
   - `DescribeWorker`, `ListWorkers`, `DescribeWorkerDeployment`, `DescribeWorkerDeploymentVersion`, `SetWorkerDeploymentCurrentVersion`, `SetWorkerDeploymentRampingVersion`, `DeleteWorkerDeployment`, `DeleteWorkerDeploymentVersion`, `ListWorkerDeployments`, `UpdateWorkerDeploymentVersionMetadata`, `SetWorkerDeploymentManager` — all Classification_Deferred (Worker Deployments feature → `worker-deployments` spec).
   - `CreateWorkflowRule`, `DescribeWorkflowRule`, `DeleteWorkflowRule`, `ListWorkflowRules`, `TriggerWorkflowRule` — all Classification_Deferred (Workflow Rules feature → `workflow-rules` spec).
   - `StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`, `ListActivityExecutions`, `CountActivityExecutions`, `RequestCancelActivityExecution`, `TerminateActivityExecution`, `DeleteActivityExecution` — all Classification_Deferred (Activity executions feature → `activity-executions-first-class` spec).
   - `FetchWorkerConfig`, `UpdateWorkerConfig` — Classification_Deferred (→ `worker-config-management` spec).
   - `PauseWorkflowExecution`, `UnpauseWorkflowExecution` — Classification_Deferred (→ `kernel-pause-workflow` spec).
   - `UpdateActivityOptions`, `PauseActivity`, `UnpauseActivity`, `ResetActivity` (renamed from `UpdateActivityOptionsById`, `PauseActivityById`, `UnpauseActivityById`, `ResetActivityById`) — Classification_WireThrough (rename-only; existing handlers migrate to the new names with identical semantics).
4. FOR every RPC the Surface_Audit classifies as Classification_Deferred or Classification_Ignore, THE Workflow_Service_Impl SHALL include an explicit handler that returns `Status::unimplemented(...)` with a human-readable reason including the RPC name. The implementation SHALL NOT rely on the default `tonic` behaviour of returning `Unimplemented` because the `WorkflowService` trait on the v1.62 generated stubs requires every method to have a concrete implementation.
5. FOR every RPC the Surface_Audit classifies as Classification_NoOp, THE Workflow_Service_Impl SHALL include a handler that returns the corresponding empty response message with `Ok(_)` and emits a `debug!` log line named after the RPC.
6. WHERE an RPC renaming preserves behaviour (e.g. `UpdateActivityOptionsById` → `UpdateActivityOptions`), THE existing handler logic SHALL be migrated under the new name and the old name SHALL NOT be re-declared — the v1.43 RPC names no longer exist in the v1.62 generated code.

### Requirement 2.2: Enumerate every new field on an existing message

**User Story:** As a Tokeira developer, I want a complete enumeration of every new field added between v1.43 and v1.62.11 on a message that already existed in v1.43, so that `tokeirad` decodes and preserves those fields rather than silently dropping them on translation.

#### Acceptance Criteria

1. THE Surface_Audit SHALL enumerate every field added to an existing v1.43 message in the v1.62.11 vendor.
2. THE Surface_Audit SHALL classify each such field into Classification_Capability, Classification_WireThrough, or Classification_Deferred.
3. THE Surface_Audit SHALL include at minimum the following fields, each with its classification fixed by this spec:
   - `NamespaceInfo.Capabilities.worker_heartbeats` — Classification_Capability (advertise `true`; replaces the Commit_214895e backport).
   - `NamespaceInfo.Capabilities.reported_problems_search_attribute` — Classification_Capability (advertise `false` unless Tokeira gains a reported-problems search attribute; explicitly documented intent).
   - `GetSystemInfoResponse.Capabilities.server_scaled_deployments` (field 12) — Classification_Capability (advertise `false`; Worker Deployments not yet served).
   - `RespondWorkflowTaskCompletedRequest.Capabilities.discard_speculative_workflow_task_with_events` — Classification_Capability (advertise `true`; speculative-task handling does not need to fabricate a history side-effect, see `request_response.proto` line 387–394 of the v1.62 vendor).
   - Any field added to `PollWorkflowTaskQueueResponse`, `PollActivityTaskQueueResponse`, `RespondWorkflowTaskCompletedRequest`, `RespondActivityTaskCompletedRequest`, `RespondActivityTaskFailedRequest`, `RespondActivityTaskCanceledRequest`, `RecordActivityTaskHeartbeatRequest`, or `StartWorkflowExecutionRequest` between v1.43 and v1.62.11 — each classified individually in the audit; Classification_WireThrough by default unless explicitly noted otherwise.
4. FOR every field classified as Classification_Capability, THE corresponding translator in Edge_Translate SHALL emit the advertised value on every response that carries that capability message.
5. FOR every field classified as Classification_WireThrough, THE corresponding edge-to-internal and internal-to-edge translator functions SHALL decode and propagate the field, adding new fields to the relevant Edge_DTO in `crates/tokeira-edge/src/translate/mod.rs` and updating its callers in Workflow_Service_Impl and downstream (Runtime, Kernel, or Projection, per Feature 4).
6. FOR every field classified as Classification_Deferred, THE translator SHALL explicitly drop the field on the request path and emit the protobuf default on the response path. THE DTO SHALL NOT carry the deferred field (neither as a typed field nor as opaque bytes), and a comment at the DTO definition site SHALL name every neighbouring Classification_Deferred field together with the spec that owns its implementation. THIS explicit-drop contract is the edge-layer equivalent of "the deferring spec has not yet decided how to carry or consume this field" — preserving bytes would force every DTO to grow a generic opaque-field bag, which this spec rejects as gratuitous surface.

### Requirement 2.3: Surface audit artifact location

**User Story:** As a reviewer, I want the surface audit to live in a reviewable artifact inside the spec directory, so that the classification decisions and their rationale are visible alongside the requirements rather than buried in the implementation PR's commit messages.

#### Acceptance Criteria

1. THE design document for this spec (`.kiro/specs/temporal-api-v1.62-sync/design.md`) SHALL contain the complete Surface_Audit table as a markdown table with columns: `Kind` (RPC, Field, Message, Enum, Package), `Qualified Name`, `Added In` (e.g. `v1.48`, `v1.55`), `Classification`, `Disposition` (concrete action — e.g. `unimplemented handler`, `advertise true`, `wire to SystemCapabilities.server_scaled_deployments`, `stub; deferred to worker-deployments spec`), and `Target Spec`. THE `Target Spec` cell SHALL be populated for every Classification_Deferred row (the deferring spec is mandatory — see Req 5.1.3 escalation) and MAY be populated for non-deferred rows as a forward pointer to a follow-up spec that extends the current in-scope implementation (e.g. `NamespaceInfo.Capabilities.worker_heartbeats` is a Capability row that advertises `true` in this spec and names `worker-heartbeat-observability` as the spec that later lands real observability). Non-deferred rows without a meaningful follow-up SHALL use `—` in the `Target Spec` cell. Structural checks that treat the column as deferred-only-ownership MUST restrict themselves to rows with `Classification == Deferred`.
2. THE Surface_Audit table SHALL be complete in the sense that every RPC, field, message, enum, and package change enumerable from `diff -r proto/upstream/ (v1.43) proto/upstream/ (v1.62.11)` has exactly one row in the table.
3. WHEN a reviewer inspects the Surface_Audit, THE count of rows classified as Classification_WireThrough SHALL match the count of translator updates delivered by Feature 3 — classifications and implementation work are in 1:1 correspondence.

---

## Feature 3: Replace the Interim Shims from Commit 214895e

### Requirement 3.1: Remove the hand-backported `worker_heartbeats` field

**User Story:** As a Tokeira developer, I want the hand-backported `worker_heartbeats` field in `proto/upstream/temporal/api/namespace/v1/message.proto` removed, so that the vendored proto tree contains only what `buf export` emits for v1.62.11 with no hand-authored deltas.

#### Acceptance Criteria

1. WHEN Feature 1's Proto_Sync_Tool invocation completes, THE file `proto/upstream/temporal/api/namespace/v1/message.proto` SHALL be the unmodified v1.62.11 vendor output — the hand-authored `worker_heartbeats` field backport introduced by Commit_214895e SHALL NOT be present as a manual edit.
2. THE resynced `namespace_info::Capabilities` struct SHALL carry `worker_heartbeats: bool` as field 4 because upstream v1.62.11 declares it there; this is distinct from the Commit_214895e backport in that the field now comes from `buf export` rather than a hand-authored edit.
3. THE grep pattern `"Tokeirad currently accepts heartbeats as a no-op"` or `"A production implementation is tracked in a follow-up spec"` SHALL return no matches in `proto/upstream/` after the resync, because those commented rationale lines were added by Commit_214895e and are not part of the upstream vendor.

### Requirement 3.2: Remove the hand-backported `RecordWorkerHeartbeat` RPC and messages

**User Story:** As a Tokeira developer, I want the hand-backported `RecordWorkerHeartbeat` RPC and its empty `Request`/`Response` messages removed from `service.proto` and `request_response.proto`, so that the RPC and message definitions come from the v1.62.11 vendor and carry the real `temporal.api.worker.v1.WorkerHeartbeat` submessage on the request.

#### Acceptance Criteria

1. WHEN Feature 1's Proto_Sync_Tool invocation completes, THE file `proto/upstream/temporal/api/workflowservice/v1/service.proto` SHALL be the unmodified v1.62.11 vendor output. The hand-authored `rpc RecordWorkerHeartbeat` declaration introduced by Commit_214895e (with its backport rationale comment, see grep pattern `"RecordWorkerHeartbeat is used by v0.4+ SDK workers"` around line 1023–1029) SHALL NOT be present as a manual edit.
2. WHEN Feature 1's Proto_Sync_Tool invocation completes, THE file `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` SHALL be the unmodified v1.62.11 vendor output. The hand-authored `RecordWorkerHeartbeatRequest` with `repeated bytes worker_heartbeat = 3` SHALL be replaced by the upstream message, which declares `repeated temporal.api.worker.v1.WorkerHeartbeat worker_heartbeat` on the real field number.
3. WHEN the build runs, THE generated `RecordWorkerHeartbeatRequest` type SHALL expose `worker_heartbeat: Vec<temporal::api::worker::v1::WorkerHeartbeat>` rather than `Vec<Vec<u8>>`, because the upstream import of `temporal/api/worker/v1/message.proto` is now present in the vendored tree.

### Requirement 3.3: Migrate the `describe_namespace` capability advertisement

**User Story:** As a Tokeira developer, I want the `worker_heartbeats: true` literal in the `describe_namespace` translator in `crates/tokeira-edge/src/grpc/translate.rs` to survive the resync, so that the v0.4_Liveness_Invariant still holds without depending on any hand-backported proto edits.

#### Acceptance Criteria

1. THE file `crates/tokeira-edge/src/grpc/translate.rs` SHALL continue to advertise `worker_heartbeats: true` on `namespace_proto::namespace_info::Capabilities` in the `namespace_to_proto` function.
2. WHERE the `namespace_proto::namespace_info::Capabilities` struct layout changed due to the resync (e.g. new fields added upstream), THE implementation SHALL set any newly-present Classification_Capability fields (notably `reported_problems_search_attribute`) to their documented values per Requirement 2.2 rather than relying on `..Default::default()` to silently default-initialise them.
3. THE rationale comment currently anchored at `crates/tokeira-edge/src/grpc/translate.rs:865` SHALL be updated to reference this spec (`temporal-api-v1.62-sync`) rather than a generic "follow-up spec", and SHALL name the follow-up spec that owns real worker-heartbeat observability by its placeholder name.

### Requirement 3.4: Update the `record_worker_heartbeat` handler to accept the real request type

**User Story:** As a Tokeira developer, I want the no-op `record_worker_heartbeat` handler in `crates/tokeira-edge/src/grpc/workflow_service.rs` to accept the real `RecordWorkerHeartbeatRequest` with its `Vec<WorkerHeartbeat>` payload, so that the handler compiles against the post-resync generated code without requiring the Commit_214895e backport.

#### Acceptance Criteria

1. THE handler `record_worker_heartbeat` in Workflow_Service_Impl SHALL accept `Request<workflowservice::RecordWorkerHeartbeatRequest>` where `RecordWorkerHeartbeatRequest` is the upstream-generated type containing `worker_heartbeat: Vec<temporal::api::worker::v1::WorkerHeartbeat>`.
2. THE handler SHALL return `Ok(Response::new(workflowservice::RecordWorkerHeartbeatResponse {}))` with no side effects on Kernel, Runtime, Storage, or Projection.
3. THE handler SHALL emit a `tracing::debug!` log at most once per call with the RPC name; heartbeats SHALL NOT be logged at `info` level or above because a v0.4 worker emits one every 30 s per registered worker and higher log levels would flood operator logs.
4. THE rationale comment on the handler SHALL be updated to reference this spec by name (`temporal-api-v1.62-sync`) and SHALL point at the same follow-up spec referenced in Requirement 3.3 for real observability.
5. THE `namespace` field on `RecordWorkerHeartbeatRequest` SHALL be read and validated as non-empty; IF the request arrives with an empty namespace, THEN the handler SHALL return `Status::invalid_argument("namespace is required")`, matching the validation convention established by neighbouring handlers such as `shutdown_worker` (see `crates/tokeira-edge/src/grpc/workflow_service.rs` lines 636–640).

### Requirement 3.5: Leave `describe_namespace` Interim_Shims as the only behavioural remnant

**User Story:** As a reviewer, I want to confirm that after this feature lands, the only behavioural remnants of Commit_214895e are (a) the `worker_heartbeats: true` advertisement (now properly motivated by v1.62 upstream) and (b) the no-op `record_worker_heartbeat` handler — every other shim is dissolved into the upstream re-export.

#### Acceptance Criteria

1. WHEN `git diff 214895e^..HEAD -- proto/upstream/` runs on the post-sync tree, THE output SHALL show only the net effect of the resync — no hand-authored hunks introduced by Commit_214895e SHALL persist.
2. WHEN a reviewer inspects `crates/tokeira-edge/src/grpc/translate.rs` and `crates/tokeira-edge/src/grpc/workflow_service.rs` after this feature lands, THE two behavioural remnants enumerated above (capability advertisement and no-op handler) SHALL be the only references to worker heartbeats in the edge layer — no new worker-heartbeat storage, projection, or kernel code SHALL be introduced by this spec.

---

## Feature 4: Translation Layer and Internal DTO Updates

### Requirement 4.1: Propagate new Classification_Capability fields through the edge

**User Story:** As an SDK integrator, I want every v1.62-added capability flag on `GetSystemInfoResponse.Capabilities` and `NamespaceInfo.Capabilities` to be advertised with an explicit value so that the SDK takes the right code path, and I want Tokeira's internal SystemCapabilities DTO to carry the same surface so the advertisement is wire-driven rather than hardcoded at the translator boundary.

#### Acceptance Criteria

1. THE `SystemCapabilities` struct in `crates/tokeira-edge/src/translate/mod.rs` (lines 244–256) SHALL gain a `server_scaled_deployments: bool` field with a default value of `false`.
2. THE `SystemCapabilities` struct SHALL gain a `worker_heartbeats: bool` field with a default value of `true` — the advertised value matches the `describe_namespace` capability advertisement so both GetSystemInfo and DescribeNamespace agree on SDK-facing behaviour.
3. THE `system_info_to_proto` function in `crates/tokeira-edge/src/grpc/translate.rs` (around lines 825–848) SHALL map every field of `SystemCapabilities` into the corresponding field of `workflowservice::get_system_info_response::Capabilities` — specifically including the v1.62-added `server_scaled_deployments` flag, and any other Classification_Capability fields identified in the Surface_Audit.
4. WHERE the `NamespaceInfo.Capabilities` message gained new fields in v1.62 beyond `worker_heartbeats` (notably `reported_problems_search_attribute`), THE `namespace_to_proto` function SHALL emit those fields with explicit documented values per Requirement 2.2.
5. THE `SystemInfo` construction in `crates/tokeira-edge/src/workflow_service.rs` around lines 2283–2288 SHALL populate the new capability flags consistent with the defaults declared in Requirement 4.1.1 and 4.1.2.

### Requirement 4.2: Handle the `discard_speculative_workflow_task_with_events` client capability

**User Story:** As a Tokeira developer, I want the edge to decode the `RespondWorkflowTaskCompletedRequest.Capabilities.discard_speculative_workflow_task_with_events` client-sent capability so that speculative workflow task handling can branch on it when real speculative-task support lands, without today's lack of support silently dropping the client's advertised behaviour.

#### Acceptance Criteria

1. THE `RespondWorkflowTaskCompletedRequest` DTO in `crates/tokeira-edge/src/translate/mod.rs` SHALL gain a `client_discards_speculative_with_events: bool` field.
2. THE `to_internal::respond_workflow_task_completed_request_from_proto` translator SHALL decode `RespondWorkflowTaskCompletedRequest.capabilities.discard_speculative_workflow_task_with_events` into `RespondWorkflowTaskCompletedRequest.client_discards_speculative_with_events` in the Edge_DTO.
3. WHERE the kernel or runtime today does not yet emit speculative workflow tasks as distinct-from-regular tasks, THE decoded capability SHALL be stored on the Edge_DTO but SHALL NOT be propagated deeper than Workflow_Service_Impl — a comment on the DTO field SHALL state that the read will become meaningful when speculative workflow task support lands (tracked in a future speculative-wft spec).
4. THE translator SHALL default the field to `false` if the client did not send the capability message at all, matching the protobuf default-semantics for optional scalar fields on a nested message.

### Requirement 4.3: Rename v1.43 `*ById` activity RPCs to their v1.62 unsuffixed names

**User Story:** As a Tokeira developer, I want the activity-management RPCs renamed from `UpdateActivityOptionsById`, `PauseActivityById`, `UnpauseActivityById`, `ResetActivityById` to `UpdateActivityOptions`, `PauseActivity`, `UnpauseActivity`, `ResetActivity` (matching the v1.62 upstream), with behaviour preserved, so that existing kernel/runtime pause/reset logic continues to work under the new names.

#### Acceptance Criteria

1. WHEN the Proto_Sync_Tool completes, THE v1.43 RPC names `UpdateActivityOptionsById`, `PauseActivityById`, `UnpauseActivityById`, `ResetActivityById` SHALL NOT be present in the generated `workflowservice::workflow_service_server::WorkflowService` trait.
2. THE Workflow_Service_Impl handlers SHALL be renamed from `update_activity_options_by_id` / `pause_activity_by_id` / `unpause_activity_by_id` / `reset_activity_by_id` to `update_activity_options` / `pause_activity` / `unpause_activity` / `reset_activity`, with method bodies unchanged modulo the renaming and any message-field signature drift the resync introduces.
3. WHERE the v1.62 `PauseActivityRequest` / `UnpauseActivityRequest` / `ResetActivityRequest` / `UpdateActivityOptionsRequest` messages gained new fields relative to their `*ById` predecessors, THE Surface_Audit SHALL list those fields and this requirement SHALL be amended to enumerate their classifications; a blanket "preserve behaviour" migration SHALL NOT proceed if any new field is classified Classification_WireThrough.
4. THE internal Edge_DTO names for these operations (e.g. `PauseActivityRequest` DTO) SHALL lose any `ById` suffixes they carry today, matching the v1.62 upstream naming convention.

### Requirement 4.4: Preserve wire-compat for `DescribeNamespaceResponse` additions

**User Story:** As an SDK integrator, I want `DescribeNamespaceResponse` fields added between v1.43 and v1.62.11 (e.g. new `NamespaceConfig` fields) to be emitted with documented defaults rather than silently defaulted, so that clients do not observe behavioural drift they cannot distinguish from pre-v1.43 defaults.

#### Acceptance Criteria

1. THE `namespace_to_proto` function SHALL populate every v1.62-introduced field on `DescribeNamespaceResponse`, `NamespaceInfo`, `NamespaceConfig`, and `NamespaceReplicationConfig` with a documented default value per the Surface_Audit's classification.
2. WHERE a new field on `NamespaceConfig` reflects a Tokeira policy that does not yet exist (e.g. a new archival-related field), THE translator SHALL emit the protobuf default and the Surface_Audit's row for that field SHALL be classified Classification_Deferred with a pointer to the future spec that implements it.
3. THE internal `NamespaceDescription` DTO SHALL gain fields for any v1.62 addition classified Classification_WireThrough; additions classified Classification_Deferred SHALL NOT be mirrored on the DTO.

### Requirement 4.5: Round-trip property for updated translators

**User Story:** As a Tokeira developer, I want every translator touched by Feature 4 to preserve round-trip wire fidelity — bytes in, DTO, bytes out — so that no newly-added field is silently dropped or re-synthesized with a different value on the return path.

#### Acceptance Criteria

1. FOR every translator function touched by Feature 4 (minimum: `system_info_to_proto`, `namespace_to_proto`, `respond_workflow_task_completed_request_from_proto`, `update_activity_options_*`, `pause_activity_*`, `unpause_activity_*`, `reset_activity_*`), THE test suite SHALL include a round-trip property: decoding a proto message into the Edge_DTO and re-encoding it SHALL produce an encoded message byte-equivalent to the input for all fields the translator is responsible for preserving.
2. WHERE a translator intentionally does not preserve a field byte-for-byte (e.g. a field classified Classification_Deferred that is stored on the DTO but not re-emitted because the server side regenerates it), THE property test SHALL compare the subset of fields the translator owns and SHALL explicitly exclude the non-preserved fields with a comment naming the Surface_Audit row that justifies the exclusion.
3. THE property tests SHALL live in `crates/tokeira-edge/src/translate/` submodule test modules and SHALL use `proptest` strategies over the relevant Edge_DTO types, consistent with the property-testing convention in `tokeira/AGENTS.md` line 79.

### Requirement 4.6: Implement `CountSchedules` over the existing ScheduleStore

**User Story:** As an SDK integrator, I want `CountSchedules` to return the real count of schedules matching the request's namespace and optional query string, because the operation is small enough to implement against Tokeira's existing `ScheduleStore` and deferring it forces SDKs down a slower `ListSchedules`-and-count path.

#### Acceptance Criteria

1. THE `CountSchedules` handler in Workflow_Service_Impl SHALL be implemented (not stubbed). It SHALL accept `Request<workflowservice::CountSchedulesRequest>` and return `Response<workflowservice::CountSchedulesResponse>` containing the count of schedules matching `namespace` and the optional `query` field.
2. THE implementation SHALL delegate to a new method on the `ScheduleStore` trait in `tokeira-runtime` (see `crates/tokeira-runtime/` for the existing trait) with the signature `count_schedules(&self, namespace: &NamespaceId, query: Option<&str>) -> Result<u64, ScheduleCountError>`. THE `ScheduleCountError` type SHALL be a concrete `thiserror`-derived enum (minimum variant: `UnsupportedQuery`) defined alongside the method; `anyhow::Error` at the store boundary is explicitly rejected so the edge layer can `match` on failure modes without downcasting. THE method SHALL be implemented on the existing in-memory `ScheduleStore::default()` backing.
3. WHERE the optional `query` field is empty, THE handler SHALL return the count of all schedules in the namespace. WHERE `query` is a non-empty filter expression, THE handler SHALL apply the filter using the existing filter-compilation primitives in `crates/tokeira-projection/src/filter.rs`. IF the filter syntax is unsupported or malformed, THEN the handler SHALL return `Status::invalid_argument("unsupported schedule query")`.
4. IF the namespace does not exist, THEN the handler SHALL return `Status::not_found("namespace not found")` rather than `Ok(0)`, matching the error-semantics convention of neighbouring handlers such as `DescribeNamespace`.
5. THE Edge_DTO for `CountSchedulesRequest` and `CountSchedulesResponse` SHALL live in `crates/tokeira-edge/src/translate/mod.rs` and follow the naming and structure of the existing `CountWorkflowExecutionsRequest` / `Response` DTOs, which are re-exported from `tokeira-projection`.
6. Behavioural alignment with upstream Temporal's `CountSchedules` semantics beyond what this requirement specifies (e.g. exact filter-grammar coverage, hit-count-vs-unique semantics) is out of scope for this spec and remains a Temporal-compatibility concern tracked by the `temporal-compatibility` spec (see backlog P1).

### Requirement 4.7: Implement `UpdateTaskQueueConfig` as setter-only task-queue config

**User Story:** As an operator using Temporal's UI-managed task queue configuration, I want `UpdateTaskQueueConfig` to persist a task-queue-level config (rate limits, retention, description) and make it observable on subsequent `DescribeTaskQueue` calls, because the operation is a simple setter against existing per-task-queue state and deferring it would leave the UI with a half-working management page.

#### Acceptance Criteria

1. THE `UpdateTaskQueueConfig` handler in Workflow_Service_Impl SHALL be implemented (not stubbed). It SHALL accept `Request<workflowservice::UpdateTaskQueueConfigRequest>` and return `Response<workflowservice::UpdateTaskQueueConfigResponse>`.
2. THE implementation SHALL persist the updated config into a new `TaskQueueConfigStore` trait in `tokeira-runtime`, with a default in-memory backing (matching the pattern of the existing `ScheduleStore` and `VersioningRuleStore`). The store SHALL be keyed by `(NamespaceId, TaskQueueName)` and SHALL carry the config fields defined in `workflowservice::UpdateTaskQueueConfigRequest` (rate-limit override, description, custom task-queue tier hint — whichever the v1.62 proto surfaces). No DSQL migration is delivered by this spec; persistence to DSQL is deferred to whichever spec lands DSQL-backed versioning and task-queue state next.
3. THE `DescribeTaskQueue` handler SHALL be updated to read from the `TaskQueueConfigStore` and populate the corresponding config fields on `DescribeTaskQueueResponse` if the proto carries them.
4. IF the namespace or task queue name is empty, THEN the handler SHALL return `Status::invalid_argument(...)` matching the error-semantics convention of existing namespace-and-task-queue-validating handlers.
5. THE change SHALL NOT alter task-queue admission, polling, or dispatch behaviour in this spec — rate-limit-override enforcement (e.g. admission-control interaction) is out of scope and remains the responsibility of a future admission-control spec.

### Requirement 4.8: Wire-through Nexus v2 additions on existing Nexus surfaces

**User Story:** As a Tokeira developer, I want any v1.62-added fields on existing Nexus RPCs (`PollNexusTaskQueueResponse`, `RespondNexusTaskCompletedRequest`, `RespondNexusTaskFailedRequest`, `NexusEndpointSpec`) to be wire-through decoded and re-emitted on the existing Nexus translation path, so that a v0.4 SDK worker interacting with Nexus operations does not observe Tokeira silently dropping fields its counterparts expect.

#### Acceptance Criteria

1. THE Surface_Audit SHALL enumerate every field added to a Nexus-related message between v1.43 and v1.62.11 and classify each as Classification_WireThrough or Classification_Capability.
2. FOR every field classified Classification_WireThrough on a Nexus-related message, THE corresponding translator function in Edge_Translate (minimum: the translators for `PollNexusTaskQueueResponse`, `RespondNexusTaskCompletedRequest`, `RespondNexusTaskFailedRequest`, and the `NexusEndpointSpec` family) SHALL decode and propagate the field, adding the field to the relevant Edge_DTO and updating `NexusTaskBroker` and `NexusEndpointRegistry` consumers (see `tokeirad/src/main.rs:125–133` for the registry construction) to the extent the field semantics demand.
3. WHERE a new Nexus field affects endpoint classification or routing (e.g. a new `NexusEndpointSpec.endpoint_type` variant), THE `NexusEndpointRegistry::resolve` method SHALL be updated to handle the new variant, returning an appropriate error for a new variant that tokeirad does not yet route.
4. THE implementation SHALL NOT introduce new Nexus features (new RPCs, new kernel transitions for nexus). Only field-level wire-through and registry/broker propagation of already-understood-shape fields are in scope.
5. Behavioural changes to Nexus dispatch or retry beyond wire-through propagation are out of scope. IF a Nexus field's semantics cannot be expressed by simple propagation, THEN its row SHALL be escalated to Classification_Deferred and surfaced for a future Nexus-focused spec.

---

## Feature 5: Kernel, Runtime, and Projection Impact Assessment

### Requirement 5.1: Per-field impact classification matrix

**User Story:** As a Tokeira developer, I want every Classification_WireThrough field to be accompanied by an explicit record of whether it drives a kernel transition change, a runtime state change, a projection change, or only edge-level struct propagation, so that implementation scope and review expectations are clear per field.

#### Acceptance Criteria

1. THE design document for this spec SHALL contain an "Impact Matrix" table with one row per Classification_WireThrough field from the Surface_Audit, and columns: `Field Qualified Name`, `Edge DTO Change`, `Kernel Impact` (one of `none`, `new transition variant`, `existing transition field`), `Runtime Impact` (one of `none`, `new broker state`, `existing broker state`, `new timer behaviour`), `Projection Impact` (one of `none`, `new visibility column`, `existing visibility column`, `new search attribute`), and `Implementation Notes`.
2. WHERE a Classification_WireThrough field is recorded with `none` in all of Kernel Impact, Runtime Impact, and Projection Impact columns, THE implementation SHALL treat that field as edge-only propagation: the Edge_DTO gains the field, the translator decodes it, Workflow_Service_Impl consumes it at the edge boundary, and no further plumbing is required.
3. WHERE a Classification_WireThrough field is recorded with a non-`none` Kernel Impact, THE implementation SHALL NOT land the field in this spec — instead, the field's row SHALL be escalated to Classification_Deferred with a pointer to the kernel-facing spec that owns its semantics. This spec SHALL NOT introduce new kernel transition variants.
4. WHERE a Classification_WireThrough field is recorded with a non-`none` Runtime Impact, THE implementation SHALL land the field in this spec only if the runtime change fits within a single-file modification in `crates/tokeira-runtime/`. IF the runtime change spans more than one file or introduces new runtime state types, THEN the field's row SHALL be escalated to Classification_Deferred with a pointer to a runtime-facing spec.
5. WHERE a Classification_WireThrough field is recorded with a non-`none` Projection Impact, THE implementation SHALL land the field in this spec only if the projection change is a pure additive column or search attribute. IF the projection change requires a migration file against the visibility store, THEN the field's row SHALL be escalated to Classification_Deferred with a pointer to a projection-facing spec.

### Requirement 5.2: Kernel purity is preserved

**User Story:** As a Tokeira developer, I want the kernel to remain pure per Rule 2 of `tokeira/AGENTS.md`, so that this spec's work does not leak I/O, async, storage, metrics, or network concerns into `tokeira-kernel`.

#### Acceptance Criteria

1. WHEN the implementation lands, THE `crates/tokeira-kernel/` crate SHALL have no new `use` statements on `tokio`, `async_trait`, `tonic`, `prost`, or any crate classified as async or I/O-bearing by the existing kernel Cargo.toml.
2. WHEN the implementation lands, THE `crates/tokeira-kernel/Cargo.toml` dependency section SHALL NOT gain any new dependency introduced by this spec — kernel additions (if any) SHALL be pure data-structure changes.
3. IF any Classification_WireThrough field escalates to Classification_Deferred per Requirement 5.1.3 because it would require a kernel transition change, THEN the Surface_Audit SHALL be amended to move the field into Classification_Deferred and the design document SHALL record the target kernel-facing spec name.

---

## Feature 6: Stub Handlers for Deferred and Ignored RPCs

### Requirement 6.1: Exhaustive `unimplemented` coverage for Classification_Deferred RPCs

**User Story:** As a Tokeira developer, I want every Classification_Deferred RPC to have an explicit handler in Workflow_Service_Impl and Operator_Service_Impl that returns `Status::unimplemented(...)` with a human-readable message naming the feature area and the target spec, so that the server speaks the full v1.62 gRPC surface and operators receive clear error messages rather than generic `Unimplemented`.

#### Acceptance Criteria

1. FOR every Classification_Deferred RPC enumerated in Requirement 2.1.3, THE Workflow_Service_Impl SHALL declare a handler method that returns `Err(Status::unimplemented(format!("{} is not implemented; tracked in spec {}", <rpc name>, <spec name>)))`.
2. THE `<spec name>` argument SHALL match the placeholder spec names declared in this spec's "What this spec explicitly defers" section — specifically `worker-deployments`, `workflow-rules`, `activity-executions-first-class`, `worker-config-management`, `kernel-pause-workflow`, `worker-heartbeat-observability` (for the full observability side of `RecordWorkerHeartbeat`), or similar placeholder names for deferred features.
3. THE handler SHALL emit a single `tracing::debug!` log line per call naming the RPC and the deferring spec, consistent with existing handler logging conventions.
4. THE handler SHALL NOT emit `tracing::warn!` or higher because SDKs may call these RPCs opportunistically during feature detection and elevated log levels would create operator noise.

### Requirement 6.2: Worker-deployments RPCs stub placement

**User Story:** As a Tokeira developer, I want the 11 Worker Deployments RPCs stubbed together in a clearly-labelled block within Workflow_Service_Impl, so that a future `worker-deployments` spec can find and replace them atomically.

#### Acceptance Criteria

1. THE Workflow_Service_Impl handlers for `DescribeWorker`, `ListWorkers`, `DescribeWorkerDeployment`, `DescribeWorkerDeploymentVersion`, `SetWorkerDeploymentCurrentVersion`, `SetWorkerDeploymentRampingVersion`, `DeleteWorkerDeployment`, `DeleteWorkerDeploymentVersion`, `ListWorkerDeployments`, `UpdateWorkerDeploymentVersionMetadata`, and `SetWorkerDeploymentManager` SHALL appear as a contiguous block in `crates/tokeira-edge/src/grpc/workflow_service.rs`, bracketed by a leading comment `// === Worker Deployments — deferred to worker-deployments spec ===` and a trailing comment `// === End Worker Deployments block ===`.
2. EACH handler in the block SHALL follow the template from Requirement 6.1.
3. IF a future Tokeira spec implements one of these RPCs, THEN that spec SHALL remove the corresponding line from the block and SHALL NOT leave the stub behind.

### Requirement 6.3: Workflow-rules and activity-executions stub placement

**User Story:** As a Tokeira developer, I want the Workflow Rules and Activity Executions RPCs stubbed in their own clearly-labelled blocks, following the same convention as Worker Deployments.

#### Acceptance Criteria

1. THE Workflow_Service_Impl handlers for `CreateWorkflowRule`, `DescribeWorkflowRule`, `DeleteWorkflowRule`, `ListWorkflowRules`, `TriggerWorkflowRule` SHALL appear as a contiguous block bracketed by `// === Workflow Rules — deferred to workflow-rules spec ===` and `// === End Workflow Rules block ===`.
2. THE Workflow_Service_Impl handlers for `StartActivityExecution`, `DescribeActivityExecution`, `PollActivityExecution`, `ListActivityExecutions`, `CountActivityExecutions`, `RequestCancelActivityExecution`, `TerminateActivityExecution`, `DeleteActivityExecution` SHALL appear as a contiguous block bracketed by `// === Activity Executions — deferred to activity-executions-first-class spec ===` and `// === End Activity Executions block ===`.
3. EACH handler in each block SHALL follow the template from Requirement 6.1.

### Requirement 6.4: Preserve existing `Unimplemented` handlers for v1.43-era unhandled RPCs

**User Story:** As a Tokeira developer, I want v1.43-era RPCs that were already stubbed `Unimplemented` before this spec to remain stubbed, so that this spec does not accidentally expand or contract the set of live RPCs beyond the v1.62 deltas.

#### Acceptance Criteria

1. THE set of RPCs with a non-`Unimplemented` handler in Workflow_Service_Impl after this spec lands SHALL equal (the set present before the spec landed) ∪ (Classification_NoOp and Classification_Capability RPCs introduced by v1.62) − (RPCs renamed with behaviour preserved per Requirement 4.3).
2. ANY RPC that returned `Status::unimplemented(...)` before this spec SHALL continue to return `Status::unimplemented(...)` after this spec unless this spec explicitly classifies it into Classification_NoOp, Classification_Capability, or Classification_WireThrough.

---

## Feature 7: v0.4 SDK Integration Test

### Requirement 7.1: End-to-end bench run proves v0.4_Liveness_Invariant

**User Story:** As a Tokeira maintainer, I want a test that spawns a local `tokeirad` and runs `Bench_Binaries` against it, proving that a v0.4-era SDK worker connects, initialises, polls, heartbeats, starts workflows, and receives completions — all without relying on the Interim_Shims.

#### Acceptance Criteria

1. THE test SHALL live at `apps/tokeira-bench/tests/v0_4_integration.rs` and SHALL be gated behind a `cargo test --package tokeira-bench --test v0_4_integration` invocation — it SHALL NOT run under a plain `cargo test --workspace` by default because it spawns a `tokeirad` process and is therefore closer to an integration test than a unit test. The gating SHALL be via a `#[ignore]` attribute with a rationale comment naming this spec, so the test is opt-in via `--include-ignored`.
2. THE test SHALL spawn a `tokeirad` instance in the same process using the in-memory storage backend (`--storage in-memory`), binding it to an ephemeral port. The test SHALL NOT require Docker Compose, AWS credentials, or an external network.
3. THE test SHALL instantiate a `temporalio-client::Client` v0.4 pointed at the ephemeral `tokeirad` and SHALL invoke `GetSystemInfo` and `DescribeNamespace` on the default namespace, asserting that `capabilities.worker_heartbeats == true` on the `DescribeNamespace` response.
4. THE test SHALL register `EchoWorkflow` (the existing bench workflow exposed via `apps/tokeira-bench/src/lib.rs`) on a `temporalio-sdk::Worker` v0.4, start the worker, and wait for the worker's SharedNamespaceWorker to complete startup without shutting down — the worker SHALL remain alive until at least one observed `record_worker_heartbeat` call from the SDK worker reaches `tokeirad`. Observing at least one heartbeat proves the SDK-to-server heartbeat path works end-to-end; steady-state / multi-heartbeat observability belongs to the `worker-heartbeat-observability` spec. The 90-second upper bound in Req 7.1.7 gives the 30-second SDK heartbeat interval two chances to fire before the test times out.
5. THE test SHALL start at least one `EchoWorkflow` execution, assert the workflow completes successfully, and assert the returned payload matches the input — exercising the full gRPC → edge → runtime → kernel → storage → projection → response path.
6. THE test SHALL assert that `tokeirad`'s log output during the test contains at least one `record_worker_heartbeat` debug line, confirming the v0.4 SDK actually called the heartbeat RPC during the test window — this serves as a regression guard against future changes that accidentally strip the heartbeat handler.
7. THE test SHALL complete in under 120 seconds on a developer laptop, using `tokio::sync::Notify` or channel-based synchronisation per Rule 1 of `tokeira/AGENTS.md` rather than fixed sleeps.

### Requirement 7.2: Bench invariance under full resync

**User Story:** As a Tokeira maintainer, I want the existing `Bench_Binaries` (`bench-worker`, `bench-starter`) to continue running against a post-sync `tokeirad` with no source changes, so that the resync is transparent to the bench harness.

#### Acceptance Criteria

1. THE `apps/tokeira-bench/src/bin/bench_worker.rs` and `apps/tokeira-bench/src/bin/bench_starter.rs` sources SHALL NOT require any source changes to compile or run against the post-sync `tokeirad`.
2. WHERE `apps/tokeira-bench` gains a new integration test per Requirement 7.1, THAT test SHALL be the only source change under `apps/tokeira-bench/` introduced by this spec.
3. IF the v0.4 SDK signatures referenced by `bench_worker.rs` or `bench_starter.rs` drift due to SDK version pinning changes that accompany this spec, THEN the corresponding Cargo.toml version pin SHALL be updated to a version compatible with v1.62.11 server — the pin update SHALL be a minimal-diff change and SHALL NOT introduce other SDK behavioural changes.

---

## Feature 8: Documentation and Rationale

### Requirement 8.1: Record the classification rationale in the spec directory

**User Story:** As a future Tokeira contributor, I want to understand why each v1.62 surface addition was classified as `Ignore`, `No-op`, `Capability`, `Wire-through`, or `Deferred`, so that later decisions to promote a Deferred item to a full implementation have a documented starting point.

#### Acceptance Criteria

1. THE design document (`.kiro/specs/temporal-api-v1.62-sync/design.md`) SHALL include a "Classification Rationale" section with one paragraph per classification bucket summarising the principle that governed placement into that bucket — minimally: `Ignore` covers RPCs SDKs never call in normal operation, `No-op` covers RPCs SDKs call for liveness but whose behaviour is unobservable to workflow correctness, `Capability` covers boolean feature-detection flags, `Wire-through` covers fields carrying operator-observable or workflow-observable data, `Deferred` covers features whose implementation spans more than one crate or requires migration-level changes.
2. THE design document SHALL cross-reference each deferred-spec placeholder name (e.g. `worker-deployments`) with a short description of the feature scope, so that a future author opening that spec directory knows immediately which surface to address.

### Requirement 8.2: Update `proto/UPSTREAM_VERSION` commentary

**User Story:** As an operator reading the workspace, I want the upstream version pin visibly at the top of the repo, so that the synced version is part of the repository's contract.

#### Acceptance Criteria

1. THE `proto/UPSTREAM_VERSION` file SHALL contain `v1.62.11\n` after the resync lands.
2. THE workspace `README.md` or `CONTRIBUTING.md` SHALL be updated to state the supported Temporal API version range (at minimum: "Tokeira tracks Temporal API v1.62.11"), if such a statement is not already present. IF the workspace already contains such a statement pinned to v1.43, THEN it SHALL be updated to v1.62.11.
3. THE statement in 8.2.2 SHALL name the supported SDK generation (v0.4) alongside the API version, so that the contract between server and client is explicit.
