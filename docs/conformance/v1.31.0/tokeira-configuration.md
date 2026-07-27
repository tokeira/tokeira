# Tokeira configuration and feature availability

> This is the canonical operator-facing configuration reference for the Temporal v1.31.0 compatibility profile. The Feature Catalog and strict typed configuration schema generate it; hand-maintained test outcomes do not define feature availability.

## Empty Configuration guarantee

An empty TOML document is valid. It selects Tokeira's documented safe defaults: Temporal priority bands remain active, User Fairness is disabled, Standalone Activities are disabled, authentication/authorization is a stock-compatible no-op until an identity source is configured, and no emergency restriction is active. Production accepts typed Tokeira fields only—never raw Temporal dynamic configuration keys.

## Operational warnings

- **JWT issuer routing is exact.** Each configured `policy.authorization.jwt.issuers[].issuer` must exactly match the signed token's `iss` value; a friendly provider name is not a substitute.
- **Nexus callbacks require routability.** `policy.nexus_completion.system_callback_url` must be reachable from Nexus workers. The loopback default is suitable only when workers are co-located.
- **Priority and User Fairness are distinct.** Omitting `[policy.task_queues]` preserves five priority bands and default key 3 while leaving weighted User Fairness disabled. Enable it with `[policy.task_queues] enable_fairness = true`.
- **Conformance overrides are not production configuration.** A conformance build may receive selected Temporal keys from the test bridge; stock production builds expose no such input path.

## Feature catalog

| Feature | State | Conformance | Temporal maturity | Temporal default | Empty Configuration | Enablement | Scope / mutability |
|---|---|---|---|---|---|---|---|
| `activity-executions` — Activity execution management | experimental | in-surface | public-preview | disabled | disabled | toml: `policy.compatibility.enable_standalone_activities = true` | namespace / startup-static |
| `activity-management` — Workflow-scoped activity management | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `activity-task-lifecycle` — Activity task lifecycle | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `authorization` — Authentication, authorization, and principal attribution | implemented | in-surface | general-availability | disabled | disabled | toml: `policy.authorization` | cluster / startup-static |
| `aws-iam-bearer-authorization` — AWS IAM bearer authorization | implemented | not-applicable | not-applicable | not-applicable | disabled | toml: `policy.authorization.aws_iam` | cluster / startup-static |
| `batch-operations` — Batch operations | experimental | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `cluster-info` — Cluster and system metadata | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `compatibility-metadata` — Tokeira compatibility metadata service | implemented | not-applicable | not-applicable | not-applicable | enabled | none | cluster / immutable |
| `deployment-v0` — Deployment v0 (deprecated) | unsupported | out-of-surface | deprecated | conditional | unavailable | unavailable | not-applicable / not-applicable |
| `eager-workflow-start` — Eager workflow start | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `http-json-api` — Temporal HTTP/JSON API gateway | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `legacy-visibility` — Legacy visibility | unsupported | out-of-surface | deprecated | conditional | unavailable | unavailable | not-applicable / not-applicable |
| `multi-operation` — Multi-operation execution | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `namespace-management` — Namespace management | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `nexus-admin` — Nexus endpoint administration | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `nexus-operation-executions` — Nexus operation executions | unsupported | out-of-surface | absent | not-applicable | unavailable | unavailable | not-applicable / not-applicable |
| `nexus-task-transport` — Nexus task transport | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `remote-cluster` — Remote cluster administration | unsupported | in-surface | general-availability | enabled | unavailable | unavailable | not-applicable / not-applicable |
| `reported-problems-search-attribute` — Workflow task reported problems | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `schedules` — Schedules | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `search-attributes` — Search attributes | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `task-queue-management` — Task queue management, priority, and fairness | implemented | in-surface | general-availability | enabled | enabled | public-api: `WorkflowService.UpdateTaskQueueConfig` | task-queue / durable-live-api |
| `user-fairness` — Task queue User Fairness | implemented | in-surface | general-availability | disabled | disabled | toml: `policy.task_queues.enable_fairness = true` | cluster, task-queue / startup-static |
| `visibility` — Workflow visibility | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `worker-config` — Worker configuration | unsupported | in-surface | general-availability | enabled | unavailable | unavailable | not-applicable / not-applicable |
| `worker-deployments` — Worker deployments | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `worker-deployments-pre-release` — Worker deployments pre-release additions | experimental | out-of-surface | experimental | conditional | enabled | none | not-applicable / immutable |
| `worker-heartbeats` — Worker heartbeats and live inventory | implemented | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `worker-versioning-v1-v2` — Worker versioning v1/v2 (deprecated) | implemented | in-surface | deprecated | disabled | disabled | unavailable | cluster / immutable |
| `workflow-cancel-terminate` — Workflow cancel and terminate | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-history` — Workflow history reads | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-pause` — Workflow pause | unsupported | out-of-surface | experimental | conditional | unavailable | unavailable | not-applicable / not-applicable |
| `workflow-query` — Workflow query | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-reset` — Workflow reset | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-rules` — Workflow rules | partial | in-surface | general-availability | disabled | unavailable | conformance-only: `frontend.workflowRulesAPIsEnabled` | namespace / conformance-only |
| `workflow-signal` — Workflow signal | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-start` — Workflow start | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-task-lifecycle` — Workflow task lifecycle | partial | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |
| `workflow-update` — Workflow updates | experimental | in-surface | general-availability | enabled | enabled | none | not-applicable / immutable |

## Feature details

### Activity execution management (`activity-executions`)

Standalone (CHASM) activity execution — the first CHASM component. A v1.31.0 feature gated per-namespace by `activity.enableStandalone` (default off); disabled it answers UNIMPLEMENTED (`chasm/lib/activity/frontend.go:36 @ v1.31.0`), enabled it is served, so default conformance is preserved. Tokeira's enable is a server-start config (`policy.compatibility.enable_standalone_activities`), server-uniform and not runtime-injectable: the functional harness's dynamic-config override path is unsupported, so SA functional tests run under the server's start-time setting.

- Guidance: Set [policy.compatibility].enable_standalone_activities = true and restart tokeirad.
- Prerequisites: none
- Evidence: manual-review `chasm-foundation spec; ground-truthed to chasm/lib/activity/{frontend.go,statemachine.go,config.go} @ v1.31.0`

### Workflow-scoped activity management (`activity-management`)

UpdateActivityOptions, PauseActivity, UnpauseActivity, and ResetActivity implement the served v1.31.0 workflow-scoped activity lifecycle, including id/type/all targeting, retry-policy and restore-original option updates, reset/pause heartbeat flags, and paused-retry parking. Their API comments announce a future deprecation, but v1.31.0 has no replacement RPCs and keeps them in surface.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestActivityApiResetClientTestSuite @ v1.31.0: 6 pass / 0 fail (repeated fresh-process runs)`; test `Temporal functional corpus TestActivityAPIUpdateClientTestSuite @ v1.31.0: 5 pass / 0 fail (2 consecutive fresh-process runs)`; test `Temporal functional corpus TestActivityApiBatchUpdateOptionsClientTestSuite @ v1.31.0: 3 pass / 0 fail (2 consecutive fresh-process runs)`

### Activity task lifecycle (`activity-task-lifecycle`)

Activity polling, heartbeats, and terminal responses exist, but strict Temporal conformance remains partial until SDK matrix coverage is complete.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Authentication, authorization, and principal attribution (`authorization`)

Presence-enabled JWT authentication, authorization, namespace/task-queue access classification, and durable principal attribution match the configured v1.31.0 behavior.

- Guidance: Configure [policy.authorization] with at least one identity source and grant, then restart tokeirad.
- Prerequisites: `Configured JWT issuer or AWS IAM verifier`
- Evidence: test `Temporal functional corpus TestAuthorizationTestSuite @ v1.31.0: Tier 7.36`

### AWS IAM bearer authorization (`aws-iam-bearer-authorization`)

Tokeira-native AWS IAM bearer verification composes with the same typed grant and authorization model and is outside the Temporal compatibility claim.

- Guidance: Configure [policy.authorization.aws_iam] grants together with authorization, then restart tokeirad.
- Prerequisites: `AWS identity verification and configured authorization grants`
- Evidence: manual-review `.kiro/specs/authorization-foundation`

### Batch operations (`batch-operations`)

Batch APIs are visible but remain an experimental operator surface pending compatibility evidence.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Cluster and system metadata (`cluster-info`)

Cluster metadata and GetSystemInfo responses preserve the existing SDK-visible baseline while the matrix records conservative conformance state.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `apps/tokeirad/tests/grpc_roundtrip.rs`

### Tokeira compatibility metadata service (`compatibility-metadata`)

Tokeira's separate compatibility service publishes build pins, feature ownership, SDK evidence, and stable digests without altering Temporal services.

- Guidance: Enabled as a Tokeira metadata extension; clients may ignore the separate service.
- Prerequisites: none
- Evidence: test `crates/tokeira-compatibility-service/src/lib.rs::compatibility_response_contains_static_matrices`

### Deployment v0 (deprecated) (`deployment-v0`)

Temporal v1.31.0 deprecates these five deployment-v0 RPCs in favor of GA Worker Deployments; Tokeira does not expose their enabled behavior.

- Guidance: Excluded because Temporal v1.31.0 marks this surface deprecated and provides a GA replacement.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Eager workflow start (`eager-workflow-start`)

StartWorkflowExecution atomically commits and returns the first WFT when eager execution is requested and no first-WFT backoff applies. Fresh and immediate request-id retry responses derive from authoritative started-task state; the v1.31.0 enabled default is pinned as a constant rather than an operator knob.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestEagerWorkflowTestSuite @ v1.31.0: 5 pass / 0 fail / 1 classified skip (3 consecutive runs)`; test `crates/tokeira-edge/src/workflow_service.rs::eager_start_does_not_require_registered_poller`; test `crates/tokeira-storage/src/dsql/codec.rs::legacy_workflow_started_fixture_decodes_and_v2_round_trips`

### Temporal HTTP/JSON API gateway (`http-json-api`)

WorkflowService and OperatorService google.api.http annotations are discovered from the pinned descriptor set and transcoded on the existing listener into the ordinary Tonic service stack. Host/header policy, protobuf JSON, Temporal payload shorthand, v2/v3 OpenAPI documents, gRPC status translation, and admitted-request metrics match v1.31.0 without adding workflow semantics or another internal service.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestHttpApiTestSuite @ v1.31.0: 11 pass / 0 fail / 0 skip (2 consecutive fresh-process runs)`; test `crates/tokeira-edge/src/http_api: Properties 1-7 and 9-11; apps/tokeirad/src/http_api_transport.rs: Property 8 and layer integration tests`

### Legacy visibility (`legacy-visibility`)

The deprecated ScanWorkflowExecutions RPC is excluded; the still-served ListOpen, ListClosed, and ListArchived RPCs belong to the ordinary visibility feature.

- Guidance: Excluded because Temporal v1.31.0 marks this surface deprecated and provides a GA replacement.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Multi-operation execution (`multi-operation`)

ExecuteMultiOperation implements Update-with-Start ([Start, Update] only, per v1.31.0): atomic fresh-start admission via Command::StartAndUpdate, attach/dedup/already-completed paths, and the structured MultiOperationExecutionFailure error with the Aborted sibling.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `crates/tokeira-edge/src/grpc/translate.rs::multi_operation_shape_gate_rejects_non_start_update_pairs`; test `crates/tokeira-edge/src/grpc/translate.rs::multi_operation_start_conflict_keeps_already_exists_and_typed_detail`; test `crates/tokeira-edge/src/grpc/translate.rs::multi_operation_response_serializes_ordered_start_update_pair`

### Namespace management (`namespace-management`)

Namespace APIs exist but remain partial because upstream namespace semantics are broader than the current implementation.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Nexus endpoint administration (`nexus-admin`)

Nexus endpoint CRUD, optimistic update, pagination, validation, and namespace-safe callback routing implement the v1.31.0 GA operator surface.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus Nexus endpoint admin coverage: Tier 7.35`

### Nexus operation executions (`nexus-operation-executions`)

These eight RPCs exist only in vendored API v1.62.11 and are absent from the v1.31.0 server's API v1.62.8.

- Guidance: Absent from Temporal v1.31.0/API v1.62.8 and outside this compatibility claim.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Nexus task transport (`nexus-task-transport`)

The three v1.31.0 Nexus worker transport RPCs and workflow operation lifecycle are implemented; the eight newer operation-execution RPCs are classified separately.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestNexusWorkflowTestSuite and TestNexusApiTestSuite @ v1.31.0: Tiers 7.37-7.38`

### Remote cluster administration (`remote-cluster`)

Multi-cluster administration is outside the current deployment model.

- Guidance: Unavailable in Tokeira; no production enablement mechanism exists.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Workflow task reported problems (`reported-problems-search-attribute`)

Describe derives the v1.31.0 TemporalReportedProblems KeywordList from kernel-state consecutive-problem accounting (failures and start-to-close timeouts, sticky-suppressed and cleared on WFT success, per failWorkflowTask @ v1.31.0) at the pinned default threshold of five; the last non-transient problem supplies the Failed or TimedOut category pair. The accumulator is durable with the run's hot state. Visibility-index projection of the attribute (v1.31.0 upserts it for ListWorkflowExecutions) remains open — the attribute currently surfaces on Describe only.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `crates/tokeira-runtime/src/runtime/mod.rs::reported_problem_appears_at_default_threshold_and_carries_latest_cause`; test `apps/tokeirad/src/lib.rs::reported_problem_search_attribute_has_exact_v131_keyword_list`

### Schedules (`schedules`)

The public v1.31.0 schedule behavior is conformance-tested; the native schedule store remains process-local, so restart durability is still open.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestScheduleV1 @ v1.31.0: Tier 5.30`

### Search attributes (`search-attributes`)

Search-attribute administration interacts with visibility projection and remains experimental.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Task queue management, priority, and fairness (`task-queue-management`)

Priority-aware workflow, activity, child, sticky, and durable-backlog handout follows the v1.31.0 stock defaults. Optional User Fairness, auto-enable, queue/per-key rate shaping, atomic kind-isolated task-queue config updates, and real per-priority statistics are implemented in Tokeira's delivery runtime without matching/history service objects. Public task-queue policy commits through a dedicated CAS repository, survives process replacement, and is hydrated before traffic without becoming workflow history or kernel state.

- Guidance: Use UpdateTaskQueueConfig for queue/per-key rates and fairness-weight overrides; priority delivery needs no activation.
- Prerequisites: none
- Evidence: test `crates/tokeira-edge/tests/grpc_new_endpoints.rs::priority_orders_workflow_polls_and_projects_real_band_stats_via_grpc`; test `crates/tokeira-runtime/src/task_ordering.rs property tests`; test `Temporal functional corpus TestPrioritySuite, TestFairnessSuite, and TestFairnessAutoEnableSuite @ v1.31.0`

### Task queue User Fairness (`user-fairness`)

Weighted within-priority handout is disabled by default, preserves metadata while disabled, excludes sticky queues, and composes queue overrides over task-carried weights.

- Guidance: Set [policy.task_queues].enable_fairness = true and restart tokeirad; use UpdateTaskQueueConfig for per-key weights and rates.
- Prerequisites: `Priority-aware delivery (enabled by default)`
- Evidence: test `crates/tokeira-edge/tests/grpc_new_endpoints.rs::priority_orders_workflow_polls_and_projects_real_band_stats_via_grpc`; test `crates/tokeira-runtime/src/task_ordering.rs property tests`; test `Temporal functional corpus TestPrioritySuite, TestFairnessSuite, and TestFairnessAutoEnableSuite @ v1.31.0`

### Workflow visibility (`visibility`)

Visibility list/count/describe APIs are backed by projection, but strict Temporal query compatibility remains partial.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Worker configuration (`worker-config`)

FetchWorkerConfig and UpdateWorkerConfig remain unsupported; live worker inventory belongs to worker-heartbeats.

- Guidance: Unavailable in Tokeira; no production enablement mechanism exists.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Worker deployments (`worker-deployments`)

The nine GA Worker Deployment RPCs are implemented, including version membership, routing, drainage, limits, metadata, and manager/current/ramping transitions. Deprecated and pre-release companions are cataloged separately.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `crates/tokeira-edge/src/grpc/workflow_service.rs::worker_deployment_handlers_are_no_longer_deferred`; test `crates/tokeira-edge/src/grpc/workflow_service.rs::deployment_handlers_return_unimplemented_messages`; test `crates/tokeira-runtime/src/runtime/activity.rs::activity_deployment_transition_lifecycle`

### Worker deployments pre-release additions (`worker-deployments-pre-release`)

Temporal v1.31.0 labels these four Worker Deployment additions pre-release. Tokeira implements them through the same registry but excludes them from the compatibility claim.

- Guidance: Implemented as an experimental Tokeira surface but excluded from the v1.31.0 compatibility claim.
- Prerequisites: none
- Evidence: test `crates/tokeira-edge/src/grpc/workflow_service.rs::worker_deployment_handlers_are_no_longer_deferred`; test `crates/tokeira-edge/src/grpc/workflow_service.rs::deployment_handlers_return_unimplemented_messages`; test `crates/tokeira-runtime/src/runtime/activity.rs::activity_deployment_transition_lifecycle`

### Worker heartbeats and live inventory (`worker-heartbeats`)

RecordWorkerHeartbeat, shutdown removal, Nexus-piggyback admission, and lossless DescribeWorker/ListWorkers inventory reads match Temporal v1.31.0's volatile registry behavior.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `crates/tokeira-edge/src/grpc/workflow_service.rs::worker_inventory_round_trips_complete_heartbeats`; test `crates/tokeira-edge/src/worker_inventory.rs::pagination_is_ordered_and_duplicate_free`

### Worker versioning v1/v2 (deprecated) (`worker-versioning-v1-v2`)

Conformant as stock-default rejections: a default-config Temporal v1.31.0 server refuses all five deprecated RPCs with PERMISSION_DENIED (the versioning gates default off), and tokeira reproduces those exact errors. The enabled-path semantics are out of surface; the owning decision record is .kiro/specs/worker-deployments/reference/v1-v2-conformance-decision.md.

- Guidance: Only v1.31.0 stock-default rejection behavior is supported; the deprecated enabled path is excluded.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Workflow cancel and terminate (`workflow-cancel-terminate`)

DeleteWorkflowExecution is proven against the v1.31.0 functional corpus with authoritative state/history purge and monotonic visibility tombstones. The group remains Partial until cancel and terminate independently have broader failure-mode conformance evidence.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestWorkflowDeleteExecutionSuite @ v1.31.0: 3 pass / 0 fail (2 consecutive runs)`; test `crates/tokeira-storage/src/memory.rs::authoritative workflow deletion Property 5`; test `crates/tokeira-projection/src/visibility_sink.rs::visibility tombstone monotonicity Property 11`; test `crates/tokeira-runtime/src/runtime/lifecycle.rs::running_workflow_deletion_terminates_then_purges`

### Workflow history reads (`workflow-history`)

History reads are core SDK surfaces, but the audit classifies them as partial until field-level completeness is proven.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `apps/tokeirad/tests/grpc_roundtrip.rs`

### Workflow pause (`workflow-pause`)

Pause/unpause workflow APIs are upstream surfaces without current compatibility support.

- Guidance: Excluded because Temporal v1.31.0 labels this surface experimental.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Workflow query (`workflow-query`)

Query APIs are present but remain partial until ordering, consistency, and SDK behavior are covered.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Workflow reset (`workflow-reset`)

Reset has an edge surface but remains partial until full reset semantics are verified.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Workflow rules (`workflow-rules`)

The default-off v1.31.0 gate, CRUD surface, target-conformant TriggerWorkflowRule rejection, ActivityType equality predicate, and ActivityPause evaluation at initial and retry dispatch are implemented. The registry is process-local and automatic evaluation does not yet implement the complete visibility/activity predicate language, so restart durability and the broader predicate surface remain open.

- Guidance: The configured enabled path is currently available only to the conformance harness; production exposes no activation setting.
- Prerequisites: none
- Evidence: test `Temporal functional corpus TestActivityApiRulesClientTestSuite @ v1.31.0: 5 pass / 0 fail (2 consecutive fresh-process runs)`

### Workflow signal (`workflow-signal`)

Signal delivery is part of the core workflow surface, but the audit classifies the broader compatibility state as partial.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

### Workflow start (`workflow-start`)

Start and signal-with-start are accepted as core surfaces, but the server compatibility claim remains conservative.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `apps/tokeirad/tests/grpc_roundtrip.rs`

### Workflow task lifecycle (`workflow-task-lifecycle`)

Workflow task polling and completion are core SDK paths. The current matrix shape records sdk_metadata as the primary capability; upsert_memo is preserved by the baseline until multi-capability matrix entries are introduced.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: test `apps/tokeirad/tests/grpc_roundtrip.rs`

### Workflow updates (`workflow-update`)

Workflow updates are visible but remain experimental until protocol-level SDK conformance evidence is added.

- Guidance: Enabled with an empty production configuration.
- Prerequisites: none
- Evidence: manual-review `docs/conformance/v1.31.0/{supported.md,excluded.md}; docs/readiness/conformance.md`

## Production TOML fields

All accepted production leaves are listed below. Fields are startup-static; changing one requires a `tokeirad` restart. Optional live task-queue policy is not a TOML field and is described separately.

| Field | Class | Default | Required in optional section | Restart | Owning feature | Guidance |
|---|---|---|---|---|---|---|
| `capacity.dsql.burst_capacity` | capacity | `1000` | no | yes | — | DSQL connection-rate burst allowance. |
| `capacity.dsql.connection_rate_per_second` | capacity | `100` | no | yes | — | Fleet-wide DSQL connection establishment rate. |
| `capacity.dsql.max_connections` | capacity | `10000` | no | yes | — | Fleet-wide DSQL connection ceiling. |
| `capacity.performance.target_p99_wft_latency_ms` | capacity | `50` | no | yes | — | Capacity-planning workflow-task p99 latency target. |
| `capacity.performance.target_workflow_starts_per_second` | capacity | `1000` | no | yes | — | Capacity-planning workflow-start target. |
| `emergency.cap_poll_admission` | emergency | `<unset>` | no | yes | — | Emergency-only cap on concurrent poll admission. |
| `emergency.disable_stickiness` | emergency | `false` | no | yes | — | Emergency-only restriction that disables sticky execution. |
| `emergency.freeze_projection` | emergency | `false` | no | yes | — | Emergency-only restriction that freezes projection advancement. |
| `infrastructure.cluster_name` | infrastructure | `"tokeira-local"` | no | yes | — | Stable cluster label used in operator output and telemetry. |
| `infrastructure.dsql.admin_role_arn` | infrastructure | `<unset>` | no | yes | — | Provisioning identity for DSQL administrative operations. |
| `infrastructure.dsql.conn_lease_table` | infrastructure | `<unset>` | no | yes | — | Provisioned DynamoDB table for distributed connection-slot leases. |
| `infrastructure.dsql.endpoint` | infrastructure | `<unset>` | yes | yes | — | Required when storage is dsql; normally written by the provisioner. |
| `infrastructure.dsql.rate_limiter_table` | infrastructure | `<unset>` | no | yes | — | Provisioned DynamoDB table for distributed rate limiting. |
| `infrastructure.dsql.readonly_role_arn` | infrastructure | `<unset>` | no | yes | — | Read-only identity for inspection tooling. |
| `infrastructure.dsql.region` | infrastructure | `<unset>` | no | yes | — | Optional DSQL signing region; defaults to infrastructure.region. |
| `infrastructure.dsql.runtime_role_arn` | infrastructure | `<unset>` | no | yes | — | Runtime identity for authoritative DSQL reads and writes. |
| `infrastructure.network.grpc_addr` | infrastructure | `"0.0.0.0:7233"` | no | yes | — | Temporal gRPC listener address. |
| `infrastructure.network.metrics_addr` | infrastructure | `"0.0.0.0:9090"` | no | yes | — | Prometheus metrics listener address. |
| `infrastructure.observability.alert_thresholds.autoscaler_metric_staleness_seconds` | infrastructure | `30` | no | yes | — | Autoscaler metric-staleness alert threshold. |
| `infrastructure.observability.alert_thresholds.dsql_occ_conflict_rate_per_sec` | infrastructure | `10.0` | no | yes | — | OCC-conflict alert rate. |
| `infrastructure.observability.alert_thresholds.dsql_reservoir_exhaustion_ratio` | infrastructure | `0.9` | no | yes | — | DSQL connection-reservoir alert ratio. |
| `infrastructure.observability.alert_thresholds.projection_checkpoint_lag_seconds` | infrastructure | `60` | no | yes | — | Projection checkpoint-lag alert threshold. |
| `infrastructure.observability.dashboard_provisioning_enabled` | infrastructure | `true` | no | yes | — | Provision bundled observability dashboards. |
| `infrastructure.observability.leak_detection_deadline_ms` | infrastructure | `30000` | no | yes | — | Deadline used by task-leak detection. |
| `infrastructure.observability.log_filter` | infrastructure | `"info"` | no | yes | — | Tracing filter applied at process startup. |
| `infrastructure.observability.log_format` | infrastructure | `"text"` | no | yes | — | Structured json or human-readable text logs. |
| `infrastructure.observability.metrics_enabled` | infrastructure | `true` | no | yes | — | Enable the Prometheus metrics endpoint. |
| `infrastructure.observability.otlp_enabled` | infrastructure | `false` | no | yes | — | Enable OTLP trace export. |
| `infrastructure.observability.otlp_endpoint` | infrastructure | `"http://localhost:4317"` | no | yes | — | OTLP trace collector endpoint. |
| `infrastructure.observability.otlp_metrics.enabled` | infrastructure | `false` | no | yes | — | Enable OTLP metrics export. |
| `infrastructure.observability.otlp_metrics.endpoint` | infrastructure | `<unset>` | yes | yes | — | Collector endpoint required when OTLP metrics are enabled. |
| `infrastructure.observability.otlp_metrics.max_buffered_batches` | infrastructure | `1024` | no | yes | — | Maximum metrics batches buffered before backpressure. |
| `infrastructure.observability.otlp_metrics.protocol` | infrastructure | `"grpc"` | no | yes | — | OTLP metrics transport protocol. |
| `infrastructure.observability.otlp_protocol` | infrastructure | `"grpc"` | no | yes | — | OTLP trace transport protocol. |
| `infrastructure.observability.smoke_test_timeout_ms` | infrastructure | `30000` | no | yes | — | Observability smoke-test timeout. |
| `infrastructure.observability.trace_sample_rate` | infrastructure | `1.0` | no | yes | — | Base trace sampling ratio from zero through one. |
| `infrastructure.placement.bundle_count` | infrastructure | `1` | no | yes | — | Placement bundle count. |
| `infrastructure.placement.controller_endpoint` | infrastructure | `<unset>` | no | yes | — | Placement-controller endpoint; absence selects the single-node path. |
| `infrastructure.placement.hash_version` | infrastructure | `1` | no | yes | — | Pinned placement hash algorithm version. |
| `infrastructure.placement.heartbeat_interval_ms` | infrastructure | `5000` | no | yes | — | Node membership heartbeat interval. |
| `infrastructure.placement.node_host` | infrastructure | `"127.0.0.1"` | no | yes | — | Advertised node host; TOKEIRA_NODE_HOST may supply a per-pod value. |
| `infrastructure.placement.node_port` | infrastructure | `<unset>` | no | yes | — | Advertised node port; defaults to the gRPC listener port. |
| `infrastructure.placement.partition_count` | infrastructure | `16` | no | yes | — | Storage partition count; the DSQL profile promotes the legacy default to 4. |
| `infrastructure.placement.reconnect_base_delay_ms` | infrastructure | `1000` | no | yes | — | Initial controller reconnection delay. |
| `infrastructure.placement.reconnect_max_delay_ms` | infrastructure | `30000` | no | yes | — | Maximum controller reconnection delay. |
| `infrastructure.placement.routing_max_retries` | infrastructure | `3` | no | yes | — | Maximum retries after a stale placement route. |
| `infrastructure.placement.shard_count` | infrastructure | `1` | no | yes | — | Logical shard count; the DSQL profile promotes the legacy default to 32. |
| `infrastructure.region` | infrastructure | `"us-east-1"` | no | yes | — | AWS region used by regional infrastructure integrations. |
| `infrastructure.storage` | infrastructure | `"in-memory"` | no | yes | — | Select in-memory development storage or Aurora DSQL. |
| `policy.authorization.aws_iam.grants[].grant` | Tokeira native | `[]` | yes | yes | `aws-iam-bearer-authorization` | Temporal namespace:role grants for matching AWS identities. |
| `policy.authorization.aws_iam.grants[].match_arn` | Tokeira native | `""` | yes | yes | `aws-iam-bearer-authorization` | Full-string STS caller-ARN glob. |
| `policy.authorization.expose_authorizer_errors` | configured parity | `false` | no | yes | `authorization` | Expose authorizer implementation failures instead of generic denial. |
| `policy.authorization.jwt.issuers[].audience` | configured parity | `""` | no | yes | `authorization` | Optional exact audience; blank disables audience validation. |
| `policy.authorization.jwt.issuers[].grants[].grant` | configured parity | `[]` | yes | yes | `authorization` | Temporal namespace:role grants for matching JWT subjects. |
| `policy.authorization.jwt.issuers[].grants[].match_sub` | configured parity | `""` | yes | yes | `authorization` | Full-string subject glob for supplemental grants. |
| `policy.authorization.jwt.issuers[].issuer` | configured parity | `""` | yes | yes | `authorization` | Exact signed iss value; issuer routing is case-sensitive and exact. |
| `policy.authorization.jwt.issuers[].jwks_uri` | configured parity | `""` | yes | yes | `authorization` | JWKS document URI for this exact issuer. |
| `policy.authorization.jwt.issuers[].name` | configured parity | `""` | yes | yes | `authorization` | Stable operator label for one issuer profile. |
| `policy.authorization.jwt.issuers[].permissions_claim` | configured parity | `"permissions"` | no | yes | `authorization` | JWT array claim containing Temporal namespace:role grants. |
| `policy.authorization.jwt.issuers[].refresh_interval` | configured parity | `<unset>` | no | yes | `authorization` | Optional positive JWKS refresh duration using ms, s, m, or h. |
| `policy.authorization.principal_attribution` | configured parity | `false` | no | yes | `authorization` | Write the authenticated principal to server-authored history metadata. |
| `policy.compatibility.enable_standalone_activities` | configured parity | `false` | no | yes | `activity-executions` | Enable the v1.31.0 preview standalone-activity surface. |
| `policy.default_retention_days` | stock parity | `30` | no | yes | `namespace-management` | Default workflow-history retention for newly created namespaces. |
| `policy.http_api.additional_forwarded_headers` | configured parity | `[]` | no | yes | `http-json-api` | Additional exact or trailing-star header rules forwarded to gRPC. |
| `policy.http_api.allowed_hosts` | stock parity | `["*"]` | no | yes | `http-json-api` | Case-sensitive host patterns admitted by the HTTP/JSON gateway. |
| `policy.namespace_creation` | configured parity | `"open"` | no | yes | `namespace-management` | Choose open or controlled namespace creation. |
| `policy.nexus_completion.http_addr` | Tokeira native | `"127.0.0.1:7253"` | no | yes | `nexus-task-transport` | Inbound Nexus completion-callback listener. |
| `policy.nexus_completion.retry_backoff_coefficient` | stock parity | `2.0` | no | yes | `nexus-task-transport` | Asynchronous Nexus completion retry multiplier. |
| `policy.nexus_completion.retry_initial_interval_ms` | stock parity | `1000` | no | yes | `nexus-task-transport` | Initial asynchronous Nexus completion retry interval. |
| `policy.nexus_completion.retry_max_attempts` | Tokeira native | `0` | no | yes | `nexus-task-transport` | Zero preserves v1.31.0's unbounded retry horizon; positive values impose a safety cap. |
| `policy.nexus_completion.retry_max_interval_ms` | stock parity | `3600000` | no | yes | `nexus-task-transport` | Maximum asynchronous Nexus completion retry interval. |
| `policy.nexus_completion.system_callback_url` | Tokeira native | `"http://127.0.0.1:7253"` | no | yes | `nexus-task-transport` | Callback URL that must be reachable from Nexus workers. |
| `policy.nexus_endpoint_limits.description_max_size` | stock parity | `20000` | no | yes | `nexus-admin` | Maximum encoded Nexus endpoint description size. |
| `policy.nexus_endpoint_limits.external_url_max_length` | stock parity | `4096` | no | yes | `nexus-admin` | Maximum external Nexus URL length. |
| `policy.nexus_endpoint_limits.list_default_page_size` | stock parity | `100` | no | yes | `nexus-admin` | Default Nexus endpoint list page size. |
| `policy.nexus_endpoint_limits.list_max_page_size` | stock parity | `1000` | no | yes | `nexus-admin` | Maximum Nexus endpoint list page size. |
| `policy.nexus_endpoint_limits.name_max_length` | stock parity | `200` | no | yes | `nexus-admin` | Maximum Nexus endpoint name length. |
| `policy.nexus_endpoint_limits.task_queue_max_length` | stock parity | `1000` | no | yes | `nexus-admin` | Maximum Nexus worker task-queue name length. |
| `policy.quotas.max_signal_payload_bytes` | configured parity | `4194304` | no | yes | `workflow-signal` | Maximum admitted signal payload size. |
| `policy.quotas.max_workflow_timeout_seconds` | configured parity | `315360000` | no | yes | `workflow-start` | Maximum admitted workflow execution timeout. |
| `policy.task_queues.enable_fairness` | configured parity | `false` | no | yes | `user-fairness` | Opt in to weighted User Fairness; priority remains enabled. |

## Durable live task-queue policy

`UpdateTaskQueueConfig` authors durable policy independently for each `(namespace, task queue, task kind)`. Queue rate limits, the default per-fairness-key rate, and fairness-weight overrides commit through compare-and-swap storage before the API returns success. Every server hydrates its disposable cache before admitting traffic and refreshes remote revisions internally. This public API policy therefore survives process replacement without becoming workflow history or kernel state.

## What Tokeira does not configure

Temporal's file-backed dynamic-config loader, separate frontend/history/matching/worker service topology, plugin persistence selection, multi-cluster redirection, and excluded feature controls are not Tokeira production configuration surfaces. See [`temporal-configuration.md`](./temporal-configuration.md) for the complete denominator and treatment of all 613 source declarations.
