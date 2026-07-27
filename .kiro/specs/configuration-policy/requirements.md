# Requirements Document

Feature: Configuration Policy

## Introduction

Tokeira claims observable compatibility with Temporal server v1.31.0 while deliberately
offering a much smaller operator configuration surface. Temporal v1.31.0 defines hundreds
of production dynamic settings across its source tree plus a multi-service static YAML
topology. Tokeira has a typed, strict `tokeirad.toml`, release-pinned behavioral constants,
internally owned mechanical settings, public runtime policy APIs, and a feature-gated
conformance override bridge. What it lacks is one normative doctrine that classifies those
sources, resolves precedence, and prevents test controls from being mistaken for production
policy.

This feature turns the owner-approved
[`configuration-policy-proposal.md`](./reference/configuration-policy-proposal.md)
into an executable product contract. Tokeira conforms to Temporal's observable configured
behavior, not to Temporal's dynamic-configuration control plane or raw key spelling. Empty
production configuration preserves stock v1.31.0 behavior for every supported compatibility
feature. Production variability remains typed and intentionally small.

The compatibility authority is Temporal server `v1.31.0` under root `AGENTS.md §8`. The
current Temporal denominator is
[`temporal-configuration.md`](../../../docs/conformance/v1.31.0/temporal-configuration.md),
which this feature corrects through a source-aware extraction of
production setting declarations. Dynamic defaults and scopes were verified against
production `New*Setting` declarations throughout the v1.31.0 source tree; public task-queue
configuration behavior was verified against
`service/frontend/workflow_handler.go` and
`service/matching/matching_engine.go @ v1.31.0`.

This specification follows the completed
`.kiro/specs/configuration-foundation/` and
`.kiro/specs/task-queue-priority-fairness/` features. It preserves their shipped typed TOML
and delivery behavior while superseding two provisional boundaries:

- User Fairness gains an opt-in production deployment policy whose absence retains the
  stock-disabled default.
- `UpdateTaskQueueConfig` policy becomes durable rather than process-local.

The feature also creates a complete, checked classification ledger for Temporal's
configuration surface and a typed effective-policy boundary for production and
conformance consumers. It also prepares the v1.31.0 conformance documentation for public
release by separating authoritative product truth from the valuable deliberation records
that belong with their owning specs. It does not reproduce Temporal's service topology,
generic dynamic-config selectors, gradual rollout, file polling, subscriptions, or generic
hot reload.

## Glossary

- **Behavioral Compatibility:** Equality of public responses, history, lifecycle,
  admission results, and delivery tendencies for the same effective policy.
- **Configuration Mechanism Compatibility:** Compatibility with Temporal's raw key
  names, selector syntax, loader, polling, subscriptions, or rollout mechanism. This
  feature explicitly does not claim it.
- **Stock Default:** The default value and resulting behavior in Temporal server
  v1.31.0.
- **Empty Configuration:** A missing or empty `tokeirad.toml` resolved entirely through
  `TokeiraConfig::default()`.
- **Classification Ledger:** The checked, structured inventory assigning every
  Temporal configuration item one Tokeira disposition.
- **Feature Catalog:** The checked, structured inventory of every public feature Tokeira
  presents, including its origin, support state, defaults, enablement mechanism,
  prerequisites, and evidence.
- **Feature Origin:** Whether a feature belongs to the v1.31.0 behavioral target, exists
  only in the newer vendored wire API, or is a Tokeira-native extension.
- **Enablement Mechanism:** The exact operator action that activates a supported but
  default-disabled feature, or `none` when the feature is enabled by default.
- **Authoritative Conformance Document:** Public release documentation that states the
  current conformance contract or product truth without carrying superseded deliberation.
- **Deliberation Record:** Research, alternatives, review history, and decision rationale
  retained with the spec that owns the resulting implementation.
- **Public API Policy:** Mutable operator policy authored through a public Temporal API.
- **Deployment Policy:** Typed cluster policy authored in `tokeirad.toml`.
- **Pinned Behavioral Constant:** Observable policy fixed to the v1.31.0 default.
- **Auto-Tuned Mechanical Setting:** Internal throughput, batching, caching,
  concurrency, polling, or resource behavior that does not define the public contract.
- **Conformance-Only Override:** An allow-listed test value available only in
  `--features conformance` builds.
- **Architecturally Irrelevant Setting:** A Temporal setting whose service topology or
  excluded behavior has no Tokeira counterpart.
- **Tokeira-Native Extension:** Typed product policy with no Temporal equivalent and no
  place in the v1.31.0 compatibility claim.
- **Effective Policy:** The typed value observed at an edge, runtime, storage, or
  projection decision site after applying the approved source precedence.
- **Policy Source:** A pinned default, typed deployment field, durable public API
  record, emergency restriction, or conformance-only overlay.
- **Task Queue Policy:** Durable public policy keyed by namespace, task-queue name, and
  task kind.
- **User Fairness:** Weighted within-priority delivery among fairness keys in one
  normal task queue.
- **Emergency Restriction:** Explicit break-glass policy that may narrow normal
  behavior during an incident.

## Target State

1. Empty production configuration selects stock v1.31.0 defaults for every supported
   compatibility behavior.
2. Every unique production dynamic setting declared by Temporal v1.31.0 and every
   relevant static configuration group has exactly one checked Tokeira classification.
3. Production consumers resolve policy through typed interfaces rather than raw
   Temporal key strings.
4. Static deployment policy remains startup-bound; no generic production hot-reload
   mechanism exists.
5. Conformance overrides remain isolated, allow-listed test transport and never become
   production configuration by implication.
6. Priority remains enabled with five bands and default key 3. User Fairness is an
   opt-in `[policy.task_queues]` deployment policy whose default is `false`.
7. `UpdateTaskQueueConfig` values are durably committed, survive restart, and continue
   shaping task handout through the already-landed delivery behavior.
8. Per-task-queue rates and fairness weights are authored only through the public API;
   `tokeirad.toml` does not duplicate them.
9. The pure kernel receives already-resolved deterministic inputs and never owns,
   reads, or retains configuration policy.
10. Operator documentation distinguishes stock parity, configured parity,
    conformance-only capability, and Tokeira-native extensions.
11. An exhaustive Feature Catalog states exactly which public features Tokeira
    presents, whether each is enabled by default, and how an operator enables it.
12. The public v1.31.0 conformance folder contains only authoritative contract,
    configuration, feature-availability, and published conformance-report documents;
    deliberation records live with their owning specs.

The following remain out of scope:

- a generic Temporal-compatible dynamic-config service;
- arbitrary raw Temporal keys in production TOML;
- generic global/namespace/task-queue/shard selector syntax;
- percentage rollout and subscription semantics;
- production live reload of `tokeirad.toml`;
- Temporal frontend/history/matching/worker process topology;
- settings owned exclusively by excluded multi-cluster, replication, archival, or DLQ
  behavior;
- exposing `RuntimeConfig` mechanical values in TOML.

## Evidence From Current Code

### Authoritative contract and behavior

- A source-aware extraction of production `New*Setting` declarations at v1.31.0 finds
  613 unique dynamic settings: 565 in `common/dynamicconfig/constants.go` and 48 in
  other production packages. The previous 564-key count came from incidental string
  matching in one file, included non-setting literals, and omitted real settings
  declared elsewhere.
- `common/dynamicconfig/constants.go @ v1.31.0` sets
  `matching.useNewMatcher = true`,
  `matching.enableFairness = false`, `matching.priorityLevels = 5`,
  `matching.autoEnableV2 = false`, and
  `matching.maxFairnessKeyWeightOverrides = 1000`.
- `chasm/lib/activity/config.go @ v1.31.0` declares
  `activity.enableStandalone = false`, `activity.longPollTimeout`, and
  `activity.longPollBuffer`; callback, Nexus-operation, scheduler, and other CHASM
  settings are likewise declared outside `common/dynamicconfig/constants.go`.
- `proto/upstream/temporal/api/workflowservice/v1/request_response.proto` defines all
  eight fields of `UpdateTaskQueueConfigRequest` and the response `config`.
- `proto/upstream/temporal/api/taskqueue/v1/message.proto` defines
  `TaskQueueConfig`, `RateLimitConfig`, `ConfigMetadata`, and `RateLimit`.
- `service/frontend/workflow_handler.go @ v1.31.0` validates public task-queue policy
  before forwarding it.
- `service/matching/matching_engine.go @ v1.31.0` atomically updates durable task-queue
  user data and returns the committed configuration.
- `service/matching/task_queue_partition_manager.go @ v1.31.0` makes fairness imply
  priority-aware delivery and keeps sticky queues fairness-disabled.

### Current Tokeira implementation

- `crates/tokeira-config/src/lib.rs` owns strict, typed `TokeiraConfig` and
  `PolicyConfig`; it currently has no task-queue fairness field.
- `crates/tokeira-runtime/src/task_ordering.rs` pins stock delivery defaults and reads
  raw `matching.*` keys only in conformance builds through
  `StockDeliveryModeProvider`.
- `crates/tokeira-runtime/src/task_queue_config.rs` faithfully validates, atomically
  merges, reads, and consumes task-queue policy, but
  `InMemoryTaskQueueConfigStore` loses it on restart.
- `apps/tokeirad/src/lib.rs` constructs one in-memory task-queue store and shares it
  across the runtime and edge.
- `crates/tokeira-conformance/src/lib.rs` owns the allow-listed raw Temporal keys used
  by the conformance bridge.
- `.kiro/specs/task-queue-priority-fairness/` completed priority ordering, optional
  User Fairness, rate shaping, weight precedence, enhanced statistics, and scoped
  conformance controls while explicitly deferring production fairness configuration
  and task-queue policy durability to this decision.
- `docs/conformance/v1.31.0/supported.md` currently describes
  `UpdateTaskQueueConfig` without disclosing its process-local persistence boundary.
- `crates/tokeira-compatibility/src/matrix.rs` currently assigns all 129 vendored
  WorkflowService and OperatorService RPCs to feature records and already proves exact
  set equality against the vendored service definitions. Its feature records do not yet
  encode origin, maturity, defaults, operator enablement, scope, or mutability, and some
  states lag landed conformance evidence.
- `docs/readiness/conformance.md` is intentionally a measured suite-by-suite campaign
  ledger. It records the behavior encountered while making the corpus green; it is not
  an exhaustive product-feature inventory.
- `docs/conformance/v1.31.0/supported.md` defines the target Temporal surface and
  maturity boundary. It does not describe every Tokeira implementation state or
  default.
- `docs/conformance/v1.31.0/configuration-policy-proposal.md`,
  `authorization.md`, and `worker-versioning.md` preserve useful research and owner
  deliberation, but each now has an implementation-owning spec:
  `.kiro/specs/configuration-policy/`,
  `.kiro/specs/authorization-foundation/`, and
  `.kiro/specs/worker-deployments/`, respectively.
- `docs/conformance/v1.31.0/decisions.md` reports no open decisions. Its resolved
  outcomes are already represented in `supported.md`, `excluded.md`, or an owning spec,
  so it need not remain a public authority after links are repaired.

## Contract Policy

### Classification disposition

| Disposition | Target policy | Invalid representation | Persistence / side effect |
|---|---|---|---|
| `public-api-policy` | Implement the public API's validation, mutation, read-back, durability, and runtime effect | A duplicate TOML control for the same scope is rejected by the schema | Durable API-owned policy |
| `deployment-policy` | Expose one typed `[policy.*]` field with a documented stock default | Unknown/raw Temporal keys are rejected by strict TOML decoding | Startup-static cluster policy |
| `pinned-behavioral-constant` | Fix the value to the v1.31.0 default and test its boundary | No operator override exists in production | Code constant; no mutable record |
| `auto-tuned-mechanical-setting` | Keep the value internal to `RuntimeConfig` or an adaptive controller | No production TOML field exists | Volatile internal mechanics |
| `conformance-only-override` | Allow an exact typed key only in conformance builds | Unknown, wrongly typed, kernel-owned, or unenforced keys return the existing explicit override error | Process-local test scope only |
| `architecturally-irrelevant-or-excluded` | Record the owning architecture or exclusion reason | No no-op production field exists | None |
| `tokeira-native-extension` | Expose typed Tokeira policy outside the compatibility claim | Raw Temporal aliases are rejected unless separately classified for conformance | As documented by the owning extension |

### Effective-policy sources

| Source | Scope | Mutability | Precedence role |
|---|---|---|---|
| Release-pinned default | Release compatibility profile | Immutable for the release | Baseline |
| Typed deployment policy | Cluster/deployment | Startup-static | Overrides the baseline where the field exists |
| Durable public API policy | API-defined namespace/task-queue scope | Live after durable commit | Overrides less-specific deployment policy where the owning API defines composition |
| Emergency restriction | Cluster/deployment | Startup-static initially | May narrow lower layers for safety |
| Conformance-only overlay | Test-supplied scope | Live in conformance builds only | Overrides the tested consult site without becoming product policy |
| Mechanical controller | Internal ownership unit | Automatic | Not part of behavioral-policy precedence |

### Classification ledger record

| Field | Target policy | Invalid representation | Persistence / side effect |
|---|---|---|---|
| `temporal_key` | Exact unique v1.31.0 key spelling | Duplicate or absent denominator key fails the ledger check | Documentation metadata |
| `temporal_default` | Human-readable stock default or an explicit structured value | Missing default fails the ledger check | Documentation metadata |
| `temporal_scope` | Exact global, namespace, task-queue, shard, or other source scope | Missing scope fails the ledger check | Documentation metadata |
| `classification` | Exactly one disposition from the classification table | Unknown or multiple dispositions fail decoding/checking | Drives generated summaries only |
| `tokeira_treatment` | Typed field/API path, constant, controller, or exclusion explanation | Blank treatment fails the ledger check | Documentation metadata |
| `owner` | Owning crate, spec, decision, or exclusion record | Missing owner fails the ledger check | Documentation metadata |
| `conformance_override` | `none`, `wired`, `kernel-excluded`, or `not-enforced` | A value inconsistent with `KEY_CLASSIFICATION` fails checking | Cross-checks test transport |
| `evidence` | Repository-relative v1.31.0 and Tokeira anchors | Absolute machine paths fail checking | Review traceability |

### Feature Catalog record

| Field | Target policy | Invalid representation | Persistence / side effect |
|---|---|---|---|
| `feature_id` | Stable unique identifier | Duplicate or absent identifier fails checking | Documentation metadata |
| `name` | Operator-facing feature name | Blank name fails checking | Documentation metadata |
| `origin` | `temporal-v1.31.0`, `newer-wire-only`, or `tokeira-native` | Unknown origin fails decoding/checking | Defines compatibility context |
| `temporal_maturity` | v1.31.0 maturity or `not-applicable` | Missing target maturity fails checking | Documentation metadata |
| `tokeira_state` | Supported, experimental, excluded, unavailable, or explicitly partial | Missing or ambiguous state fails checking | Drives published availability |
| `temporal_default` | Enabled, disabled, conditional, or `not-applicable` | Missing default fails checking | Documents stock behavior |
| `tokeira_default` | Enabled, disabled, conditional, or unavailable | Missing default fails checking | Documents empty-config behavior |
| `enablement` | Exact typed TOML field, public API, automatic condition, build-only path, or `none` | A default-disabled supported feature without a usable mechanism fails checking | Drives operator guidance |
| `scope` | Cluster, namespace, task queue, workflow, worker, or other exact scope | Missing scope fails checking | Documents policy ownership |
| `mutability` | Startup-static, durable live API, automatic, immutable, or conformance-only | Missing mutability fails checking | Documents lifecycle |
| `surfaces` | Every owned public RPC and non-RPC surface | Missing or duplicate ownership fails totality checking | Defines catalog completeness |
| `guidance` | Exact enabling action and operational prerequisites | Blank guidance for an available feature fails checking | Drives operator documentation |
| `evidence` | Repository-relative v1.31.0 and Tokeira anchors | Absolute machine paths fail checking | Review traceability |

### Task Queue Priority and Fairness policy

| Policy | v1.31.0 default | Production treatment | Conformance treatment |
|---|---:|---|---|
| `matching.useNewMatcher` | `true` | Pinned priority-aware delivery | Exact typed override |
| `matching.priorityLevels` | `5` | Pinned five bands; default key 3 | No variable-band mode |
| `matching.enableFairness` | `false` | `[policy.task_queues].enable_fairness`, default `false` | Exact typed override |
| `matching.autoEnableV2` | `false` | Pinned `false` | Exact typed override with queue-local activation |
| `matching.enableMigration` | `true` | Architecturally irrelevant to Tokeira's backlog | No product setting |
| `matching.maxFairnessKeyWeightOverrides` | `1000` | Pinned public-API admission limit | Existing exact typed override |
| Queue/per-key rates and weight overrides | Unset/empty | `UpdateTaskQueueConfig` only | Same public API plus scoped cap override |

### `UpdateTaskQueueConfig`

The field-level validation and dispatch effects remain governed by
`.kiro/specs/task-queue-priority-fairness/`. This feature changes their storage lifetime
as follows:

| Field | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `namespace` (1) | Resolve the namespace through the existing edge contract | Existing namespace error | Part of the durable key |
| `identity` (2) | Preserve as update metadata | Existing ID-length error | Durable metadata on changed rate fields |
| `task_queue` (3) | Resolve the normal task-queue family name | Existing task-queue-name error | Part of the durable key |
| `task_queue_type` (4) | Isolate Workflow, Activity, and Nexus policy | Existing enum/type validation | Part of the durable key |
| `update_queue_rate_limit` (5) | Unset, set, or preserve according to field presence | Existing v1.31.0-compatible rate/type error | Durable atomic field patch and live handout effect |
| `update_fairness_key_rate_limit_default` (6) | Unset, set, or preserve according to field presence | Existing v1.31.0-compatible rate/type error | Durable atomic field patch and live per-key handout effect |
| `set_fairness_weight_overrides` (7) | Merge named overrides | Existing key/weight/count/conflict error | Durable atomic map patch and future weight resolution |
| `unset_fairness_weight_overrides` (8) | Remove named overrides | Existing key/count/conflict error | Durable atomic map patch and future weight resolution |
| response `config` (1) | Return the committed effective record | Not applicable | Read-after-commit projection |

## Requirements

### Requirement 1: Govern Observable Configuration Compatibility

**User Story:** As a compatibility owner, I want one configuration doctrine, so that
Tokeira can match v1.31.0 behavior without reproducing Temporal's control plane.

#### Acceptance Criteria

1. THE Compatibility Policy SHALL define Behavioral Compatibility as the governing
   configuration claim.
2. THE Compatibility Policy SHALL exclude Configuration Mechanism Compatibility from
   the v1.31.0 claim.
3. WHEN Empty Configuration is loaded, THE Effective Policy SHALL select the Stock
   Default for every supported compatibility feature.
4. IF a Tokeira default intentionally differs from a Stock Default, THEN THE
   Conformance Record SHALL cite an owner-approved divergence decision.
5. THE production configuration schema SHALL reject arbitrary Temporal
   dynamic-configuration key spellings.
6. THE production configuration schema SHALL reject unknown typed fields.
7. THE Compatibility Policy SHALL keep generic production dynamic-config selectors
   out of scope.
8. THE Compatibility Policy SHALL keep generic production hot reload out of scope.

### Requirement 2: Classify the Complete Temporal Configuration Surface

**User Story:** As a product owner, I want every Temporal configuration item explicitly
classified, so that omission is distinguishable from an intentional architectural
choice.

#### Acceptance Criteria

1. THE Classification Ledger SHALL contain exactly one record for each unique
   production dynamic setting declared through a `New*Setting` constructor in the
   Temporal v1.31.0 source tree.
2. THE Classification Ledger SHALL contain one record for each relevant static
   configuration field group in `common/config/config.go @ v1.31.0`.
3. THE Classification Ledger SHALL assign exactly one primary disposition to every
   record.
4. THE Classification Ledger SHALL encode every field defined by the ledger-record
   policy table.
5. IF a denominator key is absent from the ledger, THEN THE ledger verifier SHALL fail.
6. IF the ledger contains an unknown denominator key, THEN THE ledger verifier SHALL
   fail.
7. IF the ledger contains a duplicate key, THEN THE ledger verifier SHALL fail.
8. IF a ledger record has no owner or evidence, THEN THE ledger verifier SHALL fail.
9. WHEN the structured ledger changes, THE documentation generator SHALL reproduce the
   human-readable classification summary deterministically.
10. THE generated summary SHALL report counts by primary disposition.
11. THE generated summary SHALL distinguish production policy from conformance-only
    override support.
12. THE Classification Ledger SHALL contain no unresolved disposition when this
    feature completes.
13. THE denominator extractor SHALL inspect source-aware production setting
    declarations outside `common/dynamicconfig/constants.go`.
14. THE denominator extractor SHALL exclude string literals that are not setting
    declarations.

### Requirement 3: Resolve Policy Through Typed Boundaries

**User Story:** As a Tokeira developer, I want typed policy access at every production
decision site, so that raw test keys cannot silently become product configuration.

#### Acceptance Criteria

1. THE production Effective Policy interface SHALL expose typed accessors.
2. THE production Effective Policy interface SHALL reject arbitrary string-key lookup.
3. THE conformance adapter SHALL be the only production-workspace component that maps
   raw Temporal keys to typed policy values.
4. WHEN no more-specific source exists, THE Effective Policy SHALL return the
   release-pinned default.
5. WHERE a typed Deployment Policy exists, THE Effective Policy SHALL apply it over the
   release-pinned default.
6. WHERE a durable Public API Policy exists, THE Effective Policy SHALL apply the
   owning API's documented scope and precedence.
7. WHERE an Emergency Restriction applies, THE Effective Policy SHALL narrow the
   lower-precedence value according to its owning policy.
8. WHERE a Conformance-Only Override applies in a conformance build, THE Effective
   Policy SHALL expose it at the live tested decision site.
9. WHEN a typed policy value is invalid or unavailable, THE owning boundary SHALL
   return an explicit error.
10. THE Effective Policy architecture SHALL allow static cluster policy and durable
    task-queue policy to have different storage lifetimes.

### Requirement 4: Expose Intentional Deployment Policy

**User Story:** As a Tokeira operator, I want only legitimate deployment choices in
`tokeirad.toml`, so that configuration remains small and supportable.

#### Acceptance Criteria

1. THE `PolicyConfig` schema SHALL contain a typed `task_queues` section.
2. THE task-queue deployment policy SHALL contain an `enable_fairness` Boolean.
3. WHEN the task-queue deployment policy is absent, THE Config Loader SHALL resolve
   `enable_fairness` to `false`.
4. WHEN `enable_fairness` is `false`, THE runtime SHALL preserve User Fairness metadata
   without applying weighted dispatch.
5. WHEN `enable_fairness` is `true`, THE runtime SHALL enable User Fairness for
   non-sticky task queues.
6. WHEN `enable_fairness` is `true`, THE runtime SHALL keep priority-aware delivery
   enabled.
7. THE Config Loader SHALL treat task-queue deployment policy as startup-static.
8. THE effective-config output SHALL report the resolved task-queue deployment policy.
9. THE production TOML schema SHALL omit per-task-queue rates.
10. THE production TOML schema SHALL omit per-fairness-key weight overrides.
11. THE `RuntimeConfig` schema SHALL remain unavailable through production TOML.

### Requirement 5: Preserve v1.31.0 Priority and Fairness Defaults

**User Story:** As a Temporal operator, I want Tokeira's default and configured delivery
modes to match v1.31.0, so that priority metadata has predictable effects.

#### Acceptance Criteria

1. WHERE no conformance override is active, THE runtime SHALL enable priority-aware
   delivery.
2. THE runtime SHALL use five priority bands.
3. THE runtime SHALL use priority key 3 when a task supplies no effective key.
4. WHERE no Deployment Policy or conformance override enables fairness, THE runtime
   SHALL disable User Fairness.
5. WHERE no conformance override is active, THE runtime SHALL disable automatic
   fairness activation.
6. WHILE User Fairness is disabled, THE runtime SHALL preserve task-carried fairness
   keys and weights.
7. WHILE a task queue is sticky, THE runtime SHALL disable User Fairness for that
   queue.
8. WHERE the conformance `matching.enableFairness` override is active, THE runtime
   SHALL apply the override at the live delivery decision site.
9. WHERE the conformance `matching.autoEnableV2` override is active, THE runtime SHALL
   preserve the already-specified queue-local activation behavior.
10. THE runtime SHALL keep User Fairness independent from inter-queue drain-share
    fairness.

### Requirement 6: Persist Public Task Queue Policy

**User Story:** As a task-queue operator, I want accepted configuration to survive
process replacement, so that public API policy remains effective until changed.

#### Acceptance Criteria

1. WHEN a valid `UpdateTaskQueueConfig` request is accepted, THE Task Queue Policy
   Repository SHALL durably commit the complete resulting record before success is
   returned.
2. WHEN `UpdateTaskQueueConfig` returns success, THE response SHALL contain the
   committed record.
3. WHEN `tokeirad` restarts, THE Task Queue Policy Repository SHALL return the last
   committed record.
4. WHEN a task-queue policy record is loaded, THE runtime delivery path SHALL consume
   it without requiring a second API update.
5. WHEN a committed task-queue policy changes, THE runtime delivery path SHALL apply
   it to subsequent handout decisions.
6. THE Task Queue Policy Repository SHALL key records by namespace, task-queue name,
   and task kind.
7. THE Task Queue Policy Repository SHALL apply each field and map patch atomically.
8. IF concurrent updates conflict, THEN THE Task Queue Policy Repository SHALL prevent
   a stale writer from silently replacing a newer committed record.
9. THE Task Queue Policy Repository SHALL preserve rate-limit update metadata.
10. THE in-memory storage profile SHALL provide behaviorally equivalent process-local
    task-queue policy operations.
11. THE DSQL storage profile SHALL provide durable task-queue policy operations.
12. THE task-queue policy record SHALL remain outside workflow history.
13. THE task-queue policy record SHALL remain outside authoritative per-run state.
14. WHEN task-queue policy storage is unavailable, THE API SHALL return an explicit
    service error.
15. IF no task-queue policy record exists, THEN THE runtime SHALL use the public API's
    unset/default behavior.

### Requirement 7: Keep Conformance Overrides Honest and Isolated

**User Story:** As a conformance maintainer, I want test-only overrides to exercise
supported modes without broadening the production configuration claim.

#### Acceptance Criteria

1. THE Conformance-Only Override registry SHALL compile only under the conformance
   feature.
2. THE Conformance-Only Override registry SHALL accept only exact allow-listed keys.
3. THE Conformance-Only Override registry SHALL validate each key's value type.
4. THE Conformance-Only Override registry SHALL reset scoped values between corpus
   leaves.
5. IF a key is kernel-owned or unenforced, THEN THE Conformance-Only Override registry
   SHALL return its existing explicit rejection.
6. THE production binary SHALL expose no raw Temporal dynamic-config input.
7. THE Classification Ledger SHALL identify every wired Conformance-Only Override.
8. IF `KEY_CLASSIFICATION` and the Classification Ledger disagree, THEN THE ledger
   verifier SHALL fail.
9. THE readiness ledger SHALL distinguish a conformance-only green mode from a
   production-configurable mode.

### Requirement 8: Preserve Architectural Boundaries

**User Story:** As a Tokeira maintainer, I want policy resolved in its owning plane, so
that conformance work does not taint the kernel or delivery architecture.

#### Acceptance Criteria

1. THE kernel SHALL have no dependency on Tokeira configuration crates.
2. THE kernel SHALL have no dependency on the Conformance-Only Override registry.
3. THE kernel SHALL perform no policy-provider lookup.
4. WHEN policy affects a transition, THE runtime or edge SHALL pass an
   already-resolved deterministic input to the kernel.
5. THE kernel SHALL retain no scheduler or configuration state between calls.
6. THE runtime delivery plane SHALL own User Fairness mode selection.
7. THE Task Queue Policy Repository SHALL remain non-authoritative for workflow
   correctness.
8. THE lane router SHALL remain independent of task priority and User Fairness.
9. THE mechanical `RuntimeConfig` SHALL remain internal and default-owned.

### Requirement 9: Migrate Without Changing Existing Defaults

**User Story:** As an operator upgrading Tokeira, I want the policy refactor to preserve
existing default behavior, so that adopting the new release is safe.

#### Acceptance Criteria

1. WHEN an existing valid `tokeirad.toml` omits `policy.task_queues`, THE Config Loader
   SHALL continue accepting it.
2. WHEN an existing valid `tokeirad.toml` is loaded, THE new Policy Boundary SHALL
   preserve every unrelated effective value.
3. WHEN `TokeiraConfig` is serialized and deserialized, THE task-queue deployment
   policy SHALL round-trip losslessly.
4. WHEN an empty TOML document is loaded, THE Config Loader SHALL produce a valid
   `TokeiraConfig`.
5. WHEN no task-queue policy has ever been written, THE durable repository SHALL
   require no data migration from the previous volatile store.
6. THE DSQL schema change SHALL follow the storage crate's forward-only migration
   rules.
7. THE in-memory development profile SHALL continue to require no external service.

### Requirement 10: Audit the Complete Presented Feature Surface

**User Story:** As an operator or compatibility owner, I want an exhaustive feature
catalog, so that I can distinguish what Tokeira actually presents from what happened to
be exercised by the conformance campaign.

#### Acceptance Criteria

1. THE Feature Catalog SHALL assign every vendored WorkflowService and OperatorService
   RPC to exactly one feature record.
2. THE Feature Catalog SHALL distinguish the 121 RPCs in the v1.31.0 behavioral target
   from the 8 RPCs present only in the newer vendored wire API.
3. THE Feature Catalog SHALL include public Temporal features whose observable surface
   is not fully represented by RPC ownership.
4. THE Feature Catalog SHALL include every public Tokeira-Native Extension.
5. THE Feature Catalog SHALL encode every field defined by the Feature Catalog record
   policy table.
6. THE Feature Catalog SHALL state the Tokeira support state separately from its
   default enablement.
7. THE Feature Catalog SHALL state both the Temporal v1.31.0 default and the Tokeira
   Empty Configuration default.
8. WHERE an available feature is not enabled by default, THE Feature Catalog SHALL
   provide its exact Enablement Mechanism.
9. THE Feature Catalog SHALL state each feature's policy scope and mutability.
10. THE Feature Catalog SHALL provide actionable guidance, prerequisites, and
    repository-relative evidence for every available feature.
11. IF a vendored public RPC is missing from the Feature Catalog, THEN THE catalog
    verifier SHALL fail.
12. IF a public RPC is assigned to more than one feature, THEN THE catalog verifier
    SHALL fail.
13. WHEN the initial feature audit completes, THE Feature Catalog SHALL reconcile every
    existing `FEATURE_MATRIX` state with landed implementation and conformance evidence.
14. THE Feature Catalog SHALL be the canonical product-feature inventory.
15. THE readiness conformance ledger SHALL remain an evidence/status ledger rather
    than becoming a competing product-feature inventory.

### Requirement 11: Make the Policy Operator-Visible

**User Story:** As a Tokeira operator, I want a small authoritative public conformance
set, so that I can understand the contract, configuration, and feature availability
without mistaking historical deliberation for current product truth.

#### Acceptance Criteria

1. THE public v1.31.0 conformance `README.md` SHALL state the approved
   behavioral-compatibility doctrine.
2. THE public v1.31.0 conformance `README.md` SHALL state the empty-configuration
   guarantee.
3. THE supported-surface record SHALL describe `UpdateTaskQueueConfig` as durable only
   after durable enforcement lands.
4. THE excluded-surface record SHALL identify Temporal configuration mechanisms that
   Tokeira does not claim.
5. THE existing upstream configuration reference SHALL be renamed
   `temporal-configuration.md`.
6. THE Temporal configuration reference SHALL enumerate every source-aware production
   dynamic setting and every relevant static configuration group at v1.31.0.
7. THE repository SHALL contain a separate `tokeira-configuration.md` as the canonical
   operator-facing Tokeira configuration reference.
8. THE Tokeira configuration reference SHALL enumerate every production configuration
   field accepted by the strict typed schema.
9. THE Tokeira configuration reference SHALL include a Feature Catalog-derived table
   of supported, partial, experimental, excluded, and unavailable features.
10. FOR EACH feature, THE Tokeira configuration reference SHALL state whether Empty
    Configuration enables it.
11. WHERE a feature is available but disabled by default, THE Tokeira configuration
    reference SHALL give the exact operator action that enables it.
12. THE Tokeira configuration reference SHALL distinguish unavailable features from
    available but default-disabled features.
13. THE Tokeira configuration reference SHALL distinguish production enablement from
    Conformance-Only Override support.
14. THE repository SHALL contain one canonical annotated `config.example.toml`.
15. THE canonical example SHALL identify every operationally load-bearing field.
16. THE canonical example SHALL identify stock-parity fields.
17. THE canonical example SHALL identify configured-parity fields.
18. THE canonical example SHALL identify Tokeira-native extensions.
19. THE canonical example SHALL warn that JWT issuer routing requires an exact match
    to the token's signed `iss` value.
20. THE canonical example SHALL warn that the Nexus system callback URL must be
    reachable from Nexus workers.
21. THE canonical example SHALL state that absent task-queue fairness policy preserves
    priority while disabling User Fairness.
22. THE readiness documentation SHALL link the generated complete classification as
    evidence for the close-to-zero configuration claim.
23. THE public v1.31.0 conformance folder SHALL retain `README.md`, `supported.md`,
    `excluded.md`, `temporal-configuration.md`, and `tokeira-configuration.md` as its
    authoritative core.
24. WHEN a release conformance report is published, THE public v1.31.0 conformance
    folder SHALL admit it as measured authoritative evidence.
25. THE configuration policy Deliberation Record SHALL move to
    `.kiro/specs/configuration-policy/reference/configuration-policy-proposal.md`.
26. THE authorization Deliberation Record SHALL move to
    `.kiro/specs/authorization-foundation/reference/v1.31.0-conformance-decision.md`.
27. THE Worker Versioning Deliberation Record SHALL move to
    `.kiro/specs/worker-deployments/reference/v1-v2-conformance-decision.md`.
28. WHEN no conformance-surface decision remains open, THE repository SHALL retire the
    public `decisions.md` page.
29. WHEN a Deliberation Record moves to an owning spec, THE authoritative supported or
    excluded surface SHALL retain its concise outcome and evidence link.
30. WHEN an undecided conformance question arises, THE owning feature spec SHALL
    capture the research and decision before public authoritative documents change.
31. WHEN documentation is reorganized, THE repository SHALL preserve all substantive
    decision rationale.
32. WHEN documentation is reorganized, THE offline link verifier SHALL report no
    broken internal links.

### Requirement 12: Verify Policy Composition and Durability

**User Story:** As a Tokeira contributor, I want executable policy invariants, so that
future configuration additions cannot bypass the approved doctrine.

#### Acceptance Criteria

1. THE policy-precedence property test SHALL match every generated valid policy-source
   combination against the specified precedence reference model.
2. THE configuration-round-trip property test SHALL preserve equality for every
   generated valid configuration value.
3. THE ledger-completeness property test SHALL accept exactly the complete unique
   denominator for every generated ledger mutation.
4. THE task-queue patch property test SHALL match every generated valid patch sequence
   against an atomic reference state machine.
5. THE task-queue conflict property test SHALL preserve at most one valid successor per
   expected version for every generated conflicting-write schedule.
6. THE task-queue codec property test SHALL preserve equivalent values through the
   in-memory and DSQL codecs for every generated record.
7. WHEN a DSQL-backed server restarts, THE integration test SHALL observe the last
   committed task-queue policy.
8. WHEN the fairness deployment field is absent, THE integration test SHALL observe
   stock-disabled User Fairness.
9. WHEN the fairness deployment field is true, THE integration test SHALL observe
   enabled non-sticky User Fairness.
10. WHEN the conformance overlay changes a wired policy, THE integration test SHALL
    observe the live override without changing production configuration.
11. THE existing feature-catalog totality test SHALL continue comparing all vendored
    WorkflowService and OperatorService definitions with catalog ownership.
12. THE feature-catalog guidance test SHALL reject every default-disabled available
    feature that lacks an exact production Enablement Mechanism.

## Iteration and Feedback Notes

- Owner approval on 2026-07-26 accepted the doctrine and recommendations in
  `configuration-policy-proposal.md`, including stock empty-config defaults,
  startup-static production policy, opt-in User Fairness, durable
  `UpdateTaskQueueConfig`, and a structured classification manifest.
- The exact persistent repository interface, DSQL schema, effective-policy type
  boundaries, manifest format, generated-document path, and property-test placement
  remain design decisions for the next consent gate.
- The already-landed task-queue priority/fairness behavior is not reopened. This
  feature supplies its approved production enablement and persistence dependencies.
- Owner feedback on 2026-07-26 separated the exhaustive upstream Temporal reference
  from an operator-facing Tokeira configuration and feature guide, and requested an
  audit of the complete feature surface rather than treating the conformance campaign
  ledger as exhaustive.
- Ground-truth follow-up replaced the incidental 564-key denominator with 613
  source-aware production dynamic-setting declarations at v1.31.0. The implementation
  SHALL reproduce that denominator from declarations rather than pinning the count as
  an unverified literal.
- Owner feedback on 2026-07-26 established the public-release documentation boundary:
  the v1.31.0 folder retains a small authoritative truth set, while historical
  deliberation remains preserved under the spec that owns each resulting feature.
