# Configuration surface — proposal for owner review

> **Status: historical deliberation record.** The owner approved this proposal and
> `.kiro/specs/configuration-policy/` implemented it. Its original 564-key estimate
> is preserved as deliberation context; the source-aware audit corrected the
> authoritative denominator to 613 declarations in
> [`temporal-configuration.md`](../../../../docs/conformance/v1.31.0/temporal-configuration.md).
> Current normative outcomes live in
> [`README.md`](../../../../docs/conformance/v1.31.0/README.md),
> [`supported.md`](../../../../docs/conformance/v1.31.0/supported.md), and
> [`excluded.md`](../../../../docs/conformance/v1.31.0/excluded.md).
>
> **Behavioural target:** Temporal server `v1.31.0`. Temporal source references below are to that
> tag. Tokeira conforms to observable behaviour, following
> [`AGENTS.md §8`](../../../../AGENTS.md); it does not port Temporal's service architecture or
> implementation.

## Proposed decision

**Tokeira conforms to Temporal's observable configured behaviour, not to Temporal's dynamic-
configuration mechanism or its 564-key spelling.**

An empty `tokeirad.toml` SHALL select the stock Temporal v1.31.0 behavioural defaults for every
supported compatibility feature. Tokeira SHALL expose variability only when at least one of these
is true:

1. Temporal exposes the policy through a public API that clients or operators call.
2. A real deployment must choose the value for correctness, security, reachability, tenancy, or
   externally visible resource policy.
3. Tokeira deliberately offers a documented product extension.
4. The conformance corpus needs a non-production override to exercise a supported behavioural
   mode.

Everything else SHALL be a release-pinned constant, an internally auto-tuned mechanical value, or
explicitly irrelevant to Tokeira's architecture. Tokeira SHALL NOT implement a generic
Temporal-compatible dynamic-config server, accept arbitrary raw Temporal keys in production, or
make `RuntimeConfig` TOML-configurable.

This preserves the close-to-zero-configuration product claim while making it falsifiable: every
Temporal key receives an explicit disposition, and every Tokeira policy field identifies the
behavioural reason it exists.

## Why a decision is needed now

The earlier `configuration.md` audit estimated the denominator at 564 Temporal dynamic-config
keys plus the static YAML topology. Its promised classification step has not happened. In the
meantime, individual conformance tiers made local decisions:

- authentication became a typed, presence-enabled `[policy.authorization]` surface;
- HTTP/JSON host and header policy became `[policy.http_api]`;
- standalone activities gained a typed compatibility switch;
- Nexus callback reachability gained Tokeira-native policy;
- limits were variously pinned, exposed as Tokeira quotas, or wired only through the
  conformance override registry;
- `UpdateTaskQueueConfig` began accepting and echoing queue and fairness policy, but the stored
  policy remained volatile and was not consumed by dispatch.

Those individual outcomes are mostly defensible, but the absence of a common doctrine has produced
three problems:

1. **Documentation overclaim.** [`supported.md`](../../../../docs/conformance/v1.31.0/supported.md) lists
   `UpdateTaskQueueConfig` as GA without saying that Tokeira currently implements setter/read-back
   only. Rate limits and fairness weights do not affect dispatch.
2. **Test policy is easy to mistake for product policy.** Raw Temporal key strings are read from
   the conformance-only override registry at several edge and runtime sites. That is valid test
   transport, but it does not make those keys production configuration.
3. **New features accidentally decide configuration architecture.** Task Queue Priority &
   Fairness cannot be implemented honestly until `matching.enableFairness`,
   `matching.priorityLevels`, public task-queue configuration, and stock defaults have explicit
   dispositions.

The configuration decision therefore belongs before, not inside, the priority/fairness
implementation.

## Compatibility boundary

### What conformance means

For a supported configuration mode, the public response, history, lifecycle, admission result, and
delivery tendency SHALL match what Temporal v1.31.0 would expose for the same effective policy.
Tokeira does not claim compatibility with:

- Temporal's file-based dynamic-config client;
- Temporal's key-value schema or selector syntax;
- gradual percentage rollout machinery;
- live subscriptions as a generic operator facility;
- Temporal's frontend/history/matching/worker service-specific tuning;
- Temporal's multi-cluster, replication, archival, or DLQ topology.

Configuration keys have no public wire identity. Behaviour does. A Tokeira-native field may
therefore represent a Temporal policy provided that its default, validation, scope, and observable
effect are documented.

### Empty configuration is the baseline

The empty-file guarantee is part of the compatibility contract:

- no policy section is required to boot an in-memory deployment;
- supported Temporal features use v1.31.0 stock defaults;
- optional configured modes are absent unless the operator chooses them;
- Tokeira-native extensions do not silently alter the stock baseline.

An exception requires its own recorded conformance decision. It cannot be introduced by a default
change hidden inside an implementation spec.

## Classification model

Every Temporal static or dynamic configuration item SHALL receive exactly one primary
classification. A key may additionally have a conformance-only alias, but that alias is test
transport rather than a second production classification.

| Classification | Meaning | Tokeira treatment |
|---|---|---|
| **Public API policy** | Temporal clients configure it through an RPC or another public wire surface. | Implement the API faithfully; validate, durably store, consume, and describe the policy. Do not duplicate it in TOML without a separate reason. |
| **Deployment policy** | A real operator must choose or may legitimately vary observable behaviour. | Expose a typed, documented `[policy.*]` field with the v1.31.0 default. Startup-static unless separately approved for reload. |
| **Pinned behavioural constant** | The value affects observable behaviour, but Tokeira does not support operator variation. | Pin the v1.31.0 default in the owning crate, document the source, and test the boundary. |
| **Auto-tuned mechanical setting** | The value controls throughput, batching, caching, concurrency, polling, or internal resource mechanics rather than the public contract. | Keep it inside `RuntimeConfig::default` or an adaptive controller. No production config field. |
| **Conformance-only override** | A corpus leaf must vary a value to exercise an implemented behaviour, but Tokeira does not expose the control to production operators. | Wire an allow-listed, typed key through `--features conformance`. Never compile the registry into production behaviour. |
| **Architecturally irrelevant or excluded** | The setting belongs to topology or behaviour Tokeira collapses or excludes. | Record the reason and owning exclusion/architecture reference. Add no no-op production field. |
| **Tokeira-native extension** | The policy has no Temporal equivalent but is required by Tokeira's product architecture. | Expose it explicitly as Tokeira policy and keep it outside the v1.31.0 conformance claim. |

### Classification tests

A proposed production field must answer all of these:

1. What observable or operational outcome changes?
2. Why is a pinned default or automatic mechanism insufficient?
3. Which plane owns the decision?
4. Is the setting authored at deployment time or through a public runtime API?
5. What is the v1.31.0 empty-config value?
6. Does changing it require restart, take effect live, or apply only to newly scheduled work?
7. How is it validated and surfaced back to the operator?

If those answers are absent, the field does not belong in `TokeiraConfig`.

## Effective-policy architecture

### One typed boundary

Tokeira SHOULD introduce one typed effective-policy boundary rather than allowing production code
to look up raw Temporal key strings.

Conceptually:

```text
v1.31.0 pinned defaults
        │
        ├── typed static TokeiraConfig policy
        │
        ├── durable public-API policy (for example task-queue config)
        │
        ├── break-glass emergency policy
        │
        └── conformance-only typed overlay (test builds only)
                         │
                         ▼
                 EffectivePolicy accessors
                         │
              edge / runtime / projection
```

The exact Rust type belongs in a Kiro design, but the boundary SHALL have these properties:

- production accessors are typed; they do not accept arbitrary string keys;
- Temporal key strings are confined to the conformance adapter and its allow-list;
- policy ownership is explicit by plane;
- edge validation and runtime consumption use the same effective value;
- the pure kernel never reads configuration or a policy provider;
- any kernel transition affected by policy receives already-resolved deterministic inputs;
- `RuntimeConfig` remains internal and default-only;
- rejected or unavailable settings fail explicitly rather than becoming silent no-ops.

This need not be one monolithic struct. Static cluster policy and a task-queue policy repository
have different lifetimes. “One boundary” means one doctrine and typed access path, not one lock or
one storage object.

### Proposed precedence

Where two sources can set the same effective behaviour, precedence SHALL be explicit:

1. release-pinned default;
2. typed static deployment policy;
3. more-specific durable public-API policy;
4. break-glass emergency restriction;
5. conformance-only overlay, in conformance builds only.

Higher layers may narrow or override lower layers only where the owning policy defines that
composition. Public API policy does not automatically defeat a safety emergency restriction.

### Scope and mutability

Tokeira SHOULD NOT reproduce Temporal's generic global/namespace/task-queue/shard selector model.
Instead:

| Policy source | Scope | Mutability |
|---|---|---|
| `tokeirad.toml` | Cluster/deployment | Startup-static; changing it requires a controlled restart unless a later spec adds reload |
| Namespace public APIs | Namespace | Live and durable according to the API contract |
| `UpdateTaskQueueConfig` | Namespace + task queue | Live and durable |
| Emergency policy | Cluster/deployment | Startup-static initially; always reported as break-glass |
| Conformance override registry | Whatever scope the corpus seam supplies | Live, test-process-only |
| Mechanical controllers | Internal ownership unit | Automatic and non-contractual |

Generic production hot reload is out of scope for this decision. A future reload mechanism must
define atomicity, partial-failure, validation, observability, and rollback rather than growing from
test-only override code.

## Task Queue Priority & Fairness — worked decision

Priority and fairness demonstrate how the classification works.

### v1.31.0 ground truth

The stock values in `common/dynamicconfig/constants.go @ v1.31.0` are:

| Temporal key | Default | Effective stock behaviour |
|---|---:|---|
| `matching.useNewMatcher` | `true` | Priority-capable matcher enabled |
| `matching.priorityLevels` | `5` | Priority keys 1–5; computed default key 3 |
| `matching.enableFairness` | `false` | Fairness keys and weights do not influence dispatch |
| `matching.autoEnableV2` | `false` | Seeing a priority/fairness key does not auto-enable fairness |
| `matching.enableMigration` | `true` | Internal matcher-backlog migration mechanism |
| `matching.maxFairnessKeyWeightOverrides` | `1000` | Public task-queue update admission limit |

`service/matching/task_queue_partition_manager.go @ v1.31.0` also disables fairness for sticky
queues even when the base fairness gate is enabled.

### Proposed Tokeira disposition

| Behaviour | Classification | Proposed treatment |
|---|---|---|
| Priority-capable delivery | Pinned behavioural constant | Always enabled |
| Five priority levels | Pinned behavioural constant | Fixed at five for the v1.31.0 compatibility profile |
| Fairness enablement | Deployment policy | Typed static task-queue policy, default `false` |
| Auto-enable on key observation | Pinned behavioural constant | `false`; Tokeira SHALL NOT infer enablement from key presence |
| Matcher/backlog migration | Architecturally irrelevant | No public setting; Tokeira has its own backlog format and migration rules |
| Maximum fairness-weight overrides | Pinned behavioural constant plus conformance alias | Default 1000; validate `UpdateTaskQueueConfig`; allow the existing test override |
| Per-key weights and dispatch limits | Public API policy | Authored through `UpdateTaskQueueConfig`, durably stored and consumed |

The proposed production shape is a typed field under task-queue policy, with final spelling chosen
by the implementation spec. For example:

```toml
[policy.task_queues]
enable_fairness = true
```

Absence remains stock-conformant: priority on, fairness off. The production field is cluster-wide
in the first version. Tokeira does not need Temporal's selector machinery merely because Temporal
can scope the source dynamic key more narrowly.

The conformance build SHOULD recognize `matching.enableFairness` as the test alias for the same
effective gate. That alias does not appear in production config and does not make raw `matching.*`
keys supported operator input.

### Behavioural consequences

- When fairness is disabled, fairness keys and task/queue weights are preserved but do not affect
  dispatch.
- When fairness is enabled, non-sticky task delivery uses fairness keys and effective weights;
  priority ordering remains the outer ordering dimension.
- Sticky dispatch remains outside fairness, matching v1.31.0.
- A task's effective weight is captured at schedule/publication time where the v1.31.0 contract
  requires it; later configuration changes do not retroactively rewrite already-dispatched work.
- Priority/fairness policy remains entirely in delivery. Lanes are unchanged. The kernel may copy
  already-captured priority metadata into pure `DispatchOp` values but SHALL retain no scheduler
  state and make no policy lookup.

An always-on fairness default is rejected by this proposal. It would be a deliberate divergence
from stock v1.31.0 and is unnecessary once a small typed production switch exists.

## `UpdateTaskQueueConfig`

### Current state

Tokeira currently:

- admits the RPC;
- accepts queue rate limit, fairness-key default rate limit, and fairness-weight overrides;
- merges set/unset updates;
- enforces the default maximum of 1000 fairness-weight overrides;
- stores the result in `InMemoryTaskQueueConfigStore`;
- returns and describes the stored fields.

It does **not** currently:

- persist the policy across process restart;
- apply queue or fairness-key rate limits;
- resolve weight overrides during scheduling/dispatch;
- populate priority-band statistics from the effective backlog.

The existing implementation is therefore **partial**, not fully supported policy. The runtime
module documentation also says reads occur on the dispatch path, but no such consumer exists
today; that wording must be corrected or fulfilled.

### Proposed target

Because `UpdateTaskQueueConfig` is public API policy, completion requires:

1. faithful v1.31.0 validation and merge semantics;
2. durable storage keyed by namespace and task-queue identity;
3. live consumption by the queue-home delivery path;
4. weight resolution and schedule-time capture;
5. queue and per-key dispatch-rate enforcement;
6. `DescribeTaskQueue` projection of effective config and priority statistics;
7. restart tests proving the accepted policy survives;
8. explicit treatment of concurrent updates and stale writes.

The policy repository is not workflow history and does not belong in the kernel. It is durable
delivery-plane configuration. A DSQL repository may own it without making a queue write
authoritative for workflow correctness.

The production TOML SHALL NOT duplicate per-queue rates or weight overrides. Those values already
have a public, appropriately scoped authoring API.

Until this target lands, [`supported.md`](../../../../docs/conformance/v1.31.0/supported.md) should identify
`UpdateTaskQueueConfig` as partial, or distinguish “RPC admitted and echoed” from “policy
enforced.”

## Conformance override boundary

The override bridge exists to let an out-of-process test corpus request modes that Temporal's
in-process test cluster would normally obtain from `OverrideDynamicConfig`. It SHALL remain:

- compiled only under `--features conformance`;
- allow-listed by exact key and value type;
- consumed through typed feature accessors;
- reset between leaves and process runs;
- unavailable as a production control plane;
- incapable of changing kernel behaviour through a process-global lookup inside the kernel.

Three different claims must not be conflated:

1. **Default conformance:** empty production config matches stock v1.31.0.
2. **Configured behavioural conformance:** Tokeira has a production policy/API that can select the
   same mode.
3. **Test-only behavioural capability:** the engine can exercise the mode through a conformance
   overlay, but production does not expose it.

The readiness ledger and decision records SHOULD name which claim supports each suite. A green
test using a conformance-only key is not evidence that production operators can configure that
key.

Runtime mutation inside a corpus leaf is acceptable test transport only when the expected
transition is intentionally in the supported behavioural surface. Tests whose subject is
Temporal's generic dynamic-config loader, selector precedence, gradual rollout, or subscription
mechanism are outside this proposal's compatibility claim.

## Initial classification examples

These examples establish the intended cut; they are not a substitute for classifying all 564
keys.

| Temporal/Tokeira setting | Classification | Reason |
|---|---|---|
| `frontend.httpAllowedHosts` | Deployment policy | Existing typed `[policy.http_api]`; affects public HTTP admission |
| `frontend.enablePrincipalPropagation` | Deployment policy plus conformance alias | Existing auth policy controls durable principal attribution |
| `frontend.exposeAuthorizerErrors` | Deployment policy plus conformance alias | Existing typed authorization policy |
| `activity.dispatch` | Deployment compatibility policy plus conformance alias | Existing standalone-activity switch; default preserves v1.31.0 baseline |
| `matching.enableFairness` | Deployment policy plus conformance alias | Operators need a way to enable the supported configured mode |
| `matching.priorityLevels` | Pinned behavioural constant | Five levels at the target; no demonstrated production need for variation |
| `matching.maxFairnessKeyWeightOverrides` | Pinned behavioural constant plus conformance alias | Public API validation boundary, default 1000 |
| `matching.numTaskqueueReadPartitions` | Architecturally irrelevant | Tokeira has a queue-home broker rather than Temporal matching partitions |
| `matching.getTasksBatchSize` | Auto-tuned mechanical setting | Internal backlog-drain mechanics |
| `history.cacheTTL` | Auto-tuned mechanical setting | Tokeira runtime cache mechanics, not API behaviour |
| `history.defaultActivityRetryPolicy` | Pinned behavioural constant unless separately exposed | Observable defaulting belongs to the release profile |
| `limit.blobSize.error` | Pinned behavioural constant or typed deployment quota | Must be decided by whether operators need supported variation; never a raw key |
| replication/standby/XDC keys | Architecturally irrelevant or excluded | Multi-cluster behaviour is outside the current surface |
| `nexus_completion.system_callback_url` | Tokeira-native extension | Reachability is operationally mandatory in Tokeira's HTTP callback architecture |
| emergency stickiness/projection/poll controls | Tokeira-native extension | Break-glass restrictions, not Temporal dynamic config |

The ambiguous rows, such as size/count limits, are where owner/product judgement is genuinely
needed. The classification exercise should not prejudge every limit as a knob merely because
Temporal exposes one.

## Classification ledger deliverable

The Temporal configuration reference SHOULD remain the human-readable denominator, but its
“next step” must become a checked ledger rather than another prose promise.

The owning spec should choose a machine-checkable source format and require:

- one row for every one of the 564 dynamic keys;
- one row or section for every relevant static YAML field group;
- the Temporal default and scope;
- one primary classification from this proposal;
- the Tokeira effective value or field/API path;
- the owning crate/spec/decision;
- whether conformance override support exists;
- verification evidence;
- no duplicate or unclassified keys;
- generated summary counts in `configuration.md`.

A Markdown rendering may be generated from a small structured manifest if that keeps completeness
testable. The manifest is documentation metadata, not a runtime dynamic-config registry.

## Documentation and operator surface

This decision unblocks the existing readiness work in
[`docs/readiness/configuration.md`](../../../../docs/readiness/configuration.md):

- create one canonical annotated `config.example.toml`;
- show the empty in-memory file and minimal DSQL configuration;
- identify fields that are operationally load-bearing;
- mark every field as stock parity, configured parity, or Tokeira-native;
- document startup-static semantics;
- document public runtime policy separately from deployment TOML;
- link the classified Temporal denominator as evidence for the close-to-zero claim.

The example config must include the authorization issuer-routing warning already required by the
authorization decision and the Nexus callback reachability warning. If fairness is approved, its
field must say plainly that absence matches stock v1.31.0: priority remains enabled; fairness
remains disabled.

## Alternatives rejected by this proposal

### Reproduce Temporal's dynamic-config system

Rejected. It adds hundreds of raw keys, selector precedence, polling/subscriptions, rollout
semantics, and service-topology concepts that Tokeira deliberately collapses. It would make
close-to-zero configuration false without improving public API compatibility.

### Hardcode every stock default and expose no configured modes

Rejected. Authentication, HTTP admission, callback reachability, fairness, quotas, and public
task-queue policy have legitimate configured use cases. Default-only compatibility is insufficient
for a production engine when the configured mode is a supported product requirement.

### Accept arbitrary Temporal keys and ignore unknown or irrelevant ones

Rejected. Silent no-ops are hostile to operators and contradict `serde(deny_unknown_fields)`.
Accepting a key implies ownership of its semantics.

### Use conformance overrides as the production policy system

Rejected. The registry is process-global test transport with corpus-driven mutation semantics. It
is not a validated, documented, durable, or supportable operator interface.

### Enable fairness whenever a fairness key is present

Rejected. `matching.autoEnableV2` defaults to `false` in v1.31.0. Key presence is data, not operator
consent to change delivery policy.

### Enable fairness by default

Rejected unless recorded as an explicit divergence. It contradicts stock v1.31.0 and is unnecessary
when an opt-in typed policy exists.

## Implementation sequence after approval

1. **Record the decision.** Move this proposal into a resolved decision record; amend
   the public [`README.md`](../../../../docs/conformance/v1.31.0/README.md),
   [`supported.md`](../../../../docs/conformance/v1.31.0/supported.md), and
   [`excluded.md`](../../../../docs/conformance/v1.31.0/excluded.md) as applicable.
2. **Create the owning Kiro spec.** Define the classification manifest, effective-policy boundary,
   config validation, documentation generation/checking, and migration from scattered raw-key
   reads. This requires the explicit spec-edit authorization and snapshot prescribed by
   `AGENTS.md §6`.
3. **Correct current overclaims.** Mark `UpdateTaskQueueConfig` partial until durable enforcement
   lands and correct any runtime comments that describe a nonexistent dispatch consumer.
4. **Complete the ledger.** Classify all 564 keys and static groups, with completeness checks.
5. **Land the typed policy boundary.** Preserve existing behaviour while routing production and
   conformance inputs through typed accessors.
6. **Implement Task Queue Priority & Fairness.** Add the default-off production policy, the
   conformance alias, priority/fairness delivery, and durable public task-queue policy.
7. **Finish the operator reference.** Add the canonical annotated example and update readiness.

Priority/fairness should not wait for all 564 rows to produce code, but its relevant keys and the
effective-policy boundary must be approved first. The remaining ledger can proceed as a parallel
documentation audit once the categories are stable.

## Consequences

### Benefits

- preserves stock default behaviour with an empty config;
- keeps the production surface small, typed, and supportable;
- makes configured conformance claims honest;
- prevents test-only controls from leaking into product policy;
- gives priority/fairness a clear enablement model;
- preserves kernel purity and lane independence;
- turns the 564-key contrast into evidence rather than rhetoric.

### Costs

- requires a complete classification audit;
- introduces a typed policy-access refactor across current direct override reads;
- requires durable task-queue policy storage;
- adds at least one production policy field for fairness;
- makes partial surfaces and intentional exclusions more visible.

Those costs already exist as ambiguity and scattered implementation. This proposal makes them
finite and reviewable.

## Owner review questions

The proposal recommends “yes” to questions 1–5 and “structured manifest rendered to Markdown” for
question 6:

1. Is behavioural compatibility, rather than key/control-plane compatibility, the governing
   doctrine?
2. Must empty `tokeirad.toml` preserve stock v1.31.0 defaults?
3. Should production configuration remain startup-static, with no generic hot reload?
4. Should fairness be an opt-in typed deployment policy, default `false`, while priority remains
   always enabled at five levels?
5. Should public `UpdateTaskQueueConfig` policy become durable and fully enforced, with no TOML
   duplicate?
6. Should the 564-key classification live in a machine-checkable structured manifest with a
   generated human-readable rendering, or directly in Markdown with a completeness checker?
7. Should `supported.md` be corrected immediately to label `UpdateTaskQueueConfig` partial, or in
   the same change that lands enforcement?

Approval of these principles authorizes specification, not implementation. The Kiro spec should
still expose exact type names, storage schema, reload semantics, migration compatibility, and
property-based tests before code changes begin.
