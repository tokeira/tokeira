# Requirements Document: Explanation Change Semantics

## Introduction

This spec covers **Feature 2 (Change Semantics in the Kind Library)** from the umbrella
[`operator-explanation`](../operator-explanation/requirements.md). It answers the question
an operator asks immediately after "what will change?" — namely **"and what will that cost
me?"** — by having each resource kind declare what a change to it actually does: whether
it happens in place, whether it replaces, whether it interrupts, what happens to the data,
and whether it can be undone.

The declaring kind is the only honest source for this. A generic layer cannot know whether
updating a load-balancer listener interrupts connections; the kind that owns the provider
can, from that provider's documentation. This is why the umbrella places provider
knowledge in the kind library (decision D3) and why every declaration carries a citation.

The scope is:

1. **A declaration point** — `Resource::change_semantics`, defaulting to fully unknown.
2. **The semantic vocabulary** — lifecycle operation, replacement policy, disruption,
   data effect, reversibility, each wrapped in a confidence that carries its citation.
3. **Transport** — declared semantics travel from the engine's change computation to the
   explanation model.
4. **Impact derivation** — the operational impacts Feature 1 left empty become a
   deterministic function of declared semantics.
5. **Uncertainty activation** — the `SemanticsUndeclared` reason Feature 1 defined and
   deliberately did not emit comes into force here, now that some fields are stated and
   an unstated one is a real gap.
6. **Kind coverage** — every `Resource` implementation in the workspace accounted for:
   declared, or deliberately defaulted with the reason recorded.
7. **Golden tests** — per declaring kind, across the six change scenarios.

### Amendment to Feature 1's design

Writing this spec surfaced a placement error in
[`explanation-evidence-model/design.md`](../explanation-evidence-model/design.md), which
locates `ChangeSemantics` and `Confidence<T>` in `tokeira-explain`. That is not
implementable: the declaration point is a method on `tokeira_iac::Resource`, and
`tokeira-iac` cannot depend on `tokeira-explain` (which depends on it). The precise edits:

- **Move** `ChangeSemantics`, `Confidence<T>`, `LifecycleOperation`, `ReplacementPolicy`,
  `Disruption`, `DataEffect`, and `Reversibility` from `tokeira-explain` to
  `tokeira-iac`; `tokeira-explain` re-exports them so its public surface is unchanged.
- **Keep** `Cause`, `SourceLocation`, `Uncertainty`, and the evidence types in
  `tokeira-explain` — those are computed by explanation, not declared by kinds.
- **Amend** Feature 1's `PlanOutcome` to carry `semantics_by_id: BTreeMap<ResourceId,
  ChangeSemantics>` alongside `refresh`, following the join-by-id pattern already
  established there. Feature 1 populates it empty; this feature fills it.

Feature 1's behaviour is otherwise unchanged: with no kind declaring, every value is
`Unknown` and the renderer stays silent.

### What This Spec Covers

- The `change_semantics` method on `Resource`, its default, and its purity contract.
- The semantic types, relocated to `tokeira-iac` per the amendment above.
- Population during change computation, including deletes reached through the recovery
  seam.
- Impact derivation from declared semantics.
- `SemanticsUndeclared` uncertainty activation.
- Declarations for the Tier 1 and Tier 2 kinds in the inventory below, each with a
  provider citation.
- Golden tests for every declaring kind.

### What This Spec Does NOT Cover

- Why a change is present (Feature 3, `explanation-causality`) — semantics say what a
  change does, not what caused it.
- Dependant sets and blast radius (Feature 3).
- Source spans (Feature 4).
- Declarations for Tier 3 kinds — they remain deliberately unknown until their platform is
  operator-exercised, and the inventory records that as a decision.
- Any change to which changes the engine computes or how destructiveness is classified.

## Evidence From Current Code

| Fact | Anchor | Consequence |
|---|---|---|
| `Resource` is consumed as `&dyn Resource` throughout the engine (`resource_map: HashMap<ResourceId, &dyn Resource>`) | `crates/tokeira-iac/src/engine.rs` | The declaration point must be a dyn-compatible method on `Resource`, not a separate trait requiring downcast |
| `compute_changes(desired, &ctx.state, ctx)` holds the desired resources while producing changes | `crates/tokeira-iac/src/engine.rs` | Semantics can be collected exactly where changes are computed, with no second pass |
| `ResourceRecovery` reconstructs a `Resource` from recorded state for resources no longer in the definition | `crates/tokeira-iac/src/lib.rs` | Deletions of removed resources can still be explained by their own kind |
| `ChangeKind::is_destructive` classifies Delete and Replace; the destructive set drives the apply gate | `crates/tokeira-iac/src/types.rs` | Destructiveness stays engine-owned; declarations enrich it and never override it |
| `FieldDiff` carries the changed field with before/after, or a named observation | `crates/tokeira-iac/src/types.rs` | A kind can decide semantics from *which* fields changed, not merely that something did |
| 39 `Resource` implementations exist across `crates/` and `platforms/`, of which 3 are test-only | workspace-wide (`impl Resource for`) | The accounting surface for this spec |
| AWS resources already encode replacement knowledge informally in `diff` (e.g. VPC CIDR "requires replacement", ECS "task definition manifest changed") | `crates/tokeira-aws/src/resources/*.rs` | Some semantics exist today as prose inside diff details; this spec gives them a typed home |

## Kind Inventory and Declaration Policy

Every `Resource` implementation is accounted for. Tier 1 and Tier 2 declare in this spec;
Tier 3 defaults deliberately; test kinds never declare.

| Tier | Kinds | Policy |
|---|---|---|
| **Tier 1 — operating set** | `ComposeService`, `ObservabilityConfigFilesResource` (the compose platform), `LocalStateDirResource` | SHALL declare every field with a citation or an explicit engine-fact justification. These run on the platform operators use today. |
| **Tier 2 — storage path** | `DsqlCluster`, `DynamoDbTable` | SHALL declare. Storage transitions are the highest-stakes explanation Tokeira produces; an unexplained DSQL change is the worst possible silence. |
| **Tier 3 — unexercised providers** | The remaining 24 AWS kinds (`Ec2Instance`, `EcsServiceResource`, `TaskDefinitionResource`, `EksClusterResource`, `Alb*`, `Iam*`, `S3*`, `SecurityGroup`, `Vpc*`, `SsmParameterResource`, `SecretsManagerSecret`, `EbsVolume`, `Ecr*`, `Asg*`, `CapacityProvider*`, `LaunchTemplate*`, `CloudMapNamespace*`, `PodIdentityAssociation`, `DsqlConnectionEndpoint`, `VpcEndpoint`, `IamInstanceProfile`), `NamespaceResource` (k8s), and the three `tokeira-remote-workstation` kinds *(the three legacy `platforms/compose` kinds left this table when that crate retired, 2026-07-30 — the platform's successor owns `platforms/compose` now)* | SHALL default to unknown. Declaring provider semantics for a platform no operator currently exercises would be asserting facts nobody has verified. |
| **Test kinds** | `StubResource`, `TestResource`, `NumberedResource` | SHALL NOT declare — a test double has no provider to be honest about. |

## Glossary

Terms additional to the umbrella and Feature 1 glossaries:

- **Declaration** — a kind's statement of what a change to it does, expressed as
  `ChangeSemantics`.
- **Lifecycle Operation** — how the provider effects the change: created, updated in
  place, replaced, deleted.
- **Replacement Policy** — whether the change requires destroying and recreating the
  resource, and if so whether the replacement is created before the original is destroyed.
- **Disruption** — the expected availability effect: none, rolling, brief interruption,
  unavailable for the duration, or unknown.
- **Data Effect** — what happens to data the resource holds: preserved, migrated,
  destroyed, or none held.
- **Reversibility** — whether applying the inverse change restores the prior state:
  reversible, reversible with data loss, irreversible, or unknown.
- **Citation** — the provider documentation reference that establishes a declared
  behaviour. Required by construction for any `ProviderGuarantee`.
- **Tier** — a kind's declaration obligation under the inventory above.

## Requirements

### Requirement 1: A dyn-compatible declaration point with an honest default

**User Story:** As a kind author, I want one obvious place to state what a change to my
resource does, so that operator comprehension is part of implementing a resource rather
than a separate project.

#### Acceptance Criteria

1. THE `Resource` trait SHALL expose a `change_semantics` method returning
   `ChangeSemantics`.
2. THE `change_semantics` method SHALL be callable through `&dyn Resource`.
3. THE `change_semantics` method SHALL receive the recorded state where one exists, the
   change kind, and the computed field differences.
4. WHERE a kind does not override `change_semantics` THE default implementation SHALL
   return unknown for every field.
5. THE `change_semantics` method SHALL be pure: it SHALL NOT perform I/O, SHALL NOT call a
   provider, and SHALL NOT mutate the resource or the context.
6. THE `change_semantics` method SHALL be total: it SHALL return a value for every change
   kind and every field-difference set without panicking.

*Amendment (2026-07-29):* WHERE the kind can state a change's mechanism more precisely
than the vocabulary's generic rendering, the declaration MAY carry a single kind-authored
statement (one sentence of operator prose, e.g. "it would be stopped, removed, and
recreated from the definition"); absence falls back to the generic template. The
statement is part of the declaration and carries the same research obligation.

### Requirement 2: Confidence is explicit and citations are structural

**User Story:** As an operator, I want to know whether Tokeira is telling me something the
provider guarantees or something Tokeira inferred, so that I can calibrate how much weight
to put on it.

#### Acceptance Criteria

1. THE semantic vocabulary SHALL express each field as a value paired with one of:
   unknown, inference, engine fact, or provider guarantee.
2. WHERE a field is declared as a provider guarantee THE declaration SHALL carry a
   citation identifying the provider documentation establishing it.
3. THE type SHALL make a provider guarantee without a citation unrepresentable.
4. THE unknown confidence SHALL be the default value of every field.
5. IF a behaviour can neither be established from provider documentation nor derived
   from documented facts THEN the kind SHALL declare unknown; a conclusion derived from
   documented facts SHALL be declared as inference, never presented as a guarantee.
6. WHERE a behaviour follows from Tokeira's own engine rather than the provider THE
   declaration SHALL use engine fact and SHALL NOT claim a provider guarantee.
7. THE citation type SHALL distinguish a code citation (module identity) from a
   product-documentation citation carrying a title, a URL, and optionally the
   establishing quote — so a documentation reference is machine-usable and renders as a
   link. *(Amended 2026-07-29: product-doc references become first-class in lifecycle
   annotations.)*
8. THE inference confidence SHALL carry a citation identifying the documented facts it
   derives from; an uncited inference SHALL be unrepresentable, exactly as for the other
   cited tiers. *(Amended 2026-07-29.)*

### Requirement 3: Declarations reach the explanation unaltered

**User Story:** As an operator, I want the report to tell me exactly what the kind
declared, so that no layer between the provider and my terminal quietly upgrades a guess
into a fact.

#### Acceptance Criteria

1. WHEN the engine computes changes THE engine SHALL collect the declared semantics for
   each changed resource.
2. THE plan outcome SHALL carry the collected semantics keyed by resource id.
3. WHEN a resource is deleted and is reachable through the recovery seam THE engine SHALL
   collect that resource's declared semantics.
4. IF a deleted resource cannot be recovered THEN THE collected semantics SHALL be unknown
   and the explanation SHALL record the reason.
5. THE explanation model SHALL carry declared semantics verbatim, without substitution,
   inference, or promotion to a higher confidence.
6. THE explanation model SHALL NOT derive semantics for a resource whose kind declared
   none.
7. WHERE no change is computed for a resource THE explanation SHALL carry no semantics for
   it.

### Requirement 4: Destructiveness remains engine-owned

**User Story:** As an operator, I want the destructive-action set to be decided by the
engine, so that a kind's optimism can never quietly disarm the confirmation gate.

#### Acceptance Criteria

1. THE explanation model SHALL derive its destructive-action set from the engine's change
   classification.
2. THE explanation model SHALL NOT add a change to the destructive set because of a
   declaration, and SHALL NOT remove one because of a declaration.
3. WHEN a change is destructive THE explanation SHALL present its declared data effect and
   reversibility alongside it.
4. WHERE a destructive change's data effect is unknown THE explanation SHALL record an
   uncertainty naming that gap.
5. THE explanation SHALL NOT describe any change as safe, non-disruptive, or reversible
   unless a declaration states it with engine-fact or provider-guarantee confidence.

### Requirement 5: Operational impacts are derived deterministically

**User Story:** As an operator, I want the plan to tell me its consequences in operational
terms, so that I understand the effect on my running system rather than on a resource
graph.

#### Acceptance Criteria

1. THE explanation model SHALL derive operational impacts from the declared semantics of
   its changes.
2. WHEN one or more changes declare a disruption other than none THE explanation SHALL
   emit one impact per distinct disruption class, naming the affected resources.
3. WHEN one or more changes declare that data is destroyed THE explanation SHALL emit an
   impact naming those resources and the irreversibility of the loss.
4. WHEN one or more changes declare a required replacement THE explanation SHALL emit an
   impact naming the replaced resources.
5. WHERE every change declares unknown semantics THE explanation SHALL emit no impacts and
   SHALL record the absence as uncertainty rather than as an absence of consequence.
6. THE derivation SHALL be a pure function of the declared semantics: two plans with
   identical declarations SHALL produce identical impacts.
7. THE impacts SHALL be ordered deterministically, most consequential first.

### Requirement 6: Undeclared semantics become uncertainty

**User Story:** As an operator, I want an unstated consequence to appear as a gap rather
than as silence, so that I can tell "this is safe" from "nobody said".

#### Acceptance Criteria

1. WHEN a change's declared field is unknown AND the report states that field for other
   changes in the same plan THE explanation SHALL record a `SemanticsUndeclared`
   uncertainty naming the resource and the field.
2. WHERE every change in a plan declares unknown for a field THE explanation SHALL record
   one uncertainty for the plan rather than one per change.
3. THE uncertainty SHALL name the resolving action by its concrete identifiers — the
   field, the resource type, and the declaration point — never as generic advice.
   *(Amended 2026-07-29, operator-directed: concrete and realisable.)*
4. THE explanation SHALL NOT record an undeclared-semantics uncertainty for a change kind
   that the field does not apply to.
5. THE undeclared-semantics uncertainties SHALL be carried in the model and the artifact
   for machine consumers (agents, CI gates); narrative output SHALL NOT render them.
   The narrative states established behaviour only — **knowledge renders; gaps
   enforce** — and Requirement 7's coverage enforcement is the demand-side guarantee
   that first-party kinds never ship gaps. *(Amended 2026-07-29, operator-directed:
   Tokeira states what it knows; authors research their contributions fully.)*

### Requirement 7: Kind coverage is accounted for and enforced where it matters

**User Story:** As a maintainer, I want every resource kind to have a recorded declaration
posture, so that silence is always a decision and never an oversight.

#### Acceptance Criteria

1. THE kind inventory SHALL account for every `Resource` implementation in the workspace.
2. WHERE a kind is Tier 1 or Tier 2 THE kind SHALL declare every applicable field above
   unknown confidence — engine fact, provider guarantee, or cited inference. *(Amended
   2026-07-29, consistent with 2.8: a researched declaration may honestly be an
   inference; the enforcement bar is above-unknown, as Property 9 states.)*
3. WHERE a kind is Tier 3 THE kind SHALL retain the unknown default and the inventory
   SHALL record that as deliberate.
4. WHEN a new `Resource` implementation is added THE inventory SHALL be updated in the
   same change.
5. THE test suite SHALL assert that every Tier 1 and Tier 2 kind declares its applicable
   fields.

### Requirement 8: Golden explanation tests per declaring kind

**User Story:** As a maintainer, I want a kind's declared semantics to be a maintained
surface, so that operator comprehension cannot regress silently.

#### Acceptance Criteria

1. WHERE a kind declares semantics THE kind SHALL carry golden tests covering creation,
   in-place update, replacement, deletion, drift-driven update, and the unknown case.
2. THE golden tests SHALL assert the semantic classification and its confidence, and SHALL
   NOT assert rendered prose.
3. WHEN a declaration changes THE corresponding golden test SHALL fail until updated.
4. WHERE a scenario does not apply to a kind THE test SHALL assert that the kind reports
   it as inapplicable rather than omitting the case.

### Requirement 9: Rendering declared semantics

**User Story:** As an operator, I want consequences at a glance and evidence on request,
so that the report respects my attention.

#### Acceptance Criteria

*(The document form and line templates are owned by
[output-templates.md](../operator-explanation/output-templates.md).)*

1. WHEN rendering at summary depth THE renderer SHALL state the operational impacts as
   an `## Impacts` section — one templated line per subject, severity-first, speaking
   descriptive names only — and SHALL NOT enumerate per-change semantics.
2. WHEN rendering at detail depth THE renderer SHALL state each change's declared
   behaviour as templated would-mood prose beneath the change's line, in the
   declaration's confidence voice.
3. THE confidence voices SHALL be: an engine fact speaks plainly in the engine's own
   voice; a provider guarantee attributes itself ("AWS documents that …"); an inference
   owns itself ("Tokeira derives this from …"). No scaffolding labels (`note:`, `help:`)
   appear.
4. WHERE a field's confidence is unknown THE renderer SHALL omit the field from narrative
   output.
5. THE `--json` rendering SHALL carry every declared field with its confidence and
   citation regardless of depth.
6. THE renderer SHALL use only vocabulary defined in `operator-language.md`, extending the
   lexicon in the same change where this feature introduces terms.
7. WHEN rendering at detail depth THE renderer SHALL render each claim's citation —
   a product-documentation citation as a Markdown link titled by the document, a code
   citation as a code span.
8. THE impact statement templates SHALL specialize on the subject's change kind (an
   unavailability reads "would be unavailable while the change applies" for an update
   and "would no longer be available" for a deletion) and SHALL state irreversibility
   where every contributing declaration establishes it.

## Notes

- The tiering exists because this spec's failure mode is not omission but **fabrication**.
  Twenty-four unexercised AWS kinds could each be given plausible-sounding semantics in an
  afternoon; several would be wrong, and a wrong provider guarantee is worse than an
  admitted unknown. Tier 3 stays unknown until someone runs the platform and reads the
  provider's documentation.
- Requirement 4 exists to keep the confirmation gate honest: the destructive set already
  gates `apply --yes`, and a kind that declared "reversible, no data effect" must never be
  able to influence it.
- Requirement 6.2 (one uncertainty per plan when nothing is declared) is deliberate noise
  control: the alternative is an uncertainty per change on every plan, which is the
  behaviour Feature 1 explicitly avoided by not emitting this reason at all.
