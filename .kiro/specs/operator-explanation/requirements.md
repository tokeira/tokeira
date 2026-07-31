# Requirements Document: Operator Explanation

## Introduction

Tokeira's provisioning engine knows a great deal that it never tells the operator. It
knows which resources will change, which changes destroy data, which live state it could
not confirm, which resource depends on which, and which definition revision produced the
current desired state. Today all of that collapses into counts and resource ids:

```text
infra plan: 3 to update (4 unchanged)
  ~ grafana::compose/grafana  (compose_service)
```

The operator is left to answer *why*, *what else*, and *what will it cost me* from
memory and inference. This umbrella spec covers the work that closes that gap: a
**deterministic explanation layer** that turns the engine's own semantic knowledge into
an explanation-grade model, rendered for humans, serialized for machines, and — only at
the far end, and only as one renderer among several — narratable by an agent.

The governing principle, in three clauses:

> **The engine establishes truth. The explanation layer establishes meaning. An agent
> only helps the operator navigate it.**

This is deliberately the inverse of "add an LLM to a plan diff". Every fact an operator
reads is computed by Rust from engine state, kind-library semantics, and the dependency
graph. No language model is required to produce any part of the default experience, and
no language model is permitted to originate a fact.

### Behavioural authority

`AGENTS.md §8` (Temporal ground truth) **does not apply to this spec**: explanation is
Tokeira-native surface with no Temporal analog. Its authorities are instead:

1. **The engine's own semantics** — `tokeira-iac` state, diff, refresh, and dependency
   ordering are authoritative for what changes and in what order.
2. **The provider's documented behaviour** — for any claim about *how* a provider applies
   a change (in place, rolling, replacement, interrupting, irreversible), the citation is
   that provider's own documentation, recorded in the kind implementation. A provider
   fact that cannot be cited SHALL be reported as unknown rather than guessed. This rule
   exists because the failure mode this spec most needs to prevent is a confident,
   plausible, wrong statement about someone else's infrastructure.
3. **The operator experience constitution** —
   [operator-output-contract.md](../../../docs/platforms/operator-output-contract.md)
   (report shape: depth, form, collapse rule) and
   [operator-language.md](../../../docs/platforms/operator-language.md) (the lexicon and
   the banned internal vocabulary). Explanation output is bound by both.

### Feature decomposition and dependency order

The work is organized into six features, each with its own child spec carrying design
and tasks:

- **Feature 1 (Evidence Model and Explanation IR)** — no dependencies — child spec
  `explanation-evidence-model`
- **Feature 2 (Change Semantics in the Kind Library)** — depends on Feature 1 — child
  spec `explanation-change-semantics`
- **Feature 3 (Causality)** — depends on Feature 1; strengthened by Feature 2 but not
  blocked by it — child spec `explanation-causality`
- **Feature 4 (Source Attribution)** — depends on Feature 3 — child spec
  `explanation-source-spans`
- **Feature 5 (Analysis Protocol)** — depends on Features 1, 3, and 4: bundles retain
  Feature 3's desired snapshots, and revision comparison composes Feature 4's syntactic
  diff; Feature 2 enriches bundle content but gates nothing *(dependency corrected
  2026-07-30 to match the child's retention architecture)* — child spec
  `explanation-analysis-protocol`
- **Feature 6 (Agent Clients)** — depends on Feature 5 — child spec
  `explanation-agent-clients`

Features 1–3 deliver the entire default operator experience with no network, no
credentials, and no model. Features 5–6 are strictly additive consumers.

During spec review, the analysis excerpt surface made a latent defect concrete: the
shipped template carries `admin_password: "admin"` in `config()`, and every explanation
surface would have served it as authored. The resolution is **not** a redaction layer
inside this umbrella (a drafted cross-cutting child to that effect was withdrawn as
treating the symptom): it is decision D7 below — secrets do not belong in definitions at
all.

**Uncertainty is not a feature; it is a requirement of Feature 1.** It is modelled and
rendered from the first slice, because a report that hides what the engine could not
determine is less trustworthy than one that says so plainly — and because the engine
already computes this and discards it (see *Evidence From Current Code*).

## Evidence From Current Code

The facts this spec builds on, verified in the tree at the time of writing:

| Fact | Anchor | Consequence |
|---|---|---|
| Refresh already classifies per-resource confirmation status, including `RefreshStatus::Unknown` when a `describe` cannot confirm existence | `crates/tokeira-iac/src/engine.rs` (`refresh_state`, `RefreshReport`) | The engine computes uncertainty today |
| `RefreshReport.status_by_id` is annotated `#[allow(dead_code)] // status_by_id retained for diagnostic reporting` | `crates/tokeira-iac/src/engine.rs` | …and no consumer reads it. Uncertainty is computed, retained, and never surfaced |
| `DescribeResult::Unsupported` is a first-class describe outcome the engine deliberately refuses to treat as absence | `crates/tokeira-iac/src/lib.rs`; delete/prune paths in `engine.rs` | "Cannot confirm" is already a modelled state, not an error |
| `InternalChange::Update`/`Replace` carry `Vec<FieldDiff>` end to end; the flattener passes evidence through untouched | `crates/tokeira-iac/src/lib.rs`, `engine.rs` (`internal_change_to_flat`) | Field-level evidence transport exists; explanation extends it rather than inventing it |
| `FieldDiff::observation` marks a named change whose values were not captured | `crates/tokeira-iac/src/types.rs` | The fact/no-value distinction is already representable |
| `ChangeKind::is_destructive` classifies Delete and Replace; `destructive_changes`/`plan_is_destructive` expose the set | `crates/tokeira-iac/src/types.rs` | Destructiveness is engine-classified today and gates apply |
| Every applied definition is retained per revision at `{dir}/state/config-revisions/{n}/{basename}`, with `snapshot` and `restore` | `crates/tokeira-provisioner-cli/src/config_history.rs` | Definition-vs-definition comparison is available without new storage |
| Resources declare `dependencies()`; the engine topologically orders create and reverse-orders delete | `crates/tokeira-iac/src/engine.rs` | The dependency graph needed for cascade reasoning already exists |
| `ResourceRecovery` reconstructs a deletable resource from recorded state | `crates/tokeira-iac/src/lib.rs`; registered by `platforms/compose/src/provisioner.rs` *(path updated 2026-07-30 with the platform rename)* | Resources removed from the definition remain explainable, not just deletable |
| The definition is interpreted by `syn` with located parse errors; values flow through `Value<Host>` evaluation into resource structs | `crates/tokeira-tkd/src/lib.rs`, `eval.rs` | Spans exist at parse time and are **discarded during evaluation** — the gap Feature 4 closes |

## Target State

**In scope across the umbrella:**

- A versioned, serializable explanation model produced by every plan, and by every apply
  as the record of what it did.
- Per-change semantics contributed by the resource kind that owns the provider: lifecycle
  operation, replacement policy, disruption expectation, data effect, reversibility, and
  the confidence of each.
- Causal classification for every change: definition edit, dependency output change,
  provider drift, replacement cascade, or engine advance — with causal grouping so an
  operator sees one story rather than five unrelated lines.
- First-class uncertainty: what the engine could not determine, why, what follows from
  it, and what would resolve it.
- Deterministic rendering under the existing output contract (summary / `--detail` /
  `--json`), and a stable JSON artifact.
- A read-only analysis surface over that artifact, consumable by agent tooling.
- Optional agent narration that is visibly separated from verified fact.

**Out of scope for the umbrella** (each may return as its own spec):

- Any language-model dependency inside `tkp`, in any feature, at any time.
- A multi-provider model gateway, provider routing, free-tier arbitrage, or credential
  management for consumer subscriptions.
- Model-generated risk assessment, model-generated destructiveness classification, or
  any model-originated fact.
- Conversation persistence, agent memory, or multi-turn state owned by Tokeira.
- Graphical rendering (a GraphViz or web renderer is a later consumer of the same model).
- Cost estimation and policy/compliance evaluation.

## Surface Accounting

Explanation is a total function over the engine's outcome vocabulary: every variant below
has a defined explanation obligation, so no engine outcome can silently render as nothing.

### Change kinds

| `ChangeKind` | Explanation obligation |
|---|---|
| `Create` | Kind-declared creation semantics; provider-assigned values flagged as uncertain until apply |
| `Update` | Field-level evidence, cause, kind-declared disruption and data effect |
| `Replace` | As Update, plus explicit replacement semantics, data effect, and reversibility |
| `Delete` | Explicit destructiveness, data effect, reversibility, and the reason the resource is no longer desired |
| `NoChange` | Absent from the summary narrative; listed at detail as the `## Unchanged` section; never narrated as a change *(amended 2026-07-30: counts left the summary with D8)* |

### Refresh outcomes

| Outcome | Explanation obligation |
|---|---|
| `Present` (matches state) | No uncertainty; live state confirmed |
| `Present` (differs from state) | Drift: reported as cause `ProviderDrift` with field evidence |
| `Absent` / `ManagedMissing` | Reported as a managed resource missing from the provider |
| `Unsupported` → `RefreshStatus::Unknown` | **Uncertainty**: the engine could not confirm live state; every downstream claim about this resource is qualified |

### Semantic confidence

| Confidence | Meaning | Rendering rule |
|---|---|---|
| `ProviderGuarantee` | The provider documents this behaviour; the kind cites it | Stated plainly |
| `EngineFact` | Tokeira's own engine determines it (ordering, state, diff) | Stated plainly |
| `Inference` | Tokeira derives it from graph or heuristics | Marked as derived |
| `Unknown` | Not determinable — **the default** | Rendered as uncertainty, never as a claim |

## Glossary

- **Explanation** — the deterministic model of what an operation will do or has done, and
  why. Produced by Rust from engine state; never by a model.
- **Evidence** — a fact in the explanation with a stable identifier (`EvidenceId`) that
  any renderer, query, or agent citation can reference.
- **Evidence Index** — the addressable collection of evidence in one explanation.
- **Explained Change** — one resource change enriched with cause, semantics, dependants,
  and evidence references.
- **Cause** — why a change is present: definition edit, dependency output change, provider
  drift, replacement cascade, or engine advance.
- **Causal Group** — a set of changes sharing a root cause, presented as one story.
- **Change Semantics** — the provider-lifecycle facts a kind declares about a change:
  operation, replacement policy, disruption, data effect, reversibility, confidence.
- **Disruption** — the expected effect on availability (none, rolling, brief interruption,
  full unavailability, unknown).
- **Data Effect** — the expected effect on data the resource holds (none held, preserved,
  migrated, destroyed, or policy-governed — the provider applies a documented
  data-lifecycle policy such as TTL expiry); unknown when undeclared, via the confidence
  wrapper. *(Policy joined 2026-07-29 with the resolved TTL vocabulary decision.)*
- **Uncertainty** — a modelled statement that something could not be determined, carrying
  its subject, reason, consequence, and (optionally) the action that would resolve it.
- **Impact** — an operational consequence of the plan as a whole, derived from change
  semantics and the dependency graph.
- **Analysis Protocol** — the read-only query surface over a produced explanation.
- **Agent Client** — an external consumer (Claude Code, Codex, `tkr ask`) that navigates
  the explanation. Never a producer of facts.

## Architecture Decisions

These decisions bind every child spec.

### D1. Explanation is an artifact, never a service

`tkp` produces explanation artifacts and exits. It SHALL NOT listen on a socket, serve a
protocol, or run as a daemon in any feature of this umbrella. The analysis protocol
(Feature 5) is served by a separate process that reads produced artifacts. Rationale: the
provisioner is the privileged, trusted, mutation-capable binary; its trusted surface must
shrink or hold, never grow. A one-shot process that parses argv is a materially smaller
surface than a listener, however read-only that listener claims to be.

### D2. No model dependency in the provisioning path

No feature of this umbrella introduces a language-model dependency into `tkp`, into
`tokeira-iac`, or into any crate on the apply path. The complete default experience works
offline, with no credentials.

### D3. Provider knowledge belongs to the kind that owns the provider

A generic layer cannot know whether an update is in place, rolling, or replacing. The
resource kind declares it, with a citation. Explanation quality then improves as the kind
library improves — not as models improve.

### D4. Unknown is the default, and honesty is the lazy path

Every semantic field defaults to `Unknown`. A kind that declares nothing yields
uncertainty, not a confident-sounding default. This is deliberate: a taxonomy whose
easiest path is "claim it's a fact" degrades to decoration within two release cycles.

### D5. Agents select and frame; they never originate

An agent may choose what to surface, order it, and phrase it. It may not introduce a
resource, consequence, count, or risk that is not in the evidence index, and its output is
visibly separated from verified fact in every rendering.

### D6. Explanation extends the output contract; it does not fork it

Explanation renders through `tokeira-report` under the existing depth/form rules, in the
existing lexicon. New vocabulary (cause, impact, uncertainty, confidence) joins
`operator-language.md` before it reaches an operator.

### D7. Definitions never carry secrets in the clear

A platform definition SHALL NOT feature passwords, keys, or tokens as cleartext values.
Secrets reach running services only through the platform's secure injection mechanism —
AWS Secrets Manager on AWS platforms, the platform-appropriate equivalent elsewhere —
with the definition carrying at most a *reference* (a name or ARN), which is not itself
sensitive. Consequence for this umbrella: explanation surfaces (artifacts, bundles,
excerpts, comparisons, payloads) serve definitions as authored, and D7 is what makes
that safe — there is nothing in a conforming definition to redact. The secret-reference
vocabulary and per-platform injection mechanics are deliberately **outside this
umbrella** (future platform work); the shipped template's `admin_password` field is a
recorded defect against this decision until that work lands.

### D8. The narrative is deterministic Markdown (2026-07-29)

One narrative, three consumers: reports are emitted as deterministic Markdown, rendered
for the terminal through `termimad` and emitted raw when stdout is not a TTY — the form
agents, PR comments, and pipes consume natively. Templated would-mood prose over
descriptive names; ids stated once; no glyphs or counts where a section states the
action.

### D9. Knowledge renders; gaps enforce (2026-07-29)

Narrative states established behaviour only, in the declaration's confidence voice —
engine facts plainly, provider guarantees attributed, inferences owned ("Tokeira derives
…"). Undeclared behaviour never renders as prose: it is carried machine-side (model,
artifact) for agents and CI, and tier coverage makes it a build failure for first-party
kinds. Authors research their contributions fully; the product never surrenders a meek
don't-know.

### D10. Output templates are managed in one executable document

Templated report output is owned by [output-templates.md](output-templates.md), under
this umbrella. A rendering change is an amendment to that document first; child specs
reference its templates rather than restating them, and its reference transcripts are
asserted byte-for-byte against the renderer, so the document and the product cannot
drift.

## Requirements

## Feature 1: Evidence Model and Explanation IR

### Requirement 1.1: A deterministic explanation model

**User Story:** As an operator, I want every plan and apply to produce a complete,
structured account of what will happen or did happen, so that my understanding does not
depend on inference from resource ids.

#### Acceptance Criteria

1. WHEN the provisioner produces a plan THE provisioner SHALL construct an explanation
   model containing the deployment identity, the current and proposed revisions, one
   explained change per engine change, the operational impacts, the destructive actions,
   the uncertainties, and an evidence index.
2. WHEN the provisioner completes an apply THE provisioner SHALL construct an explanation
   model recording the changes actually committed.
3. THE explanation model SHALL be serializable and carry an explicit schema version.
4. WHERE an engine change of any `ChangeKind` is present THE explanation model SHALL
   contain exactly one explained change for it.
5. THE explanation model SHALL be constructed without network access, provider
   credentials, or any language model.

### Requirement 1.2: Stable evidence identity

**User Story:** As an agent client, I want every fact to have a stable identifier, so that
any statement I make can be traced to the evidence that supports it.

#### Acceptance Criteria

1. THE explanation model SHALL assign each addressable fact — each explained change,
   uncertainty, operational impact, and the deployment itself — a unique `EvidenceId`;
   destructive actions reference their change's id, and causes and source locations ride
   the change they annotate rather than carrying ids of their own. *(Amended 2026-07-30
   to the natural-key identity set Feature 1 established.)*
2. WHEN the same explanation model is constructed twice from identical inputs THE
   provisioner SHALL assign identical `EvidenceId`s.
3. WHEN a renderer or client cites an `EvidenceId` THE explanation model SHALL resolve it
   to exactly one fact.
4. IF an `EvidenceId` cannot be resolved in the evidence index THEN THE consuming surface
   SHALL treat the citation as invalid rather than rendering it.

### Requirement 1.3: First-class uncertainty

**User Story:** As an operator, I want to know what Tokeira could *not* determine, so that
I can distinguish a quiet plan from an uninformed one.

#### Acceptance Criteria

1. WHEN refresh classifies a resource as `RefreshStatus::Unknown` THE explanation model
   SHALL contain an uncertainty naming that resource, the reason live state could not be
   confirmed, and the consequence for the plan's claims about it.
2. WHEN a resource's desired state contains a value the provider assigns during apply THE
   explanation model SHALL record an uncertainty for that value rather than presenting a
   placeholder as fact. *(Ownership assigned 2026-07-30: Feature 1 defined
   `ProviderAssignedAtApply` and deliberately does not emit it; an
   `explanation-change-semantics` addendum activates it — kinds declare their
   provider-assigned fields through the declaration vocabulary. Until that addendum
   lands, this criterion is open, not silently unmet.)*
3. WHERE a change's semantics carry `Unknown` confidence THE explanation model SHALL
   record an uncertainty rather than omitting the field.
4. THE uncertainty record SHALL carry its subject `EvidenceId`, its reason, its
   consequence, and — where one exists — the operator action that would resolve it.
5. IF the plan contains no uncertainties THEN THE renderer SHALL state that live state was
   fully confirmed rather than rendering an empty section.

### Requirement 1.4: Rendering under the output contract

**User Story:** As an operator, I want explanation to read like the rest of the CLI, so
that one product speaks with one voice.

#### Acceptance Criteria

*(Criteria 1–2 amended 2026-07-30, reconciled to D8/D9: the counted-summary phrasing
predated the Markdown pivot. The document form — sections, templates, display names, the
header assurance line — is owned by the evidence-model child's Requirement 6 as amended
2026-07-29.)*

1. WHEN rendering at summary depth THE renderer SHALL state the principal effect, the
   destructive actions, the operational impacts, and the live-state assurance, and SHALL
   NOT render change counts or per-change semantics.
2. WHEN rendering at detail depth THE renderer SHALL additionally state per-change
   evidence, causes, and declared behaviour in the declaration's confidence voice;
   uncertainty renders through each class's channel — live state in the header and in
   place, undeclared semantics machine-side only (change-semantics Requirement 6.5).
3. WHEN `--json` is requested THE renderer SHALL emit the complete explanation model
   irrespective of depth.
4. THE renderer SHALL use only the vocabulary defined in `operator-language.md`.
5. WHERE explanation introduces vocabulary not present in `operator-language.md` THE
   change SHALL add those terms to the lexicon in the same change.

### Requirement 1.5: The artifact

**User Story:** As a CI system or agent client, I want the explanation as a file, so that
I can consume it without re-running or parsing terminal output.

#### Acceptance Criteria

1. WHEN the operator requests the explanation artifact THE provisioner SHALL write the
   complete explanation model as JSON.
2. THE artifact SHALL be self-contained: every citation resolvable within it, with no
   reference to provisioner-internal state paths.
3. THE artifact SHALL NOT contain secret values.
4. THE provisioner SHALL NOT listen on a socket, port, or stream to serve the artifact.

## Feature 2: Change Semantics in the Kind Library

### Requirement 2.1: Kinds declare their lifecycle semantics

**User Story:** As an operator, I want to know whether a change replaces, restarts, or
quietly updates a resource, so that I can judge the operational cost before applying.

#### Acceptance Criteria

1. THE resource trait SHALL expose a change-semantics method returning the lifecycle
   operation, replacement policy, disruption expectation, data effect, reversibility, and
   the confidence for each.
2. WHEN a kind does not implement change semantics THE default implementation SHALL
   return `Unknown` for every field.
3. WHEN a kind declares a provider behaviour THE implementation SHALL cite the provider
   documentation that establishes it, in the source.
4. IF a provider behaviour cannot be established from provider documentation THEN THE kind
   SHALL declare `Unknown` rather than infer.
5. THE explanation model SHALL carry kind-declared semantics verbatim, without
   substitution, inference, or defaulting to a more confident value.

### Requirement 2.2: Destructiveness is engine-classified and semantics-enriched

**User Story:** As an operator, I want destructive actions surfaced with their data
consequences, so that "destructive" is a statement about my data, not about an enum.

#### Acceptance Criteria

1. THE explanation model SHALL derive the destructive-action set from the engine's own
   classification (`ChangeKind::is_destructive`), never from kind-declared semantics.
2. WHEN a change is destructive THE explanation model SHALL carry its kind-declared data
   effect and reversibility.
3. WHERE a destructive change's data effect is `Unknown` THE explanation model SHALL
   record an uncertainty for it.
4. THE explanation model SHALL NOT describe any change as safe, low risk, or
   non-disruptive unless a kind declares that with confidence `ProviderGuarantee` or
   `EngineFact`.

### Requirement 2.3: Golden explanation tests per kind

**User Story:** As a maintainer, I want operator comprehension to be a maintained surface,
so that explanation quality cannot silently regress.

#### Acceptance Criteria

1. WHERE a kind implements change semantics THE kind SHALL carry golden tests covering
   creation, in-place update, replacement, deletion, drift, and the unsupported/uncertain
   case.
2. WHEN a kind's declared semantics change THE golden test SHALL fail until the expected
   explanation is updated.
3. THE golden tests SHALL assert the semantic classification, not the rendered prose.

## Feature 3: Causality

### Requirement 3.1: Every change carries a cause

**User Story:** As an operator, I want to know why each change is in the plan, so that I
can distinguish my own edit from the provider drifting underneath me.

#### Acceptance Criteria

1. THE explanation model SHALL assign every non-`NoChange` change exactly one cause.
2. WHEN the deployment's current definition differs from the previously applied revision
   in a way that affects a resource THE provisioner SHALL classify that resource's cause
   as a definition edit.
3. WHEN a resource's definition-derived desired state is unchanged from the previously
   applied revision AND its live state differs from recorded state THE provisioner SHALL
   classify the cause as provider drift.
4. WHEN a resource changes because a resource it depends on produced a different output
   THE provisioner SHALL classify the cause as a dependency output change and name that
   dependency.
5. WHEN a resource is deleted and recreated because a dependency is being replaced THE
   provisioner SHALL classify the cause as a replacement cascade and name the root
   resource.
6. IF a cause cannot be determined THEN THE provisioner SHALL record an uncertainty rather
   than assigning a cause speculatively.

### Requirement 3.2: Causal grouping

**User Story:** As an operator, I want related changes presented as one story, so that a
five-line plan does not read as five unrelated events.

#### Acceptance Criteria

1. WHEN multiple changes share a root cause THE explanation model SHALL group them into
   one causal group naming that root.
2. THE causal group SHALL order its members along the dependency path from root to
   consequence.
3. WHEN rendering at summary depth THE renderer SHALL present the root cause and the
   resulting effect; WHEN rendering at detail depth it SHALL present the full chain.

### Requirement 3.3: Dependants and blast radius

**User Story:** As an operator, I want to know what else is affected by a change, so that
I can anticipate consequences the plan does not list as changes.

#### Acceptance Criteria

1. WHEN a resource changes THE explanation model SHALL name the resources that depend on
   it, whether or not those resources themselves change.
2. WHERE a dependant is unaffected THE explanation model SHALL state that the dependency
   relationship continues unchanged.
3. THE dependant set SHALL be derived from the engine's dependency graph.

## Feature 4: Source Attribution

### Requirement 4.1: Definition values carry their source location

**User Story:** As an operator, I want a change traced to the line of my definition that
caused it, so that I can go straight to the edit.

#### Acceptance Criteria

*(Criteria 1–2 amended 2026-07-30 per `explanation-source-spans`: the originals
prescribed interpreter value-taint; the child spec owns the mechanism decision —
syntactic attribution — and these criteria now state the outcome.)*

1. WHEN the baseline and working definitions are available THE explanation SHALL locate
   the edits between them, each carrying a span in the working definition's coordinates.
2. WHEN an edit explains a definition-caused change THE explanation SHALL associate the
   change with that edit's location and SHALL state the basis of the association.
3. WHEN a change is caused by a definition edit THE explanation model SHALL name the
   source location of the changed value.
4. IF a value's source location cannot be established THEN THE explanation model SHALL
   fall back to revision-level attribution and record the reduced precision as an
   uncertainty.
5. THE source location SHALL be reported as a definition path with line and column.

### Requirement 4.2: Attribution never changes evaluation

**User Story:** As a maintainer, I want span tracking to be observationally invisible, so
that explanation cannot alter what a deployment realizes.

#### Acceptance Criteria

1. THE attribution computation SHALL NOT alter definition evaluation: values realized
   with attribution present SHALL be identical to those realized without it. *(Amended
   2026-07-30: rephrased mechanism-neutrally; the original presupposed span threading.)*
2. THE realized deployment SHALL be byte-identical with and without source attribution.

## Feature 5: Analysis Protocol

### Requirement 5.1: A read-only query surface

**User Story:** As an agent client, I want to query a produced explanation, so that I can
answer an operator's questions from evidence rather than from prose.

#### Acceptance Criteria

1. THE analysis surface SHALL expose queries for the deployment summary, the change
   summary, an individual change, a resource, a service, a dependency path, a source
   excerpt, a revision comparison, and the uncertainties.
2. THE analysis surface SHALL expose no operation that creates, updates, deletes, applies,
   destroys, scales, or otherwise mutates a deployment.
3. THE analysis surface SHALL operate on produced explanation artifacts and SHALL NOT
   invoke the provisioner.
4. THE analysis surface SHALL be served by a process separate from `tkp`.
5. WHEN a query names an unknown identifier THE analysis surface SHALL return an explicit
   not-found result rather than an empty success.

### Requirement 5.2: Revision comparison

**User Story:** As an operator, I want to compare two revisions of my deployment, so that
I can understand what changed between them without re-running an apply.

#### Acceptance Criteria

1. WHEN two retained revisions are named THE analysis surface SHALL report the definition
   differences between them and the resource-level consequences.
2. IF a named revision is not retained THEN THE analysis surface SHALL say so and name the
   revisions that are.

## Feature 6: Agent Clients

### Requirement 6.1: Facts and interpretation are visibly separated

**User Story:** As an operator, I want to see plainly which statements Tokeira computed
and which an assistant wrote, so that I never mistake narration for fact.

#### Acceptance Criteria

1. WHEN an agent client renders an answer THE client SHALL present verified facts and
   agent interpretation as visibly distinct sections.
2. THE client SHALL NOT merge agent prose and computed fact into a single undifferentiated
   statement.
3. WHEN an agent statement names an identifier-shaped fact — a resource, a revision, an
   evidence id — THE client SHALL require the statement's section to cover it with
   resolvable citations; assessments of risk, safety, or consequence appear only inside
   labeled interpretation, per criterion 5. *(Amended 2026-07-30 to the enforceable
   lexical contract the child spec delivers — a guard that judges coverage, not
   semantics.)*
4. IF a section of an agent answer cannot be traced to evidence THEN THE client SHALL
   suppress that section and report the suppression. *(Amended 2026-07-30: suppression
   is section-granular — partial honesty beats plausible completeness.)*
5. THE client SHALL NOT present an agent-originated risk assessment as a Tokeira
   determination.

### Requirement 6.2: Agent absence is not degradation

**User Story:** As an operator without AI tooling, I want the full explanation experience,
so that comprehension is not gated on credentials.

#### Acceptance Criteria

1. WHEN no agent client is configured THE deterministic explanation SHALL remain complete
   and unqualified.
2. THE provisioner SHALL NOT require, prompt for, or degrade without model credentials.
3. WHERE an agent client is unavailable mid-session THE operator SHALL retain access to
   the full deterministic explanation.

## Notes

- Features 1–3 are the product. Features 5–6 validate whether conversational
  interrogation earns its place; if it does not, the deterministic layer stands alone
  without apology.
- Feature 4 is the precision upgrade to Feature 3, not a prerequisite: revision-level
  causality is computable from retained revisions today, and span-level attribution
  refines it once Feature 4's syntactic pass locates the edits.
- Each child spec owns its own design and tasks, including the property-based tests for
  its correctness properties. This umbrella owns only the requirements and the boundaries
  between them.
