# Requirements Document: Operator Explanation

## Introduction

Tokeira's provisioning engine knows a great deal that a bare change list never tells the
operator: which resources will change and why, which changes destroy data, which live
state it could not confirm, which resource depends on which, and which definition
revision produced the current desired state. The **explanation layer** turns that
knowledge into a deterministic model, rendered for humans and serialized for machines.

The governing principle, in three clauses:

> **The engine establishes truth. The explanation layer establishes meaning. An agent
> only helps the operator navigate it.**

This is deliberately the inverse of "add an LLM to a plan diff". Every fact an operator
reads is computed by Rust from engine state, kind-library semantics, and the dependency
graph. No language model is required to produce any part of the experience, and no
language model is permitted to originate a fact.

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
4. **The output authority** — [output-templates.md](output-templates.md) owns every
   templated report form; its reference transcripts are asserted byte-for-byte against
   the renderer (decision D10).

### Scope

**In scope** (all delivered; the requirements below state the standing contract):

- A versioned, serializable explanation model produced by every plan, and by every apply
  as the record of what it did — retained per applied revision.
- Per-change semantics contributed by the resource kind that owns the provider:
  lifecycle operation, replacement policy, disruption expectation, data effect,
  reversibility, provider-assigned values, and the confidence of each.
- Causal classification for every change, with causal grouping, dependants, and
  per-field departure annotation.
- First-class uncertainty: what the engine could not determine, why, what follows from
  it, and what would resolve it.
- Typed platform issues at the describe boundary, and definition verification that
  refuses the two conditions a plan must never meet (an undescribable kind, a dangling
  dependency edge).
- Deterministic rendering under the output contract, and a stable JSON artifact.

**Out of scope** (each returns only as its own spec, if ever):

- Any language-model dependency inside `tkp`, in any feature, at any time.
- Source-span attribution: locating a definition edit at file:line. Causality already
  attributes definition edits at revision precision; the syntactic upgrade was assessed
  and deliberately deferred — `git diff` over the retained revision sources answers the
  line-number question without new machinery.
- A served analysis surface (queries, protocols, MCP) and agent clients (`tkr ask`).
  The retained artifact at a well-known path *is* the machine interface: raw Markdown
  (D8) plus `state/config-revisions/{n}/explanation.json` serve agents, CI, and
  operators alike with no daemon, no protocol, and no new crate.
- Per-platform SDK error-class direction tables beyond compose's Docker seam — each
  platform arc (ECS, EKS) owns its own tables under D3/D4 discipline as it matures.
- Model-generated risk assessment or any model-originated fact; conversation
  persistence; graphical rendering; cost estimation; policy evaluation.

## Surface Accounting

Explanation is a total function over the engine's outcome vocabulary: every variant below
has a defined explanation obligation, so no engine outcome can silently render as nothing.

### Change kinds

| `ChangeKind` | Explanation obligation |
|---|---|
| `Create` | Kind-declared creation semantics; one uncertainty per declared provider-assigned value |
| `Update` | Field-level evidence, cause, kind-declared disruption and data effect |
| `Replace` | As Update, plus explicit replacement semantics, data effect, and reversibility |
| `Delete` | Explicit destructiveness, data effect, reversibility, and the reason the resource is no longer desired |
| `NoChange` | Absent from the summary narrative; listed at detail as the `## Unchanged` section; never narrated as a change |

### Refresh outcomes

| Outcome | Explanation obligation |
|---|---|
| `Present` (matches state) | No uncertainty; live state confirmed |
| `Present` (differs from state) | Departure: cause `ProviderDrift` when the definition is unchanged; per-diff annotation when an edit owns the change |
| `Absent` / `ManagedMissing` | Reported as a managed resource missing from the provider |
| `Unsupported` → `RefreshStatus::Unknown` | **Uncertainty**: the engine could not confirm live state; every downstream claim about this resource is qualified |

### Semantic confidence

| Confidence | Meaning | Rendering rule |
|---|---|---|
| `ProviderGuarantee` | The provider documents this behaviour; the kind cites it | Stated plainly, attributed |
| `EngineFact` | Tokeira's own engine determines it (ordering, state, diff) | Stated plainly |
| `Inference` | Tokeira derives it from cited facts | Owned: "Tokeira derives this" |
| `Unknown` | Not determinable — **the default** | Machine-side uncertainty, never a claim |

## Glossary

- **Explanation** — the deterministic model of what an operation will do or has done, and
  why. Produced by Rust from engine state; never by a model.
- **Evidence** — a fact in the explanation with a stable identifier (`EvidenceId`) that
  any renderer or citation can reference; the evidence index resolves each id to exactly
  one fact.
- **Cause** — why a change is present: definition edit, dependency output change,
  provider drift, replacement cascade, or engine advance.
- **Causal group** — a set of changes sharing an ultimate root, presented as one story.
- **Change semantics** — the provider-lifecycle facts a kind declares about a change:
  operation, replacement policy, disruption, data effect, reversibility,
  provider-assigned values, confidence.
- **Disruption** — the expected effect on availability (none, rolling, brief
  interruption, full unavailability, unknown).
- **Data effect** — the expected effect on data the resource holds (none held,
  preserved, migrated, destroyed, or policy-governed); unknown when undeclared.
- **Uncertainty** — a modelled statement that something could not be determined,
  carrying its subject, reason, consequence, and (optionally) the action that would
  resolve it.
- **Impact** — an operational consequence of the plan, derived from change semantics,
  the engine's own change classification, and the dependency graph.

## Architecture Decisions

### D1. Explanation is an artifact, never a service

`tkp` produces explanation artifacts and exits. It SHALL NOT listen on a socket, serve a
protocol, or run as a daemon. Rationale: the provisioner is the privileged, trusted,
mutation-capable binary; its trusted surface must shrink or hold, never grow. Retention
extends this: the per-revision artifact on disk is the read surface, and reading it
requires no process at all.

### D2. No model dependency in the provisioning path

No part of this surface introduces a language-model dependency into `tkp`, into
`tokeira-iac`, or into any crate on the apply path. The complete experience works
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

Any agent consuming the artifact may choose what to surface, order it, and phrase it. It
may not introduce a resource, consequence, count, or risk that is not in the evidence
index, and its output must be visibly separated from verified fact in any rendering.

### D6. Explanation extends the output contract; it does not fork it

Explanation renders through `tokeira-report` under the existing depth/form rules, in the
existing lexicon. New vocabulary joins `operator-language.md` before it reaches an
operator.

### D7. Definitions never carry secrets in the clear

A platform definition SHALL NOT feature passwords, keys, or tokens as cleartext values.
Secrets reach running services only through the platform's secure injection mechanism —
AWS Secrets Manager on AWS platforms, the platform-appropriate equivalent elsewhere —
with the definition carrying at most a *reference* (a name or ARN), which is not itself
sensitive. Consequence: explanation surfaces serve definitions as authored, and D7 is
what makes that safe — there is nothing in a conforming definition to redact. The
secret-reference vocabulary and per-platform injection mechanics are platform work,
outside this spec.

### D8. The narrative is deterministic Markdown

One narrative, three consumers: reports are emitted as deterministic Markdown, rendered
for the terminal through `termimad` and emitted raw when stdout is not a TTY — the form
agents, PR comments, and pipes consume natively. Templated would-mood prose over
descriptive names; ids stated once; no glyphs or counts where a section states the
action.

### D9. Knowledge renders; gaps enforce

Narrative states established behaviour only, in the declaration's confidence voice —
engine facts plainly, provider guarantees attributed, inferences owned ("Tokeira derives
…"). Undeclared behaviour never renders as prose: it is carried machine-side (model,
artifact) for agents and CI, and tier coverage makes it a build failure for first-party
kinds. Authors research their contributions fully; the product never surrenders a meek
don't-know.

### D10. Output templates are managed in one executable document

Templated report output is owned by [output-templates.md](output-templates.md), under
this spec. A rendering change is an amendment to that document first; the renderer
follows, and the document's reference transcripts are asserted byte-for-byte against the
renderer, so the document and the product cannot drift.

## Requirements

### Requirement 1: The explanation model

**User Story:** As an operator, I want every plan and apply to produce a complete,
structured account of what will happen or did happen, so that my understanding does not
depend on inference from resource ids.

#### Acceptance Criteria

1. WHEN the provisioner produces a plan THE provisioner SHALL construct an explanation
   model containing the deployment identity, the current and proposed revisions, one
   explained change per engine change, the operational impacts, the destructive actions,
   the uncertainties, the platform issues, and an evidence index.
2. WHEN the provisioner completes an apply THE provisioner SHALL construct an explanation
   model recording the changes actually committed — identities only, with field evidence
   reused from a gating plan in the same invocation and its absence recorded as
   uncertainty, never as a fabricated before-image.
3. THE explanation model SHALL be serializable, SHALL carry an explicit schema version,
   and SHALL be constructed without network access, provider credentials, or any
   language model.
4. THE model SHALL assign each addressable fact — each explained change, uncertainty,
   operational impact, and the deployment itself — a unique `EvidenceId` from its
   natural key; identical inputs SHALL yield identical ids, and every citation SHALL
   resolve to exactly one fact.
5. THE destructive-action set SHALL derive from the engine's own classification
   (`ChangeKind::is_destructive`), never from kind-declared semantics.

### Requirement 2: Uncertainty is modelled from the sources that exist

**User Story:** As an operator, I want to know what Tokeira could *not* determine, so
that I can distinguish a quiet plan from an uninformed one.

#### Acceptance Criteria

1. WHEN refresh classifies a resource as `RefreshStatus::Unknown` THE model SHALL
   contain an uncertainty naming that resource, the reason, and the consequence for the
   plan's claims about it; WHERE a verb performs no refresh THE model SHALL record that
   live state was not examined.
2. WHEN a creation's kind declares provider-assigned values THE model SHALL record one
   uncertainty per declared name — the plan's silence about those values reads as "not
   yet assigned", never as oversight.
3. WHERE a change's declared semantics carry `Unknown` for an applicable field THE
   model SHALL record an uncertainty rather than omitting the field; undeclared-
   semantics uncertainty is machine-channel only and never renders as narrative (D9).
4. THE uncertainty record SHALL carry its subject `EvidenceId`, its reason, its
   consequence, and — where one exists — the operator action that would resolve it.
5. Live-state uncertainty renders in place, per resource, at detail depth; the document
   header carries the revision anchor only and no coverage clause
   ([output-templates.md](output-templates.md) §The rules).

### Requirement 3: The artifact and its retention

**User Story:** As a CI system, an agent, or an operator returning later, I want each
apply's explanation as a file, so that "what did revision N mean" is answerable without
re-running anything.

#### Acceptance Criteria

1. WHEN the operator requests the explanation artifact THE provisioner SHALL write the
   complete model as JSON before claiming success; a failed write fails the verb without
   costing the apply its committed record.
2. WHEN an apply commits a revision THE provisioner SHALL retain the applied explanation
   at `state/config-revisions/{n}/explanation.json`, beside that revision's retained
   definition sources.
3. THE artifact SHALL be self-contained — every citation resolvable within it, no
   provisioner-internal state paths — and SHALL NOT contain secret values.

### Requirement 4: Change semantics in the kind library

**User Story:** As an operator, I want to know whether a change replaces, restarts, or
quietly updates a resource, so that I can judge the operational cost before applying.

#### Acceptance Criteria

1. THE resource trait SHALL expose a change-semantics method returning the lifecycle
   operation, replacement policy, disruption expectation, data effect, reversibility,
   provider-assigned creation values, and the confidence for each; the default
   implementation SHALL return `Unknown` for every field.
2. WHEN a kind declares a provider behaviour THE implementation SHALL cite the provider
   documentation or the engine code that establishes it, in the source; a behaviour that
   cannot be established SHALL stay `Unknown` rather than be inferred.
3. THE model SHALL carry kind-declared semantics verbatim, without substitution,
   inference, or defaulting to a more confident value; a deletion whose kind cannot be
   reached (no registered recoverer) SHALL be stated as uncertainty, never silence.
4. THE model SHALL NOT describe any change as safe, low risk, or non-disruptive unless a
   kind declares that with confidence `ProviderGuarantee` or `EngineFact`.
5. WHERE a kind implements change semantics THE kind SHALL carry golden tests covering
   its applicable change kinds, asserting the semantic classification, not the rendered
   prose.

#### Declaration ownership

Capturing the properties the deployment engine acts on is the resource + kind
author's responsibility, discharged at authoring time: `change_semantics` has no
default, so a kind cannot compile without stating its semantics alongside the
lifecycle it implements. There is no inventory, no registry, and no tier
classification — the enforcement is structural:

- **Presence** — the trait requires the declaration; the compiler asks every
  author, for every kind, on every platform.
- **Evidence** — fact-bearing confidence values (`EngineFact`, `Inference`,
  `ProviderGuarantee`) carry citations structurally; a claim cannot be stated
  without naming its source.
- **Honesty** — verification travels in the confidence tier, not in a policy
  table: what the kind's own lifecycle code does is `EngineFact`; what is
  derived from that code is `Inference`, owned as derived; a documented
  provider behaviour is `ProviderGuarantee` with the document cited; and a
  claim nobody has established is declared `Unknown` per-field, with intent —
  never defaulted.

### Requirement 5: Causality

**User Story:** As an operator, I want to know why each change is in the plan, so that I
can distinguish my own edit from the provider drifting underneath me.

#### Acceptance Criteria

1. THE model SHALL assign every non-`NoChange` change exactly one cause, classified by
   the algebra below over D (the working definition's desired snapshot), P (the baseline
   revision's, realized from the retained sources through the same interpretation path),
   S (recorded state, read from the store as persisted — never from a planning context
   the refresh has contaminated), and L (the confirmed live read).
2. WHEN a cause cannot be determined THE model SHALL record an uncertainty rather than
   assign a cause speculatively.
3. WHEN multiple changes share an ultimate root THE model SHALL group them into one
   causal group naming that root, ordered along the dependency path; the walk is bounded
   by engine-version and baseline-revision boundaries, and each member's own cause still
   names its nearest dependency.
4. WHEN a resource changes THE model SHALL name its dependants — changing and unchanged
   alike — derived from the union of the desired and recorded dependency graphs.
5. WHERE a confirmed live read departs from recorded state on a field an edit also
   changes THE model SHALL annotate that diff in place ("changed outside the
   definition") — the edit owns the change; the departure stays visible per field. A
   diff with no recorded value makes no departure claim.

#### The classification algebra

The normative decision procedure, per resource R with change kind other than `NoChange`.
Comparisons are over canonical manifests; ∉ means absent from the source. The
implementation is `tokeira_explain::causality`; its table-literal oracle asserts this
table row for row.

| # | Condition over (D, P, S, L) | Cause | Confidence |
|---|---|---|---|
| A1 | R ∈ D and R ∉ P | `DefinitionEdit` (introduced) | Engine fact |
| A2 | R ∉ D and R ∈ P | `DefinitionEdit` (removed) | Engine fact |
| A3 | R ∉ D and R ∉ P and R ∈ S | `DefinitionEdit` (removed at or before the baseline; the reconcile is completing now) | Engine fact |
| A3b | R ∈ D and R ∈ P and D(R) = P(R) and R ∉ S | `DefinitionEdit` (introduced at or before the baseline; the reconcile is completing now — an interrupted or partially recorded apply) | Engine fact |
| A4 | D(R) ≠ P(R), and the difference traces unambiguously to a dependency's changed output | `DependencyOutputChanged { dependency }` | Inference |
| A5 | D(R) ≠ P(R), otherwise | `DefinitionEdit` | Engine fact |
| A6 | D(R) = P(R), R's dependency is planned as `Replace`, and R's own change is forced by that replacement | `ReplacementCascade { root }` | Inference |
| A7 | D(R) = P(R), L(R) confirmed, L(R) ≠ S(R) | `ProviderDrift` | Engine fact |
| A8 | D(R) = P(R), L(R) confirmed, L(R) = S(R), D(R) ≠ S(R) | `EngineAdvance` (the same definition realizes differently under the current provisioner) | Engine fact |
| A9 | D(R) = P(R), L(R) unconfirmed or unexamined | `Unknown` + uncertainty (a drift claim without a confirmed L would be speculation) | Unknown |
| A10 | P unavailable (no baseline: never applied, or the retained revision is missing) | Creates on a never-applied deployment: `DefinitionEdit`, engine fact. Otherwise `Unknown` + uncertainty naming the missing revision | per case |

Order is significant: A1–A3b (existence, including absence from recorded state) before
A4–A5 (content) before A6–A9 (state), A6 before A7 (a cascade would otherwise misread as
drift), A9 guards A7–A8 (no drift claim over an unconfirmed live read). A3b sits in the
existence family deliberately: its change is a create, so the cascade and drift rows
cannot meaningfully apply to it — the never-recorded create is a fact the engine holds,
and it SHALL NOT surface as a generic could-not-establish uncertainty. A4's trace rides
identity and the recorded-state diff — a dependency edge plus a recorded output that
equals the leaf's working side and differs from its baseline side; bare value equality
never classifies, and an ambiguous or partial trace falls through to A5.

### Requirement 6: Impacts

**User Story:** As an operator, I want the plan's operational consequences stated per
resource, so that "destructive" is a statement about my data, not about an enum.

#### Acceptance Criteria

1. Impacts derive from two sources: declared semantics, and the engine's own change
   classification — every `Replace` carries unavailability-while-applying (lifted by a
   declared create-before-destroy) and the replacement itself; every `Delete` carries
   no-longer-available; no declaration has to exist for the engine floor to render.
2. WHEN the desired graph drops a recorded dependency edge whose target this plan
   deletes THE model SHALL carry a dependency-loss impact for the dependant — an engine
   fact from the graph delta.
3. Impacts group per resource, consequences merged severity-first, permanent
   consequences before transient ones (data destruction, dependency loss,
   unavailability, replacement), per [output-templates.md](output-templates.md).

### Requirement 7: Platform issues and definition verification

**User Story:** As an operator, I want an unreachable platform or an unverifiable
definition refused with the facts, so that no plan is ever narrated from guesswork.

#### Acceptance Criteria

1. WHEN a platform's describe seam cannot reach its provider THE plan SHALL refuse with
   a typed `PlatformIssue` — the fact naming the platform component, the SDK error
   verbatim as evidence, and a direction only where the error class itself establishes
   one — rendered per [output-templates.md](output-templates.md) with no change
   sections, exiting non-zero. Apply and destroy propagate the same failure as a hard
   error.
2. Direction tables are declared by the owning platform (D3/D4): compose owns the Docker
   seam's; each future platform arc owns its provider's.
3. `definition check` SHALL refuse a definition realizing a kind whose `describe`
   performs no live query (`Resource::describes` false), and a definition carrying a
   dangling dependency edge — naming both ends. These are verification concerns; they
   never reach a plan, which is what lets impacts speak entirely through changes.

### Requirement 8: The server-config coupling

**User Story:** As an operator, I want an edit to `tokeirad.toml` to plan and apply like
any other desired-state change, so that the server configuration is part of the
deployment's story rather than a silent side channel.

#### Acceptance Criteria

1. THE server configuration SHALL be an authored graph node (`ServerConfig`, engine id
   `server-config`, display noun "server configuration"); a consuming service declares
   a resource dependency on it, exactly as configuration consumers do — ordering,
   dependants, and dependency loss name the server configuration.
2. THE declared consumer SHALL mount the live file and carry the node's desired-content
   identity in its manifest (`TOKEIRA_SERVER_CONFIG_DIGEST`, the framework's
   dependency-content coupling), so an edit is a manifest diff: the plan states the
   update and the apply recreates the container onto the new content.
3. THE node's desired manifest SHALL digest the interpreted source set's copy: revision
   retention keeps `tokeirad.toml` beside the retained definition, and a baseline
   realization digests the retained bytes — the edit classifies as the operator's own
   (A5), never as a provisioner advance. `revert` rewrites only the definition source;
   the live server configuration stays the operator's file.
4. WHERE the node is declared and the file is absent THE manifest SHALL state the
   absence and the node's create SHALL refuse naming the missing path — never a silent
   skip, never a container bound to a missing file.

### Requirement 9: Rendering

**User Story:** As an operator, I want explanation to read like the rest of the CLI, so
that one product speaks with one voice.

#### Acceptance Criteria

1. THE renderer SHALL emit the plan and apply documents exactly as
   [output-templates.md](output-templates.md) states them — sections, lines, clauses,
   detail sub-bullets, voices, computed plurals — asserted byte-for-byte by the
   executable-transcript tests.
2. THE renderer SHALL use only vocabulary defined in `operator-language.md`; WHERE new
   vocabulary is introduced THE change SHALL add it to the lexicon in the same change,
   and the banned list is asserted against the renderer's own constant.
3. WHEN `--json` is requested THE renderer SHALL emit the complete explanation model
   irrespective of depth.

## Correctness Properties

The property-based tests carry these invariants; a test's tag names its area and number
(for example `operator-explanation §Evidence, Property 9`). The lists are the stable
anchors — renumbering is a spec change.

**Evidence (model construction and rendering)** — 1 change coverage is total ·
2 construction is deterministic · 3 evidence closure holds · 4 uncertainty is exhaustive
over unconfirmed state · 5 detail is a superset of summary · 6 structured form is
complete and depth-blind · 7 widening preserves planning · 8 not-determined slots are
silent · 9 apply-side explanation invents nothing · 10 the artifact is self-contained
and bounded · 11 rendering stays inside the lexicon · provider-assigned names become
create uncertainties, exhaustively.

**Semantics (declarations)** — 1 the default declares nothing · 2 declaration is total ·
3 transport is verbatim · 4 declarations cannot move the destructive set · 5 impact
derivation is a pure function of declarations and kinds · 6 every impact is grounded ·
7 unknown never becomes a claim · 8 uncertainty activation is exact · 9 tier coverage
holds · 10 every claim cites.

**Causality (classification)** — 1 assessment is total and unique · 2 the algebra is
followed exactly (table-literal oracle) · 3 classification is deterministic and pure ·
4 no drift claim without a confirmed live read · 5 S-isolation: refresh contamination
cannot reclassify · 6 output tracing is unambiguous or absent, riding identity and the
state diff · 7 snapshot canonicality · 8 groups partition and roots are the bounded
ultimate roots · 9 dependants are the reverse graph, exactly · 10 unknown causes surface
as uncertainty, one-to-one.
