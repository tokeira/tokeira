# Requirements Document: Explanation Causality

## Introduction

This spec covers **Feature 3 (Causality)** from the umbrella
[`operator-explanation`](../operator-explanation/requirements.md). It answers the
operator's first question about any plan — **"why is this changing?"** — and its
immediate successors: "what else changes because of it?" and "what depends on the thing
that's changing?"

The motivating incident is recorded in this session's history: five services showed as
updates and nothing in the plan could say why. The diagnosis (image-baked environment
variables perpetually re-read as drift) took a day of `docker inspect` archaeology. With
this feature, the first plan would have read `cause: provider drift` on all five — not
`definition edit` — and the investigation would have started, and likely ended, in the
right place.

Causality here is **a comparison problem over sources Tokeira already retains**, not an
inference problem. Four sources exist today, verified in *Evidence From Current Code*:

- **D** — the desired state realized from the *current* definition,
- **P** — the desired state realized from the *previously applied revision's* definition
  (retained at `state/config-revisions/{n}/`),
- **S** — the recorded state from the last apply,
- **L** — the live state from refresh.

Classification is an algebra over (D, P, S, L) computed per resource, at revision
granularity. Feature 4 sharpens `DefinitionEdit` to a line and column; this feature
establishes *which revision comparison* explains the change, deterministically.

The scope is:

1. **Desired snapshots** — a platform seam that realizes a named definition source in
   memory (no providers, no state writes) into canonical per-resource manifests.
2. **Cause classification** — the (D, P, S, L) algebra, total over every non-`NoChange`
   change, honest about the cases it cannot decide.
3. **Cause confidence** — the classification carries how it was established; an
   undecidable cause is `Unknown` plus an uncertainty, never a guess.
4. **Causal grouping** — changes sharing a root presented as one story, ordered along the
   dependency path.
5. **Dependants** — every change names what depends on it, from the engine's graph,
   including dependants that are themselves unchanged.
6. **Rendering** — causes at summary as the plan's "why", full chains and dependants at
   detail.

### Amendment to Feature 1's design

Feature 1 defined the cause slot as `Cause` with an `Undetermined` default variant. That
duplicates what `Confidence::Unknown` already expresses, and it deprives causes of the
confidence vocabulary Features 2 and 3 share. The precise edits to
[`explanation-evidence-model/design.md`](../explanation-evidence-model/design.md):

- `ExplainedChange.cause` becomes `Confidence<Cause>` (default `Unknown`), and the
  `Cause::Undetermined` variant is removed.
- `Cause` remains in `tokeira-explain` (it is computed by explanation, not declared by
  kinds), and after Feature 2's relocation of `Confidence<T>` to `tokeira-iac` the wrapper
  is shared by both.

This amendment is applied before Feature 1's Phase 3 lands, or as this spec's first task
if Feature 1 has already shipped; either way the JSON shape of an *undetermined* cause is
`"unknown"` from the first artifact, so no consumer migration exists.

### What This Spec Covers

- The desired-snapshot seam on `ProvisionerPlatform` and its compose-platform implementation
  (reusing the `definition check` interpretation machinery).
- The classification algebra, its implementation in `tokeira-explain`, and its confidence
  rules.
- Dependency-output tracing at derived confidence, where unambiguous.
- Replacement-cascade classification from the dependency graph.
- Causal grouping and dependant sets.
- Rendering and lexicon additions.

### What This Spec Does NOT Cover

- Line/column attribution of a definition edit (Feature 4, `explanation-source-spans`) —
  this feature attributes to the *revision comparison*, and its rendering says so
  honestly.
- Kind-declared change semantics (Feature 2) — causality is computable with every
  semantics field `Unknown`; where both are present the renderer composes them.
- Any change to how the engine computes changes, orders operations, or classifies
  destructiveness.
- Cross-deployment or historical causality (why did revision 3 change) — the analysis
  protocol's revision comparison (Feature 5) builds on this feature's snapshots.

## Evidence From Current Code

| Fact | Anchor | Consequence |
|---|---|---|
| Every applied definition is retained per revision, with `snapshot` and `restore` | `crates/tokeira-provisioner-cli/src/config_history.rs` | **P** exists for every applied revision without new storage |
| The envelope records `config_revision` and `effective_config_ref` | `crates/tokeira-provisioner/src/*` | The baseline revision to compare against is known at plan time |
| The definition interprets fully in memory with no provider access (`definition check`, `load_tkd_config`) | `platforms/compose/src/provisioner.rs` | Realizing a retained revision is the same machinery pointed at a different file |
| `TkdDeployment::realize` + `realize_module` produce resources whose `to_manifest()` is the desired shape | `platforms/compose/src/adapter.rs`, `crates/tokeira-compose/src/lib.rs` | **D** and **P** are comparable values, not texts |
| Set-valued manifest fields require canonicalization before comparison (`canonicalize_manifest`; the port-order roulette) | `crates/tokeira-compose/src/lib.rs` | Desired-vs-desired comparison MUST be canonical or it will manufacture phantom definition edits — the same demon in a new seam |
| `InfraState` carries `resources: BTreeMap<ResourceId, ResourceState>`; its `outputs` map has no producer | `crates/tokeira-iac/src/document.rs`; writeback reads at `platforms/compose/src/adapter.rs` | **S** exists. The recorded-output channel is `ResourceState.properties` — written per resource at apply, read by name by the writeback wiring — so A4 traces through dependency **properties** |
| `ResourceState.dependencies` is recorded, and the engine topologically orders from it (`topological_sort_from_state`) | `crates/tokeira-iac/src/engine.rs` | The dependency graph for grouping, cascades, and dependants exists on both desired and recorded sides |
| Refresh classifies per-resource live status, carried by Feature 1's `RefreshCoverage` | `crates/tokeira-iac/src/engine.rs`; Feature 1 | **L**'s availability is knowable; an unconfirmed **L** must degrade drift claims |
| Refresh overwrites in-context state properties with live describe output before diffing (`ctx.state = refreshed.state`) | `crates/tokeira-iac/src/engine.rs` | **S in the diff context is contaminated with live observations** — the classifier must read recorded state from the store, not from the post-refresh context |
| `Cause` and the cause slot exist with an explicit not-determined default | Feature 1 design; `tokeira-explain` | This feature populates a slot; the schema does not reshape |

## Target State

Every plan answers "why" for every change, at one of three honesty levels: an engine-fact
classification from the revision algebra; a derived (inference) classification from
output tracing or cascade analysis; or an explicit `Unknown` carrying an uncertainty that
names what would decide it. Causes ride the change lines in the report's Markdown form;
chains read as one story:

```markdown
# Infra Plan
**Plan for compose** at revision 4, with *live state* confirmed

## Update
- the *grafana* service would be updated - `image`: `12.4.3` → `12.5.0` - `observability::compose/grafana`
- the *mimir* service would be updated - `environment` changed outside the definition - `observability::compose/mimir`
```

The clause is the concrete change — the operator's own diff, or the fields that changed
outside the definition — never a cause category, and never a revision number (the
header anchors the revision once). At detail depth each change adds its dependants,
derived causes owned as derived, and — for an unknown cause — the uncertainty in place;
a multi-member chain presents once, root first. The templates are owned by
[output-templates.md](../operator-explanation/output-templates.md).

## Glossary

Terms additional to the umbrella and sibling glossaries:

- **Desired Snapshot** — the per-resource canonical manifests realized from one definition
  source in memory, keyed by resource id. No provider is consulted to produce it.
- **Baseline Revision** — the last applied revision (`envelope.config_revision`), whose
  retained definition realizes to **P**.
- **Working Definition** — the deployment's current definition file, which realizes to
  **D**; it may be edited and not yet applied.
- **Recorded State (S)** — the persisted `InfraState` from the store, read *before* any
  refresh contamination.
- **Live State (L)** — what refresh observed, with per-resource confirmation status.
- **Cause Assessment** — a `Cause` wrapped in the shared confidence vocabulary.
- **Root** — the resource (or the revision comparison itself) a causal group hangs from.
- **Causal Group** — the set of changes sharing one root, ordered along the dependency
  path from root to consequence.
- **Dependant** — a resource with a dependency edge *onto* the changing resource; the
  inverse of the engine's `dependencies()` direction.
- **Output Tracing** — matching a changed desired value against a dependency's changed
  recorded output; derived confidence by construction.

## The Classification Algebra

The normative decision procedure, per resource R with change kind other than `NoChange`.
Comparisons are over canonical manifests; ∉ means absent from the source.

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
and it SHALL NOT surface as a generic could-not-establish uncertainty.

## Requirements

### Requirement 1: Desired snapshots from any retained definition

**User Story:** As the explanation layer, I want the platform to realize a named
definition source into comparable manifests, so that revision comparison is a comparison
of values rather than texts.

#### Acceptance Criteria

1. THE platform seam SHALL expose a desired-snapshot operation taking the deployment
   directory and a definition source path and returning per-resource canonical manifests
   keyed by resource id.
2. THE desired-snapshot operation SHALL NOT contact any provider, SHALL NOT read live
   state, and SHALL NOT write state.
3. WHEN the named definition source does not interpret THE operation SHALL return the
   located verdict (the `definition check` error) rather than a partial snapshot.
4. THE returned manifests SHALL be canonical: two snapshots of semantically identical
   definitions SHALL be equal, including set-valued fields in any authored order.
5. WHERE a platform has no interpreted definition THE operation SHALL answer with the
   typed not-applicable refusal, and causality for that platform SHALL classify per A10.
6. THE snapshot of the working definition and the snapshot of a retained revision SHALL be
   produced by the same code path.

### Requirement 2: Every change receives exactly one cause assessment

**User Story:** As an operator, I want each change to say why it is happening, so that I
never diagnose a plan by re-deriving its inputs myself.

#### Acceptance Criteria

1. WHEN a plan is explained THE explanation SHALL assign every non-`NoChange` change
   exactly one cause assessment.
2. THE classification SHALL follow the algebra's conditions and precedence exactly as
   specified in *The Classification Algebra*.
3. THE classifier SHALL read recorded state from the state store as persisted, not from
   the post-refresh planning context.
4. WHEN live state for a resource is unconfirmed or unexamined THE classifier SHALL NOT
   classify `ProviderDrift` or `EngineAdvance` for it.
5. WHEN the baseline revision's definition is unavailable THE classifier SHALL classify
   per A10 and SHALL name the missing revision in the uncertainty.
6. THE classifier SHALL be a pure function of (D, P, S, L, the dependency graph, and the
   plan's changes): identical inputs SHALL yield identical assessments.
7. WHERE the algebra yields `Unknown` THE explanation SHALL carry an uncertainty whose
   consequence states that the change's origin could not be established and whose
   resolution names what would establish it.

### Requirement 3: Dependency-output tracing is derived and unambiguous

**User Story:** As an operator, I want "this changed because its dependency's output
changed" distinguished from "someone edited this", so that I chase the root, not the
symptom.

#### Acceptance Criteria

1. WHEN a resource's desired difference against the baseline consists of values equal to
   outputs that changed on a resource it depends on THE classifier MAY classify
   `DependencyOutputChanged`, naming that dependency.
2. THE classification SHALL be made only when the trace is unambiguous: every differing
   field traces to exactly one changed dependency output.
3. WHERE the trace is ambiguous or partial THE classifier SHALL fall through to A5 and
   SHALL NOT name a dependency speculatively.
4. THE cause assessment SHALL carry inference confidence, and the renderer SHALL mark it
   as derived.
5. THE trace SHALL be established against identity and the state diff, never bare value
   equality: the named dependency SHALL be reachable by a recorded dependency edge from
   the resource, and an output SHALL count as changed only when its recorded value (S)
   departs from the value the baseline definition realized for the consuming field (P)
   while matching the working value (D).

### Requirement 4: Causal grouping tells one story

**User Story:** As an operator, I want related changes grouped under their root, so that a
plan reads as reasons rather than as a list.

#### Acceptance Criteria

1. WHEN multiple changes share a root THE explanation SHALL group them into one causal
   group naming that root.
2. THE members of a group SHALL be ordered along the dependency path from the root
   outward; members not connected by a path SHALL be ordered deterministically after
   path-connected members.
3. THE roots for grouping SHALL be: the revision comparison for definition edits; the
   named dependency for output-traced changes; for cascades, the ultimate root — the
   first non-cascade cause reached by walking the cascade chain — so a transitive
   replacement reads as one story; the drifted resource itself for drift; the provisioner
   advance for engine-advance changes.
4. THE rendering of groups SHALL follow Requirement 6: causes as clauses of the change
   lines at summary depth, the chain told once at detail depth (Requirement 6.4).
5. THE groups SHALL partition the plan's non-`NoChange` changes: every such change SHALL
   appear in exactly one group.
6. THE root walk SHALL be bounded by the engine-version and baseline-revision
   boundaries: a chain whose origin is an engine advance SHALL root at the provisioner
   advance and reach no further back, and no root SHALL attribute across a revision
   earlier than the baseline comparison.

### Requirement 5: Dependants are stated

**User Story:** As an operator, I want to see what depends on a changing resource, so that
consequences the plan does not list as changes are still visible.

#### Acceptance Criteria

1. WHEN a resource changes THE explained change SHALL name the resources that depend on
   it, derived from the engine's dependency graph over the union of desired and recorded
   state.
2. WHERE a dependant is itself unchanged THE detail rendering SHALL state that the
   dependency relationship continues unchanged.
3. THE dependant set SHALL NOT be derived from declared semantics, heuristics, or name
   matching — the graph only.
4. WHERE a change has no dependants THE detail rendering SHALL omit the section rather
   than render an empty one.

### Requirement 6: Rendering and lexicon

**User Story:** As an operator, I want causes in the same voice as the rest of the report,
so that "why" reads as naturally as "what".

#### Acceptance Criteria

1. WHEN rendering at summary depth THE renderer SHALL state each non-`NoChange`
   change's established cause as a clause of its change line per the clause table in
   [output-templates.md](../operator-explanation/output-templates.md): the concrete
   change, never a cause category, and never a revision number — the header anchors
   the revision once.
2. WHEN rendering at detail depth THE renderer SHALL render a cause voice line only
   where it adds information beyond the line clause — a derived classification owned
   as derived with its citation; an unknown cause per criterion 3 — and the change's
   dependants per Requirement 5. An engine-fact cause speaks through its line clause.
3. WHERE a change's cause is unknown THE renderer SHALL render no cause clause at
   summary depth and SHALL render the cause's uncertainty in place with the change at
   detail depth: the consequence and, where one exists, the resolving action.
4. WHEN a causal group holds more than one member THE detail rendering SHALL present
   the chain as one story: the root stated once, members along the dependency path.
5. THE renderer SHALL NOT claim line-level attribution before Feature 4 populates source
   locations; revision-level attribution SHALL be phrased as such.
6. WHERE this feature introduces operator-facing vocabulary (cause, drift, dependant,
   causal group, root) THE change SHALL add those terms to `operator-language.md` in the
   same change.
7. THE `--json` rendering SHALL carry every cause assessment, group, and dependant set
   regardless of depth.

## Notes

- **A9 is the requirement the phantom hunt earns.** A drift claim over an unconfirmed live
  read is precisely the confident-but-wrong statement this umbrella exists to prevent; the
  algebra refuses it structurally.
- **The classifier's S must bypass the planning context** (2.3): the engine overwrites
  in-context properties with live observations before diffing. Reading S from the context
  would make A7 compare live against live — silently reclassifying all drift as clean.
  This is the kind of trap that becomes a day-long hunt; it is named here so it becomes a
  property test instead.
- **A8 exists because of yesterday's upgrade**: two services changed under an unchanged
  definition and unchanged live state, because the new provisioner realizes labels the old
  one did not. That is neither an edit nor drift, and naming it (`EngineAdvance`) is what
  keeps the other two categories honest.
- Canonicalization (1.4) is non-negotiable: without it, desired-vs-desired comparison
  re-imports the port-order roulette as phantom definition edits.
- **A3b is A3's mirror**: a resource desired unchanged since the baseline yet absent
  from recorded state is the create-side reconcile completion — an interrupted apply, a
  partially recorded commit. The engine knows this; classifying it `Unknown` would be
  exactly the generic could-not-establish this table exists to prevent.
- **Cause uncertainties render in narrative** (Requirement 6.3), unlike
  undeclared-semantics gaps (change-semantics Requirement 6.5, machine-side): a cause
  gap is operator-actionable — confirm live state, restore a revision — and hiding it
  would make the one change without a "why" read as quietly fine. An undecidable cause
  is knowledge about the plan, not a missing declaration.
