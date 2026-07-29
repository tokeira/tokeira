# Requirements Document: Explanation Evidence Model

## Introduction

This spec covers **Feature 1 (Evidence Model and Explanation IR)** from the umbrella
[`operator-explanation`](../operator-explanation/requirements.md). It builds the
foundation every other explanation feature stands on: a versioned, serializable model of
what an operation will do or did, with stable evidence identity, first-class uncertainty,
deterministic rendering, and a self-contained artifact.

The scope is:

1. **The explanation model** — a typed, serializable, schema-versioned structure produced
   by plan and by apply, carrying deployment identity, revisions, explained changes,
   operational impacts, destructive actions, uncertainties, and an evidence index.
2. **Evidence identity** — stable, deterministic `EvidenceId`s addressing every fact.
3. **Uncertainty** — modelled from the sources that exist today, starting with the
   refresh statuses the engine already computes and currently discards.
4. **Refresh-status plumbing** — widening the engine → orchestrator → platform → shell
   chain so per-resource confirmation status reaches the model.
5. **Rendering** — the model rendered through `tokeira-report` under the existing output
   contract, and emitted whole as a JSON artifact.
6. **Forward-compatible slots** — the fields Features 2–4 populate (semantics, cause,
   dependants, source location) exist in the schema from this feature, carrying explicit
   "not yet determined" values rather than being absent.

### What This Spec Covers

- The `tokeira-explain` crate: model types, evidence index, uncertainty, construction from
  engine outputs.
- Widening `refresh_state`'s report through `InfraEngine::plan*`,
  `InfraEngine::plan_destroy*`, `orchestrator::plan*`, and
  `ProvisionerPlatform::infra_plan`/`deploy_plan` so refresh status survives to the shell.
- Plan-side and apply-side model construction in `tokeira-provisioner-cli`.
- Rendering the model at summary and detail depth, and as `--json`.
- Writing the explanation artifact.
- Lexicon additions to `operator-language.md` for the new operator-facing vocabulary.

### What This Spec Does NOT Cover

- Kind-declared change semantics (Feature 2, `explanation-change-semantics`) — this spec
  defines the slot and its `Unknown` default only.
- Cause classification and causal grouping (Feature 3, `explanation-causality`) — slot
  only.
- Dependant sets and blast radius (Feature 3) — slot only.
- Source spans (Feature 4, `explanation-source-spans`) — slot only; this spec permits
  revision-level attribution to be absent.
- The analysis protocol and any query surface (Feature 5).
- Any agent client, narration, or model integration (Feature 6).
- Changing what the engine *decides*: this spec surfaces the engine's existing
  determinations and adds no new provider calls, no new diffing, and no new mutation.

## Evidence From Current Code

| Fact | Anchor | Consequence for this spec |
|---|---|---|
| `refresh_state` builds a `RefreshReport { state, status_by_id, has_managed_missing }`, classifying each resource | `crates/tokeira-iac/src/engine.rs` | The uncertainty source already exists and needs no new provider calls |
| `RefreshReport` carries `#[allow(dead_code)] // status_by_id retained for diagnostic reporting` | `crates/tokeira-iac/src/engine.rs` | The statuses are computed, deliberately retained, and consumed by nothing |
| `plan_with_known` binds `let refreshed = refresh_state(...)`, assigns `ctx.state = refreshed.state`, and returns only `Vec<Change>` | `crates/tokeira-iac/src/engine.rs` | **Seam 1**: statuses are dropped one line after they are computed |
| `InfraEngine::plan` / `plan_for_modules` / `plan_destroy*` all return `Result<Vec<Change>>` | `crates/tokeira-iac/src/engine.rs` | **Seam 2**: the engine's public plan surface cannot express confirmation status |
| `orchestrator::plan` / `plan_destroy` return `Result<Vec<iac::Change>>` | `crates/tokeira-orchestrator/src/lib.rs` | **Seam 3** |
| `ProvisionerPlatform::infra_plan` returns `Result<Vec<Change>>`; `deploy_plan` returns `Realization<Vec<Change>>` | `crates/tokeira-provisioner-cli/src/lib.rs` | **Seam 4**: the platform seam the shell consumes |
| `PlanReport` renders platform, binding, and changes through `tokeira_report::Report` | `crates/tokeira-provisioner-cli/src/render.rs` | The renderer this model extends; the contract it must satisfy |
| `InternalChange::Update`/`Replace` carry `Vec<FieldDiff>`; `internal_change_to_flat` passes them through | `crates/tokeira-iac/src/lib.rs`, `engine.rs` | Field evidence already reaches `Change.details` |
| `FieldDiff::observation` represents a named change with no captured values | `crates/tokeira-iac/src/types.rs` | The "known to differ, values not captured" case is already modelled |
| Apply returns `Vec<ChangeLogEntry>` — **ids and ops only**, never before-images (Proposal 002) | `crates/tokeira-provisioner-cli/src/lib.rs`, `apply.rs` | Apply-side explanation is necessarily thinner than plan-side and MUST NOT fabricate before-images |
| The envelope carries `deployment_id`, `config_revision`, `effective_config_ref`, and the binding | `crates/tokeira-provisioner/src/*` | Deployment identity and revision numbers are available at model-construction time |
| `tokeira-report` provides `Depth`, `Form`, `Mode`, `Report`, `render`, `counted`, `symbol` | `crates/tokeira-report/src/lib.rs` | Rendering infrastructure exists; explanation implements `Report` |

## Target State

An operator running a plan receives a report derived entirely from a structured model. A
machine consumer receives that same model verbatim. Where the engine could not confirm
something, both say so. Nothing in the path requires a network, a credential, or a model.

The model carries slots for the facts Features 2–4 will supply, so those features add
data without changing the schema's shape.

## Glossary

Terms additional to the umbrella glossary:

- **Explanation Model** — the concrete Rust type produced by this spec
  (`DeploymentExplanation`), its serialized JSON form, and its schema version.
- **Slot** — a model field reserved for a later feature, present from this spec and
  carrying an explicit not-determined value.
- **Refresh Status** — the engine's per-resource classification of whether live state was
  confirmed: confirmed present, differing, managed-missing, or unknown.
- **Confirmation Coverage** — the proportion of the plan's resources whose live state the
  engine confirmed; the summary-level expression of uncertainty.
- **Artifact** — the JSON serialization of one explanation model, written to a file.

## Explanation Model Field Policy

Every field, its source of truth, the depth at which it renders, and what it holds when
its source cannot supply it. No field may be silently omitted.

### `DeploymentExplanation`

| Field | Source | Renders at | When unavailable |
|---|---|---|---|
| `schema_version` | This spec (constant) | never (JSON only) | n/a — always present |
| `deployment` | Envelope `deployment_id` | summary | Platform-derived identity; never empty |
| `platform` | `ProvisionerPlatform::label` | summary | n/a |
| `operation` | The verb producing it (`infra plan`, `infra apply`, …) | summary | n/a |
| `current_revision` | Envelope `config_revision` | summary | `0` for a never-applied deployment |
| `proposed_revision` | `current_revision + 1` for a mutating verb | detail | absent for read-only verbs |
| `definition_ref` | Envelope `effective_config_ref` | detail | `"default"` when no definition exists |
| `changes[]` | Engine `Vec<Change>` (plan) / `Vec<ChangeLogEntry>` (apply) | summary (counts + acting), detail (all) | empty vector, rendered as "no changes" |
| `impacts[]` | Derived from change kinds + semantics slots | summary | empty until Feature 2 supplies semantics |
| `destructive[]` | `ChangeKind::is_destructive` over `changes` | summary | empty vector |
| `uncertainties[]` | Refresh statuses, unknown semantics, provider-assigned values | summary (presence), detail (each) | empty vector, rendered as full confirmation |
| `evidence` | Built during construction | never (JSON + citations) | n/a — always present |

### `ExplainedChange`

| Field | Source | Renders at | When unavailable |
|---|---|---|---|
| `evidence_id` | Construction | detail | n/a |
| `resource_id` | `Change.resource` | summary | n/a |
| `module` | `Change.module` | summary | n/a |
| `resource_type` | `Change.resource_type` | `--json` only *(2026-07-29: the type annotation left the narrative)* | n/a |
| `kind` | `Change.kind` | summary (as the action section) *(2026-07-29: symbols retired from the plan)* | n/a |
| `display` | Kind-declared noun (+ instance name) *(added 2026-07-29)* | summary — "the *tokeirad* service" | `None` — falls back to the engine id |
| `field_diffs[]` | `Change.details` | detail | empty vector |
| `refresh_status` | Refresh report (new plumbing) | detail | `Unknown`, which itself produces an uncertainty |
| `semantics` | **Slot** — Feature 2 | detail | all fields `Unknown` |
| `cause` | **Slot** — Feature 3 | summary once populated | `Undetermined` |
| `dependants[]` | **Slot** — Feature 3 | detail once populated | empty vector |
| `source` | **Slot** — Feature 4 | detail once populated | `None` |

### `Uncertainty`

| Field | Source | Renders at | When unavailable |
|---|---|---|---|
| `evidence_id` | Construction | detail | n/a |
| `subject` | The `EvidenceId` it qualifies | detail | n/a — an uncertainty without a subject is invalid |
| `reason` | Typed enum (unconfirmed live state, provider-assigned value, undeclared semantics, unsupported describe) | summary (aggregated), detail (each) | n/a |
| `consequence` | What the operator cannot rely on as a result | detail | n/a — required |
| `resolvable_by` | The action that would resolve it | detail | `None` where nothing resolves it |

## Requirements

### Requirement 1: The explanation model

**User Story:** As an operator, I want every plan to produce a complete structured account
of what it found and what it would do, so that the report I read and the data a tool
consumes are the same thing.

#### Acceptance Criteria

1. WHEN a plan verb runs to completion THE provisioner SHALL construct one explanation
   model containing every field in the Explanation Model Field Policy.
2. THE explanation model SHALL carry an explicit schema version.
3. THE explanation model SHALL contain exactly one explained change per engine change,
   including changes of kind `NoChange`.
4. THE explanation model SHALL be constructed without any provider call beyond those the
   engine already performs for the verb.
5. THE explanation model SHALL be constructed without network access, provider
   credentials, or any language model.
6. WHERE a field's source cannot supply a value THE explanation model SHALL carry the
   documented fallback from the field policy rather than omitting the field.

### Requirement 2: Apply-side explanation within the ids-only constraint

**User Story:** As an operator, I want the record of what an apply committed, so that I
can see the outcome in the same terms as the plan that preceded it.

#### Acceptance Criteria

1. WHEN an apply verb completes THE provisioner SHALL construct an explanation model whose
   changes are the committed change-log entries.
2. THE apply-side explanation model SHALL NOT contain before-images of committed
   resources.
3. WHERE the apply was preceded by a plan in the same invocation THE explanation model MAY
   carry that plan's field evidence for the same resource ids.
4. IF no plan preceded the apply in the same invocation THEN THE explanation model SHALL
   record the absence of field evidence as an uncertainty rather than presenting the
   change as evidence-free.

### Requirement 3: Stable, deterministic evidence identity

**User Story:** As a tool author, I want to reference a fact by identifier and have that
reference remain valid, so that citations mean something.

#### Acceptance Criteria

1. THE explanation model SHALL assign a unique `EvidenceId` to every explained change,
   uncertainty, impact, and destructive action.
2. WHEN two explanation models are constructed from identical engine inputs THE
   provisioner SHALL assign identical `EvidenceId`s to corresponding facts.
3. WHEN an `EvidenceId` is presented to the evidence index THE index SHALL resolve it to
   exactly one fact or report that it is unknown.
4. THE `EvidenceId` SHALL NOT encode a memory address, an iteration order that varies
   between runs, or a wall-clock value.
5. WHERE the same resource appears in two explanation models for the same deployment and
   revision THE `EvidenceId` for its change SHALL be identical.

### Requirement 4: Uncertainty is modelled from the sources that exist

**User Story:** As an operator, I want to know when Tokeira could not confirm live state,
so that I can tell a confident plan from an uninformed one.

#### Acceptance Criteria

1. WHEN the engine classifies a resource's refresh status as unknown THE explanation model
   SHALL contain an uncertainty naming that resource, the reason, and the consequence for
   the plan's claims about it.
2. WHEN a resource's `describe` returns unsupported THE explanation model SHALL record the
   reason as an unconfirmed live-state check rather than as an error.
3. WHEN an explained change's semantics slot carries `Unknown` for a field the renderer
   would otherwise state THE explanation model SHALL record an uncertainty for that field.
4. THE uncertainty record SHALL carry a subject that resolves in the evidence index.
5. IF the plan contains no uncertainties THEN THE renderer SHALL state full confirmation
   in the report's header assurance line: `**Plan for {platform}** with *live state*
   confirmed`. *(Amended 2026-07-29, operator-directed: the assurance joins the header,
   styled with the document, replacing the trailing fact line.)*
6. WHERE live-state uncertainties exist THE header SHALL state the coverage (`with *live
   state* unconfirmed for N resources` / `without *live state* examined`) and the
   affected changes SHALL carry the statement in place at detail depth; undeclared-
   semantics uncertainties are machine-channel only and never render (change-semantics
   Req 6.5). *(Amended 2026-07-29.)*

### Requirement 5: Refresh status reaches the shell

**User Story:** As a maintainer, I want the engine's confirmation statuses to survive to
the surface, so that uncertainty has a source instead of a placeholder.

#### Acceptance Criteria

1. THE engine's plan surface SHALL return the per-resource refresh statuses alongside the
   changes.
2. THE orchestrator's plan surface SHALL return the per-resource refresh statuses
   alongside the changes.
3. THE platform seam's plan methods SHALL return the per-resource refresh statuses
   alongside the changes.
4. WHEN the platform seam returns refresh statuses THE shell SHALL construct the
   explanation model's uncertainties from them.
5. WHERE the engine performs no refresh for a verb THE returned statuses SHALL be empty
   and the explanation model SHALL record that live state was not examined.
6. THE widened plan surface SHALL NOT change which changes the engine computes.

### Requirement 6: Rendering under the output contract

**User Story:** As an operator, I want the explanation to read like every other Tokeira
report, so that I learn one product, not two.

#### Acceptance Criteria

*(Amended 2026-07-29, operator-directed: the narrative form is deterministic Markdown —
rendered for the terminal via `termimad`, emitted raw when stdout is not a TTY — replacing
the counted-summary text form. Sections carry the actions; lines carry no glyphs, no
counts, and no type annotation; ids appear once.)*

1. WHEN rendering at summary depth THE renderer SHALL emit a Markdown document: the
   `# {verb}` title, the header assurance line (`**Plan for {platform}** with *live
   state* …`), one `## {Action}` section per present action listing each change as
   templated would-mood prose ("the *{name}* {kind} would be {verb}") with its engine id
   stated once, and the `## Impacts` section — and SHALL NOT enumerate per-change
   semantics or render counts.
2. WHEN rendering at detail depth THE renderer SHALL additionally state per-change field
   evidence (as code spans), the declared behaviour in its confidence voice with
   citations, in-place live-state statements where unconfirmed, and the `## Unchanged`
   section.
3. WHEN `--json` is requested THE renderer SHALL emit the complete explanation model
   regardless of depth.
4. THE renderer SHALL render through `tokeira-report` and SHALL NOT assemble narrative
   outside a `Report` implementation.
5. THE renderer SHALL use only vocabulary defined in `operator-language.md` in prose;
   code identifiers appear only inside code spans and citation links.
6. THE renderer SHALL NOT render a slot that carries a not-determined value as though it
   were a determination.
7. WHEN counts are rendered (the header's coverage state) THE renderer SHALL compute
   plurals and SHALL attach the noun to every number.
8. THE renderer SHALL name resources by their descriptive names — the kind noun, joined
   by the instance name only when the plan holds more than one resource of that kind —
   with Markdown emphasis per the target transcripts. *(Amended 2026-07-29; the
   descriptive-name slot joins the field policy.)*

### Requirement 7: The artifact

**User Story:** As a CI system, I want the explanation as a file, so that I can gate on it
without scraping terminal output.

#### Acceptance Criteria

1. WHEN the operator requests an explanation artifact THE provisioner SHALL write the
   complete explanation model as JSON to the requested path.
2. THE artifact SHALL contain the schema version.
3. THE artifact SHALL be self-contained: every `EvidenceId` referenced within it SHALL
   resolve within it.
4. THE artifact SHALL NOT contain secret values.
5. THE provisioner SHALL NOT open a socket, port, or listening stream in order to produce
   or serve the artifact.
6. IF the artifact cannot be written THEN THE provisioner SHALL fail the verb with the
   path and the underlying reason, and SHALL NOT report the verb as succeeded.

### Requirement 8: Forward-compatible slots

**User Story:** As a maintainer, I want later features to add data without reshaping the
schema, so that consumers written today keep working.

#### Acceptance Criteria

1. THE explanation model SHALL define the semantics, cause, dependants, and source fields
   in this feature.
2. WHERE a slot's owning feature is not yet implemented THE slot SHALL carry its
   documented not-determined value.
3. WHEN a slot is populated by a later feature THE schema version SHALL NOT require a
   breaking change for consumers that ignore the slot.
4. THE renderer SHALL omit a not-determined slot from narrative output rather than
   rendering it as empty or unknown text.

### Requirement 9: Placement and dependency discipline

**User Story:** As a maintainer, I want the model to live where every consumer can reach
it without inheriting the provisioner, so that Features 5–6 need no restructuring.

#### Acceptance Criteria

1. THE explanation model SHALL live in a library crate depending only on the engine types
   it models and serialization support.
2. THE explanation model crate SHALL NOT depend on any language-model client, HTTP client,
   or network transport.
3. THE explanation model crate SHALL NOT depend on the provisioner shell.
4. THE rendering implementation MAY live with the shell's other `Report` implementations.
5. WHERE a consumer outside `tkp` needs the model THE consumer SHALL be able to depend on
   the model crate alone.

### Requirement 10: Lexicon additions

**User Story:** As an operator, I want new report vocabulary defined where all the other
vocabulary is defined, so that the language stays one language.

#### Acceptance Criteria

1. WHERE this feature introduces operator-facing vocabulary THE change SHALL add those
   terms to `operator-language.md` in the same change.
2. THE added terms SHALL include uncertainty, confirmation coverage, impact, and evidence.
3. THE renderer SHALL NOT emit a term absent from the lexicon.

## Notes

- Requirement 5 is the enabling change and touches four crates; it is deliberately
  specified as behaviour-preserving (5.6) so it can land and be verified before any
  explanation output depends on it.
- Requirement 2's ids-only constraint is a Proposal 002 boundary, not a limitation to
  design around: apply reports what it committed; the plan that preceded it is where
  before-images live.
- Requirement 8 exists because Features 2–4 each add a dimension to the same changes. The
  cost of reserving the slots now is a few `Unknown` variants; the cost of not reserving
  them is three schema breaks against consumers built on Feature 5.
