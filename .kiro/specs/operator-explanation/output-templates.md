# Output Templates

The single owner of `tkr`/`tkp` templated report output, stating the agreed form in its
entirety: every transcript in this document obeys the rules below. A rendering change
is an amendment to **this document first**; the renderer follows, and
`render.rs::the_output_templates_doc_is_executable` asserts the renderer's output
equals the reference transcripts byte-for-byte.

Companions: vocabulary lives in
[operator-language.md](../../../docs/platforms/operator-language.md); shape rules
(depth, form, collapse) in
[operator-output-contract.md](../../../docs/platforms/operator-output-contract.md).
Umbrella decision D10 records this document as the output authority.

## The delivery map

Every example carries a *what's speaking* list attributing each narrative element to
its area of the umbrella ([requirements.md](requirements.md)) and the **responsible
module**, so realisability is checkable, never assumed.

| Key | Umbrella area | Narrative aspects | Responsible modules |
|---|---|---|---|
| **F1** | the model (Reqs 1–3, 9) | the document frame: title, header + revision anchor, action sections, would-mood lines with ids once, field diffs, `## Unchanged`, `--json`/artifact parity | model + construction: `tokeira_explain::{model, build, evidence, artifact}` · field-evidence transport: `tokeira_iac::engine` (`compute_changes` → `Change::details`) · rendering: `tokeira_provisioner_cli::render` |
| **F2** | declarations (Reqs 4, 6) | declared behaviour, kind-authored statements, confidence voices, impacts, display nouns | declarations live with their owning kinds: `tokeira_compose` (the compose service), `tokeira_aws::resources::{dsql_cluster, dynamodb_table}`, `tokeira_compose_deployment::{kinds, observability_config}` · vocabulary: `tokeira_iac::semantics` · impact derivation: `tokeira_explain::impacts` · voices: `tokeira_provisioner_cli::render` (`voiced`) |
| **F3** | causality (Req 5) | cause clauses, drift lines, per-diff drift annotations, dependants, chains, why-unknown | classifier, grouping, dependants, per-field departure (each diff checked against the record): `tokeira_explain::causality` (the algebra) · live-departure + edge seams: `tokeira_iac::engine` (`refresh_state`, `PlanOutcome`) · clause and detail rendering: `tokeira_provisioner_cli::render` (`cause_phrase`, `change_detail`) |
| **platform** | platform issues (Req 7) | `## Platform Issue[s]`: the typed issue, verbatim SDK evidence, established-only directions | the typed issue at the describe boundary: `tokeira_iac` (describe surface) and each platform's SDK seam (`tokeira_compose` for Docker) · carried through `PlanOutcome` into `tokeira_explain::model` · section rendering: `tokeira_provisioner_cli::render` · per-error-class direction tables declared by the owning platform under D3/D4 discipline |

## The rules

1. **A plan only renders what could execute.** An unreachable platform yields the
   document frame with `## Platform Issue[s]` and **no change sections** — never a
   record-based plan: updating a recorded resource presupposes describing it, and an
   apply would hit the same wall the describe hit.
2. **Sections split by source.** The action sections carry what the definition does
   (edits, their cascades, provisioner advances); `## Resource Drift` carries what the
   world did outside the definition, and what the plan does about it.
3. **Lines state the concrete change.** Never a cause category, never a revision
   number — the header anchors the revision once, and carries no coverage clause
   (rule 1 makes it invariant).
   Computed plurals apply everywhere, **headings included**: `## Impact` /
   `## Impacts`, `## Platform Issue` / `## Platform Issues`.
4. **Detail states the why**, in the declaration's confidence voice, only where it adds
   information beyond the line.

Two conditions are **definition-verification concerns** (`tkd` verify refuses them;
they never reach a plan): a kind whose `describe` is unimplemented, and a dangling
dependency — a definition keeping a dependant of a resource it removes.

## The plan document

```text
# {Verb}                       — the operation, title-cased ("Infra Plan")
**Plan for {platform}** {revision anchor}
[**binding:** {attention line}]  — only when something blocks or qualifies the apply
## Platform Issue[s]           — first when present; suppresses every section below
## {Action} sections           — Create / Update / Replace / Delete, definition-driven
## Resource Drift              — what moved outside the definition; present only when drift exists
## Unchanged                   — detail depth only
## Impact[s]                   — severity-first, always last
```

The revision anchor is `at revision {N}`, or `before its first apply` for a
never-applied deployment — the one place the document states a revision number.

### Platform Issue lines

Each issue: the fact, then the SDK error verbatim as evidence, then — **only when the
error itself establishes one** — a factually grounded direction. Direction is never
assumed: `ECONNREFUSED` on a socket establishes refusal at that path, not that the
daemon is stopped.

```text
- {the fact, naming the platform component}:
  - `{the SDK error, verbatim}`
  - {direction the error establishes — omitted when it establishes none}
```

### Change lines

```text
- {noun phrase} would be {verb}[ - {clause}] - `{module}::{id}`   (kinds with a display noun)
- `{module}::{id}` would be {verb}[ - {clause}]                    (kinds without one)
```

| Cause | Change kind | Clause |
|---|---|---|
| the operator's own edit | Update / Replace | `` `field`: `a` → `b` ``[` and {N} more fields`] — the first diff; the field name alone when values were not captured |
| the operator's own edit | Create / Delete | *(none — the verb states the change; the definition being its source is implicit)* |
| provisioner advance | any | `this provisioner realizes the definition differently` |
| replacement cascade | any | `forced by {noun phrase} replacement` — the noun phrase carries its own article |
| dependency output | any | `an output of {noun phrase} changed` |
| unknown | any | *(none — detail carries the fact, in operator words)* |

Drifted resources do not appear in the action sections — they render in
`## Resource Drift`.

### Resource Drift lines

What the world did, then what the plan does about it — one line per drifted resource.
Only definition-unchanged resources enter this section: a resource that is **both
edited and drifted** stays in its action section (the edit owns the change), with its
drifted fields annotated per diff at detail.

```text
- {noun phrase}'s `field`[ and {N} more fields] changed outside the definition - it would be {restored|updated|replaced} - `{module}::{id}`
- {noun phrase} could not be found - it would be recreated - `{module}::{id}`
```

### Detail sub-bullets, in order

1. Field diffs: `` - `field`: `a` → `b` `` — or the field name alone for an observed
   change without captured values. A diff whose live value departed from the record is
   annotated in place: `` - `field`: `a` → `b` - changed outside the definition `` —
   the edit owns the change; the drift fact stays visible per field.
2. Declared behaviour in its voice: mechanism (or the kind-authored statement), data
   effect, reversibility (the voices below).
3. The cause's voice line, **only where it adds information beyond the line clause**:
   - derived causes: `- why: {clause}; Tokeira derives this - per {cite}`
   - unknown cause with a missing/broken baseline: `- why this differs is unknown -
     revision {N}'s definition is not retained for comparison`
   - engine-fact causes render no `why:` line — their concrete change is the line.
   The machine channel (model, artifact) keeps the full consequence text for agents
   and CI; narrative never lifts it.
4. Dependants: `- dependants changing with it: {noun phrases}` ·
   `- dependants continuing unchanged: {noun phrases}` — each omitted when empty.
5. The chain, once, on a multi-member group's first member:
   `- chain: {root}: {member}, then {member}…` — root is `the definition change`,
   `the first apply of this definition`, `the provisioner advance`, or a noun phrase.

### Voices

Shared with semantics rendering: an engine fact `{claim} - {cite}` · a provider
guarantee `{claim} - per {cite}` · an inference
`{claim}; Tokeira derives this - per {cite}`. Citations: documentation as a Markdown
link, code by module identity as a code span.

### Impact lines

Heading pluralized by count. One templated line per subject, severity-first,
descriptive names only (ids live in the action sections):
`data held by {noun} would be destroyed[, irreversibly]` ·
`{noun} would be unavailable while the change applies` / `would no longer be available`
(deletes) · `{noun} would be replaced` · `would be briefly interrupted` ·
`would be replaced one at a time` (rolling). Irreversibility is stated only where every
contributing declaration establishes it.

Impacts derive from **two sources**: declared semantics, and the engine's own change
classification — every `Replace` carries unavailability-during-the-change and the
replacement itself as engine facts (the engine executes a replace as delete-then-
create); every `Delete` carries no-longer-available; and a resource whose desired
state drops its recorded dependency on a deleted resource carries **dependency
loss** — `would continue without {the deleted noun}` — an engine fact from the graph
delta. A declaration refines the engine fact (a provider-guaranteed
create-before-destroy lifts the unavailability window); it never has to exist for the
floor to render. Within a resource's line, permanent consequences precede transient
ones: data destruction, then dependency loss, then unavailability, then replacement.

**Impacts group per resource** — one line per resource, carrying every impact on it:
consequences merge severity-first as verb phrases sharing the would-mood (`would be
unavailable while the change applies, and replaced`). Data destruction leads its line
(`data held by {noun} would be destroyed[, irreversibly]`), with further consequences
back-referencing (`, and it would no longer be available`). Resources are ordered by
their most severe impact, ties by evidence id; the heading is pluralized by count.

## Reference transcripts

The reference world, at revision 4 on compose: the *grafana* service's image edited
(its compose declaration speaking); the *Aurora DSQL cluster*'s deletion protection
edited (a replacement) cascading into the *tokeirad* service; the *mimir* service's
environment changed outside the definition; the *alloy* service an unchanged
dependant of mimir. These transcripts are the byte-for-byte assertion targets for
`render.rs::the_output_templates_doc_is_executable`.

<!-- reference: infra-plan-summary -->
```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *grafana* service would be updated - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0` - `grafana::compose/grafana`

## Replace
- the *tokeirad* service would be replaced - forced by the *Aurora DSQL cluster* replacement - `tokeirad::compose/tokeirad`
- the *Aurora DSQL cluster* would be replaced - `deletion_protection`: `enabled` → `disabled` - `storage::dsql/cluster`

## Resource Drift
- the *mimir* service's `environment` changed outside the definition - it would be updated - `mimir::compose/mimir`

## Impacts
- the *grafana* service would be unavailable while the change applies, and replaced
- the *Aurora DSQL cluster* would be unavailable while the change applies, and replaced
- the *tokeirad* service would be unavailable while the change applies, and replaced
```

*What's speaking:*

- the anchor-only header — F1, `tokeira_provisioner_cli::render` (`revision_anchor`)
- grafana's diff clause — F3 verdict (`tokeira_explain::causality`, row A5) over F1's
  evidence transport (`tokeira_iac::engine`), rendered by `render` (`diff_clause`)
- the cascade clause — F3 (row A6), `render` (`cause_phrase`)
- the cluster's diff clause — F3 (row A5) over F1's evidence
- `## Resource Drift` — F3 (row A7, over `tokeira_iac::engine::refresh_state`'s
  `live_departed`), a drift section emitter in `render`
- the impacts — F2, two sources: grafana's from its compose declaration
  (`tokeira_compose`); tokeirad's and the cluster's as engine facts from
  `ChangeKind::Replace` (the engine's delete-then-create) — both derived by
  `tokeira_explain::impacts`, rendered by `render` (`impacts_section`)

<!-- reference: infra-plan-detail -->
```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *grafana* service would be updated - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0` - `grafana::compose/grafana`
  - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0`
  - the update replaces it - it would be stopped, removed, and recreated from the definition - `tokeira_compose::reconcile`
  - the data it holds would be preserved - `tokeira_compose::reconcile`
  - re-applying the prior definition would restore it - `tokeira_compose::reconcile`
  - chain: the definition change: the *grafana* service, then the *Aurora DSQL cluster*, then the *tokeirad* service

## Replace
- the *tokeirad* service would be replaced - forced by the *Aurora DSQL cluster* replacement - `tokeirad::compose/tokeirad`
  - why: forced by the *Aurora DSQL cluster* replacement; Tokeira derives this - per `tokeira_explain::causality`
- the *Aurora DSQL cluster* would be replaced - `deletion_protection`: `enabled` → `disabled` - `storage::dsql/cluster`
  - `deletion_protection`: `enabled` → `disabled`
  - dependants changing with it: the *tokeirad* service

## Resource Drift
- the *mimir* service's `environment` changed outside the definition - it would be updated - `mimir::compose/mimir`
  - environment
  - dependants continuing unchanged: the *alloy* service

## Unchanged
- the *alloy* service - `alloy::compose/alloy`

## Impacts
- the *grafana* service would be unavailable while the change applies, and replaced
- the *Aurora DSQL cluster* would be unavailable while the change applies, and replaced
- the *tokeirad* service would be unavailable while the change applies, and replaced
```

*What's speaking:*

- grafana's mechanism, data effect, and reversibility — F2: declared by the compose
  service kind (`tokeira_compose`, its `change_semantics`), voiced with citations by
  `render` (`voiced`)
- the one-story chain across both edits and the cascade — F3: bounded ultimate-root
  grouping in `tokeira_explain::causality`, told once by `render` (`change_detail`)
- the cascade's derived `why:` — F3, `render` voicing the inference
- dependants both ways — F3: the reverse union graph in `tokeira_explain::causality`
  over `PlanOutcome::edges_by_id` (`tokeira_iac::engine`)
- drift's detail — F3 over F1's field evidence, under the drift section
- `## Unchanged` — F1, `render` (`action_sections`)

## Worked examples

Every example is a **complete output**, never a snippet.

### The platform cannot be reached

<!-- reference: infra-plan-platform-issue -->
```markdown
# Infra Plan
**Plan for compose** at revision 4

## Platform Issue
- Unable to connect to Docker:
  - `connect ECONNREFUSED /var/run/docker.sock`
  - **nothing accepted connections at `/var/run/docker.sock` - verify Docker is listening there**
```

The verb exits non-zero; `--json` carries the issue in the model.

*What's speaking:*

- the document frame and anchor — F1, `render` (`revision_anchor`)
- the issue, its verbatim SDK evidence, and the established-only direction —
  platform: the typed issue at the describe boundary (`tokeira_iac` +
  `tokeira_compose`'s Docker seam), carried through `PlanOutcome` into
  `tokeira_explain::model`, rendered by `render`; the direction from the compose
  platform's declared error-class table

### Several fields of one resource change (detail depth)

The definition edits grafana's image, log level, and published ports in one pass — one
resource, one line, every diff at detail.

```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *grafana* service would be updated - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0` and 2 more fields - `grafana::compose/grafana`
  - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0`
  - `environment`: `GF_LOG_LEVEL=info` → `GF_LOG_LEVEL=debug`
  - `publish`: `3000` → `3000, 3001`
  - the update replaces it - it would be stopped, removed, and recreated from the definition - `tokeira_compose::reconcile`
  - the data it holds would be preserved - `tokeira_compose::reconcile`

## Impact
- the *grafana* service would be unavailable while the change applies, and replaced
```

*What's speaking:*

- the clause: the first diff with the computed more-fields tail — F3
  (`render::diff_clause`, `more_fields`) over F1's evidence transport; the engine
  emits **one change per resource** — several edits to one resource are one line with
  its diffs, never several lines
- every diff at detail — F1 (`render::change_detail`)
- the mechanism and data effect — F2, the compose declaration
- the single-resource impact under its computed-singular heading — F2

### A resource both edited and drifted (detail depth)

The definition bumps grafana's image while its environment was also changed by hand:
the edit owns the change (one cause per change; the definition is being applied), and
the drifted field is annotated where it appears.

```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *grafana* service would be updated - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0` and 1 more field - `grafana::compose/grafana`
  - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0`
  - `environment`: `GF_LOG_LEVEL=debug` → `GF_LOG_LEVEL=info` - changed outside the definition
  - the update replaces it - it would be stopped, removed, and recreated from the definition - `tokeira_compose::reconcile`

## Impact
- the *grafana* service would be unavailable while the change applies, and replaced
```

*What's speaking:*

- the edit clause — F3 (content rows outrank state rows: one cause per change,
  `tokeira_explain::causality`)
- the per-diff drift annotation — F3: each diff's live value checked against the
  record (`tokeira_explain::causality`, over `tokeira_iac::engine`'s refresh and the
  store-read S), rendered in place by `render` (`change_detail`) — the section
  partition holds (the resource never enters `## Resource Drift`) while the
  hand-change stays visible
- the mechanism — F2, the compose declaration

### A service joins the definition

```markdown
# Infra Plan
**Plan for compose** at revision 4

## Create
- the *pyroscope* service would be created - `pyroscope::compose/pyroscope`
```

*What's speaking:*

- the create line with no clause — F1 line template (`render`, `action_sections`)
  with F3 withholding (edit-creates carry no clause: `render::cause_phrase`)

### Removing DSQL storage (detail depth)

Switching `storage` from DSQL to in-memory removes the whole dsql module — the
cluster and its two DynamoDB tables — and tokeirad's AWS runtime edge leaves its
stanza with it: tokeirad's own diffs carry the consequence. (A definition that removed
the cluster while a dependant still referenced it would not verify — a dangling
dependency is a definition-verification concern, refused by `tkd` verify, never a
plannable world. That guarantee is what lets impacts speak entirely through changes.)

```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *tokeirad* service would be updated - `environment`: `AWS_REGION=us-east-1` → `(removed)` and 1 more field - `tokeirad::compose/tokeirad`
  - `environment`: `AWS_REGION=us-east-1` → `(removed)`
  - `volumes`: `~/.aws:/home/nonroot/.aws:ro` → `(removed)`
  - the update replaces it - it would be stopped, removed, and recreated from the definition - `tokeira_compose::reconcile`

## Delete
- the *Aurora DSQL cluster* would be deleted - `dsql::dsql/cluster`
  - deletion protection would be disabled first, then the cluster deleted - per [DeleteCluster](https://docs.aws.amazon.com/dsql/latest/APIReference/API_DeleteCluster.html)
  - its stored data would be destroyed; Tokeira derives this - per [Restoring an Aurora DSQL cluster](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-auroradsql.html)
  - it could not be reversed - per [Restoring an Aurora DSQL cluster](https://docs.aws.amazon.com/aws-backup/latest/devguide/restore-auroradsql.html)
  - chain: the definition change: the *Aurora DSQL cluster*, then `dsql::dsql/conn-lease`, then `dsql::dsql/rate-limiter`, then the *tokeirad* service
- `dsql::dsql/conn-lease` would be deleted
  - its stored data would be destroyed - per [DeleteTable](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html)
  - it could not be reversed - per [Point-in-time recovery](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery_Howitworks.html)
- `dsql::dsql/rate-limiter` would be deleted
  - its stored data would be destroyed - per [DeleteTable](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html)
  - it could not be reversed - per [Point-in-time recovery](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery_Howitworks.html)

## Impacts
- data held by the *Aurora DSQL cluster* would be destroyed, irreversibly, and it would no longer be available
- data held by `dsql::dsql/conn-lease` would be destroyed, irreversibly, and it would no longer be available
- data held by `dsql::dsql/rate-limiter` would be destroyed, irreversibly, and it would no longer be available
- the *tokeirad* service would be unavailable while the change applies, and replaced
```

*What's speaking:*

- tokeirad's forced edit — F3 (the storage switch reaches it through its own stanza;
  the clause is its concrete environment diff, its volumes diff at detail) with F2's
  compose mechanism voice
- the clause-less delete lines — F3 withholding (`render::cause_phrase`); the tables
  render id-form (kinds without a display noun)
- the cluster's and tables' cited statements — F2: declared by
  `tokeira_aws::resources::{dsql_cluster, dynamodb_table}` (`change_semantics`, the
  researched citations), voiced by `render` (`voiced`) with `Doc` citations as links
- the one-story chain across all four edits — F3 (bounded ultimate-root grouping,
  told once on the group's first member)
- the impact lines, permanent consequences first — F2: the storage kinds'
  declarations with the engine-fact delete floor; tokeirad's engine-fact replace
  floor with the compose declaration (`tokeira_explain::impacts` →
  `render::impacts_section`)

### An output ripples downstream (detail depth)

```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *tokeirad* service would be updated - an output of the *Aurora DSQL cluster* changed - `tokeirad::compose/tokeirad`
  - `environment`: `DSQL_ENDPOINT=old.dsql.aws` → `DSQL_ENDPOINT=new.dsql.aws`
  - why: an output of the *Aurora DSQL cluster* changed; Tokeira derives this - per `tokeira_explain::causality`
```

*What's speaking:*

- the trace clause and its derived `why:` — F3 (row A4 in
  `tokeira_explain::causality`: the dependency-edge + state-diff gates), rendered by
  `render` (`cause_phrase`, `change_detail`)
- the diff — F1 evidence transport (`tokeira_iac::engine`)

### A provisioner upgrade re-realizes services (detail depth)

```markdown
# Infra Plan
**Plan for compose** at revision 5

## Update
- the *alloy* service would be updated - this provisioner realizes the definition differently - `alloy::compose/alloy`
  - chain: the provisioner advance: the *alloy* service, then the *grafana* service
- the *grafana* service would be updated - this provisioner realizes the definition differently - `grafana::compose/grafana`
```

*What's speaking:*

- the engine-advance clause — F3 (row A8: the change's existence is the D ≠ S
  evidence under a confirmed, matching live read), `tokeira_explain::causality` +
  `render` (`cause_phrase`)
- the chain under the provisioner advance — F3 grouping
  (`CausalRoot::ProvisionerAdvance`, the engine-version boundary), told once by
  `render` (`change_detail`)

### The baseline revision is not retained (detail depth)

```markdown
# Infra Plan
**Plan for compose** at revision 4

## Update
- the *mimir* service would be updated - `mimir::compose/mimir`
  - `environment`: `RETENTION=30d` → `RETENTION=90d`
  - why this differs is unknown - revision 4's definition is not retained for comparison
```

*What's speaking:*

- the diff — F1 evidence transport
- the clause-less line and the one-usable-fact unknown — F3 (row A10 in
  `tokeira_explain::causality`; the narrative line reads the `BaselineUnavailable`
  revision from the model), `render` (`change_detail`); the machine channel keeps the
  full uncertainty with its restore-or-apply resolution
  (`tokeira_explain::model::UncertaintyReason`) — F1

## The apply document

```text
# {Verb}                        — "Infra Apply"
**Applied to {platform}** — revision {N} → {N+1}[, without a gating plan's evidence]
## {Action} sections            — past tense: Created / Updated / Replaced / Deleted
```

### Apply after its gating plan

<!-- reference: infra-apply-after-plan -->
```markdown
# Infra Apply
**Applied to compose** — revision 4 → 5

## Updated
- the *grafana* service - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0` - `grafana::compose/grafana`

## Replaced
- the *Aurora DSQL cluster* - `storage::dsql/cluster`
```

*What's speaking:*

- the revision-advance header — F1, `tokeira_provisioner_cli::{apply, render}`
- the committed lines — F1: `tokeira_explain::build` (`explain_applied`) — ids only,
  never before-images, with field evidence reused from the gating plan where ids match

### Apply with no preceding plan in the run

<!-- reference: infra-apply-no-plan -->
```markdown
# Infra Apply
**Applied to compose** — revision 4 → 5, without a gating plan's evidence

## Updated
- the *grafana* service - `grafana::compose/grafana`
```

*What's speaking:*

- identities alone, the absence stated in the header — F1:
  `tokeira_explain::build` (`explain_applied`) records one
  field-evidence-unavailable uncertainty per committed change;
  `tokeira_provisioner_cli::{apply, render}` state it once
