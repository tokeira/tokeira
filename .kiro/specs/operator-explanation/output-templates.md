# Output Templates

The single owner of `tkr`/`tkp` templated report output. A rendering change is an
amendment to **this document first**: the child specs reference these templates instead
of restating them, and the reference transcripts below are **executable** —
`render.rs::the_output_templates_doc_is_executable` asserts the renderer's output equals
them byte-for-byte, so this document and the product cannot drift.

Companions: vocabulary lives in
[operator-language.md](../../../docs/platforms/operator-language.md); shape rules
(depth, form, collapse) in
[operator-output-contract.md](../../../docs/platforms/operator-output-contract.md).
Umbrella decision D10 records this document as the output authority.

## The plan document

```text
# {Verb}                       — the operation, title-cased ("Infra Plan")
**Plan for {platform}** {revision anchor}, {live-state coverage}
[**binding:** {attention line}]  — only when something blocks or qualifies the apply
## {Action} sections           — Create / Update / Replace / Delete, present kinds only
## Unchanged                   — detail depth only
## Impacts                     — severity-first, always last
```

### Header

- **Revision anchor** — the one place the document states a revision number:
  `at revision {N}`, or `before its first apply` for a never-applied deployment.
- **Coverage** — `with *live state* confirmed` · `with *live state* unconfirmed for
  {N} resource(s)` (computed plural) · `without *live state* examined`.

### Change lines

```text
- {noun phrase} would be {verb}[ - {clause}] - `{module}::{id}`   (kinds with a display noun)
- `{module}::{id}` would be {verb}[ - {clause}]                    (kinds without one)
```

The clause is the **concrete change, never a cause category**. No clause references a
revision number — the header anchors it once.

| Cause | Change kind | Clause |
|---|---|---|
| the operator's own edit | Update / Replace | `` `field`: `a` → `b` ``[` and {N} more fields`] — the first diff; the field name alone when values were not captured |
| the operator's own edit | Create / Delete | *(none — the verb states the change; the definition being its source is implicit)* |
| changed outside the definition (drift) | Update / Replace | `` `field` ``[` and {N} more fields`]` changed outside the definition` |
| changed outside the definition (drift) | Create | `missing from the platform` |
| provisioner advance | any | `this provisioner realizes the definition differently` |
| replacement cascade | any | `forced by the {noun phrase} replacement` |
| dependency output | any | `its {noun phrase} dependency's output changed` |
| unknown | any | *(none — the uncertainty speaks at detail)* |

### Detail sub-bullets, in order

1. Field diffs: `` - `field`: `a` → `b` `` — or the field name alone for an observed
   change without captured values.
2. `- its live state could not be confirmed` — when the read was unconfirmed.
3. Declared behaviour in its voice: mechanism (or the kind-authored statement), data
   effect, reversibility (change-semantics spec, Requirement 9).
4. The cause's voice line, **only where it adds information beyond the line clause**:
   - derived causes: `- why: {clause}; Tokeira derives this - per {cite}`
   - unknown causes: `- why: could not be established - {consequence}[; {resolution}]`
   - engine-fact causes render no `why:` line — their concrete change is the line.
5. Dependants: `- dependants changing with it: {noun phrases}` ·
   `- dependants continuing unchanged: {noun phrases}` — each omitted when empty.
6. The chain, once, on a multi-member group's first member:
   `- chain: {root}: {member}, then {member}…` — root is `the definition change`,
   `the first apply of this definition`, `the provisioner advance`, or a noun phrase.

### Voices

Shared with semantics rendering: an engine fact `{claim} - {cite}` · a provider
guarantee `{claim} - per {cite}` · an inference
`{claim}; Tokeira derives this - per {cite}`. Citations: documentation as a Markdown
link, code by module identity as a code span.

### Impacts lines

One templated line per subject, severity-first, descriptive names only (ids live in the
action sections): `data held by {noun} would be destroyed[, irreversibly]` ·
`{noun} would be unavailable while the change applies` / `would no longer be available`
(deletes) · `{noun} would be replaced` · brief-interruption and rolling forms per the
change-semantics spec, Requirement 9.8.

## Reference transcripts (executable)

The renderer's exact output for the reference fixture
(`render.rs::causality_reference_report`): an edited Replace (`m/alpha`), a cascading
Replace (`m/beta`), a drifted Update (`m/gamma`), an undecidable Update (`m/delta`),
and an unchanged dependant (`m/epsilon`) — engine-id line forms, since the fixture's
kinds declare no display nouns.

<!-- reference: infra-plan-summary -->
```markdown
# Infra Plan
**Plan for test** at revision 1, with *live state* unconfirmed for 1 resource

## Update
- `m::m/gamma` would be updated - `environment` changed outside the definition
- `m::m/delta` would be updated

## Replace
- `m::m/alpha` would be replaced - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0`
- `m::m/beta` would be replaced - forced by the `m::m/alpha` replacement
```

<!-- reference: infra-plan-detail -->
```markdown
# Infra Plan
**Plan for test** at revision 1, with *live state* unconfirmed for 1 resource

## Update
- `m::m/gamma` would be updated - `environment` changed outside the definition
  - environment
- `m::m/delta` would be updated
  - `image`: `a` → `b`
  - its live state could not be confirmed
  - why: could not be established - the change's origin could not be established: live state was not confirmed, so a departure cannot be told from the provisioner realizing the definition differently; confirm live state for this resource and plan again

## Replace
- `m::m/alpha` would be replaced - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0`
  - `image`: `grafana/grafana-oss:12.4.3` → `grafana/grafana-oss:12.5.0`
  - dependants changing with it: `m::m/beta`
  - dependants continuing unchanged: `m::m/epsilon`
  - chain: the definition change: `m::m/alpha`, then `m::m/beta`
- `m::m/beta` would be replaced - forced by the `m::m/alpha` replacement
  - `image`: `a` → `b`
  - why: forced by the `m::m/alpha` replacement; Tokeira derives this - per `tokeira_explain::causality`

## Unchanged
- `m::m/epsilon`
```
