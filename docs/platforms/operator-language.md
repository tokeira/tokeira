# Operator language — the words `tkr` and `tkp` use

The sibling of the [operator output contract](operator-output-contract.md): the contract
governs the *shape* of reports (depth, form, copy mechanics); this document governs the
*words*. Together they are the operator experience's constitution.

## The principle

**Operators meet a small, stable vocabulary drawn from what they can see and touch.**
Every word in a report names something in the operator's world — a file in the
deployment directory, a running resource, a numbered revision — never an internal type,
field, mechanism, or lifecycle phase. One concept, one word, identical in both binaries.

The test for any word: *could the operator point at the thing it names?* They can point
at `definition.tkd`, at a container, at revision 4 in the retained history. They cannot
point at an envelope, a marker, or a launch class — those name our machinery, and our
machinery is not their problem.

## The lexicon

The operator's nouns. Reports use these words and no synonyms.

| Term | Names | Replaces (banned in prose) |
|---|---|---|
| **deployment** | The named unit `tkr` manages: the deployment directory and everything it stands for. | — |
| **definition** | `definition.tkd` — what the deployment *is*: its services, resources, and configuration. "The definition" always means the deployment's own. | `.tkd` as a bare noun, config source, manifest |
| **server configuration** | `tokeirad.toml` — the server's policy deviations from stock. | config (unqualified) |
| **provisioner** | The deployment's own `tkp` binary — the engine that operates it. There is exactly one per deployment; `upgrade` replaces it. | married/bound binary, candidate, engine (in prose), tkp copy |
| **revision** | One entry in the deployment's forward-only history of applied definitions. Numbers never rewind; reverted content returns under a new number. | config_revision, envelope revision |
| **platform** | What realizes the deployment's resources (`compose`, `local`, `ecs`). | provider, backend |
| **resource** | One engine-managed unit, named `module::id` (`grafana::compose/grafana`). | physical id, state entry |
| **service** | A resource that runs — a container, a task. | workload unit |
| **plan** | The read-only statement of what an apply would do, counted by kind; **destructive** covers deletes and replacements. | delta, diff (as a noun in prose) |
| **evidence** | The field-level facts behind a change (`depends_on: [] → ["mimir","loki"]`) — what `--detail` shows. | details (as a label) |
| **verifies / does not verify** | The definition parses and interprets cleanly (`definition check`); failures carry a location. | valid/invalid, parse OK |
| **binding** | The recorded pairing of deployment and provisioner identity. Spoken **only** when it blocks a verb (mismatch, downgrade, mode regression) or the deployment is not yet initialized. | provenance stamp, verdict (as prose), gate |
| **retained** | Kept for return: retained revisions (`revert`), the retained prior provisioner (`rollback`). | checkpoint, snapshot (in prose) |
| **uncertainty** | Something the engine could not determine, stated with its consequence and (where one exists) the action that resolves it. An empty set is itself stated: "live state: fully confirmed". | unknown status, refresh status (in prose) |
| **live state** | What actually runs, as the engine examined it during a plan. Spoken with its coverage: "live state: fully confirmed" when every planned resource answered, "live state: unconfirmed" as per-resource evidence otherwise. | refresh, refreshed state (in prose) |
| **impact** | An operational consequence of a change, stated only with cited evidence — the engine speaks impacts it can substantiate, never speculation. Rendered as the `## Impacts` section, severity-first, speaking descriptive names. | side effect |
| **disruption** | What a change does to availability while it applies ("would be unavailable while the change applies"). | downtime (as a label), outage |
| **data effect** | What a change does to the data a resource holds — preserved, migrated, destroyed, or following the resource's own declared policy. | data loss (as a bare label) |
| **reversibility** | Whether a change could be undone — reversible, reversible with data loss, or not reversible. | rollbackable |
| **replacement** | A change effected by destroying and recreating the resource; the plan says so even when the engine classifies it as an update. | recreate (as a label) |
| **confidence** | How firmly a stated behaviour is held: the engine's own fact, the provider's documented guarantee, or a conclusion the engine derives — each carrying its citation. | certainty |
| **citation** | The reference behind a stated behaviour: the engine's module identity, or a product document (title, link, and the establishing sentence). | source (as a label) |
| **cause** | Why a change is in the plan, stated as a clause of its line ("the definition changed since revision 4") and, at detail, in its confidence voice as the `why:` line. An unknown cause speaks through its uncertainty, never as silence. | classification, algebra (in prose) |
| **drift** | Live state departing from what an applied revision recorded — spoken as the fact ("live state departed from what revision 4 applied"), never as jargon. | drifted (as a bare verb) |
| **dependant** | A resource that depends on a changing resource — stated at detail whether it changes with it or continues unchanged. | downstream, consumer (in prose) |
| **causal chain** | Changes sharing one root, told once as the `chain:` line — the root first, members along the dependency path. | cascade (in prose), causal group (in prose) |
| **root** | What a causal chain hangs from: the definition change, a resource's own change, or the provisioner advance. | ultimate root (in prose) |

The operator's verbs, with their one-line meanings:

| Verb | Means |
|---|---|
| **apply** | Reconcile the world to the definition. |
| **plan** | Show what apply would do, without doing it. |
| **revert** | Apply an earlier revision's definition — content returns, history moves forward. |
| **upgrade** | Replace the deployment's provisioner with a newer build; reconcile under it. |
| **rollback** | Return to the retained prior provisioner and its configuration. |
| **check** | Interpret the definition in memory; touch nothing. |
| **describe** | Report identity, binding, and state facts; never gates. |

## The banned list

Internal vocabulary that must never appear in narrative output, with its replacement.
`--json` schemas may keep internal field names (`config_revision` is a fine wire name);
prose may not.

| Banned | Why | Say instead |
|---|---|---|
| envelope | Internal state-container struct | (nothing — name the fact: "revision: 4") |
| config_revision | Its field name | revision |
| marker, operation marker | Crash-recovery machinery | "an upgrade was interrupted — finishing it" |
| ownership transferred, `[A final]`, checkpoint captured | Ceremony internals / spec notation | (silent at summary; evidence at detail) |
| advisory / authoritative / DevIterate / `{verdict:?}` | Gate-regime internals, Debug formatting | (silent when proceeding; the refusal text when not) |
| candidate, married, re-married, dev-candidate | Resolution-pool and lifecycle jargon | provisioner / the current dev build |
| driving, forwarding, launching | Harness narration | (silent — the verb's report is the story) |
| reconcile (in report lines) | Engineering dialect | apply / the verb's own name |
| `(s)`, bare counts | Hedged plurals, numbers without nouns | computed plurals: "1 change", "6 changes" |

## Canonical transcripts

The lexicon applied — these are the reference renderings new output imitates. The
templates themselves are owned by the umbrella spec's
[output-templates.md](../../.kiro/specs/operator-explanation/output-templates.md),
whose reference transcripts are asserted against the renderer.

```markdown
# Infra Plan
**Plan for compose-explore** at revision 4

## Update
- the *tokeirad* service would be updated - `image`: `tokeirad:v3` → `tokeirad:v4` - `tokeirad::compose/tokeirad`

## Delete
- the *Aurora DSQL cluster* would be deleted - `dsql::dsql/monitored`

## Impacts
- data held by the *Aurora DSQL cluster* would be destroyed, irreversibly
- the *Aurora DSQL cluster* would no longer be available
- the *tokeirad* service would be unavailable while the change applies
- the *tokeirad* service would be replaced
```

```text
# apply, closing lines
[compose] infra apply: 1 change
  - compose/grafana
revision: 3 (definition sha256:43386b5879da…)

# revert
revert: restored the definition of revision 2
revert: 1 change
  + compose/grafana
revision: 4 (the definition of revision 2, applied again)

# upgrade (dev refresh)
upgrade: replacing the deployment's provisioner with the current dev build
infra apply: 2 changes
  ~ compose/grafana
  ~ compose/alloy
upgrade complete — the deployment runs the new provisioner
provisioner: `tkp` updated (sha256 dbe04f076ab5…)

# nothing to do
upgrade: no changes to apply — the deployment's provisioner is already current (sha256 dbe0…)

# a refusal (what/why/next, no roadmap)
Error: infra apply: refusing — the plan is destructive: 1 destructive change
  - grafana::compose/grafana  (compose_service)
re-run with `--yes` to proceed
```

## Naming new surface

Before a new verb, flag, or message ships, its words pass this checklist:

1. Does every noun in it appear in the lexicon? If a new concept genuinely needs a new
   word, the word joins the lexicon in the same change.
2. Could the operator point at what it names?
3. Is the internal name confined to `--json` and the code?
4. Is it the *same word* the other binary would use for the same concept?
5. Do its counts compute their plurals, and its digests truncate to 12 in narrative?

## Enforcement

Like the output contract: binding for new and reworked output; existing lines migrate
in the dedicated output pass tracked by the CLI-discovery ledger. A report that leaks a
banned word is a defect with the same weight as a failing lint.
