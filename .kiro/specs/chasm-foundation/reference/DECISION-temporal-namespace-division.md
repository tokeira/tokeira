# Decision note — TemporalNamespaceDivision is a thin field-resolution shim (task 25.2)

**For:** task 25.2 (Req 13.2). **Status:** decided with the user/Kiro.

## Decision

`TemporalNamespaceDivision` is a **virtual system search attribute that resolves to the
`archetype_id` column** — nothing more. tokeira does **not** model Temporal's
namespace-division semantics (the `IS NULL`-by-default hiding, the
`QueryWithAnyNamespaceDivision` mention-to-reveal dance). That whole mechanism is the
legacy mutable-state retrofit tokeira deliberately replaced with **endpoint-forced
archetype scope** (Req 13.1; design.md "why one index" / "Archetype scoping at the edge").

Concretely:

1. **Field resolution, not value semantics.** A visibility query referencing
   `TemporalNamespaceDivision` (or the reserved `archetype`) resolves to the
   `archetype_id` column instead of erroring as an "unknown search attribute" or being
   looked up in `sa_registry` as a generic string SA (Req 13.2). This is the entire
   mandate.

2. **The forced scope is authoritative; the shim's predicate is subordinate and AND-ed.**
   `ListWorkflowExecutions` forces `archetype = workflow`; `ListActivityExecutions` forces
   `archetype = activity` (Req 13.1, the `CompiledFilter.archetype` scope from 25.1). A
   query that also mentions `TemporalNamespaceDivision` compiles to an `archetype_id`
   predicate that sits *under* the forced scope — it can never widen it. If they
   contradict, the result is empty, never a scope escape.

3. **Layering.** The projection filter compiler stays **archetype-neutral**: it resolves
   the field name to the `archetype_id` *column* (a column reference is not
   archetype-value knowledge) and compares the value as the archetype id. The only
   archetype it names is the universal default — `ArchetypeId::WORKFLOW` (id 0,
   `LEGACY_WORKFLOW_ARCHETYPE_ID`) — so Temporal's empty/default division
   (`""`/`"default"`/`"workflow"`) maps to the workflow archetype. Mapping any *other*
   archetype name (e.g. `activity`) to its id needs the registry and therefore belongs at
   the edge; in practice `TemporalNamespaceDivision` is a workflow-visibility attribute and
   activity endpoints do not receive it, so the activity-name convenience mapping is a
   documented non-requirement (the activity endpoint's forced scope already selects it).

4. **No value-mapping table, no division registry.** tokeira has only `workflow` and
   `activity` archetypes, both already selected by their endpoints; there are no
   scheduler/batcher division workflows to map.

5. **`archetype` is reserved** (design) — a user search attribute cannot spoof it
   (Req 10.10).

## Rationale

Req 13.2's literal text is "compiles to `archetype_id`, AND SHALL NOT store or resolve it
as a generic string search attribute." That is field resolution. The division-scoping
*behaviour* Temporal layers on top is explicitly out of scope — tokeira's first-class
`archetype_id` column plus endpoint-forced scope is the greenfield replacement for it.
