# Requirements Document: Explanation Source Spans

## Introduction

This spec covers **Feature 4 (Source Attribution)** from the umbrella
[`operator-explanation`](../operator-explanation/requirements.md). It sharpens Feature 3's
revision-level answer — "the definition changed between revision 4 and the working
definition" — into the answer an operator can act on without searching:

```text
cause: `observability.grafana.image` changed at definition.tkd:75
```

### The mechanism decision, stated up front

The obvious implementation is value taint: thread a source span through the interpreter
so every evaluated value remembers where it was written. This spec **rejects that
mechanism** for a syntactic one, on four grounds:

1. **The operator's question is "which edit did this?", and edits are syntax.** An
   AST-level diff of the baseline definition against the working definition finds every
   edit exactly, with its span, by construction — no dataflow required to locate what
   changed.
2. **Taint would reshape `Value<H>`**, the type every evaluator arm and every platform
   bridge pattern-matches. That blast radius buys precision for a question (where did
   this *unchanged* value come from?) that no requirement in this umbrella asks.
3. **Non-interference becomes structural.** The umbrella demands attribution never alter
   realization. A separate syntactic pass over two parsed files cannot alter evaluation;
   instrumentation inside evaluation must *prove* it doesn't, forever.
4. **The interpreter's own architecture cooperates**: `config()` is host-free and
   independently evaluable (`interpret` returns the config value; `retarget_check`
   already compares config values across sources — the prior art for cross-revision
   config comparison is in the tree).

The syntactic mechanism: diff the two definitions' ASTs (formatting-insensitive, spans
from the working file); resolve each edit's **enclosing construct** (a service stanza, a
resource stanza, a config value path); attribute stanza-contained edits to their resource
directly (syntactic containment — engine fact), and config-value edits to changed
manifest fields by host-free value comparison (derived). What cannot be attributed falls
back to Feature 3's revision-level phrasing with the reduced precision recorded as an
uncertainty.

### Amendment to the umbrella

Umbrella Requirements 4.1.1–4.1.2 prescribe the taint mechanism ("the interpreter SHALL
retain the source location…", "…retain the association between that field and its source
location"). Those two criteria are **replaced** by outcome-focused criteria — the
operator-visible obligations 4.1.3–4.1.5 stand unchanged, and this spec's Requirements 1–4
carry the outcome. The umbrella's Feature 4 summary sentence ("spans must be threaded, not
reconstructed") is superseded by this Introduction. Umbrella Requirement 4.2 (attribution
never changes evaluation) stands and is strengthened: under this design it holds by
construction and is still asserted by property.

### Amendment to Feature 1's design

`SourceLocation` gains an `AttributionBasis` field (`Stanza` | `ValueFlow`), serde-default
so the addition is non-breaking, letting the renderer mark value-flow attributions as
derived while stanza attributions state plainly. Recorded here per the established
sibling-amendment pattern.

### What This Spec Covers

- AST-level edit detection between the baseline and working definitions, in
  `tokeira-tkd` (which owns `syn` parsing).
- Enclosing-construct resolution for every edit.
- Attribution of edits to `DefinitionEdit`-caused changes: stanza containment directly;
  config edits via host-free config evaluation and exact value matching.
- Population of the `source` slot on Feature 3's `DefinitionEdit` causes.
- Edits with no effect on the plan, surfaced at detail depth (the mistyped-but-valid
  config edit an operator most needs shown).
- Rendering, lexicon, and the fallback uncertainty.

### What This Spec Does NOT Cover

- Value taint through the interpreter — rejected above; if a future feature needs
  provenance for *unchanged* values, that is a new spec with its own justification.
- Attribution for drift, cascade, output-traced, or engine-advance causes — those causes
  are not definition edits; their roots are resources, not lines.
- Definition formatting, linting, or edit suggestions.
- Cross-revision archaeology beyond baseline-vs-working (Feature 5's comparison reuses
  the same diff machinery over any two retained revisions).

## Evidence From Current Code

| Fact | Anchor | Consequence |
|---|---|---|
| `syn` parses definitions with `full` + `extra-traits`; `proc-macro2` has `span-locations` | `crates/tokeira-tkd/Cargo.toml` | AST nodes are comparable and every token has a line/column on stable |
| `config()` is host-free and independently evaluated (`interpret` returns it; host contamination is rejected) | `crates/tokeira-tkd/src/lib.rs` | Config values from two revisions are comparable values, not texts |
| `retarget_check(src, old, new)` already compares config values across sources | `crates/tokeira-tkd/src/lib.rs` | Cross-revision config comparison has prior art in the exact crate this spec extends |
| The definition's structure is builder calls with string-literal names (`d.service(&module, "grafana", …)`, `d.resource(&module, "cluster", …)`) | `platforms/compose-syn/definition.tkd` (deployed and template) | An edit's enclosing stanza names its resource syntactically |
| Feature 3 realizes baseline and working definitions into canonical desired snapshots and classifies `DefinitionEdit` | `explanation-causality` | The set of changes needing attribution, and the changed manifest fields to match config values against, both exist |
| Feature 3's `BaselineSnapshot` types the missing/broken-baseline cases | `explanation-causality` design | Attribution inherits the same honest fallbacks without new machinery |
| `Cause::DefinitionEdit { source: Option<SourceLocation> }` is a Feature 1 slot, `None` until this feature | Feature 1 design | This spec populates a slot; the schema does not reshape |

## Target State

Every `DefinitionEdit`-caused change carries the line and column of the edit that
explains it, in the working definition's coordinates, with the attribution basis visible.
Edits that changed nothing are named at detail depth. Nothing about evaluation,
realization, or planning behaves differently with attribution present.

## Glossary

Terms additional to the umbrella and sibling glossaries:

- **Edit** — a contiguous AST-level difference between the baseline definition and the
  working definition, located by its span in the working file.
- **Enclosing Construct** — the smallest named definition structure containing an edit: a
  config value path, a service stanza, a resource stanza, a writeback, a type definition,
  or the module level.
- **Stanza** — one `d.service(…)` or `d.resource(…)` call: the definition's unit of
  resource authorship.
- **Attribution** — the association of an edit with a `DefinitionEdit`-caused change.
- **Attribution Basis** — how the association was established: `Stanza` (syntactic
  containment) or `ValueFlow` (config value matching; derived).
- **No-Effect Edit** — an edit that attributes to no change in the plan.

## Requirements

### Requirement 1: Edit detection is syntactic, exact, and formatting-insensitive

**User Story:** As the attribution layer, I want the precise set of edits between two
definitions, so that attribution starts from ground truth rather than heuristics.

#### Acceptance Criteria

1. WHEN the baseline and working definitions both parse THE attribution pass SHALL compute
   the set of AST-level edits between them, each carrying a span in the working
   definition's coordinates.
2. WHERE two definitions differ only in formatting, whitespace, or comments THE edit set
   SHALL be empty.
3. WHEN a definition item is added or removed THE edit SHALL span that item; WHEN an
   expression within an item differs THE edit SHALL span the smallest differing
   expression.
4. THE edit detection SHALL NOT evaluate the definition: it operates on parsed syntax
   only.
5. IF either definition does not parse THEN THE attribution pass SHALL yield no edits and
   the explanation SHALL fall back to revision-level attribution with the located parse
   verdict as the uncertainty's reason.
6. THE edit set SHALL be deterministic: identical definition pairs SHALL yield identical
   edits in identical order.

### Requirement 2: Every edit resolves to its enclosing construct

**User Story:** As the attribution layer, I want each edit named by the structure that
contains it, so that "what did the operator touch" is answered before "what did it
affect".

#### Acceptance Criteria

1. THE attribution pass SHALL resolve each edit to exactly one enclosing construct.
2. WHERE an edit lies within a `config()` value THE construct SHALL be the config value
   path (e.g. `observability.grafana.image`).
3. WHERE an edit lies within a service or resource stanza THE construct SHALL name the
   module and resource, taken from the stanza's own arguments.
4. WHERE an edit lies elsewhere (type definitions, writebacks, module declarations, the
   function level) THE construct SHALL name that location class.
5. THE resolution SHALL be purely syntactic and SHALL carry engine-fact confidence.

### Requirement 3: Attribution to changes

**User Story:** As an operator, I want each definition-caused change to point at the edit
that explains it, so that the path from plan line to definition line is one step.

#### Acceptance Criteria

1. WHEN an edit's enclosing construct is a stanza naming resource R AND R carries a
   `DefinitionEdit` cause in the plan THE explanation SHALL attribute R's change to that
   edit's span with basis `Stanza`.
2. WHEN an edit's enclosing construct is a config value path THE attribution pass SHALL
   evaluate both revisions' `config()` host-free, and WHERE the changed config value
   matches a changed manifest field of a `DefinitionEdit`-caused change exactly THE
   explanation SHALL attribute that change to the edit's span with basis `ValueFlow`.
3. THE `ValueFlow` basis SHALL be applied only when the match is unambiguous: the changed
   config value matches changed fields of the candidate change and of no other
   unattributed change.
4. WHERE multiple edits attribute to one change THE explanation SHALL carry all of them,
   stanza-based first.
5. WHERE a `DefinitionEdit`-caused change receives no attribution THE explanation SHALL
   retain revision-level phrasing and SHALL record the reduced precision as an
   uncertainty naming the change.
6. THE attribution SHALL NOT alter the cause classification: attribution decorates
   Feature 3's verdicts and SHALL NOT reclassify, add, or remove causes.

### Requirement 4: Attribution never changes evaluation

**User Story:** As a maintainer, I want attribution to be observationally invisible to
provisioning, so that explanation cannot alter what a deployment realizes.

#### Acceptance Criteria

1. THE attribution pass SHALL NOT evaluate `deployment()`: its only evaluation is the
   host-free `config()` of each revision.
2. THE realized deployment SHALL be identical whether or not the attribution pass runs.
3. THE attribution pass SHALL read only the two definition sources already retained; it
   SHALL NOT contact providers, read live state, or write state.
4. IF the attribution pass fails for any reason THEN THE verb SHALL complete with
   revision-level attribution and an uncertainty, and SHALL NOT fail on attribution's
   account.

### Requirement 5: No-effect edits are surfaced

**User Story:** As an operator, I want to see edits that changed nothing, so that a
mistyped-but-valid value does not silently do nothing while I believe it took effect.

#### Acceptance Criteria

1. WHEN the edit set is non-empty AND an edit attributes to no change in the plan THE
   detail rendering SHALL list it under a no-effect section, naming its construct and
   span.
2. WHERE every edit attributes THE no-effect section SHALL be omitted.
3. THE no-effect determination SHALL be made against the full plan, including changes the
   summary does not list.
4. THE no-effect section SHALL state what it means plainly: the edit produced no change in
   this plan.

### Requirement 6: Rendering and lexicon

**User Story:** As an operator, I want attribution in the report's established voice, so
that the sharper answer reads like the rest of the product.

#### Acceptance Criteria

1. WHEN a `DefinitionEdit` cause carries an attribution THE summary SHALL phrase it as the
   construct and location ("`observability.grafana.image` changed at definition.tkd:75"),
   replacing the revision-level phrasing for that group.
2. WHERE the basis is `ValueFlow` THE renderer SHALL mark the attribution as derived.
3. WHERE no attribution exists THE renderer SHALL keep Feature 3's revision-level
   phrasing.
4. THE location SHALL be rendered as the definition's basename with line (and column at
   detail depth), in the working definition's coordinates.
5. WHERE this feature introduces operator-facing vocabulary (edit, no-effect edit) THE
   change SHALL add those terms to `operator-language.md` in the same change.
6. THE `--json` rendering SHALL carry every attribution with its basis and every no-effect
   edit regardless of depth.

## Notes

- **The mechanism decision is the spec.** Rejecting taint keeps `Value<H>` untouched,
  keeps every platform bridge untouched, makes Requirement 4 structural, and answers the
  question operators actually ask. The cost is honest: a config value consumed only
  through interpolation (`format!("{}-dsql-rate-limiter", cx.project_name)`) will not
  value-match and falls back to revision-level phrasing with an uncertainty — visible,
  bounded, and recorded, versus an invasive mechanism whose precision no requirement
  needs.
- Requirement 3.6 is the discipline boundary: Feature 3 owns *why*, this feature owns
  *where*. A span can never promote, demote, or invent a cause.
- Requirement 5 exists because the most expensive definition edit is the one that does
  nothing: the operator believes it took effect. Surfacing it turns a silent misfire into
  a one-line notice.
