# Operator output contract — `tkr` and `tkp`

Every operator-facing report from `tkr` and `tkp` follows one contract, so the two binaries
read as one product. This document is that contract; the shared vocabulary lives in
`crates/tokeira-report`, and both CLIs render through it. Its sibling,
[operator-language.md](operator-language.md), governs the *words* — the lexicon reports
draw from and the internal vocabulary they must never leak.

## The principle

**Every report is data first; prose is a rendering.** A verb produces a structured result
model, then renders it. Narrative text is never assembled ad hoc inside verb logic — a
report that cannot be emitted as JSON is a defect in the report, not a formatting choice.

This is the CLI-scale form of the house doctrine (history is authority, `AGENTS.md §3`):
the model is the truth; every presentation derives from it.

## Two axes, three forms

### Depth — how much of the model the narrative shows

| Level | Flag | Contract |
|---|---|---|
| **summary** | *(default)* | The outcome and anything demanding operator attention. One screen. States the answer. |
| **detail** | `--detail` | Summary **plus** the evidence — per-resource lines, field diffs, digests, provenance, paths. Substantiates the answer. |

### Form — who is reading

| Form | Flag | Contract |
|---|---|---|
| **narrative** | *(default)* | Deterministic Markdown under the depth contract above — skinned for a terminal (termimad) and emitted raw when stdout is not a TTY, so pipes, PR comments, and agents receive the Markdown itself. |
| **structured** | `--json` | The complete result model, verbatim. |

### The collapse rule

**`--json` ignores depth.** Structured output is always the full model: one stable schema,
consumers filter with `jq`, and a script never breaks because a human added `--detail`.
Depth is a human affordance only, so the 2×2 matrix has three real cells.

## Flag surface

- `--json` and `--detail` are **global flags on every verb** of both binaries.
- A verb with no additional evidence at detail depth renders identically — that is
  conformant, not an error.
- `tkr` forwards both flags verbatim when a verb is forwarded to a deployment's bound
  `tkp`, so the operator cannot tell (and need not care) which binary rendered the report.

## Narrative copy rules

1. Fact lines are `subject: state` — lowercase key, no trailing period. Markdown
   reports carry structure instead: a `#` title (title-case), the `**Plan for
   {deployment}**` assurance line, and `##` sections whose headings state the action
   once — lines beneath carry no glyphs, counts, or type annotations, and code
   identifiers appear only inside code spans and citation links (values embedding a
   backtick widen to double-backtick spans).
2. Tense carries meaning: past = done (`placed \`tkp\``), infinitive = planned
   (`1 to update`), present = standing fact (`storage: in-memory`).
3. Errors state *what happened, why, and what to do next* — the contract as it stands,
   never implementation status. No roadmap apologies, no "not yet".
4. The symbol vocabulary is fleet-wide for anything resembling a plan or delta:
   `+` create · `~` update · `±` replace (destructive) · `-` delete · `=` unchanged ·
   `?` uncertainty.
5. Numbers always carry their noun ("3 services", never a bare "3").
6. Counts never inflate: an unchanged resource is not a "change".
7. stdout carries the report; stderr carries advisories (warnings, dev-iteration
   notices). Under `--json`, stdout carries **only** JSON — advisories stay on stderr.
8. Pluralization is computed, never hedged: `1 change`, `6 changes`. `(s)` never
   appears in a report — the count is always known by the time it prints.

## Depth placement guide

What belongs where, by example:

| Content | Depth |
|---|---|
| Plan action sections (`## Update` + templated resource lines) | summary |
| Operational impacts (`## Impacts`, severity-first) | summary |
| The header's live-state coverage clause | summary |
| Applied resource lines after a mutation | summary |
| Binding verdict and what it means for this verb | summary |
| Field-level diffs (`` `image` ``: `sha256:9f3c…` → `sha256:41c2…`) | detail |
| Declared behaviour in its confidence voice, with citations | detail |
| The `## Unchanged` section | detail |
| Digests, manifest entries, retained revisions, state heads | detail |
| Filesystem paths and provenance chains | detail |

The test: summary answers *"what happened / what will happen, and must I act?"* — detail
answers *"show me why that is true."*

## Migration status

The contract is enforced for new and reworked output. Existing messages migrate
opportunistically; the dedicated sweep is tracked in the CLI-discovery ledger (output
polish pass). New verbs MUST NOT ship narrative-only reports.
