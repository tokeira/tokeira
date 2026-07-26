# Requirements Document: Explanation Agent Clients

## Introduction

This spec covers **Feature 6 (Agent Clients)** from the umbrella
[`operator-explanation`](../operator-explanation/requirements.md). It is the layer where
a language model may finally appear — and its entire architecture exists to keep that
appearance honest. Umbrella decision **D5** is the governing law: *agents select and
frame; they never originate.* An agent may choose which evidence to surface, order it,
and write prose around it. It may not introduce a resource, count, consequence, or risk
that the evidence index does not contain — and where it tries, the client suppresses the
statement and says so.

Two consumers are in scope, deliberately asymmetric:

1. **External agents over the analysis protocol** (Claude Code, Codex, Kiro, any MCP
   client): Tokeira cannot bind their behaviour and does not pretend to. Tokeira's
   obligations end at what Feature 5 built — a read-only, evidence-addressed protocol —
   plus registration documentation. Their prose is theirs, rendered in their surfaces.
2. **`tkr ask`** — Tokeira's own one-shot conversational entry, where Tokeira *does* own
   the rendering and therefore *can* enforce the contract: the model receives evidence
   and returns a structured answer plan citing `EvidenceId`s; the client validates every
   citation against the store; **facts are rendered deterministically from the evidence
   itself** (the model cannot re-word a fact); agent prose appears only in visibly marked
   interpretation blocks; uncited claims are suppressed, and the suppression is reported.

The second consumer is optional in the strongest sense (umbrella Requirement 6.2): no
provider configured means `tkr ask` refuses with pointers to the deterministic surface,
and nothing else in the product knows the difference.

### What This Spec Covers

- The **answer contract**: the structured response schema an ask provider must return,
  and its validation against the analysis store.
- **Fact-preserving rendering**: verified facts rendered from evidence by deterministic
  templates; interpretation visibly separated; suppressions reported.
- **`tkr ask`**: question → evidence gathering (via the Feature 5 library) → one
  provider call → validate → render. One shot, no conversation state.
- **The payload policy**: what leaves the machine, and the consent gate for anything
  beyond explanation facts.
- **Provider configuration**: minimal, operator-scoped, environment-based; one adapter
  seam with the concrete provider an implementation-time dependency decision.
- **MCP client documentation**: registering `tkr analysis serve` with external agents.

### What This Spec Does NOT Cover

(Umbrella exclusions, restated as this spec's hard edges.)

- A multi-provider gateway, routing, fallback chains, free-tier arbitrage, or cost
  tracking.
- Conversation persistence, agent memory, or multi-turn state — `tkr ask` is one shot.
- Streaming output.
- Any model dependency in `tkp`, `tokeira-iac`, `tokeira-explain`, or `tokeira-analysis`
  — the model call lives in `tkr` alone, behind the adapter seam.
- Local model management (an OpenAI-compatible local endpoint works through the same
  adapter; managing it does not).
- An MCP *client* implementation — Tokeira serves; the agent brings itself.

## Evidence From Current Code

| Fact | Anchor | Consequence |
|---|---|---|
| The analysis library answers evidence queries byte-faithfully, with typed not-found | Feature 5 (`tokeira-analysis`, Properties 3–4) | Citation validation is a store lookup; a fact's rendering source exists and cannot drift |
| Every explanation fact carries a stable, natural-key `EvidenceId` | Feature 1 (Requirement 3) | The citation currency exists |
| The explanation artifact is secret-free; the definition source is not | Feature 1 (R7.4); Feature 5 (R6.4) | The payload policy's default/consent split has a principled boundary |
| `reqwest` is workspace-pinned rustls-pure | root `Cargo.toml` | The HTTP adapter adds no TLS baggage; adding the dependency edge to `tkr` is still a flagged dependency change |
| The output contract and lexicon govern all operator-facing rendering | `docs/platforms/operator-output-contract.md`, `operator-language.md` | Ask output is a report like any other: depth rules, lexicon, computed plurals |
| `tkr` refusals state contracts, never roadmaps | house rule (operator-language.md) | The no-provider refusal is a statement of what `ask` requires, not an apology |

## Target State

```text
❯ tkr ask "why does tokeirad restart?"
verified
  ~ compose/tokeirad — image: sha256:9f3c… → sha256:41c2…
  cause: `tokeirad.image` changed at definition.tkd:66
  impact: service unavailable during replacement

interpretation (agent)
  The restart is a consequence of the image edit: compose replaces the
  container rather than updating it in place, so the new digest means a new
  container.

1 statement suppressed: cited no evidence
```

Every line under `verified` was rendered by Tokeira from the store. Every line under
`interpretation (agent)` is prose, labeled as such. The suppression line is the contract
enforcing itself in public.

## Glossary

Terms additional to the umbrella and sibling glossaries:

- **Answer Plan** — the structured response an ask provider returns: sections, each
  carrying cited `EvidenceId`s and optional commentary.
- **Verified Block** — facts rendered deterministically from the cited evidence by
  Tokeira's templates. The model selects them; it does not word them.
- **Interpretation Block** — agent-authored prose, always visibly labeled.
- **Suppression** — the removal of a section or statement that failed validation, always
  reported with its reason.
- **Identifier Guard** — the check that identifier-shaped tokens in commentary are
  covered by that section's citations.
- **Reserved Headings** — report sections that only deterministic content may occupy:
  impacts, destructive actions, uncertainties.
- **Payload** — everything serialized into the provider request.
- **Provider Adapter** — the seam through which exactly one configured provider is
  called.

## Requirements

### Requirement 1: The answer contract

**User Story:** As the ask client, I want the provider's response in a structure I can
validate, so that honesty is checked, not hoped for.

#### Acceptance Criteria

1. THE provider request SHALL instruct the provider to return an answer plan: an ordered
   set of sections, each with a title, a list of cited `EvidenceId`s, and optional
   commentary.
2. WHEN the response parses as an answer plan THE client SHALL validate every cited
   `EvidenceId` against the analysis store.
3. WHERE a section's citations all resolve THE section SHALL render as its verified block
   (the cited evidence, deterministically rendered) followed by its interpretation block
   when commentary is present.
4. WHERE any citation in a section does not resolve THE client SHALL suppress that
   section and report the suppression with the unresolvable id.
5. IF the response does not parse as an answer plan THEN THE client SHALL issue at most
   one corrective retry carrying the validation errors, and IF the retry also fails THEN
   THE client SHALL report the failure and point to the deterministic surface — it SHALL
   NOT render unvalidated prose as a degraded answer.
6. THE client SHALL render the suppression count whenever it is non-zero.

### Requirement 2: Facts are rendered, never re-worded

**User Story:** As an operator, I want every statement under "verified" to be Tokeira's
own rendering of stored evidence, so that the model cannot subtly restate a fact into a
falsehood.

#### Acceptance Criteria

1. THE verified block SHALL be rendered from the store's evidence values through
   deterministic templates — the same rendering vocabulary the reports use.
2. THE provider's text SHALL NOT appear inside a verified block.
3. WHEN the same evidence is cited by two answers THE verified rendering SHALL be
   identical in both.
4. THE interpretation block SHALL be visibly labeled as agent-authored in every rendering
   and SHALL be typographically separated from verified content.
5. THE client SHALL NOT merge, interleave, or reflow verified and interpretation content
   into a single block.

### Requirement 3: The identifier guard and reserved headings

**User Story:** As an operator, I want prose that names things to be covered by evidence,
so that a fluent hallucination cannot ride in under a valid section.

#### Acceptance Criteria

1. WHEN commentary contains identifier-shaped tokens (resource ids, module::resource
   forms, revision numbers, evidence ids) THE client SHALL verify each is covered by the
   section's resolved citations, and WHERE one is not THE client SHALL suppress that
   section and report the uncovered identifier.
2. THE reserved headings — impacts, destructive actions, uncertainties — SHALL be
   populated only from the deterministic explanation; an answer plan section claiming a
   reserved heading SHALL be suppressed and reported.
3. THE client SHALL NOT present any agent-originated assessment of risk, safety, or
   reversibility as a Tokeira determination; such statements appear only inside labeled
   interpretation blocks.

### Requirement 4: `tkr ask` — one shot, evidence in, answer out

**User Story:** As an operator, I want to ask one question about my deployment and get an
evidence-grounded answer, so that interrogation costs one command.

#### Acceptance Criteria

1. THE `tkr ask <question>` command SHALL gather evidence from the selected deployment's
   analysis bundles via the analysis library, call the configured provider once, validate
   per Requirements 1–3, and render.
2. THE command SHALL hold no conversation state: each invocation is complete in itself.
3. WHEN no bundles exist for the deployment THE command SHALL refuse with the producing
   verbs named, before any provider call.
4. THE command SHALL respect the output contract: narrative under the depth rules;
   `--json` emitting the validated answer (verified evidence ids, interpretation text,
   suppressions) as a typed value.
5. THE command SHALL NOT invoke `tkp`, acquire the deployment lock, or read anything
   beyond the analysis bundles and its own configuration.

### Requirement 5: The payload policy

**User Story:** As an operator, I want to know exactly what leaves my machine when I ask,
so that using a provider is a decision, not a leak.

#### Acceptance Criteria

1. THE payload SHALL contain only: the question, the evidence index entries and evidence
   values from the explanation artifact, and the answer-contract instructions.
2. THE payload SHALL NOT contain the definition source, definition excerpts, or any file
   content beyond the explanation artifact's values, unless the operator passes the
   explicit per-invocation consent flag.
3. WHERE the consent flag is passed THE command SHALL state in its output that definition
   content was included in the payload.
4. THE payload SHALL be inspectable: a flag SHALL print the exact payload without calling
   the provider.
5. THE documentation SHALL state plainly that the payload is transmitted to the
   operator's configured provider and leaves the machine.

### Requirement 6: Provider configuration and the adapter seam

**User Story:** As an operator, I want to point ask at my provider with my key, so that
Tokeira rides my existing account rather than becoming a credential manager.

#### Acceptance Criteria

1. THE provider configuration SHALL be operator-scoped (environment variables), never
   deployment-scoped: it SHALL NOT appear in `tokeirad.toml`, the definition, or any
   deployment file.
2. THE configuration SHALL name an endpoint, a model, and a credential; absence of any of
   them means no provider is configured.
3. THE provider SHALL be called through one adapter seam; the concrete first adapter is
   an implementation-time dependency decision under the house rules, and the seam SHALL
   confine it to one module.
4. THE credential SHALL NOT be logged, echoed, rendered, or serialized into any output,
   including `--json` and the payload-inspection flag.
5. THE adapter SHALL enforce a request timeout, and a provider failure SHALL report as a
   typed error naming the endpoint — never as a hang.

### Requirement 7: Absence is not degradation

**User Story:** As an operator without a provider, I want the entire product unchanged,
so that AI remains a lens, never a dependency.

#### Acceptance Criteria

1. WHEN no provider is configured THE `tkr ask` command SHALL refuse by stating what it
   requires and naming the deterministic surface (`tkr infra plan --detail`,
   `tkr analysis query`), in contract voice.
2. THE presence or absence of provider configuration SHALL have no observable effect on
   any other command: plans, applies, analysis serving, and their outputs SHALL be
   byte-identical either way.
3. WHERE the provider fails mid-invocation THE failure SHALL affect only that
   invocation's answer; the deterministic surface remains fully available.
4. THE provisioning path (`tkp`, `tokeira-iac`, `tokeira-explain`, `tokeira-analysis`)
   SHALL carry no model-client dependency; the adapter lives in `tkr` alone.

### Requirement 8: External agents over the protocol

**User Story:** As an operator using Claude Code or a similar agent, I want to wire it to
my deployment's analysis server in minutes, so that my existing subscription becomes the
conversational layer.

#### Acceptance Criteria

1. THE documentation SHALL provide registration instructions for `tkr analysis serve` as
   an MCP server for at least the MCP-capable agents named in the umbrella (Claude Code,
   Codex, Kiro).
2. THE documentation SHALL state what such agents can and cannot do through the protocol
   (read-only, evidence-addressed, no mutation) and that their prose is theirs — the
   fact/interpretation separation of `tkr ask` applies where Tokeira renders, and
   external surfaces render under their own rules.
3. THE analysis server's tool descriptions (Feature 5) SHALL be reviewed under this spec
   for agent-facing clarity: each description states what evidence the tool returns and
   its id shape, so agents cite correctly by construction.

### Requirement 9: Lexicon

**User Story:** As an operator, I want ask's vocabulary defined with all the rest, so the
last feature speaks like the first.

#### Acceptance Criteria

1. WHERE this feature introduces operator-facing vocabulary (verified, interpretation,
   suppressed, payload) THE change SHALL add those terms to `operator-language.md` in the
   same change.
2. THE labels `verified` and `interpretation (agent)` SHALL be lexicon-fixed strings, not
   per-rendering improvisations.

## Notes

- **The contract's teeth are structural, in order**: facts render from the store (the
  model never words them); citations must resolve (unresolvable → suppressed, reported);
  the identifier guard catches fluent name-dropping; reserved headings keep deterministic
  sections deterministic. What survives all four gates and is still wrong can only be
  wrong *interpretation of true facts* — visibly labeled as exactly that.
- The identifier guard (3.1) is a lexical check, and the spec is honest about its class:
  it cannot judge semantics, only coverage. It exists to make the cheapest hallucination
  (naming things) expensive, not to certify prose.
- Requirement 5.4 (print the payload) is the trust feature that costs an afternoon and
  answers every "what does it send?" conversation forever.
- One-shot-ness (4.2) is not a limitation to apologize for: it is what keeps Tokeira out
  of the conversation-state business the umbrella excluded, and what external agents (who
  own real conversation state) are for.
