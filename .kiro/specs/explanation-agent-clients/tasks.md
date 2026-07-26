# Implementation Plan: Explanation Agent Clients

Ordered: the pure contract first (validation proven against adversarial plans before any
provider exists) → payload policy → the adapter and command → rendering → docs. Every
correctness property is a required property-based test task. No task in this plan calls
a real provider.

**Prerequisite:** Feature 5 complete (the store is the evidence source and citation
oracle).

## Phase 1 — The answer contract, pure

- [ ] 1.1 `tokeira-analysis::ask`: `AnswerPlan`, `ValidatedAnswer`, `Suppression`
  - The plan carries selection and prose only — no field in which a fact can be asserted;
    say so in the type's WHY comment
  - _Requirements: 1.1_

- [ ] 1.2 `validate_answer`
  - Per-section order: citations resolve → reserved-heading check → identifier guard →
    assemble; suppression is section-granular with typed reasons
  - _Requirements: 1.2, 1.4, 3.1, 3.2_

- [ ] 1.3 Verified rendering from the store
  - Cited evidence rendered through the deterministic fact templates the reports use;
    no code path accepts provider text into a verified block
  - _Requirements: 2.1, 2.2, 2.3_

- [ ] 1.4 **PBT: Property 1 — validation is exhaustive and section-granular**
  - _Property 1; Requirements: 1.2, 1.4, 1.6_

- [ ] 1.5 **PBT: Property 2 — verified content is store-rendered, provider-independent**
  - Plan pairs sharing citations with adversarially different commentary
  - _Property 2; Requirements: 2.1, 2.2, 2.3_

- [ ] 1.6 **PBT: Property 4 — the identifier guard covers or suppresses**
  - Near-miss tokens: valid shapes citing absent ids
  - _Property 4; Requirements: 3.1_

- [ ] 1.7 **PBT: Property 5 — reserved headings are impenetrable**
  - Including lexicon synonyms of the reserved headings
  - _Property 5; Requirements: 3.2, 3.3_

- [ ] 1.8 **PBT: Property 8 — validation is deterministic and pure**
  - _Property 8; Requirements: 1.2, 7.4_

- [ ] 1.9 **Checkpoint** — the contract stands alone: `cargo test -p tokeira-analysis`
  green with no provider, no network, no key anywhere in the tree.

## Phase 2 — Payload policy

- [ ] 2.1 `AskPayload` and `build_payload`
  - Question + explanation-derived evidence + contract instructions; definition content
    only under the consent flag; the credential is not a payload field (transmission
    concern, adapter-applied)
  - _Requirements: 5.1, 5.2_

- [ ] 2.2 `--show-payload`
  - Serialize exactly the `AskPayload`, print, exit — no provider call
  - _Requirements: 5.4_

- [ ] 2.3 **PBT: Property 6 — the payload respects the policy**
  - Sentinel-string fixture: definition content absent without consent, present with
    consent plus the output statement; credential absent from every serialization
  - _Property 6; Requirements: 5.1, 5.2, 5.3, 6.4_

- [ ] 2.4 **Checkpoint** — payload inspectable and policy-proven before anything can
  transmit it.

## Phase 3 — The adapter and the command

- [ ] 3.1 `ProviderConfig`, `Secret`, and the `AskProvider` seam in `tkr`
  - Environment-only configuration (`TOKEIRA_ASK_ENDPOINT` / `_MODEL` / `_KEY`); missing
    any → unconfigured; `Secret` redacts in `Debug`/`Display`/`Serialize`
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [ ] 3.2 The first concrete adapter
  - One HTTP JSON endpoint, request timeout, typed failure naming the endpoint
  - **Dependency gate**: the HTTP-client edge for `tkr` and the concrete provider shape
    are proposed for approval at this task under the house dependency rules; the choice
    is confined to the adapter module
  - _Requirements: 6.3, 6.5_

- [ ] 3.3 `tkr ask <question>`
  - Gather (store) → refuse-before-call cases (no bundles; no provider) → payload →
    one call → validate → render; no lock, no `tkp`, no state beyond bundles and config
  - Corrective retry: at most one, errors appended; then typed failure with
    deterministic pointers — never prose-as-fallback
  - _Requirements: 1.5, 4.1, 4.2, 4.3, 4.5, 7.1_

- [ ] 3.4 **PBT: Property 7 — absence is invisible elsewhere**
  - Fixture deployment's plan/apply/analysis outputs byte-diffed with and without
    `TOKEIRA_ASK_*` present
  - _Property 7; Requirements: 7.2, 7.4_

- [ ] 3.5 **Checkpoint** — mock-provider round trip green: malformed-then-valid retry
  path, timeout path, all-suppressed path.

## Phase 4 — Rendering and lexicon

- [ ] 4.1 Render `ValidatedAnswer` through `tokeira-report`
  - Summary: verified blocks, labeled interpretation blocks, suppression count; detail:
    evidence ids per fact, reasons per suppression; `--json`: the value verbatim
  - The two labels as lexicon constants: `verified`, `interpretation (agent)`
  - _Requirements: 2.4, 2.5, 4.4, 9.2_

- [ ] 4.2 **PBT: Property 3 — separation is total**
  - Every rendered line in exactly one of: verified block, labeled interpretation,
    suppression report
  - _Property 3; Requirements: 2.4, 2.5_

- [ ] 4.3 Lexicon additions
  - verified, interpretation, suppressed, payload → `operator-language.md`; Feature 1's
    lexicon-conformance property re-run over ask's rendering
  - _Requirements: 9.1_

- [ ] 4.4 Golden transcript
  - The target-state transcript as a fixture: shape, labels, suppression line; plus the
    no-provider refusal wording checked against the lexicon (contract voice, no roadmap)
  - _Requirements: 7.1_

- [ ] 4.5 **Checkpoint** — rendering green; transcript reviewed against the canonical
  form.

## Phase 5 — External agents and closure

- [ ] 5.1 `docs/platforms/analysis-agents.md`
  - Registration for Claude Code, Codex, Kiro against `tkr analysis serve`; what the
    protocol exposes and refuses; external prose renders under external rules; the
    transmission statement (Requirement 5.5) for ask
  - _Requirements: 5.5, 8.1, 8.2_

- [ ] 5.2 Feature 5 tool-description review
  - Each analysis tool description names its evidence shape and id form, so agents cite
    correctly by construction
  - _Requirements: 8.3_

- [ ] 5.3 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`. The provisioning-path crates' dependency graphs carry no model
  client (Requirement 7.4), asserted in CI-visible tests, not by inspection.

## Task Dependency Graph

```text
[Feature 5 complete]
        ↓
1.1 → 1.2 → 1.3 → {1.4 … 1.8} → 1.9 (checkpoint)
        ↓
1.9 → 2.1 → 2.2 → 2.3 → 2.4 (checkpoint)
        ↓
2.4 → 3.1 → 3.2 → 3.3 → 3.4 → 3.5 (checkpoint)
        ↓
3.5 → 4.1 → {4.2, 4.3} → 4.4 → 4.5 (checkpoint)
        ↓
4.5 → 5.1 → 5.2 → 5.3 (final)
```

## Notes

- **Phase 1 before any provider exists is the point.** The contract's entire value is
  that it holds against an adversarial responder; proving it against generated
  adversarial plans first means the provider integration inherits a sealed contract
  rather than shaping it.
- **The dependency gate at 3.2** is where the house dependency rules bite: the HTTP edge
  and the concrete provider are approved there, in implementation, with the session's
  reviewer — not smuggled in by a spec.
- The all-suppressed answer rendering (an honest suppression report plus a pointer) is a
  fixture, not an edge case: it is what a completely hallucinating provider looks like
  through this contract, and it should look *boring*.
