# Implementation Plan: Explanation Change Semantics

Ordered: Feature 1 amendment → vocabulary → declaration point → collection → declarations
→ derivation → rendering → coverage enforcement. Every correctness property is a required
property-based test task.

**Prerequisite:** `explanation-evidence-model` (Feature 1) complete through its Phase 5.

## Phase 1 — Apply the Feature 1 amendment

- [x] 1.1 Relocate the semantic vocabulary to `tokeira-iac`
  - Move `ChangeSemantics`, `Confidence<T>`, `LifecycleOperation`, `ReplacementPolicy`,
    `Disruption`, `DataEffect`, `Reversibility` from `tokeira-explain` into a new
    `crates/tokeira-iac/src/semantics.rs`
  - Re-export from `tokeira-explain` so its public surface is unchanged
  - Update `.kiro/specs/explanation-evidence-model/design.md` to record the corrected
    placement, so the two specs do not disagree
  - _Requirements: 1.1, 2.1_
  - DONE — satisfied at Feature 1 Phase 3 (PR #31, f0b13f8b): the vocabulary was born in
    `crates/tokeira-iac/src/semantics.rs` (never created in `tokeira-explain`, so nothing
    moved); re-exported from `tokeira-explain`; F1 design.md records the placement.

- [x] 1.2 Add `Citation` with a compile-time non-empty guarantee
  - `const fn new(&'static str)` asserting non-empty; document that declarations are
    `const` items precisely so the assertion evaluates at compile time
  - Extend `Confidence` so `EngineFact` carries a citation alongside `ProviderGuarantee` —
    a claim about Tokeira's own behaviour is as citation-worthy as one about a provider's
  - _Requirements: 2.2, 2.3, 2.6_
  - DONE — satisfied at Feature 1 Phase 3 (PR #31): `Citation::new` is `const fn` with a
    non-empty assert; `Citation` stores `Cow<'static, str>` (recorded deviation:
    `&'static str` alone cannot `Deserialize` for the JSON round-trip); `EngineFact` and
    `ProviderGuarantee` both carry citations structurally.

- [x] 1.3 Extend `PlanOutcome` with `semantics_by_id`
  - `BTreeMap<ResourceId, ChangeSemantics>` beside the refresh coverage, populated empty
    until Phase 3
  - _Requirements: 3.2_

- [x] 1.4 **Checkpoint** — workspace check, test, lint clean; explanation output unchanged
  (every field still `Unknown`, renderer still silent). DONE — Phases 1–3 slice:
  `semantics_by_id: BTreeMap<ResourceId, ChangeSemantics>` on `PlanOutcome`, carried
  unfiltered by module selection (same rationale as refresh coverage).

## Phase 2 — The declaration point

- [x] 2.1 Add `SemanticsContext` and `Resource::change_semantics`
  - Context struct carrying change kind, recorded state, and field differences, so
    Features 3–4 can extend inputs without breaking declaration sites
  - Default implementation returns `ChangeSemantics::default()`
  - Doc comment states the purity and totality contract, and that the default is the
    honest posture rather than a placeholder
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

- [x] 2.2 **PBT: Property 1 — the default declares nothing**
  - _Property 1; Requirements: 1.4, 2.4_

- [x] 2.3 **PBT: Property 2 — declaration is total**
  - Generated over every `ChangeKind`, empty/observation-only/populated diff sets, and
    present/absent recorded state; run against the default and (after Phase 4) every
    declaring kind
  - _Property 2; Requirements: 1.5, 1.6_

- [x] 2.4 **Checkpoint** — trait change compiles workspace-wide with no kind edits (the
  default keeps all 34 non-declaring kinds correct). DONE — `SemanticsContext` in
  `semantics.rs`; defaulted `change_semantics` on `Resource` (dyn-compatible, purity/
  totality contract in the doc); Properties 1+2 as one engine proptest over every kind,
  diff shape, and state presence; zero kind edits workspace-wide. `ChangeKind` gained
  `Copy` (fieldless enum; the context carries it by value).

## Phase 3 — Collection during change computation

- [x] 3.1 Collect declarations in `compute_changes`
  - One call per non-`NoChange` change; result keyed by resource id
  - No second composition pass and no additional provider call
  - _Requirements: 3.1, 3.2, 3.7_

- [x] 3.2 Reach removed resources through the recovery seam
  - For deletions, recover the kind from recorded state via `ResourceRecovery` and declare
  - Where no recoverer claims the type, leave the id absent from the map
  - _Requirements: 3.3, 3.4_

- [x] 3.3 Carry declarations into the explanation
  - `ExplainedChange.semantics` populated verbatim from `semantics_by_id`
  - Absent id on a deletion → `KindUnavailableForRemovedResource` uncertainty
  - _Requirements: 3.4, 3.5, 3.6_

- [x] 3.4 **PBT: Property 3 — transport is verbatim**
  - _Property 3; Requirements: 3.5, 3.6_

- [x] 3.5 **PBT: Property 4 — declarations cannot move the destructive set**
  - Fuzz declarations against a fixed change set; the destructive set must equal the
    engine's classification every time
  - _Property 4; Requirements: 4.1, 4.2_

- [x] 3.6 **Checkpoint** — with no kind declaring yet, plans are unchanged; the transport
  is proven inert before any declaration lands. DONE — collection inside
  `compute_changes` (one `ComputedDelta` pass: desired changes declare, `NoChange`
  never, deletions via `ResourceRecovery` with unclaimed types absent — unit-pinned);
  verbatim transport into `ExplainedChange.semantics` on the plan side and preceding-plan
  reuse on the apply side; `KindUnavailableForRemovedResource` states the unclaimed-
  deletion absence; Properties 3+4 green over declarations fuzzed at every confidence
  tier; full §10.4 bar green with every declaration still all-Unknown.

## Phase 4 — Tier 1 and Tier 2 declarations

- [x] 4.1 Declare `ComposeService`
  - Established from `reconcile_service` (stop → force-remove → create): operation
    `Replaced` even when the change kind is `Update`, replacement `DestroyBeforeCreate`,
    disruption `UnavailableDuringChange`, data effect `Preserved` (state rides
    bind-mounted volumes), reversibility `Reversible` — all `EngineFact`, citing
    `crates/tokeira-compose/src/lib.rs`
  - Verify the data-effect claim against the volume handling before asserting it
  - _Requirements: 2.6, 7.2_

- [x] 4.2 Declare `ObservabilityConfigFilesResource` and `LocalStateDirResource`
  - `EngineFact` from each resource's own write and delete paths
  - The delete data effect MUST be read from the delete implementation, not assumed
  - _Requirements: 2.6, 7.2_

- [x] 4.3 Declare `DsqlCluster`
  - `ProviderGuarantee` with an AWS documentation URL in each citation
  - Establish from AWS documentation: which field changes force replacement, what deletion
    does to stored data, and whether deletion protection changes reversibility
  - Where documentation does not establish a field, declare `Unknown` — do not infer
  - _Requirements: 2.2, 2.5, 7.2_

- [x] 4.4 Declare `DynamoDbTable`
  - `ProviderGuarantee` with citations; establish which attribute changes require
    replacement and the data effect of each
  - _Requirements: 2.2, 2.5, 7.2_

- [x] 4.5 Golden tests for each declaring kind
  - Six scenarios per kind: creation, in-place update, replacement, deletion, drift-driven
    update, unknown
  - Assert classification and confidence; never assert prose
  - Where a scenario does not apply, assert the kind reports it inapplicable rather than
    omitting the case
  - _Requirements: 8.1, 8.2, 8.3, 8.4_

- [x] 4.6 **Checkpoint** — `tkr infra plan --detail` against the reference compose
  definition states compose service semantics; the misleading in-place reading of `~` is
  gone. DONE — Phase 4 slice, all five kinds declared from ground truth:
  **ComposeService** (EngineFacts from `reconcile_service`: Update/Replace are effected as
  destroy-before-create, unavailable meanwhile, bind-mounted data preserved);
  **LocalStateDirResource** (the delete is a deliberate no-op → deletion declares data
  *preserved* — read from the delete impl, exactly as the task demands);
  **ObservabilityConfigFilesResource** (in-place writes; delete destroys the managed
  files yet stays reversible — the tree re-renders from the definition);
  **DsqlCluster** (mode-aware: preexisting = engine-fact restraint; managed delete's
  data fate and recoverability left **Unknown** — the AWS pages read
  (API_DeleteCluster + the user guide's delete-cluster section) establish the
  disable-protection-then-delete sequence, not data fate — the spec's do-not-infer rule
  applied); **DynamoDbTable** (delete cites AWS verbatim — "deletes a table and all of
  its items" (API_DeleteTable) — as ProviderGuarantee; recoverability and TTL-bearing
  updates' data effect stay Unknown; Replace asserted inapplicable — the diff cannot
  produce one). Golden tests per kind assert classification + confidence, never prose;
  the end-to-end half of this checkpoint is the exercise-test assertion that compose
  creates carry declared operations through the real engine — the `--detail` prose
  review completes at Phase 6's checkpoint (rendering does not exist yet; recorded
  deviation). *2026-07-29:* Phase 4's deliberate DSQL/DynamoDB Unknowns were closed by
  the task-6.2 research — the goldens now assert the cited answers.

## Phase 5 — Impacts and uncertainty activation

- [x] 5.1 Implement `derive_impacts`
  - Trigger table from the design, severity-ordered (`DataDestroyed` → `Unavailability` →
    `Replacement` → `BriefInterruption` → `RollingReplacement`)
  - Deterministic statements from templates; subjects ordered by evidence id
  - `Inference` contributes and is marked derived; `Unknown` contributes nothing
  - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.7_

- [x] 5.2 Activate `SemanticsUndeclared`
  - Per-change when the field is stated elsewhere in the plan; one plan-level uncertainty
    when the field is `Unknown` across every change; nothing where the field does not
    apply to the change kind
  - Resolution action names declaring semantics for the kind
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 5.3 Record absence of impacts honestly
  - All-`Unknown` plan emits no impacts and records the absence as uncertainty, never as
    an absence of consequence
  - _Requirements: 5.5_

- [x] 5.4 **PBT: Property 5 — impact derivation is a pure function of declarations**
  - _Property 5; Requirements: 5.6, 5.7_

- [x] 5.5 **PBT: Property 6 — every impact is grounded**
  - Both directions: every subject justifies its class, and no qualifying change is missing
  - _Property 6; Requirements: 5.2, 5.3, 5.4_

- [x] 5.6 **PBT: Property 7 — Unknown never becomes a claim**
  - _Property 7; Requirements: 5.5, 9.4_

- [x] 5.7 **PBT: Property 8 — uncertainty activation is exact**
  - If-and-only-if in both directions, plus the plan-level aggregation case
  - _Property 8; Requirements: 6.1, 6.2, 6.4_

- [x] 5.8 **Checkpoint** — `cargo test -p tokeira-explain` green; a DSQL storage plan
  reviewed by hand against the umbrella's canonical transcript. DONE — Phase 5 slice:
  `tokeira_explain::impacts::derive_impacts` (severity-first trigger table; deterministic
  templates; subjects by evidence id; deployment-anchored impact identity — recorded
  signature deviation; Inference contributes, renderer derives the marking from
  subjects); `SemanticsUndeclared` activated plan-side with the fixed applicability
  matrix (replacement→Update/Replace; data effect→Update/Replace/Delete; others→all
  non-NoChange) and field-qualified evidence identity so five plan-level gaps never
  collide; apply-side derives impacts from reused declarations but never gap-hunts
  (Req 6 is plan-scoped); all-Unknown plans emit no impacts and state the absence as
  plan-level uncertainty. Properties 5–8 green; F1's renderer Property 8 evolved
  (gaps allowed, claims banned — recorded). The storage-plan checkpoint test mirrors
  the real compose/DSQL declarations and pinned the narrative — and caught a live
  demon: `counted(2, "uncertainty")` rendered "uncertaintys"; the irregular plural now
  owns its call-site copy. Impacts render next phase; the model carries them now.

## Phase 6 — Rendering (rewritten 2026-07-29 to the Markdown target)

The narrative becomes deterministic Markdown, rendered for the terminal via `termimad`
(new dependency — architectural, approved in the 2026-07-29 exploration; raw Markdown is
emitted when stdout is not a TTY, which is the agent- and PR-native form). The document
form (sections, templates, display names, header) is specified by the evidence-model
spec's amended Requirement 6; this phase implements it together with the semantics
voices this spec owns.

- [x] 6.1 Vocabulary amendments
  - `Citation` → `Code(..) | Doc { title, url, quote }` with const constructors and
    non-empty guarantees; `Inference` gains a citation; `ChangeSemantics` gains the
    optional kind-authored mechanism `statement`
  - Migrate the five Tier 1/2 declarations onto the new shapes
  - _Requirements: 1 (amendment), 2.7, 2.8_

- [x] 6.2 Declaration upgrades from the 2026-07-29 research
  - DSQL managed delete: `reversibility = ProviderGuarantee(Irreversible)` (doc-cited),
    `data_effect = Inference(Destroyed)` (derivation-cited); managed create:
    `reversibility = Inference(ReversibleWithDataLoss)`
  - DynamoDB delete: `reversibility = ProviderGuarantee(Irreversible)` (doc-cited; the
    engine's create leaves PITR at its documented default)
  - Golden tests updated; Phase 4 DONE records annotated
  - _Requirements: 2.5 (amended), 7.2_

- [x] 6.3 Display names
  - Kind noun declared beside the kind (e.g. "service", "Aurora DSQL cluster"); carried
    through `Change` and the model as a new slot in the field policy (evidence-model
    amendment); instance name joins the rendering only when the plan holds more than one
    resource of the kind; `.tkd` author override ledgered for the source-spans feature
  - _Requirements: evidence-model Req 6 (amended)_
  - DONE — slice A (with 6.1/6.2): `Citation` → `Code | Doc {title, url, quote}` with
    const constructors; `Inference {value, citation}`; `ChangeSemantics.statement`
    (authored for the compose replace mechanism and the DSQL protection-disable
    sequence); all five declarations migrated; researched upgrades landed with their
    establishing quotes (DSQL delete PG(Irreversible)+Inference(Destroyed), create
    Inference(ReversibleWithDataLoss); DynamoDB delete PG(Irreversible) via the PITR
    page); goldens evolved to the researched answers; display channel as
    `display_by_id` on `PlanOutcome` (collected beside semantics, recovery path
    included — a deviation from the task's `Change`-field sketch, recorded: the map
    pattern breaks no literals and matches coverage/semantics) + `Resource::display_kind`
    (defaulted) + five nouns + the model's `display` slot, pinned end-to-end in the
    exercise test.

- [x] 6.4 The Markdown renderer
  - `# Infra Plan` + `**Plan for {platform}** with *live state* {state}` header; `##`
    action sections in would-mood templated prose; ids once (sections, never impacts);
    `## Impacts` severity-first, one line per subject, kind-specialized templates;
    `## Unchanged` at detail; field diffs as code spans; escaping rule for literal text
  - Confidence voices (engine fact plain / "AWS documents …" / "Tokeira derives …");
    kind-authored `statement` in place of the generic mechanism template when present
  - Citations at detail: `Doc` as links, `Code` as code spans
  - No narrative rendering of undeclared-semantics uncertainties (Req 6.5); live-state
    coverage speaks through the header and, when unconfirmed, per-change lines
  - termimad skin at the binary edge; raw Markdown when piped; `--json` untouched
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.7, 9.8; evidence-model Req 4.5/6 (amended)_

- [x] 6.5 Lexicon and contract migration
  - Add disruption, data effect, reversibility, replacement, confidence, citation to
    `docs/platforms/operator-language.md`; replace the canonical plan transcripts with
    the Markdown target; amend `operator-output-contract.md` (narrative form = rendered
    Markdown; header-case exception; escaping; symbol vocabulary scoped to compact
    delta lines pending the output pass)
  - _Requirements: 9.6_

- [x] 6.6 Rendering properties reworked
  - Feature 1's renderer properties re-anchored to the Markdown form (depth superset,
    depth-blind JSON, lexicon conformance over prose outside code spans/links); the
    claims-not-gaps form of slot silence retained
  - _Property 7; Requirements: 9.4, 6.5_

- [x] 6.7 **Checkpoint** — rendered summary and detail reviewed against the 2026-07-29
  target transcripts (the PR #40 body's rendering target); `tkr infra plan` against
  `compose-explore` reviewed live; a TTL-update plan reviewed for the `DataEffect`
  vocabulary gap and the resolution decided (extend the enum vs hold).
  DONE — slice B: the storage-plan checkpoint test pins the target (voices, citations,
  impacts, gaps machine-side); live against `compose-explore` the fresh `tkp` rendered
  the header assurance and the `## Unchanged` section with real nouns — the
  multiplicity rule visible in the wild (five *services* instance-named, singleton
  kinds kind-forward). **Resolved at the slice-B review (operator-directed):** `DataEffect`
  gains the general `Policy` value ("too specific" ruled out `ExpiresByPolicy`) — the
  TTL update declares `ProviderGuarantee(Policy)` with the statement carrying the
  specific meaning and `Inference(ReversibleWithDataLoss)`; the settings-vs-policy
  rule recorded for DynamoDB's wider update surface.

## Phase 7 — Coverage enforcement

- [x] 7.1 Maintain the kind registry
  - A test-visible registry of Tier 1 and Tier 2 kinds, mirroring the requirements'
    inventory
  - DONE (2026-07-29): `platforms/compose-syn/tests/semantics_registry.rs` — the
    operating platform realizes every kind through its own factory (the reference
    definition plus both DSQL storage variants, so the mode-aware DSQL declaration is
    probed in both constructions), and the accounting test holds registry and platform
    in exact agreement both ways (an unclassified realized kind and a stale row each
    fail with a message saying what to do)
  - _Requirements: 7.1, 7.3, 7.4_

- [x] 7.2 **PBT: Property 9 — tier coverage holds**
  - Every registered Tier 1/2 kind declares its applicable fields above `Unknown`
  - DONE (2026-07-29): generated probes (applicable change kind × diff lists mixing the
    kind's real field names with arbitrary ones — DynamoDB's TTL branch covered by
    construction) assert every semantic field above `Unknown`, and every inapplicable
    change kind answers the all-`Unknown` default. Teeth verified: misregistering
    DynamoDB `Replace` as applicable fails the property. Req 7.2 amended for
    consistency with 2.8 (cited inference is a legitimate tier; the bar is
    above-unknown)
  - _Property 9; Requirements: 7.2, 7.5_

- [x] 7.3 **PBT: Property 10 — every claim cites**
  - Every `ProviderGuarantee` and `EngineFact` in the registry carries a non-empty citation
  - DONE (2026-07-29): over the same probe space, every declared field (all cited
    tiers, per amended 2.8 — inference included) carries a citation; code citations
    non-empty; documentation citations carry a non-empty title and URL, and a non-empty
    establishing quote when one is given
  - _Property 10; Requirements: 2.2, 2.3, 2.6_

- [x] 7.4 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`.
  - DONE (2026-07-29): all five green. The feature is complete — Phases 1–7 landed.

## Addenda

- [ ] A.1 Engine-fact impacts (Requirement 5.5)
  - `derive_impacts` gains the engine-classification trigger family: every `Replace`
    emits unavailability-while-applying (lifted by a declared `CreateBeforeDestroy`
    above `Unknown`) and the replacement impact; every `Delete` emits
    no-longer-available — as engine facts, with no declaration required
  - Property 5 re-anchored (pure over declarations **and** kinds); Property 6's
    grounding extended to the engine-fact triggers; the output-templates reference
    transcripts are the acceptance fixtures
  - _Requirements: 5.1, 5.5, 5.7_

- [ ] A.2 Activate `ProviderAssignedAtApply` (umbrella 1.3.2)
  - Kinds declare their provider-assigned fields through the declaration vocabulary;
    creates carry one uncertainty per such field until apply supplies the value
  - _Requirements: umbrella 1.3.2_

## Task Dependency Graph

```text
[Feature 1 through its Phase 5]
        ↓
1.1 → 1.2 → 1.3 → 1.4 (checkpoint)
        ↓
1.4 → 2.1 → {2.2, 2.3} → 2.4 (checkpoint)
        ↓
2.4 → 3.1 → 3.2 → 3.3 → {3.4, 3.5} → 3.6 (checkpoint)
        ↓
3.6 → {4.1, 4.2, 4.3, 4.4} → 4.5 → 4.6 (checkpoint)
        ↓
4.6 → 5.1 → 5.2 → 5.3 → {5.4, 5.5, 5.6, 5.7} → 5.8 (checkpoint)
        ↓
5.8 → 6.1 → 6.2 → 6.3 → 6.4 (checkpoint)
        ↓
6.4 → 7.1 → {7.2, 7.3} → 7.4 (final)
```

Tasks 4.1–4.4 are independent of one another and may proceed in any order or in parallel;
each is gated only on the transport being proven inert at 3.6.

## Notes

- **Phase 3 lands before Phase 4 deliberately.** Transport is proven with nothing to
  transport, so when declarations arrive, any change in plan output is attributable to a
  declaration rather than to the wiring.
- **Task 4.1 will change what an operator believes.** Today `~ compose/grafana` reads as an
  in-place edit; after it, the same change reads as a destroy-before-create with the
  service unavailable for the duration. That is not a regression in the report — it is the
  report stopping being wrong.
- **Tasks 4.3 and 4.4 are research tasks first and coding tasks second.** The AWS
  documentation must be read and cited. A field the documentation does not establish stays
  `Unknown`; an implementer who finds themselves reasoning about what AWS "probably" does
  has found the exact failure mode this feature exists to prevent.
- Property 4 is the safety property of this feature: no declaration, however wrong, can
  move a change into or out of the destructive set that gates `apply --yes`.
