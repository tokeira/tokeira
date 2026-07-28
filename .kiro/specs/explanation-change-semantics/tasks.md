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
  deviation).

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

## Phase 6 — Rendering

- [ ] 6.1 Render impacts at summary depth
  - Operational impacts stated; per-change semantics not enumerated
  - _Requirements: 9.1_

- [ ] 6.2 Render semantics at detail depth
  - Per-change fields with confidence; `Inference` marked as derived; `Unknown` omitted
  - `--json` carries every declared field with confidence and citation at any depth
  - _Requirements: 9.2, 9.3, 9.4, 9.5_

- [ ] 6.3 Lexicon additions
  - Add disruption, data effect, reversibility, replacement, confidence, and citation to
    `docs/platforms/operator-language.md` with their definitions and banned alternatives
  - _Requirements: 9.6_

- [ ] 6.4 **Checkpoint** — Feature 1's Property 11 (lexicon conformance) still passes with
  the new vocabulary; rendered output reviewed against the canonical transcripts.

## Phase 7 — Coverage enforcement

- [ ] 7.1 Maintain the kind registry
  - A test-visible registry of Tier 1 and Tier 2 kinds, mirroring the requirements'
    inventory
  - _Requirements: 7.1, 7.3, 7.4_

- [ ] 7.2 **PBT: Property 9 — tier coverage holds**
  - Every registered Tier 1/2 kind declares its applicable fields above `Unknown`
  - _Property 9; Requirements: 7.2, 7.5_

- [ ] 7.3 **PBT: Property 10 — every claim cites**
  - Every `ProviderGuarantee` and `EngineFact` in the registry carries a non-empty citation
  - _Property 10; Requirements: 2.2, 2.3, 2.6_

- [ ] 7.4 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`.

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
