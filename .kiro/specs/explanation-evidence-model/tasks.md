# Implementation Plan: Explanation Evidence Model

Ordered by dependency: engine widening (proven inert) → seam propagation → the model crate
→ construction → rendering → artifact and CLI → integration. Every correctness property
from the design is a required property-based test task.

## Phase 1 — Engine: refresh status survives planning

- [x] 1.1 Make refresh status public and orderable
  - Change `RefreshStatus` in `crates/tokeira-iac/src/engine.rs` to `pub`, derive
    `Serialize`/`Deserialize`, and re-export from `lib.rs`
  - Derive `Ord`/`PartialOrd` on `ResourceId` so it can key a `BTreeMap`
  - Document each variant's meaning for explanation consumers (what an operator can and
    cannot conclude from it)
  - _Requirements: 5.1_

- [x] 1.2 Add `RefreshCoverage` and `PlanOutcome`
  - `RefreshCoverage { status_by_id: BTreeMap<ResourceId, RefreshStatus>, examined: bool }`
  - `PlanOutcome { changes: Vec<Change>, refresh: RefreshCoverage }`
  - `BTreeMap` is required, not preferred — serialization order must be a function of the
    keys (Property 2); state this as the WHY comment on the field
  - _Requirements: 5.1, 5.5_

- [x] 1.3 Widen the five plan entry points
  - `Engine::plan`, `plan_with_known`, `plan_for_modules`, `plan_destroy`,
    `plan_destroy_for_modules` return `PlanOutcome`
  - `plan_with_known` stops discarding `refresh_state`'s `status_by_id`; remove the
    `#[allow(dead_code)]` on `RefreshReport`
  - `Engine::destroy` consumes `plan_destroy(..).changes` — no behaviour change
  - Set `examined: false` on any path that produces changes without a refresh
  - _Requirements: 5.1, 5.5, 5.6_

- [x] 1.4 **PBT: Property 7 — widening preserves planning**
  - Retain the pre-widening change computation as a test-only helper; assert over
    generated compositions and states that `PlanOutcome.changes` equals it in order and
    content
  - _Property 7; Requirements: 5.6_

- [x] 1.5 **Checkpoint** — `cargo check --workspace --locked`, `cargo test -p tokeira-iac`,
  `cargo lint --locked` clean. The engine's behaviour is unchanged and proven so before
  anything consumes the new data. DONE — Phases 1–2 merged as PR #28 (e517923c):
  `RefreshStatus` pub + kebab-case serde with per-variant operator docs; `RefreshCoverage`/
  `PlanOutcome` (+`Default`); five entry points widened; `has_managed_missing` removed
  (derivable); Property 7 differential oracle (`legacy_plan_changes`) green.

## Phase 2 — Seams: orchestrator and platform

- [x] 2.1 Widen the orchestrator plan surface
  - `InfraEngine::plan` and `plan_destroy` return `PlanOutcome`
  - Module filtering continues to apply to `.changes` only; coverage is carried unfiltered
    (a resource filtered from this plan's changes was still examined)
  - _Requirements: 5.2_

- [x] 2.2 Widen the platform seam
  - `ProvisionerPlatform::infra_plan -> Result<PlanOutcome>`;
    `deploy_plan -> Result<Realization<PlanOutcome>>`
  - Update `compose-syn` and any other implementor; `deploy_plan` continues to realize as
    the infra plan
  - _Requirements: 5.3_

- [x] 2.3 Adapt existing consumers with no behaviour change
  - `plan.rs`, `deploy.rs`, and the destructive gate in `apply.rs`/`deploy.rs` take
    `.changes` where they took the vector
  - `platforms/compose-syn/tests/exercise.rs` and orchestrator tests updated
  - _Requirements: 5.4, 5.6_

- [x] 2.4 **Checkpoint** — workspace check, test, lint clean; `tkr infra plan` output is
  byte-identical to before this phase. DONE — in PR #28: orchestrator + `ProvisionerPlatform`
  seams return `PlanOutcome` (coverage carried unfiltered through module selection);
  consumers take `.changes`; exercise tests assert coverage totality, never a specific
  status (environment-independent).

## Phase 3 — The model crate

- [x] 3.1 Create `crates/tokeira-explain`
  - Workspace member; dependencies limited to `serde`, `serde_json`, `tokeira-iac`
  - Module docs stating the crate's contract: it models, it does not decide; no network,
    no model, no provisioner dependency
  - _Requirements: 9.1, 9.2, 9.3_

- [x] 3.2 Evidence identity
  - `EvidenceId` with natural-key constructors (change, uncertainty, impact, deployment)
  - `EvidenceIndex` as `BTreeMap<EvidenceId, EvidenceKind>` with `resolve`
  - WHY comment recording why ordinal ids were rejected (stable only while iteration order
    is; Property 2)
  - _Requirements: 3.1, 3.3, 3.4, 3.5_

- [x] 3.3 Model types
  - `DeploymentExplanation`, `ExplainedChange`, `OperationalImpact`, `Uncertainty`,
    `UncertaintyReason`, `CommittedChange`/`CommittedOp`
  - `EXPLANATION_SCHEMA_VERSION` constant
  - Every field documented with its source of truth and its unavailable-value, matching
    the requirements' field policy
  - _Requirements: 1.1, 1.2, 1.6, 2.1_

- [x] 3.4 Slot types with honest defaults
  - `Confidence<T>` with `Unknown` as `#[default]`; `ProviderGuarantee` carries its
    citation in the value so an uncited provider claim is unrepresentable
  - `ChangeSemantics`, `Cause` (`Undetermined` default), `SourceLocation`
  - `SemanticsUndeclared` and `ProviderAssignedAtApply` defined and **not constructed** by
    this feature; comment stating why (they activate in Feature 2, and emitting them now
    would attach an uncertainty to every change in every plan)
  - _Requirements: 8.1, 8.2, 8.3_

- [x] 3.5 **PBT: Property 3 — evidence closure**
  - For any constructed explanation, every `EvidenceId` referenced anywhere resolves in
    the index to exactly one fact
  - _Property 3; Requirements: 3.1, 3.3_

- [x] 3.6 **PBT: Property 2 — construction is deterministic**
  - Construct twice from one generated input; assert serializations are byte-identical,
    including every collection's order
  - _Property 2; Requirements: 3.2, 3.4_

- [x] 3.7 **Checkpoint** — `cargo test -p tokeira-explain`, lint and doc clean.
  DONE — Phases 3–4 merged as PR #31 (f0b13f8b): crate with serde/serde_json/tokeira-iac
  only; natural-key `EvidenceId`s; `Citation` stores `Cow<'static, str>` (recorded
  deviation: `&'static str` cannot `Deserialize`) with const non-empty assert; Properties
  2 and 3 green.

## Phase 4 — Construction from engine outputs

- [x] 4.1 Plan-side construction
  - `DeploymentContext` (identity, platform, operation, revisions, definition ref) and
    `explain_plan(context, &PlanOutcome)`
  - Exactly one explained change per engine change, including `NoChange`
  - Destructive set derived from the engine's own classification, never from slots
  - _Requirements: 1.1, 1.3, 1.4, 1.5, 1.6_

- [x] 4.2 Uncertainty derivation
  - `RefreshStatus::Unknown` → one `LiveStateUnconfirmed` per affected resource, with
    consequence and (where one exists) the resolving action
  - `examined: false` → exactly one `LiveStateNotExamined` for the plan
  - No uncertainties → the model states full confirmation rather than an empty section
  - _Requirements: 4.1, 4.2, 4.4, 4.5, 5.5_

- [x] 4.3 Apply-side construction
  - `explain_applied(context, &[ChangeLogEntry], preceding: Option<&PlanOutcome>)`, mapping
    entries to `CommittedChange` at the shell boundary so the crate stays free of
    `tokeira-provisioner`
  - Reuse the preceding plan's field evidence for matching ids; with no preceding plan,
    emit `FieldEvidenceUnavailable` and no field diffs
  - Never synthesize a before-image (Proposal 002)
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 4.4 **PBT: Property 1 — change coverage is total**
  - _Property 1; Requirements: 1.1, 1.3_

- [x] 4.5 **PBT: Property 4 — uncertainty is exhaustive over unconfirmed state**
  - Generated coverage including all-unknown, none-unknown, and unexamined
  - _Property 4; Requirements: 4.1, 4.2, 5.5_

- [x] 4.6 **PBT: Property 9 — apply-side explanation invents nothing**
  - _Property 9; Requirements: 2.2, 2.3, 2.4_

- [x] 4.7 **Checkpoint** — model construction green under `cargo test -p tokeira-explain`.
  DONE — in PR #31: `explain_plan`/`explain_applied` + `DeploymentContext`;
  refresh-derived uncertainties (one `LiveStateNotExamined` for an unexamined verb, one
  `LiveStateUnconfirmed` per Unknown-status change); Properties 1, 4, 9 green.

## Phase 5 — Rendering

- [x] 5.1 `Report` implementation in `crates/tokeira-provisioner-cli/src/render.rs`
  - Summary: change counts by kind, acting resources, destructive actions, impacts,
    uncertainty count
  - Detail: field evidence, refresh status, each uncertainty in full, populated slots only
  - Not-determined slots render as nothing
  - _Requirements: 6.1, 6.2, 6.4, 6.6, 6.7, 8.4_

- [x] 5.2 Migrate the plan verbs onto the explanation
  - `tkp infra plan` and `deploy plan` render the explanation model; `PlanReport` retains
    only the attention-only binding line, so one model backs one report
  - `--json` emits the complete explanation regardless of depth
  - _Requirements: 6.3, 6.4_

- [x] 5.3 **PBT: Property 5 — detail is a superset of summary**
  - _Property 5; Requirements: 6.1, 6.2_

- [x] 5.4 **PBT: Property 8 — not-determined slots are silent**
  - _Property 8; Requirements: 6.6, 8.4_

- [x] 5.5 **PBT: Property 6 — structured form is complete and depth-blind**
  - Identical JSON at either depth; round-trips to an equal model
  - _Property 6; Requirements: 6.3, 7.2_

- [x] 5.6 **PBT: Property 11 — rendering stays inside the lexicon**
  - Extract the banned list from `docs/platforms/operator-language.md` at test time and
    assert absence from rendered output at both depths — the lexicon becomes executable
  - _Property 11; Requirements: 6.5, 10.3_

- [x] 5.7 **Checkpoint** — rendering green; `tkr infra plan` and `--detail` reviewed
  against the canonical transcripts in the language doc. DONE — Phase 5 merged as PR #33
  (2021629b): `ExplanationReport` replaces `PlanReport` (Serialize delegates to the model,
  Req 6.3); uncertainty section with the `?` glyph; Properties 5/6/8/11 live in
  `render.rs` against the shipped renderer (5.6 deviation, recorded: the banned list is a
  suite constant cross-checked against the doc rather than parsed from it — same
  no-drift guarantee, and the executable list also covers the lexicon table's
  banned-in-prose column, which caught `provider` in the resolution copy). Live review
  against `compose-explore`: summary/detail/JSON conform.

## Phase 6 — Artifact and CLI surface

- [x] 6.1 Artifact write and read
  - Serialize the complete model; parse without deployment-directory access
  - Write failure fails the verb with path and reason; the verb is never reported as
    succeeded after a failed write
  - _Requirements: 7.1, 7.2, 7.3, 7.6_

- [x] 6.2 `--explanation <path>` on `tkp` plan and apply verbs, forwarded by `tkr`
  - Orthogonal to `--json`; both may be requested
  - No socket, port, or listening stream is opened (umbrella D1)
  - _Requirements: 7.1, 7.5_

- [x] 6.3 **PBT: Property 10 — the artifact is self-contained and bounded**
  - Parse without directory access; assert evidence closure and that serialized keys are a
    subset of the field policy
  - _Property 10; Requirements: 7.1, 7.3, 7.4_

- [x] 6.4 **Checkpoint** — artifact round-trip green; `--explanation` verified against a
  live compose deployment. DONE — Phase 6 slice: `tokeira_explain::artifact` (write/read +
  `ExplainError` carrying path and reason, Req 7.6; `std::fs` only — the structural
  no-socket guarantee, D1); `--explanation` on `tkp` infra/deploy plan and apply
  (plan writes before reporting; apply writes after the envelope commit so a failed write
  fails the verb without costing the revision advance — tested); forwarded verbatim by
  `tkr` (in-process platforms refuse with the contract); Property 10 green; live against
  `compose-explore`: artifact byte-equals the `--json` model, closure holds over the
  parsed file alone.

## Phase 7 — Lexicon and integration

- [x] 7.1 Lexicon additions
  - Add uncertainty, confirmation coverage, impact, and evidence to
    `docs/platforms/operator-language.md`, with the banned-word replacements they imply
  - _Requirements: 10.1, 10.2_

- [x] 7.2 End-to-end integration test
  - `platforms/compose-syn/tests/exercise.rs`: a plan against the reference definition
    produces a model whose changes match the plan's, whose evidence closure holds, and
    whose uncertainties reflect the refresh coverage — exercising engine → orchestrator →
    platform → shell
  - _Requirements: 1.1, 4.1, 5.4_

- [x] 7.3 Crate and module documentation
  - `tokeira-explain` module docs state the contract and the boundary; the `Report` impl
    records the C5 migration path (if a consumer outside `tkp` needs to render, the impl
    moves into the crate and takes `tokeira-report`)
  - _Requirements: 9.4, 9.5_

- [x] 7.4 **Final checkpoint** — the full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`. DONE — Phase 7 slice: lexicon gains **live state** (coverage
  statement) and **impact** (defined ahead of emission; uncertainty landed in Phase 5,
  evidence predates F1); exercise.rs asserts the model over the real engine outcome
  (coverage totality, closure, uncertainty==Unknown-status count — internal-consistency
  form, never a specific status, so Docker-present and Docker-absent worlds both pass);
  C5 migration note recorded on the `Report` impl. Full bar green. **Feature 1 is
  complete.**

## Task Dependency Graph

```text
1.1 → 1.2 → 1.3 → 1.4 → 1.5 (checkpoint)
                    ↓
1.5 → 2.1 → 2.2 → 2.3 → 2.4 (checkpoint)
                    ↓
2.4 → 3.1 → 3.2 → 3.3 → 3.4 → {3.5, 3.6} → 3.7 (checkpoint)
                                      ↓
3.7 → 4.1 → 4.2 → 4.3 → {4.4, 4.5, 4.6} → 4.7 (checkpoint)
                                      ↓
4.7 → 5.1 → 5.2 → {5.3, 5.4, 5.5, 5.6} → 5.7 (checkpoint)
                                      ↓
5.7 → 6.1 → 6.2 → 6.3 → 6.4 (checkpoint)
                    ↓
6.4 → 7.1 → 7.2 → 7.3 → 7.4 (final)
```

Phase 1 must complete before anything else: it is the only phase that touches the engine's
public plan surface, and its checkpoint proves the widening inert. Phases 3 and 4 could in
principle proceed in parallel with Phase 2, but the model's construction inputs are the
widened seam's outputs, so the serial order is kept.

## Notes

- **Phase 1 is deliberately dull and deliberately first.** It changes five signatures
  across a crate that everything depends on, and it changes no behaviour. Landing it alone,
  with Property 7 proving equivalence, means every later failure is attributable to
  explanation logic rather than to the widening.
- **Property 11 turns the language doc into a test.** If it proves noisy in practice (a
  banned word appearing legitimately inside a resource id, say), the fix is to scope the
  assertion to narrative lines the renderer produces, not to weaken the lexicon.
- **Task 5.2 retires duplicate rendering.** After it, `PlanReport` no longer renders
  changes; one model backs one report. Leaving both would guarantee divergence.
- No task in this plan adds a provider call, a network dependency, or a model dependency.
  Any task that appears to require one is a signal the design has been misread.
