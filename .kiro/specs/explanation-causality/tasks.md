# Implementation Plan: Explanation Causality

Ordered: Feature 1 amendment → snapshot seam (proven canonical) → source gathering
(proven uncontaminated) → the classifier (proven against an oracle) → grouping and
dependants → rendering. Every correctness property is a required property-based test
task.

**Prerequisite:** Feature 1 complete through its Phase 5. Feature 2's Phase 1 (the
`Confidence<T>` relocation) if Feature 2 lands first; otherwise task 1.1 here relocates
nothing and simply uses `Confidence<T>` from wherever it lives at the time — the two
specs' amendments are commutative.

## Phase 1 — Feature 1 amendment

- [x] 1.1 Cause slot becomes `Confidence<Cause>`
  - Remove `Cause::Undetermined`; `ExplainedChange.cause: Confidence<Cause>` with
    `Unknown` default
  - Update `.kiro/specs/explanation-evidence-model/design.md` so the sibling records the
    corrected shape
  - Feature 1's Property 8 (slots silent) re-run against the new default
  - DONE (2026-07-30): the code half pre-landed with Feature 1 (the amendment was
    applied before F1 Phase 3, per this spec's first branch) — `ExplainedChange.cause:
    Confidence<Cause>`, no `Undetermined`. This task updated the sibling design's Data
    Models (stale `Cause`/`Confidence` sketches → the landed shapes; `Confidence<T>`
    pointer to the change-semantics vocabulary in `tokeira-iac`) and re-ran Property 8
  - _Requirements: amendment; 2.7_

- [x] 1.2 New uncertainty reasons
  - `CauseUndecidable { resource }` and `BaselineUnavailable { revision }` in
    `UncertaintyReason`; both constructed only by this feature
  - DONE (2026-07-30): both variants + evidence-id tags (`cause-undecidable`,
    `baseline-unavailable`), reserved-until doc comments naming A9/A10; inert this
    phase — no constructor exists yet
  - _Requirements: 2.5, 2.7_

- [x] 1.3 **Checkpoint** — workspace green; explanation output unchanged (unknown cause
  renders as nothing, exactly as the undetermined default did).
  - DONE (2026-07-30): tokeira-explain + tokeira-provisioner-cli suites green with the
    variants inert; full bar at the slice boundary.

## Phase 2 — The desired-snapshot seam

- [x] 2.1 Share the canonicalization
  - Move `canonicalize_manifest` to a location shared by the compose diff boundary and
    the snapshot path, so one function — not two agreeing ones — owns canonical form
  - DONE (2026-07-30): `canonicalize_manifest` made `pub` in its owning crate (no move
    needed — the diff boundary lives beside it and the snapshot path imports it); doc
    states the one-owner rule
  - _Requirements: 1.4_

- [x] 2.2 `ProvisionerPlatform::desired_snapshot`
  - Trait method with the purity contract in its doc comment; default is NotApplicable
    for platforms without an interpreted definition
  - DONE (2026-07-30): `DesiredSnapshot` type alias (BTreeMap keyed by the engine's
    `ResourceId`, so snapshots join changes without translation) + the defaulted trait
    method with the MUST-NOT purity contract; default asserted by test
  - _Requirements: 1.1, 1.2, 1.5_

- [x] 2.3 compose platform implementation
  - `load_tkd_config_from(dir, definition)` → realize → per-resource `to_manifest()` →
    canonicalize; a source that does not interpret returns the located verdict
  - The working definition and a retained revision go through this one path
  - DONE (2026-07-30): the one path is `interpret_definition` — config loading,
    `definition check`, and snapshots all verify through it. Design deviation, for the
    better: `to_manifest()` exists only on compose services, so the platform's `Kind`
    trait gains an explicit `manifest()` (authored desired content as JSON) and
    `Deployment::desired_snapshot` composes kind manifests + canonical service
    manifests (service realization factored to one helper shared with
    `realize_module`, so infra-dependency wiring cannot diverge). The IaC framework is
    untouched — desired content is a platform concern
  - _Requirements: 1.3, 1.6_

- [x] 2.4 **PBT: Property 7 — snapshot canonicality**
  - Realize twice → equal; permute set-valued authored order → equal; working vs retained
    copy of identical content → equal
  - DONE (2026-07-30): `platforms/compose/tests/causality_snapshot.rs` (authored pre-rename as compose-syn) — generated
    permutations of ports/volumes/needs authored order → equal snapshots; the reference
    definition deterministic; two-paths equality through the platform seam; the broken
    source returns the located verdict
  - _Property 7; Requirements: 1.4, 1.6_

- [x] 2.5 **Checkpoint** — seam green; no provider calls observable from snapshot tests
  (test platform records and asserts zero).
  - DONE (2026-07-30): purity is structural — the snapshot path is interpret + realize
    with no `ProvisionContext`, so no provider handle exists to call (AWS clients and
    the Docker platform enter only as apply-time context extensions); the suite runs
    providerless (no daemon, no credentials) and passes.

## Phase 3 — Gathering the sources

- [x] 3.1 `CausalityInputs` and `BaselineSnapshot`
  - Baseline resolved from `envelope.config_revision` via `config_history`'s path;
    `NeverApplied`, `Missing { revision }`, `DoesNotInterpret { verdict }` typed, not
    stringly
  - DONE (2026-07-31) — `causality::GatheredCausality` + `gather_causality` in
    `tokeira-provisioner-cli`; `config_history::snapshot_path` widened to `pub(crate)`.
    Recorded deviations: the shell reuses the classifier's `BaselineView` (one typed
    enum, not a second agreeing shell mirror); `DoesNotInterpret` carries its
    `revision` so the plan-level uncertainty can name it; a fifth arm
    `NotInterpreted` types the platform-without-definitions case (the defaulted
    snapshot seam), classifying per A10 without a `BaselineUnavailable`
  - _Requirements: 2.5_

- [x] 3.2 Read S before refresh
  - Recorded state loaded from the store as persisted, before the verb's refresh runs;
    WHY comment naming the contamination trap (refresh overwrites in-context properties
    with live observations — a contaminated S turns drift detection into live-vs-live)
  - DONE (2026-07-31) — S loads through the new
    `ProvisionerPlatform::recorded_state` seam (defaulted to the empty state; compose
    implements it over `adapter::infra_store`, the one owner of the `state/infra`
    convention — the engine and causality cannot drift onto different paths); gathered
    before `infra_plan`/`deploy_plan` in both plan verbs, with the trap named at the
    module and both call sites. Plan verbs verified store-write-free on every path
  - _Requirements: 2.3_

- [x] 3.3 **PBT: Property 5 — S-isolation**
  - Classify before and after a simulated refresh overwrite of the planning context;
    assessments must be identical
  - DONE (2026-07-31), in two halves (recorded deviation): the shell half is
    structural — `gather_causality` takes no planning context at all, so a
    contaminated S is unrepresentable in the pipeline; the explain half
    (`property_5_a_contaminated_s_would_reclassify_drift_as_clean`) demonstrates the
    hazard the isolation prevents — the same scenario classifies `ProviderDrift` with
    store-S and `EngineAdvance` with the live-overwritten view
  - _Property 5; Requirements: 2.3_

- [x] 3.4 **Checkpoint** — inputs gather on a live-shaped fixture deployment; baseline
  variants (present, never-applied, missing, broken) each produce their typed value.
  DONE (2026-07-31) — baseline variants exercised across the property battery's
  generated worlds (all five arms); crate suites green.

## Phase 4 — The classifier

- [x] 4.1 Implement the algebra (A1–A10 with A3b)
  - `classify_causes` in `tokeira-explain`: pure, order-significant per the requirements
    table; existence rows (A1–A3b) first — the never-recorded create classifies A3b,
    never `Unknown`; A6 before A7; A9 guarding A7/A8; A10 branches from
    `BaselineSnapshot`
  - DONE (2026-07-31) — `causality::assess` + the public `apply_causality` enrichment
    (causes, dependants, uncertainties, groups joined onto the built explanation; the
    entry point is the join, so `classify_causes`' map never exists separately —
    recorded naming deviation). A7/A8's `L ≠ S` operationalized by an inert engine
    widening: `RefreshCoverage.live_departed`, computed inside `refresh_state` at the
    only moment recorded and live both exist (properties-only comparison; confirmed
    absence of a recorded resource is departure by definition; unit-pinned engine-side).
    A8 takes the change's existence as the `D ≠ S` evidence — the engine's own diff,
    not a second agreeing comparison. Off-table inputs (a change in no source;
    non-create on a never-applied deployment) answer `Unknown` + uncertainty
  - _Requirements: 2.1, 2.2, 2.4, 2.6_

- [x] 4.2 Output tracing (A4)
  - Differing leaves of `D(R) − P(R)` traced per the state-diff predicate: a recorded
    dependency edge in G, and the output's S-value matching the working side while
    departing from the baseline side; fires only on the exactly-one-dependency,
    every-field-traced condition; inference confidence
  - DONE (2026-07-31) — `trace_outputs` with the three gates; the output vocabulary is
    the dependency's recorded **properties** scalars (ground-truth correction, evidence
    table 2026-07-31: `InfraState.outputs` has no producer; properties are what the
    writeback wiring reads). Leaf diffing recurses objects and treats arrays as one
    leaf (can never match a scalar output — the conservative, A5-ward direction);
    exactly-one is counted over (dependency, output) pairs, so two matching outputs
    within one dependency are ambiguity too
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [x] 4.3 Uncertainties for undecidable causes
  - A9/A10 emit `CauseUndecidable` / `BaselineUnavailable` with consequence and
    resolution; one per affected change, one `BaselineUnavailable` per plan
  - DONE (2026-07-31) — one `CauseUndecidable` per undecided change (each names its
    resource, consequence, and — where one exists — the resolving action), reusing
    Feature 1's `push_uncertainty` so evidence-id minting has one owner; one
    plan-level `BaselineUnavailable` (subject: the deployment) for `Missing` **and**
    `DoesNotInterpret` baselines, both carrying the revision
  - _Requirements: 2.5, 2.7_

- [x] 4.4 **PBT: Property 2 — the algebra is followed exactly**
  - Independent table-literal oracle of the full table (A1–A10 with A3b) kept test-side;
    generated tuples cover every row and every precedence collision
  - DONE (2026-07-31) — `tests/causality.rs::oracle`: a sequential row walk in the
    requirements' own shape (A10's baseline branch, then A1→A3b→A4→A5→A6→A9→A7→A8),
    with its own independent A4 predicate; compared shape-and-confidence against the
    classifier over generated (D, P, S, L, G, changes) worlds spanning every row,
    every baseline arm, and unexamined/unconfirmed L
  - _Property 2; Requirements: 2.2_

- [x] 4.5 **PBT: Property 1 — assessment is total and unique**
  - DONE (2026-07-31) — non-`NoChange`: a known cause XOR exactly one
    `CauseUndecidable` naming it; `NoChange`: `Unknown` cause and no uncertainty
  - _Property 1; Requirements: 2.1_

- [x] 4.6 **PBT: Property 3 — deterministic and pure**
  - DONE (2026-07-31) — two applications from one input serialize byte-identically;
    purity is structural (a pure fn over refs in a crate with no I/O capability)
  - _Property 3; Requirements: 2.6_

- [x] 4.7 **PBT: Property 4 — no drift claim without a confirmed live read**
  - DONE (2026-07-31) — unconfirmed/unexamined L never classifies `ProviderDrift` or
    `EngineAdvance` (Property 1 supplies the paired uncertainty guarantee)
  - _Property 4; Requirements: 2.4, 2.7_

- [x] 4.8 **PBT: Property 6 — output tracing unambiguous or absent, on identity + state diff**
  - Constructed ambiguities (two candidates; partial trace; value match without the
    edge; value match without the state departure) must fall to A5
  - DONE (2026-07-31) — the clean trace fires A4; the no-edge, no-departure, and
    two-output constructions each fall to A5
  - _Property 6; Requirements: 3.2, 3.3, 3.5_

- [x] 4.9 **PBT: Property 10 — unknown causes ↔ uncertainties, one-to-one**
  - DONE (2026-07-31) — |Unknown causes| == |`CauseUndecidable`|, and
    `BaselineUnavailable` present exactly when the baseline is `Missing` or
    `DoesNotInterpret`
  - _Property 10; Requirements: 2.7_

- [x] 4.10 Example tests: the named scenarios
  - The demon test (D = P, L ≠ S ×5 → five `ProviderDrift`); the label migration
    (D = P, L = S, D ≠ S → `EngineAdvance`); grafana's removal (A3); the interrupted
    apply (R ∈ D, D = P, R ∉ S → A3b, never `Unknown`); the never-applied deployment
    (A10 creates rule)
  - DONE (2026-07-31) — all five as fixtures in `tests/causality.rs`, plus the
    transitive cascade (task 5.1's one-story assertion rides it)
  - _Requirements: 2.2_

- [x] 4.11 **Checkpoint** — classifier green against oracle and scenarios.
  DONE (2026-07-31) — 15/15 in `tests/causality.rs`; F1's suites green with the
  closure walk and artifact key allow-list extended for `causal_groups`.

## Phase 5 — Grouping and dependants

- [x] 5.1 `CausalGroup` and `CausalRoot`; grouping per design
  - Partition; BFS-from-root member order with the deterministic tiebreak; roots per
    Requirement 4.3 — cascades walk to the ultimate root, bounded by the engine-version
    and baseline boundaries (Requirement 4.6); per-change causes keep naming the nearest
    dependency
  - DONE (2026-07-31) — `CausalGroup`/`CausalRoot` in the model
    (`causal_groups` serde-defaulted at the document level — the slot pattern);
    `EvidenceId::group` + `EvidenceKind::CausalGroup`; the walk takes the terminal
    cause's root (design C4, tightened 2026-07-31), so the edit-driven transitive
    cascade is genuinely one group under the revision comparison; output-trace roots
    reference the dependency's change id (`NoChange` changes carry ids, so closure
    holds without a new identity kind); unknown-cause changes are their own roots;
    member order is in-group BFS layering, ties and unconnected members by id
  - _Requirements: 4.1, 4.2, 4.3, 4.5, 4.6_

- [x] 5.2 Dependant sets
  - Reverse edges over the union of desired and recorded resources; joined onto each
    explained change; graph only, no heuristics
  - DONE (2026-07-31) — desired-side edges arrive by an inert engine widening
    (`PlanOutcome.edges_by_id`, collected over the *known* set so unchanged
    dependants keep their reverse edges; carried unfiltered like coverage and
    semantics); the shell unions them with `ResourceState.dependencies` in
    `causality_view`; reverse edges joined per non-`NoChange` change
  - _Requirements: 5.1, 5.3_

- [x] 5.3 **PBT: Property 8 — groups partition; roots are the bounded ultimate roots**
  - DONE (2026-07-31) — exact partition of the non-`NoChange` changes, no empty
    group, no group rooted at a cascade member, every group id resolving
  - _Property 8; Requirements: 4.1, 4.2, 4.3, 4.5, 4.6_

- [x] 5.4 **PBT: Property 9 — dependants are the reverse graph, exactly**
  - DONE (2026-07-31) — set equality against the view's reverse edges, both
    directions, over generated worlds
  - _Property 9; Requirements: 5.1, 5.3_

- [x] 5.5 **Checkpoint** — `cargo test -p tokeira-explain` green.
  DONE (2026-07-31) — 29 tests across the crate's suites, plus the engine's
  live-departure unit pin and the shell's 79 green.

## Phase 6 — Rendering, lexicon, integration

- [x] 6.1 Causes on the lines; assessments, chains, dependants at detail
  - The clause is the concrete change per the umbrella's `output-templates.md` —
    the operator's own diff for edits, the fields that changed outside the definition
    for drift, never a cause category, never a revision number (the header anchors
    the revision once); detail adds the derived `why:` voice, the unknown cause's
    uncertainty in place, dependants split between changing-with-it and
    continuing-unchanged (empty lines omitted), and the `chain:` line once on a
    multi-member group's first member, root first
  - DONE (2026-07-31, operator-reviewed twice) — `cause_phrase`/`diff_clause`/
    `drift_clause` + `revision_anchor` + `change_detail` in `render.rs`; the
    classifier's uncertainty copy reworded to the lexicon; the umbrella gained D10
    and `output-templates.md`, whose reference transcripts the renderer is asserted
    against byte-for-byte (`the_output_templates_doc_is_executable`)
  - _Requirements: 4.4, 5.2, 5.4, 6.1, 6.2, 6.3, 6.4_

- [x] 6.2 Lexicon additions
  - cause, drift, dependant, causal chain, root added to `operator-language.md`;
    Feature 1's lexicon-conformance property re-run
  - DONE (2026-07-31) — five rows with their banned-in-prose synonyms
    (classification, algebra, drifted, downstream, cascade, causal group, ultimate
    root) joined the suite's executable list; the canonical transcript carries cause
    clauses; the doc-drift test and Property 11 green
  - _Requirements: 6.6_

- [x] 6.3 `--json` carries assessments, groups, dependants at any depth
  - DONE (2026-07-31) — asserted in the rendering test: `causal_groups` serialized,
    engine-fact assessments present; the `Serialize` delegation is unchanged, so the
    artifact and `--json` remain one schema
  - _Requirements: 6.7_

- [x] 6.4 End-to-end integration
  - `platforms/compose/tests/exercise.rs`: edit the definition copy → plan → the
    edited resource classifies `DefinitionEdit`, untouched resources classify clean;
    over the live seam chain
  - DONE (2026-07-31) — `causality_classifies_a_definition_edit_over_the_live_seam_chain`:
    real `ComposeProvisioner::infra_plan`, real snapshots of the working and retained
    definitions, recorded state written and read through the platform's own store
    layout; assertions environment-independent (the edit is a content row needing no
    live read; untouched resources classify by the running world — drift or an
    in-place uncertainty — but never as the edit)
  - _Requirements: 1.6, 2.1, 2.2_

- [x] 6.5 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`.
  DONE (2026-07-31) — all five green. **Feature 3 is complete**: Phases 1–6 landed.

## Task Dependency Graph

```text
[Feature 1 through its Phase 5]
        ↓
1.1 → 1.2 → 1.3 (checkpoint)
        ↓
1.3 → 2.1 → 2.2 → 2.3 → 2.4 → 2.5 (checkpoint)
        ↓
2.5 → 3.1 → 3.2 → 3.3 → 3.4 (checkpoint)
        ↓
3.4 → 4.1 → 4.2 → 4.3 → {4.4 … 4.9} → 4.10 → 4.11 (checkpoint)
        ↓
4.11 → 5.1 → 5.2 → {5.3, 5.4} → 5.5 (checkpoint)
        ↓
5.5 → 6.1 → 6.2 → 6.3 → 6.4 → 6.5 (final)
```

## Notes

- **The oracle test (4.4) is the load-bearing verification.** The algebra is normative in
  the requirements as a table; keeping a second, table-literal implementation test-side
  means the shipping classifier is checked against the specification's own shape, not
  against itself.
- **Property 5 is the trap made into a test.** The S-contamination hazard is exactly the
  species of bug that costs a day (this session has the receipts); it is cheaper as a
  generated property than as a future hunt.
- **The named example tests are regression memory.** The demon test and the
  label-migration test encode this session's two hardest diagnoses as permanent fixtures;
  if either ever fails, the plan has started lying about "why" again.
- Grouping and dependants (Phase 5) are deliberately after the classifier: groups hang
  off assessments, and an unproven classifier would make group tests assert the wrong
  thing with confidence.
