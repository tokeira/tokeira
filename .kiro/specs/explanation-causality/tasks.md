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

- [ ] 3.1 `CausalityInputs` and `BaselineSnapshot`
  - Baseline resolved from `envelope.config_revision` via `config_history`'s path;
    `NeverApplied`, `Missing { revision }`, `DoesNotInterpret { verdict }` typed, not
    stringly
  - _Requirements: 2.5_

- [ ] 3.2 Read S before refresh
  - Recorded state loaded from the store as persisted, before the verb's refresh runs;
    WHY comment naming the contamination trap (refresh overwrites in-context properties
    with live observations — a contaminated S turns drift detection into live-vs-live)
  - _Requirements: 2.3_

- [ ] 3.3 **PBT: Property 5 — S-isolation**
  - Classify before and after a simulated refresh overwrite of the planning context;
    assessments must be identical
  - _Property 5; Requirements: 2.3_

- [ ] 3.4 **Checkpoint** — inputs gather on a live-shaped fixture deployment; baseline
  variants (present, never-applied, missing, broken) each produce their typed value.

## Phase 4 — The classifier

- [ ] 4.1 Implement the A1–A10 algebra
  - `classify_causes` in `tokeira-explain`: pure, order-significant per the requirements
    table; A6 before A7; A9 guarding A7/A8; A10 branches from `BaselineSnapshot`
  - _Requirements: 2.1, 2.2, 2.4, 2.6_

- [ ] 4.2 Output tracing (A4)
  - Differing leaves of `D(R) − P(R)` matched against changed recorded outputs of R's
    dependencies; fires only on the exactly-one-dependency, every-field-traced condition;
    inference confidence
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 4.3 Uncertainties for undecidable causes
  - A9/A10 emit `CauseUndecidable` / `BaselineUnavailable` with consequence and
    resolution; one per affected change, one `BaselineUnavailable` per plan
  - _Requirements: 2.5, 2.7_

- [ ] 4.4 **PBT: Property 2 — the algebra is followed exactly**
  - Independent table-literal oracle of A1–A10 kept test-side; generated tuples cover
    every row and every precedence collision
  - _Property 2; Requirements: 2.2_

- [ ] 4.5 **PBT: Property 1 — assessment is total and unique**
  - _Property 1; Requirements: 2.1_

- [ ] 4.6 **PBT: Property 3 — deterministic and pure**
  - _Property 3; Requirements: 2.6_

- [ ] 4.7 **PBT: Property 4 — no drift claim without a confirmed live read**
  - _Property 4; Requirements: 2.4, 2.7_

- [ ] 4.8 **PBT: Property 6 — output tracing unambiguous or absent**
  - Constructed ambiguities (two candidates; partial trace) must fall to A5
  - _Property 6; Requirements: 3.2, 3.3_

- [ ] 4.9 **PBT: Property 10 — unknown causes ↔ uncertainties, one-to-one**
  - _Property 10; Requirements: 2.7_

- [ ] 4.10 Example tests: the named scenarios
  - The demon test (D = P, L ≠ S ×5 → five `ProviderDrift`); the label migration
    (D = P, L = S, D ≠ S → `EngineAdvance`); grafana's removal (A3); the never-applied
    deployment (A10 creates rule)
  - _Requirements: 2.2_

- [ ] 4.11 **Checkpoint** — classifier green against oracle and scenarios.

## Phase 5 — Grouping and dependants

- [ ] 5.1 `CausalGroup` and `CausalRoot`; grouping per design
  - Partition; BFS-from-root member order with the deterministic tiebreak; roots per
    Requirement 4.3
  - _Requirements: 4.1, 4.2, 4.3, 4.5_

- [ ] 5.2 Dependant sets
  - Reverse edges over the union of desired and recorded resources; joined onto each
    explained change; graph only, no heuristics
  - _Requirements: 5.1, 5.3_

- [ ] 5.3 **PBT: Property 8 — groups partition**
  - _Property 8; Requirements: 4.1, 4.2, 4.5_

- [ ] 5.4 **PBT: Property 9 — dependants are the reverse graph, exactly**
  - _Property 9; Requirements: 5.1, 5.3_

- [ ] 5.5 **Checkpoint** — `cargo test -p tokeira-explain` green.

## Phase 6 — Rendering, lexicon, integration

- [ ] 6.1 Summary renders groups; detail renders assessments, chains, dependants
  - Revision-level phrasing mandatory ("between revision N and the working definition");
    derived classifications marked derived; unknown causes render via uncertainty only;
    unaffected dependants stated at detail; empty dependant sections omitted
  - _Requirements: 4.4, 5.2, 5.4, 6.1, 6.2, 6.3_

- [ ] 6.2 Lexicon additions
  - cause, drift, dependant, causal group, root added to `operator-language.md`;
    Feature 1's lexicon-conformance property re-run
  - _Requirements: 6.4_

- [ ] 6.3 `--json` carries assessments, groups, dependants at any depth
  - _Requirements: 6.5_

- [ ] 6.4 End-to-end integration
  - `platforms/compose/tests/exercise.rs`: edit the definition copy → plan → the
    edited resource classifies `DefinitionEdit`, untouched resources classify clean;
    over the live seam chain
  - _Requirements: 1.6, 2.1, 2.2_

- [ ] 6.5 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`.

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
