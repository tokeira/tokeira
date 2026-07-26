# Implementation Plan: Explanation Source Spans

Ordered: amendments → the diff (proven formatting-blind before anything consumes it) →
enclosure resolution → config value comparison → the seam → the join → rendering. Every
correctness property is a required property-based test task.

**Prerequisite:** Feature 3 complete (its causes, snapshots, and `BaselineSnapshot` are
this feature's inputs).

## Phase 1 — Amendments

- [ ] 1.1 Umbrella amendment
  - Replace umbrella Requirements 4.1.1–4.1.2 with outcome-focused criteria per this
    spec's Introduction; supersede the "threaded, not reconstructed" note
  - _Requirements: Introduction (amendment)_

- [ ] 1.2 Feature 1 amendment: `SourceLocation.basis`
  - `AttributionBasis { Stanza, ValueFlow }`, serde-default so the addition is
    non-breaking; update the Feature 1 design record
  - _Requirements: Introduction (amendment); 6.2_

- [ ] 1.3 **Checkpoint** — workspace green; no behaviour change anywhere.

## Phase 2 — Edit detection in `tokeira-tkd`

- [ ] 2.1 `attribution` module: parallel AST walk
  - `definition_edits(baseline, working)`: token-normalized comparison per node
    (`to_token_stream().to_string()` — the normalization IS the equality; extra-traits
    equality would be defeated by span fields, say so in the WHY comment)
  - Smallest-differing-expression descent; item-granularity additions/removals;
    length-mismatched sequences emit at the parent; removal spans land on the enclosing
    working-side node
  - _Requirements: 1.1, 1.3, 1.4_

- [ ] 2.2 **PBT: Property 1 — formatting is invisible**
  - Generated formatting-only transforms: whitespace, comments, token spacing
  - _Property 1; Requirements: 1.2_

- [ ] 2.3 **PBT: Property 2 — edits are exactly the semantic difference**
  - Empty-iff-equal both directions; single-node mutations report exactly the mutated
    region; spans verified by re-slicing the source at the reported line/column
  - _Property 2; Requirements: 1.1, 1.3_

- [ ] 2.4 **PBT: Property 3 — detection is pure and deterministic**
  - _Property 3; Requirements: 1.4, 1.6_

- [ ] 2.5 **Checkpoint** — `cargo test -p tokeira-tkd` green.

## Phase 3 — Enclosure resolution and config comparison

- [ ] 3.1 `resolve_enclosing`
  - Config value paths from `fn config()`'s struct expression; stanza module/name from
    `d.service`/`d.resource` literal arguments; non-literal stanza names resolve to
    `Definition` with no speculation; writebacks, types, function level
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

- [ ] 3.2 `changed_config_values`
  - Host-free `config()` evaluation of both sides (the `retarget_check` pattern);
    parallel walk of the two config values; canonical scalar rendering;
    introduced/removed as `None` sides
  - _Requirements: 3.2_

- [ ] 3.3 **PBT: Property 4 — every edit resolves; stanza resolution is faithful**
  - Generated stanzas with known names; constructed non-literal name case
  - _Property 4; Requirements: 2.1, 2.3, 2.5_

- [ ] 3.4 **Checkpoint** — resolution green over the reference definition (every line of
  it enclosed by exactly one construct).

## Phase 4 — The platform seam

- [ ] 4.1 `ProvisionerPlatform::attribute_edits`
  - `EditAttributionInputs { edits, changed_config }`; read-only contract in the doc
    comment; NotApplicable default; compose-syn delegates to `tokeira-tkd::attribution`
  - Invoked by the shell only when the plan carries a `DefinitionEdit` cause; baseline
    path from Feature 3's `BaselineSnapshot`
  - _Requirements: 1.5, 4.3_

- [ ] 4.2 **PBT: Property 8 — non-interference holds observationally**
  - Desired snapshots byte-identical with and without the pass; instrumented platform
    asserts zero `deployment()` evaluations from attribution
  - _Property 8; Requirements: 4.1, 4.2_

- [ ] 4.3 **Checkpoint** — seam green; zero provider calls recorded.

## Phase 5 — The join in `tokeira-explain`

- [ ] 5.1 `attribute_definition_edits`
  - Stanza pass, then value-flow pass (exact, unambiguous, against Feature 3's changed
    manifest fields), then bookkeeping: no-effect edits and `AttributionUnavailable`
    uncertainties; multiple attributions ordered stanza-first then by span
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 5.2 **PBT: Property 5 — attribution decorates, never classifies**
  - Cause multiset identical before and after the join, for any inputs
  - _Property 5; Requirements: 3.6_

- [ ] 5.3 **PBT: Property 6 — value-flow fires only on unambiguous exact matches**
  - Constructed two-candidate ambiguity and partial-match cases yield none
  - _Property 6; Requirements: 3.2, 3.3_

- [ ] 5.4 **PBT: Property 7 — the accounting is exact**
  - Edits partition into attributed ∪ no-effect; `DefinitionEdit` changes partition into
    sourced ∪ uncertainty-named
  - _Property 7; Requirements: 3.5, 5.1, 5.2_

- [ ] 5.5 **PBT: Property 9 — attribution failure cannot fail the verb**
  - _Property 9; Requirements: 1.5, 4.4_

- [ ] 5.6 **Checkpoint** — `cargo test -p tokeira-explain` green.

## Phase 6 — Rendering, lexicon, integration

- [ ] 6.1 Rendering
  - Summary: attributed groups phrase construct + basename:line, replacing revision-level
    phrasing only where attribution exists; unattributed groups keep Feature 3's phrasing
    with the could-not-establish note
  - Detail: per-change attributions with basis (`ValueFlow` marked derived), columns, and
    the no-effect section (omitted when empty, plain statement of meaning)
  - _Requirements: 5.4, 6.1, 6.2, 6.3, 6.4_

- [ ] 6.2 Lexicon additions
  - edit, no-effect edit → `operator-language.md`; Feature 1's lexicon-conformance
    property re-run
  - _Requirements: 6.5_

- [ ] 6.3 `--json` carries attributions with basis and no-effect edits at any depth
  - _Requirements: 6.6_

- [ ] 6.4 Example fixtures
  - The canonical `storage` transcript scenario; grafana image bump (value-flow); a
    stanza-internal `publish` edit (stanza basis); the interpolation limitation as a
    fixture (fallback + uncertainty, documented not surprising); a no-effect edit
  - _Requirements: 3.1, 3.2, 3.5, 5.1_

- [ ] 6.5 End-to-end integration
  - `platforms/compose-syn/tests/exercise.rs`: edit the definition copy's grafana image →
    plan → the cause line carries `observability.grafana.image` and the correct line
    number
  - _Requirements: 6.1_

- [ ] 6.6 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`.

## Task Dependency Graph

```text
[Feature 3 complete]
        ↓
1.1 → 1.2 → 1.3 (checkpoint)
        ↓
1.3 → 2.1 → {2.2, 2.3, 2.4} → 2.5 (checkpoint)
        ↓
2.5 → 3.1 → 3.2 → 3.3 → 3.4 (checkpoint)
        ↓
3.4 → 4.1 → 4.2 → 4.3 (checkpoint)
        ↓
4.3 → 5.1 → {5.2, 5.3, 5.4, 5.5} → 5.6 (checkpoint)
        ↓
5.6 → 6.1 → 6.2 → 6.3 → 6.4 → 6.5 → 6.6 (final)
```

## Notes

- **The diff is proven blind before it is trusted** (Phase 2 before everything): a
  formatting-sensitive diff would flood plans with phantom edits — the same demon family
  as the port-order roulette, in a new seam, and this plan refuses to build on it
  unproven.
- **The interpolation limitation ships as a fixture** (6.4): the known case where
  value-flow cannot match is encoded as a test asserting the *fallback* behaviour, so the
  boundary of the mechanism is documented by the suite rather than discovered by an
  operator.
- Property 5 is this feature's safety property, the analogue of Feature 2's
  destructive-set invariance: a span can never move a cause.
