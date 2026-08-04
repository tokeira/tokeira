# Tkdp Frontend Implementation Plan

- [x] 1. Platform accommodations — DONE 2026-08-04 (658e9821), delivered as the
  operator-directed engine kind library: provider crates export complete kind sets,
  `crates/tokeira-kinds` unions them (`EngineKind`, engine-owned `kind_functions`),
  platforms declare wired providers, and `verify_wiring` refuses unwired kinds at
  `definition check`.
  - [x] 1.1 Add the kind-name inventory to `KindFunctions<K>`: a `names: &'static [&'static str]`
    field backing `contains`; Compose supplies one `const` slice for both; `tokeira-tkd` ignores
    the field; add the platform test asserting `contains(n)` for every listed name and that the
    decode arms cover exactly the listed set. _Requirements: 2.9_
  - [x] 1.2 — DONE 2026-08-04 (658e9821). Add the enum-position struct admission arm to the `LocatedValue` deserializer: a
    `ValueShape::Struct { name, fields }` where an enum is expected decodes as the variant tagged
    by `name` — unit when `fields` is empty, newtype-struct otherwise; unknown variant names fail
    with the struct's range; `Enum` and `String` admissions unchanged. _Requirements: 2.10, 2.11_
  - [x] 1.3 — DONE 2026-08-04 (658e9821). Required PBT — Property 11 (variant-spelling equivalence, deserializer half): for any
    enum target and variant instance spelled as a struct-shaped value, the decoded result equals
    the `Enum`-shape spelling's result; unit/newtype boundary covered. Lives beside the arm in
    `tokeira-platform`. _Property 11. Requirements: 2.10, 2.11_

- [x] 2. Checkpoint — DONE 2026-08-04: full §10.4 bar green (457-test workspace
  suite). Checkpoint: workspace bar green with accommodations in place, `.tkd` behaviour unchanged
  (full §10.4 bar; Compose parity-untouched: existing platform tests pass unmodified).

- [x] 3. Monty admission and crate scaffold — DONE 2026-08-04 (225c26d4): git pin 69f8a613 + deny sources exception; descriptor carries identity facts only (post-descriptor-slice convention); probes gate the pin.
  Original:
  - [x] 3.1 Admit Monty into the workspace in one lockfile-owning slice: `monty` + `monty-types`
    git-pinned at the recorded revision (`69f8a613e4f42d2f4dc0e659c792569923531e4f`), ruff crates
    at `0.0.3`, `get-size2` held at `0.10.1`; add the operator-approved `deny.toml` sources
    exception scoped to the two Monty crates with a comment naming its retirement condition (first
    crates.io release containing native dataclasses). _Requirements: 8.1, 8.2, 8.6_
  - [x] 3.2 Scaffold `crates/tokeira-tkdp` as a workspace member: descriptor metadata
    (`format = "tkdp"`, `source-extension = "tkdp"`, `default-relative-path = "definition.tkdp"`,
    no contract or version field), conventional `frontend()` export, `TkdpFrontend` implementing
    `DefinitionFrontend` with a not-yet-wired body, crate docs stating the pipeline and
    boundaries. _Requirements: 1.1, 1.2, 1.4_
  - [x] 3.3 Required probes — Property 14 (capability probes): dataclass construction with
    defaults and keywords, unevaluated field annotations (union spelling), `type()` identity,
    `getattr`/`hasattr`, native-`match` rejection; a probe failure fails the suite and gates any
    pin movement. _Property 14. Requirements: 8.3, 8.4, 8.5_

- [x] 4. Pipeline core: preflight, lowering, source map — DONE 2026-08-04 (225c26d4): spike core productized; TKDP012 import contract; both-entrypoints rule; strict-only lowering with import blanking.
  Original:
  - [x] 4.1 Port the spike's `source_map` (byte-covering segments, verbatim linearity, line
    tables, char-column translation) with internal-region labels for facade and driver.
    _Requirements: 5.1, 5.2, 5.3_
  - [x] 4.2 Port and extend preflight: the restricted match table, hygiene, tab rejection, and
    all-findings-per-pass (TKDP001–011) unchanged; add the import contract (only
    `from tokeira import <names>` with `as` aliases; names validated against builders +
    `kinds.names`; `import tokeira` / `import *` rejected — TKDP012) and the product entrypoint
    rules (both `config` and `deployment` required, exactly once, exact arities).
    _Requirements: 3.1–3.7, 2.2, 2.4, 2.14_
  - [x] 4.3 Port the lowering with strict exhaustion only (no faithful mode), deterministic
    reserved names, and the source label in the exhaustion raise. _Requirements: 4.1–4.10_
  - [x] 4.4 Required PBTs — Properties 1–3: admission soundness over generated admitted
    definitions; rejection completeness with covering ranges over generated rejected constructs;
    byte-identical lowering determinism. _Properties 1, 2, 3. Requirements: 3.1–3.7, 4.10_
  - [x] 4.5 Required PBTs — Properties 4–5: dispatch equivalence against a reference
    CPython-semantics evaluator over generated match tables and subjects (single subject
    evaluation, capture-before-guard, binding persistence, no later-guard evaluation); strict
    exhaustion naming position and subject. _Properties 4, 5. Requirements: 4.1–4.9_
  - [x] 4.6 Required PBT — Property 6: source-map totality, verbatim linearity with multi-byte
    columns, and internal-region rendering with no transient coordinates.
    _Property 6. Requirements: 5.1, 5.2, 5.3, 5.6_

- [x] 5. Checkpoint: `tokeira-tkdp` pipeline core green — DONE 2026-08-04: crate suite green within the workspace bar.
  Original: (crate tests plus workspace bar).

- [x] 6. Facade, assembly, execution, conversion — DONE 2026-08-04 (225c26d4): synthesized facade + offset-preserving import satisfaction; in-sandbox structural exporter (native dataclasses cross as Repr — probed — so the sandbox exports tagged plain data); name-correlated ranges; Monty runner with limits.
  Original:
  - [x] 6.1 Implement facade synthesis from `kinds.names` + serialized context: builder classes
    accumulating plain data with handle-misuse checks, kwargs-shell kind constructors, read-only
    context class, alias bindings; and offset-preserving import satisfaction (equal-width comment
    replacement of the validated import statement). _Requirements: 2.2, 2.3, 2.5–2.9, 2.12_
  - [x] 6.2 Implement transient-program assembly and the driver returning the config value plus
    the deployment envelope as one plain structure, with builder-call ranges recorded from the
    preflight AST. _Requirements: 6.1, 5.1_
  - [x] 6.3 Implement the runner: `MontyRun` execution with configured resource limits, captured
    print output (attached to failures, trace-logged on success), and traceback translation
    through the map. _Requirements: 6.5, 5.2–5.5_
  - [x] 6.4 Implement conversion: envelope → `LocatedValue` (dataclasses as structs), kind kwargs
    merged over `defaults` then `decode` with the constructing call's range,
    `StructuralGraphBuilder` population in declaration order, located `finish()` findings, config
    admission via the entrypoint range + serde field path. _Requirements: 6.2, 6.3, 6.7, 2.5_
  - [x] 6.5 Wire `TkdpFrontend::evaluate` end to end, stateless per invocation, no filesystem or
    network access anywhere in the crate. _Requirements: 6.1, 6.4, 6.6_
  - [x] 6.6 Required PBTs — Properties 7–10 and 12: repeated-evaluate equality and purity;
    resource-limit diagnostics; kind-decode discipline with located unknown-kind/field failures;
    facade totality against `kinds.names`; structural declaration-order preservation with located
    graph findings. _Properties 7, 8, 9, 10, 12. Requirements: 6.1–6.5, 2.7–2.9, 6.3, 6.7_
  - [x] 6.7 Migrate the spike's semantics corpus as fixed example tests (first-match, guard
    fall-through, binding persistence, break/return in case bodies, nested match, field-missing)
    and add error-rendering goldens for each row of the design's error table.
    _Requirements: 4.1–4.9, 5.4, 5.5_

- [x] 7. Checkpoint: full frontend green — DONE 2026-08-04: 13 end-to-end tests + 6 probes green.
  Original: under the workspace bar; `evaluate` exercised through
  `evaluate_definition`/`verify_definition` with a synthetic platform.

- [x] 8. Compose seed, parity, lifecycle — DONE 2026-08-04: definition.tkdp seed beside definition.tkd; Property 13 parity green for both storage variants (equal config/graph/manifests, distinct identities). Peer seeds made no-format creation ambiguous: resolved per R9.7 by the platform-declared `default-format` (compose declares `tkd`; explicit `--format` selects either peer; multiple seeds without a declaration refuse, naming the formats and the remedy) — discovery schema, catalog selection, and both test batteries landed with this slice.
  Original:
  - [x] 8.1 Author the Compose `definition.tkdp` seed as the logical equivalent of
    `definition.tkd` (variant spelling per the requirements exemplars, both storage variants
    expressible, create-time choices encoded identically) and register it as the Compose package's
    `tkdp` seed asset. _Requirements: 7.1, 9.1, 9.2_
  - [x] 8.2 Required parity suite — Property 13: evaluate both seeds with equal contexts under
    both storage variants; assert equal typed configs, equal structural graphs, equal realized
    manifests with fixed invocation facts, and unequal configuration identities.
    _Property 13. Requirements: 7.2–7.6_
  - [x] 8.3 Verify the `.tkdp` deployment lifecycle end to end without Docker: creation-time
    validation and all-or-nothing publication with format `tkdp`; `definition check` through the
    existing output modes; revision retention and same-format revert; authoring-mode
    `definition check --definition <path> --format tkdp`; composition-root assembly with
    `expected_format = "tkdp"` and no new enum arms anywhere. _Requirements: 1.3–1.7, 9.3–9.6_

- [x] 9. Checkpoint: parity and lifecycle green — DONE 2026-08-04 within the workspace bar.
  Original:; a created `tkdp` Compose deployment
  checks/plans/applies in the no-Docker suite exactly as its `tkd` twin.

- [x] 10. Documentation and retirement — DONE 2026-08-04: .tkdp authoring section in docs/provisioning/deployment-definitions.md; spike removed after a full disposition pass — README preserved as crates/tokeira-tkdp/README.md (match boundary, semantics, pipeline, pinning incl. the get-size2 hold, Monty findings updated to the Repr-boundary reality), preflight/lowering/mapping/semantics batteries ported, the guarded-dispatch example carried as an executable Compose fixture, and the `lower --show-generated` capability retained as `TkdpFrontend::transient_program`; final bar green.
  Original:
  - [x] 10.1 Operator documentation: the `.tkdp` authoring surface, facade import form, the match
    admission/rejection table, the two sanctioned CPython deviations, entrypoint signatures, and
    the seed workflow — in `docs/provisioning/deployment-definitions.md` or a sibling it links.
    _Requirements: 10.4_
  - [x] 10.2 Remove `spikes/monty-tkdp` (crate and root `Cargo.toml` exclude entry), absorbing
    its README findings into `tokeira-tkdp` rustdoc. _Requirements: 10.5_
  - [x] 10.3 Final ledger pass: DONE records on every completed task, full §10.4 bar, docs build
    with warnings denied. _Requirements: 10.1, 10.2, 10.3, 10.6_

## Task Dependency Graph

```
1.1 ─┬─▶ 1.3
1.2 ─┘
1.* ──▶ 2 ──▶ 3.1 ──▶ 3.2 ──▶ 3.3
3.2 ──▶ 4.1 ──▶ 4.2 ──▶ 4.3 ──▶ 4.4 ──▶ 4.5 ──▶ 4.6 ──▶ 5
5 ──▶ 6.1 ──▶ 6.2 ──▶ 6.3 ──▶ 6.4 ──▶ 6.5 ──▶ 6.6 ──▶ 6.7 ──▶ 7
1.1 ──▶ 6.1        (facade consumes the inventory)
1.2 ──▶ 6.4        (conversion relies on the admission arm)
3.3 ──▶ 6.5        (probes gate the wired frontend)
7 ──▶ 8.1 ──▶ 8.2 ──▶ 8.3 ──▶ 9 ──▶ 10.1 ──▶ 10.2 ──▶ 10.3
```

## Notes

- **Property tests are required, not optional**: Properties 1–14 each appear above as a named task
  (11 in `tokeira-platform`, 13 as integration, 14 as probes, the rest in `tokeira-tkdp`), tagged
  `// Feature: tkdp-frontend, Property N` in code.
- **Lockfile discipline**: task 3.1 is the only slice that moves workspace dependencies; every
  other slice builds `--locked`. The `deny.toml` exception requires explicit operator approval in
  that slice's review and names its retirement condition.
- **Pin movement protocol**: any Monty revision change re-runs 3.3, 4.4–4.6, 6.6–6.7, and 8.2
  before merge (Requirement 8.5).
- **No Docker, no credentials** anywhere in the default suite; parity and lifecycle tasks operate
  at evaluation/verification/realization level with fixed invocation facts.
- **DONE records**: each task gains a dated DONE line in the landing slice, per repository
  convention.
