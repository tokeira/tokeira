# Implementation Plan

- [x] 1. The engine indication replaces the contract counters — DONE 2026-08-04 in `ce88bc91`
  (merged via PR #74 `d9c3c9ff`): platform descriptors carry exact `engine` asserted against the
  workspace version at discovery with the adoption-instruction refusal; range syntax rejected;
  frontend descriptors carry no version field; `PLATFORM_BINDING_CONTRACT` /
  `DEFINITION_FRONTEND_CONTRACT` and their validation sites removed; `BoundProvisionerEvidence`
  records `engine` in place of both counters and bound admission compares it as an assembly fact;
  generated-root digest bumped; live descriptors (`platforms/compose`, `crates/tokeira-tkd`, and
  `crates/tokeira-tkdp`) migrated. _Requirements: 1.1, 1.2, 1.3, 1.4, 2.1, 2.2, 2.3, 2.4, 2.5,
  3.2, 3.3, 3.4, 3.5, 5.3_

- [ ] 2. Refusal quality in discovery
  - [ ] 2.1 Named counter rejection: inspect the raw metadata value before typed decode; presence
    of `binding-contract` / `frontend-contract` refuses naming the field and its replacement
    (`engine`), ahead of generic unknown-field handling. _Requirements: 3.1_
  - [ ] 2.2 Stable-field refusals: add `stable_fields` best-effort extraction and thread it
    through every descriptor refusal path so a rejection whose `id` and `engine` (or `format`)
    parse names them. _Requirements: 2.6_

- [ ] 3. Property test: Property 1 — descriptor admission is total over the indication
  - Generated raw metadata values (counters present/absent, indication well-formed/range/mismatch);
    ≥100 iterations. Tag: `// Feature: engine-versioning, Property 1`.
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 3.1, 3.2_

- [ ] 4. Property test: Property 2 — refusals name the stable fields
  - ≥100 iterations over refusal-inducing descriptors with parseable stable fields.
    Tag: `// Feature: engine-versioning, Property 2`. _Requirements: 2.6_

- [ ] 5. Checkpoint: `tokeira-build` compiles, lints clean, discovery tests green.

- [ ] 6. The published catalog keyed by engine (`crates/tokeira-provisioner/src/catalog.rs`)
  - [ ] 6.1 `PublishedProvisionerLocator` gains `engine`; document the descriptor/locator
    division (descriptor `engine` = newest admitted release, locator `engine` = resolution key).
    _Requirements: 4.3_
  - [ ] 6.2 `EngineReleaseRecord { engine, locators, surface_delta }` and
    `admit_release` enforcing at-most-one locator per `(platform, format, engine)`, duplicate
    refusal naming the triple, catalog unchanged on refusal, locator/release engine agreement,
    and the unchanged canonical-authority gate. _Requirements: 4.1, 4.3, 4.4, 4.6_
  - [ ] 6.3 Evidence round-trip coverage for the `engine` field where the evidence
    serialization tests live. _Requirements: 1.3, 3.3_

- [ ] 7. Resolution against the indication (`apps/tkr/src/catalog.rs`)
  - [ ] 7.1 `published_locator(platform, format)` selecting the entry whose engine equals the
    platform's indication; missing triple refuses naming the triple and the engines published
    for the pair. _Requirements: 5.1, 5.2_
  - [ ] 7.2 Published-fixture integration test with two engines: exact selection, and the
    missing-triple refusal text. _Requirements: 5.1, 5.2, 5.4_

- [ ] 8. Property tests: Properties 3–5
  - Property 3 (evidence round-trip + bound-admission agreement), Property 4 (catalog key
    uniqueness over admission sequences), Property 5 (resolution selects exactly the
    indication). ≥100 iterations each; tags `// Feature: engine-versioning, Property N`.
  - _Requirements: 1.3, 3.3, 4.3, 4.4, 5.1, 5.2_

- [ ] 9. Checkpoint: `tokeira-provisioner` + `tkr` compile, lints clean, catalog and resolution
  tests green.

- [ ] 10. The versioning document and release runbook
  - [ ] 10.1 `docs/operations/engine-versioning.md`: the layer table (keys vs labels vs
    declaration), the descriptor contract, and the release act — bump commit as the only
    change-site, tag, canonical builds of the admitted matrix, `admit_release`, and the
    Engine_Surface_Delta convention (`docs/releases/engine-<version>.md`). Reference it from the
    descriptor and catalog rustdoc. _Requirements: 4.1, 4.2, 4.5, 6.3_

- [ ] 11. Final ledger pass: DONE records on completed tasks; full workspace finishing bar
  (fmt, lints zero-warning, check, tests, docs with warnings denied) plus the local lychee and
  cargo-deny gates. _Requirements: 6.1, 6.2, 6.4_

## Task Dependency Graph

```
1 (landed) ──▶ 2.1 ──▶ 3
1 ──▶ 2.2 ──▶ 4
2.* ──▶ 5
1 ──▶ 6.1 ──▶ 6.2 ──▶ 7.1 ──▶ 7.2
6.2 ──▶ 8 ◀── 6.3
7.* ──▶ 8 ──▶ 9
6.2 ──▶ 10.1
9, 10.1 ──▶ 11
```

## Notes

- Task 1 is the ledger record of the already-merged slice; nothing in it remains to implement.
- Tasks 6–8 touch `tokeira-provisioner` catalog surfaces the platform-config workstream may also
  reshape; re-ground 6.1's type layout against main when implementation starts.
- The release act's git steps (bump commit, tag) and the CI runner are operational, not coding
  tasks; the in-repo machinery is the record type, admission, the surface-delta convention, and
  the runbook (task 10).
