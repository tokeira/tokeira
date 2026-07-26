# Implementation Plan: Explanation Analysis Protocol

Ordered: bundles (produce side proven round-trippable) → the library (proven confined,
typed, faithful) → comparison → transports (proven at parity) → integration. Every
correctness property is a required property-based test task.

**Prerequisite:** Features 1, 3, and 4 complete (artifact, snapshots + edges, syntactic
diff). Feature 2 enriches bundle content but gates nothing here.

## Phase 1 — Bundles

- [ ] 1.1 `BundleSnapshot` and the bundle layout
  - `snapshot.json` (canonical manifests + edges from the realized resources'
    `dependencies()` at produce time), `explanation.json` (Feature 1 artifact verbatim),
    definition copy under the platform basename
  - `state/analysis/last-plan/` and `state/analysis/revisions/{n}/`
  - _Requirements: 1.1, 1.2, 1.4_

- [ ] 1.2 Retention in the producing verbs
  - Applying verbs retain beside `config_history`'s revision snapshot, same conventions
    (idempotent overwrite, config-less tolerance, basename keying); plan verbs replace
    last-plan
  - Retention failure fails the verb through the artifact-write error shape
  - _Requirements: 1.1, 1.2, 1.5, 1.6, 1.7_

- [ ] 1.3 **PBT: Property 10 — retention round-trips**
  - Produce through real plan/apply paths against the reference definition; open with the
    library; closure, canonical parse, edges name residents
  - _Property 10; Requirements: 1.1, 1.3, 1.4_

- [ ] 1.4 **Checkpoint** — bundles appear under a fixture deployment; workspace green;
  producing verbs' behaviour otherwise unchanged.

## Phase 2 — The library

- [ ] 2.1 `crates/tokeira-analysis`: `AnalysisStore` and the query methods
  - Dependencies exactly: `tokeira-explain`, `tokeira-tkd`, serde stack, `thiserror`; no
    platform crate, no provider SDK, no process/network capability
  - Typed `AnalysisError` variants carrying each not-found's promised payload
  - Answers verbatim from the bundle (only comparison composes; only excerpt slices)
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.7_

- [ ] 2.2 Path confinement
  - One resolution function for every file access: canonicalize, verify ancestry under
    the bundle root; `OutOfBundle` otherwise
  - _Requirements: 6.2_

- [ ] 2.3 Schema gating
  - Newer → typed refusal with both versions and remedy; readable-older → absent fields
    absent, never defaulted
  - _Requirements: 2.5, 2.6_

- [ ] 2.4 **PBT: Property 2 — analysis reads only the bundle**
  - Instrumented I/O sandbox records every open; set-inclusion under the bundle root;
    dependency-graph assertion for process/network absence
  - _Property 2; Requirements: 2.2, 2.3, 6.2, 6.3, 6.5_

- [ ] 2.5 **PBT: Property 3 — not-found is typed, everywhere**
  - _Property 3; Requirements: 2.4, 3.4, 5.4_

- [ ] 2.6 **PBT: Property 4 — answers are byte-faithful**
  - _Property 4; Requirements: 2.7_

- [ ] 2.7 **PBT: Property 7 — dependency paths are exactly the retained graph's**
  - _Property 7; accounting table_

- [ ] 2.8 **PBT: Property 8 — excerpts are confined and faithful**
  - Includes constructed traversal attacks: `../`, absolute paths, symlinked escapes
  - _Property 8; Requirements: 6.2_

- [ ] 2.9 **PBT: Property 9 — schema gating is honest in both directions**
  - _Property 9; Requirements: 2.5, 2.6_

- [ ] 2.10 **Checkpoint** — `cargo test -p tokeira-analysis` green.

## Phase 3 — Comparison

- [ ] 3.1 `compare_revisions`
  - `definition_edits` over the two retained texts; snapshot deltas
    (introduced/removed/modified with field-level differences); counts; computed once and
    reversible
  - Cross-platform pair refuses via the basename mismatch, mirroring revert
  - _Requirements: 3.1, 3.2, 3.3, 3.5, 3.6_

- [ ] 3.2 **PBT: Property 6 — comparison is grounded and symmetric**
  - Both directions of grounding, plus (N,M)/(M,N) reversal equality
  - _Property 6; Requirements: 3.1, 3.2, 3.3, 3.6_

- [ ] 3.3 **Checkpoint** — the session's own scenario as fixture: revision-with-grafana vs
  revision-without → one removed resource, the stanza edit located.

## Phase 4 — Transports

- [ ] 4.1 `tkr analysis query <name> [inputs]`
  - One query, exit; `--json` = the typed answer verbatim; narrative through
    `tokeira-report` under the depth rules; unknown query name refuses with the table's
    names
  - _Requirements: 5.1, 5.2, 5.4, 7.2_

- [ ] 4.2 `tkr analysis serve` (MCP over stdio)
  - One tool per table row, snake_cased, read-only-marked, lexicon descriptions; no
    deployment lock held; bundle re-stat per query, reload on change; empty deployment
    answers name the producing verbs
  - The MCP implementation dependency (official SDK vs. minimal JSON-RPC loop) is
    proposed for approval at this task with the house dependency rules; the choice is
    confined to `transport::mcp` either way
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 7.3_

- [ ] 4.3 **PBT: Property 1 — the surface is closed**
  - Tool set ≡ table ≡ one-shot names; no mutating entry point exists to reach
  - _Property 1; Requirements: 2.1, 4.2, 5.1, 6.1_

- [ ] 4.4 **PBT: Property 5 — transport parity**
  - Both transports in-process over one store; identical values
  - _Property 5; Requirements: 5.3_

- [ ] 4.5 **Checkpoint** — scripted MCP session over real stdio pipes against a fixture
  deployment; parity asserted end to end.

## Phase 5 — Lexicon, docs, integration

- [ ] 5.1 Lexicon additions
  - analysis, bundle, comparison → `operator-language.md`; Feature 1's lexicon-conformance
    property re-run
  - _Requirements: 7.1_

- [ ] 5.2 The D7 statement
  - Document that the analysis surface serves definitions as authored and that umbrella
    D7 (no cleartext secrets in definitions; platform-secret references only) is the
    guarantee that makes this safe
  - _Requirements: 6.4_

- [ ] 5.3 End-to-end integration
  - Produce bundles on a live-shaped fixture (plan + apply), then:
    `tkr analysis query get-deployment-summary --json`, a dependency-path query, an
    excerpt from a Feature 4 attribution, and a two-revision comparison
  - _Requirements: 1.1, 3.1, 5.1_

- [ ] 5.4 **Final checkpoint** — full bar: `cargo +nightly fmt --all`,
  `cargo lint --locked` (zero warnings), `cargo check --workspace --locked`,
  `cargo test --workspace --locked`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace
  --no-deps --locked`.

## Task Dependency Graph

```text
[Features 1, 3, 4 complete]
        ↓
1.1 → 1.2 → 1.3 → 1.4 (checkpoint)
        ↓
1.4 → 2.1 → 2.2 → 2.3 → {2.4 … 2.9} → 2.10 (checkpoint)
        ↓
2.10 → 3.1 → 3.2 → 3.3 (checkpoint)
        ↓
3.3 → 4.1 → 4.2 → {4.3, 4.4} → 4.5 (checkpoint)
        ↓
4.5 → 5.1 → 5.2 → 5.3 → 5.4 (final)
```

## Notes

- **The bundle is the API.** Everything after Phase 1 is a reader; if a query ever needs
  something a bundle lacks, the fix is to retain more at produce time, never to compute
  at query time — that line is what keeps the analysis process credential-free,
  provider-free, and safe to hand to an agent.
- **Property 2's sandbox is the trust story made executable**: "read-only over files"
  is asserted by recording every open, not by review.
- Requirement 4.3's phrasing ("under this spec or its successors") is deliberate: the
  next person who proposes `tkp serve` should find the refusal already written down,
  with the session's reasoning behind it.
