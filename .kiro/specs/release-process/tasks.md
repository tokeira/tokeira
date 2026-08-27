# Implementation Plan: Release Process Governance

## Overview

Build the release-governance layer over the existing `temporal-compatibility` substrate: build-time
provenance enforcement, the Dagger-hosted version-pin monotonicity + bump-trailer CI checks, and the
`tkr compat bump` engine. Order: provenance gate → CI checks (the gate that the bump engine relies on) →
bump engine → CLI wiring → baseline record → docs. Consumes `temporal-compatibility` tasks 9–11 (the
`tkr ci`/Dagger substrate); if those are not yet landed, they are a prerequisite. Remote triggers are
out of scope (`pipeline-foundation`).

## Tasks

- [x] 1. Build-time provenance gate (`tokeira-build-info` `build.rs`)
  - [x] 1.1 Release+CI fails on empty `TOKEIRA_GIT_SHA`; release-non-CI warns + `dev`; debug resolves
    `git rev-parse --short=8 HEAD` or `dev`. Confirm the image pipeline injects the env vars.
    - _Requirements: 1.1, 1.2, 1.3, 1.4_
    - **DONE (2026-08-27, PR #130):** the build script enforces the profile/CI provenance gate,
      and the image pipeline injects `CI`, `TOKEIRA_GIT_SHA`, and `TOKEIRA_SOURCE_TREE_HASH`
      before its locked release build.

- [ ] 2. CI checks in `crates/tokeira-build/src/pipelines/ci.rs`
  - [ ] 2.1 `run_ci_checks(request, dagger) -> CiCheckReport` + `CiCheck`/`CiCheckRequest`/`CiCheckResult`
    (serde), workspace mounted with the `target/`-excluding filter, pinned `debian:bookworm-slim`.
    - _Requirements: 2.4, 6.1, 6.2_
  - [ ] 2.2 Proto + server-compat monotonicity checks (pin@tip vs pin@last-tag; override trailers
    `Proto-Downgrade:` / `Server-Compat-Downgrade:`).
    - _Requirements: 2.1, 2.2, 2.3_
  - [ ] 2.3 Bump-trailer check (any `pinned.rs` diff → `git interpret-trailers --parse` → validate
    against the diff).
    - _Requirements: 3.3_
  - [ ] 2.4 `trailer.rs`: `BumpTrailer` parse/render (Req 3.3 regex).
    - _Requirements: 3.3_

- [ ] 3. Property tests for checks + trailer
  - [ ] 3.1 P1 (pin regression detection + override), P2 (trailer/diff consistency), P6 (report
    round-trip).
    - _Feature: release-process, Property 1, Property 2, Property 6_
    - _Requirements: 2.1, 2.2, 2.3, 3.2, 3.3, 6.1, 6.2_
  - [ ] 3.2 P3 trailer round-trip + regex.
    - _Feature: release-process, Property 3_
    - _Requirements: 3.3_

- [ ] 4. Bump engine `crates/tokeira-build/src/compat_bump/`
  - [ ] 4.1 `mod.rs` (`BumpRequest`/`BumpOutcome`/`run_bump`) + `BumpContext` + phase scaffolding.
    - _Requirements: 4.1, 4.4_
  - [ ] 4.2 phases: preflight (target newer / tree clean / branch / creds), evidence (octocrab release
    enumeration + bodies + matrix delta + optional `--derive-surfaces`), mutate (branch/edit/commit+
    trailer/run CI), publish (push / PR / rewrite PR number / amend).
    - _Requirements: 4.1, 4.2, 4.3_
  - [ ] 4.3 `github.rs` (octocrab pagination + rate-limit → `BumpError::RateLimited`), `template.rs`,
    `pr_template.md` rendering the Upstream Releases / Disposition / Matrix Delta / SDK Evidence sections.
    - _Requirements: 3.1, 3.4, 3.5_
  - [ ] 4.4 P4 phase-ordering / fail-closed tests (stubbed git + GitHub, dry-run); equal/downgrade
    rejected in preflight.
    - _Feature: release-process, Property 4_
    - _Requirements: 4.1, 4.2_

- [ ] 5. CLI wiring (`apps/tkr`)
  - [ ] 5.1 Extract the shared `dagger_reexec` helper into `apps/tkr/src/dagger_reexec.rs`; wire
    `tkr ci check [--check] [--json]`.
    - _Requirements: 2.4, 6.2_
  - [ ] 5.2 Wire `tkr compat bump --to <version> [--dry-run] [--no-open] [--derive-surfaces] [--resume]`.
    - _Requirements: 4.1, 4.3_

- [ ] 6. Governance records + docs
  - [ ] 6.1 `CODEOWNERS` names `crates/tokeira-build-info/src/pinned.rs`; `AGENTS.md` gains the
    "Server compat bump protocol" subsection.
    - _Requirements: 3.6, 3.7_
  - [ ] 6.2 Baseline `docs/compat-bumps/0-baseline.md` for the current `TEMPORAL_SERVER_COMPAT`.
    - _Requirements: 5.1_

- [ ] 7. Checkpoint
  - `cargo +nightly fmt`, `cargo lint`, `cargo test` on touched crates; `tkr ci check` green locally.

## Task Dependency Graph

```json
{
  "waves": [
    { "wave": 0, "tasks": ["1.1"] },
    { "wave": 1, "tasks": ["2.1", "2.2", "2.3", "2.4"] },
    { "wave": 2, "tasks": ["3.1", "3.2"] },
    { "wave": 3, "tasks": ["4.1", "4.2", "4.3", "4.4"] },
    { "wave": 4, "tasks": ["5.1", "5.2"] },
    { "wave": 5, "tasks": ["6.1", "6.2"] },
    { "wave": 6, "tasks": ["7"] }
  ]
}
```

The CI checks (wave 1) are a prerequisite of the bump engine's `mutate` phase (it runs them). CLI wiring
(wave 4) depends on the engine + checks. Provenance (wave 0) is independent and first.

## Notes

- Prerequisite: the `tkr ci`/Dagger substrate from `temporal-compatibility` tasks 9–11.
- New dependency `octocrab` (per the source drafts) — confirm at implementation.
- Out of scope: remote triggers (`pipeline-foundation`); tagging/channels/changelog/registry publication
  (deferred broad release-management); matrix content + handshake + `tkr compat show/diff`
  (`temporal-compatibility`); image build/publish (`image-lifecycle`).
- This spec consolidates `temporal-compatibility/{requirements,design}-orig.md` §§(5.4, 5.5, 6.x / 7, 8),
  which are retired once this spec lands.
