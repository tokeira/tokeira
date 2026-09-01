# Implementation Plan

- [x] 1. Establish the shared release contract and changie files
  - [x] 1.1 Add byte-equivalent changie configuration to both repositories
    - Add `.changie.yaml`, `.changes/header.tpl.md`, `.changes/unreleased/`, and root
      `CHANGELOG.md` with the exact design configuration and an initial valid fragment.
    - Add a config-digest fixture proving the two repository copies are byte-equivalent
      and that distinct UUID Slice values produce distinct fragment paths.
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.9, 4.11_
    - DONE: Tokeira copy landed; `fragment.rs` pins the config-set digest against the
      repository files and proves distinct Slice paths. The Odori copy lands with the
      Odori slice.
  - [x] 1.2 Add repository release configuration and Odori's external `tkr` pin file
    - Declare only workspace-specific version replacements, release branch metadata,
      and the full Tokeira Git revision Odori consumes.
    - Keep the tool pin outside both `Cargo.toml` and `Cargo.lock`.
    - _Requirements: 3.3, 12.2, 12.6_
    - DONE: `.tokeira-release.toml` (`schema_version = 1`, `release_branch = "main"`);
      `ReleaseConfig` refuses a `tkr` table in the Tokeira repository. The Odori pin
      file lands with the Odori slice.
  - [x] 1.3 Add the release Plan and Report schema types
    - Implement the complete policy-table fields, portable relative paths, closed enums,
      canonical JSON, and secret-free `Debug`/serialization behavior in
      `crates/tokeira-build/src/pipelines/release/model.rs`.
    - _Requirements: 2.1, 2.12, 8.10, 9.1, 11.1_
    - DONE: `model.rs`; `PackagePlan` carries the hermetic checksum, the Train Identity
      excludes the dated note preview, and `RepositoryIdentity::from_remote` is the one
      normalization both `tkr` and the executor use.
  - [ ] 1.4 Land the owner-authored repository constitution line
    - The operator, not an implementation agent, adds the exact one-line fragment rule
      proposed by Requirement 4.10 to both repository `AGENTS.md` files.
    - _Requirements: 4.10_

- [x] 2. Pin and resolve changie `v1.25.2`
  - [x] 2.1 Add the single changie release pin
    - Implement `ChangieRelease`, the four supported assets, source revision, exact
      upstream archive names, and SHA-256 values in
      `crates/tokeira-build/src/changie_release.rs`.
    - Re-export only the immutable pin and typed asset selector.
    - _Requirements: 5.1, 5.2, 5.3_
    - DONE: `changie_release.rs`.
  - [x] 2.2 Add the verified local changie resolver
    - Reuse the existing cache-lock, temporary download, checksum verification, atomic
      rename, and version-probe pattern under `apps/tkr/src/commands/release/changie.rs`.
    - Refuse unsupported platforms and ignore ambient changie binaries.
    - _Requirements: 5.4, 5.5, 5.6, 5.7_
    - DONE: `apps/tkr/src/commands/release/changie.rs`; fragment cleanup on refusal
      removes only the fragment carrying this invocation's Slice.
  - [x] 2.3 Add Dagger-side changie acquisition
    - Select only the matching pinned Linux asset, verify it inside the Dagger graph,
      and expose the binary to release and CI steps without a hosted action.
    - _Requirements: 5.3, 5.5, 5.8_
    - DONE: `dagger.rs::changie_acquisition_script`, shared by the executor and the
      changelog CI gate.

- [x] 3. Checkpoint: shared contract and tool resolver are green
  - Run formatting, `cargo lint --locked -p tokeira-build -p tkr`, focused unit tests,
    and repository config-digest validation.
  - Confirm neither repository lockfile changed.
  - DONE: crate-scoped clippy, nextest, and rustdoc for `tokeira-build`,
    `tokeira-build-info`, and `tkr`; no lockfile movement.

- [x] 4. Implement workspace discovery, graph planning, and canonical Plans
  - [x] 4.1 Build the Publishable Package graph from Cargo metadata
    - Admit exactly one workspace, select crates.io-publishable members, derive internal
      publishable edges from normal and build dependencies, including target-specific
      and enabled-optional dependencies; exclude dev-dependencies, reject cycles, and
      use a lexical topological tie break. Cover the harmless
      `tokeira-chasm-derive` dev-dependency on `tokeira-chasm` explicitly.
    - _Requirements: 1.2, 1.3, 1.4, 2.3, 2.5, 6.2, 8.1, 8.2_
    - DONE: `graph.rs`.
  - [x] 4.2 Implement source and fragment admission
    - Check clean/up-to-date Git state, stable increasing target version, Unified
      Version, configured base ref, fragment inventory, and canonical config digest.
    - _Requirements: 2.2, 2.3, 2.4, 4.8, 4.9_
    - DONE: `plan.rs`, `fragment.rs`; a fragment body is one bounded sentence judged by
      how it ends, so versions and hosts may appear inside it.
  - [x] 4.3 Implement read-only external observations
    - Observe the release branch and tag together, package/version checksum, dependency
      availability, and existing release state through Dagger-backed seams without
      resolving a registry token. Refuse divergent branch/tag observations while
      naming both object IDs.
    - _Requirements: 2.6, 7.7, 11.1, 11.10, 12.8_
    - DONE: `dagger.rs::observe_release_inputs` also observes the release tag, and the
      Plan's Git effect says "resume" when it is already published; an existing registry
      version whose checksum differs from the hermetic build is refused at plan time.
  - [x] 4.4 Implement deterministic notes preview and Plan digesting
    - Canonicalize portable fields, exclude local roots and advancing observations from
      Train Identity, and emit every outward effect plus release-notes digest.
    - _Requirements: 2.1, 2.7, 2.8, 2.9, 2.12, 10.1, 10.2, 10.3_
    - DONE: `model.rs`; the dated note preview is excluded from the digest and the
      hermetic checksums are included (Property 2).

- [x] 5. Add the standing changelog and packaging CI checks
  - [x] 5.1 Extend `CiCheck` and CLI selection
    - Add `ChangelogFragments` and `PackageDryRun` to the existing report, serde shape,
      `CliCiCheck` mapping, and all-check registry.
    - _Requirements: 1.8, 6.1, 6.8_
    - DONE: `ci.rs`, `cli.rs`.
  - [x] 5.2 Implement the changelog-fragment gate
    - Compare with the admitted base ref, validate added fragments with pinned changie,
      model the exact release batch transition, validate the Slice filename identity,
      and report the failing fragment path.
    - _Requirements: 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.11_
    - DONE: `ci.rs::execute_changelog_gate`; an empty diff passes, manifest-only
      bookkeeping passes, a release preparation is compared with the reference batch
      without the dated headings.
  - [x] 5.3 Implement the locked multi-package publish dry-run
    - Select every Publishable Package in graph order in one Cargo invocation under the
      pinned toolchain, inspect normalized packaged manifests and required consumer
      metadata, and prove source bytes and lockfiles remain unchanged. Keep the sibling
      archives in one packaging overlay so unpublished sibling versions never resolve
      against the registry.
    - _Requirements: 6.2, 6.3, 6.4, 6.5, 6.6, 6.7_
    - DONE: `ci.rs::execute_package_gate`; stale archives in the cache volume are
      cleared before the invocation.

- [x] 6. Checkpoint: planning and standing CI checks are green
  - Run `cargo lint --locked -p tokeira-build -p tkr`, focused nextest suites, doctests
    for changed crates, and `tkr ci check --check changelog-fragments --check
    package-dry-run` against both fixture workspace shapes.
  - DONE: crate-scoped lint, nextest, and doctests; the engine-backed gate invocation
    is operator-run on a Dagger host.

- [x] 7. Implement transactional release source preparation
  - [x] 7.1 Add the structured Unified Version rewriter
    - Update package versions, internal publishable dependency requirements, and the
      exact configured non-Cargo version fields without changing dependency membership,
      sources, features, or ordering.
    - Do not invoke `cargo add` or `cargo remove`.
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 12.6, 12.7_
    - DONE: `prepare.rs::rewrite_manifest` handles inline, aliased, sub-table, and
      target-specific internal dependencies; `rewrite_workspace_manifests` is the one
      rewrite shared by planning and preparation.
  - [x] 7.2 Add changie batch and merge execution
    - Run the explicit target version with `--allow-no-changes=false`, merge the version
      file, and validate the output diff against the admitted fragments and preview.
    - _Requirements: 3.5, 3.6, 4.7, 10.1_
    - DONE: `dagger.rs` preparation chain plus `VALIDATE_PREPARED_SOURCE_SCRIPT`.
  - [x] 7.3 Add atomic preparation export
    - Prepare in an isolated Dagger source, export only a completely validated diff,
      and restore only executor-authored host mutations on pre-push failure.
    - _Requirements: 2.11, 3.10_
    - DONE: preparation, commit, and tag happen on the engine-side snapshot and the
      operator checkout is never written, so there is nothing to export or restore;
      the operator fetches the published refs. The design records this shape.

- [x] 8. Implement release commit, tag, and hermetic packaging
  - [x] 8.1 Create and validate Train Identity in Git objects
    - Add the Plan digest trailer to the Release Commit, create the annotated
      `v<version>` tag, and verify matching local/remote branch and tag objects without
      moving refs. Require both remote refs to exist and identify the same Release
      Commit and Train Identity on resume.
    - _Requirements: 3.7, 3.8, 3.12, 11.10_
    - DONE: `scripts.rs` preparation scripts; `apply.rs::admit_release_refs` decides
      fresh versus resume in Rust from one observation.
  - [x] 8.2 Build all `.crate` artifacts from tagged source
    - Package every Publishable Package in one multi-package Cargo invocation in the
      Dagger toolchain from the exact Release Tag, retain deterministic artifact bytes
      and SHA-256 values, and reject host-source or host-target leakage.
    - _Requirements: 3.9, 3.13, 9.1_
    - DONE: `dagger.rs`; every archive checksum must equal the Plan's hermetic checksum
      before the push, and the SSH agent is removed before packaging.
  - [x] 8.3 Push only after the complete Hermetic Tag Build succeeds
    - Push the exact Release Commit to the configured release branch and the annotated
      tag together with `git push --atomic` before publication; leave no remote
      mutation on preparation/build or atomic-push failure.
    - _Requirements: 3.10, 3.11, 3.12_
    - DONE: `atomic_git_push_arguments` is the executed argv; the push target must
      normalize to the Plan's repository; both refs are observed again after the push.

- [x] 9. Implement token admission and the publish state machine
  - [x] 9.1 Add last-responsible-moment token resolution
    - Accept only an environment-variable name, resolve it after Plan validation and
      confirmation, and omit token values from all serializable/debuggable types. Define
      structurally separate publish-and-parity and release-note request types and
      executor invocations: the first carries only an opaque registry-token handle;
      only after parity succeeds may the handler resolve fixed `GH_TOKEN` and create a
      second request carrying only that release API credential.
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 10.9, 10.10, 10.11, 10.12_
    - DONE: `apply.rs`, `apps/tkr/src/commands/release/mod.rs`; the operator's answer
      is the fence's input.
  - [x] 9.2 Add registry observation and Skip Existing
    - Observe before upload, download/verify Existing Packages, inspect after Cargo
      polling timeouts, then poll after 5 seconds with exponential backoff for no more
      than 10 minutes per crate; retain pending state after bounded ambiguity.
    - _Requirements: 8.3, 8.4, 8.8, 8.9, 8.10, 11.3_
    - DONE: `REGISTRY_PUBLISH_SCRIPT` with the Rust-generated schedule; proven offline
      with stub tools.
  - [x] 9.3 Add serial pacing with a virtual-clock seam
    - Allow one upload in flight, enforce the 600-second success cooldown, and honor a
      longer registry retry deadline without delaying verification-only work.
    - _Requirements: 8.5, 8.6, 8.7_
    - DONE: the cooldown is handed to the script from `apply.rs`; tests stub `date`
      and `sleep`.

- [x] 10. Implement parity, release notes, and resume classification
  - [x] 10.1 Add three-way Artifact Parity
    - Hash hermetic and downloaded bytes, compare registry metadata, classify mismatches
      as terminal, and block all release-note mutation until every package passes.
    - _Requirements: 8.4, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7_
    - DONE: script parity plus `validate_parity_report`, which also requires the
      Plan's hermetic checksum.
  - [x] 10.2 Add deterministic release-note generation
    - Preserve the tagged changie version body, append the Rust 1.97 minimum statement,
      and append the complete lexical package/checksum/page/README table.
    - _Requirements: 10.1, 10.2, 10.3, 10.7_
    - DONE: `notes.rs`; the release-note container receives the generated bytes.
  - [x] 10.3 Add idempotent `gh release create`
    - Run in the separate release-note Dagger invocation with `--verify-tag` and
      `--notes-file`, skip an exact existing release, refuse conflicts, use fixed
      `GH_TOKEN` as that invocation's only secret, and use no generated-note or
      hosted-action mode.
    - _Requirements: 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11, 10.12_
    - DONE: `gh_release_create_arguments` is the executed argv.
  - [x] 10.4 Add the complete train state model
    - Classify pre-publication failure, partial publication, terminal mismatch, and
      completion; admit only the specified resume transitions. Gate every resume on
      observing both remote refs at the same Release Commit and return
      terminal `GitRefConflict` with both observed ref values for any absence or
      divergence.
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 11.10_
    - DONE: `classify_train_state` drives every stopped-train report through
      `ReleaseError::Incomplete`.

- [x] 11. Wire the four `tkr release` sub-verbs
  - [x] 11.1 Add clap types and dispatcher wiring
    - Expose exactly `fragment`, `plan`, `apply`, and `verify`; add no release mutation
      to `tkr ci`.
    - _Requirements: 1.1, 1.8_
    - DONE.
  - [x] 11.2 Implement fragment authoring
    - Resolve the selected workspace, invoke pinned changie interactively or with the
      admitted kind/body and a generated lowercase UUID version 4 Slice value, and
      report the one created relative path.
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 4.3, 4.4, 4.5, 4.11_
    - DONE.
  - [x] 11.3 Implement Plan and Verify rendering
    - Support stdout/atomic output, human and JSON modes, typed exit codes, and the
      read-only boundaries.
    - _Requirements: 1.6, 1.7, 2.1, 9.6, 11.1_
    - DONE: stdout carries only the verb's answer; a stopped train renders its report
      before the refusal.
  - [x] 11.4 Implement Apply revalidation and confirmation
    - Recompute the Plan, render every required outward effect, enforce interactive and
      non-interactive confirmation, invoke publish-and-parity with only the registry
      credential, and only after parity succeeds resolve `GH_TOKEN` and invoke release
      notes with no registry credential.
    - _Requirements: 2.7, 2.8, 2.9, 2.10, 2.11, 10.9, 10.11, 10.12_
    - DONE: the confirmation prompt and rendering go to stderr under `--json`.

- [ ] 12. Add the Odori bootstrap and cross-repository wiring
  - [ ] 12.1 Implement the pinned Git-source `tkr` bootstrap
    - Install with the exact locked Cargo command into the operator cache and verify the
      full source revision before forwarding a release command.
    - _Requirements: 12.2, 12.3, 12.4_
  - [ ] 12.2 Add identical Tokeira and Odori invocation adapters
    - Tokeira runs the in-tree binary; Odori runs the verified installed binary; both
      pass an explicit workspace root and identical remaining release arguments.
    - _Requirements: 12.1, 12.5_
  - [x] 12.3 Add external dependency preflight for Odori
    - Refuse a train before confirmation when a required external registry version is
      absent.
    - _Requirements: 12.8_
    - DONE: `plan.rs` refuses `ExternalDependency` from the observed registry state.

- [x] 13. Checkpoint: end-to-end command compiles and focused tests are green
  - Run formatting, lint, check, focused nextest suites, and doctests for `tokeira-build`
    and `tkr`.
  - Run Odori's focused bootstrap/config tests without changing either lockfile.
  - DONE for the Tokeira slice; the Odori tests belong to the Odori slice.

- [x] 14. Property test: Property 1 — workspace-generic deterministic package plan
  - Generate acyclic workspace graphs containing normal, build, target-specific,
    enabled-optional, and dev-dependency links plus absolute root variants and
    isomorphic Tokeira / Odori fixtures; compare against a reference stable topological
    sort that excludes dev-dependencies for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 1: workspace-generic deterministic package plan`
  - _Requirements: 1.2, 1.3, 1.4, 2.3, 2.5, 8.1, 8.2, 12.5_
  - DONE: `graph.rs`.

- [x] 15. Property test: Property 2 — canonical Plan determinism and secret independence
  - Generate source/external observations, root paths, and token values; assert
    canonical Plan bytes, digest, and confirmation noninterference for at least 256
    cases.
  - Tag: `// Feature: release-engineering, Property 2: canonical Plan determinism and secret independence`
  - _Requirements: 2.1, 2.6, 2.7, 2.8, 2.12, 7.2, 7.6_
  - DONE: `model.rs`; host path, registry state, and the dated preview vary, the
    hermetic bytes do not, and no token byte reaches the canonical Plan.

- [x] 16. Property test: Property 3 — confirmation is a mutation fence
  - Model generated source/Git/registry/release states and refusal causes; assert
    byte-identical state after each refusal for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 3: confirmation is a mutation fence`
  - _Requirements: 2.7, 2.8, 2.9, 2.10, 2.11, 3.10_
  - DONE: `apply.rs`; the fence consumes the operator's real answer.

- [x] 17. Property test: Property 4 — Unified Version rewrite preserves dependency membership
  - Generate manifests with workspace/path/registry dependencies, features, and
    unrelated entries; compare structured rewrite output to a reference model for at
    least 256 cases.
  - Tag: `// Feature: release-engineering, Property 4: Unified Version rewrite preserves dependency membership`
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 12.6, 12.7_
  - DONE: `prepare.rs`; inline and sub-table declarations are generated.

- [x] 18. Property test: Property 5 — fragment gate is complete and explicit
  - Generate non-release and batch-shaped diffs, every fragment kind, and pairs of
    distinct UUID Slice values; compare gate results, rendered notes, and fragment paths
    to pinned reference fixtures for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 5: fragment gate is complete and explicit`
  - _Requirements: 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8, 4.11_
  - DONE: `fragment.rs`.

- [x] 19. Property test: Property 6 — tool acquisition fails closed
  - Generate platform selection, archive corruption, version substitution, cache state,
    and ambient binaries with a fake command executor for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 6: tool acquisition fails closed`
  - _Requirements: 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_
  - DONE: `apps/tkr/src/commands/release/changie.rs`.

- [x] 20. Property test: Property 7 — packaging gate covers the publishable closure
  - Generate workspace metadata and normalized package manifests with path-only edges,
    missing fields, source snapshots, and recorded Cargo invocations; require exactly
    one multi-package invocation and compare to a reference admission model for at
    least 256 cases.
  - Tag: `// Feature: release-engineering, Property 7: packaging gate covers the publishable closure`
  - _Requirements: 3.13, 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8_
  - DONE: `prepare.rs`.

- [x] 21. Property test: Property 8 — publish execution is idempotent
  - Drive generated package DAGs and registry outcome sequences through a fake Dagger
    registry and virtual clock; assert at-most-once observable publication, a first poll
    after 5 seconds, exponential backoff, a hard 10-minute per-crate bound, and correct
    resume for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 8: publish execution is idempotent`
  - _Requirements: 8.3, 8.4, 8.5, 8.8, 8.9, 8.10, 11.1, 11.2, 11.3_
  - DONE: the schedule half in `apply.rs`; the at-most-once half runs the real
    registry script against stub tools in `scripts.rs`.

- [x] 22. Property test: Property 9 — publish pacing respects both clocks
  - Generate success timestamps, retry deadlines, and verification events under a
    virtual clock; compare next-upload decisions to `max(success + 600s, retry_at)` for
    at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 9: publish pacing respects both clocks`
  - _Requirements: 8.5, 8.6, 8.7_
  - DONE: `apply.rs`, with the pending-window run of the real script in `scripts.rs`.

- [x] 23. Property test: Property 10 — credential noninterference
  - Generate arbitrary non-empty registry and release API credential bytes plus every
    report/error path; prove fake gateways never receive `GH_TOKEN` during the
    publish-and-parity invocation or a registry token during the release-note
    invocation, scan all output, and compare non-secret results across credentials for
    at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 10: credential noninterference`
  - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 10.9, 10.10, 10.11, 10.12_
  - DONE: `apply.rs` with the recording fake client.

- [x] 24. Property test: Property 11 — Artifact Parity is three-way equality
  - Generate local/download bytes and registry checksum values; assert admission exactly
    matches three-way SHA-256 equality and notes stay immutable otherwise for at least
    256 cases.
  - Tag: `// Feature: release-engineering, Property 11: Artifact Parity is three-way equality`
  - _Requirements: 8.4, 9.1, 9.2, 9.3, 9.4, 9.5, 9.7_
  - DONE: `notes.rs`, plus the mismatch run of the real script in `scripts.rs`.

- [x] 25. Property test: Property 12 — release notes are deterministic and changelog-authored
  - Generate version bodies and verified package inventories; compare exact bytes,
    ordering, minimum-Rust annotation, and README URLs for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 12: release notes are deterministic and changelog-authored`
  - _Requirements: 10.1, 10.2, 10.3, 10.7_
  - DONE: `notes.rs`.

- [x] 26. Property test: Property 13 — partial-train state classification and resume
  - Generate phase outcome sequences, atomic branch/tag update outcomes, and observed
    remote branch/tag object pairs; compare state/resume decisions and terminal
    `GitRefConflict` diagnostics naming both ref values with the design state machine
    for at least 256 cases.
  - Tag: `// Feature: release-engineering, Property 13: partial-train state classification and resume`
  - _Requirements: 3.11, 10.4, 10.5, 10.6, 11.1, 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8, 11.9, 11.10_
  - DONE: `apply.rs` (classification and admission) and `scripts.rs` (the real push
    against a local remote, including the taken-tag rollback).

- [ ] 27. Property test: Property 14 — cross-repository tool bootstrap isolation
  - Generate Odori manifest/lockfile bytes, tool revisions, bootstrap outcomes, and
    modeled version rewrites; assert isolation and revision fencing for at least 256
    cases.
  - Tag: `// Feature: release-engineering, Property 14: cross-repository tool bootstrap isolation`
  - _Requirements: 12.2, 12.3, 12.4, 12.6, 12.7, 12.8_

- [ ] 28. Add end-to-end offline release-train integration tests
  - [x] 28.1 Exercise a fresh train and complete verify pass
    - Use fake Dagger, Git, registry, and release gateways while executing the real CLI,
      planner, preparer, both executor invocations, report, and note-generation layers;
      assert atomic ref publication, one multi-package packaging invocation, and the
      structural credential boundary.
    - _Requirements: 1.1, 2.1, 3.1, 3.5, 3.8, 3.9, 3.11, 3.13, 8.3, 9.3, 10.4, 10.9, 10.11, 10.12, 11.9_
    - DONE at the gateway layer: `scripts.rs` runs the real preparation, push, and
      registry scripts against a local remote and stub tools; `apply.rs` proves the
      credential boundary with the recording fake client; the executed argv helpers are
      pinned by example tests. The CLI handler is exercised by its own unit tests.
  - [x] 28.2 Exercise every resumable partial state
    - Cover timeout-after-upload, all-existing rerun, subset-published resume,
      note-only resume, and missing-token all-existing verification.
    - _Requirements: 7.7, 8.4, 8.8, 8.9, 11.1, 11.2, 11.3, 11.4, 11.7_
    - DONE at the gateway layer: inconclusive-then-visible, all-existing, pending, and
      resume admission run in `scripts.rs`; note-only resume is the `existing release`
      path in the CLI handler.
  - [x] 28.3 Exercise every terminal conflict
    - Cover dirty source, Plan drift, tag conflict, atomic-push refusal, divergent
      remote branch/tag object IDs, artifact mismatch, release conflict, invalid tool
      asset, and unsupported tool platform.
    - _Requirements: 2.4, 2.8, 3.11, 3.12, 5.5, 5.7, 9.4, 10.6, 11.5, 11.8, 11.10_
    - DONE at the gateway layer: taken tag, divergent refs, mismatch, and conclusive
      refusal run in `scripts.rs`; drift and decline in `apply.rs`; tool refusals in
      `changie.rs`.
  - [ ] 28.4 Exercise both repository workspace shapes
    - Prove byte-equivalent changie config, identical command arguments, preserved Odori
      dependency membership, and external dependency preflight.
    - _Requirements: 4.1, 4.9, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8_

- [x] 29. Final checkpoint: both repositories satisfy their full bars
  - In Tokeira, run the root `AGENTS.md` §10.4 bar plus the offline Markdown link check.
  - In Odori, run its complete documented bar.
  - Confirm all release tests are offline, use virtual clocks, and contain no token
    value or hosted-workflow fixture.
  - Confirm `git diff --exit-code` after checks and confirm no unexpected dependency or
    lockfile movement.
  - DONE for Tokeira: the §10.4 bar is green on the devbox and the spec files pass the
    offline link check; every release test is offline, the scripts' clock is stubbed,
    and no lockfile moved. The Odori bar belongs to the Odori slice.

## Task Dependency Graph

```json
{
  "1": [],
  "2": ["1"],
  "3": ["1", "2"],
  "4": ["1", "2"],
  "5": ["1", "2", "4"],
  "6": ["4", "5"],
  "7": ["1", "2", "4"],
  "8": ["7"],
  "9": ["4", "8"],
  "10": ["8", "9"],
  "11": ["2", "4", "5", "7", "8", "9", "10"],
  "12": ["1", "11"],
  "13": ["11", "12"],
  "14": ["4"],
  "15": ["4"],
  "16": ["11"],
  "17": ["7"],
  "18": ["5"],
  "19": ["2"],
  "20": ["5"],
  "21": ["9"],
  "22": ["9"],
  "23": ["9", "11"],
  "24": ["10"],
  "25": ["10"],
  "26": ["10"],
  "27": ["12"],
  "28": ["13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27"],
  "29": ["28"]
}
```

## Notes

- Implementation begins only after this spec PR is approved.
- Tasks spanning both repositories must land as separately reviewable repository PRs;
  the Tokeira tool/config slice lands before the Odori pin/bootstrap slice.
- The operator owns the constitution edits in Task 1.4. Agents propose and test the
  exact line but do not land policy text without that owner action.
- No implementation task adds a GitHub Actions file, reads a real registry token, or
  performs a live publish.
- Live apply validation is a separately authorized operator action after offline and
  read-only verification are green.
