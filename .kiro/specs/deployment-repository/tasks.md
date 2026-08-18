# Deployment Repository — Implementation Plan

Ordering rule: mechanical restructuring first (each slice independently bar-green —
the passing bar *is* the behaviour-preservation proof Requirement 12.3 demands), then
platform seams, then the repository machinery, then wiring. Slices are PR boundaries;
no slice mixes restructuring noise with feature diffs.

## A. Restructuring (behaviour-preserving, mechanical)

- [x] 1. Rename `crates/tokeira-provisioner` → `crates/tokeira-deployment`
  - [x] 1.1 Move the directory; rename the package; update the workspace member list
        and `[workspace.dependencies]`; update imports in `tokeira-provisioner-cli`,
        `apps/tkr`, `crates/tokeira-build`. Per the audited classification, every
        module moves with the crate unchanged; the `ORCHESTRATED_LOCK_*_ENV`
        constants gain the wire-protocol comment recorded in the design.
        _Requirements: 12.3_
  - [x] 1.2 Retire the `catalog` module: delete the published-catalog types and
        `apps/tkr`'s published arm (`from_published`, `admit_locators`, the
        `PlatformSource::Published` variant and its tests); rename `apps/tkr`'s
        catalog surface toward platform-discovery vocabulary (`PlatformCatalog` →
        `PlatformDiscovery`, file rename included). Workspace-arm behaviour is
        byte-for-byte unchanged. _Requirements: 12.3; glossary (Platform Discovery)_
  - [x] 1.3 Checkpoint: full §10.4 bar green.
        DONE 2026-08-17: bar green on the workspace devbox (15m17s); Cargo.lock diff
        verified rename-only; tkr surface renamed to platform_discovery.rs
        (PlatformDiscovery/PlatformDiscoveryError, single-variant sources
        flattened at call sites).

- [x] 2. Rename `crates/tokeira-provisioner-cli` → `crates/tokeira-tkp`
  - [x] 2.1 Move the directory; rename the package; update the workspace member list;
        update `tokeira-build` composition (`PROVISIONER_CLI_PACKAGE`, generated
        manifest template, `bound_provisioner_main!` macro path, composition tests).
        _Requirements: 12.3_
  - [x] 2.2 Checkpoint: full bar green, including a composition round-trip test
        proving a bound provisioner still generates and compiles.
        DONE 2026-08-17: bar on the workspace devbox green except the pre-existing
        tokeira-runtime lane-span flake (passes isolated; chipped for its own
        fix); composition tests passed in this run.

- [x] 3. Migrate shell-resident deployment-domain modules into `tokeira-deployment`
  - [x] 3.1 Move `config_history.rs`, `lock.rs`, `marker.rs`, and `ConfigSource`
        (from the shell's `lib.rs`) into `tokeira-deployment`; tests move with them;
        shell re-imports. No behaviour, digest, on-disk format, or report changes.
        _Requirements: 12.1, 12.3_
  - [x] 3.2 Checkpoint: full bar green.
        DONE 2026-08-17 (same bar as 2.2): ConfigSource lives in
        deployment.rs (recorded() public, anyhow-flavoured definition()
        dropped — test-only caller rebuilt on explicit constructors). Two
        stated transitional deviations, commented in place and assigned to
        the operation-lease spec: anyhow/tokio/tokeira-explain deps in the
        domain crate, and lock.rs's module-level print_stderr allow — R12.3
        byte-identical reports outweigh §1's letter for a pure move.

- [x] 4. Collapse `tokeira-tkd` + `tokeira-tkdp` → `crates/tokeira-platform-definition`
  - [x] 4.1 Create the crate with `tkd`/`tkdp` as feature-gated modules; the Monty/ruff
        dependency train sits behind the `tkdp` feature only. Both frontends' sources,
        tests, and READMEs move unchanged. _Requirements: 12.3_
  - [x] 4.2 Make `[package.metadata.tokeira.definition-frontend]` multi-format; teach
        `tokeira-build` discovery to read multi-format frontend packages and
        composition to select the frontend by feature instead of by package
        (generated manifest gains `features = ["<format>"]`). _Requirements: 12.3_
  - [x] 4.3 Update `platforms/{compose,ecs,eks}` frontend dependencies; remove the two
        old crates from the workspace. _Requirements: 12.3_
  - [x] 4.4 Checkpoint: full bar green; a `tkd`-only bound composition build compiles
        without the `tkdp` feature's dependency tree (assert via `cargo tree` in a
        test or a build assertion).
        DONE 2026-08-18: bar green on the workspace devbox in its NEW seven-step
        form — this slice also moved §10.4's test step to nextest (one process
        per test) after root-causing the parallel-load flakes to a
        tracing-core callsite-registration race that test code cannot
        deterministically win; diagnosis + adopted contract recorded in the
        tracing-span-lifecycle-hygiene spec, production hardening in
        tokeira-observability's installer. tkd-only dependency exclusion
        verified: cargo tree -e normal shows zero monty/ruff without the
        tkdp feature, five with it. nextest stress 5x on the previously
        flaking crates: 578/578 every iteration.

## B. Platform seams

- [x] 5. Expose the served set and the identity recomputation
  - [x] 5.1 `EvaluatedDefinition` gains `served_companions: Vec<(String, Arc<[u8]>)>`;
        `ConfigurationIdentity::compute_set` becomes public. Digest layouts unchanged.
        _Requirements: 1.5, 10.1, 10.3_
  - [x] 5.2 PBT: for any generated (format, root, companions), `compute_set` over the
        recorded served set equals the identity `evaluate_definition` computed; golden
        vectors (independently computed, carried from the spike) pin both layouts.
        // Feature: deployment-repository, Property P2 (identity layer)
        _Requirements: 10.2, 10.3_

- [x] 6. Emit identity + companions from the check path
  - [x] 6.1 `tokeira-tkp` `CheckReport` gains `identity` and `companions` (populated,
        not dropped, from the evaluated definition; serialized under `--json`).
        _Requirements: 1.4, 2 (evidence)_
  - [x] 6.2 Unit tests: report shape, single-document (no companions) case.
        _Requirements: 10.3_
        DONE 2026-08-18 (slice B): served_companions on EvaluatedDefinition
        (first-request order, exactly the identity's coverage); compute_set
        public with layout goldens (independent vectors) + the P2 identity
        PBT; ExecutionState threads identity+companions so deployment-mode
        check shares one evaluation; CheckReport emits identity/companions
        (absent-not-null on failure; empty-present for single documents) in
        --json and as narrative facts.

## C. Repository machinery (`tokeira-deployment`, offline-tested)

- [x] 7. Dependencies and skeleton
  - [x] 7.1 Add `tough`, `tough-kms`, `aws-sdk-kms`, `aws-lc-rs`, `jiff` (workspace
        deps; sanctioned by this spec); create the new modules
        (`locator`, `config`, `keys`, `claim`, `publish`, `writer`, `transport`,
        `open`, `fetch`, `list`, `error`) with docs. _Requirements: intro (crates)_
        DONE 2026-08-18 (slice C): all five workspace deps added; the lock
        movement bumped the aws-smithy stack, and the test-util dev-dep's
        feature unification (`serde_json/preserve_order`) grows
        `serde_json::Value` in `--all-targets` builds — accommodated with a
        commented `allow(large_enum_variant)` on `tokeira-iac`'s
        `DescribeResult` rather than a Box API break.

- [x] 8. Locator, config, keys
  - [x] 8.1 `RepositoryLocator`, `RepositoryConfig`, `RoleKeyConfig`,
        `KeySourceConfig` (`deny_unknown_fields` throughout); key-source construction
        (local Ed25519 files via `SharedKeySource`; KMS RSA with the constraint named
        in errors); local keygen under the deployments root.
        _Requirements: 8.1, 8.2, 8.4, 8.5_
  - [x] 8.2 Unit tests: serde round-trips, unknown-field rejection, KMS construction
        (not called), key generation + reload. _Requirements: 8.1–8.5_

- [x] 9. Claim
  - [x] 9.1 `DeploymentClaim` + sections + `Transition`; custom-metadata keys; serde
        shape tests including `build_authority` surfacing. _Requirements: claim table,
        1.2 (recorded tier)_

- [x] 10. Transport + testkit
  - [x] 10.1 `S3Transport` (productionized from the spike); `testkit` module with the
        in-memory S3 endpoint (closure-backed HTTP client) promoted from the spike.
        _Requirements: 6.1, 6.3_
  - [x] 10.2 PBT: absence signal — for any absent key, `FileNotFound`; for any other
        injected failure, never `FileNotFound`; non-s3 schemes refused.
        // Feature: deployment-repository, Property P5 _Requirements: 6.2_

- [x] 11. Writer
  - [x] 11.1 `RepositoryWriter` + local (create_new/rename) and S3 (`If-None-Match:*`)
        implementations; streaming `WriteSource`; byte-verify on collision.
        _Requirements: 6.4, 3.3_
        DONE 2026-08-18 (slice C): the S3 home reads one object into memory
        per write (the collision path needs the bytes for verification
        anyway) — a publication's memory bound is one artifact at a time,
        never the whole bundle.
  - [x] 11.2 PBT: create-only immutability — shared content `AlreadyPresent`; any
        differing same-key write refused naming the key; mutable heads unaffected.
        // Feature: deployment-repository, Property P4 _Requirements: 3.3, 3.6_

- [x] 12. Publish
  - [x] 12.1 Root authoring (v1 + rotation); `PublicationInput`; `publish_transition`
        with `expected_version`; manifest published verbatim; mutable heads written last.
        _Requirements: 2.1, 2.5, 3.1, 3.2, 3.4, 3.5, 7 (authoring), 8.3_
  - [x] 12.2 PBT: monotonic lineage — versions advance by 1 across generated
        transition sequences; revert publishes new-version-old-content; stale
        `expected_version` refused. // Feature: deployment-repository, Property P3
        _Requirements: 3.2, 4.1, 4.3_
  - [x] 12.3 PBT: home equivalence — identical inventories/claims/digests across
        local and in-memory-S3 homes. // Feature: deployment-repository, Property P11
        _Requirements: 1.6, 4.5_
        DONE 2026-08-18 (slice C): the testkit endpoint additionally answers
        ListObjectsV2 (common prefixes) so home equivalence covers listing.

- [x] 13. Open + verify
  - [x] 13.1 `open()` (trust anchor, datastore, expiration enforcement, root walk +
        re-pin); `verified_publication()` (exactly-one-claim, claim/target agreement,
        identity recomputation via `tokeira-platform`, engine manifest cross-check);
        typed `Refusal` set per the error table. _Requirements: 5.1, 7.1–7.4, 9.1,
        9.2, 10.1, 10.2, 11.4_
  - [x] 13.2 PBT: tamper refusal — any single-byte mutation of any object refuses.
        // Feature: deployment-repository, Property P6 _Requirements: 5.4, 10.2_
  - [x] 13.3 PBT: identity agreement — permuted companion order / mutated bytes /
        renamed companions refuse with the specific refusal.
        // Feature: deployment-repository, Property P2 _Requirements: 10.2_
  - [x] 13.4 PBT: freshness + rollback — expired refuses under Safe, loads under the
        break-glass; datastore-known newer version refuses older.
        // Feature: deployment-repository, Property P7 _Requirements: 9.1, 9.2_
  - [x] 13.5 PBT: engine agreement — descriptor sha vs TUF hash divergence refuses;
        the derived `tkp-<triple>` target must exist. // Feature: deployment-repository,
        Property P8 _Requirements: 2.5_
  - [x] 13.6 PBT: rotation — online-key rotation via root N+1 verifies from anchor N
        and re-pins. // Feature: deployment-repository, Property P9
        _Requirements: 7.2, 7.3_

- [x] 14. Fetch planning + listing
  - [x] 14.1 `MaterializePlan` (placements + host-target engine selection with
        refusal); `list_deployments` for both scopes. _Requirements: 5.2, 11.1_
  - [x] 14.2 PBT: round-trip — publish then plan materializes every published file
        byte-identically, host artifact selected. // Feature: deployment-repository,
        Property P1 _Requirements: 2.2, 5.2_
  - [x] 14.3 PBT: commit authority — injected publication failure leaves committed
        input re-publishable to identical content. // Feature: deployment-repository,
        Property P10 _Requirements: 2.4, 4.2_
        DONE 2026-08-18 (slice C): P10 sharpened the repair contract — a torn
        version's metadata is immutable, so a re-publish at the stale
        expected version refuses with a conflict NAMING the torn version;
        retrying with that version as `expected_version` completes the
        publication at the next version with all content deduping
        `AlreadyPresent`. The `publication_conflict` remedy text states
        exactly this.
  - [x] 14.4 Checkpoint: `tokeira-deployment` suite green offline (no AWS, no Dagger).

## D. Wiring

- [x] 15. `tkr deployment create` publishes
  - [x] 15.1 Bundle path becomes the default engine obtainment; `--dev-engine`
        (settled name) local-only, synthesizing the dev-tier single-target bundle
        manifest, tier recorded. _Requirements: 1.1, 1.2_
  - [x] 15.2 Create flow: keygen/collect keys, write `publisher.json`, evaluate via
        extended check report, assemble `PublicationInput` (documents from staging,
        bundle from CAS), birth publication after local commit, trust pin + datastore
        init before rename, `deployment_repository` in `metadata.json`; publication
        failure → created-with-pending report. _Requirements: 1.3, 1.4, 2.1–2.4, 11.2_
        DONE 2026-08-18 (slice D): the trust anchor is authored pre-rename
        via the new `author_trust_anchor` (publish accepts it at
        expected_version 0), so staging carries keys/publisher.json/anchor/
        datastore/binding before the atomic rename and only the upload
        follows the commit. The extended check report arrives over the
        process boundary: `validate_staged_definition` captures the staged
        engine's own `--json` check (identity + companions) — `tkr` never
        recomputes identity. Claim + input assembly live in
        `repository::assemble`, shared by every publisher. Local keys under
        `<root>/.repository-keys/<name>/` (outside the dir, so
        publisher.json paths survive the rename).
  - [x] 15.3 Integration tests with `tokeira-build` fake bundles (no Dagger): local
        create → local repository; publication content asserted. _Requirements: 2.2_
        DONE 2026-08-18 (slice D): `tokeira-build`'s mock-Dagger testkit
        promoted behind a `testing` feature; the test drives the real
        obtain pipeline → marry → provision → rename → birth publication →
        verified open, asserting claim fields and byte-identity for every
        published file.

- [x] 16. `tkp` lifecycle publications
  - [x] 16.1 Post-commit hook in apply/upgrade/revert (after `config_history`
        snapshot, when `publisher.json` exists): assemble input from the committed
        dir, `publish_transition`; failure reports pending + remedy.
        _Requirements: 4.1–4.5, 12.1, 12.2_
  - [x] 16.2 Integration: apply/revert sequences against a local repository —
        transitions and `config_revision` asserted in claims; revert content equals
        reverted-to publication. _Requirements: 4.3_
        DONE 2026-08-18 (slice D): the hook derives identity + companions
        from the engine's own evaluation, reads the current version from
        the repository itself (never mutable local state), and is
        infallible by contract — every failure is the pending report. The
        integration test drives init → birth → apply → apply → revert over
        the stub engine: versions 1→4, final claim `revert`/revision 3,
        published root equals the reverted-to document.

- [x] 17. `tkr` repository verbs
  - [x] 17.1 `fetch` (materialize via plan inside atomic staging, tkp placement +
        sidecar, trust pin, datastore, metadata binding; refusal before any byte).
        _Requirements: 5.1–5.5_
  - [x] 17.2 `list`, `publish` (repair), `refresh`, `inspect` — reports, `--json`,
        typed refusals, §4 confirmation on writes. _Requirements: 9.3, 11.1–11.4_
        DONE 2026-08-18 (slice D): `refresh` gained its library operation —
        timestamp.json is the only rewritten object; a snapshot whose
        expiry falls inside the new freshness window is re-signed as a NEW
        versioned create-only object over the same targets (the
        publication-conflict repair contract absorbs the later version
        collision). Fetch re-pins the caller's own anchor bytes when no
        root walk happened, keeping binding digests stable across seats.
  - [x] 17.3 Integration: create → fetch onto a second deployment-dir root →
        byte-identical published files; post-fetch `tkp describe` admission green.
        _Requirements: 2.2, 5.5, 12.4_
        DONE 2026-08-18 (slice D): byte-identity holds for every
        materialized file, the manifest sidecar included — publish carries
        it verbatim, `retrieval_ref` constant (Ian's ruling; Req 2.5
        amended same day). Describe admission runs against a fetched
        dev-engine deployment in the generated-provisioner integration
        test.

- [x] 18. Retire the spike and close out
  - [x] 18.1 Remove `spikes/tuf-platform-definition` + the workspace exclude entry;
        disposition map in the PR (golden vectors → task 5.2 tests; transport/testkit
        → tasks 10; README findings → crate docs). _Requirements: 10.1_
  - [x] 18.2 Docs: `docs/agents/engineering-reference.md` package boundaries,
        AGENTS.md workspace map line (provisioner{,-cli} → deployment/tkp;
        tkd/tkdp → platform-definition), crate-level docs. Shared-file edits are
        named here deliberately (§10.3). _Requirements: intro (crates)_
        DONE 2026-08-18 (slice D): engineering-reference gains the
        repository boundary contract (derived projection, envelope/rename
        authority, one assembly implementation); the AGENTS.md workspace
        map already carried the slice-A renames — verified current, no
        edit needed.
  - [x] 18.3 Final checkpoint: full §10.4 bar; lychee-clean docs.
        DONE 2026-08-18 (slice D): bar green on the workspace devbox —
        fmt, lint, check, nextest 2887/2887, doctests, doc -D warnings
        (6m50s); doc-links (lychee) gate runs in CI on the PR. Includes
        the same-day retrieval_ref ruling: manifest published verbatim,
        Req 2.5/P8 amended, byte-identity across create and fetch now
        exceptionless.

## Task Dependency Graph

```
1 → 2 → 3 ─┬─────────────→ 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 ─┬→ 15 → 16 → 17 → 18
4 ──────────┘                                                    │
5 → 6 ──────────────────────────────(13.1 identity recompute)────┘
```

Slices A (1–4), B (5–6), C (7–14), D (15–18) are PR boundaries; A's internal order is
1 → 2 → 3 with 4 parallel after 1.

## Notes

- Run the bar remotely: `tkw devbox bar --box <name>` (or `TKW_DEVBOX`) from the worktree; fmt runs
  locally before push (remote bar checks with `--check`).
- Restructuring slices carry no functional diffs; if one needs a behavioural change,
  stop — that's a defect in the plan.
- The dev-engine option is named: `--dev-engine` (settled 2026-08-17).
- The operation-lease spec follows immediately after slice D lands; `config_history`,
  `lock`, `marker` are reworked there, in their new home.
