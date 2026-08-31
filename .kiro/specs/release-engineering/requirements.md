# Requirements Document: Release Engineering

## Introduction

This spec owns release engineering for the `tokeira/tokeira` and
`tokeira/tokeira-odori` Rust workspaces. It defines two connected surfaces:

1. `tkr release`, the operator-driven, Dagger-executed crates.io release train.
2. changie, the conflict-resistant fragment system that supplies the train's changelog
   and release notes.

The contract is workspace-generic: the same `tkr release` invocation operates on an
explicit workspace root, discovers that workspace's publishable Cargo graph, and moves
all publishable members through one version train. The two repositories run separate
trains; they share the command contract and byte-equivalent changie configuration.

Publishing is an outward-facing, irreversible mutation. The command therefore follows
the repository's `plan -> confirm -> apply` rule (root `AGENTS.md` §4). The Dagger
pipeline is the only release-train executor. No part of this feature uses a GitHub
Actions workflow, runner, or issuer.

The authoritative external contracts are:

- Cargo's [`publish`](https://doc.rust-lang.org/cargo/commands/cargo-publish.html),
  [`package`](https://doc.rust-lang.org/cargo/commands/cargo-package.html), and
  [registry authentication](https://doc.rust-lang.org/cargo/reference/registry-authentication.html)
  documentation.
- changie `v1.25.2` at commit
  `8406ffac34697bd95d153550d0423e403fac9a90`, especially its
  [`new`](https://changie.dev/cli/changie_new/),
  [`batch`](https://changie.dev/cli/changie_batch/),
  [`merge`](https://changie.dev/cli/changie_merge/), and
  [configuration](https://changie.dev/config/) contracts.
- GitHub CLI's
  [`gh release create`](https://cli.github.com/manual/gh_release_create) contract,
  including `--verify-tag` and `--notes-file`.

## Glossary

- **Apply** — `tkr release apply`, the confirmed command that prepares the release
  commit and tag, executes the publish train, verifies parity, and creates release
  notes.
- **Artifact Parity** — equality between the SHA-256 of a `.crate` produced
  hermetically from the release tag and the bytes downloaded from crates.io after
  publication.
- **Batched Changelog** — the version file produced from unreleased fragments by
  `changie batch`, merged into root `CHANGELOG.md` by `changie merge`.
- **Dagger Executor** — the only environment in which release preparation, package
  building, publication, parity verification, and release-note creation execute.
- **Existing Package** — a package/version already present on crates.io when a plan or
  apply pass observes it.
- **Fragment** — one changie YAML document under `.changes/unreleased/` declaring the
  user-facing change for one coherent change slice, or explicitly declaring that the
  slice is internal.
- **Hermetic Tag Build** — packaging from the exact release tag in the pinned Dagger
  toolchain, without using the host working tree or host build artifacts.
- **Internal Fragment** — a valid fragment with changie kind `internal`; it satisfies
  the declaration gate but emits no line or heading into release notes.
- **Plan** — a serializable, secret-free description of the exact release inputs,
  package order, observed external state, intended effects, and content digest.
- **Publishable Package** — a Cargo workspace member whose manifest permits
  publication to crates.io.
- **Release Commit** — the commit containing the unified version update, batched
  changelog, generated version file, and deleted consumed fragments.
- **Release Config** — root `.tokeira-release.toml`, the small checked repository-local
  contract for the release branch, non-Cargo version fields, and (in Odori only) the
  pinned external `tkr` source.
- **Release Report** — the secret-free machine-readable outcome of plan, apply, or
  verify.
- **Release Tag** — the annotated `v<version>` tag pointing to the Release Commit.
- **Release Train** — one workspace's ordered progression through version preparation,
  changelog batching, tagging, hermetic packaging, crates.io publication, Artifact
  Parity, and release-note creation.
- **Skip Existing** — resume behavior that never uploads an already-present
  package/version and instead admits it only after Artifact Parity succeeds.
- **Train Identity** — repository identity, base commit, target version, and Plan digest
  carried by the Release Commit and Release Tag.
- **Unified Version** — the single SemVer value shared by every Publishable Package in
  one workspace for one train.

## Target State

The following becomes supported:

- Both repositories carry the same changie configuration, root `CHANGELOG.md`, and
  `.changes/unreleased/` fragment convention.
- `tkr release` exposes the exact sub-verbs `fragment`, `plan`, `apply`, and `verify`.
- `tkr ci check` enforces fragment declarations and packaging dry-runs as standing
  checks; it does not publish, tag, or create release notes.
- `tkr release apply` walks the Publishable Package dependency DAG in deterministic
  order, publishes one package at a time with conservative pacing, verifies registry
  bytes, and creates release notes from the Batched Changelog.
- Re-running the same train after a partial failure is safe: Existing Packages are
  verified and skipped, incomplete work resumes, and conflicting external state is
  refused.
- The crates.io token remains in operator-side custody, crosses into the executor only
  as a Dagger secret, and is absent from all persisted state and output.
- Tokeira Odori obtains `tkr` from a full pinned Tokeira Git revision outside its Cargo
  dependency graph, then invokes the same command against the Odori workspace root.

The following stays out of scope:

- Trusted Publishing or any OIDC exchange. crates.io's current Trusted Publishing
  support is limited to a CI issuer this project does not use; token mode is the
  current contract. The dormant policy watch remains outside these repositories.
- Token minting, scope selection, rotation, revocation, or expiry procedure. Those are
  operator responsibilities; this spec owns only the invocation boundary.
- Registry targets other than crates.io.
- Pre-release, nightly, channel, or multi-version workspace trains.
- GitHub-generated release notes. The Batched Changelog is the release-note authority.
- Any hosted workflow or scheduled release runner.
- Retrofitting release behavior into `tkr ci`; release mutations live only under
  `tkr release`.

## Evidence From Current Code

- `apps/tkr/src/cli.rs` is the authoritative clap command tree and documents the
  confirmation convention for mutating subcommands.
- `apps/tkr/src/commands/ci/mod.rs` resolves the current workspace and runs frozen,
  in-process Dagger sessions without a hosted runner.
- `crates/tokeira-build/src/pipelines/ci.rs` owns `CiCheck`, the reusable check report,
  and the complete workspace bar.
- `crates/tokeira-build/src/dagger_release.rs` is the existing single-pin pattern for a
  checksum-verified external tool release shared between pipeline and local command
  surfaces.
- `Cargo.toml` declares workspace Rust `1.97`, while the 17 currently Publishable
  Packages inherit one workspace version and fence non-publishable members explicitly.
- `tokeira/tokeira-odori:Cargo.toml @ 03a46d3` declares five Publishable Packages with
  one workspace version and centralizes their internal dependency requirements under
  `[workspace.dependencies]`.
- `tokeira/tokeira-odori:AGENTS.md @ 03a46d3` forbids unrequested dependency movement;
  the release implementation must not invoke `cargo add` or `cargo remove` because
  those commands can prune unreferenced workspace dependency entries.
- The crates.io version records for the current 17-package closure expose package
  checksums, `rust_version = 1.97`, and registry README endpoints. Their publication
  timestamps also provide the evidence for the conservative ten-minute upload
  cooldown used by this contract.
- Cargo documents that publication is permanent, a version cannot be overwritten, and
  a publish timeout can occur after a successful upload. Those facts require Skip
  Existing and verification-before-retry rather than blind retry.
- The changie `v1.25.2` release publishes checksum-bearing archives for the supported
  macOS and Linux host pairs. Its tagged `cmd/new.go`, `core/prompt.go`,
  `core/config.go`, and `cmd/batch.go` sources verify custom-value injection,
  dry-run batching, explicit kinds, non-empty per-kind format overrides, and
  no-change refusal; an empty per-kind override falls back to the root format.
- GitHub CLI documents that `gh release create --verify-tag` refuses an absent remote
  tag and `--notes-file` supplies deterministic caller-authored notes.

## Contract Policy

### `tkr release fragment`

| Input | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `--workspace-root <path>` | Optional; defaults to the Cargo workspace containing the current directory | `WorkspaceNotFound` if no root manifest is resolved | Reads repository config; writes one fragment only after validation |
| `--kind <kind>` | Optional in an interactive terminal; required in non-interactive mode | `InvalidFragmentKind` for a value outside the configured kind set | Stored as the fragment `kind` |
| `--body <text>` | Optional in an interactive terminal; required for every non-`internal` kind in non-interactive mode | `InvalidFragmentBody` when required, empty, or outside configured bounds | Stored as the fragment `body`; never copied into commit messages automatically |
| generated Slice ID | No CLI input; `tkr` creates a lowercase UUID version 4 for every invocation | `FragmentIdentityFailed` if generation or pinned-changie validation fails | Passed as changie's `Slice` custom value and stored in the collision-resistant fragment filename |

### `tkr release plan`

| Input | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `--workspace-root <path>` | Optional; resolves exactly one Cargo workspace | `WorkspaceNotFound` or `AmbiguousWorkspace` | Read-only |
| `--version <semver>` | Required stable SemVer greater than the current Unified Version | `InvalidTargetVersion` | Included in Plan and Train Identity |
| `--base-ref <git-ref>` | Optional; defaults to the configured release branch's upstream tip | `InvalidBaseRef` or `StaleWorkspace` | Read-only Git comparison |
| `--output <path>` | Optional; stdout when absent, atomically written JSON when present | `PlanOutputFailed` | Writes only the named Plan file, never inside the workspace implicitly |

### `tkr release apply`

| Input | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `--workspace-root <path>` | Required when the Plan path does not resolve it unambiguously | `WorkspaceMismatch` | Selects the only workspace allowed to mutate |
| `--plan <path>` | Required secret-free JSON produced by `plan` | `InvalidPlan` or `PlanDrift` | Read-only input; digest enters Release Commit and Release Tag |
| `--token-env <name>` | Required only while at least one package still needs upload; names an environment variable and never accepts the token value | `RegistryCredentialMissing` | Value crosses only as an executor secret; name may appear in diagnostics |
| `GH_TOKEN` environment | Required only while the matching release object is absent; fixed environment name used by `gh` | `ReleaseCredentialMissing` | Value crosses only as an executor secret and is never serialized |
| `--yes` | Optional in an interactive terminal; required in non-interactive mode | `ConfirmationRequired` | Authorizes the exact recomputed Plan effects |

### `tkr release verify`

| Input | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `--workspace-root <path>` | Optional; resolves exactly one Cargo workspace | `WorkspaceNotFound` | Read-only |
| `--version <semver>` | Required stable released or partially released version | `ReleaseNotFound` | Selects Release Tag, package set, and expected notes |
| `--output <path>` | Optional; stdout when absent | `ReportOutputFailed` | Writes only the named secret-free report |

### Plan fields

| Field | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `schema_version` | Exact supported Plan schema | `UnsupportedPlanSchema` | Serialized |
| `repository` | Canonical repository identity derived from Git remote metadata | `RepositoryMismatch` | Serialized; no credentials or remote URL user-info |
| `workspace_root` | Canonical path used only for local admission | `WorkspaceMismatch` | Serialized locally; omitted from committed trailers and release notes |
| `base_commit` | Full Git object ID of the clean source base | `StaleWorkspace` | Serialized and carried by Train Identity |
| `target_version` / `tag` | Stable SemVer plus exact `v`-prefixed tag | `InvalidTargetVersion` | Serialized and externally visible |
| `packages` | Every Publishable Package, target version, manifest path relative to root, publishable dependencies, current registry state, and deterministic order | `InvalidPublishGraph` | Serialized; drives packaging and publication |
| `fragments` | Relative path and SHA-256 of every admitted unreleased fragment | `InvalidFragment` | Serialized; no fragment body duplication outside its file |
| `changelog_config_sha256` | Digest of the canonical changie config set | `ChangelogConfigDrift` | Serialized |
| `changie_release` | Exact version, source revision, platform asset, and asset SHA-256 | `ToolPinDrift` | Serialized |
| `toolchain` | Pinned Rust and Dagger identities used by checks and build | `ToolchainDrift` | Serialized |
| `release_notes_sha256` | Digest of the deterministic preview notes | `ReleaseNotesDrift` | Serialized |
| `effects` | Ordered human-readable outward effects, including Git, registry, and release API mutations | `InvalidPlan` | Rendered before confirmation |
| `digest` | SHA-256 of canonical Plan content excluding `workspace_root`, observations that may advance monotonically, and the digest field itself | `PlanDrift` | Carried by commit trailer, tag annotation, and Release Report |

### Root `.tokeira-release.toml`

| Field | Target policy | Error if invalid | Persistence / side effect |
|---|---|---|---|
| `schema_version` | Required integer `1` | `UnsupportedReleaseConfig` | Selects strict deserialization rules |
| `release_branch` | Required branch name whose upstream tip is the default Plan base | `InvalidReleaseBranch` | Read-only Git admission input |
| `extra_version_fields` | Optional array of workspace-relative TOML path plus exact scalar key path; no regex or arbitrary command | `InvalidVersionField` if the file/key is absent, non-string, outside the workspace, or matches the workspace version zero/multiple times | Each admitted scalar moves from current to target Unified Version |
| `tkr.repository` | Odori-only HTTPS Git source for the Tokeira repository; forbidden in Tokeira's in-tree config | `InvalidToolSource` | Read only by the Odori bootstrap |
| `tkr.revision` | Odori-only full 40-hex commit corresponding to `tkr.repository` | `InvalidToolRevision` | Selects the external source build; never enters Cargo manifests |

## Requirements

### Requirement 1: Workspace-generic release command surface

**User Story:** As a release operator, I want one small command surface for both Rust
workspaces, so that repository-specific release scripts cannot diverge.

#### Acceptance Criteria

1. THE `tkr release` command group SHALL expose exactly the `fragment`, `plan`,
   `apply`, and `verify` sub-verbs.
2. WHEN `--workspace-root` is omitted, THE release command SHALL resolve the Cargo
   workspace containing the current directory.
3. WHEN `--workspace-root` is present, THE release command SHALL operate only on the
   canonical workspace rooted at that path.
4. IF the selected path does not resolve exactly one Cargo workspace, THEN THE release
   command SHALL return a workspace admission error without mutation.
5. WHEN `tkr release fragment` succeeds, THE command SHALL create exactly one valid
   file under the selected workspace's `.changes/unreleased/` directory.
6. THE `tkr release plan` command SHALL be read-only except for an explicitly named
   Plan output path.
7. THE `tkr release verify` command SHALL be read-only except for an explicitly named
   report output path.
8. THE `tkr ci` command group SHALL contain no tag, publish, parity, or release-note
   mutation.

### Requirement 2: Secret-free planning and explicit confirmation

**User Story:** As a release operator, I want to review an exact train before any
outward effect occurs, so that an irreversible publish cannot result from stale or
implicit inputs.

#### Acceptance Criteria

1. WHEN `tkr release plan` succeeds, THE command SHALL emit every field in the Plan
   policy table.
2. IF the target is not stable SemVer greater than the current Unified Version, THEN
   THE planner SHALL return `InvalidTargetVersion`.
3. IF Publishable Packages do not share one current version, THEN THE planner SHALL
   return `NonUnifiedWorkspaceVersion`.
4. IF the source workspace contains uncommitted changes, THEN THE planner SHALL return
   `DirtyWorkspace`.
5. THE planner SHALL compute package order from the selected workspace rather than a
   repository-specific package list.
6. THE planner SHALL complete without resolving a crates.io credential.
7. WHEN `tkr release apply` starts, THE command SHALL recompute the Plan from current
   source and public external state before requesting confirmation.
8. IF immutable Plan inputs or the canonical Plan digest differ, THEN THE apply command
   SHALL return `PlanDrift` before mutation.
9. WHEN apply runs in an interactive terminal without `--yes`, THE command SHALL render
   the target repository, version, package count, existing package count, tag effect,
   publish effect, and release-note effect in its confirmation prompt.
10. IF apply runs non-interactively without `--yes`, THEN THE command SHALL return
    `ConfirmationRequired` before mutation.
11. WHEN an operator declines confirmation, THE apply command SHALL leave source, Git
    refs, registry state, and release state unchanged.
12. THE Plan and confirmation rendering SHALL contain no registry token value or
    token-derived value.

### Requirement 3: Unified source preparation and immutable tag identity

**User Story:** As a crate consumer, I want every package in one workspace train to come
from one versioned source identity, so that package versions, changelog, tag, and source
cannot disagree.

#### Acceptance Criteria

1. WHEN source preparation begins, THE release executor SHALL update every Publishable
   Package to the target Unified Version.
2. WHEN a publishable workspace dependency names another Publishable Package, THE
   release executor SHALL update its registry version requirement to the target Unified
   Version.
3. THE release executor SHALL preserve every existing workspace dependency entry that
   is not a version field owned by the train.
4. THE release executor SHALL never invoke `cargo add` or `cargo remove`.
5. WHEN version rewriting succeeds, THE release executor SHALL run changie batch for
   the explicit target version with no-change batching disabled.
6. WHEN batching succeeds, THE release executor SHALL merge the generated version file
   into root `CHANGELOG.md`.
7. WHEN the release source diff is committed, THE Release Commit SHALL contain a
   `Release-Plan-Digest: sha256:<digest>` trailer matching the admitted Plan.
8. WHEN the Release Tag is created, THE annotated tag SHALL point to the Release Commit
   and carry the same Train Identity.
9. WHEN the hermetic package build begins, THE Dagger Executor SHALL obtain source from
   the Release Tag rather than the host working tree.
10. IF source preparation or the hermetic build fails before the tag is pushed, THEN
    THE apply command SHALL leave no remote Git, registry, or release mutation.
11. WHEN the hermetic build succeeds, THE apply command SHALL push the exact Release
    Commit and annotated Release Tag before the first registry upload.
12. IF the remote Release Tag exists at a different object, THEN THE apply command
    SHALL return `TagConflict` before any registry upload.

### Requirement 4: Conflict-resistant changelog declarations

**User Story:** As a maintainer working with concurrent agents, I want each coherent
change slice to own a separate fragment, so that user-facing release notes accumulate
without shared-file conflicts.

#### Acceptance Criteria

1. THE two repositories SHALL carry byte-equivalent `.changie.yaml` and changie
   template files.
2. THE changie configuration SHALL store unreleased fragments under
   `.changes/unreleased/` and merged output at root `CHANGELOG.md`.
3. THE changie kind set SHALL be `added`, `changed`, `deprecated`, `removed`, `fixed`,
   `security`, and `internal` in that order.
4. WHEN a non-`internal` fragment is created, THE fragment command SHALL require one
   bounded user-facing sentence as its body.
5. WHEN an `internal` fragment is batched, THE pinned configuration SHALL render
   neither an `Internal` heading nor a change line for it.
6. WHEN `tkr ci check` evaluates a non-release diff from its admitted base, THE
   changelog-fragment check SHALL require at least one newly added valid fragment.
7. WHEN `tkr ci check` evaluates a release-preparation diff, THE changelog-fragment
   check SHALL admit only the exact changie batch-and-merge transition for the new
   workspace version.
8. IF any fragment fails pinned changie's dry-run batch validation, THEN THE
   changelog-fragment check SHALL fail with the fragment path and validation reason.
9. IF the selected repository's changie config-set digest differs from the canonical
   digest embedded in `tkr`, THEN THE release planner SHALL return
   `ChangelogConfigDrift`.
10. THE proposed repository constitution line SHALL be: “Every coherent change slice
    adds one `.changes/unreleased/` changie fragment; use the `internal` kind when no
    user-facing changelog entry is warranted.”
11. WHEN `tkr release fragment` creates a fragment, THE command SHALL generate a
    lowercase UUID version 4 `Slice` custom value and pass it to pinned changie for the
    fragment filename.

### Requirement 5: One checksum-verified changie release

**User Story:** As a maintainer, I want local authoring and release batching to use the
same verified changie binary, so that tool drift cannot change accepted fragments or
rendered notes.

#### Acceptance Criteria

1. THE single changie pin SHALL name version `1.25.2` and source revision
   `8406ffac34697bd95d153550d0423e403fac9a90`.
2. THE pin SHALL include the upstream SHA-256 for each supported macOS and Linux
   `x86_64` and `aarch64` archive.
3. WHEN a Dagger release step needs changie, THE executor SHALL obtain the pinned Linux
   archive matching the executor architecture.
4. WHEN local fragment authoring needs changie, THE `tkr` tool resolver SHALL place the
   verified binary in the platform cache directory keyed by version and asset digest.
5. WHEN changie execution is requested, THE tool resolver SHALL verify both the archive
   SHA-256 and reported binary version before starting the tool.
6. IF an ambient `changie` binary differs from the pin, THEN THE tool resolver SHALL
   ignore it.
7. IF no pinned asset supports the current host, THEN THE fragment command SHALL return
   `UnsupportedToolPlatform` with a remediation message.
8. THE release design SHALL use no changie GitHub Action or other hosted action.

### Requirement 6: Publishability is a standing CI invariant

**User Story:** As a crate maintainer, I want packaging failures detected in ordinary
CI, so that the release train does not discover that a crate has silently stopped being
publishable.

#### Acceptance Criteria

1. THE `CiCheck` registry SHALL include `ChangelogFragments` and `PackageDryRun` as
   first-class report-producing checks.
2. WHEN `PackageDryRun` runs, THE Dagger Executor SHALL discover every Publishable
   Package through Cargo metadata.
3. WHEN `PackageDryRun` runs, THE Dagger Executor SHALL execute Cargo's locked publish
   dry-run for each Publishable Package in dependency order.
4. IF a package archive contains an unresolved path-only normal or build dependency,
   THEN THE package dry-run SHALL fail with the package and dependency names.
5. IF a Publishable Package lacks a packaged README, license, repository metadata, or
   `rust-version`, THEN THE package dry-run SHALL fail with the package and missing
   field.
6. WHEN package verification compiles an archive, THE package dry-run SHALL use the
   workspace's pinned Rust toolchain.
7. THE package dry-run SHALL use `--locked` and leave the source workspace byte-identical.
8. WHEN the complete check report is serialized, THE new checks SHALL use the same
   `CiCheckResult` schema as the existing workspace bar.

### Requirement 7: Token mode with operator-side custody

**User Story:** As a release operator, I want a scoped token to exist only for the
publish process that needs it, so that release automation does not persist or disclose
registry authority.

#### Acceptance Criteria

1. THE release train SHALL use crates.io token authentication rather than Trusted
   Publishing.
2. THE apply CLI SHALL accept only the name of the environment variable holding the
   token.
3. WHEN at least one package needs upload, THE apply command SHALL resolve the named
   environment variable only after Plan validation and confirmation succeed.
4. IF a package needs upload and the named environment variable is absent, THEN THE
   apply command SHALL return `RegistryCredentialMissing` before remote mutation.
5. WHEN the token crosses into Dagger, THE release executor SHALL inject it as a secret
   environment value only into the publish process.
6. THE release executor SHALL never write the token to a file, credential store, Plan,
   report, command argument, log field, error chain, or release note.
7. WHEN every package is Existing, THE apply and verify commands SHALL complete without
   resolving a registry token.
8. THE release examples and fixtures SHALL use placeholders rather than token-shaped
   sample values.
9. THE release implementation SHALL contain no API for minting, rotating, or revoking
   registry tokens.

### Requirement 8: Dependency-ordered, paced, idempotent publication

**User Story:** As a release operator, I want a partially completed train to resume
without duplicate uploads or dependency races, so that a transient failure does not
force an unsafe manual script.

#### Acceptance Criteria

1. THE planner SHALL topologically order Publishable Packages by publishable workspace
   dependency edges with a lexical package-name tie break.
2. IF the publishable dependency graph is cyclic, THEN THE planner SHALL return
   `InvalidPublishGraph`.
3. WHEN a package/version is absent from crates.io, THE apply executor SHALL publish
   that package from the Hermetic Tag Build.
4. WHEN a package/version already exists on crates.io, THE apply executor SHALL skip
   its upload only after Artifact Parity succeeds.
5. THE apply executor SHALL allow at most one registry upload request in flight.
6. WHEN an upload acknowledgement succeeds, THE apply executor SHALL wait at least 600
   seconds before the next upload request.
7. IF crates.io supplies a longer retry interval, THEN THE apply executor SHALL honor
   that interval before another request.
8. WHEN Cargo times out while polling for a published package, THE apply executor SHALL
   inspect registry state before deciding whether an upload retry is necessary.
9. IF a registry response is ambiguous and the package remains absent after the bounded
   observation window, THEN THE apply executor SHALL stop the train with a resumable
   package state.
10. THE Release Report SHALL record each package as `published`, `existing-verified`,
    `pending`, or `failed` without carrying credential material.

### Requirement 9: Checksum parity is the integrity claim

**User Story:** As a crate consumer, I want the registry archive to match the release
tag's hermetic archive byte for byte, so that the public artifact is attributable to
the reviewed source identity.

#### Acceptance Criteria

1. WHEN the Hermetic Tag Build packages a crate, THE release executor SHALL record the
   local `.crate` SHA-256 in memory and in the secret-free Release Report.
2. WHEN crates.io exposes the package/version, THE release executor SHALL download the
   registry `.crate` bytes through the registry download endpoint.
3. THE parity verifier SHALL require equality among the hermetic artifact SHA-256, the
   downloaded artifact SHA-256, and the registry checksum metadata.
4. IF any checksum differs, THEN THE parity verifier SHALL return `ArtifactMismatch`
   and mark the train terminal for that version.
5. IF any package lacks verified parity, THEN THE apply command SHALL refuse to create
   or update release notes.
6. WHEN `tkr release verify` runs, THE command SHALL rebuild from the Release Tag and
   re-evaluate parity for every Publishable Package without a registry token.
7. THE parity verifier SHALL never treat matching package name and version as evidence
   of matching content.

### Requirement 10: Changelog-authored release notes and consumer facts

**User Story:** As a release consumer, I want release notes derived from the fragments
that shipped with the tag and annotated with package facts, so that the announcement is
both readable and mechanically attributable.

#### Acceptance Criteria

1. WHEN parity succeeds for every package, THE release executor SHALL derive the
   release-note body from the changie version file committed by the Release Tag.
2. THE release-note body SHALL state the workspace's minimum supported Rust version as
   “Rust 1.97 or newer” while that remains the workspace contract.
3. THE release-note body SHALL append a deterministically ordered table of every
   published package, version, SHA-256, crates.io page, and registry README URL.
4. WHEN no GitHub release exists for the tag, THE Dagger Executor SHALL run
   `gh release create` with the exact remote tag, `--verify-tag`, and a generated notes
   file.
5. IF a GitHub release already exists with the same tag and notes digest, THEN THE
   release executor SHALL treat note creation as an idempotent success.
6. IF a GitHub release already exists with different target or notes, THEN THE release
   executor SHALL return `ReleaseConflict` without editing it.
7. THE release executor SHALL use no auto-generated release-note mode.
8. THE release executor SHALL use no GitHub Actions workflow or action.
9. WHEN release-note creation needs authentication, THE apply command SHALL inject the
   fixed `GH_TOKEN` environment value into Dagger as a secret only after parity succeeds.
10. THE release executor SHALL omit the release API credential from every file, Plan,
    report, command argument, log field, error chain, and release note.

### Requirement 11: Explicit partial-train recovery

**User Story:** As a release operator, I want every failure to leave a classified,
inspectable train state, so that the safe resume action is clear and conflicting state
is never silently repaired.

#### Acceptance Criteria

1. WHEN plan observes a matching Release Tag, THE Plan SHALL classify each package and
   release-note phase from public external state.
2. WHEN apply resumes a Train Identity with a matching tag, THE executor SHALL use the
   immutable tagged source rather than repeat version or changelog mutation.
3. WHEN apply resumes after a subset of packages published, THE executor SHALL verify
   every Existing Package before continuing with the first pending DAG node.
4. WHEN apply resumes after parity completed but note creation failed, THE executor
   SHALL skip registry upload and retry only release-note creation.
5. IF an existing commit, tag, package, or release conflicts with Train Identity, THEN
   THE executor SHALL stop with the typed conflict for that surface.
6. IF failure occurs before the first package publish and no remote release exists,
   THEN THE executor SHALL classify the train as `pre-publication-failed`.
7. IF at least one package exists but the train is incomplete, THEN THE executor SHALL
   classify the train as `partially-published`.
8. IF Artifact Parity fails, THEN THE executor SHALL classify the train as
   `terminal-mismatch` and require a new corrective version.
9. WHEN every package has parity and the release notes match, THE executor SHALL
   classify the train as `complete`.

### Requirement 12: Tokeira Odori consumes `tkr` without dependency mutation

**User Story:** As the Odori release operator, I want to run the engine repository's
release tool against Odori without adding it to Odori's workspace graph, so that both
repositories share behavior without destabilizing Odori dependencies.

#### Acceptance Criteria

1. THE Tokeira repository SHALL run its in-tree binary with `cargo run --locked -p tkr
   -- release <sub-verb> --workspace-root <tokeira-root>`.
2. THE Odori repository SHALL pin one full Tokeira Git revision as its release-tool
   source outside `Cargo.toml` and `Cargo.lock`.
3. WHEN Odori needs the release tool, THE bootstrap command SHALL use `cargo install
   --locked --git <tokeira-repository> --rev <full-revision> --bin tkr --root
   <operator-cache> tkr`.
4. WHEN an Odori release command is requested, THE bootstrap SHALL verify that `tkr
   version --json` reports the pinned source revision before forwarding the command.
5. WHEN the Odori train runs, THE installed binary SHALL receive `release <sub-verb>
   --workspace-root <odori-root>` with the same remaining arguments as the Tokeira
   invocation.
6. THE Odori bootstrap and release train SHALL leave `[workspace.dependencies]`
   membership unchanged except for explicit Unified Version rewrites on publishable
   internal edges.
7. THE Odori release integration SHALL invoke neither `cargo add` nor `cargo remove`.
8. IF an external dependency version needed by Odori is absent from its registry, THEN
   THE Odori planner SHALL return `ExternalDependencyUnavailable` before confirmation.
