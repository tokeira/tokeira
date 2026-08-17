# Deployment Repository — Requirements

## Introduction

A **Platform** is the prototypical platform: the crate under `platforms/` — its `src/`,
its definition in every format it ships, its manifests and templated config. A
**Deployment** is a concrete, created instance of a platform: the `tkp` provisioner
binary (built once, at creation, with all dependent providers, the IaC framework, the
platform crate, and the proto platform src), the definition modules of the **format the
operator selected at create**, and the platform's manifests and templated config.

The **deployment repository** is one named Deployment's durable, authenticated lineage:
a TUF repository (The Update Framework, v1.0, via the `tough` crate). Every deployment
has one; its residency follows the state choice made at create — a local deployment's
repository is a local filesystem repository, a remote-state deployment's lives in S3 —
and the verification machinery is identical in both homes, differing only in transport.
Role keys can be local files or, for remote repositories, AWS KMS-held keys. Its versions are **Deployment
Publications** — the first is written as part of `deployment create` (publish is part of
create); subsequent publications follow the deployment's committed lifecycle
transitions (`apply`, `upgrade`, `revert`). A publication is **fetched** into a
deployment dir to materialize the Deployment on any operator seat — recovery, additional
seats, migration — verified end-to-end from a pinned trust anchor.

Correctness stance (AGENTS.md §3): the deployment state envelope remains the
authority. A Deployment Publication is a derived, signed projection of committed
state. Publication failure never un-commits a transition; it is reported and
re-runnable.

Vocabulary rule, binding throughout: *Platform* = prototype (workspace, source form);
*Deployment* = the created instance (built engine + selected-format definition +
config), existing **published** (in its repository) and **materialized** (in a
deployment dir); *definition* = the root plus zero or more **companion** definition
modules of the selected format. CLI verbs on Deployments live under `tkr deployment`;
`tkr definition` remains scoped to definition-module authoring.

Behaviour authority: the TUF specification as implemented by `tough` 0.24, validated by
the spike at `spikes/tuf-platform-definition/` (merged PR #81). The spike's README
*Findings* and *Operational shape* sections are pre-validated input to this document.

Crates this spec names for implementation (AGENTS.md §10.3):
`crates/tokeira-deployment` — **the rename of `crates/tokeira-provisioner`**, which
already holds the deployment domain (binding, bundle, engine identity, admission,
integrity, envelope) under a name that stopped describing it; the published-form
machinery this spec adds (publication, claim, fetch, verification, listing, trust,
transports, keys) lands inside it, and its dormant `catalog` module retires. Also: the
workspace `Cargo.toml` (renames + `[workspace.dependencies]` entries),
`crates/tokeira-platform` (served-parts and identity seams), `crates/tokeira-tkp` —
**the rename of `crates/tokeira-provisioner-cli`**, the `tkp` shell (check-report
additions, lifecycle publication hooks) — whose deployment-domain residents
(`config_history`, `lock`, `marker`, and the `ConfigSource` type they share) **migrate
into `crates/tokeira-deployment` in this wave**, behaviour unchanged;
`crates/tokeira-platform-definition` — **the collapse of `crates/tokeira-tkd` and
`crates/tokeira-tkdp` into one frontend crate in this wave**, feature-gated per format
so `tkd`-only builds never carry the Monty/ruff dependency train, with the
`definition-frontend` package metadata becoming multi-format; `platforms/*`
(frontend-dependency updates); `crates/tokeira-build` (composition constants/templates
follow the `tokeira-tkp` rename; frontend discovery reads multi-format packages;
composition selects the frontend feature); `apps/tkr` (cli/main/commands,
`deployment_dir`, `bundle_create`, launcher, dependency renames). Dependency additions
(`tough`, `tough-kms`, `aws-sdk-kms`, `aws-lc-rs`, `jiff`) are sanctioned by this spec
(AGENTS.md change classification: Architectural).

## Glossary

- **Platform**: the prototypical platform — the `platforms/` crate: `src/`, definition
  in every shipped format, manifests, templated config. Source form, workspace-resident.
- **Deployment**: a concrete created instance — built `tkp` + selected-format definition
  + manifests/templated config. One name, one lineage, one repository.
- **Definition**: the root definition module plus zero or more **companion** modules, in
  the deployment's selected format (`tkd` or `tkdp`, chosen at create).
- **Deployment Repository**: the TUF repository holding one named Deployment's lineage
  — a local filesystem directory for a local deployment, an S3 location for a
  remote-state deployment. Same object contract and verification in both homes.
- **Deployment Publication**: one signed repository version — the Deployment's content
  as TUF targets plus the Deployment Claim. Monotonically versioned; created by
  `deployment create` (birth) and by committed `apply`/`upgrade`/`revert` transitions.
- **Deployment Claim**: the signed statement binding one publication's halves: platform
  id, selected format, definition section (root, companion order, configuration
  identity), engine section (engine identity digest, bundle-manifest target).
- **Fetch**: the TUF-verified retrieval of a Deployment Publication's content into a
  deployment dir — download plus full chain verification, never bare download. (`tough`'s
  transport vocabulary; the TUF specification says "download".)
- **Deployment dir**: a materialized Deployment's home: `tkp`, definition files,
  `tokeirad.toml`, `metadata.json`, `state/`.
- **Local / Remote-state Deployment**: the state-residency choice made at
  `deployment create` and immutable thereafter. Both publish: a local deployment to its
  local repository, a remote-state deployment to its S3 repository.
- **Deployment Listing**: enumerating deployments — local (from the deployments root)
  or remote-state (from the configured remote deployments base), selected by CLI
  option.
- **Platform Discovery**: resolving the available Platforms (and their formats) at
  `deployment create` time — today from the workspace's platform packages. The word
  "catalog" is retired from this vocabulary; existing catalog-named code is deprecated
  toward these terms as it is touched.
- **Dev Engine**: a `tkp` built non-hermetically from the workspace for the platform
  development inner loop — the explicit `--dev-engine` create option, never the
  default, local deployments only.
- **Engine**: the bound `tkp`: shell + providers + IaC framework + platform crate +
  proto platform src, compiled for one platform/format pair; identity = `EngineIdentity`.
- **Bundle Manifest**: the `ProvisionerBundle` document (`tkp.manifest.json`): engine
  identity, build authority, per-target artifact descriptors, evidence.
- **Trust Anchor**: the pinned `root.json` bytes verification starts from. Distributed
  out-of-band and pinned in the deployment dir; never fetched blindly.
- **Role Keys**: TUF's four roles — root (trust anchor, offline), and the online roles
  targets, snapshot, timestamp. File- or KMS-held.
- **Freshness Window**: the lifetime of `timestamp.json`; after it lapses, consumers
  refuse the repository by default.
- **Refresh**: re-signing `timestamp.json` (and `snapshot.json` when required) within an
  unchanged publication to extend the freshness window.
- **Client Datastore**: the persistent directory where a consumer's TUF client caches
  trusted metadata; what makes rollback detection hold across separate loads.
- **Create-Only Object**: an S3 object written with `If-None-Match: *`, byte-verified on
  collision, never overwritten.
- **Mutable Head**: `timestamp.json` and the convenience un-versioned `root.json` copy —
  the only objects that move in place.

## Target State

Supported when this spec is implemented:

- `tkr deployment create`: the operator selects a discovered Platform, a definition
  format, and local vs remote state — the latter two immutable for the deployment's
  lifetime. `tkp` is built **once**, by default through the hermetic bundle machinery
  (CAS hit or one Dagger build) for local and remote-state creates alike; an explicit
  dev-engine option (local only) admits the workspace build for the platform dev
  cycle. The Deployment is constructed and the birth Deployment Publication is written
  to the deployment's repository — local filesystem or S3 per the state choice — as
  part of create, with trust pinned and the client datastore initialized in the
  deployment dir.
- Committed `apply`, `upgrade`, and `revert` transitions produce the next Deployment
  Publication, capturing the deployment's post-transition content. `revert` publishes a
  **new** publication carrying prior content — repository versions are monotonic even
  when configuration history moves backward.
- `tkr deployment fetch` materializes a Deployment Publication into a deployment dir on
  any seat: definition files at their recorded paths, `tkp` placed from the published
  engine artifact for the host target with its manifest sidecar, config trees, pinned
  trust anchor, initialized datastore.
- `tkr deployment list` enumerates deployments — local or remote-state per CLI
  option; a remote-state listing reads the configured remote deployments base in S3,
  where each deployment's repository lives under its name.
- `tkr deployment publish` re-runs publication of the current committed state — the
  repair/catch-up verb for a publication that failed after a committed transition.
- `tkr deployment refresh` re-signs the freshness statement; `tkr deployment inspect`
  verifies read-only and reports the publication, claim, expirations, and inventory.
- Verification enforces, beyond TUF's own chain: exactly one Deployment Claim per
  publication; claim/target agreement across both halves; recomputed configuration
  identity equal to the claimed identity; bundle-manifest artifact digests equal to the
  TUF target hashes of the engine binaries.

Out of scope, with remedies:

- **Scheduled refresh automation.** The freshness obligation is real; the remedy in this
  wave is the `refresh` verb (cheap, online-keys-only). Automation is deferred to its
  own design once an operational host exists.
- **Multi-seat write coordination.** Once fetch exists, the same deployment can be
  materialized on two machines, and both could run a state-changing action at once.
  The lock that serializes them (an operation lease held for the duration of a
  transition) is a spec that MUST immediately follow this one. Until it lands, a race
  fails loudly rather than corrupting: the envelope compare-and-swap refuses the
  second committer, and repository writes are create-only, so the second publication
  attempt is refused, never overwriting.
- **Discovery beyond listing.** `fetch` takes an explicit repository locator (plus
  trust anchor) and `list` enumerates the two homes; richer cross-machine discovery is
  a follow-on. The dormant published-catalog types
  (`PublishedProvisionerCatalog`/`PublishedProvisionerLocator`) are deprecated by this
  spec's vocabulary and removed as the new listing/discovery surface replaces them.
- **TUF delegated targets and thresholds > 1.** Single-operator repositories; threshold
  raising is configuration, not new code.
- **Engine rebuilds after create.** `tkp` is built once at creation; `upgrade`'s
  engine-replacement path continues through its existing machinery, and its publication
  captures the post-upgrade engine like any committed transition.

## Evidence From Current Code

Create and engine ground truth:

- The hermetic engine build and placement exist end-to-end: composition freezes one
  platform/frontend pair (`crates/tokeira-build/src/composition.rs:36`), Dagger builds
  it (`crates/tokeira-build/src/dagger.rs`), and `place_bundle_provisioner_at`
  (`apps/tkr/src/bundle_create.rs:32`) obtains via CAS (`.bundle-cas/`) or build, selects
  the host-target artifact (refusing when absent), retains it, and writes the manifest
  sidecar beside `tkp`.
- `ProvisionerBundle` (`crates/tokeira-provisioner/src/bundle.rs:163`): `EngineIdentity`,
  optional `BoundProvisionerEvidence`, `BuildAuthority`, per-target
  `BinaryArtifactDescriptor` artifacts. `BinaryArtifactDescriptor`
  (`crates/tokeira-provisioner/src/lib.rs:140`) carries
  `retrieval_ref: Option<String>` — "Optional retrieval pointer (e.g. an S3 key)" — the
  field this spec gives a concrete meaning. The bundle deliberately carries **all**
  targets so another operator platform can run it (`lib.rs:152`).
- `EngineIdentity` (`crates/tokeira-provisioner/src/identity.rs:72`): source closure,
  lock closure, toolchain, hermetic build container (`None` = non-hermetic dev build),
  features, profile.
- `begin_create` (`apps/tkr/src/deployment_dir.rs:316`) stages atomically (hidden
  staging dir, publish by rename at `:118`, `Drop` cleanup); `state/` is the only
  eagerly-created directory (`:346`); provisioner placement and staged validation run
  between stage and publish (`apps/tkr/src/commands/deployment.rs:97-115`).

Definition and identity ground truth:

- Evaluation records the served set and computes the identity, then discards the served
  list: `crates/tokeira-platform/src/definition.rs:327` (`evaluate_definition`,
  `RecordingResolver` at `:161`); `EvaluatedDefinition` (`:314`) carries only
  config/graph/identity; `ConfigurationIdentity::compute_set` is private (`:277`); the
  identity is persisted nowhere today.
- `tkp definition check` drops the evaluated value
  (`crates/tokeira-provisioner-cli/src/definition.rs:104`); its `CheckReport` (`:21`)
  carries no identity and no companion list.
- The staged set today is `sibling_parts` (`apps/tkr/src/deployment_dir.rs:74`) — every
  sibling file with the root's extension, ascending by name; retention
  (`crates/tokeira-provisioner-cli/src/config_history.rs:30-36`) deliberately keeps the
  same whole authored set.

Lifecycle transition ground truth:

- Apply commits through the envelope CAS and then retains:
  `crates/tokeira-provisioner-cli/src/apply.rs` (writeback → envelope re-stamp →
  `config_history::snapshot`); the envelope's `effective_config_ref` is a root-bytes-only
  digest (`apply.rs:191`), unrelated to the configuration identity.
- `metadata.json` evolution must be additive-optional (`apps/tkr/src/metadata.rs:34`);
  `DeploymentBindingMetadata` tolerates unknown fields by design
  (`crates/tokeira-provisioner/src/deployment.rs:21`).

Storage precedent and boundary constraints:

- `S3Backend` already implements CAS via `If-None-Match: *` / `If-Match`
  (`crates/tokeira-state/src/s3_backend.rs:14`). `crates/tokeira-state/AGENTS.md` binds
  that crate to immutable snapshots + single mutable CAS manifest — TUF's mutable
  `timestamp.json` does not fit that contract, so the repository layout lives in the new
  crate, with `tokeira-state` untouched.
- Config ownership rules (`docs/agents/engineering-reference.md`): config structs are
  `deny_unknown_fields`; no environment variables on invocation (AWS SDK ambient
  credential resolution is the sanctioned exception, per the spike).
- Spike ground truth (`spikes/tuf-platform-definition/`): `S3Transport` contract
  (`NoSuchKey` → `FileNotFound` for the root-version walk), create-only upload with
  byte-verify on collision, expiry fail-closed with `ExpirationEnforcement::Unsafe` as
  break-glass, rollback detection requiring a persistent datastore, KMS = RSA-only
  `RSASSA_PSS_SHA_256` at `tough-kms` 0.16, online-key rotation via the root-version
  walk, metadata republish not byte-identical (version-named metadata is the immutable
  unit; content-named targets republish idempotently).

## Repository Object Contract

Let `R = s3://<bucket>/<prefix>` for one Deployment's repository.

| Object class | Key | Write condition | Mutability / crash-visible result |
|---|---|---|---|
| Versioned root | `R/metadata/<N>.root.json` | `If-None-Match: *`; byte-verify on collision | Immutable; the root-version walk reads these |
| Trust-anchor copy | `R/metadata/root.json` | Unconditional put | Mutable head; operator-pinning convenience, never fetched by the verification chain |
| Versioned targets | `R/metadata/<N>.targets.json` | `If-None-Match: *`; byte-verify on collision | Immutable |
| Versioned snapshot | `R/metadata/<N>.snapshot.json` | `If-None-Match: *`; byte-verify on collision | Immutable |
| Freshness statement | `R/metadata/timestamp.json` | Unconditional put (operator-serialized) | Mutable head; the only object refresh rewrites |
| Definition target | `R/targets/<sha256>.<file-name>` | `If-None-Match: *`; byte-verify on collision | Immutable, content-named; unchanged files re-publish idempotently across publications |
| Config-tree target | `R/targets/<sha256>.<relative-path>` | `If-None-Match: *`; byte-verify on collision | Immutable; manifests/templated config under their platform-relative paths |
| Bundle manifest target | `R/targets/<sha256>.tkp.manifest.json` | `If-None-Match: *`; byte-verify on collision | Immutable; one per publication (unchanged engine re-publishes idempotently) |
| Engine binary target | `R/targets/<sha256>.tkp-<target-triple>` | `If-None-Match: *`; byte-verify on collision | Immutable; one per bundle artifact |

A collision on any create-only object with differing bytes SHALL refuse the upload and
identify the key; identical bytes are reported as already present, not rewritten.

## Deployment Claim Contract

Carried as `custom["tokeira:deployment"]` on the definition root's target; companion
targets carry `custom["tokeira:definition-companion"] = { format }`; engine targets
carry `custom["tokeira:engine-artifact"] = { target }`; config-tree targets carry
`custom["tokeira:config"] = {}`.

| Field | Target policy | Error if invalid | Notes |
|---|---|---|---|
| `deployment` | The deployment's name and id, equal to the fetched-into or created deployment's identity | `claim_deployment_mismatch` | Binds the lineage to one named Deployment |
| `platform` | Equals the deployment's platform id | `claim_platform_mismatch` | |
| `format` | The format selected at create | `claim_format_mismatch` | One format per Deployment, for life |
| `definition.root` | Equals the target name carrying the claim | `claim_root_mismatch` | The root module's file name |
| `definition.companions` | Bare companion names, served order, each resolvable as `<name>.<format>` among targets | `claim_companion_missing` | May be a strict subset of the published set |
| `definition.identity.algorithm` | `sha256-v1` (no served companions) or `sha256-set-v1` | `claim_identity_invalid` | Labels per `ConfigurationIdentity::algorithm` |
| `definition.identity.digest` | Equals the identity recomputed from fetched bytes in claimed order | `identity_mismatch` | Recomputed with the product implementation, not a mirror |
| `engine.identity_digest` | Equals the bundle manifest's `EngineIdentity` digest | `engine_identity_mismatch` | Binds the claim to the exact engine closure |
| `engine.provisioner_version` | Equals the bundle manifest's version label | `engine_version_mismatch` | Human-facing label, never a key |
| `engine.manifest` | Names the bundle-manifest target, present among targets | `engine_manifest_missing` | The manifest enumerates the binaries |
| `transition` | `create` \| `apply` \| `upgrade` \| `revert` — the committed transition this publication captures | `claim_transition_invalid` | With the post-transition `config_revision` |

Exactly one target SHALL carry a Deployment Claim; zero or several is a verification
refusal (`claim_missing` / `claim_ambiguous`). Every artifact listed in the fetched
bundle manifest SHALL have a matching engine binary target whose TUF hash equals the
descriptor's `sha256` (`engine_artifact_mismatch` otherwise).

## Deployment Directory Additions

All additions live inside existing reservations (`metadata.json`, `state/`); no new
reserved path.

| Path / field | Content | Written when | Mutability |
|---|---|---|---|
| `state/repository/root.json` | Trust anchor bytes as verified (updated after an accepted root-version walk) | Create / fetch; refreshed on load | Owned by the repository client |
| `state/repository/datastore/` | TUF client datastore | Create / fetch; every verified load | Owned by the repository client |
| `metadata.json` → `deployment_repository` | `{ locator, trusted_root_digest }` | Create / fetch | Additive; locator is a filesystem path (local) or S3 base (remote-state) |
| `tkp` + `tkp.manifest.json` | On fetch: engine binary (host target) + bundle manifest from verified targets | Fetch | Exactly the placement shape of `place_bundle_provisioner_at` |

## Requirements

### Requirement 1: Create builds once and constructs the Deployment

**User Story:** As an operator, I want `deployment create` to select my definition
format and build `tkp` exactly once through the hermetic pipeline, so that the
Deployment that comes into existence is complete, evaluated, and reproducible from its
recorded identity.

#### Acceptance Criteria

1. WHEN `tkr deployment create` runs, THE Platform SHALL be resolved through platform
   discovery, and THE operator's definition-format and local/remote-state selections
   SHALL be fixed for the Deployment's lifetime — no later verb SHALL change either.
   THE engine SHALL be obtained by default through the existing bundle machinery (CAS
   hit or one hermetic Dagger build) for exactly the selected platform/format pair,
   for local and remote-state creates alike.
2. THE dev engine SHALL be available only as an explicit create option and only for
   local deployments; WHERE it is used, THE claim SHALL record the build authority
   tier. IF an engine without a pinned build container in its `EngineIdentity` would
   enter an S3 repository, THEN the create or publication SHALL refuse, naming the
   hermeticity requirement.
3. WHEN the Deployment is constructed, THE definition modules staged SHALL be the
   selected format's root and companions (the `sibling_parts` rule), together with the
   platform's manifests and templated config trees.
4. WHEN create validates the staged Deployment, THE evaluation SHALL produce the
   configuration identity and the served companion order for the claim, computed by the
   engine itself; THE check report SHALL be extended to emit them.
5. THE `tokeira-platform` crate SHALL expose the served set recorded during
   `evaluate_definition` to callers, and `ConfigurationIdentity` recomputation over
   (format, root bytes, served companions) SHALL be publicly callable, WHILE the digest
   layouts of `sha256-v1` and `sha256-set-v1` remain byte-for-byte unchanged.
6. WHILE a create selects local state, THE deployment's repository SHALL be a local
   filesystem repository under the deployments root, and THE publication flow SHALL be
   identical to the remote-state flow except for transport and key defaults.

### Requirement 2: Publish is part of create

**User Story:** As an operator, I want the birth publication written as part of
`deployment create`, so that every Deployment is authenticated and
distributable from its first moment.

#### Acceptance Criteria

1. WHEN a create commits, THE system SHALL write Deployment
   Publication 1 to the deployment's repository: the engine binaries (every bundle
   artifact), the bundle manifest, the selected format's definition modules, the
   config trees, and the Deployment Claim with `transition = create`.
2. THE local deployment dir materialized by create SHALL be byte-identical, for every
   published file, to what a fetch of publication 1 would materialize.
3. WHEN create pins trust, THE trust anchor and client datastore SHALL be initialized
   inside the staged deployment before the atomic publish rename, and THE
   `deployment_repository` binding SHALL be recorded in `metadata.json` per the
   Deployment Directory Additions table.
4. IF the publication upload fails after the local deployment is committed, THEN create
   SHALL report the deployment as created and the publication as pending, and
   `tkr deployment publish` SHALL complete it — THE local commit SHALL NOT be unwound
   by a publication failure.
5. WHEN publish assembles a publication, THE `retrieval_ref` of each bundle artifact
   descriptor SHALL name its engine binary target, and each descriptor's `sha256` SHALL
   equal the TUF target hash of that binary.

### Requirement 3: The repository is one Deployment's monotonic lineage

**User Story:** As an operator, I want the repository to hold my deployment's
publications as an append-only, consistently-snapshotted version history, so that every
state my deployment committed is durably authenticated and none can be silently
rewritten.

#### Acceptance Criteria

1. THE repository SHALL enable consistent snapshots, with role metadata and targets laid
   out exactly per the Repository Object Contract table.
2. WHEN a publication is created, THE targets, snapshot, and timestamp roles SHALL share
   one publication version, strictly greater than any version already present.
3. IF the repository already holds a publication at the same version with differing
   metadata bytes, THEN upload SHALL refuse at the first colliding create-only object,
   leaving mutable heads unwritten.
4. THE Deployment Claim SHALL ride in targets metadata per the Deployment Claim
   Contract, signed by the targets role, binding both halves and the transition in one
   signature.
5. WHEN publish completes, THE mutable heads SHALL be written last, after every
   create-only object has been admitted.
6. WHEN consecutive publications share content (an unchanged engine across an apply, an
   unchanged definition file), THE shared content SHALL re-publish idempotently as the
   same content-named targets — never duplicated, never rewritten.

### Requirement 4: Lifecycle transitions publish their committed state

**User Story:** As an operator, I want each committed `apply`, `upgrade`, and `revert`
to yield the next Deployment Publication, so that the repository always carries the
deployment's current authenticated state and its full committed history.

#### Acceptance Criteria

1. WHEN an `apply`, `upgrade`, or `revert` transition commits (envelope CAS succeeded),
   THE system SHALL publish the post-transition Deployment as the next publication,
   with `transition` and the post-transition `config_revision` in the claim.
2. THE envelope commit SHALL remain the authority: publication follows commit, a
   publication failure SHALL NOT fail or unwind the committed transition, and the
   failure SHALL be reported with `tkr deployment publish` as the stated remedy.
3. WHEN `revert` publishes, THE publication SHALL be a new, higher version whose
   content is the reverted-to state — repository versions advance even when
   configuration history moves backward.
4. WHEN `upgrade` replaces the engine, THE next publication SHALL carry the new engine
   binaries and manifest, claim-bound to the new `EngineIdentity` digest.
5. WHERE a deployment is local, THE lifecycle transitions SHALL publish to its local
   repository through the same machinery, differing only in transport and keys.

### Requirement 5: Fetch materializes a Deployment on any seat

**User Story:** As an operator, I want `tkr deployment fetch` to materialize a
published Deployment into a deployment dir from nothing but a locator and a trust
anchor, so that recovery and additional seats never depend on the workspace or on
unverified bytes.

#### Acceptance Criteria

1. WHEN `tkr deployment fetch` runs with a repository locator and trust anchor, THE
   system SHALL load and verify the repository from the pinned anchor, fetch the
   current publication, and enforce the Deployment Claim in full before materializing
   any byte.
2. WHEN materializing, THE system SHALL stage the definition modules at their recorded
   paths, the config trees at their relative paths, and `tkp` from the verified engine
   artifact for the host target with the manifest sidecar beside it — matching the
   placement semantics of `place_bundle_provisioner_at` — refusing IF the manifest
   carries no artifact for the host target.
3. WHEN fetch completes, THE deployment dir SHALL carry the pinned trust anchor, the
   initialized client datastore, and the `deployment_repository` binding in
   `metadata.json`; staging SHALL be atomic with cleanup on failure, exactly as
   create's staging is today.
4. IF verification, claim enforcement, identity recomputation, or engine-artifact
   agreement fails, THEN fetch SHALL refuse before materializing any byte.
5. WHEN a fetched deployment runs `tkp` verbs, THE bound-provisioner admission and
   integrity checks SHALL operate on the placed bundle exactly as they do for a
   `--bundle` workspace create.

### Requirement 6: S3 residency through a minimal verified transport

**User Story:** As a consumer of publications, I want repository objects fetched through
a transport that adds no trust of its own, so that S3 (or any S3-compatible endpoint) is
storage, never an authority.

#### Acceptance Criteria

1. THE `S3Transport` SHALL implement `tough::Transport` over `GetObject` for
   `s3://<bucket>/<key>` URLs and SHALL refuse other schemes with the transport's
   unsupported-scheme error.
2. WHEN S3 reports `NoSuchKey` (or a bare 404 from an S3-compatible endpoint), THE
   transport SHALL classify it as file-not-found, WHILE all other failures classify as
   transport faults — preserving the absence signal the TUF root-version walk requires.
3. THE transport SHALL stream object bytes through unchanged; integrity verification
   against signed metadata remains the TUF client's, and the transport SHALL NOT cache,
   truncate, or transform bodies.
4. THE uploader SHALL implement the Repository Object Contract write conditions,
   reporting per-object outcomes (created / already-present / replaced).

### Requirement 7: Trust is anchored and rotates in place

**User Story:** As an operator, I want trust pinned to explicit root bytes and key
rotation to happen inside the repository, so that verification never depends on the
storage being honest and rotation never requires re-touching every seat.

#### Acceptance Criteria

1. WHEN a repository is loaded for verification, THE client SHALL start from trust-anchor
   bytes supplied out-of-band (fetch input or the deployment's pinned copy) and SHALL
   NOT bootstrap trust from any fetched object.
2. WHEN a newer `<N+1>.root.json` exists, THE client SHALL walk root versions per TUF and
   accept the new chain only under the prior root's signing requirements; WHEN the walk
   succeeds, THE deployment's pinned trust-anchor copy SHALL be updated to the accepted
   version.
3. WHEN the online role keys are rotated via a new root version signed by the existing
   root key, THE consumer pinned to the prior root SHALL verify subsequent publications
   without operator intervention.
4. IF the trust-anchor bytes fail to parse or verify as a root role, THEN load SHALL
   refuse before any network fetch.

### Requirement 8: Role keys are sources, and KMS is one of them

**User Story:** As an operator, I want each TUF role's key to be an abstract key source,
so that a role can move from a local file to KMS without the publisher changing.

#### Acceptance Criteria

1. THE publisher SHALL accept one key source per role (root, targets, snapshot,
   timestamp), each independently either a local Ed25519 PKCS#8 file or a KMS key.
2. WHERE a role uses KMS, THE key SHALL be an RSA signing key used with
   `RSASSA_PSS_SHA_256` (the `tough-kms` 0.16 support surface), and THE error for an
   unsupported KMS key spec SHALL name the constraint.
3. THE root key source SHALL be exercised only when authoring or rotating root.json
   (create; explicit rotation); lifecycle publications and refresh SHALL require only
   the online role keys they sign with.
4. THE repository configuration (locator, key sources, role lifetimes) SHALL be a
   `deny_unknown_fields` structure supplied explicitly at create/fetch; THE system SHALL
   NOT read environment variables for it, WHILE AWS SDK ambient credential resolution
   remains the sanctioned exception.
5. WHERE a deployment is local, THE role keys SHALL default to locally generated
   Ed25519 files stored under the deployments root, outside the repository itself.

### Requirement 9: Freshness fails closed and rollback is detected

**User Story:** As an operator, I want a frozen or rewound repository to be refused by
default, so that stale or replayed publications cannot be fetched without my explicit
consent.

#### Acceptance Criteria

1. WHEN the freshness statement has expired, THE load SHALL refuse, naming the
   expiration; verification with expiration enforcement disabled SHALL exist only behind
   an explicit operator flag whose report states that freshness was not enforced.
2. THE repository client SHALL persist its datastore under
   `state/repository/datastore/`, and WHEN a repository presents role versions
   lower than those already trusted in that datastore, THE load SHALL refuse as a
   rollback.
3. WHEN `tkr deployment refresh` runs, THE system SHALL re-sign the freshness statement
   (and snapshot, if its expiry requires) for the current publication without altering
   targets or their claim, and SHALL report the new expiration instants.
4. THE default role lifetimes SHALL make the freshness window the shortest lifetime, and
   all lifetimes SHALL be configurable.

### Requirement 10: Identity has one implementation and it always agrees

**User Story:** As a maintainer, I want exactly one implementation of the configuration
identity, so that publish, fetch, and future comparisons can never drift.

#### Acceptance Criteria

1. THE identity recomputed at verification SHALL call the `tokeira-platform`
   implementation; THE spike's mirrored computation SHALL NOT be promoted into any
   production crate.
2. WHEN verification recomputes the identity over fetched bytes in claimed order, THE
   result SHALL equal the claimed identity, and any mismatch SHALL refuse the whole
   publication (no partial materialization), naming both digests.
3. THE single-document identity (`sha256-v1`) SHALL remain byte-stable for a root whose
   evaluation serves no companions, published and fetched through the repository.

### Requirement 11: The operator surface explains itself

**User Story:** As an operator, I want the repository verbs to follow the house CLI
contract, so that repository operations are reviewable, scriptable, and their failures
actionable.

#### Acceptance Criteria

1. THE verbs `fetch`, `list`, `publish`, `refresh`, and `inspect` SHALL live under
   `tkr deployment` and SHALL honour the global `--json` and `--detail` contract.
   WHEN `list` runs, THE CLI option SHALL select local or remote-state enumeration:
   local from the deployments root, remote-state from the configured remote
   deployments base.
2. WHEN a `create` (or a `publish`) would write to a repository, THE
   confirmation surface SHALL include the repository locator and what will be
   published (publication version, both halves' inventories with digests, claim
   identity, engine identity digest), under create's existing `--yes`/interactive
   gating (AGENTS.md §4).
3. WHEN `tkr deployment inspect` runs, THE system SHALL verify read-only and report the
   publication version, transition, role expirations, target inventory, and Deployment
   Claim, refusing with the same classification a fetch would (expired / rolled back /
   tampered / claim mismatch / engine-artifact mismatch).
4. WHEN any verb refuses, THE report SHALL state what happened, why, and what to do
   next, and refusal classes SHALL be typed (distinct exit path and stable refusal
   name), not stringly-matched.

### Requirement 12: Downstream machinery is invariant

**User Story:** As a maintainer of retention, retarget, and the envelope, I want
publications to be a pure projection, so that this feature adds durable authentication
without forking any lifecycle behaviour.

#### Acceptance Criteria

1. WHEN a deployment applies a revision, THE `config_history` snapshot, sidecar, and
   retained-companion behaviour SHALL be unchanged by this feature; publication happens
   after and beside retention, never instead of it.
2. THE deployment state envelope SHALL remain the sole commit authority; no correctness
   decision SHALL read repository state.
3. THE evaluation, verification, and realization behaviour of `tokeira-platform` and
   the frontends SHALL be unchanged except for the additive seams of Requirement 1.5;
   the frontends' relocation into `tokeira-platform-definition` and the
   `config_history`/`lock`/`marker` migration into `tokeira-deployment` SHALL move
   code without changing any behaviour, digest, on-disk format, or report.
4. WHEN retarget compares definition sets, THE resolvers SHALL be constructed exactly as
   today; THE repository SHALL NOT appear in the retarget path in this wave.
