# Engine Versioning Requirements

## Introduction

A bound provisioner is one composition: `tkp` = platform definition + engine. The engine — the
provisioner shell, the platform framework, the definition frontends, and the provider crates with
their kinds — is the shared machinery compiled into every `tkp`. The platform definition is the
platform package's declaration of itself: its config, context, ops, catalogs, and descriptor.

Today the compatibility statement between those two halves is expressed as two private counters —
`binding-contract` on platform descriptors and `frontend-contract` on definition-frontend
descriptors — validated by exact match against constants compiled into `tokeira-build`. Counters
have the failure mode this codebase already rejected for drift detection: a human can forget to
bump them, and nothing notices. Meanwhile the engine already has exactly one human-meaningful
version — the workspace package version, surfaced as `TOKEIRA_VERSION`, stamped into every
provisioner's provenance, and recorded on every bundle — that nothing currently declares against.

This feature names that axis and makes it the only declaration in the assembly: **the platform
definition indicates the engine version it composes with.** Definition frontends carry no
compatibility statement at all — they are engine components and version with it. The two contract
counters are removed. Publication gives the axis its operational meaning: an engine release is one
act that produces tagged, canonically built, catalogued bundles keyed by platform, format, and
engine version, so an installed `tkr` resolves exactly the engine a platform indicates.

Authority boundaries preserved unchanged: the binding gate's drift authority remains
`source_tree_hash` (a digest a developer cannot forget); `EngineIdentity` remains the
interchangeability key; the upgrade boundary's monotonic ordering and state-schema migration chain
are untouched. This feature changes only the declaration and publication layers.

## Glossary

- **Engine** — The shared machinery compiled into every bound provisioner: `tokeira-provisioner-cli`
  (shell), `tokeira-platform` (framework), the definition frontends (`tokeira-tkd` and successors),
  `tokeira-orchestrator`/`tokeira-iac`/state, and the provider crates including their kinds.
- **Engine_Version** — The single human-meaningful version of the engine: the workspace package
  version, surfaced as `TOKEIRA_VERSION` and recorded wherever a version label already exists. A
  label for declaration, ordering, and publication — never a drift-detection key.
- **Platform_Definition** — The platform package composed with the engine to produce `tkp`: its
  source convention (`config.rs`/`context.rs`/`ops.rs`/`lib.rs`), platform-owned assets, and its
  trusted descriptor.
- **Engine_Indication** — The `engine` field of a platform descriptor: the Engine_Version the
  Platform_Definition declares it composes with.
- **Descriptor_Stable_Fields** — `id` and `engine` on platform descriptors; `format` on
  definition-frontend descriptors. The fields every present and future `tkr` can read regardless of
  how the rest of the descriptor evolves.
- **Engine_Release** — The publication act for one Engine_Version: version-bump commit, tag,
  canonical builds, catalogue admission, and surface delta.
- **Canonical_Build** — The hermetic build (pinned container, trusted authority) whose artifacts
  are admissible to the published catalog; the existing `BuildAuthority` distinction is unchanged.
- **Published_Catalog** — The admitted catalog installed `tkr` resolves bundles from; entries are
  keyed by platform, definition format, and Engine_Version.
- **Engine_Surface_Delta** — The per-release record of authoring-surface changes: provider kinds
  added, changed, or removed, and definition-frontend surface changes.

## Target State

The platform descriptor declares its engine; no descriptor carries a contract counter:

```toml
[package.metadata.tokeira.platform]
id = "compose"
engine = "0.1.0"
default = false
```

```toml
[package.metadata.tokeira.definition-frontend]
format = "tkd"
source-extension = "tkd"
default-relative-path = "definition.tkd"
```

At assembly, the indication is asserted against the workspace's Engine_Version. A platform that has
not adopted the current engine refuses with both versions and the adoption instruction:

```text
platform `compose` indicates engine 0.1.0; this workspace is engine 0.2.0.
Adopt the 0.2.0 surface (see its engine surface delta), then update `engine`.
```

The indication is an exact version, not a range. While platform definitions live in the engine's
workspace this is a consistency assertion whose bump is a reviewable adoption act; if platform
definitions ever separate from the engine tree, the same field becomes the resolution input, with
no model change.

Publishing engine `X.Y.Z` is one act:

1. the version-bump commit — the only place the Engine_Version changes;
2. the release tag on that commit — the audit anchor build manifests already reference;
3. Canonical_Builds of the admitted platform × format matrix at that tree, landing
   identity-addressed artifacts and evidence in the bundle store;
4. Published_Catalog entries keyed `(platform, format, engine)`;
5. the Engine_Surface_Delta for the release.

An installed `tkr` resolving a deployment's provisioner reads the platform's Engine_Indication and
resolves the catalog entry for exactly that engine. Development builds remain unpublished and
advisory, exactly as today.

Out of scope: version ranges or any resolution policy beyond exact indication; out-of-tree or
third-party platform distribution; changes to the binding gate, `EngineIdentity`, upgrade
monotonicity, or state-schema migrations; per-kind or per-frontend version numbers; the CI system
hosting the release pipeline (the act is defined by its artifacts, not its runner).

## Evidence From Current Code

- `Cargo.toml` — `[workspace.package] version = "0.1.0"`: the Engine_Version's single source.
- `crates/tokeira-build-info/src/lib.rs` — `TOKEIRA_VERSION`, `TOKEIRA_GIT_SHA`,
  `SOURCE_TREE_HASH`, `BUILD_MODE`: the compiled surfacing of that version and its companions.
- `crates/tokeira-provisioner/src/lib.rs` (`ProvenanceStamp`) and `src/binding.rs` — the stamp
  records the semver but the binding gate's authoritative drift key is `source_tree_hash`, "never
  the semver (which a developer can forget to bump)". This feature keeps that division and applies
  the same reasoning against contract counters.
- `crates/tokeira-provisioner/src/version.rs` and `src/upgrade.rs` — numeric ordering and the
  monotonic upgrade decision already operate on the version label; unparseable versions yield no
  ordering. Unchanged by this feature.
- `crates/tokeira-provisioner/src/bundle.rs` — `ProvisionerBundle.provisioner_version` ("the
  semver; never a key") and `BoundProvisionerEvidence { platform, format, binding_contract,
  frontend_contract, … }`: the label already travels with bundles; the two contract fields are the
  ones this feature replaces with the indicated engine.
- `crates/tokeira-build/src/discovery.rs` — `PLATFORM_BINDING_CONTRACT: u32 = 1` and
  `DEFINITION_FRONTEND_CONTRACT: u32 = 1` with exact-match rejection of any other value: the
  counters and their validation sites this feature removes.
- `crates/tokeira-provisioner/src/catalog.rs` — `PublishedPlatformDescriptor`,
  `PublishedDefinitionFrontendDescriptor`, `PublishedProvisionerLocator`: published resolution
  today pairs platform and format with no Engine_Version key.
- `platforms/compose/Cargo.toml` and `crates/tokeira-tkd/Cargo.toml` — the live descriptors:
  `id`/`binding-contract`/`default` and
  `format`/`frontend-contract`/`source-extension`/`default-relative-path` respectively.

## Contract Policy

### Platform descriptor (final form)

| Field | Policy | Stability |
|---|---|---|
| `id` | canonical lower-kebab platform identity | Descriptor_Stable_Field |
| `engine` | exact Engine_Version the platform composes with; asserted at assembly | Descriptor_Stable_Field |
| `default` | unchanged existing semantics | may evolve |
| `binding-contract` | **removed**; presence is a descriptor violation | — |

### Definition-frontend descriptor (final form)

| Field | Policy | Stability |
|---|---|---|
| `format` | canonical format identity | Descriptor_Stable_Field |
| `source-extension` | seed convention, unchanged | may evolve |
| `default-relative-path` | seed convention, unchanged | may evolve |
| `frontend-contract` | **removed**; presence is a descriptor violation | — |

### Bound-provisioner evidence

| Field | Policy |
|---|---|
| `platform`, `format` | unchanged admission facts |
| `engine` | the Engine_Indication admitted at assembly; replaces both contract fields |
| `binding_contract`, `frontend_contract` | removed |
| root digest, source closure, lock closure | unchanged |

### Published catalog entry

| Field | Policy |
|---|---|
| key | `(platform id, definition format, engine version)` — one entry per admitted triple |
| payload | bundle locator, `EngineIdentity` digest, authority, evidence |

## Requirements

### Requirement 1: One engine version, one source

**User Story:** As the workspace owner, I want the engine's version defined in exactly one place
and surfaced consistently, so that every record naming an engine version is naming the same fact.

#### Acceptance Criteria

1. THE Engine_Version SHALL be the workspace package version, with no second engine-version
   constant anywhere in the workspace.
2. THE compiled provisioner SHALL surface the Engine_Version through the existing build-info and
   provenance-stamp paths without a parallel mechanism.
3. WHEN a bundle records its version label, THE recorded value SHALL be the Engine_Version of the
   tree that built it.
4. THE Engine_Version SHALL NOT replace `source_tree_hash` as the binding gate's drift authority or
   `EngineIdentity` as the interchangeability key.

### Requirement 2: The platform's engine indication

**User Story:** As a platform owner, I want my platform definition to declare the engine version it
composes with, so that adopting an engine's surface changes is an explicit, reviewable act rather
than an invisible drift.

#### Acceptance Criteria

1. THE platform descriptor SHALL carry `engine`, an exact Engine_Version string.
2. WHEN platform discovery admits a descriptor, THE assembly SHALL assert the Engine_Indication
   equals the workspace Engine_Version before the composition root is generated.
3. IF the indication and the workspace Engine_Version differ, THEN THE assembly SHALL refuse with
   both versions and the adoption instruction, and SHALL NOT generate a composition root.
4. THE indication SHALL be an exact version; range or constraint syntax SHALL be rejected as a
   descriptor violation.
5. WHEN a platform adopts a new engine surface, THE `engine` bump SHALL be the reviewable adoption
   record; no other adoption declaration SHALL exist.
6. THE Descriptor_Stable_Fields SHALL remain readable by any `tkr` regardless of other descriptor
   evolution, and a `tkr` that cannot admit a descriptor SHALL still name the descriptor's `id` and
   `engine` in its refusal when they parse.

### Requirement 3: Contract counters removed

**User Story:** As a maintainer, I want the private contract counters gone, so that the assembly
has exactly one compatibility declaration and no forgettable numbers.

#### Acceptance Criteria

1. THE platform descriptor SHALL NOT carry `binding-contract`; THE definition-frontend descriptor
   SHALL NOT carry `frontend-contract`; presence of either SHALL be rejected as a descriptor
   violation naming the replacement.
2. THE discovery constants validating those counters SHALL be removed with their validation sites.
3. THE bound-provisioner evidence SHALL record the admitted Engine_Indication in place of the two
   contract fields, and bound admission SHALL compare it as an assembly fact exactly as the
   contract fields were compared.
4. THE definition-frontend descriptor SHALL carry no engine or version field: frontends are engine
   components and SHALL derive their version from the engine alone.
5. THE feature SHALL introduce no new contract, schema, or capability counter anywhere in the
   descriptor, evidence, or catalog surfaces.

### Requirement 4: Publishing an engine version

**User Story:** As the release owner, I want publishing an engine version to be one defined act
with named artifacts, so that "engine 0.2.0 exists" has a precise, auditable meaning.

#### Acceptance Criteria

1. THE Engine_Release for version `V` SHALL comprise: the version-bump commit setting the workspace
   version to `V`; a release tag on that commit; Canonical_Builds of every admitted platform ×
   format pair at that tree; Published_Catalog admission of those bundles; and the
   Engine_Surface_Delta for `V`.
2. THE version-bump commit SHALL be the only change-site of the Engine_Version.
3. WHEN a Canonical_Build is admitted, THE catalog entry SHALL be keyed by
   `(platform, format, engine)` and SHALL carry the bundle's identity digest, authority, and
   evidence.
4. THE Published_Catalog SHALL admit at most one entry per key triple; a duplicate SHALL be a
   publication error, not a silent replacement.
5. THE Engine_Surface_Delta SHALL record provider kinds added, changed, or removed and
   definition-frontend surface changes for the release.
6. WHERE a build lacks canonical authority, THE bundle SHALL remain unpublishable exactly as
   today's admission rules require.

### Requirement 5: Resolution against the indication

**User Story:** As a deployment operator, I want `tkr` to obtain the engine my platform indicates,
so that what runs is what the platform declared, not whatever happens to be newest.

#### Acceptance Criteria

1. WHEN an installed `tkr` resolves a provisioner for a platform and format, THE resolution SHALL
   select the Published_Catalog entry whose engine equals the platform's Engine_Indication.
2. IF no entry exists for the indicated triple, THEN THE resolution SHALL refuse, naming the triple
   and the engines that are published for that platform and format.
3. WHEN a workspace source build is used instead of the catalog, THE Requirement 2 assertion SHALL
   be the equivalent gate.
4. THE upgrade boundary's monotonic rules, the binding gate's drift authority, and development
   builds' advisory, unpublished status SHALL be unchanged by resolution against the indication.

### Requirement 6: Completeness, tests, and documentation

**User Story:** As a maintainer, I want the versioning story enforced by tests and written down
once, so that it stays true after the people who decided it stop being the ones editing it.

#### Acceptance Criteria

1. THE implementation SHALL pass the workspace finishing bar with no Docker or live credentials in
   the default suite.
2. Property tests SHALL cover: descriptor admission (stable fields, counter rejection, exact-match
   indication assertion and refusal), evidence round-tripping with the engine field, and catalog
   key uniqueness and resolution.
3. THE versioning story — the layer table of keys, labels, indication, authority, and schema
   versions, and the release act — SHALL be documented as one operations/architecture document that
   the descriptor and catalog rustdoc reference.
4. WHEN this specification's tasks complete, THE `tasks.md` ledger SHALL carry DONE records
   reflecting the landed slices.
