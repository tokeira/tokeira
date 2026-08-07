# Requirements Document

## Introduction

This feature refines the Compose platform to a description-only model: a platform
describes its infrastructure and its services, and the provisioning framework (`tkp`)
owns everything about changing them. The refined Compose platform package consists of
the deployment definition, its companion content, a catalog descriptor, and one
exported entry-point function. Change management — evaluation, verification, planning,
confirmation, apply and destroy ordering, recorded state, module selection, writeback,
inspection projections, retarget refusal — lives in the framework, implemented once
for every platform.

Compose is the vehicle because it is the only platform on the catalog-driven bound-tkp
path today. The framework and substrate seams this spec changes
(`tokeira-provisioner-cli`, `tokeira-orchestrator`, `tokeira-compose`, `tokeira-aws`,
`tokeira-kinds`, `tokeira-build`) are changed only as far as Compose needs; migrating
the ECS, local, and EKS platforms onto the refined seams is follow-on work outside
this spec.

The target picture is the reference sketch at `reference/compose-idealized/` in this
spec directory (deliberately non-compiling, workspace-detached). Where this document
and the sketch disagree, this document wins.

## Glossary

- **Platform**: a named description of a deployment's infrastructure and services —
  for Compose: the definition, its companion content, a catalog descriptor, and an
  entry point. A platform performs no change management.
- **Framework**: the shared provisioning machinery in `tokeira-provisioner-cli` (the
  `tkp` lifecycle shell) together with the engine crates it drives. The framework
  invokes the platform, never the reverse.
- **Substrate**: the provider a platform fundamentally runs on. Compose's substrate is
  `ComposePlatform` in `tokeira-compose`: the Docker mechanics, the compose kind
  library, and the runtime reads.
- **Kind**: an author-visible resource type a definition can instantiate
  (`Service`, `LocalStateDir`, `ServerConfig`, `ObservabilityConfiguration`,
  `DsqlCluster`, `DynamoDbTable`).
- **Kind library**: a provider crate's exported set of kinds — input types, input
  validation, and realization mechanics.
- **Authoring vocabulary**: the set of kinds a platform's definitions may name — the
  substrate's kind library united with the platform's auxiliary selections.
- **Auxiliary selection**: an explicit, value-level choice of kinds from a
  non-substrate provider (`tokeira_aws::kinds::all()` or
  `tokeira_aws::kinds::select([...])`).
- **Definition frontend**: the evaluator for one definition format (`tokeira-tkd`,
  `tokeira-tkdp`), selected by format identifier and married to the platform by the
  framework.
- **Bound tkp**: the per-deployment provisioner executable assembled from one
  platform, one frontend, and the framework.
- **Catalog descriptor**: the `[package.metadata.tokeira.platform]` block in the
  platform package's `Cargo.toml`, declaring platform identity and default format.
- **Desired-source companion**: a file recorded and resolved beside the definition
  source (today `tokeirad.toml`), digested into content identity so edits surface as
  plannable changes and baseline realizations digest retained revision bytes.
- **Runtime read**: a live question about a running deployment answered from its
  substrate — for Compose: log streams and port mappings.
- **Verbs by presence**: the framework exposes an operator verb if and only if the
  composed platform registers the capability behind it; absent capability means an
  absent verb, not a stub answer.

## Target State

The refined Compose platform package (`platforms/compose`):

- contains `definition.tkd` (and `definition.tkdp`), the observability content
  (dashboards, alert rules, backend config templates) as files beside the
  definitions, the catalog descriptor, and a library exporting one entry-point
  function;
- the entry point declares the substrate and the auxiliary AWS kind selection, and
  nothing else;
- has exactly three direct dependencies: `tokeira-compose`, `tokeira-aws`,
  `tokeira-provisioner-cli`;
- defines no configuration types, no `iac::Resource` or `Module` adapters, no state
  stores, and no operation methods.

The framework:

- evaluates, verifies, plans, confirms, applies, destroys, selects modules, resolves
  and persists writeback, publishes inspection projections, and enforces retarget
  refusal — in one implementation shared by all platforms;
- marries the platform to the frontend at assembly; the platform is not generic over
  the frontend;
- surfaces the substrate's runtime reads as `logs` / `port-mappings` verbs by
  presence, validating service names against the evaluated definition.

The substrate (`tokeira-compose`):

- carries the compose kind library and Docker mechanics, as today;
- additionally carries the runtime reads (relocated from the platform's `ops.rs`);
- carries no deployment content: the observability templates, dashboards, and alert
  rules move out; the rendering machinery (parameter substitution, digests, consumer
  fencing) stays.

Deleted: `tokeira-kinds` (the engine-wide kind union and `verify_wiring`), the
platform's config twin (`config.rs`), the platform's adapter layer, the platform's
image inventory, and the platform-side `ops.rs`.

Retained unchanged: the catalog descriptor mechanism, definition co-location by the
frontend's conventional relative path, definition bytes staged as deployment data
(never compiled in), the recorded `{ platform, format, path }` triple, and the
bound-tkp double-entry verification of that triple.

Out of scope: migrating the ECS, local, and EKS platforms to the refined seams;
reshaping the definition's `DsqlMode` enum so invalid storage states are
unrepresentable (an optional follow-up — the cross-field rule moves into kind input
validation either way); an image build/mirror framework (Compose needs none; other
platforms' needs are their specs' business); and any change to definition-language
semantics or the `.tkd`/`.tkdp` frontends beyond the marriage seam.

## Evidence From Current Code

Current platform surface (what this spec removes from the platform):

- `platforms/compose/src/lib.rs:56` — `provisioner<F: DefinitionFrontend>(frontend)`:
  the platform receives and wraps the frontend.
- `platforms/compose/src/lib.rs:589` — `impl ProvisionerPlatform for
  ComposeProvisioner`, 18 methods; `scale` answers
  `Realization::NotApplicable` (`platforms/compose/src/lib.rs:804`).
- `platforms/compose/src/lib.rs:244` — `impl orchestrator::Deployment`, 11 callbacks,
  including AWS client registration into the provision context's extension map
  (`register_infra_extensions`, `platforms/compose/src/lib.rs:291`).
- `platforms/compose/src/lib.rs:66` (`ExecutionConfig`), `:104` (`ConcreteDeployment`),
  `:113` (`ConcreteModule`), `:151` (`SharedResource`) — the adapter layer
  re-presenting one realized graph to neighbouring traits.
- `platforms/compose/src/config.rs` — 222 lines mirroring the definition-authored
  configuration shape field for field.
- `platforms/compose/src/ops.rs:10` — `VALID_SERVICES: [&str; 5]`, a hard-coded copy
  of the definition's service set; `ops::platform()` re-reads `metadata.json` and
  threads the `state/compose-services.yaml` ledger path into log/port reads.
- `platforms/compose/src/images/mod.rs:54` — an image inventory constructed by
  nothing: `Deployment::images()` returns `Vec::new()`
  (`platforms/compose/src/lib.rs:283`).

Current kind union (what this spec deletes):

- `crates/tokeira-kinds/src/lib.rs:48` — the manually ordered engine-wide
  `KIND_NAMES`; `:61` — `EngineKind` with per-method delegating matches; `:155` —
  `verify_wiring` refusing at check what the platform cannot execute.

Current content placement (what this spec relocates):

- `crates/tokeira-compose/src/observability/mod.rs:338-408` — dashboards, alert
  rules, and templates `include_str!`-embedded in the substrate crate
  (`crates/tokeira-compose/{templates,dashboards,alerts}/`).

Binding architecture (what this spec retains, with one seam inverted):

- `platforms/compose/Cargo.toml:10` — the catalog descriptor
  (`id = "compose"`, `default-format = "tkd"`).
- `crates/tokeira-tkd/Cargo.toml:10` — the frontend descriptor
  (`default-relative-path = "definition.tkd"`).
- `apps/tkr/src/catalog.rs:291` — `PlatformCatalog::workspace_frontend`, the
  catalog-convention-plus-co-location join.
- `crates/tokeira-build/src/composition.rs:371` — `assemble_bound_provisioner`, the
  three-dependency generated package.
- `crates/tokeira-provisioner-cli/src/lib.rs:53` — `bound_provisioner_main!`,
  expanding today to `platform::provisioner(frontend())`.
- `crates/tokeira-provisioner-cli/src/bound.rs:18` — `BoundPlatform`, the
  double-entry verification of the recorded `{ platform, format }` pair.

Target picture: `reference/compose-idealized/` in this spec directory.

## Method Disposition Policy

Every element of the two platform-facing traits is accounted for. "Framework" means
the behaviour moves into the shared pipeline with no per-platform code; "Substrate
extension" means it becomes the opt-in runtime-reads capability; "Catalog/metadata"
means the framework reads it from the descriptor or `metadata.json`; "Capability"
means the framework keeps the operator surface as an opt-in declaration — Compose
declares none of these, so Compose presents no verb, and the capability remains for
platforms that have it (the local/ECS/EKS migrations decide their own declarations
in their own specs); "Retired" means the concept disappears from the model entirely.
A Capability disposition binds the refined framework contract not to preclude what
other platforms need.

### `ProvisionerPlatform` (crates/tokeira-provisioner-cli/src/lib.rs:197)

| Method | Disposition |
|---|---|
| `label` | Catalog/metadata (platform id) |
| `config_source` | Catalog/metadata (recorded definition triple) |
| `definition_format` | Catalog/metadata (descriptor `default-format`, recorded format) |
| `deployment_id` | Catalog/metadata (`metadata.json`) |
| `definition_check` | Framework (evaluate + verify, pure) |
| `retarget_check` | Framework (delegates to the frontend) |
| `log_stream` | Substrate extension |
| `port_mappings` | Substrate extension |
| `desired_snapshot` | Framework (realize desired manifests) |
| `recorded_state` | Framework (its own state document) |
| `infra_plan` | Framework |
| `infra_apply` | Framework |
| `deploy_plan` | Capability (workload plane; Compose declares none — its workload rides the infra universe, crates/tokeira-provisioner-cli/src/cli.rs:63 — so the verb aliases or is absent for Compose; platforms with a distinct workload plane keep it) |
| `deploy_apply` | Capability (as above) |
| `publish_inspection` | Framework (renders from desired manifests) |
| `infra_destroy` | Framework |
| `infra_destroy_selected` | Framework |
| `scale` | Capability (scale dimensions; Compose declares none, so no verb and no `NotApplicable` stub; platforms with scale dimensions keep it) |

### `orchestrator::Deployment` (crates/tokeira-orchestrator/src/lib.rs:346)

| Method | Disposition |
|---|---|
| `remote_state_module` | Framework (the engine bootstraps its own state) |
| `infra_modules` | Framework (from the verified graph) |
| `services` | Capability (workload inventory; empty on Compose today — platforms with a distinct workload plane declare theirs) |
| `images` | Capability (image inventory; Compose declares none, Requirement 8 — ECS's inventory is live, called from four tkr sites) |
| `required_namespaces` | Framework (from the verified graph) |
| `register_infra_extensions` | Framework (execution stacks constructed from the wired declarations) |
| `register_deploy_extensions` | Framework (constructed from declared capabilities; Compose declares none) |
| `create_infra_store` | Framework (the engine owns its storage) |
| `create_deploy_store` | Framework (as above) |
| `hydrate_config` | Framework (identity on Compose today; where a platform's capabilities need config rehydration from recorded state — as ECS does — the framework applies its recorded outputs to the evaluated config) |
| `collect_writeback` | Framework (resolves declared writeback against its own state) |

### `orchestrator::Ops` (crates/tokeira-orchestrator/src/lib.rs:466)

Not a Compose surface, accounted for completeness: the legacy tkr path's
composite of runtime reads (`logs`, `port_mappings`), scaling (`scale_up`,
`scale_down`, `desired_replicas`), and a `valid_services()` inventory —
implemented by the ECS and local platforms only (`platforms/ecs/src/lib.rs:354`,
`platforms/local/src/lib.rs:307`), never by Compose. Untouched by this spec. The
local/ECS migration specs are expected to decompose it into the declaration-gated
capability surface this spec introduces, after which it retires with the legacy
path.

### `tokeira-kinds` exports (crates/tokeira-kinds/src/lib.rs)

| Export | Disposition |
|---|---|
| `Provider` enum | Retired (providers are wired by value, not enumerated globally) |
| `KIND_NAMES` | Retired (the vocabulary is the wired union, computed at composition) |
| `EngineKind` + delegating impls | Retired |
| `provider_of` / `decode` routing | Framework (name-to-kind resolution over the wired union) |
| `kind_functions` | Framework (constructed from the wired kind libraries) |
| `verify_wiring` | Retired (an unwired kind is simply not in the vocabulary; Requirement 3.3) |

## Requirements

### Requirement 1: The platform surface is description only

**User Story:** As a platform maintainer, I want the Compose platform package to
contain nothing but description, so that reading it tells me what a Compose
deployment is — and changing how deployments are provisioned never requires touching
a platform.

#### Acceptance Criteria

1. THE Compose platform package SHALL consist of the deployment definitions, the
   observability content files, the catalog descriptor, and a library crate
   exporting one entry-point function.
2. THE entry-point function SHALL declare the substrate and the auxiliary AWS kind
   selection, and SHALL accept no arguments.
3. THE platform crate SHALL NOT contain implementations of planning, applying,
   destroying, state persistence, module selection, writeback resolution,
   inspection publication, confirmation, or definition evaluation.
4. THE platform crate SHALL have exactly three direct dependencies:
   `tokeira-compose`, `tokeira-aws`, and `tokeira-provisioner-cli`.
5. WHEN the framework operates a Compose deployment THEN every invocation of
   platform code SHALL be either the entry-point function or capability code
   supplied by the substrate.

### Requirement 2: The definition owns the configuration shape

**User Story:** As a definition author, I want the configuration shape authored once
in the definition, so that the shape I edit is the shape the system admits — with no
hand-synchronised Rust mirror to drift.

#### Acceptance Criteria

1. THE platform crate SHALL define no types mirroring definition-authored
   configuration shapes.
2. WHEN evaluation completes THEN the framework SHALL hold the evaluated
   configuration, and no platform code SHALL receive a decoded configuration value.
3. WHEN a `Service` kind input carries an empty image reference or zero replicas or
   a zero published port THEN kind input validation SHALL refuse with an error
   located at the authoring site.
4. WHEN a `DsqlCluster` kind input carries managed mode together with an endpoint or
   ARN, or preexisting mode without both endpoint and ARN, or an empty region THEN
   kind input validation SHALL refuse with an error located at the authoring site.
5. WHEN a definition's evaluated configuration violates no kind input rule THEN the
   framework SHALL NOT invoke any platform-supplied configuration validator, because
   none exists.

### Requirement 3: The authoring vocabulary is the substrate plus explicit selections

**User Story:** As a platform maintainer, I want the vocabulary my definitions may
use to be exactly what the entry point declares, so that wiring is one positive
declaration instead of an engine-wide union narrowed after the fact.

#### Acceptance Criteria

1. WHEN the entry point names the substrate THEN the substrate's complete kind
   library SHALL be part of the authoring vocabulary with no separate wiring
   declaration.
2. THE auxiliary AWS vocabulary SHALL be selected explicitly at the entry point,
   as either the provider's complete kind export or a named subset.
3. WHEN a definition names a kind outside the composed vocabulary THEN
   `definition check` SHALL refuse with an unknown-kind error located at the
   authoring site.
4. WHEN two wired kind libraries export the same kind name THEN composition SHALL
   fail, naming both providers and the colliding name.
5. THE engine SHALL contain no global kind inventory: kind-name resolution SHALL be
   computed from the composed vocabulary, and no check SHALL exist that refuses a
   kind as "known but unwired".

### Requirement 4: Runtime reads are substrate capability, surfaced by presence

**User Story:** As an operator, I want `tkp logs` and `tkp port-mappings` to exist
because Compose can answer them, so that capability discovery is honest and absent
capabilities are absent verbs rather than stub refusals.

#### Acceptance Criteria

1. THE substrate crate SHALL implement the framework's runtime-reads extension —
   log streams and port mappings — beside `ComposePlatform`.
2. WHEN the composed platform registers runtime reads THEN the framework SHALL
   expose the `logs` and `port-mappings` verbs; WHEN it registers none THEN the
   framework SHALL NOT present those verbs.
3. WHEN an operator names a service for a runtime read THEN the framework SHALL
   validate the name against the evaluated definition's services and SHALL refuse
   unknown names listing the definition's actual services.
4. THE runtime reads SHALL receive deployment identity (name and directory) from
   the framework, and SHALL NOT re-read deployment metadata or require any
   change-management artifact to answer.
5. THE platform crate SHALL contain no hard-coded service inventory.

### Requirement 5: Observability content is platform description

**User Story:** As a platform maintainer, I want the dashboards, alert rules, and
backend config templates to live with the platform as files, so that content edits
are visible, diffable, per-deployment plannable changes instead of substrate-crate
releases.

#### Acceptance Criteria

1. THE observability content (backend config templates, Grafana dashboards, alert
   rules) SHALL reside in the platform package beside the definitions.
2. THE substrate crate SHALL embed no deployment content; its rendering machinery
   SHALL accept the content it renders as input.
3. WHEN a deployment is created or its definition revision is recorded THEN the
   observability content SHALL be recorded with the definition as desired-source
   companions.
4. WHEN a realization resolves observability content THEN it SHALL resolve against
   the interpreted definition source's directory, so a baseline realization from a
   retained revision digests that revision's content.
5. WHEN observability content changes THEN the content digest fencing its consumer
   services SHALL change, surfacing the edit as a plannable difference.

### Requirement 6: The framework owns change management, once

**User Story:** As an engine maintainer, I want evaluation, verification, and the
provisioning lifecycle implemented once in the framework, so that platforms cannot
diverge in behaviour they should share and a new platform adds no pipeline code.

#### Acceptance Criteria

1. THE framework SHALL evaluate the recorded definition through the frontend
   selected by the recorded format, using the composed vocabulary, in one
   implementation shared by all platforms on the bound path.
2. THE framework SHALL implement plan, apply, destroy, selected-resource destroy,
   desired-manifest snapshot, recorded-state read, inspection publication,
   definition check, and retarget check with no per-platform method behind any of
   them, per the `ProvisionerPlatform` disposition table.
3. THE framework SHALL derive module structure, namespaces, and writeback
   resolution from the verified graph and its own recorded state, per the
   `Deployment` disposition table.
4. THE framework SHALL construct provider execution stacks from the entry point's
   wired declarations, and the platform SHALL perform no extension-map
   registration.
5. WHEN apply or destroy requires Docker and the daemon is unreachable THEN the
   operation SHALL fail with the substrate's operator-facing story; WHEN plan
   encounters the same condition THEN the plan SHALL complete and carry the issue
   as a finding.
6. WHERE an operator surface is a capability (workload plan/apply, scale, image
   verbs, runtime reads) THE framework SHALL gate it on the composed platform's
   declaration — absent for platforms that declare none, fully available to
   platforms that declare it, with no stub answers in either case.

### Requirement 7: The binding architecture is retained; the marriage seam inverts

**User Story:** As an operator, I want the durable association between a deployment's
definition bytes and the engine interpreting them preserved exactly, while the
platform stops being a construction site for the frontend.

#### Acceptance Criteria

1. THE catalog SHALL discover platforms by the `[package.metadata.tokeira.platform]`
   descriptor and SHALL resolve definitions by joining the platform package
   directory with the frontend's conventional relative path, as today.
2. WHEN a deployment is created THEN the definition bytes SHALL be staged into the
   deployment directory as data — never compiled into the executable — and
   `{ platform, format, path }` SHALL be recorded in deployment metadata.
3. WHEN a bound tkp operates a deployment THEN it SHALL refuse a deployment whose
   recorded platform/format pair differs from the pair it was built as.
4. THE generated entry of a bound tkp SHALL hand the platform declaration and the
   frontend to the framework, and the framework SHALL perform the marriage.
5. THE platform crate SHALL NOT be generic over, construct, or invoke a definition
   frontend.

### Requirement 8: Compose declares no image inventory

**User Story:** As a platform maintainer, I want Compose to carry no image
build/mirror inventory, so that the platform declares only concepts Compose actually
has.

#### Acceptance Criteria

1. THE Compose platform SHALL declare no image inventory, and the framework SHALL
   present no image verbs for a composed platform that declares none.
2. WHEN a Compose service is reconciled THEN its image reference SHALL be taken
   from the evaluated definition configuration, as today.
3. WHEN a locally-built image named by the definition is absent from the Docker
   daemon THEN the operation SHALL refuse with the existing remediation naming the
   image.

### Requirement 9: Operator-observable behaviour is preserved

**User Story:** As an operator with existing Compose deployments, I want the refined
platform to provision exactly what today's does, so that the refactor is invisible
except where this spec says otherwise.

#### Acceptance Criteria

1. WHEN the reference definition is evaluated under each storage mode (in-memory,
   DSQL managed, DSQL preexisting) THEN the realized module and resource graph
   SHALL match today's: `local_state`, `runtime`, and `observability` always; `dsql`
   exactly when DSQL storage is selected.
2. WHEN services consuming rendered configuration are realized THEN the content
   digest environment fencing (`TOKEIRA_CONFIG_DIGEST`,
   `TOKEIRA_SERVER_CONFIG_DIGEST`, `TOKEIRA_CONFIG`) and the server-config mount
   SHALL be preserved.
3. WHEN `definition check` runs THEN it SHALL remain pure: no provider access, no
   state or config side effects in the deployment directory.
4. WHEN the inspection projection is published THEN `docker-compose.yml` SHALL
   render deterministically from desired state and SHALL never be read back as an
   input.
5. WHEN a module is named for destroy THEN the selection SHALL expand to
   dependants; WHEN named for plan or apply THEN it SHALL expand to prerequisites —
   as today.
6. WHEN DSQL storage is selected THEN the recorded writeback declarations
   (`infrastructure.storage`, `infrastructure.dsql.*`) SHALL resolve from applied
   outputs to the same keys and values as today.
