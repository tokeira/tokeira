# Requirements Document

## Introduction

The Compose platform is description only: a platform describes its
infrastructure and its services, and the provisioning framework (`tkp`) owns
everything about changing them. The Compose platform package consists of the
deployment definitions and their parts, the companion observability content,
the platform's own kinds, a catalog descriptor, and one exported entry-point
function. Change management — admission, evaluation, verification, planning,
confirmation, apply and destroy ordering, recorded state, module selection,
writeback resolution and persistence, inspection projections, retarget
refusal — lives in the framework, implemented once for every platform on the
bound path.

Definitions may span multiple documents: a root that declares parts, each
part a file beside it, evaluated with the host language's own module
semantics in both frontends. Config revisions retain the whole definition
set, and the retarget gate compares sets.

The reference sketches at `reference/` in this spec directory carry
direction that is not yet contract (the ECS onboarding shape, the tkdp
vocabulary split and `platform` model part); where a sketch and this
document disagree, this document wins.

## Glossary

- **Platform**: a named description of a deployment's infrastructure and
  services — for Compose: the definitions and parts, companion content, the
  platform's own kinds, a catalog descriptor, and an entry point. A platform
  performs no change management and contains no binary; `tkp` is the binary
  and invokes the platform.
- **Framework**: the shared provisioning machinery — the `tkp` lifecycle
  shell in `tokeira-provisioner-cli` driving the engine crates, speaking the
  contract types in `tokeira-platform`. The framework invokes the platform,
  never the reverse.
- **Platform definition**: the platform package's integration layer. Its one
  entry point constructs a `PlatformDeclaration` by selecting authoring
  namespaces and integrating the operational capabilities supplied by the
  platform implementation.
- **Platform implementation**: the crate-level implementation of a
  platform's infrastructure and services, including their concrete
  resources, service realization, extension registration, live operations,
  reachability, and service-manifest application. For Compose this machinery
  is supplied by `tokeira-compose` and integrated by `platforms/compose`.
- **Platform declaration**: the value handed to the framework. It directly
  exposes authoring namespaces, optional live ops, a reachability seam, and
  the platform implementation; there is no intermediate provider export.
- **Authoring namespace**: the frontend-only description of one normalized
  crate dependency: its normalized crate name, advertised kind names,
  optional authoring defaults, and decoder. It introduces resources to a
  definition and has no execution responsibility.
- **Kind**: an author-visible resource type a definition can instantiate
  (`Service`, `LocalStateDir`, `ServerConfig`,
  `ObservabilityConfiguration`, `DsqlCluster`, `DynamoDbTable`). A
  `Kind<R>` decodes author input and realizes exactly one `R`; it carries no
  lifecycle commands.
- **Realized resource**: the concrete value produced by a kind. An
  infrastructure `Resource` owns its validation, outputs, desired evidence,
  and create/update/delete/describe lifecycle; a service `Resource` owns its
  validation, outputs, desired service model, and manifest realization for
  the deploy engine.
- **Authoring surface**: the union of kind names advertised by the
  declaration's authoring namespaces; a definition may name exactly these
  kinds.
- **Ops**: the platform's optional surface over a running deployment — log
  streams, port mappings, and scale — carried directly by the platform
  declaration. The framework surfaces the corresponding verbs by presence.
- **Deployment namespace**: a target-platform tenancy unit requested by an
  evaluated deployment graph (for example `default`). It is distinct from
  an authoring namespace and may participate in execution where the target
  platform has such a concept.
- **Definition frontend**: the evaluator for one definition format
  (`tokeira-tkd`, `tokeira-tkdp`), selected by format identifier and married
  to the platform by the framework.
- **Definition part**: one document of a multi-document definition, resolved
  beside the root by name: `networking.tkd` for `mod networking;`,
  `networking.tkdp` for `import networking`.
- **Definition set**: the root plus the parts an evaluation resolves;
  identified by `sha256-set-v1` over the root and served parts in
  first-request order.
- **Admission**: the once-per-command gate producing `Admitted` — verified
  deployment metadata plus deployment identity — before any verb logic runs.
- **Bound tkp**: the per-deployment provisioner executable assembled from one
  platform, one frontend, and the framework.
- **Catalog descriptor**: the `[package.metadata.tokeira.platform]` block in
  the platform package's `Cargo.toml`, declaring platform identity and
  default format.
- **Desired-source companion**: a file recorded and resolved beside the
  definition source (today `tokeirad.toml`), digested into content identity
  so edits surface as plannable changes.

## Target State

The Compose platform package (`platforms/compose`):

- contains the `.tkd` definition set (`deployment.tkd`, `platform.tkd`,
  `observability.tkd`) and `definition.tkdp`, the observability content
  (`observability/`: dashboards, alert rules, backend config templates) as
  files beside the definitions, the platform's own observability kind, the
  catalog descriptor, and a library exporting one entry-point function;
- the entry point directly constructs the `PlatformDeclaration`: the
  Compose, platform-owned observability, and AWS authoring namespaces; the
  Docker ops and reachability capabilities; and the Compose platform
  implementation; construction is pure;
- depends on the implementation/resource crates it wires
  (`tokeira-compose`, `tokeira-aws`) and
  the contract/machinery crates its own kind needs (`tokeira-platform`,
  `tokeira-iac`) — never on the framework shell;
- defines no configuration types, no state stores, and no operation methods.

The framework:

- admits once per command, then evaluates, verifies, plans, confirms,
  applies, destroys, selects modules, resolves **and persists** writeback,
  publishes inspection projections, and enforces retarget refusal — one
  implementation shared by every platform on the bound path;
- marries the platform to the frontend at assembly; the platform is not
  generic over the frontend;
- surfaces the platform's ops as `logs` / `port-mappings` / `scale` verbs by
  presence, validating service names against the evaluated definition;
- probes the platform substrate before mutating verbs: an unreachable substrate
  **blocks** plan (the plan plans nothing; the issue is the outcome's only
  content) and refuses apply and destroy.

The platform implementation crate (`tokeira-compose`):

- owns the Compose infrastructure resources and services, the kinds that
  realize them, the authoring namespace that introduces those kinds, Docker
  ops (logs, port mappings, scale via Compose service scaling), the
  reachability probe, extension registration, and deployment-scoped
  `ComposePlatform` construction;
- exports no preassembled declaration or provider bundle; the platform
  definition integrates the implementation's individual pieces;
- carries no deployment content: rendering machinery (parameter
  substitution, digests, consumer fencing) takes content as input.

Definitions may declare parts; the frontends evaluate them with the host
language's own semantics (Requirement 10). Config revisions retain the
definition set and the retarget gate compares sets (Requirement 11).

Absent from the engine: `tokeira-kinds` (no global kind inventory exists),
`ProviderExport`, namespace-owned execution constructors, platform-side
pipeline traits, platform config twins, and the local platform's provisioner
(`platforms/local` describes; its bound path arrives with its own
onboarding).

## Evidence From Current Code

The contract as adopted, by seam:

- `crates/tokeira-platform/src/declaration.rs` — `PlatformDeclaration {
  namespaces, ops, execution, implementation }`; `Ops` (log stream, port
  mappings, scale — required, undefaulted); `PlatformExecution`
  (probe-only, blocked-plan semantics documented); and
  `PlatformIntegration` (engine extension registration and service-platform
  construction, receiving deployment identity but no namespace metadata).
- `crates/tokeira-platform/src/definition.rs` — the frontend-only
  `Namespace { name, kinds, defaults, decode }`, definition source and part
  resolution, invocation-bound resource realization, and `sha256-set-v1`
  definition-set identity.
- `crates/tokeira-platform/src/kind.rs` — `Kind<R>::realize`, the
  heterogeneous `DecodedKind` storage boundary, and the single-owner
  `RealizedResource::{Infra, Service}` hand-off.
- `crates/tokeira-iac/src/lib.rs` and
  `crates/tokeira-deploy-engine/src/service.rs` — concrete infrastructure
  resources and services own type identity, validation, declared outputs,
  desired/runtime representation, and their respective engine behaviour.
- `platforms/compose/src/lib.rs` — the entry point directly constructs the
  declaration from the Compose, observability, and AWS authoring namespaces
  and the Compose operational implementation.
- `crates/tokeira-compose/src/execution.rs` — `ComposeExecution` probes
  Docker; `ComposeIntegration` owns extension registration and constructs
  the deployment-scoped `ComposePlatform` that applies service manifests.
- `crates/tokeira-provisioner-cli/src/platform.rs` — admission (`Admitted`,
  once per command, double-entry verification of the recorded pair);
  `src/engine.rs` — the verbs (plan, apply, destroy, selected destroy,
  deploy plan/apply, desired snapshot, recorded state, retarget check) over
  `DescribedDeployment` (`src/described.rs`), the one
  `orchestrator::Deployment` on the bound path, delegating extension
  registration to the platform implementation without passing authoring
  namespaces; `src/lib.rs` —
  `run_bound_provisioner(platform, format, declaration, frontend)` and
  writeback persistence into the server config document.
- `crates/tokeira-orchestrator/src/lib.rs` — infrastructure and deploy
  engines consume their respective realized values; deploy construction
  loads the recorded `InfraState` into `ServiceContext` as the operator-made
  phase hand-off.
- `crates/tokeira-tkd/src/{parts,subset,eval}.rs` — the `.tkd` parts
  mechanism; `crates/tokeira-tkdp/src/{frontend,preflight,program,runner}.rs`
  — the `.tkdp` parts mechanism over registered Monty modules, with the
  facade as a genuine `tokeira` module.
- `Cargo.toml:153` — the Monty fork pin (`github.com/iw/monty`), one patch
  atop the previously pinned upstream revision, adding embedder-registered
  source modules; retire condition recorded beside it and in `deny.toml`.
- `crates/tokeira-provisioner-cli/src/config_history.rs` — definition-set
  retention (root, sidecar-listed parts, server config) and set-restoring
  revert.

## Requirements

### Requirement 1: The platform surface is description only

**User Story:** As a platform maintainer, I want the Compose platform
package to contain nothing but description, so that reading it tells me what
a Compose deployment is — and changing how deployments are provisioned never
requires touching a platform.

#### Acceptance Criteria

1. THE Compose platform package SHALL consist of the deployment definitions
   (and their parts), the observability content files, the platform's own
   kinds, the catalog descriptor, and a library crate exporting one
   entry-point function.
2. THE entry-point function SHALL accept no arguments and directly construct
   one `PlatformDeclaration`.
3. THE entry-point function SHALL perform no filesystem, network, or Docker
   access.
4. THE platform declaration SHALL directly expose its authoring namespaces
   and operational capabilities.
5. THE platform declaration SHALL carry the platform implementation that
   owns engine extension registration and service-manifest application.
6. THE platform crate SHALL NOT contain implementations of planning,
   applying, destroying, state persistence, module selection, writeback
   resolution, inspection publication, confirmation, or definition
   evaluation.
7. THE platform crate SHALL NOT depend on the framework shell.
8. WHEN the framework operates a Compose deployment, THE framework SHALL
   invoke only the entry-point function, a declared kind, or a capability
   carried directly by the platform declaration.
9. THE platform contract SHALL NOT define `ProviderExport` or require a
   provider-bundling function between an implementation crate and a platform
   declaration.

### Requirement 2: The definition owns the configuration shape

**User Story:** As a definition author, I want the configuration shape
authored once in the definition, so that the shape I edit is the shape the
system admits — with no hand-synchronised Rust mirror to drift.

#### Acceptance Criteria

1. THE platform crate SHALL define no types mirroring definition-authored
   configuration shapes.
2. WHEN evaluation completes, THE framework SHALL hold the evaluated
   configuration as a located value.
3. THE platform declaration and platform implementation SHALL NOT receive a
   decoded definition configuration value.
4. WHEN a realized `Service` carries an empty image reference, zero replicas,
   or a zero published port, THE resource validation seam SHALL refuse it
   with an error located at the authoring site.
5. WHEN a realized `DsqlCluster` carries managed mode together with an
   endpoint or ARN, preexisting mode without both endpoint and ARN, or an
   empty region, THE resource validation seam SHALL refuse it with an error
   located at the authoring site.
6. WHEN a definition's evaluated configuration violates no resource input
   rule, THE framework SHALL NOT invoke any platform-supplied configuration
   validator, because none exists.

### Requirement 3: Namespaces introduce kinds; resources own realization

**User Story:** As a platform maintainer, I want the vocabulary my
definitions may use to be exactly the authoring namespaces the entry point
declares, while the resulting resources own engine behaviour, so that
frontend integration stays concise and execution responsibility stays with
the platform implementation.

#### Acceptance Criteria

1. THE `PlatformDeclaration` SHALL expose its authoring namespaces directly
   as `Vec<Namespace>`.
2. THE `Namespace` contract SHALL contain only its normalized crate name,
   advertised kind names, optional authoring defaults, and decoder.
3. THE `Namespace` contract SHALL NOT contain an engine, lifecycle command,
   extension-registration callback, or provider constructor.
4. WHEN a frontend resolves an authored kind, THE frontend SHALL resolve it
   only through the namespaces listed by the platform declaration.
5. IF a namespace advertises a kind name that its decoder does not accept,
   THEN THE frontend SHALL refuse the definition as an inconsistent
   namespace declaration.
6. IF two declared namespaces advertise the same kind name, THEN THE
   platform binding SHALL fail naming both namespaces and the colliding kind.
7. THE `Kind<R>` contract SHALL do nothing beyond realizing one `R` from its
   authored input and `PlacementContext`.
8. THE realized infrastructure `Resource` or deploy `Service` SHALL own its
   resource type, validation, declared outputs, desired representation, and
   engine lifecycle or manifest behaviour.
9. WHEN a heterogeneous definition graph erases a `Kind<R>`, THE realization
   container SHALL assign its result to `RealizedResource::Infra` or
   `RealizedResource::Service` from `R`'s engine trait bound.
10. WHEN a definition names a kind outside the declared namespaces,
    `definition check` SHALL refuse with an unknown-kind error located at the
    authoring site.
11. THE engine SHALL contain no global kind inventory.
12. THE framework SHALL NOT refuse a kind as "known but unwired".

### Requirement 4: Ops are platform capability, surfaced by presence

**User Story:** As an operator, I want `tkp logs`, `tkp port-mappings`, and
`tkp scale` to exist because the platform can answer them, so that
capability discovery is honest and absent capabilities are absent verbs
rather than stub refusals.

#### Acceptance Criteria

1. THE `PlatformDeclaration` SHALL carry its ops surface directly as an
   optional value implementing log streams, port mappings, and scale.
2. THE `Ops` contract SHALL require scale without a default implementation.
3. WHEN the declared platform carries ops, THE framework SHALL expose the
   `logs`, `port-mappings`, and `scale` verbs.
4. WHEN the declared platform carries no ops, THE framework SHALL NOT
   present the `logs`, `port-mappings`, or `scale` verbs.
5. WHEN an operator names a service for an ops verb, THE framework SHALL
   validate the name against the evaluated definition's services.
6. WHEN an operator names a service absent from the evaluated definition,
   THE framework SHALL refuse it while listing the definition's actual
   services.
7. THE ops surface SHALL receive deployment identity (name and directory)
   from the framework.
8. THE ops surface SHALL NOT re-read deployment metadata to answer.
9. THE platform crate SHALL contain no hard-coded service inventory.
10. WHEN `tkp scale` changes Compose service capacity, THE framework
    SHALL re-stamp recorded state after the platform applies the change.

### Requirement 5: Observability content is platform description

**User Story:** As a platform maintainer, I want the dashboards, alert
rules, and backend config templates to live with the platform as files, so
that content edits are visible, diffable, per-deployment plannable changes
instead of platform-implementation-crate releases.

#### Acceptance Criteria

1. THE observability content (backend config templates, Grafana dashboards,
   alert rules) SHALL reside in the platform package beside the definitions.
2. THE platform implementation crate SHALL embed no deployment content.
3. EACH platform SHALL own its content tree — Compose's under
   `platforms/compose/observability/`, ECS's under
   `platforms/ecs/observability/`.
4. THE rendering machinery SHALL accept the content it renders as input.
5. THE kind that renders the content (`ObservabilityConfiguration`) SHALL be
   owned by the platform and introduced through the platform's authoring
   namespace.
6. WHEN a realization resolves observability content, THE resource SHALL
   resolve against the interpreted definition source's directory, so a
   baseline realization from a retained revision digests that revision's
   content.
7. WHEN observability content changes, THE content digest fencing its
   consumer services SHALL change, surfacing the edit as a plannable
   difference.
8. EACH platform's content tree SHALL be validated by that platform's own
   tests against the shared dashboard and alert-rule style contracts.

### Requirement 6: The framework owns change management, once

**User Story:** As an engine maintainer, I want admission, evaluation,
verification, and the provisioning lifecycle implemented once in the
framework, so that platforms cannot diverge in behaviour they should share
and a new platform adds no pipeline code.

#### Acceptance Criteria

1. THE framework SHALL admit once per command, producing verified deployment
   identity and metadata before any verb logic runs.
2. IF a deployment's recorded platform/format pair disagrees with the
   binary's pair, THEN THE framework SHALL refuse it at admission.
3. THE framework SHALL evaluate the recorded definition through the frontend
   selected by the recorded format, using the declared authoring namespaces
   and resolving parts beside the interpreted document, in one implementation
   shared by all platforms on the bound path.
4. THE framework SHALL implement plan, apply, destroy, selected-resource
   destroy, deploy plan, deploy apply, desired-manifest snapshot,
   recorded-state read, definition check, and retarget check with no
   per-platform method behind any of them.
5. WHEN an engine initializes its operation context, THE described
   deployment SHALL delegate extension registration to the platform
   implementation carried by the declaration.
6. THE platform implementation SHALL receive deployment identity and the
   respective engine context when registering extensions.
7. THE platform implementation's extension-registration seams SHALL NOT
   receive authoring namespace metadata or definition configuration blocks.
8. WHEN the platform reachability probe reports the substrate unreachable,
   THE plan SHALL be blocked with no planned changes.
9. WHEN a plan is blocked by platform reachability, THE plan outcome SHALL
   contain only the platform issue.
10. WHEN the platform reachability probe reports the substrate unreachable,
   THE framework SHALL refuse apply and destroy on the same issue.
11. WHEN an apply, upgrade, revert, or rollback resolves writeback, THE
   framework SHALL persist the resolved entries into the deployment's server
   configuration document before re-stamping recorded state.
12. WHEN definition realization completes, THE realization container SHALL
    hand infrastructure resources only to the infrastructure engine and
    services only to the deploy engine.
13. THE deploy verbs SHALL reconcile the definition's realized services
    through the deploy engine and the service platform supplied by the
    platform implementation.
14. WHEN an operator applies infrastructure, THE framework SHALL NOT deploy
    services as part of that operation.
15. WHEN an operator subsequently plans or applies services, THE deploy
    engine SHALL load the persisted `InfraState` into `ServiceContext` as the
    data hand-off from the infrastructure phase.
16. WHEN a `PlatformDeclaration` includes the AWS namespace, THE described
    deployment SHALL register exactly one deployment-scoped `AwsClients`
    bundle before platform-specific extension registration.
17. WHEN the framework constructs `AwsClients`, THE platform region SHALL
    resolve from authored `aws.region` when present and otherwise from the AWS
    SDK provider chain.
18. WHEN an AWS resource selects a region, THE resource lifecycle client
    SHALL use that region in precedence to the platform region.

### Requirement 7: The binding architecture is retained; the framework performs the marriage

**User Story:** As an operator, I want the durable association between a
deployment's definition bytes and the engine interpreting them preserved
exactly, while the platform stops being a construction site for the
frontend.

#### Acceptance Criteria

1. THE catalog SHALL discover platforms by the
   `[package.metadata.tokeira.platform]` descriptor.
2. THE catalog SHALL resolve definitions by joining the platform package
   directory with the frontend's conventional relative path.
3. WHEN a deployment is created, THE framework SHALL stage the definition
   bytes into the deployment directory as data, never compile them into the
   executable.
4. WHEN a deployment is created, THE framework SHALL record
   `{ platform, format, path }` in deployment metadata.
5. THE generated entry of a bound tkp SHALL hand the platform declaration
   and the frontend to the framework through
   `run_bound_provisioner(platform, format, declaration, frontend)`.
6. WHEN the bound entry invokes `run_bound_provisioner`, THE framework SHALL
   marry the platform declaration to the selected frontend.
7. THE platform crate SHALL NOT be generic over, construct, or invoke a
   definition frontend.

### Requirement 8: Compose declares no image inventory

**User Story:** As a platform maintainer, I want Compose to carry no image
build/mirror inventory, so that the platform declares only concepts Compose
actually has.

#### Acceptance Criteria

1. THE Compose platform SHALL declare no image inventory.
2. WHEN a platform declares no image inventory, THE framework SHALL present
   no image verbs for it.
3. WHEN a Compose service is reconciled, THE deploy engine SHALL take its
   image reference from the evaluated definition configuration.
4. WHEN a locally-built image named by the definition is absent from the
   Docker daemon, THE operation SHALL refuse with the existing
   remediation naming the image.

### Requirement 9: Operator-observable behaviour is preserved

**User Story:** As an operator with existing Compose deployments, I want the
refined platform to provision exactly what the pre-refinement one did, so
that the refactor is invisible except where this spec says otherwise.

#### Acceptance Criteria

1. WHEN the reference definition is evaluated under each storage mode
   (in-memory, DSQL managed, DSQL preexisting), THE realized module and
   resource graph SHALL match: `local_state`, `runtime`, and `observability`
   always; `dsql` exactly when DSQL storage is selected.
2. WHEN services consuming rendered configuration are realized, THE
   realized services SHALL preserve the content-digest environment fencing
   (`TOKEIRA_CONFIG_DIGEST`, `TOKEIRA_SERVER_CONFIG_DIGEST`,
   `TOKEIRA_CONFIG`) and the server-config mount.
3. WHEN `definition check` runs, THE framework SHALL remain pure: no
   platform-substrate access and no state or config side effects in the
   deployment directory.
4. WHEN the inspection projection is published, `docker-compose.yml` SHALL
   render deterministically from desired state.
5. THE framework SHALL NOT read `docker-compose.yml` back as an input.
6. WHEN a module is named for destroy, THE selection SHALL expand to
   dependants.
7. WHEN a module is named for plan or apply, THE selection SHALL expand to
   prerequisites.
8. WHEN DSQL storage is selected, THE recorded writeback declarations
   (`infrastructure.storage`, `infrastructure.dsql.*`) SHALL resolve from
   applied outputs to the same keys and values as before the refinement.
9. WHEN DSQL writeback declarations resolve, THE framework SHALL persist
   them per Requirement 6.11.

### Requirement 10: Definitions may span multiple documents

**User Story:** As a definition author, I want to split a definition into a
root and parts — one file per module of the deployment — with each
frontend's host language supplying the module semantics, so that a large
definition reads as a set of focused documents instead of one monolith.

#### Acceptance Criteria

1. WHEN a `.tkd` root declares `mod name;`, THE frontend SHALL resolve
   `name.tkd` beside the root and evaluate it as a part: namespaced calls
   (`name::function(…)`), `pub`-gated exports, part scopes seeing their own
   then the root's types, and wiring flowing through the root.
2. THE `.tkd` frontend SHALL NOT permit a part to reference another part.
3. WHEN a `.tkdp` root imports a name the part resolver serves, THE
   frontend SHALL register the part as a genuine Python module: its own
   namespace, qualified member access, imports resolving transitively
   (parts may import parts), and traceback frames carrying the part's own
   file name at original positions.
4. THE `.tkdp` facade SHALL be a genuine registered `tokeira` module with one
   set of class identities shared by the root and every part.
5. WHEN a `.tkdp` file executes `from tokeira import …`, THE sandbox SHALL
   execute it as a real import.
6. WHEN a `.tkdp` import is not served by the part resolver, THE sandbox SHALL
   fall through to the sandbox runtime — a built-in module, or a runtime
   module-not-found error at the import site.
7. THE frontends SHALL refuse, by name: an inline `mod` body, a part
   declaring a part, a private part function called from the root, a part
   shadowing a root type (`.tkd`); a dotted or relative import, a plain
   import shadowed by the file's own binding (with the from-form as the
   stated remedy), and an import cycle among parts, naming the cycle path
   (`.tkdp`).
8. THE evaluation SHALL record the definition set — root plus served parts
   in first-request order — under the `sha256-set-v1` identity.
9. THE `.tkdp` part mechanism SHALL stand on the pinned Monty fork's
   embedder-registered source modules.
10. THE Monty fork pin SHALL carry its retire condition beside the pin.

### Requirement 11: Config revisions retain the definition set

**User Story:** As an operator, I want a config revision to capture the
whole definition set my apply ran with, so that reverts restore what I
actually had and the retarget gate compares what actually changed.

#### Acceptance Criteria

1. WHEN a config revision is retained, THE config history SHALL capture the
   root, every sibling part file of the definition's format, and the server
   configuration document.
2. WHEN a config revision is retained, THE config history SHALL list the
   retained part names in the revision's identity sidecar.
3. WHEN a revision is restored, THE config history SHALL write the root and
   its retained parts back over their live counterparts.
4. WHEN a revision is restored, THE config history SHALL leave in place a
   live part file the revision never knew.
5. WHEN the retarget gate compares revisions, THE prior and current
   evaluations SHALL each resolve parts against their own definition set.
6. WHEN a create-time-immutable value changes in any document of the set,
   THE retarget gate SHALL refuse the change.
7. WHEN a baseline realization evaluates a retained revision's root, THE
   part resolver SHALL resolve parts from that revision's folder, never the
   live tree.
