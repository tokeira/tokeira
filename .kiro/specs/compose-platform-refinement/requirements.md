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
- **Provider**: what a platform fundamentally runs on, carried as one value:
  `ProviderExport` — the provider's complete kind library, its optional ops
  surface, its execution probe, and its optional infra-phase constructor.
  Compose's provider is `tokeira_compose::provider()`.
- **Kind**: an author-visible resource type a definition can instantiate
  (`Service`, `LocalStateDir`, `ServerConfig`, `ObservabilityConfiguration`,
  `DsqlCluster`, `DynamoDbTable`).
- **Kind selection**: a `KindSet` added to the declaration — the platform's
  own kinds, or a typed selection from another provider's export
  (`tokeira_aws::kinds::select(vec![kind::<DsqlCluster>(…), …])`), each
  entry selected under the word its resource owns.
- **Authoring vocabulary**: the union of every declared kind set; a
  definition may name exactly these kinds.
- **Ops**: the provider's surface over a running deployment — log streams,
  port mappings, and scale — carried as `Option<Box<dyn Ops>>` on the
  export. The framework surfaces the corresponding verbs by presence.
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

- contains `definition.tkd` and `definition.tkdp`, the observability content
  (`observability/`: dashboards, alert rules, backend config templates) as
  files beside the definitions, the platform's own observability kind, the
  catalog descriptor, and a library exporting one entry-point function;
- the entry point declares the provider and the kind selections — the
  platform's own observability kinds plus a typed AWS selection — and
  nothing else; construction is pure;
- depends on the providers it wires (`tokeira-compose`, `tokeira-aws`) and
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
- surfaces the provider's ops as `logs` / `port-mappings` / `scale` verbs by
  presence, validating service names against the evaluated definition;
- probes the provider before mutating verbs: an unreachable provider
  **blocks** plan (the plan plans nothing; the issue is the outcome's only
  content) and refuses apply and destroy.

The provider crate (`tokeira-compose`):

- exports `provider() -> ProviderExport`: the compose kind library, the
  Docker ops (logs, port mappings, scale via compose service scaling), the
  reachability probe, and the infra constructor that connects
  `ComposePlatform` at operation start;
- carries no deployment content: rendering machinery (parameter
  substitution, digests, consumer fencing) takes content as input.

Definitions may declare parts; the frontends evaluate them with the host
language's own semantics (Requirement 10). Config revisions retain the
definition set and the retarget gate compares sets (Requirement 11).

Absent from the engine: `tokeira-kinds` (no global kind inventory exists),
platform-side pipeline traits, platform config twins, and the local
platform's provisioner (`platforms/local` describes; its bound path arrives
with its own onboarding).

## Evidence From Current Code

The contract as adopted, by seam:

- `crates/tokeira-platform/src/declaration.rs:30` — `PlatformDeclaration`
  (`on(provider)`, `.kinds(selection)`, `.vocabulary()`); `:85` —
  `ProviderExport { kinds, ops, execution, infra }`; `:112` — `KindEntry`
  with `kind::<K>(name)` typed construction under the word the kind's
  resource owns (the resource's `TYPE` const); `:152` —
  `KindSet::new(provider, entries).infra(constructor)`; `:182` —
  `Vocabulary::of` with collision refusal; `Ops` (log stream, port
  mappings, scale — required, undefaulted); `ProviderExecution`
  (probe-only, blocked-plan semantics documented); `InfraConstructor`
  (per-selection registration with the selection's namespace attributes).
- `platforms/compose/src/lib.rs:71` — the entry point:
  `PlatformDeclaration::on(tokeira_compose::provider())`, the platform-owned
  `observability::kind_set()`, and the typed AWS selection.
- `crates/tokeira-provisioner-cli/src/platform.rs` — admission (`Admitted`,
  once per command, double-entry verification of the recorded pair);
  `src/engine.rs` — the verbs (plan, apply, destroy, selected destroy,
  deploy plan/apply, desired snapshot, recorded state, retarget check) over
  `DescribedDeployment` (`src/described.rs`), the one
  `orchestrator::Deployment` on the bound path; `src/lib.rs` —
  `run_bound_provisioner(platform, format, declaration, frontend)` and
  writeback persistence into the server config document.
- `crates/tokeira-platform/src/definition.rs` — `SourceResolver`,
  `DirectoryPartSources`, `NoPartSources`, `RecordingResolver`, and the
  `sha256-set-v1` definition-set identity.
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
2. THE entry-point function SHALL declare the provider and the kind
   selections, SHALL accept no arguments, and SHALL perform no filesystem,
   network, or Docker access.
3. THE platform crate SHALL NOT contain implementations of planning,
   applying, destroying, state persistence, module selection, writeback
   resolution, inspection publication, confirmation, or definition
   evaluation.
4. THE platform crate SHALL NOT depend on the framework shell; its
   dependencies SHALL be the providers it wires and the contract and
   machinery crates its own kinds need.
5. WHEN the framework operates a Compose deployment THEN every invocation of
   platform code SHALL be the entry-point function, a declared kind, or a
   capability carried by the provider export.

### Requirement 2: The definition owns the configuration shape

**User Story:** As a definition author, I want the configuration shape
authored once in the definition, so that the shape I edit is the shape the
system admits — with no hand-synchronised Rust mirror to drift.

#### Acceptance Criteria

1. THE platform crate SHALL define no types mirroring definition-authored
   configuration shapes.
2. WHEN evaluation completes THEN the framework SHALL hold the evaluated
   configuration as a located value, and no platform code SHALL receive a
   decoded configuration value.
3. WHEN a `Service` kind input carries an empty image reference or zero
   replicas or a zero published port THEN kind input validation SHALL refuse
   with an error located at the authoring site.
4. WHEN a `DsqlCluster` kind input carries managed mode together with an
   endpoint or ARN, or preexisting mode without both endpoint and ARN, or an
   empty region THEN kind input validation SHALL refuse with an error
   located at the authoring site.
5. WHEN a definition's evaluated configuration violates no kind input rule
   THEN the framework SHALL NOT invoke any platform-supplied configuration
   validator, because none exists.

### Requirement 3: The authoring vocabulary is exactly the declared selections

**User Story:** As a platform maintainer, I want the vocabulary my
definitions may use to be exactly what the entry point declares, so that
wiring is one positive declaration instead of an engine-wide union narrowed
after the fact.

#### Acceptance Criteria

1. WHEN the entry point names the provider THEN the provider's complete kind
   library SHALL be part of the authoring vocabulary with no separate wiring
   declaration.
2. THE auxiliary vocabulary SHALL be selected explicitly at the entry point
   by kind type, under the word the kind's resource owns
   (`kind::<DsqlCluster>(…::TYPE)`); a selection typo SHALL be a compile
   error.
3. WHEN a definition names a kind outside the composed vocabulary THEN
   `definition check` SHALL refuse with an unknown-kind error located at the
   authoring site.
4. WHEN two declared kind sets export the same kind name THEN composition
   SHALL fail, naming both providers and the colliding name.
5. THE engine SHALL contain no global kind inventory: kind-name resolution
   SHALL be computed from the composed vocabulary, and no check SHALL exist
   that refuses a kind as "known but unwired".

### Requirement 4: Ops are provider capability, surfaced by presence

**User Story:** As an operator, I want `tkp logs`, `tkp port-mappings`, and
`tkp scale` to exist because the provider can answer them, so that
capability discovery is honest and absent capabilities are absent verbs
rather than stub refusals.

#### Acceptance Criteria

1. THE provider export SHALL carry its ops surface as an optional value
   implementing log streams, port mappings, and scale; scale SHALL be a
   required method of that surface — a provider without a scale dimension
   states its own refusal as the error.
2. WHEN the declared provider carries ops THEN the framework SHALL expose
   the `logs`, `port-mappings`, and `scale` verbs; WHEN it carries none THEN
   the framework SHALL NOT present those verbs.
3. WHEN an operator names a service for an ops verb THEN the framework SHALL
   validate the name against the evaluated definition's services and SHALL
   refuse unknown names listing the definition's actual services.
4. THE ops surface SHALL receive deployment identity (name and directory)
   from the framework, and SHALL NOT re-read deployment metadata to answer.
5. THE platform crate SHALL contain no hard-coded service inventory.
6. WHEN `tkp scale` changes Compose service capacity THEN the framework
   SHALL re-stamp recorded state after the provider applies the change.

### Requirement 5: Observability content is platform description

**User Story:** As a platform maintainer, I want the dashboards, alert
rules, and backend config templates to live with the platform as files, so
that content edits are visible, diffable, per-deployment plannable changes
instead of provider-crate releases.

#### Acceptance Criteria

1. THE observability content (backend config templates, Grafana dashboards,
   alert rules) SHALL reside in the platform package beside the definitions,
   and each platform SHALL own its content tree — Compose's under
   `platforms/compose/observability/`, ECS's under
   `platforms/ecs/observability/`.
2. THE provider crate SHALL embed no deployment content; rendering machinery
   SHALL accept the content it renders as input.
3. THE kind that renders the content (`ObservabilityConfiguration`) SHALL be
   owned by the platform and contributed to the vocabulary as the platform's
   own selection.
4. WHEN a realization resolves observability content THEN it SHALL resolve
   against the interpreted definition source's directory, so a baseline
   realization from a retained revision digests that revision's content.
5. WHEN observability content changes THEN the content digest fencing its
   consumer services SHALL change, surfacing the edit as a plannable
   difference.
6. EACH platform's content tree SHALL be validated by that platform's own
   tests against the shared dashboard and alert-rule style contracts.

### Requirement 6: The framework owns change management, once

**User Story:** As an engine maintainer, I want admission, evaluation,
verification, and the provisioning lifecycle implemented once in the
framework, so that platforms cannot diverge in behaviour they should share
and a new platform adds no pipeline code.

#### Acceptance Criteria

1. THE framework SHALL admit once per command, producing verified deployment
   identity and metadata before any verb logic runs; a deployment whose
   recorded platform/format pair disagrees with the binary's SHALL refuse at
   admission.
2. THE framework SHALL evaluate the recorded definition through the frontend
   selected by the recorded format, using the composed vocabulary, resolving
   parts beside the interpreted document, in one implementation shared by
   all platforms on the bound path.
3. THE framework SHALL implement plan, apply, destroy, selected-resource
   destroy, deploy plan, deploy apply, desired-manifest snapshot,
   recorded-state read, definition check, and retarget check with no
   per-platform method behind any of them.
4. THE framework SHALL run each declared selection's infra constructor
   inside the deployment's registration seam, passing the selection's
   namespace block from the evaluated configuration; the platform SHALL
   perform no extension-map registration.
5. WHEN the provider probe reports the substrate unreachable THEN plan SHALL
   be blocked — it SHALL plan nothing and the issue SHALL be the outcome's
   only content — and apply and destroy SHALL refuse on the same issue.
6. WHEN an apply, upgrade, revert, or rollback resolves writeback THEN the
   framework SHALL persist the resolved entries into the deployment's server
   configuration document before re-stamping recorded state.
7. THE deploy verbs SHALL reconcile the definition's service plane through
   the deploy engine; WHILE the service plane is empty THE deploy apply
   SHALL be fail-closed — a non-empty reconciliation refuses rather than
   pretending to apply.

### Requirement 7: The binding architecture is retained; the framework performs the marriage

**User Story:** As an operator, I want the durable association between a
deployment's definition bytes and the engine interpreting them preserved
exactly, while the platform stops being a construction site for the
frontend.

#### Acceptance Criteria

1. THE catalog SHALL discover platforms by the
   `[package.metadata.tokeira.platform]` descriptor and SHALL resolve
   definitions by joining the platform package directory with the frontend's
   conventional relative path.
2. WHEN a deployment is created THEN the definition bytes SHALL be staged
   into the deployment directory as data — never compiled into the
   executable — and `{ platform, format, path }` SHALL be recorded in
   deployment metadata.
3. THE generated entry of a bound tkp SHALL hand the platform declaration
   and the frontend to the framework
   (`run_bound_provisioner(platform, format, declaration, frontend)`), and
   the framework SHALL perform the marriage.
4. THE platform crate SHALL NOT be generic over, construct, or invoke a
   definition frontend.

### Requirement 8: Compose declares no image inventory

**User Story:** As a platform maintainer, I want Compose to carry no image
build/mirror inventory, so that the platform declares only concepts Compose
actually has.

#### Acceptance Criteria

1. THE Compose platform SHALL declare no image inventory, and the framework
   SHALL present no image verbs for a composed platform that declares none.
2. WHEN a Compose service is reconciled THEN its image reference SHALL be
   taken from the evaluated definition configuration.
3. WHEN a locally-built image named by the definition is absent from the
   Docker daemon THEN the operation SHALL refuse with the existing
   remediation naming the image.

### Requirement 9: Operator-observable behaviour is preserved

**User Story:** As an operator with existing Compose deployments, I want the
refined platform to provision exactly what the pre-refinement one did, so
that the refactor is invisible except where this spec says otherwise.

#### Acceptance Criteria

1. WHEN the reference definition is evaluated under each storage mode
   (in-memory, DSQL managed, DSQL preexisting) THEN the realized module and
   resource graph SHALL match: `local_state`, `runtime`, and `observability`
   always; `dsql` exactly when DSQL storage is selected.
2. WHEN services consuming rendered configuration are realized THEN the
   content digest environment fencing (`TOKEIRA_CONFIG_DIGEST`,
   `TOKEIRA_SERVER_CONFIG_DIGEST`, `TOKEIRA_CONFIG`) and the server-config
   mount SHALL be preserved.
3. WHEN `definition check` runs THEN it SHALL remain pure: no provider
   access, no state or config side effects in the deployment directory.
4. WHEN the inspection projection is published THEN `docker-compose.yml`
   SHALL render deterministically from desired state and SHALL never be read
   back as an input.
5. WHEN a module is named for destroy THEN the selection SHALL expand to
   dependants; WHEN named for plan or apply THEN it SHALL expand to
   prerequisites.
6. WHEN DSQL storage is selected THEN the recorded writeback declarations
   (`infrastructure.storage`, `infrastructure.dsql.*`) SHALL resolve from
   applied outputs to the same keys and values as before the refinement,
   and SHALL persist per Requirement 6.6.

### Requirement 10: Definitions may span multiple documents

**User Story:** As a definition author, I want to split a definition into a
root and parts — one file per module of the deployment — with each
frontend's host language supplying the module semantics, so that a large
definition reads as a set of focused documents instead of one monolith.

#### Acceptance Criteria

1. WHEN a `.tkd` root declares `mod name;` THEN the frontend SHALL resolve
   `name.tkd` beside the root and evaluate it as a part: namespaced calls
   (`name::function(…)`), `pub`-gated exports, part scopes seeing their own
   then the root's types, and wiring flowing through the root — a part
   SHALL NOT reference another part.
2. WHEN a `.tkdp` root imports a name the part resolver serves THEN the
   frontend SHALL register the part as a genuine Python module: its own
   namespace, qualified member access, imports resolving transitively
   (parts may import parts), and traceback frames carrying the part's own
   file name at original positions.
3. THE `.tkdp` facade SHALL be a genuine registered `tokeira` module — one
   set of class identities shared by the root and every part; `from tokeira
   import …` SHALL execute as a real import in every file.
4. WHEN a `.tkdp` import is not served by the part resolver THEN it SHALL
   fall through to the sandbox runtime — a built-in module, or a runtime
   module-not-found error at the import site.
5. THE frontends SHALL refuse, by name: an inline `mod` body, a part
   declaring a part, a private part function called from the root, a part
   shadowing a root type (`.tkd`); a dotted or relative import, a plain
   import shadowed by the file's own binding (with the from-form as the
   stated remedy), and an import cycle among parts, naming the cycle path
   (`.tkdp`).
6. THE evaluation SHALL record the definition set — root plus served parts
   in first-request order — under the `sha256-set-v1` identity.
7. THE `.tkdp` part mechanism SHALL stand on the pinned Monty fork's
   embedder-registered source modules; the fork SHALL carry its retire
   condition beside the pin.

### Requirement 11: Config revisions retain the definition set

**User Story:** As an operator, I want a config revision to capture the
whole definition set my apply ran with, so that reverts restore what I
actually had and the retarget gate compares what actually changed.

#### Acceptance Criteria

1. WHEN a config revision is retained THEN it SHALL capture the root, every
   sibling part file of the definition's format, and the server
   configuration document; the retained part names SHALL be listed in the
   revision's identity sidecar.
2. WHEN a revision is restored THEN the root and its retained parts SHALL be
   written back over their live counterparts; a live part file the revision
   never knew SHALL be left in place.
3. WHEN the retarget gate compares revisions THEN each side SHALL resolve
   its parts against its own set — the retained revision's folder for the
   prior, the live directory for the current — and a create-time-immutable
   change SHALL refuse regardless of which document of the set carries it.
4. WHEN a baseline realization evaluates a retained revision's root THEN
   parts SHALL resolve from that revision's folder, never the live tree.
