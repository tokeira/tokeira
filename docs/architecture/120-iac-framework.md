# IaC Framework

**Status:** current — describes the framework as implemented

Tokeira ships a custom infrastructure-as-code engine that manages resource
lifecycles through a declarative convergence model. The engine core is
provider-agnostic — it knows nothing about AWS, Docker, or Kubernetes. Two
ideas shape everything above that core:

- **A platform is a description.** A platform package contributes a pure
  declaration — the resource kinds it offers, how to connect to its provider,
  and how to operate a running deployment. It performs no I/O when constructed
  and holds no deployment state.
- **A deployment runs its own provisioner.** Operator lifecycle commands are
  executed by a per-deployment binary (`tkp`), generated and built for exactly
  one platform and one definition format. `tkr` creates deployments and
  forwards lifecycle commands to the deployment's own `tkp`; it does not run
  the engine itself.

Authoring depth for definitions lives in
[deployment-definitions](../provisioning/deployment-definitions.md); the
operator surface is described in [tkr-and-tkp](../provisioning/tkr-and-tkp.md).

## Layered Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  CLI (tkr)                                                   │
│  Deployment registry, create, platform discovery; forwards   │
│  lifecycle commands to the deployment's own tkp binary       │
├──────────────────────────────────────────────────────────────┤
│  Bound provisioner (tokeira-tkp, generated per deployment)   │
│  BoundPlatform + DefinitionFrontend + Engine<F>              │
│  Admission, operation lock, plan/confirm/apply shell         │
├──────────────────────────────────────────────────────────────┤
│  Orchestrator (tokeira-orchestrator)                         │
│  InfraEngine<D> / DeployEngine<D>                            │
│  Owns state persistence, composes modules, drives engines    │
├──────────────────────────────────────────────────────────────┤
│  Engines (tokeira-iac, tokeira-deploy-engine)                │
│  Stateless plan/apply/destroy, topological sort, diff,       │
│  change semantics; service manifests and images              │
├──────────────────────────────────────────────────────────────┤
│  State (tokeira-state, tokeira-deployment)                   │
│  DeploymentStore<T>, CAS backends, operation lock,           │
│  deployment state envelope, TUF repository                   │
├──────────────────────────────────────────────────────────────┤
│  Platform packages (platforms/compose, platforms/eks, …)     │
│  Pure PlatformDeclaration + definition documents + content   │
└──────────────────────────────────────────────────────────────┘
```

Platform packages contribute *declarations*; deployment definitions contribute
*modules and resources*. The orchestrator and engines below them stay generic.

## Platforms as Descriptions

A platform is one pure function:

```rust
pub fn platform() -> PlatformDeclaration
```

`PlatformDeclaration` (`crates/tokeira-platform/src/declaration.rs`) carries:

- **`namespaces`** — the resource kinds the platform offers to definitions,
  grouped as `Namespace { name, kinds, defaults, decode }`. Provider crates
  (e.g. `tokeira-compose`, `tokeira-aws`, `tokeira-k8s`) implement the actual
  resource behaviour; the namespace exposes it to authors.
- **`execution`** — a `PlatformExecution` whose `probe(&DeploymentRef)`
  answers whether the provider is reachable. The probe returns a
  `PlatformIssue` as *data*: plan blocks on it, apply and destroy refuse on
  it. Nothing connects at construction time.
- **`implementation`** — a `PlatformIntegration` that registers provider
  handles (infra, deploy, and image extensions) and supplies the service
  platform for the deploy plane.
- **`ops`** — the operational interface for a running deployment.

The only deployment coordinate a platform ever receives is
`DeploymentRef { name, dir }` — identity, never state. Deployment identity
flows from the deployment *name*; the deployment directory is where a platform
reads its admitted configuration when an operation requires it.

### Ops

```rust
#[async_trait::async_trait]
pub trait Ops: Send + Sync + fmt::Debug {
    async fn log_stream(&self, deployment: &DeploymentRef, service: &str,
                        follow: bool, tail: Option<u32>) -> anyhow::Result<LogStream>;
    async fn port_mappings(&self, deployment: &DeploymentRef, service: &str)
                        -> anyhow::Result<Vec<PortMapping>>;
    async fn scale(&self, deployment: &DeploymentRef, specs: &[String])
                        -> anyhow::Result<usize>;
}
```

Capacity is expressed as `<dimension>=<count>` specs interpreted by the
platform (`tokeirad=2`), reached through `tkr scale`, `tkr logs`, and
`tkr port-forward`, which forward to the deployment's `tkp`.

## Deployment Definitions

The shape of a deployment — its configuration surface, modules, resources,
and their wiring — is authored in a **definition document**, not in Rust.
Two formats ship, each behind a `DefinitionFrontend`
(`crates/tokeira-platform-definition`):

- **`.tkd`** — Rust syntax interpreted through a sandboxed `syn` allow-list
  (schema → subset → eval → admission). No macro or arbitrary code execution.
- **`.tkdp`** — Python executed by a pinned Monty interpreter, with a
  match-lowering pass and a byte-covering source map.

```rust
pub trait DefinitionFrontend: Clone + Send + Sync + 'static {
    fn format(&self) -> &DefinitionFormatId;
    fn evaluate<C: Serialize>(&self, source: FrontendSource<'_>, context: &C,
        namespaces: &[Namespace], parts: &dyn SourceResolver)
        -> Result<FrontendOutput, FrontendDiagnostic>;
    fn retarget_check<C: Serialize>(&self, prior: FrontendSource<'_>,
        current: FrontendSource<'_>, /* … */) -> Result<(), Vec<String>>;
}
```

`evaluate` returns `FrontendOutput`:

- **`config: LocatedValue`** — a host-free, source-located value tree. A full
  `serde::Deserializer` over it (`crates/tokeira-platform/src/author.rs`)
  decodes typed platform configuration and kind inputs; a decode failure
  carries a `SourceRange` pointing at the operator's own line.
- **`graph: VerifiedGraph<DecodedKind>`** — the structural deployment graph:
  modules, resource nodes, inter-resource references, output references, and
  declared writeback entries, validated for shape before anything realizes.

`retarget_check` is the create-time admission gate: it diffs the previously
admitted configuration against the one about to apply and refuses the
operation naming each changed `#[create]` field. Create-time values (the
fields that name and place a deployment) cannot drift after inception.

`ConfigurationIdentity` is the versioned SHA-256 identity over the format and
the exact definition bytes — the anchor for recorded revisions and rollback.

### Kinds and realization

A definition's resource nodes decode into platform kinds. At operation time
each kind realizes into the engine object it drives:

```rust
pub trait Kind<R>: fmt::Debug + Send + Sync {
    fn realize(&self, placement: &PlacementContext) -> Result<R, KindError>;
}
```

`R` is either `tokeira_iac::Resource` (infra plane) or
`tokeira_deploy_engine::Service` (deploy plane). Module ownership comes from
the graph node, so `--module` targeting needs no wrapper types.

## The Bound Provisioner (`tkp`)

`crates/tokeira-tkp` is the platform-agnostic shell that every provisioner
binary *is*. A composition root is **generated** per platform/format pair
(`crates/tokeira-build/src/composition.rs`) as an ordinary member of a frozen
source workspace — no platform dispatch table, no private lockfile; Cargo
remains the sole resolver. The generated `main` is one macro invocation:

```rust
bound_provisioner_main!(/* platform id, format, content roots, declaration, frontend */);
```

which calls `run_bound_provisioner`. Binding is checked before any deployment
is read: the declaration must match the identity pair the binary was built as,
and a kind-name collision refuses the binary outright.

- **`BoundPlatform`** — deliberately a struct, not a trait. It holds the
  built-as `(platform, format)` pair and decides whether a deployment
  directory belongs to this platform and whether this binary may operate it.
  Admission happens once per command at the CLI boundary, before even the
  operation lock; the resulting `Admitted` value threads through every verb.
- **`Engine<F>`** — the lifecycle owner for one bound platform: evaluation
  (recorded revision or authoring source), verification and realization into a
  per-operation `ExecutionState`, and the probe-first `plan` / `apply` /
  `destroy` verbs. It also nominates the bootstrap module: the graph must have
  exactly one dependency-free module, and that module is the deployment's
  state bootstrap.
- **`DescribedDeployment`** — the one implementation of the orchestrator's
  `Deployment` trait on this path. It derives every answer from
  `ExecutionState` + `DeploymentRef` + the platform's integration, owning no
  platform knowledge of its own beyond installing the standard AWS clients
  when the declaration includes that namespace. The framework, not the
  platform, chooses the state store layout under the deployment directory.

### Writeback

Definitions declare writebacks — values that provisioning discovers and the
server configuration needs (`d.writeback("key", resource.output("name"))`).
After apply, `collect_writeback` resolves declared entries: literals pass
through, output references resolve through the realized index into recorded
resource properties. The shell persists the resolved pairs into the
deployment's server configuration document via `toml_edit`, preserving the
operator's formatting. Config projection is the declared writeback's job;
`hydrate_config` on this path is the identity function.

## The Core Engine (`tokeira-iac`)

### Resource

A managed infrastructure object with a full lifecycle:

```rust
#[async_trait::async_trait]
pub trait Resource: Send + Sync {
    fn resource_type(&self) -> ResourceType;
    fn resource_id(&self) -> ResourceId;
    fn dependencies(&self) -> Vec<ResourceId>;
    fn module(&self) -> &str;
    fn validate_input(&self) -> Result<(), String> { Ok(()) }
    fn declared_outputs(&self) -> &'static [&'static str] { &[] }
    fn desired_manifest(&self) -> serde_json::Value { serde_json::Value::Null }
    fn describes(&self) -> bool { true }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError>;
    async fn update(&self, current: &ResourceState, ctx: &ProvisionContext)
        -> Result<ResourceState, IacError>;
    async fn delete(&self, current: &ResourceState, ctx: &ProvisionContext)
        -> Result<(), IacError>;
    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError>;
    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange;

    fn change_semantics(&self, ctx: &SemanticsContext<'_>) -> ChangeSemantics;
    fn display_kind(&self) -> Option<&'static str> { None }
}
```

Key contracts:

- `resource_id()` must be stable across runs (derived from config, not
  provider-assigned IDs).
- `describe()` reads live provider state and is **three-valued**:
  `DescribeResult::Present(state)`, `Absent` (confirmed gone — prunes state),
  or `Unsupported` — which a resource must return when it cannot confirm
  absence. Treating "no answer" as absence would delete state the provider
  still holds; the three-valued result makes that mistake unrepresentable.
- `diff()` is a local comparison — no side effects.
- `change_semantics()` is required, pure, and total: every resource states
  what a lifecycle operation means for it (replacement policy, disruption,
  data effect, reversibility) in the shared vocabulary of
  `tokeira-iac/src/semantics.rs`. Every field defaults to `Unknown`, and the
  confident grades (`EngineFact`, `ProviderGuarantee`) cannot be constructed
  without a citation.

### Module

```rust
#[async_trait::async_trait]
pub trait Module: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn dependencies(&self) -> Vec<&str>;
    fn resources(&self, ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError>;
}
```

Modules are topologically sorted by `dependencies()` before resource
expansion. On the bound path, modules come from the definition's graph, not
from hand-written Rust.

### Engine lifecycle

**Plan**

1. Probe the platform; a `PlatformIssue` blocks the plan — the outcome carries
   the issue and no changes.
2. Collect resources from all known modules (topologically sorted) and verify
   them: kinds with stub `describe` implementations and dangling dependency
   edges are refused before any provider call.
3. Call `describe()` on each resource to refresh live state.
4. Compare desired resources against refreshed state via `diff()`: desired but
   not in state → Create; in both → Update, Replace, or NoChange; in state but
   not desired → Delete.
5. The `PlanOutcome` carries the changes plus refresh coverage, per-resource
   change semantics, display metadata, and dependency edges — enough for the
   CLI to explain the plan, not merely list it.

**Apply**

1. Refresh and compute changes as for plan.
2. Destructive changes (Delete, Replace) gate confirmation before execution.
3. Create and update in topological order; delete in reverse order.
4. After each mutation, a `StateSaver` callback persists incrementally for
   crash-safety, tracking the expected CAS version across saves.
5. Deletion is fail-closed: a resource that cannot confirm its absence is not
   pruned from state.

**Destroy**

1. Refresh state against known resources; everything in state becomes a
   Delete.
2. Re-describe each resource before deleting (live state, not stale persisted
   state), then delete in reverse dependency order.

### Composition and module targeting

The orchestrator composes modules into an `InfraComposition` with three sets:
**desired** (what should exist after apply), **known** (everything the
deployment can manage — resources in known-but-not-desired that exist in state
are deleted), and **active** (the scope of this operation). `--module`
selection expands to keep operations coherent: prerequisites join a plan or
apply, dependants join a destroy, and unknown module names are refused.

## State and the Deployment Envelope

### Stores

The seam the engines hold is `DeploymentStore<T>` — `load() -> (T, version)`
and CAS `save(doc, expected_version)`. Implementations:

- **`CasStore<T>` over `LocalBackend`** — filesystem-backed; version tags are
  SHA-256 content hashes, writes are atomic via temp file + rename, and a
  stale version yields `StateError::Conflict`.
- **`S3StateStore<T>`** — manifest pointer + immutable snapshots, ETag-based
  CAS, lease locks.

An `OperationLock` serializes lifecycle operations on a deployment.

### The envelope

`DeploymentStateEnvelope` (`crates/tokeira-deployment`) is the
deployment-level authority: provenance, the integrity manifest, the admitted
configuration revision, the rollback checkpoint, the in-flight operation
marker, the operation lock, and the infra/runtime state heads — under one
revision. Rollback is definition-driven: the checkpoint records the
configuration reference to return to, not before-images of provider state.

`ServerConfig` lives here too: the deployment-owned graph node that renders
`tokeirad.toml`, shared by every platform rather than reimplemented per
platform.

### Distribution

Each named deployment is backed by a TUF repository whose residency follows
the create-time state choice (local directory or S3), with one verification
path over both. Fetching materializes a plan of placements — definition
documents, the config tree, and the `tkp` binary with its integrity-manifest
sidecar. Publication is derived, never authoritative: a publication failure
does not unwind a committed state transition. Provisioner binaries are built
hermetically by the Dagger pipelines in `crates/tokeira-build` (cold,
`--locked`, digest-pinned build container, artifacts hashed host-side), and
`tkr deployment create` resolves-or-builds: a content-addressed hit is
re-verified through the admission gate; a miss builds and publishes back.

## The Compose Platform, as the Worked Example

`platforms/compose` declares the platform in a single pure `platform()`; the
deployment shape is authored in its `.tkd` definition documents, and the crate
defines no configuration types of its own.

- **Configuration surface** (`platform.tkd`): a `Compose` root with storage
  selection, AWS settings, the `tokeirad` service, and the observability
  stack.
- **Module graph** (`deployment.tkd`, `observability.tkd`): a bootstrap
  `local_state` module; a `dsql` module that exists only when DSQL storage is
  selected; a `runtime` module whose `tokeirad` node depends on a
  `ServerConfig` node; and an `observability` module (Mimir, Loki, Grafana,
  Alloy) whose services all depend on a rendered-configuration node and carry
  its content digest — image versions are pinned in the definition, where a
  bump is a plannable change.
- **Content bundle**: the observability configuration tree ships with the
  platform package and is rendered at realization with strict two-way
  `{{ name }}` substitution — an unknown placeholder is refused at plan,
  naming the file and the placeholder. A retained revision renders its own
  content, so a dashboard edit is a change to the deployment, not a code
  release.
- **Writeback**: the definition declares the provisioning outputs that flow
  into the server configuration document after apply.
- **Ops**: implemented against the Docker daemon alone — label-filtered
  container lookup plus inspect; no compose-file ledger is consulted at
  operation time.

## Adding a New Platform

1. **Describe the package.** A platform is discovered from Cargo metadata
   (`[package.metadata.tokeira.platform]`: id, engine, default format,
   definition documents, content trees) — `tkr` builds its catalog from the
   workspace; there is no CLI wiring step.
2. **Declare it.** Export a pure `fn platform() -> PlatformDeclaration` with
   the namespaces, execution probe, integration, and ops.
3. **Implement the kinds.** Provider resource behaviour lives in provider
   crates implementing `Resource` (and `Service` for the deploy plane),
   exposed to authors through the declaration's namespaces.
4. **Author the definition.** Modules, resources, wiring, create-time fields,
   and writebacks are written in `.tkd` or `.tkdp`, alongside any content
   trees the platform owns.
5. **Generate the provisioner.** The build pipeline assembles the bound
   composition root; the resulting `tkp` binary is admitted per deployment and
   runs the whole lifecycle.

The framework handles admission, dependency ordering, state persistence,
change detection and semantics, confirmation gating, and progress reporting.
Platform code focuses on provider-specific behaviour; definitions focus on
deployment shape.

## Legacy In-Process Path

Two deployment types — bare-host Local and `deployment.toml` ECS — still run
through `tkr`'s own in-process `InfraEngine` behind a facade documented as
legacy in `apps/tkr`. New platform work happens on the definition-bound path
above.
