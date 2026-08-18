# IaC Framework

Tokeira ships a custom infrastructure-as-code engine that manages resource lifecycles through a declarative convergence model. The engine is provider-agnostic — it knows nothing about AWS, Docker, or Kubernetes. Provider-specific behaviour lives in platform crates that implement the framework's traits.

## Layered Architecture

```
┌─────────────────────────────────────────────────────────┐
│  CLI (tkr)                                              │
│  Parses commands, loads config, formats output          │
├─────────────────────────────────────────────────────────┤
│  Orchestrator (tokeira-orchestrator)                    │
│  InfraEngine<D> / DeployEngine<D>                       │
│  Owns state persistence, composes modules, drives engine│
├─────────────────────────────────────────────────────────┤
│  IaC Engine (tokeira-iac)                               │
│  Stateless plan/apply/destroy, topological sort, diff   │
├─────────────────────────────────────────────────────────┤
│  State Backend (tokeira-state)                          │
│  CasStore<T>, LocalBackend, S3StateStore<T>             │
├─────────────────────────────────────────────────────────┤
│  Platform Crates                                        │
│  platforms/compose, platforms/ecs, etc.                 │
│  Implement Deployment trait, define Modules + Resources │
└─────────────────────────────────────────────────────────┘
```

## Core Traits

### Resource

A managed infrastructure object with a full lifecycle. Provider crates implement this for concrete resource types (EC2 instances, EBS volumes, Docker Compose services, etc.).

```rust
#[async_trait]
pub trait Resource: Send + Sync {
    fn resource_type(&self) -> ResourceType;
    fn resource_id(&self) -> ResourceId;
    fn dependencies(&self) -> Vec<ResourceId>;
    fn module(&self) -> &str;

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError>;
    async fn update(&self, current: &ResourceState, ctx: &ProvisionContext) -> Result<ResourceState, IacError>;
    async fn delete(&self, current: &ResourceState, ctx: &ProvisionContext) -> Result<(), IacError>;
    async fn describe(&self, ctx: &ProvisionContext) -> Result<Option<ResourceState>, IacError>;
    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange;
}
```

Key contracts:
- `resource_id()` must be stable across runs (derived from config, not provider-assigned IDs).
- `dependencies()` references other resource IDs for ordering.
- `describe()` reads live provider state; returns `None` when the resource is absent.
- `diff()` is a local comparison — no side effects.

### Module

A named deployment unit that groups resources with explicit inter-module dependencies.

```rust
pub trait Module: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn dependencies(&self) -> &[&str];
    fn resources(&self, ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError>;
}
```

Modules are topologically sorted by `dependencies()` before resource expansion. This ensures a module's resources are collected after the modules it depends on.

### Deployment

The integration point between the orchestrator and a platform. Each platform implements this trait to supply config, state backends, modules, and provider handles.

```rust
#[async_trait]
pub trait Deployment: Send + Sync {
    type Config: Send + Sync + Clone + 'static;

    fn remote_state_module(&self, config: &Self::Config, deployment_dir: &Path) -> Box<dyn Module>;
    fn infra_modules(&self, config: &Self::Config, selection: &ModuleSelection) -> Vec<Box<dyn Module>>;
    fn services(&self, config: &Self::Config) -> Vec<Box<dyn Service>>;
    fn images(&self, config: &Self::Config) -> Vec<Box<dyn Image>>;
    async fn register_infra_extensions(&self, config: &Self::Config, ctx: &mut ProvisionContext) -> Result<()>;
    fn create_infra_store(&self, config: &Self::Config, deployment_dir: &Path) -> Box<dyn StateBackend>;
    fn hydrate_config(&self, config: &Self::Config, state: &InfraState) -> Self::Config;
    fn collect_writeback(&self, config: &Self::Config, state: &InfraState) -> Vec<(String, String)>;
    // ... additional methods
}
```

## Engine Lifecycle

### Plan

1. Collect resources from all known modules (topologically sorted).
2. Call `describe()` on each resource to refresh live state.
3. Compare desired resources against refreshed state via `diff()`.
4. Resources in desired but not in state → Create.
5. Resources in both → delegate to `diff()` (Update or NoChange).
6. Resources in state but not in desired → Delete.

### Apply

1. Refresh state (same as plan).
2. Compute changes.
3. Create and update in topological order (dependencies first).
4. Delete in reverse topological order (dependents first).
5. After each mutation, invoke the optional `StateSaver` callback for incremental crash-safety.

### Destroy

1. Refresh state against known resources.
2. All resources in state become Delete changes.
3. Re-describe each resource before deleting (uses live state, not stale persisted state).
4. Delete in reverse dependency order.

## State Persistence

### InfraState

```rust
pub struct InfraState {
    pub version: u32,
    pub resources: BTreeMap<ResourceId, ResourceState>,
    pub outputs: BTreeMap<String, String>,
    pub last_applied: String,
}
```

Each resource's persisted state includes its `physical_id` (provider-assigned identifier), `properties` (JSON blob for diff comparison), `dependencies`, and owning `module`.

### CasStore and Backends

State is persisted through `CasStore<T>` which delegates to a `StateBackend` trait object:

- **LocalBackend**: Filesystem-backed. Version tags are SHA-256 content hashes. Atomic writes via temp file + rename. CAS semantics: stale version → `StateError::Conflict`.
- **S3StateStore**: Manifest pointer + immutable snapshots, ETag-based CAS, lease locks.

The orchestrator's `InfraEngine` wraps the saver in a `StateSaver` closure that tracks the latest version via a `Mutex<String>`, ensuring each incremental save uses the correct expected version.

### ProvisionContext

Carries project identity, tags, the current `InfraState`, progress reporters, and a typed extension map (`HashMap<TypeId, Box<dyn Any + Send + Sync>>`). Platform crates register provider handles (e.g., `AwsClients`) via `set_extension()` and resources retrieve them via `extension::<T>()`.

## InfraComposition

The orchestrator composes modules into an `InfraComposition` with three sets:

- **desired_modules**: what should exist after apply.
- **known_modules**: everything the deployment can manage (superset of desired). Resources in known but not desired that exist in state will be deleted.
- **active_modules**: which modules are in scope for this operation (for `--module` filtering).

This desired-vs-known model ensures removed modules are properly cleaned up rather than silently orphaned.

---

## Compose Platform

The compose platform (`platforms/compose`) manages a Docker Compose stack with tokeirad and observability services (Mimir, Loki, Grafana, Alloy).

### Deployment Implementation

`ComposeDeployment` implements both `Deployment` and `Ops` traits with:

- **Config**: `ComposeConfig` containing `TokeiradServiceConfig` (image, ports, replicas) and `ObservabilityConfig` (pinned image versions for Mimir 3.0.6, Loki 3.7.1, Grafana 12.4.3, Alloy v1.16.0, plus AWS CLI and BusyBox).
- **State backend**: `LocalBackend` at `<deployment_dir>/state/infra/`.
- **Extensions**: None for infra; `ComposeConfig` registered on `ImageContext` for image resolution.

### Module Graph

```
remote-state (no deps)           → LocalStateResource (ensures state directory exists)
runtime      (depends: remote-state) → tokeirad compose service
observability (depends: remote-state, runtime) → mimir, loki, grafana, alloy compose services
```

### Resources

Each compose service is wrapped as an `OwnedComposeResource` that delegates to `ComposeService` (from `tokeira-compose` crate) for create/update/delete/describe/diff. The wrapper adds module ownership so `--module observability` correctly targets the right services.

| Module | Services |
|--------|----------|
| runtime | tokeirad |
| observability | mimir, loki, grafana, alloy |

### Operational Flow

```
tkr infra apply
  → InfraEngine::new(ComposeDeployment, config, deployment_dir)
  → engine.compose(ModuleSelection::All)
  → engine.apply(...)  // creates/updates compose services via Docker API

tkr infra apply --module observability
  → engine.compose(ModuleSelection::Only(["observability"]))
  → engine.apply_for_modules(...)  // only touches mimir, loki, grafana, alloy

tkr deploy apply
  → DeployEngine::new(ComposeDeployment, config, deployment_dir)
  → engine.apply(ComposePlatform)  // resolves images, applies service manifests
```

### Ops Interface

The compose platform also implements `Ops` for operational commands:

```
tkr scale up tokeirad 2    → docker compose scale
tkr logs tokeirad          → docker compose logs
tkr port-forward grafana   → reads container port mappings
```

---

## Adding a New Platform

To add a new platform to the IaC framework:

1. Create a crate or module implementing `Deployment`.
2. Define modules (implement `Module`) that group your resources.
3. Implement `Resource` for each managed infrastructure object.
4. Choose a state backend (`LocalBackend` for local, `S3StateStore` for cloud).
5. Register provider handles via `register_infra_extensions()`.
6. Wire into the CLI's command dispatch.

The framework handles dependency ordering, state persistence, change detection, and progress reporting. Platform code focuses solely on provider-specific API calls.
