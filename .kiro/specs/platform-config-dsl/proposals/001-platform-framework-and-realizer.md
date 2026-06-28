# Proposal 001 — `tokeira-platform` framework and the `Realizer` seam

- **Status:** Proposed (design only; no code moved yet)
- **Refines:** tasks 12 (extract reusable DSL-platform runtime) and 13 (ecs-dsl realizer)
- **Owner area:** `crates/tokeira-platform-dsl`, `platforms/compose-dsl`, a new `crates/tokeira-platform`
- **Audience:** whoever builds the next DSL platform (e.g. `ecs-dsl`, future `eks`) and the `tkr` wiring (Wave 8)

## Why

`platforms/compose-dsl/src/deployment.rs` is ~800 lines today, and almost all of it is **generic** DSL-platform plumbing — plan building, `DslOwnedResource`, the `Deployment`/`Ops` trait glue, writeback resolution, `Ops` verbs, helpers. Only a thin slice is actually compose-specific (which kind name maps to which compiled resource). If Wave 8 (`tkr` wiring) hardens against that shape, every future platform copies the 800 lines, and `tokeira-platform-dsl` keeps accreting platform knowledge (`KindLibrary::compose()` lives in the compiler today — `crates/tokeira-platform-dsl/src/kind.rs`).

The operator and platform-author experience we want:

- **Platform author** writes the *least possible* Rust: the `.platform` definition (DSL), a kind library (data), and a small realizer (one arm per kind, reusing the existing resource crates). No orchestration, no persistence, no `Ops` glue.
- **Operator** writes *no Rust*: selects a platform, supplies input values, and may edit the persisted `.platform` to evolve structure (an ordinary `apply`, per Requirement 16).
- The `.platform` definition carries as much of the deployment's specification as possible; conventions live as **data in the kind library**, not as **logic in the realizer**.
- `tokeira-platform-dsl` stays a pure **compiler** — no deployment orchestration, no persisted config, no platform specifics.

## The core insight

A DSL platform has exactly **three** platform-specific things:

1. its **kind library** — `KindSchema` entries (names, field types, defaults, constraints, outputs). Data.
2. its **realizer** — one arm per kind mapping a `CompositionItem`/`CompositionImage` to a compiled engine resource/service/image. The only substantial Rust.
3. its **authored `.platform`** files.

Everything else — plan building, `DslOwnedResource`, the `Deployment`/`Ops` impls, writeback resolution, `RuntimeContext` resolution and precedence (Req 14.8/14.9), create-persist, and the loader — is identical across platforms and belongs in **one** reusable framework crate.

## Naming

The framework crate is **`tokeira-platform`**: the platform framework on which concrete platforms are built, with **`Realizer`** as its core trait — the exact parallel of `tokeira-iac` (the IaC framework) whose core trait is `Resource`. It slots into the provisioner spec's own sentence ("`tkp` owns the IaC engine, **the platforms**, and the AWS resource implementations") as the framework those platforms specialise:

> `tkp` owns the IaC engine, the **platform framework** (`tokeira-platform`), the platforms (`compose-dsl`, `ecs-dsl`), and the AWS resource implementations.

It pairs as **define/compile** (`tokeira-platform-dsl`) → **realize** (`tokeira-platform`). Names rejected: `-host` (the host *process* is `tkp`; this is a library it consumes, not the host) and `-runtime` (collides with `tokeira-runtime`, the kernel runtime). If the `-dsl`-as-apparent-suffix overlap grates, `tokeira-platform-realize` is the fallback.

## Crate responsibilities (target state)

| Crate | Owns | May depend on | MUST NOT depend on |
|-------|------|---------------|--------------------|
| `tokeira-platform-dsl` | **Compiler only**: assemble → lex → parse → resolve → type-check → evaluate → `Composition` IR; the `KindSchema` / `FieldType` / `Constraint` / `KindLibrary` *types*. Pure and total. | `logos`, `thiserror` | `tokeira-iac`, `-deploy-engine`, `-orchestrator`, `-state`; any platform crate; persistence/orchestration |
| **NEW `tokeira-platform`** | The platform **framework**: the `Realizer` trait (core), `DslPlatform<R>` implementing `Deployment` + `Ops`, the plan, `DslOwnedResource`, the local-state bootstrap, `RuntimeContext` resolution + precedence, writeback resolution, create-persist, the loader, leak helpers, and shared host helpers (e.g. `aws_clients`). | `-platform-dsl`, `-iac`, `-deploy-engine`, `-orchestrator`, `-state`, `-aws` | any specific platform crate (`-compose`, `-ecs`, …) |
| `platforms/compose-dsl` (slimmed) | A compose **`Realizer`**, the compose **kind library**, the embedded `.platform` files, one `platform()` constructor. Target ~150 lines. | `-platform`, `-platform-dsl`, `-compose`, `-compose-deployment`, `-aws` | — |
| `tokeira-compose`, `-compose-deployment`, `-aws` | Unchanged — the **executable** resource/service/image kinds the realizer maps onto (`ComposeService`, `DsqlCluster`, `DynamoDbTable`, `ObservabilityConfigFilesResource`, images). | | |

Two relocations vs. today:

- **`KindLibrary::compose()` leaves `tokeira-platform-dsl`** (`kind.rs`) → into `platforms/compose-dsl`. The compiler must not know about compose. The dsl crate keeps only the schema *types* (`KindSchema`, `FieldType`, `Constraint`, the empty `KindLibrary` registry).
- **All of `compose-dsl/deployment.rs` except the realizer** → into `tokeira-platform`.

## The seam — the `Realizer` trait (core trait of `tokeira-platform`)

`KindSchema` is the **compile-time** half of a kind and lives in the pure compiler; the realizer is the **run-time** half and needs `tokeira-iac`, so it cannot live in the compiler. The two halves are paired by kind-name string. This is the one well-contained boundary the whole design rests on, and it is `tokeira-platform`'s core trait the way `Resource` is `tokeira-iac`'s.

```rust
/// Resolved identity + ambient context the framework injects for realization.
/// Built by the framework's RuntimeContext resolution (recorded-identity
/// precedence, Req 14.8/14.9) — the platform author never resolves this.
pub struct RealizeContext {
    pub project_name: String,
    pub deployment_dir: PathBuf,
    pub region: Option<String>, // recorded-identity; None for in-memory
}

/// The per-platform half of a kind library: turn lowered items into the engine's
/// compiled resources. The ONLY substantial Rust a platform author writes, and
/// the core trait of `tokeira-platform` (the `Resource`-analog).
#[async_trait]
pub trait Realizer: Send + Sync {
    /// Compile-time schemas (paired with the arms below by kind name).
    fn kind_library(&self) -> &KindLibrary;

    fn realize_resource(&self, item: &CompositionItem, cx: &RealizeContext)
        -> Result<Box<dyn iac::Resource>, RealizeError>;

    /// A service lowers to BOTH an infra resource and a workload; the framework
    /// calls `realize_resource` and `realize_service` for a service item
    /// (mirroring today's `OwnedComposeResource` + `ComposeWorkload`).
    fn realize_service(&self, item: &CompositionItem, cx: &RealizeContext)
        -> Result<Box<dyn deploy_engine::Service>, RealizeError>;

    fn realize_image(&self, image: &CompositionImage, cx: &RealizeContext)
        -> Result<Box<dyn deploy_engine::Image>, RealizeError>;

    /// Host-edge effects (provider handles, AWS clients). Default: no-op.
    async fn register_infra_extensions(
        &self, comp: &Composition, ctx: &mut ProvisionContext, cx: &RealizeContext,
    ) -> Result<()> { Ok(()) }

    /// Day-2 backend (scale/logs/port mappings). Compose → ComposePlatform;
    /// ECS → AWS. This is the part that genuinely differs between Docker and AWS,
    /// so it is delegated rather than generic.
    fn ops(&self) -> &dyn PlatformOpsBackend;
}
```

The generic deployment, parameterised by the realizer, replaces ~700 lines of today's `deployment.rs`:

```rust
pub struct DslPlatform<R: Realizer> {
    realizer: R,
    authored: &'static [(&'static str, &'static str)], // embedded .platform set
}

impl<R: Realizer> tokeira_orchestrator::Deployment for DslPlatform<R> {
    type Config = ConfigurationRevision; // generic: the compiled Composition + RealizeContext + writeback

    fn infra_modules(&self, cfg, sel) -> Vec<Box<dyn iac::Module>> {
        // GENERIC: walk cfg.composition.modules; for each item call
        // realizer.realize_resource; wrap in DslOwnedResource(id="<plat>/<id>",
        // module, deps); prepend the local-state bootstrap module.
    }
    fn services(&self, cfg) -> Vec<Box<dyn deploy_engine::Service>> { /* realize_service per service */ }
    fn images(&self, cfg)   -> Vec<Box<dyn deploy_engine::Image>>   { /* realize_image per image */ }
    fn collect_writeback(&self, cfg, state) -> Vec<(String,String)> { /* resolve OutputRefs from state */ }
    async fn register_infra_extensions(..) { self.realizer.register_infra_extensions(..).await }
    // create-persist (materialize the authored set), the loader (compile the
    // persisted set against realizer.kind_library()), and RuntimeContext
    // precedence ALL live here — generic, once.
}

// Ops is generic too, delegating the four verbs to self.realizer.ops().
```

`ConfigurationRevision` (generic, in `tokeira-platform`) is the in-memory **desired-state definition** the realizer consumes — the compiled `Composition` plus the resolved `RealizeContext` and writeback targets. It is what `tkr`'s `PlatformDeploymentConfig::ComposeDsl(..)` carries; it is **not** a per-platform config struct, which is what keeps Wave 8's `deployment_dir.rs` from growing a new bespoke config type per DSL platform. The name deliberately matches the provisioner's "configuration revision" — see *Relationship to `tkp`* below for the boundary.

## The slim `compose-dsl` (all the Rust a platform author writes)

```rust
//! platforms/compose-dsl/src/lib.rs — the whole crate, ~150 lines.
use tokeira_platform::{DslPlatform, Realizer, RealizeContext, RealizeError, PlatformOpsBackend};

mod kinds; // the compose KindLibrary (data; moved out of tokeira-platform-dsl)
mod ops;   // ComposePlatform-backed PlatformOpsBackend (today's task-9.5 verbs)

const AUTHORED: &[(&str, &str)] = &[
    ("compose.platform", include_str!("../platform/compose.platform")),
    // infra / observability / runtime / images
];

pub struct ComposeRealizer { kinds: KindLibrary, ops: ops::ComposeOps }

impl Realizer for ComposeRealizer {
    fn kind_library(&self) -> &KindLibrary { &self.kinds }

    fn realize_resource(&self, item: &CompositionItem, cx: &RealizeContext)
        -> Result<Box<dyn iac::Resource>, RealizeError> {
        Ok(match item.kind.as_str() {
            "LocalStateDir"            => Box::new(LocalStateDir::new(cx.deployment_dir.join("state"))),
            "ObservabilityConfigFiles" => Box::new(ObservabilityConfigFilesResource::new(
                                              cx.deployment_dir.clone(), obs_params(item)?)),
            "DsqlCluster"              => Box::new(dsql_cluster(item, cx)?),
            "DynamoDbTable"            => Box::new(dynamodb_table(item, cx)?),
            other => return Err(RealizeError::unknown(other)),
        })
    }
    fn realize_service(&self, item, _cx) -> Result<Box<dyn deploy_engine::Service>, RealizeError> {
        Ok(Box::new(ComposeWorkload { service: compose_service(item)? })) // reuse tokeira-compose
    }
    fn realize_image(&self, image, _cx) -> Result<Box<dyn deploy_engine::Image>, RealizeError> {
        Ok(Box::new(build_or_mirror(image)))
    }
    async fn register_infra_extensions(&self, comp, ctx, cx) -> Result<()> {
        ctx.set_extension(ComposePlatform::connect(
            &cx.deployment_dir.join("docker-compose.yml"), &cx.project_name)?);
        if comp.uses_dsql() { ctx.set_extension(tokeira_platform::aws_clients(cx).await?); }
        Ok(())
    }
    fn ops(&self) -> &dyn PlatformOpsBackend { &self.ops }
}

/// The single symbol `tkr` references for this platform.
pub fn platform() -> DslPlatform<ComposeRealizer> {
    DslPlatform::new(ComposeRealizer::new(), AUTHORED)
}
```

The realizer arms are pure "typed fields → constructor". No plan, no `DslOwnedResource`, no writeback walking, no `Ops` glue, no ctx resolution, no create/persist — all in `tokeira-platform`.

## Make the `.platform` carry more, so the realizer is dumber

Today `obs_params()` and `dsql_cluster()` bake in conventions (the Mimir/Loki URLs, ports, retention; the DynamoDB table-name format; the cluster identity) — see `platforms/compose-dsl/src/deployment.rs` `observability_params_from` and the table-name `format!`. Two moves push that burden off the realizer:

1. **Compile-time field defaults on the kind schema.** Extend `FieldSpec` with an optional default the evaluator fills when the field is unset:

   ```rust
   FieldSpec::optional_default("mimir_remote_write_url", FieldType::Str, "http://mimir:9009/api/v1/push")
   FieldSpec::optional_default("loki_retention_hours",   FieldType::Int, 168)
   ```

   The realizer then reads fully-populated typed fields with no embedded constants, and the operator can still override any of them from the `.platform`. Convention becomes **data in the kind library**, not **logic in the realizer**.

2. **Express derived names in the DSL, not the realizer.** Cluster identity and coordination-table names become explicit `let`s / fields:

   ```
   let cluster_id = project_name ++ "-compose"
   module dsql when storage is Dsql {
     match storage { Dsql(d) => {
       resource cluster      = DsqlCluster   { identity: cluster_id, region: d.region, mode: d.mode }
       resource rate_limiter = DynamoDbTable { table: project_name ++ "-dsql-rate-limiter", hash_key: "pk", ttl: "ttl_epoch" }
       resource conn_lease   = DynamoDbTable { table: project_name ++ "-dsql-conn-lease",   hash_key: "pk", ttl: "ttl_epoch" }
     } _ => {} }
   }
   ```

   The realizer's `dynamodb_table(item)` then just reads `item.fields["table"]` — no `format!`, no convention.

## Who implements what

- **Platform author** (once per platform, e.g. future `eks`):
  - the `.platform` files (DSL),
  - a kind library (data: schemas + defaults + constraints + outputs),
  - a `Realizer` (one arm per kind, reusing existing resource crates) + a `PlatformOpsBackend`,
  - a one-line `platform()` and the registration touch-points in `tkr` (see Wave 8).
- **Operator**: selects a platform, supplies input values, optionally edits the persisted `.platform` to evolve structure. No Rust.

## Relationship to `tkp`

`tkp` (the provisioner binary, `platform-provisioner-binary` spec) owns the IaC engine, the **platform framework** (`tokeira-platform`), the **platforms** (`compose-dsl`, `ecs-dsl`), and the AWS resource implementations. This proposal defines the framework `tkp` contains; it does **not** take on any of `tkp`'s provenance/binding/retention responsibilities. The boundary is precise:

- **Engine identity** is the provisioner's binding key — `tokeira-build-info::SOURCE_TREE_HASH` over the *engine/resource-implementation surface*. The `Realizer` arms, the resource crates they call (`tokeira-compose`, `-aws`, …), and `tokeira-platform` itself are part of that surface. So **changing a realizer or a resource implementation is an engine-identity change**, gated through `tkp upgrade` — never a silent apply. This is exactly why the realizer is the right home for "how a kind becomes a resource": it is correctly inside the versioned surface.
- **Editing the `.platform`** changes only the desired state and is an **ordinary `apply`**, orthogonal to engine identity — the Requirement 16 evolution envelope. The `.platform` is *not* part of engine identity.
- **Three layers, not two.** The `.platform` files are the authored, persisted **source** (retention + digest owned by Req 11/16 and the provisioner). The compiler turns them into a **`Composition`** (the desired-state definition), which `tokeira-platform` carries as a **`ConfigurationRevision`**. `tkp` then *records and versions* that as its **configuration revision** in the deployment state envelope. `tokeira-platform` **compiles and realizes; it does not stamp, version, retain, or record revisions** — that is `tkp`'s job. We deliberately do **not** equate the `.platform` with tkp's configuration revision: the source compiles to the desired state, which `tkp` records as a versioned revision.

## On-disk deployment layout and config persistence

This section pins **exactly how a deployment's configuration is defined and where each piece lives on disk**, so the boundary between *authored*, *operator-chosen*, *generated*, and *server* config is unambiguous. The reference is a `compose-dsl` deployment under the `tkr` state root (the pre-DSL `compose` equivalent is `dev-dsql`, whose `deployment.toml` this design splits).

### Config taxonomy — four distinct things, four homes

1. **Definition / structure** — modules, resources, services, their wiring, the input *declarations* + their defaults, and the writeback targets. **Defined by** the platform author in the `.platform` files (shipped in the platform crate). **Persisted** into the deployment as the authored file set at `tkr deployment create`. It is the deployment's config *source*; the operator may later edit the persisted copy (an ordinary `apply`, Req 16), but it is never regenerated from the crate.
2. **Input values** — the operator's choices that select among the definition's options (`storage`, `region`, image refs, replicas, ports). **Defined by** the operator at create (editable after). **Persisted** as an explicit **input-bindings snapshot** (`inputs.toml`). Defaults live in the `.platform`; the snapshot records only what the operator set — including recorded-identity values such as `region`.
3. **Ambient context** — `deployment_dir`, `home`. **Supplied by the host** at every invocation and **never persisted** (re-derived per host — the ambient-never-retained rule, Req 14.9).
4. **Server runtime config** — `tokeirad.toml` (`TokeiraConfig`). **Not the DSL's** (Req 1.4). Seeded at create by the host (`prototypical_server_config`) from the create-time inputs (`storage`, `region`); the DSL's `writeback` block names the *post-apply* keys (`infrastructure.dsql.endpoint`, the coordination-table names) the host writes back into it.

Everything else on disk is **derived or runtime state**, not config.

### Layout

```
<tkr-state-root>/<name>/
  platform/                      # (1) AUTHORED definition — the desired-state source
    compose.platform             #     root: inputs+defaults, lets, namespaces, writeback, `use`
    infra.platform               #     `use`d modules (depth ≤ 1; this dir is the import-containment root)
    observability.platform
    runtime.platform
    images.platform
  inputs.toml                    # (2) OPERATOR input bindings (overrides of declared defaults) + recorded identity
  tokeirad.toml                  # (4) SERVER config — seeded at create, writeback-updated; NOT the DSL
  metadata.json                  #     identity/status: name, id, platform=compose-dsl, storage, timestamps
  docker-compose.yml             # GENERATED at apply (compose provider artifact)
  config/                        # GENERATED at apply (observability config files + dashboards)
  state/{infra,deploy}/…         # engine CAS state (tokeira-state)
  .tokeira-state/…               # container runtime data volumes (mimir/loki/grafana)
```

Two roots, deliberately separated:

- the **definition root** `<name>/platform/` is the DSL import-containment boundary — a `use` cannot escape it, and it contains *only* authored files (no generated artifacts, no state). Isolating it tightens the security boundary versus compiling from the deployment root.
- `ctx.deployment_dir` is `<name>/` (the parent). The `.platform` builds paths under it (`ctx.deployment_dir / ".tokeira-state"`, `… / "config"`) that the realizer passes to kinds; these are path *values*, never filesystem reads (Req 12.1).

This **refines the current code**, which compiles from `<deployment_dir>/compose.platform` directly (`ROOT_DEFINITION` at the deployment root); the layout moves the authored set into `platform/` so the containment root excludes generated and state files. `ctx.deployment_dir` stays the parent.

### Lifecycle — who writes what, when

- **`tkr deployment create`** persists the *config inputs only*: materializes the authored `platform/` set, writes `inputs.toml` (operator values + recorded identity), seeds `tokeirad.toml`, writes `metadata.json`. No engine artifacts yet.
- **`tkr infra|deploy plan|apply`** compiles `platform/` + `inputs.toml` + ambient ctx → `ConfigurationRevision` → realizes → engine resources. The realizer's providers *generate* `docker-compose.yml` and `config/` and write `state/`; writeback updates `tokeirad.toml`. These are **derived and reproducible**, not retained as config source.

### What defines the config, precisely

A deployment's configuration is **exactly**:

```
config  ==  platform/ file set (digested)      # structure + defaults + writeback declarations
          + inputs.toml                          # operator values + recorded identity
          + (language, kind-library) version     # the engine identity that compiles them
```

Given those three, the `ConfigurationRevision` — and therefore every generated artifact (`docker-compose.yml`, `config/`, the realized engine resources) — is reproducible. So **only the first two are retained as config**; the third is the engine identity the provisioner already binds. The content digest is taken over the sorted `(relative_path, sha256)` of `platform/` (Req 13.6) together with the `inputs.toml` digest.

This is the **input-bindings snapshot** that was previously implicit: `platform/` alone cannot reconstruct a DSQL deployment, because its `storage` default is `InMemory` — the operator's `storage = Dsql { region }` choice must be recorded, and `inputs.toml` is its home. It is also where recorded-identity precedence (Req 14.8) anchors: a host ambient value conflicting with a recorded `inputs.toml` identity is a retarget requiring confirmation.

### Boundary with `tkp`

This proposal defines the **shape** of the config-definition artifacts (`platform/`, `inputs.toml`) and how they compile. The **versioning, provenance-stamping, integrity manifest, and retention** of `platform/` + `inputs.toml` + the engine identity are the provisioner's (`platform-provisioner-binary`): together they become the deployment's recorded *configuration revision* and its rollback checkpoints. `tokeira-platform` writes the create-time artifacts and compiles them; it does not stamp, version, or retain them.

## What moves, concretely (migration map)

| Today (`compose-dsl/deployment.rs` / `platform-dsl/kind.rs`) | Target |
|---|---|
| `ComposeDslConfig`, `ModulePlan`, `ItemPlan`, plan building in `from_composition` | generic `ConfigurationRevision` + plan in `tokeira-platform` |
| `DslOwnedResource`, `LocalStateModule`/`LocalStateDirResource`, `leak_*` | `tokeira-platform` |
| `Deployment`/`Ops` impls, `collect_writeback`, `resolve_output` | `tokeira-platform` (generic) |
| `register_infra_extensions` AWS-clients preflight | `tokeira-platform::aws_clients` helper, called by the realizer |
| `compose_service_from`, `dsql_cluster_resource`, `dynamodb_table_resource`, `DslImage`, `obs_params` | stay in `compose-dsl` as the realizer arms |
| `KindLibrary::compose()` | moves from `platform-dsl/kind.rs` to `compose-dsl/kinds.rs` |
| `Ops` 9.5 verbs (`scale`/`logs`/`port_mappings` via `ComposePlatform`) | `compose-dsl` `PlatformOpsBackend` impl |

## Risk and sequencing

The plan's task 12 says *extract the reusable runtime after a second platform (`ecs-dsl`) exists* — "extract from two examples, not one". This proposal is the **bet** on where the seam goes (`Realizer` + `DslPlatform<R>` + field defaults). Confidence is moderate-to-high because `platforms/ecs/src/services.rs` and `platforms/compose/src/services.rs` already share the shape (a fixed service list + module ownership + storage-conditional wiring), and the design's ECS preview adds only *kinds* and one language feature (output references), not new orchestration.

The parts most likely to need adjustment when `ecs-dsl` lands:

- `register_infra_extensions` / the `aws_clients` helper shape (Docker vs. AWS differ most here).
- `PlatformOpsBackend` (Compose's Docker verbs vs. ECS's AWS verbs).
- Whether some kinds (`DsqlCluster`, `DynamoDbTable`, `SecretsManagerSecret`) and their realizer arms are shared between compose and ecs (a small shared `aws-kinds` module) rather than duplicated.

**Recommended order:** validate the `Realizer` signature against a thin `ecs-dsl` realizer *before* moving compose-dsl's code wholesale; then extract `tokeira-platform` from the two; then point Wave 8 at `DslPlatform<R>` so `tkr` gains DSL platforms generically (one `ConfigurationRevision`, not one config type per platform).

## How this updates the task plan

- Task 12 gains the crate name (`tokeira-platform`), the `Realizer`/`DslPlatform<R>` contract, the `ConfigurationRevision` config type, and the `FieldSpec` default extension as an explicit sub-item.
- Task 13 (`ecs-dsl`) is reframed as "the second realizer that validates the seam", to be done **before** the extraction is finalized.
- Wave 8 (`tkr` wiring, task 11) should target the generic `ConfigurationRevision` / `DslPlatform<R>` rather than a compose-specific config, so adding a future DSL platform is a `Realizer` + kind library + `.platform` set, with no new `tkr` dispatch type.
