# Requirements Document: Image Lifecycle

## Introduction

Tokeira ships a single server binary today, `tokeirad`, that runs on three platforms: `local` (bare-process), `compose` (Docker Compose), and `ecs` (AWS ECS on EC2, private-only). Today the workspace has no documented path for producing the `tokeirad` container image that the compose platform defaults to (`tokeirad:latest`), no path for publishing it to ECR, and no path for mirroring the pinned third-party images the ECS platform depends on. In a private-only VPC with no internet gateway, direct pulls from Docker Hub fail, so every image referenced by an ECS task definition must already live in a project-owned ECR repository before `tkr infra apply` runs.

This spec owns the image plane of the deployment lifecycle. It strengthens Tokeira's existing IaC abstractions by completing the `Image` trait already declared in `tokeira-deploy-engine::image`, populating it with concrete per-platform image implementations, and introducing a Dagger-backed build/publish/mirror pipeline alongside an ECR repository resource.

The design follows a clean separation between **image resolution** (what ref does a service deploy against — a `tokeira-deploy-engine` concern) and **image production** (how do we build, push, or mirror a specific image — a `tokeira-build` concern):

- `tokeira-deploy-engine::image` owns the `Image` trait, `ImageSourceType`, `DesiredImageRef`, `ImageContext`, and `WritebackTarget`. It is the deployment-resolution abstraction. It has no knowledge of Dagger, Docker, or any build pipeline.
- `platforms/compose/src/images/` and `platforms/ecs/src/images/` each own concrete `Image` trait implementations for their platform. Each platform's image structs read that platform's own config type directly. Each platform exposes two entry points: `construct()` — context-free enumeration used by `Deployment::images(&self, config)` — and `all(ctx)` — constructs then validates, used by CLI handlers that already have an `ImageContext`. No cross-platform imports; no shared config DTO.
- `tokeira-build` owns the Dagger-backed build/publish/mirror free functions (`build_tokeirad_image`, `publish_image`, `mirror_image`) and the `DaggerClient` trait. Each build recipe is hardcoded in the corresponding function. `tokeira-build` is not aware of the `Image` trait and does not walk any image registry.
- `tkr image` dispatches on the active deployment's platform when the subcommand needs config (`list`, `push`, `mirror`) and is config-free for the `build` subcommand (which knows statically which images are buildable).

### What this spec delivers

- Widen `tokeira-deploy-engine::Image::desired_ref` to return `Result<DesiredImageRef, RuntimeError>` and add a default-empty `writeback_targets(ctx) -> Vec<WritebackTarget>` method. Introduce `WritebackTarget { field: &'static str }`.
- A new `tokeira-build` library crate at `crates/tokeira-build/` exposing three free functions (`build_tokeirad_image`, `publish_image`, `mirror_image`) plus the `DaggerClient` trait as the public testing seam.
- A new in-repo `dagger-client` crate at `crates/dagger-client/` providing a minimal GraphQL wrapper over a Dagger session.
- `platforms/compose/src/images/{tokeirad.rs, observability/mod.rs}` concrete `Image` trait implementations reading `ComposeConfig` directly. Each file's `pub fn all() -> Vec<Box<dyn Image>>` enumerates that submodule's images; a root `platforms::compose::images` module exposes `construct()` and `all(ctx)` — the former context-free for `Deployment::images`, the latter validated for CLI callers.
- `platforms/ecs/src/images/{tokeirad.rs, observability/mod.rs}` concrete `Image` trait implementations reading `EcsConfig` directly. Same `construct()` / `all(ctx)` shape.
- An `EcrRepository` resource implementation in `tokeira-aws` + an `EcrClient` trait + an `EcrClientHandle` `ProvisionContext` extension + ad-hoc `ensure_ecr_repository` / `ensure_ecr_repositories` helpers.
- An `ImagesModule` IaC module in `platforms/ecs/src/modules/images.rs` that enumerates `platforms::ecs::images::all(ctx)` (the validated, context-bearing variant) and registers one `EcrRepository` per image, so `tkr infra plan`/`destroy` see every project-owned repository.
- A `tkr image` command group in `apps/tkr` with four subcommands: `list`, `build`, `push`, `mirror`.
- Writeback driven by `image.writeback_targets(ctx)` rather than CLI-side hardcoded field lists, calling a new public `tokeira_iac::write_config_values` helper extracted from the existing private helper in `apps/tkr/src/commands/infra.rs`.
- Platform lifecycle gates: ECS `validate_mirrors` / `validate_builds` (pure predicates over a platform's image list + config); compose `validate_local_build` plus a `DockerImageInspector` trait so the gate is mockable without a live Docker daemon.

### What this spec does NOT cover

- CI/CD pipeline integration. The [`pipeline-foundation`](../pipeline-foundation/requirements.md) spec defines the CI substrate; a future pipeline crate may wrap `tokeira-build` as a library.
- Multi-region mirroring — one mirror region per deployment.
- Image signing, SBOM generation, or vulnerability scanning.
- Compose-platform image loading (Docker Compose reads the local image cache directly; no additional action is required for `tkr image build` to satisfy `tokeirad:latest`).
- A cross-platform version-consistency linter (comparing compose and ECS image pins). Each platform owns its own pins; cross-platform comparison, if ever needed, lives in a separate operator-facing tool outside this spec.

### Cross-references

- [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md): Progress callbacks on `ProvisionContext` and TOML writeback via `toml_edit` are owned there. The writeback helper extraction (Req 7.3) extends `tokeira-iac` with a public API consumed by `tkr infra` and `tkr image`.
- [`ecs-deployment`](../ecs-deployment/requirements.md): Requires that ECR repositories exist and that `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields be populated before `tkr infra apply` or `tkr deploy apply` can succeed. This spec owns that image plane.
- [`tkr-cli`](../tkr-cli/requirements.md): Owns the global CLI structure, `--deployment` / `--json` flags, XDG paths, and command-tree conventions. This spec adds a new `image` command group that follows those conventions.
- [`pipeline-foundation`](../pipeline-foundation/requirements.md): Future image-related pipelines wrap the library surface defined here.

## Glossary

- **Image_Trait**: The `tokeira_deploy_engine::Image` trait every deployable artifact implements. Provides `name()`, `source_type()`, `desired_ref(ctx)`, and a default-empty `writeback_targets(ctx)`. Already declared in `tokeira-deploy-engine::image`; this spec widens `desired_ref` to return `DesiredImageRef` (currently `String`) and adds `writeback_targets`.
- **Image_Source_Type**: The `ImageSourceType` enum declared in `tokeira-deploy-engine`. Variants: `Build` (produced locally via a Dagger pipeline), `Mirror` (pulled from an upstream ref and re-pushed to a project-owned destination), `Registry` (pulled from a registry as-is with the configured ref used verbatim — not used by any image declared in this spec but given an unambiguous contract in Req 1.6 for future use).
- **Desired_Image_Ref**: The `DesiredImageRef` struct declared in `tokeira-deploy-engine`. Fields: `repository` (project-scoped name without registry host prefix), `tag`, and `upstream_ref: Option<String>` (`Some` for Mirror, `None` for Build).
- **Image_Context**: The `ImageContext` struct declared in `tokeira-deploy-engine`. Carries platform state and typed extensions; platforms register their own config type on it before invoking image trait methods.
- **Writeback_Target**: The `WritebackTarget { field: &'static str }` struct declared in `tokeira-deploy-engine`. Images return a `Vec<WritebackTarget>` from `writeback_targets(ctx)` describing which dotted `deployment.toml` keys their remote ref populates after push or mirror.
- **Build_Crate**: The `tokeira-build` library crate at `crates/tokeira-build/`. Owns the Dagger-backed pipelines `build_tokeirad_image`, `publish_image`, `mirror_image` as free functions with hardcoded recipes. Does not implement the `Image` trait and does not enumerate platforms' image registries.
- **Dagger_Client**: The in-repo GraphQL client wrapper at `crates/dagger-client/` that drives a Dagger session from Rust. Introduced by this spec; reference implementation at `.kiro/specs/image-lifecycle/reference/`.
- **Image_CLI**: The `tkr image` command group in `apps/tkr`, exposing `list`, `build`, `push`, and `mirror` subcommands.
- **Platform_Image_Module**: A Rust module under `platforms/{compose,ecs}/src/images/` that declares concrete `Image` trait implementations for that platform. Each module exposes `construct() -> Vec<Box<dyn Image>>` (context-free enumeration) and `all(ctx) -> Result<Vec<Box<dyn Image>>, RuntimeError>` (validated resolution) per Req 2.3.4. Reads the platform's own config type directly — no cross-platform imports.
- **ECR_Registry**: The Amazon Elastic Container Registry in a specific AWS region and account, identified by its registry host (`{account}.dkr.ecr.{region}.amazonaws.com`).
- **Project_Repository**: An ECR repository owned by the Tokeira deployment. Its name is the `DesiredImageRef.repository` field verbatim — already project-scoped by the platform's image implementation (for example `"tokeira-dev/tokeirad"` or `"tokeira-dev/grafana-mimir"`). No additional prefixing occurs downstream. `DesiredImageRef.repository` is the full project-scoped repository name without registry host prefix.
- **Lifecycle_Policy**: The ECR repository lifecycle policy JSON applied by this spec. Canonical policy: keep the last 10 untagged images; tagged images are never expired by lifecycle rules.
- **Mirror_Operation**: The act of pulling a Mirror image's `upstream_ref` and publishing it to the corresponding Project_Repository.
- **Push_Operation**: The act of publishing a Build image's locally-produced artifact to a Project_Repository under two tags: `latest` and a version-specific tag supplied by the operator (defaulting to `latest` when no version is supplied).
- **Registry_Credentials**: The username, password, and registry host obtained by calling ECR `GetAuthorizationToken` and base64-decoding the returned token in `user:password` form.
- **Image_Writeback**: The act of writing discovered image references back into `deployment.toml` under specific TOML keys, using `toml_edit` to preserve comments and formatting. Each image declares its own writeback targets.
- **Image_Tag_Mutability**: The ECR repository setting that controls whether a tag (e.g., `latest`) may be overwritten. All repositories created by this spec SHALL be set to `MUTABLE` so `latest` can move with each push.
- **Reproducible_Build**: A build in which the same source tree plus the same pinned toolchain (`rust-toolchain.toml`) produces an image whose application binary layer is bit-identical across invocations on the same host architecture.
- **Target_Architecture**: The CPU architecture of a Build image: `arm64` (default — Graviton4 on ECS, native on Apple Silicon for compose) or `amd64` (operator override for x86 hosts and Intel-based deployments).

## Requirements

---

## Feature 1: Image Trait Extensions in `tokeira-deploy-engine`

### Requirement 1.1: Widen `desired_ref` return type

**User Story:** As a Tokeira developer, I want `Image::desired_ref` to return a structured `DesiredImageRef` rather than a plain string, so that downstream consumers (ECR provisioning, writeback, mirror pipeline) can read the repository, tag, and upstream ref as distinct fields without re-parsing.

#### Acceptance Criteria

1. THE `tokeira_deploy_engine::Image::desired_ref` method signature SHALL be `fn desired_ref(&self, ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError>`. The existing `Result<String, RuntimeError>` signature SHALL be replaced in this spec's implementation phase; no backwards-compatibility shim is required.
2. THE `DesiredImageRef` struct SHALL carry `repository: String` (project-scoped name without registry host prefix, e.g. `"tokeira-dev/tokeirad"`), `tag: String`, and `upstream_ref: Option<String>`.
3. FOR Build images, `desired_ref(ctx)?.upstream_ref` SHALL be `None`. FOR Mirror images, it SHALL be `Some(_)` with a fully-qualified source ref.
4. THE `repository` field SHALL always begin with the deployment's project name prefix FOR Build and Mirror images. An image of those source types that produces a repository without the project prefix SHALL fail validation at image-module construction time. Registry images (Req 1.6) are exempt from the project-prefix rule.
5. THE `tag` field SHALL never include `/`, `@`, or `:`. Tag validation SHALL reject any such input.
6. EXISTING code paths that use the current `Result<String, RuntimeError>` return SHALL migrate to the new shape as part of this spec's implementation. If no such code paths exist at spec-implementation time, this is a no-op.

### Requirement 1.2: Add `writeback_targets` default-empty method

**User Story:** As a Tokeira developer adding a new image, I want the image itself to declare which config field the push or mirror flow writes back to, so that adding an image is one change in one place rather than a multi-file coordinated edit.

#### Acceptance Criteria

1. THE `Image` trait SHALL expose `fn writeback_targets(&self, ctx: &ImageContext) -> Vec<WritebackTarget>` with a default implementation returning an empty `Vec`.
2. THE `WritebackTarget` struct SHALL contain `field: &'static str` (dotted TOML key path). No `kind` or arity enum — arity is expressed by the length of the returned `Vec`.
3. THE default empty implementation SHALL satisfy images whose refs are consumed only by operator-facing tooling (not by config). Images whose refs are written back to config SHALL override.
4. `tkr image push` and `tkr image mirror` SHALL iterate `image.writeback_targets(ctx)` rather than carrying their own hardcoded field list.

### Requirement 1.3: Fallible extension lookup

**User Story:** As a Tokeira maintainer, I want concrete `Image` implementations to return an `IacError`/`RuntimeError` when a required `ImageContext` extension is missing, so that misconfigured platform wiring surfaces as an operator-facing error instead of a panic.

#### Acceptance Criteria

1. CONCRETE `Image` implementations in platform crates SHALL obtain required config via `ctx.extension::<T>().ok_or_else(|| RuntimeError::Image(format!("image context missing extension: {}", std::any::type_name::<T>())))?`.
2. THE same fallible-lookup pattern SHALL apply to `EcrRepository::create`/`update`/`delete`/`describe` when fetching `EcrClientHandle` from `ProvisionContext`. The error type SHALL be `IacError::Other(anyhow!(...))` with a message naming the missing extension.
3. NO `Image` implementation SHALL use `.expect()` or `.unwrap()` on a `ctx.extension::<T>()` lookup.

### Requirement 1.4: `ImageContext` population hook on the `Deployment` trait

**User Story:** As a Tokeira maintainer, I want the deploy-engine flow to populate `ImageContext` with the deployment's platform config before calling any `Image` trait method, so that `deploy apply` and any other consumer receives a context carrying the extensions concrete images require.

#### Acceptance Criteria

1. THE `tokeira_orchestrator::Deployment` trait SHALL gain a new method `async fn register_image_extensions(&self, config: &Self::Config, ctx: &mut deploy_engine::ImageContext) -> Result<()>` with a default empty implementation.
2. `DeployEngine::new` SHALL construct its `ImageContext` by calling `ImageContext::default()` and then `deployment.register_image_extensions(config, &mut image_ctx).await?` before storing it on the facade.
3. THE compose platform's `ComposeDeployment::register_image_extensions` SHALL register `ComposeConfig` on the supplied context via `ctx.set_extension(config.clone())`.
4. THE ECS platform's `EcsDeployment::register_image_extensions` SHALL register `EcsConfig` on the supplied context.
5. THE local platform's `LocalDeployment::register_image_extensions` SHALL remain the default empty implementation (local has no image set).
6. THE `tkr image list`, `tkr image push`, and `tkr image mirror` handlers SHALL construct their `ImageContext` the same way: construct a default context, then call `deployment.register_image_extensions(config, &mut ctx).await?` before iterating the platform's image set. This keeps `deploy apply` and `tkr image <subcommand>` on one wiring path.
7. `tkr image build` SHALL NOT construct an `ImageContext` — the build subcommand is deployment-free (Req 6.3).

### Requirement 1.5: Structured desired refs mapped to persisted image state

**User Story:** As a Tokeira maintainer, I want the deploy engine to translate the structured `DesiredImageRef` into the existing `tokeira_iac::ImageState` / `ImageSource` shape, so that widening the trait return type does not break state persistence or require a state-format migration.

#### Acceptance Criteria

1. `ServiceEngine::record_images` SHALL continue to write each resolved image into `state.images` as `ImageState { name, resolved_ref, digest: None, published_at, source }`.
2. THE `resolved_ref: String` field SHALL be populated by formatting the `DesiredImageRef`: `format!("{}:{}", desired.repository, desired.tag)`. Callers that need a registry-qualified reference (for example the push handler's writeback) compose the registry host themselves; the persisted ref is project-scoped and registry-agnostic.
3. THE `source: ImageSource` field SHALL be populated from `image.source_type()` and `desired.upstream_ref`:
   - `ImageSourceType::Build` ⇒ `ImageSource::Built`
   - `ImageSourceType::Mirror` ⇒ `ImageSource::Mirrored { upstream_ref }` where `upstream_ref` is obtained from `desired.upstream_ref` via a fallible `ok_or_else` that returns `RuntimeError::Image(format!("image '{}' is Mirror but desired_ref.upstream_ref is None", image.name()))`. The implementation MUST NOT use `.expect()` or `.unwrap()` — Property 2 prevents this in practice, but the error path must remain operator-facing per the workspace's no-`unwrap`-outside-tests rule.
   - `ImageSourceType::Registry` ⇒ `ImageSource::PullThrough { upstream_ref: desired.upstream_ref.unwrap_or_default() }` — the Registry contract (Req 1.6) permits `None`, so this is the single variant where `unwrap_or_default()` is acceptable because the field is informational only.
4. NO fields SHALL be removed from `ImageState` or `ImageSource`. All additions are strictly additive per the existing state-format compatibility rule.
5. A unit test in `tokeira-deploy-engine` SHALL assert that `record_images` produces the expected `ImageState` for each of the three `ImageSourceType` variants against a small fixture of fake `Image` impls. A negative test SHALL construct a fake Mirror image that returns `upstream_ref: None` and assert `record_images` returns the exact documented `RuntimeError::Image` rather than panicking.

### Requirement 1.6: `Registry` source-type semantics

**User Story:** As a Tokeira maintainer, I want `ImageSourceType::Registry` defined rather than reserved, so that an image that is pulled from an existing registry (neither built locally nor mirrored into project ECR) has an unambiguous contract — even if no image in the current spec uses this variant.

#### Acceptance Criteria

1. `ImageSourceType::Registry` represents an image whose reference is consumed as-is from an external registry (for example a public ECR image referenced directly by a compose service without mirroring). This variant is not exercised by any image declared in this spec; it is reserved for future use.
2. FOR Registry images, `DesiredImageRef.repository` SHALL carry the full registry-qualified reference path (not a project-scoped suffix). The project-prefix requirement in Req 1.1.4 applies to Build and Mirror images only.
3. FOR Registry images, `DesiredImageRef.upstream_ref` MAY be `None` or `Some(_)`. When `Some`, it SHALL equal `repository:tag` — the field is informational only for this variant.
4. FOR Registry images, `writeback_targets` SHALL default to empty. A Registry image's ref is authored by the operator in config and never discovered by `tkr image` machinery.
5. THE per-platform property tests (Req 9.1, 9.2, 9.7, 9.8) SHALL either skip Registry images or assert only the Registry-specific invariants above; they SHALL NOT assert the Build-or-Mirror upstream-ref invariant on Registry images.

---

## Feature 2: Per-Platform Image Declarations

### Requirement 2.1: Compose platform image modules

**User Story:** As a Tokeira developer, I want the compose platform to own its image set so that compose's image needs and config are expressed directly without cross-platform coupling.

#### Acceptance Criteria

1. THE compose platform SHALL include image modules at `platforms/compose/src/images/`:
   - `tokeirad.rs` — `TokeiradImage` struct implementing `Image` with `source_type = Build`, reading `ComposeConfig` via `ctx.extension::<ComposeConfig>()`
   - `observability/mod.rs` — six structs (`MimirImage`, `LokiImage`, `GrafanaImage`, `AlloyImage`, `AwsCliImage`, `BusyBoxImage`), all `source_type = Mirror`, all reading their upstream refs from `ComposeConfig.observability`
   - `mod.rs` — exposing BOTH `pub fn construct() -> Vec<Box<dyn Image>>` (context-free enumeration — concatenates `tokeirad::all() + observability::all()`; used by `ComposeDeployment::images(&self, config)`) AND `pub fn all(ctx: &ImageContext) -> Result<Vec<Box<dyn Image>>, RuntimeError>` (calls `construct()` then `validate_registry`; used by CLI handlers that already have an `ImageContext`). Per Req 2.3.4
2. `TokeiradImage::desired_ref` on the compose platform SHALL return `DesiredImageRef { repository: format!("{project}/tokeirad"), tag: "latest", upstream_ref: None }` where `project` comes from `ComposeConfig.project_name`.
3. `TokeiradImage::writeback_targets` on the compose platform SHALL return `vec![WritebackTarget { field: "tokeirad.image" }]` — the single field the compose config reads for the `tokeirad` service image. The `field` SHALL be populated with the `latest` tag, not a version tag.
4. EACH observability image's `desired_ref` SHALL return `DesiredImageRef { repository: format!("{project}/{repo_suffix}"), tag: image_tag(upstream).unwrap_or("latest"), upstream_ref: Some(upstream.clone()) }` where `upstream` is read from the matching `ComposeConfig.observability.*_image` field.
5. EACH observability image's `writeback_targets` SHALL return `vec![WritebackTarget { field: "observability.{name}_image" }]` matching the upstream source field.

### Requirement 2.2: ECS platform image modules

**User Story:** As a Tokeira developer, I want the ECS platform to own its image set so that ECS's image needs and config are expressed directly without cross-platform coupling.

#### Acceptance Criteria

1. THE ECS platform SHALL include image modules at `platforms/ecs/src/images/`:
   - `tokeirad.rs` — `TokeiradImage` struct implementing `Image` with `source_type = Build`, reading `EcsConfig` via `ctx.extension::<EcsConfig>()`
   - `observability/mod.rs` — six structs matching compose's set (`MimirImage`, `LokiImage`, `GrafanaImage`, `AlloyImage`, `AwsCliImage`, `BusyBoxImage`), all `source_type = Mirror`, all reading their upstream refs from `EcsConfig.observability`
   - `mod.rs` — exposing BOTH `pub fn construct() -> Vec<Box<dyn Image>>` (context-free, used by `EcsDeployment::images(&self, config)`) AND `pub fn all(ctx: &ImageContext) -> Result<Vec<Box<dyn Image>>, RuntimeError>` (validated, used by CLI handlers and `ImagesModule`). Per Req 2.3.4
2. `TokeiradImage::desired_ref` on the ECS platform SHALL return `DesiredImageRef { repository: format!("{project}/tokeirad"), tag: "latest", upstream_ref: None }` where `project` comes from `EcsConfig.project_name`.
3. `TokeiradImage::writeback_targets` on the ECS platform SHALL return a `Vec<WritebackTarget>` with one entry per ECS service: `services.edge_api.image`, `services.edge_poll.image`, `services.runtime.image`, `services.projection.image`, `services.controller.image`, `services.autoscaler.image`, `services.admin.image`. The remote ref written back into each service field is determined by `tkr image push` at publish time (Req 6.4.6): when a non-`latest` tag is supplied via `--tag`, the version-tagged ref `{registry}/{project}/tokeirad:{tag}` is written; when the default `--tag latest` is in effect, the single `:latest` ref `{registry}/{project}/tokeirad:latest` is written. The image's `writeback_targets` method itself only declares which dotted keys are targets — the ref value is chosen by the CLI handler.
4. EACH observability image's `desired_ref` SHALL produce `DesiredImageRef { repository: format!("{project}/{repo_suffix}"), tag: image_tag(upstream).unwrap_or("latest"), upstream_ref: Some(upstream.clone()) }` where `upstream` is read from the matching `EcsConfig.observability.*_image` field.
5. EACH observability image's `writeback_targets` SHALL return `vec![WritebackTarget { field: "observability.{name}_image" }]` matching the upstream source field.

### Requirement 2.3: Per-platform registry validation

**User Story:** As a Tokeira maintainer, I want each platform's `images::all()` to refuse duplicate names or duplicate repositories at construction time, so that a malformed image module cannot ship.

#### Acceptance Criteria

1. EACH platform's `images::all()` SHALL call a shared `validate_registry(images: &[Box<dyn Image>], ctx: &ImageContext) -> Result<(), RuntimeError>` helper (living in `tokeira-deploy-engine::image::validate_registry`) before returning.
2. `validate_registry` SHALL walk the list and return an error on duplicate `name()` values or duplicate `repository` values (after resolving `desired_ref(ctx)`), regardless of `source_type`. A `Build` image and a `Mirror` image that resolve to the same `repository` SHALL be rejected — the downstream `ImagesModule` would otherwise register duplicate `EcrRepository` resources for the same AWS repository.
3. Duplicate violations SHALL return `RuntimeError::Image(format!("image registry validation failed: duplicate {kind} = {value}"))`. Callers SHALL NOT be expected to handle this — a failing `all()` is a programming error, not a runtime condition.
4. EACH platform SHALL split image construction from image validation:
   - `images::construct() -> Vec<Box<dyn Image>>` — synchronous, context-free, always returns the full image list. Used by `Deployment::images(&self, config)` which receives no context.
   - `images::all(ctx) -> Result<Vec<Box<dyn Image>>, RuntimeError>` — calls `construct()` then `validate_registry`. Used by CLI handlers that already have an `ImageContext`.
   The two functions return the same set of images in the same order. `validate_registry` is run at test time via a per-platform property test (Req 9.1), not on every `deploy apply`.

### Requirement 2.4: ECR name grammar enforcement

**User Story:** As a Tokeira maintainer, I want every image's resolved repository name validated against the ECR grammar, so that an image that accidentally declares an invalid repo name cannot ship.

#### Acceptance Criteria

1. A per-platform property test SHALL iterate its own `images::all(ctx)` across realistic `ctx` values and assert that `desired_ref(ctx)?.repository` satisfies the ECR repository name grammar: 2–256 characters, lowercase alphanumerics plus `/`, `-`, `_`, `.`, not starting with `/` or `.`.

### Requirement 2.5: Platform `images()` returns the new registry

**User Story:** As a Tokeira maintainer, I want `ComposeDeployment::images()` and `EcsDeployment::images()` to return the new platform image registry, so that the deploy-engine actually records the Build and Mirror images it provisions rather than the legacy per-service Registry wrappers.

#### Acceptance Criteria

1. THE existing `ComposeImage` struct in `platforms/compose/src/services.rs` and its surrounding `ComposeDeployment::images()` body SHALL be removed as part of this spec's implementation. The adapter becomes obsolete once the platform registry exists.
2. `ComposeDeployment::images(&self, config)` SHALL return `platforms::compose::images::construct()` — the context-free constructor (Req 2.3.4). The deploy engine runs `register_image_extensions` before `record_images`, so `desired_ref(ctx)` sees `ComposeConfig` registered at call time.
3. `EcsDeployment::images(&self, config)` SHALL return `platforms::ecs::images::construct()` using the same pattern.
4. `LocalDeployment::images()` remains an empty `Vec` (local has no image set).
5. AFTER this change, running `tkr deploy apply` on a compose deployment SHALL result in `ServiceEngine::record_images` persisting one `ImageState` per image in the new platform registry — `tokeirad` as `Built` and each observability image as `Mirrored { upstream_ref }`. No `ImageSource::PullThrough` entries SHALL be written unless a future `RegistryImage` is added to a platform's `images::construct()` (Req 1.6).

---

## Feature 3: Dagger-Backed Build, Publish, and Mirror Pipelines

### Requirement 3.1: Build crate structure

**User Story:** As a Tokeira developer, I want a dedicated library crate for image build/publish/mirror workflows, so that the pipeline orchestration is isolated from both the CLI and the deployment-resolution abstractions.

#### Acceptance Criteria

1. THE Build_Crate SHALL live at `crates/tokeira-build/` and SHALL be a workspace member.
2. THE Build_Crate SHALL expose public API: `TokeiradBuildRequest`, `TokeiradBuildResult`, `PublishRequest`, `PublishResult`, `PublishedReference`, `MirrorRequest`, `MirroredReference`, `BuildError`, `Arch`, and the `DaggerClient` trait.
3. THE Build_Crate SHALL expose public functions: `build_tokeirad_image(&TokeiradBuildRequest, &dyn DaggerClient) -> Result<TokeiradBuildResult, BuildError>`, `publish_image(&PublishRequest, &dyn DaggerClient) -> Result<PublishResult, BuildError>`, `mirror_image(&MirrorRequest, &dyn DaggerClient) -> Result<MirroredReference, BuildError>`.
4. THE Build_Crate SHALL use `thiserror` for its error type and SHALL NOT expose `anyhow::Error` in its public API.
5. THE Build_Crate SHALL use `tracing` for structured logging. THE Build_Crate SHALL NOT use `println!` or `eprintln!` in library code.
6. THE Build_Crate SHALL NOT depend on `apps/tkr` or on any platform crate.
7. THE Build_Crate SHALL NOT implement the `Image` trait and SHALL NOT enumerate platforms' image registries. Each pipeline function hardcodes the image recipe it produces.

### Requirement 3.2: Reproducible tokeirad image build

**User Story:** As a Tokeira operator, I want `tokeirad` image builds to be reproducible across hosts and invocations, so that the same source tree produces functionally equivalent images every time.

#### Acceptance Criteria

1. `build_tokeirad_image` SHALL drive the build through a Dagger pipeline rather than invoking `docker build` directly.
2. THE Dagger pipeline SHALL resolve the Rust toolchain version from `rust-toolchain.toml` at the workspace root and SHALL pin the build container's Rust version to that value.
3. THE Dagger pipeline SHALL build the `tokeirad` binary with `cargo build --release --bin tokeirad --target <target-triple>` using the Target_Architecture supplied in the build request.
4. THE resulting container image SHALL contain exactly one application binary (`/usr/local/bin/tokeirad`) and the minimal runtime dependencies required to execute it (CA certificates, timezone data).
5. THE resulting container image SHALL run as a non-root user (UID/GID 1000) with `tokeirad` as both the username and group name.
6. THE resulting container image SHALL declare `ENTRYPOINT ["/usr/local/bin/tokeirad"]` and SHALL leave CMD empty by default.
7. FOR ALL invocations of `build_tokeirad_image` on the same source tree, same `rust-toolchain.toml`, and same Target_Architecture, the produced application binary layer SHALL be bit-identical.
8. EVERY successful build SHALL export `tokeirad:latest` regardless of the value of `--tag`. When `--tag <value>` is supplied (and `<value> != "latest"`), the pipeline SHALL additionally export `tokeirad:<value>`, pointing at the same digest.

### Requirement 3.3: Target architecture support

**User Story:** As a Tokeira operator, I want to build `arm64` images by default with an opt-in for `amd64`, so that the same workflow serves Graviton4 ECS hosts, Apple Silicon compose users, and x86 Intel hosts.

#### Acceptance Criteria

1. THE `TokeiradBuildRequest` SHALL include `arch: Arch` where `pub enum Arch { Arm64, Amd64 }`.
2. `Arch` SHALL implement `FromStr` with an error type of `BuildError::UnsupportedArch { supplied: String }`. Valid strings SHALL be `"arm64"` and `"amd64"`; any other string returns the error.
3. WHEN `arch = Arm64`, THE Dagger pipeline SHALL use target triple `aarch64-unknown-linux-musl`. WHEN `arch = Amd64`, the pipeline SHALL use `x86_64-unknown-linux-musl`.
4. THE produced image's manifest SHALL declare the platform (`linux/arm64` or `linux/amd64`) matching the Target_Architecture.

### Requirement 3.4: Publish operation

**User Story:** As a Tokeira operator, I want to publish a locally-built image to multiple remote refs in a single authenticated Dagger session, so that `:latest` and `:{tag}` get the same digest.

#### Acceptance Criteria

1. `publish_image(request, dagger)` SHALL authenticate to `request.registry` using `request.username` and `request.password` and SHALL push `request.local_image` to every entry in `request.remote_refs`.
2. WHEN `request.remote_refs` is empty, THE function SHALL return `BuildError::Validation { reason: "remote_refs cannot be empty" }`.
3. THE `PublishResult` SHALL contain one `PublishedReference { remote_ref, published_ref }` entry per successfully pushed reference; `published_ref` carries the digest-pinned reference Dagger returns.
4. IF any push fails, the function SHALL return `BuildError::Publish { remote_ref, source }` naming the failing ref. Prior successful pushes SHALL NOT be undone.

### Requirement 3.5: Mirror operation

**User Story:** As a Tokeira operator, I want to mirror a remote image from an upstream registry to a destination ECR reference, so that a private-only ECS deployment has every image it needs before `tkr infra apply`.

#### Acceptance Criteria

1. `mirror_image(request, dagger)` SHALL authenticate to `request.registry` using `request.username` and `request.password` and SHALL pull `request.source_ref` then push to `request.remote_ref`.
2. THE function SHALL NOT require a local `docker pull` step — the Dagger pipeline handles source-to-destination transfer in a single session.
3. FOR ALL invocations with identical `source_ref` and `remote_ref`, calling `mirror_image` twice SHALL produce the same destination image (digest-level idempotence).
4. IF the upstream source returns an authentication error, THE function SHALL return `BuildError::UpstreamAuth`. The `RegistryCredentials` in the request are only for the destination.

---

## Feature 4: Dagger Client Dependency

### Requirement 4.1: Dagger session bootstrap

**User Story:** As a Tokeira developer, I want the build crate to obtain a Dagger session without requiring operators to manage session lifetime manually, so that one `tkr image <subcommand>` invocation is self-contained.

#### Acceptance Criteria

1. WHEN the Image_CLI is invoked without active Dagger session environment variables (`DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` both unset), THE Image_CLI SHALL re-execute itself under `dagger run` with the same arguments and exit with that process's status.
2. WHEN both `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` are set, THE Image_CLI SHALL NOT re-execute under `dagger run` and SHALL proceed with the existing session.
3. IF `dagger` is not on the operator's PATH, THE Image_CLI SHALL return an error stating that `dagger` CLI (>= 0.20) must be installed, with a link to the Dagger installation documentation.
4. THE re-exec flow SHALL forward the `--deployment`, `--json`, and all `image` subcommand arguments unchanged.

### Requirement 4.2: Dagger client location

**User Story:** As a Tokeira developer, I want the Dagger client dependency owned in the Tokeira workspace, so that the build crate has a small, auditable GraphQL surface rather than pulling a heavyweight SDK.

#### Acceptance Criteria

1. THE Build_Crate SHALL depend on an in-repo `crates/dagger-client/` crate. THE reference implementation at `.kiro/specs/image-lifecycle/reference/` SHALL be ported into the workspace with minor adjustments documented in the reference README.
2. THE Dagger client interface consumed by the Build_Crate SHALL include at minimum: `host_directory(path)`, `container_from(image)`, `container_build(context, dockerfile)`, `with_exec(args)`, `with_file(path, file)`, `with_entrypoint(args)`, `export_image(tag)`, `publish(remote_ref)`, `with_registry_auth(registry, username, secret)`, `set_secret(name, value)`.
3. THE Build_Crate SHALL expose a `DaggerClient` trait over the in-repo client as the public testing seam. Pipeline functions take `&dyn DaggerClient`. The trait is first-class public API so tests can substitute a mock; production callers obtain the default via `Client::from_env()` on the `dagger-client` crate.

---

## Feature 5: ECR Repository Provisioning

### Requirement 5.1: `EcrRepository` IaC resource

**User Story:** As a Tokeira operator, I want ECR repositories to be provisioned as IaC resources alongside the rest of the deployment's AWS infrastructure, so that repositories are tracked in state, diffed on plan, and cleaned up on destroy according to the same lifecycle rules as every other resource.

#### Acceptance Criteria

1. THE `tokeira-aws` crate SHALL define an `EcrRepository` resource implementing the `Resource` trait from `tokeira-iac`. Trait method signatures SHALL match the existing trait verbatim: `async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError>` and siblings. NO additional parameters.
2. THE `EcrRepository` SHALL fetch its ECR client via a `ProvisionContext` extension named `EcrClientHandle(Arc<dyn EcrClient>)`. The lookup SHALL be fallible (Req 1.3); missing extension returns `IacError::Other(anyhow!("ProvisionContext missing extension: EcrClientHandle"))`.
3. THE `EcrRepository` resource SHALL accept `name: String` and `module: String` fields at construction. It SHALL NOT accept `tags` at construction — tags are computed at lifecycle time (see Req 5.1.9). The `name` SHALL be validated against the ECR grammar (Req 2.4) at construction time.
4. THE `EcrRepository` SHALL set Image_Tag_Mutability to `MUTABLE` on create.
5. THE `EcrRepository` SHALL apply the canonical Lifecycle_Policy (keep last 10 untagged images) on both create and update.
6. THE `EcrRepository::describe()` method SHALL return `None` when the repository does not exist in AWS. When the repository exists, `describe()` SHALL read LIVE state from AWS — calling `list_tags_for_resource(arn)` for tags and `get_lifecycle_policy(name)` for the policy — and populate `ResourceState.properties` with those live values. It SHALL NOT call `ctx.resource_tags()` on the describe path; that would echo desired tags into state and hide external drift.
7. THE `EcrRepository::diff()` method SHALL compare the LIVE values persisted into `current.properties` by `describe()` against the desired values. Desired tags come from `ctx.resource_tags(&self.name)`; desired policy is the canonical `ECR_LIFECYCLE_POLICY` constant. Lifecycle-policy comparison SHALL be JSON-normalised (key ordering, rule ordering, whitespace all irrelevant). Any mismatch — including drift introduced outside the tool, such as an operator retagging or retrofitting a policy in the AWS console — SHALL signal `Update`.
8. THE `EcrRepository` SHALL carry the same auto-generated and operator-defined tags as all other AWS resources per the [`ecs-deployment`](../ecs-deployment/requirements.md) tagging requirement. This includes a resource-specific `Name` tag equal to the repository name (for example `Name = "tokeira-dev/tokeirad"`), a `Project` tag equal to the project name, and a `ManagedBy = "tokeira-cli"` tag, merged with any operator-defined tags.
9. THE desired tag set SHALL be computed AT LIFECYCLE TIME via `ctx.resource_tags(&self.name)` inside `create`/`update`/`diff` — NOT hoisted to a module-level constant or passed through the resource constructor. The `describe` method SHALL NOT use this helper; it reads LIVE tags from AWS (Req 5.1.6). The existing `tokeira_iac::ProvisionContext::resource_tags(resource_name)` helper already returns the correct per-resource merge (operator tags + `Name = resource_name` + `Project` + `ManagedBy`) as a `HashMap<String, String>`. The resource's constructor SHALL NOT accept a tags parameter, and `ImagesModule` SHALL NOT compute tags itself — letting each `EcrRepository` resolve its own `Name`-including tag map at apply time.

### Requirement 5.2: ECR repositories are wired into an IaC module

**User Story:** As a Tokeira operator, I want every project-owned ECR repository to appear in `tkr infra plan` output and to be cleaned up on `tkr infra destroy`, so that repositories are first-class members of the deployment's state.

#### Acceptance Criteria

1. THE ECS platform SHALL include an `ImagesModule` IaC module at `platforms/ecs/src/modules/images.rs` that declares one `EcrRepository` resource per entry in `platforms::ecs::images::all(ctx)`, using `desired_ref(ctx)?.repository` as the repository name.
2. `ImagesModule` SHALL follow the in-constructor capture pattern already used by the compose platform's `ComposeModule::runtime(config)`: the module's constructor takes `EcsConfig` by value and stores it as a field. It SHALL NOT capture or pass tags — tags are computed per-resource at lifecycle time by each `EcrRepository::create`/`update` via `ctx.resource_tags(&self.name)` (Req 5.1.9). It SHALL NOT attempt to read `EcsConfig` from `ModuleContext` extensions, because `ModuleContext` in this workspace exposes only `state` and typed extensions — not arbitrary config, not project tags, and not a `default_tags()` helper.
3. THE `resources(ctx)` body SHALL construct an `ImageContext`, register the captured `EcsConfig` on it via `ctx.set_extension(config.clone())`, call `platforms::ecs::images::all(&image_ctx)?`, and map each resolved image to an `EcrRepository::new(desired.repository, "images".into())?` — with no tags parameter.
4. THE module's resource list SHALL be computed by iterating the ECS platform's image set so adding a new image automatically adds its repository to `infra plan` / `infra destroy`.
5. `tkr infra plan` on an ECS deployment SHALL list every project-owned ECR repository under the `images` module. Each repository's per-resource `Name` tag SHALL equal its repository name (for example `Name = "tokeira-dev/tokeirad"`), confirming that `resource_tags(&self.name)` is computed per-repository and not reused across the module.
6. `tkr infra destroy` on an ECS deployment SHALL delete every project-owned ECR repository (with `force = true`), subject to the describe-before-delete idempotency rule.
7. THE module is ECS-specific. Local and compose platforms SHALL NOT register it.
8. THE tags applied to each ECR repository SHALL match the `resource_tags(&name)` computation in `tokeira_iac::ProvisionContext`. The `ecs-deployment` spec defines the project-level tag set (auto-generated `Name`/`Project`/`ManagedBy` plus operator-defined tags) via the `ProvisionContext::tags` map that `ProvisionContext::new` receives. This spec consumes that map through the existing `resource_tags(name)` helper — it does NOT define or compute tags independently.

### Requirement 5.3: Ad-hoc repository ensure for pre-apply image flows

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to work on a fresh deployment before `tkr infra apply` has run, so that the image plane can prepare ECR independently of the rest of the infrastructure.

#### Acceptance Criteria

1. THE `tokeira-aws` crate SHALL expose `async fn ensure_ecr_repository(ecr: &dyn EcrClient, name: &str, tags: &HashMap<String, String>) -> Result<(), EcrError>` that:
   a. Describes the repository.
   b. If absent, creates it with `MUTABLE` mutability and the supplied `tags`.
   c. Regardless of whether the repository was newly created or already existed, unconditionally re-applies the Lifecycle_Policy via `put_lifecycle_policy`.
   d. Regardless of existence, unconditionally re-applies the supplied `tags` via `tag_resource`. This is the reconciliation step that lets `EcrRepository::diff` report `NoChange` on a subsequent `tkr infra apply`.
2. THE crate SHALL expose `async fn ensure_ecr_repositories(ecr: &dyn EcrClient, repos: &[(String, HashMap<String, String>)]) -> Result<(), EcrError>` that calls the single-repo helper in sequence.
3. THE caller SHALL supply tags that exactly match what `EcrRepository::create`/`update` would apply for the same repository. The canonical way to produce these tags at the CLI call site is: build a `ProvisionContext` via the ECS platform's `register_infra_extensions` hook (same code path `tkr infra apply` uses), then call `ctx.resource_tags(&name)` once per repository. Reusing the project-level `ctx.tags` map WITHOUT calling `resource_tags` would omit the per-resource `Name` tag and cause subsequent `infra apply` to report spurious `Update` changes.
4. THE ad-hoc helpers SHALL produce the same end state as the `EcrRepository` IaC resource's `create` + `update` path: same mutability, same lifecycle policy JSON, same tags including the correct per-repository `Name`. A repository created ad-hoc SHALL be adopted cleanly by a subsequent `tkr infra apply` without additional changes, AND a repository created with stale tags SHALL be reconciled on the next `ensure_ecr_repository` call.
5. A unit test SHALL construct a repository via the ad-hoc helper using `ctx.resource_tags(&name)` as the tag source, then invoke `EcrRepository::describe(&ctx)` and `diff()` against the same mocked ECR state and assert `Change::NoChange`. This is the adoption-consistency test.
6. A unit test SHALL seed the mock with a repository carrying stale tags, call `ensure_ecr_repository` with `ctx.resource_tags(&name)`, and assert `tag_resource` was invoked with the current tags and that `EcrRepository::diff` subsequently reports `NoChange`.

### Requirement 5.3a: Shared `ensure_ecr_repositories_from_images` helper

**User Story:** As a Tokeira maintainer, I want a single helper that `tkr image push` and `tkr image mirror` both call, so that the tag-source pattern stays consistent between the two subcommands and with `ImagesModule`.

#### Acceptance Criteria

1. THE `platforms/ecs` crate SHALL expose `pub async fn ensure_ecr_repositories_from_images(ecr: &dyn EcrClient, ctx: &ProvisionContext, images: &[Box<dyn Image>], image_ctx: &ImageContext) -> Result<(), IacError>` that:
   a. Iterates the supplied image list.
   b. For each image, resolves `desired_ref(image_ctx)?.repository`.
   c. Computes `ctx.resource_tags(&desired.repository)` — the SAME helper `EcrRepository::create` uses.
   d. Collects `(repository, tags)` pairs.
   e. Calls `tokeira_aws::ensure_ecr_repositories(ecr, &repos)` once.
2. `tkr image push` SHALL call this helper (filtered to Build images) rather than computing tags inline.
3. `tkr image mirror` SHALL call this helper (filtered to Mirror images) rather than computing tags inline.
4. A property test SHALL assert that for every image in `platforms::ecs::images::all(ctx)`, the tags produced by `ensure_ecr_repositories_from_images` for that image equal the tags that a fresh `EcrRepository { name: desired.repository, module: "images" }` would apply during `create` — ensuring the two paths converge on the same end state.

### Requirement 5.4: Canonical Lifecycle_Policy

**User Story:** As a Tokeira operator, I want a standard lifecycle policy applied to every project-owned ECR repository, so that storage costs are bounded without manually setting policies per repository.

#### Acceptance Criteria

1. THE Lifecycle_Policy SHALL be a single rule with `rulePriority = 1`, `tagStatus = "untagged"`, `countType = "imageCountMoreThan"`, `countNumber = 10`, `action.type = "expire"`.
2. THE Lifecycle_Policy SHALL NOT expire tagged images.
3. FOR ALL repositories provisioned by this spec, applying the policy twice SHALL be idempotent.

### Requirement 5.5: Repository existence handling in the CLI

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to create repositories on first use and tolerate pre-existing repositories on re-runs, so that the commands are idempotent.

#### Acceptance Criteria

1. WHEN `tkr image push` or `tkr image mirror` runs, THE Image_CLI SHALL ensure each required Project_Repository exists before attempting to push, via the Req 5.3 ad-hoc helpers.
2. WHEN the repository already exists, THE Image_CLI SHALL proceed to apply the Lifecycle_Policy (idempotent) and then to push.
3. WHEN the repository does not exist, THE Image_CLI SHALL create it with Image_Tag_Mutability = `MUTABLE` and THEN apply the Lifecycle_Policy.
4. FOR ALL invocations with the same project name and image set, calling `tkr image push` or `tkr image mirror` twice in a row SHALL produce the same set of repositories with the same lifecycle policy.

---

## Feature 6: Image CLI

### Requirement 6.1: `tkr image` command group

**User Story:** As a Tokeira operator, I want a single `tkr image` command group for all image-plane operations, so that list, build, push, and mirror workflows are discoverable together.

#### Acceptance Criteria

1. THE Image_CLI SHALL expose a top-level `image` subcommand under `tkr` with four children: `list`, `build`, `push`, and `mirror`.
2. THE `tkr image` command group SHALL follow the [`tkr-cli`](../tkr-cli/requirements.md) conventions for global flags (`--deployment`, `--json`) and for help-text formatting.
3. THE `tkr image` command group SHALL appear between `tkr deployment` and `tkr infra` in `tkr --help` output.
4. WHEN `tkr image` is invoked with no subcommand, THE Image_CLI SHALL print a help message listing the four children.

### Requirement 6.2: `list` subcommand

**User Story:** As a Tokeira operator, I want `tkr image list` to enumerate every image the active deployment's platform knows about, so that I can see what would be built and what would be mirrored.

#### Acceptance Criteria

1. `tkr image list` SHALL dispatch on the active deployment's platform and call the context-bearing, validating registry accessor:
   - Compose: `platforms::compose::images::all(&ctx)`
   - ECS: `platforms::ecs::images::all(&ctx)`
   The `ImageContext` SHALL be populated through `Deployment::register_image_extensions` per Req 1.4.6 so image resolution can read each platform's config. Calling `images::construct()` directly from this subcommand is incorrect — `list` needs the resolved `DesiredImageRef` to render the `REPOSITORY`, `TAG`, and `UPSTREAM` columns, and skipping validation would let a malformed registry silently pass through.
2. THE command SHALL print one row per image with columns: `NAME`, `SOURCE`, `REPOSITORY`, `TAG`, `UPSTREAM` (empty for Build images).
3. `tkr image list --source-type build` SHALL filter to Build images. `--source-type mirror` SHALL filter to Mirror images.
4. `tkr image list --json` SHALL emit an array of `{ name, source_type, repository, tag, upstream_ref }` objects.
5. THE subcommand SHALL require an active deployment (`--deployment`) because image resolution reads the platform config via `ImageContext`.

### Requirement 6.3: `build` subcommand

**User Story:** As a Tokeira developer, I want `tkr image build` to build `tokeirad` with sensible defaults, so that the compose platform works out of the box after a single command.

#### Acceptance Criteria

1. THE `tkr image build` subcommand SHALL accept the following optional flags: `--arch <arm64|amd64>` (default `arm64`), `--tag <value>` (optional; when supplied, an additional tag is exported alongside `latest` — see Req 3.2.8).
2. THE subcommand SHALL call `tokeira_build::build_tokeirad_image` directly. No `Image` trait iteration; no platform dispatch; no `ImageContext`.
3. THE subcommand SHALL NOT require an active deployment (`--deployment`). The build only uses workspace sources and produces local images.
4. THE subcommand SHALL emit progress events via the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) progress callback surface for each build stage.
5. WHEN the operator passes `--json`, THE subcommand SHALL emit JSON progress events plus a final `{ "action": "build", "image": "tokeirad", "tags": ["latest", "<tag>"], "arch": "<arch>" }` summary.
6. THE subcommand SHALL NOT prompt for confirmation (build produces only local artifacts).

### Requirement 6.4: `push` subcommand

**User Story:** As a Tokeira operator, I want `tkr image push` to authenticate with ECR, ensure repositories exist, push `tokeirad` with both `latest` and a version tag, and write back the resulting remote refs, so that the next `tkr infra apply` and `tkr deploy apply` can consume them.

#### Acceptance Criteria

1. THE `tkr image push` subcommand SHALL accept `--tag <value>` (defaults to `latest` when omitted).
2. THE subcommand SHALL require an active deployment and dispatch on its platform (ECS-only for the initial spec scope; compose and local reject push).
3. THE subcommand SHALL verify that `tokeirad:latest` is present in the local Docker image store BEFORE any Dagger startup, ECR authentication, repository-ensure call, or other AWS mutation. On absence, the subcommand SHALL fail with a descriptive error instructing the operator to run `tkr image build` first. The intent is cheap-error ordering: an operator who forgot the build step pays no Dagger launch cost, no ECR `GetAuthorizationToken` round-trip, and writes nothing to AWS before seeing the actionable message.
4. AFTER the local preflight in 6.4.3 passes, THE subcommand SHALL call ECR `GetAuthorizationToken` once per run, decode the token, and use the decoded credentials for both repository-ensure and image-publish steps.
5. FOR EACH Build image in the platform's image set, THE subcommand SHALL compute the list of remote references to publish as follows:
   - Always include `{registry}/{desired.repository}:latest`.
   - When `--tag <value>` was supplied AND `<value> != "latest"`, additionally include `{registry}/{desired.repository}:{value}`.
   - When `<value> == "latest"` (the default), the version-tagged ref is IDENTICAL to the `:latest` ref. In this case the list SHALL contain exactly ONE entry — no deduplicated-against-itself double push. This mirrors the build pipeline's tag-dedup rule in Req 3.2.8.
6. THE subcommand SHALL perform Image_Writeback: for each Build image, for each `WritebackTarget` in `image.writeback_targets(ctx)`, write the remote ref into the target's dotted key via `tokeira_iac::write_config_values` (Req 7.3). When `--tag <value>` with `<value> != "latest"` was supplied, the ref written back is `{registry}/{desired.repository}:{value}` (the version-tagged ref). When `<value> == "latest"` (the default), the ref written back is `{registry}/{desired.repository}:latest` (the sole ref that was published).
7. WHEN the operator passes `--json`, THE subcommand SHALL emit JSON progress events plus a final summary event. The `published` array SHALL contain one entry per distinct ref actually pushed — never two entries for the same ref.
8. WHEN `--image <name>` is supplied and no Build image in the platform's image set has a `name()` matching `<name>`, THE subcommand SHALL return an operator-facing error listing the valid Build image names: `unknown Build image '<name>'; valid Build images are: <sorted comma-separated list>`. This validation SHALL run AFTER the image-set enumeration in 6.4 but BEFORE the local preflight in 6.4.3 — a typo like `--image tokierad` must not silently match nothing and appear successful.

### Requirement 6.5: `mirror` subcommand

**User Story:** As a Tokeira operator, I want `tkr image mirror` to copy every Mirror image into project-owned ECR repositories and write the mirrored refs back, so that a private-only ECS deployment has every image it needs.

#### Acceptance Criteria

1. THE `tkr image mirror` subcommand SHALL accept no positional arguments and MAY accept `--image <name>` (default: every Mirror image).
2. THE subcommand SHALL require an active deployment and dispatch on its platform (ECS-only for the initial spec scope; compose and local reject mirror).
3. THE subcommand SHALL iterate every Mirror image in the platform's image set (filtered by `--image` if supplied) and for each:
   - ensure the destination Project_Repository exists via the Req 5.3 ad-hoc helpers
   - invoke `tokeira_build::mirror_image` with the source ref and the destination remote ref
   - collect the result into a writeback list driven by `image.writeback_targets(ctx)`
4. THE subcommand SHALL perform Image_Writeback via `tokeira_iac::write_config_values`.
5. THE subcommand SHALL be idempotent. FOR ALL invocations with the same deployment, calling `tkr image mirror` twice in a row SHALL succeed both times and SHALL leave the same set of config keys populated with the same values after the second invocation.
6. WHEN a source field in `deployment.toml` already points to the corresponding project-scoped destination, THE subcommand SHALL treat that image as already-mirrored and SHALL skip the pull/push. The skip SHALL be reported in the summary.
7. WHEN the operator passes `--json`, THE subcommand SHALL emit JSON progress events plus a final summary event.
8. WHEN `--image <name>` is supplied and no Mirror image in the platform's image set has a `name()` matching `<name>`, THE subcommand SHALL return an operator-facing error listing the valid Mirror image names: `unknown Mirror image '<name>'; valid Mirror images are: <sorted comma-separated list>`. This validation SHALL run AFTER the image-set enumeration but BEFORE any repository-ensure or Dagger work — a typo must not silently match nothing and appear successful.

### Requirement 6.6: Confirmation prompts

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to respect the same confirmation rules as other mutating commands, so that I cannot accidentally overwrite remote state.

#### Acceptance Criteria

1. THE `tkr image push` and `tkr image mirror` subcommands SHALL follow the [`tkr-cli`](../tkr-cli/requirements.md) confirmation rules: interactive confirmation by default, `--yes` to bypass, refuse to proceed when stdout is non-TTY and `--yes` is not provided.
2. THE `tkr image list` and `tkr image build` subcommands SHALL NOT require confirmation: `list` is read-only and `build` produces only local artifacts.

---

## Feature 7: Config Integration

### Requirement 7.1: Compose platform config alignment

**User Story:** As a Tokeira operator, I want the compose platform's `tokeirad.image` default to remain `tokeirad:latest` and to be produced by `tkr image build`, so that no manual intervention is needed to bring up a compose deployment.

#### Acceptance Criteria

1. THE `ComposeConfig::default()` value for `tokeirad.image` SHALL be `"tokeirad:latest"` (flipped from the current `"tokeirad:local"`).
2. THE `tkr image build` subcommand with default flags SHALL produce `tokeirad:latest`.
3. THE compose platform SHALL NOT invoke `tkr image build` automatically on `tkr deploy apply`.
4. IF `tkr deploy apply` is invoked on a compose deployment and the `tokeirad:latest` image is absent from the local Docker image store, THE compose platform SHALL return an error instructing the operator to run `tkr image build` first, including the exact command.

### Requirement 7.2: ECS platform config writeback

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to write discovered refs into `deployment.toml` using the same writeback machinery as infra apply, so that the config file stays a faithful record of what is deployed.

#### Acceptance Criteria

1. THE Image_CLI SHALL use the shared public writeback helper defined in Req 7.3. Neither `tkr infra` nor `tkr image` SHALL implement its own dotted-key TOML writer.
2. THE Image_Writeback SHALL preserve existing TOML comments and formatting in `deployment.toml`.
3. THE Image_Writeback SHALL create intermediate TOML tables when the target dotted key does not yet exist.
4. THE Image_Writeback SHALL overwrite an existing value when the target dotted key is already present.
5. FOR ALL Image_Writeback operations with N key-value pairs, reading each value at its specified path after write SHALL produce the original value (round-trip property).
6. WHEN the writeback fails (permission, I/O error, malformed TOML), THE Image_CLI SHALL return an error describing the failure and SHALL NOT claim the push or mirror succeeded.

### Requirement 7.3: Shared writeback helper extraction

**User Story:** As a Tokeira maintainer, I want the existing private writeback helper in `apps/tkr/src/commands/infra.rs` extracted into a public API, so that `tkr infra` and `tkr image` call the same code path — while recognising that they target different config files.

#### Acceptance Criteria

1. THE current private helpers in `apps/tkr/src/commands/infra.rs` (`write_tokeirad_writeback` and its private dotted-key `toml_edit` writer) SHALL be extracted into `tokeira_iac::writeback` as `pub fn write_config_values(path: &Path, values: &[(&str, &str)]) -> Result<(), WritebackError>` — taking the absolute file path explicitly so the helper is file-agnostic.
2. `WritebackError` SHALL be a `thiserror` enum covering I/O failure, TOML parse failure, and dotted-key validation failure.
3. AFTER extraction, `apps/tkr/src/commands/infra.rs` SHALL call `tokeira_iac::write_config_values(&deployment_path.join("tokeirad.toml"), &borrowed)` — preserving the existing behaviour of writing IaC outputs to the server config file.
4. THE `tkr image push` and `tkr image mirror` handlers SHALL call `tokeira_iac::write_config_values(&deployment_path.join("deployment.toml"), &borrowed)` — image refs live in the platform config file, not in `tokeirad.toml`.
5. THE existing proptests in `apps/tkr/src/commands/infra.rs` SHALL be moved to `crates/tokeira-iac/src/writeback.rs` and continue to pass against the new path-explicit signature.
6. THE helper SHALL NOT hardcode any file name. Callers decide which file is written.

### Requirement 7.4: ECS config field declarations

**User Story:** As a Tokeira developer, I want the `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields to be explicitly documented as writeback targets populated by `tkr image`, so that operators know where image references come from.

#### Acceptance Criteria

1. THE `EcsConfig.services.<service>.image` field for each of the seven services SHALL be populated by Image_Writeback from `tkr image push` via the ECS `TokeiradImage::writeback_targets` (Req 2.2.3).
2. THE `EcsConfig.observability.{mimir_image, loki_image, grafana_image, alloy_image, aws_cli_image, busybox_image}` fields SHALL be populated by Image_Writeback from `tkr image mirror` via each ECS observability image's `writeback_targets`.
3. WHEN any field above is empty or points to an upstream source at the time `tkr infra apply` or `tkr deploy apply` runs, THE ECS platform SHALL return an error instructing the operator to run the corresponding `tkr image` subcommand.

### Requirement 7.5: Observability config field additions

**User Story:** As a Tokeira developer, I want `aws_cli_image` and `busybox_image` fields added to both platforms' observability configs, so that init-container utility images are also mirror targets.

#### Acceptance Criteria

1. `ComposeConfig.observability` SHALL add `aws_cli_image: String` and `busybox_image: String` fields. Each field SHALL use a per-field `#[serde(default = "default_<field_name>")]` attribute pointing at a standalone `fn default_<field_name>() -> String` that returns the upstream ref — NOT the bare `#[serde(default)]` which would deserialize missing fields as `String::default()` (the empty string).
2. `EcsConfig.observability` SHALL use the same per-field `#[serde(default = "…")]` pattern with identical default functions.
3. Defaults: `default_aws_cli_image()` returns `"public.ecr.aws/aws-cli/aws-cli:latest"`; `default_busybox_image()` returns `"public.ecr.aws/docker/library/busybox:latest"`.
4. Each platform's observability `mirror_image!` invocations SHALL rely on these field values being non-empty. The `desired_ref` implementation SHALL return a `RuntimeError::Image(format!("image '<name>' has empty upstream_ref in config"))` when the configured upstream is the empty string, so a malformed config surfaces as an operator-facing error rather than an empty mirror pull.
5. Each platform's `TokeiradImage::writeback_targets` / observability-images `writeback_targets` SHALL be updated to cover these two new fields on the Mirror side.
6. A deserialization unit test SHALL feed a `deployment.toml` that omits `aws_cli_image` and `busybox_image` and assert the parsed `ObservabilityConfig` carries the upstream defaults (not empty strings). A separate test SHALL feed a `deployment.toml` that sets either field to an empty string and assert `desired_ref` returns the documented `RuntimeError::Image`.

---

## Feature 8: Lifecycle Ordering

### Requirement 8.1: Mirror before infra apply (ECS platform)

**User Story:** As a Tokeira operator, I want the CLI to refuse `tkr infra apply` on an ECS deployment until `tkr image mirror` has populated the observability image refs, so that I cannot deploy task definitions that would fail to pull their images.

#### Acceptance Criteria

1. WHEN the deployment platform is `ecs` AND `tkr infra apply` is invoked, THE ECS platform SHALL iterate its own image set filtered to Mirror images and validate that each image's writeback-target fields in `EcsConfig` point to a ref whose host matches the expected ECR registry.
2. IF any such field is empty or points to an upstream source, THE CLI SHALL return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image mirror`.
3. THE validation SHALL occur during `tkr infra apply` only. `tkr infra plan` SHALL warn but SHALL NOT refuse to produce a plan.
4. THE validation SHALL occur for the `ecs` platform only.

### Requirement 8.2: Build and push before deploy apply (ECS platform)

**User Story:** As a Tokeira operator, I want the CLI to refuse `tkr deploy apply` on an ECS deployment until `tkr image push` has populated the service image refs, so that ECS task definitions cannot reference a missing image.

#### Acceptance Criteria

1. WHEN the deployment platform is `ecs` AND `tkr deploy apply` is invoked, THE ECS platform SHALL iterate its own image set filtered to Build images and validate that each image's writeback-target fields point to a ref whose host matches the expected ECR registry.
2. IF any such field is empty or points to an upstream source, THE CLI SHALL return an error listing the fields and instructing the operator to run `tkr image push --tag <version>`.
3. THE validation SHALL occur during `tkr deploy apply` only. `tkr deploy plan` SHALL warn but SHALL NOT refuse.
4. THE validation SHALL occur for the `ecs` platform only.

### Requirement 8.3: Build before deploy apply (compose platform)

**User Story:** As a Tokeira developer, I want the CLI to refuse `tkr deploy apply` on a compose deployment until `tokeirad:latest` exists in the local Docker image store, so that `docker compose up` does not fail with a pull error.

#### Acceptance Criteria

1. WHEN the deployment platform is `compose` AND `tkr deploy apply` is invoked AND `ComposeConfig.tokeirad.image == "tokeirad:latest"`, THE compose platform SHALL query the local Docker image store via a `DockerImageInspector` trait.
2. IF `tokeirad:latest` is absent, THE compose platform SHALL return an error instructing the operator to run `tkr image build`, including the exact command.
3. WHEN `ComposeConfig.tokeirad.image` is any value other than `"tokeirad:latest"`, THE compose platform SHALL NOT enforce this check. Operators who point at a remote ref take responsibility for pull authentication and availability.
4. THE `DockerImageInspector` trait SHALL be defined so that unit tests can substitute a mock implementation without requiring a live Docker daemon.

### Requirement 8.4: Image commands do not require prior lifecycle stages

**User Story:** As a Tokeira operator, I want `tkr image mirror` and `tkr image push` to run on a fresh deployment before any infrastructure has been provisioned, so that images are ready in ECR before `tkr infra apply` references them.

#### Acceptance Criteria

1. THE `tkr image mirror` and `tkr image push` subcommands SHALL NOT require `tkr infra apply` to have run first. They need only an ECR registry reachable from the operator's workstation and valid AWS credentials.
2. WHEN the deployment's `infra state` is empty, THE Image_CLI SHALL derive the ECR registry host as `{account_id}.dkr.ecr.{region}.amazonaws.com` using the account and region from the deployment's config and the operator's AWS credentials.
3. THE Image_CLI SHALL NOT create VPC endpoints, IAM roles, or any other AWS resources beyond ECR repositories.

---

## Feature 9: Correctness Properties

### Requirement 9.1: Per-platform registry validation property

**User Story:** As a Tokeira maintainer, I want each platform's `images::all()` refused at construction time if it has duplicate names or repositories.

#### Acceptance Criteria

1. A property test (per platform) SHALL construct that platform's `images::all(ctx)` across generated config values and assert `validate_registry` accepts the canonical set.
2. A negative test SHALL inject a duplicate and assert `validate_registry` returns `RuntimeError::Image`.

### Requirement 9.2: Source-type / upstream invariant

**User Story:** As a Tokeira maintainer, I want each image's `source_type` and `upstream_ref` kept consistent.

#### Acceptance Criteria

1. A per-platform property test SHALL iterate its own `images::all(ctx)` (with multiple realistic `ctx` values) and assert:
   - `image.source_type() == Build` ⇒ `image.desired_ref(ctx)?.upstream_ref == None`.
   - `image.source_type() == Mirror` ⇒ `image.desired_ref(ctx)?.upstream_ref == Some(_)`.
   - `image.source_type() == Registry` ⇒ no invariant beyond Req 1.6 — the property SHALL skip Registry images or assert only that `desired_ref` returns `Ok(_)`.
2. THE property SHALL run as part of the default `cargo test` invocation.

### Requirement 9.3: Mirror idempotence property

**User Story:** As a Tokeira maintainer, I want mirror idempotence encoded as a testable property.

#### Acceptance Criteria

1. FOR the ECS platform, running `tkr image mirror` twice in sequence (with mocked ECR and mocked Dagger) SHALL produce:
   - the same set of ensured repositories
   - the same set of mirrored digests
   - the same set of writeback key-value pairs in `deployment.toml`
2. THE test SHALL assert equality of the `deployment.toml` contents before the second invocation and after the second invocation.
3. THE test SHALL mock the Dagger client at the `DaggerClient` trait boundary and the ECR client at the `EcrClient` trait boundary.

### Requirement 9.4: Repository creation idempotence property

**User Story:** As a Tokeira maintainer, I want ECR repository creation idempotence encoded as a testable property.

#### Acceptance Criteria

1. FOR ALL valid `repo_names: Vec<String>` with no duplicates, calling `ensure_ecr_repositories(names)` twice with the same input SHALL leave the AWS-mocked state identical.
2. THE property SHALL be tested with at least 64 generated cases via `proptest`.

### Requirement 9.5: Lifecycle policy round-trip property

**User Story:** As a Tokeira maintainer, I want the canonical Lifecycle_Policy JSON to round-trip through parse and serialize without loss.

#### Acceptance Criteria

1. Parsing `ECR_LIFECYCLE_POLICY`, serializing, and re-parsing SHALL produce the same `serde_json::Value`.

### Requirement 9.6: Writeback round-trip property

**User Story:** As a Tokeira maintainer, I want Image_Writeback to round-trip through the TOML writer.

#### Acceptance Criteria

1. FOR ALL generated `(dotted_key, value)` pairs, writing the pair via `tokeira_iac::write_config_values` and then reading at `dotted_key` SHALL produce the original `value`.
2. THE test SHALL be implemented in `tokeira-iac` (migrated from the existing `apps/tkr/src/commands/infra.rs` tests).

### Requirement 9.7: Per-platform mirror stability property

**User Story:** As a Tokeira maintainer, I want each platform's mirror set to stay aligned with its own config defaults.

#### Acceptance Criteria

1. FOR each Mirror image returned by the compose platform's `observability::all()`, `desired_ref(ctx).upstream_ref.unwrap()` with `ComposeConfig::default()` registered SHALL equal the matching field in `ComposeConfig::default().observability`.
2. FOR each Mirror image returned by the ECS platform's `observability::all()`, `desired_ref(ctx).upstream_ref.unwrap()` with `EcsConfig::default()` registered SHALL equal the matching field in `EcsConfig::default().observability`.
3. Each test lives in the platform it tests — no cross-platform imports.

### Requirement 9.8: ECR name grammar property (per platform)

**User Story:** As a Tokeira maintainer, I want every image's resolved repository name validated against the ECR grammar.

#### Acceptance Criteria

1. Per Req 2.4 — a per-platform property test validates `desired_ref(ctx)?.repository` against the grammar across realistic `ctx` values.

### Requirement 9.9: Lifecycle gate predicates

**User Story:** As a Tokeira maintainer, I want platform lifecycle gates validated as pure predicates driven by the image set, so that the ECS platform cannot drift from what the image plane writes back.

#### Acceptance Criteria

1. `platforms/ecs` SHALL expose `fn validate_mirrors(cfg: &EcsConfig, registry: &str, images: &[Box<dyn Image>]) -> Result<(), EcsError>` that returns `Err` iff any Mirror image's writeback-target fields are empty or don't start with `{registry}/`.
2. A symmetric `fn validate_builds(cfg: &EcsConfig, registry: &str, images: &[Box<dyn Image>]) -> Result<(), EcsError>` SHALL cover Build images.
3. Property tests SHALL drive each predicate across generated configs plus the platform's image set and assert the predicate returns `Err` iff at least one targeted field is empty or not `{registry}/`-prefixed.

---

## Feature 10: Cross-Cutting Requirements

### Requirement 10.1: Tests without network or Docker

**User Story:** As a Tokeira developer, I want the default test suite to run without Docker, without the Dagger daemon, and without AWS credentials.

#### Acceptance Criteria

1. THE unit tests for the Build_Crate SHALL NOT require a running Dagger daemon. Tests supply a mock `DaggerClient`.
2. THE unit tests for the ECR repository resource SHALL NOT require AWS credentials. Tests supply a mock `EcrClient`.
3. THE unit tests for the compose build gate SHALL NOT require a running Docker daemon. Tests supply a mock `DockerImageInspector`.
4. THE integration tests that require Dagger, Docker, or real AWS credentials SHALL be gated behind a feature flag (`integration-test`) or an `#[ignore]` attribute.

### Requirement 10.2: Documentation

**User Story:** As a Tokeira operator new to the project, I want the `tkr image` command group documented in `README.md` and `AGENTS.md`.

#### Acceptance Criteria

1. THE root `README.md` SHALL be updated in four specific places:
   a. **`### Command Tree`** (under `## `tkr` — Operator and Developer CLI`) — the ASCII command tree SHALL include an `image` subtree placed between `deploy` and `schema`:
      ```
      ├── image
      │   ├── list [--source-type <build|mirror>] [--json]
      │   ├── build [--arch <arm64|amd64>] [--tag <version>]
      │   ├── push --tag <version> [--image <name>] [--yes]
      │   └── mirror [--image <name>] [--yes]
      ```
   b. **The `### Compose Platform` section** SHALL be rewritten so the walkthrough shows the image-build step explicitly as part of deploying a compose stack. The updated walkthrough SHALL:
      - Add `tkr image build` between `tkr deployment create` and `tkr infra apply`, making it clear that compose depends on `tokeirad:latest` existing in the local Docker image store before `tkr deploy apply` (Req 8.3)
      - Add a short prose paragraph immediately after the walkthrough explaining WHY the build step is separate: `tkr deploy apply` does not invoke the builder; it requires the image to already be present locally
      - Add a paragraph on storage and schema: the default example uses `--storage in-memory` (no schema to set up). For compose deployments with `--storage dsql`, operators must point `tokeirad.toml`'s `infrastructure.dsql.endpoint` at an externally-provisioned DSQL cluster (the compose platform has no DSQL module) and then run `tkr schema setup` before `tkr deploy apply`. State that DSQL schema provisioning for compose is currently deferred to a future spec, and that the in-memory path is the supported compose workflow today
   c. **A new section `### Image Management`** SHALL be added under `## `tkr` — Operator and Developer CLI`, positioned after the updated `### Compose Platform` section, covering all four subcommands with example invocations, the lifecycle ordering rules (Req 8.1–8.3), and the prerequisites (Dagger ≥ 0.20, AWS credentials with `ecr:*` for push/mirror).
   d. **The `## Quick Start` section** SHALL be updated if and only if it currently references compose deployment; if so, align the example with the image-build-first ordering in (b).
2. THE root `AGENTS.md` SHALL reference:
   - The lifecycle ordering rules from Feature 8, added to the "Working Agreements" section.
   - An "Adding a new image" checklist in the "Working Agreements" section, alongside the existing "Adding a new CLI command" and "Adding a new IaC Module" checklists.
   - A note in the "Observability Stack (Compose Platform)" section that the six mirror images (`grafana-mimir`, `grafana-loki`, `grafana-oss`, `grafana-alloy`, `aws-cli`, `busybox`) are declared in each platform's `src/images/observability/mod.rs`, so version bumps are one-line changes in the platform's config defaults.
3. THE `tkr image <subcommand> --help` output SHALL be sufficient to use the subcommand without reading the spec.

### Requirement 10.3: No tool sprawl

**User Story:** As a Tokeira maintainer, I want image lifecycle to avoid introducing new build tools beyond Dagger.

#### Acceptance Criteria

1. THE Build_Crate SHALL NOT depend on a Dockerfile templater, Helm-like tool, or any manifest templating engine.
2. THE Build_Crate SHALL NOT introduce a new image format or registry protocol. Standard OCI images and the ECR Docker Registry V2 API are the only formats used.

### Requirement 10.4: Trait stability

**User Story:** As a Tokeira maintainer, I want the `Image` trait surface stable across releases, so that downstream consumers (platform image modules, CLI, gates) can rely on its shape.

#### Acceptance Criteria

1. Adding fields to `DesiredImageRef`, `ImageContext`, or `WritebackTarget` SHALL be backwards-compatible (additive only).
2. Renaming existing fields on these types SHALL require a spec amendment.
3. Adding a method to the `Image` trait SHALL be done with a default implementation where possible, so existing image structs do not need to change.
4. Removing a method from the `Image` trait SHALL require a spec amendment.
