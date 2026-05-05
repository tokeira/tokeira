# Requirements Document: Image Lifecycle

## Introduction

Tokeira ships a single server binary today, `tokeirad`, that runs on three platforms: `local` (bare-process), `compose` (Docker Compose), and `ecs` (AWS ECS on EC2, private-only). Today the workspace has no documented path for producing the `tokeirad` container image that the compose platform defaults to (`tokeirad:local`), no path for publishing it to ECR, and no path for mirroring the pinned third-party images the ECS platform depends on (`grafana/mimir`, `grafana/loki`, `grafana/grafana-oss`, `grafana/alloy`, `public.ecr.aws/aws-cli/aws-cli`, `public.ecr.aws/docker/library/busybox`). In a private-only VPC with no internet gateway, direct pulls from Docker Hub fail, so every image referenced by an ECS task definition must already live in a project-owned ECR repository before `tkr infra apply` runs.

This spec owns the image plane of the deployment lifecycle. It strengthens Tokeira's IaC abstractions by introducing a single trait-based image model: every deployable container image — whether we build it from source or pull it from an upstream registry — implements one `Image` trait, and the CLI, Dagger pipelines, writeback machinery, and platform gates iterate that one abstraction.

The core abstraction is a single `Image` trait that every deployable artifact implements:

- A built image (`tokeirad`, future `tokeira-tool`) implements `Image` with `source_type = Build`.
- A mirrored image (`grafana-mimir`, `grafana-loki`, etc.) implements `Image` with `source_type = Mirror`.
- Image sets are grouped into modules: `tokeira::all()`, `observability::all()`, etc.
- The CLI iterates the composed image set uniformly: build every `Build` image, mirror every `Mirror` image, publish every image whose desired ref differs from its resolved live ref.

What this gives us:

1. A **`tokeira-build` library crate** that owns the `Image` trait, the `ImageSourceType` enum, and the `DesiredImageRef` / `ImageContext` types, plus the Dagger-backed build and mirror pipelines keyed off `source_type`.
2. **Per-domain image modules** declaring the concrete images the deployment uses. The first-cut set is `tokeira::TokeiradImage` (build) plus `observability::{MimirImage, LokiImage, GrafanaImage, AlloyImage, AwsCliImage, BusyBoxImage}` (all mirror). Adding a new image — whether built or mirrored — is a three-line struct declaration.
3. A **`tkr image` command group** with four subcommands: `list`, `build`, `push`, `mirror`. The command handlers iterate the image-trait registry, not hardcoded switch statements.
4. An **`EcrRepository` IaC resource** — project-scoped names, MUTABLE tag mutability, the canonical "keep last 10 untagged" lifecycle policy.
5. **Config writeback** that populates `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields after push and mirror, reusing the `toml_edit` writeback machinery owned by [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md).
6. **Lifecycle ordering rules** implemented as platform-level validators driven from the image-trait registry, not from hardcoded field lists.

### What this spec delivers

- A `tokeira-build` library crate at `crates/tokeira-build/` containing:
  - The `Image` trait, `ImageSourceType` enum, `DesiredImageRef` struct, `ImageContext` type — the core abstractions for the image plane.
  - Dagger-backed build pipeline for `tokeirad` (and any future `Build` image).
  - Dagger-backed mirror pipeline for any `Mirror` image.
  - The concrete image modules: `images::tokeira`, `images::observability`, and a registry-level `all(context)` assembling them.
- A `dagger-client` dependency (new in-repo `crates/dagger-client/` crate, per the reference implementation in `.kiro/specs/image-lifecycle/reference/`).
- A `tkr image` command group in `apps/tkr` with four subcommands: `list`, `build`, `push`, `mirror`.
- An `EcrRepository` resource implementation in `tokeira-aws` with the canonical "keep last 10 untagged" lifecycle policy.
- Config writeback into `deployment.toml` after push and mirror, reusing `toml_edit` machinery from the iac-resource-lifecycle spec.

### What this spec does NOT cover

- CI/CD pipeline integration. The [`pipeline-foundation`](../pipeline-foundation/requirements.md) spec defines the CI substrate; a future pipeline crate may wrap `tokeira-build` as a library.
- Multi-region mirroring — one mirror region per deployment.
- Image signing, SBOM generation, or vulnerability scanning.
- Compose-platform image loading (Docker Compose reads the local image cache directly; no additional action is required for `tkr image build` to satisfy `tokeirad:local`).
- The ECS platform's `EcsConfig.services.*` field definitions themselves — those are owned by the [`ecs-deployment`](../ecs-deployment/requirements.md) spec. This spec consumes those fields as writeback targets.

### Cross-references

- [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md): Progress callbacks on `ProvisionContext` and TOML writeback via `toml_edit` are owned there. This spec consumes those surfaces — it does not redefine them.
- [`ecs-deployment`](../ecs-deployment/requirements.md): Requires that ECR repositories exist and that `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields be populated before `tkr infra apply` or `tkr deploy apply` can succeed. This spec owns that image plane.
- [`tkr-cli`](../tkr-cli/requirements.md): Owns the global CLI structure, `--deployment` / `--json` flags, XDG paths, and command-tree conventions. This spec adds a new `image` command group that follows those conventions.
- [`pipeline-foundation`](../pipeline-foundation/requirements.md): Future image-related pipelines wrap the library surface defined here.

## Glossary

- **Image_Trait**: The `tokeira_build::Image` trait that every deployable artifact implements. Provides `name()`, `source_type()`, and `desired_ref(ctx)`. The single abstraction through which the CLI, pipelines, writeback, and platform gates interact with images.
- **Image_Source_Type**: The `ImageSourceType` enum with exactly two variants: `Build` (produced locally via a Dagger build pipeline) and `Mirror` (pulled from an upstream ref and re-pushed to a project-owned destination).
- **Desired_Image_Ref**: The `DesiredImageRef` struct produced by `Image::desired_ref()`, carrying `repository` (the project-scoped repo name without a registry host prefix), `tag`, and `upstream_ref: Option<String>` (`Some` for Mirror, `None` for Build).
- **Image_Context**: The `ImageContext` struct passed to `Image::desired_ref()`. Carries the deployment config via a typed extension mechanism so images can read the project name, observability image pins, and any other config fields without coupling.
- **Image_Registry**: The flat list produced by `tokeira_build::images::all(ctx)` composing every image module's contribution (`tokeira::all()`, `observability::all()`, and future modules). Consumed by the CLI for iteration.
- **Image_Module**: A Rust module under `crates/tokeira-build/src/images/` containing concrete `Image` trait implementations grouped by domain (`tokeira`, `observability`). Each module exports an `all() -> Vec<Box<dyn Image>>` function enumerating its images.
- **Tokeirad_Image**: The `TokeiradImage` struct implementing `Image` with `source_type = Build`. Produces `tokeirad` from `apps/tokeirad/` via the Dagger build pipeline. Its local tag is `tokeirad:local`; its remote refs are `{registry}/{project}/tokeirad:{tag}`.
- **Observability_Image**: Any of `MimirImage`, `LokiImage`, `GrafanaImage`, `AlloyImage`, `AwsCliImage`, `BusyBoxImage` — all implementing `Image` with `source_type = Mirror` and reading their `upstream_ref` from `EcsConfig.observability.*_image`.
- **Build_Crate**: The `tokeira-build` library crate in `crates/tokeira-build/` that owns the `Image` trait and the Dagger-backed pipelines.
- **Dagger_Client**: The in-repo GraphQL client wrapper at `crates/dagger-client/` that drives a Dagger session from Rust. Introduced by this spec, reference implementation at `.kiro/specs/image-lifecycle/reference/`.
- **Image_CLI**: The `tkr image` command group in `apps/tkr`, exposing `list`, `build`, `push`, and `mirror` subcommands.
- **ECR_Registry**: The Amazon Elastic Container Registry in a specific AWS region and account, identified by its registry host (`{account}.dkr.ecr.{region}.amazonaws.com`).
- **Project_Repository**: An ECR repository owned by the Tokeira deployment, named `{project_name}/{repo_suffix}` where `repo_suffix` comes from the image's `DesiredImageRef.repository` field.
- **Lifecycle_Policy**: The ECR repository lifecycle policy JSON applied by this spec. Canonical policy: keep the last 10 untagged images; tagged images are never expired by lifecycle rules.
- **Mirror_Operation**: The act of pulling a Mirror image's `upstream_ref` and publishing it to the corresponding Project_Repository.
- **Push_Operation**: The act of publishing a Build image's locally-produced artifact to a Project_Repository under two tags: `latest` and a version-specific tag supplied by the operator (defaulting to `latest` when no version is supplied).
- **Registry_Credentials**: The username, password, and registry host obtained by calling ECR `GetAuthorizationToken` and base64-decoding the returned token in `user:password` form.
- **Image_Writeback**: The act of writing discovered image references (a pushed ref or a mirrored ref) back into `deployment.toml` under specific TOML keys, using `toml_edit` to preserve comments and formatting. Each image declares its own writeback target (see Requirement 2.4).
- **Config_Writeback_Module**: The `toml_edit`-backed writer owned by the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) spec. This spec consumes that module rather than reimplementing dotted-key TOML insertion.
- **Image_Tag_Mutability**: The ECR repository setting that controls whether a tag (e.g., `latest`) may be overwritten. All repositories created by this spec SHALL be set to `MUTABLE` so `latest` can move with each push.
- **Reproducible_Build**: A build in which the same source tree plus the same pinned toolchain (`rust-toolchain.toml`) produces an image whose application binary layer is bit-identical across invocations on the same host architecture.
- **Target_Architecture**: The CPU architecture of a Build image: `arm64` (default — Graviton4 on ECS, native on Apple Silicon for compose) or `amd64` (operator override for x86 hosts and Intel-based deployments).

## Requirements

---

## Feature 1: Image Trait and Source-Type Abstraction

### Requirement 1.1: `Image` trait surface

**User Story:** As a Tokeira developer, I want a single `Image` trait that every deployable artifact implements, so that the build, push, mirror, and listing flows iterate one abstraction instead of special-casing per image.

#### Acceptance Criteria

1. THE Build_Crate SHALL define a `pub trait Image: std::fmt::Debug + Send + Sync` exposing at minimum:
   - `fn name(&self) -> &str` — a stable human-readable identifier (for example, `"tokeirad"`, `"grafana-mimir"`).
   - `fn source_type(&self) -> ImageSourceType` — `Build` or `Mirror`.
   - `fn desired_ref(&self, ctx: &ImageContext) -> Result<DesiredImageRef, BuildError>` — resolves the desired target repository, tag, and (for Mirror) upstream ref.
2. THE `Image` trait SHALL be the only abstraction the Image_CLI and the Dagger pipelines consume. Ad-hoc enumeration of per-image functions SHALL NOT appear in `tokeira-build` or `apps/tkr`.
3. THE trait SHALL be stable enough that adding a new image (built or mirrored) requires writing one struct plus inserting it into one module's `all()` function. No trait changes, no CLI changes, no Dagger-pipeline changes.

### Requirement 1.2: `ImageSourceType` enum

**User Story:** As a Tokeira developer, I want the image source-type dimension encoded as a two-variant enum, so that build and mirror flows can branch on it and the CLI can filter or group images by source type.

#### Acceptance Criteria

1. THE Build_Crate SHALL define `pub enum ImageSourceType { Build, Mirror }` with `#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]`.
2. FOR ALL images the `desired_ref(ctx)` contract SHALL hold: if `source_type() == Build`, then `desired_ref(ctx)?.upstream_ref == None`; if `source_type() == Mirror`, then `desired_ref(ctx)?.upstream_ref == Some(_)`.
3. A property test SHALL assert this invariant over every image in the Image_Registry.

### Requirement 1.3: `DesiredImageRef` struct

**User Story:** As a Tokeira developer, I want each image to return a uniform reference descriptor, so that downstream code (publish, mirror, writeback, gates) consumes a consistent shape.

#### Acceptance Criteria

1. THE Build_Crate SHALL define:
   ```rust
   pub struct DesiredImageRef {
       pub repository: String,      // project-scoped, without registry host (e.g. "tokeira-dev/tokeirad")
       pub tag: String,             // e.g. "latest", "v1.2.3", "3.0.6"
       pub upstream_ref: Option<String>,  // Some for Mirror, None for Build
   }
   ```
2. THE `repository` field SHALL always begin with the project name prefix. Any image that attempts to produce a repository without the project prefix SHALL fail validation at registry-construction time.
3. THE `tag` field SHALL never include a `/` or `@`. Tag validation SHALL reject any input matching `[:\/@]`.
4. FOR Mirror images, `upstream_ref` SHALL be a fully-qualified source ref (including upstream registry host when present) matching the value in the deployment's `EcsConfig.observability.*_image` field.
5. THE struct SHALL derive `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`.

### Requirement 1.4: `ImageContext` and typed extensions

**User Story:** As a Tokeira image author, I want `desired_ref()` to read config via typed extensions rather than via a fixed parameter list, so that adding a new config source (for example, a new config struct) does not force changes to every image's signature.

#### Acceptance Criteria

1. THE Build_Crate SHALL define `pub struct ImageContext` carrying typed extensions:
   - `pub fn new() -> Self`
   - `pub fn set_extension<T: 'static + Send + Sync>(&mut self, value: T)`
   - `pub fn extension<T: 'static + Send + Sync>(&self) -> Option<&T>`
2. THE production code path SHALL register the deployment's platform config (for example `EcsConfig`) on the `ImageContext` before invoking `desired_ref()` on any image. An image that needs config reads it via `ctx.extension::<EcsConfig>()`.
3. THE `ImageContext` SHALL NOT reference any specific config type in its public API — extensions are added by the caller at runtime.
4. A property test SHALL assert that for every registered image, `desired_ref(&ctx)` where `ctx` carries a realistic `EcsConfig` produces a `DesiredImageRef` whose invariants (Req 1.3) hold.

---

## Feature 2: Concrete Image Implementations

### Requirement 2.1: `images::tokeira::TokeiradImage`

**User Story:** As a Tokeira developer, I want the `tokeirad` binary modelled as a concrete `Image` implementation, so that the build flow and the listing flow treat it uniformly with every other image.

#### Acceptance Criteria

1. THE Build_Crate SHALL provide `pub mod images::tokeira` containing a `TokeiradImage` struct implementing `Image`.
2. `TokeiradImage::name()` SHALL return `"tokeirad"`.
3. `TokeiradImage::source_type()` SHALL return `ImageSourceType::Build`.
4. `TokeiradImage::desired_ref(ctx)` SHALL read the project name from the `ImageContext` and produce `DesiredImageRef { repository: format!("{project}/tokeirad"), tag: <computed>, upstream_ref: None }`.
5. THE module SHALL export `pub fn all() -> Vec<Box<dyn Image>>` returning a `vec!` of the `TokeiradImage` and any future Tokeira-owned built images.
6. A `TokeiraToolImage` (schema migration utility) MAY be added in a follow-up amendment under the same module without changing any other part of this spec.

### Requirement 2.2: `images::observability` set

**User Story:** As a Tokeira developer, I want every observability-stack image modelled as a concrete `Image` struct in a single module, so that a bump to any observability image pin is localised and the mirror flow picks it up without glue changes.

#### Acceptance Criteria

1. THE Build_Crate SHALL provide `pub mod images::observability` containing six concrete `Image` implementations:
   - `MimirImage` — `name = "grafana-mimir"`, source = Mirror, upstream = `ctx.extension::<EcsConfig>().observability.mimir_image`.
   - `LokiImage` — `name = "grafana-loki"`, source = Mirror, upstream = `observability.loki_image`.
   - `GrafanaImage` — `name = "grafana-oss"`, source = Mirror, upstream = `observability.grafana_image`.
   - `AlloyImage` — `name = "grafana-alloy"`, source = Mirror, upstream = `observability.alloy_image`.
   - `AwsCliImage` — `name = "aws-cli"`, source = Mirror, upstream = `observability.aws_cli_image`.
   - `BusyBoxImage` — `name = "busybox"`, source = Mirror, upstream = `observability.busybox_image`.
2. EACH image's `desired_ref()` SHALL produce `DesiredImageRef { repository: format!("{project}/{repo_suffix}"), tag: image_tag(upstream).unwrap_or("latest").to_string(), upstream_ref: Some(upstream.clone()) }`.
3. THE module SHALL export `pub fn all() -> Vec<Box<dyn Image>>` returning all six in the canonical order above.
4. THE observability repo-suffix names (`grafana-mimir`, `grafana-loki`, `grafana-oss`, `grafana-alloy`, `aws-cli`, `busybox`) SHALL reflect the upstream image names each mirrors. The `aws-cli` and `busybox` entries additionally feed the ECS platform's init-container story defined in [`ecs-deployment`](../ecs-deployment/requirements.md).

### Requirement 2.3: Image-registry composition (`images::all`)

**User Story:** As a Tokeira developer, I want a single `all(ctx) -> Vec<Box<dyn Image>>` function that composes every image-module's contribution, so that the CLI has one uniform iteration target and duplicates / orphans are detectable.

#### Acceptance Criteria

1. THE Build_Crate SHALL expose `pub fn images::all(ctx: &ImageContext) -> Vec<Box<dyn Image>>` returning the concatenation of `tokeira::all() + observability::all()` (and any future image-module contributions).
2. THE function SHALL validate the concatenated registry before returning it:
   - No duplicate `name()` values.
   - No duplicate `(source_type, repository)` pairs (after resolving `desired_ref(ctx)`).
3. Duplicate violations SHALL return `BuildError::RegistryValidation { kind, names }`. Callers SHALL NOT be expected to handle this — a failing `all()` is a programming error, not a runtime condition.

### Requirement 2.4: Image_Writeback targets are image-provided, not caller-hardcoded

**User Story:** As a Tokeira developer adding a new image, I want the image itself to declare which config field the push or mirror flow writes back to, so that adding an image is one change in one place rather than a multi-file coordinated edit.

#### Acceptance Criteria

1. THE `Image` trait SHALL expose `fn writeback_targets(&self, ctx: &ImageContext) -> Vec<WritebackTarget>` where `WritebackTarget { field: &'static str, kind: WritebackKind }` describes the dotted TOML key(s) the push or mirror flow writes the computed remote ref to.
2. THE `WritebackKind` enum SHALL have variants `Once` (single field; for example a Mirror image writes to its `observability.mimir_image` field) and `Many` (multiple fields; for example the `tokeirad` Build image writes to all seven `services.*.image` fields on ECS).
3. THE default implementation SHALL return an empty `Vec` — an image with no writeback does nothing on the config side (useful for Build images whose refs are consumed only by operator-facing tooling, not config).
4. THE `tkr image push` and `tkr image mirror` handlers SHALL iterate `image.writeback_targets(ctx)` rather than carrying their own hardcoded field list. Adding a new service to the ECS platform means updating `TokeiradImage::writeback_targets`, not the CLI.

---

## Feature 3: Dagger-Backed Build and Mirror Pipelines

### Requirement 3.1: Build pipeline for Build images

**User Story:** As a Tokeira operator, I want every `Build` image produced through a reproducible Dagger pipeline keyed off the image's trait data, so that the CLI can build any Build image uniformly without per-image wiring.

#### Acceptance Criteria

1. THE Build_Crate SHALL expose `pub fn build_image(image: &dyn Image, request: &BuildRequest, dagger: &dyn DaggerClient) -> Result<BuildResult, BuildError>` that executes the Dagger pipeline for a single Build image.
2. `BuildRequest { arch: Arch, tag: String, workspace_root: PathBuf }` SHALL carry the architecture, local tag, and workspace root. The `image` argument supplies name and desired repo via its trait methods.
3. THE pipeline SHALL drive the build through Dagger rather than invoking `docker build` directly.
4. THE pipeline SHALL resolve the Rust toolchain version from `rust-toolchain.toml` at the workspace root and SHALL pin the build container's Rust version to that value.
5. THE pipeline SHALL build the binary with `cargo build --release --bin {image.name()} --target <target-triple>` using the `Target_Architecture` from the request.
6. THE resulting container image SHALL contain exactly one application binary at `/usr/local/bin/{image.name()}` and the minimal runtime dependencies (CA certificates, timezone data).
7. THE resulting container image SHALL run as a non-root user (UID/GID 1000) with `{image.name()}` as both the username and group name.
8. THE resulting container image SHALL declare `ENTRYPOINT ["/usr/local/bin/{image.name()}"]` and SHALL leave CMD empty by default.
9. FOR ALL invocations with the same source tree, same `rust-toolchain.toml`, same Target_Architecture, and same image, the produced application binary layer SHALL be bit-identical (Reproducible_Build property).
10. THE pipeline SHALL refuse to run on images whose `source_type() != Build`, returning `BuildError::SourceTypeMismatch`.

### Requirement 3.2: Mirror pipeline for Mirror images

**User Story:** As a Tokeira operator, I want every `Mirror` image pulled and re-pushed through a uniform Dagger flow, so that the CLI handles the six observability images through one code path.

#### Acceptance Criteria

1. THE Build_Crate SHALL expose `pub fn mirror_image(image: &dyn Image, ctx: &ImageContext, creds: &RegistryCredentials, dagger: &dyn DaggerClient) -> Result<MirroredReference, BuildError>`.
2. THE function SHALL call `image.desired_ref(ctx)` to resolve the source and destination, authenticate to the destination registry using `creds`, and publish the pulled image to `{creds.registry_host}/{desired.repository}:{desired.tag}`.
3. THE function SHALL NOT require a local `docker pull` step — the Dagger pipeline handles source-to-destination transfer in a single session.
4. FOR ALL invocations with identical `image`, `ctx`, and destination, calling `mirror_image` twice SHALL produce the same destination image (digest-level idempotence: the second call re-pushes the same digest).
5. IF the upstream source returns an authentication error, THEN THE function SHALL return `BuildError::UpstreamAuth`. The `RegistryCredentials` argument is only for the destination; upstream sources are assumed to be public.
6. THE pipeline SHALL refuse to run on images whose `source_type() != Mirror`, returning `BuildError::SourceTypeMismatch`.

### Requirement 3.3: Publish pipeline shared by push

**User Story:** As a Tokeira operator, I want the `tkr image push` flow to publish a locally-built image to multiple remote refs in a single authenticated Dagger session, so that `:latest` and `:{tag}` get the same digest.

#### Acceptance Criteria

1. THE Build_Crate SHALL expose `pub fn publish_image(local_image: &str, remote_refs: &[String], creds: &RegistryCredentials, dagger: &dyn DaggerClient) -> Result<PublishResult, BuildError>`.
2. THE function SHALL authenticate to `creds.registry_host` using `creds.username` and `creds.password` and push `local_image` to every reference in `remote_refs`.
3. WHEN `remote_refs` is empty, THE function SHALL return `BuildError::Validation { reason: "remote_refs cannot be empty" }`.
4. THE `PublishResult` SHALL contain one `PublishedReference { remote_ref, published_ref }` entry per successfully pushed reference; `published_ref` carries the digest-pinned reference Dagger returns.
5. IF any `remote_ref` push fails, THEN THE function SHALL return `BuildError::Publish { remote_ref, source }` naming the failing reference. Prior successful pushes SHALL NOT be undone.

### Requirement 3.4: Target-architecture support for Build images

**User Story:** As a Tokeira operator, I want to build `arm64` images by default with an opt-in for `amd64`, so that the same workflow serves Graviton4 ECS hosts, Apple Silicon compose users, and x86 Intel hosts.

#### Acceptance Criteria

1. `BuildRequest` SHALL include an `arch: Arch` field where `pub enum Arch { Arm64, Amd64 }`.
2. THE Image_CLI SHALL default `arch` to `Arch::Arm64`.
3. IF an invalid architecture string is supplied on the CLI, THE Image_CLI SHALL return a usage error naming the invalid value and listing the valid values.
4. WHEN `arch = Arm64`, THE Dagger pipeline SHALL use a Rust build container whose target triple is `aarch64-unknown-linux-musl`.
5. WHEN `arch = Amd64`, THE Dagger pipeline SHALL use a Rust build container whose target triple is `x86_64-unknown-linux-musl`.
6. THE produced image's manifest SHALL declare the platform (`linux/arm64` or `linux/amd64`) matching the Target_Architecture.
7. `Arch` SHALL implement `FromStr` with an error type of `BuildError::UnsupportedArch { supplied }`.

### Requirement 3.5: Local image tag for the compose platform

**User Story:** As a Tokeira developer, I want `tkr image build` with no flags to produce `tokeirad:local`, so that the compose platform's default image reference works without any additional configuration.

#### Acceptance Criteria

1. WHEN `tkr image build` is invoked with no `--tag` override, THE Build_Crate SHALL export the image as `{image.name()}:local` (for `TokeiradImage`, that is `tokeirad:local`).
2. WHEN `tkr image build` is invoked with `--tag <value>`, THE Build_Crate SHALL export the image as `{image.name()}:<value>`.
3. FOR ALL invocations that succeed, the local Docker image store SHALL contain the exported tag so subsequent `docker compose up` commands resolve the image without a registry pull.
4. THE `tokeirad:local` tag SHALL match the compose platform's default `ComposeConfig.tokeirad.image` value defined in `platforms/compose/src/config.rs`.

---

## Feature 4: Dagger Client Dependency

### Requirement 4.1: Dagger session bootstrap

**User Story:** As a Tokeira developer, I want the build crate to obtain a Dagger session without requiring operators to manage session lifetime manually, so that one `tkr image <subcommand>` invocation is self-contained.

#### Acceptance Criteria

1. WHEN the Image_CLI is invoked without active Dagger session environment variables (`DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` both unset), THE Image_CLI SHALL re-execute itself under `dagger run` with the same arguments and exit with that process's status.
2. WHEN both `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` are set, THE Image_CLI SHALL NOT re-execute under `dagger run` and SHALL proceed with the Dagger session established by the wrapping process.
3. IF `dagger` is not on the operator's PATH, THEN THE Image_CLI SHALL return an error stating that the `dagger` CLI (>= 0.20) must be installed, with a link to the Dagger installation documentation.
4. THE re-exec flow SHALL forward the `--deployment`, `--json`, and all `image` subcommand arguments unchanged.

### Requirement 4.2: Dagger client location

**User Story:** As a Tokeira developer, I want the Dagger client dependency owned in the Tokeira workspace, so that the build crate has a small, auditable GraphQL surface rather than pulling a heavyweight SDK.

#### Acceptance Criteria

1. THE Build_Crate SHALL depend on an in-repo `crates/dagger-client/` crate. THE reference implementation at `.kiro/specs/image-lifecycle/reference/` SHALL be ported into the workspace with minor adjustments documented in the reference README.
2. THE Dagger client interface consumed by the Build_Crate SHALL include at minimum: `host_directory(path)`, `container_from(image)`, `container_build(context, dockerfile)`, `with_exec(args)`, `with_file(path, file)`, `with_entrypoint(args)`, `export_image(tag)`, `publish(remote_ref)`, `with_registry_auth(registry, username, secret)`, `set_secret(name, value)`.
3. THE Dagger client SHALL NOT be exposed as a public dependency of the Build_Crate — its types SHALL be internal implementation details. The Build_Crate exposes a thin `DaggerClient` trait over the in-repo client so tests can substitute a mock.

---

## Feature 5: ECR Repository Provisioning

### Requirement 5.1: `EcrRepository` IaC resource

**User Story:** As a Tokeira operator, I want ECR repositories to be provisioned as IaC resources alongside the rest of the deployment's AWS infrastructure, so that repositories are tracked in state, diffed on plan, and cleaned up on destroy according to the same lifecycle rules as every other resource.

#### Acceptance Criteria

1. THE `tokeira-aws` crate SHALL define an `EcrRepository` resource implementing the `Resource` trait from `tokeira-iac`.
2. THE `EcrRepository` resource SHALL accept a repository name field. THE resource SHALL require that the name is non-empty and matches the AWS ECR repository name grammar (lowercase alphanumerics, `/`, `-`, `_`, `.`, 2–256 characters).
3. THE `EcrRepository` SHALL set Image_Tag_Mutability to `MUTABLE` on create.
4. THE `EcrRepository` SHALL apply the canonical Lifecycle_Policy (keep last 10 untagged images) on both create and update.
5. THE `EcrRepository::describe()` method SHALL return `None` when the repository does not exist in AWS, so destroy operations following the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) describe-before-delete rule are idempotent.
6. THE `EcrRepository::diff()` method SHALL report a lifecycle policy drift as an update when the current policy JSON differs from the canonical Lifecycle_Policy.
7. THE `EcrRepository` SHALL carry the same auto-generated and operator-defined tags as all other AWS resources per the [`ecs-deployment`](../ecs-deployment/requirements.md) tagging requirement.

### Requirement 5.2: Canonical Lifecycle_Policy

**User Story:** As a Tokeira operator, I want a standard lifecycle policy applied to every project-owned ECR repository, so that storage costs are bounded without manually setting policies per repository.

#### Acceptance Criteria

1. THE Lifecycle_Policy SHALL be a single rule with `rulePriority = 1`, `tagStatus = "untagged"`, `countType = "imageCountMoreThan"`, `countNumber = 10`, `action.type = "expire"`.
2. THE Lifecycle_Policy SHALL NOT expire tagged images. Operators pruning tagged images SHALL do so manually or via a future operator-driven command — this spec does not introduce one.
3. FOR ALL repositories provisioned by this spec, applying the policy twice SHALL be idempotent: the second `PutLifecyclePolicy` call SHALL produce the same policy state as the first.

### Requirement 5.3: Project-scoped repository names derive from `DesiredImageRef.repository`

**User Story:** As a Tokeira operator, I want repository names derived uniformly from the `Image::desired_ref().repository` field, so that the set of repositories the deployment needs is mechanically derivable from the image registry.

#### Acceptance Criteria

1. THE Image_CLI SHALL derive the set of required repositories by iterating `images::all(ctx)` and collecting `desired_ref(ctx)?.repository` values.
2. FOR the `TokeiradImage`, `desired_ref(ctx)?.repository` SHALL equal `{project_name}/tokeirad`.
3. FOR the observability Mirror images, `desired_ref(ctx)?.repository` SHALL equal `{project_name}/{repo_suffix}` where `repo_suffix` matches Req 2.2.1.
4. WHERE a deployment's `project_name` contains characters outside the ECR repository name grammar, THE Image_CLI SHALL return a validation error naming the invalid character.

### Requirement 5.4: Repository existence handling

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to create repositories on first use and tolerate pre-existing repositories on re-runs, so that the commands are idempotent without requiring a separate provisioning step.

#### Acceptance Criteria

1. WHEN `tkr image push` or `tkr image mirror` runs, THE Image_CLI SHALL ensure each required Project_Repository (from Req 5.3) exists before attempting to push.
2. WHEN the repository already exists, THE Image_CLI SHALL NOT return an error. It SHALL proceed to apply the Lifecycle_Policy (which is idempotent) and then to push.
3. WHEN the repository does not exist, THE Image_CLI SHALL create it with Image_Tag_Mutability = `MUTABLE` and THEN apply the Lifecycle_Policy.
4. FOR ALL invocations with the same project name and image set, calling `tkr image push` or `tkr image mirror` twice in a row SHALL produce the same set of repositories with the same lifecycle policy.

---

## Feature 6: Image CLI

### Requirement 6.1: `tkr image` command group

**User Story:** As a Tokeira operator, I want a single `tkr image` command group for all image-plane operations, so that list, build, push, and mirror workflows are discoverable together and do not pollute other command groups.

#### Acceptance Criteria

1. THE Image_CLI SHALL expose a top-level `image` subcommand under `tkr` with four children: `list`, `build`, `push`, and `mirror`.
2. THE `tkr image` command group SHALL follow the [`tkr-cli`](../tkr-cli/requirements.md) conventions for global flags (`--deployment`, `--json`) and for help-text formatting.
3. THE `tkr image` command group SHALL appear between `tkr deployment` and `tkr infra` in `tkr --help` output, reflecting its position in the deployment lifecycle.
4. WHEN `tkr image` is invoked with no subcommand, THE Image_CLI SHALL print a help message listing the four children.

### Requirement 6.2: `list` subcommand

**User Story:** As a Tokeira operator, I want `tkr image list` to enumerate every image the deployment knows about, so that I can see at a glance what would be built and what would be mirrored.

#### Acceptance Criteria

1. `tkr image list` SHALL print one row per image in `images::all(ctx)`, with columns: `NAME`, `SOURCE`, `REPOSITORY`, `TAG`, `UPSTREAM` (empty for Build images).
2. `tkr image list --source-type build` SHALL filter to Build images. `--source-type mirror` SHALL filter to Mirror images.
3. `tkr image list --json` SHALL emit an array of `{ name, source_type, repository, tag, upstream_ref }` objects, one per image.
4. THE subcommand SHALL require an active deployment (`--deployment`) because image resolution reads the platform config via `ImageContext`.

### Requirement 6.3: `build` subcommand

**User Story:** As a Tokeira developer, I want `tkr image build` to build every registered Build image (today: `tokeirad`; future: additional built images) with sensible defaults, so that the compose platform works out of the box.

#### Acceptance Criteria

1. THE `tkr image build` subcommand SHALL accept the following optional flags: `--arch <arm64|amd64>` (default `arm64`), `--tag <value>` (default `local`), `--image <name>` (default: every Build image).
2. WHEN invoked with no flags, THE `tkr image build` subcommand SHALL build every Build image returned by `images::all(ctx)` for the `arm64` architecture and export each as `{image.name()}:local`.
3. WHEN invoked with `--image tokeirad --tag v1.2.3`, THE subcommand SHALL build only `tokeirad` and export it as `tokeirad:v1.2.3`.
4. THE subcommand SHALL emit progress events via the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) progress callback surface for each stage (toolchain resolution, compile, image assembly, export) of each image.
5. WHEN the operator passes `--json`, THE subcommand SHALL emit JSON progress events plus a final `{ "action": "build", "images": [{ "name": "<n>", "tag": "<t>", "arch": "<a>" }, ...] }` summary.
6. THE subcommand SHALL NOT require an active deployment (`--deployment`) because the build only uses workspace sources and produces local images.

### Requirement 6.4: `push` subcommand

**User Story:** As a Tokeira operator, I want `tkr image push` to authenticate with ECR, ensure repositories exist, push every Build image with both `latest` and a version tag, and write back the resulting remote refs, so that the next `tkr infra apply` and `tkr deploy apply` can consume them without manual edits.

#### Acceptance Criteria

1. THE `tkr image push` subcommand SHALL accept `--tag <value>` (defaults to `latest` only when explicitly omitted) and `--image <name>` (default: every Build image).
2. THE subcommand SHALL require an active deployment.
3. THE subcommand SHALL fail with a clear error message when a required local image (e.g. `tokeirad:latest`) is not present in the Docker image store, instructing the operator to run `tkr image build` first.
4. THE subcommand SHALL call ECR `GetAuthorizationToken` once per run, decode the base64 `user:password` token, and use the decoded credentials for both repository-ensure and image-publish steps.
5. FOR EACH selected Build image, THE subcommand SHALL publish two references: `{registry}/{desired.repository}:latest` and `{registry}/{desired.repository}:{tag}`. The `latest` tag SHALL always be pushed regardless of the `--tag` value.
6. THE subcommand SHALL perform Image_Writeback: for each selected Build image, for each target in `image.writeback_targets(ctx)`, write the version-tagged remote ref into the target's dotted key in `deployment.toml`.
7. WHEN the operator passes `--json`, THE subcommand SHALL emit JSON progress events plus a final summary event.

### Requirement 6.5: `mirror` subcommand

**User Story:** As a Tokeira operator, I want `tkr image mirror` to copy every Mirror image into project-owned ECR repositories and write the mirrored refs back into the deployment config, so that a private-only ECS deployment has every image it needs before `tkr infra apply`.

#### Acceptance Criteria

1. THE `tkr image mirror` subcommand SHALL accept no positional arguments and MAY accept `--image <name>` (default: every Mirror image).
2. THE subcommand SHALL require an active deployment.
3. THE subcommand SHALL iterate every Mirror image returned by `images::all(ctx)` (filtered by `--image` if supplied) and for each:
   - ensure the destination Project_Repository exists (Req 5.4),
   - invoke `mirror_image(image, ctx, creds, dagger)`,
   - collect the result into a writeback list driven by `image.writeback_targets(ctx)`.
4. THE subcommand SHALL perform Image_Writeback: each mirrored remote ref SHALL be written into its mapped config fields in `deployment.toml`.
5. THE subcommand SHALL be idempotent. FOR ALL invocations with the same deployment, calling `tkr image mirror` twice in a row SHALL succeed both times and SHALL leave the same set of config keys populated with the same values after the second invocation as after the first.
6. WHEN a source field in `deployment.toml` already points to the corresponding project-scoped destination (for example, `observability.mimir_image` is already `{registry}/{project}/grafana-mimir:3.0.6`), THE subcommand SHALL treat that image as already-mirrored and SHALL skip the pull/push. The skip SHALL be reported in the summary.
7. WHEN the operator passes `--json`, THE subcommand SHALL emit JSON progress events plus a final summary event.

### Requirement 6.6: Confirmation prompts

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to respect the same confirmation rules as other mutating commands, so that I cannot accidentally overwrite remote state or configs.

#### Acceptance Criteria

1. THE `tkr image push` and `tkr image mirror` subcommands SHALL follow the [`tkr-cli`](../tkr-cli/requirements.md) confirmation rules: interactive confirmation by default, `--yes` to bypass, refuse to proceed when stdout is non-TTY and `--yes` is not provided.
2. THE `tkr image list` and `tkr image build` subcommands SHALL NOT require confirmation: `list` is read-only and `build` produces only local artifacts.
3. WHEN the operator passes `--yes`, THE subcommand SHALL proceed without prompting.
4. WHEN stdout is non-TTY and `--yes` is not provided, THE subcommand SHALL return an error instructing the operator to re-run with `--yes` in non-interactive contexts.

---

## Feature 7: Config Integration

### Requirement 7.1: Compose platform config alignment

**User Story:** As a Tokeira operator, I want the compose platform's `tokeirad.image` default to remain `tokeirad:local` and to be produced by `tkr image build`, so that no manual intervention is needed to bring up a compose deployment after a fresh clone.

#### Acceptance Criteria

1. THE `ComposeConfig::default()` value for `tokeirad.image` SHALL be `"tokeirad:local"`.
2. THE `tkr image build` subcommand with default flags SHALL produce an image with the tag `tokeirad:local` for the `TokeiradImage`.
3. THE compose platform SHALL NOT invoke `tkr image build` automatically on `tkr deploy apply`. Operators SHALL run `tkr image build` manually. This spec does not introduce an automatic build step on deploy apply.
4. IF `tkr deploy apply` is invoked on a compose deployment and the `tokeirad:local` image is absent from the local Docker image store, THEN THE compose platform SHALL return an error instructing the operator to run `tkr image build` first, including the exact command to run.

### Requirement 7.2: ECS platform config writeback

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to write discovered refs into `deployment.toml` using the same writeback machinery as infra apply, so that the config file stays a faithful record of what is deployed.

#### Acceptance Criteria

1. THE Image_CLI SHALL use the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) `toml_edit` writeback module for all Image_Writeback operations.
2. THE Image_Writeback SHALL preserve existing TOML comments and formatting in `deployment.toml`.
3. THE Image_Writeback SHALL create intermediate TOML tables when the target dotted key does not yet exist.
4. THE Image_Writeback SHALL overwrite an existing value when the target dotted key is already present.
5. FOR ALL Image_Writeback operations with N key-value pairs, reading each value at its specified path after write SHALL produce the original value (round-trip property, inherited from iac-resource-lifecycle).
6. WHEN the writeback fails (permission, I/O error, malformed TOML), THE Image_CLI SHALL return an error describing the failure and SHALL NOT claim the push or mirror succeeded. Images in ECR SHALL remain in place — writeback failure is reported but not rolled back.

### Requirement 7.3: ECS config field declarations

**User Story:** As a Tokeira developer, I want the `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields to be explicitly documented as writeback targets populated by `tkr image`, so that operators and downstream specs know where the image reference comes from.

#### Acceptance Criteria

1. THE `EcsConfig.services.<service>.image` field for each of the seven services (`edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`) SHALL be populated by Image_Writeback from `tkr image push` (via `TokeiradImage::writeback_targets`).
2. THE `EcsConfig.observability.{mimir_image, loki_image, grafana_image, alloy_image, aws_cli_image, busybox_image}` fields SHALL be populated by Image_Writeback from `tkr image mirror` (via each observability image's `writeback_targets`).
3. WHEN any of these fields are empty or point to an upstream source at the time `tkr infra apply` or `tkr deploy apply` runs on an ECS deployment, THE ECS platform SHALL return an error instructing the operator to run the corresponding `tkr image` subcommand. This matches the existing error pattern for Managed-mode DSQL hydration defined in [`ecs-deployment`](../ecs-deployment/requirements.md).

### Requirement 7.4: Version source-of-truth

**User Story:** As a Tokeira operator, I want the pinned version of each third-party image to match the compose platform exactly, so that local compose deployments and ECS deployments run the same binaries.

#### Acceptance Criteria

1. THE pinned versions in `ComposeConfig::default()` SHALL match the prototypical defaults generated by `tkr deployment create --platform ecs`.
2. WHEN a version in `ComposeConfig::default()` is changed, THE prototypical ECS config SHALL be updated in the same change set. A unit test SHALL enforce this equality.
3. THE `busybox` and `aws-cli` versions SHALL be pinned to stable references; using `:latest` is explicitly documented as acceptable for these two images because they are used only as init-container utility images where tag stability matters less than service-image stability.

---

## Feature 8: Lifecycle Ordering

### Requirement 8.1: Mirror before infra apply (ECS platform)

**User Story:** As a Tokeira operator, I want the CLI to refuse `tkr infra apply` on an ECS deployment until `tkr image mirror` has populated the observability image refs, so that I cannot accidentally deploy task definitions that would fail to pull their images.

#### Acceptance Criteria

1. WHEN the deployment platform is `ecs` AND `tkr infra apply` is invoked, THE ECS platform SHALL iterate `images::all(ctx)` filtered to Mirror images and validate that each image's writeback targets in `EcsConfig` point to a ref whose host matches the expected ECR registry for the deployment's account and region.
2. IF any such field is empty or points to an upstream source, THEN THE CLI SHALL return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image mirror`.
3. THE validation SHALL occur during `tkr infra apply` only. `tkr infra plan` SHALL warn but SHALL NOT refuse to produce a plan.
4. THE validation SHALL occur for the `ecs` platform only. `local` and `compose` platforms SHALL skip this check.

### Requirement 8.2: Build and push before deploy apply (ECS platform)

**User Story:** As a Tokeira operator, I want the CLI to refuse `tkr deploy apply` on an ECS deployment until `tkr image push` has populated the service image refs, so that ECS task definitions cannot reference a missing image.

#### Acceptance Criteria

1. WHEN the deployment platform is `ecs` AND `tkr deploy apply` is invoked, THE ECS platform SHALL iterate `images::all(ctx)` filtered to Build images and validate that each image's writeback targets point to a ref whose host matches the expected ECR registry for the deployment's account and region.
2. IF any such field is empty or points to an upstream source, THEN THE CLI SHALL return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image push --tag <version>`.
3. THE validation SHALL occur during `tkr deploy apply` only. `tkr deploy plan` SHALL warn but SHALL NOT refuse to produce a plan.
4. THE validation SHALL occur for the `ecs` platform only.

### Requirement 8.3: Build before deploy apply (compose platform)

**User Story:** As a Tokeira developer, I want the CLI to refuse `tkr deploy apply` on a compose deployment until `tokeirad:local` exists in the local Docker image store, so that `docker compose up` does not fail with a pull error on the default registry.

#### Acceptance Criteria

1. WHEN the deployment platform is `compose` AND `tkr deploy apply` is invoked AND `ComposeConfig.tokeirad.image == "tokeirad:local"`, THE compose platform SHALL query the local Docker image store for the presence of `tokeirad:local`.
2. IF `tokeirad:local` is absent from the local store, THEN THE compose platform SHALL return an error instructing the operator to run `tkr image build`, including the exact command.
3. WHEN `ComposeConfig.tokeirad.image` is any value other than `"tokeirad:local"`, THE compose platform SHALL NOT enforce this check. Operators who point at a remote ref take responsibility for pull authentication and availability.

### Requirement 8.4: Image commands do not require prior lifecycle stages

**User Story:** As a Tokeira operator, I want `tkr image mirror` and `tkr image push` to run on a fresh deployment before any infrastructure has been provisioned, so that images are ready in ECR before `tkr infra apply` references them.

#### Acceptance Criteria

1. THE `tkr image mirror` and `tkr image push` subcommands SHALL NOT require `tkr infra apply` to have run first. They need only an ECR registry reachable from the operator's workstation and valid AWS credentials.
2. WHEN the deployment's `infra state` is empty, THE Image_CLI SHALL derive the ECR registry host as `{account_id}.dkr.ecr.{region}.amazonaws.com` using the account and region from the deployment's config and the operator's AWS credentials.
3. THE Image_CLI SHALL NOT create VPC endpoints, IAM roles, or any other AWS resources beyond ECR repositories. All other infrastructure provisioning belongs to the `tkr infra` command group.

---

## Feature 9: Correctness Properties

### Requirement 9.1: Registry validation property

**User Story:** As a Tokeira maintainer, I want the image registry to refuse duplicates at construction time, so that duplicate names or repositories cannot ship.

#### Acceptance Criteria

1. A property test SHALL generate arbitrary registries composed of variant-selected images and assert that `images::all(ctx)` rejects a registry containing two images with the same `name()` or the same `(source_type, repository)` pair after resolution.
2. THE property SHALL be tested with at least 64 generated cases via `proptest`.

### Requirement 9.2: Source-type / upstream invariant

**User Story:** As a Tokeira maintainer, I want the `source_type` and `upstream_ref` fields kept consistent for every image, so that a Build image cannot accidentally declare an upstream or a Mirror image forget its upstream.

#### Acceptance Criteria

1. A property test SHALL iterate every image in `images::all(ctx)` (with multiple realistic `ctx` values) and assert:
   - `image.source_type() == Build` ⇒ `image.desired_ref(ctx)?.upstream_ref == None`.
   - `image.source_type() == Mirror` ⇒ `image.desired_ref(ctx)?.upstream_ref == Some(_)`.
2. THE property SHALL run as part of the default `cargo test` invocation.

### Requirement 9.3: Mirror idempotence property

**User Story:** As a Tokeira maintainer, I want mirror idempotence encoded as a testable property, so that regressions that turn mirror into a non-idempotent operation fail CI.

#### Acceptance Criteria

1. FOR ALL valid `EcsConfig` values `cfg`, running `tkr image mirror` twice in sequence (with mocked ECR and mocked Dagger) SHALL produce:
   - the same set of ensured repositories,
   - the same set of mirrored digests,
   - the same set of writeback key-value pairs in `deployment.toml`.
2. THE test SHALL assert equality of the `deployment.toml` contents before the second invocation and after the second invocation.
3. THE test SHALL mock the Dagger client at the `DaggerClient` trait boundary so no real network calls are made.

### Requirement 9.4: Repository creation idempotence property

**User Story:** As a Tokeira maintainer, I want ECR repository creation idempotence encoded as a testable property, so that re-running the repository-ensure step under any ordering produces the same end state.

#### Acceptance Criteria

1. FOR ALL valid `repo_names: Vec<String>` with no duplicates, calling `ensure_ecr_repositories(names)` twice with the same input SHALL leave the AWS-mocked state identical to calling it once.
2. THE property SHALL be tested with at least 64 generated cases via `proptest`, covering empty lists, single-element lists, and multi-element lists (up to 20 names) with random ASCII-printable names passing the ECR name grammar filter.

### Requirement 9.5: Lifecycle policy round-trip property

**User Story:** As a Tokeira maintainer, I want the canonical Lifecycle_Policy JSON to round-trip through parse and serialize without loss, so that we can diff stored policies against the canonical form reliably.

#### Acceptance Criteria

1. Parsing the canonical Lifecycle_Policy string, serializing, and re-parsing SHALL produce the same `serde_json::Value`.
2. THE test SHALL run as a unit test in `tokeira-aws`.

### Requirement 9.6: Writeback round-trip property

**User Story:** As a Tokeira maintainer, I want Image_Writeback to round-trip through the TOML writer, so that values written back are exactly what `tkr image push` and `tkr image mirror` intended.

#### Acceptance Criteria

1. Covered by [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md); this spec consumes it and does not duplicate it.

### Requirement 9.7: Mirror mapping stability property

**User Story:** As a Tokeira maintainer, I want the observability mirror set to remain in sync with the compose platform's default image pins, so that the local and remote image planes use the same versions.

#### Acceptance Criteria

1. FOR each Mirror image returned by `observability::all()`, the image's `desired_ref(ctx)?.upstream_ref.unwrap()` with a default-constructed `EcsConfig::observability` SHALL equal the corresponding field in `ComposeConfig::default().observability`.
2. WHEN a default is bumped in either place, this property SHALL fail in CI unless both are bumped together.

### Requirement 9.8: ECR name grammar property

**User Story:** As a Tokeira maintainer, I want every image's resolved repository name validated against the ECR grammar, so that an image that accidentally declares an invalid repo name cannot ship.

#### Acceptance Criteria

1. A property test SHALL iterate every image in `images::all(ctx)` across realistic `ctx` values and assert that `desired_ref(ctx)?.repository` satisfies the ECR repository name grammar: 2–256 characters, lowercase alphanumerics, `/`, `-`, `_`, `.`, not starting with `/` or `.`.

### Requirement 9.9: Lifecycle gate predicates

**User Story:** As a Tokeira maintainer, I want the platform lifecycle gates validated as pure predicates driven by the image registry, so that the ECS platform cannot drift from what the image plane writes back.

#### Acceptance Criteria

1. `platforms/ecs` SHALL expose a pure predicate `fn validate_mirrors(cfg: &EcsConfig, registry: &str, images: &[Box<dyn Image>]) -> Result<(), EcsError>` that returns `Err` iff any Mirror image's writeback-target fields are empty or don't start with `{registry}/`.
2. A symmetric `fn validate_builds(cfg: &EcsConfig, registry: &str, images: &[Box<dyn Image>]) -> Result<(), EcsError>` SHALL cover Build images.
3. Property tests SHALL drive each predicate across generated configs plus the concrete image registry and assert the predicate returns `Err` iff at least one targeted field is empty or not `{registry}/`-prefixed.

---

## Feature 10: Cross-Cutting Requirements

### Requirement 10.1: Tests without network or Docker

**User Story:** As a Tokeira developer, I want the default test suite to run without Docker, without the Dagger daemon, and without AWS credentials, so that tests run in any contributor environment.

#### Acceptance Criteria

1. THE unit tests for the Build_Crate SHALL NOT require a running Dagger daemon. THE Build_Crate SHALL use dependency inversion (a trait-bounded `DaggerClient`) so tests can supply a mock.
2. THE unit tests for the ECR repository resource SHALL NOT require AWS credentials. THE resource SHALL use dependency inversion (a trait-bounded `EcrClient`) so tests can supply a mock.
3. THE integration tests that require Dagger, Docker, or real AWS credentials SHALL be gated behind a feature flag (`integration-test`) or an `#[ignore]` attribute, matching the AGENTS.md testing guidance for this workspace.

### Requirement 10.2: Documentation

**User Story:** As a Tokeira operator new to the project, I want the `tkr image` command group documented in `README.md` and `AGENTS.md`, so that I can find the expected workflow without reading specs.

#### Acceptance Criteria

1. THE root `README.md` SHALL include a "Building and publishing images" section covering `tkr image list`, `tkr image build`, `tkr image push`, and `tkr image mirror`.
2. THE root `AGENTS.md` SHALL reference:
   - The image lifecycle ordering rules from Feature 8.
   - The "Adding a new image" checklist: write a struct implementing `Image`, add it to the domain module's `all()`, run the property tests.
3. THE `tkr image <subcommand> --help` output SHALL be sufficient to use the subcommand without reading the spec.

### Requirement 10.3: No introduction of tool sprawl

**User Story:** As a Tokeira maintainer, I want image lifecycle to avoid introducing new build tools beyond Dagger, so that the workspace stays consistent with the "no tool sprawl" principle.

#### Acceptance Criteria

1. THE Build_Crate SHALL NOT depend on a Dockerfile templater, Helm-like tool, or any manifest templating engine. The Dagger pipeline is constructed programmatically in Rust.
2. THE Build_Crate SHALL NOT introduce a new image format or registry protocol. Standard OCI images and the ECR Docker Registry V2 API are the only formats used.
3. THE Build_Crate SHALL NOT introduce any network-facing dependencies that operate outside of Dagger sessions or `aws-sdk-ecr` calls.

### Requirement 10.4: Trait stability

**User Story:** As a Tokeira maintainer, I want the `Image` trait surface and its associated types to be stable, so that downstream consumers (CLI, platform gates, future image-lifecycle pipelines) can rely on the shape across releases.

#### Acceptance Criteria

1. Adding fields to `DesiredImageRef`, `ImageContext`, or `WritebackTarget` SHALL be backwards-compatible (new fields are additive, default-constructed at the call sites that do not populate them).
2. Renaming existing fields on any of these types SHALL require a spec amendment and a deprecation cycle.
3. Adding a method to the `Image` trait SHALL be done with a default implementation where at all possible, so that existing image structs do not need to change.
4. Removing a method from the `Image` trait SHALL require a spec amendment and a deprecation cycle.
