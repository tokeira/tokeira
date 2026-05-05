# Requirements Document: Image Lifecycle

## Introduction

Tokeira ships a single server binary, `tokeirad`, that runs on three platforms: `local` (bare-process), `compose` (Docker Compose), and `ecs` (AWS ECS on EC2, private-only). Today the workspace has no documented path for producing the `tokeirad` container image that the compose platform defaults to (`tokeirad:local`), and no path for publishing it to ECR or for mirroring the pinned third-party images the ECS platform depends on (`grafana/mimir`, `grafana/loki`, `grafana/grafana-oss`, `grafana/alloy`, `public.ecr.aws/aws-cli/aws-cli`, `public.ecr.aws/docker/library/busybox`). In a private-only VPC with no internet gateway, direct pulls from Docker Hub fail, so every image referenced by an ECS task definition must already live in a project-owned ECR repository before `tkr infra apply` runs.

This spec owns the image plane of the deployment lifecycle:

1. A new `tokeira-build` library crate that drives reproducible `tokeirad` image builds through a Dagger pipeline, using the Rust toolchain pinned in `rust-toolchain.toml`.
2. A new `image` command group on the `tkr` CLI (`tkr image build|push|mirror`) that wraps the library.
3. ECR repository provisioning (project-scoped names, lifecycle policy: keep last 10 untagged).
4. Mirroring of the pinned third-party images into project-owned ECR repositories, with idempotent behaviour on re-runs.
5. Config writeback that populates `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields after push and mirror, reusing the `toml_edit` writeback pattern owned by [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md).
6. Lifecycle ordering rules: `tkr image mirror` must precede `tkr infra apply` on the ECS platform; `tkr image build` + `tkr image push` must precede `tkr deploy apply` on the ECS platform; `tkr image build` with no flags produces the `tokeirad:local` tag the compose platform references by default.

### What this spec delivers

- A `tokeira-build` library crate at `crates/tokeira-build/` with a Dagger-backed build flow for the `tokeirad` binary.
- A `dagger-client` dependency (either a new `crates/dagger-client/` crate or a re-use of a published Dagger SDK; design phase decides).
- A `tkr image` command group in `apps/tkr` with three subcommands: `build`, `push`, `mirror`.
- An `EcrRepository` resource implementation in `tokeira-aws` with a lifecycle policy of "keep last 10 untagged images".
- A canonical mirror mapping table that covers the six pinned third-party images referenced by the compose and ECS platforms.
- Config writeback into `deployment.toml` after push and mirror, reusing `toml_edit` machinery from the iac-resource-lifecycle spec.

### What this spec does NOT cover

- CI/CD pipeline integration (no GitHub Actions, no automated release tagging).
- Multi-region mirroring — one mirror region per deployment.
- Image signing, SBOM generation, or vulnerability scanning.
- Runtime image variants beyond `tokeirad` (if a future spec adds a separate `tokeira-admin` image, it extends this spec; it does not pre-empt it).
-  Compose-platform image loading (Docker Compose reads the local image cache directly; no additional action is required for `tkr image build` to satisfy `tokeirad:local`).
- The ECS platform's `EcsConfig.services.*` field definitions themselves — those are owned by the [`ecs-deployment`](../ecs-deployment/requirements.md) spec. This spec consumes those fields as writeback targets.

### Cross-references

- [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md): Progress callbacks on `ProvisionContext` and TOML writeback via `toml_edit` are owned there. This spec consumes those surfaces — it does not redefine them.
- [`ecs-deployment`](../ecs-deployment/requirements.md): Requires that ECR repositories exist and that `EcsConfig.services.*.image` and `EcsConfig.observability.*_image` fields be populated before `tkr infra apply` or `tkr deploy apply` can succeed. This spec owns that image plane.
- [`tkr-cli`](../tkr-cli/requirements.md): Owns the global CLI structure, `--deployment` / `--json` flags, XDG paths, and command-tree conventions. This spec adds a new `image` command group that follows those conventions.

## Glossary

- **Tokeirad_Image**: The container image produced from compiling the `tokeirad` binary in `apps/tokeirad/`. The local tag is `tokeirad:local`; remote tags are `{registry}/{project}/tokeirad:{tag}`.
- **Build_Crate**: The `tokeira-build` library crate in `crates/tokeira-build/` that drives the Dagger-backed build/publish/mirror workflows. Mirrors the EKS `dsqld_build` crate.
- **Dagger_Client**: The GraphQL client wrapper used to drive a Dagger session from native Rust. Either a new crate (`crates/dagger-client/`) or an external dependency — the design phase selects.
- **Image_CLI**: The `tkr image` command group in `apps/tkr`, exposing `build`, `push`, and `mirror` subcommands.
- **ECR_Registry**: The Amazon Elastic Container Registry in a specific AWS region and account, identified by its registry host (`{account}.dkr.ecr.{region}.amazonaws.com`).
- **Project_Repository**: An ECR repository owned by the Tokeira deployment, named `{project_name}/{image}` (for example `tokeira-dev/tokeirad`, `tokeira-dev/grafana-mimir`).
- **Lifecycle_Policy**: The ECR repository lifecycle policy JSON applied by this spec. Canonical policy: keep the last 10 untagged images; tagged images are never expired by lifecycle rules.
- **Third_Party_Image**: An image produced outside the Tokeira project and referenced by a pinned source tag. The six managed third-party images are `grafana/mimir:3.0.6`, `grafana/loki:3.7.1`, `grafana/grafana-oss:12.4.3`, `grafana/alloy:v1.16.0`, `public.ecr.aws/aws-cli/aws-cli:latest`, and `public.ecr.aws/docker/library/busybox:latest`.
- **Mirror_Mapping**: A record that pairs a Third_Party_Image source ref with its destination Project_Repository name, the config field to write back, and the resulting remote ref.
- **Mirror_Operation**: The act of pulling a Third_Party_Image from its upstream source and publishing it to the corresponding Project_Repository.
- **Push_Operation**: The act of publishing the locally-built Tokeirad_Image to a Project_Repository under two tags: `latest` and a version-specific tag supplied by the operator (defaulting to `latest` when no version is supplied).
- **Registry_Credentials**: The username, password, and registry host obtained by calling ECR `GetAuthorizationToken` and base64-decoding the returned token in `user:password` form.
- **Image_Writeback**: The act of writing discovered image references (a pushed Tokeirad_Image ref or a mirrored Third_Party_Image ref) back into `deployment.toml` under specific TOML keys, using `toml_edit` to preserve comments and formatting.
- **Config_Writeback_Module**: The `toml_edit`-backed writer owned by the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) spec. This spec consumes that module rather than reimplementing dotted-key TOML insertion.
- **Image_Tag_Mutability**: The ECR repository setting that controls whether a tag (e.g., `latest`) may be overwritten. All repositories created by this spec SHALL be set to `MUTABLE` so `latest` can move with each push.
- **Reproducible_Build**: A build in which the same source tree plus the same pinned toolchain (`rust-toolchain.toml`) produces an image whose application binary layer is bit-identical across invocations on the same host architecture.
- **Target_Architecture**: The CPU architecture of the produced image: `arm64` (default — Graviton4 on ECS, native on Apple Silicon for compose) or `amd64` (operator override for x86 hosts and Intel-based deployments).

## Requirements

---

## Feature 1: Tokeira Build Crate

### Requirement 1.1: Build Crate Structure

**User Story:** As a Tokeira developer, I want a dedicated library crate for image build workflows, so that the build orchestration is isolated from the CLI and reusable across platforms.

#### Acceptance Criteria

1. THE Build_Crate SHALL live at `crates/tokeira-build/` and SHALL be a workspace member declared in the root `Cargo.toml`.
2. THE Build_Crate SHALL expose a public API with at least the following request types: `TokeiradBuildRequest`, `PublishRequest`, `MirrorRequest`.
3. THE Build_Crate SHALL expose at least the following public functions: `build_tokeirad_image(&TokeiradBuildRequest) -> Result<TokeiradBuildResult>`, `publish_image(&PublishRequest) -> Result<PublishResult>`, `mirror_image(&MirrorRequest) -> Result<PublishedReference>`.
4. THE Build_Crate SHALL use `thiserror` for its error type and SHALL NOT expose `anyhow::Error` in its public API.
5. THE Build_Crate SHALL use `tracing` for structured logging. THE Build_Crate SHALL NOT use `println!` or `eprintln!` in library code.
6. THE Build_Crate SHALL NOT depend on `apps/tkr` or on any CLI-only crates.

### Requirement 1.2: Reproducible Tokeirad Image Build

**User Story:** As a Tokeira operator, I want `tokeirad` image builds to be reproducible across hosts and invocations, so that the same source tree produces functionally equivalent images every time.

#### Acceptance Criteria

1. THE Build_Crate SHALL drive the build through a Dagger pipeline rather than invoking `docker build` directly.
2. THE Dagger pipeline SHALL resolve the Rust toolchain version from `rust-toolchain.toml` at the workspace root and SHALL pin the build container's Rust version to that value.
3. THE Dagger pipeline SHALL build the `tokeirad` binary with `cargo build --release --bin tokeirad --target <target-triple>` using the Target_Architecture supplied in the build request.
4. THE resulting container image SHALL contain exactly one application binary (`/usr/local/bin/tokeirad`) and the minimal runtime dependencies required to execute it (CA certificates, timezone data).
5. THE resulting container image SHALL run as a non-root user (UID/GID 1000) with `tokeirad` as both the username and group name.
6. THE resulting container image SHALL declare `ENTRYPOINT ["/usr/local/bin/tokeirad"]` and SHALL leave CMD empty by default so operators supply `--config <path>` at runtime.
7. FOR ALL invocations of `build_tokeirad_image` on the same source tree, same `rust-toolchain.toml`, and same Target_Architecture, the produced application binary layer SHALL be bit-identical (Reproducible_Build property).

### Requirement 1.3: Target Architecture Support

**User Story:** As a Tokeira operator, I want to build `arm64` images by default with an opt-in for `amd64`, so that the same workflow serves Graviton4 ECS hosts, Apple Silicon compose users, and x86 Intel hosts.

#### Acceptance Criteria

1. THE `TokeiradBuildRequest` SHALL include an `arch: String` field. Valid values SHALL be `"arm64"` and `"amd64"`.
2. WHEN the build request is constructed without an explicit architecture, THE Image_CLI SHALL default `arch` to `"arm64"`.
3. IF `arch` is any value other than `"arm64"` or `"amd64"`, THEN THE Build_Crate SHALL return a validation error naming the invalid value and listing the valid values.
4. WHEN `arch = "arm64"`, THE Dagger pipeline SHALL use a Rust build container whose target triple is `aarch64-unknown-linux-musl` (or the equivalent glibc triple if musl cross-compilation is not feasible in the design phase — the design phase selects one and documents the rationale).
5. WHEN `arch = "amd64"`, THE Dagger pipeline SHALL use a Rust build container whose target triple is `x86_64-unknown-linux-musl` (or the equivalent glibc triple chosen in the design phase).
6. THE produced image's manifest SHALL declare the platform (`linux/arm64` or `linux/amd64`) matching the Target_Architecture.

### Requirement 1.4: Local Image Tag for Compose

**User Story:** As a Tokeira developer, I want `tkr image build` with no flags to produce `tokeirad:local`, so that the compose platform's default image reference works without any additional configuration.

#### Acceptance Criteria

1. WHEN the build is invoked with no `--tag` override, THE Build_Crate SHALL export the image as `tokeirad:local`.
2. WHEN the build is invoked with `--tag <value>`, THE Build_Crate SHALL export the image as `tokeirad:<value>`.
3. FOR ALL invocations that succeed, the local Docker image store SHALL contain the exported tag so subsequent `docker compose up` commands resolve the image without a registry pull.
4. THE `tokeirad:local` tag SHALL match the compose platform's default `ComposeConfig.tokeirad.image` value defined in `platforms/compose/src/config.rs`.

### Requirement 1.5: Publish Operation

**User Story:** As a Tokeira operator, I want the build crate to publish a locally-built image to one or more remote references, so that the CLI can tag the same image with both `latest` and a version tag in a single authenticated session.

#### Acceptance Criteria

1. THE `PublishRequest` SHALL include: `local_image: String`, `remote_refs: Vec<String>`, `registry: String`, `username: String`, `password: String`.
2. THE `publish_image` function SHALL authenticate to `registry` using `username` and `password` and SHALL push the image identified by `local_image` to every reference in `remote_refs`.
3. WHEN `remote_refs` is empty, THE `publish_image` function SHALL return an error naming the empty `remote_refs` field.
4. THE `PublishResult` SHALL contain one `PublishedReference { remote_ref, published_ref }` entry per successfully pushed reference.
5. IF any `remote_ref` push fails, THEN THE `publish_image` function SHALL return an error naming the failing reference. Successful prior pushes in the same call SHALL NOT be undone — push is not transactional across references.

### Requirement 1.6: Mirror Operation

**User Story:** As a Tokeira operator, I want the build crate to mirror a single remote image from an upstream registry to a destination ECR reference, so that the CLI can drive the full mirror table by invoking the same primitive for each Mirror_Mapping.

#### Acceptance Criteria

1. THE `MirrorRequest` SHALL include: `source_ref: String`, `remote_ref: String`, `registry: String`, `username: String`, `password: String`.
2. THE `mirror_image` function SHALL pull the image identified by `source_ref`, authenticate to `registry` using `username` and `password`, and push the pulled image to `remote_ref`.
3. THE `mirror_image` function SHALL NOT require a local `docker pull` step — the Dagger pipeline handles source-to-destination transfer in a single session.
4. FOR ALL invocations with identical `source_ref` and `remote_ref`, calling `mirror_image` twice SHALL produce the same destination image (idempotence at the digest level — the second call re-pushes the same digest).
5. IF the upstream source returns an authentication error, THEN THE `mirror_image` function SHALL return an error stating that source authentication failed. The only authentication provided by the `MirrorRequest` is for the destination; upstream sources are assumed to be public.

---

## Feature 2: Dagger Client Dependency

### Requirement 2.1: Dagger Session Bootstrap

**User Story:** As a Tokeira developer, I want the build crate to obtain a Dagger session without requiring operators to manage session lifetime manually, so that one `tkr image build` invocation is self-contained.

#### Acceptance Criteria

1. WHEN the Image_CLI is invoked without active Dagger session environment variables (`DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` both unset), THE Image_CLI SHALL re-execute itself under `dagger run` with the same arguments and exit with that process's status.
2. WHEN both `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN` are set, THE Image_CLI SHALL NOT re-execute under `dagger run` and SHALL proceed with the Dagger session established by the wrapping process.
3. IF `dagger` is not on the operator's PATH, THEN THE Image_CLI SHALL return an error stating that the `dagger` CLI (>= 0.20) must be installed, with a link to the Dagger installation documentation.
4. THE re-exec flow SHALL forward the `--deployment`, `--json`, and all `image` subcommand arguments unchanged.

### Requirement 2.2: Dagger Client Location

**User Story:** As a Tokeira developer, I want the Dagger client dependency decision documented in the requirements, so that the design phase has a clear scope for the client surface.

#### Acceptance Criteria

1. THE Build_Crate SHALL depend on a single Dagger client (either an in-repo `crates/dagger-client/` crate or an external dependency). THE design phase SHALL select one option and document the rationale.
2. THE Dagger client interface consumed by the Build_Crate SHALL include at minimum: `host_directory(path)`, `container_from(image)`, `container_build(context, dockerfile)`, `with_exec(args)`, `with_file(path, file)`, `with_entrypoint(args)`, `export_image(tag)`, `publish(remote_ref)`, `with_registry_auth(registry, username, secret)`, `set_secret(name, value)`.
3. THE Dagger client SHALL NOT be exposed as a public dependency of the Build_Crate — its types SHALL be internal implementation details.

---

## Feature 3: ECR Repository Provisioning

### Requirement 3.1: ECR Repository Resource

**User Story:** As a Tokeira operator, I want ECR repositories to be provisioned as IaC resources alongside the rest of the deployment's AWS infrastructure, so that repositories are tracked in state, diffed on plan, and cleaned up (or preserved) on destroy according to the same lifecycle rules as every other resource.

#### Acceptance Criteria

1. THE `tokeira-aws` crate SHALL define an `EcrRepository` resource implementing the `Resource` trait from `tokeira-iac`.
2. THE `EcrRepository` resource SHALL accept a repository name field. THE resource SHALL require that the name is non-empty and matches the AWS ECR repository name grammar (lowercase alphanumerics, `/`, `-`, `_`, `.`, 2–256 characters).
3. THE `EcrRepository` SHALL set Image_Tag_Mutability to `MUTABLE` on create.
4. THE `EcrRepository` SHALL apply the canonical Lifecycle_Policy (keep last 10 untagged images) on both create and update.
5. THE `EcrRepository::describe()` method SHALL return `None` when the repository does not exist in AWS, so destroy operations following the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) describe-before-delete rule are idempotent.
6. THE `EcrRepository::diff()` method SHALL report a lifecycle policy drift as an update when the current policy JSON differs from the canonical Lifecycle_Policy.
7. THE `EcrRepository` SHALL carry the same auto-generated and operator-defined tags as all other AWS resources per the [`ecs-deployment`](../ecs-deployment/requirements.md) tagging requirement.

### Requirement 3.2: Canonical Lifecycle Policy

**User Story:** As a Tokeira operator, I want a standard lifecycle policy applied to every project-owned ECR repository, so that storage costs are bounded without manually setting policies per repository.

#### Acceptance Criteria

1. THE Lifecycle_Policy SHALL be a single rule with `rulePriority = 1`, `tagStatus = "untagged"`, `countType = "imageCountMoreThan"`, `countNumber = 10`, `action.type = "expire"`.
2. THE Lifecycle_Policy SHALL NOT expire tagged images. Operators pruning tagged images SHALL do so manually or via a future operator-driven command — this spec does not introduce one.
3. FOR ALL repositories provisioned by this spec, applying the policy twice SHALL be idempotent: the second `PutLifecyclePolicy` call SHALL produce the same policy state as the first.

### Requirement 3.3: Project-Scoped Repository Names

**User Story:** As a Tokeira operator, I want every repository to be scoped by the project name, so that multiple deployments in the same AWS account do not collide.

#### Acceptance Criteria

1. THE Image_CLI SHALL derive repository names using the pattern `{project_name}/{image_suffix}`.
2. FOR the Tokeirad_Image, `image_suffix` SHALL be `tokeirad`. THE resulting repository name SHALL be `{project_name}/tokeirad`.
3. FOR the mirrored Third_Party_Images, `image_suffix` SHALL follow the canonical mapping in Requirement 5.1.
4. WHERE a deployment's `project_name` contains characters outside the ECR repository name grammar, THE Image_CLI SHALL return a validation error naming the invalid character.

### Requirement 3.4: Repository Existence Handling

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to create repositories on first use and tolerate pre-existing repositories on re-runs, so that the commands are idempotent without requiring a separate provisioning step.

#### Acceptance Criteria

1. WHEN `tkr image push` or `tkr image mirror` runs, THE Image_CLI SHALL ensure each required Project_Repository exists before attempting to push.
2. WHEN the repository already exists, THE Image_CLI SHALL NOT return an error. It SHALL proceed to apply the Lifecycle_Policy (which is idempotent) and then to push.
3. WHEN the repository does not exist, THE Image_CLI SHALL create it with Image_Tag_Mutability = `MUTABLE` and THEN apply the Lifecycle_Policy.
4. FOR ALL invocations with the same project name and image set, calling `tkr image push` or `tkr image mirror` twice in a row SHALL produce the same set of repositories with the same lifecycle policy.

---

## Feature 4: Image Build CLI

### Requirement 4.1: Image Command Group

**User Story:** As a Tokeira operator, I want a single `tkr image` command group for all image-plane operations, so that build, push, and mirror workflows are discoverable together and do not pollute other command groups.

#### Acceptance Criteria

1. THE Image_CLI SHALL expose a top-level `image` subcommand under `tkr` with three children: `build`, `push`, and `mirror`.
2. THE `tkr image` command group SHALL follow the [`tkr-cli`](../tkr-cli/requirements.md) conventions for global flags (`--deployment`, `--json`) and for help-text formatting.
3. THE `tkr image` command group SHALL appear between `tkr deployment` and `tkr infra` in `tkr --help` output, reflecting its position in the deployment lifecycle.
4. WHEN `tkr image` is invoked with no subcommand, THE Image_CLI SHALL print a help message listing the three children.

### Requirement 4.2: Build Subcommand

**User Story:** As a Tokeira developer, I want `tkr image build` to build the `tokeirad` image with sensible defaults, so that the compose platform works out of the box after a single command.

#### Acceptance Criteria

1. THE `tkr image build` subcommand SHALL accept the following optional flags: `--arch <arm64|amd64>` (default `arm64`), `--tag <value>` (default `local`).
2. WHEN invoked with no flags, THE `tkr image build` subcommand SHALL produce the image `tokeirad:local` for the `arm64` architecture.
3. WHEN invoked with `--tag v1.2.3`, THE `tkr image build` subcommand SHALL produce the image `tokeirad:v1.2.3` in addition to overwriting any existing `tokeirad:local` tag only when `--tag local` is explicitly passed; otherwise the `tokeirad:local` tag is left untouched.
4. THE `tkr image build` subcommand SHALL print a progress indicator via the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) progress callback surface for each build stage (toolchain resolution, compile, image assembly, export).
5. WHEN the operator passes `--json`, THE `tkr image build` subcommand SHALL emit JSON progress events following the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) JSON event schema, plus a final `{ "action": "build", "image": "tokeirad:<tag>", "arch": "<arch>" }` summary event.
6. THE `tkr image build` subcommand SHALL NOT require an active deployment (`--deployment`) because the build only uses workspace sources and produces a local image.

### Requirement 4.3: Push Subcommand

**User Story:** As a Tokeira operator, I want `tkr image push` to authenticate with ECR, ensure repositories exist, push `tokeirad` with both `latest` and a version tag, and write the resulting remote refs into the deployment config, so that the next `tkr infra apply` and `tkr deploy apply` can consume them without manual edits.

#### Acceptance Criteria

1. THE `tkr image push` subcommand SHALL accept the following flags: `--tag <value>` (required in the CLI signature; defaults to `latest` only when the operator explicitly omits it, following EKS parity).
2. THE `tkr image push` subcommand SHALL require an active deployment (`--deployment`) because it reads the deployment's `project_name`, `region`, and writes back to `deployment.toml`.
3. THE `tkr image push` subcommand SHALL fail with a clear error message when the local image `tokeirad:latest` is not present in the Docker image store, instructing the operator to run `tkr image build` first.
4. THE `tkr image push` subcommand SHALL call ECR `GetAuthorizationToken`, decode the base64 `user:password` token, and use the decoded credentials for both repository ensure and image publish steps.
5. THE `tkr image push` subcommand SHALL publish two references per push invocation: `{registry}/{project_name}/tokeirad:latest` and `{registry}/{project_name}/tokeirad:{tag}`. The `latest` tag SHALL always be pushed regardless of the `--tag` value.
6. THE `tkr image push` subcommand SHALL perform Image_Writeback: the version-tagged remote ref SHALL be written into every `EcsConfig.services.*.image` field (`edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`) in `deployment.toml`.
7. WHEN the operator passes `--json`, THE `tkr image push` subcommand SHALL emit JSON progress events plus a final `{ "action": "push", "registry": "...", "pushed": ["tokeirad:latest", "tokeirad:<tag>"], "writeback": [...] }` summary event.

### Requirement 4.4: Mirror Subcommand

**User Story:** As a Tokeira operator, I want `tkr image mirror` to copy all pinned third-party images into project-owned ECR repositories and write the mirrored refs back into the deployment config, so that a private-only ECS deployment has every image it needs before `tkr infra apply`.

#### Acceptance Criteria

1. THE `tkr image mirror` subcommand SHALL accept no positional arguments — it always mirrors the complete canonical mapping defined in Requirement 5.1.
2. THE `tkr image mirror` subcommand SHALL require an active deployment.
3. THE `tkr image mirror` subcommand SHALL, for each Mirror_Mapping:
   - ensure the destination Project_Repository exists (Requirement 3.4),
   - invoke `mirror_image` with the source ref and the destination remote ref,
   - collect the result into a writeback list.
4. THE `tkr image mirror` subcommand SHALL perform Image_Writeback: each mirrored remote ref SHALL be written into its mapped config field in `deployment.toml`.
5. THE `tkr image mirror` subcommand SHALL be idempotent. FOR ALL invocations with the same deployment, calling `tkr image mirror` twice in a row SHALL succeed both times and SHALL leave the same set of config keys populated with the same values after the second invocation as after the first. The second invocation SHALL NOT treat already-mirrored images as errors.
6. WHEN the operator passes `--json`, THE `tkr image mirror` subcommand SHALL emit JSON progress events plus a final `{ "action": "mirror", "registry": "...", "mirrored": [...], "writeback": [...] }` summary event.
7. WHEN a source ref in `deployment.toml` already points to the corresponding project-scoped destination (for example, `observability.mimir_image` is already `{registry}/{project_name}/grafana-mimir:3.0.6`), THE `tkr image mirror` subcommand SHALL treat that mapping as already-mirrored and SHALL skip the pull/push for that entry. This prevents operators from pulling their own project's ECR image and re-pushing it. The skip SHALL be reported in the summary.

### Requirement 4.5: Confirmation Prompts

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to respect the same confirmation rules as other mutating commands, so that I cannot accidentally overwrite remote state or configs.

#### Acceptance Criteria

1. THE `tkr image push` and `tkr image mirror` subcommands SHALL follow the [`tkr-cli`](../tkr-cli/requirements.md) confirmation rules: interactive confirmation by default, `--yes` to bypass, refuse to proceed when stdout is non-TTY and `--yes` is not provided.
2. THE `tkr image build` subcommand SHALL NOT require confirmation because it only produces local artifacts and does not mutate remote state.
3. WHEN the operator passes `--yes`, THE subcommand SHALL proceed without prompting.
4. WHEN stdout is non-TTY and `--yes` is not provided, THE subcommand SHALL return an error instructing the operator to re-run with `--yes` in non-interactive contexts.

---

## Feature 5: Canonical Mirror Mapping

### Requirement 5.1: Mirror Mapping Table

**User Story:** As a Tokeira developer, I want a single authoritative list of third-party images to mirror and the config fields they populate, so that adding or removing a mirrored image requires one change in one place.

#### Acceptance Criteria

1. THE Build_Crate SHALL expose a canonical `mirror_mappings(config) -> Vec<MirrorMapping>` function that returns one entry per managed third-party image, given the current deployment config.
2. THE canonical mappings SHALL include at minimum the following six entries:

   | Source ref | Destination suffix | Config field to write back |
   |---|---|---|
   | `grafana/mimir:3.0.6` | `{project}/grafana-mimir:3.0.6` | `observability.mimir_image` |
   | `grafana/loki:3.7.1` | `{project}/grafana-loki:3.7.1` | `observability.loki_image` |
   | `grafana/grafana-oss:12.4.3` | `{project}/grafana-oss:12.4.3` | `observability.grafana_image` |
   | `grafana/alloy:v1.16.0` | `{project}/grafana-alloy:v1.16.0` | `observability.alloy_image` |
   | `public.ecr.aws/aws-cli/aws-cli:latest` | `{project}/aws-cli:latest` | `observability.aws_cli_image` |
   | `public.ecr.aws/docker/library/busybox:latest` | `{project}/busybox:latest` | `observability.busybox_image` |

3. THE destination tag SHALL match the source tag exactly, so operators reading `deployment.toml` can trace any mirrored ref back to its upstream pinned version.
4. WHEN a source field in config is empty, THE `mirror_mappings` function SHALL skip that mapping — the deployment has opted out of that component.
5. WHEN a source field in config already points to the project-scoped destination, THE `mirror_mappings` function SHALL skip that mapping per Requirement 4.4.7.
6. THE `aws_cli_image` and `busybox_image` fields SHALL be new fields introduced to `EcsConfig.observability` by this spec; their semantic role is consumed by task-definition init containers per the [`ecs-deployment`](../ecs-deployment/requirements.md) init-container requirement.

### Requirement 5.2: Version Source of Truth

**User Story:** As a Tokeira operator, I want the pinned version of each third-party image to match the compose platform exactly, so that local compose deployments and ECS deployments run the same binaries.

#### Acceptance Criteria

1. THE pinned versions in Requirement 5.1 SHALL match the defaults in `platforms/compose/src/config.rs` (`ComposeConfig::default()`).
2. WHEN a version in `ComposeConfig::default()` is changed, THE mapping in Requirement 5.1 SHALL be updated to match in the same change set. Unit tests SHALL enforce this equality.
3. THE `busybox` and `aws-cli` versions SHALL be pinned to stable references; using `:latest` is explicitly documented as acceptable for these two images because they are used only as init-container utility images where tag stability matters less than service-image stability.

---

## Feature 6: Config Integration

### Requirement 6.1: Compose Platform Config Alignment

**User Story:** As a Tokeira operator, I want the compose platform's `tokeirad.image` default to remain `tokeirad:local` and to be produced by `tkr image build`, so that no manual intervention is needed to bring up a compose deployment after a fresh clone.

#### Acceptance Criteria

1. THE `ComposeConfig::default()` value for `tokeirad.image` SHALL be `"tokeirad:local"`.
2. THE `tkr image build` subcommand with default flags SHALL produce an image with the tag `tokeirad:local`.
3. THE compose platform SHALL NOT invoke `tkr image build` automatically on `tkr deploy apply`. Operators SHALL run `tkr image build` manually (or via `tkr dev build-image`, should a future spec add that alias). This spec does not introduce an automatic build step on deploy apply.
4. IF `tkr deploy apply` is invoked on a compose deployment and the `tokeirad:local` image is absent from the local Docker image store, THEN THE compose platform SHALL return an error instructing the operator to run `tkr image build` first, including the exact command to run.

### Requirement 6.2: ECS Platform Config Writeback

**User Story:** As a Tokeira operator, I want `tkr image push` and `tkr image mirror` to write discovered refs into `deployment.toml` using the same writeback machinery as infra apply, so that the config file stays a faithful record of what is deployed.

#### Acceptance Criteria

1. THE Image_CLI SHALL use the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) `toml_edit` writeback module for all Image_Writeback operations.
2. THE Image_Writeback SHALL preserve existing TOML comments and formatting in `deployment.toml`.
3. THE Image_Writeback SHALL create intermediate TOML tables when the target dotted key does not yet exist.
4. THE Image_Writeback SHALL overwrite an existing value when the target dotted key is already present.
5. FOR ALL Image_Writeback operations with N key-value pairs, reading each value at its specified path after write SHALL produce the original value (round-trip property, inherited from the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) writeback requirement).
6. WHEN the writeback fails (permission, I/O error, malformed TOML), THE Image_CLI SHALL return an error describing the failure and SHALL NOT claim the push or mirror succeeded. The pushed or mirrored images in ECR SHALL remain in place — writeback failure is reported but not rolled back.

### Requirement 6.3: ECS Config Service Image Fields

**User Story:** As a Tokeira developer, I want the `EcsConfig.services.*.image` fields to be explicitly documented as writeback targets populated by `tkr image push`, so that operators and downstream specs know where the image reference comes from.

#### Acceptance Criteria

1. THE `EcsConfig.services.<service>.image` field for each of the seven services (`edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`) SHALL be populated by Image_Writeback from `tkr image push`.
2. WHEN `EcsConfig.services.<service>.image` is empty at the time `tkr infra apply` or `tkr deploy apply` runs on an ECS deployment, THE ECS platform SHALL return an error instructing the operator to run `tkr image push` first. This matches the existing error pattern for Managed-mode DSQL hydration defined in [`ecs-deployment`](../ecs-deployment/requirements.md) Requirement 7.5a.3.
3. WHEN `EcsConfig.observability.{mimir_image, loki_image, grafana_image, alloy_image, aws_cli_image, busybox_image}` is empty or points to an upstream source (not the project-scoped ECR destination) at the time `tkr infra apply` runs on an ECS deployment, THE ECS platform SHALL return an error instructing the operator to run `tkr image mirror` first.

---

## Feature 7: Lifecycle Ordering

### Requirement 7.1: Mirror Before Infra Apply (ECS Platform)

**User Story:** As a Tokeira operator, I want the CLI to refuse `tkr infra apply` on a private-only ECS deployment until `tkr image mirror` has populated the observability image refs, so that I cannot accidentally deploy task definitions that would fail to pull their images.

#### Acceptance Criteria

1. WHEN the deployment platform is `ecs` AND `tkr infra apply` is invoked, THE CLI SHALL validate that every `EcsConfig.observability.{mimir_image, loki_image, grafana_image, alloy_image, aws_cli_image, busybox_image}` field is non-empty AND points to a ref whose host matches the expected ECR registry for the deployment's account and region.
2. IF any observability image field is empty or points to an upstream source, THEN THE CLI SHALL return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image mirror`.
3. THE validation SHALL occur during `tkr infra apply` only. `tkr infra plan` SHALL warn but SHALL NOT refuse to produce a plan — plans are informational.
4. THE validation SHALL occur for the `ecs` platform only. `local` and `compose` platforms SHALL skip this check.

### Requirement 7.2: Build and Push Before Deploy Apply (ECS Platform)

**User Story:** As a Tokeira operator, I want the CLI to refuse `tkr deploy apply` on an ECS deployment until `tkr image push` has populated the service image refs, so that ECS task definitions cannot reference a missing image.

#### Acceptance Criteria

1. WHEN the deployment platform is `ecs` AND `tkr deploy apply` is invoked, THE CLI SHALL validate that every `EcsConfig.services.<service>.image` field is non-empty AND points to a ref whose host matches the expected ECR registry for the deployment's account and region.
2. IF any service image field is empty or points to an upstream source, THEN THE CLI SHALL return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image push --tag <version>`.
3. THE validation SHALL occur during `tkr deploy apply` only. `tkr deploy plan` SHALL warn but SHALL NOT refuse to produce a plan.
4. THE validation SHALL occur for the `ecs` platform only. `local` and `compose` platforms SHALL skip this check (the local platform runs from a cargo-built binary; the compose platform uses the local image tag).

### Requirement 7.3: Build Before Deploy Apply (Compose Platform)

**User Story:** As a Tokeira developer, I want the CLI to refuse `tkr deploy apply` on a compose deployment until `tokeirad:local` exists in the local Docker image store, so that `docker compose up` does not fail with a pull error on the default registry.

#### Acceptance Criteria

1. WHEN the deployment platform is `compose` AND `tkr deploy apply` is invoked AND `ComposeConfig.tokeirad.image == "tokeirad:local"`, THE compose platform SHALL query the local Docker image store for the presence of `tokeirad:local`.
2. IF `tokeirad:local` is absent from the local store, THEN THE compose platform SHALL return an error instructing the operator to run `tkr image build`, including the exact command.
3. WHEN `ComposeConfig.tokeirad.image` is any value other than `"tokeirad:local"`, THE compose platform SHALL NOT enforce this check. Operators who point at a remote ref take responsibility for pull authentication and availability.

### Requirement 7.4: Image Commands Do Not Require Prior Lifecycle Stages

**User Story:** As a Tokeira operator, I want `tkr image mirror` and `tkr image push` to run on a fresh deployment before any infrastructure has been provisioned, so that images are ready in ECR before `tkr infra apply` references them.

#### Acceptance Criteria

1. THE `tkr image mirror` and `tkr image push` subcommands SHALL NOT require `tkr infra apply` to have run first. They need only an ECR registry reachable from the operator's workstation and valid AWS credentials.
2. WHEN the deployment's `infra state` is empty, THE Image_CLI SHALL derive the ECR registry host as `{account_id}.dkr.ecr.{region}.amazonaws.com` using the account and region from the deployment's config and the operator's AWS credentials.
3. THE Image_CLI SHALL NOT create VPC endpoints, IAM roles, or any other AWS resources beyond ECR repositories. All other infrastructure provisioning belongs to the `tkr infra` command group.

---

## Feature 8: Correctness Properties

### Requirement 8.1: Mirror Idempotence Property

**User Story:** As a Tokeira developer, I want mirror idempotence to be encoded as a testable property, so that regressions that turn mirror into a non-idempotent operation fail CI.

#### Acceptance Criteria

1. FOR ALL valid `(config, mirror_mappings(config))` pairs, running `tkr image mirror` twice in sequence (with mocked ECR and mocked Dagger) SHALL produce:
   - the same set of ensured repositories,
   - the same set of mirrored digests,
   - the same set of writeback key-value pairs in `deployment.toml`.
2. THE test SHALL assert equality of the `deployment.toml` contents before the second invocation and after the second invocation.
3. THE test SHALL mock the Dagger client at the `dagger_client` trait boundary so no real network calls are made.

### Requirement 8.2: Repository Creation Idempotence Property

**User Story:** As a Tokeira developer, I want ECR repository creation idempotence to be encoded as a testable property, so that re-running the repository-ensure step under any ordering produces the same end state.

#### Acceptance Criteria

1. FOR ALL valid `repo_names: Vec<String>` with no duplicates, calling `ensure_ecr_repositories` twice with the same input SHALL leave the AWS-mocked state identical to calling it once.
2. THE property SHALL be tested with at least 64 generated cases via `proptest`, covering empty lists, single-element lists, and multi-element lists (up to 20 names) with random ASCII-printable names passing the ECR name grammar filter.
3. THE test SHALL mock the ECR client at the `aws-sdk-ecr` trait boundary.

### Requirement 8.3: Lifecycle Policy Round-Trip Property

**User Story:** As a Tokeira developer, I want the canonical Lifecycle_Policy JSON to round-trip through parse and serialize without loss, so that we can diff stored policies against the canonical form reliably.

#### Acceptance Criteria

1. FOR the canonical Lifecycle_Policy string, `serde_json::from_str::<LifecyclePolicy>(s)` followed by `serde_json::to_string_pretty(&policy)` followed by re-parse SHALL produce the same `LifecyclePolicy` value.
2. THE test SHALL run as a unit test in `tokeira-aws`.

### Requirement 8.4: Writeback Round-Trip Property

**User Story:** As a Tokeira developer, I want Image_Writeback to round-trip through the TOML writer, so that values written back are exactly what `tkr image push` and `tkr image mirror` intended.

#### Acceptance Criteria

1. FOR ALL generated `(dotted_key, value)` pairs where `dotted_key` is a valid TOML dotted key and `value` is a valid string, writing the pair to an empty or existing `deployment.toml` and then reading the value at `dotted_key` SHALL produce the original `value`.
2. THE property SHALL be tested with at least 64 generated cases via `proptest`.
3. THE property test SHALL be implemented in the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) spec's writeback crate; this spec consumes it and does not duplicate it.

### Requirement 8.5: Mirror Mapping Table Stability Property

**User Story:** As a Tokeira developer, I want the mirror mapping table to remain in sync with the compose platform's default image pins, so that the local and remote image planes use the same versions.

#### Acceptance Criteria

1. FOR each entry in `mirror_mappings(ComposeConfig::default())`, the source ref SHALL equal the corresponding field in `ComposeConfig::default().observability` (or `ComposeConfig::default().init_containers` for `busybox` and `aws-cli` once those fields exist).
2. THE test SHALL be a unit test in the Build_Crate.
3. WHEN the assertion fails, THE test message SHALL state which mapping drifted and what the compose default is.

---

## Feature 9: Error Handling and Operator Guidance

### Requirement 9.1: Actionable Error Messages

**User Story:** As a Tokeira operator, I want every image-plane error to tell me what happened, why, and what to do next, so that I can resolve failures without reading source code.

#### Acceptance Criteria

1. WHEN ECR `GetAuthorizationToken` fails, THE Image_CLI SHALL return an error including: the underlying AWS SDK error, the region the call was made against, and a remediation hint ("verify AWS credentials and that the IAM principal has `ecr:GetAuthorizationToken`").
2. WHEN a Dagger publish call fails with a 401 or 403 from ECR, THE Image_CLI SHALL return an error noting that ECR authentication failed and suggesting the operator re-run after verifying the IAM role and any MFA requirements.
3. WHEN the local Docker image store does not contain the expected local image, THE Image_CLI SHALL return an error naming the missing image and instructing the operator to run `tkr image build`.
4. WHEN the Dagger CLI is missing, THE Image_CLI SHALL return an error naming the expected version range (>= 0.20) and linking to the Dagger install documentation.
5. WHEN `rust-toolchain.toml` cannot be parsed, THE Build_Crate SHALL return an error naming the file and the parse failure location.

### Requirement 9.2: Credential Handling

**User Story:** As a Tokeira operator, I want ECR credentials to be handled through Dagger's secret primitive rather than environment variables or shell history, so that registry passwords never appear in logs or process listings.

#### Acceptance Criteria

1. THE Build_Crate SHALL pass the ECR password to Dagger via the `set_secret` primitive followed by `with_registry_auth`, matching the EKS reference implementation.
2. THE Image_CLI SHALL NOT log the ECR password at any log level. THE Image_CLI MAY log the registry host and username at `tracing::debug`.
3. THE Image_CLI SHALL NOT write the ECR password to `deployment.toml` or any other file. Writeback is limited to image references only.

### Requirement 9.3: Non-Zero Exit Codes

**User Story:** As a Tokeira operator scripting image operations, I want failed commands to exit non-zero so that my scripts can detect failure.

#### Acceptance Criteria

1. WHEN any `tkr image <subcommand>` invocation returns an error, THE Image_CLI SHALL exit with a non-zero status code.
2. WHEN any `tkr image <subcommand>` invocation succeeds (including idempotent no-op cases like "all mirrors already up to date"), THE Image_CLI SHALL exit with status code 0.
3. THE specific non-zero exit code SHALL be 1 for user-facing errors and 2 for usage errors (invalid flags, missing required arguments), matching the clap convention.

---

## Feature 10: Cross-Cutting Requirements

### Requirement 10.1: Tests without Network or Docker

**User Story:** As a Tokeira developer, I want the default test suite to run without Docker, without the Dagger daemon, and without AWS credentials, so that tests run in any contributor environment.

#### Acceptance Criteria

1. THE unit tests for the Build_Crate SHALL NOT require a running Dagger daemon. THE Build_Crate SHALL use dependency inversion (a trait-bounded `DaggerClient`) so tests can supply a mock.
2. THE unit tests for the ECR repository resource SHALL NOT require AWS credentials. THE resource SHALL use dependency inversion (a trait-bounded `EcrClient`) so tests can supply a mock.
3. THE integration tests that require Dagger, Docker, or real AWS credentials SHALL be gated behind a feature flag (`integration-test`) or an `#[ignore]` attribute, matching the AGENTS.md testing guidance for this workspace.

### Requirement 10.2: Documentation

**User Story:** As a Tokeira operator new to the project, I want the `tkr image` command group documented in `README.md` and `AGENTS.md`, so that I can find the expected workflow without reading specs.

#### Acceptance Criteria

1. THE root `README.md` SHALL include a "Building and publishing images" section covering `tkr image build`, `tkr image push`, and `tkr image mirror`.
2. THE root `AGENTS.md` SHALL reference the image lifecycle ordering rules from Feature 7.
3. THE `tkr image <subcommand> --help` output SHALL be sufficient to use the subcommand without reading the spec.

### Requirement 10.3: No Introduction of Tool Sprawl

**User Story:** As a Tokeira maintainer, I want image lifecycle to avoid introducing new build tools beyond Dagger, so that the workspace stays consistent with the "no tool sprawl" principle.

#### Acceptance Criteria

1. THE Build_Crate SHALL NOT depend on a Dockerfile templater, Helm-like tool, or any manifest templating engine. The Dagger pipeline is constructed programmatically in Rust.
2. THE Build_Crate SHALL NOT introduce a new image format or registry protocol. Standard OCI images and the ECR Docker Registry V2 API are the only formats used.
3. THE Build_Crate SHALL NOT introduce any network-facing dependencies that operate outside of Dagger sessions or `aws-sdk-ecr` calls.
