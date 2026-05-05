# Implementation Plan: Image Lifecycle

## Overview

Introduce image-plane capabilities to Tokeira by strengthening the IaC abstractions: a `tokeira-build` library crate exposing the `Image` trait, `ImageSourceType`, `DesiredImageRef`, and `ImageContext`; concrete image modules (`images::tokeira`, `images::observability`); Dagger-backed `build_image` / `mirror_image` / `publish_image` pipelines that take `&dyn Image`; an `EcrRepository` IaC resource; a `tkr image list|build|push|mirror` CLI that iterates the image registry; and platform lifecycle gates driven by the registry rather than by hardcoded field lists.

Target crates:
- `crates/dagger-client/` — NEW in-repo GraphQL client for Dagger sessions (see [`reference/`](reference/))
- `crates/tokeira-build/` — NEW library crate: `Image` trait, `images::tokeira` + `images::observability` modules, Dagger pipelines
- `crates/tokeira-aws/` — NEW `EcrRepository` resource + `EcrClient` trait and default impl over `aws-sdk-ecr`
- `apps/tkr/` — NEW `image` command group (`list`, `build`, `push`, `mirror`), writeback driven by `image.writeback_targets(ctx)`
- `platforms/compose/` — extend `ObservabilityConfig` with `aws_cli_image`, `busybox_image`; add `validate_local_build` gate on `deploy apply`
- `platforms/ecs/` — extend `ObservabilityConfig` with `aws_cli_image`, `busybox_image`; add `validate_mirrors` gate on `infra apply`; add `validate_builds` gate on `deploy apply`

Crucially, this plan does **not** introduce a new IaC module for ECR repositories, a Dockerfile templater, a second TOML-edit code path, or any tool that duplicates an existing workspace concern. Adding a new image in the future requires one struct plus one line in a module's `all()` function — no plumbing changes.

## Tasks

- [ ] 1. Bootstrap `crates/dagger-client/`
  - [ ] 1.1 Port the reference `dagger-client` implementation into the workspace
    - THE complete reference implementation is provided in [`reference/`](reference/) alongside a README covering port mechanics and what to change vs. what to leave untouched
    - Create `crates/dagger-client/Cargo.toml`, `crates/dagger-client/src/lib.rs`, and `crates/dagger-client/tests/quote_tests.rs` by copying `reference/Cargo.toml`, `reference/lib.rs`, and `reference/quote_tests.rs` respectively
    - Add `"crates/dagger-client"` to the workspace `[workspace.members]` list in the root `Cargo.toml`
    - Replace the reference `Cargo.toml` dependency versions with `workspace = true` entries where the workspace already pins the dependency (`serde`, `serde_json`, `base64`, `reqwest`, `proptest`, `eyre`). If a pin is missing from `[workspace.dependencies]`, add it at the version in the reference
    - Update the doc-comment example in `lib.rs` from `dsqld-build` to `tokeira-build`
    - Follow [`reference/README.md`](reference/README.md) for the full list of "do not change" items (query strings, `quote` helper, `container_op!` macro, `export_image` docker-load flow, 600s timeout) and "must change" items (doc examples)
    - _Requirements: 4.2_

  - [ ]* 1.2 Write unit test for session env-var detection
    - Unset `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN`, assert `Client::from_env()` returns an error
    - Set both to dummy values, assert `Client::from_env()` succeeds (without making a request)
    - Test location: `crates/dagger-client/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 4.1_

- [ ] 2. Scaffold `crates/tokeira-build/` with the `Image` trait and associated types
  - [ ] 2.1 Add the crate to the workspace
    - Create `crates/tokeira-build/Cargo.toml` with `thiserror`, `tracing`, `toml`, `serde`, `serde_json`, `eyre`, and path-dep on `crates/dagger-client`
    - Add `"crates/tokeira-build"` to `[workspace.members]` in the root `Cargo.toml`
    - _Requirements: 10.3_

  - [ ] 2.2 Define `ImageSourceType`, `DesiredImageRef`, and `ImageContext`
    - In `crates/tokeira-build/src/image.rs`:
      - `pub enum ImageSourceType { Build, Mirror }` deriving `Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize`
      - `pub struct DesiredImageRef { repository: String, tag: String, upstream_ref: Option<String> }` deriving `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`
      - `pub struct ImageContext` with typed-extension machinery (`set_extension`, `extension`)
    - _Requirements: 1.2, 1.3, 1.4_

  - [ ] 2.3 Define the `Image` trait and `WritebackTarget`
    - In the same file, define `pub trait Image: Debug + Send + Sync` with `name()`, `source_type()`, `desired_ref(ctx)`, and a default-empty `writeback_targets(ctx) -> Vec<WritebackTarget>`
    - Define `pub struct WritebackTarget { field: &'static str }` deriving `Debug, Clone, PartialEq, Eq`
    - _Requirements: 1.1, 2.4_

  - [ ] 2.4 Define `BuildError` and `Arch`
    - In `crates/tokeira-build/src/error.rs`, define `BuildError` with variants: `ToolchainFile`, `ToolchainParse`, `UnsupportedArch`, `DaggerMissing`, `Publish`, `Mirror`, `UpstreamAuth`, `Validation`, `MissingContextExtension`, `RegistryValidation`, `SourceTypeMismatch`
    - In `crates/tokeira-build/src/arch.rs`, define `pub enum Arch { Arm64, Amd64 }` with `rust_target()`, `platform()`, `FromStr`
    - All public types derive `Debug`. Serializable types derive `Serialize, Deserialize`
    - _Requirements: 3.4_

  - [ ]* 2.5 Write property test for `Arch` parsing
    - **Property: Arch Parsing Rejects Unknown Values**
    - **Validates: Requirement 3.4**
    - Generate arbitrary strings via `proptest`. For strings in `{"arm64", "amd64"}`, assert `Arch::from_str(s)` returns `Ok(_)` and round-trips via `as_str()`. For all other strings, assert `Err(BuildError::UnsupportedArch { supplied })` where `supplied == s`
    - Test location: `crates/tokeira-build/src/arch.rs` `#[cfg(test)]` module
    - Minimum 256 iterations

  - [ ]* 2.6 Write unit test for `ImageContext` extensions
    - Register an `EcsConfig` on a fresh context, assert `ctx.extension::<EcsConfig>()` returns `Some(_)`
    - With no extension registered, assert `ctx.extension::<EcsConfig>()` returns `None`
    - Test location: `crates/tokeira-build/src/image.rs` `#[cfg(test)]` module
    - _Requirements: 1.4_

- [ ] 3. Implement the `DaggerClient` trait and its default implementation
  - [ ] 3.1 Define the trait
    - Create `crates/tokeira-build/src/dagger.rs` with `DaggerClient`, `ContainerRef`, `DirectoryRef`, `FileRef`, `SecretRef` traits from the Design doc
    - Use `Box<Self>` builder pattern on `ContainerRef` methods to match the underlying `dagger-client::Container<'_>` owned-by-value API
    - _Requirements: 4.2_

  - [ ] 3.2 Wire the default implementation to `dagger-client::Client`
    - Create `crates/tokeira-build/src/dagger_default.rs` with `DefaultDaggerClient` wrapping `dagger_client::Client`
    - Each trait method delegates to the corresponding method on the in-repo client
    - _Requirements: 4.2_

  - [ ] 3.3 Provide a `MockDaggerClient` in `#[cfg(test)]`
    - Create `crates/tokeira-build/src/testing.rs` with a mock that records call sequences and returns canned `Box<dyn ContainerRef>` / `Box<dyn FileRef>` handles
    - Expose a helper `MockDaggerClient::calls() -> Vec<MockCall>` that tests inspect
    - _Requirements: 10.1_

- [ ] 4. Implement `images::tokeira`
  - [ ] 4.1 `TokeiradImage` struct
    - Create `crates/tokeira-build/src/images/mod.rs` and `crates/tokeira-build/src/images/tokeira.rs`
    - Define `pub struct TokeiradImage` implementing `Image`: `name = "tokeirad"`, `source_type = Build`, `desired_ref` reads `EcsConfig::project_name` and produces `{project}/tokeirad:latest`, `upstream_ref = None`
    - Define `writeback_targets(ctx)` returning the seven `services.{name}.image` targets: `edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`
    - Define `pub fn all() -> Vec<Box<dyn Image>>` returning `vec![Box::new(TokeiradImage)]`
    - _Requirements: 2.1_

  - [ ]* 4.2 Write unit test for `TokeiradImage::desired_ref`
    - Register a canonical `EcsConfig { project_name: "tokeira-test", .. }` on a fresh `ImageContext`
    - Assert `TokeiradImage.desired_ref(&ctx)?` yields `DesiredImageRef { repository: "tokeira-test/tokeirad", tag: "latest", upstream_ref: None }`
    - _Requirements: 2.1.4_

  - [ ]* 4.3 Write unit test for `TokeiradImage::writeback_targets`
    - Assert the returned list has exactly seven entries with the expected dotted keys in canonical order
    - _Requirements: 2.4_

- [ ] 5. Implement `images::observability`
  - [ ] 5.1 Define the `mirror_image!` macro
    - In `crates/tokeira-build/src/images/observability.rs`, define the declarative `mirror_image!` macro with the shape in the Design doc
    - The macro takes `(struct_name, name, repo_suffix, upstream_field, writeback)` and emits a `#[derive(Debug)] struct` plus `impl Image for ...`
    - _Requirements: 2.2_

  - [ ] 5.2 Emit the six Mirror images via the macro
    - `MimirImage { name = "grafana-mimir", repo_suffix = "grafana-mimir", upstream_field = mimir_image, writeback = "observability.mimir_image" }`
    - `LokiImage { name = "grafana-loki", repo_suffix = "grafana-loki", upstream_field = loki_image, writeback = "observability.loki_image" }`
    - `GrafanaImage { name = "grafana-oss", repo_suffix = "grafana-oss", upstream_field = grafana_image, writeback = "observability.grafana_image" }`
    - `AlloyImage { name = "grafana-alloy", repo_suffix = "grafana-alloy", upstream_field = alloy_image, writeback = "observability.alloy_image" }`
    - `AwsCliImage { name = "aws-cli", repo_suffix = "aws-cli", upstream_field = aws_cli_image, writeback = "observability.aws_cli_image" }`
    - `BusyBoxImage { name = "busybox", repo_suffix = "busybox", upstream_field = busybox_image, writeback = "observability.busybox_image" }`
    - Define `pub fn all() -> Vec<Box<dyn Image>>` returning all six in canonical order
    - Re-export the `image_tag` helper
    - _Requirements: 2.2_

  - [ ]* 5.3 Write unit tests for each observability image's `desired_ref`
    - For each of the six, register a canonical `EcsConfig` and assert `desired_ref` returns the expected `repository`, `tag`, and `upstream_ref`
    - Edge cases: upstream with digest, upstream with no tag (falls back to "latest")
    - _Requirements: 2.2.2_

- [ ] 6. Compose the registry and validate for duplicates
  - [ ] 6.1 Implement `images::all(ctx)` and `validate_registry`
    - In `crates/tokeira-build/src/images/mod.rs`, implement `pub fn all(ctx: &ImageContext) -> Result<Vec<Box<dyn Image>>, BuildError>` that concatenates `tokeira::all() + observability::all()` then runs `validate_registry`
    - `validate_registry` walks the list and returns `BuildError::RegistryValidation` on duplicate `name()` or duplicate `(source_type, repository)` pair
    - _Requirements: 2.3_

  - [ ]* 6.2 Write property test for registry validation (Property 1)
    - **Property 1: Registry Validation**
    - **Validates: Requirement 9.1**
    - Via `proptest`, construct synthetic registries with 0–5 images where each image is selected from a pool of stubs with randomised `name()` values
    - Assert `validate_registry` returns `Err(RegistryValidation)` iff the registry has duplicate names or repositories
    - Test location: `crates/tokeira-build/src/images/mod.rs` `#[cfg(test)]` module
    - Minimum 64 iterations

  - [ ]* 6.3 Write property test for source-type / upstream invariant (Property 2)
    - **Property 2: Source-Type / Upstream Invariant**
    - **Validates: Requirement 9.2**
    - For every image in `images::all(ctx)` with a realistic `EcsConfig` registered:
      - If `source_type() == Build` ⇒ `desired_ref(ctx)?.upstream_ref.is_none()`.
      - If `source_type() == Mirror` ⇒ `desired_ref(ctx)?.upstream_ref.is_some()`.
    - Run over 32 generated `EcsConfig` values (varying project names, observability refs)
    - Test location: `crates/tokeira-build/src/images/mod.rs` `#[cfg(test)]` module

  - [ ]* 6.4 Write property test for ECR name grammar (Property 7)
    - **Property 7: ECR Name Grammar**
    - **Validates: Requirement 9.8**
    - For every image in `images::all(ctx)` with generated `EcsConfig` values, assert `desired_ref(ctx)?.repository` satisfies the ECR grammar: 2–256 characters, `[a-z0-9._/-]+`, not starting with `/` or `.`
    - Minimum 128 iterations

  - [ ] 6.5 Checkpoint — the workspace compiles with the Image trait + concrete impls
    - Run `cargo lint` and `cargo check --workspace`; verify `tokeira-build` compiles with no warnings

- [ ] 7. Implement the Dagger-backed pipelines
  - [ ] 7.1 Resolve `rust-toolchain.toml`
    - In `crates/tokeira-build/src/toolchain.rs`, add `fn rust_toolchain_version(workspace_root: &Path) -> Result<String, BuildError>` that reads `rust-toolchain.toml`, parses it via `toml`, extracts the `[toolchain] channel` (or `version`) field
    - Map I/O errors to `BuildError::ToolchainFile`; map parse errors to `BuildError::ToolchainParse`
    - _Requirements: 3.1.4_

  - [ ] 7.2 Implement `build_image`
    - In `crates/tokeira-build/src/pipelines/build.rs`, implement `pub fn build_image(image: &dyn Image, request: &BuildRequest, dagger: &dyn DaggerClient) -> Result<BuildResult, BuildError>`
    - Fail fast with `SourceTypeMismatch` if `image.source_type() != Build`
    - Stage 1: `rust:{toolchain}-alpine` container, install musl-dev/openssl-dev/pkgconfig/protobuf-dev/protoc, copy workspace, `rustup target add <arch-target>`, `cargo build --release --target <arch-target> --bin {image.name()} -p {image.name()}`, extract binary
    - Stage 2: `alpine:3.23` container, install ca-certificates + tzdata, create user/group `{image.name()}` (UID/GID 1000), copy binary to `/usr/local/bin/{image.name()}`, `with_user({image.name()})`, `with_entrypoint(["/usr/local/bin/{image.name()}"])`
    - Stage 3: `export_image(&format!("{}:{}", image.name(), request.tag))`
    - Return `BuildResult { image_name, local_tag, arch, toolchain_version }`
    - _Requirements: 3.1, 3.4, 3.5_

  - [ ]* 7.3 Write unit test for build invocation sequence
    - With `MockDaggerClient`, call `build_image(&TokeiradImage, &request, &mock)`; assert the recorded call sequence includes: `container_from("rust:{toolchain}-alpine")`, `rustup target add aarch64-unknown-linux-musl`, `cargo build --release --target aarch64-unknown-linux-musl --bin tokeirad -p tokeirad`, `container_from("alpine:3.23")`, `with_user("tokeirad")`, `with_entrypoint(["/usr/local/bin/tokeirad"])`, `export_image("tokeirad:local")` in order
    - Also assert source-type-mismatch: `build_image(&MimirImage, ..)` returns `Err(SourceTypeMismatch)`
    - _Requirements: 3.1_

  - [ ] 7.4 Implement `publish_image`
    - In `crates/tokeira-build/src/pipelines/publish.rs`, implement `pub fn publish_image(local_image: &str, remote_refs: &[String], creds: &RegistryCredentials, dagger: &dyn DaggerClient) -> Result<PublishResult, BuildError>`
    - `set_secret`, `container_from(local_image).with_registry_auth`, loop over `remote_refs` calling `publish`, collect into `PublishResult`
    - Reject empty `remote_refs` with `BuildError::Validation`
    - Map publish failures to `BuildError::Publish { remote_ref, source }` naming the failing ref
    - _Requirements: 3.3_

  - [ ] 7.5 Implement `mirror_image`
    - In `crates/tokeira-build/src/pipelines/mirror.rs`, implement `pub fn mirror_image(image: &dyn Image, ctx: &ImageContext, creds: &RegistryCredentials, dagger: &dyn DaggerClient) -> Result<MirroredReference, BuildError>`
    - Fail fast with `SourceTypeMismatch` if `image.source_type() != Mirror`
    - Compute `desired_ref`, extract `upstream_ref`, compute destination ref as `{creds.registry_host}/{desired.repository}:{desired.tag}`
    - Skip-self check: if `upstream_ref == destination_ref` (or starts with the destination prefix), return early with a no-op `MirroredReference`
    - Otherwise: `set_secret`, `container_from(upstream_ref).with_registry_auth`, `publish(destination_ref)`
    - Map failures to `BuildError::Mirror { source_ref, remote_ref, source }`
    - _Requirements: 3.2_

  - [ ]* 7.6 Write property test for publish reference count (Property 10)
    - **Property 10: Publish Reference Count**
    - **Validates: Requirement 3.3.4, 3.3.5**
    - Generate `Vec<String>` of length 1..16 of valid-looking remote refs via `proptest`
    - Assert `publish_image(local, &remote_refs, &creds, &mock).unwrap().published.len() == remote_refs.len()` and `published[i].remote_ref == remote_refs[i]` for all i
    - Test location: `crates/tokeira-build/src/pipelines/publish.rs` `#[cfg(test)]` module
    - Minimum 100 iterations

  - [ ] 7.7 Checkpoint — pipelines compile and mock-backed tests pass
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-build`

- [ ] 8. Extend platform configs with new mirror targets
  - [ ] 8.1 Extend `ComposeConfig::observability` with `aws_cli_image` and `busybox_image`
    - In `platforms/compose/src/config.rs`, add `pub aws_cli_image: String` and `pub busybox_image: String` to `ObservabilityConfig`
    - Add `#[serde(default)]` so existing `deployment.toml` files without these fields still parse
    - Update `ObservabilityConfig::default()` with `aws_cli_image = "public.ecr.aws/aws-cli/aws-cli:latest"` and `busybox_image = "public.ecr.aws/docker/library/busybox:latest"`
    - _Requirements: 2.2, 7.4_

  - [ ] 8.2 Extend `EcsConfig::observability` with the same two fields
    - In `platforms/ecs/src/config.rs`, add `aws_cli_image` and `busybox_image` to its `ObservabilityConfig`. Same defaults as compose
    - Update any prototypical-config generation helpers that produce `deployment.toml` for ECS so both new fields appear with their upstream source defaults
    - _Requirements: 7.4_

  - [ ]* 8.3 Write property test for mirror mapping stability (Property 6)
    - **Property 6: Mirror Mapping Stability**
    - **Validates: Requirement 9.7**
    - No generation — direct assertion
    - Register `EcsConfig` populated from `ComposeConfig::default().observability` fields
    - For each Mirror image in `observability::all()`, assert `desired_ref(&ctx)?.upstream_ref.unwrap()` equals the matching compose default
    - Test location: `crates/tokeira-build/src/images/observability.rs` `#[cfg(test)]` module
    - _Requirements: 9.7_

  - [ ] 8.4 Checkpoint — configs align across platforms
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-build -p tokeira-compose` (unit tests only)

- [ ] 9. Implement `EcrRepository` resource and `EcrClient` trait in `tokeira-aws`
  - [ ] 9.1 Define the `EcrClient` trait
    - In `crates/tokeira-aws/src/clients/ecr.rs`, define the trait with `get_authorization_token`, `describe_repository`, `create_repository`, `delete_repository`, `put_lifecycle_policy`, `get_lifecycle_policy`, `tag_resource`
    - Define `EcrAuthorization`, `RepositoryDescription`, `ImageTagMutability`, `EcrError` with variants including `NotFound` and `InvalidToken`
    - Implement the default over `aws-sdk-ecr` with `#[async_trait]`
    - Add `aws-sdk-ecr` and `base64` to `crates/tokeira-aws/Cargo.toml`
    - _Requirements: 5.1_

  - [ ] 9.2 Implement the ECR authorization decoder
    - In `crates/tokeira-aws/src/clients/ecr.rs`, implement `fn decode_authorization_data(token_b64, proxy_endpoint) -> Result<EcrAuthorization, EcrError>`
    - The decoder validates base64 decoding, UTF-8 decoding, presence of a `:` separator, and trims `http(s)://` and trailing `/` from the proxy endpoint
    - _Requirements: 5.1_

  - [ ]* 9.3 Write unit tests for the authorization decoder
    - Four tests covering the four failure modes: success, invalid base64, invalid UTF-8, missing `:`
    - Each test constructs a canned `(token_b64, proxy_endpoint)` input and asserts the exact error variant on failure or the exact `EcrAuthorization` on success
    - Test location: `crates/tokeira-aws/src/clients/ecr.rs` `#[cfg(test)]` module

  - [ ] 9.4 Implement `EcrRepository` resource
    - In `crates/tokeira-aws/src/resources/ecr_repository.rs`, define `EcrRepository { name, tags }` with `#[derive(Debug, Clone, Serialize, Deserialize)]`
    - Define `ECR_LIFECYCLE_POLICY` constant as the canonical JSON from the Design doc
    - Implement `Resource` trait: `create` (create repo with `MUTABLE` + apply lifecycle policy), `update` (re-apply lifecycle policy + tags), `delete` (force-delete), `describe` (return `None` on `NotFound`), `diff` (policy drift and tag drift signal updates), `dependencies` (empty)
    - Implement a constructor `EcrRepository::new(name: &str, tags: BTreeMap<String, String>) -> Result<Self, EcrError>` that validates the name against the ECR grammar (2–256 chars, `[a-z0-9._/-]+`, not starting/ending with `/` or `.`)
    - _Requirements: 5.1, 5.2_

  - [ ]* 9.5 Write unit tests for `EcrRepository` resource methods
    - Construct a `MockEcrClient` that records calls and serves canned responses
    - Unit-test `create`, `update`, `delete`, `describe`, `diff` each with a focused scenario
    - Test location: `crates/tokeira-aws/src/resources/ecr_repository.rs` `#[cfg(test)]` module

  - [ ]* 9.6 Write property test for lifecycle policy JSON round-trip (Property 5)
    - **Property 5: Lifecycle Policy JSON Round-Trip**
    - **Validates: Requirement 9.5**
    - Parse `ECR_LIFECYCLE_POLICY` with `serde_json::from_str::<serde_json::Value>`, serialize with `to_string`, re-parse
    - Assert the two parsed `Value`s are equal
    - Test location: `crates/tokeira-aws/src/resources/ecr_repository.rs` `#[cfg(test)]` module

  - [ ] 9.7 Implement `ensure_ecr_repository` and `ensure_ecr_repositories`
    - In `crates/tokeira-aws/src/clients/ecr.rs`, add `async fn ensure_ecr_repository(ecr: &dyn EcrClient, name: &str, tags: &BTreeMap<String, String>) -> Result<(), EcrError>` that describes first, creates if absent, then always applies the lifecycle policy
    - Add `async fn ensure_ecr_repositories(ecr: &dyn EcrClient, repos: &[(String, BTreeMap<String, String>)]) -> Result<(), EcrError>` that calls the single-repo helper in sequence
    - _Requirements: 5.4_

  - [ ]* 9.8 Write property test for repository creation idempotence (Property 4)
    - **Property 4: ECR Repository Creation Idempotence**
    - **Validates: Requirement 9.4**
    - Generate `Vec<(String, BTreeMap<String, String>)>` of length 0..20 with distinct grammar-valid names
    - Call `ensure_ecr_repositories` twice with the same input against a shared `MockEcrClient`
    - Assert the mock's repository set after the second call equals the set after the first, same policies, same tags
    - Test location: `crates/tokeira-aws/src/clients/ecr.rs` `#[cfg(test)]` module
    - Minimum 64 iterations

  - [ ] 9.9 Checkpoint — `tokeira-aws` compiles with ECR additions
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-aws`

- [ ] 10. Wire the `tkr image` command group
  - [ ] 10.1 Add `ImageCommand` enum to `apps/tkr/src/cli.rs`
    - Add `Image(ImageArgs)` variant to the top-level `Command` enum, positioned between `Deployment` and `Infra`
    - Define `ImageArgs` with a subcommand field bound to `ImageCommand { List { source_type }, Build { arch, tag, image }, Push { tag, image, yes }, Mirror { image, yes } }`
    - _Requirements: 6.1_

  - [ ] 10.2 Implement the `list` handler
    - Create `apps/tkr/src/commands/image.rs`. Implement `run(cmd, deployment, format)` from the Design doc
    - For `List { source_type }`: build `ImageContext` with the active deployment's config, call `tokeira_build::images::all(&ctx)`, filter by `source_type`, render table (human) or JSON array
    - Require an active deployment (ImageContext needs it)
    - _Requirements: 6.2_

  - [ ] 10.3 Implement the `build` handler
    - For `Build { arch, tag, image }`: parse `arch`, resolve workspace root, construct `ImageContext`, filter `images::all` to Build images (and to `--image name` if set), re-exec under `dagger run` if session env vars absent, then iterate selected images calling `tokeira_build::build_image`
    - Report progress via the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) callback surface per stage per image
    - When `--json` is active, emit progress events plus a final `{ "action": "build", "images": [...] }` summary
    - Build does NOT require `--yes`; it does NOT require an active deployment for sources-only `TokeiradImage`, but per Req 6.2.4 `list` does. `build` requires `--deployment` because `desired_ref` needs the project name for repo derivation
    - _Requirements: 6.3, 4.1_

  - [ ] 10.4 Implement the `push` handler
    - For `Push { tag, image, yes }`: confirm per `tkr-cli`, obtain `EcrClient` + credentials, build `ImageContext`, filter `images::all` to Build images
    - For each selected image: resolve `desired_ref`, verify local image exists in Docker, ensure ECR repo, publish two remote refs (`:latest` and `:{tag}`)
    - After all pushes: iterate each image's `writeback_targets(&ctx)` and call the `iac_lifecycle::write_config_values` helper with the version-tagged ref
    - Emit progress events and `--json` summary
    - _Requirements: 6.4, 6.6_

  - [ ] 10.5 Implement the `mirror` handler
    - For `Mirror { image, yes }`: confirm per `tkr-cli`, obtain `EcrClient` + credentials, build `ImageContext`, filter `images::all` to Mirror images
    - For each selected image: resolve `desired_ref`, ensure ECR repo, call `tokeira_build::mirror_image` (which handles skip-self internally)
    - After all mirrors: iterate each image's `writeback_targets(&ctx)` and write the destination ref back to `deployment.toml`
    - Emit progress events and `--json` summary
    - _Requirements: 6.5, 6.6_

  - [ ] 10.6 Wire the `image` command into `apps/tkr/src/main.rs`
    - Add a `Command::Image(args) => commands::image::run(args.command, &deployment, format).await?` arm
    - Thread the global `--json` flag
    - _Requirements: 6.1.2_

  - [ ]* 10.7 Write unit tests for CLI parse
    - `tkr image list`: default; `--source-type build`; `--source-type mirror`; `--json`
    - `tkr image build`: defaults (`arch=arm64`, `tag=local`); `--arch amd64 --tag v1.2.3`; `--image tokeirad`
    - `tkr image push`: default (`tag=latest`); `--tag v2026-03-21 --yes`; `--image tokeirad`
    - `tkr image mirror`: default; `--image grafana-mimir`; `--yes`
    - Test location: `apps/tkr/src/commands/image.rs` `#[cfg(test)]` module
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [ ]* 10.8 Write property test for mirror idempotence (Property 3)
    - **Property 3: Mirror Idempotence**
    - **Validates: Requirement 9.3**
    - Generate `EcsConfig` values with populated observability fields (tag generation bounded to a small set)
    - Call `run_mirror(cfg)` twice in sequence with a shared `MockDaggerClient` + `MockEcrClient`
    - Assert: both calls succeed; mock repo set after second call equals after first; `deployment.toml` contents unchanged between the two calls
    - Test location: `apps/tkr/src/commands/image.rs` `#[cfg(test)]` module
    - Minimum 32 iterations (I/O cost; tempdir per iteration)

  - [ ] 10.9 Checkpoint — CLI and pipelines tie together
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tkr -p tokeira-build -p tokeira-aws`

- [ ] 11. Add lifecycle gates driven by the image registry
  - [ ] 11.1 Implement ECS `validate_mirrors`
    - In `platforms/ecs/src/gates.rs`, add `pub fn validate_mirrors(cfg: &EcsConfig, registry: &str, images: &[Box<dyn Image>], ctx: &ImageContext) -> Result<(), EcsError>`
    - Iterate images filtered to `ImageSourceType::Mirror`; for each, iterate `writeback_targets(ctx)` and read the dotted key from `cfg`; collect any empty / non-`{registry}/`-prefixed fields
    - Return an error listing the unmirrored fields and the `tkr image mirror` remediation
    - Call this validator from `EcsDeployment::validate_for_apply` (invoked before `tkr infra apply`)
    - _Requirements: 8.1, 9.9_

  - [ ]* 11.2 Write property test for `validate_mirrors` (Property 8 Mirror)
    - **Property 8 (Mirror side): Lifecycle Gate Predicate**
    - **Validates: Requirement 9.9**
    - Generate `EcsConfig` values with observability fields chosen from: empty, upstream source, project-scoped ECR ref
    - Assert `validate_mirrors(cfg, registry, &observability::all(), &ctx)` returns `Err` iff at least one targeted field is empty or not `{registry}/`-prefixed
    - Test location: `platforms/ecs/src/gates.rs` `#[cfg(test)]` module
    - Minimum 128 iterations

  - [ ] 11.3 Implement ECS `validate_builds`
    - Symmetric to `validate_mirrors` but filters images to `ImageSourceType::Build` and recommends `tkr image push --tag <version>` in the remediation message
    - Call this validator from `EcsDeployment::validate_for_deploy_apply` (invoked before `tkr deploy apply`)
    - _Requirements: 8.2, 9.9_

  - [ ]* 11.4 Write property test for `validate_builds` (Property 8 Build)
    - Same shape as 11.2, filtering to Build images (today: just `TokeiradImage`)
    - Minimum 128 iterations

  - [ ] 11.5 Implement the compose `validate_local_build` gate
    - In `platforms/compose/src/gates.rs`, add `pub async fn validate_local_build(cfg: &ComposeConfig, docker: &bollard::Docker) -> Result<(), ComposeError>`
    - When `cfg.tokeirad.image == "tokeirad:local"`, query bollard for image existence; return an error with the `tkr image build` remediation if absent
    - When `cfg.tokeirad.image` is any other value, skip the check
    - _Requirements: 8.3_

  - [ ]* 11.6 Write unit tests for the compose build gate
    - With a fake bollard client returning `NotFound`, assert the gate returns an error with "tkr image build" in the message
    - With a fake client returning `Ok`, assert the gate returns `Ok(())`
    - With `cfg.tokeirad.image = "my-registry.example/tokeirad:custom"`, assert the gate returns `Ok(())` without querying bollard
    - Test location: `platforms/compose/src/gates.rs` `#[cfg(test)]` module

  - [ ] 11.7 Checkpoint — gates pass property tests
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p platforms-ecs -p platforms-compose`

- [ ] 12. Integration and documentation
  - [ ] 12.1 Update `README.md`
    - Add a "Building and publishing images" section covering:
      - `tkr image list` — enumerate every image the deployment knows about
      - `tkr image build` — default produces `tokeirad:local` for compose; `--arch amd64 --tag v1.2.3` for explicit
      - `tkr image push --tag <version>` — pushes Build images with latest + version, writes back to `services.*.image`
      - `tkr image mirror` — mirrors every Mirror image into project-owned ECR, writes back to `observability.*_image`
      - Required prerequisites: Dagger >= 0.20 for `build`/`push`/`mirror`; AWS credentials with `ecr:*` permissions for `push` and `mirror`
    - Add a "Lifecycle order" subsection: `mirror` before `infra apply`, `build` + `push` before `deploy apply` (ECS), `build` before `deploy apply` (compose)
    - Add a short "Adding a new image" section pointing at `images::tokeira` / `images::observability` and the `Image` trait
    - _Requirements: 10.2_

  - [ ] 12.2 Update `AGENTS.md`
    - Add the lifecycle ordering rules to the "Working Agreements" section
    - Add an "Adding a new image" checklist:
      1. Write a struct implementing `Image` (see `images::tokeira::TokeiradImage` or `images::observability::MimirImage` for templates)
      2. Add the struct to the owning module's `all()` function
      3. Add property-test coverage if the image has non-trivial `desired_ref` or `writeback_targets` logic
    - Pointer from the "Adding a new service" checklist to the image-lifecycle spec
    - _Requirements: 10.2_

  - [ ] 12.3 Update `tkr deployment create --platform ecs` prototypical config
    - In `platforms/ecs/src/config.rs` prototypical config generation, populate `observability.aws_cli_image` and `observability.busybox_image` with their upstream source defaults so `tkr image mirror` has something to mirror on first run
    - Ensure the generated `deployment.toml` carries helpful comments (e.g., `# populated by \`tkr image mirror\``)
    - _Requirements: 7.4_

  - [ ]* 12.4 Integration test: build the tokeirad image end-to-end
    - Gated behind the `integration-test` feature flag
    - Run `tkr image build` against the workspace, assert `docker image inspect tokeirad:local` succeeds
    - Test location: `apps/tkr/tests/image_build.rs`
    - Documented as skipped in the default test suite per AGENTS.md testing guidance

  - [ ]* 12.5 Integration test: mirror canonical images into LocalStack ECR
    - Gated behind the `integration-test` feature flag
    - Start LocalStack with ECR service enabled
    - Run `tkr image mirror` against a test deployment pointing at LocalStack
    - Assert all six expected repositories exist, each with the canonical lifecycle policy
    - Re-run `tkr image mirror` and assert the repository set and `deployment.toml` contents are unchanged
    - Test location: `apps/tkr/tests/image_mirror.rs`

  - [ ] 12.6 Final checkpoint — full workspace verification
    - Run `cargo +nightly fmt --all --check`
    - Run `cargo lint`
    - Run `cargo test-lint`
    - Run `cargo check --workspace`
    - Run `cargo test --workspace`
    - Run `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`
    - All commands must pass with zero warnings
