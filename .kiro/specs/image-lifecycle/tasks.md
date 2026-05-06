# Implementation Plan: Image Lifecycle

## Overview

Implement the image plane of the Tokeira deployment lifecycle by splitting responsibility across three clearly-bounded crates: the `Image` trait and associated types in `tokeira-deploy-engine`, concrete per-platform image modules in `platforms/compose/src/images/` and `platforms/ecs/src/images/`, and a Dagger-backed pipeline library in `tokeira-build` exposing `build_tokeirad_image`, `publish_image`, and `mirror_image` as free functions. Alongside, add an `EcrRepository` IaC resource in `tokeira-aws`, an `ImagesModule` IaC module in `platforms/ecs`, a `tkr image list|build|push|mirror` command group in `apps/tkr`, and platform lifecycle gates driven by each platform's image set. Extract the existing private writeback helper into `tokeira_iac::write_config_values` so `tkr infra` and `tkr image` share one implementation.

Target crates:
- `crates/dagger-client/` — new: minimal GraphQL wrapper over a Dagger session
- `crates/tokeira-deploy-engine/` — widen `Image::desired_ref` return; add `writeback_targets` default method; add `WritebackTarget`; add `validate_registry`
- `crates/tokeira-build/` — new: `DaggerClient` trait + default impl, `build_tokeirad_image`, `publish_image`, `mirror_image` as free functions with hardcoded recipes
- `crates/tokeira-aws/` — new: `EcrRepository` resource, `EcrClient` trait + default impl, `EcrClientHandle` extension, ad-hoc ensure helpers
- `crates/tokeira-iac/` — extract `write_config_values` + `WritebackError` from the private helper currently in `apps/tkr/src/commands/infra.rs`
- `platforms/compose/` — new `images/` submodule; extend `ObservabilityConfig` with `aws_cli_image` and `busybox_image`; flip `ComposeConfig::default().tokeirad.image` to `"tokeirad:latest"`; add `gates::validate_local_build` + `DockerImageInspector` trait + `ComposePlatform::validate_for_deploy_apply` hook
- `platforms/ecs/` — new `images/` submodule; extend `ObservabilityConfig` with the same two fields; add `ImagesModule` in `modules/images.rs`; add `gates::validate_mirrors` / `validate_builds`; register `EcrClientHandle` on `ProvisionContext`
- `apps/tkr/` — new `commands/image.rs` with four subcommands; migrate `commands/infra.rs` to the public writeback helper

Each platform carries its own copy of the `mirror_image!` macro and its own concrete image impls. There is no cross-platform import, no shared image-config DTO, and no central registry in `tokeira-build`.

## Tasks

- [x] 1. Bootstrap `crates/dagger-client/`
  - [x] 1.1 Port the reference `dagger-client` implementation into the workspace
    - THE complete reference implementation is provided in [`reference/`](reference/) alongside a README covering port mechanics and what to change vs. what to leave untouched
    - Create `crates/dagger-client/Cargo.toml`, `crates/dagger-client/src/lib.rs`, and `crates/dagger-client/tests/quote_tests.rs` by copying `reference/Cargo.toml`, `reference/lib.rs`, and `reference/quote_tests.rs` respectively
    - Add `"crates/dagger-client"` to the workspace `[workspace.members]` list in the root `Cargo.toml`
    - Replace the reference `Cargo.toml` dependency versions with `workspace = true` entries where the workspace already pins the dependency (`serde`, `serde_json`, `base64`, `reqwest`, `proptest`, `eyre`). If a pin is missing from `[workspace.dependencies]`, add it at the version specified in the reference
    - Update the one doc-comment example in `lib.rs` that names the legacy build crate (`dsqld-build`) to reference `tokeira-build` instead. This is the only reference-folder-to-spec rename; every other port mechanic is documented in [`reference/README.md`](reference/README.md)
    - Follow [`reference/README.md`](reference/README.md) for the full list of "do not change" items (query strings, `quote` helper, `container_op!` macro, `export_image` docker-load flow, 600s timeout)
    - _Requirements: 4.2_

  - [x]* 1.2 Write unit test for session env-var detection
    - Unset `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN`, assert `Client::from_env()` returns an error
    - Set both to dummy values, assert `Client::from_env()` succeeds (without making a request)
    - Test location: `crates/dagger-client/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 4.1_

- [ ] 2. Extend `tokeira_deploy_engine::image` and the deploy-engine wiring
  - [ ] 2.1 Widen `Image::desired_ref` return type
    - In `crates/tokeira-deploy-engine/src/image.rs`, change the trait method signature from `fn desired_ref(&self, ctx: &ImageContext) -> Result<String, RuntimeError>` to `fn desired_ref(&self, ctx: &ImageContext) -> Result<DesiredImageRef, RuntimeError>`
    - `DesiredImageRef` (already declared with `repository`, `tag`, `upstream_ref`) stays as-is
    - Audit the workspace for call sites of the old signature and migrate them to the new shape. Expected audit scope at this point in the spec: zero external implementors
    - _Requirements: 1.1_

  - [ ] 2.2 Add `writeback_targets` default-empty trait method and `WritebackTarget` type
    - In `crates/tokeira-deploy-engine/src/image.rs`, define `pub struct WritebackTarget { pub field: &'static str }` deriving `Debug, Clone, PartialEq, Eq`
    - Add a default-empty `fn writeback_targets(&self, _ctx: &ImageContext) -> Vec<WritebackTarget> { Vec::new() }` method on the `Image` trait
    - Re-export `WritebackTarget` from the `image` module
    - _Requirements: 1.2_

  - [ ] 2.3 Add the `validate_registry` helper
    - In `crates/tokeira-deploy-engine/src/image.rs`, add `pub fn validate_registry(images: &[Box<dyn Image>], ctx: &ImageContext) -> Result<(), RuntimeError>`
    - The implementation walks the list, inserts each `name()` into a `HashSet` (returning `RuntimeError::Image(format!("image registry validation failed: duplicate name = {name}"))` on re-insert), then resolves each `desired_ref(ctx)` and inserts `desired.repository` into a second `HashSet` — keyed by repository ALONE, not `(source_type, repository)`. A `Build` image and a `Mirror` image resolving to the same `repository` MUST be rejected (the downstream `ImagesModule` would otherwise create two `EcrRepository` resources pointing at the same AWS repo)
    - Duplicate-repository error: `RuntimeError::Image(format!("image registry validation failed: duplicate repository = {repo}"))`
    - _Requirements: 2.3_

  - [ ] 2.4 Update `ServiceEngine::record_images` to consume the structured `DesiredImageRef`
    - In `crates/tokeira-deploy-engine/src/engine.rs`, update `record_images` to compute `resolved_ref = format!("{}:{}", desired.repository, desired.tag)` from the widened return
    - Map `ImageSourceType` onto the existing `tokeira_iac::ImageSource` variants:
      - `Build` ⇒ `ImageSource::Built`
      - `Mirror` ⇒ `ImageSource::Mirrored { upstream_ref: desired.upstream_ref.ok_or_else(|| RuntimeError::Image(format!("image '{}' is Mirror but desired_ref.upstream_ref is None", image.name())))? }`
      - `Registry` ⇒ `ImageSource::PullThrough { upstream_ref: desired.upstream_ref.unwrap_or_default() }`
    - Do NOT remove any fields from `ImageState` or `ImageSource`. Persisted state format is append-only
    - _Requirements: 1.5, 1.6_

  - [ ] 2.5 Add `register_image_extensions` hook to the `Deployment` trait
    - In `crates/tokeira-orchestrator/src/lib.rs`, add `async fn register_image_extensions(&self, _config: &Self::Config, _ctx: &mut deploy_engine::ImageContext) -> Result<()> { Ok(()) }` to the `Deployment` trait (default empty)
    - Update `DeployEngine::new` to construct `image_ctx` via `let mut image_ctx = deploy_engine::ImageContext::default(); deployment.register_image_extensions(config, &mut image_ctx).await?;` — AFTER `register_deploy_extensions` and BEFORE storing the context on the facade
    - Update the existing in-crate test `Deployment` impls in `crates/tokeira-orchestrator/src/lib.rs` `#[cfg(test)] mod tests` if any rely on the old construction order (expected: none — the hook is default-empty)
    - _Requirements: 1.4_

  - [ ] 2.6 Implement `register_image_extensions` on `LocalDeployment`
    - In `platforms/local/src/lib.rs`, leave the default empty implementation inherited from the trait. The local platform declares no images, so the default is correct. Add a comment referencing Req 1.4.5 so future maintainers know the emptiness is deliberate
    - _Requirements: 1.4.5_

  - [ ] 2.7 Implement `register_image_extensions` on `ComposeDeployment`
    - In `platforms/compose/src/lib.rs`, implement `async fn register_image_extensions(&self, config: &Self::Config, ctx: &mut deploy_engine::ImageContext) -> Result<()> { ctx.set_extension(config.clone()); Ok(()) }`
    - _Requirements: 1.4.3_

  - [ ]* 2.8 Write unit test for `ImageContext` extensions, `validate_registry`, and `record_images` mapping
    - Register a dummy config type on a fresh context, assert `ctx.extension::<T>()` returns `Some(_)`. With no extension registered, assert it returns `None`
    - Construct two image lists: one with no duplicates (assert `validate_registry` returns `Ok(())`), one with a duplicate name (assert it returns the exact `RuntimeError::Image` variant)
    - Construct three fake `Image` impls — one Build, one Mirror (with an upstream), one Registry (with/without an upstream). Pass them to `record_images`. Assert the resulting `ImageState` entries have the expected `resolved_ref` format (`repository:tag`) and the expected `ImageSource` variant. Assert the Mirror-without-upstream case returns the documented `RuntimeError::Image`
    - Test location: `crates/tokeira-deploy-engine/src/image.rs` + `crates/tokeira-deploy-engine/src/engine.rs` `#[cfg(test)]` modules

  - [ ] 2.9 Checkpoint — deploy-engine compiles with the widened trait and new state mapping
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-deploy-engine -p tokeira-orchestrator -p tokeira-local-deployment -p tokeira-compose-deployment`

- [ ] 3. Scaffold `crates/tokeira-build/` with free-function pipelines
  - [ ] 3.1 Add the crate to the workspace
    - Create `crates/tokeira-build/Cargo.toml` with dependencies on `thiserror`, `tracing`, `toml`, `serde`, `serde_json`, `eyre`, and a path-dep on `crates/dagger-client`
    - Do NOT add a dependency on any platform crate. Do NOT add a dependency on `tokeira-deploy-engine`
    - Add `"crates/tokeira-build"` to `[workspace.members]` in the root `Cargo.toml`
    - _Requirements: 3.1_

  - [ ] 3.2 Define `BuildError` and `Arch`
    - In `crates/tokeira-build/src/error.rs`, define `pub enum BuildError` with variants: `ToolchainFile`, `ToolchainParse`, `UnsupportedArch`, `DaggerMissing`, `Publish`, `Mirror`, `UpstreamAuth`, `Validation`
    - In `crates/tokeira-build/src/arch.rs`, define `pub enum Arch { Arm64, Amd64 }` with methods `rust_target() -> &'static str` (`aarch64-unknown-linux-gnu` / `x86_64-unknown-linux-gnu` — glibc gnu targets, NOT musl; see Req 3.2.3 for rationale), `platform() -> &'static str` (`linux/arm64` / `linux/amd64`), and `FromStr` that maps unknown strings to `BuildError::UnsupportedArch`
    - All public types derive `Debug`. Serializable types derive `Serialize, Deserialize`
    - _Requirements: 3.1, 3.3_

  - [ ]* 3.3 Write property test for `Arch` parsing
    - **Property: Arch Parsing Rejects Unknown Values**
    - **Validates: Requirement 3.3**
    - Generate arbitrary strings via `proptest`. For strings in `{"arm64", "amd64"}`, assert `Arch::from_str(s)` returns `Ok(_)` and round-trips via `as_str()`. For all other strings, assert `Err(BuildError::UnsupportedArch { supplied })` where `supplied == s`
    - Test location: `crates/tokeira-build/src/arch.rs` `#[cfg(test)]` module
    - Minimum 256 iterations

  - [ ] 3.4 Define the `DaggerClient` trait and helper traits
    - Create `crates/tokeira-build/src/dagger.rs` with `DaggerClient`, `ContainerRef`, `DirectoryRef`, `FileRef`, `SecretRef` traits as in the Design doc
    - Use the `Box<Self>` builder pattern on `ContainerRef` methods to match `dagger_client::Container<'_>`'s owned-by-value API
    - _Requirements: 4.2_

  - [ ] 3.5 Implement the default `DaggerClient` over `dagger_client::Client`
    - Create `crates/tokeira-build/src/dagger_default.rs` with `pub struct DefaultDaggerClient { client: dagger_client::Client }`
    - Add `DefaultDaggerClient::from_env()` that calls `dagger_client::Client::from_env()` and wraps the result
    - Implement every trait method by delegating to the corresponding method on the inner client
    - _Requirements: 4.2_

  - [ ] 3.6 Provide `MockDaggerClient` under `#[cfg(test)]`
    - Create `crates/tokeira-build/src/testing.rs` with a mock that records call sequences and returns canned `Box<dyn ContainerRef>` / `Box<dyn FileRef>` / `Box<dyn SecretRef>` handles
    - Expose `MockDaggerClient::calls() -> Vec<MockCall>` for tests to inspect

  - [ ] 3.7 Implement `rust-toolchain.toml` resolution
    - In `crates/tokeira-build/src/toolchain.rs`, add `pub fn rust_toolchain_version(workspace_root: &Path) -> Result<String, BuildError>`
    - Read the file at `workspace_root/rust-toolchain.toml`, parse via `toml`, extract `[toolchain] channel` (fallback to `version`)
    - Map I/O errors to `BuildError::ToolchainFile`; map parse errors to `BuildError::ToolchainParse`
    - _Requirements: 3.2_

  - [ ] 3.8 Implement `build_tokeirad_image` using the 2026 Rust-server pattern
    - Create `crates/tokeira-build/src/pipelines/build.rs`
    - Define `pub struct TokeiradBuildRequest { arch: Arch, tag: Option<String>, workspace_root: PathBuf }` deriving `#[derive(Debug, Clone)]`, and `pub struct TokeiradBuildResult { image_name: String, tags: Vec<String>, arch: Arch, toolchain_version: String }` deriving `#[derive(Debug, Clone)]`
    - Define `pub fn build_tokeirad_image(request: &TokeiradBuildRequest, dagger: &dyn DaggerClient) -> Result<TokeiradBuildResult, BuildError>`
    - Recipe is hardcoded. Reference the design.md §4.3 code block for the exact container operations. Summary:
      - **Chef base** (`rust:{toolchain}-slim-bookworm`): Debian slim with `pkg-config`, `libssl-dev`, `protobuf-compiler`, `ca-certificates` installed via `apt-get`. Then `cargo install cargo-chef --locked`. Then `rustup target add <arch-target>`. glibc gnu target, NOT musl
      - **Planner stage**: copy workspace, run `cargo chef prepare --recipe-path recipe.json`
      - **Cacher stage**: copy `recipe.json` onto the chef base, run `cargo chef cook --release --target <arch> --bin tokeirad --recipe-path recipe.json`. This produces the dependency-cache layer
      - **Builder stage**: copy full workspace on top of the cacher, run `cargo build --release --target <arch> --bin tokeirad -p tokeirad`, then `strip /app/target/<arch>/release/tokeirad`, extract binary
      - **Runtime** (`cgr.dev/chainguard/glibc-dynamic:latest`): copy binary to `/usr/local/bin/tokeirad`, `with_user("nonroot")` (UID 65532 provided by the base image), `with_entrypoint(["/usr/local/bin/tokeirad"])`. NO user creation, NO apk/apt-get. CA certificates and tzdata are provided by the chainguard base
    - Always export `tokeirad:latest`. When `request.tag` is `Some(t)` with `t != "latest"`, additionally export `tokeirad:{t}` from the same container handle
    - _Requirements: 3.2, 3.3, 3.3a_

  - [ ] 3.8a Configure the release profile in the root `Cargo.toml`
    - Add (or update) `[profile.release]` in the root workspace `Cargo.toml`:
      ```toml
      [profile.release]
      lto = "fat"
      codegen-units = 1
      strip = "symbols"
      panic = "abort"
      ```
    - Rationale is captured in Req 3.2.5: LTO + single codegen unit for throughput, strip + panic=abort for binary size and cold start. `panic = "abort"` is safe because tokeirad always restarts on panic under compose/ECS
    - Audit existing tests for any that rely on catching panics via `std::panic::catch_unwind` — `panic = "abort"` makes that non-functional in release builds. If found, either gate the tests behind `cfg(debug_assertions)` or restructure them to use `Result` instead of panics for the error condition
    - _Requirements: 3.2.5_

  - [ ] 3.8b Register `mimalloc` as the global allocator in the `tokeirad` binary
    - Add `mimalloc = { version = "...", default-features = false }` to the workspace `[workspace.dependencies]` section. Pin to the latest stable (check https://crates.io/crates/mimalloc at implementation time)
    - Add `mimalloc.workspace = true` to `apps/tokeirad/Cargo.toml` `[dependencies]`
    - In `apps/tokeirad/src/main.rs`, at the top of the file (after module-level docs, before `use` statements), add:
      ```rust
      #[global_allocator]
      static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
      ```
    - `default-features = false` excludes the secure-heap feature, which adds overhead we don't need for a server that runs in a controlled environment
    - _Requirements: 3.3a_

  - [ ] 3.8c Revise the existing implementation in `crates/tokeira-build/`
    - The existing implementation (committed at Phase 3 partial) uses the alpine+musl pattern. The updated spec (Req 3.2, 3.3) mandates the 2026 glibc+chainguard+cargo-chef pattern
    - Update `crates/tokeira-build/src/arch.rs` to return `aarch64-unknown-linux-gnu` / `x86_64-unknown-linux-gnu` from `rust_target()` — NOT the musl variants
    - Update `crates/tokeira-build/src/pipelines/build.rs` to use the chef-based pipeline from task 3.8. Remove the existing alpine base, apk installations, and manual user creation
    - The existing proptest for `Arch::from_str` continues to pass (it tests the enum's string mapping, not the target triples returned by `rust_target()`), but add a new assertion that `Arch::Arm64.rust_target() == "aarch64-unknown-linux-gnu"` and the same for amd64
    - _Requirements: 3.2, 3.3_

  - [ ]* 3.9 Write unit test for `build_tokeirad_image` invocation sequence
    - With `MockDaggerClient`, call `build_tokeirad_image(&request_arm64_no_tag, &mock)`. Assert the recorded call sequence includes, in order: `container_from("rust:{toolchain}-slim-bookworm")`, `apt-get install` for `pkg-config libssl-dev protobuf-compiler ca-certificates`, `cargo install cargo-chef --locked`, `rustup target add aarch64-unknown-linux-gnu`, `cargo chef prepare`, `cargo chef cook --release --target aarch64-unknown-linux-gnu --bin tokeirad`, `cargo build --release --target aarch64-unknown-linux-gnu --bin tokeirad -p tokeirad`, `strip /app/target/aarch64-unknown-linux-gnu/release/tokeirad`, `container_from("cgr.dev/chainguard/glibc-dynamic:latest")`, `with_user("nonroot")`, `with_entrypoint(["/usr/local/bin/tokeirad"])`, `export_image("tokeirad:latest")` — and only that one export
    - Second test: with `tag = Some("v1.2.3")`, assert the additional `export_image("tokeirad:v1.2.3")` appears after the `:latest` export
    - Third test: with `arch = Amd64`, assert target triple is `x86_64-unknown-linux-gnu` (gnu, NOT musl)
    - Fourth test: assert the sequence does NOT include any `apk` command (no alpine), does NOT include any `addgroup`/`adduser` command (nonroot user comes from the chainguard base image), and does NOT include `container_from("alpine:..)`

  - [ ] 3.10 Implement `publish_image`
    - Create `crates/tokeira-build/src/pipelines/publish.rs`
    - Define `pub struct PublishRequest { local_image: String, remote_refs: Vec<String>, registry_host: String, username: String, password: String }`, `pub struct PublishResult { published: Vec<PublishedReference> }`, `pub struct PublishedReference { remote_ref: String, published_ref: String }`
    - Define `pub fn publish_image(request: &PublishRequest, dagger: &dyn DaggerClient) -> Result<PublishResult, BuildError>`
    - Reject empty `remote_refs` with `BuildError::Validation { reason: "remote_refs cannot be empty" }`
    - `set_secret`, `container_from(local_image).with_registry_auth`, loop over `remote_refs` calling `publish`
    - Map publish failures to `BuildError::Publish { remote_ref, source }` naming the failing ref; prior successful pushes are not rolled back
    - _Requirements: 3.4_

  - [ ]* 3.11 Write property test for publish reference count
    - **Property 10: Publish Reference Count**
    - **Validates: Requirement 3.4**
    - Generate `Vec<String>` of length 1..16 of valid-looking remote refs via `proptest`
    - Assert `publish_image(&request, &mock).unwrap().published.len() == request.remote_refs.len()` and `published[i].remote_ref == request.remote_refs[i]` for all i
    - Test location: `crates/tokeira-build/src/pipelines/publish.rs` `#[cfg(test)]` module
    - Minimum 100 iterations

  - [ ] 3.12 Implement `mirror_image`
    - Create `crates/tokeira-build/src/pipelines/mirror.rs`
    - Define `pub struct MirrorRequest { source_ref: String, remote_ref: String, registry_host: String, username: String, password: String }` and `pub struct MirroredReference { source_ref: String, remote_ref: String, published_ref: String }`
    - Define `pub fn mirror_image(request: &MirrorRequest, dagger: &dyn DaggerClient) -> Result<MirroredReference, BuildError>`
    - `set_secret`, `container_from(source_ref).with_registry_auth`, `publish(remote_ref)`
    - Map failures to `BuildError::Mirror { source_ref, remote_ref, source }`
    - The skip-self check (source already equals destination) is performed by the CLI caller, not by the pipeline
    - _Requirements: 3.5_

  - [ ]* 3.13 Write unit test for `mirror_image` invocation sequence
    - With `MockDaggerClient`, call `mirror_image(&request, &mock)`. Assert the recorded sequence: `set_secret`, `container_from(source_ref)`, `with_registry_auth(registry_host, username, _)`, `publish(remote_ref)`
    - With a mock that returns an error on `publish`, assert the returned error is `BuildError::Mirror` with the exact `source_ref` and `remote_ref` fields

  - [ ] 3.13a Audit public API structs for `#[derive(Debug)]`
    - AGENTS.md requires all public types to derive `Debug`. Sweep every public struct and enum introduced by this phase and confirm each carries `#[derive(Debug, ...)]` (or a manual impl when a field prevents the derive)
    - Required derives this phase:
      - `Arch` — `#[derive(Debug, Clone, Copy, PartialEq, Eq)]`
      - `BuildError` — `#[derive(Debug, thiserror::Error)]`
      - `TokeiradBuildRequest` — `#[derive(Debug, Clone)]`
      - `TokeiradBuildResult` — `#[derive(Debug, Clone)]`
      - `PublishRequest` — `#[derive(Debug, Clone)]` (safe because `password` is `RegistryPassword` with a masked `Debug` impl, not a raw `String`)
      - `PublishResult` — `#[derive(Debug, Clone)]`
      - `PublishedReference` — `#[derive(Debug, Clone)]`
      - `MirrorRequest` — `#[derive(Debug, Clone)]` (same `RegistryPassword` reasoning)
      - `MirroredReference` — `#[derive(Debug, Clone)]`
      - `RegistryPassword` — manual `Debug` that prints `"RegistryPassword(***)"`, never the cleartext
      - `DaggerClient`, `ContainerRef`, `DirectoryRef`, `FileRef`, `SecretRef` — trait objects, not structs; the trait itself adds no `Debug` bound because implementors vary
    - Secret fields (registry passwords, tokens) MUST go through `RegistryPassword` or an equivalent masking newtype; no struct derives `Debug` while holding a raw `String` password field
    - _Requirements: AGENTS.md "Rust Standards" §1 (`All public types derive Debug`)_

  - [ ] 3.14 Checkpoint — pipelines compile and mock-backed tests pass
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-build`

- [ ] 4. Extend platform configs with new mirror targets and scaffold the ECS platform
  - [ ] 4.1 Extend `ComposeConfig::observability` with `aws_cli_image` and `busybox_image`
    - In `platforms/compose/src/config.rs`, add `pub aws_cli_image: String` and `pub busybox_image: String` to `ObservabilityConfig`
    - Use per-field default functions, NOT bare `#[serde(default)]`. Add two free functions: `fn default_aws_cli_image() -> String { "public.ecr.aws/aws-cli/aws-cli:latest".into() }` and `fn default_busybox_image() -> String { "public.ecr.aws/docker/library/busybox:latest".into() }`. Annotate each field with `#[serde(default = "default_aws_cli_image")]` / `#[serde(default = "default_busybox_image")]`. This is critical: bare `#[serde(default)]` yields `String::default()` (empty string) for missing fields and would silently produce empty `upstream_ref` on a fresh `deployment.toml`
    - Update `ObservabilityConfig::default()` to call the same `default_aws_cli_image()` / `default_busybox_image()` functions
    - _Requirements: 7.5_

  - [ ] 4.2 Flip `ComposeConfig::default().tokeirad.image` from `"tokeirad:local"` to `"tokeirad:latest"`
    - In `platforms/compose/src/config.rs` (or wherever `ComposeConfig::default()` lives), change the default value for `tokeirad.image` to `"tokeirad:latest"`
    - Audit any tests, snapshots, or fixture files that encode `"tokeirad:local"` and update them. Search with `grepSearch` for the literal string `tokeirad:local` across the workspace
    - Update any prototypical-config generation helpers (e.g., `tkr deployment create --platform compose`) to emit `tokeirad:latest`
    - _Requirements: 7.1_

  - [ ] 4.3 Scaffold the ECS platform crate
    - There is no `platforms/ecs/` crate in the workspace yet. Create `platforms/ecs/Cargo.toml` with `name = "tokeira-ecs-deployment"`, `version.workspace = true`, `edition.workspace = true`, and the same dependency pattern as `platforms/compose/Cargo.toml`: `anyhow`, `async-trait`, `serde`, `serde_json`, `tokeira-aws` (new path-dep), `tokeira-config`, `tokeira-deploy-engine`, `tokeira-iac`, `tokeira-orchestrator`, `tokeira-state`
    - Add `"platforms/ecs"` to `[workspace.members]` in the root `Cargo.toml`
    - Create `platforms/ecs/src/lib.rs` with a placeholder `EcsDeployment` struct implementing the `Deployment` trait with stubbed methods (`infra_modules`, `services`, `images` each return empty `Vec`, `register_infra_extensions` and `register_deploy_extensions` are no-ops). The full implementation is filled in by subsequent phases
    - Create `platforms/ecs/src/config.rs` with `EcsConfig` matching the existing ecs-deployment spec's config model, including an `ObservabilityConfig` with `aws_cli_image` and `busybox_image` fields annotated with `#[serde(default = "default_aws_cli_image")]` / `#[serde(default = "default_busybox_image")]` pointing at module-local functions that return the same upstream defaults as compose. Do NOT use bare `#[serde(default)]`
    - Create `platforms/ecs/src/modules/mod.rs` as a placeholder for the `ImagesModule` (added in task 7.11)
    - Create `platforms/ecs/src/images/mod.rs` as a placeholder for the image modules (filled by phase 6)
    - _Requirements: 7.5_

  - [ ] 4.4 Implement `register_image_extensions` on `EcsDeployment`
    - In `platforms/ecs/src/lib.rs`, implement `async fn register_image_extensions(&self, config: &Self::Config, ctx: &mut deploy_engine::ImageContext) -> Result<()> { ctx.set_extension(config.clone()); Ok(()) }`
    - _Requirements: 1.4.4_

  - [ ] 4.5 Checkpoint — config defaults compile and the ECS crate builds
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-compose-deployment -p tokeira-ecs-deployment` (unit tests only)

- [ ] 5. Implement compose platform image modules and retire `ComposeImage`
  - [ ] 5.1 Scaffold `platforms/compose/src/images/{mod, tokeirad, observability/mod}.rs`
    - Create `platforms/compose/src/images/mod.rs` exposing TWO functions: `pub fn construct() -> Vec<Box<dyn Image>>` (context-free — concatenates `tokeirad::all()` and `observability::all()`) AND `pub fn all(ctx: &ImageContext) -> Result<Vec<Box<dyn Image>>, RuntimeError>` (calls `construct()` then `tokeira_deploy_engine::image::validate_registry`)
    - `construct()` is what `ComposeDeployment::images(&self, config)` calls; `all(ctx)` is what CLI handlers call
    - Create `platforms/compose/src/images/tokeirad.rs` with `TokeiradImage` implementing `Image` for the compose platform: `source_type = Build`, `desired_ref` reads `ComposeConfig` via `ctx.extension::<ComposeConfig>().ok_or_else(|| RuntimeError::Image(format!("image context missing extension: {}", std::any::type_name::<ComposeConfig>())))?`, `writeback_targets` returns the single `{ field: "tokeirad.image" }` target
    - Create `platforms/compose/src/images/observability/mod.rs` with the compose-local `mirror_image!` macro and the six struct invocations (Mimir, Loki, Grafana, Alloy, AwsCli, BusyBox) reading `ComposeConfig.observability.*_image`. Include the `image_tag` helper
    - CRITICAL: the `mirror_image!` macro's `desired_ref` body MUST include an empty-string guard: `if upstream.is_empty() { return Err(RuntimeError::Image(format!("image '{}' has empty upstream_ref in config", $name))); }` placed immediately after cloning the upstream field. This defends against a `deployment.toml` that explicitly sets an observability field to the empty string
    - _Requirements: 2.1, 7.5_

  - [ ] 5.2 Wire `ComposeDeployment::images` to the new registry and remove `ComposeImage`
    - In `platforms/compose/src/lib.rs`, change `fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>>` to return `crate::images::construct()`
    - Delete the legacy `ComposeImage` struct from `platforms/compose/src/services.rs` and its `impl Image` block
    - Delete the `ComposeImage` re-export from `platforms/compose/src/lib.rs` (currently `use services::{ComposeImage, ComposeWorkload};` — change to `use services::ComposeWorkload;`)
    - Delete the old `images()` body that mapped every compose service to a `ComposeImage` — the new registry replaces it
    - Audit callers of `ComposeImage` across the workspace via `grepSearch`. Expected scope: zero external callers; only the local re-export and the adapter path in `images()` need removal
    - _Requirements: 2.5_

  - [ ]* 5.3 Write unit tests for compose image `desired_ref` / `writeback_targets`
    - Register `ComposeConfig::default()` on a fresh `ImageContext`. For `TokeiradImage`, assert `desired_ref` returns `DesiredImageRef { repository: "tokeira/tokeirad", tag: "latest", upstream_ref: None }` and `writeback_targets` returns the single `tokeirad.image` target
    - For each of the six observability images, assert `desired_ref` produces the expected `repository` (`tokeira/<suffix>`), `tag` (extracted from the upstream ref), and `upstream_ref` (the full upstream ref)
    - Edge cases: upstream with digest (`repo@sha256:...`), upstream with no tag (falls back to `"latest"`), upstream with port in registry host
    - NEW: set `ComposeConfig.observability.mimir_image = ""` on the context and assert `MimirImage::desired_ref` returns the exact `RuntimeError::Image(format!("image 'grafana-mimir' has empty upstream_ref in config"))`
    - NEW: deserialize a `deployment.toml` that omits `aws_cli_image` and `busybox_image` and assert the parsed config carries the upstream defaults (not empty strings) — verifies the per-field `#[serde(default = "…")]` pattern

  - [ ]* 5.4 Write per-platform property test for compose registry / source-type / grammar (Properties 1, 2, 7)
    - **Property 1 (compose): Registry Validation**
    - **Property 2 (compose): Source-Type / Upstream Invariant**
    - **Property 7 (compose): ECR Name Grammar**
    - Generate `ComposeConfig` values via `proptest` with varied project names and observability refs
    - Assert `platforms_compose::images::all(&ctx)?` always succeeds for valid configs
    - Assert each resulting image's `source_type() == Build` implies `desired_ref.upstream_ref.is_none()`, and `== Mirror` implies `.is_some()`
    - Assert each `desired_ref.repository` matches the ECR grammar (`[a-z0-9._/-]+`, 2..=256 chars, no leading `/` or `.`)
    - Add a negative test: construct an image list with two images resolving to the same repository (one Build, one Mirror). Assert `validate_registry` returns `RuntimeError::Image` with `duplicate repository = ...` — validates that dedup is keyed by repository ALONE
    - Test location: `platforms/compose/src/images/mod.rs` `#[cfg(test)]` module
    - Minimum 128 iterations

  - [ ]* 5.5 Write compose-platform mirror stability property test (Property 6 compose side)
    - **Property 6 (compose): Mirror Mapping Stability**
    - **Validates: Requirement 9.7.1**
    - No generation — direct assertion
    - Register `ComposeConfig::default()` on a fresh `ImageContext`
    - For each Mirror image in `platforms_compose::images::observability::all()`, assert `desired_ref(&ctx)?.upstream_ref.unwrap()` equals the matching field in `ComposeConfig::default().observability`
    - Test location: `platforms/compose/src/images/observability/mod.rs` `#[cfg(test)]` module

- [ ] 6. Implement ECS platform image modules and wire `EcsDeployment::images`
  - [ ] 6.1 Scaffold `platforms/ecs/src/images/{mod, tokeirad, observability/mod}.rs`
    - Structure identical to compose (task 5.1): `mod.rs` exposes both `pub fn construct() -> Vec<Box<dyn Image>>` and `pub fn all(ctx: &ImageContext) -> Result<Vec<Box<dyn Image>>, RuntimeError>`
    - The modules read `EcsConfig` instead of `ComposeConfig`
    - `TokeiradImage::writeback_targets` returns the seven `services.<name>.image` targets in canonical order: `edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`
    - The ECS observability module carries its OWN copy of the `mirror_image!` macro reading `EcsConfig` — do NOT import the compose macro. Include the same empty-upstream guard (`if upstream.is_empty() { return Err(...) }`)
    - _Requirements: 2.2, 7.5_

  - [ ] 6.2 Wire `EcsDeployment::images` to the new registry
    - In `platforms/ecs/src/lib.rs`, replace the `fn images` stub (task 4.3) with `fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> { crate::images::construct() }`
    - The ECS platform has no pre-existing `ComposeImage`-style adapter to remove — the scaffold from task 4.3 was already a stub
    - _Requirements: 2.5_

  - [ ]* 6.3 Write unit tests for ECS image `desired_ref` / `writeback_targets`
    - Register `EcsConfig::default()` on a fresh `ImageContext`. For `TokeiradImage`, assert `desired_ref` returns `DesiredImageRef { repository: "<project>/tokeirad", tag: "latest", upstream_ref: None }` and `writeback_targets` returns the seven service targets in the canonical order above
    - For each of the six observability images, assert `desired_ref` produces the expected fields from `EcsConfig::default().observability`
    - Empty-upstream case: as per task 5.3's equivalent test

  - [ ]* 6.4 Write per-platform property test for ECS registry / source-type / grammar (Properties 1, 2, 7)
    - Mirrors task 5.4 but generates `EcsConfig` values and calls `platforms_ecs::images::all(&ctx)`
    - Include the negative duplicate-repository test: two images (Build + Mirror) resolving to the same repository must be rejected with `duplicate repository = ...`
    - Test location: `platforms/ecs/src/images/mod.rs` `#[cfg(test)]` module
    - Minimum 128 iterations

  - [ ]* 6.5 Write ECS-platform mirror stability property test (Property 6 ECS side)
    - **Property 6 (ecs): Mirror Mapping Stability**
    - **Validates: Requirement 9.7.2**
    - No generation — direct assertion
    - Register `EcsConfig::default()` on a fresh `ImageContext`
    - For each Mirror image in `platforms_ecs::images::observability::all()`, assert `desired_ref(&ctx)?.upstream_ref.unwrap()` equals the matching field in `EcsConfig::default().observability`
    - Test location: `platforms/ecs/src/images/observability/mod.rs` `#[cfg(test)]` module

  - [ ] 6.6 Checkpoint — both platforms' image sets compile and feed `Deployment::images`
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-compose-deployment -p tokeira-ecs-deployment`

- [ ] 7. Implement `EcrRepository`, `EcrClient`, `EcrClientHandle`, ensure helpers, and `ImagesModule`
  - [ ] 7.1 Define the `EcrClient` trait and default impl
    - In `crates/tokeira-aws/src/clients/ecr.rs`, define the trait with the mutation-path methods (`get_authorization_token`, `create_repository`, `delete_repository`, `put_lifecycle_policy`, `tag_resource`) AND the live-read methods (`describe_repository`, `list_tags_for_resource`, `get_lifecycle_policy`) required by drift detection
    - Method signatures: `describe_repository(name) -> Result<RepositoryDescription, EcrError>` (returns `arn`, `uri`); `create_repository(name, mutability, tags) -> Result<RepositoryDescription, EcrError>` (returns the created repo's description so `create` has the ARN without a round-trip); `list_tags_for_resource(arn) -> Result<HashMap<String, String>, EcrError>`; `get_lifecycle_policy(name) -> Result<Option<String>, EcrError>` that returns `Ok(None)` when the repository has no policy (map ECR's `LifecyclePolicyNotFoundException` to `Ok(None)` at the wrapper layer)
    - Define `EcrAuthorization`, `RepositoryDescription`, `ImageTagMutability`, `EcrError` with variants including `NotFound` and `InvalidToken`
    - Implement the default over `aws-sdk-ecr` with `#[async_trait::async_trait]`
    - Add `aws-sdk-ecr` and `base64` to `crates/tokeira-aws/Cargo.toml`
    - _Requirements: 5.1_

  - [ ] 7.2 Implement the ECR authorization decoder
    - In `crates/tokeira-aws/src/clients/ecr.rs`, implement `fn decode_authorization_data(token_b64: &str, proxy_endpoint: &str) -> Result<EcrAuthorization, EcrError>`
    - The decoder validates base64 decoding, UTF-8 decoding, presence of a `:` separator, and trims `http(s)://` and any trailing `/` from the proxy endpoint
    - _Requirements: 5.1_

  - [ ]* 7.3 Write unit tests for the authorization decoder
    - Four tests covering the four failure modes: success, invalid base64, invalid UTF-8, missing `:`
    - Each test constructs a canned `(token_b64, proxy_endpoint)` input and asserts the exact error variant on failure or the exact `EcrAuthorization` on success

  - [ ] 7.4 Define `EcrClientHandle` extension wrapper and the `ecr_client` helper
    - In `crates/tokeira-aws/src/clients/ecr.rs`, define `pub struct EcrClientHandle(pub Arc<dyn EcrClient>)` deriving `Clone`
    - Add a private `fn ecr_client(ctx: &ProvisionContext) -> Result<Arc<dyn EcrClient>, IacError>` helper that calls `ctx.extension::<EcrClientHandle>().map(|h| h.0.clone()).ok_or_else(|| IacError::Other(anyhow::anyhow!("ProvisionContext missing extension: EcrClientHandle")))`
    - _Requirements: 1.3, 5.1.2_

  - [ ] 7.5 Implement `EcrRepository` resource with per-lifecycle tag computation and live-read describe
    - In `crates/tokeira-aws/src/resources/ecr_repository.rs`, define `pub struct EcrRepository { name: String, module: String }` with `#[derive(Debug, Clone, Serialize, Deserialize)]`. Do NOT add a `tags` field. Tags are computed at lifecycle time by calling `ctx.resource_tags(&self.name)` inside each lifecycle method
    - Define `pub const ECR_LIFECYCLE_POLICY: &str` with the canonical JSON from the Design doc
    - Add a `fn state_from_live(name, desc, live_tags, live_policy, module) -> ResourceState` helper that builds `ResourceState` from LIVE values read back from ECR — not from what was asked for. The persisted `properties` map includes `repository_name`, `repository_uri`, `lifecycle_policy` (live), `tags` (live). This is what allows `diff()` to detect external drift such as an operator retagging the repo in the AWS console
    - Implement the `Resource` trait exactly matching the existing trait signature (`async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError>` and siblings). Do NOT add any additional parameter to the trait methods. Inside each method, fetch the ECR client via the `ecr_client(ctx)?` helper from task 7.4
    - `create`: compute `let tags = ctx.resource_tags(&self.name);` → `create_repository(&self.name, Mutable, &tags)` → `put_lifecycle_policy` → read back `list_tags_for_resource(&desc.arn)` + `get_lifecycle_policy(&self.name)` → return `state_from_live(...)`. Persisting the live state (not the desired state) means later diffs compare against what AWS actually holds
    - `update`: recompute `let tags = ctx.resource_tags(&self.name);` → `put_lifecycle_policy` + `tag_resource(arn_from_state, &tags)` → read back live tags + live policy → return `state_from_live(...)`
    - `delete`: force-delete (`force=true`)
    - `describe`: call `describe_repository`. On `NotFound` return `Ok(None)`. On success, additionally call `list_tags_for_resource(&desc.arn)` and `get_lifecycle_policy(&self.name)` (`get_lifecycle_policy` returns `Ok(None)` when no policy is attached — the wrapper maps `LifecyclePolicyNotFoundException` to `None`). Return `Ok(Some(state_from_live(...)))`. Do NOT call `ctx.resource_tags` here — describe reads live state, not desired state
    - `diff`: compare `desired_tags = ctx.resource_tags(&self.name)` against `current.properties["tags"]` (which describe populated from live data); compare `normalize_lifecycle_policy(ECR_LIFECYCLE_POLICY)` against `normalize_lifecycle_policy(current.properties["lifecycle_policy"])`. Either mismatch signals `Update`
    - Implement a constructor `EcrRepository::new(name: String, module: String) -> Result<Self, EcrError>` that validates the name against the ECR grammar (2–256 chars, `[a-z0-9._/-]+`, not starting with `/` or `.`). The constructor signature takes exactly TWO arguments — no tags parameter
    - _Requirements: 5.1_

  - [ ]* 7.6 Write unit tests for `EcrRepository` resource methods
    - Construct a `MockEcrClient` that records calls AND holds a mutable in-memory map of `(repo_name -> (tags, policy))` so drift can be simulated. Register the mock on a `ProvisionContext` via `EcrClientHandle`
    - Test `create`: assert it calls `create_repository` with the expected tags, then `put_lifecycle_policy`, then `list_tags_for_resource` + `get_lifecycle_policy`, and returns a `ResourceState` whose `properties.tags` match the mock's live state (not the supplied desired tags — proves the live-read path is wired)
    - Test `describe` — repository absent: assert `Ok(None)` on `EcrError::NotFound`
    - Test `describe` — repository present: assert the returned `ResourceState` carries live tags and live policy from the mock, NOT `ctx.resource_tags(&self.name)`
    - Test `describe` — repository present with no policy: assert `get_lifecycle_policy` returns `Ok(None)` (wrapper handles `LifecyclePolicyNotFoundException`) and `state.properties["lifecycle_policy"]` is the empty string
    - Test `diff` — external tag drift: seed the mock with a repo whose live tags differ from `ctx.resource_tags(&self.name)`; call `describe` then `diff` and assert `Change::Update { details: "tags changed" }`. This is the regression test for P1
    - Test `diff` — external policy drift: seed the mock with a repo whose live policy differs from `ECR_LIFECYCLE_POLICY`; call `describe` then `diff` and assert `Change::Update { details: "lifecycle policy changed" }`
    - Test `diff` — no drift: seed the mock with a repo whose live tags match `ctx.resource_tags(&self.name)` and whose live policy matches `ECR_LIFECYCLE_POLICY`; call `describe` then `diff` and assert `Change::NoChange`
    - Test `update`: assert it calls `put_lifecycle_policy`, `tag_resource`, then reads live state back, and that the returned `ResourceState` reflects the live state
    - Test `delete`: assert it calls `delete_repository(name, force = true)`
    - Test missing-extension: construct a `ProvisionContext` WITHOUT `EcrClientHandle`; call `create` and assert the returned error is `IacError::Other` with the exact message `"ProvisionContext missing extension: EcrClientHandle"`

  - [ ]* 7.7 Write property test for lifecycle policy JSON round-trip (Property 5)
    - **Property 5: Lifecycle Policy JSON Round-Trip**
    - **Validates: Requirement 9.5**
    - Parse `ECR_LIFECYCLE_POLICY` with `serde_json::from_str::<serde_json::Value>`, serialize with `to_string`, re-parse
    - Assert the two parsed `Value`s are equal
    - Test location: `crates/tokeira-aws/src/resources/ecr_repository.rs` `#[cfg(test)]` module

  - [ ] 7.8 Implement ad-hoc `ensure_ecr_repository` and `ensure_ecr_repositories`
    - In `crates/tokeira-aws/src/clients/ecr.rs`, add `pub async fn ensure_ecr_repository(ecr: &dyn EcrClient, name: &str, tags: &HashMap<String, String>) -> Result<(), EcrError>`
    - The helper SHALL: (a) describe the repository; (b) if `EcrError::NotFound`, create it with `MUTABLE` mutability and the supplied tags, capturing the ARN from the create response; otherwise capture the ARN from the describe response; (c) unconditionally call `put_lifecycle_policy(name, ECR_LIFECYCLE_POLICY)`; (d) unconditionally call `tag_resource(&arn, tags)`
    - Step (d) is the critical reconciliation step: it ensures that a pre-existing repository with stale tags is brought into agreement with the current tag set. Without it, `EcrRepository::diff` would still report tag drift on the next `tkr infra apply`
    - Add `pub async fn ensure_ecr_repositories(ecr: &dyn EcrClient, repos: &[(String, HashMap<String, String>)]) -> Result<(), EcrError>` that calls the single-repo helper in sequence
    - _Requirements: 5.3_

  - [ ]* 7.9 Write ad-hoc / IaC consistency tests
    - Test A (fresh create): construct a `ProvisionContext` registering a `MockEcrClient` via `EcrClientHandle`. Call `ensure_ecr_repository(ecr, name, &tags)` against the mock, then `EcrRepository::describe(&ctx)` and assert the returned `ResourceState` matches what `EcrRepository::create` would have produced. Call `EcrRepository::diff(&state, &ctx)` and assert `Change::NoChange`
    - Test B (stale-tag reconciliation): seed the mock with a pre-existing repository carrying stale tags (for example `{"Version": "old"}`). Call `ensure_ecr_repository(ecr, name, &current_tags)` where `current_tags = {"Version": "new"}`. Assert the mock recorded a `tag_resource(arn, current_tags)` call (proving reconciliation happened) and that `EcrRepository::describe(&ctx)` followed by `diff` reports `NoChange`
    - _Requirements: 5.3.4, 5.3.5_

  - [ ]* 7.10 Write property test for repository creation idempotence (Property 4)
    - **Property 4: ECR Repository Creation Idempotence**
    - **Validates: Requirement 9.4**
    - Generate `Vec<(String, HashMap<String, String>)>` of length 0..20 with distinct grammar-valid names
    - Call `ensure_ecr_repositories` twice with the same input against a shared `MockEcrClient`
    - Assert the mock's repository set after the second call equals the set after the first, same policies, same tags
    - Minimum 64 iterations

  - [ ] 7.11 Implement the ECS `ImagesModule` using the in-constructor capture pattern
    - Create `platforms/ecs/src/modules/images.rs` with `pub struct ImagesModule { config: EcsConfig }` and `pub fn new(config: EcsConfig) -> Self`. Do NOT add a `tags` field and do NOT accept tags in the constructor — tags are computed per-repository at lifecycle time by each `EcrRepository::create`/`update` via `ctx.resource_tags(&self.name)`
    - This mirrors the existing compose `ComposeModule::runtime(config)` idiom — do NOT attempt to read `EcsConfig` from `ModuleContext::extension`, and do NOT call fabricated helpers like `mctx.default_tags()` or `mctx.runtime_state()`. `ModuleContext` in this workspace exposes only `state` and typed extensions
    - `Module::name()` returns `"images"`; `Module::dependencies()` returns `&[]`; `Module::resources(&self, _ctx)` constructs an `ImageContext::default()`, calls `image_ctx.set_extension(self.config.clone())`, iterates `platforms_ecs::images::all(&image_ctx)?`, and for each image constructs `EcrRepository::new(desired.repository, "images".into())?`. Map any `RuntimeError` / `EcrError` to `IacError::Other(anyhow::anyhow!(e))`
    - In the ECS platform's `infra_modules` composition (added by ecs-deployment spec; this spec extends the composition list), construct the module via `ImagesModule::new(config.clone())`. No tag map passes through — `ProvisionContext::tags` (populated by ecs-deployment's `register_infra_extensions`) is the single source of truth, and each resource resolves its own `Name`-including tag map via `resource_tags(&name)` at apply time
    - Register `ImagesModule` in the ECS platform's module composition list alongside `foundation`, `networking`, `dsql`, `cluster`, `observability`, `services`
    - _Requirements: 5.2_

  - [ ] 7.11a Implement `ensure_ecr_repositories_from_images` glue for CLI handlers
    - Create `platforms/ecs/src/images/ensure.rs` with `pub async fn ensure_ecr_repositories_from_images(ecr: &dyn EcrClient, ctx: &ProvisionContext, images: &[Box<dyn Image>], image_ctx: &ImageContext) -> Result<(), IacError>`
    - Implementation: iterate `images`; for each, compute `desired = img.desired_ref(image_ctx)?;` and `tags = ctx.resource_tags(&desired.repository);` — the SAME `resource_tags` helper that `EcrRepository::create` uses. Collect `(repository, tags)` pairs and call `tokeira_aws::ensure_ecr_repositories(ecr, &repos).await`
    - Re-export from `platforms/ecs/src/images/mod.rs` so `tkr image push` and `tkr image mirror` both import this single entry point
    - _Requirements: 5.3a_

  - [ ]* 7.11b Write property test for push-vs-IaC tag parity
    - **Property: Tag Parity Between Ad-Hoc Ensure and IaC**
    - **Validates: Requirement 5.3a.4**
    - Generate `EcsConfig` values and synthetic `ProvisionContext::tags` maps via `proptest`
    - For each generated config: construct a `ProvisionContext` with those tags; build `platforms_ecs::images::all(&image_ctx)`; call `ensure_ecr_repositories_from_images` with a recording `MockEcrClient`; assert the `(name, tags)` pairs passed to `ensure_ecr_repository` match what `EcrRepository { name: desired.repository, module: "images" }` would compute via `ctx.resource_tags(&name)`
    - This property proves that a repository created via `tkr image push` is adopted by a later `tkr infra apply` with `NoChange`
    - Test location: `platforms/ecs/src/images/ensure.rs` `#[cfg(test)]` module
    - Minimum 64 iterations

  - [ ] 7.12 Register `EcrClientHandle` on the ECS orchestrator's `ProvisionContext`
    - In `platforms/ecs/src/lib.rs` (or wherever ECS-platform provision-context construction lives), register `EcrClientHandle(Arc::new(DefaultEcrClient::from_aws_config(...).await?))` once, before any `EcrRepository` lifecycle method runs
    - Mirror the pattern used by any existing DSQL client registration
    - _Requirements: 5.1.2_

  - [ ]* 7.13 Write integration test for `ImagesModule` composition
    - With a realistic `EcsConfig`, construct `ImagesModule` via the platform's module-selection helper
    - Assert the module's resource list contains one `EcrRepository` per image in `platforms_ecs::images::all(ctx)`, with repository names matching `desired_ref(ctx)?.repository` exactly
    - Test location: `platforms/ecs/src/modules/images.rs` `#[cfg(test)]` module

  - [ ] 7.13a Audit `tokeira-aws` public API structs for `#[derive(Debug)]`
    - Sweep every public struct/enum introduced by this phase and confirm each carries `#[derive(Debug, ...)]` or a manual `Debug` impl:
      - `EcrRepository` — `#[derive(Debug, Clone, Serialize, Deserialize)]`
      - `EcrClientHandle` — wraps `Arc<dyn EcrClient>` which cannot derive `Debug` through the trait object. Provide a MANUAL impl: `impl std::fmt::Debug for EcrClientHandle { fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { f.debug_tuple("EcrClientHandle").field(&"<dyn EcrClient>").finish() } }`
      - `EcrAuthorization`, `RepositoryDescription`, `ImageTagMutability`, `EcrError` — all derive `Debug`
      - `EcrClient` trait — no `Debug` bound (implementors vary)
    - _Requirements: AGENTS.md "Rust Standards" §1 (`All public types derive Debug`)_

  - [ ] 7.14 Checkpoint — ECR resource, module, and extensions compile
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-aws -p tokeira-ecs-deployment`

- [ ] 8. Extract shared writeback helper into `tokeira-iac`
  - [ ] 8.1 Move the private helper into `tokeira_iac::writeback` with a file-agnostic signature
    - Move `write_tokeirad_writeback` (currently in `apps/tkr/src/commands/infra.rs`) and its private dotted-key `toml_edit` writer into `crates/tokeira-iac/src/writeback.rs` as `pub fn write_config_values(path: &Path, values: &[(&str, &str)]) -> Result<(), WritebackError>` — taking the absolute file path explicitly, NOT a deployment directory
    - Remove the current hardcoded `deployment_path.join(TOKEIRAD_TOML)` from the body; use the supplied `path` directly
    - Define `pub enum WritebackError` in the same module as a `thiserror` enum with variants `Io { path, source }`, `Parse { path, source }`, `InvalidKey { key, reason }`, `Write { path, source }`
    - Re-export `write_config_values` and `WritebackError` from the `tokeira_iac` crate root
    - Migrate the existing proptests (`toml_writeback_round_trips`, `toml_writeback_preserves_comments`) from `apps/tkr/src/commands/infra.rs` to `crates/tokeira-iac/src/writeback.rs` `#[cfg(test)]` module, adjusting their inputs to supply the full file path
    - _Requirements: 7.3_

  - [ ] 8.2 Migrate `apps/tkr/src/commands/infra.rs` to the public helper — `tokeirad.toml` target
    - Replace the local `write_tokeirad_writeback` call with `tokeira_iac::write_config_values(&ctx.path.join(TOKEIRAD_TOML), &borrowed)` — preserving the existing behaviour: IaC outputs are written to the server config file
    - Delete the now-unused private helper and the private dotted-key writer
    - Do NOT change which keys are written or which file is written; only the call path moves
    - _Requirements: 7.3.3_

  - [ ] 8.3 `tkr image push` / `tkr image mirror` target `deployment.toml`
    - When the image handlers (task 9.4 / 9.5) call the shared helper, they pass `&ctx.path.join(DEPLOYMENT_TOML)` — NOT `TOKEIRAD_TOML`. Image refs (`services.*.image`, `observability.*_image`) live in the platform config file, not the server config file
    - Add a small assertion in the handlers: before calling the helper, verify `deployment.toml` exists and return a descriptive error if not
    - _Requirements: 7.3.4_

  - [ ]* 8.4 Write unit test asserting parity and file-target correctness
    - Call `tokeira_iac::write_config_values(tempdir.path().join("tokeirad.toml"), &...)` with a set of values that exercise: creating a new dotted key, overwriting an existing dotted key, creating intermediate tables, preserving a comment line. Assert the result matches the pre-extraction behaviour
    - Call `tokeira_iac::write_config_values(tempdir.path().join("deployment.toml"), &...)` with an image writeback value and assert the key lands in `deployment.toml`, not `tokeirad.toml` (create both files in the tempdir and read both after the call)

  - [ ] 8.5 Checkpoint — writeback extraction complete, tkr infra parity preserved
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-iac -p tkr`

- [ ] 9. Wire the `tkr image` command group
  - [ ] 9.1 Add `Image` subcommand to `apps/tkr/src/cli.rs`
    - Add `Image(ImageArgs)` variant to the top-level `Command` enum, positioned between `Deployment` and `Infra`
    - Define `ImageArgs` with a subcommand field bound to `ImageCommand { List { source_type }, Build { arch, tag }, Push { tag, image, yes }, Mirror { image, yes } }`
    - Only `List`, `Push`, and `Mirror` require an active deployment. `Build` does not require `--deployment`
    - _Requirements: 6.1_

  - [ ] 9.2 Implement `run_build` — deployment-free
    - Create `apps/tkr/src/commands/image.rs` with `run_build(cmd, format)`
    - Parse `--arch`, resolve workspace root via `workspace_root_from_cargo`, re-exec under `dagger run` if `DAGGER_SESSION_*` env vars are absent, then call `tokeira_build::build_tokeirad_image(&request, &DefaultDaggerClient::from_env()?)` directly
    - No platform dispatch, no `ImageContext`, no image-set iteration
    - Emit progress events via the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) callback surface. When `--json` is active, emit the summary `{ "action": "build", "image": "tokeirad", "tags": [...], "arch": "<arch>" }`
    - Build does NOT prompt for confirmation
    - _Requirements: 6.3_

  - [ ] 9.3 Implement `run_list` — platform-dispatched, uses `register_image_extensions`
    - For `List { source_type }`: require `--deployment`, construct an `ImageContext` via `let mut ctx = deploy_engine::ImageContext::default(); deployment.register_image_extensions(config, &mut ctx).await?;` (same hook `DeployEngine` uses — see Req 1.4)
    - Dispatch on `deployment.platform_kind()` to `tokeira_compose_deployment::images::all(&ctx)` or `tokeira_ecs_deployment::images::all(&ctx)`, filter by `source_type`
    - Render a table with columns `NAME`, `SOURCE`, `REPOSITORY`, `TAG`, `UPSTREAM` (human) or JSON array with keys `{ name, source_type, repository, tag, upstream_ref }`
    - Local platform: return an error stating local has no image set
    - _Requirements: 6.2, 1.4.6_

  - [ ] 9.2a Implement the `LocalImageInspector` trait and `DockerCliInspector` in `apps/tkr`
    - Create `apps/tkr/src/commands/image/local_inspector.rs` with `#[async_trait::async_trait] pub trait LocalImageInspector: Send + Sync { async fn image_exists(&self, image_ref: &str) -> anyhow::Result<bool>; }`
    - Define `#[derive(Debug)] pub struct DockerCliInspector;` implementing the trait by shelling out to `docker image inspect <ref>` via `tokio::process::Command`. Exit 0 ⇒ `Ok(true)`; exit non-zero with `stderr.contains("No such image")` ⇒ `Ok(false)`; anything else ⇒ `Err` with stderr attached via `anyhow::Context`
    - Provide a `#[cfg(test)] pub struct MockLocalImageInspector` that records calls and returns canned `Ok(bool)` / `Err(...)` responses
    - Do NOT take a dependency on bollard — shelling out to `docker` keeps `apps/tkr` slim and matches how operators invoke Docker on their workstation
    - Do NOT reuse `platforms::compose::gates::DockerImageInspector` — that trait returns `ComposeError` and lives in a crate `apps/tkr` does not depend on
    - _Requirements: 6.4.3_

  - [ ] 9.3a Implement the `--image <name>` validation helper
    - In `apps/tkr/src/commands/image.rs`, add `fn validate_image_filter(filter: Option<&str>, images: &[Box<dyn Image>], source: ImageSourceType) -> anyhow::Result<Vec<&Box<dyn Image>>>`
    - When `filter` is `None`, return every image in `images` whose `source_type() == source`
    - When `filter` is `Some(name)`, search `images` for one with `name()` matching `name` AND `source_type() == source`; if found return a single-element `Vec`; if not found, return `Err(anyhow!("unknown {source:?} image '{name}'; valid {source:?} images are: {}", valid_names.join(", ")))` where `valid_names` is the sorted list of `name()` values for images whose `source_type() == source`
    - `run_push` and `run_mirror` both call this helper before their preflight / Dagger work
    - _Requirements: 6.4.8, 6.5.8_
  - [ ] 9.4 Implement `run_push` — ECS-only, preflight-first ordering, uses `register_image_extensions` + `ensure_ecr_repositories_from_images`
    - For `Push { tag, image, yes }`: require `--deployment`, confirm per `tkr-cli` rules, reject non-ECS platforms with a descriptive error
    - Build `ImageContext` via `deployment.register_image_extensions(config, &mut ctx).await?` (NOT a handler-local `build_ecs_image_context`)
    - Call `tokeira_ecs_deployment::images::all(&image_ctx)`, then pass the result through `validate_image_filter(image.as_deref(), &images, ImageSourceType::Build)?` from task 9.3a. This handles both the filter-matches-nothing case (returns an operator-facing `unknown Build image '<name>'` error) and the no-filter case (returns all Build images)
    - **Preflight FIRST — local image store check.** Construct an `inspector: &dyn LocalImageInspector` (production: `DockerCliInspector`; tests: `MockLocalImageInspector`). For every selected image, call `inspector.image_exists(&ref).await?` and fail with the "run `tkr image build` first" message on `Ok(false)`. Do NOT re-exec under `dagger run`, do NOT construct `DefaultDaggerClient`, do NOT construct `DefaultEcrClient`, do NOT call `get_authorization_token`, do NOT call `ensure_ecr_repositories_from_images`. The preflight gate MUST pass for every selected image before any AWS or Dagger work begins — this keeps the cheap-error path cheap when the operator forgot the build step
    - ONLY after preflight passes: build a `ProvisionContext` via the ECS platform's `register_infra_extensions` hook so its `tags` map is populated (same code path `tkr infra apply` uses)
    - Re-exec under `dagger run` if needed; construct `DefaultDaggerClient::from_env()`, construct `DefaultEcrClient`, call `get_authorization_token` once, decode
    - Ensure repos ONCE for the entire selected image set: `platforms_ecs::images::ensure_ecr_repositories_from_images(&ecr, &provision_ctx, &selected_images, &image_ctx).await?`. Do NOT construct `(repository, tags)` pairs inline — that duplicates logic and risks tag drift
    - For each selected image, compute the deduped publish ref list: always start with `format!("{reg}/{repo}:latest")`; when `tag != "latest"`, append `format!("{reg}/{repo}:{tag}")`. When `tag == "latest"`, the list has exactly one entry — no double-publish of the same ref. Mirror the build pipeline's tag-dedup rule from Req 3.2.8
    - Call `publish_image(&PublishRequest { local_image: "tokeirad:latest", remote_refs: deduped, ... })`
    - Collect writebacks: for each image, for each `WritebackTarget` in `image.writeback_targets(&image_ctx)`, push `(target.field, effective_ref)` where `effective_ref` is the version-tagged `{reg}/{repo}:{tag}` when `tag != "latest"` and the single `{reg}/{repo}:latest` ref otherwise
    - Call `tokeira_iac::write_config_values(&deployment_path.join(DEPLOYMENT_TOML), &borrowed)` once with the full writeback list — the platform config file, NOT `tokeirad.toml`
    - Emit progress events and `--json` summary. The `published` array SHALL have exactly the length of the deduped `remote_refs` list
    - _Requirements: 6.4, 7.2, 7.3.4, 1.4.6, 6.4.3, 6.4.8_

  - [ ] 9.5 Implement `run_mirror` — ECS-only, uses `register_image_extensions` + `ensure_ecr_repositories_from_images`
    - For `Mirror { image, yes }`: require `--deployment`, confirm per `tkr-cli` rules, reject non-ECS platforms
    - Build `ImageContext` via `deployment.register_image_extensions(config, &mut ctx).await?`
    - Build `ProvisionContext` via the ECS platform's `register_infra_extensions` hook
    - Call `tokeira_ecs_deployment::images::all(&image_ctx)`, then pass the result through `validate_image_filter(image.as_deref(), &images, ImageSourceType::Mirror)?` from task 9.3a. The filter-matches-nothing case returns an operator-facing `unknown Mirror image '<name>'` error
    - Re-exec under `dagger run` if needed; construct `DefaultDaggerClient`, `DefaultEcrClient`, get auth token
    - Ensure repos ONCE via `ensure_ecr_repositories_from_images(&ecr, &provision_ctx, &selected_images, &image_ctx).await?` — same canonical helper as push
    - For each selected image: resolve `desired_ref`, compute `destination_ref = format!("{reg}/{repo}:{tag}")`
    - **Skip-self check:** if `desired.upstream_ref == Some(destination_ref.clone())`, or the upstream already has the destination registry-host + repo prefix, report the image as `skipped` in the summary and do NOT call `mirror_image`
    - Otherwise: compute the source via fallible unwrapping — `let source_ref = desired.upstream_ref.clone().ok_or_else(|| anyhow!("image '{}' is Mirror but desired_ref.upstream_ref is None", image.name()))?;` — and call `mirror_image(&MirrorRequest { source_ref, remote_ref: destination_ref.clone(), ... })`. Do NOT use `.unwrap()` on `desired.upstream_ref` — Property 2 prevents `None` in practice but the operator-facing error path must remain
    - Collect writebacks: for each image (including skipped ones), for each `WritebackTarget`, push `(target.field, destination_ref.clone())`
    - Call `tokeira_iac::write_config_values(&deployment_path.join(DEPLOYMENT_TOML), &borrowed)` once — the platform config file, NOT `tokeirad.toml`
    - Emit progress events and `--json` summary
    - _Requirements: 6.5, 7.2, 7.3.4, 1.4.6, 6.5.8_

  - [ ] 9.6 Wire the `image` command into `apps/tkr/src/main.rs`
    - Add a `Command::Image(args) => commands::image::run(args.command, deployment_for_subcommand, format).await?` arm
    - `Build` receives `None` for the deployment; `List`, `Push`, `Mirror` require `Some(deployment)`
    - _Requirements: 6.1_

  - [ ]* 9.7 Write unit tests for CLI parse
    - `tkr image list`: default; `--source-type build`; `--source-type mirror`; `--json`
    - `tkr image build`: defaults (arch=arm64, no tag); `--arch amd64 --tag v1.2.3`; assert NO `--deployment` is required (parse succeeds without it)
    - `tkr image push`: default (`tag=latest`); `--tag v2026-03-21 --yes`; `--image tokeirad`; assert `--deployment` IS required
    - `tkr image mirror`: default; `--image grafana-mimir`; `--yes`; assert `--deployment` IS required
    - Test location: `apps/tkr/src/commands/image.rs` `#[cfg(test)]` module

  - [ ]* 9.7a Write unit tests for `--image` filter validation
    - With a fresh `MockEcrClient` and `MockDaggerClient`, call `run_push` and `run_mirror` with `--image tokierad` (typo). Assert the call returns an error containing `unknown Build image 'tokierad'` (or `Mirror` for mirror) AND the mocks recorded ZERO calls — validation must run before any AWS or Dagger work
    - Positive case: `--image tokeirad` matches and executes normally
    - _Requirements: 6.4.8, 6.5.8_

  - [ ]* 9.7b Write unit test for the push preflight short-circuit
    - Build a `MockLocalImageInspector` that returns `Ok(false)` for `tokeirad:latest`. Wire it into a test harness that substitutes all external clients with mocks
    - Call `run_push` on a valid ECS deployment with no `--image` filter. Assert:
      - The returned error contains `tokeirad:latest` and `tkr image build`
      - The `MockLocalImageInspector` recorded exactly one `image_exists("tokeirad:latest")` call
      - The `MockDaggerClient` was NEVER constructed (the handler stopped before `DefaultDaggerClient::from_env()`)
      - The `MockEcrClient` recorded ZERO calls (no `get_authorization_token`, no `ensure_ecr_repository_from_images`, no `publish`)
    - This test is the regression guard for Req 6.4.3 preflight-first ordering
    - _Requirements: 6.4.3_

  - [ ]* 9.8 Write property test for mirror idempotence (Property 3)
    - **Property 3: Mirror Idempotence**
    - **Validates: Requirement 9.3**
    - Generate `EcsConfig` values with populated observability fields (tag generation bounded to a small alphabet)
    - With a shared `MockDaggerClient` + `MockEcrClient` + a tempdir-backed `deployment.toml`, invoke `run_mirror` twice in sequence
    - Assert both calls succeed; mock repo set after second call equals after first; `deployment.toml` contents unchanged between the two calls
    - Test location: `apps/tkr/src/commands/image.rs` `#[cfg(test)]` module
    - Minimum 32 iterations (tempdir cost)

  - [ ] 9.9 Checkpoint — CLI and pipelines tie together
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tkr -p tokeira-build -p tokeira-aws`

- [ ] 10. Add lifecycle gates driven by the image registry
  - [ ] 10.1 Implement ECS `validate_mirrors`
    - In `platforms/ecs/src/gates.rs`, add `pub fn validate_mirrors(cfg: &EcsConfig, registry: &str, images: &[Box<dyn Image>], ctx: &ImageContext) -> Result<(), EcsError>`
    - Iterate `images` filtered to `ImageSourceType::Mirror`; for each, iterate `writeback_targets(ctx)` and read the dotted key from `cfg`; collect any empty / non-`{registry}/`-prefixed fields
    - Return `EcsError::UnmirroredImages { fields, remediation: "run `tkr image mirror`" }` when any unmirrored field is found
    - Call this validator from `EcsPlatform::validate_for_apply` (invoked before `tkr infra apply`)
    - _Requirements: 8.1, 9.9_

  - [ ] 10.2 Implement ECS `validate_builds`
    - Symmetric to `validate_mirrors` but filters images to `ImageSourceType::Build` and recommends `tkr image push --tag <version>` in the remediation message
    - Call this validator from `EcsPlatform::validate_for_deploy_apply` (invoked before `tkr deploy apply`)
    - _Requirements: 8.2, 9.9_

  - [ ]* 10.3 Write property test for `validate_mirrors` and `validate_builds` (Property 8)
    - **Property 8: Lifecycle Gate Predicates**
    - **Validates: Requirement 9.9**
    - Generate `EcsConfig` values with writeback-target fields chosen from: empty, upstream source, `{registry}/<repo>:<tag>`
    - Assert `validate_mirrors` returns `Err(UnmirroredImages)` iff at least one Mirror image's writeback-target field is empty or not `{registry}/`-prefixed
    - Assert `validate_builds` returns `Err` symmetrically for Build images
    - Test location: `platforms/ecs/src/gates.rs` `#[cfg(test)]` module
    - Minimum 128 iterations

  - [ ] 10.4 Implement the compose `DockerImageInspector` trait
    - In `platforms/compose/src/gates.rs` (new file), define `#[async_trait::async_trait] pub trait DockerImageInspector: Send + Sync { async fn image_exists(&self, image: &str) -> Result<bool, ComposeError>; }`
    - Define `pub struct BollardInspector(pub bollard::Docker)` implementing the trait by calling `self.0.inspect_image(image)` and mapping `bollard::errors::Error::NotFound` to `Ok(false)`, other bollard errors to `ComposeError::DockerIo`
    - _Requirements: 8.3.4_

  - [ ] 10.5 Implement the compose `validate_local_build` gate
    - Add `pub async fn validate_local_build<I: DockerImageInspector + ?Sized>(cfg: &ComposeConfig, inspector: &I) -> Result<(), ComposeError>`
    - When `cfg.tokeirad.image == "tokeirad:latest"`, call `inspector.image_exists("tokeirad:latest").await?`; return `ComposeError::LocalBuildMissing` with the `tkr image build` remediation if `false`
    - When `cfg.tokeirad.image` is any other value, return `Ok(())` without calling the inspector
    - _Requirements: 8.3_

  - [ ]* 10.6 Write unit tests for the compose build gate
    - With a fake `DockerImageInspector` returning `Ok(false)`, assert the gate returns `ComposeError::LocalBuildMissing` with "tkr image build" in the message
    - With a fake returning `Ok(true)`, assert `Ok(())`
    - With `cfg.tokeirad.image = "my-registry.example/tokeirad:custom"`, assert `Ok(())` without invoking the inspector (use a mock that records calls and assert call count is 0)
    - Test location: `platforms/compose/src/gates.rs` `#[cfg(test)]` module

  - [ ] 10.7 Wire the compose gate into `ComposePlatform::validate_for_deploy_apply`
    - In `platforms/compose/src/lib.rs`, add `pub async fn validate_for_deploy_apply(&self, config: &ComposeConfig) -> Result<(), ComposeError>` on `ComposePlatform`
    - Implementation: `gates::validate_local_build(config, &BollardInspector(self.docker.clone())).await`
    - Update the `tkr deploy apply` command handler to invoke this hook before constructing the deploy-engine service list
    - The check does NOT live in `platforms/compose/src/services.rs` — that module only builds deploy-engine service descriptors and has no Docker access
    - _Requirements: 8.3_

  - [ ] 10.8 Checkpoint — gates pass property tests
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-ecs-deployment -p tokeira-compose-deployment`

- [ ] 11. Integration and documentation
  - [ ] 11.1 Update `README.md` in four specific places
    - **Edit 1 — `### Command Tree` (under `## `tkr` — Operator and Developer CLI`)**: insert the `image` subtree between `deploy` and `schema`, matching the exact shape in Req 10.2.1.a:
      ```
      ├── image
      │   ├── list [--source-type <build|mirror>] [--json]
      │   ├── build [--arch <arm64|amd64>] [--tag <version>]
      │   ├── push --tag <version> [--image <name>] [--yes]
      │   └── mirror [--image <name>] [--yes]
      ```
    - **Edit 2 — Rewrite the `### Compose Platform` walkthrough** to include `tkr image build` as an explicit step. The updated walkthrough SHALL look like:
      ```bash
      # Create
      tkr deployment create --name dev-compose --platform compose --storage in-memory

      # Build the tokeirad image (compose reads it from the local Docker image store)
      tkr image build

      # Provision infrastructure (creates containers via bollard)
      tkr infra plan
      tkr infra apply --yes

      # Deploy services
      tkr deploy apply --yes

      # Operations
      tkr scale status
      tkr logs tokeirad --follow --tail 50
      tkr logs grafana --tail 20
      tkr port-forward grafana

      # Module-scoped operations
      tkr infra apply --yes --module observability
      tkr infra destroy --yes --module observability

      # Tear down
      tkr infra destroy --yes
      tkr deployment destroy dev-compose --yes
      ```
    - Append two explanatory paragraphs after the walkthrough:
      > **Why the build step is separate.** `tkr deploy apply` does not invoke the image builder — it requires `tokeirad:latest` to already exist in the local Docker image store. This keeps the deploy path deterministic and fast: a repeat deploy does not rebuild. Re-run `tkr image build` whenever you want a fresh `tokeirad` binary in the compose stack.
      >
      > **Storage and schema.** The example above uses `--storage in-memory`, which needs no schema setup. For compose deployments that target DSQL (`--storage dsql`), operators must currently point `tokeirad.toml`'s `infrastructure.dsql.endpoint` at an externally-provisioned DSQL cluster — the compose platform has no DSQL module. After the endpoint is set, run `tkr schema setup --yes` before `tkr deploy apply`. First-class DSQL provisioning for the compose platform is deferred to a future spec; for now the in-memory path is the supported compose workflow and the DSQL path is an advanced use case.
    - **Edit 3 — Add a new `### Image Management` section** under `## `tkr` — Operator and Developer CLI`, positioned immediately after the updated `### Compose Platform`. Include:
      - A short prose introduction explaining that `tkr image` manages the image plane: building `tokeirad` from source, pushing built images to ECR, and mirroring upstream observability images into project-owned ECR
      - Example: `tkr image build` (deployment-free, produces `tokeirad:latest`)
      - Example: `tkr image build --arch amd64 --tag v1.2.3` (additionally exports `tokeirad:v1.2.3`)
      - Example: `tkr image list --source-type mirror --json` (enumerate mirror images for the active deployment)
      - Example: `tkr image mirror --yes` (mirror every upstream observability image into project-owned ECR; ECS only)
      - Example: `tkr image push --tag v2026-03-21 --yes` (push `tokeirad` to ECR, writeback to `services.*.image`; ECS only)
      - Lifecycle ordering rules (from Req 8.1–8.3): `mirror` before `infra apply` (ECS); `build` + `push` before `deploy apply` (ECS); `build` before `deploy apply` (compose)
      - Prerequisites: Dagger ≥ 0.20 (`build`/`push`/`mirror` all re-exec under `dagger run` if the session is absent); AWS credentials with `ecr:GetAuthorizationToken`, `ecr:BatchCheckLayerAvailability`, `ecr:PutImage`, `ecr:InitiateLayerUpload`, `ecr:UploadLayerPart`, `ecr:CompleteLayerUpload`, `ecr:CreateRepository`, `ecr:DescribeRepositories`, `ecr:PutLifecyclePolicy`, `ecr:TagResource`, `ecr:ListTagsForResource`, `ecr:GetLifecyclePolicy` for `push` and `mirror`
      - Short "Adding a new image" pointer: see `platforms/{compose,ecs}/src/images/` and the `Image` trait in `tokeira-deploy-engine`
    - **Edit 4 — Audit `## Quick Start`** for any compose example. If one exists, align it with the image-build-first ordering from Edit 2. If Quick Start currently covers only local (bare-process) deployment, no change is needed here — local has no image step
    - _Requirements: 10.2.1_

  - [ ] 11.2 Update `AGENTS.md` with three specific additions
    - **Addition 1** — In `## Working Agreements`, append a new sub-section titled `### Adding a New Image` after the existing `### Adding a New CLI Command`:
      1. Decide which platform(s) need the image (compose, ECS, or both)
      2. In each owning platform's `src/images/` module, declare a struct implementing `tokeira_deploy_engine::image::Image`
      3. Add the struct to that submodule's `all()` function (e.g., `images::tokeirad::all()` or `images::observability::all()`)
      4. If the image's remote ref is referenced by config, override `writeback_targets(ctx)` to list the dotted TOML keys
      5. Add property-test coverage if `desired_ref` or `writeback_targets` logic is non-trivial
      6. If the image needs a new build recipe (not just `tokeirad`), add a free function to `tokeira-build` with its own hardcoded Dagger pipeline
    - **Addition 2** — In the existing `### Adding a New CLI Command` checklist, append a pointer to the image-lifecycle spec as an example of multi-file CLI additions (clap variant, handler module, main.rs wiring, re-exec helper for dagger session)
    - **Addition 3** — In `## Observability Stack (Compose Platform)`, append a bullet noting that the six mirror images (Mimir, Loki, Grafana, Alloy, AwsCli, BusyBox) are declared in each platform's `src/images/observability/mod.rs` via a platform-local `mirror_image!` macro. Version bumps are a one-line change in the platform's `ObservabilityConfig::default()` defaults (or the `default_<field>_image()` helpers for the newer `aws_cli_image` / `busybox_image` fields)
    - Also add a pinned-versions line for the new aws-cli and busybox images: `public.ecr.aws/aws-cli/aws-cli:latest`, `public.ecr.aws/docker/library/busybox:latest`
    - _Requirements: 10.2.2_

  - [ ] 11.3 Update prototypical configs for `tkr deployment create`
    - For `--platform ecs`: populate `observability.aws_cli_image` and `observability.busybox_image` with their upstream source defaults so `tkr image mirror` has something to mirror on first run
    - For `--platform compose`: same two fields; also ensure `tokeirad.image = "tokeirad:latest"` is emitted (not `"tokeirad:local"`)
    - Ensure the generated `deployment.toml` carries helpful comments (e.g., `# populated by \`tkr image mirror\``)
    - _Requirements: 7.1, 7.5_

  - [ ]* 11.4 Integration test: build the tokeirad image end-to-end
    - Gated behind the `integration-test` feature flag
    - Run `tkr image build` against the workspace, assert `docker image inspect tokeirad:latest` succeeds
    - Test location: `apps/tkr/tests/image_build.rs`
    - Documented as skipped in the default test suite per AGENTS.md testing guidance

  - [ ]* 11.5 Integration test: mirror canonical images into LocalStack ECR
    - Gated behind the `integration-test` feature flag
    - Start LocalStack with ECR service enabled
    - Run `tkr image mirror` against a test deployment pointing at LocalStack
    - Assert all six expected repositories exist per platform, each with the canonical lifecycle policy
    - Re-run `tkr image mirror` and assert the repository set and `deployment.toml` contents are unchanged
    - Test location: `apps/tkr/tests/image_mirror.rs`

  - [ ] 11.6 Final checkpoint — full workspace verification
    - Run `cargo +nightly fmt --all --check`
    - Run `cargo lint`
    - Run `cargo test-lint`
    - Run `cargo check --workspace`
    - Run `cargo test --workspace`
    - Run `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`
    - All commands must pass with zero warnings
