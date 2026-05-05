# Implementation Plan: Image Lifecycle

## Overview

Introduce image-plane capabilities to Tokeira: a `tokeira-build` library crate driving reproducible `tokeirad` image builds through a Dagger pipeline, an `EcrRepository` resource in `tokeira-aws` with the canonical "keep last 10 untagged" lifecycle policy, a `tkr image build|push|mirror` command group, and lifecycle gates in the ECS and compose platforms that refuse to apply when images have not been prepared.

Target crates:
- `crates/dagger-client/` — NEW in-repo GraphQL client for Dagger sessions (mirrors EKS reference)
- `crates/tokeira-build/` — NEW library crate with `build_tokeirad_image`, `publish_image`, `mirror_image`, `mirror_mappings`
- `crates/tokeira-aws/` — NEW `EcrRepository` resource + `EcrClient` trait and default impl over `aws-sdk-ecr`
- `apps/tkr/` — NEW `image` command group (`build`, `push`, `mirror`), writeback via [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) helpers
- `platforms/compose/` — extend `ObservabilityConfig` with `aws_cli_image`, `busybox_image`; add build-gate on `deploy apply`
- `platforms/ecs/` — extend `ObservabilityConfig` with `aws_cli_image`, `busybox_image`; add mirror-gate on `infra apply`; add push-gate on `deploy apply`

Crucially, this plan does **not** introduce a new IaC module for ECR repositories, a Dockerfile templater, a second TOML-edit code path, or any tool that duplicates an existing workspace concern.

## Tasks

- [ ] 1. Bootstrap `crates/dagger-client/`
  - [ ] 1.1 Port the reference `dagger-client` implementation into the workspace
    - THE complete reference implementation is provided in [`reference/`](reference/) alongside a README covering port mechanics and what to change vs. what to leave untouched
    - Create `crates/dagger-client/Cargo.toml`, `crates/dagger-client/src/lib.rs`, and `crates/dagger-client/tests/quote_tests.rs` by copying `reference/Cargo.toml`, `reference/lib.rs`, and `reference/quote_tests.rs` respectively
    - Add `"crates/dagger-client"` to the workspace `[workspace.members]` list in the root `Cargo.toml`
    - Replace the reference `Cargo.toml` dependency versions with `workspace = true` entries where the workspace already pins the dependency (`serde`, `serde_json`, `base64`, `reqwest`, `proptest`, `eyre`). If a pin is missing from `[workspace.dependencies]`, add it at the version in the reference
    - Update the doc-comment example in `lib.rs` from `dsqld-build` to `tokeira-build`
    - Follow [`reference/README.md`](reference/README.md) for the full list of "do not change" items (query strings, `quote` helper, `container_op!` macro, `export_image` docker-load flow, 600s timeout) and "must change" items (doc examples)
    - _Requirements: 2.2_

  - [ ]* 1.2 Write unit test for session env-var detection
    - Unset `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN`, assert `Client::from_env()` returns an error
    - Set both to dummy values, assert `Client::from_env()` succeeds (without making a request)
    - Test location: `crates/dagger-client/src/lib.rs` `#[cfg(test)]` module
    - _Requirements: 2.1_

- [ ] 2. Scaffold `crates/tokeira-build/`
  - [ ] 2.1 Add the crate to the workspace
    - Create `crates/tokeira-build/Cargo.toml` with `thiserror`, `tracing`, `toml`, `serde`, `serde_json`, `eyre`, and path-dep on `crates/dagger-client`
    - Add `"crates/tokeira-build"` to `[workspace.members]` in the root `Cargo.toml`
    - _Requirements: 1.1, 10.3_

  - [ ] 2.2 Define `Arch`, request types, and `BuildError`
    - In `crates/tokeira-build/src/lib.rs`, define `Arch { Arm64, Amd64 }` with `rust_target()`, `platform()`, `FromStr`
    - Define `TokeiradBuildRequest { workspace_root, arch, tag }`, `TokeiradBuildResult { local_tag, arch, toolchain_version }`, `PublishRequest`, `PublishResult`, `PublishedReference`, `MirrorRequest`
    - Define `BuildError` with variants `ToolchainFile`, `ToolchainParse`, `UnsupportedArch`, `DaggerMissing`, `Publish`, `Mirror`, `Validation`
    - All public types derive `Debug`. Serializable types derive `Serialize, Deserialize`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [ ]* 2.3 Write property test for `Arch` parsing (Property 5)
    - **Property 5: Arch Parsing Rejects Unknown Values**
    - **Validates: Requirements 1.3.1, 1.3.2, 1.3.3**
    - Generate arbitrary strings via `proptest`. For strings in `{"arm64", "amd64"}`, assert `Arch::from_str(s)` returns `Ok(_)` and `as_str()` round-trips to `s`. For all other strings, assert `Arch::from_str(s)` returns `Err(BuildError::UnsupportedArch { supplied })` where `supplied == s`
    - Test location: `crates/tokeira-build/src/lib.rs` `#[cfg(test)]` module
    - Minimum 256 iterations

  - [ ] 2.4 Define the `DaggerClient` trait in `tokeira-build`
    - Create `crates/tokeira-build/src/dagger.rs` with the traits from the Design doc (`DaggerClient`, `ContainerRef`, `DirectoryRef`, `FileRef`, `SecretRef`)
    - The default implementation wraps the `dagger_client::Client` from task 1, adapting its session primitives to the trait
    - Implement a `MockDaggerClient` in `#[cfg(test)]` that records call sequences and returns canned responses; to be reused across tests
    - _Requirements: 2.2, 10.1_

- [ ] 3. Implement the `tokeirad` image build pipeline
  - [ ] 3.1 Resolve `rust-toolchain.toml`
    - In `crates/tokeira-build/src/toolchain.rs`, add `fn rust_toolchain_version(workspace_root: &Path) -> Result<String, BuildError>` that reads `rust-toolchain.toml`, parses it via `toml`, extracts the `[toolchain] channel` (or `version`) field, and returns the version string
    - Map I/O errors to `BuildError::ToolchainFile`; map parse errors to `BuildError::ToolchainParse`
    - _Requirements: 1.2.2, 9.1.5_

  - [ ] 3.2 Implement `build_tokeirad_image`
    - In `crates/tokeira-build/src/tokeirad.rs`, implement the three-stage pipeline described in the Design doc
    - Stage 1: create a `rust:{toolchain}-alpine` container, install musl-dev/openssl-dev/pkgconfig/protobuf-dev/protoc, copy the workspace, `rustup target add <arch-target>`, `cargo build --release --target <arch-target> --bin tokeirad -p tokeirad`, extract the built binary file
    - Stage 2: create an `alpine:3.23` container, install ca-certificates + tzdata, create user/group `tokeirad` (UID/GID 1000), copy the binary to `/usr/local/bin/tokeirad`, `with_user("tokeirad")`, `with_entrypoint(["/usr/local/bin/tokeirad"])`
    - Stage 3: `export_image(&format!("tokeirad:{tag}"))`
    - Return `TokeiradBuildResult` with `local_tag = format!("tokeirad:{tag}")`, `arch`, `toolchain_version`
    - _Requirements: 1.2.1, 1.2.3, 1.2.4, 1.2.5, 1.2.6, 1.4.1, 1.4.2, 1.4.3_

  - [ ]* 3.3 Write unit test for build invocation sequence
    - Use `MockDaggerClient` to record the call sequence: assert `container_from("rust:{toolchain}-alpine")`, `rustup target add {rust_target}`, `cargo build --release --target {rust_target} --bin tokeirad`, `container_from("alpine:3.23")`, `with_user("tokeirad")`, `with_entrypoint(["/usr/local/bin/tokeirad"])`, `export_image("tokeirad:{tag}")` are all present in order
    - Test location: `crates/tokeira-build/src/tokeirad.rs` `#[cfg(test)]` module
    - _Requirements: 1.2.1, 1.2.3, 1.2.4, 1.2.5, 1.2.6_

  - [ ] 3.4 Implement `publish_image`
    - In `crates/tokeira-build/src/publish.rs`, implement `publish(request, dagger)`: set secret, `container_from(&local_image)`, `with_registry_auth(registry, username, &secret)`, loop over `remote_refs` calling `publish(remote)`, collect results into `PublishResult`
    - Map any error to `BuildError::Publish { remote_ref, source }` where `remote_ref` is the failing ref
    - _Requirements: 1.5_

  - [ ] 3.5 Implement `mirror_image`
    - In `crates/tokeira-build/src/mirror.rs`, implement `mirror(request, dagger)`: set secret, `container_from(&source_ref)`, `with_registry_auth(registry, username, &secret)`, `publish(&remote_ref)`, return `PublishedReference`
    - Map any error to `BuildError::Mirror { source_ref, remote_ref, source }`
    - _Requirements: 1.6_

  - [ ]* 3.6 Write property test for publish reference count (Property 3)
    - **Property 3: Publish Reference Count**
    - **Validates: Requirements 1.5.4, 1.5.5**
    - Generate `Vec<String>` of length 1–16 of valid-looking remote refs via `proptest`
    - Assert `publish_image(&request, &mock)` returns `PublishResult { published }` with `published.len() == request.remote_refs.len()` and `published[i].remote_ref == request.remote_refs[i]` for all `i`
    - Test location: `crates/tokeira-build/src/publish.rs` `#[cfg(test)]` module
    - Minimum 100 iterations

  - [ ] 3.7 Checkpoint — Ensure the workspace compiles
    - Run `cargo lint` and `cargo check --workspace`; verify `tokeira-build` and `dagger-client` compile with no warnings

- [ ] 4. Implement canonical mirror mapping table
  - [ ] 4.1 Extend `ComposeConfig::observability` with `aws_cli_image` and `busybox_image`
    - In `platforms/compose/src/config.rs`, add `pub aws_cli_image: String` and `pub busybox_image: String` to `ObservabilityConfig`
    - Add `#[serde(default)]` so existing `deployment.toml` files without these fields still parse
    - Update `ObservabilityConfig` Default with `aws_cli_image = "public.ecr.aws/aws-cli/aws-cli:latest"` and `busybox_image = "public.ecr.aws/docker/library/busybox:latest"`
    - _Requirements: 5.1.6, 5.2.1_

  - [ ] 4.2 Extend `EcsConfig.observability` with the same two fields
    - In `platforms/ecs/src/config.rs`, add `aws_cli_image` and `busybox_image` to its `ObservabilityConfig`. Same defaults as compose
    - Update any prototypical-config generation helpers that produce `deployment.toml` for ECS so both new fields appear with their upstream source defaults
    - _Requirements: 5.1.6, 6.3.3_

  - [ ] 4.3 Implement `mirror_mappings(config, registry)`
    - In `crates/tokeira-build/src/mirror_map.rs`, implement the function described in the Design doc returning `Vec<MirrorMapping>` for the six canonical entries
    - Skip entries where the source field is empty
    - Skip entries where the source field already starts with `{registry}/{project}/{suffix}:` or `{registry}/{project}/{suffix}@`
    - Reuse the `image_tag` helper from the EKS reference (handles digest refs and host-with-port cases)
    - _Requirements: 5.1, 4.4.7_

  - [ ]* 4.4 Write property test for mirror mapping stability (Property 6)
    - **Property 6: Mirror Mapping Stability**
    - **Validates: Requirements 5.2.1, 5.2.2, 8.5**
    - No generation — direct assertion against `ComposeConfig::default()`
    - Convert the compose default observability section into an `EcsConfig` shape and call `mirror_mappings(&ecs_cfg, "<test-registry>")`
    - Assert exactly six entries with source refs equal to: `grafana/mimir:3.0.6`, `grafana/loki:3.7.1`, `grafana/grafana-oss:12.4.3`, `grafana/alloy:v1.16.0`, `public.ecr.aws/aws-cli/aws-cli:latest`, `public.ecr.aws/docker/library/busybox:latest`
    - Assert destination tags match source tags exactly
    - Test location: `crates/tokeira-build/src/mirror_map.rs` `#[cfg(test)]` module

  - [ ]* 4.5 Write property test for skip-already-mirrored (Property 7)
    - **Property 7: Mirror Mapping Skip-Already-Mirrored**
    - **Validates: Requirements 4.4.7, 5.1.5**
    - Generate an `EcsConfig` where every observability field is set to `{registry}/{project}/{suffix}:{random-tag}` with the correct suffix per field
    - Assert `mirror_mappings(&config, registry)` returns an empty vector
    - Test location: `crates/tokeira-build/src/mirror_map.rs` `#[cfg(test)]` module
    - Minimum 100 iterations

  - [ ] 4.6 Checkpoint — Ensure the workspace compiles with config additions
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-build -p tokeira-compose` (unit tests only)

- [ ] 5. Implement `EcrRepository` resource and `EcrClient` trait in `tokeira-aws`
  - [ ] 5.1 Define the `EcrClient` trait
    - In `crates/tokeira-aws/src/clients/ecr.rs`, define the trait from the Design doc (`get_authorization_token`, `describe_repository`, `create_repository`, `delete_repository`, `put_lifecycle_policy`, `get_lifecycle_policy`, `tag_resource`)
    - Define `EcrAuthorization`, `RepositoryDescription`, `ImageTagMutability`, `EcrError` with variants including `NotFound` and `InvalidToken`
    - Implement the default over `aws-sdk-ecr` with `#[async_trait]`
    - Add `aws-sdk-ecr` and `base64` to `crates/tokeira-aws/Cargo.toml`
    - _Requirements: 3.1, 9.2_

  - [ ] 5.2 Implement the ECR authorization decoder
    - In `crates/tokeira-aws/src/clients/ecr.rs`, implement `fn decode_authorization_data(token_b64, proxy_endpoint) -> Result<EcrAuthorization, EcrError>` mirroring the EKS reference
    - The decoder validates base64 decoding, UTF-8 decoding, presence of a `:` separator, and trims `http(s)://` and trailing `/` from the proxy endpoint
    - _Requirements: 9.2_

  - [ ]* 5.3 Write unit tests for the authorization decoder
    - Four tests mirroring the EKS reference: success, invalid base64, invalid UTF-8, missing `:`
    - Each test constructs a canned `(token_b64, proxy_endpoint)` input and asserts the exact error variant on failure or the exact `EcrAuthorization` on success
    - Test location: `crates/tokeira-aws/src/clients/ecr.rs` `#[cfg(test)]` module
    - _Requirements: 9.2_

  - [ ] 5.4 Implement `EcrRepository` resource
    - In `crates/tokeira-aws/src/resources/ecr_repository.rs`, define `EcrRepository { name, tags }` with `#[derive(Debug, Clone, Serialize, Deserialize)]`
    - Define `ECR_LIFECYCLE_POLICY` constant as the canonical JSON from the Design doc
    - Implement `Resource` trait: `create` (create repo with `MUTABLE` + apply lifecycle policy), `update` (re-apply lifecycle policy + tags), `delete` (force-delete), `describe` (return `None` on `NotFound`), `diff` (policy drift and tag drift signal updates), `dependencies` (empty)
    - Implement a constructor `EcrRepository::new(name: &str, tags: BTreeMap<String, String>) -> Result<Self, EcrError>` that validates the name against the ECR grammar: 2–256 characters, `[a-z0-9._/-]+`, not starting or ending with `/` or `.`
    - _Requirements: 3.1, 3.2, 3.3_

  - [ ]* 5.5 Write unit tests for `EcrRepository` resource methods
    - Construct a `MockEcrClient` that records calls and serves canned responses
    - Unit-test `create`, `update`, `delete`, `describe`, `diff` each with a focused scenario (repo absent on describe, policy drift signals update, tags drift signals update, delete calls force-delete)
    - Test location: `crates/tokeira-aws/src/resources/ecr_repository.rs` `#[cfg(test)]` module
    - _Requirements: 3.1.1, 3.1.2, 3.1.4, 3.1.5, 3.1.6_

  - [ ]* 5.6 Write property test for ECR name grammar (Property 9)
    - **Property 9: ECR Name Grammar Validation**
    - **Validates: Requirements 3.1.2, 3.3.4**
    - Generate arbitrary ASCII-ish strings via `proptest` with length 0..260 and a character pool covering grammar + invalid characters
    - Assert `EcrRepository::new(s, BTreeMap::new())` succeeds iff `s` passes the documented grammar (length 2..=256, chars in `[a-z0-9._/-]`, not starting with `/` or `.`)
    - Test location: `crates/tokeira-aws/src/resources/ecr_repository.rs` `#[cfg(test)]` module
    - Minimum 256 iterations

  - [ ]* 5.7 Write property test for lifecycle policy JSON round-trip (Property 4)
    - **Property 4: Lifecycle Policy JSON Round-Trip**
    - **Validates: Requirements 3.2, 8.3**
    - Parse `ECR_LIFECYCLE_POLICY` with `serde_json::from_str::<serde_json::Value>`, serialize with `serde_json::to_string`, re-parse
    - Assert the two parsed `Value`s are equal
    - Test location: `crates/tokeira-aws/src/resources/ecr_repository.rs` `#[cfg(test)]` module
    - _Requirements: 3.2, 8.3_

  - [ ] 5.8 Implement `ensure_ecr_repository` and `ensure_ecr_repositories` helpers
    - In `crates/tokeira-aws/src/clients/ecr.rs`, add `async fn ensure_ecr_repository(ecr: &dyn EcrClient, name: &str, tags: &BTreeMap<String, String>) -> Result<(), EcrError>` that describes first, creates if absent, then (always) applies the lifecycle policy
    - Add `async fn ensure_ecr_repositories(ecr: &dyn EcrClient, repos: &[(String, BTreeMap<String, String>)]) -> Result<(), EcrError>` that calls the single-repo helper in sequence
    - _Requirements: 3.4_

  - [ ]* 5.9 Write property test for repository creation idempotence (Property 2)
    - **Property 2: ECR Repository Creation Idempotence**
    - **Validates: Requirements 3.4.1, 3.4.2, 3.4.3, 3.4.4, 8.2**
    - Generate `Vec<(String, BTreeMap<String, String>)>` of length 0..20 with distinct grammar-valid names
    - Call `ensure_ecr_repositories` twice with the same input against a shared `MockEcrClient`
    - Assert that after the second call, the mock has the same set of repositories as after the first, each with the same lifecycle policy and the same tags
    - Test location: `crates/tokeira-aws/src/clients/ecr.rs` `#[cfg(test)]` module
    - Minimum 64 iterations

  - [ ] 5.10 Checkpoint — Ensure the workspace compiles
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tokeira-aws` (unit tests only)

- [ ] 6. Wire the `tkr image` command group
  - [ ] 6.1 Add `ImageCommand` enum to `apps/tkr/src/cli.rs`
    - Add `Image(ImageArgs)` variant to the top-level `Command` enum, positioned between `Deployment` and `Infra`
    - Define `ImageArgs` with a subcommand field bound to `ImageCommand { Build { arch, tag }, Push { tag, yes }, Mirror { yes } }`
    - Wire the subcommand into the help text
    - _Requirements: 4.1_

  - [ ] 6.2 Implement the build handler
    - Create `apps/tkr/src/commands/image.rs`. Implement `run(cmd, deployment, format)` from the Design doc
    - For `Build { arch, tag }`: parse `arch` via `Arch::from_str`, resolve workspace root, call `rust_toolchain_version`, construct `TokeiradBuildRequest`, delegate to `tokeira_build::build_tokeirad_image`
    - Implement Dagger re-exec: check `DAGGER_SESSION_PORT` and `DAGGER_SESSION_TOKEN`; if either is missing, spawn `dagger run -- <current_exe> <args>` with the same flags
    - Report progress via the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) callback surface: a start event for each stage (toolchain resolve, compile, assemble, export), a complete event on success, a failed event on error
    - When `--json` is active, emit the progress events as JSON plus a final `{ "action": "build", "image": "tokeirad:<tag>", "arch": "<arch>" }` summary
    - Build does NOT require `--yes` or prompt for confirmation
    - _Requirements: 4.2, 2.1_

  - [ ] 6.3 Implement the push handler
    - In `apps/tkr/src/commands/image.rs`, implement the `Push { tag, yes }` branch
    - Confirm via the [`tkr-cli`](../tkr-cli/requirements.md) confirmation rules (interactive by default, `--yes` bypass, refuse on non-TTY without `--yes`)
    - Obtain an `EcrClient` via the default AWS SDK implementation; call `get_authorization_token`; derive registry host from the decoded proxy endpoint
    - Fail early with a descriptive error if the local `tokeirad:latest` image is not present in the Docker image store
    - Call `ensure_ecr_repository(ecr, &format!("{project}/tokeirad"), &tags)`
    - Construct a `PublishRequest` with `local_image = "tokeirad:latest"`, `remote_refs = [format!("{registry}/{project}/tokeirad:latest"), format!("{registry}/{project}/tokeirad:{tag}")]`
    - Call `tokeira_build::publish_image`
    - Writeback: for each of the seven ECS services (`edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`), call `iac_lifecycle::write_config_values(deployment_dir, &[(format!("services.{svc}.image"), version_tagged_ref)])`
    - Emit progress events and a final JSON summary event when `--json` is active
    - _Requirements: 4.3, 4.5, 6.2_

  - [ ] 6.4 Implement the mirror handler
    - In `apps/tkr/src/commands/image.rs`, implement the `Mirror { yes }` branch
    - Confirm per `tkr-cli` rules
    - Obtain `EcrClient`, call `get_authorization_token`, derive registry host
    - Compute `mappings = tokeira_build::mirror_mappings(&config, &registry)`
    - If empty, emit a success "nothing to mirror" message and return
    - Ensure each mapped repository exists via `ensure_ecr_repositories`
    - For each mapping, construct a `MirrorRequest` and call `tokeira_build::mirror_image`
    - Writeback: for each mapping, call `iac_lifecycle::write_config_values(deployment_dir, &[(mapping.field, mapping.destination_ref)])`
    - Emit progress events and a final JSON summary event when `--json` is active
    - _Requirements: 4.4, 4.5, 6.2_

  - [ ] 6.5 Wire the `image` command into `apps/tkr/src/main.rs`
    - Add a `Command::Image(args) => commands::image::run(args.command, &deployment, format).await?` arm
    - Thread the global `--json` flag into the handler (matches the threading added by [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) for the `infra` command)
    - _Requirements: 4.1.2_

  - [ ]* 6.6 Write unit tests for CLI parse
    - Parse `tkr image build` with default flags: assert `arch == "arm64"`, `tag == "local"`
    - Parse `tkr image build --arch amd64 --tag v1.2.3`: assert values match
    - Parse `tkr image push`: assert `tag == "latest"`, `yes == false`
    - Parse `tkr image push --tag v2026-03-21 --yes`: assert values match
    - Parse `tkr image mirror`: assert `yes == false`
    - Parse `tkr image mirror --yes`: assert `yes == true`
    - Test location: `apps/tkr/src/commands/image.rs` `#[cfg(test)]` module
    - _Requirements: 4.2, 4.3, 4.4_

  - [ ]* 6.7 Write property test for mirror idempotence (Property 1)
    - **Property 1: Mirror Idempotence**
    - **Validates: Requirements 4.4.5, 8.1**
    - Generate `EcsConfig` values with populated observability fields (tag generation bounded to a small set)
    - Call `run_mirror(cfg)` twice in sequence with a shared `MockDaggerClient` + `MockEcrClient`
    - Assert: (a) both calls succeed, (b) the mock's repository set after the second call equals the set after the first, (c) the `deployment.toml` contents after the second call equal the contents after the first
    - Test location: `apps/tkr/src/commands/image.rs` `#[cfg(test)]` module
    - Minimum 32 iterations (higher cost than other properties due to writeback I/O; use a tempdir per iteration)

  - [ ] 6.8 Checkpoint — Ensure the workspace compiles
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p tkr -p tokeira-build -p tokeira-aws` (unit tests only)

- [ ] 7. Add lifecycle gates in platform code
  - [ ] 7.1 Implement the ECS observability mirror gate
    - In `platforms/ecs/src/lib.rs`, add `fn validate_observability_mirrored(config: &EcsConfig, registry: &str) -> Result<(), EcsError>`
    - Walk `config.observability.{mimir_image, loki_image, grafana_image, alloy_image, aws_cli_image, busybox_image}`; collect any empty or non-`{registry}/`-prefixed fields
    - Return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image mirror`
    - Call this validator from `EcsDeployment::validate_for_apply` (invoked before `tkr infra apply`)
    - _Requirements: 7.1_

  - [ ]* 7.2 Write property test for observability gate (Property 10)
    - **Property 10: ECS Observability Gate Predicate**
    - **Validates: Requirements 7.1.1, 7.1.2**
    - Generate `EcsConfig` values with observability fields chosen from: empty, upstream source, project-scoped ECR ref
    - Assert `validate_observability_mirrored(config, registry)` returns `Err` iff at least one field is empty or not `{registry}/`-prefixed
    - Test location: `platforms/ecs/src/lib.rs` `#[cfg(test)]` module
    - Minimum 128 iterations

  - [ ] 7.3 Implement the ECS services push gate
    - In `platforms/ecs/src/lib.rs`, add `fn validate_services_pushed(config: &EcsConfig, registry: &str) -> Result<(), EcsError>`
    - Walk `config.services.{edge_api, edge_poll, runtime, projection, controller, autoscaler, admin}.image`; collect any empty or non-`{registry}/`-prefixed fields
    - Return an error listing the unpopulated or upstream-pointing fields and instructing the operator to run `tkr image push --tag <version>`
    - Call this validator from `EcsDeployment::validate_for_deploy_apply` (invoked before `tkr deploy apply`)
    - _Requirements: 7.2_

  - [ ]* 7.4 Write property test for services gate (Property 11)
    - **Property 11: ECS Services Gate Predicate**
    - **Validates: Requirements 7.2.1, 7.2.2**
    - Generate `EcsConfig` values with service image fields chosen from: empty, upstream source, project-scoped ECR ref
    - Assert `validate_services_pushed(config, registry)` returns `Err` iff at least one service image field is empty or not `{registry}/`-prefixed
    - Test location: `platforms/ecs/src/lib.rs` `#[cfg(test)]` module
    - Minimum 128 iterations

  - [ ] 7.5 Implement the compose build gate
    - In `platforms/compose/src/services.rs`, add a validation step invoked when building the Docker Compose service list
    - When `config.tokeirad.image == "tokeirad:local"`, query the bollard client for image existence (`images.inspect_image("tokeirad:local")`)
    - If absent, return an error instructing the operator to run `tkr image build`, including the exact command
    - When `config.tokeirad.image` is anything else, skip the check
    - _Requirements: 7.3_

  - [ ]* 7.6 Write unit tests for the compose build gate
    - With a fake bollard client returning `NotFound`, assert the gate returns an error with "tkr image build" in the message
    - With a fake client returning `Ok`, assert the gate returns `Ok(())`
    - With `config.tokeirad.image = "my-registry.example/tokeirad:custom"`, assert the gate returns `Ok(())` without querying bollard
    - Test location: `platforms/compose/src/services.rs` `#[cfg(test)]` module
    - _Requirements: 7.3_

  - [ ] 7.7 Checkpoint — Ensure the workspace compiles and all gates fire as expected
    - Run `cargo lint`, `cargo check --workspace`, `cargo test -p platforms-ecs -p platforms-compose` (unit tests only, names may be `tokeira-ecs` / `tokeira-compose` depending on the final crate naming — adjust as needed)

- [ ] 8. Integration and documentation
  - [ ] 8.1 Update `README.md`
    - Add a "Building and publishing images" section covering:
      - `tkr image build` — default produces `tokeirad:local` for compose
      - `tkr image build --arch amd64 --tag v1.2.3` — explicit flags
      - `tkr image push --tag <version>` — pushes latest + version, writes back to `services.*.image`
      - `tkr image mirror` — mirrors six pinned third-party images into project-owned ECR
      - Required prerequisites: Dagger >= 0.20 for `build` and `push`; AWS credentials with `ecr:*` permissions for `push` and `mirror`
    - Add a "Lifecycle order" subsection stating: `mirror` before `infra apply`, `build` + `push` before `deploy apply` (ECS), `build` before `deploy apply` (compose)
    - _Requirements: 10.2_

  - [ ] 8.2 Update `AGENTS.md`
    - Add the lifecycle ordering rules from Feature 7 to the "Working Agreements" section
    - Add a pointer from the "Adding a new service" checklist to the image-lifecycle spec for image requirements
    - _Requirements: 10.2_

  - [ ] 8.3 Update `tkr deployment create --platform ecs` prototypical config
    - In `platforms/ecs/src/config.rs` prototypical config generation, populate `observability.aws_cli_image` and `observability.busybox_image` with their upstream source defaults so `tkr image mirror` has something to mirror on first run
    - Ensure the generated `deployment.toml` carries helpful comments (e.g., `# populated by \`tkr image mirror\``)
    - _Requirements: 5.2, 6.3.3_

  - [ ]* 8.4 Integration test: build the tokeirad image end-to-end
    - Gated behind the `integration-test` feature flag
    - Run `tkr image build` against the workspace, assert `docker image inspect tokeirad:local` succeeds
    - Test location: `apps/tkr/tests/image_build.rs`
    - Documented as skipped in the default test suite per AGENTS.md testing guidance
    - _Requirements: 10.1.3_

  - [ ]* 8.5 Integration test: mirror canonical images into LocalStack ECR
    - Gated behind the `integration-test` feature flag
    - Start LocalStack with ECR service enabled
    - Run `tkr image mirror` against a test deployment pointing at LocalStack
    - Assert all six expected repositories exist, each with the canonical lifecycle policy
    - Re-run `tkr image mirror` and assert the repository set and `deployment.toml` contents are unchanged
    - Test location: `apps/tkr/tests/image_mirror.rs`
    - _Requirements: 10.1.3, 8.1_

  - [ ] 8.6 Final checkpoint — full workspace verification
    - Run `cargo +nightly fmt --all --check`
    - Run `cargo lint`
    - Run `cargo test-lint`
    - Run `cargo check --workspace`
    - Run `cargo test --workspace`
    - Run `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`
    - All commands must pass with zero warnings
