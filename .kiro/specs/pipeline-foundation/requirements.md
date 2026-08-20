# Requirements Document: Pipeline Foundation

## Introduction

Tokeira needs a repeatable, portable way to run continuous-integration work across build, lint, test, container publication, infrastructure apply dry-runs, and compatibility conformance. Today that work is scattered: some of it lives in `cargo` aliases, some in shell scripts in `dev/`, and anything that needs a live environment (container build, conformance, integration testing) has no home. The first piece of the model already exists: `tokeira-build` orchestrates image and provisioner builds on the vendored first-party Dagger Rust SDK (`dagger-sdk`, connected in-process — no wrapper process, no session environment variables). The in-flight [`temporal-compatibility`](../temporal-compatibility/requirements.md) spec will need a conformance runner that looks almost identical in shape.

This spec generalises the image-lifecycle pattern into a substrate that future pipelines build on. It sets up:

1. A convention for **Rust-authored pipelines** driven by the vendored `dagger-sdk` — no Dagger Modules, no `dagger.json`, no Dagger CLI module-call surface. Pipelines are library + binary crates under `crates/tokeira-pipeline-{name}/`.
2. A shared **pipeline runtime crate** (`tokeira-pipeline-runtime`) holding the common Dagger orchestration helpers (workspace mount, Rust toolchain container, `cargo` wrappers, container publish, artifact upload) so individual pipelines do not reinvent them.
3. A **policy-layer pattern**: wherever a pipeline has to decide what results *mean* (pass/fail, release gate, classification), that logic lives in a dedicated Rust crate on the non-Dagger side of the boundary. Pipelines execute; policy crates classify.
4. A **trigger layer** on **Buildkite Personal with Buildkite hosted Linux agents** as the primary substrate from day one. The trigger layer's only job is to install the Dagger CLI (already present on hosted agents) and invoke `cargo run -p tokeira-pipeline-{name} -- {subcommand}`. A parallel GitHub Actions template is retained as a parity exemplar so substrate independence is exercised, not as an active substrate.
5. An **artifact contract**: every pipeline run that produces operator-visible results writes a versioned JSON artifact to a known S3 location. `tkr` commands consume artifacts, not CI APIs.
6. A **local-parity rule**: every pipeline binary runs identically on a developer's workstation under `cargo run -p tokeira-pipeline-{name} -- {subcommand}` and on CI. There is no "works only on CI" logic.
7. A **conformance orchestration model** that stages what runs where: Tier-0 wire checks and `workspace`-pipeline Rust checks plus Tier-3 Tokeira-owned smoke tests run on every PR; Tier-1 `temporalio/features` against tokeirad runs as a **manually-triggered** Buildkite pipeline until the maintainer judges it ready for PR gating; Tier-2 full SDK integration suites are deferred until Tokeira implements enough of the Temporal surface that their result is signal rather than noise.

The spec is substrate-only for plumbing. It does not define any specific pipeline (no `tokeirad` build pipeline, no conformance pipeline, no release pipeline). It does define **when** each conformance tier is expected to run, because that choice is a substrate concern (PR vs manual vs deferred) rather than a per-pipeline concern. The semantic interpretation of each tier — what "passing T1" means, which scenarios are classified which way — is owned by [`temporal-compatibility`](../temporal-compatibility/requirements.md).

### What this spec delivers

- A `tokeira-pipeline-runtime` Rust crate at `crates/tokeira-pipeline-runtime/` holding the common Dagger orchestration helpers, including `CiContext` resolution from environment variables with local fallbacks.
- A `tokeira-ci-policy` Rust crate at `crates/tokeira-ci-policy/` holding the shared artifact envelope, policy exit codes, and artifact reader/writer.
- A per-pipeline crate template documented in `dev/pipelines/README.md` describing the `crates/tokeira-pipeline-{name}/` layout every pipeline follows.
- A **Buildkite pipeline at `.buildkite/pipeline.yml` as the primary, live trigger**. Targets a Buildkite hosted Linux agent queue. Dagger Cloud is off by default; if it is ever enabled it MUST be the free individual tier, not a paid team tier.
- A **GitHub Actions workflow at `.github/workflows/ci.yml` kept as a parity template**, exercising the same pipeline binaries directly. It is either disabled (workflow not registered) or runs in a shadow mode that does not gate merges. Its purpose is to prove substrate independence mechanically, not to act as the day-to-day CI.
- An `ArtifactBucket` resource in `tokeira-aws` (S3 bucket with the shared-bucket semantics from [`ecs-deployment`](../ecs-deployment/requirements.md) `RemoteStateBucket`) for pipeline outputs.
- A `tkr pipeline` command group with subcommands `list`, `run`, `artifacts` for local invocation and artifact inspection.
- The first pipeline to ship under the new substrate: a `workspace` pipeline that runs `cargo fmt --check`, `cargo lint`, `cargo test-lint`, `cargo check --workspace`, `cargo test --workspace`, and `cargo doc --workspace`. This pipeline becomes the canonical reference for "how a pipeline looks" and replaces whatever ad-hoc workspace CI exists today.
- A **manually-triggered** `tokeira-pipeline-conformance-features` Buildkite pipeline that runs `temporalio/features` against a freshly-built tokeirad. Manual trigger is the starting point; migration to PR gating happens per the criteria in Feature 10.

### What this spec does NOT cover

- The container build, conformance tier semantics, or release pipelines themselves. Container builds are introduced by [`image-lifecycle`](../image-lifecycle/requirements.md). The _meaning_ of each conformance tier (what T0/T1/T2/T3 contain, which scenarios are classified which way) is owned by [`temporal-compatibility`](../temporal-compatibility/requirements.md); this spec defines only _when each tier runs_ (Feature 10). Release automation is out of scope.
- Self-hosted Buildkite agents. The spec intentionally ships with Buildkite hosted Linux agents and defers the Elastic CI Stack for AWS (or any other self-hosted agent story) until hosted-agent minute cost becomes operationally annoying. When that happens, a follow-up `buildkite-agents` spec SHALL handle it.
- Dagger Cloud paid tiers. Dagger Cloud is disabled by default. The free individual tier MAY be enabled for local developer traces; the paid team tier SHALL NOT be enabled without a spec amendment.
- Deployment orchestration from CI. `tkr deploy apply` remains operator-initiated; pipelines never call it.
- Dagger Modules (`dagger.json`, `dagger call`, module composition). Tokeira drives Dagger via the vendored `dagger-sdk` crate. Adopting the Dagger Module system is a future decision, not a requirement here.
- Caching infrastructure design beyond declaring that the pipeline runtime exposes Dagger cache mounts through the shared helpers. Cache-key schemas per pipeline are that pipeline's concern.

### Cross-references

- [`image-lifecycle`](../image-lifecycle/requirements.md): First consumer of the Dagger boundary. The `tokeira-build` crate becomes the policy crate for a future `tokeira-pipeline-image` following the template in this spec.
- [`temporal-compatibility`](../temporal-compatibility/requirements.md): Second consumer. Its conformance runner (T0–T3 tiers) will live as `crates/tokeira-pipeline-conformance/` with policy decisions made by a `tokeira-conformance` crate.
- [`ecs-deployment`](../ecs-deployment/requirements.md): Provides the S3 shared-bucket pattern (via the shared `RemoteStateBucket` resource) that the `ArtifactBucket` in this spec parallels. Also defines the canonical tagging requirement this spec inherits.
- [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md): The `ArtifactBucket` is an IaC resource and follows the describe-before-delete, progress-callback, and writeback patterns owned there.
- [`tkr-cli`](../tkr-cli/requirements.md): Adds the `pipeline` command group to the existing command tree.

## Glossary

- **Pipeline**: A named unit of CI work implemented as a Rust library + binary crate at `crates/tokeira-pipeline-{name}/`. Examples: `workspace`, `image`, `conformance`, `release`.
- **Pipeline_Crate**: The crate at `crates/tokeira-pipeline-{name}/`. Exposes a library for reuse and a binary (`cargo run -p tokeira-pipeline-{name} -- {subcommand}`) for CI invocation.
- **Subcommand**: A CLI argument on a pipeline binary naming one of the pipeline's entry points. Every Pipeline_Crate binary SHALL accept at minimum `check`, `test`, `build`, `artifact`.
- **Pipeline_Runtime**: The `tokeira-pipeline-runtime` crate. Exposes shared Dagger orchestration helpers (workspace mount, Rust toolchain container, `cargo` wrappers, container publish, artifact upload) plus Ci_Context resolution. Consumed by every Pipeline_Crate.
- **Policy_Crate**: A Rust crate that owns the interpretation of a pipeline's outputs. Example: `tokeira-build` (image-lifecycle), `tokeira-conformance` (temporal-compatibility). Small pipelines may roll their policy into the Pipeline_Crate itself. Larger pipelines separate.
- **Ci_Policy**: The `tokeira-ci-policy` crate. Exposes the shared `CiContext` struct, `ArtifactEnvelope<T>`, `ExitCode` enum, and artifact read/write helpers. Consumed by every Policy_Crate.
- **Dagger_Client**: The vendored `dagger-sdk` crate from [`image-lifecycle`](../image-lifecycle/requirements.md). Wraps a Dagger session's GraphQL interface. Pipelines use it to construct containers, run execs, publish images, and upload artifacts.
- **CI_Substrate**: The hosted runner that triggers a pipeline. Initially GitHub Actions; designed to be replaceable with Buildkite without changing pipeline source.
- **Trigger_Workflow**: A thin CI-substrate file (`.github/workflows/ci.yml`, `.buildkite/pipeline.yml`) whose only job is to check out the repo, install the Dagger CLI, and invoke `cargo run -p tokeira-pipeline-{name} -- {subcommand}`. Contains no business logic beyond matrix/parallelism control.
- **Artifact**: A JSON document produced by a pipeline run and written to the Artifact_Bucket. Every artifact conforms to a versioned schema. Readable by `tkr` commands.
- **Artifact_Bucket**: An S3 bucket owned by a Tokeira deployment, used to store pipeline artifacts. Keyed by `{project}/pipelines/{pipeline}/{git_sha}/{run_id}.json`.
- **Artifact_Schema**: The versioned JSON schema for artifacts. Every artifact has a `schema_version: u32` field at the top level; readers refuse to parse unknown major-version artifacts.
- **Run_ID**: An opaque string identifying a single pipeline invocation. Set by the CI substrate (GHA run id, Buildkite build number, or `local-{timestamp}-{random}` for local runs).
- **Local_Engine**: A Dagger engine running on the developer's workstation (via the `dagger` CLI). Every pipeline runs against a local engine with the same code that runs in CI.
- **Ci_Context**: The runtime context a pipeline sees: git SHA, branch name, CI substrate name, run ID, actor identity, a monotonic run-start instant. Provided as environment variables read by the `Pipeline_Runtime::ci_context()` helper; pipelines do not hard-code runner-specific lookups.
- **Buildkite_Agent**: A Buildkite hosted Linux agent. Managed by Buildkite, ephemeral, destroyed after each job, Docker and common CLIs pre-installed. Self-hosted agents are out of scope for this spec.
- **Buildkite_Hosted_Queue**: The Buildkite queue that hosted Linux agents pull from. The spec uses a single queue (`hosted-linux-default`) until job volume or cost justifies further segmentation.
- **Dagger_Cloud**: Dagger's optional remote trace/cache service. Disabled by default in this spec. MAY be enabled at the free individual tier for local traces; SHALL NOT be enabled at any paid team tier without a spec amendment.
- **Conformance_Tier**: A compatibility-evidence level defined by [`temporal-compatibility`](../temporal-compatibility/requirements.md). The tier numbering (T0/T1/T2/T3) and scenario-classification rules live in that spec; this spec only decides where each tier runs (PR gate, manual trigger, deferred).
- **Manual_Pipeline**: A Buildkite pipeline configured so that builds are only created on explicit operator action (Buildkite "New Build" button or `buildkite-agent pipeline upload` from another pipeline), not on branch push or PR.

## Requirements

---

## Feature 1: Pipeline Crate Convention

### Requirement 1.1: Pipeline crate layout

**User Story:** As a Tokeira developer, I want every pipeline to live in a predictable place with a predictable structure, so that adding a new pipeline or understanding an existing one is a one-directory operation.

#### Acceptance Criteria

1. EVERY Pipeline SHALL live at `crates/tokeira-pipeline-{name}/` where `{name}` is lowercase kebab-case matching `[a-z][a-z0-9-]{1,31}`.
2. EACH Pipeline_Crate SHALL have a `Cargo.toml` declaring both a library (`[lib]`) target and a binary (`[[bin]]`) target with `name = "tokeira-pipeline-{name}"`.
3. EACH Pipeline_Crate SHALL have a `README.md` documenting the pipeline's subcommands, secrets (per Req 7.1), and expected duration per subcommand.
4. Pipeline_Crates SHALL depend on `dagger-sdk`, `tokeira-pipeline-runtime`, and `tokeira-ci-policy` via path dependencies. They MAY depend on a dedicated Policy_Crate.
5. Pipeline_Crate binaries SHALL NOT depend on `apps/tkr` — the dependency flows the other way. `tkr pipeline run` invokes Pipeline_Crate binaries through `cargo run` or via a compiled-in dispatcher.
6. THE root `dev/pipelines/README.md` SHALL document the Pipeline_Crate convention, the Subcommand contract, the Artifact_Schema, the local-parity rule, and the Buildkite vs GitHub Actions parity expectation.

### Requirement 1.2: Authored in Rust

**User Story:** As a Tokeira developer, I want pipelines authored in the same language as the rest of the codebase, so that pipeline source is readable by every contributor without learning a second ecosystem.

#### Acceptance Criteria

1. EVERY Pipeline_Crate SHALL be written in Rust.
2. Pipeline_Crates SHALL drive Dagger via the vendored `dagger-sdk` crate. They SHALL NOT use the official Dagger Go, TypeScript, Python, or Rust SDK, SHALL NOT introduce a `dagger.json`, and SHALL NOT be invoked via `dagger call`.
3. The invocation path from CI is `cargo run -p tokeira-pipeline-{name} --release -- {subcommand} [args]`. The binary owns its session: `dagger_sdk::connect()` provisions and authenticates in-process; engine selection is configuration (the pinned engine + CLI pair via the two engine-selection variables), never a wrapper process.
4. Non-Rust helper scripts (Python, shell) SHALL NOT be introduced by this foundation. If a future pipeline genuinely needs a non-Rust helper (for example, invoking a Python SDK's own test harness), the helper SHALL be called from inside a Dagger-orchestrated container, not from the host runner.

### Requirement 1.3: Subcommand contract

**User Story:** As a Tokeira developer wiring a new pipeline into CI, I want a guaranteed set of subcommands on every pipeline binary, so that the CI trigger workflow can generically invoke them without per-pipeline customisation.

#### Acceptance Criteria

1. EVERY Pipeline_Crate binary SHALL expose the following subcommands via `clap` (the workspace-standard arg parser):
   - `check` — fast verification: format, lint, compile. Exits non-zero on any finding.
   - `test` — run the pipeline's full test suite. Exits non-zero on any failing test.
   - `build` — produce the pipeline's primary artifact (container image, binary, or JSON document).
   - `artifact` — emit the pipeline's machine-readable artifact to stdout or to `--output <path>`. MUST conform to Artifact_Schema.
2. Pipelines MAY expose additional subcommands (for example, `tokeira-pipeline-image push`, `tokeira-pipeline-conformance tier1`). Additional subcommands SHALL be documented in the Pipeline_Crate's `README.md`.
3. WHEN a subcommand is not applicable to a pipeline (for example, a lint-only pipeline has no `build`), the subcommand SHALL still exist and SHALL return immediately with a no-op message and exit status 0. This keeps the CI trigger workflow uniform.
4. EVERY subcommand SHALL complete (in local-engine reference conditions) within a documented budget: `check` ≤ 2 minutes, `test` ≤ 10 minutes, `build` ≤ 15 minutes, `artifact` ≤ 30 seconds. Pipelines whose work exceeds a budget SHALL split into additional subcommands.
5. WHEN a subcommand fails, THE exit status SHALL be non-zero (per `tokeira-ci-policy::ExitCode`) and stderr SHALL include at minimum a one-line reason citing the pipeline name and the subcommand.
6. Pipeline_Crate binaries SHALL support a global `--json` flag that switches progress output to JSON events consistent with the [`iac-resource-lifecycle`](../iac-resource-lifecycle/requirements.md) JSON event schema.

### Requirement 1.4: Local-parity rule

**User Story:** As a Tokeira developer, I want every pipeline to run identically on my workstation and in CI, so that "works on CI, fails locally" and "works locally, fails on CI" are both impossible by construction.

#### Acceptance Criteria

1. EVERY Pipeline_Crate binary SHALL be runnable via `cargo run -p tokeira-pipeline-{name} -- {subcommand}` against a Local_Engine.
2. Pipeline_Crates SHALL read environment variables only via `Pipeline_Runtime::ci_context()`. The helper SHALL expose `git_sha`, `branch`, `substrate`, `run_id`, `actor`, and `run_started_at`, each with a documented local fallback.
3. Pipeline_Crates SHALL NOT invoke runner-specific CLIs (`gh`, `buildkite-agent`) from pipeline source. If an operation needs runner-specific APIs (for example, uploading a GitHub Actions artifact with a UI link), it SHALL be performed by the Trigger_Workflow after the pipeline binary exits, not by the pipeline binary itself.
4. Pipeline_Crate binaries SHALL NOT require secrets to run `check` or `test`. Secrets MAY be required for `build` (container registry credentials) or operator-facing subcommands (mirror to ECR). Required secrets SHALL be declared in the Pipeline_Crate's `README.md` and SHALL fail fast with a descriptive error if missing when needed.

### Requirement 1.5: `tkr pipeline run` invocation contract

**User Story:** As a Tokeira operator invoking a pipeline locally, I want a predictable invocation pattern, so that I can run anything in CI from my workstation with a single command shape.

#### Acceptance Criteria

1. `tkr pipeline run {name} [subcommand]` SHALL be a thin wrapper that invokes `cargo run -p tokeira-pipeline-{name} --release -- {subcommand}`.
2. When `subcommand` is omitted, it SHALL default to `check`.
3. `tkr pipeline run` SHALL forward any trailing `--` arguments to the pipeline binary unchanged.
4. `tkr pipeline run` SHALL respect the `--json` global flag from `tkr-cli` by passing `--json` to the pipeline binary.
5. WHEN the `dagger` CLI is not on PATH, `tkr pipeline run` SHALL exit with a descriptive error (from image-lifecycle's existing Dagger-missing handling) and a link to the install documentation.

---

## Feature 2: Pipeline Runtime Crate

### Requirement 2.1: `tokeira-pipeline-runtime` crate

**User Story:** As a Tokeira pipeline author, I want a shared Rust crate of Dagger orchestration helpers, so that I do not reinvent workspace mounting or `cargo fmt` wrapping in every pipeline.

#### Acceptance Criteria

1. THE workspace SHALL include a `crates/tokeira-pipeline-runtime/` library crate depending on `dagger-sdk` and `tokeira-ci-policy`.
2. THE crate SHALL expose at minimum the following functions, each taking a `&dagger_sdk::Client`:
   - `fn workspace(client: &dagger_sdk::Client, workspace_root: &Path) -> Directory` — mounts the workspace root with gitignored paths excluded (`target/`, `node_modules/`, `.git/`, `.kiro/cache/`).
   - `fn rust_toolchain(client: &dagger_sdk::Client, workspace_root: &Path) -> Result<Container>` — returns a Rust container based on `rust:{toolchain}-alpine` where `{toolchain}` is read from `rust-toolchain.toml`.
   - `fn cargo_fmt_check(container: &dyn ContainerRef, workspace: &dyn DirectoryRef) -> Result<Box<dyn ContainerRef>>` — wraps `cargo +nightly fmt --all --check`.
   - `fn cargo_lint(container, workspace) -> Result<Box<dyn ContainerRef>>` — wraps `cargo lint`.
   - `fn cargo_test_lint(container, workspace) -> Result<Box<dyn ContainerRef>>` — wraps `cargo test-lint`.
   - `fn cargo_check(container, workspace) -> Result<Box<dyn ContainerRef>>` — wraps `cargo check --workspace --all-targets`.
   - `fn cargo_test(container, workspace) -> Result<Box<dyn ContainerRef>>` — wraps `cargo test --workspace`.
   - `fn cargo_doc(container, workspace) -> Result<Box<dyn ContainerRef>>` — wraps `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS="-D warnings"`.
   - `fn container_build(dagger, workspace, dockerfile) -> Result<Box<dyn ContainerRef>>` — builds from a Dockerfile path within a workspace directory.
   - `fn container_publish(container, registry, username, password, remote_refs) -> Result<Vec<PublishedReference>>` — publishes to one or more remote refs. Signature matches `tokeira-build::publish_image` (see image-lifecycle design doc).
   - `fn upload_artifact(s3: &dyn S3Client, bucket: &str, key: &str, artifact: &[u8]) -> Result<String>` — uploads an artifact blob to the Artifact_Bucket, returns the `s3://` URL.
   - `fn ci_context() -> CiContext` — reads environment variables per Ci_Context glossary entry, with documented local fallbacks.
3. THE crate SHALL use `thiserror` for its error type and SHALL NOT expose `anyhow::Error` in its public API.
4. THE crate SHALL use `tracing` for structured logging; SHALL NOT use `println!` or `eprintln!` in library code.

### Requirement 2.2: Rust toolchain container versioning

**User Story:** As a Tokeira developer, I want the Rust build container version to follow `rust-toolchain.toml`, so that bumping the workspace toolchain automatically updates all pipelines.

#### Acceptance Criteria

1. `rust_toolchain()` SHALL read `rust-toolchain.toml` at call time and use the extracted version in the container image tag (for example, `rust:1.95.0-alpine`).
2. IF `rust-toolchain.toml` is missing or malformed, `rust_toolchain()` SHALL fail with a descriptive error citing the file path.
3. Pipelines SHALL NOT override the Rust version without an explicit opt-out via an additional overload; the default behaviour is workspace-pinned toolchain.

### Requirement 2.3: Caching

**User Story:** As a Tokeira developer waiting for a pipeline to complete, I want aggressive build caching, so that unchanged code paths are fast to re-verify.

#### Acceptance Criteria

1. `cargo_*` helper functions SHALL mount Dagger cache volumes for `/root/.cargo/registry`, `/root/.cargo/git`, and `target/` scoped to the workspace fingerprint (`Cargo.lock` + `rust-toolchain.toml`).
2. Cache-volume keying SHALL use only stable inputs. Transient inputs (run ID, timestamps, secrets) SHALL NOT participate in keys.
3. THE caching strategy SHALL be documented in `tokeira-pipeline-runtime/README.md`.

### Requirement 2.4: Ci_Context resolution

**User Story:** As a Tokeira pipeline author, I want to read the CI context through one helper, so that substrate specifics (GHA vs Buildkite vs local) are invisible to the pipeline code.

#### Acceptance Criteria

1. `ci_context()` SHALL resolve each Ci_Context field from the following ordered sources:
   - `git_sha`: `GITHUB_SHA`, `BUILDKITE_COMMIT`, `git rev-parse HEAD`, panic on unavailable.
   - `branch`: `GITHUB_REF_NAME`, `BUILDKITE_BRANCH`, `git rev-parse --abbrev-ref HEAD`, `local`.
   - `substrate`: `github` if `GITHUB_ACTIONS == "true"`, `buildkite` if `BUILDKITE == "true"`, else `local`.
   - `run_id`: `GITHUB_RUN_ID`, `BUILDKITE_BUILD_NUMBER`, `local-{iso8601}-{random}`.
   - `actor`: `GITHUB_ACTOR`, `BUILDKITE_BUILD_CREATOR`, `$USER`, `local`.
   - `run_started_at`: `GITHUB_RUN_STARTED_AT`, `BUILDKITE_STARTED_AT`, the binary's monotonic start-instant converted to ISO-8601 at call time (with a one-wall-clock-read-per-process exemption — see Req 5.1.4).
2. `ci_context()` SHALL be pure enough to unit-test without network or subprocess fallbacks (`git rev-parse` fallback gated behind a feature flag or injected via a `ContextSource` trait).
3. Pipelines SHALL NOT re-read these environment variables directly.

---

## Feature 3: CI Substrate Layer

### Requirement 3.1: Primary substrate — Buildkite Personal with hosted Linux agents

**User Story:** As a Tokeira developer opening a pull request, I want CI to run the standard verification pipelines on Buildkite's hosted Linux agents, so that regressions are caught before merge without having to manage runner infrastructure.

#### Acceptance Criteria

1. THE workspace SHALL include `.buildkite/pipeline.yml` as the primary, live Trigger_Workflow.
2. THE pipeline SHALL target the **Buildkite hosted Linux agent queue**. The agent targeting line SHALL be `agents: { queue: "hosted-linux-default" }` (or the current Buildkite-hosted Linux queue name at registration time — whichever matches the organisation's hosted-agent configuration).
3. THE pipeline SHALL be registered under a **Buildkite Personal** organisation. The spec does not mandate Business-tier features.
4. Self-hosted agents SHALL NOT be used in the initial rollout. The decision to introduce self-hosted agents (Elastic CI Stack for AWS or otherwise) SHALL be driven by one or more of:
   - Hosted-agent minute consumption exceeding a documented monthly budget.
   - Sustained queue-wait time above a documented threshold on PR checks.
   - A new pipeline requiring capabilities unavailable on hosted Linux agents (for example, GPU, macOS, or network-isolated testing).
   Until that trigger is documented and acknowledged by a maintainer, the spec remains hosted-agents-only. A follow-up `buildkite-agents` spec SHALL handle self-hosted rollout when the trigger fires.
5. Dagger Cloud SHALL be **disabled** in the pipeline by default. If enabled locally by a developer or later enabled for the Buildkite org, it SHALL be the free individual tier only. The paid Dagger Team tier SHALL NOT be enabled without a spec amendment. The pipeline SHALL NOT set `DAGGER_CLOUD_TOKEN` on the hosted agent by default.
6. EACH pipeline step SHALL consist of at most: a shell command that invokes the Dagger CLI (pre-installed on Buildkite hosted agents — if not, one `curl | sh` install with a pinned version), and `cargo run -p tokeira-pipeline-{name} --release -- {subcommand} --json`.
7. THE pipeline SHALL NOT contain inline bash that reimplements pipeline logic. If a step needs more than the two operations above, the additional logic SHALL move into the Pipeline_Crate.
8. THE pipeline SHALL NOT require repository secrets to run `check`, `test`, `lint`, or T0/T3 steps (see Feature 10). Secrets for `build` (for example, ECR push) SHALL be scoped via Buildkite pipeline-level environment variables, disclosed only on default-branch runs, and delivered into Dagger as typed secrets (`Query::set_secret`).

### Requirement 3.2: Parity template — GitHub Actions as a substrate-independence exemplar

**User Story:** As a Tokeira maintainer, I want a second substrate implementation maintained alongside the primary one, so that substrate independence is demonstrably real and a cutover is possible without major rework.

#### Acceptance Criteria

1. THE workspace SHALL include `.github/workflows/ci.yml` using the same Pipeline_Crate invocations as the Buildkite pipeline.
2. THE GHA workflow SHALL either (a) be disabled (committed but not registered as a required check on the repo), or (b) run in shadow mode (registered but non-blocking for PR merge). The spec explicitly does not require GHA to gate PRs; Buildkite is the gate.
3. EACH job SHALL consist of at most these steps: `actions/checkout@v4` pinned by full SHA, install the Dagger CLI via the upstream installer at a pinned version (not via a third-party action), and `cargo run -p tokeira-pipeline-{name} --release -- {subcommand} --json`.
4. THE workflow SHALL NOT use `github-script`, `actions/github-script`, or any action whose output is specific to GitHub's pipeline model.
5. THE workflow SHALL NOT be deleted by this spec — it remains as substrate-independence evidence.

### Requirement 3.3: Substrate independence

**User Story:** As a Tokeira maintainer, I want Pipeline_Crates to be substrate-agnostic, so that switching from Buildkite to another runner (or adding a secondary runner) does not require rewriting pipeline code.

#### Acceptance Criteria

1. Pipeline_Crate source SHALL NOT contain the substrings `github`, `buildkite`, `hosted-linux-`, or any substrate-specific SDK usage.
2. Pipeline_Crates SHALL read Ci_Context only via `Pipeline_Runtime::ci_context()`, not via raw `GITHUB_SHA` or `BUILDKITE_COMMIT`.
3. Adding a new CI substrate SHALL require only a new Trigger_Workflow template plus optional Ci_Context resolution updates (Req 2.4.1 ordered sources); no changes to pipeline source.
4. A `grep` check in `dev/ci/check-substrate-leakage.sh` SHALL scan `crates/tokeira-pipeline-*/src/` for substrate-name leakage and fail the build on hit.

### Requirement 3.4: Buildkite hosted-agent specifics

**User Story:** As a Tokeira maintainer running on Buildkite hosted Linux agents, I want the hosted-agent contract and its limits documented in the spec itself, so that operational expectations are explicit.

#### Acceptance Criteria

1. THE spec SHALL note that Buildkite hosted agents are **ephemeral** — destroyed after each job — so caches SHALL NOT be assumed to carry across jobs unless explicitly uploaded to Buildkite cache volumes or stored in S3.
2. THE spec SHALL note that Buildkite hosted Linux agents have Docker and common CLIs pre-installed. The pipeline SHALL verify the Dagger CLI is present and install it at a pinned version if not.
3. THE spec SHALL note that `buildkite-agent artifact upload` is UI convenience only; the canonical artifact surface is S3 per Feature 4.
4. THE spec SHALL note that the hosted Linux queue is shared across Buildkite Personal pipelines; sustained queue-wait time is an input to the Req 3.1.4 trigger for adopting self-hosted agents.
5. THE spec SHALL note that Buildkite hosted agents do not support privileged Docker or macOS execution; any future pipeline requiring these capabilities triggers the self-hosted-agent decision.
6. THE spec SHALL note that the CI integration pattern (install the pinned Dagger CLI, point the engine-selection variables at the pinned engine, invoke the pipeline binary) applies to Buildkite identically to every other substrate; there is no Buildkite-specific Dagger plugin and none is needed.

---

## Feature 4: Artifact Contract

### Requirement 4.1: Artifact schema

**User Story:** As a Tokeira developer consuming pipeline outputs, I want a versioned schema for every artifact, so that `tkr` commands can parse artifacts without guessing at the structure.

#### Acceptance Criteria

1. EVERY artifact produced by a pipeline SHALL conform to a JSON schema documented at `dev/pipelines/artifact-schema.json` and mirrored in `tokeira-ci-policy::ArtifactEnvelope<T>`.
2. EVERY artifact SHALL carry the following top-level fields:
   - `schema_version: u32` — major version of the artifact schema. Readers refuse parse on unknown major versions.
   - `pipeline: string` — the pipeline name (kebab-case, matching the Pipeline_Crate).
   - `generated_at: string` — ISO-8601 UTC timestamp, set from `ci_context.run_started_at`. This is the ONLY wall-clock-sourced field permitted.
   - `ci_context: object` — the five Ci_Context fields (`git_sha`, `branch`, `substrate`, `run_id`, `actor`).
   - `results: object` — pipeline-specific payload. Pipelines define the structure within `results`.
3. Adding a new top-level field SHALL bump the schema minor version. Removing or renaming a field SHALL bump the major version.
4. Pipelines SHALL NOT include other wall-clock timestamps in `results`. Event timing SHALL be expressed as durations in milliseconds relative to `generated_at`.

### Requirement 4.2: Artifact bucket

**User Story:** As a Tokeira operator, I want pipeline artifacts stored in a known location with retention, so that `tkr` commands can read the latest results and historical runs without probing CI APIs.

#### Acceptance Criteria

1. THE `tokeira-aws` crate SHALL define an `ArtifactBucket` resource following the shared-bucket pattern from `RemoteStateBucket` (adoption, versioning enforcement, public access block, no-op delete when adopted).
2. THE default bucket name SHALL be `{project_name}-artifacts-{region}`. The bucket SHALL be keyed by `{project_name}/pipelines/{pipeline}/{git_sha}/{run_id}.json`.
3. THE bucket SHALL have a lifecycle policy that transitions artifacts older than 90 days to STANDARD_IA and expires artifacts older than 365 days.
4. THE bucket SHALL carry the same auto-generated and operator-defined tags as all other AWS resources per the [`ecs-deployment`](../ecs-deployment/requirements.md) tagging requirement.
5. THE bucket resource SHALL be provisionable through any platform's IaC modules. The initial integration is via the ECS platform's networking/observability grouping or a sibling `artifacts` module; the `pipeline-foundation` spec does not hard-bind the bucket to one platform.

### Requirement 4.3: Artifact upload

**User Story:** As a Tokeira pipeline author, I want a single shared function for artifact upload, so that every pipeline produces artifacts in the right place without reinventing S3 logic.

#### Acceptance Criteria

1. `Pipeline_Runtime::upload_artifact` SHALL authenticate to the bucket using the CI substrate's ambient AWS credentials (IAM role on the agent, OIDC token exchange on GHA, or `~/.aws` locally).
2. IF credentials are absent (for example, a PR-triggered GHA run from a fork), the function SHALL skip the upload and emit a `tracing::warn!` — it SHALL NOT fail the pipeline.
3. ON successful upload, the function SHALL return the `s3://{bucket}/{key}` URL and log it at INFO for the Trigger_Workflow to capture.
4. LOCAL runs SHALL write the artifact to `./artifacts/{pipeline}/{run_id}.json` and skip the S3 upload unless the operator has explicitly set AWS credentials and passed `--upload` to `tkr pipeline run`.

### Requirement 4.4: Artifact reading from `tkr`

**User Story:** As a Tokeira operator, I want `tkr` commands to read pipeline artifacts from a known location, so that compatibility or build status is available without CI-substrate-specific API knowledge.

#### Acceptance Criteria

1. `tkr pipeline artifacts {name}` SHALL list the most recent 10 artifacts for a named pipeline in the deployment's Artifact_Bucket, sorted newest-first.
2. `tkr pipeline artifacts {name} --run-id {id}` SHALL print the artifact JSON for a specific run.
3. `tkr pipeline artifacts {name} --latest --json` SHALL print the most recent artifact as JSON.
4. Consumer specs (for example, [`temporal-compatibility`](../temporal-compatibility/requirements.md)'s `tkr compat conformance`) SHALL read artifacts via `tokeira-ci-policy::read_artifact`, not via the S3 SDK directly.

---

## Feature 5: Policy-Layer Pattern

### Requirement 5.1: `tokeira-ci-policy` crate

**User Story:** As a Tokeira maintainer, I want a shared Rust crate for cross-pipeline policy types, so that every Pipeline_Crate and Policy_Crate does not redefine `CiContext`, `ArtifactEnvelope`, or exit codes.

#### Acceptance Criteria

1. THE workspace SHALL include a `crates/tokeira-ci-policy/` library crate.
2. THE crate SHALL expose:
   - `CiContext` struct with the six fields from the Ci_Context glossary entry, `Serialize + Deserialize`.
   - `ArtifactEnvelope<T>` generic struct carrying the mandatory top-level fields (`schema_version`, `pipeline`, `generated_at`, `ci_context`, `results: T`).
   - `ExitCode` enum covering pipeline-result exit codes (`Ok = 0`, `Failed = 1`, `Unclassified = 2`, `StaleMatrix = 3`, `UsageError = 64`).
   - A `read_artifact<T>(reader) -> Result<ArtifactEnvelope<T>>` helper that validates `schema_version` and refuses unknown major versions.
   - A `write_artifact<T>(writer, envelope) -> Result<()>` helper that serialises with a canonical field order for byte-deterministic output.
3. THE crate SHALL have minimal dependencies: `serde`, `serde_json`, `thiserror`, `time` (for ISO-8601 parsing only, not for `now`).
4. THE crate SHALL NOT call wall-clock functions. `generated_at` is supplied by the caller from `Ci_Context::run_started_at`, which itself records one wall-clock read at pipeline-binary startup (the single permitted wall-clock read per process).

### Requirement 5.2: Per-pipeline policy crates

**User Story:** As a Tokeira pipeline author, I want a documented pattern for declaring pipeline-specific policy crates, so that new pipelines follow the same shape as `tokeira-build` and (future) `tokeira-conformance`.

#### Acceptance Criteria

1. A pipeline with non-trivial result classification SHALL have a dedicated Policy_Crate at `crates/tokeira-{domain}/` implementing the classification logic (for example, `tokeira-build`, `tokeira-conformance`).
2. A Policy_Crate SHALL depend on `tokeira-ci-policy` for shared types.
3. A Pipeline_Crate SHALL invoke its Policy_Crate via in-process library calls (not via shelling out to a binary) — both live in the same workspace, so a direct call is cleaner than reinvoking `cargo run`.
4. Policy_Crates SHALL produce artifacts via `ArtifactEnvelope<T>::write_to`. Pipelines do not construct artifacts by hand.
5. Policy_Crates SHALL be unit-testable without a Dagger engine, Docker daemon, or network access.

### Requirement 5.3: Policy-execution separation

**User Story:** As a Tokeira maintainer, I want the "what happened" vs "what it means" separation enforced, so that future changes to classification logic don't require changes to Pipeline_Crate source.

#### Acceptance Criteria

1. Pipeline_Crates SHALL NOT classify pipeline results. They collect raw outputs (JUnit XML, test logs, exit codes) and pass them to a Policy_Crate's classifier function.
2. Policy_Crates SHALL NOT invoke Dagger or `dagger-sdk`. They are pure input-in, artifact-out Rust libraries.
3. Policy_Crates SHALL expose both a library API and a small binary entry point (for example, `cargo run -p tokeira-conformance -- classify --input junit.xml`) so that other Rust code (`tkr` commands) can reuse classification logic and so that classification is independently runnable when debugging.

---

## Feature 6: `tkr pipeline` Command Group

### Requirement 6.1: Command surface

**User Story:** As a Tokeira operator, I want a single `tkr` subcommand for pipeline operations, so that local runs, artifact inspection, and CI parity are one consistent surface.

#### Acceptance Criteria

1. THE `tkr` CLI SHALL expose a top-level `pipeline` subcommand with children: `list`, `run`, `artifacts`.
2. `tkr pipeline list` SHALL enumerate every Pipeline_Crate in the workspace (discovered by scanning `crates/tokeira-pipeline-*`), showing name and subcommands parsed from the binary's clap help.
3. `tkr pipeline run {name} [{subcommand}]` SHALL invoke `cargo run -p tokeira-pipeline-{name} --release -- {subcommand}` against the Local_Engine. When `subcommand` is omitted, it defaults to `check`.
4. `tkr pipeline artifacts {name}` SHALL list recent artifacts per Req 4.4.1.
5. ALL `tkr pipeline` commands SHALL respect the `--json` global flag from [`tkr-cli`](../tkr-cli/requirements.md).

### Requirement 6.2: Prerequisite checks

**User Story:** As a Tokeira operator running a pipeline locally for the first time, I want clear error messages when prerequisites are missing, so that I am not left guessing.

#### Acceptance Criteria

1. WHEN `dagger` is not on PATH, `tkr pipeline run` SHALL exit with the same descriptive error used by [`image-lifecycle`](../image-lifecycle/requirements.md) (pointing at the Dagger install docs).
2. WHEN Docker (or another Dagger-supported container runtime) is not running, the invocation SHALL surface Dagger's own error message unmodified and exit non-zero.
3. Prerequisite checks SHALL NOT be performed for `tkr pipeline list` or `tkr pipeline artifacts` — only the `run` subcommand needs Dagger.

---

## Feature 7: Secrets and Credentials

### Requirement 7.1: Secret declaration

**User Story:** As a Tokeira pipeline author, I want secrets declared in one place per pipeline, so that a reader can tell what a pipeline needs without reading Dagger code.

#### Acceptance Criteria

1. EACH Pipeline_Crate's `README.md` SHALL have a "Secrets" section listing every secret the pipeline requires by name, purpose, and default source (GHA secret name, Buildkite agent binding, or `~/.aws` for AWS credentials).
2. Pipeline_Crates SHALL read secrets only via `dagger-sdk` typed secrets (`Query::set_secret`) or, for host-side values, via the `aws-sdk-*` credential provider chain. Raw secret values SHALL NOT appear in pipeline source or in logs.
3. Pipeline_Runtime helper functions that take credentials (`container_publish`, `upload_artifact`) SHALL accept `dagger_sdk::Secret` or `aws_credential_types::provider::SharedCredentialsProvider` arguments, not plain strings.

### Requirement 7.2: Secret rotation

**User Story:** As a Tokeira operator rotating a CI secret, I want the rotation process documented, so that there is no guessing which pipelines break when a secret changes.

#### Acceptance Criteria

1. THE root `dev/pipelines/README.md` SHALL include a "Secret rotation" section describing the steps for rotating each class of secret (AWS credentials, container registry credentials, third-party API keys).
2. Rotation instructions SHALL name both the GitHub Actions and Buildkite rotation paths.

---

## Feature 8: Correctness Properties

### Requirement 8.1: Subcommand registration property

**User Story:** As a Tokeira maintainer, I want every Pipeline_Crate binary validated at build time, so that a pipeline accidentally missing `check` or `test` cannot land.

#### Acceptance Criteria

1. A deterministic test in `tokeira-ci-policy` (or a companion crate) SHALL enumerate `crates/tokeira-pipeline-*/` directories, invoke each binary with `--help`, parse the clap-produced usage, and assert every binary exposes `check`, `test`, `build`, and `artifact`.
2. THE test SHALL be implemented in safe Rust and SHALL run as part of `cargo test`.
3. THE test SHALL fail with a descriptive message naming the pipeline and the missing subcommand.

### Requirement 8.2: Artifact schema round-trip property

**User Story:** As a Tokeira maintainer, I want the artifact envelope round-trip property-tested, so that schema drift is caught in CI.

#### Acceptance Criteria

1. A property test (via `proptest`) SHALL generate arbitrary `ArtifactEnvelope<serde_json::Value>` values, serialise to JSON, deserialise, and assert structural equality.
2. THE test SHALL generate at least one `schema_version` above the current maximum and assert that `read_artifact` returns `Err`.

### Requirement 8.3: No wall-clock in policy-crate property

**User Story:** As a Tokeira maintainer, I want a CI check preventing policy crates from embedding wall-clock reads, so that artifact generation stays deterministic.

#### Acceptance Criteria

1. A CI grep check SHALL scan `crates/tokeira-ci-policy/src/`, `crates/tokeira-pipeline-runtime/src/`, and every `crates/tokeira-pipeline-*/src/` directory for `SystemTime::now|Utc::now|Local::now|OffsetDateTime::now_utc` and fail the build on any hit.
2. THE single permitted wall-clock read (for `run_started_at`) SHALL live in `Pipeline_Runtime::ci_context()` in a clearly-named function `wall_clock_once_at_process_startup` that the grep check whitelists by function name.
3. THE check SHALL NOT flag `std::time::Instant` usage.

### Requirement 8.4: Substrate-leakage property

**User Story:** As a Tokeira maintainer, I want a CI check preventing substrate-specific strings from leaking into Pipeline_Crate source, so that substrate independence is mechanically enforced.

#### Acceptance Criteria

1. A CI grep check SHALL scan `crates/tokeira-pipeline-*/src/` for the substrings `GITHUB_`, `BUILDKITE_`, `github_script`, `buildkite_agent`, and fail the build on any hit.
2. THE check SHALL NOT scan `tokeira-pipeline-runtime/src/ci_context.rs` (which is the one place those env-var names are permitted to appear).

### Requirement 8.5: Pipeline binary connects its session cleanly

**User Story:** As a Tokeira maintainer, I want a smoke test that every Pipeline_Crate binary connects to its Dagger session cleanly, so that a mis-configured pipeline is caught in PR CI, not in release.

#### Acceptance Criteria

1. A CI integration job SHALL invoke `cargo run -p tokeira-pipeline-{name} -- check` for every Pipeline_Crate on every PR.
2. Duration budgets apply (Req 1.3.4). Pipelines exceeding their budget in a PR job SHALL surface a warning in the PR comment (automation optional — documented as the intent).

---

## Feature 9: Cross-Cutting Requirements

### Requirement 9.1: Documentation

**User Story:** As a Tokeira developer new to the project, I want the pipeline story documented in `README.md` and `AGENTS.md`, so that I can understand where CI lives without reading this spec.

#### Acceptance Criteria

1. THE root `README.md` SHALL include a "CI and pipelines" section pointing at `crates/tokeira-pipeline-*`, enumerating the registered pipelines, and listing each pipeline's subcommands.
2. THE root `AGENTS.md` SHALL include an "Adding a new pipeline" section referencing the Pipeline_Crate convention (Req 1.1–1.5) and the policy-layer pattern (Req 5.1–5.3).
3. EACH Pipeline_Crate SHALL have its own `README.md` meeting the documentation bar in Req 1.1.3 and Req 7.1.1.

### Requirement 9.2: Migration from existing CI

**User Story:** As a Tokeira maintainer, I want a documented migration path from whatever CI currently exists to the new foundation, so that the transition is incremental and reversible.

#### Acceptance Criteria

1. THE first Pipeline_Crate to land SHALL be `tokeira-pipeline-workspace` covering `cargo fmt/lint/test-lint/check/test/doc` for the whole workspace.
2. THE Buildkite pipeline SHALL be registered against a **Buildkite Personal** organisation as the primary live substrate. Any existing CI (ad-hoc workflows, legacy scripts) SHALL run in parallel during migration. Cutover SHALL be a single PR that deletes the legacy workflow(s).
3. THE GitHub Actions workflow SHALL be committed alongside the Buildkite pipeline but remain non-gating (disabled or shadow mode per Req 3.2.2) to exercise substrate independence.
4. AFTER migration, only Pipeline_Crates triggered via the Buildkite pipeline SHALL produce merge-gating CI signal. Stray workflow files that still gate merges SHALL be deleted as part of the migration PR.

### Requirement 9.3: Third-party integrations

**User Story:** As a Tokeira maintainer, I want the list of third-party integrations bounded, so that the pipeline substrate does not accrete vendor lock-in.

#### Acceptance Criteria

1. THE initial approved third-party CI-adjacent integrations SHALL be: Dagger (pipeline engine, via vendored `dagger-sdk`), Buildkite (primary trigger layer, Personal organisation with hosted Linux agents), GitHub Actions (parity template only; non-gating), AWS S3 (artifact storage), AWS Secrets Manager (secret source for the ECS deployment's runtime, not required for PR CI).
2. Dagger Cloud is an allowed integration **only at the free individual tier**. Enabling the paid team tier requires a spec amendment.
3. Self-hosted Buildkite agents (Elastic CI Stack for AWS or equivalent) are explicitly NOT yet approved. Their adoption is gated on the Req 3.1.4 cost/queue-wait/capability trigger and a follow-up `buildkite-agents` spec.
4. Adding a new third-party integration SHALL require a spec amendment.
5. Pipeline_Crates SHALL NOT call third-party services beyond the approved list without an amendment.

### Requirement 9.4: Workspace pipeline reference implementation

**User Story:** As a Tokeira maintainer, I want the first shipping pipeline to be a minimal but complete reference, so that subsequent pipelines have an unambiguous template.

#### Acceptance Criteria

1. `tokeira-pipeline-workspace` SHALL expose the four required subcommands:
   - `check` — `cargo fmt --check`, `cargo lint`, `cargo test-lint`, `cargo check --workspace`.
   - `test` — `cargo test --workspace`.
   - `build` — `cargo doc --workspace --no-deps` (so "build" produces inspectable output; this is the pipeline's primary artifact beyond green/red).
   - `artifact` — emits an `ArtifactEnvelope<WorkspaceResults>` summarising the last `check` and `test` runs.
2. `tokeira-pipeline-workspace` SHALL be documented as the canonical template. New Pipeline_Crates SHALL be created by copying its structure.
3. `tokeira-pipeline-workspace` SHALL be the first pipeline wired into both the Buildkite pipeline and the GitHub Actions parity workflow.

---

## Feature 10: Conformance Tier Orchestration

This feature defines **where each conformance tier runs**. The tier numbering and the semantic rules for classifying scenarios (what T0/T1/T2/T3 actually mean, which `temporalio/features` directories map to which feature-matrix states) are owned by [`temporal-compatibility`](../temporal-compatibility/requirements.md). This spec owns only the orchestration choices: PR gate, manual trigger, or deferred.

### Requirement 10.1: PR-gating tier set

**User Story:** As a Tokeira developer opening a PR, I want fast compatibility-adjacent feedback before merge, so that regressions in wire compatibility, Rust workspace checks, or Tokeira-owned smoke tests are caught immediately.

#### Acceptance Criteria

1. THE Buildkite pipeline (Req 3.1) SHALL run the following on **every PR and every push to the default branch**, all on the hosted Linux agent queue:
   - **T0 wire checks** (owned by `temporal-compatibility`): proto/API-descriptor consistency. A dedicated Pipeline_Crate or a subcommand of an existing Pipeline_Crate implements this.
   - **Rust workspace checks**: `tokeira-pipeline-workspace` subcommands `check` and `test`.
   - **T3 Tokeira smoke tests** (owned by `temporal-compatibility`): Tokeira-owned black-box scenarios that exercise behaviours not yet covered by upstream suites. Starts with a minimal set (workflow start, signal, query, activity). Expands as `temporal-compatibility` adds scenarios.
2. Each of the three items above SHALL be its own Buildkite step (parallelism is Buildkite's to schedule; logical grouping is per step for clarity in the UI).
3. PR-gating SHALL mean: the step blocks PR merge on failure. Branch protection on the default branch SHALL be configured to require these steps to pass.
4. NO other conformance tier SHALL be PR-gating at the time this spec lands. T1 and T2 are handled by Req 10.2 and Req 10.3 respectively.

### Requirement 10.2: Manually-triggered T1 pipeline

**User Story:** As a Tokeira maintainer, I want to run the `temporalio/features` suite against tokeirad on demand before it's ready to gate PRs, so that I can validate cross-SDK compatibility without every PR paying its cost.

#### Acceptance Criteria

1. THE workspace SHALL include a `.buildkite/pipeline-features.yml` (or an equivalent Manual_Pipeline definition in the primary pipeline gated on a Buildkite metadata flag) that runs the T1 `temporalio/features` suite against a freshly-built tokeirad.
2. THE T1 pipeline SHALL be configured so that it only runs on **explicit maintainer trigger** (Buildkite "New Build" button with metadata `run_t1=true`, or `buildkite-agent pipeline upload` from another pipeline gated on a maintainer-only label). It SHALL NOT run on PR open, PR update, or push automatically.
3. THE T1 pipeline SHALL be implemented as `tokeira-pipeline-conformance-features`, a Pipeline_Crate whose `build` subcommand produces an artifact conforming to the envelope in Feature 4 and whose classification is performed by a future `tokeira-conformance` Policy_Crate (owned by `temporal-compatibility`).
4. THE T1 pipeline SHALL pin `temporalio/features` by immutable git SHA. The pinned SHA SHALL appear in the resulting artifact.
5. THE initial T1 language scope SHALL be Go only. Adding TypeScript and Python to the T1 pipeline SHALL be a subsequent amendment, not a requirement of this spec.
6. Promotion of the T1 pipeline from manual to PR-gating SHALL require:
   - Ten consecutive manual invocations (across a rolling window) passing with zero unexpected failures.
   - A maintainer amendment to Req 10.1 moving T1 into the PR-gating set.
   Neither condition is automatic. Both are explicit acts.

### Requirement 10.3: Deferred T2 SDK integration suites

**User Story:** As a Tokeira maintainer, I want to defer expensive SDK integration suites until Tokeira has enough Temporal-compatible behaviour for their result to matter, so that CI-minute and operator-attention cost is not spent on noise.

#### Acceptance Criteria

1. T2 full SDK integration suites (Go, TypeScript, Python SDK canonical test suites against tokeirad) SHALL NOT be run in CI at the time this spec lands. Neither PR-gating nor nightly.
2. Adoption of T2 SHALL be gated on a documented **readiness criterion** owned by [`temporal-compatibility`](../temporal-compatibility/requirements.md), expressed in terms of the feature matrix: for example, "T2 for language L runs when the feature matrix reports at least N% of L's required-feature scenarios as `Implemented`." The specific threshold is that spec's choice; this spec requires only that the threshold exists.
3. ONCE T2 is adopted for a given language, it SHALL land first as a Manual_Pipeline per the pattern in Req 10.2. Promotion to nightly (or PR-gating) follows the same two-condition promotion rule as Req 10.2.6, adapted for the target cadence.
4. THE spec SHALL NOT prescribe a calendar for T2 adoption. "When the feature matrix makes the result signal, not noise" is the trigger.

### Requirement 10.4: Tier-independence from substrate

**User Story:** As a Tokeira maintainer, I want each tier's orchestration (PR vs manual vs deferred) to be a pure Buildkite-configuration concern, so that moving tiers between run cadences does not require Pipeline_Crate code changes.

#### Acceptance Criteria

1. Promoting a tier (for example, moving T1 from manual to PR-gating) SHALL be a pipeline-YAML change only, not a Pipeline_Crate source change.
2. A tier's Pipeline_Crate SHALL NOT encode its cadence (PR vs manual vs deferred) in its Rust source. The Pipeline_Crate always does the work; Buildkite chooses when to trigger it.
3. THE artifact emitted by a tier SHALL carry a `ci_context.substrate` field of `buildkite`, `github`, or `local` per Req 4.1.2; it SHALL NOT carry a "this ran as PR gate / as manual" field. That distinction is recoverable from `ci_context.branch` and Buildkite build metadata without being stored in the artifact.

### Requirement 10.5: Tier orchestration documentation

**User Story:** As a Tokeira contributor, I want the tier orchestration model documented clearly, so that I know what runs when without reading multiple specs.

#### Acceptance Criteria

1. THE root `dev/pipelines/README.md` SHALL include a table mapping each conformance tier to its current cadence (PR gate, manual, deferred) with a last-updated date.
2. THE table SHALL link to [`temporal-compatibility`](../temporal-compatibility/requirements.md) for the tier _semantics_ (what each tier contains and classifies).
3. Changes to tier cadence SHALL update the table in the same PR as the Buildkite YAML change.
