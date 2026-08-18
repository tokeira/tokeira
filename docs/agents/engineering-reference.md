# Engineering reference

*Reference contracts and recipes backing [`AGENTS.md`](../../AGENTS.md). The root file
holds the always-relevant rules (§1–§12) and stays under its byte budget; this file
holds the material you need only when doing the specific task it describes. It is
equally binding.*

## Package boundaries

- `tokeira-kernel` is pure (root §2). `tokeira-edge` is thin — translates requests,
  never implements workflow semantics.
- `tokeira-projection` owns visibility types and the `VisibilityApi` trait; edge
  re-exports them.
- `tokeira-state`: `CasStore` (backend-agnostic single-document CAS) and `S3StateStore`
  (manifest + immutable snapshots).
- `tokeira-iac` and `tokeira-deploy-engine` are provider-agnostic; platform-specific
  resources and services live in platform crates, which follow the deploy-eks `project`
  pattern: `config.rs`, `modules.rs`, `services.rs`, `compose.rs`.
- `tokeira-config` owns the server config model (`TokeiraConfig`) and the generic TOML
  loader (same crate: one consumer today).
- `tokeira-deployment::repository` owns the Deployment Publication lineage (TUF via
  `tough`): locator/keys/claim, create-only writer, `publish_transition`, verified
  `open`, fetch planning, listing, freshness refresh. Publication is a **derived
  projection** — the deployment-dir rename (create) and the envelope CAS (transitions)
  stay the sole commit authorities; a publication failure reports `tkr deployment
  publish` as its remedy and never unwinds a commit. Publishers assemble input through
  `repository::assemble` (one implementation for create, the `tkp` lifecycle hook, and
  the repair verb); the shells stay thin: `tkr` owns keys/trust/metadata binding,
  `tkp` owns only the post-commit hook.
- `proto/upstream/` is the authoritative wire shape; `tokeira-proto` generates from it.
  Never treat generated output under `target/` as authoritative.

## Configuration

- Server config `tokeirad.toml` (`TokeiraConfig`: infrastructure, policy, capacity,
  emergency). Platform config `deployment.toml` (`LocalConfig`/`ComposeConfig`; compose
  DSQL deployments carry `storage = "dsql"` + `[dsql]` mode/endpoint/arn/region, and
  writeback updates `tokeirad.toml`'s `infrastructure.storage`/`infrastructure.dsql`).
- `serde(deny_unknown_fields)` on all config structs — typos die at parse time.
- `RuntimeConfig` is always `Default`, never TOML-configurable; mechanical settings
  auto-tune. No env vars on invocation; defaults characterized by expected performance.
- Emergency overrides (`disable_stickiness`, `freeze_projection`, `cap_poll_admission`)
  are logged as warnings.

## IaC engine contracts

- **Desired** vs **known** resources (everything manageable, including deletion
  candidates); `InfraComposition` carries both plus `active_modules` for scoped
  operations.
- Resources implement `create/update/delete/describe/diff/dependencies`; modules
  implement `name/dependencies/resources`; both topologically sorted before execution.
  `describe()` feeds `refresh_state` before diffing.
- An optional `StateSaver` callback runs after each mutating operation for incremental
  crash-safety. State backends tolerate a missing backing store on `load()` (return
  default) so the remote-state module can bootstrap during the first apply.

## Recipes

### Adding a New Platform

`platforms/{name}/` with `config.rs`/`modules.rs`/`services.rs`/`compose.rs` (or
equivalent); implement `Deployment` + `Ops` from `tokeira-orchestrator`; add
`PlatformKind` + `CliPlatformKind` variants; prototypical config generation in
`tkr/src/prototypical.rs`; tests for config generation, module composition, service
ordering.

### Adding a New IaC Module

Implement `Module` in the platform's `modules.rs`; register in `infra_modules()`; tests
for resource enumeration and dependency ordering. For compose storage modules,
`DsqlModule` is the reference pattern: module-owned config, explicit dependencies,
provider handles via `register_infra_extensions()`.

### Adding a New CLI Command

Subcommand variant in `tkr/src/cli.rs`; handler in `tkr/src/commands/{group}.rs`; wire
in `main.rs`; CLI parse tests. Multi-file command groups: `.kiro/specs/image-lifecycle/`
is the reference pattern.

### Adding a New Image

Declare the `tokeira_deploy_engine::image::Image` impl in each owning platform's
`src/images/` module and add it to that submodule's `all()`. If config references the
remote ref, override `writeback_targets(ctx)` with the dotted TOML keys. Property-test
non-trivial `desired_ref`/`writeback_targets` logic. New build recipes are free
functions in `tokeira-build`, each with its own hardcoded Dagger pipeline.

## Observability stack (compose platform)

Pinned images: Mimir `3.0.6` · Loki `3.7.1` · Grafana OSS `12.4.3` · Alloy `v1.16.0` ·
AWS CLI and BusyBox from public ECR. DSQL-mode module order: `local-state` → `dsql` →
`observability` → `runtime` (in-memory omits `dsql`). Dashboards: broker-runtime-health,
grpc-edge-health, storage-projection-health, log-exploration. The six mirror images are
declared per-platform in `src/images/observability/mod.rs` via `mirror_image!`; version
bumps are one-line changes in `ObservabilityConfig::default()` (or the
`default_<field>_image()` helpers for AWS CLI and BusyBox).
