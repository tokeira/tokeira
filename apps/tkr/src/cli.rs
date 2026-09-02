//! Clap-based command surface for `tkr`.
//!
//! Keep the enums here as the single source of truth for the CLI shape. The
//! `main.rs` dispatcher maps top-level variants to handler modules in
//! `commands/`, and the `From` impls at the bottom of this file bridge the
//! CLI-local enums to the domain enums in `tokeira-orchestrator`,
//! `tokeira-deploy-engine`, and `tokeira-build`.
//!
//! # Conventions for new subcommands
//!
//! - Destructive actions (apply/destroy/remove) take `--yes` and route
//!   through `commands::require_confirmation` before doing anything.
//! - Global flags live on `Cli` (`--deployment`, `--json`). Prefer extending
//!   the global flags over adding per-subcommand equivalents.
//! - Enum variants mirror the dispatcher in `main.rs` one-to-one; add the
//!   clap variant, the handler module, and wire them together in `main`.
//!
//! The CLI-to-domain `From` impls at the bottom keep clap off the library
//! crates — library crates stay free of a clap dependency even though they
//! expose equivalent enum shapes.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId, StorageKind};

#[derive(Parser)]
#[command(name = "tkr")]
#[command(version)]
#[command(about = "Tokeira deployment and developer workflow CLI")]
pub(crate) struct Cli {
    /// Selects which named deployment this invocation operates on.
    /// When absent, `DeploymentResolver` falls back to the `.latest`
    /// sentinel written by `tkr deployment use`.
    #[arg(long, global = true)]
    pub deployment: Option<String>,
    /// Switches human output (tabular text, spinners) for newline-delimited
    /// JSON events. Machine consumers and CI integrations should pass this.
    #[arg(long, global = true)]
    pub json: bool,
    /// Show the evidence behind the summary — per-resource lines, field
    /// diffs, digests, provenance. Forwarded to the deployment's bound `tkp`.
    #[arg(long, global = true)]
    pub detail: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
    Deployment {
        #[command(subcommand)]
        action: DeploymentAction,
    },
    Definition {
        #[command(subcommand)]
        action: DefinitionAction,
    },
    Image(ImageArgs),
    Infra {
        #[command(subcommand)]
        action: InfraAction,
    },
    Deploy {
        #[command(subcommand)]
        action: DeployAction,
    },
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
    Scale {
        #[command(subcommand)]
        action: ScaleAction,
    },
    Logs {
        service: String,
        #[arg(long)]
        follow: bool,
        #[arg(long)]
        tail: Option<u32>,
    },
    PortForward {
        service: String,
        /// Local port to bind when the selected platform opens a tunnel.
        #[arg(long)]
        local_port: Option<u16>,
    },
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    Compat(CompatArgs),
    Ci(CiArgs),
    Release(ReleaseArgs),
    Observability {
        #[command(subcommand)]
        action: ObservabilityAction,
    },
    /// Inspect durable operational state without mutating the deployment.
    Diagnostics {
        #[command(subcommand)]
        action: DiagnosticsAction,
    },
    Version {
        #[arg(long)]
        verbose: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum DiagnosticsAction {
    /// Report durable Worker Compute Controller health for one namespace.
    WorkerCompute {
        /// Public Temporal namespace name.
        #[arg(long)]
        namespace: String,
    },
}

#[derive(Subcommand)]
pub(crate) enum DevAction {
    Build,
    Test {
        #[arg(long = "crate")]
        crate_name: Option<String>,
    },
    Check,
    Lint,
    Fmt,
    Docs,
}

#[derive(Subcommand)]
pub(crate) enum DefinitionAction {
    /// Check a definition. `--definition <path>` is the frontend syntax
    /// tier: parse + admitted-subset + companion parts, in process,
    /// instantly. The selected deployment's definition (or
    /// `--deployment <name>`'s) is the full check: interpreted through the
    /// deployment's platform — vocabulary, typed config, context.
    Check {
        /// Check the source at this path through its frontend's syntax
        /// tier: authoring mode — no deployment, no platform.
        #[arg(long, alias = "path")]
        definition: Option<std::path::PathBuf>,
        /// Frontend override when the file extension is misleading;
        /// normally the extension (`.tkd`, `.tkdp`) names the frontend.
        #[arg(long, requires = "definition")]
        format: Option<tokeira_orchestrator::DefinitionFormatId>,
    },
}

#[derive(Subcommand)]
pub(crate) enum DeploymentAction {
    Create {
        #[arg(long)]
        name: Option<String>,
        /// Defaults to the zero-dependency dev pairing: a `local` deployment
        /// with `in-memory` storage — `tkr deployment create` alone always
        /// works on a fresh machine.
        #[arg(long, default_value = "local")]
        platform: PlatformId,
        /// Definition frontend format. Omit when the selected platform package
        /// supplies exactly one recognized seed.
        #[arg(long)]
        format: Option<DefinitionFormatId>,
        #[arg(long, default_value = "in-memory")]
        storage: CliStorageKind,
        #[arg(long)]
        region: Option<String>,
        /// Obtain the engine as the native workspace build instead of the
        /// default verified hermetic bundle. Local deployments only; the
        /// publication claim records the dev authority tier.
        #[arg(long)]
        dev_engine: bool,
        /// The digest-pinned build container for the default hermetic bundle
        /// path (`<image>@sha256:<digest>` — a floating tag is refused; the
        /// container is an engine-identity input). Required unless
        /// `--dev-engine`.
        #[arg(long, conflicts_with = "dev_engine")]
        build_image: Option<String>,
        /// Pre-existing, operator-owned S3 bucket for authoritative deployment
        /// state. Supply this together with `--state-region` and
        /// `--state-prefix`; omit all three to keep state local.
        #[arg(
            long,
            value_name = "BUCKET",
            requires_all = ["state_region", "state_prefix"]
        )]
        state_bucket: Option<String>,
        /// AWS region containing `--state-bucket`. It may differ from the
        /// deployment's provider region.
        #[arg(
            long,
            value_name = "REGION",
            requires_all = ["state_bucket", "state_prefix"]
        )]
        state_region: Option<String>,
        /// Deployment-exclusive key prefix within `--state-bucket`. Tokeira
        /// retains this prefix after destroy; bucket policy and lifecycle stay
        /// under operator control.
        #[arg(
            long,
            value_name = "PREFIX",
            requires_all = ["state_bucket", "state_region"]
        )]
        state_prefix: Option<String>,
    },
    List {
        /// Enumerate published deployment repositories instead of local
        /// deployment dirs: `local` (the deployments root's repositories)
        /// or an `s3://<bucket>/<prefix>` remote deployments base.
        #[arg(long)]
        repositories: Option<String>,
    },
    /// Materialize a published Deployment into a new deployment dir from a
    /// repository locator and trust anchor — verified in full before any
    /// byte is placed.
    Fetch {
        /// Name for the materialized deployment.
        #[arg(long)]
        name: String,
        /// Repository locator: a local path or `s3://<bucket>/<prefix>`.
        #[arg(long)]
        repository: String,
        /// The pinned trust anchor (a root.json file) to verify from.
        #[arg(long)]
        trust_anchor: PathBuf,
    },
    /// Complete a pending Deployment Publication from the committed state
    /// (the repair verb the "publication pending" report names).
    Publish {
        /// The transition the pending publication captures. Defaults to
        /// `create` when the repository holds no publication yet, `apply`
        /// otherwise.
        #[arg(long)]
        transition: Option<String>,
        /// Confirm the repository write (§4).
        #[arg(long)]
        yes: bool,
    },
    /// Re-sign the repository's freshness statement (and snapshot, when its
    /// expiry requires) for the current publication; targets and claim are
    /// untouched.
    Refresh {
        /// Confirm the repository write (§4).
        #[arg(long)]
        yes: bool,
    },
    /// Verify the deployment's repository read-only and report the current
    /// publication: version, transition, claim, expirations, inventory.
    Inspect,
    Use {
        name: String,
    },
    Destroy {
        /// The deployment to destroy. Deliberately required and deliberately
        /// a flag (consistent with `create --name`): a destructive verb
        /// never infers its target from the current selection.
        #[arg(long)]
        name: String,
        #[arg(long)]
        yes: bool,
    },
    /// Lock every mutating command to one deployment (the mis-apply guard).
    /// Defaults to the currently-selected deployment.
    Lock {
        name: Option<String>,
    },
    /// Clear the deployment lock (requires confirmation).
    Unlock {
        #[arg(long)]
        yes: bool,
    },
    /// Report the deployment's provisioner identity + binding (forwards to `tkp
    /// describe`; read-only, never gates).
    Describe,
    /// Apply the deployment via its creation-bound provisioner.
    Apply {
        /// Confirm a destructive plan (deletes or replacements).
        #[arg(long)]
        yes: bool,
    },
    /// Upgrade the deployment's engine identity (forwards to `tkp upgrade`).
    Upgrade,
    /// Roll back the deployment to its retained prior revision (forwards to `tkp
    /// rollback`).
    Rollback,
}

#[derive(Args)]
pub(crate) struct ImageArgs {
    #[command(subcommand)]
    pub command: ImageCommand,
}

#[derive(Args)]
pub(crate) struct CompatArgs {
    #[command(subcommand)]
    pub command: CompatCommand,
}

#[derive(Args)]
pub(crate) struct CiArgs {
    #[command(subcommand)]
    pub command: CiCommand,
}

#[derive(Args)]
pub(crate) struct ReleaseArgs {
    #[command(subcommand)]
    pub command: ReleaseCommand,
}

#[derive(Subcommand)]
pub(crate) enum ReleaseCommand {
    /// Add one collision-resistant changie fragment.
    Fragment {
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        body: Option<String>,
    },
    /// Produce a deterministic, secret-free release Plan.
    Plan {
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        version: String,
        #[arg(long)]
        base_ref: Option<String>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Revalidate and apply an exact release Plan.
    Apply {
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        plan: PathBuf,
        #[arg(long)]
        token_env: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    /// Observe a released or partially released train without credentials.
    Verify {
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        #[arg(long)]
        version: String,
        #[arg(long)]
        output: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
pub(crate) enum CompatCommand {
    Show {
        #[arg(long)]
        remote: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        verbose: bool,
    },
    Diff {
        #[arg(long)]
        a: String,
        #[arg(long)]
        b: String,
        #[arg(long)]
        fail_on_incompatible: bool,
    },
}

#[derive(Subcommand)]
pub(crate) enum CiCommand {
    Check {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        update_lock: bool,
        /// Run only the named check; repeat to select more than one.
        #[arg(long = "check", value_enum)]
        checks: Vec<CliCiCheck>,
    },
    Build {
        #[arg(long)]
        versioned: bool,
        #[arg(long)]
        json: bool,
    },
    LockUpdate {
        #[arg(long)]
        json: bool,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum CliCiCheck {
    ProtoMonotonicity,
    ServerCompatMonotonicity,
    BumpTrailer,
    Fmt,
    Lint,
    Check,
    Nextest,
    Doctests,
    Rustdoc,
    Deny,
    Links,
    ChangelogFragments,
    PackageDryRun,
}

#[derive(Subcommand)]
pub(crate) enum ImageCommand {
    /// List images declared by the selected definition-bound platform.
    List {
        #[arg(long)]
        source_type: Option<CliImageSource>,
    },
    Build {
        #[arg(long, default_value = "arm64")]
        arch: CliArch,
        #[arg(long)]
        tag: Option<String>,
    },
    /// Push a locally built image to the selected platform registry.
    Push {
        #[arg(long, default_value = "latest")]
        tag: String,
        #[arg(long)]
        image: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    /// Mirror authored upstream images into the selected platform registry.
    Mirror {
        #[arg(long)]
        image: Option<String>,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum CliImageSource {
    Build,
    Mirror,
    Registry,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliArch {
    Arm64,
    Amd64,
}

#[derive(Subcommand)]
pub(crate) enum InfraAction {
    Plan {
        #[arg(long)]
        module: Option<String>,
        /// Also write the complete explanation model as JSON to this path.
        /// Orthogonal to `--json`: the report still renders to stdout.
        #[arg(long, value_name = "PATH")]
        explanation: Option<PathBuf>,
    },
    Apply {
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        module: Option<String>,
        /// Also write the complete explanation model as JSON to this path.
        /// Orthogonal to `--json`: the report still renders to stdout.
        #[arg(long, value_name = "PATH")]
        explanation: Option<PathBuf>,
    },
    Destroy {
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        module: Option<String>,
    },
    Status,
}

#[derive(Subcommand)]
pub(crate) enum DeployAction {
    Plan {
        /// Also write the complete explanation model as JSON to this path.
        /// Orthogonal to `--json`: the report still renders to stdout.
        #[arg(long, value_name = "PATH")]
        explanation: Option<PathBuf>,
    },
    Apply {
        #[arg(long)]
        yes: bool,
        /// Force recreation of all services regardless of manifest hash.
        /// Use after rebuilding a local image behind the same tag.
        #[arg(long)]
        force: bool,
        /// Also write the complete explanation model as JSON to this path.
        /// Orthogonal to `--json`: the report still renders to stdout.
        #[arg(long, value_name = "PATH")]
        explanation: Option<PathBuf>,
    },
    /// Tear down every deployed service while retaining infrastructure and
    /// deployment records.
    Destroy {
        #[arg(long)]
        yes: bool,
    },
    Status,
}

#[derive(Subcommand)]
pub(crate) enum SchemaAction {
    Setup {
        #[arg(long)]
        yes: bool,
    },
    Status,
    Validate,
}

#[derive(Subcommand)]
pub(crate) enum ScaleAction {
    Up {
        service: Option<String>,
        replicas: Option<u32>,
    },
    Down {
        service: Option<String>,
        replicas: Option<u32>,
    },
    Status,
}

#[derive(Subcommand)]
pub(crate) enum ConfigAction {
    Show,
}

#[derive(Subcommand)]
pub(crate) enum ObservabilityAction {
    Check {
        /// Dashboard JSON file validated by `--grafana`.
        #[arg(long, value_name = "DASHBOARD_JSON", requires = "grafana")]
        path: Option<PathBuf>,
        /// Validate only the Grafana dashboard supplied by `--path`.
        #[arg(long, requires = "path")]
        grafana: bool,
        #[arg(long, default_value = "30")]
        timeout_seconds: u64,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliStorageKind {
    #[value(name = "in-memory")]
    InMemory,
    Dsql,
}

impl From<CliStorageKind> for StorageKind {
    fn from(value: CliStorageKind) -> Self {
        match value {
            CliStorageKind::InMemory => StorageKind::InMemory,
            CliStorageKind::Dsql => StorageKind::Dsql,
        }
    }
}

impl From<CliArch> for tokeira_build::Arch {
    fn from(value: CliArch) -> Self {
        match value {
            CliArch::Arm64 => Self::Arm64,
            CliArch::Amd64 => Self::Amd64,
        }
    }
}
