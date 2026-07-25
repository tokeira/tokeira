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

use clap::{Args, Parser, Subcommand, ValueEnum};
use tokeira_orchestrator::{PlatformKind, StorageKind};

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
        /// Local port to bind. Defaults to the service's canonical port
        /// (grafana=3000, edge-api=7233, edge-poll=7234, controller=7240,
        /// mimir=9009, loki=3100).
        #[arg(long)]
        local_port: Option<u16>,
    },
    /// Execute a command in a running ECS container.
    Exec {
        service: String,
        /// Container name to exec into. Defaults to the primary application
        /// container for the service (never the Alloy sidecar).
        #[arg(long)]
        container: Option<String>,
        /// Command to execute inside the container.
        #[arg(last = true)]
        cmd: Vec<String>,
    },
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    Compat(CompatArgs),
    Ci(CiArgs),
    Observability {
        #[command(subcommand)]
        action: ObservabilityAction,
    },
    Workstation {
        #[command(subcommand)]
        action: WorkstationAction,
    },
    /// Run an admin command against the tokeira-admin ECS service.
    ///
    /// Scales the admin service from 0→1, waits for RUNNING, executes the
    /// command via ECS Exec, streams output, then scales back to 0.
    Admin {
        /// The full command to forward to the admin binary (e.g. "schema setup").
        #[arg(trailing_var_arg = true, required = true)]
        command: Vec<String>,
    },
    Version {
        #[arg(long)]
        verbose: bool,
        #[arg(long)]
        json: bool,
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
    /// Check a definition: parse + interpret it in memory — no providers
    /// touched, nothing changes. Checks the selected deployment's
    /// `definition.tkd` (or `--deployment <name>`'s); `--path` checks a
    /// definition anywhere on disk instead — a `.tkd` file, or a directory
    /// holding a `definition.tkd`.
    Check {
        /// Check the definition at this path instead of a deployment's:
        /// authoring mode — the definition needs no deployment to exist yet.
        #[arg(long)]
        path: Option<std::path::PathBuf>,
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
        platform: CliPlatformKind,
        #[arg(long, default_value = "in-memory")]
        storage: CliStorageKind,
        #[arg(long)]
        region: Option<String>,
        /// Obtain the provisioner as a verified hermetic bundle (CAS hit or
        /// one Dagger build) instead of the native dev copy — Phase 2 of
        /// Proposal 005. Requires a running Dagger engine and the tokeira
        /// workspace.
        #[arg(long)]
        bundle: bool,
        /// The digest-pinned build container for `--bundle`
        /// (`<image>@sha256:<digest>` — a floating tag is refused; the
        /// container is an engine-identity input).
        #[arg(long, requires = "bundle")]
        build_image: Option<String>,
    },
    List,
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
    /// Apply the deployment via its bound provisioner (forwards to `tkp apply`,
    /// initializing it first if needed).
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

#[derive(Subcommand)]
pub(crate) enum ImageCommand {
    List {
        #[arg(long)]
        source_type: Option<CliImageSourceType>,
    },
    Build {
        #[arg(long, default_value = "arm64")]
        arch: CliArch,
        #[arg(long)]
        tag: Option<String>,
    },
    Push {
        #[arg(long, default_value = "latest")]
        tag: String,
        #[arg(long)]
        image: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    Mirror {
        #[arg(long)]
        image: Option<String>,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliImageSourceType {
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
    },
    Apply {
        #[arg(long)]
        yes: bool,
        #[arg(long)]
        module: Option<String>,
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
    Plan,
    Apply {
        #[arg(long)]
        yes: bool,
        /// Force recreation of all services regardless of manifest hash.
        /// Use after rebuilding a local image behind the same tag.
        #[arg(long)]
        force: bool,
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
        #[arg(long, default_value = "30")]
        timeout_seconds: u64,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum WorkstationAction {
    Up {
        #[arg(long, default_value = "c8gd-rust")]
        profile: String,
        #[arg(long)]
        workstation: Option<String>,
        #[arg(long)]
        cache_volume_gib: Option<u32>,
        #[arg(long)]
        repo_volume_gib: Option<u32>,
        #[arg(long)]
        root_volume_gib: Option<u32>,
        #[arg(long)]
        instance_type: Option<String>,
        #[arg(long)]
        region: Option<String>,
        #[arg(long)]
        subnet_id: Option<String>,
    },
    Stop {
        #[arg(long)]
        workstation: Option<String>,
    },
    Destroy {
        #[arg(long)]
        workstation: Option<String>,
        #[arg(long)]
        yes: bool,
    },
    Ssh {
        #[arg(long)]
        workstation: Option<String>,
    },
    RemoteExec {
        #[arg(long)]
        workstation: Option<String>,
        #[arg(long, default_value = "/work/repo/tokeira")]
        cwd: String,
        #[arg(long)]
        yes_secret_in_command: bool,
        #[arg(trailing_var_arg = true)]
        command: Vec<String>,
    },
    Status {
        #[arg(long)]
        workstation: Option<String>,
    },
    List,
    Bootstrap {
        #[arg(long)]
        workstation: Option<String>,
    },
    Idle {
        #[arg(long)]
        workstation: Option<String>,
        #[arg(long)]
        defer: Option<humantime::Duration>,
    },
    GithubKey {
        #[command(subcommand)]
        action: GithubKeyAction,
    },
    /// Manage code on the workstation (clone, sync, push).
    Code {
        #[command(subcommand)]
        action: CodeAction,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum CodeAction {
    /// Clone the repo if missing, or pull latest from origin.
    Sync {
        #[arg(long)]
        workstation: Option<String>,
        /// Branch to checkout/pull. Defaults to main.
        #[arg(long)]
        branch: Option<String>,
    },
    /// Push the current branch to origin.
    Push {
        #[arg(long)]
        workstation: Option<String>,
        /// Branch to push. Defaults to the current branch.
        #[arg(long)]
        branch: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum GithubKeyAction {
    Add {
        #[arg(long)]
        workstation: Option<String>,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        read_only: bool,
    },
    Remove {
        #[arg(long)]
        workstation: Option<String>,
        #[arg(long)]
        repo: Option<String>,
    },
    List {
        #[arg(long)]
        workstation: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliPlatformKind {
    Local,
    Compose,
    Ecs,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliStorageKind {
    #[value(name = "in-memory")]
    InMemory,
    Dsql,
}

impl From<CliPlatformKind> for PlatformKind {
    fn from(value: CliPlatformKind) -> Self {
        match value {
            CliPlatformKind::Local => PlatformKind::Local,
            CliPlatformKind::Compose => PlatformKind::Compose,
            CliPlatformKind::Ecs => PlatformKind::Ecs,
        }
    }
}

impl From<CliStorageKind> for StorageKind {
    fn from(value: CliStorageKind) -> Self {
        match value {
            CliStorageKind::InMemory => StorageKind::InMemory,
            CliStorageKind::Dsql => StorageKind::Dsql,
        }
    }
}

impl From<CliImageSourceType> for tokeira_deploy_engine::ImageSourceType {
    fn from(value: CliImageSourceType) -> Self {
        match value {
            CliImageSourceType::Build => Self::Build,
            CliImageSourceType::Mirror => Self::Mirror,
            CliImageSourceType::Registry => Self::Registry,
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
