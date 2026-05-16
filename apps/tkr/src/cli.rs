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
#[command(about = "Tokeira deployment and developer workflow CLI")]
pub struct Cli {
    /// Selects which named deployment this invocation operates on.
    /// When absent, `DeploymentResolver` falls back to the `.latest`
    /// sentinel written by `tkr deployment use`.
    #[arg(long)]
    pub deployment: Option<String>,
    /// Switches human output (tabular text, spinners) for newline-delimited
    /// JSON events. Machine consumers and CI integrations should pass this.
    #[arg(long)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Dev {
        #[command(subcommand)]
        action: DevAction,
    },
    Deployment {
        #[command(subcommand)]
        action: DeploymentAction,
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
    },
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    Workstation {
        #[command(subcommand)]
        action: WorkstationAction,
    },
    Version,
}

#[derive(Subcommand)]
pub enum DevAction {
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
pub enum DeploymentAction {
    Create {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        platform: CliPlatformKind,
        #[arg(long)]
        storage: CliStorageKind,
        #[arg(long)]
        region: Option<String>,
    },
    List,
    Use {
        name: String,
    },
    Destroy {
        name: String,
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Args)]
pub struct ImageArgs {
    #[command(subcommand)]
    pub command: ImageCommand,
}

#[derive(Subcommand)]
pub enum ImageCommand {
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
pub enum InfraAction {
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
pub enum DeployAction {
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
pub enum SchemaAction {
    Setup {
        #[arg(long)]
        yes: bool,
    },
    Status,
    Validate,
}

#[derive(Subcommand)]
pub enum ScaleAction {
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
pub enum ConfigAction {
    Show,
}

#[derive(Debug, Subcommand)]
pub enum WorkstationAction {
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
pub enum CodeAction {
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
pub enum GithubKeyAction {
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
