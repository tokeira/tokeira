use clap::{Parser, Subcommand, ValueEnum};
use tokeira_orchestrator::{PlatformKind, StorageKind};

#[derive(Parser)]
#[command(name = "tkr")]
#[command(about = "Tokeira deployment and developer workflow CLI")]
pub struct Cli {
    #[arg(long)]
    pub deployment: Option<String>,
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

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum CliPlatformKind {
    Local,
    Compose,
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
