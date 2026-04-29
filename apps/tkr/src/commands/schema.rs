use anyhow::{Result, bail};
use tokeira_orchestrator::StorageKind;

use crate::cli::SchemaAction;
use crate::deployment_dir::DeploymentContext;
use crate::deployment_dir::TOKEIRAD_TOML;

pub fn run(action: SchemaAction, ctx: DeploymentContext) -> Result<()> {
    if ctx.metadata.storage != StorageKind::Dsql {
        bail!("schema commands require dsql storage");
    }
    let server_config_path = ctx.path.join(TOKEIRAD_TOML);
    let server_config =
        crate::commands::infra::read_tokeirad_config(&server_config_path)?;
    let Some(endpoint) = server_config.infrastructure.dsql.endpoint else {
        bail!(
            "dsql endpoint is not configured in {}",
            server_config_path.display()
        );
    };
    match action {
        SchemaAction::Setup { yes } => {
            super::require_confirmation(yes, "schema setup")?;
            println!("schema setup requested for DSQL endpoint {endpoint}");
        }
        SchemaAction::Status => {
            println!("DSQL endpoint configured: {endpoint}");
        }
    }
    Ok(())
}
