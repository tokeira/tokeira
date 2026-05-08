//! `tkr schema` — DSQL schema lifecycle.
//!
//! Only meaningful for deployments configured with DSQL storage; bails
//! cleanly otherwise so in-memory deployments don't need to pretend.
//!
//! The DSQL endpoint is discovered during `tkr infra apply` and written
//! back into `tokeirad.toml` by [`crate::commands::infra::write_tokeirad_writeback`];
//! this handler reads it from there rather than re-querying AWS.
//!
//! Today the `Setup` action only prints the endpoint it would target.
//! Actual schema migration goes through `temporal-dsql-tool` and is
//! tracked as dedicated work.

use anyhow::{Result, bail};
use tokeira_orchestrator::StorageKind;

use crate::{
    cli::SchemaAction,
    deployment_dir::{DeploymentContext, TOKEIRAD_TOML},
};

pub fn run(action: SchemaAction, ctx: DeploymentContext) -> Result<()> {
    if ctx.metadata.storage != StorageKind::Dsql {
        bail!("schema commands require dsql storage");
    }
    let server_config_path = ctx.path.join(TOKEIRAD_TOML);
    let server_config = crate::commands::infra::read_tokeirad_config(&server_config_path)?;
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
