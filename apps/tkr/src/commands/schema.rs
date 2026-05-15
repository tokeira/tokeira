//! `tkr schema` — DSQL schema lifecycle.
//!
//! Schema commands operate from the selected deployment's `tokeirad.toml`
//! because infra writeback is the authoritative source of the DSQL endpoint
//! that the server will use at runtime.

use anyhow::{Context, Result, bail};
use sqlx::PgConnection;
use tokeira_orchestrator::StorageKind;
use tokeira_storage::dsql::{ConnectionFactory, DsqlAuthConfig, MigrationRunner};

use crate::{
    cli::SchemaAction,
    deployment_dir::{DeploymentContext, TOKEIRAD_TOML},
};

pub async fn run(action: SchemaAction, ctx: DeploymentContext) -> Result<()> {
    if ctx.metadata.storage != StorageKind::Dsql {
        bail!("schema commands require dsql storage");
    }
    let server_config_path = ctx.path.join(TOKEIRAD_TOML);
    match action {
        SchemaAction::Setup { yes } => {
            super::require_confirmation(yes, "schema setup")?;
            let server_config = crate::commands::infra::read_tokeirad_config(&server_config_path)?;
            let auth = dsql_auth_config(&server_config_path, &server_config)?;
            let mut connection = connect_admin(&auth).await?;
            let report = migration_runner().apply_connection(&mut connection).await?;
            println!("Applied {} DSQL schema migration(s)", report.applied);
        }
        SchemaAction::Status => {
            let server_config = crate::commands::infra::read_tokeirad_config(&server_config_path)?;
            let auth = dsql_auth_config(&server_config_path, &server_config)?;
            let mut connection = connect_admin(&auth).await?;
            let status = migration_runner()
                .status_connection(&mut connection)
                .await?;
            match status.current_version {
                Some(version) => {
                    println!(
                        "Schema version: V{version:03} (checked at {})",
                        status.checked_at
                    );
                }
                None => {
                    println!("Schema not initialized (no schema_version table)");
                }
            }
        }
        SchemaAction::Validate => {
            let issues = migration_runner().validate()?;
            if issues.is_empty() {
                println!("All migrations valid");
            } else {
                for issue in issues {
                    println!(
                        "{}:{}: {:?}: {}",
                        issue.file, issue.line, issue.kind, issue.message
                    );
                }
                bail!("schema validation found issues");
            }
        }
    }
    Ok(())
}

fn dsql_auth_config(
    server_config_path: &std::path::Path,
    server_config: &tokeira_config::TokeiraConfig,
) -> Result<DsqlAuthConfig> {
    let dsql = &server_config.infrastructure.dsql;
    let endpoint = dsql
        .endpoint
        .clone()
        .filter(|endpoint| !endpoint.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "dsql endpoint is not configured in {}; run `tkr infra apply --module dsql` first",
                server_config_path.display()
            )
        })?;
    Ok(DsqlAuthConfig {
        endpoint,
        region: dsql.region.clone(),
        admin_role_arn: dsql.admin_role_arn.clone(),
        runtime_role_arn: dsql.runtime_role_arn.clone(),
        readonly_role_arn: dsql.readonly_role_arn.clone(),
    })
}

async fn connect_admin(auth: &DsqlAuthConfig) -> Result<PgConnection> {
    let region = auth.resolved_region().ok_or_else(|| {
        anyhow::anyhow!("dsql region must be configured or derivable from endpoint")
    })?;
    ConnectionFactory::new(&auth.endpoint, &region)?
        .create_connection()
        .await
        .context("failed to connect to DSQL for schema command")
}

fn migration_runner() -> MigrationRunner {
    MigrationRunner::embedded()
}
