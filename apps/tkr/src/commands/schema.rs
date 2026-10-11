//! `tkr schema` — DSQL schema lifecycle.
//!
//! Schema commands operate from the selected deployment's `tokeirad.toml`
//! because infra writeback makes it the authoritative source of the storage
//! the server uses at runtime and of its DSQL endpoint.

use anyhow::{Context, Result, bail};
use sqlx::PgConnection;
use tokeira_config::ConfigStorageKind;
use tokeira_storage::dsql::{ConnectionFactory, DsqlAuthConfig, MigrationRunner};

use crate::{
    cli::SchemaAction,
    deployment_dir::{DeploymentRecordContext, TOKEIRAD_TOML},
};

pub(crate) async fn run(action: SchemaAction, ctx: DeploymentRecordContext) -> Result<()> {
    // The deployment's `tokeirad.toml` decides, as it decides for tokeirad
    // itself. Its metadata can't: `tkr deployment create` records in-memory
    // storage for every discovered platform, and an infrastructure apply then
    // selects DSQL in this file alone.
    let server_config_path = ctx.path.join(TOKEIRAD_TOML);
    let server_config = crate::commands::infra::read_tokeirad_config(&server_config_path)
        .with_context(|| format!("failed to read {}", server_config_path.display()))?;
    if server_config.infrastructure.storage != ConfigStorageKind::Dsql {
        bail!(
            "schema commands require dsql storage, and {} doesn't select \
             infrastructure.storage = \"dsql\"",
            server_config_path.display()
        );
    }
    match action {
        SchemaAction::Setup { yes } => {
            super::require_confirmation(yes, "schema setup")?;
            let auth = dsql_auth_config(&server_config_path, &server_config)?;
            let mut connection = connect_admin(&auth).await?;
            let report = migration_runner().apply_connection(&mut connection).await?;
            println!("Applied {} DSQL schema migration(s)", report.applied);
        }
        SchemaAction::Status => {
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

#[cfg(test)]
mod tests {
    use std::fs;

    use tokeira_orchestrator::{PlatformId, StorageKind};

    use super::*;
    use crate::deployment_dir::{DeploymentResolver, load_record_context};

    /// A deployment whose metadata records `metadata_storage` and whose
    /// `tokeirad.toml` selects `server_storage`, with no DSQL endpoint.
    fn deployment(
        metadata_storage: StorageKind,
        server_storage: &str,
    ) -> (tempfile::TempDir, DeploymentRecordContext) {
        let root = tempfile::tempdir().unwrap();
        let deployments = DeploymentResolver::with_root(root.path().to_path_buf());
        deployments
            .create(
                "schema-gate",
                PlatformId::new("local").unwrap(),
                metadata_storage,
                None,
            )
            .unwrap();
        let ctx = load_record_context(&deployments, Some("schema-gate")).unwrap();
        fs::write(
            ctx.path.join(TOKEIRAD_TOML),
            format!("[infrastructure]\nstorage = \"{server_storage}\"\n"),
        )
        .unwrap();
        (root, ctx)
    }

    /// A Compose or ECS deployment's metadata records in-memory storage while
    /// its `tokeirad.toml` selects DSQL: `tkr schema` follows the file, so it
    /// goes on to look for the endpoint.
    #[tokio::test]
    async fn schema_setup_follows_the_server_configuration_over_the_metadata() {
        let (_root, ctx) = deployment(StorageKind::InMemory, "dsql");
        let error = run(SchemaAction::Setup { yes: true }, ctx)
            .await
            .expect_err("no endpoint is configured");
        assert!(
            format!("{error:#}").contains("dsql endpoint is not configured"),
            "{error:#}"
        );
    }

    // Feature: distributed-schema-startup-check, Property 4: `tkr schema`
    // follows the server configuration
    #[tokio::test]
    async fn property_schema_commands_follow_the_server_configuration() {
        for metadata_storage in [StorageKind::InMemory, StorageKind::Dsql] {
            for server_storage in ["in-memory", "dsql"] {
                for (name, action) in [
                    ("setup", SchemaAction::Setup { yes: true }),
                    ("status", SchemaAction::Status),
                    ("validate", SchemaAction::Validate),
                ] {
                    let label = format!(
                        "{metadata_storage:?} metadata, {server_storage} server config, schema {name}"
                    );
                    let (_root, ctx) = deployment(metadata_storage, server_storage);
                    let outcome = run(action, ctx).await;
                    let refused = matches!(
                        &outcome,
                        Err(error) if format!("{error:#}").contains("schema commands require dsql storage")
                    );
                    assert_eq!(refused, server_storage != "dsql", "{label}: {outcome:?}");
                }
            }
        }
    }
}
