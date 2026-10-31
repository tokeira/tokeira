//! Offline storage review/apply entry point. It never constructs a runtime,
//! takes a node claim, or opens a listener. Even a refused plan closes the normal
//! DSQL director so its connection-slot allocation is released.

use anyhow::{Context, Result, ensure};
use std::path::Path;
use tokeira_config::{ConfigStorageKind, OutboundRecoveryCommand, StorageCommand, TokeiraConfig};
use tokeira_storage::{
    dsql::DsqlStore,
    outbound_upgrade::{OutboundRepair, apply_outbound_recovery, plan_outbound_recovery},
};

async fn read_repairs(path: Option<&Path>) -> Result<Vec<OutboundRepair>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    ensure!(
        tokio::fs::metadata(path).await?.len() <= 1024 * 1024,
        "repair evidence exceeds 1 MiB"
    );
    let bytes = tokio::fs::read(path)
        .await
        .context("cannot read repair evidence")?;
    ensure!(bytes.len() <= 1024 * 1024, "repair evidence exceeds 1 MiB");
    serde_json::from_slice(&bytes).context("invalid outbound repair JSON")
}

/// CLI JSON is a reviewed, payload-free storage report, not a workflow dump.
#[allow(clippy::print_stdout)] // Operator command output is the review artifact.
pub(super) async fn run(command: StorageCommand) -> Result<()> {
    let StorageCommand::OutboundRecovery { config, command } = command;
    let (config, _) = TokeiraConfig::resolve(config.as_deref())?;
    ensure!(
        matches!(config.infrastructure.storage, ConfigStorageKind::Dsql),
        "outbound storage maintenance requires an explicit DSQL configuration"
    );
    let repairs = match &command {
        OutboundRecoveryCommand::Plan { repairs }
        | OutboundRecoveryCommand::Apply { repairs, .. } => {
            read_repairs(repairs.as_deref()).await?
        }
    };
    let auth = super::dsql_auth_config(&config)?;
    let (pool, client) = super::dsql_pool_config(&config, &auth).await?;
    let store = DsqlStore::connect(auth, pool, client).await?;
    let result = async {
        super::check_distributed_schema(&store).await?;
        let repo = store.run_repository();
        match command {
            OutboundRecoveryCommand::Plan { .. } => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&plan_outbound_recovery(repo, &repairs).await?)?
                );
            }
            OutboundRecoveryCommand::Apply {
                plan_digest,
                confirm_stopped_writers,
                ..
            } => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &apply_outbound_recovery(
                            repo,
                            &plan_digest,
                            confirm_stopped_writers,
                            &repairs
                        )
                        .await?
                    )?
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let shutdown = store.shutdown().await;
    result?;
    shutdown.context("storage maintenance finished but connection shutdown failed")
}
