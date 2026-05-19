use std::path::PathBuf;

use anyhow::{Context, Result};
use tokeira_autoscaler::{config::AutoscalerServiceConfig, mimir::MimirClient};
use tokio::signal;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing()?;
    let config_path = config_path_from_args()?;
    let config: AutoscalerServiceConfig = tokeira_config::load_config(&config_path, None)
        .with_context(|| {
            format!(
                "failed to load autoscaler config at {}",
                config_path.display()
            )
        })?;
    let mimir = MimirClient::new(config.mimir_endpoint.clone(), config.staleness_threshold);
    run(config, mimir).await
}

async fn run(config: AutoscalerServiceConfig, mimir: MimirClient) -> Result<()> {
    info!(
        cluster = %config.cluster_name,
        mimir_endpoint = %config.mimir_endpoint,
        "starting Tokeira autoscaler"
    );
    if !mimir.is_available().await {
        warn!(
            "Mimir is not ready; autoscaler will remain in degraded mode until metrics are available"
        );
    }
    signal::ctrl_c()
        .await
        .context("failed to wait for shutdown signal")?;
    info!("stopping Tokeira autoscaler");
    Ok(())
}

fn install_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .try_init()
        .context("failed to install tracing subscriber")
}

fn config_path_from_args() -> Result<PathBuf> {
    let mut args = std::env::args().skip(1);
    let Some(first) = args.next() else {
        return Ok(PathBuf::from("autoscaler.toml"));
    };
    if first == "--config" {
        args.next()
            .map(PathBuf::from)
            .context("--config requires a path")
    } else {
        Ok(PathBuf::from(first))
    }
}
