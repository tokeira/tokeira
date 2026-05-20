//! Tokeira placement controller binary.
//!
//! Active-active controller that orchestrates bundle placement, routing
//! snapshot publication, and DSQL connection budget allocation across
//! runtime nodes. Multiple controller instances race on CAS operations —
//! no leader election required.

mod config;

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use config::ControllerProcessConfig;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokeira_controller::PlacementControllerState;
use tokeira_proto::controller::placement_controller_server::PlacementControllerServer;
use tokeira_storage::{
    ControlRepository, LeaseRepository,
    dsql::{DsqlAuthConfig, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore},
};
use tokeira_types::IncarnationId;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<()> {
    install_tracing()?;

    let config_path = config_path_from_args()?;
    let config: ControllerProcessConfig = tokeira_config::load_config(&config_path, None)
        .with_context(|| {
            format!(
                "failed to load controller config at {}",
                config_path.display()
            )
        })?;
    config.validate()?;

    let controller_config = config.to_controller_config();

    info!(
        cluster = %config.cluster_name,
        grpc_addr = %config.grpc_listen_addr,
        metrics_addr = %config.metrics_addr,
        placement_interval_s = config.placement_interval_secs,
        budget_interval_s = config.budget_interval_secs,
        "starting tokeira-controller"
    );

    let cancel = CancellationToken::new();

    // Graceful shutdown on SIGTERM or ctrl-c.
    let cancel_on_signal = cancel.clone();
    tokio::spawn(async move {
        let ctrl_c = signal::ctrl_c();
        #[cfg(unix)]
        let mut sigterm =
            signal::unix::signal(signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        #[cfg(unix)]
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = ctrl_c.await;
        info!("shutdown signal received");
        cancel_on_signal.cancel();
    });

    // Prometheus metrics endpoint.
    install_metrics(&config.metrics_addr)?;

    // DSQL storage backend — required for production operation.
    let (lease_repo, control_repo) = build_repositories(&config).await?;

    // Library components wired from the shared repositories.
    let state = PlacementControllerState::new(
        controller_config,
        Arc::clone(&lease_repo),
        Arc::clone(&control_repo),
    );

    // Unique identity for this controller instance's CAS budget allocations.
    let allocator_id = IncarnationId::new();

    // Spawn the placement loop: scan leases → compute snapshot → CAS advance
    // generation → publish to subscribers → send desired placement directives.
    let placement_state = state.clone();
    let placement_cancel = cancel.clone();
    let placement_interval =
        tokio::time::Duration::from_secs(config.placement_interval_secs.max(1));
    let placement_handle = tokio::spawn(async move {
        run_placement_loop(placement_state, placement_interval, placement_cancel).await;
    });

    // Spawn the budget allocation loop: periodic CAS allocation → compute
    // per-node shares → send ConnectionBudgetDirective over membership streams.
    let budget_state = state.clone();
    let budget_cancel = cancel.clone();
    let budget_interval = tokio::time::Duration::from_secs(config.budget_interval_secs.max(1));
    let budget_handle = tokio::spawn(async move {
        run_budget_loop(budget_state, allocator_id, budget_interval, budget_cancel).await;
    });

    // gRPC server with the PlacementController service.
    let grpc_addr: SocketAddr = config
        .grpc_listen_addr
        .parse()
        .context("invalid grpc_listen_addr")?;
    let grpc_cancel = cancel.clone();
    let grpc_handle = tokio::spawn(async move {
        info!(%grpc_addr, "gRPC server listening");
        Server::builder()
            .add_service(PlacementControllerServer::new(state))
            .serve_with_shutdown(grpc_addr, grpc_cancel.cancelled_owned())
            .await
            .context("gRPC server failed")
    });

    // Await shutdown — cancel token propagates to all loops.
    cancel.cancelled().await;
    info!("cancellation propagated, draining loops");

    // Allow loops to finish their current iteration.
    let _ = tokio::join!(placement_handle, budget_handle);
    // gRPC server drains in-flight RPCs after shutdown signal.
    let _ = grpc_handle.await;

    info!("tokeira-controller shut down cleanly");
    Ok(())
}

/// Periodic placement loop.
///
/// Each tick: read lease state from DSQL, compute the routing snapshot,
/// advance the generation counter via CAS, and log the result. Directive
/// publication to connected runtimes happens reactively through the
/// membership stream (handled by `PlacementControllerState`).
async fn run_placement_loop(
    state: PlacementControllerState,
    interval: tokio::time::Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }

        let generation = match state.generation.current_generation().await {
            Ok(current) => current,
            Err(err) => {
                warn!(%err, "placement loop: failed to read current generation");
                continue;
            }
        };

        match state.advance_snapshot_generation(generation).await {
            Ok(new_gen) => {
                tracing::debug!(
                    previous = generation.0,
                    current = new_gen.0,
                    "placement loop: generation advanced"
                );
            }
            Err(err) => {
                warn!(%err, "placement loop: generation advance failed");
            }
        }
    }

    info!("placement loop exited");
}

/// Periodic budget allocation loop.
///
/// Each tick: attempt CAS allocation of the cluster-wide DSQL connection
/// budget, then compute per-node shares. Budget directives are delivered
/// to runtimes through their membership streams.
async fn run_budget_loop(
    state: PlacementControllerState,
    allocator_id: IncarnationId,
    interval: tokio::time::Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }

        match state.allocate_connection_budgets(allocator_id).await {
            Ok(budgets) if !budgets.is_empty() => {
                tracing::debug!(
                    node_count = budgets.len(),
                    "budget loop: allocated connection budgets"
                );
            }
            Ok(_) => {
                // CAS conflict — another controller won this cycle.
                tracing::trace!("budget loop: CAS conflict, skipping cycle");
            }
            Err(err) => {
                warn!(%err, "budget loop: allocation failed");
            }
        }
    }

    info!("budget loop exited");
}

/// Build DSQL-backed lease and control repositories.
///
/// The controller requires DSQL — there is no in-memory fallback. If the
/// endpoint is misconfigured, we fail fast during startup.
async fn build_repositories(
    config: &ControllerProcessConfig,
) -> Result<(Arc<dyn LeaseRepository>, Arc<dyn ControlRepository>)> {
    let region = if config.dsql_region.is_empty() {
        // Derive region from endpoint (e.g., "cluster.dsql.us-east-1.on.aws").
        config
            .dsql_endpoint
            .split('.')
            .nth(2)
            .map(|s| s.to_owned())
    } else {
        Some(config.dsql_region.clone())
    };

    let auth = DsqlAuthConfig {
        endpoint: config.dsql_endpoint.clone(),
        region,
        ..DsqlAuthConfig::default()
    };

    let resolved_region = auth
        .resolved_region()
        .ok_or_else(|| anyhow::anyhow!("cannot derive DSQL region from endpoint"))?;

    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(resolved_region))
        .load()
        .await;

    let ddb_client = aws_sdk_dynamodb::Client::new(&sdk_config);

    let pool_config = DsqlPoolConfig {
        coordination: DsqlCoordinationConfig {
            rate_limiter_table: format!("{}-dsql-rate-limiter", config.cluster_name),
            conn_lease_table: format!("{}-dsql-conn-lease", config.cluster_name),
            ddb_client,
        },
        ..DsqlPoolConfig::default()
    };

    let dsql_store = DsqlStore::connect(auth, pool_config)
        .await
        .context("failed to connect DSQL storage backend")?;

    let (_director, run_repository, _projection_log, _migration_runner) = dsql_store.into_parts();

    // DsqlRunRepository implements both LeaseRepository and ControlRepository.
    let repo = Arc::new(run_repository);
    Ok((
        Arc::clone(&repo) as Arc<dyn LeaseRepository>,
        repo as Arc<dyn ControlRepository>,
    ))
}

fn install_tracing() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer())
        .try_init()
        .context("failed to install tracing subscriber")
}

fn install_metrics(metrics_addr: &str) -> Result<()> {
    let addr: SocketAddr = metrics_addr
        .parse()
        .context("invalid metrics_addr for Prometheus exporter")?;
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .context("failed to install Prometheus metrics exporter")?;
    info!(%addr, "Prometheus metrics endpoint listening");
    Ok(())
}

fn config_path_from_args() -> Result<PathBuf> {
    let mut args = std::env::args().skip(1);
    let Some(first) = args.next() else {
        return Ok(PathBuf::from("controller.toml"));
    };
    if first == "--config" {
        args.next()
            .map(PathBuf::from)
            .context("--config requires a path")
    } else {
        Ok(PathBuf::from(first))
    }
}
