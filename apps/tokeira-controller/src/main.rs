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
use connectrpc::Router;
use tokeira_controller::{
    ConnectPlacementController, PlacementControllerState, metrics as controller_metrics,
};
use tokeira_observability::{
    ErrorBiasedSamplingReason, LogFormat, MetricManifest, ObservabilityRuntime, OtlpMetricsConfig,
    PROCESS_METRIC_MANIFEST, ProcessObservabilityConfig, ReadinessHandle, ReadinessRegistry,
    ReadinessStatus, ServiceName, TraceExportConfig, install_observability,
    mark_error_biased_sample,
};
use tokeira_proto::connect::tokeira::internal::controller::v1::PlacementControllerExt;
use tokeira_storage::{
    ControlRepository, LeaseRepository,
    dsql::{DsqlAuthConfig, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore},
};
use tokeira_types::IncarnationId;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

static PROCESS_MANIFESTS: &[&MetricManifest] = &[&PROCESS_METRIC_MANIFEST];

#[derive(Clone, Debug)]
struct ControllerReadiness {
    registry: ReadinessRegistry,
    storage: ReadinessHandle,
    placement: ReadinessHandle,
    membership: ReadinessHandle,
}

impl ControllerReadiness {
    fn new() -> Self {
        let storage = readiness_handle("storage");
        let placement = readiness_handle("placement_state");
        let membership = readiness_handle("membership_streams");
        let registry = ReadinessRegistry::from_handles(vec![
            storage.clone(),
            placement.clone(),
            membership.clone(),
        ]);
        Self {
            registry,
            storage,
            placement,
            membership,
        }
    }
}

fn readiness_handle(name: &'static str) -> ReadinessHandle {
    let (_, handle) = ReadinessRegistry::mutable(
        name,
        ReadinessStatus::NotReady,
        Some("component is still starting".to_string()),
    );
    handle
}

#[tokio::main]
async fn main() -> Result<()> {
    let config_path = config_path_from_args()?;
    let config: ControllerProcessConfig = tokeira_config::load_config(&config_path, None)
        .with_context(|| {
            format!(
                "failed to load controller config at {}",
                config_path.display()
            )
        })?;
    config.validate()?;

    let readiness = ControllerReadiness::new();
    let _observability = install_process_observability(&config, readiness.registry.clone()).await?;
    log_build_info("tokeira-controller");
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

    // DSQL storage backend — required for production operation.
    let (lease_repo, control_repo) = build_repositories(&config).await?;
    readiness.storage.ready();

    // Library components wired from the shared repositories.
    let state = PlacementControllerState::new(
        controller_config,
        Arc::clone(&lease_repo),
        Arc::clone(&control_repo),
    );
    readiness.placement.ready();

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

    // gRPC server with the PlacementController service via connect-rust.
    // Serves Connect, gRPC, and gRPC-Web on the same handlers.
    let grpc_addr: SocketAddr = config
        .grpc_listen_addr
        .parse()
        .context("invalid grpc_listen_addr")?;
    let grpc_cancel = cancel.clone();

    let connect_service = Arc::new(ConnectPlacementController::new(state));
    let connect_router = connect_service.register(Router::new());
    let app = axum::Router::new().fallback_service(connect_router.into_axum_service());
    readiness.membership.ready();

    let grpc_handle = tokio::spawn(async move {
        info!(%grpc_addr, "gRPC server listening (connect-rust)");
        let listener = tokio::net::TcpListener::bind(grpc_addr)
            .await
            .expect("failed to bind gRPC listener");
        axum::serve(listener, app)
            .with_graceful_shutdown(grpc_cancel.cancelled_owned())
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

fn log_build_info(process: &'static str) {
    let info = tokeira_build_info::summary();
    info!(
        process,
        tokeira_version = info.tokeira_version,
        tokeira_git_sha = info.tokeira_git_sha,
        temporal_proto_version = info.temporal_proto_version,
        temporal_server_compat = info.temporal_server_compat,
        rust_toolchain = info.rust_toolchain,
        source_tree_hash = info.source_tree_hash,
        feature_matrix_digest = info.feature_matrix_digest,
        sdk_matrix_digest = info.sdk_matrix_digest,
        build_mode = info.build_mode,
        "tokeira build provenance"
    );
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

        let loop_started = std::time::Instant::now();
        let generation = match state.generation.current_generation().await {
            Ok(current) => current,
            Err(err) => {
                mark_error_biased_sample(ErrorBiasedSamplingReason::ControllerPlacementError);
                warn!(%err, "placement loop: failed to read current generation");
                controller_metrics::record_placement_loop_duration(loop_started.elapsed());
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
                mark_error_biased_sample(ErrorBiasedSamplingReason::ControllerPlacementError);
                warn!(%err, "placement loop: generation advance failed");
            }
        }
        controller_metrics::record_placement_loop_duration(loop_started.elapsed());
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
        config.dsql_endpoint.split('.').nth(2).map(|s| s.to_owned())
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
        },
        ..DsqlPoolConfig::default()
    };

    let dsql_store = DsqlStore::connect(auth, pool_config, ddb_client)
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

async fn install_process_observability(
    config: &ControllerProcessConfig,
    readiness: ReadinessRegistry,
) -> Result<ObservabilityRuntime> {
    install_observability(
        process_observability_config(config)?,
        PROCESS_MANIFESTS,
        readiness,
    )
    .await
    .context("failed to install controller observability")
}

fn process_observability_config(
    config: &ControllerProcessConfig,
) -> Result<ProcessObservabilityConfig> {
    let metrics_addr: SocketAddr = config
        .metrics_addr
        .parse()
        .context("invalid metrics_addr for observability endpoint")?;
    Ok(ProcessObservabilityConfig {
        service_name: ServiceName::Controller,
        cluster_name: config.cluster_name.clone(),
        deployment_name: config.cluster_name.clone(),
        node_id: None,
        task_id: None,
        metrics_enabled: true,
        metrics_addr,
        log_format: LogFormat::Text,
        log_filter: std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
        otlp_metrics: OtlpMetricsConfig::default(),
        tracing: TraceExportConfig::default(),
        shutdown_flush_timeout: std::time::Duration::from_secs(5),
        redacted_config: None,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn controller_config() -> ControllerProcessConfig {
        ControllerProcessConfig {
            dsql_endpoint: "cluster.dsql.eu-west-2.on.aws".to_string(),
            dsql_region: "eu-west-2".to_string(),
            grpc_listen_addr: "127.0.0.1:0".to_string(),
            metrics_addr: "127.0.0.1:0".to_string(),
            placement_interval_secs: 5,
            budget_interval_secs: 10,
            cluster_name: "test-cluster".to_string(),
            dsql_connection_rate_budget: 100.0,
            dsql_connection_capacity_budget: 10_000,
            placement: config::PlacementTable::default(),
            membership: config::MembershipTable::default(),
        }
    }

    #[test]
    fn controller_process_observability_config_validates() {
        let observability = process_observability_config(&controller_config()).unwrap();

        observability.validate().unwrap();
        assert_eq!(observability.service_name.as_str(), "tokeira-controller");
        assert_eq!(observability.cluster_name, "test-cluster");
    }

    #[test]
    fn controller_process_manifest_validates() {
        tokeira_observability::validate_manifests(&[&PROCESS_METRIC_MANIFEST]).unwrap();
    }

    #[tokio::test]
    async fn controller_readiness_handles_track_startup_state() {
        let readiness = ControllerReadiness::new();

        assert!(
            readiness
                .registry
                .check_all()
                .await
                .iter()
                .all(|check| check.status == ReadinessStatus::NotReady)
        );

        readiness.storage.ready();
        readiness.placement.ready();
        readiness.membership.ready();

        assert!(
            readiness
                .registry
                .check_all()
                .await
                .iter()
                .all(|check| check.status == ReadinessStatus::Ready)
        );
    }
}
