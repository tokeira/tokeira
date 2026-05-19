//! Tokeira autoscaler binary.
//!
//! Runs as a standalone process (separate from tokeirad) that:
//! 1. Acquires a singleton leader lease via DSQL shard_lease
//! 2. Reads cluster metrics from Mimir
//! 3. Runs three control loops (A: replica scaling, B: runtime scale-out,
//!    C: runtime retirement) on a configurable polling interval
//! 4. Applies scaling decisions via the platform-specific Actuator
//!
//! Only the leader instance runs the control loops. Non-leaders wait and
//! attempt to acquire the lease on each renewal interval.

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use time::Duration;
use tokeira_autoscaler::{
    config::AutoscalerServiceConfig,
    freshness::{FreshnessTracker, MetricFreshness, ScalingPermission},
    leader::AutoscalerLeader,
    loop_a::{ReplicaScalingLoop, ServicePressure, ServiceSignal},
    mimir::MimirClient,
    reconciler::{CurrentState, DesiredState},
};
use tokio::signal;
use tokio_util::sync::CancellationToken;
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

    info!(
        cluster = %config.cluster_name,
        mimir_endpoint = %config.mimir_endpoint,
        polling_interval_s = config.polling_interval.whole_seconds(),
        "starting tokeira-autoscaler"
    );

    let cancel = CancellationToken::new();
    let cancel_on_signal = cancel.clone();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        info!("shutdown signal received");
        cancel_on_signal.cancel();
    });

    let mimir = MimirClient::new(config.mimir_endpoint.clone(), config.staleness_threshold);

    // The leader election loop runs until cancelled. Only the leader
    // executes the scaling loops; non-leaders sleep and retry acquisition.
    run_leader_loop(config, mimir, cancel).await
}

/// Leader election + control loop orchestration.
///
/// On each iteration:
/// 1. Attempt to acquire/renew the leader lease
/// 2. If leader: read metrics, run loops A/B/C, reconcile, apply actions
/// 3. If not leader: sleep until next renewal attempt
async fn run_leader_loop(
    config: AutoscalerServiceConfig,
    mimir: MimirClient,
    cancel: CancellationToken,
) -> Result<()> {
    // TODO(ecs): construct a real LeaseRepository from DSQL config.
    // For now, the binary starts but cannot acquire leadership without
    // a storage backend. This will be wired when the ECS platform
    // provides the DSQL connection for the autoscaler's lease table.
    let lease_repo: Arc<dyn tokeira_storage::LeaseRepository> =
        Arc::new(tokeira_storage::InMemoryStore::default());

    let mut leader = AutoscalerLeader::new(
        lease_repo,
        uuid::Uuid::new_v4().to_string(),
        "autoscaler".to_string(),
        Duration::seconds(30),
        Duration::seconds(10),
    );

    let mut loop_a = ReplicaScalingLoop::default();
    let polling_interval =
        tokio::time::Duration::from_secs(config.polling_interval.whole_seconds().max(1) as u64);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("autoscaler shutting down");
                return Ok(());
            }
            _ = tokio::time::sleep(polling_interval) => {}
        }

        // Attempt to acquire or renew the leader lease.
        let is_leader = if leader.is_leader() {
            leader.renew().await.unwrap_or(false)
        } else {
            leader.try_acquire().await.unwrap_or(false)
        };

        if !is_leader {
            continue;
        }

        // Check metric freshness before making scaling decisions.
        let mimir_available = mimir.is_available().await;
        let freshness = FreshnessTracker {
            mimir_available,
            service_metrics: if mimir_available {
                MetricFreshness::Fresh
            } else {
                MetricFreshness::Missing
            },
            controller_snapshot: MetricFreshness::Fresh,
            dsql_headroom: MetricFreshness::Fresh,
            overload_signal: false,
        };

        if freshness.scaling_permission(true) == ScalingPermission::Freeze {
            warn!("metrics unavailable — freezing desired state");
            continue;
        }

        // Loop A: replica scaling based on service pressure signals.
        // TODO(ecs): query Mimir for per-service pressure signals
        // (schedule-to-start latency, poll success rate, queue depth).
        let signals: Vec<ServiceSignal> = Vec::new();
        let mut desired = DesiredState::default();
        let _actions = loop_a.apply_signals(&config, &mut desired, &signals);

        // Loop B: runtime scale-out.
        // TODO(ecs): query Mimir for runtime saturation signals.

        // Loop C: runtime retirement.
        // TODO(ecs): identify retirement candidates from controller state.

        // Reconcile desired vs current and apply actions via actuator.
        let current = CurrentState::default();
        let actions = tokeira_autoscaler::reconciler::reconcile(&desired, &current);

        if !actions.is_empty() {
            info!(action_count = actions.len(), "reconciler produced scaling actions");
            // TODO(ecs): apply actions via the platform Actuator trait.
            // The ECS actuator will be constructed from AWS SDK clients
            // and passed into this loop.
            for action in &actions {
                info!(?action, "would apply scaling action");
            }
        }
    }
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
