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
    actuator::Actuator,
    config::AutoscalerServiceConfig,
    envelope::ScalingEnvelope,
    freshness::{FreshnessTracker, MetricFreshness, ScalingPermission},
    leader::AutoscalerLeader,
    loop_a::ReplicaScalingLoop,
    loop_b::apply_runtime_scale_out,
    loop_c::{advance_drain_phase, request_runtime_retirement},
    mimir::MimirClient,
    reconciler::{CurrentState, DesiredState, ScalingAction},
    signals,
};
use tokeira_storage::{
    LeaseRepository,
    dsql::{DsqlAuthConfig, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore},
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

    // Construct the DSQL-backed lease repository for leader election.
    let lease_repo = build_lease_repository(&config).await?;

    let actuator = Arc::new(LoggingActuator);

    run_leader_loop(config, mimir, lease_repo, actuator, cancel).await
}

/// Build a LeaseRepository from the DSQL config fields.
///
/// The autoscaler uses the same shard_lease table as the runtime for its
/// singleton leader election. The connection pool is minimal — the autoscaler
/// only needs a handful of connections for lease operations.
async fn build_lease_repository(
    config: &AutoscalerServiceConfig,
) -> Result<Arc<dyn LeaseRepository>> {
    if config.dsql_endpoint.is_empty() {
        // Fallback for local development without DSQL.
        info!("dsql_endpoint not configured — using in-memory lease store");
        return Ok(Arc::new(tokeira_storage::InMemoryStore::default()));
    }

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

    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(auth.resolved_region().ok_or_else(
            || anyhow::anyhow!("cannot derive DSQL region from endpoint"),
        )?))
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
        .context("failed to connect DSQL storage backend for lease repository")?;

    let (_director, run_repository, _projection_log, _migration_runner) = dsql_store.into_parts();

    Ok(Arc::new(run_repository))
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
    lease_repo: Arc<dyn LeaseRepository>,
    actuator: Arc<dyn Actuator>,
    cancel: CancellationToken,
) -> Result<()> {
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

    let envelope = ScalingEnvelope {
        configured_max_runtime_hosts: 100,
        dsql_connection_budget: config.dsql_connection_budget,
        dsql_connection_rate_budget: config.dsql_connection_rate_budget,
        per_runtime_reserved_connections: config.per_runtime_reserved_connections,
        per_runtime_startup_connection_rate: config.per_runtime_startup_connection_rate,
    };

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

        let mut desired = DesiredState::default();

        // Loop A: replica scaling based on service pressure signals.
        let signals = signals::query_service_signals(&mimir, &config)
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "failed to query service signals");
                Vec::new()
            });
        let _actions = loop_a.apply_signals(&config, &mut desired, &signals);

        // Loop B: runtime scale-out gated by DSQL headroom.
        let runtime_input = signals::query_runtime_pressure(
            &mimir,
            envelope.effective_max_runtime_hosts().min(10), // current hosts placeholder
            1,
            config.per_runtime_reserved_connections,
        )
        .await
        .unwrap_or_else(|e| {
            warn!(error = %e, "failed to query runtime pressure");
            tokeira_autoscaler::loop_b::RuntimeScaleOutInput {
                current_hosts: 0,
                step: 1,
                pressure: tokeira_autoscaler::loop_b::RuntimePressure::None,
                dsql_headroom_available: false,
            }
        });
        apply_runtime_scale_out(&mut desired, "tokeira-runtime-asg", envelope, runtime_input);

        // Loop C: runtime retirement.
        // Target load per host: 70% CPU utilization is the saturation threshold.
        let retirement_candidates = signals::query_retirement_candidates(&mimir, 10, 0.70)
            .await
            .unwrap_or_else(|e| {
                warn!(error = %e, "failed to query retirement candidates");
                Vec::new()
            });
        for candidate in retirement_candidates {
            if request_runtime_retirement(&mut desired, candidate) {
                // Advance through drain phases for any newly-added candidates.
                // In production, each phase transition would be gated by
                // confirmation from the controller/platform.
            }
        }

        // Advance existing drain intents that are ready for the next phase.
        // TODO(shard-placement-membership): gate phase transitions on actual
        // controller confirmation (bundle migration complete, etc.).
        let drain_keys: Vec<String> = desired.drain_intents.keys().cloned().collect();
        for instance_id in &drain_keys {
            advance_drain_phase(&mut desired, instance_id);
        }

        // Reconcile desired vs current and apply actions via actuator.
        let current = CurrentState::default();
        let actions = tokeira_autoscaler::reconciler::reconcile(&desired, &current);

        if !actions.is_empty() {
            info!(
                action_count = actions.len(),
                "reconciler produced scaling actions"
            );
            for action in &actions {
                apply_action(&actuator, &config.cluster_name, action).await;
            }
        }
    }
}

async fn apply_action(actuator: &Arc<dyn Actuator>, cluster: &str, action: &ScalingAction) {
    let result = match action {
        ScalingAction::UpdateService {
            service,
            desired_count,
        } => actuator
            .update_service_desired_count(cluster, service, *desired_count)
            .await
            .map(|_| ()),
        ScalingAction::UpdateAsg {
            asg,
            desired_capacity,
        } => actuator
            .set_asg_desired_capacity(asg, *desired_capacity)
            .await
            .map(|_| ()),
        ScalingAction::AdvanceDrain { instance_id, phase } => {
            info!(?phase, instance_id, "advancing drain phase");
            Ok(())
        }
    };

    if let Err(e) = result {
        warn!(error = %e, ?action, "failed to apply scaling action");
    }
}

// ── Placeholder actuator ────────────────────────────────────────────────────

/// Logging-only actuator for development and initial wiring.
///
/// The real ECS actuator lives in `platforms/ecs/` and will be wired once
/// the ECS platform crate is integrated.
#[derive(Debug)]
struct LoggingActuator;

#[async_trait::async_trait]
impl Actuator for LoggingActuator {
    async fn update_service_desired_count(
        &self,
        cluster: &str,
        service: &str,
        desired: u32,
    ) -> Result<bool> {
        info!(
            cluster,
            service, desired, "would update service desired count"
        );
        Ok(true)
    }

    async fn set_asg_desired_capacity(&self, asg_name: &str, desired: u32) -> Result<bool> {
        info!(asg_name, desired, "would set ASG desired capacity");
        Ok(true)
    }

    async fn drain_container_instance(
        &self,
        cluster: &str,
        container_instance_arn: &str,
    ) -> Result<()> {
        info!(
            cluster,
            container_instance_arn, "would drain container instance"
        );
        Ok(())
    }

    async fn clear_instance_protection(&self, asg_name: &str, instance_id: &str) -> Result<()> {
        info!(asg_name, instance_id, "would clear instance protection");
        Ok(())
    }

    async fn terminate_instance_with_decrement(&self, instance_id: &str) -> Result<()> {
        info!(instance_id, "would terminate instance with decrement");
        Ok(())
    }

    async fn describe_service(
        &self,
        _cluster: &str,
        _service: &str,
    ) -> Result<tokeira_autoscaler::actuator::ServiceState> {
        Ok(tokeira_autoscaler::actuator::ServiceState {
            desired_count: 0,
            running_count: 0,
        })
    }

    async fn describe_asg(
        &self,
        _asg_name: &str,
    ) -> Result<tokeira_autoscaler::actuator::AsgState> {
        Ok(tokeira_autoscaler::actuator::AsgState {
            desired_capacity: 0,
            min_size: 0,
            max_size: 100,
        })
    }

    async fn resolve_container_instance_for_ec2(
        &self,
        _cluster: &str,
        ec2_instance_id: &str,
    ) -> Result<String> {
        Ok(format!("arn:placeholder:{ec2_instance_id}"))
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
