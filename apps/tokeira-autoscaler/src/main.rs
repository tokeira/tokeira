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

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use async_trait::async_trait;
use time::Duration;
use tokeira_autoscaler::{
    actuator::Actuator,
    config::AutoscalerServiceConfig,
    controller_client::{ControllerClient, PlacementControl},
    envelope::ScalingEnvelope,
    freshness::{FreshnessTracker, MetricFreshness, ScalingPermission},
    leader::AutoscalerLeader,
    loop_a::{ReplicaScalingLoop, ServicePressure},
    loop_b::apply_runtime_scale_out,
    loop_c::{RetirementLoop, apply_drain_phase},
    metrics as autoscaler_metrics,
    mimir::MimirClient,
    reconciler::{CurrentState, DesiredState, ScalingAction},
    signals,
};
use tokeira_observability::{
    AutoscalerLoopLabel, ErrorBiasedSamplingReason, LogFormat, MetricManifest,
    ObservabilityRuntime, OtlpMetricsConfig, PROCESS_METRIC_MANIFEST, ProcessObservabilityConfig,
    ReadinessCheck, ReadinessCheckResult, ReadinessHandle, ReadinessRegistry, ReadinessStatus,
    ScalingDirectionLabel, ServiceName, TraceExportConfig, install_observability,
    mark_error_biased_sample,
};
use tokeira_storage::{
    LeaseRepository,
    dsql::{DsqlAuthConfig, DsqlCoordinationConfig, DsqlPoolConfig, DsqlStore},
};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

static PROCESS_MANIFESTS: &[&MetricManifest] = &[&PROCESS_METRIC_MANIFEST];

/// Auto Scaling group backing the runtime fleet (045: the runtime scale lever
/// is ASG desired capacity, not ECS desired count).
const RUNTIME_ASG_NAME: &str = "tokeira-runtime-asg";
/// CPU utilisation treated as one host's worth of load when judging excess
/// capacity: the saturation threshold Loop B scales out at.
const RETIREMENT_TARGET_LOAD_PER_HOST: f64 = 0.70;

#[derive(Clone, Debug)]
struct AutoscalerReadiness {
    registry: ReadinessRegistry,
    leader_lease: ReadinessHandle,
    control_plane: ReadinessHandle,
}

#[derive(Debug)]
struct MimirReadinessCheck {
    client: MimirClient,
}

#[async_trait]
impl ReadinessCheck for MimirReadinessCheck {
    fn name(&self) -> &'static str {
        "mimir"
    }

    async fn check(
        &self,
    ) -> Result<ReadinessCheckResult, tokeira_observability::ObservabilityError> {
        if self.client.is_available().await {
            Ok(ReadinessCheckResult::new(ReadinessStatus::Ready))
        } else {
            Ok(ReadinessCheckResult::with_message(
                ReadinessStatus::NotReady,
                "Mimir readiness endpoint is unavailable",
            ))
        }
    }
}

impl AutoscalerReadiness {
    fn new(mimir: MimirClient) -> Self {
        let leader_lease = readiness_handle("leader_lease");
        let control_plane = readiness_handle("control_plane");
        let registry = ReadinessRegistry::new(vec![
            Arc::new(MimirReadinessCheck { client: mimir }) as Arc<dyn ReadinessCheck>,
            leader_lease.as_check(),
            control_plane.as_check(),
        ]);
        Self {
            registry,
            leader_lease,
            control_plane,
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
    let (config_source, config_label) = config_source_from_args()?;
    let config: AutoscalerServiceConfig =
        tokeira_config::load_config_from_source(&config_source, None)
            .with_context(|| format!("failed to load autoscaler config from {config_label}"))?;

    let mimir = MimirClient::new(config.mimir_endpoint.clone(), config.staleness_threshold);
    let readiness = AutoscalerReadiness::new(mimir.clone());
    let _observability = install_process_observability(&config, readiness.registry.clone()).await?;
    log_build_info("tokeira-autoscaler");

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

    // Construct the DSQL-backed lease repository for leader election.
    let lease_repo = build_lease_repository(&config).await?;
    readiness.leader_lease.ready();

    let actuator = Arc::new(LoggingActuator);
    let controller = build_controller_client(&config)?;
    readiness.control_plane.ready();

    run_leader_loop(config, mimir, lease_repo, actuator, controller, cancel).await
}

/// Build the placement-controller client Loop C retires runtime nodes through.
///
/// Without an endpoint the autoscaler still scales replicas and runtime hosts
/// out, but never retires a node: it has no one to nominate a safe candidate
/// or confirm a drain, and guessing would terminate hosts that own bundles.
fn build_controller_client(
    config: &AutoscalerServiceConfig,
) -> Result<Option<Arc<dyn PlacementControl>>> {
    let Some(endpoint) = config.controller_endpoint.as_deref() else {
        warn!(
            "controller_endpoint not configured — runtime retirement (loop C) is disabled; \
             runtime hosts are never terminated by this autoscaler"
        );
        return Ok(None);
    };
    let client = ControllerClient::new(endpoint)
        .with_context(|| format!("invalid controller_endpoint {endpoint}"))?;
    info!(controller_endpoint = endpoint, "runtime retirement enabled");
    Ok(Some(Arc::new(client)))
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
        },
        ..DsqlPoolConfig::default()
    };

    let dsql_store = DsqlStore::connect(auth, pool_config, ddb_client)
        .await
        .context("failed to connect DSQL storage backend for lease repository")?;

    let (_director, run_repository, _projection_log, _worker_deployments, _migration_runner) =
        dsql_store.into_parts();

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
    controller: Option<Arc<dyn PlacementControl>>,
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
    // Retirements span many polls; their phases live here for the leader's
    // lifetime rather than in the per-cycle desired state.
    let mut loop_c = RetirementLoop::default();
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
        autoscaler_metrics::set_active_reconciler_lease_held(is_leader);

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
            autoscaler_metrics::record_stale_metrics("mimir");
            warn!("metrics unavailable — freezing desired state");
            continue;
        }

        let mut desired = DesiredState::default();

        // Loop A: replica scaling based on service pressure signals.
        let loop_started = std::time::Instant::now();
        let signals = signals::query_service_signals(&mimir, &config)
            .await
            .unwrap_or_else(|e| {
                mark_error_biased_sample(ErrorBiasedSamplingReason::AutoscalerReconciliationError);
                warn!(error = %e, "failed to query service signals");
                Vec::new()
            });
        let _actions = loop_a.apply_signals(&config, &mut desired, &signals);
        for signal in &signals {
            let (direction, reason) = match signal.pressure {
                ServicePressure::ScaleOut => (ScalingDirectionLabel::Up, "service_pressure"),
                ServicePressure::ScaleIn => (ScalingDirectionLabel::Down, "low_utilization"),
                ServicePressure::Hold => (ScalingDirectionLabel::Hold, "hysteresis"),
            };
            autoscaler_metrics::record_scaling_decision(
                AutoscalerLoopLabel::Replica,
                direction,
                reason,
            );
        }
        for (service, replicas) in &desired.service_counts {
            autoscaler_metrics::set_desired_replicas(service, *replicas);
        }
        autoscaler_metrics::record_loop_duration(
            AutoscalerLoopLabel::Replica,
            loop_started.elapsed(),
        );

        // The runtime fleet's size is the platform's own report, read once per
        // cycle for both runtime loops. A guessed count would let a small,
        // lightly loaded fleet look oversized, and with retirement marking
        // nodes through the controller that would drain a real node each
        // cycle. When the platform cannot be read, both loops hold, including
        // open retirements, which do not advance without a fleet to reason
        // about.
        let runtime_fleet = match actuator.describe_asg(RUNTIME_ASG_NAME).await {
            Ok(fleet) => Some(fleet),
            Err(e) => {
                mark_error_biased_sample(ErrorBiasedSamplingReason::AutoscalerReconciliationError);
                warn!(error = %e, "failed to describe the runtime fleet; scale-out and retirement hold");
                autoscaler_metrics::record_scaling_decision(
                    AutoscalerLoopLabel::ScaleOut,
                    ScalingDirectionLabel::Hold,
                    "fleet_unknown",
                );
                autoscaler_metrics::record_scaling_decision(
                    AutoscalerLoopLabel::Retirement,
                    ScalingDirectionLabel::Hold,
                    "fleet_unknown",
                );
                None
            }
        };

        // Loop B: runtime scale-out gated by DSQL headroom.
        let loop_started = std::time::Instant::now();
        if let Some(fleet) = &runtime_fleet {
            let runtime_input = signals::query_runtime_pressure(
                &mimir,
                fleet.desired_capacity,
                1,
                config.per_runtime_reserved_connections,
            )
            .await
            .unwrap_or_else(|e| {
                mark_error_biased_sample(ErrorBiasedSamplingReason::AutoscalerReconciliationError);
                warn!(error = %e, "failed to query runtime pressure");
                tokeira_autoscaler::loop_b::RuntimeScaleOutInput {
                    current_hosts: fleet.desired_capacity,
                    step: 1,
                    pressure: tokeira_autoscaler::loop_b::RuntimePressure::None,
                    dsql_headroom_available: false,
                }
            });
            let direction = match runtime_input.pressure {
                tokeira_autoscaler::loop_b::RuntimePressure::BroadSaturation
                    if runtime_input.dsql_headroom_available =>
                {
                    ScalingDirectionLabel::Up
                }
                _ => ScalingDirectionLabel::Hold,
            };
            autoscaler_metrics::record_scaling_decision(
                AutoscalerLoopLabel::ScaleOut,
                direction,
                "runtime_pressure",
            );
            apply_runtime_scale_out(&mut desired, RUNTIME_ASG_NAME, envelope, runtime_input);
        }
        autoscaler_metrics::record_loop_duration(
            AutoscalerLoopLabel::ScaleOut,
            loop_started.elapsed(),
        );

        // Loop C: runtime retirement. Mimir answers whether the fleet has
        // excess capacity against the size the platform reports; the
        // controller chooses the node and confirms, from the node's own
        // heartbeat, when it is safe to terminate.
        let loop_started = std::time::Instant::now();
        if let (Some(controller), Some(fleet)) = (controller.as_deref(), &runtime_fleet) {
            let excess_capacity = signals::query_excess_runtime_capacity(
                &mimir,
                fleet,
                RETIREMENT_TARGET_LOAD_PER_HOST,
            )
            .await
            .unwrap_or_else(|e| {
                mark_error_biased_sample(ErrorBiasedSamplingReason::AutoscalerReconciliationError);
                warn!(error = %e, "failed to query excess runtime capacity");
                false
            });
            if let Err(e) = loop_c.plan(controller, excess_capacity).await {
                mark_error_biased_sample(ErrorBiasedSamplingReason::AutoscalerReconciliationError);
                warn!(error = %e, "retirement planning failed; open retirements hold");
            }
        }
        desired.drain_intents = loop_c.desired_intents();
        autoscaler_metrics::record_loop_duration(
            AutoscalerLoopLabel::Retirement,
            loop_started.elapsed(),
        );

        // Reconcile desired vs current and apply actions via actuator.
        let current = CurrentState {
            drain_intents: loop_c.applied_intents(),
            ..CurrentState::default()
        };
        let actions = tokeira_autoscaler::reconciler::reconcile(&desired, &current);

        if !actions.is_empty() {
            info!(
                action_count = actions.len(),
                "reconciler produced scaling actions"
            );
            for action in &actions {
                if apply_action(&actuator, &config.cluster_name, action)
                    .await
                    .is_ok()
                    && let ScalingAction::AdvanceDrain { node_id, phase } = action
                {
                    loop_c.record_applied(node_id, *phase);
                }
            }
        }
    }
}

async fn apply_action(
    actuator: &Arc<dyn Actuator>,
    cluster: &str,
    action: &ScalingAction,
) -> Result<()> {
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
        ScalingAction::AdvanceDrain { node_id, phase } => {
            info!(?phase, node_id, "advancing drain phase");
            apply_drain_phase(
                actuator.as_ref(),
                cluster,
                RUNTIME_ASG_NAME,
                node_id,
                *phase,
            )
            .await
        }
    };

    if let Err(e) = &result {
        mark_error_biased_sample(ErrorBiasedSamplingReason::AutoscalerReconciliationError);
        warn!(error = %e, ?action, "failed to apply scaling action");
    }
    result
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

async fn install_process_observability(
    config: &AutoscalerServiceConfig,
    readiness: ReadinessRegistry,
) -> Result<ObservabilityRuntime> {
    install_observability(
        process_observability_config(config)?,
        PROCESS_MANIFESTS,
        readiness,
    )
    .await
    .context("failed to install autoscaler observability")
}

fn process_observability_config(
    config: &AutoscalerServiceConfig,
) -> Result<ProcessObservabilityConfig> {
    let metrics_addr: SocketAddr = config
        .metrics_addr
        .parse()
        .context("invalid metrics_addr for observability endpoint")?;
    Ok(ProcessObservabilityConfig {
        service_name: ServiceName::Autoscaler,
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

fn config_source_from_args() -> Result<(tokeira_config::ConfigSource, String)> {
    let mut args = std::env::args().skip(1);
    let flag = match args.next() {
        None => None,
        Some(first) if first == "--config" => {
            Some(args.next().context("--config requires a locator")?)
        }
        Some(first) => Some(first),
    };
    // Flag, then TOKEIRA_CONFIG, then the conventional file name — the same
    // precedence every Tokeira binary uses; the locator forms (a path,
    // `file:<path>`, `env:<VAR>`) are shared too.
    match tokeira_config::ConfigSource::from_cli_env(flag.as_deref())? {
        Some(resolved) => Ok(resolved),
        None => Ok((
            tokeira_config::ConfigSource::File(PathBuf::from("autoscaler.toml")),
            "default autoscaler.toml".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn autoscaler_process_observability_config_validates() {
        let config = AutoscalerServiceConfig {
            metrics_addr: "127.0.0.1:0".to_string(),
            ..Default::default()
        };

        let observability = process_observability_config(&config).unwrap();

        observability.validate().unwrap();
        assert_eq!(observability.service_name.as_str(), "tokeira-autoscaler");
        assert_eq!(observability.cluster_name, config.cluster_name);
    }

    #[test]
    fn autoscaler_process_manifest_validates() {
        tokeira_observability::validate_manifests(&[&PROCESS_METRIC_MANIFEST]).unwrap();
    }

    #[tokio::test]
    async fn autoscaler_mutable_readiness_handles_track_startup_state() {
        let (_, leader_lease) = ReadinessRegistry::mutable(
            "leader_lease",
            ReadinessStatus::NotReady,
            Some("component is still starting".to_string()),
        );
        let (_, control_plane) = ReadinessRegistry::mutable(
            "control_plane",
            ReadinessStatus::NotReady,
            Some("component is still starting".to_string()),
        );

        assert_eq!(
            leader_lease.as_check().check().await.unwrap().status,
            ReadinessStatus::NotReady
        );

        leader_lease.ready();
        control_plane.ready();

        assert_eq!(
            leader_lease.as_check().check().await.unwrap().status,
            ReadinessStatus::Ready
        );
        assert_eq!(
            control_plane.as_check().check().await.unwrap().status,
            ReadinessStatus::Ready
        );
    }
}
