//! Mimir-backed signal queries for the three autoscaler control loops.
//!
//! # Why separate from the loop modules?
//!
//! The loop modules (`loop_a`, `loop_b`, `loop_c`) contain pure decision logic
//! that operates on typed inputs. This module owns the I/O boundary: it queries
//! Mimir for raw PromQL metrics and classifies them into the typed signals that
//! the loops consume. Keeping the I/O separate means the loop logic remains
//! unit-testable without mocking HTTP calls.

use anyhow::{Context, Result};
use tracing::warn;

use crate::{
    config::AutoscalerServiceConfig,
    loop_a::{ServicePressure, ServiceSignal},
    loop_b::{RuntimePressure, RuntimeScaleOutInput},
    loop_c::RetirementCandidate,
    mimir::MimirClient,
};

// ── Thresholds ──────────────────────────────────────────────────────────────

/// Schedule-to-start p95 above this triggers scale-out (seconds).
const SCHEDULE_TO_START_SCALE_OUT_THRESHOLD: f64 = 0.5;
/// Schedule-to-start p95 below this allows scale-in (seconds).
const SCHEDULE_TO_START_SCALE_IN_THRESHOLD: f64 = 0.1;
/// Queue depth above this triggers scale-out.
const QUEUE_DEPTH_SCALE_OUT_THRESHOLD: f64 = 100.0;
/// Queue depth below this allows scale-in.
const QUEUE_DEPTH_SCALE_IN_THRESHOLD: f64 = 10.0;
/// Poll success rate above this allows scale-in (fraction, not percentage).
const POLL_SUCCESS_SCALE_IN_THRESHOLD: f64 = 0.95;

/// CPU utilization above this contributes to BroadSaturation (fraction).
const CPU_SATURATION_THRESHOLD: f64 = 0.70;
/// Lane queue wait above this contributes to BroadSaturation (seconds).
const LANE_QUEUE_SATURATION_THRESHOLD: f64 = 0.05;

// ── Loop A: per-service pressure signals ────────────────────────────────────

/// Query Mimir for per-service pressure signals and classify each service.
///
/// Returns one `ServiceSignal` per configured service. Services whose metrics
/// are missing are classified as `Hold` — the autoscaler does not scale based
/// on absent data.
pub async fn query_service_signals(
    mimir: &MimirClient,
    config: &AutoscalerServiceConfig,
) -> Result<Vec<ServiceSignal>> {
    let mut signals = Vec::new();

    for (service_name, _service_config) in &config.service_configs {
        let pressure = classify_service_pressure(mimir, service_name).await?;
        signals.push(ServiceSignal {
            service: service_name.clone(),
            // Current count is read from the platform via the actuator; use 0
            // as a placeholder that apply_signals will override from desired state.
            current_count: 0,
            pressure,
        });
    }

    Ok(signals)
}

async fn classify_service_pressure(
    mimir: &MimirClient,
    service_name: &str,
) -> Result<ServicePressure> {
    let schedule_to_start = mimir
        .query_instant_value(&format!(
            "histogram_quantile(0.95, rate(tokeira_runtime_lane_queue_wait_seconds{{service=\"{service_name}\"}}[5m]))"
        ))
        .await
        .context("schedule-to-start query failed")?;

    let queue_depth = mimir
        .query_instant_value(&format!(
            "tokeira_runtime_broker_queue_depth{{service=\"{service_name}\"}}"
        ))
        .await
        .context("broker queue depth query failed")?;

    let poll_success = mimir
        .query_instant_value(&format!(
            "rate(tokeira_edge_grpc_request_total{{method=\"poll_workflow_task_queue\",status=\"ok\",service=\"{service_name}\"}}[5m])"
        ))
        .await
        .context("poll success rate query failed")?;

    let sts = schedule_to_start.unwrap_or(0.0);
    let depth = queue_depth.unwrap_or(0.0);
    let poll = poll_success.unwrap_or(1.0);

    // Scale-out: immediate pressure indicators.
    if sts > SCHEDULE_TO_START_SCALE_OUT_THRESHOLD || depth > QUEUE_DEPTH_SCALE_OUT_THRESHOLD {
        return Ok(ServicePressure::ScaleOut);
    }

    // Scale-in: all indicators must confirm low utilization.
    if sts < SCHEDULE_TO_START_SCALE_IN_THRESHOLD
        && depth < QUEUE_DEPTH_SCALE_IN_THRESHOLD
        && poll > POLL_SUCCESS_SCALE_IN_THRESHOLD
    {
        return Ok(ServicePressure::ScaleIn);
    }

    Ok(ServicePressure::Hold)
}

// ── Loop B: runtime-level pressure ──────────────────────────────────────────

/// Query Mimir for runtime-level saturation signals.
///
/// Returns a `RuntimeScaleOutInput` that the existing `apply_runtime_scale_out`
/// function can consume directly.
pub async fn query_runtime_pressure(
    mimir: &MimirClient,
    current_hosts: u32,
    step: u32,
    per_runtime_reserved_connections: u32,
) -> Result<RuntimeScaleOutInput> {
    let cpu_utilization = mimir
        .query_instant_value(
            "avg(rate(container_cpu_usage_seconds_total{service=\"tokeira-runtime\"}[5m]))",
        )
        .await
        .context("CPU utilization query failed")?;

    let _commit_latency = mimir
        .query_instant_value(
            "histogram_quantile(0.95, rate(tokeira_dsql_operation_duration_seconds{operation=\"commit_transition_for_bundle\"}[5m]))",
        )
        .await
        .context("commit latency query failed")?;

    let lane_queue_wait = mimir
        .query_instant_value("avg(tokeira_runtime_lane_queue_wait_seconds)")
        .await
        .context("lane queue saturation query failed")?;

    let dsql_headroom = mimir
        .query_instant_value("tokeira_dsql_pool_connections_total - tokeira_dsql_pool_class_in_use")
        .await
        .context("DSQL connection headroom query failed")?;

    let cpu = cpu_utilization.unwrap_or(0.0);
    let lane_wait = lane_queue_wait.unwrap_or(0.0);
    let headroom = dsql_headroom.unwrap_or(0.0);

    // BroadSaturation requires both CPU and lane queue pressure.
    let pressure = if cpu > CPU_SATURATION_THRESHOLD && lane_wait > LANE_QUEUE_SATURATION_THRESHOLD
    {
        RuntimePressure::BroadSaturation
    } else {
        RuntimePressure::None
    };

    let dsql_headroom_available = headroom > per_runtime_reserved_connections as f64;

    Ok(RuntimeScaleOutInput {
        current_hosts,
        step,
        pressure,
        dsql_headroom_available,
    })
}

// ── Loop C: retirement candidates ───────────────────────────────────────────

/// Identify runtime hosts that are candidates for retirement.
///
/// The controller gRPC (NominateScaleInCandidates, MarkNodeDraining) is not
/// yet implemented — blocked on the shard-placement-membership spec. This
/// placeholder identifies the least-loaded host from Mimir metrics and
/// returns it as a retirement candidate when excess capacity exists.
pub async fn query_retirement_candidates(
    mimir: &MimirClient,
    current_hosts: u32,
    target_load_per_host: f64,
) -> Result<Vec<RetirementCandidate>> {
    let total_cpu = mimir
        .query_instant_value(
            "sum(rate(container_cpu_usage_seconds_total{service=\"tokeira-runtime\"}[5m]))",
        )
        .await
        .context("total CPU query for retirement failed")?;

    let Some(total) = total_cpu else {
        return Ok(Vec::new());
    };

    // Derive needed hosts from current aggregate load.
    let needed_hosts = (total / target_load_per_host).ceil() as u32;
    if current_hosts <= needed_hosts {
        return Ok(Vec::new());
    }

    // Identify the least-loaded host. In production this would come from the
    // controller's shard-placement view; for now we query per-instance CPU.
    let least_loaded_instance = mimir
        .query_instant_value(
            "bottomk(1, sum by (instance_id) (rate(container_cpu_usage_seconds_total{service=\"tokeira-runtime\"}[5m])))",
        )
        .await
        .context("least-loaded instance query failed")?;

    if least_loaded_instance.is_none() {
        warn!("retirement: could not identify least-loaded instance from metrics");
        return Ok(Vec::new());
    }

    // TODO(shard-placement-membership): Replace this placeholder with actual
    // controller interaction:
    // 1. NominateScaleInCandidates — ask controller which host to retire
    // 2. MarkNodeDraining — tell controller to stop assigning bundles
    // For now, use a synthetic instance ID derived from the query. The real
    // implementation will resolve instance IDs from the controller's membership
    // view.
    let candidate = RetirementCandidate {
        instance_id: format!("runtime-host-excess-{}", current_hosts - needed_hosts),
    };

    Ok(vec![candidate])
}
