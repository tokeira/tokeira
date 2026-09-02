//! Read-only durable-state diagnostics for operators.
//!
//! Diagnostics query the selected deployment directly and never start runtime
//! services or mutate controller state. Human and JSON renderers share one
//! redacted view so provider configuration and credentials cannot leak.

use anyhow::{Context, Result, bail};
use serde::Serialize;
use tokeira_orchestrator::StorageKind;
use tokeira_storage::{
    WorkerComputeControllerHealthView, WorkerComputeHealthFilter,
    dsql::{ConnectionFactory, DsqlWorkerComputeRepository},
};
use tokeira_types::{NamespaceId, WorkerComputeFailureCategory, WorkerComputeHealth};
use uuid::Uuid;

use crate::{
    cli::DiagnosticsAction,
    deployment_dir::{DeploymentRecordContext, TOKEIRAD_TOML},
};

pub(crate) async fn run(
    action: DiagnosticsAction,
    ctx: DeploymentRecordContext,
    json: bool,
) -> Result<()> {
    match action {
        DiagnosticsAction::WorkerCompute { namespace } => {
            worker_compute(ctx, &namespace, json).await
        }
    }
}

async fn worker_compute(ctx: DeploymentRecordContext, namespace: &str, json: bool) -> Result<()> {
    if ctx.metadata.storage != StorageKind::Dsql {
        bail!("worker-compute diagnostics require dsql storage");
    }
    let config_path = ctx.path.join(TOKEIRAD_TOML);
    let config = crate::commands::infra::read_tokeirad_config(&config_path)?;
    let dsql = &config.infrastructure.dsql;
    let endpoint = dsql
        .endpoint
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "dsql endpoint is not configured in {}; run `tkr infra apply --module dsql` first",
                config_path.display()
            )
        })?;
    let region = dsql
        .region
        .clone()
        .or_else(|| tokeira_storage::dsql::detect_region_from_endpoint(endpoint))
        .ok_or_else(|| {
            anyhow::anyhow!("dsql region must be configured or derivable from endpoint")
        })?;
    let mut connection = ConnectionFactory::new(endpoint, &region)?
        .create_connection()
        .await
        .context("connecting to DSQL for worker-compute diagnostics")?;
    let rows = DsqlWorkerComputeRepository::list_health_with_connection(
        &mut connection,
        namespace_id_for(namespace),
        WorkerComputeHealthFilter::default(),
    )
    .await
    .with_context(|| format!("reading worker-compute health for namespace '{namespace}'"))?;
    render_worker_compute(rows, json)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct WorkerComputeDiagnosticRow {
    namespace: String,
    deployment: String,
    build_id: String,
    scaling_group: String,
    fingerprint: String,
    health: &'static str,
    last_action_id: Option<Uuid>,
    last_failure_category: Option<&'static str>,
    next_metrics_poll_at: Option<String>,
}

impl From<WorkerComputeControllerHealthView> for WorkerComputeDiagnosticRow {
    fn from(value: WorkerComputeControllerHealthView) -> Self {
        Self {
            namespace: value.namespace_name,
            deployment: value.controller_key.deployment_name.0,
            build_id: value.controller_key.build_id.0,
            scaling_group: value.scaling_group.0,
            fingerprint: fingerprint_hex(value.fingerprint.as_bytes()),
            health: health_label(value.health),
            last_action_id: value.last_action_id,
            last_failure_category: value.last_failure_category.map(failure_label),
            next_metrics_poll_at: value
                .next_metrics_poll_at
                .map(|timestamp| timestamp.to_string()),
        }
    }
}

fn render_worker_compute(rows: Vec<WorkerComputeControllerHealthView>, json: bool) -> Result<()> {
    let rows = rows
        .into_iter()
        .map(WorkerComputeDiagnosticRow::from)
        .collect::<Vec<_>>();
    if json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("No worker-compute controller state found.");
        return Ok(());
    }
    println!(
        "NAMESPACE\tDEPLOYMENT\tBUILD ID\tSCALING GROUP\tHEALTH\tLAST ACTION\tFAILURE\tNEXT POLL"
    );
    for row in rows {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            row.namespace,
            row.deployment,
            row.build_id,
            row.scaling_group,
            row.health,
            row.last_action_id
                .map_or_else(|| "-".to_owned(), |id| id.to_string()),
            row.last_failure_category.unwrap_or("-"),
            row.next_metrics_poll_at.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

fn namespace_id_for(name: &str) -> NamespaceId {
    // Keep this byte-for-byte aligned with edge namespace admission. The namespace
    // registry currently derives IDs rather than persisting a separate name index,
    // so diagnostics must use the same stable mapping.
    let mut bytes = *b"tokeira-edge-ns!";
    for (index, byte) in name.as_bytes().iter().enumerate() {
        let slot = index % 16;
        bytes[slot] = bytes[slot]
            .wrapping_add(*byte)
            .rotate_left((index % 8) as u32);
    }
    NamespaceId(Uuid::from_bytes(bytes))
}

fn fingerprint_hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

const fn health_label(health: WorkerComputeHealth) -> &'static str {
    match health {
        WorkerComputeHealth::Active => "active",
        WorkerComputeHealth::Disabled => "disabled",
        WorkerComputeHealth::UnsupportedProvider => "unsupported-provider",
        WorkerComputeHealth::UnsupportedScaler => "unsupported-scaler",
        WorkerComputeHealth::InvalidConfiguration => "invalid-configuration",
        WorkerComputeHealth::ProviderRequestTooLarge => "provider-request-too-large",
        WorkerComputeHealth::MisconfiguredEndpoint => "misconfigured-endpoint",
        WorkerComputeHealth::CapacityLimited => "capacity-limited",
        WorkerComputeHealth::DeliveryRetrying => "delivery-retrying",
        WorkerComputeHealth::DeliveryTerminalFailure => "delivery-terminal-failure",
        WorkerComputeHealth::Inactive => "inactive",
    }
}

const fn failure_label(category: WorkerComputeFailureCategory) -> &'static str {
    match category {
        WorkerComputeFailureCategory::NamespaceUnresolved => "namespace-unresolved",
        WorkerComputeFailureCategory::EndpointNotFound => "endpoint-not-found",
        WorkerComputeFailureCategory::Transport => "transport",
        WorkerComputeFailureCategory::RetryableHandler => "retryable-handler",
        WorkerComputeFailureCategory::NonRetryableHandler => "non-retryable-handler",
        WorkerComputeFailureCategory::OperationUnsuccessful => "operation-unsuccessful",
        WorkerComputeFailureCategory::AsyncResponse => "async-response",
        WorkerComputeFailureCategory::RequestTooLarge => "request-too-large",
        WorkerComputeFailureCategory::InvalidResponsePayload => "invalid-response-payload",
        WorkerComputeFailureCategory::ResponseIdMismatch => "response-id-mismatch",
        WorkerComputeFailureCategory::Storage => "storage",
    }
}

#[cfg(test)]
mod tests {
    use tokeira_types::{
        BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, ScalingGroupId,
    };

    use super::*;

    #[test]
    fn diagnostic_view_is_stable_and_redacted() {
        let action_id = Uuid::from_u128(7);
        let row = WorkerComputeControllerHealthView {
            namespace_name: "payments".to_owned(),
            controller_key: ControllerInstanceKey {
                namespace_id: namespace_id_for("payments"),
                deployment_name: DeploymentId("worker".to_owned()),
                build_id: BuildId("2026-07-27".to_owned()),
            },
            scaling_group: ScalingGroupId("workflow".to_owned()),
            fingerprint: ConfigurationFingerprint::from_canonical_bytes(b"config"),
            health: WorkerComputeHealth::DeliveryRetrying,
            last_action_id: Some(action_id),
            last_failure_category: Some(WorkerComputeFailureCategory::Transport),
            next_metrics_poll_at: None,
        };
        let rendered =
            serde_json::to_string(&WorkerComputeDiagnosticRow::from(row)).expect("diagnostic JSON");
        assert_eq!(
            rendered,
            format!(
                "{{\"namespace\":\"payments\",\"deployment\":\"worker\",\"build_id\":\"2026-07-27\",\
                 \"scaling_group\":\"workflow\",\"fingerprint\":\"{}\",\"health\":\"delivery-retrying\",\
                 \"last_action_id\":\"{action_id}\",\"last_failure_category\":\"transport\",\
                 \"next_metrics_poll_at\":null}}",
                fingerprint_hex(
                    ConfigurationFingerprint::from_canonical_bytes(b"config").as_bytes()
                ),
            )
        );
        assert!(!rendered.contains("provider_details"));
        assert!(!rendered.contains("credential"));
    }

    #[test]
    fn namespace_mapping_matches_the_edge_fixed_vector() {
        assert_eq!(
            namespace_id_for("default").0,
            Uuid::from_bytes([
                216, 169, 71, 54, 237, 219, 117, 45, 101, 100, 103, 101, 45, 110, 115, 33,
            ])
        );
    }
}
