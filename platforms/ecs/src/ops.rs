//! Definition-derived coordinates for live ECS operations.
//!
//! The framework gives every platform the same identity-only
//! [`DeploymentRef`]. ECS owns
//! the extra facts its AWS operations require, so it re-evaluates the exact
//! definition revision recorded in the admitted deployment directory using
//! the ECS namespaces and frontend selected by metadata. This intentionally
//! repeats platform evaluation at the operational boundary: frontend choice,
//! validation, and the meaning of ECS configuration remain platform policy
//! rather than additions to the generic operations interface.
//!
//! Only non-secret routing coordinates are retained. Provider credentials
//! stay ambient, provider state is queried live by each operation, and this
//! projection never contains resource identifiers discovered during apply.
//! The metadata state location remains authoritative for infrastructure,
//! runtime, and lock state; it does not relocate the authored definition,
//! which is always read from the admitted deployment directory.

use std::{
    collections::{HashSet, VecDeque},
    fs,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, bail};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use tokeira_aws::AwsClients;
use tokeira_deployment::DeploymentBindingMetadata;
use tokeira_platform::{
    author::{LocatedValue, from_located_value},
    declaration::{DeploymentRef, LogStream, Ops, PortMapping},
    definition::{
        DefinitionSource, DefinitionSourceName, DirectoryPartSources, evaluate_definition,
    },
};

const METADATA_JSON: &str = "metadata.json";
const DEFAULT_LOG_TAIL: u32 = 100;
const LOG_FOLLOW_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize)]
struct EvaluationContext {
    project_name: String,
}

// This is deliberately a projection, not a second ECS configuration model.
// Serde ignores fields owned by deployment realization, while definition
// evaluation above still validates the complete graph through every kind.
#[derive(Debug, Deserialize)]
struct OpsConfiguration {
    environment: String,
    aws: OpsAwsConfiguration,
    cluster: OpsClusterConfiguration,
    networking: OpsNetworkingConfiguration,
    observability: OpsObservabilityConfiguration,
}

#[derive(Debug, Deserialize)]
struct OpsAwsConfiguration {
    region: String,
}

#[derive(Debug, Deserialize)]
struct OpsClusterConfiguration {
    name: String,
    service_connect_namespace: String,
}

#[derive(Debug, Deserialize)]
struct OpsNetworkingConfiguration {
    private_dns_zone: String,
}

#[derive(Debug, Deserialize)]
struct OpsObservabilityConfiguration {
    loki_query_url: String,
}

/// Authored, non-secret coordinates required by ECS day-2 operations.
///
/// These values are recovered afresh from the admitted definition instead
/// of copied into lifecycle state. That keeps remote-state deployments
/// portable and prevents desired routing facts from drifting between the
/// definition and a platform-private operations ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcsOperationCoordinates {
    environment: String,
    region: String,
    cluster: String,
    service_connect_namespace: String,
    private_dns_zone: String,
    loki_query_url: String,
}

impl EcsOperationCoordinates {
    /// Re-evaluate a deployment's recorded definition and recover its ECS
    /// operation coordinates.
    ///
    /// The deployment directory must belong to the supplied name, record the
    /// `ecs` platform, and contain a supported `tkd` or `tkdp` definition.
    /// Full definition validation runs before this narrow projection is
    /// admitted, so malformed resources are refused before any AWS client is
    /// selected.
    pub fn read(deployment: &DeploymentRef) -> Result<Self> {
        let configuration = evaluated_configuration(deployment)?;
        let config: OpsConfiguration = from_located_value(configuration)
            .context("admitted ECS definition has no usable operations configuration")?;

        Ok(Self {
            environment: required(config.environment, "environment")?,
            region: required(config.aws.region, "aws.region")?,
            cluster: required(config.cluster.name, "cluster.name")?,
            service_connect_namespace: required(
                config.cluster.service_connect_namespace,
                "cluster.service_connect_namespace",
            )?,
            private_dns_zone: required(
                config.networking.private_dns_zone,
                "networking.private_dns_zone",
            )?,
            loki_query_url: required(
                config.observability.loki_query_url,
                "observability.loki_query_url",
            )?,
        })
    }

    /// Authored environment label used by workload and telemetry selection.
    pub fn environment(&self) -> &str {
        &self.environment
    }

    /// AWS region in which live ECS resources must be queried.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Authored ECS cluster name.
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Authored Service Connect namespace used for service discovery.
    pub fn service_connect_namespace(&self) -> &str {
        &self.service_connect_namespace
    }

    /// Authored private DNS zone, independent of Service Connect naming.
    pub fn private_dns_zone(&self) -> &str {
        &self.private_dns_zone
    }

    /// Operator-reachable Loki endpoint used for service log queries.
    pub fn loki_query_url(&self) -> &str {
        &self.loki_query_url
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Service {
    operator_name: &'static str,
    ecs_name: &'static str,
    port: u16,
    scalable: bool,
}

const SERVICES: [Service; 10] = [
    Service {
        operator_name: "edge-api",
        ecs_name: "tokeira-edge-api",
        port: 7233,
        scalable: true,
    },
    Service {
        operator_name: "edge-poll",
        ecs_name: "tokeira-edge-poll",
        port: 7234,
        scalable: true,
    },
    Service {
        operator_name: "runtime",
        ecs_name: "tokeira-runtime",
        port: 7241,
        scalable: false,
    },
    Service {
        operator_name: "projection",
        ecs_name: "tokeira-projection",
        port: 7242,
        scalable: true,
    },
    Service {
        operator_name: "controller",
        ecs_name: "tokeira-controller",
        port: 7240,
        scalable: true,
    },
    Service {
        operator_name: "autoscaler",
        ecs_name: "tokeira-autoscaler",
        port: 7243,
        scalable: true,
    },
    Service {
        operator_name: "admin",
        ecs_name: "tokeira-admin",
        port: 7244,
        scalable: true,
    },
    Service {
        operator_name: "mimir",
        ecs_name: "tokeira-mimir",
        port: 9009,
        scalable: true,
    },
    Service {
        operator_name: "loki",
        ecs_name: "tokeira-loki",
        port: 3100,
        scalable: true,
    },
    Service {
        operator_name: "grafana",
        ecs_name: "tokeira-grafana",
        port: 3000,
        scalable: true,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScaleTarget {
    service: Service,
    replicas: u32,
}

#[derive(Debug, Clone)]
struct LogEntry {
    timestamp: u64,
    line: String,
}

#[derive(Debug)]
struct FollowState {
    client: reqwest::Client,
    base_url: String,
    service: &'static str,
    cursor: u64,
    pending: VecDeque<LogEntry>,
}

/// Definition-bound live ECS operations.
///
/// Each call re-evaluates the admitted definition and creates an AWS client
/// in its authored region. There is deliberately no process-global client
/// cache: two deployments or concurrent calls cannot race to install one
/// region and accidentally query another.
#[derive(Debug)]
pub(crate) struct EcsOps;

#[async_trait::async_trait]
impl Ops for EcsOps {
    async fn log_stream(
        &self,
        deployment: &DeploymentRef,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> Result<LogStream> {
        let service = lookup_service(service)?;
        let coordinates = EcsOperationCoordinates::read(deployment)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("build Loki query client")?;
        let limit = tail.unwrap_or(DEFAULT_LOG_TAIL);
        let entries = if limit == 0 {
            Vec::new()
        } else {
            query_loki(
                &client,
                coordinates.loki_query_url(),
                service.ecs_name,
                None,
                limit,
            )
            .await?
        };

        if !follow {
            return Ok(Box::pin(stream::iter(
                entries.into_iter().map(|entry| Ok(entry.line)),
            )));
        }

        let cursor = entries
            .last()
            .map_or_else(now_unix_nanos, |entry| Ok(entry.timestamp))?;
        let state = FollowState {
            client,
            base_url: coordinates.loki_query_url().to_owned(),
            service: service.ecs_name,
            cursor,
            pending: entries.into(),
        };
        Ok(Box::pin(stream::unfold(state, next_log_line)))
    }

    async fn port_mappings(
        &self,
        deployment: &DeploymentRef,
        service: &str,
    ) -> Result<Vec<PortMapping>> {
        let service = lookup_service(service)?;
        let coordinates = EcsOperationCoordinates::read(deployment)?;
        let clients = AwsClients::load(Some(coordinates.region())).await;
        let task_arns = running_tasks(&clients, &coordinates, service).await?;
        let output = clients
            .ecs
            .describe_tasks()
            .cluster(coordinates.cluster())
            .set_tasks(Some(task_arns))
            .send()
            .await
            .map_err(|error| provider_error(&coordinates, "ecs:DescribeTasks", error))?;
        let mut seen = HashSet::new();
        let mappings = output
            .tasks()
            .iter()
            .filter_map(task_private_ip)
            .filter(|address| seen.insert(address.clone()))
            .map(|host_addr| PortMapping {
                host_addr,
                host_port: service.port,
                container_port: service.port,
                protocol: "tcp".to_owned(),
            })
            .collect::<Vec<_>>();
        if mappings.is_empty() {
            bail!(
                "ECS service `{}` has running tasks but none reports an awsvpc private IPv4 address",
                service.ecs_name
            );
        }
        Ok(mappings)
    }

    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> Result<usize> {
        let targets = parse_scale_specs(specs)?;
        let coordinates = EcsOperationCoordinates::read(deployment)?;
        let clients = AwsClients::load(Some(coordinates.region())).await;
        let mut changed = 0usize;
        for target in targets {
            let current = desired_count(&clients, &coordinates, target.service).await?;
            if current == target.replicas {
                continue;
            }
            let desired_count = i32::try_from(target.replicas).map_err(|_| {
                anyhow::anyhow!(
                    "replica count {} for `{}` exceeds the ECS API limit",
                    target.replicas,
                    target.service.operator_name
                )
            })?;
            clients
                .ecs
                .update_service()
                .cluster(coordinates.cluster())
                .service(target.service.ecs_name)
                .desired_count(desired_count)
                .send()
                .await
                .map_err(|error| provider_error(&coordinates, "ecs:UpdateService", error))?;
            changed += 1;
        }
        Ok(changed)
    }
}

async fn next_log_line(mut state: FollowState) -> Option<(Result<String>, FollowState)> {
    loop {
        if let Some(entry) = state.pending.pop_front() {
            state.cursor = state.cursor.max(entry.timestamp);
            return Some((Ok(entry.line), state));
        }
        tokio::time::sleep(LOG_FOLLOW_INTERVAL).await;
        match query_loki(
            &state.client,
            &state.base_url,
            state.service,
            state.cursor.checked_add(1),
            DEFAULT_LOG_TAIL,
        )
        .await
        {
            Ok(entries) => state.pending = entries.into(),
            Err(error) => return Some((Err(error), state)),
        }
    }
}

async fn query_loki(
    client: &reqwest::Client,
    base_url: &str,
    service: &str,
    start: Option<u64>,
    limit: u32,
) -> Result<Vec<LogEntry>> {
    let query = format!(r#"{{service_name="{service}"}}"#);
    let mut parameters = vec![
        ("query", query),
        ("limit", limit.to_string()),
        ("direction", "backward".to_owned()),
    ];
    if let Some(start) = start {
        parameters.push(("start", start.to_string()));
    }
    let response = client
        .get(format!(
            "{}/loki/api/v1/query_range",
            base_url.trim_end_matches('/')
        ))
        .query(&parameters)
        .send()
        .await
        .with_context(|| format!("failed to query Loki at `{base_url}`"))?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("Loki at `{base_url}` returned {status}: {body}");
    }
    let body: serde_json::Value = response
        .json()
        .await
        .with_context(|| format!("Loki at `{base_url}` returned invalid JSON"))?;
    let mut entries = Vec::new();
    if let Some(results) = body
        .pointer("/data/result")
        .and_then(serde_json::Value::as_array)
    {
        for result in results {
            let Some(values) = result.get("values").and_then(serde_json::Value::as_array) else {
                continue;
            };
            for value in values {
                let Some(parts) = value.as_array() else {
                    continue;
                };
                let Some(timestamp) = parts.first().and_then(serde_json::Value::as_str) else {
                    continue;
                };
                let Some(line) = parts.get(1).and_then(serde_json::Value::as_str) else {
                    continue;
                };
                entries.push(LogEntry {
                    timestamp: timestamp.parse().with_context(|| {
                        format!("Loki returned invalid nanosecond timestamp `{timestamp}`")
                    })?,
                    line: line.to_owned(),
                });
            }
        }
    }
    entries.sort_by_key(|entry| entry.timestamp);
    Ok(entries)
}

fn now_unix_nanos() -> Result<u64> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos();
    u64::try_from(nanos).context("current Unix timestamp exceeds Loki's nanosecond range")
}

fn lookup_service(name: &str) -> Result<Service> {
    SERVICES
        .iter()
        .copied()
        .find(|service| service.operator_name == name)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unknown ECS service `{name}`; valid services are: {}",
                SERVICES
                    .iter()
                    .map(|service| service.operator_name)
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })
}

fn parse_scale_specs(specs: &[String]) -> Result<Vec<ScaleTarget>> {
    let mut seen = HashSet::with_capacity(specs.len());
    specs
        .iter()
        .map(|spec| {
            let (name, replicas) = spec.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("scale spec `{spec}` is not `<service>=<replicas>`")
            })?;
            let service = lookup_service(name.trim())?;
            if !service.scalable {
                bail!(
                    "ECS service `{}` uses daemon scheduling and has no replica dimension",
                    service.operator_name
                );
            }
            if !seen.insert(service.operator_name) {
                bail!("duplicate scale target `{}`", service.operator_name);
            }
            let replicas = replicas.trim().parse::<u32>().with_context(|| {
                format!("scale spec `{spec}` has an invalid unsigned replica count")
            })?;
            Ok(ScaleTarget { service, replicas })
        })
        .collect()
}

async fn running_tasks(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: Service,
) -> Result<Vec<String>> {
    let output = clients
        .ecs
        .list_tasks()
        .cluster(coordinates.cluster())
        .service_name(service.ecs_name)
        .desired_status(aws_sdk_ecs::types::DesiredStatus::Running)
        .send()
        .await
        .map_err(|error| provider_error(coordinates, "ecs:ListTasks", error))?;
    if output.task_arns().is_empty() {
        bail!("ECS service `{}` has no running tasks", service.ecs_name);
    }
    Ok(output.task_arns().to_vec())
}

async fn desired_count(
    clients: &AwsClients,
    coordinates: &EcsOperationCoordinates,
    service: Service,
) -> Result<u32> {
    let output = clients
        .ecs
        .describe_services()
        .cluster(coordinates.cluster())
        .services(service.ecs_name)
        .send()
        .await
        .map_err(|error| provider_error(coordinates, "ecs:DescribeServices", error))?;
    let described = output.services().first().ok_or_else(|| {
        anyhow::anyhow!(
            "ECS service `{}` was not found in cluster `{}`",
            service.ecs_name,
            coordinates.cluster()
        )
    })?;
    u32::try_from(described.desired_count()).map_err(|_| {
        anyhow::anyhow!(
            "ECS service `{}` returned negative desired count {}",
            service.ecs_name,
            described.desired_count()
        )
    })
}

fn task_private_ip(task: &aws_sdk_ecs::types::Task) -> Option<String> {
    task.attachments()
        .iter()
        .flat_map(|attachment| attachment.details())
        .find(|detail| detail.name() == Some("privateIPv4Address"))
        .and_then(|detail| detail.value())
        .filter(|address| !address.is_empty())
        .map(ToOwned::to_owned)
}

fn provider_error(
    coordinates: &EcsOperationCoordinates,
    operation: &str,
    error: impl std::fmt::Display,
) -> anyhow::Error {
    anyhow::anyhow!(
        "{operation} failed for ECS cluster `{}` in `{}`: {error}",
        coordinates.cluster(),
        coordinates.region()
    )
}

/// Evaluate the admitted ECS definition once and return its authored
/// configuration value.
///
/// Operational capabilities share this entry point so frontend selection,
/// companion resolution, metadata admission, and full kind validation cannot
/// drift between logs/exec coordinates and image publication.
pub(crate) fn evaluated_configuration(deployment: &DeploymentRef) -> Result<LocatedValue> {
    let metadata_path = deployment.dir.join(METADATA_JSON);
    let metadata: DeploymentBindingMetadata = serde_json::from_slice(
        &fs::read(&metadata_path)
            .with_context(|| format!("failed to read {}", metadata_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", metadata_path.display()))?;
    if metadata.name != deployment.name {
        bail!(
            "deployment directory records name `{}` but the admitted deployment is `{}`",
            metadata.name,
            deployment.name
        );
    }
    if metadata.platform.as_str() != "ecs" {
        bail!(
            "deployment `{}` records platform `{}`, not `ecs`",
            deployment.name,
            metadata.platform
        );
    }
    let definition = metadata.definition.ok_or_else(|| {
        anyhow::anyhow!(
            "deployment `{}` records no definition revision",
            deployment.name
        )
    })?;
    let definition_path = deployment.dir.join(definition.path.as_path());
    let source =
        DefinitionSource {
            format: definition.format.clone(),
            source_name: DefinitionSourceName::DeploymentRelative(definition.path),
            bytes: Arc::from(fs::read(&definition_path).with_context(|| {
                format!("failed to read definition {}", definition_path.display())
            })?),
        };
    let parts_dir = definition_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(deployment.dir.as_path());
    let parts = DirectoryPartSources::new(parts_dir, definition.format.as_str());
    let context = EvaluationContext {
        project_name: deployment.name.clone(),
    };
    let namespaces = crate::namespaces();
    let evaluated = match definition.format.as_str() {
        "tkd" => evaluate_definition(
            &tokeira_platform_definition::tkd::frontend(),
            source,
            &context,
            &namespaces,
            &parts,
        ),
        "tkdp" => evaluate_definition(
            &tokeira_platform_definition::tkdp::frontend(),
            source,
            &context,
            &namespaces,
            &parts,
        ),
        format => bail!(
            "deployment `{}` records unsupported ECS definition format `{format}`",
            deployment.name
        ),
    }
    .with_context(|| {
        format!(
            "failed to evaluate admitted ECS definition {}",
            definition_path.display()
        )
    })?;
    Ok(evaluated.config)
}

fn required(value: String, path: &str) -> Result<String> {
    if value.trim().is_empty() {
        bail!("admitted ECS definition has an empty `{path}`");
    }
    Ok(value)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::path::Path;

    use super::*;

    fn replace_required(source: &mut String, authored: &str, replacement: &str) {
        assert!(source.contains(authored), "fixture contains `{authored}`");
        *source = source.replacen(authored, replacement, 1);
    }

    pub(crate) fn stage_definition(temp: &Path, format: &str) -> DeploymentRef {
        let source_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root_name = if format == "tkd" {
            "deployment.tkd"
        } else {
            "definition.tkdp"
        };
        for entry in fs::read_dir(source_dir).expect("ECS source directory") {
            let entry = entry.expect("source entry");
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some(format) {
                continue;
            }
            let mut source = fs::read_to_string(&path).expect("definition source");
            if path.file_name().and_then(|name| name.to_str()) == Some(root_name) {
                if format == "tkd" {
                    replace_required(
                        &mut source,
                        "environment: \"dev\".into(),",
                        "environment: \"review\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "region: \"eu-west-2\".into(),",
                        "region: \"eu-north-1\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "name: \"tokeira\".into(),",
                        "name: \"ops-cluster\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "service_connect_namespace: \"tokeira.internal\".into(),",
                        "service_connect_namespace: \"mesh.example\".into(),",
                    );
                    replace_required(
                        &mut source,
                        "private_dns_zone: \"tokeira.internal\".into(),",
                        "private_dns_zone: \"private.example\".into(),",
                    );
                } else {
                    replace_required(
                        &mut source,
                        "environment=\"dev\",",
                        "environment=\"review\",",
                    );
                    replace_required(&mut source, "region=\"eu-west-2\"", "region=\"eu-north-1\"");
                    replace_required(&mut source, "name=\"tokeira\",", "name=\"ops-cluster\",");
                    replace_required(
                        &mut source,
                        "service_connect_namespace=\"tokeira.internal\",",
                        "service_connect_namespace=\"mesh.example\",",
                    );
                    replace_required(
                        &mut source,
                        "private_dns_zone=\"tokeira.internal\",",
                        "private_dns_zone=\"private.example\",",
                    );
                }
            }
            fs::write(temp.join(entry.file_name()), source).expect("stage definition source");
        }
        fs::write(
            temp.join(METADATA_JSON),
            serde_json::to_vec(&serde_json::json!({
                "name": "ops-fixture",
                "id": "7698ae09-197e-4325-9f77-256dac98f23a",
                "platform": "ecs",
                "definition": { "format": format, "path": root_name }
            }))
            .expect("metadata serializes"),
        )
        .expect("stage metadata");
        DeploymentRef {
            name: "ops-fixture".to_string(),
            dir: temp.to_path_buf(),
        }
    }

    #[test]
    fn coordinates_follow_each_admitted_definition_format() {
        for format in ["tkd", "tkdp"] {
            let temp = tempfile::tempdir().expect("deployment directory");
            let deployment = stage_definition(temp.path(), format);
            let coordinates =
                EcsOperationCoordinates::read(&deployment).expect("definition-derived coordinates");

            assert_eq!(coordinates.environment(), "review");
            assert_eq!(coordinates.region(), "eu-north-1");
            assert_eq!(coordinates.cluster(), "ops-cluster");
            assert_eq!(coordinates.service_connect_namespace(), "mesh.example");
            assert_eq!(coordinates.private_dns_zone(), "private.example");
            assert_eq!(coordinates.loki_query_url(), "http://localhost:3100");
        }
    }

    #[test]
    fn scale_specs_are_absolute_unique_replica_targets() {
        let targets = parse_scale_specs(&["edge-api=3".to_owned(), "grafana=0".to_owned()])
            .expect("valid scale targets");
        assert_eq!(
            targets,
            vec![
                ScaleTarget {
                    service: lookup_service("edge-api").expect("known service"),
                    replicas: 3,
                },
                ScaleTarget {
                    service: lookup_service("grafana").expect("known service"),
                    replicas: 0,
                },
            ]
        );
        assert!(parse_scale_specs(&["edge-api=1".into(), "edge-api=2".into()]).is_err());
        assert!(parse_scale_specs(&["runtime=2".into()]).is_err());
        assert!(parse_scale_specs(&["stranger=1".into()]).is_err());
    }

    #[test]
    fn coordinates_refuse_a_directory_bound_to_another_deployment() {
        let temp = tempfile::tempdir().expect("deployment directory");
        let mut deployment = stage_definition(temp.path(), "tkd");
        deployment.name = "different-deployment".to_string();

        let error = EcsOperationCoordinates::read(&deployment)
            .expect_err("metadata name must match admission");
        assert!(error.to_string().contains("records name `ops-fixture`"));
    }

    #[test]
    fn coordinates_refuse_another_platform() {
        let temp = tempfile::tempdir().expect("deployment directory");
        let deployment = stage_definition(temp.path(), "tkd");
        let metadata_path = temp.path().join(METADATA_JSON);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read staged metadata"))
                .expect("decode staged metadata");
        metadata["platform"] = "compose".into();
        fs::write(
            metadata_path,
            serde_json::to_vec(&metadata).expect("encode staged metadata"),
        )
        .expect("rewrite staged metadata");

        let error = EcsOperationCoordinates::read(&deployment)
            .expect_err("platform identity must match admission");
        assert!(error.to_string().contains("platform `compose`, not `ecs`"));
    }

    #[test]
    fn coordinates_refuse_an_unregistered_frontend() {
        let temp = tempfile::tempdir().expect("deployment directory");
        let deployment = stage_definition(temp.path(), "tkd");
        let definition_path = temp.path().join("definition.json");
        fs::write(&definition_path, "{}").expect("stage unsupported definition");
        let metadata_path = temp.path().join(METADATA_JSON);
        let mut metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata_path).expect("read staged metadata"))
                .expect("decode staged metadata");
        metadata["definition"] = serde_json::json!({
            "format": "json",
            "path": "definition.json"
        });
        fs::write(
            metadata_path,
            serde_json::to_vec(&metadata).expect("encode staged metadata"),
        )
        .expect("rewrite staged metadata");

        let error = EcsOperationCoordinates::read(&deployment)
            .expect_err("only registered ECS frontends are admitted");
        assert!(
            error
                .to_string()
                .contains("unsupported ECS definition format `json`")
        );
    }

    #[test]
    fn required_coordinates_refuse_whitespace() {
        let error = required("  \n".to_string(), "cluster.name")
            .expect_err("whitespace is not a usable provider coordinate");
        assert_eq!(
            error.to_string(),
            "admitted ECS definition has an empty `cluster.name`"
        );
    }
}
