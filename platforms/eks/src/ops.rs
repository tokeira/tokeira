//! Live EKS day-2 operations.
//!
//! The framework supplies deployment identity and its directory. This module
//! re-evaluates that directory's recorded definition to recover the authored
//! namespace, derives the provider cluster name from the admitted deployment
//! name, and then speaks only through [`tokeira_k8s::KubePlatform`]. Scale
//! ordering remains platform-owned because it is a property of the tokeira
//! service graph, not of Kubernetes.

use std::{
    collections::BTreeMap,
    fs,
    sync::{Arc, Mutex},
};

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};
use tokeira_deployment::DeploymentBindingMetadata;
use tokeira_k8s::{KubePlatform, LogOptions, PortForwardConfig, PortForwardSession};
use tokeira_platform::{
    author::from_located_value,
    declaration::{DeploymentRef, LogStream, Ops, PortMapping},
    definition::{
        DefinitionSource, DefinitionSourceName, DirectoryPartSources, evaluate_definition,
    },
};

const METADATA_JSON: &str = "metadata.json";

/// One valid topological order of the authored workload graph.
const STARTUP_ORDER: [&str; 6] = [
    "tokeira-controller",
    "mimir",
    "loki",
    "tokeirad",
    "tokeira-autoscaler",
    "grafana",
];

#[derive(Debug, Serialize)]
struct EvaluationContext {
    project_name: String,
}

#[derive(Debug, Deserialize)]
struct OpsConfiguration {
    eks: OpsEksConfiguration,
}

#[derive(Debug, Deserialize)]
struct OpsEksConfiguration {
    namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeploymentCoordinates {
    namespace: String,
    cluster_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScaleStep {
    service: String,
    replicas: u32,
}

/// EKS's deployment-scoped live operations surface.
///
/// Port-forward sessions are retained for the lifetime of the provisioner
/// process; dropping a session would abort its loopback listener immediately.
#[derive(Debug, Default)]
pub(crate) struct EksOps {
    port_forwards: Mutex<Vec<PortForwardSession>>,
}

fn read_coordinates(deployment: &DeploymentRef) -> Result<DeploymentCoordinates> {
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
    if metadata.platform.as_str() != "eks" {
        bail!(
            "deployment `{}` records platform `{}`, not `eks`",
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
    let bytes = fs::read(&definition_path)
        .with_context(|| format!("failed to read definition {}", definition_path.display()))?;
    let source = DefinitionSource {
        format: definition.format.clone(),
        source_name: DefinitionSourceName::DeploymentRelative(definition.path),
        bytes: Arc::from(bytes),
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
            "deployment `{}` records unsupported EKS definition format `{format}`",
            deployment.name
        ),
    }
    .with_context(|| {
        format!(
            "failed to evaluate admitted EKS definition {}",
            definition_path.display()
        )
    })?;
    let config: OpsConfiguration = from_located_value(evaluated.config)
        .context("admitted EKS definition has no usable `eks.namespace`")?;
    if config.eks.namespace.trim().is_empty() {
        bail!("admitted EKS definition has an empty `eks.namespace`");
    }
    Ok(DeploymentCoordinates {
        namespace: config.eks.namespace,
        cluster_name: format!("{}-eks", deployment.name),
    })
}

fn provider_error(
    coordinates: &DeploymentCoordinates,
    operation: &str,
    error: impl std::fmt::Display,
) -> anyhow::Error {
    anyhow::anyhow!(
        "EKS {operation} failed against cluster `{}` (namespace `{}`): {error}; External operator access assumes kubeconfig credentials and an operator-provided route to the private API (VPN or Direct Connect)",
        coordinates.cluster_name,
        coordinates.namespace
    )
}

fn ensure_service(service: &str) -> Result<()> {
    if STARTUP_ORDER.contains(&service) {
        Ok(())
    } else {
        bail!(
            "unknown EKS service `{service}`; valid services: {}",
            STARTUP_ORDER.join(", ")
        )
    }
}

fn primary_port(service: &str) -> Result<u16> {
    ensure_service(service)?;
    Ok(match service {
        "tokeirad" => 7233,
        "tokeira-controller" => 9091,
        "tokeira-autoscaler" => 9090,
        "mimir" => 9009,
        "loki" => 3100,
        "grafana" => 3000,
        _ => unreachable!("service validation and port inventory agree"),
    })
}

fn parse_scale_specs(specs: &[String]) -> Result<BTreeMap<String, u32>> {
    let mut targets = BTreeMap::new();
    for spec in specs {
        let (service, replicas) = spec
            .split_once('=')
            .and_then(|(service, replicas)| {
                Some((service.trim(), replicas.trim().parse::<u32>().ok()?))
            })
            .ok_or_else(|| anyhow::anyhow!("scale spec `{spec}` is not `<service>=<replicas>`"))?;
        ensure_service(service)?;
        if targets.insert(service.to_string(), replicas).is_some() {
            bail!("scale service `{service}` is specified more than once");
        }
    }
    Ok(targets)
}

fn scale_plan(
    current: &BTreeMap<String, u32>,
    targets: &BTreeMap<String, u32>,
) -> Result<Vec<ScaleStep>> {
    let final_replicas = |service: &str| {
        targets
            .get(service)
            .copied()
            .or_else(|| current.get(service).copied())
            .unwrap_or_default()
    };
    if final_replicas("tokeira-controller") == 0 && final_replicas("tokeirad") > 0 {
        bail!(
            "refusing to scale `tokeira-controller` to zero while `tokeirad` replicas remain; scale `tokeirad=0` in the same request first"
        );
    }

    let mut steps = Vec::new();
    for &service in STARTUP_ORDER.iter().rev() {
        if let Some(&target) = targets.get(service)
            && target < current.get(service).copied().unwrap_or_default()
        {
            steps.push(ScaleStep {
                service: service.to_string(),
                replicas: target,
            });
        }
    }
    for &service in &STARTUP_ORDER {
        if let Some(&target) = targets.get(service)
            && target > current.get(service).copied().unwrap_or_default()
        {
            steps.push(ScaleStep {
                service: service.to_string(),
                replicas: target,
            });
        }
    }
    Ok(steps)
}

async fn current_replicas(
    platform: &KubePlatform,
    coordinates: &DeploymentCoordinates,
) -> Result<BTreeMap<String, u32>> {
    let mut current = BTreeMap::new();
    for service in STARTUP_ORDER {
        let status = platform
            .deployment_status(&coordinates.namespace, service)
            .await
            .map_err(|error| provider_error(coordinates, "scale discovery", error))?;
        current.insert(
            service.to_string(),
            status.map_or(0, |status| status.desired),
        );
    }
    Ok(current)
}

#[async_trait::async_trait]
impl Ops for EksOps {
    async fn log_stream(
        &self,
        deployment: &DeploymentRef,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> Result<LogStream> {
        ensure_service(service)?;
        let coordinates = read_coordinates(deployment)?;
        let platform = KubePlatform::lazy();
        let options = LogOptions::new(follow, tail.map(i64::from), None);
        platform
            .log_stream(&coordinates.namespace, service, &options)
            .await
            .map_err(|error| provider_error(&coordinates, "logs", error))
    }

    async fn port_mappings(
        &self,
        deployment: &DeploymentRef,
        service: &str,
    ) -> Result<Vec<PortMapping>> {
        let remote_port = primary_port(service)?;
        let coordinates = read_coordinates(deployment)?;
        let platform = KubePlatform::lazy();
        let session = platform
            .port_forward(&PortForwardConfig::new(
                &coordinates.namespace,
                service,
                remote_port,
                0,
            ))
            .await
            .map_err(|error| provider_error(&coordinates, "port-forward", error))?;
        let mapping = PortMapping {
            host_addr: session.local_addr.ip().to_string(),
            host_port: session.local_addr.port(),
            container_port: remote_port,
            protocol: "tcp".to_string(),
        };
        self.port_forwards
            .lock()
            .map_err(|_| anyhow::anyhow!("EKS port-forward registry lock is poisoned"))?
            .push(session);
        Ok(vec![mapping])
    }

    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> Result<usize> {
        let targets = parse_scale_specs(specs)?;
        let coordinates = read_coordinates(deployment)?;
        let platform = KubePlatform::lazy();
        let current = current_replicas(&platform, &coordinates).await?;
        let steps = scale_plan(&current, &targets)?;
        for step in &steps {
            platform
                .scale(&coordinates.namespace, &step.service, step.replicas)
                .await
                .map_err(|error| provider_error(&coordinates, "scale", error))?;
            platform
                .wait_ready(&coordinates.namespace, &step.service, None)
                .await
                .map_err(|error| provider_error(&coordinates, "scale readiness", error))?;
        }
        Ok(steps.len())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use proptest::prelude::*;

    use super::*;

    fn replica_map(values: &[u32]) -> BTreeMap<String, u32> {
        STARTUP_ORDER
            .iter()
            .zip(values)
            .map(|(&service, &replicas)| (service.to_string(), replicas))
            .collect()
    }

    fn stage_definition(temp: &Path, format: &str) -> DeploymentRef {
        let source_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let root_name = if format == "tkd" {
            "deployment.tkd"
        } else {
            "definition.tkdp"
        };
        for entry in fs::read_dir(source_dir).expect("EKS source directory") {
            let entry = entry.expect("source entry");
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some(format) {
                continue;
            }
            let mut source = fs::read_to_string(&path).expect("definition source");
            if path.file_name().and_then(|name| name.to_str()) == Some(root_name) {
                source = source.replace("tokeira-system", "from-definition");
            }
            fs::write(temp.join(entry.file_name()), source).expect("stage definition source");
        }
        fs::write(
            temp.join(METADATA_JSON),
            serde_json::to_vec(&serde_json::json!({
                "name": "ops-fixture",
                "id": "7698ae09-197e-4325-9f77-256dac98f23a",
                "platform": "eks",
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
    fn operations_recover_namespace_from_each_admitted_definition_format() {
        for format in ["tkd", "tkdp"] {
            let temp = tempfile::tempdir().expect("deployment directory");
            let deployment = stage_definition(temp.path(), format);
            let coordinates = read_coordinates(&deployment).expect("admitted coordinates");

            assert_eq!(coordinates.namespace, "from-definition");
            assert_eq!(coordinates.cluster_name, "ops-fixture-eks");
        }
    }

    #[test]
    fn scale_specs_refuse_unknown_and_duplicate_services() {
        assert!(parse_scale_specs(&["stranger=1".to_string()]).is_err());
        assert!(parse_scale_specs(&["mimir=1".to_string(), "mimir=2".to_string()]).is_err());
    }

    #[test]
    fn controller_zero_requires_runtime_zero_in_the_same_final_state() {
        let current = replica_map(&[1, 1, 1, 2, 1, 1]);
        let refused = BTreeMap::from([("tokeira-controller".to_string(), 0)]);
        assert!(scale_plan(&current, &refused).is_err());

        let admitted = BTreeMap::from([
            ("tokeira-controller".to_string(), 0),
            ("tokeirad".to_string(), 0),
        ]);
        let plan = scale_plan(&current, &admitted).expect("runtime stops before controller");
        assert_eq!(
            plan,
            vec![
                ScaleStep {
                    service: "tokeirad".to_string(),
                    replicas: 0,
                },
                ScaleStep {
                    service: "tokeira-controller".to_string(),
                    replicas: 0,
                },
            ]
        );
    }

    // Feature: platform-eks, Property 11
    // Every generated mixed scale request orders reductions in reverse startup
    // order, then additions in startup order, while preserving the controller
    // constraint even when either final target is zero.
    proptest! {
        #[test]
        fn scale_ordering_with_zero_admissible(
            current_values in prop::collection::vec(0u32..5, STARTUP_ORDER.len()),
            target_values in prop::collection::vec(0u32..5, STARTUP_ORDER.len()),
        ) {
            let current = replica_map(&current_values);
            let targets = replica_map(&target_values);
            let final_controller = targets["tokeira-controller"];
            let final_runtime = targets["tokeirad"];
            let plan = scale_plan(&current, &targets);

            if final_controller == 0 && final_runtime > 0 {
                prop_assert!(plan.is_err());
                return Ok(());
            }

            let plan = plan.expect("valid final controller/runtime state plans");
            let split = plan
                .iter()
                .position(|step| step.replicas > current[&step.service])
                .unwrap_or(plan.len());
            let indices = |steps: &[ScaleStep]| {
                steps
                    .iter()
                    .map(|step| {
                        STARTUP_ORDER
                            .iter()
                            .position(|service| *service == step.service)
                            .expect("planned service belongs to startup order")
                    })
                    .collect::<Vec<_>>()
            };
            let down = indices(&plan[..split]);
            let up = indices(&plan[split..]);
            prop_assert!(down.windows(2).all(|pair| pair[0] > pair[1]));
            prop_assert!(up.windows(2).all(|pair| pair[0] < pair[1]));
            let every_step_changes_to_its_target = plan.iter().all(|step| {
                step.replicas == targets[&step.service]
                    && step.replicas != current[&step.service]
            });
            prop_assert!(every_step_changes_to_its_target);
            let every_changed_target_is_planned = target_values.iter().enumerate().all(|(index, target)| {
                target == &current_values[index]
                    || plan.iter().any(|step| {
                        step.service == STARTUP_ORDER[index] && step.replicas == *target
                    })
            });
            prop_assert!(every_changed_target_is_planned);
        }
    }
}
