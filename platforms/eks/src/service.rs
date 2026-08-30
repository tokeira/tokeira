//! Deploy-engine service and Kubernetes manifest application.
//!
//! Workloads are services, not IaC bundles: the definition graph establishes
//! ordering, [`KubernetesService`] produces stable desired manifests, and the
//! deploy engine hands those manifests to [`EksServicePlatform`] for live
//! server-side apply through the registered `KubePlatform`.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use tokeira_deploy_engine::{Platform, RuntimeError, Service, ServiceContext};
use tokeira_iac::ResourceId;
use tokeira_k8s::KubePlatform;

use crate::manifests::{self, ServiceManifest};

/// One realized Kubernetes workload.
#[derive(Debug)]
pub struct KubernetesService {
    spec: ServiceManifest,
    config_content: String,
    alloy_config_content: String,
    dependencies: Vec<String>,
    module: String,
    retained_server_config: Option<PathBuf>,
    live_server_config: Option<PathBuf>,
    dsql_endpoint_dependency: Option<ResourceId>,
}

impl KubernetesService {
    /// Construct a workload at its invocation-bound placement.
    #[allow(
        clippy::too_many_arguments,
        reason = "the arguments are the complete realized service contract"
    )]
    pub fn new(
        spec: ServiceManifest,
        config_content: String,
        alloy_config_content: String,
        dependencies: Vec<String>,
        module: String,
        retained_server_config: Option<PathBuf>,
        live_server_config: Option<PathBuf>,
        dsql_endpoint_dependency: Option<ResourceId>,
    ) -> Self {
        Self {
            spec,
            config_content,
            alloy_config_content,
            dependencies,
            module,
            retained_server_config,
            live_server_config,
            dsql_endpoint_dependency,
        }
    }

    fn server_config_path(&self) -> Result<&PathBuf, RuntimeError> {
        let Some(live) = &self.live_server_config else {
            return Err(RuntimeError::Service(format!(
                "{} has no ServerConfig dependency",
                self.spec.name
            )));
        };
        self.retained_server_config
            .as_ref()
            .into_iter()
            .chain(std::iter::once(live))
            .find(|path| path.is_file())
            .ok_or_else(|| {
                RuntimeError::Service(format!(
                    "{} depends on ServerConfig but neither retained nor live {} can be read",
                    self.spec.name,
                    live.display()
                ))
            })
    }

    fn config_content(&self, ctx: &ServiceContext) -> Result<String, RuntimeError> {
        if self.spec.name == "tokeirad" {
            let path = self.server_config_path()?;
            let mut config = tokeira_config::TokeiraConfig::load(path).map_err(|error| {
                RuntimeError::Service(format!(
                    "tokeirad ServerConfig at {} is invalid: {error}",
                    path.display()
                ))
            })?;
            // The controller Service is stable within the authored namespace;
            // this delivery-only overlay keeps substrate coordinates out of the
            // deployment-owned ServerConfig document.
            config.infrastructure.placement.controller_endpoint =
                Some("http://tokeira-controller:9091".to_string());
            return config
                .to_toml()
                .map_err(|error| RuntimeError::Service(error.to_string()));
        }

        let Some(dependency) = &self.dsql_endpoint_dependency else {
            return Ok(self.config_content.clone());
        };
        let endpoint = ctx
            .infra_state
            .resources
            .get(dependency)
            .and_then(|state| state.properties.get("private_hostname"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                RuntimeError::Service(format!(
                    "{} needs the applied DSQL connection endpoint `{}` before its config can be rendered",
                    self.spec.name, dependency.0
                ))
            })?;
        let rendered = self
            .config_content
            .replace("__WRITEBACK_DSQL_ENDPOINT__", endpoint);
        if rendered.contains("__WRITEBACK_") {
            return Err(RuntimeError::Service(format!(
                "{} config contains an unresolved infrastructure output",
                self.spec.name
            )));
        }
        Ok(rendered)
    }
}

impl Service for KubernetesService {
    fn resource_type(&self) -> &'static str {
        crate::kinds::SERVICE_DEPLOYMENT_TYPE
    }

    fn name(&self) -> &str {
        &self.spec.name
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn dependencies(&self) -> Vec<&str> {
        self.dependencies.iter().map(String::as_str).collect()
    }

    fn manifests(&self, ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError> {
        let config = self.config_content(ctx)?;
        let mut desired = vec![manifests::service_account(
            &self.spec.service_account,
            &self.spec.namespace,
            &self.spec.project,
        )];
        if !self.spec.config_from_content {
            desired.push(manifests::config_map(
                &self.spec.config_map,
                &self.spec.namespace,
                &self.spec.project,
                &self.spec.config_file,
                &config,
            ));
        }
        if !self.spec.alloy_from_content {
            desired.push(manifests::config_map(
                &format!("alloy-config-{}", self.spec.name),
                &self.spec.namespace,
                &self.spec.project,
                "config.alloy",
                &self.alloy_config_content,
            ));
        }
        desired.push(manifests::deployment(&self.spec));
        desired.push(manifests::service(&self.spec));
        Ok(desired)
    }
}

/// Deployment-scoped Kubernetes manifest applier.
#[derive(Debug)]
pub(crate) struct EksServicePlatform {
    deployment: String,
    platforms: Arc<RwLock<BTreeMap<String, KubePlatform>>>,
}

impl EksServicePlatform {
    pub(crate) fn new(
        deployment: String,
        platforms: Arc<RwLock<BTreeMap<String, KubePlatform>>>,
    ) -> Self {
        Self {
            deployment,
            platforms,
        }
    }

    fn platform(&self) -> Result<KubePlatform, RuntimeError> {
        self.platforms
            .read()
            .map_err(|_| RuntimeError::Platform("EKS platform registry lock is poisoned".into()))?
            .get(&self.deployment)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::Platform(format!(
                    "KubePlatform is not registered for deployment `{}`; check kubeconfig, VPC reachability, and EKS authentication",
                    self.deployment
                ))
            })
    }
}

#[async_trait::async_trait]
impl Platform for EksServicePlatform {
    async fn apply_manifests(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<usize, RuntimeError> {
        self.platform()?
            .apply(manifests)
            .await
            .map_err(|error| RuntimeError::Platform(error.to_string()))
    }

    fn supports_delete(&self) -> bool {
        true
    }

    async fn delete_service(
        &self,
        _service_name: &str,
        manifests: &[serde_json::Value],
    ) -> Result<(), RuntimeError> {
        self.platform()?
            .delete(manifests)
            .await
            .map(|_| ())
            .map_err(|error| RuntimeError::Platform(error.to_string()))
    }
}
