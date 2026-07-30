//! Engine adapter — exposes an interpreted `.tkd` as a
//! [`tokeira_orchestrator::Deployment`], the seam the bound `tkp` consumes to
//! drive the existing `InfraEngine`/`DeployEngine` from a deployment definition
//! (Proposal 004 §19).
//!
//! The interpreted [`crate::builder::Deployment`]'s artifacts map onto the trait:
//! `remote_state_module`/`infra_modules` ← `realize_module`, `services` ←
//! `realize_workloads`, `required_namespaces` ← `namespaces`, `collect_writeback`
//! ← `writeback_entries` (resolving the deferred `Output` handles against the
//! post-apply `InfraState`).
//!
//! The `.tkd` is the **config revision** the trait reads; the interpreter + kind
//! library that this module bridges to are the **engine identity** compiled into
//! `tkp` (see the provisioner-binary spec's engine-identity / config-revision
//! split).

use std::path::Path;

use async_trait::async_trait;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_orchestrator::{
    self as orchestrator, OrchestratorError, PlatformConfig, PortMapping, Result as OrchResult,
    ServiceReplicas, StorageKind,
};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

use crate::{
    builder::{Deployment as TkdBuilt, WbValue},
    context::Cx,
    interp,
};

/// The bootstrap module — the engine surfaces it as `remote_state_module`, not an
/// ordinary infra module, so the adapter excludes it from `infra_modules`.
const BOOTSTRAP_MODULE: &str = "local_state";

/// The configuration revision for an interpreted deployment: the `.tkd` source +
/// the engine-injected context. (Mirrors how `ComposeConfig` carries the
/// deployment dir — here the whole structure is the interpreted source.)
#[derive(Clone, Debug)]
pub struct TkdConfig {
    pub source: String,
    pub cx: Cx,
}

/// Adapts an interpreted `.tkd` to [`tokeira_orchestrator::Deployment`].
#[derive(Clone, Default, Debug)]
pub struct TkdDeployment;

impl TkdDeployment {
    /// Interpret the config-revision source into the realized builder deployment.
    /// The `.tkd` is validated when the deployment is loaded, so this does not
    /// fail in the engine's hot path.
    pub(crate) fn realize(cfg: &TkdConfig) -> TkdBuilt {
        interp::interpret(&cfg.source, &cfg.cx)
            .map(|(dep, _cfg)| dep)
            .expect("`.tkd` is interpreted/validated at load")
    }

    fn module_box(cfg: &TkdConfig, name: &str, deps: &[String]) -> Box<dyn iac::Module> {
        let deps: Vec<&'static str> = deps
            .iter()
            .map(|d| &*Box::leak(d.clone().into_boxed_str()))
            .collect();
        Box::new(TkdModule {
            cfg: cfg.clone(),
            name: name.to_string(),
            deps,
        })
    }
}

#[async_trait]
impl orchestrator::Deployment for TkdDeployment {
    type Config = TkdConfig;

    fn remote_state_module(
        &self,
        config: &Self::Config,
        _deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        Self::module_box(config, BOOTSTRAP_MODULE, &[])
    }

    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        let dep = Self::realize(config);
        dep.module_names()
            .into_iter()
            .filter(|m| *m != BOOTSTRAP_MODULE)
            .filter(|m| selection.includes(m))
            .map(|m| {
                let deps = dep
                    .module_deps(m)
                    .map(<[String]>::to_vec)
                    .unwrap_or_default();
                Self::module_box(config, m, &deps)
            })
            .collect()
    }

    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        Self::realize(config).realize_workloads(&config.cx)
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        // Image resolution is not part of the `.tkd` structure: image refs are
        // config strings, and `tkr image push/mirror` is a separate mechanic.
        Vec::new()
    }

    fn required_namespaces(&self, config: &Self::Config) -> Vec<String> {
        Self::realize(config).namespaces().to_vec()
    }

    async fn register_infra_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut iac::ProvisionContext,
    ) -> OrchResult<()> {
        // Live-apply handles (the Docker Compose platform, AWS clients) are
        // mechanic plumbing the bound `tkp` registers; they are not needed to
        // compute the desired shape. (Follow-on, when `tkp` drives a live apply.)
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        _config: &Self::Config,
        _ctx: &mut deploy_engine::ServiceContext,
    ) -> OrchResult<()> {
        Ok(())
    }

    fn create_infra_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::InfraState>> {
        Box::new(CasStore::new(
            Box::new(LocalBackend::new(deployment_dir.join("state/infra"))),
            "infra".to_string(),
        ))
    }

    fn create_deploy_store(
        &self,
        _config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::RuntimeState>> {
        Box::new(CasStore::new(
            Box::new(LocalBackend::new(deployment_dir.join("state/deploy"))),
            "deploy".to_string(),
        ))
    }

    fn hydrate_config(&self, config: &Self::Config, _state: &iac::InfraState) -> Self::Config {
        config.clone()
    }

    fn collect_writeback(
        &self,
        config: &Self::Config,
        state: &iac::InfraState,
    ) -> Vec<(String, String)> {
        let dep = Self::realize(config);
        dep.writeback_entries()
            .iter()
            .filter_map(|(key, wb)| {
                let value = match wb {
                    WbValue::Const(s) => s.clone(),
                    WbValue::Output(out) => {
                        // Resolve the deferred handle: logical (module, resource)
                        // → physical ResourceId → the named state property.
                        let rid =
                            dep.realize_resource_id(&out.module, &out.resource, &config.cx)?;
                        state
                            .resources
                            .get(&rid)?
                            .properties
                            .get(&out.output)?
                            .as_str()?
                            .to_string()
                    }
                };
                Some((key.clone(), value))
            })
            .collect()
    }
}

/// Day-2 ops surface. Replica intent is read from the `.tkd`; the live verbs
/// (scale/logs/ports) need the running platform and are driven through the
/// provisioner, not tkr's day-2 facade.
#[async_trait]
impl orchestrator::Ops for TkdDeployment {
    type Config = TkdConfig;

    fn valid_services(&self) -> &[&str] {
        // The service set is defined in the `.tkd` (per-config), and this method
        // has no config; day-2 verbs are unsupported here, so report none.
        &[]
    }

    fn desired_replicas(&self, config: &Self::Config) -> Vec<ServiceReplicas> {
        Self::realize(config)
            .service_replicas()
            .into_iter()
            .map(|(service, replicas)| ServiceReplicas { service, replicas })
            .collect()
    }

    async fn scale_up(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> OrchResult<()> {
        Err(unsupported_day2("scale"))
    }

    async fn scale_down(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> OrchResult<()> {
        Err(unsupported_day2("scale"))
    }

    async fn logs(&self, _service: &str, _config: &Self::Config) -> OrchResult<Vec<String>> {
        Err(unsupported_day2("logs"))
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> OrchResult<Vec<PortMapping>> {
        Err(unsupported_day2("port mappings"))
    }
}

fn unsupported_day2(op: &str) -> OrchestratorError {
    OrchestratorError::Other(anyhow::anyhow!(
        "`{op}` is not yet supported for compose via tkr; drive it through the provisioner (tkp)"
    ))
}

impl PlatformConfig for TkdDeployment {
    /// The "config" for the compose platform IS the `.tkd` (its `config()` + structure).
    fn prototypical_config(_storage: StorageKind) -> String {
        crate::DEFAULT_TKD.to_string()
    }

    /// The server config (`tokeirad.toml`) the runtime reads — the same default
    /// the engine compose platform ships.
    fn prototypical_server_config(storage: StorageKind) -> String {
        let mut config = tokeira_config::TokeiraConfig::default();
        if storage == StorageKind::Dsql {
            config.infrastructure.storage = tokeira_config::ConfigStorageKind::Dsql;
            config.infrastructure.dsql.endpoint = Some("replace-with-dsql-endpoint".to_string());
            config.infrastructure.dsql.region = Some("us-east-1".to_string());
        }
        config.to_toml().expect("server config serializes")
    }
}

/// An interpreted module, realized on demand (like `ComposeModule`, which
/// reconstructs its resources per `resources()` call).
#[derive(Clone, Debug)]
struct TkdModule {
    cfg: TkdConfig,
    name: String,
    deps: Vec<&'static str>,
}

impl iac::Module for TkdModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn dependencies(&self) -> &[&str] {
        &self.deps
    }

    fn resources(
        &self,
        _ctx: &iac::ModuleContext,
    ) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        Ok(TkdDeployment::realize(&self.cfg)
            .realize_module(&self.name, &self.cfg.cx)
            .unwrap_or_default())
    }
}
