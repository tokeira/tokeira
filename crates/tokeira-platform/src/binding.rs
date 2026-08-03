//! Typed platform identity, catalogs, context/config contracts, and provider selection.

use std::{collections::BTreeSet, marker::PhantomData, path::Path, sync::Arc};

use crate::{
    artifact::{ArtifactCatalog, ArtifactClass, InspectionSpec},
    catalog::{ImageCatalog, KindSet, ProviderSet, ServiceCatalog},
    config::{ConfigContract, PlatformConfig},
    context::{ContextContract, InvocationContext, PlatformContext},
    error::BindingError,
    ops::PlatformOps,
};

/// Static relationship between one platform marker and its config/context types.
pub trait Platform: Clone + Send + Sync + 'static {
    /// Typed platform choices admitted from a definition frontend.
    type Config: PlatformConfig;
    /// Immutable platform-specific runtime facts exposed to author code.
    type Context: PlatformContext;

    /// Assemble and validate this platform's immutable first-party binding.
    fn binding(&self) -> PlatformBinding<Self>;
}

/// Existing state-store policy selected by a platform binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatePolicy {
    /// Deployment-local CAS state.
    LocalCas,
    /// Bootstrap locally, then use the admitted S3 state namespace.
    S3Bootstrap,
}

/// Provider-owned construction of a non-local infrastructure/runtime state pair.
///
/// The framework owns the bootstrap ordering; the provider owns credentials,
/// client construction, namespaces, and the concrete store implementation.
pub trait ProviderStateStores<P: Platform>: std::fmt::Debug + Send + Sync {
    /// Construct the infrastructure store selected for this invocation.
    fn infra_store(
        &self,
        config: &P::Config,
        invocation: &InvocationContext,
        deployment_dir: &Path,
    ) -> Box<dyn tokeira_state::DeploymentStore<tokeira_iac::InfraState>>;

    /// Construct the runtime-service store selected for this invocation.
    fn deploy_store(
        &self,
        config: &P::Config,
        invocation: &InvocationContext,
        deployment_dir: &Path,
    ) -> Box<dyn tokeira_state::DeploymentStore<tokeira_iac::RuntimeState>>;
}

/// Typed state policy declaration; store implementations remain in their owning crates.
#[derive(Clone)]
pub struct StateBinding<P> {
    /// Existing store/bootstrap policy selected for this platform.
    pub policy: StatePolicy,
    provider: Option<Arc<dyn ProviderStateStores<P>>>,
    marker: PhantomData<fn() -> P>,
}

impl<P> std::fmt::Debug for StateBinding<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateBinding")
            .field("policy", &self.policy)
            .field("has_provider_stores", &self.provider.is_some())
            .finish()
    }
}

impl<P: Platform> StateBinding<P> {
    /// Construct a local-CAS state declaration.
    ///
    /// `S3Bootstrap` requires [`Self::with_provider`] so the AWS-owned store
    /// implementation remains outside the framework crate.
    pub fn new(policy: StatePolicy) -> Self {
        Self {
            policy,
            provider: None,
            marker: PhantomData,
        }
    }

    /// Construct a provider-owned non-local state declaration.
    pub fn with_provider(policy: StatePolicy, provider: Arc<dyn ProviderStateStores<P>>) -> Self {
        Self {
            policy,
            provider: Some(provider),
            marker: PhantomData,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), BindingError> {
        match (self.policy, self.provider.is_some()) {
            (StatePolicy::LocalCas, false) | (StatePolicy::S3Bootstrap, true) => Ok(()),
            (StatePolicy::LocalCas, true) => Err(BindingError::new(
                "local CAS state must not install a provider-owned store factory",
            )),
            (StatePolicy::S3Bootstrap, false) => Err(BindingError::new(
                "S3 bootstrap state requires a provider-owned store factory",
            )),
        }
    }

    pub(crate) fn infra_store(
        &self,
        config: &P::Config,
        invocation: &InvocationContext,
        deployment_dir: &Path,
    ) -> Box<dyn tokeira_state::DeploymentStore<tokeira_iac::InfraState>> {
        match self.policy {
            StatePolicy::LocalCas => Box::new(tokeira_state::CasStore::new(
                Box::new(tokeira_state::LocalBackend::new(
                    deployment_dir.join("state/infra"),
                )),
                "infra".to_string(),
            )),
            StatePolicy::S3Bootstrap => self
                .provider
                .as_ref()
                .expect("binding validation requires provider stores for S3 bootstrap")
                .infra_store(config, invocation, deployment_dir),
        }
    }

    pub(crate) fn deploy_store(
        &self,
        config: &P::Config,
        invocation: &InvocationContext,
        deployment_dir: &Path,
    ) -> Box<dyn tokeira_state::DeploymentStore<tokeira_iac::RuntimeState>> {
        match self.policy {
            StatePolicy::LocalCas => Box::new(tokeira_state::CasStore::new(
                Box::new(tokeira_state::LocalBackend::new(
                    deployment_dir.join("state/deploy"),
                )),
                "deploy".to_string(),
            )),
            StatePolicy::S3Bootstrap => self
                .provider
                .as_ref()
                .expect("binding validation requires provider stores for S3 bootstrap")
                .deploy_store(config, invocation, deployment_dir),
        }
    }
}

/// Complete immutable input used by the generic framework for one platform.
#[derive(Debug, Clone)]
pub struct PlatformBinding<P: Platform> {
    /// Open, validated platform identity.
    pub id: tokeira_orchestrator::PlatformId,
    /// Module that establishes the selected state substrate.
    pub bootstrap_module: String,
    /// Typed configuration admission.
    pub config: ConfigContract<P::Config>,
    /// Typed immutable context construction/access.
    pub context: ContextContract<P::Context>,
    /// Selected first-party provider kinds.
    pub kinds: KindSet,
    /// Platform-owned complete service declarations.
    pub services: ServiceCatalog<P>,
    /// Platform-owned operational and inspection content.
    pub artifacts: ArtifactCatalog<P>,
    /// Platform-owned image selections.
    pub images: ImageCatalog<P>,
    /// Selected provider delivery/execution registrations.
    pub providers: ProviderSet<P>,
    /// Existing state implementation and bootstrap policy.
    pub state: StateBinding<P>,
    /// Pure log and port-forward declarations.
    pub ops: PlatformOps,
    /// Reproducible inspection publications.
    pub inspection: Vec<InspectionSpec<P>>,
}

impl<P: Platform> PlatformBinding<P> {
    /// Validate and construct one binding without introducing a runtime plugin registry.
    #[allow(clippy::too_many_arguments)] // mirrors the explicit binding contract reviewed in the spec
    pub fn new(
        id: tokeira_orchestrator::PlatformId,
        bootstrap_module: impl Into<String>,
        config: ConfigContract<P::Config>,
        context: ContextContract<P::Context>,
        kinds: KindSet,
        services: ServiceCatalog<P>,
        artifacts: ArtifactCatalog<P>,
        images: ImageCatalog<P>,
        providers: ProviderSet<P>,
        state: StateBinding<P>,
        ops: PlatformOps,
        inspection: Vec<InspectionSpec<P>>,
    ) -> Result<Self, BindingError> {
        let binding = Self {
            id,
            bootstrap_module: bootstrap_module.into(),
            config,
            context,
            kinds,
            services,
            artifacts,
            images,
            providers,
            state,
            ops,
            inspection,
        };
        binding.validate()?;
        Ok(binding)
    }

    fn validate(&self) -> Result<(), BindingError> {
        if self.bootstrap_module.is_empty() {
            return Err(BindingError::new("bootstrap module cannot be empty"));
        }
        self.kinds.validate()?;

        for (class, names) in [
            ("context field", P::Context::fields()),
            ("context method", P::Context::methods()),
        ] {
            let mut seen = BTreeSet::new();
            for name in names {
                if name.is_empty() || !seen.insert(*name) {
                    return Err(BindingError::new(format!(
                        "duplicate or empty {class} `{name}`"
                    )));
                }
            }
        }

        let delivery_keys = self.providers.delivery_keys();
        if delivery_keys.len() != self.providers.deliveries().len() {
            return Err(BindingError::new("duplicate provider delivery key"));
        }
        let execution_keys = self.providers.execution_keys();
        if execution_keys.len() != self.providers.executions().len() {
            return Err(BindingError::new("duplicate provider execution key"));
        }
        let operation_keys = self.providers.operation_keys();
        if operation_keys.len() != self.providers.operations().len() {
            return Err(BindingError::new("duplicate provider operation key"));
        }
        if operation_keys
            .iter()
            .any(|(provider, operation)| provider.is_empty() || operation.is_empty())
        {
            return Err(BindingError::new("provider operation keys cannot be empty"));
        }
        self.state.validate()?;

        let mut images = BTreeSet::new();
        for image in self.images.identities() {
            if image.is_empty() || !images.insert(image.as_str()) {
                return Err(BindingError::new(format!(
                    "duplicate or empty image identity `{image}`"
                )));
            }
        }

        let mut service_ids = BTreeSet::new();
        for service in self.services.entries() {
            if service.logical_id.is_empty() || !service_ids.insert(service.logical_id.clone()) {
                return Err(BindingError::new(format!(
                    "duplicate or empty service identity `{}`",
                    service.logical_id
                )));
            }
            if !images.contains(service.image.logical_id.as_str()) {
                return Err(BindingError::new(format!(
                    "service `{}` references unknown image `{}`",
                    service.logical_id, service.image.logical_id
                )));
            }
            if !delivery_keys.contains(service.delivery.as_str()) {
                return Err(BindingError::new(format!(
                    "service `{}` references unknown delivery `{}`",
                    service.logical_id,
                    service.delivery.as_str()
                )));
            }
            if service.document.schema.is_empty() {
                return Err(BindingError::new(format!(
                    "service `{}` has an empty provider document schema",
                    service.logical_id
                )));
            }
            let mut port_names = BTreeSet::new();
            for port in &service.ports {
                if port.name.is_empty() || !port_names.insert(port.name.as_str()) {
                    return Err(BindingError::new(format!(
                        "service `{}` has duplicate or empty port identity `{}`",
                        service.logical_id, port.name
                    )));
                }
                if port.protocol.is_empty() {
                    return Err(BindingError::new(format!(
                        "service `{}` port `{}` has an empty protocol",
                        service.logical_id, port.name
                    )));
                }
            }
            let delivery = self
                .providers
                .delivery(&service.delivery)
                .expect("delivery membership was checked above");
            delivery.canonicalize(&service.document).map_err(|error| {
                BindingError::new(format!(
                    "service `{}` has an invalid provider document: {}",
                    service.logical_id, error
                ))
            })?;
        }
        for service in self.services.entries() {
            let mut dependencies = BTreeSet::new();
            for dependency in &service.placement.needs {
                if !service_ids.contains(dependency) {
                    return Err(BindingError::new(format!(
                        "service `{}` depends on unknown service `{dependency}`",
                        service.logical_id
                    )));
                }
                if !dependencies.insert(dependency.as_str()) {
                    return Err(BindingError::new(format!(
                        "service `{}` declares duplicate dependency `{dependency}`",
                        service.logical_id
                    )));
                }
            }
        }
        if let Some(cycle) = service_cycle(&self.services) {
            return Err(BindingError::new(format!(
                "service placement cycle involving {}",
                cycle.join(", ")
            )));
        }

        let mut artifact_ids = BTreeSet::new();
        for artifact in self.artifacts.entries() {
            if artifact.logical_id.is_empty() || !artifact_ids.insert(artifact.logical_id.clone()) {
                return Err(BindingError::new(format!(
                    "duplicate or empty artifact identity `{}`",
                    artifact.logical_id
                )));
            }
            if !delivery_keys.contains(artifact.delivery.as_str()) {
                return Err(BindingError::new(format!(
                    "artifact `{}` references unknown delivery `{}`",
                    artifact.logical_id,
                    artifact.delivery.as_str()
                )));
            }
            for consumer in &artifact.consumers {
                if !service_ids.contains(consumer) {
                    return Err(BindingError::new(format!(
                        "artifact `{}` references unknown consumer `{consumer}`",
                        artifact.logical_id
                    )));
                }
            }
            let consumer_count = artifact.consumers.iter().collect::<BTreeSet<_>>().len();
            if consumer_count != artifact.consumers.len() {
                return Err(BindingError::new(format!(
                    "artifact `{}` contains duplicate consumers",
                    artifact.logical_id
                )));
            }
            if artifact.class == ArtifactClass::Inspection && !artifact.consumers.is_empty() {
                return Err(BindingError::new(format!(
                    "inspection artifact `{}` cannot have operational consumers",
                    artifact.logical_id
                )));
            }
        }
        for service in self.services.entries() {
            let mut uses = BTreeSet::new();
            for use_ in &service.configuration {
                if use_.role.is_empty()
                    || !uses.insert((use_.artifact.as_str(), use_.role.as_str()))
                {
                    return Err(BindingError::new(format!(
                        "service `{}` has a duplicate or empty artifact role for `{}`",
                        service.logical_id, use_.artifact
                    )));
                }
                let Some(artifact) = self.artifacts.get(&use_.artifact) else {
                    return Err(BindingError::new(format!(
                        "service `{}` references unknown artifact `{}`",
                        service.logical_id, use_.artifact
                    )));
                };
                if artifact.class != ArtifactClass::Operational {
                    return Err(BindingError::new(format!(
                        "service `{}` cannot consume inspection artifact `{}`",
                        service.logical_id, use_.artifact
                    )));
                }
                if !artifact.consumers.contains(&service.logical_id) {
                    return Err(BindingError::new(format!(
                        "service `{}` is not an authorized consumer of artifact `{}`",
                        service.logical_id, use_.artifact
                    )));
                }
            }
        }

        self.ops.validate(&service_ids, &self.providers)?;

        let mut inspection_paths = BTreeSet::new();
        for inspection in &self.inspection {
            if !inspection_paths.insert(inspection.path().clone()) {
                return Err(BindingError::new(format!(
                    "duplicate inspection path `{}`",
                    inspection.path().as_path().display()
                )));
            }
            if inspection.renderer_key().is_empty() {
                return Err(BindingError::new(format!(
                    "inspection path `{}` has an empty renderer identity",
                    inspection.path().as_path().display()
                )));
            }
        }
        Ok(())
    }
}

fn service_cycle<P>(services: &ServiceCatalog<P>) -> Option<Vec<String>> {
    fn visit<P>(
        service: &str,
        services: &ServiceCatalog<P>,
        visiting: &mut Vec<String>,
        visited: &mut BTreeSet<String>,
    ) -> Option<Vec<String>> {
        if let Some(index) = visiting.iter().position(|candidate| candidate == service) {
            return Some(visiting[index..].to_vec());
        }
        if visited.contains(service) {
            return None;
        }
        visiting.push(service.to_string());
        let declaration = services
            .get(service)
            .expect("binding validation calls cycle detection only for known services");
        for dependency in &declaration.placement.needs {
            if let Some(cycle) = visit(dependency, services, visiting, visited) {
                return Some(cycle);
            }
        }
        let completed = visiting.pop().expect("visited service is on the DFS stack");
        visited.insert(completed);
        None
    }

    let mut visiting = Vec::new();
    let mut visited = BTreeSet::new();
    for service in services.entries() {
        if let Some(cycle) = visit(&service.logical_id, services, &mut visiting, &mut visited) {
            return Some(cycle);
        }
    }
    None
}
