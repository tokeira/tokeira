//! Typed platform identity, catalogs, context/config contracts, and provider selection.

use std::{collections::BTreeSet, marker::PhantomData, path::Component};

use crate::{
    artifact::{ArtifactCatalog, ArtifactClass, InspectionSpec},
    catalog::{ImageCatalog, KindSet, ProviderSet, ServiceCatalog},
    config::{ConfigContract, PlatformConfig},
    context::{ContextContract, PlatformContext},
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

/// Typed state policy declaration; store implementations remain in their owning crates.
#[derive(Debug, Clone)]
pub struct StateBinding<P> {
    /// Existing store/bootstrap policy selected for this platform.
    pub policy: StatePolicy,
    marker: PhantomData<fn() -> P>,
}

impl<P> StateBinding<P> {
    /// Construct a state declaration.
    pub const fn new(policy: StatePolicy) -> Self {
        Self {
            policy,
            marker: PhantomData,
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
            for dependency in &service.placement.needs {
                if !service_ids.contains(dependency) {
                    return Err(BindingError::new(format!(
                        "service `{}` depends on unknown service `{dependency}`",
                        service.logical_id
                    )));
                }
            }
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
            if artifact.class == ArtifactClass::Inspection && !artifact.consumers.is_empty() {
                return Err(BindingError::new(format!(
                    "inspection artifact `{}` cannot have operational consumers",
                    artifact.logical_id
                )));
            }
        }
        for service in self.services.entries() {
            for use_ in &service.configuration {
                if !artifact_ids.contains(&use_.artifact) {
                    return Err(BindingError::new(format!(
                        "service `{}` references unknown artifact `{}`",
                        service.logical_id, use_.artifact
                    )));
                }
            }
        }

        self.ops.validate(&service_ids)?;

        let mut inspection_paths = BTreeSet::new();
        for inspection in &self.inspection {
            if inspection.path.as_os_str().is_empty()
                || inspection.path.is_absolute()
                || inspection
                    .path
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return Err(BindingError::new(format!(
                    "inspection path `{}` is not a safe canonical relative path",
                    inspection.path.display()
                )));
            }
            if !inspection_paths.insert(inspection.path.clone()) {
                return Err(BindingError::new(format!(
                    "duplicate inspection path `{}`",
                    inspection.path.display()
                )));
            }
        }
        Ok(())
    }
}
