//! Typed first-party provider-kind, service, image, and delivery registrations.

use std::{collections::BTreeSet, fmt::Debug, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{
    artifact::{
        ArtifactUse, CanonicalDocument, ContentIdentitySet, DeliveryKey, DesiredDocument,
        OperationalArtifactReceipt, OperationalArtifactRequest,
    },
    author::{AuthorNode, KindSchema, from_author_node},
    error::{BindingError, DeliveryError, KindError},
    graph::WorkloadDeclaration,
};

/// Logical placement supplied to provider kinds and delivery implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementContext {
    /// Stable deployment identity used by provider naming policies.
    pub deployment_id: String,
    /// Owning logical module.
    pub module: String,
    /// Logical id within the module.
    pub logical_id: String,
    /// Realized provider resource ids of declared dependencies.
    pub dependencies: Vec<tokeira_iac::ResourceId>,
    /// Stable platform/provider tags.
    pub tags: std::collections::BTreeMap<String, String>,
}

/// Canonical authored mapping owned beside one provider resource implementation.
pub trait ProviderKind: Debug + Send + Sync {
    /// Stable selected kind name.
    fn kind_name(&self) -> &'static str;

    /// Provider-owned pure input validation.
    fn validate(&self) -> Result<(), KindError>;

    /// Complete names that author code may request through a resource handle.
    fn declared_outputs(&self) -> &'static [&'static str];

    /// Provider-owned desired input manifest used for explanation and evidence.
    fn desired_manifest(&self) -> serde_json::Value;

    /// Convert typed input and logical placement into the existing provider resource.
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError>;
}

/// One typed provider-kind constructor selected by a first-party catalog.
#[derive(Clone, Copy)]
pub struct KindRegistration {
    /// Stable author-visible name.
    pub name: &'static str,
    decode: fn(AuthorNode) -> Result<Box<dyn ProviderKind>, KindError>,
    /// Optional provider-owned defaults for frontend schema presentation.
    pub defaults: Option<fn() -> serde_json::Map<String, serde_json::Value>>,
    /// Complete output inventory, checked against every decoded instance.
    pub declared_outputs: &'static [&'static str],
}

impl std::fmt::Debug for KindRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KindRegistration")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl KindRegistration {
    /// Build the one standard Serde-backed constructor for a provider-owned type.
    pub const fn typed<T>(
        name: &'static str,
        declared_outputs: &'static [&'static str],
        defaults: Option<fn() -> serde_json::Map<String, serde_json::Value>>,
    ) -> Self
    where
        T: ProviderKind + DeserializeOwned + 'static,
    {
        Self {
            name,
            decode: decode_kind::<T>,
            defaults,
            declared_outputs,
        }
    }

    fn decode(&self, input: AuthorNode) -> Result<Box<dyn ProviderKind>, KindError> {
        (self.decode)(input)
    }
}

fn decode_kind<T>(input: AuthorNode) -> Result<Box<dyn ProviderKind>, KindError>
where
    T: ProviderKind + DeserializeOwned + 'static,
{
    let root_range = input.range;
    let kind: T = from_author_node(input).map_err(|error| KindError {
        message: error.message().to_string(),
        range: error.range().or(root_range),
    })?;
    kind.validate().map_err(|error| error.at(root_range))?;
    Ok(Box::new(kind))
}

/// Immutable provider-owned collection of canonical authored kinds.
#[derive(Debug, Clone, Copy)]
pub struct ProviderKindCatalog {
    /// Stable provider identity.
    pub provider: &'static str,
    /// Provider-owned typed registrations.
    pub entries: &'static [KindRegistration],
}

/// Selected union of first-party provider catalogs for one platform.
#[derive(Debug, Clone, Default)]
pub struct KindSet {
    catalogs: Vec<ProviderKindCatalog>,
}

impl KindSet {
    /// Select first-party provider catalogs in deterministic platform order.
    pub fn new(catalogs: Vec<ProviderKindCatalog>) -> Result<Self, BindingError> {
        let set = Self { catalogs };
        set.validate()?;
        Ok(set)
    }

    /// Decode one kind without exposing provider-specific dispatch to a platform.
    pub fn decode(
        &self,
        name: &str,
        input: AuthorNode,
    ) -> Result<Box<dyn ProviderKind>, KindError> {
        let Some(registration) = self
            .catalogs
            .iter()
            .flat_map(|catalog| catalog.entries)
            .find(|registration| registration.name == name)
        else {
            return Err(KindError::new(format!(
                "unknown kind `{name}`; supported kinds: {}",
                self.names().join(", ")
            )));
        };
        let kind = registration.decode(input)?;
        if kind.kind_name() != registration.name {
            return Err(KindError::new(format!(
                "kind registration `{}` decoded provider kind `{}`",
                registration.name,
                kind.kind_name()
            )));
        }
        if kind.declared_outputs() != registration.declared_outputs {
            return Err(KindError::new(format!(
                "kind `{}` output contract differs from its registration",
                registration.name
            )));
        }
        Ok(kind)
    }

    /// Discover selected kind names in catalog order.
    pub fn names(&self) -> Vec<&'static str> {
        self.catalogs
            .iter()
            .flat_map(|catalog| catalog.entries)
            .map(|registration| registration.name)
            .collect()
    }

    /// Build frontend schema entries without retaining provider runtime values.
    pub fn schemas(&self) -> Vec<KindSchema> {
        self.catalogs
            .iter()
            .flat_map(|catalog| catalog.entries)
            .map(|registration| KindSchema {
                name: registration.name.to_string(),
                outputs: registration
                    .declared_outputs
                    .iter()
                    .map(|output| (*output).to_string())
                    .collect(),
            })
            .collect()
    }

    pub(crate) fn validate(&self) -> Result<(), BindingError> {
        let mut providers = BTreeSet::new();
        let mut kinds = BTreeSet::new();
        for catalog in &self.catalogs {
            if catalog.provider.is_empty() || !providers.insert(catalog.provider) {
                return Err(BindingError::new(format!(
                    "duplicate or empty provider kind catalog `{}`",
                    catalog.provider
                )));
            }
            for entry in catalog.entries {
                if entry.name.is_empty() || !kinds.insert(entry.name) {
                    return Err(BindingError::new(format!(
                        "duplicate or empty provider kind `{}`",
                        entry.name
                    )));
                }
                let mut outputs = BTreeSet::new();
                for output in entry.declared_outputs {
                    if output.is_empty() || !outputs.insert(*output) {
                        return Err(BindingError::new(format!(
                            "kind `{}` has duplicate or empty output `{output}`",
                            entry.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Desired image selected by a platform service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageSelection {
    /// Logical image identity in the same platform binding.
    pub logical_id: String,
}

/// One declared service port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePort {
    /// Stable logical port name.
    pub name: String,
    /// Container or workload port.
    pub port: u16,
    /// Transport protocol in canonical lowercase form.
    pub protocol: String,
}

/// Provider-neutral health intent selected by a platform.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HealthDeclaration {
    /// Provider-owned health mode key.
    pub mode: String,
}

/// Provider-neutral placement relationships selected by a platform.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlacementDeclaration {
    /// Logical service dependencies.
    pub needs: Vec<String>,
}

/// One complete platform-owned service declaration.
#[derive(Debug, Clone, PartialEq)]
pub struct PlatformService {
    /// Stable logical identity within the platform binding.
    pub logical_id: String,
    /// Selected platform image.
    pub image: ImageSelection,
    /// Provider command arguments.
    pub command: Vec<String>,
    /// Declared ports.
    pub ports: Vec<ServicePort>,
    /// Health intent.
    pub health: HealthDeclaration,
    /// Placement relationships.
    pub placement: PlacementDeclaration,
    /// Platform-owned content consumed by this service.
    pub configuration: Vec<ArtifactUse>,
    /// Selected provider delivery mechanics.
    pub delivery: DeliveryKey,
    /// Complete provider-specific desired document.
    pub document: DesiredDocument,
}

/// Immutable platform service inventory.
#[derive(Debug, Clone)]
pub struct ServiceCatalog<P> {
    entries: Vec<PlatformService>,
    marker: PhantomData<fn() -> P>,
}

impl<P> ServiceCatalog<P> {
    /// Construct a platform-owned service catalog.
    pub fn new(entries: Vec<PlatformService>) -> Self {
        Self {
            entries,
            marker: PhantomData,
        }
    }

    /// Borrow services in platform declaration order.
    pub fn entries(&self) -> &[PlatformService] {
        &self.entries
    }

    /// Return logical identities for graph admission.
    pub fn identities(&self) -> BTreeSet<String> {
        self.entries
            .iter()
            .map(|service| service.logical_id.clone())
            .collect()
    }

    /// Resolve one logical platform service without constructing a second inventory.
    pub fn get(&self, logical_id: &str) -> Option<&PlatformService> {
        self.entries
            .iter()
            .find(|service| service.logical_id == logical_id)
    }
}

impl<P> Default for ServiceCatalog<P> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Platform image inventory; provider resolution remains outside author evaluation.
#[derive(Debug, Clone)]
pub struct ImageCatalog<P> {
    identities: Vec<String>,
    marker: PhantomData<fn() -> P>,
}

impl<P> ImageCatalog<P> {
    /// Construct an ordered logical image inventory.
    pub fn new(identities: Vec<String>) -> Self {
        Self {
            identities,
            marker: PhantomData,
        }
    }

    /// Borrow image identities in platform order.
    pub fn identities(&self) -> &[String] {
        &self.identities
    }
}

impl<P> Default for ImageCatalog<P> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Pure projection selected by provider delivery for one platform workload.
pub enum DeliveryProjection {
    /// Ordinary infrastructure resource, used when no separate workload universe exists.
    Infrastructure(Box<dyn tokeira_iac::Resource>),
    /// Runtime service for providers managed through the deploy engine.
    Workload(Box<dyn tokeira_deploy_engine::Service>),
}

impl std::fmt::Debug for DeliveryProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Infrastructure(resource) => f
                .debug_tuple("Infrastructure")
                .field(&resource.resource_id())
                .finish(),
            Self::Workload(service) => f.debug_tuple("Workload").field(&service.name()).finish(),
        }
    }
}

/// Provider-owned delivery seam selected by a platform service or artifact.
#[async_trait]
pub trait ProviderDelivery: Debug + Send + Sync {
    /// Stable delivery key.
    fn key(&self) -> &DeliveryKey;

    /// Canonicalize and validate a provider document without changing its semantics.
    fn canonicalize(&self, document: &DesiredDocument) -> Result<CanonicalDocument, DeliveryError>;

    /// Project a provider-delivered workload when the provider has a separate runtime universe.
    ///
    /// `None` keeps the declaration on the infrastructure-resource path, as
    /// Kubernetes manifest bundles do for EKS.
    fn realize(
        &self,
        declaration: &WorkloadDeclaration,
        placement: &PlacementContext,
        content: &ContentIdentitySet,
    ) -> Result<DeliveryProjection, DeliveryError>;

    /// Materialize one operational artifact only during apply through provider-safe publication.
    async fn materialize_operational(
        &self,
        request: OperationalArtifactRequest<'_>,
        context: &tokeira_iac::ProvisionContext,
    ) -> Result<OperationalArtifactReceipt, DeliveryError>;
}

/// Selected provider capabilities for one platform binding.
#[derive(Clone)]
pub struct ProviderSet<P> {
    deliveries: Vec<Arc<dyn ProviderDelivery>>,
    marker: PhantomData<fn() -> P>,
}

impl<P> std::fmt::Debug for ProviderSet<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderSet")
            .field("delivery_keys", &self.delivery_keys())
            .finish()
    }
}

impl<P> ProviderSet<P> {
    /// Construct a selected provider set.
    pub fn new(deliveries: Vec<Arc<dyn ProviderDelivery>>) -> Self {
        Self {
            deliveries,
            marker: PhantomData,
        }
    }

    /// Return delivery identities for graph and binding validation.
    pub fn delivery_keys(&self) -> BTreeSet<String> {
        self.deliveries
            .iter()
            .map(|delivery| delivery.key().as_str().to_string())
            .collect()
    }

    /// Borrow selected delivery implementations.
    pub fn deliveries(&self) -> &[Arc<dyn ProviderDelivery>] {
        &self.deliveries
    }

    /// Resolve selected provider delivery mechanics by stable key.
    pub fn delivery(&self, key: &DeliveryKey) -> Option<&dyn ProviderDelivery> {
        self.deliveries
            .iter()
            .find(|delivery| delivery.key() == key)
            .map(Arc::as_ref)
    }
}

impl<P> Default for ProviderSet<P> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
