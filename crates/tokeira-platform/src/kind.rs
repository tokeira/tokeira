//! Typed frontend kinds and invocation-bound resource realization.

use std::{collections::BTreeMap, fmt, marker::PhantomData};

use crate::{content::ContentIdentity, error::KindError};

/// A frontend definition capable of realizing exactly one resource type.
///
/// The contract ends when `R` is constructed. Validation, outputs, desired
/// representation, and lifecycle all belong to the realized resource.
pub trait Kind<R>: fmt::Debug + Send + Sync {
    /// Construct the represented resource with its invocation placement.
    fn realize(&self, placement: &PlacementContext) -> Result<R, KindError>;
}

/// One realized resource, retained only until it is handed to its owner.
pub enum RealizedResource {
    /// Infrastructure lifecycle owned by `tokeira-iac`.
    Infra(Box<dyn tokeira_iac::Resource>),
    /// Workload lifecycle owned by `tokeira-deploy-engine`.
    Service(Box<dyn tokeira_deploy_engine::Service>),
}

impl RealizedResource {
    /// The resource type supplied by the concrete lifecycle contract.
    pub(crate) fn resource_type(&self) -> String {
        match self {
            Self::Infra(resource) => resource.resource_type().0,
            Self::Service(service) => service.resource_type().to_string(),
        }
    }

    /// Validate the complete resource before its owning engine sees it.
    pub(crate) fn validate_input(&self) -> Result<(), String> {
        match self {
            Self::Infra(resource) => resource.validate_input(),
            Self::Service(service) => service.validate_input(),
        }
    }

    /// Complete output names supplied by the concrete resource.
    pub(crate) fn declared_outputs(&self) -> &'static [&'static str] {
        match self {
            Self::Infra(resource) => resource.declared_outputs(),
            Self::Service(service) => service.declared_outputs(),
        }
    }
}

impl fmt::Debug for RealizedResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Infra(resource) => formatter
                .debug_tuple("Infra")
                .field(&resource.resource_id())
                .finish(),
            Self::Service(service) => formatter
                .debug_tuple("Service")
                .field(&service.name())
                .finish(),
        }
    }
}

/// A decoded frontend kind whose concrete `R` is hidden from the graph.
///
/// Type erasure exists only because one definition contains heterogeneous
/// resources. Provider authors implement [`Kind<R>`]; they never implement
/// this storage boundary or select lifecycle behaviour through it.
pub struct DecodedKind {
    authored_name: &'static str,
    inner: Box<dyn ErasedKind>,
}

impl DecodedKind {
    /// Erase an infrastructure resource kind for heterogeneous graph storage.
    pub fn resource<K, R>(name: &'static str, kind: K) -> Self
    where
        K: Kind<R> + 'static,
        R: tokeira_iac::Resource + 'static,
    {
        Self {
            authored_name: name,
            inner: Box::new(InfraKind::<K, R> {
                kind,
                resource: PhantomData,
            }),
        }
    }

    /// Erase a service kind for heterogeneous graph storage.
    pub(crate) fn service<K, R>(name: &'static str, kind: K) -> Self
    where
        K: Kind<R> + 'static,
        R: tokeira_deploy_engine::Service + 'static,
    {
        Self {
            authored_name: name,
            inner: Box::new(ServiceKind::<K, R> {
                kind,
                resource: PhantomData,
            }),
        }
    }

    /// Author-visible name selected through the namespace.
    pub fn name(&self) -> &'static str {
        self.authored_name
    }

    /// Realize the concrete resource and verify that the namespace introduced
    /// the type the resource itself names.
    pub fn realize(&self, placement: &PlacementContext) -> Result<RealizedResource, KindError> {
        let resource = self.inner.realize(placement)?;
        let actual = resource.resource_type();
        if actual != self.authored_name {
            return Err(KindError::new(format!(
                "kind `{}` realized resource type `{actual}`",
                self.authored_name
            )));
        }
        resource.validate_input().map_err(KindError::new)?;
        Ok(resource)
    }
}

impl fmt::Debug for DecodedKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedKind")
            .field("name", &self.authored_name)
            .finish_non_exhaustive()
    }
}

trait ErasedKind: Send + Sync {
    fn realize(&self, placement: &PlacementContext) -> Result<RealizedResource, KindError>;
}

struct InfraKind<K, R> {
    kind: K,
    resource: PhantomData<fn() -> R>,
}

impl<K, R> ErasedKind for InfraKind<K, R>
where
    K: Kind<R>,
    R: tokeira_iac::Resource + 'static,
{
    fn realize(&self, placement: &PlacementContext) -> Result<RealizedResource, KindError> {
        self.kind
            .realize(placement)
            .map(|resource| RealizedResource::Infra(Box::new(resource)))
    }
}

struct ServiceKind<K, R> {
    kind: K,
    resource: PhantomData<fn() -> R>,
}

impl<K, R> ErasedKind for ServiceKind<K, R>
where
    K: Kind<R>,
    R: tokeira_deploy_engine::Service + 'static,
{
    fn realize(&self, placement: &PlacementContext) -> Result<RealizedResource, KindError> {
        self.kind
            .realize(placement)
            .map(|service| RealizedResource::Service(Box::new(service)))
    }
}

/// Decode an authored kind whose resource belongs to the infrastructure
/// lifecycle. The lifecycle bound is applied here, after [`Kind<R>`], by the
/// definition container that must erase heterogeneous resources.
pub fn decode_resource<K, R>(
    name: &'static str,
    value: crate::author::LocatedValue,
) -> Result<DecodedKind, KindError>
where
    K: Kind<R> + serde::de::DeserializeOwned + 'static,
    R: tokeira_iac::Resource + 'static,
{
    decode(value).map(|kind| DecodedKind::resource::<K, R>(name, kind))
}

/// Decode an authored kind whose resource belongs to the service lifecycle.
/// The frontend still receives one opaque [`DecodedKind`]; this function only
/// supplies the container's unavoidable type erasure.
pub fn decode_service<K, R>(
    name: &'static str,
    value: crate::author::LocatedValue,
) -> Result<DecodedKind, KindError>
where
    K: Kind<R> + serde::de::DeserializeOwned + 'static,
    R: tokeira_deploy_engine::Service + 'static,
{
    decode(value).map(|kind| DecodedKind::service::<K, R>(name, kind))
}

fn decode<K>(value: crate::author::LocatedValue) -> Result<K, KindError>
where
    K: serde::de::DeserializeOwned,
{
    let range = value.range;
    crate::author::from_located_value::<K>(value)
        .map_err(|error| KindError::new(error.to_string()).at(error.range().or(range)))
}

/// Logical placement supplied to a resource exactly once at execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementContext {
    /// Stable deployment identity used by provider naming policies.
    pub deployment_id: String,
    /// Deployment root admitted by the shell for platform-local resources.
    pub deployment_dir: std::path::PathBuf,
    /// Directory the interpreted definition source was read from — the
    /// deployment root for a working realization, a retained revision folder
    /// for a baseline. Resources that read desired-source companion files
    /// resolve them here, so a baseline realization digests the retained
    /// companion rather than the live one.
    pub definition_dir: std::path::PathBuf,
    /// Owning logical module.
    pub module: String,
    /// Logical id within the module.
    pub logical_id: String,
    /// Realized provider resource ids of declared dependencies.
    pub dependencies: Vec<tokeira_iac::ResourceId>,
    /// Desired-content identities of those same dependencies.
    pub dependency_content: BTreeMap<tokeira_iac::ResourceId, ContentIdentity>,
    /// Stable platform/provider tags.
    pub tags: BTreeMap<String, String>,
}
