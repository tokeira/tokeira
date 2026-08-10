//! What a platform is to the framework: one declaration.
//!
//! A platform describes its infrastructure and services; the framework owns
//! change management. This module is the boundary between them — the value a
//! platform definition returns ([`PlatformDeclaration`]), the authoring-only
//! namespaces it exposes, its live operational capabilities, and the
//! platform implementation that realizes those capabilities.

use std::{fmt, path::PathBuf, pin::Pin, sync::Arc};

use futures_util::Stream;

use crate::definition::Namespace;

/// Live host/container port mapping shape shared with the operator surface.
pub use tokeira_orchestrator::PortMapping;

/// Everything the framework needs to operate one platform.
///
/// Constructed by the platform's entry point; construction is pure — no
/// filesystem, no network, no provider clients. Connections happen when the
/// framework runs an operation, never when the platform is declared.
pub struct PlatformDeclaration {
    /// Normalized crate namespaces visible to definition frontends. These
    /// carry authoring facts only; execution remains in the declaration's
    /// other fields.
    pub namespaces: Vec<Namespace>,
    /// Live operational queries exposed by this platform, when supported.
    pub ops: Option<Box<dyn Ops>>,
    /// Reachability of the platform's execution substrate.
    pub execution: Box<dyn PlatformExecution>,
    /// The platform-owned execution implementation.
    pub implementation: Arc<dyn PlatformIntegration>,
}

impl fmt::Debug for PlatformDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlatformDeclaration")
            .field(
                "namespaces",
                &self
                    .namespaces
                    .iter()
                    .map(|namespace| namespace.name)
                    .collect::<Vec<_>>(),
            )
            .field("ops", &self.ops.is_some())
            .field("execution", &self.execution)
            .field("implementation", &self.implementation)
            .finish_non_exhaustive()
    }
}

/// Deployment coordinates the framework supplies to platform behaviour:
/// identity, never state.
#[derive(Debug, Clone)]
pub struct DeploymentRef {
    /// Deployment name — the project scope provider resources carry.
    pub name: String,
    /// Deployment directory, for providers whose artifacts are path-scoped.
    pub dir: PathBuf,
}

/// Incremental log output for one service.
pub type LogStream = Pin<Box<dyn Stream<Item = anyhow::Result<String>> + Send>>;

/// A platform's ops surface over a running deployment: the live operator
/// questions answered from the substrate itself.
///
/// Implemented by platforms that have one; the framework mounts the
/// corresponding operator verbs by presence and passes the operator's
/// service name through — an implementation answers for the services it
/// finds on the substrate, and an unknown name surfaces as the provider's
/// own error.
#[async_trait::async_trait]
pub trait Ops: Send + Sync + fmt::Debug {
    /// Open incremental logs for one declared service.
    async fn log_stream(
        &self,
        deployment: &DeploymentRef,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> anyhow::Result<LogStream>;

    /// Resolve the live host/container port mappings for one declared
    /// service.
    async fn port_mappings(
        &self,
        deployment: &DeploymentRef,
        service: &str,
    ) -> anyhow::Result<Vec<PortMapping>>;

    /// Change workload capacity (`<dim>=<n>` specs), returning the change
    /// count. Required, deliberately undefaulted: an ops surface answers
    /// every one of its verbs in its own words — a provider without a scale
    /// dimension states its own refusal as the error.
    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> anyhow::Result<usize>;
}

/// The platform's execution seam, invoked by the framework: reachability
/// stated as data. Registration is owned independently by
/// [`PlatformIntegration`].
#[async_trait::async_trait]
pub trait PlatformExecution: Send + Sync + fmt::Debug {
    /// Is the platform's substrate reachable for this deployment?
    ///
    /// `Ok(None)` — reachable. `Ok(Some(issue))` — the degradable answer,
    /// stated as data: the engine's plan blocks (it plans nothing and the
    /// issue is the outcome's only content — describing the live substrate
    /// is a precondition of comparing against the record); apply and
    /// destroy refuse on it. `Err` — a non-provider
    /// failure.
    ///
    /// A point-in-time answer, not a guarantee: the substrate can go down
    /// the moment after a passing probe. Failures after a passing probe
    /// surface through the operation's own error path, carrying the same
    /// platform-issue evidence — nothing may be "optimized" on the
    /// assumption that a probe protects the operation behind it.
    async fn probe(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Option<tokeira_iac::PlatformIssue>>;
}

/// Execution owned by a platform implementation.
///
/// The provisioner delegates the orchestrator's extension-registration seams
/// and service-manifest application to this object. It receives deployment
/// identity but no frontend namespace metadata: resources and services carry
/// their own desired configuration, while shared platform-specific handles
/// enter the engine contexts only through these methods. Standard framework
/// extensions are installed before this delegation.
#[async_trait::async_trait]
pub trait PlatformIntegration: Send + Sync + fmt::Debug {
    /// Register infrastructure handles used by realized resources.
    async fn register_infra_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()>;

    /// Register runtime handles used while realizing service manifests.
    async fn register_deploy_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_deploy_engine::ServiceContext,
    ) -> anyhow::Result<()>;

    /// Register image handles used while resolving service images.
    async fn register_image_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_deploy_engine::ImageContext,
    ) -> anyhow::Result<()>;

    /// Construct the platform that applies this deployment's service
    /// manifests.
    fn service_platform(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Box<dyn tokeira_deploy_engine::Platform>>;
}
