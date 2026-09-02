//! What a platform is to the framework: one declaration.
//!
//! A platform describes its infrastructure and services; the framework owns
//! change management. This module is the boundary between them — the value a
//! platform definition returns ([`PlatformDeclaration`]), the authoring-only
//! namespaces it exposes, its live operational capabilities, and the
//! platform implementation that realizes those capabilities.

use std::{fmt, path::PathBuf, pin::Pin, sync::Arc, time::Duration};

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
    /// Platform-owned read-only observability checks, when supported.
    pub observability: Option<Box<dyn ObservabilityCheck>>,
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
            .field("observability", &self.observability.is_some())
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

/// One realized service's sanitized hand-off to live platform operations.
///
/// The framework owns the envelope (resource type and service name); the
/// platform owns the attribute schema. Attributes are deliberately opaque to
/// the framework and must obey [`tokeira_deploy_engine::Service::operations_metadata`].
#[derive(Clone, PartialEq)]
pub struct OperationalService {
    resource_type: String,
    name: String,
    attributes: serde_json::Value,
}

impl OperationalService {
    /// Capture metadata emitted by one realized service.
    pub fn new(resource_type: &str, name: &str, attributes: serde_json::Value) -> Self {
        Self {
            resource_type: resource_type.to_owned(),
            name: name.to_owned(),
            attributes,
        }
    }

    /// Author-visible kind that produced the service.
    pub fn resource_type(&self) -> &str {
        &self.resource_type
    }

    /// Stable service name from the realized graph.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Platform-owned, non-secret descriptor attributes.
    pub fn attributes(&self) -> &serde_json::Value {
        &self.attributes
    }
}

impl fmt::Debug for OperationalService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Attribute values are operational coordinates today, but keeping
        // them out of generic diagnostics prevents a future descriptor bug
        // from turning framework debug output into a disclosure path.
        f.debug_struct("OperationalService")
            .field("resource_type", &self.resource_type)
            .field("name", &self.name)
            .field("attributes", &"<redacted>")
            .finish()
    }
}

/// Definition-derived input to one platform live-operation call.
///
/// The context is rebuilt from the admitted definition for each operator
/// command. It carries authored service coordinates but no provider state;
/// live state remains authoritative for task identity, health, and current
/// capacity. The platform validates its closed descriptor schema before any
/// provider call, so stale or mixed deployment coordinates fail at admission.
#[derive(Clone)]
pub struct DefinitionOperationsContext {
    deployment: DeploymentRef,
    services: Vec<OperationalService>,
}

impl DefinitionOperationsContext {
    /// Assemble the admitted deployment identity and realized service descriptors.
    pub fn new(deployment: DeploymentRef, services: Vec<OperationalService>) -> Self {
        Self {
            deployment,
            services,
        }
    }

    /// Admitted deployment identity associated with every descriptor.
    pub fn deployment(&self) -> &DeploymentRef {
        &self.deployment
    }

    /// Sanitized descriptors in definition declaration order.
    pub fn services(&self) -> &[OperationalService] {
        &self.services
    }
}

impl fmt::Debug for DefinitionOperationsContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefinitionOperationsContext")
            .field("deployment", &self.deployment)
            .field("service_count", &self.services.len())
            .finish()
    }
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

    /// Open logs using the realized definition's operational coordinates.
    ///
    /// Existing identity-only implementations retain their behaviour through
    /// this default. Definition-aware platforms override it when provider
    /// routing needs authored service coordinates beyond [`DeploymentRef`].
    async fn log_stream_with_context(
        &self,
        context: &DefinitionOperationsContext,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> anyhow::Result<LogStream> {
        self.log_stream(context.deployment(), service, follow, tail)
            .await
    }

    /// Resolve the live host/container port mappings for one declared
    /// service.
    async fn port_mappings(
        &self,
        deployment: &DeploymentRef,
        service: &str,
    ) -> anyhow::Result<Vec<PortMapping>>;

    /// Resolve port mappings using realized operational coordinates.
    async fn port_mappings_with_context(
        &self,
        context: &DefinitionOperationsContext,
        service: &str,
    ) -> anyhow::Result<Vec<PortMapping>> {
        self.port_mappings(context.deployment(), service).await
    }

    /// Change workload capacity (`<dim>=<n>` specs), returning the change
    /// count. Required, deliberately undefaulted: an ops surface answers
    /// every one of its verbs in its own words — a provider without a scale
    /// dimension states its own refusal as the error.
    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> anyhow::Result<usize>;

    /// Change capacity using realized operational coordinates.
    async fn scale_with_context(
        &self,
        context: &DefinitionOperationsContext,
        specs: &[String],
    ) -> anyhow::Result<usize> {
        self.scale(context.deployment(), specs).await
    }
}

/// Result of running one platform's observability checks for a deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityCheckReport {
    /// Ordered checks rendered by the operator shell.
    pub checks: Vec<ObservabilityCheckOutcome>,
}

/// One operator-facing observability validation result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilityCheckOutcome {
    /// Stable short name for the checked surface.
    pub name: &'static str,
    /// Whether the checked surface passed or needs operator follow-up.
    pub status: ObservabilityCheckStatus,
    /// Concise evidence or direction for the operator.
    pub detail: String,
}

/// Non-failing statuses in a platform-owned observability check report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservabilityCheckStatus {
    /// The platform's check succeeded.
    Pass,
    /// The platform found a condition that needs operator follow-up but does
    /// not make the check fail.
    Warn,
}

/// Platform-owned observability validation for an admitted deployment.
///
/// The framework deliberately defines no common observability stack or check
/// categories. It realizes the admitted definition before calling this
/// capability, and the platform decides which of its desired resources and
/// artifacts are relevant. Implementations must remain read-only and must not
/// mutate deployment state.
pub trait ObservabilityCheck: Send + Sync + fmt::Debug {
    /// Run the platform-defined checks over its realized resource set.
    ///
    /// `timeout` bounds any read-only reachability work the platform elects to
    /// perform; a purely static check may ignore it.
    fn check(
        &self,
        deployment: &DeploymentRef,
        resources: &[Arc<dyn tokeira_iac::Resource>],
        timeout: Duration,
    ) -> anyhow::Result<ObservabilityCheckReport>;
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
    /// Validate the definition-derived operational hand-off for this platform.
    ///
    /// The framework calls this after realizing service descriptors and before
    /// exposing them to an operations implementation. Platforms that emit no
    /// descriptors need no validation and retain this default.
    fn validate_operations_context(
        &self,
        _context: &DefinitionOperationsContext,
    ) -> anyhow::Result<()> {
        Ok(())
    }

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
