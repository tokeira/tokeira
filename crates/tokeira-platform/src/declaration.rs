//! What a platform is to the framework: one declaration.
//!
//! A platform describes its infrastructure and services; the framework owns
//! change management. This module is the boundary between them — the value a
//! platform's entry point returns ([`PlatformDeclaration`]) and everything it
//! carries: the export of each provider the platform's definitions draw on.
//!
//! Wiring is by construction: a provider's kinds are authorable because its
//! export is in the declaration, and its mechanics run because they arrived
//! in the same export. There is no global kind inventory, no "known but
//! unwired" state, and no separate registration of provider clients — each
//! export carries its own extension constructor ([`InfraConstructor`]), run
//! by the deployment's registration seam.

use std::{fmt, path::PathBuf, pin::Pin, sync::Arc};

use futures_util::Stream;

/// Live host/container port mapping shape shared with the operator surface.
pub use tokeira_orchestrator::PortMapping;

/// Everything the framework needs to operate one platform.
///
/// Constructed by the platform's entry point; construction is pure — no
/// filesystem, no network, no provider clients. Connections happen when the
/// framework runs an operation, never when the platform is declared.
pub struct PlatformDeclaration {
    /// The provider the platform runs on.
    pub provider: ProviderExport,
}

impl PlatformDeclaration {
    /// Declare the provider the platform runs on. Its kind library, runtime
    /// reads, and execution extensions arrive with it; no separate wiring
    /// act exists.
    pub fn on(provider: ProviderExport) -> Self {
        Self { provider }
    }
}

impl fmt::Debug for PlatformDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlatformDeclaration")
            .field("provider", &self.provider)
            .finish()
    }
}

/// One provider's complete export: what running on it means.
///
/// The ops surface (when present) backs the framework's live verbs; the
/// execution seam answers reachability; the infra constructor (when
/// present) is the registration ingredient the deployment runs at
/// operation start.
pub struct ProviderExport {
    /// The provider's ops surface over a running deployment, or `None` —
    /// the framework surfaces the corresponding verbs by presence.
    pub ops: Option<Box<dyn Ops>>,
    /// Answers whether the provider's endpoint is reachable for a
    /// deployment.
    pub execution: Box<dyn ProviderExecution>,
    /// The provider's infra-phase extension constructor, when it has
    /// context handles to register.
    pub infra: Option<Arc<dyn InfraConstructor>>,
}

impl fmt::Debug for ProviderExport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderExport")
            .field("ops", &self.ops.is_some())
            .field("infra", &self.infra.is_some())
            .finish_non_exhaustive()
    }
}

/// Deployment coordinates the framework supplies to provider behaviour:
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

/// A provider's ops surface over a running deployment: the live operator
/// questions answered from the substrate itself.
///
/// Implemented by providers that have one; the framework mounts the
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

/// The provider's execution seam, invoked by the framework: reachability
/// stated as data. Platforms never call it. Registration is not this
/// seam's question — each selection's [`InfraConstructor`] carries it.
#[async_trait::async_trait]
pub trait ProviderExecution: Send + Sync + fmt::Debug {
    /// Is the provider's substrate reachable for this deployment?
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

/// A selection's infra-phase extension constructor: the registration
/// ingredient the deployment runs inside `register_infra_extensions`.
///
/// Registration happens through the deployment's unchanged seam and nowhere
/// else: the deployment runs each declared selection's constructor, the
/// constructor puts handles into the context, and resources read them via
/// `ctx.extension::<T>()` at the mechanics moment. `attributes` is the
/// selection's namespace block from the evaluated configuration (e.g. the
/// `aws` block), transported by the framework and interpreted only here —
/// the provider owns its precedence rule over it.
///
/// Failures are errors — real ones, and equally the unreachable class
/// arriving after a passing probe; the provider keeps that class typed at
/// the error root so it renders as the same platform issue, never a
/// blended message.
#[async_trait::async_trait]
pub trait InfraConstructor: Send + Sync + fmt::Debug {
    /// Register this selection's extensions on the provision context.
    async fn construct(
        &self,
        deployment: &DeploymentRef,
        attributes: Option<&serde_json::Value>,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()>;
}
