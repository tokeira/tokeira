//! Live operator actions exposed by a platform.
//!
//! The framework admits deployment identity and forwards an operator request;
//! each platform owns its service vocabulary, provider discovery, credentials,
//! and any long-lived subprocess or stream. These operations inspect or act on
//! the live substrate and never create a second deployment-state authority.

use std::{fmt, pin::Pin};

use futures_util::Stream;

use crate::declaration::DeploymentRef;

/// Live host/container port mapping shape shared with the operator surface.
pub use tokeira_orchestrator::PortMapping;

/// Incremental log output for one service.
pub type LogStream = Pin<Box<dyn Stream<Item = anyhow::Result<String>> + Send>>;

/// Result of a platform-owned `port-forward` operation.
///
/// Platforms whose services are already published on the operator's host
/// return [`Self::Mappings`]. Private substrates may instead hold the call
/// open while a provider tunnel runs and return [`Self::SessionClosed`] only
/// after that session exits. This keeps provider credentials and topology
/// behind the platform boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PortForwardOutcome {
    /// No tunnel was necessary; these live host mappings are the answer.
    Mappings(Vec<PortMapping>),
    /// A platform-managed forwarding session completed successfully.
    SessionClosed,
}

/// A platform's operations surface over a running deployment.
///
/// Implemented by platforms that have one; the framework mounts the
/// corresponding operator verbs by presence and passes the operator's service
/// name through. Provider-specific selection and refusal remain platform
/// policy rather than becoming generic deployment configuration.
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

    /// Reach one service from the operator's host.
    ///
    /// The default preserves mapping-only platforms: `local_port` has no
    /// effect because those services are already published. A private
    /// platform overrides this method to own target discovery, credentials,
    /// and the lifetime of its tunnel.
    async fn port_forward(
        &self,
        deployment: &DeploymentRef,
        service: &str,
        _local_port: Option<u16>,
    ) -> anyhow::Result<PortForwardOutcome> {
        Ok(PortForwardOutcome::Mappings(
            self.port_mappings(deployment, service).await?,
        ))
    }

    /// Execute an interactive command in one live service container.
    ///
    /// Container execution is deliberately optional even when a platform has
    /// other live operations. An implementation that supports it owns task
    /// and container selection, remote authorization, terminal attachment,
    /// and subprocess cleanup.
    async fn exec(
        &self,
        _deployment: &DeploymentRef,
        service: &str,
        _container: Option<&str>,
        _command: &[String],
    ) -> anyhow::Result<()> {
        anyhow::bail!("interactive exec is not supported for service `{service}` on this platform")
    }

    /// Run one command through the platform's on-demand administrative
    /// workload.
    ///
    /// Platforms that support this operation own temporary capacity,
    /// readiness, command execution, and restoration of the prior capacity.
    /// The generic shell never assumes that an admin workload is continuously
    /// running or that it is named in a particular way.
    async fn admin(&self, _deployment: &DeploymentRef, _command: &[String]) -> anyhow::Result<()> {
        anyhow::bail!("on-demand administration is not supported on this platform")
    }

    /// Change workload capacity (`<dim>=<n>` specs), returning the change
    /// count. Required, deliberately undefaulted: an ops surface answers
    /// every one of its verbs in its own words — a provider without a scale
    /// dimension states its own refusal as the error.
    async fn scale(&self, deployment: &DeploymentRef, specs: &[String]) -> anyhow::Result<usize>;
}
