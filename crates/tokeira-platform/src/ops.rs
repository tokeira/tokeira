//! Pure platform operation declarations and provider-owned execution contracts.
//!
//! Platforms name logical services and serialize typed provider targets. The
//! framework validates those envelopes against selected provider registrations,
//! reports one stable supported inventory, and applies local-port overrides
//! outside the provider target. Provider crates retain discovery and transport;
//! operator-facing process and session lifetime remains outside this crate.

use std::{collections::BTreeSet, fmt::Debug, marker::PhantomData};

use async_trait::async_trait;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::error::{BindingError, OpsError};

/// Stable provider identity used to dispatch an operational request.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProviderKey(String);

impl ProviderKey {
    /// Construct a non-empty provider operation identity.
    pub fn new(value: impl Into<String>) -> Result<Self, OpsError> {
        let value = value.into();
        if value.is_empty() {
            return Err(OpsError::InvalidRegistration(
                "operation provider key cannot be empty".into(),
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the provider identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable provider-owned operation identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationKey(String);

impl OperationKey {
    /// Construct a non-empty provider operation key.
    pub fn new(value: impl Into<String>) -> Result<Self, OpsError> {
        let value = value.into();
        if value.is_empty() {
            return Err(OpsError::InvalidRegistration(
                "operation key cannot be empty".into(),
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the provider-owned operation identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Typed provider registration used by platforms to construct request envelopes.
#[derive(Debug, Clone, Copy)]
pub struct OperationRegistration<T> {
    provider: &'static str,
    operation: &'static str,
    marker: PhantomData<fn() -> T>,
}

impl<T> OperationRegistration<T> {
    /// Declare one provider-owned typed target contract.
    pub const fn new(provider: &'static str, operation: &'static str) -> Self {
        Self {
            provider,
            operation,
            marker: PhantomData,
        }
    }

    /// Stable provider identity recorded in request envelopes.
    pub const fn provider(&self) -> &'static str {
        self.provider
    }

    /// Stable provider-owned operation identity recorded in request envelopes.
    pub const fn operation(&self) -> &'static str {
        self.operation
    }
}

impl<T: DeserializeOwned> OperationRegistration<T> {
    /// Decode and validate the serialized target through the provider-owned type.
    pub fn decode(&self, target: serde_json::Value) -> Result<T, OpsError> {
        serde_json::from_value(target).map_err(|source| OpsError::InvalidTarget {
            provider: self.provider.to_string(),
            operation: self.operation.to_string(),
            message: source.to_string(),
        })
    }
}

/// Provider-neutral envelope holding one admitted provider target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationRequest {
    provider: ProviderKey,
    operation: OperationKey,
    target: serde_json::Value,
}

impl OperationRequest {
    /// Serialize one provider-owned typed target without retaining its Rust type.
    pub fn typed<T: Serialize>(
        registration: &'static OperationRegistration<T>,
        target: T,
    ) -> Result<Self, OpsError> {
        let provider = ProviderKey::new(registration.provider())?;
        let operation = OperationKey::new(registration.operation())?;
        let target = serde_json::to_value(target).map_err(|source| OpsError::InvalidTarget {
            provider: provider.as_str().to_string(),
            operation: operation.as_str().to_string(),
            message: source.to_string(),
        })?;
        Ok(Self {
            provider,
            operation,
            target,
        })
    }

    /// Selected provider identity.
    pub fn provider(&self) -> &ProviderKey {
        &self.provider
    }

    /// Selected provider-owned operation identity.
    pub fn operation(&self) -> &OperationKey {
        &self.operation
    }

    /// Borrow the serialized provider target admitted by binding validation.
    pub fn target(&self) -> &serde_json::Value {
        &self.target
    }
}

/// Provider-neutral operation class selected by the operator-facing command.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationKind {
    /// Fetch or stream service logs.
    Logs,
    /// Resolve provider-owned access to one or more declared service ports.
    PortForward,
}

/// One provider request plus invocation-local information owned by the common layer.
#[derive(Debug, Clone, PartialEq)]
pub struct OperationInvocation {
    kind: OperationKind,
    request: OperationRequest,
    local_port: Option<u16>,
}

impl OperationInvocation {
    /// Operation class whose result the provider must return.
    pub fn kind(&self) -> OperationKind {
        self.kind
    }

    /// Borrow the unchanged provider target envelope.
    pub fn request(&self) -> &OperationRequest {
        &self.request
    }

    /// Optional local listener selected for this invocation only.
    pub fn local_port(&self) -> Option<u16> {
        self.local_port
    }
}

/// Provider-observed endpoint exposed to an operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalEndpoint {
    /// Local bind host selected by provider mechanics.
    pub local_host: String,
    /// Local listener port, including an operator override when supplied.
    pub local_port: u16,
    /// Provider-observed remote host or service identity.
    pub remote_host: String,
    /// Provider-observed remote/container port.
    pub remote_port: u16,
    /// Provider-observed transport protocol.
    pub protocol: String,
    /// Provider-owned access-mode identity preserved for presentation/session launch.
    pub access_mode: String,
}

/// Process launch request assembled by provider mechanics but owned at runtime by `tkr`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPlan {
    /// Executable selected by the provider implementation.
    pub program: String,
    /// Exact non-secret arguments selected by the provider implementation.
    pub arguments: Vec<String>,
}

/// Provider-owned result returned through the common operation boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OperationOutput {
    /// Complete fetched log lines; a streaming executor may emit these incrementally at the shell edge.
    Logs(Vec<String>),
    /// Resolved endpoint and optional provider-constructed session command.
    PortForward {
        /// Provider-observed endpoint.
        endpoint: OperationalEndpoint,
        /// `None` for an already-published endpoint; otherwise a process plan whose lifecycle `tkr` owns.
        session: Option<SessionPlan>,
    },
}

/// Type-erased provider-owned target validation and live operation execution.
#[async_trait]
pub trait ProviderOperation: Debug + Send + Sync {
    /// Stable provider identity matching [`OperationRequest::provider`].
    fn provider(&self) -> &str;

    /// Stable operation identity matching [`OperationRequest::operation`].
    fn operation(&self) -> &str;

    /// Decode and validate one platform-declared target without provider I/O.
    fn validate_target(&self, target: &serde_json::Value) -> Result<(), OpsError>;

    /// Perform provider discovery or retrieval without spawning an operator session process.
    async fn execute(
        &self,
        invocation: &OperationInvocation,
        context: &tokeira_iac::ProvisionContext,
    ) -> Result<OperationOutput, OpsError>;
}

/// Log and port declarations for one logical platform service.
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceOps {
    /// Logical identity from the same platform service catalog.
    pub logical_service: String,
    /// Provider request used for logs, when supported.
    pub logs: Option<OperationRequest>,
    /// Provider requests used for port access in platform declaration order.
    pub ports: Vec<OperationRequest>,
}

/// Immutable operation inventory assembled by a platform binding.
#[derive(Debug, Clone, Default)]
pub struct PlatformOps {
    services: Vec<ServiceOps>,
}

impl PlatformOps {
    /// Construct a pure operation inventory.
    pub fn new(services: Vec<ServiceOps>) -> Self {
        Self { services }
    }

    /// Borrow operation declarations in platform order.
    pub fn services(&self) -> &[ServiceOps] {
        &self.services
    }

    /// Return the deterministic duplicate-free service inventory for one operation class.
    pub fn supported(&self, kind: OperationKind) -> Vec<&str> {
        let mut supported = self
            .services
            .iter()
            .filter(|service| match kind {
                OperationKind::Logs => service.logs.is_some(),
                OperationKind::PortForward => !service.ports.is_empty(),
            })
            .map(|service| service.logical_service.as_str())
            .collect::<Vec<_>>();
        supported.sort_unstable();
        supported
    }

    /// Resolve one logical log request or return the complete supported inventory.
    pub fn logs(&self, logical_service: &str) -> Result<OperationInvocation, OpsError> {
        let request = self
            .services
            .iter()
            .find(|service| service.logical_service == logical_service)
            .and_then(|service| service.logs.clone())
            .ok_or_else(|| self.unknown_service(OperationKind::Logs, logical_service))?;
        Ok(OperationInvocation {
            kind: OperationKind::Logs,
            request,
            local_port: None,
        })
    }

    /// Resolve port requests while keeping the override outside every provider target.
    pub fn ports(
        &self,
        logical_service: &str,
        local_port: Option<u16>,
    ) -> Result<Vec<OperationInvocation>, OpsError> {
        if local_port == Some(0) {
            return Err(OpsError::InvalidLocalPort(0));
        }
        let requests = self
            .services
            .iter()
            .find(|service| service.logical_service == logical_service)
            .filter(|service| !service.ports.is_empty())
            .ok_or_else(|| self.unknown_service(OperationKind::PortForward, logical_service))?;
        Ok(requests
            .ports
            .iter()
            .cloned()
            .map(|request| OperationInvocation {
                kind: OperationKind::PortForward,
                request,
                local_port,
            })
            .collect())
    }

    pub(crate) fn validate<P: crate::binding::Platform>(
        &self,
        services: &BTreeSet<String>,
        providers: &crate::catalog::ProviderSet<P>,
    ) -> Result<(), BindingError> {
        let mut logical_services = BTreeSet::new();
        for service in &self.services {
            if service.logical_service.is_empty()
                || !logical_services.insert(service.logical_service.as_str())
            {
                return Err(BindingError::new(format!(
                    "duplicate or empty operations service `{}`",
                    service.logical_service
                )));
            }
            if !services.contains(&service.logical_service) {
                return Err(BindingError::new(format!(
                    "operations reference unknown service `{}`",
                    service.logical_service
                )));
            }
            if service.logs.is_none() && service.ports.is_empty() {
                return Err(BindingError::new(format!(
                    "operations service `{}` declares no supported operation",
                    service.logical_service
                )));
            }
            let mut requests = BTreeSet::new();
            for request in service.logs.iter().chain(&service.ports) {
                let target = serde_json::to_string(request.target()).map_err(|source| {
                    BindingError::new(format!(
                        "operation target for service `{}` cannot be canonicalized: {source}",
                        service.logical_service
                    ))
                })?;
                let identity = (
                    request.provider().as_str(),
                    request.operation().as_str(),
                    target,
                );
                if !requests.insert(identity) {
                    return Err(BindingError::new(format!(
                        "operations service `{}` contains a duplicate provider request",
                        service.logical_service
                    )));
                }
                let executor = providers
                    .operation(request.provider(), request.operation())
                    .ok_or_else(|| {
                        BindingError::new(format!(
                            "operations service `{}` references unknown provider operation `{}/{}`",
                            service.logical_service,
                            request.provider().as_str(),
                            request.operation().as_str()
                        ))
                    })?;
                executor
                    .validate_target(request.target())
                    .map_err(|error| {
                        BindingError::new(format!(
                            "operations service `{}` has an invalid provider target: {error}",
                            service.logical_service
                        ))
                    })?;
            }
        }
        Ok(())
    }

    fn unknown_service(&self, kind: OperationKind, logical_service: &str) -> OpsError {
        OpsError::UnknownService {
            kind,
            requested: logical_service.to_string(),
            supported: self
                .supported(kind)
                .into_iter()
                .map(str::to_string)
                .collect(),
        }
    }
}
