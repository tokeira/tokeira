//! Pure platform operation declarations and provider-executor selection.

use std::collections::BTreeSet;

use crate::error::BindingError;

/// Provider-neutral operation supported for a logical platform service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationKind {
    /// Stream or fetch service logs.
    Logs,
    /// Establish access to one declared service port.
    PortForward { port: u16 },
}

/// One pure operation declaration; it contains no client or execution callback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationDeclaration {
    /// Logical service from the same platform service catalog.
    pub service: String,
    /// Provider-neutral requested operation.
    pub kind: OperationKind,
    /// Selected provider-owned executor key.
    pub executor: String,
}

/// Immutable operation inventory assembled by a platform binding.
#[derive(Debug, Clone, Default)]
pub struct PlatformOps {
    declarations: Vec<OperationDeclaration>,
}

impl PlatformOps {
    /// Construct a pure operation inventory.
    pub fn new(declarations: Vec<OperationDeclaration>) -> Self {
        Self { declarations }
    }

    /// Borrow operation declarations in platform order.
    pub fn declarations(&self) -> &[OperationDeclaration] {
        &self.declarations
    }

    pub(crate) fn validate(&self, services: &BTreeSet<String>) -> Result<(), BindingError> {
        let mut identities = BTreeSet::new();
        for declaration in &self.declarations {
            if !services.contains(&declaration.service) {
                return Err(BindingError::new(format!(
                    "operation references unknown service `{}`",
                    declaration.service
                )));
            }
            let port = match declaration.kind {
                OperationKind::Logs => None,
                OperationKind::PortForward { port } => Some(port),
            };
            if !identities.insert((declaration.service.as_str(), port)) {
                return Err(BindingError::new(format!(
                    "duplicate operation for service `{}` and port {port:?}",
                    declaration.service
                )));
            }
        }
        Ok(())
    }
}
