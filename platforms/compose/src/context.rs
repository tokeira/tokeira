//! Compose-specific runtime information supplied to deployment definitions.

use serde::Serialize;
use tokeira_platform::context::InvocationContext;

/// Author-visible immutable Compose context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ComposeContext {
    /// Docker Compose project and provider naming prefix.
    pub project_name: String,
}

impl ComposeContext {
    /// Construct from universal shell-admitted invocation facts.
    pub fn from_invocation(invocation: &InvocationContext) -> Self {
        Self {
            project_name: invocation.deployment_id.clone(),
        }
    }
}
