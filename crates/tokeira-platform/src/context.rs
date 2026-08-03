//! Universal invocation facts from which a platform constructs its typed context.

use std::path::PathBuf;

/// Shell-admitted facts available to every platform invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationContext {
    /// Stable operator-visible deployment identity.
    pub deployment_id: String,
    /// Stable UUID recorded for the deployment.
    pub deployment_uuid: uuid::Uuid,
    /// Host deployment root.
    pub deployment_dir: PathBuf,
}
