//! Persistent CLI-side metadata for a deployment.
//!
//! Kept separate from the platform-specific `deployment.toml` because this
//! JSON file records *who* the deployment is (`id`, `name`, `platform`,
//! `storage`) and *what state it is in from the CLI's perspective* rather
//! than the declarative shape of its resources. The engine layer never
//! reads or writes this file; only the operator CLI does.

use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{PlatformKind, PlatformLaunchClass, StorageKind};
use tokeira_provisioner::RecordedDefinition;
use uuid::Uuid;

use crate::deployment_dir::METADATA_JSON;

/// Lifecycle state as observed by the CLI.
///
/// `Created` means infra has been provisioned but services haven't been
/// started; `Running` means `tkr deploy apply` completed successfully;
/// `Stopped` means a scale-down brought replicas to zero. Local-platform
/// deployments additionally reconcile this against the presence of a
/// `tokeirad.pid` file via [`crate::process::local_process_status`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DeploymentStatus {
    Created,
    Running,
    Stopped,
}

/// Stable JSON shape persisted at `metadata.json`. Changes to the field
/// set are a breaking change for existing deployments on disk; prefer
/// additive, optional fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DeploymentMetadata {
    pub name: String,
    pub id: Uuid,
    pub platform: PlatformKind,
    /// Descriptor-selected launch mechanism. Absent only on deployments
    /// created before launch-class metadata was introduced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_class: Option<PlatformLaunchClass>,
    /// Format and safe live source path for a bound-provisioner deployment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<RecordedDefinition>,
    pub storage: StorageKind,
    pub status: DeploymentStatus,
    pub created_at: String,
    pub updated_at: String,
}

pub(crate) fn read(path: &Path) -> Result<DeploymentMetadata> {
    let metadata_path = path.join(METADATA_JSON);
    let bytes = fs::read_to_string(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    Ok(serde_json::from_str(&bytes)?)
}

pub(crate) fn write(path: &Path, metadata: &DeploymentMetadata) -> Result<()> {
    fs::write(
        path.join(METADATA_JSON),
        serde_json::to_string_pretty(metadata)?,
    )?;
    Ok(())
}
