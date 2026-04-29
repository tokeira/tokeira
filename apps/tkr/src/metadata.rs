use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{PlatformKind, StorageKind};
use uuid::Uuid;

use crate::deployment_dir::METADATA_JSON;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DeploymentStatus {
    Created,
    Running,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentMetadata {
    pub name: String,
    pub id: Uuid,
    pub platform: PlatformKind,
    pub storage: StorageKind,
    pub status: DeploymentStatus,
    pub created_at: String,
    pub updated_at: String,
}

pub fn read(path: &Path) -> Result<DeploymentMetadata> {
    let metadata_path = path.join(METADATA_JSON);
    let bytes = fs::read_to_string(&metadata_path)
        .with_context(|| format!("failed to read {}", metadata_path.display()))?;
    Ok(serde_json::from_str(&bytes)?)
}

pub fn write(path: &Path, metadata: &DeploymentMetadata) -> Result<()> {
    fs::write(
        path.join(METADATA_JSON),
        serde_json::to_string_pretty(metadata)?,
    )?;
    Ok(())
}
