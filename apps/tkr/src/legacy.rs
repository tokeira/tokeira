//! In-process deployment creation retained for platforms not yet migrated to
//! generated bound provisioners.
//!
//! Compose is deliberately absent. Its source, config, and binary are selected
//! through the trusted catalog. This module exists only to avoid changing the
//! Local and ECS platform crates during the Compose remediation.

use anyhow::Result;
use tokeira_ecs_deployment::EcsDeployment;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{PlatformConfig, PlatformId, StorageKind};
use toml_edit::{DocumentMut, value};

/// Current in-process platform adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyPlatform {
    Local,
    Ecs,
}

impl LegacyPlatform {
    /// Recognize an id without extending the public platform vocabulary.
    pub(crate) fn from_id(id: &PlatformId) -> Option<Self> {
        match id.as_str() {
            "local" => Some(Self::Local),
            "ecs" => Some(Self::Ecs),
            _ => None,
        }
    }

    pub(crate) fn deployment_config(self, storage: StorageKind) -> String {
        match self {
            Self::Local => LocalDeployment::prototypical_config(storage),
            Self::Ecs => EcsDeployment::prototypical_config(storage),
        }
    }

    pub(crate) fn server_config(
        self,
        storage: StorageKind,
        region: Option<&str>,
    ) -> Result<String> {
        let toml = match self {
            Self::Local => LocalDeployment::prototypical_server_config(storage),
            Self::Ecs => EcsDeployment::prototypical_server_config(storage),
        };
        let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
        if storage == StorageKind::Dsql {
            patch_server_dsql_region(toml, region.unwrap_or("us-east-1"))
        } else {
            Ok(toml)
        }
    }
}

fn patch_server_dsql_region(toml: String, region: &str) -> Result<String> {
    let mut document = toml.parse::<DocumentMut>()?;
    document["infrastructure"]["region"] = value(region);
    if let Some(dsql) = document
        .get_mut("infrastructure")
        .and_then(|infrastructure| infrastructure.get_mut("dsql"))
    {
        dsql["region"] = value(region);
    }
    Ok(document.to_string())
}

#[cfg(test)]
mod tests {
    use tokeira_ecs_deployment::EcsConfig;

    use super::*;

    #[test]
    fn ecs_template_remains_owned_by_the_legacy_ecs_platform() {
        let toml = LegacyPlatform::Ecs.deployment_config(StorageKind::Dsql);
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("populated by `tkr image push`"));
        let _: EcsConfig = toml::from_str(&toml).expect("ECS template parses");
    }
}
