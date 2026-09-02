//! In-process operation retained for deployments created before their platform
//! migrated to generated bound provisioners.
//!
//! New ECS deployments are definition-bound; `from_id` still recognizes ECS
//! so existing directories carrying `deployment.toml` remain operable. Local
//! is the sole platform that still creates through this adapter.

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

    /// Select the in-process adapter allowed to create a new deployment.
    ///
    /// Recognition and creation are deliberately separate: dropping ECS from
    /// [`Self::from_id`] would strand existing deployments, while routing new
    /// ECS deployments here would bypass its recorded definition and remote
    /// state contract.
    pub(crate) fn creation_adapter(id: &PlatformId) -> Option<Self> {
        match id.as_str() {
            "local" => Some(Self::Local),
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
    fn ecs_template_remains_available_only_for_existing_deployments() {
        let toml = LegacyPlatform::Ecs.deployment_config(StorageKind::Dsql);
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("populated by `tkr image push`"));
        let _: EcsConfig = toml::from_str(&toml).expect("ECS template parses");
        let ecs = PlatformId::new("ecs").expect("platform id");
        assert_eq!(LegacyPlatform::from_id(&ecs), Some(LegacyPlatform::Ecs));
        assert_eq!(LegacyPlatform::creation_adapter(&ecs), None);
    }
}
