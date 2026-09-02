//! In-process operation for the local development platform.
//!
//! Cloud platforms are definition-bound and never produce `deployment.toml`.

use anyhow::Result;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{PlatformConfig, PlatformId, StorageKind};
use toml_edit::{DocumentMut, value};

/// Current in-process platform adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyPlatform {
    Local,
}

impl LegacyPlatform {
    /// Recognize an id without extending the public platform vocabulary.
    pub(crate) fn from_id(id: &PlatformId) -> Option<Self> {
        match id.as_str() {
            "local" => Some(Self::Local),
            _ => None,
        }
    }

    pub(crate) fn deployment_config(self, storage: StorageKind) -> String {
        match self {
            Self::Local => LocalDeployment::prototypical_config(storage),
        }
    }

    pub(crate) fn server_config(
        self,
        storage: StorageKind,
        region: Option<&str>,
    ) -> Result<String> {
        let toml = match self {
            Self::Local => LocalDeployment::prototypical_server_config(storage),
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
    use super::*;

    #[test]
    fn cloud_platforms_have_no_in_process_adapter() {
        let ecs = PlatformId::new("ecs").expect("platform id");
        assert_eq!(LegacyPlatform::from_id(&ecs), None);
    }
}
