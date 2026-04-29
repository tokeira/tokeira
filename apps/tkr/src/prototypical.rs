use anyhow::Result;
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{PlatformConfig, PlatformKind, StorageKind};

pub fn deployment_config(platform: PlatformKind, storage: StorageKind) -> Result<String> {
    match platform {
        PlatformKind::Local => Ok(LocalDeployment::prototypical_config(storage)),
        PlatformKind::Compose => Ok(ComposeDeployment::prototypical_config(storage)),
    }
}

pub fn server_config(platform: PlatformKind, storage: StorageKind) -> Result<String> {
    match platform {
        PlatformKind::Local => {
            let toml = LocalDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            Ok(toml)
        }
        PlatformKind::Compose => {
            let toml = ComposeDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            Ok(toml)
        }
    }
}
