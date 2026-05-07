use anyhow::Result;
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_ecs_deployment::EcsDeployment;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{PlatformConfig, PlatformKind, StorageKind};

pub fn deployment_config(platform: PlatformKind, storage: StorageKind) -> Result<String> {
    match platform {
        PlatformKind::Local => Ok(LocalDeployment::prototypical_config(storage)),
        PlatformKind::Compose => Ok(ComposeDeployment::prototypical_config(storage)),
        PlatformKind::Ecs => Ok(EcsDeployment::prototypical_config(storage)),
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
        PlatformKind::Ecs => {
            let toml = EcsDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            Ok(toml)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_compose_deployment::ComposeConfig;
    use tokeira_ecs_deployment::EcsConfig;

    #[test]
    fn compose_prototypical_config_contains_image_defaults_and_comments() {
        let toml = deployment_config(PlatformKind::Compose, StorageKind::InMemory).unwrap();
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("aws_cli_image = \"public.ecr.aws/aws-cli/aws-cli:latest\""));
        assert!(toml.contains("busybox_image = \"public.ecr.aws/docker/library/busybox:latest\""));
        assert!(toml.contains("run `tkr image build`"));
        assert!(toml.contains("populated by `tkr image mirror`"));

        let _: ComposeConfig = toml::from_str(&toml).unwrap();
    }

    #[test]
    fn ecs_prototypical_config_contains_image_defaults_and_comments() {
        let toml = deployment_config(PlatformKind::Ecs, StorageKind::Dsql).unwrap();
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("aws_cli_image = \"public.ecr.aws/aws-cli/aws-cli:latest\""));
        assert!(toml.contains("busybox_image = \"public.ecr.aws/docker/library/busybox:latest\""));
        assert!(toml.contains("populated by `tkr image push`"));
        assert!(toml.contains("populated by `tkr image mirror`"));

        let _: EcsConfig = toml::from_str(&toml).unwrap();
    }
}
