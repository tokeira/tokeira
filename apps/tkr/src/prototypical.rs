//! Template TOML written at `tkr deployment create` time.
//!
//! Each platform contributes its own prototypical `deployment.toml` (the
//! platform-specific config) and `tokeirad.toml` (the server-side
//! [`tokeira_config::TokeiraConfig`]). The server-config branch also
//! round-trips the TOML through the parser as a compile-time safety net:
//! if a platform ships a template that no longer parses, `tkr deployment
//! create` will fail here with a clear error rather than minutes later
//! when `tokeirad` tries to start.

use anyhow::Result;
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_ecs_deployment::EcsDeployment;
use tokeira_local_deployment::LocalDeployment;
use tokeira_orchestrator::{PlatformConfig, PlatformKind, StorageKind};
use toml_edit::{DocumentMut, value};

pub fn deployment_config(
    platform: PlatformKind,
    storage: StorageKind,
    region: Option<&str>,
) -> Result<String> {
    let toml = match platform {
        PlatformKind::Local => LocalDeployment::prototypical_config(storage),
        PlatformKind::Compose => ComposeDeployment::prototypical_config(storage),
        PlatformKind::Ecs => EcsDeployment::prototypical_config(storage),
    };
    if platform == PlatformKind::Compose && storage == StorageKind::Dsql {
        patch_dsql_region(toml, region.unwrap_or("us-east-1"))
    } else {
        Ok(toml)
    }
}

pub fn server_config(
    platform: PlatformKind,
    storage: StorageKind,
    region: Option<&str>,
) -> Result<String> {
    let toml = match platform {
        PlatformKind::Local => {
            let toml = LocalDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            toml
        }
        PlatformKind::Compose => {
            let toml = ComposeDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            toml
        }
        PlatformKind::Ecs => {
            let toml = EcsDeployment::prototypical_server_config(storage);
            let _: tokeira_config::TokeiraConfig = toml::from_str(&toml)?;
            toml
        }
    };
    if storage == StorageKind::Dsql {
        patch_server_dsql_region(toml, region.unwrap_or("us-east-1"))
    } else {
        Ok(toml)
    }
}

fn patch_dsql_region(toml: String, region: &str) -> Result<String> {
    let mut document = toml.parse::<DocumentMut>()?;
    document["dsql"]["region"] = value(region);
    Ok(document.to_string())
}

fn patch_server_dsql_region(toml: String, region: &str) -> Result<String> {
    let mut document = toml.parse::<DocumentMut>()?;
    document["infrastructure"]["region"] = value(region);
    if let Some(dsql) = document
        .get_mut("infrastructure")
        .and_then(|i| i.get_mut("dsql"))
    {
        dsql["region"] = value(region);
    }
    Ok(document.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_compose_deployment::ComposeConfig;
    use tokeira_ecs_deployment::EcsConfig;

    #[test]
    fn compose_prototypical_config_contains_image_defaults_and_comments() {
        let toml = deployment_config(PlatformKind::Compose, StorageKind::InMemory, None).unwrap();
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("aws_cli_image = \"public.ecr.aws/aws-cli/aws-cli:latest\""));
        assert!(toml.contains("busybox_image = \"public.ecr.aws/docker/library/busybox:latest\""));
        assert!(toml.contains("run `tkr image build`"));
        assert!(toml.contains("populated by `tkr image mirror`"));

        let _: ComposeConfig = toml::from_str(&toml).unwrap();
    }

    #[test]
    fn ecs_prototypical_config_contains_image_defaults_and_comments() {
        let toml = deployment_config(PlatformKind::Ecs, StorageKind::Dsql, None).unwrap();
        assert!(toml.contains("image = \"tokeirad:latest\""));
        assert!(toml.contains("aws_cli_image = \"amazon/aws-cli:latest\""));
        assert!(toml.contains("busybox_image = \"public.ecr.aws/docker/library/busybox:latest\""));
        assert!(toml.contains("populated by `tkr image push`"));
        assert!(toml.contains("populated by `tkr image mirror`"));

        let _: EcsConfig = toml::from_str(&toml).unwrap();
    }
}
