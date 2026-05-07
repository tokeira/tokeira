//! Local Docker Compose provider for the orchestration framework.
//!
//! This crate is a concrete specialization of the generic IaC and runtime
//! traits for local development. [`ComposeService`] implements
//! [`tokeira_iac::Resource`] so infrastructure apply can create/update/delete a
//! local service. [`ComposePlatform`] implements
//! [`tokeira_deploy_engine::Platform`] so runtime apply can reconcile service
//! manifests against Docker.
//!
//! ## Drift detection
//!
//! The engine calls `describe()` during `refresh_state` to get live Docker
//! state via `docker inspect`. The reconstructed `ComposeService` includes
//! image, ports, volumes, environment, and healthcheck — drift in any of
//! these fields triggers an update on the next plan/apply.
//!
//! `depends_on` is a compose-file concept with no Docker runtime equivalent
//! and cannot be reconstructed from inspect. If the desired config changes
//! `depends_on`, `diff()` detects the change because the desired manifest
//! includes it and the live manifest does not.
//!
//! A deployment that uses this crate must register a [`ComposePlatform`] on the
//! infrastructure [`tokeira_iac::ProvisionContext`] before applying modules that
//! return compose resources. The same platform can be passed to the deploy
//! facade for runtime service apply.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use bollard::{
    Docker,
    container::{
        Config as ContainerConfig, CreateContainerOptions, ListContainersOptions, LogsOptions,
        RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
    },
    models::{ContainerInspectResponse, HostConfig, PortBinding},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;

#[derive(Debug, Error)]
pub enum ComposeError {
    #[error("docker is not available at {socket_path}")]
    DockerNotAvailable { socket_path: String },
    #[error("container operation failed for '{container}': {source}")]
    ContainerFailed {
        container: String,
        #[source]
        source: anyhow::Error,
    },
    #[error("compose file operation failed: {0}")]
    YamlError(#[from] serde_yaml::Error),
    #[error("compose file I/O failed at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("docker operation failed: {0}")]
    DockerIo(#[source] bollard::errors::Error),
    #[error("local image '{image}' is missing; {remediation}")]
    LocalBuildMissing { image: String, remediation: String },
}

impl From<ComposeError> for iac::IacError {
    fn from(value: ComposeError) -> Self {
        iac::IacError::Other(anyhow::anyhow!(value))
    }
}

impl From<ComposeError> for deploy_engine::DeployError {
    fn from(value: ComposeError) -> Self {
        deploy_engine::DeployError::Other(anyhow::anyhow!(value))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Healthcheck {
    pub test: Vec<String>,
    pub interval: Option<String>,
    pub timeout: Option<String>,
    pub retries: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ComposeService {
    /// Stable service name. Used as the compose service key, Docker label, and
    /// framework resource ID suffix.
    pub name: String,
    /// Container image reference to run.
    pub image: String,
    /// Port mappings in `host:container` form.
    pub ports: Vec<String>,
    /// Volume mappings in Docker syntax.
    pub volumes: Vec<String>,
    /// Environment variables passed to the container.
    pub environment: HashMap<String, String>,
    /// Names of compose services that should exist before this service.
    pub depends_on: Vec<String>,
    /// Optional Docker healthcheck definition.
    pub healthcheck: Option<Healthcheck>,
}

impl ComposeService {
    /// Convert the service into the provider-agnostic manifest shape used by
    /// the runtime deploy engine.
    pub fn to_manifest(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("compose service serializes")
    }

    fn container_name(&self, project_name: &str) -> String {
        format!("{project_name}_{}", self.name)
    }
}

#[async_trait]
impl iac::Resource for ComposeService {
    fn resource_type(&self) -> iac::ResourceType {
        iac::ResourceType::new("compose_service")
    }

    fn resource_id(&self) -> iac::ResourceId {
        iac::ResourceId(format!("compose/{}", self.name))
    }

    fn dependencies(&self) -> Vec<iac::ResourceId> {
        self.depends_on
            .iter()
            .map(|dep| iac::ResourceId(format!("compose/{dep}")))
            .collect()
    }

    fn module(&self) -> &str {
        &self.name
    }

    async fn create(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        let platform = ctx.extension::<ComposePlatform>().ok_or_else(|| {
            iac::IacError::Other(anyhow::anyhow!("ComposePlatform not registered"))
        })?;
        platform.reconcile_service(self).await?;
        Ok(iac::ResourceState {
            resource_type: iac::ResourceType::new("compose_service"),
            physical_id: self.name.clone(),
            properties: self.to_manifest(),
            dependencies: self.dependencies(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.name.clone(),
        })
    }

    async fn update(
        &self,
        _current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::ResourceState, iac::IacError> {
        let platform = ctx.extension::<ComposePlatform>().ok_or_else(|| {
            iac::IacError::Other(anyhow::anyhow!("ComposePlatform not registered"))
        })?;
        platform.reconcile_service(self).await?;
        Ok(iac::ResourceState {
            resource_type: iac::ResourceType::new("compose_service"),
            physical_id: self.name.clone(),
            properties: self.to_manifest(),
            dependencies: self.dependencies(),
            created_at: String::new(),
            updated_at: String::new(),
            module: self.name.clone(),
        })
    }

    async fn delete(
        &self,
        _current: &iac::ResourceState,
        ctx: &iac::ProvisionContext,
    ) -> Result<(), iac::IacError> {
        let platform = ctx.extension::<ComposePlatform>().ok_or_else(|| {
            iac::IacError::Other(anyhow::anyhow!("ComposePlatform not registered"))
        })?;
        platform.delete_service(&self.name).await?;
        Ok(())
    }

    async fn describe(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<Option<iac::ResourceState>, iac::IacError> {
        let platform = ctx.extension::<ComposePlatform>().ok_or_else(|| {
            iac::IacError::Other(anyhow::anyhow!("ComposePlatform not registered"))
        })?;
        match platform.running_service(&self.name).await? {
            Some(service) => Ok(Some(iac::ResourceState {
                resource_type: iac::ResourceType::new("compose_service"),
                physical_id: service.name.clone(),
                properties: service.to_manifest(),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: self.name.clone(),
            })),
            None => Ok(None),
        }
    }

    fn diff(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        // Compare desired config against persisted properties
        let current_manifest = &current.properties;
        let desired_manifest = self.to_manifest();
        if current_manifest == &desired_manifest {
            iac::InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            iac::InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details: "service configuration changed".into(),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ComposePlatform {
    docker: Docker,
    compose_file: PathBuf,
    project_name: String,
    socket_path: String,
}

impl ComposePlatform {
    /// Connect to Docker using the local default socket and write compose state
    /// to the supplied file.
    pub fn connect(
        compose_file: impl Into<PathBuf>,
        project_name: impl Into<String>,
    ) -> Result<Self, ComposeError> {
        let docker = Docker::connect_with_local_defaults().map_err(|_| {
            ComposeError::DockerNotAvailable {
                socket_path: "local-default".into(),
            }
        })?;
        Ok(Self {
            docker,
            compose_file: compose_file.into(),
            project_name: project_name.into(),
            socket_path: "local-default".into(),
        })
    }

    /// Connect to Docker through an explicit Unix socket path.
    pub fn connect_with_socket(
        compose_file: impl Into<PathBuf>,
        project_name: impl Into<String>,
        socket_path: impl Into<String>,
    ) -> Result<Self, ComposeError> {
        let socket_path = socket_path.into();
        let docker = Docker::connect_with_unix(&socket_path, 120, bollard::API_DEFAULT_VERSION)
            .map_err(|_| ComposeError::DockerNotAvailable {
                socket_path: socket_path.clone(),
            })?;
        Ok(Self {
            docker,
            compose_file: compose_file.into(),
            project_name: project_name.into(),
            socket_path,
        })
    }

    /// Verify that Docker is reachable before performing an operation.
    pub async fn ensure_reachable(&self) -> Result<(), ComposeError> {
        self.docker
            .version()
            .await
            .map(|_| ())
            .map_err(|error| ComposeError::DockerNotAvailable {
                socket_path: format!("{} ({error})", self.socket_path),
            })
    }

    pub fn docker_client(&self) -> Docker {
        self.docker.clone()
    }

    /// Reconcile one compose service by updating the compose file and replacing
    /// the corresponding local container.
    pub async fn reconcile_service(&self, service: &ComposeService) -> Result<(), ComposeError> {
        self.ensure_reachable().await?;
        let mut state = self.load_compose_state()?;
        state.services.insert(service.name.clone(), service.clone());
        self.save_compose_state(&state)?;

        let container_name = service.container_name(&self.project_name);
        let _ = self
            .docker
            .stop_container(&container_name, Some(StopContainerOptions { t: 1 }))
            .await;
        let _ = self
            .docker
            .remove_container(
                &container_name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;

        let config = container_config(service, &self.project_name);
        self.docker
            .create_container(
                Some(CreateContainerOptions {
                    name: container_name.clone(),
                    platform: None,
                }),
                config,
            )
            .await
            .map_err(|error| ComposeError::ContainerFailed {
                container: container_name.clone(),
                source: anyhow::anyhow!(error),
            })?;
        self.docker
            .start_container(&container_name, None::<StartContainerOptions<String>>)
            .await
            .map_err(|error| ComposeError::ContainerFailed {
                container: container_name,
                source: anyhow::anyhow!(error),
            })?;
        Ok(())
    }

    /// Remove one compose service and its corresponding container.
    ///
    /// This is intentionally service-scoped. It does not run project-wide
    /// `docker compose down`, because infrastructure delete may target a single
    /// resource while leaving the rest of the local stack intact.
    pub async fn delete_service(&self, service: &str) -> Result<(), ComposeError> {
        self.ensure_reachable().await?;
        let mut state = self.load_compose_state()?;
        state.services.remove(service);
        self.save_compose_state(&state)?;

        let container_name = format!("{}_{}", self.project_name, service);
        let _ = self
            .docker
            .stop_container(&container_name, Some(StopContainerOptions { t: 1 }))
            .await;
        let _ = self
            .docker
            .remove_container(
                &container_name,
                Some(RemoveContainerOptions {
                    force: true,
                    ..Default::default()
                }),
            )
            .await;
        Ok(())
    }

    /// Read the currently known Docker state for one service.
    ///
    /// Reconstructs a `ComposeService` from `docker inspect`. The following
    /// fields are extracted and will participate in drift detection:
    /// - `image`
    /// - `ports` (from host_config.port_bindings)
    /// - `volumes` (from host_config.binds)
    /// - `environment` (from config.env)
    /// - `healthcheck` (from config.healthcheck)
    ///
    /// `depends_on` cannot be reconstructed — it is a compose-file concept
    /// with no Docker runtime equivalent. Changes to `depends_on` in the
    /// desired config will still trigger an update because the desired
    /// manifest includes it and the live manifest does not.
    pub async fn running_service(
        &self,
        service: &str,
    ) -> Result<Option<ComposeService>, ComposeError> {
        self.ensure_reachable().await?;
        let filters = HashMap::from([(
            "label".to_string(),
            vec![
                format!("com.docker.compose.service={service}"),
                format!("com.docker.compose.project={}", self.project_name),
            ],
        )]);
        let containers = self
            .docker
            .list_containers(Some(ListContainersOptions::<String> {
                all: true,
                filters,
                ..Default::default()
            }))
            .await
            .map_err(|error| ComposeError::ContainerFailed {
                container: service.to_string(),
                source: anyhow::anyhow!(error),
            })?;
        let Some(container) = containers.into_iter().next() else {
            return Ok(None);
        };
        let inspect = self
            .docker
            .inspect_container(
                &container.id.unwrap_or_default(),
                None::<bollard::container::InspectContainerOptions>,
            )
            .await
            .map_err(|error| ComposeError::ContainerFailed {
                container: service.to_string(),
                source: anyhow::anyhow!(error),
            })?;
        Ok(Some(service_from_inspect(service, &inspect)))
    }

    /// Return recent logs from the service container.
    pub async fn logs(&self, service: &str) -> Result<Vec<String>, ComposeError> {
        use futures_util::TryStreamExt;

        self.ensure_reachable().await?;
        let container_name = format!("{}_{}", self.project_name, service);
        let bytes = self
            .docker
            .logs(
                &container_name,
                Some(LogsOptions::<String> {
                    follow: false,
                    stdout: true,
                    stderr: true,
                    tail: "100".into(),
                    ..Default::default()
                }),
            )
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| ComposeError::ContainerFailed {
                container: container_name,
                source: anyhow::anyhow!(error),
            })?;
        Ok(bytes
            .into_iter()
            .map(|chunk| chunk.to_string())
            .collect::<Vec<_>>())
    }

    /// Resolve the local host/port pair for a service container port.
    pub async fn port_forward_target(
        &self,
        service: &str,
        port: u16,
    ) -> Result<Option<(String, u16)>, ComposeError> {
        let Some(service) = self.running_service(service).await? else {
            return Ok(None);
        };
        for mapping in service.ports {
            if let Some((host, container)) = parse_port_mapping(&mapping) {
                if container == port {
                    return Ok(Some(("127.0.0.1".into(), host)));
                }
            }
        }
        Ok(None)
    }

    /// Return every host/container port mapping for a running service.
    pub async fn port_mappings(
        &self,
        service: &str,
    ) -> Result<Vec<(String, u16, u16, String)>, ComposeError> {
        let Some(service) = self.running_service(service).await? else {
            return Ok(Vec::new());
        };
        Ok(service
            .ports
            .into_iter()
            .filter_map(|mapping| {
                parse_port_mapping(&mapping).map(|(host, container)| {
                    ("127.0.0.1".to_string(), host, container, "tcp".to_string())
                })
            })
            .collect())
    }

    /// Start numbered local replicas for a service.
    ///
    /// This is a direct Docker implementation used by local ops. It creates
    /// containers named `{project}_{service}-{index}` and records those entries
    /// in the generated compose file; it does not rely on `deploy.replicas`.
    pub async fn scale_service(
        &self,
        service: &ComposeService,
        replicas: u32,
    ) -> Result<(), ComposeError> {
        self.ensure_reachable().await?;
        for replica in 0..replicas {
            let instance = service.clone();
            let mut state = self.load_compose_state()?;
            state
                .services
                .insert(format!("{}-{replica}", instance.name), instance.clone());
            self.save_compose_state(&state)?;
            let container_name = format!("{}_{}-{replica}", self.project_name, service.name);
            let config = container_config(service, &self.project_name);
            self.docker
                .create_container(
                    Some(CreateContainerOptions {
                        name: container_name.clone(),
                        platform: None,
                    }),
                    config,
                )
                .await
                .map_err(|error| ComposeError::ContainerFailed {
                    container: container_name.clone(),
                    source: anyhow::anyhow!(error),
                })?;
            self.docker
                .start_container(&container_name, None::<StartContainerOptions<String>>)
                .await
                .map_err(|error| ComposeError::ContainerFailed {
                    container: container_name,
                    source: anyhow::anyhow!(error),
                })?;
        }
        Ok(())
    }

    fn load_compose_state(&self) -> Result<ComposeFile, ComposeError> {
        if !self.compose_file.exists() {
            return Ok(ComposeFile::default());
        }
        let contents =
            std::fs::read_to_string(&self.compose_file).map_err(|source| ComposeError::Io {
                path: self.compose_file.display().to_string(),
                source,
            })?;
        serde_yaml::from_str(&contents).map_err(ComposeError::YamlError)
    }

    fn save_compose_state(&self, state: &ComposeFile) -> Result<(), ComposeError> {
        if let Some(parent) = self.compose_file.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ComposeError::Io {
                path: parent.display().to_string(),
                source,
            })?;
        }
        let yaml = serde_yaml::to_string(state)?;
        std::fs::write(&self.compose_file, yaml).map_err(|source| ComposeError::Io {
            path: self.compose_file.display().to_string(),
            source,
        })
    }
}

#[async_trait]
impl deploy_engine::Platform for ComposePlatform {
    async fn apply_manifests(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<usize, deploy_engine::DeployError> {
        self.ensure_reachable().await?;
        let mut state = self.load_compose_state()?;
        let mut count = 0;
        for manifest in manifests {
            let service: ComposeService = serde_json::from_value(manifest.clone())
                .map_err(|error| deploy_engine::DeployError::Other(anyhow::anyhow!(error)))?;
            state.services.insert(service.name.clone(), service.clone());
            self.reconcile_service(&service).await?;
            count += 1;
        }
        self.save_compose_state(&state)?;
        Ok(count)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ComposeFile {
    #[serde(default = "compose_version")]
    version: String,
    #[serde(default)]
    services: HashMap<String, ComposeService>,
}

fn compose_version() -> String {
    "3.9".into()
}

fn parse_port_mapping(value: &str) -> Option<(u16, u16)> {
    let (host, container) = value.split_once(':')?;
    Some((host.parse().ok()?, container.parse().ok()?))
}

fn container_config(service: &ComposeService, project_name: &str) -> ContainerConfig<String> {
    let exposed_ports = if service.ports.is_empty() {
        None
    } else {
        Some(
            service
                .ports
                .iter()
                .filter_map(|mapping| parse_port_mapping(mapping))
                .map(|(_, container)| (format!("{container}/tcp"), HashMap::new()))
                .collect(),
        )
    };
    let port_bindings = if service.ports.is_empty() {
        None
    } else {
        let mut bindings = HashMap::new();
        for mapping in &service.ports {
            if let Some((host, container)) = parse_port_mapping(mapping) {
                bindings.insert(
                    format!("{container}/tcp"),
                    Some(vec![PortBinding {
                        host_ip: Some("0.0.0.0".into()),
                        host_port: Some(host.to_string()),
                    }]),
                );
            }
        }
        Some(bindings)
    };
    let labels = Some(HashMap::from([
        ("com.docker.compose.service".into(), service.name.clone()),
        (
            "com.docker.compose.project".into(),
            project_name.to_string(),
        ),
    ]));

    ContainerConfig {
        image: Some(service.image.clone()),
        env: if service.environment.is_empty() {
            None
        } else {
            Some(
                service
                    .environment
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect(),
            )
        },
        host_config: Some(HostConfig {
            binds: if service.volumes.is_empty() {
                None
            } else {
                Some(service.volumes.clone())
            },
            port_bindings,
            ..Default::default()
        }),
        labels,
        exposed_ports,
        ..Default::default()
    }
}

fn service_from_inspect(name: &str, inspect: &ContainerInspectResponse) -> ComposeService {
    let image = inspect
        .config
        .as_ref()
        .and_then(|config| config.image.clone())
        .unwrap_or_default();
    let ports = inspect
        .host_config
        .as_ref()
        .and_then(|host| host.port_bindings.as_ref())
        .map(|bindings| {
            bindings
                .iter()
                .flat_map(|(container, host_bindings)| {
                    host_bindings
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(move |binding| {
                            binding.host_port.map(|host| {
                                format!("{host}:{}", container.trim_end_matches("/tcp"))
                            })
                        })
                })
                .collect()
        })
        .unwrap_or_default();

    let environment = inspect
        .config
        .as_ref()
        .and_then(|config| config.env.as_ref())
        .map(|env_list| {
            env_list
                .iter()
                .filter_map(|entry| {
                    let (key, value) = entry.split_once('=')?;
                    Some((key.to_string(), value.to_string()))
                })
                .collect()
        })
        .unwrap_or_default();

    ComposeService {
        name: name.to_string(),
        image,
        ports,
        volumes: inspect
            .host_config
            .as_ref()
            .and_then(|host| host.binds.clone())
            .unwrap_or_default(),
        environment,
        // depends_on is a compose-file concept, not a Docker runtime concept.
        // It cannot be reconstructed from inspect. Drift in depends_on will
        // be detected because the desired side includes it and the live side
        // does not, causing diff() to report an update.
        depends_on: Vec::new(),
        healthcheck: inspect
            .config
            .as_ref()
            .and_then(|config| config.healthcheck.as_ref())
            .and_then(|hc| {
                let test = hc.test.clone().unwrap_or_default();
                if test.is_empty() {
                    return None;
                }
                Some(Healthcheck {
                    test,
                    interval: hc.interval.map(|ns| format!("{}s", ns / 1_000_000_000)),
                    timeout: hc.timeout.map(|ns| format!("{}s", ns / 1_000_000_000)),
                    retries: hc.retries.map(|r| r as u32),
                })
            }),
    }
}

pub fn compose_yaml_fragment(service: &ComposeService) -> Result<String, ComposeError> {
    let mut file = ComposeFile::default();
    file.services.insert(service.name.clone(), service.clone());
    serde_yaml::to_string(&file).map_err(ComposeError::YamlError)
}

pub fn service_from_manifest(value: serde_json::Value) -> Result<ComposeService, ComposeError> {
    serde_json::from_value(value).map_err(|error| ComposeError::ContainerFailed {
        container: "manifest".into(),
        source: anyhow::anyhow!(error),
    })
}

pub fn compose_file_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_identifier() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_-]{0,7}".prop_map(|s| s.to_string())
    }

    fn arb_compose_service() -> impl Strategy<Value = ComposeService> {
        (
            arb_identifier(),
            arb_identifier(),
            prop::collection::vec((1u16..=65535, 1u16..=65535), 0..4),
            prop::collection::vec(arb_identifier(), 0..3),
            prop::collection::vec((arb_identifier(), arb_identifier()), 0..4),
            prop::collection::vec(arb_identifier(), 0..3),
            prop::option::of((
                prop::collection::vec(arb_identifier(), 1..4),
                prop::option::of("[1-9][0-9]{0,2}s"),
                prop::option::of("[1-9][0-9]{0,2}s"),
                prop::option::of(0u32..10),
            )),
        )
            .prop_map(
                |(name, image_tag, ports, volumes, environment, depends_on, healthcheck)| {
                    ComposeService {
                        image: format!("example/{name}:{image_tag}"),
                        name: name.clone(),
                        ports: ports
                            .into_iter()
                            .map(|(host, container)| format!("{host}:{container}"))
                            .collect(),
                        volumes: volumes
                            .into_iter()
                            .map(|volume| format!("/tmp/{volume}:/data/{volume}"))
                            .collect(),
                        environment: environment.into_iter().collect(),
                        depends_on,
                        healthcheck: healthcheck.map(|(test, interval, timeout, retries)| {
                            Healthcheck {
                                test,
                                interval,
                                timeout,
                                retries,
                            }
                        }),
                    }
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn p11_compose_service_serialization_is_complete(service in arb_compose_service()) {
            let yaml = compose_yaml_fragment(&service).unwrap();
            let parsed: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
            let rendered = &parsed["services"][&service.name];
            prop_assert_eq!(rendered["image"].as_str(), Some(service.image.as_str()));

            let rendered_ports = rendered["ports"]
                .as_sequence()
                .map(|seq| {
                    seq.iter()
                        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            prop_assert_eq!(rendered_ports, service.ports);

            let rendered_volumes = rendered["volumes"]
                .as_sequence()
                .map(|seq| {
                    seq.iter()
                        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            prop_assert_eq!(rendered_volumes, service.volumes);

            let rendered_depends = rendered["depends_on"]
                .as_sequence()
                .map(|seq| {
                    seq.iter()
                        .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            prop_assert_eq!(rendered_depends, service.depends_on);

            for (key, value) in &service.environment {
                prop_assert_eq!(rendered["environment"][key].as_str(), Some(value.as_str()));
            }
            if let Some(healthcheck) = &service.healthcheck {
                let rendered_test = rendered["healthcheck"]["test"]
                    .as_sequence()
                    .map(|seq| seq.iter().filter_map(|value| value.as_str().map(ToOwned::to_owned)).collect::<Vec<_>>())
                    .unwrap_or_default();
                prop_assert_eq!(rendered_test, healthcheck.test.clone());
            }
        }

        #[test]
        fn p12_port_mapping_extraction_round_trips(host in 1u16..=65535, container in 1u16..=65535) {
            let mapping = format!("{host}:{container}");
            prop_assert_eq!(parse_port_mapping(&mapping), Some((host, container)));
        }
    }

    #[test]
    fn serializes_compose_yaml_with_all_fields() {
        let service = ComposeService {
            name: "grafana".into(),
            image: "grafana/grafana-oss:12.4.3".into(),
            ports: vec!["3000:3000".into()],
            volumes: vec!["/tmp/data:/var/lib/grafana".into()],
            environment: HashMap::from([("GF_SECURITY_ADMIN_PASSWORD".into(), "admin".into())]),
            depends_on: vec!["mimir".into()],
            healthcheck: Some(Healthcheck {
                test: vec!["CMD".into(), "curl".into(), "http://localhost:3000".into()],
                interval: Some("10s".into()),
                timeout: Some("3s".into()),
                retries: Some(3),
            }),
        };
        let yaml = compose_yaml_fragment(&service).unwrap();
        assert!(yaml.contains("grafana/grafana-oss:12.4.3"));
        assert!(yaml.contains("3000:3000"));
        assert!(yaml.contains("GF_SECURITY_ADMIN_PASSWORD"));
        assert!(yaml.contains("/tmp/data:/var/lib/grafana"));
    }

    #[test]
    fn invalid_socket_reports_docker_not_available() {
        let error = ComposePlatform::connect_with_socket(
            "/tmp/compose.yaml",
            "test",
            "/definitely/not/a/docker.sock",
        )
        .unwrap_err();
        assert!(matches!(error, ComposeError::DockerNotAvailable { .. }));
    }

    #[test]
    fn extracts_port_mapping() {
        assert_eq!(parse_port_mapping("3000:3000"), Some((3000, 3000)));
    }
}
