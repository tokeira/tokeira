//! Local Docker Compose provider for the orchestration framework.
//!
//! This crate is a concrete specialization of the runtime deployment traits
//! for local development. [`ComposeService`] implements
//! [`tokeira_deploy_engine::Service`], while [`ComposePlatform`] implements
//! [`tokeira_deploy_engine::Platform`] and reconciles those service manifests
//! against Docker. Infrastructure resources in [`kinds`] remain separate and
//! are owned only by the IaC engine.
//!
//! ## Drift detection
//!
//! The Compose implementation of
//! [`tokeira_deploy_engine::Platform::is_service_current`] reads live Docker
//! state during a service plan. The reconstructed service helpers include
//! image, ports, volumes, environment, and healthcheck so richer deploy-plane
//! drift comparison can remain provider-owned as the deploy engine grows that
//! reporting surface.
//!
//! `depends_on` is a compose-file concept with no Docker runtime equivalent —
//! it round-trips through the compose-conventional
//! `com.docker.compose.depends_on` label written at create and read back by
//! `describe`, so ordering drift is real drift (a container created without
//! it), never a reconstruction artifact.
//!
//! A deployment that uses this crate passes a [`ComposePlatform`] to the deploy
//! facade for runtime service apply.

mod docker_endpoint;
pub mod execution;
pub mod kinds;
pub mod ops;

/// The well-known identity of the deployment's rendered-config-content
/// resource — the fencing contract between the [`kinds::Service`] consumer
/// (which injects `TOKEIRA_CONFIG_DIGEST` from this dependency's content
/// identity) and whatever platform-owned resource renders the content and
/// declares itself under this id. The provider owns the contract; a platform
/// owns the implementation.
pub fn config_content_resource_id() -> iac::ResourceId {
    iac::ResourceId("compose/observability-config-files".into())
}

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::Pin,
};

use futures_util::Stream;

use async_trait::async_trait;
use bollard::{
    Docker,
    container::{
        Config as ContainerConfig, CreateContainerOptions, ListContainersOptions, LogsOptions,
        RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
    },
    image::CreateImageOptions,
    models::{ContainerInspectResponse, HostConfig, PortBinding},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;

use crate::docker_endpoint::DockerEndpoint;

/// The Compose resource namespace exposed to definition frontends.
///
/// This value contains authoring facts only. The Compose platform definition
/// separately integrates live operations, reachability, and the runtime
/// [`ComposePlatform`].
pub fn namespace() -> tokeira_platform::definition::Namespace {
    tokeira_platform::definition::Namespace {
        name: kinds::NAMESPACE,
        kinds: kinds::KINDS,
        defaults: Some(kinds::defaults),
        decode: kinds::decode,
    }
}

#[derive(Debug, Error)]
pub enum ComposeError {
    #[error("docker is not available at {socket_path}: {evidence}")]
    DockerNotAvailable {
        socket_path: String,
        /// The SDK's own error, verbatim — the evidence a platform issue
        /// carries unblended.
        evidence: String,
    },
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

/// The compose platform's typed issue for an unreachable Docker daemon —
/// the platform-owned half of `## Platform Issue` rendering
/// (operator-explanation `output-templates.md`, platform row): the fact and
/// any direction are declared here, by the layer that owns the Docker SDK;
/// the evidence passes through verbatim, never blended into another
/// sentence.
///
/// The direction table admits only error classes whose text establishes the
/// direction — absent is the honest default (umbrella D4). A
/// connection-refused error establishes that nothing accepted connections at
/// the socket path, not that the daemon is stopped. The provider-SDK error
/// audit extends this table class by class.
pub fn docker_unreachable_issue(socket_path: &str, evidence: &str) -> iac::PlatformIssue {
    // Both spellings of the same errno class: bollard/hyper say
    // "Connection refused"; other SDK stacks say "ECONNREFUSED".
    let refused = evidence.contains("ECONNREFUSED") || evidence.contains("Connection refused");
    iac::PlatformIssue {
        component: "Docker".to_string(),
        fact: "Unable to connect to Docker".to_string(),
        evidence: evidence.to_string(),
        direction: refused.then(|| {
            format!(
                "nothing accepted connections at `{socket_path}` - verify Docker is listening there"
            )
        }),
    }
}

impl From<ComposeError> for iac::IacError {
    fn from(value: ComposeError) -> Self {
        match value {
            ComposeError::DockerNotAvailable {
                socket_path,
                evidence,
            } => iac::IacError::PlatformIssue(docker_unreachable_issue(&socket_path, &evidence)),
            error => iac::IacError::Other(anyhow::anyhow!(error)),
        }
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

/// One environment entry without tuple-shaped author data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    /// Environment variable name.
    pub name: String,
    /// Environment variable value.
    pub value: String,
}

/// Platform-owned logical volume vocabulary. Host paths never appear here:
/// lowering to concrete bind strings happens when the platform talks to
/// Docker, so manifests and their digests stay free of host state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Volume {
    /// Persistent path beneath the deployment's local state root.
    State(StateVolume),
    /// Generated path beneath the deployment's configuration root.
    Config(ConfigVolume),
    /// Docker daemon socket.
    DockerSocket,
}

/// Persistent state mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateVolume {
    /// Logical state subpath.
    pub sub: String,
    /// Container mount target.
    pub at: String,
}

/// Generated configuration mount.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigVolume {
    /// Logical configuration subpath.
    pub sub: String,
    /// Container mount target.
    pub at: String,
}

/// The compose service resource: what manifests record and what the engine
/// executes. Its authored face is the separate [`kinds::Service`] kind,
/// which realizes this model directly. Fields are logical — no host path or
/// host environment ever enters this value, so manifests and their digests
/// are host-independent; lowering to concrete Docker shapes happens inside
/// [`ComposePlatform`] at apply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ComposeService {
    /// Stable service name from the graph's logical id. Used as the compose
    /// service key, Docker label, and framework resource ID suffix.
    pub name: String,
    /// Container image reference to run.
    pub image: String,
    /// Desired container count. Applied by reconcile: replicas beyond the
    /// first run as `<name>-<index>` containers.
    #[serde(default = "default_replicas")]
    pub replicas: u32,
    /// Published equal host/container ports.
    #[serde(default)]
    pub publish: Vec<u16>,
    /// Logical volumes; lowered to bind strings at apply.
    #[serde(default)]
    pub volumes: Vec<Volume>,
    /// Explicit environment entries.
    #[serde(default)]
    pub environment: Vec<Environment>,
    /// Names of compose services that should exist before this service.
    #[serde(default)]
    pub depends_on: Vec<String>,
    /// Optional Docker healthcheck definition.
    #[serde(default)]
    pub healthcheck: Option<Healthcheck>,
    /// Command to run in the container (overrides image CMD).
    #[serde(default)]
    pub command: Vec<String>,
    /// Mount the non-secret AWS runtime selectors for this region. The
    /// host's credential paths and profile are resolved at apply, never
    /// recorded.
    #[serde(default)]
    pub aws_region: Option<String>,
    /// Desired-content identity of the deployment's server-config node,
    /// when this service declared a dependency on it. In the manifest so a
    /// `tokeirad.toml` edit surfaces as a diff on this service.
    #[serde(default)]
    pub server_config_digest: Option<String>,
    /// Desired-content identity of the rendered config-files resource, when
    /// a `Config` volume couples this service to it. Same contract as
    /// [`server_config_digest`](Self::server_config_digest).
    #[serde(default)]
    pub config_digest: Option<String>,
    /// Owning logical module. Not part of the manifest: recovery answers
    /// module questions from the recorded state row, and a recovered value
    /// falls back to the service name.
    #[serde(skip)]
    pub module: String,
}

fn default_replicas() -> u32 {
    1
}

/// Incremental Docker log output for one service.
pub type LogStream = Pin<Box<dyn Stream<Item = Result<String, ComposeError>> + Send>>;

impl ComposeService {
    /// The resource's one word: engine resource type and author-visible
    /// name, stated once here.
    pub const TYPE: &'static str = "Service";

    /// Convert the service into the provider-agnostic manifest shape used by
    /// the runtime deploy engine.
    pub fn to_manifest(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("compose service serializes")
    }

    fn container_name(&self, project_name: &str) -> String {
        format!("{project_name}_{}", self.name)
    }
}

impl deploy_engine::Service for ComposeService {
    fn resource_type(&self) -> &'static str {
        Self::TYPE
    }

    fn validate_input(&self) -> Result<(), String> {
        if self.image.is_empty() {
            return Err("Compose service image cannot be empty".to_string());
        }
        if self.replicas == 0 {
            return Err("Compose service replicas must be greater than zero".to_string());
        }
        if self.publish.contains(&0) {
            return Err("Compose service published ports must be greater than zero".to_string());
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn module(&self) -> &str {
        if self.module.is_empty() {
            &self.name
        } else {
            &self.module
        }
    }

    fn dependencies(&self) -> Vec<&str> {
        self.depends_on.iter().map(String::as_str).collect()
    }

    fn manifests(
        &self,
        _ctx: &deploy_engine::ServiceContext,
    ) -> Result<Vec<serde_json::Value>, deploy_engine::RuntimeError> {
        Ok(vec![canonicalize_manifest(self.to_manifest())])
    }
}

/// Sort the set-valued manifest arrays so equality means set equality. The
/// desired side is authored order; the live side is Docker-map iteration
/// order — only the canonical form is comparable.
///
/// Public because canonical form must have exactly one owner: the diff
/// boundary here and any consumer comparing desired manifests against each
/// other (desired-snapshot paths) call this one function — two independently
/// maintained canonicalizations would drift and manufacture phantom diffs.
pub fn canonicalize_manifest(mut manifest: serde_json::Value) -> serde_json::Value {
    if let Some(object) = manifest.as_object_mut() {
        for key in ["publish", "volumes", "environment", "depends_on"] {
            if let Some(array) = object.get_mut(key).and_then(|v| v.as_array_mut()) {
                array.sort_by_cached_key(std::string::ToString::to_string);
            }
        }
    }
    manifest
}

/// Field-level evidence for the plan report: one [`iac::FieldDiff`] per
/// differing top-level manifest key, values rendered compactly. This is what
/// `--detail` prints — a bare "configuration changed" is what let the
/// depends_on/port-order phantoms hide for a day.
#[cfg(test)]
fn manifest_field_diffs(
    current: &serde_json::Value,
    desired: &serde_json::Value,
) -> Vec<iac::FieldDiff> {
    let render = |value: Option<&serde_json::Value>| -> Option<String> {
        value.map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
    };
    let empty = serde_json::Map::new();
    let (current_map, desired_map) = (
        current.as_object().unwrap_or(&empty),
        desired.as_object().unwrap_or(&empty),
    );
    let mut fields: Vec<&String> = current_map.keys().chain(desired_map.keys()).collect();
    fields.sort();
    fields.dedup();
    fields
        .into_iter()
        .filter(|field| current_map.get(*field) != desired_map.get(*field))
        .map(|field| iac::FieldDiff {
            field: field.clone(),
            before: render(current_map.get(field)),
            after: render(desired_map.get(field)),
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct ComposePlatform {
    docker: Docker,
    compose_file: PathBuf,
    /// Deployment root: the anchor lowering resolves logical volumes
    /// against. Empty on the ledger-free ops handle, whose paths never
    /// lower.
    deployment_dir: PathBuf,
    project_name: String,
    socket_path: String,
}

impl ComposePlatform {
    /// Connect to Docker using the local default socket and write compose state
    /// to the supplied file.
    pub fn connect(
        compose_file: impl Into<PathBuf>,
        deployment_dir: impl Into<PathBuf>,
        project_name: impl Into<String>,
    ) -> Result<Self, ComposeError> {
        let endpoint =
            DockerEndpoint::resolve().map_err(|evidence| ComposeError::DockerNotAvailable {
                socket_path: "Docker context".into(),
                evidence,
            })?;
        let docker = endpoint
            .connect()
            .map_err(|error| ComposeError::DockerNotAvailable {
                socket_path: endpoint.label().to_string(),
                evidence: error.to_string(),
            })?;
        Ok(Self {
            docker,
            compose_file: compose_file.into(),
            deployment_dir: deployment_dir.into(),
            project_name: project_name.into(),
            socket_path: endpoint.label().to_string(),
        })
    }

    /// Ledger-free ops handle: Docker plus the project scope, for live
    /// questions (logs, port mappings, running containers). The compose-file
    /// path is empty by construction — the ops paths never touch it, and
    /// reconcile/scale/remove must never be called on this handle.
    pub fn ops(project_name: impl Into<String>) -> Result<Self, ComposeError> {
        Self::connect(
            std::path::PathBuf::new(),
            std::path::PathBuf::new(),
            project_name,
        )
    }

    /// Connect to Docker through an explicit Unix socket path.
    pub fn connect_with_socket(
        compose_file: impl Into<PathBuf>,
        deployment_dir: impl Into<PathBuf>,
        project_name: impl Into<String>,
        socket_path: impl Into<String>,
    ) -> Result<Self, ComposeError> {
        let socket_path = socket_path.into();
        let docker = Docker::connect_with_unix(&socket_path, 120, bollard::API_DEFAULT_VERSION)
            .map_err(|error| ComposeError::DockerNotAvailable {
                socket_path: socket_path.clone(),
                evidence: error.to_string(),
            })?;
        Ok(Self {
            docker,
            compose_file: compose_file.into(),
            deployment_dir: deployment_dir.into(),
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
                socket_path: self.socket_path.clone(),
                evidence: error.to_string(),
            })
    }

    pub fn docker_client(&self) -> Docker {
        self.docker.clone()
    }

    /// The project-scoped Docker network name. All services in this project
    /// are attached to this network so they can resolve each other by name.
    fn network_name(&self) -> String {
        format!("{}_default", self.project_name)
    }

    /// Ensure the project network exists, creating it if necessary.
    async fn ensure_network(&self) -> Result<(), ComposeError> {
        use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};

        let name = self.network_name();
        match self
            .docker
            .inspect_network(&name, None::<InspectNetworkOptions<String>>)
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => {
                self.docker
                    .create_network(CreateNetworkOptions {
                        name: name.clone(),
                        driver: "bridge".into(),
                        ..Default::default()
                    })
                    .await
                    .map_err(|error| ComposeError::ContainerFailed {
                        container: name,
                        source: anyhow::anyhow!(error),
                    })?;
                Ok(())
            }
        }
    }

    /// Returns the container's image digest if it differs from the local image's
    /// current digest for the same tag. This detects rebuilt images behind the
    /// same tag (e.g., `app:latest` rebuilt locally).
    ///
    /// Returns `None` if the image is current, or if either lookup fails (in
    /// which case we fall back to tag-only comparison).
    pub async fn container_image_stale(
        &self,
        service_name: &str,
        image_tag: &str,
    ) -> Option<String> {
        let container_name = format!("{}_{}", self.project_name, service_name);
        let inspect = self
            .docker
            .inspect_container(
                &container_name,
                None::<bollard::container::InspectContainerOptions>,
            )
            .await
            .ok()?;
        // The container's `image` field is the sha256 digest of the image it
        // was created from.
        let container_image_id = inspect.image.as_deref()?;

        // Resolve the current local image ID for the same tag.
        let local_image = self.docker.inspect_image(image_tag).await.ok()?;
        let local_image_id = local_image.id.as_deref()?;

        if container_image_id != local_image_id {
            // Return the stale digest so diff() sees a mismatch against the
            // desired tag-only image field.
            Some(format!(
                "{image_tag}@stale:{}",
                &container_image_id[..19.min(container_image_id.len())]
            ))
        } else {
            None
        }
    }

    /// Reconcile one compose service by updating the compose file and
    /// replacing its local containers. The authored `replicas` count is
    /// honoured here: the first container runs as `{project}_{name}`,
    /// further replicas as `{project}_{name}-{index}`.
    pub async fn reconcile_service(&self, service: &ComposeService) -> Result<(), ComposeError> {
        self.ensure_reachable().await?;
        let mut state = self.load_compose_state()?;
        state.services.insert(service.name.clone(), service.clone());
        self.save_compose_state(&state)?;

        // Ensure the project network exists so containers can resolve each other by name
        self.ensure_network().await?;

        // Lowering happens here, at the Docker boundary: host paths and host
        // environment enter the container config and nothing else.
        let config = self.lower(service);

        // Pull the image if not present locally
        let image_ref = &service.image;
        let (image_name, image_tag) = image_ref
            .rsplit_once(':')
            .unwrap_or((image_ref.as_str(), "latest"));
        let mut pull_stream = self.docker.create_image(
            Some(CreateImageOptions {
                from_image: image_name.to_string(),
                tag: image_tag.to_string(),
                ..Default::default()
            }),
            None,
            None,
        );
        while let Some(result) = pull_stream.next().await {
            if result.is_err() {
                break;
            }
        }

        for index in 0..service.replicas {
            let container_name = if index == 0 {
                service.container_name(&self.project_name)
            } else {
                format!("{}-{index}", service.container_name(&self.project_name))
            };
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

            // Only the first replica publishes host ports — a second binding
            // of the same host port would refuse at start.
            let mut config = config.clone();
            if index > 0
                && let Some(host_config) = config.host_config.as_mut()
            {
                host_config.port_bindings = None;
            }
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

            // Connect the container to the project network before starting
            self.docker
                .connect_network(
                    &self.network_name(),
                    bollard::network::ConnectNetworkOptions {
                        container: container_name.clone(),
                        endpoint_config: bollard::models::EndpointSettings {
                            aliases: Some(vec![service.name.clone()]),
                            ..Default::default()
                        },
                    },
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

    /// Remove one compose service and its corresponding container.
    ///
    /// This is intentionally service-scoped. It does not run project-wide
    /// `docker compose down`, because infrastructure delete may target a single
    /// resource while leaving the rest of the local stack intact.
    /// Tear down a single service: remove it from compose state and force-remove
    /// its container. Idempotent — an already-absent container is success.
    pub async fn remove_service(&self, service: &str) -> Result<(), ComposeError> {
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
        let replica_count = containers.len() as u32;
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
        let mut live = lift_from_inspect(service, &inspect, &self.deployment_dir);
        live.replicas = replica_count.max(1);
        // A container's inspect env is the MERGE of image-baked vars and what
        // we injected — every image bakes at least PATH, so comparing the
        // merge against the declared env makes every service drift forever.
        // Subtract the image's own env (exact KEY=VALUE matches only, so an
        // operator override of an image var still surfaces as real drift):
        // what remains is what this platform injected — the comparable set.
        if let Some(image_id) = inspect.image.as_deref()
            && let Ok(image) = self.docker.inspect_image(image_id).await
        {
            let baked: Vec<String> = image.config.and_then(|c| c.env).unwrap_or_default();
            live.environment.retain(|entry| {
                !baked
                    .iter()
                    .any(|baked| baked == &format!("{}={}", entry.name, entry.value))
            });
        }
        Ok(Some(live))
    }

    /// Open service logs as an incremental stream.
    pub async fn log_stream(
        &self,
        service: &str,
        follow: bool,
        tail: Option<u32>,
    ) -> Result<LogStream, ComposeError> {
        use futures_util::StreamExt;

        self.ensure_reachable().await?;
        let container_name = format!("{}_{}", self.project_name, service);
        let error_name = container_name.clone();
        let stream = self
            .docker
            .logs(
                &container_name,
                Some(LogsOptions::<String> {
                    follow,
                    stdout: true,
                    stderr: true,
                    tail: tail.unwrap_or(100).to_string(),
                    ..Default::default()
                }),
            )
            .map(move |item| {
                item.map(|chunk| chunk.to_string())
                    .map_err(|error| ComposeError::ContainerFailed {
                        container: error_name.clone(),
                        source: anyhow::anyhow!(error),
                    })
            });
        Ok(Box::pin(stream))
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
        Ok(service
            .publish
            .contains(&port)
            .then(|| ("127.0.0.1".into(), port)))
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
            .publish
            .into_iter()
            .map(|port| ("127.0.0.1".to_string(), port, port, "tcp".to_string()))
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
            let mut config = self.lower(service);
            // Scaled replicas never publish host ports — the primary
            // container holds the binding.
            if let Some(host_config) = config.host_config.as_mut() {
                host_config.port_bindings = None;
            }
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

    /// The recorded spec for one service, from the deployment's compose
    /// state — the container configuration a scale-up replicates.
    pub fn recorded_service(&self, name: &str) -> Result<Option<ComposeService>, ComposeError> {
        Ok(self.load_compose_state()?.services.get(name).cloned())
    }

    /// Lower one logical service to its concrete Docker container config —
    /// the only place host paths and host environment are resolved.
    fn lower(&self, service: &ComposeService) -> ContainerConfig<String> {
        lower_container_config(service, &self.project_name, &self.deployment_dir)
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

    async fn is_service_current(
        &self,
        service_name: &str,
        manifests: &[serde_json::Value],
    ) -> bool {
        let Some(manifest) = manifests.first() else {
            return true;
        };
        let image_tag = manifest.get("image").and_then(|v| v.as_str()).unwrap_or("");
        if image_tag.is_empty() {
            return true;
        }
        let container_name = format!("{}_{}", self.project_name, service_name);
        let Ok(inspect) = self
            .docker
            .inspect_container(
                &container_name,
                None::<bollard::container::InspectContainerOptions>,
            )
            .await
        else {
            // Container doesn't exist — not current.
            return false;
        };
        let Some(container_image_id) = inspect.image.as_deref() else {
            return false;
        };
        let Ok(local_image) = self.docker.inspect_image(image_tag).await else {
            // Can't resolve local image — assume current to avoid false positives.
            return true;
        };
        let Some(local_image_id) = local_image.id.as_deref() else {
            return true;
        };
        container_image_id == local_image_id
    }

    fn supports_delete(&self) -> bool {
        true
    }

    async fn delete_service(
        &self,
        service_name: &str,
        _manifests: &[serde_json::Value],
    ) -> Result<(), deploy_engine::DeployError> {
        self.remove_service(service_name)
            .await
            .map_err(|error| deploy_engine::DeployError::Other(anyhow::anyhow!(error)))
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

/// The concrete Docker shapes for one logical service: bind strings, the
/// environment map, and any host-resolved AWS selector paths. Produced only
/// inside [`ComposePlatform::lower`] — host state never travels further up.
fn lower_container_config(
    service: &ComposeService,
    project_name: &str,
    deployment_dir: &Path,
) -> ContainerConfig<String> {
    let exposed_ports = if service.publish.is_empty() {
        None
    } else {
        Some(
            service
                .publish
                .iter()
                .map(|port| (format!("{port}/tcp"), HashMap::new()))
                .collect(),
        )
    };
    let port_bindings = if service.publish.is_empty() {
        None
    } else {
        let mut bindings = HashMap::new();
        for port in &service.publish {
            bindings.insert(
                format!("{port}/tcp"),
                Some(vec![PortBinding {
                    host_ip: Some("0.0.0.0".into()),
                    host_port: Some(port.to_string()),
                }]),
            );
        }
        Some(bindings)
    };

    let mut volumes: Vec<String> = service
        .volumes
        .iter()
        .map(|volume| match volume {
            Volume::State(StateVolume { sub, at }) => format!(
                "{}:{at}",
                deployment_dir.join(".tokeira-state").join(sub).display()
            ),
            Volume::Config(ConfigVolume { sub, at }) => {
                format!("{}:{at}", deployment_dir.join("config").join(sub).display())
            }
            Volume::DockerSocket => "/var/run/docker.sock:/var/run/docker.sock".to_string(),
        })
        .collect();
    let mut environment: HashMap<String, String> = service
        .environment
        .iter()
        .map(|entry| (entry.name.clone(), entry.value.clone()))
        .collect();

    // The server-config coupling: mount the live file and carry the desired
    // digest so a `tokeirad.toml` edit recreates the container onto the new
    // content.
    if let Some(digest) = &service.server_config_digest {
        volumes.push(format!(
            "{}:/etc/tokeira/tokeirad.toml:ro",
            deployment_dir.join("tokeirad.toml").display()
        ));
        environment.insert(
            "TOKEIRA_CONFIG".to_string(),
            "/etc/tokeira/tokeirad.toml".to_string(),
        );
        environment.insert("TOKEIRA_SERVER_CONFIG_DIGEST".to_string(), digest.clone());
    }
    if let Some(digest) = &service.config_digest {
        environment.insert("TOKEIRA_CONFIG_DIGEST".to_string(), digest.clone());
    }

    // Host-state resolution happens here and nowhere else: the credential
    // path and profile are apply-time facts of the operator's machine, never
    // part of the desired manifest.
    if let Some(region) = &service.aws_region {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/root".to_string());
        volumes.push(format!("{home}/.aws:/home/nonroot/.aws:ro"));
        environment.insert("HOME".to_string(), "/home/nonroot".to_string());
        environment.insert("AWS_REGION".to_string(), region.clone());
        if let Ok(profile) = std::env::var("AWS_PROFILE") {
            environment.insert("AWS_PROFILE".to_string(), profile);
        }
    }

    let mut label_map = HashMap::from([
        ("com.docker.compose.service".into(), service.name.clone()),
        (
            "com.docker.compose.project".into(),
            project_name.to_string(),
        ),
    ]);
    // Start-order is a compose concept Docker does not model — record it as
    // the compose-conventional label (`dep:condition:restart` triplets) so
    // `describe` can reconstruct `depends_on` instead of reporting eternal
    // drift against a fact the container cannot remember.
    if !service.depends_on.is_empty() {
        label_map.insert(
            "com.docker.compose.depends_on".into(),
            service
                .depends_on
                .iter()
                .map(|dep| format!("{dep}:service_started:false"))
                .collect::<Vec<_>>()
                .join(","),
        );
    }
    let labels = Some(label_map);

    ContainerConfig {
        image: Some(service.image.clone()),
        cmd: if service.command.is_empty() {
            None
        } else {
            Some(service.command.clone())
        },
        env: if environment.is_empty() {
            None
        } else {
            Some(
                environment
                    .iter()
                    .map(|(key, value)| format!("{key}={value}"))
                    .collect(),
            )
        },
        host_config: Some(HostConfig {
            binds: if volumes.is_empty() {
                None
            } else {
                Some(volumes)
            },
            port_bindings,
            ..Default::default()
        }),
        labels,
        exposed_ports,
        ..Default::default()
    }
}

/// Lift one live container back into the logical model — the inverse of
/// lowering, so drift compares logical-to-logical. Couplings the lowering
/// injected (config digests, the AWS selector mounts) fold back into their
/// model fields; a bind that matches no logical form is dropped, the same
/// honesty class as the image-baked-environment subtraction: a hand-added
/// host bind is invisible to drift rather than a permanent phantom.
fn lift_from_inspect(
    name: &str,
    inspect: &ContainerInspectResponse,
    deployment_dir: &Path,
) -> ComposeService {
    let image = inspect
        .config
        .as_ref()
        .and_then(|config| config.image.clone())
        .unwrap_or_default();
    let mut publish = inspect
        .host_config
        .as_ref()
        .and_then(|host| host.port_bindings.as_ref())
        .map(|bindings| {
            bindings
                .iter()
                .flat_map(|(container, host_bindings)| {
                    let container: Option<u16> = container.trim_end_matches("/tcp").parse().ok();
                    host_bindings
                        .clone()
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(move |binding| {
                            // The logical vocabulary publishes equal pairs;
                            // the host side is the honest lift of anything
                            // hand-rebound.
                            binding
                                .host_port
                                .and_then(|host| host.parse::<u16>().ok())
                                .or(container)
                        })
                })
                .collect::<Vec<u16>>()
        })
        .unwrap_or_default();
    // Docker hands back a map; its iteration order is not a fact about the
    // service. Sort so records are stable run-to-run.
    publish.sort_unstable();

    let state_root = deployment_dir.join(".tokeira-state");
    let config_root = deployment_dir.join("config");
    let server_config_source = deployment_dir.join("tokeirad.toml");
    let mut volumes = Vec::new();
    for bind in inspect
        .host_config
        .as_ref()
        .and_then(|host| host.binds.as_ref())
        .into_iter()
        .flatten()
    {
        let Some((source, target)) = bind.split_once(':') else {
            continue;
        };
        let at = target.trim_end_matches(":ro").to_string();
        let source = Path::new(source);
        if source == Path::new("/var/run/docker.sock") {
            volumes.push(Volume::DockerSocket);
        } else if let Ok(sub) = source.strip_prefix(&state_root) {
            volumes.push(Volume::State(StateVolume {
                sub: sub.display().to_string(),
                at,
            }));
        } else if let Ok(sub) = source.strip_prefix(&config_root) {
            volumes.push(Volume::Config(ConfigVolume {
                sub: sub.display().to_string(),
                at,
            }));
        } else if source == server_config_source
            || source.file_name().is_some_and(|name| name == ".aws")
        {
            // Lowering-injected couplings: represented by their model fields
            // (`server_config_digest`, `aws_region`), not as volumes.
        }
    }

    let mut aws_region = None;
    let mut server_config_digest = None;
    let mut config_digest = None;
    let mut environment = Vec::new();
    for entry in inspect
        .config
        .as_ref()
        .and_then(|config| config.env.as_ref())
        .into_iter()
        .flatten()
    {
        let Some((key, value)) = entry.split_once('=') else {
            continue;
        };
        match key {
            "TOKEIRA_SERVER_CONFIG_DIGEST" => server_config_digest = Some(value.to_string()),
            "TOKEIRA_CONFIG_DIGEST" => config_digest = Some(value.to_string()),
            "AWS_REGION" => aws_region = Some(value.to_string()),
            // Lowering-injected companions of the couplings above.
            "TOKEIRA_CONFIG" | "AWS_PROFILE" => {}
            "HOME" if value == "/home/nonroot" => {}
            _ => environment.push(Environment {
                name: key.to_string(),
                value: value.to_string(),
            }),
        }
    }

    ComposeService {
        name: name.to_string(),
        image,
        // The caller owns the live replica count; one container answers for
        // one replica here.
        replicas: 1,
        publish,
        volumes,
        environment,
        // Start-order round-trips through the compose-conventional label
        // written at create (`dep:condition:restart` triplets) — a container
        // created without it (or by hand) reads as no ordering, which is now
        // honest drift rather than a permanent phantom.
        depends_on: inspect
            .config
            .as_ref()
            .and_then(|config| config.labels.as_ref())
            .and_then(|labels| labels.get("com.docker.compose.depends_on"))
            .map(|raw| {
                raw.split(',')
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| entry.split(':').next().unwrap_or(entry).to_string())
                    .collect()
            })
            .unwrap_or_default(),
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
        command: inspect
            .config
            .as_ref()
            .and_then(|config| config.cmd.clone())
            .unwrap_or_default(),
        aws_region,
        server_config_digest,
        config_digest,
        // Desired-side declarations, not container facts.
        module: String::new(),
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
    use proptest::prelude::*;

    use super::*;

    // The two compose phantom-drift classes, pinned:
    //
    // 1. Set-order roulette: the live reconstruction assembles ports/volumes
    //    from Docker maps with unstable iteration order — ordered comparison
    //    made every multi-port service flap between clean and "updated".
    #[test]
    fn diff_treats_set_valued_fields_as_sets() {
        let desired = serde_json::json!({
            "name": "tokeirad",
            "publish": [7233, 9090],
            "volumes": [{"State": {"sub": "a", "at": "/a"}}, {"State": {"sub": "b", "at": "/b"}}],
            "depends_on": ["mimir", "loki"],
        });
        let live = serde_json::json!({
            "name": "tokeirad",
            "publish": [9090, 7233],
            "volumes": [{"State": {"sub": "b", "at": "/b"}}, {"State": {"sub": "a", "at": "/a"}}],
            "depends_on": ["loki", "mimir"],
        });
        let diffs = manifest_field_diffs(
            &canonicalize_manifest(live),
            &canonicalize_manifest(desired),
        );
        assert!(diffs.is_empty(), "order is not drift: {diffs:?}");
    }

    // 2. Real differences name their field with both values — the evidence
    //    `--detail` prints. A bare "configuration changed" hid the
    //    depends_on phantom for a day.
    #[test]
    fn diff_names_the_differing_field_with_evidence() {
        let desired = serde_json::json!({ "depends_on": ["mimir", "loki"] });
        let live = serde_json::json!({ "depends_on": [] });
        let diffs = manifest_field_diffs(
            &canonicalize_manifest(live),
            &canonicalize_manifest(desired),
        );
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].field, "depends_on");
        assert_eq!(diffs[0].before.as_deref(), Some("[]"));
        assert_eq!(diffs[0].after.as_deref(), Some(r#"["loki","mimir"]"#));
    }

    // Start-order round-trips through the compose-conventional label: written
    // as `dep:condition:restart` triplets at create, read back to plain names
    // by the reconstruction.
    #[test]
    fn depends_on_round_trips_through_the_container_label() {
        let inspect = ContainerInspectResponse {
            config: Some(bollard::models::ContainerConfig {
                labels: Some(HashMap::from([(
                    "com.docker.compose.depends_on".to_string(),
                    "mimir:service_started:false,loki:service_started:false".to_string(),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        };
        let service = lift_from_inspect("grafana", &inspect, Path::new("/deployments/demo"));
        assert_eq!(service.depends_on, vec!["mimir", "loki"]);

        // And a container without the label reads as no ordering — honest
        // drift for hand-made containers, not a crash.
        let bare = ContainerInspectResponse::default();
        assert!(
            lift_from_inspect("grafana", &bare, Path::new("/deployments/demo"))
                .depends_on
                .is_empty()
        );
    }

    // The lift is the lowering's inverse: a lowered container inspected back
    // yields the logical service — host paths, injected couplings, and host
    // AWS selectors all fold back into their model fields.
    #[test]
    fn lift_inverts_lowering() {
        let deployment_dir = Path::new("/deployments/demo");
        let service = ComposeService {
            name: "tokeirad".into(),
            image: "tokeirad:latest".into(),
            replicas: 1,
            publish: vec![7233],
            volumes: vec![
                Volume::State(StateVolume {
                    sub: "data".into(),
                    at: "/var/lib/tokeira".into(),
                }),
                Volume::Config(ConfigVolume {
                    sub: "alloy.alloy".into(),
                    at: "/etc/alloy/config.alloy".into(),
                }),
                Volume::DockerSocket,
            ],
            environment: vec![Environment {
                name: "RUST_LOG".into(),
                value: "info".into(),
            }],
            aws_region: Some("eu-west-2".into()),
            server_config_digest: Some("sha256:abc".into()),
            config_digest: Some("sha256:def".into()),
            ..Default::default()
        };
        let config = lower_container_config(&service, "demo", deployment_dir);
        let inspect = ContainerInspectResponse {
            config: Some(bollard::models::ContainerConfig {
                image: config.image.clone(),
                env: config.env.clone(),
                cmd: config.cmd.clone(),
                labels: config.labels.clone(),
                ..Default::default()
            }),
            host_config: config.host_config.clone().map(|host| HostConfig {
                binds: host.binds,
                port_bindings: host.port_bindings,
                ..Default::default()
            }),
            ..Default::default()
        };
        let lifted = lift_from_inspect("tokeirad", &inspect, deployment_dir);
        let desired = canonicalize_manifest(service.to_manifest());
        let live = canonicalize_manifest(lifted.to_manifest());
        let diffs = manifest_field_diffs(&live, &desired);
        assert!(diffs.is_empty(), "lift must invert lowering: {diffs:?}");
    }

    fn arb_identifier() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_-]{0,7}".prop_map(|s| s.to_string())
    }

    fn arb_compose_service() -> impl Strategy<Value = ComposeService> {
        (
            arb_identifier(),
            arb_identifier(),
            1u32..3,
            prop::collection::vec(1u16..=65535, 0..4),
            prop::collection::vec(arb_identifier(), 0..3),
            prop::collection::vec((arb_identifier(), arb_identifier()), 0..4),
            prop::collection::vec(arb_identifier(), 0..3),
            prop::option::of((
                prop::collection::vec(arb_identifier(), 1..4),
                prop::option::of("[1-9][0-9]{0,2}s"),
                prop::option::of("[1-9][0-9]{0,2}s"),
                prop::option::of(0u32..10),
            )),
            prop::collection::vec(arb_identifier(), 0..3),
        )
            .prop_map(
                |(
                    name,
                    image_tag,
                    replicas,
                    publish,
                    volumes,
                    environment,
                    depends_on,
                    healthcheck,
                    command,
                )| {
                    ComposeService {
                        image: format!("example/{name}:{image_tag}"),
                        name: name.clone(),
                        replicas,
                        publish,
                        volumes: volumes
                            .into_iter()
                            .map(|volume| {
                                Volume::State(StateVolume {
                                    sub: volume.clone(),
                                    at: format!("/data/{volume}"),
                                })
                            })
                            .collect(),
                        environment: environment
                            .into_iter()
                            .map(|(name, value)| Environment { name, value })
                            .collect(),
                        depends_on,
                        healthcheck: healthcheck.map(|(test, interval, timeout, retries)| {
                            Healthcheck {
                                test,
                                interval,
                                timeout,
                                retries,
                            }
                        }),
                        command,
                        ..Default::default()
                    }
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        // The manifest round-trip is the serialization-completeness property
        // now: every logical field survives manifest → recovery, so recorded
        // state can rebuild the exact service.
        #[test]
        fn p11_compose_service_manifest_round_trips(service in arb_compose_service()) {
            let recovered = service_from_manifest(service.to_manifest()).unwrap();
            // `name` rides the manifest for recovery but is refused as
            // authored input, so it round-trips through the recovery path's
            // deserialization only via the manifest field.
            prop_assert_eq!(&recovered.image, &service.image);
            prop_assert_eq!(recovered.replicas, service.replicas);
            prop_assert_eq!(&recovered.publish, &service.publish);
            prop_assert_eq!(&recovered.volumes, &service.volumes);
            prop_assert_eq!(&recovered.environment, &service.environment);
            prop_assert_eq!(&recovered.depends_on, &service.depends_on);
            prop_assert_eq!(&recovered.healthcheck, &service.healthcheck);
            prop_assert_eq!(&recovered.command, &service.command);
        }
    }

    #[test]
    fn serializes_compose_yaml_with_all_fields() {
        let service = ComposeService {
            name: "grafana".into(),
            image: "grafana/grafana-oss:12.4.3".into(),
            replicas: 1,
            publish: vec![3000],
            volumes: vec![Volume::State(StateVolume {
                sub: "grafana".into(),
                at: "/var/lib/grafana".into(),
            })],
            environment: vec![Environment {
                name: "GF_SECURITY_ADMIN_PASSWORD".into(),
                value: "admin".into(),
            }],
            depends_on: vec!["mimir".into()],
            healthcheck: Some(Healthcheck {
                test: vec!["CMD".into(), "curl".into(), "http://localhost:3000".into()],
                interval: Some("10s".into()),
                timeout: Some("3s".into()),
                retries: Some(3),
            }),
            command: vec!["run".into(), "--config".into()],
            ..Default::default()
        };
        let yaml = compose_yaml_fragment(&service).unwrap();
        assert!(yaml.contains("grafana/grafana-oss:12.4.3"));
        assert!(yaml.contains("3000"));
        assert!(yaml.contains("GF_SECURITY_ADMIN_PASSWORD"));
        assert!(yaml.contains("/var/lib/grafana"));
    }

    #[test]
    fn invalid_socket_reports_docker_not_available() {
        let error = ComposePlatform::connect_with_socket(
            "/tmp/compose.yaml",
            "/tmp",
            "test",
            "/definitely/not/a/docker.sock",
        )
        .unwrap_err();
        assert!(matches!(error, ComposeError::DockerNotAvailable { .. }));
    }

    #[test]
    fn docker_reachability_maps_to_the_typed_iac_issue() {
        let error = ComposeError::DockerNotAvailable {
            socket_path: "/var/run/docker.sock".to_string(),
            evidence: "connect ECONNREFUSED /var/run/docker.sock".to_string(),
        };
        let iac::IacError::PlatformIssue(issue) = iac::IacError::from(error) else {
            panic!("Docker reachability must retain the typed plan refusal");
        };
        assert_eq!(issue.component, "Docker");
        assert_eq!(issue.fact, "Unable to connect to Docker");
        assert_eq!(issue.evidence, "connect ECONNREFUSED /var/run/docker.sock");
        assert_eq!(
            issue.direction.as_deref(),
            Some(
                "nothing accepted connections at `/var/run/docker.sock` - verify Docker is listening there"
            )
        );
    }

    // The direction table admits both spellings of connection-refused and
    // establishes exactly the socket-level claim — never "the daemon is
    // stopped" (umbrella D4: direction only where the error establishes it).
    #[test]
    fn connection_refused_establishes_the_socket_direction() {
        for evidence in [
            "connect ECONNREFUSED /var/run/docker.sock",
            "error trying to connect: Connection refused (os error 61)",
        ] {
            let issue = docker_unreachable_issue("/var/run/docker.sock", evidence);
            assert_eq!(issue.component, "Docker");
            assert_eq!(issue.fact, "Unable to connect to Docker");
            assert_eq!(issue.evidence, evidence, "evidence passes through verbatim");
            assert_eq!(
                issue.direction.as_deref(),
                Some(
                    "nothing accepted connections at `/var/run/docker.sock` - verify Docker is listening there"
                ),
            );
        }
    }

    #[test]
    fn an_unrecognized_error_class_establishes_no_direction() {
        let issue = docker_unreachable_issue(
            "/var/run/docker.sock",
            "error trying to connect: operation timed out",
        );
        assert_eq!(issue.direction, None, "absent is the honest default");
    }
}
