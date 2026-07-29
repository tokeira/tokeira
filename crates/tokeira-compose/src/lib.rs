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
//! `depends_on` is a compose-file concept with no Docker runtime equivalent —
//! it round-trips through the compose-conventional
//! `com.docker.compose.depends_on` label written at create and read back by
//! `describe`, so ordering drift is real drift (a container created without
//! it), never a reconstruction artifact.
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
    image::CreateImageOptions,
    models::{ContainerInspectResponse, HostConfig, PortBinding},
};
use futures_util::StreamExt;
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
    /// Command to run in the container (overrides image CMD).
    pub command: Vec<String>,
    /// Infra-graph-only dependencies (full engine `ResourceId` strings) —
    /// resources that must exist **before this container is created**, e.g.
    /// the config-files resource whose outputs this service bind-mounts
    /// (Docker manufactures a missing bind source as a *directory*, so
    /// creating the container first poisons the config path). Distinct from
    /// [`depends_on`](Self::depends_on), which is compose *container start*
    /// ordering and must stay pure service names; excluded from the manifest.
    #[serde(skip)]
    pub resource_dependencies: Vec<String>,
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
            .chain(
                self.resource_dependencies
                    .iter()
                    .map(|dep| iac::ResourceId(dep.clone())),
            )
            .collect()
    }

    fn module(&self) -> &str {
        &self.name
    }

    fn display_kind(&self) -> Option<&'static str> {
        Some("service")
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
        platform.remove_service(&self.name).await?;
        Ok(())
    }

    /// What a compose-service change does, established from this crate's own
    /// provider paths. The load-bearing claim:
    /// an engine `Update` is *effected* as a replacement — `reconcile_service`
    /// stops and force-removes the container before creating the new one —
    /// so `~ compose/grafana` means the service goes away and comes back,
    /// not an in-place edit. Data rides bind-mounted host paths, which
    /// neither the removal nor the recreation touches.
    fn change_semantics(&self, ctx: &iac::SemanticsContext<'_>) -> iac::ChangeSemantics {
        // EngineFacts, cited by module identity — never repo layout — so the
        // citation stays true wherever this crate is used. Every name below
        // is a real identifier in this module.
        const RECONCILE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::ComposePlatform::reconcile_service — stop_container(t: 1) → \
             remove_container(force: true) → create fresh from the definition"
        ));
        const REMOVE: iac::Citation = iac::Citation::code(concat!(
            module_path!(),
            "::ComposePlatform::remove_service — stop_container + remove_container \
             with RemoveContainerOptions { force: true, ..Default::default() } — the \
             `v` (remove volumes) flag stays false, and bind mounts are host paths"
        ));
        use iac::{
            ChangeKind, Confidence, DataEffect, Disruption, LifecycleOperation, ReplacementPolicy,
            Reversibility,
        };
        match ctx.kind {
            ChangeKind::Create => iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Created,
                    citation: RECONCILE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: RECONCILE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::None,
                    citation: RECONCILE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::NoDataHeld,
                    citation: RECONCILE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: REMOVE,
                },
                statement: None,
            },
            // Update and Replace share one provider path — reconcile — and
            // therefore one truth: destroy-before-create, unavailable while
            // it happens, volumes preserved, reversible by re-applying the
            // prior definition.
            ChangeKind::Update | ChangeKind::Replace => iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Replaced,
                    citation: RECONCILE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::DestroyBeforeCreate,
                    citation: RECONCILE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: RECONCILE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: REMOVE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: RECONCILE,
                },
                statement: Some(std::borrow::Cow::Borrowed(
                    "it would be stopped, removed, and recreated from the definition",
                )),
            },
            ChangeKind::Delete => iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: REMOVE,
                },
                replacement: Confidence::EngineFact {
                    value: ReplacementPolicy::NotRequired,
                    citation: REMOVE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: REMOVE,
                },
                data_effect: Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    citation: REMOVE,
                },
                reversibility: Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    citation: RECONCILE,
                },
                statement: None,
            },
            // The engine never asks about NoChange; totality answers anyway,
            // with nothing.
            ChangeKind::NoChange => iac::ChangeSemantics::default(),
        }
    }

    async fn describe(
        &self,
        ctx: &iac::ProvisionContext,
    ) -> Result<iac::DescribeResult, iac::IacError> {
        // Without the live ComposePlatform handle we cannot query Docker, so
        // existence is unknown rather than confirmed absent.
        let Some(platform) = ctx.extension::<ComposePlatform>() else {
            return Ok(iac::DescribeResult::Unsupported);
        };
        match platform.running_service(&self.name).await? {
            Some(mut service) => {
                // Detect image rebuild: if the container's image digest differs
                // from the local image's current digest for the same tag, report
                // the running image as the full digest so diff() sees a change.
                if let Some(stale_digest) = platform
                    .container_image_stale(&self.name, &service.image)
                    .await
                {
                    service.image = stale_digest;
                }
                Ok(iac::DescribeResult::Present(iac::ResourceState {
                    resource_type: iac::ResourceType::new("compose_service"),
                    physical_id: service.name.clone(),
                    properties: service.to_manifest(),
                    dependencies: Vec::new(),
                    created_at: String::new(),
                    updated_at: String::new(),
                    module: self.name.clone(),
                }))
            }
            None => Ok(iac::DescribeResult::Absent),
        }
    }

    fn diff(
        &self,
        current: &iac::ResourceState,
        _ctx: &iac::ProvisionContext,
    ) -> iac::InternalChange {
        // Compare canonicalized manifests: `ports`/`volumes`/`depends_on` are
        // sets semantically, but the live reconstruction assembles them from
        // Docker maps whose iteration order is unstable — ordered comparison
        // manufactured flapping phantom updates for every multi-port service.
        let current_manifest = canonicalize_manifest(current.properties.clone());
        let desired_manifest = canonicalize_manifest(self.to_manifest());
        let details = manifest_field_diffs(&current_manifest, &desired_manifest);
        if details.is_empty() {
            iac::InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            iac::InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: self.resource_type(),
                details,
            }
        }
    }
}

/// Sort the set-valued manifest arrays so equality means set equality. The
/// desired side is authored order; the live side is Docker-map iteration
/// order — only the canonical form is comparable.
fn canonicalize_manifest(mut manifest: serde_json::Value) -> serde_json::Value {
    if let Some(object) = manifest.as_object_mut() {
        for key in ["ports", "volumes", "depends_on"] {
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

    /// Reconcile one compose service by updating the compose file and replacing
    /// the corresponding local container.
    pub async fn reconcile_service(&self, service: &ComposeService) -> Result<(), ComposeError> {
        self.ensure_reachable().await?;
        let mut state = self.load_compose_state()?;
        state.services.insert(service.name.clone(), service.clone());
        self.save_compose_state(&state)?;

        // Ensure the project network exists so containers can resolve each other by name
        self.ensure_network().await?;

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
        let mut live = service_from_inspect(service, &inspect);
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
            live.environment
                .retain(|key, value| !baked.iter().any(|entry| entry == &format!("{key}={value}")));
        }
        Ok(Some(live))
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
            if let Some((host, container)) = parse_port_mapping(&mapping)
                && container == port
            {
                return Ok(Some(("127.0.0.1".into(), host)));
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
                .collect::<Vec<_>>()
        })
        .map(|mut ports: Vec<String>| {
            // Docker hands back a map; its iteration order is not a fact
            // about the service. Sort so records are stable run-to-run.
            ports.sort();
            ports
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
        // Infra-graph edges are desired-side declarations, not container
        // facts — same reasoning as depends_on above (and serde-skipped, so
        // they never participate in the manifest diff either).
        resource_dependencies: Vec::new(),
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
    use iac::Resource as _;
    use proptest::prelude::*;

    use super::*;

    // Golden declarations (change-semantics task 4.5): the six scenarios,
    // asserting classification and confidence — never prose. The pivotal
    // row: an engine Update is *effected* as a replacement with the service
    // unavailable — the in-place reading of `~` was wrong, and this is where
    // it stops.
    #[test]
    fn compose_service_declarations_are_the_reconcile_truth() {
        use iac::{
            ChangeKind, Confidence, DataEffect, Disruption, LifecycleOperation, ReplacementPolicy,
            Reversibility, SemanticsContext,
        };

        let service = ComposeService::default();
        let declared = |kind: ChangeKind, field_diffs: &[iac::FieldDiff]| {
            service.change_semantics(&SemanticsContext {
                kind,
                current: None,
                field_diffs,
            })
        };

        // Creation.
        let create = declared(ChangeKind::Create, &[]);
        assert!(matches!(
            create.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::Created,
                ..
            }
        ));
        assert!(matches!(
            create.data_effect,
            Confidence::EngineFact {
                value: DataEffect::NoDataHeld,
                ..
            }
        ));

        // In-place update and drift-driven update: one provider path, one
        // truth — replaced, destroy-before-create, unavailable meanwhile.
        // (Drift is the same declaration: the reconcile does not care why
        // the container differs.)
        let definition_update = declared(
            ChangeKind::Update,
            &[iac::FieldDiff {
                field: "image".into(),
                before: Some("a".into()),
                after: Some("b".into()),
            }],
        );
        let drift_update = declared(
            ChangeKind::Update,
            &[iac::FieldDiff::observation("live container drifted")],
        );
        for update in [&definition_update, &drift_update] {
            assert!(matches!(
                update.operation,
                Confidence::EngineFact {
                    value: LifecycleOperation::Replaced,
                    ..
                }
            ));
            assert!(matches!(
                update.replacement,
                Confidence::EngineFact {
                    value: ReplacementPolicy::DestroyBeforeCreate,
                    ..
                }
            ));
            assert!(matches!(
                update.disruption,
                Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    ..
                }
            ));
            assert!(matches!(
                update.data_effect,
                Confidence::EngineFact {
                    value: DataEffect::Preserved,
                    ..
                }
            ));
            assert!(matches!(
                update.reversibility,
                Confidence::EngineFact {
                    value: Reversibility::Reversible,
                    ..
                }
            ));
        }

        // Replacement: identical to update — same reconcile path.
        let replace = declared(ChangeKind::Replace, &[]);
        assert_eq!(replace, definition_update);

        // Deletion: the service goes away; bind-mounted data does not.
        let delete = declared(ChangeKind::Delete, &[]);
        assert!(matches!(
            delete.operation,
            Confidence::EngineFact {
                value: LifecycleOperation::Deleted,
                ..
            }
        ));
        assert!(matches!(
            delete.disruption,
            Confidence::EngineFact {
                value: Disruption::UnavailableDuringChange,
                ..
            }
        ));
        assert!(matches!(
            delete.data_effect,
            Confidence::EngineFact {
                value: DataEffect::Preserved,
                ..
            }
        ));

        // Unknown/inapplicable: NoChange declares nothing — the honest
        // all-Unknown default, not an invented claim.
        assert_eq!(
            declared(ChangeKind::NoChange, &[]),
            iac::ChangeSemantics::default()
        );
    }

    // The two compose phantom-drift classes, pinned:
    //
    // 1. Set-order roulette: the live reconstruction assembles ports/volumes
    //    from Docker maps with unstable iteration order — ordered comparison
    //    made every multi-port service flap between clean and "updated".
    #[test]
    fn diff_treats_set_valued_fields_as_sets() {
        let desired = serde_json::json!({
            "name": "tokeirad",
            "ports": ["7233:7233", "9090:9090"],
            "volumes": ["/a:/a", "/b:/b"],
            "depends_on": ["mimir", "loki"],
        });
        let live = serde_json::json!({
            "name": "tokeirad",
            "ports": ["9090:9090", "7233:7233"],
            "volumes": ["/b:/b", "/a:/a"],
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
        let service = service_from_inspect("grafana", &inspect);
        assert_eq!(service.depends_on, vec!["mimir", "loki"]);

        // And a container without the label reads as no ordering — honest
        // drift for hand-made containers, not a crash.
        let bare = ContainerInspectResponse::default();
        assert!(service_from_inspect("grafana", &bare).depends_on.is_empty());
    }

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
            prop::collection::vec(arb_identifier(), 0..3),
        )
            .prop_map(
                |(
                    name,
                    image_tag,
                    ports,
                    volumes,
                    environment,
                    depends_on,
                    healthcheck,
                    command,
                )| {
                    ComposeService {
                        resource_dependencies: Vec::new(),
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
                        command,
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
            resource_dependencies: Vec::new(),
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
            command: vec!["run".into(), "--config".into()],
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
