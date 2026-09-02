//! The platform-owned observability content: the artifact tree shipped
//! beside the definition, uploaded as one resource, and the rendered Alloy
//! sidecar configuration parameters.
//!
//! Follows the Compose observability pattern: content is read from the
//! definition source directory (`placement.definition_dir`), so a retained
//! revision renders its own content; the artifact set is one resource — one
//! content tree with one digest — mirroring how Compose treats its rendered
//! configuration files.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context as _;
use serde::Deserialize;
use sha2::Digest;
use tokeira_aws::resources::{s3_object::S3Object, ssm_parameter::SsmParameterResource};
use tokeira_ecs::modules::observability::{
    AlloyRenderContext, load_observability_artifacts, render_alloy_config,
};
use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType,
};
use tokeira_observability::validation::{
    AlertRuleValidator, AlloyConfigValidator, DashboardValidator,
};
use tokeira_platform::{
    author::LocatedValue,
    declaration::{
        DeploymentRef, ObservabilityCheck, ObservabilityCheckOutcome, ObservabilityCheckReport,
        ObservabilityCheckStatus,
    },
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

use crate::ops::EcsOperationCoordinates;

/// Author-visible name of the artifact-tree resource type.
pub(crate) const ARTIFACTS_TYPE: &str = "ObservabilityArtifacts";
/// Author-visible name of the rendered Alloy parameter resource type.
pub(crate) const ALLOY_CONFIG_TYPE: &str = "AlloyConfig";

/// The platform package's namespace word.
pub(crate) const NAMESPACE: &str = "tokeira_ecs_deployment";

/// The package's author-visible kind names.
pub(crate) const KINDS: &[&str] = &[ALLOY_CONFIG_TYPE, ARTIFACTS_TYPE];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub(crate) fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    Some(match name {
        n if n == ALLOY_CONFIG_TYPE => {
            kind::decode_resource::<AlloyConfig, AlloyConfigResource>(ALLOY_CONFIG_TYPE, value)
        }
        n if n == ARTIFACTS_TYPE => kind::decode_resource::<
            ObservabilityArtifacts,
            ObservabilityArtifactsResource,
        >(ARTIFACTS_TYPE, value),
        _ => return None,
    })
}

/// The assembled namespace for the platform declaration.
pub(crate) fn namespace() -> Namespace {
    Namespace {
        name: NAMESPACE,
        kinds: KINDS,
        defaults: None,
        decode,
    }
}

/// ECS's read-only validation of one definition-derived observability stack.
///
/// The check reuses the admitted deployment's staged content and realized
/// resource identities. It performs no AWS mutation and does not load
/// deployment state. The only network action is a timeout-bounded HTTP
/// readiness request to the authored Loki query endpoint; unreachable private
/// endpoints are reported as a warning because operators may intentionally
/// require `port-forward` first.
#[derive(Debug, Default)]
pub(crate) struct EcsObservabilityCheck;

impl ObservabilityCheck for EcsObservabilityCheck {
    fn check(
        &self,
        deployment: &DeploymentRef,
        resources: &[Arc<dyn Resource>],
        timeout: Duration,
    ) -> anyhow::Result<ObservabilityCheckReport> {
        if timeout.is_zero() {
            anyhow::bail!("observability check timeout must be positive");
        }

        let coordinates = EcsOperationCoordinates::read(deployment)?;
        let artifact_count = validate_artifacts(deployment)?;
        let alloy_services = validate_realized_resources(deployment, resources)?;
        let alloy_count = validate_alloy_rendering(deployment, &coordinates, &alloy_services)?;
        let live = probe_loki_readiness(coordinates.loki_query_url(), timeout)?;

        Ok(ObservabilityCheckReport {
            checks: vec![
                ObservabilityCheckOutcome {
                    name: "ecs-alloy",
                    status: ObservabilityCheckStatus::Pass,
                    detail: format!("{alloy_count} task-scoped Alloy configurations render"),
                },
                ObservabilityCheckOutcome {
                    name: "ecs-artifacts",
                    status: ObservabilityCheckStatus::Pass,
                    detail: format!(
                        "{artifact_count} dashboard and alert artifacts satisfy the style contract"
                    ),
                },
                ObservabilityCheckOutcome {
                    name: "ecs-resources",
                    status: ObservabilityCheckStatus::Pass,
                    detail: format!(
                        "the artifact tree and {alloy_count} definition-declared Alloy parameters are realized"
                    ),
                },
                live,
            ],
        })
    }
}

fn validate_artifacts(deployment: &DeploymentRef) -> anyhow::Result<usize> {
    let content = deployment.dir.join("observability");
    let artifacts = load_observability_artifacts(&content)?;
    let mut dashboard_count = 0usize;
    let mut alert_count = 0usize;
    for artifact in &artifacts {
        let path = content.join(&artifact.key);
        if artifact.key.starts_with("dashboards/") {
            DashboardValidator::validate_str(&path, &artifact.content)?;
            dashboard_count += 1;
        } else if artifact.key.starts_with("alerts/") {
            AlertRuleValidator::validate_str(&path, &artifact.content, &deployment.dir)?;
            alert_count += 1;
        }
    }
    if dashboard_count == 0 || alert_count == 0 {
        anyhow::bail!(
            "ECS observability content must contain at least one dashboard and one alert bundle"
        );
    }
    Ok(artifacts.len())
}

fn validate_alloy_rendering(
    deployment: &DeploymentRef,
    coordinates: &EcsOperationCoordinates,
    services: &[String],
) -> anyhow::Result<usize> {
    let context = AlloyRenderContext {
        project_name: &deployment.name,
        environment: coordinates.environment(),
        cluster_name: coordinates.cluster(),
    };
    for service in services {
        let rendered = render_alloy_config(service, &context);
        let path = PathBuf::from(format!("alloy/{service}.alloy"));
        AlloyConfigValidator::validate_scrape_jobs(&path, &rendered, &["tokeira"])?;
        for required in [
            "TASK_ARN_PLACEHOLDER",
            "loki.source.docker",
            "http://tokeira-mimir:9009/api/v1/push",
            "http://tokeira-loki:3100/loki/api/v1/push",
        ] {
            if !rendered.contains(required) {
                anyhow::bail!(
                    "rendered Alloy configuration for `{service}` is missing `{required}`"
                );
            }
        }
    }
    Ok(services.len())
}

fn validate_realized_resources(
    deployment: &DeploymentRef,
    resources: &[Arc<dyn Resource>],
) -> anyhow::Result<Vec<String>> {
    let actual = resources
        .iter()
        .map(|resource| (resource.resource_type().0, resource.resource_id().0))
        .collect::<BTreeSet<_>>();
    if !actual.contains(&(
        ARTIFACTS_TYPE.to_owned(),
        "observability:artifacts".to_owned(),
    )) {
        anyhow::bail!("the realized ECS definition contains no observability artifact tree");
    }

    let prefix = format!("ssm-parameter:/{}/alloy/sidecar/", deployment.name);
    let services = actual
        .iter()
        .filter(|(resource_type, _)| resource_type == ALLOY_CONFIG_TYPE)
        .map(|(_, id)| {
            id.strip_prefix(&prefix)
                .filter(|service| !service.is_empty())
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "realized Alloy parameter `{id}` is outside deployment `{}`",
                        deployment.name
                    )
                })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    if services.is_empty() {
        anyhow::bail!("the realized ECS definition contains no Alloy configuration parameters");
    }
    Ok(services)
}

fn probe_loki_readiness(
    base_url: &str,
    timeout: Duration,
) -> anyhow::Result<ObservabilityCheckOutcome> {
    let url = loki_readiness_url(base_url)?;
    let endpoint = url.to_string();

    // `ObservabilityCheck` predates asynchronous reachability. Run the async
    // HTTP client on a short-lived dedicated runtime so this synchronous
    // capability never nests a runtime inside the provisioner's Tokio
    // executor. The reqwest total timeout bounds connect, response, and body
    // work; joining therefore cannot outlive the operator's stated budget.
    let result = std::thread::spawn(move || -> anyhow::Result<reqwest::StatusCode> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build Loki readiness runtime")?;
        runtime.block_on(async move {
            let client = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .context("build Loki readiness client")?;
            Ok(client.get(&endpoint).send().await?.status())
        })
    })
    .join()
    .map_err(|_| anyhow::anyhow!("Loki readiness worker panicked"))?;

    Ok(match result {
        Ok(status) if status.is_success() => ObservabilityCheckOutcome {
            name: "ecs-loki-readiness",
            status: ObservabilityCheckStatus::Pass,
            detail: format!("{url} returned {status}"),
        },
        Ok(status) => ObservabilityCheckOutcome {
            name: "ecs-loki-readiness",
            status: ObservabilityCheckStatus::Warn,
            detail: format!("{url} returned {status}"),
        },
        Err(error) => ObservabilityCheckOutcome {
            name: "ecs-loki-readiness",
            status: ObservabilityCheckStatus::Warn,
            detail: format!("{url} was not reachable within {timeout:?}: {error}"),
        },
    })
}

fn loki_readiness_url(base_url: &str) -> anyhow::Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(base_url)
        .map_err(|error| anyhow::anyhow!("invalid observability.loki_query_url: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("observability.loki_query_url must use http or https");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("observability.loki_query_url must not contain credentials");
    }
    let base_path = url.path().trim_end_matches('/').to_owned();
    url.set_path(&format!("{base_path}/ready"));
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

/// Reusable author input for the shipped observability artifact tree. The
/// content is read from the definition source directory at apply time; the
/// artifacts bucket is the declared dependency.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityArtifacts {}

impl Kind<ObservabilityArtifactsResource> for ObservabilityArtifacts {
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<ObservabilityArtifactsResource, KindError> {
        let bucket = placement
            .dependencies
            .iter()
            .find(|id| id.0.starts_with("s3-") && id.0.ends_with("-observability-artifacts"))
            .cloned()
            .ok_or_else(|| {
                KindError::new(
                    "the observability artifacts need their bucket declared as a dependency",
                )
            })?;
        Ok(ObservabilityArtifactsResource {
            bucket_dependency: bucket,
            content_dir: placement.definition_dir.join("observability"),
            module: placement.module.clone(),
        })
    }
}

/// The artifact tree as one resource: every shipped dashboard plus the
/// alert rules, uploaded to the artifacts bucket, fenced by one digest.
#[derive(Debug)]
pub struct ObservabilityArtifactsResource {
    bucket_dependency: ResourceId,
    content_dir: PathBuf,
    module: String,
}

impl ObservabilityArtifactsResource {
    fn objects(&self) -> Result<Vec<S3Object>, tokeira_iac::IacError> {
        Ok(load_observability_artifacts(&self.content_dir)?
            .into_iter()
            .map(|artifact| S3Object {
                bucket_dependency: self.bucket_dependency.clone(),
                key: artifact.key,
                content: artifact.content,
                content_type: artifact.content_type.to_owned(),
                module: self.module.clone(),
            })
            .collect())
    }

    fn digest(&self) -> Result<String, tokeira_iac::IacError> {
        let mut hasher = sha2::Sha256::new();
        for artifact in load_observability_artifacts(&self.content_dir)? {
            hasher.update(artifact.key.as_bytes());
            hasher.update([0]);
            hasher.update(artifact.content.as_bytes());
            hasher.update([0]);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    fn state(&self, bucket_name: String, digest: String, keys: Vec<String>) -> ResourceState {
        let now = chrono_now();
        ResourceState {
            resource_type: Resource::resource_type(self),
            physical_id: self.resource_id().0,
            properties: serde_json::json!({
                "bucket_name": bucket_name,
                "content_digest": digest,
                "keys": keys,
            }),
            dependencies: Resource::dependencies(self),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
    }

    fn recorded_keys(current: &ResourceState) -> BTreeSet<String> {
        current
            .properties
            .get("keys")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default()
    }

    fn bucket_name(
        &self,
        current: Option<&ResourceState>,
        ctx: &ProvisionContext,
    ) -> Result<String, tokeira_iac::IacError> {
        if let Some(bucket_name) = current
            .and_then(|state| state.properties.get("bucket_name"))
            .and_then(|value| value.as_str())
        {
            return Ok(bucket_name.to_owned());
        }

        // State written before the aggregate recorded its bucket contains only
        // the digest and keys. Resolving the declared dependency preserves
        // destroy compatibility for those already-applied deployments.
        ctx.get_resource_state(&self.bucket_dependency)?
            .properties
            .get("bucket_name")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned)
            .ok_or_else(|| tokeira_iac::IacError::StateNotFound("bucket_name missing".into()))
    }
}

fn chrono_now() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[async_trait::async_trait]
impl Resource for ObservabilityArtifactsResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new(ARTIFACTS_TYPE)
    }

    fn resource_id(&self) -> ResourceId {
        ResourceId("observability:artifacts".to_owned())
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        vec![self.bucket_dependency.clone()]
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, tokeira_iac::IacError> {
        let bucket_name = self.bucket_name(None, ctx)?;
        let objects = self.objects()?;
        let mut keys = Vec::with_capacity(objects.len());
        // Each object rides the provider's own upsert; the set is fenced as
        // one resource with one digest.
        for object in &objects {
            object.create(ctx).await?;
            keys.push(object.key.clone());
        }
        Ok(self.state(bucket_name, self.digest()?, keys))
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, tokeira_iac::IacError> {
        let bucket_name = self.bucket_name(Some(current), ctx)?;
        let objects = self.objects()?;
        let desired_keys = objects
            .iter()
            .map(|object| object.key.clone())
            .collect::<BTreeSet<_>>();

        for object in &objects {
            object.create(ctx).await?;
        }

        // Remove keys no longer present in the shipped tree before advancing
        // state. A failed delete leaves the prior key recorded, so retry still
        // owns and attempts cleanup of the stale object.
        for key in Self::recorded_keys(current).difference(&desired_keys) {
            self.object_for_delete(key.clone())
                .delete_from_bucket(&bucket_name, ctx)
                .await?;
        }

        Ok(self.state(
            bucket_name,
            self.digest()?,
            desired_keys.into_iter().collect(),
        ))
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), tokeira_iac::IacError> {
        let bucket_name = self.bucket_name(Some(current), ctx)?;
        for key in Self::recorded_keys(current) {
            self.object_for_delete(key)
                .delete_from_bucket(&bucket_name, ctx)
                .await?;
        }
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &ProvisionContext,
    ) -> Result<DescribeResult, tokeira_iac::IacError> {
        // The tree's presence is a function of its bucket and content
        // digest, both engine-recorded; the per-object provider calls carry
        // their own failures at apply time.
        Ok(DescribeResult::Unsupported)
    }

    fn diff(&self, current: &ResourceState, _ctx: &ProvisionContext) -> InternalChange {
        let recorded = current
            .properties
            .get("content_digest")
            .and_then(|value| value.as_str())
            .map(str::to_owned);
        let desired = self.digest().ok();
        if desired.is_some() && recorded == desired {
            InternalChange::NoChange {
                resource_id: self.resource_id(),
            }
        } else {
            InternalChange::Update {
                resource_id: self.resource_id(),
                resource_type: Resource::resource_type(self),
                details: Vec::new(),
            }
        }
    }

    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        const UPSERT: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::{create,update} — upload the shipped observability artifact \
             tree to the artifacts bucket"
        ));
        const DELETE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::delete — remove the recorded artifact objects from the \
             artifacts bucket"
        ));
        tokeira_aws::resources::generated_content_semantics(ctx.kind, UPSERT, DELETE)
    }
}

impl ObservabilityArtifactsResource {
    fn object_for_delete(&self, key: String) -> S3Object {
        S3Object {
            bucket_dependency: self.bucket_dependency.clone(),
            key,
            content: String::new(),
            content_type: String::new(),
            module: self.module.clone(),
        }
    }
}

/// Reusable author input for one service's rendered Alloy sidecar
/// configuration parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlloyConfig {
    /// Canonical service name the sidecar scrapes.
    pub(crate) service: String,
    /// Operator-authored deployment environment retained in observability labels.
    pub(crate) environment: String,
    /// Operator-authored ECS cluster retained in metric labels.
    pub(crate) cluster: String,
}

impl Kind<AlloyConfigResource> for AlloyConfig {
    fn realize(&self, placement: &PlacementContext) -> Result<AlloyConfigResource, KindError> {
        let render_context = AlloyRenderContext {
            project_name: &placement.deployment_id,
            environment: &self.environment,
            cluster_name: &self.cluster,
        };
        Ok(AlloyConfigResource {
            inner: SsmParameterResource {
                name: format!(
                    "/{}/alloy/sidecar/{}",
                    placement.deployment_id, self.service
                ),
                value: render_alloy_config(&self.service, &render_context),
                secure: true,
                module: placement.module.clone(),
            },
        })
    }
}

/// A rendered Alloy configuration parameter: a named wrapper over the
/// generic SSM parameter, carrying the platform kind's own type name (the
/// generic `tokeira_aws` namespace owns `"SsmParameter"`).
#[derive(Debug)]
pub struct AlloyConfigResource {
    inner: SsmParameterResource,
}

#[async_trait::async_trait]
impl Resource for AlloyConfigResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new(ALLOY_CONFIG_TYPE)
    }

    fn resource_id(&self) -> ResourceId {
        self.inner.resource_id()
    }

    fn dependencies(&self) -> Vec<ResourceId> {
        self.inner.dependencies()
    }

    fn module(&self) -> &str {
        self.inner.module()
    }

    fn validate_input(&self) -> Result<(), String> {
        self.inner.validate_input()
    }

    fn desired_manifest(&self) -> serde_json::Value {
        self.inner.desired_manifest()
    }

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, tokeira_iac::IacError> {
        self.inner.create(ctx).await
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, tokeira_iac::IacError> {
        self.inner.update(current, ctx).await
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), tokeira_iac::IacError> {
        self.inner.delete(current, ctx).await
    }

    async fn describe(
        &self,
        ctx: &ProvisionContext,
    ) -> Result<DescribeResult, tokeira_iac::IacError> {
        self.inner.describe(ctx).await
    }

    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange {
        self.inner.diff(current, ctx)
    }

    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        self.inner.change_semantics(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use super::*;

    fn recorded_state(properties: serde_json::Value) -> ResourceState {
        ResourceState {
            resource_type: ResourceType::new(ARTIFACTS_TYPE),
            physical_id: "observability:artifacts".to_owned(),
            properties,
            dependencies: Vec::new(),
            created_at: "then".to_owned(),
            updated_at: "then".to_owned(),
            module: "observability".to_owned(),
        }
    }

    fn artifact_resource() -> ObservabilityArtifactsResource {
        ObservabilityArtifactsResource {
            bucket_dependency: ResourceId("s3-demo-observability-artifacts".to_owned()),
            content_dir: PathBuf::from("unused"),
            module: "observability".to_owned(),
        }
    }

    #[test]
    fn aggregate_state_retains_bucket_and_sorted_key_ownership() {
        let resource = artifact_resource();
        let state = resource.state(
            "demo-artifacts".to_owned(),
            "digest".to_owned(),
            vec!["dashboards/workflows.json".to_owned()],
        );

        assert_eq!(state.properties["bucket_name"], "demo-artifacts");
        assert_eq!(
            ObservabilityArtifactsResource::recorded_keys(&state),
            BTreeSet::from(["dashboards/workflows.json".to_owned()])
        );
    }

    #[test]
    fn old_aggregate_state_resolves_bucket_from_declared_dependency() {
        let resource = artifact_resource();
        let old_state = recorded_state(serde_json::json!({
            "content_digest": "old",
            "keys": ["alerts/observability-alerts.yaml"],
        }));
        let mut ctx = ProvisionContext::new("demo", HashMap::new());
        ctx.state.resources.insert(
            resource.bucket_dependency.clone(),
            recorded_state(serde_json::json!({"bucket_name": "demo-artifacts"})),
        );

        assert_eq!(
            resource
                .bucket_name(Some(&old_state), &ctx)
                .expect("legacy state resolves through its dependency"),
            "demo-artifacts"
        );
    }

    #[test]
    fn alloy_parameter_preserves_authored_identity_and_service_connect_names() {
        let placement = PlacementContext {
            deployment_id: "author-project".to_owned(),
            deployment_dir: PathBuf::from("."),
            definition_dir: PathBuf::from("."),
            module: "observability".to_owned(),
            logical_id: "alloy-runtime".to_owned(),
            dependencies: Vec::new(),
            dependency_content: BTreeMap::new(),
            tags: BTreeMap::new(),
        };
        let resource = AlloyConfig {
            service: "tokeira-runtime".to_owned(),
            environment: "production".to_owned(),
            cluster: "author-cluster".to_owned(),
        }
        .realize(&placement)
        .expect("Alloy config realizes");

        assert_eq!(
            resource.inner.name,
            "/author-project/alloy/sidecar/tokeira-runtime"
        );
        assert!(resource.inner.secure);
        assert!(
            resource
                .inner
                .value
                .contains("environment = \"production\"")
        );
        assert!(
            resource
                .inner
                .value
                .contains("cluster = \"author-cluster\"")
        );
        assert!(
            resource
                .inner
                .value
                .contains("http://tokeira-mimir:9009/api/v1/push")
        );
        assert!(
            resource
                .inner
                .value
                .contains("http://tokeira-loki:3100/loki/api/v1/push")
        );
        assert!(!resource.inner.value.contains("tokeira.local"));
    }

    #[test]
    fn loki_readiness_uses_the_authored_endpoint() {
        let endpoint = loki_readiness_url("https://loki.example/base/?ignored=yes#fragment")
            .expect("readiness endpoint derives");

        assert_eq!(endpoint.as_str(), "https://loki.example/base/ready");
    }

    #[test]
    fn loki_readiness_url_must_not_embed_credentials() {
        let error = loki_readiness_url("http://operator:secret@127.0.0.1:3100")
            .expect_err("credentials in a diagnostic URL must be refused");

        assert!(error.to_string().contains("must not contain credentials"));
    }
}
