//! The platform-owned observability content: the artifact tree shipped
//! beside the definition, uploaded as one resource, and the rendered Alloy
//! sidecar configuration parameters.
//!
//! Follows the Compose observability pattern: content is read from the
//! definition source directory (`placement.definition_dir`), so a retained
//! revision renders its own content; the artifact set is one resource — one
//! content tree with one digest — mirroring how Compose treats its rendered
//! configuration files.

use std::{collections::BTreeSet, path::PathBuf};

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
use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

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
}
