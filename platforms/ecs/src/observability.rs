//! The platform-owned observability content: the artifact tree shipped
//! beside the definition, uploaded as one resource, and the rendered Alloy
//! sidecar configuration parameters.
//!
//! Follows the Compose observability pattern: content is read from the
//! definition source directory (`placement.definition_dir`), so a retained
//! revision renders its own content; the artifact set is one resource — one
//! content tree with one digest — mirroring how Compose treats its rendered
//! configuration files.

use std::path::PathBuf;

use serde::Deserialize;
use sha2::Digest;
use tokeira_aws::resources::{s3_object::S3Object, ssm_parameter::SsmParameterResource};
use tokeira_ecs::modules::observability::{load_observability_artifacts, render_alloy_config};
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
pub const ARTIFACTS_TYPE: &str = "ObservabilityArtifacts";
/// Author-visible name of the rendered Alloy parameter resource type.
pub const ALLOY_CONFIG_TYPE: &str = "AlloyConfig";

/// The platform package's namespace word.
pub const NAMESPACE: &str = "tokeira_ecs_deployment";

/// The package's author-visible kind names.
pub const KINDS: &[&str] = &[ALLOY_CONFIG_TYPE, ARTIFACTS_TYPE];

/// Decode one authored kind of this namespace; `None` when the name is not
/// ours.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
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
pub fn namespace() -> Namespace {
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

    fn state(&self, digest: String, keys: Vec<String>) -> ResourceState {
        let now = chrono_now();
        ResourceState {
            resource_type: Resource::resource_type(self),
            physical_id: self.resource_id().0,
            properties: serde_json::json!({
                "content_digest": digest,
                "keys": keys,
            }),
            dependencies: Resource::dependencies(self),
            created_at: now.clone(),
            updated_at: now,
            module: self.module.clone(),
        }
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
        let objects = self.objects()?;
        let mut keys = Vec::with_capacity(objects.len());
        // Each object rides the provider's own upsert; the set is fenced as
        // one resource with one digest.
        for object in &objects {
            object.create(ctx).await?;
            keys.push(object.key.clone());
        }
        Ok(self.state(self.digest()?, keys))
    }

    async fn update(
        &self,
        _current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, tokeira_iac::IacError> {
        self.create(ctx).await
    }

    async fn delete(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<(), tokeira_iac::IacError> {
        // Delete per recorded key through the per-object resource so bucket
        // resolution stays in one place.
        let keys: Vec<String> = current
            .properties
            .get("keys")
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default();
        for key in keys {
            let object = S3Object {
                bucket_dependency: self.bucket_dependency.clone(),
                key,
                content: String::new(),
                content_type: String::new(),
                module: self.module.clone(),
            };
            object.delete(current, ctx).await?;
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

/// Reusable author input for one service's rendered Alloy sidecar
/// configuration parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlloyConfig {
    /// Canonical service name the sidecar scrapes.
    pub service: String,
    /// AWS region.
    pub region: String,
}

impl Kind<AlloyConfigResource> for AlloyConfig {
    fn realize(&self, placement: &PlacementContext) -> Result<AlloyConfigResource, KindError> {
        let config = tokeira_ecs::EcsConfig {
            project_name: placement.deployment_id.clone(),
            region: self.region.clone(),
            ..tokeira_ecs::EcsConfig::default()
        };
        Ok(AlloyConfigResource {
            inner: SsmParameterResource {
                name: format!(
                    "/{}/alloy/sidecar/{}",
                    placement.deployment_id, self.service
                ),
                value: render_alloy_config(&self.service, &config),
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
