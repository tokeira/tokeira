use async_trait::async_trait;
use tokeira_aws::resources::s3_bucket::{S3Bucket, S3BucketConfig};
use tokeira_iac::{
    DescribeResult, IacError, InternalChange, Module, ModuleContext, ProvisionContext, Resource,
    ResourceId, ResourceState, ResourceType,
};

use crate::{config::EcsConfig, state_bucket_name, state_key_prefix};

#[derive(Debug, Clone)]
pub struct RemoteStateModule {
    config: EcsConfig,
}

impl RemoteStateModule {
    pub fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for RemoteStateModule {
    fn name(&self) -> &str {
        "remote-state"
    }

    fn dependencies(&self) -> Vec<&str> {
        Vec::new()
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let rctx = tokeira_aws::ResourceContext {
            project: self.config.project_name.clone(),
            region: self.config.region.clone(),
            tags: self.config.tags.clone(),
        };
        let inner = S3Bucket::new(
            state_bucket_name(&self.config),
            S3BucketConfig {
                versioning: true,
                module: "remote-state".into(),
                key_prefix: Some(state_key_prefix(&self.config)),
            },
            &rctx,
        );
        Ok(vec![Box::new(RemoteStateBucket { inner })])
    }
}

#[derive(Debug)]
struct RemoteStateBucket {
    inner: S3Bucket,
}

#[async_trait]
impl Resource for RemoteStateBucket {
    fn change_semantics(
        &self,
        ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        const DELETE: tokeira_iac::Citation = tokeira_iac::Citation::code(concat!(
            module_path!(),
            "::delete — deliberately a no-op: the shared remote-state bucket \
             is never deleted; only the engine's record is retired"
        ));
        match ctx.kind {
            // The wrapper's one behavioural difference from the inner
            // S3Bucket is the preserving delete — every other arm is the
            // inner kind's own story.
            tokeira_iac::ChangeKind::Delete => tokeira_iac::ChangeSemantics {
                operation: tokeira_iac::Confidence::EngineFact {
                    value: tokeira_iac::LifecycleOperation::Deleted,
                    citation: DELETE,
                },
                replacement: tokeira_iac::Confidence::EngineFact {
                    value: tokeira_iac::ReplacementPolicy::NotRequired,
                    citation: DELETE,
                },
                disruption: tokeira_iac::Confidence::EngineFact {
                    value: tokeira_iac::Disruption::None,
                    citation: DELETE,
                },
                data_effect: tokeira_iac::Confidence::EngineFact {
                    value: tokeira_iac::DataEffect::Preserved,
                    citation: DELETE,
                },
                reversibility: tokeira_iac::Confidence::EngineFact {
                    value: tokeira_iac::Reversibility::Reversible,
                    citation: DELETE,
                },
                statement: None,
                provider_assigned: Vec::new(),
            },
            _ => self.inner.change_semantics(ctx),
        }
    }

    fn resource_type(&self) -> ResourceType {
        self.inner.resource_type()
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

    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        self.inner.create(ctx).await
    }

    async fn update(
        &self,
        current: &ResourceState,
        ctx: &ProvisionContext,
    ) -> Result<ResourceState, IacError> {
        self.inner.update(current, ctx).await
    }

    async fn delete(
        &self,
        _current: &ResourceState,
        _ctx: &ProvisionContext,
    ) -> Result<(), IacError> {
        tracing::info!("shared remote-state bucket is not deleted");
        Ok(())
    }

    async fn describe(&self, ctx: &ProvisionContext) -> Result<DescribeResult, IacError> {
        self.inner.describe(ctx).await
    }

    fn diff(&self, current: &ResourceState, ctx: &ProvisionContext) -> InternalChange {
        self.inner.diff(current, ctx)
    }
}
