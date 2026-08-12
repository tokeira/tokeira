//! Platform-owned role resources: named wrappers over the generic
//! [`IamRole`].
//!
//! Kind names must equal the realized `resource_type()` and stay unique
//! across namespaces; the generic `tokeira_aws` namespace owns `"IamRole"`.
//! The platform's derived-policy roles (task, execution, observability
//! storage) therefore realize this wrapper, which carries the kind's own
//! type name and delegates every behaviour to the inner role — the same
//! pattern as the remote-state bucket wrapper.

use tokeira_aws::resources::iam_role::IamRole;
use tokeira_iac::{
    DescribeResult, InternalChange, ProvisionContext, Resource, ResourceId, ResourceState,
    ResourceType,
};

#[derive(Debug)]
pub(crate) struct PlatformRoleResource {
    inner: IamRole,
    kind_name: &'static str,
}

impl PlatformRoleResource {
    pub(crate) fn wrap(inner: IamRole, kind_name: &'static str) -> Self {
        Self { inner, kind_name }
    }
}

#[async_trait::async_trait]
impl Resource for PlatformRoleResource {
    fn resource_type(&self) -> ResourceType {
        ResourceType::new(self.kind_name)
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        // Every platform role's one consumable fact.
        &["role_arn"]
    }

    fn validate_input(&self) -> Result<(), String> {
        self.inner.validate_input()
    }

    fn desired_manifest(&self) -> serde_json::Value {
        self.inner.desired_manifest()
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
