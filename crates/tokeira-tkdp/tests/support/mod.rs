use tokeira_platform::kind::{DecodedKind, PlacementContext, RealizedResource};

#[derive(Debug)]
pub(crate) struct FixtureResource {
    resource_type: &'static str,
    outputs: &'static [&'static str],
    desired: serde_json::Value,
}

impl FixtureResource {
    pub(crate) fn new(
        resource_type: &'static str,
        outputs: &'static [&'static str],
        desired: serde_json::Value,
    ) -> Self {
        Self {
            resource_type,
            outputs,
            desired,
        }
    }
}

// `async-trait` is intentionally not added to this frontend crate merely for
// an integration fixture; these methods spell its object-safe expansion.
#[allow(clippy::manual_async_fn)]
impl tokeira_iac::Resource for FixtureResource {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new(self.resource_type)
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        self.outputs
    }

    fn desired_manifest(&self) -> serde_json::Value {
        self.desired.clone()
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        tokeira_iac::ResourceId("fixture".to_string())
    }

    fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        "fixture"
    }

    fn create<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _ctx: &'life1 tokeira_iac::ProvisionContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<tokeira_iac::ResourceState, tokeira_iac::IacError>,
                > + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("frontend fixtures never execute resource lifecycle") })
    }

    fn update<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _current: &'life1 tokeira_iac::ResourceState,
        _ctx: &'life2 tokeira_iac::ProvisionContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<tokeira_iac::ResourceState, tokeira_iac::IacError>,
                > + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("frontend fixtures never execute resource lifecycle") })
    }

    fn delete<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _current: &'life1 tokeira_iac::ResourceState,
        _ctx: &'life2 tokeira_iac::ProvisionContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), tokeira_iac::IacError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("frontend fixtures never execute resource lifecycle") })
    }

    fn describe<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _ctx: &'life1 tokeira_iac::ProvisionContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<tokeira_iac::DescribeResult, tokeira_iac::IacError>,
                > + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("frontend fixtures never execute resource lifecycle") })
    }

    fn diff(
        &self,
        _current: &tokeira_iac::ResourceState,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> tokeira_iac::InternalChange {
        unreachable!("frontend fixtures never execute resource lifecycle")
    }

    fn change_semantics(
        &self,
        _ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        tokeira_iac::ChangeSemantics::default()
    }
}

pub(crate) fn desired_manifest(kind: &DecodedKind) -> serde_json::Value {
    let placement = PlacementContext {
        deployment_id: "test".to_string(),
        deployment_dir: std::path::PathBuf::new(),
        definition_dir: std::path::PathBuf::new(),
        module: String::new(),
        logical_id: String::new(),
        dependencies: Vec::new(),
        dependency_content: Default::default(),
        tags: Default::default(),
    };
    match kind.realize(&placement).expect("fixture kind realizes") {
        RealizedResource::Infra(resource) => resource.desired_manifest(),
        RealizedResource::Service(_) => panic!("fixture kind must realize infrastructure"),
    }
}
