use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{
        ArtifactCatalog, ArtifactClass, ArtifactUse, CanonicalDocument, ContentIdentity,
        ContentIdentitySet, DeliveryKey, DesiredContent, DesiredDocument, InspectionRenderRequest,
        InspectionRenderer, InspectionSpec, OperationalArtifactReceipt,
        OperationalArtifactReceipts, OperationalArtifactRequest, OperationalArtifactStage,
        PlatformArtifact, RelativeArtifactPath,
    },
    author::{AuthorArgument, AuthorHandle, AuthorNode, AuthorResult, AuthorValue},
    binding::{Platform, PlatformBinding, StateBinding, StatePolicy},
    catalog::{
        DeliveryProjection, HealthDeclaration, ImageCatalog, ImageSelection, KindRegistration,
        KindSet, PlacementContext, PlacementDeclaration, PlatformService, ProviderDelivery,
        ProviderExecution, ProviderKind, ProviderKindCatalog, ProviderSet, ServiceCatalog,
    },
    config::{ConfigContract, PlatformConfig},
    context::{
        ContextArgument, ContextContract, ContextProjection, InvocationContext, PlatformContext,
    },
    definition::{
        ConfigurationIdentity, DefinitionEngine, DefinitionFrontend, DefinitionRequest,
        DefinitionSource, DefinitionSourceName, EvaluatedDefinition, FrontendOutput,
        FrontendSource, RelativeDefinitionPath,
    },
    error::{
        ConfigError, ContextError, DeliveryError, FrontendDiagnostic, GraphError,
        InspectionRenderError, OpsError, VerificationFinding,
    },
    graph::{DeploymentGraphBuilder, WorkloadDeclaration, WritebackValue},
    ops::{
        OperationInvocation, OperationKind, OperationOutput, OperationRegistration,
        OperationRequest, OperationalEndpoint, PlatformOps, ProviderOperation, ServiceOps,
        SessionPlan,
    },
    projection::{
        FrameworkDeployment, no_change_issue_outcome, realize_resources, replace_selected_state,
        resolve_writeback,
    },
    selection::{SelectionDirection, select_modules},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestConfig {
    storage: TestStorage,
    replicas: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum TestStorage {
    Memory,
    Dsql { region: String },
}

impl PlatformConfig for TestConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.replicas == 0 {
            return Err(ConfigError::validation(
                "replicas must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
enum TestContextValue {
    Anchor(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestContext {
    project: String,
}

impl PlatformContext for TestContext {
    type Value = TestContextValue;

    fn fields() -> &'static [&'static str] {
        &["project_name"]
    }

    fn methods() -> &'static [&'static str] {
        &["anchor"]
    }

    fn field(&self, name: &str) -> Result<ContextProjection<Self::Value>, ContextError> {
        match name {
            "project_name" => Ok(ContextProjection::Value(AuthorNode::string(&self.project))),
            _ => Err(ContextError::new(format!("unknown context field `{name}`"))),
        }
    }

    fn call(
        &self,
        method: &str,
        args: &[ContextArgument<Self::Value>],
    ) -> Result<ContextProjection<Self::Value>, ContextError> {
        match (method, args) {
            (
                "anchor",
                [
                    ContextArgument::Value(AuthorNode {
                        value: AuthorValue::String(suffix),
                        ..
                    }),
                ],
            ) => Ok(ContextProjection::Token(TestContextValue::Anchor(format!(
                "{}:{suffix}",
                self.project
            )))),
            ("anchor", _) => Err(ContextError::new("anchor expects one string")),
            _ => Err(ContextError::new(format!(
                "unknown context method `{method}`"
            ))),
        }
    }
}

fn context_from_invocation(input: &InvocationContext) -> Result<TestContext, ContextError> {
    Ok(TestContext {
        project: input.deployment_id.clone(),
    })
}

fn authoring_context() -> Result<TestContext, ContextError> {
    Ok(TestContext {
        project: "authoring".into(),
    })
}

#[derive(Debug, Clone)]
struct TestPlatform;

impl Platform for TestPlatform {
    type Config = TestConfig;
    type Context = TestContext;

    fn binding(&self) -> PlatformBinding<Self> {
        test_binding()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestKind {
    suffix: String,
    describes: bool,
    extra_dependency: Option<String>,
}

impl ProviderKind for TestKind {
    fn kind_name(&self) -> &'static str {
        "test-resource"
    }

    fn validate_input(&self) -> Result<(), crate::error::KindError> {
        if self.suffix.is_empty() {
            return Err(crate::error::KindError::new("suffix cannot be empty"));
        }
        Ok(())
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &["value"]
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({
            "suffix": self.suffix,
            "describes": self.describes,
            "extra_dependency": self.extra_dependency,
        })
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, crate::error::KindError> {
        let mut dependencies = placement.dependencies.clone();
        if let Some(extra) = &self.extra_dependency {
            dependencies.push(tokeira_iac::ResourceId(extra.clone()));
        }
        Ok(Box::new(TestResource {
            id: tokeira_iac::ResourceId(format!(
                "{}/{}-{}",
                placement.module, placement.logical_id, self.suffix
            )),
            module: placement.module.clone(),
            dependencies,
            describes: self.describes,
        }))
    }
}

#[derive(Debug)]
struct TestResource {
    id: tokeira_iac::ResourceId,
    module: String,
    dependencies: Vec<tokeira_iac::ResourceId>,
    describes: bool,
}

#[derive(Debug)]
struct ValidationProbeKind {
    valid: bool,
    realizations: Arc<AtomicUsize>,
}

impl ProviderKind for ValidationProbeKind {
    fn kind_name(&self) -> &'static str {
        "validation-probe"
    }

    fn validate_input(&self) -> Result<(), crate::error::KindError> {
        if self.valid {
            Ok(())
        } else {
            Err(crate::error::KindError::new("probe input is invalid"))
        }
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &[]
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::json!({ "valid": self.valid })
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, crate::error::KindError> {
        self.realizations.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TestResource {
            id: tokeira_iac::ResourceId(format!("{}/{}", placement.module, placement.logical_id)),
            module: placement.module.clone(),
            dependencies: placement.dependencies.clone(),
            describes: true,
        }))
    }
}

#[async_trait]
impl tokeira_iac::Resource for TestResource {
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new("test-resource")
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        self.id.clone()
    }

    fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
        self.dependencies.clone()
    }

    fn describes(&self) -> bool {
        self.describes
    }

    fn module(&self) -> &str {
        &self.module
    }

    async fn create(
        &self,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        Ok(resource_state(&self.id.0, &self.module, "created"))
    }

    async fn update(
        &self,
        current: &tokeira_iac::ResourceState,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::ResourceState, tokeira_iac::IacError> {
        Ok(current.clone())
    }

    async fn delete(
        &self,
        _current: &tokeira_iac::ResourceState,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<(), tokeira_iac::IacError> {
        Ok(())
    }

    async fn describe(
        &self,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> Result<tokeira_iac::DescribeResult, tokeira_iac::IacError> {
        Ok(tokeira_iac::DescribeResult::Unsupported)
    }

    fn diff(
        &self,
        _current: &tokeira_iac::ResourceState,
        _ctx: &tokeira_iac::ProvisionContext,
    ) -> tokeira_iac::InternalChange {
        tokeira_iac::InternalChange::NoChange {
            resource_id: self.id.clone(),
        }
    }
}

const TEST_REGISTRATIONS: &[KindRegistration] = &[KindRegistration::typed::<TestKind>(
    "test-resource",
    &["value"],
    None,
)];

fn test_kinds() -> KindSet {
    KindSet::new(vec![ProviderKindCatalog {
        provider: "test",
        entries: TEST_REGISTRATIONS,
    }])
    .expect("test catalog is valid")
}

fn test_binding() -> PlatformBinding<TestPlatform> {
    test_binding_with_providers(ProviderSet::default())
}

fn test_binding_with_providers(
    providers: ProviderSet<TestPlatform>,
) -> PlatformBinding<TestPlatform> {
    PlatformBinding::new(
        tokeira_orchestrator::PlatformId::new("test").expect("canonical platform id"),
        "state",
        ConfigContract::new(),
        ContextContract::new(context_from_invocation, authoring_context),
        test_kinds(),
        ServiceCatalog::default(),
        ArtifactCatalog::default(),
        ImageCatalog::default(),
        providers,
        StateBinding::new(StatePolicy::LocalCas),
        PlatformOps::default(),
        Vec::new(),
    )
    .expect("test binding is valid")
}

struct TestProviderExecution {
    deploy_platform: Arc<dyn tokeira_deploy_engine::Platform>,
}

impl std::fmt::Debug for TestProviderExecution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestProviderExecution")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ProviderExecution<TestPlatform> for TestProviderExecution {
    fn provider(&self) -> &str {
        "test"
    }

    fn deploy_platform(&self) -> Option<Arc<dyn tokeira_deploy_engine::Platform>> {
        Some(Arc::clone(&self.deploy_platform))
    }

    fn hydrate_config(&self, config: &TestConfig, _state: &tokeira_iac::InfraState) -> TestConfig {
        let mut hydrated = config.clone();
        hydrated.replicas += 1;
        hydrated
    }
}

#[derive(Debug)]
struct TestDeployPlatform;

#[async_trait]
impl tokeira_deploy_engine::Platform for TestDeployPlatform {
    async fn apply_manifests(
        &self,
        manifests: &[serde_json::Value],
    ) -> Result<usize, tokeira_deploy_engine::RuntimeError> {
        Ok(manifests.len())
    }
}

#[derive(Debug)]
struct CoupledService {
    manifest: serde_json::Value,
}

impl tokeira_deploy_engine::Service for CoupledService {
    fn name(&self) -> &str {
        "server"
    }

    fn module(&self) -> &str {
        "state"
    }

    fn dependencies(&self) -> Vec<&str> {
        Vec::new()
    }

    fn manifests(
        &self,
        _context: &tokeira_deploy_engine::ServiceContext,
    ) -> Result<Vec<serde_json::Value>, tokeira_deploy_engine::RuntimeError> {
        Ok(vec![self.manifest.clone()])
    }
}

#[derive(Debug)]
struct CoupledDelivery {
    key: DeliveryKey,
    credential: String,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl ProviderDelivery for CoupledDelivery {
    fn key(&self) -> &DeliveryKey {
        &self.key
    }

    fn canonicalize(&self, document: &DesiredDocument) -> Result<CanonicalDocument, DeliveryError> {
        Ok(CanonicalDocument {
            bytes: serde_json::to_vec(&document.value)
                .map_err(|error| DeliveryError::new(error.to_string()))?,
        })
    }

    fn realize(
        &self,
        declaration: &WorkloadDeclaration,
        _placement: &PlacementContext,
        content: &ContentIdentitySet,
    ) -> Result<DeliveryProjection, DeliveryError> {
        // Provider credentials are runtime authority, never desired content.
        // Keeping the field live in this implementation makes accidental use
        // visible to the secret-mutation property below.
        let _credential = &self.credential;
        let identities = content
            .iter()
            .map(|(use_, identity)| {
                serde_json::json!({
                    "artifact": use_.artifact,
                    "role": use_.role,
                    "domain": identity.domain,
                    "sha256": identity.sha256,
                })
            })
            .collect::<Vec<_>>();
        Ok(DeliveryProjection::Workload(Box::new(CoupledService {
            manifest: serde_json::json!({
                "service": declaration.service,
                "content": identities,
            }),
        })))
    }

    async fn materialize_operational(
        &self,
        request: OperationalArtifactRequest<'_>,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<OperationalArtifactReceipt, DeliveryError> {
        self.published
            .lock()
            .expect("publication recorder lock")
            .push(request.content.to_vec());
        Ok(OperationalArtifactReceipt {
            artifact: request.artifact.logical_id.clone(),
            provider_reference: format!("generated/{}", request.artifact.logical_id),
            identity: request.identity.clone(),
            consumers: request.artifact.consumers.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SemanticDocument {
    image: String,
    command: Vec<String>,
    replicas: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestPortTarget {
    remote_host: String,
    remote_port: u16,
    protocol: String,
    access_mode: String,
    default_local_port: u16,
}

static TEST_PORT_OPERATION: OperationRegistration<TestPortTarget> =
    OperationRegistration::new("test-provider", "port-forward");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestLogTarget {
    source: String,
}

static TEST_LOG_OPERATION: OperationRegistration<TestLogTarget> =
    OperationRegistration::new("test-provider", "logs");

#[derive(Debug)]
struct TestProviderOperation {
    operation: &'static str,
}

#[async_trait]
impl ProviderOperation for TestProviderOperation {
    fn provider(&self) -> &str {
        "test-provider"
    }

    fn operation(&self) -> &str {
        self.operation
    }

    fn validate_target(&self, target: &serde_json::Value) -> Result<(), OpsError> {
        match self.operation {
            "logs" => TEST_LOG_OPERATION.decode(target.clone()).map(|_| ()),
            "port-forward" => TEST_PORT_OPERATION
                .decode(target.clone())
                .and_then(|target| {
                    if target.remote_host.is_empty()
                        || target.remote_port == 0
                        || target.protocol.is_empty()
                        || target.access_mode.is_empty()
                        || target.default_local_port == 0
                    {
                        return Err(OpsError::InvalidTarget {
                            provider: self.provider().into(),
                            operation: self.operation.into(),
                            message: "endpoint fields and ports must be non-empty".into(),
                        });
                    }
                    Ok(())
                }),
            other => Err(OpsError::InvalidRegistration(format!(
                "unexpected test operation `{other}`"
            ))),
        }
    }

    async fn execute(
        &self,
        invocation: &OperationInvocation,
        _context: &tokeira_iac::ProvisionContext,
    ) -> Result<OperationOutput, OpsError> {
        match self.operation {
            "logs" => {
                let target = TEST_LOG_OPERATION.decode(invocation.request().target().clone())?;
                Ok(OperationOutput::Logs(vec![target.source]))
            }
            "port-forward" => {
                let target = TEST_PORT_OPERATION.decode(invocation.request().target().clone())?;
                let session = (target.access_mode != "published").then(|| SessionPlan {
                    program: "provider-session".into(),
                    arguments: vec![target.remote_host.clone(), target.remote_port.to_string()],
                });
                Ok(OperationOutput::PortForward {
                    endpoint: OperationalEndpoint {
                        local_host: "127.0.0.1".into(),
                        local_port: invocation.local_port().unwrap_or(target.default_local_port),
                        remote_host: target.remote_host,
                        remote_port: target.remote_port,
                        protocol: target.protocol,
                        access_mode: target.access_mode,
                    },
                    session,
                })
            }
            other => Err(OpsError::Provider {
                provider: self.provider().into(),
                operation: other.into(),
                message: "unexpected test operation".into(),
            }),
        }
    }
}

#[derive(Debug)]
struct TestInspectionRenderer {
    renders: Arc<AtomicUsize>,
    fail: bool,
}

impl InspectionRenderer<TestPlatform> for TestInspectionRenderer {
    fn key(&self) -> &str {
        "test-inspection"
    }

    fn render(
        &self,
        request: InspectionRenderRequest<'_, TestPlatform>,
    ) -> Result<Vec<u8>, InspectionRenderError> {
        self.renders.fetch_add(1, Ordering::SeqCst);
        if self.fail {
            return Err(InspectionRenderError::new("injected renderer failure"));
        }
        let content = request
            .content_for("server")
            .and_then(|identities| identities.iter().next())
            .map(|(_, identity)| identity.sha256.as_str())
            .unwrap_or("none");
        Ok(format!(
            "deployment={}\nmodules={}\ncontent={content}\n",
            request.invocation.deployment_id,
            request.graph.modules().len(),
        )
        .into_bytes())
    }
}

fn coupled_binding_with_inspection(
    content: DesiredContent,
    credential: String,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    inspection: Vec<InspectionSpec<TestPlatform>>,
) -> PlatformBinding<TestPlatform> {
    coupled_binding_with_capabilities(
        content,
        credential,
        published,
        PlatformOps::default(),
        Vec::new(),
        inspection,
    )
}

fn coupled_binding_with_capabilities(
    content: DesiredContent,
    credential: String,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    ops: PlatformOps,
    provider_operations: Vec<Arc<dyn ProviderOperation>>,
    inspection: Vec<InspectionSpec<TestPlatform>>,
) -> PlatformBinding<TestPlatform> {
    let delivery = DeliveryKey::new("test-delivery").expect("delivery key");
    PlatformBinding::new(
        tokeira_orchestrator::PlatformId::new("test").expect("platform id"),
        "state",
        ConfigContract::new(),
        ContextContract::new(context_from_invocation, authoring_context),
        test_kinds(),
        ServiceCatalog::new(vec![PlatformService {
            logical_id: "server".into(),
            image: ImageSelection {
                logical_id: "server-image".into(),
            },
            command: vec!["serve".into()],
            ports: Vec::new(),
            health: HealthDeclaration::default(),
            placement: PlacementDeclaration::default(),
            configuration: vec![ArtifactUse {
                artifact: "runtime-config".into(),
                role: "server-config".into(),
            }],
            delivery: delivery.clone(),
            document: DesiredDocument {
                schema: "test.service.v1".into(),
                value: serde_json::json!({"image": "server-image"}),
            },
        }]),
        ArtifactCatalog::new(vec![PlatformArtifact {
            logical_id: "runtime-config".into(),
            class: ArtifactClass::Operational,
            content,
            consumers: vec!["server".into()],
            delivery: delivery.clone(),
        }]),
        ImageCatalog::new(vec!["server-image".into()]),
        ProviderSet::with_capabilities(
            vec![Arc::new(CoupledDelivery {
                key: delivery,
                credential,
                published,
            })],
            Vec::new(),
            provider_operations,
        ),
        StateBinding::new(StatePolicy::LocalCas),
        ops,
        inspection,
    )
    .expect("coupled binding")
}

fn coupled_framework(
    content: DesiredContent,
    credential: String,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    deployment_dir: std::path::PathBuf,
) -> FrameworkDeployment<TestPlatform> {
    coupled_framework_with_inspection(content, credential, published, deployment_dir, Vec::new())
}

fn coupled_framework_with_inspection(
    content: DesiredContent,
    credential: String,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    deployment_dir: std::path::PathBuf,
    inspection: Vec<InspectionSpec<TestPlatform>>,
) -> FrameworkDeployment<TestPlatform> {
    let binding = coupled_binding_with_inspection(content, credential, published, inspection);
    coupled_framework_from_binding(binding, deployment_dir)
}

fn coupled_framework_from_binding(
    binding: PlatformBinding<TestPlatform>,
    deployment_dir: std::path::PathBuf,
) -> FrameworkDeployment<TestPlatform> {
    let mut graph = DeploymentGraphBuilder::with_catalogs(
        binding.services.identities(),
        binding.providers.delivery_keys(),
    )
    .require_bootstrap("state");
    let deployment = graph.deployment_handle();
    let state = graph
        .add_module(&deployment, "state".into(), Vec::new())
        .expect("state module");
    let service = binding
        .services
        .get("server")
        .expect("server catalog entry");
    graph
        .add_workload(
            &state,
            WorkloadDeclaration {
                service: service.logical_id.clone(),
                dependencies: service.placement.needs.clone(),
                desired_capacity: 1,
                delivery: service.delivery.clone(),
                document: service.document.clone(),
            },
        )
        .expect("workload");
    FrameworkDeployment::new(
        EvaluatedDefinition {
            config: TestConfig {
                storage: TestStorage::Memory,
                replicas: 1,
            },
            graph: graph.finish().expect("graph"),
            configuration_identity: ConfigurationIdentity::compute(
                &tokeira_orchestrator::DefinitionFormatId::new("test").expect("format"),
                b"definition",
            ),
        },
        binding,
        InvocationContext {
            deployment_id: "deployment".into(),
            deployment_uuid: uuid::Uuid::nil(),
            environment: None,
            region: None,
            account_id: None,
            deployment_dir,
        },
    )
    .expect("coupled framework")
}

fn coupled_manifest(framework: &FrameworkDeployment<TestPlatform>) -> serde_json::Value {
    framework
        .services("deployment", &BTreeMap::new())
        .expect("service projection")
        .remove(0)
        .manifests(&tokeira_deploy_engine::ServiceContext::default())
        .expect("service manifest")
        .remove(0)
}

fn complete_immediate<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let waker = std::task::Waker::noop();
    let mut task_context = std::task::Context::from_waker(waker);
    match std::future::Future::poll(future.as_mut(), &mut task_context) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("test future must complete without external I/O"),
    }
}

#[derive(Debug)]
struct TestDeploymentDir(std::path::PathBuf);

impl TestDeploymentDir {
    fn new(label: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("tokeira-platform-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("temporary deployment root");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestDeploymentDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn test_kind(suffix: impl Into<String>, describes: bool) -> Box<dyn ProviderKind> {
    Box::new(TestKind {
        suffix: suffix.into(),
        describes,
        extra_dependency: None,
    })
}

fn kind_node(
    suffix: impl Into<String>,
    describes: bool,
    extra_dependency: Option<String>,
) -> AuthorNode {
    AuthorNode::new(AuthorValue::Struct {
        name: "TestKind".into(),
        fields: vec![
            ("suffix".into(), AuthorNode::string(suffix)),
            (
                "describes".into(),
                AuthorNode::new(AuthorValue::Bool(describes)),
            ),
            (
                "extra_dependency".into(),
                AuthorNode::new(AuthorValue::Option(
                    extra_dependency.map(|value| Box::new(AuthorNode::string(value))),
                )),
            ),
        ],
    })
}

fn config_node(storage: &TestStorage, replicas: u16) -> AuthorNode {
    let storage = match storage {
        TestStorage::Memory => AuthorNode::new(AuthorValue::Enum {
            name: "TestStorage".into(),
            variant: "Memory".into(),
            body: crate::author::AuthorVariantBody::Unit,
        }),
        TestStorage::Dsql { region } => AuthorNode::new(AuthorValue::Enum {
            name: "TestStorage".into(),
            variant: "Dsql".into(),
            body: crate::author::AuthorVariantBody::Struct(vec![(
                "region".into(),
                AuthorNode::string(region),
            )]),
        }),
    };
    AuthorNode::new(AuthorValue::Struct {
        name: "TestConfig".into(),
        fields: vec![
            ("storage".into(), storage),
            (
                "replicas".into(),
                AuthorNode::new(AuthorValue::Integer(i128::from(replicas))),
            ),
        ],
    })
}

fn resource_state(id: &str, module: &str, value: &str) -> tokeira_iac::ResourceState {
    tokeira_iac::ResourceState {
        resource_type: tokeira_iac::ResourceType::new("test-resource"),
        physical_id: format!("physical-{id}"),
        properties: serde_json::json!({"value": value}),
        dependencies: Vec::new(),
        created_at: "created".into(),
        updated_at: "updated".into(),
        module: module.into(),
    }
}

#[derive(Debug, Clone)]
struct UnusedFrontend {
    format: tokeira_orchestrator::DefinitionFormatId,
}

#[derive(Debug, Clone)]
struct EchoFrontend {
    format: tokeira_orchestrator::DefinitionFormatId,
}

impl DefinitionFrontend<TestPlatform> for EchoFrontend {
    fn format(&self) -> &tokeira_orchestrator::DefinitionFormatId {
        &self.format
    }

    fn evaluate(
        &self,
        _source: FrontendSource<'_>,
        author: &mut crate::author::AuthorSession<TestPlatform>,
    ) -> Result<FrontendOutput, FrontendDiagnostic> {
        let AuthorResult::Handle(AuthorHandle::Deployment(deployment)) = author
            .associated("Deployment.new", Vec::new())
            .expect("standard associated function")
        else {
            panic!("Deployment.new must return a deployment handle");
        };
        author
            .call(
                AuthorHandle::Deployment(deployment.clone()),
                "module",
                vec![AuthorArgument::Value(AuthorNode::string("state"))],
            )
            .expect("bootstrap module");
        Ok(FrontendOutput {
            config: config_node(&TestStorage::Memory, 1),
            deployment,
        })
    }
}

impl DefinitionFrontend<TestPlatform> for UnusedFrontend {
    fn format(&self) -> &tokeira_orchestrator::DefinitionFormatId {
        &self.format
    }

    fn evaluate(
        &self,
        _source: FrontendSource<'_>,
        _author: &mut crate::author::AuthorSession<TestPlatform>,
    ) -> Result<FrontendOutput, FrontendDiagnostic> {
        unreachable!("verification tests do not invoke the frontend")
    }
}

fn verification_engine() -> DefinitionEngine<TestPlatform, UnusedFrontend> {
    DefinitionEngine::new(
        test_binding(),
        UnusedFrontend {
            format: tokeira_orchestrator::DefinitionFormatId::new("test")
                .expect("canonical format"),
        },
    )
}

proptest! {
    // Graph mutation is transactional with respect to ownership failures.
    // Feature: platform-builder-abstraction, Property 1: graph declarations preserve order and reject foreign handles
    #[test]
    fn property_1_graph_order_and_foreign_handles(count in 1_usize..24) {
        let mut graph = DeploymentGraphBuilder::new();
        let deployment = graph.deployment_handle();
        let mut foreign = DeploymentGraphBuilder::new();
        let foreign_deployment = foreign.deployment_handle();
        let foreign_module = foreign
            .add_module(&foreign_deployment, "foreign".into(), Vec::new())
            .expect("foreign graph is valid");

        let error = graph
            .add_module(&deployment, "rejected".into(), vec![foreign_module])
            .expect_err("foreign dependency must be rejected");
        prop_assert!(
            matches!(error, GraphError::ForeignHandle { kind: "module" }),
            "expected foreign module ownership error"
        );

        let mut expected = Vec::new();
        for index in 0..count {
            let name = format!("module-{index}");
            graph
                .add_module(&deployment, name.clone(), Vec::new())
                .expect("local module is accepted");
            expected.push(name);
        }
        let verified = graph.finish().expect("local graph is valid");
        let actual = verified
            .modules()
            .iter()
            .map(|module| module.name().to_string())
            .collect::<Vec<_>>();
        prop_assert_eq!(actual, expected);
    }

    // Completion agrees with the small uniqueness/catalog reference model.
    // Feature: platform-builder-abstraction, Property 2: finished graphs are exactly the well-formed graphs
    #[test]
    fn property_2_finish_matches_reference(
        count in 1_usize..20,
        duplicate in any::<bool>(),
        unknown_workload in any::<bool>(),
    ) {
        let services = BTreeSet::from(["known".to_string()]);
        let deliveries = BTreeSet::from(["test".to_string()]);
        let mut graph = DeploymentGraphBuilder::with_catalogs(services, deliveries);
        let deployment = graph.deployment_handle();
        let mut first = None;
        for index in 0..count {
            let name = if duplicate && index + 1 == count && count > 1 {
                "module-0".to_string()
            } else {
                format!("module-{index}")
            };
            let handle = graph
                .add_module(&deployment, name, Vec::new())
                .expect("owned module is accepted");
            if first.is_none() {
                first = Some(handle);
            }
        }
        if unknown_workload {
            graph
                .add_workload(
                    first.as_ref().expect("one module exists"),
                    WorkloadDeclaration {
                        service: "unknown".into(),
                        dependencies: Vec::new(),
                        desired_capacity: 1,
                        delivery: DeliveryKey::new("test").expect("valid key"),
                        document: crate::artifact::DesiredDocument {
                            schema: "test".into(),
                            value: serde_json::json!({}),
                        },
                    },
                )
                .expect("workload ownership is valid before completion");
        }
        let expected_valid = (!duplicate || count == 1) && !unknown_workload;
        prop_assert_eq!(graph.finish().is_ok(), expected_valid);
    }

    // Typed registration behavior is exactly Serde + provider validation + output admission.
    // Feature: platform-builder-abstraction, Property 3: typed kind admission is schema-total
    #[test]
    fn property_3_kind_admission_is_schema_total(
        suffix in "[a-z]{1,12}",
        describes in any::<bool>(),
        surplus in any::<bool>(),
        output in prop_oneof![Just("value".to_string()), "[a-z]{2,8}"],
    ) {
        let mut node = kind_node(&suffix, describes, None);
        if surplus {
            let AuthorValue::Struct { fields, .. } = &mut node.value else {
                unreachable!("helper always constructs a struct");
            };
            fields.push(("surplus".into(), AuthorNode::new(AuthorValue::Bool(true))));
        }
        let decoded = test_kinds().decode("test-resource", node);
        prop_assert_eq!(decoded.is_ok(), !surplus);
        if let Ok(kind) = decoded {
            let manifest = kind.desired_manifest();
            prop_assert_eq!(manifest["suffix"].as_str(), Some(suffix.as_str()));
            let mut graph = DeploymentGraphBuilder::new();
            let deployment = graph.deployment_handle();
            let module = graph
                .add_module(&deployment, "module".into(), Vec::new())
                .expect("module");
            let resource = graph
                .add_resource(&module, "resource".into(), kind, Vec::new())
                .expect("resource");
            prop_assert_eq!(resource.output(&output).is_ok(), output == "value");
        }
    }

    // Provider realization receives exact logical placement and prior physical dependency ids.
    // Feature: platform-builder-abstraction, Property 4: provider realization preserves logical placement
    #[test]
    fn property_4_realization_preserves_placement(count in 1_usize..18) {
        let mut graph = DeploymentGraphBuilder::new();
        let deployment = graph.deployment_handle();
        let module = graph
            .add_module(&deployment, "module".into(), Vec::new())
            .expect("module");
        let mut previous = None;
        for index in 0..count {
            let dependencies = previous.iter().cloned().collect();
            let handle = graph
                .add_resource(
                    &module,
                    format!("resource-{index}"),
                    test_kind(index.to_string(), true),
                    dependencies,
                )
                .expect("resource");
            previous = Some(handle);
        }
        let graph = graph.finish().expect("valid graph");
        let realized = realize_resources(&graph, "deployment", &BTreeMap::new())
            .expect("realization succeeds");
        for (index, resource) in realized.iter().enumerate() {
            prop_assert_eq!(
                resource.resource_id().0,
                format!("module/resource-{index}-{index}")
            );
            prop_assert_eq!(resource.module(), "module");
            let dependencies = resource.dependencies();
            if index == 0 {
                prop_assert!(dependencies.is_empty());
            } else {
                prop_assert_eq!(
                    dependencies,
                    vec![tokeira_iac::ResourceId(format!(
                        "module/resource-{}-{}",
                        index - 1,
                        index - 1
                    ))]
                );
            }
        }
    }

    // Platform config admission round-trips valid values and rejects unknown input without side effects.
    // Feature: platform-builder-abstraction, Property 5: platform config admission round-trips and rejects surplus input
    #[test]
    fn property_5_config_admission_round_trips(
        replicas in 1_u16..2048,
        dsql in any::<bool>(),
        region in "[a-z]{2}-[a-z]+-[1-9]",
        surplus in any::<bool>(),
    ) {
        let storage = if dsql {
            TestStorage::Dsql { region }
        } else {
            TestStorage::Memory
        };
        let expected = TestConfig { storage: storage.clone(), replicas };
        let mut node = config_node(&storage, replicas);
        if surplus {
            let AuthorValue::Struct { fields, .. } = &mut node.value else {
                unreachable!("helper always constructs a struct");
            };
            fields.push(("surplus".into(), AuthorNode::string("rejected")));
        }
        let admitted = ConfigContract::<TestConfig>::new().admit(node);
        prop_assert_eq!(admitted.is_ok(), !surplus);
        if let Ok(admitted) = admitted {
            prop_assert_eq!(&admitted, &expected);
            let bytes = serde_json::to_vec(&admitted).expect("serialize admitted config");
            let round_trip: TestConfig = serde_json::from_slice(&bytes).expect("deserialize config");
            prop_assert_eq!(round_trip, expected);
        }
    }

    // Context dispatch is repeatable, immutable, and limited to the declared schema.
    // Feature: platform-builder-abstraction, Property 6: platform context exposure is immutable and allow-listed
    #[test]
    fn property_6_context_is_immutable_and_allow_listed(
        project in "[a-z]{1,16}",
        requests in prop::collection::vec(any::<bool>(), 1..24),
    ) {
        let mut session = crate::author::AuthorSession::new(
            test_binding(),
            TestContext { project: project.clone() },
        );
        let context = AuthorHandle::Context(session.context_handle());
        for valid in requests {
            let result = session.field(
                context.clone(),
                if valid { "project_name" } else { "deployment_dir" },
            );
            if valid {
                let AuthorResult::Value(AuthorNode {
                    value: AuthorValue::String(actual),
                    ..
                }) = result.expect("declared field is available")
                else {
                    prop_assert!(false, "declared field returned the wrong value shape");
                    continue;
                };
                prop_assert_eq!(actual, project.as_str());
            } else {
                prop_assert!(result.is_err());
            }
        }
    }

    // Verification validates only authored input; execution realizes the same logical set once.
    // Feature: platform-builder-abstraction, Property 8: definition verification is complete and pure
    #[test]
    fn property_8_verification_is_complete_and_pure(
        valid in prop::collection::vec(any::<bool>(), 1..20),
    ) {
        let realizations = Arc::new(AtomicUsize::new(0));
        let mut graph = DeploymentGraphBuilder::new();
        let deployment = graph.deployment_handle();
        let module = graph
            .add_module(&deployment, "module".into(), Vec::new())
            .expect("module");
        for (index, is_valid) in valid.iter().copied().enumerate() {
            graph
                .add_resource(
                    &module,
                    format!("resource-{index}"),
                    Box::new(ValidationProbeKind {
                        valid: is_valid,
                        realizations: Arc::clone(&realizations),
                    }),
                    Vec::new(),
                )
                .expect("resource");
        }
        let definition = EvaluatedDefinition {
            config: TestConfig {
                storage: TestStorage::Memory,
                replicas: 1,
            },
            graph: graph.finish().expect("graph"),
            configuration_identity: ConfigurationIdentity::compute(
                &tokeira_orchestrator::DefinitionFormatId::new("test").expect("format"),
                b"definition",
            ),
        };
        let result = verification_engine().verify(&definition);
        prop_assert_eq!(realizations.load(Ordering::SeqCst), 0);
        let expected = valid.iter().filter(|value| !**value).count();
        if expected == 0 {
            prop_assert!(result.is_ok());
            let tags = BTreeMap::from([("deployment".to_string(), "real".to_string())]);
            let realized = realize_resources(&definition.graph, "real-deployment", &tags)
                .expect("validated inputs realize with the admitted invocation");
            prop_assert_eq!(realized.iter().len(), valid.len());
            prop_assert_eq!(realizations.load(Ordering::SeqCst), valid.len());
        } else {
            let report = result.expect_err("faults must be reported");
            prop_assert_eq!(report.findings.len(), expected);
            let invalid = report
                .findings
                .iter()
                .filter(|finding| matches!(finding, VerificationFinding::InvalidInput { .. }))
                .count();
            prop_assert_eq!(invalid, expected);
            prop_assert_eq!(realizations.load(Ordering::SeqCst), 0);
        }
    }

    // Shared selection matches a reference transitive closure and returns definition order.
    // Feature: platform-builder-abstraction, Property 9: module selection computes the required closure
    #[test]
    fn property_9_selection_matches_reference(
        count in 1_usize..18,
        edges in prop::collection::vec(any::<bool>(), 1..18),
        requested_indexes in prop::collection::vec(0_usize..18, 1..12),
        dependents in any::<bool>(),
    ) {
        let mut graph = DeploymentGraphBuilder::new();
        let deployment = graph.deployment_handle();
        let mut handles: Vec<crate::graph::ModuleHandle> = Vec::new();
        let mut dependencies = Vec::<Vec<usize>>::new();
        for index in 0..count {
            let dependency_indexes = if index > 0 && edges[index % edges.len()] {
                vec![index - 1]
            } else {
                Vec::new()
            };
            let dependency_handles = dependency_indexes
                .iter()
                .map(|dependency| handles[*dependency].clone())
                .collect();
            handles.push(
                graph
                    .add_module(
                        &deployment,
                        format!("module-{index}"),
                        dependency_handles,
                    )
                    .expect("module"),
            );
            dependencies.push(dependency_indexes);
        }
        let graph = graph.finish().expect("graph");
        let all = select_modules(&graph, None, SelectionDirection::Prerequisites)
            .expect("omitted selector selects all")
            .modules()
            .to_vec();
        prop_assert_eq!(
            all,
            (0..count)
                .map(|index| format!("module-{index}"))
                .collect::<Vec<_>>()
        );
        prop_assert!(select_modules(
            &graph,
            Some(&[]),
            SelectionDirection::Prerequisites
        )
        .is_err());
        prop_assert!(select_modules(
            &graph,
            Some(&["unknown".to_string()]),
            SelectionDirection::Prerequisites
        )
        .is_err());
        let requested = requested_indexes
            .into_iter()
            .map(|index| index % count)
            .collect::<BTreeSet<_>>();
        let requested_names = requested
            .iter()
            .map(|index| format!("module-{index}"))
            .collect::<Vec<_>>();
        let direction = if dependents {
            SelectionDirection::Dependents
        } else {
            SelectionDirection::Prerequisites
        };
        let actual = select_modules(&graph, Some(&requested_names), direction)
            .expect("known non-empty selection")
            .modules()
            .to_vec();

        let mut expected = requested;
        loop {
            let before = expected.len();
            for (module, module_dependencies) in dependencies.iter().enumerate().take(count) {
                if dependents {
                    if module_dependencies
                        .iter()
                        .any(|dependency| expected.contains(dependency))
                    {
                        expected.insert(module);
                    }
                } else if expected.contains(&module) {
                    expected.extend(module_dependencies.iter().copied());
                }
            }
            if expected.len() == before {
                break;
            }
        }
        let expected = (0..count)
            .filter(|index| expected.contains(index))
            .map(|index| format!("module-{index}"))
            .collect::<Vec<_>>();
        prop_assert_eq!(actual, expected);
    }

    // Writeback emits only declared keys in declaration order and resolves through physical state.
    // Feature: platform-builder-abstraction, Property 10: writeback is explicit, ordered, and resolved through physical state
    #[test]
    fn property_10_writeback_is_explicit_and_physical(
        value in "[a-z]{1,16}",
        state_present in any::<bool>(),
        string_property in any::<bool>(),
    ) {
        let mut graph = DeploymentGraphBuilder::new();
        let deployment = graph.deployment_handle();
        let module = graph
            .add_module(&deployment, "module".into(), Vec::new())
            .expect("module");
        let resource = graph
            .add_resource(&module, "resource".into(), test_kind("one", true), Vec::new())
            .expect("resource");
        graph
            .add_writeback(
                &deployment,
                "runtime.literal".into(),
                WritebackValue::Literal("literal".into()),
            )
            .expect("literal");
        graph
            .add_writeback(
                &deployment,
                "runtime.output".into(),
                WritebackValue::Output(resource.output("value").expect("declared output")),
            )
            .expect("output");
        let graph = graph.finish().expect("graph");
        let realized = realize_resources(&graph, "deployment", &BTreeMap::new())
            .expect("realized");
        let mut state = tokeira_iac::InfraState::default();
        if state_present {
            let id = realized
                .index
                .get("module", "resource")
                .expect("physical id")
                .clone();
            let mut entry = resource_state(&id.0, "module", &value);
            if !string_property {
                entry.properties = serde_json::json!({"value": 42});
            }
            state.resources.insert(id, entry);
        }
        let actual = resolve_writeback(&graph, &realized.index, &state);
        prop_assert_eq!(
            &actual[0],
            &("runtime.literal".to_string(), "literal".to_string())
        );
        if state_present && string_property {
            prop_assert_eq!(
                actual,
                vec![
                    ("runtime.literal".into(), "literal".into()),
                    ("runtime.output".into(), value),
                ]
            );
        } else {
            prop_assert_eq!(actual.len(), 1);
        }
    }

    // Provider reachability evidence is transported without changes or rewritten fields.
    // Feature: platform-builder-abstraction, Property 13: reachability issues are lossless no-change outcomes
    #[test]
    fn property_13_reachability_issues_are_lossless(
        component in "[A-Za-z]{1,12}",
        fact in "[A-Za-z ]{1,32}",
        evidence in ".{1,48}",
        direction in prop::option::of("[A-Za-z ]{1,32}"),
    ) {
        let issue = tokeira_iac::PlatformIssue {
            component,
            fact,
            evidence,
            direction,
        };
        let outcome = no_change_issue_outcome(vec![issue.clone()]);
        prop_assert!(outcome.changes.is_empty());
        prop_assert_eq!(outcome.platform_issues, vec![issue]);
    }

    // Selected state replacement never rewrites entries owned by unrelated modules.
    // Feature: platform-builder-abstraction, Property 14: partial reconciliation preserves unrelated state
    #[test]
    fn property_14_partial_reconciliation_preserves_unrelated_state(
        selected_a in any::<bool>(),
        selected_b in any::<bool>(),
        old_a in "[a-z]{1,12}",
        old_b in "[a-z]{1,12}",
        new_a in "[a-z]{1,12}",
        new_b in "[a-z]{1,12}",
    ) {
        let id_a = tokeira_iac::ResourceId("a/resource".into());
        let id_b = tokeira_iac::ResourceId("b/resource".into());
        let mut recorded = tokeira_iac::InfraState::default();
        recorded.resources.insert(id_a.clone(), resource_state(&id_a.0, "a", &old_a));
        recorded.resources.insert(id_b.clone(), resource_state(&id_b.0, "b", &old_b));
        let mut replacement = tokeira_iac::InfraState::default();
        replacement.resources.insert(id_a.clone(), resource_state(&id_a.0, "a", &new_a));
        replacement.resources.insert(id_b.clone(), resource_state(&id_b.0, "b", &new_b));
        let selected = [(selected_a, "a"), (selected_b, "b")]
            .into_iter()
            .filter(|(selected, _)| *selected)
            .map(|(_, module)| module.to_string())
            .collect::<Vec<_>>();
        let actual = replace_selected_state(&recorded, &selected, &replacement);
        for (selected, id) in [(selected_a, id_a), (selected_b, id_b)] {
            let expected = if selected {
                replacement.resources.get(&id).expect("replacement")
            } else {
                recorded.resources.get(&id).expect("recorded")
            };
            prop_assert_eq!(
                serde_json::to_vec(actual.resources.get(&id).expect("retained"))
                    .expect("serialize actual"),
                serde_json::to_vec(expected).expect("serialize expected"),
            );
        }
    }
}

#[test]
fn context_tokens_are_rejected_by_typed_kind_admission() {
    let mut session = crate::author::AuthorSession::new(
        test_binding(),
        TestContext {
            project: "project".into(),
        },
    );
    let context = AuthorHandle::Context(session.context_handle());
    let token = session
        .call(
            context,
            "anchor",
            vec![AuthorArgument::Value(AuthorNode::string("state"))],
        )
        .expect("context projection succeeds");
    let AuthorResult::Handle(AuthorHandle::ContextValue(token)) = token else {
        panic!("anchor must return a typed token");
    };
    let node = AuthorNode::new(AuthorValue::Struct {
        name: "TestKind".into(),
        fields: vec![
            (
                "suffix".into(),
                AuthorNode::new(AuthorValue::ContextToken(token)),
            ),
            ("describes".into(), AuthorNode::new(AuthorValue::Bool(true))),
            (
                "extra_dependency".into(),
                AuthorNode::new(AuthorValue::Option(None)),
            ),
        ],
    });
    let error = test_kinds()
        .decode("test-resource", node)
        .expect_err("provider input cannot contain a context token");
    assert!(error.message.contains("context token"));
}

#[test]
fn relative_definition_paths_reject_aliases_and_escaping() {
    for path in [
        "",
        "/definition.tkd",
        "../definition.tkd",
        "a/../b",
        "a//b",
        "a\\b",
        "C:/definition.tkd",
    ] {
        assert!(crate::definition::RelativeDefinitionPath::new(path).is_err());
    }
    assert_eq!(
        crate::definition::RelativeDefinitionPath::new("definitions/live.tkd")
            .expect("canonical relative path")
            .as_str(),
        "definitions/live.tkd"
    );
}

#[test]
fn definition_engine_rejects_format_mismatch_before_frontend_evaluation() {
    let engine = DefinitionEngine::new(
        test_binding(),
        EchoFrontend {
            format: tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format"),
        },
    );
    let result = engine.evaluate(DefinitionRequest {
        source: DefinitionSource {
            format: tokeira_orchestrator::DefinitionFormatId::new("tkdp").expect("format"),
            source_name: DefinitionSourceName::DeploymentRelative(
                RelativeDefinitionPath::new("definition.tkdp").expect("path"),
            ),
            bytes: std::sync::Arc::from(&b"ignored"[..]),
        },
        context: TestContext {
            project: "project".into(),
        },
    });
    assert!(matches!(
        result,
        Err(crate::error::DefinitionError::FormatMismatch { .. })
    ));
}

#[test]
fn definition_engine_admits_config_and_bootstrap_graph_in_memory() {
    let format = tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format");
    let engine = DefinitionEngine::new(
        test_binding(),
        EchoFrontend {
            format: format.clone(),
        },
    );
    let evaluated = engine
        .evaluate(DefinitionRequest {
            source: DefinitionSource {
                format: format.clone(),
                source_name: DefinitionSourceName::DeploymentRelative(
                    RelativeDefinitionPath::new("definition.tkd").expect("path"),
                ),
                bytes: std::sync::Arc::from(&b"definition"[..]),
            },
            context: TestContext {
                project: "project".into(),
            },
        })
        .expect("definition is admitted");
    assert_eq!(evaluated.config.replicas, 1);
    assert_eq!(evaluated.graph.modules()[0].name(), "state");
    assert_eq!(
        evaluated.configuration_identity,
        ConfigurationIdentity::compute(&format, b"definition")
    );
}

#[test]
fn configuration_identity_serialization_remains_byte_stable() {
    let format = tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format");
    let identity = ConfigurationIdentity::compute(&format, b"definition");
    let expected = format!(
        r#"{{"algorithm":"sha256-v1","digest":"{}"}}"#,
        identity.digest
    );

    assert_eq!(identity.algorithm(), "sha256-v1");
    assert_eq!(
        serde_json::to_string(&identity).expect("identity serializes"),
        expected
    );
    assert_eq!(
        serde_json::from_str::<ConfigurationIdentity>(&expected).expect("identity deserializes"),
        identity
    );
}

proptest! {
    // Feature: platform-builder-abstraction, Property 7: configuration identity follows admitted semantics
    #[test]
    fn configuration_identity_depends_only_on_format_and_exact_bytes(
        format_segment in "[a-z][a-z0-9]{0,10}",
        bytes in prop::collection::vec(any::<u8>(), 0..256),
        edit in any::<u8>(),
        path_segment in "[a-z][a-z0-9]{0,10}",
        project in ".{0,64}",
        timestamp in any::<i64>(),
    ) {
        let format = tokeira_orchestrator::DefinitionFormatId::new(&format_segment)
            .expect("generated format is canonical");
        let other_format = tokeira_orchestrator::DefinitionFormatId::new(format!(
            "{format_segment}-other"
        ))
        .expect("derived format is canonical and unequal");
        let first = ConfigurationIdentity::compute(&format, &bytes);
        let repeated = ConfigurationIdentity::compute(&format, &bytes);
        prop_assert_eq!(&first, &repeated);

        let mut edited = bytes.clone();
        edited.push(edit);
        prop_assert_ne!(
            &first,
            &ConfigurationIdentity::compute(&format, &edited)
        );
        prop_assert_ne!(
            &first,
            &ConfigurationIdentity::compute(&other_format, &bytes)
        );

        // Paths, context facts, timestamps, state, and inspection bytes are
        // deliberately absent from the identity input. Constructing arbitrary
        // values for them cannot perturb the repeated result or the binding id.
        let _unrelated_path = RelativeDefinitionPath::new(format!(
            "definitions/{path_segment}.tkd"
        ))
        .expect("generated path is safe");
        let _unrelated_context = TestContext { project };
        let _unrelated_timestamp = timestamp;
        let binding_id = test_binding().id;
        prop_assert_eq!(
            ConfigurationIdentity::compute(&format, &bytes),
            repeated
        );
        prop_assert_eq!(binding_id.as_str(), "test");
    }
}

#[test]
fn framework_modules_preserve_dynamic_dependency_names_without_leaking() {
    let mut graph = DeploymentGraphBuilder::new();
    let deployment = graph.deployment_handle();
    let state = graph
        .add_module(&deployment, "state".into(), Vec::new())
        .expect("state module");
    let runtime = graph
        .add_module(&deployment, "runtime".into(), vec![state])
        .expect("runtime module");
    graph
        .add_resource(
            &runtime,
            "server".into(),
            test_kind("one", true),
            Vec::new(),
        )
        .expect("runtime resource");
    let definition: EvaluatedDefinition<TestPlatform> = EvaluatedDefinition {
        config: TestConfig {
            storage: TestStorage::Memory,
            replicas: 1,
        },
        graph: graph.finish().expect("graph"),
        configuration_identity: ConfigurationIdentity::compute(
            &tokeira_orchestrator::DefinitionFormatId::new("test").expect("format"),
            b"definition",
        ),
    };
    let framework = FrameworkDeployment::new(
        definition,
        test_binding(),
        InvocationContext {
            deployment_id: "deployment".into(),
            deployment_uuid: uuid::Uuid::nil(),
            environment: None,
            region: None,
            account_id: None,
            deployment_dir: std::path::PathBuf::from("deployment"),
        },
    )
    .expect("framework deployment");
    let selection = framework
        .select(None, SelectionDirection::Prerequisites)
        .expect("all modules");
    let modules = framework.infra_modules(&selection, "deployment", &BTreeMap::new());
    assert_eq!(modules[1].dependencies(), vec!["state"]);
    let state = tokeira_iac::InfraState::default();
    let extensions = std::collections::HashMap::new();
    let context = tokeira_iac::ModuleContext::new(&state, &extensions);
    let resources = modules[1].resources(&context).expect("resources realize");
    assert_eq!(resources[0].resource_id().0, "runtime/server-one");
}

#[test]
fn framework_resolves_the_provider_owned_deploy_executor() {
    let mut graph = DeploymentGraphBuilder::new();
    let deployment = graph.deployment_handle();
    graph
        .add_module(&deployment, "state".into(), Vec::new())
        .expect("state module");
    let definition = EvaluatedDefinition {
        config: TestConfig {
            storage: TestStorage::Memory,
            replicas: 1,
        },
        graph: graph.finish().expect("graph"),
        configuration_identity: ConfigurationIdentity::compute(
            &tokeira_orchestrator::DefinitionFormatId::new("test").expect("format"),
            b"definition",
        ),
    };
    let providers = ProviderSet::with_executions(
        Vec::new(),
        vec![Arc::new(TestProviderExecution {
            deploy_platform: Arc::new(TestDeployPlatform),
        })],
    );
    let invocation = InvocationContext {
        deployment_id: "deployment".into(),
        deployment_uuid: uuid::Uuid::nil(),
        environment: None,
        region: None,
        account_id: None,
        deployment_dir: std::path::PathBuf::from("deployment"),
    };
    let framework = FrameworkDeployment::new(
        definition,
        test_binding_with_providers(providers),
        invocation,
    )
    .expect("framework deployment");

    assert!(
        framework
            .deploy_platform()
            .expect("one selected executor")
            .is_some()
    );
    let config = framework.engine_config();
    let hydrated = tokeira_orchestrator::Deployment::hydrate_config(
        &framework,
        &config,
        &tokeira_iac::InfraState::default(),
    );
    assert_eq!(hydrated.platform().replicas, 2);
}

proptest! {
    // Feature: platform-builder-abstraction, Property 11: content coupling is deterministic, sensitive, and secret-free
    #[test]
    fn content_coupling_is_deterministic_sensitive_and_secret_free(
        content in prop::collection::vec(any::<u8>(), 0..256),
        edit in any::<u8>(),
        credential in "[G-Z]{8,32}",
        other_credential in "[G-Z]{8,32}",
    ) {
        let first = coupled_framework(
            DesiredContent::Bytes(content.clone()),
            credential.clone(),
            Arc::new(Mutex::new(Vec::new())),
            "unused".into(),
        );
        let repeated = coupled_framework(
            DesiredContent::Bytes(content.clone()),
            credential.clone(),
            Arc::new(Mutex::new(Vec::new())),
            "unused".into(),
        );
        let secret_mutation = coupled_framework(
            DesiredContent::Bytes(content.clone()),
            other_credential.clone(),
            Arc::new(Mutex::new(Vec::new())),
            "unused".into(),
        );
        let mut changed = content.clone();
        changed.push(edit);
        let changed = coupled_framework(
            DesiredContent::Bytes(changed),
            credential.clone(),
            Arc::new(Mutex::new(Vec::new())),
            "unused".into(),
        );

        let first = coupled_manifest(&first);
        prop_assert_eq!(&first, &coupled_manifest(&repeated));
        prop_assert_eq!(&first, &coupled_manifest(&secret_mutation));
        prop_assert_ne!(&first, &coupled_manifest(&changed));

        let identity = ContentIdentity::new("platform-artifact/runtime-config", &content);
        prop_assert_eq!(first["content"][0]["sha256"].as_str(), Some(identity.sha256.as_str()));
        let rendered = serde_json::to_string(&first).expect("manifest serialization");
        prop_assert!(!rendered.contains(&credential));
        prop_assert!(!rendered.contains(&other_credential));
    }

    // Feature: platform-builder-abstraction, Property 12: provider canonicalization preserves platform semantic content
    #[test]
    fn provider_canonicalization_preserves_platform_semantic_content(
        image in "[a-z][a-z0-9./:-]{0,31}",
        command in prop::collection::vec("[a-z][a-z0-9-]{0,12}", 0..6),
        replicas in 1u16..1000,
    ) {
        let semantic = SemanticDocument {
            image,
            command,
            replicas,
        };
        let document = DesiredDocument {
            schema: "test.semantic.v1".into(),
            value: serde_json::to_value(&semantic).expect("semantic document"),
        };
        let canonical = CanonicalDocument::typed::<SemanticDocument>(
            &document,
            "test.semantic.v1",
        )
        .expect("valid typed provider document");
        let decoded: SemanticDocument =
            serde_json::from_slice(&canonical.bytes).expect("canonical typed decode");
        prop_assert_eq!(&decoded, &semantic);

        let repeated_document = DesiredDocument {
            schema: document.schema.clone(),
            value: serde_json::from_slice(&canonical.bytes).expect("canonical JSON value"),
        };
        let repeated = CanonicalDocument::typed::<SemanticDocument>(
            &repeated_document,
            "test.semantic.v1",
        )
        .expect("idempotent canonicalization");
        prop_assert_eq!(&canonical, &repeated);

        let mut added = document.clone();
        added.value["provider_invented"] = serde_json::json!(true);
        prop_assert!(
            CanonicalDocument::typed::<SemanticDocument>(&added, "test.semantic.v1").is_err()
        );
        let mut removed = document.clone();
        removed.value
            .as_object_mut()
            .expect("typed document object")
            .remove("image");
        prop_assert!(
            CanonicalDocument::typed::<SemanticDocument>(&removed, "test.semantic.v1").is_err()
        );
        let mut substituted = semantic.clone();
        substituted.image.push_str("-other");
        let substituted_document = DesiredDocument {
            schema: document.schema,
            value: serde_json::to_value(&substituted).expect("substituted document"),
        };
        let substituted_bytes = CanonicalDocument::typed::<SemanticDocument>(
            &substituted_document,
            "test.semantic.v1",
        )
        .expect("valid substituted platform choice");
        let substituted_decoded: SemanticDocument =
            serde_json::from_slice(&substituted_bytes.bytes).expect("substituted decode");
        prop_assert_eq!(substituted_decoded, substituted);
        prop_assert_ne!(substituted_bytes, canonical);
    }

    // Feature: platform-builder-abstraction, Property 15: operations declarations are catalog-bound and deterministic
    #[test]
    fn operations_declarations_are_catalog_bound_and_deterministic(
        service_names in prop::collection::btree_set("[a-z][a-z0-9]{0,7}", 1..8),
        remote_port in 1u16..u16::MAX,
        default_local_port in 1u16..u16::MAX,
        override_port in 1u16..u16::MAX,
        access_mode in prop_oneof![Just("published".to_string()), Just("session".to_string())],
        reverse_declarations in any::<bool>(),
    ) {
        let mut declarations = service_names
            .iter()
            .map(|service| ServiceOps {
                logical_service: service.clone(),
                logs: Some(
                    OperationRequest::typed(
                        &TEST_LOG_OPERATION,
                        TestLogTarget {
                            source: format!("logs/{service}"),
                        },
                    )
                    .expect("typed log request"),
                ),
                ports: vec![
                    OperationRequest::typed(
                        &TEST_PORT_OPERATION,
                        TestPortTarget {
                            remote_host: format!("{service}.internal"),
                            remote_port,
                            protocol: "tcp".into(),
                            access_mode: access_mode.clone(),
                            default_local_port,
                        },
                    )
                    .expect("typed port request"),
                ],
            })
            .collect::<Vec<_>>();
        if reverse_declarations {
            declarations.reverse();
        }
        let operations = PlatformOps::new(declarations);
        let providers = ProviderSet::<TestPlatform>::with_capabilities(
            Vec::new(),
            Vec::new(),
            vec![
                Arc::new(TestProviderOperation { operation: "logs" }),
                Arc::new(TestProviderOperation { operation: "port-forward" }),
            ],
        );
        operations
            .validate(&service_names, &providers)
            .expect("catalog-bound operation inventory");
        let expected = service_names.iter().map(String::as_str).collect::<Vec<_>>();
        prop_assert_eq!(operations.supported(OperationKind::Logs), expected.clone());
        prop_assert_eq!(
            operations.supported(OperationKind::PortForward),
            expected.clone()
        );

        let unknown = operations.logs("not-declared").expect_err("unknown service");
        let OpsError::UnknownService { supported, .. } = unknown else {
            prop_assert!(false, "unexpected error: {unknown}");
            return Ok(());
        };
        prop_assert_eq!(supported, expected.iter().map(|name| (*name).to_string()).collect::<Vec<_>>());

        let selected = expected[0];
        let baseline = operations.ports(selected, None).expect("default port request");
        let overridden = operations
            .ports(selected, Some(override_port))
            .expect("overridden port request");
        prop_assert_eq!(baseline[0].request(), overridden[0].request());
        prop_assert_eq!(baseline[0].local_port(), None);
        prop_assert_eq!(overridden[0].local_port(), Some(override_port));

        let executor = providers
            .operation(
                overridden[0].request().provider(),
                overridden[0].request().operation(),
            )
            .expect("registered provider operation");
        let output = complete_immediate(executor.execute(
            &overridden[0],
            &tokeira_iac::ProvisionContext::default(),
        ))
        .expect("provider operation");
        let OperationOutput::PortForward { endpoint, .. } = output else {
            prop_assert!(false, "expected port-forward output");
            return Ok(());
        };
        prop_assert_eq!(endpoint.local_port, override_port);
        prop_assert_eq!(endpoint.remote_host, format!("{selected}.internal"));
        prop_assert_eq!(endpoint.remote_port, remote_port);
        prop_assert_eq!(endpoint.access_mode, access_mode);

        let mut missing_catalog_entry = service_names.clone();
        missing_catalog_entry.remove(selected);
        prop_assert!(operations.validate(&missing_catalog_entry, &providers).is_err());
    }

    // Feature: platform-builder-abstraction, Property 24: artifact write boundaries are disjoint
    #[test]
    fn artifact_write_boundaries_are_disjoint(lifecycle in 0u8..8) {
        let root = TestDeploymentDir::new("artifact-boundary");
        let target = root.path().join("inspection.txt");
        std::fs::write(&target, "operator edit\n").expect("prior inspection bytes");
        let operational = Arc::new(Mutex::new(Vec::new()));
        let inspections = Arc::new(AtomicUsize::new(0));
        let renderer: Arc<dyn InspectionRenderer<TestPlatform>> =
            Arc::new(TestInspectionRenderer {
                renders: Arc::clone(&inspections),
                fail: false,
            });
        let ops = PlatformOps::new(vec![ServiceOps {
            logical_service: "server".into(),
            logs: Some(OperationRequest::typed(
                &TEST_LOG_OPERATION,
                TestLogTarget {
                    source: "runtime/server".into(),
                },
            ).expect("typed log request")),
            ports: Vec::new(),
        }]);
        let binding = coupled_binding_with_capabilities(
            DesiredContent::Text("desired = true\n".into()),
            "CREDENTIAL".into(),
            Arc::clone(&operational),
            ops,
            vec![Arc::new(TestProviderOperation { operation: "logs" })],
            vec![InspectionSpec::new(
                RelativeArtifactPath::new("inspection.txt").expect("inspection path"),
                renderer,
            )],
        );
        let framework = coupled_framework_from_binding(binding, root.path().to_path_buf());
        let mut context = tokeira_iac::ProvisionContext::default();

        match lifecycle {
            // definition check, plan, rollback, and destroy have no
            // publication call available in their framework traversal.
            0 | 1 | 3 | 4 => {
                let _ = coupled_manifest(&framework);
            }
            // Provider discovery and retrieval are operational reads; they do
            // not cross either artifact publication boundary.
            2 => {
                let output = complete_immediate(framework.execute_operation_with_context(
                    OperationKind::Logs,
                    "server",
                    None,
                    &context,
                )).expect("provider operation");
                prop_assert_eq!(
                    output,
                    vec![OperationOutput::Logs(vec!["runtime/server".into()])]
                );
            }
            // A committed apply publishes operational content before
            // convergence and inspection only after the commit boundary.
            5 => {
                complete_immediate(framework.materialize_operational_artifacts(
                    OperationalArtifactStage::Workload,
                    &mut context,
                ))
                .expect("operational apply publication");
                framework
                    .publish_inspection()
                    .expect("post-commit inspection publication");
            }
            // A failed apply may have staged operational content, but never
            // crosses the committed inspection boundary.
            6 => {
                complete_immediate(framework.materialize_operational_artifacts(
                    OperationalArtifactStage::Workload,
                    &mut context,
                ))
                .expect("pre-convergence operational publication");
            }
            // Creation rendering is pure and can be staged by the separate
            // all-or-nothing deployment transaction without publishing here.
            _ => {
                let rendered = framework.render_inspection().expect("pure rendering");
                prop_assert_eq!(rendered.len(), 1);
            }
        }

        let expected_operational = usize::from(matches!(lifecycle, 5 | 6));
        let expected_inspection = usize::from(matches!(lifecycle, 5 | 7));
        prop_assert_eq!(
            operational.lock().expect("operational recorder").len(),
            expected_operational
        );
        prop_assert_eq!(inspections.load(Ordering::SeqCst), expected_inspection);
        let bytes = std::fs::read(&target).expect("inspection target remains complete");
        if lifecycle == 5 {
            prop_assert_ne!(bytes, b"operator edit\n");
        } else {
            prop_assert_eq!(bytes, b"operator edit\n");
        }
    }
}

#[test]
fn operational_materialization_uses_cached_bytes_and_installs_validated_receipts() {
    let published = Arc::new(Mutex::new(Vec::new()));
    let framework = coupled_framework(
        DesiredContent::Text("authoritative = true\n".into()),
        "CREDENTIAL".into(),
        Arc::clone(&published),
        "unused".into(),
    );
    let mut context = tokeira_iac::ProvisionContext::default();

    let receipts = complete_immediate(
        framework
            .materialize_operational_artifacts(OperationalArtifactStage::Workload, &mut context),
    )
    .expect("operational publication");

    assert_eq!(
        published.lock().expect("publication recorder").as_slice(),
        &[b"authoritative = true\n".to_vec()]
    );
    assert_eq!(receipts.iter().len(), 1);
    assert_eq!(
        receipts.iter().next().expect("receipt").artifact,
        "runtime-config"
    );
    assert_eq!(
        context
            .extension::<OperationalArtifactReceipts>()
            .expect("consumer receipts")
            .iter()
            .len(),
        1
    );
}

#[test]
fn deployment_file_content_is_resolved_once_per_framework_invocation() {
    let root =
        std::env::temp_dir().join(format!("tokeira-platform-content-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("temporary deployment root");
    let source = root.join("tokeirad.toml");
    std::fs::write(&source, "first\n").expect("initial runtime configuration");
    let published = Arc::new(Mutex::new(Vec::new()));
    let framework = coupled_framework(
        DesiredContent::DeploymentFile(
            RelativeArtifactPath::new("tokeirad.toml").expect("relative artifact path"),
        ),
        "CREDENTIAL".into(),
        Arc::clone(&published),
        root.clone(),
    );
    let first_manifest = coupled_manifest(&framework);

    std::fs::write(&source, "second\n").expect("edited runtime configuration");
    assert_eq!(first_manifest, coupled_manifest(&framework));
    let next = coupled_framework(
        DesiredContent::DeploymentFile(
            RelativeArtifactPath::new("tokeirad.toml").expect("relative artifact path"),
        ),
        "CREDENTIAL".into(),
        Arc::new(Mutex::new(Vec::new())),
        root.clone(),
    );
    assert_ne!(first_manifest, coupled_manifest(&next));
    std::fs::remove_dir_all(root).expect("remove temporary deployment root");
}

#[test]
fn framework_operations_dispatch_typed_targets_without_changing_remote_topology() {
    let root = TestDeploymentDir::new("operations");
    let ops = PlatformOps::new(vec![ServiceOps {
        logical_service: "server".into(),
        logs: Some(
            OperationRequest::typed(
                &TEST_LOG_OPERATION,
                TestLogTarget {
                    source: "runtime/server".into(),
                },
            )
            .expect("typed log target"),
        ),
        ports: vec![
            OperationRequest::typed(
                &TEST_PORT_OPERATION,
                TestPortTarget {
                    remote_host: "server.internal".into(),
                    remote_port: 7233,
                    protocol: "tcp".into(),
                    access_mode: "remote-host".into(),
                    default_local_port: 7233,
                },
            )
            .expect("typed port target"),
        ],
    }]);
    let binding = coupled_binding_with_capabilities(
        DesiredContent::Text("desired = true\n".into()),
        "CREDENTIAL".into(),
        Arc::new(Mutex::new(Vec::new())),
        ops,
        vec![
            Arc::new(TestProviderOperation { operation: "logs" }),
            Arc::new(TestProviderOperation {
                operation: "port-forward",
            }),
        ],
        Vec::new(),
    );
    let framework = coupled_framework_from_binding(binding, root.path().to_path_buf());
    let context = tokeira_iac::ProvisionContext::default();

    let unknown =
        complete_immediate(framework.execute_operation(OperationKind::Logs, "missing", None))
            .expect_err("unknown service is rejected before provider or state access");
    assert!(matches!(unknown, OpsError::UnknownService { .. }));

    let logs = complete_immediate(framework.execute_operation_with_context(
        OperationKind::Logs,
        "server",
        None,
        &context,
    ))
    .expect("provider logs");
    assert_eq!(
        logs,
        vec![OperationOutput::Logs(vec!["runtime/server".into()])]
    );
    let ports = complete_immediate(framework.execute_operation_with_context(
        OperationKind::PortForward,
        "server",
        Some(17233),
        &context,
    ))
    .expect("provider port resolution");
    let OperationOutput::PortForward { endpoint, session } = &ports[0] else {
        panic!("port resolution must return an endpoint");
    };
    assert_eq!(endpoint.local_port, 17233);
    assert_eq!(endpoint.remote_host, "server.internal");
    assert_eq!(endpoint.remote_port, 7233);
    assert_eq!(endpoint.access_mode, "remote-host");
    assert_eq!(
        session.as_ref().expect("provider session plan").program,
        "provider-session"
    );
}

#[test]
fn inspection_render_failure_preserves_the_prior_complete_file() {
    let root = TestDeploymentDir::new("inspection-render-failure");
    let target = root.path().join("inspection.txt");
    std::fs::write(&target, "prior complete bytes\n").expect("prior inspection");
    let renderer: Arc<dyn InspectionRenderer<TestPlatform>> = Arc::new(TestInspectionRenderer {
        renders: Arc::new(AtomicUsize::new(0)),
        fail: true,
    });
    let framework = coupled_framework_with_inspection(
        DesiredContent::Text("desired = true\n".into()),
        "CREDENTIAL".into(),
        Arc::new(Mutex::new(Vec::new())),
        root.path().to_path_buf(),
        vec![InspectionSpec::new(
            RelativeArtifactPath::new("inspection.txt").expect("inspection path"),
            renderer,
        )],
    );

    let error = framework
        .publish_inspection()
        .expect_err("injected renderer failure");
    assert!(error.to_string().contains("injected renderer failure"));
    assert_eq!(
        std::fs::read(&target).expect("prior inspection remains"),
        b"prior complete bytes\n"
    );
}

#[test]
fn inspection_publication_replaces_operator_edits_from_same_directory_staging() {
    let root = TestDeploymentDir::new("inspection-atomic");
    let target = root.path().join("nested/inspection.txt");
    std::fs::create_dir_all(target.parent().expect("target parent")).expect("target parent");
    std::fs::write(&target, "operator edit\n").expect("operator-edited inspection");
    let renderer: Arc<dyn InspectionRenderer<TestPlatform>> = Arc::new(TestInspectionRenderer {
        renders: Arc::new(AtomicUsize::new(0)),
        fail: false,
    });
    let framework = coupled_framework_with_inspection(
        DesiredContent::Text("desired = true\n".into()),
        "CREDENTIAL".into(),
        Arc::new(Mutex::new(Vec::new())),
        root.path().to_path_buf(),
        vec![InspectionSpec::new(
            RelativeArtifactPath::new("nested/inspection.txt").expect("inspection path"),
            renderer,
        )],
    );

    let publications = framework
        .publish_inspection()
        .expect("atomic inspection publication");
    assert_eq!(publications.len(), 1);
    let bytes = std::fs::read(&target).expect("published inspection");
    assert!(
        !bytes
            .windows("operator edit".len())
            .any(|window| window == b"operator edit")
    );
    let siblings = std::fs::read_dir(target.parent().expect("target parent"))
        .expect("published directory")
        .map(|entry| entry.expect("directory entry").file_name())
        .collect::<Vec<_>>();
    assert_eq!(siblings, vec![std::ffi::OsString::from("inspection.txt")]);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&target)
                .expect("inspection metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[cfg(unix)]
#[test]
fn inspection_publication_rejects_a_parent_symlink_that_escapes_the_deployment() {
    use std::os::unix::fs::symlink;

    let root = TestDeploymentDir::new("inspection-symlink-root");
    let outside = TestDeploymentDir::new("inspection-symlink-outside");
    symlink(outside.path(), root.path().join("alias")).expect("escaping parent symlink");
    let renderer: Arc<dyn InspectionRenderer<TestPlatform>> = Arc::new(TestInspectionRenderer {
        renders: Arc::new(AtomicUsize::new(0)),
        fail: false,
    });
    let framework = coupled_framework_with_inspection(
        DesiredContent::Text("desired = true\n".into()),
        "CREDENTIAL".into(),
        Arc::new(Mutex::new(Vec::new())),
        root.path().to_path_buf(),
        vec![InspectionSpec::new(
            RelativeArtifactPath::new("alias/new/inspection.txt").expect("inspection path"),
            renderer,
        )],
    );

    let error = framework
        .publish_inspection()
        .expect_err("escaping parent must be rejected");
    assert!(
        error
            .to_string()
            .contains("escapes the deployment directory")
    );
    assert!(!outside.path().join("new").exists());
}
