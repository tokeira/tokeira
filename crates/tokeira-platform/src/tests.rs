use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    artifact::{ArtifactCatalog, DeliveryKey},
    author::{AuthorArgument, AuthorHandle, AuthorNode, AuthorResult, AuthorValue},
    binding::{Platform, PlatformBinding, StateBinding, StatePolicy},
    catalog::{
        ImageCatalog, KindRegistration, KindSet, PlacementContext, ProviderExecution, ProviderKind,
        ProviderKindCatalog, ProviderSet, ServiceCatalog,
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
    error::{ConfigError, ContextError, FrontendDiagnostic, GraphError, VerificationFinding},
    graph::{DeploymentGraphBuilder, WorkloadDeclaration, WritebackValue},
    ops::PlatformOps,
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

    fn validate(&self) -> Result<(), crate::error::KindError> {
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

    fn hydrate_config(
        &self,
        config: &TestConfig,
        _state: &tokeira_iac::InfraState,
    ) -> TestConfig {
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

    // Verification reports every describing and dependency fault and performs no provider operation.
    // Feature: platform-builder-abstraction, Property 8: definition verification is complete and pure
    #[test]
    fn property_8_verification_is_complete_and_pure(
        describes in prop::collection::vec(any::<bool>(), 1..20),
        dangling in prop::collection::vec(any::<bool>(), 1..20),
    ) {
        let count = describes.len().min(dangling.len());
        let mut graph = DeploymentGraphBuilder::new();
        let deployment = graph.deployment_handle();
        let module = graph
            .add_module(&deployment, "module".into(), Vec::new())
            .expect("module");
        for index in 0..count {
            graph
                .add_resource(
                    &module,
                    format!("resource-{index}"),
                    Box::new(TestKind {
                        suffix: index.to_string(),
                        describes: describes[index],
                        extra_dependency: dangling[index]
                            .then(|| format!("missing-{index}")),
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
        let expected = (0..count)
            .map(|index| usize::from(!describes[index]) + usize::from(dangling[index]))
            .sum::<usize>();
        if expected == 0 {
            prop_assert!(result.is_ok());
        } else {
            let report = result.expect_err("faults must be reported");
            prop_assert_eq!(report.findings.len(), expected);
            let cannot_describe = report
                .findings
                .iter()
                .filter(|finding| matches!(finding, VerificationFinding::CannotDescribe { .. }))
                .count();
            let missing = report
                .findings
                .iter()
                .filter(|finding| matches!(finding, VerificationFinding::MissingDependency { .. }))
                .count();
            prop_assert_eq!(cannot_describe, describes[..count].iter().filter(|value| !**value).count());
            prop_assert_eq!(missing, dangling[..count].iter().filter(|value| **value).count());
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
