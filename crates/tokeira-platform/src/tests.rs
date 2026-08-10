use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use proptest::prelude::*;
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::{
    author::{LocatedValue, ValueShape, VariantShape, from_located_value},
    content::ContentIdentity,
    definition::{ConfigurationIdentity, EvaluatedDefinition, verify_definition},
    error::{GraphError, KindError},
    graph::{StructuralGraphBuilder, WritebackValue},
    inspection::publish_inspection,
    kind::{DecodedKind, Kind, PlacementContext},
};

/// The evaluated-config placeholder: definitions author the shape and the
/// framework holds the value as data, so tests carry an empty struct value.
fn test_config_value() -> LocatedValue {
    LocatedValue::new(ValueShape::Struct {
        name: "TestConfig".to_string(),
        fields: Vec::new(),
    })
}

#[derive(Debug)]
struct TestKind;

#[derive(Debug)]
struct ValidationProbeKind {
    valid: bool,
    validations: Arc<AtomicUsize>,
    manifests: Arc<AtomicUsize>,
    realizations: Arc<AtomicUsize>,
    placements: Arc<Mutex<Vec<PlacementContext>>>,
}

impl Kind<ProbeResource> for ValidationProbeKind {
    fn realize(&self, placement: &PlacementContext) -> Result<ProbeResource, KindError> {
        self.realizations.fetch_add(1, Ordering::SeqCst);
        self.placements
            .lock()
            .expect("placement capture mutex")
            .push(placement.clone());
        Ok(ProbeResource {
            id: tokeira_iac::ResourceId(format!("{}/{}", placement.module, placement.logical_id)),
            module: placement.module.clone(),
            valid: self.valid,
            validations: Arc::clone(&self.validations),
            manifests: Arc::clone(&self.manifests),
            placement: placement.clone(),
        })
    }
}

#[derive(Debug)]
struct ProbeResource {
    id: tokeira_iac::ResourceId,
    module: String,
    valid: bool,
    validations: Arc<AtomicUsize>,
    manifests: Arc<AtomicUsize>,
    placement: PlacementContext,
}

impl tokeira_iac::Resource for ProbeResource {
    fn change_semantics(
        &self,
        _ctx: &tokeira_iac::SemanticsContext<'_>,
    ) -> tokeira_iac::ChangeSemantics {
        tokeira_iac::ChangeSemantics::default()
    }
    fn resource_type(&self) -> tokeira_iac::ResourceType {
        tokeira_iac::ResourceType::new("ValidationProbe")
    }

    fn validate_input(&self) -> Result<(), String> {
        self.validations.fetch_add(1, Ordering::SeqCst);
        if self.valid {
            Ok(())
        } else {
            Err("probe input is invalid".to_string())
        }
    }

    fn desired_manifest(&self) -> serde_json::Value {
        self.manifests.fetch_add(1, Ordering::SeqCst);
        serde_json::json!({
            "deployment": self.placement.deployment_id,
            "module": self.placement.module,
            "logical_id": self.placement.logical_id,
        })
    }

    fn resource_id(&self) -> tokeira_iac::ResourceId {
        self.id.clone()
    }

    fn dependencies(&self) -> Vec<tokeira_iac::ResourceId> {
        Vec::new()
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn create<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _context: &'life1 tokeira_iac::ProvisionContext,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<tokeira_iac::ResourceState, tokeira_iac::IacError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("probe resources are never executed") })
    }

    fn update<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _current: &'life1 tokeira_iac::ResourceState,
        _context: &'life2 tokeira_iac::ProvisionContext,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<tokeira_iac::ResourceState, tokeira_iac::IacError>>
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
        Box::pin(async { unreachable!("probe resources are never executed") })
    }

    fn delete<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        _current: &'life1 tokeira_iac::ResourceState,
        _context: &'life2 tokeira_iac::ProvisionContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), tokeira_iac::IacError>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("probe resources are never executed") })
    }

    fn describe<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _context: &'life1 tokeira_iac::ProvisionContext,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<tokeira_iac::DescribeResult, tokeira_iac::IacError>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async { unreachable!("probe resources are never executed") })
    }

    fn diff(
        &self,
        _current: &tokeira_iac::ResourceState,
        _context: &tokeira_iac::ProvisionContext,
    ) -> tokeira_iac::InternalChange {
        tokeira_iac::InternalChange::NoChange {
            resource_id: self.id.clone(),
        }
    }
}

fn probe_graph(
    valid: impl IntoIterator<Item = bool>,
    validations: &Arc<AtomicUsize>,
    manifests: &Arc<AtomicUsize>,
    realizations: &Arc<AtomicUsize>,
    placements: &Arc<Mutex<Vec<PlacementContext>>>,
) -> crate::graph::VerifiedGraph<DecodedKind> {
    let mut graph = StructuralGraphBuilder::new();
    graph.add_module("module", Vec::new());
    for (index, valid) in valid.into_iter().enumerate() {
        graph.add_resource(
            "module",
            format!("resource-{index}"),
            DecodedKind::resource::<ValidationProbeKind, ProbeResource>(
                "ValidationProbe",
                ValidationProbeKind {
                    valid,
                    validations: Arc::clone(validations),
                    manifests: Arc::clone(manifests),
                    realizations: Arc::clone(realizations),
                    placements: Arc::clone(placements),
                },
            ),
            Vec::new(),
        );
    }
    graph.finish().expect("probe graph")
}

#[test]
// Feature: platform-builder-abstraction, Property 2: structural graph completion is exact.
fn graph_preserves_declaration_order_and_output_references() {
    let mut graph = StructuralGraphBuilder::new();
    graph.add_namespace("default");
    graph.add_module("state", Vec::new());
    graph.add_module("service", vec!["state".to_string()]);
    let resource = graph.add_resource("state", "primary", TestKind, Vec::new());
    let output = graph.output(&resource, "value").expect("declared output");
    graph.add_writeback("runtime.value", WritebackValue::Output(output));
    let graph = graph.finish().expect("valid graph");
    assert_eq!(
        graph
            .modules()
            .iter()
            .map(|module| module.name())
            .collect::<Vec<_>>(),
        ["state", "service"]
    );
}

#[test]
// Feature: platform-builder-abstraction, Property 2: structural graph completion is exact.
fn graph_rejects_cycles_without_handle_identity_machinery() {
    let mut graph = StructuralGraphBuilder::<TestKind>::new();
    graph.add_module("first", vec!["second".to_string()]);
    graph.add_module("second", vec!["first".to_string()]);
    assert!(matches!(graph.finish(), Err(GraphError::Invalid(_))));
}

#[test]
// Feature: platform-builder-abstraction, Property 2: structural graph completion is exact.
fn graph_rejects_forward_module_dependencies() {
    let mut graph = StructuralGraphBuilder::<TestKind>::new();
    graph.add_module("service", vec!["state".to_string()]);
    graph.add_module("state", Vec::new());
    let Err(GraphError::Invalid(findings)) = graph.finish() else {
        panic!("forward dependency must fail");
    };
    assert!(findings.iter().any(|finding| matches!(
        finding,
        crate::error::GraphFinding::ModuleDependencyOrder { module, dependency }
            if module == "service" && dependency == "state"
    )));
}

proptest! {
    // Feature: platform-builder-abstraction, Property 8: verification is pure and execution uses the verified set.
    #[test]
    fn verification_is_pure_and_realization_uses_the_exact_verified_set_once(
        resource_count in 1_usize..20,
    ) {
        let validations = Arc::new(AtomicUsize::new(0));
        let manifests = Arc::new(AtomicUsize::new(0));
        let realizations = Arc::new(AtomicUsize::new(0));
        let placements = Arc::new(Mutex::new(Vec::new()));
        let definition = EvaluatedDefinition {
            config: test_config_value(),
            graph: probe_graph(
                std::iter::repeat_n(true, resource_count),
                &validations,
                &manifests,
                &realizations,
                &placements,
            ),
            configuration_identity: ConfigurationIdentity::compute(
                &DefinitionFormatId::new("tkd").expect("format"),
                b"probe",
            ),
        };

        let verified = verify_definition(&definition);
        prop_assert_eq!(validations.load(Ordering::SeqCst), 0);
        prop_assert_eq!(manifests.load(Ordering::SeqCst), 0);
        prop_assert_eq!(realizations.load(Ordering::SeqCst), 0);

        let directory = tempfile::tempdir().expect("deployment directory");
        let tags = std::collections::BTreeMap::from([(
            "deployment".to_string(),
            "real".to_string(),
        )]);
        let realized = verified
            .realize("real-deployment", directory.path(), directory.path(), &tags)
            .expect("realize verified set");
        prop_assert_eq!(realized.iter().len(), resource_count);
        prop_assert_eq!(manifests.load(Ordering::SeqCst), resource_count);
        prop_assert_eq!(realizations.load(Ordering::SeqCst), resource_count);

        let placements = placements.lock().expect("placement capture mutex");
        prop_assert_eq!(placements.len(), resource_count);
        for (index, placement) in placements.iter().enumerate() {
            prop_assert_eq!(&placement.deployment_id, "real-deployment");
            prop_assert_eq!(placement.deployment_dir.as_path(), directory.path());
            prop_assert_eq!(&placement.module, "module");
            prop_assert_eq!(&placement.logical_id, &format!("resource-{index}"));
            prop_assert_eq!(&placement.tags, &tags);
        }
    }
}

#[test]
fn invalid_resource_never_reaches_an_engine() {
    let validations = Arc::new(AtomicUsize::new(0));
    let manifests = Arc::new(AtomicUsize::new(0));
    let realizations = Arc::new(AtomicUsize::new(0));
    let placements = Arc::new(Mutex::new(Vec::new()));
    let definition = EvaluatedDefinition {
        config: test_config_value(),
        graph: probe_graph(
            [true, false, true],
            &validations,
            &manifests,
            &realizations,
            &placements,
        ),
        configuration_identity: ConfigurationIdentity::compute(
            &DefinitionFormatId::new("tkd").expect("format"),
            b"invalid-probe",
        ),
    };

    let verified = verify_definition(&definition);
    let directory = tempfile::tempdir().expect("deployment directory");
    let error = verified
        .realize(
            "invalid-deployment",
            directory.path(),
            directory.path(),
            &std::collections::BTreeMap::new(),
        )
        .expect_err("invalid resource must fail realization");
    assert_eq!(error.message, "probe input is invalid");
    assert_eq!(validations.load(Ordering::SeqCst), 2);
    assert_eq!(manifests.load(Ordering::SeqCst), 1);
    assert_eq!(realizations.load(Ordering::SeqCst), 2);
    assert_eq!(placements.lock().expect("placement capture mutex").len(), 2);
}

#[test]
// Feature: platform-builder-abstraction, Property 7: configuration identity is byte stable.
fn configuration_identity_serialization_remains_byte_stable() {
    let identity = ConfigurationIdentity::compute(
        &DefinitionFormatId::new("tkd").expect("format"),
        b"config bytes",
    );
    let serialized = serde_json::to_string(&identity).expect("serialize identity");
    assert_eq!(
        serialized,
        format!(
            "{{\"algorithm\":\"sha256-v1\",\"digest\":\"{}\"}}",
            identity.digest
        )
    );
}

#[test]
// Feature: platform-builder-abstraction, Property 17: inspection is deterministic and non-authoritative.
fn inspection_publication_is_atomic_and_uses_definition_path_admission() {
    let directory = tempfile::tempdir().expect("temporary deployment");
    let path = RelativeDefinitionPath::new("docker-compose.yml").expect("safe path");
    let first =
        publish_inspection(directory.path(), path.clone(), b"first").expect("first publication");
    let second =
        publish_inspection(directory.path(), path, b"second").expect("replacement publication");
    assert_ne!(first.identity, second.identity);
    assert_eq!(
        std::fs::read(directory.path().join("docker-compose.yml")).expect("published bytes"),
        b"second"
    );
}

proptest! {
    // Feature: platform-builder-abstraction, Property 11: content coupling is deterministic and sensitive.
    #[test]
    fn content_identity_is_deterministic_and_domain_separated(
        domain in "[a-z]{1,16}",
        other_domain in "[a-z]{1,16}",
        bytes in proptest::collection::vec(any::<u8>(), 0..128),
    ) {
        let first = ContentIdentity::new(&domain, &bytes);
        let second = ContentIdentity::new(&domain, &bytes);
        prop_assert_eq!(&first, &second);
        if domain != other_domain {
            prop_assert_ne!(first, ContentIdentity::new(&other_domain, &bytes));
        }
    }
}

/// Test enum mirroring the shapes real config/kind enums use: a unit variant
/// and a newtype-struct variant.
#[derive(Debug, PartialEq, serde::Deserialize)]
enum VariantTarget {
    InMemory,
    Dsql(DsqlPayload),
}

#[derive(Debug, PartialEq, serde::Deserialize)]
struct DsqlPayload {
    region: String,
    replicas: u32,
}

fn struct_spelling(name: &str, fields: Vec<(String, LocatedValue)>) -> LocatedValue {
    LocatedValue::new(ValueShape::Struct {
        name: name.to_string(),
        fields,
    })
}

proptest! {
    // Feature: tkdp-frontend, Property 11: variant-spelling equivalence — a
    // struct-shaped value in enum position decodes identically to the
    // explicit externally tagged Enum spelling.
    #[test]
    fn struct_spelling_decodes_like_enum_spelling(
        region in "[a-z0-9-]{1,24}",
        replicas in 0u32..64,
    ) {
        let fields = vec![
            ("region".to_string(), LocatedValue::string(region.clone())),
            (
                "replicas".to_string(),
                LocatedValue::new(ValueShape::Integer(i128::from(replicas))),
            ),
        ];
        let via_struct: VariantTarget =
            from_located_value(struct_spelling("Dsql", fields.clone())).expect("struct spelling");
        let via_enum: VariantTarget = from_located_value(LocatedValue::new(ValueShape::Enum {
            name: "VariantTarget".to_string(),
            variant: "Dsql".to_string(),
            body: VariantShape::Newtype(Box::new(struct_spelling("Dsql", fields))),
        }))
        .expect("enum spelling");
        prop_assert_eq!(&via_struct, &via_enum);
        prop_assert_eq!(
            via_struct,
            VariantTarget::Dsql(DsqlPayload { region, replicas })
        );
    }
}

#[test]
// Feature: tkdp-frontend, Property 11: zero-field structs are the unit-variant
// spelling; unknown variant names fail; non-enum positions are unaffected.
fn struct_spelling_unit_and_unknown_variant_boundaries() {
    let unit: VariantTarget =
        from_located_value(struct_spelling("InMemory", Vec::new())).expect("unit spelling");
    assert_eq!(unit, VariantTarget::InMemory);

    let unknown = from_located_value::<VariantTarget>(struct_spelling(
        "Postgres",
        vec![("region".to_string(), LocatedValue::string("eu"))],
    ))
    .expect_err("unknown variant name");
    assert!(unknown.message().contains("Postgres"), "{unknown:?}");

    // A struct where a struct is expected still decodes as a struct: the
    // admission arm applies only in enum position.
    let plain: DsqlPayload = from_located_value(struct_spelling(
        "DsqlPayload",
        vec![
            ("region".to_string(), LocatedValue::string("eu")),
            (
                "replicas".to_string(),
                LocatedValue::new(ValueShape::Integer(1)),
            ),
        ],
    ))
    .expect("plain struct");
    assert_eq!(plain.region, "eu");
}
