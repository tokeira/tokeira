use proptest::prelude::*;
use serde::Deserialize;
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::{
    author::{LocatedValue, ValueShape},
    config::admit_config,
    content::ContentIdentity,
    definition::ConfigurationIdentity,
    error::{ConfigError, GraphError, KindError},
    graph::{StructuralGraphBuilder, WritebackValue},
    inspection::publish_inspection,
    kind::{PlacementContext, ProviderKind},
};

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct TestConfig {
    name: String,
}

fn validate_config(config: &TestConfig) -> Result<(), ConfigError> {
    if config.name.is_empty() {
        Err(ConfigError::validation("name cannot be empty"))
    } else {
        Ok(())
    }
}

#[derive(Debug)]
struct TestKind;

impl ProviderKind for TestKind {
    fn kind_name(&self) -> &'static str {
        "TestKind"
    }

    fn validate_input(&self) -> Result<(), KindError> {
        Ok(())
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &["value"]
    }

    fn desired_manifest(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    fn realize(
        &self,
        _placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        Err(KindError::new("test kind is not executed"))
    }
}

#[test]
fn located_config_admission_is_serde_backed_and_pure() {
    let value = LocatedValue::new(ValueShape::Struct {
        name: "TestConfig".to_string(),
        fields: vec![("name".to_string(), LocatedValue::string("demo"))],
    });
    assert_eq!(
        admit_config(value, validate_config).expect("valid config"),
        TestConfig {
            name: "demo".to_string()
        }
    );
}

#[test]
fn graph_preserves_declaration_order_and_checks_outputs() {
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
fn graph_rejects_cycles_without_handle_identity_machinery() {
    let mut graph = StructuralGraphBuilder::<TestKind>::new();
    graph.add_module("first", vec!["second".to_string()]);
    graph.add_module("second", vec!["first".to_string()]);
    assert!(matches!(graph.finish(), Err(GraphError::Invalid(_))));
}

#[test]
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

#[test]
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
