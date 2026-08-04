//! The spike-carried guarded-dispatch example, evaluated against the real
//! engine kind library: the guard admits the authored region, the fallback
//! case pins a default, and the `InMemory` variant swaps the storage module.
//! Alongside the seed parity suite this keeps the guard/fallback authoring
//! pattern executable, not just documented.

use serde::Deserialize;
use tokeira_aws::kinds::{
    AwsKind,
    dsql_cluster::{DsqlCluster, DsqlClusterMode},
};
use tokeira_compose_deployment::context::ComposeContext;
use tokeira_kinds::EngineKind;
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};
use tokeira_platform::{
    definition::{
        DefinitionSource, DefinitionSourceName, EvaluatedDefinition, evaluate_definition,
    },
    error::ConfigError,
};

const EXAMPLE: &str = include_str!("guarded-dsql.tkdp");

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Minimal {
    storage: MinimalStorage,
}

#[derive(Debug, PartialEq, Deserialize)]
enum MinimalStorage {
    InMemory,
    ManagedDsql(ManagedDsqlStorage),
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagedDsqlStorage {
    region: String,
}

fn validate_minimal(_config: &Minimal) -> Result<(), ConfigError> {
    Ok(())
}

fn evaluate(bytes: &str) -> EvaluatedDefinition<Minimal, EngineKind> {
    evaluate_definition(
        &tokeira_tkdp::frontend(),
        DefinitionSource {
            format: DefinitionFormatId::new("tkdp").expect("format"),
            source_name: DefinitionSourceName::DeploymentRelative(
                RelativeDefinitionPath::new("guarded-dsql.tkdp").expect("path"),
            ),
            bytes: bytes.as_bytes().into(),
        },
        &ComposeContext {
            project_name: "demo".to_string(),
        },
        tokeira_kinds::kind_functions(),
        validate_minimal,
    )
    .expect("example evaluates")
}

fn cluster(region: &str) -> AwsKind {
    AwsKind::DsqlCluster(DsqlCluster {
        identity: "demo".to_string(),
        region: region.to_string(),
        mode: DsqlClusterMode::Managed,
        endpoint: None,
        arn: None,
    })
}

/// `EngineKind` deliberately carries no `PartialEq` (the compose kinds hold
/// non-comparable realization inputs), so equality is asserted at the AWS
/// layer after destructuring the provider.
fn assert_aws_kind(kind: &EngineKind, expected: &AwsKind) {
    match kind {
        EngineKind::Aws(aws) => assert_eq!(aws, expected),
        other => panic!("expected an AWS kind, got {other:?}"),
    }
}

#[test]
fn guarded_case_admits_the_authored_region() {
    let evaluated = evaluate(EXAMPLE);
    assert_eq!(
        evaluated.config.storage,
        MinimalStorage::ManagedDsql(ManagedDsqlStorage {
            region: "eu-west-2".to_string(),
        })
    );
    let resources = evaluated.graph.resources();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].module(), "dsql");
    assert_eq!(resources[0].logical_id(), "cluster");
    assert_aws_kind(resources[0].kind(), &cluster("eu-west-2"));
}

#[test]
fn failed_guard_falls_through_to_the_pinned_default() {
    let source = EXAMPLE.replace("region=\"eu-west-2\"", "region=\"\"");
    assert_ne!(source, EXAMPLE, "region substitution must apply");
    let evaluated = evaluate(&source);
    let resources = evaluated.graph.resources();
    assert_eq!(resources.len(), 1);
    assert_aws_kind(resources[0].kind(), &cluster("eu-west-1"));
}

#[test]
fn in_memory_variant_swaps_the_storage_module() {
    let source = EXAMPLE.replace(
        "storage=ManagedDsql(region=\"eu-west-2\")",
        "storage=InMemory()",
    );
    assert_ne!(source, EXAMPLE, "storage substitution must apply");
    let evaluated = evaluate(&source);
    assert_eq!(evaluated.config.storage, MinimalStorage::InMemory);
    let resources = evaluated.graph.resources();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].module(), "local_state");
    assert_eq!(resources[0].logical_id(), "dir");
    assert!(
        matches!(resources[0].kind(), EngineKind::Compose(_)),
        "{:?}",
        resources[0].kind()
    );
}
