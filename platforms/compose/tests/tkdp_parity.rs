//! Frontend parity: the Compose `.tkd` and `.tkdp` seeds are one logical
//! definition. Evaluated with equal typed context, they must admit equal
//! typed configs, equal structural graphs, and equal realized desired
//! manifests — while their configuration identities must differ (different
//! format, different bytes). Both storage variants are covered.
//
// Feature: tkdp-frontend, Property 13: Compose parity.

use std::collections::BTreeMap;

use tokeira_compose_deployment::{
    config::{self, ComposeConfig},
    context::ComposeContext,
};
use tokeira_kinds::EngineKind;
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};
use tokeira_platform::definition::{
    DefinitionSource, DefinitionSourceName, EvaluatedDefinition, evaluate_definition,
    verify_definition,
};

const TKD: &str = include_str!("../definition.tkd");
const TKDP: &str = include_str!("../definition.tkdp");

fn context() -> ComposeContext {
    ComposeContext {
        project_name: "demo".to_string(),
    }
}

fn source(format: &str, path: &str, bytes: &str) -> DefinitionSource {
    DefinitionSource {
        format: DefinitionFormatId::new(format).expect("format"),
        source_name: DefinitionSourceName::DeploymentRelative(
            RelativeDefinitionPath::new(path).expect("path"),
        ),
        bytes: bytes.as_bytes().into(),
    }
}

fn evaluate_tkd(bytes: &str) -> EvaluatedDefinition<ComposeConfig, EngineKind> {
    evaluate_definition(
        &tokeira_tkd::frontend(),
        source("tkd", "definition.tkd", bytes),
        &context(),
        tokeira_kinds::kind_functions(),
        config::validate,
    )
    .expect("tkd seed evaluates")
}

fn evaluate_tkdp(bytes: &str) -> EvaluatedDefinition<ComposeConfig, EngineKind> {
    evaluate_definition(
        &tokeira_tkdp::frontend(),
        source("tkdp", "definition.tkdp", bytes),
        &context(),
        tokeira_kinds::kind_functions(),
        config::validate,
    )
    .expect("tkdp seed evaluates")
}

/// One comparable rendering of a structural graph. Kind values render through
/// `Debug`, which every kind derives; identity of the decoded inputs is what
/// parity claims.
fn structure(evaluated: &EvaluatedDefinition<ComposeConfig, EngineKind>) -> String {
    let graph = &evaluated.graph;
    let modules: Vec<_> = graph
        .modules()
        .iter()
        .map(|module| (module.name().to_string(), module.dependencies().to_vec()))
        .collect();
    let resources: Vec<_> = graph
        .resources()
        .iter()
        .map(|resource| {
            let deps: Vec<_> = resource
                .dependencies()
                .iter()
                .map(|dep| format!("{}/{}", dep.module(), dep.logical_id()))
                .collect();
            format!(
                "{}/{} kind={:?} deps={deps:?}",
                resource.module(),
                resource.logical_id(),
                resource.kind()
            )
        })
        .collect();
    let writebacks: Vec<_> = graph
        .writeback()
        .iter()
        .map(|entry| format!("{}={:?}", entry.key(), entry.value()))
        .collect();
    format!(
        "namespaces={:?}\nmodules={modules:?}\nresources={resources:#?}\nwritebacks={writebacks:#?}",
        graph.namespaces()
    )
}

fn assert_parity(tkd_bytes: &str, tkdp_bytes: &str) {
    let tkd = evaluate_tkd(tkd_bytes);
    let tkdp = evaluate_tkdp(tkdp_bytes);

    assert_eq!(tkd.config, tkdp.config, "typed configs must be equal");
    assert_eq!(
        structure(&tkd),
        structure(&tkdp),
        "structural graphs must be equal"
    );
    assert_ne!(
        tkd.configuration_identity, tkdp.configuration_identity,
        "configuration identities must differ across formats"
    );

    // Realized desired manifests with identical invocation facts. The
    // deployment dir carries the `tokeirad.toml` companion the ServerConfig
    // kind digests.
    let deployment = tempfile::tempdir().expect("deployment dir");
    std::fs::write(
        deployment.path().join("tokeirad.toml"),
        b"[server]\nname = \"demo\"\n",
    )
    .expect("companion");
    let manifests = |evaluated: &EvaluatedDefinition<ComposeConfig, EngineKind>| {
        let verified = verify_definition(evaluated).expect("verifies");
        let realized = verified
            .realize(
                "demo",
                deployment.path(),
                deployment.path(),
                &BTreeMap::new(),
            )
            .expect("realizes");
        realized.manifests().clone()
    };
    assert_eq!(
        manifests(&tkd),
        manifests(&tkdp),
        "realized desired manifests must be equal"
    );
}

#[test]
fn in_memory_seeds_are_parity_equal() {
    assert_parity(TKD, TKDP);
}

#[test]
fn dsql_seeds_are_parity_equal() {
    let tkd = TKD.replace(
        "storage: Storage::InMemory,",
        "storage: Storage::Dsql(DsqlStorage { region: \"eu-west-2\".into(), mode: DsqlMode::Managed, endpoint: None, arn: None }),",
    );
    assert_ne!(tkd, TKD, "tkd storage substitution must apply");
    let tkdp = TKDP.replace(
        "storage=InMemory(),",
        "storage=Dsql(region=\"eu-west-2\", mode=Managed()),",
    );
    assert_ne!(tkdp, TKDP, "tkdp storage substitution must apply");
    assert_parity(&tkd, &tkdp);
}
