//! The shipped definition set, evaluated whole: the split `.tkd` documents
//! (`deployment.tkd` wiring, `platform.tkd` model, `observability.tkd`
//! services) resolve as parts beside the root and build the same deployment
//! the monolithic document did — through the real platform namespaces, with
//! nothing stubbed.

use std::path::Path;

use serde::Serialize;
use tokeira_platform::definition::{
    DefinitionFrontend, DefinitionSourceName, DirectoryPartSources, FrontendOutput, FrontendSource,
};

#[derive(Serialize)]
struct Ctx {
    project_name: String,
}

fn evaluate(root_text: &str) -> Result<FrontendOutput, String> {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let platform = tokeira_compose_deployment::platform();
    let parts = DirectoryPartSources::new(package, "tkd");
    let source_name = DefinitionSourceName::AuthoringPath(package.join("deployment.tkd"));
    tokeira_tkd::frontend()
        .evaluate(
            FrontendSource {
                source_name: &source_name,
                bytes: root_text.as_bytes(),
            },
            &Ctx {
                project_name: "demo".to_string(),
            },
            &platform.namespaces,
            &parts,
        )
        .map_err(|diagnostic| diagnostic.to_string())
}

fn shipped_root() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("deployment.tkd"))
        .expect("the shipped root document reads")
}

// The shipped defaults select in-memory storage: three modules, the four
// observability services plus tokeirad, and no writeback.
#[test]
fn the_shipped_set_evaluates_with_in_memory_defaults() {
    let output = evaluate(&shipped_root()).expect("the shipped definition set evaluates");
    let modules: Vec<&str> = output
        .graph
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert_eq!(modules, ["local_state", "runtime", "observability"]);
    let resources: Vec<String> = output
        .graph
        .resources()
        .iter()
        .map(|resource| format!("{}:{}", resource.module(), resource.logical_id()))
        .collect();
    for expected in [
        "local_state:dir",
        "runtime:server_config",
        "runtime:tokeirad",
        "observability:config_files",
        "observability:mimir",
        "observability:loki",
        "observability:grafana",
        "observability:alloy",
    ] {
        assert!(
            resources.iter().any(|r| r == expected),
            "{expected} in {resources:?}"
        );
    }
    assert!(output.graph.writeback().is_empty());
}

// Selecting DSQL storage in the root's config adds the dsql module and the
// five writeback declarations — the part-carried `#[create]` types drive
// the same structural selection the monolith did. The edit is what an
// operator's would be: take the two DSQL types from the platform part and
// swap the storage literal.
#[test]
fn selecting_dsql_adds_the_module_and_writeback() {
    let shipped = shipped_root();
    let use_line =
        "use platform::{Aws, Backend, Compose, Grafana, Observability, Storage, Tokeirad};";
    assert!(
        shipped.contains(use_line),
        "the root's use line is as shipped"
    );
    let root = shipped
        .replace(
            use_line,
            "use platform::{Aws, Backend, Compose, DsqlMode, DsqlStorage, Grafana, Observability, Storage, Tokeirad};",
        )
        .replace(
            "storage: Storage::InMemory,",
            "storage: Storage::Dsql(DsqlStorage {\n            region: \"eu-west-2\".into(),\n            mode: DsqlMode::Managed,\n            endpoint: None,\n            arn: None,\n        }),",
        );
    assert_ne!(root, shipped, "the storage literal was rewritten");
    let output = evaluate(&root).expect("the DSQL-selected set evaluates");
    let modules: Vec<&str> = output
        .graph
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert_eq!(modules, ["local_state", "dsql", "runtime", "observability"]);
    assert_eq!(output.graph.writeback().len(), 5);
}
