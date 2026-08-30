//! The shipped definition set, evaluated whole: the split `.tkd` documents
//! (`deployment.tkd` wiring, `platform.tkd` model, `observability.tkd`
//! services) resolve as parts beside the root and build the same deployment
//! the monolithic document did — through the real platform namespaces, with
//! nothing stubbed.

use std::path::Path;

use serde::{Deserialize, Serialize};
use tokeira_platform::{
    author::from_located_value,
    definition::{
        DefinitionFrontend, DefinitionSourceName, DirectoryPartSources, FrontendOutput,
        FrontendSource,
    },
};

#[derive(Serialize)]
struct Ctx {
    project_name: String,
}

#[derive(Deserialize)]
struct ShippedConfig {
    tokeirad: ShippedImage,
    observability: ShippedObservability,
}

#[derive(Deserialize)]
struct ShippedObservability {
    mimir: ShippedImage,
    loki: ShippedImage,
    grafana: ShippedImage,
    alloy: ShippedImage,
}

#[derive(Deserialize)]
struct ShippedImage {
    image: String,
    pull_policy: String,
}

fn evaluate(root_text: &str) -> Result<FrontendOutput, String> {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let platform = tokeira_compose_deployment::platform();
    let parts = DirectoryPartSources::new(package, "tkd");
    let source_name = DefinitionSourceName::AuthoringPath(package.join("deployment.tkd"));
    tokeira_platform_definition::tkd::frontend()
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

#[test]
fn the_shipped_set_pins_current_observability_images() {
    let output = evaluate(&shipped_root()).expect("the shipped definition set evaluates");
    let config: ShippedConfig =
        from_located_value(output.config).expect("the shipped config admits");
    assert_eq!(config.tokeirad.image, "tokeirad:latest");
    assert_eq!(config.tokeirad.pull_policy, "never");
    assert_eq!(config.observability.mimir.image, "grafana/mimir:3.2.0");
    assert_eq!(config.observability.mimir.pull_policy, "missing");
    assert_eq!(config.observability.loki.image, "grafana/loki:3.7.6");
    assert_eq!(config.observability.loki.pull_policy, "missing");
    assert_eq!(config.observability.grafana.image, "grafana/grafana:12.4.9");
    assert_eq!(config.observability.grafana.pull_policy, "missing");
    assert_eq!(config.observability.alloy.image, "grafana/alloy:v1.19.0");
    assert_eq!(config.observability.alloy.pull_policy, "missing");
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

// Feature: platform-builder-abstraction, Property 16: compose storage modes
// preserve graph parity. In-memory omits DSQL and carries no writeback;
// managed and preexisting DSQL add the dsql module and its five writebacks —
// and all three modes realize the identical non-DSQL service resource set.
#[test]
fn storage_modes_preserve_the_reference_graph_shape() {
    let shipped = shipped_root();
    let use_line =
        "use platform::{Aws, Backend, Compose, Grafana, Observability, Storage, Tokeirad};";
    let dsql_use = shipped.replace(
        use_line,
        "use platform::{Aws, Backend, Compose, DsqlMode, DsqlStorage, Grafana, Observability, Storage, Tokeirad};",
    );
    let managed = dsql_use.replace(
        "storage: Storage::InMemory,",
        "storage: Storage::Dsql(DsqlStorage {\n            region: \"eu-west-2\".into(),\n            mode: DsqlMode::Managed,\n            endpoint: None,\n            arn: None,\n        }),",
    );
    let preexisting = dsql_use.replace(
        "storage: Storage::InMemory,",
        "storage: Storage::Dsql(DsqlStorage {\n            region: \"eu-west-2\".into(),\n            mode: DsqlMode::Preexisting,\n            endpoint: \"example-cluster.dsql.eu-west-2.on.aws\".into(),\n            arn: \"arn:aws:dsql:eu-west-2:000000000000:cluster/example\".into(),\n        }),",
    );
    assert_ne!(managed, shipped, "the managed literal was rewritten");
    assert_ne!(
        preexisting, shipped,
        "the preexisting literal was rewritten"
    );

    let service_set = |root: &str, label: &str| {
        let output = evaluate(root).unwrap_or_else(|err| panic!("{label} evaluates: {err}"));
        let has_dsql = output
            .graph
            .modules()
            .iter()
            .any(|module| module.name() == "dsql");
        let services: Vec<String> = output
            .graph
            .resources()
            .iter()
            .filter(|resource| resource.module() != "dsql")
            .map(|resource| format!("{}:{}", resource.module(), resource.logical_id()))
            .collect();
        (has_dsql, output.graph.writeback().len(), services)
    };

    let (in_memory_dsql, in_memory_writebacks, in_memory_services) =
        service_set(&shipped, "in-memory");
    let (managed_dsql, managed_writebacks, managed_services) = service_set(&managed, "managed");
    let (preexisting_dsql, preexisting_writebacks, preexisting_services) =
        service_set(&preexisting, "preexisting");

    assert!(!in_memory_dsql, "in-memory carries no dsql module");
    assert_eq!(in_memory_writebacks, 0);
    assert!(managed_dsql, "managed DSQL adds the dsql module");
    assert_eq!(managed_writebacks, 5);
    assert!(preexisting_dsql, "preexisting DSQL adds the dsql module");
    assert_eq!(preexisting_writebacks, 5);

    assert_eq!(
        in_memory_services, managed_services,
        "the service resource set is storage-mode independent"
    );
    assert_eq!(in_memory_services, preexisting_services);
}
