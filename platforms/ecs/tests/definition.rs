//! The shipped ECS definition set, evaluated whole: the nine `.tkd`
//! documents resolve as parts beside the root and build the deployment
//! through the real platform namespaces, with nothing stubbed.

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
    let platform = tokeira_ecs_deployment::platform();
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

// The shipped defaults select managed DSQL: seven modules in dependency
// order, and the managed mode's three writeback declarations.
#[test]
fn the_shipped_set_evaluates_with_managed_defaults() {
    let output = evaluate(&shipped_root()).expect("the shipped definition set evaluates");
    let modules: Vec<&str> = output
        .graph
        .modules()
        .iter()
        .map(|module| module.name())
        .collect();
    assert_eq!(
        modules,
        [
            "remote_state",
            "images",
            "networking",
            "dsql",
            "cluster",
            "observability",
            "services",
        ]
    );
    assert_eq!(output.graph.writeback().len(), 3);
}

// Selecting preexisting DSQL in the root's config keeps the same module
// set and the same five well-known dsql identities, and declares all five
// writebacks from the platform's own adopters.
#[test]
fn selecting_preexisting_dsql_keeps_identities_and_writes_five_back() {
    let shipped = shipped_root();
    let managed = "dsql: Dsql::Managed,";
    assert!(shipped.contains(managed), "the dsql literal is as shipped");
    let root = shipped.replace(
        managed,
        "dsql: Dsql::Preexisting(PreexistingDsql {\n            endpoint: \"adopted.dsql.example\".into(),\n            management_endpoint_id: \"vpce-mgmt\".into(),\n            connection_endpoint_id: \"vpce-conn\".into(),\n            runtime_role_arn: \"arn:aws:iam::1:role/runtime\".into(),\n            admin_role_arn: \"arn:aws:iam::1:role/admin\".into(),\n        }),",
    );
    let root = root.replace("PreexistingRole,", "PreexistingDsql, PreexistingRole,");
    assert!(
        root.contains("PreexistingDsql"),
        "the root's use line gained the adopted-DSQL type"
    );
    assert_ne!(root, shipped, "the dsql literal was rewritten");
    let output = evaluate(&root).expect("the preexisting-DSQL set evaluates");
    assert_eq!(output.graph.writeback().len(), 5);
}
