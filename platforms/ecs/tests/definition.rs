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

    // Managed mode's writebacks: the cluster endpoint plus the two role
    // ARNs. The two endpoint-id writebacks await declared outputs on the
    // provider endpoint resources (the recorded follow-up).
    let mut keys: Vec<&str> = output
        .graph
        .writeback()
        .iter()
        .map(|entry| entry.key())
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "dsql.admin_role_arn",
            "dsql.endpoint",
            "dsql.runtime_role_arn"
        ]
    );

    assert_dsql_identities(&output);
    assert_plane_separation(&output);
}

/// Both DSQL modes realize the same five well-known identities in the dsql
/// module — the invariant every consumer and writeback binds against.
fn assert_dsql_identities(output: &FrontendOutput) {
    let mut dsql: Vec<&str> = output
        .graph
        .resources()
        .iter()
        .filter(|resource| resource.module() == "dsql")
        .map(|resource| resource.logical_id())
        .collect();
    dsql.sort_unstable();
    assert_eq!(
        dsql,
        [
            "admin_role",
            "cluster",
            "connection_endpoint",
            "management_endpoint",
            "runtime_role",
        ]
    );
}

/// Single-owner split: the deploy plane owns exactly the ten workloads
/// (every EcsWorkload sits in the services module), and no ECS task
/// definition or ECS service is authored anywhere in the infrastructure
/// graph — those kinds are not even advertised to the definition.
fn assert_plane_separation(output: &FrontendOutput) {
    let workloads: Vec<&str> = output
        .graph
        .resources()
        .iter()
        .filter(|resource| resource.kind().name() == "EcsWorkload")
        .map(|resource| resource.logical_id())
        .collect();
    assert_eq!(workloads.len(), 10, "{workloads:?}");
    assert!(
        output
            .graph
            .resources()
            .iter()
            .filter(|resource| resource.kind().name() == "EcsWorkload")
            .all(|resource| resource.module() == "services")
    );
    for forbidden in ["EcsTaskDefinition", "EcsService"] {
        assert!(
            !output
                .graph
                .resources()
                .iter()
                .any(|resource| resource.kind().name() == forbidden),
            "{forbidden} must not be authored in the infrastructure graph"
        );
        let advertised = tokeira_ecs_deployment::platform()
            .namespaces
            .iter()
            .any(|namespace| namespace.kinds.contains(&forbidden));
        assert!(!advertised, "{forbidden} must not be advertised");
    }
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
    let mut keys: Vec<&str> = output
        .graph
        .writeback()
        .iter()
        .map(|entry| entry.key())
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "dsql.admin_role_arn",
            "dsql.connection_endpoint_id",
            "dsql.endpoint",
            "dsql.management_endpoint_id",
            "dsql.runtime_role_arn",
        ]
    );
    assert_dsql_identities(&output);
    assert_plane_separation(&output);
}
