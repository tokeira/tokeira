//! The shipped ECS definition set, evaluated whole: the nine `.tkd`
//! documents resolve as parts beside the root and build the deployment
//! through the real platform namespaces, with nothing stubbed.

use std::{path::Path, sync::Arc};

use serde::Serialize;
use tokeira_platform::{
    definition::{
        DefinitionFrontend, DefinitionSource, DefinitionSourceName, DirectoryPartSources,
        EvaluatedDefinition, evaluate_definition, verify_definition,
    },
    kind::DecodedKind,
};

#[derive(Serialize)]
struct Ctx {
    project_name: String,
}

fn evaluate(root_text: &str) -> Result<EvaluatedDefinition<DecodedKind>, String> {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let platform = tokeira_ecs_deployment::platform();
    let parts = DirectoryPartSources::new(package, "tkd");
    let source_name = DefinitionSourceName::AuthoringPath(package.join("deployment.tkd"));
    let frontend = tokeira_platform_definition::tkd::frontend();
    evaluate_definition(
        &frontend,
        DefinitionSource {
            format: frontend.format().clone(),
            source_name,
            bytes: Arc::from(root_text.as_bytes()),
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

#[test]
fn cluster_uses_the_authored_service_connect_namespace() {
    let cluster =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("cluster.tkd"))
            .expect("the shipped cluster part reads");

    assert!(
        cluster.contains("cfg.cluster.service_connect_namespace.clone()"),
        "the cluster default must follow the Service Connect setting"
    );
    assert!(
        !cluster.contains("cfg.networking.private_dns_zone.clone()"),
        "private DNS and Service Connect may be authored independently"
    );
}

// The shipped defaults select managed DSQL: seven modules in dependency
// order, and the complete canonical server-config writeback surface.
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

    // Managed and preexisting modes publish the same complete DSQL surface.
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
            "infrastructure.dsql.admin_role_arn",
            "infrastructure.dsql.conn_lease_table",
            "infrastructure.dsql.endpoint",
            "infrastructure.dsql.rate_limiter_table",
            "infrastructure.dsql.region",
            "infrastructure.dsql.runtime_role_arn",
            "infrastructure.storage",
        ]
    );

    assert_dsql_identities(&output);
    assert_plane_separation(&output);
    assert_workload_role_dependencies(&output);
    assert_realizes(&output);
}

/// Both DSQL modes realize the same DSQL and coordination identities in the
/// dsql module — the invariant every consumer and writeback binds against.
fn assert_dsql_identities(output: &EvaluatedDefinition<DecodedKind>) {
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
            "conn_lease",
            "connection_endpoint",
            "management_endpoint",
            "rate_limiter",
            "runtime_role",
        ]
    );
}

/// Single-owner split: the deploy plane owns exactly the ten workloads
/// (every EcsWorkload sits in the services module), and no ECS task
/// definition or ECS service is authored anywhere in the infrastructure
/// graph — those kinds are not even advertised to the definition.
fn assert_plane_separation(output: &EvaluatedDefinition<DecodedKind>) {
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

/// Every deploy-plane workload stands on its VPC, workload security group,
/// and task role. Edge services also stand on their ALB target group;
/// TokeiraConfig consumers stand on ServerConfig; Grafana stands on the
/// execution role its secret requires.
fn assert_workload_role_dependencies(output: &EvaluatedDefinition<DecodedKind>) {
    for workload in output
        .graph
        .resources()
        .iter()
        .filter(|resource| resource.kind().name() == "EcsWorkload")
    {
        let mut dependency_kinds = workload
            .dependencies()
            .iter()
            .map(|dependency| {
                output
                    .graph
                    .resources()
                    .iter()
                    .find(|resource| resource.reference() == dependency)
                    .expect("verified dependency exists")
                    .kind()
                    .name()
            })
            .collect::<Vec<_>>();
        dependency_kinds.sort_unstable();
        let mut expected = vec!["EcsTaskRole", "SecurityGroup", "Vpc"];
        if workload.logical_id() == "tokeira-grafana" {
            expected.push("EcsExecutionRole");
        }
        if matches!(
            workload.logical_id(),
            "tokeira-edge-api"
                | "tokeira-edge-poll"
                | "tokeira-runtime"
                | "tokeira-projection"
                | "tokeira-admin"
        ) {
            expected.push("ServerConfig");
        }
        if matches!(
            workload.logical_id(),
            "tokeira-edge-api" | "tokeira-edge-poll"
        ) {
            expected.push("AlbTargetGroup");
        }
        expected.sort_unstable();
        assert_eq!(dependency_kinds, expected, "{}", workload.logical_id());
    }
}

fn assert_realizes(output: &EvaluatedDefinition<DecodedKind>) {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    verify_definition(output)
        .realize("demo", package, package, &std::collections::BTreeMap::new())
        .expect("the verified definition realizes every resource and service");
}

// Selecting preexisting DSQL in the root's config keeps the same module set,
// DSQL identities, and canonical writebacks.
#[test]
fn selecting_preexisting_dsql_keeps_identities_and_canonical_writeback() {
    let shipped = shipped_root();
    let managed = "dsql: Dsql::Managed,";
    assert!(shipped.contains(managed), "the dsql literal is as shipped");
    let root = shipped.replace(
        managed,
        "dsql: Dsql::Preexisting(PreexistingDsql {\n            endpoint: \"adopted.dsql.example\".into(),\n            arn: \"arn:aws:dsql:eu-west-2:1:cluster/adopted\".into(),\n            management_endpoint_id: \"vpce-mgmt\".into(),\n            connection_endpoint_id: \"vpce-conn\".into(),\n            runtime_role_arn: \"arn:aws:iam::1:role/runtime\".into(),\n            admin_role_arn: \"arn:aws:iam::1:role/admin\".into(),\n        }),",
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
            "infrastructure.dsql.admin_role_arn",
            "infrastructure.dsql.conn_lease_table",
            "infrastructure.dsql.endpoint",
            "infrastructure.dsql.rate_limiter_table",
            "infrastructure.dsql.region",
            "infrastructure.dsql.runtime_role_arn",
            "infrastructure.storage",
        ]
    );
    assert_dsql_identities(&output);
    assert_plane_separation(&output);
    assert_workload_role_dependencies(&output);
    assert_realizes(&output);
}
