//! The shipped ECS definition sets, evaluated whole: both modular frontends
//! resolve their companion parts and build the deployment through the real
//! platform namespaces, with nothing stubbed.

use std::{collections::BTreeMap, path::Path, sync::Arc};

use serde::{Deserialize, Serialize};
use tokeira_platform::{
    author::from_located_value,
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

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedConfig {
    environment: String,
    aws: ShippedAws,
    cluster: ShippedCluster,
    networking: ShippedNetworking,
    dsql: ShippedDsql,
    services: ShippedServices,
    capacity: ShippedCapacity,
    alb: ShippedAlb,
    observability: ShippedObservability,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedAws {
    region: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedCluster {
    name: String,
    service_connect_namespace: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedNetworking {
    vpc_cidr: String,
    availability_zones: Vec<String>,
    private_dns_zone: String,
}

#[derive(Debug, PartialEq, Deserialize)]
enum ShippedDsql {
    Managed,
    Preexisting(ShippedPreexistingDsql),
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedPreexistingDsql {
    endpoint: String,
    arn: String,
    management_endpoint_id: String,
    connection_endpoint_id: String,
    runtime_role_arn: String,
    admin_role_arn: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedServices {
    edge_api: ShippedReplicaPolicy,
    edge_poll: ShippedReplicaPolicy,
    runtime: ShippedDaemonPolicy,
    projection: ShippedReplicaPolicy,
    controller: ShippedReplicaPolicy,
    autoscaler: ShippedReplicaPolicy,
    admin: ShippedReplicaPolicy,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedReplicaPolicy {
    image: String,
    replicas: u32,
    cpu: u32,
    memory_mb: u32,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedDaemonPolicy {
    image: String,
    cpu: u32,
    memory_mb: u32,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedCapacity {
    edge_api: ShippedCapacityPlane,
    edge_poll: ShippedCapacityPlane,
    runtime: ShippedCapacityPlane,
    projection: ShippedCapacityPlane,
    control: ShippedCapacityPlane,
    mimir: ShippedCapacityPlane,
    loki: ShippedCapacityPlane,
    grafana: ShippedCapacityPlane,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedCapacityPlane {
    instance_type: String,
    min: u32,
    desired: u32,
    max: u32,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedAlb {
    protocol: ShippedAlbProtocol,
    health_check_path: String,
    health_check_interval_secs: u64,
}

#[derive(Debug, PartialEq, Deserialize)]
enum ShippedAlbProtocol {
    Http2,
    Https(ShippedHttps),
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedHttps {
    certificate_arn: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedObservability {
    mimir: ShippedImage,
    loki: ShippedImage,
    grafana: ShippedImage,
    alloy_image: String,
    aws_cli_image: String,
    busybox_image: String,
}

#[derive(Debug, PartialEq, Deserialize)]
struct ShippedImage {
    image: String,
}

#[derive(Debug, PartialEq, Eq)]
struct GraphProjection {
    namespaces: Vec<String>,
    modules: Vec<ModuleProjection>,
    resources: Vec<ResourceProjection>,
}

#[derive(Debug, PartialEq, Eq)]
struct ModuleProjection {
    name: String,
    dependencies: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct ResourceProjection {
    module: String,
    logical_id: String,
    kind: String,
    dependencies: Vec<(String, String)>,
}

fn evaluate<F>(
    root_text: &str,
    root_name: &str,
    extension: &str,
    frontend: &F,
) -> Result<EvaluatedDefinition<DecodedKind>, String>
where
    F: DefinitionFrontend,
{
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let platform = tokeira_ecs_deployment::platform();
    let parts = DirectoryPartSources::new(package, extension);
    let source_name = DefinitionSourceName::AuthoringPath(package.join(root_name));
    evaluate_definition(
        frontend,
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

fn evaluate_tkd(root_text: &str) -> Result<EvaluatedDefinition<DecodedKind>, String> {
    evaluate(
        root_text,
        "deployment.tkd",
        "tkd",
        &tokeira_platform_definition::tkd::frontend(),
    )
}

fn evaluate_tkdp(root_text: &str) -> Result<EvaluatedDefinition<DecodedKind>, String> {
    evaluate(
        root_text,
        "definition.tkdp",
        "tkdp",
        &tokeira_platform_definition::tkdp::frontend(),
    )
}

fn shipped_tkd_root() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("deployment.tkd"))
        .expect("the shipped TKD root document reads")
}

fn shipped_tkdp_root() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("definition.tkdp"))
        .expect("the shipped TKDP root document reads")
}

fn decode_config(output: &EvaluatedDefinition<DecodedKind>) -> ShippedConfig {
    from_located_value(output.config.clone()).expect("the shipped config admits")
}

fn assert_replica_defaults(shipped: &ShippedReplicaPolicy, legacy: &serde_json::Value) {
    assert_eq!(legacy["image"].as_str(), Some(shipped.image.as_str()));
    assert_eq!(
        legacy["desired_count"].as_u64(),
        Some(shipped.replicas.into())
    );
    assert_eq!(legacy["cpu"].as_u64(), Some(shipped.cpu.into()));
    assert_eq!(legacy["memory_mb"].as_u64(), Some(shipped.memory_mb.into()));
}

fn assert_capacity_defaults(shipped: &ShippedCapacityPlane, legacy: &serde_json::Value) {
    assert_eq!(
        legacy["instance_type"].as_str(),
        Some(shipped.instance_type.as_str())
    );
    assert_eq!(legacy["min_capacity"].as_u64(), Some(shipped.min.into()));
    assert_eq!(
        legacy["desired_capacity"].as_u64(),
        Some(shipped.desired.into())
    );
    assert_eq!(legacy["max_capacity"].as_u64(), Some(shipped.max.into()));
}

fn graph_projection(output: &EvaluatedDefinition<DecodedKind>) -> GraphProjection {
    let namespaces = output.graph.namespaces().to_vec();
    let modules = output
        .graph
        .modules()
        .iter()
        .map(|module| ModuleProjection {
            name: module.name().to_string(),
            dependencies: module.dependencies().to_vec(),
        })
        .collect();
    let resources = output
        .graph
        .resources()
        .iter()
        .map(|resource| ResourceProjection {
            module: resource.module().to_string(),
            logical_id: resource.logical_id().to_string(),
            kind: resource.kind().name().to_string(),
            dependencies: resource
                .dependencies()
                .iter()
                .map(|dependency| {
                    (
                        dependency.module().to_string(),
                        dependency.logical_id().to_string(),
                    )
                })
                .collect(),
        })
        .collect();
    GraphProjection {
        namespaces,
        modules,
        resources,
    }
}

fn realize(
    output: &EvaluatedDefinition<DecodedKind>,
) -> tokeira_platform::definition::RealizedResources {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    verify_definition(output)
        .realize("demo", package, package, &BTreeMap::new())
        .expect("the verified definition realizes every resource and service")
}

fn assert_definition_parity(
    tkd: &EvaluatedDefinition<DecodedKind>,
    tkdp: &EvaluatedDefinition<DecodedKind>,
) {
    assert_eq!(decode_config(tkd), decode_config(tkdp));
    assert_eq!(graph_projection(tkd), graph_projection(tkdp));
    assert_eq!(tkd.graph.writeback(), tkdp.graph.writeback());
    assert_eq!(realize(tkd).manifests(), realize(tkdp).manifests());
    assert_eq!(tkd.served_companions.len(), 9);
    assert_eq!(tkdp.served_companions.len(), 9);
}

/// State placement is admitted before definition evaluation and therefore
/// cannot safely be duplicated as provider desired state. The dependency-free
/// module remains as the graph's ordering root, but it must never realize a
/// bucket whose security or retention policy ECS would then own.
fn assert_state_location_is_shell_owned(output: &EvaluatedDefinition<DecodedKind>) {
    let bootstrap = output
        .graph
        .modules()
        .iter()
        .find(|module| module.name() == "remote_state")
        .expect("the state ordering root exists");
    assert!(bootstrap.dependencies().is_empty());
    assert!(
        output
            .graph
            .resources()
            .iter()
            .all(|resource| resource.module() != "remote_state")
    );

    for dependent in ["images", "networking"] {
        let module = output
            .graph
            .modules()
            .iter()
            .find(|module| module.name() == dependent)
            .expect("state-dependent module exists");
        assert_eq!(module.dependencies(), &["remote_state"]);
    }

    assert!(
        realize(output)
            .iter()
            .all(|resource| resource.resource_type().0 != "RemoteStateBucket")
    );
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

#[test]
fn the_shipped_set_pins_current_observability_images() {
    for output in [
        evaluate_tkd(&shipped_tkd_root()).expect("the shipped TKD set evaluates"),
        evaluate_tkdp(&shipped_tkdp_root()).expect("the shipped TKDP set evaluates"),
    ] {
        let config: ShippedConfig =
            from_located_value(output.config).expect("the shipped config admits");

        assert_eq!(config.observability.mimir.image, "grafana/mimir:3.2.0");
        assert_eq!(config.observability.loki.image, "grafana/loki:3.7.6");
        assert_eq!(config.observability.grafana.image, "grafana/grafana:12.4.9");
        assert_eq!(config.observability.alloy_image, "grafana/alloy:v1.19.0");
        assert_eq!(config.observability.aws_cli_image, "amazon/aws-cli:2.17.0");
        assert_eq!(config.observability.busybox_image, "busybox:1.36");
    }
}

// The legacy model remains a derivation dependency for definition kinds. Its
// defaults must therefore be the same authored policy as the shipped roots,
// even though the live creation path is definition-driven.
#[test]
fn legacy_derivation_defaults_match_the_shipped_definition() {
    let output = evaluate_tkd(&shipped_tkd_root()).expect("the shipped TKD set evaluates");
    let shipped = decode_config(&output);
    let legacy =
        serde_json::to_value(tokeira_ecs::EcsConfig::default()).expect("legacy defaults serialize");

    assert_eq!(
        legacy["environment"].as_str(),
        Some(shipped.environment.as_str())
    );
    assert_eq!(legacy["region"].as_str(), Some(shipped.aws.region.as_str()));
    assert_eq!(
        legacy["cluster"]["name"].as_str(),
        Some(shipped.cluster.name.as_str())
    );
    assert_eq!(
        legacy["cluster"]["service_connect_namespace"].as_str(),
        Some(shipped.cluster.service_connect_namespace.as_str())
    );
    assert_eq!(
        legacy["networking"]["vpc_cidr"].as_str(),
        Some(shipped.networking.vpc_cidr.as_str())
    );
    assert_eq!(
        legacy["networking"]["availability_zones"],
        serde_json::json!(&shipped.networking.availability_zones)
    );
    assert_eq!(
        legacy["networking"]["private_dns_zone"].as_str(),
        Some(shipped.networking.private_dns_zone.as_str())
    );

    assert_replica_defaults(&shipped.services.edge_api, &legacy["services"]["edge_api"]);
    assert_replica_defaults(
        &shipped.services.edge_poll,
        &legacy["services"]["edge_poll"],
    );
    assert_eq!(
        legacy["services"]["runtime"]["image"].as_str(),
        Some(shipped.services.runtime.image.as_str())
    );
    assert_eq!(
        legacy["services"]["runtime"]["cpu"].as_u64(),
        Some(shipped.services.runtime.cpu.into())
    );
    assert_eq!(
        legacy["services"]["runtime"]["memory_mb"].as_u64(),
        Some(shipped.services.runtime.memory_mb.into())
    );
    assert_replica_defaults(
        &shipped.services.projection,
        &legacy["services"]["projection"],
    );
    assert_replica_defaults(
        &shipped.services.controller,
        &legacy["services"]["controller"],
    );
    assert_replica_defaults(
        &shipped.services.autoscaler,
        &legacy["services"]["autoscaler"],
    );
    assert_replica_defaults(&shipped.services.admin, &legacy["services"]["admin"]);
    assert_eq!(
        legacy["autoscaler"]["image"].as_str(),
        Some(shipped.services.autoscaler.image.as_str())
    );

    for (shipped_capacity, legacy_key) in [
        (&shipped.capacity.edge_api, "edge_api"),
        (&shipped.capacity.edge_poll, "edge_poll"),
        (&shipped.capacity.runtime, "runtime"),
        (&shipped.capacity.projection, "projection"),
        (&shipped.capacity.control, "control"),
        (&shipped.capacity.mimir, "mimir"),
        (&shipped.capacity.loki, "loki"),
        (&shipped.capacity.grafana, "grafana"),
    ] {
        assert_capacity_defaults(shipped_capacity, &legacy["capacity_providers"][legacy_key]);
    }

    assert_eq!(
        legacy["alb"]["health_check_path"].as_str(),
        Some(shipped.alb.health_check_path.as_str())
    );
    assert_eq!(
        legacy["alb"]["health_check_interval_secs"].as_u64(),
        Some(shipped.alb.health_check_interval_secs)
    );
    assert_eq!(
        legacy["observability"]["mimir_image"].as_str(),
        Some(shipped.observability.mimir.image.as_str())
    );
    assert_eq!(
        legacy["observability"]["loki_image"].as_str(),
        Some(shipped.observability.loki.image.as_str())
    );
    assert_eq!(
        legacy["observability"]["grafana_image"].as_str(),
        Some(shipped.observability.grafana.image.as_str())
    );
}

// The two source sets are interchangeable authoring projections: a frontend
// choice cannot alter operator configuration or provider intent.
#[test]
fn the_shipped_definition_formats_are_exact_peers() {
    let tkd = evaluate_tkd(&shipped_tkd_root()).expect("the shipped TKD set evaluates");
    let tkdp = evaluate_tkdp(&shipped_tkdp_root()).expect("the shipped TKDP set evaluates");

    assert_definition_parity(&tkd, &tkdp);
}

// Both frontends must preserve the shared shell's state authority. A bucket
// resource here would be too late to bootstrap remote state and would let an
// ECS apply mutate an operator-owned bucket's security or retention controls.
#[test]
fn state_location_remains_outside_ecs_desired_state() {
    for output in [
        evaluate_tkd(&shipped_tkd_root()).expect("the shipped TKD set evaluates"),
        evaluate_tkdp(&shipped_tkdp_root()).expect("the shipped TKDP set evaluates"),
    ] {
        assert_state_location_is_shell_owned(&output);
    }
}

// The shipped defaults select managed DSQL: seven modules in dependency
// order, and the complete canonical server-config writeback surface.
#[test]
fn the_shipped_set_evaluates_with_managed_defaults() {
    let output = evaluate_tkd(&shipped_tkd_root()).expect("the shipped definition set evaluates");
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
    realize(output);
}

fn preexisting_tkd_root() -> String {
    let shipped = shipped_tkd_root();
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
    root
}

fn preexisting_tkdp_root() -> String {
    let shipped = shipped_tkdp_root();
    let managed = "dsql=ManagedDsql(),";
    assert!(shipped.contains(managed), "the dsql literal is as shipped");
    let root = shipped.replace(
        managed,
        "dsql=PreexistingDsql(\n            endpoint=\"adopted.dsql.example\",\n            arn=\"arn:aws:dsql:eu-west-2:1:cluster/adopted\",\n            management_endpoint_id=\"vpce-mgmt\",\n            connection_endpoint_id=\"vpce-conn\",\n            runtime_role_arn=\"arn:aws:iam::1:role/runtime\",\n            admin_role_arn=\"arn:aws:iam::1:role/admin\",\n        ),",
    );
    assert_ne!(root, shipped, "the dsql literal was rewritten");
    root
}

fn https_tkd_root() -> String {
    let shipped = shipped_tkd_root();
    shipped.replace(
        "protocol: AlbProtocol::Http2,",
        "protocol: AlbProtocol::Https(HttpsAlb {\n                certificate_arn: \"arn:aws:acm:eu-west-2:1:certificate/test\".into(),\n            }),",
    )
}

fn https_tkdp_root() -> String {
    let shipped = shipped_tkdp_root();
    shipped.replace(
        "protocol=Http2(),",
        "protocol=Https(\n                certificate_arn=\"arn:aws:acm:eu-west-2:1:certificate/test\",\n            ),",
    )
}

// Variant payloads are part of the operator contract too; parity must hold
// beyond the default unit variants.
#[test]
fn selecting_https_keeps_the_definition_formats_exact_peers() {
    let tkd = evaluate_tkd(&https_tkd_root()).expect("the TKD HTTPS set evaluates");
    let tkdp = evaluate_tkdp(&https_tkdp_root()).expect("the TKDP HTTPS set evaluates");

    assert_definition_parity(&tkd, &tkdp);
}

// Selecting preexisting DSQL in either root keeps the same module set, DSQL
// identities, canonical writebacks, and cross-format provider projection.
#[test]
fn selecting_preexisting_dsql_keeps_identities_and_canonical_writeback() {
    let tkd = evaluate_tkd(&preexisting_tkd_root()).expect("the TKD adopted set evaluates");
    let tkdp = evaluate_tkdp(&preexisting_tkdp_root()).expect("the TKDP adopted set evaluates");

    for output in [&tkd, &tkdp] {
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
        assert_dsql_identities(output);
        assert_plane_separation(output);
        assert_workload_role_dependencies(output);
        assert_realizes(output);
    }
    assert_definition_parity(&tkd, &tkdp);
}
