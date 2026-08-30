//! Whole-set verification for both shipped EKS definition frontends.
//!
//! Every test evaluates the real modular source set against the package's
//! declared namespaces. No kind, companion, or provider projection is stubbed.

use std::{collections::BTreeMap, path::Path, sync::Arc};

use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use tokeira_platform::{
    author::from_located_value,
    definition::{
        DefinitionFrontend, DefinitionSource, DefinitionSourceName, DirectoryPartSources,
        EvaluatedDefinition, FrontendSource, evaluate_definition, verify_definition,
    },
    kind::DecodedKind,
};

#[derive(Serialize)]
struct Ctx {
    project_name: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedConfig {
    aws: ShippedAws,
    tags: Vec<ShippedTag>,
    networking: ShippedNetworking,
    eks: ShippedEks,
    dsql: ShippedDsql,
    images: ShippedImages,
    services: ShippedServices,
    observability: ShippedObservability,
    debug: ShippedDebug,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedAws {
    region: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedTag {
    key: String,
    value: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedNetworking {
    vpc_cidr: String,
    availability_zones: Vec<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedEks {
    version: String,
    namespace: String,
    node_families: Vec<String>,
    kms_key_arn: Option<String>,
    deletion_protection: bool,
    bootstrap_admin_permissions: bool,
    cluster_admin_principal_arn: Option<String>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum ShippedDsql {
    Managed,
    Preexisting(ShippedPreexistingDsql),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedPreexistingDsql {
    endpoint: String,
    arn: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedImages {
    tokeirad: String,
    controller: String,
    autoscaler: String,
    mimir: String,
    loki: String,
    grafana: String,
    alloy: String,
    pull_policy: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedServices {
    tokeirad: ShippedWorkload,
    controller: ShippedWorkload,
    autoscaler: ShippedWorkload,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedObservability {
    mimir: ShippedWorkload,
    loki: ShippedWorkload,
    grafana: ShippedWorkload,
    retention_days: u32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedWorkload {
    replicas: u32,
    cpu: String,
    memory: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct ShippedDebug {
    cloudwatch_logs: bool,
    log_retention_days: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct GraphProjection {
    namespaces: Vec<String>,
    modules: Vec<(String, Vec<String>)>,
    resources: Vec<ResourceProjection>,
}

type ResourceProjection = (String, String, String, Vec<(String, String)>);

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
    let declaration = tokeira_eks_deployment::platform();
    evaluate_definition(
        frontend,
        DefinitionSource {
            format: frontend.format().clone(),
            source_name: DefinitionSourceName::AuthoringPath(package.join(root_name)),
            bytes: Arc::from(root_text.as_bytes()),
        },
        &Ctx {
            project_name: "demo".into(),
        },
        &declaration.namespaces,
        &DirectoryPartSources::new(package, extension),
    )
    .map_err(|diagnostic| diagnostic.to_string())
}

fn evaluate_tkd(root: &str) -> Result<EvaluatedDefinition<DecodedKind>, String> {
    evaluate(
        root,
        "deployment.tkd",
        "tkd",
        &tokeira_platform_definition::tkd::frontend(),
    )
}

fn evaluate_tkdp(root: &str) -> Result<EvaluatedDefinition<DecodedKind>, String> {
    evaluate(
        root,
        "definition.tkdp",
        "tkdp",
        &tokeira_platform_definition::tkdp::frontend(),
    )
}

fn shipped(name: &str) -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(name))
        .expect("shipped definition root reads")
}

fn graph_projection(output: &EvaluatedDefinition<DecodedKind>) -> GraphProjection {
    GraphProjection {
        namespaces: output.graph.namespaces().to_vec(),
        modules: output
            .graph
            .modules()
            .iter()
            .map(|module| (module.name().into(), module.dependencies().to_vec()))
            .collect(),
        resources: output
            .graph
            .resources()
            .iter()
            .map(|resource| {
                (
                    resource.module().into(),
                    resource.logical_id().into(),
                    resource.kind().name().into(),
                    resource
                        .dependencies()
                        .iter()
                        .map(|dependency| {
                            (dependency.module().into(), dependency.logical_id().into())
                        })
                        .collect(),
                )
            })
            .collect(),
    }
}

fn realize(
    output: &EvaluatedDefinition<DecodedKind>,
) -> tokeira_platform::definition::RealizedResources {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    realize_at(output, package)
}

fn realize_at(
    output: &EvaluatedDefinition<DecodedKind>,
    deployment_dir: &Path,
) -> tokeira_platform::definition::RealizedResources {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    verify_definition(output)
        .realize("demo", deployment_dir, package, &BTreeMap::new())
        .expect("verified definition realizes")
}

fn assert_parity(tkd: &EvaluatedDefinition<DecodedKind>, tkdp: &EvaluatedDefinition<DecodedKind>) {
    let tkd_config: ShippedConfig = from_located_value(tkd.config.clone()).expect("TKD config");
    let tkdp_config: ShippedConfig = from_located_value(tkdp.config.clone()).expect("TKDP config");
    assert_eq!(tkd_config, tkdp_config);
    assert_eq!(graph_projection(tkd), graph_projection(tkdp));
    assert_eq!(tkd.graph.writeback(), tkdp.graph.writeback());
    assert_eq!(realize(tkd).manifests(), realize(tkdp).manifests());
    assert_eq!(tkd.served_companions.len(), 9);
    assert_eq!(tkdp.served_companions.len(), 9);
}

fn preexisting_tkd() -> String {
    shipped("deployment.tkd").replace(
        "dsql: Dsql::Managed,",
        "dsql: Dsql::Preexisting(PreexistingDsql {\n            endpoint: \"adopted.dsql.example\".into(),\n            arn: \"arn:aws:dsql:eu-west-2:1:cluster/adopted\".into(),\n        }),",
    )
}

fn preexisting_tkdp() -> String {
    shipped("definition.tkdp").replace(
        "dsql=ManagedDsql(),",
        "dsql=PreexistingDsql(\n            endpoint=\"adopted.dsql.example\",\n            arn=\"arn:aws:dsql:eu-west-2:1:cluster/adopted\",\n        ),",
    )
}

fn mutated_roots(replicas: u32, retention: u32, logs: bool) -> (String, String) {
    let python_logs = if logs { "True" } else { "False" };
    let tkd = shipped("deployment.tkd")
        .replace(
            "tokeirad: Workload {\n                replicas: 2,",
            &format!("tokeirad: Workload {{\n                replicas: {replicas},"),
        )
        .replace(
            "retention_days: 30,",
            &format!("retention_days: {retention},"),
        )
        .replace(
            "cloudwatch_logs: false,",
            &format!("cloudwatch_logs: {logs},"),
        );
    let tkdp = shipped("definition.tkdp")
        .replace(
            "tokeirad=Workload(replicas=2,",
            &format!("tokeirad=Workload(replicas={replicas},"),
        )
        .replace(
            "retention_days=30,",
            &format!("retention_days={retention},"),
        )
        .replace(
            "cloudwatch_logs=False,",
            &format!("cloudwatch_logs={python_logs},"),
        );
    (tkd, tkdp)
}

fn assert_module_dag(output: &EvaluatedDefinition<DecodedKind>) {
    let modules = output.graph.modules();
    assert_eq!(
        modules
            .iter()
            .filter(|module| module.dependencies().is_empty())
            .count(),
        1
    );
    let positions = modules
        .iter()
        .enumerate()
        .map(|(index, module)| (module.name(), index))
        .collect::<BTreeMap<_, _>>();
    for (index, module) in modules.iter().enumerate() {
        for dependency in module.dependencies() {
            assert!(positions[dependency.as_str()] < index);
        }
    }
    for resource in output.graph.resources() {
        let resource_module = positions[resource.module()];
        for dependency in resource.dependencies() {
            assert!(positions[dependency.module()] <= resource_module);
        }
    }
}

fn writeback_fixture(
    output: &EvaluatedDefinition<DecodedKind>,
    endpoint: &str,
    rate_limiter: &str,
    conn_lease: &str,
) -> Vec<(String, String)> {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let verified = verify_definition(output);
    let realized = verified
        .realize("demo", package, package, &BTreeMap::new())
        .expect("verified definition realizes");
    let mut state = tokeira_iac::InfraState::default();
    for (logical_id, output_name, value) in [
        ("connection-endpoint", "private_hostname", endpoint),
        ("rate-limiter", "table_name", rate_limiter),
        ("conn-lease", "table_name", conn_lease),
    ] {
        let resource_id = realized
            .index()
            .get("dsql", logical_id)
            .expect("writeback resource is indexed")
            .clone();
        state.resources.insert(
            resource_id,
            tokeira_iac::ResourceState {
                resource_type: tokeira_iac::ResourceType::new("fixture"),
                physical_id: logical_id.to_string(),
                properties: serde_json::json!({ output_name: value }),
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "dsql".to_string(),
            },
        );
    }
    verified.resolve_writeback(&realized, &state)
}

// Feature: platform-eks, Property 1
#[test]
fn shipped_frontends_are_exact_peers() {
    let tkd = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    let tkdp = evaluate_tkdp(&shipped("definition.tkdp")).expect("TKDP set evaluates");

    assert_parity(&tkd, &tkdp);
}

#[test]
fn shipped_defaults_pin_current_eks_and_observability_inputs() {
    for output in [
        evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates"),
        evaluate_tkdp(&shipped("definition.tkdp")).expect("TKDP set evaluates"),
    ] {
        let config: ShippedConfig = from_located_value(output.config).expect("config admits");
        assert_eq!(config.eks.version, "1.36");
        assert_eq!(config.eks.node_families, ["m8g", "c8g", "r8g"]);
        assert_eq!(config.images.mimir, "grafana/mimir:3.2.0");
        assert_eq!(config.images.loki, "grafana/loki:3.7.6");
        assert_eq!(config.images.grafana, "grafana/grafana:12.4.9");
        assert_eq!(config.images.alloy, "grafana/alloy:v1.19.0");
    }
}

// Adopted storage changes provider inputs, never graph identity or frontend
// projection. Both roots must therefore remain exact peers in this mode too.
#[test]
fn preexisting_dsql_frontends_are_exact_peers() {
    let tkd = evaluate_tkd(&preexisting_tkd()).expect("adopted TKD set evaluates");
    let tkdp = evaluate_tkdp(&preexisting_tkdp()).expect("adopted TKDP set evaluates");
    assert_parity(&tkd, &tkdp);

    let cluster = realize(&tkd)
        .iter()
        .find(|resource| resource.resource_type().0 == "DsqlCluster")
        .expect("one DSQL cluster")
        .desired_manifest();
    assert_eq!(cluster["mode"], "preexisting");
    assert_eq!(cluster["identity"], "demo");
}

// The module and resource census is the reviewer-facing shape of the shipped
// managed definition, including the five canonical TokeiraConfig writebacks.
#[test]
fn shipped_graph_has_the_canonical_census_and_writebacks() {
    let output = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    assert_eq!(
        output
            .graph
            .modules()
            .iter()
            .map(|module| (module.name(), module.dependencies()))
            .collect::<Vec<_>>(),
        [
            ("remote_state", &[][..]),
            ("images", &["remote_state".to_string()][..]),
            ("networking", &["remote_state".to_string()][..]),
            ("dsql", &["networking".to_string()][..]),
            ("cluster", &["dsql".to_string()][..]),
            (
                "observability",
                &["cluster".to_string(), "images".to_string()][..]
            ),
            ("services", &["observability".to_string()][..]),
        ]
    );
    assert_eq!(output.graph.resources().len(), 51);
    let mut writebacks = output
        .graph
        .writeback()
        .iter()
        .map(|entry| entry.key())
        .collect::<Vec<_>>();
    writebacks.sort_unstable();
    assert_eq!(
        writebacks,
        [
            "infrastructure.dsql.conn_lease_table",
            "infrastructure.dsql.endpoint",
            "infrastructure.dsql.rate_limiter_table",
            "infrastructure.dsql.region",
            "infrastructure.storage",
        ]
    );
    assert_module_dag(&output);
}

// Ian-ruled identity: the evaluation context's deployment name is the only
// deployment discriminator. Neither frontend may reintroduce an environment.
#[test]
fn state_and_dsql_identity_derive_only_from_the_deployment_name() {
    for name in [
        "deployment.tkd",
        "definition.tkdp",
        "platform.tkd",
        "platform.tkdp",
    ] {
        assert!(!shipped(name).contains("environment"), "{name}");
    }
    let output = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    let realized = realize(&output);
    let state = realized
        .iter()
        .find(|resource| resource.resource_type().0 == "RemoteStateBucket")
        .expect("remote state")
        .desired_manifest();
    assert_eq!(state["key_prefix"], "demo");
    let dsql = realized
        .iter()
        .find(|resource| resource.resource_type().0 == "DsqlCluster")
        .expect("DSQL")
        .desired_manifest();
    assert_eq!(dsql["identity"], "demo");
}

fn assert_private_and_least_privilege_plan(output: &EvaluatedDefinition<DecodedKind>) {
    let realized = realize(output);
    let manifests = serde_json::to_string(realized.manifests()).expect("manifests encode");
    assert!(!manifests.contains("0.0.0.0/0"));
    for forbidden in ["InternetGateway", "Ingress", "LoadBalancer"] {
        assert!(!manifests.contains(forbidden), "{forbidden}");
    }
    assert_eq!(
        output
            .graph
            .resources()
            .iter()
            .filter(|resource| resource.kind().name() == "PodIdentityAssociation")
            .count(),
        6
    );
    for service in output
        .graph
        .resources()
        .iter()
        .filter(|resource| resource.kind().name() == "ServiceDeployment")
    {
        assert!(service.dependencies().iter().any(|dependency| {
            output.graph.resources().iter().any(|candidate| {
                candidate.reference() == dependency
                    && candidate.kind().name() == "PodIdentityAssociation"
            })
        }));
    }

    let declared_outputs = realized
        .iter()
        .map(|resource| (resource.resource_id().0, resource.declared_outputs()))
        .collect::<BTreeMap<_, _>>();
    let roles = realized
        .iter()
        .filter(|resource| resource.resource_type().0 == "IamRole")
        .map(|resource| {
            let manifest = resource.desired_manifest();
            let name = manifest["role_name"]
                .as_str()
                .expect("role name is desired")
                .to_string();
            (name, resource, manifest)
        })
        .filter(|(name, _, _)| name.ends_with("-task"))
        .collect::<Vec<_>>();
    assert_eq!(roles.len(), 6);
    for (name, role, manifest) in roles {
        assert!(name.starts_with("demo-"), "{name}");
        assert!(name.ends_with("-task"), "{name}");
        assert_eq!(manifest["inline_policies"], serde_json::json!({}));
        assert_eq!(manifest["managed_policy_arns"], serde_json::json!([]));
        assert!(!manifest.to_string().contains("\"*\""), "{name}");

        let policies = manifest["dependent_inline_policies"]
            .as_array()
            .expect("dependency-backed policies are desired");
        let expected_policy_count = if [
            "demo-tokeirad-task",
            "demo-tokeira-controller-task",
            "demo-tokeira-autoscaler-task",
        ]
        .contains(&name.as_str())
        {
            3
        } else {
            1
        };
        assert_eq!(policies.len(), expected_policy_count, "{name}");

        let dependencies = role
            .dependencies()
            .into_iter()
            .map(|dependency| dependency.0)
            .collect::<Vec<_>>();
        for policy in policies {
            let dependency = policy["dependency"]
                .as_str()
                .expect("policy dependency is desired");
            let property = policy["property"]
                .as_str()
                .expect("policy output is desired");
            assert!(dependencies.iter().any(|candidate| candidate == dependency));
            assert!(
                declared_outputs
                    .get(dependency)
                    .is_some_and(|outputs| outputs.contains(&property)),
                "{name} policy asks `{dependency}.{property}` but the provider does not declare it"
            );
        }
    }
}

// Feature: platform-eks, Property 6
#[test]
fn realized_plan_is_private_and_every_pod_has_pod_identity() {
    let output = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    assert_private_and_least_privilege_plan(&output);
}

// Feature: platform-eks, Property 7
#[test]
fn one_dsql_cluster_is_the_only_datastore() {
    let output = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    assert_eq!(
        output
            .graph
            .resources()
            .iter()
            .filter(|resource| resource.kind().name() == "DsqlCluster")
            .count(),
        1
    );
    for forbidden in ["OpenSearch", "Rds", "Visibility"] {
        assert!(
            output
                .graph
                .resources()
                .iter()
                .all(|resource| !resource.kind().name().contains(forbidden))
        );
    }
}

// Feature: platform-eks, Property 5
#[test]
fn realized_service_graph_is_acyclic_and_matches_startup_order() {
    let output = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    let (_, services) = realize(&output).into_parts();
    let positions = services
        .iter()
        .enumerate()
        .map(|(index, service)| (service.name().to_string(), index))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(positions.len(), 7);
    for (index, service) in services.iter().enumerate() {
        for dependency in service.dependencies() {
            let dependency_index = positions.get(dependency).unwrap_or_else(|| {
                panic!("{} names unknown dependency {dependency}", service.name())
            });
            assert!(dependency_index < &index);
        }
    }
    assert_eq!(
        services
            .iter()
            .find(|service| service.name() == "tokeirad")
            .expect("tokeirad service")
            .dependencies(),
        ["tokeira-controller"]
    );
    assert_eq!(
        services
            .iter()
            .find(|service| service.name() == "tokeira-autoscaler")
            .expect("autoscaler service")
            .dependencies(),
        ["tokeira-controller", "mimir"]
    );
    assert_eq!(
        services
            .iter()
            .find(|service| service.name() == "grafana")
            .expect("grafana service")
            .dependencies(),
        ["mimir", "loki"]
    );
}

#[test]
fn process_configs_resolve_applied_dsql_and_in_cluster_controller_coordinates() {
    let output = evaluate_tkd(&shipped("deployment.tkd")).expect("TKD set evaluates");
    let temp = tempfile::tempdir().expect("deployment dir");
    let mut config = tokeira_config::TokeiraConfig::default();
    config.infrastructure.storage = tokeira_config::ConfigStorageKind::Dsql;
    config.infrastructure.dsql.endpoint = Some("vpce.demo.dsql".to_string());
    config.infrastructure.dsql.region = Some("eu-west-2".to_string());
    std::fs::write(
        temp.path().join("tokeirad.toml"),
        config.to_toml().expect("server config serializes"),
    )
    .expect("server config writes");

    let realized = realize_at(&output, temp.path());
    let endpoint_id = realized
        .index()
        .get("dsql", "connection-endpoint")
        .expect("endpoint identity")
        .clone();
    let (_, services) = realized.into_parts();
    let mut ctx = tokeira_deploy_engine::ServiceContext::default();
    ctx.infra_state.resources.insert(
        endpoint_id,
        tokeira_iac::ResourceState {
            resource_type: tokeira_iac::ResourceType::new("DsqlConnectionEndpoint"),
            physical_id: "vpce-123".to_string(),
            properties: serde_json::json!({ "private_hostname": "vpce.demo.dsql" }),
            dependencies: Vec::new(),
            created_at: String::new(),
            updated_at: String::new(),
            module: "dsql".to_string(),
        },
    );

    let config_for = |name: &str| {
        let service = services
            .iter()
            .find(|service| service.name() == name)
            .unwrap_or_else(|| panic!("{name} service"));
        let manifests = service.manifests(&ctx).expect("process config renders");
        manifests
            .iter()
            .find(|manifest| manifest["kind"] == "ConfigMap")
            .and_then(|manifest| manifest["data"].as_object())
            .and_then(|data| data.values().next())
            .and_then(serde_json::Value::as_str)
            .expect("process ConfigMap content")
            .to_string()
    };
    let server = config_for("tokeirad");
    assert!(server.contains("controller_endpoint = \"http://tokeira-controller:9091\""));
    let controller = config_for("tokeira-controller");
    assert!(controller.contains("dsql_endpoint = \"vpce.demo.dsql\""));
    let autoscaler = config_for("tokeira-autoscaler");
    assert!(autoscaler.contains("dsql_endpoint = \"vpce.demo.dsql\""));
    assert!(!controller.contains("__WRITEBACK_"));
    assert!(!autoscaler.contains("__WRITEBACK_"));
}

// Feature: platform-eks, Property 10
#[test]
fn unknown_config_fields_refuse_in_both_frontends() {
    let tkd = shipped("deployment.tkd").replace(
        "aws: Aws {\n            region:",
        "aws: Aws {\n            unknown: true,\n            region:",
    );
    assert!(evaluate_tkd(&tkd).is_err());
    let tkdp = shipped("definition.tkdp").replace(
        "aws=Aws(region=\"eu-west-2\"),",
        "aws=Aws(region=\"eu-west-2\", unknown=True),",
    );
    assert!(evaluate_tkdp(&tkdp).is_err());
}

// Feature: platform-eks, Property 4
#[test]
fn both_frontends_refuse_dsql_retargets_and_admit_replica_changes() {
    let package = Path::new(env!("CARGO_MANIFEST_DIR"));
    let declaration = tokeira_eks_deployment::platform();
    let tkd_source_name = DefinitionSourceName::AuthoringPath(package.join("deployment.tkd"));
    let tkd_parts = DirectoryPartSources::new(package, "tkd");
    let prior_tkd = shipped("deployment.tkd");
    let check_tkd = |current: &str| {
        tokeira_platform_definition::tkd::frontend().retarget_check(
            FrontendSource {
                source_name: &tkd_source_name,
                bytes: prior_tkd.as_bytes(),
            },
            FrontendSource {
                source_name: &tkd_source_name,
                bytes: current.as_bytes(),
            },
            &Ctx {
                project_name: "demo".into(),
            },
            &declaration.namespaces,
            &tkd_parts,
            &tkd_parts,
        )
    };
    let tkdp_source_name = DefinitionSourceName::AuthoringPath(package.join("definition.tkdp"));
    let tkdp_parts = DirectoryPartSources::new(package, "tkdp");
    let prior_tkdp = shipped("definition.tkdp");
    let check_tkdp = |current: &str| {
        tokeira_platform_definition::tkdp::frontend().retarget_check(
            FrontendSource {
                source_name: &tkdp_source_name,
                bytes: prior_tkdp.as_bytes(),
            },
            FrontendSource {
                source_name: &tkdp_source_name,
                bytes: current.as_bytes(),
            },
            &Ctx {
                project_name: "demo".into(),
            },
            &declaration.namespaces,
            &tkdp_parts,
            &tkdp_parts,
        )
    };
    let (reconciled_tkd, reconciled_tkdp) = mutated_roots(3, 30, false);
    let tkd_messages =
        check_tkd(&preexisting_tkd()).expect_err("TKD DSQL identity is create-time immutable");
    assert!(tkd_messages.iter().any(|message| message.contains("dsql")));
    check_tkd(&reconciled_tkd).expect("TKD replicas reconcile");

    let tkdp_messages =
        check_tkdp(&preexisting_tkdp()).expect_err("TKDP DSQL identity is create-time immutable");
    assert!(tkdp_messages.iter().any(|message| message.contains("dsql")));
    check_tkdp(&reconciled_tkdp).expect("TKDP replicas reconcile");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    // Feature: platform-eks, Property 1
    #[test]
    fn frontend_parity_holds_across_admitted_config(
        replicas in 1_u32..5,
        retention in 1_u32..366,
        logs in any::<bool>(),
    ) {
        let (tkd_root, tkdp_root) = mutated_roots(replicas, retention, logs);
        let tkd = evaluate_tkd(&tkd_root).expect("generated TKD config evaluates");
        let tkdp = evaluate_tkdp(&tkdp_root).expect("generated TKDP config evaluates");
        assert_parity(&tkd, &tkdp);
    }

    // Feature: platform-eks, Property 2
    #[test]
    fn module_dag_holds_across_admitted_config(
        replicas in 0_u32..5,
        retention in 1_u32..366,
        logs in any::<bool>(),
    ) {
        let (root, _) = mutated_roots(replicas, retention, logs);
        let output = evaluate_tkd(&root).expect("generated TKD config evaluates");
        assert_module_dag(&output);
    }

    // Feature: platform-eks, Property 6
    #[test]
    fn private_and_least_privilege_plan_holds_across_admitted_config(
        replicas in 0_u32..5,
        retention in 1_u32..366,
        logs in any::<bool>(),
    ) {
        let (root, _) = mutated_roots(replicas, retention, logs);
        let output = evaluate_tkd(&root).expect("generated TKD config evaluates");
        assert_private_and_least_privilege_plan(&output);
    }

    // Feature: platform-eks, Property 4
    #[test]
    fn both_frontends_admit_reconcilable_changes_across_config(
        replicas in 0_u32..5,
        retention in 1_u32..366,
        logs in any::<bool>(),
    ) {
        let package = Path::new(env!("CARGO_MANIFEST_DIR"));
        let declaration = tokeira_eks_deployment::platform();
        let (current_tkd, current_tkdp) = mutated_roots(replicas, retention, logs);
        let tkd_source_name = DefinitionSourceName::AuthoringPath(package.join("deployment.tkd"));
        let tkd_parts = DirectoryPartSources::new(package, "tkd");
        let tkd_result = tokeira_platform_definition::tkd::frontend().retarget_check(
            FrontendSource {
                source_name: &tkd_source_name,
                bytes: shipped("deployment.tkd").as_bytes(),
            },
            FrontendSource {
                source_name: &tkd_source_name,
                bytes: current_tkd.as_bytes(),
            },
            &Ctx { project_name: "demo".into() },
            &declaration.namespaces,
            &tkd_parts,
            &tkd_parts,
        );
        prop_assert!(tkd_result.is_ok(), "{tkd_result:?}");

        let tkdp_source_name = DefinitionSourceName::AuthoringPath(package.join("definition.tkdp"));
        let tkdp_parts = DirectoryPartSources::new(package, "tkdp");
        let prior_tkdp = shipped("definition.tkdp");
        let tkdp_result = tokeira_platform_definition::tkdp::frontend().retarget_check(
            FrontendSource {
                source_name: &tkdp_source_name,
                bytes: prior_tkdp.as_bytes(),
            },
            FrontendSource {
                source_name: &tkdp_source_name,
                bytes: current_tkdp.as_bytes(),
            },
            &Ctx { project_name: "demo".into() },
            &declaration.namespaces,
            &tkdp_parts,
            &tkdp_parts,
        );
        prop_assert!(tkdp_result.is_ok(), "{tkdp_result:?}");
    }

    // Feature: platform-eks, Property 10
    #[test]
    fn admitted_config_round_trips_without_loss(
        replicas in 0_u32..5,
        retention in 1_u32..366,
        logs in any::<bool>(),
    ) {
        let (root, _) = mutated_roots(replicas, retention, logs);
        let output = evaluate_tkd(&root).expect("generated TKD config evaluates");
        let config: ShippedConfig = from_located_value(output.config).expect("config admits");
        let encoded = serde_json::to_value(&config).expect("config serializes");
        let decoded: ShippedConfig = serde_json::from_value(encoded).expect("config deserializes");
        prop_assert_eq!(config, decoded);
    }

    // Feature: platform-eks, Property 3
    #[test]
    fn writeback_resolves_exactly_the_declared_dsql_identity(
        endpoint in "[a-z0-9.-]{1,32}",
        rate_limiter in "[a-zA-Z0-9_-]{1,32}",
        conn_lease in "[a-zA-Z0-9_-]{1,32}",
    ) {
        let output = evaluate_tkd(&shipped("deployment.tkd"))
            .expect("TKD set evaluates");
        let resolved = writeback_fixture(
            &output,
            &endpoint,
            &rate_limiter,
            &conn_lease,
        ).into_iter().collect::<BTreeMap<_, _>>();
        prop_assert_eq!(resolved.len(), 5);
        prop_assert_eq!(resolved.get("infrastructure.storage"), Some(&"dsql".to_string()));
        prop_assert_eq!(resolved.get("infrastructure.dsql.endpoint"), Some(&endpoint));
        prop_assert_eq!(resolved.get("infrastructure.dsql.region"), Some(&"eu-west-2".to_string()));
        prop_assert_eq!(resolved.get("infrastructure.dsql.rate_limiter_table"), Some(&rate_limiter));
        prop_assert_eq!(resolved.get("infrastructure.dsql.conn_lease_table"), Some(&conn_lease));
    }
}
