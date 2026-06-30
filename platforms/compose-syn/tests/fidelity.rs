//! Fidelity: the operator's `deployment(cfg, cx)` must produce the *same* engine
//! artifacts as the hand-written `tokeira_compose_deployment::ComposeDeployment`.
//!
//! This drives the real `ComposeDeployment` (the engine's compose platform) and
//! the playground side by side, comparing the deploy-engine service manifests,
//! the required namespaces, and the writeback keys. If the author vocabulary +
//! operator definition are faithful, these are byte-identical — including after
//! the hermetic refactor that relocated the volume/AWS/config-mount mechanics
//! author-side (Proposal 004 Phase 1).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::Value;
use tokeira_compose_deployment::ComposeConfig;
use tokeira_compose_deployment::ComposeDeployment;
use tokeira_compose_deployment::config::{ComposeDsqlConfig, DsqlMode as CfgDsqlMode};
use tokeira_compose_deployment::modules::dsql_resource_id;
use tokeira_compose_syn::context::Cx;
use tokeira_compose_syn::definition::{self, DsqlMode, Storage};
use tokeira_deploy_engine::{Service, ServiceContext};
use tokeira_iac::{InfraState, ResourceId, ResourceState, ResourceType};
use tokeira_orchestrator::{Deployment as _, StorageKind};

/// A comparable projection of a deploy-engine service: its module, ordered deps,
/// and rendered manifests. `serde_json::Value` object equality is key-order
/// independent (so env maps compare cleanly), but array equality is POSITIONAL —
/// so this also locks volume ordering.
type ServiceShape = BTreeMap<String, (String, Vec<String>, Vec<Value>)>;

fn shape(services: &[Box<dyn Service>]) -> ServiceShape {
    let ctx = ServiceContext::default();
    services
        .iter()
        .map(|s| {
            let deps = s.dependencies().iter().map(|d| d.to_string()).collect();
            let manifests = s.manifests(&ctx).expect("manifests render");
            (s.name().to_string(), (s.module().to_string(), deps, manifests))
        })
        .collect()
}

/// The reference `ComposeConfig`, anchored to `dir` so volume host paths match.
fn reference_config(dir: PathBuf, storage: StorageKind, region: &str) -> ComposeConfig {
    let mut config = ComposeConfig {
        storage,
        deployment_dir: dir,
        ..ComposeConfig::default()
    };
    if storage == StorageKind::Dsql {
        config.dsql = Some(ComposeDsqlConfig {
            mode: CfgDsqlMode::Managed,
            region: region.into(),
            ..ComposeDsqlConfig::default()
        });
    }
    config
}

/// The playground `Cx` for the same deployment identity + directory.
fn playground_cx(dir: PathBuf, region: &str) -> Cx {
    Cx {
        project_name: "tokeira".into(),
        region: Some(region.into()),
        deployment_dir: dir,
    }
}

/// A playground config with a managed DSQL cluster in `region`.
fn dsql_config(region: &str) -> definition::Compose {
    let mut cfg = definition::config();
    cfg.storage = Storage::Dsql {
        region: region.into(),
        mode: DsqlMode::Managed,
        endpoint: None,
        arn: None,
    };
    cfg
}

#[test]
fn in_memory_services_match_compose_deployment() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let reference = reference_config(dir.clone(), StorageKind::InMemory, "us-east-1");
    let ref_services = ComposeDeployment.services(&reference);
    let ref_namespaces = ComposeDeployment.required_namespaces(&reference);

    let cx = playground_cx(dir, "us-east-1");
    let d = definition::deployment(&definition::config(), &cx);

    assert_eq!(d.namespaces(), ref_namespaces.as_slice());
    assert_eq!(shape(&d.realize_workloads(&cx)), shape(&ref_services));
}

#[test]
fn dsql_services_match_compose_deployment() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let reference = reference_config(dir.clone(), StorageKind::Dsql, "us-east-1");
    let ref_services = ComposeDeployment.services(&reference);

    let cx = playground_cx(dir, "us-east-1");
    let d = definition::deployment(&dsql_config("us-east-1"), &cx);

    // tokeirad now carries the AWS edge (volumes + env); both read the same
    // process environment, so the manifests are identical.
    let play = shape(&d.realize_workloads(&cx));
    let refm = shape(&ref_services);
    assert_eq!(play, refm);

    // Explicit positional guard on tokeirad's volumes — base→server_config→aws
    // ordering is load-bearing because `to_manifest` serializes the array
    // positionally. (shape() equality already implies this; this localizes a
    // volume-order regression to one assertion.)
    let pv = &play["tokeirad"].2[0]["volumes"];
    assert!(pv.is_array(), "tokeirad volumes should be an array");
    assert_eq!(pv, &refm["tokeirad"].2[0]["volumes"]);
}

#[test]
fn dsql_services_match_in_a_non_default_region() {
    // The AWS_REGION env flows from Storage::Dsql.region; prove fidelity holds
    // when that region is not the us-east-1 default (review: region coupling).
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let reference = reference_config(dir.clone(), StorageKind::Dsql, "eu-west-2");
    let ref_services = ComposeDeployment.services(&reference);

    let cx = playground_cx(dir, "eu-west-2");
    let d = definition::deployment(&dsql_config("eu-west-2"), &cx);

    assert_eq!(shape(&d.realize_workloads(&cx)), shape(&ref_services));
}

#[test]
fn dsql_with_server_config_present_matches() {
    // When tokeirad.toml is present, BOTH the engine and the playground mount it.
    // This exercises the relocated `server_config` branch (the bare tempdir in the
    // other tests has no toml, so that branch is otherwise untested).
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    std::fs::write(dir.join("tokeirad.toml"), "# server config\n").unwrap();

    let reference = reference_config(dir.clone(), StorageKind::Dsql, "us-east-1");
    let ref_services = ComposeDeployment.services(&reference);

    let cx = playground_cx(dir, "us-east-1");
    let d = definition::deployment(&dsql_config("us-east-1"), &cx);

    let play = shape(&d.realize_workloads(&cx));
    assert_eq!(play, shape(&ref_services));

    // The toml mount + TOKEIRA_CONFIG env are present (and, per ordering, the toml
    // mount precedes the ~/.aws mount).
    let vols = play["tokeirad"].2[0]["volumes"].as_array().unwrap();
    let toml_idx = vols.iter().position(|v| v.as_str().unwrap().contains("tokeirad.toml"));
    let aws_idx = vols.iter().position(|v| v.as_str().unwrap().contains("/.aws:"));
    assert!(toml_idx.is_some(), "expected tokeirad.toml mount");
    assert!(aws_idx.is_some(), "expected ~/.aws mount under DSQL");
    assert!(toml_idx < aws_idx, "server_config mount must precede the aws mount");
    assert_eq!(
        play["tokeirad"].2[0]["environment"]["TOKEIRA_CONFIG"],
        Value::from("/etc/tokeira/tokeirad.toml")
    );
}

#[test]
fn dsql_writeback_keys_match_compose_deployment() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let reference = reference_config(dir.clone(), StorageKind::Dsql, "us-east-1");

    // Populate an InfraState the way a real DSQL apply would, so the engine's
    // collect_writeback emits its full key set.
    let mut state = InfraState::default();
    state.resources.insert(
        dsql_resource_id(&reference.project_name),
        resource_state("cluster_endpoint", "tokeira.dsql.us-east-1.on.aws"),
    );
    state.resources.insert(
        ResourceId(format!("dynamodb-{}-dsql-rate-limiter", reference.project_name)),
        resource_state("table_name", "tokeira-dsql-rate-limiter"),
    );
    state.resources.insert(
        ResourceId(format!("dynamodb-{}-dsql-conn-lease", reference.project_name)),
        resource_state("table_name", "tokeira-dsql-conn-lease"),
    );
    let ref_writeback = ComposeDeployment.collect_writeback(&reference, &state);
    let ref_keys: Vec<&str> = ref_writeback.iter().map(|(k, _)| k.as_str()).collect();

    let cx = playground_cx(dir, "us-east-1");
    let d = definition::deployment(&dsql_config("us-east-1"), &cx);
    let keys: Vec<&str> = d
        .writeback_entries()
        .iter()
        .map(|(k, _)| k.as_str())
        .collect();

    assert_eq!(keys, ref_keys);

    // The two literal (non-output) writebacks resolve identically.
    let ref_map: BTreeMap<_, _> = ref_writeback.iter().cloned().collect();
    use tokeira_compose_syn::builder::WbValue;
    for (key, value) in d.writeback_entries() {
        if let WbValue::Const(v) = value {
            assert_eq!(Some(v), ref_map.get(key));
        }
    }
}

fn resource_state(prop: &str, value: &str) -> ResourceState {
    ResourceState {
        resource_type: ResourceType::new("test"),
        physical_id: value.into(),
        properties: serde_json::json!({ prop: value }),
        dependencies: Vec::new(),
        created_at: String::new(),
        updated_at: String::new(),
        module: "dsql".into(),
    }
}
