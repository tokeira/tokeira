//! Adapter fidelity (Proposal 004 §19): the interpreted `.tkd`, adapted to
//! `tokeira_orchestrator::Deployment`, must drive the engine to the *same*
//! infrastructure + workloads + namespaces + writeback as the hand-written
//! `ComposeDeployment` — proven through the engine TRAIT surface (the methods the
//! bound `tkp` would call), not just the raw builder artifacts.

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde_json::Value as Json;
use tokeira_compose_deployment::config::{ComposeDsqlConfig, DsqlMode as CfgDsqlMode};
use tokeira_compose_deployment::modules::dsql_resource_id;
use tokeira_compose_deployment::{ComposeConfig, ComposeDeployment};
use tokeira_compose_syn::adapter::{TkdConfig, TkdDeployment};
use tokeira_compose_syn::context::Cx;
use tokeira_deploy_engine::{Service, ServiceContext};
use tokeira_iac::{InfraState, ModuleContext, ModuleSelection, ResourceId, ResourceState, ResourceType};
use tokeira_orchestrator::{Deployment, StorageKind};

const SRC: &str = include_str!("../definition.tkd");

fn dsql_src(region: &str) -> String {
    SRC.replace(
        "storage: Storage::InMemory,",
        &format!(
            "storage: Storage::Dsql {{ region: \"{region}\".into(), mode: DsqlMode::Managed, endpoint: None, arn: None }},"
        ),
    )
}

fn cx(dir: PathBuf, region: &str) -> Cx {
    Cx {
        project_name: "tokeira".into(),
        region: Some(region.into()),
        deployment_dir: dir,
    }
}

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

fn tkd_config(dir: PathBuf, storage: StorageKind, region: &str) -> TkdConfig {
    let source = match storage {
        StorageKind::Dsql => dsql_src(region),
        _ => SRC.to_string(),
    };
    TkdConfig { source, cx: cx(dir, region) }
}

type ServiceShape = BTreeMap<String, (String, Vec<String>, Vec<Json>)>;

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

/// Every infra resource id the deployment provisions, via the engine trait
/// surface: `remote_state_module` + `infra_modules(All)` → each module's
/// `resources()`. This is the "drives the engine to the same infrastructure"
/// claim, independent of module grouping/naming.
fn infra_resource_ids<D: Deployment>(d: &D, cfg: &D::Config, dir: &Path) -> BTreeSet<String> {
    let state = InfraState::default();
    let ext: HashMap<TypeId, Box<dyn Any + Send + Sync>> = HashMap::new();
    let ctx = ModuleContext::new(&state, &ext);

    let mut ids = BTreeSet::new();
    let bootstrap = d.remote_state_module(cfg, dir);
    for r in bootstrap.resources(&ctx).unwrap() {
        ids.insert(r.resource_id().0);
    }
    for m in d.infra_modules(cfg, &ModuleSelection::All) {
        for r in m.resources(&ctx).unwrap() {
            ids.insert(r.resource_id().0);
        }
    }
    ids
}

fn assert_same_engine_drive(storage: StorageKind, region: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let tcfg = tkd_config(dir.clone(), storage, region);
    let rcfg = reference_config(dir.clone(), storage, region);

    // namespaces
    assert_eq!(
        TkdDeployment.required_namespaces(&tcfg),
        ComposeDeployment.required_namespaces(&rcfg),
        "required_namespaces"
    );

    // deploy workloads (services + manifests)
    assert_eq!(
        shape(&TkdDeployment.services(&tcfg)),
        shape(&ComposeDeployment.services(&rcfg)),
        "services"
    );

    // the full set of infra resources the engine would provision
    assert_eq!(
        infra_resource_ids(&TkdDeployment, &tcfg, &dir),
        infra_resource_ids(&ComposeDeployment, &rcfg, &dir),
        "infra resource set"
    );
}

#[test]
fn adapter_drives_engine_like_compose_in_memory() {
    assert_same_engine_drive(StorageKind::InMemory, "us-east-1");
}

#[test]
fn adapter_drives_engine_like_compose_dsql() {
    assert_same_engine_drive(StorageKind::Dsql, "us-east-1");
}

#[test]
fn adapter_collect_writeback_matches_engine() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let tcfg = tkd_config(dir.clone(), StorageKind::Dsql, "us-east-1");
    let rcfg = reference_config(dir, StorageKind::Dsql, "us-east-1");

    // A fully-applied DSQL InfraState (cluster endpoint + both table names).
    let mut state = InfraState::default();
    state.resources.insert(
        dsql_resource_id(&rcfg.project_name),
        rs("cluster_endpoint", "tokeira.dsql.us-east-1.on.aws"),
    );
    state.resources.insert(
        ResourceId(format!("dynamodb-{}-dsql-rate-limiter", rcfg.project_name)),
        rs("table_name", "tokeira-dsql-rate-limiter"),
    );
    state.resources.insert(
        ResourceId(format!("dynamodb-{}-dsql-conn-lease", rcfg.project_name)),
        rs("table_name", "tokeira-dsql-conn-lease"),
    );

    let mut adapter_wb = TkdDeployment.collect_writeback(&tcfg, &state);
    let mut engine_wb = ComposeDeployment.collect_writeback(&rcfg, &state);
    adapter_wb.sort();
    engine_wb.sort();

    // The adapter resolves the deferred Output handles against InfraState into the
    // exact (dotted-key, value) pairs the engine's collect_writeback produces.
    assert_eq!(adapter_wb, engine_wb);
    assert_eq!(adapter_wb.len(), 5, "5 writeback keys under fully-applied DSQL");
}

fn rs(prop: &str, value: &str) -> ResourceState {
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
