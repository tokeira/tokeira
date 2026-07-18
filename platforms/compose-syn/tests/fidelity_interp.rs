//! Interpreted fidelity: `interpret(definition.tkd)` must produce a `Deployment`
//! identical to (a) the compiled `definition.rs` and (b) the real engine
//! `ComposeDeployment`. The three-way lock proves the `syn` interpreter realizes
//! the operator definition with full fidelity (Proposal 004 Phase 5).

// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

use serde_json::Value as Json;
use tokeira_compose_deployment::{
    ComposeConfig, ComposeDeployment,
    config::{ComposeDsqlConfig, DsqlMode as CfgDsqlMode},
};
use tokeira_compose_syn::{builder::Deployment, context::Cx, definition, interp::interpret};
use tokeira_deploy_engine::{Service, ServiceContext};
use tokeira_orchestrator::{Deployment as _, StorageKind};

const SRC: &str = include_str!("../definition.tkd");

/// The DSQL variant of the `.tkd` — as if the operator edited `config()`'s
/// storage line. Exercises the full parse+eval of a `Storage::Dsql { .. }` literal.
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

/// The compiled operator definition, for the storage mode.
fn compiled(storage: StorageKind, region: &str, cx: &Cx) -> Deployment {
    let mut cfg = definition::config();
    if storage == StorageKind::Dsql {
        cfg.storage = definition::Storage::Dsql {
            region: region.into(),
            mode: definition::DsqlMode::Managed,
            endpoint: None,
            arn: None,
        };
    }
    definition::deployment(&cfg, cx)
}

type ServiceShape = BTreeMap<String, (String, Vec<String>, Vec<Json>)>;

fn shape(services: &[Box<dyn Service>]) -> ServiceShape {
    let ctx = ServiceContext::default();
    services
        .iter()
        .map(|s| {
            let deps = s.dependencies().iter().map(|d| d.to_string()).collect();
            let manifests = s.manifests(&ctx).expect("manifests render");
            (
                s.name().to_string(),
                (s.module().to_string(), deps, manifests),
            )
        })
        .collect()
}

/// Per-module resource identity: `module -> {(resource_id, owning_module, deps)}`.
/// Covers the infra resources the registry ctors build (DSQL cluster + tables,
/// observability config files, local state) that the service-shape oracle misses.
type ResourcesShape = BTreeMap<String, BTreeSet<(String, String, Vec<String>)>>;

fn resources_shape(d: &Deployment, cx: &Cx) -> ResourcesShape {
    d.module_names()
        .into_iter()
        .map(|m| {
            let set = d
                .realize_module(m, cx)
                .unwrap_or_default()
                .iter()
                .map(|r| {
                    let mut deps: Vec<String> = r.dependencies().into_iter().map(|x| x.0).collect();
                    deps.sort();
                    (r.resource_id().0.clone(), r.module().to_string(), deps)
                })
                .collect();
            (m.to_string(), set)
        })
        .collect()
}

/// Interpreted == compiled, exactly, across every observable surface.
fn assert_interp_matches_compiled(storage: StorageKind, region: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let cx = cx(dir, region);

    let src = match storage {
        StorageKind::Dsql => dsql_src(region),
        _ => SRC.to_string(),
    };
    let (interp_dep, _cfg) = interpret(&src, &cx).expect("interpret");
    let comp = compiled(storage, region, &cx);

    assert_eq!(interp_dep.namespaces(), comp.namespaces(), "namespaces");
    assert_eq!(
        interp_dep.writeback_entries(),
        comp.writeback_entries(),
        "writeback entries (keys + values)"
    );
    assert_eq!(
        shape(&interp_dep.realize_workloads(&cx)),
        shape(&comp.realize_workloads(&cx)),
        "service workloads"
    );
    assert_eq!(
        resources_shape(&interp_dep, &cx),
        resources_shape(&comp, &cx),
        "per-module infra resources"
    );
}

/// Interpreted == the real engine `ComposeDeployment` (the headline claim).
fn assert_interp_matches_engine(storage: StorageKind, region: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let cx = cx(dir.clone(), region);

    let src = match storage {
        StorageKind::Dsql => dsql_src(region),
        _ => SRC.to_string(),
    };
    let (interp_dep, _cfg) = interpret(&src, &cx).expect("interpret");

    let reference = reference_config(dir, storage, region);
    let ref_services = ComposeDeployment.services(&reference);

    assert_eq!(
        shape(&interp_dep.realize_workloads(&cx)),
        shape(&ref_services),
        "interpreted services vs ComposeDeployment"
    );
    assert_eq!(
        interp_dep.namespaces(),
        ComposeDeployment.required_namespaces(&reference).as_slice(),
        "namespaces vs ComposeDeployment"
    );
}

#[test]
fn in_memory_interpreted_matches_compiled() {
    assert_interp_matches_compiled(StorageKind::InMemory, "us-east-1");
}

#[test]
fn dsql_interpreted_matches_compiled() {
    assert_interp_matches_compiled(StorageKind::Dsql, "us-east-1");
}

#[test]
fn dsql_interpreted_matches_compiled_non_default_region() {
    assert_interp_matches_compiled(StorageKind::Dsql, "eu-west-2");
}

#[test]
fn in_memory_interpreted_matches_engine() {
    assert_interp_matches_engine(StorageKind::InMemory, "us-east-1");
}

#[test]
fn dsql_interpreted_matches_engine() {
    assert_interp_matches_engine(StorageKind::Dsql, "us-east-1");
}
