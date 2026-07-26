//! Cheap end-to-end exercise of compose-syn — drives the REAL engine
//! (`InfraEngine`/`DeployEngine`, exactly what tkr/tkp would use) against the
//! interpreted `.tkd`, and renders the observability stack to disk. No tkr, no
//! tkp, no Docker, no AWS — just `cargo test`.
//!
//! What it can't reach cheaply: the live `apply` of the compose *service*
//! resources (mimir/loki/grafana/alloy/tokeirad), which needs the Docker Compose
//! platform. Everything else — interpretation, infra composition + plan, the
//! observability artifact rendering (dashboards + alerts), local state, and the
//! deploy-engine service manifests — runs here.

// Live-exercise test narration prints progress deliberately.
#![allow(clippy::print_stdout, clippy::print_stderr)]
use std::{fs, path::PathBuf};

use tokeira_compose_syn::{
    DEFAULT_TKD,
    adapter::{TkdConfig, TkdDeployment},
    context::Cx,
    interp,
};
use tokeira_deploy_engine::ServiceContext;
use tokeira_iac::{ChangeKind, ModuleSelection, ProvisionContext};
use tokeira_orchestrator::{Deployment, InfraEngine};

fn cx(dir: PathBuf) -> Cx {
    Cx {
        project_name: "demo".into(),
        region: Some("us-east-1".into()),
        deployment_dir: dir,
    }
}

#[tokio::test]
async fn exercise_compose_syn_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let cfg = TkdConfig {
        source: DEFAULT_TKD.to_string(),
        cx: cx(dir.clone()),
    };

    // ── 1. Drive the real InfraEngine: interpret → compose → plan ────────────
    let mut infra = InfraEngine::new(TkdDeployment, &cfg, &dir).await.unwrap();
    let composition = infra.compose(ModuleSelection::All).unwrap();
    let module_names: Vec<&str> = composition
        .desired_modules
        .iter()
        .map(|m| m.name())
        .collect();
    let outcome = infra
        .plan(&composition, ModuleSelection::All)
        .await
        .unwrap();
    let changes = &outcome.changes;

    // From an empty state, every desired resource plans as a Create.
    assert!(!changes.is_empty(), "infra plan should propose creates");
    assert!(
        changes.iter().all(|c| c.kind == ChangeKind::Create),
        "from empty state every change is a Create"
    );
    assert!(outcome.refresh.examined, "plan refreshed live state");
    println!(
        "infra plan: {} resource(s) to create across modules {:?}",
        changes.len(),
        module_names
    );

    // ── 2. Render the observability stack to disk (no Docker) ────────────────
    // The config-files resource is the first in the observability module; the
    // remaining four (mimir/loki/grafana/alloy) are Docker services we skip.
    let (dep, _config) = interp::interpret(&cfg.source, &cfg.cx).unwrap();
    let pctx = ProvisionContext::default();

    let local_state = dep.realize_module("local_state", &cfg.cx).unwrap();
    local_state[0].create(&pctx).await.unwrap();
    assert!(
        dir.join("state").exists(),
        "local-state apply creates the state dir"
    );

    let observability = dep.realize_module("observability", &cfg.cx).unwrap();
    observability[0].create(&pctx).await.unwrap();

    // the alert rules + every dashboard + the base configs are now on disk
    let dashboards = dir.join("config/grafana/dashboards");
    let rendered: Vec<String> = fs::read_dir(&dashboards)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        rendered.len(),
        10,
        "all 10 dashboards rendered: {rendered:?}"
    );
    assert!(
        dir.join("config/mimir/rules/observability-alerts.yaml")
            .exists(),
        "alert rules"
    );
    assert!(dir.join("config/mimir.yaml").exists(), "mimir config");
    assert!(
        dir.join("config/grafana/provisioning/datasources/datasources.yaml")
            .exists()
    );
    println!(
        "observability rendered: {} dashboards + alert rules + base configs under {}",
        rendered.len(),
        dir.join("config").display()
    );

    // ── 3. Deploy-engine service manifests (no Docker) ───────────────────────
    let services = TkdDeployment.services(&cfg);
    assert_eq!(services.len(), 5, "5 compose services");
    let sctx = ServiceContext::default();
    for s in &services {
        assert!(
            !s.manifests(&sctx).unwrap().is_empty(),
            "{} has a manifest",
            s.name()
        );
    }
    let names: Vec<&str> = services.iter().map(|s| s.name()).collect();
    println!(
        "deploy plan: {} service manifest(s): {names:?}",
        services.len()
    );
}

#[tokio::test]
async fn exercise_dsql_variant_plans_the_cluster_and_writeback() {
    // Flip the config to DSQL (the operator edit) and confirm the engine composes
    // the dsql module + the writeback — still no AWS (plan only).
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let source = DEFAULT_TKD.replace(
        "storage: Storage::InMemory,",
        "storage: Storage::Dsql { region: \"us-east-1\".into(), mode: DsqlMode::Managed, endpoint: None, arn: None },",
    );
    let cfg = TkdConfig {
        source,
        cx: cx(dir.clone()),
    };

    let infra = InfraEngine::new(TkdDeployment, &cfg, &dir).await.unwrap();
    let composition = infra.compose(ModuleSelection::All).unwrap();
    let names: Vec<&str> = composition
        .desired_modules
        .iter()
        .map(|m| m.name())
        .collect();
    assert!(
        names.contains(&"dsql"),
        "DSQL config composes the dsql module: {names:?}"
    );

    let (dep, _) = interp::interpret(&cfg.source, &cfg.cx).unwrap();
    assert_eq!(
        dep.writeback_entries().len(),
        5,
        "5 writeback keys under DSQL"
    );
    println!(
        "dsql variant: modules {names:?}, {} writeback keys",
        dep.writeback_entries().len()
    );
}
