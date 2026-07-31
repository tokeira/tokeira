//! Cheap end-to-end exercise of the compose platform — drives the REAL engine
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

use tokeira_compose_deployment::{
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
async fn exercise_compose_end_to_end() {
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

    // ── 1b. The explanation model over the real outcome (evidence-model 7.2):
    // engine → orchestrator → model against the reference definition. The
    // assertions are internal-consistency properties, never a specific
    // refresh status — coverage answers differ between a machine with a
    // Docker daemon and CI without one, and both are correct worlds.
    let explanation = tokeira_explain::explain_plan(
        tokeira_explain::DeploymentContext {
            deployment: "demo".to_string(),
            platform: "compose".to_string(),
            operation: "infra plan".to_string(),
            current_revision: 0,
            proposed_revision: None,
            definition_ref: None,
        },
        &outcome,
    );
    assert_eq!(
        explanation.changes.len(),
        changes.len(),
        "one explained change per engine change"
    );
    for (explained, engine) in explanation.changes.iter().zip(changes) {
        assert_eq!(explained.resource_id, engine.resource);
        assert_eq!(explained.kind, engine.kind);
        assert!(
            explanation
                .evidence
                .resolve(&explained.evidence_id)
                .is_some(),
            "evidence closure over changes"
        );
    }
    for uncertainty in &explanation.uncertainties {
        assert!(
            explanation.evidence.resolve(&uncertainty.subject).is_some(),
            "evidence closure over uncertainty subjects"
        );
    }
    let unknown_planned = explanation
        .changes
        .iter()
        .filter(|c| matches!(c.refresh_status, Some(tokeira_iac::RefreshStatus::Unknown)))
        .count();
    assert_eq!(
        explanation.uncertainties.len(),
        unknown_planned,
        "uncertainty mirrors the refresh coverage exactly"
    );
    println!(
        "explanation: {} changes, {} uncertainties, evidence closed",
        explanation.changes.len(),
        explanation.uncertainties.len()
    );

    // Change-semantics 4.6 (end-to-end half): the declaring kinds' words
    // reach the model over the real engine — every compose-service create
    // in this plan carries a declared, cited operation instead of the
    // all-Unknown default. The `--detail` prose review completes at the
    // rendering phase.
    for change in &explanation.changes {
        if change.resource_type == "compose_service" {
            assert!(
                change.semantics.operation.is_known(),
                "compose service {} declares its operation",
                change.resource_id
            );
            assert_eq!(
                change.display.as_deref(),
                Some("service"),
                "the kind's display noun reaches the model"
            );
        }
    }

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

/// Causality over the live seam chain (explanation-causality task 6.4): a
/// real `ComposeProvisioner::infra_plan`, real desired snapshots of the
/// working and retained definitions, and recorded state read through the
/// platform's own store. The assertions are environment-independent — the
/// edited resource's classification is a content row (D ≠ P needs no live
/// read), and the untouched resources classify by whatever world runs the
/// test (Docker present or absent), but never as the edit.
#[tokio::test]
async fn causality_classifies_a_definition_edit_over_the_live_seam_chain() {
    use std::collections::BTreeMap;

    use tokeira_compose_deployment::provisioner::ComposeProvisioner;
    use tokeira_explain::{BaselineView, CausalityView, Cause, apply_causality};
    use tokeira_iac::{InfraState, ResourceId, ResourceState, ResourceType};
    use tokeira_provisioner_cli::{ProvisionerPlatform, Realization};

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    // Baseline (revision 1): the reference definition, retained where the
    // shell keeps config revisions. Working: the same definition with the
    // grafana image edited.
    let baseline_dir = dir.join("state/config-revisions/1");
    fs::create_dir_all(&baseline_dir).unwrap();
    fs::write(baseline_dir.join("definition.tkd"), DEFAULT_TKD).unwrap();
    let edited = DEFAULT_TKD.replace("grafana/grafana-oss:12.4.3", "grafana/grafana-oss:12.5.0");
    assert_ne!(
        edited, DEFAULT_TKD,
        "the grafana image edit must hit the definition"
    );
    let working = dir.join("definition.tkd");
    fs::write(&working, &edited).unwrap();

    let platform = ComposeProvisioner;
    let snapshot = |path: PathBuf| {
        let dir = dir.clone();
        async move {
            match platform.desired_snapshot(&dir, &path).await.unwrap() {
                Realization::Realized(snapshot) => snapshot,
                Realization::NotApplicable { reason } => panic!("compose snapshots: {reason}"),
            }
        }
    };
    let desired = snapshot(working.clone()).await;
    let baseline = snapshot(baseline_dir.join("definition.tkd")).await;

    // Recorded state: every desired resource recorded (so untouched
    // resources sit in S and cannot fall to the A3b create row), written
    // through the same store layout the engine and `recorded_state` share.
    let recorded_fixture = InfraState {
        version: 1,
        resources: desired
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    ResourceState {
                        resource_type: ResourceType::new("fixture"),
                        physical_id: id.0.clone(),
                        properties: serde_json::json!({}),
                        dependencies: Vec::new(),
                        created_at: "t0".into(),
                        updated_at: "t0".into(),
                        module: "m".into(),
                    },
                )
            })
            .collect(),
        outputs: BTreeMap::new(),
        last_applied: "t0".into(),
    };
    let store_dir = dir.join("state/infra/infra");
    fs::create_dir_all(&store_dir).unwrap();
    fs::write(
        store_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&recorded_fixture).unwrap(),
    )
    .unwrap();
    let recorded = platform.recorded_state(&dir).await.unwrap();
    assert_eq!(
        recorded.resources.len(),
        desired.len(),
        "recorded_state reads the store the fixture wrote"
    );

    // The real plan over the edited definition, through the platform seam.
    let outcome = platform.infra_plan(&dir).await.unwrap();

    // The union graph, exactly as the shell assembles it.
    let mut edges: BTreeMap<ResourceId, Vec<ResourceId>> = outcome
        .edges_by_id
        .iter()
        .map(|(id, deps)| (id.clone(), deps.clone()))
        .collect();
    for (id, state) in &recorded.resources {
        let deps = edges.entry(id.clone()).or_default();
        for dep in &state.dependencies {
            if !deps.contains(dep) {
                deps.push(dep.clone());
            }
        }
    }

    let mut explanation = tokeira_explain::explain_plan(
        tokeira_explain::DeploymentContext {
            deployment: "demo".to_string(),
            platform: "compose".to_string(),
            operation: "infra plan".to_string(),
            current_revision: 1,
            proposed_revision: None,
            definition_ref: None,
        },
        &outcome,
    );
    apply_causality(
        &mut explanation,
        &CausalityView {
            desired: Some(desired),
            baseline: BaselineView::Realized(baseline),
            baseline_revision: 1,
            recorded: recorded.resources,
            edges,
            refresh: outcome.refresh.clone(),
        },
    );

    // The edited resource classifies as the definition edit — a content
    // row, needing no live read.
    let grafana = explanation
        .changes
        .iter()
        .find(|c| c.resource_id == "compose/grafana")
        .expect("the grafana service is in the plan");
    assert!(
        matches!(grafana.cause.value(), Some(Cause::DefinitionEdit { .. })),
        "the edited resource classifies as the definition edit: {:?}",
        grafana.cause
    );
    // Untouched resources never classify as the edit, whatever this world's
    // live answers are (drift or an in-place uncertainty are both correct;
    // the edit is not).
    for change in &explanation.changes {
        if change.kind == ChangeKind::NoChange || change.resource_id == "compose/grafana" {
            continue;
        }
        assert!(
            !matches!(change.cause.value(), Some(Cause::DefinitionEdit { .. })),
            "untouched {} must not classify as the edit: {:?}",
            change.resource_id,
            change.cause
        );
    }
}
