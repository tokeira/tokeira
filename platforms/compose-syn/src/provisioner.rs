//! The compose-syn provisioner realization (Req 14): this platform's
//! [`ProvisionerPlatform`] implementation plus the engine wiring behind it.
//!
//! The platform ships its own provisioner: the `tkp-compose` bin target
//! (`src/bin/tkp.rs`) is `tokeira-provisioner-cli` — the platform-agnostic
//! shell — composed with exactly this realization. `tkr deployment create`
//! builds/obtains that binary and places it in the deployment dir as `tkp`.
//!
//! A compose-syn deployment is defined by its `definition.tkd` (the interpreted
//! config revision, Proposal 004 §19): `tkp` loads + interprets the `.tkd`
//! (validating it at load) and drives the engine over
//! `tokeira_orchestrator::Deployment` — the structure is config, not compiled
//! code.

use std::{collections::HashSet, path::Path};

use anyhow::{Context, Result};
use tokeira_compose::{ComposeError, ComposePlatform};
use tokeira_iac::{ModuleSelection, PlanOutcome, ResourceId};
use tokeira_orchestrator::InfraEngine;
use tokeira_provisioner_cli::{
    ChangeLogEntry, ProvisionerPlatform, Realization, change_log_entries,
};

use crate::{
    adapter::{TkdConfig, TkdDeployment},
    context::Cx,
};

const TKD_FILE: &str = "definition.tkd";

/// The compose-syn realization of the provisioner seam. One platform, no
/// detection: a deployment operated by `tkp-compose` **is** compose-syn.
#[derive(Debug, Clone, Copy, Default)]
pub struct ComposeSynPlatform;

impl ProvisionerPlatform for ComposeSynPlatform {
    fn label(&self, _deployment_dir: &Path) -> &'static str {
        "compose-syn"
    }

    fn config_basename(&self, _deployment_dir: &Path) -> &'static str {
        TKD_FILE
    }

    fn deployment_id(&self, deployment_dir: &Path) -> Result<String> {
        // The deployment identity is the deployment dir's basename — the same
        // derivation as `Cx.project_name` (the compose project / container-name
        // prefix), so the Day-0 stamp and the realized resources agree.
        Ok(project_name(deployment_dir))
    }

    async fn definition_check(
        &self,
        deployment_dir: &Path,
        source: Option<&Path>,
    ) -> Result<Realization<()>> {
        // The whole check IS the load: parse + subset + interpret, in memory —
        // the loader touches no provider and writes no state. `source` is
        // authoring mode: any `.tkd` on disk, no deployment required.
        let definition = source
            .map(Path::to_path_buf)
            .unwrap_or_else(|| deployment_dir.join(TKD_FILE));
        load_tkd_config_from(deployment_dir, &definition).map(|_| Realization::Realized(()))
    }

    async fn desired_snapshot(
        &self,
        deployment_dir: &Path,
        definition: &Path,
    ) -> Result<Realization<tokeira_provisioner_cli::DesiredSnapshot>> {
        // Interpret + realize in memory — the same path as `definition
        // check`, so a broken source yields the located verdict, never a
        // partial snapshot. No provider, no live state, no writes.
        let (_source, cx, deployment) = interpret_definition(deployment_dir, definition)?;
        Ok(Realization::Realized(deployment.desired_snapshot(&cx)))
    }

    async fn infra_plan(&self, deployment_dir: &Path) -> Result<PlanOutcome> {
        let config = load_tkd_config(deployment_dir)?;
        let mut engine = open_engine(config, deployment_dir, false).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        engine
            .plan(&composition, ModuleSelection::All)
            .await
            .context("infrastructure plan failed")
    }

    async fn infra_apply(&self, deployment_dir: &Path) -> Result<Vec<ChangeLogEntry>> {
        let config = load_tkd_config(deployment_dir)?;
        let mut engine = open_engine(config, deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let changes = engine
            .apply(&composition, ModuleSelection::All)
            .await
            .context("infrastructure apply failed")?;
        Ok(change_log_entries(&changes))
    }

    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize> {
        let config = load_tkd_config(deployment_dir)?;
        let mut engine = open_engine(config, deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let removed = engine
            .destroy(&composition, ModuleSelection::All)
            .await
            .context("infrastructure destroy failed")?;
        Ok(removed.len())
    }

    async fn infra_destroy_selected(
        &self,
        deployment_dir: &Path,
        ids: &[String],
    ) -> Result<Vec<ChangeLogEntry>> {
        let config = load_tkd_config(deployment_dir)?;
        // Deleting live containers needs the Docker handle, like destroy.
        let mut engine = open_engine(config, deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let id_set: HashSet<ResourceId> = ids.iter().cloned().map(ResourceId).collect();
        let deleted = engine
            .destroy_selected(&composition, &id_set)
            .await
            .context("the delete-only pass failed")?;
        Ok(change_log_entries(&deleted))
    }

    async fn deploy_plan(&self, deployment_dir: &Path) -> Result<Realization<PlanOutcome>> {
        // The workload rides the infra universe: the tokeirad containers are
        // compose_service infra resources of the interpreted `.tkd`.
        Ok(Realization::Realized(
            self.infra_plan(deployment_dir).await?,
        ))
    }

    async fn deploy_apply(
        &self,
        deployment_dir: &Path,
    ) -> Result<Realization<Vec<ChangeLogEntry>>> {
        Ok(Realization::Realized(
            self.infra_apply(deployment_dir).await?,
        ))
    }

    async fn scale(&self, _deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: "compose-syn has no scale dimension (scaling lands with ECS)",
        })
    }
}

/// The deployment identity: the deployment dir's basename (`{registry}/{name}/`).
/// It is the compose project / container-name prefix, so it must be the
/// operator's chosen name, not a default.
fn project_name(deployment_dir: &Path) -> String {
    deployment_dir
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("tokeira")
        .to_string()
}

/// Load + validate the compose-syn `.tkd` config revision, seeding the
/// engine-injected [`Cx`].
///
/// `Cx.region` is `None`: region lives in the `.tkd`'s own `config()`
/// (`Storage::Dsql { region }`), which the definition reads directly, so
/// `cx.region` is unused by compose.
pub fn load_tkd_config(deployment_dir: &Path) -> Result<TkdConfig> {
    load_tkd_config_from(deployment_dir, &deployment_dir.join(TKD_FILE))
}

/// [`load_tkd_config`] with an explicit definition file — the `definition
/// check --definition` authoring path; the deployment dir still seeds the
/// interpretation context (project name, state anchors).
fn load_tkd_config_from(deployment_dir: &Path, path: &Path) -> Result<TkdConfig> {
    let (source, cx, _) = interpret_definition(deployment_dir, path)?;
    Ok(TkdConfig { source, cx })
}

/// The one interpretation path every definition consumer shares — config
/// loading, `definition check`, and desired snapshots all verify a source
/// through exactly this function (causality Requirement 1.6), so the located
/// verdict and the realized shape can never diverge between them.
fn interpret_definition(
    deployment_dir: &Path,
    path: &Path,
) -> Result<(String, Cx, crate::builder::Deployment)> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let cx = Cx {
        project_name: project_name(deployment_dir),
        region: None,
        deployment_dir: deployment_dir.to_path_buf(),
    };
    // Validate at load so the engine hot-path `realize()` cannot panic. The
    // wording is the operator verdict every verb inherits ("the definition
    // does not verify: parse error at line 112, …"); `definition check`
    // renders the same interpreter error as its report payload.
    let (deployment, _config) = crate::interp::interpret(&source, &cx)
        .map_err(|e| anyhow::anyhow!("the definition does not verify: {e}"))?;
    Ok((source, cx, deployment))
}

/// Open the compose-syn infra engine and register the live-apply Docker handle
/// that compose-syn's `register_infra_extensions` leaves for `tkp` to provide
/// (its container resources read `ComposePlatform` from the context).
///
/// The handle is registered only when the Docker daemon actually **responds**:
/// `ComposePlatform::connect` succeeds whenever the socket *file* exists (it does
/// not ping), so an installed-but-stopped daemon would otherwise be registered
/// and then fail inside `describe`. We probe with `ensure_reachable` first.
/// `require_docker` (apply/destroy) errors clearly when the daemon is missing or
/// unreachable; otherwise (plan) its absence is tolerated — an unregistered
/// platform makes container `describe` return `Unsupported`, so a fresh
/// deployment still plans.
async fn open_engine(
    config: TkdConfig,
    dir: &Path,
    require_docker: bool,
) -> Result<InfraEngine<TkdDeployment>> {
    let mut engine = InfraEngine::new(TkdDeployment, &config, dir)
        .await
        .context("failed to open the infrastructure engine")?;

    // compose-syn assembles containers through the Docker API (Bollard) in the
    // `tokeira-compose` crate — there is no user-authored `docker-compose.yml`; a
    // deployment is defined by its `definition.tkd`, and the InfraEngine's own
    // state store is authoritative. `ComposePlatform::connect` still takes a path
    // for its internal service-state ledger, so point it inside the deployment's
    // state directory rather than a root compose file that would misrepresent the
    // model. (`describe` reads live Docker, not this ledger.)
    let compose_state_ledger = dir.join("state").join("compose-services.yaml");
    let platform = match ComposePlatform::connect(compose_state_ledger, &config.cx.project_name) {
        Ok(platform) => match platform.ensure_reachable().await {
            Ok(()) => Some(platform),
            Err(err) => docker_or_skip(err, require_docker)?,
        },
        Err(err) => docker_or_skip(err, require_docker)?,
    };
    if let Some(platform) = platform {
        engine.provision_context_mut().set_extension(platform);
    }
    // The delete-recovery seam (Property 10): a service removed from the
    // definition has no realizing module, so the engine reconstructs it from
    // its recorded manifest to delete the live container — the recovered
    // `ComposeService::delete` reads the `ComposePlatform` handle registered
    // above. Unrecoverable types stay fail-closed.
    engine
        .provision_context_mut()
        .set_extension(tokeira_iac::ResourceRecovery::new(|state| {
            if state.resource_type.0 != "compose_service" {
                return None;
            }
            tokeira_compose::service_from_manifest(state.properties.clone())
                .ok()
                .map(|service| Box::new(service) as Box<dyn tokeira_iac::Resource>)
        }));
    Ok(engine)
}

/// Turn a Docker connect/reachability failure into either a hard error (apply /
/// destroy — the daemon is required) or a tolerated skip (plan).
fn docker_or_skip(err: ComposeError, require_docker: bool) -> Result<Option<ComposePlatform>> {
    if require_docker {
        Err(anyhow::anyhow!(err))
            .context("compose-syn apply/destroy needs a reachable Docker daemon")
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_tkd_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(TKD_FILE), crate::DEFAULT_TKD).unwrap();
        tmp
    }

    // The demon of 2026-07-21: services bind-mounting config files carried no
    // infra-graph edge to the config resource, and the engine's lexicographic
    // tie-break created containers first — Docker then manufactured DIRECTORY
    // stubs at the missing bind sources and the config writer died on EISDIR.
    // The edge is wired automatically from the typed `Vol::Config` anchors.
    #[tokio::test]
    async fn config_mounting_services_depend_on_the_config_files_resource() {
        let tmp = reference_tkd_dir();
        let config = crate::provisioner::load_tkd_config(tmp.path()).expect("load");
        let realized = crate::adapter::TkdDeployment::realize(&config)
            .realize_module("observability", &config.cx)
            .expect("observability module realizes");

        let config_id =
            crate::observability_config::ObservabilityConfigFilesResource::resource_id_value();
        let mimir = realized
            .iter()
            .find(|r| r.resource_id().0 == "compose/mimir")
            .expect("mimir service resource");
        assert!(
            mimir.dependencies().contains(&config_id),
            "a config-mounting service depends on the config-files resource; got {:?}",
            mimir.dependencies()
        );
    }

    #[tokio::test]
    async fn plan_interprets_the_reference_tkd() {
        // Plan interprets the `.tkd` into the container + observability + state
        // resources. No Docker: container `describe` returns Unsupported, so a
        // fresh deployment still plans (all Creates).
        let tmp = reference_tkd_dir();
        let outcome = ComposeSynPlatform
            .infra_plan(tmp.path())
            .await
            .expect("plan");
        assert!(
            outcome.changes.len() >= 6,
            "compose-syn plan produced only {} changes",
            outcome.changes.len()
        );
        assert!(
            outcome
                .changes
                .iter()
                .any(|c| c.resource_type == "compose_service"),
            "plan includes compose_service container resources"
        );
        // Refresh examines every known resource. Whether a given container
        // describe confirms, denies, or cannot determine existence depends on
        // the environment (a dev machine's Docker answers; CI's may not), so
        // the assertion is coverage totality, never a specific status.
        assert!(outcome.refresh.examined);
        assert!(
            !outcome.refresh.status_by_id.is_empty(),
            "refresh covered the known resources"
        );
    }

    #[tokio::test]
    async fn invalid_tkd_is_rejected_at_load() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(TKD_FILE), "not valid rust-via-syn {{{").unwrap();
        assert!(load_tkd_config(tmp.path()).is_err());
    }

    #[test]
    fn deployment_id_is_the_deployment_dir_basename() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("orders-prod");
        std::fs::create_dir(&dir).unwrap();
        assert_eq!(
            ComposeSynPlatform.deployment_id(&dir).unwrap(),
            "orders-prod"
        );
    }
}
