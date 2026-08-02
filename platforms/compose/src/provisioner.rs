//! The compose provisioner realization (Req 14): this platform's
//! [`ProvisionerPlatform`] implementation plus the engine wiring behind it.
//!
//! The platform ships its own provisioner: the `tkp` bin target
//! (`src/bin/tkp.rs`) is `tokeira-provisioner-cli` — the platform-agnostic
//! shell — composed with exactly this realization. Deployment create
//! builds/obtains that binary and marries it to the deployment dir.
//!
//! A compose deployment is defined by its `definition.tkd` (the interpreted
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
    AppliedOutcome, ChangeLogEntry, ConfigSource, ProvisionerPlatform, Realization,
    change_log_entries,
};

use crate::{
    adapter::{TkdConfig, TkdDeployment},
    context::Cx,
};

const TKD_FILE: &str = "definition.tkd";

/// The compose realization of the provisioner seam. One platform, no
/// detection: this crate's `tkp` is the compose platform's provisioner,
/// married to its deployment at create.
#[derive(Debug, Clone, Copy, Default)]
pub struct ComposeProvisioner;

/// The create-time templates: the platform owns its prototypical
/// `deployment.toml` and `tokeirad.toml`, beside the default definition it
/// already ships.
impl tokeira_orchestrator::PlatformConfig for ComposeProvisioner {
    fn prototypical_config(storage: tokeira_orchestrator::StorageKind) -> String {
        crate::config::prototypical_config_toml(storage)
    }

    fn prototypical_server_config(storage: tokeira_orchestrator::StorageKind) -> String {
        crate::config::prototypical_server_config_toml(storage)
    }
}

impl ProvisionerPlatform for ComposeProvisioner {
    fn label(&self, _deployment_dir: &Path) -> &'static str {
        "compose"
    }

    fn config_source(&self, _deployment_dir: &Path) -> Result<ConfigSource> {
        ConfigSource::definition("tkd", TKD_FILE)
    }

    fn definition_format(&self) -> Option<&'static str> {
        Some("tkd")
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
        // The check is the load plus the verification pass: parse + subset +
        // interpret in memory, then verify the realization — no provider, no
        // state. `source` is authoring mode: any `.tkd` on disk, no
        // deployment required.
        let definition = source
            .map(Path::to_path_buf)
            .unwrap_or_else(|| deployment_dir.join(TKD_FILE));
        let config = load_tkd_config_from(deployment_dir, &definition)?;
        let resources =
            crate::adapter::TkdDeployment::realize(&config).realized_resources(&config.cx);
        let refs: Vec<&dyn tokeira_iac::Resource> = resources.iter().map(|r| r.as_ref()).collect();
        let findings = tokeira_iac::verify_resources(&refs);
        if findings.is_empty() {
            Ok(Realization::Realized(()))
        } else {
            anyhow::bail!("the definition does not verify: {}", findings.join("; "));
        }
    }

    async fn recorded_state(&self, deployment_dir: &Path) -> Result<tokeira_iac::InfraState> {
        // The same store the engine provisions through (adapter::infra_store —
        // the one owner of the convention). A missing store loads the default
        // empty state, which is the honest S for a never-applied deployment.
        let (state, _version) = crate::adapter::infra_store(deployment_dir)
            .load()
            .await
            .context("loading recorded infrastructure state for causality")?;
        Ok(state)
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
        let (mut engine, issue) = open_engine(config, deployment_dir, false).await?;
        // A plan only renders what could execute (output-templates rule 1):
        // updating a recorded resource presupposes describing it, so an
        // unreachable platform refuses with its typed issue — never a
        // record-based plan.
        if let Some(issue) = issue {
            return Ok(PlanOutcome {
                platform_issues: vec![issue],
                ..Default::default()
            });
        }
        let composition = engine.compose(ModuleSelection::All)?;
        engine
            .plan(&composition, ModuleSelection::All)
            .await
            .context("infrastructure plan failed")
    }

    async fn infra_apply(&self, deployment_dir: &Path) -> Result<AppliedOutcome> {
        let config = load_tkd_config(deployment_dir)?;
        let (mut engine, _) = open_engine(config, deployment_dir, true).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let changes = engine
            .apply(&composition, ModuleSelection::All)
            .await
            .context("infrastructure apply failed")?;
        let display_by_id = engine.display_map(&composition)?;
        Ok(AppliedOutcome {
            changes,
            display_by_id,
        })
    }

    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize> {
        let config = load_tkd_config(deployment_dir)?;
        let (mut engine, _) = open_engine(config, deployment_dir, true).await?;
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
        let (mut engine, _) = open_engine(config, deployment_dir, true).await?;
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

    async fn deploy_apply(&self, deployment_dir: &Path) -> Result<Realization<AppliedOutcome>> {
        Ok(Realization::Realized(
            self.infra_apply(deployment_dir).await?,
        ))
    }

    async fn scale(&self, _deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: "the compose platform has no scale dimension (scaling lands with ECS)",
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

/// Load + validate the compose `.tkd` config revision, seeding the
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

/// Open the compose infra engine and register the live-apply Docker handle
/// that the platform's `register_infra_extensions` leaves for `tkp` to provide
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
/// Open the engine, connecting the Docker seam. With `require_docker` the
/// daemon is mandatory and an unreachable one is a hard error (apply /
/// destroy mutate through it); without it (plan), an unreachable daemon
/// comes back as the platform's typed issue — the caller refuses with the
/// issue rather than planning against the record, and the returned `Option`
/// is always `None` when `require_docker` held.
async fn open_engine(
    config: TkdConfig,
    dir: &Path,
    require_docker: bool,
) -> Result<(
    InfraEngine<TkdDeployment>,
    Option<tokeira_iac::PlatformIssue>,
)> {
    let mut engine = InfraEngine::new(TkdDeployment, &config, dir)
        .await
        .context("failed to open the infrastructure engine")?;

    // The platform assembles containers through the Docker API (Bollard) in the
    // `tokeira-compose` crate — there is no user-authored `docker-compose.yml`; a
    // deployment is defined by its `definition.tkd`, and the InfraEngine's own
    // state store is authoritative. `ComposePlatform::connect` still takes a path
    // for its internal service-state ledger, so point it inside the deployment's
    // state directory rather than a root compose file that would misrepresent the
    // model. (`describe` reads live Docker, not this ledger.)
    let compose_state_ledger = dir.join("state").join("compose-services.yaml");
    let (platform, issue) =
        match ComposePlatform::connect(compose_state_ledger, &config.cx.project_name) {
            Ok(platform) => match platform.ensure_reachable().await {
                Ok(()) => (Some(platform), None),
                Err(err) => (None, Some(docker_issue(err, require_docker)?)),
            },
            Err(err) => (None, Some(docker_issue(err, require_docker)?)),
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
    Ok((engine, issue))
}

/// Turn a Docker connect/reachability failure into either a hard error (apply /
/// destroy — the daemon is required) or a tolerated skip (plan).
/// Classify a Docker seam failure. With `require_docker` (apply / destroy)
/// the daemon is mandatory: hard error. Otherwise an unreachable daemon
/// becomes the platform's typed issue through the compose-owned direction
/// table; any non-reachability failure (a broken service-state ledger) stays
/// a hard error — it is not the platform being away.
fn docker_issue(err: ComposeError, require_docker: bool) -> Result<tokeira_iac::PlatformIssue> {
    if require_docker {
        return Err(anyhow::anyhow!(err))
            .context("compose apply/destroy needs a reachable Docker daemon");
    }
    match err {
        ComposeError::DockerNotAvailable {
            socket_path,
            evidence,
        } => Ok(tokeira_compose::docker_unreachable_issue(
            &socket_path,
            &evidence,
        )),
        other => Err(anyhow::anyhow!(other)).context("the compose platform failed to open"),
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

    // The content half of the config coupling: every `Vol::Config` consumer
    // carries the declaration's digest in its manifest, so an authored
    // config-parameter edit diffs the consumers too — the edge alone only
    // orders creation; it cannot restart a running consumer.
    #[test]
    fn config_consumers_carry_the_config_content_digest() {
        use tokeira_iac::ResourceId;

        let tmp = reference_tkd_dir();
        let config = load_tkd_config(tmp.path()).expect("load");
        let snapshot = crate::adapter::TkdDeployment::realize(&config).desired_snapshot(&config.cx);

        let digest_of = |snapshot: &std::collections::BTreeMap<ResourceId, serde_json::Value>,
                         id: &str| {
            snapshot[&ResourceId(id.to_string())]["environment"]["TOKEIRA_CONFIG_DIGEST"]
                .as_str()
                .map(str::to_string)
        };
        let mimir = digest_of(&snapshot, "compose/mimir").expect("mimir consumes config");
        assert!(mimir.starts_with("sha256:"), "digest form: {mimir}");
        assert_eq!(
            digest_of(&snapshot, "compose/tokeirad"),
            None,
            "a service without `Vol::Config` carries no config digest"
        );

        // An authored parameter move moves every consumer's digest.
        let edited = crate::DEFAULT_TKD.replace("retention_hours: 168", "retention_hours: 24");
        assert_ne!(edited, crate::DEFAULT_TKD, "the reference param exists");
        std::fs::write(tmp.path().join(TKD_FILE), edited).unwrap();
        let config = load_tkd_config(tmp.path()).expect("load edited");
        let snapshot = crate::adapter::TkdDeployment::realize(&config).desired_snapshot(&config.cx);
        let moved = digest_of(&snapshot, "compose/mimir").expect("mimir still consumes config");
        assert_ne!(mimir, moved, "a config-parameter edit moves the digest");
    }

    #[tokio::test]
    async fn plan_interprets_the_reference_tkd() {
        // Plan interprets the `.tkd` into the container + observability + state
        // resources. Runs in whichever world hosts the test: with Docker away
        // the platform refuses with its typed issue (output-templates rule 1)
        // — assert the refusal and end; a reachable Docker carries on into
        // the interpretation assertions.
        let tmp = reference_tkd_dir();
        let outcome = ComposeProvisioner
            .infra_plan(tmp.path())
            .await
            .expect("plan");
        if !outcome.platform_issues.is_empty() {
            assert!(
                outcome.changes.is_empty(),
                "an issue-carrying outcome never plans against the record"
            );
            assert_eq!(outcome.platform_issues[0].component, "Docker");
            return;
        }
        assert!(
            outcome.changes.len() >= 6,
            "compose plan produced only {} changes",
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

    // The verification pass behind `definition check` (output-templates §The
    // rules): the reference definition verifies; a dangling `needs` — a kept
    // dependant of a service this definition does not declare — refuses,
    // naming both ends of the broken edge.
    #[tokio::test]
    async fn definition_check_verifies_the_reference_and_refuses_dangling_needs() {
        let tmp = reference_tkd_dir();
        assert!(matches!(
            ComposeProvisioner
                .definition_check(tmp.path(), None)
                .await
                .expect("the reference definition verifies"),
            Realization::Realized(())
        ));

        let dangling = crate::DEFAULT_TKD.replace(
            "needs: vec![\"tokeirad\".into(), \"mimir\".into(), \"loki\".into()]",
            "needs: vec![\"tokeirad\".into(), \"mimir\".into(), \"ghost\".into()]",
        );
        assert_ne!(dangling, crate::DEFAULT_TKD, "the reference needs exist");
        std::fs::write(tmp.path().join(TKD_FILE), dangling).unwrap();
        let err = ComposeProvisioner
            .definition_check(tmp.path(), None)
            .await
            .expect_err("a dangling dependency refuses verification");
        let message = format!("{err:#}");
        assert!(
            message.contains("`compose/alloy` depends on `compose/ghost`"),
            "both ends named: {message}"
        );
        assert!(
            message.contains("does not verify"),
            "the check frame: {message}"
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
            ComposeProvisioner.deployment_id(&dir).unwrap(),
            "orders-prod"
        );
    }
}
