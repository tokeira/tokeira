//! Platform resolution + dispatch for the applying verbs (task 8.3 breadth).
//!
//! A deployment is **compose-syn** if it carries a `definition.tkd` (the
//! interpreted config revision); otherwise **local**. Per Proposal 004 §19, `tkp`
//! loads + interprets the `.tkd` (validating it at load) and drives the same
//! engine over `tokeira_orchestrator::Deployment` — the structure is config, not
//! compiled code. (ECS is deferred.)

use std::path::Path;

use anyhow::{Context, Result};
use tokeira_compose::{ComposeError, ComposePlatform};
use tokeira_compose_syn::{
    adapter::{TkdConfig, TkdDeployment},
    context::Cx,
};
use tokeira_iac::{Change, ModuleSelection};
use tokeira_local_deployment::{LocalConfig, LocalDeployment};
use tokeira_orchestrator::{Deployment, InfraEngine};
use tokeira_provisioner_cli::{ProvisionerPlatform, Realization};

const TKD_FILE: &str = "definition.tkd";

/// The transitional bundled realization: BOTH `local` and `compose-syn`, resolved
/// per-deployment by directory content ([`detect`]). A per-platform binary
/// (task 15.4) implements the same seam with exactly one platform and no
/// detection.
pub(crate) struct BundledPlatform;

impl ProvisionerPlatform for BundledPlatform {
    fn label(&self, deployment_dir: &Path) -> &'static str {
        detect(deployment_dir).label()
    }

    fn config_basename(&self, deployment_dir: &Path) -> &'static str {
        match detect(deployment_dir) {
            Platform::ComposeSyn => TKD_FILE,
            Platform::Local => "deployment.toml",
        }
    }

    fn deployment_id(&self, deployment_dir: &Path) -> Result<String> {
        // Historical bundled behavior: the Day-0 identity comes from the local
        // config (defaulted when absent) regardless of platform. The compose
        // per-platform binary derives it from the deployment dir name instead
        // (aligning with `Cx.project_name`) — that lands with the split (15.4).
        Ok(load_local_config(deployment_dir)?.project_name)
    }

    async fn infra_plan(&self, deployment_dir: &Path) -> Result<Vec<Change>> {
        infra_plan(deployment_dir).await
    }

    async fn infra_apply(&self, deployment_dir: &Path) -> Result<usize> {
        infra_apply(deployment_dir).await
    }

    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize> {
        infra_destroy(deployment_dir).await
    }

    async fn deploy_plan(&self, deployment_dir: &Path) -> Result<Realization<Vec<Change>>> {
        // The workload rides the infra universe on both bundled platforms:
        // compose-syn models its tokeirad containers as infra resources, and
        // local has no separate workload notion.
        Ok(Realization::Realized(infra_plan(deployment_dir).await?))
    }

    async fn deploy_apply(&self, deployment_dir: &Path) -> Result<Realization<usize>> {
        Ok(Realization::Realized(infra_apply(deployment_dir).await?))
    }

    async fn scale(&self, deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: match detect(deployment_dir) {
                Platform::ComposeSyn => {
                    "compose-syn has no scale dimension (scaling lands with ECS)"
                }
                Platform::Local => "the local platform has no scale dimension",
            },
        })
    }
}

pub(crate) fn load_local_config(deployment_dir: &Path) -> Result<LocalConfig> {
    let path = deployment_dir.join("deployment.toml");
    if path.exists() {
        tokeira_config::load_config(&path, None)
            .with_context(|| format!("failed to load {}", path.display()))
    } else {
        // A missing config falls back to defaults so the flow is exercisable; a
        // real deployment carries a `deployment.toml`.
        Ok(LocalConfig::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Platform {
    Local,
    ComposeSyn,
}

impl Platform {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Platform::Local => "local",
            Platform::ComposeSyn => "compose-syn",
        }
    }
}

/// Resolve a deployment's platform from its directory.
pub(crate) fn detect(deployment_dir: &Path) -> Platform {
    if deployment_dir.join(TKD_FILE).exists() {
        Platform::ComposeSyn
    } else {
        Platform::Local
    }
}

/// Load + validate the compose-syn `.tkd` config revision, seeding the
/// engine-injected [`Cx`].
///
/// `Cx.project_name` is the **deployment name** — the deployment dir's basename
/// (`{registry}/{name}/`). It is the compose project / container-name prefix, so
/// it must be the operator's chosen name, not a default. `Cx.region` is `None`:
/// region lives in the `.tkd`'s own `config()` (`Storage::Dsql { region }`), which
/// the definition reads directly, so `cx.region` is unused by compose.
pub(crate) fn load_tkd_config(deployment_dir: &Path) -> Result<TkdConfig> {
    let path = deployment_dir.join(TKD_FILE);
    let source = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let project_name = deployment_dir
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("tokeira")
        .to_string();
    let cx = Cx {
        project_name,
        region: None,
        deployment_dir: deployment_dir.to_path_buf(),
    };
    // Validate at load so the engine hot-path `realize()` cannot panic.
    tokeira_compose_syn::interp::interpret(&source, &cx)
        .map_err(|e| anyhow::anyhow!("invalid `.tkd`: {e}"))?;
    Ok(TkdConfig { source, cx })
}

/// Plan the infrastructure for the resolved platform (read-only, no mutation).
pub(crate) async fn infra_plan(deployment_dir: &Path) -> Result<Vec<Change>> {
    match detect(deployment_dir) {
        Platform::Local => {
            plan_infra(
                LocalDeployment,
                load_local_config(deployment_dir)?,
                deployment_dir,
            )
            .await
        }
        Platform::ComposeSyn => {
            let config = load_tkd_config(deployment_dir)?;
            let mut engine = open_compose_syn_engine(config, deployment_dir, false).await?;
            let composition = engine.compose(ModuleSelection::All)?;
            engine
                .plan(&composition, ModuleSelection::All)
                .await
                .context("infrastructure plan failed")
        }
    }
}

/// Apply the infrastructure for the resolved platform. Returns the change count.
pub(crate) async fn infra_apply(deployment_dir: &Path) -> Result<usize> {
    match detect(deployment_dir) {
        Platform::Local => {
            apply_infra(
                LocalDeployment,
                load_local_config(deployment_dir)?,
                deployment_dir,
            )
            .await
        }
        Platform::ComposeSyn => {
            let config = load_tkd_config(deployment_dir)?;
            let mut engine = open_compose_syn_engine(config, deployment_dir, true).await?;
            let composition = engine.compose(ModuleSelection::All)?;
            let changes = engine
                .apply(&composition, ModuleSelection::All)
                .await
                .context("infrastructure apply failed")?;
            Ok(changes.len())
        }
    }
}

/// Destroy the infrastructure for the resolved platform. Returns the change count.
pub(crate) async fn infra_destroy(deployment_dir: &Path) -> Result<usize> {
    match detect(deployment_dir) {
        Platform::Local => {
            destroy_infra(
                LocalDeployment,
                load_local_config(deployment_dir)?,
                deployment_dir,
            )
            .await
        }
        Platform::ComposeSyn => {
            let config = load_tkd_config(deployment_dir)?;
            let mut engine = open_compose_syn_engine(config, deployment_dir, true).await?;
            let composition = engine.compose(ModuleSelection::All)?;
            let removed = engine
                .destroy(&composition, ModuleSelection::All)
                .await
                .context("infrastructure destroy failed")?;
            Ok(removed.len())
        }
    }
}

async fn destroy_infra<D: Deployment>(
    deployment: D,
    config: D::Config,
    dir: &Path,
) -> Result<usize> {
    let mut engine = InfraEngine::new(deployment, &config, dir)
        .await
        .context("failed to open the infrastructure engine")?;
    let composition = engine.compose(ModuleSelection::All)?;
    let changes = engine
        .destroy(&composition, ModuleSelection::All)
        .await
        .context("infrastructure destroy failed")?;
    Ok(changes.len())
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
async fn open_compose_syn_engine(
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

async fn plan_infra<D: Deployment>(
    deployment: D,
    config: D::Config,
    dir: &Path,
) -> Result<Vec<Change>> {
    let mut engine = InfraEngine::new(deployment, &config, dir)
        .await
        .context("failed to open the infrastructure engine")?;
    let composition = engine.compose(ModuleSelection::All)?;
    engine
        .plan(&composition, ModuleSelection::All)
        .await
        .context("infrastructure plan failed")
}

async fn apply_infra<D: Deployment>(deployment: D, config: D::Config, dir: &Path) -> Result<usize> {
    let mut engine = InfraEngine::new(deployment, &config, dir)
        .await
        .context("failed to open the infrastructure engine")?;
    let composition = engine.compose(ModuleSelection::All)?;
    let changes = engine
        .apply(&composition, ModuleSelection::All)
        .await
        .context("infrastructure apply failed")?;
    Ok(changes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference_tkd_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        let src = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../platforms/compose-syn/definition.tkd"
        );
        std::fs::copy(src, tmp.path().join(TKD_FILE)).unwrap();
        tmp
    }

    #[test]
    fn detects_compose_syn_by_tkd_presence() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(detect(tmp.path()), Platform::Local);
        std::fs::write(tmp.path().join(TKD_FILE), "").unwrap();
        assert_eq!(detect(tmp.path()), Platform::ComposeSyn);
    }

    #[tokio::test]
    async fn compose_syn_plan_interprets_the_reference_tkd() {
        // Plan interprets the `.tkd` into the container + observability + state
        // resources. No Docker: container `describe` returns Unsupported, so a
        // fresh deployment still plans (all Creates).
        let tmp = reference_tkd_dir();
        assert_eq!(detect(tmp.path()), Platform::ComposeSyn);
        let changes = infra_plan(tmp.path()).await.expect("plan");
        assert!(
            changes.len() >= 6,
            "compose-syn plan produced only {} changes",
            changes.len()
        );
        assert!(
            changes.iter().any(|c| c.resource_type == "compose_service"),
            "plan includes compose_service container resources"
        );
    }

    #[tokio::test]
    async fn invalid_tkd_is_rejected_at_load() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(TKD_FILE), "not valid rust-via-syn {{{").unwrap();
        assert!(load_tkd_config(tmp.path()).is_err());
    }
}
