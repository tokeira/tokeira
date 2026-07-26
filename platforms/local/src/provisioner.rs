//! The local provisioner realization (Req 14): this platform's
//! [`ProvisionerPlatform`] implementation.
//!
//! Local is the in-process exception platform — `tkr` drives it directly and
//! never forwards to a bound binary — but the `tkp-local` bin target
//! (`src/bin/tkp.rs`) still exists: it exercises the real shell binary end to
//! end without Docker, keeping "one tkp per platform" uniform.

use std::{collections::HashSet, path::Path};

use anyhow::{Context, Result};
use tokeira_iac::{ModuleSelection, PlanOutcome, ResourceId};
use tokeira_orchestrator::InfraEngine;
use tokeira_provisioner_cli::{
    ChangeLogEntry, ProvisionerPlatform, Realization, change_log_entries,
};

use crate::{LocalConfig, LocalDeployment};

/// The local realization of the provisioner seam.
#[derive(Debug, Clone, Copy, Default)]
pub struct LocalPlatform;

impl ProvisionerPlatform for LocalPlatform {
    fn label(&self, _deployment_dir: &Path) -> &'static str {
        "local"
    }

    fn config_basename(&self, _deployment_dir: &Path) -> &'static str {
        "deployment.toml"
    }

    fn deployment_id(&self, deployment_dir: &Path) -> Result<String> {
        Ok(load_local_config(deployment_dir)?.project_name)
    }

    async fn infra_plan(&self, deployment_dir: &Path) -> Result<PlanOutcome> {
        let mut engine = open_engine(deployment_dir).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        engine
            .plan(&composition, ModuleSelection::All)
            .await
            .context("infrastructure plan failed")
    }

    async fn infra_apply(&self, deployment_dir: &Path) -> Result<Vec<ChangeLogEntry>> {
        let mut engine = open_engine(deployment_dir).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let changes = engine
            .apply(&composition, ModuleSelection::All)
            .await
            .context("infrastructure apply failed")?;
        Ok(change_log_entries(&changes))
    }

    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize> {
        let mut engine = open_engine(deployment_dir).await?;
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
        let mut engine = open_engine(deployment_dir).await?;
        let composition = engine.compose(ModuleSelection::All)?;
        let id_set: HashSet<ResourceId> = ids.iter().cloned().map(ResourceId).collect();
        let deleted = engine
            .destroy_selected(&composition, &id_set)
            .await
            .context("the delete-only pass failed")?;
        Ok(change_log_entries(&deleted))
    }

    async fn deploy_plan(&self, deployment_dir: &Path) -> Result<Realization<PlanOutcome>> {
        // Local has no separate workload notion; the workload rides the
        // infra universe.
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
            reason: "the local platform has no scale dimension",
        })
    }
}

/// Load the deployment's `deployment.toml`, defaulting when absent so the flow
/// is exercisable; a real deployment carries its config.
pub fn load_local_config(deployment_dir: &Path) -> Result<LocalConfig> {
    let path = deployment_dir.join("deployment.toml");
    if path.exists() {
        tokeira_config::load_config(&path, None)
            .with_context(|| format!("failed to load {}", path.display()))
    } else {
        Ok(LocalConfig::default())
    }
}

async fn open_engine(deployment_dir: &Path) -> Result<InfraEngine<LocalDeployment>> {
    let config = load_local_config(deployment_dir)?;
    InfraEngine::new(LocalDeployment, &config, deployment_dir)
        .await
        .context("failed to open the infrastructure engine")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_plan_creates_the_state_dir_and_id_defaults() {
        // The local platform composes exactly one infra resource — the
        // deployment's state dir — and the config-less identity defaults; the
        // shell's Day-0/dev-loop substrate.
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(LocalPlatform.deployment_id(tmp.path()).unwrap(), "tokeira");
        let outcome = LocalPlatform.infra_plan(tmp.path()).await.expect("plan");
        assert_eq!(
            outcome.changes.len(),
            1,
            "local plans the state-dir resource"
        );
        assert_eq!(outcome.changes[0].resource_type, "local_state_dir");
        assert!(outcome.refresh.examined, "the plan performed a refresh");
    }

    #[tokio::test]
    async fn local_config_project_name_is_the_deployment_id() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("deployment.toml"),
            "project_name = \"dev-one\"\n",
        )
        .unwrap();
        assert_eq!(LocalPlatform.deployment_id(tmp.path()).unwrap(), "dev-one");
    }
}
