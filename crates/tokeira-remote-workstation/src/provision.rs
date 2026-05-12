//! IaC-driven provisioning for the workstation.
//!
//! This module provides the `provision_workstation` and `destroy_workstation`
//! functions that drive the IaC engine with the `WorkstationModule`. State is
//! persisted locally at `~/.tokeira/workstations/<id>/state.json`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokeira_aws::AwsClients;
use tokeira_iac::{Engine, InfraComposition, InfraState, ProvisionContext, StateSaver};
use tokeira_state::{CasStore, LocalBackend};

use crate::module::{WorkstationModule, WorkstationModuleConfig};

/// Provision a workstation using the IaC engine.
///
/// Creates all resources (IAM role, instance profile, security group, EBS
/// volumes, EC2 instance) in dependency order with state persisted after
/// each successful operation.
///
/// Returns the final `InfraState` containing all resource physical IDs.
pub async fn provision_workstation(
    config: WorkstationModuleConfig,
    aws_clients: AwsClients,
    state_dir: &Path,
    ctx_setup: impl FnOnce(&mut ProvisionContext),
) -> Result<InfraState> {
    let rctx = tokeira_aws::ResourceContext {
        project: format!("tokeira-workstation-{}", config.workstation_id),
        region: config.region.clone(),
        tags: workstation_tags(&config.workstation_id),
    };

    let module = WorkstationModule::new(config, rctx);

    let state_store = create_state_store(state_dir)?;
    let (state, _) = state_store.load().await.unwrap_or_default();

    let mut ctx = ProvisionContext::default();
    ctx.state = state;
    ctx.set_extension(aws_clients);

    // Let the caller install TUI progress reporters
    ctx_setup(&mut ctx);

    let composition = InfraComposition {
        desired_modules: vec![Box::new(module)],
        known_modules: vec![], // For create, known = desired
        active_modules: vec!["workstation".to_string()],
    };

    // Build a StateSaver that persists after each mutation
    let store_for_saver = Arc::new(state_store);
    let store_ref = Arc::clone(&store_for_saver);
    let saver: StateSaver = Box::new(move |state| {
        let store = Arc::clone(&store_ref);
        let state = state.clone();
        Box::pin(async move {
            store.save(&state).await.map_err(|e| {
                tokeira_iac::error::IacError::StateError(format!(
                    "failed to persist workstation state: {e}"
                ))
            })
        })
    });

    let engine = Engine::new();
    let _changes = engine.apply(&composition, &mut ctx, Some(&saver)).await?;

    Ok(ctx.state)
}

/// Destroy a workstation using the IaC engine.
///
/// Deletes all resources in reverse dependency order (instance first, then
/// volumes, then SG, then IAM). State is updated after each deletion.
pub async fn destroy_workstation(
    config: WorkstationModuleConfig,
    aws_clients: AwsClients,
    state_dir: &Path,
    ctx_setup: impl FnOnce(&mut ProvisionContext),
) -> Result<InfraState> {
    let rctx = tokeira_aws::ResourceContext {
        project: format!("tokeira-workstation-{}", config.workstation_id),
        region: config.region.clone(),
        tags: workstation_tags(&config.workstation_id),
    };

    let module = WorkstationModule::new(config, rctx);

    let state_store = create_state_store(state_dir)?;
    let (state, _) = state_store.load().await.unwrap_or_default();

    let mut ctx = ProvisionContext::default();
    ctx.state = state;
    ctx.set_extension(aws_clients);

    ctx_setup(&mut ctx);

    let composition = InfraComposition {
        desired_modules: vec![], // Empty desired = delete everything
        known_modules: vec![Box::new(module)],
        active_modules: vec!["workstation".to_string()],
    };

    let store_for_saver = Arc::new(state_store);
    let store_ref = Arc::clone(&store_for_saver);
    let saver: StateSaver = Box::new(move |state| {
        let store = Arc::clone(&store_ref);
        let state = state.clone();
        Box::pin(async move {
            store.save(&state).await.map_err(|e| {
                tokeira_iac::error::IacError::StateError(format!(
                    "failed to persist workstation state: {e}"
                ))
            })
        })
    });

    let engine = Engine::new();
    let _changes = engine.destroy(&composition, &mut ctx, Some(&saver)).await?;

    Ok(ctx.state)
}

fn create_state_store(state_dir: &Path) -> Result<CasStore<InfraState>> {
    std::fs::create_dir_all(state_dir)
        .with_context(|| format!("failed to create state directory: {}", state_dir.display()))?;
    let backend = LocalBackend::new(state_dir.to_path_buf());
    Ok(CasStore::new(Box::new(backend), "infra".to_string()))
}

fn workstation_tags(workstation_id: &str) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    tags.insert("tokeira-workstation".into(), "true".into());
    tags.insert("workstation-id".into(), workstation_id.into());
    tags.insert("ManagedBy".into(), "tokeira-cli".into());
    tags
}

/// Resolve the state directory for a workstation.
pub fn state_dir_for(workstation_id: &str) -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".tokeira")
        .join("workstations")
        .join(workstation_id)
}
