//! `tokeira-provisioner-cli` — the platform-agnostic `tkp` shell (Req 14.2).
//!
//! Every per-platform provisioner binary (`tkp-compose`, `tkp-local`, …) is this
//! library plus one [`ProvisionerPlatform`] implementation: the shell owns the
//! lifecycle verbs, the binding-gate orchestration, the operation-lock wrapper,
//! the deployment state envelope, `describe`, the Day-0 stamp, and the
//! config-revision history; the platform supplies only the resource realization.
//! This is the clean split of design §"Per-platform provisioner and three-part
//! provenance": the shell is a distinct layer over `tokeira-provisioner` (the
//! domain library — stamps, binding, integrity), never folded into it.

// CLI shell: stdout/stderr are the operator interface.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::Path;

use anyhow::Result;
use tokeira_iac::Change;
use tokeira_provisioner::DeploymentStateEnvelope;
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

mod apply;
mod cli;
mod config_history;
mod describe;
mod destroy;
mod gate;
mod init;
mod lock;
mod plan;
mod revert;
mod rollback;
mod upgrade;

pub use cli::run;

/// The seam a per-platform provisioner binary implements: the platform-realized
/// verbs plus the platform properties the shell genuinely needs. Everything else
/// — gating, locking, the envelope, revisions, describe — is the shell's.
///
/// Methods take the deployment directory because a provisioner is married to one
/// deployment and every verb operates on it; a single-platform implementation is
/// free to ignore it for the property methods.
#[allow(async_fn_in_trait)] // implementations are workspace-internal and monomorphized; no Send bound needed
pub trait ProvisionerPlatform {
    /// Human label for reports (e.g. `"compose-syn"`).
    fn label(&self, deployment_dir: &Path) -> &'static str;

    /// The config **source** file's basename for this deployment (e.g.
    /// `"definition.tkd"`). The shell keys config-revision snapshots and the
    /// `effective_config_ref` digest on this file.
    fn config_basename(&self, deployment_dir: &Path) -> &'static str;

    /// The deployment identity recorded at Day-0 stamp time.
    fn deployment_id(&self, deployment_dir: &Path) -> Result<String>;

    /// Preview the infrastructure Delta (read-only, no mutation).
    async fn infra_plan(&self, deployment_dir: &Path) -> Result<Vec<Change>>;

    /// Reconcile infrastructure to desired. Returns the change count.
    async fn infra_apply(&self, deployment_dir: &Path) -> Result<usize>;

    /// Tear down the deployment's infrastructure. Returns the removed count.
    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize>;
}

/// The deployment-level envelope store.
///
/// For now a local CAS store under `{deployment_dir}/state/envelope`; cloud
/// deployments will select an `S3StateStore` through the platform store seam
/// (task 13.2), just like the infra/runtime state.
pub(crate) fn envelope_store(
    deployment_dir: &Path,
) -> Box<dyn DeploymentStore<DeploymentStateEnvelope>> {
    Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/envelope"))),
        "envelope".to_string(),
    ))
}

/// A no-op platform for exercising the shell in tests: empty plans, zero-change
/// applies, `deployment.toml` config keying, and the historical `"tokeira"`
/// default identity — the same observable behavior the `local` platform's empty
/// apply gave the shell's tests before the extraction.
#[cfg(test)]
pub(crate) struct TestPlatform;

#[cfg(test)]
impl ProvisionerPlatform for TestPlatform {
    fn label(&self, _deployment_dir: &Path) -> &'static str {
        "test"
    }

    fn config_basename(&self, _deployment_dir: &Path) -> &'static str {
        "deployment.toml"
    }

    fn deployment_id(&self, _deployment_dir: &Path) -> Result<String> {
        Ok("tokeira".to_string())
    }

    async fn infra_plan(&self, _deployment_dir: &Path) -> Result<Vec<Change>> {
        Ok(Vec::new())
    }

    async fn infra_apply(&self, _deployment_dir: &Path) -> Result<usize> {
        Ok(0)
    }

    async fn infra_destroy(&self, _deployment_dir: &Path) -> Result<usize> {
        Ok(0)
    }
}
