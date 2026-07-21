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
use tokeira_iac::{Change, ChangeKind};
use tokeira_provisioner::DeploymentStateEnvelope;
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

mod apply;
mod cli;
mod config_history;
mod deploy;
mod describe;
mod destroy;
mod gate;
mod init;
mod lock;
mod marker;
mod plan;
mod revert;
mod rollback;
mod scale;
mod upgrade;

pub use cli::run;
// The seam's audit vocabulary travels with the seam: platform realizations
// return these from their applying verbs (task 19.2).
pub use tokeira_provisioner::{ChangeLogEntry, ChangeOp};

/// The outcome of a **conditionally realized** platform verb (design §"Command
/// behaviour and outputs"): the surface is the same for every platform, and
/// where a platform cannot honor a verb it answers `NotApplicable` — a
/// first-class result the shell turns into a typed non-zero refusal, never a
/// missing subcommand and never a crash.
#[derive(Debug)]
pub enum Realization<T> {
    /// The platform realized the verb.
    Realized(T),
    /// This platform cannot honor the verb; `reason` is operator-facing.
    NotApplicable { reason: &'static str },
}

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

    /// Preview the infrastructure Delta (read-only, no mutation). The infra
    /// verbs are universal — every provisioner provisions infrastructure — so
    /// they are unconditional, unlike the workload verbs below.
    async fn infra_plan(&self, deployment_dir: &Path) -> Result<Vec<Change>>;

    /// Reconcile infrastructure to desired. Returns the **identities** of the
    /// changes committed (ids-only — never before-images, Proposal 002): the
    /// shell prints the count and `upgrade` records them as the audit change
    /// log (task 19.2). [`change_log_entries`] maps an engine Delta.
    async fn infra_apply(&self, deployment_dir: &Path) -> Result<Vec<ChangeLogEntry>>;

    /// Tear down the deployment's infrastructure. Returns the removed count.
    async fn infra_destroy(&self, deployment_dir: &Path) -> Result<usize>;

    /// Preview the workload Delta (read-only). A platform whose workload rides
    /// the infra universe (compose-syn models its containers as infra
    /// resources) realizes this as the infra plan.
    async fn deploy_plan(&self, deployment_dir: &Path) -> Result<Realization<Vec<Change>>>;

    /// Reconcile the workload to desired. Returns the committed change
    /// identities, like [`infra_apply`](Self::infra_apply).
    async fn deploy_apply(&self, deployment_dir: &Path)
    -> Result<Realization<Vec<ChangeLogEntry>>>;

    /// Change workload capacity (`<dim>=<n>` specs). `NotApplicable` where the
    /// platform has no scale dimension. Returns the change count.
    async fn scale(&self, deployment_dir: &Path, specs: &[String]) -> Result<Realization<usize>>;
}

/// Map an applied engine Delta to the ids-only audit entries (task 19.2).
///
/// The id is `module/resource` — the engine's stable resource address. Kinds
/// map onto the audit vocabulary: `Replace` records as `Updated` (the id
/// survives a delete-then-recreate; the audit log tracks identities, not
/// mechanics), and `NoChange` records nothing.
pub fn change_log_entries(changes: &[Change]) -> Vec<ChangeLogEntry> {
    changes
        .iter()
        .filter_map(|change| {
            let op = match change.kind {
                ChangeKind::Create => ChangeOp::Created,
                ChangeKind::Update | ChangeKind::Replace => ChangeOp::Updated,
                ChangeKind::Delete => ChangeOp::Deleted,
                ChangeKind::NoChange => return None,
            };
            Some(ChangeLogEntry {
                id: format!("{}/{}", change.module, change.resource),
                op,
            })
        })
        .collect()
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

    async fn infra_apply(&self, _deployment_dir: &Path) -> Result<Vec<ChangeLogEntry>> {
        Ok(Vec::new())
    }

    async fn infra_destroy(&self, _deployment_dir: &Path) -> Result<usize> {
        Ok(0)
    }

    async fn deploy_plan(&self, _deployment_dir: &Path) -> Result<Realization<Vec<Change>>> {
        Ok(Realization::Realized(Vec::new()))
    }

    async fn deploy_apply(
        &self,
        _deployment_dir: &Path,
    ) -> Result<Realization<Vec<ChangeLogEntry>>> {
        Ok(Realization::Realized(Vec::new()))
    }

    async fn scale(&self, _deployment_dir: &Path, _specs: &[String]) -> Result<Realization<usize>> {
        Ok(Realization::NotApplicable {
            reason: "the test platform has no scale dimension",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::ChangeKind;

    fn change(kind: ChangeKind, module: &str, resource: &str) -> Change {
        Change {
            kind,
            resource_type: "t".to_string(),
            module: module.to_string(),
            resource: resource.to_string(),
            details: Vec::new(),
        }
    }

    // Task 19.2: the audit mapping — module/resource ids, Replace folds to
    // Updated (the id survives the delete-then-recreate), NoChange vanishes.
    #[test]
    fn change_log_entries_map_ids_and_ops() {
        let entries = change_log_entries(&[
            change(ChangeKind::Create, "vpc", "main"),
            change(ChangeKind::Update, "svc", "web"),
            change(ChangeKind::Replace, "db", "primary"),
            change(ChangeKind::Delete, "svc", "old"),
            change(ChangeKind::NoChange, "svc", "steady"),
        ]);
        assert_eq!(entries.len(), 4, "NoChange records nothing");
        assert_eq!(entries[0].id, "vpc/main");
        assert_eq!(entries[0].op, ChangeOp::Created);
        assert_eq!(entries[1].op, ChangeOp::Updated);
        assert_eq!(
            entries[2].op,
            ChangeOp::Updated,
            "Replace records as Updated — identities, not mechanics"
        );
        assert_eq!(entries[3].op, ChangeOp::Deleted);
    }
}
