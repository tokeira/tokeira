//! `tokeira-tkp` — the platform-agnostic `tkp` shell.
//!
//! Every platform's provisioner binary (each constructed as `tkp`) is this
//! library over one bound engine: a [`PlatformDeclaration`] bound to its
//! built-as identity pair ([`BoundPlatform`]) and married to a definition
//! frontend ([`Engine`]). The shell owns the lifecycle verbs, the
//! binding-gate orchestration, the operation-lock wrapper, the deployment
//! state envelope after creation, `describe`, and the config-revision
//! history; `tkr deployment create` owns the initial binding and revision.
//! The declaration supplies what varies — the kinds, the ops
//! surface, the reachability probe, and the registration constructors. The
//! shell is a distinct layer over `tokeira-deployment` (the domain
//! library — stamps, binding, integrity), never folded into it.

// CLI shell: stdout/stderr are the operator interface.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::Path;

use anyhow::{Context, Result};
use tokeira_deployment::DeploymentStateEnvelope;
use tokeira_iac::{Change, ChangeKind};

#[cfg(test)]
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

mod apply;
mod causality;
mod cli;

mod config_seed;
mod definition;
mod deploy;
mod describe;
mod described;
mod destroy;
pub mod engine;
mod gate;
mod image;
mod observability;

mod plan;
pub mod platform;
mod publication;
mod render;
mod revert;
mod rollback;
mod scale;
#[cfg(test)]
pub(crate) mod testkit;
mod upgrade;

pub use cli::run;
pub use engine::Engine;
pub use platform::{Admitted, BoundPlatform};
// The seam's audit vocabulary travels with the seam: platform realizations
// return these from their applying verbs.
pub use tokeira_deployment::{ChangeLogEntry, ChangeOp, ConfigSource};

/// Describe the running binary when no placed bundle is available, notably a
/// native candidate performing an upgrade. Ordinary creation records the
/// independently verified bundle manifest and never needs a provisioner
/// inception command.
pub(crate) fn running_integrity_manifest() -> anyhow::Result<tokeira_deployment::IntegrityManifest>
{
    let executable =
        std::env::current_exe().context("failed to locate the running provisioner binary")?;
    let bytes = std::fs::read(&executable)
        .with_context(|| format!("failed to read {}", executable.display()))?;
    Ok(tokeira_deployment::IntegrityManifest {
        engine_identity: None,
        authority: tokeira_deployment::BuildAuthority::LocalDeveloper,
        provisioner_version: tokeira_build_info::TOKEIRA_VERSION.to_string(),
        artifacts: vec![tokeira_deployment::BinaryArtifactDescriptor {
            target: tokeira_deployment::Target(env!("TKP_TARGET").to_string()),
            sha256: tokeira_deployment::sha256_hex(&bytes),
            retrieval_ref: None,
            size_bytes: bytes.len() as u64,
        }],
    })
}
pub(crate) use tokeira_deployment::{config_history, lock, marker};

/// Generate the disposable `tkp` entrypoint for one statically selected
/// platform declaration and one definition frontend.
#[macro_export]
macro_rules! bound_provisioner_main {
    (
        expected_platform: $platform:literal,
        platform: $platform_factory:path,
        expected_format: $format:literal,
        content_roots: [$($content_root:literal),* $(,)?],
        frontend: $frontend:path $(,)?
    ) => {
        fn main() -> std::process::ExitCode {
            $crate::run_bound_provisioner(
                $platform,
                $format,
                &[$($content_root),*],
                $platform_factory(),
                $frontend(),
            )
        }
    };
}

/// Synchronous process boundary used only by generated composition roots:
/// bind the declaration to the built-as identity pair (a kind-name
/// collision refuses the binary here, before any deployment is read),
/// marry the frontend, run the shell.
pub fn run_bound_provisioner<F>(
    expected_platform: &'static str,
    expected_format: &'static str,
    content_roots: &'static [&'static str],
    declaration: PlatformDeclaration,
    frontend: F,
) -> std::process::ExitCode
where
    F: tokeira_platform::definition::DefinitionFrontend,
{
    let engine = crate::platform::BoundPlatform::bind_with_content(
        expected_platform,
        expected_format,
        content_roots,
        declaration,
    )
    .and_then(|platform| crate::engine::Engine::new(platform, frontend));
    let engine = match engine {
        Ok(engine) => engine,
        Err(error) => {
            eprintln!("{error:#}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to start provisioner runtime: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(engine)) {
        Ok(code) => code,
        Err(error) => {
            eprintln!("{error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Canonical per-resource desired manifests realized from one definition
/// source, in memory: the comparison value revision-level causality is built
/// on. Keys are the engine's own resource identities, so a snapshot joins the
/// plan's changes without translation.
pub(crate) type DesiredSnapshot =
    std::collections::BTreeMap<tokeira_iac::ResourceId, serde_json::Value>;

/// The platform declaration surface: what a platform's entry point returns
/// and everything it carries. The framework defines what it consumes; the
/// types live with the definition-boundary library and are re-exported here
/// as the surface platforms speak.
pub use tokeira_platform::declaration::{
    DeclaredImage, DeploymentRef, ImageOperations, Ops, PlatformDeclaration, PlatformExecution,
    PlatformIntegration, PublishedImage,
};
pub use tokeira_platform::{definition::Namespace, kind};

/// The typed failure a verb returns after emitting its complete report.
///
/// [`cli::run`] turns this into a bare non-zero exit so the process boundary
/// cannot restate a platform issue or service failure that Markdown or JSON
/// already carried. The message exists for any other caller.
#[derive(Debug)]
pub(crate) struct ReportEmitted;

impl std::fmt::Display for ReportEmitted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the operation failed; the emitted report carries the failure")
    }
}

impl std::error::Error for ReportEmitted {}

/// What an apply committed, under the identity the engine executed it with.
///
/// The persisted audit vocabulary stays ids-only —
/// `change_log_entries` distills `changes` wherever an audit record is
/// needed. The full engine identity (module, resource type, a `Replace`
/// kind the audit log folds away) and the operator nouns exist here because
/// the applied report states what committed in the operator's language, and
/// reading them back from anywhere else would be re-derivation or invention.
#[derive(Debug, Clone, Default)]
pub struct AppliedOutcome {
    pub(crate) changes: Vec<tokeira_iac::Change>,
    pub(crate) display_by_id: std::collections::BTreeMap<tokeira_iac::ResourceId, String>,
    /// Declared writeback resolved against the applied state — the pairs
    /// the shell persists into the server configuration document.
    pub(crate) writeback: Vec<(String, String)>,
}

/// The explanation's deployment context, assembled from what the envelope
/// records and the platform knows. Read-only
/// verbs pass no proposed revision — the plan proposes nothing by itself.
pub(crate) fn explain_context<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &crate::engine::Engine<F>,
    _admitted: &crate::platform::Admitted,
    envelope: &DeploymentStateEnvelope,
    operation: &str,
) -> tokeira_explain::DeploymentContext {
    tokeira_explain::DeploymentContext {
        deployment: crate::apply::deployment_identity(&envelope.deployment_id),
        platform: engine.platform().id().to_string(),
        operation: operation.to_string(),
        current_revision: envelope.config_revision,
        proposed_revision: None,
        definition_ref: envelope.effective_config_ref.clone(),
    }
}

/// Map an applied engine Delta to the ids-only audit entries.
///
/// The id is the engine's **`ResourceId`** (`Change::resource`) — the exact
/// address `destroy_selected` keys on, so the audit entries double as the
/// rollback delete-set's feed; the module is grouping metadata
/// the ids-only log deliberately drops. Kinds map onto the audit vocabulary:
/// `Replace` records as `Updated` (the id survives a delete-then-recreate;
/// the audit log tracks identities, not mechanics), and `NoChange` records
/// nothing.
pub(crate) fn change_log_entries(changes: &[Change]) -> Vec<ChangeLogEntry> {
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
                id: change.resource.clone(),
                op,
            })
        })
        .collect()
}

/// Emit a rendered report: Markdown narrative is skinned for a terminal via
/// termimad and emitted raw everywhere else — a pipe, a redirect, or an
/// agent receives the deterministic Markdown itself. `--json` output is never skinned.
pub(crate) fn emit_report(text: &str, mode: tokeira_report::Mode) {
    use std::io::IsTerminal;
    if matches!(mode.form, tokeira_report::Form::Narrative) && std::io::stdout().is_terminal() {
        termimad::print_text(text);
    } else {
        print!("{text}");
    }
}

/// Map an apply's committed changes into the explanation's vocabulary at the
/// shell boundary: the model crate must not depend on
/// the engine types, so identity crosses here — ids, operations,
/// the module/type halves of the natural key, and the operator noun. Never
/// before-images. `NoChange` rows cross as the unchanged
/// census — they render nothing, but they carry the noun ambiguity the
/// renderer needs.
pub(crate) fn committed_changes(applied: &AppliedOutcome) -> Vec<tokeira_explain::CommittedChange> {
    applied
        .changes
        .iter()
        .map(|change| {
            let op = match change.kind {
                tokeira_iac::ChangeKind::Create => tokeira_explain::CommittedOp::Created,
                tokeira_iac::ChangeKind::Update => tokeira_explain::CommittedOp::Updated,
                tokeira_iac::ChangeKind::Replace => tokeira_explain::CommittedOp::Replaced,
                tokeira_iac::ChangeKind::Delete => tokeira_explain::CommittedOp::Deleted,
                tokeira_iac::ChangeKind::NoChange => tokeira_explain::CommittedOp::Unchanged,
            };
            tokeira_explain::CommittedChange {
                id: change.resource.clone(),
                op,
                module: change.module.clone(),
                resource_type: change.resource_type.clone(),
                display: applied
                    .display_by_id
                    .get(&tokeira_iac::ResourceId(change.resource.clone()))
                    .cloned(),
            }
        })
        .collect()
}

/// The retarget gate, run by every config-applying verb after the binding
/// gate and before any mutation: a `#[create]` change is a new deployment,
/// not an apply. No retained prior revision means nothing has ever applied,
/// so there is nothing to gate — deployment creation retains revision 0 from
/// the operator's already-edited definition, which is exactly why choosing a
/// `#[create]` value *before* the first apply is legitimate and changing it
/// *after* is refused.
pub(crate) async fn retarget_gate<F: tokeira_platform::definition::DefinitionFrontend>(
    engine: &crate::engine::Engine<F>,
    admitted: &crate::platform::Admitted,
    envelope: &DeploymentStateEnvelope,
) -> Result<()> {
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    let config_source = admitted.config_source();
    let Some(prior) =
        config_history::retained_source(deployment_dir, &config_source, envelope.config_revision)?
    else {
        return Ok(());
    };
    let live_path = config_history::config_file(deployment_dir, &config_source);
    let current = std::fs::read_to_string(&live_path).with_context(|| {
        format!(
            "failed to read the live definition at {}",
            live_path.display()
        )
    })?;
    // Each side resolves its definition parts against its own set: the
    // retained revision folder for the prior, the live directory for the
    // current. A format-less source has no part convention on either side.
    match &config_source.format {
        Some(format) => {
            let prior_parts = tokeira_platform::definition::DirectoryPartSources::new(
                config_history::retained_parts_dir(
                    deployment_dir,
                    &config_source,
                    envelope.config_revision,
                ),
                format.as_str(),
            );
            let current_parts = tokeira_platform::definition::DirectoryPartSources::new(
                live_path
                    .parent()
                    .expect("a live definition path has a parent"),
                format.as_str(),
            );
            // The engine's refusal IS the error, already naming every changed field.
            engine.retarget_check(admitted, &prior, &current, &prior_parts, &current_parts)
        }
        None => engine.retarget_check(
            admitted,
            &prior,
            &current,
            &tokeira_platform::definition::NoPartSources,
            &tokeira_platform::definition::NoPartSources,
        ),
    }
}

/// Persist an apply's resolved writeback into the deployment's server
/// configuration document (`tokeirad.toml` — the definitive story). Called
/// by every verb that re-applies infrastructure, after the apply commits
/// and before the envelope re-stamp, so the retained revision snapshots
/// the post-writeback document. Empty writeback writes nothing.
pub(crate) fn persist_writeback(deployment_dir: &Path, values: &[(String, String)]) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let path = deployment_dir.join(config_history::SERVER_CONFIG);
    let borrowed = values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect::<Vec<_>>();
    tokeira_iac::write_config_values(&path, &borrowed)
        .context("failed to persist writeback into the server configuration")?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn envelope_store(
    deployment_dir: &Path,
) -> Box<dyn DeploymentStore<DeploymentStateEnvelope>> {
    Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/envelope"))),
        "envelope".to_string(),
    ))
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

    // Task 19.2/19.3: the audit mapping — ids are the engine's ResourceId
    // (the destroy_selected key), Replace folds to Updated (the id survives
    // the delete-then-recreate), NoChange vanishes.
    #[test]
    fn change_log_entries_map_ids_and_ops() {
        let entries = change_log_entries(&[
            change(ChangeKind::Create, "vpc", "main-vpc"),
            change(ChangeKind::Update, "svc", "web"),
            change(ChangeKind::Replace, "db", "primary"),
            change(ChangeKind::Delete, "svc", "old"),
            change(ChangeKind::NoChange, "svc", "steady"),
        ]);
        assert_eq!(entries.len(), 4, "NoChange records nothing");
        assert_eq!(
            entries[0].id, "main-vpc",
            "the id IS the engine ResourceId — the delete-set key"
        );
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
