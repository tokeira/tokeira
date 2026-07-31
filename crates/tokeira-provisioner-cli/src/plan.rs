//! `tkp plan` — surface the binding verdict + the infrastructure plan (read-only;
//! annotates but never gates/refuses, Req 2.5).

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_provisioner::{ProvenanceStamp, check_binding};
use tokeira_report::Mode;

use crate::{ProvisionerPlatform, envelope_store, render::ExplanationReport};

pub(crate) async fn plan<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    mode: Mode,
    explanation_path: Option<&Path>,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(deployment_dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    let verdict = check_binding(envelope.binding.as_ref(), &running);
    // Causality's S is gathered before the plan runs its refresh
    // (Requirement 2.3) — see the causality module's isolation rule.
    let gathered = crate::causality::gather_causality(platform, deployment_dir, &envelope).await?;
    let outcome = platform.infra_plan(deployment_dir).await?;
    let mut explanation = tokeira_explain::explain_plan(
        crate::explain_context(platform, deployment_dir, &envelope, "infra plan"),
        &outcome,
    );
    tokeira_explain::apply_causality(
        &mut explanation,
        &crate::causality::causality_view(gathered, &outcome),
    );
    let report = ExplanationReport {
        initialized: envelope.binding.is_some(),
        binding: verdict,
        explanation,
    };
    // The artifact precedes the report: a verb that cannot deliver the
    // requested artifact fails before claiming anything (Req 7.6). The error
    // already names the path and the reason.
    if let Some(path) = explanation_path {
        tokeira_explain::artifact::write(path, &report.explanation)?;
    }
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TestPlatform;

    // Req 7.1/7.2: the requested artifact is the complete model — the same
    // schema `--json` emits — parseable from the file alone.
    #[tokio::test]
    async fn plan_writes_the_explanation_artifact() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("explanation.json");
        plan(
            &TestPlatform,
            tmp.path(),
            Mode::resolve(false, false),
            Some(&path),
        )
        .await
        .expect("plan with a writable artifact path succeeds");

        let model = tokeira_explain::artifact::read(&path).expect("artifact parses alone");
        assert_eq!(
            model.schema_version,
            tokeira_explain::EXPLANATION_SCHEMA_VERSION
        );
        assert_eq!(model.operation, "infra plan");
    }

    // Req 7.6: an unwritable artifact path fails the verb, naming the path
    // and the reason.
    #[tokio::test]
    async fn plan_fails_when_the_artifact_cannot_be_written() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("no-such-dir").join("explanation.json");
        let err = plan(
            &TestPlatform,
            tmp.path(),
            Mode::resolve(false, false),
            Some(&path),
        )
        .await
        .expect_err("an unwritable artifact path fails the verb");
        let message = format!("{err:#}");
        assert!(message.contains("no-such-dir"), "path named: {message}");
        assert!(
            message.contains("explanation artifact"),
            "reason named: {message}"
        );
    }
}
