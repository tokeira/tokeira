//! `tkp plan` — surface the binding verdict + the infrastructure plan (read-only;
//! annotates but never gates/refuses).

use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tokeira_deployment::{ProvenanceStamp, check_binding};
use tokeira_report::Mode;

use tokeira_platform::definition::DefinitionFrontend;

use crate::{engine::Engine, envelope_store, platform::Admitted, render::ExplanationReport};

pub(crate) async fn plan<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    module: Option<&str>,
    mode: Mode,
    explanation_path: Option<&Path>,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _) = envelope_store(&admitted.deployment_ref.dir)
        .load()
        .await
        .context("failed to load the deployment envelope")?;

    let verdict = check_binding(envelope.binding.as_ref(), &running);
    // Causality's S is gathered before the plan runs its refresh
    // — see the causality module's isolation rule.
    let gathered = crate::causality::gather_causality(engine, admitted, &envelope).await?;
    let outcome = engine.plan(admitted, module).await?;
    let mut explanation = tokeira_explain::explain_plan(
        crate::explain_context(engine, admitted, &envelope, "infra plan"),
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
    // requested artifact fails before claiming anything. The error
    // already names the path and the reason.
    if let Some(path) = explanation_path {
        tokeira_explain::artifact::write(path, &report.explanation)?;
    }
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    // The verb exits non-zero on a platform issue (output-templates §The
    // platform cannot be reached); the document above is the whole report.
    if report.explanation.platform_issues.is_empty() {
        Ok(())
    } else {
        Err(crate::ReportEmitted.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::testkit;

    // The requested artifact is the complete model — the same
    // schema `--json` emits — parseable from the file alone.
    #[tokio::test]
    async fn plan_writes_the_explanation_artifact() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("explanation.json");
        let (engine, admitted) = testkit::engine(tmp.path());
        plan(
            &engine,
            &admitted,
            None,
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

    // The platform-issue refusal: the verb exits non-zero through the typed
    // error `cli::run` maps to a bare exit code, and the artifact still
    // carries the issue for machines (output-templates §The platform cannot
    // be reached).
    #[tokio::test]
    async fn a_platform_issue_refuses_the_verb_and_the_artifact_carries_it() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("explanation.json");
        let (engine, admitted) = testkit::engine_over(
            tmp.path(),
            testkit::FixedProbe(Some(testkit::unreachable_issue())),
            testkit::StubFrontend::new(),
        );
        let err = plan(
            &engine,
            &admitted,
            None,
            Mode::resolve(false, false),
            Some(&path),
        )
        .await
        .expect_err("a platform issue exits the verb non-zero");
        assert!(
            err.downcast_ref::<crate::ReportEmitted>().is_some(),
            "the typed refusal, not a prose error: {err:#}"
        );

        let model = tokeira_explain::artifact::read(&path).expect("artifact parses alone");
        assert_eq!(model.platform_issues.len(), 1);
        assert_eq!(
            model.platform_issues[0].fact,
            "Unable to reach the test substrate"
        );
        assert!(model.changes.is_empty(), "no record-based plan");
    }

    // An unwritable artifact path fails the verb, naming the path
    // and the reason.
    #[tokio::test]
    async fn plan_fails_when_the_artifact_cannot_be_written() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("no-such-dir").join("explanation.json");
        let (engine, admitted) = testkit::engine(tmp.path());
        let err = plan(
            &engine,
            &admitted,
            None,
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
