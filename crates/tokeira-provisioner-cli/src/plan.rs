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
    module: Option<&str>,
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
    let outcome = platform.infra_plan(deployment_dir, module).await?;
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
    // The verb exits non-zero on a platform issue (output-templates §The
    // platform cannot be reached); the document above is the whole report.
    if report.explanation.platform_issues.is_empty() {
        Ok(())
    } else {
        Err(crate::PlatformBlocked.into())
    }
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

    /// The no-op platform with an unreachable world: `infra_plan` answers
    /// the typed issue and nothing else, exactly as a platform refusing at
    /// its describe boundary does. The fixture evidence is a class the
    /// direction table establishes nothing from, so `direction: None` is a
    /// world the real table produces.
    struct BlockedPlatform;

    impl crate::ProvisionerPlatform for BlockedPlatform {
        fn label(&self, _deployment_dir: &std::path::Path) -> &'static str {
            "test"
        }

        fn config_source(
            &self,
            _deployment_dir: &std::path::Path,
        ) -> anyhow::Result<crate::ConfigSource> {
            crate::ConfigSource::legacy("deployment.toml")
        }

        fn deployment_id(&self, _deployment_dir: &std::path::Path) -> anyhow::Result<String> {
            Ok("tokeira".to_string())
        }

        async fn infra_plan(
            &self,
            _deployment_dir: &std::path::Path,
            _module: Option<&str>,
        ) -> anyhow::Result<tokeira_iac::PlanOutcome> {
            Ok(tokeira_iac::PlanOutcome {
                platform_issues: vec![tokeira_iac::PlatformIssue {
                    component: "Docker".to_string(),
                    fact: "Unable to connect to Docker".to_string(),
                    evidence: "error trying to connect: operation timed out".to_string(),
                    direction: None,
                }],
                ..Default::default()
            })
        }

        async fn infra_apply(
            &self,
            _d: &std::path::Path,
            _module: Option<&str>,
        ) -> anyhow::Result<crate::AppliedOutcome> {
            unreachable!("the blocked platform never applies")
        }

        async fn infra_destroy(
            &self,
            _d: &std::path::Path,
            _module: Option<&str>,
        ) -> anyhow::Result<usize> {
            unreachable!()
        }

        async fn infra_destroy_selected(
            &self,
            _d: &std::path::Path,
            _ids: &[String],
        ) -> anyhow::Result<Vec<tokeira_provisioner::ChangeLogEntry>> {
            unreachable!()
        }

        async fn deploy_plan(
            &self,
            _d: &std::path::Path,
        ) -> anyhow::Result<crate::Realization<tokeira_iac::PlanOutcome>> {
            unreachable!()
        }

        async fn deploy_apply(
            &self,
            _d: &std::path::Path,
        ) -> anyhow::Result<crate::Realization<crate::AppliedOutcome>> {
            unreachable!()
        }

        async fn scale(
            &self,
            _d: &std::path::Path,
            _specs: &[String],
        ) -> anyhow::Result<crate::Realization<usize>> {
            unreachable!()
        }
    }

    // The platform-issue refusal: the verb exits non-zero through the typed
    // error `cli::run` maps to a bare exit code, and the artifact still
    // carries the issue for machines (output-templates §The platform cannot
    // be reached).
    #[tokio::test]
    async fn a_platform_issue_refuses_the_verb_and_the_artifact_carries_it() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("explanation.json");
        let err = plan(
            &BlockedPlatform,
            tmp.path(),
            None,
            Mode::resolve(false, false),
            Some(&path),
        )
        .await
        .expect_err("a platform issue exits the verb non-zero");
        assert!(
            err.downcast_ref::<crate::PlatformBlocked>().is_some(),
            "the typed refusal, not a prose error: {err:#}"
        );

        let model = tokeira_explain::artifact::read(&path).expect("artifact parses alone");
        assert_eq!(model.platform_issues.len(), 1);
        assert_eq!(model.platform_issues[0].fact, "Unable to connect to Docker");
        assert!(model.changes.is_empty(), "no record-based plan");
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
