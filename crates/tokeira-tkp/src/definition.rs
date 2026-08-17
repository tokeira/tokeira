//! `tkp definition check` — parse + interpret the deployment definition in
//! memory. Read-only: no provider calls, no state writes, never gates — the
//! definition analog of `cargo check`. A definition that does not verify is a
//! *result*, rendered as a report (and a non-zero exit for CI), never an
//! anyhow crash.

use std::path::Path;

use anyhow::Result;
use tokeira_orchestrator::DefinitionFormatId;
use tokeira_report::{Depth, Mode, Report};

use tokeira_platform::definition::DefinitionFrontend;

use crate::{engine::Engine, platform::Admitted};

/// One definition, one verdict, the located error when it does not verify.
/// `deployment` is the check's context — present when checking a deployment's
/// own definition, absent in authoring mode (`--definition <path>`), where the
/// path itself is the subject.
#[derive(Debug, serde::Serialize)]
struct CheckReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    deployment: Option<String>,
    /// The checked definition: the deployment's basename, or the authored path.
    definition: String,
    /// The deployment's current config revision — the anchor the checked
    /// definition would advance from. Absent in authoring mode, and absent
    /// when the envelope cannot be read (the check never gates on it).
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
    verifies: bool,
    /// The interpreter's located verdict ("parse error at line 112, …").
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Value column for this report's fact lines (`definition`, 10, plus two).
const FACT_LABEL_WIDTH: usize = 12;

fn push_fact(out: &mut String, label: &str, value: &str) {
    let pad = " ".repeat(FACT_LABEL_WIDTH.saturating_sub(label.len()));
    out.push_str(&format!("- **{label}**{pad}{value}\n"));
}

impl Report for CheckReport {
    fn narrative(&self, _depth: Depth, out: &mut String) {
        // Deployment mode is headed by the deployment; authoring mode has no
        // deployment context, so the path-bearing fact line is the whole
        // report.
        if let Some(deployment) = &self.deployment {
            out.push_str(&format!("# {deployment}\n"));
        }
        match &self.error {
            None => push_fact(
                out,
                "definition",
                &format!("`{}` — verifies", self.definition),
            ),
            Some(error) => {
                // Other verbs inherit the full sentence through their anyhow
                // chains; this report already names the subject, so strip the
                // duplicated lead-in when present.
                let error = error
                    .strip_prefix("the definition does not verify: ")
                    .unwrap_or(error);
                push_fact(
                    out,
                    "definition",
                    &format!("`{}` — does not verify: {error}", self.definition),
                );
            }
        }
        if let Some(revision) = self.revision {
            push_fact(out, "revision", &revision.to_string());
        }
    }
}

pub(crate) async fn check<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: Option<&Admitted>,
    source: Option<&Path>,
    requested_format: Option<&DefinitionFormatId>,
    mode: Mode,
) -> Result<()> {
    if source.is_some() && requested_format.is_none() {
        anyhow::bail!("standalone definition checking requires `--format <id>`");
    }
    if let Some(requested) = requested_format {
        let supported = engine.platform().format();
        if requested != supported {
            anyhow::bail!(
                "definition format `{requested}` does not match this provisioner's `{supported}` frontend"
            );
        }
    }
    // A failed check IS the report: catch the located error rather than
    // crashing through anyhow (which would double-print and break the
    // `--json`-stdout-purity rule). Deployment mode runs the whole pure
    // pipeline (evaluate, realize, resource validation); authoring mode has
    // no placement facts, so it verifies the frontend structure alone.
    let checked: Result<()> = match source {
        Some(path) => engine.evaluate_authoring(path).map(|_| ()),
        None => {
            let admitted = admitted.expect("deployment-mode check admits its deployment");
            engine.execution(admitted, None).map(|_| ())
        }
    };
    let error = checked.err().map(|e| format!("{e:#}"));
    // Authoring mode (`source`) names the path and no deployment; deployment
    // mode names the deployment, its definition basename, and the revision
    // anchor (read tolerantly — the check's subject is the definition, so an
    // unreadable envelope drops the fact rather than failing the verb).
    let (deployment, definition, revision) = match (source, admitted) {
        (Some(path), _) => (None, path.display().to_string(), None),
        (None, Some(admitted)) => (
            Some(admitted.deployment_ref.name.clone()),
            admitted.config_source().path.as_str().to_string(),
            crate::envelope_store(&admitted.deployment_ref.dir)
                .load()
                .await
                .ok()
                .map(|(envelope, _)| envelope.config_revision),
        ),
        (None, None) => unreachable!("deployment-mode check admits its deployment"),
    };
    let report = CheckReport {
        deployment,
        definition,
        revision,
        verifies: error.is_none(),
        error,
    };
    crate::emit_report(&tokeira_report::render(&report, mode)?, mode);
    if !report.verifies {
        // The report has already said everything; the exit code is for CI and
        // scripts (`cargo check` discipline: findings are output + non-zero).
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // A bound platform always interprets: the deployment-mode check runs
    // the whole pure pipeline over the recorded definition and verifies.
    #[tokio::test]
    async fn a_bound_platform_checks_its_recorded_definition() {
        let tmp = tempfile::tempdir().unwrap();
        let (engine, admitted) = crate::testkit::engine(tmp.path());
        check(&engine, Some(&admitted), None, None, Mode::default())
            .await
            .expect("the stub world verifies");
    }

    // The verifying case renders under the output contract in both forms.
    #[test]
    fn a_verifying_definition_reports_in_both_forms() {
        let report = CheckReport {
            deployment: Some("compose-explore".to_string()),
            definition: "definition.tkd".to_string(),
            revision: Some(3),
            verifies: true,
            error: None,
        };
        let narrative = tokeira_report::render(&report, Mode::resolve(false, false)).unwrap();
        assert_eq!(
            narrative,
            "# compose-explore\n\
             - **definition**  `definition.tkd` — verifies\n\
             - **revision**    3\n"
        );
        let json = tokeira_report::render(&report, Mode::resolve(true, false)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["deployment"], "compose-explore");
        assert_eq!(value["revision"], 3);
        assert_eq!(value["verifies"], true);
        assert!(value.get("error").is_none(), "no error key when clean");
    }

    // A broken definition is a report, not a crash: the located verdict is the
    // payload.
    #[test]
    fn a_broken_definition_reports_the_located_error() {
        let report = CheckReport {
            // Authoring mode: no deployment context, the path is the subject.
            deployment: None,
            definition: "./defs/staging.tkd".to_string(),
            revision: None,
            verifies: false,
            error: Some(
                "the definition does not verify: parse error at line 112, column 18: expected `,`"
                    .to_string(),
            ),
        };
        let narrative = tokeira_report::render(&report, Mode::resolve(false, false)).unwrap();
        assert!(
            narrative.contains(
                "- **definition**  `./defs/staging.tkd` — does not verify: parse error at line 112"
            ),
            "subject stated once, location survives: {narrative}"
        );
        assert!(
            !narrative.starts_with("#") && !narrative.contains("revision"),
            "authoring mode carries no deployment context: {narrative}"
        );
        let json = tokeira_report::render(&report, Mode::resolve(true, false)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["verifies"], false);
        assert!(value["error"].as_str().unwrap().contains("line 112"));
    }
}
