//! `tkp definition check` — parse + interpret the deployment definition in
//! memory. Read-only: no provider calls, no state writes, never gates — the
//! definition analog of `cargo check`. A definition that does not verify is a
//! *result*, rendered as a report (and a non-zero exit for CI), never an
//! anyhow crash.

use std::path::Path;

use anyhow::Result;
use tokeira_orchestrator::DefinitionFormatId;
use tokeira_report::{Depth, Mode, Report};

use crate::{ProvisionerPlatform, Realization};

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
    verifies: bool,
    /// The interpreter's located verdict ("parse error at line 112, …").
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl Report for CheckReport {
    fn narrative(&self, _depth: Depth, out: &mut String) {
        if let Some(deployment) = &self.deployment {
            out.push_str(&format!("deployment: {deployment}\n"));
        }
        match &self.error {
            None => out.push_str(&format!("definition: {} verifies\n", self.definition)),
            Some(error) => {
                // Other verbs inherit the full sentence through their anyhow
                // chains; this report already names the subject, so strip the
                // duplicated lead-in when present.
                let error = error
                    .strip_prefix("the definition does not verify: ")
                    .unwrap_or(error);
                out.push_str(&format!(
                    "definition: {} does not verify — {error}\n",
                    self.definition
                ));
            }
        }
    }
}

pub(crate) async fn check<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    source: Option<&Path>,
    requested_format: Option<&DefinitionFormatId>,
    mode: Mode,
) -> Result<()> {
    if source.is_some() && requested_format.is_none() {
        anyhow::bail!("standalone definition checking requires `--format <id>`");
    }
    if let Some(requested) = requested_format {
        let supported = platform.definition_format().ok_or_else(|| {
            anyhow::anyhow!("this provisioner has no interpreted definition frontend")
        })?;
        if requested.as_str() != supported {
            anyhow::bail!(
                "definition format `{requested}` does not match this provisioner's `{supported}` frontend"
            );
        }
    }
    // A failed check IS the report: catch the platform's located error rather
    // than crashing through anyhow (which would double-print and break the
    // `--json`-stdout-purity rule).
    let error = match platform.definition_check(deployment_dir, source).await {
        Ok(Realization::Realized(())) => None,
        Ok(Realization::NotApplicable { reason }) => {
            anyhow::bail!("not applicable: {reason}");
        }
        Err(e) => Some(format!("{e:#}")),
    };
    // Authoring mode (`source`) names the path and no deployment; deployment
    // mode names the deployment and its definition basename.
    let (deployment, definition) = match source {
        Some(path) => (None, path.display().to_string()),
        None => (
            platform.deployment_id(deployment_dir).ok(),
            platform
                .config_source(deployment_dir)?
                .path
                .as_str()
                .to_string(),
        ),
    };
    let report = CheckReport {
        deployment,
        definition,
        verifies: error.is_none(),
        error,
    };
    print!("{}", tokeira_report::render(&report, mode)?);
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
    use crate::TestPlatform;

    // The default platform (no interpreted definition) refuses in the typed
    // NotApplicable shape, matching every other conditionally-realized verb.
    #[tokio::test]
    async fn platforms_without_a_definition_refuse_as_not_applicable() {
        let tmp = tempfile::tempdir().unwrap();
        let err = check(&TestPlatform, tmp.path(), None, None, Mode::default())
            .await
            .expect_err("no interpreted definition");
        assert!(
            err.to_string().contains("not applicable"),
            "unexpected: {err}"
        );
    }

    // The verifying case renders under the output contract in both forms.
    #[test]
    fn a_verifying_definition_reports_in_both_forms() {
        let report = CheckReport {
            deployment: Some("compose-explore".to_string()),
            definition: "definition.tkd".to_string(),
            verifies: true,
            error: None,
        };
        let narrative = tokeira_report::render(&report, Mode::resolve(false, false)).unwrap();
        assert_eq!(
            narrative,
            "deployment: compose-explore\ndefinition: definition.tkd verifies\n"
        );
        let json = tokeira_report::render(&report, Mode::resolve(true, false)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["deployment"], "compose-explore");
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
            verifies: false,
            error: Some(
                "the definition does not verify: parse error at line 112, column 18: expected `,`"
                    .to_string(),
            ),
        };
        let narrative = tokeira_report::render(&report, Mode::resolve(false, false)).unwrap();
        assert!(
            narrative.contains(
                "definition: ./defs/staging.tkd does not verify — parse error at line 112"
            ),
            "subject stated once, location survives: {narrative}"
        );
        assert!(
            !narrative.contains("deployment:"),
            "authoring mode carries no deployment context: {narrative}"
        );
        let json = tokeira_report::render(&report, Mode::resolve(true, false)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["verifies"], false);
        assert!(value["error"].as_str().unwrap().contains("line 112"));
    }
}
