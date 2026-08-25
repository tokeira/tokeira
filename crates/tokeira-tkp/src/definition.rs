//! `tkp definition check` — parse + interpret the deployment definition in
//! memory. Read-only: no provider calls, no state writes, never gates — the
//! definition analog of `cargo check`. A definition that does not verify is a
//! *result*, rendered as a report (and a non-zero exit for CI), never an
//! anyhow crash.

use std::path::Path;

use anyhow::Result;
use chrono::Utc;
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
    /// Format-plus-source configuration identity of the checked definition —
    /// what evaluation actually covered. Absent when the check failed before
    /// an identity existed.
    #[serde(skip_serializing_if = "Option::is_none")]
    identity: Option<tokeira_platform::definition::ConfigurationIdentity>,
    /// Served companion names in first-request order; empty for a
    /// single-document definition, absent when the check failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    companions: Option<Vec<String>>,
    /// The exact running-engine stamp creation records after this read-only
    /// admission succeeds. Absent in standalone authoring mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    provenance: Option<tokeira_deployment::ProvenanceStamp>,
    /// The admitted source set creation retains as its initial revision.
    /// Absent in standalone authoring mode.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<tokeira_deployment::ConfigSource>,
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
        if let Some(identity) = &self.identity {
            push_fact(
                out,
                "identity",
                &format!("{}:{}", identity.algorithm(), identity.digest),
            );
        }
        if let Some(companions) = self.companions.as_deref()
            && !companions.is_empty()
        {
            push_fact(out, "companions", &companions.join(", "));
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
    let checked: Result<(
        tokeira_platform::definition::ConfigurationIdentity,
        Vec<String>,
    )> = match source {
        Some(path) => engine.evaluate_authoring(path).map(|evaluated| {
            (
                evaluated.configuration_identity.clone(),
                evaluated
                    .served_companions
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect(),
            )
        }),
        None => {
            let admitted = admitted.expect("deployment-mode check admits its deployment");
            engine
                .execution(admitted, None)
                .map(|state| (state.configuration_identity, state.served_companions))
        }
    };
    let (evaluated_facts, error) = match checked {
        Ok(facts) => (Some(facts), None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };
    let (identity, companions) = match evaluated_facts {
        Some((identity, companions)) => (Some(identity), Some(companions)),
        None => (None, None),
    };
    // Authoring mode (`source`) names the path and no deployment; deployment
    // mode names the deployment, its definition basename, and the revision
    // anchor (read tolerantly — the check's subject is the definition, so an
    // unreadable envelope drops the fact rather than failing the verb).
    let (deployment, definition, revision, provenance, config_source) = match (source, admitted) {
        (Some(path), _) => (None, path.display().to_string(), None, None, None),
        (None, Some(admitted)) => {
            let config_source = admitted.config_source();
            (
                Some(admitted.deployment_ref.name.clone()),
                config_source.path.as_str().to_string(),
                crate::envelope_store(&admitted.deployment_ref.dir)
                    .load()
                    .await
                    .ok()
                    .map(|(envelope, _)| envelope.config_revision),
                Some(tokeira_deployment::ProvenanceStamp::current(Utc::now())),
                Some(config_source),
            )
        }
        (None, None) => unreachable!("deployment-mode check admits its deployment"),
    };
    let report = CheckReport {
        deployment,
        definition,
        revision,
        verifies: error.is_none(),
        identity,
        companions,
        provenance,
        source: config_source,
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

    // The identity + companions facts serialize under --json exactly as the
    // deployment repository's claim consumes them: identity as
    // {algorithm, digest}, companions as the served-order list; both absent
    // (not null) when the check failed before an identity existed.
    #[test]
    fn check_report_serializes_identity_and_companions() {
        let format = tokeira_orchestrator::DefinitionFormatId::new("tkd").unwrap();
        let identity = tokeira_platform::definition::ConfigurationIdentity::compute_set(
            &format,
            b"root",
            &[("platform".to_string(), std::sync::Arc::from(&b"p"[..]))],
        );
        let report = CheckReport {
            deployment: None,
            definition: "deployment.tkd".to_string(),
            revision: None,
            verifies: true,
            identity: Some(identity.clone()),
            companions: Some(vec!["platform".to_string()]),
            provenance: None,
            source: None,
            error: None,
        };
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["identity"]["algorithm"], "sha256-set-v1");
        assert_eq!(value["identity"]["digest"], identity.digest.as_str());
        assert_eq!(value["companions"], serde_json::json!(["platform"]));

        let failed = CheckReport {
            deployment: None,
            definition: "deployment.tkd".to_string(),
            revision: None,
            verifies: false,
            identity: None,
            companions: None,
            provenance: None,
            source: None,
            error: Some("parse error".to_string()),
        };
        let value = serde_json::to_value(&failed).unwrap();
        assert!(value.get("identity").is_none(), "absent, not null");
        assert!(value.get("companions").is_none(), "absent, not null");
    }

    // A single-document definition reports sha256-v1 with an EMPTY (present)
    // companion list — distinguishing "evaluated, no companions" from
    // "never evaluated".
    #[test]
    fn single_document_check_reports_empty_companions() {
        let format = tokeira_orchestrator::DefinitionFormatId::new("tkd").unwrap();
        let identity =
            tokeira_platform::definition::ConfigurationIdentity::compute(&format, b"root");
        let report = CheckReport {
            deployment: None,
            definition: "deployment.tkd".to_string(),
            revision: None,
            verifies: true,
            identity: Some(identity),
            companions: Some(Vec::new()),
            provenance: None,
            source: None,
            error: None,
        };
        let value = serde_json::to_value(&report).unwrap();
        assert_eq!(value["identity"]["algorithm"], "sha256-v1");
        assert_eq!(value["companions"], serde_json::json!([]));
    }

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
            identity: None,
            companions: None,
            provenance: None,
            source: None,
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
            identity: None,
            companions: None,
            provenance: None,
            source: None,
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
