//! The standalone `definition check` syntax tier: admit a definition source
//! through its frontend library, in process, instantly — no deployment, no
//! platform, no provisioner build.
//!
//! The check surface is two-tier by design. A bare `--definition <path>`
//! answers the authoring question — does this source parse, stay inside the
//! frontend's admitted subset, and hold together with its companion parts?
//! Everything a platform contributes — its kind vocabulary, typed config,
//! injected context — is deliberately out of scope here: those questions are
//! only answerable against a deployment's platform, through
//! `tkr definition check --deployment <name>`. Every verdict names its tier
//! so the operator never mistakes a syntax pass for an engine pass.
//!
//! The tier runs against the frontend registry published by
//! `tokeira-platform-definition`, not workspace discovery: the check must work
//! from any directory and cost nothing. The CLI reads the named root and
//! renders the format-neutral verdict; frontend selection and companion-source
//! composition stay behind the library boundary.

use anyhow::{Context, Result, bail};
use std::path::Path;
use tokeira_platform_definition::SyntaxCheck;

/// Check the definition at `path` through its frontend's syntax tier.
pub(crate) fn check_at_path(
    path: &Path,
    format: Option<&tokeira_orchestrator::DefinitionFormatId>,
    json: bool,
) -> Result<()> {
    if path.is_dir() {
        bail!(
            "`{}` is a directory — name the definition's root source file\n\n\
             The syntax tier reads one source (companion parts resolve beside \
             it):\n\n\
             ```\n\
             tkr definition check --definition {}/deployment.tkd\n\
             ```",
            path.display(),
            path.display(),
        );
    }
    if !path.exists() {
        bail!("no definition found at {}", path.display());
    }
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let check = tokeira_platform_definition::check_syntax(path, &source, format)?;
    render_verdict(path, &check, json);
    if check.findings.is_empty() {
        Ok(())
    } else {
        // The findings on stdout are the answer; the exit code carries the
        // verdict without a second stderr complaint restating it.
        std::process::exit(1);
    }
}

fn render_verdict(path: &Path, check: &SyntaxCheck, json: bool) {
    if json {
        let report = serde_json::json!({
            "tier": "syntax",
            "format": check.format.as_str(),
            "root": path.display().to_string(),
            "admitted": check.findings.is_empty(),
            "parts": check.parts,
            "findings": check.findings
                .iter()
                .map(|finding| {
                    serde_json::json!({
                        "file": finding.file,
                        "message": finding.message,
                    })
                })
                .collect::<Vec<_>>(),
        });
        println!("{report}");
        return;
    }
    let parts_note = match check.parts.len() {
        0 => String::new(),
        1 => " with 1 companion part".to_string(),
        n => format!(" with {n} companion parts"),
    };
    let text = if check.findings.is_empty() {
        format!(
            "**admitted:** `{}` — `.{}` syntax and subset verify{parts_note}\n\n\
             This is the frontend syntax tier. Engine interpretation — platform \
             vocabulary, typed config, context — happens against a deployment's \
             platform: `tkr definition check --deployment <name>`.\n",
            path.display(),
            check.source_extension.as_str(),
        )
    } else {
        let mut text = format!(
            "**not admitted:** `{}` — {} finding{}{parts_note}\n\n",
            path.display(),
            check.findings.len(),
            if check.findings.len() == 1 { "" } else { "s" },
        );
        for finding in &check.findings {
            text.push_str(&format!("- `{}` — {}\n", finding.file, finding.message));
        }
        text
    };
    crate::output::render_markdown(&text);
}
