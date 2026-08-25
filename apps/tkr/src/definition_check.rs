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
//! The tier runs against the frontends linked into `tkr` itself (`.tkd`,
//! `.tkdp`), not workspace discovery: the check must work from any directory
//! and cost nothing.

use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
use tokeira_platform::definition::DirectoryPartSources;
use tokeira_platform_definition::{
    tkd,
    tkd::{EvalError, FieldMap, HostBridge},
    tkdp::preflight,
};

/// The frontends compiled into this binary, keyed by source extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Frontend {
    Tkd,
    Tkdp,
}

impl Frontend {
    fn from_extension(extension: &str) -> Option<Self> {
        match extension {
            "tkd" => Some(Self::Tkd),
            "tkdp" => Some(Self::Tkdp),
            _ => None,
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Tkd => "tkd",
            Self::Tkdp => "tkdp",
        }
    }
}

/// One finding, normalized across the two frontends for rendering.
struct CheckFinding {
    /// The file the finding is in, as the operator named it (root) or as
    /// resolved beside it (parts).
    file: String,
    message: String,
}

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
    // `--format` overrides a misleading extension; otherwise the extension
    // names the frontend.
    let frontend = match format {
        Some(format) => Frontend::from_extension(&format.to_string()).ok_or_else(|| {
            anyhow::anyhow!(
                "`{format}` is not a linked frontend\n\n\
                     The syntax tier checks the frontends built into `tkr`: \
                     `tkd`, `tkdp`."
            )
        })?,
        None => {
            let extension = path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            Frontend::from_extension(extension).ok_or_else(|| {
                anyhow::anyhow!(
                    "no frontend reads `.{extension}`\n\n\
                     The syntax tier checks the frontends built into `tkr`: \
                     `.tkd`, `.tkdp`. Name one explicitly with `--format <id>` \
                     if the extension is misleading."
                )
            })?
        }
    };
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let dir = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let (findings, parts) = match frontend {
        Frontend::Tkd => (check_tkd(path, &source, &dir), Vec::new()),
        Frontend::Tkdp => check_tkdp(path, &source, &dir),
    };
    render_verdict(path, frontend, &findings, &parts, json);
    if findings.is_empty() {
        Ok(())
    } else {
        // The findings on stdout are the answer; the exit code carries the
        // verdict without a second stderr complaint restating it.
        std::process::exit(1);
    }
}

/// `.tkd`: parse + schema + subset + companion parts, through the frontend's
/// own `validate` — no evaluation.
fn check_tkd(path: &Path, source: &str, dir: &Path) -> Vec<CheckFinding> {
    let parts = DirectoryPartSources::new(dir, "tkd");
    match tkd::validate(source, &SyntaxBridge, &parts) {
        Ok(()) => Vec::new(),
        Err(messages) => messages
            .into_iter()
            .map(|message| CheckFinding {
                file: path.display().to_string(),
                message,
            })
            .collect(),
    }
}

/// `.tkdp`: preflight the root, then every companion part it (transitively)
/// imports that resolves to a sibling `.tkdp` file. A part import with no
/// sibling file is left alone — the frontend's contract leaves such names to
/// Monty (a built-in, or a runtime `ModuleNotFoundError`).
fn check_tkdp(path: &Path, source: &str, dir: &Path) -> (Vec<CheckFinding>, Vec<String>) {
    let mut findings = Vec::new();
    let mut parts_checked = Vec::new();
    let facade_names = tokeira_facade_imports(source);
    let facade_refs: Vec<&str> = facade_names.iter().map(String::as_str).collect();
    let mut pending: Vec<String> = Vec::new();
    match preflight::preflight(source, &facade_refs) {
        Ok(preflight) => {
            pending.extend(preflight.part_imports.into_iter().map(|part| part.name));
        }
        Err(errors) => {
            findings.extend(render_tkdp_findings(path, source, errors));
        }
    }
    let mut visited: BTreeSet<String> = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !visited.insert(name.clone()) {
            continue;
        }
        let part_path = dir.join(format!("{name}.tkdp"));
        if !part_path.exists() {
            continue;
        }
        let Ok(part_source) = std::fs::read_to_string(&part_path) else {
            findings.push(CheckFinding {
                file: part_path.display().to_string(),
                message: "companion part exists but could not be read".to_string(),
            });
            continue;
        };
        parts_checked.push(name);
        let part_facades = tokeira_facade_imports(&part_source);
        let part_refs: Vec<&str> = part_facades.iter().map(String::as_str).collect();
        match preflight::preflight_part(&part_source, &part_refs) {
            Ok(part) => {
                pending.extend(part.part_imports.into_iter().map(|entry| entry.name));
            }
            Err(errors) => {
                findings.extend(render_tkdp_findings(&part_path, &part_source, errors));
            }
        }
    }
    parts_checked.sort();
    (findings, parts_checked)
}

fn render_tkdp_findings(
    path: &Path,
    source: &str,
    errors: Vec<preflight::Finding>,
) -> Vec<CheckFinding> {
    errors
        .into_iter()
        .map(|finding| {
            let (line, col) = line_col(source, finding.range.start().to_usize());
            CheckFinding {
                file: format!("{}:{line}:{col}", path.display()),
                message: format!("{}: {}", finding.code, finding.message),
            }
        })
        .collect()
}

/// Line and column (1-based) of a byte offset, for pointing an operator at
/// the finding — the frontend reports byte ranges.
fn line_col(source: &str, offset: usize) -> (usize, usize) {
    let clamped = offset.min(source.len());
    let prefix = &source[..clamped];
    let line = prefix.matches('\n').count() + 1;
    let col = prefix
        .rfind('\n')
        .map(|newline| clamped - newline)
        .unwrap_or(clamped + 1);
    (line, col)
}

/// The facade names a `.tkdp` source imports (`from tokeira import …`).
///
/// The syntax tier has no platform to ask which names the facade actually
/// publishes — that membership question belongs to the engine tier — so the
/// preflight is fed exactly the names the source imports, taking each at its
/// word while every structural check keeps its teeth. A textual scan is
/// enough here: the preflight re-reads the same imports from the real AST,
/// and a miss can only make this tier stricter, never quieter, about a name
/// the engine tier would settle anyway.
fn tokeira_facade_imports(source: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut lines = source.lines();
    while let Some(line) = lines.next() {
        let trimmed = line.trim_start();
        let Some(rest) = trimmed.strip_prefix("from") else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix("tokeira") else {
            continue;
        };
        let Some(clause) = rest.trim_start().strip_prefix("import") else {
            continue;
        };
        if !clause.is_empty() && !clause.starts_with(|c: char| c.is_whitespace() || c == '(') {
            continue;
        }
        let mut clause = clause.to_string();
        if clause.contains('(') {
            while !clause.contains(')') {
                match lines.next() {
                    Some(continuation) => {
                        clause.push(' ');
                        clause.push_str(continuation);
                    }
                    None => break,
                }
            }
        }
        while clause.trim_end().ends_with('\\') {
            clause = clause.trim_end().trim_end_matches('\\').to_string();
            match lines.next() {
                Some(continuation) => {
                    clause.push(' ');
                    clause.push_str(continuation);
                }
                None => break,
            }
        }
        for piece in clause.replace(['(', ')'], " ").split(',') {
            let piece = piece.split('#').next().unwrap_or("");
            let name = piece.split_whitespace().next().unwrap_or("");
            if !name.is_empty() && name != "*" {
                names.push(name.to_string());
            }
        }
    }
    names
}

fn render_verdict(
    path: &Path,
    frontend: Frontend,
    findings: &[CheckFinding],
    parts: &[String],
    json: bool,
) {
    if json {
        let report = serde_json::json!({
            "tier": "syntax",
            "format": frontend.extension(),
            "root": path.display().to_string(),
            "admitted": findings.is_empty(),
            "parts": parts,
            "findings": findings
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
    let parts_note = match parts.len() {
        0 => String::new(),
        1 => " with 1 companion part".to_string(),
        n => format!(" with {n} companion parts"),
    };
    let text = if findings.is_empty() {
        format!(
            "**admitted:** `{}` — `.{}` syntax and subset verify{parts_note}\n\n\
             This is the frontend syntax tier. Engine interpretation — platform \
             vocabulary, typed config, context — happens against a deployment's \
             platform: `tkr definition check --deployment <name>`.\n",
            path.display(),
            frontend.extension(),
        )
    } else {
        let mut text = format!(
            "**not admitted:** `{}` — {} finding{}{parts_note}\n\n",
            path.display(),
            findings.len(),
            if findings.len() == 1 { "" } else { "s" },
        );
        for finding in findings {
            text.push_str(&format!("- `{}` — {}\n", finding.file, finding.message));
        }
        text
    };
    crate::output::render_markdown(&text);
}

/// The syntax tier's [`HostBridge`]: recognition without vocabulary. The
/// `.tkd` checker classifies expressions through `is_kind`/`knows_method`/
/// `knows_assoc` but never evaluates, so the eval half of the trait is
/// unreachable here and refuses defensively.
struct SyntaxBridge;

/// Never constructed: `tkd::validate` stops before evaluation.
#[derive(Clone, Debug)]
struct SyntaxHost;

impl HostBridge for SyntaxBridge {
    type Host = SyntaxHost;
    type Cx = ();
    type Output = ();

    // A blanket `true` would swallow ordinary bindings: the subset checker
    // classifies any single-segment path the bridge claims as a kind
    // literal. Kinds are Rust type names, so the type-name convention —
    // leading uppercase — separates them from `snake_case` bindings without
    // knowing any platform's inventory. Whether the name is *actually* in
    // the platform's vocabulary is the engine tier's question.
    fn is_kind(&self, name: &str) -> bool {
        name.chars().next().is_some_and(char::is_uppercase)
    }

    fn knows_method(&self, _name: &str) -> bool {
        true
    }

    fn knows_assoc(&self, _path: &str) -> bool {
        true
    }

    fn kind_defaults(&self, _name: &str) -> Option<FieldMap<Self::Host>> {
        None
    }

    fn construct_kind(
        &self,
        _name: &str,
        _fields: FieldMap<Self::Host>,
        _cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        Err(EvalError::new("the syntax tier never evaluates"))
    }

    fn assoc(
        &self,
        _path: &str,
        _args: Vec<tkd::Value<Self::Host>>,
        _cx: &Self::Cx,
    ) -> Result<Self::Host, EvalError> {
        Err(EvalError::new("the syntax tier never evaluates"))
    }

    fn call_method(
        &self,
        _recv: &Self::Host,
        _method: &str,
        _args: Vec<tkd::Value<Self::Host>>,
        _cx: &Self::Cx,
    ) -> Result<tkd::Value<Self::Host>, EvalError> {
        Err(EvalError::new("the syntax tier never evaluates"))
    }

    fn host_field(
        &self,
        _host: &Self::Host,
        _field: &str,
    ) -> Result<tkd::Value<Self::Host>, EvalError> {
        Err(EvalError::new("the syntax tier never evaluates"))
    }

    fn cx_host(&self, _cx: &Self::Cx) -> Self::Host {
        SyntaxHost
    }

    fn finish(&self, _ret: Self::Host) -> Result<Self::Output, EvalError> {
        Err(EvalError::new("the syntax tier never evaluates"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facade_imports_read_plain_and_aliased_names() {
        let source = "from tokeira import Deployment, Service as S\n";
        assert_eq!(
            tokeira_facade_imports(source),
            vec!["Deployment", "Service"]
        );
    }

    #[test]
    fn facade_imports_read_parenthesized_multiline_imports() {
        let source = "from tokeira import (\n    Deployment,  # root\n    Service,\n)\n";
        assert_eq!(
            tokeira_facade_imports(source),
            vec!["Deployment", "Service"]
        );
    }

    #[test]
    fn facade_imports_ignore_other_modules() {
        let source = "from tokeira_extras import X\nfrom tokeira.sub import Y\nimport tokeira\n";
        assert!(tokeira_facade_imports(source).is_empty());
    }

    #[test]
    fn line_col_is_one_based() {
        assert_eq!(line_col("ab\ncd", 0), (1, 1));
        assert_eq!(line_col("ab\ncd", 3), (2, 1));
        assert_eq!(line_col("ab\ncd", 4), (2, 2));
    }
}
