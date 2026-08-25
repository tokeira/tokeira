//! The definition frontends: every shipped format, one crate.
//!
//! A platform definition is authored in one of the formats this crate
//! carries — `tkd` (Rust syntax, interpreted) or `tkdp` (Python, executed by
//! Monty) — and each frontend lives here as a feature-gated module with its
//! public surface unchanged from its former standalone crate. Composition
//! selects exactly one format feature per bound `tkp` (the generated
//! manifest names it), so a `tkd`-only build never compiles the Monty/ruff
//! dependency train; workspace consumers name the features they need.
//!
//! Frontend contracts are format-owned: see the `tkd` and `tkdp` modules for
//! each format's own invariants and entry points. (Named in prose, not
//! intra-doc links — a single-feature build documents only the enabled
//! module, and a link to the absent one would break the doc build.)
//!
//! The crate root owns dispatch across the frontends linked into a consumer.
//! [`check_syntax`] selects a frontend by its format identity or canonical
//! source extension, resolves companion sources beside the root, and returns
//! one format-neutral report. Callers therefore do not duplicate the set of
//! shipped formats or learn how an individual frontend composes source files.

use std::path::{Path, PathBuf};

use thiserror::Error;
use tokeira_orchestrator::{DefinitionFormatId, DefinitionSourceExtension};
use tokeira_platform::definition::{DirectoryPartSources, SourceResolver};

#[cfg(feature = "tkd")]
pub mod tkd;

#[cfg(feature = "tkdp")]
pub mod tkdp;

/// A platform-free syntax verdict for one complete definition source set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxCheck {
    /// Canonical identity of the frontend that checked the source.
    pub format: DefinitionFormatId,
    /// Canonical extension owned by that frontend, without a leading dot.
    pub source_extension: DefinitionSourceExtension,
    /// Resolvable companion names, sorted lexically.
    pub parts: Vec<String>,
    /// Frontend findings; an empty list admits the source set.
    pub findings: Vec<SyntaxCheckFinding>,
}

/// One format-neutral finding from the root or a companion source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxCheckFinding {
    /// Display-ready source location, including line and column when known.
    pub file: String,
    /// Stable frontend diagnostic code, when available, and actionable text.
    pub message: String,
}

/// Failure to select a syntax frontend linked into the current binary.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct SyntaxCheckError {
    message: String,
}

#[derive(Debug, Clone, Copy)]
struct SyntaxFrontend {
    format: &'static str,
    source_extension: &'static str,
    validate: fn(&str, &dyn SourceResolver) -> FrontendValidation,
}

#[derive(Debug)]
struct FrontendValidation {
    parts: Vec<String>,
    findings: Vec<FrontendFinding>,
}

#[derive(Debug)]
struct FrontendFinding {
    part: Option<String>,
    position: Option<(u32, u32)>,
    message: String,
}

const LINKED_SYNTAX_FRONTENDS: &[SyntaxFrontend] = &[
    #[cfg(feature = "tkd")]
    SyntaxFrontend {
        format: "tkd",
        source_extension: "tkd",
        validate: validate_tkd_syntax,
    },
    #[cfg(feature = "tkdp")]
    SyntaxFrontend {
        format: "tkdp",
        source_extension: "tkdp",
        validate: validate_tkdp_syntax,
    },
];

/// Check a root source through one linked frontend's platform-free syntax tier.
///
/// `format` overrides a misleading path extension. Otherwise the root's
/// extension selects the frontend. Companion-source discovery and traversal
/// are frontend-owned: resolvable siblings are read from the root's directory
/// using the selected frontend's canonical extension.
pub fn check_syntax(
    root: &Path,
    source: &str,
    format: Option<&DefinitionFormatId>,
) -> Result<SyntaxCheck, SyntaxCheckError> {
    let frontend = match format {
        Some(format) => LINKED_SYNTAX_FRONTENDS
            .iter()
            .find(|frontend| frontend.format == format.as_str())
            .ok_or_else(|| unknown_format(format))?,
        None => {
            let extension = root
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            LINKED_SYNTAX_FRONTENDS
                .iter()
                .find(|frontend| frontend.source_extension == extension)
                .ok_or_else(|| unknown_extension(extension))?
        }
    };

    let dir = root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    // The descriptor is the single source of truth for both dispatch and
    // sibling resolution, so adding a frontend cannot leave the two mappings
    // inconsistent in an operator client.
    let part_sources = DirectoryPartSources::new(&dir, frontend.source_extension);
    let validation = (frontend.validate)(source, &part_sources);
    let findings = validation
        .findings
        .into_iter()
        .map(|finding| {
            let path = finding.part.map_or_else(
                || root.to_path_buf(),
                |part| dir.join(format!("{part}.{}", frontend.source_extension)),
            );
            let file = finding.position.map_or_else(
                || path.display().to_string(),
                |(line, column)| format!("{}:{line}:{column}", path.display()),
            );
            SyntaxCheckFinding {
                file,
                message: finding.message,
            }
        })
        .collect();

    Ok(SyntaxCheck {
        format: DefinitionFormatId::new(frontend.format)
            .expect("linked frontend formats are valid static identifiers"),
        source_extension: DefinitionSourceExtension::new(frontend.source_extension)
            .expect("linked frontend extensions are valid static identifiers"),
        parts: validation.parts,
        findings,
    })
}

fn unknown_format(format: &DefinitionFormatId) -> SyntaxCheckError {
    SyntaxCheckError {
        message: format!(
            "`{format}` is not a linked frontend\n\n\
             The syntax tier checks the linked frontends: {}.",
            linked_formats()
        ),
    }
}

fn unknown_extension(extension: &str) -> SyntaxCheckError {
    SyntaxCheckError {
        message: format!(
            "no frontend reads `.{extension}`\n\n\
             The syntax tier checks the linked source extensions: {}. Name one \
             explicitly with `--format <id>` if the extension is misleading.",
            linked_extensions()
        ),
    }
}

fn linked_formats() -> String {
    LINKED_SYNTAX_FRONTENDS
        .iter()
        .map(|frontend| format!("`{}`", frontend.format))
        .collect::<Vec<_>>()
        .join(", ")
}

fn linked_extensions() -> String {
    LINKED_SYNTAX_FRONTENDS
        .iter()
        .map(|frontend| format!("`.{}`", frontend.source_extension))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(feature = "tkd")]
fn validate_tkd_syntax(source: &str, parts: &dyn SourceResolver) -> FrontendValidation {
    let findings = match tkd::validate_syntax(source, parts) {
        Ok(()) => Vec::new(),
        Err(messages) => messages
            .into_iter()
            .map(|message| FrontendFinding {
                part: None,
                position: None,
                message,
            })
            .collect(),
    };
    FrontendValidation {
        parts: Vec::new(),
        findings,
    }
}

#[cfg(feature = "tkdp")]
fn validate_tkdp_syntax(source: &str, parts: &dyn SourceResolver) -> FrontendValidation {
    let validation = tkdp::validate_syntax(source, parts);
    FrontendValidation {
        parts: validation.parts,
        findings: validation
            .findings
            .into_iter()
            .map(|finding| FrontendFinding {
                part: finding.part,
                position: Some((finding.line, finding.column)),
                message: finding.message,
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "tkd")]
    const TKD_SOURCE: &str = r#"
struct Config {}

fn config() -> Config {
    Config {}
}

fn deployment(cfg: Config, cx: Context) -> Deployment {
    Deployment::new(&["default"])
}
"#;

    #[cfg(feature = "tkdp")]
    const TKDP_SOURCE: &str =
        "import first\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n";

    #[cfg(feature = "tkd")]
    #[test]
    fn format_override_selects_tkd_despite_a_misleading_extension() {
        let format = DefinitionFormatId::new("tkd").expect("format");
        let check = check_syntax(Path::new("definition.txt"), TKD_SOURCE, Some(&format))
            .expect("tkd is linked");

        assert_eq!(check.format.as_str(), "tkd");
        assert_eq!(check.source_extension.as_str(), "tkd");
        assert!(check.parts.is_empty());
        assert!(check.findings.is_empty());
    }

    #[cfg(feature = "tkdp")]
    #[test]
    fn extension_dispatch_owns_transitive_parts_and_locations() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("definition.tkdp");
        std::fs::write(temp.path().join("first.tkdp"), "import second\n").expect("first part");
        std::fs::write(temp.path().join("second.tkdp"), "from tokeira import *\n")
            .expect("second part");

        let check = check_syntax(&root, TKDP_SOURCE, None).expect("tkdp is linked");

        assert_eq!(check.format.as_str(), "tkdp");
        assert_eq!(check.source_extension.as_str(), "tkdp");
        assert_eq!(check.parts, ["first", "second"]);
        assert_eq!(check.findings.len(), 1);
        assert_eq!(
            check.findings[0].file,
            format!("{}:1:21", temp.path().join("second.tkdp").display())
        );
        assert!(check.findings[0].message.contains("TKDP012"));
    }

    #[test]
    fn unknown_extension_reports_the_registry_owned_inventory() {
        let error =
            check_syntax(Path::new("definition.txt"), "", None).expect_err("txt is not linked");
        let message = error.to_string();
        assert!(message.contains("no frontend reads `.txt`"));
        assert!(message.contains("`.tkd`"));
        assert!(message.contains("`.tkdp`"));
    }
}
