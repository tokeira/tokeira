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

use std::{
    fs,
    path::{Path, PathBuf},
};

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

/// Exact source bytes staged for one definition root and its frontend-owned
/// companion candidates.
///
/// The root dispatcher owns the canonical extension and sibling convention;
/// deployment clients consume this result without learning either detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionSourceSet {
    /// Bytes of the selected root document.
    pub root: Vec<u8>,
    /// Companion filenames and bytes, ordered lexically by filename.
    pub parts: Vec<(String, Vec<u8>)>,
}

/// Failure to select or read a complete definition source set.
#[derive(Debug, Error)]
pub enum DefinitionSourceSetError {
    /// No linked frontend owns the requested format or root extension.
    #[error(transparent)]
    Frontend(#[from] SyntaxCheckError),
    /// The root document could not be read.
    #[error("failed to read definition root {path}: {source}")]
    Root {
        /// Root path supplied by the caller.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// The root's containing directory could not be enumerated.
    #[error("failed to read definition directory {path}: {source}")]
    Directory {
        /// Directory containing the selected root.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// A companion filename was not portable UTF-8.
    #[error("definition companion {path} has a non-UTF-8 filename")]
    NonUtf8Part {
        /// Companion path that could not be represented.
        path: PathBuf,
    },
    /// A companion document could not be read.
    #[error("failed to read definition companion {path}: {source}")]
    Part {
        /// Companion path selected by the frontend.
        path: PathBuf,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
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
    let frontend = select_frontend(root, format)?;

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

/// Read a root and the complete sibling candidate set selected by its linked
/// frontend.
///
/// Evaluation decides which candidates are actually served. Staging retains
/// all same-format siblings so later operator edits cannot introduce a part
/// that creation silently discarded.
pub fn read_source_set(
    root: &Path,
    format: Option<&DefinitionFormatId>,
) -> Result<DefinitionSourceSet, DefinitionSourceSetError> {
    let frontend = select_frontend(root, format)?;
    let root_bytes = fs::read(root).map_err(|source| DefinitionSourceSetError::Root {
        path: root.to_path_buf(),
        source,
    })?;
    let dir = root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let root_name = root.file_name();
    let entries = fs::read_dir(dir).map_err(|source| DefinitionSourceSetError::Directory {
        path: dir.to_path_buf(),
        source,
    })?;
    let mut parts = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| DefinitionSourceSetError::Directory {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some(frontend.source_extension)
            || path.file_name() == root_name
            || !path.is_file()
        {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| DefinitionSourceSetError::NonUtf8Part { path: path.clone() })?
            .to_string();
        let bytes =
            fs::read(&path).map_err(|source| DefinitionSourceSetError::Part { path, source })?;
        parts.push((name, bytes));
    }
    parts.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(DefinitionSourceSet {
        root: root_bytes,
        parts,
    })
}

fn select_frontend(
    root: &Path,
    format: Option<&DefinitionFormatId>,
) -> Result<&'static SyntaxFrontend, SyntaxCheckError> {
    match format {
        Some(format) => LINKED_SYNTAX_FRONTENDS
            .iter()
            .find(|frontend| frontend.format == format.as_str())
            .ok_or_else(|| unknown_format(format)),
        None => {
            let extension = root
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            LINKED_SYNTAX_FRONTENDS
                .iter()
                .find(|frontend| frontend.source_extension == extension)
                .ok_or_else(|| unknown_extension(extension))
        }
    }
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

    #[cfg(feature = "tkd")]
    #[test]
    fn source_set_collection_uses_the_selected_frontends_extension() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("deployment.tkd");
        std::fs::write(&root, b"root").expect("root");
        std::fs::write(temp.path().join("zeta.tkd"), b"zeta").expect("zeta");
        std::fs::write(temp.path().join("alpha.tkd"), b"alpha").expect("alpha");
        std::fs::write(temp.path().join("peer.tkdp"), b"peer").expect("peer format");

        let sources = read_source_set(&root, None).expect("source set");

        assert_eq!(sources.root, b"root");
        assert_eq!(
            sources.parts,
            [
                ("alpha.tkd".to_string(), b"alpha".to_vec()),
                ("zeta.tkd".to_string(), b"zeta".to_vec()),
            ]
        );
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
