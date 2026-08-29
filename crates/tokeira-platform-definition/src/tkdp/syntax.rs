//! Platform-free validation of a complete `.tkdp` definition source set.
//!
//! The root and every resolvable transitive companion part pass through the
//! real Ruff parser and preflight checker. Resolver misses remain candidates
//! for Monty built-ins (or later runtime import failures), matching frontend
//! evaluation; this tier only reports source files the resolver actually
//! serves.

use std::collections::{BTreeSet, VecDeque};

use ruff_text_size::TextSize;
use tokeira_platform::definition::SourceResolver;

use crate::tkdp::{
    preflight::{Finding, preflight_part_syntax, preflight_syntax},
    source_map::LineTable,
};

/// One platform-free preflight finding in the root or a companion part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxFinding {
    /// Companion part name without an extension, or `None` for the root.
    pub(crate) part: Option<String>,
    /// Stable finding code followed by its actionable message.
    pub(crate) message: String,
    /// One-based source line.
    pub(crate) line: u32,
    /// One-based source column.
    pub(crate) column: u32,
}

/// Deterministic result of validating one complete `.tkdp` source set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxValidation {
    /// Resolvable companion part names, sorted lexically.
    pub(crate) parts: Vec<String>,
    /// Findings in discovery order; an empty list admits the source set.
    pub(crate) findings: Vec<SyntaxFinding>,
}

/// Validate a `.tkdp` root and every resolvable transitive companion part
/// through the platform-free frontend syntax tier.
///
/// Explicit `tokeira` facade names are not checked for membership because
/// only engine interpretation has a deployment platform's vocabulary. Every
/// other preflight rule stays active. A resolver miss is not a finding: the
/// frontend leaves unserved imports for Monty to resolve as built-ins or
/// report at runtime.
pub fn validate_syntax(source: &str, part_sources: &dyn SourceResolver) -> SyntaxValidation {
    let root = match preflight_syntax(source) {
        Ok(root) => root,
        Err(findings) => {
            return SyntaxValidation {
                parts: Vec::new(),
                findings: locate_findings(None, source, findings),
            };
        }
    };

    let mut pending: VecDeque<String> = root
        .part_imports
        .into_iter()
        .map(|part| part.name)
        .collect();
    let mut visited = BTreeSet::new();
    let mut parts = BTreeSet::new();
    let mut findings = Vec::new();
    while let Some(name) = pending.pop_front() {
        if !visited.insert(name.clone()) {
            continue;
        }
        let Ok(bytes) = part_sources.resolve(&name) else {
            continue;
        };
        parts.insert(name.clone());
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(error) => {
                let prefix = std::str::from_utf8(&bytes[..error.valid_up_to()])
                    .expect("the UTF-8 error's valid prefix is valid");
                let (line, column) = line_column(prefix, prefix.len());
                findings.push(SyntaxFinding {
                    part: Some(name),
                    message: format!("part source is not UTF-8: {error}"),
                    line,
                    column,
                });
                continue;
            }
        };
        match preflight_part_syntax(text) {
            Ok(part) => pending.extend(part.part_imports.into_iter().map(|part| part.name)),
            Err(part_findings) => {
                findings.extend(locate_findings(Some(name), text, part_findings));
            }
        }
    }

    SyntaxValidation {
        parts: parts.into_iter().collect(),
        findings,
    }
}

fn locate_findings(
    part: Option<String>,
    source: &str,
    findings: Vec<Finding>,
) -> Vec<SyntaxFinding> {
    findings
        .into_iter()
        .map(|finding| {
            let (line, column) = line_column(source, finding.range.start().to_usize());
            SyntaxFinding {
                part: part.clone(),
                message: format!("{}: {}", finding.code, finding.message),
                line,
                column,
            }
        })
        .collect()
}

fn line_column(source: &str, offset: usize) -> (u32, u32) {
    let clamped = offset.min(source.len());
    LineTable::new(source).line_column(TextSize::new(clamped as u32))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use tokeira_platform::definition::{PartResolveError, SourceResolver};

    use super::*;

    struct MapParts(BTreeMap<&'static str, &'static str>);

    impl SourceResolver for MapParts {
        fn resolve(&self, name: &str) -> Result<Arc<[u8]>, PartResolveError> {
            self.0
                .get(name)
                .map(|source| Arc::from(source.as_bytes()))
                .ok_or_else(|| PartResolveError {
                    name: name.to_string(),
                    reason: "absent from the fixture".to_string(),
                })
        }
    }

    const ROOT: &str =
        "import first\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n";

    #[test]
    fn line_and_column_are_one_based() {
        assert_eq!(line_column("ab\ncd", 0), (1, 1));
        assert_eq!(line_column("ab\ncd", 3), (2, 1));
        assert_eq!(line_column("ab\ncd", 4), (2, 2));
    }

    #[test]
    fn facade_membership_is_deferred_but_star_imports_still_refuse() {
        let named = "from tokeira import PlatformSpecific\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n";
        assert!(
            validate_syntax(named, &MapParts(BTreeMap::new()))
                .findings
                .is_empty()
        );

        let star = "from tokeira import *\n\n\ndef config():\n    pass\n\n\ndef deployment(cfg, cx):\n    pass\n";
        let validation = validate_syntax(star, &MapParts(BTreeMap::new()));
        assert_eq!(validation.findings.len(), 1);
        assert!(validation.findings[0].message.contains("TKDP012"));
    }

    #[test]
    fn transitive_resolvable_parts_are_checked() {
        let parts = MapParts(BTreeMap::from([
            ("first", "import second\n"),
            ("second", "from tokeira import *\n"),
        ]));
        let validation = validate_syntax(ROOT, &parts);
        assert_eq!(validation.parts, ["first", "second"]);
        assert_eq!(validation.findings.len(), 1);
        assert_eq!(validation.findings[0].part.as_deref(), Some("second"));
        assert!(validation.findings[0].message.contains("TKDP012"));
    }

    #[test]
    fn unserved_imports_are_left_for_monty() {
        let validation = validate_syntax(ROOT, &MapParts(BTreeMap::new()));
        assert!(validation.parts.is_empty());
        assert!(validation.findings.is_empty());
    }
}
