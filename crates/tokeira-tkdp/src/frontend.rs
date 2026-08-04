//! The `.tkdp` definition frontend: one stateless `evaluate` from source
//! bytes to the completed transient structure.
//!
//! Pipeline per invocation: UTF-8 admission → preflight (restricted subset,
//! hygiene, entrypoints, facade import contract) → lowering (match splice,
//! import blanking) → facade synthesis from the engine kind inventory and the
//! serialized typed context → assembly → Monty execution → structural-result
//! conversion. Every failure path lands as one [`FrontendDiagnostic`] whose
//! position, when one exists, is in the operator's `.tkdp` file.

use serde::Serialize;
use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource},
    error::{DiagnosticCategory, FrontendDiagnostic, SourceRange},
    kind::{KindFunctions, ProviderKind},
};

use crate::{
    convert::convert,
    facade,
    lower::lower,
    preflight::{Finding, preflight},
    program::assemble,
    runner::execute,
    source_map::LineTable,
};

/// The trusted `.tkdp` frontend, selected independently of any platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TkdpFrontend {
    format: DefinitionFormatId,
}

impl TkdpFrontend {
    /// Construct the canonical first-party `.tkdp` frontend.
    pub fn new() -> Self {
        Self {
            format: DefinitionFormatId::new("tkdp")
                .expect("the built-in tkdp definition-format id is canonical"),
        }
    }
}

impl Default for TkdpFrontend {
    fn default() -> Self {
        Self::new()
    }
}

/// Conventional definition-frontend export consumed by generated composition
/// roots.
pub fn frontend() -> TkdpFrontend {
    TkdpFrontend::new()
}

impl DefinitionFrontend for TkdpFrontend {
    fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    fn evaluate<C, K>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        kinds: KindFunctions<K>,
    ) -> Result<FrontendOutput<K>, FrontendDiagnostic>
    where
        C: Serialize,
        K: ProviderKind + 'static,
    {
        let diagnostic = |range: Option<SourceRange>, message: String| FrontendDiagnostic {
            format: self.format.clone(),
            source_name: source.source_name.clone(),
            range,
            category: DiagnosticCategory::Frontend,
            message,
        };
        let to_range = |range: ruff_text_size::TextRange| {
            SourceRange::new(range.start().to_usize(), range.end().to_usize()).ok()
        };

        let text = std::str::from_utf8(source.bytes).map_err(|error| {
            diagnostic(None, format!("definition source is not UTF-8: {error}"))
        })?;

        let names = facade::facade_names(kinds.names);
        let pf = preflight(text, &names).map_err(|findings| {
            diagnostic(
                findings.first().and_then(|f| to_range(f.range)),
                render_findings(text, &findings),
            )
        })?;

        let context_value = serde_json::to_value(context).map_err(|error| {
            diagnostic(None, format!("platform context did not serialize: {error}"))
        })?;
        let synthesized = facade::render(kinds.names, &pf.imports, &context_value);

        let label = source.source_name.to_string();
        let lowered = lower(text, &pf.module, &label, &pf.import_ranges);
        let program = assemble(&synthesized, lowered);

        let result = execute(&program, text)
            .map_err(|failure| diagnostic(failure.range.and_then(to_range), failure.message))?;

        convert(result, &kinds, &pf.call_sites, pf.deployment_range)
            .map_err(|error| diagnostic(to_range(error.range), error.message))
    }
}

/// All findings of one preflight pass as a single operator-facing message,
/// each with its code and position.
fn render_findings(source: &str, findings: &[Finding]) -> String {
    let table = LineTable::new(source);
    let mut message = format!(
        "definition rejected with {} finding{}:",
        findings.len(),
        if findings.len() == 1 { "" } else { "s" }
    );
    for finding in findings {
        let (line, column) = table.line_column(finding.range.start());
        message.push_str(&format!(
            "\n  [{}] {line}:{column}: {}",
            finding.code, finding.message
        ));
    }
    message
}
