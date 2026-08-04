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
    diagnostics, facade,
    lower::lower,
    preflight::{Preflight, preflight},
    program::{Program, assemble},
    runner::execute,
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

/// The front half of one evaluation: the validated source with the transient
/// program assembled for it, not yet executed.
struct Prepared<'a> {
    text: &'a str,
    preflight: Preflight,
    program: Program,
}

fn to_range(range: ruff_text_size::TextRange) -> Option<SourceRange> {
    SourceRange::new(range.start().to_usize(), range.end().to_usize()).ok()
}

impl TkdpFrontend {
    fn diagnostic(
        &self,
        source: &FrontendSource<'_>,
        range: Option<SourceRange>,
        message: String,
    ) -> FrontendDiagnostic {
        FrontendDiagnostic {
            format: self.format.clone(),
            source_name: source.source_name.clone(),
            range,
            category: DiagnosticCategory::Frontend,
            message,
        }
    }

    /// Runs admission, preflight, lowering, facade synthesis, and assembly —
    /// everything `evaluate` does before Monty runs. Shared by `evaluate` and
    /// [`Self::transient_program`] so an inspected program is byte-for-byte
    /// the program that executes.
    fn prepare<'a, C, K>(
        &self,
        source: &FrontendSource<'a>,
        context: &C,
        kinds: &KindFunctions<K>,
    ) -> Result<Prepared<'a>, FrontendDiagnostic>
    where
        C: Serialize,
        K: ProviderKind + 'static,
    {
        let text = std::str::from_utf8(source.bytes).map_err(|error| {
            self.diagnostic(
                source,
                None,
                format!("definition source is not UTF-8: {error}"),
            )
        })?;

        let label = source.source_name.to_string();
        let names = facade::facade_names(kinds.names);
        let pf = preflight(text, &names).map_err(|findings| {
            self.diagnostic(
                source,
                findings.first().and_then(|f| to_range(f.range)),
                diagnostics::render(&label, text, &findings),
            )
        })?;

        let context_value = serde_json::to_value(context).map_err(|error| {
            self.diagnostic(
                source,
                None,
                format!("platform context did not serialize: {error}"),
            )
        })?;
        let synthesized = facade::render(kinds.names, &pf.imports, &context_value);

        let lowered = lower(text, &pf.module, &label, &pf.import_ranges);
        let program = assemble(&synthesized, lowered);
        Ok(Prepared {
            text,
            preflight: pf,
            program,
        })
    }

    /// Assembles the transient program `evaluate` would execute for this
    /// source, without executing it.
    ///
    /// This is the inspection seam for an operator-level `lower` /
    /// `--show-generated` verb beside `definition check` (carried from the
    /// spike CLI): the returned [`Program`] holds the assembled text and its
    /// source map. No operator command surfaces it today, and the text is
    /// never persisted — evaluation always reassembles.
    pub fn transient_program<C, K>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        kinds: KindFunctions<K>,
    ) -> Result<Program, FrontendDiagnostic>
    where
        C: Serialize,
        K: ProviderKind + 'static,
    {
        self.prepare(&source, context, &kinds)
            .map(|prepared| prepared.program)
    }
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
        let prepared = self.prepare(&source, context, &kinds)?;

        let result = execute(&prepared.program, prepared.text).map_err(|failure| {
            self.diagnostic(&source, failure.range.and_then(to_range), failure.message)
        })?;

        let pf = prepared.preflight;
        convert(result, &kinds, &pf.call_sites, pf.deployment_range)
            .map_err(|error| self.diagnostic(&source, to_range(error.range), error.message))
    }
}
