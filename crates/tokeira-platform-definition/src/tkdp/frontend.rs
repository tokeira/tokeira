//! The `.tkdp` definition frontend: one stateless `evaluate` from source
//! bytes to the completed transient structure.
//!
//! Pipeline per invocation: UTF-8 admission → preflight (restricted subset,
//! hygiene, entrypoints, facade import contract) → lowering (match splice,
//! import blanking) → facade synthesis from the platform namespaces and the
//! serialized typed context → assembly → Monty execution → structural-result
//! conversion. Every failure path lands as one [`FrontendDiagnostic`] whose
//! position, when one exists, is in the operator's `.tkdp` file.

use serde::Serialize;
use tokeira_orchestrator::DefinitionFormatId;
use tokeira_platform::{
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource, Namespace},
    error::{DiagnosticCategory, FrontendDiagnostic, SourceRange},
};

use std::collections::{BTreeSet, VecDeque};

use crate::tkdp::{
    convert::convert,
    diagnostics,
    facade::{self, FACADE_MODULE_NAME},
    lower::lower,
    preflight::{PartImport, Preflight, preflight, preflight_part},
    program::{PartUnit, Program, assemble},
    runner::execute,
};

/// The trusted `.tkdp` frontend, selected independently of any platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TkdpFrontend {
    format: DefinitionFormatId,
}

impl TkdpFrontend {
    /// Construct the canonical first-party `.tkdp` frontend.
    pub(crate) fn new() -> Self {
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

    /// Discovers and prepares the definition's parts: every non-facade import
    /// is offered to the resolver, transitively — a served name is a part
    /// (validated, lowered, registered as a source module); an unserved name
    /// is left for Monty, where it is either a built-in or a runtime
    /// `ModuleNotFoundError` at the import site.
    ///
    /// Part failures carry the part's file name in the message and no range —
    /// diagnostic ranges are root-source-relative only.
    fn discover_parts(
        &self,
        source: &FrontendSource<'_>,
        root_imports: &[PartImport],
        resolver: &dyn tokeira_platform::definition::SourceResolver,
        facade_names: &[&str],
    ) -> Result<Vec<PartUnit>, FrontendDiagnostic> {
        let mut queue: VecDeque<String> = root_imports.iter().map(|i| i.name.clone()).collect();
        let mut seen = BTreeSet::new();
        let mut parts = Vec::new();
        while let Some(name) = queue.pop_front() {
            if name == FACADE_MODULE_NAME || !seen.insert(name.clone()) {
                continue;
            }
            let Ok(bytes) = resolver.resolve(&name) else {
                continue;
            };
            let file_name = format!("{name}.tkdp");
            let text = std::str::from_utf8(&bytes)
                .map_err(|error| {
                    self.diagnostic(
                        source,
                        None,
                        format!("{file_name}: part source is not UTF-8: {error}"),
                    )
                })?
                .to_owned();
            let part = preflight_part(&text, facade_names).map_err(|findings| {
                self.diagnostic(
                    source,
                    None,
                    diagnostics::render(&file_name, &text, &findings),
                )
            })?;
            queue.extend(part.part_imports.iter().map(|i| i.name.clone()));
            let lowered = lower(&text, &part.module, &file_name);
            parts.push(PartUnit {
                name,
                file_name,
                original: text,
                lowered,
            });
        }
        Ok(parts)
    }

    /// Runs admission, preflight, part discovery, lowering, facade synthesis,
    /// and assembly — everything `evaluate` does before Monty runs. Shared by
    /// `evaluate` and [`Self::transient_program`] so an inspected program is
    /// byte-for-byte the program that executes.
    fn prepare<'a, C>(
        &self,
        source: &FrontendSource<'a>,
        context: &C,
        namespaces: &[Namespace],
        parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<Prepared<'a>, FrontendDiagnostic>
    where
        C: Serialize,
    {
        let text = std::str::from_utf8(source.bytes).map_err(|error| {
            self.diagnostic(
                source,
                None,
                format!("definition source is not UTF-8: {error}"),
            )
        })?;

        let label = source.source_name.to_string();
        let kind_names: Vec<&str> = namespaces
            .iter()
            .flat_map(|namespace| namespace.kinds.iter().copied())
            .collect();
        let names = facade::facade_names(&kind_names);
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
        let synthesized = facade::render(&kind_names, &context_value);

        let part_units = self.discover_parts(source, &pf.part_imports, parts, &names)?;
        let lowered = lower(text, &pf.module, &label);
        let program = assemble(synthesized, lowered, part_units);
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
    pub fn transient_program<C>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        namespaces: &[Namespace],
        parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<Program, FrontendDiagnostic>
    where
        C: Serialize,
    {
        self.prepare(&source, context, namespaces, parts)
            .map(|prepared| prepared.program)
    }
}

impl DefinitionFrontend for TkdpFrontend {
    fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    fn evaluate<C>(
        &self,
        source: FrontendSource<'_>,
        context: &C,
        namespaces: &[Namespace],
        parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<FrontendOutput, FrontendDiagnostic>
    where
        C: Serialize,
    {
        let prepared = self.prepare(&source, context, namespaces, parts)?;

        let result = execute(&prepared.program, prepared.text).map_err(|failure| {
            self.diagnostic(&source, failure.range.and_then(to_range), failure.message)
        })?;

        let pf = prepared.preflight;
        convert(result, namespaces, &pf.call_sites, pf.deployment_range)
            .map_err(|error| self.diagnostic(&source, to_range(error.range), error.message))
    }
}
