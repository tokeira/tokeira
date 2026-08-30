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
    author::{LocatedValue, ValueShape, VariantShape},
    definition::{DefinitionFrontend, FrontendOutput, FrontendSource, Namespace},
    error::{DiagnosticCategory, FrontendDiagnostic, SourceRange},
};

use std::collections::{BTreeSet, VecDeque};

use crate::tkdp::{
    convert::convert,
    diagnostics,
    facade::{self, FACADE_MODULE_NAME},
    lower::lower,
    preflight::{CreateField, PartImport, Preflight, preflight, preflight_part},
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
    creates: Vec<CreateField>,
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
                creates: part.creates,
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
        let mut creates = pf.creates.clone();
        creates.extend(
            part_units
                .iter()
                .flat_map(|part| part.creates.iter().cloned()),
        );
        let lowered = lower(text, &pf.module, &label);
        let program = assemble(synthesized, lowered, part_units);
        Ok(Prepared {
            text,
            preflight: pf,
            program,
            creates,
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

    fn execute_prepared(
        &self,
        source: &FrontendSource<'_>,
        namespaces: &[Namespace],
        prepared: Prepared<'_>,
    ) -> Result<FrontendOutput, FrontendDiagnostic> {
        let result = execute(&prepared.program, prepared.text).map_err(|failure| {
            self.diagnostic(source, failure.range.and_then(to_range), failure.message)
        })?;

        convert(
            result,
            namespaces,
            &prepared.preflight.call_sites,
            prepared.preflight.deployment_range,
        )
        .map_err(|error| self.diagnostic(source, to_range(error.range), error.message))
    }
}

fn values_equal(left: &LocatedValue, right: &LocatedValue) -> bool {
    match (&left.value, &right.value) {
        (ValueShape::Unit, ValueShape::Unit) => true,
        (ValueShape::Bool(left), ValueShape::Bool(right)) => left == right,
        (ValueShape::Integer(left), ValueShape::Integer(right)) => left == right,
        // Compare authored f64 values exactly, with no tolerance or NaN
        // normalization. IEEE equality makes a NaN-valued create field refuse
        // even against itself; authored configuration accepts that safe bias.
        (ValueShape::Float(left), ValueShape::Float(right)) => left == right,
        (ValueShape::String(left), ValueShape::String(right)) => left == right,
        (ValueShape::Sequence(left), ValueShape::Sequence(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| values_equal(left, right))
        }
        (ValueShape::Option(left), ValueShape::Option(right)) => match (left, right) {
            (Some(left), Some(right)) => values_equal(left, right),
            (None, None) => true,
            _ => false,
        },
        (ValueShape::Map(left), ValueShape::Map(right)) => {
            left.len() == right.len()
                && left.iter().all(|(left_key, left_value)| {
                    right.iter().any(|(right_key, right_value)| {
                        values_equal(left_key, right_key) && values_equal(left_value, right_value)
                    })
                })
        }
        (
            ValueShape::Struct {
                name: left_name,
                fields: left_fields,
            },
            ValueShape::Struct {
                name: right_name,
                fields: right_fields,
            },
        ) => {
            left_name == right_name
                && left_fields.len() == right_fields.len()
                && left_fields.iter().zip(right_fields).all(
                    |((left_name, left_value), (right_name, right_value))| {
                        left_name == right_name && values_equal(left_value, right_value)
                    },
                )
        }
        (
            ValueShape::Enum {
                name: left_name,
                variant: left_variant,
                body: left_body,
            },
            ValueShape::Enum {
                name: right_name,
                variant: right_variant,
                body: right_body,
            },
        ) => {
            left_name == right_name
                && left_variant == right_variant
                && match (left_body, right_body) {
                    (VariantShape::Unit, VariantShape::Unit) => true,
                    (VariantShape::Newtype(left), VariantShape::Newtype(right)) => {
                        values_equal(left, right)
                    }
                    _ => false,
                }
        }
        _ => false,
    }
}

fn collect_retargets(
    creates: &[CreateField],
    prior: &LocatedValue,
    current: &LocatedValue,
    messages: &mut Vec<String>,
) {
    match (&prior.value, &current.value) {
        (
            ValueShape::Struct {
                name: prior_name,
                fields: prior_fields,
            },
            ValueShape::Struct {
                name: current_name,
                fields: current_fields,
            },
        ) if prior_name == current_name => {
            for create in creates.iter().filter(|create| create.ty == *current_name) {
                let prior_value = prior_fields
                    .iter()
                    .find(|(name, _)| name == &create.field)
                    .map(|(_, value)| value);
                let current_value = current_fields
                    .iter()
                    .find(|(name, _)| name == &create.field)
                    .map(|(_, value)| value);
                if !matches!((prior_value, current_value), (Some(prior), Some(current)) if values_equal(prior, current))
                {
                    messages.push(format!(
                        "`{}.{}` is create-time-immutable; changing it is a retarget, refused (not reconciled)",
                        create.ty, create.field
                    ));
                }
            }
            for (current_field, current_value) in current_fields {
                if let Some((_, prior_value)) = prior_fields
                    .iter()
                    .find(|(prior_field, _)| prior_field == current_field)
                {
                    collect_retargets(creates, prior_value, current_value, messages);
                }
            }
        }
        (ValueShape::Sequence(prior), ValueShape::Sequence(current)) => {
            // Sequence elements pair positionally. A prepend can therefore
            // mis-pair later create fields and false-refuse the edit, which is
            // the safe direction for create-time identity admission.
            for (prior, current) in prior.iter().zip(current) {
                collect_retargets(creates, prior, current, messages);
            }
        }
        (ValueShape::Option(Some(prior)), ValueShape::Option(Some(current))) => {
            collect_retargets(creates, prior, current, messages);
        }
        (ValueShape::Map(prior), ValueShape::Map(current)) => {
            for (current_key, current_value) in current {
                if let Some((_, prior_value)) = prior
                    .iter()
                    .find(|(prior_key, _)| values_equal(prior_key, current_key))
                {
                    collect_retargets(creates, prior_value, current_value, messages);
                }
            }
        }
        (
            ValueShape::Enum {
                variant: prior_variant,
                body: VariantShape::Newtype(prior),
                ..
            },
            ValueShape::Enum {
                variant: current_variant,
                body: VariantShape::Newtype(current),
                ..
            },
        ) if prior_variant == current_variant => {
            collect_retargets(creates, prior, current, messages);
        }
        _ => {}
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
        self.execute_prepared(&source, namespaces, prepared)
    }

    fn retarget_check<C>(
        &self,
        prior: FrontendSource<'_>,
        current: FrontendSource<'_>,
        context: &C,
        namespaces: &[Namespace],
        prior_parts: &dyn tokeira_platform::definition::SourceResolver,
        current_parts: &dyn tokeira_platform::definition::SourceResolver,
    ) -> Result<(), Vec<String>>
    where
        C: Serialize,
    {
        let evaluate = |source: FrontendSource<'_>,
                        parts: &dyn tokeira_platform::definition::SourceResolver,
                        label: &str|
         -> Result<(FrontendOutput, Vec<CreateField>), Vec<String>> {
            let prepared = self
                .prepare(&source, context, namespaces, parts)
                .map_err(|error| vec![format!("{label} definition: {}", error.message)])?;
            let creates = prepared.creates.clone();
            let output = self
                .execute_prepared(&source, namespaces, prepared)
                .map_err(|error| vec![format!("{label} definition: {}", error.message)])?;
            Ok((output, creates))
        };
        let (prior, _) = evaluate(prior, prior_parts, "prior")?;
        // As with TKD, the current definition supplies the admission metadata:
        // it is the contract the operator is proposing to apply.
        let (current, creates) = evaluate(current, current_parts, "current")?;
        let mut messages = Vec::new();
        collect_retargets(&creates, &prior.config, &current.config, &mut messages);
        if messages.is_empty() {
            Ok(())
        } else {
            Err(messages)
        }
    }
}
