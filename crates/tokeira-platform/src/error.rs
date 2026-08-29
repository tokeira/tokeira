//! Located frontend, structural graph, provider-kind, and publication errors.

use std::{fmt, path::PathBuf};

use thiserror::Error;
use tokeira_orchestrator::{DefinitionFormatId, RelativeDefinitionPath};

use crate::definition::DefinitionSourceName;

/// Half-open byte range supplied by a definition frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceRange {
    /// Inclusive byte offset.
    pub(crate) start: usize,
    /// Exclusive byte offset.
    pub(crate) end: usize,
}

impl SourceRange {
    /// Construct a range whose end is not before its start.
    pub fn new(start: usize, end: usize) -> Result<Self, SourceRangeError> {
        if end < start {
            return Err(SourceRangeError { start, end });
        }
        Ok(Self { start, end })
    }
}

/// Error returned for an inverted source range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("source range end {end} is before start {start}")]
pub struct SourceRangeError {
    /// Inclusive start supplied by the caller.
    pub(crate) start: usize,
    /// Exclusive end supplied by the caller.
    pub(crate) end: usize,
}

/// Stable category used by shells to render a frontend-neutral diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiagnosticCategory {
    /// Parsing or frontend subset admission failed.
    Frontend,
    /// Typed platform configuration admission failed.
    Config,
    /// Provider-kind construction failed.
    Kind,
    /// Deployment graph construction or completion failed.
    Graph,
}

/// A located diagnostic independent of the selected frontend runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendDiagnostic {
    /// Definition format selected for evaluation.
    pub format: DefinitionFormatId,
    /// Display-safe source identity supplied by the shell.
    pub source_name: DefinitionSourceName,
    /// Frontend-supplied byte range, when one exists.
    pub range: Option<SourceRange>,
    /// Stable diagnostic class.
    pub category: DiagnosticCategory,
    /// Actionable human-readable detail.
    pub message: String,
}

impl fmt::Display for FrontendDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}: {}", self.format, self.source_name, self.message)
    }
}

impl std::error::Error for FrontendDiagnostic {}

/// Failure to decode, validate, or realize one provider kind.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("provider kind error: {message}")]
pub struct KindError {
    /// Actionable provider-owned or decode failure detail.
    pub message: String,
    /// Most specific author-value range involved in the failure.
    pub range: Option<SourceRange>,
}

impl KindError {
    /// Construct an unlocated kind failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            range: None,
        }
    }

    /// Attach a range unless a nested decode already supplied one.
    pub fn at(mut self, range: Option<SourceRange>) -> Self {
        if self.range.is_none() {
            self.range = range;
        }
        self
    }
}

/// One structural finding accumulated while completing a graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphFinding {
    /// More than one namespace uses the same name.
    DuplicateNamespace(String),
    /// More than one module uses the same name.
    DuplicateModule(String),
    /// A module dependency refers to no declared module.
    UnknownModuleDependency { module: String, dependency: String },
    /// A module dependency is declared after its dependant.
    ModuleDependencyOrder { module: String, dependency: String },
    /// Module dependencies contain a cycle.
    ModuleCycle(Vec<String>),
    /// More than one resource in a module uses the same logical id.
    DuplicateResource { module: String, resource: String },
    /// A resource belongs to no declared module.
    UnknownResourceModule { module: String, resource: String },
    /// A resource dependency refers to no declared logical resource.
    UnknownResourceDependency {
        module: String,
        resource: String,
        dependency_module: String,
        dependency_resource: String,
    },
    /// A resource dependency is declared after its dependant.
    ResourceDependencyOrder {
        module: String,
        resource: String,
        dependency_module: String,
        dependency_resource: String,
    },
    /// Resource dependencies contain a cycle.
    ResourceCycle(Vec<String>),
    /// More than one writeback uses the same dotted key.
    DuplicateWriteback(String),
    /// A writeback refers to no declared resource.
    UnknownWritebackResource {
        key: String,
        module: String,
        resource: String,
    },
}

/// Failure to construct or complete a structural graph.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GraphError {
    /// A private frontend reference does not name a declared resource.
    #[error("unknown resource `{module}/{resource}`")]
    UnknownResource { module: String, resource: String },
    /// A resource kind does not declare the requested output.
    #[error("kind `{kind}` has no output `{output}`; supported outputs: {supported:?}")]
    UnknownOutput {
        /// Provider kind name.
        kind: String,
        /// Rejected output name.
        output: String,
        /// Complete declared output inventory.
        supported: Vec<String>,
    },
    /// Completion found structural failures.
    #[error("deployment graph is invalid: {0:?}")]
    Invalid(Vec<GraphFinding>),
}

/// Failure to evaluate or admit one source.
#[derive(Debug, Error)]
pub enum DefinitionError {
    /// Selected source and statically assembled frontend disagree.
    #[error(
        "definition source selects format `{source_format}` but provisioner contains `{frontend_format}`"
    )]
    FormatMismatch {
        /// Source-selected format.
        source_format: DefinitionFormatId,
        /// Statically assembled frontend format.
        frontend_format: DefinitionFormatId,
    },
    /// Frontend evaluation failed with a located diagnostic.
    #[error(transparent)]
    Frontend(#[from] FrontendDiagnostic),
}

/// One pure provider-kind verification finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationFinding {
    /// Authored kind input is invalid independent of invocation placement.
    InvalidInput {
        /// Logical module/resource identity.
        resource: String,
        /// Provider kind identity.
        provider_kind: String,
        /// Provider-owned failure detail.
        message: String,
    },
    /// The kind's provider is not wired by the executing platform. Produced
    /// by the engine kind library's wiring check; carried here so every
    /// verification finding renders through one report path. Strings only —
    /// this crate stays ignorant of concrete providers.
    UnwiredProvider {
        /// Logical module/resource identity.
        resource: String,
        /// Provider kind identity.
        provider_kind: String,
        /// Provider the kind requires.
        provider: String,
        /// Platform that does not wire it.
        platform: String,
    },
}

/// Complete pure verification report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationReport {
    /// Findings in resource declaration order.
    pub(crate) findings: Vec<VerificationFinding>,
}

impl fmt::Display for VerificationReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "definition verification failed: {:?}", self.findings)
    }
}

impl std::error::Error for VerificationReport {}

/// Failure to realize one already-verified resource at real placement.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("failed to realize `{resource}` as `{provider_kind}`: {message}")]
pub struct ProjectionError {
    /// Logical resource identity.
    pub(crate) resource: String,
    /// Concrete provider kind identity.
    pub(crate) provider_kind: String,
    /// Provider-owned failure detail.
    pub(crate) message: String,
}

/// Failure to atomically publish deterministic inspection bytes.
#[derive(Debug, Error)]
pub enum InspectionError {
    /// Deployment root could not be canonicalized.
    #[error("failed to prepare inspection target `{path}`: {source}")]
    Prepare {
        /// Relative target involved.
        path: PathBuf,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// Existing symlink ancestry escapes the admitted deployment root.
    #[error("inspection target `{path}` escapes the deployment directory through a symlink")]
    EscapingTarget {
        /// Rejected relative target.
        path: PathBuf,
    },
    /// Same-directory temporary bytes could not be written durably.
    #[error("failed to stage inspection target `{path}`: {source}")]
    Stage {
        /// Relative target involved.
        path: RelativeDefinitionPath,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// Atomic replacement failed.
    #[error("failed to publish inspection target `{path}`: {source}")]
    Publish {
        /// Relative target involved.
        path: RelativeDefinitionPath,
        /// Filesystem failure.
        source: std::io::Error,
    },
}
