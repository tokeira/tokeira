//! Typed framework errors and format-neutral source diagnostics.

use std::fmt;

use thiserror::Error;
use tokeira_orchestrator::DefinitionFormatId;

use crate::definition::DefinitionSourceName;

/// Half-open byte range supplied by a definition frontend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SourceRange {
    /// Inclusive byte offset.
    pub start: usize,
    /// Exclusive byte offset.
    pub end: usize,
}

impl SourceRange {
    /// Construct a non-empty or empty half-open range whose end is not before its start.
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
    pub start: usize,
    /// Exclusive end supplied by the caller.
    pub end: usize,
}

/// Stable category used by shells to render a frontend-neutral diagnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DiagnosticCategory {
    /// Parsing or frontend subset admission failed.
    Frontend,
    /// Typed platform configuration admission failed.
    Config,
    /// Platform context dispatch failed.
    Context,
    /// Provider-kind construction failed.
    Kind,
    /// Deployment graph construction or completion failed.
    Graph,
}

/// A located diagnostic independent of the selected frontend's runtime types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendDiagnostic {
    /// Definition format selected for the evaluation.
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

/// Failure to decode or validate typed platform configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid platform configuration: {message}")]
pub struct ConfigError {
    /// Actionable validation or decoding detail.
    pub message: String,
    /// Most specific frontend range involved in the failure.
    pub range: Option<SourceRange>,
}

impl ConfigError {
    /// Construct an unlocated pure validation failure.
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            range: None,
        }
    }

    /// Attach a range when validation was triggered by a particular author value.
    pub fn at(mut self, range: Option<SourceRange>) -> Self {
        if self.range.is_none() {
            self.range = range;
        }
        self
    }
}

/// Failure to access an immutable platform context contract.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("platform context error: {message}")]
pub struct ContextError {
    /// Actionable failure detail, including supported names where applicable.
    pub message: String,
}

impl ContextError {
    /// Construct a context failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

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

/// A structural finding accumulated while completing a deployment graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphFinding {
    /// More than one module uses the same logical name.
    DuplicateModule(String),
    /// More than one resource in a module uses the same logical id.
    DuplicateResource { module: String, resource: String },
    /// More than one writeback declaration uses the same dotted key.
    DuplicateWriteback(String),
    /// A module dependency refers to no declared module.
    UnknownModuleDependency { module: String, dependency: String },
    /// A resource dependency refers to no declared logical resource.
    UnknownResourceDependency {
        module: String,
        resource: String,
        dependency_module: String,
        dependency_resource: String,
    },
    /// The binding's state-bootstrap module is absent from the completed graph.
    MissingBootstrap(String),
    /// Module dependencies contain a cycle.
    ModuleCycle(Vec<String>),
    /// A workload names no selected platform service.
    UnknownService(String),
    /// A workload names no selected provider delivery.
    UnknownDelivery(String),
}

impl fmt::Display for GraphFinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateModule(name) => write!(f, "duplicate module `{name}`"),
            Self::DuplicateResource { module, resource } => {
                write!(f, "duplicate resource `{module}/{resource}`")
            }
            Self::DuplicateWriteback(key) => write!(f, "duplicate writeback key `{key}`"),
            Self::UnknownModuleDependency { module, dependency } => {
                write!(
                    f,
                    "module `{module}` depends on unknown module `{dependency}`"
                )
            }
            Self::UnknownResourceDependency {
                module,
                resource,
                dependency_module,
                dependency_resource,
            } => write!(
                f,
                "resource `{module}/{resource}` depends on unknown resource \
                 `{dependency_module}/{dependency_resource}`"
            ),
            Self::MissingBootstrap(module) => {
                write!(f, "required bootstrap module `{module}` is not declared")
            }
            Self::ModuleCycle(members) => {
                write!(
                    f,
                    "module dependency cycle involving {}",
                    members.join(", ")
                )
            }
            Self::UnknownService(service) => write!(f, "unknown platform service `{service}`"),
            Self::UnknownDelivery(delivery) => write!(f, "unknown provider delivery `{delivery}`"),
        }
    }
}

/// Failure to mutate or complete a deployment graph.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GraphError {
    /// A handle belongs to another deployment graph.
    #[error("the {kind} handle belongs to another deployment graph")]
    ForeignHandle { kind: &'static str },
    /// The graph owning a handle no longer exists.
    #[error("the {kind} handle has expired")]
    ExpiredHandle { kind: &'static str },
    /// A copied take-once provider-kind handle has already been installed.
    #[error("the provider-kind handle has already been consumed")]
    ConsumedKind,
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
    /// Graph completion found one or more structural failures.
    #[error("deployment graph is invalid: {0:?}")]
    Invalid(Vec<GraphFinding>),
}

/// Failure in generic authoring dispatch.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("authoring contract error: {message}")]
pub struct AuthorError {
    /// Actionable dispatch, receiver, or graph detail.
    pub message: String,
    /// Most specific frontend range supplied by the failed argument.
    pub range: Option<SourceRange>,
}

impl AuthorError {
    /// Construct an unlocated authoring error.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            range: None,
        }
    }

    /// Attach a range unless a lower layer supplied one.
    pub fn at(mut self, range: Option<SourceRange>) -> Self {
        if self.range.is_none() {
            self.range = range;
        }
        self
    }
}

impl From<GraphError> for AuthorError {
    fn from(value: GraphError) -> Self {
        Self::new(value.to_string())
    }
}

/// Failure to assemble one immutable platform binding.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid platform binding: {message}")]
pub struct BindingError {
    /// The duplicate, missing, or inconsistent identity.
    pub message: String,
}

/// Failure before or during one format-neutral definition evaluation.
#[derive(Debug, Error)]
pub enum DefinitionError {
    /// Recorded/requested format differs from the statically selected frontend.
    #[error(
        "definition format `{source_format}` does not match selected frontend `{frontend_format}`"
    )]
    FormatMismatch {
        /// Independently recorded source format.
        source_format: DefinitionFormatId,
        /// Statically selected frontend format.
        frontend_format: DefinitionFormatId,
    },
    /// The selected frontend rejected parsing, checking, or evaluation.
    #[error(transparent)]
    Frontend(#[from] FrontendDiagnostic),
    /// Typed platform configuration admission failed with selected source identity.
    #[error("{format} {source_name}: {error}")]
    Config {
        /// Selected definition format.
        format: DefinitionFormatId,
        /// Deployment-relative or explicit authoring source identity.
        source_name: DefinitionSourceName,
        /// Located typed admission failure.
        error: ConfigError,
    },
    /// Deployment graph completion failed with selected source identity.
    #[error("{format} {source_name}: {error}")]
    Graph {
        /// Selected definition format.
        format: DefinitionFormatId,
        /// Deployment-relative or explicit authoring source identity.
        source_name: DefinitionSourceName,
        /// Structural graph failure.
        error: Box<GraphError>,
    },
}

/// One pure definition-verification finding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerificationFinding {
    /// A realized provider resource declares a dependency absent from the complete set.
    MissingDependency {
        /// Resource declaring the edge.
        resource: String,
        /// Missing physical resource identity.
        dependency: String,
    },
    /// A realized resource does not truthfully perform live description.
    CannotDescribe {
        /// Physical resource identity.
        resource: String,
        /// Selected provider kind.
        provider_kind: String,
    },
    /// Pure provider-kind realization rejected its typed desired input or placement.
    CannotRealize {
        /// Logical `module/resource` identity.
        resource: String,
        /// Selected provider kind.
        provider_kind: String,
        /// Provider-owned pure failure detail.
        message: String,
    },
}

impl fmt::Display for VerificationFinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingDependency {
                resource,
                dependency,
            } => write!(f, "resource `{resource}` depends on missing `{dependency}`"),
            Self::CannotDescribe {
                resource,
                provider_kind,
            } => write!(
                f,
                "resource `{resource}` from kind `{provider_kind}` cannot describe live state"
            ),
            Self::CannotRealize {
                resource,
                provider_kind,
                message,
            } => write!(
                f,
                "resource `{resource}` from kind `{provider_kind}` cannot be realized: {message}"
            ),
        }
    }
}

/// Complete deterministic report returned by pure definition verification.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("definition verification failed with {} finding(s)", .findings.len())]
pub struct VerificationReport {
    /// Findings in resource declaration order and dependency order.
    pub findings: Vec<VerificationFinding>,
}

/// Failure to compute a shared module selection before engine execution.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SelectionError {
    /// An explicitly supplied selector contained no module names.
    #[error("module selection cannot be empty; supported modules: {supported:?}")]
    Empty {
        /// Complete module inventory in definition order.
        supported: Vec<String>,
    },
    /// One or more requested module names are not declared.
    #[error("unknown modules {unknown:?}; supported modules: {supported:?}")]
    Unknown {
        /// Unknown names in request order after deduplication.
        unknown: Vec<String>,
        /// Complete module inventory in definition order.
        supported: Vec<String>,
    },
}

/// Failure while projecting a verified logical graph into provider resources.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("cannot project `{resource}` from kind `{provider_kind}`: {message}")]
pub struct ProjectionError {
    /// Logical `module/resource` identity.
    pub resource: String,
    /// Selected canonical provider kind.
    pub provider_kind: String,
    /// Pure realization failure detail.
    pub message: String,
}

/// Failure to validate or project platform-owned content through a provider delivery.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("provider delivery error: {message}")]
pub struct DeliveryError {
    /// Provider-owned validation, canonicalization, or projection detail.
    pub message: String,
}

/// Failure while preparing or invoking one provider-owned runtime capability.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("provider execution `{provider}` failed: {message}")]
pub struct ProviderExecutionError {
    /// Stable provider registration identity.
    pub provider: String,
    /// Provider-owned actionable failure detail.
    pub message: String,
}

/// Failure while resolving or publishing one platform-owned operational artifact.
#[derive(Debug, Error)]
pub enum ArtifactError {
    /// An authoritative deployment-local content source could not be read.
    #[error("cannot read source `{path}` for operational artifact `{artifact}`: {source}")]
    SourceRead {
        /// Logical artifact identity.
        artifact: String,
        /// Deployment-local source path.
        path: std::path::PathBuf,
        /// Host filesystem failure.
        #[source]
        source: std::io::Error,
    },
    /// A provider runtime registration needed for publication failed.
    #[error(transparent)]
    ProviderExecution(#[from] ProviderExecutionError),
    /// Recorded infrastructure state needed by provider publication could not be loaded.
    #[error("cannot load infrastructure state before operational artifact publication: {0}")]
    State(#[from] tokeira_state::StateError),
    /// Selected provider delivery rejected publication.
    #[error("cannot publish operational artifact `{artifact}`: {source}")]
    Delivery {
        /// Logical artifact identity.
        artifact: String,
        /// Provider-owned publication failure.
        #[source]
        source: DeliveryError,
    },
    /// Provider publication evidence disagreed with the admitted declaration.
    #[error(
        "provider returned an invalid receipt for operational artifact `{artifact}`: {message}"
    )]
    InvalidReceipt {
        /// Logical artifact identity.
        artifact: String,
        /// Exact receipt invariant that failed.
        message: String,
    },
}

impl ProviderExecutionError {
    /// Construct a provider-owned runtime failure without rewriting its detail.
    pub fn new(provider: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            message: message.into(),
        }
    }
}

/// Failure to bind a verified definition to the generic engine adapter.
#[derive(Debug, Error)]
pub enum FrameworkError {
    /// The completed definition omitted the binding's required state-bootstrap module.
    #[error("verified definition omits required bootstrap module `{0}`")]
    MissingBootstrap(String),
    /// A workload could not be projected by its selected provider delivery.
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
    /// Platform-owned content could not be resolved for deterministic projection.
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    /// More than one provider offered the single deploy-engine executor.
    #[error("multiple provider executions offer a deploy-engine platform: {0}")]
    AmbiguousDeployPlatform(String),
}

impl DeliveryError {
    /// Construct a provider delivery failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl BindingError {
    /// Construct a binding validation failure.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
