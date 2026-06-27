//! Compiler diagnostics.
//!
//! Every phase (lex, parse, resolve, type-check, validate, lower) reports
//! failures as [`Diag`] values carrying a source [`Span`], a message, and an
//! optional remediation hint (Requirement 6.1). A future step wires `ariadne`
//! for rich rendering; the data model here is the stable contract, and it
//! deliberately holds **no resolved `RuntimeContext` value** so secrets can
//! never be echoed through a diagnostic (Requirement 12.3 / Property 16).

use crate::token::Span;

/// Whether a [`Diag`] blocks compilation or is advisory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// Prevents lowering; the operation aborts (no partial composition escapes,
    /// Property 6).
    Error,
    /// Advisory; does not prevent lowering.
    Warning,
}

/// A single located compiler message.
///
/// Constructed by any phase and rendered for the operator. The `span` indexes
/// the original source; `message` states the problem; `hint` (when present)
/// states the fix. Never include a resolved secret value in any field.
#[derive(Debug, Clone)]
pub struct Diag {
    /// Source range the diagnostic points at.
    pub span: Span,
    /// Whether this blocks compilation.
    pub severity: Severity,
    /// Human-readable description of the problem.
    pub message: String,
    /// Optional remediation guidance.
    pub hint: Option<String>,
}

impl Diag {
    /// Build a blocking error at `span`.
    pub fn error(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            severity: Severity::Error,
            message: message.into(),
            hint: None,
        }
    }

    /// Build an advisory warning at `span`.
    pub fn warning(span: Span, message: impl Into<String>) -> Self {
        Self {
            span,
            severity: Severity::Warning,
            message: message.into(),
            hint: None,
        }
    }

    /// Attach a remediation hint, consuming and returning `self` for chaining.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Whether this diagnostic blocks compilation.
    pub fn is_error(&self) -> bool {
        matches!(self.severity, Severity::Error)
    }
}
