//! Operator-facing diagnostics for `.tkdp` preflight and lowering.
//!
//! Every diagnostic points at a range in the *original* source; nothing in
//! this module knows about the generated program. Rendering follows the
//! `file:line:col: error[CODE]: message` shape with a source excerpt and
//! caret underline, so definitions fail with the same texture as `rustc`
//! output rather than a bare panic string.

use ruff_text_size::TextRange;

use crate::source_map::LineTable;

/// A single preflight/lowering finding against the original source.
#[derive(Debug, Clone)]
pub struct Diagnostic {
    /// Stable code (`TKDP0xx`) so messages can be tightened without breaking
    /// anything that filters on kind.
    pub code: &'static str,
    pub message: String,
    pub range: TextRange,
}

impl Diagnostic {
    pub fn new(code: &'static str, message: impl Into<String>, range: TextRange) -> Self {
        Self {
            code,
            message: message.into(),
            range,
        }
    }
}

/// Renders diagnostics with source context, one block per finding.
pub fn render(file: &str, source: &str, diagnostics: &[Diagnostic]) -> String {
    let table = LineTable::new(source);
    let mut out = String::new();
    for d in diagnostics {
        let (line, col) = table.line_column(d.range.start());
        out.push_str(&format!(
            "{file}:{line}:{col}: error[{}]: {}\n",
            d.code, d.message
        ));
        let line_start = table.line_start(line);
        let line_text = source[usize::from(line_start)..]
            .lines()
            .next()
            .unwrap_or("");
        out.push_str(&format!("  {line} | {line_text}\n"));
        // Caret width covers the finding but stays on one line: multi-line
        // ranges underline only their first line.
        let start_in_line = (col - 1) as usize;
        let width = usize::from(d.range.len())
            .clamp(1, line_text.len().saturating_sub(start_in_line).max(1));
        let gutter = " ".repeat(line.to_string().len() + 4 + start_in_line);
        out.push_str(&format!("{gutter}{}\n", "^".repeat(width)));
    }
    out
}
