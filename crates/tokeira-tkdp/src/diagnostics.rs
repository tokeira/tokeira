//! Operator-facing rendering of preflight findings.
//!
//! Every finding points at a range in the operator's source; rendering
//! follows the `label:line:col: error[CODE]: message` shape with a source
//! excerpt and caret underline, so a rejected definition fails with the same
//! texture as `rustc` output. The rendered block becomes the
//! `FrontendDiagnostic` message — presentation of the *diagnostic* stays with
//! the shell, but the message text is the frontend's to compose.

use crate::{preflight::Finding, source_map::LineTable};

/// Renders all findings of one preflight pass, one block per finding.
pub fn render(label: &str, source: &str, findings: &[Finding]) -> String {
    let table = LineTable::new(source);
    let mut out = format!(
        "definition rejected with {} finding{}:",
        findings.len(),
        if findings.len() == 1 { "" } else { "s" }
    );
    for finding in findings {
        let (line, col) = table.line_column(finding.range.start());
        out.push_str(&format!(
            "\n{label}:{line}:{col}: error[{}]: {}",
            finding.code, finding.message
        ));
        let line_start = table.line_start(line);
        let line_text = source[usize::from(line_start)..]
            .lines()
            .next()
            .unwrap_or("");
        out.push_str(&format!("\n  {line} | {line_text}"));
        // Caret width covers the finding but stays on one line: multi-line
        // ranges underline only their first line.
        let start_in_line = (col - 1) as usize;
        let width = usize::from(finding.range.len())
            .clamp(1, line_text.len().saturating_sub(start_in_line).max(1));
        let gutter = " ".repeat(line.to_string().len() + 4 + start_in_line);
        out.push_str(&format!("\n{gutter}{}", "^".repeat(width)));
    }
    out
}
