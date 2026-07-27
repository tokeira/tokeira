//! The operator output contract for `tkr` and `tkp`.
//!
//! Every operator-facing report is **data first; prose is a rendering**: a verb
//! produces a structured result model, then renders it through this crate so
//! the two binaries read as one product. The contract itself (axes, collapse
//! rule, copy rules, depth placement) is documented in
//! `docs/platforms/operator-output-contract.md`; this crate is its shared
//! vocabulary and render seam.
//!
//! Invariants owned here:
//! - the **collapse rule**: structured output is always the complete model —
//!   [`Mode::resolve`] discards depth under `--json`, so a script can never
//!   break because a human added `--detail`;
//! - the fleet-wide **symbol vocabulary** for plans and deltas ([`symbol`]);
//! - the render seam ([`Report`] / [`render`]) that keeps narrative assembly
//!   out of verb logic.

use serde::Serialize;

/// How much of the model the narrative shows. A human affordance only —
/// structured output ignores it (the collapse rule).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Depth {
    /// The outcome and anything demanding operator attention. One screen.
    /// States the answer.
    #[default]
    Summary,
    /// Summary plus the evidence — per-resource lines, field diffs, digests,
    /// provenance, paths. Substantiates the answer.
    Detail,
}

/// Who is reading: human narrative, or the complete model as JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Form {
    /// Human prose under the depth contract.
    #[default]
    Narrative,
    /// The complete result model, verbatim. Depth-blind by contract.
    Json,
}

/// A resolved output mode — the pair every rendering verb consumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Mode {
    pub depth: Depth,
    pub form: Form,
}

impl Mode {
    /// Resolve a mode from the two global flags (`--json`, `--detail`).
    ///
    /// Enforces the collapse rule at the boundary: under `--json` the depth is
    /// normalized to `Summary` so no renderer can accidentally branch on a
    /// depth the operator believes has no effect.
    pub fn resolve(json: bool, detail: bool) -> Self {
        if json {
            Self {
                depth: Depth::Summary,
                form: Form::Json,
            }
        } else {
            Self {
                depth: if detail {
                    Depth::Detail
                } else {
                    Depth::Summary
                },
                form: Form::Narrative,
            }
        }
    }
}

/// Pluralization is computed, never hedged: `counted(1, "change")` renders
/// `1 change`, `counted(6, "change")` renders `6 changes`. `(s)` never
/// appears in a report — the count is always known by the time it prints.
/// (Simple `-s` plurals only; a noun that inflects differently gets its own
/// copy at the call site.)
pub fn counted(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// The fleet-wide symbol vocabulary for anything resembling a plan or delta.
/// One meaning per glyph, in both binaries — an operator learns it once.
pub mod symbol {
    /// A resource that will be (or was) created.
    pub const CREATE: &str = "+";
    /// A resource that will be (or was) updated in place.
    pub const UPDATE: &str = "~";
    /// Delete-then-recreate — destructive, called out as such.
    pub const REPLACE: &str = "±";
    /// A resource that will be (or was) deleted.
    pub const DELETE: &str = "-";
    /// A resource the operation leaves untouched (detail-depth evidence).
    pub const UNCHANGED: &str = "=";
    /// Something the engine could not determine — an uncertainty line.
    pub const UNCERTAIN: &str = "?";
}

/// A renderable operator report: a serializable model plus its narrative.
///
/// The `Serialize` bound is the contract's teeth — a report that cannot be
/// emitted as JSON is a defect in the report, not a formatting choice.
pub trait Report: Serialize {
    /// Write the human narrative for this model at the given depth into `out`.
    ///
    /// Implementations append complete lines (each ending `\n`) and follow the
    /// copy rules in `docs/platforms/operator-output-contract.md`.
    fn narrative(&self, depth: Depth, out: &mut String);
}

/// Rendering failures — only the structured form can fail, and only if the
/// model does not serialize (a programming error surfaced explicitly rather
/// than swallowed).
#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("report model failed to serialize: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// Render a report for the resolved mode: the complete model as pretty JSON,
/// or the narrative at the requested depth. Returns the text; the binary owns
/// where it goes (stdout for reports — advisories belong on stderr).
pub fn render<R: Report>(report: &R, mode: Mode) -> Result<String, RenderError> {
    match mode.form {
        Form::Json => Ok(serde_json::to_string_pretty(report)?),
        Form::Narrative => {
            let mut out = String::new();
            report.narrative(mode.depth, &mut out);
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct Probe {
        answer: u32,
        evidence: &'static str,
    }

    impl Report for Probe {
        fn narrative(&self, depth: Depth, out: &mut String) {
            out.push_str(&format!("answer: {}\n", self.answer));
            if depth == Depth::Detail {
                out.push_str(&format!("  evidence: {}\n", self.evidence));
            }
        }
    }

    // The collapse rule: --json wins over --detail, and the depth it carries
    // is normalized so renderers cannot branch on it.
    #[test]
    fn json_collapses_depth() {
        let mode = Mode::resolve(true, true);
        assert_eq!(mode.form, Form::Json);
        assert_eq!(mode.depth, Depth::Summary);
    }

    #[test]
    fn structured_form_carries_the_complete_model_regardless_of_flags() {
        let probe = Probe {
            answer: 7,
            evidence: "sha256:41c2",
        };
        let with_detail = render(&probe, Mode::resolve(true, true)).unwrap();
        let without = render(&probe, Mode::resolve(true, false)).unwrap();
        assert_eq!(with_detail, without, "JSON is depth-blind");
        assert!(with_detail.contains("sha256:41c2"), "the model is complete");
    }

    #[test]
    fn narrative_depth_gates_evidence_not_the_answer() {
        let probe = Probe {
            answer: 7,
            evidence: "sha256:41c2",
        };
        let summary = render(&probe, Mode::resolve(false, false)).unwrap();
        let detail = render(&probe, Mode::resolve(false, true)).unwrap();
        assert!(summary.contains("answer: 7") && !summary.contains("sha256"));
        assert!(detail.contains("answer: 7") && detail.contains("sha256:41c2"));
    }
}
