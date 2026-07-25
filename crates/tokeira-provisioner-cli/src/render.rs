//! Operator-facing rendering for binding verdicts and plans.
//!
//! Reports here follow the operator output contract
//! (`docs/platforms/operator-output-contract.md`): each is a serializable
//! model rendered through `tokeira-report` — summary states the answer,
//! `--detail` adds the evidence, `--json` emits the complete model. The house
//! value behind the copy (§Values 3, operator empathy): every report says
//! what happened, why, and what to do next — in operator language, not ours.
//! "Unknown — apply would REFUSE" is a true statement about internals and a
//! useless one to a person staring at a fresh deployment; "not initialized —
//! `apply` stamps it on first run" is the same fact, usable.

use tokeira_iac::{Change, ChangeKind};
use tokeira_provisioner::{BindingVerdict, DeploymentStateEnvelope};
use tokeira_report::{Depth, Report, symbol};

/// The read-only plan report: which plane, standing on which binding, with
/// what changes. One model serves `infra plan` and `deploy plan`.
#[derive(Debug, serde::Serialize)]
pub(crate) struct PlanReport {
    /// The platform label (e.g. `compose-syn`).
    pub platform: String,
    /// The plane the plan covers: `"infra plan"` or `"deploy plan"`.
    pub plane: &'static str,
    /// Whether the deployment carries a Day-0 binding stamp yet.
    pub initialized: bool,
    pub binding: BindingVerdict,
    pub changes: Vec<Change>,
}

impl PlanReport {
    pub(crate) fn new(
        platform: String,
        plane: &'static str,
        envelope: &DeploymentStateEnvelope,
        binding: BindingVerdict,
        changes: Vec<Change>,
    ) -> Self {
        Self {
            platform,
            plane,
            initialized: envelope.binding.is_some(),
            binding,
            changes,
        }
    }
}

impl Report for PlanReport {
    fn narrative(&self, depth: Depth, out: &mut String) {
        out.push_str(&format!("platform: {}\n", self.platform));
        // Verdict narration is attention-only: a verdict that lets the apply
        // proceed is a standing fact (describe's story), not news on every
        // plan. Only what blocks or qualifies the apply earns a line; the
        // JSON model always carries the verdict.
        if let Some(line) = binding_attention(self.initialized, self.binding) {
            out.push_str(&format!("binding:  {line}\n"));
        }
        plan_narrative(self.plane, &self.changes, depth, out);
    }
}

/// The attention-worthy binding line for a **read-only** report: `Some` only
/// when the verdict would block the apply, or the deployment is fresh (the
/// first apply does more than the plan shows — the Day-0 stamp). Proceeding
/// verdicts return `None`.
fn binding_attention(initialized: bool, verdict: BindingVerdict) -> Option<&'static str> {
    if !initialized {
        return Some("not initialized — `apply` stamps this deployment on first run");
    }
    match verdict {
        BindingVerdict::Match | BindingVerdict::DevIterate => None,
        BindingVerdict::Mismatch => Some(
            "MISMATCH — the running provisioner is not this deployment's recorded engine; \
             apply refuses (run the recorded binary, or `upgrade` to advance)",
        ),
        BindingVerdict::Downgrade => Some(
            "DOWNGRADE — the running provisioner is older than the recorded engine; \
             apply refuses (run the recorded binary, or `rollback` to re-pin)",
        ),
        BindingVerdict::ModeRegression => Some(
            "MODE REGRESSION — a dev build cannot operate a versioned deployment; \
             apply refuses (use the released binary)",
        ),
        // Unreachable today (Unknown ⇔ no recorded binding) — kept exhaustive
        // so a future verdict cannot fall through silently.
        BindingVerdict::Unknown => Some("unknown — apply refuses"),
    }
}

/// Write a plan in operator form: a categorized summary line, then one aligned
/// line per change. Depth gates the evidence, never the answer: summary shows
/// the acting resources; detail adds field-level diffs and the unchanged
/// listing.
fn plan_narrative(heading: &str, changes: &[Change], depth: Depth, out: &mut String) {
    let count = |kind: ChangeKind| changes.iter().filter(|c| c.kind == kind).count();
    let (creates, updates, replaces, deletes) = (
        count(ChangeKind::Create),
        count(ChangeKind::Update),
        count(ChangeKind::Replace),
        count(ChangeKind::Delete),
    );
    let unchanged = count(ChangeKind::NoChange);

    let mut parts = Vec::new();
    if creates > 0 {
        parts.push(format!("{creates} to create"));
    }
    if updates > 0 {
        parts.push(format!("{updates} to update"));
    }
    if replaces > 0 {
        parts.push(format!("{replaces} to replace (destructive)"));
    }
    if deletes > 0 {
        parts.push(format!("{deletes} to delete (destructive)"));
    }
    if parts.is_empty() {
        out.push_str(&format!(
            "{heading}: no changes — everything matches the definition\n"
        ));
    } else {
        let unchanged_note = if unchanged > 0 {
            format!(" ({unchanged} unchanged)")
        } else {
            String::new()
        };
        out.push_str(&format!(
            "{heading}: {}{unchanged_note}\n",
            parts.join(", ")
        ));
    }

    // Acting lines first, unchanged last — the eye lands on what will happen.
    let width = changes
        .iter()
        .map(|c| c.module.len() + c.resource.len() + 1)
        .max()
        .unwrap_or(0);
    let mut ordered: Vec<&Change> = changes.iter().collect();
    ordered.sort_by_key(|c| matches!(c.kind, ChangeKind::NoChange));
    for change in ordered {
        let glyph = match change.kind {
            ChangeKind::Create => symbol::CREATE,
            ChangeKind::Update => symbol::UPDATE,
            ChangeKind::Replace => symbol::REPLACE,
            ChangeKind::Delete => symbol::DELETE,
            // The unchanged listing is detail-depth evidence; the summary
            // already carries its count.
            ChangeKind::NoChange if depth == Depth::Detail => symbol::UNCHANGED,
            ChangeKind::NoChange => continue,
        };
        out.push_str(&format!(
            "  {glyph} {:<width$}  ({})\n",
            format!("{}::{}", change.module, change.resource),
            change.resource_type,
        ));
        // Updates and replaces say WHY — the field-level evidence, values
        // truncated so an environment map cannot flood the report.
        if depth == Depth::Detail && matches!(change.kind, ChangeKind::Update | ChangeKind::Replace)
        {
            for diff in &change.details {
                // A valueless diff is a named observation ("tags changed") —
                // render the name alone, never `(none) → (none)`.
                if diff.before.is_none() && diff.after.is_none() {
                    out.push_str(&format!("      {}\n", diff.field));
                } else {
                    out.push_str(&format!(
                        "      {}: {} → {}\n",
                        diff.field,
                        truncate(diff.before.as_deref().unwrap_or("(none)")),
                        truncate(diff.after.as_deref().unwrap_or("(none)")),
                    ));
                }
            }
        }
    }
}

/// Print what an apply actually committed — one line per resource, the
/// audit entries as the operator report (`+` created, `~` updated, `-`
/// deleted). An apply that hides its work behind a count blinds the
/// operator during the highest-stakes verb.
pub(crate) fn print_applied(entries: &[tokeira_provisioner::ChangeLogEntry]) {
    use tokeira_provisioner::ChangeOp;
    for entry in entries {
        let glyph = match entry.op {
            ChangeOp::Created => symbol::CREATE,
            ChangeOp::Updated => symbol::UPDATE,
            ChangeOp::Deleted => symbol::DELETE,
        };
        println!("  {glyph} {}", entry.id);
    }
}

/// Clamp a diff value for the one-line field report.
fn truncate(value: &str) -> String {
    const MAX: usize = 72;
    if value.len() <= MAX {
        return value.to_string();
    }
    let mut end = MAX;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_report::{Mode, render};

    fn change(kind: ChangeKind) -> Change {
        Change {
            kind,
            resource_type: "t".into(),
            module: "m".into(),
            resource: "r".into(),
            details: vec![tokeira_iac::FieldDiff {
                field: "image".into(),
                before: Some("a".into()),
                after: Some("b".into()),
            }],
        }
    }

    fn report(changes: Vec<Change>) -> PlanReport {
        PlanReport {
            platform: "test".into(),
            plane: "infra plan",
            initialized: true,
            binding: BindingVerdict::DevIterate,
            changes,
        }
    }

    // Depth gates the evidence, never the answer: the summary names the acting
    // resource but withholds field diffs and the unchanged listing; detail
    // shows both. Counts never inflate — unchanged is not a "change".
    #[test]
    fn depth_gates_evidence_not_the_answer() {
        let r = report(vec![
            change(ChangeKind::Update),
            change(ChangeKind::NoChange),
        ]);
        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(summary.contains("1 to update (1 unchanged)"));
        assert!(summary.contains("~ m::r"));
        assert!(!summary.contains("image:") && !summary.contains("= m::r"));

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(detail.contains("image: a → b"));
        assert!(detail.contains("= m::r"));
    }

    // The collapse rule end-to-end: JSON carries the complete model (field
    // diffs included) whatever the depth flags said.
    #[test]
    fn json_is_the_complete_model() {
        let r = report(vec![change(ChangeKind::Update)]);
        let json = render(&r, Mode::resolve(true, true)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["binding"], "dev-iterate");
        assert_eq!(value["changes"][0]["details"][0]["field"], "image");
    }

    // Verdict narration is attention-only: proceeding verdicts are silent
    // (describe's story); blocking verdicts and the fresh-deployment case
    // speak. The JSON model carries the verdict either way.
    #[test]
    fn binding_narration_is_attention_only() {
        let proceeding = report(Vec::new());
        let text = render(&proceeding, Mode::resolve(false, true)).unwrap();
        assert!(!text.contains("binding:"), "silent on proceed: {text}");

        let blocked = PlanReport {
            binding: BindingVerdict::Mismatch,
            ..report(Vec::new())
        };
        let text = render(&blocked, Mode::resolve(false, false)).unwrap();
        assert!(text.contains("MISMATCH"), "blocking verdicts speak: {text}");

        let fresh = PlanReport {
            initialized: false,
            ..report(Vec::new())
        };
        let text = render(&fresh, Mode::resolve(false, false)).unwrap();
        assert!(
            text.contains("not initialized"),
            "fresh case speaks: {text}"
        );

        let json = render(&proceeding, Mode::resolve(true, false)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            value["binding"], "dev-iterate",
            "the model always carries it"
        );
    }

    #[test]
    fn a_quiet_plan_says_so() {
        let r = report(Vec::new());
        let text = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(text.contains("no changes — everything matches the definition"));
    }
}
