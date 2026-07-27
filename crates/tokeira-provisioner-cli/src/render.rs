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

use tokeira_explain::{DeploymentExplanation, ExplainedChange, Uncertainty};
use tokeira_iac::{ChangeKind, RefreshStatus};
use tokeira_provisioner::BindingVerdict;
use tokeira_report::{Depth, Report, symbol};

/// The read-only plan report: the deployment explanation, framed by the
/// verb-level annotations (platform line, attention-only binding).
///
/// `--json` emits the **explanation model alone** (evidence-model Req 6.3):
/// the manual `Serialize` delegates to it, so the artifact and the `--json`
/// output are one schema. The binding verdict is verb framing, not model
/// content — `describe` owns it.
#[derive(Debug)]
pub(crate) struct ExplanationReport {
    /// Whether the deployment carries a Day-0 binding stamp yet.
    pub initialized: bool,
    pub binding: BindingVerdict,
    pub explanation: DeploymentExplanation,
}

impl serde::Serialize for ExplanationReport {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.explanation.serialize(serializer)
    }
}

impl Report for ExplanationReport {
    fn narrative(&self, depth: Depth, out: &mut String) {
        out.push_str(&format!("platform: {}\n", self.explanation.platform));
        // Verdict narration is attention-only: a verdict that lets the apply
        // proceed is a standing fact (describe's story), not news on every
        // plan. Only what blocks or qualifies the apply earns a line.
        if let Some(line) = binding_attention(self.initialized, self.binding) {
            out.push_str(&format!("binding:  {line}\n"));
        }
        plan_narrative(
            &self.explanation.operation,
            &self.explanation.changes,
            depth,
            out,
        );
        uncertainty_narrative(&self.explanation, depth, out);
    }
}

/// The uncertainty section (evidence-model Req 4.5/4.6): empty uncertainties
/// mean live state was fully confirmed — stated, not implied by silence; a
/// non-empty set states its count at summary and each member at detail.
fn uncertainty_narrative(explanation: &DeploymentExplanation, depth: Depth, out: &mut String) {
    if explanation.uncertainties.is_empty() {
        out.push_str("live state: fully confirmed\n");
        return;
    }
    out.push_str(&format!(
        "{} — `--detail` explains each\n",
        tokeira_report::counted(explanation.uncertainties.len(), "uncertainty"),
    ));
    if depth == Depth::Detail {
        for uncertainty in &explanation.uncertainties {
            out.push_str(&format!(
                "  {} {} — {}\n",
                symbol::UNCERTAIN,
                uncertainty_subject(explanation, uncertainty),
                uncertainty.consequence,
            ));
            if let Some(resolve) = &uncertainty.resolvable_by {
                out.push_str(&format!("    resolve: {resolve}\n"));
            }
        }
    }
}

/// Display name for an uncertainty's subject: the resource it qualifies, or
/// the operation itself for plan-level uncertainties.
fn uncertainty_subject(explanation: &DeploymentExplanation, uncertainty: &Uncertainty) -> String {
    explanation
        .changes
        .iter()
        .find(|change| change.evidence_id == uncertainty.subject)
        .map(|change| change.resource_id.clone())
        .unwrap_or_else(|| "this operation".to_string())
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
            "MISMATCH — the running provisioner is not the one this deployment recorded; \
             apply refuses (run the recorded provisioner, or `upgrade` to advance)",
        ),
        BindingVerdict::Downgrade => Some(
            "DOWNGRADE — the running provisioner is older than the one this deployment \
             recorded; apply refuses (run the recorded provisioner, or `rollback` to re-pin)",
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
fn plan_narrative(heading: &str, changes: &[ExplainedChange], depth: Depth, out: &mut String) {
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
        .map(|c| c.module.len() + c.resource_id.len() + 1)
        .max()
        .unwrap_or(0);
    let mut ordered: Vec<&ExplainedChange> = changes.iter().collect();
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
            format!("{}::{}", change.module, change.resource_id),
            change.resource_type,
        ));
        // An unconfirmable live read is detail-tier evidence on the change
        // itself; the plan-level uncertainty section carries the consequence.
        if depth == Depth::Detail && matches!(change.refresh_status, Some(RefreshStatus::Unknown)) {
            out.push_str("      live state: unconfirmed\n");
        }
        // Updates and replaces say WHY — the field-level evidence, values
        // truncated so an environment map cannot flood the report.
        if depth == Depth::Detail && matches!(change.kind, ChangeKind::Update | ChangeKind::Replace)
        {
            for diff in &change.field_diffs {
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
    use tokeira_explain::{DeploymentContext, explain_plan};
    use tokeira_iac::{Change, PlanOutcome, RefreshCoverage, ResourceId};
    use tokeira_report::{Mode, render};

    fn change(kind: ChangeKind, resource: &str) -> Change {
        Change {
            kind,
            resource_type: "t".into(),
            module: "m".into(),
            resource: resource.into(),
            details: vec![tokeira_iac::FieldDiff {
                field: "image".into(),
                before: Some("a".into()),
                after: Some("b".into()),
            }],
        }
    }

    fn context() -> DeploymentContext {
        DeploymentContext {
            deployment: "test-deployment".into(),
            platform: "test".into(),
            operation: "infra plan".into(),
            current_revision: 1,
            proposed_revision: None,
            definition_ref: None,
        }
    }

    fn report_for(
        outcome: &PlanOutcome,
        binding: BindingVerdict,
        initialized: bool,
    ) -> ExplanationReport {
        ExplanationReport {
            initialized,
            binding,
            explanation: explain_plan(context(), outcome),
        }
    }

    fn examined(changes: Vec<Change>) -> PlanOutcome {
        // Every planned resource confirmed live — the fully-confirmed case.
        let status_by_id = changes
            .iter()
            .map(|c| (ResourceId(c.resource.clone()), RefreshStatus::DesiredLive))
            .collect();
        PlanOutcome {
            changes,
            refresh: RefreshCoverage {
                status_by_id,
                examined: true,
            },
        }
    }

    // Depth gates the evidence, never the answer: the summary names the acting
    // resource but withholds field diffs and the unchanged listing; detail
    // shows both. Counts never inflate — unchanged is not a "change".
    #[test]
    fn depth_gates_evidence_not_the_answer() {
        let outcome = examined(vec![
            change(ChangeKind::Update, "r"),
            change(ChangeKind::NoChange, "r2"),
        ]);
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);
        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(summary.contains("1 to update (1 unchanged)"));
        assert!(summary.contains("~ m::r"));
        assert!(!summary.contains("image:") && !summary.contains("= m::r2"));

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(detail.contains("image: a → b"));
        assert!(detail.contains("= m::r2"));
    }

    // The collapse rule end-to-end: JSON is the explanation model alone —
    // schema-versioned, evidence-indexed — whatever the depth flags said.
    #[test]
    fn json_is_the_explanation_model() {
        let outcome = examined(vec![change(ChangeKind::Update, "r")]);
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);
        let json = render(&r, Mode::resolve(true, true)).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["changes"][0]["field_diffs"][0]["field"], "image");
        assert!(
            value.get("binding").is_none(),
            "binding is verb framing, not model content"
        );
    }

    // Verdict narration is attention-only: proceeding verdicts are silent
    // (describe's story); blocking verdicts and the fresh-deployment case
    // speak.
    #[test]
    fn binding_narration_is_attention_only() {
        let outcome = examined(Vec::new());
        let proceeding = report_for(&outcome, BindingVerdict::DevIterate, true);
        let text = render(&proceeding, Mode::resolve(false, true)).unwrap();
        assert!(!text.contains("binding:"), "silent on proceed: {text}");

        let blocked = report_for(&outcome, BindingVerdict::Mismatch, true);
        let text = render(&blocked, Mode::resolve(false, false)).unwrap();
        assert!(text.contains("MISMATCH"), "blocking verdicts speak: {text}");

        let fresh = report_for(&outcome, BindingVerdict::DevIterate, false);
        let text = render(&fresh, Mode::resolve(false, false)).unwrap();
        assert!(
            text.contains("not initialized"),
            "fresh case speaks: {text}"
        );
    }

    // Requirement 4.5: no uncertainties is a statement, not a silence.
    #[test]
    fn a_quiet_plan_states_full_confirmation() {
        let outcome = examined(Vec::new());
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);
        let text = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(text.contains("no changes — everything matches the definition"));
        assert!(text.contains("live state: fully confirmed"));
    }

    // Requirement 4.6: summary reveals uncertainty's presence by count;
    // detail lists each with its consequence and resolution.
    #[test]
    fn uncertainty_presence_at_summary_members_at_detail() {
        let mut outcome = examined(vec![change(ChangeKind::Update, "r")]);
        outcome
            .refresh
            .status_by_id
            .insert(ResourceId("r".into()), RefreshStatus::Unknown);
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);

        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(
            summary.contains("1 uncertainty"),
            "count at summary: {summary}"
        );
        assert!(
            !summary.contains("? r"),
            "members are detail-tier: {summary}"
        );

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(detail.contains("? r —"), "member at detail: {detail}");
        assert!(
            detail.contains("resolve:"),
            "resolution at detail: {detail}"
        );
        assert!(
            detail.contains("live state: unconfirmed"),
            "per-change evidence: {detail}"
        );
    }

    // ---- Property suite (evidence-model Properties 5, 6, 8, 11) ----
    //
    // These bind the shipped renderer itself — the same `ExplanationReport`
    // the binary prints, binding lines included — so a copy change that leaks
    // internal vocabulary or breaks depth discipline fails here, not in a
    // mirror that can drift. Operator content (resource names, field values)
    // passes through reports verbatim by design, so the generators keep
    // content to fixed clean tokens: any banned term in the output is the
    // renderer's own copy.

    use std::collections::BTreeMap;

    use proptest::prelude::*;

    /// The vocabulary ban enforced over this report's narrative: every term
    /// from `operator-language.md`'s banned list, plus the lexicon-table
    /// "banned in prose" synonyms this report's domain could plausibly leak.
    /// Cross-checked against the doc below so the suite and the doc cannot
    /// drift.
    const BANNED: &[&str] = &[
        // §The banned list.
        "envelope",
        "config_revision",
        "marker",
        "ownership transferred",
        "[A final]",
        "advisory",
        "authoritative",
        "DevIterate",
        "candidate",
        "married",
        "driving",
        "forwarding",
        "launching",
        "reconcile",
        "(s)",
        // §The lexicon, third column.
        "provider",
        "backend",
        "delta",
        "physical id",
        "state entry",
        "unknown status",
        "refresh status",
        "checkpoint",
        "snapshot",
    ];

    #[test]
    fn the_banned_list_matches_the_language_doc() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/platforms/operator-language.md"
        );
        let doc = std::fs::read_to_string(path).expect("operator-language.md exists");
        for term in BANNED {
            assert!(
                doc.contains(*term),
                "banned term {term:?} is not recorded in operator-language.md — \
                 the suite and the doc have drifted"
            );
        }
    }

    fn arb_kind() -> impl Strategy<Value = ChangeKind> {
        prop_oneof![
            Just(ChangeKind::Create),
            Just(ChangeKind::Update),
            Just(ChangeKind::Replace),
            Just(ChangeKind::Delete),
            Just(ChangeKind::NoChange),
        ]
    }

    fn arb_verdict() -> impl Strategy<Value = BindingVerdict> {
        prop_oneof![
            Just(BindingVerdict::Match),
            Just(BindingVerdict::DevIterate),
            Just(BindingVerdict::Mismatch),
            Just(BindingVerdict::Downgrade),
            Just(BindingVerdict::ModeRegression),
            Just(BindingVerdict::Unknown),
        ]
    }

    prop_compose! {
        /// A report over an arbitrary plan outcome: every change kind, per-
        /// resource refresh statuses, both examined states, every binding
        /// verdict, and diff values long enough to cross the truncation
        /// boundary.
        fn arb_report()(
            rows in proptest::collection::vec((arb_kind(), any::<bool>(), 0usize..90), 0..6),
            examined in any::<bool>(),
            binding in arb_verdict(),
            initialized in any::<bool>(),
        ) -> ExplanationReport {
            let mut changes = Vec::new();
            let mut status_by_id = BTreeMap::new();
            for (index, (kind, unknown, value_len)) in rows.into_iter().enumerate() {
                let resource = format!("compose/r{index}");
                changes.push(Change {
                    kind,
                    resource_type: "compose_service".to_string(),
                    module: format!("m{index}"),
                    resource: resource.clone(),
                    details: vec![
                        tokeira_iac::FieldDiff::observation("dependencies changed"),
                        tokeira_iac::FieldDiff {
                            field: "image".to_string(),
                            before: Some("x".repeat(value_len)),
                            after: Some("y".repeat(value_len)),
                        },
                    ],
                });
                status_by_id.insert(
                    ResourceId(resource),
                    if unknown { RefreshStatus::Unknown } else { RefreshStatus::DesiredLive },
                );
            }
            let outcome = PlanOutcome {
                changes,
                refresh: RefreshCoverage { status_by_id, examined },
            };
            ExplanationReport {
                initialized,
                binding,
                explanation: explain_plan(context(), &outcome),
            }
        }
    }

    proptest! {
        // Property 5 — detail is a superset of summary: depth gates evidence,
        // never the answer, so every line the summary states appears verbatim
        // in the detail rendering.
        #[test]
        fn property_5_detail_is_a_superset_of_summary(report in arb_report()) {
            let summary = render(&report, Mode::resolve(false, false)).unwrap();
            let detail = render(&report, Mode::resolve(false, true)).unwrap();
            let detail_lines: std::collections::HashSet<&str> = detail.lines().collect();
            for line in summary.lines() {
                prop_assert!(
                    detail_lines.contains(line),
                    "summary line missing from detail: {:?}\ndetail:\n{}",
                    line,
                    detail,
                );
            }
        }

        // Property 6 — the structured form is complete and depth-blind:
        // identical JSON whatever the depth flag said, round-tripping to a
        // model equal to the one the report carries (the report serializes as
        // the explanation model alone).
        #[test]
        fn property_6_json_is_complete_and_depth_blind(report in arb_report()) {
            let a = render(&report, Mode::resolve(true, false)).unwrap();
            let b = render(&report, Mode::resolve(true, true)).unwrap();
            prop_assert_eq!(&a, &b, "JSON must be depth-blind");
            let back: DeploymentExplanation = serde_json::from_str(&a).unwrap();
            prop_assert_eq!(&back, &report.explanation, "JSON must round-trip to an equal model");
        }

        // Property 8 — not-determined slots are silent: Feature 1 leaves the
        // forward-compatible slots (semantics, cause, dependants, source) at
        // their defaults, and no narrative line may derive from them.
        #[test]
        fn property_8_not_determined_slots_are_silent(report in arb_report()) {
            for detail in [false, true] {
                let text = render(&report, Mode::resolve(false, detail)).unwrap().to_lowercase();
                for slot_word in ["cause", "impact", "disruption", "data effect",
                                  "reversibility", "replacement policy", "derived"] {
                    prop_assert!(
                        !text.contains(slot_word),
                        "slot vocabulary {:?} leaked into narrative: {}",
                        slot_word,
                        text,
                    );
                }
            }
        }

        // Property 11 — the narrative stays inside the lexicon at every depth,
        // over every binding verdict the generator ranges across.
        #[test]
        fn property_11_rendering_stays_inside_the_lexicon(report in arb_report()) {
            for detail in [false, true] {
                let text = render(&report, Mode::resolve(false, detail)).unwrap().to_lowercase();
                for term in BANNED {
                    prop_assert!(
                        !text.contains(&term.to_lowercase()),
                        "banned term {:?} appeared in narrative: {}",
                        term,
                        text,
                    );
                }
            }
        }
    }
}
