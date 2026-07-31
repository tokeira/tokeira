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

use tokeira_explain::{DeploymentExplanation, ExplainedChange};
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
///
/// C5 migration note (evidence-model design): if a consumer outside `tkp`
/// ever needs to render an explanation — an artifact viewer, Feature 5's
/// analysis bundles — this `Report` impl moves into `tokeira-explain` and
/// that crate takes `tokeira-report` (a serde-only crate) as a dependency.
/// The move is mechanical; recorded here so it is a decision, not a
/// surprise.
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
        let explanation = &self.explanation;
        // `# Infra Plan` — the operation, title-cased.
        out.push_str(&format!("# {}\n", title_case(&explanation.operation)));
        // The header assurance line carries the live-state coverage
        // (evidence-model Req 4.5/4.6 as amended): silence is never the
        // signal — the header always says what the plan's claims rest on.
        out.push_str(&format!(
            "**Plan for {}** {}\n",
            explanation.platform,
            coverage_clause(explanation)
        ));
        // Verdict narration is attention-only: a verdict that lets the apply
        // proceed is a standing fact (describe's story), not news on every
        // plan. Only what blocks or qualifies the apply earns a line.
        if let Some(line) = binding_attention(self.initialized, self.binding) {
            out.push_str(&format!("\n**binding:** {line}\n"));
        }
        action_sections(explanation, depth, out);
        impacts_section(explanation, out);
    }
}

/// Title-case an operation ("infra plan" → "Infra Plan").
fn title_case(operation: &str) -> String {
    operation
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// The header's coverage clause: what the plan's claims rest on. Gaps in
/// live state speak here (and in place at detail); declared behaviour is
/// the sections' business. Undeclared-semantics entries are machine-channel
/// only and never render (change-semantics Req 6.5).
fn coverage_clause(explanation: &DeploymentExplanation) -> String {
    use tokeira_explain::UncertaintyReason;
    if explanation
        .uncertainties
        .iter()
        .any(|u| matches!(u.reason, UncertaintyReason::LiveStateNotExamined))
    {
        return "without *live state* examined".to_string();
    }
    let unconfirmed = explanation
        .uncertainties
        .iter()
        .filter(|u| matches!(u.reason, UncertaintyReason::LiveStateUnconfirmed))
        .count();
    match unconfirmed {
        0 => "with *live state* confirmed".to_string(),
        1 => "with *live state* unconfirmed for 1 resource".to_string(),
        n => format!("with *live state* unconfirmed for {n} resources"),
    }
}

/// The operator noun phrase for a change: "the *tokeirad* service" when the
/// plan holds more than one resource of the kind, "the *Aurora DSQL
/// cluster*" when the kind noun alone identifies it; the engine id when the
/// kind declares no noun.
fn noun_phrase(explanation: &DeploymentExplanation, change: &ExplainedChange) -> String {
    match &change.display {
        Some(noun) => {
            let siblings = explanation
                .changes
                .iter()
                .filter(|c| c.display.as_deref() == Some(noun.as_str()))
                .count();
            if siblings > 1 {
                format!("the *{}* {noun}", change.module)
            } else {
                format!("the *{noun}*")
            }
        }
        None => format!("`{}::{}`", change.module, change.resource_id),
    }
}

/// Wrap a value as a Markdown code span, surviving embedded backticks.
fn code_span(value: &str) -> String {
    if value.contains('`') {
        format!("`` {value} ``")
    } else {
        format!("`{value}`")
    }
}

/// Render a citation for detail depth: documentation as a link, code by
/// module identity as a code span (change-semantics Req 9.7).
fn cite(citation: &tokeira_iac::Citation) -> String {
    match citation {
        tokeira_iac::Citation::Code(reference) => code_span(reference),
        tokeira_iac::Citation::Doc { title, url, .. } => format!("[{title}]({url})"),
    }
}

/// A behaviour claim in its confidence voice with its citation (Req 9.2/9.3
/// at detail): an engine fact speaks plainly, a provider guarantee is
/// attributed by its linked document, an inference owns itself.
fn voiced(claim: &str, confidence_citation: &tokeira_iac::Confidence<impl Sized>) -> String {
    use tokeira_iac::Confidence;
    match confidence_citation {
        Confidence::Unknown => claim.to_string(),
        Confidence::EngineFact { citation, .. } => format!("{claim} - {}", cite(citation)),
        Confidence::ProviderGuarantee { citation, .. } => {
            format!("{claim} - per {}", cite(citation))
        }
        Confidence::Inference { citation, .. } => {
            format!("{claim}; Tokeira derives this - per {}", cite(citation))
        }
    }
}

/// The engine-kind verb in would-mood.
fn kind_verb(kind: ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Create => "created",
        ChangeKind::Update => "updated",
        ChangeKind::Replace => "replaced",
        ChangeKind::Delete => "deleted",
        ChangeKind::NoChange => "unchanged",
    }
}

/// The `##` action sections: one per present action, each change a line of
/// templated would-mood prose with its engine id stated once. Detail adds
/// field evidence, the declared behaviour in its confidence voice, and the
/// in-place live-state statement.
fn action_sections(explanation: &DeploymentExplanation, depth: Depth, out: &mut String) {
    let acting_exists = explanation
        .changes
        .iter()
        .any(|c| c.kind != ChangeKind::NoChange);
    if !acting_exists {
        out.push_str("\nNo changes - everything matches the definition.\n");
    }
    for (kind, section) in [
        (ChangeKind::Create, "## Create"),
        (ChangeKind::Update, "## Update"),
        (ChangeKind::Replace, "## Replace"),
        (ChangeKind::Delete, "## Delete"),
    ] {
        let members: Vec<&ExplainedChange> = explanation
            .changes
            .iter()
            .filter(|c| c.kind == kind)
            .collect();
        if members.is_empty() {
            continue;
        }
        out.push_str(&format!("\n{section}\n"));
        for change in members {
            // A named resource reads as prose with its id once at the tail;
            // a kind without a noun leads with the id and never repeats it.
            let line = if change.display.is_some() {
                format!(
                    "- {} would be {} - `{}::{}`\n",
                    noun_phrase(explanation, change),
                    kind_verb(kind),
                    change.module,
                    change.resource_id,
                )
            } else {
                format!(
                    "- `{}::{}` would be {}\n",
                    change.module,
                    change.resource_id,
                    kind_verb(kind),
                )
            };
            out.push_str(&line);
            if depth == Depth::Detail {
                change_detail(change, out);
            }
        }
    }
    if depth == Depth::Detail {
        let unchanged: Vec<&ExplainedChange> = explanation
            .changes
            .iter()
            .filter(|c| c.kind == ChangeKind::NoChange)
            .collect();
        if !unchanged.is_empty() {
            out.push_str("\n## Unchanged\n");
            for change in unchanged {
                let line = if change.display.is_some() {
                    format!(
                        "- {} - `{}::{}`\n",
                        noun_phrase(explanation, change),
                        change.module,
                        change.resource_id,
                    )
                } else {
                    format!("- `{}::{}`\n", change.module, change.resource_id)
                };
                out.push_str(&line);
            }
        }
    }
}

/// Detail sub-bullets for one change: field diffs as code spans, then the
/// declared behaviour — mechanism, data effect, reversibility — each in its
/// confidence voice with its citation. Unknown fields render nothing
/// (knowledge renders; gaps enforce).
fn change_detail(change: &ExplainedChange, out: &mut String) {
    use tokeira_iac::{DataEffect, LifecycleOperation, Reversibility};
    if matches!(change.kind, ChangeKind::Update | ChangeKind::Replace) {
        for diff in &change.field_diffs {
            if diff.before.is_none() && diff.after.is_none() {
                out.push_str(&format!("  - {}\n", diff.field));
            } else {
                out.push_str(&format!(
                    "  - {}: {} → {}\n",
                    code_span(&diff.field),
                    code_span(&truncate(diff.before.as_deref().unwrap_or("(none)"))),
                    code_span(&truncate(diff.after.as_deref().unwrap_or("(none)"))),
                ));
            }
        }
    }
    // An unconfirmable live read speaks in place (Req 4.6 as amended).
    if matches!(change.refresh_status, Some(RefreshStatus::Unknown)) {
        out.push_str("  - its live state could not be confirmed\n");
    }

    let semantics = &change.semantics;
    // The mechanism: an update effected as a replacement is the load-bearing
    // truth; the kind-authored statement carries the how when one exists.
    let effected_as_replace = change.kind == ChangeKind::Update
        && semantics.operation.value() == Some(&LifecycleOperation::Replaced);
    match (&semantics.statement, effected_as_replace) {
        (Some(statement), true) => out.push_str(&format!(
            "  - {}\n",
            voiced(
                &format!("the update replaces it - {statement}"),
                &semantics.operation
            )
        )),
        (Some(statement), false) => {
            out.push_str(&format!(
                "  - {}\n",
                voiced(statement, &semantics.operation)
            ));
        }
        (None, true) => out.push_str(&format!(
            "  - {}\n",
            voiced(
                "the update replaces it - it would be destroyed, then recreated",
                &semantics.operation
            )
        )),
        (None, false) => {}
    }
    if let Some(effect) = semantics.data_effect.value() {
        let claim = match effect {
            DataEffect::NoDataHeld => None,
            DataEffect::Preserved => Some("the data it holds would be preserved"),
            DataEffect::Migrated => Some("its data would be migrated"),
            DataEffect::Destroyed => Some("its stored data would be destroyed"),
            // The general value; the kind's statement carries the specific
            // policy ("items past their declared expiry would be deleted").
            DataEffect::Policy => Some("its data follows its declared policy"),
        };
        if let Some(claim) = claim {
            out.push_str(&format!("  - {}\n", voiced(claim, &semantics.data_effect)));
        }
    }
    if let Some(reversibility) = semantics.reversibility.value() {
        let claim = match (reversibility, change.kind) {
            (Reversibility::Reversible, ChangeKind::Update) => {
                "re-applying the prior definition would restore it"
            }
            (Reversibility::Reversible, _) => "re-applying the definition would restore it",
            (Reversibility::ReversibleWithDataLoss, _) => {
                "reversing it would lose data written since"
            }
            (Reversibility::Irreversible, _) => "it could not be reversed",
        };
        out.push_str(&format!(
            "  - {}\n",
            voiced(claim, &semantics.reversibility)
        ));
    }
}

/// The `## Impacts` section: one templated line per subject, severity-first
/// (the model's own order), speaking descriptive names only — ids live in
/// the action sections. Templates specialize on the subject's change kind,
/// and data destruction states irreversibility where the subject declares
/// it (change-semantics Req 9.1/9.8).
fn impacts_section(explanation: &DeploymentExplanation, out: &mut String) {
    use tokeira_explain::ImpactClass;
    use tokeira_iac::Reversibility;
    if explanation.impacts.is_empty() {
        return;
    }
    out.push_str("\n## Impacts\n");
    for impact in &explanation.impacts {
        for subject in &impact.subjects {
            let Some(change) = explanation
                .changes
                .iter()
                .find(|c| &c.evidence_id == subject)
            else {
                continue;
            };
            let phrase = noun_phrase(explanation, change);
            let line = match impact.class {
                ImpactClass::DataDestroyed => {
                    let irreversibly = change.semantics.reversibility.value()
                        == Some(&Reversibility::Irreversible);
                    if irreversibly {
                        format!("data held by {phrase} would be destroyed, irreversibly")
                    } else {
                        format!("data held by {phrase} would be destroyed")
                    }
                }
                ImpactClass::Unavailability => {
                    if change.kind == ChangeKind::Delete {
                        format!("{phrase} would no longer be available")
                    } else {
                        format!("{phrase} would be unavailable while the change applies")
                    }
                }
                ImpactClass::Replacement => format!("{phrase} would be replaced"),
                ImpactClass::BriefInterruption => {
                    format!("{phrase} would be briefly interrupted")
                }
                ImpactClass::RollingReplacement => {
                    format!("{phrase} would be replaced one at a time")
                }
            };
            out.push_str(&format!("- {line}\n"));
        }
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

    /// A fully-declared semantics value: these tests exercise refresh and
    /// depth behaviour, so declarations are complete to keep the
    /// undeclared-semantics activation out of their frame (it has its own
    /// tests below).
    fn declared() -> tokeira_iac::ChangeSemantics {
        use tokeira_iac::{
            Citation, Confidence, DataEffect, Disruption, LifecycleOperation, ReplacementPolicy,
            Reversibility,
        };
        const CITE: Citation = Citation::code("test/declared");
        tokeira_iac::ChangeSemantics {
            operation: Confidence::EngineFact {
                value: LifecycleOperation::UpdatedInPlace,
                citation: CITE,
            },
            replacement: Confidence::EngineFact {
                value: ReplacementPolicy::NotRequired,
                citation: CITE,
            },
            disruption: Confidence::EngineFact {
                value: Disruption::None,
                citation: CITE,
            },
            data_effect: Confidence::EngineFact {
                value: DataEffect::Preserved,
                citation: CITE,
            },
            reversibility: Confidence::EngineFact {
                value: Reversibility::Reversible,
                citation: CITE,
            },
            statement: None,
        }
    }

    fn examined(changes: Vec<Change>) -> PlanOutcome {
        // Every planned resource confirmed live — the fully-confirmed case.
        let status_by_id = changes
            .iter()
            .map(|c| (ResourceId(c.resource.clone()), RefreshStatus::DesiredLive))
            .collect();
        let semantics_by_id = changes
            .iter()
            .filter(|c| c.kind != ChangeKind::NoChange)
            .map(|c| (ResourceId(c.resource.clone()), declared()))
            .collect();
        PlanOutcome {
            changes,
            refresh: RefreshCoverage {
                status_by_id,
                examined: true,
                ..Default::default()
            },
            semantics_by_id,
            ..Default::default()
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
        assert!(summary.contains("# Infra Plan\n"));
        assert!(summary.contains("## Update\n- `m::r` would be updated\n"));
        assert!(!summary.contains("`image`") && !summary.contains("## Unchanged"));

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(detail.contains("  - `image`: `a` → `b`"));
        assert!(detail.contains("## Unchanged\n- `m::r2`"));
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
        assert!(text.contains("No changes - everything matches the definition."));
        assert!(text.contains("**Plan for test** with *live state* confirmed"));
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
            summary.contains("with *live state* unconfirmed for 1 resource"),
            "the header carries the coverage: {summary}"
        );
        assert!(
            !summary.contains("could not be confirmed"),
            "the in-place statement is detail-tier: {summary}"
        );

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(
            detail.contains("  - its live state could not be confirmed"),
            "in-place statement at detail: {detail}"
        );
    }

    // Phase 6 checkpoint (task 6.7, in-crate half): the storage plan renders
    // the Markdown target — declared behaviour in confidence voices with
    // citations, impacts severity-first in kind-specialized templates, and
    // NO gap prose: the undeclared DSQL fields stay machine-channel (model
    // and artifact) under the knowledge-renders doctrine.
    #[test]
    fn a_storage_plan_renders_knowledge_and_keeps_gaps_machine_side() {
        use tokeira_iac::{Citation, Confidence, Disruption, LifecycleOperation};
        const CITE: Citation = Citation::code("test/dsql-delete");

        let mut outcome = examined(vec![
            change(ChangeKind::Update, "compose/tokeirad"),
            change(ChangeKind::Delete, "dsql/monitored"),
        ]);
        // Mirror the real ComposeService update declaration, statement
        // included.
        outcome.semantics_by_id.insert(
            ResourceId("compose/tokeirad".into()),
            tokeira_iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Replaced,
                    citation: CITE,
                },
                replacement: Confidence::EngineFact {
                    value: tokeira_iac::ReplacementPolicy::DestroyBeforeCreate,
                    citation: CITE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: CITE,
                },
                data_effect: Confidence::EngineFact {
                    value: tokeira_iac::DataEffect::Preserved,
                    citation: CITE,
                },
                reversibility: Confidence::EngineFact {
                    value: tokeira_iac::Reversibility::Reversible,
                    citation: CITE,
                },
                statement: Some(std::borrow::Cow::Borrowed(
                    "it would be stopped, removed, and recreated from the definition",
                )),
            },
        );
        // Mirror the real DsqlCluster managed-delete declaration shape with
        // its fields-not-yet-declared world: operation/disruption stated,
        // data effect and reversibility Unknown.
        outcome.semantics_by_id.insert(
            ResourceId("dsql/monitored".into()),
            tokeira_iac::ChangeSemantics {
                operation: Confidence::EngineFact {
                    value: LifecycleOperation::Deleted,
                    citation: CITE,
                },
                replacement: Confidence::EngineFact {
                    value: tokeira_iac::ReplacementPolicy::NotRequired,
                    citation: CITE,
                },
                disruption: Confidence::EngineFact {
                    value: Disruption::UnavailableDuringChange,
                    citation: CITE,
                },
                data_effect: Confidence::Unknown,
                reversibility: Confidence::Unknown,
                statement: Some(std::borrow::Cow::Borrowed(
                    "deletion protection would be disabled first, then the cluster deleted",
                )),
            },
        );
        let r = report_for(&outcome, BindingVerdict::DevIterate, true);

        let summary = render(&r, Mode::resolve(false, false)).unwrap();
        assert!(summary.contains("# Infra Plan\n"), "title: {summary}");
        assert!(
            summary.contains("**Plan for test** with *live state* confirmed"),
            "header assurance: {summary}"
        );
        assert!(
            summary.contains("## Update\n- `m::compose/tokeirad` would be updated\n"),
            "update section: {summary}"
        );
        assert!(
            summary.contains("## Delete\n- `m::dsql/monitored` would be deleted\n"),
            "delete section: {summary}"
        );
        assert!(
            summary.contains(
                "## Impacts\n- `m::compose/tokeirad` would be unavailable while the change applies\n- `m::dsql/monitored` would no longer be available\n- `m::compose/tokeirad` would be replaced\n"
            ),
            "impacts severity-first, kind-specialized: {summary}"
        );

        let detail = render(&r, Mode::resolve(false, true)).unwrap();
        assert!(
            detail.contains(
                "  - the update replaces it - it would be stopped, removed, and recreated from the definition - `test/dsql-delete`"
            ),
            "mechanism in the engine's voice with its citation: {detail}"
        );
        assert!(
            detail.contains("  - the data it holds would be preserved - `test/dsql-delete`"),
            "data effect voiced and cited: {detail}"
        );
        assert!(
            detail.contains(
                "  - deletion protection would be disabled first, then the cluster deleted - `test/dsql-delete`"
            ),
            "the delete's kind-authored statement: {detail}"
        );

        // Knowledge renders; gaps enforce: no gap prose at either depth —
        // the undeclared fields live machine-side only.
        for text in [&summary, &detail] {
            assert!(
                !text.contains("no declaration states"),
                "gap prose leaked into narrative: {text}"
            );
        }
        use tokeira_explain::UncertaintyReason;
        let machine_gaps = r
            .explanation
            .uncertainties
            .iter()
            .filter(|u| matches!(u.reason, UncertaintyReason::SemanticsUndeclared { .. }))
            .count();
        assert_eq!(machine_gaps, 2, "the gaps stay in the model for machines");
        assert_eq!(
            r.explanation.impacts.len(),
            2,
            "unavailability + replacement"
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
                refresh: RefreshCoverage {
                    status_by_id,
                    examined,
                    ..Default::default()
                },
                ..Default::default()
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

        // Property 8 — not-determined slots make no claims. Evolved with
        // change-semantics Phase 5: an undeclared field may now appear as a
        // *gap* (a SemanticsUndeclared uncertainty naming it), but never as
        // a consequence — with every declaration defaulted there are no
        // impacts, so no impact-statement vocabulary can appear, and the
        // still-dormant slots (cause, dependants, source) stay wholly
        // silent.
        #[test]
        fn property_8_not_determined_slots_are_silent(report in arb_report()) {
            for detail in [false, true] {
                let text = render(&report, Mode::resolve(false, detail)).unwrap().to_lowercase();
                for claim in ["would be destroyed",
                              "unavailable while the change applies",
                              "would be briefly interrupted",
                              "one at a time"] {
                    prop_assert!(
                        !text.contains(claim),
                        "undeclared semantics produced a consequence claim {:?}: {}",
                        claim,
                        text,
                    );
                }
                for slot_word in ["cause", "derived", "tokeira derives", "because of"] {
                    prop_assert!(
                        !text.contains(slot_word),
                        "dormant slot vocabulary {:?} leaked into narrative: {}",
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
